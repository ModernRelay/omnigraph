use axum::body::Bytes;
use futures::Stream;
use omnigraph::db::{EXPORT_CHUNK_MAX_BYTES, StreamExportCut};
use omnigraph::error::{OmniError, Result};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::timeout;

/// At most two produced chunks may wait behind the response consumer.
pub(crate) const EXPORT_QUEUE_CHUNKS: usize = 2;
/// One response reserves enough process memory for its bounded channel, the
/// producer's chunk awaiting admission, and the consumer's current chunk.
pub(crate) const EXPORT_QUEUE_RESERVED_BYTES: usize =
    (EXPORT_QUEUE_CHUNKS + 2) * EXPORT_CHUNK_MAX_BYTES;
/// At most eight fully reserved export responses may coexist process-wide.
pub(crate) const EXPORT_PROCESS_QUEUE_RESERVED_BYTES: usize = 8 * EXPORT_QUEUE_RESERVED_BYTES;
/// A saturated request waits only briefly for a queue reservation.
pub(crate) const EXPORT_RESERVATION_TIMEOUT: Duration = Duration::from_millis(250);

static PROCESS_EXPORT_QUEUE_BYTES: OnceLock<Arc<Semaphore>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct ExportTransport {
    available_bytes: Arc<Semaphore>,
    queue_reserved_bytes: u32,
    process_queue_reserved_bytes: usize,
    reservation_timeout: Duration,
}

impl ExportTransport {
    pub(crate) fn with_defaults() -> Self {
        let available_bytes = Arc::clone(
            PROCESS_EXPORT_QUEUE_BYTES
                .get_or_init(|| Arc::new(Semaphore::new(EXPORT_PROCESS_QUEUE_RESERVED_BYTES))),
        );
        Self {
            available_bytes,
            queue_reserved_bytes: u32::try_from(EXPORT_QUEUE_RESERVED_BYTES)
                .expect("served export response reservation fits in u32"),
            process_queue_reserved_bytes: EXPORT_PROCESS_QUEUE_RESERVED_BYTES,
            reservation_timeout: EXPORT_RESERVATION_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn new(
        process_queue_reserved_bytes: usize,
        queue_reserved_bytes: usize,
        reservation_timeout: Duration,
    ) -> Self {
        assert!(queue_reserved_bytes > 0);
        assert!(queue_reserved_bytes <= process_queue_reserved_bytes);
        let queue_reserved_bytes = u32::try_from(queue_reserved_bytes)
            .expect("served export response reservation fits in u32");
        Self {
            available_bytes: Arc::new(Semaphore::new(process_queue_reserved_bytes)),
            queue_reserved_bytes,
            process_queue_reserved_bytes,
            reservation_timeout,
        }
    }

    pub(crate) async fn reserve(&self) -> Result<Arc<ExportQueueLease>> {
        let acquisition =
            Arc::clone(&self.available_bytes).acquire_many_owned(self.queue_reserved_bytes);
        match timeout(self.reservation_timeout, acquisition).await {
            Ok(Ok(permit)) => Ok(Arc::new(ExportQueueLease { _permit: permit })),
            Ok(Err(_closed)) => Err(OmniError::manifest_internal(
                "served export transport byte budget closed unexpectedly",
            )),
            Err(_elapsed) => Err(OmniError::ResourceLimitExceeded {
                resource: "stream_export_transport_bytes".to_string(),
                limit: self.process_queue_reserved_bytes as u64,
                actual: self
                    .process_queue_reserved_bytes
                    .saturating_add(self.queue_reserved_bytes as usize)
                    as u64,
            }),
        }
    }
}

/// Two-party ownership of one transport-queue reservation. The body and
/// producer each retain an `Arc`, so a disconnect cannot recycle the permit
/// while an unscheduled producer still owns a pending chunk.
#[derive(Debug)]
pub(crate) struct ExportQueueLease {
    _permit: OwnedSemaphorePermit,
}

pub(crate) enum ExportFrame {
    Data(Bytes),
    Terminal {
        /// The move-only cut stays queued behind every data frame. Dropping a
        /// disconnected response drops this frame and releases the root slot.
        cut: StreamExportCut,
        error: Option<io::Error>,
    },
}

/// Response body stream that owns the consumer half of the process-wide queue
/// reservation and any queued terminal export cut. The producer owns the other
/// lease half, so disconnect closes its receiver immediately but cannot recycle
/// bytes until the producer has actually unwound.
pub(crate) struct ExportBodyStream {
    receiver: mpsc::Receiver<ExportFrame>,
    lease: Option<Arc<ExportQueueLease>>,
    done: bool,
}

pub(crate) fn channel(
    lease: Arc<ExportQueueLease>,
) -> (mpsc::Sender<ExportFrame>, ExportBodyStream) {
    let (sender, receiver) = mpsc::channel(EXPORT_QUEUE_CHUNKS);
    (
        sender,
        ExportBodyStream {
            receiver,
            lease: Some(lease),
            done: false,
        },
    )
}

impl Stream for ExportBodyStream {
    type Item = std::result::Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(ExportFrame::Data(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(ExportFrame::Terminal { cut, error })) => {
                drop(cut);
                self.lease.take();
                self.done = true;
                match error {
                    Some(error) => Poll::Ready(Some(Err(error))),
                    None => Poll::Ready(None),
                }
            }
            Poll::Ready(None) => {
                self.lease.take();
                self.done = true;
                Poll::Ready(Some(Err(io::Error::other(
                    "served export producer ended without terminal status",
                ))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use omnigraph::db::Omnigraph;

    #[test]
    fn default_budget_accounts_for_every_owned_chunk() {
        assert_eq!(EXPORT_CHUNK_MAX_BYTES, 64 * 1024);
        assert_eq!(EXPORT_QUEUE_RESERVED_BYTES, 4 * EXPORT_CHUNK_MAX_BYTES);
        assert_eq!(
            EXPORT_PROCESS_QUEUE_RESERVED_BYTES,
            8 * EXPORT_QUEUE_RESERVED_BYTES
        );
    }

    #[tokio::test]
    async fn saturated_budget_refuses_then_recovers_without_leaking() {
        let transport = ExportTransport::new(4, 4, Duration::from_millis(10));
        let first = transport.reserve().await.unwrap();

        let error = transport.reserve().await.unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 4,
                actual: 8,
            } if resource == "stream_export_transport_bytes"
        ));

        drop(first);
        let second = transport.reserve().await.unwrap();
        drop(second);
        assert_eq!(transport.available_bytes.available_permits(), 4);
    }

    #[tokio::test]
    async fn disconnected_body_cannot_recycle_bytes_before_producer_exits() {
        let transport = ExportTransport::new(4, 4, Duration::from_millis(10));
        let lease = transport.reserve().await.unwrap();
        let producer_lease = Arc::clone(&lease);
        let (_sender, body) = channel(lease);

        drop(body);
        assert!(matches!(
            transport.reserve().await.unwrap_err(),
            OmniError::ResourceLimitExceeded { .. }
        ));

        drop(producer_lease);
        let recovered = transport.reserve().await.unwrap();
        drop(recovered);
        assert_eq!(transport.available_bytes.available_permits(), 4);
    }

    #[tokio::test]
    async fn bounded_channel_backpressures_after_two_chunks() {
        let transport = ExportTransport::new(4, 4, Duration::from_millis(10));
        let lease = transport.reserve().await.unwrap();
        let (sender, _body) = channel(lease);

        sender
            .try_send(ExportFrame::Data(Bytes::from_static(b"a")))
            .unwrap();
        sender
            .try_send(ExportFrame::Data(Bytes::from_static(b"b")))
            .unwrap();
        assert!(matches!(
            sender.try_send(ExportFrame::Data(Bytes::from_static(b"c"))),
            Err(mpsc::error::TrySendError::Full(_))
        ));
    }

    #[tokio::test]
    async fn missing_terminal_frame_is_a_body_error_not_clean_eof() {
        let transport = ExportTransport::new(4, 4, Duration::from_millis(10));
        let lease = transport.reserve().await.unwrap();
        let (sender, mut body) = channel(lease);
        drop(sender);

        let error = body.next().await.unwrap().unwrap_err();
        assert_eq!(
            error.to_string(),
            "served export producer ended without terminal status"
        );
        assert!(body.next().await.is_none());
    }

    #[tokio::test]
    async fn completed_producer_transfers_root_cut_to_terminal_frame() {
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(
            Omnigraph::init(
                temp.path().to_string_lossy().as_ref(),
                "node Empty { key: String @key }",
            )
            .await
            .unwrap(),
        );
        let cut = db
            .capture_served_export_cut("main", &[], &[])
            .await
            .unwrap();
        let transport = ExportTransport::new(4, 4, Duration::from_millis(10));
        let lease = transport.reserve().await.unwrap();
        let producer_lease = Arc::clone(&lease);
        let (sender, body) = channel(lease);

        let data_sender = sender.clone();
        let (cut, result) = cut
            .write_chunks(move |chunk| {
                let data_sender = data_sender.clone();
                async move {
                    data_sender
                        .send(ExportFrame::Data(Bytes::from(chunk)))
                        .await
                        .map_err(|_| OmniError::Io(io::Error::other("closed")))
                }
            })
            .await;
        result.unwrap();
        sender
            .send(ExportFrame::Terminal { cut, error: None })
            .await
            .unwrap();
        drop(sender);
        drop(producer_lease);

        let error = match db.capture_served_export_cut("main", &[], &[]).await {
            Ok(_) => panic!("queued terminal cut must keep the root slot"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 1,
                actual: 2,
            } if resource == "stream_export_slots"
        ));

        drop(body);
        let retry = db
            .capture_served_export_cut("main", &[], &[])
            .await
            .unwrap();
        drop(retry);
        assert_eq!(transport.available_bytes.available_permits(), 4);
    }
}
