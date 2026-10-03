use axum::body::Bytes;
use futures::Stream;
use omnigraph::db::{EXPORT_CHUNK_MAX_BYTES, ExportCut};
use omnigraph::error::{OmniError, Result};
use std::io::{self, Write};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::timeout;

/// At most two produced chunks may wait behind the response consumer.
pub(crate) const EXPORT_QUEUE_CHUNKS: usize = 2;
/// Queued and yielded chunks share these credits; clones and slices keep a
/// credit until their last owner drops. One further slot covers the sequential
/// producer's chunk while it waits for a credit.
const EXPORT_OUTSTANDING_FRAMES: usize = EXPORT_QUEUE_CHUNKS + 1;
/// One response reserves payload capacity for outstanding and pending chunks.
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
            Ok(Ok(permit)) => Ok(Arc::new(ExportQueueLease {
                _permit: permit,
                frames: Arc::new(Semaphore::new(EXPORT_OUTSTANDING_FRAMES)),
            })),
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

/// The producer, body and every yielded chunk retain this process reservation.
/// Frame credits subdivide its existing allowance; they are not another pool.
#[derive(Debug)]
pub(crate) struct ExportQueueLease {
    _permit: OwnedSemaphorePermit,
    frames: Arc<Semaphore>,
}

/// A single sequential producer hands off at most one unadmitted chunk at a
/// time. The engine's write_chunks callback awaits each send before creating
/// the next chunk; the baseline record uses the same producer after it ends.
#[derive(Clone)]
pub(crate) struct ExportSender {
    sender: mpsc::Sender<ExportFrame>,
    lease: Arc<ExportQueueLease>,
}

impl ExportSender {
    pub(crate) async fn closed(&self) {
        self.sender.closed().await;
    }

    pub(crate) async fn send_chunk(&self, chunk: Vec<u8>) -> Result<()> {
        let bytes = chunk.len().max(chunk.capacity());
        if bytes > EXPORT_CHUNK_MAX_BYTES {
            return Err(OmniError::resource_limit(
                "stream_export_chunk_bytes",
                EXPORT_CHUNK_MAX_BYTES as u64,
                bytes as u64,
            ));
        }
        // These bytes already have the pending producer slot. Ordinary socket
        // backpressure is not admission failure: retain the reservation while
        // waiting for the consumer to release a preceding allocation. Observe
        // closure here too because the baseline cursor is sent after the
        // handler's outer export/closed select has completed.
        let frame = tokio::select! {
            biased;
            () = self.sender.closed() => {
                return Err(OmniError::Io(io::Error::other("served export response closed")));
            }
            frame = Arc::clone(&self.lease.frames).acquire_owned() => {
                frame.map_err(|_| OmniError::manifest_internal("served export frame credits closed"))?
            }
        };
        let bytes = Bytes::from_owner(ExportChunk {
            chunk,
            _frame: frame,
            _lease: Arc::clone(&self.lease),
        });
        // Queue backpressure has no new deadline. This chunk already holds a
        // credit, and the handler's close select interrupts a disconnected body.
        self.sender
            .send(ExportFrame::Data(bytes))
            .await
            .map_err(|_| OmniError::Io(io::Error::other("served export response closed")))
    }

    /// The baseline handshake fits one chunk (its cursor is capped at 4 KiB).
    /// Encode into that pending slot, then transfer the same allocation. Future
    /// larger records fail before any cursor bytes enter the response.
    pub(crate) async fn send_json_line<T: serde::Serialize>(&self, value: &T) -> Result<()> {
        let mut output = JsonChunk(Vec::with_capacity(EXPORT_CHUNK_MAX_BYTES));
        serde_json::to_writer(&mut output, value)
            .map_err(|error| OmniError::Io(io::Error::other(error)))?;
        output.write_all(b"\n")?;
        self.send_chunk(output.0).await
    }

    pub(crate) async fn finish(&self, cut: ExportCut, error: Option<io::Error>) {
        let _ = self
            .sender
            .send(ExportFrame::Terminal {
                cut: Box::new(cut),
                error,
            })
            .await;
    }
}

struct ExportChunk {
    // Drop payload storage before releasing either capacity owner.
    chunk: Vec<u8>,
    _frame: OwnedSemaphorePermit,
    _lease: Arc<ExportQueueLease>,
}

impl AsRef<[u8]> for ExportChunk {
    fn as_ref(&self) -> &[u8] {
        &self.chunk
    }
}

struct JsonChunk(Vec<u8>);

impl Write for JsonChunk {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > EXPORT_CHUNK_MAX_BYTES - self.0.len() {
            return Err(io::Error::other(
                "served export JSON record exceeds chunk limit",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum ExportFrame {
    Data(Bytes),
    Terminal {
        /// The move-only cut stays queued behind every data frame. Dropping a
        /// disconnected response drops this frame and releases the root slot.
        cut: Box<ExportCut>,
        error: Option<io::Error>,
    },
}

/// Response body stream that owns the consumer half of the process-wide queue
/// reservation and any queued terminal export cut. Disconnect closes its
/// receiver immediately; capacity remains owned by the producer and any chunks
/// retained outside the body, including clones and slices.
pub(crate) struct ExportBodyStream {
    receiver: mpsc::Receiver<ExportFrame>,
    lease: Option<Arc<ExportQueueLease>>,
    done: bool,
}

pub(crate) fn channel(lease: Arc<ExportQueueLease>) -> (ExportSender, ExportBodyStream) {
    let (sender, receiver) = mpsc::channel(EXPORT_QUEUE_CHUNKS);
    (
        ExportSender {
            sender,
            lease: Arc::clone(&lease),
        },
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
                let cut = *cut;
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
        let transport = ExportTransport::new(
            EXPORT_QUEUE_RESERVED_BYTES,
            EXPORT_QUEUE_RESERVED_BYTES,
            Duration::from_millis(10),
        );
        let lease = transport.reserve().await.unwrap();
        let producer_lease = Arc::clone(&lease);
        let (sender, mut body) = channel(lease);
        sender
            .send_chunk(vec![b'x'; EXPORT_CHUNK_MAX_BYTES])
            .await
            .unwrap();
        let yielded = body.next().await.unwrap().unwrap();
        let retained = yielded.slice(0..1);
        let cloned = yielded.clone();
        drop(yielded);
        drop(sender);
        drop(body);
        assert!(matches!(
            transport.reserve().await.unwrap_err(),
            OmniError::ResourceLimitExceeded { .. }
        ));

        drop(producer_lease);
        assert!(matches!(
            transport
                .reserve()
                .await
                .expect_err("a retained slice must keep its whole chunk charged"),
            OmniError::ResourceLimitExceeded { .. }
        ));
        drop(cloned);
        assert_eq!(transport.available_bytes.available_permits(), 0);
        assert_eq!(retained.as_ref(), b"x");
        drop(retained);
        let recovered = transport.reserve().await.unwrap();
        drop(recovered);
        assert_eq!(
            transport.available_bytes.available_permits(),
            EXPORT_QUEUE_RESERVED_BYTES
        );
    }

    #[tokio::test]
    async fn bounded_channel_backpressures_after_two_chunks() {
        let transport = ExportTransport::new(
            EXPORT_QUEUE_RESERVED_BYTES,
            EXPORT_QUEUE_RESERVED_BYTES,
            Duration::from_millis(10),
        );
        let lease = transport.reserve().await.unwrap();
        let (sender, mut body) = channel(lease);

        sender
            .send_chunk(vec![b'a'; EXPORT_CHUNK_MAX_BYTES])
            .await
            .unwrap();
        sender
            .send_chunk(vec![b'b'; EXPORT_CHUNK_MAX_BYTES])
            .await
            .unwrap();
        let first = {
            let third = sender.send_chunk(vec![b'c'; EXPORT_CHUNK_MAX_BYTES]);
            tokio::pin!(third);
            assert!(futures::poll!(&mut third).is_pending());
            let first = body.next().await.unwrap().unwrap();
            third.await.unwrap();
            first
        };
        let mut retained = vec![
            first,
            body.next().await.unwrap().unwrap(),
            body.next().await.unwrap().unwrap(),
        ];
        assert_eq!(
            retained.iter().map(Bytes::len).sum::<usize>(),
            3 * EXPORT_CHUNK_MAX_BYTES
        );
        assert_eq!(sender.lease.frames.available_permits(), 0);

        // The single pending chunk occupies the fourth reserved slot. A body
        // that retains all three preceding allocations must not admit it.
        {
            let pending = sender.send_chunk(vec![b'd'; EXPORT_CHUNK_MAX_BYTES]);
            tokio::pin!(pending);
            assert!(futures::poll!(&mut pending).is_pending());
            assert!(
                timeout(Duration::from_millis(30), &mut pending)
                    .await
                    .is_err(),
                "an admitted stream must wait beyond its initial admission deadline"
            );
            // Dropping one of several views of an allocation returns no credit.
            let last_view = retained.pop().unwrap().slice(0..1);
            assert_eq!(sender.lease.frames.available_permits(), 0);
            drop(last_view);
            pending.await.unwrap();
        }
        let fourth = body.next().await.unwrap().unwrap();
        assert_eq!(fourth[0], b'd');
        drop(fourth);
        // A consuming transport can deliver arbitrarily many frames. Its own
        // copied result is separate from the server-owned transport buffers.
        let mut received = Vec::new();
        for byte in b'd'..=b'z' {
            sender
                .send_chunk(vec![byte; EXPORT_CHUNK_MAX_BYTES])
                .await
                .unwrap();
            let frame = body.next().await.unwrap().unwrap();
            received.push(frame[0]);
            drop(frame);
        }
        assert_eq!(received, (b'd'..=b'z').collect::<Vec<_>>());
        drop(retained);

        for chunk in [vec![0; EXPORT_CHUNK_MAX_BYTES + 1], {
            let mut small = Vec::with_capacity(EXPORT_CHUNK_MAX_BYTES + 1);
            small.push(0);
            small
        }] {
            assert!(matches!(sender.send_chunk(chunk).await.unwrap_err(),
                OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "stream_export_chunk_bytes"));
        }
        // A future oversized handshake cannot allocate beyond one slot or
        // deliver a partial cursor record.
        let oversized = "x".repeat(EXPORT_CHUNK_MAX_BYTES);
        assert!(sender.send_json_line(&oversized).await.is_err());
        assert!(futures::poll!(body.next()).is_pending());
        sender.send_json_line(&"cursor").await.unwrap();
        assert_eq!(
            body.next().await.unwrap().unwrap().as_ref(),
            b"\"cursor\"\n"
        );

        drop(sender);
        drop(body);
        assert_eq!(
            transport.available_bytes.available_permits(),
            EXPORT_QUEUE_RESERVED_BYTES
        );
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
        let cut = db.capture_served_export_cut("main", &[]).await.unwrap();
        let transport = ExportTransport::new(
            EXPORT_QUEUE_RESERVED_BYTES,
            EXPORT_QUEUE_RESERVED_BYTES,
            Duration::from_millis(10),
        );
        let lease = transport.reserve().await.unwrap();
        let producer_lease = Arc::clone(&lease);
        let (sender, mut body) = channel(lease);

        let data_sender = sender.clone();
        let (cut, result) = cut
            .write_chunks(move |chunk| {
                let data_sender = data_sender.clone();
                async move { data_sender.send_chunk(chunk).await }
            })
            .await;
        result.unwrap();
        sender
            .send_chunk(vec![b'x'; EXPORT_CHUNK_MAX_BYTES])
            .await
            .unwrap();
        sender.finish(cut, None).await;
        drop(sender);
        drop(producer_lease);

        let error = match db.capture_served_export_cut("main", &[]).await {
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

        let retained = body.next().await.unwrap().unwrap();
        assert!(body.next().await.is_none());
        drop(body);
        let retry = db.capture_served_export_cut("main", &[]).await.unwrap();
        drop(retry);
        assert_eq!(transport.available_bytes.available_permits(), 0);
        drop(retained);
        assert_eq!(
            transport.available_bytes.available_permits(),
            EXPORT_QUEUE_RESERVED_BYTES
        );

        // Baseline sends its cursor after the outer export/closed select has
        // completed. Receiver closure must release this producer's cut even
        // when all frame credits survive in transport-owned clones or slices.
        let cut = db.capture_served_export_cut("main", &[]).await.unwrap();
        let (sender, mut body) = channel(transport.reserve().await.unwrap());
        let mut retained = Vec::new();
        for _ in 0..EXPORT_OUTSTANDING_FRAMES {
            sender
                .send_chunk(vec![b'x'; EXPORT_CHUNK_MAX_BYTES])
                .await
                .unwrap();
            retained.push(body.next().await.unwrap().unwrap().slice(..1));
        }
        {
            let terminal = async {
                let error = sender
                    .send_json_line(&serde_json::json!({"baseline": {"resume_cursor": "cursor"}}))
                    .await
                    .unwrap_err();
                assert!(matches!(error, OmniError::Io(_)));
                sender
                    .finish(cut, Some(io::Error::other(error.to_string())))
                    .await;
            };
            tokio::pin!(terminal);
            assert!(futures::poll!(&mut terminal).is_pending());
            drop(body);
            assert!(futures::poll!(&mut terminal).is_ready());
        }
        drop(sender);
        assert_eq!(transport.available_bytes.available_permits(), 0);
        let retry = db.capture_served_export_cut("main", &[]).await.unwrap();
        drop(retry);
        drop(retained);
        assert_eq!(
            transport.available_bytes.available_permits(),
            EXPORT_QUEUE_RESERVED_BYTES
        );
    }
}
