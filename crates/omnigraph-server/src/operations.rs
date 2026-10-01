//! Process-owned HTTP operations. This tracks server futures and response
//! producers, not arbitrary native I/O or a reusable engine-drain proof.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::FutureExt;
use tokio::sync::{Notify, oneshot};
use tracing::Instrument;

use crate::ApiError;

pub const DEFAULT_READ_OBSERVERS: usize = 128;
pub const DEFAULT_WRITE_OBSERVERS: usize = 64;

#[derive(Clone)]
pub struct OperationRuntime {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    changed: Notify,
    read_limit: usize,
    write_response_limit: usize,
}

#[derive(Default)]
struct State {
    closed: bool,
    active_writes: usize,
    active_reads: usize,
    active_write_responses: usize,
    // Ambiguity never returns admission capacity to a live process. The list
    // is bounded by admission; closing the epoch forbids further registration.
    uncertain: Vec<Box<dyn Any + Send>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationSnapshot {
    pub closed: bool,
    pub active_writes: usize,
    pub active_reads: usize,
    pub active_write_responses: usize,
    pub uncertain_writes: usize,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl OperationRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_read_limit(read_limit: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                read_limit,
                write_response_limit: DEFAULT_WRITE_OBSERVERS,
            }),
        }
    }

    /// Close both lanes under the same boundary as registration. There is no
    /// reopen method: a replacement serving epoch needs a new runtime.
    pub fn close(&self) {
        locked(&self.inner.state).closed = true;
        self.inner.changed.notify_waiters();
    }

    pub fn snapshot(&self) -> OperationSnapshot {
        let state = locked(&self.inner.state);
        OperationSnapshot {
            closed: state.closed,
            active_writes: state.active_writes,
            active_reads: state.active_reads,
            active_write_responses: state.active_write_responses,
            uncertain_writes: state.uncertain.len(),
        }
    }

    /// Server read lifetime only. Clone into each producer and response body;
    /// the last clone releases the observation.
    pub fn try_observe(&self) -> Result<ReadObserver, ApiError> {
        self.try_observe_response(false)
    }

    /// Writes have independent response capacity so slow readers cannot
    /// prevent a write from reaching operation admission.
    pub fn try_observe_write(&self) -> Result<ReadObserver, ApiError> {
        self.try_observe_response(true)
    }

    fn try_observe_response(&self, write: bool) -> Result<ReadObserver, ApiError> {
        let mut state = locked(&self.inner.state);
        if state.closed {
            return Err(ApiError::admission_closed());
        }
        let (active, limit, lane) = if write {
            (
                &mut state.active_write_responses,
                self.inner.write_response_limit,
                "write",
            )
        } else {
            (&mut state.active_reads, self.inner.read_limit, "read")
        };
        if *active >= limit {
            return Err(ApiError::too_many_requests(format!(
                "server {lane} observer cap {limit} exceeded"
            )));
        }
        *active += 1;
        Ok(ReadObserver {
            _life: Arc::new(ReadLife {
                inner: Arc::clone(&self.inner),
                write,
            }),
        })
    }

    /// Wait for known logical owners, refusing a clean result on ambiguity.
    /// Callers impose the original process shutdown deadline. This is not an
    /// engine-reuse or object-store-quiescence capability.
    pub async fn wait_logical_owners(&self) -> bool {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let snapshot = self.snapshot();
            if snapshot.active_writes == 0
                && snapshot.active_reads == 0
                && snapshot.active_write_responses == 0
            {
                return snapshot.uncertain_writes == 0;
            }
            changed.await;
        }
    }

    /// Sticky notification for the production host's fail-stop path.
    pub async fn fatal(&self) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.snapshot().uncertain_writes != 0 {
                return;
            }
            changed.await;
        }
    }

    /// Synchronous registration precedes spawning and the operation's first
    /// poll. The caller can drop its receiver without cancelling this owner.
    pub(crate) fn submit<T, R, F>(
        &self,
        reservation: R,
        operation: F,
    ) -> Result<OwnedResponse<T>, ApiError>
    where
        T: Send + 'static,
        R: Send + 'static,
        F: Future<Output = OwnedResult<T>> + Send + 'static,
    {
        {
            let mut state = locked(&self.inner.state);
            if state.closed {
                return Err(ApiError::admission_closed());
            }
            state.active_writes += 1;
        }
        let owner = WriteOwner {
            inner: Arc::clone(&self.inner),
            reservation: Some(Box::new(reservation)),
        };
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(
            async move {
                let result = match AssertUnwindSafe(operation).catch_unwind().await {
                    Ok(result) => result,
                    Err(_) => OwnedResult {
                        result: Err(ApiError::internal(
                            "owned write panicked; effects are unknown and require reconciliation",
                        )),
                        uncertain: true,
                    },
                };
                let uncertain = result.uncertain;
                // A single completed result is offered once. With no receiver the
                // result is dropped here; there is no detached-result history.
                owner.finish(uncertain);
                let _ = sender.send(result.result);
            }
            .in_current_span(),
        );
        Ok(OwnedResponse(receiver))
    }
}

impl Default for OperationRuntime {
    fn default() -> Self {
        Self::with_read_limit(DEFAULT_READ_OBSERVERS)
    }
}

pub(crate) struct OwnedResponse<T>(oneshot::Receiver<Result<T, ApiError>>);

impl<T> OwnedResponse<T> {
    pub(crate) async fn result(self) -> Result<T, ApiError> {
        self.0.await.unwrap_or_else(|_| {
            Err(ApiError::internal(
                "owned write result was lost; effects are unknown and require reconciliation",
            ))
        })
    }
}

/// Optional work can fail after an exact successful primary result. Preserve
/// that result while independently flagging an uncertain completion.
pub(crate) struct OwnedResult<T> {
    pub(crate) result: Result<T, ApiError>,
    pub(crate) uncertain: bool,
}

impl<T> From<Result<T, ApiError>> for OwnedResult<T> {
    fn from(result: Result<T, ApiError>) -> Self {
        let uncertain = result
            .as_ref()
            .is_err_and(|error| error.completion_uncertain());
        Self { result, uncertain }
    }
}

struct WriteOwner {
    inner: Arc<Inner>,
    reservation: Option<Box<dyn Any + Send>>,
}

impl WriteOwner {
    fn finish(mut self, uncertain: bool) {
        let reservation = self.reservation.take().expect("owner finishes once");
        if uncertain {
            let mut state = locked(&self.inner.state);
            state.active_writes -= 1;
            state.closed = true;
            state.uncertain.push(reservation);
        } else {
            drop(reservation);
            locked(&self.inner.state).active_writes -= 1;
        }
        self.inner.changed.notify_waiters();
    }
}

impl Drop for WriteOwner {
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            // Runtime cancellation and unwinding cannot masquerade as clean
            // completion. Retain the reservation until the process is gone.
            let mut state = locked(&self.inner.state);
            state.active_writes -= 1;
            state.closed = true;
            state.uncertain.push(reservation);
            self.inner.changed.notify_waiters();
        }
    }
}

#[derive(Clone)]
/// A response/producer lifetime in either independent observer lane.
pub struct ReadObserver {
    _life: Arc<ReadLife>,
}

struct ReadLife {
    inner: Arc<Inner>,
    write: bool,
}

impl Drop for ReadLife {
    fn drop(&mut self) {
        let mut state = locked(&self.inner.state);
        if self.write {
            state.active_write_responses -= 1;
        } else {
            state.active_reads -= 1;
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Reservation(Arc<AtomicUsize>);

    impl Drop for Reservation {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn receiver_loss_preserves_reservation_and_executes_once() {
        let runtime = OperationRuntime::new();
        let releases = Arc::new(AtomicUsize::new(0));
        let effects = Arc::new(AtomicUsize::new(0));
        let observed_effects = Arc::clone(&effects);
        let (release, held) = oneshot::channel();
        let result = runtime
            .submit(Reservation(Arc::clone(&releases)), async move {
                held.await.unwrap();
                observed_effects.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ApiError>(()).into()
            })
            .unwrap();
        drop(result);
        runtime.close();
        assert_eq!(runtime.snapshot().active_writes, 1);
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        assert!(runtime.try_observe().is_err());
        release.send(()).unwrap();
        assert!(runtime.wait_logical_owners().await);
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn panic_retains_capacity_and_poisoned_epoch_never_reopens() {
        let runtime = OperationRuntime::new();
        let releases = Arc::new(AtomicUsize::new(0));
        let response = runtime
            .submit(Reservation(Arc::clone(&releases)), async {
                panic!("contained write failure");
                #[allow(unreachable_code)]
                OwnedResult::from(Ok::<_, ApiError>(()))
            })
            .unwrap();
        assert!(response.result().await.is_err());
        runtime.fatal().await;
        assert!(!runtime.wait_logical_owners().await);
        assert_eq!(runtime.snapshot().uncertain_writes, 1);
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        assert!(
            runtime
                .submit((), async { Ok::<_, ApiError>(()).into() })
                .is_err()
        );
    }

    #[tokio::test]
    async fn read_observation_waits_for_the_last_producer_or_body() {
        let runtime = OperationRuntime::new();
        let body = runtime.try_observe().unwrap();
        let producer = body.clone();
        runtime.close();
        drop(body);
        assert_eq!(runtime.snapshot().active_reads, 1);
        drop(producer);
        assert!(runtime.wait_logical_owners().await);
        assert_eq!(runtime.snapshot().active_reads, 0);
    }

    #[tokio::test]
    async fn compound_uncertainty_keeps_success_but_closes_admission_before_delivery() {
        let runtime = OperationRuntime::new();
        let releases = Arc::new(AtomicUsize::new(0));
        let response = runtime
            .submit(Reservation(Arc::clone(&releases)), async {
                OwnedResult {
                    result: Ok::<_, ApiError>("exact primary receipt"),
                    uncertain: true,
                }
            })
            .unwrap();
        assert_eq!(response.result().await.unwrap(), "exact primary receipt");
        assert!(runtime.snapshot().closed);
        assert_eq!(runtime.snapshot().uncertain_writes, 1);
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        assert!(!runtime.wait_logical_owners().await);
    }

    #[tokio::test]
    async fn uncertainty_waits_for_other_logical_owners_before_nonclean_completion() {
        let runtime = OperationRuntime::new();
        let releases = Arc::new(AtomicUsize::new(0));
        let (release, held) = oneshot::channel();
        let healthy = runtime
            .submit(Reservation(Arc::clone(&releases)), async move {
                held.await.unwrap();
                Ok::<_, ApiError>(()).into()
            })
            .unwrap();
        let uncertain = runtime
            .submit(Reservation(Arc::clone(&releases)), async {
                Err::<(), _>(ApiError::internal("completion lost")).into()
            })
            .unwrap();
        assert!(uncertain.result().await.is_err());
        let wait = runtime.wait_logical_owners();
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        release.send(()).unwrap();
        healthy.result().await.unwrap();
        assert!(!wait.await);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.snapshot().uncertain_writes, 1);
    }

    #[test]
    fn disconnected_owned_operation_keeps_its_request_span() {
        let subscriber =
            tracing_subscriber::registry().with(tracing_subscriber::filter::LevelFilter::DEBUG);
        use tracing_subscriber::prelude::*;
        tracing::subscriber::with_default(subscriber, || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async {
                    let runtime = OperationRuntime::new();
                    let span = tracing::debug_span!("request", graph = "owned");
                    let expected = span.id().unwrap();
                    let (sent, observed) = oneshot::channel();
                    let response = {
                        let _entered = span.enter();
                        runtime
                            .submit((), async move {
                                sent.send(tracing::Span::current().id()).unwrap();
                                Ok::<_, ApiError>(()).into()
                            })
                            .unwrap()
                    };
                    drop(response);
                    assert_eq!(observed.await.unwrap(), Some(expected));
                    assert!(runtime.wait_logical_owners().await);
                });
        });
    }

    #[tokio::test]
    async fn typed_engine_uncertainty_controls_capacity_independently_of_status() {
        use omnigraph::error::OmniError;
        for (error, uncertain) in [
            (
                OmniError::Io(std::io::Error::other("schema read failed")).before_effect(),
                false,
            ),
            (
                OmniError::DataFusion("plan refusal".into()).before_effect(),
                false,
            ),
            (
                OmniError::manifest("post-publication fault")
                    .with_completion_evidence(omnigraph::error::CompletionEvidence::Uncertain),
                true,
            ),
            (
                OmniError::manifest_publish_in_doubt("uncertain publication").before_effect(),
                true,
            ),
            (
                OmniError::RecoveryRequired {
                    operation_id: "published".into(),
                    reason: "pending".into(),
                }
                .before_effect(),
                true,
            ),
            (
                OmniError::Io(std::io::Error::other("I/O completion failure")),
                true,
            ),
            (OmniError::DataFusion("execution failure".into()), true),
            (
                OmniError::RecoveryRequired {
                    operation_id: "published".into(),
                    reason: "contract installation pending".into(),
                },
                true,
            ),
            (OmniError::manifest_internal("internal failure"), true),
            (
                OmniError::manifest_publish_in_doubt("publication acknowledgement unavailable"),
                true,
            ),
            (OmniError::manifest("invalid request"), false),
            (OmniError::manifest_conflict("authority conflict"), false),
            (
                OmniError::PreconditionFailed {
                    branch: "main".into(),
                    expected: "old".into(),
                    actual: Some("current".into()),
                },
                false,
            ),
            (OmniError::Policy("denied".into()), false),
        ] {
            let runtime = OperationRuntime::new();
            let releases = Arc::new(AtomicUsize::new(0));
            let response = runtime
                .submit(Reservation(Arc::clone(&releases)), async move {
                    OwnedResult::from(Err::<(), _>(ApiError::from_omni(error)))
                })
                .unwrap();
            assert!(response.result().await.is_err());
            assert_eq!(runtime.snapshot().closed, uncertain);
            assert_eq!(runtime.snapshot().uncertain_writes, usize::from(uncertain));
            assert_eq!(releases.load(Ordering::SeqCst), usize::from(!uncertain));
            assert_eq!(runtime.wait_logical_owners().await, !uncertain);
        }
    }

    #[test]
    fn cancelled_owner_retains_its_reservation_without_a_supervisor_task() {
        let operations = OperationRuntime::new();
        let releases = Arc::new(AtomicUsize::new(0));
        let executor = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        executor.block_on(async {
            let response = operations
                .submit(Reservation(Arc::clone(&releases)), async {
                    std::future::pending::<()>().await;
                    Ok::<_, ApiError>(()).into()
                })
                .unwrap();
            drop(response);
            tokio::task::yield_now().await;
            assert_eq!(operations.snapshot().active_writes, 1);
        });
        drop(executor);
        assert!(operations.snapshot().closed);
        assert_eq!(operations.snapshot().active_writes, 0);
        assert_eq!(operations.snapshot().uncertain_writes, 1);
        assert_eq!(releases.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn read_cap_is_released_only_by_the_last_observer() {
        let runtime = OperationRuntime::with_read_limit(1);
        let first = runtime.try_observe().unwrap();
        let producer = first.clone();
        assert_eq!(
            runtime.try_observe().err().unwrap().status,
            StatusCode::TOO_MANY_REQUESTS
        );
        drop(first);
        assert!(runtime.try_observe().is_err());
        drop(producer);
        assert!(runtime.try_observe().is_ok());
    }

    #[tokio::test]
    async fn saturated_read_and_write_response_lanes_are_independent() {
        let runtime = OperationRuntime::with_read_limit(1);
        let read = runtime.try_observe().unwrap();
        assert!(runtime.try_observe().is_err());
        let writes = (0..DEFAULT_WRITE_OBSERVERS)
            .map(|_| runtime.try_observe_write().unwrap())
            .collect::<Vec<_>>();
        assert!(runtime.try_observe_write().is_err());
        assert_eq!(
            runtime.snapshot().active_write_responses,
            DEFAULT_WRITE_OBSERVERS
        );
        drop(read);
        let read = runtime.try_observe().unwrap();
        runtime.close();
        drop(writes);
        assert_eq!(runtime.snapshot().active_write_responses, 0);
        drop(read);
        assert!(runtime.wait_logical_owners().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn closed_registration_race_never_executes_a_refused_operation() {
        let runtime = OperationRuntime::new();
        let gate = Arc::new(tokio::sync::Barrier::new(33));
        let effects = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let runtime = runtime.clone();
            let gate = Arc::clone(&gate);
            let effects = Arc::clone(&effects);
            tasks.push(tokio::spawn(async move {
                gate.wait().await;
                match runtime.submit((), async move {
                    effects.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ApiError>(()).into()
                }) {
                    Ok(response) => {
                        response.result().await.unwrap();
                        true
                    }
                    Err(_) => false,
                }
            }));
        }
        gate.wait().await;
        runtime.close();
        let mut accepted = 0;
        for task in tasks {
            accepted += usize::from(task.await.unwrap());
        }
        assert!(runtime.wait_logical_owners().await);
        assert_eq!(effects.load(Ordering::SeqCst), accepted);
        assert!(
            runtime
                .submit((), async { Ok::<_, ApiError>(()).into() })
                .is_err()
        );
    }
}
