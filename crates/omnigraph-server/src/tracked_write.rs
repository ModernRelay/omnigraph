use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::FutureExt;
use omnigraph::error::OmniError;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use tracing::{error, warn};

use crate::ApiError;
use crate::registry::{GraphHandle, GraphRegistry};
use crate::workload::AdmissionGuard;

/// Owns served write futures beyond the lifetime of their HTTP response
/// futures. Dropping a response receiver never cancels the engine operation,
/// its admission reservation, or its recovery notification.
pub(crate) struct TrackedWriteExecutor {
    registry: Arc<GraphRegistry>,
    tracker: TaskTracker,
    accepting: AtomicBool,
    admission_gate: std::sync::Mutex<()>,
}

impl TrackedWriteExecutor {
    pub fn new(registry: Arc<GraphRegistry>) -> Arc<Self> {
        Arc::new(Self {
            registry,
            tracker: TaskTracker::new(),
            accepting: AtomicBool::new(true),
            admission_gate: std::sync::Mutex::new(()),
        })
    }

    pub async fn execute<T, F, Fut>(
        &self,
        handle: Arc<GraphHandle>,
        admission: AdmissionGuard,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<GraphHandle>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, OmniError>> + Send + 'static,
    {
        let availability = self
            .registry
            .write_availability(&handle.key)
            .ok_or_else(|| ApiError::not_found(format!("graph '{}' not found", handle.key)))?;
        if !availability.write_ready {
            return Err(ApiError::graph_unavailable(
                &handle.key.graph_id,
                &availability,
            ));
        }

        let (sender, receiver) = oneshot::channel();
        {
            let _gate = self
                .admission_gate
                .lock()
                .expect("write executor admission mutex poisoned");
            if !self.accepting.load(Ordering::Acquire) {
                return Err(ApiError::service_unavailable(
                    "server is shutting down; write admission is closed",
                ));
            }
            let registry = Arc::clone(&self.registry);
            self.tracker.spawn(async move {
                let _admission = admission;
                let graph_key = handle.key.clone();
                let outcome = AssertUnwindSafe(async move { operation(handle).await })
                    .catch_unwind()
                    .await;
                let response = match outcome {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error @ OmniError::RecoveryRequired { .. })) => {
                        let operation_id = match &error {
                            OmniError::RecoveryRequired { operation_id, .. } => {
                                Some(operation_id.clone())
                            }
                            _ => None,
                        };
                        registry.mark_recovering(&graph_key, operation_id).await;
                        Err(ApiError::from_omni(error))
                    }
                    Ok(Err(error)) => Err(ApiError::from_omni(error)),
                    Err(_) => {
                        error!(graph_id = %graph_key.graph_id, "tracked write task panicked");
                        registry.mark_recovering(&graph_key, None).await;
                        Err(ApiError::internal("tracked write task panicked"))
                    }
                };
                // A disconnected caller drops only this receiver. The owned
                // operation has already reached a terminal result.
                let _ = sender.send(response);
            });
        }

        receiver.await.unwrap_or_else(|_| {
            Err(ApiError::internal(
                "tracked write task exited without a result",
            ))
        })
    }

    /// Atomically prevent any later task submission. Existing owned tasks keep
    /// their admission guards and continue to a terminal engine result.
    pub fn close_admission(&self) {
        let _gate = self
            .admission_gate
            .lock()
            .expect("write executor admission mutex poisoned");
        self.accepting.store(false, Ordering::Release);
        self.tracker.close();
    }

    pub async fn drain(&self, timeout: Duration) {
        if tokio::time::timeout(timeout, self.tracker.wait())
            .await
            .is_err()
        {
            warn!(
                timeout_seconds = timeout.as_secs(),
                "timed out draining tracked writes; durable sidecars remain next-boot recovery authority"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use tempfile::TempDir;
    use tokio::sync::Notify;

    use super::*;
    use crate::registry::{GraphHandle, GraphRegistry};
    use crate::{GraphId, GraphKey};

    async fn fixture() -> (
        TempDir,
        Arc<GraphHandle>,
        Arc<GraphRegistry>,
        Arc<TrackedWriteExecutor>,
    ) {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().join("graph").to_string_lossy().to_string();
        let engine = omnigraph::db::Omnigraph::init(&uri, "node Person { name: String @key }\n")
            .await
            .unwrap();
        let handle = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("test").unwrap()),
            uri,
            engine: Arc::new(engine),
            policy: None,
            queries: None,
        });
        let registry = Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&handle)]).unwrap());
        let executor = TrackedWriteExecutor::new(Arc::clone(&registry));
        (dir, handle, registry, executor)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_response_waiter_does_not_cancel_owned_write_or_release_admission() {
        let (_dir, handle, registry, executor) = fixture().await;
        let workload = Arc::new(crate::workload::WorkloadController::new(1, 1024));
        let actor: Arc<str> = "actor".into();
        let admission = workload.try_admit(&actor, 1).unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let terminal = Arc::new(AtomicBool::new(false));
        let executions = Arc::new(AtomicUsize::new(0));

        let waiter = {
            let executor = Arc::clone(&executor);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let terminal = Arc::clone(&terminal);
            let executions = Arc::clone(&executions);
            tokio::spawn(async move {
                executor
                    .execute(handle, admission, move |_| async move {
                        executions.fetch_add(1, Ordering::SeqCst);
                        started.notify_one();
                        release.notified().await;
                        terminal.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            })
        };
        started.notified().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(
            workload.try_admit(&actor, 1).is_err(),
            "the detached task must retain the admission guard"
        );

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !terminal.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned write must reach a terminal state");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(guard) = workload.try_admit(&actor, 1) {
                    drop(guard);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("admission must release after terminal execution");
        assert_eq!(executions.load(Ordering::SeqCst), 1, "no request replay");
        assert!(
            registry
                .write_availability(&GraphKey::cluster(GraphId::try_from("test").unwrap()))
                .unwrap()
                .write_ready
        );
    }

    #[tokio::test]
    async fn recovery_required_and_panics_schedule_conservative_recovery() {
        let (_dir, handle, registry, executor) = fixture().await;
        let workload = crate::workload::WorkloadController::new(2, 1024);
        let actor: Arc<str> = "actor".into();
        let error = executor
            .execute(
                Arc::clone(&handle),
                workload.try_admit(&actor, 1).unwrap(),
                |_| async {
                    Err::<(), _>(OmniError::RecoveryRequired {
                        operation_id: "01JPRIORBLOCKER".to_string(),
                        reason: "test residual".to_string(),
                    })
                },
            )
            .await
            .expect_err("recovery-required result must reach a connected caller");
        assert_eq!(error.status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        let availability = registry.write_availability(&handle.key).unwrap();
        assert_eq!(availability.state, "recovering");
        assert_eq!(
            availability.blocking_operation_id.as_deref(),
            Some("01JPRIORBLOCKER")
        );

        // Reset to ready so the panic path can be admitted independently.
        let entry = registry.entry(&handle.key).unwrap();
        entry.store_runtime(crate::GraphRuntimeState::Serving {
            handle: Arc::clone(&handle),
            writes: crate::WriteState::Ready,
        });
        let panic_error = executor
            .execute(
                handle,
                workload.try_admit(&actor, 1).unwrap(),
                |_| async move {
                    panic!("synthetic tracked write panic");
                    #[allow(unreachable_code)]
                    Ok::<(), OmniError>(())
                },
            )
            .await
            .expect_err("panic must be contained at the executor boundary");
        assert_eq!(
            panic_error.status,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            registry.write_availability(&entry.key).unwrap().state,
            "recovering"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_closes_admission_then_drains_existing_owned_writes() {
        let (_dir, handle, _registry, executor) = fixture().await;
        let workload = Arc::new(crate::workload::WorkloadController::new(2, 1024));
        let actor: Arc<str> = "actor".into();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let later_handle = Arc::clone(&handle);
        let waiter = {
            let executor = Arc::clone(&executor);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                executor
                    .execute(
                        handle,
                        workload.try_admit(&actor, 1).unwrap(),
                        move |_| async move {
                            started.notify_one();
                            release.notified().await;
                            Ok(())
                        },
                    )
                    .await
            })
        };
        started.notified().await;
        executor.close_admission();

        let second_workload = crate::workload::WorkloadController::new(1, 1024);
        let error = executor
            .execute(
                later_handle,
                second_workload
                    .try_admit(&Arc::<str>::from("actor"), 1)
                    .unwrap(),
                |_| async { Ok(()) },
            )
            .await
            .expect_err("closed admission must reject later submissions");
        assert_eq!(error.status, axum::http::StatusCode::SERVICE_UNAVAILABLE);

        let drain = {
            let executor = Arc::clone(&executor);
            tokio::spawn(async move { executor.drain(Duration::from_secs(2)).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!drain.is_finished(), "drain must wait for the owned write");
        release.notify_one();
        waiter.await.unwrap().unwrap();
        drain.await.unwrap();
    }
}
