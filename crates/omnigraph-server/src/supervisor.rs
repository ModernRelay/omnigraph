use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use omnigraph::db::Omnigraph;
use omnigraph::error::{ManifestErrorKind, OmniError};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep_until;
use tracing::{error, info, warn};

use crate::registry::{
    FailureClass, GraphEntry, GraphHandle, GraphRegistry, GraphRuntimeState, WriteState,
};
use crate::{GraphStartupConfig, load_graph_policy, validate_and_attach};

const MAX_CONCURRENT_ATTEMPTS: usize = 4;
const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[cfg(test)]
mod attempt_probe {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    static HOLD: AtomicBool = AtomicBool::new(false);
    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    static MAXIMUM: AtomicUsize = AtomicUsize::new(0);

    struct ActiveAttempt;

    impl Drop for ActiveAttempt {
        fn drop(&mut self) {
            ACTIVE.fetch_sub(1, Ordering::SeqCst);
        }
    }

    pub fn begin_hold() {
        ACTIVE.store(0, Ordering::SeqCst);
        MAXIMUM.store(0, Ordering::SeqCst);
        HOLD.store(true, Ordering::Release);
    }

    pub fn maximum() -> usize {
        MAXIMUM.load(Ordering::SeqCst)
    }

    pub fn release() {
        HOLD.store(false, Ordering::Release);
    }

    pub async fn hold_if_enabled() {
        if !HOLD.load(Ordering::Acquire) {
            return;
        }
        let active = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
        MAXIMUM.fetch_max(active, Ordering::SeqCst);
        let _active = ActiveAttempt;
        while HOLD.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GraphFailure {
    pub class: FailureClass,
    pub message: String,
}

impl GraphFailure {
    fn new(class: FailureClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: sanitize_error(message.into()),
        }
    }
}

fn sanitize_error(message: String) -> String {
    let compact = message.split_whitespace().collect::<Vec<_>>().join(" ");
    compact.chars().take(512).collect()
}

fn classify_engine_error(error: OmniError) -> GraphFailure {
    match error {
        OmniError::Io(error) => {
            let class = match error.kind() {
                ErrorKind::TimedOut => FailureClass::Timeout,
                ErrorKind::InvalidInput
                | ErrorKind::InvalidData
                | ErrorKind::Unsupported
                | ErrorKind::PermissionDenied => FailureClass::InvalidConfiguration,
                _ => FailureClass::Io,
            };
            GraphFailure::new(class, error.to_string())
        }
        OmniError::Manifest(error)
            if error.message.contains("internal schema v")
                && (error.message.contains("upgrade omnigraph")
                    || error.message.contains("reads only")) =>
        {
            GraphFailure::new(FailureClass::UnsupportedFormat, error.message)
        }
        OmniError::Manifest(error) => {
            let class = match error.kind {
                ManifestErrorKind::BadRequest | ManifestErrorKind::NotFound => {
                    FailureClass::InvalidConfiguration
                }
                ManifestErrorKind::Conflict | ManifestErrorKind::Internal => {
                    FailureClass::InvariantViolation
                }
            };
            GraphFailure::new(class, error.message)
        }
        OmniError::Compiler(error) => {
            GraphFailure::new(FailureClass::InvalidConfiguration, error.to_string())
        }
        error => GraphFailure::new(FailureClass::Unknown, error.to_string()),
    }
}

fn full_jitter_backoff(attempts: u32) -> Duration {
    let shift = attempts.saturating_sub(1).min(6);
    let ceiling = Duration::from_secs(1u64 << shift).min(MAX_BACKOFF);
    let ceiling_millis = ceiling.as_millis() as u64;
    Duration::from_millis(fastrand::u64(1..=ceiling_millis.max(1)))
}

async fn open_graph(config: &GraphStartupConfig) -> Result<Arc<GraphHandle>, GraphFailure> {
    let graph_id = crate::GraphId::try_from(config.graph_id.clone()).map_err(|error| {
        GraphFailure::new(FailureClass::InvalidConfiguration, error.to_string())
    })?;
    let db = Omnigraph::open(&config.uri)
        .await
        .map_err(classify_engine_error)?;
    let db = if let Some(embedding) = &config.embedding {
        db.with_embedding_config(Arc::new(embedding.clone()))
    } else {
        db
    };
    let queries = validate_and_attach(config.queries.clone(), &db.catalog(), graph_id.as_str())
        .map_err(|error| {
            GraphFailure::new(FailureClass::InvalidConfiguration, error.to_string())
        })?;
    let (policy, db) = match &config.policy {
        Some(source) => {
            let policy = load_graph_policy(source, graph_id.as_str()).map_err(|error| {
                GraphFailure::new(FailureClass::InvalidConfiguration, error.to_string())
            })?;
            let policy = Arc::new(policy);
            let checker = Arc::clone(&policy) as Arc<dyn omnigraph_policy::PolicyChecker>;
            (Some(policy), db.with_policy(checker))
        }
        None => (None, db),
    };
    Ok(Arc::new(GraphHandle {
        key: crate::GraphKey::cluster(graph_id),
        uri: config.uri.clone(),
        engine: Arc::new(db),
        policy,
        queries,
    }))
}

async fn attempt_open(entry: &Arc<GraphEntry>, semaphore: &Arc<Semaphore>) -> Option<GraphFailure> {
    let config = entry.startup_config()?;
    let attempts = match entry.runtime().as_ref() {
        GraphRuntimeState::Opening { attempts, .. } => *attempts + 1,
        _ => return None,
    };
    let _permit = semaphore
        .acquire()
        .await
        .expect("supervisor semaphore closed");
    #[cfg(test)]
    attempt_probe::hold_if_enabled().await;
    match open_graph(&config).await {
        Ok(handle) => {
            let _guard = entry.mutation.lock().await;
            if matches!(entry.runtime().as_ref(), GraphRuntimeState::Opening { .. }) {
                entry.store_runtime(GraphRuntimeState::Serving {
                    handle,
                    writes: WriteState::Ready,
                });
                info!(graph_id = %entry.key.graph_id, attempts, "graph opened");
            }
            None
        }
        Err(failure) => {
            let _guard = entry.mutation.lock().await;
            if failure.class.retryable() {
                let delay = full_jitter_backoff(attempts);
                entry.store_runtime(GraphRuntimeState::Opening {
                    attempts,
                    next_retry: Instant::now() + delay,
                    failure_class: Some(failure.class),
                    last_error: Some(failure.message.clone()),
                });
                warn!(
                    graph_id = %entry.key.graph_id,
                    attempts,
                    retry_after_ms = delay.as_millis(),
                    error = %failure.message,
                    "graph open failed transiently"
                );
            } else {
                entry.store_runtime(GraphRuntimeState::Unavailable {
                    failure_class: failure.class,
                    last_error: failure.message.clone(),
                });
                error!(
                    graph_id = %entry.key.graph_id,
                    failure_class = failure.class.as_str(),
                    error = %failure.message,
                    "graph open failed permanently"
                );
            }
            Some(failure)
        }
    }
}

async fn attempt_recovery(
    entry: &Arc<GraphEntry>,
    semaphore: &Arc<Semaphore>,
) -> Option<GraphFailure> {
    let (handle, attempts, blocking_operation_id) = match entry.runtime().as_ref() {
        GraphRuntimeState::Serving {
            handle,
            writes:
                WriteState::Recovering {
                    attempts,
                    blocking_operation_id,
                }
                | WriteState::Blocked {
                    attempts,
                    blocking_operation_id,
                    ..
                },
        } => (
            Arc::clone(handle),
            *attempts + 1,
            blocking_operation_id.clone(),
        ),
        _ => return None,
    };
    let _permit = semaphore
        .acquire()
        .await
        .expect("supervisor semaphore closed");
    match handle.engine.refresh().await {
        Ok(()) => {
            let _guard = entry.mutation.lock().await;
            if matches!(entry.runtime().as_ref(), GraphRuntimeState::Serving { .. }) {
                entry.store_runtime(GraphRuntimeState::Serving {
                    handle,
                    writes: WriteState::Ready,
                });
                info!(graph_id = %entry.key.graph_id, attempts, "graph recovery completed");
            }
            None
        }
        Err(error) => {
            let failure = classify_engine_error(error);
            let _guard = entry.mutation.lock().await;
            if failure.class.retryable() {
                let delay = full_jitter_backoff(attempts);
                entry.store_runtime(GraphRuntimeState::Serving {
                    handle,
                    writes: WriteState::Blocked {
                        attempts,
                        next_retry: Instant::now() + delay,
                        failure_class: failure.class,
                        last_error: failure.message.clone(),
                        blocking_operation_id,
                    },
                });
                warn!(
                    graph_id = %entry.key.graph_id,
                    attempts,
                    retry_after_ms = delay.as_millis(),
                    error = %failure.message,
                    "graph recovery failed transiently; reads remain available"
                );
            } else {
                entry.store_runtime(GraphRuntimeState::Unavailable {
                    failure_class: failure.class,
                    last_error: failure.message.clone(),
                });
                error!(
                    graph_id = %entry.key.graph_id,
                    failure_class = failure.class.as_str(),
                    error = %failure.message,
                    "graph recovery failed permanently; routing disabled"
                );
            }
            Some(failure)
        }
    }
}

pub(crate) async fn initial_open_all(registry: Arc<GraphRegistry>) -> Vec<(String, GraphFailure)> {
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_ATTEMPTS));
    futures::stream::iter(registry.list())
        .map(|entry| {
            let semaphore = Arc::clone(&semaphore);
            async move {
                let graph_id = entry.key.graph_id.as_str().to_string();
                attempt_open(&entry, &semaphore)
                    .await
                    .map(|failure| (graph_id, failure))
            }
        })
        .buffer_unordered(MAX_CONCURRENT_ATTEMPTS)
        .filter_map(futures::future::ready)
        .collect()
        .await
}

pub(crate) struct SupervisorSet {
    shutdown: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl SupervisorSet {
    pub fn idle() -> Arc<Self> {
        let (shutdown, _) = watch::channel(false);
        Arc::new(Self {
            shutdown,
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub fn start(registry: Arc<GraphRegistry>) -> Arc<Self> {
        let (shutdown, _) = watch::channel(false);
        let set = Arc::new(Self {
            shutdown,
            tasks: Mutex::new(Vec::new()),
        });
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_ATTEMPTS));
        let mut tasks = Vec::with_capacity(registry.len());
        for entry in registry.list() {
            tasks.push(tokio::spawn(supervise_graph(
                entry,
                Arc::clone(&semaphore),
                set.shutdown.subscribe(),
            )));
        }
        *set.tasks.lock().expect("supervisor task mutex poisoned") = tasks;
        set
    }

    #[allow(dead_code)]
    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let tasks =
            std::mem::take(&mut *self.tasks.lock().expect("supervisor task mutex poisoned"));
        for task in tasks {
            if let Err(error) = task.await {
                warn!(error = %error, "graph supervisor task did not exit cleanly");
            }
        }
    }
}

async fn supervise_graph(
    entry: Arc<GraphEntry>,
    semaphore: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let deadline = match entry.runtime().as_ref() {
            GraphRuntimeState::Opening { next_retry, .. } => Some(*next_retry),
            GraphRuntimeState::Serving {
                writes: WriteState::Recovering { .. },
                ..
            } => Some(Instant::now()),
            GraphRuntimeState::Serving {
                writes: WriteState::Blocked { next_retry, .. },
                ..
            } => Some(*next_retry),
            GraphRuntimeState::Serving {
                writes: WriteState::Ready,
                ..
            }
            | GraphRuntimeState::Unavailable { .. } => None,
        };

        match deadline {
            Some(deadline) if deadline <= Instant::now() => {}
            Some(deadline) => {
                tokio::select! {
                    _ = sleep_until(deadline.into()) => {}
                    _ = entry.notify.notified() => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return; }
                    }
                }
            }
            None => {
                tokio::select! {
                    _ = entry.notify.notified() => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return; }
                    }
                }
            }
        }

        match entry.runtime().as_ref() {
            GraphRuntimeState::Opening { next_retry, .. } if *next_retry <= Instant::now() => {
                attempt_open(&entry, &semaphore).await;
            }
            GraphRuntimeState::Serving {
                writes: WriteState::Recovering { .. },
                ..
            } => {
                attempt_recovery(&entry, &semaphore).await;
            }
            GraphRuntimeState::Serving {
                writes: WriteState::Blocked { next_retry, .. },
                ..
            } if *next_retry <= Instant::now() => {
                attempt_recovery(&entry, &semaphore).await;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::registry::GraphHandle;
    use crate::{GraphId, GraphKey};

    async fn serving_registry() -> (TempDir, Arc<GraphHandle>, Arc<GraphRegistry>) {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().join("graph").to_string_lossy().to_string();
        let engine = Omnigraph::init(&uri, "node Person { name: String @key }\n")
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
        (dir, handle, registry)
    }

    #[test]
    fn full_jitter_backoff_starts_bounded_and_caps_at_sixty_seconds() {
        for attempts in 1..=20 {
            let delay = full_jitter_backoff(attempts);
            assert!(delay > Duration::ZERO);
            assert!(delay <= MAX_BACKOFF);
            let ceiling = Duration::from_secs(1u64 << attempts.saturating_sub(1).min(6));
            assert!(delay <= ceiling.min(MAX_BACKOFF));
        }
    }

    #[test]
    fn error_classification_retries_only_proven_io_and_timeout_failures() {
        let timed_out = classify_engine_error(OmniError::Io(std::io::Error::new(
            ErrorKind::TimedOut,
            "deadline",
        )));
        assert_eq!(timed_out.class, FailureClass::Timeout);
        assert!(timed_out.class.retryable());

        let io = classify_engine_error(OmniError::Io(std::io::Error::new(
            ErrorKind::ConnectionReset,
            "reset",
        )));
        assert_eq!(io.class, FailureClass::Io);
        assert!(io.class.retryable());

        let invalid = classify_engine_error(OmniError::Io(std::io::Error::new(
            ErrorKind::InvalidInput,
            "bad uri",
        )));
        assert_eq!(invalid.class, FailureClass::InvalidConfiguration);
        assert!(!invalid.class.retryable());

        let invariant = classify_engine_error(OmniError::manifest_internal("broken invariant"));
        assert_eq!(invariant.class, FailureClass::InvariantViolation);
        assert!(!invariant.class.retryable());

        let unknown = classify_engine_error(OmniError::Lance("opaque storage error".into()));
        assert_eq!(unknown.class, FailureClass::Unknown);
        assert!(!unknown.class.retryable());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repeated_notifications_coalesce_into_live_handle_refresh() {
        let (_dir, handle, registry) = serving_registry().await;
        for index in 0..32 {
            registry
                .mark_recovering(&handle.key, Some(format!("operation-{index}")))
                .await;
        }
        let supervisors = SupervisorSet::start(Arc::clone(&registry));

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if registry
                    .write_availability(&handle.key)
                    .is_some_and(|availability| availability.write_ready)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("coalesced supervisor wake must complete recovery");
        let routed = match registry.get(&handle.key) {
            crate::RegistryLookup::Ready(routed) => routed,
            crate::RegistryLookup::Unavailable(_) | crate::RegistryLookup::Gone => {
                panic!("same live handle must return to service")
            }
        };
        assert!(
            Arc::ptr_eq(&routed, &handle),
            "recovery must not reopen/swap"
        );
        supervisors.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial_test::serial(supervisor_attempts)]
    async fn initial_open_attempts_are_process_wide_bounded_to_four() {
        let temp = TempDir::new().unwrap();
        let mut configs = Vec::new();
        for index in 0..5 {
            let uri = temp
                .path()
                .join(format!("graph-{index}"))
                .to_string_lossy()
                .to_string();
            Omnigraph::init(&uri, "node Person { name: String @key }\n")
                .await
                .unwrap();
            configs.push(GraphStartupConfig {
                graph_id: format!("graph-{index}"),
                uri,
                policy: None,
                startup_error: None,
                embedding: None,
                queries: crate::QueryRegistry::default(),
            });
        }
        let registry = Arc::new(GraphRegistry::from_configs(configs).unwrap());
        attempt_probe::begin_hold();
        let opener = {
            let registry = Arc::clone(&registry);
            tokio::spawn(async move { initial_open_all(registry).await })
        };
        let reached_four = tokio::time::timeout(Duration::from_secs(10), async {
            while attempt_probe::maximum() < MAX_CONCURRENT_ATTEMPTS {
                tokio::task::yield_now().await;
            }
        })
        .await;
        attempt_probe::release();
        reached_four.expect("four parallel attempts never entered the open seam");
        assert_eq!(attempt_probe::maximum(), 4);
        assert!(opener.await.unwrap().is_empty());
        assert!(
            registry
                .list()
                .iter()
                .all(|entry| entry.availability().write_ready)
        );
    }
}
