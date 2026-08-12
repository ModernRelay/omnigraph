use std::io::ErrorKind;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures::StreamExt;
use omnigraph::db::Omnigraph;
use omnigraph::error::{ManifestErrorKind, OmniError};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep_until;
use tracing::{error, info, warn};

use omnigraph::error::StorageFailureKind;

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

/// Map an engine failure onto the supervisor's retry decision.
///
/// The storage arm is the load-bearing one: the engine classifies Lance and
/// object-store failures at the substrate boundary, so a transport condition
/// arrives here already typed. Before that, every such failure was an opaque
/// string that landed in `Unknown` — which meant a transient S3 error pinned a
/// graph permanently and the retry ladder below was unreachable code.
///
/// Nothing here inspects error text. This drives a wire-visible field, and a
/// wire-visible field must not depend on internal error prose.
fn classify_engine_error(error: OmniError) -> GraphFailure {
    if let Some(failure) = error.storage_failure() {
        let class = match failure.kind {
            StorageFailureKind::Transient => FailureClass::TransientStorage,
            StorageFailureKind::Configuration | StorageFailureKind::NotFound => {
                FailureClass::InvalidConfiguration
            }
            StorageFailureKind::Permanent => FailureClass::InvariantViolation,
        };
        return GraphFailure::new(class, failure.message.clone());
    }
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

/// Open one configured graph.
///
/// The id comes from the entry's key rather than being re-parsed: an entry only
/// exists because `GraphEntry::from_config` already parsed it, so re-deriving it
/// here would be a second source of truth with an unreachable error arm.
async fn open_graph(
    graph_id: &crate::GraphId,
    config: &GraphStartupConfig,
) -> Result<Arc<GraphHandle>, GraphFailure> {
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
        key: crate::GraphKey::cluster(graph_id.clone()),
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
    match open_graph(&entry.key.graph_id, &config).await {
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

/// One recovery attempt for a serving graph.
///
/// Two properties this must preserve:
///
/// **A serving graph never stops serving reads.** Recovery is a *write*
/// concern. A failed `refresh()` leaves the engine's live coordinator and
/// schema view exactly as they were, and the destructive actions recovery can
/// take (`Dataset::restore`, prefix deletion) either append a version or are
/// guarded to objects absent from the live manifest — so reads pinned to a
/// version stay correct. Dropping the handle would trade a write outage for a
/// total one, permanently, with nothing able to bring it back. A permanent
/// failure is therefore `Blocked { retry_at: None }`, not `Unavailable`.
///
/// **A conclusion drawn before a newer request must not overwrite it.** The
/// generation is captured after the permit is acquired — as late as possible —
/// and re-checked under the entry lock before storing. If it moved, a newer
/// trigger arrived during the refresh and its state stands.
async fn attempt_recovery(
    entry: &Arc<GraphEntry>,
    semaphore: &Arc<Semaphore>,
) -> Option<GraphFailure> {
    let handle = match entry.runtime().as_ref() {
        GraphRuntimeState::Serving {
            handle,
            writes: WriteState::Recovering { .. } | WriteState::Blocked { .. },
        } => Arc::clone(handle),
        _ => return None,
    };
    let _permit = semaphore
        .acquire()
        .await
        .expect("supervisor semaphore closed");
    // Capture after the permit: the queue wait can be long, and anything that
    // happened during it belongs to a later attempt, not this one.
    let generation = entry.recovery_generation();
    let (attempts, blocking_operation_id) = match entry.runtime().as_ref() {
        GraphRuntimeState::Serving {
            writes:
                WriteState::Recovering {
                    attempts,
                    blocking_operation_id,
                    ..
                }
                | WriteState::Blocked {
                    attempts,
                    blocking_operation_id,
                    ..
                },
            ..
        } => (*attempts + 1, blocking_operation_id.clone()),
        _ => return None,
    };
    #[cfg(test)]
    attempt_probe::hold_if_enabled().await;
    let outcome = handle.engine.refresh().await;

    let _guard = entry.mutation.lock().await;
    if entry.recovery_generation() != generation {
        // A newer trigger landed while this attempt ran. Whatever it concluded
        // describes a superseded request; leave the newer state to be scanned.
        entry.wake();
        return outcome.err().map(classify_engine_error);
    }
    let GraphRuntimeState::Serving { .. } = entry.runtime().as_ref() else {
        return outcome.err().map(classify_engine_error);
    };
    match outcome {
        Ok(()) => {
            entry.store_runtime(GraphRuntimeState::Serving {
                handle,
                writes: WriteState::Ready,
            });
            info!(graph_id = %entry.key.graph_id, attempts, "graph recovery completed");
            None
        }
        Err(error) => {
            let failure = classify_engine_error(error);
            let retry_at = failure
                .class
                .retryable()
                .then(|| Instant::now() + full_jitter_backoff(attempts));
            entry.store_runtime(GraphRuntimeState::Serving {
                handle,
                writes: WriteState::Blocked {
                    generation,
                    attempts,
                    retry_at,
                    failure_class: failure.class,
                    blocking_operation_id,
                },
            });
            match retry_at {
                Some(at) => warn!(
                    graph_id = %entry.key.graph_id,
                    attempts,
                    retry_after_ms = at.saturating_duration_since(Instant::now()).as_millis(),
                    error = %failure.message,
                    "graph recovery failed transiently; reads remain available"
                ),
                None => error!(
                    graph_id = %entry.key.graph_id,
                    failure_class = failure.class.as_str(),
                    error = %failure.message,
                    "graph recovery failed permanently; writes blocked, reads remain available"
                ),
            }
            Some(failure)
        }
    }
}

/// The one attempt-permit pool for this process.
///
/// Open and recovery attempts both perform unbounded-latency graph I/O, so the
/// bound has to span every supervisor set and the boot sweep — otherwise two
/// `AppState`s in one process quietly get twice the concurrency, and the test
/// asserting a process-wide bound is asserting a property the code lacks.
fn attempt_permits() -> &'static Arc<Semaphore> {
    static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    PERMITS.get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_ATTEMPTS)))
}

pub(crate) async fn initial_open_all(registry: Arc<GraphRegistry>) -> Vec<(String, GraphFailure)> {
    let semaphore = Arc::clone(attempt_permits());
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
        let semaphore = Arc::clone(attempt_permits());
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
                writes: WriteState::Blocked { retry_at, .. },
                ..
            } => *retry_at,
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
                writes: WriteState::Blocked { retry_at, .. },
                ..
            } if retry_at.is_some_and(|at| at <= Instant::now()) => {
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

        // The case that used to brick a graph. The engine classifies transport
        // failures at the substrate boundary (pinned there against real Lance
        // and object-store errors); this pins that the supervisor keeps that
        // verdict, so the retry ladder is reachable rather than dead code.
        for (kind, expected, retryable) in [
            (
                StorageFailureKind::Transient,
                FailureClass::TransientStorage,
                true,
            ),
            (
                StorageFailureKind::Configuration,
                FailureClass::InvalidConfiguration,
                false,
            ),
            (
                StorageFailureKind::NotFound,
                FailureClass::InvalidConfiguration,
                false,
            ),
            (
                StorageFailureKind::Permanent,
                FailureClass::InvariantViolation,
                false,
            ),
        ] {
            let classified = classify_engine_error(OmniError::Storage(
                omnigraph::error::StorageFailure::new(kind, "storage said so"),
            ));
            assert_eq!(classified.class, expected, "{kind:?}");
            assert_eq!(classified.class.retryable(), retryable, "{kind:?}");
        }

        // An engine error carrying no storage classification is still unknown,
        // and unknown is still not retried.
        let unknown = classify_engine_error(OmniError::Policy("denied".into()));
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

    /// A conclusion drawn before a newer request must not overwrite it.
    ///
    /// The supervisor reads state, refreshes without holding the entry lock,
    /// then writes a result derived from that read. A `mark_recovering` landing
    /// in that window used to be clobbered: the success path re-checked only
    /// that the graph was still `Serving`, which is true for every write state,
    /// so a brand-new recovery request was silently marked `Ready` and dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(supervisor_attempts)]
    async fn a_trigger_arriving_mid_attempt_is_not_clobbered() {
        let (_dir, handle, registry) = serving_registry().await;
        registry
            .mark_recovering(&handle.key, Some("operation-first".into()))
            .await;
        let entry = registry.entry(&handle.key).unwrap();

        attempt_probe::begin_hold();
        let attempt = {
            let entry = Arc::clone(&entry);
            let permits = Arc::clone(attempt_permits());
            tokio::spawn(async move { attempt_recovery(&entry, &permits).await })
        };
        // Wait until the attempt is parked inside the refresh seam.
        tokio::time::timeout(Duration::from_secs(10), async {
            while attempt_probe::maximum() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery attempt never reached the probe seam");

        // A second failure arrives while that attempt is still running.
        registry
            .mark_recovering(&handle.key, Some("operation-second".into()))
            .await;
        attempt_probe::release();
        attempt.await.unwrap();

        let availability = entry.availability();
        assert_eq!(
            availability.state,
            crate::registry::GraphState::Recovering,
            "the newer request must survive the in-flight attempt's conclusion"
        );
        assert!(!availability.write_ready);
        assert_eq!(
            availability.blocking_operation_id.as_deref(),
            Some("operation-first"),
            "the original blocking operation stays the diagnostic"
        );
    }

    /// Coalescing must not restart the backoff ladder.
    ///
    /// Every 503'd write against a broken graph calls `mark_recovering`. If that
    /// reset the attempt count, sustained traffic would hold the supervisor at
    /// its shortest delay forever — and each attempt is a full recovery sweep
    /// whose cost grows with commit depth.
    #[tokio::test]
    async fn coalescing_preserves_the_backoff_ladder() {
        let (_dir, handle, registry) = serving_registry().await;
        let entry = registry.entry(&handle.key).unwrap();
        let retry_at = Instant::now() + Duration::from_secs(45);
        entry.store_runtime(GraphRuntimeState::Serving {
            handle: entry.serving_handle().unwrap(),
            writes: WriteState::Blocked {
                generation: entry.recovery_generation(),
                attempts: 6,
                retry_at: Some(retry_at),
                failure_class: FailureClass::TransientStorage,
                blocking_operation_id: Some("operation-first".into()),
            },
        });

        registry
            .mark_recovering(&handle.key, Some("operation-second".into()))
            .await;

        match entry.runtime().as_ref() {
            GraphRuntimeState::Serving {
                writes:
                    WriteState::Blocked {
                        attempts,
                        retry_at: scheduled,
                        ..
                    },
                ..
            } => {
                assert_eq!(*attempts, 6, "a new trigger must not reset the ladder");
                assert_eq!(
                    *scheduled,
                    Some(retry_at),
                    "a new trigger must not pull the deadline earlier"
                );
            }
            _ => panic!("a coalesced trigger must keep the scheduled backoff"),
        }
    }

    /// A permanent recovery failure blocks writes and keeps serving reads.
    ///
    /// Recovery is a write concern: a failed `refresh()` leaves the engine's
    /// live view exactly as it was, so reads pinned to a version stay correct.
    /// Dropping the handle would trade a write outage for a total one,
    /// permanently, with nothing able to bring it back.
    #[tokio::test]
    async fn a_permanent_recovery_failure_keeps_reads_available() {
        let (_dir, handle, registry) = serving_registry().await;
        let entry = registry.entry(&handle.key).unwrap();
        entry.store_runtime(GraphRuntimeState::Serving {
            handle: entry.serving_handle().unwrap(),
            writes: WriteState::Blocked {
                generation: entry.recovery_generation(),
                attempts: 3,
                retry_at: None,
                failure_class: FailureClass::InvariantViolation,
                blocking_operation_id: None,
            },
        });

        let availability = entry.availability();
        assert_eq!(availability.state, crate::registry::GraphState::Degraded);
        assert!(availability.read_ready, "reads must survive");
        assert!(!availability.write_ready);
        assert_eq!(
            availability.retry_after_seconds, None,
            "advertise Retry-After only when a retry is actually scheduled"
        );
        assert!(matches!(
            registry.get(&handle.key),
            crate::RegistryLookup::Ready(_)
        ));
    }
}
