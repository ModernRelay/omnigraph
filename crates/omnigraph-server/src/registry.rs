//! Configured-graph registry and atomically readable runtime state.
//!
//! The configured graph set is immutable for the lifetime of a server process;
//! runtime add/remove and config hot reload are deliberately out of scope. Each
//! entry owns canonical configuration plus a small state machine that can move
//! between opening, serving, recovery-blocked, and unavailable without
//! rebuilding the registry or replacing a live engine handle.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use omnigraph::db::Omnigraph;
use omnigraph::storage::normalize_root_uri;
use tokio::sync::{Mutex, Notify};

use crate::GraphStartupConfig;
use crate::graph_id::GraphId;
use crate::identity::GraphKey;
use crate::policy::PolicyEngine;
use crate::queries::QueryRegistry;

/// Open handle for a single graph in the registry. Cheap to clone (`Arc`-wrapped
/// engine + policy). Cluster-mode handlers extract this via
/// `Extension<Arc<GraphHandle>>` injected by the routing middleware.
pub struct GraphHandle {
    /// Registry key. In Cluster mode `key.tenant_id` is always `None`.
    pub key: GraphKey,
    /// The URI the engine was opened from (`s3://...` or local path).
    /// Stable for the engine's lifetime; surfaced in responses like
    /// `BranchCreateOutput.uri`.
    pub uri: String,
    /// Engine. Reads/writes go directly through `&self` methods on
    /// `Omnigraph` (no `RwLock` — MR-686 preserved).
    pub engine: Arc<Omnigraph>,
    /// Per-graph Cedar policy. `None` means "no policy gate on engine-layer
    /// `_as` writers"; the HTTP-layer `require_bearer_auth` middleware still
    /// runs regardless.
    pub policy: Option<Arc<PolicyEngine>>,
    /// Per-graph stored-query registry, loaded and validated at
    /// startup. `None` means the operator declared no stored queries for
    /// this graph — `POST /queries/{name}` then 404s. Mirrors the
    /// optional `policy` shape.
    pub queries: Option<Arc<QueryRegistry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Io,
    Timeout,
    InvalidConfiguration,
    UnsupportedFormat,
    InvariantViolation,
    Unknown,
}

impl FailureClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Io => "io",
            Self::Timeout => "timeout",
            Self::InvalidConfiguration => "invalid_configuration",
            Self::UnsupportedFormat => "unsupported_format",
            Self::InvariantViolation => "invariant_violation",
            Self::Unknown => "unknown",
        }
    }

    pub fn retryable(self) -> bool {
        matches!(self, Self::Io | Self::Timeout)
    }
}

#[derive(Clone)]
pub enum WriteState {
    Ready,
    Recovering {
        attempts: u32,
        blocking_operation_id: Option<String>,
    },
    Blocked {
        attempts: u32,
        next_retry: Instant,
        failure_class: FailureClass,
        last_error: String,
        blocking_operation_id: Option<String>,
    },
}

#[derive(Clone)]
pub enum GraphRuntimeState {
    Serving {
        handle: Arc<GraphHandle>,
        writes: WriteState,
    },
    Opening {
        attempts: u32,
        next_retry: Instant,
        failure_class: Option<FailureClass>,
        last_error: Option<String>,
    },
    Unavailable {
        failure_class: FailureClass,
        last_error: String,
    },
}

/// Request- and wire-facing projection of a configured graph's state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphAvailability {
    pub state: &'static str,
    pub read_ready: bool,
    pub write_ready: bool,
    pub failure_class: Option<&'static str>,
    pub last_error: Option<String>,
    pub retry_after_seconds: Option<u64>,
    pub blocking_operation_id: Option<String>,
}

fn retry_after_seconds(deadline: Instant) -> u64 {
    let remaining = deadline.saturating_duration_since(Instant::now());
    remaining
        .as_secs()
        .saturating_add(u64::from(remaining.subsec_nanos() > 0))
        .max(1)
}

fn sanitize_status_error(message: &str) -> String {
    message
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(512)
        .collect()
}

impl GraphRuntimeState {
    pub fn availability(&self) -> GraphAvailability {
        match self {
            Self::Serving {
                writes: WriteState::Ready,
                ..
            } => GraphAvailability {
                state: "ready",
                read_ready: true,
                write_ready: true,
                failure_class: None,
                last_error: None,
                retry_after_seconds: None,
                blocking_operation_id: None,
            },
            Self::Serving {
                writes:
                    WriteState::Recovering {
                        blocking_operation_id,
                        ..
                    },
                ..
            } => GraphAvailability {
                state: "recovering",
                read_ready: true,
                write_ready: false,
                failure_class: None,
                last_error: None,
                retry_after_seconds: Some(1),
                blocking_operation_id: blocking_operation_id.clone(),
            },
            Self::Serving {
                writes:
                    WriteState::Blocked {
                        next_retry,
                        failure_class,
                        last_error,
                        blocking_operation_id,
                        ..
                    },
                ..
            } => GraphAvailability {
                state: "degraded",
                read_ready: true,
                write_ready: false,
                failure_class: Some(failure_class.as_str()),
                last_error: Some(last_error.clone()),
                retry_after_seconds: Some(retry_after_seconds(*next_retry)),
                blocking_operation_id: blocking_operation_id.clone(),
            },
            Self::Opening {
                next_retry,
                failure_class,
                last_error,
                ..
            } => GraphAvailability {
                state: "opening",
                read_ready: false,
                write_ready: false,
                failure_class: failure_class.map(FailureClass::as_str),
                last_error: last_error.clone(),
                retry_after_seconds: Some(retry_after_seconds(*next_retry)),
                blocking_operation_id: None,
            },
            Self::Unavailable {
                failure_class,
                last_error,
            } => GraphAvailability {
                state: "unavailable",
                read_ready: false,
                write_ready: false,
                failure_class: Some(failure_class.as_str()),
                last_error: Some(last_error.clone()),
                retry_after_seconds: None,
                blocking_operation_id: None,
            },
        }
    }
}

/// One immutable configured graph plus its atomically replaceable runtime
/// state. `mutation` serializes short state transitions only; no graph I/O is
/// performed while it is held.
pub struct GraphEntry {
    pub key: GraphKey,
    pub uri: String,
    pub policy_configured: bool,
    startup: Option<Arc<GraphStartupConfig>>,
    runtime: ArcSwap<GraphRuntimeState>,
    pub(crate) mutation: Mutex<()>,
    pub(crate) notify: Notify,
}

impl GraphEntry {
    fn from_handle(handle: Arc<GraphHandle>) -> Result<Self, InsertError> {
        let (canonical_uri, handle) = canonicalize_handle_uri(handle)?;
        Ok(Self {
            key: handle.key.clone(),
            uri: canonical_uri,
            policy_configured: handle.policy.is_some(),
            startup: None,
            runtime: ArcSwap::from_pointee(GraphRuntimeState::Serving {
                handle,
                writes: WriteState::Ready,
            }),
            mutation: Mutex::new(()),
            notify: Notify::new(),
        })
    }

    fn from_config(mut config: GraphStartupConfig) -> Result<Self, InsertError> {
        let graph_id = GraphId::try_from(config.graph_id.clone()).map_err(|err| {
            InsertError::InvalidGraphId {
                graph_id: config.graph_id.clone(),
                message: err.to_string(),
            }
        })?;
        config.uri = normalize_root_uri(&config.uri).map_err(|err| InsertError::InvalidUri {
            uri: config.uri.clone(),
            message: err.to_string(),
        })?;
        let policy_configured = config.policy.is_some();
        let runtime = match config.startup_error.as_deref() {
            Some(error) => GraphRuntimeState::Unavailable {
                failure_class: FailureClass::InvalidConfiguration,
                last_error: sanitize_status_error(error),
            },
            None => GraphRuntimeState::Opening {
                attempts: 0,
                next_retry: Instant::now(),
                failure_class: None,
                last_error: None,
            },
        };
        Ok(Self {
            key: GraphKey::cluster(graph_id),
            uri: config.uri.clone(),
            policy_configured,
            startup: Some(Arc::new(config)),
            runtime: ArcSwap::from_pointee(runtime),
            mutation: Mutex::new(()),
            notify: Notify::new(),
        })
    }

    pub fn startup_config(&self) -> Option<Arc<GraphStartupConfig>> {
        self.startup.clone()
    }

    pub fn runtime(&self) -> arc_swap::Guard<Arc<GraphRuntimeState>> {
        self.runtime.load()
    }

    pub(crate) fn store_runtime(&self, state: GraphRuntimeState) {
        self.runtime.store(Arc::new(state));
    }

    pub fn availability(&self) -> GraphAvailability {
        self.runtime.load().availability()
    }

    pub fn serving_handle(&self) -> Option<Arc<GraphHandle>> {
        match self.runtime.load().as_ref() {
            GraphRuntimeState::Serving { handle, .. } => Some(Arc::clone(handle)),
            GraphRuntimeState::Opening { .. } | GraphRuntimeState::Unavailable { .. } => None,
        }
    }

    pub fn wake(&self) {
        self.notify.notify_one();
    }
}

/// Immutable configured-set snapshot. Entry runtime changes do not rebuild it.
pub struct RegistrySnapshot {
    pub graphs: HashMap<GraphKey, Arc<GraphEntry>>,
    pub any_per_graph_policy: bool,
}

impl RegistrySnapshot {
    pub fn new(graphs: HashMap<GraphKey, Arc<GraphEntry>>) -> Self {
        let any_per_graph_policy = graphs.values().any(|entry| entry.policy_configured);
        Self {
            graphs,
            any_per_graph_policy,
        }
    }
}

impl Default for RegistrySnapshot {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

pub enum RegistryLookup {
    Ready(Arc<GraphHandle>),
    Unavailable(GraphAvailability),
    Gone,
}

/// Why an `insert` was rejected.
#[derive(Debug, thiserror::Error)]
pub enum InsertError {
    /// Another handle already exists for this `GraphKey`. Maps to HTTP 409.
    #[error("graph '{0}' is already registered")]
    DuplicateKey(GraphKey),
    /// Another handle is open against this URI. Two graphs sharing a URI
    /// would commit through the same Lance manifest and corrupt each other.
    /// Maps to HTTP 409.
    #[error("URI '{0}' is already registered as another graph")]
    DuplicateUri(String),
    /// A handle carried an invalid graph URI. Maps to startup failure.
    #[error("URI '{uri}' is invalid: {message}")]
    InvalidUri { uri: String, message: String },
    #[error("graph id '{graph_id}' is invalid: {message}")]
    InvalidGraphId { graph_id: String, message: String },
}

pub struct GraphRegistry {
    snapshot: ArcSwap<RegistrySnapshot>,
    #[cfg(test)]
    mutate: Mutex<()>,
}

impl GraphRegistry {
    /// Empty registry. Used as a placeholder before startup populates it.
    pub fn new() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(RegistrySnapshot::default()),
            #[cfg(test)]
            mutate: Mutex::new(()),
        }
    }

    pub fn from_handles(handles: Vec<Arc<GraphHandle>>) -> Result<Self, InsertError> {
        let mut entries = Vec::with_capacity(handles.len());
        for handle in handles {
            entries.push(Arc::new(GraphEntry::from_handle(handle)?));
        }
        Self::from_entries(entries)
    }

    /// Validate every graph id and canonical URI, including collisions, before
    /// any open attempt can begin.
    pub fn from_configs(configs: Vec<GraphStartupConfig>) -> Result<Self, InsertError> {
        let mut entries = Vec::with_capacity(configs.len());
        for config in configs {
            entries.push(Arc::new(GraphEntry::from_config(config)?));
        }
        Self::from_entries(entries)
    }

    fn from_entries(entries: Vec<Arc<GraphEntry>>) -> Result<Self, InsertError> {
        let mut graphs = HashMap::with_capacity(entries.len());
        let mut seen_uris = HashMap::with_capacity(entries.len());
        for entry in entries {
            if graphs.contains_key(&entry.key) {
                return Err(InsertError::DuplicateKey(entry.key.clone()));
            }
            if seen_uris.contains_key(&entry.uri) {
                return Err(InsertError::DuplicateUri(entry.uri.clone()));
            }
            seen_uris.insert(entry.uri.clone(), entry.key.clone());
            graphs.insert(entry.key.clone(), entry);
        }
        Ok(Self {
            snapshot: ArcSwap::from_pointee(RegistrySnapshot::new(graphs)),
            #[cfg(test)]
            mutate: Mutex::new(()),
        })
    }

    /// Lock-free snapshot read. Callers that need derived state cached
    /// on the snapshot (e.g. `any_per_graph_policy`) go through here;
    /// callers that only need values of `graphs` should use [`list`]
    /// or [`get`].
    pub fn snapshot_ref(&self) -> arc_swap::Guard<Arc<RegistrySnapshot>> {
        self.snapshot.load()
    }

    pub fn get(&self, key: &GraphKey) -> RegistryLookup {
        let snapshot = self.snapshot.load();
        match snapshot.graphs.get(key) {
            Some(entry) => match entry.serving_handle() {
                Some(handle) => RegistryLookup::Ready(handle),
                None => RegistryLookup::Unavailable(entry.availability()),
            },
            None => RegistryLookup::Gone,
        }
    }

    pub fn entry(&self, key: &GraphKey) -> Option<Arc<GraphEntry>> {
        self.snapshot.load().graphs.get(key).cloned()
    }

    /// Block new writes and coalesce a Full-recovery wake-up for a serving
    /// graph. The prior blocking operation remains the public diagnostic until
    /// recovery succeeds; later requests are never treated as replays of it.
    pub async fn mark_recovering(
        &self,
        key: &GraphKey,
        blocking_operation_id: Option<String>,
    ) -> bool {
        let Some(entry) = self.entry(key) else {
            return false;
        };
        let _guard = entry.mutation.lock().await;
        let current = entry.runtime().as_ref().clone();
        let next = match current {
            GraphRuntimeState::Serving { handle, writes } => {
                let existing = match writes {
                    WriteState::Ready => None,
                    WriteState::Recovering {
                        blocking_operation_id,
                        ..
                    }
                    | WriteState::Blocked {
                        blocking_operation_id,
                        ..
                    } => blocking_operation_id,
                };
                GraphRuntimeState::Serving {
                    handle,
                    writes: WriteState::Recovering {
                        attempts: 0,
                        blocking_operation_id: existing.or(blocking_operation_id),
                    },
                }
            }
            GraphRuntimeState::Opening { .. } | GraphRuntimeState::Unavailable { .. } => {
                return false;
            }
        };
        entry.store_runtime(next);
        drop(_guard);
        entry.wake();
        true
    }

    pub fn write_availability(&self, key: &GraphKey) -> Option<GraphAvailability> {
        self.entry(key).map(|entry| entry.availability())
    }

    /// Snapshot every configured entry, including graphs that never opened.
    pub fn list(&self) -> Vec<Arc<GraphEntry>> {
        let snapshot = self.snapshot.load();
        snapshot.graphs.values().cloned().collect()
    }

    /// Number of registered graphs (excluding any future tombstones).
    pub fn len(&self) -> usize {
        self.snapshot.load().graphs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    pub async fn insert(&self, handle: Arc<GraphHandle>) -> Result<(), InsertError> {
        let _guard = self.mutate.lock().await;
        let current = self.snapshot.load();
        let entry = Arc::new(GraphEntry::from_handle(handle)?);
        if current.graphs.contains_key(&entry.key) {
            return Err(InsertError::DuplicateKey(entry.key.clone()));
        }
        for existing in current.graphs.values() {
            if existing.uri == entry.uri {
                return Err(InsertError::DuplicateUri(entry.uri.clone()));
            }
        }
        let mut new_graphs = current.graphs.clone();
        new_graphs.insert(entry.key.clone(), entry);
        self.snapshot
            .store(Arc::new(RegistrySnapshot::new(new_graphs)));
        Ok(())
    }
}

fn canonicalize_handle_uri(
    handle: Arc<GraphHandle>,
) -> Result<(String, Arc<GraphHandle>), InsertError> {
    let canonical_uri = normalize_root_uri(&handle.uri).map_err(|err| InsertError::InvalidUri {
        uri: handle.uri.clone(),
        message: err.to_string(),
    })?;
    if canonical_uri == handle.uri {
        return Ok((canonical_uri, handle));
    }
    let canonical_handle = Arc::new(GraphHandle {
        key: handle.key.clone(),
        uri: canonical_uri.clone(),
        engine: Arc::clone(&handle.engine),
        policy: handle.policy.clone(),
        queries: handle.queries.clone(),
    });
    Ok((canonical_uri, canonical_handle))
}

impl Default for GraphRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;
    use crate::graph_id::GraphId;

    const TEST_SCHEMA: &str = "node Person { name: String @key }\n";

    fn startup_config(graph_id: &str, uri: String) -> GraphStartupConfig {
        GraphStartupConfig {
            graph_id: graph_id.to_string(),
            uri,
            policy: None,
            startup_error: None,
            embedding: None,
            queries: QueryRegistry::default(),
        }
    }

    async fn build_handle(graph_id: &str, dir: &Path) -> Arc<GraphHandle> {
        let graph_uri = dir.join(graph_id).to_str().unwrap().to_string();
        let engine = Omnigraph::init(&graph_uri, TEST_SCHEMA)
            .await
            .expect("init engine for registry test");
        Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from(graph_id).unwrap()),
            uri: graph_uri,
            engine: Arc::new(engine),
            policy: None,
            queries: None,
        })
    }

    #[tokio::test]
    async fn new_registry_is_empty() {
        let registry = GraphRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.list().is_empty());
    }

    #[test]
    fn configured_entries_exist_before_open_and_validate_all_collisions() {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().join("same").to_string_lossy().to_string();
        let duplicate = GraphRegistry::from_configs(vec![
            startup_config("alpha", uri.clone()),
            startup_config("beta", format!("file://{uri}/")),
        ]);
        assert!(matches!(duplicate, Err(InsertError::DuplicateUri(_))));

        let mut configured = startup_config("alpha", uri);
        configured.policy = Some(crate::PolicySource::Inline(
            "version: 1\nrules: []\n".into(),
        ));
        let registry = GraphRegistry::from_configs(vec![configured]).unwrap();
        assert_eq!(registry.len(), 1);
        assert!(registry.snapshot_ref().any_per_graph_policy);
        let entry = registry.list().pop().unwrap();
        assert_eq!(entry.availability().state, "opening");
        assert!(!entry.availability().read_ready);
        assert!(matches!(
            registry.get(&entry.key),
            RegistryLookup::Unavailable(_)
        ));
    }

    #[test]
    fn configured_entries_reject_invalid_ids_before_open() {
        let dir = TempDir::new().unwrap();
        let error = match GraphRegistry::from_configs(vec![startup_config(
            "not/valid",
            dir.path().join("graph").to_string_lossy().to_string(),
        )]) {
            Ok(_) => panic!("invalid graph id must fail preflight"),
            Err(error) => error,
        };
        assert!(matches!(error, InsertError::InvalidGraphId { .. }));
    }

    #[tokio::test]
    async fn insert_then_get_returns_ready() {
        let dir = TempDir::new().unwrap();
        let registry = GraphRegistry::new();
        let handle = build_handle("alpha", dir.path()).await;
        registry.insert(Arc::clone(&handle)).await.unwrap();

        match registry.get(&handle.key) {
            RegistryLookup::Ready(found) => {
                assert!(Arc::ptr_eq(&found, &handle));
            }
            RegistryLookup::Unavailable(_) => panic!("expected Ready, got Unavailable"),
            RegistryLookup::Gone => panic!("expected Ready, got Gone"),
        }
    }

    #[tokio::test]
    async fn get_nonexistent_returns_gone() {
        let registry = GraphRegistry::new();
        let key = GraphKey::cluster(GraphId::try_from("ghost").unwrap());
        match registry.get(&key) {
            RegistryLookup::Gone => {}
            RegistryLookup::Ready(_) => panic!("expected Gone"),
            RegistryLookup::Unavailable(_) => panic!("expected Gone"),
        }
    }

    #[tokio::test]
    async fn insert_duplicate_key_returns_error() {
        let dir = TempDir::new().unwrap();
        let registry = GraphRegistry::new();
        let h1 = build_handle("alpha", dir.path()).await;
        // Same key, different URI sub-path (build_handle uses graph_id as subdir).
        let dir2 = TempDir::new().unwrap();
        let h2 = build_handle("alpha", dir2.path()).await;
        registry.insert(h1).await.unwrap();

        match registry.insert(h2).await {
            Err(InsertError::DuplicateKey(_)) => {}
            other => panic!("expected DuplicateKey, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn insert_duplicate_uri_returns_error() {
        let dir = TempDir::new().unwrap();
        // Two handles with the same URI but different keys.
        let shared_uri = dir.path().join("shared").to_str().unwrap().to_string();
        let engine = Omnigraph::init(&shared_uri, TEST_SCHEMA).await.unwrap();
        let engine = Arc::new(engine);
        let h1 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: shared_uri.clone(),
            engine: Arc::clone(&engine),
            policy: None,
            queries: None,
        });
        let h2 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: shared_uri,
            engine,
            policy: None,
            queries: None,
        });

        let registry = GraphRegistry::new();
        registry.insert(h1).await.unwrap();
        match registry.insert(h2).await {
            Err(InsertError::DuplicateUri(_)) => {}
            other => panic!("expected DuplicateUri, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_returns_all_inserted_handles() {
        let dir = TempDir::new().unwrap();
        let registry = GraphRegistry::new();
        for name in ["alpha", "beta", "gamma"] {
            let h = build_handle(name, dir.path()).await;
            registry.insert(h).await.unwrap();
        }
        assert_eq!(registry.len(), 3);
        let mut ids: Vec<_> = registry
            .list()
            .into_iter()
            .map(|h| h.key.graph_id.as_str().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn from_handles_bulk_init_succeeds() {
        let dir = TempDir::new().unwrap();
        let handles = vec![
            build_handle("alpha", dir.path()).await,
            build_handle("beta", dir.path()).await,
        ];
        let registry = GraphRegistry::from_handles(handles).unwrap();
        assert_eq!(registry.len(), 2);
    }

    #[tokio::test]
    async fn from_handles_rejects_duplicate_keys() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let h1 = build_handle("alpha", dir1.path()).await;
        let h2 = build_handle("alpha", dir2.path()).await;
        let err = match GraphRegistry::from_handles(vec![h1, h2]) {
            Ok(_) => panic!("expected DuplicateKey, got Ok"),
            Err(err) => err,
        };
        assert!(
            matches!(err, InsertError::DuplicateKey(_)),
            "expected DuplicateKey, got {err}",
        );
    }

    #[tokio::test]
    async fn from_handles_rejects_duplicate_uris() {
        let dir = TempDir::new().unwrap();
        let shared_uri = dir.path().join("shared").to_str().unwrap().to_string();
        let engine = Arc::new(Omnigraph::init(&shared_uri, TEST_SCHEMA).await.unwrap());
        let h1 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("alpha").unwrap()),
            uri: shared_uri.clone(),
            engine: Arc::clone(&engine),
            policy: None,
            queries: None,
        });
        let h2 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: shared_uri,
            engine,
            policy: None,
            queries: None,
        });
        let err = match GraphRegistry::from_handles(vec![h1, h2]) {
            Ok(_) => panic!("expected DuplicateUri, got Ok"),
            Err(err) => err,
        };
        assert!(
            matches!(err, InsertError::DuplicateUri(_)),
            "expected DuplicateUri, got {err}",
        );
    }

    /// Race test modeled on `actor_admission_race_does_not_exceed_cap`
    /// at `tests/server.rs:3596+`. Spawn N concurrent inserts with the
    /// same `GraphKey` (each constructing its own `GraphHandle` against
    /// its own tempdir). Exactly one must succeed; the others must
    /// return `DuplicateKey`. No `unwrap` panic: the `Mutex<()>` +
    /// in-mutex re-check is the linearization point.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_insert_same_key_exactly_one_succeeds() {
        const N: usize = 8;

        let registry = Arc::new(GraphRegistry::new());
        // Pre-create N handles (each in its own tempdir; same key).
        let mut handles = Vec::with_capacity(N);
        let mut dirs = Vec::with_capacity(N);
        for _ in 0..N {
            let d = TempDir::new().unwrap();
            handles.push(build_handle("contested", d.path()).await);
            dirs.push(d);
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(N));
        let mut tasks = Vec::with_capacity(N);
        for handle in handles {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                registry.insert(handle).await
            }));
        }

        let mut ok_count = 0usize;
        let mut dup_count = 0usize;
        for t in tasks {
            match t.await.unwrap() {
                Ok(()) => ok_count += 1,
                Err(InsertError::DuplicateKey(_)) => dup_count += 1,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
        assert_eq!(ok_count, 1, "exactly one insert must succeed");
        assert_eq!(dup_count, N - 1, "the rest must return DuplicateKey");
        assert_eq!(registry.len(), 1);

        // Drop the dirs at the end (preserves engines until tasks finish).
        drop(dirs);
    }

    /// Concurrent inserts with **distinct** keys all succeed.
    /// Linearizability over the mutex still serializes them.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_insert_distinct_keys_all_succeed() {
        const N: usize = 8;

        let registry = Arc::new(GraphRegistry::new());
        // Pre-create N handles with distinct ids, each in its own tempdir.
        let mut handles = Vec::with_capacity(N);
        let mut dirs = Vec::with_capacity(N);
        for i in 0..N {
            let d = TempDir::new().unwrap();
            handles.push(build_handle(&format!("graph-{i}"), d.path()).await);
            dirs.push(d);
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(N));
        let mut tasks = Vec::with_capacity(N);
        for handle in handles {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                registry.insert(handle).await
            }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(registry.len(), N);
        drop(dirs);
    }

    /// Concurrent reads during a write must always see a consistent
    /// snapshot (no torn state). With `ArcSwap`, the read either sees
    /// the old snapshot or the new one — never both, never neither.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_reads_during_inserts_see_consistent_snapshots() {
        let dir = TempDir::new().unwrap();
        let registry = Arc::new(GraphRegistry::new());

        // Spawn a writer that inserts graph-0..graph-9 sequentially.
        const N_WRITES: usize = 10;
        let writer_registry = Arc::clone(&registry);
        let writer_dir = dir.path().to_path_buf();
        let writer = tokio::spawn(async move {
            for i in 0..N_WRITES {
                let h = build_handle(&format!("graph-{i}"), &writer_dir).await;
                writer_registry.insert(h).await.unwrap();
            }
        });

        // Reader loop: repeatedly snapshot the registry until the writer
        // finishes. Every snapshot's len must be in [0, N_WRITES], and
        // for every key g in the snapshot, get(g) must return Ready.
        let reader_registry = Arc::clone(&registry);
        let reader = tokio::spawn(async move {
            for _ in 0..200 {
                let snap = reader_registry.list();
                assert!(snap.len() <= N_WRITES);
                for entry in &snap {
                    match reader_registry.get(&entry.key) {
                        RegistryLookup::Ready(found) => {
                            let expected = entry.serving_handle().expect("listed entry is serving");
                            assert!(Arc::ptr_eq(&found, &expected));
                        }
                        RegistryLookup::Unavailable(_) => panic!(
                            "snapshot listed key {} but get() returned Unavailable",
                            entry.key.graph_id
                        ),
                        RegistryLookup::Gone => panic!(
                            "snapshot listed key {} but get() returned Gone",
                            entry.key.graph_id
                        ),
                    }
                }
                tokio::task::yield_now().await;
            }
        });

        writer.await.unwrap();
        reader.await.unwrap();
        assert_eq!(registry.len(), N_WRITES);
    }
}
