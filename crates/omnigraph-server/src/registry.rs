//! Configured graph identity and the single serving-admission authority.
//!
//! Snapshot readers retain one immutable view. Request capture and transitions
//! use the same synchronous registry boundary under process admission.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use omnigraph::db::Omnigraph;
use omnigraph::storage::normalize_root_uri;
use tokio::time::Instant;

use crate::ApiError;
use crate::identity::GraphKey;
use crate::operations::OperationRuntime;
use crate::policy::PolicyEngine;
use crate::queries::QueryRegistry;
use crate::serving::{
    GraphRequest, PreparedTransition, ServingEpoch, ServingTransitionError, ServingView,
    TransitionRecord,
};

/// Fixed engine and effective bindings for one registered graph. Requests use
/// a [`GraphRequest`] to retain these together with their admitted epoch.
pub struct GraphHandle {
    /// Registry key. In Cluster mode `key.tenant_id` is always `None`.
    pub key: GraphKey,
    /// The URI the engine was opened from (`s3://...`, `az://...`, or local path).
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

pub use crate::api::GraphStartupFailure as StartupFailure;

/// A configured graph whose startup failed. Invalid policy is not equivalent
/// to absence of a policy when authorizing status disclosure.
pub struct BlockedGraph {
    pub key: GraphKey,
    pub uri: String,
    pub policy: Option<Arc<PolicyEngine>>,
    pub failure: StartupFailure,
}

#[derive(Clone)]
pub enum GraphEntry {
    Ready(Arc<ServingView>),
    Transitioning(Arc<ServingView>),
    Blocked(Arc<BlockedGraph>),
}

impl GraphEntry {
    /// Startup input; the view grants no request admission on its own.
    pub fn ready(handle: Arc<GraphHandle>) -> Self {
        Self::Ready(Arc::new(ServingView::new(handle, ServingEpoch::INITIAL)))
    }

    pub fn key(&self) -> &GraphKey {
        match self {
            Self::Ready(view) | Self::Transitioning(view) => &view.key,
            Self::Blocked(graph) => &graph.key,
        }
    }

    pub fn uri(&self) -> &str {
        match self {
            Self::Ready(view) | Self::Transitioning(view) => &view.uri,
            Self::Blocked(graph) => &graph.uri,
        }
    }

    pub fn policy(&self) -> Option<&Arc<PolicyEngine>> {
        match self {
            Self::Ready(view) | Self::Transitioning(view) => view.policy.as_ref(),
            Self::Blocked(graph) => graph.policy.as_ref(),
        }
    }

    fn requires_policy_auth(&self) -> bool {
        self.policy().is_some()
            || matches!(self, Self::Blocked(graph) if graph.failure == StartupFailure::InvalidPolicy)
    }
}

/// One immutable inventory/availability snapshot. Its policy cache is derived
/// on construction and remains valid while an epoch is transitioning.
pub struct RegistrySnapshot {
    pub graphs: HashMap<GraphKey, GraphEntry>,
    pub any_per_graph_policy: bool,
}

impl RegistrySnapshot {
    pub fn new(graphs: HashMap<GraphKey, GraphEntry>) -> Self {
        let any_per_graph_policy = graphs.values().any(GraphEntry::requires_policy_auth);
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

/// Introspection only. Ready views do not admit requests or allow a caller to
/// resume a closed epoch.
pub enum RegistryLookup {
    Ready(Arc<ServingView>),
    Transitioning(Arc<ServingView>),
    Blocked(Arc<BlockedGraph>),
    Gone,
}

/// Coherent request capture, including the effective bindings required to
/// authorize disclosure of an unavailable graph.
pub(crate) enum RegistryCapture {
    Ready(GraphRequest),
    Transitioning(Arc<ServingView>),
    Blocked(Arc<BlockedGraph>),
    Gone,
}

#[derive(Debug, thiserror::Error)]
pub enum InsertError {
    #[error("graph '{0}' is already registered")]
    DuplicateKey(GraphKey),
    #[error("URI '{0}' is already registered as another graph")]
    DuplicateUri(String),
    #[error("URI '{uri}' is invalid: {message}")]
    InvalidUri { uri: String, message: String },
    #[error("graph '{0}' has a live serving transition and cannot initialize a new registry")]
    Transitioning(GraphKey),
}

struct RegistryState {
    snapshot: Arc<RegistrySnapshot>,
    // One O(1) candidate per process registry. It retains the predecessor;
    // graph entry kind is the sole open/closed state, not a second flag here.
    candidate: Option<Arc<TransitionRecord>>,
}

pub struct GraphRegistry {
    state: Mutex<RegistryState>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl GraphRegistry {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(RegistryState {
                snapshot: Arc::new(RegistrySnapshot::default()),
                candidate: None,
            }),
        }
    }

    pub fn from_handles(handles: Vec<Arc<GraphHandle>>) -> Result<Self, InsertError> {
        Self::from_entries(handles.into_iter().map(GraphEntry::ready).collect())
    }

    pub fn from_entries(entries: Vec<GraphEntry>) -> Result<Self, InsertError> {
        let mut graphs = HashMap::with_capacity(entries.len());
        let mut seen_uris = HashMap::with_capacity(entries.len());
        for entry in entries {
            let entry = canonicalize_entry_uri(entry)?;
            if graphs.contains_key(entry.key()) {
                return Err(InsertError::DuplicateKey(entry.key().clone()));
            }
            if seen_uris.contains_key(entry.uri()) {
                return Err(InsertError::DuplicateUri(entry.uri().to_string()));
            }
            seen_uris.insert(entry.uri().to_string(), entry.key().clone());
            graphs.insert(entry.key().clone(), entry);
        }
        Ok(Self {
            state: Mutex::new(RegistryState {
                snapshot: Arc::new(RegistrySnapshot::new(graphs)),
                candidate: None,
            }),
        })
    }

    pub fn snapshot_ref(&self) -> Arc<RegistrySnapshot> {
        Arc::clone(&locked(&self.state).snapshot)
    }

    pub fn get(&self, key: &GraphKey) -> RegistryLookup {
        match self.snapshot_ref().graphs.get(key) {
            Some(GraphEntry::Ready(view)) => RegistryLookup::Ready(Arc::clone(view)),
            Some(GraphEntry::Transitioning(view)) => {
                RegistryLookup::Transitioning(Arc::clone(view))
            }
            Some(GraphEntry::Blocked(graph)) => RegistryLookup::Blocked(Arc::clone(graph)),
            None => RegistryLookup::Gone,
        }
    }

    pub fn list(&self) -> Vec<Arc<ServingView>> {
        self.snapshot_ref()
            .graphs
            .values()
            .filter_map(|entry| match entry {
                GraphEntry::Ready(view) => Some(Arc::clone(view)),
                GraphEntry::Transitioning(_) | GraphEntry::Blocked(_) => None,
            })
            .collect()
    }

    pub fn entries(&self) -> Vec<GraphEntry> {
        self.snapshot_ref().graphs.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.snapshot_ref().graphs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Process admission is always acquired before the registry lock. Only
    /// AppState's HTTP/MCP boundary passes its actual process runtime here.
    pub(crate) fn capture(
        &self,
        operations: &OperationRuntime,
        key: &GraphKey,
    ) -> Result<RegistryCapture, ApiError> {
        operations.while_open(|| {
            let state = locked(&self.state);
            Ok(match state.snapshot.graphs.get(key) {
                Some(GraphEntry::Ready(view)) => RegistryCapture::Ready(view.capture()?),
                Some(GraphEntry::Transitioning(view)) => {
                    RegistryCapture::Transitioning(Arc::clone(view))
                }
                Some(GraphEntry::Blocked(graph)) => RegistryCapture::Blocked(Arc::clone(graph)),
                None => RegistryCapture::Gone,
            })
        })
    }

    /// Reserve the sole candidate before closing healthy graph admission.
    /// There is no query parsing, storage refresh, or engine effect here.
    pub(crate) fn prepare_same_view(
        self: &Arc<Self>,
        operations: &OperationRuntime,
        key: &GraphKey,
        deadline: Instant,
    ) -> Result<PreparedTransition, ServingTransitionError> {
        operations.while_open(|| {
            let mut state = locked(&self.state);
            if Instant::now() >= deadline {
                return Err(ServingTransitionError::DeadlineElapsed);
            }
            if state.candidate.is_some() {
                return Err(ServingTransitionError::Busy);
            }
            let predecessor = match state.snapshot.graphs.get(key) {
                Some(GraphEntry::Ready(view)) => Arc::clone(view),
                Some(GraphEntry::Transitioning(_) | GraphEntry::Blocked(_)) => {
                    return Err(ServingTransitionError::Unavailable);
                }
                None => return Err(ServingTransitionError::Gone),
            };
            if !predecessor.contract_is_current() {
                return Err(ServingTransitionError::SchemaChanged);
            }
            // Reject identity exhaustion before closing a healthy epoch.
            predecessor.epoch().successor()?;
            let record = Arc::new(TransitionRecord {
                predecessor,
                deadline,
            });
            if Instant::now() >= deadline {
                return Err(ServingTransitionError::DeadlineElapsed);
            }
            state.candidate = Some(Arc::clone(&record));
            Ok(PreparedTransition::new(
                Arc::clone(self),
                record,
                operations.clone(),
            ))
        })
    }

    pub(crate) fn close_prepared(
        &self,
        record: &Arc<TransitionRecord>,
    ) -> Result<(), ServingTransitionError> {
        let mut state = locked(&self.state);
        validate_candidate(&state, record)?;
        if !matches!(state.snapshot.graphs.get(&record.predecessor.key),
            Some(GraphEntry::Ready(view)) if Arc::ptr_eq(view, &record.predecessor))
        {
            return Err(ServingTransitionError::StaleAttempt);
        }
        if !record.predecessor.contract_is_current() {
            return Err(ServingTransitionError::SchemaChanged);
        }
        let mut graphs = state.snapshot.graphs.clone();
        graphs.insert(
            record.predecessor.key.clone(),
            GraphEntry::Transitioning(Arc::clone(&record.predecessor)),
        );
        let snapshot = Arc::new(RegistrySnapshot::new(graphs));
        if Instant::now() >= record.deadline {
            return Err(ServingTransitionError::DeadlineElapsed);
        }
        state.snapshot = snapshot;
        Ok(())
    }

    pub(crate) fn discard_prepared(&self, record: &Arc<TransitionRecord>) {
        let mut state = locked(&self.state);
        if state
            .candidate
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, record))
            && matches!(state.snapshot.graphs.get(&record.predecessor.key),
                Some(GraphEntry::Ready(view)) if Arc::ptr_eq(view, &record.predecessor))
        {
            state.candidate = None;
        }
    }

    pub(crate) fn resume_same_view(
        &self,
        record: &Arc<TransitionRecord>,
    ) -> Result<ServingEpoch, ServingTransitionError> {
        let mut state = locked(&self.state);
        validate_candidate(&state, record)?;
        if !matches!(state.snapshot.graphs.get(&record.predecessor.key),
            Some(GraphEntry::Transitioning(view)) if Arc::ptr_eq(view, &record.predecessor))
        {
            return Err(ServingTransitionError::StaleAttempt);
        }
        if record.predecessor.request_count() != 0 {
            return Err(ServingTransitionError::RequestsActive);
        }
        let epoch = record.predecessor.epoch().successor()?;
        let successor = Arc::new(record.predecessor.successor(epoch));
        let mut graphs = state.snapshot.graphs.clone();
        graphs.insert(record.predecessor.key.clone(), GraphEntry::Ready(successor));
        let snapshot = Arc::new(RegistrySnapshot::new(graphs));
        if !record.predecessor.contract_is_current() {
            return Err(ServingTransitionError::SchemaChanged);
        }
        if Instant::now() >= record.deadline {
            return Err(ServingTransitionError::DeadlineElapsed);
        }
        state.snapshot = snapshot;
        state.candidate = None;
        Ok(epoch)
    }

    /// Test-only inventory mutation; production inventory stays fixed.
    #[cfg(test)]
    pub async fn insert(&self, handle: Arc<GraphHandle>) -> Result<(), InsertError> {
        let (canonical_uri, handle) = canonicalize_handle_uri(handle)?;
        let mut state = locked(&self.state);
        if state.snapshot.graphs.contains_key(&handle.key) {
            return Err(InsertError::DuplicateKey(handle.key.clone()));
        }
        if state
            .snapshot
            .graphs
            .values()
            .any(|entry| entry.uri() == canonical_uri)
        {
            return Err(InsertError::DuplicateUri(canonical_uri));
        }
        let mut graphs = state.snapshot.graphs.clone();
        graphs.insert(handle.key.clone(), GraphEntry::ready(handle));
        state.snapshot = Arc::new(RegistrySnapshot::new(graphs));
        Ok(())
    }
}

fn validate_candidate(
    state: &RegistryState,
    record: &Arc<TransitionRecord>,
) -> Result<(), ServingTransitionError> {
    if !state
        .candidate
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, record))
    {
        return Err(ServingTransitionError::StaleAttempt);
    }
    if Instant::now() >= record.deadline {
        return Err(ServingTransitionError::DeadlineElapsed);
    }
    Ok(())
}

fn canonicalize_entry_uri(entry: GraphEntry) -> Result<GraphEntry, InsertError> {
    match entry {
        GraphEntry::Ready(view) => {
            let (_, handle) = canonicalize_handle_uri(Arc::clone(view.handle()))?;
            if Arc::ptr_eq(&handle, view.handle()) {
                Ok(GraphEntry::Ready(view))
            } else {
                Ok(GraphEntry::ready(handle))
            }
        }
        GraphEntry::Transitioning(view) => Err(InsertError::Transitioning(view.key.clone())),
        GraphEntry::Blocked(graph) => {
            let uri = normalize_root_uri(&graph.uri).map_err(|err| InsertError::InvalidUri {
                uri: graph.uri.clone(),
                message: err.to_string(),
            })?;
            if uri == graph.uri {
                Ok(GraphEntry::Blocked(graph))
            } else {
                Ok(GraphEntry::Blocked(Arc::new(BlockedGraph {
                    key: graph.key.clone(),
                    uri,
                    policy: graph.policy.clone(),
                    failure: graph.failure,
                })))
            }
        }
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

    #[tokio::test]
    async fn insert_then_get_returns_ready() {
        let dir = TempDir::new().unwrap();
        let registry = GraphRegistry::new();
        let handle = build_handle("alpha", dir.path()).await;
        registry.insert(Arc::clone(&handle)).await.unwrap();

        match registry.get(&handle.key) {
            RegistryLookup::Ready(found) => {
                assert!(Arc::ptr_eq(found.handle(), &handle));
            }
            RegistryLookup::Gone
            | RegistryLookup::Blocked(_)
            | RegistryLookup::Transitioning(_) => panic!("expected Ready"),
        }
    }

    #[tokio::test]
    async fn get_nonexistent_returns_gone() {
        let registry = GraphRegistry::new();
        let key = GraphKey::cluster(GraphId::try_from("ghost").unwrap());
        match registry.get(&key) {
            RegistryLookup::Gone => {}
            RegistryLookup::Ready(_)
            | RegistryLookup::Blocked(_)
            | RegistryLookup::Transitioning(_) => panic!("expected Gone"),
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
        assert!(!registry.snapshot_ref().any_per_graph_policy);

        // Availability never removes configured identity. A refused policy
        // must still require authentication even though no engine was opened.
        let mut entries = registry.entries();
        let blocked = Arc::new(BlockedGraph {
            key: GraphKey::cluster(GraphId::try_from("blocked").unwrap()),
            uri: dir.path().join("blocked").to_string_lossy().into_owned(),
            policy: None,
            failure: StartupFailure::InvalidPolicy,
        });
        entries.push(GraphEntry::Blocked(Arc::clone(&blocked)));
        let registry = GraphRegistry::from_entries(entries).unwrap();
        assert_eq!(registry.len(), 3);
        assert_eq!(registry.entries().len(), 3);
        assert_eq!(registry.list().len(), 2);
        assert!(registry.snapshot_ref().any_per_graph_policy);
        match registry.get(&blocked.key) {
            RegistryLookup::Blocked(found) => {
                assert_eq!(found.failure, StartupFailure::InvalidPolicy);
                assert!(found.policy.is_none());
                assert_eq!(found.uri, normalize_root_uri(&blocked.uri).unwrap());
            }
            RegistryLookup::Ready(_) | RegistryLookup::Gone | RegistryLookup::Transitioning(_) => {
                panic!("expected Blocked")
            }
        }

        let mut duplicate_key = registry.entries();
        duplicate_key.push(GraphEntry::Blocked(Arc::clone(&blocked)));
        assert!(matches!(
            GraphRegistry::from_entries(duplicate_key),
            Err(InsertError::DuplicateKey(_)),
        ));
        let mut duplicate_uri = registry.entries();
        duplicate_uri.push(GraphEntry::Blocked(Arc::new(BlockedGraph {
            key: GraphKey::cluster(GraphId::try_from("other").unwrap()),
            uri: blocked.uri.clone(),
            policy: None,
            failure: StartupFailure::OpenFailed,
        })));
        assert!(matches!(
            GraphRegistry::from_entries(duplicate_uri),
            Err(InsertError::DuplicateUri(_)),
        ));
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
    /// return `DuplicateKey`. No `unwrap` panic: the registry mutex +
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
    /// snapshot (no torn state). The read either sees
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
                for handle in &snap {
                    match reader_registry.get(&handle.key) {
                        RegistryLookup::Ready(found) => {
                            assert!(Arc::ptr_eq(&found, handle));
                        }
                        RegistryLookup::Gone
                        | RegistryLookup::Blocked(_)
                        | RegistryLookup::Transitioning(_) => panic!(
                            "snapshot listed ready key {} but get() did not return Ready",
                            handle.key.graph_id
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

    fn captured(
        registry: &GraphRegistry,
        operations: &OperationRuntime,
        key: &GraphKey,
    ) -> GraphRequest {
        match registry.capture(operations, key).unwrap() {
            RegistryCapture::Ready(request) => request,
            _ => panic!("expected admitted ready graph"),
        }
    }

    fn transition_deadline() -> Instant {
        Instant::now() + std::time::Duration::from_secs(10)
    }

    #[tokio::test]
    async fn same_view_transition_retains_descendants_and_allocates_a_fresh_epoch() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
        );
        let operations = OperationRuntime::new();
        let request = captured(&registry, &operations, &alpha.key);
        let descendant = request.clone();
        let old_epoch = request.epoch();
        let old_snapshot = registry.snapshot_ref();
        let prepared = registry
            .prepare_same_view(&operations, &alpha.key, transition_deadline())
            .unwrap();
        // Reserving the one candidate does not close healthy admission.
        drop(captured(&registry, &operations, &alpha.key));
        assert!(matches!(
            registry.prepare_same_view(&operations, &beta.key, transition_deadline()),
            Err(ServingTransitionError::Busy)
        ));
        let transition = prepared.close().unwrap();
        assert!(matches!(
            registry.capture(&operations, &alpha.key).unwrap(),
            RegistryCapture::Transitioning(_)
        ));
        assert!(matches!(
            registry.get(&alpha.key),
            RegistryLookup::Transitioning(_)
        ));
        assert_eq!(registry.entries().len(), 2);
        assert_eq!(registry.list().len(), 1);
        drop(captured(&registry, &operations, &beta.key));
        {
            let waiting = transition.wait_requests();
            tokio::pin!(waiting);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            drop(request);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            drop(descendant);
            waiting.await.unwrap();
        }
        let fresh_epoch = transition.resume_same_view().unwrap();
        assert_ne!(fresh_epoch, old_epoch);
        let request = captured(&registry, &operations, &alpha.key);
        assert_eq!(request.epoch(), fresh_epoch);
        assert!(Arc::ptr_eq(&request.engine, &alpha.engine));
        assert_eq!(
            request.schema_contract(),
            &alpha.engine.schema_contract_digest()
        );
        // Retaining an introspection snapshot cannot revive its old epoch.
        match old_snapshot.graphs.get(&alpha.key).unwrap() {
            GraphEntry::Ready(view) => assert_eq!(view.epoch(), old_epoch),
            _ => panic!("the retained predecessor snapshot changed"),
        }
    }

    #[tokio::test]
    async fn prepared_drop_releases_only_unclosed_capacity_and_closed_drop_stays_closed() {
        let dir = TempDir::new().unwrap();
        let handle = build_handle("alpha", dir.path()).await;
        let registry = Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&handle)]).unwrap());
        let operations = OperationRuntime::new();
        let prepared = registry
            .prepare_same_view(&operations, &handle.key, transition_deadline())
            .unwrap();
        drop(prepared);
        let transition = registry
            .prepare_same_view(&operations, &handle.key, transition_deadline())
            .unwrap()
            .close()
            .unwrap();
        drop(transition);
        assert!(matches!(
            registry.capture(&operations, &handle.key).unwrap(),
            RegistryCapture::Transitioning(_)
        ));
        assert!(matches!(
            registry.prepare_same_view(&operations, &handle.key, transition_deadline()),
            Err(ServingTransitionError::Busy)
        ));
        assert!(matches!(
            GraphRegistry::from_entries(registry.entries()),
            Err(InsertError::Transitioning(_))
        ));
    }

    #[tokio::test]
    async fn process_closure_revokes_the_original_transition_runtime() {
        let dir = TempDir::new().unwrap();
        let handle = build_handle("alpha", dir.path()).await;
        for close_before_graph in [true, false] {
            let registry =
                Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&handle)]).unwrap());
            let operations = OperationRuntime::new();
            let request = captured(&registry, &operations, &handle.key);
            let prepared = registry
                .prepare_same_view(&operations, &handle.key, transition_deadline())
                .unwrap();
            if close_before_graph {
                operations.close();
                assert!(matches!(
                    prepared.close(),
                    Err(ServingTransitionError::ProcessClosed)
                ));
            } else {
                let transition = prepared.close().unwrap();
                {
                    let waiting = transition.wait_requests();
                    tokio::pin!(waiting);
                    assert!(futures::poll!(waiting.as_mut()).is_pending());
                    operations.close();
                    assert_eq!(waiting.await, Err(ServingTransitionError::ProcessClosed));
                }
                drop(request);
                assert_eq!(
                    transition.resume_same_view(),
                    Err(ServingTransitionError::ProcessClosed)
                );
                assert!(matches!(
                    registry.get(&handle.key),
                    RegistryLookup::Transitioning(_)
                ));
                continue;
            }
            assert!(registry.capture(&operations, &handle.key).is_err());
            drop(request);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn capture_racing_close_either_owns_predecessor_or_refuses() {
        let dir = TempDir::new().unwrap();
        let handle = build_handle("alpha", dir.path()).await;
        let registry = Arc::new(GraphRegistry::from_handles(vec![Arc::clone(&handle)]).unwrap());
        let operations = OperationRuntime::new();
        let prepared = registry
            .prepare_same_view(&operations, &handle.key, transition_deadline())
            .unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let capture = {
            let registry = Arc::clone(&registry);
            let operations = operations.clone();
            let key = handle.key.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                registry.capture(&operations, &key).unwrap()
            })
        };
        barrier.wait().await;
        let transition = prepared.close().unwrap();
        match capture.await.unwrap() {
            RegistryCapture::Ready(request) => {
                let waiting = transition.wait_requests();
                tokio::pin!(waiting);
                assert!(futures::poll!(waiting.as_mut()).is_pending());
                drop(request);
                waiting.await.unwrap();
            }
            RegistryCapture::Transitioning(_) => transition.wait_requests().await.unwrap(),
            _ => panic!("close/capture changed graph identity or startup state"),
        }
        transition.resume_same_view().unwrap();
    }

    #[tokio::test]
    async fn final_resume_rechecks_elapsed_deadline_without_a_timer_callback() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
        );
        let operations = OperationRuntime::new();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let transition = registry
            .prepare_same_view(&operations, &alpha.key, deadline)
            .unwrap()
            .close()
            .unwrap();
        transition.wait_requests().await.unwrap();
        // Deliberately block this current-thread runtime: no timer callback
        // or background expiry task can make the final predicate true for us.
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now())
                + std::time::Duration::from_millis(1),
        );
        assert_eq!(
            transition.resume_same_view(),
            Err(ServingTransitionError::DeadlineElapsed)
        );
        assert!(matches!(
            registry.get(&alpha.key),
            RegistryLookup::Transitioning(_)
        ));
        assert!(matches!(
            registry.prepare_same_view(&operations, &beta.key, transition_deadline()),
            Err(ServingTransitionError::Busy)
        ));
        drop(captured(&registry, &operations, &beta.key));
    }

    #[tokio::test]
    async fn schema_drift_refuses_at_prepare_close_and_final_resume() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        for stage in ["prepare", "close", "resume"] {
            let registry = Arc::new(
                GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
            );
            let operations = OperationRuntime::new();
            let prepared = (stage != "prepare").then(|| {
                registry
                    .prepare_same_view(&operations, &alpha.key, transition_deadline())
                    .unwrap()
            });
            let (prepared, transition) = if stage == "resume" {
                (None, Some(prepared.unwrap().close().unwrap()))
            } else {
                (prepared, None)
            };
            // Deliberately bypass request admission to model foreign handle
            // drift. Even a source-only change invalidates the exact binding.
            alpha
                .engine
                .apply_schema(&format!("// foreign {stage} revision\n{TEST_SCHEMA}"))
                .await
                .unwrap();
            match stage {
                "prepare" => assert!(matches!(
                    registry.prepare_same_view(&operations, &alpha.key, transition_deadline()),
                    Err(ServingTransitionError::SchemaChanged)
                )),
                "close" => assert!(matches!(
                    prepared.unwrap().close(),
                    Err(ServingTransitionError::SchemaChanged)
                )),
                "resume" => {
                    let transition = transition.unwrap();
                    transition.wait_requests().await.unwrap();
                    assert_eq!(
                        transition.resume_same_view(),
                        Err(ServingTransitionError::SchemaChanged)
                    );
                }
                _ => unreachable!(),
            }
            if stage == "resume" {
                assert!(matches!(
                    registry.get(&alpha.key),
                    RegistryLookup::Transitioning(_)
                ));
                assert!(matches!(
                    registry.prepare_same_view(&operations, &beta.key, transition_deadline()),
                    Err(ServingTransitionError::Busy)
                ));
            } else {
                assert!(matches!(registry.get(&alpha.key), RegistryLookup::Ready(_)));
                // Failure before closure did not take the sole candidate slot.
                drop(
                    registry
                        .prepare_same_view(&operations, &beta.key, transition_deadline())
                        .unwrap(),
                );
            }
        }
    }

    #[tokio::test]
    async fn stale_preparation_and_epoch_exhaustion_cannot_revoke_current_authority() {
        let dir = TempDir::new().unwrap();
        let alpha = build_handle("alpha", dir.path()).await;
        let beta = build_handle("beta", dir.path()).await;
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![Arc::clone(&alpha), Arc::clone(&beta)]).unwrap(),
        );
        let operations = OperationRuntime::new();
        let first = registry
            .prepare_same_view(&operations, &alpha.key, transition_deadline())
            .unwrap();
        let stale = Arc::clone(locked(&registry.state).candidate.as_ref().unwrap());
        drop(first);
        let current = registry
            .prepare_same_view(&operations, &beta.key, transition_deadline())
            .unwrap();
        registry.discard_prepared(&stale);
        assert!(matches!(
            registry.prepare_same_view(&operations, &alpha.key, transition_deadline()),
            Err(ServingTransitionError::Busy)
        ));
        assert_eq!(
            operations.while_open(|| registry.close_prepared(&stale)),
            Err(ServingTransitionError::StaleAttempt)
        );
        assert!(matches!(registry.get(&alpha.key), RegistryLookup::Ready(_)));
        let transition = current.close().unwrap();
        assert_eq!(
            operations.while_open(|| registry.resume_same_view(&stale)),
            Err(ServingTransitionError::StaleAttempt)
        );
        transition.wait_requests().await.unwrap();
        transition.resume_same_view().unwrap();

        // The exhausted identity is a boundary-value fixture, not a publicly
        // constructible epoch or a test-only activation capability.
        {
            let mut state = locked(&registry.state);
            let mut graphs = state.snapshot.graphs.clone();
            graphs.insert(
                alpha.key.clone(),
                GraphEntry::Ready(Arc::new(ServingView::new(
                    Arc::clone(&alpha),
                    ServingEpoch::MAX,
                ))),
            );
            state.snapshot = Arc::new(RegistrySnapshot::new(graphs));
        }
        assert!(matches!(
            registry.prepare_same_view(&operations, &alpha.key, transition_deadline()),
            Err(ServingTransitionError::EpochExhausted)
        ));
        drop(captured(&registry, &operations, &alpha.key));
        drop(
            registry
                .prepare_same_view(&operations, &beta.key, transition_deadline())
                .unwrap(),
        );
    }
}
