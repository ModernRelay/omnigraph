//! `GraphRegistry` — the multi-graph routing substrate (MR-668).
//!
//! Contains every configured graph, including graphs blocked during startup.
//! Immutable snapshots keep identity, availability and policy together. Ready
//! request handles retain their engine through their last owner; blocked entries
//! retain only fixed bindings and a bounded public failure classification.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use omnigraph::db::Omnigraph;
use omnigraph::storage::normalize_root_uri;
#[cfg(test)]
use tokio::sync::Mutex;

use crate::identity::GraphKey;
use crate::policy::PolicyEngine;
use crate::queries::QueryRegistry;

/// Open handle for a single graph in the registry. Cheap to clone (`Arc`-wrapped
/// engine + policy). Cluster-mode handlers extract this via
/// `Extension<Arc<GraphHandle>>` injected by the routing middleware.
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

/// A configured graph whose startup failed. An invalid policy must not be
/// interpreted as absence of a policy when authorizing status disclosure.
pub struct BlockedGraph {
    pub key: GraphKey,
    pub uri: String,
    pub policy: Option<Arc<PolicyEngine>>,
    pub failure: StartupFailure,
}

/// One configured graph's identity, fixed bindings and serving availability.
#[derive(Clone)]
pub enum GraphEntry {
    Ready(Arc<GraphHandle>),
    Blocked(Arc<BlockedGraph>),
}

impl GraphEntry {
    pub fn key(&self) -> &GraphKey {
        match self {
            Self::Ready(handle) => &handle.key,
            Self::Blocked(graph) => &graph.key,
        }
    }

    pub fn uri(&self) -> &str {
        match self {
            Self::Ready(handle) => &handle.uri,
            Self::Blocked(graph) => &graph.uri,
        }
    }

    pub fn policy(&self) -> Option<&Arc<PolicyEngine>> {
        match self {
            Self::Ready(handle) => handle.policy.as_ref(),
            Self::Blocked(graph) => graph.policy.as_ref(),
        }
    }

    fn requires_policy_auth(&self) -> bool {
        self.policy().is_some()
            || matches!(self, Self::Blocked(graph) if graph.failure == StartupFailure::InvalidPolicy)
    }
}

/// Immutable snapshot of the registry's current state. Replaced atomically
/// via `ArcSwap`; readers see a consistent view of all graphs without locking.
///
/// Derived state (`any_per_graph_policy`) is computed at snapshot
/// construction so request-time middleware doesn't have to walk the
/// graph map every call. Construct only via [`RegistrySnapshot::new`]
/// (or `Default`) so the field stays in sync with `graphs`.
pub struct RegistrySnapshot {
    pub graphs: HashMap<GraphKey, GraphEntry>,
    /// `true` iff any configured graph has a valid or refused policy binding.
    /// Used by `AppState::requires_bearer_auth` to decide whether the
    /// auth middleware should challenge a request — a per-graph policy
    /// implies bearer auth is required even when no server-level tokens
    /// or policy are configured.
    pub any_per_graph_policy: bool,
}

impl RegistrySnapshot {
    /// Build a snapshot from a graph map, deriving cached fields.
    /// The only construction path — direct struct-literal use elsewhere
    /// would let derived state drift from `graphs`.
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

/// Availability and identity from one registry snapshot.
pub enum RegistryLookup {
    /// Graph is open and ready to serve.
    Ready(Arc<GraphHandle>),
    /// Graph is configured but unavailable after a startup failure.
    Blocked(Arc<BlockedGraph>),
    /// Graph is not in the registry (never existed, or was unregistered in a
    /// future release). Handlers respond with 404.
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
}

pub struct GraphRegistry {
    snapshot: ArcSwap<RegistrySnapshot>,
    /// Serializes runtime mutations through [`GraphRegistry::insert`].
    /// Gated with `insert` because they share a single contract — if
    /// the consumer goes away, so does the lock. Re-introducing one
    /// requires re-introducing the other.
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

    /// Build a registry from a startup-time list of open handles.
    /// Rejects duplicate `GraphKey`s and duplicate URIs.
    pub fn from_handles(handles: Vec<Arc<GraphHandle>>) -> Result<Self, InsertError> {
        Self::from_entries(handles.into_iter().map(GraphEntry::Ready).collect())
    }

    /// Build the complete startup inventory. Unavailable entries participate
    /// in the same identity and URI uniqueness checks as ready handles.
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

    /// Lock-free lookup preserving known unavailable graphs.
    pub fn get(&self, key: &GraphKey) -> RegistryLookup {
        let snapshot = self.snapshot.load();
        match snapshot.graphs.get(key) {
            Some(GraphEntry::Ready(handle)) => RegistryLookup::Ready(Arc::clone(handle)),
            Some(GraphEntry::Blocked(graph)) => RegistryLookup::Blocked(Arc::clone(graph)),
            None => RegistryLookup::Gone,
        }
    }

    /// Snapshot the ready handles, excluding blocked entries. Ordering
    /// matches the underlying `HashMap` iteration (intentionally
    /// non-deterministic — callers that need a stable order sort by
    /// `handle.key.graph_id`).
    pub fn list(&self) -> Vec<Arc<GraphHandle>> {
        let snapshot = self.snapshot.load();
        snapshot
            .graphs
            .values()
            .filter_map(|entry| match entry {
                GraphEntry::Ready(handle) => Some(Arc::clone(handle)),
                GraphEntry::Blocked(_) => None,
            })
            .collect()
    }

    /// One complete inventory snapshot, including unavailable graphs.
    pub fn entries(&self) -> Vec<GraphEntry> {
        self.snapshot.load().graphs.values().cloned().collect()
    }

    /// Number of configured graphs, including unavailable entries.
    pub fn len(&self) -> usize {
        self.snapshot.load().graphs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add a new handle. Async because the mutex is `tokio::sync::Mutex`
    /// (a future managed-catalog flow may hold it across `.await` points
    /// during atomic registry mutations). Rejects duplicate `GraphKey`
    /// and duplicate `uri`.
    ///
    /// **Test-only surface.** No production code reaches this — startup
    /// uses `from_handles`, and runtime add/remove is deferred. The
    /// race-contract tests below pin the mutex linearization point so
    /// that when a real consumer ships (managed cluster catalog), the
    /// concurrency contract is already proven. Ungate by removing
    /// `#[cfg(test)]` once that consumer is in scope.
    ///
    /// Race semantics (pinned by `concurrent_insert_same_key_exactly_one_succeeds`):
    /// under N concurrent calls with the same key, exactly one returns
    /// `Ok(())` and the rest return `Err(InsertError::DuplicateKey(_))`.
    #[cfg(test)]
    pub async fn insert(&self, handle: Arc<GraphHandle>) -> Result<(), InsertError> {
        let _guard = self.mutate.lock().await;
        let current = self.snapshot.load();
        let (canonical_uri, handle) = canonicalize_handle_uri(handle)?;
        if current.graphs.contains_key(&handle.key) {
            return Err(InsertError::DuplicateKey(handle.key.clone()));
        }
        for existing in current.graphs.values() {
            let existing_uri =
                normalize_root_uri(existing.uri()).map_err(|err| InsertError::InvalidUri {
                    uri: existing.uri().to_string(),
                    message: err.to_string(),
                })?;
            if existing_uri == canonical_uri {
                return Err(InsertError::DuplicateUri(handle.uri.clone()));
            }
        }
        let mut new_graphs = current.graphs.clone();
        new_graphs.insert(handle.key.clone(), GraphEntry::Ready(handle));
        self.snapshot
            .store(Arc::new(RegistrySnapshot::new(new_graphs)));
        Ok(())
    }
}

fn canonicalize_entry_uri(entry: GraphEntry) -> Result<GraphEntry, InsertError> {
    match entry {
        GraphEntry::Ready(handle) => {
            canonicalize_handle_uri(handle).map(|(_, handle)| GraphEntry::Ready(handle))
        }
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
                assert!(Arc::ptr_eq(&found, &handle));
            }
            RegistryLookup::Gone | RegistryLookup::Blocked(_) => panic!("expected Ready"),
        }
    }

    #[tokio::test]
    async fn get_nonexistent_returns_gone() {
        let registry = GraphRegistry::new();
        let key = GraphKey::cluster(GraphId::try_from("ghost").unwrap());
        match registry.get(&key) {
            RegistryLookup::Gone => {}
            RegistryLookup::Ready(_) | RegistryLookup::Blocked(_) => panic!("expected Gone"),
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
            RegistryLookup::Ready(_) | RegistryLookup::Gone => panic!("expected Blocked"),
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
                for handle in &snap {
                    match reader_registry.get(&handle.key) {
                        RegistryLookup::Ready(found) => {
                            assert!(Arc::ptr_eq(&found, handle));
                        }
                        RegistryLookup::Gone | RegistryLookup::Blocked(_) => panic!(
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
}
