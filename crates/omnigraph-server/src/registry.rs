//! `GraphRegistry` — the multi-graph routing substrate (MR-668).
//!
//! Holds the open `Arc<GraphHandle>` for every graph the server is currently
//! serving. Lock-free reads via `ArcSwap<RegistrySnapshot>`; mutations
//! serialize through `mutate: Mutex<()>` for read-modify-write atomicity.
//!
//! **Deletion is deferred** in v0.6.0 (MR-668 scope cut). The registry has
//! no `tombstones` field, no `RegistryLookup::Tombstoned` variant, no
//! `tombstone()` / `clear_tombstone()` methods. When `DELETE /graphs/{id}`
//! lands in a follow-up release, those return without breaking caller
//! signatures (`Gone` is the closest semantic — the graph is no longer
//! in the registry).
//!
//! Engine instance survival across registry mutations:
//! a request that grabbed `Arc<GraphHandle>` before a registry swap keeps
//! the engine alive via its own `Arc` clone (see `server_export` at
//! `lib.rs:1019-1033` for the spawn-and-clone pattern). The engine drops
//! when the last `Arc<Omnigraph>` clone drops, regardless of the
//! registry's current state.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use omnigraph::db::Omnigraph;
use omnigraph::storage::normalize_root_uri;
use tokio::sync::Mutex;

use crate::identity::GraphKey;
use crate::policy::PolicyEngine;
use crate::queries::QueryRegistry;

/// Supervision state for a configured graph whose open failed (RFC-029 W3).
///
/// A quarantined graph is *configured but not serving*: its startup config is
/// retained, the supervision loop retries the full `open_single_graph` with
/// capped backoff, and this record is what `GET /graphs` and the routing
/// middleware surface meanwhile.
#[derive(Debug, Clone)]
pub struct QuarantineInfo {
    /// The configured graph URI (the open never succeeded, so there is no
    /// handle to carry it).
    pub uri: String,
    /// When the graph first entered quarantine (this boot).
    pub since: std::time::SystemTime,
    /// Open attempts so far (boot's failed open counts as the first).
    pub attempts: u32,
    /// The most recent open error, verbatim.
    pub last_error: String,
    /// When the supervision loop will try again.
    pub retry_at: std::time::SystemTime,
    /// Whether the graph's startup config declares a per-graph policy.
    /// Folded into [`RegistrySnapshot::any_per_graph_policy`] so bearer auth
    /// stays required while the policy-bearing graph is quarantined — today
    /// such a graph silently vanishes from the registry, so this is strictly
    /// safer than the pre-RFC-029 behavior.
    pub policy_configured: bool,
}

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

/// Immutable snapshot of the registry's current state. Replaced atomically
/// via `ArcSwap`; readers see a consistent view of all graphs without locking.
///
/// Derived state (`any_per_graph_policy`) is computed at snapshot
/// construction so request-time middleware doesn't have to walk the
/// graph map every call. Construct only via [`RegistrySnapshot::new`]
/// (or `Default`) so the field stays in sync with `graphs`.
pub struct RegistrySnapshot {
    pub graphs: HashMap<GraphKey, Arc<GraphHandle>>,
    /// Configured graphs whose open failed and are under supervision
    /// (RFC-029 W3). Disjoint from `graphs` by construction: the writer
    /// methods maintain "a key is never in both maps" under the `mutate`
    /// mutex, and [`RegistrySnapshot::with_quarantined`] debug-asserts it.
    pub quarantined: HashMap<GraphKey, QuarantineInfo>,
    /// `true` iff any registered graph has a per-graph policy installed,
    /// OR any quarantined graph's config declares one. Used by
    /// `AppState::requires_bearer_auth` to decide whether the auth
    /// middleware should challenge a request — a per-graph policy implies
    /// bearer auth is required even when no server-level tokens or policy
    /// are configured, and a quarantined policy-bearing graph must not
    /// flap auth off while it heals.
    pub any_per_graph_policy: bool,
}

impl RegistrySnapshot {
    /// Build a snapshot from a graph map, deriving cached fields.
    /// The only construction paths are this and
    /// [`RegistrySnapshot::with_quarantined`] — direct struct-literal use
    /// elsewhere would let derived state drift from `graphs`.
    pub fn new(graphs: HashMap<GraphKey, Arc<GraphHandle>>) -> Self {
        Self::with_quarantined(graphs, HashMap::new())
    }

    /// Build a snapshot carrying quarantine entries (RFC-029 W3).
    pub fn with_quarantined(
        graphs: HashMap<GraphKey, Arc<GraphHandle>>,
        quarantined: HashMap<GraphKey, QuarantineInfo>,
    ) -> Self {
        debug_assert!(
            quarantined.keys().all(|key| !graphs.contains_key(key)),
            "a graph key must never be both serving and quarantined",
        );
        let any_per_graph_policy = graphs.values().any(|h| h.policy.is_some())
            || quarantined.values().any(|q| q.policy_configured);
        Self {
            graphs,
            quarantined,
            any_per_graph_policy,
        }
    }
}

impl Default for RegistrySnapshot {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

/// Result of a registry lookup. `Tombstoned` remains deferred with DELETE.
pub enum RegistryLookup {
    /// Graph is open and ready to serve.
    Ready(Arc<GraphHandle>),
    /// Graph is configured but not serving: its open failed and the
    /// supervision loop is retrying (RFC-029 W3). Handlers respond 503 —
    /// "retry later", not "no such resource".
    Quarantined(QuarantineInfo),
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
    /// Serializes runtime mutations (`publish`, `set_quarantined`, and the
    /// test-only `insert`) so read-modify-write cycles over the `ArcSwap`
    /// snapshot are atomic. Ungated from `#[cfg(test)]` by RFC-029 W3/W2(b):
    /// the supervision loop is the anticipated production consumer the
    /// original gate's doc comment named.
    mutate: Mutex<()>,
}

impl GraphRegistry {
    /// Empty registry. Used as a placeholder before startup populates it.
    pub fn new() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(RegistrySnapshot::default()),
            mutate: Mutex::new(()),
        }
    }

    /// Build a registry from a startup-time list of open handles.
    /// Rejects duplicate `GraphKey`s and duplicate URIs.
    pub fn from_handles(handles: Vec<Arc<GraphHandle>>) -> Result<Self, InsertError> {
        Self::from_boot(handles, Vec::new())
    }

    /// Boot-time constructor carrying both healthy handles and quarantined
    /// entries (RFC-029 W3). Rejects duplicate `GraphKey`s and duplicate
    /// URIs among the handles; quarantined keys must be disjoint from the
    /// serving keys (the boot loop guarantees it — a graph either opened or
    /// it didn't).
    pub fn from_boot(
        handles: Vec<Arc<GraphHandle>>,
        quarantined: Vec<(GraphKey, QuarantineInfo)>,
    ) -> Result<Self, InsertError> {
        let mut graphs: HashMap<GraphKey, Arc<GraphHandle>> = HashMap::with_capacity(handles.len());
        let mut seen_uris: HashMap<String, GraphKey> = HashMap::with_capacity(handles.len());
        for handle in handles {
            let (canonical_uri, handle) = canonicalize_handle_uri(handle)?;
            if graphs.contains_key(&handle.key) {
                return Err(InsertError::DuplicateKey(handle.key.clone()));
            }
            if seen_uris.contains_key(&canonical_uri) {
                return Err(InsertError::DuplicateUri(handle.uri.clone()));
            }
            seen_uris.insert(canonical_uri, handle.key.clone());
            graphs.insert(handle.key.clone(), handle);
        }
        let quarantined: HashMap<GraphKey, QuarantineInfo> = quarantined
            .into_iter()
            .filter(|(key, _)| !graphs.contains_key(key))
            .collect();
        Ok(Self {
            snapshot: ArcSwap::from_pointee(RegistrySnapshot::with_quarantined(
                graphs,
                quarantined,
            )),
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

    /// Lock-free read. Returns `Ready` if the graph is serving,
    /// `Quarantined` if it is configured-but-healing (RFC-029 W3), and
    /// `Gone` otherwise.
    pub fn get(&self, key: &GraphKey) -> RegistryLookup {
        let snapshot = self.snapshot.load();
        if let Some(handle) = snapshot.graphs.get(key) {
            return RegistryLookup::Ready(Arc::clone(handle));
        }
        if let Some(info) = snapshot.quarantined.get(key) {
            return RegistryLookup::Quarantined(info.clone());
        }
        RegistryLookup::Gone
    }

    /// Snapshot the full set of currently-registered handles. Ordering
    /// matches the underlying `HashMap` iteration (intentionally
    /// non-deterministic — callers that need a stable order sort by
    /// `handle.key.graph_id`).
    pub fn list(&self) -> Vec<Arc<GraphHandle>> {
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

    /// Add a new handle. Async because the mutex is `tokio::sync::Mutex`
    /// (a future managed-catalog flow may hold it across `.await` points
    /// during atomic registry mutations). Rejects duplicate `GraphKey`
    /// and duplicate `uri`.
    ///
    /// **Test-only surface.** No production code reaches this — startup
    /// uses `from_boot`, and production runtime mutation goes through
    /// [`GraphRegistry::publish`] (replace-allowed) /
    /// [`GraphRegistry::set_quarantined`], the RFC-029 consumers that
    /// ungated the `mutate` mutex. `insert` stays test-only because its
    /// add-only duplicate-key semantics exist to pin the mutex
    /// linearization contract, not to serve traffic.
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
                normalize_root_uri(&existing.uri).map_err(|err| InsertError::InvalidUri {
                    uri: existing.uri.clone(),
                    message: err.to_string(),
                })?;
            if existing_uri == canonical_uri {
                return Err(InsertError::DuplicateUri(handle.uri.clone()));
            }
        }
        let mut new_graphs = current.graphs.clone();
        new_graphs.insert(handle.key.clone(), handle);
        self.snapshot.store(Arc::new(RegistrySnapshot::with_quarantined(
            new_graphs,
            current.quarantined.clone(),
        )));
        Ok(())
    }

    /// RCU publish (RFC-029 W3/W2(b)): install `handle` for its key, clearing
    /// any quarantine entry and replacing any prior serving handle. Same-key
    /// replacement is the supervised-reopen swap — in-flight requests on the
    /// old handle finish on their own `Arc` clone (the engine-survival
    /// contract in this module's docs); new requests resolve the new handle.
    /// The duplicate-URI check skips the key being replaced.
    pub async fn publish(&self, handle: Arc<GraphHandle>) -> Result<(), InsertError> {
        let _guard = self.mutate.lock().await;
        let current = self.snapshot.load();
        let (canonical_uri, handle) = canonicalize_handle_uri(handle)?;
        for (key, existing) in &current.graphs {
            if *key == handle.key {
                continue;
            }
            let existing_uri =
                normalize_root_uri(&existing.uri).map_err(|err| InsertError::InvalidUri {
                    uri: existing.uri.clone(),
                    message: err.to_string(),
                })?;
            if existing_uri == canonical_uri {
                return Err(InsertError::DuplicateUri(handle.uri.clone()));
            }
        }
        let mut new_graphs = current.graphs.clone();
        new_graphs.insert(handle.key.clone(), Arc::clone(&handle));
        let mut new_quarantined = current.quarantined.clone();
        new_quarantined.remove(&handle.key);
        self.snapshot.store(Arc::new(RegistrySnapshot::with_quarantined(
            new_graphs,
            new_quarantined,
        )));
        Ok(())
    }

    /// Record a new or updated quarantine entry for a key with no serving
    /// handle (RFC-029 W3). **No-op if the key is currently serving**: a
    /// failed supervised reopen of a still-healthy graph must not take it
    /// down — the old handle keeps serving and the retry reschedules.
    pub async fn set_quarantined(&self, key: GraphKey, info: QuarantineInfo) {
        let _guard = self.mutate.lock().await;
        let current = self.snapshot.load();
        if current.graphs.contains_key(&key) {
            return;
        }
        let mut new_quarantined = current.quarantined.clone();
        new_quarantined.insert(key, info);
        self.snapshot.store(Arc::new(RegistrySnapshot::with_quarantined(
            current.graphs.clone(),
            new_quarantined,
        )));
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
            RegistryLookup::Quarantined(_) => panic!("expected Ready, got Quarantined"),
            RegistryLookup::Gone => panic!("expected Ready, got Gone"),
        }
    }

    #[tokio::test]
    async fn get_nonexistent_returns_gone() {
        let registry = GraphRegistry::new();
        let key = GraphKey::cluster(GraphId::try_from("ghost").unwrap());
        match registry.get(&key) {
            RegistryLookup::Gone => {}
            RegistryLookup::Quarantined(_) => panic!("expected Gone, got Quarantined"),
            RegistryLookup::Ready(_) => panic!("expected Gone"),
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
                for handle in &snap {
                    match reader_registry.get(&handle.key) {
                        RegistryLookup::Ready(found) => {
                            assert!(Arc::ptr_eq(&found, handle));
                        }
                        RegistryLookup::Quarantined(_) | RegistryLookup::Gone => panic!(
                            "snapshot listed key {} but get() did not return Ready",
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

    fn quarantine_info(uri: &str, policy_configured: bool) -> QuarantineInfo {
        let now = std::time::SystemTime::now();
        QuarantineInfo {
            uri: uri.to_string(),
            since: now,
            attempts: 1,
            last_error: "open failed (test)".to_string(),
            retry_at: now,
            policy_configured,
        }
    }

    /// RFC-029 W3/W2(b): `publish` installs a handle, clears its quarantine
    /// entry, and replaces a prior serving handle for the same key (the
    /// supervised-reopen swap `insert` deliberately refuses).
    #[tokio::test]
    async fn publish_replaces_serving_handle_and_clears_quarantine() {
        let dir = TempDir::new().unwrap();
        let key = GraphKey::cluster(GraphId::try_from("alpha").unwrap());
        let registry = GraphRegistry::from_boot(
            Vec::new(),
            vec![(key.clone(), quarantine_info("file:///nowhere/alpha", false))],
        )
        .unwrap();
        match registry.get(&key) {
            RegistryLookup::Quarantined(info) => assert_eq!(info.attempts, 1),
            _ => panic!("boot quarantine entry must surface as Quarantined"),
        }

        // Heal: publish clears the quarantine entry and serves.
        let healed = build_handle("alpha", dir.path()).await;
        registry.publish(Arc::clone(&healed)).await.unwrap();
        assert!(registry.snapshot_ref().quarantined.is_empty());
        match registry.get(&key) {
            RegistryLookup::Ready(found) => assert!(Arc::ptr_eq(&found, &healed)),
            _ => panic!("published handle must be Ready"),
        }

        // Swap: publishing again for the same key replaces the handle
        // (same-key replace skips the duplicate-URI check).
        let dir2 = TempDir::new().unwrap();
        let replacement = build_handle("alpha", dir2.path()).await;
        registry.publish(Arc::clone(&replacement)).await.unwrap();
        match registry.get(&key) {
            RegistryLookup::Ready(found) => assert!(Arc::ptr_eq(&found, &replacement)),
            _ => panic!("replacement handle must be Ready"),
        }
        assert_eq!(registry.len(), 1);

        // A different key colliding on URI is still rejected.
        let colliding = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: replacement.uri.clone(),
            engine: Arc::clone(&replacement.engine),
            policy: None,
            queries: None,
        });
        match registry.publish(colliding).await {
            Err(InsertError::DuplicateUri(_)) => {}
            other => panic!("expected DuplicateUri, got {other:?}"),
        }
    }

    /// RFC-029 W3: a failed supervised reopen of a still-serving graph must
    /// not take it down — `set_quarantined` is a no-op while the key serves.
    #[tokio::test]
    async fn set_quarantined_is_noop_while_key_is_serving() {
        let dir = TempDir::new().unwrap();
        let handle = build_handle("alpha", dir.path()).await;
        let key = handle.key.clone();
        let registry = GraphRegistry::from_handles(vec![handle]).unwrap();

        registry
            .set_quarantined(key.clone(), quarantine_info("file:///nowhere/alpha", false))
            .await;
        assert!(
            registry.snapshot_ref().quarantined.is_empty(),
            "serving graph must not gain a quarantine entry",
        );
        assert!(matches!(registry.get(&key), RegistryLookup::Ready(_)));

        // And for a non-serving key it records/updates the entry.
        let ghost = GraphKey::cluster(GraphId::try_from("ghost").unwrap());
        registry
            .set_quarantined(ghost.clone(), quarantine_info("file:///nowhere/ghost", false))
            .await;
        let mut updated = quarantine_info("file:///nowhere/ghost", false);
        updated.attempts = 3;
        registry.set_quarantined(ghost.clone(), updated).await;
        match registry.get(&ghost) {
            RegistryLookup::Quarantined(info) => assert_eq!(info.attempts, 3),
            _ => panic!("non-serving key must surface Quarantined"),
        }
    }

    /// RFC-029 W3 auth-flap closure: a quarantined graph whose config
    /// declares a per-graph policy keeps `any_per_graph_policy` true, so
    /// bearer auth stays required while it heals.
    #[tokio::test]
    async fn quarantined_policy_bearing_graph_keeps_auth_required() {
        let key = GraphKey::cluster(GraphId::try_from("secured").unwrap());
        let registry = GraphRegistry::from_boot(
            Vec::new(),
            vec![(key, quarantine_info("file:///nowhere/secured", true))],
        )
        .unwrap();
        assert!(
            registry.snapshot_ref().any_per_graph_policy,
            "quarantined policy-bearing graph must keep bearer auth required",
        );
    }

    /// Concurrent `publish` calls for the same key serialize on the mutate
    /// mutex: all succeed (replace-allowed), the registry ends with exactly
    /// one handle, and it is one of the published ones.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_publish_same_key_all_succeed_last_wins() {
        const N: usize = 8;
        let registry = Arc::new(GraphRegistry::new());
        let mut handles = Vec::with_capacity(N);
        let mut dirs = Vec::with_capacity(N);
        for _ in 0..N {
            let d = TempDir::new().unwrap();
            handles.push(build_handle("contested", d.path()).await);
            dirs.push(d);
        }
        let published: Vec<_> = handles.clone();

        let barrier = Arc::new(tokio::sync::Barrier::new(N));
        let mut tasks = Vec::with_capacity(N);
        for handle in handles {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                registry.publish(handle).await
            }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(registry.len(), 1);
        let winner = match registry.get(&published[0].key) {
            RegistryLookup::Ready(handle) => handle,
            _ => panic!("contested key must be Ready"),
        };
        assert!(
            published.iter().any(|h| {
                // publish may canonicalize the handle; compare engines.
                Arc::ptr_eq(&h.engine, &winner.engine)
            }),
            "winner must be one of the published handles",
        );
        drop(dirs);
    }
}
