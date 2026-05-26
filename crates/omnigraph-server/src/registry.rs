//! `GraphRegistry` — the multi-graph routing substrate (MR-668 PR 3).
//!
//! Holds the open `Arc<GraphHandle>` for every graph the server is currently
//! serving. Lock-free reads via `ArcSwap<RegistrySnapshot>`; mutations
//! serialize through `mutate: Mutex<()>` for read-modify-write atomicity.
//!
//! **Deletion is deferred** in v0.7.0 (MR-668 scope cut). The registry has
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
use tokio::sync::Mutex;

use crate::identity::GraphKey;
use crate::policy::PolicyEngine;

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
}

/// Immutable snapshot of the registry's current state. Replaced atomically
/// via `ArcSwap`; readers see a consistent view of all graphs without locking.
#[derive(Default)]
pub struct RegistrySnapshot {
    pub graphs: HashMap<GraphKey, Arc<GraphHandle>>,
}

/// Result of a registry lookup. Two-valued — `Tombstoned` deferred with DELETE.
pub enum RegistryLookup {
    /// Graph is open and ready to serve.
    Ready(Arc<GraphHandle>),
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
}

pub struct GraphRegistry {
    snapshot: ArcSwap<RegistrySnapshot>,
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
        let mut graphs: HashMap<GraphKey, Arc<GraphHandle>> = HashMap::with_capacity(handles.len());
        let mut seen_uris: HashMap<String, GraphKey> = HashMap::with_capacity(handles.len());
        for handle in handles {
            if graphs.contains_key(&handle.key) {
                return Err(InsertError::DuplicateKey(handle.key.clone()));
            }
            if let Some(other) = seen_uris.get(&handle.uri) {
                let _ = other; // existing key shown in the error message via uri
                return Err(InsertError::DuplicateUri(handle.uri.clone()));
            }
            seen_uris.insert(handle.uri.clone(), handle.key.clone());
            graphs.insert(handle.key.clone(), handle);
        }
        Ok(Self {
            snapshot: ArcSwap::from_pointee(RegistrySnapshot { graphs }),
            mutate: Mutex::new(()),
        })
    }

    /// Lock-free read. Returns `Ready` if the graph is in the current snapshot,
    /// `Gone` otherwise.
    pub fn get(&self, key: &GraphKey) -> RegistryLookup {
        let snapshot = self.snapshot.load();
        match snapshot.graphs.get(key) {
            Some(handle) => RegistryLookup::Ready(Arc::clone(handle)),
            None => RegistryLookup::Gone,
        }
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
    /// and duplicate `uri`. Currently unused at runtime — only construction
    /// via `from_handles` runs at startup — but kept for the tests that
    /// pin its concurrency contract.
    ///
    /// Race semantics (pinned by `concurrent_insert_same_key_one_succeeds_one_errors`):
    /// under two concurrent calls with the same key, exactly one returns
    /// `Ok(())` and the other returns `Err(InsertError::DuplicateKey(_))`.
    pub async fn insert(&self, handle: Arc<GraphHandle>) -> Result<(), InsertError> {
        let _guard = self.mutate.lock().await;
        let current = self.snapshot.load();
        if current.graphs.contains_key(&handle.key) {
            return Err(InsertError::DuplicateKey(handle.key.clone()));
        }
        for existing in current.graphs.values() {
            if existing.uri == handle.uri {
                return Err(InsertError::DuplicateUri(handle.uri.clone()));
            }
        }
        let mut new_graphs = current.graphs.clone();
        new_graphs.insert(handle.key.clone(), handle);
        self.snapshot
            .store(Arc::new(RegistrySnapshot { graphs: new_graphs }));
        Ok(())
    }
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
        });
        let h2 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: shared_uri,
            engine,
            policy: None,
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
        });
        let h2 = Arc::new(GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("beta").unwrap()),
            uri: shared_uri,
            engine,
            policy: None,
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
                        RegistryLookup::Gone => panic!(
                            "snapshot listed key {} but get() returned Gone",
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
