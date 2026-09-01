use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use lance::Dataset;
use lance::session::Session;
use tokio::sync::Mutex;

use crate::db::{ResolvedTarget, Snapshot};
use crate::error::Result;
use crate::graph_index::{GraphIndex, persist};

/// Cache key for a built `GraphIndex`. Keyed (A1) by the physical identity of the
/// edge tables the topology is derived from, NOT by the resolved snapshot id. The
/// topology is a pure function of the edge tables' `src`/`dst`, so two snapshots
/// (e.g. main and a lazy-fork branch whose edge tables physically *are* main's)
/// with identical edge tables share one built index: a fresh branch reuses main's
/// instead of rebuilding it from a cold scan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GraphIndexCacheKey {
    edge_tables: Vec<GraphIndexTableState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GraphIndexTableState {
    /// Logical table lifetime. This closes local-filesystem same-name
    /// reincarnation ABA even when Lance version and e_tag are unavailable or
    /// repeat after drop/re-add.
    identity: crate::db::manifest::TableIdentity,
    table_key: String,
    table_version: u64,
    table_branch: Option<String>,
    /// Lance manifest incarnation token for this edge table version. Preserves the
    /// incarnation distinction the dropped synthetic snapshot id used to carry: a
    /// branch deleted and recreated at the same version number gets a new e_tag, so
    /// the cache rebuilds instead of serving stale topology. `None` only on stores
    /// without e_tags (local FS); there a same-branch manifest refresh clears the
    /// cache as the fallback (the read-path gap in docs/dev/invariants.md).
    e_tag: Option<String>,
    /// The edge's `(from_type, to_type)` endpoint names at build time. `GraphIndex`
    /// keys its `TypeIndex`es by these, and `execute_expand_csr` looks them up by
    /// the *current* catalog's endpoint names — so a schema change that repoints an
    /// edge type while leaving the edge table's physical identity unchanged must
    /// invalidate the entry (else the reused index has the old type-index namespace
    /// and the new traversal fails with "no type index for <new type>").
    endpoints: (String, String),
}

#[derive(Debug, Default)]
pub struct RuntimeCache {
    graph_indices: Mutex<GraphIndexCache>,
    /// Single-flight gate for the persisted-artifact decode: simultaneous
    /// misses serialize here, so on the fresh-artifact path the full-catalog
    /// GET + decode runs once and waiters are served from the shared decode
    /// by the post-acquisition re-check. A FAILED attempt records its scope
    /// key in `artifact_negative`, so a stale/absent artifact costs one gated
    /// GET per distinct key — never a repeated queue of doomed downloads —
    /// and later misses for that key go straight to the scan build. Scan
    /// builds themselves run outside this gate.
    artifact_admission: Mutex<()>,
}

#[derive(Debug)]
struct GraphIndexCache {
    entries: LruMap<GraphIndexCacheKey, Arc<GraphIndex>>,
    /// The shared full-catalog decode slot ("shelf"): the most recent
    /// artifact decode, keyed by its identity stamps and Arc-shared into
    /// every scope entry it can serve — N scoped misses cost one decode and
    /// one allocation instead of N full copies (the LRU could otherwise pin
    /// `capacity` complete catalogs). Freshness stays per-request:
    /// `shelf_serve` re-verifies every requested edge's current stamp, so a
    /// written edge stops the slot serving scopes that touch it while
    /// untouched scopes keep hitting. Retention: replaced only by a newer
    /// successful decode or `invalidate_all` — if edges keep changing and no
    /// fresh artifact ever loads, the slot pins one stale catalog decode for
    /// the handle's lifetime (deliberate; bounded at one).
    shelf: Option<(Vec<persist::TableStamp>, Arc<GraphIndex>)>,
    /// Scope keys whose artifact load attempt FAILED (absent/stale/corrupt
    /// for exactly those stamps). A key embeds its edge stamps, so the
    /// verdict cannot go stale: new stamps make a new key. Cleared when a
    /// new decode lands (a fresh artifact may serve previously-doomed keys)
    /// and on `invalidate_all`.
    artifact_negative: LruMap<GraphIndexCacheKey, ()>,
}

impl RuntimeCache {
    /// Note on in-flight loaders: an artifact decode already running under
    /// `artifact_admission` can repopulate the shelf and one entry AFTER this
    /// returns. That is safe, not racy: every serve re-verifies stamps
    /// against the caller's CURRENT snapshot, and entry keys embed stamps —
    /// stale state parked by a straggler is simply never served.
    pub async fn invalidate_all(&self) {
        let mut cache = self.graph_indices.lock().await;
        cache.entries.invalidate_all();
        cache.shelf = None;
        cache.artifact_negative.invalidate_all();
    }

    /// Build (or fetch) the CSR/CSC graph index scoped to exactly `edge_types` —
    /// the edge types the query actually traverses, not every edge type in the
    /// catalog. Scoping is what keeps a single-edge join (`$x identifiesPerson
    /// $p`) from scanning the whole graph's edge data; the cache key carries the
    /// scoped set, so a `{Knows}` index and a `{Knows, WorksAt}` index are
    /// distinct entries and never serve each other.
    pub async fn graph_index(
        &self,
        resolved: &ResolvedTarget,
        edge_types: &HashMap<String, (String, String)>,
        adapter: &dyn crate::storage::StorageAdapter,
    ) -> Result<Arc<GraphIndex>> {
        let key = graph_index_cache_key(resolved, edge_types);
        {
            let mut cache = self.graph_indices.lock().await;
            if let Some(index) = cache.entries.get(&key).cloned() {
                return Ok(index);
            }
            if let Some(index) = cache.shelf_serve(&resolved.snapshot, edge_types) {
                cache.insert(key, Arc::clone(&index));
                return Ok(index);
            }
        }

        // Miss for both the scope entry and the shelf: try the persisted
        // artifact once, single-flight, so concurrent misses never duplicate
        // the full-catalog decode. A key whose attempt already failed skips
        // the gate entirely (the key embeds the stamps, so the failure
        // verdict holds until the stamps move or a fresh decode lands).
        let known_doomed = {
            let mut cache = self.graph_indices.lock().await;
            cache.artifact_negative.get(&key).is_some()
        };
        if !known_doomed {
            let _admission = self.artifact_admission.lock().await;
            let doomed_while_waiting = {
                let mut cache = self.graph_indices.lock().await;
                if let Some(index) = cache.entries.get(&key).cloned() {
                    return Ok(index);
                }
                if let Some(index) = cache.shelf_serve(&resolved.snapshot, edge_types) {
                    cache.insert(key.clone(), Arc::clone(&index));
                    return Ok(index);
                }
                // A same-key waiter whose winner just FAILED shares that
                // verdict instead of repeating the doomed GET.
                cache.artifact_negative.get(&key).is_some()
            };
            if doomed_while_waiting {
                // fall through to the scan build below
            } else {
                match GraphIndex::load_persisted(&resolved.snapshot, edge_types, Some(adapter))
                    .await
                {
                    Some((index, stamps)) => {
                        let index = Arc::new(index);
                        let mut cache = self.graph_indices.lock().await;
                        cache.shelf = Some((stamps, Arc::clone(&index)));
                        cache.artifact_negative.invalidate_all();
                        cache.insert(key, Arc::clone(&index));
                        return Ok(index);
                    }
                    None => {
                        let mut cache = self.graph_indices.lock().await;
                        cache.artifact_negative.insert(key.clone(), ());
                    }
                }
            }
        }

        // No usable artifact: scan-build the SCOPED index (small, never
        // shelved), outside the admission gate so unrelated scopes still
        // build concurrently. The graph-build probe fires inside
        // `GraphIndex::build` itself, so a persisted-artifact load is never
        // counted as a build.
        let index = Arc::new(GraphIndex::build(&resolved.snapshot, edge_types).await?);
        let mut cache = self.graph_indices.lock().await;
        if let Some(existing) = cache.entries.get(&key).cloned() {
            return Ok(existing);
        }
        cache.insert(key, Arc::clone(&index));
        Ok(index)
    }
}

impl GraphIndexCache {
    fn insert(&mut self, key: GraphIndexCacheKey, value: Arc<GraphIndex>) {
        self.entries.insert(key, value);
    }

    /// Serve a scope from the shelved full-catalog decode when every
    /// requested edge's CURRENT stamp matches the shelf's identity. A stale
    /// or uncovered edge returns `None` (that request re-admits); the shelf
    /// itself is kept — other scopes not touching the written edge stay
    /// fresh against it.
    fn shelf_serve(
        &self,
        snapshot: &Snapshot,
        edge_types: &HashMap<String, (String, String)>,
    ) -> Option<Arc<GraphIndex>> {
        // An empty scope would match vacuously (`all` over nothing); refuse
        // rather than hand out the full catalog with zero verification.
        if edge_types.is_empty() {
            return None;
        }
        let (stamps, index) = self.shelf.as_ref()?;
        persist::stamps_cover_and_match(snapshot, edge_types, stamps).then(|| Arc::clone(index))
    }

    #[cfg(test)]
    fn touch(&mut self, key: GraphIndexCacheKey) {
        self.entries.touch(key);
    }
}

#[derive(Debug)]
struct LruMap<K, V>
where
    K: Clone + Eq + Hash,
{
    entries: HashMap<K, V>,
    lru: VecDeque<K>,
    cap: usize,
}

impl<K, V> LruMap<K, V>
where
    K: Clone + Eq + Hash,
{
    fn new(cap: usize) -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            cap,
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if self.entries.contains_key(key) {
            self.touch(key.clone());
            self.entries.get(key)
        } else {
            None
        }
    }

    fn insert(&mut self, key: K, value: V) {
        self.entries.insert(key.clone(), value);
        self.touch(key);
        while self.entries.len() > self.cap {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }

    fn invalidate_all(&mut self) {
        self.entries.clear();
        self.lru.clear();
    }

    #[cfg(test)]
    fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn touch(&mut self, key: K) {
        self.lru.retain(|existing| existing != &key);
        self.lru.push_back(key);
    }
}

impl Default for GraphIndexCache {
    fn default() -> Self {
        Self {
            entries: LruMap::new(8),
            shelf: None,
            artifact_negative: LruMap::new(32),
        }
    }
}

fn graph_index_cache_key(
    resolved: &ResolvedTarget,
    edge_types: &HashMap<String, (String, String)>,
) -> GraphIndexCacheKey {
    let mut edge_tables: Vec<GraphIndexTableState> = edge_types
        .iter()
        .filter_map(|(edge_name, endpoints)| {
            let table_key = format!("edge:{}", edge_name);
            resolved
                .snapshot
                .dataset(&table_key)
                .map(|entry| GraphIndexTableState {
                    identity: entry.identity,
                    table_key,
                    table_version: entry.published_dataset_version,
                    table_branch: entry.native_dataset_branch.clone(),
                    e_tag: entry.version_metadata.e_tag().map(str::to_string),
                    endpoints: endpoints.clone(),
                })
        })
        .collect();
    edge_tables.sort_by(|a, b| a.table_key.cmp(&b.table_key));

    GraphIndexCacheKey { edge_tables }
}

/// Max held `Dataset` handles. A handle holds only Arcs (object store + manifest),
/// never table data, so this is cheap; it bounds how many `(table, branch,
/// version, e_tag)` cells stay warm. One graph's live table set across a couple
/// of branches at the current version fits comfortably, with headroom for the
/// recently-superseded versions left by writes until they age out.
const TABLE_HANDLE_CACHE_CAP: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableHandleKey {
    table_path: String,
    table_branch: Option<String>,
    version: u64,
    e_tag: Option<String>,
}

/// Held open-`Dataset` handles keyed by `(table_path, branch, version, e_tag)` — the
/// version-keyed analogue of LanceDB's `DatasetConsistencyWrapper`
/// (`rust/lancedb/src/table/dataset.rs`). A warm read reuses a held handle with
/// zero open IO (a cheap `Dataset` clone); a miss opens once at the location with
/// the shared `Session`. Version plus e_tag are in the key, so a write (or a
/// delete/recreate that reuses a version number on object stores with e_tags) is
/// simply a new key. A same-branch manifest refresh clears this cache as the
/// fallback for e_tag-less table locations. Only read-path Data opens use this —
/// writes open HEAD directly and never receive a pinned handle.
#[derive(Default)]
pub struct TableHandleCache {
    inner: Mutex<TableHandleCacheInner>,
}

struct TableHandleCacheInner {
    entries: LruMap<TableHandleKey, Dataset>,
}

impl TableHandleCache {
    /// Drop all held handles. Correctness never requires this (version-in-key);
    /// it is memory hygiene, called from the same hooks that clear the graph
    /// index cache (branch switch / refresh).
    pub async fn invalidate_all(&self) {
        let mut inner = self.inner.lock().await;
        inner.entries.invalidate_all();
    }

    /// Return the dataset for `(dataset_path, branch, version, e_tag)`, reusing a
    /// held handle (0 open IO) or opening it once at `location` with the shared
    /// `session` on a miss.
    pub async fn get_or_open(
        &self,
        dataset_path: &str,
        table_branch: Option<&str>,
        version: u64,
        e_tag: Option<&str>,
        location: &str,
        session: Option<&Arc<Session>>,
    ) -> Result<Dataset> {
        let key = TableHandleKey {
            table_path: dataset_path.to_string(),
            table_branch: table_branch.map(str::to_string),
            version,
            e_tag: e_tag.map(str::to_string),
        };
        {
            let mut inner = self.inner.lock().await;
            if let Some(ds) = inner.entries.get(&key).cloned() {
                return Ok(ds);
            }
        }
        // Miss: open without holding the lock (the open is async IO). A concurrent
        // double-miss opens twice and one wins the insert — correct (the dataset
        // at a version is immutable) and rare.
        let ds = crate::instrumentation::open_dataset(
            location,
            crate::instrumentation::VersionResolution::At(version),
            session,
            crate::instrumentation::table_wrapper(),
        )
        .await?;
        let mut inner = self.inner.lock().await;
        if let Some(existing) = inner.entries.get(&key).cloned() {
            return Ok(existing);
        }
        inner.insert(key, ds.clone());
        Ok(ds)
    }
}

impl TableHandleCacheInner {
    fn insert(&mut self, key: TableHandleKey, value: Dataset) {
        self.entries.insert(key, value);
    }
}

impl Default for TableHandleCacheInner {
    fn default() -> Self {
        Self {
            entries: LruMap::new(TABLE_HANDLE_CACHE_CAP),
        }
    }
}

/// Per-graph read caches handed to a resolved `Snapshot` so its table opens reuse
/// one shared `Session` (LanceDB's one-session-per-connection pattern) and the
/// held-handle cache. Manual `Debug` because `lance::session::Session` is not
/// `Debug`; this lets `Snapshot` keep its `#[derive(Debug)]`.
pub struct ReadCaches {
    pub session: Arc<Session>,
    pub handles: Arc<TableHandleCache>,
}

impl std::fmt::Debug for ReadCaches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadCaches").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn key(id: usize) -> GraphIndexCacheKey {
        // Distinct keys via a distinct edge table per id (the key no longer carries
        // a snapshot id — it is the physical edge-table identity set, A1).
        GraphIndexCacheKey {
            edge_tables: vec![GraphIndexTableState {
                identity: crate::db::manifest::TableIdentity::new(id as u64 + 1, 1).unwrap(),
                table_key: format!("edge:t{id}"),
                table_version: 1,
                table_branch: None,
                e_tag: None,
                endpoints: ("A".to_string(), "B".to_string()),
            }],
        }
    }

    fn empty_index() -> Arc<GraphIndex> {
        Arc::new(GraphIndex::empty_for_test())
    }

    /// An edge table at the same physical identity but a different `(from_type,
    /// to_type)` endpoint mapping (a schema repoint) must NOT share a cache entry
    /// — the built index's `TypeIndex` namespace is keyed by those endpoints.
    #[test]
    fn endpoint_remap_at_same_physical_identity_splits_cache_key() {
        let base = GraphIndexTableState {
            identity: crate::db::manifest::TableIdentity::new(1, 2).unwrap(),
            table_key: "edge:Knows".to_string(),
            table_version: 7,
            table_branch: None,
            e_tag: Some("etag".to_string()),
            endpoints: ("Person".to_string(), "Person".to_string()),
        };
        let repointed = GraphIndexTableState {
            endpoints: ("Person".to_string(), "Account".to_string()),
            ..base.clone()
        };
        let k_old = GraphIndexCacheKey {
            edge_tables: vec![base],
        };
        let k_new = GraphIndexCacheKey {
            edge_tables: vec![repointed],
        };
        assert_ne!(
            k_old, k_new,
            "a schema endpoint remap must produce a distinct graph-index cache key"
        );
    }

    #[test]
    fn table_reincarnation_splits_graph_index_cache_key_without_etag_help() {
        let old_lifetime = GraphIndexTableState {
            identity: crate::db::manifest::TableIdentity::new(1, 2).unwrap(),
            table_key: "edge:Knows".to_string(),
            table_version: 1,
            table_branch: None,
            e_tag: None,
            endpoints: ("Person".to_string(), "Person".to_string()),
        };
        let new_lifetime = GraphIndexTableState {
            identity: crate::db::manifest::TableIdentity::new(1, 3).unwrap(),
            ..old_lifetime.clone()
        };
        let old_key = GraphIndexCacheKey {
            edge_tables: vec![old_lifetime],
        };
        let new_key = GraphIndexCacheKey {
            edge_tables: vec![new_lifetime],
        };

        let mut cache = GraphIndexCache::default();
        cache.insert(old_key.clone(), empty_index());
        cache.insert(new_key.clone(), empty_index());

        assert_ne!(old_key, new_key);
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.entries.contains_key(&old_key));
        assert!(cache.entries.contains_key(&new_key));
    }

    #[test]
    fn graph_index_cache_evicts_oldest_entry() {
        let mut cache = GraphIndexCache::default();
        for idx in 0..9 {
            cache.insert(key(idx), empty_index());
        }

        assert_eq!(cache.entries.len(), 8);
        assert!(!cache.entries.contains_key(&key(0)));
        assert!(cache.entries.contains_key(&key(8)));
    }

    #[test]
    fn graph_index_cache_touch_keeps_recent_entry() {
        let mut cache = GraphIndexCache::default();
        for idx in 0..8 {
            cache.insert(key(idx), empty_index());
        }

        cache.touch(key(0));
        cache.insert(key(8), empty_index());

        assert!(cache.entries.contains_key(&key(0)));
        assert!(!cache.entries.contains_key(&key(1)));
    }

    #[test]
    fn lru_map_evicts_oldest_and_touch_refreshes_order() {
        let mut map = LruMap::new(2);
        map.insert("a", 1);
        map.insert("b", 2);

        assert_eq!(map.get(&"a"), Some(&1));
        map.insert("c", 3);

        assert!(map.contains_key(&"a"));
        assert!(!map.contains_key(&"b"));
        assert!(map.contains_key(&"c"));

        map.invalidate_all();
        assert_eq!(map.len(), 0);
    }

    /// PR #544 review finding 3 regression: two different edge scopes served
    /// from the persisted artifact must share ONE decoded allocation (the
    /// shelf), never retain independent full-catalog copies per scope key.
    #[tokio::test]
    async fn scoped_requests_share_one_persisted_artifact_decode() {
        const SCHEMA: &str = r#"
node Person { name: String @key }
edge Knows: Person -> Person {}
edge Likes: Person -> Person {}
"#;
        const DATA: &str = r#"{"type":"Person","data":{"name":"a"}}
{"type":"Person","data":{"name":"b"}}
{"edge":"Knows","from":"a","to":"b"}
{"edge":"Likes","from":"b","to":"a"}"#;

        let dir = tempfile::tempdir().unwrap();
        let db = crate::db::Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        crate::loader::load_jsonl(&db, DATA, crate::loader::LoadMode::Overwrite)
            .await
            .unwrap();
        db.optimize().await.unwrap();
        // Fresh handle: cold in-memory caches, the artifact stays on the store.
        let db = crate::db::Omnigraph::open(dir.path().to_str().unwrap())
            .await
            .unwrap();

        let (resolved, catalog) = db.capture_current_read_view().await.unwrap();
        let scope = |edge: &str| {
            let et = &catalog.edge_types[edge];
            HashMap::from([(edge.to_string(), (et.from_type.clone(), et.to_type.clone()))])
        };
        let knows = db
            .graph_index_for_resolved(&resolved, &scope("Knows"))
            .await
            .unwrap();
        let likes = db
            .graph_index_for_resolved(&resolved, &scope("Likes"))
            .await
            .unwrap();
        assert!(
            Arc::ptr_eq(&knows, &likes),
            "scoped requests must share the shelved full-catalog decode"
        );
        // Sanity that the shared index really is the artifact's full catalog.
        assert!(knows.csr("Knows").is_some() && knows.csr("Likes").is_some());
    }
}
