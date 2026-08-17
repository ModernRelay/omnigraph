//! BENCH HARNESS v1 (0006 `bench_harness/`, design authority RFC-031) —
//! the COUNTING PASS: deterministic seam-event tallies, per op kind, both
//! storage realms. The profile-independent half of benchmarking: the sim
//! cannot measure wall clock, but it counts storage actions EXACTLY, so
//! "merge's PUT count went 12 -> 17" becomes a named regression in a
//! golden diff instead of a number nobody re-checks
//! (`QueryIoProbes`/`assert_flat` lineage engine-side).
//!
//! Slot-armed and default-None: zero draws, zero behavior change, near
//! zero cost for every other test. Verbs are prefixed by realm —
//! `a.` = adapter (manifest/refs/sidecars via `StorageAdapter`),
//! `l.` = Lance (table data/manifests via the provider shim). Labels are
//! the universe's op kinds plus harness phases (`_setup`, `_verify`,
//! `_history`, `_close`, `_audit`) so oracle reads never pollute op costs.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct CostLedger {
    label: Mutex<String>,
    /// (label, verb) -> (calls, bytes).
    #[allow(clippy::type_complexity)]
    rows: Mutex<BTreeMap<(String, &'static str), (u64, u64)>>,
}

impl CostLedger {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            label: Mutex::new("_setup".to_string()),
            rows: Mutex::new(BTreeMap::new()),
        })
    }
    /// The GOLDEN surface: call counts only — measured to be exactly
    /// deterministic. Byte totals are excluded because Lance-internal
    /// wall-clock timestamps are embedded in file bytes (varint-encoded,
    /// so their LENGTH wobbles run-to-run by a few dozen bytes per
    /// category) — the known `mock_instant` upstream gap, now visible in
    /// the cost data itself.
    pub fn render_calls(&self) -> String {
        let rows = self.rows.lock().unwrap();
        let mut out = String::new();
        for ((label, verb), (calls, _)) in rows.iter() {
            out.push_str(&format!("{label:<28} {verb:<12} calls={calls}\n"));
        }
        out
    }
    /// The full table incl. byte totals (informational — bytes carry the
    /// timestamp-varint wobble, see `render_calls`).
    pub fn render(&self) -> String {
        let rows = self.rows.lock().unwrap();
        let mut out = String::new();
        for ((label, verb), (calls, bytes)) in rows.iter() {
            out.push_str(&format!(
                "{label:<28} {verb:<12} calls={calls:<5} bytes~{bytes}\n"
            ));
        }
        out
    }
}

static LEDGER: RwLock<Option<Arc<CostLedger>>> = RwLock::new(None);

/// Arm a fresh ledger (the instrument holds the Arc; disarm when done).
pub fn arm() -> Arc<CostLedger> {
    let ledger = CostLedger::new();
    *LEDGER.write().unwrap() = Some(ledger.clone());
    ledger
}

pub fn disarm() {
    *LEDGER.write().unwrap() = None;
}

/// Switch the attribution label (op kind or harness phase).
pub fn set_label(label: &str) {
    if let Some(ledger) = LEDGER.read().unwrap().as_ref() {
        *ledger.label.lock().unwrap() = label.to_string();
    }
}

/// The op-kind label: the first token of a value's Debug rendering
/// ("InsertV", "BranchMerge", ...). For `WorldOp::Data` pass the inner op.
pub fn debug_head<T: std::fmt::Debug>(v: &T) -> String {
    let s = format!("{v:?}");
    let head = s.split([' ', '{', '(']).next().unwrap_or("op");
    // Data-wrapped ops render as `Data`; recover the inner kind from the
    // `op:` field's first token instead.
    if head == "Data"
        && let Some(inner) = s.split("op: ").nth(1)
    {
        return inner
            .split([' ', '{', '(', ',', '}'])
            .next()
            .unwrap_or("Data")
            .to_string();
    }
    head.to_string()
}

/// Tally one storage action under the current label.
pub(crate) fn tally(verb: &'static str, bytes: u64) {
    if let Some(ledger) = LEDGER.read().unwrap().as_ref() {
        let label = ledger.label.lock().unwrap().clone();
        let mut rows = ledger.rows.lock().unwrap();
        let entry = rows.entry((label, verb)).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += bytes;
    }
}

/// Is a ledger armed? `run_universe` consults this to insert the
/// `CostStorage` wrapper and interpose the Lance provider.
pub fn armed() -> bool {
    LEDGER.read().unwrap().is_some()
}

/// The adapter-realm counting wrapper: innermost (under fault/kill
/// wrappers, over the real store), inserted by `run_universe` only when a
/// ledger is armed — so counting sees exactly the calls that reach the
/// store, once, with real payload sizes in both directions.
pub struct CostStorage {
    inner: std::sync::Arc<dyn omnigraph::storage::StorageAdapter>,
}

impl std::fmt::Debug for CostStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CostStorage")
    }
}

impl CostStorage {
    pub fn new(inner: std::sync::Arc<dyn omnigraph::storage::StorageAdapter>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl omnigraph::storage::StorageAdapter for CostStorage {
    async fn read_text(&self, uri: &str) -> omnigraph::error::Result<String> {
        let out = self.inner.read_text(uri).await;
        tally("a.get", out.as_ref().map(|s| s.len() as u64).unwrap_or(0));
        out
    }
    async fn read_text_if_exists(&self, uri: &str) -> omnigraph::error::Result<Option<String>> {
        let out = self.inner.read_text_if_exists(uri).await;
        tally(
            "a.get",
            out.as_ref()
                .ok()
                .and_then(|o| o.as_ref())
                .map(|s| s.len() as u64)
                .unwrap_or(0),
        );
        out
    }
    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> omnigraph::error::Result<Option<String>> {
        let out = self.inner.read_text_if_exists_bounded(uri, max_bytes).await;
        tally(
            "a.get",
            out.as_ref()
                .ok()
                .and_then(|o| o.as_ref())
                .map(|s| s.len() as u64)
                .unwrap_or(0),
        );
        out
    }
    async fn write_text(&self, uri: &str, contents: &str) -> omnigraph::error::Result<()> {
        tally("a.put", contents.len() as u64);
        self.inner.write_text(uri, contents).await
    }
    async fn write_text_if_absent(
        &self,
        uri: &str,
        contents: &str,
    ) -> omnigraph::error::Result<bool> {
        tally("a.put_if_absent", contents.len() as u64);
        self.inner.write_text_if_absent(uri, contents).await
    }
    async fn exists(&self, uri: &str) -> omnigraph::error::Result<bool> {
        tally("a.exists", 0);
        self.inner.exists(uri).await
    }
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> omnigraph::error::Result<()> {
        tally("a.rename", 0);
        self.inner.rename_text(from_uri, to_uri).await
    }
    async fn delete(&self, uri: &str) -> omnigraph::error::Result<()> {
        tally("a.delete", 0);
        self.inner.delete(uri).await
    }
    async fn list_dir(&self, dir_uri: &str) -> omnigraph::error::Result<Vec<String>> {
        tally("a.list", 0);
        self.inner.list_dir(dir_uri).await
    }
    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: omnigraph::storage::ListDirBounds,
    ) -> omnigraph::error::Result<Vec<String>> {
        tally("a.list", 0);
        self.inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await
    }
    async fn read_text_versioned(&self, uri: &str) -> omnigraph::error::Result<(String, String)> {
        let out = self.inner.read_text_versioned(uri).await;
        tally(
            "a.get",
            out.as_ref().map(|(s, _)| s.len() as u64).unwrap_or(0),
        );
        out
    }
    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> omnigraph::error::Result<Option<String>> {
        tally("a.cas", contents.len() as u64);
        self.inner
            .write_text_if_match(uri, contents, expected_version)
            .await
    }
    async fn delete_prefix(&self, prefix_uri: &str) -> omnigraph::error::Result<()> {
        tally("a.delete_prefix", 0);
        self.inner.delete_prefix(prefix_uri).await
    }
}
