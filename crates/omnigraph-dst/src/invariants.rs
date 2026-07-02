//! D4: the white-box invariant battery + the structured known/novel classifier.
//!
//! A `Finding` is either an `Engine(OmniError)` (an op/query returned an engine
//! error) or a `Logical(String)` (a harness-computed structural mismatch:
//! HEAD!=manifest, count!=model, orphan edge). Classification is structured —
//! NOT free-text substring matching over arbitrary messages:
//!   * Engine errors are allow-listed only for the two known bugs, each gated on
//!     a narrow signal (RC-1 on the `OmniError::Manifest` variant; RC-X on
//!     Lance's specific internal string).
//!   * Logical findings are NEVER allow-listed except the dup-`@key` marker — a
//!     structural divergence is otherwise always a real finding.
//!
//! The white-box checks take the real `Omnigraph` handle (via `Embedded::db()`),
//! so they are embedded-only; `run_battery` therefore takes `&Embedded`. A
//! cross-backend walk runs the black-box `model::check_*` oracles instead.

use std::collections::HashSet;

use arrow_array::{RecordBatch, UInt64Array};
use futures::TryStreamExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph_compiler::ir::ParamMap;

use crate::backend::{BackendError, Embedded};
use crate::model::Model;

#[derive(Debug)]
pub enum Finding {
    /// An engine-returned error (variant preserved for structured classification).
    Engine(OmniError),
    /// A harness-computed structural violation — always novel.
    Logical(String),
}

impl Finding {
    pub fn message(&self) -> String {
        match self {
            Finding::Engine(e) => e.to_string(),
            Finding::Logical(s) => s.clone(),
        }
    }
}

/// Classify an EMBEDDED engine error on its TYPED shape — never generic
/// substrings. RC-1's write-time surface is a manifest conflict carrying
/// structured `ExpectedVersionMismatch` details; its read-your-drift surface
/// (a later write's precondition refusing uncovered drift) has no structured
/// details yet, so it keeps its one distinctive message. A generic
/// `"expected" + "current"` substring pair would also match novel stale-cache
/// and publisher-CAS bugs and let the harness walk past a real regression.
fn classify_engine_error(e: &OmniError) -> Option<&'static str> {
    if let OmniError::Manifest(m) = e {
        if matches!(
            m.details,
            Some(omnigraph::error::ManifestConflictDetails::ExpectedVersionMismatch { .. })
        ) || m.message.contains("ahead of manifest version")
        {
            return Some("RC-1 stale-view");
        }
    }
    // RC-X / Lance #7230: scalar-BTREE duplicate row addresses. The Lance
    // internal panic string is highly specific (near-zero false positive).
    let s = e.to_string();
    if s.contains("from_sorted_iter") || s.contains("non-sorted input") {
        return Some("RC-X/#7230 scalar-BTREE");
    }
    None
}

/// Returns `Some(known-bug-name)` if the finding matches a known open bug we
/// allow-list (so the walk explores past it), else `None` (= NOVEL → fail).
pub fn classify(f: &Finding) -> Option<&'static str> {
    match f {
        Finding::Engine(e) => classify_engine_error(e),
        // Structural divergences are novel by default, with ONE narrow
        // exception: dup-`@key` (MR-714) is a known open bug.
        Finding::Logical(s) => {
            if s.contains("dup-@key") {
                Some("dup-@key MR-714")
            } else {
                None
            }
        }
    }
}

/// Classify a backend op error the same KNOWN-vs-NOVEL way as `classify`. The
/// embedded arm matches the TYPED error; the CLI arm has only stderr text, so
/// it matches the (distinctive) RC-1/RC-X/dup-@key strings directly —
/// acceptable given how specific those messages are.
pub fn classify_backend(e: &BackendError) -> Option<&'static str> {
    match e {
        BackendError::Engine(oe) => classify_engine_error(oe),
        BackendError::Cli { stderr, .. } => {
            // Out-of-process there's no OmniError variant to gate on, so gate RC-1
            // on its DISTINCTIVE strings only — NOT the generic "expected"+"current"
            // that `classify_engine_signal` allows under is_manifest, which could
            // appear in unrelated CLI errors and mask a novel finding.
            if stderr.contains("stale view") || stderr.contains("ahead of manifest version") {
                Some("RC-1 stale-view")
            } else if stderr.contains("from_sorted_iter") || stderr.contains("non-sorted input") {
                Some("RC-X/#7230 scalar-BTREE")
            } else if stderr.contains("dup-@key") {
                Some("dup-@key MR-714")
            } else {
                None
            }
        }
    }
}

/// Extract a readable message from a caught panic payload.
pub fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Classify a caught panic (a Lance-internal `unwrap`/index crash that unwinds
/// through the engine) the same KNOWN-vs-NOVEL way as `classify`. Under fault
/// injection and at walk depth the substrate WILL crash, so the harness must
/// treat a panic as a finding, not let it abort the suite. NONE → re-raise.
///
/// Note on `index out of bounds`: in THIS harness the only source of that panic
/// is Lance's inverted (FTS) index builder — the harness's own code uses checked
/// loops and returns `Logical` findings, never an OOB panic — so the broad match
/// is safe here and would not mask a harness bug.
pub fn classify_panic(msg: &str) -> Option<&'static str> {
    if msg.contains("from_sorted_iter") || msg.contains("non-sorted input") {
        return Some("RC-X/#7230 scalar-BTREE");
    }
    if msg.contains("index out of bounds") {
        return Some("Lance FTS inverted-builder OOB");
    }
    None
}

// ── INVARIANT #1: Lance HEAD == manifest table version, per table ──
//
// The HEAD must be resolved with a RAW latest-open of the table's location —
// NOT `Snapshot::open`, which opens at the manifest-PINNED version and would
// make this check compare the pin to itself (a tautology blind to exactly the
// HEAD>manifest uncovered-drift class it exists to catch).
pub async fn head_eq_manifest(db: &Omnigraph) -> Result<(), Finding> {
    let snap = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(Finding::Engine)?;
    let root = db.uri().trim_end_matches('/');
    for entry in snap.entries() {
        // Branch-owned tables live under `tree/<branch>`; mirror the engine's
        // location join for the raw open.
        let mut location = format!("{root}/{}", entry.table_path);
        if let Some(branch) = entry.table_branch.as_deref() {
            if branch != "main" {
                location = format!("{location}/tree/{branch}");
            }
        }
        let ds = lance::Dataset::open(&location)
            .await
            .map_err(|e| Finding::Logical(format!("open {} at HEAD: {e}", entry.table_key)))?;
        let lance_head = ds.version().version;
        if lance_head > entry.table_version {
            return Err(Finding::Logical(format!(
                "uncovered drift on {}: lance HEAD {lance_head} ahead of manifest pin {}",
                entry.table_key, entry.table_version
            )));
        }
        if lance_head < entry.table_version {
            return Err(Finding::Logical(format!(
                "corruption on {}: lance HEAD {lance_head} BEHIND manifest pin {} \
                 (the manifest references a version that does not exist)",
                entry.table_key, entry.table_version
            )));
        }
    }
    Ok(())
}

// ── INVARIANT #2: Lance Dataset::validate() per table (general corruption) ──
pub async fn dataset_validate(db: &Omnigraph) -> Result<(), Finding> {
    let snap = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(Finding::Engine)?;
    for entry in snap.entries() {
        let ds = snap.open(&entry.table_key).await.map_err(Finding::Engine)?;
        // Dataset::validate returns a Lance error; a validation failure IS a
        // structural corruption finding (always novel).
        ds.validate()
            .await
            .map_err(|e| Finding::Logical(format!("Dataset::validate {}: {e}", entry.table_key)))?;
    }
    Ok(())
}

// ── INVARIANT #3a: no duplicate LIVE stable row-id within a table ──
// A stable row id uniquely identifies one live row for the dataset's lifetime;
// the same id appearing on two live rows is exactly the duplicate-row-address
// corruption class behind RC-X / Lance #7230. We read the truth deletion-vector-
// correctly by scanning each table with `with_row_id()` (Lance's `_rowid`
// projection returns only LIVE rows, so a tombstoned id masked by an UPDATE or
// compaction never counts) and asserting the live `_rowid`s are unique. Unlike
// `index_probe` (which only surfaces a bad scalar-BTREE page when a filtered
// READ loads it), this is a direct structural check on every committed row.
pub async fn no_duplicate_live_row_ids(db: &Omnigraph) -> Result<(), Finding> {
    let snap = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(Finding::Engine)?;
    for entry in snap.entries() {
        let ds = snap.open(&entry.table_key).await.map_err(Finding::Engine)?;
        let mut scanner = ds.scan();
        scanner.with_row_id();
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(|e| Finding::Logical(format!("scan {}: {e}", entry.table_key)))?
            .try_collect()
            .await
            .map_err(|e| Finding::Logical(format!("scan collect {}: {e}", entry.table_key)))?;
        let mut seen: HashSet<u64> = HashSet::new();
        for batch in &batches {
            let col = batch
                .column_by_name("_rowid")
                .ok_or_else(|| Finding::Logical(format!("no _rowid column on {}", entry.table_key)))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| Finding::Logical(format!("_rowid not u64 on {}", entry.table_key)))?;
            for i in 0..col.len() {
                let id = col.value(i);
                if !seen.insert(id) {
                    return Err(Finding::Logical(format!(
                        "duplicate live stable row-id {id} in {} (RC-X-class duplicate row address)",
                        entry.table_key
                    )));
                }
            }
        }
    }
    Ok(())
}

// ── INVARIANT #3: scalar-index probe (catches RC-X at creation) ──
// Force-loads the Doc.source BTREE flat pages by filtering each enum value.
pub async fn index_probe(db: &Omnigraph) -> Result<(), Finding> {
    for src in ["whatsapp", "email", "linkedin", "slack", "telegram"] {
        let q = format!("query w() {{ match {{ $d: Doc {{ source: \"{src}\" }} }} return {{ $d.slug }} }}");
        db.query(ReadTarget::branch("main"), &q, "w", &ParamMap::new())
            .await
            .map_err(Finding::Engine)?;
    }
    Ok(())
}

// ── @key uniqueness (no sequential model — for the concurrent oracle) ──
// Every `@key` value may appear on at most one live row. Concurrent same-key
// upserts that produce duplicates are MR-714 (dup-`@key`); `classify` allow-
// lists the `dup-@key` marker as that known bug.
pub async fn no_duplicate_keys(db: &Omnigraph, ty: &str, branch: &str) -> Result<(), Finding> {
    let q = format!("query q() {{ match {{ $x: {ty} }} return {{ $x.slug }} }}");
    let res = db
        .query(ReadTarget::branch(branch), &q, "q", &ParamMap::new())
        .await
        .map_err(Finding::Engine)?;
    let json = res.to_rust_json();
    let rows = json
        .as_array()
        .ok_or_else(|| Finding::Logical(format!("{ty} key scan not an array")))?;
    let total = rows.len();
    let mut seen: HashSet<String> = HashSet::new();
    for row in rows {
        let slug = row["x.slug"]
            .as_str()
            .ok_or_else(|| Finding::Logical(format!("missing x.slug in {ty}")))?;
        if !seen.insert(slug.to_string()) {
            return Err(Finding::Logical(format!(
                "duplicate @key {slug:?} in {ty} ({total} rows, {} distinct) — dup-@key",
                seen.len()
            )));
        }
    }
    Ok(())
}

/// The full battery as a registry: `(name, result)` per invariant. Adding an
/// invariant is one line here; the walk iterates and the coverage map records.
/// White-box checks use `emb.db()`; the count/content/edges oracles run through
/// the same embedded backend (so they share the handle's view).
pub async fn run_battery(emb: &Embedded, model: &Model) -> Vec<(&'static str, Result<(), Finding>)> {
    let db = emb.db();
    vec![
        ("head==manifest", head_eq_manifest(db).await),
        ("dataset.validate", dataset_validate(db).await),
        ("row-id-unique", no_duplicate_live_row_ids(db).await),
        ("index-probe", index_probe(db).await),
        ("count==model", crate::model::check_counts(emb, model).await),
        ("content==model", crate::model::check_content(emb, model).await),
        ("edges==model", crate::model::check_edges(emb, model).await),
    ]
}
