//! D4: the white-box invariant battery + the structured known/novel classifier.
//!
//! A `Finding` is either an `Engine(OmniError)` (an op/query returned an engine
//! error) or a `Logical(String)` (a harness-computed structural mismatch:
//! HEAD!=manifest, count!=model, orphan edge). Classification is structured —
//! NOT free-text substring matching over arbitrary messages:
//!   * Engine errors are allow-listed only for the two known bugs, each gated on
//!     a narrow signal (RC-1 on the `OmniError::Manifest` variant; RC-X on
//!     Lance's specific internal string).
//!   * Logical findings are NEVER allow-listed — a structural divergence is
//!     always a real finding (the named regressions cover the known concurrency
//!     cases separately).

use lance_table::format::RowIdMeta;
use lance_table::rowids::read_row_ids;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph_compiler::ir::ParamMap;

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

/// Returns `Some(known-bug-name)` if the finding matches a known open bug we
/// allow-list (so the walk explores past it), else `None` (= NOVEL → fail).
pub fn classify(f: &Finding) -> Option<&'static str> {
    match f {
        Finding::Engine(e) => {
            let s = e.to_string();
            // RC-1: stale-view manifest CAS — gated on the Manifest variant so a
            // coincidental substring in another subsystem cannot mask it.
            if matches!(e, OmniError::Manifest(_))
                && (s.contains("stale view") || (s.contains("expected") && s.contains("current")))
            {
                return Some("RC-1 stale-view");
            }
            // RC-X / Lance #7230: scalar-BTREE duplicate row addresses. The Lance
            // internal panic string is highly specific (near-zero false positive).
            if s.contains("from_sorted_iter") || s.contains("non-sorted input") {
                return Some("RC-X/#7230 scalar-BTREE");
            }
            None
        }
        // Structural divergences are never allow-listed.
        Finding::Logical(_) => None,
    }
}

// ── INVARIANT #1: Lance HEAD == manifest table version, per table ──
pub async fn head_eq_manifest(db: &Omnigraph) -> Result<(), Finding> {
    let snap = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(Finding::Engine)?;
    for entry in snap.entries() {
        let ds = snap.open(&entry.table_key).await.map_err(Finding::Engine)?;
        let lance_head = ds.version().version;
        if lance_head != entry.table_version {
            return Err(Finding::Logical(format!(
                "HEAD!=manifest on {}: lance_head={lance_head} manifest_pin={}",
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

// ── INVARIANT #3a: no overlapping stable row-id ranges across fragments ──
// A structural integrity check decoded from each fragment's `row_id_meta`
// (`RowIdMeta::Inline` → `read_row_ids` → `row_id_range`): in a stable-row-id
// dataset every fragment owns a DISJOINT range of stable row ids, so any two
// fragments whose `[start..=end]` ranges intersect is corruption — a double-
// allocated / re-used row-id range. Unlike `index_probe` (which only surfaces a
// bad scalar-BTREE page when a filtered READ loads it), this fires the moment
// the bad fragment metadata is committed, before any read. Reaches Lance only
// through public surfaces: `Snapshot::open → Dataset::fragments()` +
// `lance_table::rowids::read_row_ids`. `RowIdMeta::External` and `None` (legacy
// assign-by-position) fragments carry no inline range and are skipped.
pub async fn no_overlapping_row_ids(db: &Omnigraph) -> Result<(), Finding> {
    let snap = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(Finding::Engine)?;
    for entry in snap.entries() {
        let ds = snap.open(&entry.table_key).await.map_err(Finding::Engine)?;
        // (frag_id, start, end) for every fragment carrying an inline row-id range.
        let mut ranges: Vec<(u64, u64, u64)> = Vec::new();
        for frag in ds.fragments().iter() {
            if let Some(RowIdMeta::Inline(bytes)) = &frag.row_id_meta {
                let seq = read_row_ids(bytes).map_err(|e| {
                    Finding::Logical(format!(
                        "row_id_meta decode failed on {} frag {}: {e}",
                        entry.table_key, frag.id
                    ))
                })?;
                if let Some(r) = seq.row_id_range() {
                    ranges.push((frag.id, *r.start(), *r.end()));
                }
            }
        }
        // Fragment counts per table are small; an O(n²) pairwise scan is fine and
        // reports the exact colliding pair.
        for i in 0..ranges.len() {
            for j in (i + 1)..ranges.len() {
                let (ai, a0, a1) = ranges[i];
                let (bj, b0, b1) = ranges[j];
                if a0 <= b1 && b0 <= a1 {
                    return Err(Finding::Logical(format!(
                        "overlapping stable row-id ranges on {}: frag {ai} [{a0}..={a1}] ∩ frag {bj} [{b0}..={b1}]",
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

/// The full battery as a registry: `(name, result)` per invariant. Adding an
/// invariant is one line here; the walk iterates and the coverage map records.
pub async fn run_battery(db: &Omnigraph, model: &crate::model::Model) -> Vec<(&'static str, Result<(), Finding>)> {
    vec![
        ("head==manifest", head_eq_manifest(db).await),
        ("dataset.validate", dataset_validate(db).await),
        ("row-id-disjoint", no_overlapping_row_ids(db).await),
        ("index-probe", index_probe(db).await),
        ("count==model", crate::model::check_counts(db, model).await),
        ("content==model", crate::model::check_content(db, model).await),
    ]
}
