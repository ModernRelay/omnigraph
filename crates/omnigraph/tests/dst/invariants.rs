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
        ("index-probe", index_probe(db).await),
        ("count==model", crate::model::check_counts(db, model).await),
    ]
}
