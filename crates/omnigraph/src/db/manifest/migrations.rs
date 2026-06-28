//! Internal schema versioning for the `__manifest` Lance dataset.
//!
//! ## Why this exists
//!
//! The on-disk shape of `__manifest` evolves alongside the engine. This module
//! is the *single* place where on-disk shape is reconciled with what the binary
//! expects:
//!
//! - One constant `INTERNAL_MANIFEST_SCHEMA_VERSION` declares the shape this
//!   binary writes.
//! - One stamp `omnigraph:internal_schema_version` in the manifest dataset's
//!   schema-level metadata records the on-disk shape.
//! - One guard `refuse_if_stamp_unsupported` rejects any graph this binary
//!   cannot serve — in either direction — with a clear, actionable error.
//!
//! ## Single-version contract (strand + export/import)
//!
//! This binary reads exactly ONE internal-schema version (`MIN_SUPPORTED ==
//! CURRENT`). There is no in-place migration: a graph stamped below CURRENT is
//! refused on open with a "rebuild via `omnigraph export` + `init`/`load`"
//! message, not silently upgraded. This is the deliberate pre-release contract —
//! storage-format changes are a cutover, not a rolling in-place migration (see
//! `docs/user/operations/upgrade.md` and the versioning policy in `docs/dev`).
//! `stamp_current_version` stamps fresh graphs at CURRENT, so newly initialized
//! graphs always pass.
//!
//! ## If an in-place migration is ever needed
//!
//! The stamp + `refuse_if_stamp_unsupported` are the seam a future migration
//! would plug into: re-introduce a dispatcher that walks the stamp forward and
//! lower `MIN_SUPPORTED` below CURRENT for exactly the versions it can upgrade.
//! Until a concrete graph demands it, that machinery is unearned complexity and
//! is deliberately absent. A future converter is best shaped as a standalone
//! one-shot tool, not a framework baked into the open path.
//!
//! ## Forward-version protection
//!
//! A stamp *higher* than this binary's version triggers a clear "upgrade
//! omnigraph first" error. An old binary cannot clobber a newer schema by
//! silently treating "unknown stamp" as "missing stamp".

use lance::Dataset;

use crate::error::{OmniError, Result};

/// Current internal schema version this binary expects to find on disk.
///
/// History:
/// - v1 — implicit (pre-stamp). `__manifest.object_id` carried no
///   `lance-schema:unenforced-primary-key` annotation.
/// - v2 — `__manifest.object_id` carries the unenforced-PK annotation,
///   engaging Lance's bloom-filter conflict resolver at commit time.
/// - v3 — one-time sweep of legacy `__run__<id>` staging branches left on the
///   `__manifest` dataset by the pre-v0.4.0 Run state machine.
/// - v4 — RFC-013 Phase 7 folds graph lineage into `__manifest` as
///   `graph_commit`/`graph_head` rows written in the publish CAS (no
///   `_graph_commits.lance`).
///
/// v1–v3 graphs are not served by this binary (see `MIN_SUPPORTED`); the history
/// is kept for provenance and to document what each stamp value meant.
pub(crate) const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = 4;

/// The oldest on-disk internal-schema stamp this binary will open. With no
/// in-place migration, this equals `INTERNAL_MANIFEST_SCHEMA_VERSION`: a graph
/// stamped below it is refused (`refuse_if_stamp_unsupported`) with a
/// rebuild-via-export/import message rather than silently upgraded.
///
/// Lowering this below CURRENT only makes sense alongside a re-introduced
/// migration dispatcher that can actually walk those versions forward (see the
/// module doc).
pub(crate) const MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION: u32 = INTERNAL_MANIFEST_SCHEMA_VERSION;

const INTERNAL_SCHEMA_VERSION_KEY: &str = "omnigraph:internal_schema_version";

/// Read the on-disk stamp from `__manifest`'s schema-level metadata.
/// Absent ⇒ v1 (pre-stamp world), which is below `MIN_SUPPORTED` and so refused.
pub(crate) fn read_stamp(dataset: &Dataset) -> u32 {
    dataset
        .schema()
        .metadata
        .get(INTERNAL_SCHEMA_VERSION_KEY)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

/// Stamp a freshly-initialized manifest with the current internal schema
/// version. Idempotent — safe to call on an already-stamped dataset.
pub(super) async fn stamp_current_version(dataset: &mut Dataset) -> Result<()> {
    set_stamp(dataset, INTERNAL_MANIFEST_SCHEMA_VERSION).await
}

/// Refuse to open a manifest whose stamp this binary cannot serve — in either
/// direction — with a clear, actionable path. Shared by every open path (the
/// read-write open guard, the read-only open guard, and the publisher), so a new
/// stamp-reading caller gets the floor and the ceiling together and cannot
/// half-enforce.
///
/// - `stamp > CURRENT`: the graph was written by a newer binary — upgrade omnigraph.
/// - `stamp < MIN_SUPPORTED`: the graph was made by an older omnigraph whose
///   storage format this binary does not read — rebuild it via export/import.
pub(crate) fn refuse_if_stamp_unsupported(stamp: u32) -> Result<()> {
    if stamp > INTERNAL_MANIFEST_SCHEMA_VERSION {
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{} but this binary expects v{} \
             — upgrade omnigraph before opening this graph",
            stamp, INTERNAL_MANIFEST_SCHEMA_VERSION,
        )));
    }
    if stamp < MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION {
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{stamp}, but this omnigraph reads only v{current}. \
             This graph was created by an older release; rebuild it: run `omnigraph export` with that \
             older release, then `omnigraph init` + `omnigraph load` with this one. \
             (Data, vectors, and blobs are preserved; commit history and branches are not.)",
            current = INTERNAL_MANIFEST_SCHEMA_VERSION,
        )));
    }
    Ok(())
}

async fn set_stamp(dataset: &mut Dataset, version: u32) -> Result<()> {
    dataset
        .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY.to_string(), version.to_string())])
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(())
}

/// Test-only: force the on-disk internal-schema stamp to `version`. The minimal
/// seam used to synthesize a sub-CURRENT graph and assert the open path refuses
/// it. Its only caller is the in-source refusal test, so it is `cfg(test)`-only.
#[cfg(test)]
pub(crate) async fn set_stamp_for_test(dataset: &mut Dataset, version: u32) -> Result<()> {
    set_stamp(dataset, version).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The guard accepts exactly the single served version and refuses anything
    /// below the floor or above the ceiling. With `MIN == CURRENT == 4` the live
    /// range is exactly `[4, 4]`.
    #[test]
    fn unsupported_guard_accepts_exactly_the_supported_range() {
        for stamp in MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION..=INTERNAL_MANIFEST_SCHEMA_VERSION {
            assert!(
                refuse_if_stamp_unsupported(stamp).is_ok(),
                "stamp v{stamp} is within [MIN, CURRENT] and must be accepted"
            );
        }
        if MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION > 0 {
            assert!(
                refuse_if_stamp_unsupported(MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION - 1).is_err(),
                "a sub-floor stamp must be refused"
            );
        }
        assert!(
            refuse_if_stamp_unsupported(INTERNAL_MANIFEST_SCHEMA_VERSION + 1).is_err(),
            "a future stamp must be refused"
        );
    }
}
