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
//! Fresh graphs are stamped at CURRENT *inside* the init `Dataset::write`
//! Create commit (`current_stamp_entry` rides the write's schema metadata), so
//! the stamp is atomic with manifest birth: no crash can leave `__manifest`
//! durable but unstamped.
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
/// - v5 — RFC-028 adds non-zero stable-table/incarnation identity columns;
///   table registration, version, tombstone, fold, OCC, and physical paths are
///   keyed by that immutable identity rather than the mutable table alias.
/// - v6 — RFC-023 makes every node/edge physical table keyed by exactly its
///   non-null `id` field using Lance's unenforced-primary-key metadata. The
///   annotation is present at dataset creation and preserved by overwrites;
///   older graphs cross this immutable boundary by export/init/load rebuild.
///
/// v1–v5 graphs are not served by this binary (see `MIN_SUPPORTED`); the history
/// is kept for provenance and to document what each stamp value meant.
pub(crate) const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = 6;

/// The oldest on-disk internal-schema stamp this binary will open. With no
/// in-place migration, this equals `INTERNAL_MANIFEST_SCHEMA_VERSION`: a graph
/// stamped below it is refused (`refuse_if_stamp_unsupported`) with a
/// rebuild-via-export/import message rather than silently upgraded.
///
/// Lowering this below CURRENT only makes sense alongside a re-introduced
/// migration dispatcher that can actually walk those versions forward (see the
/// module doc).
pub(crate) const MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION: u32 = INTERNAL_MANIFEST_SCHEMA_VERSION;

/// The omnigraph release or exact development build that wrote a given
/// internal-schema stamp. The
/// open-refusal uses it to tell an operator exactly which binary to use to
/// export a sub-CURRENT graph (the export side of the strand-model upgrade —
/// see `docs/user/operations/upgrade.md`). Ranges are the release tags that
/// stamped each version (verify with
/// `git show vX.Y.Z:crates/omnigraph/src/db/manifest/migrations.rs`):
/// v1 ≤ 0.3.1, v2 0.4.1–0.6.1, v3 0.6.2–0.7.2, v4 0.8.x, v5 was
/// unreleased (final source commit pinned below), and v6 is 0.9.x.
pub(crate) fn release_for_internal_schema_version(stamp: u32) -> &'static str {
    match stamp {
        1 => "0.3.1 or earlier",
        2 => "0.4.1 to 0.6.1",
        3 => "0.6.2 to 0.7.2",
        4 => "0.8.x",
        5 => {
            "built from unreleased final-v5 source commit 46b6d9084fb629b88d4ac9e8c546e0a30d213d19"
        }
        6 => "0.9.x",
        // Unreachable today (1–6 are mapped; > CURRENT is caught by the ceiling
        // guard before this is consulted). Worded to read naturally after
        // "created by omnigraph " if a future bump ever leaves a gap.
        _ => "an unrecognized older release",
    }
}

const INTERNAL_SCHEMA_VERSION_KEY: &str = "omnigraph:internal_schema_version";

/// The schema-metadata entry stamping a fresh manifest at CURRENT. Folded into
/// the Arrow schema of init's `Dataset::write` so the stamp lands in the same
/// Lance commit that creates `__manifest` — the atomic-birth half of the
/// torn-init fix (the other half is `guard_stamp`'s absent arm).
pub(super) fn current_stamp_entry() -> (String, String) {
    (
        INTERNAL_SCHEMA_VERSION_KEY.to_string(),
        INTERNAL_MANIFEST_SCHEMA_VERSION.to_string(),
    )
}

/// Read the on-disk stamp from `__manifest`'s schema-level metadata for
/// display surfaces (`omnigraph snapshot`). `None` covers both an absent key
/// and an unparseable value; the open paths never use this — they go through
/// `guard_stamp`, which distinguishes those shapes and refuses each with its
/// own diagnosis instead of flooring to a version.
pub(crate) fn read_stamp(dataset: &Dataset) -> Option<u32> {
    dataset
        .schema()
        .metadata
        .get(INTERNAL_SCHEMA_VERSION_KEY)
        .and_then(|s| s.parse().ok())
}

/// The single stamp gate for every open path: read the stamp and refuse
/// anything this binary cannot serve, with an honest diagnosis for each shape.
///
/// - A parseable stamp — the ordinary floor/ceiling refusal
///   (`refuse_if_stamp_unsupported`).
/// - A stamp key whose value is not a version number — refused naming the
///   raw value. Never classified as absent: a corrupt stamp must not flow
///   into the delete-and-re-init advice below.
/// - No stamp key on a manifest with the modern layout — not a genuine
///   pre-stamp (v1) manifest, because the RFC-028 identity columns arrived at
///   v5, after stamping began. This can be an older binary's init interrupted
///   between the `__manifest` Create commit and its separate stamp commit, or
///   damaged/externally modified metadata on a graph that progressed further.
///   Those cases are indistinguishable from the remaining metadata, so the
///   guard fails closed. Delete-and-re-init is advised only when the operator
///   independently knows initialization never completed; otherwise the root
///   must be preserved for investigation or recovery.
/// - No stamp key on a pre-modern layout — the genuine pre-stamp world:
///   treated as v1 and refused through the ordinary sub-floor message naming
///   the 0.3.1 export path.
pub(crate) fn guard_stamp(dataset: &Dataset) -> Result<u32> {
    match dataset.schema().metadata.get(INTERNAL_SCHEMA_VERSION_KEY) {
        Some(value) => match value.parse::<u32>() {
            Ok(stamp) => {
                refuse_if_stamp_unsupported(stamp)?;
                Ok(stamp)
            }
            Err(_) => Err(OmniError::manifest(format!(
                "__manifest carries an internal-schema stamp that is not a version \
                 number ('{value}'). The stamp metadata may be corrupt; refusing to \
                 open rather than guess the storage format.",
            ))),
        },
        None if manifest_layout_is_modern(dataset) => Err(OmniError::manifest(
            "__manifest has the current manifest layout but no internal-schema stamp. \
             This may be an interrupted `omnigraph init` from an older binary, which \
             stamped `__manifest` in a separate commit, or damaged or externally \
             modified metadata. OmniGraph cannot safely distinguish those cases and \
             will not open the graph. If you know initialization never completed, \
             delete the graph root and run `omnigraph init` again. Otherwise preserve \
             the root and investigate or restore from a known-good backup; do not \
             reinitialize it in place.",
        )),
        None => {
            refuse_if_stamp_unsupported(1)?;
            Ok(1)
        }
    }
}

/// Whether `__manifest`'s schema carries the RFC-028 stable-identity columns
/// (v5+). Distinguishes an unstamped modern manifest (possible interrupted init
/// or metadata damage) from a genuine pre-stamp v1 store — free, since the
/// schema is already in memory when the stamp is read.
fn manifest_layout_is_modern(dataset: &Dataset) -> bool {
    dataset.schema().field("stable_table_id").is_some()
        && dataset.schema().field("table_incarnation_id").is_some()
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
             This graph was created by omnigraph {release}. Rebuild it: with an omnigraph {release} binary run \
             `omnigraph export <graph> > graph.jsonl`, then with this binary run \
             `omnigraph init --schema <schema.pg> <new-graph>` and \
             `omnigraph load --mode overwrite --data graph.jsonl <new-graph>`. \
             (Data, vectors, and blobs are preserved; commit history and branches are not.) \
             See docs/user/operations/upgrade.md.",
            current = INTERNAL_MANIFEST_SCHEMA_VERSION,
            release = release_for_internal_schema_version(stamp),
        )));
    }
    Ok(())
}

#[cfg(test)]
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

/// Test-only: overwrite the internal-schema stamp with a raw (possibly
/// non-numeric) value. Used to pin `guard_stamp`'s unreadable-stamp arm.
#[cfg(test)]
pub(crate) async fn set_raw_stamp_for_test(dataset: &mut Dataset, value: &str) -> Result<()> {
    dataset
        .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY.to_string(), value.to_string())])
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(())
}

/// Test-only: strip the internal-schema stamp entirely, synthesizing the torn
/// state a pre-atomic-stamp binary left when init died between the `__manifest`
/// Create commit and the stamp commit. Used to pin `guard_stamp`'s absent arm.
#[cfg(test)]
pub(crate) async fn remove_stamp_for_test(dataset: &mut Dataset) -> Result<()> {
    let remaining: Vec<(String, String)> = dataset
        .schema()
        .metadata
        .iter()
        .filter(|(k, _)| k.as_str() != INTERNAL_SCHEMA_VERSION_KEY)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    dataset
        .update_schema_metadata(remaining)
        .replace()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The guard accepts exactly the single served version and refuses anything
    /// below the floor or above the ceiling. With `MIN == CURRENT == 6` the live
    /// range is exactly `[6, 6]`.
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
        let future_stamp = INTERNAL_MANIFEST_SCHEMA_VERSION + 1;
        let future = refuse_if_stamp_unsupported(future_stamp)
            .expect_err("the first abandoned post-v6 stamp must be refused")
            .to_string();
        assert!(future.contains("internal schema v7"), "got: {future}");
        assert!(future.contains("expects v6"), "got: {future}");
        assert!(future.contains("upgrade omnigraph"), "got: {future}");
    }

    /// The refusal names the release line that wrote each stamp so an operator
    /// knows which binary to use for the export step; unknown stamps fall back
    /// without panicking.
    #[test]
    fn release_names_the_writing_line_for_each_stamp() {
        assert_eq!(release_for_internal_schema_version(3), "0.6.2 to 0.7.2");
        assert_eq!(release_for_internal_schema_version(4), "0.8.x");
        assert!(release_for_internal_schema_version(5).contains("unreleased final-v5"));
        assert!(release_for_internal_schema_version(5).contains("46b6d908"));
        assert_eq!(release_for_internal_schema_version(6), "0.9.x");
        assert_eq!(
            release_for_internal_schema_version(99),
            "an unrecognized older release"
        );
        // The sub-CURRENT refusal embeds the named release.
        let err = refuse_if_stamp_unsupported(3).unwrap_err().to_string();
        assert!(err.contains("0.6.2 to 0.7.2"), "got: {err}");
        assert!(err.contains("omnigraph export"), "got: {err}");
    }
}
