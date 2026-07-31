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
/// - v5 — RFC-028 adds non-zero stable-table/incarnation identity columns;
///   table registration, version, tombstone, fold, OCC, and physical paths are
///   keyed by that immutable identity rather than the mutable table alias.
/// - v6 — RFC-023 makes every node/edge physical table keyed by exactly its
///   non-null `id` field using Lance's unenforced-primary-key metadata. The
///   annotation is present at dataset creation and preserved by overwrites;
///   older graphs cross this immutable boundary by export/init/load rebuild.
/// - v7 — RFC-026 adds identity-keyed `stream_state` authority rows carrying
///   the physical enrollment, mutable current-HEAD witness, lifecycle, and
///   per-shard epoch floor.
/// - v8 — RFC-026 Phase B1 activates data-bearing MemWAL state with the exact
///   persisted config-v2 writer profile and recovery-v11 `StreamFold` intents.
/// - v9 — RFC-026 Phase B2 provisions the reserved trusted-row metadata and
///   manifest-selected token authority, and upgrades enrolled streams to
///   config-v3/state-v2/recovery-v12 authority. V8 graphs cross this immutable
///   format boundary by export/init/load rebuild.
/// - v10 — RFC-026 §4.7 P1 adds the required graph-global `stream_profile`
///   enablement singleton (present from genesis, disabled) and adds the
///   now-frozen explicit-null fold-attribution dead-letter placeholder. V9
///   graphs cross this immutable format
///   boundary by export/init/load rebuild: v9 decoders silently skip unknown
///   row kinds, so only the stamp can make an older binary refuse a
///   streaming-capable graph instead of writing blind to the flag.
/// - v11 — RFC-026 §4.7 F2 replaces the boolean stream-profile payload with
///   discriminated `DISABLED | ENABLED | DISABLING | RETIRED` authority and
///   upgrades `_stream_tokens.lance` to the tagged current-token/control-ledger
///   schema. Profile changes use recovery-v13 and immutable management receipts.
/// - v12 — RFC-026 F2 lifecycle tranche replaces inline lane receipt vectors
///   with lifecycle-v3 bounded ledger-chain/current pointers and authenticated
///   incremental WAL-tail authority. Claim/drain effects use recovery-v14;
///   historical recovery-v10 enrollment and recovery-v12 ordinary fold retain
///   their exact lifecycle-v2 wire types and are never reinterpreted.
/// - v13 — RFC-026 F3a activates explicit `SEALED -> OPEN` resume and guarded
///   `DRAINING -> OPEN` abort-drain through recovery-v15. Recovery-v14 remains
///   frozen; its dormant resume/rebind scaffolds keep their original wire
///   meaning and continue to fail closed.
/// - v14 — RFC-026 F3b adds the same-binding, `SEALED`-only `EnsureIndices`
///   maintenance bridge. The v13 format remains frozen and is never
///   reinterpreted as carrying that maintenance authority.
/// - v15 — RFC-026 F3c adds the checked-runtime, `SEALED`-only `Optimize`
///   maintenance bridge through recovery-v17. The v14/recovery-v16
///   `EnsureIndices` format remains frozen and is never reinterpreted as
///   carrying Optimize's internally committing maintenance effects.
/// - v16 — RFC-026 F3d adds recovery-v18 for physical rebind. The
///   v15/recovery-v17 `StreamSealedOptimize` format remains frozen and is never
///   reinterpreted as carrying rebind's fresh binding, shard, receipt, and
///   `SEALED` proof authority.
///
/// v1–v15 graphs are not served by this binary (see `MIN_SUPPORTED`); the history
/// is kept for provenance and to document what each stamp value meant.
pub(crate) const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = 16;

/// The oldest on-disk internal-schema stamp this binary will open. With no
/// in-place migration, this equals `INTERNAL_MANIFEST_SCHEMA_VERSION`: a graph
/// stamped below it is refused (`refuse_if_stamp_unsupported`) with a
/// rebuild-via-export/import message rather than silently upgraded.
///
/// Lowering this below CURRENT only makes sense alongside a re-introduced
/// migration dispatcher that can actually walk those versions forward (see the
/// module doc).
pub(crate) const MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION: u32 = INTERNAL_MANIFEST_SCHEMA_VERSION;

/// The omnigraph release line that wrote a given internal-schema stamp. The
/// open-refusal uses it to tell an operator exactly which binary to use to
/// export a sub-CURRENT graph (the export side of the strand-model upgrade —
/// see `docs/user/operations/upgrade.md`). Ranges are the release tags that
/// stamped each version (verify with
/// `git show vX.Y.Z:crates/omnigraph/src/db/manifest/migrations.rs`):
/// v1 ≤ 0.3.1, v2 0.4.1–0.6.1, v3 0.6.2–0.7.2, v4 0.8.x, v5–v8 unreleased,
/// v9 0.9.x, v10–v16 unreleased. V10–v16 are source-only development
/// formats; release preparation designates whichever later strand actually
/// ships instead of relabeling superseded stamps.
///
/// v5 through v8 never reached a published release: the format advanced five
/// times (RFC-028 identity, RFC-023 key fencing, and the three RFC-026 stream
/// slices) inside the single 0.8.1 → 0.9.0 development window, so the only
/// graphs carrying those stamps came from source builds off `main`. An earlier
/// revision of this table optimistically assigned each of them its own release
/// line (0.9.x–0.12.x); those releases do not exist and naming them here would
/// send an operator hunting for a binary that was never published. V10 and v11
/// remain 0.10.0-dev permanently because each was superseded before release.
/// V16 is currently written only by 0.10.0-dev source builds. If it is the
/// format that ships, the 0.10.0 release-prep commit flips only its entry to
/// the published line; superseded v15 stays dev.
pub(crate) fn release_for_internal_schema_version(stamp: u32) -> &'static str {
    match stamp {
        1 => "0.3.1 or earlier",
        2 => "0.4.1 to 0.6.1",
        3 => "0.6.2 to 0.7.2",
        4 => "0.8.x",
        // Reads in both message slots ("created by omnigraph X" and "with an
        // omnigraph X binary"). No such binary was ever published, so
        // upgrade.md explains that these graphs must be exported with the
        // source commit that stamped them.
        5..=8 => "0.9.0-dev",
        // The published line whose binaries serve v9 — the string the gated
        // v9↔v10 crossversion fence asserts inside the v10 refusal message.
        9 => "0.9.x",
        // Unreachable in refusals while CURRENT == 16 (the sub-floor path
        // consults 1–15 only; the ceiling path never consults the map). It
        // exists for the table's honesty and the next bump. Release-prep for
        // 0.10.0 release prep MUST split this arm and flip only the stamp that
        // actually ships. Every superseded source-only stamp stays
        // "0.10.0-dev" permanently.
        10..=16 => "0.10.0-dev",
        // Worded to read naturally after "created by omnigraph " if a future
        // bump ever leaves a gap.
        _ => "an unrecognized older release",
    }
}

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
    /// below the floor or above the ceiling. With `MIN == CURRENT == 16` the
    /// live range is exactly `[16, 16]`.
    #[test]
    fn unsupported_guard_accepts_exactly_the_supported_range() {
        assert_eq!(INTERNAL_MANIFEST_SCHEMA_VERSION, 16);
        assert_eq!(MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION, 16);
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

    /// The refusal names the release line that wrote each stamp so an operator
    /// knows which binary to use for the export step; unknown stamps fall back
    /// without panicking.
    #[test]
    fn release_names_the_writing_line_for_each_stamp() {
        assert_eq!(release_for_internal_schema_version(3), "0.6.2 to 0.7.2");
        assert_eq!(release_for_internal_schema_version(4), "0.8.x");
        // v5-v8 advanced and were superseded entirely within the 0.8.1 -> 0.9.0
        // development window, so no published release stamped them.
        for unreleased in 5..=8 {
            assert_eq!(release_for_internal_schema_version(unreleased), "0.9.0-dev");
        }
        assert_eq!(release_for_internal_schema_version(9), "0.9.x");
        assert_eq!(release_for_internal_schema_version(10), "0.10.0-dev");
        assert_eq!(release_for_internal_schema_version(11), "0.10.0-dev");
        assert_eq!(release_for_internal_schema_version(12), "0.10.0-dev");
        assert_eq!(release_for_internal_schema_version(13), "0.10.0-dev");
        assert_eq!(release_for_internal_schema_version(14), "0.10.0-dev");
        assert_eq!(release_for_internal_schema_version(15), "0.10.0-dev");
        assert_eq!(release_for_internal_schema_version(16), "0.10.0-dev");
        assert_eq!(
            release_for_internal_schema_version(99),
            "an unrecognized older release"
        );
        // The sub-CURRENT refusal embeds the named release.
        let err = refuse_if_stamp_unsupported(3).unwrap_err().to_string();
        assert!(err.contains("0.6.2 to 0.7.2"), "got: {err}");
        assert!(err.contains("omnigraph export"), "got: {err}");

        let v6_err = refuse_if_stamp_unsupported(6).unwrap_err().to_string();
        assert!(v6_err.contains("0.9.0-dev"), "got: {v6_err}");
        assert!(v6_err.contains("omnigraph export"), "got: {v6_err}");
        // The embedded release must read naturally in both slots of the
        // rebuild instruction, not just the "created by" clause.
        assert!(
            v6_err.contains("with an omnigraph 0.9.0-dev binary"),
            "got: {v6_err}"
        );

        // The v9 refusal pins the exact strings the gated genuine-binary
        // v9↔v10 crossversion fence asserts (`OMNIGRAPH_V9_BIN`). A future
        // map edit that changes them must break HERE, locally and unskippably,
        // not only in the env-gated CI cell (the #387 failure class).
        let v9_err = refuse_if_stamp_unsupported(9).unwrap_err().to_string();
        assert!(
            v9_err.contains("created by omnigraph 0.9.x"),
            "got: {v9_err}"
        );
        assert!(
            v9_err.contains("with an omnigraph 0.9.x binary"),
            "got: {v9_err}"
        );
        assert!(v9_err.contains("omnigraph export"), "got: {v9_err}");

        // The v10 refusal strings are also asserted by the genuine-binary
        // v10↔v11 seam (`OMNIGRAPH_V10_BIN`). Keep a local, unskippable guard
        // so release-map drift fails before reaching the historical-binary job.
        let v10_err = refuse_if_stamp_unsupported(10).unwrap_err().to_string();
        assert!(
            v10_err.contains("created by omnigraph 0.10.0-dev"),
            "got: {v10_err}"
        );
        assert!(
            v10_err.contains("with an omnigraph 0.10.0-dev binary"),
            "got: {v10_err}"
        );
        assert!(v10_err.contains("omnigraph export"), "got: {v10_err}");

        // The v11 refusal strings are asserted by the genuine-binary v11↔v12
        // seam (`OMNIGRAPH_V11_BIN`). Pin them locally so release-prep cannot
        // accidentally relabel the source-only v11 stamp while updating v12.
        let v11_err = refuse_if_stamp_unsupported(11).unwrap_err().to_string();
        assert!(
            v11_err.contains("created by omnigraph 0.10.0-dev"),
            "got: {v11_err}"
        );
        assert!(
            v11_err.contains("with an omnigraph 0.10.0-dev binary"),
            "got: {v11_err}"
        );
        assert!(v11_err.contains("omnigraph export"), "got: {v11_err}");

        // V12 is the immediate predecessor used by the genuine-binary
        // v12↔v13 format fence. Keep its source-build release wording local
        // and unskippable so release preparation cannot relabel it with v13.
        let v12_err = refuse_if_stamp_unsupported(12).unwrap_err().to_string();
        assert!(
            v12_err.contains("created by omnigraph 0.10.0-dev"),
            "got: {v12_err}"
        );
        assert!(
            v12_err.contains("with an omnigraph 0.10.0-dev binary"),
            "got: {v12_err}"
        );
        assert!(v12_err.contains("omnigraph export"), "got: {v12_err}");

        // V13 is the immediate predecessor used by the v13↔v14 format fence.
        // Keep its source-build release wording local and unskippable so
        // release preparation cannot relabel it with v14.
        let v13_err = refuse_if_stamp_unsupported(13).unwrap_err().to_string();
        assert!(
            v13_err.contains("created by omnigraph 0.10.0-dev"),
            "got: {v13_err}"
        );
        assert!(
            v13_err.contains("with an omnigraph 0.10.0-dev binary"),
            "got: {v13_err}"
        );
        assert!(v13_err.contains("omnigraph export"), "got: {v13_err}");

        // V14 is the immediate predecessor used by the v14↔v15 format fence.
        // Keep its source-build release wording local and unskippable so
        // release preparation cannot relabel it with v15.
        let v14_err = refuse_if_stamp_unsupported(14).unwrap_err().to_string();
        assert!(
            v14_err.contains("created by omnigraph 0.10.0-dev"),
            "got: {v14_err}"
        );
        assert!(
            v14_err.contains("with an omnigraph 0.10.0-dev binary"),
            "got: {v14_err}"
        );
        assert!(v14_err.contains("omnigraph export"), "got: {v14_err}");

        // V15 is the immediate predecessor used by the v15↔v16 format fence.
        // Keep its source-build release wording local and unskippable so
        // release preparation cannot relabel it with v16.
        let v15_err = refuse_if_stamp_unsupported(15).unwrap_err().to_string();
        assert!(
            v15_err.contains("created by omnigraph 0.10.0-dev"),
            "got: {v15_err}"
        );
        assert!(
            v15_err.contains("with an omnigraph 0.10.0-dev binary"),
            "got: {v15_err}"
        );
        assert!(v15_err.contains("omnigraph export"), "got: {v15_err}");
    }
}
