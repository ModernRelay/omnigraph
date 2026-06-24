//! Internal schema migrations for the `__manifest` Lance dataset.
//!
//! ## Why this exists
//!
//! The on-disk shape of `__manifest` evolves alongside the engine. We do not
//! want healing hooks scattered through every read/write path that ask
//! "is this an old shape? am I supposed to upgrade it?" — that pattern
//! accrues liability with every change. Instead this module is the *single*
//! place where on-disk shape is reconciled with what the binary expects:
//!
//! - One constant `INTERNAL_MANIFEST_SCHEMA_VERSION` declares the shape this
//!   binary writes.
//! - One stamp `omnigraph:internal_schema_version` in the manifest dataset's
//!   schema-level metadata records the on-disk shape.
//! - One dispatcher `migrate_internal_schema` walks the on-disk stamp forward
//!   to the expected version via `match`-arm steps. Each future change adds
//!   one arm + one test, never a new branch in unrelated code paths.
//!
//! After the dispatcher runs, the rest of the engine assumes current shape.
//! No code outside this module should ever inspect the stamp.
//!
//! ## When it runs
//!
//! Only on open-for-write paths (the publisher's `load_publish_state`).
//! Reads are side-effect-free by contract; an old-shape `__manifest` reads
//! fine, it just lacks the protections introduced by later versions.
//! `init_manifest_graph` stamps the current version at creation, so newly
//! initialized graphs never need migration.
//!
//! ## Forward-version protection
//!
//! A stamp *higher* than this binary's known version triggers a clear
//! "upgrade omnigraph first" error. An old binary cannot clobber a newer
//! schema by silently treating "unknown stamp" as "missing stamp".

use lance::Dataset;

use crate::error::{OmniError, Result};

use crate::db::commit_graph::GraphCommit;
use super::state::{GraphLineageRow, graph_lineage_row_parts, merge_lineage_rows, read_graph_lineage};

/// Current internal schema version this binary expects to find on disk.
///
/// History:
/// - v1 — implicit (pre-stamp). `__manifest.object_id` carried no
///   `lance-schema:unenforced-primary-key` annotation; the publisher had
///   no row-level CAS protection (see `.context/merge-insert-cas-granularity.md`).
/// - v2 — `__manifest.object_id` carries the unenforced-PK annotation,
///   engaging Lance's bloom-filter conflict resolver at commit time. Added
///   alongside `expected_table_versions` OCC on `ManifestBatchPublisher::publish`.
/// - v3 — one-time sweep of legacy `__run__<id>` staging branches left on the
///   `__manifest` dataset by the pre-v0.4.0 Run state machine (removed in
///   MR-771). Once swept, the `is_internal_run_branch` defense-in-depth guard
///   is no longer needed (MR-770).
/// - v4 — RFC-013 Phase 7 folds graph lineage into `__manifest` as
///   `graph_commit`/`graph_head` rows written in the publish CAS. A pre-Phase-7
///   (v3) graph has its lineage only in `_graph_commits.lance`, so the new
///   binary would read an empty commit DAG. This one-time per-branch backfill
///   copies the lineage from `_graph_commits.lance` into `__manifest`
///   (`migrate_v3_to_v4`). `_graph_commits.lance` is left in place as the
///   branch-ref carrier; no commit rows are ever written to it again.
pub(crate) const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = 4;

const INTERNAL_SCHEMA_VERSION_KEY: &str = "omnigraph:internal_schema_version";
const OBJECT_ID_PK_KEY: &str = "lance-schema:unenforced-primary-key";

/// Read the on-disk stamp from `__manifest`'s schema-level metadata.
/// Absent ⇒ v1 (pre-stamp world).
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

/// Refuse to open a manifest stamped at a version this binary does not know,
/// with a clear "upgrade omnigraph first" error. Shared by the write-path
/// migration dispatcher and the read-only open guard (a read-only open of a
/// future-stamped graph must still refuse, even though it never writes).
pub(crate) fn refuse_if_stamp_too_new(stamp: u32) -> Result<()> {
    if stamp > INTERNAL_MANIFEST_SCHEMA_VERSION {
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{} but this binary expects v{} \
             — upgrade omnigraph before opening this graph",
            stamp, INTERNAL_MANIFEST_SCHEMA_VERSION,
        )));
    }
    Ok(())
}

/// Apply any pending internal-schema migrations to the manifest dataset.
///
/// Idempotent: when the on-disk stamp matches the binary, this is a single
/// metadata read with no writes.
///
/// `root_uri` + `branch` identify which graph + branch this `dataset` is a
/// manifest for. The v3→v4 lineage backfill needs them to read that branch's
/// `_graph_commits.lance`. `migrate_on_open` passes the main branch
/// (`branch = None`); the publisher's `load_publish_state` passes its own
/// branch, so each branch backfills on its first write.
pub(super) async fn migrate_internal_schema(
    dataset: &mut Dataset,
    root_uri: &str,
    branch: Option<&str>,
) -> Result<()> {
    let mut current = read_stamp(dataset);

    refuse_if_stamp_too_new(current)?;

    while current < INTERNAL_MANIFEST_SCHEMA_VERSION {
        match current {
            1 => {
                migrate_v1_to_v2(dataset).await?;
                current = 2;
            }
            2 => {
                migrate_v2_to_v3(dataset).await?;
                current = 3;
            }
            3 => {
                migrate_v3_to_v4(dataset, root_uri, branch).await?;
                current = 4;
            }
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "no internal-schema migration registered for v{} → v{}",
                    other,
                    other + 1,
                )));
            }
        }
    }
    Ok(())
}

/// v1 → v2: annotate `__manifest.object_id` as Lance's unenforced primary key
/// so the merge-insert conflict resolver enforces row-level CAS at commit
/// time, then bump the stamp.
///
/// Idempotent under crash-retry by construction. Lance 7 makes the unenforced
/// primary key **immutable once set**: any write that touches the reserved
/// `lance-schema:unenforced-primary-key` field metadata after the PK is set
/// errors ("cannot be changed once set", `lance::dataset::transaction`), even
/// re-applying the same value. A crash between the field-set and the stamp
/// bump leaves the field set without a stamp, so the next open re-enters here
/// with the PK already present — we must therefore set it only when absent.
/// (Fresh graphs bake the PK into `manifest_schema()` at init and never run
/// this migration; only genuine pre-v0.4.0 graphs do.)
async fn migrate_v1_to_v2(dataset: &mut Dataset) -> Result<()> {
    // The guard is over the *specific* field, not just "any PK is set": skipping
    // when `object_id` is already the PK is the idempotent crash-recovery path,
    // but a manifest whose PK is some *other* field has the wrong CAS key — and
    // Lance 7 won't let us change it. Refuse loudly rather than silently leave
    // merge-insert conflict detection keyed on the wrong column.
    let pk_fields: Vec<&str> = dataset
        .schema()
        .unenforced_primary_key()
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    match pk_fields.as_slice() {
        ["object_id"] => {} // already migrated (or a crash re-entry) — idempotent no-op
        [] => {
            dataset
                .update_field_metadata()
                .update(
                    "object_id",
                    [(OBJECT_ID_PK_KEY.to_string(), "true".to_string())],
                )
                .map_err(|e| OmniError::Lance(e.to_string()))?
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        other => {
            return Err(OmniError::manifest_internal(format!(
                "__manifest unenforced primary key is {other:?}, expected [\"object_id\"]; \
                 refusing to migrate a manifest with an unexpected CAS key"
            )));
        }
    }
    set_stamp(dataset, 2).await
}

/// v2 → v3: sweep legacy `__run__<id>` staging branches off the `__manifest`
/// dataset, then bump the stamp.
///
/// The pre-v0.4.0 Run state machine (removed in MR-771) created graph-level
/// staging branches named `__run__<ulid>` on `__manifest`. MR-771 stopped
/// creating them but left any pre-existing ones in place; Lance's
/// `list_branches` still enumerates them, so they leak into `branch_list()`
/// and count as blocking branches at schema-apply time. This one-time sweep
/// removes them so the `is_internal_run_branch` guard can retire (MR-770).
///
/// The `"__run__"` prefix is inlined here on purpose: this migration must keep
/// working after the `run_registry` module (the guard) is deleted, so it does
/// not depend on it.
///
/// Idempotent under both sequential retry and concurrent runners: each run
/// re-enumerates `list_branches` fresh, and `force_delete_branch` tolerates a
/// branch that is already gone — so a crash before the stamp bump, or a second
/// process opening the same legacy graph at the same time, never errors out.
async fn migrate_v2_to_v3(dataset: &mut Dataset) -> Result<()> {
    const LEGACY_RUN_BRANCH_PREFIX: &str = "__run__";
    let branches = dataset
        .list_branches()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let run_branches: Vec<String> = branches
        .into_keys()
        .filter(|name| {
            name.trim_start_matches('/')
                .starts_with(LEGACY_RUN_BRANCH_PREFIX)
        })
        .collect();
    for name in run_branches {
        // `force_delete_branch` deletes even when the `BranchContents` is
        // already gone. Plain `delete_branch` errors "BranchContents not
        // found", which would fail a second concurrent open (or a retry that
        // raced another runner) after the first one swept the branch. Force is
        // exactly Lance's documented path for cleaning up zombie branches.
        dataset
            .force_delete_branch(&name)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
    }
    set_stamp(dataset, 3).await
}

/// v3 → v4: backfill the graph lineage from `_graph_commits.lance` into
/// `__manifest`, then bump the stamp.
///
/// RFC-013 Phase 7 made `__manifest` the single source of graph lineage
/// (`graph_commit` / `graph_head:<branch>` rows, written in the publish CAS).
/// A pre-Phase-7 (v3) graph has its lineage only in `_graph_commits.lance` and
/// none in `__manifest`, so the new binary would read an EMPTY commit DAG. This
/// one-time per-branch migration copies that branch's commits + the single head
/// into `__manifest` so reads see the real history. `_graph_commits.lance`
/// itself is left untouched as the branch-ref carrier (no commit row is ever
/// written to it again).
///
/// `dataset` is the `__manifest` for `branch` (main when `branch` is `None`);
/// the migration runs per-branch on that branch's first write, so it reads
/// `_graph_commits.lance` at the SAME branch.
///
/// Idempotency + crash recovery: the stamp bump is the LAST step, and the
/// lineage merge is keyed on `object_id` (re-inserting the same commit rows is a
/// no-op update). A crash after the merge but before the stamp bump re-enters
/// here at v3 and re-runs harmlessly. As a fast path, if `__manifest` already
/// carries `graph_commit` rows (a previous run completed the merge), we skip
/// straight to the stamp bump.
async fn migrate_v3_to_v4(
    dataset: &mut Dataset,
    root_uri: &str,
    branch: Option<&str>,
) -> Result<()> {
    // Fast path / idempotency guard: if the backfill already landed (commit rows
    // present in `__manifest`), don't re-read or re-merge — just (re)stamp.
    let (existing_lineage, _heads) = read_graph_lineage(dataset).await?;
    if !existing_lineage.is_empty() {
        return set_stamp(dataset, 4).await;
    }

    // Read this branch's legacy commit cache (commits + the head). An absent or
    // empty `_graph_commits.lance` yields no commits — nothing to backfill.
    let (commit_by_id, head) =
        crate::db::commit_graph::read_legacy_commit_cache(root_uri, branch).await?;
    if commit_by_id.is_empty() {
        return set_stamp(dataset, 4).await;
    }

    // Build the manifest rows: one immutable `graph_commit` row per commit, plus
    // EXACTLY ONE `graph_head:<branch>` row for the actual head. Each commit
    // encodes to a [graph_commit, graph_head] pair, but only the head commit's
    // head row is kept — the others would be redundant updates of the same
    // `graph_head:<branch>` object_id (the head is per-branch, not per-commit).
    let head_id = head.as_ref().map(|h| h.graph_commit_id.as_str());
    // Deterministic iteration order (the source is a HashMap): merge-insert is
    // keyed on `object_id` so the final manifest content is order-independent,
    // but a stable order keeps the produced batch reproducible regardless.
    let mut commits: Vec<&GraphCommit> = commit_by_id.values().collect();
    commits.sort_by(|a, b| a.graph_commit_id.cmp(&b.graph_commit_id));
    let mut parts = Vec::with_capacity(commits.len() + 1);
    for commit in commits {
        let row = GraphLineageRow {
            graph_commit_id: commit.graph_commit_id.clone(),
            manifest_branch: commit.manifest_branch.clone(),
            manifest_version: commit.manifest_version,
            parent_commit_id: commit.parent_commit_id.clone(),
            merged_parent_commit_id: commit.merged_parent_commit_id.clone(),
            actor_id: commit.actor_id.clone(),
            created_at: commit.created_at,
        };
        let [commit_part, head_part] = graph_lineage_row_parts(&row, branch)?;
        parts.push(commit_part);
        if Some(commit.graph_commit_id.as_str()) == head_id {
            parts.push(head_part);
        }
    }

    *dataset = merge_lineage_rows(dataset.clone(), &parts).await?;
    // Stamp LAST. Crash window: a failure between the merge above and this stamp
    // bump leaves stamp v3 + lineage present in `__manifest`. The next open
    // re-enters at v3, the idempotency guard at the top of this fn sees the
    // lineage and skips straight to the stamp bump — completing the migration
    // with no duplicate rows (the merge is keyed on `object_id`). Pinned by
    // `crash_after_merge_before_stamp_completes_on_next_open`.
    set_stamp(dataset, 4).await
}

async fn set_stamp(dataset: &mut Dataset, version: u32) -> Result<()> {
    dataset
        .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY.to_string(), version.to_string())])
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(())
}

/// Test-only: force the on-disk internal-schema stamp to `version`. Used to
/// synthesize a pre-migration graph (rewinding to v3) and to simulate a crash
/// that lost the final stamp bump. Gated on `test` OR `failpoints` so the
/// fault-injection migration test (in the `failpoints` integration binary,
/// compiled without `cfg(test)`) can reach it too.
#[cfg(any(test, feature = "failpoints"))]
pub(crate) async fn set_stamp_for_test(dataset: &mut Dataset, version: u32) -> Result<()> {
    set_stamp(dataset, version).await
}
