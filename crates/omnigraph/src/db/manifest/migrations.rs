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
///
/// Concurrent runners: two processes (or two open-for-write handles) can open the
/// same legacy graph at once and both reach the backfill merge. `merge_lineage_rows`
/// uses `conflict_retries(0)`, so the row-level CAS loser on `graph_head:<branch>`
/// must be re-driven here rather than failing the open — `migrate_v2_to_v3` is
/// concurrent-runner idempotent and this step must be too. The bounded loop
/// re-reads the fast path (a concurrent winner's merge is one atomic Lance commit,
/// so a re-read sees either zero or all of its rows, never partial), re-opens the
/// stale handle past the winner's commit, and retries. On budget exhaustion it
/// returns a `RowLevelCasContention`-typed error so the publisher's OUTER retry
/// loop (which only re-runs `is_retryable_publish_conflict` conflicts) completes
/// it on the next attempt — the same converge-on-next-attempt contract the
/// recovery sweep uses.
async fn migrate_v3_to_v4(
    dataset: &mut Dataset,
    root_uri: &str,
    branch: Option<&str>,
) -> Result<()> {
    // Mirror the publisher's budget (`publisher::PUBLISHER_RETRY_BUDGET = 5`); kept
    // as a local const rather than re-exporting that private one — the two are the
    // same shape (bounded row-level-CAS retries) but independent knobs.
    const MIGRATION_MERGE_RETRY_BUDGET: u32 = 5;

    // Exclusive range + an unguarded retryable arm (see `commit_v4_stamp_idempotently`
    // for the rationale): every retryable conflict re-opens and retries inside the
    // loop, and the SINGLE reachable exhaustion path is the typed contention return
    // below — so the retryable variant can never fall through to the `Err(err)`
    // propagate arm on the last iteration.
    for _ in 0..MIGRATION_MERGE_RETRY_BUDGET {
        // Fast path / idempotency + concurrent-winner guard: if the backfill
        // already landed (a previous run, OR a concurrent runner that won the CAS
        // — its merge is atomic, so this is all-or-nothing), don't re-merge — just
        // (re)stamp. `dataset` is re-opened past any winner's commit below, so this
        // re-read sees the winner's rows on a retry.
        let (existing_lineage, _heads) = read_graph_lineage(dataset).await?;
        if !existing_lineage.is_empty() {
            return commit_v4_stamp_idempotently(dataset, root_uri, branch).await;
        }

        // Read this branch's legacy commit cache (commits + the head). An absent or
        // empty `_graph_commits.lance` yields no commits — nothing to backfill.
        let (commit_by_id, head) =
            crate::db::commit_graph::read_legacy_commit_cache(root_uri, branch).await?;
        if commit_by_id.is_empty() {
            return commit_v4_stamp_idempotently(dataset, root_uri, branch).await;
        }

        let parts = build_lineage_backfill_parts(&commit_by_id, head.as_ref(), branch)?;

        match merge_lineage_rows(dataset.clone(), &parts).await {
            Ok(new_dataset) => {
                *dataset = new_dataset;
                // Stamp LAST. Crash window: a failure between the merge above and
                // this stamp bump leaves stamp v3 + lineage present in `__manifest`.
                // The next open re-enters at v3, the fast path at the top sees the
                // lineage and skips straight to the stamp bump — completing the
                // migration with no duplicate rows (the merge is keyed on
                // `object_id`). Pinned by
                // `crash_after_merge_before_stamp_completes_on_next_open`.
                return commit_v4_stamp_idempotently(dataset, root_uri, branch).await;
            }
            // A concurrent runner won the `graph_head:<branch>` CAS. Our in-hand
            // handle is stale at the pre-contention HEAD, so a re-open is required
            // to see the winner's commit; then re-loop (the fast path will see the
            // winner's lineage and stamp). Bounded by the budget.
            Err(err) if super::publisher::is_retryable_publish_conflict(&err) => {
                *dataset = super::layout::open_manifest_dataset(root_uri, branch).await?;
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    // Budget exhausted under sustained contention. Return a CAS-typed error (not a
    // plain conflict) so the publisher's outer retry loop — which only re-runs
    // `is_retryable_publish_conflict` — re-runs `load_publish_state` and completes
    // the migration, rather than giving up.
    Err(OmniError::manifest_row_level_cas_contention(format!(
        "v3→v4 lineage backfill exhausted {} retries against concurrent runners",
        MIGRATION_MERGE_RETRY_BUDGET
    )))
}

/// Stamp the v3→v4 migration's terminal version idempotently under concurrent
/// runners. `set_stamp` issues an `UpdateConfig` Lance commit; once the merge CAS
/// loser is made to converge (above), BOTH runners reach this stamp bump and race
/// it — the loser gets `lance::Error::IncompatibleTransaction` (two `UpdateConfig`
/// commits touching the same metadata key), which is NOT a row-level CAS
/// contention and so is not caught by the merge loop. But both write the SAME
/// value, so the conflict is benign: re-open and, if the stamp already reached the
/// target (the concurrent runner finished it), succeed; otherwise re-apply.
/// Bounded; on exhaustion surface a CAS-typed error for the publisher's outer
/// retry, same as the merge loop.
async fn commit_v4_stamp_idempotently(
    dataset: &mut Dataset,
    root_uri: &str,
    branch: Option<&str>,
) -> Result<()> {
    const STAMP_RETRY_BUDGET: u32 = 5;
    // Exclusive range + an UNGUARDED `IncompatibleTransaction` arm: the retryable
    // variant is always handled inside the loop (re-open + same-value check + retry),
    // so it can never fall through to the stringifying `Err(e)` catch-all, and the
    // SINGLE reachable exhaustion path is the typed contention return below. (A
    // `0..=BUDGET` range with an `attempt < BUDGET` guard let the last iteration's
    // retryable conflict reach the catch-all and return a non-retryable
    // `OmniError::Lance` — the publisher's outer retry would then give up.)
    for _ in 0..STAMP_RETRY_BUDGET {
        // Inline the `update_schema_metadata` write (rather than `set_stamp`) so the
        // raw Lance error variant is in hand — `set_stamp` pre-stringifies it.
        let stamp_result = stamp_internal_schema(dataset).await;
        match stamp_result {
            Ok(_) => return Ok(()),
            Err(lance::Error::IncompatibleTransaction { .. }) => {
                // A concurrent runner's `UpdateConfig` preempted ours — the
                // retryable case. Re-open past its commit; if it already stamped to
                // the target we're done (the value is identical), else fall through
                // to retry on the advanced handle.
                *dataset = super::layout::open_manifest_dataset(root_uri, branch).await?;
                if read_stamp(dataset) >= INTERNAL_MANIFEST_SCHEMA_VERSION {
                    return Ok(());
                }
            }
            Err(e) => return Err(OmniError::Lance(e.to_string())),
        }
    }

    // Exhausted the budget against sustained concurrent stampers. Return a
    // CAS-typed (retryable) error so the publisher's OUTER retry — which only
    // re-runs `is_retryable_publish_conflict` — completes it, rather than the
    // stringified `OmniError::Lance` it would treat as fatal.
    Err(OmniError::manifest_row_level_cas_contention(format!(
        "v3→v4 stamp bump exhausted {} retries against concurrent runners",
        STAMP_RETRY_BUDGET
    )))
}

/// The single `update_schema_metadata` write that bumps the on-disk internal-schema
/// stamp to the current version. Extracted from `commit_v4_stamp_idempotently`'s
/// retry loop so a `failpoints` test can inject a concurrent-stamper
/// `IncompatibleTransaction` deterministically (the loop's exhaustion path is
/// otherwise near-unreachable). Returns the RAW `lance::Error` so the loop can match
/// the `IncompatibleTransaction` variant — `set_stamp` pre-stringifies it.
async fn stamp_internal_schema(dataset: &mut Dataset) -> std::result::Result<(), lance::Error> {
    crate::failpoints::maybe_fail_lance_incompatible("migration.v4_stamp.force_incompatible")?;
    dataset
        .update_schema_metadata([(
            INTERNAL_SCHEMA_VERSION_KEY.to_string(),
            INTERNAL_MANIFEST_SCHEMA_VERSION.to_string(),
        )])
        .await
        .map(|_| ())
}

/// Build the `__manifest` rows for the v3→v4 backfill: one immutable
/// `graph_commit` row per commit, plus EXACTLY ONE `graph_head:<branch>` row for
/// the actual head. Each commit encodes to a `[graph_commit, graph_head]` pair,
/// but only the head commit's head row is kept — the others would be redundant
/// updates of the same `graph_head:<branch>` object_id (the head is per-branch,
/// not per-commit).
fn build_lineage_backfill_parts(
    commit_by_id: &std::collections::HashMap<String, GraphCommit>,
    head: Option<&GraphCommit>,
    branch: Option<&str>,
) -> Result<Vec<super::state::GraphLineageRowPart>> {
    let head_id = head.map(|h| h.graph_commit_id.as_str());
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
    Ok(parts)
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
