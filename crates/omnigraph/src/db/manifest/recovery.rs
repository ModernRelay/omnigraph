//! MR-847 — Recovery-on-open primitives.
//!
//! This module implements the building blocks of the per-sidecar recovery
//! sweep that closes the documented Phase B → Phase C residual (see
//! `docs/runs.md` "Finalize → publisher residual"). The high-level shape:
//!
//! 1. Each writer that performs a multi-table commit writes a small JSON
//!    sidecar at `__recovery/{ulid}.json` BEFORE its per-table
//!    `commit_staged` loop, listing every `(table_key, table_path,
//!    expected_version, post_commit_pin)` it intends to publish.
//! 2. After the manifest publish (Phase C) succeeds, the writer deletes
//!    the sidecar.
//! 3. If the writer crashes between Phase B begin and Phase C success,
//!    the sidecar remains. The next `Omnigraph::open` (gated on
//!    `OpenMode::ReadWrite`) classifies each table in each sidecar and
//!    either rolls forward all tables (if every table is at
//!    `post_commit_pin` AND matches the sidecar) or rolls back all
//!    `RolledPastExpected` tables to `expected_version`.
//!
//! Phase 2 (this commit) ships only the primitives: sidecar I/O,
//! classifier, decision dispatcher, restore-with-fragment-set-shortcut.
//! No integration into `Omnigraph::open` or any writer yet — those land
//! in Phase 3+.
//!
//! ## Verified Lance behavior the rollback path depends on
//!
//! - `Dataset::restore()` takes no version arg; restores
//!   `self.manifest.version` (currently checked-out version). From HEAD =
//!   `h`, produces a new commit at `h + 1` with content == checked-out
//!   version. Pinned by
//!   `tests/staged_writes.rs::lance_restore_appends_one_commit_with_checked_out_content`.
//! - `Dataset::restore` "wins" against concurrent Append/Update/Delete/
//!   CreateIndex/Merge — see `check_restore_txn` at lance-4.0.0
//!   `src/io/commit/conflict_resolver.rs:986`. The hazard is documented
//!   by `tests/staged_writes.rs::lance_restore_loses_to_concurrent_append_via_orphaning`.
//!   MR-847 sidesteps this by running recovery only at `Omnigraph::open`
//!   (before any other writers can race); MR-856's continuous-recovery
//!   reconciler must guard via per-(table_key, branch) queue acquisition
//!   once MR-686 lands.
//!
//! See `.context/mr-847-design.md` for the full design.

use std::collections::HashMap;

use lance::Dataset;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::db::commit_graph::CommitGraph;
use crate::db::recovery_audit::{
    now_micros, RecoveryAudit, RecoveryAuditRecord, RecoveryKind, TableOutcome,
};
use crate::error::{OmniError, Result};
use crate::storage::StorageAdapter;

use super::Snapshot;
use super::publisher::{GraphNamespacePublisher, ManifestBatchPublisher};
use super::{ManifestChange, SubTableUpdate};

/// System actor identifier recorded on every recovery commit. Operators
/// distinguish recovery commits from user commits in `omnigraph commit list`
/// by filtering on this actor; the original sidecar's actor (if any) flows
/// into the audit row's `recovery_for_actor` field.
pub(crate) const RECOVERY_ACTOR: &str = "omnigraph:recovery";

/// Subdirectory under the repo root holding sidecar files.
pub(crate) const RECOVERY_DIR_NAME: &str = "__recovery";

/// Current sidecar JSON shape version. Bumping this is a breaking change:
/// older binaries will refuse to interpret newer sidecars (intentional —
/// see [`SidecarSchemaError`]).
pub(crate) const SIDECAR_SCHEMA_VERSION: u32 = 1;

/// Categorizes the writer that produced a sidecar so audit trail and
/// observability can attribute recoveries to the right code path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) enum SidecarKind {
    /// `MutationStaging::finalize` — `mutate_as` and the bulk loader.
    Mutation,
    /// `loader/mod.rs` — distinct from mutations only for audit clarity.
    Load,
    /// `schema_apply::apply_schema_with_lock` — table rewrites + indices.
    SchemaApply,
    /// `branch_merge_on_current_target` — three-way merge publishes.
    BranchMerge,
    /// `ensure_indices_for_branch` — index lifecycle commits.
    EnsureIndices,
}

/// One table's contribution to a sidecar's intended commit. The classifier
/// uses these to decide per-table state at recovery time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SidecarTablePin {
    /// Stable identifier (`node:Person`, `edge:Knows`, etc.).
    pub table_key: String,
    /// Full URI to the Lance dataset for this table.
    pub table_path: String,
    /// Manifest-pinned version at writer start (CAS expectation).
    pub expected_version: u64,
    /// Lance HEAD that the writer's `commit_staged` would produce
    /// (typically `expected_version + 1`).
    pub post_commit_pin: u64,
}

/// In-memory representation of the on-disk JSON sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecoverySidecar {
    pub schema_version: u32,
    pub operation_id: String,
    pub started_at: String,
    pub branch: Option<String>,
    pub actor_id: Option<String>,
    pub writer_kind: SidecarKind,
    pub tables: Vec<SidecarTablePin>,
}

/// Opaque handle returned by [`write_sidecar`] so the caller can delete
/// the sidecar after Phase C succeeds. Holding the handle does NOT keep
/// the sidecar alive — it just records the URI to delete.
#[derive(Debug, Clone)]
pub(crate) struct RecoverySidecarHandle {
    pub(crate) operation_id: String,
    pub(crate) sidecar_uri: String,
}

/// Error returned when the sidecar's `schema_version` is unknown to this
/// binary. We refuse-and-error rather than read-and-warn: an old binary
/// cannot guess what semantics a newer writer baked into a future shape.
/// Operator action is required (typically: upgrade the binary).
#[derive(Debug)]
pub(crate) struct SidecarSchemaError {
    pub sidecar_uri: String,
    pub found_version: u32,
    pub supported_version: u32,
}

impl std::fmt::Display for SidecarSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "recovery sidecar at '{}' declares schema_version={}, but this \
             binary supports only schema_version={}; refusing to interpret \
             — upgrade omnigraph or remove the sidecar with operator review",
            self.sidecar_uri, self.found_version, self.supported_version,
        )
    }
}

impl std::error::Error for SidecarSchemaError {}

impl From<SidecarSchemaError> for OmniError {
    fn from(err: SidecarSchemaError) -> Self {
        OmniError::manifest_internal(err.to_string())
    }
}

/// Per-table classification of observed Lance HEAD vs. manifest-pinned
/// state, computed against the sidecar's intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableClassification {
    /// `lance_head == manifest_pinned == sidecar.expected_version`.
    /// The writer never reached this table's `commit_staged` (or this
    /// table wasn't touched yet). No drift; no action.
    NoMovement,
    /// `lance_head == manifest_pinned + 1` AND
    /// `sidecar.expected_version == manifest_pinned` AND
    /// `sidecar.post_commit_pin == lance_head`. The writer's
    /// `commit_staged` for this table succeeded; only Phase C did not
    /// land. Eligible for roll-forward (in the all-or-nothing decision).
    RolledPastExpected,
    /// `lance_head == manifest_pinned + 1` but the sidecar's
    /// `expected_version`/`post_commit_pin` don't match. Some other writer
    /// or recovery action moved this table. Roll back to
    /// `sidecar.expected_version`.
    UnexpectedAtP1,
    /// `lance_head > manifest_pinned + 1`. Multi-step orphan from a
    /// previous restore attempt or an external mutation. Roll back to
    /// `sidecar.expected_version`.
    UnexpectedMultistep,
    /// `lance_head < manifest_pinned`. Should be impossible: the manifest
    /// pin can only advance after a successful Lance commit. Surface
    /// loudly and abort recovery.
    InvariantViolation { observed: u64 },
}

/// Per-sidecar decision derived from the table classifications.
///
/// **All-or-nothing**: the writer that produced the sidecar intended an
/// atomic publish across every table it listed. Rolling forward only some
/// of them would publish a partial commit and violate `docs/invariants.md`
/// §VI.23. The decision is based on the worst classification:
///
/// - Any `InvariantViolation` → `Abort` (operator action required).
/// - Any `UnexpectedAtP1` / `UnexpectedMultistep` / `NoMovement` →
///   `RollBack` all `RolledPastExpected` tables to `expected_version`.
/// - All `RolledPastExpected` → `RollForward` every table in one
///   manifest publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SidecarDecision {
    /// All tables successfully reached Phase B for this writer; only the
    /// manifest publish (Phase C) didn't land. Roll the pin forward atomically.
    RollForward,
    /// Some tables didn't reach Phase B (or sidecar doesn't match observed state).
    /// Roll back the rolled-past-expected ones; leave the no-movement ones alone.
    RollBack,
    /// An invariant was violated. Refuse to act; surface for operator review.
    Abort,
}

/// Build the `__recovery/` directory URI under a repo root.
pub(crate) fn recovery_dir_uri(root_uri: &str) -> String {
    let trimmed = root_uri.trim_end_matches('/');
    format!("{}/{}", trimmed, RECOVERY_DIR_NAME)
}

/// Build the URI for a specific sidecar (`__recovery/{operation_id}.json`).
pub(crate) fn sidecar_uri(root_uri: &str, operation_id: &str) -> String {
    let dir = recovery_dir_uri(root_uri);
    format!("{}/{}.json", dir, operation_id)
}

/// Write a sidecar atomically and return a handle for later deletion.
///
/// The atomicity contract is inherited from [`StorageAdapter::write_text`]:
/// LocalStorageAdapter writes via `tokio::fs::write` (whole-file replace);
/// S3StorageAdapter writes via PutObject (atomic at the object level).
/// Both are sufficient for sidecar semantics — readers either see the
/// complete sidecar or none.
pub(crate) async fn write_sidecar(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
) -> Result<RecoverySidecarHandle> {
    debug_assert_eq!(sidecar.schema_version, SIDECAR_SCHEMA_VERSION);
    let uri = sidecar_uri(root_uri, &sidecar.operation_id);
    let json = serde_json::to_string_pretty(sidecar).map_err(|err| {
        OmniError::manifest_internal(format!("failed to serialize recovery sidecar: {}", err))
    })?;
    storage.write_text(&uri, &json).await?;
    Ok(RecoverySidecarHandle {
        operation_id: sidecar.operation_id.clone(),
        sidecar_uri: uri,
    })
}

/// Delete a sidecar after Phase C succeeded. Idempotent (safe to retry).
pub(crate) async fn delete_sidecar(
    handle: &RecoverySidecarHandle,
    storage: &dyn StorageAdapter,
) -> Result<()> {
    storage.delete(&handle.sidecar_uri).await
}

/// Read every sidecar under `__recovery/`. Returns an empty vec if the
/// directory does not exist or is empty (the steady-state path).
///
/// Sidecars whose `schema_version` is unsupported by this binary are NOT
/// silently skipped — the function returns an error so an operator can
/// investigate. Rationale: a sidecar with an unknown shape may encode
/// state we don't know how to recover; better to fail open than guess.
pub(crate) async fn list_sidecars(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<Vec<RecoverySidecar>> {
    let dir = recovery_dir_uri(root_uri);
    let uris = storage.list_dir(&dir).await?;
    let mut out = Vec::with_capacity(uris.len());
    for uri in uris {
        // Skip non-JSON files defensively; the directory is ours but a
        // future feature might leave other artifacts here.
        if !uri.ends_with(".json") {
            continue;
        }
        let body = storage.read_text(&uri).await?;
        let sidecar = parse_sidecar(&uri, &body)?;
        out.push(sidecar);
    }
    Ok(out)
}

/// Parse a sidecar body, enforcing the schema-version refusal policy.
/// Exposed separately so unit tests can exercise the parse path without
/// going through storage.
pub(crate) fn parse_sidecar(sidecar_uri: &str, body: &str) -> Result<RecoverySidecar> {
    // First check the schema_version peek — gives a typed error before we
    // try to deserialize the rest of the structure (which might fail with
    // a less-helpful "missing field" message).
    #[derive(Deserialize)]
    struct Peek {
        schema_version: u32,
    }
    let peek: Peek = serde_json::from_str(body).map_err(|err| {
        OmniError::manifest_internal(format!(
            "recovery sidecar at '{}' is not valid JSON: {}",
            sidecar_uri, err
        ))
    })?;
    if peek.schema_version != SIDECAR_SCHEMA_VERSION {
        return Err(SidecarSchemaError {
            sidecar_uri: sidecar_uri.to_string(),
            found_version: peek.schema_version,
            supported_version: SIDECAR_SCHEMA_VERSION,
        }
        .into());
    }
    serde_json::from_str(body).map_err(|err| {
        OmniError::manifest_internal(format!(
            "recovery sidecar at '{}' failed to deserialize: {}",
            sidecar_uri, err
        ))
    })
}

/// Classify one table's observed state vs. the sidecar's intent.
///
/// `kind` adjusts the precision of the `RolledPastExpected` predicate:
/// - **Strict** (`Mutation`, `Load`, `BranchMerge`): exactly one
///   `commit_staged` per table, so `lance_head == manifest_pinned + 1`
///   AND `post_commit_pin == lance_head` is required.
/// - **Loose** (`SchemaApply`, `EnsureIndices`): the writer may run
///   N ≥ 1 `commit_staged` calls per table (one per index built + one
///   for the overwrite, etc.) and the exact N is hard to compute at
///   sidecar-write time. The loose match accepts any
///   `lance_head > manifest_pinned` as `RolledPastExpected` when
///   `pin.expected_version == manifest_pinned` (the writer's CAS
///   target matches what the manifest currently shows). The risk this
///   admits — an external agent advancing HEAD between sidecar write
///   and recovery — is out of scope for the single-coordinator model
///   (MR-668 territory).
pub(crate) fn classify_table(
    pin: &SidecarTablePin,
    lance_head: u64,
    manifest_pinned: u64,
    kind: SidecarKind,
) -> TableClassification {
    use TableClassification::*;
    if lance_head < manifest_pinned {
        return InvariantViolation {
            observed: lance_head,
        };
    }
    if lance_head == manifest_pinned {
        return NoMovement;
    }
    // lance_head > manifest_pinned
    let strict = matches!(
        kind,
        SidecarKind::Mutation | SidecarKind::Load | SidecarKind::BranchMerge,
    );
    if strict {
        if lance_head == manifest_pinned + 1 {
            if pin.expected_version == manifest_pinned && pin.post_commit_pin == lance_head {
                RolledPastExpected
            } else {
                UnexpectedAtP1
            }
        } else {
            // lance_head > manifest_pinned + 1
            UnexpectedMultistep
        }
    } else {
        // Loose match for multi-commit writers (SchemaApply, EnsureIndices).
        if pin.expected_version == manifest_pinned {
            RolledPastExpected
        } else if lance_head == manifest_pinned + 1 {
            UnexpectedAtP1
        } else {
            UnexpectedMultistep
        }
    }
}

/// Compute the per-sidecar decision from a slice of table classifications.
///
/// All-or-nothing per `docs/invariants.md` §VI.23 — see [`SidecarDecision`].
pub(crate) fn decide(classifications: &[TableClassification]) -> SidecarDecision {
    use SidecarDecision::*;
    use TableClassification::*;
    if classifications.iter().any(|c| matches!(c, InvariantViolation { .. })) {
        return Abort;
    }
    if classifications
        .iter()
        .any(|c| matches!(c, NoMovement | UnexpectedAtP1 | UnexpectedMultistep))
    {
        return RollBack;
    }
    // All RolledPastExpected (or the slice is empty — no-op trivially).
    RollForward
}

/// Restore a single table's Lance HEAD to `expected_version`, producing a
/// new commit at HEAD+1 with content == content-at-`expected_version`.
///
/// Idempotency: if the latest Lance commit's fragment-id set already equals
/// the fragment-id set at `expected_version`, this is a no-op. Soundness —
/// Lance fragments are immutable; equal fragment-ids ⇒ equal content.
/// This guards against version pile-up under repeated mid-rollback crashes
/// (see `docs/runs.md` "Finalize → publisher residual" + `.context/mr-847-design.md`
/// §"Fragment-set equality short-circuit").
pub(crate) async fn restore_table_to_version(
    table_path: &str,
    expected_version: u64,
) -> Result<()> {
    let head = Dataset::open(table_path)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let target = head
        .checkout_version(expected_version)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    if fragment_ids(&head) == fragment_ids(&target) {
        // Lance HEAD already reflects target content (a prior restore
        // landed; we just didn't get to delete the sidecar). No-op.
        return Ok(());
    }

    // checkout returns a NEW Dataset; restore() takes &mut self.
    let mut to_restore = target;
    to_restore
        .restore()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(())
}

fn fragment_ids(ds: &Dataset) -> Vec<u64> {
    let mut ids: Vec<u64> = ds.manifest.fragments.iter().map(|f| f.id).collect();
    ids.sort_unstable();
    ids
}

/// Open-time recovery sweep — the entry point invoked from
/// `Omnigraph::open` (gated on `OpenMode::ReadWrite`).
///
/// Enumerates every sidecar in `__recovery/`, classifies each table per
/// the sidecar's intent, and applies the all-or-nothing decision:
/// roll-forward (every table eligible), roll-back (mixed or unexpected
/// state), or abort (invariant violation).
///
/// **Phase 3 scope** (this commit): roll-back path is fully implemented;
/// roll-forward errors out with a "Phase 4" placeholder so the
/// open-time wiring + sidecar I/O + classification + decision dispatch
/// can land independently of the audit/manifest-publish work. Tests
/// exercising the end-to-end roll-forward path land alongside Phase 4.
///
/// Idempotency: a crash mid-sweep leaves the sidecar (deletion is the
/// final step). Re-opening re-classifies; the fragment-set short-circuit
/// in [`restore_table_to_version`] prevents version pile-up under
/// repeated mid-rollback crashes.
///
/// Concurrency: today (pre-MR-686) recovery runs synchronously in
/// `Omnigraph::open` *before* the engine is wrapped in the server's
/// `Arc<RwLock<Omnigraph>>`. No request handlers can race. Under MR-686
/// + MR-856 (background reconciler) the per-(table_key, branch) queues
/// will need acquisition before the sweep restores or publishes — see
/// `.context/mr-847-design.md` "Concurrency policy" §"After MR-686".
pub(crate) async fn recover_manifest_drift(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    snapshot: &Snapshot,
) -> Result<()> {
    let sidecars = list_sidecars(root_uri, storage).await?;
    if sidecars.is_empty() {
        return Ok(());
    }

    for sidecar in sidecars {
        process_sidecar(root_uri, storage, snapshot, &sidecar).await?;
    }
    Ok(())
}

async fn process_sidecar(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    let mut classifications = Vec::with_capacity(sidecar.tables.len());
    for pin in &sidecar.tables {
        let lance_head = open_lance_head(&pin.table_path).await?;
        let manifest_pinned = snapshot
            .entry(&pin.table_key)
            .map(|e| e.table_version)
            .unwrap_or(0);
        classifications.push(classify_table(
            pin,
            lance_head,
            manifest_pinned,
            sidecar.writer_kind,
        ));
    }

    match decide(&classifications) {
        SidecarDecision::Abort => {
            // Surface loudly without deleting the sidecar — operator must
            // investigate. This includes any classification of
            // InvariantViolation (Lance HEAD < manifest pinned: should be
            // impossible).
            Err(OmniError::manifest_internal(format!(
                "recovery sidecar '{}' has invariant violation; refusing to act \
                 — operator review required (sidecar at '{}', classifications: {:?})",
                sidecar.operation_id,
                sidecar_uri(root_uri, &sidecar.operation_id),
                classifications,
            )))
        }
        SidecarDecision::RollBack => {
            warn!(
                operation_id = sidecar.operation_id.as_str(),
                writer_kind = ?sidecar.writer_kind,
                "recovery: rolling back sidecar (mixed or unexpected state)"
            );
            // Restore every table whose Lance HEAD has drifted from the
            // manifest pin (RolledPastExpected, UnexpectedAtP1,
            // UnexpectedMultistep). NoMovement tables are already at
            // expected_version — no action. The fragment-set short-circuit
            // in restore_table_to_version makes drift-with-equivalent-content
            // a no-op (sound: equal fragment-ids ⇒ equal content).
            let mut outcomes = Vec::with_capacity(sidecar.tables.len());
            for (pin, cls) in sidecar.tables.iter().zip(classifications.iter()) {
                if matches!(
                    cls,
                    TableClassification::RolledPastExpected
                        | TableClassification::UnexpectedAtP1
                        | TableClassification::UnexpectedMultistep
                ) {
                    restore_table_to_version(&pin.table_path, pin.expected_version).await?;
                    outcomes.push(TableOutcome {
                        table_key: pin.table_key.clone(),
                        from_version: snapshot
                            .entry(&pin.table_key)
                            .map(|e| e.table_version)
                            .unwrap_or(0),
                        to_version: pin.expected_version,
                    });
                }
            }
            // Manifest pin doesn't move on rollback; record an audit-only
            // commit at the existing version so operators can correlate via
            // `omnigraph commit list --filter actor=omnigraph:recovery`.
            record_audit(
                root_uri,
                sidecar,
                snapshot.version(),
                RecoveryKind::RolledBack,
                outcomes,
            )
            .await?;
            delete_sidecar_by_operation_id(root_uri, storage, &sidecar.operation_id).await?;
            Ok(())
        }
        SidecarDecision::RollForward => {
            warn!(
                operation_id = sidecar.operation_id.as_str(),
                writer_kind = ?sidecar.writer_kind,
                "recovery: rolling forward sidecar (Phase B completed; \
                 Phase C did not land)"
            );
            let new_manifest_version = roll_forward_all(root_uri, sidecar).await?;
            let outcomes: Vec<TableOutcome> = sidecar
                .tables
                .iter()
                .map(|pin| TableOutcome {
                    table_key: pin.table_key.clone(),
                    from_version: pin.expected_version,
                    to_version: pin.post_commit_pin,
                })
                .collect();
            record_audit(
                root_uri,
                sidecar,
                new_manifest_version,
                RecoveryKind::RolledForward,
                outcomes,
            )
            .await?;
            delete_sidecar_by_operation_id(root_uri, storage, &sidecar.operation_id).await?;
            Ok(())
        }
    }
}

/// Atomically extend every table's manifest pin from `expected_version` to
/// `post_commit_pin` via a single `ManifestBatchPublisher::publish` call.
/// Returns the new manifest version produced by the publish.
///
/// All-or-nothing at the substrate: the publish writes one `__manifest`
/// row-level CAS that either advances every listed pin together or fails
/// with `ExpectedVersionMismatch` (no partial publish). The publisher's
/// internal `PUBLISHER_RETRY_BUDGET = 5` handles transient row-level CAS
/// contention; persistent contention surfaces the typed conflict error to
/// the recovery sweep, which leaves the sidecar in place for the next
/// open's retry.
async fn roll_forward_all(root_uri: &str, sidecar: &RecoverySidecar) -> Result<u64> {
    let mut updates: Vec<ManifestChange> = Vec::with_capacity(sidecar.tables.len());
    let mut expected: HashMap<String, u64> = HashMap::with_capacity(sidecar.tables.len());

    for pin in &sidecar.tables {
        // Open the dataset at its CURRENT Lance HEAD (not at the sidecar's
        // post_commit_pin). For strict-match writers (Mutation/Load/
        // BranchMerge) HEAD == post_commit_pin by construction. For
        // loose-match writers (SchemaApply/EnsureIndices) HEAD may be
        // higher than post_commit_pin (multiple commit_staged calls per
        // table); we want to publish to the actual current HEAD.
        let head_ds = Dataset::open(&pin.table_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let head_version = head_ds.version().version;

        let row_count = head_ds
            .count_rows(None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))? as u64;

        let table_relative_path = super::table_path_for_table_key(&pin.table_key)?;
        let version_metadata =
            super::metadata::TableVersionMetadata::from_dataset(
                root_uri,
                &table_relative_path,
                &head_ds,
            )?;

        updates.push(ManifestChange::Update(SubTableUpdate {
            table_key: pin.table_key.clone(),
            table_version: head_version,
            table_branch: sidecar.branch.clone(),
            row_count,
            version_metadata,
        }));
        expected.insert(pin.table_key.clone(), pin.expected_version);
    }

    let publisher = GraphNamespacePublisher::new(root_uri, sidecar.branch.as_deref());
    let new_dataset = publisher.publish(&updates, &expected).await?;
    Ok(new_dataset.version().version)
}

/// Append the audit row describing this recovery action.
///
/// Two-part write: (a) `_graph_commits.lance` row anchored on the recovery
/// actor (`omnigraph:recovery`); (b) `_graph_commit_recoveries.lance` row
/// linking back to (a) and naming the original actor + per-table outcomes.
/// Same not-atomic-pair-write shape as the existing `_graph_commits`
/// + `_graph_commit_actors` split — a crash between the two leaves an
/// orphan commit row with no audit row. The recovery sweep tolerates this:
/// on re-entry the classifier surfaces `NoMovement` for already-restored /
/// already-published tables, the action is a no-op, and the audit append
/// is retried.
async fn record_audit(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    manifest_version: u64,
    kind: RecoveryKind,
    outcomes: Vec<TableOutcome>,
) -> Result<()> {
    let mut graph = CommitGraph::open(root_uri).await?;
    let graph_commit_id = graph
        .append_commit(
            sidecar.branch.as_deref(),
            manifest_version,
            Some(RECOVERY_ACTOR),
        )
        .await?;
    let mut audit = RecoveryAudit::open(root_uri).await?;
    audit
        .append(RecoveryAuditRecord {
            graph_commit_id,
            recovery_kind: kind,
            recovery_for_actor: sidecar.actor_id.clone(),
            operation_id: sidecar.operation_id.clone(),
            sidecar_writer_kind: format!("{:?}", sidecar.writer_kind),
            per_table_outcomes: outcomes,
            created_at: now_micros()?,
        })
        .await?;
    Ok(())
}

async fn open_lance_head(table_path: &str) -> Result<u64> {
    let ds = Dataset::open(table_path)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(ds.version().version)
}

async fn delete_sidecar_by_operation_id(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    operation_id: &str,
) -> Result<()> {
    storage.delete(&sidecar_uri(root_uri, operation_id)).await
}

/// Convenience: build a [`RecoverySidecar`] with auto-generated
/// `operation_id` and `started_at`. The caller fills in the other fields.
pub(crate) fn new_sidecar(
    writer_kind: SidecarKind,
    branch: Option<String>,
    actor_id: Option<String>,
    tables: Vec<SidecarTablePin>,
) -> RecoverySidecar {
    use std::time::{SystemTime, UNIX_EPOCH};
    let operation_id = ulid::Ulid::new().to_string();
    let started_at = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => format!("{}", d.as_micros()),
        Err(_) => "0".to_string(),
    };
    RecoverySidecar {
        schema_version: SIDECAR_SCHEMA_VERSION,
        operation_id,
        started_at,
        branch,
        actor_id,
        writer_kind,
        tables,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use crate::storage::LocalStorageAdapter;
    use crate::table_store::TableStore;

    fn person_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]))
    }

    fn person_batch(rows: &[(&str, Option<i32>)]) -> RecordBatch {
        let ids: Vec<&str> = rows.iter().map(|(id, _)| *id).collect();
        let ages: Vec<Option<i32>> = rows.iter().map(|(_, age)| *age).collect();
        RecordBatch::try_new(
            person_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int32Array::from(ages)),
            ],
        )
        .unwrap()
    }

    fn make_pin(table_key: &str, table_path: &str, expected: u64, post: u64) -> SidecarTablePin {
        SidecarTablePin {
            table_key: table_key.to_string(),
            table_path: table_path.to_string(),
            expected_version: expected,
            post_commit_pin: post,
        }
    }

    #[test]
    fn sidecar_round_trips_through_json() {
        let original = new_sidecar(
            SidecarKind::Mutation,
            Some("main".to_string()),
            Some("act-alice".to_string()),
            vec![make_pin("node:Person", "file:///tmp/people.lance", 5, 6)],
        );
        let json = serde_json::to_string(&original).unwrap();
        let parsed = parse_sidecar("file:///tmp/__recovery/x.json", &json).unwrap();
        assert_eq!(parsed.schema_version, SIDECAR_SCHEMA_VERSION);
        assert_eq!(parsed.operation_id, original.operation_id);
        assert_eq!(parsed.writer_kind, SidecarKind::Mutation);
        assert_eq!(parsed.branch.as_deref(), Some("main"));
        assert_eq!(parsed.actor_id.as_deref(), Some("act-alice"));
        assert_eq!(parsed.tables.len(), 1);
        assert_eq!(parsed.tables[0].table_key, "node:Person");
    }

    #[test]
    fn parse_sidecar_refuses_unknown_schema_version() {
        let body = r#"{
            "schema_version": 99,
            "operation_id": "01H000000000000000000000XX",
            "started_at": "0",
            "branch": null,
            "actor_id": null,
            "writer_kind": "Mutation",
            "tables": []
        }"#;
        let err = parse_sidecar("file:///tmp/__recovery/x.json", body).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("schema_version=99") && msg.contains("supports only schema_version=1"),
            "expected SidecarSchemaError mentioning the version mismatch, got: {}",
            msg,
        );
    }

    #[test]
    fn classify_no_movement_when_head_equals_pinned() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 5, 5, SidecarKind::Mutation),
            TableClassification::NoMovement,
        );
    }

    #[test]
    fn classify_rolled_past_expected_when_sidecar_matches_strict() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 6, 5, SidecarKind::Mutation),
            TableClassification::RolledPastExpected,
        );
    }

    #[test]
    fn classify_unexpected_at_p1_when_sidecar_does_not_match_strict() {
        // Same +1 drift but post_commit_pin says it should be 7, not 6.
        let pin = make_pin("node:Person", "irrelevant", 5, 7);
        assert_eq!(
            classify_table(&pin, 6, 5, SidecarKind::Mutation),
            TableClassification::UnexpectedAtP1,
        );
    }

    #[test]
    fn classify_unexpected_multistep_when_head_jumped_more_than_one_strict() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 8, 5, SidecarKind::Mutation),
            TableClassification::UnexpectedMultistep,
        );
    }

    #[test]
    fn classify_invariant_violation_when_head_below_pinned() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 3, 5, SidecarKind::Mutation),
            TableClassification::InvariantViolation { observed: 3 },
        );
    }

    // Loose-match writers (SchemaApply, EnsureIndices) accept any
    // lance_head > expected_version as RolledPastExpected when the
    // expected version still matches the manifest pin. The exact
    // post_commit_pin is allowed to be a lower bound.
    #[test]
    fn classify_loose_match_accepts_multi_commit_drift_for_schema_apply() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        // Sidecar's post_commit_pin says 6, but Lance HEAD is 8 (SchemaApply
        // built two indices). Strict would say UnexpectedMultistep; loose
        // accepts it as RolledPastExpected.
        assert_eq!(
            classify_table(&pin, 8, 5, SidecarKind::SchemaApply),
            TableClassification::RolledPastExpected,
        );
    }

    #[test]
    fn classify_loose_match_accepts_multi_commit_drift_for_ensure_indices() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 9, 5, SidecarKind::EnsureIndices),
            TableClassification::RolledPastExpected,
        );
    }

    #[test]
    fn classify_loose_match_no_movement_unchanged() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 5, 5, SidecarKind::SchemaApply),
            TableClassification::NoMovement,
        );
    }

    #[test]
    fn classify_loose_match_invariant_violation_unchanged() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 3, 5, SidecarKind::SchemaApply),
            TableClassification::InvariantViolation { observed: 3 },
        );
    }

    #[test]
    fn decide_roll_forward_when_all_classifications_match() {
        let cls = vec![
            TableClassification::RolledPastExpected,
            TableClassification::RolledPastExpected,
        ];
        assert_eq!(decide(&cls), SidecarDecision::RollForward);
    }

    #[test]
    fn decide_roll_back_on_mid_phase_b_crash_mix() {
        // Mid-Phase-B crash: one table iterated (RolledPastExpected),
        // another not yet iterated (NoMovement).
        let cls = vec![
            TableClassification::RolledPastExpected,
            TableClassification::NoMovement,
        ];
        assert_eq!(decide(&cls), SidecarDecision::RollBack);
    }

    #[test]
    fn decide_roll_back_on_unexpected_at_p1() {
        let cls = vec![
            TableClassification::RolledPastExpected,
            TableClassification::UnexpectedAtP1,
        ];
        assert_eq!(decide(&cls), SidecarDecision::RollBack);
    }

    #[test]
    fn decide_abort_on_invariant_violation() {
        let cls = vec![
            TableClassification::RolledPastExpected,
            TableClassification::InvariantViolation { observed: 1 },
        ];
        assert_eq!(decide(&cls), SidecarDecision::Abort);
    }

    #[test]
    fn decide_roll_forward_on_empty_slice() {
        // Degenerate case: no tables in the sidecar. Vacuously RollForward
        // (and the executor will iterate zero tables).
        assert_eq!(decide(&[]), SidecarDecision::RollForward);
    }

    #[tokio::test]
    async fn restore_table_to_version_appends_one_commit() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
        let store = TableStore::new(dir.path().to_str().unwrap());

        let mut ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
            .await
            .unwrap();
        store
            .append_batch(&uri, &mut ds, person_batch(&[("bob", Some(25))]))
            .await
            .unwrap();
        store
            .append_batch(&uri, &mut ds, person_batch(&[("carol", Some(40))]))
            .await
            .unwrap();
        let head_before = ds.version().version;
        assert_eq!(head_before, 3);

        restore_table_to_version(&uri, 1).await.unwrap();

        let post = Dataset::open(&uri).await.unwrap();
        assert_eq!(post.version().version, head_before + 1);
        // Content matches v1 (just alice).
        let scanner = post.scan();
        let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(
            scanner.try_into_stream().await.unwrap(),
        )
        .await
        .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn restore_table_to_version_no_ops_when_fragments_already_match() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
        let store = TableStore::new(dir.path().to_str().unwrap());

        let mut ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
            .await
            .unwrap();
        store
            .append_batch(&uri, &mut ds, person_batch(&[("bob", Some(25))]))
            .await
            .unwrap();
        // First restore: HEAD goes from 2 to 3 (with content == v1).
        restore_table_to_version(&uri, 1).await.unwrap();
        let mid = Dataset::open(&uri).await.unwrap().version().version;
        assert_eq!(mid, 3);

        // Second restore to v1: content already matches; no-op.
        restore_table_to_version(&uri, 1).await.unwrap();
        let post = Dataset::open(&uri).await.unwrap().version().version;
        assert_eq!(
            post, mid,
            "second restore must short-circuit via fragment-set equality"
        );
    }

    #[tokio::test]
    async fn list_sidecars_returns_empty_when_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalStorageAdapter::default();
        let result = list_sidecars(dir.path().to_str().unwrap(), &storage)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn write_then_list_then_delete_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        // Create the __recovery/ subdir so write_sidecar's parent exists
        // (LocalStorageAdapter::write_text doesn't mkdir parents).
        std::fs::create_dir(dir.path().join(RECOVERY_DIR_NAME)).unwrap();
        let storage = LocalStorageAdapter::default();
        let root = dir.path().to_str().unwrap();

        let sidecar = new_sidecar(
            SidecarKind::Mutation,
            Some("main".to_string()),
            Some("act-alice".to_string()),
            vec![make_pin("node:Person", "file:///tmp/x.lance", 5, 6)],
        );
        let handle = write_sidecar(root, &storage, &sidecar).await.unwrap();
        assert_eq!(handle.operation_id, sidecar.operation_id);

        let listed = list_sidecars(root, &storage).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].operation_id, sidecar.operation_id);

        delete_sidecar(&handle, &storage).await.unwrap();
        let after = list_sidecars(root, &storage).await.unwrap();
        assert!(after.is_empty());
    }

    #[tokio::test]
    async fn list_sidecars_skips_non_json_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(RECOVERY_DIR_NAME)).unwrap();
        // Drop a non-JSON file the sweep must ignore (e.g., .DS_Store).
        std::fs::write(
            dir.path().join(RECOVERY_DIR_NAME).join(".DS_Store"),
            "noise",
        )
        .unwrap();
        let storage = LocalStorageAdapter::default();
        let result = list_sidecars(dir.path().to_str().unwrap(), &storage)
            .await
            .unwrap();
        assert!(result.is_empty());
    }
}
