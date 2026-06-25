//! Recovery-on-open primitives.
//!
//! This module implements the building blocks of the per-sidecar recovery
//! sweep that closes the documented Phase B → Phase C residual (see
//! `docs/dev/writes.md` "Open-time recovery sweep"). The high-level shape:
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
//!    drifted tables to the manifest-pinned version.
//!
//! ## Verified Lance behavior the rollback path depends on
//!
//! - `Dataset::restore()` takes no version arg; restores
//!   `self.manifest.version` (currently checked-out version). From HEAD =
//!   `h`, produces a new commit at `h + 1` with content == checked-out
//!   version. Pinned by
//!   `tests/staged_writes.rs::lance_restore_appends_one_commit_with_checked_out_content`.
//! - `Dataset::restore` "wins" against concurrent Append/Update/Delete/
//!   CreateIndex/Merge — see `check_restore_txn` at lance-6.0.1
//!   `src/io/commit/conflict_resolver.rs:986`. The hazard is documented
//!   by `tests/staged_writes.rs::lance_restore_loses_to_concurrent_append_via_orphaning`.
//!   The open-time sweep sidesteps the hazard by running before any
//!   other writers can race; the in-process heal
//!   ([`heal_pending_sidecars_roll_forward`]) never restores (roll-
//!   forward only) and guards via per-(table_key, branch) queue
//!   acquisition.

use std::collections::HashMap;

use lance::Dataset;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::db::graph_coordinator::GraphCoordinator;
use crate::db::recovery_audit::{RecoveryAudit, RecoveryAuditRecord, RecoveryKind, TableOutcome};
use crate::db::schema_state::SchemaStateRecovery;
use crate::error::{OmniError, Result};
use crate::storage::StorageAdapter;

use super::Snapshot;
use super::publisher::{GraphNamespacePublisher, LineageIntent, ManifestBatchPublisher};
use super::{ManifestChange, SubTableUpdate, TableRegistration, TableTombstone};

/// System actor identifier recorded on every recovery commit. Operators
/// distinguish recovery commits from user commits in `omnigraph commit list`
/// by filtering on this actor; the original sidecar's actor (if any) flows
/// into the audit row's `recovery_for_actor` field.
pub(crate) const RECOVERY_ACTOR: &str = "omnigraph:recovery";

/// Publish a recovery action's manifest `updates` AND its recovery commit in one
/// CAS (RFC-013 Phase 7). The recovery commit's lineage (`graph_commit` +
/// `graph_head`) rides the same merge-insert as the table-version re-pin — there
/// is no separate `_graph_commits.lance` write and no manifest→commit-graph gap.
/// `updates` is empty for the no-table-change recovery paths (all-NoMovement
/// roll-back, stale-sidecar cleanup, orphaned-branch discard); the lineage rows
/// still publish, so the recovery commit is always durable.
///
/// The commit's first parent is resolved by the publisher (the live head of the
/// recovery's branch); its merged-in parent is the sidecar's recorded source
/// head for a rolled-forward branch merge, matching the pre-Phase-7 merge-commit
/// shape. Returns the new manifest version and the minted recovery commit id
/// (which the audit row references).
async fn publish_recovery_commit(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    kind: RecoveryKind,
    updates: &[ManifestChange],
    expected: &HashMap<String, u64>,
) -> Result<(u64, String)> {
    let merged_parent_commit_id = match (sidecar.writer_kind, kind) {
        (SidecarKind::BranchMerge, RecoveryKind::RolledForward) => {
            sidecar.merge_source_commit_id.clone()
        }
        _ => None,
    };
    let intent = LineageIntent {
        graph_commit_id: ulid::Ulid::new().to_string(),
        branch: sidecar.branch.clone(),
        actor_id: Some(RECOVERY_ACTOR.to_string()),
        merged_parent_commit_id,
        created_at: crate::db::now_micros()?,
    };
    let publisher = GraphNamespacePublisher::new(root_uri, sidecar.branch.as_deref());
    let outcome = publisher.publish(updates, expected, Some(&intent)).await?;
    Ok((outcome.dataset.version().version, intent.graph_commit_id))
}

/// Subdirectory under the graph root holding sidecar files.
pub(crate) const RECOVERY_DIR_NAME: &str = "__recovery";

/// Max sidecar JSON shape/semantics version this binary writes and understands.
/// The reader accepts every version `<= ` this and refuses only versions ABOVE
/// it (an older binary cannot guess semantics a newer writer baked in — see
/// [`SidecarSchemaError`] and [`parse_sidecar`]). Bump this whenever a change
/// alters how an existing field is *interpreted* (not merely adds an optional
/// one), and add a fixed `*_SCHEMA_VERSION` floor like the one below so older
/// generations keep their original semantics.
///
/// v1 → v2: Phase-B confirmation. A `BranchMerge` sidecar at v2 carries
/// `confirmed_version` and is classified strictly (unconfirmed ⇒ partial ⇒ roll
/// back); at v1 it predates confirmation and keeps the loose roll-forward. The
/// reader must distinguish the two, so this is a real version bump, not an
/// additive field.
pub(crate) const SIDECAR_SCHEMA_VERSION: u32 = 2;

/// The version at which Phase-B confirmation shipped. A `BranchMerge` sidecar is
/// confirmation-aware (strict classification) iff `schema_version >=` this.
/// FIXED at 2 — NOT derived from [`SIDECAR_SCHEMA_VERSION`] — so a future bump to
/// v3+ still treats v2 sidecars as confirmation-aware.
pub(crate) const CONFIRMATION_SCHEMA_VERSION: u32 = 2;

/// Selects which recovery actions are allowed in a sweep.
///
/// Open-time recovery (`Omnigraph::open` with `OpenMode::ReadWrite`)
/// runs the full sweep — `Dataset::restore` is safe because no other
/// writers are active yet. In-process recovery (called from
/// `Omnigraph::refresh` during a long-running server) must NOT call
/// `Dataset::restore`: it "wins" against concurrent Append/Update/
/// Delete/CreateIndex/Merge per `check_restore_txn`, silently orphaning
/// the concurrent writer's commit (pinned by
/// `tests/staged_writes.rs::lance_restore_loses_to_concurrent_append_via_orphaning`).
/// Roll-forward is safe under concurrency because
/// `ManifestBatchPublisher::publish` uses row-level CAS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryMode {
    /// Open-time: the full sweep. RolledPastExpected → roll forward;
    /// mixed/unexpected → roll back via `Dataset::restore`; invariant
    /// violation → abort with a loud error.
    Full,
    /// In-process (refresh): roll-forward only. Sidecars that would
    /// require restore or abort are LEFT ON DISK for the next ReadWrite
    /// open. Closes the common case (mutation/load finalize → publisher
    /// failure) without restart.
    RollForwardOnly,
}

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
    /// `optimize_all_tables` — Lance `compact_files` (reserve-fragments +
    /// rewrite commits) followed by a manifest publish of the compacted
    /// version. Loose-match like the other multi-commit writers; roll-forward
    /// is always safe because compaction is content-preserving (Lance
    /// `Operation::Rewrite` "reorganizes data without semantic modification").
    Optimize,
}

/// Which recovery-classification semantics a sidecar's tables use. Resolved once
/// from `(writer_kind, schema_version)` — see [`SidecarKind::classification_mode`]
/// — so [`classify_table`] dispatches on the mode instead of re-deriving it from
/// a kind×version match. Adding a writer kind or a version floor is then one arm
/// in the resolver, not a guard threaded through `classify_table`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClassificationMode {
    /// Exactly one `commit_staged` per table (`Mutation`, `Load`): require
    /// `lance_head == manifest_pinned + 1` and the pin to match.
    Strict,
    /// N ≥ 1 commits per table whose drift is content-preserving / derived
    /// state (`SchemaApply`, `EnsureIndices`, `Optimize`, and pre-confirmation
    /// `BranchMerge`): any `lance_head > manifest_pinned` rolls forward.
    Loose,
    /// Multi-commit publish of *distinct logical rows* with a recorded
    /// `confirmed_version` (`BranchMerge` at `schema_version >=
    /// CONFIRMATION_SCHEMA_VERSION`): roll forward ONLY to the confirmed
    /// version; an unconfirmed moved HEAD is a partial publish and rolls back.
    Confirmed,
}

impl SidecarKind {
    /// Resolve the classification mode for this writer at a given sidecar
    /// `schema_version`. Exhaustive over `SidecarKind`, so adding a variant is a
    /// compile error here until its recovery semantics are declared.
    pub(crate) fn classification_mode(self, schema_version: u32) -> ClassificationMode {
        match self {
            SidecarKind::Mutation | SidecarKind::Load => ClassificationMode::Strict,
            // BranchMerge gained two-phase confirmation at
            // `CONFIRMATION_SCHEMA_VERSION`. A sidecar written before that
            // carries no `confirmed_version` and must keep the prior loose
            // roll-forward — classifying it strictly would misread a *completed*
            // pre-upgrade merge as a partial and roll it back. (The read gate
            // already refused any version newer than this binary.)
            SidecarKind::BranchMerge => {
                if schema_version >= CONFIRMATION_SCHEMA_VERSION {
                    ClassificationMode::Confirmed
                } else {
                    ClassificationMode::Loose
                }
            }
            SidecarKind::SchemaApply | SidecarKind::EnsureIndices | SidecarKind::Optimize => {
                ClassificationMode::Loose
            }
        }
    }
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
    /// (typically `expected_version + 1`). For multi-commit writers this is
    /// only a *lower bound* — see `confirmed_version`.
    pub post_commit_pin: u64,
    /// Phase-B confirmation: the exact Lance HEAD this table reached once the
    /// writer's *entire* multi-commit publish for it finished, recorded by a
    /// second sidecar write immediately before the manifest publish (Phase C).
    /// `None` means Phase B did not complete (the writer crashed mid-publish),
    /// so the on-disk drift is a *partial* commit and recovery must roll the
    /// whole operation BACK rather than publish an incomplete state. Only the
    /// `BranchMerge` writer records this today (its per-table publish is
    /// append → upsert → delete, several HEAD advances that the manifest
    /// publish makes atomic); other writers leave it `None` and keep their
    /// existing loose roll-forward. Backward-compatible: absent on older
    /// sidecars.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_version: Option<u64>,
    /// Lance branch ref this table lives on (mirrors
    /// `SubTableEntry::table_branch`). Required for the recovery sweep
    /// to open the dataset at the correct ref — `Dataset::open(path)`
    /// alone returns the default ref (typically main), which would
    /// classify a feature-branch sidecar against main's HEAD and silently
    /// no-op or roll back the wrong table version. Optional for backward
    /// compatibility with older sidecars; `None` means main / default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_branch: Option<String>,
}

/// New-table registration captured by SchemaApply sidecars so recovery
/// can publish a `ManifestChange::RegisterTable` for tables that the
/// writer was about to create. Without this, added tables exist as
/// orphan datasets on disk after recovery while the live `_schema.pg`
/// declares types the manifest doesn't know about — `snapshot.entry()`
/// returns None when the engine tries to read them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SidecarTableRegistration {
    /// Stable identifier (`node:Tag`, `edge:WorksAt`, etc.).
    pub table_key: String,
    /// Graph-relative path the manifest will register
    /// (e.g. `nodes/{fnv1a64-hex}`); recovery joins this with `root_uri`
    /// to open the dataset Lance HEAD when constructing the
    /// accompanying `Update`.
    pub table_path: String,
    /// Lance branch ref the dataset lives on (None for main / default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_branch: Option<String>,
}

/// Tombstone metadata captured by SchemaApply sidecars so recovery can
/// publish a `ManifestChange::Tombstone` for tables the writer was
/// about to mark removed. Without this, tombstoned types stay visible
/// in the manifest snapshot after recovery even though the live
/// schema no longer declares them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SidecarTombstone {
    pub table_key: String,
    /// Manifest version at which this table was active before the
    /// tombstone — required by the publisher's CAS pre-check.
    pub tombstone_version: u64,
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
    /// For `SidecarKind::BranchMerge` only: the source branch's HEAD
    /// commit id at the time the sidecar was written. Used by the
    /// recovery sweep's audit step to call `append_merge_commit`
    /// (recording `merged_parent_commit_id`) instead of `append_commit`,
    /// so future merges between the same pair recognize "already up-to-
    /// date" and merge-base computations stay correct. Optional for
    /// backward compatibility — older sidecars (or non-BranchMerge
    /// kinds) carry `None` and recovery falls back to `append_commit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_source_commit_id: Option<String>,
    /// SchemaApply-only: tables the writer was about to register
    /// (added types + renamed targets). Recovery emits a
    /// `RegisterTable` + `Update` pair per entry so the manifest
    /// catches up to the live schema's declared type set.
    /// Backward-compat: empty / absent for older sidecars and
    /// non-SchemaApply writers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_registrations: Vec<SidecarTableRegistration>,
    /// SchemaApply-only: tables the writer was about to tombstone
    /// (removed types + renamed sources). Recovery emits a
    /// `ManifestChange::Tombstone` per entry.
    /// Backward-compat: empty / absent for older sidecars and
    /// non-SchemaApply writers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tombstones: Vec<SidecarTombstone>,
}

/// Opaque handle returned by [`write_sidecar`] so the caller can delete
/// the sidecar after Phase C succeeds. Holding the handle does NOT keep
/// the sidecar alive — it just records the URI to delete.
#[derive(Debug, Clone)]
pub(crate) struct RecoverySidecarHandle {
    pub(crate) operation_id: String,
    pub(crate) sidecar_uri: String,
}

/// Error returned when the sidecar's `schema_version` is NEWER than this binary
/// understands. We refuse-and-error rather than read-and-warn: an old binary
/// cannot guess what semantics a newer writer baked into a future shape. (Older
/// versions are accepted and interpreted with their original semantics — see
/// [`parse_sidecar`].) Operator action is required (typically: upgrade the
/// binary).
#[derive(Debug)]
pub(crate) struct SidecarSchemaError {
    pub sidecar_uri: String,
    pub found_version: u32,
    pub max_supported_version: u32,
}

impl std::fmt::Display for SidecarSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "recovery sidecar at '{}' declares schema_version={}, newer than the \
             maximum this binary supports (schema_version={}); refusing to interpret \
             — upgrade omnigraph or remove the sidecar with operator review",
            self.sidecar_uri, self.found_version, self.max_supported_version,
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
    /// or recovery action moved this table. Roll back to the manifest pin.
    UnexpectedAtP1,
    /// `lance_head > manifest_pinned + 1`. Multi-step orphan from a
    /// previous restore attempt or an external mutation. Roll back to
    /// the manifest pin.
    UnexpectedMultistep,
    /// A confirmation-using writer (`BranchMerge`) advanced this table's HEAD
    /// (`lance_head > manifest_pinned`) but the sidecar carries no
    /// `confirmed_version` — its multi-commit publish crashed mid-flight, so
    /// the drift is a *partial* commit (e.g. an append without its sibling
    /// upsert/delete). Roll back to the manifest pin; the whole operation is
    /// re-run from scratch. Distinct from `UnexpectedMultistep` so the audit
    /// records a partial Phase B, not a foreign mutation.
    IncompletePhaseB,
    /// `lance_head < manifest_pinned`. Should be impossible: the manifest
    /// pin can only advance after a successful Lance commit. Surface
    /// loudly and abort recovery.
    InvariantViolation { observed: u64 },
}

/// Per-sidecar decision derived from the table classifications.
///
/// **All-or-nothing**: the writer that produced the sidecar intended an
/// atomic publish across every table it listed. Rolling forward only some
/// of them would publish a partial commit and violate the manifest-atomic
/// graph visibility invariant in `docs/dev/invariants.md`. The decision is
/// based on the worst classification:
///
/// - Any `InvariantViolation` → `Abort` (operator action required).
/// - Any `UnexpectedAtP1` / `UnexpectedMultistep` / `NoMovement` →
///   `RollBack` all drifted tables to the manifest pin.
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

/// Build the `__recovery/` directory URI under a graph root.
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
/// the local backend publishes via a staged temp file + rename (atomic on
/// POSIX); object stores write via PutObject (atomic at the object level).
/// Both are sufficient for sidecar semantics — readers either see the
/// complete sidecar or none.
pub(crate) async fn write_sidecar(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
) -> Result<RecoverySidecarHandle> {
    // Failpoint: models a storage put failure (S3 PutObject / fs write)
    // in Phase A — every writer must abort before any HEAD advance.
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_SIDECAR_WRITE)?;
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

/// Phase-B confirmation: stamp each pin with the exact Lance HEAD its publish
/// reached, then re-write the sidecar in place (same object). Called once, after
/// the writer's whole multi-commit publish completed and before the manifest
/// publish (Phase C). Recovery then rolls forward ONLY to these confirmed
/// versions; a sidecar still missing them is a partial Phase B that rolls back.
///
/// Overwriting the same object is atomic (same contract as [`write_sidecar`]):
/// a torn rewrite is never observed, so recovery reads either the pre-confirm
/// sidecar (→ roll back, safe) or the confirmed one (→ roll forward). A failure
/// here leaves the pre-confirm sidecar, so the operation rolls back — correct.
///
/// SURVIVES the fragment-adopt work (unlike the row-level merge it currently
/// serves — see `AdoptDelta` in `exec/merge.rs`). The recovery sidecar is the
/// cross-table write-ahead log that makes a fast-forward-main commit
/// all-or-nothing across N tables, which a fragment graft still needs. What
/// narrows is the *within-table* reason for confirmation: once each table's
/// merge is a single graft commit, the multi-step partial window shrinks to one
/// commit, so the `BranchMerge` arm of `classify_table` could fold back into the
/// strict single-commit path and `IncompletePhaseB` retire. Do NOT delete this
/// with the row path — keep the sidecar; only simplify the classifier.
pub(crate) async fn confirm_sidecar_phase_b(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &mut RecoverySidecar,
    confirmed_versions: &HashMap<String, u64>,
) -> Result<()> {
    // Failpoint: models a storage failure on the confirmation write — the
    // pre-confirm sidecar stays on disk, so recovery rolls the operation back.
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_SIDECAR_CONFIRM)?;
    for pin in &mut sidecar.tables {
        // Every pinned table MUST have an achieved version. A miss means the
        // pin set and the publish `updates` diverged — fail loudly at the
        // producer rather than leave the pin unconfirmed, which recovery would
        // read as a partial Phase B and silently roll the whole (complete) merge
        // back. Today the two are kept in lockstep by construction; this guards
        // the invariant against a future edit to either filter.
        let version = confirmed_versions.get(&pin.table_key).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "confirm_sidecar_phase_b: no achieved version for pinned table '{}' \
                 (pins and publish updates diverged)",
                pin.table_key
            ))
        })?;
        pin.confirmed_version = Some(*version);
    }
    let uri = sidecar_uri(root_uri, &sidecar.operation_id);
    let json = serde_json::to_string_pretty(sidecar).map_err(|err| {
        OmniError::manifest_internal(format!("failed to serialize recovery sidecar: {}", err))
    })?;
    storage.write_text(&uri, &json).await
}

/// Delete a sidecar after Phase C succeeded. Idempotent (safe to retry).
pub(crate) async fn delete_sidecar(
    handle: &RecoverySidecarHandle,
    storage: &dyn StorageAdapter,
) -> Result<()> {
    // Failpoint: models a storage delete failure (S3 DeleteObject) in
    // Phase D — callers swallow it (the write already published) and the
    // stale sidecar is healed by the next write or open.
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_SIDECAR_DELETE)?;
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
    // Failpoint: models a storage list failure (S3 ListObjectsV2) — every
    // consumer (open-time sweep, write-entry heal) must fail loudly
    // rather than silently skipping recovery.
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_SIDECAR_LIST)?;
    let dir = recovery_dir_uri(root_uri);
    let mut uris = storage.list_dir(&dir).await?;
    // Sort by URI so the sweep processes sidecars deterministically.
    // Sidecar filenames are ULIDs, which are lexicographically sortable
    // === chronologically sortable; the older sidecar is processed
    // before the newer one. Without this sort, `list_dir` returns
    // filesystem-order results which are nondeterministic and can mask
    // ordering-sensitive bugs.
    uris.sort();
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
    // Accept every version we were built to understand (`<= max`); refuse only
    // versions NEWER than us. Interpreting older generations with their original
    // semantics (rather than refusing them) is what avoids billing operators to
    // drain pre-upgrade sidecars; classification then dispatches by version.
    if peek.schema_version > SIDECAR_SCHEMA_VERSION {
        return Err(SidecarSchemaError {
            sidecar_uri: sidecar_uri.to_string(),
            found_version: peek.schema_version,
            max_supported_version: SIDECAR_SCHEMA_VERSION,
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
/// - **Confirmation** (`BranchMerge`): the writer's per-table publish is
///   several HEAD advances (append → upsert → delete), so a bare
///   `lance_head > manifest_pinned` is ambiguous — it may be a *complete*
///   publish or a *partial* one crashed mid-sequence. The writer resolves
///   the ambiguity by recording the exact achieved version
///   (`confirmed_version`) only after the whole publish finished. So roll
///   forward ONLY to that confirmed version; a missing confirmation is a
///   partial commit (`IncompletePhaseB`) and rolls back. This is the safe
///   form of the loose match for writers where a partial would publish an
///   incomplete delta.
/// - **Strict** (`Mutation`, `Load`): exactly one `commit_staged` per
///   table, so `lance_head == manifest_pinned + 1` AND
///   `post_commit_pin == lance_head` is required.
/// - **Loose** (`SchemaApply`, `EnsureIndices`, `Optimize`): the writer
///   advances the Lance HEAD by N ≥ 1 commits per table (one per index
///   built + one for the overwrite, etc.; `Optimize` runs `compact_files`,
///   which commits reserve-fragments + rewrite) and the exact N is hard to
///   compute at sidecar-write time. The loose match accepts
///   any `lance_head > manifest_pinned` as `RolledPastExpected` when
///   `pin.expected_version == manifest_pinned` (the writer's CAS
///   target matches what the manifest currently shows). This is safe for
///   these writers because their drift is derived state (index coverage,
///   compaction) the reconciler reproduces — a partial roll-forward loses
///   no logical rows. The risk it admits — an external agent advancing HEAD
///   between sidecar write and recovery — is out of scope for the
///   single-coordinator model.
pub(crate) fn classify_table(
    pin: &SidecarTablePin,
    lance_head: u64,
    manifest_pinned: u64,
    kind: SidecarKind,
    schema_version: u32,
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
    // lance_head > manifest_pinned. The "which semantics" decision is resolved
    // once from (kind, schema_version); dispatch on it.
    match kind.classification_mode(schema_version) {
        ClassificationMode::Confirmed => {
            // Two-phase confirmation: roll forward only to the exact version the
            // writer recorded after its whole multi-commit publish completed. No
            // confirmation ⇒ the publish crashed mid-sequence ⇒ partial ⇒ roll
            // back. A confirmation that doesn't match the observed HEAD means a
            // foreign writer advanced the table — don't roll a surprise forward.
            match pin.confirmed_version {
                Some(confirmed)
                    if lance_head == confirmed && pin.expected_version == manifest_pinned =>
                {
                    RolledPastExpected
                }
                Some(_) => UnexpectedMultistep,
                None => IncompletePhaseB,
            }
        }
        ClassificationMode::Strict => {
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
        }
        ClassificationMode::Loose => {
            // Multi-commit writers whose drift is content-preserving / derived
            // state (and pre-confirmation BranchMerge sidecars): any
            // `lance_head > manifest_pinned` rolls forward when the CAS target
            // matches what the manifest currently shows.
            if pin.expected_version == manifest_pinned {
                RolledPastExpected
            } else if lance_head == manifest_pinned + 1 {
                UnexpectedAtP1
            } else {
                UnexpectedMultistep
            }
        }
    }
}

/// Compute the per-sidecar decision from a slice of table classifications.
///
/// All-or-nothing per `docs/dev/invariants.md` -- see [`SidecarDecision`].
pub(crate) fn decide(classifications: &[TableClassification]) -> SidecarDecision {
    use SidecarDecision::*;
    use TableClassification::*;
    if classifications
        .iter()
        .any(|c| matches!(c, InvariantViolation { .. }))
    {
        return Abort;
    }
    if classifications
        .iter()
        .any(|c| matches!(c, NoMovement | UnexpectedAtP1 | UnexpectedMultistep | IncompletePhaseB))
    {
        return RollBack;
    }
    // All RolledPastExpected (or the slice is empty — no-op trivially).
    RollForward
}

/// Restore a single table's Lance HEAD to `target_version`, producing a
/// new commit at HEAD+1 with content == content-at-`target_version`.
///
/// Always runs the actual `Dataset::restore` — there is NO fragment-set
/// short-circuit because equal fragment IDs do NOT imply equal content:
/// Lance index commits and deletion-vector updates change the manifest
/// (and therefore the user-visible state) without changing fragment IDs.
/// Skipping the restore in those cases would leave Lance HEAD ahead of
/// the manifest with no recovery artifact left.
///
/// Cost: a successful roll-back appends one restore commit and then publishes
/// the manifest to match (`roll_back_sidecar`), so the table converges
/// (`manifest == HEAD`) in one pass. Only repeated crashes *between* the restore
/// and that publish (rare) accumulate extra restore commits; each re-classified
/// roll-back restores again and `omnigraph cleanup` reclaims the surplus.
/// Bounded by the number of interrupted recovery iterations — typically 0.
pub(crate) async fn restore_table_to_version(
    table_path: &str,
    branch: Option<&str>,
    target_version: u64,
) -> Result<()> {
    let head = Dataset::open(table_path)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let head = match branch {
        Some(b) if b != "main" => head
            .checkout_branch(b)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?,
        _ => head,
    };
    let mut to_restore = head
        .checkout_version(target_version)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    to_restore
        .restore()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(())
}

/// In-process heal for pending recovery sidecars — the entry point for
/// long-lived handles (`Omnigraph::refresh` and the staged-write entry
/// points `load_as` / `mutate_as`).
///
/// Steady-state cost is one `list_dir` of `__recovery/` (typically
/// empty → immediate return), so write entry points can afford to call
/// this on every request. When sidecars exist, each is processed in
/// `RecoveryMode::RollForwardOnly`: the common Phase B → Phase C
/// residual (per-table `commit_staged` landed, manifest publish did
/// not) rolls forward in-process; rollback-eligible or invariant-
/// violating sidecars are deferred to the next ReadWrite open, exactly
/// as `Omnigraph::refresh` documents.
///
/// Concurrency: unlike the open-time sweep, this runs while other
/// writers may be in flight. Every sidecar writer (mutation/load
/// finalize, schema_apply, branch_merge, ensure_indices, optimize)
/// acquires its per-`(table_key, table_branch)` write queues *before*
/// `write_sidecar` and holds them until after `delete_sidecar` — so
/// acquiring the same queues here blocks until that writer either
/// finished (sidecar deleted; the existence re-check skips it) or died
/// (sidecar is genuinely orphaned; safe to process). Without this, the
/// heal could observe a live writer's sidecar in its commit→publish
/// window, roll it forward, and fail that writer's own publish CAS.
/// Lock order is queues → coordinator, matching every writer's
/// commit→publish path.
///
/// The schema-staging reconcile runs lazily, per SchemaApply sidecar,
/// AFTER that sidecar's queue guards are held and its existence is
/// re-confirmed — never up front. An up-front reconcile can promote a
/// LIVE schema apply's staging files and steal its commit (pinned by
/// `tests/failpoints.rs::heal_does_not_promote_live_schema_apply_staging`).
///
/// Returns `true` when at least one sidecar was processed (the caller
/// should invalidate per-snapshot caches).
pub(crate) async fn heal_pending_sidecars_roll_forward(
    root_uri: &str,
    storage: std::sync::Arc<dyn StorageAdapter>,
    coordinator: &tokio::sync::RwLock<GraphCoordinator>,
    write_queue: &crate::db::write_queue::WriteQueueManager,
) -> Result<bool> {
    let sidecars = list_sidecars(root_uri, storage.as_ref()).await?;
    if sidecars.is_empty() {
        return Ok(false);
    }
    let mut processed_any = false;
    for sidecar in sidecars {
        // Serialize against a possibly-live writer (see fn docs). Guards
        // are scoped per sidecar so two sidecars never hold queues
        // simultaneously (no cross-sidecar lock-order surface).
        let mut queue_keys: Vec<crate::db::write_queue::TableQueueKey> = sidecar
            .tables
            .iter()
            .map(|pin| (pin.table_key.clone(), pin.table_branch.clone()))
            .collect();
        let is_schema_apply = matches!(sidecar.writer_kind, SidecarKind::SchemaApply);
        if is_schema_apply {
            // A SchemaApply sidecar's per-table pins don't cover a
            // registration-only migration (no existing tables touched,
            // but staging files + a sidecar on disk). The schema-apply
            // writer holds this serialization key from before its
            // sidecar write until after its sidecar delete, so blocking
            // on it — then re-checking sidecar existence — guarantees
            // the writer is gone before the reconcile below mutates
            // schema staging.
            queue_keys.push(schema_apply_serial_queue_key());
        }
        let _guards = write_queue.acquire_many(&queue_keys).await;
        // Re-check after the wait: the writer we blocked on may have
        // completed Phase C and deleted its sidecar.
        if !storage
            .exists(&sidecar_uri(root_uri, &sidecar.operation_id))
            .await?
        {
            continue;
        }
        // Schema-staging reconcile, per SchemaApply sidecar, UNDER the
        // sidecar's guards: a sidecar still on disk after the queue wait
        // belongs to a dead writer, so promoting its staging files can no
        // longer race the live apply's own renames or steal its commit.
        // It also re-runs per sidecar, so a multi-sidecar pass never
        // classifies against a reconcile result an earlier roll-forward
        // staled. Non-SchemaApply sidecars never consult the value.
        let schema_state_recovery = if is_schema_apply {
            let snapshot = {
                let mut coord = coordinator.write().await;
                coord.refresh().await?;
                coord.snapshot()
            };
            crate::db::schema_state::recover_schema_state_files(
                root_uri,
                std::sync::Arc::clone(&storage),
                &snapshot,
            )
            .await?
        } else {
            SchemaStateRecovery::Noop
        };
        // Fresh per-branch snapshot — same rationale as
        // `recover_manifest_drift`: classify against the branch the
        // sidecar's writer targeted, refreshed after any prior
        // sidecar's roll-forward.
        let branch_snapshot = match sidecar.branch.as_deref() {
            Some(b) => {
                // Orphan check against the manifest's branch list (the
                // authority) BEFORE opening: a deferred sidecar whose
                // branch was deleted would otherwise wedge every write
                // on the dead-branch open.
                let branch_exists = {
                    let mut coord = coordinator.write().await;
                    coord.refresh().await?;
                    coord.all_branches().await?.iter().any(|name| name == b)
                };
                if !branch_exists {
                    discard_orphaned_branch_sidecar(root_uri, storage.as_ref(), &sidecar).await?;
                    processed_any = true;
                    continue;
                }
                let mut branch_coord =
                    GraphCoordinator::open_branch(root_uri, b, std::sync::Arc::clone(&storage))
                        .await?;
                branch_coord.refresh().await?;
                branch_coord.snapshot()
            }
            None => {
                let mut coord = coordinator.write().await;
                coord.refresh().await?;
                coord.snapshot()
            }
        };
        if process_sidecar(
            root_uri,
            &storage,
            &branch_snapshot,
            &sidecar,
            RecoveryMode::RollForwardOnly,
            schema_state_recovery,
        )
        .await?
        {
            processed_any = true;
        }
    }
    // Re-read coordinator state so the caller's handle observes the
    // post-heal manifest.
    coordinator.write().await.refresh().await?;
    Ok(processed_any)
}

/// Discard a sidecar whose branch no longer exists in the manifest (the
/// authority — callers must key the orphan classification off the branch
/// LIST, never off a `Not found` from an open, which could be a transient
/// storage error masking real recovery intent). The branch's tree and
/// per-table forks are already reclaimed, so the drift the sidecar pins is
/// unreachable and the sidecar is provably moot; leaving it would wedge
/// every heal (write entry) and every ReadWrite open on a dead-branch
/// open, with `repair` refusing while it pends. Records an
/// `OrphanedBranchDiscarded` audit row (commit appended on main — the
/// sidecar's own branch has no commit graph anymore).
async fn discard_orphaned_branch_sidecar(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
) -> Result<()> {
    warn!(
        operation_id = sidecar.operation_id.as_str(),
        writer_kind = ?sidecar.writer_kind,
        branch = sidecar.branch.as_deref().unwrap_or("<none>"),
        "recovery: discarding sidecar for a deleted branch (drift unreachable; audit recorded)"
    );
    let mut audit = RecoveryAudit::open(root_uri).await?;
    // Idempotency across a Phase D delete fault: the audit row + commit
    // land before the sidecar delete, so a failed delete re-enters here
    // with the audit already durable. Append only once per operation —
    // the retry's sole remaining job is finishing the delete. (Cold
    // path: the list scan runs only when an orphaned sidecar exists.)
    //
    // Documented residual: the commit append and the audit append are
    // two writes. A failure BETWEEN them leaves a recovery commit with
    // no audit row; the retry (keyed on the audit row, the operator-
    // facing record) appends a second commit before the audit lands —
    // bounded commit-graph noise, audit row still exactly-once. Same
    // not-atomic-pair-write tolerance as `record_audit` and the
    // manifest→commit-graph Known Gap; keying on commit rows instead
    // would need an operation_id column on `_graph_commits`, and
    // audit-before-commit would dangle the `graph_commit_id` join.
    let already_recorded = audit.list().await?.iter().any(|record| {
        record.operation_id == sidecar.operation_id
            && record.recovery_kind == RecoveryKind::OrphanedBranchDiscarded
    });
    if !already_recorded {
        // The orphan-discard commit is recorded on MAIN (the sidecar's own
        // branch is gone), via a lineage-only publish into `__manifest` (RFC-013
        // Phase 7) — no `_graph_commits.lance` row. The publisher stamps the
        // commit at the version it produces.
        let intent = LineageIntent {
            graph_commit_id: ulid::Ulid::new().to_string(),
            branch: None,
            actor_id: Some(RECOVERY_ACTOR.to_string()),
            merged_parent_commit_id: None,
            created_at: crate::db::now_micros()?,
        };
        let publisher = GraphNamespacePublisher::new(root_uri, None);
        publisher.publish(&[], &HashMap::new(), Some(&intent)).await?;
        // Failpoint: the residual window above — commit published, audit
        // not yet durable.
        crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_ORPHAN_DISCARD_AUDIT_APPEND)?;
        audit
            .append(RecoveryAuditRecord {
                graph_commit_id: intent.graph_commit_id,
                recovery_kind: RecoveryKind::OrphanedBranchDiscarded,
                recovery_for_actor: sidecar.actor_id.clone(),
                operation_id: sidecar.operation_id.clone(),
                sidecar_writer_kind: format!("{:?}", sidecar.writer_kind),
                per_table_outcomes: Vec::new(),
                created_at: crate::db::now_micros()?,
            })
            .await?;
    }
    let handle = RecoverySidecarHandle {
        operation_id: sidecar.operation_id.clone(),
        sidecar_uri: sidecar_uri(root_uri, &sidecar.operation_id),
    };
    delete_sidecar(&handle, storage).await
}

/// The write-queue key serializing schema-apply's sidecar lifecycle
/// against the write-entry heal. The schema-apply writer acquires it
/// (alongside its per-table keys) from before `write_sidecar` until
/// after `delete_sidecar`; the heal acquires it before reconciling
/// schema staging or processing a SchemaApply sidecar. The name cannot
/// collide with real table keys (those are `node:`/`edge:`-prefixed).
pub(crate) fn schema_apply_serial_queue_key() -> crate::db::write_queue::TableQueueKey {
    ("__schema_apply__".to_string(), None)
}

/// Open-time recovery sweep — the entry point invoked from
/// `Omnigraph::open` (gated on `OpenMode::ReadWrite`).
///
/// Enumerates every sidecar in `__recovery/`, classifies each table per
/// the sidecar's intent, and applies the all-or-nothing decision:
/// roll-forward (every table eligible), roll-back (mixed or unexpected
/// state), or abort (invariant violation).
///
/// Idempotency: a crash mid-sweep leaves the sidecar (deletion is the
/// final step). Re-opening re-classifies; repeated rollbacks of the
/// same table append extra Lance restore commits which `omnigraph
/// cleanup` reclaims.
///
/// Concurrency: the open-time sweep runs synchronously in `Omnigraph::open`
/// before the engine handle is published to any caller, so no request
/// handler can race it and it does NOT acquire write queues. In-process
/// callers (refresh, write entry points) must use
/// [`heal_pending_sidecars_roll_forward`] instead, which serializes
/// against live writers via per-(table_key, branch) queue acquisition.
pub(crate) async fn recover_manifest_drift(
    root_uri: &str,
    storage: std::sync::Arc<dyn StorageAdapter>,
    coordinator: &mut GraphCoordinator,
    mode: RecoveryMode,
    schema_state_recovery: SchemaStateRecovery,
) -> Result<()> {
    let sidecars = list_sidecars(root_uri, storage.as_ref()).await?;
    if sidecars.is_empty() {
        return Ok(());
    }

    // For each sidecar, classify against a FRESH snapshot AT THE
    // SIDECAR'S BRANCH. Two reasons:
    // 1. Per-sidecar refresh: sidecar N's roll-forward writes manifest
    //    changes that sidecar N+1 must observe, otherwise N+1 classifies
    //    its tables against stale pins.
    // 2. Per-branch snapshot: a sidecar from a feature-branch writer
    //    pins entries on that feature branch. Classifying against the
    //    main coordinator's snapshot would compare to main's pins (and
    //    main's Lance HEAD if pin.table_branch isn't honored), silently
    //    no-op'ing or rolling back the wrong table version. Open a
    //    separate per-branch coordinator and use ITS snapshot.
    for sidecar in sidecars {
        let branch_snapshot = match sidecar.branch.as_deref() {
            Some(b) => {
                // Orphan check against the manifest's branch list (the
                // authority) BEFORE opening — same classification as the
                // write-entry heal: a deferred sidecar whose branch was
                // deleted would otherwise fail every ReadWrite open.
                coordinator.refresh().await?;
                if !coordinator
                    .all_branches()
                    .await?
                    .iter()
                    .any(|name| name == b)
                {
                    discard_orphaned_branch_sidecar(root_uri, storage.as_ref(), &sidecar).await?;
                    continue;
                }
                let mut branch_coord =
                    GraphCoordinator::open_branch(root_uri, b, std::sync::Arc::clone(&storage))
                        .await?;
                branch_coord.refresh().await?;
                branch_coord.snapshot()
            }
            None => {
                coordinator.refresh().await?;
                coordinator.snapshot()
            }
        };
        process_sidecar(
            root_uri,
            &storage,
            &branch_snapshot,
            &sidecar,
            mode,
            schema_state_recovery,
        )
        .await?;
    }
    // Final refresh so the caller sees the post-sweep state.
    coordinator.refresh().await?;
    Ok(())
}

async fn process_sidecar(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
    mode: RecoveryMode,
    schema_state_recovery: SchemaStateRecovery,
) -> Result<bool> {
    // Returns whether durable state changed (roll-forward, roll-back, or
    // stale-sidecar audit recovery). `false` = the sidecar was deferred
    // untouched -- callers must not treat that as a completed heal (no
    // schema reload / cache invalidation is warranted).
    let mut states = Vec::with_capacity(sidecar.tables.len());
    for pin in &sidecar.tables {
        let lance_head = open_lance_head(&pin.table_path, pin.table_branch.as_deref()).await?;
        let manifest_pinned = snapshot
            .entry(&pin.table_key)
            .map(|e| e.table_version)
            .unwrap_or(0);
        states.push(ClassifiedTable {
            classification: classify_table(
                pin,
                lance_head,
                manifest_pinned,
                sidecar.writer_kind,
                sidecar.schema_version,
            ),
            manifest_pinned,
            lance_head,
        });
    }
    let classifications = states
        .iter()
        .map(|state| state.classification)
        .collect::<Vec<_>>();

    match decide(&classifications) {
        SidecarDecision::Abort => match mode {
            RecoveryMode::Full => {
                // Surface loudly without deleting the sidecar — operator
                // must investigate. This includes any InvariantViolation
                // classification (Lance HEAD < manifest pinned: should
                // be impossible).
                Err(OmniError::manifest_internal(format!(
                    "recovery sidecar '{}' has invariant violation; refusing to act \
                     — operator review required (sidecar at '{}', classifications: {:?})",
                    sidecar.operation_id,
                    sidecar_uri(root_uri, &sidecar.operation_id),
                    classifications,
                )))
            }
            RecoveryMode::RollForwardOnly => {
                // In-process refresh-time recovery: leave the sidecar
                // and defer the loud abort to the next ReadWrite open.
                // Operator-actionable error surfacing belongs at open,
                // not silently inside refresh.
                warn!(
                    operation_id = sidecar.operation_id.as_str(),
                    writer_kind = ?sidecar.writer_kind,
                    "recovery: deferring sidecar with invariant violation to next ReadWrite open"
                );
                Ok(false)
            }
        },
        SidecarDecision::RollBack => {
            // Distinguish "stale sidecar from a previous successful
            // roll-forward whose audit/delete failed" from a legitimate
            // rollback. If every table is at NoMovement AND any pin's
            // manifest_pinned has advanced past expected_version, the
            // manifest already reflects the writer's intent — a previous
            // recovery's `roll_forward_all` succeeded but `record_audit`
            // or `delete_sidecar` failed, leaving the sidecar to be
            // re-discovered. Recording this as RolledBack with empty
            // outcomes (the naive RollBack path's behavior under all-
            // NoMovement) misleads operators reading
            // `_graph_commit_recoveries.lance` — the actual outcome was
            // a successful roll-forward.
            let all_no_movement = states
                .iter()
                .all(|s| matches!(s.classification, TableClassification::NoMovement));
            let any_pin_advanced = sidecar
                .tables
                .iter()
                .zip(states.iter())
                .any(|(pin, state)| state.manifest_pinned > pin.expected_version);
            if all_no_movement && any_pin_advanced {
                if matches!(mode, RecoveryMode::RollForwardOnly) {
                    // Refresh-time audit-recovery is safe: no
                    // Dataset::restore involved; just an audit-row write
                    // and sidecar delete.
                    warn!(
                        operation_id = sidecar.operation_id.as_str(),
                        writer_kind = ?sidecar.writer_kind,
                        "recovery: cleaning up stale sidecar from a prior successful \
                         roll-forward (audit-recovery, in-process refresh)"
                    );
                } else {
                    warn!(
                        operation_id = sidecar.operation_id.as_str(),
                        writer_kind = ?sidecar.writer_kind,
                        "recovery: cleaning up stale sidecar from a prior successful \
                         roll-forward (manifest already advanced; recording RolledForward audit)"
                    );
                }
                return record_audit_recovery_rollforward(
                    root_uri, storage.as_ref(), snapshot, sidecar, &states,
                )
                .await
                .map(|()| true);
            }
            if matches!(mode, RecoveryMode::RollForwardOnly) {
                // In-process recovery cannot run Dataset::restore safely
                // (would orphan a concurrent writer's commit). Leave the
                // sidecar in place; the next ReadWrite open will handle
                // it via the full sweep.
                warn!(
                    operation_id = sidecar.operation_id.as_str(),
                    writer_kind = ?sidecar.writer_kind,
                    "recovery: deferring rollback-eligible sidecar to next ReadWrite open"
                );
                return Ok(false);
            }
            warn!(
                operation_id = sidecar.operation_id.as_str(),
                writer_kind = ?sidecar.writer_kind,
                "recovery: rolling back sidecar (mixed or unexpected state)"
            );
            roll_back_sidecar(root_uri, storage.as_ref(), sidecar, &states)
                .await
                .map(|()| true)
        }
        SidecarDecision::RollForward => {
            if matches!(sidecar.writer_kind, SidecarKind::SchemaApply)
                && !schema_state_recovery.completed_schema_apply_sidecar_rename()
            {
                return match mode {
                    RecoveryMode::Full => {
                        warn!(
                            operation_id = sidecar.operation_id.as_str(),
                            "recovery: rolling back SchemaApply sidecar because schema staging \
                             files were not promoted in this recovery pass"
                        );
                        roll_back_sidecar(root_uri, storage.as_ref(), sidecar, &states)
                            .await
                            .map(|()| true)
                    }
                    RecoveryMode::RollForwardOnly => {
                        warn!(
                            operation_id = sidecar.operation_id.as_str(),
                            "recovery: deferring SchemaApply sidecar because schema staging files \
                             were not promoted in this recovery pass"
                        );
                        Ok(false)
                    }
                };
            }
            warn!(
                operation_id = sidecar.operation_id.as_str(),
                writer_kind = ?sidecar.writer_kind,
                "recovery: rolling forward sidecar (Phase B completed; \
                 Phase C did not land)"
            );
            // TOCTOU window: between `classify_table` (which read the manifest
            // pin) and the publish CAS below, a concurrent live writer can
            // advance the manifest past our expected version. The failpoint
            // lets a test force that interleave deterministically.
            crate::failpoints::maybe_fail(
                crate::failpoints::names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH,
            )?;
            // RFC-013 Phase 7: `roll_forward_all` folds the recovery commit into the
            // manifest publish CAS, so it also returns the minted `graph_commit_id`
            // for the audit row below.
            let (new_manifest_version, published_versions, graph_commit_id) =
                match roll_forward_all(root_uri, sidecar, &states, snapshot).await {
                    Ok(published) => published,
                    // Convergence-idempotent (invariants 7 & 15): a roll-forward's
                    // postcondition is "the manifest reflects the sidecar's committed
                    // Lance state", NOT "this sweep personally won the CAS". A
                    // concurrent writer that advanced the manifest to/past that goal
                    // during the classify→publish window is convergence, not a logical
                    // conflict — so re-read and either record the already-achieved
                    // roll-forward or defer to the next pass; never fail the open.
                    // Any other error still propagates.
                    Err(err) if is_expected_version_mismatch(&err) => {
                        return converge_or_defer_roll_forward(
                            root_uri, storage, sidecar, &states, err,
                        )
                        .await;
                    }
                    Err(err) => return Err(err),
                };
            let _ = new_manifest_version;
            // `to_version` records the ACTUAL Lance HEAD published for
            // each table (not pin.post_commit_pin, which is a lower bound
            // for loose-match writers like SchemaApply / EnsureIndices /
            // BranchMerge that run multiple commit_staged calls per table).
            // SchemaApply additional_registrations are also included so
            // operators reading the audit row see the full publish set,
            // not just the pinned subset.
            let mut outcomes: Vec<TableOutcome> = sidecar
                .tables
                .iter()
                .map(|pin| TableOutcome {
                    table_key: pin.table_key.clone(),
                    from_version: pin.expected_version,
                    to_version: published_versions
                        .get(&pin.table_key)
                        .copied()
                        .unwrap_or(pin.post_commit_pin),
                })
                .collect();
            for reg in &sidecar.additional_registrations {
                outcomes.push(TableOutcome {
                    table_key: reg.table_key.clone(),
                    from_version: 0,
                    to_version: published_versions.get(&reg.table_key).copied().unwrap_or(0),
                });
            }
            record_audit(
                root_uri,
                sidecar,
                graph_commit_id,
                RecoveryKind::RolledForward,
                outcomes,
            )
            .await?;
            delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &sidecar.operation_id)
                .await?;
            Ok(true)
        }
    }
}

/// True if `err` is the publisher's per-table CAS precondition failure
/// (`ExpectedVersionMismatch`) — the signal that a concurrent writer advanced
/// the manifest past what this caller expected.
fn is_expected_version_mismatch(err: &OmniError) -> bool {
    matches!(
        err,
        OmniError::Manifest(m)
            if matches!(
                m.details,
                Some(crate::error::ManifestConflictDetails::ExpectedVersionMismatch { .. })
            )
    )
}

/// Whether the live manifest already reflects everything this sidecar intended
/// to publish.
///
/// SOUNDNESS: the per-table test is `current_version >= observed lance_head`, a
/// *proxy* for "the sidecar's committed Lance commit is an ancestor of the
/// published HEAD" (so a higher version is a descendant that contains it). The
/// proxy is sound only because of the heal-first invariant: every writer that
/// can advance a table's manifest version first heals pending sidecars
/// (`heal_pending_recovery_sidecars` runs at the head of `load`/`mutate`/
/// schema-apply/branch-merge) or refuses on an unrecovered graph (`optimize`).
/// So the only path past `expected_version` is one that first publishes THIS
/// sidecar's commit at `lance_head` — version ordering then implies lineage
/// containment. A future writer that advances a pinned table WITHOUT healing
/// first (e.g. a non-heal-first `Overwrite` that replaces rows) would void this
/// proxy and must be re-validated by row-id lineage, not version ordering.
/// Added tables must be registered; tombstoned tables must be gone.
fn sidecar_intent_satisfied(
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
) -> bool {
    for (pin, state) in sidecar.tables.iter().zip(states.iter()) {
        let current = snapshot
            .entry(&pin.table_key)
            .map(|e| e.table_version)
            .unwrap_or(0);
        if current < state.lance_head {
            return false;
        }
    }
    for reg in &sidecar.additional_registrations {
        if snapshot.entry(&reg.table_key).is_none() {
            return false;
        }
    }
    for tomb in &sidecar.tombstones {
        if snapshot.entry(&tomb.table_key).is_some() {
            return false;
        }
    }
    true
}

/// Re-read the live manifest snapshot for the sidecar's branch.
async fn fresh_snapshot_for_sidecar(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    sidecar: &RecoverySidecar,
) -> Result<Snapshot> {
    let mut coordinator = match sidecar.branch.as_deref() {
        Some(branch) if branch != "main" => {
            GraphCoordinator::open_branch(root_uri, branch, std::sync::Arc::clone(storage)).await?
        }
        _ => GraphCoordinator::open(root_uri, std::sync::Arc::clone(storage)).await?,
    };
    coordinator.refresh().await?;
    Ok(coordinator.snapshot())
}

/// Convergence-idempotent handling of a roll-forward publish CAS that lost to a
/// concurrent writer (`ExpectedVersionMismatch`). A roll-forward's postcondition
/// is "the manifest reflects the sidecar's committed Lance state", not "this
/// sweep won the CAS" (invariants 7 & 15). Re-read the live manifest:
///
/// - if it already reached the sidecar's goal, the work is done (just not by us)
///   — record the `RolledForward` audit and delete the sidecar idempotently;
/// - otherwise the manifest is progressing but not yet at the goal — leave the
///   sidecar for the next open / the live writer's own Phase D.
///
/// Either way the open does NOT fail. A genuine logical conflict (a table below
/// `expected_version`, i.e. data lost) is not satisfiable here and re-surfaces
/// loudly via the classifier's `InvariantViolation` on the next pass.
/// See iss-schema-apply-reopen-recovery-race.
async fn converge_or_defer_roll_forward(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
    conflict: OmniError,
) -> Result<bool> {
    let fresh = fresh_snapshot_for_sidecar(root_uri, storage, sidecar).await?;
    if !sidecar_intent_satisfied(&fresh, sidecar, states) {
        warn!(
            operation_id = sidecar.operation_id.as_str(),
            writer_kind = ?sidecar.writer_kind,
            "recovery: roll-forward publish lost a CAS and the manifest has not \
             yet reached the sidecar's goal; deferring to the next pass \
             (conflict: {conflict})"
        );
        return Ok(false);
    }
    // The manifest already reached the sidecar's goal — some other actor
    // advanced it. Under the heal-first invariant, whoever advanced past
    // `expected_version` first healed THIS sidecar (recorded its RolledForward
    // audit and deleted it). So the audit row already exists; recording another
    // here would put two RolledForward rows in `_graph_commit_recoveries` for
    // one recovery event (visible in `commit list --filter actor=…recovery`).
    // Only finish the bookkeeping if the sidecar is still on disk (the winner
    // crashed between audit and delete); if it is already gone, the winner
    // completed it — return success WITHOUT a duplicate audit, keeping the
    // audit append-idempotent per operation_id across concurrent sweeps.
    let sidecar_path = sidecar_uri(root_uri, &sidecar.operation_id);
    if !storage.exists(&sidecar_path).await? {
        warn!(
            operation_id = sidecar.operation_id.as_str(),
            writer_kind = ?sidecar.writer_kind,
            "recovery: roll-forward publish lost a CAS; the winner already \
             converged and cleaned up this sidecar — nothing to do"
        );
        return Ok(true);
    }
    warn!(
        operation_id = sidecar.operation_id.as_str(),
        writer_kind = ?sidecar.writer_kind,
        "recovery: roll-forward publish lost a CAS to a concurrent writer that \
         already reached the goal; converging (RolledForward audit + delete)"
    );
    let mut outcomes: Vec<TableOutcome> = sidecar
        .tables
        .iter()
        .map(|pin| TableOutcome {
            table_key: pin.table_key.clone(),
            from_version: pin.expected_version,
            to_version: fresh
                .entry(&pin.table_key)
                .map(|e| e.table_version)
                .unwrap_or(pin.post_commit_pin),
        })
        .collect();
    // Mirror the normal roll-forward audit shape: SchemaApply sidecars also
    // register added tables, so the audit must list them too (else a converge
    // audit row is incomplete vs the `roll_forward_all` path for the same
    // recovery kind).
    for reg in &sidecar.additional_registrations {
        outcomes.push(TableOutcome {
            table_key: reg.table_key.clone(),
            from_version: 0,
            to_version: fresh
                .entry(&reg.table_key)
                .map(|e| e.table_version)
                .unwrap_or(0),
        });
    }
    // RFC-013 Phase 7: the winning writer folded its recovery commit into the
    // manifest CAS, so the converge audit references THAT commit — the branch's
    // current `graph_head` — not a freshly minted one. (We only reach here with
    // the sidecar still on disk: the winner advanced the manifest but crashed
    // before its own audit+delete, so we finish its bookkeeping.)
    let converged_commit_id = match sidecar.branch.as_deref() {
        Some(branch) => {
            crate::db::commit_graph::CommitGraph::open_at_branch(root_uri, branch).await?
        }
        None => crate::db::commit_graph::CommitGraph::open(root_uri).await?,
    }
    .head_commit_id()
    .await?
    .unwrap_or_default();
    record_audit(
        root_uri,
        sidecar,
        converged_commit_id,
        RecoveryKind::RolledForward,
        outcomes,
    )
    .await?;
    delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &sidecar.operation_id).await?;
    Ok(true)
}

#[derive(Debug, Clone, Copy)]
struct ClassifiedTable {
    classification: TableClassification,
    manifest_pinned: u64,
    /// Lance HEAD observed at classification time. Captured so the
    /// rollback audit's `from_version` can record where Lance HEAD was
    /// before `Dataset::restore` ran (operators reading
    /// `_graph_commit_recoveries.lance` see actual drift, not
    /// `from_version == to_version == manifest_pinned`).
    lance_head: u64,
}

async fn roll_back_sidecar(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
) -> Result<()> {
    // Restore every drifted table (RolledPastExpected / UnexpectedAtP1 /
    // UnexpectedMultistep) to its manifest-pinned content, then PUBLISH so
    // `manifest == Lance HEAD` for each — symmetric with roll-forward. The
    // restore commit's content equals the manifest-pinned version, so re-pinning
    // the manifest to the new (restored) HEAD is content-correct and closes the
    // orphaned-drift class (`HEAD > manifest` with no covering sidecar). This is
    // what makes a failed-then-retried schema_apply converge: after one
    // roll-back `manifest == HEAD`, so the retry's precondition passes instead of
    // failing one version higher each iteration.
    //
    // NoMovement tables are already at the pin — excluded from both the restore
    // and the publish. The audit `to_version` stays the *logical* rolled-back-to
    // version (`manifest_pinned`), while the manifest is published at
    // `manifest_pinned + 1` (the restore commit, same content) — keep that
    // asymmetry so the audit records the drift (`from_version > to_version`).
    let mut outcomes = Vec::with_capacity(sidecar.tables.len());
    let mut updates: Vec<ManifestChange> = Vec::with_capacity(sidecar.tables.len());
    let mut expected: HashMap<String, u64> = HashMap::with_capacity(sidecar.tables.len());
    for (pin, state) in sidecar.tables.iter().zip(states.iter()) {
        if matches!(
            state.classification,
            TableClassification::RolledPastExpected
                | TableClassification::UnexpectedAtP1
                | TableClassification::UnexpectedMultistep
                | TableClassification::IncompletePhaseB
        ) {
            restore_table_to_version(
                &pin.table_path,
                pin.table_branch.as_deref(),
                state.manifest_pinned,
            )
            .await?;
            // Publish the post-restore HEAD (the restore commit we just made),
            // CAS against the current (unmoved) manifest pin — the same helper
            // roll-forward uses. `None` target: there is no prior observation to
            // pin to; the version to publish is the HEAD the restore produced.
            push_table_update(
                root_uri,
                &pin.table_key,
                &pin.table_path,
                pin.table_branch.as_deref(),
                state.manifest_pinned,
                None,
                &mut updates,
                &mut expected,
            )
            .await?;
            // `from_version` records the Lance HEAD observed BEFORE the restore
            // (the actual drift); `to_version` the logical pin we rolled back to.
            outcomes.push(TableOutcome {
                table_key: pin.table_key.clone(),
                from_version: state.lance_head,
                to_version: state.manifest_pinned,
            });
        }
    }
    // Publish the restored HEADs so manifest == HEAD AND record the recovery
    // commit in the same CAS (RFC-013 Phase 7). A degenerate all-NoMovement
    // roll-back restores no table — `updates` is empty — but the recovery commit
    // lineage still publishes (a lineage-only merge), so the rollback is recorded
    // in the commit history just like a roll-forward.
    let (_manifest_version, graph_commit_id) =
        publish_recovery_commit(root_uri, sidecar, RecoveryKind::RolledBack, &updates, &expected)
            .await?;
    record_audit(
        root_uri,
        sidecar,
        graph_commit_id,
        RecoveryKind::RolledBack,
        outcomes,
    )
    .await?;
    delete_sidecar_by_operation_id(root_uri, storage, &sidecar.operation_id).await?;
    Ok(())
}

/// Cleanup path for stale sidecars where a previous recovery's
/// roll-forward succeeded (manifest pin advanced past `expected_version`)
/// but `record_audit` or sidecar deletion failed, leaving the sidecar
/// to be re-discovered on a subsequent open. By the time we re-classify,
/// every table reads as `NoMovement` (lance_head == manifest_pinned),
/// which the naive `RollBack` arm would record as RolledBack-with-empty-
/// outcomes — misleading for operators because the actual outcome was
/// a successful roll-forward.
///
/// This helper records the correct shape: a `RolledForward` audit row
/// whose `from_version` is the original `expected_version` and whose
/// `to_version` is the current `manifest_pinned` (the actual published
/// version after the prior roll-forward). No Lance writes are needed —
/// the substrate is already in the post-roll-forward state.
async fn record_audit_recovery_rollforward(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
) -> Result<()> {
    let _ = snapshot;
    let outcomes: Vec<TableOutcome> = sidecar
        .tables
        .iter()
        .zip(states.iter())
        .map(|(pin, state)| TableOutcome {
            table_key: pin.table_key.clone(),
            from_version: pin.expected_version,
            to_version: state.manifest_pinned,
        })
        .collect();
    // The substrate is already in the post-roll-forward state (the prior pass's
    // table re-pin landed), so there are no table `updates` — but a recovery
    // commit is still recorded for this cleanup pass via a lineage-only publish
    // (RFC-013 Phase 7), which the audit row references.
    let (_manifest_version, graph_commit_id) = publish_recovery_commit(
        root_uri,
        sidecar,
        RecoveryKind::RolledForward,
        &[],
        &HashMap::new(),
    )
    .await?;
    record_audit(
        root_uri,
        sidecar,
        graph_commit_id,
        RecoveryKind::RolledForward,
        outcomes,
    )
    .await?;
    delete_sidecar_by_operation_id(root_uri, storage, &sidecar.operation_id).await?;
    Ok(())
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
/// Returns `(new_manifest_version, per_table_published_versions,
/// recovery_commit_id)`. The per-table map is what the audit row's `to_version`
/// should record — for loose-match writers the actual Lance HEAD can be higher
/// than the sidecar's `post_commit_pin` (which is a lower bound), so the pin is
/// the wrong source of truth for an operator-facing audit field. The recovery
/// commit id is the `graph_commit` folded into the publish CAS (RFC-013
/// Phase 7), which the audit row references.
async fn roll_forward_all(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
    snapshot: &Snapshot,
) -> Result<(u64, HashMap<String, u64>, String)> {
    let total_changes =
        sidecar.tables.len() + sidecar.additional_registrations.len() + sidecar.tombstones.len();
    let mut updates: Vec<ManifestChange> = Vec::with_capacity(total_changes);
    let mut expected: HashMap<String, u64> = HashMap::with_capacity(total_changes);
    let mut published_versions: HashMap<String, u64> =
        HashMap::with_capacity(sidecar.tables.len() + sidecar.additional_registrations.len());

    for (pin, state) in sidecar.tables.iter().zip(states.iter()) {
        // Publish the version classification OBSERVED (`state.lance_head`), not a
        // fresh HEAD re-read. For a `Confirmed` pin classify already validated
        // `lance_head == confirmed_version`, so this publishes the recorded WAL
        // intent by construction; for loose/strict pins it's the multi-commit
        // HEAD classify saw. Single observation, no classify→publish TOCTOU. CAS
        // against the pin's pre-write `expected_version`.
        let published = push_table_update(
            root_uri,
            &pin.table_key,
            &pin.table_path,
            pin.table_branch.as_deref(),
            pin.expected_version,
            Some(state.lance_head),
            &mut updates,
            &mut expected,
        )
        .await?;
        published_versions.insert(pin.table_key.clone(), published);
    }

    // SchemaApply-only: register added tables (and renamed targets) and
    // emit accompanying Update entries so recovery's manifest commit
    // matches what the writer would have published. Without this, added
    // tables exist as orphan datasets on disk but never receive a
    // manifest entry, leaving the live schema and manifest mismatched.
    //
    // Filtered against `snapshot`: when the manifest already has a live
    // entry for `reg.table_key`, a previous recovery (or the writer
    // itself, before crashing in Phase D) has already published the
    // registration — skip it to avoid the publisher's ExpectedVersionMismatch
    // (expected=0, actual=current_version) on the redundant Register.
    for reg in &sidecar.additional_registrations {
        if snapshot.entry(&reg.table_key).is_some() {
            // Already registered — record the current version in
            // published_versions so the audit row's `to_version` reflects
            // reality, but emit no manifest change.
            if let Some(entry) = snapshot.entry(&reg.table_key) {
                published_versions.insert(reg.table_key.clone(), entry.table_version);
            }
            continue;
        }
        let dataset_uri = format!("{}/{}", root_uri.trim_end_matches('/'), reg.table_path);
        let head_ds = Dataset::open(&dataset_uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let head_ds = match reg.table_branch.as_deref() {
            Some(b) if b != "main" => head_ds
                .checkout_branch(b)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?,
            _ => head_ds,
        };
        let head_version = head_ds.version().version;
        let row_count = head_ds
            .count_rows(None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))? as u64;
        let version_metadata = super::metadata::TableVersionMetadata::from_dataset(
            root_uri,
            &reg.table_path,
            &head_ds,
        )?;

        updates.push(ManifestChange::RegisterTable(TableRegistration {
            table_key: reg.table_key.clone(),
            table_path: reg.table_path.clone(),
        }));
        updates.push(ManifestChange::Update(SubTableUpdate {
            table_key: reg.table_key.clone(),
            table_version: head_version,
            table_branch: reg.table_branch.clone(),
            row_count,
            version_metadata,
        }));
        // No prior manifest entry expected for an added table.
        expected.insert(reg.table_key.clone(), 0);
        published_versions.insert(reg.table_key.clone(), head_version);
    }

    // SchemaApply-only: tombstone removed types (and renamed sources).
    //
    // Filtered against `snapshot`: when the manifest no longer has an
    // entry for `tomb.table_key`, the tombstone has already landed in
    // a prior recovery / the writer's Phase C — skip emit so the
    // publisher doesn't error on a redundant tombstone.
    for tomb in &sidecar.tombstones {
        if snapshot.entry(&tomb.table_key).is_none() {
            continue;
        }
        updates.push(ManifestChange::Tombstone(TableTombstone {
            table_key: tomb.table_key.clone(),
            tombstone_version: tomb.tombstone_version,
        }));
        // Tombstone CAS pre-check expects the table to be at
        // `tombstone_version - 1` (the pre-tombstone version, since
        // schema_apply sets `tombstone_version = source.table_version + 1`).
        expected.insert(
            tomb.table_key.clone(),
            tomb.tombstone_version.saturating_sub(1),
        );
    }

    let (new_manifest_version, graph_commit_id) =
        publish_recovery_commit(root_uri, sidecar, RecoveryKind::RolledForward, &updates, &expected)
            .await?;
    Ok((new_manifest_version, published_versions, graph_commit_id))
}

/// Open `table_path` at its branch HEAD, read the current Lance HEAD version,
/// row count, and version metadata, and push a `ManifestChange::Update` (plus
/// its CAS `expected` entry) that re-pins the manifest to that HEAD. Returns the
/// published HEAD version.
///
/// Shared by `roll_forward_all` (where `expected_version` is the sidecar's
/// pre-write pin) and `roll_back_sidecar` (where it is the manifest-pinned
/// version the table was just restored to). The HEAD is read AFTER any restore
/// in the same single-threaded sweep, so no concurrent writer can have advanced
/// it.
/// Stage a manifest `Update` for one table.
///
/// `target_version` selects WHICH Lance version's state to publish:
/// - `Some(v)` — pin the dataset at version `v` and publish it. Roll-FORWARD
///   passes the version classification observed (and, for a `Confirmed` pin,
///   validated equals `confirmed_version`), so recovery publishes the version it
///   *decided* on rather than re-reading a HEAD a concurrent writer may have
///   advanced since classification — one observation, used for both the decision
///   and the publish (invariant 15).
/// - `None` — publish the dataset's current HEAD. Roll-BACK uses this: it just
///   created the restore commit, so HEAD *is* the version to publish.
async fn push_table_update(
    root_uri: &str,
    table_key: &str,
    table_path: &str,
    branch: Option<&str>,
    expected_version: u64,
    target_version: Option<u64>,
    updates: &mut Vec<ManifestChange>,
    expected: &mut HashMap<String, u64>,
) -> Result<u64> {
    let ds = Dataset::open(table_path)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let ds = match branch {
        Some(b) if b != "main" => ds
            .checkout_branch(b)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?,
        _ => ds,
    };
    let ds = match target_version {
        Some(v) => ds
            .checkout_version(v)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?,
        None => ds,
    };
    let published_version = ds.version().version;
    let row_count = ds
        .count_rows(None)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))? as u64;
    let table_relative_path = super::table_path_for_table_key(table_key)?;
    let version_metadata =
        super::metadata::TableVersionMetadata::from_dataset(root_uri, &table_relative_path, &ds)?;
    updates.push(ManifestChange::Update(SubTableUpdate {
        table_key: table_key.to_string(),
        table_version: published_version,
        table_branch: branch.map(str::to_string),
        row_count,
        version_metadata,
    }));
    expected.insert(table_key.to_string(), expected_version);
    Ok(published_version)
}

/// Append the audit row describing this recovery action (RFC-013 Phase 7).
///
/// The recovery COMMIT (`graph_commit` + `graph_head`) was already recorded
/// durably in `__manifest` by `publish_recovery_commit` (folded into the same
/// CAS as the table re-pin), so this only writes the `_graph_commit_recoveries`
/// row, referencing that commit by `graph_commit_id`. A crash between the
/// recovery publish and this audit append leaves a recovery commit with no audit
/// row — the same not-atomic-pair-write shape as before; the sweep tolerates it
/// (on re-entry the classifier surfaces `NoMovement`, the action is a no-op, and
/// the audit append is retried, minting a fresh recovery commit).
async fn record_audit(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    graph_commit_id: String,
    kind: RecoveryKind,
    outcomes: Vec<TableOutcome>,
) -> Result<()> {
    // Failpoint: models an audit write failure after the roll-forward /
    // roll-back publish (with its folded-in recovery commit) already landed —
    // the sweep aborts, the sidecar stays, and re-entry records the audit row.
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_RECORD_AUDIT)?;
    let mut audit = RecoveryAudit::open(root_uri).await?;
    audit
        .append(RecoveryAuditRecord {
            graph_commit_id,
            recovery_kind: kind,
            recovery_for_actor: sidecar.actor_id.clone(),
            operation_id: sidecar.operation_id.clone(),
            sidecar_writer_kind: format!("{:?}", sidecar.writer_kind),
            per_table_outcomes: outcomes,
            created_at: crate::db::now_micros()?,
        })
        .await?;
    Ok(())
}

/// Returns `true` if any `SchemaApply` sidecar is present in
/// `__recovery/`. Schema-state recovery (`recover_schema_state_files`)
/// uses this to skip its normal pre-vs-post-commit disambiguation —
/// when a SchemaApply sidecar is present, we know the writer reached
/// Phase B (Lance HEADs advanced) but didn't complete Phase C (manifest
/// publish + staging→final renames). The right action is to complete
/// the rename so the recovery sweep's roll-forward step sees the new
/// catalog. Without this, the disambiguation logic deletes the staging
/// files (since manifest still pins the old table set) and leaves the
/// graph with new-schema data on disk but the old `_schema.pg` live —
/// real corruption.
pub(crate) async fn has_schema_apply_sidecar(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<bool> {
    let sidecars = list_sidecars(root_uri, storage).await?;
    Ok(sidecars
        .iter()
        .any(|s| matches!(s.writer_kind, SidecarKind::SchemaApply)))
}

/// Open the Lance dataset at `table_path` checked out at the given
/// branch ref (or default if `branch` is None or "main") and return its
/// HEAD version. Recovery uses this so feature-branch sidecars classify
/// against the feature-branch's Lance HEAD, not main's.
async fn open_lance_head(table_path: &str, branch: Option<&str>) -> Result<u64> {
    let ds = Dataset::open(table_path)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let ds = match branch {
        Some(b) if b != "main" => ds
            .checkout_branch(b)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?,
        _ => ds,
    };
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
        merge_source_commit_id: None,
        additional_registrations: Vec::new(),
        tombstones: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ObjectStorageAdapter;
    use crate::table_store::TableStore;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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
            confirmed_version: None,
            table_branch: None,
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
    fn parse_sidecar_refuses_future_but_accepts_older_schema_version() {
        let body = |version: u32| {
            format!(
                r#"{{
                "schema_version": {version},
                "operation_id": "01H000000000000000000000XX",
                "started_at": "0",
                "branch": null,
                "actor_id": null,
                "writer_kind": "BranchMerge",
                "tables": []
            }}"#
            )
        };
        // A version NEWER than this binary's max → refuse (can't guess the future).
        let err = parse_sidecar("file:///tmp/__recovery/x.json", &body(99)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("schema_version=99") && msg.contains("newer than the maximum"),
            "expected a future-version refusal, got: {msg}",
        );
        // An OLDER version (pre-confirmation v1) → accept and interpret with its
        // original semantics; never refuse a version we were built to understand.
        let parsed = parse_sidecar("file:///tmp/__recovery/x.json", &body(1))
            .expect("a v1 (older) sidecar must parse, not be refused");
        assert_eq!(parsed.schema_version, 1);
    }

    #[test]
    fn classify_no_movement_when_head_equals_pinned() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 5, 5, SidecarKind::Mutation, SIDECAR_SCHEMA_VERSION),
            TableClassification::NoMovement,
        );
    }

    #[test]
    fn classify_rolled_past_expected_when_sidecar_matches_strict() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 6, 5, SidecarKind::Mutation, SIDECAR_SCHEMA_VERSION),
            TableClassification::RolledPastExpected,
        );
    }

    #[test]
    fn classify_unexpected_at_p1_when_sidecar_does_not_match_strict() {
        // Same +1 drift but post_commit_pin says it should be 7, not 6.
        let pin = make_pin("node:Person", "irrelevant", 5, 7);
        assert_eq!(
            classify_table(&pin, 6, 5, SidecarKind::Mutation, SIDECAR_SCHEMA_VERSION),
            TableClassification::UnexpectedAtP1,
        );
    }

    #[test]
    fn classify_unexpected_multistep_when_head_jumped_more_than_one_strict() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 8, 5, SidecarKind::Mutation, SIDECAR_SCHEMA_VERSION),
            TableClassification::UnexpectedMultistep,
        );
    }

    #[test]
    fn classify_invariant_violation_when_head_below_pinned() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 3, 5, SidecarKind::Mutation, SIDECAR_SCHEMA_VERSION),
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
            classify_table(&pin, 8, 5, SidecarKind::SchemaApply, SIDECAR_SCHEMA_VERSION),
            TableClassification::RolledPastExpected,
        );
    }

    #[test]
    fn classify_loose_match_accepts_multi_commit_drift_for_ensure_indices() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 9, 5, SidecarKind::EnsureIndices, SIDECAR_SCHEMA_VERSION),
            TableClassification::RolledPastExpected,
        );
    }

    #[test]
    fn classify_loose_match_no_movement_unchanged() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 5, 5, SidecarKind::SchemaApply, SIDECAR_SCHEMA_VERSION),
            TableClassification::NoMovement,
        );
    }

    #[test]
    fn classify_loose_match_invariant_violation_unchanged() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 3, 5, SidecarKind::SchemaApply, SIDECAR_SCHEMA_VERSION),
            TableClassification::InvariantViolation { observed: 3 },
        );
    }

    /// BranchMerge advances each table by several commits per table
    /// (adopt: append + upsert + delete; three-way: merge_insert + delete +
    /// index), so a bare "HEAD moved" is ambiguous between a complete and a
    /// partial publish. At a confirmation-aware version the two-phase
    /// confirmation resolves it: roll forward ONLY to the recorded
    /// `confirmed_version`; an unconfirmed moved HEAD is a partial publish
    /// (`IncompletePhaseB` ⇒ roll back), and a confirmed version that doesn't
    /// match the observed HEAD is a foreign advance (`UnexpectedMultistep` ⇒
    /// roll back). A *pre-confirmation* (v1) sidecar carries no confirmation and
    /// must keep the original loose roll-forward — reading it as strict would
    /// roll a completed pre-upgrade merge back (silent discard).
    #[test]
    fn classify_branch_merge_requires_phase_b_confirmation() {
        // Unconfirmed multi-commit drift at a confirmation-aware version →
        // partial Phase B → roll back.
        let unconfirmed = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&unconfirmed, 8, 5, SidecarKind::BranchMerge, SIDECAR_SCHEMA_VERSION),
            TableClassification::IncompletePhaseB,
        );
        // Backward-compat: the SAME unconfirmed pin in a PRE-confirmation (v1)
        // sidecar → loose roll-forward (the regression fix — a completed
        // pre-upgrade merge must not be discarded).
        assert_eq!(
            classify_table(
                &unconfirmed,
                8,
                5,
                SidecarKind::BranchMerge,
                CONFIRMATION_SCHEMA_VERSION - 1,
            ),
            TableClassification::RolledPastExpected,
        );
        // Confirmed to the observed HEAD → complete Phase B → roll forward.
        let confirmed = SidecarTablePin {
            confirmed_version: Some(8),
            ..make_pin("node:Person", "irrelevant", 5, 6)
        };
        assert_eq!(
            classify_table(&confirmed, 8, 5, SidecarKind::BranchMerge, SIDECAR_SCHEMA_VERSION),
            TableClassification::RolledPastExpected,
        );
        // Confirmed, but HEAD drifted past it (foreign writer) → roll back.
        assert_eq!(
            classify_table(&confirmed, 9, 5, SidecarKind::BranchMerge, SIDECAR_SCHEMA_VERSION),
            TableClassification::UnexpectedMultistep,
        );
    }

    #[test]
    fn classify_loose_match_branch_merge_no_movement_unchanged() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 5, 5, SidecarKind::BranchMerge, SIDECAR_SCHEMA_VERSION),
            TableClassification::NoMovement,
        );
    }

    #[test]
    fn classify_loose_match_branch_merge_invariant_violation_unchanged() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(&pin, 3, 5, SidecarKind::BranchMerge, SIDECAR_SCHEMA_VERSION),
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

        restore_table_to_version(&uri, None, 1).await.unwrap();

        let post = Dataset::open(&uri).await.unwrap();
        assert_eq!(post.version().version, head_before + 1);
        // Content matches v1 (just alice).
        let scanner = post.scan();
        let batches: Vec<RecordBatch> =
            futures::TryStreamExt::try_collect(scanner.try_into_stream().await.unwrap())
                .await
                .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn restore_table_to_version_always_appends_a_commit() {
        // Restore is unconditional — equal fragment IDs do NOT imply
        // equal content (Lance index commits and deletion-vector
        // updates change the manifest without touching fragment IDs).
        // Repeated restore calls each produce a new HEAD+1 commit.
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
        restore_table_to_version(&uri, None, 1).await.unwrap();
        let mid = Dataset::open(&uri).await.unwrap().version().version;
        assert_eq!(mid, 3);

        // Second restore to v1: still appends a commit (HEAD = 4) because
        // restore is unconditional. The pile-up is bounded and reclaimed
        // by `omnigraph cleanup`.
        restore_table_to_version(&uri, None, 1).await.unwrap();
        let post = Dataset::open(&uri).await.unwrap().version().version;
        assert_eq!(
            post,
            mid + 1,
            "restore must always append a commit (no fragment-set short-circuit)"
        );
    }

    #[tokio::test]
    async fn list_sidecars_returns_empty_when_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ObjectStorageAdapter::local();
        let result = list_sidecars(dir.path().to_str().unwrap(), &storage)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn write_then_list_then_delete_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        // No pre-created __recovery/ subdir: the storage backend creates
        // missing parents on put, which is what the first sidecar write
        // of a fresh graph relies on.
        let storage = ObjectStorageAdapter::local();
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
    async fn confirm_sidecar_phase_b_errors_when_pin_missing_from_updates() {
        // A pinned table with no achieved version in the publish `updates` must
        // be a loud producer error, NOT a silent skip that leaves the pin
        // unconfirmed (which recovery would read as a partial Phase B and roll
        // the whole complete merge back). Guards the implicit `pins ⊆ updates`
        // invariant against a future divergence between the two filters.
        let dir = tempfile::tempdir().unwrap();
        let storage = ObjectStorageAdapter::local();
        let mut sidecar = new_sidecar(
            SidecarKind::BranchMerge,
            Some("main".to_string()),
            None,
            vec![make_pin("node:Person", "file:///tmp/x.lance", 5, 6)],
        );
        // The confirmed-versions map does NOT cover the pinned table.
        let confirmed: HashMap<String, u64> = HashMap::new();
        let err = confirm_sidecar_phase_b(
            dir.path().to_str().unwrap(),
            &storage,
            &mut sidecar,
            &confirmed,
        )
        .await
        .expect_err("a pinned table with no achieved version must be a loud error");
        assert!(
            err.to_string().contains("pins and publish updates diverged"),
            "expected a pin/updates divergence error, got: {err}",
        );
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
        let storage = ObjectStorageAdapter::local();
        let result = list_sidecars(dir.path().to_str().unwrap(), &storage)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    /// `list_dir` returns filesystem-order results, which would make
    /// sidecar processing nondeterministic. Sidecar filenames are ULIDs
    /// (lexicographically sortable === chronologically sortable), so
    /// sorting by URI gives deterministic, time-ordered processing —
    /// the older sidecar processed before the newer one.
    #[tokio::test]
    async fn list_sidecars_returns_deterministic_order() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(RECOVERY_DIR_NAME)).unwrap();
        let storage = ObjectStorageAdapter::local();
        let root = dir.path().to_str().unwrap();

        // Write sidecars in REVERSE chronological order (newest first).
        // The classifier shouldn't care, but the sweep needs deterministic
        // processing for reproducibility.
        let ids = [
            "01H000000000000000000000ZZ",
            "01H000000000000000000000MM",
            "01H000000000000000000000AA",
        ];
        for id in &ids {
            let sc = new_sidecar(
                SidecarKind::Mutation,
                None,
                None,
                vec![make_pin("node:Person", "/dev/null/x.lance", 1, 2)],
            );
            // Override operation_id to use our deterministic ID.
            let mut sc = sc;
            sc.operation_id = id.to_string();
            write_sidecar(root, &storage, &sc).await.unwrap();
        }

        let listed = list_sidecars(root, &storage).await.unwrap();
        let listed_ids: Vec<&str> = listed.iter().map(|s| s.operation_id.as_str()).collect();
        let mut sorted_ids = listed_ids.clone();
        sorted_ids.sort();
        assert_eq!(
            listed_ids, sorted_ids,
            "list_sidecars must return sidecars in deterministic (sorted) order; got {:?}",
            listed_ids,
        );
    }
}
