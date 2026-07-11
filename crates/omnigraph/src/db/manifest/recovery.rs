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
//!   The open-time sweep and live healer both join the root-scoped ordered
//!   gates. The in-process healer ([`heal_pending_sidecars_roll_forward`])
//!   additionally never restores (roll-forward only); foreign processes remain
//!   outside this serialization boundary.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::db::graph_coordinator::GraphCoordinator;
use crate::db::recovery_audit::{RecoveryAudit, RecoveryAuditRecord, RecoveryKind, TableOutcome};
use crate::db::schema_state::SchemaStateRecovery;
use crate::error::{OmniError, Result};
use crate::storage::StorageAdapter;
use crate::table_store::StagedTransactionIdentity;

use super::Snapshot;
use super::publisher::{
    GraphHeadExpectation, GraphNamespacePublisher, LineageIntent, ManifestBatchPublisher,
    PublishPrecondition,
};
use super::{
    ManifestChange, ManifestCoordinator, SubTableUpdate, TableRegistration, TableTombstone,
};

/// System actor identifier for recovery-owned lineage: legacy recovery,
/// exact-protocol rollback, and orphan discard. A v3/v4 roll-forward
/// deliberately publishes the original writer's fixed lineage and actor
/// instead. The recovery audit row is the authoritative attribution surface in
/// both cases; the original sidecar actor flows into its `recovery_for_actor`
/// field.
pub(crate) const RECOVERY_ACTOR: &str = "omnigraph:recovery";

/// Publish a recovery action's manifest `updates` AND its lineage in one CAS
/// (RFC-013 Phase 7). Legacy recovery and v3/v4 rollback publish a recovery
/// commit; v3/v4 roll-forward publishes the original writer's fixed lineage
/// intent. The lineage (`graph_commit` + `graph_head`) rides the same
/// merge-insert as the table-version re-pin — there is no separate
/// `_graph_commits.lance` write and no manifest→commit-graph gap.
/// `updates` is empty for the no-table-change recovery paths (all-NoMovement
/// roll-back, stale-sidecar cleanup, orphaned-branch discard); the lineage rows
/// still publish, so the recovery commit is always durable.
///
/// Legacy commits resolve their parent from the live branch head. An exact
/// roll-forward instead carries an exact graph-head precondition, so it cannot
/// be silently re-parented after authority changes. Returns the new manifest
/// version and the stable commit id the audit row references.
async fn publish_recovery_commit(
    root_uri: &str,
    sidecar: &RecoverySidecar,
    kind: RecoveryKind,
    updates: &[ManifestChange],
    expected: &HashMap<String, u64>,
) -> Result<(u64, String)> {
    let exact_lineage = sidecar
        .protocol_v3
        .as_ref()
        .map(|protocol| &protocol.lineage)
        .or_else(|| {
            sidecar
                .protocol_v4
                .as_ref()
                .map(|protocol| &protocol.lineage)
        });
    let exact_rollback_id = sidecar
        .protocol_v3
        .as_ref()
        .map(|protocol| protocol.rollback_graph_commit_id.as_str())
        .or_else(|| {
            sidecar
                .protocol_v4
                .as_ref()
                .map(|protocol| protocol.rollback_graph_commit_id.as_str())
        });
    let intent = match (exact_lineage, exact_rollback_id, kind) {
        (Some(lineage), _, RecoveryKind::RolledForward) => LineageIntent::from(lineage),
        (_, Some(rollback_graph_commit_id), RecoveryKind::RolledBack) => LineageIntent {
            graph_commit_id: rollback_graph_commit_id.to_string(),
            branch: sidecar.branch.clone(),
            actor_id: Some(RECOVERY_ACTOR.to_string()),
            merged_parent_commit_id: None,
            created_at: sidecar
                .started_at
                .parse::<i64>()
                .unwrap_or(crate::db::now_micros()?),
        },
        (Some(_), _, RecoveryKind::OrphanedBranchDiscarded)
        | (_, Some(_), RecoveryKind::OrphanedBranchDiscarded) => {
            return Err(OmniError::manifest_internal(
                "orphaned-branch discard cannot publish through an exact recovery protocol",
            ));
        }
        (None, None, _) => {
            let merged_parent_commit_id = match (sidecar.writer_kind, kind) {
                (SidecarKind::BranchMerge, RecoveryKind::RolledForward) => {
                    sidecar.merge_source_commit_id.clone()
                }
                _ => None,
            };
            LineageIntent {
                graph_commit_id: ulid::Ulid::new().to_string(),
                branch: sidecar.branch.clone(),
                actor_id: Some(RECOVERY_ACTOR.to_string()),
                merged_parent_commit_id,
                created_at: crate::db::now_micros()?,
            }
        }
        _ => {
            return Err(OmniError::manifest_internal(
                "exact recovery sidecar is missing either lineage or rollback identity",
            ));
        }
    };
    let publisher = GraphNamespacePublisher::new(root_uri, sidecar.branch.as_deref());
    let exact_authority = sidecar
        .protocol_v3
        .as_ref()
        .map(|protocol| &protocol.authority)
        .or_else(|| {
            sidecar
                .protocol_v4
                .as_ref()
                .map(|protocol| &protocol.authority)
        });
    let precondition = match (exact_authority, kind) {
        (Some(authority), RecoveryKind::RolledForward) => {
            PublishPrecondition::ExactGraphHead(GraphHeadExpectation::new(
                sidecar.branch.as_deref(),
                authority.branch_identifier.clone(),
                authority.graph_head.clone(),
            ))
        }
        _ => PublishPrecondition::Any,
    };
    let outcome = publisher
        .publish_with_precondition(updates, expected, Some(&intent), &precondition)
        .await?;
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
///
/// v2 → v3: RFC-022 exact mutation/load effects. An enrolled sidecar carries
/// the captured authority, stable lineage/rollback ids, planned Lance
/// transaction identities, a canonical manifest delta, and an explicit
/// Armed → EffectsConfirmed transition. Legacy writers deliberately continue
/// producing v2 until their adapter opts into [`new_occ_sidecar`].
///
/// v3 → v4: RFC-022 branch-merge authority and exact confirmed output. A v4
/// sidecar carries the captured target authority, fixed merge lineage and
/// rollback ids, the complete intended manifest delta (including pointer-only
/// updates), and an ordered exact transaction chain for each multi-commit HEAD
/// effect or an explicit ref-only first-touch fork. Mutation/Load remain fixed
/// at v3: raising the reader maximum must not silently enroll an existing writer
/// in new semantics.
pub(crate) const SIDECAR_SCHEMA_VERSION: u32 = 4;

/// Schema version emitted by the legacy constructor. Fixed at v2 so merely
/// teaching this binary to understand v3 does not silently enroll every writer
/// in new recovery semantics.
pub(crate) const LEGACY_SIDECAR_SCHEMA_VERSION: u32 = 2;

/// The version at which Phase-B confirmation shipped. A `BranchMerge` sidecar is
/// confirmation-aware (strict classification) iff `schema_version >=` this.
/// FIXED at 2 — NOT derived from [`SIDECAR_SCHEMA_VERSION`] — so a future bump to
/// v3+ still treats v2 sidecars as confirmation-aware.
pub(crate) const CONFIRMATION_SCHEMA_VERSION: u32 = 2;

/// Mutation/load exact-effect semantics shipped at v3. Fixed rather than tied
/// to the current maximum so future sidecar versions retain this floor.
pub(crate) const EXACT_EFFECT_IDENTITY_SCHEMA_VERSION: u32 = 3;

/// Exact schema generation emitted by [`new_occ_sidecar`]. Keep this fixed when
/// the reader learns a newer sidecar generation; v3 Mutation/Load semantics are
/// not reinterpreted by the v4 BranchMerge adapter.
pub(crate) const MUTATION_LOAD_SIDECAR_SCHEMA_VERSION: u32 = 3;

/// Exact schema generation emitted by [`new_branch_merge_sidecar`].
pub(crate) const BRANCH_MERGE_SIDECAR_SCHEMA_VERSION: u32 = 4;

/// Bound the cold-path transaction-history probes used by the v3/v4 exact
/// recovery protocols. Normal v3 recovery reads one version and a v4 logical
/// merge chain is currently at most three versions; a larger gap is derived
/// index work or foreign activity. Exceeding the bound is surfaced as
/// unverifiable rather than turning open into an unbounded history walk.
const MAX_EFFECT_IDENTITY_SCAN_VERSIONS: u64 = 1024;

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
    /// RFC-022 mutation/load effect: the sidecar must be durably confirmed and
    /// the observed Lance HEAD transaction `(read_version, uuid)` must exactly
    /// equal the planned and confirmed transaction identity.
    ExactEffect,
}

impl SidecarKind {
    /// Resolve the classification mode for this writer at a given sidecar
    /// `schema_version`. Exhaustive over `SidecarKind`, so adding a variant is a
    /// compile error here until its recovery semantics are declared.
    pub(crate) fn classification_mode(self, schema_version: u32) -> ClassificationMode {
        match self {
            SidecarKind::Mutation | SidecarKind::Load => {
                if schema_version >= EXACT_EFFECT_IDENTITY_SCHEMA_VERSION {
                    ClassificationMode::ExactEffect
                } else {
                    ClassificationMode::Strict
                }
            }
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

/// Authority captured before an RFC-022 mutation/load attempt was prepared.
/// Recovery compares this complete token before it may roll confirmed effects
/// forward. The native Lance branch identifier closes delete/recreate ABA;
/// `graph_head: None` deliberately preserves absence on a fresh branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryAuthorityToken {
    pub branch_identifier: lance::dataset::refs::BranchIdentifier,
    pub graph_head: Option<String>,
    pub schema_ir_hash: String,
    pub schema_identity_version: u32,
}

/// Serializable copy of the original graph lineage intent. It is minted once
/// before recovery is armed and never re-parented or replaced during recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryLineageIntent {
    pub graph_commit_id: String,
    pub branch: Option<String>,
    pub actor_id: Option<String>,
    pub merged_parent_commit_id: Option<String>,
    pub created_at: i64,
}

impl From<&RecoveryLineageIntent> for LineageIntent {
    fn from(intent: &RecoveryLineageIntent) -> Self {
        Self {
            graph_commit_id: intent.graph_commit_id.clone(),
            branch: intent.branch.clone(),
            actor_id: intent.actor_id.clone(),
            merged_parent_commit_id: intent.merged_parent_commit_id.clone(),
            created_at: intent.created_at,
        }
    }
}

/// Durable phase of an RFC-022 effect set. `Armed` is written before the first
/// HEAD advance. `EffectsConfirmed` is written only after every planned table
/// effect landed with its exact transaction identity and full manifest update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) enum RecoveryEffectPhase {
    Armed,
    EffectsConfirmed,
}

/// One exact physical effect enrolled in an RFC-022 sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryTableEffect {
    pub table_key: String,
    pub planned_transaction: StagedTransactionIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_transaction: Option<StagedTransactionIdentity>,
}

/// The confirmed value for one table-version output slot in the intended
/// manifest delta. Metadata is copied from `SubTableUpdate` so recovery can
/// publish exactly what the original writer prepared rather than re-deriving a
/// wider delta from an arbitrary live HEAD.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryConfirmedTableUpdate {
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
    pub version_metadata: super::TableVersionMetadata,
}

/// Canonical manifest-delta output slot for one touched table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryTableUpdateSlot {
    pub table_key: String,
    pub expected_version: u64,
    pub table_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed: Option<RecoveryConfirmedTableUpdate>,
}

/// Complete manifest delta carried by an RFC-022 mutation/load sidecar. The
/// table keys and expectations are immutable at arm time; physical output
/// slots are bound exactly once by Phase-B confirmation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryManifestDelta {
    pub table_updates: Vec<RecoveryTableUpdateSlot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registrations: Vec<SidecarTableRegistration>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tombstones: Vec<SidecarTombstone>,
}

/// v3 payload. Kept optional on [`RecoverySidecar`] so v1/v2 continue to parse
/// with their original semantics; [`parse_sidecar`] requires it for v3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryProtocolV3 {
    pub authority: RecoveryAuthorityToken,
    pub lineage: RecoveryLineageIntent,
    /// Stable id for the rollback lineage outcome. A recovery retry reuses it
    /// instead of manufacturing an unbounded series of indistinguishable ids.
    pub rollback_graph_commit_id: String,
    /// Exact operator-facing outcomes for the fixed rollback commit. Recovery
    /// persists this plan before the first restore or rollback publish and
    /// reuses it verbatim after a crash. `None` means no rollback has been
    /// prepared; `Some([])` is a prepared lineage-only rollback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_audit_outcomes: Option<Vec<TableOutcome>>,
    pub effect_phase: RecoveryEffectPhase,
    pub effects: Vec<RecoveryTableEffect>,
    pub intended_delta: RecoveryManifestDelta,
}

/// One branch-merge physical effect. A merge can make several staged commits
/// to one table, or it can create only a native target ref and publish that
/// source state by pointer. The latter is independently durable even though no
/// table HEAD moves, so it must be represented explicitly rather than inferred
/// from a numeric version comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryBranchMergeEffect {
    pub table_key: String,
    pub kind: RecoveryBranchMergeEffectKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "PascalCase")]
pub(crate) enum RecoveryBranchMergeEffectKind {
    /// One or more logical data commits on the target table. The exact final
    /// version is unknown while Armed and is bound atomically with the complete
    /// manifest delta at EffectsConfirmed. `source_fork_version` is present
    /// when the target table ref itself is first-touch and therefore must be
    /// recovered before any HEAD movement is considered.
    MultiCommitHead {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_fork_version: Option<u64>,
        /// Exact Lance transactions the writer pre-minted for the logical
        /// data effect, in commit order. Recovery proves ownership by reading
        /// these identities back at their expected versions; a numeric HEAD
        /// advance alone is never sufficient evidence.
        planned_transactions: Vec<StagedTransactionIdentity>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confirmed_version: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confirmed_branch_identifier: Option<lance::dataset::refs::BranchIdentifier>,
    },
    /// A first-touch target ref whose HEAD remains exactly the captured source
    /// fork version. Lance mints the target BranchIdentifier during create, so
    /// it is necessarily an EffectsConfirmed output rather than an arm-time
    /// input.
    RefOnlyFork {
        source_version: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confirmed_branch_identifier: Option<lance::dataset::refs::BranchIdentifier>,
    },
}

impl RecoveryBranchMergeEffectKind {
    fn source_fork_version(&self) -> Option<u64> {
        match self {
            Self::MultiCommitHead {
                source_fork_version,
                ..
            } => *source_fork_version,
            Self::RefOnlyFork { source_version, .. } => Some(*source_version),
        }
    }

    fn confirmed_branch_identifier(&self) -> Option<&lance::dataset::refs::BranchIdentifier> {
        match self {
            Self::MultiCommitHead {
                confirmed_branch_identifier,
                ..
            }
            | Self::RefOnlyFork {
                confirmed_branch_identifier,
                ..
            } => confirmed_branch_identifier.as_ref(),
        }
    }
}

/// v4 BranchMerge payload. Kept separate from v3 so old Mutation/Load files
/// retain their one-transaction-per-table interpretation forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryProtocolV4 {
    pub authority: RecoveryAuthorityToken,
    pub lineage: RecoveryLineageIntent,
    pub rollback_graph_commit_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_audit_outcomes: Option<Vec<TableOutcome>>,
    pub effect_phase: RecoveryEffectPhase,
    pub effects: Vec<RecoveryBranchMergeEffect>,
    pub intended_delta: RecoveryManifestDelta,
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
    /// commit id at the time the sidecar was written. Recovery replays the
    /// merge commit recording this as its `merged_parent_commit_id` (folded
    /// into the manifest publish, RFC-013 Phase 7), so future merges between
    /// the same pair recognize "already up-to-date" and merge-base
    /// computations stay correct. Optional — older sidecars (or
    /// non-BranchMerge kinds) carry `None`, recording a plain commit.
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
    /// RFC-022 exact-effect protocol. Absent for every v1/v2 sidecar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_v3: Option<RecoveryProtocolV3>,
    /// RFC-022 BranchMerge protocol. Present only on schema-v4 sidecars.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_v4: Option<RecoveryProtocolV4>,
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
    /// The observed HEAD version may have the expected number, but the Lance
    /// transaction identity does not equal the exact planned+confirmed effect.
    /// A foreign writer or transparent rebase produced it; never roll it
    /// forward as this sidecar's result.
    UnexpectedEffectIdentity,
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
/// - Any `UnexpectedAtP1` / `UnexpectedMultistep` /
///   `UnexpectedEffectIdentity` / `NoMovement` →
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
    debug_assert!(sidecar.schema_version <= SIDECAR_SCHEMA_VERSION);
    let uri = sidecar_uri(root_uri, &sidecar.operation_id);
    validate_sidecar_shape(&uri, sidecar)?;
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
#[cfg(test)]
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

/// Re-read one listed sidecar after its process-local recovery gates are held.
///
/// `list_sidecars` necessarily runs before gate acquisition so the caller knows
/// which branch/table keys to take.  The sidecar body is mutable, however:
/// BranchMerge stamps `confirmed_version`, and v3 writers move Armed ->
/// EffectsConfirmed (or persist a rollback audit plan) while holding those same
/// gates.  Existence alone therefore is not a sufficient post-wait recheck.  A
/// recovery pass must classify the body that is durable *after* it wins the
/// gates, not the stale body it used only for coordination discovery.
async fn reread_sidecar_under_gates(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    listed: &RecoverySidecar,
) -> Result<Option<RecoverySidecar>> {
    let uri = sidecar_uri(root_uri, &listed.operation_id);
    if !storage.exists(&uri).await? {
        return Ok(None);
    }
    let body = storage.read_text(&uri).await?;
    let current = parse_sidecar(&uri, &body)?;

    // Confirmation/rollback-plan rewrites may change only recovery payload, not
    // the keys whose gates protect the artifact.  Refuse a coordination-shape
    // rewrite rather than processing it under the wrong locks.
    let listed_keys = listed
        .tables
        .iter()
        .map(|pin| (&pin.table_key, &pin.table_path, &pin.table_branch))
        .collect::<Vec<_>>();
    let current_keys = current
        .tables
        .iter()
        .map(|pin| (&pin.table_key, &pin.table_path, &pin.table_branch))
        .collect::<Vec<_>>();
    if current.operation_id != listed.operation_id
        || current.writer_kind != listed.writer_kind
        || current.branch != listed.branch
        || current_keys != listed_keys
    {
        return Err(OmniError::manifest_internal(format!(
            "recovery sidecar '{}' changed its coordination keys while recovery waited for gates",
            listed.operation_id
        )));
    }
    Ok(Some(current))
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
    let sidecar: RecoverySidecar = serde_json::from_str(body).map_err(|err| {
        OmniError::manifest_internal(format!(
            "recovery sidecar at '{}' failed to deserialize: {}",
            sidecar_uri, err
        ))
    })?;
    validate_sidecar_shape(sidecar_uri, &sidecar)?;
    Ok(sidecar)
}

fn validate_sidecar_shape(sidecar_uri: &str, sidecar: &RecoverySidecar) -> Result<()> {
    let malformed = |reason: String| {
        OmniError::manifest_internal(format!(
            "recovery sidecar at '{}' has an invalid schema-v{} shape: {}",
            sidecar_uri, sidecar.schema_version, reason
        ))
    };

    if sidecar.schema_version < EXACT_EFFECT_IDENTITY_SCHEMA_VERSION {
        if sidecar.protocol_v3.is_some() || sidecar.protocol_v4.is_some() {
            return Err(malformed(
                "an exact-effect protocol is present on a pre-v3 sidecar".to_string(),
            ));
        }
        return Ok(());
    }

    if sidecar.schema_version == BRANCH_MERGE_SIDECAR_SCHEMA_VERSION {
        return validate_branch_merge_v4_shape(sidecar_uri, sidecar);
    }

    if sidecar.protocol_v4.is_some() {
        return Err(malformed(
            "protocol_v4 is present on a pre-v4 sidecar".to_string(),
        ));
    }

    if !matches!(
        sidecar.writer_kind,
        SidecarKind::Mutation | SidecarKind::Load
    ) {
        return Err(malformed(format!(
            "protocol_v3 exact effects are only defined for Mutation/Load, found {:?}",
            sidecar.writer_kind
        )));
    }
    let protocol = sidecar
        .protocol_v3
        .as_ref()
        .ok_or_else(|| malformed("missing required protocol_v3 payload".to_string()))?;
    if !sidecar.additional_registrations.is_empty()
        || !sidecar.tombstones.is_empty()
        || !protocol.intended_delta.registrations.is_empty()
        || !protocol.intended_delta.tombstones.is_empty()
    {
        return Err(malformed(
            "v3 Mutation/Load sidecars cannot register or tombstone tables".to_string(),
        ));
    }

    let sidecar_branch = sidecar.branch.as_deref().filter(|branch| *branch != "main");
    let lineage_branch = protocol
        .lineage
        .branch
        .as_deref()
        .filter(|branch| *branch != "main");
    if sidecar_branch != lineage_branch {
        return Err(malformed(
            "sidecar branch does not match original lineage branch".to_string(),
        ));
    }
    if sidecar.actor_id != protocol.lineage.actor_id {
        return Err(malformed(
            "sidecar actor does not match original lineage actor".to_string(),
        ));
    }
    if protocol.lineage.merged_parent_commit_id.is_some() {
        return Err(malformed(
            "v3 Mutation/Load lineage cannot carry a merged parent".to_string(),
        ));
    }
    if protocol.rollback_graph_commit_id == protocol.lineage.graph_commit_id {
        return Err(malformed(
            "original and rollback graph commit ids must differ".to_string(),
        ));
    }

    let pin_keys: HashSet<&str> = sidecar
        .tables
        .iter()
        .map(|pin| pin.table_key.as_str())
        .collect();
    if pin_keys.len() != sidecar.tables.len() {
        return Err(malformed("duplicate table pin".to_string()));
    }
    if let Some(outcomes) = protocol.rollback_audit_outcomes.as_ref() {
        let outcome_keys: HashSet<&str> = outcomes
            .iter()
            .map(|outcome| outcome.table_key.as_str())
            .collect();
        if outcome_keys.len() != outcomes.len() || !outcome_keys.is_subset(&pin_keys) {
            return Err(malformed(
                "rollback audit outcomes must name a unique subset of table pins".to_string(),
            ));
        }
    }
    let effect_keys: HashSet<&str> = protocol
        .effects
        .iter()
        .map(|effect| effect.table_key.as_str())
        .collect();
    let effect_uuids: HashSet<&str> = protocol
        .effects
        .iter()
        .map(|effect| effect.planned_transaction.uuid.as_str())
        .collect();
    let delta_keys: HashSet<&str> = protocol
        .intended_delta
        .table_updates
        .iter()
        .map(|slot| slot.table_key.as_str())
        .collect();
    if effect_keys.len() != protocol.effects.len()
        || effect_uuids.len() != protocol.effects.len()
        || delta_keys.len() != protocol.intended_delta.table_updates.len()
        || effect_keys != pin_keys
        || delta_keys != pin_keys
    {
        return Err(malformed(
            "table pins, planned transaction identities, and intended-delta key sets are not one-to-one"
                .to_string(),
        ));
    }

    for pin in &sidecar.tables {
        let effect = protocol
            .effects
            .iter()
            .find(|effect| effect.table_key == pin.table_key)
            .expect("key sets checked above");
        let slot = protocol
            .intended_delta
            .table_updates
            .iter()
            .find(|slot| slot.table_key == pin.table_key)
            .expect("key sets checked above");
        if effect.planned_transaction.read_version != pin.expected_version {
            return Err(malformed(format!(
                "planned transaction for '{}' reads version {}, pin expects {}",
                pin.table_key, effect.planned_transaction.read_version, pin.expected_version
            )));
        }
        if slot.expected_version != pin.expected_version || slot.table_branch != pin.table_branch {
            return Err(malformed(format!(
                "intended-delta pre-state for '{}' does not match its table pin",
                pin.table_key
            )));
        }
        match protocol.effect_phase {
            RecoveryEffectPhase::Armed => {
                if effect.confirmed_transaction.is_some()
                    || slot.confirmed.is_some()
                    || pin.confirmed_version.is_some()
                {
                    return Err(malformed(format!(
                        "Armed effect '{}' already carries confirmation",
                        pin.table_key
                    )));
                }
            }
            RecoveryEffectPhase::EffectsConfirmed => {
                let confirmed_transaction =
                    effect.confirmed_transaction.as_ref().ok_or_else(|| {
                        malformed(format!(
                            "EffectsConfirmed effect '{}' lacks a transaction identity",
                            pin.table_key
                        ))
                    })?;
                let confirmed_update = slot.confirmed.as_ref().ok_or_else(|| {
                    malformed(format!(
                        "EffectsConfirmed effect '{}' lacks a manifest update",
                        pin.table_key
                    ))
                })?;
                if confirmed_transaction != &effect.planned_transaction {
                    return Err(malformed(format!(
                        "confirmed transaction for '{}' differs from its planned identity",
                        pin.table_key
                    )));
                }
                if pin.confirmed_version != Some(confirmed_update.table_version)
                    || confirmed_update.table_branch != pin.table_branch
                {
                    return Err(malformed(format!(
                        "confirmed update for '{}' does not match its table pin",
                        pin.table_key
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_branch_merge_v4_shape(sidecar_uri: &str, sidecar: &RecoverySidecar) -> Result<()> {
    let malformed = |reason: String| {
        OmniError::manifest_internal(format!(
            "recovery sidecar at '{}' has an invalid schema-v{} shape: {}",
            sidecar_uri, sidecar.schema_version, reason
        ))
    };

    if sidecar.writer_kind != SidecarKind::BranchMerge {
        return Err(malformed(format!(
            "protocol_v4 is only defined for BranchMerge, found {:?}",
            sidecar.writer_kind
        )));
    }
    if sidecar.protocol_v3.is_some() {
        return Err(malformed(
            "schema-v4 BranchMerge sidecar also carries protocol_v3".to_string(),
        ));
    }
    if sidecar.merge_source_commit_id.is_some()
        || !sidecar.additional_registrations.is_empty()
        || !sidecar.tombstones.is_empty()
    {
        return Err(malformed(
            "schema-v4 BranchMerge must carry lineage/delta only in protocol_v4".to_string(),
        ));
    }
    let protocol = sidecar
        .protocol_v4
        .as_ref()
        .ok_or_else(|| malformed("missing required protocol_v4 payload".to_string()))?;
    if !protocol.intended_delta.registrations.is_empty()
        || !protocol.intended_delta.tombstones.is_empty()
    {
        return Err(malformed(
            "BranchMerge cannot register or tombstone tables".to_string(),
        ));
    }

    let sidecar_branch = sidecar.branch.as_deref().filter(|branch| *branch != "main");
    let lineage_branch = protocol
        .lineage
        .branch
        .as_deref()
        .filter(|branch| *branch != "main");
    if sidecar_branch != lineage_branch {
        return Err(malformed(
            "sidecar branch does not match original merge lineage branch".to_string(),
        ));
    }
    if sidecar.actor_id != protocol.lineage.actor_id {
        return Err(malformed(
            "sidecar actor does not match original merge lineage actor".to_string(),
        ));
    }
    if protocol.lineage.merged_parent_commit_id.is_none() {
        return Err(malformed(
            "BranchMerge lineage must carry the captured source commit".to_string(),
        ));
    }
    if protocol.rollback_graph_commit_id == protocol.lineage.graph_commit_id {
        return Err(malformed(
            "original and rollback graph commit ids must differ".to_string(),
        ));
    }

    let pin_keys: HashSet<&str> = sidecar
        .tables
        .iter()
        .map(|pin| pin.table_key.as_str())
        .collect();
    let effect_keys: HashSet<&str> = protocol
        .effects
        .iter()
        .map(|effect| effect.table_key.as_str())
        .collect();
    let delta_keys: HashSet<&str> = protocol
        .intended_delta
        .table_updates
        .iter()
        .map(|slot| slot.table_key.as_str())
        .collect();
    if sidecar.tables.is_empty()
        || pin_keys.len() != sidecar.tables.len()
        || effect_keys.len() != protocol.effects.len()
        || delta_keys.len() != protocol.intended_delta.table_updates.len()
        || effect_keys != pin_keys
        || !effect_keys.is_subset(&delta_keys)
    {
        return Err(malformed(
            "physical table pins/effects must be one-to-one and a subset of unique intended-delta slots"
                .to_string(),
        ));
    }
    if let Some(outcomes) = protocol.rollback_audit_outcomes.as_ref() {
        let outcome_keys: HashSet<&str> = outcomes
            .iter()
            .map(|outcome| outcome.table_key.as_str())
            .collect();
        if outcome_keys.len() != outcomes.len() || !outcome_keys.is_subset(&pin_keys) {
            return Err(malformed(
                "rollback audit outcomes must name a unique subset of physical table pins"
                    .to_string(),
            ));
        }
    }

    for slot in &protocol.intended_delta.table_updates {
        match protocol.effect_phase {
            RecoveryEffectPhase::Armed if slot.confirmed.is_some() => {
                return Err(malformed(format!(
                    "Armed manifest slot '{}' already carries confirmation",
                    slot.table_key
                )));
            }
            RecoveryEffectPhase::EffectsConfirmed if slot.confirmed.is_none() => {
                return Err(malformed(format!(
                    "EffectsConfirmed manifest slot '{}' lacks its exact output",
                    slot.table_key
                )));
            }
            _ => {}
        }
        if let Some(confirmed) = slot.confirmed.as_ref()
            && confirmed.table_branch != slot.table_branch
        {
            return Err(malformed(format!(
                "confirmed output branch for '{}' differs from its planned slot",
                slot.table_key
            )));
        }
    }

    let mut planned_transaction_uuids = HashSet::new();
    for pin in &sidecar.tables {
        let effect = protocol
            .effects
            .iter()
            .find(|effect| effect.table_key == pin.table_key)
            .expect("effect/pin key sets checked above");
        let slot = protocol
            .intended_delta
            .table_updates
            .iter()
            .find(|slot| slot.table_key == pin.table_key)
            .expect("physical effects are a subset of delta slots");
        if slot.expected_version != pin.expected_version || slot.table_branch != pin.table_branch {
            return Err(malformed(format!(
                "intended-delta pre-state for '{}' does not match its physical table pin",
                pin.table_key
            )));
        }
        if pin
            .table_branch
            .as_deref()
            .is_none_or(|branch| branch == "main")
            && effect.kind.source_fork_version().is_some()
        {
            return Err(malformed(format!(
                "first-touch effect '{}' does not name a non-main target ref",
                pin.table_key
            )));
        }
        if let Some(source_fork_version) = effect.kind.source_fork_version()
            && matches!(
                &effect.kind,
                RecoveryBranchMergeEffectKind::MultiCommitHead { .. }
            )
            && source_fork_version != pin.expected_version
        {
            return Err(malformed(format!(
                "first-touch multi-commit effect '{}' forks from version {}, pin expects {}",
                pin.table_key, source_fork_version, pin.expected_version
            )));
        }
        match (&effect.kind, protocol.effect_phase) {
            (
                RecoveryBranchMergeEffectKind::MultiCommitHead {
                    planned_transactions,
                    confirmed_version,
                    confirmed_branch_identifier,
                    source_fork_version,
                },
                RecoveryEffectPhase::Armed,
            ) => {
                validate_branch_merge_transaction_chain(
                    &malformed,
                    pin,
                    planned_transactions,
                    &mut planned_transaction_uuids,
                )?;
                if confirmed_version.is_some()
                    || confirmed_branch_identifier.is_some()
                    || pin.confirmed_version.is_some()
                {
                    return Err(malformed(format!(
                        "Armed multi-commit effect '{}' already carries confirmation",
                        pin.table_key
                    )));
                }
                if source_fork_version.is_none() && confirmed_branch_identifier.is_some() {
                    return Err(malformed(format!(
                        "owned-ref effect '{}' unexpectedly carries a branch identifier",
                        pin.table_key
                    )));
                }
            }
            (
                RecoveryBranchMergeEffectKind::MultiCommitHead {
                    planned_transactions,
                    confirmed_version,
                    confirmed_branch_identifier,
                    source_fork_version,
                },
                RecoveryEffectPhase::EffectsConfirmed,
            ) => {
                let planned_final_version = validate_branch_merge_transaction_chain(
                    &malformed,
                    pin,
                    planned_transactions,
                    &mut planned_transaction_uuids,
                )?;
                let version = confirmed_version.ok_or_else(|| {
                    malformed(format!(
                        "EffectsConfirmed multi-commit effect '{}' lacks final version",
                        pin.table_key
                    ))
                })?;
                let confirmed = slot.confirmed.as_ref().expect("phase checked above");
                if pin.confirmed_version != Some(version)
                    || confirmed.table_version != version
                    || version < planned_final_version
                {
                    return Err(malformed(format!(
                        "multi-commit effect '{}' confirmation does not match its pin/delta",
                        pin.table_key
                    )));
                }
                if source_fork_version.is_some() != confirmed_branch_identifier.is_some() {
                    return Err(malformed(format!(
                        "first-touch multi-commit effect '{}' must confirm its target ref identity",
                        pin.table_key
                    )));
                }
            }
            (
                RecoveryBranchMergeEffectKind::RefOnlyFork {
                    source_version: _,
                    confirmed_branch_identifier,
                },
                RecoveryEffectPhase::Armed,
            ) => {
                if confirmed_branch_identifier.is_some() || pin.confirmed_version.is_some() {
                    return Err(malformed(format!(
                        "Armed ref-only effect '{}' already carries confirmation",
                        pin.table_key
                    )));
                }
            }
            (
                RecoveryBranchMergeEffectKind::RefOnlyFork {
                    source_version,
                    confirmed_branch_identifier,
                },
                RecoveryEffectPhase::EffectsConfirmed,
            ) => {
                let confirmed = slot.confirmed.as_ref().expect("phase checked above");
                if confirmed_branch_identifier.is_none()
                    || pin.confirmed_version != Some(*source_version)
                    || confirmed.table_version != *source_version
                {
                    return Err(malformed(format!(
                        "ref-only effect '{}' confirmation does not match its exact fork version",
                        pin.table_key
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_branch_merge_transaction_chain(
    malformed: &impl Fn(String) -> OmniError,
    pin: &SidecarTablePin,
    planned_transactions: &[StagedTransactionIdentity],
    planned_transaction_uuids: &mut HashSet<String>,
) -> Result<u64> {
    let first = planned_transactions.first().ok_or_else(|| {
        malformed(format!(
            "multi-commit effect '{}' has an empty planned transaction chain",
            pin.table_key
        ))
    })?;
    if first.read_version != pin.expected_version {
        return Err(malformed(format!(
            "planned transaction chain for '{}' begins at version {}, pin expects {}",
            pin.table_key, first.read_version, pin.expected_version
        )));
    }
    let first_output = first.read_version.checked_add(1).ok_or_else(|| {
        malformed(format!(
            "planned transaction chain for '{}' overflows its first output version",
            pin.table_key
        ))
    })?;
    if pin.post_commit_pin != first_output {
        return Err(malformed(format!(
            "post-commit pin for '{}' is {}, first planned output is {}",
            pin.table_key, pin.post_commit_pin, first_output
        )));
    }

    for (index, planned) in planned_transactions.iter().enumerate() {
        if planned.uuid.is_empty() || !planned_transaction_uuids.insert(planned.uuid.clone()) {
            return Err(malformed(format!(
                "planned transaction chain for '{}' contains an empty or duplicate identity at offset {}",
                pin.table_key, index
            )));
        }
        let expected_read_version = pin
            .expected_version
            .checked_add(u64::try_from(index).map_err(|_| {
                malformed(format!(
                    "planned transaction chain for '{}' exceeds u64",
                    pin.table_key
                ))
            })?)
            .ok_or_else(|| {
                malformed(format!(
                    "planned transaction chain for '{}' overflows at offset {}",
                    pin.table_key, index
                ))
            })?;
        if planned.read_version != expected_read_version {
            return Err(malformed(format!(
                "planned transaction chain for '{}' reads version {} at offset {}, expected {}",
                pin.table_key, planned.read_version, index, expected_read_version
            )));
        }
    }

    planned_transactions
        .last()
        .expect("non-empty chain checked above")
        .read_version
        .checked_add(1)
        .ok_or_else(|| {
            malformed(format!(
                "planned transaction chain for '{}' overflows its final output version",
                pin.table_key
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
#[cfg(test)]
pub(crate) fn classify_table(
    pin: &SidecarTablePin,
    lance_head: u64,
    manifest_pinned: u64,
    kind: SidecarKind,
    schema_version: u32,
) -> TableClassification {
    classify_table_observation(
        pin,
        lance_head,
        None,
        manifest_pinned,
        kind,
        schema_version,
        None,
    )
}

fn classify_table_observation(
    pin: &SidecarTablePin,
    lance_head: u64,
    observed_transaction: Option<&StagedTransactionIdentity>,
    manifest_pinned: u64,
    kind: SidecarKind,
    schema_version: u32,
    protocol_v3: Option<&RecoveryProtocolV3>,
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
        ClassificationMode::ExactEffect => {
            let Some(protocol) = protocol_v3 else {
                return IncompletePhaseB;
            };
            if protocol.effect_phase != RecoveryEffectPhase::EffectsConfirmed {
                return IncompletePhaseB;
            }
            let Some(effect) = protocol
                .effects
                .iter()
                .find(|effect| effect.table_key == pin.table_key)
            else {
                return IncompletePhaseB;
            };
            let Some(slot) = protocol
                .intended_delta
                .table_updates
                .iter()
                .find(|slot| slot.table_key == pin.table_key)
            else {
                return IncompletePhaseB;
            };
            let (Some(confirmed_transaction), Some(confirmed_update)) = (
                effect.confirmed_transaction.as_ref(),
                slot.confirmed.as_ref(),
            ) else {
                return IncompletePhaseB;
            };
            if pin.expected_version != manifest_pinned {
                return UnexpectedAtP1;
            }
            if lance_head != confirmed_update.table_version
                || pin.confirmed_version != Some(confirmed_update.table_version)
            {
                return UnexpectedMultistep;
            }
            if confirmed_transaction != &effect.planned_transaction
                || observed_transaction != Some(confirmed_transaction)
            {
                return UnexpectedEffectIdentity;
            }
            RolledPastExpected
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
    if classifications.iter().any(|c| {
        matches!(
            c,
            NoMovement
                | UnexpectedAtP1
                | UnexpectedMultistep
                | UnexpectedEffectIdentity
                | IncompletePhaseB
        )
    }) {
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
    let head = crate::instrumentation::open_dataset(
        table_path,
        crate::instrumentation::VersionResolution::Latest,
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await?;
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
/// Concurrency: unlike the open-time sweep, this runs while other writers may
/// be in flight. RFC-022 mutation/load holds root-scoped schema → branch →
/// sorted table gates across its sidecar lifetime; legacy adapters hold their
/// applicable schema/table gates. Healing takes the ordered superset, so it
/// blocks until the sidecar writer either
/// finished (sidecar deleted; the under-gate reread skips it) or died
/// (the freshly parsed sidecar is genuinely orphaned and safe to process). Without this, the
/// heal could observe a live writer's sidecar in its commit→publish
/// window, roll it forward, and fail that writer's own publish CAS.
/// Lock order is schema → branch → sorted tables → coordinator, matching the
/// RFC-022 writer and Full-recovery paths.
///
/// The schema-staging reconcile runs lazily, per SchemaApply sidecar,
/// AFTER that sidecar's queue guards are held and its existence is
/// re-confirmed — never up front. An up-front reconcile can promote a
/// LIVE schema apply's staging files and steal its commit (pinned by
/// `tests/failpoints.rs::heal_does_not_promote_live_schema_apply_staging`).
///
/// One sidecar that RollForwardOnly deliberately left in place after acquiring
/// its root-scoped gates and re-checking that the artifact still exists.
///
/// Write-entry barriers use the captured graph branch to reject only relevant
/// intents. SchemaApply is graph-global and is filtered specially by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnresolvedRecoveryIntent {
    pub(crate) operation_id: String,
    /// Normalized graph branch (`None` means main; `Some("main")` is folded).
    pub(crate) branch: Option<String>,
    pub(crate) writer_kind: SidecarKind,
    /// Diagnostic ownership surface for future table-aware barriers. RFC-022's
    /// first adapter fences mutation/load at graph-branch granularity.
    pub(crate) table_keys: Vec<String>,
}

/// Returns whether durable state changed and every guarded intent that could
/// not be resolved without destructive recovery. Callers invalidate caches on
/// `processed_any`; mutation/load barriers additionally filter `unresolved` by
/// their captured graph branches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HealPendingOutcome {
    pub(crate) processed_any: bool,
    pub(crate) unresolved: Vec<UnresolvedRecoveryIntent>,
}

pub(crate) async fn heal_pending_sidecars_roll_forward(
    root_uri: &str,
    storage: std::sync::Arc<dyn StorageAdapter>,
    coordinator: &tokio::sync::RwLock<GraphCoordinator>,
    write_queue: &crate::db::write_queue::WriteQueueManager,
) -> Result<HealPendingOutcome> {
    let sidecars = list_sidecars(root_uri, storage.as_ref()).await?;
    if sidecars.is_empty() {
        return Ok(HealPendingOutcome {
            processed_any: false,
            unresolved: Vec::new(),
        });
    }
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_POST_LIST_PRE_GATES)?;
    let mut processed_any = false;
    let mut unresolved = Vec::new();
    for sidecar in sidecars {
        // Serialize against a possibly-live writer (see fn docs). Guards are
        // scoped per sidecar and follow the one total order shared by writers,
        // Full recovery, and live healing. Taking schema + branch even for an
        // empty/legacy sidecar also serializes its audit/delete lifecycle.
        let _schema_guard = write_queue.acquire(&schema_apply_serial_queue_key()).await;
        let _branch_guard = write_queue.acquire_branch(sidecar.branch.as_deref()).await;
        let queue_keys: Vec<crate::db::write_queue::TableQueueKey> = sidecar
            .tables
            .iter()
            .map(|pin| (pin.table_key.clone(), pin.table_branch.clone()))
            .collect();
        let is_schema_apply = matches!(sidecar.writer_kind, SidecarKind::SchemaApply);
        let _table_guards = write_queue.acquire_many(&queue_keys).await;
        // Re-read after the wait: the writer we blocked on may have completed
        // Phase C and deleted the sidecar, or may have durably confirmed Phase B
        // before releasing its gates.  Processing the pre-wait body would turn a
        // fully confirmed effect set back into an apparent partial one.
        let Some(sidecar) =
            reread_sidecar_under_gates(root_uri, storage.as_ref(), &sidecar).await?
        else {
            continue;
        };
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
        } else {
            let mut table_keys = sidecar
                .tables
                .iter()
                .map(|pin| pin.table_key.clone())
                .collect::<Vec<_>>();
            table_keys.sort();
            table_keys.dedup();
            unresolved.push(UnresolvedRecoveryIntent {
                operation_id: sidecar.operation_id.clone(),
                branch: sidecar.branch.clone().filter(|branch| branch != "main"),
                writer_kind: sidecar.writer_kind,
                table_keys,
            });
        }
    }
    // Re-read coordinator state so the caller's handle observes the
    // post-heal manifest.
    coordinator.write().await.refresh().await?;
    Ok(HealPendingOutcome {
        processed_any,
        unresolved,
    })
}

/// Discard a sidecar whose branch no longer exists in the manifest (the
/// authority — callers must key the orphan classification off the branch
/// LIST, never off a `Not found` from an open, which could be a transient
/// storage error masking real recovery intent). The branch's tree and
/// per-table forks are already reclaimed, so the drift the sidecar pins is
/// unreachable and the sidecar is provably moot; leaving it would wedge
/// every heal (write entry) and every ReadWrite open on a dead-branch
/// open, with `repair` refusing while it pends. Records an
/// `OrphanedBranchDiscarded` audit row (lineage published on main — the
/// sidecar's own branch no longer has a live graph head).
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
    // Documented residual: the lineage CAS in `__manifest` and the audit append
    // are two writes. A failure between them leaves a recovery commit with no
    // audit row; a legacy sidecar carries no fixed recovery-commit id, so the
    // retry (keyed on the operator-facing audit row) may append a second lineage
    // commit before that one audit row lands. Keying idempotency on lineage
    // would require a durable operation id there, while audit-before-lineage
    // would leave a dangling `graph_commit_id` join.
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
        publisher
            .publish(&[], &HashMap::new(), Some(&intent))
            .await?;
        // Failpoint: the residual window above — commit published, audit
        // not yet durable.
        crate::failpoints::maybe_fail(
            crate::failpoints::names::RECOVERY_ORPHAN_DISCARD_AUDIT_APPEND,
        )?;
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
/// Idempotency: a crash mid-sweep leaves the sidecar (deletion is the final
/// step). v3 persists an exact rollback audit plan before restoring and uses a
/// fixed rollback commit id, so re-entry resumes that outcome without another
/// restore or synthetic commit. Legacy sidecars re-classify and may append an
/// extra restore commit, which `omnigraph cleanup` reclaims.
///
/// Concurrency: a newly-opening handle is not yet published, but another handle
/// for the same root may already be serving writes. Every handle obtains the
/// root-scoped [`WriteQueueManager`](crate::db::write_queue::WriteQueueManager).
/// The caller holds its schema gate across schema-file recovery and this whole
/// pass; Full recovery adds branch → sorted table gates per sidecar and
/// re-reads/re-parses the sidecar after waiting. A live writer either finishes
/// and deletes its sidecar (recovery skips it), durably confirms a newer body
/// (recovery classifies that body), or releases the gates with a genuinely pending
/// intent (recovery owns it). This is process-local serialization only;
/// the documented foreign-process boundary remains.
pub(crate) async fn recover_manifest_drift(
    root_uri: &str,
    storage: std::sync::Arc<dyn StorageAdapter>,
    coordinator: &mut GraphCoordinator,
    mode: RecoveryMode,
    schema_state_recovery: SchemaStateRecovery,
    write_queue: &crate::db::write_queue::WriteQueueManager,
) -> Result<()> {
    let sidecars = list_sidecars(root_uri, storage.as_ref()).await?;
    if sidecars.is_empty() {
        return Ok(());
    }
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_POST_LIST_PRE_GATES)?;

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
        // Total order shared with RFC-022 writers. The caller already holds the
        // root-scoped schema gate; add branch then sorted table gates so a
        // concurrent open/refresh cannot Restore or delete a ref underneath a
        // live writer from another `Omnigraph` handle.
        let _branch_guard = write_queue.acquire_branch(sidecar.branch.as_deref()).await;
        let table_keys = sidecar
            .tables
            .iter()
            .map(|pin| (pin.table_key.clone(), pin.table_branch.clone()))
            .collect::<Vec<_>>();
        let _table_guards = write_queue.acquire_many(&table_keys).await;

        // The writer may have completed Phase C/D or rewritten Armed ->
        // confirmed while this recovery instance waited.  Re-read the complete
        // body under the gates; an existence-only check still leaves recovery
        // classifying stale confirmation state.
        let Some(sidecar) =
            reread_sidecar_under_gates(root_uri, storage.as_ref(), &sidecar).await?
        else {
            continue;
        };

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
    if sidecar.protocol_v4.is_some() {
        return process_branch_merge_sidecar_v4(root_uri, storage, snapshot, sidecar, mode).await;
    }
    if sidecar.protocol_v3.is_some() {
        if let Some(outcome) = detect_visible_v3_outcome(root_uri, sidecar).await? {
            return finalize_visible_v3_outcome(root_uri, storage.as_ref(), sidecar, outcome).await;
        }
    }
    let mut states = Vec::with_capacity(sidecar.tables.len());
    for pin in &sidecar.tables {
        let manifest_pinned = snapshot
            .entry(&pin.table_key)
            .map(|e| e.table_version)
            .unwrap_or(0);
        // A first-touch named-branch mutation/load stages against the inherited
        // source snapshot, arms its v3 sidecar, and only then creates the Lance
        // target ref. Until Phase C publishes, the branch snapshot therefore
        // still points at the source ref. Armed recovery must tolerate the
        // target ref not existing yet: that is the crash-before-fork state, not
        // storage corruption. Once effects are confirmed, a missing ref is
        // impossible and remains a loud error.
        let unpublished_fork = sidecar.protocol_v3.is_some()
            && pin
                .table_branch
                .as_deref()
                .is_some_and(|branch| branch != "main")
            && sidecar.branch.as_deref() == pin.table_branch.as_deref()
            && snapshot
                .entry(&pin.table_key)
                .map(|entry| entry.table_branch != pin.table_branch)
                .unwrap_or(true);
        let allow_missing_target_ref = unpublished_fork
            && sidecar
                .protocol_v3
                .as_ref()
                .is_some_and(|protocol| protocol.effect_phase == RecoveryEffectPhase::Armed);
        let planned_effect = sidecar.protocol_v3.as_ref().and_then(|protocol| {
            protocol
                .effects
                .iter()
                .find(|effect| effect.table_key == pin.table_key)
                .map(|effect| {
                    (
                        pin.post_commit_pin,
                        &effect.planned_transaction,
                        manifest_pinned,
                    )
                })
        });
        let observation = open_lance_head_if_present(
            &pin.table_path,
            pin.table_branch.as_deref(),
            planned_effect,
            allow_missing_target_ref,
        )
        .await?;
        let observation = observation.unwrap_or(LanceHeadObservation {
            version: manifest_pinned,
            transaction: None,
            effect_ownership: EffectOwnership::None,
        });
        let lance_head = observation.version;
        states.push(ClassifiedTable {
            classification: classify_table_observation(
                pin,
                lance_head,
                observation.transaction.as_ref(),
                manifest_pinned,
                sidecar.writer_kind,
                sidecar.schema_version,
                sidecar.protocol_v3.as_ref(),
            ),
            manifest_pinned,
            lance_head,
            effect_ownership: observation.effect_ownership,
            unpublished_fork,
        });
    }

    if let Some(protocol) = sidecar.protocol_v3.as_ref() {
        let live_authority =
            read_live_recovery_authority(root_uri, storage, sidecar.branch.as_deref()).await?;
        let branch_recreated =
            live_authority.branch_identifier != protocol.authority.branch_identifier;
        let authority_changed = live_authority != protocol.authority;
        let any_unverifiable = states
            .iter()
            .any(|state| state.effect_ownership == EffectOwnership::Unverifiable);
        let any_own_effect = states.iter().any(|state| {
            matches!(
                state.effect_ownership,
                EffectOwnership::OwnAtHead
                    | EffectOwnership::OwnBeforeHead
                    | EffectOwnership::OwnCompensatedAtHead
            )
        });
        let own_effect_not_at_head = states
            .iter()
            .any(|state| state.effect_ownership == EffectOwnership::OwnBeforeHead);
        let ambiguous_manifest_advance =
            sidecar
                .tables
                .iter()
                .zip(states.iter())
                .any(|(pin, state)| {
                    // Lance's one-attempt commit can preflight-rebase our
                    // staged append over a winner that already committed and
                    // published: expected=v1, manifest winner=v2, our UUID
                    // still HEAD=v3. Restoring to CURRENT manifest pin v2
                    // preserves the winner and removes only our stale effect.
                    // A manifest that selects our UUID (OwnBeforeHead or
                    // manifest_pinned == lance_head) remains unsafe.
                    // The same exception remains safe after a previous Full
                    // pass has already appended Restore(current manifest pin)
                    // but crashed before publishing that restore commit.
                    // `OwnCompensatedAtHead` is assigned only when the HEAD
                    // Restore target exactly equals this current pin.
                    let safe_published_lower_winner = matches!(
                        state.effect_ownership,
                        EffectOwnership::OwnAtHead | EffectOwnership::OwnCompensatedAtHead
                    ) && state.manifest_pinned
                        > pin.expected_version
                        && state.manifest_pinned < state.lance_head;
                    state.effect_ownership != EffectOwnership::None
                        && state.manifest_pinned != pin.expected_version
                        && !safe_published_lower_winner
                });
        let foreign_table_drift = states.iter().any(|state| {
            state.effect_ownership == EffectOwnership::None
                && state.lance_head > state.manifest_pinned
        });

        if any_unverifiable {
            let message = format!(
                "OCC recovery sidecar '{}' cannot verify one or more planned Lance transaction \
                 files; refusing destructive recovery",
                sidecar.operation_id
            );
            return match mode {
                RecoveryMode::RollForwardOnly => {
                    warn!(operation_id = sidecar.operation_id.as_str(), "{message}");
                    Ok(false)
                }
                RecoveryMode::Full => Err(OmniError::manifest_internal(message)),
            };
        }

        if !any_own_effect {
            // The sidecar was armed but this writer never landed a physical
            // effect. RollForwardOnly recovery may be looking at a LIVE writer,
            // so an Armed sidecar is still ownership and must be left untouched
            // even though root-scoped queues make it wait for in-process
            // handles. A quiesced Full sweep may abandon it; first remove only
            // exact, unpublished first-touch forks that this intent owns. Do
            // not manufacture lineage for an empty intent and never restore a
            // foreign advance.
            if matches!(mode, RecoveryMode::RollForwardOnly)
                && (protocol.effect_phase == RecoveryEffectPhase::Armed
                    || states.iter().any(|state| state.unpublished_fork))
            {
                warn!(
                    operation_id = sidecar.operation_id.as_str(),
                    "recovery: deferring v3 sidecar with no physical effects and live ownership"
                );
                return Ok(false);
            }
            if matches!(mode, RecoveryMode::Full) {
                if let NoEffectForkCleanup::DeferredPathChild {
                    table_path,
                    target_branch,
                    path_child,
                } = cleanup_unpublished_no_effect_forks(
                    root_uri,
                    storage.as_ref(),
                    sidecar,
                    &states,
                )
                .await?
                {
                    warn!(
                        operation_id = sidecar.operation_id.as_str(),
                        table_path,
                        branch = target_branch,
                        path_child,
                        "recovery: deferring no-effect fork cleanup until legacy path-child \
                         branches are deleted leaf-first"
                    );
                    return Ok(false);
                }
            }
            warn!(
                operation_id = sidecar.operation_id.as_str(),
                authority_changed, "recovery: abandoning v3 sidecar with no owned physical effects"
            );
            delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &sidecar.operation_id)
                .await?;
            return Ok(true);
        }

        if branch_recreated {
            // A restore through the reused branch name would target the NEW
            // incarnation and can destroy unrelated data. Leave the sidecar for
            // operator review in every mode; the old incarnation's effects are
            // not safe to compensate through this ref.
            let message = format!(
                "OCC recovery sidecar '{}' targets a branch incarnation that was deleted and \
                 recreated; refusing to restore through the reused branch name",
                sidecar.operation_id
            );
            return match mode {
                RecoveryMode::RollForwardOnly => {
                    warn!(operation_id = sidecar.operation_id.as_str(), "{message}");
                    Ok(false)
                }
                RecoveryMode::Full => Err(OmniError::manifest_internal(message)),
            };
        }

        if own_effect_not_at_head || ambiguous_manifest_advance || foreign_table_drift {
            let message = format!(
                "OCC recovery sidecar '{}' is interleaved with a foreign table/manifest advance; \
                 refusing a restore that could discard another writer",
                sidecar.operation_id
            );
            return match mode {
                RecoveryMode::RollForwardOnly => {
                    warn!(operation_id = sidecar.operation_id.as_str(), "{message}");
                    Ok(false)
                }
                RecoveryMode::Full => Err(OmniError::manifest_internal(message)),
            };
        }

        if authority_changed {
            // At least one exact owned effect is still HEAD, but the branch
            // authority used to prepare it changed. It must never roll forward.
            // In-process healing cannot restore safely; a quiesced Full sweep
            // compensates only the owned-at-HEAD tables.
            if matches!(mode, RecoveryMode::RollForwardOnly) {
                warn!(
                    operation_id = sidecar.operation_id.as_str(),
                    "recovery: deferring token-mismatched v3 sidecar with physical effects"
                );
                return Ok(false);
            }
            warn!(
                operation_id = sidecar.operation_id.as_str(),
                "recovery: rolling back v3 sidecar because its captured authority changed"
            );
            roll_back_sidecar(root_uri, storage.as_ref(), sidecar, &states).await?;
            return Ok(true);
        }
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
                if sidecar.protocol_v3.is_some() {
                    // Fixed outcome ids were checked before table
                    // classification. Alignment without either id is not proof
                    // of this sidecar's outcome.
                    return match mode {
                        RecoveryMode::RollForwardOnly => Ok(false),
                        RecoveryMode::Full => Err(OmniError::manifest_internal(format!(
                            "OCC recovery sidecar '{}' is manifest-aligned but neither fixed \
                             outcome id is visible; operator review required",
                            sidecar.operation_id
                        ))),
                    };
                }
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
                    root_uri,
                    storage.as_ref(),
                    sidecar,
                    &states,
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
                    // Another recovery instance may have published this exact
                    // fixed intent after our classification but before our CAS.
                    // Exact-head OCC correctly rejects our stale attempt; prove
                    // the original id + delta are now visible and converge
                    // without manufacturing another commit.
                    Err(err) if sidecar.protocol_v3.is_some() && err.is_read_set_changed() => {
                        if let Some(outcome) = detect_visible_v3_outcome(root_uri, sidecar).await? {
                            return finalize_visible_v3_outcome(
                                root_uri,
                                storage.as_ref(),
                                sidecar,
                                outcome,
                            )
                            .await;
                        }
                        return Err(err);
                    }
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

struct BranchMergeRefObservation {
    dataset: lance::Dataset,
    version: u64,
    branch_identifier: lance::dataset::refs::BranchIdentifier,
    parent_version: Option<u64>,
}

/// Observe one physical target ref without treating an absent first-touch ref
/// as a storage error. A named ref's BranchContents identity is separate from
/// the graph-manifest branch identity carried by `RecoveryAuthorityToken`; both
/// are required to close their respective delete/recreate ABA windows.
async fn observe_branch_merge_target_ref(
    pin: &SidecarTablePin,
) -> Result<Option<BranchMergeRefObservation>> {
    let dataset = crate::instrumentation::open_dataset(
        &pin.table_path,
        crate::instrumentation::VersionResolution::Latest,
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await?;
    let Some(branch) = pin
        .table_branch
        .as_deref()
        .filter(|branch| *branch != "main")
    else {
        let branch_identifier = dataset
            .branch_identifier()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        let version = dataset.version().version;
        return Ok(Some(BranchMergeRefObservation {
            dataset,
            version,
            branch_identifier,
            parent_version: None,
        }));
    };
    let branches = dataset
        .list_branches()
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let Some(contents) = branches.get(branch) else {
        return Ok(None);
    };
    let target = dataset
        .checkout_branch(branch)
        .await
        .map_err(|error| OmniError::Lance(error.to_string()))?;
    let version = target.version().version;
    Ok(Some(BranchMergeRefObservation {
        dataset: target,
        version,
        branch_identifier: contents.identifier.clone(),
        parent_version: Some(contents.parent_version),
    }))
}

struct BranchMergeMultiCommitProof {
    effect_ownership: EffectOwnership,
    /// Every planned logical-data transaction is present in exact order and
    /// every later commit through the observed HEAD is derived CreateIndex
    /// work. Confirmation still has to bind that exact HEAD before recovery
    /// may roll it forward.
    full_effect_at_head: bool,
    unsafe_reason: Option<String>,
}

impl BranchMergeMultiCommitProof {
    fn unverifiable(reason: String) -> Self {
        Self {
            effect_ownership: EffectOwnership::Unverifiable,
            full_effect_at_head: false,
            unsafe_reason: Some(reason),
        }
    }
}

/// Prove ownership of a BranchMerge multi-commit effect from Lance's durable
/// transaction history. Numeric version movement is only a scan bound: each
/// logical data commit must match its pre-minted identity at its exact output
/// version. Once the full chain is present, inline index reconciliation may
/// contribute a derived `CreateIndex` tail. No other operation is attributable
/// to this merge.
///
/// A final Restore to the still-manifest-pinned version is the compensation a
/// previous Full recovery created. Recognizing it makes the restore→manifest
/// publish crash window restartable without appending another Restore commit.
async fn prove_branch_merge_multi_commit_effect(
    observation: &BranchMergeRefObservation,
    planned_transactions: &[StagedTransactionIdentity],
    manifest_pinned: u64,
    table_key: &str,
) -> Result<BranchMergeMultiCommitProof> {
    let first = planned_transactions
        .first()
        .expect("v4 sidecar validation requires a non-empty transaction chain");
    let base_version = first.read_version;
    let lance_head = observation.version;
    if lance_head < base_version {
        return Ok(BranchMergeMultiCommitProof::unverifiable(format!(
            "table '{table_key}' HEAD {lance_head} is behind its planned transaction base {base_version}"
        )));
    }
    if lance_head == base_version {
        return Ok(BranchMergeMultiCommitProof {
            effect_ownership: EffectOwnership::None,
            full_effect_at_head: false,
            unsafe_reason: None,
        });
    }
    if lance_head.saturating_sub(base_version) > MAX_EFFECT_IDENTITY_SCAN_VERSIONS {
        return Ok(BranchMergeMultiCommitProof::unverifiable(format!(
            "table '{table_key}' requires scanning {} transaction versions, above the recovery bound {}",
            lance_head.saturating_sub(base_version),
            MAX_EFFECT_IDENTITY_SCAN_VERSIONS
        )));
    }

    let mut planned_index = 0usize;
    for version in base_version + 1..=lance_head {
        let transaction = if version == lance_head {
            observation
                .dataset
                .read_transaction()
                .await
                .map_err(|error| OmniError::Lance(error.to_string()))?
        } else {
            observation
                .dataset
                .read_transaction_by_version(version)
                .await
                .map_err(|error| OmniError::Lance(error.to_string()))?
        };
        let Some(transaction) = transaction else {
            return Ok(BranchMergeMultiCommitProof::unverifiable(format!(
                "table '{table_key}' has no readable transaction identity at version {version}"
            )));
        };

        // Rollback can interrupt a proper prefix of the logical chain, not
        // only the complete chain. Its final Restore is attributable after at
        // least one exact planned transaction has established ownership.
        if version == lance_head
            && planned_index > 0
            && matches!(
                &transaction.operation,
                lance::dataset::transaction::Operation::Restore { version }
                    if *version == manifest_pinned
            )
        {
            return Ok(BranchMergeMultiCommitProof {
                effect_ownership: EffectOwnership::OwnCompensatedAtHead,
                full_effect_at_head: false,
                unsafe_reason: None,
            });
        }

        let observed_identity = StagedTransactionIdentity::from(&transaction);
        if let Some(planned) = planned_transactions.get(planned_index) {
            let expected_output_version = planned.read_version.checked_add(1).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "BranchMerge transaction version overflow for table '{table_key}'"
                ))
            })?;
            if version != expected_output_version || &observed_identity != planned {
                return Ok(BranchMergeMultiCommitProof::unverifiable(format!(
                    "table '{table_key}' transaction at version {version} is {observed_identity:?}, expected exact planned identity {planned:?} at version {expected_output_version}"
                )));
            }
            planned_index += 1;
            continue;
        }

        if !matches!(
            &transaction.operation,
            lance::dataset::transaction::Operation::CreateIndex { .. }
        ) {
            return Ok(BranchMergeMultiCommitProof::unverifiable(format!(
                "table '{table_key}' has non-derived operation {:?} at version {version} after its complete planned transaction chain",
                transaction.operation
            )));
        }
    }

    let full_effect_at_head = planned_index == planned_transactions.len();
    Ok(BranchMergeMultiCommitProof {
        effect_ownership: if planned_index > 0 {
            EffectOwnership::OwnAtHead
        } else {
            EffectOwnership::None
        },
        full_effect_at_head,
        unsafe_reason: None,
    })
}

async fn process_branch_merge_sidecar_v4(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    sidecar: &RecoverySidecar,
    mode: RecoveryMode,
) -> Result<bool> {
    if let Some(outcome) = detect_visible_v4_outcome(root_uri, sidecar).await? {
        return finalize_visible_v4_outcome(root_uri, storage.as_ref(), sidecar, outcome).await;
    }
    let protocol = sidecar
        .protocol_v4
        .as_ref()
        .expect("caller checked protocol_v4");
    let mut states = Vec::with_capacity(sidecar.tables.len());
    let mut any_physical_effect = false;
    let mut any_head_movement = false;
    let mut confirmed_mismatch: Option<String> = None;
    let mut unsafe_observation: Option<String> = None;
    let mut all_confirmed_effects_at_head =
        protocol.effect_phase == RecoveryEffectPhase::EffectsConfirmed;

    for pin in &sidecar.tables {
        let effect = protocol
            .effects
            .iter()
            .find(|effect| effect.table_key == pin.table_key)
            .expect("v4 sidecar shape validates effect/pin key sets");
        let manifest_pinned = snapshot
            .entry(&pin.table_key)
            .map(|entry| entry.table_version)
            .unwrap_or(0);
        let unpublished_fork = pin
            .table_branch
            .as_deref()
            .is_some_and(|branch| branch != "main")
            && snapshot
                .entry(&pin.table_key)
                .map(|entry| entry.table_branch != pin.table_branch)
                .unwrap_or(true);
        if manifest_pinned != pin.expected_version {
            unsafe_observation.get_or_insert_with(|| {
                format!(
                    "table '{}' manifest pin changed from {} to {} while its merge effect remained pending",
                    pin.table_key, pin.expected_version, manifest_pinned
                )
            });
        }

        let observed = observe_branch_merge_target_ref(pin).await?;
        let first_touch_version = effect.kind.source_fork_version();
        if observed.is_none() && first_touch_version.is_none() {
            unsafe_observation.get_or_insert_with(|| {
                format!(
                    "existing target ref for table '{}' disappeared while BranchMerge recovery was pending",
                    pin.table_key
                )
            });
        }
        if let (Some(observed), Some(source_version)) = (&observed, first_touch_version) {
            any_physical_effect = true;
            if observed.parent_version != Some(source_version) {
                unsafe_observation.get_or_insert_with(|| {
                    format!(
                        "first-touch target ref for table '{}' was forked at {:?}, expected exact source version {}",
                        pin.table_key, observed.parent_version, source_version
                    )
                });
            }
            if let Some(expected_identifier) = effect.kind.confirmed_branch_identifier()
                && &observed.branch_identifier != expected_identifier
            {
                unsafe_observation.get_or_insert_with(|| {
                    format!(
                        "first-touch target ref identity for table '{}' differs from its confirmed merge effect",
                        pin.table_key
                    )
                });
            }
        }

        let (classification, lance_head, effect_ownership) = match &effect.kind {
            RecoveryBranchMergeEffectKind::MultiCommitHead {
                planned_transactions,
                confirmed_version,
                ..
            } => {
                let lance_head = observed
                    .as_ref()
                    .map(|observation| observation.version)
                    .unwrap_or(manifest_pinned);
                if lance_head < pin.expected_version {
                    unsafe_observation.get_or_insert_with(|| {
                        format!(
                            "table '{}' HEAD {} is behind prepared merge pin {}",
                            pin.table_key, lance_head, pin.expected_version
                        )
                    });
                }
                let moved = lance_head > pin.expected_version;
                any_physical_effect |= moved;
                any_head_movement |= moved;
                let proof = if let Some(observation) = observed.as_ref() {
                    prove_branch_merge_multi_commit_effect(
                        observation,
                        planned_transactions,
                        manifest_pinned,
                        &pin.table_key,
                    )
                    .await?
                } else {
                    BranchMergeMultiCommitProof {
                        effect_ownership: EffectOwnership::None,
                        full_effect_at_head: false,
                        unsafe_reason: None,
                    }
                };
                if let Some(reason) = proof.unsafe_reason.as_ref() {
                    unsafe_observation.get_or_insert_with(|| reason.clone());
                }
                let complete = protocol.effect_phase == RecoveryEffectPhase::EffectsConfirmed
                    && proof.full_effect_at_head
                    && confirmed_version == &Some(lance_head)
                    && pin.confirmed_version == Some(lance_head);
                all_confirmed_effects_at_head &= complete;
                if protocol.effect_phase == RecoveryEffectPhase::EffectsConfirmed
                    && !complete
                    && proof.effect_ownership != EffectOwnership::OwnCompensatedAtHead
                {
                    confirmed_mismatch.get_or_insert_with(|| {
                        format!(
                            "confirmed multi-commit effect '{}' expected HEAD {:?}, observed {}",
                            pin.table_key, confirmed_version, lance_head
                        )
                    });
                }
                let classification = if complete {
                    TableClassification::RolledPastExpected
                } else if moved {
                    TableClassification::IncompletePhaseB
                } else {
                    TableClassification::NoMovement
                };
                (classification, lance_head, proof.effect_ownership)
            }
            RecoveryBranchMergeEffectKind::RefOnlyFork { source_version, .. } => {
                let complete = protocol.effect_phase == RecoveryEffectPhase::EffectsConfirmed
                    && observed
                        .as_ref()
                        .is_some_and(|observation| observation.version == *source_version)
                    && pin.confirmed_version == Some(*source_version);
                all_confirmed_effects_at_head &= complete;
                if protocol.effect_phase == RecoveryEffectPhase::EffectsConfirmed && !complete {
                    confirmed_mismatch.get_or_insert_with(|| {
                        format!(
                            "confirmed ref-only effect '{}' expected exact fork version {}, observed {:?}",
                            pin.table_key,
                            source_version,
                            observed.as_ref().map(|observation| observation.version)
                        )
                    });
                }
                (
                    if complete {
                        TableClassification::RolledPastExpected
                    } else if observed.is_some() {
                        TableClassification::IncompletePhaseB
                    } else {
                        TableClassification::NoMovement
                    },
                    // A ref-only fork never represents a data-HEAD delta. Keep
                    // rollback on the explicit ref-cleanup path rather than
                    // presenting the source version as a restore candidate.
                    manifest_pinned,
                    EffectOwnership::None,
                )
            }
        };
        states.push(ClassifiedTable {
            classification,
            manifest_pinned,
            lance_head,
            effect_ownership,
            unpublished_fork,
        });
    }

    if let Some(reason) = unsafe_observation {
        let message = format!(
            "BranchMerge recovery sidecar '{}' observed foreign or unverifiable physical state: {}",
            sidecar.operation_id, reason
        );
        return match mode {
            RecoveryMode::RollForwardOnly => {
                warn!(operation_id = sidecar.operation_id.as_str(), "{message}");
                Ok(false)
            }
            RecoveryMode::Full => Err(OmniError::manifest_internal(message)),
        };
    }
    if let Some(reason) = confirmed_mismatch {
        let message = format!(
            "BranchMerge recovery sidecar '{}' no longer matches its exact confirmed effects: {}",
            sidecar.operation_id, reason
        );
        return match mode {
            RecoveryMode::RollForwardOnly => {
                warn!(operation_id = sidecar.operation_id.as_str(), "{message}");
                Ok(false)
            }
            RecoveryMode::Full => Err(OmniError::manifest_internal(message)),
        };
    }

    let live_authority =
        read_live_recovery_authority(root_uri, storage, sidecar.branch.as_deref()).await?;
    let branch_recreated = live_authority.branch_identifier != protocol.authority.branch_identifier;
    let authority_changed = live_authority != protocol.authority;
    if branch_recreated && any_physical_effect {
        let message = format!(
            "BranchMerge recovery sidecar '{}' targets a branch incarnation that was deleted and recreated; refusing to act through the reused name",
            sidecar.operation_id
        );
        return match mode {
            RecoveryMode::RollForwardOnly => {
                warn!(operation_id = sidecar.operation_id.as_str(), "{message}");
                Ok(false)
            }
            RecoveryMode::Full => Err(OmniError::manifest_internal(message)),
        };
    }

    if !authority_changed && all_confirmed_effects_at_head {
        return roll_forward_branch_merge_v4(root_uri, storage, sidecar, mode).await;
    }

    // A first-touch ref with no data commit is a recoverable physical artifact,
    // but cleaning it restores the exact pre-attempt graph state. Do not
    // manufacture a rollback graph commit: doing so would advance the target
    // lineage and turn a later logical fast-forward into a three-way merge.
    if !any_head_movement {
        if matches!(mode, RecoveryMode::RollForwardOnly) {
            warn!(
                operation_id = sidecar.operation_id.as_str(),
                "recovery: deferring armed BranchMerge intent with no proven physical effects"
            );
            return Ok(false);
        }
        if let NoEffectForkCleanup::DeferredPathChild { .. } =
            cleanup_unpublished_no_effect_forks(root_uri, storage.as_ref(), sidecar, &states)
                .await?
        {
            return Ok(false);
        }
        delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &sidecar.operation_id).await?;
        return Ok(true);
    }

    if matches!(mode, RecoveryMode::RollForwardOnly) {
        warn!(
            operation_id = sidecar.operation_id.as_str(),
            authority_changed, "recovery: deferring rollback-eligible BranchMerge sidecar"
        );
        return Ok(false);
    }
    roll_back_sidecar(root_uri, storage.as_ref(), sidecar, &states).await?;
    Ok(true)
}

async fn roll_forward_branch_merge_v4(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    sidecar: &RecoverySidecar,
    mode: RecoveryMode,
) -> Result<bool> {
    let protocol = sidecar
        .protocol_v4
        .as_ref()
        .expect("caller checked protocol_v4");
    let mut updates = Vec::with_capacity(protocol.intended_delta.table_updates.len());
    let mut expected = HashMap::with_capacity(protocol.intended_delta.table_updates.len());
    let mut outcomes = Vec::with_capacity(protocol.intended_delta.table_updates.len());
    for slot in &protocol.intended_delta.table_updates {
        let confirmed = slot.confirmed.as_ref().ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "BranchMerge recovery sidecar '{}' delta slot '{}' is not confirmed",
                sidecar.operation_id, slot.table_key
            ))
        })?;
        expected.insert(slot.table_key.clone(), slot.expected_version);
        updates.push(ManifestChange::Update(SubTableUpdate {
            table_key: slot.table_key.clone(),
            table_version: confirmed.table_version,
            table_branch: confirmed.table_branch.clone(),
            row_count: confirmed.row_count,
            version_metadata: confirmed.version_metadata.clone(),
        }));
        outcomes.push(TableOutcome {
            table_key: slot.table_key.clone(),
            from_version: slot.expected_version,
            to_version: confirmed.table_version,
        });
    }

    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH)?;
    let graph_commit_id = match publish_recovery_commit(
        root_uri,
        sidecar,
        RecoveryKind::RolledForward,
        &updates,
        &expected,
    )
    .await
    {
        Ok((_, graph_commit_id)) => graph_commit_id,
        Err(error) if error.is_read_set_changed() => {
            if let Some(outcome) = detect_visible_v4_outcome(root_uri, sidecar).await? {
                return finalize_visible_v4_outcome(root_uri, storage.as_ref(), sidecar, outcome)
                    .await;
            }
            return match mode {
                RecoveryMode::RollForwardOnly => Ok(false),
                RecoveryMode::Full => Err(error),
            };
        }
        Err(error) => return Err(error),
    };
    record_audit(
        root_uri,
        sidecar,
        graph_commit_id,
        RecoveryKind::RolledForward,
        outcomes,
    )
    .await?;
    delete_sidecar_by_operation_id(root_uri, storage.as_ref(), &sidecar.operation_id).await?;
    Ok(true)
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

/// Read the complete coarse authority token from current storage state. Called
/// only for a pending v3 sidecar after fixed outcome ids were proven absent.
async fn read_live_recovery_authority(
    root_uri: &str,
    storage: &std::sync::Arc<dyn StorageAdapter>,
    branch: Option<&str>,
) -> Result<RecoveryAuthorityToken> {
    let coordinator = match branch {
        Some(branch) if branch != "main" => {
            GraphCoordinator::open_branch(root_uri, branch, std::sync::Arc::clone(storage)).await?
        }
        _ => GraphCoordinator::open(root_uri, std::sync::Arc::clone(storage)).await?,
    };
    let schema_state =
        crate::db::schema_state::validate_schema_contract(root_uri, std::sync::Arc::clone(storage))
            .await?;
    Ok(RecoveryAuthorityToken {
        branch_identifier: coordinator.branch_identifier().await?,
        graph_head: coordinator.exact_graph_head(),
        schema_ir_hash: schema_state.schema_ir_hash,
        schema_identity_version: schema_state.schema_identity_version,
    })
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
    // one recovery event.
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
    // RFC-013 Phase 7: the winning legacy recovery folded its commit into the
    // manifest CAS, so the converge audit references THAT commit. (v3 fixed-id
    // outcomes are detected and finalized before this fallback.) We lost the
    // CAS and never minted it, but a legacy recovery commit is distinguishable
    // by `RECOVERY_ACTOR` authorship, so the latest recovery-actored commit on
    // this branch is it. Do NOT use the branch head:
    // a concurrent USER write can advance `graph_head` past the recovery commit
    // between the winner's publish and this read, which would attribute the audit
    // row to the wrong (later, user) commit. (We only reach here with the sidecar
    // still on disk: the winner advanced the manifest but crashed before its own
    // audit+delete, so we finish its bookkeeping.)
    let cache = match sidecar.branch.as_deref() {
        Some(branch) => {
            crate::db::commit_graph::CommitGraph::open_at_branch(root_uri, branch).await?
        }
        None => crate::db::commit_graph::CommitGraph::open(root_uri).await?,
    };
    let converged_commit_id = match cache
        .load_commits()
        .await?
        .into_iter()
        .rfind(|c| c.actor_id.as_deref() == Some(RECOVERY_ACTOR))
    {
        Some(recovery_commit) => recovery_commit.graph_commit_id,
        // No recovery commit visible — unexpected on this path (the winner just
        // published one); fall back to the head rather than an empty id.
        None => cache.head_commit_id().await?.unwrap_or_default(),
    };
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
    effect_ownership: EffectOwnership,
    /// The branch snapshot still inherits this table from another Lance ref,
    /// while the sidecar's final pin names a first-touch target ref. This is the
    /// durable distinction between an unpublished fork and an already-owned
    /// table on the branch.
    unpublished_fork: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectOwnership {
    None,
    OwnAtHead,
    OwnBeforeHead,
    /// A previous Full recovery restored this owned effect to the manifest pin
    /// but crashed before publishing the restored HEAD. The compensation is
    /// complete and recovery should publish it, not restore again or wedge.
    OwnCompensatedAtHead,
    Unverifiable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NoEffectForkCleanup {
    Complete,
    DeferredPathChild {
        table_path: String,
        target_branch: String,
        path_child: String,
    },
}

fn has_exact_protocol(sidecar: &RecoverySidecar) -> bool {
    sidecar.protocol_v3.is_some() || sidecar.protocol_v4.is_some()
}

fn v4_effect_for<'a>(
    sidecar: &'a RecoverySidecar,
    table_key: &str,
) -> Option<&'a RecoveryBranchMergeEffect> {
    sidecar
        .protocol_v4
        .as_ref()?
        .effects
        .iter()
        .find(|effect| effect.table_key == table_key)
}

fn first_touch_fork_version(sidecar: &RecoverySidecar, pin: &SidecarTablePin) -> u64 {
    v4_effect_for(sidecar, &pin.table_key)
        .and_then(|effect| effect.kind.source_fork_version())
        .unwrap_or(pin.expected_version)
}

/// Remove first-touch named-branch refs created by an Armed exact-protocol
/// attempt that never completed this table's planned effect.
///
/// The sidecar is durable before the ref is created, so it is the ownership
/// record while the manifest still inherits the table from another branch.
/// Destruction is deliberately narrow: the manifest must not select the ref,
/// no other pending sidecar may claim the same `(table_path, branch)`, and the
/// live ref must still be exactly the fork point. Full recovery is quiesced, so
/// this fresh re-check closes ordinary crash/retry races. Lance does not expose
/// a compare-and-delete-by-branch-identifier primitive; callers must retain the
/// Full-sweep quiescence guarantee rather than reusing this helper in a live
/// reconciler.
async fn cleanup_unpublished_no_effect_forks(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
) -> Result<NoEffectForkCleanup> {
    if !has_exact_protocol(sidecar) {
        return Ok(NoEffectForkCleanup::Complete);
    }

    let all_sidecars = list_sidecars(root_uri, storage).await?;
    for (pin, state) in sidecar.tables.iter().zip(states.iter()) {
        if !state.unpublished_fork || state.effect_ownership != EffectOwnership::None {
            continue;
        }
        let Some(target_branch) = pin
            .table_branch
            .as_deref()
            .filter(|branch| *branch != "main")
        else {
            continue;
        };

        let has_competing_claim = all_sidecars.iter().any(|candidate| {
            candidate.operation_id != sidecar.operation_id
                && candidate.tables.iter().any(|candidate_pin| {
                    candidate_pin.table_path == pin.table_path
                        && candidate_pin.table_branch.as_deref() == Some(target_branch)
                })
        });
        if has_competing_claim {
            // Never delete a ref while another pending intent claims it. Full
            // recovery is quiesced by the shared gates, so this no-effect
            // sidecar can safely discard itself without touching the ref. A
            // later/last claimant either cleans the still-untouched fork or
            // recovers its owned effect. RollForwardOnly never calls this
            // helper and continues to defer the unresolved ownership.
            continue;
        }

        let mut dataset = crate::instrumentation::open_dataset(
            &pin.table_path,
            crate::instrumentation::VersionResolution::Latest,
            None,
            crate::instrumentation::table_wrapper(),
        )
        .await?;
        let branches = dataset
            .list_branches()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if let Some(child) = crate::branch_control::path_descendant(&branches, target_branch) {
            // Lance cannot reclaim an ancestor tree while a slash-separated
            // path-child remains. Old stores could admit that namespace shape.
            // Keep the ownership sidecar and let open complete so the operator
            // can delete the child branch first; the next Full sweep then
            // rechecks authority and reclaims the ancestor fork.
            warn!(
                operation_id = sidecar.operation_id.as_str(),
                table_path = pin.table_path.as_str(),
                branch = target_branch,
                path_child = child,
                "recovery: deferring unpublished fork cleanup for legacy path overlap"
            );
            return Ok(NoEffectForkCleanup::DeferredPathChild {
                table_path: pin.table_path.clone(),
                target_branch: target_branch.to_string(),
                path_child: child.to_string(),
            });
        }
        let Some(contents) = branches.get(target_branch) else {
            // BranchContents is authoritative, but Lance create writes the
            // shallow-cloned target dataset first. The absent-ref state is
            // therefore either crash-before-fork (idempotent no-op) or an
            // exact clone-only zombie owned by this already-armed sidecar.
            // Reclaim both through Lance's force API before retiring intent;
            // merely skipping here leaves the zombie blocking every retry.
            if !crate::branch_control::reclaim_ref_absent_tree(&mut dataset, target_branch).await? {
                return Err(OmniError::manifest_conflict(format!(
                    "target ref '{target_branch}' appeared during no-effect recovery; refusing \
                     to retire its ownership intent"
                )));
            }
            continue;
        };
        let exact_fork_version = first_touch_fork_version(sidecar, pin);
        if contents.parent_version != exact_fork_version {
            return Err(OmniError::manifest_internal(format!(
                "OCC recovery sidecar '{}' cannot discard unpublished fork '{}:{}': \
                 parent version is {}, expected exact fork point {}",
                sidecar.operation_id,
                pin.table_path,
                target_branch,
                contents.parent_version,
                exact_fork_version
            )));
        }
        if let Some(expected_identifier) = v4_effect_for(sidecar, &pin.table_key)
            .and_then(|effect| effect.kind.confirmed_branch_identifier())
            && &contents.identifier != expected_identifier
        {
            return Err(OmniError::manifest_internal(format!(
                "BranchMerge recovery sidecar '{}' cannot discard unpublished fork '{}:{}': \
                 live target ref identity differs from the confirmed effect",
                sidecar.operation_id, pin.table_path, target_branch
            )));
        }
        let target = dataset
            .checkout_branch(target_branch)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if target.version().version != exact_fork_version {
            return Err(OmniError::manifest_internal(format!(
                "OCC recovery sidecar '{}' cannot discard unpublished fork '{}:{}': \
                 live HEAD is {}, expected untouched version {}",
                sidecar.operation_id,
                pin.table_path,
                target_branch,
                target.version().version,
                exact_fork_version
            )));
        }
        dataset
            .force_delete_branch(target_branch)
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?;
    }
    Ok(NoEffectForkCleanup::Complete)
}

fn table_requires_rollback_effect(state: &ClassifiedTable) -> bool {
    matches!(
        state.classification,
        TableClassification::RolledPastExpected
            | TableClassification::UnexpectedAtP1
            | TableClassification::UnexpectedMultistep
            | TableClassification::UnexpectedEffectIdentity
            | TableClassification::IncompletePhaseB
    )
}

fn exact_rollback_audit_outcomes(
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
) -> Vec<TableOutcome> {
    sidecar
        .tables
        .iter()
        .zip(states.iter())
        .filter(|(_, state)| {
            matches!(
                state.effect_ownership,
                EffectOwnership::OwnAtHead | EffectOwnership::OwnCompensatedAtHead
            ) && table_requires_rollback_effect(state)
        })
        .map(|(pin, state)| TableOutcome {
            table_key: pin.table_key.clone(),
            from_version: state.lance_head,
            to_version: state.manifest_pinned,
        })
        .collect()
}

/// Durably bind the exact audit payload for a v3/v4 rollback before the first
/// restore or rollback publish. A retry must replay the original observation,
/// not reconstruct it from post-restore HEADs. In particular, partial
/// multi-table attempts omit untouched pins, and a preflight-rebased v3 effect
/// records its actual `own_head -> current_manifest_pin` compensation.
async fn prepare_exact_rollback_audit_plan(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
) -> Result<RecoverySidecar> {
    let mut prepared = sidecar.clone();
    let already_prepared = prepared
        .protocol_v3
        .as_ref()
        .and_then(|protocol| protocol.rollback_audit_outcomes.as_ref())
        .or_else(|| {
            prepared
                .protocol_v4
                .as_ref()
                .and_then(|protocol| protocol.rollback_audit_outcomes.as_ref())
        })
        .is_some();
    if already_prepared {
        return Ok(prepared);
    }

    let outcomes = exact_rollback_audit_outcomes(sidecar, states);
    if let Some(protocol) = prepared.protocol_v3.as_mut() {
        protocol.rollback_audit_outcomes = Some(outcomes);
    } else if let Some(protocol) = prepared.protocol_v4.as_mut() {
        protocol.rollback_audit_outcomes = Some(outcomes);
    } else {
        return Err(OmniError::manifest_internal(
            "prepare_exact_rollback_audit_plan called for a legacy sidecar",
        ));
    }
    let uri = sidecar_uri(root_uri, &prepared.operation_id);
    validate_sidecar_shape(&uri, &prepared)?;
    let json = serde_json::to_string_pretty(&prepared).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to serialize exact rollback audit plan for sidecar '{}': {}",
            prepared.operation_id, error
        ))
    })?;
    // `write_text` replaces one complete object atomically (temp+rename for
    // local storage, PutObject for object stores), the same durability contract
    // as arming and Phase-B confirmation.
    storage.write_text(&uri, &json).await?;
    Ok(prepared)
}

async fn roll_back_sidecar(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
) -> Result<()> {
    // An Armed multi-table attempt can create every first-touch ref and then
    // land effects on only a subset. No-effect refs are not selected by the
    // rollback manifest publish, so they must be removed BEFORE that publish.
    // If recovery crashed after publishing first, the fixed rollback outcome
    // would make the next pass finalize/delete the sidecar without ever seeing
    // the still-orphaned refs. A rollback that owns any physical effect may not
    // defer and let read-write open succeed: legacy writers are not all enrolled
    // in the v3 preparation barrier. Fail closed until the path child is removed.
    if let NoEffectForkCleanup::DeferredPathChild {
        table_path,
        target_branch,
        path_child,
    } = cleanup_unpublished_no_effect_forks(root_uri, storage, sidecar, states).await?
    {
        return Err(OmniError::manifest_internal(format!(
            "OCC recovery sidecar '{}' owns physical effects but cannot clean unpublished fork \
             '{}:{}' while legacy path-child '{}' is live; refusing read-write open; delete the \
             child branch leaf-first using an existing handle or an offline Lance-level branch \
             tool, then reopen",
            sidecar.operation_id, table_path, target_branch, path_child
        )));
    }

    // Once the fixed rollback commit is visible, early recovery finalization no
    // longer has pre-restore table observations. Persist the exact audit plan
    // after fork cleanup and before the first restore so that path can replay it
    // without fabricating outcomes from pins.
    let prepared_exact = if has_exact_protocol(sidecar) {
        Some(prepare_exact_rollback_audit_plan(root_uri, storage, sidecar, states).await?)
    } else {
        None
    };
    let sidecar = prepared_exact.as_ref().unwrap_or(sidecar);

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
        if has_exact_protocol(sidecar)
            && !matches!(
                state.effect_ownership,
                EffectOwnership::OwnAtHead | EffectOwnership::OwnCompensatedAtHead
            )
        {
            // v3 compensation is ownership-exact. Never restore a table whose
            // current HEAD is foreign (or whose owned effect is buried beneath
            // another writer); callers reject the latter before reaching here.
            continue;
        }
        if table_requires_rollback_effect(state) {
            if state.effect_ownership != EffectOwnership::OwnCompensatedAtHead {
                restore_table_to_version(
                    &pin.table_path,
                    pin.table_branch.as_deref(),
                    state.manifest_pinned,
                )
                .await?;
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::RECOVERY_POST_TABLE_RESTORE_PRE_PUBLISH,
                )?;
            }
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
    let (_manifest_version, graph_commit_id) = publish_recovery_commit(
        root_uri,
        sidecar,
        RecoveryKind::RolledBack,
        &updates,
        &expected,
    )
    .await?;
    crate::failpoints::maybe_fail(
        crate::failpoints::names::RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT,
    )?;
    let outcomes = sidecar
        .protocol_v3
        .as_ref()
        .and_then(|protocol| protocol.rollback_audit_outcomes.clone())
        .or_else(|| {
            sidecar
                .protocol_v4
                .as_ref()
                .and_then(|protocol| protocol.rollback_audit_outcomes.clone())
        })
        .unwrap_or(outcomes);
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
    sidecar: &RecoverySidecar,
    states: &[ClassifiedTable],
) -> Result<()> {
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

/// Finalize an exact-protocol sidecar whose tables are already manifest-aligned.
///
/// Numeric table alignment alone is not evidence of WHICH outcome happened:
/// the original manifest CAS may have succeeded before sidecar deletion, or a
/// prior full recovery may have rolled the effects back and then failed during
/// audit/delete. v3/v4 give both outcomes fixed graph-commit ids, so inspect the
/// authoritative lineage and finish exactly that outcome without publishing a
/// second synthetic commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VisibleExactOutcome {
    Original,
    RolledBack,
}

async fn detect_visible_v3_outcome(
    root_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<Option<VisibleExactOutcome>> {
    let protocol = sidecar
        .protocol_v3
        .as_ref()
        .expect("caller checked protocol_v3");
    let (commits, _) =
        ManifestCoordinator::read_graph_lineage_at(root_uri, sidecar.branch.as_deref()).await?;
    let original_commit = commits
        .iter()
        .find(|commit| commit.graph_commit_id == protocol.lineage.graph_commit_id);
    let rollback_visible = commits
        .iter()
        .any(|commit| commit.graph_commit_id == protocol.rollback_graph_commit_id);
    let original_visible = if let Some(commit) = original_commit {
        let expected_branch = protocol
            .lineage
            .branch
            .as_deref()
            .filter(|branch| *branch != "main");
        if commit.manifest_branch.as_deref() != expected_branch
            || protocol
                .authority
                .graph_head
                .as_ref()
                .is_some_and(|head| commit.parent_commit_id.as_ref() != Some(head))
            || commit.merged_parent_commit_id != protocol.lineage.merged_parent_commit_id
            || commit.actor_id != protocol.lineage.actor_id
            || commit.created_at != protocol.lineage.created_at
        {
            return Err(OmniError::manifest_internal(format!(
                "OCC recovery sidecar '{}' found original commit id '{}' with mismatched lineage",
                sidecar.operation_id, protocol.lineage.graph_commit_id
            )));
        }
        let committed_snapshot = ManifestCoordinator::snapshot_at(
            root_uri,
            sidecar.branch.as_deref(),
            commit.manifest_version,
        )
        .await?;
        let delta_matches = protocol.intended_delta.table_updates.iter().all(|slot| {
            let Some(confirmed) = slot.confirmed.as_ref() else {
                return false;
            };
            committed_snapshot
                .entry(&slot.table_key)
                .is_some_and(|entry| {
                    entry.table_version == confirmed.table_version
                        && entry.table_branch == confirmed.table_branch
                        && entry.row_count == confirmed.row_count
                        && entry.version_metadata == confirmed.version_metadata
                })
        });
        if !delta_matches {
            return Err(OmniError::manifest_internal(format!(
                "OCC recovery sidecar '{}' found original commit id '{}' but its manifest delta differs",
                sidecar.operation_id, protocol.lineage.graph_commit_id
            )));
        }
        true
    } else {
        false
    };

    match (original_visible, rollback_visible) {
        (true, false) => Ok(Some(VisibleExactOutcome::Original)),
        (false, true) => Ok(Some(VisibleExactOutcome::RolledBack)),
        (false, false) => Ok(None),
        (true, true) => Err(OmniError::manifest_internal(format!(
            "OCC recovery sidecar '{}' has both original commit '{}' and rollback commit '{}' \
             visible; refusing ambiguous finalization",
            sidecar.operation_id,
            protocol.lineage.graph_commit_id,
            protocol.rollback_graph_commit_id
        ))),
    }
}

async fn finalize_visible_v3_outcome(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    outcome: VisibleExactOutcome,
) -> Result<bool> {
    let protocol = sidecar
        .protocol_v3
        .as_ref()
        .expect("caller checked protocol_v3");

    let (kind, graph_commit_id, outcomes) = match outcome {
        VisibleExactOutcome::Original => {
            let outcomes = sidecar
                .tables
                .iter()
                .map(|pin| {
                    let confirmed = protocol
                        .intended_delta
                        .table_updates
                        .iter()
                        .find(|slot| slot.table_key == pin.table_key)
                        .and_then(|slot| slot.confirmed.as_ref())
                        .expect("validated EffectsConfirmed v3 sidecar");
                    TableOutcome {
                        table_key: pin.table_key.clone(),
                        from_version: pin.expected_version,
                        to_version: confirmed.table_version,
                    }
                })
                .collect();
            (
                RecoveryKind::RolledForward,
                protocol.lineage.graph_commit_id.clone(),
                outcomes,
            )
        }
        VisibleExactOutcome::RolledBack => {
            let outcomes = protocol.rollback_audit_outcomes.clone().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "OCC recovery sidecar '{}' has visible fixed rollback commit '{}' but no \
                     durable rollback audit outcomes; refusing to infer them from table pins",
                    sidecar.operation_id, protocol.rollback_graph_commit_id
                ))
            })?;
            (
                RecoveryKind::RolledBack,
                protocol.rollback_graph_commit_id.clone(),
                outcomes,
            )
        }
    };

    // Idempotent across an audit-success / sidecar-delete failure. The fixed
    // outcome id is already durable in __manifest; the only remaining work is
    // to ensure one audit row exists and delete the artifact.
    let mut audit = RecoveryAudit::open(root_uri).await?;
    let already_recorded =
        audit.list().await?.iter().any(|record| {
            record.operation_id == sidecar.operation_id && record.recovery_kind == kind
        });
    if !already_recorded {
        crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_RECORD_AUDIT)?;
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
    }
    delete_sidecar_by_operation_id(root_uri, storage, &sidecar.operation_id).await?;
    Ok(true)
}

async fn detect_visible_v4_outcome(
    root_uri: &str,
    sidecar: &RecoverySidecar,
) -> Result<Option<VisibleExactOutcome>> {
    let protocol = sidecar
        .protocol_v4
        .as_ref()
        .expect("caller checked protocol_v4");
    let (commits, _) =
        ManifestCoordinator::read_graph_lineage_at(root_uri, sidecar.branch.as_deref()).await?;
    let original_commit = commits
        .iter()
        .find(|commit| commit.graph_commit_id == protocol.lineage.graph_commit_id);
    let rollback_visible = commits
        .iter()
        .any(|commit| commit.graph_commit_id == protocol.rollback_graph_commit_id);
    let original_visible = if let Some(commit) = original_commit {
        let expected_branch = protocol
            .lineage
            .branch
            .as_deref()
            .filter(|branch| *branch != "main");
        if commit.manifest_branch.as_deref() != expected_branch
            || protocol
                .authority
                .graph_head
                .as_ref()
                .is_some_and(|head| commit.parent_commit_id.as_ref() != Some(head))
            || commit.merged_parent_commit_id != protocol.lineage.merged_parent_commit_id
            || commit.actor_id != protocol.lineage.actor_id
            || commit.created_at != protocol.lineage.created_at
        {
            return Err(OmniError::manifest_internal(format!(
                "BranchMerge recovery sidecar '{}' found original commit id '{}' with mismatched lineage",
                sidecar.operation_id, protocol.lineage.graph_commit_id
            )));
        }
        let committed_snapshot = ManifestCoordinator::snapshot_at(
            root_uri,
            sidecar.branch.as_deref(),
            commit.manifest_version,
        )
        .await?;
        let delta_matches = protocol.intended_delta.table_updates.iter().all(|slot| {
            let Some(confirmed) = slot.confirmed.as_ref() else {
                return false;
            };
            committed_snapshot
                .entry(&slot.table_key)
                .is_some_and(|entry| {
                    entry.table_version == confirmed.table_version
                        && entry.table_branch == confirmed.table_branch
                        && entry.row_count == confirmed.row_count
                        && entry.version_metadata == confirmed.version_metadata
                })
        });
        if !delta_matches {
            return Err(OmniError::manifest_internal(format!(
                "BranchMerge recovery sidecar '{}' found original commit id '{}' but its exact manifest delta differs",
                sidecar.operation_id, protocol.lineage.graph_commit_id
            )));
        }
        true
    } else {
        false
    };

    match (original_visible, rollback_visible) {
        (true, false) => Ok(Some(VisibleExactOutcome::Original)),
        (false, true) => Ok(Some(VisibleExactOutcome::RolledBack)),
        (false, false) => Ok(None),
        (true, true) => Err(OmniError::manifest_internal(format!(
            "BranchMerge recovery sidecar '{}' has both original commit '{}' and rollback commit '{}' visible; refusing ambiguous finalization",
            sidecar.operation_id,
            protocol.lineage.graph_commit_id,
            protocol.rollback_graph_commit_id
        ))),
    }
}

async fn finalize_visible_v4_outcome(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &RecoverySidecar,
    outcome: VisibleExactOutcome,
) -> Result<bool> {
    let protocol = sidecar
        .protocol_v4
        .as_ref()
        .expect("caller checked protocol_v4");
    let (kind, graph_commit_id, outcomes) = match outcome {
        VisibleExactOutcome::Original => {
            let outcomes = protocol
                .intended_delta
                .table_updates
                .iter()
                .map(|slot| {
                    let confirmed = slot
                        .confirmed
                        .as_ref()
                        .expect("visible v4 original requires confirmed delta");
                    TableOutcome {
                        table_key: slot.table_key.clone(),
                        from_version: slot.expected_version,
                        to_version: confirmed.table_version,
                    }
                })
                .collect();
            (
                RecoveryKind::RolledForward,
                protocol.lineage.graph_commit_id.clone(),
                outcomes,
            )
        }
        VisibleExactOutcome::RolledBack => {
            let outcomes = protocol.rollback_audit_outcomes.clone().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "BranchMerge recovery sidecar '{}' has visible fixed rollback commit '{}' but no durable rollback audit outcomes",
                    sidecar.operation_id, protocol.rollback_graph_commit_id
                ))
            })?;
            (
                RecoveryKind::RolledBack,
                protocol.rollback_graph_commit_id.clone(),
                outcomes,
            )
        }
    };

    let mut audit = RecoveryAudit::open(root_uri).await?;
    let already_recorded =
        audit.list().await?.iter().any(|record| {
            record.operation_id == sidecar.operation_id && record.recovery_kind == kind
        });
    if !already_recorded {
        crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_RECORD_AUDIT)?;
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
    }
    delete_sidecar_by_operation_id(root_uri, storage, &sidecar.operation_id).await?;
    Ok(true)
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
        if let Some(protocol) = sidecar.protocol_v3.as_ref() {
            // Exact-effect recovery publishes the durably confirmed output
            // slot, byte-for-byte, rather than deriving metadata from whatever
            // HEAD happens to be live later. Classification already proved the
            // observed HEAD transaction identity and version equal this slot.
            let slot = protocol
                .intended_delta
                .table_updates
                .iter()
                .find(|slot| slot.table_key == pin.table_key)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "OCC recovery delta missing table '{}'",
                        pin.table_key
                    ))
                })?;
            let confirmed = slot.confirmed.as_ref().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "OCC recovery delta table '{}' is not confirmed",
                    pin.table_key
                ))
            })?;
            if confirmed.table_version != state.lance_head {
                return Err(OmniError::manifest_internal(format!(
                    "OCC recovery table '{}' observed HEAD {} but confirmed delta names {}",
                    pin.table_key, state.lance_head, confirmed.table_version
                )));
            }
            expected.insert(pin.table_key.clone(), slot.expected_version);
            updates.push(ManifestChange::Update(SubTableUpdate {
                table_key: pin.table_key.clone(),
                table_version: confirmed.table_version,
                table_branch: confirmed.table_branch.clone(),
                row_count: confirmed.row_count,
                version_metadata: confirmed.version_metadata.clone(),
            }));
            published_versions.insert(pin.table_key.clone(), confirmed.table_version);
            continue;
        }
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
        let head_ds = crate::instrumentation::open_dataset(
            &dataset_uri,
            crate::instrumentation::VersionResolution::Latest,
            None,
            crate::instrumentation::table_wrapper(),
        )
        .await?;
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

    let (new_manifest_version, graph_commit_id) = publish_recovery_commit(
        root_uri,
        sidecar,
        RecoveryKind::RolledForward,
        &updates,
        &expected,
    )
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
    let ds = crate::instrumentation::open_dataset(
        table_path,
        crate::instrumentation::VersionResolution::Latest,
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await?;
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
/// row. For v3, re-entry detects the fixed original/rollback commit and replays
/// its durable exact audit payload without minting another commit. Legacy
/// sidecars retain their stale-sidecar cleanup behavior.
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
struct LanceHeadObservation {
    version: u64,
    transaction: Option<StagedTransactionIdentity>,
    effect_ownership: EffectOwnership,
}

#[cfg(test)]
async fn open_lance_head(
    table_path: &str,
    branch: Option<&str>,
    planned_effect: Option<(u64, &StagedTransactionIdentity, u64)>,
) -> Result<LanceHeadObservation> {
    open_lance_head_if_present(table_path, branch, planned_effect, false)
        .await?
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "required Lance branch '{}' is missing for table '{}'",
                branch.unwrap_or("main"),
                table_path
            ))
        })
}

/// Variant used only for an Armed v3 first-touch fork. `allow_missing_branch`
/// turns an absent named target ref into `None`; every storage/list failure and
/// every missing ref outside that exact recovery state remains a loud error.
async fn open_lance_head_if_present(
    table_path: &str,
    branch: Option<&str>,
    planned_effect: Option<(u64, &StagedTransactionIdentity, u64)>,
    allow_missing_branch: bool,
) -> Result<Option<LanceHeadObservation>> {
    let ds = crate::instrumentation::open_dataset(
        table_path,
        crate::instrumentation::VersionResolution::Latest,
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await?;
    let ds = match branch {
        Some(b) if b != "main" => {
            if allow_missing_branch {
                let branches = ds
                    .list_branches()
                    .await
                    .map_err(|error| OmniError::Lance(error.to_string()))?;
                if !branches.contains_key(b) {
                    return Ok(None);
                }
            }
            ds.checkout_branch(b)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?
        }
        _ => ds,
    };
    let head_transaction = if planned_effect.is_some() {
        ds.read_transaction()
            .await
            .map_err(|error| OmniError::Lance(error.to_string()))?
    } else {
        None
    };
    let transaction = head_transaction
        .as_ref()
        .map(StagedTransactionIdentity::from);
    let head_restore_target = head_transaction
        .as_ref()
        .and_then(|transaction| match &transaction.operation {
            lance::dataset::transaction::Operation::Restore { version } => Some(*version),
            _ => None,
        });
    let mut effect_ownership = EffectOwnership::None;
    if let Some((first_possible_version, planned, compensation_target)) = planned_effect {
        if ds.version().version >= first_possible_version {
            if ds
                .version()
                .version
                .saturating_sub(first_possible_version)
                .saturating_add(1)
                > MAX_EFFECT_IDENTITY_SCAN_VERSIONS
            {
                return Ok(Some(LanceHeadObservation {
                    version: ds.version().version,
                    transaction,
                    effect_ownership: EffectOwnership::Unverifiable,
                }));
            }
            let mut unverifiable = false;
            for version in first_possible_version..=ds.version().version {
                let observed = if version == ds.version().version {
                    transaction.clone()
                } else {
                    ds.read_transaction_by_version(version)
                        .await
                        .map_err(|error| OmniError::Lance(error.to_string()))?
                        .as_ref()
                        .map(StagedTransactionIdentity::from)
                };
                match observed {
                    // UUID is the durable ownership marker. A transparent
                    // Lance rebase is still this writer's physical effect
                    // whether or not Lance preserves `read_version`; exact
                    // classification separately refuses to roll it forward.
                    Some(observed) if observed.uuid == planned.uuid => {
                        effect_ownership = if version == ds.version().version {
                            EffectOwnership::OwnAtHead
                        } else {
                            EffectOwnership::OwnBeforeHead
                        };
                        break;
                    }
                    Some(_) => {}
                    None => unverifiable = true,
                }
            }
            if effect_ownership == EffectOwnership::None && unverifiable {
                effect_ownership = EffectOwnership::Unverifiable;
            }
            if effect_ownership == EffectOwnership::OwnBeforeHead
                && head_restore_target == Some(compensation_target)
            {
                effect_ownership = EffectOwnership::OwnCompensatedAtHead;
            }
        }
    }
    Ok(Some(LanceHeadObservation {
        version: ds.version().version,
        transaction,
        effect_ownership,
    }))
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
        schema_version: LEGACY_SIDECAR_SCHEMA_VERSION,
        operation_id,
        started_at,
        branch,
        actor_id,
        writer_kind,
        tables,
        merge_source_commit_id: None,
        additional_registrations: Vec::new(),
        tombstones: Vec::new(),
        protocol_v3: None,
        protocol_v4: None,
    }
}

/// Arm an RFC-022 mutation/load recovery sidecar.
///
/// The returned v3 sidecar is still purely an intent (`Armed`): every staged
/// transaction identity and manifest output slot is durable, but no achieved
/// version is recorded. The caller must persist it with [`write_sidecar`]
/// before the first table HEAD advance and later call
/// [`confirm_occ_sidecar_phase_b`] after every exact effect completes.
pub(crate) fn new_occ_sidecar(
    writer_kind: SidecarKind,
    branch: Option<String>,
    actor_id: Option<String>,
    tables: Vec<SidecarTablePin>,
    authority: RecoveryAuthorityToken,
    lineage: RecoveryLineageIntent,
    planned_transactions: HashMap<String, StagedTransactionIdentity>,
) -> Result<RecoverySidecar> {
    if !matches!(writer_kind, SidecarKind::Mutation | SidecarKind::Load) {
        return Err(OmniError::manifest_internal(format!(
            "new_occ_sidecar only supports Mutation/Load, found {:?}",
            writer_kind
        )));
    }
    let pin_keys: HashSet<&str> = tables.iter().map(|pin| pin.table_key.as_str()).collect();
    if tables.is_empty()
        || pin_keys.len() != tables.len()
        || planned_transactions.len() != tables.len()
        || !tables
            .iter()
            .all(|pin| planned_transactions.contains_key(&pin.table_key))
    {
        return Err(OmniError::manifest_internal(
            "new_occ_sidecar requires at least one pin and exactly one planned transaction per unique table pin",
        ));
    }

    let operation_id = ulid::Ulid::new().to_string();
    let started_at = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_micros().to_string(),
        Err(_) => "0".to_string(),
    };
    let effects = tables
        .iter()
        .map(|pin| RecoveryTableEffect {
            table_key: pin.table_key.clone(),
            planned_transaction: planned_transactions
                .get(&pin.table_key)
                .expect("planned key set checked above")
                .clone(),
            confirmed_transaction: None,
        })
        .collect();
    let table_updates = tables
        .iter()
        .map(|pin| RecoveryTableUpdateSlot {
            table_key: pin.table_key.clone(),
            expected_version: pin.expected_version,
            table_branch: pin.table_branch.clone(),
            confirmed: None,
        })
        .collect();
    let sidecar = RecoverySidecar {
        schema_version: MUTATION_LOAD_SIDECAR_SCHEMA_VERSION,
        operation_id,
        started_at,
        branch,
        actor_id,
        writer_kind,
        tables,
        merge_source_commit_id: None,
        additional_registrations: Vec::new(),
        tombstones: Vec::new(),
        protocol_v3: Some(RecoveryProtocolV3 {
            authority,
            lineage,
            rollback_graph_commit_id: ulid::Ulid::new().to_string(),
            rollback_audit_outcomes: None,
            effect_phase: RecoveryEffectPhase::Armed,
            effects,
            intended_delta: RecoveryManifestDelta {
                table_updates,
                registrations: Vec::new(),
                tombstones: Vec::new(),
            },
        }),
        protocol_v4: None,
    };
    validate_sidecar_shape("<new-occ-sidecar>", &sidecar)?;
    Ok(sidecar)
}

/// Bind every physical output slot of an RFC-022 sidecar and durably transition
/// it from `Armed` to `EffectsConfirmed`.
///
/// Validation happens against a clone first. A missing table, a rebased Lance
/// transaction, or a version/branch mismatch leaves the on-disk sidecar Armed,
/// which recovery interprets as partial Phase B and rolls back. Only the exact
/// planned transaction set can become eligible for roll-forward.
pub(crate) async fn confirm_occ_sidecar_phase_b(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &mut RecoverySidecar,
    updates: &[SubTableUpdate],
    committed_transactions: &HashMap<String, StagedTransactionIdentity>,
) -> Result<()> {
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_SIDECAR_CONFIRM)?;
    validate_sidecar_shape(&sidecar_uri(root_uri, &sidecar.operation_id), sidecar)?;

    let protocol = sidecar.protocol_v3.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("confirm_occ_sidecar_phase_b requires a v3 OCC sidecar")
    })?;
    if protocol.effect_phase != RecoveryEffectPhase::Armed {
        return Err(OmniError::manifest_internal(format!(
            "OCC sidecar '{}' is already EffectsConfirmed",
            sidecar.operation_id
        )));
    }
    let update_keys: HashSet<&str> = updates
        .iter()
        .map(|update| update.table_key.as_str())
        .collect();
    let planned_keys: HashSet<&str> = protocol
        .effects
        .iter()
        .map(|effect| effect.table_key.as_str())
        .collect();
    if update_keys.len() != updates.len()
        || committed_transactions.len() != planned_keys.len()
        || update_keys != planned_keys
        || !planned_keys
            .iter()
            .all(|key| committed_transactions.contains_key(*key))
    {
        return Err(OmniError::manifest_internal(format!(
            "OCC sidecar '{}' confirmation key set differs from its planned effects",
            sidecar.operation_id
        )));
    }

    // Validate every effect before mutating the clone. Transaction identity is
    // necessary but not sufficient: the version/branch check below also
    // rejects Lance's preflight rebase, which can preserve both transaction
    // fields while landing at a later version.
    for effect in &protocol.effects {
        let committed = committed_transactions
            .get(&effect.table_key)
            .expect("confirmed key set checked above");
        if committed != &effect.planned_transaction {
            return Err(OmniError::manifest_internal(format!(
                "OCC sidecar '{}' table '{}' committed transaction {:?}, planned {:?}; \
                 leaving recovery Armed",
                sidecar.operation_id, effect.table_key, committed, effect.planned_transaction
            )));
        }
        let pin = sidecar
            .tables
            .iter()
            .find(|pin| pin.table_key == effect.table_key)
            .expect("sidecar key sets validated");
        let update = updates
            .iter()
            .find(|update| update.table_key == effect.table_key)
            .expect("confirmed key set checked above");
        if update.table_version != pin.post_commit_pin || update.table_branch != pin.table_branch {
            return Err(OmniError::manifest_internal(format!(
                "OCC sidecar '{}' table '{}' achieved version/branch ({}, {:?}), \
                 planned ({}, {:?}); leaving recovery Armed",
                sidecar.operation_id,
                effect.table_key,
                update.table_version,
                update.table_branch,
                pin.post_commit_pin,
                pin.table_branch
            )));
        }
    }

    let mut confirmed = sidecar.clone();
    let confirmed_protocol = confirmed
        .protocol_v3
        .as_mut()
        .expect("validated v3 protocol");
    for effect in &mut confirmed_protocol.effects {
        effect.confirmed_transaction = Some(
            committed_transactions
                .get(&effect.table_key)
                .expect("confirmed key set checked above")
                .clone(),
        );
    }
    for slot in &mut confirmed_protocol.intended_delta.table_updates {
        let update = updates
            .iter()
            .find(|update| update.table_key == slot.table_key)
            .expect("confirmed key set checked above");
        slot.confirmed = Some(RecoveryConfirmedTableUpdate {
            table_version: update.table_version,
            table_branch: update.table_branch.clone(),
            row_count: update.row_count,
            version_metadata: update.version_metadata.clone(),
        });
    }
    for pin in &mut confirmed.tables {
        let update = updates
            .iter()
            .find(|update| update.table_key == pin.table_key)
            .expect("confirmed key set checked above");
        pin.confirmed_version = Some(update.table_version);
    }
    confirmed_protocol.effect_phase = RecoveryEffectPhase::EffectsConfirmed;

    let uri = sidecar_uri(root_uri, &confirmed.operation_id);
    validate_sidecar_shape(&uri, &confirmed)?;
    let json = serde_json::to_string_pretty(&confirmed).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to serialize confirmed OCC recovery sidecar: {}",
            error
        ))
    })?;
    storage.write_text(&uri, &json).await?;
    *sidecar = confirmed;
    Ok(())
}

/// Arm the RFC-022 BranchMerge recovery protocol.
///
/// `tables` and `effects` describe only independently durable physical work;
/// `intended_delta.table_updates` is deliberately allowed to be a superset so
/// pointer-only manifest updates are recovered in the same exact graph commit.
/// Every physical output remains unconfirmed until
/// [`confirm_branch_merge_sidecar_phase_b`] atomically binds the full delta.
pub(crate) fn new_branch_merge_sidecar(
    branch: Option<String>,
    actor_id: Option<String>,
    tables: Vec<SidecarTablePin>,
    authority: RecoveryAuthorityToken,
    lineage: RecoveryLineageIntent,
    effects: Vec<RecoveryBranchMergeEffect>,
    intended_delta: RecoveryManifestDelta,
) -> Result<RecoverySidecar> {
    let operation_id = ulid::Ulid::new().to_string();
    let started_at = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_micros().to_string(),
        Err(_) => "0".to_string(),
    };
    let sidecar = RecoverySidecar {
        schema_version: BRANCH_MERGE_SIDECAR_SCHEMA_VERSION,
        operation_id,
        started_at,
        branch,
        actor_id,
        writer_kind: SidecarKind::BranchMerge,
        tables,
        merge_source_commit_id: None,
        additional_registrations: Vec::new(),
        tombstones: Vec::new(),
        protocol_v3: None,
        protocol_v4: Some(RecoveryProtocolV4 {
            authority,
            lineage,
            rollback_graph_commit_id: ulid::Ulid::new().to_string(),
            rollback_audit_outcomes: None,
            effect_phase: RecoveryEffectPhase::Armed,
            effects,
            intended_delta,
        }),
    };
    validate_sidecar_shape("<new-branch-merge-sidecar>", &sidecar)?;
    Ok(sidecar)
}

/// Durably transition a v4 BranchMerge sidecar from Armed to
/// EffectsConfirmed. `updates` is the complete manifest output, including
/// pointer-only slots with no physical effect. First-touch physical effects
/// additionally supply the exact Lance target-ref identity minted during
/// creation; existing-ref effects must not appear in that map.
pub(crate) async fn confirm_branch_merge_sidecar_phase_b(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    sidecar: &mut RecoverySidecar,
    updates: &[SubTableUpdate],
    confirmed_ref_identifiers: &HashMap<String, lance::dataset::refs::BranchIdentifier>,
) -> Result<()> {
    crate::failpoints::maybe_fail(crate::failpoints::names::RECOVERY_SIDECAR_CONFIRM)?;
    let uri = sidecar_uri(root_uri, &sidecar.operation_id);
    validate_sidecar_shape(&uri, sidecar)?;
    let protocol = sidecar.protocol_v4.as_ref().ok_or_else(|| {
        OmniError::manifest_internal(
            "confirm_branch_merge_sidecar_phase_b requires a v4 BranchMerge sidecar",
        )
    })?;
    if protocol.effect_phase != RecoveryEffectPhase::Armed {
        return Err(OmniError::manifest_internal(format!(
            "BranchMerge sidecar '{}' is already EffectsConfirmed",
            sidecar.operation_id
        )));
    }

    let update_keys: HashSet<&str> = updates
        .iter()
        .map(|update| update.table_key.as_str())
        .collect();
    let delta_keys: HashSet<&str> = protocol
        .intended_delta
        .table_updates
        .iter()
        .map(|slot| slot.table_key.as_str())
        .collect();
    if update_keys.len() != updates.len() || update_keys != delta_keys {
        return Err(OmniError::manifest_internal(format!(
            "BranchMerge sidecar '{}' confirmation updates differ from its complete intended delta",
            sidecar.operation_id
        )));
    }
    let first_touch_keys: HashSet<&str> = protocol
        .effects
        .iter()
        .filter(|effect| effect.kind.source_fork_version().is_some())
        .map(|effect| effect.table_key.as_str())
        .collect();
    let confirmed_ref_keys: HashSet<&str> = confirmed_ref_identifiers
        .keys()
        .map(String::as_str)
        .collect();
    if confirmed_ref_keys != first_touch_keys {
        return Err(OmniError::manifest_internal(format!(
            "BranchMerge sidecar '{}' confirmed target-ref set differs from its first-touch effects",
            sidecar.operation_id
        )));
    }

    for slot in &protocol.intended_delta.table_updates {
        let update = updates
            .iter()
            .find(|update| update.table_key == slot.table_key)
            .expect("update/delta key sets checked above");
        if update.table_branch != slot.table_branch {
            return Err(OmniError::manifest_internal(format!(
                "BranchMerge sidecar '{}' table '{}' achieved output branch {:?}, planned {:?}",
                sidecar.operation_id, slot.table_key, update.table_branch, slot.table_branch
            )));
        }
    }
    for effect in &protocol.effects {
        let pin = sidecar
            .tables
            .iter()
            .find(|pin| pin.table_key == effect.table_key)
            .expect("v4 shape validated effect/pin key sets");
        let update = updates
            .iter()
            .find(|update| update.table_key == effect.table_key)
            .expect("physical effect is a delta-slot subset");
        let observed = observe_branch_merge_target_ref(pin).await?.ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "BranchMerge sidecar '{}' cannot confirm missing target ref for table '{}'",
                sidecar.operation_id, effect.table_key
            ))
        })?;
        if observed.version != update.table_version {
            return Err(OmniError::manifest_internal(format!(
                "BranchMerge sidecar '{}' table '{}' observed HEAD {}, achieved update says {}",
                sidecar.operation_id, effect.table_key, observed.version, update.table_version
            )));
        }
        if let Some(source_fork_version) = effect.kind.source_fork_version() {
            if observed.parent_version != Some(source_fork_version) {
                return Err(OmniError::manifest_internal(format!(
                    "BranchMerge sidecar '{}' table '{}' target ref parent is {:?}, exact fork version is {}",
                    sidecar.operation_id,
                    effect.table_key,
                    observed.parent_version,
                    source_fork_version
                )));
            }
            let expected_identifier = confirmed_ref_identifiers
                .get(&effect.table_key)
                .expect("confirmed first-touch ref key set checked above");
            if &observed.branch_identifier != expected_identifier {
                return Err(OmniError::manifest_internal(format!(
                    "BranchMerge sidecar '{}' table '{}' target ref identity differs from the writer's achieved identity",
                    sidecar.operation_id, effect.table_key
                )));
            }
        }
        match &effect.kind {
            RecoveryBranchMergeEffectKind::MultiCommitHead {
                planned_transactions,
                ..
            } => {
                let planned_final_version = planned_transactions
                    .last()
                    .expect("v4 shape validation requires a non-empty transaction chain")
                    .read_version
                    .checked_add(1)
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "BranchMerge sidecar '{}' planned transaction chain for '{}' overflows its final output version",
                            sidecar.operation_id, effect.table_key
                        ))
                    })?;
                if update.table_version < planned_final_version {
                    return Err(OmniError::manifest_internal(format!(
                        "BranchMerge sidecar '{}' multi-commit table '{}' stopped before its complete planned transaction chain ({} < {})",
                        sidecar.operation_id,
                        effect.table_key,
                        update.table_version,
                        planned_final_version
                    )));
                }
                let proof = prove_branch_merge_multi_commit_effect(
                    &observed,
                    planned_transactions,
                    pin.expected_version,
                    &effect.table_key,
                )
                .await?;
                if proof.effect_ownership != EffectOwnership::OwnAtHead
                    || !proof.full_effect_at_head
                    || proof.unsafe_reason.is_some()
                {
                    return Err(OmniError::manifest_internal(format!(
                        "BranchMerge sidecar '{}' cannot confirm unproven transaction chain for table '{}': {}",
                        sidecar.operation_id,
                        effect.table_key,
                        proof
                            .unsafe_reason
                            .as_deref()
                            .unwrap_or("the full exact effect is not at HEAD")
                    )));
                }
            }
            RecoveryBranchMergeEffectKind::RefOnlyFork { source_version, .. } => {
                if update.table_version != *source_version {
                    return Err(OmniError::manifest_internal(format!(
                        "BranchMerge sidecar '{}' ref-only table '{}' achieved version {}, exact fork version {}",
                        sidecar.operation_id,
                        effect.table_key,
                        update.table_version,
                        source_version
                    )));
                }
            }
        }
    }

    let mut confirmed = sidecar.clone();
    let confirmed_protocol = confirmed
        .protocol_v4
        .as_mut()
        .expect("validated v4 protocol");
    for slot in &mut confirmed_protocol.intended_delta.table_updates {
        let update = updates
            .iter()
            .find(|update| update.table_key == slot.table_key)
            .expect("update/delta key sets checked above");
        slot.confirmed = Some(RecoveryConfirmedTableUpdate {
            table_version: update.table_version,
            table_branch: update.table_branch.clone(),
            row_count: update.row_count,
            version_metadata: update.version_metadata.clone(),
        });
    }
    for effect in &mut confirmed_protocol.effects {
        let update = updates
            .iter()
            .find(|update| update.table_key == effect.table_key)
            .expect("physical effect is a delta-slot subset");
        let ref_identifier = confirmed_ref_identifiers.get(&effect.table_key).cloned();
        match &mut effect.kind {
            RecoveryBranchMergeEffectKind::MultiCommitHead {
                confirmed_version,
                confirmed_branch_identifier,
                ..
            } => {
                *confirmed_version = Some(update.table_version);
                *confirmed_branch_identifier = ref_identifier;
            }
            RecoveryBranchMergeEffectKind::RefOnlyFork {
                confirmed_branch_identifier,
                ..
            } => {
                *confirmed_branch_identifier = ref_identifier;
            }
        }
    }
    for pin in &mut confirmed.tables {
        let update = updates
            .iter()
            .find(|update| update.table_key == pin.table_key)
            .expect("effect/pin keys are delta-slot subset");
        pin.confirmed_version = Some(update.table_version);
    }
    confirmed_protocol.effect_phase = RecoveryEffectPhase::EffectsConfirmed;

    validate_sidecar_shape(&uri, &confirmed)?;
    let json = serde_json::to_string_pretty(&confirmed).map_err(|error| {
        OmniError::manifest_internal(format!(
            "failed to serialize confirmed BranchMerge recovery sidecar: {error}"
        ))
    })?;
    storage.write_text(&uri, &json).await?;
    *sidecar = confirmed;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ObjectStorageAdapter;
    use crate::table_store::TableStore;
    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    #[cfg(feature = "failpoints")]
    use futures::TryStreamExt;
    use lance::Dataset;
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

    fn transaction(read_version: u64, uuid: &str) -> StagedTransactionIdentity {
        StagedTransactionIdentity {
            read_version,
            uuid: uuid.to_string(),
        }
    }

    fn occ_sidecar() -> RecoverySidecar {
        let planned = transaction(5, "planned-transaction");
        new_occ_sidecar(
            SidecarKind::Mutation,
            None,
            Some("act-alice".to_string()),
            vec![make_pin("node:Person", "file:///tmp/people.lance", 5, 6)],
            RecoveryAuthorityToken {
                branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
                graph_head: Some("01H000000000000000000000AA".to_string()),
                schema_ir_hash: "schema-hash".to_string(),
                schema_identity_version: 1,
            },
            RecoveryLineageIntent {
                graph_commit_id: "01H000000000000000000000BB".to_string(),
                branch: None,
                actor_id: Some("act-alice".to_string()),
                merged_parent_commit_id: None,
                created_at: 123,
            },
            HashMap::from([("node:Person".to_string(), planned)]),
        )
        .unwrap()
    }

    fn test_branch_identifier(
        version: u64,
        suffix: &str,
    ) -> lance::dataset::refs::BranchIdentifier {
        lance::dataset::refs::BranchIdentifier {
            version_mapping: vec![(version, suffix.to_string())],
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
        assert_eq!(parsed.schema_version, LEGACY_SIDECAR_SCHEMA_VERSION);
        assert_eq!(parsed.operation_id, original.operation_id);
        assert_eq!(parsed.writer_kind, SidecarKind::Mutation);
        assert_eq!(parsed.branch.as_deref(), Some("main"));
        assert_eq!(parsed.actor_id.as_deref(), Some("act-alice"));
        assert_eq!(parsed.tables.len(), 1);
        assert_eq!(parsed.tables[0].table_key, "node:Person");
    }

    #[test]
    fn occ_sidecar_round_trips_armed_with_stable_ids_and_exact_effect() {
        let original = occ_sidecar();
        assert_eq!(
            original.schema_version,
            MUTATION_LOAD_SIDECAR_SCHEMA_VERSION
        );
        let protocol = original.protocol_v3.as_ref().unwrap();
        assert_eq!(protocol.effect_phase, RecoveryEffectPhase::Armed);
        assert_eq!(protocol.effects.len(), 1);
        assert_eq!(
            protocol.effects[0].planned_transaction,
            transaction(5, "planned-transaction")
        );
        assert!(!protocol.rollback_graph_commit_id.is_empty());
        assert_eq!(
            protocol.lineage.graph_commit_id,
            "01H000000000000000000000BB"
        );

        let json = serde_json::to_string(&original).unwrap();
        let parsed = parse_sidecar("memory://graph/__recovery/op.json", &json).unwrap();
        assert_eq!(
            parsed.protocol_v3.as_ref().unwrap().authority,
            protocol.authority
        );

        let pin = &parsed.tables[0];
        assert_eq!(
            classify_table_observation(
                pin,
                6,
                Some(&transaction(5, "planned-transaction")),
                5,
                SidecarKind::Mutation,
                SIDECAR_SCHEMA_VERSION,
                parsed.protocol_v3.as_ref(),
            ),
            TableClassification::IncompletePhaseB,
            "an Armed sidecar must roll back even when its planned transaction is at HEAD"
        );
    }

    #[tokio::test]
    async fn branch_merge_v4_arms_multi_commit_ref_only_and_pointer_slots() {
        let root = "mem-root";
        let storage = ObjectStorageAdapter::in_memory();
        let target_identifier = test_branch_identifier(3, "target-incarnation");
        let lineage = RecoveryLineageIntent {
            graph_commit_id: "01H000000000000000000000M1".to_string(),
            branch: Some("target".to_string()),
            actor_id: Some("act-merge".to_string()),
            merged_parent_commit_id: Some("01H000000000000000000000S1".to_string()),
            created_at: 456,
        };
        let mut multi_pin = make_pin("node:Person", "memory://person", 5, 6);
        multi_pin.table_branch = Some("target".to_string());
        let mut ref_pin = make_pin("node:Company", "memory://company", 4, 8);
        ref_pin.table_branch = Some("target".to_string());
        let effects = vec![
            RecoveryBranchMergeEffect {
                table_key: "node:Person".to_string(),
                kind: RecoveryBranchMergeEffectKind::MultiCommitHead {
                    source_fork_version: Some(5),
                    planned_transactions: vec![
                        transaction(5, "merge-person-append"),
                        transaction(6, "merge-person-delete"),
                    ],
                    confirmed_version: None,
                    confirmed_branch_identifier: None,
                },
            },
            RecoveryBranchMergeEffect {
                table_key: "node:Company".to_string(),
                kind: RecoveryBranchMergeEffectKind::RefOnlyFork {
                    // Deliberately differs from the target manifest CAS pin (4):
                    // ref cleanup must use this exact SOURCE fork point.
                    source_version: 8,
                    confirmed_branch_identifier: None,
                },
            },
        ];
        let intended_delta = RecoveryManifestDelta {
            table_updates: vec![
                RecoveryTableUpdateSlot {
                    table_key: "node:Person".to_string(),
                    expected_version: 5,
                    table_branch: Some("target".to_string()),
                    confirmed: None,
                },
                RecoveryTableUpdateSlot {
                    table_key: "node:Company".to_string(),
                    expected_version: 4,
                    table_branch: Some("target".to_string()),
                    confirmed: None,
                },
                // No physical effect/pin: recovery must still publish this
                // pointer-only output in the same fixed merge commit.
                RecoveryTableUpdateSlot {
                    table_key: "edge:WorksAt".to_string(),
                    expected_version: 2,
                    table_branch: None,
                    confirmed: None,
                },
            ],
            registrations: Vec::new(),
            tombstones: Vec::new(),
        };
        let sidecar = new_branch_merge_sidecar(
            Some("target".to_string()),
            Some("act-merge".to_string()),
            vec![multi_pin, ref_pin],
            RecoveryAuthorityToken {
                branch_identifier: target_identifier,
                graph_head: Some("01H000000000000000000000T1".to_string()),
                schema_ir_hash: "schema-hash".to_string(),
                schema_identity_version: 1,
            },
            lineage,
            effects,
            intended_delta,
        )
        .unwrap();
        assert_eq!(sidecar.schema_version, BRANCH_MERGE_SIDECAR_SCHEMA_VERSION);
        assert!(sidecar.protocol_v3.is_none());
        assert_eq!(
            sidecar.protocol_v4.as_ref().unwrap().effect_phase,
            RecoveryEffectPhase::Armed
        );

        let mut non_sequential = sidecar.clone();
        let RecoveryBranchMergeEffectKind::MultiCommitHead {
            planned_transactions,
            ..
        } = &mut non_sequential.protocol_v4.as_mut().unwrap().effects[0].kind
        else {
            panic!("first effect must be multi-commit")
        };
        planned_transactions[1].read_version = 9;
        let error = validate_sidecar_shape("<non-sequential>", &non_sequential).unwrap_err();
        assert!(error.to_string().contains("expected 6"));

        let mut mismatched_slot = sidecar.clone();
        mismatched_slot
            .protocol_v4
            .as_mut()
            .unwrap()
            .intended_delta
            .table_updates[0]
            .expected_version = 4;
        let error = validate_sidecar_shape("<mismatched-slot>", &mismatched_slot).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match its physical table pin")
        );

        write_sidecar(root, &storage, &sidecar).await.unwrap();

        let listed = list_sidecars(root, &storage).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].schema_version,
            BRANCH_MERGE_SIDECAR_SCHEMA_VERSION
        );
    }

    #[test]
    fn parse_sidecar_refuses_v3_without_protocol_payload() {
        let mut value = serde_json::to_value(occ_sidecar()).unwrap();
        value.as_object_mut().unwrap().remove("protocol_v3");
        let error = parse_sidecar(
            "memory://graph/__recovery/op.json",
            &serde_json::to_string(&value).unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("missing required protocol_v3"));
    }

    #[test]
    fn rollback_audit_plan_is_an_explicit_unique_subset_of_pins() {
        let mut empty = occ_sidecar();
        empty.protocol_v3.as_mut().unwrap().rollback_audit_outcomes = Some(Vec::new());
        let json = serde_json::to_string(&empty).unwrap();
        assert!(
            json.contains("rollback_audit_outcomes"),
            "Some(empty) must remain distinguishable from an unprepared None plan"
        );
        parse_sidecar("memory://graph/__recovery/empty.json", &json).unwrap();

        let mut duplicate = occ_sidecar();
        duplicate
            .protocol_v3
            .as_mut()
            .unwrap()
            .rollback_audit_outcomes = Some(vec![
            TableOutcome {
                table_key: "node:Person".to_string(),
                from_version: 6,
                to_version: 5,
            },
            TableOutcome {
                table_key: "node:Person".to_string(),
                from_version: 7,
                to_version: 5,
            },
        ]);
        let duplicate_error = parse_sidecar(
            "memory://graph/__recovery/duplicate.json",
            &serde_json::to_string(&duplicate).unwrap(),
        )
        .unwrap_err();
        assert!(duplicate_error.to_string().contains("unique subset"));

        let mut foreign = occ_sidecar();
        foreign
            .protocol_v3
            .as_mut()
            .unwrap()
            .rollback_audit_outcomes = Some(vec![TableOutcome {
            table_key: "node:Foreign".to_string(),
            from_version: 2,
            to_version: 1,
        }]);
        let foreign_error = parse_sidecar(
            "memory://graph/__recovery/foreign.json",
            &serde_json::to_string(&foreign).unwrap(),
        )
        .unwrap_err();
        assert!(foreign_error.to_string().contains("unique subset"));
    }

    #[tokio::test]
    async fn occ_confirmation_binds_exact_delta_and_rejects_foreign_identity() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = ObjectStorageAdapter::local();
        let mut sidecar = occ_sidecar();
        write_sidecar(root, &storage, &sidecar).await.unwrap();

        let version_metadata: crate::db::manifest::TableVersionMetadata =
            serde_json::from_value(serde_json::json!({
                "manifest_path": "nodes/person/_versions/6.manifest",
                "manifest_size": 42,
                "e_tag": "etag-6",
                "naming_scheme": "V2"
            }))
            .unwrap();
        let updates = vec![SubTableUpdate {
            table_key: "node:Person".to_string(),
            table_version: 6,
            table_branch: None,
            row_count: 7,
            version_metadata,
        }];
        let committed = HashMap::from([(
            "node:Person".to_string(),
            transaction(5, "planned-transaction"),
        )]);
        let rebased = HashMap::from([(
            "node:Person".to_string(),
            transaction(6, "planned-transaction"),
        )]);
        let error = confirm_occ_sidecar_phase_b(root, &storage, &mut sidecar, &updates, &rebased)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("leaving recovery Armed"));
        assert_eq!(
            sidecar.protocol_v3.as_ref().unwrap().effect_phase,
            RecoveryEffectPhase::Armed
        );
        confirm_occ_sidecar_phase_b(root, &storage, &mut sidecar, &updates, &committed)
            .await
            .unwrap();

        let protocol = sidecar.protocol_v3.as_ref().unwrap();
        assert_eq!(protocol.effect_phase, RecoveryEffectPhase::EffectsConfirmed);
        assert_eq!(
            protocol.intended_delta.table_updates[0]
                .confirmed
                .as_ref()
                .unwrap()
                .row_count,
            7
        );
        let pin = &sidecar.tables[0];
        assert_eq!(
            classify_table_observation(
                pin,
                6,
                Some(&transaction(5, "planned-transaction")),
                5,
                SidecarKind::Mutation,
                SIDECAR_SCHEMA_VERSION,
                Some(protocol),
            ),
            TableClassification::RolledPastExpected
        );
        assert_eq!(
            classify_table_observation(
                pin,
                6,
                Some(&transaction(5, "foreign-transaction")),
                5,
                SidecarKind::Mutation,
                SIDECAR_SCHEMA_VERSION,
                Some(protocol),
            ),
            TableClassification::UnexpectedEffectIdentity
        );
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
            classify_table(
                &pin,
                5,
                5,
                SidecarKind::Mutation,
                LEGACY_SIDECAR_SCHEMA_VERSION,
            ),
            TableClassification::NoMovement,
        );
    }

    #[test]
    fn classify_rolled_past_expected_when_sidecar_matches_strict() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(
                &pin,
                6,
                5,
                SidecarKind::Mutation,
                LEGACY_SIDECAR_SCHEMA_VERSION,
            ),
            TableClassification::RolledPastExpected,
        );
    }

    #[test]
    fn classify_unexpected_at_p1_when_sidecar_does_not_match_strict() {
        // Same +1 drift but post_commit_pin says it should be 7, not 6.
        let pin = make_pin("node:Person", "irrelevant", 5, 7);
        assert_eq!(
            classify_table(
                &pin,
                6,
                5,
                SidecarKind::Mutation,
                LEGACY_SIDECAR_SCHEMA_VERSION,
            ),
            TableClassification::UnexpectedAtP1,
        );
    }

    #[test]
    fn classify_unexpected_multistep_when_head_jumped_more_than_one_strict() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(
                &pin,
                8,
                5,
                SidecarKind::Mutation,
                LEGACY_SIDECAR_SCHEMA_VERSION,
            ),
            TableClassification::UnexpectedMultistep,
        );
    }

    #[test]
    fn classify_invariant_violation_when_head_below_pinned() {
        let pin = make_pin("node:Person", "irrelevant", 5, 6);
        assert_eq!(
            classify_table(
                &pin,
                3,
                5,
                SidecarKind::Mutation,
                LEGACY_SIDECAR_SCHEMA_VERSION,
            ),
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
            classify_table(
                &pin,
                9,
                5,
                SidecarKind::EnsureIndices,
                SIDECAR_SCHEMA_VERSION
            ),
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
            classify_table(
                &unconfirmed,
                8,
                5,
                SidecarKind::BranchMerge,
                SIDECAR_SCHEMA_VERSION
            ),
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
            classify_table(
                &confirmed,
                8,
                5,
                SidecarKind::BranchMerge,
                SIDECAR_SCHEMA_VERSION
            ),
            TableClassification::RolledPastExpected,
        );
        // Confirmed, but HEAD drifted past it (foreign writer) → roll back.
        assert_eq!(
            classify_table(
                &confirmed,
                9,
                5,
                SidecarKind::BranchMerge,
                SIDECAR_SCHEMA_VERSION
            ),
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
        let store = TableStore::new(
            dir.path().to_str().unwrap(),
            Arc::new(lance::session::Session::default()),
        );

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
        let store = TableStore::new(
            dir.path().to_str().unwrap(),
            Arc::new(lance::session::Session::default()),
        );

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
    async fn exact_effect_observation_recognizes_interrupted_rollback_restore() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
        let store = TableStore::new(
            dir.path().to_str().unwrap(),
            Arc::new(lance::session::Session::default()),
        );
        let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
            .await
            .unwrap();
        let staged = store
            .stage_append(&ds, person_batch(&[("bob", Some(25))]), &[])
            .await
            .unwrap();
        let planned = staged.transaction_identity();
        let (committed, observed) = store
            .commit_staged_exact(Arc::new(ds), staged)
            .await
            .unwrap();
        assert_eq!(observed, planned);
        assert_eq!(committed.version().version, 2);

        // Model Full recovery crashing after Dataset::restore but before the
        // restored HEAD is published to __manifest.
        restore_table_to_version(&uri, None, 1).await.unwrap();
        let observation = open_lance_head(&uri, None, Some((2, &planned, 1)))
            .await
            .unwrap();
        assert_eq!(observation.version, 3);
        assert_eq!(
            observation.effect_ownership,
            EffectOwnership::OwnCompensatedAtHead
        );

        // A later/foreign Restore is not proof that recovery compensated to
        // the manifest pin. Exact target identity matters: with manifest still
        // pinned to v1, a HEAD Restore(v2) must remain interleaved rather than
        // being promoted to resumable compensation.
        restore_table_to_version(&uri, None, 2).await.unwrap();
        let foreign_restore = open_lance_head(&uri, None, Some((2, &planned, 1)))
            .await
            .unwrap();
        assert_eq!(foreign_restore.version, 4);
        assert_eq!(
            foreign_restore.effect_ownership,
            EffectOwnership::OwnBeforeHead
        );
    }

    #[tokio::test]
    async fn branch_merge_v4_proves_exact_chain_and_interrupted_compensation() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
        let store = TableStore::new(
            dir.path().to_str().unwrap(),
            Arc::new(lance::session::Session::default()),
        );
        let ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
            .await
            .unwrap();

        let first_staged = store
            .stage_append(&ds, person_batch(&[("bob", Some(25))]), &[])
            .await
            .unwrap();
        let first_planned = first_staged.transaction_identity();
        let (after_first, first_committed) = store
            .commit_staged_exact(Arc::new(ds), first_staged)
            .await
            .unwrap();
        assert_eq!(first_committed, first_planned);

        let second_staged = store
            .stage_append(&after_first, person_batch(&[("carol", Some(40))]), &[])
            .await
            .unwrap();
        let second_planned = second_staged.transaction_identity();
        let (after_second, second_committed) = store
            .commit_staged_exact(Arc::new(after_first), second_staged)
            .await
            .unwrap();
        assert_eq!(second_committed, second_planned);
        assert_eq!(after_second.version().version, 3);

        let observation = BranchMergeRefObservation {
            dataset: after_second.clone(),
            version: after_second.version().version,
            branch_identifier: after_second.branch_identifier().await.unwrap(),
            parent_version: None,
        };
        let planned = vec![first_planned.clone(), second_planned.clone()];
        let proof =
            prove_branch_merge_multi_commit_effect(&observation, &planned, 1, "node:Person")
                .await
                .unwrap();
        assert_eq!(proof.effect_ownership, EffectOwnership::OwnAtHead);
        assert!(proof.full_effect_at_head);
        assert!(proof.unsafe_reason.is_none());
        drop(observation);
        drop(after_second);

        // Model Full recovery crashing after it appended Restore(v1), before
        // it could publish that compensated HEAD through the manifest.
        restore_table_to_version(&uri, None, 1).await.unwrap();
        let compensated = Dataset::open(&uri).await.unwrap();
        let observation = BranchMergeRefObservation {
            version: compensated.version().version,
            branch_identifier: compensated.branch_identifier().await.unwrap(),
            parent_version: None,
            dataset: compensated,
        };
        let proof =
            prove_branch_merge_multi_commit_effect(&observation, &planned, 1, "node:Person")
                .await
                .unwrap();
        assert_eq!(
            proof.effect_ownership,
            EffectOwnership::OwnCompensatedAtHead
        );
        assert!(!proof.full_effect_at_head);
        assert!(proof.unsafe_reason.is_none());
    }

    #[tokio::test]
    async fn branch_merge_v4_refuses_unplanned_head_movement() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/people.lance", dir.path().to_str().unwrap());
        let store = TableStore::new(
            dir.path().to_str().unwrap(),
            Arc::new(lance::session::Session::default()),
        );
        let mut ds = TableStore::write_dataset(&uri, person_batch(&[("alice", Some(30))]))
            .await
            .unwrap();
        store
            .append_batch(&uri, &mut ds, person_batch(&[("foreign", Some(99))]))
            .await
            .unwrap();
        let observation = BranchMergeRefObservation {
            version: ds.version().version,
            branch_identifier: ds.branch_identifier().await.unwrap(),
            parent_version: None,
            dataset: ds,
        };

        let proof = prove_branch_merge_multi_commit_effect(
            &observation,
            &[transaction(1, "planned-but-not-committed")],
            1,
            "node:Person",
        )
        .await
        .unwrap();
        assert_eq!(proof.effect_ownership, EffectOwnership::Unverifiable);
        assert!(!proof.full_effect_at_head);
        assert!(proof.unsafe_reason.is_some());
    }

    #[tokio::test]
    async fn exact_preflight_rebase_recovery_preserves_published_winner() {
        // Lance may preflight-rebase an Append even when CommitBuilder's retry
        // budget is zero. Reproduce the exact availability hazard:
        //
        //   v1  prepared attempt's expected version
        //   v2  a competing writer commits and publishes
        //   v3  the stale prepared transaction lands at HEAD with its own UUID
        //
        // Full recovery must compensate v3 back to the CURRENT manifest pin
        // (v2), not reject the sidecar merely because v2 != its original v1
        // expectation and not restore all the way to v1 (which loses winner).
        const SCHEMA: &str = r#"
node Person {
    age: I32?
}
"#;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let db = crate::db::Omnigraph::init(root, SCHEMA).await.unwrap();

        let txn = db.open_write_txn(None).await.unwrap();
        let lineage = db.new_lineage_intent_for_branch(None, None).await.unwrap();
        let entry = txn.base.entry("node:Person").unwrap().clone();
        let table_uri = format!("{}/{}", root.trim_end_matches('/'), entry.table_path);
        let stale_dataset = txn.base.open("node:Person").await.unwrap();
        let store = TableStore::new(root, Arc::new(lance::session::Session::default()));
        let staged = store
            .stage_append(
                &stale_dataset,
                person_batch(&[("stale-attempt", Some(99))]),
                &[],
            )
            .await
            .unwrap();
        let planned = staged.transaction_identity();
        assert_eq!(planned.read_version, entry.table_version);

        db.load(
            "main",
            r#"{"type":"Person","data":{"id":"winner","age":22}}"#,
            crate::loader::LoadMode::Append,
        )
        .await
        .unwrap();
        let winner_snapshot = db.snapshot_of("main").await.unwrap();
        let winner_version = winner_snapshot.entry("node:Person").unwrap().table_version;
        assert_eq!(winner_version, planned.read_version + 1);

        let authority = RecoveryAuthorityToken {
            branch_identifier: txn.authority.branch_identifier.clone(),
            graph_head: txn.authority.graph_head.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
        };
        let recovery_lineage = RecoveryLineageIntent {
            graph_commit_id: lineage.graph_commit_id,
            branch: lineage.branch,
            actor_id: lineage.actor_id,
            merged_parent_commit_id: lineage.merged_parent_commit_id,
            created_at: lineage.created_at,
        };
        let sidecar = new_occ_sidecar(
            SidecarKind::Mutation,
            None,
            None,
            vec![SidecarTablePin {
                table_key: "node:Person".to_string(),
                table_path: table_uri.clone(),
                expected_version: planned.read_version,
                post_commit_pin: planned.read_version + 1,
                confirmed_version: None,
                table_branch: entry.table_branch,
            }],
            authority,
            recovery_lineage,
            HashMap::from([("node:Person".to_string(), planned.clone())]),
        )
        .unwrap();
        write_sidecar(root, db.storage_adapter(), &sidecar)
            .await
            .unwrap();

        let (rebased, observed) = store
            .commit_staged_exact(Arc::new(stale_dataset), staged)
            .await
            .unwrap();
        assert_eq!(observed, planned);
        assert_eq!(
            rebased.version().version,
            winner_version + 1,
            "the stale transaction must reproduce Lance's expected+2 preflight rebase"
        );
        drop(rebased);

        // Model a first Full recovery pass that correctly compensates the
        // stale v3 effect to the CURRENT manifest pin v2, then crashes before
        // publishing the Restore commit. HEAD is now v4 / Restore(v2), while
        // __manifest still selects the winner at v2 and the sidecar remains.
        // Reopen must recognize that exact compensation and publish v4 instead
        // of treating the owned v3 effect as buried beneath a foreign write.
        restore_table_to_version(&table_uri, None, winner_version)
            .await
            .unwrap();
        let compensated = open_lance_head(
            &table_uri,
            None,
            Some((planned.read_version + 1, &planned, winner_version)),
        )
        .await
        .unwrap();
        assert_eq!(compensated.version, winner_version + 2);
        assert_eq!(
            compensated.effect_ownership,
            EffectOwnership::OwnCompensatedAtHead
        );

        drop(txn);
        drop(db);

        let recovered = crate::db::Omnigraph::open(root).await.unwrap();
        let snapshot = recovered.snapshot_of("main").await.unwrap();
        let visible = snapshot.open("node:Person").await.unwrap();
        let batches: Vec<RecordBatch> =
            futures::TryStreamExt::try_collect(visible.scan().try_into_stream().await.unwrap())
                .await
                .unwrap();
        let ids: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..ids.len())
                    .map(|row| ids.value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            ids,
            vec!["winner"],
            "recovery must preserve the published winner while removing only the stale effect"
        );

        let recovered_entry = snapshot.entry("node:Person").unwrap();
        let lance_head = Dataset::open(&table_uri).await.unwrap().version().version;
        assert_eq!(
            recovered_entry.table_version, lance_head,
            "recovery must publish its compensation so manifest and Lance HEAD converge"
        );
        assert!(
            list_sidecars(root, recovered.storage_adapter())
                .await
                .unwrap()
                .is_empty(),
            "the converged sidecar must be removed"
        );
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    #[serial_test::serial]
    async fn fixed_rollback_replays_partial_multi_table_audit_plan_after_crash() {
        const SCHEMA: &str = r#"
node Person { age: I32? }
node Company { age: I32? }
"#;
        let _scenario = fail::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let db = crate::db::Omnigraph::init(root, SCHEMA).await.unwrap();
        let txn = db.open_write_txn(None).await.unwrap();
        let lineage = db.new_lineage_intent_for_branch(None, None).await.unwrap();
        let store = TableStore::new(root, Arc::new(lance::session::Session::default()));

        let person_entry = txn.base.entry("node:Person").unwrap().clone();
        let company_entry = txn.base.entry("node:Company").unwrap().clone();
        let person_uri = format!("{}/{}", root.trim_end_matches('/'), person_entry.table_path);
        let company_uri = format!(
            "{}/{}",
            root.trim_end_matches('/'),
            company_entry.table_path
        );
        let person_ds = txn.base.open("node:Person").await.unwrap();
        let company_ds = txn.base.open("node:Company").await.unwrap();
        let person_staged = store
            .stage_append(&person_ds, person_batch(&[("partial", Some(10))]), &[])
            .await
            .unwrap();
        let company_staged = store
            .stage_append(&company_ds, person_batch(&[("untouched", Some(20))]), &[])
            .await
            .unwrap();
        let person_planned = person_staged.transaction_identity();
        let company_planned = company_staged.transaction_identity();

        let authority = RecoveryAuthorityToken {
            branch_identifier: txn.authority.branch_identifier.clone(),
            graph_head: txn.authority.graph_head.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
        };
        let recovery_lineage = RecoveryLineageIntent {
            graph_commit_id: lineage.graph_commit_id,
            branch: lineage.branch,
            actor_id: lineage.actor_id,
            merged_parent_commit_id: lineage.merged_parent_commit_id,
            created_at: lineage.created_at,
        };
        let sidecar = new_occ_sidecar(
            SidecarKind::Mutation,
            None,
            None,
            vec![
                SidecarTablePin {
                    table_key: "node:Person".to_string(),
                    table_path: person_uri,
                    expected_version: person_entry.table_version,
                    post_commit_pin: person_entry.table_version + 1,
                    confirmed_version: None,
                    table_branch: person_entry.table_branch,
                },
                SidecarTablePin {
                    table_key: "node:Company".to_string(),
                    table_path: company_uri,
                    expected_version: company_entry.table_version,
                    post_commit_pin: company_entry.table_version + 1,
                    confirmed_version: None,
                    table_branch: company_entry.table_branch,
                },
            ],
            authority,
            recovery_lineage,
            HashMap::from([
                ("node:Person".to_string(), person_planned.clone()),
                ("node:Company".to_string(), company_planned),
            ]),
        )
        .unwrap();
        let operation_id = sidecar.operation_id.clone();
        write_sidecar(root, db.storage_adapter(), &sidecar)
            .await
            .unwrap();

        let (person_committed, observed) = store
            .commit_staged_exact(Arc::new(person_ds), person_staged)
            .await
            .unwrap();
        assert_eq!(observed, person_planned);
        let person_effect_version = person_committed.version().version;
        assert_eq!(person_effect_version, person_entry.table_version + 1);
        drop(person_committed);
        drop(company_staged);
        drop(txn);
        drop(db);

        let failpoint = crate::failpoints::ScopedFailPoint::new(
            crate::failpoints::names::RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT,
            "return",
        );
        let error = crate::db::Omnigraph::open(root)
            .await
            .err()
            .expect("post-rollback failpoint must fail the first open");
        assert!(
            error
                .to_string()
                .contains("recovery.post_rollback_publish_pre_audit")
        );
        drop(failpoint);

        let persisted = list_sidecars(root, &ObjectStorageAdapter::local())
            .await
            .unwrap();
        assert_eq!(persisted.len(), 1);
        let planned_outcomes = persisted[0]
            .protocol_v3
            .as_ref()
            .unwrap()
            .rollback_audit_outcomes
            .as_ref()
            .expect("rollback audit plan must be durable before the publish");
        assert_eq!(
            planned_outcomes,
            &[TableOutcome {
                table_key: "node:Person".to_string(),
                from_version: person_effect_version,
                to_version: person_entry.table_version,
            }],
            "the untouched Company pin must not be fabricated into the audit plan"
        );

        let recovered = crate::db::Omnigraph::open(root).await.unwrap();
        let audit = RecoveryAudit::open(root).await.unwrap();
        let records = audit.list().await.unwrap();
        let record = records
            .iter()
            .find(|record| record.operation_id == operation_id)
            .expect("fixed rollback retry must append its audit row");
        assert_eq!(record.recovery_kind, RecoveryKind::RolledBack);
        assert_eq!(record.per_table_outcomes, planned_outcomes.clone());
        assert_eq!(
            recovered
                .snapshot_of("main")
                .await
                .unwrap()
                .open("node:Person")
                .await
                .unwrap()
                .count_rows(None)
                .await
                .unwrap(),
            0
        );
        assert!(
            list_sidecars(root, recovered.storage_adapter())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    #[serial_test::serial]
    async fn fixed_rollback_replays_preflight_rebase_audit_plan_after_crash() {
        const SCHEMA: &str = r#"
node Person { age: I32? }
"#;
        let _scenario = fail::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let db = crate::db::Omnigraph::init(root, SCHEMA).await.unwrap();

        let txn = db.open_write_txn(None).await.unwrap();
        let lineage = db.new_lineage_intent_for_branch(None, None).await.unwrap();
        let entry = txn.base.entry("node:Person").unwrap().clone();
        let table_uri = format!("{}/{}", root.trim_end_matches('/'), entry.table_path);
        let stale_dataset = txn.base.open("node:Person").await.unwrap();
        let store = TableStore::new(root, Arc::new(lance::session::Session::default()));
        let staged = store
            .stage_append(
                &stale_dataset,
                person_batch(&[("stale-attempt", Some(99))]),
                &[],
            )
            .await
            .unwrap();
        let planned = staged.transaction_identity();

        db.load(
            "main",
            r#"{"type":"Person","data":{"id":"winner","age":22}}"#,
            crate::loader::LoadMode::Append,
        )
        .await
        .unwrap();
        let winner_version = db
            .snapshot_of("main")
            .await
            .unwrap()
            .entry("node:Person")
            .unwrap()
            .table_version;
        assert_eq!(winner_version, planned.read_version + 1);

        let sidecar = new_occ_sidecar(
            SidecarKind::Mutation,
            None,
            None,
            vec![SidecarTablePin {
                table_key: "node:Person".to_string(),
                table_path: table_uri.clone(),
                expected_version: planned.read_version,
                post_commit_pin: planned.read_version + 1,
                confirmed_version: None,
                table_branch: entry.table_branch,
            }],
            RecoveryAuthorityToken {
                branch_identifier: txn.authority.branch_identifier.clone(),
                graph_head: txn.authority.graph_head.clone(),
                schema_ir_hash: txn.authority.schema_ir_hash.clone(),
                schema_identity_version: txn.authority.schema_identity_version,
            },
            RecoveryLineageIntent {
                graph_commit_id: lineage.graph_commit_id,
                branch: lineage.branch,
                actor_id: lineage.actor_id,
                merged_parent_commit_id: lineage.merged_parent_commit_id,
                created_at: lineage.created_at,
            },
            HashMap::from([("node:Person".to_string(), planned.clone())]),
        )
        .unwrap();
        let operation_id = sidecar.operation_id.clone();
        write_sidecar(root, db.storage_adapter(), &sidecar)
            .await
            .unwrap();

        let (rebased, observed) = store
            .commit_staged_exact(Arc::new(stale_dataset), staged)
            .await
            .unwrap();
        assert_eq!(observed, planned);
        let own_head = rebased.version().version;
        assert_eq!(own_head, winner_version + 1);
        drop(rebased);
        drop(txn);
        drop(db);

        let failpoint = crate::failpoints::ScopedFailPoint::new(
            crate::failpoints::names::RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT,
            "return",
        );
        let error = crate::db::Omnigraph::open(root)
            .await
            .err()
            .expect("post-rollback failpoint must fail the first open");
        assert!(
            error
                .to_string()
                .contains("recovery.post_rollback_publish_pre_audit")
        );
        drop(failpoint);

        let persisted = list_sidecars(root, &ObjectStorageAdapter::local())
            .await
            .unwrap();
        assert_eq!(persisted.len(), 1);
        let planned_outcomes = persisted[0]
            .protocol_v3
            .as_ref()
            .unwrap()
            .rollback_audit_outcomes
            .clone()
            .expect("preflight compensation must persist its exact audit plan");
        assert_eq!(
            planned_outcomes,
            vec![TableOutcome {
                table_key: "node:Person".to_string(),
                from_version: own_head,
                to_version: winner_version,
            }],
            "audit must report the actual v3 -> v2 compensation, never infer v2 -> v1 from pins"
        );

        let recovered = crate::db::Omnigraph::open(root).await.unwrap();
        let snapshot = recovered.snapshot_of("main").await.unwrap();
        let visible = snapshot.open("node:Person").await.unwrap();
        let batches: Vec<RecordBatch> = visible
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ids = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..ids.len())
                    .map(|row| ids.value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["winner"]);

        let audit = RecoveryAudit::open(root).await.unwrap();
        let records = audit.list().await.unwrap();
        let record = records
            .iter()
            .find(|record| record.operation_id == operation_id)
            .expect("fixed rollback retry must append its audit row");
        assert_eq!(record.recovery_kind, RecoveryKind::RolledBack);
        assert_eq!(record.per_table_outcomes, planned_outcomes);
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
            err.to_string()
                .contains("pins and publish updates diverged"),
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
