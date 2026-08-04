//! RFC-026 read-only stream status.
//!
//! Status answers two operator questions: *is this graph streaming, and what
//! state is each lane in?* The public manifest-only projection is deliberately
//! the least privileged surface in the streaming family — a pure projection of one `__manifest` snapshot,
//! authorized like other graph operational metadata rather than by
//! `stream_manage` (§4.6), and structurally incapable of moving a lifecycle:
//! it takes no admission lease, resolves no recovery, and publishes nothing.
//!
//! It is also the **compare-token source**. Every mutating management call
//! (fold, quiesce, resume, abort-drain) is compare-and-set against an expected
//! `lifecycle_revision`, and §4.6 requires status to expose the revision
//! callers pass back. Shipping it before those verbs is what lets them be
//! written as CAS from the start rather than retrofitted.
//!
//! `stream_status` remains the inexpensive manifest-only SDK projection.
//! F6b6 adds a separate checked operational cut for the one cluster-served
//! handle. Expensive token/base parity and immutable ledger proofs run first
//! against exact selected versions without blocking writers. A short second
//! phase closes every selected lane, reads the mutable Lance shard/recovery
//! witnesses without healing them, then rereads authority before release. It
//! remains engine-internal; F7b exposes only its graph-redacted projection.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use std::time::Duration;

use lance_index::mem_wal::ShardId;
use sha2::{Digest, Sha256};

use crate::db::manifest::stream::{
    DrainGoal, RetainedShardInventoryCommitment, StrictBlockEvidence, stream_graph_identity_digest,
};
use crate::db::manifest::stream_token::StreamTokenDisposition;
use crate::db::manifest::token_store::stream_token_lookup_index_coverage;
#[cfg(feature = "failpoints")]
use crate::db::manifest::token_store::{
    lookup_profile_management_receipt, lookup_stream_token_row,
};
use crate::db::manifest::{RecoverySidecar, StreamLifecycle, StreamProfileMode, TableIdentity};
#[cfg(feature = "failpoints")]
use crate::db::manifest::{
    stream_profile_cluster_root_digest, stream_profile_graph_identity_digest,
};
use crate::db::write_queue::StreamAdmissionKey;
use crate::db::{Omnigraph, ReadTarget, Snapshot};
use crate::error::{OmniError, Result};
use crate::storage::ListDirBounds;
use crate::table_store::mem_wal::{
    MemWalEnrollmentError, MemWalWorkerError, PassiveB1PhysicalState, ResidentWorkerMode,
    ResidentWorkerStatus, StreamWorkerKey, capture_current_head_witness,
    read_bound_merged_generation, validate_b1_lifecycle_current_binding_physical_state,
};

use super::stream_driver::{
    StreamFoldCompletionOutcome, StreamFoldDriverErrorKind, StreamFoldDriverRunState,
    StreamFoldDriverSnapshotScope,
};
use super::stream_profile::CheckedClusterApplyAuthority;

const STREAM_OPERATIONAL_STATUS_PREFLIGHT_DEADLINE: Duration = Duration::from_secs(60);
const STREAM_OPERATIONAL_STATUS_CUT_DEADLINE: Duration = Duration::from_secs(5);
const STREAM_OPERATIONAL_STATUS_TERMINAL_SAMPLE: usize = 8;
const STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_SIDECARS: usize = 256;
const STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_RESIDUE: usize = 256;
const STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_URI_BYTES: u64 = 4 * 1024 * 1024;
const STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_SIDECAR_BYTES: u64 = 32 * 1024 * 1024;
const STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_INVENTORY_BYTES: u64 = 32 * 1024 * 1024;

/// One lane's durable status, projected from the manifest lifecycle row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamTableStatus {
    /// Current public alias of the enrolled table. The row itself is keyed by
    /// immutable identity; status resolves that identity through this exact
    /// snapshot's live table registration rather than trusting the lifecycle's
    /// diagnostic alias, which may lag a metadata-only rename.
    pub table_key: String,
    pub stable_table_id: u64,
    pub table_incarnation_id: u64,
    /// `OPEN` | `DRAINING` | `SEALED`.
    pub lifecycle: &'static str,
    /// The compare token: every mutating management call passes the revision
    /// it expects, and a mismatch refuses without retargeting (§4.6).
    pub lifecycle_revision: u64,
    /// Exact logical stream incarnation. Unlike the physical enrollment id,
    /// this survives a same-table physical rebind and fences delayed requests
    /// from a prior logical stream incarnation.
    pub stream_incarnation_id: String,
    /// Current physical enrollment binding.
    pub enrollment_id: String,
    /// Authoritative per-shard epoch floor. This is durable authority, not the
    /// physically observed writer epoch, which needs the exclusive-cut read.
    pub epoch_floor_by_shard: Vec<(String, u64)>,
    /// Present only while `DRAINING`.
    pub drain_id: Option<String>,
    /// Present only while a fold is strict-blocked; the token addresses the
    /// block in a future correction call.
    pub strict_block_token: Option<String>,
    /// Outcome of the last fold this lane recorded, if any.
    pub last_fold_outcome: Option<String>,
    /// Graph commit the last fold published, when it published one.
    pub last_fold_graph_commit_id: Option<String>,
}

/// Graph-wide streaming status: the enablement flag plus one row per enrolled
/// table.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamStatus {
    /// Exact graph-global profile mode: `DISABLED`, `ENABLED`, `DISABLING`, or
    /// `RETIRED`.
    pub profile_mode: &'static str,
    /// Compatibility projection: true only for exact `ENABLED`.
    pub streaming_enabled: bool,
    /// Revision of the enablement row itself (distinct from per-lane
    /// `lifecycle_revision`).
    pub profile_revision: u64,
    /// Enrolled lanes, ordered by `table_key` so output is deterministic
    /// within a snapshot rather than hash-ordered.
    pub tables: Vec<StreamTableStatus>,
}

impl StreamStatus {
    /// True when any lane is non-terminal — the condition that makes a
    /// disable refuse as pending-until-drained, and that a future quiesce
    /// clears.
    pub fn undrained(&self) -> bool {
        self.tables
            .iter()
            .any(|table| table.lifecycle != StreamLifecycle::Sealed.as_str())
    }
}

/// Exact source of pending-generation accounting in the checked operational
/// cut. A cold replay tail is intentionally not represented as zero: Lance's
/// current read-only surface cannot count it without advancing a cursor hint
/// or claiming a writer.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamPendingGenerationStatus {
    Exact {
        generation: u64,
        rows: u64,
        arrow_bytes: u64,
        batches: usize,
        source: &'static str,
    },
    UnavailableColdReplay {
        generation: u64,
        reason: &'static str,
    },
    UnavailableFlushed {
        generation: u64,
        reason: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamShardOperationalStatus {
    pub shard_id: String,
    pub authoritative_epoch_floor: u64,
    pub observed_writer_epoch: u64,
    pub shard_manifest_version: u64,
    pub current_generation: u64,
    pub replay_after_wal_entry_position: u64,
    pub base_merged_generation: u64,
    pub pending: StreamPendingGenerationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamDrainOperationalStatus {
    pub drain_id: String,
    pub goal: &'static str,
    pub phase: &'static str,
    pub operation_expected_revision: u64,
    pub initiating_actor: String,
    pub initiated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamStrictBlockOperationalStatus {
    pub block_token: String,
    pub correction_revision: u64,
    pub kind: &'static str,
    pub violation_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamReceiptHeadsStatus {
    pub enrollment_request_id: String,
    pub current_binding_receipt_id: String,
    pub current_claim_receipt_id: Option<String>,
    pub binding_receipt_count: u64,
    pub management_receipt_count: u64,
    pub claim_receipt_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamLastFoldOperationalStatus {
    pub operation_id: String,
    pub outcome: String,
    pub graph_commit_id: Option<String>,
    pub generation: u64,
    pub input_rows: u64,
    pub input_bytes: u64,
    pub visible_rows: u64,
    pub visible_bytes: u64,
    pub recorded_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamTableOperationalStatus {
    pub durable: StreamTableStatus,
    pub stream_config_version: u32,
    pub stream_config_hash: String,
    pub binding_scope_id: String,
    pub receipts: StreamReceiptHeadsStatus,
    pub drain: Option<StreamDrainOperationalStatus>,
    pub strict_block: Option<StreamStrictBlockOperationalStatus>,
    pub last_fold: Option<StreamLastFoldOperationalStatus>,
    pub physical: StreamTablePhysicalOperationalStatus,
}

/// Physical observation is either one coherent shard cut or explicitly
/// unavailable because a listed recovery owner exactly binds the moved live
/// HEAD to its canonical-main participant outcome. Status never heals that
/// owner and never disguises the unavailable cut as zero/current state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamTablePhysicalOperationalStatus {
    Observed {
        shards: Vec<StreamShardOperationalStatus>,
    },
    UnavailableRecovery {
        operation_ids: Vec<String>,
        reason: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamTerminalTokenOperationalStatus {
    pub stable_table_id: u64,
    pub table_incarnation_id: u64,
    pub logical_id: String,
    pub disposition: &'static str,
    pub current_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamTokenIndexCoverageStatus {
    Known {
        total_fragments: u64,
        covered_fragments: u64,
        uncovered_fragments: u64,
    },
    Unknown {
        total_fragments: u64,
        reason: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamOldestUncoveredAgeStatus {
    NotApplicable,
    Unavailable { reason: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamTokenLedgerOperationalStatus {
    pub selected_version: u64,
    pub selected_transaction_uuid: String,
    pub present_count: u64,
    pub withdrawn_count: u64,
    pub dead_lettered_count: u64,
    pub terminal_sample: Vec<StreamTerminalTokenOperationalStatus>,
    pub terminal_sample_has_more: bool,
    pub lookup_index_coverage: StreamTokenIndexCoverageStatus,
    pub oldest_uncovered_age: StreamOldestUncoveredAgeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamRecoveryOperationalStatus {
    pub operation_id: String,
    pub operation_kind: &'static str,
    pub schema_version: u32,
    pub started_at: String,
    pub branch: Option<String>,
    /// Digest of the complete validated sidecar, so an in-place phase change
    /// is detected even when the operation id remains stable.
    pub state_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamDriverPendingStatus {
    pub stable_table_id: u64,
    pub table_incarnation_id: u64,
    pub trigger_sequence: u64,
    pub consecutive_failures: u32,
    pub due_in_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamDriverEventStatus {
    pub event_sequence: u64,
    pub stable_table_id: Option<u64>,
    pub table_incarnation_id: Option<u64>,
    pub trigger_sequence: Option<u64>,
    pub kind: &'static str,
    pub retry_in_ms: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamDriverAdvisoryStatus {
    pub scope: &'static str,
    pub authoritative: bool,
    pub state: &'static str,
    pub pending_triggers: Vec<StreamDriverPendingStatus>,
    pub published_open_folds: u64,
    pub last_completion: Option<StreamDriverEventStatus>,
    pub last_error: Option<StreamDriverEventStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamRebuildBlockReason {
    ProfileNotTerminal {
        mode: &'static str,
    },
    LaneNotSealed {
        table_key: String,
    },
    StrictBlock {
        table_key: String,
    },
    PendingGeneration {
        table_key: String,
    },
    PendingGenerationUnavailable {
        table_key: String,
    },
    RelevantRecovery {
        operation_id: String,
    },
    TerminalTokenAuthority {
        withdrawn_count: u64,
        dead_lettered_count: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamRebuildReadiness {
    pub ready: bool,
    pub reasons: Vec<StreamRebuildBlockReason>,
}

/// One coherent authority-plus-observation cut. This value is diagnostic only:
/// later admission and rebuild independently rerun every authority proof.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamOperationalStatus {
    pub cut_manifest_version: u64,
    pub durable: StreamStatus,
    pub tables: Vec<StreamTableOperationalStatus>,
    pub token_ledger: StreamTokenLedgerOperationalStatus,
    pub relevant_recovery: Vec<StreamRecoveryOperationalStatus>,
    pub driver: StreamDriverAdvisoryStatus,
    pub rebuild: StreamRebuildReadiness,
}

/// Graph-logical declaration selected by one checked stream status cut.
///
/// This is deliberately the only declaration identity carried across the
/// served boundary. The physical table identity and current table key remain
/// inside the engine.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GraphStreamDeclaration {
    /// `node` or `edge`.
    pub kind: &'static str,
    /// Current accepted-schema name from the exact status cut.
    pub type_name: String,
}

/// Logical drain state for one graph declaration.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamDrainStatus {
    pub goal: &'static str,
    pub phase: &'static str,
    pub initiated_at: i64,
}

/// Logical strict-block state for one graph declaration.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamStrictBlockStatus {
    pub kind: &'static str,
    pub violation_code: String,
}

/// Aggregate pending work for one graph declaration.
///
/// Exact counts are returned only when every selected physical member has an
/// exact observation. An unavailable member makes the aggregate unavailable;
/// the booleans retain the actionable class without exposing a shard,
/// generation, recovery owner, or storage coordinate.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphStreamPendingStatus {
    Exact {
        rows: u64,
        arrow_bytes: u64,
        batches: u64,
    },
    Unavailable {
        cold_replay: bool,
        flushed: bool,
        recovery: bool,
    },
}

/// Last graph-visible fold summary, with physical generation and operation
/// identities intentionally removed.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamLastFoldStatus {
    pub outcome: String,
    pub input_rows: u64,
    pub input_bytes: u64,
    pub visible_rows: u64,
    pub visible_bytes: u64,
    pub recorded_at: i64,
}

/// Checked operational status for one accepted graph declaration.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamDeclarationStatus {
    pub declaration: GraphStreamDeclaration,
    pub lifecycle: &'static str,
    pub lifecycle_revision: u64,
    pub drain: Option<GraphStreamDrainStatus>,
    pub strict_block: Option<GraphStreamStrictBlockStatus>,
    pub pending: GraphStreamPendingStatus,
    pub last_fold: Option<GraphStreamLastFoldStatus>,
}

/// Graph-global current sequencing-authority counts.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamTokenCounts {
    pub present: u64,
    pub withdrawn: u64,
    pub dead_lettered: u64,
}

/// Sanitized process-local driver error summary.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamDriverErrorStatus {
    pub kind: &'static str,
    pub retry_in_ms: Option<u64>,
}

/// Advisory driver summary. It intentionally carries no trigger, event, or
/// declaration identity and no raw error message.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamDriverStatus {
    pub scope: &'static str,
    pub authoritative: bool,
    pub state: &'static str,
    pub pending_count: u64,
    pub published_open_folds: u64,
    pub last_completion_kind: Option<&'static str>,
    pub last_error: Option<GraphStreamDriverErrorStatus>,
}

/// Graph-safe reason a logical rebuild is not currently admissible.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphStreamRebuildBlocker {
    ProfileNotTerminal,
    DeclarationNotSealed {
        declaration: GraphStreamDeclaration,
    },
    StrictBlock {
        declaration: GraphStreamDeclaration,
    },
    PendingWork {
        declaration: GraphStreamDeclaration,
    },
    PendingWorkUnavailable {
        declaration: GraphStreamDeclaration,
    },
    RecoveryPending {
        count: u64,
    },
    TerminalTokenAuthority {
        withdrawn_count: u64,
        dead_lettered_count: u64,
    },
}

/// Graph-safe rebuild decision from the checked status cut.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamRebuildStatus {
    pub ready: bool,
    pub blockers: Vec<GraphStreamRebuildBlocker>,
}

/// One checked, graph-redacted operational status cut for the served F7
/// boundary.
///
/// The richer internal value remains private because it contains table,
/// binding, shard, generation, Lance-version, receipt, and recovery identities.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStreamOperationalStatus {
    pub manifest_version: u64,
    pub profile_mode: &'static str,
    pub profile_revision: u64,
    pub enrolled_declarations: Vec<GraphStreamDeclarationStatus>,
    pub token_counts: GraphStreamTokenCounts,
    pub recovery_pending_count: u64,
    pub driver: GraphStreamDriverStatus,
    pub rebuild: GraphStreamRebuildStatus,
}

fn graph_stream_declaration(table_key: &str) -> Result<GraphStreamDeclaration> {
    let (kind, type_name) = if let Some(type_name) = table_key.strip_prefix("node:") {
        ("node", type_name)
    } else if let Some(type_name) = table_key.strip_prefix("edge:") {
        ("edge", type_name)
    } else {
        return Err(OmniError::manifest_internal(format!(
            "checked graph stream status found invalid table key '{table_key}'"
        )));
    };
    if type_name.is_empty() {
        return Err(OmniError::manifest_internal(
            "checked graph stream status found an empty declaration name",
        ));
    }
    Ok(GraphStreamDeclaration {
        kind,
        type_name: type_name.to_string(),
    })
}

fn checked_graph_status_add(total: &mut u64, value: u64, label: &str) -> Result<()> {
    *total = total.checked_add(value).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "checked graph stream status overflowed aggregate {label}"
        ))
    })?;
    Ok(())
}

fn graph_stream_pending_status(
    physical: &StreamTablePhysicalOperationalStatus,
) -> Result<GraphStreamPendingStatus> {
    let StreamTablePhysicalOperationalStatus::Observed { shards } = physical else {
        return Ok(GraphStreamPendingStatus::Unavailable {
            cold_replay: false,
            flushed: false,
            recovery: true,
        });
    };
    if shards.is_empty() {
        return Err(OmniError::manifest_internal(
            "checked graph stream status found no physical member for an enrolled declaration",
        ));
    }

    let mut rows = 0_u64;
    let mut arrow_bytes = 0_u64;
    let mut batches = 0_u64;
    let mut cold_replay = false;
    let mut flushed = false;
    for shard in shards {
        match shard.pending {
            StreamPendingGenerationStatus::Exact {
                rows: member_rows,
                arrow_bytes: member_arrow_bytes,
                batches: member_batches,
                ..
            } => {
                checked_graph_status_add(&mut rows, member_rows, "pending rows")?;
                checked_graph_status_add(
                    &mut arrow_bytes,
                    member_arrow_bytes,
                    "pending Arrow bytes",
                )?;
                let member_batches = u64::try_from(member_batches).map_err(|_| {
                    OmniError::manifest_internal(
                        "checked graph stream status could not represent pending batch count",
                    )
                })?;
                checked_graph_status_add(&mut batches, member_batches, "pending batches")?;
            }
            StreamPendingGenerationStatus::UnavailableColdReplay { .. } => cold_replay = true,
            StreamPendingGenerationStatus::UnavailableFlushed { .. } => flushed = true,
        }
    }
    if cold_replay || flushed {
        Ok(GraphStreamPendingStatus::Unavailable {
            cold_replay,
            flushed,
            recovery: false,
        })
    } else {
        Ok(GraphStreamPendingStatus::Exact {
            rows,
            arrow_bytes,
            batches,
        })
    }
}

fn graph_stream_declaration_status(
    table: &StreamTableOperationalStatus,
) -> Result<GraphStreamDeclarationStatus> {
    Ok(GraphStreamDeclarationStatus {
        declaration: graph_stream_declaration(&table.durable.table_key)?,
        lifecycle: table.durable.lifecycle,
        lifecycle_revision: table.durable.lifecycle_revision,
        drain: table.drain.as_ref().map(|drain| GraphStreamDrainStatus {
            goal: drain.goal,
            phase: drain.phase,
            initiated_at: drain.initiated_at,
        }),
        strict_block: table
            .strict_block
            .as_ref()
            .map(|block| GraphStreamStrictBlockStatus {
                kind: block.kind,
                violation_code: block.violation_code.clone(),
            }),
        pending: graph_stream_pending_status(&table.physical)?,
        last_fold: table
            .last_fold
            .as_ref()
            .map(|fold| GraphStreamLastFoldStatus {
                outcome: fold.outcome.clone(),
                input_rows: fold.input_rows,
                input_bytes: fold.input_bytes,
                visible_rows: fold.visible_rows,
                visible_bytes: fold.visible_bytes,
                recorded_at: fold.recorded_at,
            }),
    })
}

fn graph_stream_rebuild_status(
    rebuild: StreamRebuildReadiness,
    declarations_by_table_key: &BTreeMap<String, GraphStreamDeclaration>,
    recovery_pending_count: u64,
) -> Result<GraphStreamRebuildStatus> {
    let declaration = |table_key: &str| {
        declarations_by_table_key
            .get(table_key)
            .cloned()
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "checked graph stream rebuild reason referenced unknown declaration '{table_key}'"
                ))
            })
    };
    let mut blockers = Vec::with_capacity(rebuild.reasons.len());
    let mut declaration_reasons = BTreeSet::new();
    let mut recovery_reason_count = 0_u64;
    for reason in rebuild.reasons {
        let projected = match reason {
            StreamRebuildBlockReason::ProfileNotTerminal { .. } => {
                Some(GraphStreamRebuildBlocker::ProfileNotTerminal)
            }
            StreamRebuildBlockReason::LaneNotSealed { table_key } => declaration_reasons
                .insert(("not_sealed", table_key.clone()))
                .then(|| declaration(&table_key))
                .transpose()?
                .map(|declaration| GraphStreamRebuildBlocker::DeclarationNotSealed { declaration }),
            StreamRebuildBlockReason::StrictBlock { table_key } => declaration_reasons
                .insert(("strict_block", table_key.clone()))
                .then(|| declaration(&table_key))
                .transpose()?
                .map(|declaration| GraphStreamRebuildBlocker::StrictBlock { declaration }),
            StreamRebuildBlockReason::PendingGeneration { table_key } => declaration_reasons
                .insert(("pending", table_key.clone()))
                .then(|| declaration(&table_key))
                .transpose()?
                .map(|declaration| GraphStreamRebuildBlocker::PendingWork { declaration }),
            StreamRebuildBlockReason::PendingGenerationUnavailable { table_key } => {
                declaration_reasons
                    .insert(("pending_unavailable", table_key.clone()))
                    .then(|| declaration(&table_key))
                    .transpose()?
                    .map(
                        |declaration| GraphStreamRebuildBlocker::PendingWorkUnavailable {
                            declaration,
                        },
                    )
            }
            StreamRebuildBlockReason::RelevantRecovery { .. } => {
                recovery_reason_count = recovery_reason_count.checked_add(1).ok_or_else(|| {
                    OmniError::manifest_internal(
                        "checked graph stream status overflowed recovery blocker count",
                    )
                })?;
                if recovery_reason_count == 1 {
                    Some(GraphStreamRebuildBlocker::RecoveryPending {
                        count: recovery_pending_count,
                    })
                } else {
                    None
                }
            }
            StreamRebuildBlockReason::TerminalTokenAuthority {
                withdrawn_count,
                dead_lettered_count,
            } => Some(GraphStreamRebuildBlocker::TerminalTokenAuthority {
                withdrawn_count,
                dead_lettered_count,
            }),
        };
        if let Some(projected) = projected {
            blockers.push(projected);
        }
    }
    if recovery_reason_count != recovery_pending_count {
        return Err(OmniError::manifest_internal(format!(
            "checked graph stream status found {recovery_pending_count} recovery operation(s) but {recovery_reason_count} rebuild blocker(s)"
        )));
    }
    Ok(GraphStreamRebuildStatus {
        ready: rebuild.ready,
        blockers,
    })
}

fn project_graph_stream_operational_status(
    status: StreamOperationalStatus,
) -> Result<GraphStreamOperationalStatus> {
    let StreamOperationalStatus {
        cut_manifest_version,
        durable,
        tables,
        token_ledger,
        relevant_recovery,
        driver,
        rebuild,
    } = status;
    let enrolled_declarations = tables
        .iter()
        .map(graph_stream_declaration_status)
        .collect::<Result<Vec<_>>>()?;
    let declarations_by_table_key = tables
        .iter()
        .zip(&enrolled_declarations)
        .map(|(table, declaration)| {
            (
                table.durable.table_key.clone(),
                declaration.declaration.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if declarations_by_table_key.len() != enrolled_declarations.len() {
        return Err(OmniError::manifest_internal(
            "checked graph stream status found duplicate declaration aliases",
        ));
    }
    let recovery_pending_count = u64::try_from(relevant_recovery.len()).map_err(|_| {
        OmniError::manifest_internal(
            "checked graph stream status could not represent recovery operation count",
        )
    })?;
    let pending_count = u64::try_from(driver.pending_triggers.len()).map_err(|_| {
        OmniError::manifest_internal(
            "checked graph stream status could not represent driver pending count",
        )
    })?;
    let rebuild =
        graph_stream_rebuild_status(rebuild, &declarations_by_table_key, recovery_pending_count)?;
    Ok(GraphStreamOperationalStatus {
        manifest_version: cut_manifest_version,
        profile_mode: durable.profile_mode,
        profile_revision: durable.profile_revision,
        enrolled_declarations,
        token_counts: GraphStreamTokenCounts {
            present: token_ledger.present_count,
            withdrawn: token_ledger.withdrawn_count,
            dead_lettered: token_ledger.dead_lettered_count,
        },
        recovery_pending_count,
        driver: GraphStreamDriverStatus {
            scope: driver.scope,
            authoritative: driver.authoritative,
            state: driver.state,
            pending_count,
            published_open_folds: driver.published_open_folds,
            last_completion_kind: driver.last_completion.as_ref().map(|event| event.kind),
            last_error: driver
                .last_error
                .as_ref()
                .map(|error| GraphStreamDriverErrorStatus {
                    kind: error.kind,
                    retry_in_ms: error.retry_in_ms,
                }),
        },
        rebuild,
    })
}

/// Project every durable field from one already-resolved immutable snapshot.
///
/// Keeping the projection behind one snapshot parameter makes the atomicity
/// boundary structural: callers cannot accidentally source the profile,
/// lifecycle compare tokens, or current aliases from separate manifest reads.
fn project_stream_status(snapshot: &Snapshot) -> Result<StreamStatus> {
    let profile = snapshot.stream_profile();
    let table_keys_by_identity = snapshot
        .entries()
        .map(|entry| (entry.identity, entry.table_key.as_str()))
        .collect::<HashMap<_, _>>();

    let mut tables: Vec<StreamTableStatus> = snapshot
        .stream_lifecycles()
        .map(|(identity, lifecycle)| {
            let table_key = table_keys_by_identity.get(identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "stream status found lifecycle authority for non-live table identity {identity}"
                ))
            })?;
            Ok(StreamTableStatus {
                table_key: (*table_key).to_string(),
                stable_table_id: identity.stable_table_id,
                table_incarnation_id: identity.table_incarnation_id,
                lifecycle: lifecycle.lifecycle.as_str(),
                lifecycle_revision: lifecycle.lifecycle_revision,
                stream_incarnation_id: lifecycle.enrollment_receipt.stream_incarnation_id.clone(),
                enrollment_id: lifecycle.binding.enrollment_id.clone(),
                epoch_floor_by_shard: lifecycle
                    .epoch_floor_by_shard
                    .iter()
                    .map(|(shard, floor)| (shard.clone(), *floor))
                    .collect(),
                drain_id: lifecycle.drain.as_ref().map(|drain| drain.drain_id.clone()),
                strict_block_token: lifecycle
                    .strict_block
                    .as_ref()
                    .map(|block| block.block_token.clone()),
                last_fold_outcome: lifecycle
                    .last_fold_summary
                    .as_ref()
                    .map(|summary| summary.outcome.as_str().to_string()),
                last_fold_graph_commit_id: lifecycle
                    .last_fold_summary
                    .as_ref()
                    .and_then(|summary| summary.graph_commit_id.clone()),
            })
        })
        .collect::<Result<_>>()?;
    // Deterministic output within a snapshot: the source is a HashMap, and
    // hash-map iteration order is never allowed to reach a result surface.
    tables.sort_by(|a, b| {
        a.table_key
            .cmp(&b.table_key)
            .then(a.stable_table_id.cmp(&b.stable_table_id))
            .then(a.table_incarnation_id.cmp(&b.table_incarnation_id))
    });

    Ok(StreamStatus {
        profile_mode: profile.mode().as_str(),
        streaming_enabled: profile.streaming_enabled(),
        profile_revision: profile.profile_revision,
        tables,
    })
}

fn recovery_status(sidecars: &[RecoverySidecar]) -> Result<Vec<StreamRecoveryOperationalStatus>> {
    let mut projected = sidecars
        .iter()
        // A whole-graph rebuild must settle every pending writer, including an
        // ordinary mutation on an unenrolled table or a named branch. Keep the
        // inventory complete inside the caller-enforced status envelope;
        // per-table physical unavailability below requires an exact
        // canonical-main participant outcome.
        .map(|sidecar| {
            let encoded = serde_json::to_vec(sidecar).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "failed to encode validated relevant recovery sidecar for status: {error}"
                ))
            })?;
            let state_digest = format!("sha256:{:x}", Sha256::digest(encoded));
            Ok(StreamRecoveryOperationalStatus {
                operation_id: sidecar.operation_id.clone(),
                operation_kind: sidecar.writer_kind.as_str(),
                schema_version: sidecar.schema_version,
                started_at: sidecar.started_at.clone(),
                branch: sidecar.branch.clone(),
                state_digest,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    projected.sort_by(|a, b| {
        a.operation_id
            .cmp(&b.operation_id)
            .then(a.state_digest.cmp(&b.state_digest))
    });
    Ok(projected)
}

async fn list_operational_status_sidecars(db: &Omnigraph) -> Result<Vec<RecoverySidecar>> {
    crate::db::manifest::list_sidecars_bounded(
        db.root_uri(),
        db.storage_adapter(),
        ListDirBounds {
            max_matching_entries: STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_SIDECARS,
            max_irrelevant_entries: STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_RESIDUE,
            max_uri_bytes: STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_URI_BYTES,
        },
        STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_SIDECAR_BYTES,
        STREAM_OPERATIONAL_STATUS_MAX_RECOVERY_INVENTORY_BYTES,
    )
    .await
}

async fn recovery_operations_for_table(
    sidecars: &[RecoverySidecar],
    identity: TableIdentity,
    table_path: &str,
    selected_head: &crate::db::manifest::CurrentHeadWitness,
    observed_head: &crate::db::manifest::CurrentHeadWitness,
    dataset: &lance::Dataset,
) -> Result<Vec<String>> {
    let mut operations = Vec::new();
    for sidecar in sidecars {
        if crate::db::manifest::recovery_sidecar_exactly_owns_canonical_main_base_head(
            sidecar,
            identity,
            table_path,
            selected_head,
            observed_head,
            dataset,
        )
        .await?
        {
            operations.push(sidecar.operation_id.clone());
        }
    }
    operations.sort();
    operations.dedup();
    Ok(operations)
}

fn driver_run_state(state: StreamFoldDriverRunState) -> &'static str {
    match state {
        StreamFoldDriverRunState::Stopped => "STOPPED",
        StreamFoldDriverRunState::Running => "RUNNING",
        StreamFoldDriverRunState::Stopping => "STOPPING",
        StreamFoldDriverRunState::Failed => "FAILED",
    }
}

fn driver_completion_kind(outcome: StreamFoldCompletionOutcome) -> &'static str {
    match outcome {
        StreamFoldCompletionOutcome::PublishedOpenFold => "PUBLISHED_OPEN_FOLD",
        StreamFoldCompletionOutcome::Idle => "IDLE",
        StreamFoldCompletionOutcome::Inactive => "INACTIVE",
        StreamFoldCompletionOutcome::Sealed => "SEALED",
        StreamFoldCompletionOutcome::NoLongerEligible => "NO_LONGER_ELIGIBLE",
    }
}

fn driver_error_kind(kind: StreamFoldDriverErrorKind) -> &'static str {
    match kind {
        StreamFoldDriverErrorKind::RetryScheduled => "RETRY_SCHEDULED",
        StreamFoldDriverErrorKind::StaleAttemptFailed => "STALE_ATTEMPT_FAILED",
        StreamFoldDriverErrorKind::DataBlocked => "DATA_BLOCKED",
        StreamFoldDriverErrorKind::TriggerSequenceOverflow => "TRIGGER_SEQUENCE_OVERFLOW",
        StreamFoldDriverErrorKind::UnexpectedStop => "UNEXPECTED_STOP",
    }
}

fn project_driver_status(db: &Omnigraph) -> StreamDriverAdvisoryStatus {
    let snapshot = db.stream_fold_driver.snapshot();
    let scope = match snapshot.scope {
        StreamFoldDriverSnapshotScope::ProcessLocalAdvisory => "PROCESS_LOCAL_ADVISORY",
    };
    let pending_triggers = snapshot
        .pending_triggers
        .into_iter()
        .map(|pending| StreamDriverPendingStatus {
            stable_table_id: pending.table_identity.stable_table_id,
            table_incarnation_id: pending.table_identity.table_incarnation_id,
            trigger_sequence: pending.trigger_sequence,
            consecutive_failures: pending.consecutive_failures,
            due_in_ms: pending.due_in_ms,
        })
        .collect();
    let last_completion = snapshot
        .last_completion
        .map(|completion| StreamDriverEventStatus {
            event_sequence: completion.event_sequence,
            stable_table_id: Some(completion.table_identity.stable_table_id),
            table_incarnation_id: Some(completion.table_identity.table_incarnation_id),
            trigger_sequence: Some(completion.trigger_sequence),
            kind: driver_completion_kind(completion.outcome),
            retry_in_ms: None,
            message: None,
        });
    let last_error = snapshot.last_error.map(|error| StreamDriverEventStatus {
        event_sequence: error.event_sequence,
        stable_table_id: error
            .table_identity
            .map(|identity| identity.stable_table_id),
        table_incarnation_id: error
            .table_identity
            .map(|identity| identity.table_incarnation_id),
        trigger_sequence: error.trigger_sequence,
        kind: driver_error_kind(error.kind),
        retry_in_ms: error.retry_in_ms,
        message: Some(error.message),
    });
    StreamDriverAdvisoryStatus {
        scope,
        authoritative: snapshot.authoritative,
        state: driver_run_state(snapshot.state),
        pending_triggers,
        published_open_folds: snapshot.published_open_folds,
        last_completion,
        last_error,
    }
}

async fn project_token_ledger_status(
    db: &Omnigraph,
    snapshot: &Snapshot,
) -> Result<StreamTokenLedgerOperationalStatus> {
    let authority = snapshot.stream_token_authority();
    let dataset = snapshot.open_stream_token_authority().await?;
    let parity = super::stream_retirement::inspect_current_token_base_parity(
        db,
        snapshot,
        STREAM_OPERATIONAL_STATUS_TERMINAL_SAMPLE,
    )
    .await?;
    let terminal_sample = parity
        .terminal_sample
        .into_iter()
        .map(|row| StreamTerminalTokenOperationalStatus {
            stable_table_id: row.identity.stable_table_id,
            table_incarnation_id: row.identity.table_incarnation_id,
            logical_id: row.logical_id,
            disposition: match row.disposition {
                StreamTokenDisposition::Present => "PRESENT",
                StreamTokenDisposition::Withdrawn => "WITHDRAWN",
                StreamTokenDisposition::DeadLettered => "DEAD_LETTERED",
            },
            current_token: row.current_token.to_string(),
        })
        .collect();
    let (total_fragments, uncovered_fragments) =
        stream_token_lookup_index_coverage(&dataset, authority).await?;
    let (lookup_index_coverage, oldest_uncovered_age) = match uncovered_fragments {
        Some(uncovered_fragments) => {
            let covered_fragments = total_fragments
                .checked_sub(uncovered_fragments)
                .ok_or_else(|| {
                    OmniError::manifest_internal("token lookup-index coverage exceeds fragments")
                })?;
            let age = if uncovered_fragments == 0 {
                StreamOldestUncoveredAgeStatus::NotApplicable
            } else {
                StreamOldestUncoveredAgeStatus::Unavailable {
                    reason: "Lance exposes no exact fragment creation timestamp on the selected cut",
                }
            };
            (
                StreamTokenIndexCoverageStatus::Known {
                    total_fragments,
                    covered_fragments,
                    uncovered_fragments,
                },
                age,
            )
        }
        None => (
            StreamTokenIndexCoverageStatus::Unknown {
                total_fragments,
                reason: "lookup index is absent or a segment omits its fragment bitmap",
            },
            StreamOldestUncoveredAgeStatus::Unavailable {
                reason: "uncovered fragment set is unknown",
            },
        ),
    };
    Ok(StreamTokenLedgerOperationalStatus {
        selected_version: authority.current_head_witness.table_version,
        selected_transaction_uuid: authority.current_head_witness.transaction_uuid.clone(),
        present_count: parity.present_count,
        withdrawn_count: parity.withdrawn_count,
        dead_lettered_count: parity.dead_lettered_count,
        terminal_sample,
        terminal_sample_has_more: parity.terminal_sample_has_more,
        lookup_index_coverage,
        oldest_uncovered_age,
    })
}

fn rebuild_readiness(
    durable: &StreamStatus,
    tables: &[StreamTableOperationalStatus],
    token: &StreamTokenLedgerOperationalStatus,
    recovery: &[StreamRecoveryOperationalStatus],
) -> StreamRebuildReadiness {
    let mut reasons = Vec::new();
    if !matches!(durable.profile_mode, "DISABLED" | "RETIRED") {
        reasons.push(StreamRebuildBlockReason::ProfileNotTerminal {
            mode: durable.profile_mode,
        });
    }
    for table in tables {
        if table.durable.lifecycle != "SEALED" {
            reasons.push(StreamRebuildBlockReason::LaneNotSealed {
                table_key: table.durable.table_key.clone(),
            });
        }
        if table.strict_block.is_some() {
            reasons.push(StreamRebuildBlockReason::StrictBlock {
                table_key: table.durable.table_key.clone(),
            });
        }
        if let StreamTablePhysicalOperationalStatus::Observed { shards } = &table.physical {
            for shard in shards {
                match shard.pending {
                    StreamPendingGenerationStatus::Exact {
                        rows,
                        arrow_bytes,
                        batches,
                        ..
                    } if rows != 0 || arrow_bytes != 0 || batches != 0 => {
                        reasons.push(StreamRebuildBlockReason::PendingGeneration {
                            table_key: table.durable.table_key.clone(),
                        });
                    }
                    StreamPendingGenerationStatus::UnavailableColdReplay { .. }
                    | StreamPendingGenerationStatus::UnavailableFlushed { .. } => {
                        reasons.push(StreamRebuildBlockReason::PendingGenerationUnavailable {
                            table_key: table.durable.table_key.clone(),
                        });
                    }
                    StreamPendingGenerationStatus::Exact { .. } => {}
                }
            }
        }
    }
    reasons.extend(
        recovery
            .iter()
            .map(|operation| StreamRebuildBlockReason::RelevantRecovery {
                operation_id: operation.operation_id.clone(),
            }),
    );
    if durable.profile_mode != StreamProfileMode::Retired.as_str()
        && (token.withdrawn_count != 0 || token.dead_lettered_count != 0)
    {
        reasons.push(StreamRebuildBlockReason::TerminalTokenAuthority {
            withdrawn_count: token.withdrawn_count,
            dead_lettered_count: token.dead_lettered_count,
        });
    }
    StreamRebuildReadiness {
        ready: reasons.is_empty(),
        reasons,
    }
}

fn enrollment_observation_error(error: MemWalEnrollmentError) -> OmniError {
    match error {
        MemWalEnrollmentError::Lance { .. } => OmniError::Lance(error.to_string()),
        other => OmniError::manifest_internal(other.to_string()),
    }
}

fn worker_observation_error(error: MemWalWorkerError) -> OmniError {
    match error {
        MemWalWorkerError::ResourceLimit {
            resource,
            limit,
            actual,
        } => OmniError::ResourceLimitExceeded {
            resource: resource.to_string(),
            limit,
            actual,
        },
        MemWalWorkerError::Lance { .. } => OmniError::Lance(error.to_string()),
        MemWalWorkerError::Retiring { .. } => OmniError::StreamStatusBusy {
            phase: error.to_string(),
        },
        other => OmniError::manifest_internal(other.to_string()),
    }
}

fn unavailable_flushed_accounting(
    flushed: &crate::table_store::mem_wal::FlushedGenerationState,
) -> StreamPendingGenerationStatus {
    StreamPendingGenerationStatus::UnavailableFlushed {
        generation: flushed.generation,
        reason: "Lance exposes the flushed generation's LWW winner projection, not its original admitted row/byte/batch accounting",
    }
}

fn sealed_pending_status(
    lifecycle: &crate::db::manifest::StreamLifecycleEntry,
    shard_manifest_version: u64,
    observed_writer_epoch: u64,
    current_generation: u64,
    replay_after_wal_entry_position: u64,
    base_merged_generation: u64,
) -> Result<Option<StreamPendingGenerationStatus>> {
    if lifecycle.lifecycle != StreamLifecycle::Sealed {
        return Ok(None);
    }
    let proof = lifecycle.sealed_proof.as_ref().ok_or_else(|| {
        OmniError::manifest_internal("SEALED stream lifecycle has no verified-empty proof")
    })?;
    if proof.shard_manifest_version != shard_manifest_version
        || proof.writer_epoch != observed_writer_epoch
        || proof.current_generation != current_generation
        || proof.replay_cursor != replay_after_wal_entry_position
        || proof.base_merged_generation != base_merged_generation
    {
        return Err(OmniError::manifest_internal(
            "SEALED verified-empty proof differs from the observed physical cut",
        ));
    }
    Ok(Some(StreamPendingGenerationStatus::Exact {
        generation: current_generation,
        rows: 0,
        arrow_bytes: 0,
        batches: 0,
        source: "VERIFIED_EMPTY_SEALED_PROOF",
    }))
}

fn manifest_selected_worker_key(
    identity: TableIdentity,
    lifecycle: &crate::db::manifest::StreamLifecycleEntry,
) -> Result<StreamWorkerKey> {
    let [shard_id_text] = lifecycle.binding.shard_ids.as_slice() else {
        return Err(OmniError::manifest_internal(format!(
            "stream binding for {identity} must select exactly one shard"
        )));
    };
    let shard_id = ShardId::from_str(shard_id_text).map_err(|error| {
        OmniError::manifest_internal(format!(
            "stream binding for {identity} has invalid shard UUID '{shard_id_text}': {error}"
        ))
    })?;
    let enrollment_id = ShardId::from_str(&lifecycle.binding.enrollment_id).map_err(|error| {
        OmniError::manifest_internal(format!(
            "stream binding for {identity} has invalid enrollment UUID '{}': {error}",
            lifecycle.binding.enrollment_id
        ))
    })?;
    StreamWorkerKey::new(identity, enrollment_id, shard_id).map_err(worker_observation_error)
}

fn manifest_selected_worker_keys(
    snapshot: &Snapshot,
) -> Result<BTreeMap<TableIdentity, StreamWorkerKey>> {
    snapshot
        .stream_lifecycles()
        .map(|(identity, lifecycle)| {
            manifest_selected_worker_key(*identity, lifecycle).map(|key| (*identity, key))
        })
        .collect()
}

fn validate_resident_worker_bindings(
    selected: &BTreeMap<TableIdentity, StreamWorkerKey>,
    resident: &[StreamWorkerKey],
) -> Result<()> {
    let mismatches = resident
        .iter()
        .filter_map(|key| match selected.get(&key.identity) {
            Some(expected) if expected == key => None,
            Some(expected) => Some(format!("resident {key}, selected {expected}")),
            None => Some(format!("resident {key}, no selected lifecycle")),
        })
        .collect::<Vec<_>>();
    if mismatches.is_empty() {
        return Ok(());
    }
    Err(OmniError::manifest_internal(format!(
        "stream operational status found worker ownership outside the manifest-selected physical binding: {}",
        mismatches.join("; ")
    )))
}

fn project_table_operational_shell(
    durable: &StreamTableStatus,
    lifecycle: &crate::db::manifest::StreamLifecycleEntry,
    physical: StreamTablePhysicalOperationalStatus,
) -> StreamTableOperationalStatus {
    let drain = lifecycle
        .drain
        .as_ref()
        .map(|drain| StreamDrainOperationalStatus {
            drain_id: drain.drain_id.clone(),
            goal: match drain.goal {
                DrainGoal::Sealed => "SEALED",
                DrainGoal::OpenAfterFold => "OPEN_AFTER_FOLD",
            },
            phase: if lifecycle.strict_block.is_some() {
                "STRICT_BLOCKED"
            } else {
                "DRAINING"
            },
            operation_expected_revision: drain.operation_expected_revision,
            initiating_actor: drain.initiating_actor.clone(),
            initiated_at: drain.initiated_at,
        });
    let strict_block = lifecycle.strict_block.as_ref().map(|block| {
        let (kind, violation_code) = match &block.evidence {
            StrictBlockEvidence::DataBlock { violation_code, .. } => {
                ("DATA_BLOCK", violation_code.clone())
            }
            StrictBlockEvidence::AuthorityBlock { violation_code, .. } => {
                ("AUTHORITY_BLOCK", violation_code.clone())
            }
        };
        StreamStrictBlockOperationalStatus {
            block_token: block.block_token.clone(),
            correction_revision: block.correction_revision,
            kind,
            violation_code,
        }
    });
    let last_fold =
        lifecycle
            .last_fold_summary
            .as_ref()
            .map(|summary| StreamLastFoldOperationalStatus {
                operation_id: summary.operation_id.clone(),
                outcome: summary.outcome.as_str().to_string(),
                graph_commit_id: summary.graph_commit_id.clone(),
                generation: summary.exact_generation_cut.generation,
                input_rows: summary.input_rows,
                input_bytes: summary.input_bytes,
                visible_rows: summary.visible_rows,
                visible_bytes: summary.visible_bytes,
                recorded_at: summary.recorded_at,
            });
    StreamTableOperationalStatus {
        durable: durable.clone(),
        stream_config_version: lifecycle.binding.stream_config_version,
        stream_config_hash: lifecycle.binding.stream_config_hash.clone(),
        binding_scope_id: lifecycle.binding_scope_id.clone(),
        receipts: StreamReceiptHeadsStatus {
            enrollment_request_id: lifecycle.enrollment_receipt.enrollment_request_id.clone(),
            current_binding_receipt_id: lifecycle.current_binding_receipt_id.clone(),
            current_claim_receipt_id: lifecycle.current_claim_receipt_id.clone(),
            binding_receipt_count: lifecycle.binding_receipt_chain.record_count,
            management_receipt_count: lifecycle.management_receipt_chain.record_count,
            claim_receipt_count: lifecycle.claim_receipt_chain.record_count,
        },
        drain,
        strict_block,
        last_fold,
        physical,
    }
}

async fn validate_table_operational_ledgers(
    db: &Omnigraph,
    snapshot: &Snapshot,
    durable: &StreamStatus,
) -> Result<Vec<Option<RetainedShardInventoryCommitment>>> {
    let token_authority = snapshot.stream_token_authority();
    let token_dataset = snapshot.open_stream_token_authority().await?;
    let graph_identity_domain = db.schema_view.load().schema_identity_domain.clone();
    let graph_identity_digest = stream_graph_identity_digest(&graph_identity_domain)?;
    let mut inventories = Vec::with_capacity(durable.tables.len());
    for durable_table in &durable.tables {
        let identity = TableIdentity {
            stable_table_id: durable_table.stable_table_id,
            table_incarnation_id: durable_table.table_incarnation_id,
        };
        let lifecycle = snapshot.stream_lifecycle(identity).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "operational status lost lifecycle authority for {identity}"
            ))
        })?;
        let retained_inventory =
            super::stream_enrollment::validate_selected_lifecycle_ledger_authority(
                &token_dataset,
                token_authority,
                &graph_identity_digest,
                lifecycle,
            )
            .await?;
        inventories.push(retained_inventory);
    }
    Ok(inventories)
}

async fn observe_table_physical_status(
    db: &Omnigraph,
    snapshot: &Snapshot,
    durable: &StreamStatus,
    retained_inventories: &[Option<RetainedShardInventoryCommitment>],
    sidecars: &[RecoverySidecar],
) -> Result<Vec<StreamTablePhysicalOperationalStatus>> {
    if durable.tables.len() != retained_inventories.len() {
        return Err(OmniError::manifest_internal(
            "operational status lifecycle inventory count differs from enrolled lane count",
        ));
    }
    let entries_by_identity = snapshot
        .entries()
        .map(|entry| (entry.identity, entry))
        .collect::<BTreeMap<_, _>>();
    let mut projected = Vec::with_capacity(durable.tables.len());
    for (durable_table, retained_inventory) in
        durable.tables.iter().zip(retained_inventories.iter())
    {
        let identity = TableIdentity {
            stable_table_id: durable_table.stable_table_id,
            table_incarnation_id: durable_table.table_incarnation_id,
        };
        let lifecycle = snapshot.stream_lifecycle(identity).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "operational status lost lifecycle authority for {identity}"
            ))
        })?;
        let entry = entries_by_identity.get(&identity).copied().ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "operational status found no live table registration for {identity}"
            ))
        })?;
        let full_path = format!(
            "{}/{}",
            db.root_uri().trim_end_matches('/'),
            entry.table_path.trim_start_matches('/')
        );
        let head = db.storage().open_dataset_head(&full_path, None).await?;
        let dataset = head.dataset();
        let observed_head = capture_current_head_witness(dataset)
            .await
            .map_err(enrollment_observation_error)?;
        if observed_head != lifecycle.current_head_witness {
            let operation_ids = recovery_operations_for_table(
                sidecars,
                identity,
                &full_path,
                &lifecycle.current_head_witness,
                &observed_head,
                dataset,
            )
            .await?;
            if !operation_ids.is_empty() {
                projected.push(StreamTablePhysicalOperationalStatus::UnavailableRecovery {
                    operation_ids,
                    reason: "listed recovery exactly owns physical table movement beyond the manifest-selected cut",
                });
                continue;
            }
            return Err(OmniError::StreamStatusChanged {
                member: format!("base table HEAD for '{}'", entry.table_key),
            });
        }
        let physical = validate_b1_lifecycle_current_binding_physical_state(
            dataset,
            lifecycle,
            retained_inventory.as_ref(),
        )
        .await
        .map_err(worker_observation_error)?;
        let base_merged_generation = read_bound_merged_generation(dataset, &lifecycle.binding)
            .await
            .map_err(worker_observation_error)?;
        let worker_key = manifest_selected_worker_key(identity, lifecycle)?;
        let shard_id_text = &lifecycle.binding.shard_ids[0];
        let resident = db
            .stream_workers
            .resident_worker_status(worker_key)
            .await
            .map_err(worker_observation_error)?;
        let (
            shard_manifest_version,
            current_generation,
            replay_after_wal_entry_position,
            observed_writer_epoch,
            flushed,
        ) = match physical {
            PassiveB1PhysicalState::AdmitOrReplay {
                shard_manifest_version,
                current_generation,
                replay_after_wal_entry_position,
                writer_epoch,
            } => (
                shard_manifest_version,
                current_generation,
                replay_after_wal_entry_position,
                writer_epoch,
                None,
            ),
            PassiveB1PhysicalState::FoldOnlyFlushed(flushed) => (
                flushed.shard_manifest_version,
                flushed.generation.checked_add(1).ok_or_else(|| {
                    OmniError::manifest_internal("flushed generation successor overflow")
                })?,
                flushed.replay_after_wal_entry_position,
                flushed.writer_epoch,
                Some(flushed),
            ),
        };
        let authoritative_epoch_floor = lifecycle
            .epoch_floor_by_shard
            .get(shard_id_text)
            .copied()
            .ok_or_else(|| {
                OmniError::manifest_internal("stream lifecycle has no current shard epoch floor")
            })?;
        let pending = if lifecycle.lifecycle == StreamLifecycle::Sealed {
            match resident {
                ResidentWorkerStatus::Missing => sealed_pending_status(
                    lifecycle,
                    shard_manifest_version,
                    observed_writer_epoch,
                    current_generation,
                    replay_after_wal_entry_position,
                    base_merged_generation,
                )?
                .expect("SEALED lifecycle returns a verified-empty projection"),
                ResidentWorkerStatus::Busy => {
                    return Err(OmniError::StreamStatusBusy {
                        phase: format!("SEALED resident retirement for '{}'", entry.table_key),
                    });
                }
                ResidentWorkerStatus::Active(_) => {
                    return Err(OmniError::manifest_internal(format!(
                        "SEALED stream lane '{}' retains an active resident worker",
                        entry.table_key
                    )));
                }
            }
        } else {
            match resident {
                ResidentWorkerStatus::Busy => {
                    return Err(OmniError::StreamStatusBusy {
                        phase: format!("resident worker for '{}'", entry.table_key),
                    });
                }
                ResidentWorkerStatus::Active(resident) => {
                    if resident.writer_epoch != observed_writer_epoch {
                        return Err(OmniError::StreamStatusChanged {
                            member: format!("writer epoch for '{}'", entry.table_key),
                        });
                    }
                    match (resident.mode, resident.accounting, flushed.as_ref()) {
                        (ResidentWorkerMode::Admit, Some(accounting), None)
                        | (ResidentWorkerMode::FoldReplay, Some(accounting), None) => {
                            if resident.generation != current_generation {
                                return Err(OmniError::StreamStatusChanged {
                                    member: format!(
                                        "resident generation for '{}'",
                                        entry.table_key
                                    ),
                                });
                            }
                            StreamPendingGenerationStatus::Exact {
                                generation: resident.generation,
                                rows: accounting.rows,
                                arrow_bytes: accounting.arrow_bytes,
                                batches: accounting.batches,
                                source: match resident.mode {
                                    ResidentWorkerMode::Admit => "RESIDENT_ADMIT",
                                    ResidentWorkerMode::FoldReplay => "RESIDENT_REPLAY",
                                    ResidentWorkerMode::FoldFlushed => unreachable!(),
                                },
                            }
                        }
                        (ResidentWorkerMode::FoldFlushed, None, Some(flushed))
                            if resident.generation == flushed.generation =>
                        {
                            unavailable_flushed_accounting(flushed)
                        }
                        _ => {
                            return Err(OmniError::StreamStatusChanged {
                                member: format!(
                                    "resident/physical generation disposition for '{}'",
                                    entry.table_key
                                ),
                            });
                        }
                    }
                }
                ResidentWorkerStatus::Missing => match flushed.as_ref() {
                    Some(flushed) => unavailable_flushed_accounting(flushed),
                    None => StreamPendingGenerationStatus::UnavailableColdReplay {
                        generation: current_generation,
                        reason: "exact cold WAL accounting would mutate Lance cursor state or claim a writer",
                    },
                },
            }
        };
        projected.push(StreamTablePhysicalOperationalStatus::Observed {
            shards: vec![StreamShardOperationalStatus {
                shard_id: shard_id_text.clone(),
                authoritative_epoch_floor,
                observed_writer_epoch,
                shard_manifest_version,
                current_generation,
                replay_after_wal_entry_position,
                base_merged_generation,
                pending,
            }],
        });
    }
    Ok(projected)
}

fn project_table_operational_status(
    snapshot: &Snapshot,
    durable: &StreamStatus,
    physical: Vec<StreamTablePhysicalOperationalStatus>,
) -> Result<Vec<StreamTableOperationalStatus>> {
    if durable.tables.len() != physical.len() {
        return Err(OmniError::manifest_internal(
            "operational status physical witness count differs from enrolled lane count",
        ));
    }
    durable
        .tables
        .iter()
        .zip(physical)
        .map(|(durable_table, physical)| {
            let identity = TableIdentity {
                stable_table_id: durable_table.stable_table_id,
                table_incarnation_id: durable_table.table_incarnation_id,
            };
            let lifecycle = snapshot.stream_lifecycle(identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "operational status lost lifecycle authority for {identity}"
                ))
            })?;
            Ok(project_table_operational_shell(
                durable_table,
                lifecycle,
                physical,
            ))
        })
        .collect()
}

struct StreamOperationalStatusPreflight {
    snapshot: Snapshot,
    durable: StreamStatus,
    retained_inventories: Vec<Option<RetainedShardInventoryCommitment>>,
    token_ledger: StreamTokenLedgerOperationalStatus,
    sidecars: Vec<RecoverySidecar>,
    relevant_recovery: Vec<StreamRecoveryOperationalStatus>,
}

impl Omnigraph {
    /// Expensive immutable work. Every dataset read is pinned by `snapshot`,
    /// so writers may continue while this parity/receipt proof runs. The short
    /// authority cut below accepts the result only if the manifest and
    /// recovery inventory are still identical.
    async fn prepare_stream_operational_status(
        &self,
        offline_apply: Option<&CheckedClusterApplyAuthority<'_>>,
    ) -> Result<StreamOperationalStatusPreflight> {
        let snapshot = self.open_coordinator_for_branch(None).await?.snapshot();
        self.validate_stream_operational_status_authority(
            snapshot.stream_profile(),
            offline_apply,
        )?;
        if snapshot.stream_profile().mode() == StreamProfileMode::Retired {
            self.export_stream_authority_preflight_at(&snapshot)
                .await?
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "RETIRED operational status did not recover retirement provenance",
                    )
                })?;
        }
        let durable = project_stream_status(&snapshot)?;
        let sidecars = list_operational_status_sidecars(self).await?;
        let relevant_recovery = recovery_status(&sidecars)?;
        let retained_inventories =
            validate_table_operational_ledgers(self, &snapshot, &durable).await?;
        let token_ledger = project_token_ledger_status(self, &snapshot).await?;
        Ok(StreamOperationalStatusPreflight {
            snapshot,
            durable,
            retained_inventories,
            token_ledger,
            sidecars,
            relevant_recovery,
        })
    }

    async fn capture_stream_operational_status(
        &self,
        offline_apply: Option<&CheckedClusterApplyAuthority<'_>>,
        preflight: StreamOperationalStatusPreflight,
    ) -> Result<StreamOperationalStatus> {
        let StreamOperationalStatusPreflight {
            snapshot: provisional,
            durable,
            retained_inventories,
            token_ledger,
            sidecars: before_sidecars,
            relevant_recovery,
        } = preflight;
        // Root opportunity first: this waits for every detached producer and
        // complete driver round, then prevents either class from restarting
        // while the cut is observed.
        let _root_round = self.stream_workers.acquire_stream_fold_round().await;
        let write_queue = self.write_queue();
        let _profile = write_queue.acquire_stream_profile_shared().await;

        // The immutable preflight snapshot discovers the finite lane set.
        // Movement while it ran is detected after every gate closes.
        let entries_by_identity = provisional
            .entries()
            .map(|entry| (entry.identity, entry))
            .collect::<HashMap<_, _>>();
        let mut admission_keys = provisional
            .stream_lifecycles()
            .map(|(identity, _)| {
                let entry = entries_by_identity.get(identity).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "stream status found lifecycle authority for non-live identity {identity}"
                    ))
                })?;
                Ok(StreamAdmissionKey::for_resolved_ref(
                    *identity,
                    entry.table_branch.as_deref(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        admission_keys.extend(
            self.stream_workers
                .served_runtime_resident_keys()
                .await
                .into_iter()
                .map(|key| StreamAdmissionKey::for_resolved_ref(key.identity, None)),
        );
        let _admissions = write_queue
            .acquire_stream_exclusive_many(&admission_keys)
            .await;
        let _schema = write_queue
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch = write_queue.acquire_branch(None).await;
        let _token = write_queue.acquire_stream_token().await;
        let table_keys = provisional
            .entries()
            .map(|entry| (entry.table_key.clone(), None))
            .collect::<Vec<_>>();
        let _tables = write_queue.acquire_many(&table_keys).await;

        let snapshot = self.open_coordinator_for_branch(None).await?.snapshot();
        if snapshot.version() != provisional.version() {
            return Err(OmniError::StreamStatusChanged {
                member: "canonical manifest while closing status gates".to_string(),
            });
        }
        self.validate_stream_operational_status_authority(
            snapshot.stream_profile(),
            offline_apply,
        )?;
        let current_sidecars = list_operational_status_sidecars(self).await?;
        if recovery_status(&current_sidecars)? != relevant_recovery {
            return Err(OmniError::StreamStatusChanged {
                member: "recovery inventory while closing status gates".to_string(),
            });
        }
        drop(current_sidecars);
        let selected_worker_keys = manifest_selected_worker_keys(&snapshot)?;
        let resident_worker_keys = self.stream_workers.served_runtime_resident_keys().await;
        validate_resident_worker_bindings(&selected_worker_keys, &resident_worker_keys)?;
        let physical = observe_table_physical_status(
            self,
            &snapshot,
            &durable,
            &retained_inventories,
            &before_sidecars,
        )
        .await?;
        let driver = project_driver_status(self);

        // Physical shard state is independent Lance authority and can move
        // without a graph-manifest publication. Repeat only that mutable
        // witness; immutable parity and ledger proofs stay outside this fence.
        let trailing_physical = observe_table_physical_status(
            self,
            &snapshot,
            &durable,
            &retained_inventories,
            &before_sidecars,
        )
        .await?;
        if trailing_physical != physical {
            return Err(OmniError::StreamStatusChanged {
                member: "physical shard or resident-worker authority".to_string(),
            });
        }
        let trailing_sidecars = list_operational_status_sidecars(self).await?;
        if recovery_status(&trailing_sidecars)? != relevant_recovery {
            return Err(OmniError::StreamStatusChanged {
                member: "relevant recovery inventory".to_string(),
            });
        }
        drop(trailing_sidecars);
        let trailing = self.open_coordinator_for_branch(None).await?.snapshot();
        if trailing.version() != snapshot.version() {
            return Err(OmniError::StreamStatusChanged {
                member: "canonical manifest during status observation".to_string(),
            });
        }
        let tables = project_table_operational_status(&snapshot, &durable, physical)?;
        let rebuild = rebuild_readiness(&durable, &tables, &token_ledger, &relevant_recovery);
        Ok(StreamOperationalStatus {
            cut_manifest_version: snapshot.version(),
            durable,
            tables,
            token_ledger,
            relevant_recovery,
            driver,
            rebuild,
        })
    }

    /// Checked, read-only full stream status underlying the F7b graph-safe
    /// served projection. Existing callers continue to use the non-blocking
    /// manifest-only [`Self::stream_status`].
    pub(crate) async fn stream_operational_status(&self) -> Result<StreamOperationalStatus> {
        self.stream_operational_status_with_deadlines(
            None,
            STREAM_OPERATIONAL_STATUS_PREFLIGHT_DEADLINE,
            STREAM_OPERATIONAL_STATUS_CUT_DEADLINE,
        )
        .await
    }

    /// Capture one checked operational cut and project it onto graph-logical
    /// declarations for the served F7 status boundary.
    ///
    /// The returned value intentionally cannot identify a table dataset,
    /// physical binding, receipt, shard, epoch, generation, Lance version, or
    /// recovery sidecar. The complete physical cut remains engine-private.
    #[doc(hidden)]
    pub async fn capture_served_graph_stream_status(&self) -> Result<GraphStreamOperationalStatus> {
        project_graph_stream_operational_status(self.stream_operational_status().await?)
    }

    /// Checked DISABLING observation for the stopped cluster-apply owner. The
    /// capability retains the real cluster lock for the complete cut.
    pub(crate) async fn stream_operational_status_for_apply(
        &self,
        authority: &CheckedClusterApplyAuthority<'_>,
    ) -> Result<StreamOperationalStatus> {
        self.stream_operational_status_with_deadlines(
            Some(authority),
            STREAM_OPERATIONAL_STATUS_PREFLIGHT_DEADLINE,
            STREAM_OPERATIONAL_STATUS_CUT_DEADLINE,
        )
        .await
    }

    async fn stream_operational_status_with_deadlines(
        &self,
        offline_apply: Option<&CheckedClusterApplyAuthority<'_>>,
        preflight_deadline: Duration,
        cut_deadline: Duration,
    ) -> Result<StreamOperationalStatus> {
        let _observation = self
            .write_queue()
            .try_acquire_stream_status_observation()
            .ok_or_else(|| OmniError::StreamStatusBusy {
                phase: "another checked status observation is already in progress".to_string(),
            })?;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::STREAM_STATUS_POST_OBSERVATION_ADMISSION,
        )?;
        let preflight = tokio::time::timeout(
            preflight_deadline,
            self.prepare_stream_operational_status(offline_apply),
        )
        .await
        .map_err(|_| OmniError::StreamStatusBusy {
            phase: "immutable rebuild preflight".to_string(),
        })??;
        tokio::time::timeout(
            cut_deadline,
            self.capture_stream_operational_status(offline_apply, preflight),
        )
        .await
        .map_err(|_| OmniError::StreamStatusBusy {
            phase: "exclusive authority cut".to_string(),
        })?
    }

    /// Test-only deadline control for proving wait, timeout, and cancellation
    /// without turning timing configuration into an SDK contract.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_operational_status_for_test(
        &self,
        deadline_from_now: Duration,
    ) -> Result<StreamOperationalStatus> {
        self.stream_operational_status_with_deadlines(None, deadline_from_now, deadline_from_now)
            .await
    }

    /// Test-only split deadline control. Keeping the immutable scan deadline
    /// independent from the authority-cut deadline lets concurrency tests
    /// prove cut cancellation without racing the preflight itself.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_operational_status_with_deadlines_for_test(
        &self,
        preflight_deadline: Duration,
        cut_deadline: Duration,
    ) -> Result<StreamOperationalStatus> {
        self.stream_operational_status_with_deadlines(None, preflight_deadline, cut_deadline)
            .await
    }

    /// Test-only root owner for deterministic cut-timeout and cancellation
    /// coverage. Channels avoid a failpoint callback, whose synchronous
    /// registry lock would also stall the immutable sidecar preflight.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_hold_stream_status_root_for_test(
        &self,
        acquired: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) {
        let _root_round = self.stream_workers.acquire_stream_fold_round().await;
        if acquired.send(()).is_ok() {
            let _ = release.await;
        }
    }

    /// Test-only checked-offline path for an interrupted DISABLING profile.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_operational_status_for_apply_for_test(
        &self,
        authority: &CheckedClusterApplyAuthority<'_>,
        deadline_from_now: Duration,
    ) -> Result<StreamOperationalStatus> {
        self.stream_operational_status_with_deadlines(
            Some(authority),
            deadline_from_now,
            deadline_from_now,
        )
        .await
    }

    /// Read graph-wide streaming status from one canonical-main snapshot.
    ///
    /// Read-only and lifecycle-inert by construction: it never mutates, never
    /// resolves recovery, and takes no admission lease. Authorized like other
    /// graph operational metadata, so it needs no `stream_manage` grant — an
    /// operator can always see whether a lane is stuck, including when they
    /// lack the rights to act on it.
    pub async fn stream_status(&self) -> Result<StreamStatus> {
        // Main-only, matching the profile's topology: lifecycle authority is
        // graph-global control state and named branches only ever hold the
        // copy they forked with.
        let snapshot = self.snapshot_of(ReadTarget::branch("main")).await?;
        // The projector accepts only this immutable snapshot. `"main"`
        // normalizes to the canonical branch, so the named-branch-only profile
        // projection is not involved.
        project_stream_status(&snapshot)
    }

    /// Measure one exact current-token probe without exposing raw token-table
    /// authority or folding unrelated WAL work into the observation.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_token_lookup_for_cost_test(
        &self,
        table_key: &str,
        logical_id: &str,
    ) -> Result<(u64, Option<String>)> {
        let snapshot = self.snapshot_of(ReadTarget::branch("main")).await?;
        let entry = snapshot.entry(table_key).ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "stream-token cost probe found no table '{table_key}'"
            ))
        })?;
        let authority = snapshot.stream_token_authority();
        let selected_version = authority.current_head_witness.table_version;
        let dataset = snapshot.open_stream_token_authority().await?;
        let selected =
            lookup_stream_token_row(&dataset, authority, entry.identity, logical_id).await?;
        Ok((
            selected_version,
            selected.map(|row| row.current_token.to_string()),
        ))
    }

    /// Measure one exact profile-management receipt probe against the same
    /// manifest-selected token cut used by the production retry path.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_profile_receipt_lookup_for_cost_test(
        &self,
        cluster_root: &str,
        graph_id: &str,
        operation_id: &str,
    ) -> Result<(u64, Option<String>)> {
        let snapshot = self.snapshot_of(ReadTarget::branch("main")).await?;
        let authority = snapshot.stream_token_authority();
        let selected_version = authority.current_head_witness.table_version;
        let cluster_root_digest =
            stream_profile_cluster_root_digest(cluster_root.trim_end_matches('/'))?;
        let graph_identity_digest =
            stream_profile_graph_identity_digest(&cluster_root_digest, graph_id, self.uri())?;
        let dataset = snapshot.open_stream_token_authority().await?;
        let selected = lookup_profile_management_receipt(
            &dataset,
            authority,
            &graph_identity_digest,
            operation_id,
        )
        .await?;
        Ok((selected_version, selected.map(|receipt| receipt.record_id)))
    }

    /// Measure physical lookup-index coverage on the exact selected token cut.
    /// Unknown coverage stays `None`; this probe never opens raw HEAD or
    /// publishes a derived index version.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    pub async fn failpoint_stream_token_lookup_coverage_for_cost_test(
        &self,
    ) -> Result<(u64, u64, Option<u64>)> {
        let snapshot = self.snapshot_of(ReadTarget::branch("main")).await?;
        let authority = snapshot.stream_token_authority();
        let selected_version = authority.current_head_witness.table_version;
        let dataset = snapshot.open_stream_token_authority().await?;
        let (total_fragments, uncovered_fragments) =
            stream_token_lookup_index_coverage(&dataset, authority).await?;
        Ok((selected_version, total_fragments, uncovered_fragments))
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use super::*;

    #[test]
    fn graph_rebuild_projection_collapses_physical_reason_multiplicity() {
        let declaration = GraphStreamDeclaration {
            kind: "node",
            type_name: "Person".to_string(),
        };
        let declarations = BTreeMap::from([("node:Person".to_string(), declaration)]);
        let mut reasons = Vec::new();
        for _ in 0..3 {
            reasons.extend([
                StreamRebuildBlockReason::LaneNotSealed {
                    table_key: "node:Person".to_string(),
                },
                StreamRebuildBlockReason::StrictBlock {
                    table_key: "node:Person".to_string(),
                },
                StreamRebuildBlockReason::PendingGeneration {
                    table_key: "node:Person".to_string(),
                },
                StreamRebuildBlockReason::PendingGenerationUnavailable {
                    table_key: "node:Person".to_string(),
                },
            ]);
        }
        let projected = graph_stream_rebuild_status(
            StreamRebuildReadiness {
                ready: false,
                reasons,
            },
            &declarations,
            0,
        )
        .unwrap();

        assert_eq!(projected.blockers.len(), 4);
        assert_eq!(
            projected
                .blockers
                .iter()
                .filter(|reason| matches!(reason, GraphStreamRebuildBlocker::PendingWork { .. }))
                .count(),
            1,
            "physical shard multiplicity must not appear on the graph wire"
        );
        assert_eq!(
            projected
                .blockers
                .iter()
                .filter(|reason| matches!(
                    reason,
                    GraphStreamRebuildBlocker::PendingWorkUnavailable { .. }
                ))
                .count(),
            1,
            "unavailable physical members must collapse to one logical blocker"
        );
    }

    #[test]
    fn resident_binding_validation_rejects_same_identity_stale_physical_keys() {
        let identity = TableIdentity::new(101, 103).unwrap();
        let selected_key =
            StreamWorkerKey::new(identity, ShardId::new_v4(), ShardId::new_v4()).unwrap();
        let stale_keys = (0..3)
            .map(|_| StreamWorkerKey::new(identity, ShardId::new_v4(), ShardId::new_v4()).unwrap())
            .collect::<Vec<_>>();
        let selected = BTreeMap::from([(identity, selected_key)]);

        validate_resident_worker_bindings(&selected, &[selected_key]).unwrap();
        let error = validate_resident_worker_bindings(&selected, &stale_keys).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("outside the manifest-selected physical binding"));
        assert!(message.contains(&selected_key.to_string()));
        for stale in stale_keys {
            assert!(
                message.contains(&stale.to_string()),
                "every stale resident key must remain visible in the refusal: {message}"
            );
        }
    }

    #[tokio::test]
    async fn status_uses_live_alias_when_lifecycle_diagnostic_alias_lags() {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), "node Person { score: I32 }\n")
            .await
            .unwrap();
        db.failpoint_enroll_stream_table_for_test("node:Person")
            .await
            .unwrap();

        let mut snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let identity = snapshot.entry("node:Person").unwrap().identity;
        snapshot.set_stream_diagnostic_table_key_for_test(identity, "node:StaleDiagnosticAlias");

        let status = project_stream_status(&snapshot).unwrap();
        assert_eq!(status.tables.len(), 1);
        assert_eq!(
            status.tables[0].table_key, "node:Person",
            "status must resolve the live registration by immutable identity"
        );
    }
}
