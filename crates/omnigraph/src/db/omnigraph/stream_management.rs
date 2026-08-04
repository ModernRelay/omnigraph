//! Graph-wide served stream controls.
//!
//! This is a thin orchestration layer over the existing per-lane lifecycle and
//! graph-wide SEALED-maintenance adapters. The manifest remains the work list;
//! no second coordinator, durable queue, or recovery vocabulary is introduced.

use std::sync::Arc;

use crate::db::manifest::stream::graph_stream_resume_id;
use crate::db::manifest::{StreamLifecycle, StreamProfileMode};
use crate::error::{OmniError, Result};

use super::*;

/// Graph-redacted result of converging every enrolled SEALED declaration to
/// `OPEN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct GraphStreamResumeResult {
    pub profile_revision: u64,
    pub enrolled_declarations: u64,
    pub resumed_declarations: u64,
    pub already_open_declarations: u64,
}

/// Graph-redacted result of one SEALED EnsureIndices pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct GraphStreamEnsureIndicesResult {
    pub changed: bool,
    pub pending_index_count: u64,
}

/// Graph-redacted result of one SEALED Optimize pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct GraphStreamOptimizeResult {
    pub changed: bool,
    pub pending_index_count: u64,
    pub requires_repair: bool,
}

fn graph_resume_serial_queue_key() -> crate::db::write_queue::TableQueueKey {
    // Real table aliases are always node:/edge:-prefixed. This process-local
    // key serializes only graph resume orchestration; it is not durable
    // authority and is never interpreted as a table identity.
    ("__graph_stream_resume__".to_string(), None)
}

fn count_u64(field: &str, count: usize) -> Result<u64> {
    u64::try_from(count).map_err(|_| {
        OmniError::manifest_internal(format!(
            "graph stream {field} count exceeds the u64 contract"
        ))
    })
}

impl Omnigraph {
    /// Resume every currently enrolled SEALED declaration in deterministic
    /// graph order. OPEN declarations are idempotent no-ops; a DRAINING lane or
    /// unresolved strict block refuses the whole preflight before any resume
    /// effect starts.
    #[doc(hidden)]
    pub async fn resume_served_graph_stream_as(
        self: &Arc<Self>,
        actor_id: &str,
    ) -> Result<GraphStreamResumeResult> {
        // The policy decision belongs to the graph operation, including the
        // zero-lane case. Per-lane resume is deliberately crate-private and
        // cannot be used as a public selector-shaped authorization surface.
        self.enforce(
            omnigraph_policy::PolicyAction::StreamManage,
            &omnigraph_policy::ResourceScope::Graph,
            Some(actor_id),
        )?;

        let _resume_serial = self
            .write_queue()
            .acquire(&graph_resume_serial_queue_key())
            .await;

        // Capture one immutable graph work list while the exact checked
        // ENABLED delegation cannot transition. Release this read gate before
        // invoking lane resume: that adapter acquires the root resident-worker
        // opportunity before the profile gate, and preserving that order is
        // required for shutdown freedom from lock inversion.
        let profile_guard = self.write_queue().acquire_stream_profile_shared().await;
        self.ensure_streaming_ingest_runtime_authorized().await?;
        let frozen = self.open_write_txn(None).await?;
        if frozen.base.stream_profile().mode() != StreamProfileMode::Enabled {
            return Err(OmniError::StreamingAuthorityMismatch {
                reason: "graph stream resume requires the exact ENABLED profile".to_string(),
            });
        }
        let profile_revision = frozen.base.stream_profile().profile_revision;
        let lanes = self.ordered_enrolled_stream_lanes(&frozen.base)?;
        let mut sealed = Vec::new();
        let mut already_open = 0_usize;
        for lane in &lanes {
            let lifecycle = frozen.base.stream_lifecycle(lane.identity).ok_or_else(|| {
                OmniError::StreamingAuthorityMismatch {
                    reason: "graph stream enrollment changed during resume preflight".to_string(),
                }
            })?;
            if lifecycle.strict_block.is_some() {
                return Err(OmniError::manifest_conflict(
                    "graph stream resume is blocked by unresolved validation evidence",
                ));
            }
            match lifecycle.lifecycle {
                StreamLifecycle::Open => already_open += 1,
                StreamLifecycle::Sealed => sealed.push((
                    lane.table_key.clone(),
                    lane.identity,
                    lifecycle.lifecycle_revision,
                )),
                StreamLifecycle::Draining => {
                    return Err(OmniError::manifest_conflict(
                        "graph stream resume requires every drain to reach SEALED first",
                    ));
                }
            }
        }
        drop(profile_guard);

        let mut resumed = 0_usize;
        for (table_key, identity, lifecycle_revision) in sealed {
            let resume_id = graph_stream_resume_id(identity, lifecycle_revision)?;
            // A retry may encounter the empty OPEN owner installed by an
            // earlier lane before its caller observed the handoff. Reuse the
            // graph route coordinator to release any such foreign resident
            // without asking the still-SEALED target for an OPEN disposition.
            self.release_foreign_stream_resident_for_graph_control(identity)
                .await?;
            self.stream_resume_as(
                &table_key,
                &resume_id,
                lifecycle_revision,
                crate::db::manifest::stream::StreamResumeMode::ResumeSealed,
                actor_id,
            )
            .await?;
            // Resume must install its fresh empty writer before publishing the
            // OPEN receipt. Settle that exact owner through the existing finite
            // driver round before selecting the next lane, otherwise several
            // graph declarations would contend for the bounded profile's sole
            // resident slot. This is a handoff, not a second lifecycle effect.
            self.fold_resident_stream_lane_for_handoff(identity, &table_key)
                .await?;
            resumed += 1;
        }

        Ok(GraphStreamResumeResult {
            profile_revision,
            enrolled_declarations: count_u64("enrolled declaration", lanes.len())?,
            resumed_declarations: count_u64("resumed declaration", resumed)?,
            already_open_declarations: count_u64("already-open declaration", already_open)?,
        })
    }

    /// Run the existing one-publication SEALED EnsureIndices adapter and
    /// project away every physical table detail.
    #[doc(hidden)]
    pub async fn ensure_served_graph_stream_indices_as(
        &self,
        actor_id: &str,
    ) -> Result<GraphStreamEnsureIndicesResult> {
        let outcome = table_ops::ensure_indices_sealed_as(self, actor_id).await?;
        Ok(GraphStreamEnsureIndicesResult {
            changed: outcome.changed,
            pending_index_count: count_u64("pending index", outcome.pending.len())?,
        })
    }

    /// Run the existing one-publication SEALED Optimize adapter and project
    /// away table aliases, versions, fragment counts, and physical paths.
    #[doc(hidden)]
    pub async fn optimize_served_graph_stream_as(
        &self,
        actor_id: &str,
    ) -> Result<GraphStreamOptimizeResult> {
        let stats = optimize::optimize_all_tables_sealed_as(self, actor_id).await?;
        let pending_index_count = stats.iter().try_fold(0_u64, |total, stat| {
            total
                .checked_add(count_u64("pending index", stat.pending_indexes.len())?)
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "graph stream pending index count exceeds the u64 contract",
                    )
                })
        })?;
        Ok(GraphStreamOptimizeResult {
            changed: stats.iter().any(|stat| stat.committed),
            pending_index_count,
            requires_repair: stats
                .iter()
                .any(|stat| stat.skipped == Some(SkipReason::DriftNeedsRepair)),
        })
    }
}
