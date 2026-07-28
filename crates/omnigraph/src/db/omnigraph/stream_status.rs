//! RFC-026 read-only stream status.
//!
//! Status answers two operator questions: *is this graph streaming, and what
//! state is each lane in?* It is deliberately the least privileged surface in
//! the streaming family — a pure projection of one `__manifest` snapshot,
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
//! **Scope of this slice.** This is the §4.7 *minimal* status: the
//! authoritative manifest row only. The full contract (§4.3) additionally
//! takes stream admission exclusively, settles every writer/watcher/flush
//! owner to a deadline, reads the physical shard witness, and rereads the
//! authorities before release — returning typed `StatusChanged` / `StatusBusy`
//! on movement or failure to settle. Those terms describe *physical*
//! observation; the fields below are exactly the durable ones, so nothing here
//! becomes wrong when that arrives — it gains an observed-physical section.
//! Fields whose only honest source is that physical read (observed epoch,
//! pending generation rows/bytes) are deliberately absent rather than
//! guessed-at from durable state.

use crate::db::manifest::StreamLifecycle;
use crate::db::{Omnigraph, ReadTarget};
use crate::error::Result;

/// One lane's durable status, projected from the manifest lifecycle row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTableStatus {
    /// Current public alias of the enrolled table (diagnostic only — the row
    /// is keyed by immutable identity).
    pub table_key: String,
    pub stable_table_id: u64,
    pub table_incarnation_id: u64,
    /// `OPEN` | `DRAINING` | `SEALED`.
    pub lifecycle: &'static str,
    /// The compare token: every mutating management call passes the revision
    /// it expects, and a mismatch refuses without retargeting (§4.6).
    pub lifecycle_revision: u64,
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
pub struct StreamStatus {
    /// The §4.7 P1 graph-global enablement flag.
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

impl Omnigraph {
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
        let profile = snapshot.stream_profile();

        let mut tables: Vec<StreamTableStatus> = snapshot
            .stream_lifecycles()
            .map(|(identity, lifecycle)| StreamTableStatus {
                table_key: lifecycle.diagnostic_table_key.clone(),
                stable_table_id: identity.stable_table_id,
                table_incarnation_id: identity.table_incarnation_id,
                lifecycle: lifecycle.lifecycle.as_str(),
                lifecycle_revision: lifecycle.lifecycle_revision,
                enrollment_id: lifecycle.binding.enrollment_id.clone(),
                epoch_floor_by_shard: lifecycle
                    .epoch_floor_by_shard
                    .iter()
                    .map(|(shard, floor)| (shard.clone(), *floor))
                    .collect(),
                drain_id: lifecycle
                    .drain
                    .as_ref()
                    .map(|drain| drain.drain_id.clone()),
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
            .collect();
        // Deterministic output within a snapshot: the source is a HashMap, and
        // hash-map iteration order is never allowed to reach a result surface.
        tables.sort_by(|a, b| {
            a.table_key
                .cmp(&b.table_key)
                .then(a.stable_table_id.cmp(&b.stable_table_id))
                .then(a.table_incarnation_id.cmp(&b.table_incarnation_id))
        });

        Ok(StreamStatus {
            streaming_enabled: profile.streaming_enabled,
            profile_revision: profile.profile_revision,
            tables,
        })
    }
}
