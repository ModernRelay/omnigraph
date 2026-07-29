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

use std::collections::HashMap;

use crate::db::manifest::StreamLifecycle;
use crate::db::{Omnigraph, ReadTarget, Snapshot};
use crate::error::{OmniError, Result};

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
        // The projector accepts only this immutable snapshot. `"main"`
        // normalizes to the canonical branch, so the named-branch-only profile
        // projection is not involved.
        project_stream_status(&snapshot)
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use super::*;

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
