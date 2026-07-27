//! RFC-026 §4.7 P1 — the graph-wide streaming-enablement flip.
//!
//! One Cedar-gated writer flips the required `stream_profile` manifest
//! singleton through the shared publisher. The flip has zero pre-manifest
//! durable effects — one exact-entry CAS — so it arms no recovery sidecar
//! (precedent: `repair`, the other registered sidecar-free writer). It still
//! takes the full write prelude (heal barrier → stream-admission capture →
//! schema/branch/stream-token gates → sidecar re-check) so a flip can never
//! land mid-recovery or race a stream enrollment's admission.
//!
//! OCC is the profile row itself: `SetStreamProfile` compares the complete
//! expected entry and requires a strict `profile_revision` advance, so two
//! concurrent flips serialize with a typed read-set conflict. The publish
//! precondition is deliberately `Any` — an unrelated content commit moving
//! `graph_head` must not spuriously fail a flag flip (same posture as the
//! token-authority CAS and `repair`).
//!
//! Disabling converges only when nothing could hold acknowledged-unfolded
//! stream state: every lifecycle row must be terminal. In this slice no
//! production path can enroll a stream, so the check is structurally
//! satisfied; later slices tighten the same refusal without changing its
//! shape.

use std::collections::HashMap;

use crate::db::Omnigraph;
use crate::db::manifest::{
    ManifestChange, PublishPrecondition, StreamProfileEntry, schema_apply_serial_queue_key,
};
use crate::error::{OmniError, Result};

/// Outcome of [`Omnigraph::set_streaming_enabled_as`].
///
/// `changed == false` means the profile already matched the request; no
/// manifest version was minted and `manifest_version` is the version that
/// already carried the requested state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingProfileResult {
    pub changed: bool,
    pub streaming_enabled: bool,
    pub profile_revision: u64,
    pub manifest_version: u64,
}

impl Omnigraph {
    /// Flip the graph-wide RFC-026 §4.7 streaming-enablement flag.
    ///
    /// Cluster `apply` is the intended sole production caller (the flag is
    /// declared in `cluster.yaml`; this method is how apply propagates it into
    /// the graph). The flip records one ordinary graph commit, so it is
    /// auditable in `commit list` with the acting operator.
    pub async fn set_streaming_enabled_as(
        &self,
        enabled: bool,
        actor: Option<&str>,
    ) -> Result<StreamingProfileResult> {
        self.enforce(
            omnigraph_policy::PolicyAction::Admin,
            &omnigraph_policy::ResourceScope::Graph,
            actor,
        )?;
        self.ensure_schema_state_valid().await?;
        // Main-only: the profile row lives on the canonical main manifest and
        // every branch obeys it through the graph-level snapshot.
        let relevant: [Option<&str>; 1] = [None];
        self.heal_pending_recovery_sidecars_for_write(&relevant)
            .await?;
        let admission_keys = self.capture_branch_control_stream_admission_keys().await?;
        let _stream_admission_guards = self
            .write_queue()
            .acquire_stream_shared_many(&admission_keys)
            .await;
        let control_branches: [Option<String>; 1] = [None];
        let _schema_guard = self
            .write_queue()
            .acquire(&schema_apply_serial_queue_key())
            .await;
        let _branch_guards = self.write_queue().acquire_branches(&control_branches).await;
        let _stream_token_guard = self.write_queue().acquire_stream_token().await;
        self.ensure_schema_apply_not_locked("set_streaming_enabled")
            .await?;
        self.ensure_no_pending_recovery_sidecars_under_gates(&relevant, "set_streaming_enabled")
            .await?;
        self.ensure_schema_state_valid().await?;

        // Fresh main authority under the gates: the expected profile entry and
        // the drained check both come from one coherent snapshot.
        {
            let mut coordinator = self.coordinator.write().await;
            coordinator.refresh().await?;
        }
        let snapshot = self.coordinator.read().await.snapshot();
        Self::ensure_branch_control_stream_admission_covered(
            "set_streaming_enabled",
            &admission_keys,
            &snapshot,
        )?;

        let expected = snapshot.stream_profile().clone();
        if expected.streaming_enabled == enabled {
            return Ok(StreamingProfileResult {
                changed: false,
                streaming_enabled: expected.streaming_enabled,
                profile_revision: expected.profile_revision,
                manifest_version: snapshot.version(),
            });
        }

        if !enabled {
            let status = snapshot.streaming_status();
            if status.undrained {
                let mut undrained_tables: Vec<String> = snapshot
                    .stream_lifecycles()
                    .filter(|(_, lifecycle)| {
                        lifecycle.lifecycle != crate::db::manifest::StreamLifecycle::Sealed
                    })
                    .map(|(_, lifecycle)| lifecycle.diagnostic_table_key.clone())
                    .collect();
                undrained_tables.sort();
                return Err(OmniError::StreamingDisablePending { undrained_tables });
            }
        }

        let next = StreamProfileEntry {
            streaming_enabled: enabled,
            disable_pending_since: None,
            profile_revision: expected.profile_revision + 1,
        };
        let profile_revision = next.profile_revision;
        let changes = [ManifestChange::SetStreamProfile { expected, next }];
        let intent = self
            .coordinator
            .read()
            .await
            .new_lineage_intent(actor, None)?;
        let published = self
            .coordinator
            .write()
            .await
            .commit_changes_with_intent_and_expected(
                &changes,
                &HashMap::new(),
                intent,
                &PublishPrecondition::Any,
            )
            .await?;

        Ok(StreamingProfileResult {
            changed: true,
            streaming_enabled: enabled,
            profile_revision,
            manifest_version: published.manifest_version,
        })
    }
}
