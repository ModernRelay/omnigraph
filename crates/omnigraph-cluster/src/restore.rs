use super::*;
use omnigraph::db::RestoreHistoryWitness;

/// A storage owner's statement about one staged history. Its identity is the
/// SHA-256 of the exact JSON bytes written at reservation and sealing time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedGraphRestore {
    pub version: u32,
    pub canonical_root: String,
    pub ledger_id: String,
    pub sequence: u64,
    pub graph: String,
    pub object_manifest_sha256: String,
    pub history: RestoreHistoryWitness,
}

impl PreparedGraphRestore {
    pub(crate) fn parse_exact(json: &str) -> Result<Self, Diagnostic> {
        if json.len() > MAX_RESTORE_DESCRIPTOR_BYTES {
            return Err(deployment::refusal(
                "restore_receipt_invalid",
                "restore descriptor exceeds 1 MiB",
            ));
        }
        let value = omnigraph::loader::parse_unique_json(json)
            .map_err(|err| deployment::refusal("restore_receipt_invalid", err.to_string()))?;
        serde_json::from_value(value)
            .map_err(|err| deployment::refusal("restore_receipt_invalid", err.to_string()))
    }

    pub fn validate_binding(
        &self,
        canonical_root: &str,
        ledger_id: &str,
        sequence: u64,
        graph: &str,
    ) -> Result<(), Diagnostic> {
        let mut names = Vec::new();
        config::validate_id("graph", "graph", graph, &mut names);
        if self.version != 1
            || self.canonical_root != canonical_root
            || self.ledger_id != ledger_id
            || !deployment::canonical_ulid(ledger_id)
            || self.sequence != sequence
            || sequence != 2
            || self.graph != graph
            || !names.is_empty()
            || graph.starts_with(omnigraph_compiler::SYSTEM_GRAPH_ID_PREFIX)
            || !authorization::valid_digest(&self.object_manifest_sha256)
            || self.history.version != 1
            || !authorization::valid_digest(&self.history.sha256)
            || !self.history.branch_heads.contains_key("main")
            || self.history.branch_heads.len() > deployment::MAX_RESOURCES
            || self.history.commits == 0
            || !deployment::valid_contract(&self.history.schema)
        {
            return Err(deployment::refusal(
                "restore_receipt_invalid",
                "receipt does not bind this first graph, destination ledger, sequence or history",
            ));
        }
        Ok(())
    }
}

/// The same fresh bootstrapped state must govern reservation and publication.
pub(crate) fn fresh_restore_ledger(state: &ClusterState) -> Result<&str, Diagnostic> {
    if state.version != 2
        || state.outstanding.is_some()
        || state.next_sequence != Some(2)
        || state.applied_revision.result_revision != Some(1)
        || state
            .applied_revision
            .schema_contracts
            .as_ref()
            .is_none_or(|m| !m.is_empty())
        || state
            .applied_revision
            .resources
            .keys()
            .any(|k| matches!(resource_kind(k), ResourceKind::Graph(_)))
    {
        return Err(deployment::refusal(
            "restore_destination_used",
            "restore requires a freshly bootstrapped, empty destination",
        ));
    }
    state
        .ledger_id
        .as_deref()
        .ok_or_else(|| deployment::refusal("state_missing", "missing native ledger ID"))
}
