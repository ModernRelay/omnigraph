//! Durable invocation receipts for maintenance performed by the current native
//! server. This journal is neither graph authority nor another ownership lock.
use omnigraph::db::{
    Omnigraph, OptimizeInput, OptimizeLimits, OptimizePublication, OptimizeReport,
    OptimizeSettlement, OptimizeSettlementOutcome, SchemaContractDigest,
};
use omnigraph_policy::PolicyAction;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::authorization::AppliedPolicies;
use crate::{ClusterAdmission, ClusterState, ClusterStore, Diagnostic, IdentityAuthorization};

pub(crate) const MAINTENANCE_FILE: &str = "__cluster/maintenance.json";
pub(crate) const MAX_JOURNAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_RESULT_BYTES: usize = 1024 * 1024;
const MAX_RESULTS_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESULTS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceKind {
    Optimize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceRequest {
    pub operation_id: String,
    pub graph: String,
    pub kind: MaintenanceKind,
    pub limits: OptimizeLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceStatus {
    pub ledger_id: String,
    pub next_sequence: u64,
    /// Only the requested graph's outstanding identity is exposed.
    pub outstanding_id: Option<String>,
}

impl MaintenanceStatus {
    pub fn next_operation_id(&self) -> String {
        format!("{}:{}:{}", self.ledger_id, self.next_sequence, Ulid::new())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceInvocation {
    pub request: MaintenanceRequest,
    pub actor: String,
    pub canonical_root: String,
    pub result_revision: u64,
    /// Admitted applied configuration contract. The actual accepted graph
    /// schema and table pins are bound separately by input.before_commit.
    pub schema_contract: SchemaContractDigest,
    /// Captured under the engine's retained graph gates before any effect.
    pub input: Option<OptimizeInput>,
    /// Exact native publication identity and completed table effects, retained
    /// before its only possible graph publication.
    pub publication: Option<OptimizePublication>,
    /// Native neutral competitor, persisted before recovery may publish it.
    pub settlement: Option<OptimizeSettlement>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum MaintenancePublication {
    Unpublished,
    /// The exact table publication is proven. Postpublication derived work
    /// and the original full successful completion are not asserted.
    Published {
        report: OptimizeReport,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum MaintenanceCompletion {
    Success {
        report: OptimizeReport,
    },
    /// Only a proven pre-effect refusal may close an accepted operation.
    Refused {
        code: String,
        message: String,
    },
    Interrupted {
        publication: MaintenancePublication,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceResult {
    pub operation_id: String,
    pub graph: String,
    pub invocation: MaintenanceInvocation,
    #[serde(flatten)]
    pub completion: MaintenanceCompletion,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MaintenanceLookup {
    Outstanding {
        operation_id: String,
        graph: String,
        invocation: MaintenanceInvocation,
    },
    Complete {
        result: MaintenanceResult,
    },
    /// The sequence was consumed. Missing retained evidence establishes neither
    /// acceptance of this nonce nor its outcome, and never permits replay.
    ResultExpired {
        operation_id: String,
    },
    NotRecorded {
        operation_id: String,
    },
    IdentityMismatch {
        operation_id: String,
    },
    DifferentLedger {
        operation_id: String,
    },
}

/// This value cannot be constructed from a stored receipt or cloned. Keeping
/// its admission alive also prevents a clean owner release during execution.
#[derive(Debug)]
pub struct AcceptedMaintenance {
    admission: ClusterAdmission,
    invocation: MaintenanceInvocation,
}

impl AcceptedMaintenance {
    pub fn request(&self) -> &MaintenanceRequest {
        &self.invocation.request
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum MaintenanceAcceptance {
    Accepted(AcceptedMaintenance),
    Existing(MaintenanceLookup),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    ledger_id: String,
    next_sequence: u64,
    outstanding: Option<MaintenanceInvocation>,
    results: Vec<MaintenanceResult>,
}

struct OperationId<'a> {
    ledger: &'a str,
    sequence: u64,
}

impl<'a> OperationId<'a> {
    fn parse(value: &'a str) -> Result<Self, Diagnostic> {
        let invalid = || refusal("maintenance_id_invalid", "invalid maintenance identity");
        if value.len() > 75 {
            return Err(invalid());
        }
        let mut parts = value.split(':');
        let (Some(ledger), Some(sequence), Some(nonce), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(invalid());
        };
        let sequence = sequence
            .parse::<u64>()
            .ok()
            .filter(|n| *n > 0 && n.to_string() == value.split(':').nth(1).unwrap_or_default())
            .ok_or_else(invalid)?;
        if !canonical_ulid(ledger) || !canonical_ulid(nonce) {
            return Err(invalid());
        }
        Ok(Self { ledger, sequence })
    }
}

fn canonical_ulid(value: &str) -> bool {
    value
        .parse::<Ulid>()
        .is_ok_and(|id| id.to_string() == value)
}

pub(crate) fn refusal(code: &str, message: &str) -> Diagnostic {
    Diagnostic::error(code, MAINTENANCE_FILE, message)
}

fn encode(value: &impl Serialize) -> Result<String, Diagnostic> {
    serde_json::to_string(value)
        .map_err(|_| refusal("maintenance_encode", "could not encode maintenance record"))
}

fn validate_request(request: &MaintenanceRequest) -> Result<(), Diagnostic> {
    OperationId::parse(&request.operation_id)?;
    if request.graph.is_empty()
        || request.graph.len() > 256
        || request
            .graph
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\'))
        || request.graph == "."
        || request.graph == ".."
    {
        return Err(refusal(
            "maintenance_graph_invalid",
            "invalid graph identifier",
        ));
    }
    let limits = &request.limits;
    if limits.max_tables == 0
        || limits.max_fragments == 0
        || limits.max_rows == 0
        || limits.max_source_bytes == 0
        || limits.max_files == 0
    {
        return Err(refusal(
            "maintenance_limits_invalid",
            "every optimization limit must be positive",
        ));
    }
    Ok(())
}

impl Journal {
    fn empty(state: &ClusterState) -> Result<Self, Diagnostic> {
        let ledger_id = state
            .ledger_id
            .clone()
            .filter(|id| canonical_ulid(id))
            .ok_or_else(|| {
                refusal(
                    "maintenance_ledger_required",
                    "maintenance requires a native v2 ledger",
                )
            })?;
        Ok(Self {
            version: 1,
            ledger_id,
            next_sequence: 1,
            outstanding: None,
            results: vec![],
        })
    }

    fn validate(&self) -> Result<(), Diagnostic> {
        let invalid = || {
            refusal(
                "maintenance_journal_invalid",
                "invalid maintenance journal; preserve it for recovery",
            )
        };
        if self.version != 1
            || !canonical_ulid(&self.ledger_id)
            || self.next_sequence == 0
            || self.results.len() > MAX_RESULTS
            || encode(self)?.len() > MAX_JOURNAL_BYTES
        {
            return Err(invalid());
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut total = 0usize;
        for entry in self
            .results
            .iter()
            .map(|result| &result.invocation)
            .chain(self.outstanding.iter())
        {
            validate_request(&entry.request)?;
            IdentityAuthorization::authenticated(&entry.actor)?;
            if let Some(input) = &entry.input {
                validate_input(input, &entry.request.limits)?;
            }
            if let Some(publication) = &entry.publication {
                validate_publication(entry, publication)?;
            }
            if entry.settlement.is_some() && entry.publication.is_none() {
                return Err(invalid());
            }
            let id = OperationId::parse(&entry.request.operation_id)?;
            if id.ledger != self.ledger_id
                || id.sequence >= self.next_sequence
                || !seen.insert(id.sequence)
                || entry.canonical_root.is_empty()
            {
                return Err(invalid());
            }
        }
        if let Some(outstanding) = &self.outstanding {
            if OperationId::parse(&outstanding.request.operation_id)?.sequence
                != self.next_sequence - 1
            {
                return Err(invalid());
            }
        }
        for result in &self.results {
            if result.operation_id != result.invocation.request.operation_id
                || result.graph != result.invocation.request.graph
            {
                return Err(invalid());
            }
            validate_completion(&result.completion)?;
            validate_success_input(&result.invocation, &result.completion)?;
            let bytes = encode(result)?.len();
            if bytes > MAX_RESULT_BYTES {
                return Err(invalid());
            }
            total = total.checked_add(bytes).ok_or_else(invalid)?;
        }
        if total > MAX_RESULTS_BYTES {
            return Err(invalid());
        }
        Ok(())
    }

    fn record(&self, id: &str) -> Option<&MaintenanceInvocation> {
        self.outstanding
            .as_ref()
            .filter(|entry| entry.request.operation_id == id)
            .or_else(|| {
                self.results
                    .iter()
                    .find(|entry| entry.operation_id == id)
                    .map(|entry| &entry.invocation)
            })
    }

    fn lookup(&self, id: &str) -> Result<MaintenanceLookup, Diagnostic> {
        let parsed = OperationId::parse(id)?;
        let operation_id = id.to_string();
        if parsed.ledger != self.ledger_id {
            return Ok(MaintenanceLookup::DifferentLedger { operation_id });
        }
        if let Some(entry) = &self.outstanding {
            if entry.request.operation_id == id {
                return Ok(MaintenanceLookup::Outstanding {
                    operation_id,
                    graph: entry.request.graph.clone(),
                    invocation: entry.clone(),
                });
            }
        }
        if let Some(result) = self.results.iter().find(|entry| entry.operation_id == id) {
            return Ok(MaintenanceLookup::Complete {
                result: result.clone(),
            });
        }
        let occupied = self
            .results
            .iter()
            .map(|entry| &entry.operation_id)
            .chain(
                self.outstanding
                    .iter()
                    .map(|entry| &entry.request.operation_id),
            )
            .any(|entry| {
                OperationId::parse(entry).is_ok_and(|found| found.sequence == parsed.sequence)
            });
        Ok(if occupied {
            MaintenanceLookup::IdentityMismatch { operation_id }
        } else if parsed.sequence < self.next_sequence {
            MaintenanceLookup::ResultExpired { operation_id }
        } else {
            MaintenanceLookup::NotRecorded { operation_id }
        })
    }
}

async fn load(
    store: &ClusterStore,
    state: &ClusterState,
) -> Result<(Journal, Option<String>), Diagnostic> {
    match store.read_maintenance().await? {
        None => Ok((Journal::empty(state)?, None)),
        Some((text, version)) => {
            let journal: Journal = serde_json::from_str(&text).map_err(|_| {
                refusal(
                    "maintenance_journal_invalid",
                    "could not decode maintenance journal",
                )
            })?;
            journal.validate()?;
            if Some(journal.ledger_id.as_str()) != state.ledger_id.as_deref() {
                return Err(refusal(
                    "maintenance_ledger_mismatch",
                    "maintenance journal belongs to another native ledger",
                ));
            }
            let root = store.canonical_root()?;
            if journal
                .results
                .iter()
                .map(|entry| &entry.invocation)
                .chain(journal.outstanding.iter())
                .any(|entry| entry.canonical_root != root)
            {
                return Err(refusal(
                    "maintenance_root_mismatch",
                    "maintenance journal belongs to another canonical root",
                ));
            }
            Ok((journal, Some(version)))
        }
    }
}

async fn state(admission: &ClusterAdmission) -> Result<(ClusterStore, ClusterState), Diagnostic> {
    admission.validate_serving()?;
    admission.validate_deployment().await?;
    let store = admission.store();
    let state = store
        .read_state(&mut store.observations())
        .await?
        .state
        .ok_or_else(|| refusal("maintenance_ledger_required", "native ledger is missing"))?;
    if state.version != 2 {
        return Err(refusal(
            "maintenance_ledger_required",
            "maintenance requires a native v2 ledger",
        ));
    }
    Ok((store, state))
}

async fn authorize(
    store: &ClusterStore,
    state: &ClusterState,
    graph: &str,
    identity: &IdentityAuthorization,
) -> Result<(), Diagnostic> {
    if !state
        .applied_revision
        .schema_contracts
        .as_ref()
        .is_some_and(|contracts| contracts.contains_key(graph))
    {
        return Err(refusal(
            "graph_not_applied",
            "graph is not in the applied native inventory",
        ));
    }
    AppliedPolicies::load_optional(store, state)
        .await?
        .check_graph(identity.actor(), graph, PolicyAction::Optimize)
}

/// Read-only status never creates a journal or reserves an operation ID.
pub async fn maintenance_status(
    admission: &ClusterAdmission,
    graph: &str,
    identity: &IdentityAuthorization,
) -> Result<MaintenanceStatus, Diagnostic> {
    let (store, state) = state(admission).await?;
    authorize(&store, &state, graph, identity).await?;
    let (journal, _) = load(&store, &state).await?;
    Ok(MaintenanceStatus {
        ledger_id: journal.ledger_id,
        next_sequence: journal.next_sequence,
        outstanding_id: journal
            .outstanding
            .filter(|entry| entry.request.graph == graph)
            .map(|entry| entry.request.operation_id),
    })
}

/// The original initiator may read only this retained receipt after revocation.
/// Unknown/expired identities require current permission on the requested graph.
pub async fn maintenance_lookup(
    admission: &ClusterAdmission,
    graph: &str,
    operation_id: &str,
    identity: &IdentityAuthorization,
) -> Result<MaintenanceLookup, Diagnostic> {
    OperationId::parse(operation_id)?;
    let (store, state) = state(admission).await?;
    let (journal, _) = load(&store, &state).await?;
    if let Some(record) = journal.record(operation_id) {
        if record.request.graph != graph {
            return Err(refusal(
                "maintenance_graph_mismatch",
                "operation does not match the requested graph",
            ));
        }
        if record.actor != identity.actor() {
            authorize(&store, &state, graph, identity).await?;
        }
    } else {
        authorize(&store, &state, graph, identity).await?;
    }
    journal.lookup(operation_id)
}

async fn recorded_request(
    store: &ClusterStore,
    state: &ClusterState,
    journal: &Journal,
    request: &MaintenanceRequest,
    identity: &IdentityAuthorization,
) -> Result<Option<MaintenanceLookup>, Diagnostic> {
    if let Some(record) = journal.record(&request.operation_id) {
        if record.actor != identity.actor() || record.request != *request {
            return Err(refusal(
                "maintenance_input_mismatch",
                "operation identity has different immutable input or principal",
            ));
        }
        return journal.lookup(&request.operation_id).map(Some);
    }
    authorize(store, state, &request.graph, identity).await?;
    let lookup = journal.lookup(&request.operation_id)?;
    Ok((!matches!(lookup, MaintenanceLookup::NotRecorded { .. })).then_some(lookup))
}

/// Compare an original request without reserving or executing anything. A
/// missing identity must still pass ordinary admission under the process gate.
/// Retained duplicates may be read while their operation owns that gate.
pub async fn lookup_maintenance_request(
    admission: &ClusterAdmission,
    request: &MaintenanceRequest,
    identity: &IdentityAuthorization,
) -> Result<Option<MaintenanceLookup>, Diagnostic> {
    validate_request(request)?;
    let (store, state) = state(admission).await?;
    let (journal, _) = load(&store, &state).await?;
    recorded_request(&store, &state, &journal, request, identity).await
}

/// Must run under the server's existing deployment/maintenance admission gate.
/// A returned Existing value never confers execution permission.
pub async fn accept_maintenance(
    admission: &ClusterAdmission,
    request: MaintenanceRequest,
    identity: &IdentityAuthorization,
) -> Result<MaintenanceAcceptance, Diagnostic> {
    validate_request(&request)?;
    let (store, state) = state(admission).await?;
    let (mut journal, version) = load(&store, &state).await?;
    if let Some(existing) = recorded_request(&store, &state, &journal, &request, identity).await? {
        return Ok(MaintenanceAcceptance::Existing(existing));
    }
    let id = OperationId::parse(&request.operation_id)?;
    if id.sequence != journal.next_sequence {
        return Err(refusal(
            "maintenance_id_stale",
            "new maintenance must use the exact next sequence",
        ));
    }
    if state.outstanding.is_some() {
        return Err(refusal(
            "cluster_deployment_outstanding",
            "native deployment remains outstanding",
        ));
    }
    if journal.outstanding.is_some() {
        return Err(refusal(
            "cluster_maintenance_outstanding",
            "original maintenance remains outstanding",
        ));
    }
    crate::authorization::refuse_pending_recovery(&store).await?;
    let schema_contract = state
        .applied_revision
        .schema_contracts
        .as_ref()
        .and_then(|contracts| contracts.get(&request.graph))
        .cloned()
        .ok_or_else(|| refusal("graph_not_applied", "graph is not applied"))?;
    let invocation = MaintenanceInvocation {
        request,
        actor: identity.actor().to_string(),
        canonical_root: admission.canonical_root().to_string(),
        result_revision: state
            .applied_revision
            .result_revision
            .ok_or_else(|| refusal("maintenance_ledger_required", "applied revision is missing"))?,
        schema_contract,
        input: None,
        publication: None,
        settlement: None,
    };
    journal.next_sequence = journal.next_sequence.checked_add(1).ok_or_else(|| {
        refusal(
            "maintenance_sequence_exhausted",
            "maintenance sequence is exhausted",
        )
    })?;
    journal.outstanding = Some(invocation.clone());
    journal.validate()?;
    store
        .write_maintenance(&encode(&journal)?, version.as_deref())
        .await?;
    Ok(MaintenanceAcceptance::Accepted(AcceptedMaintenance {
        admission: admission.clone(),
        invocation,
    }))
}

fn validate_completion(completion: &MaintenanceCompletion) -> Result<(), Diagnostic> {
    if let MaintenanceCompletion::Refused { code, message } = completion {
        if code.is_empty()
            || code.len() > 64
            || message.len() > 1024
            || code.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
        {
            return Err(refusal(
                "maintenance_result_invalid",
                "refusal code or message exceeds its bound",
            ));
        }
    }
    Ok(())
}

fn validate_input(input: &OptimizeInput, limits: &OptimizeLimits) -> Result<(), Diagnostic> {
    if input.before_commit.is_empty()
        || input.before_commit.len() > 256
        || input.source_tables > limits.max_tables
        || input.source_fragments > limits.max_fragments
        || input.source_rows > limits.max_rows
        || input.source_bytes > limits.max_source_bytes
        || input.source_files > limits.max_files
        || input.report_max_bytes == 0
    {
        return Err(refusal(
            "maintenance_input_limit",
            "captured native input exceeds its accepted bounds",
        ));
    }
    Ok(())
}

fn validate_success_input(
    invocation: &MaintenanceInvocation,
    completion: &MaintenanceCompletion,
) -> Result<(), Diagnostic> {
    if let MaintenanceCompletion::Success { report }
    | MaintenanceCompletion::Interrupted {
        publication: MaintenancePublication::Published { report },
    } = completion
    {
        let matches = invocation.input.as_ref().is_some_and(|input| {
            input.before_commit == report.before_commit
                && input.source_tables == report.source_tables
                && input.source_fragments == report.source_fragments
                && input.source_rows == report.source_rows
                && input.source_bytes == report.source_bytes
                && input.source_files == report.source_files
        });
        if !matches {
            return Err(refusal(
                "maintenance_input_mismatch",
                "native completion does not match the durably captured input",
            ));
        }
        if encode(report)?.len() as u64
            > invocation
                .input
                .as_ref()
                .expect("matched input")
                .report_max_bytes
        {
            return Err(refusal(
                "maintenance_result_limit",
                "native report exceeds its pre-effect reservation",
            ));
        }
    }
    Ok(())
}

/// Reserve the largest retained envelope before physical effects. A recovery
/// result carries the prepared report as well as its published result; both
/// copies are included. Fixed native identities use their maximal encodings.
fn reserve_result_capacity(invocation: &MaintenanceInvocation) -> Result<(), Diagnostic> {
    let input = invocation
        .input
        .as_ref()
        .expect("reservation follows input binding");
    let witness = serde_json::json!({
        "before_manifest_version": u64::MAX, "intent_nonce": "Z".repeat(26),
        "schema_contract": input.schema_contract, "actor": invocation.actor,
        "created_at": i64::MIN, "report": null,
    });
    let settlement = serde_json::json!({"original_digest":"f".repeat(64),"lineage":{
        "graph_commit_id":"Z".repeat(26),"branch":null,"actor_id":"\\".repeat(256),
        "merged_parent_commit_id":null,"created_at":i64::MIN,
    }});
    let mut envelope = serde_json::json!({
        "operation_id":invocation.request.operation_id,"graph":invocation.request.graph,
        "invocation":invocation,"outcome":"interrupted",
        "publication":{"status":"published","report":null},
    });
    envelope["invocation"]["publication"] = witness;
    envelope["invocation"]["settlement"] = settlement;
    let bytes = (encode(&envelope)?.len() as u64)
        .checked_sub(8)
        .and_then(|n| {
            input
                .report_max_bytes
                .checked_mul(2)
                .and_then(|reports| n.checked_add(reports))
        });
    // Refusals can contain escaped diagnostic bytes, whose JSON spelling is
    // wider than the raw 1 KiB message bound. Reserve that terminal path too.
    let refusal_envelope = serde_json::json!({
        "operation_id":invocation.request.operation_id,"graph":invocation.request.graph,
        "invocation":invocation,"outcome":"refused","code":"a".repeat(64),
        "message":"\0".repeat(1024),
    });
    if bytes.is_none_or(|bytes| bytes > MAX_RESULT_BYTES as u64)
        || encode(&refusal_envelope)?.len() > MAX_RESULT_BYTES
    {
        return Err(refusal(
            "maintenance_result_capacity",
            "exact optimization and recovery results exceed the retention capacity before effects",
        ));
    }
    Ok(())
}

fn validate_publication(
    invocation: &MaintenanceInvocation,
    publication: &OptimizePublication,
) -> Result<(), Diagnostic> {
    validate_success_input(
        invocation,
        &MaintenanceCompletion::Success {
            report: publication.report.clone(),
        },
    )?;
    if publication.before_manifest_version == 0
        || publication.before_manifest_version == u64::MAX
        || !canonical_ulid(&publication.intent_nonce)
        || publication.actor.as_deref() != Some(&invocation.actor)
        || invocation
            .input
            .as_ref()
            .is_none_or(|input| publication.schema_contract != input.schema_contract)
        || publication.report.before_commit != publication.report.after_commit
    {
        return Err(refusal(
            "maintenance_publication_invalid",
            "native publication witness does not match its accepted invocation",
        ));
    }
    Ok(())
}

fn validate_owner(
    admission: &ClusterAdmission,
    accepted: &AcceptedMaintenance,
) -> Result<(), Diagnostic> {
    if admission.lock_id() != accepted.admission.lock_id()
        || admission.canonical_root() != accepted.admission.canonical_root()
    {
        return Err(refusal(
            "maintenance_owner_mismatch",
            "operation belongs to another serving owner",
        ));
    }
    Ok(())
}

fn validate_binding(
    state: &ClusterState,
    accepted: &AcceptedMaintenance,
) -> Result<(), Diagnostic> {
    if state.outstanding.is_some()
        || state.applied_revision.result_revision != Some(accepted.invocation.result_revision)
        || state
            .applied_revision
            .schema_contracts
            .as_ref()
            .and_then(|contracts| contracts.get(&accepted.request().graph))
            != Some(&accepted.invocation.schema_contract)
    {
        return Err(refusal(
            "maintenance_binding_changed",
            "accepted applied configuration changed before completion",
        ));
    }
    Ok(())
}

/// Called once by engine preflight while its exact snapshot and native gates
/// remain held. A failed write leaves uncertainty; it is not a safe refusal.
pub async fn bind_maintenance_input(
    admission: &ClusterAdmission,
    accepted: &mut AcceptedMaintenance,
    input: OptimizeInput,
) -> Result<(), Diagnostic> {
    validate_owner(admission, accepted)?;
    validate_input(&input, &accepted.request().limits)?;
    if accepted.invocation.input.is_some() {
        return Err(refusal(
            "maintenance_input_already_bound",
            "native input is already bound to this operation",
        ));
    }
    let (store, state) = state(admission).await?;
    validate_binding(&state, accepted)?;
    let (mut journal, version) = load(&store, &state).await?;
    if journal.outstanding.as_ref() != Some(&accepted.invocation) {
        return Err(refusal(
            "maintenance_identity_mismatch",
            "original accepted maintenance is no longer outstanding",
        ));
    }
    let mut invocation = accepted.invocation.clone();
    invocation.input = Some(input);
    reserve_result_capacity(&invocation)?;
    journal.outstanding = Some(invocation.clone());
    journal.validate()?;
    store
        .write_maintenance(&encode(&journal)?, version.as_deref())
        .await?;
    accepted.invocation = invocation;
    Ok(())
}

/// Native Core invokes this while retaining the original graph gates and
/// before its exact-version publication. Failure never permits publication.
pub async fn bind_maintenance_publication(
    admission: &ClusterAdmission,
    accepted: &mut AcceptedMaintenance,
    publication: OptimizePublication,
) -> Result<(), Diagnostic> {
    validate_owner(admission, accepted)?;
    validate_publication(&accepted.invocation, &publication)?;
    if accepted.invocation.publication.is_some() {
        return Err(refusal(
            "maintenance_publication_already_bound",
            "native publication is already bound",
        ));
    }
    let (store, state) = state(admission).await?;
    validate_binding(&state, accepted)?;
    let (mut journal, version) = load(&store, &state).await?;
    if journal.outstanding.as_ref() != Some(&accepted.invocation) {
        return Err(refusal(
            "maintenance_identity_mismatch",
            "original maintenance is no longer outstanding",
        ));
    }
    let mut invocation = accepted.invocation.clone();
    invocation.publication = Some(publication);
    journal.outstanding = Some(invocation.clone());
    journal.validate()?;
    store
        .write_maintenance(&encode(&journal)?, version.as_deref())
        .await?;
    accepted.invocation = invocation;
    Ok(())
}

/// Persist the exact native completion once. Failure leaves the original
/// outstanding identity unresolved; it never grants another execution token.
pub async fn complete_maintenance(
    admission: &ClusterAdmission,
    accepted: AcceptedMaintenance,
    completion: MaintenanceCompletion,
) -> Result<MaintenanceResult, Diagnostic> {
    validate_completion(&completion)?;
    validate_owner(admission, &accepted)?;
    validate_success_input(&accepted.invocation, &completion)?;
    let (store, state) = state(admission).await?;
    validate_binding(&state, &accepted)?;
    let (mut journal, version) = load(&store, &state).await?;
    if journal.outstanding.as_ref() != Some(&accepted.invocation) {
        return Err(refusal(
            "maintenance_identity_mismatch",
            "original accepted maintenance is no longer outstanding",
        ));
    }
    let result = MaintenanceResult {
        operation_id: accepted.request().operation_id.clone(),
        graph: accepted.request().graph.clone(),
        invocation: accepted.invocation,
        completion,
    };
    append_result(&mut journal, &result)?;
    store
        .write_maintenance(&encode(&journal)?, version.as_deref())
        .await?;
    Ok(result)
}

fn append_result(journal: &mut Journal, result: &MaintenanceResult) -> Result<(), Diagnostic> {
    if encode(result)?.len() > MAX_RESULT_BYTES {
        return Err(refusal(
            "maintenance_result_limit",
            "exact maintenance result exceeds its retention byte limit",
        ));
    }
    journal.results.push(result.clone());
    while journal.results.len() > MAX_RESULTS
        || journal
            .results
            .iter()
            .map(encode)
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(String::len)
            .sum::<usize>()
            > MAX_RESULTS_BYTES
    {
        journal.results.remove(0);
    }
    journal.outstanding = None;
    journal.validate()?;
    Ok(())
}

pub(crate) async fn require_maintenance_recovery(
    store: &ClusterStore,
    state: &ClusterState,
    operation_id: &str,
) -> Result<(), Diagnostic> {
    OperationId::parse(operation_id)?;
    let (journal, _) = load(store, state).await?;
    if journal
        .outstanding
        .as_ref()
        .is_some_and(|entry| entry.request.operation_id != operation_id)
        || journal.record(operation_id).is_none()
    {
        return Err(refusal(
            "maintenance_recovery_identity_mismatch",
            "only the exact retained original maintenance identity may recover",
        ));
    }
    Ok(())
}

/// Reconcile an original operation under recovery-only native admission.
/// The caller first excludes predecessor processes, other writers and cleanup,
/// then handles the exact prior lock using the existing administrator procedure.
/// No physical optimization is rerun. Errors retain the journal and native lock.
pub async fn reconcile_maintenance(
    admission: &ClusterAdmission,
    graph: &str,
    operation_id: &str,
    identity: &IdentityAuthorization,
) -> Result<MaintenanceResult, Diagnostic> {
    admission
        .validate_maintenance_recovery(operation_id)
        .await?;
    let store = admission.store();
    let state = store
        .read_state(&mut store.observations())
        .await?
        .state
        .ok_or_else(|| refusal("maintenance_ledger_required", "native ledger is missing"))?;
    let (mut journal, mut version) = load(&store, &state).await?;
    let record = journal.record(operation_id).ok_or_else(|| {
        refusal(
            "maintenance_recovery_identity_mismatch",
            "original operation is not retained",
        )
    })?;
    if record.request.graph != graph {
        return Err(refusal(
            "maintenance_graph_mismatch",
            "operation does not match requested graph",
        ));
    }
    authorize(&store, &state, graph, identity).await?;
    if let Some(result) = journal
        .results
        .iter()
        .find(|entry| entry.operation_id == operation_id)
    {
        return Ok(result.clone());
    }
    let mut invocation = journal
        .outstanding
        .clone()
        .filter(|entry| entry.request.operation_id == operation_id)
        .ok_or_else(|| {
            refusal(
                "maintenance_recovery_identity_mismatch",
                "original operation is not outstanding",
            )
        })?;
    let accepted = AcceptedMaintenance {
        admission: admission.clone(),
        invocation: invocation.clone(),
    };
    validate_binding(&state, &accepted)?;
    drop(accepted);
    let publication = match invocation.publication.clone() {
        None => MaintenancePublication::Unpublished,
        Some(prepared) => {
            let policies = AppliedPolicies::load_optional(&store, &state).await?;
            let uri = store.graph_root(graph);
            Omnigraph::ensure_no_pending_recovery_with_storage(&uri, store.graph_storage(&uri)?)
                .await
                .map_err(|error| refusal("graph_recovery_required", &error.to_string()))?;
            let db = Omnigraph::open_with_storage(&uri, store.graph_storage(&uri)?)
                .await
                .map_err(|error| refusal("maintenance_recovery_failed", &error.to_string()))?;
            let db = match policies.graph(graph) {
                Some(policy) => db.with_policy(policy),
                None => db,
            };
            if invocation.settlement.is_none() {
                invocation.settlement = Some(
                    db.prepare_optimize_settlement_as(&prepared, Some(identity.actor()))
                        .await
                        .map_err(|error| {
                            refusal("maintenance_recovery_failed", &error.to_string())
                        })?,
                );
                journal.outstanding = Some(invocation.clone());
                journal.validate()?;
                store
                    .write_maintenance(&encode(&journal)?, version.as_deref())
                    .await?;
                // Reread the committed exact bytes, never guess the CAS token.
                let (written, next_version) = load(&store, &state).await?;
                if written.outstanding.as_ref() != Some(&invocation) {
                    return Err(refusal(
                        "maintenance_identity_mismatch",
                        "recovery binding changed",
                    ));
                }
                journal = written;
                version = next_version;
            }
            admission
                .validate_maintenance_recovery(operation_id)
                .await?;
            match db
                .settle_optimize_as(
                    &prepared,
                    invocation
                        .settlement
                        .as_ref()
                        .expect("persisted settlement"),
                    Some(identity.actor()),
                )
                .await
                .map_err(|error| refusal("maintenance_recovery_failed", &error.to_string()))?
            {
                OptimizeSettlementOutcome::Published { report } => {
                    MaintenancePublication::Published { report }
                }
                OptimizeSettlementOutcome::Unpublished => MaintenancePublication::Unpublished,
                OptimizeSettlementOutcome::Unknown => {
                    return Err(refusal(
                        "maintenance_recovery_unknown",
                        "original publication remains unknown; retain original identity and ownership",
                    ));
                }
            }
        }
    };
    admission
        .validate_maintenance_recovery(operation_id)
        .await?;
    let current = store
        .read_state(&mut store.observations())
        .await?
        .state
        .ok_or_else(|| refusal("maintenance_ledger_required", "native ledger is missing"))?;
    let accepted = AcceptedMaintenance {
        admission: admission.clone(),
        invocation: invocation.clone(),
    };
    validate_binding(&current, &accepted)?;
    drop(accepted);
    let result = MaintenanceResult {
        operation_id: operation_id.to_string(),
        graph: graph.to_string(),
        invocation,
        completion: MaintenanceCompletion::Interrupted { publication },
    };
    append_result(&mut journal, &result)?;
    store
        .write_maintenance(&encode(&journal)?, version.as_deref())
        .await?;
    Ok(result)
}

/// Deployment and clean release must not erase unresolved maintenance evidence.
/// This check is observational; the owning server also serializes acceptance.
pub(crate) async fn require_no_maintenance(store: &ClusterStore) -> Result<(), Diagnostic> {
    let Some((text, _)) = store.read_maintenance().await? else {
        return Ok(());
    };
    let journal: Journal = serde_json::from_str(&text).map_err(|_| {
        refusal(
            "maintenance_journal_invalid",
            "could not decode maintenance journal",
        )
    })?;
    journal.validate()?;
    if journal.outstanding.is_some() {
        return Err(refusal(
            "cluster_maintenance_outstanding",
            "original maintenance remains outstanding; ownership must be retained",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        ClusterAdmissionPurpose, DeploymentCaller, DeploymentLookup, acquire_cluster_admission,
        apply_deployment, deployment_status, force_unlock_storage_root,
    };
    use std::fs;

    fn identity(actor: &str) -> IdentityAuthorization {
        IdentityAuthorization::authenticated(actor).unwrap()
    }

    fn limits() -> OptimizeLimits {
        OptimizeLimits {
            max_tables: 8,
            max_fragments: 64,
            max_rows: 1000,
            max_source_bytes: 1024 * 1024,
            max_files: 128,
        }
    }

    pub(crate) async fn fixture() -> (tempfile::TempDir, ClusterAdmission) {
        let dir = crate::tests::identity_fixture();
        let policy = dir.path().join("base.policy.yaml");
        let source = fs::read_to_string(&policy).unwrap();
        fs::write(policy, format!("{source}\n  - id: owners-optimize\n    allow: {{actors: {{group: owners}}, actions: [optimize], target_branch_scope: protected}}\nprotected_branches: [main]\n")).unwrap();
        let caller = DeploymentCaller::storage_owner(None);
        let result = apply_deployment(dir.path(), None, &caller, |_, _, _| {})
            .await
            .unwrap();
        assert!(matches!(result, DeploymentLookup::Complete { ref result } if result.converged));
        let root = dir.path().to_str().unwrap();
        if let Some(lock) = deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
        {
            force_unlock_storage_root(root, &lock).await.unwrap();
        }
        let admission = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
            .await
            .unwrap()
            .unwrap();
        (dir, admission)
    }

    pub(crate) async fn request(admission: &ClusterAdmission) -> MaintenanceRequest {
        let status = maintenance_status(admission, "knowledge", &identity("principal:owner"))
            .await
            .unwrap();
        MaintenanceRequest {
            operation_id: status.next_operation_id(),
            graph: "knowledge".into(),
            kind: MaintenanceKind::Optimize,
            limits: limits(),
        }
    }

    fn refusal_completion() -> MaintenanceCompletion {
        MaintenanceCompletion::Refused {
            code: "optimize_limit_exceeded".into(),
            message: "source exceeds the accepted bound before effects".into(),
        }
    }

    #[tokio::test]
    async fn admission_is_durable_permission_based_and_duplicate_never_executes() {
        let (dir, admission) = fixture().await;
        let request = request(&admission).await;
        assert!(
            lookup_maintenance_request(&admission, &request, &identity("principal:owner"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !dir.path().join(MAINTENANCE_FILE).exists(),
            "status must not create state"
        );
        assert!(
            maintenance_status(&admission, "knowledge", &identity("principal:reader"))
                .await
                .is_err()
        );
        assert!(
            accept_maintenance(&admission, request.clone(), &identity("principal:reader"))
                .await
                .is_err()
        );
        assert!(!dir.path().join(MAINTENANCE_FILE).exists());
        let MaintenanceAcceptance::Accepted(accepted) =
            accept_maintenance(&admission, request.clone(), &identity("principal:owner"))
                .await
                .unwrap()
        else {
            panic!("expected execution token")
        };
        assert!(dir.path().join(MAINTENANCE_FILE).exists());
        let journal_bytes = fs::read(dir.path().join(MAINTENANCE_FILE)).unwrap();
        assert!(matches!(
            lookup_maintenance_request(&admission, &request, &identity("principal:owner"))
                .await
                .unwrap(),
            Some(MaintenanceLookup::Outstanding { .. })
        ));
        assert_eq!(
            fs::read(dir.path().join(MAINTENANCE_FILE)).unwrap(),
            journal_bytes
        );
        assert!(matches!(
            accept_maintenance(&admission, request.clone(), &identity("principal:owner"))
                .await
                .unwrap(),
            MaintenanceAcceptance::Existing(MaintenanceLookup::Outstanding { .. })
        ));
        let mut mismatch = request.clone();
        mismatch.limits.max_rows += 1;
        assert_eq!(
            accept_maintenance(&admission, mismatch, &identity("principal:owner"))
                .await
                .unwrap_err()
                .code,
            "maintenance_input_mismatch"
        );
        assert_eq!(
            accept_maintenance(
                &admission,
                request.clone(),
                &identity("principal:collaborator")
            )
            .await
            .unwrap_err()
            .code,
            "maintenance_input_mismatch"
        );
        assert_eq!(
            require_no_maintenance(&admission.store())
                .await
                .unwrap_err()
                .code,
            "cluster_maintenance_outstanding"
        );
        let next = self::request(&admission).await;
        assert_eq!(
            accept_maintenance(&admission, next, &identity("principal:owner"))
                .await
                .unwrap_err()
                .code,
            "cluster_maintenance_outstanding"
        );
        let result = complete_maintenance(&admission, accepted, refusal_completion())
            .await
            .unwrap();
        assert_eq!(result.operation_id, request.operation_id);
        let encoded = serde_json::to_value(&result).unwrap();
        assert_eq!(encoded["outcome"], "refused");
        assert!(encoded.get("report").is_none());
        let decoded: MaintenanceResult = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, result);
        assert!(matches!(
            accept_maintenance(&admission, request, &identity("principal:owner"))
                .await
                .unwrap(),
            MaintenanceAcceptance::Existing(MaintenanceLookup::Complete { .. })
        ));
        require_no_maintenance(&admission.store()).await.unwrap();
        // Production shutdown closes mutation admission before native release;
        // the journal blocker check is a read and must remain possible.
        admission.io_scope().unwrap().close();
        admission.release_after_settlement().await.unwrap();
    }

    #[tokio::test]
    async fn abandoned_acceptance_blocks_release_and_original_lookup_is_retained() {
        let (dir, admission) = fixture().await;
        let request = request(&admission).await;
        let MaintenanceAcceptance::Accepted(token) =
            accept_maintenance(&admission, request.clone(), &identity("principal:owner"))
                .await
                .unwrap()
        else {
            panic!()
        };
        drop(token);
        let bundle = crate::capture_deployment(dir.path()).unwrap();
        let mut effects_started = false;
        let deployment = crate::apply_captured_deployment(
            &bundle,
            None,
            &DeploymentCaller::storage_owner(None),
            &admission,
            &std::collections::BTreeMap::new(),
            |_, _, _| {},
            |_| {},
            &mut effects_started,
        )
        .await
        .unwrap_err();
        assert_eq!(deployment.code, "cluster_maintenance_outstanding");
        assert!(!effects_started);
        assert!(matches!(
            maintenance_lookup(
                &admission,
                "knowledge",
                &request.operation_id,
                &identity("principal:owner")
            )
            .await
            .unwrap(),
            MaintenanceLookup::Outstanding { .. }
        ));
        assert_eq!(
            maintenance_lookup(
                &admission,
                "other",
                &request.operation_id,
                &identity("principal:owner")
            )
            .await
            .unwrap_err()
            .code,
            "maintenance_graph_mismatch"
        );
        assert!(
            maintenance_lookup(
                &admission,
                "knowledge",
                &request.operation_id,
                &identity("principal:reader")
            )
            .await
            .is_err()
        );
        assert_eq!(
            admission.release_after_settlement().await.unwrap_err().code,
            "cluster_maintenance_outstanding"
        );
        // Administrative process exclusion and lock retirement do not settle
        // the accepted operation. Every later writable admission must still
        // preserve that unresolved identity instead of reopening its graphs.
        let root = dir.path().to_str().unwrap();
        for purpose in [
            ClusterAdmissionPurpose::Serve,
            ClusterAdmissionPurpose::GraphOperation,
            ClusterAdmissionPurpose::Deployment,
        ] {
            let lock = deployment_status(root, None, &DeploymentCaller::storage_owner(None))
                .await
                .unwrap()
                .lock_id
                .unwrap();
            force_unlock_storage_root(root, &lock).await.unwrap();
            let error = acquire_cluster_admission(root, purpose)
                .await
                .expect_err("an unresolved optimization must block successor writers");
            assert!(
                error
                    .message
                    .contains("original maintenance remains outstanding")
            );
        }
        let lock = deployment_status(root, None, &DeploymentCaller::storage_owner(None))
            .await
            .unwrap()
            .lock_id
            .unwrap();
        force_unlock_storage_root(root, &lock).await.unwrap();
        let recovery = acquire_cluster_admission(
            root,
            ClusterAdmissionPurpose::MaintenanceRecovery {
                operation_id: request.operation_id.clone(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert!(recovery.validate_serving().is_err());
        assert!(recovery.validate_deployment().await.is_err());
        assert!(
            recovery
                .validate_graph_uri(&format!("{root}/graphs/knowledge.omni"))
                .await
                .is_err()
        );
        assert!(
            reconcile_maintenance(
                &recovery,
                "knowledge",
                &request.operation_id,
                &identity("principal:reader")
            )
            .await
            .is_err()
        );
        assert_eq!(
            reconcile_maintenance(
                &recovery,
                "other",
                &request.operation_id,
                &identity("principal:owner")
            )
            .await
            .unwrap_err()
            .code,
            "maintenance_graph_mismatch"
        );
        let result = reconcile_maintenance(
            &recovery,
            "knowledge",
            &request.operation_id,
            &identity("principal:owner"),
        )
        .await
        .unwrap();
        assert!(matches!(
            result.completion,
            MaintenanceCompletion::Interrupted {
                publication: MaintenancePublication::Unpublished
            }
        ));
        assert_eq!(result.invocation.actor, "principal:owner");
        let bytes = fs::read(dir.path().join(MAINTENANCE_FILE)).unwrap();
        let duplicate = reconcile_maintenance(
            &recovery,
            "knowledge",
            &request.operation_id,
            &identity("principal:owner"),
        )
        .await
        .unwrap();
        assert_eq!(duplicate, result);
        assert_eq!(fs::read(dir.path().join(MAINTENANCE_FILE)).unwrap(), bytes);
        recovery.release_after_settlement().await.unwrap();
        let successor = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            maintenance_lookup(
                &successor,
                "knowledge",
                &request.operation_id,
                &identity("principal:owner")
            )
            .await
            .unwrap(),
            MaintenanceLookup::Complete { .. }
        ));
        successor.release_after_settlement().await.unwrap();
    }

    #[tokio::test]
    async fn result_capacity_refuses_before_binding_or_effect_permission() {
        fn graph_files(
            root: &std::path::Path,
        ) -> std::collections::BTreeMap<std::path::PathBuf, String> {
            let mut pending = vec![root.to_path_buf()];
            let mut files = std::collections::BTreeMap::new();
            while let Some(directory) = pending.pop() {
                for entry in fs::read_dir(directory).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        pending.push(path);
                    } else {
                        files.insert(
                            path.strip_prefix(root).unwrap().to_path_buf(),
                            crate::sha256_hex(&fs::read(path).unwrap()),
                        );
                    }
                }
            }
            files
        }
        let (dir, admission) = fixture().await;
        let store = admission.store();
        let uri = store.graph_root("knowledge");
        let db = Omnigraph::open_with_storage(&uri, store.graph_storage(&uri).unwrap())
            .await
            .unwrap();
        // Real accepted metadata, not a fabricated report: the retained table
        // name alone makes the original witness plus result exceed one MiB.
        // Z sorts after productive Person, proving the entire attempt preflights.
        let long_type = "Z".repeat(600 * 1024);
        db.apply_schema(&format!(
            "{}\nnode {long_type} {{ name: String @key }}\n",
            db.schema_source()
        ))
        .await
        .unwrap();
        let db = omnigraph::Session::from_defaults(
            std::sync::Arc::new(db),
            omnigraph::settings::SessionSettings::default(),
        );
        for name in ["Ada", "Grace", "Barbara"] {
            db.load_jsonl(
                &format!(r#"{{"type":"Person","data":{{"name":"{name}","age":37}}}}"#),
                omnigraph::loader::LoadMode::Merge,
            )
            .await
            .unwrap();
        }
        let snapshot = db
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        assert!(snapshot.dataset(&format!("node:{long_type}")).is_some());
        let graph_path = dir.path().join("graphs/knowledge.omni");
        let physical_before = graph_files(&graph_path);
        let mut request = request(&admission).await;
        request.limits.max_source_bytes = 64 * 1024 * 1024;
        let limits = request.limits.clone();
        let MaintenanceAcceptance::Accepted(mut accepted) =
            accept_maintenance(&admission, request, &identity("principal:owner"))
                .await
                .unwrap()
        else {
            panic!()
        };
        let before = fs::read(dir.path().join(MAINTENANCE_FILE)).unwrap();
        let accepted_ref = &mut accepted;
        let admission_ref = &admission;
        let failure = db
            .optimize_with_limits_admitted_as(
                &limits,
                Some("principal:owner"),
                |input| async move {
                    assert!(input.report_max_bytes * 2 > MAX_RESULT_BYTES as u64);
                    let error = bind_maintenance_input(admission_ref, accepted_ref, input)
                        .await
                        .unwrap_err();
                    assert_eq!(error.code, "maintenance_result_capacity");
                    Err(omnigraph::error::OmniError::manifest(error.code))
                },
            )
            .await
            .unwrap_err();
        assert!(!failure.effects_started);
        assert!(
            failure
                .error
                .to_string()
                .contains("maintenance_result_capacity")
        );
        assert!(accepted.invocation.input.is_none());
        assert_eq!(fs::read(dir.path().join(MAINTENANCE_FILE)).unwrap(), before);
        assert_eq!(
            graph_files(&graph_path),
            physical_before,
            "preflight must not write graph files"
        );
        let after = db
            .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        assert_eq!(
            after.graph_manifest_version(),
            snapshot.graph_manifest_version()
        );
        assert_eq!(after.graph_head(None), snapshot.graph_head(None));
        for entry in snapshot.datasets() {
            let unchanged = after.dataset(&entry.type_key).unwrap();
            assert_eq!(unchanged.identity, entry.identity);
            assert_eq!(unchanged.dataset_path, entry.dataset_path);
            assert_eq!(
                unchanged.published_dataset_version,
                entry.published_dataset_version
            );
            assert_eq!(unchanged.native_dataset_branch, entry.native_dataset_branch);
            assert_eq!(unchanged.entity_count, entry.entity_count);
        }
        let result = complete_maintenance(
            &admission,
            accepted,
            MaintenanceCompletion::Refused {
                code: "maintenance_result_capacity".into(),
                message: "result cannot fit".into(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(result.completion, MaintenanceCompletion::Refused { ref code, .. } if code == "maintenance_result_capacity")
        );
        require_no_maintenance(&admission.store()).await.unwrap();
        let next = self::request(&admission).await;
        let MaintenanceAcceptance::Accepted(next) =
            accept_maintenance(&admission, next, &identity("principal:owner"))
                .await
                .unwrap()
        else {
            panic!()
        };
        complete_maintenance(&admission, next, refusal_completion())
            .await
            .unwrap();
        admission.release_after_settlement().await.unwrap();
    }

    #[tokio::test]
    async fn exact_input_is_bound_once_and_success_must_report_that_input() {
        let (_dir, admission) = fixture().await;
        let request = request(&admission).await;
        let actor = identity("principal:owner");
        let MaintenanceAcceptance::Accepted(mut accepted) =
            accept_maintenance(&admission, request.clone(), &actor)
                .await
                .unwrap()
        else {
            panic!()
        };
        let input = OptimizeInput {
            before_commit: "retained-commit".into(),
            report_max_bytes: 16384,
            schema_contract: accepted.invocation.schema_contract.clone(),
            source_tables: 1,
            source_fragments: 2,
            source_rows: 5,
            source_bytes: 1024,
            source_files: 2,
        };
        bind_maintenance_input(&admission, &mut accepted, input.clone())
            .await
            .unwrap();
        assert_eq!(
            bind_maintenance_input(&admission, &mut accepted, input.clone())
                .await
                .unwrap_err()
                .code,
            "maintenance_input_already_bound"
        );
        let MaintenanceLookup::Outstanding { invocation, .. } =
            maintenance_lookup(&admission, "knowledge", &request.operation_id, &actor)
                .await
                .unwrap()
        else {
            panic!()
        };
        assert_eq!(invocation.input, Some(input.clone()));
        let mut report = OptimizeReport {
            before_commit: input.before_commit,
            after_commit: "result-commit".into(),
            tables: vec![],
            source_tables: input.source_tables,
            source_fragments: input.source_fragments,
            source_rows: input.source_rows,
            source_bytes: input.source_bytes,
            source_files: input.source_files,
        };
        report.source_rows += 1;
        assert!(
            validate_success_input(
                &accepted.invocation,
                &MaintenanceCompletion::Success {
                    report: report.clone()
                }
            )
            .is_err()
        );
        report.source_rows -= 1;
        let result = complete_maintenance(
            &admission,
            accepted,
            MaintenanceCompletion::Success { report },
        )
        .await
        .unwrap();
        let encoded = serde_json::to_value(&result).unwrap();
        assert_eq!(encoded["outcome"], "success");
        assert_eq!(
            serde_json::from_value::<MaintenanceResult>(encoded).unwrap(),
            result
        );
        admission.release_after_settlement().await.unwrap();
    }

    #[tokio::test]
    async fn revoked_initiator_can_read_only_its_retained_receipt() {
        let (dir, admission) = fixture().await;
        let actor = identity("principal:owner");
        let request = request(&admission).await;
        let MaintenanceAcceptance::Accepted(accepted) =
            accept_maintenance(&admission, request.clone(), &actor)
                .await
                .unwrap()
        else {
            panic!()
        };
        complete_maintenance(&admission, accepted, refusal_completion())
            .await
            .unwrap();
        admission.release_after_settlement().await.unwrap();
        let policy = dir.path().join("base.policy.yaml");
        let source = fs::read_to_string(&policy).unwrap();
        let without_optimize = source.split("  - id: owners-optimize").next().unwrap();
        fs::write(policy, without_optimize).unwrap();
        let caller = DeploymentCaller::storage_owner(None);
        let applied = apply_deployment(dir.path(), None, &caller, |_, _, _| {})
            .await
            .unwrap();
        assert!(matches!(applied, DeploymentLookup::Complete { ref result } if result.converged));
        let root = dir.path().to_str().unwrap();
        if let Some(lock) = deployment_status(root, None, &caller)
            .await
            .unwrap()
            .lock_id
        {
            force_unlock_storage_root(root, &lock).await.unwrap();
        }
        let admission = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
            .await
            .unwrap()
            .unwrap();
        assert!(
            maintenance_status(&admission, "knowledge", &actor)
                .await
                .is_err()
        );
        assert!(matches!(
            maintenance_lookup(&admission, "knowledge", &request.operation_id, &actor)
                .await
                .unwrap(),
            MaintenanceLookup::Complete { .. }
        ));
        assert!(
            maintenance_lookup(
                &admission,
                "knowledge",
                &request.operation_id,
                &identity("principal:collaborator")
            )
            .await
            .is_err()
        );
        let parsed = OperationId::parse(&request.operation_id).unwrap();
        let unknown = format!("{}:{}:{}", parsed.ledger, parsed.sequence + 1, Ulid::new());
        assert!(
            maintenance_lookup(&admission, "knowledge", &unknown, &actor)
                .await
                .is_err()
        );
        admission.release_after_settlement().await.unwrap();
    }

    #[tokio::test]
    async fn oversized_exact_result_remains_outstanding_instead_of_truncated_success() {
        let (_dir, admission) = fixture().await;
        let actor = identity("principal:owner");
        let request = request(&admission).await;
        let MaintenanceAcceptance::Accepted(mut accepted) =
            accept_maintenance(&admission, request.clone(), &actor)
                .await
                .unwrap()
        else {
            panic!()
        };
        let input = OptimizeInput {
            before_commit: "retained-commit".into(),
            report_max_bytes: 16384,
            schema_contract: accepted.invocation.schema_contract.clone(),
            source_tables: 0,
            source_fragments: 0,
            source_rows: 0,
            source_bytes: 0,
            source_files: 0,
        };
        bind_maintenance_input(&admission, &mut accepted, input.clone())
            .await
            .unwrap();
        let report = OptimizeReport {
            before_commit: input.before_commit,
            after_commit: "x".repeat(MAX_RESULT_BYTES),
            tables: vec![],
            source_tables: 0,
            source_fragments: 0,
            source_rows: 0,
            source_bytes: 0,
            source_files: 0,
        };
        assert_eq!(
            complete_maintenance(
                &admission,
                accepted,
                MaintenanceCompletion::Success { report }
            )
            .await
            .unwrap_err()
            .code,
            "maintenance_result_limit"
        );
        assert!(matches!(
            maintenance_lookup(&admission, "knowledge", &request.operation_id, &actor)
                .await
                .unwrap(),
            MaintenanceLookup::Outstanding { .. }
        ));
        assert_eq!(
            admission.release_after_settlement().await.unwrap_err().code,
            "cluster_maintenance_outstanding"
        );
    }

    #[tokio::test]
    async fn retention_expiry_is_never_replay_permission_and_cas_rejects_stale_bytes() {
        let (_dir, admission) = fixture().await;
        let actor = identity("principal:owner");
        let first = request(&admission).await;
        for index in 0..=MAX_RESULTS {
            let request = if index == 0 {
                first.clone()
            } else {
                self::request(&admission).await
            };
            let MaintenanceAcceptance::Accepted(accepted) =
                accept_maintenance(&admission, request, &actor)
                    .await
                    .unwrap()
            else {
                panic!()
            };
            complete_maintenance(&admission, accepted, refusal_completion())
                .await
                .unwrap();
        }
        assert!(matches!(
            maintenance_lookup(&admission, "knowledge", &first.operation_id, &actor)
                .await
                .unwrap(),
            MaintenanceLookup::ResultExpired { .. }
        ));
        assert!(matches!(
            accept_maintenance(&admission, first, &actor).await.unwrap(),
            MaintenanceAcceptance::Existing(MaintenanceLookup::ResultExpired { .. })
        ));
        let current = request(&admission).await;
        let mut future = current.clone();
        let parsed = OperationId::parse(&future.operation_id).unwrap();
        future.operation_id = format!("{}:{}:{}", parsed.ledger, parsed.sequence + 1, Ulid::new());
        assert_eq!(
            accept_maintenance(&admission, future, &actor)
                .await
                .unwrap_err()
                .code,
            "maintenance_id_stale"
        );
        let store = admission.store();
        let (previous, version) = store.read_maintenance().await.unwrap().unwrap();
        let MaintenanceAcceptance::Accepted(accepted) =
            accept_maintenance(&admission, current.clone(), &actor)
                .await
                .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            store
                .write_maintenance(&previous, Some(&version))
                .await
                .unwrap_err()
                .code,
            "maintenance_cas_mismatch"
        );
        let mut same_sequence = current.clone();
        let parsed = OperationId::parse(&current.operation_id).unwrap();
        same_sequence.operation_id =
            format!("{}:{}:{}", parsed.ledger, parsed.sequence, Ulid::new());
        assert!(matches!(
            accept_maintenance(&admission, same_sequence, &actor)
                .await
                .unwrap(),
            MaintenanceAcceptance::Existing(MaintenanceLookup::IdentityMismatch { .. })
        ));
        complete_maintenance(&admission, accepted, refusal_completion())
            .await
            .unwrap();
        let (text, _) = store.read_maintenance().await.unwrap().unwrap();
        let journal: Journal = serde_json::from_str(&text).unwrap();
        assert_eq!(journal.results.len(), MAX_RESULTS);
        assert!(text.len() < MAX_RESULTS_BYTES);
        admission.release_after_settlement().await.unwrap();
    }

    #[test]
    fn identities_and_failure_records_are_strict_and_bounded() {
        for id in [
            "",
            "x:1:x",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV:01:01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV:0:01ARZ3NDEKTSV4RRFFQ69G5FAV",
        ] {
            assert!(OperationId::parse(id).is_err());
        }
        assert!(
            validate_completion(&MaintenanceCompletion::Refused {
                code: "too_long".repeat(10),
                message: "bounded".into()
            })
            .is_err()
        );
        assert!(
            validate_completion(&MaintenanceCompletion::Refused {
                code: "valid".into(),
                message: "x".repeat(1025)
            })
            .is_err()
        );
    }
}
