//! The operation record, `__cluster/operations.json`.
//!
//! One control object per cluster root, separate from `state.json` (whose
//! `deny_unknown_fields` ledger an older binary must keep reading). Only the
//! process that holds the cluster lock writes it, and every write is a
//! conditional put against the version the writer read: at acceptance, when
//! execution starts, before the publication, and at the end. A write whose
//! answer is lost is in doubt; the caller then starts no table work and no
//! publication until a re-read settles the key.
//!
//! The record is generic over the kind of operation it holds: the id, the
//! acceptance before any effect, the states, cancel, lookup, retention, the
//! eviction mark, the nonce and the walk that settles a publication by history
//! belong to the record. A kind supplies its request, its typed outcomes, its
//! admission scope, its settlement branch and its state per graph; `optimize`
//! is the first kind.
//!
//! The header holds at most one active operation per admission scope, the
//! operations whose publication is in doubt (`unresolved`), a bounded list of
//! terminal operations newest first, the eviction mark, and each kind's state
//! per graph. A terminal record is never rewritten, and an unresolved one is
//! never evicted.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::Diagnostic;
use crate::deployment::{MAX_RESULT_BYTES, MAX_RESULTS, MAX_RESULTS_BYTES, canonical_ulid};

pub const CLUSTER_OPERATIONS_FILE: &str = "__cluster/operations.json";

/// Terminal operations kept, newest first; the oldest is evicted first. The
/// deployment receipts' bound, shared so the two records cannot drift apart.
pub const MAX_TERMINAL_OPERATIONS: usize = MAX_RESULTS;
/// The largest one operation may encode to.
pub const MAX_OPERATION_BYTES: usize = MAX_RESULT_BYTES;
/// The largest the terminal list may encode to.
pub const MAX_TERMINAL_OPERATIONS_BYTES: usize = MAX_RESULTS_BYTES;
/// Operations whose publication is in doubt, across every kind; a fifth
/// refuses admission.
pub const MAX_UNRESOLVED_OPERATIONS: usize = 4;
/// Per-table outcomes a record lists; the rest are counted per outcome code.
pub const MAX_TABLE_OUTCOMES: usize = 1024;
/// The longest detail a per-table or tail outcome keeps.
pub const MAX_OUTCOME_DETAIL_BYTES: usize = 512;
/// How far ahead of the server's clock an operation id's ULID time may be.
/// The eviction mark is the newest evicted id's time, so an id dated far in
/// the future would, once evicted, refuse every honest id after it.
pub const MAX_OPERATION_ID_LEAD_MS: u64 = 60_000;

const OPERATIONS_VERSION: u32 = 1;

/// The scope an operation is admitted in: one active operation per scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionScope {
    /// Shared by optimize, cleanup, repair and the full-text rebuild: one
    /// active operation per cluster.
    Maintenance,
}

impl AdmissionScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Maintenance => "maintenance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Optimize,
}

impl OperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Optimize => "optimize",
        }
    }

    pub fn scope(self) -> AdmissionScope {
        match self {
            Self::Optimize => AdmissionScope::Maintenance,
        }
    }

    /// The branch whose history settles a publication of this kind.
    pub fn settlement_branch(self) -> &'static str {
        match self {
            Self::Optimize => "main",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Accepted,
    Running,
    /// The before-publish write landed: the run may have sent a manifest
    /// request. Only the settlement branch's history settles it
    /// (`Resolution`).
    Publishing,
    Succeeded,
    Failed,
    /// Stopped before any manifest request; published nothing.
    Cancelled,
    /// No live run holds it any more, and it never reached `publishing`, so
    /// no attempt sent a manifest request.
    Interrupted,
    /// Terminal with its publication unknown: the authority that could
    /// settle it is gone (`reason` names the event, `graph_deleted`).
    Unsettled,
}

impl OperationState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted | Self::Unsettled
        )
    }
}

/// The exact request of a kind; the request digest covers it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperationRequest {
    Optimize(OptimizeOptions),
}

impl OperationRequest {
    pub fn kind(&self) -> OperationKind {
        match self {
            Self::Optimize(_) => OperationKind::Optimize,
        }
    }
}

/// What an HTTP caller may ask of an optimize run: the tables and how long
/// writers wait. Every ceiling is a serve setting.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizeOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_gate_hold_ms: Option<u64>,
}

/// One table's outcome, as the engine reported it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableOutcome {
    pub type_key: String,
    /// `optimized`, `unchanged`, `deferred`, `skipped` or `failed`.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The human detail of a deferral or a failure, at most
    /// [`MAX_OUTCOME_DETAIL_BYTES`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub fragments_removed: u64,
    pub fragments_added: u64,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_bytes: Option<u64>,
}

/// An outcome after the graph commit, which never fails the operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TailOutcome {
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// The typed outcomes of a kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperationOutcome {
    Optimize(OptimizeOutcome),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizeOutcome {
    /// The resume position the server supplied for a full-catalog run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_at: Option<String>,
    /// At most [`MAX_TABLE_OUTCOMES`] tables, in the run's visit order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<TableOutcome>,
    /// The tables beyond the listed ones, counted per outcome code.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub more_tables: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration_clear: Option<TailOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<TailOutcome>,
}

impl OptimizeOutcome {
    /// Keep `tables`, listing at most [`MAX_TABLE_OUTCOMES`] and counting the
    /// rest per outcome code, so a large catalog never makes a write fail
    /// validation.
    pub fn set_tables(&mut self, tables: Vec<TableOutcome>) {
        let mut tables = tables;
        self.more_tables.clear();
        if tables.len() > MAX_TABLE_OUTCOMES {
            for table in tables.drain(MAX_TABLE_OUTCOMES..) {
                *self.more_tables.entry(table.outcome).or_default() += 1;
            }
        }
        for table in &mut tables {
            if let Some(detail) = table.detail.take() {
                table.detail = Some(truncate_detail(detail));
            }
        }
        self.tables = tables;
    }
}

/// Where a `publishing` record's resolution walk stands (§2.4): the first
/// commit the next page starts at, and how many commits the walk covered.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionProgress {
    /// The unvisited parent the next attempt starts at; `None` restarts at
    /// the branch tip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub visited: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationTimings {
    /// Acceptance to the start of execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_ms: Option<u64>,
    /// How long the run held the branch's writer gates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_hold_ms: Option<u64>,
    /// The start of execution to the end of the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    /// The caller's canonical ULID.
    pub operation_id: String,
    /// `sha256:` over the canonical request: graph, its schema identity
    /// domain, the settlement branch, the request and the principal. The same
    /// id with another digest is a different request.
    pub request_digest: String,
    pub graph: String,
    /// The graph lifetime the record describes: a graph re-added under the
    /// same id is another value.
    pub schema_identity_domain: String,
    /// The branch whose history settles the publication (§2.6).
    pub settlement_branch: String,
    pub request: OperationRequest,
    pub principal: String,
    pub accepted_at: String,
    pub state: OperationState,
    /// The lineage intent's nonce, written before the publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
    /// The graph head every publish attempt reads, written with the nonce.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_manifest_version: Option<u64>,
    /// The published commit's history id, from the publisher's answer or
    /// from resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_commit: Option<String>,
    /// Present while a `publishing` record's walk is open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<ResolutionProgress>,
    pub outcome: OperationOutcome,
    #[serde(default)]
    pub timings: OperationTimings,
    /// Why a terminal operation ended as it did, or where an unresolved
    /// publication's resolution stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Each kind's state per graph.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindStates {
    /// Graph id to the table key its next full-catalog optimize starts at.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub optimize: BTreeMap<String, OptimizeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizeState {
    pub resume_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRecord {
    pub version: u32,
    /// The cluster ledger the record belongs to. A record naming another
    /// ledger is from an earlier ledger at this root and is never merged.
    pub ledger_id: String,
    /// The active operation of each admission scope.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub active: BTreeMap<AdmissionScope, Operation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved: Vec<Operation>,
    /// Newest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal: Vec<Operation>,
    /// The ULID time of the most recently evicted operation id, in
    /// milliseconds; absent until the first eviction. An id at or before it
    /// is refused as expired, so an evicted id never runs its work again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eviction_mark_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "KindStates::is_empty")]
    pub kind_state: KindStates,
}

impl KindStates {
    fn is_empty(&self) -> bool {
        self.optimize.is_empty()
    }
}

/// Why admission refused a request, before any write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionRefusal {
    /// The same id names a different request.
    DifferentRequest,
    /// Another operation holds the scope's active slot.
    Active {
        scope: AdmissionScope,
        operation_id: String,
    },
    /// Four publications are already in doubt.
    Unresolved,
    /// The id's ULID time is at or before the eviction mark.
    Expired { eviction_mark_ms: u64 },
    /// The id's ULID time is more than [`MAX_OPERATION_ID_LEAD_MS`] ahead of
    /// the server's clock.
    FutureDated { latest_ms: u64 },
}

/// Admission's answer for a request it does not refuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admitted {
    /// The id is new: it now holds its scope's active slot, `accepted`.
    New,
    /// The same request was already accepted; nothing changed.
    Existing(Box<Operation>),
}

/// How the settlement branch's history settles an unresolved publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// The branch's graph head is still the input head, or the walk could not
    /// prove the commit absent. The record stays `publishing`; `reason` says
    /// where the walk stopped.
    InDoubt { reason: String },
    /// The walk read one page and has not reached the input head's child; the
    /// next attempt starts at `cursor`.
    Walking {
        cursor: Option<String>,
        visited: u64,
    },
    /// The input head's child answers the nonce.
    Committed { published_commit: String },
    /// The input head's child does not answer the nonce.
    NotCommitted,
}

/// Whether `id` is a canonical ULID, the only spelling an operation id has.
pub fn canonical_operation_id(id: &str) -> bool {
    canonical_ulid(id)
}

/// A fresh operation id, which a caller mints before it submits.
pub fn new_operation_id() -> String {
    Ulid::new().to_string()
}

/// The ULID time of a canonical operation id, in milliseconds.
pub fn operation_id_time_ms(id: &str) -> Option<u64> {
    id.parse::<Ulid>().ok().map(|ulid| ulid.timestamp_ms())
}

/// Cut `detail` to [`MAX_OUTCOME_DETAIL_BYTES`] on a character boundary.
pub fn truncate_detail(mut detail: String) -> String {
    if detail.len() <= MAX_OUTCOME_DETAIL_BYTES {
        return detail;
    }
    let mut end = MAX_OUTCOME_DETAIL_BYTES;
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    detail.truncate(end);
    detail
}

impl Operation {
    /// A new `accepted` operation, stamped now, with its request digest.
    pub fn accepted(
        operation_id: impl Into<String>,
        graph: impl Into<String>,
        schema_identity_domain: impl Into<String>,
        request: OperationRequest,
        principal: impl Into<String>,
        start_at: Option<String>,
    ) -> Self {
        let graph = graph.into();
        let schema_identity_domain = schema_identity_domain.into();
        let principal = principal.into();
        let settlement_branch = request.kind().settlement_branch().to_string();
        let outcome = match &request {
            OperationRequest::Optimize(_) => OperationOutcome::Optimize(OptimizeOutcome {
                start_at,
                ..OptimizeOutcome::default()
            }),
        };
        Self {
            operation_id: operation_id.into(),
            request_digest: operation_request_digest(
                &graph,
                &schema_identity_domain,
                &settlement_branch,
                &request,
                &principal,
            ),
            graph,
            schema_identity_domain,
            settlement_branch,
            request,
            principal,
            accepted_at: crate::now_rfc3339(),
            state: OperationState::Accepted,
            nonce: None,
            input_commit: None,
            input_manifest_version: None,
            published_commit: None,
            resolution: None,
            outcome,
            timings: OperationTimings::default(),
            reason: None,
        }
    }

    pub fn kind(&self) -> OperationKind {
        self.request.kind()
    }

    pub fn scope(&self) -> AdmissionScope {
        self.kind().scope()
    }

    /// The optimize outcome, for a record of that kind.
    pub fn optimize_outcome_mut(&mut self) -> Option<&mut OptimizeOutcome> {
        match &mut self.outcome {
            OperationOutcome::Optimize(outcome) => Some(outcome),
        }
    }

    pub fn optimize_outcome(&self) -> Option<&OptimizeOutcome> {
        match &self.outcome {
            OperationOutcome::Optimize(outcome) => Some(outcome),
        }
    }

    fn same_lifetime(&self, graph: &str, schema_identity_domain: &str) -> bool {
        self.graph == graph && self.schema_identity_domain == schema_identity_domain
    }
}

impl OperationRecord {
    pub fn new(ledger_id: impl Into<String>) -> Self {
        Self {
            version: OPERATIONS_VERSION,
            ledger_id: ledger_id.into(),
            active: BTreeMap::new(),
            unresolved: Vec::new(),
            terminal: Vec::new(),
            eviction_mark_ms: None,
            kind_state: KindStates::default(),
        }
    }

    /// Every operation the record holds: active, unresolved, then terminal.
    pub fn operations(&self) -> impl Iterator<Item = &Operation> {
        self.active
            .values()
            .chain(&self.unresolved)
            .chain(&self.terminal)
    }

    /// The operation this record holds for `operation_id` and `graph`.
    pub fn find(&self, graph: &str, operation_id: &str) -> Option<&Operation> {
        self.operations()
            .find(|op| op.operation_id == operation_id && op.graph == graph)
    }

    /// The active operation of `scope`, if any.
    pub fn active_in(&self, scope: AdmissionScope) -> Option<&Operation> {
        self.active.get(&scope)
    }

    /// Hold `operation` in its scope's active slot, or answer the existing
    /// record of the same request. The order is the admission contract: a
    /// retained id answers first, whatever the mark or the slot says; then the
    /// id's time is judged against the eviction mark and against `now_ms`,
    /// the server's clock; then the scope's slot and the unresolved cap.
    /// Callers resolve orphans first ([`Self::interrupt_orphans`]) so a
    /// record that landed after an in-doubt acceptance never holds the slot.
    pub fn admit(
        &mut self,
        operation: Operation,
        now_ms: u64,
    ) -> Result<Admitted, AdmissionRefusal> {
        if let Some(existing) = self
            .operations()
            .find(|op| op.operation_id == operation.operation_id)
        {
            return if existing.request_digest == operation.request_digest
                && existing.graph == operation.graph
            {
                Ok(Admitted::Existing(Box::new(existing.clone())))
            } else {
                Err(AdmissionRefusal::DifferentRequest)
            };
        }
        let time = operation_id_time_ms(&operation.operation_id);
        if let (Some(mark), Some(time)) = (self.eviction_mark_ms, time)
            && time <= mark
        {
            return Err(AdmissionRefusal::Expired {
                eviction_mark_ms: mark,
            });
        }
        let latest_ms = now_ms.saturating_add(MAX_OPERATION_ID_LEAD_MS);
        if time.is_some_and(|time| time > latest_ms) {
            return Err(AdmissionRefusal::FutureDated { latest_ms });
        }
        let scope = operation.scope();
        if let Some(active) = self.active.get(&scope) {
            return Err(AdmissionRefusal::Active {
                scope,
                operation_id: active.operation_id.clone(),
            });
        }
        if self.unresolved.len() >= MAX_UNRESOLVED_OPERATIONS {
            return Err(AdmissionRefusal::Unresolved);
        }
        self.active.insert(scope, operation);
        Ok(Admitted::New)
    }

    /// Settle every active operation no live run holds (`live` lists the ids
    /// this process is running): `accepted` or `running` becomes
    /// `interrupted`, since no attempt sent a manifest request, and
    /// `publishing` moves to the unresolved list for the settlement branch's
    /// history to settle. Returns whether anything changed.
    pub fn interrupt_orphans(&mut self, live: &[String]) -> bool {
        let orphaned = self
            .active
            .iter()
            .filter(|(_, op)| !live.contains(&op.operation_id))
            .map(|(scope, _)| *scope)
            .collect::<Vec<_>>();
        for scope in &orphaned {
            let mut operation = self.active.remove(scope).expect("listed above");
            match operation.state {
                OperationState::Publishing => self.unresolved.push(operation),
                _ => {
                    operation.state = OperationState::Interrupted;
                    operation.reason =
                        Some(format!("no live run held it at {}", crate::now_rfc3339()));
                    self.push_terminal(operation);
                }
            }
        }
        !orphaned.is_empty()
    }

    /// The active operation, if `operation_id` holds a slot.
    pub fn active_mut(&mut self, operation_id: &str) -> Option<&mut Operation> {
        self.active
            .values_mut()
            .find(|op| op.operation_id == operation_id)
    }

    /// End an active operation with its terminal record, and keep the state
    /// its kind records per graph. A `publishing` operation whose publication
    /// is in doubt goes to [`Self::defer_publication`] instead.
    pub fn finish(&mut self, operation: Operation) -> Result<(), Diagnostic> {
        if !operation.state.is_terminal() {
            return Err(invalid("a finished operation must be terminal"));
        }
        let scope = operation.scope();
        if self.active.get(&scope).map(|active| &active.operation_id)
            != Some(&operation.operation_id)
        {
            return Err(invalid("only an active operation can finish"));
        }
        self.active.remove(&scope);
        self.keep_kind_state(&operation);
        self.push_terminal(operation);
        Ok(())
    }

    /// Move an active operation, its publication in doubt, to the unresolved
    /// list, freeing its slot. Its work is never run again.
    pub fn defer_publication(
        &mut self,
        operation_id: &str,
        reason: String,
    ) -> Result<(), Diagnostic> {
        let Some(scope) = self
            .active
            .iter()
            .find(|(_, op)| op.operation_id == operation_id)
            .map(|(scope, _)| *scope)
        else {
            return Err(invalid("only an active operation can defer"));
        };
        let mut operation = self.active.remove(&scope).expect("found above");
        operation.state = OperationState::Publishing;
        operation.reason = Some(reason);
        self.unresolved.push(operation);
        Ok(())
    }

    /// Apply the settlement branch's history to one unresolved operation. A
    /// settled one becomes terminal; an operation still in doubt records
    /// where the walk stopped. Returns whether anything changed.
    pub fn settle(&mut self, operation_id: &str, resolution: Resolution) -> bool {
        let Some(index) = self
            .unresolved
            .iter()
            .position(|op| op.operation_id == operation_id)
        else {
            return false;
        };
        match resolution {
            Resolution::InDoubt { reason } => {
                let operation = &mut self.unresolved[index];
                if operation.reason.as_deref() == Some(reason.as_str()) {
                    return false;
                }
                operation.reason = Some(reason);
            }
            Resolution::Walking { cursor, visited } => {
                let operation = &mut self.unresolved[index];
                let progress = ResolutionProgress { cursor, visited };
                let reason = format!(
                    "resolution walking: {visited} commits covered, next page at {}",
                    progress.cursor.as_deref().unwrap_or("the branch tip")
                );
                if operation.resolution.as_ref() == Some(&progress)
                    && operation.reason.as_deref() == Some(reason.as_str())
                {
                    return false;
                }
                operation.resolution = Some(progress);
                operation.reason = Some(reason);
            }
            Resolution::Committed { published_commit } => {
                let mut operation = self.unresolved.remove(index);
                operation.state = OperationState::Succeeded;
                operation.published_commit = Some(published_commit);
                operation.resolution = None;
                operation.reason = None;
                self.keep_kind_state(&operation);
                self.push_terminal(operation);
            }
            Resolution::NotCommitted => {
                let mut operation = self.unresolved.remove(index);
                operation.state = OperationState::Failed;
                operation.resolution = None;
                operation.reason = Some("not_committed".to_string());
                self.push_terminal(operation);
            }
        }
        true
    }

    /// Release the active operations of one graph lifetime that no run holds
    /// any more, as [`Self::interrupt_orphans`] does for every scope: the
    /// graph's deletion drained its runs, and another graph's live run keeps
    /// its slot. Returns whether anything changed.
    pub fn orphan_lifetime(&mut self, graph: &str, schema_identity_domain: &str) -> bool {
        let scopes = self
            .active
            .iter()
            .filter(|(_, op)| op.same_lifetime(graph, schema_identity_domain))
            .map(|(scope, _)| *scope)
            .collect::<Vec<_>>();
        for scope in &scopes {
            let mut operation = self.active.remove(scope).expect("listed above");
            match operation.state {
                OperationState::Publishing => self.unresolved.push(operation),
                _ => {
                    operation.state = OperationState::Interrupted;
                    operation.reason =
                        Some(format!("no live run held it at {}", crate::now_rfc3339()));
                    self.push_terminal(operation);
                }
            }
        }
        !scopes.is_empty()
    }

    /// The non-terminal operations of one graph lifetime: the unresolved ones
    /// and an active `publishing` one a stopped process left.
    pub fn non_terminal_of(&self, graph: &str, schema_identity_domain: &str) -> Vec<Operation> {
        self.operations()
            .filter(|op| !op.state.is_terminal() && op.same_lifetime(graph, schema_identity_domain))
            .cloned()
            .collect()
    }

    /// The graph is gone with all its history, so nothing can ever settle its
    /// records: every non-terminal one becomes `unsettled` with reason
    /// `graph_deleted`, its publication unknown, and the kind state of the
    /// graph is dropped. Returns whether anything changed.
    pub fn settle_graph_deleted(&mut self, graph: &str, schema_identity_domain: &str) -> bool {
        let mut changed = false;
        let gone = |op: &Operation| op.same_lifetime(graph, schema_identity_domain);
        let scopes = self
            .active
            .iter()
            .filter(|(_, op)| gone(op))
            .map(|(scope, _)| *scope)
            .collect::<Vec<_>>();
        for scope in scopes {
            let operation = self.active.remove(&scope).expect("listed above");
            self.push_terminal(unsettled_by_deletion(operation));
            changed = true;
        }
        let mut index = 0;
        while index < self.unresolved.len() {
            if gone(&self.unresolved[index]) {
                let operation = self.unresolved.remove(index);
                self.push_terminal(unsettled_by_deletion(operation));
                changed = true;
            } else {
                index += 1;
            }
        }
        changed |= self.kind_state.optimize.remove(graph).is_some();
        changed
    }

    fn keep_kind_state(&mut self, operation: &Operation) {
        // Only a full-catalog optimize that succeeded moves the position.
        let OperationRequest::Optimize(options) = &operation.request;
        if operation.state != OperationState::Succeeded || options.tables.is_some() {
            return;
        }
        let resume_at = operation
            .optimize_outcome()
            .and_then(|outcome| outcome.resume_at.clone());
        match resume_at {
            Some(resume_at) => {
                self.kind_state
                    .optimize
                    .insert(operation.graph.clone(), OptimizeState { resume_at });
            }
            None => {
                self.kind_state.optimize.remove(&operation.graph);
            }
        }
    }

    /// Newest first; the oldest terminal records go once the count or the
    /// total size passes its bound, and the eviction mark takes the evicted
    /// id's ULID time. Unresolved records are never evicted.
    fn push_terminal(&mut self, operation: Operation) {
        self.terminal.insert(0, operation);
        while self.terminal.len() > MAX_TERMINAL_OPERATIONS
            || (self.terminal.len() > 1 && self.terminal_len() > MAX_TERMINAL_OPERATIONS_BYTES)
        {
            let evicted = self.terminal.pop().expect("more than one");
            if let Some(time) = operation_id_time_ms(&evicted.operation_id) {
                self.eviction_mark_ms =
                    Some(self.eviction_mark_ms.map_or(time, |mark| mark.max(time)));
            }
        }
    }

    fn terminal_len(&self) -> usize {
        serde_json::to_vec(&self.terminal).map_or(usize::MAX, |bytes| bytes.len())
    }

    /// The bounds and identities every read and write checks.
    pub fn validate(&self) -> Result<(), Diagnostic> {
        if self.version != OPERATIONS_VERSION {
            return Err(invalid(format!(
                "unsupported operation record version {}",
                self.version
            )));
        }
        if !canonical_ulid(&self.ledger_id) {
            return Err(invalid(
                "operation record ledger id is not a canonical ULID",
            ));
        }
        if self.unresolved.len() > MAX_UNRESOLVED_OPERATIONS {
            return Err(invalid(
                "operation record holds too many unresolved operations",
            ));
        }
        if self.terminal.len() > MAX_TERMINAL_OPERATIONS {
            return Err(invalid(
                "operation record holds too many terminal operations",
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        for (place, operation) in self
            .active
            .iter()
            .map(|(scope, op)| ("active", Some(*scope), op))
            .chain(self.unresolved.iter().map(|op| ("unresolved", None, op)))
            .chain(self.terminal.iter().map(|op| ("terminal", None, op)))
            .map(|(place, scope, op)| ((place, scope), op))
        {
            if !canonical_ulid(&operation.operation_id) {
                return Err(invalid("operation id is not a canonical ULID"));
            }
            if !ids.insert(operation.operation_id.as_str()) {
                return Err(invalid(format!(
                    "operation id {} appears twice",
                    operation.operation_id
                )));
            }
            if operation.settlement_branch != operation.kind().settlement_branch() {
                return Err(invalid(format!(
                    "operation {} names settlement branch '{}' for kind {}",
                    operation.operation_id,
                    operation.settlement_branch,
                    operation.kind().as_str()
                )));
            }
            let fits = match place {
                ("active", Some(scope)) => {
                    !operation.state.is_terminal() && operation.scope() == scope
                }
                ("unresolved", _) => operation.state == OperationState::Publishing,
                _ => operation.state.is_terminal(),
            };
            if !fits {
                return Err(invalid(format!(
                    "operation {} is {:?} in the {} section",
                    operation.operation_id, operation.state, place.0
                )));
            }
            if let OperationOutcome::Optimize(outcome) = &operation.outcome
                && outcome.tables.len() > MAX_TABLE_OUTCOMES
            {
                return Err(invalid(format!(
                    "operation {} lists more than {MAX_TABLE_OUTCOMES} tables",
                    operation.operation_id
                )));
            }
            if serde_json::to_vec(operation).map_or(usize::MAX, |bytes| bytes.len())
                > MAX_OPERATION_BYTES
            {
                return Err(invalid(format!(
                    "operation {} exceeds {MAX_OPERATION_BYTES} bytes",
                    operation.operation_id
                )));
            }
        }
        if self.terminal_len() > MAX_TERMINAL_OPERATIONS_BYTES {
            return Err(invalid(format!(
                "operation record's terminal list exceeds {MAX_TERMINAL_OPERATIONS_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

fn unsettled_by_deletion(mut operation: Operation) -> Operation {
    operation.state = OperationState::Unsettled;
    operation.resolution = None;
    operation.published_commit = None;
    operation.reason = Some("graph_deleted".to_string());
    operation
}

/// The digest of a request: what makes two submissions of one id the same.
pub fn operation_request_digest(
    graph: &str,
    schema_identity_domain: &str,
    settlement_branch: &str,
    request: &OperationRequest,
    principal: &str,
) -> String {
    #[derive(Serialize)]
    struct Canonical<'a> {
        graph: &'a str,
        schema_identity_domain: &'a str,
        settlement_branch: &'a str,
        request: &'a OperationRequest,
        principal: &'a str,
    }
    let bytes = serde_json::to_vec(&Canonical {
        graph,
        schema_identity_domain,
        settlement_branch,
        request,
        principal,
    })
    .expect("an operation request serializes");
    format!("sha256:{}", crate::sha256_hex(&bytes))
}

/// The answer to one conditional write of the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationWrite {
    /// The write landed; the new version is the next write's precondition.
    Written { version: String },
    /// The precondition failed: another write changed the record.
    Conflict,
    /// Nothing applied: the store, or this process's storage scope, refused
    /// the request before it could land. A scope refuses a key with an
    /// earlier request of unknown outcome until a replacement scope, so this
    /// can hold for the rest of the process's life.
    Refused { error: String },
    /// No definite answer. The key is unsettled until a re-read settles it.
    InDoubt { error: String },
}

pub(crate) fn decode(text: &str) -> Result<OperationRecord, Diagnostic> {
    let record: OperationRecord = serde_json::from_str(text).map_err(|error| {
        Diagnostic::error(
            "operation_record_invalid",
            CLUSTER_OPERATIONS_FILE,
            format!("could not parse the operation record: {error}"),
        )
    })?;
    record.validate()?;
    Ok(record)
}

pub(crate) fn encode(record: &OperationRecord) -> Result<String, Diagnostic> {
    record.validate()?;
    serde_json::to_string(record).map_err(|error| {
        Diagnostic::error(
            "operation_record_invalid",
            CLUSTER_OPERATIONS_FILE,
            format!("could not encode the operation record: {error}"),
        )
    })
}

fn invalid(message: impl Into<String>) -> Diagnostic {
    Diagnostic::error(
        "operation_record_invalid",
        CLUSTER_OPERATIONS_FILE,
        message.into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEDGER: &str = "01JBQZ0000000000000000ABCD";
    const DOMAIN: &str = "domain-a";

    /// An id whose ULID time is `time_ms`, distinct per `n`.
    fn id_at(time_ms: u64, n: u128) -> String {
        Ulid::from_parts(time_ms, n).to_string()
    }

    fn id(n: u8) -> String {
        id_at(1_000_000 + u64::from(n), u128::from(n))
    }

    fn operation(n: u8, graph: &str) -> Operation {
        Operation {
            accepted_at: "2026-10-29T00:00:00Z".to_string(),
            ..Operation::accepted(
                id(n),
                graph,
                DOMAIN,
                OperationRequest::Optimize(OptimizeOptions::default()),
                "act-op",
                None,
            )
        }
    }

    fn finished(mut op: Operation, state: OperationState) -> Operation {
        op.state = state;
        op
    }

    /// A server clock every test id is behind.
    const NOW_MS: u64 = 2_000_000;

    trait AdmitNow {
        fn admit_now(&mut self, operation: Operation) -> Result<Admitted, AdmissionRefusal>;
    }

    impl AdmitNow for OperationRecord {
        fn admit_now(&mut self, operation: Operation) -> Result<Admitted, AdmissionRefusal> {
            self.admit(operation, NOW_MS)
        }
    }

    #[test]
    fn ids_used_by_these_tests_are_canonical() {
        assert!(canonical_ulid(LEDGER), "{LEDGER}");
        assert!(canonical_ulid(&id(1)), "{}", id(1));
        assert_eq!(operation_id_time_ms(&id(1)), Some(1_000_001));
    }

    #[test]
    fn admission_holds_one_active_operation_per_scope_and_answers_a_repeat() {
        let mut record = OperationRecord::new(LEDGER);
        assert_eq!(record.admit_now(operation(1, "g")), Ok(Admitted::New));
        assert_eq!(
            record.admit_now(operation(1, "g")),
            Ok(Admitted::Existing(Box::new(operation(1, "g"))))
        );
        let mut other = operation(1, "g");
        other.request = OperationRequest::Optimize(OptimizeOptions {
            tables: Some(vec!["node:Person".to_string()]),
            max_gate_hold_ms: None,
        });
        other.request_digest =
            operation_request_digest("g", DOMAIN, "main", &other.request, "act-op");
        assert_eq!(
            record.admit_now(other),
            Err(AdmissionRefusal::DifferentRequest)
        );
        assert_eq!(
            record.admit_now(operation(2, "h")),
            Err(AdmissionRefusal::Active {
                scope: AdmissionScope::Maintenance,
                operation_id: id(1)
            })
        );
        assert_eq!(
            record
                .active_in(AdmissionScope::Maintenance)
                .map(|op| op.operation_id.as_str()),
            Some(id(1).as_str())
        );
        record.validate().unwrap();
    }

    #[test]
    fn an_orphan_is_interrupted_unless_it_was_publishing() {
        let mut record = OperationRecord::new(LEDGER);
        record.admit_now(operation(1, "g")).unwrap();
        assert!(
            !record.interrupt_orphans(&[id(1)]),
            "a live run keeps its slot"
        );
        assert!(record.interrupt_orphans(&[]));
        assert_eq!(record.terminal[0].state, OperationState::Interrupted);
        assert!(record.active.is_empty());

        record.admit_now(operation(2, "g")).unwrap();
        record.active_mut(&id(2)).unwrap().state = OperationState::Publishing;
        assert!(record.interrupt_orphans(&[]));
        assert_eq!(record.unresolved[0].operation_id, id(2));
        record.validate().unwrap();
    }

    #[test]
    fn a_fifth_unresolved_publication_refuses_admission() {
        let mut record = OperationRecord::new(LEDGER);
        for n in 1..=MAX_UNRESOLVED_OPERATIONS as u8 {
            record.admit_now(operation(n, "g")).unwrap();
            record
                .defer_publication(&id(n), "head unchanged".to_string())
                .unwrap();
        }
        assert_eq!(
            record.admit_now(operation(9, "g")),
            Err(AdmissionRefusal::Unresolved)
        );
        record.validate().unwrap();
    }

    #[test]
    fn history_settles_an_unresolved_publication_once_and_a_walk_keeps_its_cursor() {
        let mut record = OperationRecord::new(LEDGER);
        record.admit_now(operation(1, "g")).unwrap();
        record
            .active_mut(&id(1))
            .unwrap()
            .optimize_outcome_mut()
            .unwrap()
            .resume_at = Some("node:Zebra".to_string());
        record
            .defer_publication(&id(1), "head unchanged".to_string())
            .unwrap();
        assert!(!record.settle(
            &id(1),
            Resolution::InDoubt {
                reason: "head unchanged".to_string()
            }
        ));
        assert!(record.settle(
            &id(1),
            Resolution::Walking {
                cursor: Some("hb1.0.3.x".to_string()),
                visited: 256
            }
        ));
        assert_eq!(
            record.unresolved[0].resolution,
            Some(ResolutionProgress {
                cursor: Some("hb1.0.3.x".to_string()),
                visited: 256
            })
        );
        assert!(
            !record.settle(
                &id(1),
                Resolution::Walking {
                    cursor: Some("hb1.0.3.x".to_string()),
                    visited: 256
                }
            ),
            "the same page twice changes nothing"
        );
        assert!(record.settle(
            &id(1),
            Resolution::Committed {
                published_commit: "hb1.child".to_string()
            }
        ));
        assert!(record.unresolved.is_empty());
        assert_eq!(record.terminal[0].state, OperationState::Succeeded);
        assert_eq!(record.terminal[0].resolution, None);
        assert_eq!(
            record
                .kind_state
                .optimize
                .get("g")
                .map(|state| state.resume_at.as_str()),
            Some("node:Zebra")
        );
        assert!(
            !record.settle(&id(1), Resolution::NotCommitted),
            "a terminal record is never rewritten"
        );

        record.admit_now(operation(2, "g")).unwrap();
        record
            .defer_publication(&id(2), "head unchanged".to_string())
            .unwrap();
        assert!(record.settle(&id(2), Resolution::NotCommitted));
        assert_eq!(record.terminal[0].state, OperationState::Failed);
        assert_eq!(record.terminal[0].reason.as_deref(), Some("not_committed"));
        record.validate().unwrap();
    }

    #[test]
    fn terminal_records_are_bounded_newest_first_and_eviction_marks_the_time() {
        let mut record = OperationRecord::new(LEDGER);
        assert_eq!(record.eviction_mark_ms, None);
        for n in 1..=(MAX_TERMINAL_OPERATIONS as u8 + 3) {
            record.admit_now(operation(n, "g")).unwrap();
            record
                .finish(finished(operation(n, "g"), OperationState::Succeeded))
                .unwrap();
        }
        assert_eq!(record.terminal.len(), MAX_TERMINAL_OPERATIONS);
        assert_eq!(
            record.terminal[0].operation_id,
            id(MAX_TERMINAL_OPERATIONS as u8 + 3)
        );
        assert!(record.find("g", &id(1)).is_none(), "the oldest is evicted");
        assert_eq!(
            record.eviction_mark_ms,
            Some(1_000_003),
            "the mark is the newest evicted id's ULID time"
        );
        // A late retry of an evicted id, and a fresh id minted at or before
        // the mark, are refused; one after it is admitted.
        assert_eq!(
            record.admit_now(operation(2, "g")),
            Err(AdmissionRefusal::Expired {
                eviction_mark_ms: 1_000_003
            })
        );
        let mut at_mark = operation(9, "g");
        at_mark.operation_id = id_at(1_000_003, 999);
        assert_eq!(
            record.admit_now(at_mark),
            Err(AdmissionRefusal::Expired {
                eviction_mark_ms: 1_000_003
            })
        );
        let mut after = operation(9, "g");
        after.operation_id = id_at(1_000_004, 999);
        assert_eq!(record.admit_now(after), Ok(Admitted::New));
        record.validate().unwrap();
    }

    #[test]
    fn an_id_dated_ahead_of_the_server_clock_is_refused_and_never_sets_the_mark() {
        let mut record = OperationRecord::new(LEDGER);
        let mut ahead = operation(1, "g");
        ahead.operation_id = id_at(NOW_MS + MAX_OPERATION_ID_LEAD_MS + 1, 7);
        assert_eq!(
            record.admit_now(ahead),
            Err(AdmissionRefusal::FutureDated {
                latest_ms: NOW_MS + MAX_OPERATION_ID_LEAD_MS
            })
        );
        let mut within = operation(2, "g");
        within.operation_id = id_at(NOW_MS + MAX_OPERATION_ID_LEAD_MS, 8);
        assert_eq!(record.admit_now(within), Ok(Admitted::New));
        // A retained id answers before the clock is consulted.
        let repeat = record
            .active_in(AdmissionScope::Maintenance)
            .unwrap()
            .clone();
        assert_eq!(
            record.admit(repeat.clone(), 0),
            Ok(Admitted::Existing(Box::new(repeat)))
        );
        assert_eq!(record.eviction_mark_ms, None);
    }

    #[test]
    fn a_table_list_run_never_moves_the_resume_position() {
        let mut record = OperationRecord::new(LEDGER);
        record.kind_state.optimize.insert(
            "g".to_string(),
            OptimizeState {
                resume_at: "node:Kept".to_string(),
            },
        );
        let mut listed = operation(1, "g");
        listed.request = OperationRequest::Optimize(OptimizeOptions {
            tables: Some(vec!["node:Person".to_string()]),
            max_gate_hold_ms: None,
        });
        listed.optimize_outcome_mut().unwrap().resume_at = Some("node:Other".to_string());
        record.admit_now(listed.clone()).unwrap();
        record
            .finish(finished(listed, OperationState::Succeeded))
            .unwrap();
        assert_eq!(
            record
                .kind_state
                .optimize
                .get("g")
                .map(|state| state.resume_at.as_str()),
            Some("node:Kept")
        );
    }

    #[test]
    fn graph_deletion_settles_the_lifetimes_non_terminal_records() {
        let mut record = OperationRecord::new(LEDGER);
        record.admit_now(operation(1, "g")).unwrap();
        record
            .defer_publication(&id(1), "head unchanged".to_string())
            .unwrap();
        record.admit_now(operation(2, "g")).unwrap();
        record.active_mut(&id(2)).unwrap().state = OperationState::Publishing;
        record.kind_state.optimize.insert(
            "g".to_string(),
            OptimizeState {
                resume_at: "node:Kept".to_string(),
            },
        );
        let mut other_lifetime = operation(3, "g");
        other_lifetime.schema_identity_domain = "domain-b".to_string();
        record
            .unresolved
            .push(finished(other_lifetime, OperationState::Publishing));

        assert_eq!(record.non_terminal_of("g", DOMAIN).len(), 2);
        assert!(record.settle_graph_deleted("g", DOMAIN));
        assert!(record.active.is_empty());
        assert_eq!(record.unresolved.len(), 1, "another lifetime is untouched");
        assert_eq!(record.unresolved[0].operation_id, id(3));
        for terminal in &record.terminal {
            assert_eq!(terminal.state, OperationState::Unsettled);
            assert_eq!(terminal.reason.as_deref(), Some("graph_deleted"));
            assert_eq!(terminal.published_commit, None);
        }
        assert!(!record.kind_state.optimize.contains_key("g"));
        assert!(!record.settle_graph_deleted("g", DOMAIN));
        record.validate().unwrap();
    }

    #[test]
    fn orphaning_one_lifetime_leaves_another_graphs_run_in_its_slot() {
        let mut record = OperationRecord::new(LEDGER);
        record.admit_now(operation(1, "h")).unwrap();
        assert!(
            !record.orphan_lifetime("g", DOMAIN),
            "another graph's active run is not this lifetime's"
        );
        assert_eq!(record.active.len(), 1);
        record.active.clear();
        record.admit_now(operation(2, "g")).unwrap();
        record.active_mut(&id(2)).unwrap().state = OperationState::Publishing;
        assert!(record.orphan_lifetime("g", DOMAIN));
        assert_eq!(record.unresolved[0].operation_id, id(2));
        record.validate().unwrap();
    }

    #[test]
    fn table_outcomes_are_listed_up_to_the_cap_and_details_are_cut() {
        let mut outcome = OptimizeOutcome::default();
        let tables = (0..MAX_TABLE_OUTCOMES + 3)
            .map(|n| TableOutcome {
                type_key: format!("node:T{n}"),
                outcome: if n % 2 == 0 { "unchanged" } else { "deferred" }.to_string(),
                reason: None,
                detail: Some("d".repeat(MAX_OUTCOME_DETAIL_BYTES + 100)),
                fragments_removed: 0,
                fragments_added: 0,
                duration_ms: 0,
                source_bytes: None,
            })
            .collect();
        outcome.set_tables(tables);
        assert_eq!(outcome.tables.len(), MAX_TABLE_OUTCOMES);
        assert_eq!(outcome.more_tables.get("unchanged"), Some(&2));
        assert_eq!(outcome.more_tables.get("deferred"), Some(&1));
        assert!(
            outcome
                .tables
                .iter()
                .all(|table| table.detail.as_ref().unwrap().len() == MAX_OUTCOME_DETAIL_BYTES)
        );
        assert_eq!(truncate_detail("é".repeat(300)), "é".repeat(256));
    }

    #[tokio::test]
    async fn a_conditional_write_lands_once_and_refuses_a_stale_version() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::CLUSTER_STATE_DIR)).unwrap();
        let store = crate::store::ClusterStore::for_config_dir(dir.path());
        assert!(store.read_operations().await.unwrap().is_none());

        let mut record = OperationRecord::new(LEDGER);
        let OperationWrite::Written { version: first } =
            store.write_operations(&record, None).await.unwrap()
        else {
            panic!("the first create lands");
        };
        assert_eq!(
            store.write_operations(&record, None).await.unwrap(),
            OperationWrite::Conflict,
            "a create over an existing record conflicts"
        );
        record.admit_now(operation(1, "g")).unwrap();
        let OperationWrite::Written { version: second } =
            store.write_operations(&record, Some(&first)).await.unwrap()
        else {
            panic!("a write at the read version lands");
        };
        assert_ne!(first, second);
        assert_eq!(
            store
                .write_operations(&OperationRecord::new(LEDGER), Some(&first))
                .await
                .unwrap(),
            OperationWrite::Conflict,
            "a stale version conflicts"
        );
        assert_eq!(
            store.read_operations().await.unwrap(),
            Some((record, second))
        );
    }

    #[test]
    fn decoding_refuses_an_operation_in_the_wrong_section() {
        let mut record = OperationRecord::new(LEDGER);
        record.terminal.push(operation(1, "g"));
        let text = serde_json::to_string(&record).unwrap();
        assert!(decode(&text).is_err());
        let valid = OperationRecord::new(LEDGER);
        assert_eq!(decode(&encode(&valid).unwrap()).unwrap(), valid);
    }
}
