//! The maintenance operation record, `__cluster/maintenance.json`.
//!
//! One control object per cluster root, separate from `state.json` (whose
//! `deny_unknown_fields` ledger an older binary must keep reading). Only the
//! process that holds the cluster lock writes it, and every write is a
//! conditional put against the version the writer read: at acceptance, when
//! execution starts, before the publication, and at the end. A write whose
//! answer is lost is in doubt; the caller then starts no table work and no
//! publication until a re-read settles the key.
//!
//! The ledger holds at most one active operation, the operations whose
//! publication is in doubt (`unresolved`), a bounded list of terminal
//! operations newest first, and each graph's resume position. A terminal
//! record is never rewritten, and an unresolved one is never evicted.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::Diagnostic;
use crate::deployment::canonical_ulid;

pub const CLUSTER_MAINTENANCE_FILE: &str = "__cluster/maintenance.json";

/// Terminal operations kept, newest first; the oldest is evicted first.
pub const MAX_MAINTENANCE_TERMINAL: usize = 32;
/// Operations whose publication is in doubt; a fifth refuses admission.
pub const MAX_MAINTENANCE_UNRESOLVED: usize = 4;
pub const MAX_MAINTENANCE_RECORD_BYTES: usize = 1024 * 1024;
pub const MAX_MAINTENANCE_BYTES: usize = 4 * 1024 * 1024;

const MAINTENANCE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceLedger {
    pub version: u32,
    /// The cluster ledger the record belongs to. A record naming another
    /// ledger is from an earlier ledger at this root and is never merged.
    pub ledger_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<MaintenanceOperation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved: Vec<MaintenanceOperation>,
    /// Newest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal: Vec<MaintenanceOperation>,
    /// Graph id to the table key its next full-catalog run starts at.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub resume_at: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceKind {
    Optimize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceState {
    Accepted,
    Running,
    /// The before-publish write landed: the run may have sent a manifest
    /// request. Only main's history settles it (`Resolution`).
    Publishing,
    Succeeded,
    Failed,
    /// Stopped before any manifest request; published nothing.
    Cancelled,
    /// No live run holds it any more, and it never reached `publishing`, so
    /// no attempt sent a manifest request.
    Interrupted,
}

impl MaintenanceState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

/// The options the caller asked for, exactly; the request digest covers them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizeOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_gate_hold_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_source_fragments: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_source_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_source_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_blob_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_index_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_source_bytes: Option<u64>,
}

/// One table's outcome, as the engine reported it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceTableOutcome {
    pub type_key: String,
    /// `optimized`, `unchanged`, `deferred`, `skipped` or `failed`.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The human detail of a deferral or a failure.
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
pub struct MaintenanceTailOutcome {
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceTimings {
    /// Acceptance to the start of execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_ms: Option<u64>,
    /// How long the run held main's writer gates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_hold_ms: Option<u64>,
    /// The start of execution to the end of the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceOperation {
    /// The caller's canonical ULID.
    pub maintenance_id: String,
    /// `sha256:` over the canonical request: graph, branch, kind, options and
    /// principal. The same id with another digest is a different request.
    pub request_digest: String,
    pub graph: String,
    pub branch: String,
    pub kind: MaintenanceKind,
    pub options: OptimizeOptions,
    pub principal: String,
    pub accepted_at: String,
    pub state: MaintenanceState,
    /// The resume position the server supplied for a full-catalog run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_at: Option<String>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<MaintenanceTableOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration_clear: Option<MaintenanceTailOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<MaintenanceTailOutcome>,
    #[serde(default)]
    pub timings: MaintenanceTimings,
    /// Why a terminal operation ended as it did, or where an unresolved
    /// publication's resolution stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Why admission refused a request, before any write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceRefusal {
    /// The same id names a different request.
    DifferentRequest,
    /// Another operation holds the cluster's active slot.
    Active { maintenance_id: String },
    /// Four publications are already in doubt.
    Unresolved,
}

/// Admission's answer for a request it does not refuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admitted {
    /// The id is new: it now holds the active slot, `accepted`.
    New,
    /// The same request was already accepted; nothing changed.
    Existing(Box<MaintenanceOperation>),
}

/// How main's history settles an unresolved publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// main's graph head is still the input head, or the walk could not
    /// prove the commit absent. The record stays `publishing`; `reason` says
    /// where the walk stopped.
    InDoubt { reason: String },
    /// The input head's child answers the nonce.
    Committed { published_commit: String },
    /// The input head's child does not answer the nonce.
    NotCommitted,
}

/// Whether `id` is a canonical ULID, the only spelling a maintenance id has.
pub fn canonical_maintenance_id(id: &str) -> bool {
    canonical_ulid(id)
}

impl MaintenanceOperation {
    /// A new `accepted` operation, stamped now, with its request digest.
    pub fn accepted(
        maintenance_id: impl Into<String>,
        graph: impl Into<String>,
        options: OptimizeOptions,
        principal: impl Into<String>,
        start_at: Option<String>,
    ) -> Self {
        let graph = graph.into();
        let principal = principal.into();
        let branch = "main".to_string();
        Self {
            maintenance_id: maintenance_id.into(),
            request_digest: maintenance_request_digest(
                &graph,
                &branch,
                MaintenanceKind::Optimize,
                &options,
                &principal,
            ),
            graph,
            branch,
            kind: MaintenanceKind::Optimize,
            options,
            principal,
            accepted_at: crate::now_rfc3339(),
            state: MaintenanceState::Accepted,
            start_at,
            nonce: None,
            input_commit: None,
            input_manifest_version: None,
            published_commit: None,
            tables: Vec::new(),
            resume_at: None,
            configuration_clear: None,
            artifact: None,
            timings: MaintenanceTimings::default(),
            reason: None,
        }
    }
}

impl MaintenanceLedger {
    pub fn new(ledger_id: impl Into<String>) -> Self {
        Self {
            version: MAINTENANCE_VERSION,
            ledger_id: ledger_id.into(),
            active: None,
            unresolved: Vec::new(),
            terminal: Vec::new(),
            resume_at: BTreeMap::new(),
        }
    }

    /// The operation this record holds for `maintenance_id` and `graph`.
    pub fn find(&self, graph: &str, maintenance_id: &str) -> Option<&MaintenanceOperation> {
        self.active
            .iter()
            .chain(&self.unresolved)
            .chain(&self.terminal)
            .find(|op| op.maintenance_id == maintenance_id && op.graph == graph)
    }

    /// Hold `operation` in the active slot, or answer the existing record of
    /// the same request. Callers resolve orphans first
    /// ([`Self::interrupt_orphans`]) so a record that landed after an in-doubt
    /// acceptance never holds the slot.
    pub fn admit(
        &mut self,
        operation: MaintenanceOperation,
    ) -> Result<Admitted, MaintenanceRefusal> {
        let same_id = self
            .active
            .iter()
            .chain(&self.unresolved)
            .chain(&self.terminal)
            .find(|op| op.maintenance_id == operation.maintenance_id);
        if let Some(existing) = same_id {
            return if existing.request_digest == operation.request_digest
                && existing.graph == operation.graph
            {
                Ok(Admitted::Existing(Box::new(existing.clone())))
            } else {
                Err(MaintenanceRefusal::DifferentRequest)
            };
        }
        if let Some(active) = &self.active {
            return Err(MaintenanceRefusal::Active {
                maintenance_id: active.maintenance_id.clone(),
            });
        }
        if self.unresolved.len() >= MAX_MAINTENANCE_UNRESOLVED {
            return Err(MaintenanceRefusal::Unresolved);
        }
        self.active = Some(operation);
        Ok(Admitted::New)
    }

    /// Settle the active operation when no live run holds it (`live` is the
    /// id this process is running, if any): `accepted` or `running` becomes
    /// `interrupted`, since no attempt sent a manifest request, and
    /// `publishing` moves to the unresolved list for main's history to
    /// settle. Returns whether anything changed.
    pub fn interrupt_orphans(&mut self, live: Option<&str>) -> bool {
        let Some(active) = &self.active else {
            return false;
        };
        if live == Some(active.maintenance_id.as_str()) {
            return false;
        }
        let mut operation = self.active.take().expect("checked above");
        match operation.state {
            MaintenanceState::Publishing => self.unresolved.push(operation),
            _ => {
                operation.state = MaintenanceState::Interrupted;
                operation.reason = Some(format!("no live run held it at {}", crate::now_rfc3339()));
                self.push_terminal(operation);
            }
        }
        true
    }

    /// The active operation, if `maintenance_id` holds the slot.
    pub fn active_mut(&mut self, maintenance_id: &str) -> Option<&mut MaintenanceOperation> {
        self.active
            .as_mut()
            .filter(|op| op.maintenance_id == maintenance_id)
    }

    /// End the active operation with its terminal record, and keep the
    /// resume position a full-catalog run reports. A `publishing` operation
    /// whose publication is in doubt goes to [`Self::defer_publication`]
    /// instead.
    pub fn finish(&mut self, operation: MaintenanceOperation) -> Result<(), Diagnostic> {
        if !operation.state.is_terminal() {
            return Err(invalid("a finished maintenance operation must be terminal"));
        }
        if self.active.as_ref().map(|active| &active.maintenance_id)
            != Some(&operation.maintenance_id)
        {
            return Err(invalid("only the active maintenance operation can finish"));
        }
        self.active = None;
        self.keep_resume_position(&operation);
        self.push_terminal(operation);
        Ok(())
    }

    /// Move the active operation, its publication in doubt, to the
    /// unresolved list, freeing the slot. Its work is never run again.
    pub fn defer_publication(
        &mut self,
        maintenance_id: &str,
        reason: String,
    ) -> Result<(), Diagnostic> {
        let Some(mut operation) = self
            .active
            .take_if(|op| op.maintenance_id == maintenance_id)
        else {
            return Err(invalid("only the active maintenance operation can defer"));
        };
        operation.state = MaintenanceState::Publishing;
        operation.reason = Some(reason);
        self.unresolved.push(operation);
        Ok(())
    }

    /// Apply main's history to one unresolved operation. A settled one
    /// becomes terminal; an operation still in doubt records where the walk
    /// stopped. Returns whether anything changed.
    pub fn settle(&mut self, maintenance_id: &str, resolution: Resolution) -> bool {
        let Some(index) = self
            .unresolved
            .iter()
            .position(|op| op.maintenance_id == maintenance_id)
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
            Resolution::Committed { published_commit } => {
                let mut operation = self.unresolved.remove(index);
                operation.state = MaintenanceState::Succeeded;
                operation.published_commit = Some(published_commit);
                operation.reason = None;
                self.keep_resume_position(&operation);
                self.push_terminal(operation);
            }
            Resolution::NotCommitted => {
                let mut operation = self.unresolved.remove(index);
                operation.state = MaintenanceState::Failed;
                operation.reason = Some("not_committed".to_string());
                self.push_terminal(operation);
            }
        }
        true
    }

    fn keep_resume_position(&mut self, operation: &MaintenanceOperation) {
        // Only a full-catalog run that succeeded moves the position.
        if operation.state != MaintenanceState::Succeeded || operation.options.tables.is_some() {
            return;
        }
        match &operation.resume_at {
            Some(key) => {
                self.resume_at.insert(operation.graph.clone(), key.clone());
            }
            None => {
                self.resume_at.remove(&operation.graph);
            }
        }
    }

    /// Newest first; the oldest terminal records go once the count or the
    /// total size passes its bound. Unresolved records are never evicted.
    fn push_terminal(&mut self, operation: MaintenanceOperation) {
        self.terminal.insert(0, operation);
        self.terminal.truncate(MAX_MAINTENANCE_TERMINAL);
        while self.terminal.len() > 1 && self.encoded_len() > MAX_MAINTENANCE_BYTES {
            self.terminal.pop();
        }
    }

    fn encoded_len(&self) -> usize {
        serde_json::to_vec(self).map_or(usize::MAX, |bytes| bytes.len())
    }

    /// The bounds and identities every read and write checks.
    pub fn validate(&self) -> Result<(), Diagnostic> {
        if self.version != MAINTENANCE_VERSION {
            return Err(invalid(format!(
                "unsupported maintenance record version {}",
                self.version
            )));
        }
        if !canonical_ulid(&self.ledger_id) {
            return Err(invalid(
                "maintenance record ledger id is not a canonical ULID",
            ));
        }
        if self.unresolved.len() > MAX_MAINTENANCE_UNRESOLVED {
            return Err(invalid(
                "maintenance record holds too many unresolved operations",
            ));
        }
        if self.terminal.len() > MAX_MAINTENANCE_TERMINAL {
            return Err(invalid(
                "maintenance record holds too many terminal operations",
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        for (place, operation) in self
            .active
            .iter()
            .map(|op| ("active", op))
            .chain(self.unresolved.iter().map(|op| ("unresolved", op)))
            .chain(self.terminal.iter().map(|op| ("terminal", op)))
        {
            if !canonical_ulid(&operation.maintenance_id) {
                return Err(invalid("maintenance id is not a canonical ULID"));
            }
            if !ids.insert(operation.maintenance_id.as_str()) {
                return Err(invalid(format!(
                    "maintenance id {} appears twice",
                    operation.maintenance_id
                )));
            }
            let fits = match place {
                "active" => !operation.state.is_terminal(),
                "unresolved" => operation.state == MaintenanceState::Publishing,
                _ => operation.state.is_terminal(),
            };
            if !fits {
                return Err(invalid(format!(
                    "maintenance operation {} is {:?} in the {place} section",
                    operation.maintenance_id, operation.state
                )));
            }
            if serde_json::to_vec(operation).map_or(usize::MAX, |bytes| bytes.len())
                > MAX_MAINTENANCE_RECORD_BYTES
            {
                return Err(invalid(format!(
                    "maintenance operation {} exceeds {MAX_MAINTENANCE_RECORD_BYTES} bytes",
                    operation.maintenance_id
                )));
            }
        }
        if self.encoded_len() > MAX_MAINTENANCE_BYTES {
            return Err(invalid(format!(
                "maintenance record exceeds {MAX_MAINTENANCE_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

/// The digest of a request: what makes two submissions of one id the same.
pub fn maintenance_request_digest(
    graph: &str,
    branch: &str,
    kind: MaintenanceKind,
    options: &OptimizeOptions,
    principal: &str,
) -> String {
    #[derive(Serialize)]
    struct Canonical<'a> {
        graph: &'a str,
        branch: &'a str,
        kind: MaintenanceKind,
        options: &'a OptimizeOptions,
        principal: &'a str,
    }
    let bytes = serde_json::to_vec(&Canonical {
        graph,
        branch,
        kind,
        options,
        principal,
    })
    .expect("a maintenance request serializes");
    format!("sha256:{}", crate::sha256_hex(&bytes))
}

/// The answer to one conditional write of the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceWrite {
    /// The write landed; the new version is the next write's precondition.
    Written { version: String },
    /// The precondition failed: another write changed the record.
    Conflict,
    /// No definite answer. The key is unsettled until a re-read settles it.
    InDoubt { error: String },
}

pub(crate) fn decode(text: &str) -> Result<MaintenanceLedger, Diagnostic> {
    let ledger: MaintenanceLedger = serde_json::from_str(text).map_err(|error| {
        Diagnostic::error(
            "maintenance_record_invalid",
            CLUSTER_MAINTENANCE_FILE,
            format!("could not parse the maintenance record: {error}"),
        )
    })?;
    ledger.validate()?;
    Ok(ledger)
}

pub(crate) fn encode(ledger: &MaintenanceLedger) -> Result<String, Diagnostic> {
    ledger.validate()?;
    serde_json::to_string(ledger).map_err(|error| {
        Diagnostic::error(
            "maintenance_record_invalid",
            CLUSTER_MAINTENANCE_FILE,
            format!("could not encode the maintenance record: {error}"),
        )
    })
}

fn invalid(message: impl Into<String>) -> Diagnostic {
    Diagnostic::error(
        "maintenance_record_invalid",
        CLUSTER_MAINTENANCE_FILE,
        message.into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEDGER: &str = "01JBQZ0000000000000000ABCD";

    fn id(n: u8) -> String {
        format!("01JBQZ00000000000000000{:03}", n)
    }

    fn operation(n: u8, graph: &str) -> MaintenanceOperation {
        let options = OptimizeOptions::default();
        MaintenanceOperation {
            maintenance_id: id(n),
            request_digest: maintenance_request_digest(
                graph,
                "main",
                MaintenanceKind::Optimize,
                &options,
                "act-op",
            ),
            graph: graph.to_string(),
            branch: "main".to_string(),
            kind: MaintenanceKind::Optimize,
            options,
            principal: "act-op".to_string(),
            accepted_at: "2026-10-29T00:00:00Z".to_string(),
            state: MaintenanceState::Accepted,
            start_at: None,
            nonce: None,
            input_commit: None,
            input_manifest_version: None,
            published_commit: None,
            tables: Vec::new(),
            resume_at: None,
            configuration_clear: None,
            artifact: None,
            timings: MaintenanceTimings::default(),
            reason: None,
        }
    }

    fn finished(mut op: MaintenanceOperation, state: MaintenanceState) -> MaintenanceOperation {
        op.state = state;
        op
    }

    #[test]
    fn ids_used_by_these_tests_are_canonical() {
        assert!(canonical_ulid(LEDGER), "{LEDGER}");
        assert!(canonical_ulid(&id(1)), "{}", id(1));
    }

    #[test]
    fn admission_holds_one_active_operation_and_answers_a_repeat() {
        let mut ledger = MaintenanceLedger::new(LEDGER);
        assert_eq!(ledger.admit(operation(1, "g")), Ok(Admitted::New));
        assert_eq!(
            ledger.admit(operation(1, "g")),
            Ok(Admitted::Existing(Box::new(operation(1, "g"))))
        );
        let mut other = operation(1, "g");
        other.options.tables = Some(vec!["node:Person".to_string()]);
        other.request_digest = maintenance_request_digest(
            "g",
            "main",
            MaintenanceKind::Optimize,
            &other.options,
            "act-op",
        );
        assert_eq!(
            ledger.admit(other),
            Err(MaintenanceRefusal::DifferentRequest)
        );
        assert_eq!(
            ledger.admit(operation(2, "h")),
            Err(MaintenanceRefusal::Active {
                maintenance_id: id(1)
            })
        );
        ledger.validate().unwrap();
    }

    #[test]
    fn an_orphan_is_interrupted_unless_it_was_publishing() {
        let mut ledger = MaintenanceLedger::new(LEDGER);
        ledger.admit(operation(1, "g")).unwrap();
        assert!(
            !ledger.interrupt_orphans(Some(&id(1))),
            "a live run keeps its slot"
        );
        assert!(ledger.interrupt_orphans(None));
        assert_eq!(ledger.terminal[0].state, MaintenanceState::Interrupted);
        assert!(ledger.active.is_none());

        ledger.admit(operation(2, "g")).unwrap();
        ledger.active_mut(&id(2)).unwrap().state = MaintenanceState::Publishing;
        assert!(ledger.interrupt_orphans(None));
        assert_eq!(ledger.unresolved[0].maintenance_id, id(2));
        ledger.validate().unwrap();
    }

    #[test]
    fn a_fifth_unresolved_publication_refuses_admission() {
        let mut ledger = MaintenanceLedger::new(LEDGER);
        for n in 1..=MAX_MAINTENANCE_UNRESOLVED as u8 {
            ledger.admit(operation(n, "g")).unwrap();
            ledger
                .defer_publication(&id(n), "head unchanged".to_string())
                .unwrap();
        }
        assert_eq!(
            ledger.admit(operation(9, "g")),
            Err(MaintenanceRefusal::Unresolved)
        );
        ledger.validate().unwrap();
    }

    #[test]
    fn history_settles_an_unresolved_publication_once() {
        let mut ledger = MaintenanceLedger::new(LEDGER);
        ledger.admit(operation(1, "g")).unwrap();
        ledger.active_mut(&id(1)).unwrap().resume_at = Some("node:Zebra".to_string());
        ledger
            .defer_publication(&id(1), "head unchanged".to_string())
            .unwrap();
        assert!(!ledger.settle(
            &id(1),
            Resolution::InDoubt {
                reason: "head unchanged".to_string()
            }
        ));
        assert!(ledger.settle(
            &id(1),
            Resolution::Committed {
                published_commit: "hb1.child".to_string()
            }
        ));
        assert!(ledger.unresolved.is_empty());
        assert_eq!(ledger.terminal[0].state, MaintenanceState::Succeeded);
        assert_eq!(
            ledger.resume_at.get("g").map(String::as_str),
            Some("node:Zebra")
        );
        assert!(
            !ledger.settle(&id(1), Resolution::NotCommitted),
            "a terminal record is never rewritten"
        );

        ledger.admit(operation(2, "g")).unwrap();
        ledger
            .defer_publication(&id(2), "head unchanged".to_string())
            .unwrap();
        assert!(ledger.settle(&id(2), Resolution::NotCommitted));
        assert_eq!(ledger.terminal[0].state, MaintenanceState::Failed);
        assert_eq!(ledger.terminal[0].reason.as_deref(), Some("not_committed"));
        ledger.validate().unwrap();
    }

    #[test]
    fn terminal_records_are_bounded_newest_first() {
        let mut ledger = MaintenanceLedger::new(LEDGER);
        for n in 1..=(MAX_MAINTENANCE_TERMINAL as u8 + 3) {
            ledger.admit(operation(n, "g")).unwrap();
            ledger
                .finish(finished(operation(n, "g"), MaintenanceState::Succeeded))
                .unwrap();
        }
        assert_eq!(ledger.terminal.len(), MAX_MAINTENANCE_TERMINAL);
        assert_eq!(
            ledger.terminal[0].maintenance_id,
            id(MAX_MAINTENANCE_TERMINAL as u8 + 3)
        );
        assert!(ledger.find("g", &id(1)).is_none(), "the oldest is evicted");
        ledger.validate().unwrap();
    }

    #[test]
    fn a_table_list_run_never_moves_the_resume_position() {
        let mut ledger = MaintenanceLedger::new(LEDGER);
        ledger
            .resume_at
            .insert("g".to_string(), "node:Kept".to_string());
        let mut listed = operation(1, "g");
        listed.options.tables = Some(vec!["node:Person".to_string()]);
        listed.resume_at = Some("node:Other".to_string());
        ledger.admit(listed.clone()).unwrap();
        ledger
            .finish(finished(listed, MaintenanceState::Succeeded))
            .unwrap();
        assert_eq!(
            ledger.resume_at.get("g").map(String::as_str),
            Some("node:Kept")
        );
    }

    #[tokio::test]
    async fn a_conditional_write_lands_once_and_refuses_a_stale_version() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::CLUSTER_STATE_DIR)).unwrap();
        let store = crate::store::ClusterStore::for_config_dir(dir.path());
        assert!(store.read_maintenance().await.unwrap().is_none());

        let mut ledger = MaintenanceLedger::new(LEDGER);
        let MaintenanceWrite::Written { version: first } =
            store.write_maintenance(&ledger, None).await.unwrap()
        else {
            panic!("the first create lands");
        };
        assert_eq!(
            store.write_maintenance(&ledger, None).await.unwrap(),
            MaintenanceWrite::Conflict,
            "a create over an existing record conflicts"
        );
        ledger.admit(operation(1, "g")).unwrap();
        let MaintenanceWrite::Written { version: second } = store
            .write_maintenance(&ledger, Some(&first))
            .await
            .unwrap()
        else {
            panic!("a write at the read version lands");
        };
        assert_ne!(first, second);
        assert_eq!(
            store
                .write_maintenance(&MaintenanceLedger::new(LEDGER), Some(&first))
                .await
                .unwrap(),
            MaintenanceWrite::Conflict,
            "a stale version conflicts"
        );
        assert_eq!(
            store.read_maintenance().await.unwrap(),
            Some((ledger, second))
        );
    }

    #[test]
    fn decoding_refuses_an_operation_in_the_wrong_section() {
        let mut ledger = MaintenanceLedger::new(LEDGER);
        ledger.terminal.push(operation(1, "g"));
        let text = serde_json::to_string(&ledger).unwrap();
        assert!(decode(&text).is_err());
        let valid = MaintenanceLedger::new(LEDGER);
        assert_eq!(decode(&encode(&valid).unwrap()).unwrap(), valid);
    }
}
