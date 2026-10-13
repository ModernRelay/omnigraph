//! What one read run did, per plan node: one row for the one operator of
//! every live node of the plan, read from that operator's own metrics after
//! the tree ran. A rerun appends an [`Attempt`] to every row.

use omnigraph_planner::{BoundPlan, Evidence, Explain, NodeId};
use serde::{Deserialize, Serialize};

use super::explain::ExplainRows;
use super::operators::Switch;
use crate::error::{OmniError, Result};

/// The `tree` value of a profile row: the report written back onto the
/// plan's nodes as actuals, in the explain row schema.
pub(crate) const PROFILE_TREE: &str = "profile";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RowStatus {
    /// A stream of the operator was polled.
    Executed,
    /// The operator is in the tree and no stream of it was polled.
    Skipped,
}

/// What one pass of an operator did: whether a stream of it was polled, or
/// on a node with a declared switch, which side ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Ran {
    Polled(bool),
    Took(Switch),
}

impl Ran {
    /// Whether the operator was polled at all.
    pub(crate) fn polled(self) -> bool {
        self != Self::Polled(false)
    }
}

/// One pass of the tree over a node's operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Attempt {
    /// 0 for the first pass, then one per rerun of the overfetch ladder.
    pub(crate) rung: usize,
    /// Whether the operator polled, or on a node with a declared switch,
    /// which side ran (`hash_join` / `id_lookup`, `csr` / `indexed_scan`).
    pub(crate) ran: Ran,
    /// The rows the operator's consumer received: complete when `drained` is
    /// `Some(true)`, a lower bound otherwise.
    pub(crate) actual_rows: u64,
    /// Whether the consumer pulled the operator's stream to its end; `None`
    /// on a DataFusion operator, whose end no counter observes.
    pub(crate) drained: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReportRow {
    pub(crate) id: NodeId,
    pub(crate) operator: String,
    pub(crate) status: RowStatus,
    pub(crate) attempts: Vec<Attempt>,
}

impl ReportRow {
    pub(super) fn new(
        id: NodeId,
        operator: &str,
        rung: usize,
        ran: Ran,
        actual_rows: u64,
        drained: Option<bool>,
    ) -> Self {
        Self {
            id,
            operator: operator.to_string(),
            status: if ran.polled() {
                RowStatus::Executed
            } else {
                RowStatus::Skipped
            },
            attempts: vec![Attempt {
                rung,
                ran,
                actual_rows,
                drained,
            }],
        }
    }
}

/// One adaptive search decision the run took inside the policy its plan
/// declares: a pre-pass gate's verdict with the facts it read, or the probe
/// attempts of a `nearest` scan, in the overfetch pass `rung`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SearchDecision {
    /// The ranked scan or fusion the decision belongs to.
    pub(crate) id: NodeId,
    pub(crate) rung: usize,
    #[serde(flatten)]
    pub(crate) taken: Taken,
}

/// What a [`SearchDecision`] took.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub(crate) enum Taken {
    /// The gate's plan (`prefilter`, `postfilter` or `proven_empty`), the
    /// fallback that decided a postfilter, and the counts it read.
    Gate {
        plan: &'static str,
        fallback: Option<&'static str>,
        forced: bool,
        eligible: Option<u64>,
        corpus: Option<u64>,
    },
    /// Every attempt of the scan's probe ladder, in order.
    Probes {
        attempts: Vec<super::search::ProbeAttempt>,
    },
}

impl Taken {
    /// The report form of a gate verdict under the plan the gate chose.
    pub(super) fn gate(
        plan: &'static str,
        verdict: &crate::instrumentation::RrfGateVerdict,
    ) -> Self {
        use crate::instrumentation::RrfGateFallback;
        Self::Gate {
            plan,
            fallback: verdict.fallback.map(|fallback| match fallback {
                RrfGateFallback::Threshold => "threshold",
                RrfGateFallback::Shape => "shape",
                RrfGateFallback::Coverage => "coverage",
                RrfGateFallback::BuildErr => "build_error",
                RrfGateFallback::EmptyEligible => "empty_eligible",
                RrfGateFallback::Forced => "forced",
            }),
            forced: verdict.forced,
            eligible: verdict.eligible,
            corpus: verdict.corpus,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionReport {
    rows: Vec<ReportRow>,
    /// The adaptive search decisions of the run, in the order taken.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    search: Vec<SearchDecision>,
}

impl ExecutionReport {
    #[cfg(test)]
    pub(crate) fn rows(&self) -> &[ReportRow] {
        &self.rows
    }

    #[cfg(test)]
    pub(crate) fn row(&self, id: NodeId) -> Option<&ReportRow> {
        self.rows.iter().find(|row| row.id == id)
    }

    /// Record one adaptive search decision.
    pub(super) fn decide(&mut self, id: NodeId, rung: usize, taken: Taken) {
        self.search.push(SearchDecision { id, rung, taken });
    }

    /// Fold one pass in: a node met before gains the pass's attempt, and is
    /// `executed` once any pass polled its operator.
    pub(super) fn record(&mut self, pass: Vec<ReportRow>) {
        for row in pass {
            match self.rows.iter_mut().find(|met| met.id == row.id) {
                None => self.rows.push(row),
                Some(met) => {
                    if row.status == RowStatus::Executed {
                        met.status = RowStatus::Executed;
                    }
                    met.attempts.extend(row.attempts);
                }
            }
        }
    }
}

/// What executing a bound plan alone produced: its rows, the plan, and what
/// each node of the plan did. Serialized, `report` is `{"rows": [{id,
/// operator, status, attempts}]}`.
pub struct PlanRun {
    pub result: omnigraph_compiler::result::QueryResult,
    pub plan: BoundPlan,
    /// What acceptance checked of `plan`.
    pub evidence: Evidence,
    pub report: ExecutionReport,
}

/// One finished read run: a [`PlanRun`] with the explain document rendered
/// from that same plan.
pub struct Executed {
    pub result: omnigraph_compiler::result::QueryResult,
    pub plan: BoundPlan,
    /// What acceptance checked of `plan`.
    pub evidence: Evidence,
    /// The digest of the schema `plan` was accepted under.
    pub catalog: Option<String>,
    pub explain: Explain,
    pub report: ExecutionReport,
}

impl Executed {
    /// The replay envelope of this run: `plan` with the source and name of
    /// the query it ran, its scope and its schema, serialized.
    pub fn replay_envelope(&self, source: &str, name: &str) -> Vec<u8> {
        omnigraph_planner::ReplayEnvelope::new(
            source,
            name,
            self.plan.clone(),
            &self.evidence,
            self.catalog.clone(),
        )
        .to_bytes()
    }

    /// The report as actuals rows in the explain row schema: `tree`
    /// `profile`, no `depth`, `node` the plan node's kind, `detail` the row's
    /// own fields (`id`, `operator`, `status`, `attempts`), then one row per
    /// search decision (`node` `gate` or `probes`). The third public surface
    /// beside the rows and explain.
    pub fn profile(&self) -> Result<omnigraph_compiler::result::QueryResult> {
        let mut rows = ExplainRows::default();
        for row in &self.report.rows {
            let node = self
                .plan
                .plan
                .node(row.id)
                .map_or("tombstone", |node| node.name());
            let detail = serde_json::to_value(row).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "profile row {} cannot serialize: {error}",
                    row.id
                ))
            })?;
            rows.push(PROFILE_TREE, None, node, detail.to_string());
        }
        for decision in &self.report.search {
            let detail = serde_json::to_value(decision).map_err(|error| {
                OmniError::manifest_internal(format!(
                    "search decision of node {} cannot serialize: {error}",
                    decision.id
                ))
            })?;
            let node = match decision.taken {
                Taken::Gate { .. } => "gate",
                Taken::Probes { .. } => "probes",
            };
            rows.push(PROFILE_TREE, None, node, detail.to_string());
        }
        rows.into_result()
    }
}

#[cfg(test)]
pub(super) mod tests;
