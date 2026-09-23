//! What one read run did, per plan node: one row for the one operator of
//! every live node of the plan, read from that operator's own metrics after
//! the tree ran. A rerun appends an [`Attempt`] to every row.

use omnigraph_planner::{BoundPlan, Explain, NodeId};
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

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionReport {
    rows: Vec<ReportRow>,
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
    pub report: ExecutionReport,
}

/// One finished read run: a [`PlanRun`] with the explain document rendered
/// from that same plan.
pub struct Executed {
    pub result: omnigraph_compiler::result::QueryResult,
    pub plan: BoundPlan,
    pub explain: Explain,
    pub report: ExecutionReport,
}

impl Executed {
    /// The report as actuals rows in the explain row schema: `tree`
    /// `profile`, no `depth`, `node` the plan node's kind, `detail` the row's
    /// own fields (`id`, `operator`, `status`, `attempts`). The third public
    /// surface beside the rows and explain.
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
        rows.into_result()
    }
}

#[cfg(test)]
pub(super) mod tests;
