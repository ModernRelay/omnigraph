//! The engine's execution report as the runner reads it: per node of the plan
//! that ran, its `id` and what each attempt's operator ran; every other key of
//! the row is ignored. The `ran` lines of `--- expect plan` read them.

use omnigraph_planner::NodeId;
use serde::{Deserialize, Serialize};

/// What one pass of an operator did, as the engine spells it: `true` or
/// `false` for polled or not, or the side of the node's declared switch it
/// took (`hash_join`, `id_lookup`, `csr`, `indexed_scan`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum Ran {
    Polled(bool),
    Took(String),
}

impl Ran {
    /// The side of a declared switch, `None` on a node without one.
    pub(crate) fn side(&self) -> Option<&str> {
        match self {
            Self::Took(side) => Some(side),
            Self::Polled(_) => None,
        }
    }
}

/// One pass of the tree over a node's operator: whether the operator polled
/// or which side it took.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct Attempt {
    pub(crate) ran: Ran,
}

/// One row of the engine's execution report, read from its serialized form.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct Row {
    pub(crate) id: NodeId,
    pub(crate) attempts: Vec<Attempt>,
}

#[derive(Debug, Deserialize)]
struct Report {
    rows: Vec<Row>,
}

/// The rows of an `Executed`'s `report`, whose type the engine crate exports
/// no name for.
pub(crate) fn report_rows(report: &impl Serialize) -> Result<Vec<Row>, String> {
    serde_json::to_value(report)
        .and_then(serde_json::from_value::<Report>)
        .map(|report| report.rows)
        .map_err(|error| format!("the execution report does not read as rows: {error}"))
}
