//! The explain document: what the gate decided and why, with the plans it
//! built. A request mode on the engine surfaces, a plain value here.

use serde::Serialize;
use serde_json::Value;

use crate::gate::Unrouted;
use crate::operation::{Operation, PageBudgetSpec, ScopeSpec};
use crate::physical::StatisticSource;
use crate::registry::{Entry, Route};
use crate::route::RouteOverride;

/// Incremented when a field's meaning changes.
pub const EXPLAIN_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EntrySummary {
    pub name: &'static str,
    pub shape: String,
    pub route: Route,
    pub since: &'static str,
}

impl EntrySummary {
    pub fn of(entry: &'static Entry) -> Self {
        Self {
            name: entry.name,
            shape: entry.shape.census().to_string(),
            route: entry.route,
            since: entry.since,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableSummary {
    pub type_key: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OperationSummary {
    pub kind: &'static str,
    pub tables: Vec<TableSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<ScopeSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget: Option<PageBudgetSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,
}

impl OperationSummary {
    pub fn of(op: &Operation) -> Self {
        let tables = op
            .sides()
            .into_iter()
            .map(|side| TableSummary {
                type_key: side.table.type_key.clone(),
                version: side.version,
            })
            .collect();
        let (scope, budget, resume) = match op {
            Operation::CommitDiff {
                scope,
                budget,
                resume,
                ..
            } => (Some(*scope), Some(*budget), resume.clone()),
            _ => (None, None, None),
        };
        Self {
            kind: op.kind(),
            tables,
            scope,
            budget,
            resume,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            kind: "",
            tables: Vec::new(),
            scope: None,
            budget: None,
            resume: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Explain {
    pub explain_version: u32,
    pub route: &'static str,
    #[serde(rename = "override")]
    pub override_: RouteOverride,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry: Option<EntrySummary>,
    pub operation: OperationSummary,
    pub logical_plan: Option<Value>,
    pub logical_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_plan: Option<Value>,
    /// The pipelines the push operators build from a diff or merge plan: per
    /// pipeline its source, its operators and its sink, in run order. Absent
    /// for a query, which the engine lowers to one DataFusion plan.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipelines: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Vec<StatisticSource>>,
    pub passes: Vec<&'static str>,
}

impl Explain {
    pub fn without_plan(operation: OperationSummary, override_: RouteOverride) -> Self {
        Self {
            explain_version: EXPLAIN_VERSION,
            route: "executor",
            override_,
            reason: None,
            entry: None,
            operation,
            logical_plan: None,
            logical_hash: None,
            physical_plan: None,
            pipelines: None,
            statistics: None,
            passes: Vec::new(),
        }
    }

    pub(crate) fn with_reason(mut self, reason: &Unrouted) -> Self {
        self.route = "executor";
        self.reason = Some(reason.to_json());
        self
    }

    pub(crate) fn routed(mut self) -> Self {
        self.route = "planner";
        self.reason = None;
        self
    }

    /// The engine's runner executes the physical plan (a GQ query).
    pub(crate) fn engine(mut self) -> Self {
        self.route = "engine";
        self.reason = None;
        self
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("explain serializes")
    }
}
