pub mod coerce;
pub mod core;
pub mod expr;
pub mod fold;
pub mod load;
pub(crate) mod lower;
pub(crate) mod validate;
pub mod write;

pub use expr::{BlockAggregateExpr, IRExpr, IROrdering, IRProjection, SubqueryPredicate};
pub use load::{Endpoint, LoadCheck, LoadIR, UniqueRole, ValidateCheck, checks_for, lower_load};
pub use lower::is_fresh_variable;
pub use write::{DeltaClass, WriteKind, WriteTableIR, lower_write_table};

use std::collections::HashMap;

use crate::query::ast::{Literal, MutationWriteMode, Param};
use crate::query::typecheck::MutationTarget;
use crate::traversal::EdgeSelection;
use crate::types::ExprType;

/// A declaration and the type resolved for it by the compiler.
#[derive(Debug, Clone)]
pub struct IRParam {
    pub declaration: Param,
    pub ty: ExprType,
}

#[derive(Debug, Clone)]
pub struct QueryIR {
    pub name: String,
    pub params: Vec<IRParam>,
    pub pipeline: Vec<IROp>,
    pub return_exprs: Vec<IRProjection>,
    /// `return distinct`: one row per distinct projected tuple.
    pub distinct: bool,
    pub order_by: Vec<IROrdering>,
    pub limit: Option<u64>,
}

impl QueryIR {
    pub fn has_edge_selections(&self) -> bool {
        self.any_edge_selection(|edges| edges.named().is_none())
    }

    pub fn has_wildcard_traversal(&self) -> bool {
        self.any_edge_selection(EdgeSelection::is_wildcard)
    }

    fn any_edge_selection(&self, predicate: impl Fn(&EdgeSelection) -> bool) -> bool {
        let mut pending: Vec<_> = self.pipeline.iter().collect();
        while let Some(op) = pending.pop() {
            match op {
                IROp::Expand {
                    edges,
                    src_var: _,
                    dst_var: _,
                    src_type: _,
                    dst_type: _,
                    min_hops: _,
                    max_hops: _,
                    dst_filters: _,
                    edge_binding: _,
                } => {
                    if predicate(edges) {
                        return true;
                    }
                }
                IROp::AntiJoin {
                    inner,
                    outer_var: _,
                    predicate: _,
                } => pending.extend(inner),
                IROp::NodeScan {
                    variable: _,
                    type_name: _,
                    filters: _,
                }
                | IROp::Filter(_) => {}
            }
        }
        false
    }
}

#[derive(Debug, Clone)]
pub struct MutationIR {
    pub name: String,
    pub params: Vec<IRParam>,
    pub ops: Vec<MutationOpIR>,
    /// The declaration's write clause: the branch head, an unpublished
    /// commit, or an unattached snapshot with its optional base.
    pub write_mode: MutationWriteMode,
}

#[derive(Debug, Clone)]
pub enum MutationOpIR {
    Insert {
        target: MutationTarget,
        assignments: Vec<IRAssignment>,
    },
    /// `predicate` is Boolean over the target's properties, each spelled
    /// `IRExpr::PropAccess { variable: <type name>, property: <physical column> }`,
    /// and the declared parameters.
    Update {
        target: MutationTarget,
        assignments: Vec<IRAssignment>,
        predicate: IRExpr,
    },
    Delete {
        target: MutationTarget,
        predicate: IRExpr,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IRAssignment {
    pub property: String,
    pub value: IRExpr,
}

/// Resolved runtime parameters: param name → literal value.
pub type ParamMap = HashMap<String, Literal>;

#[derive(Debug, Clone)]
pub enum IROp {
    NodeScan {
        variable: String,
        type_name: String,
        /// Boolean expressions over `variable`, one per inline binding match.
        filters: Vec<IRExpr>,
    },
    Expand {
        src_var: String,
        dst_var: String,
        edges: EdgeSelection,
        src_type: String,
        dst_type: String,
        min_hops: u32,
        max_hops: Option<u32>,
        /// Filters from a deferred destination binding, pushed into the
        /// Expand so the executor can apply them during hydration (Lance
        /// SQL pushdown) rather than as a separate post-expand pass.
        dst_filters: Vec<IRExpr>,
        /// Variable bound to the matched edge row (`$p $w:knows $f`), if any.
        /// Changes the op's contract: one output row per matching edge ROW
        /// (not per distinct endpoint pair), edge property columns carried
        /// under this prefix. Always single-hop (typecheck T23).
        edge_binding: Option<String>,
    },
    /// A Boolean expression the type checker proved, as written.
    Filter(IRExpr),
    /// A correlated subquery, decorrelated: `inner` runs once over the outer
    /// rows and `predicate` decides per outer row on the aggregate of its
    /// matches. `not { … }` is the anti-join case, `count = 0`.
    AntiJoin {
        /// The outer variable whose id is used for the join key
        outer_var: String,
        /// The inner pipeline that produces rows to anti-join against
        inner: Vec<IROp>,
        predicate: SubqueryPredicate,
    },
}
