pub(crate) mod lower;

use std::collections::{BTreeSet, HashMap};

use crate::query::ast::{AggFunc, CompOp, Literal, Param};
use crate::types::Direction;

#[derive(Debug, Clone)]
pub struct QueryIR {
    pub name: String,
    pub params: Vec<Param>,
    pub pipeline: Vec<IROp>,
    pub return_exprs: Vec<IRProjection>,
    pub order_by: Vec<IROrdering>,
    pub limit: Option<u64>,
}

impl QueryIR {
    /// Every edge type this query traverses (`Expand`) or anti-joins through
    /// (`AntiJoin.inner`), recursively, in deterministic order. The graph-index
    /// builder uses this to build only the referenced edges' topology instead of
    /// all catalog edges (the cold-engine first-build cost). Completeness is
    /// load-bearing: a missed edge surfaces as a loud "no adjacency index for edge
    /// X" at execution, never a silent wrong result.
    pub fn referenced_edge_types(&self) -> BTreeSet<String> {
        fn collect(pipeline: &[IROp], out: &mut BTreeSet<String>) {
            for op in pipeline {
                match op {
                    IROp::Expand { edge_type, .. } => {
                        out.insert(edge_type.clone());
                    }
                    IROp::AntiJoin { inner, .. } => collect(inner, out),
                    _ => {}
                }
            }
        }
        let mut out = BTreeSet::new();
        collect(&self.pipeline, &mut out);
        out
    }
}

#[derive(Debug, Clone)]
pub struct MutationIR {
    pub name: String,
    pub params: Vec<Param>,
    pub ops: Vec<MutationOpIR>,
}

#[derive(Debug, Clone)]
pub enum MutationOpIR {
    Insert {
        type_name: String,
        assignments: Vec<IRAssignment>,
    },
    Update {
        type_name: String,
        assignments: Vec<IRAssignment>,
        predicate: IRMutationPredicate,
    },
    Delete {
        type_name: String,
        predicate: IRMutationPredicate,
    },
}

#[derive(Debug, Clone)]
pub struct IRAssignment {
    pub property: String,
    pub value: IRExpr,
}

#[derive(Debug, Clone)]
pub struct IRMutationPredicate {
    pub property: String,
    pub op: CompOp,
    pub value: IRExpr,
}

/// Resolved runtime parameters: param name → literal value.
pub type ParamMap = HashMap<String, Literal>;

#[derive(Debug, Clone)]
pub enum IROp {
    NodeScan {
        variable: String,
        type_name: String,
        filters: Vec<IRFilter>,
    },
    Expand {
        src_var: String,
        dst_var: String,
        edge_type: String,
        direction: Direction,
        dst_type: String,
        min_hops: u32,
        max_hops: Option<u32>,
        /// Filters from a deferred destination binding, pushed into the
        /// Expand so the executor can apply them during hydration (Lance
        /// SQL pushdown) rather than as a separate post-expand pass.
        dst_filters: Vec<IRFilter>,
    },
    Filter(IRFilter),
    AntiJoin {
        /// The outer variable whose id is used for the join key
        outer_var: String,
        /// The inner pipeline that produces rows to anti-join against
        inner: Vec<IROp>,
    },
}

#[derive(Debug, Clone)]
pub struct IRFilter {
    pub left: IRExpr,
    pub op: CompOp,
    pub right: IRExpr,
}

#[derive(Debug, Clone)]
pub enum IRExpr {
    PropAccess {
        variable: String,
        property: String,
    },
    Nearest {
        variable: String,
        property: String,
        query: Box<IRExpr>,
    },
    Search {
        field: Box<IRExpr>,
        query: Box<IRExpr>,
    },
    Fuzzy {
        field: Box<IRExpr>,
        query: Box<IRExpr>,
        max_edits: Option<Box<IRExpr>>,
    },
    MatchText {
        field: Box<IRExpr>,
        query: Box<IRExpr>,
    },
    Bm25 {
        field: Box<IRExpr>,
        query: Box<IRExpr>,
    },
    Rrf {
        primary: Box<IRExpr>,
        secondary: Box<IRExpr>,
        k: Option<Box<IRExpr>>,
    },
    Variable(String),
    Param(String),
    Literal(Literal),
    Aggregate {
        func: AggFunc,
        arg: Box<IRExpr>,
    },
    AliasRef(String),
}

#[derive(Debug, Clone)]
pub struct IRProjection {
    pub expr: IRExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IROrdering {
    pub expr: IRExpr,
    pub descending: bool,
}

#[cfg(test)]
mod referenced_edge_tests {
    use super::*;

    fn expand(edge: &str) -> IROp {
        IROp::Expand {
            src_var: "a".into(),
            dst_var: "b".into(),
            edge_type: edge.into(),
            direction: Direction::Out,
            dst_type: "T".into(),
            min_hops: 1,
            max_hops: Some(1),
            dst_filters: vec![],
        }
    }

    fn ir(pipeline: Vec<IROp>) -> QueryIR {
        QueryIR {
            name: "q".into(),
            params: vec![],
            pipeline,
            return_exprs: vec![],
            order_by: vec![],
            limit: None,
        }
    }

    #[test]
    fn collects_expand_and_nested_antijoin_edges_dedup_sorted() {
        let q = ir(vec![
            IROp::NodeScan {
                variable: "a".into(),
                type_name: "T".into(),
                filters: vec![],
            },
            expand("Knows"),
            // AntiJoin's inner pipeline must be recursed into; the repeated Knows
            // must dedup; output is sorted (BTreeSet).
            IROp::AntiJoin {
                outer_var: "a".into(),
                inner: vec![expand("WorksAt"), expand("Knows")],
            },
        ]);
        assert_eq!(
            q.referenced_edge_types().into_iter().collect::<Vec<_>>(),
            vec!["Knows".to_string(), "WorksAt".to_string()],
        );
    }

    #[test]
    fn no_traversal_yields_empty() {
        let q = ir(vec![IROp::NodeScan {
            variable: "a".into(),
            type_name: "T".into(),
            filters: vec![],
        }]);
        assert!(q.referenced_edge_types().is_empty());
    }
}
