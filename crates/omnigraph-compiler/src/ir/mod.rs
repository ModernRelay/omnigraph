pub(crate) mod lower;

use std::collections::HashMap;

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

#[derive(Debug, Clone)]
pub struct MutationIR {
    pub name: String,
    pub params: Vec<Param>,
    pub op: MutationOpIR,
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
