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
    /// The query's retrieval plan, stated by lowering instead of rediscovered
    /// from `order_by[0]` at execution (search-contracts RFC, plan-truth
    /// phase). `order_by` still carries the rank expression for ordering
    /// semantics — score direction, secondary keys — while this field is the
    /// single authority for WHAT retrieval runs and under which bounds.
    /// Parameter and embedding resolution stay execution-time (`query` inside
    /// is an unresolved expression), so one lowered plan serves every
    /// parameterization.
    pub retrieval: Option<RetrievalIR>,
}

/// A lowered retrieval source or fusion. Leaves are `Nearest`/`Bm25`;
/// `FuseRrf` holds exactly two leaf arms (the grammar and T21 enforce that
/// shape today — a future N-arm fusion widens this enum, not `order_by`
/// inference).
#[derive(Debug, Clone)]
pub enum RetrievalIR {
    /// Vector top-k on `variable.property`. `k` is the effective candidate
    /// count, fixed at lowering: the query limit at the root (T17 guarantees
    /// one), the fusion limit — default 100 — inside an rrf arm.
    Nearest {
        variable: String,
        property: String,
        query: Box<IRExpr>,
        k: Option<u64>,
    },
    /// Ranked lexical retrieval on `variable.property`. `scan_cap` is the
    /// lowering-decided bounded-scan policy (issue #563): `limit ×
    /// BM25_SCAN_OVERFETCH_FACTOR` when the query has a limit, no aggregate
    /// return, and no secondary order keys; `None` scans every match.
    /// Always `None` inside an rrf arm — fusion needs the arm's complete
    /// ranking (a capped arm silently shifts fused ranks; see PR #574).
    Bm25 {
        variable: String,
        property: String,
        query: Box<IRExpr>,
        scan_cap: Option<u64>,
    },
    /// Reciprocal-rank fusion of two leaf arms. `k` is the rank-constant
    /// expression (engine default 60); `limit` is the query limit (T21
    /// guarantees one).
    FuseRrf {
        primary: Box<RetrievalIR>,
        secondary: Box<RetrievalIR>,
        k: Option<Box<IRExpr>>,
        limit: Option<u64>,
    },
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
        /// Variable bound to the matched edge row (`$p $w:knows $f`), if any.
        /// Changes the op's contract: one output row per matching edge ROW
        /// (not per distinct endpoint pair), edge property columns carried
        /// under this prefix. Always single-hop (typecheck T23).
        edge_binding: Option<String>,
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
