//! Read IR consumed by the frozen reference executor.

use crate::query::ast::{AggFunc, BinaryOp, CompOp, Literal, NOW_PARAM_NAME, Param, Precedence};
use crate::traversal::EdgeSelection;

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
    pub fn has_edge_selections(&self) -> bool {
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
                    if edges.named().is_none() {
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
        /// A single-hop edge binding emits one output row per edge row and
        /// carries the edge properties under this prefix.
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

/// The HAVING predicate of a correlated subquery: `func` over the inner
/// matches of one outer row, compared with `right` (a literal or parameter).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubqueryPredicate {
    pub func: AggFunc,
    /// `None` counts the matched rows.
    pub arg: Option<IRExpr>,
    pub op: CompOp,
    pub right: IRExpr,
}

impl SubqueryPredicate {
    fn is_row_count(&self) -> bool {
        self.func == AggFunc::Count && self.arg.is_none()
    }

    /// What a row-count predicate on a literal asks when it only asks whether
    /// any row matched: `Some(false)` for none (`= 0`, `< 1`, `<= 0`),
    /// `Some(true)` for any (`> 0`, `!= 0`, `>= 1`), `None` otherwise.
    fn existence(&self) -> Option<bool> {
        let IRExpr::Literal(Literal::Integer(bound)) = self.right else {
            return None;
        };
        if !self.is_row_count() {
            return None;
        }
        match (self.op, bound) {
            (CompOp::Eq | CompOp::Le, 0) | (CompOp::Lt, 1) => Some(false),
            (CompOp::Ne | CompOp::Gt, 0) | (CompOp::Ge, 1) => Some(true),
            _ => None,
        }
    }

    /// `count = 0`, the anti-join.
    pub fn is_not_exists(&self) -> bool {
        self.existence() == Some(false)
    }
}

/// `count > 2`, `sum($m.size) > 100`: what a plan prints for it.
impl std::fmt::Display for SubqueryPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.arg {
            None => write!(f, "{} {} {}", self.func, self.op, self.right),
            Some(arg) => write!(f, "{}({}) {} {}", self.func, arg, self.op, self.right),
        }
    }
}

impl IRExpr {
    /// The operands and operator of a comparison-rooted expression; `None`
    /// for every other node.
    pub fn comparison_parts(&self) -> Option<(&IRExpr, CompOp, &IRExpr)> {
        match self {
            IRExpr::Binary {
                left,
                op: BinaryOp::Compare(op),
                right,
            } => Some((left, *op, right)),
            _ => None,
        }
    }

    fn precedence(&self) -> Precedence {
        match self {
            IRExpr::Binary {
                op: BinaryOp::Or, ..
            } => Precedence::Or,
            IRExpr::Binary {
                op: BinaryOp::And, ..
            } => Precedence::And,
            IRExpr::Not(_) => Precedence::Not,
            IRExpr::Binary {
                op: BinaryOp::Compare(_),
                ..
            }
            | IRExpr::IsNull { .. } => Precedence::Comparison,
            _ => Precedence::Atom,
        }
    }

    /// Print `self` as an operand of a node with `parent` precedence, in
    /// parentheses when it binds looser, when both are comparisons or null tests,
    /// or as the right operand of an `and`/`or` it repeats (left-nested prints bare).
    fn fmt_operand(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        parent: Precedence,
        right_operand: bool,
    ) -> std::fmt::Result {
        let own = self.precedence();
        let parenthesized =
            own < parent || (own == parent && (parent == Precedence::Comparison || right_operand));
        if parenthesized {
            write!(f, "({self})")
        } else {
            write!(f, "{self}")
        }
    }
}

/// The expression as GQ text, the spelling the parser accepts; the `now()`
/// parameter prints as the call it came from. Parentheses are the minimal
/// ones (`fmt_operand`), never the user's.
impl std::fmt::Display for IRExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IRExpr::Binary { left, op, right } => {
                let precedence = self.precedence();
                left.fmt_operand(f, precedence, false)?;
                write!(f, " {op} ")?;
                right.fmt_operand(f, precedence, true)
            }
            IRExpr::Not(operand) => {
                f.write_str("not ")?;
                operand.fmt_operand(f, Precedence::Not, false)
            }
            IRExpr::IsNull { expr, negated } => {
                expr.fmt_operand(f, Precedence::Comparison, false)?;
                f.write_str(if *negated { " is not null" } else { " is null" })
            }
            IRExpr::PropAccess { variable, property } => write!(f, "${variable}.{property}"),
            IRExpr::Nearest {
                variable,
                property,
                query,
            } => write!(f, "nearest(${variable}.{property}, {query})"),
            IRExpr::Search { field, query } => write!(f, "search({field}, {query})"),
            IRExpr::Fuzzy {
                field,
                query,
                max_edits: None,
            } => write!(f, "fuzzy({field}, {query})"),
            IRExpr::Fuzzy {
                field,
                query,
                max_edits: Some(max_edits),
            } => write!(f, "fuzzy({field}, {query}, {max_edits})"),
            IRExpr::MatchText { field, query } => write!(f, "match_text({field}, {query})"),
            IRExpr::Bm25 { field, query } => write!(f, "bm25({field}, {query})"),
            IRExpr::Rrf {
                primary,
                secondary,
                k: None,
            } => write!(f, "rrf({primary}, {secondary})"),
            IRExpr::Rrf {
                primary,
                secondary,
                k: Some(k),
            } => write!(f, "rrf({primary}, {secondary}, {k})"),
            IRExpr::Variable(name) => write!(f, "${name}"),
            IRExpr::Param(name) if name == NOW_PARAM_NAME => f.write_str("now()"),
            IRExpr::Param(name) => write!(f, "${name}"),
            IRExpr::Literal(literal) => write!(f, "{literal}"),
            IRExpr::Aggregate { func, arg } => write!(f, "{func}({arg})"),
            IRExpr::AliasRef(alias) => f.write_str(alias),
        }
    }
}

/// An expression in the frozen reference read representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    Binary {
        left: Box<IRExpr>,
        op: BinaryOp,
        right: Box<IRExpr>,
    },
    Not(Box<IRExpr>),
    IsNull {
        expr: Box<IRExpr>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IRProjection {
    pub expr: IRExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IROrdering {
    pub expr: IRExpr,
    pub descending: bool,
}
