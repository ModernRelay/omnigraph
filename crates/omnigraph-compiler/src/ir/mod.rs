pub(crate) mod lower;
pub(crate) mod validate;

use std::collections::HashMap;

use crate::query::ast::{AggFunc, CompOp, Literal, NOW_PARAM_NAME, Param};
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
#[derive(Debug, Clone)]
pub struct SubqueryPredicate {
    pub func: AggFunc,
    /// `None` counts the matched rows.
    pub arg: Option<IRExpr>,
    pub op: CompOp,
    pub right: IRExpr,
}

impl SubqueryPredicate {
    /// `count = 0`: what `not { … }` lowers to.
    pub fn not_exists() -> Self {
        Self {
            func: AggFunc::Count,
            arg: None,
            op: CompOp::Eq,
            right: IRExpr::Literal(Literal::Integer(0)),
        }
    }

    pub fn is_row_count(&self) -> bool {
        self.func == AggFunc::Count && self.arg.is_none()
    }

    /// What a row-count predicate on a literal asks when it only asks whether
    /// any row matched: `Some(false)` for none (`= 0`, `< 1`, `<= 0`),
    /// `Some(true)` for any (`> 0`, `!= 0`, `>= 1`), `None` otherwise.
    pub fn existence(&self) -> Option<bool> {
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

    /// An existence check answers the predicate.
    pub fn is_existence_test(&self) -> bool {
        self.existence().is_some()
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

#[derive(Debug, Clone)]
pub struct IRFilter {
    pub left: IRExpr,
    pub op: CompOp,
    pub right: IRExpr,
}

/// The filter as GQ text, `$p.age > 30`: what a plan prints for it.
impl std::fmt::Display for IRFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

/// The expression as GQ text, the spelling the parser accepts; the `now()`
/// parameter prints as the call it came from.
impl std::fmt::Display for IRExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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

#[cfg(test)]
mod render_tests {
    use super::*;

    fn prop(variable: &str, property: &str) -> IRExpr {
        IRExpr::PropAccess {
            variable: variable.to_string(),
            property: property.to_string(),
        }
    }

    #[test]
    fn filters_and_expressions_print_as_gq() {
        let filter = IRFilter {
            left: prop("p", "age"),
            op: CompOp::Gt,
            right: IRExpr::Literal(Literal::Integer(30)),
        };
        assert_eq!(filter.to_string(), "$p.age > 30");
        let filter = IRFilter {
            left: prop("d", "slug"),
            op: CompOp::StartsWith,
            right: IRExpr::Param("prefix".to_string()),
        };
        assert_eq!(filter.to_string(), "$d.slug starts_with $prefix");
        let filter = IRFilter {
            left: prop("d", "tags"),
            op: CompOp::Contains,
            right: IRExpr::Literal(Literal::List(vec![
                Literal::String("a\"b".to_string()),
                Literal::Bool(true),
                Literal::Float(1.5),
            ])),
        };
        assert_eq!(
            filter.to_string(),
            "$d.tags contains [\"a\\\"b\", true, 1.5]"
        );
        let filter = IRFilter {
            left: prop("e", "since"),
            op: CompOp::Le,
            right: IRExpr::Param(NOW_PARAM_NAME.to_string()),
        };
        assert_eq!(filter.to_string(), "$e.since <= now()");
        let nearest = IRExpr::Nearest {
            variable: "d".to_string(),
            property: "embedding".to_string(),
            query: Box::new(IRExpr::Param("q".to_string())),
        };
        assert_eq!(nearest.to_string(), "nearest($d.embedding, $q)");
        let rrf = IRExpr::Rrf {
            primary: Box::new(nearest),
            secondary: Box::new(IRExpr::Bm25 {
                field: Box::new(prop("d", "text")),
                query: Box::new(IRExpr::Literal(Literal::String("needle".to_string()))),
            }),
            k: Some(Box::new(IRExpr::Literal(Literal::Integer(60)))),
        };
        assert_eq!(
            rrf.to_string(),
            "rrf(nearest($d.embedding, $q), bm25($d.text, \"needle\"), 60)"
        );
        let count = IRExpr::Aggregate {
            func: AggFunc::Count,
            arg: Box::new(IRExpr::Variable("f".to_string())),
        };
        assert_eq!(count.to_string(), "count($f)");
        for (expression, expected) in [
            (
                IRExpr::Search {
                    field: Box::new(prop("d", "text")),
                    query: Box::new(IRExpr::Param("q".into())),
                },
                "search($d.text, $q)",
            ),
            (
                IRExpr::MatchText {
                    field: Box::new(prop("d", "text")),
                    query: Box::new(IRExpr::Param("q".into())),
                },
                "match_text($d.text, $q)",
            ),
            (
                IRExpr::Fuzzy {
                    field: Box::new(prop("d", "text")),
                    query: Box::new(IRExpr::Param("q".into())),
                    max_edits: None,
                },
                "fuzzy($d.text, $q)",
            ),
            (
                IRExpr::Fuzzy {
                    field: Box::new(prop("d", "text")),
                    query: Box::new(IRExpr::Param("q".into())),
                    max_edits: Some(Box::new(IRExpr::Literal(Literal::Integer(2)))),
                },
                "fuzzy($d.text, $q, 2)",
            ),
            (IRExpr::AliasRef("score".into()), "score"),
            (IRExpr::Literal(Literal::Null), "null"),
            (
                IRExpr::Literal(Literal::DateTime("2026-01-31T12:00:00Z".into())),
                "datetime(\"2026-01-31T12:00:00Z\")",
            ),
        ] {
            assert_eq!(expression.to_string(), expected);
        }

        assert_eq!(
            IRExpr::Literal(Literal::Date("2026-01-31".to_string())).to_string(),
            "date(\"2026-01-31\")"
        );
    }

    #[test]
    fn floats_print_in_fixed_notation_with_a_decimal_point() {
        for (value, text) in [
            (1e-7, "0.0000001"),
            (1e21, "1000000000000000000000.0"),
            (2.0, "2.0"),
            (-0.25, "-0.25"),
            (-3e-9, "-0.000000003"),
        ] {
            assert_eq!(Literal::Float(value).to_string(), text);
        }
    }

    #[test]
    fn string_escapes_are_the_ones_the_parser_decodes() {
        let text = "a\nb\r\tc\\\"d";
        let printed = Literal::String(text.to_string()).to_string();
        assert_eq!(printed, "\"a\\nb\\r\\tc\\\\\\\"d\"");
        assert_eq!(crate::error::decode_string_literal(&printed).unwrap(), text);
    }
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
