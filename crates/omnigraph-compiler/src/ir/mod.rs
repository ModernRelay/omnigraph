pub mod fold;
pub(crate) mod lower;
pub(crate) mod validate;

use std::collections::HashMap;

use crate::query::ast::{AggFunc, BinaryOp, CompOp, Literal, NOW_PARAM_NAME, Param, Precedence};
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
    /// `predicate` is Boolean over the target's properties, each spelled
    /// `IRExpr::PropAccess { variable: <type name>, property: <physical column> }`,
    /// and the declared parameters.
    Update {
        type_name: String,
        assignments: Vec<IRAssignment>,
        predicate: IRExpr,
    },
    Delete {
        type_name: String,
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
        edge_type: String,
        direction: Direction,
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

impl IRExpr {
    /// `left <op> right` as one comparison node.
    pub fn comparison(left: IRExpr, op: CompOp, right: IRExpr) -> Self {
        IRExpr::Binary {
            left: Box::new(left),
            op: BinaryOp::Compare(op),
            right: Box::new(right),
        }
    }

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

    /// The top-level `and` chain split into its conjuncts, in written order;
    /// an `or`, a `not` or a comparison is one conjunct.
    pub fn into_conjuncts(self) -> Vec<IRExpr> {
        match self {
            IRExpr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut conjuncts = left.into_conjuncts();
                conjuncts.extend(right.into_conjuncts());
                conjuncts
            }
            other => vec![other],
        }
    }

    /// The conjunction of `conjuncts`, left-nested; `None` when empty.
    pub fn and_all(conjuncts: impl IntoIterator<Item = IRExpr>) -> Option<IRExpr> {
        conjuncts.into_iter().reduce(|left, right| IRExpr::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        })
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

#[cfg(test)]
mod render_tests {
    use super::*;

    fn prop(variable: &str, property: &str) -> IRExpr {
        IRExpr::PropAccess {
            variable: variable.to_string(),
            property: property.to_string(),
        }
    }

    fn int(value: i64) -> IRExpr {
        IRExpr::Literal(Literal::Integer(value))
    }

    fn and(left: IRExpr, right: IRExpr) -> IRExpr {
        IRExpr::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    fn or(left: IRExpr, right: IRExpr) -> IRExpr {
        IRExpr::Binary {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
        }
    }

    #[test]
    fn filters_and_expressions_print_as_gq() {
        let filter = IRExpr::comparison(prop("p", "age"), CompOp::Gt, int(30));
        assert_eq!(filter.to_string(), "$p.age > 30");
        let filter = IRExpr::comparison(
            prop("d", "slug"),
            CompOp::StartsWith,
            IRExpr::Param("prefix".to_string()),
        );
        assert_eq!(filter.to_string(), "$d.slug starts_with $prefix");
        let filter = IRExpr::comparison(
            prop("d", "tags"),
            CompOp::Contains,
            IRExpr::Literal(Literal::List(vec![
                Literal::String("a\"b".to_string()),
                Literal::Bool(true),
                Literal::Float(1.5),
            ])),
        );
        assert_eq!(
            filter.to_string(),
            "$d.tags contains [\"a\\\"b\", true, 1.5]"
        );
        let filter = IRExpr::comparison(
            prop("e", "since"),
            CompOp::Le,
            IRExpr::Param(NOW_PARAM_NAME.to_string()),
        );
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
    fn boolean_trees_print_with_minimal_parentheses() {
        let a = IRExpr::comparison(prop("p", "a"), CompOp::Eq, int(1));
        let b = IRExpr::comparison(prop("p", "b"), CompOp::Eq, int(2));
        let c = IRExpr::comparison(prop("p", "c"), CompOp::Eq, int(3));
        assert_eq!(
            or(a.clone(), and(b.clone(), c.clone())).to_string(),
            "$p.a = 1 or $p.b = 2 and $p.c = 3"
        );
        assert_eq!(
            and(or(a.clone(), b.clone()), c.clone()).to_string(),
            "($p.a = 1 or $p.b = 2) and $p.c = 3"
        );
        assert_eq!(
            IRExpr::Not(Box::new(and(a.clone(), b.clone()))).to_string(),
            "not ($p.a = 1 and $p.b = 2)"
        );
        assert_eq!(
            and(IRExpr::Not(Box::new(a.clone())), b.clone()).to_string(),
            "not $p.a = 1 and $p.b = 2"
        );
        assert_eq!(
            and(and(a.clone(), b.clone()), c.clone()).to_string(),
            "$p.a = 1 and $p.b = 2 and $p.c = 3"
        );
        assert_eq!(
            and(a.clone(), and(b.clone(), c.clone())).to_string(),
            "$p.a = 1 and ($p.b = 2 and $p.c = 3)"
        );
        assert_eq!(
            or(a.clone(), or(b.clone(), c.clone())).to_string(),
            "$p.a = 1 or ($p.b = 2 or $p.c = 3)"
        );
        let age = IRExpr::comparison(prop("p", "age"), CompOp::Gt, int(30));
        assert_eq!(
            IRExpr::comparison(
                age.clone(),
                CompOp::Eq,
                IRExpr::Literal(Literal::Bool(true))
            )
            .to_string(),
            "($p.age > 30) = true"
        );
        assert_eq!(
            IRExpr::IsNull {
                expr: Box::new(age.clone()),
                negated: false,
            }
            .to_string(),
            "($p.age > 30) is null"
        );
        assert_eq!(
            IRExpr::IsNull {
                expr: Box::new(prop("p", "age")),
                negated: true,
            }
            .to_string(),
            "$p.age is not null"
        );
        assert_eq!(
            and(and(a.clone(), b.clone()), c.clone()).into_conjuncts(),
            vec![a.clone(), b.clone(), c.clone()]
        );
        assert_eq!(
            IRExpr::and_all([a.clone(), b.clone(), c.clone()]),
            Some(and(and(a, b), c))
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

/// The bound expression: one type for filters, projections, order keys,
/// assignments and mutation predicates. Equal when the trees are equal node
/// by node (`Literal::Float` by bits), the equality the planner's filter
/// deduplication and the `Predicate` hash rely on.
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
