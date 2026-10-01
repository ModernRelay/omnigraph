//! What a checked read declaration requires of every plan for it (RFC 0047,
//! "Requirements come from the checked query"), derived from the declaration
//! as written, its type context and the catalog, never from the IR: the IR
//! has already lost meaning (a projected `bm25()` is a score column, an
//! inline match is a comparison, constants are folded). The requirements are
//! a derived validation input, never an editable description of the query.
//!
//! The [`Matcher`] relates a written expression to the IR expression a plan
//! carries through the compiler's defined lowering patterns: property leaves
//! on their physical columns, parameters as parameters, `in` as `contains`
//! with its operands swapped, `contains` over a String as `StringContains`,
//! a bare search call as `call = true`, a projected rank call as its score
//! column, and a constant subtree as the literal it evaluates to under the
//! bound parameters. It checks a lowering; it does not lower.

use std::collections::{BTreeMap, HashSet};

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, IRProjection};
use omnigraph_compiler::query::ast::{
    AggFunc, BinaryOp, Clause, CompOp, DISTANCE_COLUMN, Expr, Literal, NOW_PARAM_NAME, Ordering,
    Projection, SCORE_COLUMN,
};
use omnigraph_compiler::query::typecheck::BoundVariable;
use omnigraph_compiler::traversal::{EDGE_TYPE_COLUMN, EDGE_TYPE_META};
use omnigraph_compiler::{SYSTEM_COLUMNS_META, SystemColumns};

use super::budget::Budget;
use super::{AcceptInput, ValidationError};
use crate::physical::RankKind;

/// The value of a constant expression (literals, bound parameters, `now()`,
/// and comparisons, `and`, `or`, `not` and null tests over them) under the
/// bound parameters, or `None` when it has none. The engine implements it
/// with the evaluator its constant folding uses, so a folded literal is
/// checked against the same rules that produced it.
pub trait ConstantEvaluator {
    fn evaluate(&self, expr: &IRExpr) -> Option<Literal>;
}

/// One retrieval a search order names: `nearest($v.p, q)` or `bm25($v.p, q)`.
#[derive(Debug, Clone)]
pub(crate) struct Retrieval {
    pub kind: RankKind,
    pub binding: String,
    pub property: String,
    pub query: Expr,
}

/// The retrieval a rank call names, `None` for any other expression.
pub(crate) fn retrieval_of(expr: &Expr) -> Option<Retrieval> {
    Retrieval::of(expr)
}

impl Retrieval {
    fn of(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Nearest {
                variable,
                property,
                query,
            } => Some(Self {
                kind: RankKind::Nearest,
                binding: variable.clone(),
                property: property.clone(),
                query: query.as_ref().clone(),
            }),
            Expr::Bm25 { field, query } => match field.as_ref() {
                Expr::PropAccess { variable, property } => Some(Self {
                    kind: RankKind::Bm25,
                    binding: variable.clone(),
                    property: property.clone(),
                    query: query.as_ref().clone(),
                }),
                _ => None,
            },
            _ => None,
        }
    }

    /// The approximation the retrieval's contract declares: `nearest`
    /// membership is approximate even when a run scores flat; `bm25` is exact.
    pub fn approximate(&self) -> bool {
        self.kind == RankKind::Nearest
    }
}

/// The search a leading `order` key names.
#[derive(Debug, Clone)]
pub(crate) enum Search {
    Rank(Retrieval),
    Fuse {
        arms: [Retrieval; 2],
        k: Option<Expr>,
    },
}

/// A correlated block of the top-level scope: the aggregate it compares.
#[derive(Debug, Clone)]
pub(crate) struct Block {
    pub func: AggFunc,
    pub op: CompOp,
    pub right: Expr,
}

/// What every plan for one checked declaration must preserve.
#[derive(Debug, Clone)]
pub struct Requirements {
    /// The named bindings of the top-level scope and their types.
    pub(crate) bindings: BTreeMap<String, BoundVariable>,
    /// Edge bindings of a traversal over more than one edge type, whose
    /// identity is the pair (`@type`, `@id`).
    pub(crate) selected_edges: HashSet<String>,
    /// Traversal clauses of the top-level scope.
    pub(crate) traversals: usize,
    /// Correlated blocks of the top-level scope, in written order.
    pub(crate) blocks: Vec<Block>,
    pub(crate) search: Option<Search>,
    /// Every eligibility clause of the top-level scope as written, an
    /// inline binding match spelled as its comparison; [`Matcher::conjuncts`]
    /// splits a clause the way its lowering does.
    pub(crate) eligibility: Vec<Expr>,
    pub(crate) returns: Vec<Projection>,
    /// The `order` keys after a leading search key.
    pub(crate) order: Vec<Ordering>,
    pub(crate) limit: Option<u64>,
    pub(crate) aggregate: bool,
}

impl Requirements {
    pub(crate) fn derive(
        input: &AcceptInput<'_>,
        budget: &mut Budget,
    ) -> Result<Self, ValidationError> {
        let decl = input.checked.decl();
        let types = input.checked.types();
        let mut eligibility = Vec::new();
        let mut traversals = 0;
        let mut blocks = Vec::new();
        let mut selected_edges = HashSet::new();
        for clause in &decl.match_clause {
            budget.visit(1)?;
            match clause {
                Clause::Binding(binding) => {
                    let node_type = input
                        .catalog
                        .node_types
                        .get(&binding.type_name)
                        .ok_or_else(|| {
                            ValidationError::violated(
                                "binding identity",
                                format!(
                                    "`${}` binds `{}`, which the catalog does not hold",
                                    binding.variable, binding.type_name
                                ),
                            )
                        })?;
                    for matched in &binding.prop_matches {
                        let list = node_type
                            .properties
                            .get(&matched.prop_name)
                            .is_some_and(|property| property.list);
                        eligibility.push(Expr::comparison(
                            Expr::PropAccess {
                                variable: binding.variable.clone(),
                                property: matched.prop_name.clone(),
                            },
                            if list { CompOp::Contains } else { CompOp::Eq },
                            matched.value.clone(),
                        ));
                    }
                }
                Clause::Traversal(_) => {}
                Clause::Filter(filter) => {
                    eligibility.push(filter.clone().with_search_predicates_spelled());
                }
                Clause::Subquery(block) => blocks.push(Block {
                    func: block.func,
                    op: block.op,
                    right: block.right.clone(),
                }),
            }
        }
        for traversal in &types.traversals {
            budget.visit(1)?;
            traversals += 1;
            if let Some(edge) = traversal
                .edge_binding
                .as_deref()
                .filter(|edge| *edge != "_")
                && traversal.edges.named().is_none()
            {
                selected_edges.insert(edge.to_string());
            }
        }
        let mut order = decl.order_clause.clone();
        let search = match order.first().map(|key| &key.expr) {
            Some(Expr::Rrf {
                primary,
                secondary,
                k,
            }) => match (Retrieval::of(primary), Retrieval::of(secondary)) {
                (Some(primary), Some(secondary)) => Some(Search::Fuse {
                    arms: [primary, secondary],
                    k: k.as_deref().cloned(),
                }),
                _ => None,
            },
            Some(expr) => Retrieval::of(expr).map(Search::Rank),
            None => None,
        };
        if search.is_some() {
            order.remove(0);
        }
        Ok(Self {
            bindings: types
                .bindings
                .iter()
                .map(|(name, bound)| (name.clone(), bound.clone()))
                .collect(),
            selected_edges,
            traversals,
            blocks,
            search,
            eligibility,
            returns: decl.return_clause.clone(),
            order,
            limit: decl.limit,
            aggregate: decl
                .return_clause
                .iter()
                .any(|projection| matches!(projection.expr, Expr::Aggregate { .. })),
        })
    }

    /// Every retrieval the query names, in written order.
    pub(crate) fn retrievals(&self) -> Vec<&Retrieval> {
        match &self.search {
            None => Vec::new(),
            Some(Search::Rank(retrieval)) => vec![retrieval],
            Some(Search::Fuse { arms, .. }) => arms.iter().collect(),
        }
    }
}

/// Where an expression sits, which decides the lowering pattern it follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Position {
    /// A `match` conjunct: constant subtrees are folded to their value.
    Filter,
    /// A `return` item: a rank call is its score column.
    Return,
    /// An `order` key or a call argument: lowered as written.
    Plain,
}

/// Relates written expressions to the IR expressions of a plan.
pub(crate) struct Matcher<'a> {
    pub catalog: &'a Catalog,
    pub params: HashSet<&'a str>,
    pub bindings: &'a BTreeMap<String, BoundVariable>,
    pub constants: &'a dyn ConstantEvaluator,
}

impl<'a> Matcher<'a> {
    pub(crate) fn new(input: &'a AcceptInput<'a>, requirements: &'a Requirements) -> Self {
        Self {
            catalog: input.catalog,
            params: input
                .checked
                .decl()
                .params
                .iter()
                .map(|param| param.name.as_str())
                .collect(),
            bindings: &requirements.bindings,
            constants: input.constants,
        }
    }

    fn system_columns(&self) -> SystemColumns {
        self.catalog.system_columns
    }

    /// The physical column a written property leaf reads.
    pub(crate) fn physical(&self, property: &str) -> String {
        let columns = self.system_columns();
        match property {
            EDGE_TYPE_META => EDGE_TYPE_COLUMN.to_string(),
            name if name == SYSTEM_COLUMNS_META.id => columns.id.to_string(),
            name if name == SYSTEM_COLUMNS_META.src => columns.src.to_string(),
            name if name == SYSTEM_COLUMNS_META.dst => columns.dst.to_string(),
            other => other.to_string(),
        }
    }

    /// The one edge type a single-type edge binding's `@type` reads, which
    /// the lowering writes as a String literal.
    fn single_edge_type(&self, variable: &str, property: &str) -> Option<&str> {
        if property != EDGE_TYPE_META {
            return None;
        }
        match self.bindings.get(variable) {
            Some(BoundVariable::Edge { type_names }) => match type_names.as_slice() {
                [name] => Some(name),
                _ => None,
            },
            _ => None,
        }
    }

    /// Whether `expr` reads only literals, parameters, the clock and the
    /// `@type` of a single-type edge binding.
    pub(crate) fn constant(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) | Expr::Now => true,
            Expr::PropAccess { variable, property } => {
                self.single_edge_type(variable, property).is_some()
            }
            Expr::Variable(name) => self.params.contains(name.as_str()),
            Expr::Binary { left, right, .. } => self.constant(left) && self.constant(right),
            Expr::In { needle, list } => self.constant(needle) && self.constant(list),
            Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => self.constant(inner),
            _ => false,
        }
    }

    /// A constant written subtree as the IR expression its value is
    /// computed from, for the evaluator.
    fn constant_ir(&self, expr: &Expr) -> Option<IRExpr> {
        Some(match expr {
            Expr::Literal(literal) => IRExpr::Literal(literal.clone()),
            Expr::PropAccess { variable, property } => IRExpr::Literal(Literal::String(
                self.single_edge_type(variable, property)?.to_string(),
            )),
            Expr::Now => IRExpr::Param(NOW_PARAM_NAME.to_string()),
            Expr::Variable(name) if self.params.contains(name.as_str()) => {
                IRExpr::Param(name.clone())
            }
            Expr::Binary { left, op, right } => IRExpr::Binary {
                left: Box::new(self.constant_ir(left)?),
                op: *op,
                right: Box::new(self.constant_ir(right)?),
            },
            Expr::In { needle, list } => IRExpr::Binary {
                left: Box::new(self.constant_ir(list)?),
                op: BinaryOp::Compare(CompOp::Contains),
                right: Box::new(self.constant_ir(needle)?),
            },
            Expr::Not(inner) => IRExpr::Not(Box::new(self.constant_ir(inner)?)),
            Expr::IsNull { expr, negated } => IRExpr::IsNull {
                expr: Box::new(self.constant_ir(expr)?),
                negated: *negated,
            },
            _ => return None,
        })
    }

    /// A written clause split into the conjuncts its lowering tests: the
    /// top-level `and` chain, except that a constant `and` is folded into one
    /// value before the split.
    pub(crate) fn conjuncts<'e>(&self, clause: &'e Expr, out: &mut Vec<&'e Expr>) {
        match clause {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } if !self.constant(clause) => {
                self.conjuncts(left, out);
                self.conjuncts(right, out);
            }
            other => out.push(other),
        }
    }

    /// The value of a constant written expression under the bound
    /// parameters.
    pub(crate) fn value(&self, expr: &Expr) -> Option<Literal> {
        self.constants.evaluate(&self.constant_ir(expr)?)
    }

    /// Whether `ir` is the lowering of `written` at `position`.
    pub(crate) fn matches(
        &self,
        written: &Expr,
        ir: &IRExpr,
        position: Position,
        budget: &mut Budget,
    ) -> Result<bool, ValidationError> {
        budget.visit(1)?;
        if let IRExpr::Literal(value) = ir
            && !matches!(written, Expr::Literal(_))
            && self.constant(written)
        {
            return Ok(self
                .constant_ir(written)
                .and_then(|constant| self.constants.evaluate(&constant))
                .is_some_and(|evaluated| evaluated == *value));
        }
        if position == Position::Return
            && let Some((variable, column)) = written.score_column()
        {
            return Ok(matches!(
                ir,
                IRExpr::PropAccess { variable: v, property: p } if v == variable && p == column
            ));
        }
        Ok(match (written, ir) {
            (Expr::Now, IRExpr::Param(name)) => name == NOW_PARAM_NAME,
            (Expr::Literal(left), IRExpr::Literal(right)) => left == right,
            (Expr::Variable(name), IRExpr::Param(param)) => {
                self.params.contains(name.as_str()) && name == param
            }
            (Expr::Variable(name), IRExpr::Variable(variable)) => {
                !self.params.contains(name.as_str()) && name == variable
            }
            (Expr::AliasRef(left), IRExpr::AliasRef(right)) => left == right,
            (Expr::PropAccess { variable, property }, IRExpr::Literal(Literal::String(name))) => {
                self.single_edge_type(variable, property) == Some(name.as_str())
            }
            (
                Expr::PropAccess { variable, property },
                IRExpr::PropAccess {
                    variable: v,
                    property: p,
                },
            ) => variable == v && self.physical(property) == *p,
            (
                Expr::Binary { left, op, right },
                IRExpr::Binary {
                    left: l,
                    op: o,
                    right: r,
                },
            ) => {
                same_op(*op, *o)
                    && self.matches(left, l, position, budget)?
                    && self.matches(right, r, position, budget)?
            }
            (
                Expr::In { needle, list },
                IRExpr::Binary {
                    left: l,
                    op: o,
                    right: r,
                },
            ) => {
                same_op(BinaryOp::Compare(CompOp::Contains), *o)
                    && self.matches(list, l, position, budget)?
                    && self.matches(needle, r, position, budget)?
            }
            (Expr::Not(inner), IRExpr::Not(i)) => self.matches(inner, i, position, budget)?,
            (
                Expr::IsNull { expr, negated },
                IRExpr::IsNull {
                    expr: e,
                    negated: n,
                },
            ) => negated == n && self.matches(expr, e, position, budget)?,
            (Expr::Aggregate { func, arg }, IRExpr::Aggregate { func: f, arg: a }) => {
                func == f && self.matches(arg, a, Position::Plain, budget)?
            }
            (
                Expr::Nearest {
                    variable,
                    property,
                    query,
                },
                IRExpr::Nearest {
                    variable: v,
                    property: p,
                    query: q,
                },
            ) => {
                variable == v && property == p && self.matches(query, q, Position::Plain, budget)?
            }
            (Expr::Search { field, query }, IRExpr::Search { field: f, query: q })
            | (Expr::MatchText { field, query }, IRExpr::MatchText { field: f, query: q })
            | (Expr::Bm25 { field, query }, IRExpr::Bm25 { field: f, query: q }) => {
                self.matches(field, f, Position::Plain, budget)?
                    && self.matches(query, q, Position::Plain, budget)?
            }
            (
                Expr::Fuzzy {
                    field,
                    query,
                    max_edits,
                },
                IRExpr::Fuzzy {
                    field: f,
                    query: q,
                    max_edits: m,
                },
            ) => {
                self.matches(field, f, Position::Plain, budget)?
                    && self.matches(query, q, Position::Plain, budget)?
                    && match (max_edits, m) {
                        (None, None) => true,
                        (Some(written), Some(ir)) => {
                            self.matches(written, ir, Position::Plain, budget)?
                        }
                        _ => false,
                    }
            }
            (
                Expr::Rrf {
                    primary,
                    secondary,
                    k,
                },
                IRExpr::Rrf {
                    primary: p,
                    secondary: s,
                    k: kk,
                },
            ) => {
                self.matches(primary, p, Position::Plain, budget)?
                    && self.matches(secondary, s, Position::Plain, budget)?
                    && match (k, kk) {
                        (None, None) => true,
                        (Some(written), Some(ir)) => {
                            self.matches(written, ir, Position::Plain, budget)?
                        }
                        _ => false,
                    }
            }
            _ => false,
        })
    }

    /// Whether `ir` carries the written query argument of a retrieval: as
    /// written, or a constant argument as its bound value.
    pub(crate) fn argument(
        &self,
        written: &Expr,
        ir: &IRExpr,
        budget: &mut Budget,
    ) -> Result<bool, ValidationError> {
        self.matches(written, ir, Position::Plain, budget)
    }
}

/// `op` as the lowering spells it: `contains` over a String left operand
/// becomes `StringContains`; every other operator is kept.
fn same_op(written: BinaryOp, ir: BinaryOp) -> bool {
    written == ir
        || written == BinaryOp::Compare(CompOp::Contains)
            && ir == BinaryOp::Compare(CompOp::StringContains)
}

/// The column a projected rank expression lowers to, as `(binding, column)`.
pub(crate) fn score_column(kind: RankKind) -> &'static str {
    match kind {
        RankKind::Nearest => DISTANCE_COLUMN,
        RankKind::Bm25 => SCORE_COLUMN,
    }
}

/// The `return` item an order key names by its result column, as the
/// planner binds a non-property key: the alias, else the expression's own
/// column.
pub(crate) fn result_column(projection: &IRProjection) -> Option<String> {
    fn column(expr: &IRExpr) -> Option<String> {
        match expr {
            IRExpr::PropAccess { variable, property } => Some(format!("{variable}.{property}")),
            IRExpr::Variable(name) | IRExpr::Param(name) => Some(name.clone()),
            IRExpr::Literal(_) => Some("literal".to_string()),
            IRExpr::Aggregate { arg, .. } => column(arg),
            _ => None,
        }
    }
    projection
        .alias
        .clone()
        .or_else(|| column(&projection.expr))
}

/// Every written return item that projects a rank call, at its root or in
/// the Boolean structure around it, with the call.
pub(crate) fn projected_rank_calls(returns: &[Projection]) -> Vec<(usize, &Expr)> {
    fn calls<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
        match expr {
            Expr::Binary { left, right, .. } => {
                calls(left, out);
                calls(right, out);
            }
            Expr::In { needle, list } => {
                calls(needle, out);
                calls(list, out);
            }
            Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => calls(inner, out),
            call if call.score_column().is_some() => out.push(call),
            _ => {}
        }
    }
    let mut out = Vec::new();
    for (index, projection) in returns.iter().enumerate() {
        let mut found = Vec::new();
        calls(&projection.expr, &mut found);
        out.extend(found.into_iter().map(|call| (index, call)));
    }
    out
}
