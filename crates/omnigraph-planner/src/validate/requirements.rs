//! What a checked read declaration requires of every plan for it (RFC 0047,
//! "Requirements come from the checked query"). The declaration fixes what
//! the query asks for: its bindings and their types, how many traversals,
//! correlated blocks, return items and order keys it writes, its limit, and
//! which rank call a projected score names. The compiler's typed lowering of
//! that declaration (`AcceptInput::ir`, its filter constants folded under the
//! bound values) states each expression in the form a plan carries it: every
//! conjunct, traversal, block predicate, return item and order key a plan
//! places is a verbatim copy of an IR expression, so plan and requirement
//! compare as typed IR, explicit casts and exact-integer carriers included.
//! The declaration is consulted only where the lowering erases meaning: a
//! projected rank call becomes its score column.

use std::collections::{BTreeMap, HashSet};

use omnigraph_compiler::ir::{IRExpr, IROp, IROrdering, IRProjection, SubqueryPredicate};
use omnigraph_compiler::query::ast::{Clause, Expr, Literal, Projection};
use omnigraph_compiler::query::typecheck::BoundVariable;
use omnigraph_compiler::traversal::EdgeSelection;
use omnigraph_compiler::types::ExprType;

use super::budget::Budget;
use super::{AcceptInput, ValidationError};
use crate::physical::RankKind;

/// The canonical id of a node keyed by one String property, as every write
/// path derives it from the key's value. The engine implements it with the
/// loader's derivation, so a key narrowing the planner adds (an `@id`
/// equality beside the written key equality) is re-derived, not trusted.
pub trait KeyIds {
    /// The id of the node of `type_key` whose key is `value` (typed `ty`), or
    /// `None` when the type is not keyed by one String property.
    fn canonical_key_id(&self, type_key: &str, value: &Literal, ty: &ExprType) -> Option<String>;
}

/// One retrieval a search order names: `nearest($v.p, q)` or `bm25($v.p, q)`,
/// with the query argument as the plan's ranked scan carries it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Retrieval {
    pub kind: RankKind,
    pub binding: String,
    pub property: String,
    pub query: IRExpr,
}

impl Retrieval {
    pub(crate) fn of(expr: &IRExpr) -> Option<Self> {
        match expr {
            IRExpr::Nearest {
                variable,
                property,
                query,
                ..
            } => Some(Self {
                kind: RankKind::Nearest,
                binding: variable.clone(),
                property: property.clone(),
                query: query.as_ref().clone(),
            }),
            IRExpr::Bm25 { field, query, .. } => match field.as_ref() {
                IRExpr::PropAccess {
                    variable, property, ..
                } => Some(Self {
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
        k: Option<IRExpr>,
    },
}

/// One traversal of the top-level scope, as the IR states it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Traversal {
    pub src: String,
    pub dst: String,
    pub edges: EdgeSelection,
    pub src_type: String,
    pub dst_type: String,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
    pub edge_binding: Option<String>,
}

/// A correlated block of the top-level scope: its outer binding and the
/// predicate over the aggregate of its matches.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Block {
    pub outer_var: String,
    pub predicate: SubqueryPredicate,
}

/// What every plan for one checked declaration must preserve.
#[derive(Debug, Clone)]
pub struct Requirements {
    /// The named bindings of the top-level scope and their types.
    pub(crate) bindings: BTreeMap<String, BoundVariable>,
    /// Edge bindings of a traversal over more than one edge type, whose
    /// identity is the pair (`@type`, `@id`).
    pub(crate) selected_edges: HashSet<String>,
    /// Traversals of the top-level scope, in written order.
    pub(crate) traversals: Vec<Traversal>,
    /// Correlated blocks of the top-level scope, in written order.
    pub(crate) blocks: Vec<Block>,
    pub(crate) search: Option<Search>,
    /// Every eligibility conjunct of the top-level scope (inline matches,
    /// `match` filters and the filters of a deferred traversal destination),
    /// split at its top-level `and`; a constant `true` constrains nothing and
    /// is left out.
    pub(crate) eligibility: Vec<IRExpr>,
    pub(crate) returns: Vec<IRProjection>,
    /// The written return items that project a rank call, by position, with
    /// the call as written.
    pub(crate) projected_ranks: Vec<(usize, Expr)>,
    /// The leading `order` key as written, when it is a search.
    pub(crate) written_search: Option<Expr>,
    /// The `order` keys after a leading search key that order something (a
    /// literal key does not), each bound to a `return` item as the planner
    /// binds it.
    pub(crate) order: Vec<IROrdering>,
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
        let ir = input.ir;
        let mut eligibility = Vec::new();
        let mut traversals = Vec::new();
        let mut blocks = Vec::new();
        for op in &ir.pipeline {
            budget.visit(1)?;
            match op {
                IROp::NodeScan {
                    variable,
                    type_name,
                    filters,
                } => {
                    match types.bindings.get(variable) {
                        Some(BoundVariable::Node {
                            type_name: declared,
                        }) if declared == type_name => {}
                        _ => {
                            return Err(ValidationError::violated(
                                "binding identity",
                                format!(
                                    "the lowering scans `${variable}: {type_name}`, which the query does not bind"
                                ),
                            ));
                        }
                    }
                    push_conjuncts(&mut eligibility, filters);
                }
                IROp::Filter(filter) => {
                    push_conjuncts(&mut eligibility, std::slice::from_ref(filter))
                }
                IROp::Expand {
                    src_var,
                    dst_var,
                    edges,
                    src_type,
                    dst_type,
                    min_hops,
                    max_hops,
                    dst_filters,
                    edge_binding,
                } => {
                    traversals.push(Traversal {
                        src: src_var.clone(),
                        dst: dst_var.clone(),
                        edges: edges.clone(),
                        src_type: src_type.clone(),
                        dst_type: dst_type.clone(),
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        edge_binding: edge_binding.clone(),
                    });
                    push_conjuncts(&mut eligibility, dst_filters);
                }
                IROp::AntiJoin {
                    outer_var,
                    predicate,
                    ..
                } => blocks.push(Block {
                    outer_var: outer_var.clone(),
                    predicate: predicate.clone(),
                }),
            }
        }
        // The lowering states each expression; the declaration fixes how many
        // of each the query asks for.
        let written_blocks = decl
            .match_clause
            .iter()
            .filter(|clause| matches!(clause, Clause::Subquery(_)))
            .count();
        let counts = [
            ("traversals", types.traversals.len(), traversals.len()),
            ("correlated blocks", written_blocks, blocks.len()),
            (
                "return items",
                decl.return_clause.len(),
                ir.return_exprs.len(),
            ),
            ("order keys", decl.order_clause.len(), ir.order_by.len()),
        ];
        for (what, written, lowered) in counts {
            if written != lowered {
                return Err(ValidationError::violated(
                    "lowering",
                    format!("the query writes {written} {what}; its lowering holds {lowered}"),
                ));
            }
        }
        if ir.limit != decl.limit {
            return Err(ValidationError::violated(
                "lowering",
                format!(
                    "the query's `limit` is {:?}; its lowering's is {:?}",
                    decl.limit, ir.limit
                ),
            ));
        }
        let mut selected_edges = HashSet::new();
        for traversal in &types.traversals {
            budget.visit(1)?;
            if let Some(edge) = traversal
                .edge_binding
                .as_deref()
                .filter(|edge| *edge != "_")
                && traversal.edges.named().is_none()
            {
                selected_edges.insert(edge.to_string());
            }
        }
        let mut keys = ir.order_by.as_slice();
        let search = match keys.first().map(|key| &key.expr) {
            Some(IRExpr::Rrf {
                primary,
                secondary,
                k,
                ..
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
            keys = &keys[1..];
        }
        let order = keys
            .iter()
            .filter(|key| !matches!(key.expr, IRExpr::Literal(_, _)))
            .map(|key| IROrdering {
                expr: crate::optimizer::bind_order_key(&key.expr, &ir.return_exprs),
                descending: key.descending,
            })
            .collect();
        let written_search = search
            .as_ref()
            .and(decl.order_clause.first())
            .map(|key| key.expr.clone());
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
            returns: ir.return_exprs.clone(),
            projected_ranks: projected_rank_calls(&decl.return_clause)
                .into_iter()
                .map(|(index, call)| (index, call.clone()))
                .collect(),
            written_search,
            order,
            limit: ir.limit,
            aggregate: ir
                .return_exprs
                .iter()
                .any(|projection| matches!(projection.expr, IRExpr::Aggregate { .. })),
        })
    }
}

/// `filters` split at their top-level `and`, a constant `true` left out.
fn push_conjuncts(out: &mut Vec<IRExpr>, filters: &[IRExpr]) {
    for conjunct in filters.iter().cloned().flat_map(IRExpr::into_conjuncts) {
        if !constant_true(&conjunct) {
            out.push(conjunct);
        }
    }
}

/// Whether a conjunct is the literal `true`, which constrains nothing.
pub(crate) fn constant_true(conjunct: &IRExpr) -> bool {
    matches!(conjunct, IRExpr::Literal(Literal::Bool(true), _))
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
