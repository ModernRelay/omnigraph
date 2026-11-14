//! What a checked read requires of every plan for it (RFC 0047,
//! "Requirements come from the checked query"). The checked read is the
//! core term; its shape (`AcceptInput::ir`, a [`crate::read_shape::ReadShape`]
//! with its filter constants folded under the bound values) states each
//! expression in the form a plan carries it: every binding and its type,
//! conjunct, traversal, block predicate, return item, order key and the
//! limit. A split return reads aggregate leaves by alias after grouping;
//! acceptance checks their operands against the shape, with explicit casts
//! and types intact. A return item that reads the leading ranking's score
//! column projects that ranking.

use std::collections::BTreeMap;

use crate::read_shape::PatternStep;
use omnigraph_compiler::ir::is_fresh_variable;
use omnigraph_compiler::ir::{IRExpr, IROrdering, IRProjection, SubqueryPredicate};
use omnigraph_compiler::query::value::{DISTANCE_COLUMN, Literal, RRF_COLUMN, SCORE_COLUMN};
use omnigraph_compiler::traversal::EdgeSelection;
use omnigraph_compiler::types::ExprType;

use super::budget::Budget;
use super::{AcceptInput, ValidationError};
use crate::physical::RankKind;

/// The canonical id of a node keyed by one String property, as every write
/// path derives it from the key's value. The engine implements it with the
/// loader's derivation, so a key narrowing the planner adds (an `@id`
/// equality beside the written key equality) is re-derived, not trusted.
pub trait KeyIds: Sync {
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

/// What a variable of the top-level scope binds.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BoundVariable {
    Node { type_name: String },
    Edge { type_names: Vec<String> },
}

/// The search a leading `order` key names.
#[derive(Debug, Clone)]
pub(crate) enum Search {
    Rank(Retrieval),
    Fuse {
        arms: Box<[Retrieval; 2]>,
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
    /// Written after a ranking drawn as a pattern step, over its selected
    /// rows.
    pub after_rank: bool,
}

/// An `optional { }` of the top-level scope: its correlation binding and
/// the eligibility conjuncts of its own pattern, which the plan tests inside
/// its inner tree and nowhere after it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OptionalMatch {
    pub outer_var: String,
    pub eligibility: Vec<IRExpr>,
}

/// A `from` over an inline read with its own group or cut: its correlation
/// binding, what it returns, and the eligibility conjuncts of its own
/// pattern, which the plan tests inside its inner tree, under its group and
/// cut, and nowhere after it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CorrelateMatch {
    pub outer_var: Option<String>,
    pub read: crate::read_shape::InnerRead,
    pub eligibility: Vec<IRExpr>,
}

/// The eligibility conjuncts of a nested scope's own steps.
fn own_eligibility(inner: &[PatternStep]) -> Vec<IRExpr> {
    let mut eligibility = Vec::new();
    for own in inner {
        match own {
            PatternStep::NodeScan { filters, .. }
            | PatternStep::Expand {
                dst_filters: filters,
                ..
            } => push_conjuncts(&mut eligibility, filters),
            PatternStep::Filter(filter) => {
                push_conjuncts(&mut eligibility, std::slice::from_ref(filter))
            }
            PatternStep::Rank { .. }
            | PatternStep::Score { .. }
            | PatternStep::Let { .. }
            | PatternStep::AntiJoin { .. }
            | PatternStep::Optional { .. }
            | PatternStep::Union { .. }
            | PatternStep::Correlate { .. } => {}
        }
    }
    eligibility
}

/// The steps of `steps` with each `optional`'s own steps in its place: the
/// names an `optional` introduces are the scope's, so its bindings,
/// traversals, conjuncts and blocks are the scope's requirements too, and
/// `optionals` records where its conjuncts must run.
fn flatten<'p>(
    steps: &'p [PatternStep],
    out: &mut Vec<&'p PatternStep>,
    optionals: &mut Vec<OptionalMatch>,
    unions: &mut Vec<Vec<String>>,
    correlates: &mut Vec<CorrelateMatch>,
) {
    for step in steps {
        match step {
            // A union's arms each bind its names: their bindings,
            // traversals, conjuncts and blocks are the scope's, and the plan
            // folds the arms with one `Union` per arm after the first.
            PatternStep::Union { arms, bindings, .. } => {
                for _ in 1..arms.len() {
                    unions.push(bindings.clone());
                }
                for arm in arms {
                    flatten(arm, out, optionals, unions, correlates);
                }
            }
            // A correlated read's pattern binds its names and runs its
            // traversals, conjuncts and blocks inside its inner tree.
            PatternStep::Correlate {
                outer_var,
                inner,
                read,
            } => {
                correlates.push(CorrelateMatch {
                    outer_var: outer_var.clone(),
                    read: read.clone(),
                    eligibility: own_eligibility(inner),
                });
                flatten(inner, out, optionals, unions, correlates);
            }
            PatternStep::Optional { outer_var, inner } => {
                let eligibility = own_eligibility(inner);
                optionals.push(OptionalMatch {
                    outer_var: outer_var.clone(),
                    eligibility,
                });
                flatten(inner, out, optionals, unions, correlates);
            }
            other => out.push(other),
        }
    }
}

/// One column of the read's rows as the tie rule orders it, in column
/// order (see `optimizer::tie_rule`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TieColumn {
    /// A reference: its `@id`, after its `@type` when it is a selected edge.
    Identity { binding: String, selected: bool },
    /// A value column of a correlated read or a union, `binding.column`.
    Value { binding: String, column: String },
}

impl TieColumn {
    fn binding(&self) -> &str {
        match self {
            Self::Identity { binding, .. } | Self::Value { binding, .. } => binding,
        }
    }

    /// A reference the query names, which leads the rule.
    pub(crate) fn is_named(&self) -> bool {
        matches!(self, Self::Identity { binding, .. } if !is_fresh_variable(binding))
    }
}

/// The columns `steps` add to the rows, in the order the pattern introduces
/// them, each once: a block adds none, an `optional` the names its pattern
/// binds, a correlated read only what it returns, a union its first arm's
/// reference and value columns.
pub(crate) fn tie_columns(steps: &[PatternStep], out: &mut Vec<TieColumn>) {
    fn push(out: &mut Vec<TieColumn>, column: TieColumn) {
        if !out.contains(&column) {
            out.push(column);
        }
    }
    for step in steps {
        match step {
            PatternStep::NodeScan { variable, .. } => push(
                out,
                TieColumn::Identity {
                    binding: variable.clone(),
                    selected: false,
                },
            ),
            PatternStep::Expand {
                dst_var,
                edges,
                edge_binding,
                ..
            } => {
                push(
                    out,
                    TieColumn::Identity {
                        binding: dst_var.clone(),
                        selected: false,
                    },
                );
                if let Some(edge) = edge_binding {
                    push(
                        out,
                        TieColumn::Identity {
                            binding: edge.clone(),
                            selected: edges.named().is_none(),
                        },
                    );
                }
            }
            PatternStep::Optional { inner, .. } => tie_columns(inner, out),
            PatternStep::Correlate { inner, read, .. } => {
                let mut own = Vec::new();
                tie_columns(inner, &mut own);
                for column in &read.columns {
                    match &column.expr {
                        IRExpr::Variable(name, _) => {
                            if let Some(entry) = own.iter().find(|entry| entry.binding() == name) {
                                push(out, entry.clone());
                            }
                        }
                        _ => push(
                            out,
                            TieColumn::Value {
                                binding: read.values.clone(),
                                column: column.column.clone(),
                            },
                        ),
                    }
                }
            }
            PatternStep::Union { arms, bindings, .. } => {
                let mut arm = Vec::new();
                if let Some(first) = arms.first() {
                    tie_columns(first, &mut arm);
                }
                for entry in arm {
                    let leaves = match &entry {
                        TieColumn::Identity { binding, .. } => bindings.contains(binding),
                        TieColumn::Value { .. } => true,
                    };
                    if leaves {
                        push(out, entry);
                    }
                }
            }
            PatternStep::Filter(_)
            | PatternStep::Rank { .. }
            | PatternStep::Score { .. }
            | PatternStep::Let { .. }
            | PatternStep::AntiJoin { .. } => {}
        }
    }
}

/// What every plan for one checked declaration must preserve.
#[derive(Debug, Clone)]
pub struct Requirements {
    /// The named bindings of the top-level scope and their types.
    pub(crate) bindings: BTreeMap<String, BoundVariable>,
    /// The columns of the top-level rows in column order, for the tie rule.
    pub(crate) tie_columns: Vec<TieColumn>,
    /// Traversals of the top-level scope, in written order.
    pub(crate) traversals: Vec<Traversal>,
    /// Correlated blocks of the top-level scope, in written order.
    pub(crate) blocks: Vec<Block>,
    /// The `optional`s of the top-level scope, nested ones included.
    pub(crate) optionals: Vec<OptionalMatch>,
    /// The correlated reads of the top-level scope, nested ones included,
    /// each one `Correlate` node of the plan.
    pub(crate) correlates: Vec<CorrelateMatch>,
    /// The names every `Union` node of the plan binds: one entry per arm
    /// after the first of each written `union`.
    pub(crate) unions: Vec<Vec<String>>,
    /// Every `distance` of the query: the scored binding, its vector
    /// property and the query, each one `Score` node of the plan.
    pub(crate) scores: Vec<(String, String, IRExpr)>,
    /// The value of every `let` that can fail, each one `Let` node of the
    /// plan.
    pub(crate) lets: Vec<IRExpr>,
    pub(crate) search: Option<Search>,
    /// Every eligibility conjunct of the top-level scope (inline matches,
    /// `match` filters and the filters of a deferred traversal destination),
    /// split at its top-level `and`; a constant `true` constrains nothing and
    /// is left out.
    pub(crate) eligibility: Vec<IRExpr>,
    /// The conjuncts of `eligibility` written after a ranking drawn as a
    /// pattern step, which run over its selected rows.
    pub(crate) after_rank: Vec<IRExpr>,
    pub(crate) returns: Vec<IRProjection>,
    /// The return items that read the leading ranking's score column, by
    /// position, with that ranking.
    pub(crate) projected_ranks: Vec<(usize, IRExpr)>,
    /// The leading `order` key, when it is a search.
    pub(crate) written_search: Option<IRExpr>,
    /// The `order` keys after a leading search key that order something (a
    /// literal key does not), each bound to a `return` item as the planner
    /// binds it.
    pub(crate) order: Vec<IROrdering>,
    /// Whether the query writes an `order` clause at all: one whose every
    /// key is a constant still orders its rows by the identity tie-break.
    pub(crate) ordered: bool,
    pub(crate) limit: Option<u64>,
    /// Whether the rows are groups: an aggregate return or `return distinct`.
    pub(crate) aggregate: bool,
    /// A ranking drawn as a pattern step (`PatternStep::Rank`) rather than
    /// led by the `order`: its `nearest` window `k` in targets, `None` for
    /// one with no window (`bm25`). Its score leads no sort, and the
    /// eligibility conjuncts after it run over the selected rows.
    pub(crate) window: Option<Option<u64>>,
}

impl Requirements {
    pub(crate) fn derive(
        input: &AcceptInput<'_>,
        budget: &mut Budget,
    ) -> Result<Self, ValidationError> {
        let ir = input.ir;
        let mut eligibility = Vec::new();
        let mut traversals = Vec::new();
        let mut blocks = Vec::new();
        let mut bindings: BTreeMap<String, BoundVariable> = BTreeMap::new();
        let mut rank_step: Option<(&IRExpr, Option<u64>)> = None;
        let mut rank_at: Option<usize> = None;
        let mut steps = Vec::new();
        let mut optionals = Vec::new();
        let mut unions = Vec::new();
        let mut scores = Vec::new();
        let mut lets = Vec::new();
        let mut correlates = Vec::new();
        flatten(
            &ir.pipeline,
            &mut steps,
            &mut optionals,
            &mut unions,
            &mut correlates,
        );
        for op in steps {
            budget.visit(1)?;
            match op {
                PatternStep::Score {
                    binding,
                    property,
                    query,
                    ..
                } => scores.push((binding.clone(), property.clone(), query.clone())),
                PatternStep::Let { value, .. } => lets.push(value.clone()),
                PatternStep::Rank { key, window, .. } => {
                    rank_step = Some((key, *window));
                    rank_at = Some(eligibility.len());
                }
                PatternStep::NodeScan {
                    variable,
                    type_name,
                    filters,
                } => {
                    bindings.insert(
                        variable.clone(),
                        BoundVariable::Node {
                            type_name: type_name.clone(),
                        },
                    );
                    push_conjuncts(&mut eligibility, filters);
                }
                PatternStep::Filter(filter) => {
                    push_conjuncts(&mut eligibility, std::slice::from_ref(filter))
                }
                PatternStep::Expand {
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
                    if !is_fresh_variable(dst_var) {
                        bindings.insert(
                            dst_var.clone(),
                            BoundVariable::Node {
                                type_name: dst_type.clone(),
                            },
                        );
                    }
                    if let Some(edge) = edge_binding {
                        let mut type_names: Vec<String> = edges
                            .members()
                            .iter()
                            .map(|m| m.edge_type.clone())
                            .collect();
                        type_names.dedup();
                        bindings.insert(edge.clone(), BoundVariable::Edge { type_names });
                    }
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
                PatternStep::AntiJoin {
                    outer_var,
                    predicate,
                    ..
                } => blocks.push(Block {
                    outer_var: outer_var.clone(),
                    predicate: predicate.clone(),
                    after_rank: rank_at.is_some(),
                }),
                PatternStep::Optional { .. }
                | PatternStep::Union { .. }
                | PatternStep::Correlate { .. } => {
                    unreachable!(
                        "flatten puts an optional's, a union's and a correlated read's steps in their place"
                    )
                }
            }
        }
        let mut keys = ir.order_by.as_slice();
        let leading = match rank_step {
            Some((key, _)) => Some(key),
            None => keys.first().map(|key| &key.expr),
        };
        let search = match leading {
            Some(IRExpr::Rrf {
                primary,
                secondary,
                k,
                ..
            }) => match (Retrieval::of(primary), Retrieval::of(secondary)) {
                (Some(primary), Some(secondary)) => Some(Search::Fuse {
                    arms: Box::new([primary, secondary]),
                    k: k.as_deref().cloned(),
                }),
                _ => None,
            },
            Some(expr) => Retrieval::of(expr).map(Search::Rank),
            None => None,
        };
        if search.is_some() && rank_step.is_none() {
            keys = &keys[1..];
        }
        // Group rows (aggregates or `return distinct`) bind every key to the
        // return item it names, as the planner does, before constants drop.
        let grouped = ir.distinct
            || ir
                .return_exprs
                .iter()
                .any(|projection| crate::optimizer::contains_aggregate(&projection.expr));
        let order = keys
            .iter()
            .map(|key| IROrdering {
                expr: crate::optimizer::bind_order_key(&key.expr, &ir.return_exprs, grouped),
                descending: key.descending,
            })
            .filter(|key| !matches!(key.expr, IRExpr::Literal(_, _)))
            .collect();
        let written_search = search.as_ref().and(leading).cloned();
        let projected_ranks = match &written_search {
            Some(lead) => ir
                .return_exprs
                .iter()
                .enumerate()
                .filter(|(_, projection)| reads_score(&projection.expr))
                .map(|(index, _)| (index, lead.clone()))
                .collect(),
            None => Vec::new(),
        };
        let mut columns = Vec::new();
        tie_columns(&ir.pipeline, &mut columns);
        Ok(Self {
            bindings,
            tie_columns: columns,
            traversals,
            blocks,
            optionals,
            correlates,
            unions,
            scores,
            lets,
            search,
            after_rank: rank_at.map_or_else(Vec::new, |at| eligibility[at..].to_vec()),
            eligibility,
            returns: ir.return_exprs.clone(),
            projected_ranks,
            written_search,
            order,
            ordered: !ir.order_by.is_empty(),
            limit: ir.limit,
            aggregate: grouped,
            window: rank_step.map(|(_, window)| window),
        })
    }
}

/// Whether `derived` is the identity equality the key narrowing adds to a
/// scan of `type_key` bound to `binding` whose conjuncts include `written`:
/// the binding's id column equal, as a non-null String, to the id the
/// loader derives from the value of a written equality on the type's single
/// String key. The derivation goes through `AcceptInput::keys`, so a
/// narrowed id is re-derived, never trusted.
pub(crate) fn narrowed_identity<'w>(
    input: &AcceptInput<'_>,
    type_key: &str,
    binding: &str,
    written: impl IntoIterator<Item = &'w IRExpr>,
    derived: &IRExpr,
) -> bool {
    let narrowed_type = ExprType::Value {
        scalar: omnigraph_compiler::types::ScalarType::String,
        list: false,
        nullable: false,
    };
    let Some((
        IRExpr::PropAccess {
            variable,
            property,
            ty,
        },
        omnigraph_compiler::query::value::CompOp::Eq,
        IRExpr::Literal(Literal::String(id), literal_ty),
    )) = derived.comparison_parts()
    else {
        return false;
    };
    if variable != binding
        || property != input.catalog.system_columns.id
        || *ty != narrowed_type
        || *literal_ty != narrowed_type
    {
        return false;
    }
    let Some([key]) = type_key
        .strip_prefix("node:")
        .and_then(|name| input.catalog.node_types.get(name))
        .and_then(|node| node.key.as_deref())
    else {
        return false;
    };
    written.into_iter().any(|conjunct| {
        let Some((left, omnigraph_compiler::query::value::CompOp::Eq, right)) =
            conjunct.comparison_parts()
        else {
            return false;
        };
        [(left, right), (right, left)]
            .into_iter()
            .any(|(side, value)| {
                let keyed = matches!(side, IRExpr::PropAccess { variable, property, .. }
                if variable == binding && property == key);
                let literal = match value {
                    IRExpr::Literal(literal, _) => Some(literal),
                    IRExpr::Param(name, _) => input.params.get(name),
                    _ => None,
                };
                keyed
                    && literal.is_some_and(|literal| {
                        input
                            .keys
                            .canonical_key_id(type_key, literal, value.ty())
                            .as_deref()
                            == Some(id.as_str())
                    })
            })
    })
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

/// Whether a return item reads a retrieval's score column, at its root or
/// in the Boolean structure around it.
pub(crate) fn reads_score(expr: &IRExpr) -> bool {
    let mut found = false;
    let mut pending = vec![expr];
    while let Some(e) = pending.pop() {
        match e {
            IRExpr::PropAccess { property, .. }
                if [DISTANCE_COLUMN, SCORE_COLUMN, RRF_COLUMN].contains(&property.as_str()) =>
            {
                found = true
            }
            IRExpr::Binary { left, right, .. } => pending.extend([left.as_ref(), right.as_ref()]),
            IRExpr::Not(inner, _)
            | IRExpr::IsNull { expr: inner, .. }
            | IRExpr::Cast { expr: inner, .. } => pending.push(inner),
            _ => {}
        }
    }
    found
}
