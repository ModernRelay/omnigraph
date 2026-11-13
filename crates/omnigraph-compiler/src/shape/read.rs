//! A core read as the planner lowers it (docs/rfcs/2026-11-07-gq-3.md,
//! section 11, "One IR").
//!
//! The type checker types one core term per read ([`CoreRead`]). [`ReadShape::of`] decomposes it into what the logical
//! plan is built from: the pattern's scans, expansions, filters and
//! correlated blocks in the order they run, then the return items, the order
//! keys and the limit. A `from` over a ranking (`nearest`, `bm25`, `rrf`)
//! becomes the leading rank key the ranked scan executes, the pattern under
//! it the scan's prefilter. The shape is derived per plan and never stored.
//!
//! The pattern's facts (bindings with their filters, triples, filter
//! clauses, blocks) are recovered from the term, then placed: the first
//! binding of each connected component scans and the others are introduced
//! by the expansion that reaches them, their filters going with it; a triple
//! whose ends are both bound expands to a fresh name and filters on
//! identity; the filter clauses follow, then the blocks. A core node the
//! planner has no plan node for, or a shape outside this table, is
//! [`Unlowered`]: the type checker refuses it (`T86`) at the construct the
//! refusal names.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::catalog::Catalog;
use crate::ir::core::{
    ANON_PREFIX, Agg, Col, CoreNode, CoreRead, Count, FRESH_PREFIX, LexicalQuery, RRF_DEFAULT_K,
    ScoreKind, ScoreQuery, Term, Ty,
};
use crate::ir::{BlockAggregateExpr, IRExpr, IROrdering, IRParam, IRProjection, SubqueryPredicate};
use crate::query::file::Param;
use crate::query::value::{
    AggFunc, BinaryOp, CompOp, DISTANCE_COLUMN, Literal, RRF_COLUMN, SCORE_COLUMN,
};
use crate::traversal::EdgeSelection;
use crate::types::{AggSignature, ExprType, PropType, ScalarType};

use super::{Construct, ElementKind, ProgrammeStage, Repair, ShapeError, Unlowered};

/// A core read's top-level shape: the pattern's steps in run order, then
/// what the read returns.
#[derive(Debug, Clone)]
pub struct ReadShape {
    pub name: String,
    /// The declared parameters, every one of them, in declared order.
    pub params: Vec<IRParam>,
    pub pipeline: Vec<PatternStep>,
    pub return_exprs: Vec<IRProjection>,
    /// `return distinct`: one row per distinct projected tuple.
    pub distinct: bool,
    pub order_by: Vec<IROrdering>,
    pub limit: Option<u64>,
}

/// One step of a pattern.
#[derive(Debug, Clone)]
pub enum PatternStep {
    NodeScan {
        variable: String,
        type_name: String,
        /// The binding's filters over `variable`.
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
        /// The filters of a binding the expansion introduces, applied as it
        /// hydrates the destination.
        dst_filters: Vec<IRExpr>,
        /// The edge variable: one output row per matching edge, its
        /// properties carried under this prefix.
        edge_binding: Option<String>,
    },
    Filter(IRExpr),
    /// A ranking drawn over the rows of the steps before it, its
    /// population, when it is not the read's leading `order` key cut by the
    /// read's `limit` (that ranking is `order_by`'s first key): `key` is the
    /// ranking as a leading key holds it (`nearest`, `bm25`, `rrf`),
    /// `binding` the ranked node column, and `window` the `nearest` window
    /// `k` in targets (an `rrf`'s `nearest` arm's), `None` for `bm25`. The
    /// steps after it run over the selected rows, which nothing refills.
    Rank {
        key: IRExpr,
        binding: String,
        window: Option<u64>,
    },
    /// A correlated block: `inner` runs over the outer rows and `predicate`
    /// decides per outer row on the aggregate of its matches.
    AntiJoin {
        outer_var: String,
        inner: Vec<PatternStep>,
        predicate: SubqueryPredicate,
    },
}

impl ReadShape {
    pub fn has_edge_selections(&self) -> bool {
        self.any_edge_selection(|edges| edges.named().is_none())
    }

    pub fn has_wildcard_traversal(&self) -> bool {
        self.any_edge_selection(EdgeSelection::is_wildcard)
    }

    fn any_edge_selection(&self, predicate: impl Fn(&EdgeSelection) -> bool) -> bool {
        let mut pending: Vec<_> = self.pipeline.iter().collect();
        while let Some(step) = pending.pop() {
            match step {
                PatternStep::Expand { edges, .. } => {
                    if predicate(edges) {
                        return true;
                    }
                }
                PatternStep::AntiJoin { inner, .. } => pending.extend(inner),
                PatternStep::NodeScan { .. }
                | PatternStep::Filter(_)
                | PatternStep::Rank { .. } => {}
            }
        }
        false
    }

    /// The shape of `read`, or the term this release's planner does not
    /// lower.
    pub fn of(read: &CoreRead, catalog: &Catalog) -> Result<Self, ShapeError> {
        Shaper::new(catalog, &read.name, &read.params).read(read)
    }

    /// The shape of the read declared as `name` in `source`, checked against
    /// `catalog`; the refusal of any stage as text.
    pub fn of_source(catalog: &Catalog, source: &str, name: &str) -> Result<Self, String> {
        let decl = crate::find_named_query(source, name).map_err(|e| e.to_string())?;
        match crate::query::check::check_query_decl(catalog, &decl).map_err(|e| e.to_string())? {
            crate::ir::core::CoreDecl::Read(read) => {
                Self::of(&read, catalog).map_err(|e| e.to_string())
            }
            crate::ir::core::CoreDecl::Write(_) => Err(format!("`{name}` is a write, not a read")),
        }
    }
}

/// A term of the break that this release does not lower, at `construct`.
fn unsupported(name: &str, what: impl std::fmt::Display, construct: Construct) -> ShapeError {
    ShapeError::unlowered(Unlowered::new(name, what, construct))
}

/// A `from` element.
const FROM: &[ElementKind] = &[ElementKind::From];

/// The ranking a read's `from` draws: the leading rank key it becomes, and
/// the score column the read names.
struct Ranking {
    key: IRExpr,
    /// The column the read reads the score through (`distance`, `score`).
    out: String,
    /// The ranked node column.
    column: String,
    /// The score column the retrieval appends (`_distance`, `_score`,
    /// `_rrf`) and its type.
    score: (&'static str, ExprType),
    descending: bool,
    /// The `k` of each `nearest` window the ranking draws (its own, or its
    /// `rrf` arms'); `None` for a window read from a parameter.
    windows: Vec<Option<u64>>,
}

/// A body split at the ranking it draws.
struct Split<'t> {
    /// The pattern the ranking ranks (the whole body when it draws none).
    population: &'t Term,
    ranking: Option<Ranking>,
    /// The ranking's own node (`ApproxTopK`, a `bm25` `Score`); `None` for a
    /// fusion, whose population is the type.
    node: Option<&'t Term>,
    /// The elements and `where` after the ranking, over an empty leaf.
    after: Option<Term>,
}

/// The population term a ranking node ranks.
fn ranked_input(node: &Term) -> &Term {
    match node {
        Term::ApproxTopK { input, .. } | Term::Score { input, .. } => input,
        other => other,
    }
}

/// `term` with the node `leaf` (found by identity on its input chain)
/// replaced by an empty leaf.
fn without_leaf(term: &Term, leaf: &Term) -> Term {
    if std::ptr::eq(term, leaf) {
        return Term::Env(Vec::new());
    }
    match term {
        Term::Filter { input, pred } => Term::Filter {
            input: without_leaf(input, leaf).boxed(),
            pred: pred.clone(),
        },
        Term::Expand {
            input,
            src,
            dst,
            src_type,
            dst_type,
            edges,
            min_hops,
            max_hops,
            edge_var,
        } => Term::Expand {
            input: without_leaf(input, leaf).boxed(),
            src: src.clone(),
            dst: dst.clone(),
            src_type: src_type.clone(),
            dst_type: dst_type.clone(),
            edges: edges.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            edge_var: edge_var.clone(),
        },
        Term::Project { input, items, keep } => Term::Project {
            input: without_leaf(input, leaf).boxed(),
            items: items.clone(),
            keep: *keep,
        },
        Term::Correlate { outer, inner } => Term::Correlate {
            outer: without_leaf(outer, leaf).boxed(),
            inner: inner.clone(),
        },
        other => other.clone(),
    }
}

/// The names a placed pipeline binds physically: its scans, expansion
/// destinations and edge bindings.
fn physical_names(pipeline: &[PatternStep]) -> HashSet<String> {
    let mut names = HashSet::new();
    for step in pipeline {
        match step {
            PatternStep::NodeScan { variable, .. } => {
                names.insert(variable.clone());
            }
            PatternStep::Expand {
                dst_var,
                edge_binding,
                ..
            } => {
                names.insert(dst_var.clone());
                names.extend(edge_binding.iter().cloned());
            }
            PatternStep::Filter(_) | PatternStep::AntiJoin { .. } | PatternStep::Rank { .. } => {}
        }
    }
    names
}

struct Shaper<'a> {
    catalog: &'a Catalog,
    name: &'a str,
    params: &'a [(String, PropType)],
}

/// The pattern facts of one scope, in the order the term states them.
#[derive(Default, Debug)]
struct Facts {
    bindings: Vec<BindingFact>,
    traversals: Vec<TraversalFact>,
    filters: Vec<IRExpr>,
    blocks: Vec<BlockFact>,
    /// The type of every node variable the scope binds.
    types: HashMap<String, String>,
    /// Each `let` name and its value, earlier `let`s already read into it.
    lets: HashMap<String, IRExpr>,
    /// How many inline reads' hidden names were renamed apart so far.
    inlined: usize,
}

impl Facts {
    /// Every name the facts bind or read as a node or edge variable.
    fn names(&self) -> Vec<String> {
        let mut out: Vec<String> = self.types.keys().cloned().collect();
        out.extend(self.bindings.iter().map(|b| b.variable.clone()));
        for t in &self.traversals {
            out.extend([t.src.clone(), t.dst.clone()]);
            out.extend(t.edge_binding.iter().cloned());
        }
        out.retain(|name| name != "_");
        out
    }

    /// The facts with every variable `renames` names renamed, a block's
    /// included.
    fn rename(&mut self, renames: &HashMap<String, String>) {
        if renames.is_empty() {
            return;
        }
        let rename = |variable: &mut String| {
            if let Some(name) = renames.get(variable) {
                *variable = name.clone();
            }
        };
        for binding in &mut self.bindings {
            rename(&mut binding.variable);
            binding
                .filters
                .iter_mut()
                .for_each(|f| rename_expr(f, &rename));
        }
        for t in &mut self.traversals {
            rename(&mut t.src);
            rename(&mut t.dst);
            if let Some(edge) = &mut t.edge_binding {
                rename(edge);
            }
        }
        self.filters
            .iter_mut()
            .for_each(|f| rename_expr(f, &rename));
        self.types = std::mem::take(&mut self.types)
            .into_iter()
            .map(|(mut name, ty)| {
                rename(&mut name);
                (name, ty)
            })
            .collect();
        for block in &mut self.blocks {
            block.inner.rename(renames);
            block.outer = std::mem::take(&mut block.outer)
                .into_iter()
                .map(|mut name| {
                    rename(&mut name);
                    name
                })
                .collect();
            if let Some(arg) = block.left.arg_mut() {
                rename_expr(arg, &rename);
            }
            rename_expr(&mut block.right, &rename);
        }
    }

    /// Every expression the facts hold, a block's included, with each
    /// `let` name read as its value.
    fn substitute(&mut self, lets: &HashMap<String, IRExpr>) {
        if lets.is_empty() {
            return;
        }
        for binding in &mut self.bindings {
            binding.filters.iter_mut().for_each(|f| substitute(f, lets));
        }
        self.filters.iter_mut().for_each(|f| substitute(f, lets));
        for block in &mut self.blocks {
            block.inner.substitute(lets);
            if let Some(arg) = block.left.arg_mut() {
                substitute(arg, lets);
            }
            substitute(&mut block.right, lets);
        }
    }
}

#[derive(Debug)]
struct BindingFact {
    variable: String,
    type_name: String,
    filters: Vec<IRExpr>,
}

#[derive(Debug, Clone)]
struct TraversalFact {
    src: String,
    dst: String,
    edges: EdgeSelection,
    src_type: String,
    dst_type: String,
    min_hops: u32,
    max_hops: Option<u32>,
    edge_binding: Option<String>,
}

#[derive(Debug)]
struct BlockFact {
    inner: Facts,
    /// The outer columns the block may correlate on.
    outer: HashSet<String>,
    left: BlockAggregateExpr,
    op: CompOp,
    right: IRExpr,
}

fn is_anon(name: &str) -> bool {
    name.starts_with(ANON_PREFIX)
}

fn is_block_column(name: &str) -> bool {
    name.starts_with(FRESH_PREFIX) && name[FRESH_PREFIX.len()..].starts_with('b')
}

fn is_semi(name: &str) -> bool {
    name.starts_with(FRESH_PREFIX) && name[FRESH_PREFIX.len()..].starts_with('t')
}

/// The variable every leaf of `e` reads, when it reads exactly one.
fn single_variable(e: &IRExpr) -> Option<String> {
    let mut cols = Vec::new();
    e.columns(&mut cols);
    match cols.as_slice() {
        [one] => Some(one.clone()),
        _ => None,
    }
}

impl<'a> Shaper<'a> {
    fn new(catalog: &'a Catalog, name: &'a str, params: &'a [(String, PropType)]) -> Self {
        Self {
            catalog,
            name,
            params,
        }
    }

    /// A parameter's declared type.
    fn param_type(&self, name: &str) -> ExprType {
        self.params
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, ty)| ExprType::from_prop(ty))
            .unwrap_or_else(|| scalar(ScalarType::I64))
    }

    /// A refusal at the whole read; the caller that knows the construct
    /// sets it ([`ShapeError::at`]).
    fn refuse(&self, what: impl std::fmt::Display) -> ShapeError {
        unsupported(self.name, what, Construct::Read)
    }

    /// A refusal at `construct`.
    fn refuse_at(&self, what: impl std::fmt::Display, construct: Construct) -> ShapeError {
        unsupported(self.name, what, construct)
    }

    /// A core node with no plan node in a pattern, at the element it was
    /// written as: a `Union` is an `optional` or a `from union(…)`, the
    /// others a `from`.
    fn unlowered_node(&self, node: &Term) -> ShapeError {
        let (what, kinds): (String, &'static [ElementKind]) = match node {
            Term::Union { .. } => (
                "a `Union` in a pattern (an `optional`, or a `from` over `union`)".into(),
                &[ElementKind::Optional, ElementKind::From],
            ),
            Term::Call { name, .. } => (
                format!("a declared read's call in a pattern (`{name}`)"),
                FROM,
            ),
            Term::Group { .. } => (
                "an inline read with an aggregate or `distinct` (a `Group` in a pattern)".into(),
                FROM,
            ),
            Term::Order { .. } => (
                "an inline read with an `order` (an `Order` in a pattern)".into(),
                FROM,
            ),
            Term::Cut { .. } => (
                "an inline read with a `limit` (a `Cut` in a pattern)".into(),
                FROM,
            ),
            other => (format!("a `{}` node in a pattern", other.operator()), FROM),
        };
        self.refuse_at(what, Construct::Element(kinds, None))
    }

    fn read(&self, read: &CoreRead) -> Result<ReadShape, ShapeError> {
        // A recursive read types to `Fix`, which stage Rec evaluates.
        let mut nodes = Vec::new();
        read.term.nodes(&mut nodes);
        if nodes
            .iter()
            .any(|n| matches!(n, CoreNode::Fix | CoreNode::RecRef))
        {
            return Err(ShapeError::unlowered(
                Unlowered::new(
                    self.name,
                    "a read that calls itself (`Fix`)",
                    Construct::Element(FROM, None),
                )
                .stage(ProgrammeStage::Rec),
            ));
        }
        // The final projection names the result columns.
        let Term::Project {
            input,
            items: finals,
            keep: false,
        } = &read.term
        else {
            return Err(self.refuse("a read without its final projection"));
        };
        let mut term: &Term = input;
        let mut limit = None;
        if let Term::Cut { input, k } = term {
            limit = Some(match k {
                Count::Literal(n) => *n,
                Count::Param(_) => {
                    return Err(self.refuse_at("`limit` over a parameter", Construct::Limit));
                }
            });
            term = input;
        }
        let mut order_keys: &[(IRExpr, bool)] = &[];
        if let Term::Order { input, keys } = term {
            order_keys = keys;
            term = input;
        }
        // The items, by their hidden names, and the body under them.
        let mut items: HashMap<String, IRExpr> = HashMap::new();
        let mut aggs: HashMap<String, Agg> = HashMap::new();
        let mut distinct = false;
        let body = match term {
            Term::Project {
                input,
                items: extended,
                keep: true,
            } => match input.as_ref() {
                Term::Group {
                    input,
                    keys,
                    aggs: grouped,
                } => {
                    items.extend(extended.iter().cloned());
                    items.extend(keys.iter().cloned());
                    aggs.extend(grouped.iter().cloned());
                    input.as_ref()
                }
                other => {
                    items.extend(extended.iter().cloned());
                    other
                }
            },
            Term::Group {
                input,
                keys,
                aggs: grouped,
            } => {
                distinct = grouped.is_empty();
                items.extend(keys.iter().cloned());
                aggs.extend(grouped.iter().cloned());
                input.as_ref()
            }
            _ => return Err(self.refuse("a read whose returns are not a projection")),
        };
        let Split {
            population: body,
            ranking,
            node: ranked_node,
            after,
        } = self
            .split_ranking(body)
            .map_err(|e| e.at(Construct::Element(FROM, None)))?;
        // The pattern, with every `let` value read where its name is.
        let mut facts = Facts::default();
        self.facts(body, &mut facts)?;
        let mut after_facts = Facts::default();
        if let Some(after) = &after {
            self.facts(after, &mut after_facts)?;
        }
        let mut lets = std::mem::take(&mut facts.lets);
        lets.extend(std::mem::take(&mut after_facts.lets));
        facts.substitute(&lets);
        for e in items.values_mut() {
            substitute(e, &lets);
        }
        for agg in aggs.values_mut() {
            if let Some(arg) = &mut agg.arg {
                substitute(arg, &lets);
            }
        }
        let order_keys: Vec<(IRExpr, bool)> = order_keys
            .iter()
            .map(|(key, descending)| {
                let mut key = key.clone();
                substitute(&mut key, &lets);
                (key, *descending)
            })
            .collect();
        let order_keys = order_keys.as_slice();
        // Return items.
        let mut return_exprs = Vec::new();
        for (i, (column, expr)) in finals.iter().enumerate() {
            let (IRExpr::AliasRef(hidden, ty) | IRExpr::Variable(hidden, ty)) = expr else {
                return Err(self.refuse_at(
                    "a final projection that is not a column",
                    Construct::ReturnItem(i),
                ));
            };
            let item = match (items.get(hidden), aggs.get(hidden)) {
                (Some(e), _) => self
                    .item(e, &aggs, ranking.as_ref())
                    .map_err(|e| e.at(Construct::ReturnItem(i)))?,
                (None, Some(agg)) => self
                    .aggregate(agg, ranking.as_ref())
                    .map_err(|e| e.at(Construct::ReturnItem(i)))?,
                (None, None) => {
                    return Err(ShapeError::Internal(format!(
                        "result column `{column}` names no item"
                    )));
                }
            };
            let alias = self.alias(column, &item);
            return_exprs.push(IRProjection {
                ty: if item.ty() == ty {
                    ty.clone()
                } else {
                    item.ty().clone()
                },
                expr: item,
                alias,
                column: column.clone(),
            });
        }
        // Order keys: the rank key leads (left out under an aggregate or
        // `distinct`), the others read return items or the pattern.
        let grouped = !aggs.is_empty() || distinct;
        // Today's ranked scan runs a ranking that is the read's only element,
        // its score leading the `order` in its direction and its window the
        // read's `limit`; any other ranking is a `Rank` step over its
        // population, cut to its own window, the `order` a plain sort.
        let windowed = ranking.as_ref().is_some_and(|ranking| {
            after.is_some()
                || self
                    .check_ranking(ranking, order_keys, limit, grouped)
                    .is_err()
        });
        let mut order_by = Vec::new();
        let mut keys = order_keys.iter().enumerate().peekable();
        if let Some(ranking) = ranking.as_ref().filter(|_| !windowed) {
            let leads = matches!(keys.peek(), Some((_, (IRExpr::AliasRef(name, _), _))) if *name == ranking.out);
            let descending = match keys.peek() {
                Some((_, (_, descending))) if leads => *descending,
                _ => ranking.descending,
            };
            if leads {
                keys.next();
            }
            order_by.push(IROrdering {
                expr: ranking.key.clone(),
                descending,
            });
        }
        for (position, (key, descending)) in keys {
            let at = Construct::OrderKey(position);
            let expr = match key {
                IRExpr::AliasRef(hidden, _) | IRExpr::Variable(hidden, _)
                    if items.contains_key(hidden) || aggs.contains_key(hidden) =>
                {
                    let projection = return_exprs
                        .iter()
                        .zip(finals)
                        .find(|(_, (_, e))| {
                            matches!(e, IRExpr::AliasRef(h, _) | IRExpr::Variable(h, _) if h == hidden)
                        })
                        .map(|(p, _)| p);
                    match projection {
                        Some(IRProjection {
                            alias: Some(alias),
                            ty,
                            ..
                        }) => IRExpr::AliasRef(alias.clone(), ty.clone()),
                        Some(projection) => projection.expr.clone(),
                        None => self
                            .item(
                                items.get(hidden).ok_or_else(|| {
                                    ShapeError::unlowered(
                                        Unlowered::new(
                                            self.name,
                                            "an order key over an aggregate no return item computes",
                                            at.clone(),
                                        )
                                        .repair(Repair::ReturnTheOrderKey),
                                    )
                                })?,
                                &aggs,
                                ranking.as_ref(),
                            )
                            .map_err(|e| e.at(at.clone()))?,
                    }
                }
                other => self
                    .item(other, &aggs, ranking.as_ref())
                    .map_err(|e| e.at(at.clone()))?,
            };
            // The sort reads columns: a computed key reads the first return
            // item that computes it; one no item computes is not lowered yet.
            let computed = matches!(
                expr,
                IRExpr::Binary { .. }
                    | IRExpr::Not(..)
                    | IRExpr::Cast { .. }
                    | IRExpr::Neg { .. }
                    | IRExpr::Call { .. }
                    | IRExpr::IsNull { .. }
            );
            let expr = if computed {
                let Some(item) = return_exprs.iter().find(|p| p.expr == expr) else {
                    return Err(ShapeError::unlowered(
                        Unlowered::new(
                            self.name,
                            "an order key computed from columns no return item computes",
                            at,
                        )
                        .repair(Repair::ReturnTheOrderKey),
                    ));
                };
                IRExpr::AliasRef(
                    item.alias.clone().unwrap_or_else(|| item.column.clone()),
                    item.ty.clone(),
                )
            } else {
                expr
            };
            order_by.push(IROrdering {
                expr,
                descending: *descending,
            });
        }
        let ranked: Vec<String> = ranking
            .as_ref()
            .map(|r| vec![r.column.clone()])
            .unwrap_or_default();
        let mut pipeline = Vec::new();
        let mut fresh = FreshNames::default();
        let mut bound = HashSet::new();
        if let Some(ranking) = ranking.as_ref().filter(|_| windowed) {
            // The population as its inline read states it, the names it
            // binds without returning renamed apart from the later elements.
            let mut population = Facts::default();
            match ranked_node.map(ranked_input) {
                Some(Term::Project {
                    input,
                    items,
                    keep: false,
                }) => self.inline_read(input, items, &mut population)?,
                _ => self.facts(body, &mut population)?,
            }
            let population_lets = std::mem::take(&mut population.lets);
            population.substitute(&population_lets);
            self.place(
                &population,
                &ranked,
                &mut pipeline,
                &mut bound,
                &HashSet::new(),
                &mut fresh,
            )?;
            let window = match ranking.windows.as_slice() {
                [] => None,
                [Some(k)] => Some(*k),
                [Some(k), rest @ ..] if rest.iter().all(|w| *w == Some(*k)) => Some(*k),
                _ => {
                    return Err(self.refuse_at(
                        "a `nearest` window read from a parameter, or two `rrf` arm windows that differ",
                        Construct::Element(FROM, None),
                    ));
                }
            };
            pipeline.push(PatternStep::Rank {
                key: ranking.key.clone(),
                binding: ranking.column.clone(),
                window,
            });
            // The later elements and `where` read the score by its name.
            let mut score = HashMap::new();
            score.insert(
                ranking.out.clone(),
                IRExpr::PropAccess {
                    variable: ranking.column.clone(),
                    property: ranking.score.0.to_string(),
                    ty: ranking.score.1.clone(),
                },
            );
            after_facts.substitute(&score);
            let physical = physical_names(&pipeline);
            self.place(
                &after_facts,
                &[],
                &mut pipeline,
                &mut bound,
                &physical,
                &mut fresh,
            )?;
        } else {
            self.place(
                &facts,
                &ranked,
                &mut pipeline,
                &mut bound,
                &HashSet::new(),
                &mut fresh,
            )?;
        }
        Ok(ReadShape {
            name: read.name.clone(),
            params: read
                .params
                .iter()
                .map(|(name, ty)| IRParam {
                    declaration: Param {
                        name: name.clone(),
                        type_name: ty.display_name().trim_end_matches('?').to_string(),
                        nullable: ty.nullable,
                    },
                    ty: ExprType::from_prop(ty),
                })
                .collect(),
            pipeline,
            return_exprs,
            distinct,
            order_by,
            limit,
        })
    }

    /// The alias a return item carries: none when the column is the name the
    /// item gets unaliased, the column otherwise; a meta field always names
    /// its result key.
    fn alias(&self, column: &str, item: &IRExpr) -> Option<String> {
        let default = match item {
            IRExpr::PropAccess {
                variable, property, ..
            } => {
                let system = self.catalog.system_columns;
                if [system.id, system.src, system.dst].contains(&property.as_str()) {
                    return Some(column.to_string());
                }
                format!("{variable}.{property}")
            }
            IRExpr::Variable(name, _) | IRExpr::Param(name, _) => name.clone(),
            IRExpr::Literal(_, _) => "literal".to_string(),
            IRExpr::Aggregate { arg, .. } => match arg.as_ref() {
                IRExpr::PropAccess {
                    variable, property, ..
                } => format!("{variable}.{property}"),
                IRExpr::Variable(name, _) => name.clone(),
                _ => String::new(),
            },
            _ => String::new(),
        };
        (default != column).then(|| column.to_string())
    }

    /// A return item or order key: a hidden aggregate column read as its
    /// aggregate, a score column as the column the retrieval appends.
    fn item(
        &self,
        e: &IRExpr,
        aggs: &HashMap<String, Agg>,
        ranking: Option<&Ranking>,
    ) -> Result<IRExpr, ShapeError> {
        Ok(match e {
            IRExpr::AliasRef(name, _) if aggs.contains_key(name) => {
                self.aggregate(&aggs[name], ranking)?
            }
            IRExpr::AliasRef(name, _) if ranking.is_some_and(|r| r.out == *name) => {
                let r = ranking.unwrap();
                IRExpr::PropAccess {
                    variable: r.column.clone(),
                    property: r.score.0.to_string(),
                    ty: r.score.1.clone(),
                }
            }
            IRExpr::Binary {
                left,
                op,
                right,
                ty,
            } => IRExpr::Binary {
                left: Box::new(self.item(left, aggs, ranking)?),
                op: *op,
                right: Box::new(self.item(right, aggs, ranking)?),
                ty: ty.clone(),
            },
            IRExpr::Not(inner, ty) => {
                IRExpr::Not(Box::new(self.item(inner, aggs, ranking)?), ty.clone())
            }
            IRExpr::IsNull { expr, negated, ty } => IRExpr::IsNull {
                expr: Box::new(self.item(expr, aggs, ranking)?),
                negated: *negated,
                ty: ty.clone(),
            },
            IRExpr::Cast { expr, ty } => IRExpr::Cast {
                expr: Box::new(self.item(expr, aggs, ranking)?),
                ty: ty.clone(),
            },
            other => other.clone(),
        })
    }

    /// What today's ranked scans run: the score leads the `order` in its
    /// ranking's direction (absent over groups, which it does not order),
    /// and each `nearest` window is the read's `limit` (`100` under `rrf`
    /// without one). Any other combination is a different relation, not
    /// lowered yet.
    fn check_ranking(
        &self,
        ranking: &Ranking,
        order_keys: &[(IRExpr, bool)],
        limit: Option<u64>,
        grouped: bool,
    ) -> Result<(), ShapeError> {
        let reads_score =
            |e: &IRExpr| matches!(e, IRExpr::AliasRef(name, _) if *name == ranking.out);
        match order_keys.first() {
            Some((key, descending)) if reads_score(key) => {
                if *descending != ranking.descending {
                    return Err(self.refuse("a score ordered against its ranking's direction"));
                }
            }
            _ if order_keys.iter().any(|(k, _)| reads_score(k)) => {
                return Err(self.refuse("a score that is not the leading `order` key"));
            }
            Some(_) if !grouped => {
                return Err(
                    self.refuse("an `order` over a ranking that does not lead with its score")
                );
            }
            None if !grouped && limit.is_some() => {
                return Err(self.refuse("a `limit` over a ranking with no `order` by its score"));
            }
            _ => {}
        }
        let fused = matches!(ranking.key, IRExpr::Rrf { .. });
        let expected = match (limit, fused) {
            (Some(n), _) => Some(n),
            (None, true) => Some(RRF_NEAREST_ARM_WINDOW),
            (None, false) => None,
        };
        if ranking
            .windows
            .iter()
            .any(|w| w.is_none() || *w != expected)
        {
            return Err(self.refuse("a `nearest` window `k` other than the read's `limit`"));
        }
        Ok(())
    }

    /// A `Group` aggregate as the aggregate expression a return item holds.
    fn aggregate(&self, agg: &Agg, ranking: Option<&Ranking>) -> Result<IRExpr, ShapeError> {
        let Some(arg) = &agg.arg else {
            return Err(self.refuse("a row count without an argument in `return`"));
        };
        let mut read = Vec::new();
        arg.columns(&mut read);
        if ranking.is_some_and(|r| read.contains(&r.out)) {
            return Err(self.refuse("an aggregate over a ranking's score"));
        }
        let result = nullable_result(agg);
        Ok(IRExpr::Aggregate {
            func: agg.func,
            distinct: agg.distinct,
            arg: Box::new(arg.clone()),
            signature: AggSignature {
                arg: arg.ty().clone(),
                result,
            },
        })
    }

    /// The ranking a body draws and where: at its root (the pattern under
    /// it), or under later elements and a `where`, which `after` holds with
    /// the ranking replaced by an empty leaf.
    fn split_ranking<'t>(&self, body: &'t Term) -> Result<Split<'t>, ShapeError> {
        let (population, ranking) = self.ranking(body)?;
        if ranking.is_some() {
            return Ok(Split {
                population,
                ranking,
                node: Some(body)
                    .filter(|b| !matches!(b, Term::Fuse { .. }) && fuse_leaf(b).is_none()),
                after: None,
            });
        }
        let mut node = body;
        loop {
            node = match node {
                Term::ApproxTopK { .. }
                | Term::Score {
                    kind: ScoreKind::Bm25,
                    ..
                } => break,
                Term::Filter { input, .. }
                | Term::Expand { input, .. }
                | Term::Project {
                    input, keep: true, ..
                } => input,
                Term::Correlate { outer, .. } => outer,
                _ => {
                    return Ok(Split {
                        population,
                        ranking: None,
                        node: None,
                        after: None,
                    });
                }
            };
        }
        let (population, ranking) = self.ranking(node)?;
        Ok(Split {
            population,
            ranking,
            node: Some(node),
            after: Some(without_leaf(body, node)),
        })
    }

    /// The ranking at the root of a body, when it draws one, and the
    /// pattern under it.
    fn ranking<'t>(&self, body: &'t Term) -> Result<(&'t Term, Option<Ranking>), ShapeError> {
        match body {
            Term::ApproxTopK {
                input,
                column,
                property,
                query,
                out,
                k,
                ..
            } => Ok((
                self.population(input)?,
                Some(Ranking {
                    key: IRExpr::Nearest {
                        variable: column.clone(),
                        property: property.clone(),
                        query: Box::new(query.clone()),
                        ty: scalar(ScalarType::F32),
                    },
                    out: out.clone(),
                    column: column.clone(),
                    score: (DISTANCE_COLUMN, scalar(ScalarType::F32)),
                    descending: false,
                    windows: vec![match k {
                        Count::Literal(n) => Some(*n),
                        Count::Param(_) => None,
                    }],
                }),
            )),
            Term::Score {
                input,
                column,
                property,
                kind: ScoreKind::Bm25,
                query: ScoreQuery::Lexical(LexicalQuery { text, .. }),
                out,
            } => Ok((
                self.population(input)?,
                Some(Ranking {
                    key: IRExpr::Bm25 {
                        field: Box::new(IRExpr::PropAccess {
                            variable: column.clone(),
                            property: property.clone(),
                            ty: scalar(ScalarType::String),
                        }),
                        query: Box::new(text.clone()),
                        ty: scalar(ScalarType::F32),
                    },
                    out: out.clone(),
                    column: column.clone(),
                    score: (SCORE_COLUMN, scalar(ScalarType::F32)),
                    descending: true,
                    windows: Vec::new(),
                }),
            )),
            Term::Score { .. } => Err(self.refuse("`distance`")),
            other => match fuse_leaf(other) {
                Some(fuse) => Ok((other, Some(self.fused(fuse)?))),
                None => Ok((other, None)),
            },
        }
    }

    /// `rrf` over the type population: the `Fuse` at the leaf of the
    /// pattern, which the rest of the pattern extends from its target.
    fn fused(&self, fuse: &Term) -> Result<Ranking, ShapeError> {
        let Term::Fuse {
            arms,
            target,
            k,
            out,
            ..
        } = fuse
        else {
            unreachable!("fuse_leaf returns a Fuse")
        };
        let mut windows = Vec::new();
        let mut arm = |t: &Term| -> Result<IRExpr, ShapeError> {
            match self.ranking(t)? {
                (_, Some(r)) => {
                    windows.extend(r.windows);
                    Ok(r.key)
                }
                (_, None) => Err(self.refuse("an `rrf` arm that is not a ranking")),
            }
        };
        let [primary, secondary] = arms.as_slice() else {
            return Err(self.refuse("an `rrf` of other than two arms"));
        };
        let (primary, secondary) = (arm(primary)?, arm(secondary)?);
        // The default constant is the unwritten one.
        let k = match k {
            Count::Literal(n) if *n == RRF_DEFAULT_K => None,
            Count::Literal(n) => Some(Box::new(IRExpr::Literal(
                Literal::Integer(i128::from(*n)),
                scalar(ScalarType::I64),
            ))),
            Count::Param(p) => Some(Box::new(IRExpr::Param(p.clone(), self.param_type(p)))),
        };
        Ok(Ranking {
            key: IRExpr::Rrf {
                primary: Box::new(primary),
                secondary: Box::new(secondary),
                k,
                ty: scalar(ScalarType::F64),
            },
            out: out.clone(),
            column: target.clone(),
            score: (RRF_COLUMN, scalar(ScalarType::F64)),
            descending: true,
            windows,
        })
    }

    /// The pattern of a ranking's population: an inline read returning the
    /// variables its pattern binds.
    fn population<'t>(&self, input: &'t Term) -> Result<&'t Term, ShapeError> {
        let mut term = input;
        while let Term::Project { input, .. } = term {
            term = input;
        }
        Ok(term)
    }

    /// The facts of `term` into `facts`.
    fn facts(&self, term: &Term, facts: &mut Facts) -> Result<(), ShapeError> {
        match term {
            Term::Unit | Term::Env(_) => Ok(()),
            Term::Scan {
                var,
                ty: Ty::Node { type_name, .. },
            } => {
                facts.types.insert(var.clone(), type_name.clone());
                facts.bindings.push(BindingFact {
                    variable: var.clone(),
                    type_name: type_name.clone(),
                    filters: Vec::new(),
                });
                Ok(())
            }
            Term::Scan { var, .. } => Err(self.refuse_at(
                "a scan of an edge type in a read",
                Construct::Element(&[ElementKind::Binding], Some(var.clone())),
            )),
            Term::Expand {
                input,
                src,
                dst,
                src_type,
                dst_type,
                edges,
                min_hops,
                max_hops,
                edge_var,
            } => {
                self.facts(input, facts)?;
                self.traversal(
                    facts,
                    TraversalFact {
                        src: src.clone(),
                        dst: dst.clone(),
                        edges: edges.clone(),
                        src_type: src_type.clone(),
                        dst_type: dst_type.clone(),
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        edge_binding: edge_var.clone(),
                    },
                );
                Ok(())
            }
            Term::Filter { input, pred } => self.filter(input, pred, facts),
            // `let $v = e`: a value column, read wherever its name is. The
            // core evaluates it for every row before `where` (A3), which a
            // value that cannot fail makes the same rows as reading it in
            // place; a value that can fail needs a column computed before the
            // filter, which no operator adds inside a pattern.
            Term::Project {
                input,
                items,
                keep: true,
            } => {
                self.facts(input, facts)?;
                for (name, value) in items {
                    let at = Construct::Element(&[ElementKind::Let], Some(name.clone()));
                    if value.can_fail() {
                        return Err(ShapeError::unlowered(
                            Unlowered::new(self.name, "a `let` whose value can fail", at)
                                .repair(Repair::WriteTheLetWhereItIsRead),
                        ));
                    }
                    let mut cols = Vec::new();
                    value.columns(&mut cols);
                    if cols.iter().any(|c| is_block_column(c)) {
                        return Err(self.refuse_at("a `let` over a block value", at));
                    }
                    let mut value = value.clone();
                    substitute(&mut value, &facts.lets);
                    facts.lets.insert(name.clone(), value);
                }
                Ok(())
            }
            // A `from` that opens the pattern: the inline read alone.
            Term::Project {
                input,
                items,
                keep: false,
            } => self.inline_read(input, items, facts),
            Term::Correlate { outer, inner } => {
                self.facts(outer, facts)?;
                if let Term::Group { .. } = inner.as_ref() {
                    return Err(self.refuse_at(
                        "a correlated block read without its filter",
                        Construct::Block,
                    ));
                }
                if let Term::Project {
                    input,
                    items,
                    keep: false,
                } = inner.as_ref()
                {
                    return self.inline_read(input, items, facts);
                }
                self.facts(inner, facts)
            }
            Term::Fuse { arms, target, .. } => {
                let type_name = arms
                    .first()
                    .and_then(|arm| population_type(arm, target))
                    .ok_or_else(|| {
                        self.refuse_at(
                            "an `rrf` arm over a population that is not a type",
                            Construct::Element(FROM, None),
                        )
                    })?;
                facts.types.insert(target.clone(), type_name.clone());
                facts.bindings.push(BindingFact {
                    variable: target.clone(),
                    type_name,
                    filters: Vec::new(),
                });
                Ok(())
            }
            other => Err(self.unlowered_node(other)),
        }
    }

    /// `from { match … return { … } }`: one outer row per inner row, the
    /// inner pattern joined on the outer variables it reads. With no group,
    /// order or cut in it, that is the inner pattern's facts added to the
    /// outer ones: a returned reference column binds its name, a returned
    /// value is a `let` of that name, and every name the inner read binds
    /// without returning it is renamed apart, so a later outer binding of the
    /// same name stays another variable.
    fn inline_read(
        &self,
        input: &Term,
        items: &[(String, IRExpr)],
        facts: &mut Facts,
    ) -> Result<(), ShapeError> {
        let outer: HashSet<String> = env_columns(input).into_iter().collect();
        let mut inner = Facts::default();
        self.facts(input, &mut inner)?;
        let inner_lets = std::mem::take(&mut inner.lets);
        inner.substitute(&inner_lets);
        let items: Vec<(String, IRExpr)> = items
            .iter()
            .map(|(column, expr)| {
                let mut expr = expr.clone();
                substitute(&mut expr, &inner_lets);
                (column.clone(), expr)
            })
            .collect();
        let mut renames: HashMap<String, String> = HashMap::new();
        let mut values: Vec<(String, IRExpr)> = Vec::new();
        for (column, expr) in &items {
            match expr {
                IRExpr::Variable(variable, ExprType::Node { .. } | ExprType::Edge { .. }) => {
                    if variable != column {
                        if outer.contains(variable) {
                            return Err(self.refuse_at(
                                "a `from` returning an outer variable under another name",
                                Construct::Element(FROM, None),
                            ));
                        }
                        renames.insert(variable.clone(), column.clone());
                    }
                }
                _ => values.push((column.clone(), expr.clone())),
            }
        }
        let exposed: HashSet<&String> = items
            .iter()
            .filter_map(|(_, e)| match e {
                IRExpr::Variable(v, _) => Some(v),
                _ => None,
            })
            .collect();
        let mut hidden: Vec<String> = inner
            .names()
            .into_iter()
            .filter(|name| !outer.contains(name) && !exposed.contains(name) && !is_anon(name))
            .collect();
        hidden.sort();
        hidden.dedup();
        for name in hidden {
            let fresh = format!("{FRESH_PREFIX}f{}_{name}", facts.inlined);
            facts.inlined += 1;
            renames.insert(name, fresh);
        }
        inner.rename(&renames);
        let rename = |variable: &mut String| {
            if let Some(name) = renames.get(variable) {
                *variable = name.clone();
            }
        };
        for (column, mut value) in values {
            rename_expr(&mut value, &rename);
            facts.lets.insert(column, value);
        }
        facts.bindings.extend(inner.bindings);
        facts.traversals.extend(inner.traversals);
        facts.filters.extend(inner.filters);
        facts.blocks.extend(inner.blocks);
        facts.types.extend(inner.types);
        Ok(())
    }

    fn traversal(&self, facts: &mut Facts, mut t: TraversalFact) {
        for end in [&mut t.src, &mut t.dst] {
            if is_anon(end) {
                *end = "_".to_string();
            }
        }
        if t.dst != "_" {
            facts.types.insert(t.dst.clone(), t.dst_type.clone());
        }
        if t.src != "_" {
            facts.types.insert(t.src.clone(), t.src_type.clone());
        }
        facts.traversals.push(t);
    }

    fn filter(&self, input: &Term, pred: &IRExpr, facts: &mut Facts) -> Result<(), ShapeError> {
        // A triple whose ends were both bound: the expansion to a fresh name
        // and the identity filter tying it to the end.
        if let Term::Expand {
            input: below,
            src,
            dst,
            src_type,
            dst_type,
            edges,
            min_hops,
            max_hops,
            edge_var,
        } = input
            && is_semi(dst)
            && let Some((left, CompOp::Eq, IRExpr::PropAccess { variable: end, .. })) =
                pred.comparison_parts()
            && matches!(left, IRExpr::PropAccess { variable, .. } if variable == dst)
        {
            self.facts(below, facts)?;
            self.traversal(
                facts,
                TraversalFact {
                    src: src.clone(),
                    dst: end.clone(),
                    edges: edges.clone(),
                    src_type: src_type.clone(),
                    dst_type: dst_type.clone(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    edge_binding: edge_var.clone(),
                },
            );
            return Ok(());
        }
        // A predicate over block columns: the blocks under it.
        let mut cols = Vec::new();
        pred.columns(&mut cols);
        if cols.iter().any(|c| is_block_column(c)) {
            return self.blocks(input, pred, facts);
        }
        self.facts(input, facts)?;
        // A filter over one binding's variable, written right after it, is
        // that binding's filter.
        let mut below = input;
        while let Term::Filter { input, .. } = below {
            below = input;
        }
        if let Term::Scan { var, .. } = below
            && single_variable(pred).as_deref() == Some(var.as_str())
            && let Some(binding) = facts.bindings.iter_mut().rev().find(|b| b.variable == *var)
        {
            binding.filters.push(pred.clone());
            return Ok(());
        }
        facts.filters.push(pred.clone());
        Ok(())
    }

    /// `pred` over the aggregates of the correlated blocks under it.
    fn blocks(&self, input: &Term, pred: &IRExpr, facts: &mut Facts) -> Result<(), ShapeError> {
        let mut groups: Vec<(&Term, &str, &Agg, Vec<Col>)> = Vec::new();
        let mut below = input;
        while let Term::Correlate { outer, inner } = below {
            let Term::Group { input, keys, aggs } = inner.as_ref() else {
                break;
            };
            let [(column, agg)] = aggs.as_slice() else {
                return Err(self.refuse_at("a block of more than one aggregate", Construct::Block));
            };
            if !keys.is_empty() {
                return Err(self.refuse_at("a block with grouping keys", Construct::Block));
            }
            let mut leaf = input.as_ref();
            loop {
                match leaf {
                    Term::Env(cols) => {
                        groups.push((input, column, agg, cols.clone()));
                        break;
                    }
                    other => match other.children().first() {
                        Some(child) => leaf = child,
                        None => {
                            groups.push((input, column, agg, Vec::new()));
                            break;
                        }
                    },
                }
            }
            below = outer;
        }
        self.facts(below, facts)?;
        // One block per comparison; `not (c > 0)` is the row-count zero test.
        groups.reverse();
        for (inner, column, agg, env) in groups {
            let (op, right) = match block_comparison(pred, column) {
                Some(found) => found,
                None => return Err(self.refuse_at("a filter over block values", Construct::Block)),
            };
            if !right.columns_empty() {
                return Err(
                    self.refuse_at("a block value compared with a column", Construct::Block)
                );
            }
            let mut inner_facts = Facts::default();
            self.facts(inner, &mut inner_facts)?;
            let inner_lets = std::mem::take(&mut inner_facts.lets);
            inner_facts.substitute(&inner_lets);
            let mut agg = agg.clone();
            if let Some(arg) = &mut agg.arg {
                substitute(arg, &inner_lets);
            }
            let agg = &agg;
            let left = match &agg.arg {
                // Counting a node binding counts its rows.
                None => BlockAggregateExpr::count_rows(),
                Some(IRExpr::Variable(_, ExprType::Node { .. } | ExprType::Edge { .. }))
                    if agg.func == AggFunc::Count && !agg.distinct =>
                {
                    BlockAggregateExpr::count_rows()
                }
                Some(arg) => BlockAggregateExpr::Aggregate {
                    func: agg.func,
                    arg: Box::new(arg.clone()),
                    signature: AggSignature {
                        arg: arg.ty().clone(),
                        result: nullable_result(agg),
                    },
                },
            };
            facts.blocks.push(BlockFact {
                inner: inner_facts,
                outer: env.iter().map(|c| c.name.clone()).collect(),
                left,
                op,
                right,
            });
        }
        Ok(())
    }

    /// The pattern's steps: the facts placed as the module documentation
    /// describes.
    fn place(
        &self,
        facts: &Facts,
        ranked: &[String],
        pipeline: &mut Vec<PatternStep>,
        bound_vars: &mut HashSet<String>,
        outer_physical_names: &HashSet<String>,
        fresh: &mut FreshNames,
    ) -> Result<(), ShapeError> {
        let bindings: Vec<&BindingFact> = facts.bindings.iter().collect();
        let traversals: Vec<&TraversalFact> = facts.traversals.iter().collect();
        let binding_set: HashSet<&str> = bindings.iter().map(|b| b.variable.as_str()).collect();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for t in &traversals {
            let (src, dst) = (t.src.as_str(), t.dst.as_str());
            if src != "_" && dst != "_" {
                adj.entry(src).or_default().push(dst);
                adj.entry(dst).or_default().push(src);
            }
        }
        let mut deferred_set: HashSet<String> = HashSet::new();
        let mut component_visited: HashSet<&str> = HashSet::new();
        let searched: HashSet<String> = facts.filters.iter().filter_map(search_subject).collect();
        let mut ranked_scans: Vec<(Option<&str>, &str, &str)> = Vec::new();
        let ranked_nodes: Vec<(&str, &str)> = ranked
            .iter()
            .filter_map(|var| {
                facts
                    .types
                    .get(var)
                    .map(|type_name| (var.as_str(), type_name.as_str()))
            })
            .collect();
        let starts = bindings
            .iter()
            .map(|binding| binding.variable.as_str())
            .chain(ranked_nodes.iter().map(|(var, _)| *var))
            .collect::<Vec<_>>();
        for start in starts {
            if component_visited.contains(start) {
                continue;
            }
            let mut queue = VecDeque::new();
            queue.push_back(start);
            let mut component_bindings: Vec<&str> = Vec::new();
            let mut component_vars: Vec<&str> = Vec::new();
            while let Some(var) = queue.pop_front() {
                if !component_visited.insert(var) {
                    continue;
                }
                component_vars.push(var);
                if binding_set.contains(var) {
                    component_bindings.push(var);
                }
                if let Some(neighbours) = adj.get(var) {
                    for &n in neighbours {
                        if !component_visited.contains(n) {
                            queue.push_back(n);
                        }
                    }
                }
            }
            let reaches_outer = component_vars.iter().any(|var| bound_vars.contains(*var));
            let inner_bindings: Vec<&str> = component_bindings
                .iter()
                .copied()
                .filter(|var| !bound_vars.contains(*var))
                .collect();
            let ranked_here: Vec<(&str, &str)> = ranked_nodes
                .iter()
                .copied()
                .filter(|(var, _)| component_vars.contains(var) && !bound_vars.contains(*var))
                .collect();
            let root = match ranked_here[..] {
                [] => scan_root(&inner_bindings, reaches_outer, &searched),
                [(var, type_name)] => {
                    let declared = inner_bindings.iter().position(|binding| *binding == var);
                    if declared.is_none() {
                        ranked_scans.push((component_bindings.first().copied(), var, type_name));
                    }
                    declared
                }
                _ => {
                    return Err(self.refuse_at(
                        "a ranking over two bindings one traversal connects",
                        Construct::Element(FROM, None),
                    ));
                }
            };
            for (index, var) in inner_bindings.into_iter().enumerate() {
                if Some(index) != root {
                    deferred_set.insert(var.to_string());
                }
            }
        }
        let mut deferred_filters: HashMap<String, Vec<IRExpr>> = HashMap::new();
        let mut rebind_filters: Vec<IRExpr> = Vec::new();
        for binding in &bindings {
            ranked_scans.retain(|&(anchor, variable, type_name)| {
                if anchor != Some(binding.variable.as_str()) {
                    return true;
                }
                pipeline.push(PatternStep::NodeScan {
                    variable: variable.to_string(),
                    type_name: type_name.to_string(),
                    filters: Vec::new(),
                });
                bound_vars.insert(variable.to_string());
                false
            });
            let binding_filters = binding.filters.clone();
            if bound_vars.contains(&binding.variable) {
                rebind_filters.extend(binding_filters);
                continue;
            }
            if deferred_set.contains(&binding.variable) {
                if !binding_filters.is_empty() {
                    deferred_filters
                        .entry(binding.variable.clone())
                        .or_default()
                        .extend(binding_filters);
                }
                continue;
            }
            pipeline.push(PatternStep::NodeScan {
                variable: binding.variable.clone(),
                type_name: binding.type_name.clone(),
                filters: binding_filters,
            });
            bound_vars.insert(binding.variable.clone());
        }
        for (_, variable, type_name) in ranked_scans {
            pipeline.push(PatternStep::NodeScan {
                variable: variable.to_string(),
                type_name: type_name.to_string(),
                filters: Vec::new(),
            });
            bound_vars.insert(variable.to_string());
        }
        let mut remaining: Vec<&TraversalFact> = traversals.clone();
        while !remaining.is_empty() {
            let mut next_remaining = Vec::new();
            for traversal in &remaining {
                let src_bound = traversal.src != "_" && bound_vars.contains(&traversal.src);
                let dst_bound = traversal.dst != "_" && bound_vars.contains(&traversal.dst);
                if !src_bound && !dst_bound {
                    next_remaining.push(*traversal);
                    continue;
                }
                let edge_binding = traversal.edge_binding.clone();
                if src_bound && dst_bound {
                    let temp_var = fresh.temp(&traversal.dst);
                    pipeline.push(PatternStep::Expand {
                        src_var: traversal.src.clone(),
                        dst_var: temp_var.clone(),
                        edges: traversal.edges.clone(),
                        src_type: traversal.src_type.clone(),
                        dst_type: traversal.dst_type.clone(),
                        min_hops: traversal.min_hops,
                        max_hops: traversal.max_hops,
                        dst_filters: vec![],
                        edge_binding,
                    });
                    let id = self.catalog.system_columns.id.to_string();
                    let string = scalar(ScalarType::String);
                    pipeline.push(PatternStep::Filter(
                        crate::ir::coerce::binary(
                            IRExpr::PropAccess {
                                variable: temp_var,
                                property: id.clone(),
                                ty: string.clone(),
                            },
                            BinaryOp::Compare(CompOp::Eq),
                            IRExpr::PropAccess {
                                variable: traversal.dst.clone(),
                                property: id,
                                ty: string,
                            },
                        )
                        .map_err(|e| ShapeError::Internal(e.to_string()))?,
                    ));
                } else if !src_bound && dst_bound {
                    let introduced_filters =
                        deferred_filters.remove(&traversal.src).unwrap_or_default();
                    let dst_var = if traversal.src == "_" {
                        fresh.anon()
                    } else {
                        traversal.src.clone()
                    };
                    pipeline.push(PatternStep::Expand {
                        src_var: traversal.dst.clone(),
                        dst_var,
                        edges: traversal.edges.reversed(),
                        src_type: traversal.dst_type.clone(),
                        dst_type: traversal.src_type.clone(),
                        min_hops: traversal.min_hops,
                        max_hops: traversal.max_hops,
                        dst_filters: introduced_filters,
                        edge_binding,
                    });
                    if traversal.src != "_" {
                        bound_vars.insert(traversal.src.clone());
                    }
                } else {
                    let introduced_filters =
                        deferred_filters.remove(&traversal.dst).unwrap_or_default();
                    let dst_var = if traversal.dst == "_" {
                        fresh.anon()
                    } else {
                        traversal.dst.clone()
                    };
                    pipeline.push(PatternStep::Expand {
                        src_var: traversal.src.clone(),
                        dst_var,
                        edges: traversal.edges.clone(),
                        src_type: traversal.src_type.clone(),
                        dst_type: traversal.dst_type.clone(),
                        min_hops: traversal.min_hops,
                        max_hops: traversal.max_hops,
                        dst_filters: introduced_filters,
                        edge_binding,
                    });
                    if traversal.dst != "_" {
                        bound_vars.insert(traversal.dst.clone());
                    }
                }
            }
            if next_remaining.len() == remaining.len() {
                // Neither end bound: the source starts its own pattern.
                let t = next_remaining[0];
                pipeline.push(PatternStep::NodeScan {
                    variable: t.src.clone(),
                    type_name: t.src_type.clone(),
                    filters: Vec::new(),
                });
                bound_vars.insert(t.src.clone());
            }
            remaining = next_remaining;
        }
        pipeline.extend(rebind_filters.into_iter().map(PatternStep::Filter));
        for filter in &facts.filters {
            pipeline.push(PatternStep::Filter(filter.clone()));
        }
        if facts.blocks.is_empty() {
            return Ok(());
        }
        let mut physical_names = outer_physical_names.clone();
        for step in pipeline.iter() {
            match step {
                PatternStep::NodeScan { variable, .. } => {
                    physical_names.insert(variable.clone());
                }
                PatternStep::Expand {
                    dst_var,
                    edge_binding,
                    ..
                } => {
                    physical_names.insert(dst_var.clone());
                    physical_names.extend(edge_binding.iter().cloned());
                }
                PatternStep::Filter(_)
                | PatternStep::AntiJoin { .. }
                | PatternStep::Rank { .. } => {}
            }
        }
        for block in &facts.blocks {
            let visible_outer: HashSet<String> = bound_vars
                .iter()
                .filter(|name| block.outer.contains(*name))
                .cloned()
                .collect();
            let outer_var = find_outer_var(&block.inner, &visible_outer);
            let mut inner_names: Vec<&String> = block
                .inner
                .types
                .keys()
                .chain(block.inner.bindings.iter().map(|b| &b.variable))
                .chain(
                    block
                        .inner
                        .traversals
                        .iter()
                        .filter_map(|t| t.edge_binding.as_ref()),
                )
                .filter(|name| !block.outer.contains(*name) && physical_names.contains(*name))
                .collect();
            inner_names.sort();
            inner_names.dedup();
            let renames: HashMap<String, String> = inner_names
                .into_iter()
                .map(|name| (name.clone(), fresh.temp(name)))
                .collect();
            let mut inner_pipeline = Vec::new();
            let mut inner_bound = visible_outer;
            self.place(
                &block.inner,
                &[],
                &mut inner_pipeline,
                &mut inner_bound,
                &physical_names,
                fresh,
            )?;
            let mut left = block.left.clone();
            rename_inner_bindings(&mut inner_pipeline, left.arg_mut(), &renames);
            let predicate = crate::ir::coerce::block(left, block.op, block.right.clone())
                .map_err(|e| ShapeError::Internal(e.to_string()))?;
            pipeline.push(PatternStep::AntiJoin {
                outer_var: outer_var.unwrap_or_default(),
                inner: inner_pipeline,
                predicate,
            });
        }
        Ok(())
    }
}

fn scalar(s: ScalarType) -> ExprType {
    ExprType::from_prop(&PropType::scalar(s, false))
}

/// The comparison `pred` makes of a block column: `c op right`, or
/// `not (c > 0)` as `c = 0`.
fn block_comparison(pred: &IRExpr, column: &str) -> Option<(CompOp, IRExpr)> {
    let mut conjuncts = pred.clone().into_conjuncts();
    for conjunct in conjuncts.drain(..) {
        if let IRExpr::Not(inner, _) = &conjunct
            && let Some((
                IRExpr::AliasRef(c, _),
                CompOp::Gt,
                IRExpr::Literal(Literal::Integer(0), ty),
            )) = inner.comparison_parts()
            && c == column
        {
            return Some((CompOp::Eq, IRExpr::Literal(Literal::Integer(0), ty.clone())));
        }
        if let Some((left, op, right)) = conjunct.comparison_parts() {
            let left = match left {
                IRExpr::Cast { expr, .. } => expr.as_ref(),
                other => other,
            };
            if matches!(left, IRExpr::AliasRef(c, _) if c == column) {
                let right = match right {
                    IRExpr::Cast { expr, .. } => expr.as_ref().clone(),
                    other => other.clone(),
                };
                return Some((op, right));
            }
        }
    }
    None
}

/// The variable a search predicate filter searches.
fn search_subject(filter: &IRExpr) -> Option<String> {
    let (call, CompOp::Eq, IRExpr::Literal(Literal::Bool(true), _)) = filter.comparison_parts()?
    else {
        return None;
    };
    match call {
        IRExpr::Search { field, .. }
        | IRExpr::Fuzzy { field, .. }
        | IRExpr::MatchText { field, .. } => match field.as_ref() {
            IRExpr::PropAccess { variable, .. } => Some(variable.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn scan_root(
    inner_bindings: &[&str],
    reaches_outer: bool,
    searched: &HashSet<String>,
) -> Option<usize> {
    if reaches_outer {
        inner_bindings
            .iter()
            .position(|var| searched.contains(*var))
    } else {
        inner_bindings.first().map(|_| 0)
    }
}

/// The outer columns a correlated term reads: its `Env` leaf's.
fn env_columns(term: &Term) -> Vec<String> {
    let mut pending = vec![term];
    while let Some(term) = pending.pop() {
        if let Term::Env(cols) = term {
            return cols.iter().map(|c| c.name.clone()).collect();
        }
        pending.extend(term.children());
    }
    Vec::new()
}

/// The `Fuse` a pattern extends from: the leaf its first inputs reach.
fn fuse_leaf(term: &Term) -> Option<&Term> {
    match term {
        Term::Fuse { .. } => Some(term),
        Term::Expand { input, .. } | Term::Filter { input, .. } => fuse_leaf(input),
        Term::Correlate { outer, .. } => fuse_leaf(outer),
        _ => None,
    }
}

/// The node type an `rrf` arm ranks: the scan under its population.
fn population_type(arm: &Term, target: &str) -> Option<String> {
    let mut term = arm;
    loop {
        term = match term {
            Term::ApproxTopK { input, .. }
            | Term::Score { input, .. }
            | Term::Project { input, .. }
            | Term::Filter { input, .. } => input,
            Term::Scan {
                var,
                ty: Ty::Node { type_name, .. },
            } if var == target => return Some(type_name.clone()),
            _ => return None,
        };
    }
}

/// The window an `rrf`'s `nearest` arm draws when the read has no `limit`
/// (today's arm window).
const RRF_NEAREST_ARM_WINDOW: u64 = 100;

/// An aggregate's result as its signature states it: nullable, since an
/// aggregate over no rows has no value.
fn nullable_result(agg: &Agg) -> ExprType {
    match agg.ty.expr_type() {
        ExprType::Value { scalar, list, .. } => ExprType::Value {
            scalar,
            list,
            nullable: true,
        },
        other => other,
    }
}

/// The outer variable a block correlates on: the first its traversals
/// touch, else the first of its bindings that is outer-bound, else the first
/// its filters read. The term places a filter as soon as its variables are
/// bound, so the written order of filters and traversals is not recoverable;
/// a block's traversals come first.
fn find_outer_var(facts: &Facts, outer_bound: &HashSet<String>) -> Option<String> {
    let traversal = facts.traversals.iter().find_map(|t| {
        [&t.src, &t.dst]
            .into_iter()
            .find(|v| outer_bound.contains(*v))
            .cloned()
    });
    let binding = || {
        facts
            .bindings
            .iter()
            .find(|b| outer_bound.contains(&b.variable))
            .map(|b| b.variable.clone())
    };
    let filter = || {
        facts
            .bindings
            .iter()
            .flat_map(|b| b.filters.iter())
            .chain(facts.filters.iter())
            .find_map(|f| {
                let mut cols = Vec::new();
                f.columns(&mut cols);
                cols.into_iter().find(|c| outer_bound.contains(c))
            })
    };
    traversal.or_else(binding).or_else(filter)
}

#[derive(Default)]
struct FreshNames {
    anon: usize,
    temp: usize,
}

impl FreshNames {
    fn anon(&mut self) -> String {
        self.anon += 1;
        format!("{ANON_PREFIX}{}", self.anon)
    }

    fn temp(&mut self, dst: &str) -> String {
        self.temp += 1;
        format!("{FRESH_PREFIX}{}_{}", dst, self.temp)
    }
}

fn rename_inner_bindings(
    pipeline: &mut [PatternStep],
    argument: Option<&mut IRExpr>,
    renames: &HashMap<String, String>,
) {
    if renames.is_empty() {
        return;
    }
    let rename = |variable: &mut String| {
        if let Some(name) = renames.get(variable) {
            *variable = name.clone();
        }
    };
    let mut expressions: Vec<&mut IRExpr> = argument.into_iter().collect();
    let mut pending: Vec<_> = pipeline.iter_mut().collect();
    while let Some(step) = pending.pop() {
        match step {
            PatternStep::NodeScan {
                variable, filters, ..
            } => {
                rename(variable);
                expressions.extend(filters);
            }
            PatternStep::Expand {
                src_var,
                dst_var,
                dst_filters,
                edge_binding,
                ..
            } => {
                rename(src_var);
                rename(dst_var);
                if let Some(binding) = edge_binding {
                    rename(binding);
                }
                expressions.extend(dst_filters);
            }
            PatternStep::Filter(expr) => expressions.push(expr),
            PatternStep::Rank { binding, key, .. } => {
                rename(binding);
                expressions.push(key);
            }
            PatternStep::AntiJoin {
                outer_var,
                inner,
                predicate,
            } => {
                rename(outer_var);
                expressions.extend(predicate.left.arg_mut());
                expressions.push(&mut predicate.right);
                pending.extend(inner);
            }
        }
    }
    for expr in expressions {
        rename_expr(expr, &rename);
    }
}

/// `expr` with every value column a `let` names read as the `let`'s value.
fn substitute(expr: &mut IRExpr, lets: &HashMap<String, IRExpr>) {
    if lets.is_empty() {
        return;
    }
    match expr {
        IRExpr::AliasRef(name, _) | IRExpr::Variable(name, _) => {
            if let Some(value) = lets.get(name.as_str()) {
                *expr = value.clone();
            }
        }
        IRExpr::Nearest { query, .. } => substitute(query, lets),
        IRExpr::Search { field, query, .. }
        | IRExpr::MatchText { field, query, .. }
        | IRExpr::Bm25 { field, query, .. } => {
            substitute(field, lets);
            substitute(query, lets);
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
            ..
        } => {
            substitute(field, lets);
            substitute(query, lets);
            if let Some(m) = max_edits {
                substitute(m, lets);
            }
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
            ..
        } => {
            substitute(primary, lets);
            substitute(secondary, lets);
            if let Some(k) = k {
                substitute(k, lets);
            }
        }
        IRExpr::Aggregate { arg, .. }
        | IRExpr::Not(arg, _)
        | IRExpr::IsNull { expr: arg, .. }
        | IRExpr::Neg { expr: arg, .. }
        | IRExpr::Cast { expr: arg, .. } => substitute(arg, lets),
        IRExpr::Binary { left, right, .. } => {
            substitute(left, lets);
            substitute(right, lets);
        }
        IRExpr::Call { args, .. } => args.iter_mut().for_each(|a| substitute(a, lets)),
        IRExpr::Literal(_, _) | IRExpr::Param(_, _) | IRExpr::PropAccess { .. } => {}
    }
}

fn rename_expr(expr: &mut IRExpr, rename: &impl Fn(&mut String)) {
    match expr {
        IRExpr::PropAccess { variable, .. } | IRExpr::Variable(variable, _) => rename(variable),
        IRExpr::Nearest {
            variable, query, ..
        } => {
            rename(variable);
            rename_expr(query, rename);
        }
        IRExpr::Search { field, query, .. }
        | IRExpr::MatchText { field, query, .. }
        | IRExpr::Bm25 { field, query, .. } => {
            rename_expr(field, rename);
            rename_expr(query, rename);
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
            ..
        } => {
            rename_expr(field, rename);
            rename_expr(query, rename);
            if let Some(m) = max_edits {
                rename_expr(m, rename);
            }
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
            ..
        } => {
            rename_expr(primary, rename);
            rename_expr(secondary, rename);
            if let Some(k) = k {
                rename_expr(k, rename);
            }
        }
        IRExpr::Aggregate { arg, .. }
        | IRExpr::Not(arg, _)
        | IRExpr::IsNull { expr: arg, .. }
        | IRExpr::Neg { expr: arg, .. }
        | IRExpr::Cast { expr: arg, .. } => rename_expr(arg, rename),
        IRExpr::Binary { left, right, .. } => {
            rename_expr(left, rename);
            rename_expr(right, rename);
        }
        IRExpr::Call { args, .. } => args.iter_mut().for_each(|a| rename_expr(a, rename)),
        IRExpr::Literal(_, _) | IRExpr::Param(_, _) | IRExpr::AliasRef(_, _) => {}
    }
}
