//! A core read as the planner lowers it (the GQ 3 RFC,
//! section 11, "One IR").
//!
//! The type checker types one core term per read ([`CoreRead`]). [`ReadShape::of`] decomposes it into what the logical
//! plan is built from: the pattern's scans, expansions, filters and
//! correlated blocks in the order they run, then the return items, the order
//! keys and the limit. A `from` over a ranking (`nearest`, `bm25`, `rrf`)
//! becomes the leading rank key the ranked scan executes, the pattern under
//! it the scan's prefilter. The shape is derived per plan and never stored.
//!
//! The pattern's facts (bindings, triples, filter clauses, blocks) are
//! recovered from the term, then placed: the first binding of each connected
//! component scans and the others are introduced by the expansion that
//! reaches them; a triple whose ends are both bound expands to a fresh name
//! and filters on identity; the filter clauses follow, then the blocks. A
//! core node the planner has no plan node for, or a shape outside this
//! table, is [`Unlowered`]: the type checker refuses it (`T86`) at the
//! construct the refusal names.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::budget::{PlanWork, WorkKind, WorkRecord};
use crate::catalog::Catalog;
use crate::ir::core::{
    Agg, Col, CoreRead, Count, Fresh, FreshRole, LexicalQuery, RRF_DEFAULT_K, ScoreKind,
    ScoreQuery, Term, Ty,
};
use crate::ir::{
    BlockAggregateExpr, IRExpr, IROrdering, IRParam, IRProjection, SubqueryPredicate,
    rrf_weights_expr,
};
use crate::query::file::Param;
use crate::query::value::{
    AggFunc, BinaryOp, CompOp, DISTANCE_COLUMN, Literal, RRF_COLUMN, SCORE_COLUMN,
};
use crate::traversal::{EDGE_TYPE_COLUMN, EdgeMember, EdgeSelection};
use crate::types::{AggSignature, Direction, ExprType, PropType, ScalarType};

use super::{Construct, ElementKind, ProgrammeStage, ShapeError, Unlowered};

/// The property under which a [`PatternStep::Let`]'s binding holds its
/// value: a `let $v = e` is the column `{binding}.value` of a binding the
/// shaper makes up, and every read of `$v` reads that column.
pub const LET_VALUE: &str = "value";

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
    /// `limit $param`: the parameter the cut reads, its count bound at
    /// execution. The engine folds the bound value into `limit` before
    /// planning, so the plan's cut is that value and a replay folds the
    /// value its own value table holds.
    pub limit_param: Option<String>,
    /// The match's columns in written column order (the GQ 3 RFC, section
    /// 2, `Order`'s tie rule): the order the core term adds them, which the
    /// steps' run order need not keep (an `optional` and a `union` run after
    /// the scope's traversals). The tie rule orders reference columns by it.
    pub written_columns: Vec<String>,
    /// Every name the type checker or the shaper made up (an anonymous
    /// triple end, a closing triple's destination, a block's or an inline
    /// read's hidden column): the query declares none of them, so the tie
    /// rule orders their identities after the declared ones. A made-up name
    /// starts with an uppercase letter or `^`, which no written name does.
    pub made_up: std::collections::BTreeSet<String>,
    /// The read's plan budget and the work typing and shaping charged to
    /// it, which planning goes on charging ([`crate::budget`]).
    pub work: WorkRecord,
}

/// The columns `term` adds to its rows, in the order the core term adds
/// them, each once (the reference interpreter's row order).
pub fn written_columns(term: &Term, out: &mut Vec<String>) {
    crate::stack::deep(|| {
        fn push(out: &mut Vec<String>, name: &str) {
            if !out.iter().any(|n| n == name) {
                out.push(name.to_string());
            }
        }
        match term {
            Term::Unit => {}
            Term::Env(cols) => cols.iter().for_each(|c| push(out, &c.name)),
            Term::Scan { var, .. } => push(out, var),
            Term::Expand {
                input,
                dst,
                edge_var,
                ..
            } => {
                written_columns(input, out);
                push(out, dst);
                if let Some(edge) = edge_var {
                    push(out, edge);
                }
            }
            Term::Filter { input, .. } | Term::Order { input, .. } | Term::Cut { input, .. } => {
                written_columns(input, out)
            }
            Term::Project {
                input, items, keep, ..
            } => {
                if *keep {
                    written_columns(input, out);
                } else {
                    out.clear();
                }
                items.iter().for_each(|(name, _)| push(out, name));
            }
            Term::Group { keys, aggs, .. } => {
                out.clear();
                keys.iter().for_each(|(name, _)| push(out, name));
                aggs.iter().for_each(|(name, _)| push(out, name));
            }
            Term::Correlate { outer, inner, .. } => {
                written_columns(outer, out);
                let mut own = Vec::new();
                written_columns(inner, &mut own);
                own.iter().for_each(|name| push(out, name));
            }
            Term::Union { left, .. } => written_columns(left, out),
            Term::Score {
                input, out: score, ..
            }
            | Term::ApproxTopK {
                input, out: score, ..
            } => {
                written_columns(input, out);
                push(out, score);
            }
            Term::Fuse {
                target, out: score, ..
            } => {
                out.clear();
                push(out, target);
                push(out, score);
            }
            Term::Fix { body, .. } | Term::Call { body, .. } => written_columns(body, out),
            Term::RecRef { ty, .. } => ty.cols.iter().for_each(|c| push(out, &c.name)),
        }
    })
}

/// One step of a pattern.
#[derive(Debug, Clone)]
pub enum PatternStep {
    NodeScan {
        variable: String,
        type_name: String,
    },
    Expand {
        src_var: String,
        dst_var: String,
        edges: EdgeSelection,
        src_type: String,
        dst_type: String,
        min_hops: u32,
        max_hops: Option<u32>,
        /// The edge variable: one output row per matching edge, its
        /// properties carried under this prefix.
        edge_binding: Option<String>,
    },
    /// A `where` conjunct over the rows of the steps before it, in written
    /// order, or a closing triple's identity test. `fails`: the conjunct can
    /// fail where it is evaluated, the type checker's flag (A3); the
    /// planner's placement law moves only a conjunct that cannot, and
    /// nothing below one that can.
    Filter {
        pred: IRExpr,
        fails: bool,
    },
    /// A ranking drawn over the rows of the steps before it, its
    /// population, when it is not the read's leading `order` key cut by the
    /// read's `limit` (that ranking is `order_by`'s first key): `key` is the
    /// `nearest` ranking as a leading key holds it, or, over a population
    /// that yields the ranked node as a column of its rows rather than a
    /// binding of its own, the exact distance a `Score` step before it
    /// appended; `binding` the ranked node column, and `window` the `nearest`
    /// window `k` in targets (an `rrf`'s `nearest` arm's). The steps after
    /// it run over the selected rows, which nothing refills.
    Rank {
        key: IRExpr,
        binding: String,
        window: Option<u64>,
        /// `k: $param`: the parameter the window reads, its count bound at
        /// execution. The engine folds the bound value into `window` before
        /// planning, so the plan's window is that value and a replay folds
        /// the value its own value table holds.
        window_param: Option<String>,
        /// `nearest`'s declared `recall:`; `recall: 1` searches every
        /// partition.
        recall: Option<f64>,
    },
    /// A correlated block: `inner` runs over the outer rows and `predicate`
    /// decides per outer row on the aggregate of its matches.
    AntiJoin {
        outer_var: String,
        inner: Vec<PatternStep>,
        predicate: SubqueryPredicate,
        /// The block can fail where it is evaluated (A3).
        fails: bool,
    },
    /// `from union(a, b, …)`: the bag union of the arms, each its own
    /// pipeline from nothing, every arm binding the union's reference
    /// columns, `bindings`, under the first arm's names.
    Union {
        arms: Vec<Vec<PatternStep>>,
        bindings: Vec<String>,
        /// Inside a correlated read's pattern, the arms that read the
        /// enclosing scope each start from the outer rows, an
        /// `OuterReference` on this name; `None` for arms from nothing.
        outer_var: Option<String>,
    },
    /// The core's exact `Score` over every row of the steps before it, the
    /// other columns riding through. `distance(rel, $x.f, q)` appends the
    /// distance of `binding`'s vector property to `query` under the field's
    /// declared distance as the binding's `_distance` column (`F32`); a row
    /// whose vector is null drops. `bm25(rel, $x.f, q)` on a field whose
    /// index declares an analyzer appends `bm25_v1` of `binding`'s String
    /// property against the field corpus as the binding's `_score` column
    /// (`F64`), keeping only the rows whose value matches: `query` is then
    /// the `match_terms(field, terms(…))` predicate the score retains, its
    /// membership. `type_name` is the binding's node type: the table whose
    /// rows are the field corpus, or whose embedding recipe a String query is
    /// encoded with.
    Score {
        binding: String,
        type_name: String,
        property: String,
        kind: ScoreKind,
        query: IRExpr,
    },
    /// `optional { p }`: every outer row with each of its matches in
    /// `inner`, which runs over the outer rows; an outer row with none
    /// stays, the names `inner` introduces null on it.
    Optional {
        outer_var: String,
        inner: Vec<PatternStep>,
        /// The pattern can fail where it is evaluated: it runs on every
        /// outer row, and nothing moves below it (A3).
        fails: bool,
    },
    /// `let $v = value`, the core's `Project` adding one column: `value`
    /// computed once on every row the steps before it produce, as the
    /// [`LET_VALUE`] column of the binding `name`, which the shaper made up
    /// for the `let`; every later read of `$v` reads that column. A value
    /// that can fail refuses the read there (A3), and no filter written
    /// after the `let` runs below this step.
    Let {
        name: String,
        value: IRExpr,
    },
    /// `from` an inline read (or a declared read's call) with its own
    /// aggregate, `distinct`, `order` or `limit`, the core's `Correlate`
    /// over its `Group`, `Order` and `Cut`: `inner` is the read's pattern and
    /// `read` what it returns. With `outer_var`, the inner pattern runs over
    /// the rows of the steps before it, from its `OuterReference` to them,
    /// and the read is evaluated per outer row, each outer row joined to the
    /// rows its own read returns; without one it reads nothing of the
    /// enclosing scope and runs once, joined to every outer row.
    Correlate {
        outer_var: Option<String>,
        inner: Vec<PatternStep>,
        read: InnerRead,
        /// The read can fail where it is evaluated: it runs on every outer
        /// row, and nothing moves below it (A3).
        fails: bool,
    },
    /// `from rrf(arm, …)`, the core's `Fuse` (the GQ 3 RFC,
    /// section 4, Fusion): each arm is its own pipeline from nothing, the
    /// steps the same ranking would take standing alone, ending on the rows
    /// it ranks; every arm binds `target`, a node of `target_type`. Each arm
    /// ranks the distinct targets of its rows in its [`ArmRank`] order from
    /// 1, and the step yields one row per target any arm selected: the
    /// target's columns and `Σ weight / (k + rank)` over the arms that
    /// selected it, summed in written order, as the target's `_rrf` column.
    /// The steps after it extend from the target.
    Fuse {
        arms: Vec<FuseArm>,
        target: String,
        target_type: String,
        /// The rank constant as written (a literal or a parameter bound at
        /// execution); `None` is the default, 60.
        k: Option<IRExpr>,
        /// The weights as written, one per arm; `None` is every arm `1.0`.
        weights: Option<IRExpr>,
    },
}

/// One arm of a [`PatternStep::Fuse`]: its steps and how it ranks.
#[derive(Debug, Clone)]
pub struct FuseArm {
    pub steps: Vec<PatternStep>,
    pub rank: ArmRank,
}

/// How a fusion arm orders its targets, which then take ranks 1, 2, … in
/// that order, each at its first row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArmRank {
    /// `nearest`: its window's rows by distance ascending, then the target's
    /// identity. The arm's last step is its `Rank` step.
    Nearest,
    /// `distance`: the exact distance ascending, then the target's identity.
    /// The arm's last step is its `Score` step.
    Distance,
    /// `bm25`: the exact score descending, then the target's identity. The
    /// arm's last step is its `Score` step.
    Bm25,
    /// An inline read with an `order`: its rows in that order, the tie rule
    /// breaking equal keys. The arm's last step is its uncorrelated
    /// `Correlate`.
    Ordered,
    /// An `rrf`: its fused score descending, then the target's identity.
    /// The arm's step is its `Fuse`.
    Fused,
}

impl ArmRank {
    /// The score column the arm orders by on its target, with its
    /// direction (`true`: descending); `None` for an ordered read, whose
    /// rows arrive in its own order.
    pub fn score(self) -> Option<(&'static str, bool)> {
        match self {
            Self::Nearest | Self::Distance => Some((DISTANCE_COLUMN, false)),
            Self::Bm25 => Some((SCORE_COLUMN, true)),
            Self::Fused => Some((RRF_COLUMN, true)),
            Self::Ordered => None,
        }
    }

    /// The arm's name as explain and diagnostics spell it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Nearest => "nearest",
            Self::Distance => "distance",
            Self::Bm25 => "bm25",
            Self::Ordered => "ordered",
            Self::Fused => "rrf",
        }
    }
}

/// What a `from` over an inline read with its own aggregate, `distinct`,
/// `order` or `limit` returns (the GQ 3 RFC, section 3: an
/// inner `order` and `limit` belong to the inner read, so an outer `where`
/// never widens the inner cut and an outer `limit` never becomes it).
///
/// The read's rows are its pattern's rows, or with `group` one row per
/// group; its columns are computed over those rows, which its `order`
/// sorts, the tie rule breaking equal keys, and its `limit` cuts.
#[derive(Debug, Clone, PartialEq)]
pub struct InnerRead {
    /// The binding whose properties hold the read's value columns in the
    /// enclosing rows, and its group's key and aggregate columns in its own
    /// rows: a value column `n` is `values.n` after the `from`. It names no
    /// node or edge.
    pub values: String,
    /// `Some` when the read groups (an aggregate or `distinct`): its group
    /// keys and aggregates over the pattern's rows, each a column of
    /// `values` under its `column`. A key that is a reference
    /// (`Variable`) groups by its identity and carries its binding's
    /// columns; with no key, the read is one row, over no pattern row too.
    pub group: Option<Vec<IRProjection>>,
    /// The returned columns in written order, each `column` its name in the
    /// enclosing scope: a reference is `Variable` of that name, which the
    /// read's pattern binds; a value is its expression over the read's rows.
    pub columns: Vec<IRProjection>,
    /// The read's own `order` keys over its rows.
    pub order_by: Vec<IROrdering>,
    pub limit: Option<u64>,
    /// `limit $param`: the parameter the cut reads, folded into `limit`
    /// before planning as the read's own `limit $param` is.
    pub limit_param: Option<String>,
}

impl InnerRead {
    /// Every expression the read evaluates.
    pub fn exprs(&self) -> impl Iterator<Item = &IRExpr> {
        self.group
            .iter()
            .flatten()
            .chain(&self.columns)
            .map(|projection| &projection.expr)
            .chain(self.order_by.iter().map(|ordering| &ordering.expr))
    }

    /// Every expression the read evaluates, mutably.
    pub fn exprs_mut(&mut self) -> impl Iterator<Item = &mut IRExpr> {
        self.group
            .iter_mut()
            .flatten()
            .chain(&mut self.columns)
            .map(|projection| &mut projection.expr)
            .chain(self.order_by.iter_mut().map(|ordering| &mut ordering.expr))
    }

    /// The names of the reference columns, which the read's pattern binds.
    pub fn references(&self) -> impl Iterator<Item = &str> {
        self.columns
            .iter()
            .filter(|projection| {
                matches!(
                    projection.expr,
                    IRExpr::Variable(_, ExprType::Node { .. } | ExprType::Edge { .. })
                )
            })
            .map(|projection| projection.column.as_str())
    }
}

impl ReadShape {
    /// The shape's steps, nested ones included, its return items and its
    /// order keys: what one planner pass over its plan is charged
    /// ([`crate::budget`]).
    pub fn size(&self) -> usize {
        fn steps(list: &[PatternStep]) -> usize {
            let mut pending: Vec<&[PatternStep]> = vec![list];
            let mut n = 0;
            while let Some(list) = pending.pop() {
                n += list.len();
                for step in list {
                    match step {
                        PatternStep::AntiJoin { inner, .. }
                        | PatternStep::Optional { inner, .. }
                        | PatternStep::Correlate { inner, .. } => pending.push(inner),
                        PatternStep::Union { arms, .. } => {
                            pending.extend(arms.iter().map(Vec::as_slice))
                        }
                        PatternStep::Fuse { arms, .. } => {
                            pending.extend(arms.iter().map(|a| a.steps.as_slice()))
                        }
                        PatternStep::NodeScan { .. }
                        | PatternStep::Expand { .. }
                        | PatternStep::Filter { .. }
                        | PatternStep::Rank { .. }
                        | PatternStep::Score { .. }
                        | PatternStep::Let { .. } => {}
                    }
                }
            }
            n
        }
        steps(&self.pipeline) + self.return_exprs.len() + self.order_by.len()
    }

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
                PatternStep::AntiJoin { inner, .. }
                | PatternStep::Optional { inner, .. }
                | PatternStep::Correlate { inner, .. } => pending.extend(inner),
                PatternStep::Union { arms, .. } => pending.extend(arms.iter().flatten()),
                PatternStep::Fuse { arms, .. } => {
                    pending.extend(arms.iter().flat_map(|arm| &arm.steps))
                }
                PatternStep::NodeScan { .. }
                | PatternStep::Filter { .. }
                | PatternStep::Rank { .. }
                | PatternStep::Score { .. }
                | PatternStep::Let { .. } => {}
            }
        }
        false
    }

    /// The shape of `read`, or the term this release's planner does not
    /// lower.
    pub fn of(read: &CoreRead, catalog: &Catalog) -> Result<Self, ShapeError> {
        Shaper::new(catalog, read).read(read)
    }

    /// [`ReadShape::of`], and the term nodes the shaper visited.
    #[cfg(test)]
    pub(crate) fn of_measured(
        read: &CoreRead,
        catalog: &Catalog,
    ) -> (Result<Self, ShapeError>, usize) {
        let shaper = Shaper::new(catalog, read);
        let shape = shaper.read(read);
        (shape, shaper.visited.get())
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
    /// `rrf` arms'): a literal, or a parameter bound at execution.
    windows: Vec<Count>,
    /// An exact score: the scored property, the score's kind and its query
    /// (`distance`'s vector, or the `match_terms` predicate an exact `bm25`
    /// retains). It is a step over its population's rows, never a ranked
    /// scan, and its `order` and `limit` are the read's own sort and cut.
    exact: Option<(String, ScoreKind, IRExpr)>,
    /// `nearest`'s declared `recall:`, `None` when omitted or not a
    /// `nearest`.
    recall: Option<f64>,
    /// An `rrf`: a [`PatternStep::Fuse`] of its arms, each its own pipeline.
    fused: bool,
}

impl Ranking {
    /// The names the ranking binds for the elements after it: the ranked
    /// node column and the score's name.
    fn bound_names(&self) -> [String; 2] {
        [self.column.clone(), self.out.clone()]
    }
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
        // A fusion's arms rank one population (`Shaper::fused_population`).
        Term::Fuse { arms, .. } => arms.first().map_or(node, ranked_input),
        other => other,
    }
}

/// `term` with the node `leaf` (found by identity on its input chain)
/// replaced by an empty leaf.
fn without_leaf(term: &Term, leaf: &Term) -> Term {
    crate::stack::deep(|| {
        if std::ptr::eq(term, leaf) {
            return Term::Env(Vec::new());
        }
        match term {
            Term::Filter { input, pred, fails } => Term::Filter {
                input: without_leaf(input, leaf).boxed(),
                pred: pred.clone(),
                fails: *fails,
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
            Term::Project {
                input,
                items,
                keep,
                fails,
            } => Term::Project {
                input: without_leaf(input, leaf).boxed(),
                items: items.clone(),
                keep: *keep,
                fails: *fails,
            },
            Term::Correlate {
                outer,
                inner,
                fails,
            } => Term::Correlate {
                outer: without_leaf(outer, leaf).boxed(),
                inner: inner.clone(),
                fails: *fails,
            },
            other => other.clone(),
        }
    })
}

/// The names a placed pipeline binds physically: its scans, expansion
/// destinations and edge bindings.
fn physical_names(pipeline: &[PatternStep]) -> HashSet<String> {
    crate::stack::deep(|| {
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
                PatternStep::Optional { inner, .. } => names.extend(physical_names(inner)),
                PatternStep::Union { arms, .. } => arms
                    .iter()
                    .for_each(|arm| names.extend(physical_names(arm))),
                PatternStep::Correlate { read, .. } => {
                    names.extend(read.references().map(str::to_string));
                }
                PatternStep::Fuse { target, .. } => {
                    names.insert(target.clone());
                }
                PatternStep::Filter { .. }
                | PatternStep::AntiJoin { .. }
                | PatternStep::Rank { .. }
                | PatternStep::Score { .. }
                | PatternStep::Let { .. } => {}
            }
        }
        names
    })
}

struct Shaper<'a> {
    catalog: &'a Catalog,
    name: &'a str,
    params: &'a [(String, PropType)],
    /// The names the type checker made up, with their roles.
    fresh: &'a Fresh,
    /// The names this shaper made up ([`Shaper::shaped`]).
    minted: std::cell::RefCell<std::collections::BTreeSet<String>>,
    /// The term nodes the shaper visited: the compile-cost growth tests
    /// read it.
    visited: std::cell::Cell<usize>,
    /// The read's plan budget, charged on from where the type checker left
    /// it: one unit per term node visited and one per expression node it
    /// holds, which the shape copies.
    work: PlanWork,
    /// Set while [`Shaper::holds_optional_or_union`] reads one scope's own
    /// facts: a nested inline read is a scope of its own, which contributes
    /// no `optional` or `union` to the scope it is written in, so the probe
    /// does not read it.
    probing: std::cell::Cell<bool>,
    /// How many `let` bindings the shaper made up so far.
    lets_minted: std::cell::Cell<usize>,
}

/// The pattern facts of one scope, in the order the term states them.
#[derive(Default, Debug, Clone)]
struct Facts {
    bindings: Vec<BindingFact>,
    traversals: Vec<TraversalFact>,
    /// The scope's `where` conjuncts in written order, after every element:
    /// the steps keep that order, and the planner's placement law moves the
    /// ones that may move.
    conjuncts: Vec<ConjunctFact>,
    /// The `optional { }` elements, in written order.
    optionals: Vec<OptionalFact>,
    /// The `from union(…)` elements, in written order.
    unions: Vec<UnionFact>,
    /// The `from`s over an inline read with its own aggregate, `distinct`,
    /// `order` or `limit`, in written order.
    correlates: Vec<CorrelateFact>,
    /// The names the `optional`s introduce, which an element after them may
    /// not read: the steps place an `optional` after the scope's bindings
    /// and traversals.
    optional_names: HashSet<String>,
    /// The outer references this scope reads that are null on some rows:
    /// those an enclosing scope's `optional` introduced, as the scope's
    /// `Env` leaf types them.
    nullable_outer: HashSet<String>,
    /// The type of every node variable the scope binds.
    types: HashMap<String, String>,
    /// Names an enclosing step binds that these facts read without
    /// binding: the population's names for the elements after a ranking,
    /// the outer scope's for an `optional`'s own. An `optional` here
    /// introduces none of them.
    outer_names: HashSet<String>,
    /// Each `let` name and the column that holds its value: a read of the
    /// name is a read of that column, never a copy of the value.
    lets: HashMap<String, IRExpr>,
    /// How many inline reads' hidden names were renamed apart so far.
    inlined: usize,
    /// Each `let`, with how many facts of each kind the elements before it
    /// stated: the steps place those first, then the value's column, then
    /// the rest.
    checks: Vec<LetCheck>,
}

/// One `where` conjunct, as the type checker wrote it.
#[derive(Debug, Clone)]
enum ConjunctFact {
    /// A predicate, with the blocks it reads as values, each a correlated
    /// read placed right before it.
    Filter {
        pred: IRExpr,
        fails: bool,
        values: Vec<CorrelateFact>,
    },
    /// A block compared with a value of the outer row: a semi-join.
    Block { block: Box<BlockFact>, fails: bool },
}

/// How many facts of each kind a scope had stated at one point.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
struct Marks {
    bindings: usize,
    traversals: usize,
    conjuncts: usize,
    optionals: usize,
    unions: usize,
    correlates: usize,
}

/// A `let`, which splits a scope's facts at the point it was written: the
/// elements before it are placed, then its value is computed once as a
/// column of every row they produce, then the rest, which read the column.
#[derive(Debug, Clone)]
struct LetCheck {
    /// The binding the shaper made up whose [`LET_VALUE`] column holds the
    /// value.
    name: String,
    value: IRExpr,
    marks: Marks,
}

impl Facts {
    /// How many facts of each kind the scope states so far.
    fn marks(&self) -> Marks {
        Marks {
            bindings: self.bindings.len(),
            traversals: self.traversals.len(),
            conjuncts: self.conjuncts.len(),
            optionals: self.optionals.len(),
            unions: self.unions.len(),
            correlates: self.correlates.len(),
        }
    }

    /// The facts stated between `from` and `to`, with the scope's names and
    /// types; the `let` values are already read into them.
    fn segment(&self, from: Marks, to: Marks) -> Facts {
        Facts {
            bindings: self.bindings[from.bindings..to.bindings].to_vec(),
            traversals: self.traversals[from.traversals..to.traversals]
                .iter()
                .cloned()
                .map(|mut traversal| {
                    traversal.optionals_before =
                        traversal.optionals_before.saturating_sub(from.optionals);
                    traversal
                })
                .collect(),
            conjuncts: self.conjuncts[from.conjuncts..to.conjuncts].to_vec(),
            optionals: self.optionals[from.optionals..to.optionals].to_vec(),
            unions: self.unions[from.unions..to.unions].to_vec(),
            correlates: self.correlates[from.correlates..to.correlates]
                .iter()
                .cloned()
                .map(|mut correlate| {
                    correlate.optionals_before =
                        correlate.optionals_before.saturating_sub(from.optionals);
                    correlate
                })
                .collect(),
            optional_names: self.optional_names.clone(),
            nullable_outer: self.nullable_outer.clone(),
            types: self.types.clone(),
            outer_names: self.outer_names.clone(),
            lets: HashMap::new(),
            inlined: self.inlined,
            checks: Vec::new(),
        }
    }

    /// `other`'s facts after this scope's, its `let` checks at their place
    /// among them. `other` holds no `optional` or `union`, which are placed
    /// after their own scope's elements; such a read is drawn correlated.
    fn append(&mut self, other: Facts) -> Result<(), ShapeError> {
        let Facts {
            bindings,
            traversals,
            conjuncts,
            optionals,
            unions,
            correlates,
            optional_names,
            nullable_outer,
            types,
            outer_names: _,
            lets: _,
            inlined: _,
            checks,
        } = other;
        self.nullable_outer.extend(nullable_outer);
        if !optionals.is_empty() || !unions.is_empty() || !optional_names.is_empty() {
            return Err(ShapeError::Internal(
                "an inline read holding an `optional` or a `union` was joined into its outer scope"
                    .into(),
            ));
        }
        let at = self.marks();
        self.checks.extend(checks.into_iter().map(|mut check| {
            check.marks = Marks {
                bindings: at.bindings + check.marks.bindings,
                traversals: at.traversals + check.marks.traversals,
                conjuncts: at.conjuncts + check.marks.conjuncts,
                // An inline read's facts join the scope's bindings,
                // traversals and conjuncts.
                optionals: at.optionals,
                unions: at.unions,
                correlates: at.correlates + check.marks.correlates,
            };
            check
        }));
        self.correlates
            .extend(correlates.into_iter().map(|mut correlate| {
                correlate.optionals_before = at.optionals;
                correlate
            }));
        self.bindings.extend(bindings);
        self.traversals
            .extend(traversals.into_iter().map(|mut traversal| {
                traversal.optionals_before = at.optionals;
                traversal
            }));
        self.conjuncts.extend(conjuncts);
        self.types.extend(types);
        Ok(())
    }

    /// Every name the facts bind or read as a node or edge variable.
    fn names(&self) -> Vec<String> {
        crate::stack::deep(|| {
            let mut out: Vec<String> = self.types.keys().cloned().collect();
            out.extend(self.bindings.iter().map(|b| b.variable.clone()));
            for t in &self.traversals {
                out.extend([t.src.clone(), t.dst.clone()]);
                out.extend(t.edge_binding.iter().cloned());
            }
            for optional in &self.optionals {
                out.extend(optional.inner.names());
            }
            for union in &self.unions {
                out.extend(union.bindings.iter().map(|(name, _)| name.clone()));
                union.arms.iter().for_each(|arm| out.extend(arm.names()));
            }
            for correlate in &self.correlates {
                out.extend(correlate.read.references().map(str::to_string));
            }
            out.retain(|name| name != "_");
            out
        })
    }

    /// The facts with every variable `renames` names renamed, a block's
    /// included.
    fn rename(&mut self, renames: &HashMap<String, String>) {
        crate::stack::deep(|| {
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
            }
            for t in &mut self.traversals {
                rename(&mut t.src);
                rename(&mut t.dst);
                if let Some(edge) = &mut t.edge_binding {
                    rename(edge);
                }
            }
            self.checks
                .iter_mut()
                .for_each(|c| rename_expr(&mut c.value, &rename));
            self.types = std::mem::take(&mut self.types)
                .into_iter()
                .map(|(mut name, ty)| {
                    rename(&mut name);
                    (name, ty)
                })
                .collect();
            for conjunct in &mut self.conjuncts {
                match conjunct {
                    ConjunctFact::Filter { pred, values, .. } => {
                        rename_expr(pred, &rename);
                        values
                            .iter_mut()
                            .for_each(|correlate| correlate.rename(renames));
                    }
                    ConjunctFact::Block { block, .. } => {
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
            }
            for optional in &mut self.optionals {
                optional.inner.rename(renames);
            }
            for union in &mut self.unions {
                union.arms.iter_mut().for_each(|arm| arm.rename(renames));
                union.bindings.iter_mut().for_each(|(name, _)| rename(name));
                union.outer = std::mem::take(&mut union.outer)
                    .into_iter()
                    .map(|mut name| {
                        rename(&mut name);
                        name
                    })
                    .collect();
            }
            for correlate in &mut self.correlates {
                correlate.rename(renames);
            }
            self.optional_names = std::mem::take(&mut self.optional_names)
                .into_iter()
                .map(|mut name| {
                    rename(&mut name);
                    name
                })
                .collect();
        })
    }

    /// Every name an `optional` of the facts introduces, nested ones and
    /// those of a block's or an optional's own included.
    fn optional_names_into(&self, out: &mut HashSet<String>) {
        crate::stack::deep(|| {
            out.extend(self.optional_names.iter().cloned());
            for optional in &self.optionals {
                optional.inner.optional_names_into(out);
            }
            for conjunct in &self.conjuncts {
                match conjunct {
                    ConjunctFact::Filter { values, .. } => values
                        .iter()
                        .for_each(|correlate| correlate.inner.optional_names_into(out)),
                    ConjunctFact::Block { block, .. } => block.inner.optional_names_into(out),
                }
            }
            for union in &self.unions {
                union
                    .arms
                    .iter()
                    .for_each(|arm| arm.optional_names_into(out));
            }
            for correlate in &self.correlates {
                correlate.inner.optional_names_into(out);
            }
        })
    }

    /// `f` over every expression the facts hold, a block's and an
    /// optional's included.
    fn each_expr(&mut self, f: &mut dyn FnMut(&mut IRExpr)) {
        crate::stack::deep(|| {
            self.lets.values_mut().for_each(&mut *f);
            self.checks.iter_mut().for_each(|c| f(&mut c.value));
            for conjunct in &mut self.conjuncts {
                match conjunct {
                    ConjunctFact::Filter { pred, values, .. } => {
                        f(pred);
                        for correlate in values {
                            correlate.inner.each_expr(f);
                            correlate.read.exprs_mut().for_each(&mut *f);
                        }
                    }
                    ConjunctFact::Block { block, .. } => {
                        block.inner.each_expr(f);
                        if let Some(arg) = block.left.arg_mut() {
                            f(arg);
                        }
                        f(&mut block.right);
                    }
                }
            }
            for optional in &mut self.optionals {
                optional.inner.each_expr(f);
            }
            for union in &mut self.unions {
                union.arms.iter_mut().for_each(|arm| arm.each_expr(f));
            }
            for correlate in &mut self.correlates {
                correlate.inner.each_expr(f);
                correlate.read.exprs_mut().for_each(&mut *f);
            }
        })
    }

    /// Every expression the facts hold, a block's included, with each name
    /// `lets` maps read as the column it maps to.
    fn resolve(&mut self, lets: &HashMap<String, IRExpr>) {
        crate::stack::deep(|| {
            if lets.is_empty() {
                return;
            }
            self.checks
                .iter_mut()
                .for_each(|c| resolve(&mut c.value, lets));
            for conjunct in &mut self.conjuncts {
                match conjunct {
                    ConjunctFact::Filter { pred, values, .. } => {
                        resolve(pred, lets);
                        for correlate in values {
                            correlate.inner.resolve(lets);
                            correlate.read.exprs_mut().for_each(|e| resolve(e, lets));
                        }
                    }
                    ConjunctFact::Block { block, .. } => {
                        block.inner.resolve(lets);
                        if let Some(arg) = block.left.arg_mut() {
                            resolve(arg, lets);
                        }
                        resolve(&mut block.right, lets);
                    }
                }
            }
            for optional in &mut self.optionals {
                optional.inner.resolve(lets);
            }
            // A union's arms read the scope's `let`s and value columns in place.
            for union in &mut self.unions {
                union.arms.iter_mut().for_each(|arm| arm.resolve(lets));
            }
            for correlate in &mut self.correlates {
                correlate.inner.resolve(lets);
                correlate.read.exprs_mut().for_each(|e| resolve(e, lets));
            }
        })
    }
}

#[derive(Debug, Clone)]
struct BindingFact {
    variable: String,
    type_name: String,
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
    /// How many of the scope's `optional`s were written before it: a
    /// triple from a correlated read's column expands before the
    /// `optional`s written after it, which correlate on its ends.
    optionals_before: usize,
}

/// A `from union(…)`: each arm's facts, every arm binding the union's
/// reference columns under the first arm's names, with their node types.
#[derive(Debug, Clone)]
struct UnionFact {
    arms: Vec<Facts>,
    bindings: Vec<(String, String)>,
    /// The `values` binding every arm's value columns belong to, when the
    /// arms return values.
    values: Option<String>,
    /// The enclosing scope's names the arms read: they run inside a
    /// correlated read, each from its rows.
    outer: HashSet<String>,
}

/// An `optional { }`: its pattern's facts, and whether its pattern can
/// fail (the type checker's flag on its `Correlate`).
#[derive(Debug, Clone)]
struct OptionalFact {
    inner: Facts,
    fails: bool,
}

/// A `from` over an inline read with its own aggregate, `distinct`, `order`
/// or `limit`: the read's pattern facts, the outer names it reads, what it
/// returns, and how many `optional`s of the scope were written before it.
#[derive(Debug, Clone)]
struct CorrelateFact {
    inner: Facts,
    outer: HashSet<String>,
    read: InnerRead,
    optionals_before: usize,
    /// The read can fail (the type checker's flag on its `Correlate`).
    fails: bool,
}

impl CorrelateFact {
    /// The read with every variable `renames` names renamed.
    fn rename(&mut self, renames: &HashMap<String, String>) {
        crate::stack::deep(|| {
            let rename = |variable: &mut String| {
                if let Some(name) = renames.get(variable) {
                    *variable = name.clone();
                }
            };
            self.inner.rename(renames);
            self.outer = std::mem::take(&mut self.outer)
                .into_iter()
                .map(|mut name| {
                    rename(&mut name);
                    name
                })
                .collect();
            for column in &mut self.read.columns {
                if is_reference(&column.expr) {
                    rename(&mut column.column);
                }
            }
            self.read.exprs_mut().for_each(|e| rename_expr(e, &rename));
        })
    }
}

#[derive(Debug, Clone)]
struct BlockFact {
    inner: Facts,
    /// The outer columns the block may correlate on.
    outer: HashSet<String>,
    left: BlockAggregateExpr,
    op: CompOp,
    right: IRExpr,
}

/// Whether `expr` is a reference: a node or edge variable.
fn is_reference(expr: &IRExpr) -> bool {
    matches!(
        expr,
        IRExpr::Variable(_, ExprType::Node { .. } | ExprType::Edge { .. })
    )
}

impl<'a> Shaper<'a> {
    fn new(catalog: &'a Catalog, read: &'a CoreRead) -> Self {
        let work = PlanWork::resume(*read.work.0);
        work.phase(format!("lowering `{}`", read.name));
        Self {
            catalog,
            name: &read.name,
            params: &read.params,
            fresh: &read.fresh,
            minted: Default::default(),
            visited: Default::default(),
            work,
            probing: Default::default(),
            lets_minted: Default::default(),
        }
    }

    /// Charge `n` units of `kind` to the read's plan budget.
    fn spend(&self, kind: WorkKind, n: usize) -> Result<(), ShapeError> {
        self.work
            .charge(kind, n)
            .map_err(|overrun| ShapeError::OverBudget {
                name: self.name.to_string(),
                overrun,
            })
    }

    /// A name this shaper makes up, `Shaped` and then `suffix`: it starts
    /// with an uppercase letter, so it collides with no written name, and
    /// the word keeps it apart from the type checker's (`Fresh`).
    fn shaped(&self, suffix: impl std::fmt::Display) -> String {
        let name = format!("Shaped{suffix}");
        self.minted.borrow_mut().insert(name.clone());
        name
    }

    /// Whether `name` is an anonymous triple end.
    fn is_anon(&self, name: &str) -> bool {
        self.fresh.is(name, FreshRole::Anonymous)
    }

    /// Whether `name` is the column a block read as a value fills.
    fn is_block_column(&self, name: &str) -> bool {
        self.fresh.is(name, FreshRole::Block)
    }

    /// Whether `name` is the fresh destination of a triple closing on two
    /// bound ends.
    fn is_semi(&self, name: &str) -> bool {
        self.fresh.is(name, FreshRole::Closing)
    }

    /// A parameter's declared type.
    fn param_type(&self, name: &str) -> ExprType {
        self.params
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, ty)| ExprType::from_prop(ty))
            .unwrap_or_else(|| scalar(ScalarType::I64))
    }

    /// The identity keys of an order key that is a reference, a binding or
    /// a return item holding one: `(@type, @id)` for an edge whose selector
    /// has several types, `@id` otherwise (one type: the type is constant).
    /// Over groups the rows carry only the return items, so a node key is
    /// its returned node object, whose first member is `@id`, a unique
    /// key: ordering by the object is ordering by the identity. `None` for
    /// a value key.
    fn reference_key(
        &self,
        key: &IRExpr,
        return_exprs: &[IRProjection],
        grouped: bool,
    ) -> Option<Vec<IRExpr>> {
        let returned =
            |found: &dyn Fn(&IRProjection) -> bool| return_exprs.iter().find(|p| found(p));
        let (reference, item) = match key {
            IRExpr::Variable(..) => (key, returned(&|p| &p.expr == key)),
            IRExpr::AliasRef(alias, ExprType::Node { .. } | ExprType::Edge { .. }) => {
                let item = returned(&|p| p.alias.as_deref() == Some(alias.as_str()))?;
                (&item.expr, Some(item))
            }
            _ => return None,
        };
        let IRExpr::Variable(variable, ty) = reference else {
            return None;
        };
        if grouped {
            let item = item?;
            return matches!(ty, ExprType::Node { .. }).then(|| {
                vec![IRExpr::AliasRef(
                    item.alias.clone().unwrap_or_else(|| item.column.clone()),
                    ty.clone(),
                )]
            });
        }
        reference_identity(variable, ty, self.catalog.system_columns.id)
    }

    /// A refusal at `construct`.
    fn refuse_at(&self, what: impl std::fmt::Display, construct: Construct) -> ShapeError {
        unsupported(self.name, what, construct)
    }

    /// A core node with no plan node in a pattern, at the `from` it was
    /// written as: a ranking that is not the read's own, or a ranking's
    /// population that groups, orders or cuts.
    fn unlowered_node(&self, node: &Term) -> ShapeError {
        let what = match node {
            Term::Group { .. } => {
                "an inline read with an aggregate or `distinct` (a `Group` in a pattern)".into()
            }
            Term::Order { .. } => "an inline read with an `order` (an `Order` in a pattern)".into(),
            Term::Cut { .. } => "an inline read with a `limit` (a `Cut` in a pattern)".into(),
            other => format!("a `{}` node in a pattern", other.operator()),
        };
        self.refuse_at(what, Construct::Element(FROM, None))
    }

    fn read(&self, read: &CoreRead) -> Result<ReadShape, ShapeError> {
        // A recursive read types to `Fix`, which stage Rec evaluates.
        if read
            .term
            .any_node(|n| matches!(n, Term::Fix { .. } | Term::RecRef { .. }))
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
            ..
        } = &read.term
        else {
            return Err(ShapeError::Internal(
                "a read without its final projection, which the type checker always writes".into(),
            ));
        };
        let mut term: &Term = input;
        let mut limit = None;
        let mut limit_param = None;
        if let Term::Cut { input, k } = term {
            match k {
                Count::Literal(n) => limit = Some(*n),
                Count::Param(p) => limit_param = Some(p.clone()),
            }
            term = input;
        }
        let mut order_keys: &[IROrdering] = &[];
        if let Term::Order { input, keys } = term {
            order_keys = keys;
            term = input;
        }
        let mut written = Vec::new();
        written_columns(term, &mut written);
        // The items, by their hidden names, and the body under them.
        let mut items: HashMap<String, IRExpr> = HashMap::new();
        let mut aggs: HashMap<String, Agg> = HashMap::new();
        let mut distinct = false;
        let body = match term {
            Term::Project {
                input,
                items: extended,
                keep: true,
                ..
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
            _ => {
                return Err(ShapeError::Internal(
                    "a read whose returns are not a projection: the type checker writes a `Group` or an extending `Project` under the final projection".into(),
                ));
            }
        };
        let Split {
            population: body,
            ranking,
            node: ranked_node,
            after,
        } = self
            .split_ranking(body)
            .map_err(|e| e.at(Construct::Element(FROM, None)))?;
        // The pattern, with every `let` value read where its name is. A
        // fusion's arms are each placed on their own (`fuse_step`).
        let fused = ranking.as_ref().is_some_and(|ranking| ranking.fused);
        let mut facts = Facts::default();
        if !fused {
            self.facts(body, &mut facts)?;
        }
        let mut after_facts = Facts {
            outer_names: facts.names().into_iter().collect(),
            ..Facts::default()
        };
        if let Some(ranking) = &ranking {
            after_facts.outer_names.extend(ranking.bound_names());
        }
        if let Some(after) = &after {
            self.facts(after, &mut after_facts)?;
        }
        let mut lets = std::mem::take(&mut facts.lets);
        lets.extend(std::mem::take(&mut after_facts.lets));
        facts.resolve(&lets);
        // The elements and `where` after a ranking read its `let`s, and its
        // blocks' values, in place too.
        after_facts.resolve(&lets);
        for e in items.values_mut() {
            resolve(e, &lets);
        }
        for agg in aggs.values_mut() {
            if let Some(arg) = &mut agg.arg {
                resolve(arg, &lets);
            }
        }
        let order_keys: Vec<IROrdering> = order_keys
            .iter()
            .map(|key| {
                let mut key = key.clone();
                resolve(&mut key.expr, &lets);
                key
            })
            .collect();
        // `$x is null` over a reference tests its identity, which an
        // `optional` leaves null on an outer row with no match.
        let mut optional_names = HashSet::new();
        facts.optional_names_into(&mut optional_names);
        after_facts.optional_names_into(&mut optional_names);
        let id = self.catalog.system_columns.id;
        let mut null_tests = |e: &mut IRExpr| reference_null_tests(e, id, &optional_names);
        facts.each_expr(&mut null_tests);
        after_facts.each_expr(&mut null_tests);
        items.values_mut().for_each(&mut null_tests);
        aggs.values_mut()
            .filter_map(|agg| agg.arg.as_mut())
            .for_each(&mut null_tests);
        let mut order_keys = order_keys;
        order_keys
            .iter_mut()
            .for_each(|key| null_tests(&mut key.expr));
        let order_keys = order_keys.as_slice();
        // Return items.
        let mut return_exprs = Vec::new();
        for (i, (column, expr)) in finals.iter().enumerate() {
            let (IRExpr::AliasRef(hidden, ty) | IRExpr::Variable(hidden, ty)) = expr else {
                return Err(ShapeError::Internal(format!(
                    "result column `{column}` is not a column of the extended rows, which the type checker's final projection always reads"
                )));
            };
            // A node object has no null in this release's result types, and
            // an `optional`'s name is null on a row it kept without a match.
            if let Some(IRExpr::Variable(variable, ExprType::Node { .. })) = items.get(hidden)
                && optional_names.contains(variable)
            {
                return Err(self.refuse_at(
                    "a node an `optional` introduces returned whole (a nullable node object)",
                    Construct::ReturnItem(i),
                ));
            }
            // The public result has a node object and no edge object: an
            // edge reference is read through its fields (`$k.@id`, `$k.@src`,
            // `$k.@dst`, `$k.@type`, its properties).
            if let Some(IRExpr::Variable(_, ExprType::Edge { .. })) = items.get(hidden) {
                return Err(self.refuse_at(
                    "an edge reference returned whole (the result has no edge object)",
                    Construct::ReturnItem(i),
                ));
            }
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
            if holds_lexical_call(&item) {
                return Err(self.refuse_at(
                    "a full-text call read as a value in `return`",
                    Construct::ReturnItem(i),
                ));
            }
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
        // A cut read from a parameter has no count yet, so no ranking can
        // take it for its window. A declared `recall:` is carried by the
        // `Rank` step, which records it.
        let windowed = ranking.as_ref().is_some_and(|ranking| {
            after.is_some()
                || ranking.fused
                || ranking.exact.is_some()
                || ranking.recall.is_some()
                || limit_param.is_some()
                || !self.ranked_scan_runs(ranking, order_keys, limit, grouped)
        });
        let mut order_by = Vec::new();
        let mut keys = order_keys.iter().enumerate().peekable();
        if let Some(ranking) = ranking.as_ref().filter(|_| !windowed) {
            let leads = matches!(keys.peek(), Some((_, IROrdering { expr: IRExpr::AliasRef(name, _), .. })) if *name == ranking.out);
            let (descending, nulls_first) = match keys.peek() {
                Some((_, key)) if leads => (key.descending, key.nulls_first),
                _ => (ranking.descending, None),
            };
            if leads {
                keys.next();
            }
            order_by.push(IROrdering {
                expr: ranking.key.clone(),
                descending,
                nulls_first,
            });
        }
        for (position, written) in keys {
            let key = &written.expr;
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
                                    ShapeError::Internal(
                                        "an aggregate order key no return item repeats, which the type checker refuses (T42)".to_string(),
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
            if holds_lexical_call(&expr) {
                return Err(self.refuse_at("a full-text call read as a value in `order`", at));
            }
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
            // Over a read's rows a computed key no return item computes is
            // evaluated beside the return items and sorted on; over groups
            // the type checker binds every key to a return item.
            let expr = match return_exprs.iter().find(|p| p.expr == expr) {
                Some(item) if computed => IRExpr::AliasRef(
                    item.alias.clone().unwrap_or_else(|| item.column.clone()),
                    item.ty.clone(),
                ),
                None if computed && grouped => {
                    return Err(ShapeError::Internal(
                        "an order key over groups that no return item computes, which the type checker refuses (T42)".to_string(),
                    ));
                }
                _ => expr,
            };
            // A reference orders by its identity, returned or not: the edge
            // type first for an edge over several types, then the id.
            if let Some(reference) = self.reference_key(&expr, &return_exprs, grouped) {
                order_by.extend(reference.into_iter().map(|expr| IROrdering {
                    expr,
                    descending: written.descending,
                    nulls_first: written.nulls_first,
                }));
                continue;
            }
            order_by.push(IROrdering {
                expr,
                descending: written.descending,
                nulls_first: written.nulls_first,
            });
        }
        let ranked: Vec<String> = ranking
            .as_ref()
            .filter(|r| r.exact.is_none())
            .map(|r| vec![r.column.clone()])
            .unwrap_or_default();
        let mut pipeline = Vec::new();
        let mut fresh = FreshNames::default();
        let mut bound = HashSet::new();
        if let Some(ranking) = ranking.as_ref().filter(|_| windowed) {
            let mut inlined = 0;
            match ranked_node {
                Some(fuse @ Term::Fuse { .. }) if ranking.fused => {
                    pipeline.push(self.fuse_step(fuse, &mut inlined, &mut fresh)?);
                    bound.insert(ranking.column.clone());
                }
                _ => self.ranking_steps(
                    ranked_node,
                    body,
                    ranking,
                    &mut inlined,
                    &mut pipeline,
                    &mut bound,
                    &mut fresh,
                )?,
            }
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
            after_facts.resolve(&score);
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
        // The read's own projection, cut and order, and the expressions of
        // what it returns and orders by, which the shape copies.
        self.spend(WorkKind::Shaping, 3)?;
        self.spend(
            WorkKind::ExprNodes,
            return_exprs
                .iter()
                .map(|p| p.expr.nodes())
                .chain(order_by.iter().map(|o| o.expr.nodes()))
                .sum(),
        )?;
        Ok(ReadShape {
            name: read.name.clone(),
            work: self.work.record(),
            params: read
                .params
                .iter()
                .map(|(name, ty)| IRParam {
                    declaration: Param {
                        name: name.clone(),
                        type_name: ty.display_name().trim_end_matches('?').to_string(),
                        nullable: ty.nullable,
                        default: read
                            .defaults
                            .iter()
                            .find(|(declared, _)| declared == name)
                            .map(|(_, value)| value.clone()),
                    },
                    ty: ExprType::from_prop(ty),
                })
                .collect(),
            pipeline,
            return_exprs,
            distinct,
            order_by,
            limit,
            limit_param,
            written_columns: written,
            made_up: read
                .fresh
                .names()
                .map(str::to_string)
                .chain(self.minted.borrow().iter().cloned())
                .chain(fresh.minted)
                .collect(),
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
        crate::stack::deep(|| {
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
                // An aggregate reaches a return item under any operator: unary
                // `-`, a call (`coalesce(sum(…), 0)`) and arithmetic alike.
                other => other.try_map_children(|child| self.item(child, aggs, ranking))?,
            })
        })
    }

    /// Whether today's ranked scan runs the ranking as the read's leading
    /// `order` key cut by the read's `limit`: the score leads the `order` in
    /// its ranking's direction (absent over groups, which it does not
    /// order), and its `nearest` window is the read's `limit`. Any other
    /// ranking is a `Rank` step over its population, and an `rrf` is a
    /// `Fuse` step.
    fn ranked_scan_runs(
        &self,
        ranking: &Ranking,
        order_keys: &[IROrdering],
        limit: Option<u64>,
        grouped: bool,
    ) -> bool {
        let reads_score =
            |e: &IRExpr| matches!(e, IRExpr::AliasRef(name, _) if *name == ranking.out);
        let leads = match order_keys.first() {
            // The score leads, in its ranking's direction and null
            // placement.
            Some(key) if reads_score(&key.expr) => {
                key.descending == ranking.descending && key.nulls_first.is_none()
            }
            // The score is a later key.
            _ if order_keys.iter().any(|k| reads_score(&k.expr)) => false,
            // Another key leads.
            Some(_) => grouped,
            // No `order`: the cut takes the ranking's own order.
            None => grouped || limit.is_none(),
        };
        leads
            && ranking.windows.iter().all(|w| match w {
                Count::Literal(k) => Some(*k) == limit,
                Count::Param(_) => false,
            })
    }

    /// A `Group` aggregate as the aggregate expression a return item holds.
    fn aggregate(&self, agg: &Agg, ranking: Option<&Ranking>) -> Result<IRExpr, ShapeError> {
        let Some(arg) = &agg.arg else {
            return Err(ShapeError::Internal(
                "a row count without an argument in a read's aggregates: `count` takes one argument (T8)".into(),
            ));
        };
        let mut read = Vec::new();
        arg.columns(&mut read);
        let mut arg = arg.clone();
        match ranking {
            // A ranking's score is a column of every row it keeps, which the
            // aggregate reads like any other.
            Some(r) if read.contains(&r.out) => {
                let mut score = HashMap::new();
                score.insert(
                    r.out.clone(),
                    IRExpr::PropAccess {
                        variable: r.column.clone(),
                        property: r.score.0.to_string(),
                        ty: r.score.1.clone(),
                    },
                );
                resolve(&mut arg, &score);
            }
            _ => {}
        }
        let result = nullable_result(agg);
        Ok(IRExpr::Aggregate {
            func: agg.func,
            distinct: agg.distinct,
            arg: Box::new(arg.clone()),
            fraction: agg.fraction.clone().map(Box::new),
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
                node: Some(body),
                after: None,
            });
        }
        let mut node = body;
        loop {
            node = match node {
                Term::ApproxTopK { .. } | Term::Score { .. } | Term::Fuse { .. } => break,
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
                recall,
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
                    windows: vec![k.clone()],
                    exact: None,
                    recall: *recall,
                    fused: false,
                }),
            )),
            // `bm25` is the exact score: `bm25_v1` over the field corpus,
            // keeping the rows its `terms(…)` value matches, a step over its
            // population. Every full-text field has a profile, so the exact
            // lexical scan answers it at every index state.
            Term::Score {
                column,
                property,
                kind: ScoreKind::Bm25,
                query: ScoreQuery::Lexical(lexical),
                input,
                out,
            } => Ok((
                self.population(input)?,
                Some(Ranking {
                    key: IRExpr::PropAccess {
                        variable: column.clone(),
                        property: SCORE_COLUMN.to_string(),
                        ty: scalar(ScalarType::F64),
                    },
                    out: out.clone(),
                    column: column.clone(),
                    score: (SCORE_COLUMN, scalar(ScalarType::F64)),
                    descending: true,
                    windows: Vec::new(),
                    exact: Some((
                        property.clone(),
                        ScoreKind::Bm25,
                        match_terms_call(
                            IRExpr::PropAccess {
                                variable: column.clone(),
                                property: property.clone(),
                                ty: self.field_type(input, column, property),
                            },
                            lexical,
                        ),
                    )),
                    recall: None,
                    fused: false,
                }),
            )),
            Term::Score {
                input,
                column,
                property,
                kind: ScoreKind::Distance,
                query: ScoreQuery::Vector(query),
                out,
            } => Ok((
                self.population(input)?,
                Some(Ranking {
                    key: IRExpr::PropAccess {
                        variable: column.clone(),
                        property: DISTANCE_COLUMN.to_string(),
                        ty: scalar(ScalarType::F32),
                    },
                    out: out.clone(),
                    column: column.clone(),
                    score: (DISTANCE_COLUMN, scalar(ScalarType::F32)),
                    descending: false,
                    windows: Vec::new(),
                    exact: Some((property.clone(), ScoreKind::Distance, query.clone())),
                    recall: None,
                    fused: false,
                }),
            )),
            Term::Score { .. } => Err(ShapeError::Internal(
                "a `Score` whose kind and query disagree, which the type checker never writes"
                    .into(),
            )),
            // `rrf`: each arm is shaped as its own pipeline
            // (`Shaper::fuse_step`); the fusion's targets are the rows the
            // elements after the `from` extend.
            Term::Fuse { target, out, .. } => Ok((
                body,
                Some(Ranking {
                    key: IRExpr::PropAccess {
                        variable: target.clone(),
                        property: RRF_COLUMN.to_string(),
                        ty: scalar(ScalarType::F64),
                    },
                    out: out.clone(),
                    column: target.clone(),
                    score: (RRF_COLUMN, scalar(ScalarType::F64)),
                    descending: true,
                    windows: Vec::new(),
                    exact: None,
                    recall: None,
                    fused: true,
                }),
            )),
            other => Ok((other, None)),
        }
    }

    /// The steps of one ranking drawn as a pattern step: its population
    /// (`population`, the pattern under its node `node`) placed as its
    /// inline read states it, the ranked binding scanned first under a
    /// `nearest` and the names it binds without returning renamed apart,
    /// then its `Rank` step (a `nearest` window) or `Score` step (an exact
    /// score). A ranking standing alone and a fusion arm take the same
    /// steps. `inlined` numbers the renamed names across the read.
    #[allow(clippy::too_many_arguments)]
    fn ranking_steps(
        &self,
        node: Option<&Term>,
        population: &Term,
        ranking: &Ranking,
        inlined: &mut usize,
        pipeline: &mut Vec<PatternStep>,
        bound: &mut HashSet<String>,
        fresh: &mut FreshNames,
    ) -> Result<(), ShapeError> {
        let mut facts = Facts {
            inlined: *inlined,
            ..Facts::default()
        };
        match node.map(ranked_input) {
            Some(Term::Project {
                input,
                items,
                keep: false,
                ..
            }) => self.inline_read(input, items, &[], &mut facts)?,
            _ => self.facts(population, &mut facts)?,
        }
        *inlined = facts.inlined;
        let lets = std::mem::take(&mut facts.lets);
        facts.resolve(&lets);
        // A `nearest` window searches its binding's scan where the
        // population binds the ranked node as a binding of its own. A
        // population drawn as a read of its own (one holding an `optional`
        // or a `union`) yields the node as a column of its rows: the window
        // ranks those rows by their exact distance, whatever the shape.
        let over_rows = match (&ranking.exact, &ranking.key) {
            (None, IRExpr::Nearest { .. }) => {
                !facts.bindings.iter().any(|b| b.variable == ranking.column)
            }
            _ => false,
        };
        let ranked: Vec<String> = match ranking.exact {
            None if !over_rows => vec![ranking.column.clone()],
            _ => Vec::new(),
        };
        self.place(&facts, &ranked, pipeline, bound, &HashSet::new(), fresh)?;
        let type_name = |facts: &Facts| {
            facts.types.get(&ranking.column).cloned().ok_or_else(|| {
                ShapeError::Internal(format!(
                    "an exact score ranks `{}`, which its population binds as no node",
                    ranking.column
                ))
            })
        };
        let key = match &ranking.key {
            IRExpr::Nearest {
                variable,
                property,
                query,
                ..
            } if over_rows => {
                pipeline.push(PatternStep::Score {
                    binding: variable.clone(),
                    type_name: type_name(&facts)?,
                    property: property.clone(),
                    kind: ScoreKind::Distance,
                    query: query.as_ref().clone(),
                });
                IRExpr::PropAccess {
                    variable: variable.clone(),
                    property: DISTANCE_COLUMN.to_string(),
                    ty: scalar(ScalarType::F32),
                }
            }
            key => key.clone(),
        };
        pipeline.push(match &ranking.exact {
            Some((property, kind, query)) => PatternStep::Score {
                binding: ranking.column.clone(),
                type_name: type_name(&facts)?,
                property: property.clone(),
                kind: *kind,
                query: query.clone(),
            },
            None => {
                // A window read from a parameter takes its bound count when
                // the engine gathers the planning inputs.
                let (window, window_param) = match ranking.windows.as_slice() {
                    [] => (None, None),
                    [Count::Literal(k)] => (Some(*k), None),
                    [Count::Param(p)] => (None, Some(p.clone())),
                    _ => {
                        return Err(ShapeError::Internal(
                            "a `nearest` with more than one window".to_string(),
                        ));
                    }
                };
                PatternStep::Rank {
                    key,
                    binding: ranking.column.clone(),
                    window,
                    window_param,
                    recall: ranking.recall,
                }
            }
        });
        Ok(())
    }

    /// `rrf(arm, …)` as a [`PatternStep::Fuse`]: each arm its own pipeline
    /// from nothing, the steps the same ranking takes standing alone
    /// (`ranking_steps`), or an inline read whose own `order` ranks its
    /// targets, placed as a `from` over it is.
    fn fuse_step(
        &self,
        fuse: &Term,
        inlined: &mut usize,
        fresh: &mut FreshNames,
    ) -> Result<PatternStep, ShapeError> {
        crate::stack::deep(|| {
            let Term::Fuse {
                arms,
                target,
                k,
                weights,
                ..
            } = fuse
            else {
                return Err(ShapeError::Internal("a fusion that is not a `Fuse`".into()));
            };
            let mut shaped = Vec::with_capacity(arms.len());
            for arm in arms {
                let mut steps = Vec::new();
                let mut bound = HashSet::new();
                let rank = match self.ranking(arm)? {
                    (_, Some(ranking)) if ranking.fused => {
                        steps.push(self.fuse_step(arm, inlined, fresh)?);
                        ArmRank::Fused
                    }
                    (population, Some(ranking)) => {
                        self.ranking_steps(
                            Some(arm),
                            population,
                            &ranking,
                            inlined,
                            &mut steps,
                            &mut bound,
                            fresh,
                        )?;
                        match ranking.exact {
                            None => ArmRank::Nearest,
                            Some((_, ScoreKind::Distance, _)) => ArmRank::Distance,
                            Some((_, ScoreKind::Bm25, _)) => ArmRank::Bm25,
                        }
                    }
                    // An inline read whose own `order` ranks its targets (the
                    // type checker refuses any other arm, `T75`).
                    (_, None) => {
                        let (input, items, bindings) = read_parts(arm)
                        .filter(|(input, ..)| orders(input))
                        .ok_or_else(|| {
                            ShapeError::Internal(
                                "an `rrf` arm that is neither a ranking nor an inline read with an `order`, which the type checker refuses (T75)".into(),
                            )
                        })?;
                        let mut facts = Facts {
                            inlined: *inlined,
                            ..Facts::default()
                        };
                        self.inline_read(input, items, bindings, &mut facts)?;
                        *inlined = facts.inlined;
                        self.place(&facts, &[], &mut steps, &mut bound, &HashSet::new(), fresh)?;
                        if !matches!(
                            steps.as_slice(),
                            [PatternStep::Correlate {
                                outer_var: None,
                                ..
                            }]
                        ) {
                            return Err(ShapeError::Internal(
                                "an ordered `rrf` arm that is not one uncorrelated read".into(),
                            ));
                        }
                        ArmRank::Ordered
                    }
                };
                shaped.push(FuseArm { steps, rank });
            }
            let target_type = arms
                .first()
                .and_then(|arm| node_type_in(arm, target))
                .ok_or_else(|| {
                    ShapeError::Internal(format!("the `rrf` target `{target}` binds no node type"))
                })?;
            // The default constant is the unwritten one.
            let k = match k {
                Count::Literal(n) if *n == RRF_DEFAULT_K => None,
                Count::Literal(n) => Some(IRExpr::Literal(
                    Literal::Integer(i128::from(*n)),
                    scalar(ScalarType::I64),
                )),
                Count::Param(p) => Some(IRExpr::Param(p.clone(), self.param_type(p))),
            };
            Ok(PatternStep::Fuse {
                arms: shaped,
                target: target.clone(),
                target_type,
                k,
                weights: rrf_weights_expr(weights),
            })
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
        crate::stack::deep(|| {
            self.visited.set(self.visited.get() + 1);
            self.spend(WorkKind::Shaping, 1)?;
            self.spend(WorkKind::ExprNodes, term.expr_nodes())?;
            match term {
                Term::Unit => Ok(()),
                Term::Env(cols) => {
                    facts.nullable_outer.extend(
                        cols.iter()
                            .filter(|c| matches!(c.ty, Ty::Node { nullable: true, .. }))
                            .map(|c| c.name.clone()),
                    );
                    Ok(())
                }
                Term::Scan {
                    var,
                    ty: Ty::Node { type_name, .. },
                } => {
                    facts.types.insert(var.clone(), type_name.clone());
                    facts.bindings.push(BindingFact {
                        variable: var.clone(),
                        type_name: type_name.clone(),
                    });
                    Ok(())
                }
                // `$k: E`, the `Scan` of an edge type: every edge of `E` once,
                // which is the single hop over `E` from a scan of its source type
                // with the edge bound, its two ends fresh names nothing reads.
                Term::Scan {
                    var,
                    ty: Ty::Edge { type_name, .. },
                } => {
                    let edge = self.catalog.lookup_edge_by_name(type_name).ok_or_else(|| {
                        ShapeError::Internal(format!("`${var}` scans `{type_name}`, no edge type"))
                    })?;
                    let src = self.shaped(format_args!("e{}_{var}_src", facts.inlined));
                    let dst = self.shaped(format_args!("e{}_{var}_dst", facts.inlined));
                    facts.inlined += 1;
                    facts.types.insert(src.clone(), edge.from_type.clone());
                    facts.bindings.push(BindingFact {
                        variable: src.clone(),
                        type_name: edge.from_type.clone(),
                    });
                    self.traversal(
                        facts,
                        TraversalFact {
                            src,
                            dst,
                            edges: EdgeSelection::Named(EdgeMember {
                                edge_type: edge.name.clone(),
                                direction: Direction::Out,
                            }),
                            src_type: edge.from_type.clone(),
                            dst_type: edge.to_type.clone(),
                            min_hops: 1,
                            max_hops: Some(1),
                            edge_binding: Some(var.clone()),
                            optionals_before: 0,
                        },
                    );
                    Ok(())
                }
                Term::Scan { var, .. } => Err(ShapeError::Internal(format!(
                    "`${var}` scans a value type, which the type checker never writes"
                ))),
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
                            optionals_before: 0,
                        },
                    );
                    Ok(())
                }
                Term::Filter { input, pred, fails } => self.filter(input, pred, *fails, facts),
                // `let $v = e`: a value column, computed once where it was
                // written, on every row the elements before it produce (A3),
                // and read wherever its name is. A value that can fail keeps its
                // place: the type checker placed no `where` conjunct below it.
                Term::Project {
                    input,
                    items,
                    keep: true,
                    ..
                } => {
                    self.facts(input, facts)?;
                    for (name, value) in items {
                        let mut value = value.clone();
                        resolve(&mut value, &facts.lets);
                        let column = self.let_column(facts, name, value);
                        facts.lets.insert(name.clone(), column);
                    }
                    Ok(())
                }
                // A `from` that opens the pattern: the inline read alone.
                Term::Project {
                    input,
                    items,
                    keep: false,
                    ..
                } => self.inline_read(input, items, &[], facts),
                // `from name(args)`: the declared read's body, read as an inline
                // read with its parameters bound to the arguments.
                Term::Call { name, .. } => {
                    let (input, items, bindings) = read_parts(term).ok_or_else(|| {
                    ShapeError::Internal(format!(
                        "the body of `{name}` is not a read's final projection, which the type checker always writes"
                    ))
                })?;
                    self.inline_read(input, items, bindings, facts)
                }
                Term::Correlate {
                    outer,
                    inner,
                    fails,
                } => {
                    if let Some(pattern) = optional_pattern(inner) {
                        self.optional(outer, pattern, facts)?;
                        if let Some(optional) = facts.optionals.last_mut() {
                            optional.fails = *fails;
                        }
                        return Ok(());
                    }
                    self.facts(outer, facts)?;
                    // The correlated reads this element states run on every row
                    // the elements before it produce: the type checker's flag
                    // says whether one can fail there, and then nothing moves
                    // below it.
                    let mark = facts.correlates.len();
                    // A block read as a value in a `let` or a return item.
                    if let Term::Group { input, keys, aggs } = inner.as_ref() {
                        let ([(column, agg)], []) = (aggs.as_slice(), keys.as_slice()) else {
                            return Err(ShapeError::Internal(
                                "a block that is not one aggregate without keys".into(),
                            ));
                        };
                        let value = self.block_value(input, column, agg, *fails, facts)?;
                        facts.correlates.push(value);
                    } else if let Some((input, items, bindings)) = read_parts(inner) {
                        self.inline_read(input, items, bindings, facts)?;
                    } else {
                        self.facts(inner, facts)?;
                    }
                    for correlate in &mut facts.correlates[mark..] {
                        correlate.fails |= *fails;
                    }
                    Ok(())
                }
                Term::Union { .. } => self.union(term, facts),
                other => Err(self.unlowered_node(other)),
            }
        })
    }

    /// `optional { p }` over the elements before it (`outer`): their facts,
    /// then the optional's own, whose new names enter the scope.
    fn optional(&self, outer: &Term, pattern: &Term, facts: &mut Facts) -> Result<(), ShapeError> {
        self.facts(outer, facts)?;
        let at = || Construct::Element(&[ElementKind::Optional], None);
        // The names the scope binds before it are the outer columns; a
        // rebinding of one inside is `T68`.
        let mut outer_columns: HashSet<String> = facts.names().into_iter().collect();
        outer_columns.extend(facts.outer_names.iter().cloned());
        let mut inner = Facts {
            inlined: facts.inlined,
            outer_names: outer_columns.clone(),
            ..Facts::default()
        };
        self.facts(pattern, &mut inner)?;
        facts.inlined = inner.inlined;
        // The optional's own elements and `where` read its `let`s in place.
        let inner_lets = std::mem::take(&mut inner.lets);
        inner.resolve(&inner_lets);
        let introduced: Vec<String> = inner
            .names()
            .into_iter()
            .filter(|name| !outer_columns.contains(name) && !self.is_anon(name))
            .collect();
        // A `let` inside an `optional` is a column of its matches, null on an
        // outer row with none: its column is computed in the optional's own
        // steps, and the optional pads it null where nothing matches. A
        // block's value read in the optional's own `where` is a column of its
        // matches nothing outside it reads.
        let mut exported: Vec<(String, IRExpr)> = inner_lets
            .iter()
            .filter(|(name, _)| !self.is_block_column(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        exported.sort_by(|a, b| a.0.cmp(&b.0));
        // The marker of a match: a node the optional binds, present on the
        // rows of its matches and null on an outer row with none.
        let mut matched = None;
        if !exported.is_empty() {
            let mut nodes: Vec<(&String, &String)> = introduced
                .iter()
                .filter_map(|name| inner.types.get(name).map(|ty| (name, ty)))
                .collect();
            nodes.sort();
            let Some((marker, type_name)) = nodes.first() else {
                return Err(self.refuse_at("a `let` inside an `optional` that binds no node", at()));
            };
            matched = Some(IRExpr::IsNull {
                expr: Box::new(IRExpr::Variable(
                    marker.to_string(),
                    ExprType::Node {
                        type_name: type_name.to_string(),
                    },
                )),
                negated: true,
                ty: scalar(ScalarType::Bool),
            });
        }
        let columns: HashSet<String> = inner.checks.iter().map(|c| c.name.clone()).collect();
        facts.optional_names.extend(introduced);
        // Its `Correlate`'s flag is set by the element that wrote it.
        facts.optionals.push(OptionalFact {
            inner,
            fails: false,
        });
        for (name, value) in exported {
            let ty = match value.ty().clone() {
                ExprType::Value { scalar, list, .. } => ExprType::Value {
                    scalar,
                    list,
                    nullable: true,
                },
                other => {
                    return Err(ShapeError::Internal(format!(
                        "`let ${name}` inside an optional holds a {}, not a value",
                        other.spelling()
                    )));
                }
            };
            let read = match &value {
                // A column the optional's own steps compute, which the
                // optional pads null where nothing matches.
                IRExpr::PropAccess {
                    variable, property, ..
                } if columns.contains(variable) => IRExpr::PropAccess {
                    variable: variable.clone(),
                    property: property.clone(),
                    ty,
                },
                // A value read in place (an outer column, a parameter, a
                // constant) is taken where the optional matched and null
                // elsewhere: one column after the optional.
                value => {
                    let Some(matched) = &matched else {
                        unreachable!("an exported `let` sets the marker")
                    };
                    let mut read = IRExpr::Call {
                        func: crate::ir::expr::ScalarFunc::If,
                        args: vec![
                            matched.clone(),
                            value.clone(),
                            IRExpr::Literal(Literal::Null, ty.clone()),
                        ],
                        ty,
                    };
                    resolve(&mut read, &facts.lets);
                    self.let_column(facts, &name, read)
                }
            };
            facts.lets.insert(name, read);
        }
        Ok(())
    }

    /// `from union(a, b, …)`: the arms, folded from the left, each an inline
    /// read of its own, its returned references bound to the union's column
    /// of the same name (the arms match by name, columns in the first arm's
    /// order) and every other name it binds renamed apart.
    fn union(&self, term: &Term, facts: &mut Facts) -> Result<(), ShapeError> {
        let at = || Construct::Element(FROM, None);
        let mut arms = Vec::new();
        let mut pending = vec![term];
        while let Some(term) = pending.pop() {
            match term {
                Term::Union { left, right } => {
                    pending.push(right);
                    pending.push(left);
                }
                arm => arms.push(arm),
            }
        }
        // An arm returning a value, or grouping, ordering or cutting its own
        // rows, is drawn as an uncorrelated read: every arm
        // then returns its references and a column of one shared `values`
        // binding per value, under the first arm's names.
        let mut parts = Vec::with_capacity(arms.len());
        for arm in &arms {
            parts.push(read_parts(arm).ok_or_else(|| self.unlowered_node(arm))?);
        }
        if parts.iter().any(|(_, items, _)| {
            items
                .iter()
                .any(|(_, e)| matches!(e.ty(), ExprType::Edge { .. }))
        }) {
            return Err(self.refuse_at("a `union` arm returning an edge", at()));
        }
        if parts.iter().any(|(input, items, _)| {
            groups_or_cuts(input)
                || items
                    .iter()
                    .any(|(_, e)| !matches!(e.ty(), ExprType::Node { .. }))
        }) {
            return self.union_of_reads(&parts, facts);
        }
        let mut columns: Vec<(String, String)> = Vec::new();
        let mut shaped = Vec::new();
        let mut outer: HashSet<String> = HashSet::new();
        for (index, &(input, items, bindings)) in parts.iter().enumerate() {
            let mut arm_outer: HashSet<String> = env_columns(input).into_iter().collect();
            for (_, arg) in bindings {
                let mut read = Vec::new();
                arg.columns(&mut read);
                arm_outer.extend(read);
            }
            let mut inner = Facts {
                inlined: facts.inlined,
                ..Facts::default()
            };
            self.facts(input, &mut inner)?;
            let inner_lets = std::mem::take(&mut inner.lets);
            inner.resolve(&inner_lets);
            let mut renames: HashMap<String, String> = HashMap::new();
            let mut exposed = HashSet::new();
            for (column, expr) in items.iter() {
                let mut expr = expr.clone();
                resolve(&mut expr, &inner_lets);
                let IRExpr::Variable(variable, ExprType::Node { type_name }) = &expr else {
                    return Err(ShapeError::Internal(
                        "a union arm's column is not a node after the value arms were drawn"
                            .to_string(),
                    ));
                };
                if index == 0 {
                    columns.push((column.clone(), type_name.clone()));
                }
                let Some((name, declared)) = columns.iter().find(|(name, _)| name == column) else {
                    return Err(ShapeError::Internal(format!(
                        "union arm {} has a column `{column}` the first does not, which the type checker refuses (T73)",
                        index + 1
                    )));
                };
                if declared != type_name {
                    return Err(ShapeError::Internal(format!(
                        "union column `{name}` is node {declared} and node {type_name}, which the type checker refuses (T73)"
                    )));
                }
                exposed.insert(variable.clone());
                if variable != name {
                    renames.insert(variable.clone(), name.clone());
                }
            }
            let mut hidden: Vec<String> = inner
                .names()
                .into_iter()
                .filter(|name| {
                    !exposed.contains(name) && !self.is_anon(name) && !arm_outer.contains(name)
                })
                .collect();
            hidden.sort();
            hidden.dedup();
            for name in hidden {
                let fresh = self.shaped(format_args!("f{}_{name}", inner.inlined));
                inner.inlined += 1;
                renames.insert(name, fresh);
            }
            inner.rename(&renames);
            bind_arguments(&mut inner, bindings);
            facts.inlined = inner.inlined;
            shaped.push(inner);
            outer.extend(arm_outer);
        }
        facts
            .types
            .extend(columns.iter().map(|(name, ty)| (name.clone(), ty.clone())));
        let reads_outer = !outer.is_empty();
        let union = UnionFact {
            arms: shaped,
            bindings: columns.clone(),
            values: None,
            outer: outer.clone(),
        };
        if !reads_outer {
            facts.unions.push(union);
            return Ok(());
        }
        // Arms reading the enclosing scope run per outer row: the union is
        // the pattern of a correlated read returning its references.
        let mut inner = Facts {
            inlined: facts.inlined,
            ..Facts::default()
        };
        inner.unions.push(union);
        inner
            .types
            .extend(columns.iter().map(|(name, ty)| (name.clone(), ty.clone())));
        let values = self.shaped(format_args!("r{}", facts.inlined));
        facts.inlined += 1;
        let read = InnerRead {
            values,
            group: None,
            columns: columns
                .iter()
                .map(|(name, type_name)| {
                    let ty = ExprType::Node {
                        type_name: type_name.clone(),
                    };
                    IRProjection {
                        expr: IRExpr::Variable(name.clone(), ty.clone()),
                        alias: None,
                        column: name.clone(),
                        ty,
                    }
                })
                .collect(),
            order_by: Vec::new(),
            limit: None,
            limit_param: None,
        };
        facts.correlates.push(CorrelateFact {
            inner,
            outer,
            read,
            optionals_before: facts.optionals.len(),
            fails: false,
        });
        Ok(())
    }

    /// `from union(a, b, …)` whose arms return a value, or group, order or
    /// cut their own rows: each arm an uncorrelated read of its own
    /// (`correlate_read`) returning its columns by name in the first arm's
    /// order,
    /// its values as columns of one `values` binding all arms share, which
    /// the union carries beside the references. A column is nullable when
    /// any arm's is.
    fn union_of_reads(
        &self,
        parts: &[(&Term, &[Item], &[Item])],
        facts: &mut Facts,
    ) -> Result<(), ShapeError> {
        let at = || Construct::Element(FROM, None);
        let values = self.shaped(format_args!("r{}", facts.inlined));
        facts.inlined += 1;
        let Some((_, first, _)) = parts.first() else {
            return Err(ShapeError::Internal("a union has no arm".to_string()));
        };
        let names: Vec<String> = first.iter().map(|(column, _)| column.clone()).collect();
        let mut nodes: Vec<(String, String)> = Vec::new();
        let mut value_types: Vec<(String, ExprType)> = Vec::new();
        let mut shaped = Vec::new();
        for (index, (input, items, bindings)) in parts.iter().enumerate() {
            let reads_outer = bindings.iter().any(|(_, arg)| !arg.columns_empty());
            if reads_outer || !env_columns(input).is_empty() {
                return Err(self.refuse_at(
                    "a `union` arm that returns a value or keeps its own cut and reads the enclosing scope",
                    at(),
                ));
            }
            if items.len() != names.len() {
                return Err(ShapeError::Internal(
                    "a union arm returns another number of columns than the first".to_string(),
                ));
            }
            let renamed: Vec<(String, IRExpr)> = names
                .iter()
                .map(|name| {
                    items
                        .iter()
                        .find(|(column, _)| column == name)
                        .map(|(_, e)| (name.clone(), e.clone()))
                        .ok_or_else(|| {
                            ShapeError::Internal(format!(
                                "a union arm has no column `{name}`, which the type checker refuses (T73)"
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;
            let mut inner = Facts {
                inlined: facts.inlined,
                ..Facts::default()
            };
            self.correlate_read_as(input, &renamed, bindings, &mut inner, Some(&values))?;
            facts.inlined = inner.inlined;
            let Some(correlate) = inner.correlates.last() else {
                return Err(ShapeError::Internal("a union arm drew no read".to_string()));
            };
            for (position, column) in correlate.read.columns.iter().enumerate() {
                match (&column.expr, column.ty.clone()) {
                    (IRExpr::Variable(_, _), ExprType::Node { type_name }) => {
                        match nodes.iter().find(|(name, _)| *name == column.column) {
                            Some((_, declared)) if *declared != type_name => {
                                return Err(ShapeError::Internal(format!(
                                    "union column `{}` is node {declared} and node {type_name}, which the type checker refuses (T73)",
                                    column.column
                                )));
                            }
                            Some(_) => {}
                            None if index == 0 => nodes.push((column.column.clone(), type_name)),
                            None => {
                                return Err(ShapeError::Internal(format!(
                                    "union column {position} is a node only in a later arm"
                                )));
                            }
                        }
                    }
                    (IRExpr::Variable(_, _), _) => {
                        return Err(ShapeError::Internal(
                            "a union arm returning an edge reached the value arms, which `union` refuses first".into(),
                        ));
                    }
                    (_, ty) => match value_types
                        .iter_mut()
                        .find(|(name, _)| *name == column.column)
                    {
                        Some((_, declared)) => {
                            if ty.nullable()
                                && let ExprType::Value { nullable, .. } = declared
                            {
                                *nullable = true;
                            }
                        }
                        None => value_types.push((column.column.clone(), ty)),
                    },
                }
            }
            inner.lets.clear();
            shaped.push(inner);
        }
        for (name, ty) in &value_types {
            facts.lets.insert(
                name.clone(),
                IRExpr::PropAccess {
                    variable: values.clone(),
                    property: name.clone(),
                    ty: ty.clone(),
                },
            );
        }
        facts
            .types
            .extend(nodes.iter().map(|(name, ty)| (name.clone(), ty.clone())));
        facts.unions.push(UnionFact {
            arms: shaped,
            bindings: nodes,
            values: (!value_types.is_empty()).then_some(values),
            outer: HashSet::new(),
        });
        Ok(())
    }

    /// `from { match … return { … } }`: one outer row per inner row, the
    /// inner pattern joined on the outer variables it reads. With no group,
    /// order or cut in it, that is the inner pattern's facts added to the
    /// outer ones: a returned reference column binds its name, a returned
    /// value is a `let` of that name, and every name the inner read binds
    /// without returning it is renamed apart, so a later outer binding of the
    /// same name stays another variable.
    ///
    /// A declared read's call is its body read the same way, `bindings`
    /// giving each parameter its argument. The arguments are bound after
    /// the body's own names are renamed apart, so a caller's variable an
    /// argument reads is never one the body binds: binding, not textual
    /// substitution.
    fn inline_read(
        &self,
        input: &Term,
        items: &[(String, IRExpr)],
        bindings: &[(String, IRExpr)],
        facts: &mut Facts,
    ) -> Result<(), ShapeError> {
        if self.probing.get() {
            return Ok(());
        }
        // An `optional` or a `union` is placed after its own scope's
        // elements; joined into the outer scope it would be placed after the
        // outer elements too. Such a read is drawn as a read of its own.
        if groups_or_cuts(input) || self.holds_optional_or_union(input)? {
            return self.correlate_read(input, items, bindings, facts);
        }
        let outer: HashSet<String> = env_columns(input).into_iter().collect();
        let mut inner = Facts::default();
        self.facts(input, &mut inner)?;
        let inner_lets = std::mem::take(&mut inner.lets);
        inner.resolve(&inner_lets);
        let items: Vec<(String, IRExpr)> = items
            .iter()
            .map(|(column, expr)| {
                let mut expr = expr.clone();
                resolve(&mut expr, &inner_lets);
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
            .filter(|name| !outer.contains(name) && !exposed.contains(name) && !self.is_anon(name))
            .collect();
        hidden.sort();
        hidden.dedup();
        for name in hidden {
            let fresh = self.shaped(format_args!("f{}_{name}", facts.inlined));
            facts.inlined += 1;
            renames.insert(name, fresh);
        }
        inner.rename(&renames);
        bind_arguments(&mut inner, bindings);
        let rename = |variable: &mut String| {
            if let Some(name) = renames.get(variable) {
                *variable = name.clone();
            }
        };
        // The inner `return` computes its values on every inner row, before
        // any outer `where`: a value that can fail is evaluated there, by
        // the check its `Project` states among the inner facts.
        facts.append(inner)?;
        for (column, mut value) in values {
            rename_expr(&mut value, &rename);
            bind_params(&mut value, bindings);
            // A value that is not already a column is computed once, as a
            // `let` of the enclosing scope, so a read of it never copies it.
            let value = self.let_column(facts, &column, value);
            facts.lets.insert(column, value);
        }
        Ok(())
    }

    /// `value` as the column of a `let` named `name` written at the facts'
    /// current point: a binding made up for it, whose [`LET_VALUE`] column
    /// the steps compute there; the read of that column is returned. A value
    /// that is already one leaf (a column, a reference, a parameter, a
    /// literal) is its own column: reading it is as cheap as reading a copy
    /// and it cannot fail.
    fn let_column(&self, facts: &mut Facts, name: &str, value: IRExpr) -> IRExpr {
        if matches!(
            value,
            IRExpr::PropAccess { .. }
                | IRExpr::Variable(..)
                | IRExpr::Param(..)
                | IRExpr::Literal(..)
                | IRExpr::AliasRef(..)
        ) {
            return value;
        }
        let n = self.lets_minted.get();
        self.lets_minted.set(n + 1);
        let binding = self.shaped(format_args!("l{n}_{name}"));
        let ty = value.ty().clone();
        facts.checks.push(LetCheck {
            name: binding.clone(),
            value,
            marks: facts.marks(),
        });
        IRExpr::PropAccess {
            variable: binding,
            property: LET_VALUE.to_string(),
            ty,
        }
    }

    /// Whether the scope of `input` states an `optional` or a `union` of its
    /// own. The probe reads the scope's facts but no inline read nested in
    /// it, which is a scope of its own, so shaping nested inline reads reads
    /// each scope a bounded number of times.
    fn holds_optional_or_union(&self, input: &Term) -> Result<bool, ShapeError> {
        let was = self.probing.replace(true);
        let mut probe = Facts::default();
        let probed = self.facts(input, &mut probe);
        self.probing.set(was);
        probed?;
        Ok(!probe.optionals.is_empty() || !probe.unions.is_empty())
    }

    /// `from { match … return { … } order { … } limit n }` with an
    /// aggregate, `distinct`, an `order` or a `limit` of its own: the core's
    /// `Correlate` over the read's `Group`, `Order` and `Cut`, which the
    /// outer rows are not flattened into. The read's pattern facts are its
    /// own scope, every name it binds without returning renamed apart and a
    /// returned reference bound under its column's name; a returned value
    /// enters the enclosing scope as a column of the read's `values`
    /// binding. A declared read's call is its body read the same way.
    fn correlate_read(
        &self,
        input: &Term,
        items: &[(String, IRExpr)],
        bindings: &[(String, IRExpr)],
        facts: &mut Facts,
    ) -> Result<(), ShapeError> {
        self.correlate_read_as(input, items, bindings, facts, None)
    }

    /// `correlate_read` whose value columns are columns of `values` when
    /// given (a union's arms share one), else of a fresh binding.
    fn correlate_read_as(
        &self,
        input: &Term,
        items: &[(String, IRExpr)],
        bindings: &[(String, IRExpr)],
        facts: &mut Facts,
        values: Option<&str>,
    ) -> Result<(), ShapeError> {
        if self.probing.get() {
            return Ok(());
        }
        let at = || Construct::Element(FROM, None);
        let mut outer: HashSet<String> = env_columns(input).into_iter().collect();
        for (_, arg) in bindings {
            let mut read = Vec::new();
            arg.columns(&mut read);
            outer.extend(read);
        }
        // The read's own cut, order and grouping, as the read's are.
        let mut term = input;
        let (mut limit, mut limit_param) = (None, None);
        if let Term::Cut { input, k } = term {
            match k {
                Count::Literal(n) => limit = Some(*n),
                Count::Param(p) => limit_param = Some(p.clone()),
            }
            term = input;
        }
        // A called read's `limit $k` reads the argument bound to `$k`.
        if let Some(name) = limit_param.take() {
            match bindings.iter().find(|(param, _)| *param == name) {
                None => limit_param = Some(name),
                Some((_, IRExpr::Param(caller, _))) => limit_param = Some(caller.clone()),
                Some((_, IRExpr::Literal(Literal::Integer(n), _))) => {
                    limit = Some(u64::try_from(*n).map_err(|_| {
                        ShapeError::Internal(format!("`limit ${name}` is bound to {n}"))
                    })?)
                }
                Some(_) => {
                    return Err(self.refuse_at(
                        "a called read's `limit` over an argument that is not a literal or a parameter",
                        at(),
                    ));
                }
            }
        }
        let mut order_keys: Vec<IROrdering> = Vec::new();
        if let Term::Order { input, keys } = term {
            order_keys = keys.clone();
            term = input;
        }
        let mut extended: Vec<(String, IRExpr)> = Vec::new();
        let mut keys: Vec<(String, IRExpr)> = Vec::new();
        let mut aggs: Vec<(String, Agg)> = Vec::new();
        let mut grouped = false;
        if let Term::Project {
            input,
            items,
            keep: true,
            ..
        } = term
            && matches!(input.as_ref(), Term::Group { .. })
        {
            extended = items.clone();
            term = input;
        }
        if let Term::Group {
            input,
            keys: group_keys,
            aggs: group_aggs,
        } = term
        {
            grouped = true;
            keys = group_keys.clone();
            aggs = group_aggs.clone();
            term = input;
        }
        // An ungrouped read's computed items extend its pattern's rows.
        if !grouped
            && let Term::Project {
                input,
                items,
                keep: true,
                ..
            } = term
            && items
                .iter()
                .all(|(name, _)| self.fresh.is(name, FreshRole::Item))
        {
            extended = items.clone();
            term = input;
        }
        if term.any_node(|node| {
            matches!(
                node,
                Term::ApproxTopK { .. } | Term::Score { .. } | Term::Fuse { .. }
            )
        }) {
            return Err(self.refuse_at(
                "a ranking inside an inline read with its own aggregate, `distinct`, `order` or `limit`",
                at(),
            ));
        }
        let mut inner = Facts {
            inlined: facts.inlined,
            ..Facts::default()
        };
        self.facts(term, &mut inner)?;
        let inner_lets = std::mem::take(&mut inner.lets);
        inner.resolve(&inner_lets);
        let read_lets = |mut e: IRExpr| {
            resolve(&mut e, &inner_lets);
            e
        };
        let extended: Vec<(String, IRExpr)> = extended
            .into_iter()
            .map(|(name, e)| (name, read_lets(e)))
            .collect();
        let keys: Vec<(String, IRExpr)> = keys
            .into_iter()
            .map(|(name, e)| (name, read_lets(e)))
            .collect();
        for (_, agg) in &mut aggs {
            if let Some(arg) = &mut agg.arg {
                resolve(arg, &inner_lets);
            }
        }
        // What each hidden column is over the read's rows: an item over the
        // pattern's rows, or a group's key or aggregate column.
        let values = match values {
            Some(values) => values.to_string(),
            None => {
                let values = self.shaped(format_args!("r{}", inner.inlined));
                inner.inlined += 1;
                values
            }
        };
        let mut rows: HashMap<String, IRExpr> = HashMap::new();
        let mut group = None;
        if grouped {
            let mut columns = Vec::with_capacity(keys.len() + aggs.len());
            for (hidden, key) in &keys {
                rows.insert(
                    hidden.clone(),
                    if is_reference(key) {
                        key.clone()
                    } else {
                        IRExpr::PropAccess {
                            variable: values.clone(),
                            property: hidden.clone(),
                            ty: key.ty().clone(),
                        }
                    },
                );
                columns.push(IRProjection {
                    expr: key.clone(),
                    alias: None,
                    column: hidden.clone(),
                    ty: key.ty().clone(),
                });
            }
            for (hidden, agg) in &aggs {
                let aggregate = self.aggregate(agg, None).map_err(|e| e.at(at()))?;
                let ty = aggregate.ty().clone();
                rows.insert(
                    hidden.clone(),
                    IRExpr::PropAccess {
                        variable: values.clone(),
                        property: hidden.clone(),
                        ty: ty.clone(),
                    },
                );
                columns.push(IRProjection {
                    expr: aggregate,
                    alias: None,
                    column: hidden.clone(),
                    ty,
                });
            }
            group = Some(columns);
        }
        for (hidden, e) in &extended {
            let mut e = e.clone();
            resolve(&mut e, &rows);
            rows.insert(hidden.clone(), e);
        }
        // The returned columns: a reference binds its column's name.
        let mut renames: HashMap<String, String> = HashMap::new();
        let mut columns = Vec::with_capacity(items.len());
        for (column, item) in items {
            let mut expr = item.clone();
            resolve(&mut expr, &rows);
            if let IRExpr::Variable(variable, _) = &expr
                && is_reference(&expr)
            {
                if variable != column {
                    if outer.contains(variable) {
                        return Err(self.refuse_at(
                            "a `from` returning an outer variable under another name",
                            at(),
                        ));
                    }
                    renames.insert(variable.clone(), column.clone());
                }
                if let ExprType::Node { type_name } = expr.ty() {
                    facts.types.insert(column.clone(), type_name.clone());
                }
            }
            columns.push(IRProjection {
                ty: expr.ty().clone(),
                expr,
                alias: None,
                column: column.clone(),
            });
        }
        let exposed: HashSet<String> = columns
            .iter()
            .filter(|c| is_reference(&c.expr))
            .filter_map(|c| match &c.expr {
                IRExpr::Variable(v, _) => Some(v.clone()),
                _ => None,
            })
            .collect();
        let mut hidden: Vec<String> = inner
            .names()
            .into_iter()
            .filter(|name| !outer.contains(name) && !exposed.contains(name) && !self.is_anon(name))
            .collect();
        hidden.sort();
        hidden.dedup();
        for name in hidden {
            let fresh = self.shaped(format_args!("f{}_{name}", inner.inlined));
            inner.inlined += 1;
            renames.insert(name, fresh);
        }
        facts.inlined = inner.inlined;
        inner.rename(&renames);
        bind_arguments(&mut inner, bindings);
        let rename = |variable: &mut String| {
            if let Some(name) = renames.get(variable) {
                *variable = name.clone();
            }
        };
        // A reference key orders the read's own rows by its identity, as a
        // read's `order` does; a grouped reference keeps its binding's
        // identity column.
        let id = self.catalog.system_columns.id;
        let order_by = order_keys
            .into_iter()
            .flat_map(|written| {
                let IROrdering {
                    expr: key,
                    descending,
                    nulls_first,
                } = written;
                let mut key = read_lets(key);
                resolve(&mut key, &rows);
                let keys = match &key {
                    IRExpr::Variable(variable, ty) => reference_identity(variable, ty, id),
                    _ => None,
                };
                keys.unwrap_or_else(|| vec![key])
                    .into_iter()
                    .map(move |expr| IROrdering {
                        expr,
                        descending,
                        nulls_first,
                    })
            })
            .collect();
        let mut read = InnerRead {
            values: values.clone(),
            group,
            columns,
            order_by,
            limit,
            limit_param,
        };
        for e in read.exprs_mut() {
            rename_expr(e, &rename);
            bind_params(e, bindings);
        }
        // A value column is read after the `from` as its column of `values`.
        for column in &read.columns {
            if !is_reference(&column.expr) {
                facts.lets.insert(
                    column.column.clone(),
                    IRExpr::PropAccess {
                        variable: values.clone(),
                        property: column.column.clone(),
                        ty: column.ty.clone(),
                    },
                );
            }
        }
        facts.correlates.push(CorrelateFact {
            inner,
            outer,
            read,
            optionals_before: facts.optionals.len(),
            fails: false,
        });
        Ok(())
    }

    fn traversal(&self, facts: &mut Facts, mut t: TraversalFact) {
        for end in [&mut t.src, &mut t.dst] {
            if self.is_anon(end) {
                *end = "_".to_string();
            }
        }
        if t.dst != "_" {
            facts.types.insert(t.dst.clone(), t.dst_type.clone());
        }
        if t.src != "_" {
            facts.types.insert(t.src.clone(), t.src_type.clone());
        }
        t.optionals_before = facts.optionals.len();
        facts.traversals.push(t);
    }

    fn filter(
        &self,
        input: &Term,
        pred: &IRExpr,
        fails: bool,
        facts: &mut Facts,
    ) -> Result<(), ShapeError> {
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
            && self.is_semi(dst)
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
                    optionals_before: 0,
                },
            );
            return Ok(());
        }
        // A predicate over block columns: the blocks under it.
        let mut cols = Vec::new();
        pred.columns(&mut cols);
        if cols.iter().any(|c| self.is_block_column(c)) {
            return self.blocks(input, pred, fails, facts);
        }
        self.facts(input, facts)?;
        // `where`'s conjuncts in written order, each with the type
        // checker's flag: the planner's placement law decides which move.
        facts.conjuncts.push(ConjunctFact::Filter {
            pred: pred.clone(),
            fails,
            values: Vec::new(),
        });
        Ok(())
    }

    /// `pred` over the aggregates of the correlated blocks under it. A
    /// block compared in a top-level conjunct with a value of the outer row
    /// is a semi-join that keeps the rows its aggregate passes; any other
    /// filter over block values (a block under `or` or `not`, or compared
    /// with another block's value) reads each block's value as a column of
    /// its own correlated read, and the filter runs over those columns.
    fn blocks(
        &self,
        input: &Term,
        pred: &IRExpr,
        fails: bool,
        facts: &mut Facts,
    ) -> Result<(), ShapeError> {
        let mut groups: Vec<(&Term, &str, &Agg, Vec<Col>, bool)> = Vec::new();
        let mut below = input;
        while let Term::Correlate {
            outer,
            inner,
            fails: block_fails,
        } = below
        {
            let Term::Group { input, keys, aggs } = inner.as_ref() else {
                break;
            };
            // The type checker writes a block as `Group(p, [], [agg])`.
            let ([(column, agg)], []) = (aggs.as_slice(), keys.as_slice()) else {
                return Err(ShapeError::Internal(
                    "a block that is not one aggregate without keys".into(),
                ));
            };
            groups.push((input, column, agg, env_cols(input), *block_fails));
            below = outer;
        }
        self.facts(below, facts)?;
        groups.reverse();
        // One semi-join per comparison; `not (c > 0)` is the row-count zero
        // test. The right side is evaluated over the outer rows: it reads
        // their columns, not another block's value nor a full-text call. A
        // block that holds its group's values (`collect`, `stddev`,
        // `percentile`) is read as a value: it finishes a group's values,
        // not a running scalar a semi-join folds.
        let comparisons: Option<Vec<(CompOp, IRExpr)>> = groups
            .iter()
            .map(|(_, column, agg, _, _)| {
                if agg.func.holds_values() {
                    return None;
                }
                block_comparison(pred, column).filter(|(_, right)| {
                    let mut read = Vec::new();
                    right.columns(&mut read);
                    !read.iter().any(|c| self.is_block_column(c)) && !holds_lexical_call(right)
                })
            })
            .collect();
        let Some(comparisons) = comparisons else {
            // The blocks are read as values, each placed right before the
            // conjunct that reads them.
            let mut values = Vec::new();
            for (inner, column, agg, _, block_fails) in groups {
                values.push(self.block_value(inner, column, agg, block_fails, facts)?);
            }
            facts.conjuncts.push(ConjunctFact::Filter {
                pred: pred.clone(),
                fails,
                values,
            });
            return Ok(());
        };
        for ((inner, _, agg, env, _), (op, right)) in groups.into_iter().zip(comparisons) {
            let (inner_facts, agg) = self.block_inner(inner, agg)?;
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
                        result: nullable_result(&agg),
                    },
                },
            };
            facts.conjuncts.push(ConjunctFact::Block {
                block: Box::new(BlockFact {
                    inner: inner_facts,
                    outer: env.iter().map(|c| c.name.clone()).collect(),
                    left,
                    op,
                    right,
                }),
                fails,
            });
        }
        Ok(())
    }

    /// A block's pattern facts, its `let` values read in, and its aggregate
    /// over them.
    fn block_inner(&self, inner: &Term, agg: &Agg) -> Result<(Facts, Agg), ShapeError> {
        let mut inner_facts = Facts::default();
        self.facts(inner, &mut inner_facts)?;
        let inner_lets = std::mem::take(&mut inner_facts.lets);
        inner_facts.resolve(&inner_lets);
        let mut agg = agg.clone();
        if let Some(arg) = &mut agg.arg {
            resolve(arg, &inner_lets);
        }
        if agg.arg.as_ref().is_some_and(holds_lexical_call) {
            return Err(self.refuse_at(
                "a full-text call read as a value in a block's aggregate",
                Construct::Block,
            ));
        }
        Ok((inner_facts, agg))
    }

    /// A block read as a value (`count { p }` in a `let`, a return item or
    /// a filter over block values): the core's `Correlate(R, Group(p, [],
    /// [agg]))`, one row per outer row whatever `p` matches, so a `count` over
    /// no rows is 0 and another aggregate null. It is a correlated read
    /// grouped by no key whose one value column the block's column names,
    /// every name the block binds renamed apart.
    fn block_value(
        &self,
        inner: &Term,
        column: &str,
        agg: &Agg,
        fails: bool,
        facts: &mut Facts,
    ) -> Result<CorrelateFact, ShapeError> {
        let outer: HashSet<String> = env_columns(inner).into_iter().collect();
        let (mut inner_facts, agg) = self.block_inner(inner, agg)?;
        inner_facts.inlined = facts.inlined;
        let mut hidden: Vec<String> = inner_facts
            .names()
            .into_iter()
            .filter(|name| !outer.contains(name) && !self.is_anon(name))
            .collect();
        hidden.sort();
        hidden.dedup();
        let mut renames = HashMap::new();
        for name in hidden {
            let fresh = self.shaped(format_args!("f{}_{name}", inner_facts.inlined));
            inner_facts.inlined += 1;
            renames.insert(name, fresh);
        }
        inner_facts.rename(&renames);
        let rename = |variable: &mut String| {
            if let Some(name) = renames.get(variable) {
                *variable = name.clone();
            }
        };
        let values = self.shaped(format_args!("r{}", inner_facts.inlined));
        inner_facts.inlined += 1;
        facts.inlined = inner_facts.inlined;
        // Counting a node binding, or a block's rows, counts the rows: a
        // count of a value every row holds.
        let counts_rows = match &agg.arg {
            None => true,
            Some(IRExpr::Variable(_, ExprType::Node { .. } | ExprType::Edge { .. })) => {
                agg.func == AggFunc::Count && !agg.distinct
            }
            Some(_) => false,
        };
        let (func, distinct, mut arg, fraction) = if counts_rows {
            (
                AggFunc::Count,
                false,
                IRExpr::Literal(Literal::Bool(true), scalar(ScalarType::Bool)),
                None,
            )
        } else {
            let arg = agg.arg.clone().ok_or_else(|| {
                ShapeError::Internal("an aggregate block without its argument".into())
            })?;
            (agg.func, agg.distinct, arg, agg.fraction.clone())
        };
        rename_expr(&mut arg, &rename);
        let aggregate = IRExpr::Aggregate {
            func,
            distinct,
            fraction: fraction.map(Box::new),
            signature: AggSignature {
                arg: arg.ty().clone(),
                result: nullable_result(&agg),
            },
            arg: Box::new(arg),
        };
        // The block's value has the type the type checker gave it: a count
        // is never null.
        let ty = agg.ty.expr_type();
        let grouped = format!("^{column}");
        let value = IRExpr::PropAccess {
            variable: values.clone(),
            property: column.to_string(),
            ty: ty.clone(),
        };
        let aggregated = aggregate.ty().clone();
        let read = InnerRead {
            values: values.clone(),
            group: Some(vec![IRProjection {
                ty: aggregated.clone(),
                expr: aggregate,
                alias: None,
                column: grouped.clone(),
            }]),
            columns: vec![IRProjection {
                expr: IRExpr::PropAccess {
                    variable: values,
                    property: grouped,
                    ty: aggregated,
                },
                alias: None,
                column: column.to_string(),
                ty,
            }],
            order_by: Vec::new(),
            limit: None,
            limit_param: None,
        };
        facts.lets.insert(column.to_string(), value);
        Ok(CorrelateFact {
            inner: inner_facts,
            outer,
            read,
            optionals_before: facts.optionals.len(),
            fails,
        })
    }

    /// The scope's traversals as expansions from the names already bound,
    /// each introducing its other end; a traversal with neither end bound
    /// scans its source.
    /// `optional_names` are the references null on a row an `optional` kept
    /// without a match: those an `optional` of the scope introduced, and
    /// those of an enclosing scope's that the scope reads.
    fn expand_traversals(
        &self,
        traversals: &[&TraversalFact],
        pipeline: &mut Vec<PatternStep>,
        bound_vars: &mut HashSet<String>,
        fresh: &mut FreshNames,
        optional_names: &HashSet<String>,
    ) -> Result<(), ShapeError> {
        let mut remaining: Vec<&TraversalFact> = traversals.to_vec();
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
                        edge_binding,
                    });
                    let id = self.catalog.system_columns.id.to_string();
                    let string = scalar(ScalarType::String);
                    // An optional's node is null on a row it kept without a
                    // match; the equality is null there and drops the row.
                    let end = match optional_names.contains(&traversal.dst) {
                        true => ExprType::Value {
                            scalar: ScalarType::String,
                            list: false,
                            nullable: true,
                        },
                        false => string.clone(),
                    };
                    // The identity test closing the triple cannot fail.
                    pipeline.push(PatternStep::Filter {
                        pred: crate::ir::coerce::binary(
                            IRExpr::PropAccess {
                                variable: temp_var,
                                property: id.clone(),
                                ty: string,
                            },
                            BinaryOp::Compare(CompOp::Eq),
                            IRExpr::PropAccess {
                                variable: traversal.dst.clone(),
                                property: id,
                                ty: end,
                            },
                        )
                        .map_err(|e| ShapeError::Internal(e.to_string()))?,
                        fails: false,
                    });
                } else if !src_bound && dst_bound {
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
                        edge_binding,
                    });
                    if traversal.src != "_" {
                        bound_vars.insert(traversal.src.clone());
                    }
                } else {
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
                });
                bound_vars.insert(t.src.clone());
            }
            remaining = next_remaining;
        }
        Ok(())
    }

    /// The pattern's steps: the facts placed as the module documentation
    /// describes. A `let` splits them: the facts the elements before it
    /// stated are placed first, then its column, then the rest, so it is
    /// computed once on exactly the rows those elements produce and the
    /// elements after it read it.
    fn place(
        &self,
        facts: &Facts,
        ranked: &[String],
        pipeline: &mut Vec<PatternStep>,
        bound_vars: &mut HashSet<String>,
        outer_physical_names: &HashSet<String>,
        fresh: &mut FreshNames,
    ) -> Result<(), ShapeError> {
        if facts.checks.is_empty() {
            return self.place_scope(
                facts,
                ranked,
                pipeline,
                bound_vars,
                outer_physical_names,
                fresh,
            );
        }
        let mut from = Marks::default();
        for check in &facts.checks {
            let before = facts.segment(from, check.marks);
            self.place_scope(
                &before,
                ranked,
                pipeline,
                bound_vars,
                outer_physical_names,
                fresh,
            )?;
            pipeline.push(PatternStep::Let {
                name: check.name.clone(),
                value: check.value.clone(),
            });
            from = check.marks;
        }
        let rest = facts.segment(from, facts.marks());
        self.place_scope(
            &rest,
            ranked,
            pipeline,
            bound_vars,
            outer_physical_names,
            fresh,
        )
    }

    /// A `from` over an inline read with its own group or cut, after the
    /// steps already in `pipeline`: its pattern placed as its own scope from
    /// the outer names it reads, which it correlates on when the steps
    /// before it produce rows; its reference columns are bound after it.
    fn place_correlate(
        &self,
        correlate: &CorrelateFact,
        pipeline: &mut Vec<PatternStep>,
        bound_vars: &mut HashSet<String>,
        outer_physical_names: &HashSet<String>,
        fresh: &mut FreshNames,
    ) -> Result<(), ShapeError> {
        let visible: HashSet<String> = bound_vars
            .iter()
            .filter(|name| correlate.outer.contains(*name))
            .cloned()
            .collect();
        let mut names = outer_physical_names.clone();
        names.extend(physical_names(pipeline));
        let mut inner = Vec::new();
        let mut inner_bound = visible.clone();
        self.place(
            &correlate.inner,
            &[],
            &mut inner,
            &mut inner_bound,
            &names,
            fresh,
        )?;
        // With no row before it, the names it reads are values the type
        // checker already read into it, and it reads no outer row.
        let outer_var = (!correlate.outer.is_empty() && !pipeline.is_empty()).then(|| {
            find_outer_var(&correlate.inner, &visible).unwrap_or_else(|| {
                let mut outer: Vec<&String> = correlate.outer.iter().collect();
                outer.sort();
                outer
                    .first()
                    .map(|name| name.to_string())
                    .unwrap_or_default()
            })
        });
        bound_vars.extend(correlate.read.references().map(str::to_string));
        pipeline.push(PatternStep::Correlate {
            outer_var,
            inner,
            read: correlate.read.clone(),
            fails: correlate.fails,
        });
        Ok(())
    }

    /// One segment of a scope's facts placed after the steps already in
    /// `pipeline`.
    fn place_scope(
        &self,
        facts: &Facts,
        ranked: &[String],
        pipeline: &mut Vec<PatternStep>,
        bound_vars: &mut HashSet<String>,
        outer_physical_names: &HashSet<String>,
        fresh: &mut FreshNames,
    ) -> Result<(), ShapeError> {
        // A union is a source: its arms run from nothing, and the names it
        // binds are bound for the elements that join it.
        for union in &facts.unions {
            // Arms that read the enclosing scope start from its rows, the
            // names they read already bound.
            let visible: Vec<&String> = {
                let mut visible: Vec<&String> = bound_vars
                    .iter()
                    .filter(|name| union.outer.contains(*name))
                    .collect();
                visible.sort();
                visible
            };
            let outer_var = visible.first().map(|name| name.to_string());
            let mut arms = Vec::with_capacity(union.arms.len());
            for arm in &union.arms {
                let mut steps = Vec::new();
                let mut arm_bound: HashSet<String> = match outer_var {
                    Some(_) => visible.iter().map(|name| name.to_string()).collect(),
                    None => HashSet::new(),
                };
                self.place(arm, &[], &mut steps, &mut arm_bound, &HashSet::new(), fresh)?;
                arms.push(steps);
            }
            let mut bindings: Vec<String> = union
                .bindings
                .iter()
                .map(|(name, _)| name.clone())
                .collect();
            bound_vars.extend(bindings.iter().cloned());
            bindings.extend(union.values.iter().cloned());
            pipeline.push(PatternStep::Union {
                arms,
                bindings,
                outer_var,
            });
        }
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
                [] => scan_root(&inner_bindings, reaches_outer),
                [(var, type_name)] => {
                    let declared = inner_bindings.iter().position(|binding| *binding == var);
                    if declared.is_none() {
                        ranked_scans.push((component_bindings.first().copied(), var, type_name));
                    }
                    declared
                }
                _ => {
                    return Err(ShapeError::Internal(
                        "two ranked columns in one scope: a read ranks one column".into(),
                    ));
                }
            };
            for (index, var) in inner_bindings.into_iter().enumerate() {
                if Some(index) != root {
                    deferred_set.insert(var.to_string());
                }
            }
        }
        for binding in &bindings {
            ranked_scans.retain(|&(anchor, variable, type_name)| {
                if anchor != Some(binding.variable.as_str()) {
                    return true;
                }
                pipeline.push(PatternStep::NodeScan {
                    variable: variable.to_string(),
                    type_name: type_name.to_string(),
                });
                bound_vars.insert(variable.to_string());
                false
            });
            if bound_vars.contains(&binding.variable) || deferred_set.contains(&binding.variable) {
                continue;
            }
            pipeline.push(PatternStep::NodeScan {
                variable: binding.variable.clone(),
                type_name: binding.type_name.clone(),
            });
            bound_vars.insert(binding.variable.clone());
        }
        for (_, variable, type_name) in ranked_scans {
            pipeline.push(PatternStep::NodeScan {
                variable: variable.to_string(),
                type_name: type_name.to_string(),
            });
            bound_vars.insert(variable.to_string());
        }
        // The steps place an `optional` after the scope's other traversals;
        // a traversal from a name it introduces expands after it.
        // A `from` over an inline read with its own group or cut binds its
        // reference columns where it is placed, beside the `optional`s.
        let correlate_names: HashSet<&str> = facts
            .correlates
            .iter()
            .flat_map(|correlate| correlate.read.references())
            .collect();
        let (after_optional, before_optional): (Vec<&TraversalFact>, Vec<&TraversalFact>) =
            traversals.iter().partition(|t| {
                facts.optional_names.contains(&t.src)
                    || facts.optional_names.contains(&t.dst)
                    || correlate_names.contains(t.src.as_str())
                    || correlate_names.contains(t.dst.as_str())
            });
        // A triple from a correlated read's column and no `optional`'s name
        // expands once that read is placed, before the `optional`s written
        // after it, which read its ends as outer columns; one written after
        // an `optional` expands after it.
        let (mut after_correlate, after_optional): (Vec<&TraversalFact>, Vec<&TraversalFact>) =
            after_optional.into_iter().partition(|t| {
                !facts.optional_names.contains(&t.src) && !facts.optional_names.contains(&t.dst)
            });
        // A triple closing on a reference null on some rows, one an
        // `optional` of this scope or of an enclosing one introduced, reads
        // its identity as nullable.
        let nullable: HashSet<String> = facts
            .optional_names
            .union(&facts.nullable_outer)
            .cloned()
            .collect();
        self.expand_traversals(&before_optional, pipeline, bound_vars, fresh, &nullable)?;
        let mut correlates = facts.correlates.iter().peekable();
        for (index, optional) in facts.optionals.iter().enumerate() {
            while let Some(correlate) =
                correlates.next_if(|correlate| correlate.optionals_before <= index)
            {
                self.place_correlate(correlate, pipeline, bound_vars, outer_physical_names, fresh)?;
            }
            let (due, later): (Vec<&TraversalFact>, Vec<&TraversalFact>) = after_correlate
                .into_iter()
                .partition(|t| t.optionals_before <= index);
            after_correlate = later;
            self.expand_traversals(&due, pipeline, bound_vars, fresh, &nullable)?;
            // The inner tree reads the whole outer row; a name it shares with
            // the scope is that outer column (a rebinding is `T68`).
            let mut visible_outer: HashSet<String> = bound_vars.clone();
            let outer_var = find_outer_var(&optional.inner, &visible_outer);
            let mut names = outer_physical_names.clone();
            names.extend(physical_names(pipeline));
            let mut inner = Vec::new();
            let before = visible_outer.clone();
            self.place(
                &optional.inner,
                &[],
                &mut inner,
                &mut visible_outer,
                &names,
                fresh,
            )?;
            bound_vars.extend(visible_outer.difference(&before).cloned());
            pipeline.push(PatternStep::Optional {
                outer_var: outer_var.unwrap_or_default(),
                inner,
                fails: optional.fails,
            });
        }
        // A read returning only values that reads a name the traversals
        // from an `optional`'s names bind runs after them; nothing they
        // expand from reads its columns.
        let after_optional: Vec<&TraversalFact> =
            after_correlate.into_iter().chain(after_optional).collect();
        let late: HashSet<&str> = after_optional
            .iter()
            .flat_map(|t| [t.src.as_str(), t.dst.as_str()])
            .filter(|name| *name != "_" && !bound_vars.contains(*name))
            .collect();
        let (deferred, now): (Vec<&CorrelateFact>, Vec<&CorrelateFact>) =
            correlates.partition(|correlate| {
                correlate.read.references().next().is_none()
                    && correlate
                        .outer
                        .iter()
                        .any(|name| late.contains(name.as_str()))
            });
        for correlate in now {
            self.place_correlate(correlate, pipeline, bound_vars, outer_physical_names, fresh)?;
        }
        self.expand_traversals(&after_optional, pipeline, bound_vars, fresh, &nullable)?;
        for correlate in deferred {
            self.place_correlate(correlate, pipeline, bound_vars, outer_physical_names, fresh)?;
        }
        // The conjuncts in written order, after every element: a value
        // block right before the conjunct that reads it, a compared block
        // as its semi-join. The planner's placement law moves the ones that
        // may move.
        for conjunct in &facts.conjuncts {
            match conjunct {
                ConjunctFact::Filter {
                    pred,
                    fails,
                    values,
                } => {
                    for value in values {
                        self.place_correlate(
                            value,
                            pipeline,
                            bound_vars,
                            outer_physical_names,
                            fresh,
                        )?;
                    }
                    pipeline.push(PatternStep::Filter {
                        pred: pred.clone(),
                        fails: *fails,
                    });
                }
                ConjunctFact::Block { block, fails } => {
                    self.place_block(
                        block,
                        *fails,
                        pipeline,
                        bound_vars,
                        outer_physical_names,
                        fresh,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// A block compared with a value of the outer row, after the steps in
    /// `pipeline`: its pattern placed as a scope of its own over the outer
    /// names it correlates on, then the semi-join that keeps the outer rows
    /// its aggregate passes.
    fn place_block(
        &self,
        block: &BlockFact,
        fails: bool,
        pipeline: &mut Vec<PatternStep>,
        bound_vars: &HashSet<String>,
        outer_physical_names: &HashSet<String>,
        fresh: &mut FreshNames,
    ) -> Result<(), ShapeError> {
        let mut physical_names = outer_physical_names.clone();
        physical_names.extend(self::physical_names(pipeline));
        {
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
                fails,
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

/// Whether `expr` holds a full-text call (`search`, `fuzzy`, `match_text`,
/// `match_terms`): a filter or a `let` reads one as membership of the row in
/// its matches, which a return item, an order key or a block's aggregate
/// does not lower yet.
fn holds_lexical_call(expr: &IRExpr) -> bool {
    let mut found = false;
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        if expr.lexical_predicate_field().is_some() {
            found = true;
            continue;
        }
        match expr {
            IRExpr::Binary { left, right, .. } => pending.extend([left.as_ref(), right.as_ref()]),
            IRExpr::Not(inner, _)
            | IRExpr::Cast { expr: inner, .. }
            | IRExpr::Neg { expr: inner, .. }
            | IRExpr::IsNull { expr: inner, .. } => pending.push(inner),
            IRExpr::Aggregate { arg, .. } => pending.push(arg),
            IRExpr::Call { args, .. } => pending.extend(args),
            // The predicates themselves were taken above.
            IRExpr::PropAccess { .. }
            | IRExpr::Nearest { .. }
            | IRExpr::Variable(..)
            | IRExpr::Param(..)
            | IRExpr::Literal(..)
            | IRExpr::AliasRef(..) => {}
        }
    }
    found
}

/// The identity keys of the reference `variable` of type `ty`: its id
/// column `id`, after its edge type for an edge whose selector has several
/// types (one type: the type is constant). `None` for a value.
fn reference_identity(variable: &str, ty: &ExprType, id: &str) -> Option<Vec<IRExpr>> {
    let string = ExprType::from_prop(&PropType::scalar(ScalarType::String, false));
    let property = |property: &str| IRExpr::PropAccess {
        variable: variable.to_string(),
        property: property.to_string(),
        ty: string.clone(),
    };
    match ty {
        ExprType::Node { .. } => Some(vec![property(id)]),
        ExprType::Edge { type_name } if type_name.contains('|') => {
            Some(vec![property(EDGE_TYPE_COLUMN), property(id)])
        }
        ExprType::Edge { .. } => Some(vec![property(id)]),
        ExprType::Value { .. } | ExprType::ExactInteger { .. } => None,
    }
}

impl Shaper<'_> {
    /// The type a read of `variable.property` has where `population` binds
    /// `variable`: the declared property's, a non-null String when the
    /// population binds no stored node of that name.
    fn field_type(&self, population: &Term, variable: &str, property: &str) -> ExprType {
        node_type_in(population, variable)
            .and_then(|type_name| self.catalog.node_types.get(&type_name))
            .and_then(|node| node.properties.get(property))
            .map_or_else(|| scalar(ScalarType::String), ExprType::from_prop)
    }
}

/// `match_terms(field, terms(text, mode, max_edits))`, the predicate a
/// lexical query states over `field`.
fn match_terms_call(field: IRExpr, lexical: &LexicalQuery) -> IRExpr {
    IRExpr::Call {
        func: crate::ir::expr::ScalarFunc::MatchTerms {
            mode: if lexical.mode_all {
                crate::ir::expr::TermsMode::All
            } else {
                crate::ir::expr::TermsMode::Any
            },
        },
        args: vec![field, lexical.text.clone(), lexical.max_edits.clone()],
        ty: scalar(ScalarType::Bool),
    }
}

/// The node type `variable` takes in `term`, the relation a ranking scores:
/// the scan, traversal end or outer column that binds it there, looked up in
/// that relation alone, so a name another scope reuses for another type is
/// not read.
fn node_type_in(term: &Term, variable: &str) -> Option<String> {
    crate::stack::deep(|| match term {
        Term::Unit | Term::RecRef { .. } => None,
        Term::Env(cols) => cols.iter().find_map(|col| match &col.ty {
            Ty::Node { type_name, .. } if col.name == variable => Some(type_name.clone()),
            _ => None,
        }),
        Term::Scan { var, ty } => match ty {
            Ty::Node { type_name, .. } if var == variable => Some(type_name.clone()),
            _ => None,
        },
        Term::Expand {
            input,
            src,
            dst,
            src_type,
            dst_type,
            ..
        } => {
            if dst == variable {
                Some(dst_type.clone())
            } else if src == variable {
                Some(src_type.clone())
            } else {
                node_type_in(input, variable)
            }
        }
        Term::Filter { input, .. }
        | Term::Project { input, .. }
        | Term::Group { input, .. }
        | Term::Order { input, .. }
        | Term::Cut { input, .. }
        | Term::Score { input, .. }
        | Term::ApproxTopK { input, .. } => node_type_in(input, variable),
        Term::Correlate { outer, inner, .. } => {
            node_type_in(inner, variable).or_else(|| node_type_in(outer, variable))
        }
        Term::Union { left, .. } => node_type_in(left, variable),
        Term::Fuse { arms, .. } => arms.first().and_then(|arm| node_type_in(arm, variable)),
        Term::Fix { body, .. } | Term::Call { body, .. } => node_type_in(body, variable),
    })
}

fn scan_root(inner_bindings: &[&str], reaches_outer: bool) -> Option<usize> {
    if reaches_outer {
        None
    } else {
        inner_bindings.first().map(|_| 0)
    }
}

/// Whether an inline read's body under its final projection groups, orders
/// or cuts: an aggregate or `distinct` (a `Group`), an `order` or a
/// `limit`.
fn groups_or_cuts(term: &Term) -> bool {
    match term {
        Term::Cut { .. } | Term::Order { .. } | Term::Group { .. } => true,
        Term::Project {
            input, keep: true, ..
        } => matches!(input.as_ref(), Term::Group { .. }),
        _ => false,
    }
}

/// Whether an inline read's body under its final projection orders its
/// rows: an `order`, under its `limit` if it has one.
fn orders(term: &Term) -> bool {
    match term {
        Term::Cut { input, .. } => orders(input),
        Term::Order { .. } => true,
        _ => false,
    }
}

/// The `Env` leaf a correlated pattern starts from, along its first
/// inputs: the outer columns it reads. A nested scope's own `Env` (a
/// block's, an `optional`'s, an inline read's) is off that path.
fn env_cols(term: &Term) -> Vec<Col> {
    let mut leaf = term;
    loop {
        match leaf {
            Term::Env(cols) => return cols.clone(),
            other => match other.children().first() {
                Some(child) => leaf = child,
                None => return Vec::new(),
            },
        }
    }
}

/// The names of the outer columns a correlated pattern reads.
fn env_columns(term: &Term) -> Vec<String> {
    env_cols(term).into_iter().map(|c| c.name).collect()
}

/// `p` of an `optional { p }`, from the inner term the type checker writes
/// under `Correlate(R, …)`: `Union(p, Project(Filter(Correlate(Env,
/// Group(p, [], [count])), ¬ exists), nulls))`.
fn optional_pattern(inner: &Term) -> Option<&Term> {
    let Term::Union { left, right } = inner else {
        return None;
    };
    let Term::Project {
        input, keep: false, ..
    } = right.as_ref()
    else {
        return None;
    };
    let Term::Filter { input, .. } = input.as_ref() else {
        return None;
    };
    let Term::Correlate {
        outer: row,
        inner: grouped,
        ..
    } = input.as_ref()
    else {
        return None;
    };
    let Term::Group {
        input: counted,
        keys,
        aggs,
    } = grouped.as_ref()
    else {
        return None;
    };
    (matches!(row.as_ref(), Term::Env(_) | Term::Unit)
        && (std::sync::Arc::ptr_eq(counted, left) || counted == left)
        && keys.is_empty()
        && aggs.len() == 1)
        .then_some(left.as_ref())
}

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
/// its conjuncts read: a block's traversals come first.
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
            .conjuncts
            .iter()
            .filter_map(|c| match c {
                ConjunctFact::Filter { pred, .. } => Some(pred),
                ConjunctFact::Block { .. } => None,
            })
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
    /// Every name minted, which the shape records as made up.
    minted: Vec<String>,
}

impl FreshNames {
    fn anon(&mut self) -> String {
        self.anon += 1;
        let name = format!("ShapedAnon{}", self.anon);
        self.minted.push(name.clone());
        name
    }

    fn temp(&mut self, dst: &str) -> String {
        self.temp += 1;
        let name = format!("Shaped{}_{}", dst, self.temp);
        self.minted.push(name.clone());
        name
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
            PatternStep::NodeScan { variable, .. } => rename(variable),
            PatternStep::Expand {
                src_var,
                dst_var,
                edge_binding,
                ..
            } => {
                rename(src_var);
                rename(dst_var);
                if let Some(binding) = edge_binding {
                    rename(binding);
                }
            }
            PatternStep::Filter { pred, .. } => expressions.push(pred),
            PatternStep::Rank { binding, key, .. } => {
                rename(binding);
                expressions.push(key);
            }
            PatternStep::Score { binding, query, .. } => {
                rename(binding);
                expressions.push(query);
            }
            PatternStep::Let { value, .. } => expressions.push(value),
            PatternStep::AntiJoin {
                outer_var,
                inner,
                predicate,
                ..
            } => {
                rename(outer_var);
                expressions.extend(predicate.left.arg_mut());
                expressions.push(&mut predicate.right);
                pending.extend(inner);
            }
            PatternStep::Optional {
                outer_var, inner, ..
            } => {
                rename(outer_var);
                pending.extend(inner);
            }
            PatternStep::Union {
                arms,
                bindings,
                outer_var,
            } => {
                bindings.iter_mut().for_each(&rename);
                if let Some(outer_var) = outer_var {
                    rename(outer_var);
                }
                pending.extend(arms.iter_mut().flatten());
            }
            PatternStep::Correlate {
                outer_var,
                inner,
                read,
                ..
            } => {
                if let Some(outer_var) = outer_var {
                    rename(outer_var);
                }
                for column in &mut read.columns {
                    if matches!(
                        column.expr,
                        IRExpr::Variable(_, ExprType::Node { .. } | ExprType::Edge { .. })
                    ) {
                        rename(&mut column.column);
                    }
                }
                expressions.extend(read.exprs_mut());
                pending.extend(inner);
            }
            PatternStep::Fuse {
                arms,
                target,
                k,
                weights,
                ..
            } => {
                rename(target);
                expressions.extend(k.iter_mut().chain(weights.iter_mut()));
                pending.extend(arms.iter_mut().flat_map(|arm| arm.steps.iter_mut()));
            }
        }
    }
    for expr in expressions {
        rename_expr(expr, &rename);
    }
}

/// `expr` with every `is null` over a node or edge reference testing the
/// reference's identity column: nullable where an `optional` introduces the
/// name, else never null.
fn reference_null_tests(expr: &mut IRExpr, id: &str, optional: &HashSet<String>) {
    crate::stack::deep(|| {
        if let IRExpr::IsNull { expr: inner, .. } = expr
            && let IRExpr::Variable(variable, ExprType::Node { .. } | ExprType::Edge { .. }) =
                inner.as_ref()
        {
            let nullable = optional.contains(variable);
            **inner = IRExpr::PropAccess {
                variable: variable.clone(),
                property: id.to_string(),
                ty: ExprType::from_prop(&PropType::scalar(ScalarType::String, nullable)),
            };
            return;
        }
        let recurse = |e: &mut IRExpr| reference_null_tests(e, id, optional);
        match expr {
            IRExpr::Nearest { query, .. } => recurse(query),
            IRExpr::Aggregate { arg, .. }
            | IRExpr::Not(arg, _)
            | IRExpr::IsNull { expr: arg, .. }
            | IRExpr::Neg { expr: arg, .. }
            | IRExpr::Cast { expr: arg, .. } => recurse(arg),
            IRExpr::Binary { left, right, .. } => {
                recurse(left);
                recurse(right);
            }
            IRExpr::Call { args, .. } => args.iter_mut().for_each(recurse),
            IRExpr::AliasRef(_, _)
            | IRExpr::Variable(_, _)
            | IRExpr::Literal(_, _)
            | IRExpr::Param(_, _)
            | IRExpr::PropAccess { .. } => {}
        }
    })
}

/// A named expression: a returned item, or a parameter and its argument.
type Item = (String, IRExpr);

/// An inline read's parts: its pattern, its returned items and the
/// arguments its parameters are bound to, none for a read written inline; a
/// declared read's call is its body's.
fn read_parts(term: &Term) -> Option<(&Term, &[Item], &[Item])> {
    match term {
        Term::Project {
            input,
            items,
            keep: false,
            ..
        } => Some((input, items, &[])),
        Term::Call { bindings, body, .. } => match body.as_ref() {
            Term::Project {
                input,
                items,
                keep: false,
                ..
            } => Some((input, items, bindings)),
            _ => None,
        },
        _ => None,
    }
}

/// Every expression of a called read's facts with each of its parameters
/// read as the argument bound to it.
fn bind_arguments(facts: &mut Facts, bindings: &[(String, IRExpr)]) {
    if bindings.is_empty() {
        return;
    }
    facts.each_expr(&mut |e| bind_params(e, bindings));
}

/// `expr` with every parameter `bindings` names read as its argument.
fn bind_params(expr: &mut IRExpr, bindings: &[(String, IRExpr)]) {
    crate::stack::deep(|| {
        if bindings.is_empty() {
            return;
        }
        if let IRExpr::Param(name, _) = expr {
            if let Some((_, arg)) = bindings.iter().find(|(param, _)| param == name) {
                *expr = arg.clone();
            }
            return;
        }
        let recurse = |e: &mut IRExpr| bind_params(e, bindings);
        match expr {
            IRExpr::Nearest { query, .. } => recurse(query),
            // `percentile`'s fraction may be the called read's parameter.
            IRExpr::Aggregate { arg, fraction, .. } => {
                recurse(arg);
                if let Some(p) = fraction {
                    recurse(p);
                }
            }
            IRExpr::Not(arg, _)
            | IRExpr::IsNull { expr: arg, .. }
            | IRExpr::Neg { expr: arg, .. }
            | IRExpr::Cast { expr: arg, .. } => recurse(arg),
            IRExpr::Binary { left, right, .. } => {
                recurse(left);
                recurse(right);
            }
            IRExpr::Call { args, .. } => args.iter_mut().for_each(recurse),
            IRExpr::AliasRef(_, _)
            | IRExpr::Variable(_, _)
            | IRExpr::Literal(_, _)
            | IRExpr::Param(_, _)
            | IRExpr::PropAccess { .. } => {}
        }
    })
}

/// `expr` with every name `lets` maps read as the column it maps to: a
/// `let`'s column, a ranking's score, a read's value column. Each is a read
/// of one column, so resolving a name never copies a computed value.
fn resolve(expr: &mut IRExpr, lets: &HashMap<String, IRExpr>) {
    crate::stack::deep(|| {
        if lets.is_empty() {
            return;
        }
        match expr {
            IRExpr::AliasRef(name, _) | IRExpr::Variable(name, _) => {
                if let Some(value) = lets.get(name.as_str()) {
                    *expr = value.clone();
                }
            }
            IRExpr::Nearest { query, .. } => resolve(query, lets),
            IRExpr::Aggregate { arg, .. }
            | IRExpr::Not(arg, _)
            | IRExpr::IsNull { expr: arg, .. }
            | IRExpr::Neg { expr: arg, .. }
            | IRExpr::Cast { expr: arg, .. } => resolve(arg, lets),
            IRExpr::Binary { left, right, .. } => {
                resolve(left, lets);
                resolve(right, lets);
            }
            IRExpr::Call { args, .. } => args.iter_mut().for_each(|a| resolve(a, lets)),
            IRExpr::Literal(_, _) | IRExpr::Param(_, _) | IRExpr::PropAccess { .. } => {}
        }
    })
}

fn rename_expr(expr: &mut IRExpr, rename: &impl Fn(&mut String)) {
    crate::stack::deep(|| match expr {
        IRExpr::PropAccess { variable, .. } | IRExpr::Variable(variable, _) => rename(variable),
        IRExpr::Nearest {
            variable, query, ..
        } => {
            rename(variable);
            rename_expr(query, rename);
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
    })
}
