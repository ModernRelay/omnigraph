//! The checks every accepted plan passes (RFC 0047, "Checks have explicit
//! boundaries"): binding and traversal identity, the eligibility predicates
//! (every required conjunct placed, every placed conjunct required),
//! correlated blocks, search identity and its declared approximation, the
//! projection and the origin of every projected score, late hydration, the
//! required order and the row cut, and the policy data the plan declares for
//! its search. They establish that the plan keeps what the query requires
//! and adds nothing it does not; they do not prove row-selection
//! equivalence, which only the exact subset's checked derivation does.
//!
//! Requirements and plan are compared as typed IR (see `requirements.rs`).
//! The planner adds one predicate the query does not state: the canonical
//! `@id` of a written single String key equality (`scan_access::narrow_key`),
//! which is admitted only when the validator derives the same id from the
//! written value.

use std::collections::HashSet;

use omnigraph_compiler::ir::{IRExpr, IROrdering, IRProjection};
use omnigraph_compiler::query::ast::{Expr, RRF_COLUMN};
use omnigraph_compiler::query::typecheck::BoundVariable;
use omnigraph_compiler::types::{ExprType, ScalarType};

use super::budget::Budget;
use super::requirements::{Requirements, Retrieval, Search, constant_true};
use super::{AcceptInput, ValidationError};
use crate::cost::AntiJoinStrategy;
use crate::logical::{ColumnRef, EDGE_TYPE_MEMBER, IDENTITY_MEMBER};
use crate::optimizer::{RRF_NEAREST_ARM_K, contains_aggregate, derived_order};
use crate::physical::{
    Assumptions, Eligibility, EmptyEligible, NodeId, OverfetchRung, PhysicalNode, PhysicalPlan,
    RankKind, RankScope, RankedAccess,
};
use crate::source::FullTextCoverage;

/// The nodes of one pipeline: every node reachable from `root` without
/// entering a correlated block's inner tree.
fn pipeline(
    plan: &PhysicalPlan,
    root: NodeId,
    budget: &mut Budget,
) -> Result<Vec<NodeId>, ValidationError> {
    let mut nodes = Vec::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        budget.visit(1)?;
        let node = plan.node(id).ok_or_else(|| {
            ValidationError::violated("plan shape", format!("node {id} is a tombstone"))
        })?;
        nodes.push(id);
        match node {
            PhysicalNode::AntiJoin { input, .. } => pending.push(*input),
            other => pending.extend(other.inputs()),
        }
    }
    Ok(nodes)
}

/// A block's inner tree has the shape its strategy declares
/// (`PhysicalNode::AntiJoin`): under `correlated` its pipeline reaches the
/// `OuterReference` to the block's binding; under `key_set` it reaches no
/// `OuterReference` and an `Expand` introduces the binding, whose identity
/// keys the outer rows. Refused here, a mismatched saved or edited plan never
/// reaches the operator, where it would fail on a missing outer batch or
/// correlation column.
fn check_block_strategy(
    plan: &PhysicalPlan,
    id: NodeId,
    outer_var: &str,
    inner: NodeId,
    strategy: AntiJoinStrategy,
    budget: &mut Budget,
) -> Result<(), ValidationError> {
    let nodes = pipeline(plan, inner, budget)?;
    let reaches = |want: &dyn Fn(&PhysicalNode) -> bool| {
        nodes.iter().any(|node| plan.node(*node).is_some_and(want))
    };
    let holds = match strategy {
        AntiJoinStrategy::Correlated => reaches(
            &|node| matches!(node, PhysicalNode::OuterReference { outer_var: leaf } if leaf == outer_var),
        ),
        AntiJoinStrategy::KeySet => {
            !reaches(&|node| matches!(node, PhysicalNode::OuterReference { .. }))
                && reaches(
                    &|node| matches!(node, PhysicalNode::Expand { dst, .. } if dst == outer_var),
                )
        }
    };
    if holds {
        return Ok(());
    }
    Err(ValidationError::violated(
        "block strategy",
        format!(
            "node {id} declares the `{}` strategy for its block over `${outer_var}`, which its inner tree does not have",
            match strategy {
                AntiJoinStrategy::Correlated => "correlated",
                AntiJoinStrategy::KeySet => "key_set",
            }
        ),
    ))
}

/// One pipeline a query's eligibility constrains: `own`, the nodes that
/// produce its rows, and `after`, the nodes those rows pass through above a
/// fusion (empty without one).
struct Constrained {
    own: Vec<NodeId>,
    after: Vec<NodeId>,
}

impl Constrained {
    /// Every node the pipeline's rows reach.
    fn reached(&self) -> Vec<NodeId> {
        self.own.iter().chain(&self.after).copied().collect()
    }
}

/// The pipelines whose rows a query's eligibility constrains: one per `rrf`
/// arm, each its own copy of the match, followed by the nodes above the
/// fusion; or the whole top level.
fn constrained_pipelines(
    plan: &PhysicalPlan,
    budget: &mut Budget,
) -> Result<Vec<Constrained>, ValidationError> {
    let top = pipeline(plan, plan.root(), budget)?;
    let fuse = top.iter().find_map(|id| match plan.node(*id) {
        Some(PhysicalNode::RankFuse { arms, .. }) => Some((*id, arms.clone())),
        _ => None,
    });
    let Some((fuse, arms)) = fuse else {
        return Ok(vec![Constrained {
            own: top,
            after: Vec::new(),
        }]);
    };
    let mut below = HashSet::new();
    let mut arm_pipelines = Vec::new();
    for arm in &arms {
        let own = pipeline(plan, arm.input, budget)?;
        below.extend(own.iter().copied());
        arm_pipelines.push(own);
    }
    let after: Vec<NodeId> = top
        .into_iter()
        .filter(|id| *id != fuse && !below.contains(id))
        .collect();
    Ok(arm_pipelines
        .into_iter()
        .map(|own| Constrained {
            own,
            after: after.clone(),
        })
        .collect())
}

/// Every conjunct a pipeline's nodes test, wherever placement put it, with
/// the node that tests it.
fn conjuncts(plan: &PhysicalPlan, nodes: &[NodeId]) -> Vec<(NodeId, IRExpr)> {
    let mut out = Vec::new();
    for id in nodes {
        match plan.node(*id) {
            Some(PhysicalNode::Scan { spec, .. }) => {
                if let Some(filter) = &spec.filter {
                    out.extend(
                        filter
                            .gq_filters()
                            .into_iter()
                            .flat_map(IRExpr::into_conjuncts)
                            .map(|conjunct| (*id, conjunct)),
                    );
                }
            }
            Some(
                PhysicalNode::Filter { filters, .. } | PhysicalNode::CrossJoin { filters, .. },
            ) => {
                out.extend(
                    filters
                        .iter()
                        .cloned()
                        .flat_map(IRExpr::into_conjuncts)
                        .map(|conjunct| (*id, conjunct)),
                );
            }
            Some(PhysicalNode::ContainsJoin {
                conjunct, residual, ..
            }) => {
                out.push((*id, conjunct.clone()));
                out.extend(
                    residual
                        .iter()
                        .cloned()
                        .flat_map(IRExpr::into_conjuncts)
                        .map(|conjunct| (*id, conjunct)),
                );
            }
            _ => {}
        }
    }
    out
}

/// The expression an order key names: for an alias, the return item it
/// aliases.
fn named_expr<'e>(returns: &'e [IRProjection], key: &'e IRExpr) -> &'e IRExpr {
    match key {
        IRExpr::AliasRef(alias, _) => returns
            .iter()
            .find(|projection| projection.column == *alias)
            .map_or(key, |projection| &projection.expr),
        expr => expr,
    }
}

/// Every full-text call the query writes reads its property's index at the
/// pinned snapshot, so the plan records that index's coverage, and the
/// coverage holds a segment (full or partial; an untrained declaration
/// carries the analyzer too). Replay re-derives every recorded coverage from
/// the pinned dataset, so a saved plan can neither omit the fact nor claim a
/// segment the snapshot lacks.
fn check_full_text_indexes(
    plan: &PhysicalPlan,
    input: &AcceptInput<'_>,
    budget: &mut Budget,
) -> Result<(), ValidationError> {
    let targets = crate::optimizer::full_text_targets(input.ir);
    budget.visit(u64::try_from(targets.len()).unwrap_or(u64::MAX))?;
    for (type_name, property) in targets {
        let key = Assumptions::full_text_key(&format!("node:{type_name}"), &property);
        match plan.assumptions().full_text.get(&key) {
            Some(FullTextCoverage::Full | FullTextCoverage::Partial) => {}
            recorded => {
                return Err(ValidationError::violated(
                    "prerequisite",
                    format!(
                        "a full-text call reads the index of `{type_name}.{property}` under recorded coverage {recorded:?}; it needs a recorded index segment"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The ranked scans of the plan with their ids.
fn ranked_scans(plan: &PhysicalPlan) -> Vec<(NodeId, &str, &RankedAccess)> {
    plan.live()
        .filter_map(|(id, node)| match node {
            PhysicalNode::Scan {
                spec,
                ranked: Some(ranked),
                ..
            } => Some((id, spec.binding.as_deref().unwrap_or_default(), ranked)),
            _ => None,
        })
        .collect()
}

impl Requirements {
    pub(crate) fn check(
        &self,
        plan: &PhysicalPlan,
        input: &AcceptInput<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        // A fused query's eligibility belongs inside each arm (retention);
        // what its rows pass through after the fusion is checked with the
        // arm for origin, extra traversals and blocks.
        for pipeline in &constrained_pipelines(plan, budget)? {
            let reached = pipeline.reached();
            self.check_bindings(plan, &reached)?;
            self.check_eligibility(plan, &pipeline.own, &reached, input, budget)?;
            self.check_blocks(plan, &pipeline.own, budget)?;
            if let Some(id) = pipeline
                .after
                .iter()
                .find(|id| matches!(plan.node(**id), Some(PhysicalNode::AntiJoin { .. })))
            {
                return Err(ValidationError::violated(
                    "predicate origin",
                    format!(
                        "node {id} decides a correlated block above the fusion, outside every arm"
                    ),
                ));
            }
        }
        let top = pipeline(plan, plan.root(), budget)?;
        check_full_text_indexes(plan, input, budget)?;
        self.check_search(plan)?;
        self.check_row_cuts(plan, budget)?;
        self.check_returns(plan, &top, budget)?;
        self.check_hydration(plan, &top)?;
        self.check_order_and_cut(plan, &top, input, budget)?;
        check_policies(plan, budget)?;
        Ok(())
    }

    /// Every written binding is read with its type: a node binding by a scan
    /// (or a pushed-down count) of its node type, an edge binding by the
    /// traversal that names it over its edge types; every traversal the query
    /// writes runs once with its endpoints, edge selection, endpoint types,
    /// hop bounds and edge binding, and no other traversal runs.
    fn check_bindings(&self, plan: &PhysicalPlan, nodes: &[NodeId]) -> Result<(), ValidationError> {
        for (name, bound) in &self.bindings {
            let found = nodes.iter().any(|id| match (plan.node(*id), bound) {
                (
                    Some(
                        PhysicalNode::Scan { spec, .. } | PhysicalNode::MetadataCount { spec, .. },
                    ),
                    BoundVariable::Node { type_name },
                ) => {
                    spec.binding.as_deref() == Some(name.as_str())
                        && spec.table.node_type_name() == Some(type_name.as_str())
                }
                (
                    Some(PhysicalNode::Expand {
                        edge_binding: Some(edge),
                        edges,
                        ..
                    }),
                    BoundVariable::Edge { type_names },
                ) => {
                    let mut members: Vec<&str> = edges
                        .members()
                        .iter()
                        .map(|member| member.edge_type.as_str())
                        .collect();
                    let mut expected: Vec<&str> = type_names.iter().map(String::as_str).collect();
                    members.sort_unstable();
                    members.dedup();
                    expected.sort_unstable();
                    expected.dedup();
                    edge == name && members == expected
                }
                _ => false,
            });
            if !found {
                return Err(ValidationError::violated(
                    "binding identity",
                    format!("no read of the plan binds `${name}` as the query declares it"),
                ));
            }
        }
        let mut required: Vec<&super::requirements::Traversal> = self.traversals.iter().collect();
        for id in nodes {
            let Some(PhysicalNode::Expand {
                src,
                dst,
                edges,
                src_type,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                ..
            }) = plan.node(*id)
            else {
                continue;
            };
            let position = required.iter().position(|traversal| {
                traversal.src == *src
                    && traversal.dst == *dst
                    && traversal.edges == *edges
                    && traversal.src_type == *src_type
                    && traversal.dst_type == *dst_type
                    && traversal.min_hops == *min_hops
                    && traversal.max_hops == *max_hops
                    && traversal.edge_binding == *edge_binding
            });
            match position {
                Some(position) => {
                    required.remove(position);
                }
                None => {
                    return Err(ValidationError::violated(
                        "traversal identity",
                        format!(
                            "the plan traverses `${src}` to `${dst}` over {edges:?} ({min_hops}..{max_hops:?} hops); the query writes no such traversal"
                        ),
                    ));
                }
            }
        }
        if let Some(missing) = required.first() {
            return Err(ValidationError::violated(
                "traversal identity",
                format!(
                    "the query traverses `${}` to `${}`; the plan runs no such traversal",
                    missing.src, missing.dst
                ),
            ));
        }
        Ok(())
    }

    /// The conjuncts a pipeline tests are exactly the ones the query
    /// requires: every required conjunct is tested among `retained` (an
    /// `rrf` arm's own nodes), and every conjunct tested anywhere its rows
    /// reach, after a fusion included, is required, a constant `true` (which
    /// constrains nothing) or the canonical identity of a written key
    /// equality on the same scan.
    fn check_eligibility(
        &self,
        plan: &PhysicalPlan,
        retained: &[NodeId],
        reached: &[NodeId],
        input: &AcceptInput<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let kept = conjuncts(plan, retained);
        let placed = conjuncts(plan, reached);
        budget.visit(
            u64::try_from(kept.len() + placed.len() + self.eligibility.len()).unwrap_or(u64::MAX),
        )?;
        let kept_set: HashSet<&IRExpr> = kept.iter().map(|(_, conjunct)| conjunct).collect();
        if let Some(missing) = self
            .eligibility
            .iter()
            .find(|required| !kept_set.contains(required))
        {
            // The same text under other stored types is a retyped conjunct.
            let text = missing.to_string();
            let detail = if kept
                .iter()
                .any(|(_, conjunct)| conjunct.to_string() == text)
            {
                format!("the plan tests `{missing}` with stored types the query does not give it")
            } else {
                format!("the plan tests no conjunct for `{missing}`")
            };
            return Err(ValidationError::violated("predicate retention", detail));
        }
        let required: HashSet<&IRExpr> = self.eligibility.iter().collect();
        for (id, conjunct) in &placed {
            if constant_true(conjunct)
                || required.contains(conjunct)
                || self.narrowed_key(plan, *id, conjunct, &required, input)
            {
                continue;
            }
            return Err(ValidationError::violated(
                "predicate origin",
                format!("the plan tests `{conjunct}`, which the query does not require"),
            ));
        }
        Ok(())
    }

    /// Whether `conjunct`, placed on the scan `id`, is the identity equality
    /// the key narrowing adds for a written key equality on the same scan
    /// ([`super::requirements::narrowed_identity`]).
    fn narrowed_key(
        &self,
        plan: &PhysicalPlan,
        id: NodeId,
        conjunct: &IRExpr,
        required: &HashSet<&IRExpr>,
        input: &AcceptInput<'_>,
    ) -> bool {
        let Some(PhysicalNode::Scan { spec, .. }) = plan.node(id) else {
            return false;
        };
        let (Some(binding), Some(filter)) = (spec.binding.as_deref(), spec.filter.as_ref()) else {
            return false;
        };
        let written: Vec<IRExpr> = filter
            .gq_filters()
            .into_iter()
            .flat_map(IRExpr::into_conjuncts)
            .filter(|written| required.contains(written))
            .collect();
        super::requirements::narrowed_identity(
            input,
            &spec.table.type_key,
            binding,
            &written,
            conjunct,
        )
    }

    /// The correlated blocks of each pipeline the query's rows come from are
    /// exactly the written ones, each over its outer binding with the
    /// predicate the query states.
    fn check_blocks(
        &self,
        plan: &PhysicalPlan,
        nodes: &[NodeId],
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let mut planned: Vec<(&String, &omnigraph_compiler::ir::SubqueryPredicate)> = Vec::new();
        for id in nodes {
            if let Some(PhysicalNode::AntiJoin {
                outer_var,
                predicate,
                inner,
                strategy,
                ..
            }) = plan.node(*id)
            {
                check_block_strategy(plan, *id, outer_var, *inner, *strategy, budget)?;
                planned.push((outer_var, predicate));
            }
        }
        if planned.len() != self.blocks.len() {
            return Err(ValidationError::violated(
                "predicate retention",
                format!(
                    "the query writes {} correlated blocks; the plan runs {}",
                    self.blocks.len(),
                    planned.len()
                ),
            ));
        }
        for block in &self.blocks {
            let found = planned.iter().position(|(outer_var, predicate)| {
                **outer_var == block.outer_var && **predicate == block.predicate
            });
            match found {
                Some(index) => {
                    planned.remove(index);
                }
                None => {
                    return Err(ValidationError::violated(
                        "predicate retention",
                        format!(
                            "no correlated block of the plan over `${}` decides as the query's block does",
                            block.outer_var
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// The plan ranks exactly the retrievals the leading `order` key names,
    /// each on its binding and property, with its argument and the
    /// approximation its contract declares, in the scope it orders.
    fn check_search(&self, plan: &PhysicalPlan) -> Result<(), ValidationError> {
        let ranked = ranked_scans(plan);
        let fuse = plan.live().find_map(|(_, node)| match node {
            PhysicalNode::RankFuse { arms, k, .. } => Some((arms, k)),
            _ => None,
        });
        let expected: Vec<(RankScope, &Retrieval)> = match &self.search {
            None => Vec::new(),
            Some(Search::Rank(retrieval)) => vec![(RankScope::Order, retrieval)],
            Some(Search::Fuse { arms, .. }) => vec![
                (RankScope::Primary, &arms[0]),
                (RankScope::Secondary, &arms[1]),
            ],
        };
        if ranked.len() != expected.len() {
            return Err(ValidationError::violated(
                "search identity",
                format!(
                    "the query names {} retrievals; the plan ranks {} scans",
                    expected.len(),
                    ranked.len()
                ),
            ));
        }
        for (scope, retrieval) in &expected {
            let Some((id, binding, access)) =
                ranked.iter().find(|(_, _, access)| access.scope == *scope)
            else {
                return Err(ValidationError::violated(
                    "search identity",
                    format!("no ranked scan of the plan orders the `{scope:?}` retrieval"),
                ));
            };
            let same = *binding == retrieval.binding
                && access.kind == retrieval.kind
                && access.property == retrieval.property
                && access.query == retrieval.query;
            if !same {
                return Err(ValidationError::violated(
                    "search identity",
                    format!(
                        "the plan ranks `${binding}.{}` by {:?} with `{}`; the query names {:?} of `${}.{}` with `{}`",
                        access.property,
                        access.kind,
                        access.query,
                        retrieval.kind,
                        retrieval.binding,
                        retrieval.property,
                        retrieval.query
                    ),
                ));
            }
            if access.kind.approximate() != retrieval.approximate() {
                return Err(ValidationError::violated(
                    "approximation",
                    format!(
                        "the plan declares the {:?} retrieval of `${binding}` {}; its contract is {}",
                        access.kind,
                        exactness(access.kind.approximate()),
                        exactness(retrieval.approximate())
                    ),
                ));
            }
            let type_key = match plan.node(*id) {
                Some(PhysicalNode::Scan { spec, .. }) => spec.table.type_key.as_str(),
                _ => "",
            };
            self.check_access(plan, type_key, access)?;
        }
        match (&self.search, fuse) {
            (Some(Search::Fuse { arms, k }), Some((planned, planned_k))) => {
                for (arm, retrieval) in planned.iter().zip(arms.iter()) {
                    if arm.binding != retrieval.binding || arm.kind != retrieval.kind {
                        return Err(ValidationError::violated(
                            "search identity",
                            format!(
                                "the fusion's arm ranks `${}` by {:?}; the query names {:?} of `${}`",
                                arm.binding, arm.kind, retrieval.kind, retrieval.binding
                            ),
                        ));
                    }
                }
                if planned_k != k {
                    return Err(ValidationError::violated(
                        "search identity",
                        "the fusion's `k` is not the one the query writes",
                    ));
                }
            }
            (Some(Search::Fuse { .. }), None) => {
                return Err(ValidationError::violated(
                    "search identity",
                    "the query fuses two retrievals; the plan has no fusion",
                ));
            }
            (_, Some(_)) => {
                return Err(ValidationError::violated(
                    "search identity",
                    "the plan fuses retrievals the query does not fuse",
                ));
            }
            (_, None) => {}
        }
        Ok(())
    }

    /// Every count cut the plan declares (`PhysicalNode::row_cut`) is one the
    /// query requires, where it requires it: the root `Limit` (under a root
    /// hydration) and the final sort's `fetch` carry the query's `limit`, and
    /// a ranked scan's candidate cap is the one its declared policy sets
    /// (checked with its access). Any other cut is refused, a capped sort
    /// below the final one and a cut in a correlated block's tree included.
    fn check_row_cuts(
        &self,
        plan: &PhysicalPlan,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        budget.visit(u64::try_from(plan.live().count()).unwrap_or(u64::MAX))?;
        let limit = self
            .limit
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
        let root = below_hydration(plan, plan.root());
        let mut id = match plan.node(root) {
            Some(PhysicalNode::Limit { input, .. }) => *input,
            _ => root,
        };
        while let Some(PhysicalNode::Projection { input, .. }) = plan.node(id) {
            id = *input;
        }
        let final_sort = matches!(plan.node(id), Some(PhysicalNode::Sort { .. })).then_some(id);
        for (id, node) in plan.live() {
            let Some(cut) = node.row_cut() else {
                continue;
            };
            let required = match node {
                PhysicalNode::Limit { .. } => id == root && limit == Some(cut),
                PhysicalNode::Sort { .. } => Some(id) == final_sort && limit == Some(cut),
                PhysicalNode::Scan {
                    ranked: Some(_), ..
                } => true,
                _ => false,
            };
            if !required {
                return Err(ValidationError::violated(
                    "row cut",
                    format!(
                        "node {id} ({}) keeps at most {cut} rows; the query's `limit` is {limit:?} and requires no such cut there",
                        node.name()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The candidate cap, probe cap and overfetch ladder a ranked scan
    /// declares, from the cut its scope requires: an ordering `nearest`
    /// fetches the `limit` and may widen it by the declared ladder, a
    /// fusion's `nearest` arm fetches the `limit` (or the default arm size)
    /// and never widens, and `bm25` fetches every match.
    fn check_access(
        &self,
        plan: &PhysicalPlan,
        type_key: &str,
        access: &RankedAccess,
    ) -> Result<(), ValidationError> {
        match (access.kind, access.eligibility) {
            (RankKind::Nearest, Eligibility::AfterScoring) => {
                return Err(ValidationError::violated(
                    "declared policy",
                    "a nearest scan draws its candidates from eligible rows; it filters before scoring",
                ));
            }
            (RankKind::Bm25, Eligibility::BeforeScoring) => {
                let recorded = plan
                    .assumptions()
                    .full_text
                    .get(&Assumptions::full_text_key(type_key, &access.property));
                if recorded != Some(&FullTextCoverage::Full) {
                    return Err(ValidationError::violated(
                        "prerequisite",
                        format!(
                            "the bm25 scan of `{type_key}.{}` filters before scoring under recorded coverage {recorded:?}; only full coverage keeps its scores independent of the filter",
                            access.property
                        ),
                    ));
                }
            }
            _ => {}
        }
        let limit = self.limit.and_then(|limit| usize::try_from(limit).ok());
        let (fetch, overfetch) = match (access.kind, access.scope) {
            (RankKind::Bm25, _) => (None, Vec::new()),
            (RankKind::Nearest, RankScope::Order) => {
                (limit, limit.map(OverfetchRung::ladder).unwrap_or_default())
            }
            (RankKind::Nearest, RankScope::Primary | RankScope::Secondary) => {
                (Some(limit.unwrap_or(RRF_NEAREST_ARM_K)), Vec::new())
            }
        };
        if access.fetch != fetch || access.overfetch != overfetch {
            return Err(ValidationError::violated(
                "row cut",
                format!(
                    "the {:?} scan fetches {:?} with overfetch {:?}; the query's cut requires {fetch:?} with {overfetch:?}",
                    access.kind, access.fetch, access.overfetch
                ),
            ));
        }
        let setting = plan
            .assumptions()
            .settings
            .get(omnigraph_compiler::settings::SettingId::AnnNprobes.name());
        let nprobes = match access.kind {
            RankKind::Nearest => setting
                .map(|value| match value.as_str() {
                    "0" => None,
                    value => value.parse::<usize>().ok(),
                })
                .ok_or_else(|| {
                    ValidationError::violated(
                        "declared policy",
                        "a nearest scan's probe cap was read from no recorded `ann_nprobes`",
                    )
                })?,
            RankKind::Bm25 => None,
        };
        if access.nprobes != nprobes {
            return Err(ValidationError::violated(
                "declared policy",
                format!(
                    "the {:?} scan probes {:?}; the recorded setting is {nprobes:?}",
                    access.kind, access.nprobes
                ),
            ));
        }
        Ok(())
    }

    /// The plan's projection is the query's `return`, item for item, and
    /// every projected score is the score of the retrieval it names.
    fn check_returns(
        &self,
        plan: &PhysicalPlan,
        top: &[NodeId],
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        budget.visit(u64::try_from(self.returns.len()).unwrap_or(u64::MAX))?;
        let split = self.returns.iter().any(|item| {
            contains_aggregate(&item.expr) && !matches!(item.expr, IRExpr::Aggregate { .. })
        });
        let projections = projections_of(plan, top);
        let returns = if split && projections.len() == 2 {
            super::returns::check_split_returns(&self.returns, plan, top, budget)?;
            &self.returns
        } else if let [returns] = projections.as_slice()
            && !split
        {
            returns
        } else {
            return Err(ValidationError::violated(
                "projection",
                format!(
                    "the plan has {} projections of the query's `return`; it needs {}",
                    projections.len(),
                    if split { 2 } else { 1 }
                ),
            ));
        };
        if returns.len() != self.returns.len() {
            return Err(ValidationError::violated(
                "projection",
                format!(
                    "the plan returns {} items; the query returns {}",
                    returns.len(),
                    self.returns.len()
                ),
            ));
        }
        for (required, projected) in self.returns.iter().zip(returns.iter()) {
            if required != projected {
                return Err(ValidationError::violated(
                    "projection",
                    format!(
                        "the plan returns `{}`; the query returns `{}`",
                        projected.expr, required.expr
                    ),
                ));
            }
        }
        for (_, call) in &self.projected_ranks {
            let origin = self.written_search.as_ref().is_some_and(|lead| {
                lead == call
                    || matches!(lead, Expr::Rrf { primary, secondary, .. }
                        if **primary == *call || **secondary == *call)
            });
            let planned = match call {
                Expr::Rrf { .. } => plan
                    .live()
                    .any(|(_, node)| matches!(node, PhysicalNode::RankFuse { .. })),
                call => projected_retrieval(call).is_some_and(|(kind, binding, property)| {
                    ranked_scans(plan).into_iter().any(|(_, scanned, access)| {
                        scanned == binding && access.kind == kind && access.property == property
                    })
                }),
            };
            if !origin || !planned {
                return Err(ValidationError::violated(
                    "score origin",
                    format!(
                        "the projected `{call}` reads a score no retrieval of the query with these arguments produces"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// A root hydration (`HydrateColumns`) reads only written return items:
    /// each deferred column is the bare property the query returns at its
    /// position, under that item's column, read from the pinned table of the
    /// binding's own scan; only cuts and sorts sit between it and the
    /// projection.
    fn check_hydration(&self, plan: &PhysicalPlan, top: &[NodeId]) -> Result<(), ValidationError> {
        for id in top {
            let Some(PhysicalNode::HydrateColumns { input, bindings }) = plan.node(*id) else {
                continue;
            };
            if *id != plan.root() {
                return Err(ValidationError::violated(
                    "hydration",
                    "a hydration below the plan's root reads columns after rows are decided",
                ));
            }
            let mut below = *input;
            while let Some(PhysicalNode::Limit { input, .. } | PhysicalNode::Sort { input, .. }) =
                plan.node(below)
            {
                below = *input;
            }
            if !matches!(plan.node(below), Some(PhysicalNode::Projection { .. })) {
                return Err(ValidationError::violated(
                    "hydration",
                    "a hydration does not sit over the query's projection",
                ));
            }
            for hydrated in bindings {
                let scanned = top.iter().any(|scan| {
                    matches!(plan.node(*scan), Some(PhysicalNode::Scan { spec, .. })
                        if spec.binding.as_deref() == Some(hydrated.binding.as_str())
                            && spec.table == hydrated.table)
                });
                let declared = matches!(self.bindings.get(&hydrated.binding),
                    Some(BoundVariable::Node { type_name })
                        if hydrated.table.node_type_name() == Some(type_name.as_str()));
                if !scanned || !declared {
                    return Err(ValidationError::violated(
                        "hydration",
                        format!(
                            "the hydration of `${}` reads {}, which is not the table its scan reads",
                            hydrated.binding, hydrated.table.type_key
                        ),
                    ));
                }
                for column in &hydrated.columns {
                    let returned = self.returns.get(column.position).is_some_and(|item| {
                        item.column == column.output
                            && matches!(&item.expr, IRExpr::PropAccess { variable, property, .. }
                                if *variable == hydrated.binding && *property == column.property)
                    });
                    if !returned {
                        return Err(ValidationError::violated(
                            "hydration",
                            format!(
                                "the hydration places `${}.{}` at return position {} as `{}`, which the query does not return there",
                                hydrated.binding, column.property, column.position, column.output
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// The final sort's comparator as RFC 0047 defines it: a ranking's score
    /// (a fused ranking's `_rrf` descending) then the written keys; over a
    /// search-led aggregate or a `return distinct` the written keys then every
    /// group key not written, ascending; no sort where the query orders nothing.
    fn required_keys(&self) -> Option<Vec<IROrdering>> {
        if self.aggregate {
            // Under a search, groups have no order unless a key remains
            // after the score; otherwise any written clause sorts them.
            if !self.ordered || (self.search.is_some() && self.order.is_empty()) {
                return None;
            }
            // A search-led sort, or a `return distinct` (groups without an
            // aggregate), is total through every group key it did not write.
            let distinct = !self
                .returns
                .iter()
                .any(|projection| contains_aggregate(&projection.expr));
            let mut keys = self.order.clone();
            if self.search.is_some() || distinct {
                for projection in &self.returns {
                    if contains_aggregate(&projection.expr) {
                        continue;
                    }
                    let written = keys.iter().any(|key| match &key.expr {
                        IRExpr::AliasRef(alias, _) => *alias == projection.column,
                        expr => *expr == projection.expr,
                    });
                    if !written {
                        keys.push(IROrdering {
                            expr: IRExpr::AliasRef(
                                projection.column.clone(),
                                projection.ty.clone(),
                            ),
                            descending: false,
                        });
                    }
                }
            }
            return Some(keys);
        }
        let lead = match &self.search {
            None if !self.ordered => return None,
            None => Vec::new(),
            Some(Search::Rank(retrieval)) => {
                let (column, descending) = retrieval.kind.score();
                vec![(retrieval.binding.as_str(), column, descending)]
            }
            Some(Search::Fuse { arms, .. }) => vec![(arms[0].binding.as_str(), RRF_COLUMN, true)],
        };
        Some(
            lead.into_iter()
                .map(|(binding, column, descending)| IROrdering {
                    expr: IRExpr::PropAccess {
                        variable: binding.to_string(),
                        property: column.to_string(),
                        ty: ExprType::Value {
                            scalar: ScalarType::F64,
                            list: false,
                            nullable: false,
                        },
                    },
                    descending,
                })
                .chain(self.order.iter().cloned())
                .collect(),
        )
    }

    /// The plan's rows leave in the order the query requires and are cut
    /// where it requires: the final sort's comparator is
    /// [`Self::required_keys`] then the binding identities, the `limit` cuts
    /// its output, and nothing between them reorders or drops rows.
    fn check_order_and_cut(
        &self,
        plan: &PhysicalPlan,
        top: &[NodeId],
        input: &AcceptInput<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let mut id = below_hydration(plan, plan.root());
        if let Some(limit) = self.limit {
            let rows = usize::try_from(limit).unwrap_or(usize::MAX);
            match plan.node(id) {
                Some(PhysicalNode::Limit { input, rows: cut }) if *cut == rows => id = *input,
                _ => {
                    return Err(ValidationError::violated(
                        "row cut",
                        format!("the plan's root does not cut its rows to the `limit` {limit}"),
                    ));
                }
            }
        }
        let Some(required) = self.required_keys() else {
            let sorted = top
                .iter()
                .any(|id| matches!(plan.node(*id), Some(PhysicalNode::Sort { .. })));
            if sorted {
                return Err(ValidationError::violated(
                    "order",
                    "the plan sorts rows the query does not order",
                ));
            }
            return Ok(());
        };
        loop {
            match plan.node(id) {
                Some(PhysicalNode::Sort { .. }) => break,
                Some(PhysicalNode::Projection { input, .. }) => id = *input,
                _ => {
                    return Err(ValidationError::violated(
                        "order",
                        "no sort orders the rows the query's root returns",
                    ));
                }
            }
        }
        let Some(PhysicalNode::Sort {
            order_by,
            fetch,
            tiebreak,
            ..
        }) = plan.node(id)
        else {
            unreachable!("the loop stops at a sort");
        };
        budget.visit(u64::try_from(top.len()).unwrap_or(u64::MAX))?;
        if derived_order(plan, plan.root()) != derived_order(plan, id) {
            return Err(ValidationError::violated(
                "order",
                "the plan's root does not leave its rows in the final sort's order",
            ));
        }
        let expected_fetch = self.limit.and_then(|limit| usize::try_from(limit).ok());
        if *fetch != expected_fetch {
            return Err(ValidationError::violated(
                "row cut",
                format!(
                    "the final sort keeps {fetch:?} rows; the query's `limit` is {expected_fetch:?}"
                ),
            ));
        }
        let lead = required.len() - self.order.len().min(required.len());
        let same = order_by.len() == required.len()
            && order_by
                .iter()
                .zip(&required)
                .enumerate()
                .all(|(index, (planned, key))| {
                    if !self.aggregate && index < lead {
                        // A score key reads the ranked scan's (or the fusion's)
                        // score column of the binding, in the score's direction.
                        planned.descending == key.descending
                            && matches!((&planned.expr, &key.expr), (
                            IRExpr::PropAccess { variable, property, .. },
                            IRExpr::PropAccess { variable: v, property: p, .. },
                        ) if variable == v && property == p)
                    } else {
                        planned == key
                    }
                });
        if !same {
            let (planned, wanted) = (keys_text(order_by), keys_text(&required));
            let detail = if planned == wanted {
                format!(
                    "the final sort orders by {planned} with stored types the query does not give its keys"
                )
            } else {
                format!("the final sort orders by {planned}; the query requires {wanted}")
            };
            return Err(ValidationError::violated("order", detail));
        }
        self.check_tiebreak(order_by, tiebreak, input)?;
        Ok(())
    }

    /// The sort's metadata keys complete its comparator as RFC 0047 defines:
    /// every written binding's identity (`@type` then `@id` for a selected
    /// edge) in binding-name order, ascending, nulls first. Three planner
    /// omissions are comparator equivalences and accepted: a key the
    /// comparator already holds, every key when the query returns only order
    /// keys or constants (rows that tie are indistinguishable), and identities of
    /// bindings the query does not name (anonymous traversal endpoints)
    /// after every named one. A sort of group rows appends none.
    fn check_tiebreak(
        &self,
        order_by: &[IROrdering],
        tiebreak: &[ColumnRef],
        input: &AcceptInput<'_>,
    ) -> Result<(), ValidationError> {
        if self.aggregate {
            if !tiebreak.is_empty() {
                return Err(ValidationError::violated(
                    "order",
                    "a sort of group rows appends binding identities",
                ));
            }
            return Ok(());
        }
        let ranked_score = |index: usize| {
            matches!(self.search, Some(Search::Rank(_)))
                && self.projected_ranks.iter().any(|(position, call)| {
                    *position == index && self.written_search.as_ref() == Some(call)
                })
        };
        // A key orders the expression it names, so it covers every return
        // item lowering to that expression, under any alias.
        let covered = self.returns.iter().enumerate().all(|(index, projection)| {
            ranked_score(index)
                || matches!(projection.expr, IRExpr::Literal(_, _))
                || order_by
                    .iter()
                    .any(|key| *named_expr(&self.returns, &key.expr) == projection.expr)
        });
        if covered {
            return Ok(());
        }
        let columns = input.catalog.system_columns;
        let keyed = |column: &ColumnRef| {
            let physical = match column.property.as_deref() {
                Some(IDENTITY_MEMBER) => columns.id,
                Some(EDGE_TYPE_MEMBER) => omnigraph_compiler::traversal::EDGE_TYPE_COLUMN,
                _ => return false,
            };
            order_by.iter().any(|key| {
                matches!(named_expr(&self.returns, &key.expr), IRExpr::PropAccess { variable, property, .. }
                    if *variable == column.binding && property == physical)
            })
        };
        let mut expected = Vec::new();
        for name in self.bindings.keys() {
            if self.selected_edges.contains(name) {
                expected.push(ColumnRef::property(name, EDGE_TYPE_MEMBER));
            }
            expected.push(ColumnRef::property(name, IDENTITY_MEMBER));
        }
        expected.retain(|column| !keyed(column));
        let planned: Vec<&ColumnRef> = tiebreak.iter().filter(|column| !keyed(column)).collect();
        let named = planned.len().min(expected.len());
        let (head, tail) = planned.split_at(named);
        let head_ok = head.iter().copied().eq(expected.iter());
        let tail_ok = tail
            .iter()
            .all(|column| !self.bindings.contains_key(&column.binding));
        if !head_ok || named != expected.len() || !tail_ok {
            return Err(ValidationError::violated(
                "order",
                format!(
                    "the final sort breaks ties by {:?}; the query's comparator requires {:?} first",
                    tiebreak.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    expected.iter().map(ToString::to_string).collect::<Vec<_>>()
                ),
            ));
        }
        Ok(())
    }
}

/// The adaptive search policies the plan declares (RFC 0047, "Execution and
/// replay use the same acceptance path"): every `nearest` scan's probe
/// ladder terminates and keeps its correctness fallbacks, and every
/// pre-pass draws its eligible set from required first hops, feeds only the
/// scans its kind may prefilter, decides an empty set as its kind must, and
/// guards BM25 scans by their recorded coverage. The gate policy's
/// thresholds are finite.
fn check_policies(plan: &PhysicalPlan, budget: &mut Budget) -> Result<(), ValidationError> {
    let policy = plan.assumptions().gate_policy;
    if !policy.ratio.is_finite() || policy.ratio < 0.0 {
        return Err(ValidationError::violated(
            "declared policy",
            format!(
                "the gate admits by ratio {}, which is no finite share",
                policy.ratio
            ),
        ));
    }
    for (id, node) in plan.live() {
        budget.visit(1)?;
        match node {
            PhysicalNode::Scan {
                ranked: Some(access),
                spec,
                ..
            } => {
                match (access.kind, access.policy) {
                    (RankKind::Nearest, Some(policy)) => {
                        if policy.probe_factor < 2
                            || !policy.flat_rescan_on_unreached
                            || !policy.uncapped_on_missing_counters
                        {
                            return Err(ValidationError::violated(
                                "declared policy",
                                format!(
                                    "the nearest scan's policy {policy:?} lets its probe ladder stall or drops a fallback that keeps its rows in order"
                                ),
                            ));
                        }
                    }
                    (RankKind::Bm25, None) => {}
                    (kind, policy) => {
                        return Err(ValidationError::violated(
                            "declared policy",
                            format!("a {kind:?} scan declares the nearest policy {policy:?}"),
                        ));
                    }
                }
                if let Some(prefilter) = &access.prefilter {
                    if access.kind != RankKind::Nearest
                        || access.scope != RankScope::Order
                        || prefilter.feeds != [id]
                        || prefilter.on_empty != EmptyEligible::ProvenEmpty
                    {
                        return Err(ValidationError::violated(
                            "declared policy",
                            "only a standalone nearest scan carries its own pre-pass, feeding itself and proving an empty eligible set empty",
                        ));
                    }
                    let binding = spec.binding.as_deref().unwrap_or_default();
                    check_prefilter(plan, plan.root(), binding, prefilter, budget)?;
                }
            }
            PhysicalNode::RankFuse {
                arms, prefilter, ..
            } => {
                if prefilter.on_empty != EmptyEligible::Postfilter {
                    return Err(ValidationError::violated(
                        "declared policy",
                        "a fusion's empty eligible set proves nothing; its arms run unfiltered",
                    ));
                }
                let mut admits = true;
                for feed in &prefilter.feeds {
                    match plan.node(*feed) {
                        Some(PhysicalNode::Scan {
                            spec,
                            ranked: Some(ranked),
                            ..
                        }) if ranked.kind == RankKind::Bm25 => {
                            admits &=
                                plan.assumptions()
                                    .full_text
                                    .get(&Assumptions::full_text_key(
                                        &spec.table.type_key,
                                        &ranked.property,
                                    ))
                                    == Some(&FullTextCoverage::Full);
                        }
                        _ => {
                            return Err(ValidationError::violated(
                                "declared policy",
                                "a fusion's pre-pass feeds only its bm25 arms",
                            ));
                        }
                    }
                }
                if admits != prefilter.coverage_admits {
                    return Err(ValidationError::violated(
                        "prerequisite",
                        format!(
                            "the fusion's pre-pass claims coverage admits {}; the recorded coverage of its bm25 arms admits {admits}",
                            prefilter.coverage_admits
                        ),
                    ));
                }
                check_prefilter(plan, arms[0].input, &arms[0].binding, prefilter, budget)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Every hop of a pre-pass is a required first hop of `binding` in the
/// pipeline under `root`: a top-level traversal leaving the scanned binding
/// over that one edge type in that direction, at least one hop long. Such
/// an eligible set holds every row that can survive the traversal.
fn check_prefilter(
    plan: &PhysicalPlan,
    root: NodeId,
    binding: &str,
    prefilter: &crate::physical::Prefilter,
    budget: &mut Budget,
) -> Result<(), ValidationError> {
    if prefilter.hops.is_empty() {
        return Ok(());
    }
    let nodes = pipeline(plan, root, budget)?;
    let scanned = nodes.iter().any(|id| {
        matches!(plan.node(*id), Some(PhysicalNode::Scan { spec, .. })
            if spec.binding.as_deref() == Some(binding)
                && spec.table.node_type_name() == Some(prefilter.ranked_type.as_str()))
    });
    let introduced = nodes.iter().any(
        |id| matches!(plan.node(*id), Some(PhysicalNode::Expand { dst, .. }) if dst == binding),
    );
    if !scanned || introduced {
        return Err(ValidationError::violated(
            "declared policy",
            format!(
                "the pre-pass of `${binding}` filters a binding no scan of its type introduces"
            ),
        ));
    }
    for hop in &prefilter.hops {
        let required = nodes.iter().any(|id| {
            matches!(plan.node(*id), Some(PhysicalNode::Expand { src, edges, min_hops, .. })
            if src == binding
                && *min_hops > 0
                && edges.named().is_some_and(|member| {
                    member.edge_type == hop.edge_type && member.direction == hop.direction
                }))
        });
        if !required {
            return Err(ValidationError::violated(
                "declared policy",
                format!(
                    "the pre-pass of `${binding}` requires a {} {:?} hop no traversal of the query requires",
                    hop.edge_type, hop.direction
                ),
            ));
        }
    }
    Ok(())
}

/// The node under a root hydration, else the root.
fn below_hydration(plan: &PhysicalPlan, root: NodeId) -> NodeId {
    match plan.node(root) {
        Some(PhysicalNode::HydrateColumns { input, .. }) => *input,
        _ => root,
    }
}

/// Every node of `top` that projects the query's `return`.
fn projections_of<'p>(plan: &'p PhysicalPlan, top: &[NodeId]) -> Vec<&'p Vec<IRProjection>> {
    top.iter()
        .filter_map(|id| match plan.node(*id) {
            Some(
                PhysicalNode::Projection { return_exprs, .. }
                | PhysicalNode::Aggregate { return_exprs, .. }
                | PhysicalNode::MetadataCount { return_exprs, .. },
            ) => Some(return_exprs),
            _ => None,
        })
        .collect()
}

fn keys_text(keys: &[IROrdering]) -> String {
    keys.iter()
        .map(|key| format!("{}{}", key.expr, if key.descending { " desc" } else { "" }))
        .collect::<Vec<_>>()
        .join(", ")
}

fn exactness(approximate: bool) -> &'static str {
    if approximate { "approximate" } else { "exact" }
}

/// The (kind, binding, property) a projected `nearest` or `bm25` call reads.
fn projected_retrieval(expr: &Expr) -> Option<(RankKind, &str, &str)> {
    match expr {
        Expr::Nearest {
            variable, property, ..
        } => Some((RankKind::Nearest, variable, property)),
        Expr::Bm25 { field, .. } => match field.as_ref() {
            Expr::PropAccess { variable, property } => Some((RankKind::Bm25, variable, property)),
            _ => None,
        },
        _ => None,
    }
}
