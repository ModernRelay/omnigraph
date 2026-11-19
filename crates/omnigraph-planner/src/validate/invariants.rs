//! The checks every accepted plan passes (RFC 0047, "Checks have explicit
//! boundaries"): binding and traversal identity, the eligibility predicates
//! (every required conjunct placed, every placed conjunct required),
//! correlated blocks, search identity, the
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

use std::collections::{HashMap, HashSet};

use super::requirements::{BoundVariable, Region, TieColumn};
use omnigraph_compiler::ir::{IRExpr, IROrdering, IRProjection};
use omnigraph_compiler::types::{ExprType, ScalarType};

use super::budget::Budget;
use super::requirements::{
    Block, ExactScore, FuseRequirements, Requirements, Retrieval, Search, constant_true,
};
use super::{AcceptInput, ValidationError};
use crate::cost::AntiJoinStrategy;
use crate::logical::{ColumnRef, EDGE_TYPE_MEMBER, IDENTITY_MEMBER};
use crate::optimizer::{arm_score, contains_aggregate, derived_order};
use crate::physical::{
    NodeId, OverfetchRung, PhysicalNode, PhysicalPlan, RankScope, RankedAccess, score_kind_name,
};
use crate::read_shape::ArmRank;
use omnigraph_compiler::ir::core::ScoreKind;
use omnigraph_compiler::ir::expr::ScalarFunc;
use omnigraph_compiler::query::value::SCORE_COLUMN;
use omnigraph_compiler::schema::analysis::FullTextScorer;

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

/// Every conjunct a pipeline's nodes test, wherever placement put it, with
/// the node that tests it.
pub(super) fn conjuncts(plan: &PhysicalPlan, nodes: &[NodeId]) -> Vec<(NodeId, IRExpr)> {
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

/// The region path of every node of the scope under `root`
/// ([`Region`]): an `OptionalJoin`'s inner tree is inside its optional, each
/// arm of a `Union` fold (a left spine of unions over the same bindings) is
/// inside that arm, a `Correlate`'s inner tree is inside its read, and a
/// `TargetCut`'s input is below its window. A block's inner tree and a
/// fusion's arms are scopes of their own and are not walked.
///
/// The walk numbers each `OptionalJoin` and `Correlate` within its region
/// after every node of its input side, the first input of a node before its
/// second: the order the planner builds them in, one written element
/// wrapping the rows of the elements before it, and the order the
/// requirements number the written ones in.
fn plan_regions(
    plan: &PhysicalPlan,
    root: NodeId,
    budget: &mut Budget,
) -> Result<std::collections::HashMap<NodeId, Vec<Region>>, ValidationError> {
    fn fold_arms(
        plan: &PhysicalPlan,
        id: NodeId,
        bindings: &[String],
    ) -> (Vec<NodeId>, Vec<NodeId>) {
        match plan.node(id) {
            Some(PhysicalNode::Union {
                left,
                right,
                bindings: own,
            }) if own == bindings => {
                let (mut spine, mut arms) = fold_arms(plan, *left, bindings);
                spine.push(id);
                arms.push(*right);
                (spine, arms)
            }
            _ => (Vec::new(), vec![id]),
        }
    }
    enum Visit {
        /// The node's region is its path; its input side comes next.
        Enter(NodeId, Vec<Region>),
        /// Its input side is numbered: the node takes its ordinal and its
        /// inner tree is walked inside it.
        Leave(NodeId, Vec<Region>),
    }
    let mut regions = std::collections::HashMap::new();
    let mut ordinals = super::requirements::RegionOrdinals::default();
    let mut pending = vec![Visit::Enter(root, Vec::new())];
    while let Some(visit) = pending.pop() {
        let (id, path) = match visit {
            Visit::Enter(id, path) => (id, path),
            Visit::Leave(id, path) => {
                let within = |region: Region| {
                    let mut nested = path.clone();
                    nested.push(region);
                    nested
                };
                match plan.node(id) {
                    Some(PhysicalNode::OptionalJoin {
                        inner, outer_var, ..
                    }) => pending.push(Visit::Enter(
                        *inner,
                        within(ordinals.optional(&path, outer_var)),
                    )),
                    Some(PhysicalNode::Correlate {
                        inner, outer_var, ..
                    }) => pending.push(Visit::Enter(
                        *inner,
                        within(ordinals.correlate(&path, outer_var)),
                    )),
                    _ => {}
                }
                continue;
            }
        };
        budget.visit(1)?;
        let Some(node) = plan.node(id) else {
            continue;
        };
        let within = |region: Region| {
            let mut nested = path.clone();
            nested.push(region);
            nested
        };
        match node {
            PhysicalNode::OptionalJoin { input, .. } => {
                pending.push(Visit::Leave(id, path.clone()));
                pending.push(Visit::Enter(*input, path.clone()));
            }
            PhysicalNode::Correlate { input, .. } => {
                pending.push(Visit::Leave(id, path.clone()));
                if let Some(input) = input {
                    pending.push(Visit::Enter(*input, path.clone()));
                }
            }
            PhysicalNode::Union { bindings, .. } => {
                let (spine, arms) = fold_arms(plan, id, bindings);
                for node in spine {
                    regions.insert(node, path.clone());
                }
                for (position, arm) in arms.into_iter().enumerate() {
                    pending.push(Visit::Enter(
                        arm,
                        within(Region::UnionArm(bindings.clone(), position)),
                    ));
                }
                continue;
            }
            PhysicalNode::TargetCut { input, .. } => {
                pending.push(Visit::Enter(*input, within(Region::Window)))
            }
            PhysicalNode::AntiJoin { input, .. } => {
                pending.push(Visit::Enter(*input, path.clone()))
            }
            PhysicalNode::Fuse { .. } => {}
            // The first input is walked first: it is the last pushed.
            other => pending.extend(
                other
                    .inputs()
                    .into_iter()
                    .rev()
                    .map(|input| Visit::Enter(input, path.clone())),
            ),
        }
        regions.insert(id, path);
    }
    Ok(regions)
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

/// An exact `bm25` is `bm25_v1` of a full-text field over its field corpus:
/// its retained predicate is `match_terms` over the scored `$binding.property`
/// (the membership, evaluated before any cut), and the corpus type declares
/// a full-text index with the ranking scorer on that property.
fn check_exact_bm25(score: &ExactScore, input: &AcceptInput<'_>) -> Result<(), ValidationError> {
    let retained = matches!(
        &score.query,
        IRExpr::Call {
            func: ScalarFunc::MatchTerms { .. },
            args,
            ..
        } if matches!(
            args.first(),
            Some(IRExpr::PropAccess { variable, property, .. })
                if *variable == score.binding && *property == score.property
        ) && args.len() == 3
    );
    if !retained {
        return Err(ValidationError::violated(
            "predicate retention",
            format!(
                "the exact `bm25` of `${}.{}` retains `{}`, not the `match_terms` of its field",
                score.binding, score.property, score.query
            ),
        ));
    }
    let corpus = input
        .catalog
        .node_types
        .get(&score.type_name)
        .is_some_and(|node| {
            node.is_lexical_field(&score.property)
                && node.full_text_scorer(&score.property) == FullTextScorer::Bm25V1
        });
    if !corpus {
        return Err(ValidationError::violated(
            "scoring corpus",
            format!(
                "the exact `bm25` scores `{}.{}`, which declares no full-text index with the `bm25_v1` scorer",
                score.type_name, score.property
            ),
        ));
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
        self.check_scope(plan, plan.root(), true, input, budget)?;
        check_policies(plan, budget)?;
        Ok(())
    }

    /// The checks of one scope over the pipeline under `root`: the read's
    /// own (`outermost`), with its projection, hydration, order and cut, or
    /// a fusion arm's, whose rows the fusion ranks. A fusion's arms are
    /// scopes of their own (`check_fusion`), so the scope's nodes stop at
    /// them.
    fn check_scope(
        &self,
        plan: &PhysicalPlan,
        root: NodeId,
        outermost: bool,
        input: &AcceptInput<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let mut top = pipeline(plan, root, budget)?;
        let mut held = HashSet::new();
        match &self.fuse {
            Some(fuse) => {
                held = self.check_fusion(plan, &top, fuse, input, budget)?;
                top.retain(|id| !held.contains(id));
            }
            None => {
                if top
                    .iter()
                    .any(|id| matches!(plan.node(*id), Some(PhysicalNode::Fuse { .. })))
                {
                    return Err(ValidationError::violated(
                        "search identity",
                        "the plan fuses retrievals the query does not fuse",
                    ));
                }
            }
        }
        self.check_bindings(plan, &top)?;
        self.check_eligibility(plan, &top, &top, &self.eligibility, input, budget)?;
        let blocks: Vec<&Block> = self.blocks.iter().collect();
        self.check_blocks(plan, &top, &blocks, budget)?;
        self.check_optionals(plan, &top, budget)?;
        self.check_correlates(plan, &top, budget)?;
        self.check_unions(plan, &top)?;
        self.check_regions(plan, root, &top, budget)?;
        self.check_scores(plan, &top, input)?;
        self.check_lets(plan, &top)?;
        self.check_search(plan, &top)?;
        let scope: Vec<NodeId> = plan
            .subtree(root)
            .into_iter()
            .filter(|id| !held.contains(id))
            .collect();
        self.check_row_cuts(plan, root, &scope, budget)?;
        if outermost {
            self.check_returns(plan, &top, budget)?;
            self.check_hydration(plan, &top)?;
            self.check_order_and_cut(plan, &top, input, budget)?;
        }
        Ok(())
    }

    /// The scope's fusion is the query's: one `Fuse` node outside every
    /// other's arms, of the written target, with the written `k` and
    /// `weights`, one arm per written arm in written order, each ranking as
    /// written, its rows arriving in that rank order (`check_arm_order`) and
    /// its subtree holding exactly the arm's requirements. Returns every
    /// node of the arms' subtrees.
    fn check_fusion(
        &self,
        plan: &PhysicalPlan,
        top: &[NodeId],
        fuse: &FuseRequirements,
        input: &AcceptInput<'_>,
        budget: &mut Budget,
    ) -> Result<HashSet<NodeId>, ValidationError> {
        let mut fusions = Vec::new();
        let mut held = HashSet::new();
        for id in top {
            if let Some(PhysicalNode::Fuse { arms, .. }) = plan.node(*id) {
                fusions.push(*id);
                for arm in arms {
                    held.extend(pipeline(plan, arm.input, budget)?);
                }
            }
        }
        let outer: Vec<NodeId> = fusions
            .into_iter()
            .filter(|id| !held.contains(id))
            .collect();
        let [id] = outer[..] else {
            return Err(ValidationError::violated(
                "search identity",
                format!(
                    "the query fuses one ranking's arms; the plan's scope holds {} fusions",
                    outer.len()
                ),
            ));
        };
        let Some(PhysicalNode::Fuse {
            arms,
            target,
            k,
            weights,
        }) = plan.node(id)
        else {
            unreachable!("`outer` holds fusions");
        };
        if *target != fuse.target {
            return Err(ValidationError::violated(
                "search identity",
                format!(
                    "the fusion ranks `${target}`; the query fuses `${}`",
                    fuse.target
                ),
            ));
        }
        if arms.len() != fuse.arms.len() {
            return Err(ValidationError::violated(
                "search identity",
                format!(
                    "the fusion has {} arms; the query fuses {}",
                    arms.len(),
                    fuse.arms.len()
                ),
            ));
        }
        if *k != fuse.k {
            return Err(ValidationError::violated(
                "search identity",
                "the fusion's `k` is not the one the query writes",
            ));
        }
        if *weights != fuse.weights
            || weights.as_ref().is_some_and(|weights| {
                omnigraph_compiler::ir::rrf_weight_values(weights, arms.len()).is_none()
            })
        {
            return Err(ValidationError::violated(
                "search identity",
                "the fusion's `weights` are not the ones the query writes",
            ));
        }
        let mut nodes = HashSet::new();
        for (index, (arm, (rank, required))) in arms.iter().zip(&fuse.arms).enumerate() {
            if arm.rank != *rank {
                return Err(ValidationError::violated(
                    "search identity",
                    format!(
                        "the fusion's arm {} ranks as {}; the query's arm is {}",
                        index + 1,
                        arm.rank.name(),
                        rank.name()
                    ),
                ));
            }
            check_arm_order(plan, arm, target)?;
            required.check_scope(plan, arm.input, false, input, budget)?;
            nodes.extend(plan.subtree(arm.input));
        }
        Ok(nodes)
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
                // A fusion binds its target; each arm's own scope reads it
                // with its type.
                (Some(PhysicalNode::Fuse { target, .. }), BoundVariable::Node { .. }) => {
                    target == name
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
        required: &[IRExpr],
        input: &AcceptInput<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let placed = conjuncts(plan, reached);
        budget.visit(u64::try_from(placed.len() + self.eligibility.len()).unwrap_or(u64::MAX))?;
        self.check_retained(plan, retained, required)?;
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

    /// Every conjunct of `required` is tested by one of `retained`.
    fn check_retained(
        &self,
        plan: &PhysicalPlan,
        retained: &[NodeId],
        required: &[IRExpr],
    ) -> Result<(), ValidationError> {
        let kept = conjuncts(plan, retained);
        let kept_set: HashSet<&IRExpr> = kept.iter().map(|(_, conjunct)| conjunct).collect();
        if let Some(missing) = required
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
        blocks: &[&Block],
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
        if planned.len() != blocks.len() {
            return Err(ValidationError::violated(
                "predicate retention",
                format!(
                    "the query writes {} correlated blocks there; the plan runs {}",
                    blocks.len(),
                    planned.len()
                ),
            ));
        }
        for block in blocks {
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

    /// Every written `optional` runs as one `OptionalJoin` over its
    /// correlation binding, the conjuncts of its own pattern tested inside
    /// its inner tree and nowhere else the query does not require them: a
    /// conjunct of the optional tested after it would drop the outer rows it
    /// keeps.
    fn check_optionals(
        &self,
        plan: &PhysicalPlan,
        top: &[NodeId],
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let mut planned: Vec<(NodeId, &String, Vec<NodeId>)> = Vec::new();
        for id in top {
            if let Some(PhysicalNode::OptionalJoin {
                outer_var, inner, ..
            }) = plan.node(*id)
            {
                planned.push((*id, outer_var, pipeline(plan, *inner, budget)?));
            }
        }
        if planned.len() != self.optionals.len() {
            return Err(ValidationError::violated(
                "predicate retention",
                format!(
                    "the query writes {} optionals; the plan runs {}",
                    self.optionals.len(),
                    planned.len()
                ),
            ));
        }
        let inside: HashSet<NodeId> = planned
            .iter()
            .flat_map(|(_, _, nodes)| nodes.iter().copied())
            .collect();
        let outside: Vec<NodeId> = top
            .iter()
            .copied()
            .filter(|id| !inside.contains(id))
            .collect();
        // Each conjunct tested outside every optional, with how many times:
        // a correlated read writing the same conjunct tests its own copy
        // there, which is not the optional's.
        let mut tested_outside: HashMap<IRExpr, usize> = HashMap::new();
        for (_, conjunct) in conjuncts(plan, &outside) {
            *tested_outside.entry(conjunct).or_default() += 1;
        }
        let top_level: HashSet<&IRExpr> = self.eligibility.iter().collect();
        // Each query optional takes the plan optional on its binding that
        // tests its conjuncts and the fewest others, the optionals writing
        // the most conjuncts first, so two optionals on one binding (one
        // testing a conjunct, one none) never take each other's join.
        let mut required: Vec<&super::requirements::OptionalMatch> =
            self.optionals.iter().collect();
        required.sort_by_key(|optional| std::cmp::Reverse(optional.eligibility.len()));
        let tested: Vec<HashSet<IRExpr>> = planned
            .iter()
            .map(|(_, _, nodes)| {
                conjuncts(plan, nodes)
                    .into_iter()
                    .map(|(_, conjunct)| conjunct)
                    .collect()
            })
            .collect();
        let mut tested = tested;
        for optional in required {
            let found = planned
                .iter()
                .zip(&tested)
                .enumerate()
                .filter(|(_, ((_, outer_var, _), tested))| {
                    **outer_var == optional.outer_var
                        && optional
                            .eligibility
                            .iter()
                            .all(|conjunct| tested.contains(conjunct))
                })
                .min_by_key(|(_, (_, tested))| tested.len())
                .map(|(index, _)| index);
            let Some(index) = found else {
                return Err(ValidationError::violated(
                    "predicate retention",
                    format!(
                        "no optional of the plan over `${}` tests the conjuncts the query's optional writes",
                        optional.outer_var
                    ),
                ));
            };
            planned.remove(index);
            tested.remove(index);
            if let Some(leaked) = optional.eligibility.iter().find(|conjunct| {
                let correlated = self
                    .correlates
                    .iter()
                    .filter(|correlate| correlate.eligibility.contains(*conjunct))
                    .count();
                tested_outside
                    .get(*conjunct)
                    .is_some_and(|tested| *tested > correlated)
                    && !self.top_level_conjunct(conjunct, &top_level)
            }) {
                return Err(ValidationError::violated(
                    "predicate origin",
                    format!("the plan tests the optional's `{leaked}` outside the optional"),
                ));
            }
        }
        Ok(())
    }

    /// The plan runs one `Correlate` per written `from` over an inline read
    /// with its own group or cut, on the same correlation binding and
    /// returning the same read, each testing its pattern's conjuncts inside
    /// its inner tree and never outside it, where they would run after the
    /// read's group or cut.
    fn check_correlates(
        &self,
        plan: &PhysicalPlan,
        top: &[NodeId],
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let mut planned: Vec<(&Option<String>, &crate::read_shape::InnerRead, Vec<NodeId>)> =
            Vec::new();
        for id in top {
            if let Some(PhysicalNode::Correlate {
                input,
                inner,
                outer_var,
                read,
                ..
            }) = plan.node(*id)
            {
                if input.is_some() != outer_var.is_some() {
                    return Err(ValidationError::violated(
                        "plan shape",
                        format!(
                            "node {id} correlates without both its outer rows and its correlation binding"
                        ),
                    ));
                }
                planned.push((outer_var, read, pipeline(plan, *inner, budget)?));
            }
        }
        if planned.len() != self.correlates.len() {
            return Err(ValidationError::violated(
                "plan shape",
                format!(
                    "the query draws {} inline reads with their own group or cut; the plan correlates {}",
                    self.correlates.len(),
                    planned.len()
                ),
            ));
        }
        let inside: HashSet<NodeId> = planned
            .iter()
            .flat_map(|(_, _, nodes)| nodes.iter().copied())
            .collect();
        let outside: Vec<NodeId> = top
            .iter()
            .copied()
            .filter(|id| !inside.contains(id))
            .collect();
        // Each conjunct tested outside every correlated read, with how many
        // times: an optional writing the same conjunct tests its own copy
        // there, which is not the read's.
        let mut tested_outside: HashMap<IRExpr, usize> = HashMap::new();
        for (_, conjunct) in conjuncts(plan, &outside) {
            *tested_outside.entry(conjunct).or_default() += 1;
        }
        let top_level: HashSet<&IRExpr> = self.eligibility.iter().collect();
        for correlate in &self.correlates {
            let found = planned.iter().position(|(outer_var, read, nodes)| {
                let tested: HashSet<IRExpr> = conjuncts(plan, nodes)
                    .into_iter()
                    .map(|(_, conjunct)| conjunct)
                    .collect();
                **outer_var == correlate.outer_var
                    && **read == correlate.read
                    && correlate
                        .eligibility
                        .iter()
                        .all(|conjunct| tested.contains(conjunct))
            });
            let Some(index) = found else {
                return Err(ValidationError::violated(
                    "predicate retention",
                    format!(
                        "no correlated read of the plan returns `{}` with the conjuncts the query's inline read writes",
                        correlate
                            .read
                            .columns
                            .iter()
                            .map(|column| column.column.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            };
            planned.remove(index);
            if let Some(leaked) = correlate.eligibility.iter().find(|conjunct| {
                // An optional writing the same conjunct tests its own copy
                // outside every correlated read.
                let others = self
                    .optionals
                    .iter()
                    .filter(|optional| optional.eligibility.contains(*conjunct))
                    .count();
                tested_outside
                    .get(*conjunct)
                    .is_some_and(|tested| *tested > others)
                    && !self.top_level_conjunct(conjunct, &top_level)
            }) {
                return Err(ValidationError::violated(
                    "predicate origin",
                    format!("the plan tests the inline read's `{leaked}` outside its read"),
                ));
            }
        }
        Ok(())
    }

    /// The plan runs one `Score` per written exact score, of the same kind,
    /// over the same binding, node type, property and query. An exact
    /// `bm25` retains its `match_terms` predicate over the scored field,
    /// scores against the field corpus of a type whose index declares an
    /// analyzer and the `bm25_v1` scorer, and every read of a binding's
    /// `F64` `_score` is such a score's.
    fn check_scores(
        &self,
        plan: &PhysicalPlan,
        top: &[NodeId],
        input: &AcceptInput<'_>,
    ) -> Result<(), ValidationError> {
        let mut planned: Vec<ExactScore> = top
            .iter()
            .filter_map(|id| match plan.node(*id) {
                Some(PhysicalNode::Score {
                    binding,
                    type_name,
                    property,
                    kind,
                    query,
                    ..
                }) => Some(ExactScore {
                    binding: binding.clone(),
                    type_name: type_name.clone(),
                    property: property.clone(),
                    kind: *kind,
                    query: query.clone(),
                }),
                _ => None,
            })
            .collect();
        let scored: Vec<String> = planned
            .iter()
            .filter(|score| score.kind == ScoreKind::Bm25)
            .map(|score| score.binding.clone())
            .collect();
        if planned.len() != self.scores.len() {
            return Err(ValidationError::violated(
                "plan shape",
                format!(
                    "the query writes {} exact scores; the plan runs {} scores",
                    self.scores.len(),
                    planned.len()
                ),
            ));
        }
        for score in &self.scores {
            let ExactScore {
                binding,
                property,
                query,
                ..
            } = score;
            match planned.iter().position(|planned| planned == score) {
                Some(index) => {
                    planned.remove(index);
                }
                None => {
                    return Err(ValidationError::violated(
                        "search identity",
                        format!(
                            "no score of the plan reads `${binding}.{property}` against `{query}` as the query's {} does",
                            score_kind_name(score.kind)
                        ),
                    ));
                }
            }
            if score.kind == ScoreKind::Bm25 {
                check_exact_bm25(score, input)?;
            }
        }
        // Score origin: a `_score` read as `bm25_v1`'s `F64` comes from an
        // exact `bm25` of its binding.
        let mut reads = Vec::new();
        for expr in self
            .returns
            .iter()
            .map(|projection| &projection.expr)
            .chain(self.order.iter().map(|ordering| &ordering.expr))
            .chain(&self.eligibility)
        {
            expr.visit_nodes(|node| {
                if let IRExpr::PropAccess {
                    variable,
                    property,
                    ty:
                        ExprType::Value {
                            scalar: ScalarType::F64,
                            ..
                        },
                } = node
                    && property == SCORE_COLUMN
                {
                    reads.push(variable.clone());
                }
            });
        }
        if let Some(read) = reads.iter().find(|read| !scored.contains(read)) {
            return Err(ValidationError::violated(
                "score origin",
                format!(
                    "the query reads `${read}.{SCORE_COLUMN}`, which no exact `bm25` of the plan produces"
                ),
            ));
        }
        Ok(())
    }

    /// The plan runs one `Let` per `let`, computing the same value: a plan
    /// without one would read a column nothing computes, or skip a value's
    /// failure on a row `where` removes.
    fn check_lets(&self, plan: &PhysicalPlan, top: &[NodeId]) -> Result<(), ValidationError> {
        let mut planned: Vec<&IRExpr> = top
            .iter()
            .filter_map(|id| match plan.node(*id) {
                Some(PhysicalNode::Let { value, .. }) => Some(value),
                _ => None,
            })
            .collect();
        if planned.len() != self.lets.len() {
            return Err(ValidationError::violated(
                "plan shape",
                format!(
                    "the query has {} `let` values; the plan computes {}",
                    self.lets.len(),
                    planned.len()
                ),
            ));
        }
        for value in &self.lets {
            match planned.iter().position(|v| *v == value) {
                Some(index) => {
                    planned.remove(index);
                }
                None => {
                    return Err(ValidationError::violated(
                        "predicate retention",
                        format!(
                            "no `Let` of the plan evaluates `{value}` as the query's `let` does"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// The plan runs one `Union` per arm after the first of each written
    /// `union`, each over the names the union binds.
    fn check_unions(&self, plan: &PhysicalPlan, top: &[NodeId]) -> Result<(), ValidationError> {
        let mut planned: Vec<&Vec<String>> = top
            .iter()
            .filter_map(|id| match plan.node(*id) {
                Some(PhysicalNode::Union { bindings, .. }) => Some(bindings),
                _ => None,
            })
            .collect();
        if planned.len() != self.unions.len() {
            return Err(ValidationError::violated(
                "plan shape",
                format!(
                    "the query's unions fold {} arms; the plan runs {} unions",
                    self.unions.len(),
                    planned.len()
                ),
            ));
        }
        for bindings in &self.unions {
            match planned.iter().position(|planned| *planned == bindings) {
                Some(index) => {
                    planned.remove(index);
                }
                None => {
                    return Err(ValidationError::violated(
                        "binding identity",
                        format!("no union of the plan binds {bindings:?} as the query's does"),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Every conjunct is tested in the regions of the scope that write it
    /// ([`Region`]) and in no other: a required conjunct tested only in
    /// another region, or a copy tested in a region that does not write it,
    /// is refused. A conjunct the query writes nowhere is the eligibility
    /// check's.
    fn check_regions(
        &self,
        plan: &PhysicalPlan,
        root: NodeId,
        top: &[NodeId],
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let regions = plan_regions(plan, root, budget)?;
        let placed: Vec<(IRExpr, &[Region])> = conjuncts(plan, top)
            .into_iter()
            .map(|(id, conjunct)| (conjunct, regions.get(&id).map_or(&[][..], Vec::as_slice)))
            .collect();
        budget
            .visit(u64::try_from(placed.len() * self.regions.len().max(1)).unwrap_or(u64::MAX))?;
        let describe = |path: &[Region]| -> String {
            if path.is_empty() {
                return "the scope".to_string();
            }
            path.iter()
                .map(|region| match region {
                    Region::Optional(var, ordinal) => {
                        format!("optional {} over `${var}`", ordinal + 1)
                    }
                    Region::UnionArm(_, position) => format!("union arm {}", position + 1),
                    Region::Correlate(_, ordinal) => {
                        format!("correlated read {}", ordinal + 1)
                    }
                    Region::Window => "below the window's cut".to_string(),
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        for (conjunct, path) in &self.regions {
            if !placed
                .iter()
                .any(|(tested, at)| tested == conjunct && *at == path.as_slice())
            {
                return Err(ValidationError::violated(
                    "predicate retention",
                    format!(
                        "the query tests `{conjunct}` in {}; the plan does not test it there",
                        describe(path)
                    ),
                ));
            }
        }
        for (conjunct, at) in &placed {
            if constant_true(conjunct) {
                continue;
            }
            let written: Vec<&Vec<Region>> = self
                .regions
                .iter()
                .filter(|(own, _)| own == conjunct)
                .map(|(_, path)| path)
                .collect();
            if !written.is_empty() && !written.iter().any(|path| path.as_slice() == *at) {
                return Err(ValidationError::violated(
                    "predicate origin",
                    format!(
                        "the plan tests `{conjunct}` in {}; the query writes it in {}",
                        describe(at),
                        describe(written[0])
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Whether `conjunct` is required outside every `optional`: the scope's
    /// eligibility holds each optional's conjuncts too, so one written in
    /// `where` as well counts once more than the optionals write it.
    fn top_level_conjunct(&self, conjunct: &IRExpr, eligibility: &HashSet<&IRExpr>) -> bool {
        let in_optionals = self
            .optionals
            .iter()
            .flat_map(|optional| optional.eligibility.iter())
            .chain(
                self.correlates
                    .iter()
                    .flat_map(|correlate| correlate.eligibility.iter()),
            )
            .filter(|own| *own == conjunct)
            .count();
        eligibility.contains(conjunct)
            && self
                .eligibility
                .iter()
                .filter(|own| *own == conjunct)
                .count()
                > in_optionals
    }

    /// The scope ranks exactly the retrieval its leading `order` key or its
    /// ranking step names, on its binding and property, with its argument,
    /// in the scope it orders.
    fn check_search(&self, plan: &PhysicalPlan, top: &[NodeId]) -> Result<(), ValidationError> {
        let ranked: Vec<(NodeId, &str, &RankedAccess)> = ranked_scans(plan)
            .into_iter()
            .filter(|(id, ..)| top.contains(id))
            .collect();
        let expected: Vec<(RankScope, &Retrieval)> = match &self.search {
            None => Vec::new(),
            Some(Search::Rank(retrieval)) => {
                // A windowed `nearest` fills its own window; every other
                // ranking of the read is its ordering ranking.
                vec![(
                    if self.window.is_some() {
                        RankScope::Window
                    } else {
                        RankScope::Order
                    },
                    retrieval,
                )]
            }
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
            let Some((_, binding, access)) =
                ranked.iter().find(|(_, _, access)| access.scope == *scope)
            else {
                return Err(ValidationError::violated(
                    "search identity",
                    format!("no ranked scan of the plan orders the `{scope:?}` retrieval"),
                ));
            };
            let same = *binding == retrieval.binding
                && access.property == retrieval.property
                && access.query == retrieval.query;
            if !same {
                return Err(ValidationError::violated(
                    "search identity",
                    format!(
                        "the plan ranks `${binding}.{}` by nearest with `{}`; the query names nearest of `${}.{}` with `{}`",
                        access.property,
                        access.query,
                        retrieval.binding,
                        retrieval.property,
                        retrieval.query
                    ),
                ));
            }
            self.check_access(plan, access)?;
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
        root: NodeId,
        scope: &[NodeId],
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        budget.visit(u64::try_from(scope.len()).unwrap_or(u64::MAX))?;
        let limit = self
            .limit
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
        let root = below_hydration(plan, root);
        let mut id = match plan.node(root) {
            Some(PhysicalNode::Limit { input, .. }) => *input,
            _ => root,
        };
        while let Some(PhysicalNode::Projection { input, .. }) = plan.node(id) {
            id = *input;
        }
        let final_sort = matches!(plan.node(id), Some(PhysicalNode::Sort { .. })).then_some(id);
        for &id in scope {
            let Some(node) = plan.node(id) else {
                continue;
            };
            if let PhysicalNode::TargetCut { targets, .. } = node {
                let window = self.window.flatten();
                if window != u64::try_from(*targets).ok() {
                    return Err(ValidationError::violated(
                        "row cut",
                        format!(
                            "node {id} (TargetCut) keeps {targets} targets; the query's window is {window:?}"
                        ),
                    ));
                }
            }
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
        // A `nearest` window keeps its `k` targets: the plan cuts them.
        if let Some(window) = self.window.flatten()
            && !scope
                .iter()
                .any(|id| matches!(plan.node(*id), Some(PhysicalNode::TargetCut { .. })))
        {
            return Err(ValidationError::violated(
                "row cut",
                format!("the query's window keeps {window} targets; the plan cuts no targets"),
            ));
        }
        Ok(())
    }

    /// The candidate cap, probe cap and overfetch ladder a ranked scan
    /// declares, from the cut its scope requires: an ordering `nearest`
    /// fetches the `limit` and may widen it by the declared ladder, a
    /// fusion's `nearest` arm fetches the `limit` (or the default arm size)
    /// and never widens.
    fn check_access(
        &self,
        plan: &PhysicalPlan,
        access: &RankedAccess,
    ) -> Result<(), ValidationError> {
        let limit = self.limit.and_then(|limit| usize::try_from(limit).ok());
        // A ranking drawn as a pattern step declares its own window; today's
        // ordering ranking takes the read's `limit`.
        let window = match self.window {
            Some(window) => window.and_then(|k| usize::try_from(k).ok()),
            None => limit,
        };
        let (fetch, overfetch) = match access.scope {
            RankScope::Order | RankScope::Window => (
                window,
                window.map(OverfetchRung::ladder).unwrap_or_default(),
            ),
        };
        if access.fetch != fetch || access.overfetch != overfetch {
            return Err(ValidationError::violated(
                "row cut",
                format!(
                    "the nearest scan fetches {:?} with overfetch {:?}; the query's cut requires {fetch:?} with {overfetch:?}",
                    access.fetch, access.overfetch
                ),
            ));
        }
        let setting = plan
            .assumptions()
            .settings
            .get(omnigraph_compiler::settings::SettingId::AnnNprobes.name());
        // A step that declares `recall: 1` searches every partition.
        let nprobes = if self.recall == Some(1.0) {
            None
        } else {
            setting
                .map(|value| match value.as_str() {
                    "0" => None,
                    value => value.parse::<usize>().ok(),
                })
                .ok_or_else(|| {
                    ValidationError::violated(
                        "declared policy",
                        "a nearest scan's probe cap was read from no recorded `ann_nprobes`",
                    )
                })?
        };
        if access.nprobes != nprobes {
            return Err(ValidationError::violated(
                "declared policy",
                format!(
                    "the nearest scan probes {:?}; the recorded setting is {nprobes:?}",
                    access.nprobes
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
            let origin = self.written_search.as_ref() == Some(call);
            let planned = projected_retrieval(call).is_some_and(|(binding, property)| {
                ranked_scans(plan)
                    .into_iter()
                    .any(|(_, scanned, access)| scanned == binding && access.property == property)
            });
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
    /// then the written keys; over
    /// group rows (an aggregate or a `return distinct`) the written keys then
    /// the tie rule's group keys ([`Self::with_group_keys`]); no sort where
    /// the query neither orders nor cuts its rows. A `limit` with no `order`
    /// cuts the tie rule's order alone (RFC, `Cut`).
    fn required_keys(&self) -> Option<Vec<IROrdering>> {
        let cut_or_ordered = self.ordered || self.limit.is_some();
        if self.aggregate {
            // A search leads no sort of groups: with no key after it and no
            // `limit` they leave unordered.
            let led = self.search.is_some() && self.window.is_none();
            if !cut_or_ordered || (led && self.order.is_empty() && self.limit.is_none()) {
                return None;
            }
            return Some(self.with_group_keys(self.order.clone()));
        }
        if self.window.is_some() {
            // A ranking drawn as a pattern step leads nothing: the written
            // keys order the rows.
            if !cut_or_ordered {
                return None;
            }
            return Some(self.order.clone());
        }
        let lead = match &self.search {
            None if !cut_or_ordered => return None,
            None => Vec::new(),
            Some(Search::Rank(retrieval)) => vec![(
                retrieval.binding.as_str(),
                omnigraph_compiler::query::value::DISTANCE_COLUMN,
                false,
            )],
        };
        Some(
            lead.into_iter()
                .map(|(binding, column, descending)| {
                    IROrdering::new(
                        IRExpr::PropAccess {
                            variable: binding.to_string(),
                            property: column.to_string(),
                            ty: ExprType::Value {
                                scalar: ScalarType::F64,
                                list: false,
                                nullable: false,
                            },
                        },
                        descending,
                    )
                })
                .chain(self.order.iter().cloned())
                .collect(),
        )
    }

    /// `keys`, then every group key not written, ascending: each reference
    /// (by its identity) first, then each value, in column order. An
    /// aggregate column breaks no tie between groups.
    fn with_group_keys(&self, mut keys: Vec<IROrdering>) -> Vec<IROrdering> {
        let groups = self
            .returns
            .iter()
            .filter(|projection| !contains_aggregate(&projection.expr));
        let (references, values): (Vec<_>, Vec<_>) =
            groups.partition(|projection| crate::optimizer::is_reference_expr(&projection.expr));
        for projection in references.into_iter().chain(values) {
            let written = keys.iter().any(|key| match &key.expr {
                IRExpr::AliasRef(alias, _) => *alias == projection.column,
                expr => *expr == projection.expr,
            });
            if !written {
                keys.push(IROrdering::new(
                    IRExpr::AliasRef(projection.column.clone(), projection.ty.clone()),
                    false,
                ));
            }
        }
        keys
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
            // A window's own sort orders its candidates for its `TargetCut`;
            // it orders no row the query returns.
            let window_sort = |sort: NodeId| {
                top.iter().any(|id| {
                    matches!(plan.node(*id), Some(PhysicalNode::TargetCut { input, .. }) if *input == sort)
                })
            };
            let sorted = top.iter().any(|id| {
                matches!(plan.node(*id), Some(PhysicalNode::Sort { .. })) && !window_sort(*id)
            });
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
                        // score column of the binding, in the score's direction
                        // and null placement.
                        planned.descending == key.descending
                            && planned.nulls_first == key.nulls_first
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

    /// The sort's metadata keys complete its comparator as the tie rule
    /// defines (RFC, `Order`): the identity of every reference column the
    /// query names, in column order (`@type` then `@id` for a selected edge),
    /// then every other column in column order, a correlated read's or a
    /// union's value by value and a name the lowering made up (an anonymous
    /// end, an inlined read's unreturned binding) by identity; ascending,
    /// nulls first. Two omissions are comparator equivalences and accepted: a
    /// key the comparator already holds, and every key when the query
    /// returns only order keys or constants (rows that tie are
    /// indistinguishable). A sort of group rows appends none.
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
                Some(value) => value,
                None => return false,
            };
            order_by.iter().any(|key| {
                matches!(named_expr(&self.returns, &key.expr), IRExpr::PropAccess { variable, property, .. }
                    if *variable == column.binding && property == physical)
            })
        };
        let (named, rest): (Vec<_>, Vec<_>) = self
            .tie_columns
            .iter()
            .partition(|column| column.is_named(&self.made_up));
        let mut expected = Vec::new();
        for column in named.into_iter().chain(rest) {
            match column {
                TieColumn::Identity { binding, selected } => {
                    if *selected {
                        expected.push(ColumnRef::property(binding, EDGE_TYPE_MEMBER));
                    }
                    expected.push(ColumnRef::property(binding, IDENTITY_MEMBER));
                }
                TieColumn::Value { binding, column } => {
                    expected.push(ColumnRef::property(binding, column));
                }
            }
        }
        expected.retain(|column| !keyed(column));
        if tiebreak != expected.as_slice() {
            return Err(ValidationError::violated(
                "order",
                format!(
                    "the final sort breaks ties by {:?}; the query's comparator requires {:?}",
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
/// pre-pass draws its eligible set from required first hops and feeds its
/// own `nearest` scan alone, an empty set proving the answer empty. The gate
/// policy's thresholds are finite.
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
        let PhysicalNode::Scan {
            ranked: Some(access),
            spec,
            ..
        } = node
        else {
            continue;
        };
        let Some(policy) = access.policy else {
            return Err(ValidationError::violated(
                "declared policy",
                "a nearest scan declares no policy",
            ));
        };
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
        if let Some(prefilter) = &access.prefilter {
            if prefilter.feeds != [id] {
                return Err(ValidationError::violated(
                    "declared policy",
                    "a nearest scan's pre-pass feeds that scan alone",
                ));
            }
            let binding = spec.binding.as_deref().unwrap_or_default();
            check_prefilter(plan, scope_root(plan, id), binding, prefilter, budget)?;
        }
    }
    Ok(())
}

/// The root of the scope `id` belongs to: the input of the innermost
/// fusion arm whose subtree holds it, else the plan's root. A ranking in an
/// arm ranks that arm's population alone.
fn scope_root(plan: &PhysicalPlan, id: NodeId) -> NodeId {
    let mut root = plan.root();
    let mut size = usize::MAX;
    for (_, node) in plan.live() {
        let PhysicalNode::Fuse { arms, .. } = node else {
            continue;
        };
        for arm in arms {
            let subtree = plan.subtree(arm.input);
            if subtree.len() < size && subtree.contains(&id) {
                root = arm.input;
                size = subtree.len();
            }
        }
    }
    root
}

/// A fusion arm's rows arrive in its rank order, each target's rows
/// together at its rank: a `nearest` window's `TargetCut` of the target over
/// a sort leading with its distance ascending; an exact score's or a nested
/// fusion's rows sorted by the target's score in its kind's direction
/// (`arm_score`), then the target's identity, with no cut; an ordered
/// read's uncorrelated `Correlate`, which orders and cuts its own rows and
/// returns the target.
fn check_arm_order(
    plan: &PhysicalPlan,
    arm: &crate::physical::FuseArm,
    target: &str,
) -> Result<(), ValidationError> {
    let refused = |detail: String| Err(ValidationError::violated("search identity", detail));
    let score = arm_score(target, arm.rank);
    let ordered = match (arm.rank, plan.node(arm.input)) {
        (ArmRank::Nearest, Some(PhysicalNode::TargetCut { input, binding, .. })) => {
            binding == target
                && matches!(plan.node(*input), Some(PhysicalNode::Sort { order_by, fetch: None, .. })
                    if order_by.first() == score.as_ref())
        }
        (
            ArmRank::Ordered,
            Some(PhysicalNode::Correlate {
                input: None, read, ..
            }),
        ) => !read.order_by.is_empty() && read.references().any(|name| name == target),
        (
            ArmRank::Distance | ArmRank::Bm25 | ArmRank::Fused,
            Some(PhysicalNode::Sort {
                order_by,
                fetch: None,
                tiebreak,
                ..
            }),
        ) => {
            order_by.as_slice() == score.as_slice()
                && tiebreak.as_slice() == [ColumnRef::property(target, IDENTITY_MEMBER)]
        }
        _ => false,
    };
    if ordered {
        Ok(())
    } else {
        refused(format!(
            "the fusion's {} arm (node {}) does not yield its rows of `${target}` in its rank order",
            arm.rank.name(),
            arm.input
        ))
    }
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
        .map(|key| {
            let placement = match key.nulls_first {
                None => "",
                Some(true) => " nulls first",
                Some(false) => " nulls last",
            };
            let direction = if key.descending { " desc" } else { "" };
            format!("{}{direction}{placement}", key.expr)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The (binding, property) a projected `nearest` call reads.
fn projected_retrieval(expr: &IRExpr) -> Option<(&str, &str)> {
    match expr {
        IRExpr::Nearest {
            variable, property, ..
        } => Some((variable, property)),
        _ => None,
    }
}
