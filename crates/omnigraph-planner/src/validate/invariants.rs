//! The checks every accepted plan passes (RFC 0047, "Checks have explicit
//! boundaries"): binding identity, search identity and its declared
//! approximation, retained eligibility predicates, correlated blocks, the
//! projection and the origin of every projected score, the required order
//! and the row cut, and the policy data the plan declares for its search.
//! They establish that the plan keeps what the query requires; they do not
//! prove row-selection equivalence, which only the exact subset's checked
//! derivation does.
//!
//! The order of a fused (`rrf`) search and of a search-ordered aggregate is
//! not checked here: the plan does not yet implement the total order those
//! shapes require (RFC 0047, "One total order"), and the step that does
//! extends these checks.

use omnigraph_compiler::ir::{IRExpr, IROrdering, IRProjection};
use omnigraph_compiler::query::ast::{Expr, Literal};
use omnigraph_compiler::query::typecheck::BoundVariable;

use super::budget::Budget;
use super::requirements::{
    Matcher, Position, Requirements, Retrieval, Search, result_column, score_column,
};
use super::{AcceptInput, ValidationError};
use crate::logical::{ColumnRef, EDGE_TYPE_MEMBER, IDENTITY_MEMBER};
use crate::lower::ContainsJoinFields;
use crate::optimizer::{RRF_NEAREST_ARM_K, derived_order};
use crate::physical::{
    Assumptions, Eligibility, NodeId, OverfetchRung, PhysicalNode, PhysicalPlan, RankKind,
    RankScope, RankedAccess,
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

/// The pipelines whose rows a query's eligibility constrains: one per `rrf`
/// arm, each its own copy of the match, or the whole top level.
fn constrained_pipelines(
    plan: &PhysicalPlan,
    budget: &mut Budget,
) -> Result<Vec<Vec<NodeId>>, ValidationError> {
    let top = pipeline(plan, plan.root(), budget)?;
    let fuse = top.iter().find_map(|id| match plan.node(*id) {
        Some(PhysicalNode::RankFuse { arms, .. }) => Some(arms.clone()),
        _ => None,
    });
    match fuse {
        Some(arms) => arms
            .iter()
            .map(|arm| pipeline(plan, arm.input, budget))
            .collect(),
        None => Ok(vec![top]),
    }
}

/// Every conjunct a pipeline's nodes test, wherever placement put it.
fn conjuncts(plan: &PhysicalPlan, nodes: &[NodeId]) -> Vec<IRExpr> {
    let mut out = Vec::new();
    for id in nodes {
        match plan.node(*id) {
            Some(PhysicalNode::Scan { spec, .. }) => {
                if let Some(filter) = &spec.filter {
                    out.extend(filter.gq_filters());
                }
            }
            Some(
                PhysicalNode::Filter { filters, .. } | PhysicalNode::CrossJoin { filters, .. },
            ) => {
                out.extend(filters.iter().cloned());
            }
            Some(PhysicalNode::ContainsJoin {
                haystack,
                needle,
                residual,
                ..
            }) => {
                let fields = ContainsJoinFields {
                    haystack: (&haystack.0, &haystack.1),
                    needle: (&needle.0, &needle.1),
                    residual,
                };
                out.push(fields.conjunct());
                out.extend(residual.iter().cloned());
            }
            _ => {}
        }
    }
    out
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
        let matcher = Matcher::new(input, self);
        let pipelines = constrained_pipelines(plan, budget)?;
        for nodes in &pipelines {
            self.check_bindings(plan, nodes)?;
            self.check_eligibility(plan, nodes, &matcher, budget)?;
            self.check_blocks(plan, nodes, &matcher, budget)?;
        }
        let top = pipeline(plan, plan.root(), budget)?;
        self.check_search(plan, &matcher, budget)?;
        self.check_returns(plan, &top, &matcher, budget)?;
        self.check_order_and_cut(plan, &top, &matcher, budget)?;
        Ok(())
    }

    /// Every written binding is read with its type: a node binding by a scan
    /// (or a pushed-down count) of its node type, an edge binding by the
    /// traversal that names it over its edge types; every traversal clause
    /// is one traversal.
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
        let expands = nodes
            .iter()
            .filter(|id| matches!(plan.node(**id), Some(PhysicalNode::Expand { .. })))
            .count();
        if expands != self.traversals {
            return Err(ValidationError::violated(
                "binding identity",
                format!(
                    "the query writes {} traversals; the plan runs {expands}",
                    self.traversals
                ),
            ));
        }
        Ok(())
    }

    /// Every written eligibility conjunct is tested somewhere in each
    /// pipeline the query's rows come from; a constant `true` constrains
    /// nothing and needs no place.
    fn check_eligibility(
        &self,
        plan: &PhysicalPlan,
        nodes: &[NodeId],
        matcher: &Matcher<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let placed = conjuncts(plan, nodes);
        let mut written_conjuncts = Vec::new();
        for clause in &self.eligibility {
            matcher.conjuncts(clause, &mut written_conjuncts);
        }
        for written in written_conjuncts {
            if matcher.constant(written) && matcher.value(written) == Some(Literal::Bool(true)) {
                continue;
            }
            let mut found = false;
            for conjunct in &placed {
                if matcher.matches(written, conjunct, Position::Filter, budget)? {
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(ValidationError::violated(
                    "predicate retention",
                    format!("the plan tests no conjunct for `{written}`"),
                ));
            }
        }
        Ok(())
    }

    /// One correlated block per written block in each pipeline the query's
    /// rows come from, each with the aggregate and comparison the query
    /// wrote.
    fn check_blocks(
        &self,
        plan: &PhysicalPlan,
        top: &[NodeId],
        matcher: &Matcher<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let mut planned: Vec<_> = top
            .iter()
            .filter_map(|id| match plan.node(*id) {
                Some(PhysicalNode::AntiJoin { predicate, .. }) => Some(predicate),
                _ => None,
            })
            .collect();
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
            let mut found = None;
            for (index, predicate) in planned.iter().enumerate() {
                if predicate.func == block.func
                    && predicate.op == block.op
                    && matcher.matches(&block.right, &predicate.right, Position::Plain, budget)?
                {
                    found = Some(index);
                    break;
                }
            }
            match found {
                Some(index) => {
                    planned.remove(index);
                }
                None => {
                    return Err(ValidationError::violated(
                        "predicate retention",
                        format!(
                            "no correlated block of the plan compares `{} {} {}`",
                            block.func, block.op, block.right
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
    fn check_search(
        &self,
        plan: &PhysicalPlan,
        matcher: &Matcher<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
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
            let Some((_, binding, access)) =
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
                && matcher.argument(&retrieval.query, &access.query, budget)?;
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
            let type_key = ranked
                .iter()
                .find(|(_, _, other)| std::ptr::eq(*other, *access))
                .and_then(|(id, _, _)| match plan.node(*id) {
                    Some(PhysicalNode::Scan { spec, .. }) => Some(spec.table.type_key.as_str()),
                    _ => None,
                })
                .unwrap_or_default();
            self.check_access(plan, type_key, access)?;
        }
        match (&self.search, fuse) {
            (Some(Search::Fuse { arms, k }), Some((planned, planned_k))) => {
                for (arm, retrieval) in planned.iter().zip(arms) {
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
                let same_k = match (k, planned_k) {
                    (None, None) => true,
                    (Some(written), Some(planned)) => matcher.argument(written, planned, budget)?,
                    _ => false,
                };
                if !same_k {
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
        matcher: &Matcher<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let projections = projections_of(plan, top);
        let [returns] = projections.as_slice() else {
            return Err(ValidationError::violated(
                "projection",
                format!(
                    "the plan has {} projections of the query's `return`; it needs one",
                    projections.len()
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
        for (written, projected) in self.returns.iter().zip(returns.iter()) {
            let alias = written
                .alias
                .clone()
                .or_else(|| meta_field_result_key(&written.expr));
            if projected.alias != alias
                || !matcher.matches(&written.expr, &projected.expr, Position::Return, budget)?
            {
                return Err(ValidationError::violated(
                    "projection",
                    format!(
                        "the plan returns `{}`; the query returns `{}`",
                        projected.expr, written.expr
                    ),
                ));
            }
        }
        for (_, call) in super::requirements::projected_rank_calls(&self.returns) {
            let Some(projected) = projected_retrieval(call) else {
                continue;
            };
            let mut origin = false;
            for retrieval in self.retrievals() {
                if retrieval.kind == projected.kind
                    && retrieval.binding == projected.binding
                    && retrieval.property == projected.property
                    && retrieval.query == projected.query
                {
                    origin = true;
                    break;
                }
            }
            let planned = ranked_scans(plan).into_iter().any(|(_, binding, access)| {
                binding == projected.binding
                    && access.kind == projected.kind
                    && access.property == projected.property
            });
            if !origin || !planned {
                return Err(ValidationError::violated(
                    "score origin",
                    format!(
                        "the projected `{call}` reads `${}.{}`, which no retrieval of the query with these arguments produces",
                        projected.binding,
                        score_column(projected.kind)
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The plan's rows leave in the order the query requires and are cut
    /// where it requires: the final sort's comparator is the search's score,
    /// the remaining written keys and the binding identities, the `limit`
    /// cuts its output, and nothing between them reorders or drops rows.
    fn check_order_and_cut(
        &self,
        plan: &PhysicalPlan,
        top: &[NodeId],
        matcher: &Matcher<'_>,
        budget: &mut Budget,
    ) -> Result<(), ValidationError> {
        let mut id = plan.root();
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
        let fused = matches!(self.search, Some(Search::Fuse { .. }));
        if fused {
            let planned = top.iter().find_map(|id| match plan.node(*id) {
                Some(PhysicalNode::RankFuse { limit, .. }) => Some(*limit),
                _ => None,
            });
            let limit = self.limit.and_then(|limit| usize::try_from(limit).ok());
            if planned != Some(limit) {
                return Err(ValidationError::violated(
                    "row cut",
                    format!("the fusion keeps {planned:?} rows; the query's `limit` is {limit:?}"),
                ));
            }
            return Ok(());
        }
        let score = match &self.search {
            Some(Search::Rank(retrieval)) => Some(retrieval),
            _ => None,
        };
        if score.is_some() && self.aggregate {
            return Ok(());
        }
        let written: Vec<&omnigraph_compiler::query::ast::Ordering> = self
            .order
            .iter()
            .filter(|key| !matcher.constant(&key.expr))
            .collect();
        if score.is_none() && self.order.is_empty() {
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
        }
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
        let mut keys = order_by.iter();
        if let Some(retrieval) = score {
            let (column, descending) = retrieval.kind.score();
            let leads = keys.next().is_some_and(|key| {
                key.descending == descending
                    && matches!(&key.expr, IRExpr::PropAccess { variable, property }
                        if *variable == retrieval.binding && property == column)
            });
            if !leads {
                return Err(ValidationError::violated(
                    "order",
                    format!(
                        "the final sort does not lead with the {:?} score of `${}`",
                        retrieval.kind, retrieval.binding
                    ),
                ));
            }
        }
        let rest: Vec<&IROrdering> = keys.filter(|key| !constant_ir(&key.expr)).collect();
        if rest.len() != written.len() {
            return Err(ValidationError::violated(
                "order",
                format!(
                    "the final sort has {} written keys; the query writes {} that are not constant",
                    rest.len(),
                    written.len()
                ),
            ));
        }
        let returns = projection_of(plan, top)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for (key, planned) in written.iter().zip(&rest) {
            if key.descending != planned.descending
                || !self.same_key(&key.expr, planned, returns, matcher, budget)?
            {
                return Err(ValidationError::violated(
                    "order",
                    format!(
                        "the final sort orders by `{}`; the query writes `{}`",
                        planned.expr, key.expr
                    ),
                ));
            }
        }
        self.check_tiebreak(order_by, tiebreak, returns, matcher)?;
        Ok(())
    }

    /// Whether a planned sort key is the written key: as written, or the
    /// `return` item the planner bound it to by its result column. In a
    /// grouped query a written meta-field key binds by name to the unaliased
    /// return item whose column it names, as the compiler lowers it.
    fn same_key(
        &self,
        written: &Expr,
        planned: &IROrdering,
        returns: &[IRProjection],
        matcher: &Matcher<'_>,
        budget: &mut Budget,
    ) -> Result<bool, ValidationError> {
        if let IRExpr::AliasRef(column) = &planned.expr
            && !matches!(written, Expr::AliasRef(alias) if alias == column)
        {
            let Some(index) = returns
                .iter()
                .position(|projection| result_column(projection).as_deref() == Some(column))
            else {
                return Ok(false);
            };
            let Some(item) = self.returns.get(index) else {
                return Ok(false);
            };
            let by_name = self.aggregate
                && matches!(written, Expr::PropAccess { .. })
                && item.alias.is_none()
                && meta_field_result_key(written).as_deref() == Some(column)
                && meta_field_result_key(&item.expr).as_deref() == Some(column);
            return Ok(item.expr == *written || by_name);
        }
        matcher.matches(written, &planned.expr, Position::Plain, budget)
    }

    /// A written order key with an alias resolved to the `return` item it
    /// names.
    fn resolved_key<'e>(&'e self, key: &'e Expr) -> &'e Expr {
        match key {
            Expr::AliasRef(alias) => self
                .returns
                .iter()
                .find(|projection| projection.alias.as_deref() == Some(alias))
                .map_or(key, |projection| &projection.expr),
            other => other,
        }
    }

    /// The sort's metadata keys complete its comparator as RFC 0047 defines:
    /// every written binding's identity (`@type` then `@id` for a selected
    /// edge) in binding-name order, ascending, nulls first. Three planner
    /// omissions are comparator equivalences and accepted: a key the
    /// comparator already holds, every key when the query returns only order
    /// keys (rows that tie are indistinguishable), and identities of
    /// bindings the query does not name (anonymous traversal endpoints)
    /// after every named one.
    fn check_tiebreak(
        &self,
        order_by: &[IROrdering],
        tiebreak: &[ColumnRef],
        returns: &[IRProjection],
        matcher: &Matcher<'_>,
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
        let covered = self.returns.iter().all(|projection| {
            self.order
                .iter()
                .any(|key| *self.resolved_key(&key.expr) == projection.expr)
                || match &self.search {
                    Some(Search::Rank(retrieval)) => projected_retrieval(&projection.expr)
                        .is_some_and(|projected| {
                            projected.kind == retrieval.kind
                                && projected.binding == retrieval.binding
                                && projected.property == retrieval.property
                                && projected.query == retrieval.query
                        }),
                    _ => false,
                }
        });
        if covered {
            return Ok(());
        }
        let keyed = |column: &ColumnRef| {
            let physical = match column.property.as_deref() {
                Some(IDENTITY_MEMBER) => matcher.physical(SYSTEM_ID),
                Some(EDGE_TYPE_MEMBER) => matcher.physical(EDGE_TYPE_MEMBER),
                _ => return false,
            };
            order_by.iter().any(|key| {
                let expr = match &key.expr {
                    IRExpr::AliasRef(alias) => returns
                        .iter()
                        .find(|projection| projection.alias.as_deref() == Some(alias))
                        .map_or(&key.expr, |projection| &projection.expr),
                    expr => expr,
                };
                matches!(expr, IRExpr::PropAccess { variable, property }
                    if *variable == column.binding && *property == physical)
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

const SYSTEM_ID: &str = "@id";

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

/// The one projection of `top`, when there is one.
fn projection_of<'p>(plan: &'p PhysicalPlan, top: &[NodeId]) -> Option<&'p Vec<IRProjection>> {
    match projections_of(plan, top).as_slice() {
        [returns] => Some(*returns),
        _ => None,
    }
}

/// Whether a planned key reads no row: ordering by it orders nothing.
fn constant_ir(expr: &IRExpr) -> bool {
    match expr {
        IRExpr::Literal(_) | IRExpr::Param(_) => true,
        IRExpr::Binary { left, right, .. } => constant_ir(left) && constant_ir(right),
        IRExpr::Not(inner) | IRExpr::IsNull { expr: inner, .. } => constant_ir(inner),
        _ => false,
    }
}

fn exactness(approximate: bool) -> &'static str {
    if approximate { "approximate" } else { "exact" }
}

/// The retrieval a projected rank call names.
fn projected_retrieval(expr: &Expr) -> Option<Retrieval> {
    super::requirements::retrieval_of(expr)
}

/// The alias the compiler gives an unaliased meta-field projection: the
/// logical `var.@prop`.
fn meta_field_result_key(expr: &Expr) -> Option<String> {
    match expr {
        Expr::PropAccess { variable, property } if property.starts_with('@') => {
            Some(format!("{variable}.{property}"))
        }
        Expr::Aggregate { arg, .. } => meta_field_result_key(arg),
        _ => None,
    }
}
