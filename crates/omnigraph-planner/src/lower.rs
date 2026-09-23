//! Lowering a [`PhysicalPlan`] to an engine's operators. The walk lives
//! here, so an engine writes one method per [`PhysicalNode`] variant and a
//! variant without a method does not compile. A ranked scan is one `scan`
//! call with its [`RankedAccess`]; the two arms of a `RankFuse` are two
//! subtrees, each lowered once.

use std::collections::HashSet;

use omnigraph_compiler::ir::{IRExpr, IRFilter, IROrdering, IRProjection};
use omnigraph_compiler::types::Direction;

use crate::cost::{AccessPath, ExpandMode, ExpandPolicy};
use crate::error::PlanError;
use crate::logical::{KeyJoinKind, ScanSpec};
use crate::physical::{NodeId, PhysicalNode, PhysicalPlan, RankArm, RankedAccess, ScanInput};

#[cfg(doc)]
use crate::physical::RankKind;
use crate::source::SideId;

/// The fields of [`PhysicalNode::Expand`] beside its input.
#[derive(Debug, Clone, Copy)]
pub struct ExpandFields<'p> {
    pub src: &'p str,
    pub dst: &'p str,
    pub edge_type: &'p str,
    pub direction: Direction,
    pub dst_type: &'p str,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
    pub edge_binding: Option<&'p str>,
    pub mode: ExpandMode,
    pub frontier_estimate: Option<u64>,
    pub policy: &'p ExpandPolicy,
}

/// The fields of [`PhysicalNode::SortMergeJoin`] beside its inputs.
#[derive(Debug, Clone, Copy)]
pub struct SortMergeJoinFields<'p> {
    pub kind: KeyJoinKind,
    pub on: &'p str,
    pub build: bool,
    pub drop_equal_addresses: bool,
}

/// The fields of [`PhysicalNode::HashJoin`] beside its inputs, with the
/// `ScanSpec` of its build scan, which the fallback lookup reads too.
#[derive(Debug, Clone, Copy)]
pub struct HashJoinFields<'p> {
    pub binding: &'p str,
    pub fallback: Option<AccessPath>,
    pub spec: &'p ScanSpec,
}

/// One method per [`PhysicalNode`] variant, called by [`PhysicalPlan::lower`]
/// with the node's inputs already lowered. `id` is the node the one operator
/// the method builds belongs to.
pub trait Lower {
    type Op;
    type Error: From<PlanError>;

    fn metadata_count(
        &mut self,
        id: NodeId,
        spec: &ScanSpec,
        return_exprs: &[IRProjection],
    ) -> Result<Self::Op, Self::Error>;

    fn scan(
        &mut self,
        id: NodeId,
        source: &ScanInput,
        spec: &ScanSpec,
        ordered: bool,
        keys_only: bool,
        ranked: Option<&RankedAccess>,
        input: Option<Self::Op>,
    ) -> Result<Self::Op, Self::Error>;

    fn sort_merge_join(
        &mut self,
        id: NodeId,
        fields: SortMergeJoinFields<'_>,
        left: Self::Op,
        right: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    /// Called with the probe (the traversal) and the build (the destination's
    /// table scan) lowered.
    fn hash_join(
        &mut self,
        id: NodeId,
        fields: HashJoinFields<'_>,
        probe: Self::Op,
        build: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    fn hydrate_by_address(
        &mut self,
        id: NodeId,
        side: SideId,
        input: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    fn row_compare(&mut self, id: NodeId, input: Self::Op) -> Result<Self::Op, Self::Error>;

    fn classify_three_way(&mut self, id: NodeId, input: Self::Op) -> Result<Self::Op, Self::Error>;

    fn limit(&mut self, id: NodeId, rows: usize, input: Self::Op) -> Result<Self::Op, Self::Error>;

    fn page(
        &mut self,
        id: NodeId,
        rows: usize,
        bytes: u64,
        resume: Option<&str>,
        input: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    fn cross_join(
        &mut self,
        id: NodeId,
        left: Self::Op,
        right: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    fn filter(
        &mut self,
        id: NodeId,
        filters: &[IRFilter],
        input: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    fn expand(
        &mut self,
        id: NodeId,
        fields: ExpandFields<'_>,
        input: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    /// Called with the lowered outer input before the walk enters the inner
    /// tree, whose [`Lower::outer_reference`] leaf reads what this set up.
    fn anti_join_outer(
        &mut self,
        id: NodeId,
        outer_var: &str,
        outer: &Self::Op,
    ) -> Result<(), Self::Error>;

    fn anti_join(
        &mut self,
        id: NodeId,
        outer_var: &str,
        outer: Self::Op,
        inner: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    fn outer_reference(&mut self, id: NodeId, outer_var: &str) -> Result<Self::Op, Self::Error>;

    /// Called with both arm subtrees lowered, the primary first.
    fn rank_fuse(
        &mut self,
        id: NodeId,
        arms: &[RankArm; 2],
        k: Option<&IRExpr>,
        limit: Option<usize>,
        primary: Self::Op,
        secondary: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    fn projection(
        &mut self,
        id: NodeId,
        return_exprs: &[IRProjection],
        input: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    fn aggregate(
        &mut self,
        id: NodeId,
        return_exprs: &[IRProjection],
        input: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    /// `tiebreak` names the bindings whose ids the sort appends after
    /// `order_by`, name-sorted; the scans project those ids.
    fn sort(
        &mut self,
        id: NodeId,
        order_by: &[IROrdering],
        fetch: Option<usize>,
        tiebreak: &[String],
        input: Self::Op,
    ) -> Result<Self::Op, Self::Error>;

    /// Called once with the lowered root. It builds no operator and hands the
    /// root back: the design rule is one operator per node.
    fn finish(&mut self, root: NodeId, op: Self::Op) -> Result<Self::Op, Self::Error>;
}

impl PhysicalPlan {
    /// Lower the plan from its root, inputs before their consumer. Refuses a
    /// plan with a live node the root does not reach or a node reached twice.
    pub fn lower<L: Lower>(&self, lowering: &mut L) -> Result<L::Op, L::Error> {
        self.check_total()?;
        let op = self.lower_node(self.root(), lowering)?;
        lowering.finish(self.root(), op)
    }

    /// The consumer of `id`, `None` for the root and for a tombstone.
    pub fn parent_of(&self, id: NodeId) -> Option<NodeId> {
        self.live()
            .find(|(_, node)| node.inputs().contains(&id))
            .map(|(parent, _)| parent)
    }

    fn check_total(&self) -> Result<(), PlanError> {
        let order = self.post_order();
        let mut reached: HashSet<NodeId> = HashSet::new();
        let mut repeated: Vec<NodeId> = Vec::new();
        for &id in &order {
            if !reached.insert(id) && !repeated.contains(&id) {
                repeated.push(id);
            }
        }
        if !repeated.is_empty() {
            return Err(PlanError::Internal(format!(
                "physical nodes {repeated:?} are the input of two consumers"
            )));
        }
        let unreached: Vec<NodeId> = self
            .live()
            .map(|(id, _)| id)
            .filter(|id| !reached.contains(id))
            .collect();
        if !unreached.is_empty() {
            return Err(PlanError::Internal(format!(
                "physical nodes {unreached:?} are live and unreachable from the root"
            )));
        }
        Ok(())
    }

    fn lower_node<L: Lower>(&self, id: NodeId, l: &mut L) -> Result<L::Op, L::Error> {
        let node = self
            .node(id)
            .ok_or_else(|| PlanError::Internal(format!("physical node {id} is a tombstone")))?;
        match node {
            PhysicalNode::MetadataCount { spec, return_exprs } => {
                l.metadata_count(id, spec, return_exprs)
            }
            PhysicalNode::Scan {
                source,
                spec,
                ordered,
                keys_only,
                ranked,
            } => {
                let input = match source {
                    ScanInput::Table => None,
                    ScanInput::Dependent { input, .. } => Some(self.lower_node(*input, l)?),
                };
                l.scan(
                    id,
                    source,
                    spec,
                    *ordered,
                    *keys_only,
                    ranked.as_ref(),
                    input,
                )
            }
            PhysicalNode::SortMergeJoin {
                left,
                right,
                kind,
                on,
                build,
                drop_equal_addresses,
            } => {
                let left = self.lower_node(*left, l)?;
                let right = self.lower_node(*right, l)?;
                let fields = SortMergeJoinFields {
                    kind: *kind,
                    on,
                    build: *build,
                    drop_equal_addresses: *drop_equal_addresses,
                };
                l.sort_merge_join(id, fields, left, right)
            }
            PhysicalNode::HashJoin {
                probe,
                build,
                binding,
                fallback,
            } => {
                let spec = match self.node(*build) {
                    Some(PhysicalNode::Scan {
                        source: ScanInput::Table,
                        spec,
                        ..
                    }) => spec,
                    _ => {
                        return Err(PlanError::Internal(format!(
                            "the build of hash join {id} is not a table scan"
                        ))
                        .into());
                    }
                };
                if spec.binding.as_deref() != Some(binding.as_str()) {
                    let other = spec.binding.as_deref().unwrap_or_default();
                    return Err(PlanError::Internal(format!(
                        "the build of hash join {id} scans `{other}`, not `{binding}`"
                    ))
                    .into());
                }
                let probe = self.lower_node(*probe, l)?;
                let build = self.lower_node(*build, l)?;
                let fields = HashJoinFields {
                    binding,
                    fallback: *fallback,
                    spec,
                };
                l.hash_join(id, fields, probe, build)
            }
            PhysicalNode::HydrateByAddress { input, side } => {
                let input = self.lower_node(*input, l)?;
                l.hydrate_by_address(id, *side, input)
            }
            PhysicalNode::RowCompare { input } => {
                let input = self.lower_node(*input, l)?;
                l.row_compare(id, input)
            }
            PhysicalNode::ClassifyThreeWay { input } => {
                let input = self.lower_node(*input, l)?;
                l.classify_three_way(id, input)
            }
            PhysicalNode::Limit { input, rows } => {
                let input = self.lower_node(*input, l)?;
                l.limit(id, *rows, input)
            }
            PhysicalNode::Page {
                input,
                rows,
                bytes,
                resume,
            } => {
                let input = self.lower_node(*input, l)?;
                l.page(id, *rows, *bytes, resume.as_deref(), input)
            }
            PhysicalNode::CrossJoin { left, right } => {
                let left = self.lower_node(*left, l)?;
                let right = self.lower_node(*right, l)?;
                l.cross_join(id, left, right)
            }
            PhysicalNode::Filter { input, filters } => {
                let input = self.lower_node(*input, l)?;
                l.filter(id, filters, input)
            }
            PhysicalNode::Expand {
                input,
                src,
                dst,
                edge_type,
                direction,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                mode,
                frontier_estimate,
                policy,
                version: _,
            } => {
                let input = self.lower_node(*input, l)?;
                let fields = ExpandFields {
                    src,
                    dst,
                    edge_type,
                    direction: *direction,
                    dst_type,
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    edge_binding: edge_binding.as_deref(),
                    mode: *mode,
                    frontier_estimate: *frontier_estimate,
                    policy,
                };
                l.expand(id, fields, input)
            }
            PhysicalNode::AntiJoin {
                input,
                inner,
                outer_var,
            } => {
                let outer = self.lower_node(*input, l)?;
                l.anti_join_outer(id, outer_var, &outer)?;
                let inner = self.lower_node(*inner, l)?;
                l.anti_join(id, outer_var, outer, inner)
            }
            PhysicalNode::OuterReference { outer_var } => l.outer_reference(id, outer_var),
            PhysicalNode::RankFuse { arms, k, limit, .. } => {
                let primary = self.lower_node(arms[0].input, l)?;
                let secondary = self.lower_node(arms[1].input, l)?;
                l.rank_fuse(id, arms, k.as_ref(), *limit, primary, secondary)
            }
            PhysicalNode::Projection {
                input,
                return_exprs,
            } => {
                let input = self.lower_node(*input, l)?;
                l.projection(id, return_exprs, input)
            }
            PhysicalNode::Aggregate {
                input,
                return_exprs,
            } => {
                let input = self.lower_node(*input, l)?;
                l.aggregate(id, return_exprs, input)
            }
            PhysicalNode::Sort {
                input,
                order_by,
                fetch,
                tiebreak,
            } => {
                let input = self.lower_node(*input, l)?;
                l.sort(id, order_by, *fetch, tiebreak, input)
            }
        }
    }
}
