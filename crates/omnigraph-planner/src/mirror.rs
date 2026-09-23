//! Serde mirrors of a [`BoundPlan`]: one planner-owned type per embedded
//! type that derives no `Serialize` (the compiler's `IRExpr` family and
//! `Literal`, Arrow's schema), with `From` into the mirror and `TryFrom` out
//! of it. The mirror is the serialized form and the comparable form of a
//! plan; nothing executes from it directly.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use omnigraph_compiler::ir::{IRExpr, IRFilter, IROrdering, IRProjection};
use omnigraph_compiler::query::ast::{AggFunc, CompOp, Literal};
use omnigraph_compiler::types::Direction;
use omnigraph_compiler::{
    SYSTEM_COLUMNS_LEGACY, SYSTEM_COLUMNS_META, SYSTEM_COLUMNS_V3, SystemColumns,
};
use serde::{Deserialize, Serialize};

use crate::bound::{BoundPlan, ValueTable};
use crate::cost::{AccessPath, ExpandMode, ExpandPolicy};
use crate::error::PlanError;
use crate::logical::{ColumnRef, GqFilter, KeyJoinKind, Predicate, ScanSpec};
use crate::operation::TableRef;
use crate::physical::{
    Assumptions, Estimate, Hop, NodeId, OverfetchRung, PhysicalNode, PhysicalPlan, Prefilter,
    Properties, RankArm, RankKind, RankScope, RankedAccess, ScanInput, StatisticSource,
};
use crate::source::SideId;

fn internal(detail: impl std::fmt::Display) -> PlanError {
    PlanError::Internal(format!("the plan mirror does not read back: {detail}"))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundPlanMirror {
    pub plan: PlanMirror,
    pub values: ValueTableMirror,
}

impl From<&BoundPlan> for BoundPlanMirror {
    fn from(bound: &BoundPlan) -> Self {
        Self {
            plan: PlanMirror::from(&bound.plan),
            values: ValueTableMirror::from(&bound.values),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueTableMirror {
    pub params: BTreeMap<String, LiteralMirror>,
    pub vectors: BTreeMap<NodeId, Vec<f32>>,
}

impl From<&ValueTable> for ValueTableMirror {
    fn from(values: &ValueTable) -> Self {
        Self {
            params: values
                .params
                .iter()
                .map(|(name, literal)| (name.clone(), LiteralMirror::from(literal)))
                .collect(),
            vectors: values.vectors.clone(),
        }
    }
}

impl From<ValueTableMirror> for ValueTable {
    fn from(mirror: ValueTableMirror) -> Self {
        Self {
            params: Arc::new(
                mirror
                    .params
                    .into_iter()
                    .map(|(name, literal)| (name, Literal::from(literal)))
                    .collect(),
            ),
            vectors: mirror.vectors,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanMirror {
    pub slots: Vec<Option<NodeMirror>>,
    pub properties: Vec<Option<PropertiesMirror>>,
    pub root: NodeId,
    pub assumptions: Assumptions,
}

impl From<&PhysicalPlan> for PlanMirror {
    fn from(plan: &PhysicalPlan) -> Self {
        Self {
            slots: plan
                .slots()
                .iter()
                .map(|slot| slot.as_ref().map(NodeMirror::from))
                .collect(),
            properties: plan
                .property_slots()
                .iter()
                .map(|slot| slot.as_ref().map(PropertiesMirror::from))
                .collect(),
            root: plan.root(),
            assumptions: plan.assumptions().clone(),
        }
    }
}

impl TryFrom<PlanMirror> for PhysicalPlan {
    type Error = PlanError;

    fn try_from(mirror: PlanMirror) -> Result<Self, PlanError> {
        let slots = mirror
            .slots
            .into_iter()
            .map(|slot| slot.map(PhysicalNode::try_from).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        let properties = mirror
            .properties
            .into_iter()
            .map(|slot| slot.map(Properties::try_from).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::from_parts(
            slots,
            properties,
            mirror.root,
            mirror.assumptions,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "node")]
pub enum NodeMirror {
    MetadataCount {
        spec: ScanSpecMirror,
        return_exprs: Vec<ProjectionMirror>,
    },
    Scan {
        source: ScanInputMirror,
        spec: ScanSpecMirror,
        ordered: bool,
        keys_only: bool,
        ranked: Option<Box<RankedMirror>>,
    },
    SortMergeJoin {
        left: NodeId,
        right: NodeId,
        kind: KeyJoinKind,
        on: String,
        build: bool,
        drop_equal_addresses: bool,
    },
    HashJoin {
        probe: NodeId,
        build: NodeId,
        binding: String,
        fallback: Option<AccessPath>,
    },
    HydrateByAddress {
        input: NodeId,
        side: SideId,
    },
    RowCompare {
        input: NodeId,
    },
    ClassifyThreeWay {
        input: NodeId,
    },
    Limit {
        input: NodeId,
        rows: usize,
    },
    Page {
        input: NodeId,
        rows: usize,
        bytes: u64,
        resume: Option<String>,
    },
    CrossJoin {
        left: NodeId,
        right: NodeId,
    },
    Filter {
        input: NodeId,
        filters: Vec<FilterMirror>,
    },
    Expand {
        input: NodeId,
        src: String,
        dst: String,
        edge_type: String,
        direction: DirectionMirror,
        dst_type: String,
        min_hops: u32,
        max_hops: Option<u32>,
        edge_binding: Option<String>,
        mode: ExpandMode,
        frontier_estimate: Option<u64>,
        policy: ExpandPolicy,
        version: Option<u64>,
    },
    AntiJoin {
        input: NodeId,
        inner: NodeId,
        outer_var: String,
    },
    OuterReference {
        outer_var: String,
    },
    RankFuse {
        arms: [RankArm; 2],
        k: Option<ExprMirror>,
        limit: Option<usize>,
        prefilter: PrefilterMirror,
    },
    Projection {
        input: NodeId,
        return_exprs: Vec<ProjectionMirror>,
    },
    Aggregate {
        input: NodeId,
        return_exprs: Vec<ProjectionMirror>,
    },
    Sort {
        input: NodeId,
        order_by: Vec<OrderingMirror>,
        fetch: Option<usize>,
        tiebreak: Vec<String>,
    },
}

fn projections(exprs: &[IRProjection]) -> Vec<ProjectionMirror> {
    exprs.iter().map(ProjectionMirror::from).collect()
}

fn projections_back(exprs: Vec<ProjectionMirror>) -> Vec<IRProjection> {
    exprs.into_iter().map(IRProjection::from).collect()
}

impl From<&PhysicalNode> for NodeMirror {
    fn from(node: &PhysicalNode) -> Self {
        match node {
            PhysicalNode::MetadataCount { spec, return_exprs } => Self::MetadataCount {
                spec: ScanSpecMirror::from(spec.as_ref()),
                return_exprs: projections(return_exprs),
            },
            PhysicalNode::Scan {
                source,
                spec,
                ordered,
                keys_only,
                ranked,
            } => Self::Scan {
                source: ScanInputMirror::from(source),
                spec: ScanSpecMirror::from(spec.as_ref()),
                ordered: *ordered,
                keys_only: *keys_only,
                ranked: ranked
                    .as_ref()
                    .map(|ranked| Box::new(RankedMirror::from(ranked))),
            },
            PhysicalNode::SortMergeJoin {
                left,
                right,
                kind,
                on,
                build,
                drop_equal_addresses,
            } => Self::SortMergeJoin {
                left: *left,
                right: *right,
                kind: *kind,
                on: on.clone(),
                build: *build,
                drop_equal_addresses: *drop_equal_addresses,
            },
            PhysicalNode::HashJoin {
                probe,
                build,
                binding,
                fallback,
            } => Self::HashJoin {
                probe: *probe,
                build: *build,
                binding: binding.clone(),
                fallback: *fallback,
            },
            PhysicalNode::HydrateByAddress { input, side } => Self::HydrateByAddress {
                input: *input,
                side: *side,
            },
            PhysicalNode::RowCompare { input } => Self::RowCompare { input: *input },
            PhysicalNode::ClassifyThreeWay { input } => Self::ClassifyThreeWay { input: *input },
            PhysicalNode::Limit { input, rows } => Self::Limit {
                input: *input,
                rows: *rows,
            },
            PhysicalNode::Page {
                input,
                rows,
                bytes,
                resume,
            } => Self::Page {
                input: *input,
                rows: *rows,
                bytes: *bytes,
                resume: resume.clone(),
            },
            PhysicalNode::CrossJoin { left, right } => Self::CrossJoin {
                left: *left,
                right: *right,
            },
            PhysicalNode::Filter { input, filters } => Self::Filter {
                input: *input,
                filters: filters.iter().map(FilterMirror::from).collect(),
            },
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
                version,
            } => Self::Expand {
                input: *input,
                src: src.clone(),
                dst: dst.clone(),
                edge_type: edge_type.clone(),
                direction: DirectionMirror::from(*direction),
                dst_type: dst_type.clone(),
                min_hops: *min_hops,
                max_hops: *max_hops,
                edge_binding: edge_binding.clone(),
                mode: *mode,
                frontier_estimate: *frontier_estimate,
                policy: policy.clone(),
                version: *version,
            },
            PhysicalNode::AntiJoin {
                input,
                inner,
                outer_var,
            } => Self::AntiJoin {
                input: *input,
                inner: *inner,
                outer_var: outer_var.clone(),
            },
            PhysicalNode::OuterReference { outer_var } => Self::OuterReference {
                outer_var: outer_var.clone(),
            },
            PhysicalNode::RankFuse {
                arms,
                k,
                limit,
                prefilter,
            } => Self::RankFuse {
                arms: arms.clone(),
                k: k.as_ref().map(ExprMirror::from),
                limit: *limit,
                prefilter: PrefilterMirror::from(prefilter),
            },
            PhysicalNode::Projection {
                input,
                return_exprs,
            } => Self::Projection {
                input: *input,
                return_exprs: projections(return_exprs),
            },
            PhysicalNode::Aggregate {
                input,
                return_exprs,
            } => Self::Aggregate {
                input: *input,
                return_exprs: projections(return_exprs),
            },
            PhysicalNode::Sort {
                input,
                order_by,
                fetch,
                tiebreak,
            } => Self::Sort {
                input: *input,
                order_by: order_by.iter().map(OrderingMirror::from).collect(),
                fetch: *fetch,
                tiebreak: tiebreak.clone(),
            },
        }
    }
}

impl TryFrom<NodeMirror> for PhysicalNode {
    type Error = PlanError;

    fn try_from(mirror: NodeMirror) -> Result<Self, PlanError> {
        Ok(match mirror {
            NodeMirror::MetadataCount { spec, return_exprs } => Self::MetadataCount {
                spec: Box::new(ScanSpec::try_from(spec)?),
                return_exprs: projections_back(return_exprs),
            },
            NodeMirror::Scan {
                source,
                spec,
                ordered,
                keys_only,
                ranked,
            } => Self::Scan {
                source: ScanInput::from(source),
                spec: Box::new(ScanSpec::try_from(spec)?),
                ordered,
                keys_only,
                ranked: ranked.map(|ranked| RankedAccess::from(*ranked)),
            },
            NodeMirror::SortMergeJoin {
                left,
                right,
                kind,
                on,
                build,
                drop_equal_addresses,
            } => Self::SortMergeJoin {
                left,
                right,
                kind,
                on,
                build,
                drop_equal_addresses,
            },
            NodeMirror::HashJoin {
                probe,
                build,
                binding,
                fallback,
            } => Self::HashJoin {
                probe,
                build,
                binding,
                fallback,
            },
            NodeMirror::HydrateByAddress { input, side } => Self::HydrateByAddress { input, side },
            NodeMirror::RowCompare { input } => Self::RowCompare { input },
            NodeMirror::ClassifyThreeWay { input } => Self::ClassifyThreeWay { input },
            NodeMirror::Limit { input, rows } => Self::Limit { input, rows },
            NodeMirror::Page {
                input,
                rows,
                bytes,
                resume,
            } => Self::Page {
                input,
                rows,
                bytes,
                resume,
            },
            NodeMirror::CrossJoin { left, right } => Self::CrossJoin { left, right },
            NodeMirror::Filter { input, filters } => Self::Filter {
                input,
                filters: filters.into_iter().map(IRFilter::from).collect(),
            },
            NodeMirror::Expand {
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
                version,
            } => Self::Expand {
                input,
                src,
                dst,
                edge_type,
                direction: Direction::from(direction),
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                mode,
                frontier_estimate,
                policy,
                version,
            },
            NodeMirror::AntiJoin {
                input,
                inner,
                outer_var,
            } => Self::AntiJoin {
                input,
                inner,
                outer_var,
            },
            NodeMirror::OuterReference { outer_var } => Self::OuterReference { outer_var },
            NodeMirror::RankFuse {
                arms,
                k,
                limit,
                prefilter,
            } => Self::RankFuse {
                arms,
                k: k.map(IRExpr::from),
                limit,
                prefilter: Prefilter::from(prefilter),
            },
            NodeMirror::Projection {
                input,
                return_exprs,
            } => Self::Projection {
                input,
                return_exprs: projections_back(return_exprs),
            },
            NodeMirror::Aggregate {
                input,
                return_exprs,
            } => Self::Aggregate {
                input,
                return_exprs: projections_back(return_exprs),
            },
            NodeMirror::Sort {
                input,
                order_by,
                fetch,
                tiebreak,
            } => Self::Sort {
                input,
                order_by: order_by.into_iter().map(IROrdering::from).collect(),
                fetch,
                tiebreak,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScanInputMirror {
    Table,
    Dependent { input: NodeId },
}

impl From<&ScanInput> for ScanInputMirror {
    fn from(source: &ScanInput) -> Self {
        match source {
            ScanInput::Table => Self::Table,
            ScanInput::Dependent { input } => Self::Dependent { input: *input },
        }
    }
}

impl From<ScanInputMirror> for ScanInput {
    fn from(mirror: ScanInputMirror) -> Self {
        match mirror {
            ScanInputMirror::Table => Self::Table,
            ScanInputMirror::Dependent { input } => Self::Dependent { input },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankedMirror {
    pub kind: RankKind,
    pub property: String,
    pub query: ExprMirror,
    pub fetch: Option<usize>,
    pub nprobes: Option<usize>,
    pub scope: RankScope,
    pub overfetch: Vec<OverfetchRung>,
    pub prefilter: Option<PrefilterMirror>,
}

impl From<&RankedAccess> for RankedMirror {
    fn from(ranked: &RankedAccess) -> Self {
        Self {
            kind: ranked.kind,
            property: ranked.property.clone(),
            query: ExprMirror::from(&ranked.query),
            fetch: ranked.fetch,
            nprobes: ranked.nprobes,
            scope: ranked.scope,
            overfetch: ranked.overfetch.clone(),
            prefilter: ranked.prefilter.as_ref().map(PrefilterMirror::from),
        }
    }
}

impl From<RankedMirror> for RankedAccess {
    fn from(mirror: RankedMirror) -> Self {
        Self {
            kind: mirror.kind,
            property: mirror.property,
            query: IRExpr::from(mirror.query),
            fetch: mirror.fetch,
            nprobes: mirror.nprobes,
            scope: mirror.scope,
            overfetch: mirror.overfetch,
            prefilter: mirror.prefilter.map(Prefilter::from),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HopMirror {
    pub edge_type: String,
    pub direction: DirectionMirror,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrefilterMirror {
    pub ranked_type: String,
    pub hops: Vec<HopMirror>,
    pub feeds: Vec<NodeId>,
}

impl From<&Prefilter> for PrefilterMirror {
    fn from(prefilter: &Prefilter) -> Self {
        Self {
            ranked_type: prefilter.ranked_type.clone(),
            hops: prefilter
                .hops
                .iter()
                .map(|hop| HopMirror {
                    edge_type: hop.edge_type.clone(),
                    direction: DirectionMirror::from(hop.direction),
                })
                .collect(),
            feeds: prefilter.feeds.clone(),
        }
    }
}

impl From<PrefilterMirror> for Prefilter {
    fn from(mirror: PrefilterMirror) -> Self {
        Self {
            ranked_type: mirror.ranked_type,
            hops: mirror
                .hops
                .into_iter()
                .map(|hop| Hop {
                    edge_type: hop.edge_type,
                    direction: Direction::from(hop.direction),
                })
                .collect(),
            feeds: mirror.feeds,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanSpecMirror {
    pub side: SideId,
    pub table: TableRef,
    pub version: Option<u64>,
    pub columns: SystemColumnsMirror,
    pub fragments: Option<Vec<u64>>,
    pub projection: Option<Vec<String>>,
    pub filter: Option<PredicateMirror>,
    pub binding: Option<String>,
}

impl From<&ScanSpec> for ScanSpecMirror {
    fn from(spec: &ScanSpec) -> Self {
        Self {
            side: spec.side,
            table: spec.table.clone(),
            version: spec.version,
            columns: SystemColumnsMirror::from(spec.columns),
            fragments: spec.fragments.clone(),
            projection: spec.projection.clone(),
            filter: spec.filter.as_ref().map(PredicateMirror::from),
            binding: spec.binding.clone(),
        }
    }
}

impl TryFrom<ScanSpecMirror> for ScanSpec {
    type Error = PlanError;

    fn try_from(mirror: ScanSpecMirror) -> Result<Self, PlanError> {
        Ok(Self {
            side: mirror.side,
            table: mirror.table,
            version: mirror.version,
            columns: SystemColumns::try_from(mirror.columns)?,
            fragments: mirror.fragments,
            projection: mirror.projection,
            filter: mirror.filter.map(Predicate::from),
            binding: mirror.binding,
        })
    }
}

/// The three spellings a scan binds; read back by matching the compiler's
/// constants, since the spelling is a `&'static str`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemColumnsMirror {
    pub id: String,
    pub src: String,
    pub dst: String,
}

impl From<SystemColumns> for SystemColumnsMirror {
    fn from(columns: SystemColumns) -> Self {
        Self {
            id: columns.id.to_string(),
            src: columns.src.to_string(),
            dst: columns.dst.to_string(),
        }
    }
}

impl TryFrom<SystemColumnsMirror> for SystemColumns {
    type Error = PlanError;

    fn try_from(mirror: SystemColumnsMirror) -> Result<Self, PlanError> {
        [
            SYSTEM_COLUMNS_V3,
            SYSTEM_COLUMNS_META,
            SYSTEM_COLUMNS_LEGACY,
        ]
        .into_iter()
        .find(|known| known.id == mirror.id && known.src == mirror.src && known.dst == mirror.dst)
        .ok_or_else(|| internal(format!("unknown system-column spelling {mirror:?}")))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PredicateMirror {
    IdAfter {
        id: String,
    },
    VersionWindow {
        from: u64,
        to: u64,
    },
    And {
        left: Box<PredicateMirror>,
        right: Box<PredicateMirror>,
    },
    Gq {
        reads: Vec<ColumnRef>,
        text: String,
        filter: FilterMirror,
    },
}

impl From<&Predicate> for PredicateMirror {
    fn from(predicate: &Predicate) -> Self {
        match predicate {
            Predicate::IdAfter { id } => Self::IdAfter { id: id.clone() },
            Predicate::VersionWindow { from, to } => Self::VersionWindow {
                from: *from,
                to: *to,
            },
            Predicate::And { left, right } => Self::And {
                left: Box::new(Self::from(left.as_ref())),
                right: Box::new(Self::from(right.as_ref())),
            },
            Predicate::Gq {
                reads,
                text,
                filter,
            } => Self::Gq {
                reads: reads.clone(),
                text: text.clone(),
                filter: FilterMirror::from(&filter.0),
            },
        }
    }
}

impl From<PredicateMirror> for Predicate {
    fn from(mirror: PredicateMirror) -> Self {
        match mirror {
            PredicateMirror::IdAfter { id } => Self::IdAfter { id },
            PredicateMirror::VersionWindow { from, to } => Self::VersionWindow { from, to },
            PredicateMirror::And { left, right } => Self::And {
                left: Box::new(Self::from(*left)),
                right: Box::new(Self::from(*right)),
            },
            PredicateMirror::Gq {
                reads,
                text,
                filter,
            } => Self::Gq {
                reads,
                text,
                filter: GqFilter(IRFilter::from(filter)),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterMirror {
    pub left: ExprMirror,
    pub op: CompOpMirror,
    pub right: ExprMirror,
}

impl From<&IRFilter> for FilterMirror {
    fn from(filter: &IRFilter) -> Self {
        Self {
            left: ExprMirror::from(&filter.left),
            op: CompOpMirror::from(filter.op),
            right: ExprMirror::from(&filter.right),
        }
    }
}

impl From<FilterMirror> for IRFilter {
    fn from(mirror: FilterMirror) -> Self {
        Self {
            left: IRExpr::from(mirror.left),
            op: CompOp::from(mirror.op),
            right: IRExpr::from(mirror.right),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompOpMirror {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    Contains,
    StartsWith,
    StringContains,
}

impl From<CompOp> for CompOpMirror {
    fn from(op: CompOp) -> Self {
        match op {
            CompOp::Eq => Self::Eq,
            CompOp::Ne => Self::Ne,
            CompOp::Gt => Self::Gt,
            CompOp::Lt => Self::Lt,
            CompOp::Ge => Self::Ge,
            CompOp::Le => Self::Le,
            CompOp::Contains => Self::Contains,
            CompOp::StartsWith => Self::StartsWith,
            CompOp::StringContains => Self::StringContains,
        }
    }
}

impl From<CompOpMirror> for CompOp {
    fn from(mirror: CompOpMirror) -> Self {
        match mirror {
            CompOpMirror::Eq => Self::Eq,
            CompOpMirror::Ne => Self::Ne,
            CompOpMirror::Gt => Self::Gt,
            CompOpMirror::Lt => Self::Lt,
            CompOpMirror::Ge => Self::Ge,
            CompOpMirror::Le => Self::Le,
            CompOpMirror::Contains => Self::Contains,
            CompOpMirror::StartsWith => Self::StartsWith,
            CompOpMirror::StringContains => Self::StringContains,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggFuncMirror {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl From<AggFunc> for AggFuncMirror {
    fn from(func: AggFunc) -> Self {
        match func {
            AggFunc::Count => Self::Count,
            AggFunc::Sum => Self::Sum,
            AggFunc::Avg => Self::Avg,
            AggFunc::Min => Self::Min,
            AggFunc::Max => Self::Max,
        }
    }
}

impl From<AggFuncMirror> for AggFunc {
    fn from(mirror: AggFuncMirror) -> Self {
        match mirror {
            AggFuncMirror::Count => Self::Count,
            AggFuncMirror::Sum => Self::Sum,
            AggFuncMirror::Avg => Self::Avg,
            AggFuncMirror::Min => Self::Min,
            AggFuncMirror::Max => Self::Max,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectionMirror {
    Out,
    In,
    Both,
}

impl From<Direction> for DirectionMirror {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::Out => Self::Out,
            Direction::In => Self::In,
            Direction::Both => Self::Both,
        }
    }
}

impl From<DirectionMirror> for Direction {
    fn from(mirror: DirectionMirror) -> Self {
        match mirror {
            DirectionMirror::Out => Self::Out,
            DirectionMirror::In => Self::In,
            DirectionMirror::Both => Self::Both,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "literal", content = "value", rename_all = "snake_case")]
pub enum LiteralMirror {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Date(String),
    DateTime(String),
    List(Vec<LiteralMirror>),
}

impl From<&Literal> for LiteralMirror {
    fn from(literal: &Literal) -> Self {
        match literal {
            Literal::Null => Self::Null,
            Literal::String(text) => Self::String(text.clone()),
            Literal::Integer(value) => Self::Integer(*value),
            Literal::Float(value) => Self::Float(*value),
            Literal::Bool(value) => Self::Bool(*value),
            Literal::Date(text) => Self::Date(text.clone()),
            Literal::DateTime(text) => Self::DateTime(text.clone()),
            Literal::List(items) => Self::List(items.iter().map(Self::from).collect()),
        }
    }
}

impl From<LiteralMirror> for Literal {
    fn from(mirror: LiteralMirror) -> Self {
        match mirror {
            LiteralMirror::Null => Self::Null,
            LiteralMirror::String(text) => Self::String(text),
            LiteralMirror::Integer(value) => Self::Integer(value),
            LiteralMirror::Float(value) => Self::Float(value),
            LiteralMirror::Bool(value) => Self::Bool(value),
            LiteralMirror::Date(text) => Self::Date(text),
            LiteralMirror::DateTime(text) => Self::DateTime(text),
            LiteralMirror::List(items) => Self::List(items.into_iter().map(Self::from).collect()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "expr", rename_all = "snake_case")]
pub enum ExprMirror {
    PropAccess {
        variable: String,
        property: String,
    },
    Nearest {
        variable: String,
        property: String,
        query: Box<ExprMirror>,
    },
    Search {
        field: Box<ExprMirror>,
        query: Box<ExprMirror>,
    },
    Fuzzy {
        field: Box<ExprMirror>,
        query: Box<ExprMirror>,
        max_edits: Option<Box<ExprMirror>>,
    },
    MatchText {
        field: Box<ExprMirror>,
        query: Box<ExprMirror>,
    },
    Bm25 {
        field: Box<ExprMirror>,
        query: Box<ExprMirror>,
    },
    Rrf {
        primary: Box<ExprMirror>,
        secondary: Box<ExprMirror>,
        k: Option<Box<ExprMirror>>,
    },
    Variable {
        name: String,
    },
    Param {
        name: String,
    },
    Literal {
        value: LiteralMirror,
    },
    Aggregate {
        func: AggFuncMirror,
        arg: Box<ExprMirror>,
    },
    AliasRef {
        alias: String,
    },
}

fn boxed(expr: &IRExpr) -> Box<ExprMirror> {
    Box::new(ExprMirror::from(expr))
}

fn unboxed(mirror: ExprMirror) -> Box<IRExpr> {
    Box::new(IRExpr::from(mirror))
}

impl From<&IRExpr> for ExprMirror {
    fn from(expr: &IRExpr) -> Self {
        match expr {
            IRExpr::PropAccess { variable, property } => Self::PropAccess {
                variable: variable.clone(),
                property: property.clone(),
            },
            IRExpr::Nearest {
                variable,
                property,
                query,
            } => Self::Nearest {
                variable: variable.clone(),
                property: property.clone(),
                query: boxed(query),
            },
            IRExpr::Search { field, query } => Self::Search {
                field: boxed(field),
                query: boxed(query),
            },
            IRExpr::Fuzzy {
                field,
                query,
                max_edits,
            } => Self::Fuzzy {
                field: boxed(field),
                query: boxed(query),
                max_edits: max_edits.as_deref().map(boxed),
            },
            IRExpr::MatchText { field, query } => Self::MatchText {
                field: boxed(field),
                query: boxed(query),
            },
            IRExpr::Bm25 { field, query } => Self::Bm25 {
                field: boxed(field),
                query: boxed(query),
            },
            IRExpr::Rrf {
                primary,
                secondary,
                k,
            } => Self::Rrf {
                primary: boxed(primary),
                secondary: boxed(secondary),
                k: k.as_deref().map(boxed),
            },
            IRExpr::Variable(name) => Self::Variable { name: name.clone() },
            IRExpr::Param(name) => Self::Param { name: name.clone() },
            IRExpr::Literal(literal) => Self::Literal {
                value: LiteralMirror::from(literal),
            },
            IRExpr::Aggregate { func, arg } => Self::Aggregate {
                func: AggFuncMirror::from(*func),
                arg: boxed(arg),
            },
            IRExpr::AliasRef(alias) => Self::AliasRef {
                alias: alias.clone(),
            },
        }
    }
}

impl From<ExprMirror> for IRExpr {
    fn from(mirror: ExprMirror) -> Self {
        match mirror {
            ExprMirror::PropAccess { variable, property } => {
                Self::PropAccess { variable, property }
            }
            ExprMirror::Nearest {
                variable,
                property,
                query,
            } => Self::Nearest {
                variable,
                property,
                query: unboxed(*query),
            },
            ExprMirror::Search { field, query } => Self::Search {
                field: unboxed(*field),
                query: unboxed(*query),
            },
            ExprMirror::Fuzzy {
                field,
                query,
                max_edits,
            } => Self::Fuzzy {
                field: unboxed(*field),
                query: unboxed(*query),
                max_edits: max_edits.map(|edits| unboxed(*edits)),
            },
            ExprMirror::MatchText { field, query } => Self::MatchText {
                field: unboxed(*field),
                query: unboxed(*query),
            },
            ExprMirror::Bm25 { field, query } => Self::Bm25 {
                field: unboxed(*field),
                query: unboxed(*query),
            },
            ExprMirror::Rrf {
                primary,
                secondary,
                k,
            } => Self::Rrf {
                primary: unboxed(*primary),
                secondary: unboxed(*secondary),
                k: k.map(|k| unboxed(*k)),
            },
            ExprMirror::Variable { name } => Self::Variable(name),
            ExprMirror::Param { name } => Self::Param(name),
            ExprMirror::Literal { value } => Self::Literal(Literal::from(value)),
            ExprMirror::Aggregate { func, arg } => Self::Aggregate {
                func: AggFunc::from(func),
                arg: unboxed(*arg),
            },
            ExprMirror::AliasRef { alias } => Self::AliasRef(alias),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionMirror {
    pub expr: ExprMirror,
    pub alias: Option<String>,
}

impl From<&IRProjection> for ProjectionMirror {
    fn from(projection: &IRProjection) -> Self {
        Self {
            expr: ExprMirror::from(&projection.expr),
            alias: projection.alias.clone(),
        }
    }
}

impl From<ProjectionMirror> for IRProjection {
    fn from(mirror: ProjectionMirror) -> Self {
        Self {
            expr: IRExpr::from(mirror.expr),
            alias: mirror.alias,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderingMirror {
    pub expr: ExprMirror,
    pub descending: bool,
}

impl From<&IROrdering> for OrderingMirror {
    fn from(ordering: &IROrdering) -> Self {
        Self {
            expr: ExprMirror::from(&ordering.expr),
            descending: ordering.descending,
        }
    }
}

impl From<OrderingMirror> for IROrdering {
    fn from(mirror: OrderingMirror) -> Self {
        Self {
            expr: IRExpr::from(mirror.expr),
            descending: mirror.descending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertiesMirror {
    pub schema: Vec<FieldMirror>,
    #[serde(default)]
    pub schema_metadata: HashMap<String, String>,
    pub ordering: Option<Vec<String>>,
    pub rows: Estimate,
    pub work_bytes: Estimate,
    pub retained_limit: Option<u64>,
    pub sources: Vec<StatisticSourceMirror>,
}

impl From<&Properties> for PropertiesMirror {
    fn from(properties: &Properties) -> Self {
        Self {
            schema: properties
                .schema
                .fields()
                .iter()
                .map(|field| FieldMirror::from(field.as_ref()))
                .collect(),
            schema_metadata: properties.schema.metadata().clone(),
            ordering: properties.ordering.clone(),
            rows: properties.rows,
            work_bytes: properties.work_bytes,
            retained_limit: properties.retained_limit,
            sources: properties
                .sources
                .iter()
                .map(StatisticSourceMirror::from)
                .collect(),
        }
    }
}

impl TryFrom<PropertiesMirror> for Properties {
    type Error = PlanError;

    fn try_from(mirror: PropertiesMirror) -> Result<Self, PlanError> {
        let fields = mirror
            .schema
            .into_iter()
            .map(Field::try_from)
            .collect::<Result<Vec<Field>, _>>()?;
        let schema: SchemaRef = Arc::new(Schema::new_with_metadata(fields, mirror.schema_metadata));
        Ok(Self {
            schema,
            ordering: mirror.ordering,
            rows: mirror.rows,
            work_bytes: mirror.work_bytes,
            retained_limit: mirror.retained_limit,
            sources: mirror
                .sources
                .into_iter()
                .map(StatisticSource::from)
                .collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatisticSourceMirror {
    pub statistic: String,
    pub value: String,
    pub origin: String,
}

impl From<&StatisticSource> for StatisticSourceMirror {
    fn from(source: &StatisticSource) -> Self {
        Self {
            statistic: source.statistic.clone(),
            value: source.value.clone(),
            origin: source.origin.to_string(),
        }
    }
}

impl From<StatisticSourceMirror> for StatisticSource {
    fn from(mirror: StatisticSourceMirror) -> Self {
        Self {
            statistic: mirror.statistic,
            value: mirror.value,
            origin: intern_origin(mirror.origin),
        }
    }
}

/// A statistic's origin is a `&'static str` in the plan and one of a few
/// planner words; the process keeps each distinct one it reads back once.
fn intern_origin(origin: String) -> &'static str {
    static ORIGINS: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let mut origins = ORIGINS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(known) = origins.get(origin.as_str()) {
        return known;
    }
    let leaked: &'static str = Box::leak(origin.into_boxed_str());
    origins.insert(leaked);
    leaked
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldMirror {
    pub name: String,
    pub data_type: DataTypeMirror,
    pub nullable: bool,
    pub metadata: BTreeMap<String, String>,
}

impl From<&Field> for FieldMirror {
    fn from(field: &Field) -> Self {
        Self {
            name: field.name().clone(),
            data_type: DataTypeMirror::from(field.data_type()),
            nullable: field.is_nullable(),
            metadata: field
                .metadata()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }
}

impl TryFrom<FieldMirror> for Field {
    type Error = PlanError;

    fn try_from(mirror: FieldMirror) -> Result<Self, PlanError> {
        let metadata: HashMap<String, String> = mirror.metadata.into_iter().collect();
        Ok(Field::new(
            mirror.name,
            DataType::try_from(mirror.data_type)?,
            mirror.nullable,
        )
        .with_metadata(metadata))
    }
}

/// The Arrow types the catalog and the planner build; any other type is
/// refused when read back.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataTypeMirror {
    Null,
    Boolean,
    Int8,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    FixedSizeBinary { width: i32 },
    Date32,
    Date64,
    List { item: Box<FieldMirror> },
    LargeList { item: Box<FieldMirror> },
    FixedSizeList { item: Box<FieldMirror>, length: i32 },
    Struct { fields: Vec<FieldMirror> },
    Other { text: String },
}

impl From<&DataType> for DataTypeMirror {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::FixedSizeBinary(width) => Self::FixedSizeBinary { width: *width },
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::List(item) => Self::List {
                item: Box::new(FieldMirror::from(item.as_ref())),
            },
            DataType::LargeList(item) => Self::LargeList {
                item: Box::new(FieldMirror::from(item.as_ref())),
            },
            DataType::FixedSizeList(item, length) => Self::FixedSizeList {
                item: Box::new(FieldMirror::from(item.as_ref())),
                length: *length,
            },
            DataType::Struct(fields) => Self::Struct {
                fields: fields
                    .iter()
                    .map(|field| FieldMirror::from(field.as_ref()))
                    .collect(),
            },
            other => Self::Other {
                text: other.to_string(),
            },
        }
    }
}

impl TryFrom<DataTypeMirror> for DataType {
    type Error = PlanError;

    fn try_from(mirror: DataTypeMirror) -> Result<Self, PlanError> {
        let item = |item: Box<FieldMirror>| Field::try_from(*item).map(Arc::new);
        Ok(match mirror {
            DataTypeMirror::Null => Self::Null,
            DataTypeMirror::Boolean => Self::Boolean,
            DataTypeMirror::Int8 => Self::Int8,
            DataTypeMirror::Int32 => Self::Int32,
            DataTypeMirror::Int64 => Self::Int64,
            DataTypeMirror::UInt32 => Self::UInt32,
            DataTypeMirror::UInt64 => Self::UInt64,
            DataTypeMirror::Float32 => Self::Float32,
            DataTypeMirror::Float64 => Self::Float64,
            DataTypeMirror::Utf8 => Self::Utf8,
            DataTypeMirror::LargeUtf8 => Self::LargeUtf8,
            DataTypeMirror::Binary => Self::Binary,
            DataTypeMirror::LargeBinary => Self::LargeBinary,
            DataTypeMirror::FixedSizeBinary { width } => Self::FixedSizeBinary(width),
            DataTypeMirror::Date32 => Self::Date32,
            DataTypeMirror::Date64 => Self::Date64,
            DataTypeMirror::List { item: field } => Self::List(item(field)?),
            DataTypeMirror::LargeList { item: field } => Self::LargeList(item(field)?),
            DataTypeMirror::FixedSizeList {
                item: field,
                length,
            } => Self::FixedSizeList(item(field)?, length),
            DataTypeMirror::Struct { fields } => Self::Struct(
                fields
                    .into_iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<Field>, _>>()?
                    .into(),
            ),
            DataTypeMirror::Other { text } => {
                return Err(internal(format!("the Arrow type `{text}` has no mirror")));
            }
        })
    }
}
