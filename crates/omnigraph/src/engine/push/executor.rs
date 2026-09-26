//! Pipelines from a physical plan. Walking from the root, a streaming node
//! joins the current pipeline and the walk continues into its input; a join
//! with a build side ends the walk there, runs the build input as its own
//! pipeline into the join's sink, and continues into the probe input; a scan
//! is the source. Child pipelines therefore complete before their parent
//! starts, and inside a pipeline the push is a call chain. The root pipeline
//! is pulled by its consumer (DuckDB's `ExecutePull` for a result sink), or
//! driven into a [`Sink`] by [`run`].

use std::collections::VecDeque;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use omnigraph_planner::{NodeId, PhysicalNode, PhysicalPlan, ScanInput};

use super::chunk::{Chunk, prefixed};
use super::classify::ClassifyThreeWay;
use super::compare::RowCompare;
use super::context::{ExecContext, Probe, side_name};
use super::error::{ExecError, Result};
use super::hydrate::HydrateByAddress;
use super::join::{JoinBuild, JoinProbe, JoinRule, MergeJoinSource};
use super::page::Page;
use super::roles::{Operator, OperatorResult, Sink, SinkResult, Source};
use super::scan::ScanSource;

pub struct Pipeline {
    source: Box<dyn Source>,
    operators: Vec<Box<dyn Operator>>,
    /// Rows per chunk: the vector size, or the page's row target when that
    /// is smaller, so a small page never examines or hydrates past its need.
    chunk_rows: usize,
    morsel: VecDeque<Chunk>,
    pending: VecDeque<Chunk>,
    exhausted: bool,
    flushed: bool,
}

impl Pipeline {
    pub fn new(
        source: Box<dyn Source>,
        operators: Vec<Box<dyn Operator>>,
        chunk_rows: usize,
    ) -> Self {
        Self {
            source,
            operators,
            chunk_rows,
            morsel: VecDeque::new(),
            pending: VecDeque::new(),
            exhausted: false,
            flushed: false,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    /// The next output chunk of the root pipeline, or `None` once every
    /// operator has flushed. One chunk is pushed per demand, so the
    /// consumer's page, not the morsel, bounds the work done ahead of it.
    pub async fn pull(&mut self) -> Result<Option<Chunk>> {
        loop {
            if let Some(chunk) = self.pending.pop_front() {
                return Ok(Some(chunk));
            }
            if self.flushed {
                return Ok(None);
            }
            if let Some(chunk) = self.morsel.pop_front() {
                self.push_through(0, chunk).await?;
                continue;
            }
            if self.exhausted {
                for index in 0..self.operators.len() {
                    let mut out = Vec::new();
                    self.operators[index].finish(&mut out).await?;
                    for chunk in out {
                        self.push_through(index + 1, chunk).await?;
                    }
                }
                self.flushed = true;
                continue;
            }
            match self.source.next().await? {
                None => self.exhausted = true,
                Some(morsel) => self.morsel.extend(Chunk::split(morsel, self.chunk_rows)),
            }
        }
    }

    async fn push_through(&mut self, start: usize, chunk: Chunk) -> Result<()> {
        let mut stack = vec![(start, chunk)];
        while let Some((index, chunk)) = stack.pop() {
            if index == self.operators.len() {
                self.pending.push_back(chunk);
                continue;
            }
            let mut out = Vec::new();
            let result = self.operators[index].execute(chunk, &mut out).await?;
            for chunk in out.into_iter().rev() {
                stack.push((index + 1, chunk));
            }
            if result == OperatorResult::Finished {
                self.exhausted = true;
                self.morsel.clear();
            }
        }
        Ok(())
    }

    /// Drive the pipeline into `sink` until it finishes or the sink does.
    pub async fn drain(&mut self, sink: &mut dyn Sink) -> Result<()> {
        while let Some(chunk) = self.pull().await? {
            if sink.sink(chunk).await? == SinkResult::Finished {
                break;
            }
        }
        sink.finalize().await
    }
}

/// A finished pipeline read as a source; used where a join's input is a
/// pipeline with operators.
struct PipelineSource(Pipeline);

#[async_trait::async_trait]
impl Source for PipelineSource {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.0.pull().await? {
            Some(chunk) => Ok(Some(chunk.compact()?.batch)),
            None => Ok(None),
        }
    }
}

fn node(plan: &PhysicalPlan, id: NodeId) -> Result<&PhysicalNode> {
    plan.node(id)
        .ok_or_else(|| ExecError::internal(format!("physical node {id} is a tombstone")))
}

fn address_column(plan: &PhysicalPlan, id: NodeId) -> Result<Option<String>> {
    Ok(match node(plan, id)? {
        PhysicalNode::Scan { spec, .. } => {
            Some(prefixed(side_name(spec.side), lance_core::ROW_ADDR))
        }
        _ => None,
    })
}

/// Build the operator chain of the pipeline rooted at `id` into `operators`
/// (input first) and return its source. The read nodes of a GQ query run in
/// the engine's runner, never here; every other node has its operator below.
fn build_chain<'a>(
    plan: &'a PhysicalPlan,
    ctx: &'a ExecContext,
    id: NodeId,
    operators: &'a mut Vec<Box<dyn Operator>>,
) -> BoxFuture<'a, Result<Box<dyn Source>>> {
    Box::pin(async move {
        match node(plan, id)? {
            PhysicalNode::Scan {
                source: ScanInput::Table,
                spec,
                ordered,
                keys_only,
                ..
            } => {
                if spec.fragments.is_some() {
                    ctx.hooks.probe(Probe::ScanTargets {
                        rows: ctx.targets.rows,
                        bytes: ctx.targets.bytes,
                    });
                }
                Ok(
                    Box::new(ScanSource::open(ctx, spec, *ordered, *keys_only).await?)
                        as Box<dyn Source>,
                )
            }
            PhysicalNode::SortMergeJoin {
                left,
                right,
                kind,
                build,
                drop_equal_addresses,
                ..
            } => {
                let rule = JoinRule {
                    kind: *kind,
                    drop_equal_addresses: *drop_equal_addresses,
                };
                let left_address = address_column(plan, *left)?;
                let right_address = address_column(plan, *right)?;
                if *build {
                    let mut build_operators = Vec::new();
                    let build_source = build_chain(plan, ctx, *right, &mut build_operators).await?;
                    let mut sink = JoinBuild::new(build_source.schema());
                    Pipeline::new(build_source, build_operators, super::chunk::VECTOR_SIZE)
                        .drain(&mut sink)
                        .await?;
                    let sorted = sink.sorted()?;
                    let probe_source = build_chain(plan, ctx, *left, operators).await?;
                    operators.push(Box::new(JoinProbe::new(
                        sorted,
                        &probe_source.schema(),
                        rule,
                        left_address,
                        right_address,
                        ctx.hooks.clone(),
                    )));
                    Ok(probe_source)
                } else {
                    let left_source = build_source(plan, ctx, *left).await?;
                    let right_source = build_source(plan, ctx, *right).await?;
                    Ok(Box::new(MergeJoinSource::new(
                        left_source,
                        right_source,
                        rule,
                        left_address,
                        right_address,
                        ctx.hooks.clone(),
                    )) as Box<dyn Source>)
                }
            }
            PhysicalNode::HydrateByAddress { input, side } => {
                let fragments = match node(plan, *input)? {
                    PhysicalNode::Scan { spec, .. } if spec.side == *side => spec.fragments.clone(),
                    _ => scan_fragments_of(plan, *input, *side),
                };
                let source = build_chain(plan, ctx, *input, operators).await?;
                operators.push(Box::new(HydrateByAddress::new(ctx, *side, fragments)?));
                Ok(source)
            }
            PhysicalNode::RowCompare { input } => {
                let source = build_chain(plan, ctx, *input, operators).await?;
                operators.push(Box::new(RowCompare::new(ctx)?));
                Ok(source)
            }
            PhysicalNode::ClassifyThreeWay { input } => {
                let source = build_chain(plan, ctx, *input, operators).await?;
                operators.push(Box::new(ClassifyThreeWay::new(ctx)?));
                Ok(source)
            }
            PhysicalNode::Page {
                input, rows, bytes, ..
            } => {
                let source = build_chain(plan, ctx, *input, operators).await?;
                operators.push(Box::new(Page {
                    rows: *rows,
                    bytes: *bytes,
                }));
                Ok(source)
            }
            read @ (PhysicalNode::MetadataCount { .. }
            | PhysicalNode::Scan {
                source: ScanInput::Dependent { .. },
                ..
            }
            | PhysicalNode::HashJoin { .. }
            | PhysicalNode::CrossJoin { .. }
            | PhysicalNode::Filter { .. }
            | PhysicalNode::Expand { .. }
            | PhysicalNode::AntiJoin { .. }
            | PhysicalNode::OuterReference { .. }
            | PhysicalNode::RankFuse { .. }
            | PhysicalNode::Projection { .. }
            | PhysicalNode::Aggregate { .. }
            | PhysicalNode::Sort { .. }
            | PhysicalNode::Limit { .. }) => Err(ExecError::internal(format!(
                "`{}` is a read node; the engine's runner executes it, not the push pipeline",
                read.name()
            ))),
        }
    })
}

/// The fragment scope of `side`'s scan beneath `id`, if any.
fn scan_fragments_of(
    plan: &PhysicalPlan,
    id: NodeId,
    side: omnigraph_planner::SideId,
) -> Option<Vec<u64>> {
    let node = plan.node(id)?;
    if let PhysicalNode::Scan { spec, .. } = node {
        return (spec.side == side)
            .then(|| spec.fragments.clone())
            .flatten();
    }
    node.inputs()
        .into_iter()
        .find_map(|input| scan_fragments_of(plan, input, side))
}

fn build_source<'a>(
    plan: &'a PhysicalPlan,
    ctx: &'a ExecContext,
    id: NodeId,
) -> BoxFuture<'a, Result<Box<dyn Source>>> {
    Box::pin(async move {
        let mut operators = Vec::new();
        let source = build_chain(plan, ctx, id, &mut operators).await?;
        if operators.is_empty() {
            Ok(source)
        } else {
            Ok(Box::new(PipelineSource(Pipeline::new(
                source,
                operators,
                super::chunk::VECTOR_SIZE,
            ))) as Box<dyn Source>)
        }
    })
}

/// The root pipeline of `plan`, every child pipeline already run.
pub async fn build(plan: &PhysicalPlan, ctx: &ExecContext) -> Result<Pipeline> {
    let mut operators = Vec::new();
    let source = build_chain(plan, ctx, plan.root(), &mut operators).await?;
    Ok(Pipeline::new(source, operators, ctx.targets.rows))
}

/// Build and drive `plan` into `sink`.
pub async fn run(plan: &PhysicalPlan, ctx: &ExecContext, sink: &mut dyn Sink) -> Result<()> {
    build(plan, ctx).await?.drain(sink).await
}
