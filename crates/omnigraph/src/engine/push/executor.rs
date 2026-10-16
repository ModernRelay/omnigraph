//! Pipelines from a physical plan. [`build`] consumes
//! [`PhysicalPlan::pipelines`], the same decomposition explain serializes, so
//! the printed pipelines are the executed ones: child pipelines drain into
//! their join's build side before their parent starts, and inside a pipeline
//! the push is a call chain. The root pipeline is pulled by its consumer
//! (DuckDB's `ExecutePull` for a result sink), or driven into a [`Sink`] by
//! [`run`].

use std::collections::{HashMap, VecDeque};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use omnigraph_planner::{
    NodeId, PhysicalNode, PhysicalPlan, PipelineSink, PipelineSource, ScanInput, SideChain,
};

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

/// A finished pipeline read as a source; used where a streaming join's side
/// is a chain with operators.
struct PulledPipeline(Pipeline);

#[async_trait::async_trait]
impl Source for PulledPipeline {
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

fn require_push_node(node: &PhysicalNode) -> Result<()> {
    match node {
        read @ (PhysicalNode::MetadataCount { .. }
        | PhysicalNode::Scan {
            source: ScanInput::Dependent { .. },
            ..
        }
        | PhysicalNode::HashJoin { .. }
        | PhysicalNode::CrossJoin { .. }
        | PhysicalNode::ContainsJoin { .. }
        | PhysicalNode::Filter { .. }
        | PhysicalNode::Expand { .. }
        | PhysicalNode::AntiJoin { .. }
        | PhysicalNode::OuterReference { .. }
        | PhysicalNode::RankFuse { .. }
        | PhysicalNode::Projection { .. }
        | PhysicalNode::Aggregate { .. }
        | PhysicalNode::Sort { .. }
        | PhysicalNode::HydrateColumns { .. }
        | PhysicalNode::Limit { .. }) => Err(ExecError::internal(format!(
            "`{}` is a read node; the engine's runner executes it, not the push pipeline",
            read.name()
        ))),
        PhysicalNode::Scan {
            source: ScanInput::Table,
            ..
        }
        | PhysicalNode::SortMergeJoin { .. }
        | PhysicalNode::HydrateByAddress { .. }
        | PhysicalNode::RowCompare { .. }
        | PhysicalNode::ClassifyThreeWay { .. }
        | PhysicalNode::Page { .. } => Ok(()),
    }
}

fn refusal(node: &PhysicalNode, role: &str) -> ExecError {
    require_push_node(node).err().unwrap_or_else(|| {
        ExecError::internal(format!("`{}` cannot be a pipeline {role}", node.name()))
    })
}

/// The source of one pipeline, with every build pipeline met in a streaming
/// side already drained into `drained`.
fn source<'a>(
    plan: &'a PhysicalPlan,
    ctx: &'a ExecContext,
    source: &'a PipelineSource,
    drained: &'a mut HashMap<NodeId, RecordBatch>,
) -> BoxFuture<'a, Result<Box<dyn Source>>> {
    Box::pin(async move {
        match source {
            PipelineSource::Tombstone(id) => Err(ExecError::internal(format!(
                "physical node {id} is a tombstone"
            ))),
            PipelineSource::Node(id) => match node(plan, *id)? {
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
                other => Err(refusal(other, "source")),
            },
            PipelineSource::Streaming {
                node: id,
                left,
                right,
            } => match node(plan, *id)? {
                PhysicalNode::SortMergeJoin {
                    left: left_input,
                    right: right_input,
                    kind,
                    build: false,
                    drop_equal_addresses,
                    ..
                } => {
                    let rule = JoinRule {
                        kind: *kind,
                        drop_equal_addresses: *drop_equal_addresses,
                    };
                    let left_address = address_column(plan, *left_input)?;
                    let right_address = address_column(plan, *right_input)?;
                    let left_source = side_source(plan, ctx, left, drained).await?;
                    let right_source = side_source(plan, ctx, right, drained).await?;
                    Ok(Box::new(MergeJoinSource::new(
                        left_source,
                        right_source,
                        rule,
                        left_address,
                        right_address,
                        ctx.hooks.clone(),
                    )) as Box<dyn Source>)
                }
                other => Err(refusal(other, "streaming source")),
            },
        }
    })
}

/// One side of a streaming join: the side's source, wrapped as a pulled
/// pipeline when operators sit between it and the join.
async fn side_source(
    plan: &PhysicalPlan,
    ctx: &ExecContext,
    side: &SideChain,
    drained: &mut HashMap<NodeId, RecordBatch>,
) -> Result<Box<dyn Source>> {
    let side_src = source(plan, ctx, &side.source, drained).await?;
    if side.operators.is_empty() {
        return Ok(side_src);
    }
    let schema = side_src.schema();
    let operators = operators_of(plan, ctx, &side.operators, &schema, drained)?;
    Ok(Box::new(PulledPipeline(Pipeline::new(
        side_src,
        operators,
        super::chunk::VECTOR_SIZE,
    ))))
}

/// The operators of one pipeline, in run order. `source_schema` is the
/// pipeline's source schema, the probe input a build join's probe reads.
fn operators_of(
    plan: &PhysicalPlan,
    ctx: &ExecContext,
    ids: &[NodeId],
    source_schema: &SchemaRef,
    drained: &mut HashMap<NodeId, RecordBatch>,
) -> Result<Vec<Box<dyn Operator>>> {
    let mut operators = Vec::with_capacity(ids.len());
    for &id in ids {
        operators.push(operator(plan, ctx, id, source_schema, drained)?);
    }
    Ok(operators)
}

fn operator(
    plan: &PhysicalPlan,
    ctx: &ExecContext,
    id: NodeId,
    source_schema: &SchemaRef,
    drained: &mut HashMap<NodeId, RecordBatch>,
) -> Result<Box<dyn Operator>> {
    match node(plan, id)? {
        PhysicalNode::SortMergeJoin {
            left,
            right,
            kind,
            build: true,
            drop_equal_addresses,
            ..
        } => {
            let rule = JoinRule {
                kind: *kind,
                drop_equal_addresses: *drop_equal_addresses,
            };
            let sorted = drained.remove(&id).ok_or_else(|| {
                ExecError::internal(format!("join {id} has no drained build pipeline"))
            })?;
            Ok(Box::new(JoinProbe::new(
                sorted,
                source_schema,
                rule,
                address_column(plan, *left)?,
                address_column(plan, *right)?,
                ctx.hooks.clone(),
            )))
        }
        PhysicalNode::HydrateByAddress { input, side } => {
            let fragments = match node(plan, *input)? {
                PhysicalNode::Scan { spec, .. } if spec.side == *side => spec.fragments.clone(),
                _ => scan_fragments_of(plan, *input, *side),
            };
            Ok(Box::new(HydrateByAddress::new(ctx, *side, fragments)?))
        }
        PhysicalNode::RowCompare { .. } => Ok(Box::new(RowCompare::new(ctx)?)),
        PhysicalNode::ClassifyThreeWay { .. } => Ok(Box::new(ClassifyThreeWay::new(ctx)?)),
        PhysicalNode::Page { rows, bytes, .. } => Ok(Box::new(Page {
            rows: *rows,
            bytes: *bytes,
        })),
        other => Err(refusal(other, "operator")),
    }
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

/// The root pipeline of `plan`, every child pipeline already drained into
/// its join's build side, in [`PhysicalPlan::pipelines`] order.
pub async fn build(plan: &PhysicalPlan, ctx: &ExecContext) -> Result<Pipeline> {
    let mut pending = vec![plan.root()];
    while let Some(id) = pending.pop() {
        let node = node(plan, id)?;
        require_push_node(node)?;
        pending.extend(node.inputs());
    }
    let mut specs = plan.pipelines();
    let Some(root) = specs.pop() else {
        return Err(ExecError::internal(
            "a plan decomposes into at least one pipeline".to_string(),
        ));
    };
    let PipelineSink::Consumer = root.sink else {
        return Err(ExecError::internal(
            "the last pipeline feeds the consumer".to_string(),
        ));
    };
    let mut drained = HashMap::new();
    for spec in &specs {
        let PipelineSink::Node(sink) = spec.sink else {
            return Err(ExecError::internal(
                "only the last pipeline feeds the consumer".to_string(),
            ));
        };
        let spec_source = source(plan, ctx, &spec.source, &mut drained).await?;
        let schema = spec_source.schema();
        let operators = operators_of(plan, ctx, &spec.operators, &schema, &mut drained)?;
        let mut build = JoinBuild::new(schema);
        Pipeline::new(spec_source, operators, super::chunk::VECTOR_SIZE)
            .drain(&mut build)
            .await?;
        drained.insert(sink, build.sorted()?);
    }
    let root_source = source(plan, ctx, &root.source, &mut drained).await?;
    let schema = root_source.schema();
    let operators = operators_of(plan, ctx, &root.operators, &schema, &mut drained)?;
    Ok(Pipeline::new(root_source, operators, ctx.targets.rows))
}

/// Build and drive `plan` into `sink`.
pub async fn run(plan: &PhysicalPlan, ctx: &ExecContext, sink: &mut dyn Sink) -> Result<()> {
    build(plan, ctx).await?.drain(sink).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use super::super::chunk::{KEY, strings};
    use crate::table_store::TableStore;
    use arrow_array::{Array, StringArray};

    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::SYSTEM_COLUMNS_V3;
    use omnigraph_planner::{KeyJoinKind, ScanSpec, SideId, TableRef};

    use super::super::context::{BatchTargets, EngineHooks, SideContext, SideRow};

    struct NoHooks;

    #[async_trait::async_trait]
    impl EngineHooks for NoHooks {
        async fn rows_equal(&self, _left: SideRow<'_>, _right: SideRow<'_>) -> Result<bool> {
            Ok(true)
        }

        fn probe(&self, _probe: Probe) {}
    }

    fn context() -> ExecContext {
        let side = |side: SideId, role: &'static str| SideContext {
            side,
            dataset: None,
            schema: Arc::new(Schema::new(vec![Field::new("__id", DataType::Utf8, false)])),
            table: "node:Doc".to_string(),
            role,
            columns: SYSTEM_COLUMNS_V3,
            is_edge: false,
        };
        ExecContext {
            operation: "test",
            sides: vec![side(SideId::Parent, "parent"), side(SideId::Child, "child")],
            targets: BatchTargets {
                rows: 128,
                bytes: 1 << 20,
            },
            hydration_target_bytes: 1 << 20,
            hooks: Arc::new(NoHooks),
        }
    }

    fn scan(plan: &mut PhysicalPlan, side: SideId) -> NodeId {
        plan.add(PhysicalNode::Scan {
            source: ScanInput::Table,
            spec: Box::new(ScanSpec {
                access: None,
                side,
                table: TableRef {
                    type_key: "node:Doc".into(),
                    dataset_path: "node/Doc".into(),
                    native_branch: None,
                },
                version: None,
                columns: SYSTEM_COLUMNS_V3,
                fragments: None,
                projection: None,
                filter: None,
                binding: None,
                runtime_filter: None,
            }),
            ordered: true,
            keys_only: true,
            ranked: None,
        })
    }

    #[derive(Default)]
    struct RecordedHooks {
        scans: Mutex<Vec<SideId>>,
        examined: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl EngineHooks for RecordedHooks {
        async fn rows_equal(&self, _left: SideRow<'_>, _right: SideRow<'_>) -> Result<bool> {
            Ok(true)
        }

        fn probe(&self, probe: Probe) {
            match probe {
                Probe::Scan { side } => self.scans.lock().unwrap().push(side),
                Probe::RowsExamined(rows) => {
                    self.examined.fetch_add(rows, Ordering::Relaxed);
                }
                Probe::Hydration { .. } | Probe::ScanTargets { .. } => {}
            }
        }
    }

    async fn populated_context(root: &str, hooks: Arc<RecordedHooks>) -> ExecContext {
        let mut ctx = context();
        ctx.targets.rows = 1;
        ctx.hooks = hooks;
        for (side, ids) in [
            (SideId::Parent, ["e", "a", "c"]),
            (SideId::Child, ["d", "c", "b"]),
        ] {
            let entry = ctx
                .sides
                .iter_mut()
                .find(|entry| entry.side == side)
                .unwrap();
            let batch = RecordBatch::try_new(
                entry.schema.clone(),
                vec![Arc::new(StringArray::from(ids.to_vec()))],
            )
            .unwrap();
            entry.dataset = Some(
                TableStore::write_dataset(&format!("{root}/{}", side.name()), batch)
                    .await
                    .unwrap(),
            );
        }
        ctx
    }

    fn page(plan: &mut PhysicalPlan, input: NodeId) -> NodeId {
        plan.add(PhysicalNode::Page {
            input,
            rows: 1,
            bytes: 1 << 20,
            resume: None,
        })
    }

    #[derive(Default)]
    struct FirstChunk {
        batch: Option<RecordBatch>,
        finalized: bool,
    }

    #[async_trait::async_trait]
    impl Sink for FirstChunk {
        async fn sink(&mut self, chunk: Chunk) -> Result<SinkResult> {
            assert!(self.batch.replace(chunk.compact()?.batch).is_none());
            Ok(SinkResult::Finished)
        }

        async fn finalize(&mut self) -> Result<()> {
            assert!(!self.finalized);
            self.finalized = true;
            Ok(())
        }
    }

    /// GQT does not call the push executor; decomposition and demand need direct coverage.
    #[tokio::test]
    async fn pipeline_specs_execute_nonempty_build_and_streaming_chains() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = Arc::new(RecordedHooks::default());
        let ctx = populated_context(dir.path().to_str().unwrap(), hooks.clone()).await;
        for build_side in [false, true] {
            hooks.scans.lock().unwrap().clear();
            let mut plan = PhysicalPlan::new();
            let parent = scan(&mut plan, SideId::Parent);
            let child = scan(&mut plan, SideId::Child);
            let PhysicalNode::Scan { ordered, .. } = plan.node_mut(child).unwrap() else {
                unreachable!()
            };
            *ordered = !build_side;
            let parent_page = page(&mut plan, parent);
            let child_page = page(&mut plan, child);
            let join = plan.add(PhysicalNode::SortMergeJoin {
                left: parent_page,
                right: child_page,
                kind: KeyJoinKind::FullOuter,
                on: "__id".into(),
                build: build_side,
                drop_equal_addresses: false,
            });
            let root = page(&mut plan, join);
            plan.set_root(root);
            let specs = plan.pipelines();
            if build_side {
                assert_eq!(specs.len(), 2);
                assert_eq!(specs[0].source, PipelineSource::Node(child));
                assert_eq!(specs[0].operators, vec![child_page]);
                assert_eq!(specs[0].sink, PipelineSink::Node(join));
                assert_eq!(specs[1].source, PipelineSource::Node(parent));
                assert_eq!(specs[1].operators, vec![parent_page, join, root]);
                assert_eq!(specs[1].depends_on, vec![0]);
            } else {
                assert_eq!(specs.len(), 1);
                assert_eq!(
                    specs[0].source,
                    PipelineSource::Streaming {
                        node: join,
                        left: SideChain {
                            source: Box::new(PipelineSource::Node(parent)),
                            operators: vec![parent_page]
                        },
                        right: SideChain {
                            source: Box::new(PipelineSource::Node(child)),
                            operators: vec![child_page]
                        },
                    }
                );
                assert_eq!(specs[0].operators, vec![root]);
                assert!(specs[0].depends_on.is_empty());
            }
            assert_eq!(specs.last().unwrap().sink, PipelineSink::Consumer);
            let mut pipeline = build(&plan, &ctx).await.unwrap();
            let expected_scans = if build_side {
                vec![SideId::Child, SideId::Parent]
            } else {
                vec![SideId::Parent, SideId::Child]
            };
            assert_eq!(*hooks.scans.lock().unwrap(), expected_scans);
            let mut actual = Vec::new();
            while let Some(chunk) = pipeline.pull().await.unwrap() {
                let batch = chunk.compact().unwrap().batch;
                let keys = strings(&batch, KEY).unwrap();
                let parents = strings(&batch, "parent.__id").unwrap();
                let children = strings(&batch, "child.__id").unwrap();
                actual.extend((0..batch.num_rows()).map(|row| {
                    (
                        keys.value(row).to_owned(),
                        parents.is_valid(row).then(|| parents.value(row).to_owned()),
                        children
                            .is_valid(row)
                            .then(|| children.value(row).to_owned()),
                    )
                }));
            }
            let expected = [
                ("a", Some("a"), None),
                ("b", None, Some("b")),
                ("c", Some("c"), Some("c")),
                ("d", None, Some("d")),
                ("e", Some("e"), None),
            ]
            .map(|(key, parent, child)| {
                (
                    key.to_owned(),
                    parent.map(str::to_owned),
                    child.map(str::to_owned),
                )
            });
            assert_eq!(actual, expected, "build={build_side}");
            let PhysicalNode::SortMergeJoin { kind, .. } = plan.node_mut(join).unwrap() else {
                unreachable!()
            };
            *kind = KeyJoinKind::LeftOuter;
            hooks.examined.store(0, Ordering::Relaxed);
            let mut single_pull = build(&plan, &ctx).await.unwrap();
            assert_eq!(hooks.examined.load(Ordering::Relaxed), 0);
            assert!(single_pull.pull().await.unwrap().is_some());
            let first_pull_examined = hooks.examined.load(Ordering::Relaxed);
            drop(single_pull);
            assert!((1..=3).contains(&first_pull_examined));
            if build_side {
                assert_eq!(first_pull_examined, 1);
            }
            hooks.examined.store(0, Ordering::Relaxed);
            let mut pipeline = build(&plan, &ctx).await.unwrap();
            assert_eq!(hooks.examined.load(Ordering::Relaxed), 0);
            let mut sink = FirstChunk::default();
            pipeline.drain(&mut sink).await.unwrap();
            assert!(sink.finalized);
            let batch = sink.batch.unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(strings(&batch, KEY).unwrap().value(0), "a");
            assert_eq!(
                hooks.examined.load(Ordering::Relaxed),
                first_pull_examined,
                "a finished consumer must issue only one pull"
            );
        }
    }

    /// GQT cannot observe whether an unsupported push plan opens a build-side scan.
    #[tokio::test]
    async fn a_read_parent_refuses_before_opening_any_pipeline() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = Arc::new(RecordedHooks::default());
        let ctx = populated_context(dir.path().to_str().unwrap(), hooks.clone()).await;
        let mut plan = PhysicalPlan::new();
        let parent = scan(&mut plan, SideId::Parent);
        let child = scan(&mut plan, SideId::Child);
        let join = plan.add(PhysicalNode::SortMergeJoin {
            left: parent,
            right: child,
            kind: KeyJoinKind::FullOuter,
            on: "__id".into(),
            build: true,
            drop_equal_addresses: false,
        });
        let limit = plan.add(PhysicalNode::Limit {
            input: join,
            rows: 1,
        });
        plan.set_root(limit);
        let error = match build(&plan, &ctx).await {
            Err(error) => error,
            Ok(_) => panic!("a read node must refuse"),
        };
        assert!(error.to_string().contains("is a read node"), "{error}");
        assert!(
            hooks.scans.lock().unwrap().is_empty(),
            "a refused parent must not open its build or probe scan"
        );
    }

    /// A read node keeps its refusal under the spec-driven build.
    #[tokio::test]
    async fn a_read_node_refuses_the_push_pipeline() {
        let mut plan = PhysicalPlan::new();
        let outer = plan.add(PhysicalNode::OuterReference {
            outer_var: "x".into(),
        });
        plan.set_root(outer);
        let error = match build(&plan, &context()).await {
            Err(error) => error,
            Ok(_) => panic!("a read node must refuse"),
        };
        assert!(error.to_string().contains("is a read node"), "{error}");
    }
}
