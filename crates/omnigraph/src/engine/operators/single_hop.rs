//! The single-hop `ExpandExec`: one vectorized walk per input batch, streamed
//! in batch-size slices. Multi-hop stays on the BFS breaker in `graph.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Gauge};
use futures::StreamExt;
use omnigraph_planner::should_switch_to_csr;

use super::expand::{ExpandStep, GraphEnv};
use super::expand_stream::{align_sources, output_rows};
use super::memory::WorkMemory;
use super::producer::{BatchSender, producer_stream};
use super::{Switch, external};
use crate::engine::graph::{
    CsrSource, EndpointColumns, ExpandStart, GraphIndexHandle, HopPolicy, decide_expand_start,
    endpoint_probes, intern, resolve_csr, scan_neighbor_map,
};
use crate::error::OmniError;
use crate::graph_index::{CsrIndex, TypeIndex};

pub(super) fn execute(
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    step: ExpandStep,
    env: Arc<GraphEnv>,
    ctx: Arc<TaskContext>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let switch = Switch::gauge(metrics);
    let mut work = WorkMemory::new(ctx, "ExpandExec")?;
    work.set_metrics(metrics.clone());
    work.metric("input_rows", 0);
    let memory = Arc::new(work);
    let declared = Arc::clone(&schema);
    Ok(producer_stream(
        schema,
        memory,
        Some(metrics),
        move |memory, sender| async move {
            run(input, &declared, &step, &env, &switch, &memory, &sender).await
        },
    ))
}

async fn run(
    mut input: SendableRecordBatchStream,
    schema: &SchemaRef,
    step: &ExpandStep,
    env: &GraphEnv,
    switch: &Gauge,
    memory: &Arc<WorkMemory>,
    sender: &BatchSender,
) -> Result<()> {
    let catalog = &env.catalog;
    let edge_def = catalog.edge_types.get(&step.edge_type).ok_or_else(|| {
        external(OmniError::manifest(format!(
            "unknown edge type '{}'",
            step.edge_type
        )))
    })?;
    let src_column = format!("{}.{}", step.src, catalog.system_columns.id);
    let probes = endpoint_probes(step.direction, catalog.system_columns);
    let walk_memory = memory.child("expand walk")?;
    let first = loop {
        match input.next().await.transpose()? {
            Some(batch) if batch.num_rows() > 0 => break batch,
            Some(_) => continue,
            None => return Ok(()),
        }
    };
    let start = decide_expand_start(
        Some(first.num_rows()),
        &env.graph_index,
        &env.snapshot,
        catalog,
        step,
        memory,
    )
    .await
    .map_err(external)?;
    let mut source = match start {
        ExpandStart::Csr => {
            Switch::Csr.record(switch);
            Source::csr(env, edge_def, step).await?
        }
        ExpandStart::Indexed {
            edge_ds,
            hop_policy,
        } => {
            Switch::IndexedScan.record(switch);
            Source::Indexed {
                edge_ds,
                hop_policy,
            }
        }
    };
    let batch_size = output_rows(memory);
    walk_memory.entries::<u32>(batch_size * 2)?;
    let mut walk = Walk::new(batch_size);
    let mut observed = 0usize;
    let mut input = futures::stream::iter([Ok(first)]).chain(input);
    while let Some(batch) = input.next().await {
        let batch = batch?;
        memory.metric("input_rows", batch.num_rows());
        if batch.num_rows() == 0 {
            continue;
        }
        observed = observed.saturating_add(batch.num_rows());
        if source.switches(observed, &env.graph_index) {
            crate::instrumentation::record_traversal_mid_switch();
            crate::instrumentation::record_expand_path(false);
            memory.metric("expand_csr", 1);
            tracing::debug!(
                target: "omnigraph::traverse",
                edge = %step.edge_type,
                frontier = observed,
                mode = "csr",
                reason = "frontier outgrew the indexed path",
                "expand mode switched between input batches",
            );
            Switch::Csr.record(switch);
            source = Source::csr(env, edge_def, step).await?;
        }
        let input_memory = memory.child("expand input batch")?;
        input_memory.hold(&batch)?;
        let src_ids = batch
            .column_by_name(&src_column)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("expand input has no Utf8 '{src_column}'"))
            })?;
        let hop_memory = memory.child("expand hop")?;
        hop_memory.entries::<usize>(batch.num_rows())?;
        let row_bytes = (0..batch.num_rows())
            .map(|row| memory.slice_bytes(&batch, row, 1))
            .collect::<Result<Vec<_>>>()?;
        let (src_dense, ids, neighbours) = source
            .prepare(src_ids, &probes, &hop_memory, &walk_memory, &mut walk)
            .await?;
        memory.checkpoint()?;
        let mut cursor = Cursor::default();
        loop {
            let stop = walk.fill(
                &src_dense,
                &neighbours,
                &mut cursor,
                &row_bytes,
                ids.as_slice(),
                memory.batch_bytes(),
            );
            walk.flush(&batch, ids.as_slice(), memory, sender, schema)
                .await?;
            match stop {
                Stop::Full => {}
                Stop::Exhausted => break,
            }
        }
    }
    Ok(())
}

/// Neighbours come from the shared CSR dictionary or batch-local edge scans.
enum Source<'g> {
    Csr {
        csr: CsrSource<'g>,
    },
    Indexed {
        edge_ds: Box<lance::Dataset>,
        hop_policy: HopPolicy,
    },
}

/// One input batch's neighbour lists: CSR slices, or the batch's own scan.
enum Neighbours<'g> {
    Csr {
        adj: &'g CsrIndex,
        adj_rev: Option<&'g CsrIndex>,
    },
    Scanned(HashMap<u32, Vec<u32>>),
}

impl Neighbours<'_> {
    fn of(&self, node: u32) -> (&[u32], &[u32]) {
        match self {
            Self::Csr { adj, adj_rev } => (
                adj.neighbors(node),
                adj_rev.map(|rev| rev.neighbors(node)).unwrap_or(&[]),
            ),
            Self::Scanned(map) => (map.get(&node).map(Vec::as_slice).unwrap_or(&[]), &[]),
        }
    }
}

impl<'g> Source<'g> {
    async fn csr(
        env: &'g GraphEnv,
        edge_def: &omnigraph_compiler::catalog::EdgeType,
        step: &ExpandStep,
    ) -> Result<Self> {
        let gi = env
            .graph_index
            .get()
            .await
            .map_err(external)?
            .ok_or_else(|| {
                external(OmniError::manifest(
                    "graph index required for CSR traversal".to_string(),
                ))
            })?;
        let csr = resolve_csr(gi, edge_def, &step.edge_type, step.direction).map_err(external)?;
        Ok(Self::Csr { csr })
    }

    fn switches(&self, observed: usize, graph_index: &GraphIndexHandle) -> bool {
        let Self::Indexed { hop_policy, .. } = self else {
            return false;
        };
        match hop_policy {
            HopPolicy::Off => false,
            HopPolicy::Full(inputs) => should_switch_to_csr(
                observed as u64,
                observed as u64,
                1,
                graph_index.is_built(),
                inputs,
            ),
        }
    }

    /// The batch's sources as dense ids (`None`: unknown to the index, no
    /// edges), the id table its destinations are taken from, and its
    /// neighbour lists.
    async fn prepare(
        &self,
        src_ids: &StringArray,
        probes: &[EndpointColumns],
        hop_memory: &WorkMemory,
        walk_memory: &WorkMemory,
        walk: &mut Walk,
    ) -> Result<(Vec<Option<u32>>, DestinationIds<'g>, Neighbours<'g>)> {
        hop_memory.entries::<Option<u32>>(src_ids.len())?;
        match self {
            Self::Csr { csr } => {
                let src_dense = (0..src_ids.len())
                    .map(|row| csr.src_idx.to_dense(src_ids.value(row)))
                    .collect();
                walk.size_stamps(csr.dst_idx.len(), walk_memory)?;
                Ok((
                    src_dense,
                    DestinationIds::Borrowed(csr.dst_idx.ids()),
                    Neighbours::Csr {
                        adj: csr.adj,
                        adj_rev: csr.adj_rev,
                    },
                ))
            }
            Self::Indexed { edge_ds, .. } => {
                let mut interner = TypeIndex::new();
                let src_dense = (0..src_ids.len())
                    .map(|row| intern(&mut interner, src_ids.value(row), hop_memory).map(Some))
                    .collect::<crate::error::Result<Vec<_>>>()
                    .map_err(external)?;
                let keys = interner.ids().to_vec();
                hop_memory.entries::<String>(keys.len())?;
                hop_memory.string(keys.iter().map(String::len).sum())?;
                let mut map = HashMap::new();
                scan_neighbor_map(
                    edge_ds,
                    probes,
                    &keys,
                    &mut interner,
                    &mut map,
                    hop_memory,
                    hop_memory,
                )
                .await
                .map_err(external)?;
                walk.size_stamps(interner.len(), walk_memory)?;
                Ok((
                    src_dense,
                    DestinationIds::Owned(interner),
                    Neighbours::Scanned(map),
                ))
            }
        }
    }
}

enum DestinationIds<'g> {
    Borrowed(&'g [String]),
    Owned(TypeIndex),
}

impl DestinationIds<'_> {
    fn as_slice(&self) -> &[String] {
        match self {
            Self::Borrowed(ids) => ids,
            Self::Owned(index) => index.ids(),
        }
    }
}

/// The slice being built (`ordinals` into the input batch, `dst` dense) and
/// the dedup stamps: a destination stamped with the current source's
/// generation was already emitted for it (first occurrence wins, v1's order).
struct Walk {
    ordinals: Vec<u32>,
    dst: Vec<u32>,
    stamps: Vec<u32>,
    generation: u32,
    capacity: usize,
}

#[derive(Default)]
struct Cursor {
    row: usize,
    edge: usize,
}

enum Stop {
    Full,
    Exhausted,
}

impl Walk {
    fn new(capacity: usize) -> Self {
        Self {
            ordinals: Vec::with_capacity(capacity),
            dst: Vec::with_capacity(capacity),
            stamps: Vec::new(),
            generation: 0,
            capacity,
        }
    }

    fn size_stamps(&mut self, nodes: usize, memory: &WorkMemory) -> Result<()> {
        if nodes > self.stamps.len() {
            memory.grow((nodes - self.stamps.len()).saturating_mul(std::mem::size_of::<u32>()))?;
            self.stamps.reserve_exact(nodes - self.stamps.len());
            self.stamps.resize(nodes, 0);
        }
        Ok(())
    }

    fn next_generation(&mut self) {
        if self.generation == u32::MAX {
            self.stamps.fill(0);
            self.generation = 0;
        }
        self.generation += 1;
    }

    /// Resume a source fan-out until the row or byte budget is reached.
    fn fill(
        &mut self,
        src_dense: &[Option<u32>],
        neighbours: &Neighbours<'_>,
        cursor: &mut Cursor,
        row_bytes: &[usize],
        ids: &[String],
        byte_limit: usize,
    ) -> Stop {
        let mut bytes = 0usize;
        while cursor.row < src_dense.len() {
            let Some(node) = src_dense[cursor.row] else {
                cursor.row += 1;
                continue;
            };
            if cursor.edge == 0 {
                self.next_generation();
            }
            let (fwd, rev) = neighbours.of(node);
            let ordinal = cursor.row as u32;
            for &neighbour in fwd.iter().chain(rev).skip(cursor.edge) {
                let stamp = &mut self.stamps[neighbour as usize];
                if *stamp == self.generation {
                    cursor.edge += 1;
                    continue;
                }
                let row_size = row_bytes[cursor.row]
                    .saturating_add(ids[neighbour as usize].len())
                    .saturating_add(16);
                if !self.ordinals.is_empty() && bytes.saturating_add(row_size) > byte_limit {
                    return Stop::Full;
                }
                cursor.edge += 1;
                bytes = bytes.saturating_add(row_size);
                *stamp = self.generation;
                self.ordinals.push(ordinal);
                self.dst.push(neighbour);
                if self.ordinals.len() >= self.capacity {
                    return Stop::Full;
                }
            }
            cursor.row += 1;
            cursor.edge = 0;
        }
        Stop::Exhausted
    }

    async fn flush(
        &mut self,
        batch: &RecordBatch,
        ids: &[String],
        memory: &Arc<WorkMemory>,
        sender: &BatchSender,
        schema: &SchemaRef,
    ) -> Result<()> {
        if self.ordinals.is_empty() {
            return Ok(());
        }
        let work = Arc::new(memory.child("expand output chunk")?);
        let rows = self.ordinals.len();
        work.entries::<u32>(self.ordinals.capacity() + self.dst.capacity())?;
        let ordinals = UInt32Array::from(std::mem::replace(
            &mut self.ordinals,
            Vec::with_capacity(self.capacity),
        ));
        let dst = UInt32Array::from(std::mem::replace(
            &mut self.dst,
            Vec::with_capacity(self.capacity),
        ));
        let bytes = dst
            .values()
            .iter()
            .map(|&dense| ids[dense as usize].len())
            .sum::<usize>();
        work.string(bytes)?;
        work.grow((rows + 1).saturating_mul(std::mem::size_of::<i32>()))?;
        let destination = Arc::new(StringArray::from_iter_values(
            dst.values()
                .iter()
                .map(|&dense| ids[dense as usize].as_str()),
        ));
        let output = align_sources(batch, &ordinals, destination, &[], schema, &work)?;
        sender.send_bounded(output, work).await
    }
}
