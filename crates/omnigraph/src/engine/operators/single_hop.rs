//! Legacy named single-hop execution walks each input batch in bounded slices.
//! Budgeted windows and legacy multi-hop enter shared BFS through `expand_stream`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Gauge};
use futures::future::BoxFuture;
use omnigraph_planner::should_switch_to_csr;

use super::expand::{ExpandStep, GraphEnv, NamedExpand};
use super::expand_stream::{align_sources, output_rows};
use super::memory::WorkMemory;
use super::shape::{BatchSender, Flow, Streamed};
use super::{Switch, external};
use crate::engine::graph::{
    CsrSource, EndpointColumns, ExpandStart, GraphIndexHandle, HopPolicy, decide_expand_start,
    endpoint_probes, intern, resolve_csr, scan_neighbor_map,
};
use crate::error::OmniError;
use crate::graph_index::{CsrIndex, TypeIndex};

/// The per-batch body of a named one hop: the first non-empty input batch
/// decides the start mode, then every batch is walked in bounded slices,
/// switching to the CSR between batches once the frontier outgrows the
/// indexed path.
pub(super) struct SingleHop {
    schema: SchemaRef,
    step: ExpandStep,
    named: NamedExpand,
    env: Arc<GraphEnv>,
    switch: Gauge,
    started: Option<Started>,
}

/// What the first batch decided and what every batch after it reuses.
struct Started {
    mode: Mode,
    src_column: String,
    probes: Vec<EndpointColumns>,
    walk: Walk,
    walk_memory: WorkMemory,
    observed: usize,
}

/// Where the neighbours come from, owned across batches: the shared CSR
/// dictionary, resolved again per batch from the index handle, or the edge
/// dataset the batch's own scan reads.
enum Mode {
    Csr,
    Indexed {
        edge_ds: Box<lance::Dataset>,
        hop_policy: HopPolicy,
    },
}

impl Mode {
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
}

impl SingleHop {
    pub(super) fn new(
        schema: SchemaRef,
        step: ExpandStep,
        named: NamedExpand,
        env: Arc<GraphEnv>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            schema,
            step,
            named,
            env,
            switch: Switch::gauge(metrics),
            started: None,
        }
    }

    async fn start(&self, first: &RecordBatch, memory: &Arc<WorkMemory>) -> Result<Started> {
        let catalog = &self.env.catalog;
        let member = &self.named.member;
        let src_column = format!("{}.{}", self.step.src, catalog.system_columns.id);
        let probes = endpoint_probes(member.direction, catalog.system_columns);
        let walk_memory = memory.child("expand walk")?;
        let start = decide_expand_start(
            Some(first.num_rows()),
            &self.env.graph_index,
            &self.env.snapshot,
            catalog,
            &self.step,
            &self.named,
            memory,
        )
        .await
        .map_err(external)?;
        let mode = match start {
            ExpandStart::Csr => {
                Switch::Csr.record(&self.switch);
                Mode::Csr
            }
            ExpandStart::Indexed {
                edge_ds,
                hop_policy,
            } => {
                Switch::IndexedScan.record(&self.switch);
                Mode::Indexed {
                    edge_ds,
                    hop_policy,
                }
            }
        };
        let batch_size = output_rows(memory);
        walk_memory.entries::<u32>(batch_size * 2)?;
        Ok(Started {
            mode,
            src_column,
            probes,
            walk: Walk::new(batch_size),
            walk_memory,
            observed: 0,
        })
    }

    async fn hop(
        &mut self,
        batch: RecordBatch,
        memory: &Arc<WorkMemory>,
        sender: &BatchSender,
    ) -> Result<()> {
        memory.metric("input_rows", batch.num_rows());
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.started.is_none() {
            self.started = Some(self.start(&batch, memory).await?);
        }
        let Self {
            schema,
            env,
            named,
            switch,
            started,
            ..
        } = self;
        let started = started.as_mut().ok_or_else(|| {
            DataFusionError::Internal("single hop walked before its start was decided".into())
        })?;
        let member = &named.member;
        let edge_def = env
            .catalog
            .edge_types
            .get(&member.edge_type)
            .ok_or_else(|| {
                external(OmniError::manifest(format!(
                    "unknown edge type '{}'",
                    member.edge_type
                )))
            })?;
        started.observed = started.observed.saturating_add(batch.num_rows());
        if started.mode.switches(started.observed, &env.graph_index) {
            crate::instrumentation::record_traversal_mid_switch();
            crate::instrumentation::record_expand_path(false);
            memory.metric("expand_csr", 1);
            tracing::debug!(
                target: "omnigraph::traverse",
                edge = %member.edge_type,
                frontier = started.observed,
                mode = "csr",
                reason = "frontier outgrew the indexed path",
                "expand mode switched between input batches",
            );
            Switch::Csr.record(switch);
            started.mode = Mode::Csr;
        }
        let input_memory = memory.child("expand input batch")?;
        input_memory.hold(&batch)?;
        let src_ids = batch
            .column_by_name(&started.src_column)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "expand input has no Utf8 '{}'",
                    started.src_column
                ))
            })?;
        let hop_memory = memory.child("expand hop")?;
        hop_memory.entries::<usize>(batch.num_rows())?;
        let row_bytes = (0..batch.num_rows())
            .map(|row| memory.slice_bytes(&batch, row, 1))
            .collect::<Result<Vec<_>>>()?;
        let source = match &started.mode {
            Mode::Csr => {
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
                Source::Csr {
                    csr: resolve_csr(gi, edge_def, &member.edge_type, member.direction)
                        .map_err(external)?,
                }
            }
            Mode::Indexed { edge_ds, .. } => Source::Indexed { edge_ds },
        };
        let (src_dense, ids, neighbours) = source
            .prepare(
                src_ids,
                &started.probes,
                &hop_memory,
                &started.walk_memory,
                &mut started.walk,
            )
            .await?;
        memory.checkpoint()?;
        let mut cursor = Cursor::default();
        loop {
            let stop = started.walk.fill(
                &src_dense,
                &neighbours,
                &mut cursor,
                &row_bytes,
                ids.as_slice(),
                memory.batch_bytes(),
            );
            started
                .walk
                .flush(&batch, ids.as_slice(), memory, sender, schema)
                .await?;
            match stop {
                Stop::Full => {}
                Stop::Exhausted => break,
            }
        }
        Ok(())
    }
}

impl Streamed for SingleHop {
    fn batch<'a>(
        &'a mut self,
        batch: RecordBatch,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, Result<Flow>> {
        Box::pin(async move {
            self.hop(batch, memory, sender).await?;
            Ok(Flow::Continue)
        })
    }
}

/// One batch's neighbour source: CSR slices borrowed from the index, or the
/// edge dataset a batch-local scan reads.
enum Source<'g> {
    Csr { csr: CsrSource<'g> },
    Indexed { edge_ds: &'g lance::Dataset },
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
            Self::Indexed { edge_ds } => {
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
