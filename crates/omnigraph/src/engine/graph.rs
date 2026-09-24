//! v2's copy of v1's graph operators: the expand family with its mode
//! choice and ID emission, the anti-join arms, the RRF fusion, and the
//! handles (`GraphIndexHandle`, `EmbeddingResolver`) the doors build for
//! the v2 route (phase 4). Copied, never referenced.

use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use omnigraph_planner::{ExpandCostInputs, ExpandMode, choose_expand_mode, should_switch_to_csr};

use datafusion::physical_plan::metrics::Gauge;

use super::operators::memory::WorkMemory;
use super::operators::{ExpandStep, RowCountPredicate, Switch};
use super::*;

/// Bundles the per-handle embedding client cell with the optional injected
/// config (RFC-012 Phase 5) so the lazy init uses the injected config when
/// present, else `EmbeddingClient::from_env()`. Threaded through the query path
/// in place of the bare cell, preserving laziness (a graph that never embeds
/// builds no client and needs no key).
pub(crate) enum EmbeddingResolver<'a> {
    Client {
        cell: &'a tokio::sync::OnceCell<EmbeddingClient>,
        config: Option<&'a crate::embedding::EmbeddingConfig>,
    },
    Explain {
        requested: std::sync::atomic::AtomicBool,
    },
}

impl<'a> EmbeddingResolver<'a> {
    pub(crate) fn new(
        cell: &'a tokio::sync::OnceCell<EmbeddingClient>,
        config: Option<&'a crate::embedding::EmbeddingConfig>,
    ) -> Self {
        Self::Client { cell, config }
    }

    pub(super) fn explain() -> Self {
        Self::Explain {
            requested: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(super) fn was_requested(&self) -> bool {
        match self {
            Self::Client { .. } => false,
            Self::Explain { requested } => requested.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub(super) async fn resolve(&self) -> Result<&EmbeddingClient> {
        let (cell, config) = match self {
            Self::Client { cell, config } => (cell, config.cloned()),
            Self::Explain { requested } => {
                requested.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(OmniError::manifest_internal(
                    "explain cannot resolve an embedding client",
                ));
            }
        };
        cell.get_or_try_init(|| async move {
            match config {
                Some(cfg) => EmbeddingClient::new(cfg),
                None => EmbeddingClient::from_env(),
            }
        })
        .await
    }
}

/// Fuse arms sorted by search score and identity; BM25 arms must be uncapped.
/// Rank each entity once and retain each winning entity's downstream rows.
pub(super) fn fuse_arms(
    primary_batch: &RecordBatch,
    secondary_batch: &RecordBatch,
    rrf: &RrfMode,
    id_col_name: &str,
    memory: &WorkMemory,
) -> Result<RecordBatch> {
    let work = memory
        .child("fuse_arms")
        .map_err(|error| memory.error(error))?;
    let memory = &work;
    memory.check().map_err(|error| memory.error(error))?;
    let rows = primary_batch
        .num_rows()
        .saturating_add(secondary_batch.num_rows());
    memory
        .entries::<(String, usize)>(rows.saturating_mul(5))
        .map_err(|error| memory.error(error))?;
    let id_bytes = [primary_batch, secondary_batch]
        .into_iter()
        .filter_map(|batch| batch.column_by_name(id_col_name))
        .map(|column| column.get_array_memory_size())
        .sum::<usize>();
    memory
        .string(id_bytes.saturating_mul(6))
        .map_err(|error| memory.error(error))?;
    let primary_ids = extract_id_column_by_name(primary_batch, id_col_name)?;
    let secondary_ids = extract_id_column_by_name(secondary_batch, id_col_name)?;

    let mut primary_rank: HashMap<String, usize> = HashMap::new();
    let mut primary_unique: Vec<String> = Vec::new();
    for id in &primary_ids {
        memory.check().map_err(|error| memory.error(error))?;
        if !primary_rank.contains_key(id) {
            primary_rank.insert(id.clone(), primary_unique.len());
            primary_unique.push(id.clone());
        }
    }
    let mut secondary_rank: HashMap<String, usize> = HashMap::new();
    let mut secondary_unique: Vec<String> = Vec::new();
    for id in &secondary_ids {
        memory.check().map_err(|error| memory.error(error))?;
        if !secondary_rank.contains_key(id) {
            secondary_rank.insert(id.clone(), secondary_unique.len());
            secondary_unique.push(id.clone());
        }
    }

    let mut all_ids: Vec<String> = primary_unique;
    for id in &secondary_unique {
        if !primary_rank.contains_key(id) {
            all_ids.push(id.clone());
        }
    }

    let k = rrf.k as f64;
    let mut scored: Vec<(String, f64)> = all_ids
        .iter()
        .map(|id| {
            let p = primary_rank
                .get(id)
                .map(|&r| 1.0 / (k + r as f64 + 1.0))
                .unwrap_or(0.0);
            let s = secondary_rank
                .get(id)
                .map(|&r| 1.0 / (k + r as f64 + 1.0))
                .unwrap_or(0.0);
            (id.clone(), p + s)
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(rrf.limit);

    let winning_ids: Vec<String> = scored.iter().map(|(id, _)| id.clone()).collect();

    let mut primary_rows: HashMap<String, Vec<u32>> = HashMap::new();
    for (i, id) in primary_ids.iter().enumerate() {
        primary_rows.entry(id.clone()).or_default().push(i as u32);
    }
    let mut secondary_rows: HashMap<String, Vec<u32>> = HashMap::new();
    for (i, id) in secondary_ids.iter().enumerate() {
        secondary_rows.entry(id.clone()).or_default().push(i as u32);
    }

    build_fused_batch(
        &winning_ids,
        primary_batch,
        &primary_rows,
        secondary_batch,
        &secondary_rows,
        memory,
    )
}

pub(super) fn extract_id_column_by_name(
    batch: &RecordBatch,
    col_name: &str,
) -> Result<Vec<String>> {
    let col = batch.column_by_name(col_name).ok_or_else(|| {
        OmniError::manifest(format!("batch missing '{}' column for RRF", col_name))
    })?;
    let ids = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{}' column is not Utf8", col_name)))?;
    Ok((0..ids.len()).map(|i| ids.value(i).to_string()).collect())
}

/// Gather all rows for `ordered_ids` in entity order, preferring the primary
/// arm for shared entities and preserving each entity's row order.
pub(super) fn build_fused_batch(
    ordered_ids: &[String],
    primary_batch: &RecordBatch,
    primary_rows: &HashMap<String, Vec<u32>>,
    secondary_batch: &RecordBatch,
    secondary_rows: &HashMap<String, Vec<u32>>,
    memory: &WorkMemory,
) -> Result<RecordBatch> {
    let work = memory
        .child("build_fused_batch")
        .map_err(|error| memory.error(error))?;
    let memory = &work;
    memory.check().map_err(|error| memory.error(error))?;
    if ordered_ids.is_empty() {
        return Ok(RecordBatch::new_empty(primary_batch.schema()));
    }

    memory
        .entries::<RecordBatch>(
            primary_batch
                .num_rows()
                .saturating_add(secondary_batch.num_rows()),
        )
        .map_err(|error| memory.error(error))?;
    let mut row_slices: Vec<RecordBatch> = Vec::with_capacity(ordered_ids.len());
    for id in ordered_ids {
        memory.check().map_err(|error| memory.error(error))?;
        if let Some(rows) = primary_rows.get(id) {
            row_slices.push(
                memory
                    .take(primary_batch, &UInt32Array::from(rows.clone()))
                    .map_err(|error| memory.error(error))?,
            );
        } else if let Some(rows) = secondary_rows.get(id) {
            row_slices.push(
                memory
                    .take(secondary_batch, &UInt32Array::from(rows.clone()))
                    .map_err(|error| memory.error(error))?,
            );
        }
    }

    if row_slices.is_empty() {
        return Ok(RecordBatch::new_empty(primary_batch.schema()));
    }

    let schema = row_slices[0].schema();
    memory
        .concat(&schema, &row_slices)
        .map_err(|error| memory.error(error))
}

/// Lazily provides the in-memory CSR graph index, building it on first use and
/// memoizing for the rest of the query. Indexed-mode Expand never asks for it,
/// so a query that is entirely index-served and has no AntiJoin never pays the
/// O(|E|) CSR build (the whole point of the indexed path). The `Cached` builder
/// also reuses the cross-query `RuntimeCache` entry; `Direct` builds against an
/// arbitrary snapshot (time-travel reads); `None` is for queries with no
/// traversal at all.
pub struct GraphIndexHandle {
    cell: tokio::sync::OnceCell<Option<Arc<GraphIndex>>>,
    builder: GraphIndexBuilder,
}

/// Owned by the handle: the lowered plan's graph operators hold the handle
/// for the query, so nothing in it borrows from the doors.
enum GraphIndexBuilder {
    None,
    Cached(
        Arc<Omnigraph>,
        crate::db::ResolvedTarget,
        HashMap<String, (String, String)>,
        SystemColumns,
    ),
    Direct(Snapshot, HashMap<String, (String, String)>, SystemColumns),
}

impl GraphIndexHandle {
    pub(crate) fn none() -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::None,
        }
    }

    pub(crate) fn cached(
        db: Arc<Omnigraph>,
        resolved: crate::db::ResolvedTarget,
        edge_types: HashMap<String, (String, String)>,
        system_columns: SystemColumns,
    ) -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::Cached(db, resolved, edge_types, system_columns),
        }
    }

    pub(crate) fn direct(
        snapshot: Snapshot,
        edge_types: HashMap<String, (String, String)>,
        system_columns: SystemColumns,
    ) -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::Direct(snapshot, edge_types, system_columns),
        }
    }

    /// The CSR index, built on first call. `None` only when the query needs no
    /// traversal (the `None` builder).
    pub(super) async fn get(&self) -> Result<Option<&GraphIndex>> {
        let built = self
            .cell
            .get_or_try_init(|| async {
                match &self.builder {
                    GraphIndexBuilder::None => Ok::<Option<Arc<GraphIndex>>, OmniError>(None),
                    GraphIndexBuilder::Cached(db, resolved, edge_types, system_columns) => {
                        Ok(Some(
                            db.graph_index_for_resolved(resolved, edge_types, *system_columns)
                                .await?,
                        ))
                    }
                    GraphIndexBuilder::Direct(snapshot, edge_types, system_columns) => {
                        Ok(Some(Arc::new(
                            GraphIndex::load_or_build(snapshot, edge_types, None, *system_columns)
                                .await?,
                        )))
                    }
                }
            })
            .await?;
        Ok(built.as_deref())
    }

    /// Whether the in-memory CSR is already materialized for this query (a prior
    /// Expand or bulk AntiJoin realized it), so reusing it is ~free. Lets the
    /// cost chooser prefer the warm CSR over per-hop indexed scans.
    pub(super) fn is_built(&self) -> bool {
        matches!(self.cell.get(), Some(Some(_)))
    }
}

/// The planner's coverage value for the runtime correction. A failed coverage
/// probe is `Degraded` (conservative: the indexed path is not favored when
/// the BTREE cannot be confirmed to serve the scan).
pub(super) fn coverage_for_decision(
    coverage: &Result<crate::table_store::IndexCoverage>,
) -> omnigraph_planner::IndexCoverage {
    match coverage {
        Ok(crate::table_store::IndexCoverage::Indexed) => omnigraph_planner::IndexCoverage::Indexed,
        Ok(crate::table_store::IndexCoverage::Degraded { .. }) | Err(_) => {
            omnigraph_planner::IndexCoverage::Degraded
        }
    }
}

/// Surface the C6 silent scalar-index fallback (commit `5a7ab6d`): warn when the
/// per-hop `key_col IN (...)` won't route through the BTREE. Detection-only;
/// never fails the query. Behavior-identical to the inline check it replaced.
pub(super) fn warn_on_degraded_coverage(
    coverage: &Result<crate::table_store::IndexCoverage>,
    key_col: &str,
    edge_type: &str,
) {
    match coverage {
        Ok(crate::table_store::IndexCoverage::Degraded { reason }) => tracing::warn!(
            target: "omnigraph::traverse",
            edge = %edge_type,
            key_col = key_col,
            reason = %reason,
            "indexed traversal falls back to a full edge scan (results correct, perf degraded)"
        ),
        Ok(crate::table_store::IndexCoverage::Indexed) => {}
        Err(e) => tracing::debug!(
            target: "omnigraph::traverse",
            error = %e,
            "index-coverage check failed; proceeding with traversal"
        ),
    }
}

/// Endpoint roles for one oriented edge scan.
#[derive(Clone, Copy)]
pub(super) struct EndpointColumns {
    pub(super) key: &'static str,
    pub(super) opposite: &'static str,
}

/// Both starts with the outgoing orientation; `endpoint_probes` adds incoming.
pub(super) fn endpoint_columns(
    direction: Direction,
    system_columns: SystemColumns,
) -> EndpointColumns {
    match direction {
        Direction::Out | Direction::Both => EndpointColumns {
            key: system_columns.src,
            opposite: system_columns.dst,
        },
        Direction::In => EndpointColumns {
            key: system_columns.dst,
            opposite: system_columns.src,
        },
    }
}

/// The pessimistic combination of two coverage probes: Degraded dominates
/// (an undirected traversal pays whichever of its two columns is worse).
pub(super) fn worse_coverage(
    a: crate::table_store::IndexCoverage,
    b: crate::table_store::IndexCoverage,
) -> crate::table_store::IndexCoverage {
    use crate::table_store::IndexCoverage;
    match (a, b) {
        (IndexCoverage::Indexed, IndexCoverage::Indexed) => IndexCoverage::Indexed,
        (IndexCoverage::Degraded { reason }, _) | (_, IndexCoverage::Degraded { reason }) => {
            IndexCoverage::Degraded { reason }
        }
    }
}

/// One orientation for Out/In, both for an undirected traversal.
pub(super) fn endpoint_probes(
    direction: Direction,
    system_columns: SystemColumns,
) -> Vec<EndpointColumns> {
    let mut probes = vec![endpoint_columns(direction, system_columns)];
    if direction == Direction::Both {
        probes.push(endpoint_columns(Direction::In, system_columns));
    }
    probes
}

pub(super) struct ExpandedPairs {
    pub(super) source_rows: Vec<u32>,
    pub(super) destination_ids: Vec<String>,
    _memory: WorkMemory,
}

/// Run the topology path the plan recorded on `step` and retain its emitted
/// ID pairs: the multi-hop breaker (a single unbound hop streams through
/// `operators::single_hop`). The start is `decide_expand_start`'s.
pub(super) async fn execute_expand(
    wide: &RecordBatch,
    graph_index: &GraphIndexHandle,
    snapshot: &Snapshot,
    catalog: &Catalog,
    step: &ExpandStep,
    switch: &Gauge,
    memory: &WorkMemory,
) -> Result<ExpandedPairs> {
    let work = memory
        .child("execute_expand")
        .map_err(|error| memory.error(error))?;
    let memory = &work;
    memory.check().map_err(|error| memory.error(error))?;
    let start = decide_expand_start(
        Some(wide.num_rows()),
        graph_index,
        snapshot,
        catalog,
        step,
        memory,
    )
    .await?;
    let (start_indexed, hop_policy) = match start {
        ExpandStart::Csr => {
            Switch::Csr.record(switch);
            (None, HopPolicy::Off)
        }
        ExpandStart::Indexed {
            edge_ds,
            hop_policy,
        } => {
            Switch::IndexedScan.record(switch);
            (Some(*edge_ds), hop_policy)
        }
    };
    execute_expand_bfs(
        wide,
        graph_index,
        catalog,
        step,
        start_indexed,
        hop_policy,
        switch,
        memory,
    )
    .await
}

/// Where an Expand starts: the CSR, or the BTREE edge scans with the policy
/// that may still switch it to the CSR mid-flight.
pub(super) enum ExpandStart {
    Csr,
    Indexed {
        edge_ds: Box<Dataset>,
        hop_policy: HopPolicy,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ModeOrigin {
    Pinned,
    Costed(ExpandCostInputs),
    Uncosted,
}

/// The start the plan recorded on `step`, with the two runtime corrections:
/// a probed index coverage (or a CSR warmed by an earlier operator) re-runs
/// the cost model before an indexed start, and the indexed start carries the
/// per-hop policy that switches mid-flight (issue #533). `frontier_rows` is
/// the breaker's retained frontier; the streaming single hop has none yet.
pub(super) async fn decide_expand_start(
    frontier_rows: Option<usize>,
    graph_index: &GraphIndexHandle,
    snapshot: &Snapshot,
    catalog: &Catalog,
    step: &ExpandStep,
    memory: &WorkMemory,
) -> Result<ExpandStart> {
    let effective_max_hops = step.max_hops;
    let key_col = endpoint_columns(step.direction, catalog.system_columns).key;
    let edge_table_key = format!("edge:{}", step.edge_type);

    let observed_indexed = match (&step.origin, frontier_rows, step.mode) {
        (ModeOrigin::Costed(inputs), Some(observed), ExpandMode::Csr)
            if (observed as u64) < inputs.frontier_rows =>
        {
            let mut inputs = inputs.clone();
            inputs.frontier_rows = observed as u64;
            inputs.csr_cached = inputs.csr_cached || graph_index.is_built();
            choose_expand_mode(&inputs) == ExpandMode::IndexedScan
        }
        _ => false,
    };

    if step.mode == ExpandMode::Csr && !observed_indexed {
        tracing::debug!(
            target: "omnigraph::traverse",
            edge = %step.edge_type,
            frontier = ?frontier_rows,
            estimate = ?step.frontier_estimate,
            hops = effective_max_hops,
            mode = "csr",
            "expand mode recorded on the plan",
        );
        crate::instrumentation::record_expand_path(false);
        memory.metric("expand_csr", 1);
        return Ok(ExpandStart::Csr);
    }

    let edge_ds = snapshot.open_lance_dataset(&edge_table_key).await?;
    let mut coverage =
        crate::table_store::TableStore::key_column_index_coverage(&edge_ds, key_col).await;
    for orientation in endpoint_probes(step.direction, catalog.system_columns)
        .iter()
        .skip(1)
    {
        let extra =
            crate::table_store::TableStore::key_column_index_coverage(&edge_ds, orientation.key)
                .await;
        coverage = match (coverage, extra) {
            (Ok(a), Ok(b)) => Ok(worse_coverage(a, b)),
            (Err(e), _) | (_, Err(e)) => Err(e),
        };
    }

    let corrected = match &step.origin {
        ModeOrigin::Costed(inputs) => {
            let mut inputs = inputs.clone();
            if let Some(observed) = frontier_rows {
                inputs.frontier_rows = inputs.frontier_rows.min(observed as u64);
            }
            inputs.coverage = coverage_for_decision(&coverage);
            inputs.csr_cached = inputs.csr_cached || graph_index.is_built();
            Some(inputs)
        }
        ModeOrigin::Pinned | ModeOrigin::Uncosted => None,
    };
    if corrected
        .as_ref()
        .is_some_and(|inputs| choose_expand_mode(inputs) == ExpandMode::Csr)
    {
        tracing::debug!(
            target: "omnigraph::traverse",
            edge = %step.edge_type,
            frontier = ?frontier_rows,
            estimate = ?step.frontier_estimate,
            hops = effective_max_hops,
            mode = "csr",
            reason = "index coverage degraded or csr warm",
            "expand mode corrected at run time",
        );
        crate::instrumentation::record_expand_path(false);
        memory.metric("expand_csr", 1);
        return Ok(ExpandStart::Csr);
    }

    tracing::debug!(
        target: "omnigraph::traverse",
        edge = %step.edge_type,
        frontier = ?frontier_rows,
        estimate = ?step.frontier_estimate,
        hops = effective_max_hops,
        mode = "indexed",
        "expand mode recorded on the plan",
    );
    crate::instrumentation::record_expand_path(true);
    memory.metric("expand_indexed", 1);
    warn_on_degraded_coverage(&coverage, key_col, &step.edge_type);
    let hop_policy = match corrected {
        Some(inputs) => HopPolicy::Full(inputs),
        None if matches!(step.origin, ModeOrigin::Pinned) => HopPolicy::Off,
        None => {
            return Err(OmniError::manifest_internal(
                "indexed expand requires a pinned mode or cost inputs",
            ));
        }
    };
    Ok(ExpandStart::Indexed {
        edge_ds: Box::new(edge_ds),
        hop_policy,
    })
}

/// An edge type's property columns that a filtered scan may project, sorted for
/// determinism. Blobs are excluded: Lance rejects blob projection in a filtered
/// scan (node scans carry the same guard) and typecheck rejects the access.
pub(super) fn projectable_edge_property_columns(
    edge_def: &omnigraph_compiler::catalog::EdgeType,
) -> Vec<&str> {
    let mut cols: Vec<&str> = edge_def
        .properties
        .keys()
        .map(String::as_str)
        .filter(|c| !edge_def.blob_properties.contains(*c))
        .collect();
    cols.sort_unstable();
    cols
}

pub(super) fn bound_edge_pair_schema(
    catalog: &Catalog,
    edge_type: &str,
) -> Result<arrow_schema::SchemaRef> {
    let edge = catalog
        .edge_types
        .get(edge_type)
        .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{edge_type}'")))?;
    let mut fields = vec![
        Field::new("~expand_source_row", DataType::UInt32, false),
        Field::new("~expand_destination_id", DataType::Utf8, false),
    ];
    for name in [
        catalog.system_columns.id,
        catalog.system_columns.src,
        catalog.system_columns.dst,
    ]
    .into_iter()
    .chain(projectable_edge_property_columns(edge))
    {
        if fields.iter().any(|field| field.name() == name) {
            return Err(OmniError::manifest_internal(format!(
                "duplicate bound-edge pair column '{name}'"
            )));
        }
        fields.push(
            edge.arrow_schema
                .field_with_name(name)
                .cloned()
                .map_err(OmniError::arrow_internal)?,
        );
    }
    Ok(Arc::new(Schema::new(fields)))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn produce_bound_edge_pairs<F>(
    wide: &RecordBatch,
    snapshot: &Snapshot,
    catalog: &Catalog,
    src_var: &str,
    edge_type: &str,
    direction: Direction,
    memory: &Arc<WorkMemory>,
    mut emit: impl FnMut(RecordBatch, Arc<WorkMemory>) -> F + Send,
) -> Result<()>
where
    F: std::future::Future<Output = Result<()>> + Send,
{
    let schema = bound_edge_pair_schema(catalog, edge_type)?;
    if wide.num_rows() == 0 {
        return Ok(());
    }
    let source_memory = memory
        .child("bound expand frontier")
        .map_err(|error| memory.error(error))?;
    source_memory
        .hold(wide)
        .map_err(|error| memory.error(error))?;
    let src_name = format!("{src_var}.{}", catalog.system_columns.id);
    let src_ids = wide
        .column_by_name(&src_name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            OmniError::manifest(format!("wide batch has no Utf8 '{src_name}' column"))
        })?;
    source_memory
        .entries::<(&str, Vec<u32>, u32, String)>(src_ids.len())
        .map_err(|error| memory.error(error))?;
    source_memory
        .string(src_ids.value_data().len())
        .map_err(|error| memory.error(error))?;
    let mut rows_by_src: HashMap<&str, Vec<u32>> = HashMap::new();
    for row in 0..src_ids.len() {
        source_memory.check().map_err(|error| memory.error(error))?;
        let ordinal = u32::try_from(row)
            .map_err(|_| OmniError::manifest_internal("expand source ordinal exceeds UInt32"))?;
        rows_by_src
            .entry(src_ids.value(row))
            .or_default()
            .push(ordinal);
    }
    let keys: Vec<String> = rows_by_src.keys().map(|key| (*key).to_owned()).collect();
    let attach_columns: Vec<&str> = schema.fields()[2..]
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    source_memory
        .checkpoint()
        .map_err(|error| memory.error(error))?;
    let dataset = snapshot
        .open_lance_dataset(&format!("edge:{edge_type}"))
        .await?;
    let row_limit = memory.ctx.session_config().batch_size().max(1);
    let byte_limit = memory.batch_bytes();
    for (probe, orientation) in endpoint_probes(direction, catalog.system_columns)
        .into_iter()
        .enumerate()
    {
        let EndpointColumns {
            key: key_col,
            opposite: opposite_col,
        } = orientation;
        let scan_memory = memory
            .child("bound edge scan")
            .map_err(|error| memory.error(error))?;
        let mut stream =
            scan_edges_stream(&dataset, orientation, &attach_columns, &keys, &scan_memory).await?;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|error| memory.error(error))?;
            let input_memory = scan_memory
                .child("bound edge batch")
                .map_err(|error| memory.error(error))?;
            input_memory
                .hold(&batch)
                .map_err(|error| memory.error(error))?;
            let keys = batch
                .column_by_name(key_col)
                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    OmniError::manifest(format!("edge batch has no Utf8 '{key_col}'"))
                })?;
            let opposites = batch
                .column_by_name(opposite_col)
                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    OmniError::manifest(format!("edge batch has no Utf8 '{opposite_col}'"))
                })?;
            let indices = attach_columns
                .iter()
                .map(|name| batch.schema().index_of(name))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(OmniError::arrow_internal)?;
            let attach = batch.project(&indices).map_err(OmniError::arrow_internal)?;
            let data: Vec<_> = attach
                .columns()
                .iter()
                .map(|column| column.to_data())
                .collect();
            let mut source_rows = Vec::new();
            let mut edge_rows = Vec::new();
            let mut chunk_bytes = 0usize;
            let mut chunk_memory = Arc::new(
                memory
                    .child("bound edge pairs")
                    .map_err(|error| memory.error(error))?,
            );
            for row in 0..batch.num_rows() {
                if probe == 1 && keys.value(row) == opposites.value(row) {
                    continue;
                }
                let Some(wide_rows) = rows_by_src.get(keys.value(row)) else {
                    continue;
                };
                let edge_row = u32::try_from(row).map_err(|_| {
                    OmniError::manifest_internal("edge batch ordinal exceeds UInt32")
                })?;
                let row_bytes = data
                    .iter()
                    .try_fold(4usize, |size, column| {
                        column
                            .slice(row, 1)
                            .get_slice_memory_size()
                            .map(|bytes| size.saturating_add(bytes))
                    })
                    .map_err(OmniError::arrow_internal)?;
                for &source_row in wide_rows {
                    memory.check().map_err(|error| memory.error(error))?;
                    if !source_rows.is_empty()
                        && (source_rows.len() == row_limit
                            || chunk_bytes.saturating_add(row_bytes) > byte_limit)
                    {
                        let output = bound_edge_pair_batch(
                            &attach,
                            &schema,
                            opposite_col,
                            std::mem::take(&mut source_rows),
                            std::mem::take(&mut edge_rows),
                            &chunk_memory,
                        )?;
                        emit(output, chunk_memory).await?;
                        chunk_memory = Arc::new(
                            memory
                                .child("bound edge pairs")
                                .map_err(|error| memory.error(error))?,
                        );
                        chunk_bytes = 0;
                    }
                    chunk_memory
                        .entries::<(u32, u32)>(1)
                        .map_err(|error| memory.error(error))?;
                    source_rows.push(source_row);
                    edge_rows.push(edge_row);
                    chunk_bytes = chunk_bytes.saturating_add(row_bytes);
                }
            }
            if !source_rows.is_empty() {
                let output = bound_edge_pair_batch(
                    &attach,
                    &schema,
                    opposite_col,
                    source_rows,
                    edge_rows,
                    &chunk_memory,
                )?;
                emit(output, chunk_memory).await?;
            }
        }
    }
    Ok(())
}

fn bound_edge_pair_batch(
    attach: &RecordBatch,
    schema: &arrow_schema::SchemaRef,
    opposite_col: &str,
    source_rows: Vec<u32>,
    edge_rows: Vec<u32>,
    memory: &WorkMemory,
) -> Result<RecordBatch> {
    let attached = memory
        .take(attach, &UInt32Array::from(edge_rows))
        .map_err(|error| memory.error(error))?;
    let destination = attached.column_by_name(opposite_col).ok_or_else(|| {
        OmniError::manifest_internal(format!("edge attachment has no '{opposite_col}' column"))
    })?;
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(source_rows)),
        Arc::clone(destination),
    ];
    columns.extend(attached.columns().iter().cloned());
    let output =
        RecordBatch::try_new(Arc::clone(schema), columns).map_err(OmniError::arrow_internal)?;
    memory
        .output(&output)
        .map_err(|error| memory.error(error))?;
    Ok(output)
}

/// The CSR-side borrows of one edge type + direction: the adjacency the
/// direction reads (`adj_rev` is the CSC an undirected step adds), the
/// source and destination type dictionaries.
pub(super) struct CsrSource<'g> {
    pub(super) adj: &'g crate::graph_index::CsrIndex,
    pub(super) adj_rev: Option<&'g crate::graph_index::CsrIndex>,
    pub(super) src_idx: &'g crate::graph_index::TypeIndex,
    pub(super) dst_idx: &'g crate::graph_index::TypeIndex,
}

/// Resolve the adjacency and type dictionaries for a built graph index.
pub(super) fn resolve_csr<'g>(
    gi: &'g GraphIndex,
    edge_def: &omnigraph_compiler::catalog::EdgeType,
    edge_type: &str,
    direction: Direction,
) -> Result<CsrSource<'g>> {
    let (src_type_name, dst_type_name) = match direction {
        Direction::Out => (&edge_def.from_type, &edge_def.to_type),
        Direction::In => (&edge_def.to_type, &edge_def.from_type),
        Direction::Both => (&edge_def.from_type, &edge_def.from_type),
    };
    let src_idx = gi
        .type_index(src_type_name)
        .ok_or_else(|| OmniError::manifest(format!("no type index for '{}'", src_type_name)))?;
    let dst_idx = gi
        .type_index(dst_type_name)
        .ok_or_else(|| OmniError::manifest(format!("no type index for '{}'", dst_type_name)))?;
    let adj = match direction {
        Direction::Out | Direction::Both => gi.csr(edge_type),
        Direction::In => gi.csc(edge_type),
    }
    .ok_or_else(|| OmniError::manifest(format!("no adjacency index for edge '{}'", edge_type)))?;
    let adj_rev = match direction {
        Direction::Both => Some(gi.csc(edge_type).ok_or_else(|| {
            OmniError::manifest(format!("no adjacency index for edge '{}'", edge_type))
        })?),
        _ => None,
    };
    Ok(CsrSource {
        adj,
        adj_rev,
        src_idx,
        dst_idx,
    })
}

/// The one Expand BFS, shared by both execution strategies. Per hop it asks
/// the active source for neighbors — a batched `scan_edges_by_endpoint` per
/// orientation against the persisted src/dst BTREE (Indexed: cost scales with
/// the frontier, not |E|), or in-memory adjacency slices (Csr). Emission,
/// dedup and hop gating are identical either way, so
/// both strategies produce the same `(src_row, dst_id)` pairs by construction.
///
/// Multi-hop only advances for same-type edges; a cross-type traversal is
/// structurally single-hop. The Indexed source enforces that BEFORE scanning:
/// it interns every endpoint string into ONE dense id space, so a cross-type
/// id-string collision (a Person and a Company sharing an id) would otherwise
/// let hop 2 de-intern a destination id back to the colliding source-type id
/// and match its edges, emitting rows the CSR source never produces.
///
/// Issue #533 lives here: `hop_policy` is consulted at the top of every
/// indexed hop after the first with the OBSERVED union frontier, and a
/// traversal that has outgrown the indexed path swaps to CSR mid-flight,
/// translating frontier/visited/seen state through the id strings once. Every
/// emitted destination so far is an edge endpoint, so it exists in the CSR
/// dictionaries — nothing is lost in translation; a frontier or visited entry
/// absent from the CSR dictionary has no edges at all and is dropped as
/// unreachable.
///
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_expand_bfs(
    wide: &RecordBatch,
    graph_index: &GraphIndexHandle,
    catalog: &Catalog,
    step: &ExpandStep,
    start_indexed: Option<Dataset>,
    hop_policy: HopPolicy,
    side: &Gauge,
    memory: &WorkMemory,
) -> Result<ExpandedPairs> {
    let src_var = &step.src;
    let edge_type = &step.edge_type;
    let direction = step.direction;
    let min_hops = step.min_hops;
    let work = memory
        .child("execute_expand_bfs")
        .map_err(|error| memory.error(error))?;
    let memory = &work;
    memory.check().map_err(|error| memory.error(error))?;
    let src_id_col_name = format!("{}.{}", src_var, catalog.system_columns.id);
    let src_ids = wide
        .column_by_name(&src_id_col_name)
        .ok_or_else(|| {
            OmniError::manifest(format!("wide batch missing '{}' column", src_id_col_name))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{}' column is not Utf8", src_id_col_name)))?
        .clone();

    let edge_def = catalog
        .edge_types
        .get(edge_type)
        .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_type)))?;
    let same_type = edge_def.from_type == edge_def.to_type;
    let probes = endpoint_probes(direction, catalog.system_columns);

    let max = omnigraph_planner::cost::executed_hops(min_hops, Some(step.max_hops), same_type);

    let mut active = match start_indexed {
        Some(edge_ds) => ActiveExpandSource::Indexed(Box::new(IndexedExpandSource {
            edge_ds,
            interner: crate::graph_index::TypeIndex::new(),
            neighbor_map: HashMap::new(),
        })),
        None => {
            let gi = graph_index.get().await?.ok_or_else(|| {
                OmniError::manifest("graph index required for CSR traversal".to_string())
            })?;
            ActiveExpandSource::Csr(resolve_csr(gi, edge_def, edge_type, direction)?)
        }
    };

    let n = src_ids.len();
    memory
        .entries::<(Vec<u32>, HashSet<u32>, HashSet<u32>, u32)>(n)
        .map_err(|error| memory.error(error))?;
    let mut frontiers: Vec<Vec<u32>> = Vec::with_capacity(n);
    let mut visited: Vec<HashSet<u32>> = Vec::with_capacity(n);
    let mut seen_dst: Vec<HashSet<u32>> = Vec::with_capacity(n);
    for i in 0..n {
        let seed = match &mut active {
            ActiveExpandSource::Indexed(src) => {
                Some(intern(&mut src.interner, src_ids.value(i), memory)?)
            }
            ActiveExpandSource::Csr(CsrSource { src_idx, .. }) => {
                src_idx.to_dense(src_ids.value(i))
            }
        };
        let mut v = HashSet::new();
        if same_type {
            if let Some(s) = seed {
                v.insert(s);
            }
        }
        frontiers.push(seed.map(|s| vec![s]).unwrap_or_default());
        visited.push(v);
        seen_dst.push(HashSet::new());
    }

    memory.checkpoint().map_err(|error| memory.error(error))?;
    let emission_memory = memory
        .child("expand emissions")
        .map_err(|error| memory.error(error))?;
    let mut frontier_memory = memory
        .child("expand frontier")
        .map_err(|error| memory.error(error))?;
    let mut emitted_src: Vec<u32> = Vec::new();
    let mut emitted_dst: Vec<String> = Vec::new();
    let mut prev_union_len: usize = 0;

    for hop in 1..=max {
        memory.check().map_err(|error| memory.error(error))?;
        let hop_memory = memory
            .child("expand hop")
            .map_err(|error| memory.error(error))?;
        let next_memory = memory
            .child("expand frontier")
            .map_err(|error| memory.error(error))?;
        let frontier_count: usize = frontiers.iter().map(Vec::len).sum();
        hop_memory
            .entries::<u32>(frontier_count * 2)
            .map_err(|error| memory.error(error))?;
        let mut union_dense: Vec<u32> = Vec::new();
        {
            let mut seen: HashSet<u32> = HashSet::new();
            for f in &frontiers {
                for &node in f {
                    if seen.insert(node) {
                        union_dense.push(node);
                    }
                }
            }
        }
        if union_dense.is_empty() {
            break;
        }

        if hop > 1 && matches!(active, ActiveExpandSource::Indexed(_)) {
            let switch = match &hop_policy {
                HopPolicy::Off => false,
                HopPolicy::Full(inputs) => should_switch_to_csr(
                    union_dense.len() as u64,
                    prev_union_len as u64,
                    max - hop + 1,
                    graph_index.is_built(),
                    inputs,
                ),
            };
            if switch {
                crate::instrumentation::record_traversal_mid_switch();
                crate::instrumentation::record_expand_path(false);
                memory.metric("expand_csr", 1);
                Switch::Csr.record(side);
                let gi = graph_index.get().await?.ok_or_else(|| {
                    OmniError::manifest("graph index required for CSR traversal".to_string())
                })?;
                let csr_source =
                    ActiveExpandSource::Csr(resolve_csr(gi, edge_def, edge_type, direction)?);
                let old = std::mem::replace(&mut active, csr_source);
                let ActiveExpandSource::Indexed(old_src) = old else {
                    unreachable!("switch only fires while the Indexed source is active");
                };
                let interner = old_src.interner;
                let ActiveExpandSource::Csr(CsrSource {
                    src_idx, dst_idx, ..
                }) = &active
                else {
                    unreachable!("active source was just replaced with Csr");
                };
                hop_memory
                    .entries::<u32>(
                        frontiers.iter().map(Vec::len).sum::<usize>()
                            + visited.iter().map(HashSet::len).sum::<usize>()
                            + seen_dst.iter().map(HashSet::len).sum::<usize>(),
                    )
                    .map_err(|error| memory.error(error))?;
                let translate_set =
                    |set: &HashSet<u32>, idx: &crate::graph_index::TypeIndex| -> HashSet<u32> {
                        set.iter()
                            .filter_map(|&d| interner.to_id(d).and_then(|id| idx.to_dense(id)))
                            .collect()
                    };
                for i in 0..n {
                    frontiers[i] = frontiers[i]
                        .iter()
                        .filter_map(|&d| interner.to_id(d).and_then(|id| src_idx.to_dense(id)))
                        .collect();
                    visited[i] = translate_set(&visited[i], src_idx);
                    seen_dst[i] = translate_set(&seen_dst[i], dst_idx);
                }
                tracing::debug!(
                    target: "omnigraph::traverse",
                    edge = %edge_type,
                    hop,
                    frontier = union_dense.len(),
                    mode = "csr",
                    reason = "frontier outgrew the indexed path",
                    "expand mode switched mid-traversal",
                );
            }
        }
        prev_union_len = union_dense.len();

        if let ActiveExpandSource::Indexed(src) = &mut active {
            src.neighbor_map = HashMap::new();
            hop_memory
                .entries::<String>(union_dense.len())
                .map_err(|error| memory.error(error))?;
            for &u in &union_dense {
                hop_memory
                    .string(src.interner.to_id(u).map_or(0, str::len))
                    .map_err(|error| memory.error(error))?;
            }
            let union_keys: Vec<String> = union_dense
                .iter()
                .map(|&u| {
                    src.interner
                        .to_id(u)
                        .expect("interned frontier id must resolve")
                        .to_string()
                })
                .collect();
            scan_neighbor_map(
                &src.edge_ds,
                &probes,
                &union_keys,
                &mut src.interner,
                &mut src.neighbor_map,
                &hop_memory,
                memory,
            )
            .await?;
        }

        for i in 0..n {
            let cur = std::mem::take(&mut frontiers[i]);
            let mut next: Vec<u32> = Vec::new();
            for &node in &cur {
                let (fwd, rev): (&[u32], &[u32]) = match &active {
                    ActiveExpandSource::Indexed(src) => (
                        src.neighbor_map
                            .get(&node)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                        &[],
                    ),
                    ActiveExpandSource::Csr(CsrSource { adj, adj_rev, .. }) => (
                        adj.neighbors(node),
                        adj_rev.map(|a| a.neighbors(node)).unwrap_or(&[]),
                    ),
                };
                for &neighbor in fwd.iter().chain(rev) {
                    memory.check().map_err(|error| memory.error(error))?;
                    let is_self = same_type && hop == 1 && neighbor == node;
                    if !is_self && same_type {
                        if visited[i].contains(&neighbor) {
                            continue;
                        }
                        memory
                            .entries::<u32>(1)
                            .map_err(|error| memory.error(error))?;
                        visited[i].insert(neighbor);
                    }
                    if !is_self {
                        next_memory
                            .entries::<u32>(1)
                            .map_err(|error| memory.error(error))?;
                        next.push(neighbor);
                    }
                    if hop >= min_hops && !seen_dst[i].contains(&neighbor) {
                        memory
                            .entries::<u32>(1)
                            .map_err(|error| memory.error(error))?;
                        seen_dst[i].insert(neighbor);
                        let dst_id = match &active {
                            ActiveExpandSource::Indexed(src) => Some(
                                src.interner
                                    .to_id(neighbor)
                                    .expect("interned dst id must resolve"),
                            ),
                            ActiveExpandSource::Csr(CsrSource { dst_idx, .. }) => {
                                dst_idx.to_id(neighbor)
                            }
                        };
                        if let Some(dst_id) = dst_id {
                            emission_memory
                                .entries::<(u32, String)>(1)
                                .map_err(|error| memory.error(error))?;
                            emission_memory
                                .string(dst_id.len())
                                .map_err(|error| memory.error(error))?;
                            emitted_src.push(i as u32);
                            emitted_dst.push(dst_id.to_string());
                        }
                    }
                }
            }
            frontiers[i] = next;
        }
        drop(std::mem::replace(&mut frontier_memory, next_memory));
        if let ActiveExpandSource::Indexed(src) = &mut active {
            src.neighbor_map = HashMap::new();
        }
    }
    drop(active);
    drop(frontiers);
    drop(visited);
    drop(seen_dst);
    drop(frontier_memory);
    memory.release_work();

    Ok(ExpandedPairs {
        source_rows: emitted_src,
        destination_ids: emitted_dst,
        _memory: emission_memory,
    })
}

/// The bulk mask of a row-count block over one edge (`Lowering::bulk_row_count`
/// picks the shape): the CSR degree per outer row, or only its existence when
/// the predicate asks no more.
pub(super) fn bulk_anti_join_mask(
    wide: &RecordBatch,
    edge_type: &str,
    direction: Direction,
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
    outer_var: &str,
    row_count: &RowCountPredicate,
    memory: &WorkMemory,
) -> Result<Option<BooleanArray>> {
    let existence_only = row_count.existence_only();
    let prepared = (|| {
        let gi = graph_index?;
        let edge_def = catalog.edge_types.get(edge_type)?;

        let src_type_name = match direction {
            Direction::Out | Direction::Both => &edge_def.from_type,
            Direction::In => &edge_def.to_type,
        };
        let adj = match direction {
            Direction::Out | Direction::Both => gi.csr(edge_type),
            Direction::In => gi.csc(edge_type),
        }?;
        let adj_rev = match direction {
            Direction::Both => Some(gi.csc(edge_type)?),
            _ => None,
        };
        let type_idx = gi.type_index(src_type_name)?;

        let id_col_name = format!("{}.{}", outer_var, catalog.system_columns.id);
        let outer_ids = wide
            .column_by_name(&id_col_name)?
            .as_any()
            .downcast_ref::<StringArray>()?;

        Some((adj, adj_rev, type_idx, outer_ids))
    })();
    let Some((adj, adj_rev, type_idx, outer_ids)) = prepared else {
        return Ok(None);
    };
    memory
        .entries::<bool>(outer_ids.len())
        .map_err(|error| memory.error(error))?;
    let mut keep_mask = Vec::with_capacity(outer_ids.len());
    let mut targets = Vec::new();
    for i in 0..outer_ids.len() {
        memory.check().map_err(|error| memory.error(error))?;
        let matches = match type_idx.to_dense(outer_ids.value(i)) {
            Some(dense) if existence_only => u64::from(
                adj.has_neighbors(dense)
                    || adj_rev.map(|a| a.has_neighbors(dense)).unwrap_or(false),
            ),
            Some(dense) => {
                targets.clear();
                targets.extend_from_slice(adj.neighbors(dense));
                targets.sort_unstable();
                targets.dedup();
                targets.len() as u64
            }
            None => 0,
        };
        keep_mask.push(row_count.holds(matches)?);
    }
    Ok(Some(BooleanArray::from(keep_mask)))
}

/// Where the shared BFS core reads each hop's neighbors from. The two sources
/// are the same two execution strategies the dispatcher chooses between; the
/// core can swap Indexed → Csr BETWEEN hops (issue #533), carrying its BFS
/// state across the swap instead of restarting.
///
/// Id spaces differ per source: Indexed owns a per-traversal interner (both
/// endpoint types in ONE dense space — see the cross-type single-hop guard in
/// `execute_expand_bfs`), Csr borrows the graph index's per-type dictionaries.
/// A swap therefore translates all live state through the id strings once.
pub(super) enum ActiveExpandSource<'g> {
    Indexed(Box<IndexedExpandSource>),
    Csr(CsrSource<'g>),
}

pub(super) struct IndexedExpandSource {
    pub(super) edge_ds: Dataset,
    pub(super) interner: crate::graph_index::TypeIndex,
    /// This hop's dense key -> dense neighbors (scan order; duplicates
    /// preserved, like CSR multi-edges). Rebuilt per hop.
    pub(super) neighbor_map: HashMap<u32, Vec<u32>>,
}

/// Per-hop re-decision policy for a traversal that started on the indexed
/// path. `Off` preserves a pinned mode; `Full` re-runs the recorded cost
/// comparison with observed growth. Uncosted plans start on CSR.
pub(super) enum HopPolicy {
    Off,
    Full(ExpandCostInputs),
}

/// One `key IN (keys)` scan per probe, every row's endpoints interned into
/// `interner` and the opposite appended to `neighbor_map[key]` in scan order
/// (probe 0's rows, then probe 1's; parallel edges kept, like the CSR).
pub(super) async fn scan_neighbor_map(
    edge_ds: &Dataset,
    probes: &[EndpointColumns],
    keys: &[String],
    interner: &mut crate::graph_index::TypeIndex,
    neighbor_map: &mut HashMap<u32, Vec<u32>>,
    hop_memory: &WorkMemory,
    memory: &WorkMemory,
) -> Result<()> {
    for &orientation in probes {
        let EndpointColumns {
            key: key_col,
            opposite: opp_col,
        } = orientation;
        let batches = scan_edges(edge_ds, orientation, &[], keys, hop_memory).await?;
        for batch in &batches {
            let keys = utf8_column(batch, key_col)?;
            let opposites = utf8_column(batch, opp_col)?;
            for r in 0..batch.num_rows() {
                let k = intern(interner, keys.value(r), memory)?;
                let o = intern(interner, opposites.value(r), memory)?;
                hop_memory
                    .entries::<(u32, Vec<u32>, u32)>(1)
                    .map_err(|error| memory.error(error))?;
                neighbor_map.entry(k).or_default().push(o);
            }
        }
    }
    Ok(())
}

fn utf8_column<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| OmniError::manifest(format!("edge batch missing '{}'", name)))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("edge '{}' is not Utf8", name)))
}

pub(super) fn intern(
    index: &mut crate::graph_index::TypeIndex,
    value: &str,
    memory: &WorkMemory,
) -> Result<u32> {
    if let Some(id) = index.to_dense(value) {
        return Ok(id);
    }
    memory
        .entries::<(String, u32)>(2)
        .map_err(|error| memory.error(error))?;
    memory
        .string(value.len().saturating_mul(2))
        .map_err(|error| memory.error(error))?;
    Ok(index.get_or_insert(value))
}

async fn scan_edges(
    ds: &Dataset,
    orientation: EndpointColumns,
    extras: &[&str],
    keys: &[String],
    memory: &WorkMemory,
) -> Result<Vec<RecordBatch>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let stream = scan_edges_stream(ds, orientation, extras, keys, memory).await?;
    memory
        .collect(stream)
        .await
        .map_err(|error| memory.error(error))
}

async fn scan_edges_stream(
    ds: &Dataset,
    orientation: EndpointColumns,
    extras: &[&str],
    keys: &[String],
    memory: &WorkMemory,
) -> Result<SendableRecordBatchStream> {
    memory
        .entries::<datafusion::prelude::Expr>(keys.len())
        .map_err(|error| memory.error(error))?;
    memory
        .string(keys.iter().map(String::len).sum())
        .map_err(|error| memory.error(error))?;
    let EndpointColumns {
        key: key_col,
        opposite: opposite_col,
    } = orientation;
    let mut projection = vec![key_col, opposite_col];
    projection.extend(
        extras
            .iter()
            .copied()
            .filter(|column| *column != key_col && *column != opposite_col),
    );
    let filter = id_in_list_expr(keys, key_col);
    let plan = crate::table_store::TableStore::scan_plan_with(
        ds,
        Some(&projection),
        None,
        false,
        |scanner| {
            scanner.filter_expr(filter);
            scanner.batch_size(memory.ctx.session_config().batch_size().max(1));
            scanner.batch_size_bytes(memory.batch_bytes() as u64);
            Ok(())
        },
    )
    .await?;
    let (_, stream) = memory.stream(plan).map_err(|error| memory.error(error))?;
    Ok(stream)
}
