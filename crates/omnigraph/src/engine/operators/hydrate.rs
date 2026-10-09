//! `HydrateExec`: a `HydrateColumns` node. The rows that reached the output
//! carry each deferred binding's row address (the `^binding` column the
//! return projection below carries); this operator takes the deferred
//! columns of those rows from the binding's pinned table by address, one
//! chunk at a time under `hydrate_chunk_bytes`, and emits the return columns
//! in return order without the addresses. A chunk's bound covers the fetched
//! rows and their copies, one per output row, which the pool admits before
//! Arrow builds them.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array, new_null_array};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::TakeBuilder;
use lance_core::ROW_ADDR;
use lance_datafusion::projection::ProjectionPlan;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_planner::{HydratedBinding, ROW_ADDRESS_PREFIX, hydrate_chunk_bytes};

use super::memory::WorkMemory;
use super::producer::{BatchSender, producer_stream};
use super::{external, polled, streaming_properties};
use crate::db::Snapshot;
use crate::error::{OmniError, Result};

/// The pool owner of one hydrated chunk: the fetched rows, then the output.
const CHUNK: &str = "hydrate chunk";

/// The admission of a chunk's copied values while Arrow builds them.
const OUTPUT: &str = "hydrate output";

/// The gauge of the most bytes one chunk held at once: its fetched rows and
/// their copies. It stays under `hydrate_chunk_bytes` unless one row alone
/// exceeds it.
const PEAK_CHUNK_BYTES: &str = "peak_chunk_bytes";

/// Rows of the first chunk, before a hydrated row's width is measured.
const SEED_ROWS: usize = 4;

pub(crate) struct HydrateExec {
    input: Arc<dyn ExecutionPlan>,
    bindings: Vec<HydratedBinding>,
    layout: Vec<Slot>,
    snapshot: Snapshot,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

/// Where one output column comes from: an input column (by index), or the
/// `column`th deferred column of the `binding`th hydrated binding.
#[derive(Debug, Clone, Copy)]
enum Slot {
    Input(usize),
    Hydrated { binding: usize, column: usize },
}

impl HydrateExec {
    /// The output is the return order: a deferred column sits at its return
    /// position under its return name with the field `declared` gives it,
    /// as the projection would have carried it; the input's other columns
    /// fill the remaining positions in their order, and the row addresses
    /// leave.
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        bindings: Vec<HydratedBinding>,
        snapshot: Snapshot,
        catalog: &Catalog,
        declared: &Schema,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mut deferred: HashMap<usize, (Slot, Field)> = HashMap::new();
        for (index, binding) in bindings.iter().enumerate() {
            let address = address_column(&binding.binding);
            if input_schema.column_with_name(&address).is_none() {
                return Err(OmniError::manifest_internal(format!(
                    "the rows reaching `HydrateColumns` carry no row address for `${}`",
                    binding.binding
                )));
            }
            let node_type = binding
                .table
                .node_type_name()
                .and_then(|type_name| catalog.node_types.get(type_name))
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "`HydrateColumns` reads `{}`, which is no node type",
                        binding.table.type_key
                    ))
                })?;
            for (column_index, column) in binding.columns.iter().enumerate() {
                if node_type
                    .arrow_schema
                    .field_with_name(&column.property)
                    .is_err()
                {
                    return Err(OmniError::manifest_internal(format!(
                        "`{}` has no property `{}` to hydrate",
                        binding.table.type_key, column.property
                    )));
                }
                let field = declared.field_with_name(&column.output).map_err(|_| {
                    OmniError::manifest_internal(format!(
                        "`HydrateColumns` declares no return column `{}`",
                        column.output
                    ))
                })?;
                let slot = Slot::Hydrated {
                    binding: index,
                    column: column_index,
                };
                let field = field.clone();
                if deferred.insert(column.position, (slot, field)).is_some() {
                    return Err(OmniError::manifest_internal(format!(
                        "two hydrated columns fill return position {}",
                        column.position
                    )));
                }
            }
        }
        let mut carried = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !field.name().starts_with(ROW_ADDRESS_PREFIX));
        let width = carried.clone().count() + deferred.len();
        let mut layout = Vec::with_capacity(width);
        let mut fields = Vec::with_capacity(width);
        for position in 0..width {
            let (slot, field) = match deferred.remove(&position) {
                Some(hydrated) => hydrated,
                None => {
                    let (index, field) = carried.next().ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "`HydrateColumns` has no input column for return position {position}"
                        ))
                    })?;
                    (Slot::Input(index), field.as_ref().clone())
                }
            };
            layout.push(slot);
            fields.push(field);
        }
        if let Some(position) = deferred.keys().min() {
            return Err(OmniError::manifest_internal(format!(
                "a hydrated column fills return position {position}, past the return's {width}"
            )));
        }
        let properties = streaming_properties(Arc::new(Schema::new(fields)));
        Ok(Self {
            input,
            bindings,
            layout,
            snapshot,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// The input column holding `binding`'s row address.
fn address_column(binding: &str) -> String {
    format!("{ROW_ADDRESS_PREFIX}{binding}")
}

impl fmt::Debug for HydrateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HydrateExec")
            .field("bindings", &self.bindings)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for HydrateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bindings: Vec<String> = self
            .bindings
            .iter()
            .map(|binding| {
                let columns: Vec<&str> = binding
                    .columns
                    .iter()
                    .map(|column| column.property.as_str())
                    .collect();
                format!("${}: [{}]", binding.binding, columns.join(", "))
            })
            .collect();
        write!(f, "HydrateExec: {}", bindings.join(", "))
    }
}

impl ExecutionPlan for HydrateExec {
    fn name(&self) -> &str {
        "HydrateExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "HydrateExec has one child");
        Ok(Arc::new(Self {
            input: children.pop().expect("one child"),
            bindings: self.bindings.clone(),
            layout: self.layout.clone(),
            snapshot: self.snapshot.clone(),
            properties: Arc::clone(&self.properties),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "HydrateExec has one partition");
        let schema: SchemaRef = self.schema();
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let mut work = WorkMemory::new(ctx, "HydrateExec")?;
        work.set_metrics(self.metrics.clone());
        work.metric("input_rows", 0);
        work.metric("hydrated_rows", 0);
        let plan = Hydration {
            layout: self.layout.clone(),
            bindings: self.bindings.clone(),
            snapshot: self.snapshot.clone(),
            schema: Arc::clone(&schema),
            peak: MetricBuilder::new(&self.metrics).gauge(PEAK_CHUNK_BYTES, 0),
        };
        let stream = producer_stream(
            schema,
            Arc::new(work),
            Some(&self.metrics),
            move |memory, sender| async move { hydrate(input, plan, &memory, &sender).await },
        );
        Ok(polled(&self.metrics, stream))
    }
}

/// What one execution hydrates, moved into its producer.
struct Hydration {
    layout: Vec<Slot>,
    bindings: Vec<HydratedBinding>,
    snapshot: Snapshot,
    schema: SchemaRef,
    peak: Gauge,
}

/// One deferred binding's pinned table and the columns taken from it.
struct Source {
    binding: String,
    address: String,
    dataset: Arc<Dataset>,
    projection: Arc<ProjectionPlan>,
    /// `(return name, property)` for each deferred return item.
    columns: Vec<(String, String)>,
}

async fn hydrate(
    mut input: SendableRecordBatchStream,
    plan: Hydration,
    memory: &Arc<WorkMemory>,
    sender: &BatchSender,
) -> DfResult<()> {
    let hard = usize::try_from(hydrate_chunk_bytes(memory.pool_bytes())).unwrap_or(usize::MAX);
    let target = (hard / 2).max(1);
    let mut sources = Vec::with_capacity(plan.bindings.len());
    for binding in &plan.bindings {
        let dataset = Arc::new(
            plan.snapshot
                .open_lance_dataset(&binding.table.type_key)
                .await
                .map_err(external)?,
        );
        let mut properties: Vec<&str> = Vec::new();
        for column in &binding.columns {
            if !properties.contains(&column.property.as_str()) {
                properties.push(&column.property);
            }
        }
        let projection = dataset
            .schema()
            .project(&properties)
            .and_then(|schema| ProjectionPlan::from_schema(dataset.clone(), &schema))
            .map_err(|error| external(OmniError::storage_context("hydrate projection", error)))?;
        sources.push(Source {
            binding: binding.binding.clone(),
            address: address_column(&binding.binding),
            dataset,
            projection: Arc::new(projection),
            columns: binding
                .columns
                .iter()
                .map(|column| (column.output.clone(), column.property.clone()))
                .collect(),
        });
    }
    // The costliest output row seen so far, its share of the fetched rows
    // and its own copy, plans the next window's rows.
    let mut row_bytes = 0usize;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        memory.metric("input_rows", batch.num_rows());
        let mut start = 0;
        while start < batch.num_rows() {
            let planned = target
                .checked_div(row_bytes)
                .map_or(SEED_ROWS, |rows| rows.max(1));
            let mut rows = planned.min(batch.num_rows() - start);
            loop {
                memory.check()?;
                let window = batch.slice(start, rows);
                let chunk = Arc::new(memory.child(CHUNK)?);
                let (fetched, fetched_bytes) =
                    fetch_window(&window, &sources, &plan.schema, &chunk).await?;
                if fetched_bytes > hard && rows > 1 {
                    rows = rows.div_ceil(2);
                    continue;
                }
                // A row a join repeats is copied once per output row, so the
                // window keeps a prefix, halved until the pool's estimate of
                // its copies fits beside the fetched rows; the rest is the
                // next window.
                let mut keep = rows;
                let mut copied = copy_bytes(&fetched, keep, &chunk)?;
                while keep > 1 && fetched_bytes.saturating_add(copied) > hard {
                    keep = keep.div_ceil(2);
                    copied = copy_bytes(&fetched, keep, &chunk)?;
                }
                row_bytes = row_bytes
                    .max(fetched_bytes.saturating_add(copied) / keep)
                    .max(1);
                let taken = fetched
                    .iter()
                    .map(|source| {
                        chunk
                            .take_once(&source.values, &source.indices.slice(0, keep), OUTPUT)
                            .map(|values| values.columns().to_vec())
                    })
                    .collect::<DfResult<Vec<_>>>()?;
                let copies = taken
                    .iter()
                    .flatten()
                    .map(|column| column.get_array_memory_size())
                    .sum::<usize>();
                plan.peak.set_max(fetched_bytes.saturating_add(copies));
                drop(fetched);
                let output = assemble(&window.slice(0, keep), &taken, &plan)?;
                chunk.output(&output)?;
                memory.metric("hydrated_rows", keep);
                sender.send(output, chunk).await?;
                start += keep;
                break;
            }
        }
    }
    Ok(())
}

/// One source's deferred columns for a window: `values` holds them for the
/// distinct rows Lance returned (one null row when every address is null),
/// and `indices` places each window row among those rows, null for a null
/// address.
struct Fetched {
    values: RecordBatch,
    indices: UInt32Array,
}

/// What `take_once` admits to copy the first `rows` rows of the window from
/// every source.
fn copy_bytes(fetched: &[Fetched], rows: usize, chunk: &WorkMemory) -> DfResult<usize> {
    fetched.iter().try_fold(0usize, |sum, source| {
        Ok(sum.saturating_add(chunk.take_bytes(&source.values, &source.indices.slice(0, rows))?))
    })
}

/// Each source's deferred columns for the distinct rows of `window`, held
/// by `chunk`, and the bytes Lance returned. A row address the pinned table
/// does not hold is an integrity failure.
async fn fetch_window(
    window: &RecordBatch,
    sources: &[Source],
    schema: &SchemaRef,
    chunk: &WorkMemory,
) -> DfResult<(Vec<Fetched>, usize)> {
    let mut fetched = Vec::with_capacity(sources.len());
    let mut bytes = 0usize;
    for source in sources {
        let addresses = window
            .column_by_name(&source.address)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "`HydrateColumns` input has no UInt64 '{}'",
                    source.address
                ))
            })?;
        let mut unique: Vec<u64> = addresses.iter().flatten().collect();
        unique.sort_unstable();
        unique.dedup();
        chunk.entries::<(u64, u32)>(unique.len())?;
        let fields = source
            .columns
            .iter()
            .map(|(output, _)| {
                let field = schema.field_with_name(output)?;
                Ok(Field::new(output, field.data_type().clone(), true))
            })
            .collect::<DfResult<Vec<Field>>>()?;
        let values_schema = Arc::new(Schema::new(fields));
        if unique.is_empty() {
            let columns = values_schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), 1))
                .collect();
            fetched.push(Fetched {
                values: RecordBatch::try_new(values_schema, columns)?,
                indices: UInt32Array::new_null(window.num_rows()),
            });
            continue;
        }
        let batch = TakeBuilder::try_new_from_addresses(
            Arc::clone(&source.dataset),
            unique,
            Arc::clone(&source.projection),
        )
        .map(|builder| builder.with_row_address(true))
        .map_err(|error| external(OmniError::storage_context("hydrate take", error)))?
        .execute()
        .await
        .map_err(|error| {
            external(OmniError::storage_context(
                format!("hydrating the return columns of `${}`", source.binding),
                error,
            ))
        })?;
        chunk.hold(&batch)?;
        bytes = bytes.saturating_add(batch.get_array_memory_size());
        let positions: HashMap<u64, u32> = batch
            .column_by_name(ROW_ADDR)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                DataFusionError::Internal("a row-address take returned no _rowaddr".into())
            })?
            .values()
            .iter()
            .enumerate()
            .map(|(row, address)| Ok((*address, u32::try_from(row)?)))
            .collect::<std::result::Result<_, std::num::TryFromIntError>>()
            .map_err(|_| DataFusionError::Internal("a hydrated chunk exceeds u32 rows".into()))?;
        let indices = addresses
            .iter()
            .map(|address| {
                address
                    .map(|address| {
                        positions.get(&address).copied().ok_or_else(|| {
                            external(OmniError::manifest_internal(format!(
                                "row address {address} of `${}` is not in its pinned table",
                                source.binding
                            )))
                        })
                    })
                    .transpose()
            })
            .collect::<DfResult<UInt32Array>>()?;
        let columns = source
            .columns
            .iter()
            .map(|(_, property)| {
                batch.column_by_name(property).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "a row-address take returned no '{property}'"
                    ))
                })
            })
            .collect::<DfResult<Vec<ArrayRef>>>()?;
        fetched.push(Fetched {
            values: RecordBatch::try_new(values_schema, columns)?,
            indices,
        });
    }
    Ok((fetched, bytes))
}

/// The return columns of `window` in return order, as `plan.layout` places
/// them: a deferred one from its source's taken arrays, any other from the
/// window.
fn assemble(
    window: &RecordBatch,
    taken: &[Vec<ArrayRef>],
    plan: &Hydration,
) -> DfResult<RecordBatch> {
    let columns = plan
        .layout
        .iter()
        .map(|slot| match *slot {
            Slot::Input(index) => Arc::clone(window.column(index)),
            Slot::Hydrated { binding, column } => Arc::clone(&taken[binding][column]),
        })
        .collect::<Vec<ArrayRef>>();
    Ok(RecordBatch::try_new(Arc::clone(&plan.schema), columns)?)
}
