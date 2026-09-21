//! `ExpandExec` and the step it owns; `expand_stream` states its three strategies.

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result as DfResult, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::types::Direction;
use omnigraph_planner::{ExpandCostInputs, ExpandMode};

use super::{breaker_properties, external, joined_schema, streaming_properties};
use crate::db::Snapshot;
use crate::engine::graph::{GraphIndexHandle, ModeOrigin, projectable_edge_property_columns};
use crate::error::{OmniError, Result};

/// What every graph operator of one query shares: the lazy CSR handle, the
/// snapshot and the catalog.
pub(crate) struct GraphEnv {
    pub(crate) graph_index: Arc<GraphIndexHandle>,
    pub(crate) snapshot: Snapshot,
    pub(crate) catalog: Arc<Catalog>,
}

/// The `Expand` node's own fields, owned by the operator: the traversal, the
/// mode the plan recorded, its frontier estimate, and where the mode came
/// from (`origin`), which decides every runtime correction.
#[derive(Debug, Clone)]
pub(crate) struct ExpandStep {
    pub(crate) src: String,
    pub(crate) dst: String,
    pub(crate) edge_type: String,
    pub(crate) direction: Direction,
    pub(crate) dst_type: String,
    pub(crate) min_hops: u32,
    pub(crate) max_hops: u32,
    pub(crate) edge_binding: Option<String>,
    pub(crate) mode: ExpandMode,
    pub(crate) frontier_estimate: Option<u64>,
    pub(crate) origin: ModeOrigin,
}

impl ExpandStep {
    /// `forced`: the session's traversal pin chose the mode; `cost`: the
    /// inputs the planner costed it from, absent without statistics.
    pub(crate) fn origin(forced: bool, cost: Option<ExpandCostInputs>) -> ModeOrigin {
        match (forced, cost) {
            (true, _) => ModeOrigin::Pinned,
            (false, Some(inputs)) => ModeOrigin::Costed(inputs),
            (false, None) => ModeOrigin::Uncosted,
        }
    }

    /// One hop and no more: the streaming walk. A bound edge is always one
    /// hop (typecheck rule T23); a cross-type edge with a wider range stays
    /// on the breaker, which caps it at one hop itself.
    pub(crate) fn single_hop(&self) -> bool {
        self.max_hops == 1
    }
}

pub(crate) struct ExpandExec {
    input: Arc<dyn ExecutionPlan>,
    step: ExpandStep,
    env: Arc<GraphEnv>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ExpandExec {
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        step: ExpandStep,
        env: Arc<GraphEnv>,
    ) -> Result<Self> {
        let schema = expand_output_schema(&input.schema(), &env.catalog, &step)?;
        let properties = if step.single_hop() {
            streaming_properties(schema)
        } else {
            breaker_properties(schema)
        };
        Ok(Self {
            input,
            step,
            env,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// Source columns, the destination ID, then any bound-edge columns.
fn expand_output_schema(input: &Schema, catalog: &Catalog, step: &ExpandStep) -> Result<SchemaRef> {
    let destination = Schema::new(vec![Field::new(
        format!("{}.{}", step.dst, catalog.system_columns.id),
        DataType::Utf8,
        false,
    )]);
    let mut schema = joined_schema(input, &destination)?;
    if let Some(binding) = &step.edge_binding {
        let edge_def = catalog.edge_types.get(&step.edge_type).ok_or_else(|| {
            OmniError::manifest(format!("unknown edge type '{}'", step.edge_type))
        })?;
        let mut attach_cols: Vec<&str> = vec![
            catalog.system_columns.id,
            catalog.system_columns.src,
            catalog.system_columns.dst,
        ];
        attach_cols.extend(projectable_edge_property_columns(edge_def));
        let edge_fields: Vec<Field> = attach_cols
            .iter()
            .map(|name| {
                edge_def
                    .arrow_schema
                    .field_with_name(name)
                    .map(|field| {
                        Field::new(
                            format!("{binding}.{name}"),
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                    })
                    .map_err(|e| OmniError::manifest(e.to_string()))
            })
            .collect::<Result<_>>()?;
        schema = joined_schema(&schema, &Schema::new(edge_fields))?;
    }
    Ok(schema)
}

impl fmt::Debug for ExpandExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpandExec")
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let step = &self.step;
        write!(
            f,
            "ExpandExec: ${} {} ${}: {}, direction={:?}, hops={}..{}",
            step.src,
            step.edge_type,
            step.dst,
            step.dst_type,
            step.direction,
            step.min_hops,
            step.max_hops,
        )?;
        if let Some(binding) = &step.edge_binding {
            write!(f, ", edge_binding=${binding}, batches=bounded")?;
        }
        write!(f, ", mode={}", step.mode.word())?;
        match step.frontier_estimate {
            Some(rows) => write!(f, ", estimate={rows}")?,
            None => write!(f, ", estimate=unknown")?,
        }
        write!(f, ", streaming={}", step.single_hop())
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &str {
        "ExpandExec"
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
        let (Some(input), true) = (children.pop(), children.is_empty()) else {
            return internal_err!("ExpandExec takes one child");
        };
        Ok(Arc::new(
            Self::try_new(input, self.step.clone(), Arc::clone(&self.env)).map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("ExpandExec has one partition, asked for {}", partition);
        }
        let schema: SchemaRef = self.schema();
        let input_schema = self.input.schema();
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let step = self.step.clone();
        let env = Arc::clone(&self.env);
        super::expand_stream::execute(input, input_schema, schema, step, env, ctx, &self.metrics)
    }
}
