//! `RankFuseExec` drains and ranks both arms by score, fused identity, and
//! downstream row keys. `fuse_arms` sums reciprocal ranks per entity and
//! emits every arm row with that sum in the fused score column; the `Sort`
//! above orders and cuts (RFC 0047 §Total order).

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::memory::WorkMemory;
use super::{breaker_properties, breaker_stream, conform_positional, drain_one, external, polled};
use crate::engine::graph::{fuse_arms, fused_schema};
use crate::engine::search::RrfMode;
use crate::error::OmniError;

/// The order one arm's rows take before fusion: its score column and the
/// direction the index ranks by.
#[derive(Debug, Clone)]
pub(crate) struct ArmOrder {
    pub(crate) score_column: String,
    pub(crate) descending: bool,
}

pub(crate) struct RankFuseExec {
    primary: Arc<dyn ExecutionPlan>,
    secondary: Arc<dyn ExecutionPlan>,
    rrf: RrfMode,
    id_column: String,
    /// The fused score column appended after the primary arm's columns.
    fused_score: String,
    orders: [ArmOrder; 2],
    row_tiebreak: Vec<String>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl RankFuseExec {
    pub(crate) fn new(
        primary: Arc<dyn ExecutionPlan>,
        secondary: Arc<dyn ExecutionPlan>,
        rrf: RrfMode,
        id_column: String,
        fused_score: String,
        orders: [ArmOrder; 2],
        row_tiebreak: Vec<String>,
    ) -> Self {
        let schema = fused_schema(&primary.schema(), &fused_score);
        Self {
            primary,
            secondary,
            rrf,
            id_column,
            fused_score,
            orders,
            row_tiebreak,
            properties: breaker_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for RankFuseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RankFuseExec")
            .field("id_column", &self.id_column)
            .field("fused_score", &self.fused_score)
            .field("k", &self.rrf.k)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for RankFuseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RankFuseExec: on={}, score={}, k={}, arms=[{} {}, {} {}]",
            self.id_column,
            self.fused_score,
            self.rrf.k,
            self.orders[0].score_column,
            direction(self.orders[0].descending),
            self.orders[1].score_column,
            direction(self.orders[1].descending),
        )
    }
}

fn direction(descending: bool) -> &'static str {
    if descending { "desc" } else { "asc" }
}

/// `batch` in the arm's rank order: its score, the fused binding's id, then
/// the declared downstream row columns, nulls last on every key, so
/// equal scores fuse the same way on every run.
fn ranked(
    batch: &RecordBatch,
    order: &ArmOrder,
    id_column: &str,
    row_tiebreak: &[String],
    memory: &WorkMemory,
) -> DfResult<RecordBatch> {
    let column = |name: &str| {
        batch.column_by_name(name).cloned().ok_or_else(|| {
            external(OmniError::manifest_internal(format!(
                "RRF arm has no column '{name}'"
            )))
        })
    };
    let mut keys = vec![
        SortColumn {
            values: column(&order.score_column)?,
            options: Some(SortOptions {
                descending: order.descending,
                nulls_first: false,
            }),
        },
        SortColumn {
            values: column(id_column)?,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        },
    ];
    for name in row_tiebreak {
        keys.push(SortColumn {
            values: column(name)?,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        });
    }
    memory.entries::<u32>(batch.num_rows())?;
    let indices = lexsort_to_indices(&keys, None)?;
    memory.take(batch, &indices)
}

impl ExecutionPlan for RankFuseExec {
    fn name(&self) -> &str {
        "RankFuseExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.primary, &self.secondary]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(
            children.len(),
            2,
            "RankFuseExec has a primary and a secondary leg"
        );
        let secondary = children.pop().expect("secondary leg");
        let primary = children.pop().expect("primary leg");
        Ok(Arc::new(Self::new(
            primary,
            secondary,
            self.rrf,
            self.id_column.clone(),
            self.fused_score.clone(),
            self.orders.clone(),
            self.row_tiebreak.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "RankFuseExec has one partition");
        let schema: SchemaRef = self.schema();
        let primary_schema = self.primary.schema();
        let secondary_schema = self.secondary.schema();
        let primary = self.primary.execute(0, Arc::clone(&ctx))?;
        let secondary = self.secondary.execute(0, Arc::clone(&ctx))?;
        let rrf = self.rrf;
        let id_column = self.id_column.clone();
        let fused_score = self.fused_score.clone();
        let orders = self.orders.clone();
        let row_tiebreak = self.row_tiebreak.clone();
        let declared = Arc::clone(&schema);
        let stream = breaker_stream(
            "RankFuseExec",
            schema,
            &ctx,
            &self.metrics,
            move |reservation| async move {
                let primary = drain_one(primary, &primary_schema, &reservation).await?;
                let secondary = drain_one(secondary, &secondary_schema, &reservation).await?;
                reservation
                    .blocking(move |reservation| async move {
                        let primary = ranked(
                            &primary,
                            &orders[0],
                            &id_column,
                            &row_tiebreak,
                            &reservation,
                        )?;
                        let secondary = ranked(
                            &secondary,
                            &orders[1],
                            &id_column,
                            &row_tiebreak,
                            &reservation,
                        )?;
                        let fused = fuse_arms(
                            &primary,
                            &secondary,
                            &rrf,
                            &id_column,
                            &fused_score,
                            &reservation,
                        )
                        .map_err(external)?;
                        conform_positional(fused, &declared).map_err(external)
                    })
                    .await
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Float32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::traversal::EDGE_TYPE_COLUMN;

    #[test]
    fn issue_659_rrf_arm_orders_equal_edge_ids_by_declared_concrete_type() {
        let edge_type = format!("e.{EDGE_TYPE_COLUMN}");
        let schema = Arc::new(Schema::new(vec![
            Field::new("p._score", DataType::Float32, false),
            Field::new("p.__id", DataType::Utf8, false),
            Field::new("e.__id", DataType::Utf8, false),
            Field::new(&edge_type, DataType::Utf8, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![1.0, 1.0, 1.0])),
            Arc::new(StringArray::from(vec!["p", "p", "p"])),
            Arc::new(StringArray::from(vec!["shared", "z", "shared"])),
            Arc::new(StringArray::from(vec!["Likes", "Knows", "Knows"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let (_, ctx) = super::super::fixtures::context(1_048_576, 16);
        let memory = WorkMemory::new(ctx, "typed RRF order test").unwrap();
        let ranked = ranked(
            &batch,
            &ArmOrder {
                score_column: "p._score".into(),
                descending: true,
            },
            "p.__id",
            &[edge_type.clone(), "e.__id".into()],
            &memory,
        )
        .unwrap();
        let types = crate::engine::graph::extract_id_column_by_name(&ranked, &edge_type).unwrap();
        let ids = crate::engine::graph::extract_id_column_by_name(&ranked, "e.__id").unwrap();
        assert_eq!(types, vec!["Knows", "Knows", "Likes"]);
        assert_eq!(ids, vec!["shared", "z", "shared"]);
        let fused = fuse_arms(
            &ranked,
            &ranked,
            &RrfMode { k: 60 },
            "p.__id",
            "p._rrf",
            &memory,
        )
        .unwrap();
        assert_eq!(fused.num_rows(), 3);
        assert_eq!(
            crate::engine::graph::extract_id_column_by_name(&fused, &edge_type).unwrap(),
            types
        );
        let scores = fused
            .column_by_name("p._rrf")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert_eq!(scores.values(), &[2.0 / 61.0; 3]);
    }
}
