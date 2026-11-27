//! `LetExec`: a `let`, the core's `Project` adding one column (the GQ 3 RFC,
//! section 3). The value is computed once on every input row and appended
//! as the `value` column of the binding the shaper made up for the `let`,
//! which every later read of the name reads; every row passes through, in
//! input order. A value that can fail (`E1`, `E2`, `E3`) refuses the read on
//! the rows the elements before the `let` produce, whatever `where` later
//! removes (A3).

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_compiler::shape::read::LET_VALUE;

use super::external;
use super::shape::{Mapped, Streaming};
use crate::engine::expr::evaluate_expr;
use crate::engine::lexical::LexicalColumns;
use crate::error::{OmniError, Result};

pub(crate) struct LetExec {
    input: Arc<dyn ExecutionPlan>,
    name: String,
    value: IRExpr,
    params: Arc<ParamMap>,
    /// The membership columns the value reads full-text calls as.
    lexical: Option<Arc<LexicalColumns>>,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

impl LetExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        name: String,
        value: IRExpr,
        params: Arc<ParamMap>,
        lexical: Option<Arc<LexicalColumns>>,
    ) -> Result<Self> {
        let data_type = value.ty().to_arrow().ok_or_else(|| {
            OmniError::manifest_internal(format!("`let` value `{value}` has no Arrow type"))
        })?;
        let input_schema = input.schema();
        let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(
            format!("{name}.{LET_VALUE}"),
            data_type,
            true,
        )));
        let shape = Streaming::new(Arc::new(Schema::new(fields)));
        Ok(Self {
            input,
            name,
            value,
            params,
            lexical,
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl fmt::Debug for LetExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LetExec")
            .field("name", &self.name)
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LetExec: ${} = {}", self.name, self.value)
    }
}

impl ExecutionPlan for LetExec {
    fn name(&self) -> &str {
        "LetExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "LetExec has one child");
        Ok(Arc::new(
            Self::new(
                children.pop().expect("one child"),
                self.name.clone(),
                self.value.clone(),
                Arc::clone(&self.params),
                self.lexical.clone(),
            )
            .map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "LetExec has one partition");
        let mut input = self.input.execute(0, Arc::clone(&ctx))?;
        if let Some(lexical) = &self.lexical {
            let mut scan = super::memory::WorkMemory::new(ctx, super::filter::LEXICAL_SCAN)?;
            scan.set_metrics(self.metrics.clone());
            let lexical = Arc::clone(lexical);
            input =
                crate::engine::lexical::answered_first(
                    input,
                    async move { lexical.answer(scan).await },
                );
        }
        let value = self.value.clone();
        let params = Arc::clone(&self.params);
        let lexical = self.lexical.clone();
        let schema = self.schema();
        Ok(self.shape.map(&self.metrics, input, move |batch| {
            let column = match &lexical {
                Some(lexical) => {
                    let widened = lexical.widen(&batch).map_err(external)?;
                    evaluate_expr(&widened, &value, &params).map_err(external)?
                }
                None => evaluate_expr(&batch, &value, &params).map_err(external)?,
            };
            let mut columns = batch.columns().to_vec();
            columns.push(column);
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
                .map_err(|e| external(OmniError::arrow_internal(e)))?;
            Ok(Mapped::Batch { batch, lease: None })
        }))
    }
}
