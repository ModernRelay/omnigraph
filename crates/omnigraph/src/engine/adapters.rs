//! GQ filters and return expressions as DataFusion `PhysicalExpr`s over the
//! wide batch. Two adapters are equal when they print the same GQ text and
//! belong to the same lowering.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::common::Result as DfResult;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use omnigraph_compiler::ir::{IRExpr, IRFilter, ParamMap};

use super::expr::{ProjectionContext, evaluate_filter, evaluate_projection, identity_column};
use super::operators::external;
use crate::error::{OmniError, Result};

static NEXT_LOWERING: AtomicU64 = AtomicU64::new(0);

/// The id a lowering stamps on every adapter it builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct LoweringId(u64);

impl LoweringId {
    pub(super) fn next() -> Self {
        Self(NEXT_LOWERING.fetch_add(1, Ordering::Relaxed))
    }
}

/// A GQ filter as a boolean `PhysicalExpr`.
pub(super) struct GqFilterExpr {
    filter: IRFilter,
    params: Arc<ParamMap>,
    text: String,
    lowering: LoweringId,
}

impl GqFilterExpr {
    pub(super) fn new(filter: IRFilter, params: Arc<ParamMap>, lowering: LoweringId) -> Self {
        let text = filter.to_string();
        Self {
            filter,
            params,
            text,
            lowering,
        }
    }
}

impl fmt::Debug for GqFilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GqFilterExpr({})", self.text)
    }
}

impl fmt::Display for GqFilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl PartialEq for GqFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text && self.lowering == other.lowering
    }
}

impl Eq for GqFilterExpr {}

impl Hash for GqFilterExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.text.hash(state);
        self.lowering.hash(state);
    }
}

impl PhysicalExpr for GqFilterExpr {
    fn return_field(&self, _input_schema: &Schema) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(&self.text, DataType::Boolean, true)))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DfResult<ColumnarValue> {
        let mask = evaluate_filter(batch, &self.filter, &self.params).map_err(external)?;
        Ok(ColumnarValue::Array(Arc::new(mask)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

/// What a return expression projects: the expression itself, or the
/// binding's identity column for `count($v)`.
#[derive(Debug, Clone)]
pub(super) enum Projected {
    Expression(IRExpr),
    Identity(String),
}

impl Projected {
    /// The column name `evaluate_projection` gives the value.
    pub(super) fn name(&self) -> Result<String> {
        match self {
            Self::Identity(variable) => Ok(variable.clone()),
            Self::Expression(expr) => match expr {
                IRExpr::PropAccess { variable, property } => Ok(format!("{variable}.{property}")),
                IRExpr::Literal(_) => Ok("literal".to_string()),
                IRExpr::Param(name) => Ok(name.clone()),
                IRExpr::Variable(name) => Ok(name.clone()),
                _ => Err(OmniError::manifest(format!(
                    "unsupported projection expression: {:?}",
                    expr
                ))),
            },
        }
    }

    fn text(&self) -> String {
        match self {
            Self::Identity(variable) => format!("count(${variable})"),
            Self::Expression(expr) => expr.to_string(),
        }
    }
}

/// A GQ return expression as a `PhysicalExpr` over the wide batch.
pub(super) struct GqProjectionExpr {
    projected: Projected,
    params: Arc<ParamMap>,
    ctx: Arc<ProjectionContext>,
    text: String,
    lowering: LoweringId,
}

impl GqProjectionExpr {
    pub(super) fn new(
        projected: Projected,
        params: Arc<ParamMap>,
        ctx: Arc<ProjectionContext>,
        lowering: LoweringId,
    ) -> Self {
        let text = projected.text();
        Self {
            projected,
            params,
            ctx,
            text,
            lowering,
        }
    }

    fn project(&self, batch: &RecordBatch) -> Result<(String, arrow_array::ArrayRef)> {
        match &self.projected {
            Projected::Identity(variable) => identity_column(batch, variable, &self.ctx),
            Projected::Expression(expr) => {
                evaluate_projection(batch, expr, &self.params, &self.ctx)
            }
        }
    }
}

impl fmt::Debug for GqProjectionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GqProjectionExpr({})", self.text)
    }
}

impl fmt::Display for GqProjectionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl PartialEq for GqProjectionExpr {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text && self.lowering == other.lowering
    }
}

impl Eq for GqProjectionExpr {}

impl Hash for GqProjectionExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.text.hash(state);
        self.lowering.hash(state);
    }
}

impl PhysicalExpr for GqProjectionExpr {
    /// The type comes from projecting an empty batch of the input schema,
    /// the same code path as the evaluation, so the two cannot disagree.
    fn return_field(&self, input_schema: &Schema) -> DfResult<FieldRef> {
        let empty = RecordBatch::new_empty(Arc::new(input_schema.clone()));
        let (name, array) = self.project(&empty).map_err(external)?;
        let nullable = match &self.projected {
            Projected::Expression(IRExpr::PropAccess { .. }) | Projected::Identity(_) => {
                input_schema
                    .column_with_name(&name)
                    .is_none_or(|(_, field)| field.is_nullable())
            }
            _ => true,
        };
        Ok(Arc::new(Field::new(
            name,
            array.data_type().clone(),
            nullable,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DfResult<ColumnarValue> {
        let (_, array) = self.project(batch).map_err(external)?;
        Ok(ColumnarValue::Array(array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}
