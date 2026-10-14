//! GQ return expressions as DataFusion `PhysicalExpr`s over the wide batch.
//! Two adapters are equal when they print the same GQ text and belong to the
//! same lowering.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::{Field, FieldRef, Schema};
use datafusion::common::Result as DfResult;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_compiler::types::ExprType;

use super::expr::{ProjectionContext, evaluate_projection, identity_column};
use super::operators::external;
use crate::error::Result;

static NEXT_LOWERING: AtomicU64 = AtomicU64::new(0);

/// The id a lowering stamps on every adapter it builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct LoweringId(u64);

impl LoweringId {
    pub(super) fn next() -> Self {
        Self(NEXT_LOWERING.fetch_add(1, Ordering::Relaxed))
    }
}

/// What a return expression projects: the expression itself, or the
/// binding's identity column for `count($v)`.
#[derive(Debug, Clone)]
pub(super) enum Projected {
    Expression(IRExpr, ExprType),
    Identity(String),
}

impl Projected {
    fn text(&self) -> String {
        match self {
            Self::Identity(variable) => format!("count(${variable})"),
            Self::Expression(expr, _) => expr.to_string(),
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
            Projected::Expression(expr, _) => {
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
    /// The compiler's stored projection type, independent of bound values and
    /// of whether the input happens to contain any rows.
    fn return_field(&self, input_schema: &Schema) -> DfResult<FieldRef> {
        let field = match &self.projected {
            Projected::Identity(variable) => {
                Field::new(variable, arrow_schema::DataType::Utf8, false)
            }
            Projected::Expression(_, ty) => {
                self.ctx.declared_field(&self.text, ty).map_err(external)?
            }
        };
        if let Projected::Expression(
            IRExpr::PropAccess {
                variable,
                property,
                ty: _,
            },
            _,
        ) = &self.projected
        {
            let name = format!("{variable}.{property}");
            let actual = input_schema
                .field_with_name(&name)
                .map_err(|error| external(crate::error::OmniError::arrow_internal(error)))?;
            if actual.data_type() != field.data_type() {
                return Err(external(crate::error::OmniError::manifest_internal(
                    format!("property {name} disagrees with its input field"),
                )));
            }
        }
        Ok(Arc::new(field))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DfResult<ColumnarValue> {
        let (_, array) = self.project(batch).map_err(external)?;
        let expected = self.return_field(batch.schema().as_ref())?;
        if array.data_type() != expected.data_type()
            || !expected.is_nullable() && array.null_count() != 0
        {
            return Err(external(crate::error::OmniError::manifest_internal(
                format!(
                    "projection {} violates declared field {expected:?}",
                    self.text
                ),
            )));
        }
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
