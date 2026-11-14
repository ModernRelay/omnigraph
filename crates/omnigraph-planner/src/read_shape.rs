//! The core read as the planner lowers it: the compiler's shaper
//! ([`omnigraph_compiler::shape::read`]), which the type checker also runs
//! on every read it accepts. A term the type checker accepted that the
//! shaper does not lower is a defect here (`P006`), never a refusal.

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::core::CoreRead;
use omnigraph_compiler::query::codes::P006;
use omnigraph_compiler::shape::ShapeError;
pub use omnigraph_compiler::shape::read::{InnerRead, PatternStep, ReadShape};

use crate::error::PlanError;

/// The shape of a checked read, or the planner's defect.
pub fn shape(read: &CoreRead, catalog: &Catalog) -> Result<ReadShape, PlanError> {
    ReadShape::of(read, catalog).map_err(PlanError::from)
}

impl From<ShapeError> for PlanError {
    /// A term the type checker accepted that arrived unlowered is the
    /// planner's assertion `P006`: an internal defect.
    fn from(error: ShapeError) -> Self {
        match error {
            ShapeError::Unlowered(u) => PlanError::Internal(format!(
                "{P006}: the type checker accepted a term the planner does not lower: {}",
                u.message()
            )),
            ShapeError::Internal(detail) => PlanError::Internal(detail),
        }
    }
}
