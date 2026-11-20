//! The core read as the planner lowers it: the compiler's shaper
//! ([`omnigraph_compiler::shape::read`]), which the type checker also runs
//! on every read it accepts. A term the type checker accepted that the
//! shaper does not lower is a defect here (`P006`), never a refusal.

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::core::CoreRead;
use omnigraph_compiler::query::codes::P006;
use omnigraph_compiler::shape::ShapeError;
pub use omnigraph_compiler::shape::read::{
    ArmRank, FuseArm, InnerRead, PatternStep, ReadShape, TargetWindow,
};

use crate::error::PlanError;

/// The shape of a checked read, or the planner's defect.
pub fn shape(read: &CoreRead, catalog: &Catalog) -> Result<ReadShape, PlanError> {
    ReadShape::of(read, catalog).map_err(PlanError::from)
}

impl From<ShapeError> for PlanError {
    /// A term the type checker accepted that arrived unlowered, and any
    /// other defect the shaper meets, is the planner's assertion `P006`: an
    /// internal defect, never a refusal.
    fn from(error: ShapeError) -> Self {
        match error {
            ShapeError::Unlowered(u) => PlanError::Internal(format!(
                "{P006}: the type checker accepted a term the planner does not lower: {}",
                u.message()
            )),
            ShapeError::Internal(detail) => {
                PlanError::Internal(format!("{P006}: the shaper met a defect: {detail}"))
            }
            over @ ShapeError::OverBudget { .. } => PlanError::Unsupported(Box::new(
                over.over_budget()
                    .expect("an over-budget shaping is a refusal"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every shaper defect carries `P006`, the planner's assertion code.
    #[test]
    fn every_shaper_defect_names_p006() {
        let PlanError::Internal(message) = PlanError::from(ShapeError::Internal("x".into())) else {
            panic!("an internal defect");
        };
        assert!(message.starts_with(&format!("{P006}:")), "{message}");
    }
}
