//! A core write as the mutation executor runs it: the compiler's write
//! shaper ([`omnigraph_compiler::shape::write`]), which the type checker
//! also runs on every write it accepts, with each group's selection read
//! compiled for the read executor. A term the type checker accepted that the
//! shaper does not lower is the planner's defect (`P006`), never a refusal.

use std::sync::Arc;

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::IRParam;
use omnigraph_compiler::ir::core::CoreWrite;
use omnigraph_compiler::shape::write::WriteShape;
pub(crate) use omnigraph_compiler::shape::write::{
    Assignment, InsertRows, SelectedRows, Selection, WriteOp,
};

use crate::error::Result;
use crate::runtime_cache::CompiledQuery;

/// A checked write's statements, in run order.
#[derive(Debug, Clone)]
pub(crate) struct WritePlan {
    pub name: String,
    pub write_mode: omnigraph_compiler::query::file::MutationWriteMode,
    pub params: Vec<IRParam>,
    pub ops: Vec<WriteOp>,
    /// The groups' selection reads, each run once against the write's
    /// snapshot.
    pub selections: Vec<CompiledQuery>,
}

impl WritePlan {
    pub(crate) fn of(write: &CoreWrite, catalog: &Catalog) -> Result<Self> {
        let shape = WriteShape::of(write, catalog)
            .map_err(|e| super::plan_refusal(omnigraph_planner::PlanError::from(e)))?;
        Ok(Self {
            name: shape.name,
            write_mode: shape.write_mode,
            params: shape.params,
            ops: shape.ops,
            selections: shape
                .selections
                .into_iter()
                .map(|read| CompiledQuery { ir: Arc::new(read) })
                .collect(),
        })
    }
}
