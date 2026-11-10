use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
    builder::{
        BooleanBuilder, Date32Builder, Date64Builder, FixedSizeListBuilder, Float32Builder,
        Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, UInt32Builder,
        UInt64Builder,
    },
};
use arrow_schema::{DataType, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::Dataset;
use lance::blob::BlobArrayBuilder;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::core::MutationTarget;
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_compiler::query::value::{Literal, NOW_PARAM_NAME};
use omnigraph_compiler::result::MutationResult;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use write_plan::{Assignment, WriteOp};

use crate::db::Snapshot;
use crate::db::manifest::ManifestCoordinator;
use crate::db::{MergeOutcome, MergeResult, Omnigraph, WriteTxn};
use crate::error::{MergeConflict, MergeConflictKind, OmniError, Result};
use crate::storage_layer::SnapshotHandle;
use tempfile::{Builder as TempDirBuilder, TempDir};

mod blob_write;
pub(crate) mod merge;
pub(crate) mod mutation;
mod query_doors;
pub(crate) mod staging;
pub(crate) mod write_plan;

/// A failure to find a named statement in query text, as an engine error: a
/// compile refusal keeps its diagnostic (code, position, fix) on the read and
/// the mutation door alike, and any other lookup failure is a bad request.
/// A core node the planner does not lower (`P006`) as the compile refusal
/// it is; any other planner failure is internal.
pub(crate) fn plan_refusal(error: omnigraph_planner::PlanError) -> OmniError {
    match error {
        omnigraph_planner::PlanError::Unsupported(diagnostic) => {
            OmniError::Compiler(omnigraph_compiler::error::CompilerError::Query(diagnostic))
        }
        other => OmniError::manifest_internal(format!(
            "the planner built no plan for this query: {other}"
        )),
    }
}

pub(crate) fn query_lookup_error(error: omnigraph_compiler::RunInputError) -> OmniError {
    match error {
        omnigraph_compiler::RunInputError::Core(
            query @ omnigraph_compiler::error::CompilerError::Query(_),
        ) => OmniError::Compiler(query),
        other => OmniError::manifest(other.to_string()),
    }
}
