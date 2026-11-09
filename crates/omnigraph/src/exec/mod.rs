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
use omnigraph_compiler::ir::{IRAssignment, IRExpr, MutationOpIR, ParamMap};
use omnigraph_compiler::lower_mutation_query;
use omnigraph_compiler::query::ast::{Literal, NOW_PARAM_NAME};
use omnigraph_compiler::query::typecheck::{CheckedQuery, MutationTarget, typecheck_query_decl};
use omnigraph_compiler::result::MutationResult;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

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

/// A failure to find a named statement in query text, as an engine error: a
/// compile refusal keeps its diagnostic (code, position, fix) on the read and
/// the mutation door alike, and any other lookup failure is a bad request.
/// GQ edition 3 (`gq 3;`) is checked and does not run in this release: the
/// declaration is type-checked against `catalog`, so a type error answers
/// first, and the planner then refuses it with `P006`. Nothing here reads a
/// snapshot or storage.
pub(crate) fn edition3_refusal(
    catalog: &Catalog,
    decl: &omnigraph_compiler::query::ast::QueryDecl,
) -> OmniError {
    match typecheck_query_decl(catalog, decl) {
        Err(error) => OmniError::Compiler(error),
        Ok(CheckedQuery::Edition3(core)) => match omnigraph_planner::plan_edition3(&core) {
            omnigraph_planner::PlanError::Unsupported(diagnostic) => {
                OmniError::Compiler(omnigraph_compiler::error::CompilerError::Query(diagnostic))
            }
            other => OmniError::manifest_internal(format!(
                "the planner built no plan for this query: {other}"
            )),
        },
        Ok(CheckedQuery::Read(_) | CheckedQuery::Mutation(_)) => OmniError::manifest_internal(
            "an edition-3 declaration was checked as edition 2".to_string(),
        ),
    }
}

impl Omnigraph {
    /// The door check every read and write takes before it captures a
    /// snapshot: a source that opens with `gq 3;` has its named declaration
    /// refused through [`edition3_refusal`] against the handle's accepted
    /// catalog. Any other source returns at once, having read only its
    /// leading whitespace and comments; a missing name or a statement is left
    /// to the door's own lookup.
    pub(crate) fn refuse_edition3(&self, source: &str, name: &str) -> Result<()> {
        use omnigraph_compiler::query::ast::FileBody;
        use omnigraph_compiler::query::parser3::{EditionLine, edition_line};
        if !matches!(
            edition_line(source),
            EditionLine::Present { edition: 3, .. }
        ) {
            return Ok(());
        }
        let file =
            omnigraph_compiler::query::parser::parse_query(source).map_err(OmniError::Compiler)?;
        let decls = match file.body {
            FileBody::Queries(decls) => decls,
            FileBody::Explain(decl) => vec![decl],
            _ => return Ok(()),
        };
        match decls.iter().find(|decl| decl.name == name) {
            Some(decl) => Err(edition3_refusal(&self.catalog(), decl)),
            None => Ok(()),
        }
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
