use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
    builder::{
        BooleanBuilder, Date32Builder, Date64Builder, FixedSizeListBuilder, Float32Builder,
        Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, UInt32Builder,
        UInt64Builder,
    },
};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::Dataset;
use lance::blob::BlobArrayBuilder;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{
    IRAssignment, IRExpr, IRFilter, IRMutationPredicate, IROp, IROrdering, IRProjection,
    MutationOpIR, ParamMap, QueryIR,
};
use omnigraph_compiler::lower_mutation_query;
use omnigraph_compiler::lower_query;
use omnigraph_compiler::query::ast::{AggFunc, CompOp, Literal, NOW_PARAM_NAME};
use omnigraph_compiler::query::typecheck::{CheckedQuery, typecheck_query, typecheck_query_decl};
use omnigraph_compiler::result::{MutationResult, QueryResult};
use omnigraph_compiler::types::Direction;
use omnigraph_compiler::types::ScalarType;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::db::commit_graph::CommitGraph;
use crate::db::manifest::ManifestCoordinator;
use crate::db::{MergeOutcome, Omnigraph, is_internal_run_branch};
use crate::db::{ReadTarget, Snapshot};
use crate::embedding::EmbeddingClient;
use crate::error::{MergeConflict, MergeConflictKind, OmniError, Result};
use crate::graph_index::GraphIndex;
use tempfile::{Builder as TempDirBuilder, TempDir};

mod merge;
mod mutation;
mod projection;
mod query;
pub(crate) mod staging;
