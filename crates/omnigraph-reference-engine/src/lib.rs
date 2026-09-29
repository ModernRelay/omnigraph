//! Engine v1, the executor omnigraph ran before the plan runner, kept frozen
//! as the reference engine GQT compares engine v2 against. Reachable only
//! through [`ReferenceEngine`], a `ReadExecutor` a test session installs.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
    builder::{
        BooleanBuilder, Date32Builder, Date64Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, ListBuilder, StringBuilder, UInt32Builder, UInt64Builder,
    },
};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use omnigraph_catalog::Snapshot;
use omnigraph_catalog::read_executor::{ReadExecutor, ReadRequest};
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, IROp, IROrdering, IRProjection, ParamMap, QueryIR};
use omnigraph_compiler::query::ast::{AggFunc, CompOp, Literal};
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::types::Direction;
use omnigraph_compiler::types::ScalarType;
use omnigraph_core::error::{OmniError, Result};

use crate::graph_index::GraphIndex;

mod gate;
mod graph_index;
mod instrumentation;
mod loader;
mod projection;
mod query;
mod table_store;

/// Engine v1 as a [`ReadExecutor`]: the v1 gate, then v1's `execute_query`
/// over a graph index built from the request's snapshot.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReferenceEngine;

impl ReadExecutor for ReferenceEngine {
    fn execute<'a>(
        &'a self,
        request: ReadRequest<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult>> + Send + 'a>> {
        Box::pin(async move {
            let ReadRequest {
                ir,
                params,
                snapshot,
                catalog,
                settings,
            } = request;
            if let Some(refusal) = gate::v1_refusal(ir) {
                return Err(refusal.error());
            }
            let needs_graph = ir
                .pipeline
                .iter()
                .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
            let graph_index = if needs_graph {
                query::GraphIndexHandle::direct(
                    snapshot,
                    query::referenced_edge_types(&ir.pipeline, catalog),
                    catalog.system_columns,
                )
            } else {
                query::GraphIndexHandle::none()
            };
            query::execute_query(ir, params, snapshot, &graph_index, catalog, settings).await
        })
    }
}
