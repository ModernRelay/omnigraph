//! The test-only seam through which a session's reads run on another
//! executor: the engine builds a [`ReadRequest`] where it would plan the read
//! and hands it to the installed [`ReadExecutor`].

use std::future::Future;
use std::pin::Pin;

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{ParamMap, QueryIR};
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::settings::SessionSettings;
use omnigraph_core::error::Result;

use crate::Snapshot;

/// One compiled read query and the read view it runs against.
#[derive(Clone, Copy)]
pub struct ReadRequest<'a> {
    pub ir: &'a QueryIR,
    pub params: &'a ParamMap,
    pub snapshot: &'a Snapshot,
    pub catalog: &'a Catalog,
    pub settings: &'a SessionSettings,
}

/// An executor a session's reads run on in place of the engine's planner.
pub trait ReadExecutor: Send + Sync + std::fmt::Debug {
    /// An `Err` (a refused query shape or an execution failure) is the read's error, unchanged.
    fn execute<'a>(
        &'a self,
        request: ReadRequest<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult>> + Send + 'a>>;
}
