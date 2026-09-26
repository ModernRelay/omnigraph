//! What the engine hands the executor for one routed operation: the pinned
//! dataset of every side, the batch targets, and the hooks through which the
//! engine keeps its own typed errors, probes and Blob comparison.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use lance::Dataset;
use omnigraph_compiler::SystemColumns;
use omnigraph_planner::SideId;

use super::error::{ExecError, Result};

pub type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Fragment stream openings in flight for an unordered scan.
pub const SCAN_FRAGMENT_FANOUT: usize = 8;

#[derive(Debug, Clone, Copy)]
pub struct BatchTargets {
    pub rows: usize,
    pub bytes: u64,
}

#[derive(Clone)]
pub struct SideContext {
    pub side: SideId,
    /// `None` for a side whose snapshot holds no dataset for the table.
    pub dataset: Option<Dataset>,
    /// The side's Arrow schema: the dataset's, or the catalog's for an
    /// absent side, so its null columns are typed.
    pub schema: SchemaRef,
    pub table: String,
    pub role: &'static str,
    pub columns: SystemColumns,
    pub is_edge: bool,
}

/// One row of one side's un-prefixed batch, for the Blob-aware comparison.
pub struct SideRow<'a> {
    pub side: SideId,
    pub batch: &'a RecordBatch,
    pub row: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum Probe {
    /// A scan of one side opened.
    Scan { side: SideId },
    /// One hydration chunk of `rows` rows retaining `bytes`.
    Hydration {
        side: SideId,
        rows: usize,
        bytes: u64,
    },
    /// Probe-side rows a scoped join examined.
    RowsExamined(usize),
    /// The batch targets a scoped scan ran with.
    ScanTargets { rows: usize, bytes: u64 },
}

/// The engine's side of the seam: `rows_equal` is the Blob-aware row
/// comparison the kernels cannot express over descriptors; `probe` feeds the
/// engine's counters.
#[async_trait]
pub trait EngineHooks: Send + Sync {
    async fn rows_equal(&self, left: SideRow<'_>, right: SideRow<'_>) -> Result<bool>;
    fn probe(&self, probe: Probe);
}

#[derive(Clone)]
pub struct ExecContext {
    pub operation: &'static str,
    pub sides: Vec<SideContext>,
    pub targets: BatchTargets,
    pub hydration_target_bytes: u64,
    pub hooks: Arc<dyn EngineHooks>,
}

impl ExecContext {
    pub fn side(&self, side: SideId) -> Result<&SideContext> {
        self.sides
            .iter()
            .find(|context| context.side == side)
            .ok_or_else(|| ExecError::internal(format!("no context for side {side:?}")))
    }
}

pub fn side_name(side: SideId) -> &'static str {
    side.name()
}
