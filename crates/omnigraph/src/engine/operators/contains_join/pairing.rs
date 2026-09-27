//! How a `ContainsJoinExec` pairs its right batches: through the needle rows
//! of its collected left side, or every pair when the pool refuses them.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::common::{DataFusionError, Result as DfResult};

use super::needle_rows::{ContainsColumns, NeedleRows};
use crate::engine::operators::Needles;
use crate::engine::operators::memory::WorkMemory;
use crate::engine::operators::pair_buffer::PairBuffer;
use crate::engine::operators::producer::BatchSender;

/// The pairs the needle rows found for the non-empty needles, before the
/// join's filters; counted only beside a `MATCHER_METRIC` of 1.
pub(super) const PAIRS_METRIC: &str = "contains_join_pairs";
/// 1 when the join paired through its needle rows, 0 when the pool refused
/// them and it tested every pair; absent when no right row reached the join.
pub(super) const MATCHER_METRIC: &str = "contains_join_matcher";

/// The needle rows of the collected left side and the right side's text
/// column they search.
pub(super) struct Matched {
    haystack: String,
    needles: NeedleRows,
}

impl Matched {
    /// `None`, and every pair tested, when the fill left no needles or the
    /// pool refuses the needle rows over them.
    fn build(
        left: &RecordBatch,
        columns: &ContainsColumns,
        needles: Option<Arc<Needles>>,
        memory: &WorkMemory,
    ) -> DfResult<Option<Self>> {
        let Some(needles) = needles else {
            return Ok(None);
        };
        Ok(
            NeedleRows::build(left, &columns.needle, needles, memory)?.map(|needles| Self {
                haystack: columns.haystack.clone(),
                needles,
            }),
        )
    }

    /// Each text of `batch` paired with the left rows the needles find for
    /// it, in right row order, then each empty needle's row with each non-null
    /// text; every pair holds the `contains` conjunct, untested by the buffer.
    async fn pair(
        &mut self,
        batch: &RecordBatch,
        buffer: &mut PairBuffer,
        memory: &WorkMemory,
        sender: &BatchSender,
    ) -> DfResult<()> {
        let Self { haystack, needles } = self;
        let texts = batch
            .column_by_name(haystack)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("contains join column `{haystack}` is not text"))
            })?;
        let right_row = |row: usize| {
            u32::try_from(row).map_err(|_| {
                DataFusionError::Execution("contains join batch exceeds row index range".into())
            })
        };
        let mut found = 0;
        for (row, text) in texts.iter().enumerate() {
            let Some(text) = text else {
                continue;
            };
            let right_row = right_row(row)?;
            for left_row in needles.paired(text, memory)? {
                found += 1;
                buffer.push(left_row, right_row, sender).await?;
            }
        }
        memory.metric(PAIRS_METRIC, found);
        for &left_row in needles.empty() {
            for row in 0..texts.len() {
                if texts.is_valid(row) {
                    buffer.push(left_row, right_row(row)?, sender).await?;
                }
            }
        }
        Ok(())
    }
}

/// How the join pairs its right batches, decided at the first non-empty one
/// over the automaton the fill built, which the right side's scan may still
/// be sieving through.
pub(super) enum Pairing {
    NeedleRows(Box<Matched>),
    EveryPair,
}

impl Pairing {
    /// Through the needle rows when the fill left `needles` and the pool
    /// admits their tables, recorded on `MATCHER_METRIC`; else every pair.
    pub(super) fn decide(
        left: &RecordBatch,
        columns: &ContainsColumns,
        needles: Option<Arc<Needles>>,
        memory: &WorkMemory,
    ) -> DfResult<Self> {
        Ok(match Matched::build(left, columns, needles, memory)? {
            Some(matched) => {
                memory.metric(MATCHER_METRIC, 1);
                memory.metric(PAIRS_METRIC, 0);
                Self::NeedleRows(Box::new(matched))
            }
            None => {
                memory.metric(MATCHER_METRIC, 0);
                Self::EveryPair
            }
        })
    }

    /// The pairs of the started right `batch`, pushed into `buffer`.
    pub(super) async fn pair(
        &mut self,
        batch: &RecordBatch,
        buffer: &mut PairBuffer,
        memory: &WorkMemory,
        sender: &BatchSender,
    ) -> DfResult<()> {
        match self {
            Self::NeedleRows(matched) => matched.pair(batch, buffer, memory, sender).await,
            Self::EveryPair => buffer.push_all(sender).await,
        }
    }
}
