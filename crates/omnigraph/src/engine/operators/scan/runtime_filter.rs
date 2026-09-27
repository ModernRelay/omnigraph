//! The filter a parent hands the `ScanExec` under it at run time: the lowering
//! creates one `RuntimeFilterSlot` per marked scan, the parent fills it before
//! the scan executes, and the scan takes the `RuntimeFilter` and sieves each batch.

use std::collections::BTreeSet;
use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Mutex, PoisonError};

use aho_corasick::AhoCorasick;
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, UInt32Array};
use datafusion::common::{DataFusionError, Result as DfResult};

use crate::engine::operators::memory::WorkMemory;
use crate::engine::operators::text_match::{SET_ENTRY_BYTES, build, build_peak_bytes};

/// The pool owner of the matcher's charge.
const NEEDLES: &str = "runtime filter needles";

/// The row width up to which `lance_batch_rows` keeps one decode in its bytes.
const DECODED_ROW_BYTES: usize = 4096;

/// Rows per Lance read of a pipelined scan, filter or none: Lance 11 decodes the
/// lesser of this and the byte target over its row estimate (64 bytes a text
/// until a batch is measured). `LANCE_DEFAULT_BATCH_SIZE` replaces this count.
pub(in crate::engine) fn lance_batch_rows(batch_bytes: usize) -> usize {
    (batch_bytes / DECODED_ROW_BYTES).clamp(1, 8192)
}

const ROWS_READ: &str = "runtime_filter_rows_read";
const ROWS_DROPPED: &str = "runtime_filter_rows_dropped";
const INERT: &str = "runtime_filter_inert";

/// What `RuntimeFilterSlot::fill` left for the scan and the join.
pub(in crate::engine::operators) enum Filled {
    /// The scan sieves through these needles and the join pairs through them.
    Needles(Arc<Needles>),
    /// Every left value is null, so no pair can pass the conjunct.
    NoNeedles,
    /// The scan reads every row, as it does without a filter: a value is
    /// empty (the join still pairs through the other values' needles) or the
    /// pool refused them (`None`, and the join tests every pair).
    Inert(Option<Arc<Needles>>),
}

/// The test a pipelined scan runs on each Lance batch it holds as `runtime
/// filter input`: sieved, the kept copy admitted before it is built, then the
/// input released. Closed: the scan matches on the kind.
pub(crate) enum RuntimeFilter {
    /// The scan's `column` holds at least one of the needles.
    TextContainsAny {
        column: String,
        needles: Arc<Needles>,
    },
}

impl RuntimeFilter {
    /// Whether each row of `batch` passes, a null row not; `None` when the
    /// batch has no column the filter can test, so it is kept whole.
    fn sieve(&self, batch: &RecordBatch) -> Option<BooleanArray> {
        match self {
            Self::TextContainsAny { column, needles } => {
                let text = batch
                    .column_by_name(column)
                    .and_then(|column| column.as_any().downcast_ref::<StringArray>())?;
                Some(
                    text.iter()
                        .map(|value| Some(value.is_some_and(|value| needles.is_match(value))))
                        .collect(),
                )
            }
        }
    }

    /// The rows of `batch` that pass, counted on `work`: a mixed selection is
    /// copied only once `work` admits the copy under `owner`; a batch the
    /// filter cannot test is kept whole and marks the scan inert once.
    pub(super) fn keep(
        &self,
        batch: &RecordBatch,
        work: &WorkMemory,
        owner: &str,
        inert: &mut bool,
    ) -> DfResult<RecordBatch> {
        work.grow(batch.num_rows().div_ceil(8))?;
        let kept = match self.sieve(batch) {
            Some(mask) => match mask.true_count() {
                0 => batch.slice(0, 0),
                selected if selected == batch.num_rows() => batch.clone(),
                selected => {
                    work.entries::<u32>(selected)?;
                    let indices = UInt32Array::from_iter_values(
                        mask.iter()
                            .enumerate()
                            .filter_map(|(row, kept)| (kept == Some(true)).then_some(row as u32)),
                    );
                    work.take_once(batch, &indices, owner)?
                }
            },
            None => {
                if !*inert {
                    *inert = true;
                    work.metric(INERT, 1);
                }
                batch.clone()
            }
        };
        work.metric(ROWS_READ, batch.num_rows());
        work.metric(ROWS_DROPPED, batch.num_rows() - kept.num_rows());
        Ok(kept)
    }
}

/// The slot one parent operator shares with the `ScanExec` under it: the
/// parent fills it once, the scan takes the filter once.
pub(crate) struct RuntimeFilterSlot {
    binding: String,
    column: String,
    needle: String,
    slot: Mutex<Option<RuntimeFilter>>,
}

impl fmt::Debug for RuntimeFilterSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeFilterSlot")
            .field("display", &self.display())
            .finish_non_exhaustive()
    }
}

impl RuntimeFilterSlot {
    /// A `TextContainsAny` filter on the column `property` of the scan bound
    /// to `binding`, by the left side's column `needle` (`$l.y` as `l.y`).
    pub(crate) fn new(binding: &str, property: &str, needle: String) -> Self {
        Self {
            binding: binding.to_string(),
            column: property.to_string(),
            needle,
            slot: Mutex::new(None),
        }
    }

    /// `$r.x contains any($l.y)`, as explain prints the filter.
    pub(crate) fn display(&self) -> String {
        format!(
            "${}.{} contains any(${})",
            self.binding, self.column, self.needle
        )
    }

    /// The scan column the filter tests.
    pub(crate) fn column(&self) -> &str {
        &self.column
    }

    /// The left column, `l.y`, the needles are read from.
    pub(crate) fn needle(&self) -> &str {
        &self.needle
    }

    /// The one automaton over the distinct non-empty needle values of `left`,
    /// charged to `memory`, as `Filled` hands it to the scan and the join. An
    /// earlier fill's filter is dropped first, whatever this one leaves.
    pub(in crate::engine::operators) fn fill(
        &self,
        left: &RecordBatch,
        memory: &WorkMemory,
    ) -> DfResult<Filled> {
        drop(self.take());
        let Some(values) = left
            .column_by_name(&self.needle)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        else {
            return Ok(Filled::Inert(None));
        };
        if u32::try_from(values.len()).is_err() {
            return Ok(Filled::Inert(None));
        }
        let charge = memory.child(NEEDLES)?;
        let mut needles = BTreeSet::new();
        let mut empty = false;
        for needle in values.iter().flatten() {
            if needle.is_empty() {
                empty = true;
                continue;
            }
            let entry = (needle.len(), needle);
            if needles.contains(&entry) {
                continue;
            }
            if !charge.grow_if_free(SET_ENTRY_BYTES)? {
                return Ok(Filled::Inert(None));
            }
            needles.insert(entry);
        }
        if needles.is_empty() && !empty {
            return Ok(Filled::NoNeedles);
        }
        let ordered: Vec<(usize, &str)> = needles.into_iter().collect();
        let matcher = if ordered.is_empty() {
            None
        } else {
            if !charge.grow_if_free(build_peak_bytes(&ordered))? {
                return Ok(Filled::Inert(None));
            }
            let Some(matcher) = build(&ordered) else {
                return Ok(Filled::Inert(None));
            };
            Some(matcher)
        };
        let mut first_rows = vec![u32::MAX; ordered.len()];
        for (row, value) in values.iter().enumerate() {
            let Some(value) = value.filter(|value| !value.is_empty()) else {
                continue;
            };
            let pattern = ordered
                .binary_search(&(value.len(), value))
                .map_err(|_| DataFusionError::Internal("a needle outside its set".into()))?;
            if first_rows[pattern] == u32::MAX {
                first_rows[pattern] = row as u32;
            }
        }
        drop(ordered);
        let needles = Arc::new(Needles {
            matcher,
            first_rows,
            _charge: charge,
        });
        needles._charge.shrink_to(needles.memory_usage());
        if empty {
            return Ok(Filled::Inert(Some(needles)));
        }
        *self.slot.lock().unwrap_or_else(PoisonError::into_inner) =
            Some(RuntimeFilter::TextContainsAny {
                column: self.column.clone(),
                needles: Arc::clone(&needles),
            });
        Ok(Filled::Needles(needles))
    }

    /// The filter `fill` left, taken, so a later execution without a fresh
    /// fill reads every row and never an earlier execution's needles.
    pub(super) fn take(&self) -> Option<RuntimeFilter> {
        self.slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// The scan's counters at zero, and `runtime_filter_inert` set when its
/// parent had rows but left no filter.
pub(super) fn scan_counters(memory: &WorkMemory, filtered: bool) {
    memory.metric(ROWS_READ, 0);
    memory.metric(ROWS_DROPPED, 0);
    memory.metric(INERT, usize::from(!filtered));
}

/// `batch` read whole by a scan its parent left no filter.
pub(super) fn count_unsieved(memory: &WorkMemory, batch: &RecordBatch) {
    memory.metric(ROWS_READ, batch.num_rows());
}

/// One fill's automaton over its distinct non-empty needles in `(len, text)`
/// order, shared by the scan's sieve and the join's needle rows, with the
/// left row each pattern was first read from; its charge covers the
/// automaton's tables and `first_rows`, not its fixed-size headers.
pub(crate) struct Needles {
    matcher: Option<AhoCorasick>,
    first_rows: Vec<u32>,
    _charge: WorkMemory,
}

impl Needles {
    fn is_match(&self, text: &str) -> bool {
        self.matcher
            .as_ref()
            .is_some_and(|matcher| matcher.is_match(text))
    }

    /// The distinct non-empty needles.
    pub(in crate::engine::operators) fn len(&self) -> usize {
        self.first_rows.len()
    }

    /// The automaton, `None` when every needle was empty.
    pub(in crate::engine::operators) fn matcher(&self) -> Option<&AhoCorasick> {
        self.matcher.as_ref()
    }

    /// Pattern `p`'s first left row at `p`, in the automaton's pattern order.
    pub(in crate::engine::operators) fn first_rows(&self) -> &[u32] {
        &self.first_rows
    }

    /// The bytes the fill keeps charged for these needles.
    pub(in crate::engine::operators) fn memory_usage(&self) -> usize {
        self.matcher
            .as_ref()
            .map_or(0, AhoCorasick::memory_usage)
            .saturating_add(self.first_rows.len().saturating_mul(size_of::<u32>()))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use arrow_array::builder::StringBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::execution::context::SessionConfig;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    use super::*;
    use crate::engine::operators::memory::QueryResources;
    use crate::instrumentation::{QueryMemoryProbes, with_query_memory_probes};

    /// A join's work memory under a `limit`-byte pool, with its metrics.
    fn work(limit: usize) -> (Arc<dyn MemoryPool>, WorkMemory, ExecutionPlanMetricsSet) {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let resources = Arc::new(QueryResources::new(Arc::clone(&pool), limit as u64));
        let ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_extension(resources)),
        );
        let mut memory = WorkMemory::new(ctx, "test join").unwrap();
        let metrics = ExecutionPlanMetricsSet::new();
        memory.set_metrics(metrics.clone());
        (pool, memory, metrics)
    }

    fn left(numbers: Vec<Option<&str>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "m.number",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(numbers))],
        )
        .unwrap()
    }

    fn filter() -> RuntimeFilterSlot {
        RuntimeFilterSlot::new("p", "text", "m.number".to_string())
    }

    /// The `TextContainsAny` filter of a filled slot.
    fn needles(filter: &RuntimeFilterSlot) -> Arc<Needles> {
        let RuntimeFilter::TextContainsAny { needles, .. } = filter.take().unwrap();
        needles
    }

    /// The sum of counter `name` on `metrics`.
    fn count(metrics: &ExecutionPlanMetricsSet, name: &str) -> Option<usize> {
        metrics
            .clone_inner()
            .sum_by_name(name)
            .map(|value| value.as_usize())
    }

    /// After the build only the matcher's own size and the patterns' first
    /// rows stay charged, dropping the needles gives that back, and an empty
    /// value leaves the scan unfiltered and the join the other values' needles.
    #[test]
    fn a_filled_matcher_keeps_only_its_own_size_charged() {
        let (pool, memory, _) = work(1 << 20);
        let filter = filter();
        let filled = filter
            .fill(&left(vec![Some("cd"), Some("ab"), Some("cd")]), &memory)
            .unwrap();
        let Filled::Needles(shared) = filled else {
            panic!("two needles fill the scan's filter");
        };
        let needles = needles(&filter);
        assert!(
            Arc::ptr_eq(&shared, &needles),
            "the join and the scan share one automaton"
        );
        assert_eq!(needles.len(), 2);
        assert_eq!(needles.first_rows(), [1, 0], "`ab` sorts before `cd`");
        assert_eq!(pool.reserved(), needles.memory_usage());
        assert_eq!(
            pool.reserved(),
            needles.matcher().unwrap().memory_usage() + 2 * size_of::<u32>()
        );
        drop((shared, needles));
        assert_eq!(pool.reserved(), 0);
        let filled = filter
            .fill(&left(vec![Some("ab"), Some(""), Some("cd")]), &memory)
            .unwrap();
        let Filled::Inert(Some(needles)) = filled else {
            panic!("the join pairs through `ab` and `cd`");
        };
        assert!(filter.take().is_none(), "an empty value is in every text");
        assert_eq!(needles.len(), 2);
        assert_eq!(pool.reserved(), needles.memory_usage());
        drop(needles);
        assert_eq!(pool.reserved(), 0);
    }

    /// The pool fits the first needle's set entry but not the second's: the
    /// scan goes unfiltered, nothing stays charged, and no refusal is recorded.
    #[tokio::test]
    async fn a_needle_the_pool_refuses_leaves_the_scan_unfiltered_with_no_refusal() {
        let probes = QueryMemoryProbes::default();
        with_query_memory_probes(probes.clone(), async {
            let (pool, memory, _) = work(100);
            let filter = filter();
            let filled = filter
                .fill(&left(vec![Some("ab"), Some("cd")]), &memory)
                .unwrap();
            assert!(matches!(filled, Filled::Inert(None)));
            assert!(filter.take().is_none());
            assert_eq!(pool.reserved(), 0);
        })
        .await;
        assert!(probes.refusals().is_empty(), "{:?}", probes.refusals());
    }

    /// The two needles fit a 16 KiB pool; the build's charge does not.
    #[test]
    fn a_build_the_pool_refuses_leaves_the_scan_unfiltered_and_charges_nothing() {
        let (pool, memory, _) = work(16 << 10);
        let filter = filter();
        let filled = filter
            .fill(&left(vec![Some("ab"), Some("cd")]), &memory)
            .unwrap();
        assert!(matches!(filled, Filled::Inert(None)));
        assert!(filter.take().is_none());
        assert_eq!(pool.reserved(), 0);
    }

    /// A filter over needle `ab`, taken from its filled slot, under a
    /// `limit`-byte pool.
    fn filled(
        limit: usize,
    ) -> (
        Arc<dyn MemoryPool>,
        RuntimeFilter,
        WorkMemory,
        ExecutionPlanMetricsSet,
    ) {
        let (pool, memory, metrics) = work(limit);
        let filter = filter();
        let filled = filter.fill(&left(vec![Some("ab")]), &memory).unwrap();
        assert!(matches!(filled, Filled::Needles(_)));
        (pool, filter.take().unwrap(), memory, metrics)
    }

    /// The rows `batch` keeps twice over, as a scan keeps two Lance batches.
    fn kept_twice(filter: &RuntimeFilter, memory: &WorkMemory, batch: &RecordBatch) -> usize {
        let mut inert = false;
        (0..2)
            .map(|_| {
                filter
                    .keep(batch, memory, "test batch", &mut inert)
                    .unwrap()
                    .num_rows()
            })
            .sum()
    }

    /// The sieve has no verdict on a batch whose haystack column is not text
    /// or is missing, so the scan keeps every row of it and counts itself
    /// inert once.
    #[test]
    fn a_batch_the_sieve_cannot_test_keeps_every_row_and_marks_the_scan_inert_once() {
        let numbers = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "text",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["ab", "cd"]))],
        )
        .unwrap();
        for (batch, rows) in [(numbers, 3), (other, 2)] {
            let (_, filter, memory, metrics) = filled(1 << 20);
            assert!(filter.sieve(&batch).is_none());
            assert_eq!(kept_twice(&filter, &memory, &batch), 2 * rows);
            assert_eq!(
                count(&metrics, INERT),
                Some(1),
                "two batches, one inert scan"
            );
            assert_eq!(count(&metrics, ROWS_READ), Some(2 * rows));
            assert_eq!(count(&metrics, ROWS_DROPPED), Some(0));
        }
    }

    /// One `text` column of `values`, its buffers sized to the bytes they
    /// hold, so a hold of the batch charges those bytes and no growth slack.
    fn texts(values: Vec<Option<&str>>) -> RecordBatch {
        let bytes = values.iter().flatten().map(|value| value.len()).sum();
        let mut column = StringBuilder::with_capacity(values.len(), bytes);
        for value in values {
            column.append_option(value);
        }
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
            vec![Arc::new(column.finish())],
        )
        .unwrap()
    }

    /// A 56 KiB row beside a rejected one under a 96 KiB pool that holds the
    /// batch but not a copy of the row: `keep` refuses under the batch's owner
    /// before the copy exists; a batch kept whole is itself and charges no copy.
    #[test]
    fn a_mixed_selection_is_admitted_before_it_is_copied() {
        let (pool, filter, memory, _) = filled(96 << 10);
        let wide = format!("{}ab", "x".repeat(56 << 10));
        let batch = texts(vec![Some(&wide), Some("cd")]);
        let in_flight = memory.child("runtime filter input").unwrap();
        in_flight.hold(&batch).unwrap();
        let work = memory.child("v2 scan batch").unwrap();
        let mut inert = false;
        let refused = filter
            .keep(&batch, &work, "v2 scan batch", &mut inert)
            .unwrap_err();
        assert!(
            matches!(refused, DataFusionError::ResourcesExhausted(_)),
            "{refused}"
        );
        assert!(refused.to_string().contains("v2 scan batch"), "{refused}");
        drop(work);
        let whole = texts(vec![Some(&wide), Some("ab")]);
        let work = memory.child("v2 scan batch").unwrap();
        let before = pool.reserved();
        let kept = filter
            .keep(&whole, &work, "v2 scan batch", &mut inert)
            .unwrap();
        assert_eq!(kept.num_rows(), 2);
        assert_eq!(
            pool.reserved() - before,
            1,
            "one mask byte, no copy: the kept batch is the batch"
        );
    }
}
