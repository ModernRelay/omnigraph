//! The needle rows a `ContainsJoinExec` pairs through, after arXiv 2608.23307's
//! one-pass Aho-Corasick pairing for substring joins: `ContainsColumns` names the
//! two text columns; `NeedleRows` maps each pattern of the fill's shared
//! automaton to its left rows, the empty needle's rows apart.

use std::mem::size_of;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};
use datafusion::common::{DataFusionError, Result as DfResult};

use crate::engine::operators::Needles;
use crate::engine::operators::memory::WorkMemory;
use crate::error::{OmniError, Result};

/// The pool owner of the needle rows' charge.
const NEEDLE_ROWS: &str = "contains join needles";

/// Occurrence visits of one text between two cancellation checks.
const CANCEL_CHECK_VISITS: usize = 4096;

/// The columns the join pairs by, both text as the join's sides carry them:
/// the right side's `haystack` and the left side's `needle`.
#[derive(Debug, Clone)]
pub(super) struct ContainsColumns {
    pub(super) haystack: String,
    pub(super) needle: String,
}

impl ContainsColumns {
    /// `haystack` of `right` and `needle` of `left`, refused when either
    /// side does not carry its column as text: the plan promised both.
    pub(super) fn checked(
        haystack: String,
        needle: String,
        left: &Schema,
        right: &Schema,
    ) -> Result<Self> {
        let text = |schema: &Schema, name: &str| {
            schema
                .field_with_name(name)
                .is_ok_and(|field| field.data_type() == &DataType::Utf8)
        };
        if !text(right, &haystack) || !text(left, &needle) {
            return Err(OmniError::manifest_internal(format!(
                "a contains join over `{haystack}` and `{needle}` needs both columns as text"
            )));
        }
        Ok(Self { haystack, needle })
    }
}

/// The left rows of each pattern of the shared `needles`, in its pattern
/// order: pattern `p`'s rows are `rows[starts[p]..starts[p + 1]]`.
pub(super) struct NeedleRows {
    needles: Arc<Needles>,
    starts: Vec<u32>,
    rows: Vec<u32>,
    empty: Vec<u32>,
    seen: Vec<bool>,
    found: Vec<usize>,
    #[cfg(test)]
    visits: usize,
    _charge: WorkMemory,
}

impl NeedleRows {
    /// The rows of `left`'s column `needle` under each pattern of the fill's
    /// `needles`, the tables charged to `memory`; `None` when the column is
    /// not text, its rows exceed `u32` indices, or the pool refuses the tables.
    /// `left` must be the batch the fill read, whose rows `first_rows` index.
    pub(super) fn build(
        left: &RecordBatch,
        needle: &str,
        needles: Arc<Needles>,
        memory: &WorkMemory,
    ) -> DfResult<Option<Self>> {
        let Some(values) = left
            .column_by_name(needle)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        else {
            return Ok(None);
        };
        if u32::try_from(values.len()).is_err() {
            return Ok(None);
        }
        let charge = memory.child(NEEDLE_ROWS)?;
        let patterns = needles.len();
        let (mut keyed, mut empties) = (0usize, 0usize);
        for value in values.iter().flatten() {
            if value.is_empty() {
                empties += 1;
            } else {
                keyed += 1;
            }
        }
        let kept_bytes = keyed
            .saturating_add(empties)
            .saturating_add(patterns)
            .saturating_add(1)
            .saturating_mul(size_of::<u32>())
            .saturating_add(patterns.saturating_mul(size_of::<bool>() + size_of::<usize>()));
        let build_bytes = keyed.saturating_mul(size_of::<(u32, u32)>());
        if !charge.grow_if_free(kept_bytes.saturating_add(build_bytes))? {
            return Ok(None);
        }
        let first_rows = needles.first_rows();
        debug_assert!(
            first_rows.iter().all(|&row| (row as usize) < values.len()),
            "needle rows over a left side other than the fill's"
        );
        let key = |row: u32| {
            let value = values.value(row as usize);
            (value.len(), value)
        };
        let mut by_pattern: Vec<(u32, u32)> = Vec::with_capacity(keyed);
        let mut empty = Vec::with_capacity(empties);
        let unlisted = || DataFusionError::Internal("a needle row outside its needle set".into());
        for (row, value) in values.iter().enumerate() {
            let Some(value) = value else {
                continue;
            };
            let row = u32::try_from(row).map_err(|_| unlisted())?;
            if value.is_empty() {
                empty.push(row);
                continue;
            }
            let pattern = first_rows
                .binary_search_by(|&first| key(first).cmp(&(value.len(), value)))
                .map_err(|_| unlisted())?;
            by_pattern.push((u32::try_from(pattern).map_err(|_| unlisted())?, row));
        }
        by_pattern.sort_unstable();
        let mut starts = vec![0u32; patterns + 1];
        for &(pattern, _) in &by_pattern {
            starts[pattern as usize + 1] += 1;
        }
        for pattern in 0..patterns {
            starts[pattern + 1] += starts[pattern];
        }
        let rows: Vec<u32> = by_pattern.iter().map(|&(_, row)| row).collect();
        drop(by_pattern);
        charge.shrink_to(kept_bytes);
        Ok(Some(Self {
            needles,
            starts,
            rows,
            empty,
            seen: vec![false; patterns],
            found: Vec::with_capacity(patterns),
            #[cfg(test)]
            visits: 0,
            _charge: charge,
        }))
    }

    /// The left rows whose needle is empty.
    pub(super) fn empty(&self) -> &[u32] {
        &self.empty
    }

    /// The left rows of each distinct non-empty needle `text` holds. The pass
    /// over `text` stops once every pattern is found and checks `memory` for
    /// cancellation every `CANCEL_CHECK_VISITS` occurrences.
    pub(super) fn paired(
        &mut self,
        text: &str,
        memory: &WorkMemory,
    ) -> DfResult<impl Iterator<Item = u32> + '_> {
        self.found.clear();
        let searched = self.search(text, memory);
        for &pattern in &self.found {
            self.seen[pattern] = false;
        }
        searched?;
        let Self {
            found,
            starts,
            rows,
            ..
        } = &*self;
        Ok(found
            .iter()
            .flat_map(|&pattern| &rows[starts[pattern] as usize..starts[pattern + 1] as usize])
            .copied())
    }

    /// `found` filled with the patterns `text` holds, each once, `seen` set
    /// for each of them.
    fn search(&mut self, text: &str, memory: &WorkMemory) -> DfResult<()> {
        let Some(matcher) = self.needles.matcher() else {
            return Ok(());
        };
        let patterns = self.seen.len();
        let mut visits = 0usize;
        let mut checked = Ok(());
        for found in matcher.find_overlapping_iter(text) {
            visits += 1;
            if visits.is_multiple_of(CANCEL_CHECK_VISITS) {
                checked = memory.check();
                if checked.is_err() {
                    break;
                }
            }
            let pattern = found.pattern().as_usize();
            if !std::mem::replace(&mut self.seen[pattern], true) {
                self.found.push(pattern);
                if self.found.len() == patterns {
                    break;
                }
            }
        }
        #[cfg(test)]
        {
            self.visits += visits;
        }
        checked
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;
    use datafusion::execution::TaskContext;
    use datafusion::execution::context::SessionConfig;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

    use super::*;
    use crate::engine::operators::memory::QueryResources;
    use crate::engine::operators::{Filled, RuntimeFilterSlot};

    fn work(limit: usize) -> (Arc<dyn MemoryPool>, WorkMemory) {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let resources = Arc::new(QueryResources::new(Arc::clone(&pool), limit as u64));
        let ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_extension(resources)),
        );
        (pool, WorkMemory::new(ctx, "test join").unwrap())
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

    /// The slot a fill over `left` left the scan's filter in, still holding
    /// it, and the shared needles.
    fn filled(left: &RecordBatch, memory: &WorkMemory) -> (RuntimeFilterSlot, Arc<Needles>) {
        let slot = RuntimeFilterSlot::new("p", "text", "m.number".to_string());
        let Filled::Needles(needles) = slot.fill(left, memory).unwrap() else {
            panic!("the fill leaves the scan its needles");
        };
        (slot, needles)
    }

    /// The needle rows of `a` to `a` 64 times, and `b` when `absent`, with
    /// the scan's filter held alive beside them.
    fn nested(absent: bool) -> (WorkMemory, RuntimeFilterSlot, NeedleRows) {
        let mut numbers: Vec<String> = (1..=64).map(|len| "a".repeat(len)).collect();
        if absent {
            numbers.push("b".to_string());
        }
        let left = left(numbers.iter().map(|number| Some(number.as_str())).collect());
        let (_, memory) = work(64 << 20);
        let (slot, needles) = filled(&left, &memory);
        let rows = NeedleRows::build(&left, "m.number", needles, &memory)
            .unwrap()
            .unwrap();
        (memory, slot, rows)
    }

    /// The scan's filter and the join's needle rows share one automaton: with
    /// both alive the pool holds its size once, plus the row tables, and the
    /// tables go with the rows.
    #[test]
    fn needle_rows_share_the_fills_matcher_and_charge_only_their_tables() {
        let (pool, memory) = work(1 << 20);
        let left = left(vec![Some("ab"), Some("cd"), Some("ab"), None]);
        let (slot, needles) = filled(&left, &memory);
        let shared = needles.memory_usage();
        assert_eq!(pool.reserved(), shared);
        let rows = NeedleRows::build(&left, "m.number", Arc::clone(&needles), &memory)
            .unwrap()
            .unwrap();
        assert_eq!(
            Arc::strong_count(&needles),
            3,
            "the scan's slot, the needle rows and this test"
        );
        assert_eq!(
            pool.reserved() - shared,
            6 * size_of::<u32>() + 2 * (size_of::<bool>() + size_of::<usize>()),
            "three rows and three starts, and a seen flag and a found slot per needle"
        );
        assert_eq!(rows.starts, [0, 2, 3], "`ab` has two rows, `cd` one");
        assert_eq!(rows.rows, [0, 2, 1]);
        drop(rows);
        assert_eq!(pool.reserved(), shared);
        drop((slot, needles));
        assert_eq!(pool.reserved(), 0);
    }

    /// The needles fit; a 16-byte pool for the join refuses the 54 bytes of
    /// row tables, so no needle rows are built and the join's charge stays
    /// at zero.
    #[test]
    fn a_build_the_pool_refuses_leaves_no_needle_rows_and_charges_nothing() {
        let (_, fill_memory) = work(1 << 20);
        let left = left(vec![Some("ab"), Some("cd")]);
        let (_slot, needles) = filled(&left, &fill_memory);
        let (pool, memory) = work(16);
        let built = NeedleRows::build(&left, "m.number", needles, &memory).unwrap();
        assert!(built.is_none());
        assert_eq!(pool.reserved(), 0);
    }

    /// `a` to `a` 64 times over a 1 MiB text of `a`: every pattern is found
    /// within the first 64 bytes, and the pass stops there.
    #[test]
    fn the_text_pass_stops_once_every_needle_is_found() {
        let text = "a".repeat(1 << 20);
        let (memory, _slot, mut rows) = nested(false);
        let found: Vec<u32> = rows.paired(&text, &memory).unwrap().collect();
        assert_eq!(found.len(), 64);
        assert!(
            rows.visits <= 64 * 65 / 2,
            "{} occurrences visited for 64 needles",
            rows.visits
        );
    }

    /// With `b` never found the pass cannot stop early, so a cancelled query
    /// ends it at the first check instead of the text's 67 million occurrences.
    #[test]
    fn an_unfound_needle_leaves_the_pass_to_the_cancellation_check() {
        let text = "a".repeat(1 << 20);
        let (memory, _slot, mut rows) = nested(true);
        drop(memory.cancel_on_drop());
        let cancelled = rows
            .paired(&text, &memory)
            .err()
            .map(|error| error.to_string());
        assert_eq!(
            cancelled.as_deref(),
            Some("Execution error: query cancelled"),
            "{cancelled:?}"
        );
        assert_eq!(rows.visits, CANCEL_CHECK_VISITS);
        assert!(
            rows.seen.iter().all(|seen| !seen),
            "the found flags are reset"
        );
    }

    /// The plan promised text on both sides; a side that carries its column
    /// otherwise is a lowering defect, not a run-time fallback.
    #[test]
    fn columns_are_checked_as_text_on_their_own_sides() {
        let text = Schema::new(vec![Field::new("p.text", DataType::Utf8, true)]);
        let numbers = Schema::new(vec![Field::new("m.number", DataType::Int64, true)]);
        let checked = ContainsColumns::checked("p.text".into(), "m.number".into(), &numbers, &text);
        assert!(checked.is_err());
        let both = Schema::new(vec![
            Field::new("m.number", DataType::Utf8, true),
            Field::new("p.text", DataType::Utf8, true),
        ]);
        assert!(ContainsColumns::checked("p.text".into(), "m.number".into(), &both, &both).is_ok());
    }
}
