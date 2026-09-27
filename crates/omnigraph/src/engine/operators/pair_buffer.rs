//! `PairBuffer`: the one output buffer of both joins, over the left side they
//! collect (`collect_left`): the `(left row, right row)` index pairs of each
//! right batch, filtered and re-batched to the batch size. It chooses no pair.

use std::mem::size_of;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use omnigraph_compiler::ir::{IRExpr, ParamMap};

use super::filter::conjoined_mask;
use super::memory::WorkMemory;
use super::producer::BatchSender;
use super::{conform, external};
use crate::engine::scan::hconcat_batches;

/// The refusal name the pool reports when an output batch does not fit.
const OUTPUT: &str = "cross join output";

/// The left input as one held batch charged as `owner`; `input_rows` counts
/// the streamed right side alone, whose batches bound each output.
pub(super) async fn collect_left(
    mut left: SendableRecordBatchStream,
    schema: &SchemaRef,
    memory: &WorkMemory,
    owner: &str,
) -> DfResult<RecordBatch> {
    let held = memory.child(owner)?;
    let mut batches = Vec::new();
    while let Some(batch) = left.next().await {
        let batch = batch?;
        held.hold(&batch)?;
        held.entries::<RecordBatch>(1)?;
        batches.push(batch);
    }
    let batch = held.concat(schema, &batches)?;
    memory.hold(&batch)?;
    Ok(batch)
}

/// The rows of `batch` as `u32` indices into it.
fn row_count(batch: &RecordBatch, side: &str) -> DfResult<u32> {
    u32::try_from(batch.num_rows()).map_err(|_| {
        DataFusionError::Execution(format!("cross join {side} exceeds row index range"))
    })
}

/// Held pairs become one kept batch at `size` pairs, the byte budget (a pair's
/// bytes its two rows') or the right batch's end; kept batches leave as one output
/// at `size` rows or the budget, so an output exceeds `size` by one kept batch at most.
pub(super) struct PairBuffer {
    left: RecordBatch,
    left_count: u32,
    right: Option<RecordBatch>,
    right_count: u32,
    declared: SchemaRef,
    filters: Vec<IRExpr>,
    params: Arc<ParamMap>,
    size: usize,
    budget: usize,
    left_rows: Vec<u32>,
    right_rows: Vec<u32>,
    left_bytes: Vec<usize>,
    right_bytes: Vec<usize>,
    right_room: usize,
    bytes: usize,
    charge: WorkMemory,
    kept: Vec<RecordBatch>,
    kept_rows: usize,
    kept_bytes: usize,
    pending: WorkMemory,
}

fn row_bytes(memory: &WorkMemory, batch: &RecordBatch, rows: &mut Vec<usize>) -> DfResult<()> {
    rows.clear();
    for row in 0..batch.num_rows() {
        rows.push(memory.slice_bytes(batch, row, 1)?);
    }
    Ok(())
}

impl PairBuffer {
    /// A buffer over the collected `left`, whose outputs carry `declared`
    /// and the pairs `filters` hold for; `size` is the flush threshold of the
    /// held pairs and the re-batching target of the kept rows, not a cap.
    pub(super) fn new(
        left: RecordBatch,
        declared: SchemaRef,
        filters: Vec<IRExpr>,
        params: Arc<ParamMap>,
        size: usize,
        memory: &WorkMemory,
    ) -> DfResult<Self> {
        let size = size.max(1);
        let left_count = row_count(&left, "left side")?;
        let charge = memory.child(OUTPUT)?;
        charge.grow(
            size.saturating_mul(2 * size_of::<u32>())
                .saturating_add(left.num_rows().saturating_mul(size_of::<usize>())),
        )?;
        let mut left_bytes = Vec::with_capacity(left.num_rows());
        row_bytes(&charge, &left, &mut left_bytes)?;
        let pending = charge.child(OUTPUT)?;
        Ok(Self {
            left,
            left_count,
            right: None,
            right_count: 0,
            declared,
            filters,
            params,
            size,
            budget: memory.batch_bytes(),
            left_rows: Vec::with_capacity(size),
            right_rows: Vec::with_capacity(size),
            left_bytes,
            right_bytes: Vec::new(),
            right_room: 0,
            bytes: 0,
            charge,
            kept: Vec::new(),
            kept_rows: 0,
            kept_bytes: 0,
            pending,
        })
    }

    /// The conjuncts every later flush tests, in place of the ones given at
    /// construction; before any pair is pushed.
    pub(super) fn set_filters(&mut self, filters: Vec<IRExpr>) {
        debug_assert!(
            self.right.is_none() && self.kept.is_empty(),
            "filters changed under held pairs"
        );
        self.filters = filters;
    }

    /// The right batch the pairs pushed next index into; the batch before
    /// it was flushed. The row-size scratch's growth is charged first, its
    /// old storage freed before the new is reserved (no move peak), and the
    /// charge then brought to the capacity it has.
    pub(super) fn start(&mut self, batch: &RecordBatch) -> DfResult<()> {
        debug_assert!(
            self.right_rows.is_empty(),
            "pairs of an earlier right batch still held"
        );
        self.right_count = row_count(batch, "batch")?;
        self.right_bytes.clear();
        let rows = batch.num_rows();
        if rows > self.right_room {
            self.charge
                .grow((rows - self.right_room).saturating_mul(size_of::<usize>()))?;
            drop(std::mem::take(&mut self.right_bytes));
            self.right_bytes.reserve_exact(rows);
            let room = self.right_bytes.capacity();
            self.charge
                .grow((room - rows).saturating_mul(size_of::<usize>()))?;
            self.right_room = room;
        }
        row_bytes(&self.charge, batch, &mut self.right_bytes)?;
        self.right = Some(batch.clone());
        Ok(())
    }

    /// Each of the left rows `rows` paired with every row of the started
    /// right batch.
    pub(super) async fn push_rows(
        &mut self,
        rows: impl IntoIterator<Item = u32>,
        sender: &BatchSender,
    ) -> DfResult<()> {
        for row in rows {
            for right_row in 0..self.right_count {
                self.push(row, right_row, sender).await?;
            }
        }
        Ok(())
    }

    /// Every left row paired with every row of the started right batch.
    pub(super) async fn push_all(&mut self, sender: &BatchSender) -> DfResult<()> {
        self.push_rows(0..self.left_count, sender).await
    }

    /// One pair, after the flush its arrival forces: `size` pairs held, or
    /// its bytes would take the held pairs past the budget.
    pub(super) async fn push(
        &mut self,
        left_row: u32,
        right_row: u32,
        sender: &BatchSender,
    ) -> DfResult<()> {
        let bytes =
            self.left_bytes[left_row as usize].saturating_add(self.right_bytes[right_row as usize]);
        let full = self.left_rows.len() == self.size
            || (!self.left_rows.is_empty() && self.bytes.saturating_add(bytes) > self.budget);
        if full {
            self.flush_pairs(sender).await?;
        }
        self.left_rows.push(left_row);
        self.right_rows.push(right_row);
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    /// The end of the started right batch: its last pairs kept, and the
    /// batch itself let go, so its buffers live on only in kept batches.
    pub(super) async fn flush(&mut self, sender: &BatchSender) -> DfResult<()> {
        self.flush_pairs(sender).await?;
        self.right = None;
        Ok(())
    }

    /// The held pairs as one kept batch: left rows taken, right rows a slice when
    /// one run (sharing the right batch's buffers, compact by the time the join
    /// holds it) and taken otherwise, each copy admitted first, then filtered.
    async fn flush_pairs(&mut self, sender: &BatchSender) -> DfResult<()> {
        let Some(&first) = self.right_rows.first() else {
            return Ok(());
        };
        let right = self.right.as_ref().ok_or_else(|| {
            DataFusionError::Internal("cross join pairs before a right batch".into())
        })?;
        let count = self.right_rows.len();
        let work = self.charge.child(OUTPUT)?;
        work.entries::<u32>(count.saturating_mul(2))?;
        let indices = UInt32Array::from_iter_values(self.left_rows.drain(..));
        let left_rows = work.take_once(&self.left, &indices, OUTPUT)?;
        let run = self
            .right_rows
            .windows(2)
            .all(|pair| pair[0].checked_add(1) == Some(pair[1]));
        let right_rows = if run {
            right.slice(first as usize, count)
        } else {
            let indices = UInt32Array::from_iter_values(self.right_rows.iter().copied());
            work.take_once(right, &indices, OUTPUT)?
        };
        self.right_rows.clear();
        let bytes = std::mem::take(&mut self.bytes);
        let output = hconcat_batches(&left_rows, &right_rows).map_err(external)?;
        let output = conform(output, &self.declared).map_err(external)?;
        let kept = match conjoined_mask(&output, &self.filters, &self.params)? {
            None => output,
            Some(mask) => {
                work.grow(bytes)?;
                arrow_select::filter::filter_record_batch(&output, &mask)?
            }
        };
        if kept.num_rows() == 0 {
            return Ok(());
        }
        self.pending.hold(&kept)?;
        self.pending.entries::<RecordBatch>(1)?;
        self.kept_rows += kept.num_rows();
        self.kept_bytes =
            self.kept_bytes
                .saturating_add(self.pending.slice_bytes(&kept, 0, kept.num_rows())?);
        self.kept.push(kept);
        drop(work);
        if self.kept_rows >= self.size || self.kept_bytes >= self.budget {
            self.send(sender).await?;
        }
        Ok(())
    }

    /// The accumulated kept batches, sent as the buffer's last output.
    pub(super) async fn finish(&mut self, sender: &BatchSender) -> DfResult<()> {
        debug_assert!(
            self.right_rows.is_empty(),
            "pairs of the last right batch not flushed"
        );
        self.send(sender).await
    }

    /// The accumulated kept batches as one output, concatenated once under
    /// the `OUTPUT` charge that held them; nothing when none is held.
    async fn send(&mut self, sender: &BatchSender) -> DfResult<()> {
        if self.kept.is_empty() {
            return Ok(());
        }
        let kept = std::mem::take(&mut self.kept);
        self.kept_rows = 0;
        self.kept_bytes = 0;
        let pending = std::mem::replace(&mut self.pending, self.charge.child(OUTPUT)?);
        let output = pending.concat(&self.declared, &kept)?;
        drop(kept);
        pending.output(&output)?;
        sender.send(output, Arc::new(pending)).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::execution::memory_pool::MemoryPool;
    use omnigraph_compiler::query::ast::{CompOp, Literal};

    use super::*;
    use crate::engine::operators::fixtures::{context, pairs, text_batch, text_pairs};
    use crate::engine::operators::joined_schema;
    use crate::engine::operators::producer::producer_stream;

    fn matters(values: &[(&str, &str)]) -> RecordBatch {
        text_batch(["m.mid", "m.number"], values)
    }

    fn passages(values: &[(&str, &str)]) -> RecordBatch {
        text_batch(["p.pid", "p.text"], values)
    }

    /// The stream of a filterless buffer over `left` fed the `pairs` of each
    /// right batch in turn, flushed at each batch's end and finished, `size`
    /// rows a batch, under a `limit`-byte pool.
    fn buffered(
        left: RecordBatch,
        batches: Vec<(RecordBatch, Vec<(u32, u32)>)>,
        size: usize,
        limit: usize,
    ) -> (Arc<dyn MemoryPool>, SendableRecordBatchStream) {
        let (pool, ctx) = context(limit, size);
        let schema = joined_schema(&left.schema(), &batches[0].0.schema()).unwrap();
        let declared = Arc::clone(&schema);
        let memory = Arc::new(WorkMemory::new(ctx, "test join").unwrap());
        let stream = producer_stream(schema, memory, None, move |memory, sender| async move {
            let mut buffer = PairBuffer::new(
                left,
                declared,
                Vec::new(),
                Arc::new(ParamMap::new()),
                size,
                &memory,
            )?;
            for (batch, pairs) in batches {
                buffer.start(&batch)?;
                for (left_row, right_row) in pairs {
                    buffer.push(left_row, right_row, &sender).await?;
                }
                buffer.flush(&sender).await?;
            }
            buffer.finish(&sender).await
        });
        (pool, stream)
    }

    /// The `(m.mid, p.pid)` pairs of each output of `stream`.
    async fn outputs(stream: SendableRecordBatchStream) -> Vec<Vec<(String, String)>> {
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        text_pairs(&batches)
    }

    async fn released(pool: &Arc<dyn MemoryPool>) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while pool.reserved() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the buffer's charges must go with its stream");
    }

    /// Three pairs flush inside the first batch and leave at once; its last
    /// two, kept at its end, wait for the second batch's two (four rows reach
    /// the size of three) and leave with them in pair order, one output.
    #[tokio::test]
    async fn pairs_flush_at_the_batch_size_and_kept_rows_leave_at_the_size() {
        let left = matters(&[
            ("a0", "aa"),
            ("a1", "aa"),
            ("a2", "aa"),
            ("a3", "aa"),
            ("b", "bb"),
        ]);
        let batches = vec![
            (
                passages(&[("p0", "aa"), ("p1", "bb")]),
                vec![(0, 0), (1, 0), (2, 0), (3, 0), (4, 1)],
            ),
            (
                passages(&[("p2", "bb"), ("p3", "bb")]),
                vec![(4, 1), (4, 0)],
            ),
        ];
        let (pool, stream) = buffered(left, batches, 3, 1 << 20);
        assert_eq!(
            outputs(stream).await,
            [
                pairs(&[("a0", "p0"), ("a1", "p0"), ("a2", "p0")]),
                pairs(&[("a3", "p0"), ("b", "p1"), ("b", "p3"), ("b", "p2")]),
            ]
        );
        released(&pool).await;
    }

    /// Three right batches of one pair each stay under the size of eight, so
    /// their kept rows leave as one output at `finish`.
    #[tokio::test]
    async fn kept_rows_of_several_right_batches_leave_as_one_output() {
        let left = matters(&[("a", "aa"), ("b", "bb")]);
        let batches = vec![
            (passages(&[("p0", "aa")]), vec![(0, 0)]),
            (passages(&[("p1", "bb")]), vec![(1, 0)]),
            (passages(&[("p2", "aa")]), vec![(0, 0)]),
        ];
        let (pool, stream) = buffered(left, batches, 8, 1 << 20);
        assert_eq!(
            outputs(stream).await,
            [pairs(&[("a", "p0"), ("b", "p1"), ("a", "p2")])]
        );
        released(&pool).await;
    }

    /// Sixteen left rows of 4 KiB pair with one passage under a 1 MiB pool,
    /// whose 32 KiB batch bytes hold seven such pairs: the byte budget flushes
    /// before the size of sixteen, and the kept rows leave at that budget too.
    #[tokio::test]
    async fn wide_pairs_flush_at_the_batch_bytes_before_the_batch_size() {
        let numbers: Vec<String> = (0..16)
            .map(|n| format!("{n:02}{}", "n".repeat(4_094)))
            .collect();
        let mids: Vec<String> = (0..16).map(|n| format!("m{n}")).collect();
        let rows: Vec<(&str, &str)> = mids
            .iter()
            .zip(&numbers)
            .map(|(mid, number)| (mid.as_str(), number.as_str()))
            .collect();
        let batches = vec![(
            passages(&[("p0", "x")]),
            (0..16).map(|row| (row, 0)).collect(),
        )];
        let (pool, stream) = buffered(matters(&rows), batches, 16, 1 << 20);
        let lengths: Vec<usize> = outputs(stream).await.iter().map(Vec::len).collect();
        assert_eq!(lengths, [14, 2]);
        released(&pool).await;
    }

    /// A residual that rejects every pair keeps nothing of the right batch,
    /// so at the batch's end the buffer holds no owner of it: the next poll
    /// releases the producer's lease on buffers nobody has any more.
    #[tokio::test]
    async fn the_right_batch_is_let_go_at_its_end_when_no_pair_is_kept() {
        let left = matters(&[("m", "x")]);
        let (pool, ctx) = context(1 << 20, 8);
        let batch = passages(&[("p0", "y"), ("p1", "y")]);
        let schema = joined_schema(&left.schema(), &batch.schema()).unwrap();
        let declared = Arc::clone(&schema);
        let never = IRExpr::comparison(
            IRExpr::PropAccess {
                variable: "m".to_string(),
                property: "mid".to_string(),
            },
            CompOp::Eq,
            IRExpr::Literal(Literal::String("never".to_string())),
        );
        let memory = Arc::new(WorkMemory::new(ctx, "test join").unwrap());
        let stream = producer_stream(schema, memory, None, move |memory, sender| async move {
            let mut buffer = PairBuffer::new(
                left,
                declared,
                vec![never],
                Arc::new(ParamMap::new()),
                8,
                &memory,
            )?;
            buffer.start(&batch)?;
            buffer.push_all(&sender).await?;
            buffer.flush(&sender).await?;
            let owners = Arc::strong_count(batch.column(0));
            if owners != 1 {
                return Err(DataFusionError::Internal(format!(
                    "{owners} owners of the right batch after its end"
                )));
            }
            buffer.finish(&sender).await
        });
        assert!(outputs(stream).await.is_empty());
        released(&pool).await;
    }

    /// 8,193 right rows take the row-size scratch just past a power of two:
    /// the charge covers the capacity the vector then has, not its length.
    #[tokio::test]
    async fn right_row_scratch_is_charged_at_its_capacity() {
        let left = matters(&[("m", "x")]);
        let (pool, ctx) = context(1 << 20, 8);
        let pids: Vec<String> = (0..8_193).map(|row| format!("p{row}")).collect();
        let rows: Vec<(&str, &str)> = pids.iter().map(|pid| (pid.as_str(), "y")).collect();
        let batch = passages(&rows);
        let memory = WorkMemory::new(ctx, "test join").unwrap();
        let mut buffer = PairBuffer::new(
            left.clone(),
            joined_schema(&left.schema(), &batch.schema()).unwrap(),
            Vec::new(),
            Arc::new(ParamMap::new()),
            8,
            &memory,
        )
        .unwrap();
        let before = pool.reserved();
        buffer.start(&batch).unwrap();
        let charged = pool.reserved() - before;
        let capacity = buffer.right_bytes.capacity();
        assert!(capacity >= 8_193);
        assert!(
            charged >= capacity * size_of::<usize>(),
            "{charged} bytes charged for a scratch capacity of {capacity}"
        );
    }

    /// A 32 KiB pool admits a two-row scratch but not 8,193 rows' 64 KiB: the
    /// start refuses before the scratch grows, so its capacity stays as it was.
    #[tokio::test]
    async fn a_scratch_growth_the_pool_refuses_leaves_the_scratch_as_it_was() {
        let left = matters(&[("m", "x")]);
        let (_pool, ctx) = context(32 << 10, 8);
        let pids: Vec<String> = (0..8_193).map(|row| format!("p{row}")).collect();
        let rows: Vec<(&str, &str)> = pids.iter().map(|pid| (pid.as_str(), "y")).collect();
        let (small, large) = (passages(&rows[..2]), passages(&rows));
        let memory = WorkMemory::new(ctx, "test join").unwrap();
        let mut buffer = PairBuffer::new(
            left.clone(),
            joined_schema(&left.schema(), &small.schema()).unwrap(),
            Vec::new(),
            Arc::new(ParamMap::new()),
            8,
            &memory,
        )
        .unwrap();
        buffer.start(&small).unwrap();
        let capacity = buffer.right_bytes.capacity();
        let error = buffer.start(&large).unwrap_err();
        assert!(
            matches!(error, DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        assert_eq!(buffer.right_bytes.capacity(), capacity);
    }

    /// A 32 KiB pool admits the buffer's room but not the copy of a 64 KiB
    /// left row: the flush refuses as `cross join output`, and the stream's
    /// end releases everything.
    #[tokio::test]
    async fn a_flush_the_pool_refuses_names_the_output_and_releases() {
        let number = "n".repeat(64 << 10);
        let batches = vec![(passages(&[("p0", "x")]), vec![(0, 0)])];
        let (pool, mut stream) = buffered(matters(&[("m", number.as_str())]), batches, 8, 32 << 10);
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(
            matches!(error, DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        assert!(error.to_string().contains("cross join output"), "{error}");
        drop(stream);
        released(&pool).await;
    }
}
