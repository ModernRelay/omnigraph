//! Pure RFC-023 benchmark-shape validation shared by the scenario executable
//! and its ordinary test target.

use arrow_array::{RecordBatch, UInt32Array};

pub(crate) const KEYED_WRITE_MAX_ROWS: usize = 8192;
pub(crate) const KEYED_WRITE_MAX_BYTES: u64 = 32 * 1024 * 1024;
pub(crate) const RECOVERY_MAX_TRANSACTIONS: usize = 1024;

// Arrow's value buffers dominate these vector batches. Reserve a deliberately
// conservative fixed allowance for ArrayData/offset buffers and per-row slack
// for string offsets, validity/alignment, and implementation bookkeeping. The
// iterator also checks every realized RecordBatch against the hard byte cap.
const BATCH_FIXED_OVERHEAD_BYTES: u64 = 64 * 1024;
const STRING_OFFSET_BYTES_PER_ROW: u64 = 4;
const I32_BYTES_PER_ROW: u64 = 4;
const PER_ROW_SLACK_BYTES: u64 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkPlan {
    pub(crate) batch_rows: usize,
    pub(crate) transaction_count: usize,
    pub(crate) estimated_row_bytes: u64,
    pub(crate) estimated_full_batch_bytes: u64,
}

/// Remove an oversized backing allocation retained by a zero-copy Arrow
/// slice before the verifier examines the batch.
///
/// Lance's strict row batching slices large decoded batches without copying.
/// Arrow then reports the complete parent allocation for the smaller slice.
/// Copy only that exceptional case; ordinary compact batches remain zero-copy.
pub(crate) fn compact_oversized_verification_slice(
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    let observed_bytes = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| "verification batch memory size exceeds u64".to_string())?;
    if observed_bytes <= KEYED_WRITE_MAX_BYTES {
        return Ok(batch);
    }

    let row_count = u32::try_from(batch.num_rows())
        .map_err(|_| "verification batch row count exceeds u32".to_string())?;
    let indices = UInt32Array::from_iter_values(0..row_count);
    let compact = arrow_select::take::take_record_batch(&batch, &indices)
        .map_err(|error| format!("compact oversized verification slice: {error}"))?;
    let compact_bytes = u64::try_from(compact.get_array_memory_size())
        .map_err(|_| "compacted verification batch memory size exceeds u64".to_string())?;
    if compact_bytes > KEYED_WRITE_MAX_BYTES {
        return Err(format!(
            "verification batch contains {compact_bytes} bytes after compacting its {observed_bytes}-byte retained parent, exceeding the {KEYED_WRITE_MAX_BYTES}-byte bound"
        ));
    }
    Ok(compact)
}

fn decimal_digits(mut value: usize) -> u64 {
    let mut digits = 1_u64;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn estimated_row_bytes(dims: usize, id_prefix: &str, rows: usize) -> Result<u64, String> {
    if dims == 0 {
        return Err("--dims must be greater than zero".to_string());
    }
    if rows == 0 {
        return Err("--rows must be greater than zero".to_string());
    }
    let vector_bytes = u64::try_from(dims)
        .map_err(|_| "--dims exceeds u64".to_string())?
        .checked_mul(4)
        .ok_or_else(|| "--dims byte width overflows u64".to_string())?;
    let prefix_bytes =
        u64::try_from(id_prefix.len()).map_err(|_| "id prefix width exceeds u64".to_string())?;
    let id_digits = decimal_digits(rows - 1).max(10);
    let id_bytes = prefix_bytes
        .checked_add(1)
        .and_then(|value| value.checked_add(id_digits))
        .ok_or_else(|| "generated id width overflows u64".to_string())?;
    vector_bytes
        .checked_add(id_bytes)
        .and_then(|value| value.checked_add(STRING_OFFSET_BYTES_PER_ROW))
        .and_then(|value| value.checked_add(I32_BYTES_PER_ROW))
        .and_then(|value| value.checked_add(PER_ROW_SLACK_BYTES))
        .ok_or_else(|| "estimated RFC-023 row width overflows u64".to_string())
}

/// Derive the largest source batch that stays within both production keyed
/// ceilings. This does not apply the recovery-chain cap because seed writes do
/// not mint one recovery identity per input batch.
pub(crate) fn derive_chunk_plan(
    dims: usize,
    id_prefix: &str,
    rows: usize,
) -> Result<ChunkPlan, String> {
    let row_bytes = estimated_row_bytes(dims, id_prefix, rows)?;
    let available = KEYED_WRITE_MAX_BYTES
        .checked_sub(BATCH_FIXED_OVERHEAD_BYTES)
        .expect("fixed allowance is below byte cap");
    let max_rows_by_bytes = available / row_bytes;
    if max_rows_by_bytes == 0 {
        return Err(format!(
            "one generated row is estimated at {row_bytes} bytes, exceeding the {}-byte keyed-write budget after fixed Arrow overhead",
            KEYED_WRITE_MAX_BYTES
        ));
    }
    let batch_rows_u64 = max_rows_by_bytes.min(KEYED_WRITE_MAX_ROWS as u64);
    let batch_rows = usize::try_from(batch_rows_u64)
        .map_err(|_| "derived RFC-023 batch row count exceeds usize".to_string())?;
    let transaction_count = rows / batch_rows + usize::from(rows % batch_rows != 0);
    let estimated_full_batch_bytes = row_bytes
        .checked_mul(batch_rows as u64)
        .and_then(|value| value.checked_add(BATCH_FIXED_OVERHEAD_BYTES))
        .ok_or_else(|| "estimated RFC-023 batch width overflows u64".to_string())?;
    debug_assert!(estimated_full_batch_bytes <= KEYED_WRITE_MAX_BYTES);
    Ok(ChunkPlan {
        batch_rows,
        transaction_count,
        estimated_row_bytes: row_bytes,
        estimated_full_batch_bytes,
    })
}

/// Apply the recovery-v9 proof-walk ceiling to a derived strict-write plan.
pub(crate) fn derive_strict_chunk_plan(
    dims: usize,
    id_prefix: &str,
    rows: usize,
) -> Result<ChunkPlan, String> {
    let plan = derive_chunk_plan(dims, id_prefix, rows)?;
    if plan.transaction_count > RECOVERY_MAX_TRANSACTIONS {
        return Err(format!(
            "RFC-023 scenario needs {} keyed transactions at {} rows/chunk, exceeding the recovery proof-walk limit of {}; reduce --rows or --dims",
            plan.transaction_count, plan.batch_rows, RECOVERY_MAX_TRANSACTIONS
        ));
    }
    Ok(plan)
}

/// Require a scenario that is intentionally one logical keyed transaction to
/// fit in exactly one derived batch.
pub(crate) fn require_single_batch(
    dims: usize,
    id_prefix: &str,
    rows: usize,
) -> Result<ChunkPlan, String> {
    let plan = derive_chunk_plan(dims, id_prefix, rows)?;
    if rows > plan.batch_rows {
        return Err(format!(
            "RFC-023 single-transaction source has {rows} rows but the {}-byte keyed cap allows only {} rows at --dims {dims}",
            KEYED_WRITE_MAX_BYTES, plan.batch_rows
        ));
    }
    Ok(ChunkPlan {
        batch_rows: rows,
        transaction_count: 1,
        estimated_full_batch_bytes: plan
            .estimated_row_bytes
            .checked_mul(rows as u64)
            .and_then(|value| value.checked_add(BATCH_FIXED_OVERHEAD_BYTES))
            .expect("validated single-batch estimate"),
        ..plan
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn derives_against_rows_and_bytes() {
        let ordinary = super::derive_strict_chunk_plan(256, "adopt-new", 100_000).unwrap();
        assert_eq!(ordinary.batch_rows, super::KEYED_WRITE_MAX_ROWS);
        assert_eq!(ordinary.transaction_count, 13);
        assert!(ordinary.estimated_full_batch_bytes <= super::KEYED_WRITE_MAX_BYTES);

        let wide = super::derive_strict_chunk_plan(4096, "adopt-new", 100_000).unwrap();
        assert!(wide.batch_rows < super::KEYED_WRITE_MAX_ROWS);
        assert!(wide.estimated_full_batch_bytes <= super::KEYED_WRITE_MAX_BYTES);
    }

    #[test]
    fn recovery_transaction_boundary_is_inclusive() {
        let one = super::derive_chunk_plan(256, "adopt-new", 1).unwrap();
        let at_limit_rows = one.batch_rows * super::RECOVERY_MAX_TRANSACTIONS;
        let accepted = super::derive_strict_chunk_plan(256, "adopt-new", at_limit_rows).unwrap();
        assert_eq!(accepted.transaction_count, super::RECOVERY_MAX_TRANSACTIONS);

        let error =
            super::derive_strict_chunk_plan(256, "adopt-new", at_limit_rows + 1).unwrap_err();
        assert!(error.contains("exceeding the recovery proof-walk limit"));
    }

    #[test]
    fn rejects_one_row_or_single_transaction_that_exceeds_bytes() {
        let one_row_error = super::derive_strict_chunk_plan(9_000_000, "adopt-new", 1).unwrap_err();
        assert!(one_row_error.contains("one generated row"));

        let single_error = super::require_single_batch(300_000, "upsert-new", 32).unwrap_err();
        assert!(single_error.contains("single-transaction source"));
    }

    #[test]
    fn compacts_a_small_slice_that_retains_an_oversized_parent_buffer() {
        use std::sync::Arc;

        use arrow_array::{Array as _, Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let parent = Int32Array::from_iter_values(0..9_000_000);
        let retained_slice = parent.slice(0, super::KEYED_WRITE_MAX_ROWS);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(retained_slice)],
        )
        .unwrap();
        assert!(batch.get_array_memory_size() as u64 > super::KEYED_WRITE_MAX_BYTES);

        let compact = super::compact_oversized_verification_slice(batch).unwrap();
        assert_eq!(compact.num_rows(), super::KEYED_WRITE_MAX_ROWS);
        assert!(compact.get_array_memory_size() as u64 <= super::KEYED_WRITE_MAX_BYTES);
        let values = compact
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(super::KEYED_WRITE_MAX_ROWS - 1), 8_191);
    }
}
