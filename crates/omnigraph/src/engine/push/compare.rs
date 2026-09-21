//! Row equality as a kernel: every non-Blob user column compared with one
//! Arrow call per vector, OR-folded into `differs`; rows that tie on those
//! columns and carry Blob columns go to the engine's descriptor-aware hook
//! one at a time. `RowCompare` is the two-sided operator over it, emitting
//! `_op` and a selection of the rows that changed.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};
use omnigraph_compiler::SystemColumns;
use omnigraph_planner::SideId;

use super::chunk::{Chunk, prefixed, side_batch, with_column};
use super::context::{EngineHooks, ExecContext, SideRow, side_name};
use super::error::Result;
use super::roles::{Operator, OperatorResult};
use crate::changes::model::is_reserved_storage_system_column;

use crate::changes::row_compare::column_name_at_vintage;

fn is_blob(field: &Field) -> bool {
    lance::datatypes::Field::try_from(field).is_ok_and(|field| field.is_blob())
}

/// Whether the kernel compares this type with one vectorized call; every
/// other type compares row by row on its logical bytes, floats included so
/// a NaN stays equal to itself.
fn vectorized(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

/// Per row, whether the two columns hold distinct values (two nulls are not
/// distinct). Columns of different types are distinct everywhere; the
/// operators consult such rows only where both sides are present, which two
/// hydrated sides of one table never are with different types.
pub fn column_distinct(left: &ArrayRef, right: &ArrayRef) -> Result<BooleanArray> {
    if left.data_type() != right.data_type() {
        return Ok(BooleanArray::from(vec![true; left.len()]));
    }
    if vectorized(left.data_type()) {
        return Ok(arrow_ord::cmp::distinct(left, right)?);
    }
    Ok((0..left.len())
        .map(|row| left.slice(row, 1).to_data() != right.slice(row, 1).to_data())
        .collect())
}

#[derive(Clone)]
pub struct SidePair {
    pub left: SideId,
    pub right: SideId,
    pub left_columns: SystemColumns,
    pub right_columns: SystemColumns,
    pub is_edge: bool,
}

/// `differs[i]` for every row where both sides are present; `false` where a
/// side is absent.
pub async fn rows_differ(
    batch: &RecordBatch,
    pair: &SidePair,
    hooks: &Arc<dyn EngineHooks>,
) -> Result<Vec<bool>> {
    let rows = batch.num_rows();
    let left_name = side_name(pair.left);
    let right_name = side_name(pair.right);
    let left_id = batch
        .column_by_name(&prefixed(left_name, pair.left_columns.id))
        .cloned();
    let right_id = batch
        .column_by_name(&prefixed(right_name, pair.right_columns.id))
        .cloned();
    let (Some(left_id), Some(right_id)) = (left_id, right_id) else {
        return Ok(vec![false; rows]);
    };
    let matched: Vec<bool> = (0..rows)
        .map(|row| left_id.is_valid(row) && right_id.is_valid(row))
        .collect();
    let mut differs = vec![false; rows];
    let mut has_blob = false;
    let prefix = format!("{left_name}.");
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let Some(name) = field.name().strip_prefix(&prefix) else {
            continue;
        };
        if is_reserved_storage_system_column(name) {
            continue;
        }
        if is_blob(field) {
            has_blob = true;
            continue;
        }
        let right_column_name = prefixed(
            right_name,
            column_name_at_vintage(name, pair.left_columns, pair.right_columns, pair.is_edge),
        );
        let Some(other) = batch.column_by_name(&right_column_name) else {
            return Err(super::error::ExecError::internal(format!(
                "schema-gated change row is missing column '{right_column_name}'"
            )));
        };
        let distinct = column_distinct(column, other)?;
        for row in 0..rows {
            if matched[row] && distinct.value(row) {
                differs[row] = true;
            }
        }
    }
    if has_blob {
        let left_batch = side_batch(batch, left_name)?;
        let right_batch = side_batch(batch, right_name)?;
        for row in 0..rows {
            if matched[row] && !differs[row] {
                let equal = hooks
                    .rows_equal(
                        SideRow {
                            side: pair.left,
                            batch: &left_batch,
                            row,
                        },
                        SideRow {
                            side: pair.right,
                            batch: &right_batch,
                            row,
                        },
                    )
                    .await?;
                differs[row] = !equal;
            }
        }
    }
    Ok(differs)
}

/// `RowCompare`: the parent side is the before image, the child the after.
pub struct RowCompare {
    pair: SidePair,
    hooks: Arc<dyn EngineHooks>,
}

impl RowCompare {
    pub fn new(ctx: &ExecContext) -> Result<Self> {
        let parent = ctx.side(SideId::Parent)?;
        let child = ctx.side(SideId::Child)?;
        Ok(Self {
            pair: SidePair {
                left: SideId::Parent,
                right: SideId::Child,
                left_columns: parent.columns,
                right_columns: child.columns,
                is_edge: parent.is_edge,
            },
            hooks: ctx.hooks.clone(),
        })
    }
}

pub const OP_INSERT: &str = "insert";
pub const OP_UPDATE: &str = "update";
pub const OP_DELETE: &str = "delete";
pub const OP_COLUMN: &str = "_op";

#[async_trait::async_trait]
impl Operator for RowCompare {
    async fn execute(&mut self, chunk: Chunk, out: &mut Vec<Chunk>) -> Result<OperatorResult> {
        let batch = chunk.compact()?.batch;
        let rows = batch.num_rows();
        let parent_id = batch
            .column_by_name(&prefixed(
                side_name(SideId::Parent),
                self.pair.left_columns.id,
            ))
            .cloned();
        let child_id = batch
            .column_by_name(&prefixed(
                side_name(SideId::Child),
                self.pair.right_columns.id,
            ))
            .cloned();
        let differs = rows_differ(&batch, &self.pair, &self.hooks).await?;
        let mut ops: Vec<Option<&str>> = Vec::with_capacity(rows);
        for (row, differs) in differs.iter().enumerate() {
            let parent_present = parent_id
                .as_ref()
                .is_some_and(|column| column.is_valid(row));
            let child_present = child_id.as_ref().is_some_and(|column| column.is_valid(row));
            ops.push(match (parent_present, child_present) {
                (false, true) => Some(OP_INSERT),
                (true, false) => Some(OP_DELETE),
                (true, true) if *differs => Some(OP_UPDATE),
                _ => None,
            });
        }
        let op_column: StringArray = ops.iter().copied().collect();
        let batch = with_column(&batch, OP_COLUMN, Arc::new(op_column), true)?;
        out.push(Chunk::select(batch, ops.iter().map(Option::is_some)));
        Ok(OperatorResult::NeedMoreInput)
    }
}
