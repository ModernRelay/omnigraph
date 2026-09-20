//! The three-way merge decision as data, and `ClassifyThreeWay` over it.
//! [`decide`] is the one rule: the executor's row loop and the operator's
//! kernel both evaluate it, from presence and pairwise equality alone.

use std::sync::Arc;

use arrow_array::UInt32Array;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{Field, Schema};
use arrow_select::nullif::nullif;
use arrow_select::take::take_record_batch;
use arrow_select::zip::zip;
use omnigraph_planner::SideId;

use super::chunk::{Chunk, KEY, prefixed};
use super::compare::{SidePair, rows_differ};
use super::context::{EngineHooks, ExecContext, side_name};
use super::error::{ExecError, Result};
use super::roles::{Operator, OperatorResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Presence {
    pub base: bool,
    pub source: bool,
    pub target: bool,
}

/// Pairwise equality, read only where both rows of a pair are present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Equal {
    pub source_base: bool,
    pub target_base: bool,
    pub source_target: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictShape {
    DivergentInsert,
    DeleteVsUpdate,
    DivergentUpdate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selected {
    Source,
    Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Unchanged,
    Deleted,
    Inserted(Selected),
    Updated(Selected),
    Conflict(ConflictShape),
}

impl Outcome {
    pub fn name(self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::Deleted => "deleted",
            Self::Inserted(_) => "inserted",
            Self::Updated(_) => "updated",
            Self::Conflict(ConflictShape::DivergentInsert) => "conflict_divergent_insert",
            Self::Conflict(ConflictShape::DeleteVsUpdate) => "conflict_delete_vs_update",
            Self::Conflict(ConflictShape::DivergentUpdate) => "conflict_divergent_update",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "unchanged" => Self::Unchanged,
            "deleted" => Self::Deleted,
            "inserted" => Self::Inserted(Selected::Target),
            "updated" => Self::Updated(Selected::Target),
            "conflict_divergent_insert" => Self::Conflict(ConflictShape::DivergentInsert),
            "conflict_delete_vs_update" => Self::Conflict(ConflictShape::DeleteVsUpdate),
            "conflict_divergent_update" => Self::Conflict(ConflictShape::DivergentUpdate),
            _ => return None,
        })
    }

    pub fn selected(self) -> Option<Selected> {
        match self {
            Self::Inserted(side) | Self::Updated(side) => Some(side),
            _ => None,
        }
    }
}

fn option_equal(left: bool, right: bool, equal: bool) -> bool {
    match (left, right) {
        (false, false) => true,
        (true, true) => equal,
        _ => false,
    }
}

pub fn conflict_shape(presence: Presence) -> ConflictShape {
    match (presence.base, presence.source, presence.target) {
        (false, true, true) => ConflictShape::DivergentInsert,
        (true, false, true) | (true, true, false) => ConflictShape::DeleteVsUpdate,
        _ => ConflictShape::DivergentUpdate,
    }
}

/// The three-way rule for one id.
pub fn decide(presence: Presence, equal: Equal) -> Outcome {
    let source_changed = !option_equal(presence.source, presence.base, equal.source_base);
    let target_changed = !option_equal(presence.target, presence.base, equal.target_base);
    let source_eq_target = option_equal(presence.source, presence.target, equal.source_target);
    let selection = if !source_changed {
        presence.target.then_some(Selected::Target)
    } else if !target_changed {
        presence.source.then_some(Selected::Source)
    } else if source_eq_target {
        presence.target.then_some(Selected::Target)
    } else {
        return Outcome::Conflict(conflict_shape(presence));
    };
    match selection {
        None if presence.target => Outcome::Deleted,
        None => Outcome::Unchanged,
        Some(selected) => {
            let selected_equals_target = match selected {
                Selected::Target => true,
                Selected::Source => source_eq_target,
            };
            if selected_equals_target {
                Outcome::Unchanged
            } else if presence.target {
                Outcome::Updated(selected)
            } else {
                Outcome::Inserted(selected)
            }
        }
    }
}

pub const OUTCOME_COLUMN: &str = "_outcome";
pub const SIDE_COLUMN: &str = "_side";

/// The image column for the selected side: `source` where `from_source`,
/// `target` elsewhere. Two typed columns zip; a column that is null on one
/// side (an absent side, typed from the catalog) is masked from the other.
fn select_image(
    from_source: &BooleanArray,
    source: &ArrayRef,
    target: &ArrayRef,
) -> Result<ArrayRef> {
    if source.data_type() == target.data_type() {
        return Ok(zip(from_source, source, target)?);
    }
    if source.null_count() == source.len() {
        return Ok(nullif(target, from_source)?);
    }
    if target.null_count() == target.len() {
        let from_target: BooleanArray = from_source
            .iter()
            .map(|flag| flag.map(|flag| !flag))
            .collect();
        return Ok(nullif(source, &from_target)?);
    }
    Err(ExecError::internal(format!(
        "merge image columns differ in type: {:?} vs {:?}",
        source.data_type(),
        target.data_type()
    )))
}

/// `ClassifyThreeWay`: base, source (the parent side) and target (the child
/// side) hydrated in one chunk; out come the changed ids with the selected
/// image under the table's own column names, `_outcome` and `_side`.
pub struct ClassifyThreeWay {
    source_base: SidePair,
    target_base: SidePair,
    source_target: SidePair,
    hooks: Arc<dyn EngineHooks>,
    id_col: &'static str,
}

impl ClassifyThreeWay {
    pub fn new(ctx: &ExecContext) -> Result<Self> {
        let base = ctx.side(SideId::Base)?;
        let source = ctx.side(SideId::Parent)?;
        let target = ctx.side(SideId::Child)?;
        let pair =
            |left: &super::context::SideContext, right: &super::context::SideContext| SidePair {
                left: left.side,
                right: right.side,
                left_columns: left.columns,
                right_columns: right.columns,
                is_edge: left.is_edge,
            };
        Ok(Self {
            source_base: pair(source, base),
            target_base: pair(target, base),
            source_target: pair(source, target),
            hooks: ctx.hooks.clone(),
            id_col: base.columns.id,
        })
    }
}

#[async_trait::async_trait]
impl Operator for ClassifyThreeWay {
    async fn execute(&mut self, chunk: Chunk, out: &mut Vec<Chunk>) -> Result<OperatorResult> {
        let batch = chunk.compact()?.batch;
        let rows = batch.num_rows();
        let present = |side: SideId| -> Vec<bool> {
            match batch.column_by_name(&prefixed(side_name(side), self.id_col)) {
                Some(column) => (0..rows).map(|row| column.is_valid(row)).collect(),
                None => vec![false; rows],
            }
        };
        let base = present(SideId::Base);
        let source = present(SideId::Parent);
        let target = present(SideId::Child);
        let source_base = rows_differ(&batch, &self.source_base, &self.hooks).await?;
        let target_base = rows_differ(&batch, &self.target_base, &self.hooks).await?;
        let source_target = rows_differ(&batch, &self.source_target, &self.hooks).await?;
        let mut outcomes = Vec::with_capacity(rows);
        let mut kept: Vec<u32> = Vec::new();
        for row in 0..rows {
            let outcome = decide(
                Presence {
                    base: base[row],
                    source: source[row],
                    target: target[row],
                },
                Equal {
                    source_base: !source_base[row],
                    target_base: !target_base[row],
                    source_target: !source_target[row],
                },
            );
            if outcome != Outcome::Unchanged {
                kept.push(row as u32);
            }
            outcomes.push(outcome);
        }
        if kept.is_empty() {
            return Ok(OperatorResult::NeedMoreInput);
        }
        let indices = UInt32Array::from(kept.clone());
        let changed = take_record_batch(&batch, &indices)?;
        let from_source: BooleanArray = kept
            .iter()
            .map(|row| outcomes[*row as usize].selected() == Some(Selected::Source))
            .collect();
        let mut fields = vec![Field::new(KEY, arrow_schema::DataType::Utf8, false)];
        let mut columns: Vec<ArrayRef> = vec![
            changed
                .column_by_name(KEY)
                .cloned()
                .ok_or_else(|| ExecError::internal("classify input is missing _key"))?,
        ];
        let base_prefix = format!("{}.", side_name(SideId::Base));
        for field in changed.schema().fields() {
            let Some(name) = field.name().strip_prefix(&base_prefix) else {
                continue;
            };
            let source_column = changed
                .column_by_name(&prefixed(side_name(SideId::Parent), name))
                .ok_or_else(|| ExecError::internal(format!("source side is missing '{name}'")))?;
            let target_column = changed
                .column_by_name(&prefixed(side_name(SideId::Child), name))
                .ok_or_else(|| ExecError::internal(format!("target side is missing '{name}'")))?;
            let image = select_image(&from_source, source_column, target_column)?;
            fields.push(
                Field::new(name, image.data_type().clone(), true)
                    .with_metadata(field.metadata().clone()),
            );
            columns.push(image);
        }
        let outcome_column: StringArray = kept
            .iter()
            .map(|row| Some(outcomes[*row as usize].name()))
            .collect();
        let side_column: StringArray = kept
            .iter()
            .map(|row| {
                outcomes[*row as usize]
                    .selected()
                    .map(|selected| match selected {
                        Selected::Source => side_name(SideId::Parent),
                        Selected::Target => side_name(SideId::Child),
                    })
            })
            .collect();
        fields.push(Field::new(
            OUTCOME_COLUMN,
            arrow_schema::DataType::Utf8,
            false,
        ));
        columns.push(Arc::new(outcome_column));
        fields.push(Field::new(SIDE_COLUMN, arrow_schema::DataType::Utf8, true));
        columns.push(Arc::new(side_column));
        out.push(Chunk::new(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?));
        Ok(OperatorResult::NeedMoreInput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(base: bool, source: bool, target: bool) -> Presence {
        Presence {
            base,
            source,
            target,
        }
    }

    #[test]
    fn unchanged_when_no_side_moved() {
        let equal = Equal {
            source_base: true,
            target_base: true,
            source_target: true,
        };
        assert_eq!(decide(all(true, true, true), equal), Outcome::Unchanged);
        assert_eq!(decide(all(false, false, false), equal), Outcome::Unchanged);
    }

    #[test]
    fn source_edit_over_untouched_target_updates_with_the_source_image() {
        let equal = Equal {
            source_base: false,
            target_base: true,
            source_target: false,
        };
        assert_eq!(
            decide(all(true, true, true), equal),
            Outcome::Updated(Selected::Source)
        );
    }

    #[test]
    fn source_delete_over_untouched_target_deletes() {
        let equal = Equal {
            source_base: false,
            target_base: true,
            source_target: false,
        };
        assert_eq!(decide(all(true, false, true), equal), Outcome::Deleted);
    }

    #[test]
    fn source_insert_reaches_the_target_as_an_insert() {
        let equal = Equal {
            source_base: false,
            target_base: true,
            source_target: false,
        };
        assert_eq!(
            decide(all(false, true, false), equal),
            Outcome::Inserted(Selected::Source)
        );
    }

    #[test]
    fn divergent_edits_conflict_by_shape() {
        let equal = Equal {
            source_base: false,
            target_base: false,
            source_target: false,
        };
        assert_eq!(
            decide(all(true, true, true), equal),
            Outcome::Conflict(ConflictShape::DivergentUpdate)
        );
        assert_eq!(
            decide(all(false, true, true), equal),
            Outcome::Conflict(ConflictShape::DivergentInsert)
        );
        assert_eq!(
            decide(all(true, false, true), equal),
            Outcome::Conflict(ConflictShape::DeleteVsUpdate)
        );
    }

    #[test]
    fn same_edit_on_both_sides_is_unchanged() {
        let equal = Equal {
            source_base: false,
            target_base: false,
            source_target: true,
        };
        assert_eq!(decide(all(true, true, true), equal), Outcome::Unchanged);
        assert_eq!(decide(all(false, true, true), equal), Outcome::Unchanged);
    }
}
