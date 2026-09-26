//! `SortMergeJoin` in its two forms. Streaming: both inputs are id-ordered
//! sources and the join is itself a source merging their batches, holding
//! one batch per side. Build and probe: the right input is unordered, sinks
//! into the join (the pipeline breaker), is sorted at `finalize`, and the
//! left input then probes it one chunk at a time. Both emit matched,
//! left-only and right-only key rows in id order with a null side where a
//! row is absent, and drop right-only rows under a left-outer kind.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_ord::sort::sort_to_indices;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use arrow_select::take::{take, take_record_batch};
use omnigraph_planner::KeyJoinKind;

use super::chunk::{Chunk, KEY};
use super::context::{EngineHooks, Probe};
use super::error::{ExecError, Result};
use super::roles::{Operator, OperatorResult, Sink, SinkResult, Source};

/// Key rows one build side may hold; above it `JoinBuild` refuses with a typed
/// `ResourceLimit`. `omnigraph_planner::gate::over_bound` reads the same value
/// as `Bounds::build_key_cap_rows`.
pub const BUILD_KEY_CAP_ROWS: usize = 1 << 22;
pub const BUILD_KEY_CAP_RESOURCE: &str = "planner_build_side_key_rows";

/// The join's output schema: `_key` once, then every non-key column of the
/// left input, then of the right, all nullable.
pub fn joined_schema(left: &Schema, right: &Schema) -> SchemaRef {
    let mut fields = vec![Field::new(KEY, DataType::Utf8, false)];
    for schema in [left, right] {
        for field in schema.fields() {
            if field.name() != KEY {
                fields.push(
                    Field::new(field.name(), field.data_type().clone(), true)
                        .with_metadata(field.metadata().clone()),
                );
            }
        }
    }
    Arc::new(Schema::new(fields))
}

#[derive(Debug, Clone, Copy)]
pub struct JoinRule {
    pub kind: KeyJoinKind,
    /// A matched pair whose two `_rowaddr` values are equal is
    /// unchanged and never leaves the join.
    pub drop_equal_addresses: bool,
}

struct AddressColumns {
    left: Option<String>,
    right: Option<String>,
}

#[derive(Default)]
struct Pairs {
    left: Vec<Option<u32>>,
    right: Vec<Option<u32>>,
}

impl Pairs {
    fn is_empty(&self) -> bool {
        self.left.is_empty()
    }
}

fn address_of(batch: &RecordBatch, name: Option<&str>, row: usize) -> Option<u64> {
    let column = batch.column_by_name(name?)?;
    let addresses = column.as_any().downcast_ref::<UInt64Array>()?;
    addresses.is_valid(row).then(|| addresses.value(row))
}

/// Merge `left` from `lpos` against `right` from `rpos` by `_key`, until the
/// left batch is exhausted, or the right batch is exhausted and
/// `right_complete` is false (more right batches may follow).
#[allow(clippy::too_many_arguments)]
fn merge_batches(
    left: &RecordBatch,
    lpos: &mut usize,
    right: &RecordBatch,
    rpos: &mut usize,
    right_complete: bool,
    rule: JoinRule,
    addresses: &AddressColumns,
    pairs: &mut Pairs,
) -> Result<()> {
    let left_keys = super::chunk::strings(left, KEY)?;
    let right_keys = super::chunk::strings(right, KEY)?;
    loop {
        let left_row = (*lpos < left.num_rows()).then_some(*lpos);
        let right_row = (*rpos < right.num_rows()).then_some(*rpos);
        match (left_row, right_row) {
            (None, _) => return Ok(()),
            (Some(_), None) if !right_complete => return Ok(()),
            (Some(l), None) => {
                pairs.left.push(Some(l as u32));
                pairs.right.push(None);
                *lpos += 1;
            }
            (Some(l), Some(r)) => match left_keys.value(l).cmp(right_keys.value(r)) {
                std::cmp::Ordering::Less => {
                    pairs.left.push(Some(l as u32));
                    pairs.right.push(None);
                    *lpos += 1;
                }
                std::cmp::Ordering::Greater => {
                    if rule.kind == KeyJoinKind::FullOuter {
                        pairs.left.push(None);
                        pairs.right.push(Some(r as u32));
                    }
                    *rpos += 1;
                }
                std::cmp::Ordering::Equal => {
                    *lpos += 1;
                    *rpos += 1;
                    if rule.drop_equal_addresses
                        && address_of(left, addresses.left.as_deref(), l).is_some()
                        && address_of(left, addresses.left.as_deref(), l)
                            == address_of(right, addresses.right.as_deref(), r)
                    {
                        continue;
                    }
                    pairs.left.push(Some(l as u32));
                    pairs.right.push(Some(r as u32));
                }
            },
        }
    }
}

/// The remaining rows of `right` from `rpos` as right-only pairs.
fn drain_right(right: &RecordBatch, rpos: &mut usize, rule: JoinRule, pairs: &mut Pairs) {
    while *rpos < right.num_rows() {
        if rule.kind == KeyJoinKind::FullOuter {
            pairs.left.push(None);
            pairs.right.push(Some(*rpos as u32));
        }
        *rpos += 1;
    }
}

fn emit(
    left: &RecordBatch,
    right: &RecordBatch,
    pairs: Pairs,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let left_keys = super::chunk::strings(left, KEY)?;
    let right_keys = super::chunk::strings(right, KEY)?;
    let keys: StringArray = pairs
        .left
        .iter()
        .zip(&pairs.right)
        .map(|(l, r)| match (l, r) {
            (Some(l), _) => Some(left_keys.value(*l as usize)),
            (None, Some(r)) => Some(right_keys.value(*r as usize)),
            (None, None) => None,
        })
        .collect();
    let left_indices = UInt32Array::from(pairs.left);
    let right_indices = UInt32Array::from(pairs.right);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(keys));
    for (batch, indices) in [(left, &left_indices), (right, &right_indices)] {
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if field.name() != KEY {
                columns.push(take(column.as_ref(), indices, None)?);
            }
        }
    }
    let actual = joined_schema(&left.schema(), &right.schema());
    debug_assert_eq!(actual.fields().len(), schema.fields().len());
    Ok(RecordBatch::try_new(actual, columns)?)
}

struct Cursor {
    batch: Option<RecordBatch>,
    pos: usize,
    done: bool,
}

impl Cursor {
    fn new() -> Self {
        Self {
            batch: None,
            pos: 0,
            done: false,
        }
    }

    async fn fill(&mut self, source: &mut dyn Source) -> Result<()> {
        while !self.done
            && self
                .batch
                .as_ref()
                .is_none_or(|batch| self.pos >= batch.num_rows())
        {
            match source.next().await? {
                Some(batch) => {
                    self.batch = Some(batch);
                    self.pos = 0;
                }
                None => {
                    self.batch = None;
                    self.done = true;
                }
            }
        }
        Ok(())
    }
}

/// The streaming form: a source over two id-ordered sources.
pub struct MergeJoinSource {
    left: Box<dyn Source>,
    right: Box<dyn Source>,
    left_cursor: Cursor,
    right_cursor: Cursor,
    empty_left: RecordBatch,
    empty_right: RecordBatch,
    rule: JoinRule,
    addresses: AddressColumns,
    schema: SchemaRef,
    hooks: Arc<dyn EngineHooks>,
}

impl MergeJoinSource {
    pub fn new(
        left: Box<dyn Source>,
        right: Box<dyn Source>,
        rule: JoinRule,
        left_address: Option<String>,
        right_address: Option<String>,
        hooks: Arc<dyn EngineHooks>,
    ) -> Self {
        let left_schema = left.schema();
        let right_schema = right.schema();
        Self {
            schema: joined_schema(&left_schema, &right_schema),
            empty_left: RecordBatch::new_empty(left_schema),
            empty_right: RecordBatch::new_empty(right_schema),
            left,
            right,
            left_cursor: Cursor::new(),
            right_cursor: Cursor::new(),
            rule,
            addresses: AddressColumns {
                left: left_address,
                right: right_address,
            },
            hooks,
        }
    }
}

#[async_trait::async_trait]
impl Source for MergeJoinSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            self.left_cursor.fill(self.left.as_mut()).await?;
            self.right_cursor.fill(self.right.as_mut()).await?;
            let mut pairs = Pairs::default();
            let left = self
                .left_cursor
                .batch
                .clone()
                .unwrap_or_else(|| self.empty_left.clone());
            let right = self
                .right_cursor
                .batch
                .clone()
                .unwrap_or_else(|| self.empty_right.clone());
            match (self.left_cursor.done, self.right_cursor.done) {
                (true, true) => return Ok(None),
                (true, false) => {
                    drain_right(&right, &mut self.right_cursor.pos, self.rule, &mut pairs);
                }
                (false, _) => {
                    let examined_from = self.left_cursor.pos;
                    merge_batches(
                        &left,
                        &mut self.left_cursor.pos,
                        &right,
                        &mut self.right_cursor.pos,
                        self.right_cursor.done,
                        self.rule,
                        &self.addresses,
                        &mut pairs,
                    )?;
                    if self.rule.kind == KeyJoinKind::LeftOuter {
                        self.hooks
                            .probe(Probe::RowsExamined(self.left_cursor.pos - examined_from));
                    }
                }
            }
            if pairs.is_empty() {
                continue;
            }
            return Ok(Some(emit(&left, &right, pairs, &self.schema)?));
        }
    }
}

/// The breaker: collects the right input's key rows and sorts them once.
pub struct JoinBuild {
    batches: Vec<RecordBatch>,
    rows: usize,
    schema: SchemaRef,
}

impl JoinBuild {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            batches: Vec::new(),
            rows: 0,
            schema,
        }
    }

    /// The sorted build side, once every batch has sunk.
    pub fn sorted(self) -> Result<RecordBatch> {
        let schema = self
            .batches
            .first()
            .map_or_else(|| self.schema.clone(), RecordBatch::schema);
        let all = concat_batches(&schema, &self.batches)?;
        let keys = super::chunk::strings(&all, KEY)?;
        let indices = sort_to_indices(keys, None, None)?;
        Ok(take_record_batch(&all, &indices)?)
    }
}

#[async_trait::async_trait]
impl Sink for JoinBuild {
    async fn sink(&mut self, chunk: Chunk) -> Result<SinkResult> {
        let chunk = chunk.compact()?;
        self.rows += chunk.batch.num_rows();
        if self.rows > BUILD_KEY_CAP_ROWS {
            return Err(ExecError::ResourceLimit {
                resource: BUILD_KEY_CAP_RESOURCE.to_string(),
                limit: BUILD_KEY_CAP_ROWS as u64,
                actual: self.rows as u64,
            });
        }
        self.batches.push(chunk.batch);
        Ok(SinkResult::NeedMoreInput)
    }
}

/// The probe operator over a sorted build side.
pub struct JoinProbe {
    build: RecordBatch,
    build_pos: usize,
    rule: JoinRule,
    addresses: AddressColumns,
    schema: SchemaRef,
    empty_left: RecordBatch,
    hooks: Arc<dyn EngineHooks>,
}

impl JoinProbe {
    pub fn new(
        build: RecordBatch,
        left_schema: &Schema,
        rule: JoinRule,
        left_address: Option<String>,
        right_address: Option<String>,
        hooks: Arc<dyn EngineHooks>,
    ) -> Self {
        Self {
            schema: joined_schema(left_schema, &build.schema()),
            build,
            build_pos: 0,
            rule,
            addresses: AddressColumns {
                left: left_address,
                right: right_address,
            },
            empty_left: RecordBatch::new_empty(Arc::new(left_schema.clone())),
            hooks,
        }
    }
}

#[async_trait::async_trait]
impl Operator for JoinProbe {
    async fn execute(&mut self, chunk: Chunk, out: &mut Vec<Chunk>) -> Result<OperatorResult> {
        let chunk = chunk.compact()?;
        let mut pairs = Pairs::default();
        let mut lpos = 0usize;
        merge_batches(
            &chunk.batch,
            &mut lpos,
            &self.build,
            &mut self.build_pos,
            true,
            self.rule,
            &self.addresses,
            &mut pairs,
        )?;
        if self.rule.kind == KeyJoinKind::LeftOuter {
            self.hooks
                .probe(Probe::RowsExamined(chunk.batch.num_rows()));
        }
        if !pairs.is_empty() {
            out.push(Chunk::new(emit(
                &chunk.batch,
                &self.build,
                pairs,
                &self.schema,
            )?));
        }
        Ok(OperatorResult::NeedMoreInput)
    }

    async fn finish(&mut self, out: &mut Vec<Chunk>) -> Result<()> {
        let mut pairs = Pairs::default();
        drain_right(&self.build, &mut self.build_pos, self.rule, &mut pairs);
        if !pairs.is_empty() {
            out.push(Chunk::new(emit(
                &self.empty_left,
                &self.build,
                pairs,
                &self.schema,
            )?));
        }
        Ok(())
    }
}
