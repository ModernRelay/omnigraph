use std::io::Write;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use serde::de::DeserializeOwned;

use crate::error::{CompilerError, Result};
use crate::json_output::{record_batches_to_json_bytes, record_batches_to_json_lines};

/// The bytes of a batch's buffers one Arrow IPC record batch message carries
/// at most, reckoned from the batch's memory size spread evenly over its
/// rows; a message always carries at least one row.
const ARROW_IPC_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default)]
pub struct MutationExecResult {
    pub affected_nodes: usize,
    pub affected_edges: usize,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    ranking: Option<Ranking>,
}

/// How a ranked read ordered its rows (RFC 0047, "The ranking a read
/// reports"): the search function its order names, whether the run's
/// search compared every row it could rank, and the returned columns that
/// hold the score it ordered by. A read whose order is no search has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ranking {
    pub by: SearchFunction,
    /// No row the search admitted and did not return outranks a returned
    /// row. `bm25` scores every match. A `nearest` search is exhaustive when
    /// its last attempt searched every partition of the vector index,
    /// scored every row flat, or returned every row it admitted, and not
    /// when a probe cap left partitions unsearched. An `rrf()` is exhaustive
    /// when its `nearest` arms are.
    pub exhaustive: bool,
    /// The returned columns that hold the ranking's own score, in return
    /// order; a column computed from the score is not one.
    pub score_columns: Vec<String>,
}

/// The search function a ranked read orders by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchFunction {
    Nearest,
    Bm25,
    Rrf,
}

impl SearchFunction {
    /// Whether a larger score ranks first: a `nearest` distance ranks the
    /// smallest first, a `bm25` or fused score the largest.
    pub fn descending(self) -> bool {
        self != Self::Nearest
    }
}

impl QueryResult {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches,
            ranking: None,
        }
    }

    /// This result with the ranking of the run that produced it.
    pub fn with_ranking(mut self, ranking: Option<Ranking>) -> Self {
        self.ranking = ranking;
        self
    }

    pub fn ranking(&self) -> Option<&Ranking> {
        self.ranking.as_ref()
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn concat_batches(&self) -> Result<RecordBatch> {
        if self.batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        arrow_select::concat::concat_batches(&self.schema, &self.batches)
            .map_err(|err| CompilerError::Execution(err.to_string()))
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        record_batches_to_json_bytes(&self.batches)
    }

    /// One JSON object per row, `\n`-terminated.
    pub fn to_json_lines(&self) -> Result<Vec<u8>> {
        record_batches_to_json_lines(&self.batches)
    }

    pub fn to_rust_json(&self) -> Result<serde_json::Value> {
        serde_json::from_slice(&self.to_json_bytes()?).map_err(|err| {
            CompilerError::Execution(format!("query result rendered invalid JSON: {}", err))
        })
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.to_json_bytes()?).map_err(|err| {
            CompilerError::Execution(format!("failed to deserialize query result: {}", err))
        })
    }

    /// The result as one Arrow IPC stream in memory; see
    /// [`Self::write_arrow_ipc`].
    pub fn to_arrow_ipc(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.write_arrow_ipc(&mut buffer)?;
        Ok(buffer)
    }

    /// Write the result to `writer` as one Arrow IPC stream: the schema
    /// message, record batch messages, then the end-of-stream marker. Each
    /// batch goes out in slices of about `ARROW_IPC_MESSAGE_BYTES` of its
    /// buffers, and a slice's message carries only that slice's rows, so the
    /// encoder never holds a second copy of the whole result. A batch with no
    /// rows writes no message. `writer` receives several small writes per
    /// message; buffer it. A failed write is the I/O error it is.
    pub fn write_arrow_ipc<W: Write>(&self, writer: W) -> Result<()> {
        self.write_arrow_ipc_in_messages_of(writer, ARROW_IPC_MESSAGE_BYTES)
    }

    fn write_arrow_ipc_in_messages_of<W: Write>(
        &self,
        writer: W,
        message_bytes: usize,
    ) -> Result<()> {
        let mut stream = StreamWriter::try_new(writer, &self.schema).map_err(ipc_error)?;
        for batch in &self.batches {
            let rows = batch.num_rows();
            let step = message_rows(batch, message_bytes);
            let mut offset = 0;
            while offset < rows {
                let length = step.min(rows - offset);
                stream
                    .write(&batch.slice(offset, length))
                    .map_err(ipc_error)?;
                offset += length;
            }
        }
        stream.finish().map_err(ipc_error)
    }
}

/// How many rows of `batch` one IPC message carries to hold about
/// `message_bytes` of its buffers, and at least one.
fn message_rows(batch: &RecordBatch, message_bytes: usize) -> usize {
    let rows = batch.num_rows().max(1);
    let row_bytes = (batch.get_array_memory_size() / rows).max(1);
    (message_bytes / row_bytes).clamp(1, rows)
}

/// The IPC writer reports a failed write to its writer as an Arrow error;
/// keep it the I/O error it is.
fn ipc_error(error: ArrowError) -> CompilerError {
    match error {
        ArrowError::IoError(_, error) => CompilerError::Io(error),
        error => CompilerError::Arrow(error),
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MutationResult {
    pub affected_nodes: usize,
    pub affected_edges: usize,
}

impl MutationResult {
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("affected_nodes", DataType::UInt64, false),
            Field::new("affected_edges", DataType::UInt64, false),
        ]));
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![self.affected_nodes as u64])),
                Arc::new(UInt64Array::from(vec![self.affected_edges as u64])),
            ],
        )?)
    }
}

impl From<MutationExecResult> for MutationResult {
    fn from(value: MutationExecResult) -> Self {
        Self {
            affected_nodes: value.affected_nodes,
            affected_edges: value.affected_edges,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RunResult {
    Query(QueryResult),
    Mutation(MutationResult),
}

impl RunResult {
    pub fn into_record_batches(self) -> Result<Vec<RecordBatch>> {
        match self {
            Self::Query(result) => Ok(result.into_batches()),
            Self::Mutation(result) => Ok(vec![result.to_record_batch()?]),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder, StringBuilder};
    use arrow_array::{ArrayRef, Date32Array, Int32Array, Int64Array, StringArray};
    use arrow_ipc::reader::StreamReader;
    use serde::Deserialize;

    use super::*;

    #[test]
    fn query_result_arrow_ipc_round_trips_empty_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        // No batch, and the one empty batch the engine answers a read with
        // that matched nothing: the schema alone, and no record batch.
        for batches in [vec![], vec![RecordBatch::new_empty(schema.clone())]] {
            let result = QueryResult::new(schema.clone(), batches);

            let encoded = result.to_arrow_ipc().expect("encode empty result");
            let reader = StreamReader::try_new(Cursor::new(encoded), None).expect("open stream");

            assert_eq!(reader.schema().as_ref(), schema.as_ref());
            assert_eq!(reader.count(), 0);
        }
    }

    /// Five rows over the column shapes a result carries: a string with a
    /// null, an integer, a date, a vector and a list.
    fn mixed_batch() -> RecordBatch {
        let mut vectors = FixedSizeListBuilder::new(Float32Builder::new(), 3);
        let mut lists = ListBuilder::new(StringBuilder::new());
        for row in 0..5_u8 {
            vectors.values().append_slice(&[f32::from(row), 0.5, -1.0]);
            vectors.append(true);
            // Row 3's list is null; the others hold `row` items.
            if row != 3 {
                for item in 0..row {
                    lists.values().append_value(format!("tag{item}"));
                }
            }
            lists.append(row != 3);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("ccc"),
                Some("dd"),
                Some(""),
            ])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Date32Array::from(vec![19_723, 19_724, 0, -1, 7])),
            Arc::new(vectors.finish()),
            Arc::new(lists.finish()),
        ];
        let schema = Schema::new(
            ["name", "age", "since", "embedding", "tags"]
                .into_iter()
                .zip(&columns)
                .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(Arc::new(schema), columns).expect("batch")
    }

    fn read_stream(encoded: &[u8]) -> (SchemaRef, Vec<RecordBatch>) {
        let reader = StreamReader::try_new(Cursor::new(encoded), None).expect("open stream");
        let schema = reader.schema();
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("decode batches");
        (schema, batches)
    }

    #[test]
    fn write_arrow_ipc_slices_a_batch_into_messages_of_the_budget() {
        let batch = mixed_batch();
        let result = QueryResult::new(batch.schema(), vec![batch.clone()]);
        for (message_bytes, messages) in [(1, 5), (usize::MAX, 1)] {
            let mut encoded = Vec::new();
            result
                .write_arrow_ipc_in_messages_of(&mut encoded, message_bytes)
                .expect("write stream");

            let (schema, batches) = read_stream(&encoded);
            assert_eq!(schema, batch.schema());
            assert_eq!(batches.len(), messages, "{message_bytes}");
            assert_eq!(
                arrow_select::concat::concat_batches(&schema, &batches).expect("concat"),
                batch,
                "{message_bytes}"
            );
        }
    }

    /// A one-row message carries that row's bytes and its own framing, never
    /// the buffers the whole batch shares.
    #[test]
    fn a_sliced_message_carries_only_its_own_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let text = StringArray::from_iter_values((0..256).map(|row| format!("{row:0>1024}")));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(text)]).expect("batch");
        let result = QueryResult::new(schema, vec![batch]);

        let mut whole = Vec::new();
        result
            .write_arrow_ipc_in_messages_of(&mut whole, usize::MAX)
            .expect("write whole");
        let mut sliced = Vec::new();
        result
            .write_arrow_ipc_in_messages_of(&mut sliced, 1)
            .expect("write sliced");

        assert_eq!(read_stream(&sliced).1.len(), 256);
        assert!(
            sliced.len() < 2 * whole.len(),
            "256 one-row messages took {} bytes; the whole batch takes {}",
            sliced.len(),
            whole.len()
        );
    }

    #[test]
    fn a_failed_arrow_ipc_write_is_the_io_error_it_is() {
        struct Closed;
        impl Write for Closed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "closed",
                ))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let batch = mixed_batch();
        let error = QueryResult::new(batch.schema(), vec![batch])
            .write_arrow_ipc(Closed)
            .unwrap_err();
        assert!(
            matches!(&error, CompilerError::Io(io) if io.kind() == std::io::ErrorKind::BrokenPipe),
            "{error:?}"
        );
    }

    #[test]
    fn query_result_arrow_ipc_round_trips_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1_u64, 2_u64]))],
        )
        .expect("batch");
        let result = QueryResult::new(schema.clone(), vec![batch]);

        let encoded = result.to_arrow_ipc().expect("encode result");
        let mut reader = StreamReader::try_new(Cursor::new(encoded), None).expect("open stream");
        let decoded = reader.next().expect("first batch").expect("decode batch");

        assert_eq!(reader.schema().as_ref(), schema.as_ref());
        assert_eq!(decoded.num_rows(), 2);
        assert_eq!(decoded.schema().as_ref(), schema.as_ref());
    }

    #[test]
    fn query_result_num_rows_and_concat_cover_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1_u64, 2_u64]))],
        )
        .expect("batch1");
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![3_u64]))],
        )
        .expect("batch2");
        let result = QueryResult::new(schema.clone(), vec![batch1, batch2]);

        assert_eq!(result.num_rows(), 3);

        let concatenated = result.concat_batches().expect("concat batches");
        let ids = concatenated
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64 ids");
        assert_eq!(concatenated.schema().as_ref(), schema.as_ref());
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    #[test]
    fn query_result_concat_empty_batches_returns_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let result = QueryResult::new(schema.clone(), vec![]);

        let concatenated = result.concat_batches().expect("concat empty");

        assert_eq!(concatenated.schema().as_ref(), schema.as_ref());
        assert_eq!(concatenated.num_rows(), 0);
    }

    #[test]
    fn query_result_to_rust_json_preserves_wide_integers() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("signed", DataType::Int64, false),
            Field::new("unsigned", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![i64::MIN])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .expect("batch");
        let result = QueryResult::new(schema, vec![batch]);

        assert_eq!(
            result.to_rust_json().expect("rows"),
            serde_json::json!([{
                "signed": i64::MIN,
                "unsigned": u64::MAX,
            }])
        );
        assert_eq!(
            result.to_json_bytes().expect("bytes"),
            br#"[{"signed":-9223372036854775808,"unsigned":18446744073709551615}]"#
        );
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct PersonRow {
        id: u64,
        age: i64,
    }

    #[test]
    fn query_result_deserialize_decodes_rust_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("age", DataType::Int64, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1_u64])),
                Arc::new(Int64Array::from(vec![40_i64])),
            ],
        )
        .expect("batch1");
        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                Arc::new(Int64Array::from(vec![-5_i64])),
            ],
        )
        .expect("batch2");
        let result = QueryResult::new(batch1.schema(), vec![batch1, batch2]);

        let rows: Vec<PersonRow> = result.deserialize().expect("deserialize rows");

        assert_eq!(
            rows,
            vec![
                PersonRow { id: 1, age: 40 },
                PersonRow {
                    id: u64::MAX,
                    age: -5,
                },
            ]
        );
    }
}
