use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::de::DeserializeOwned;

use crate::error::{NanoError, Result};
use crate::json_output::{record_batches_to_json_rows, record_batches_to_rust_json_rows};
use crate::plan::physical::MutationExecResult;

#[derive(Debug, Clone)]
pub struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl QueryResult {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
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
            .map_err(|err| NanoError::Execution(err.to_string()))
    }

    pub fn to_sdk_json(&self) -> serde_json::Value {
        serde_json::Value::Array(record_batches_to_json_rows(&self.batches))
    }

    pub fn to_rust_json(&self) -> serde_json::Value {
        serde_json::Value::Array(record_batches_to_rust_json_rows(&self.batches))
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.to_rust_json()).map_err(|err| {
            NanoError::Execution(format!("failed to deserialize query result: {}", err))
        })
    }

    pub fn to_arrow_ipc(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buffer, &self.schema)?;
        for batch in &self.batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        drop(writer);
        Ok(buffer)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MutationResult {
    pub affected_nodes: usize,
    pub affected_edges: usize,
}

impl MutationResult {
    pub fn to_sdk_json(&self) -> serde_json::Value {
        serde_json::json!({
            "affectedNodes": self.affected_nodes,
            "affectedEdges": self.affected_edges,
        })
    }

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
    pub fn to_sdk_json(&self) -> serde_json::Value {
        match self {
            Self::Query(result) => result.to_sdk_json(),
            Self::Mutation(result) => result.to_sdk_json(),
        }
    }

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

    use arrow_array::Int64Array;
    use arrow_ipc::reader::StreamReader;
    use serde::Deserialize;

    use super::*;

    #[test]
    fn query_result_arrow_ipc_round_trips_empty_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let result = QueryResult::new(schema.clone(), vec![]);

        let encoded = result.to_arrow_ipc().expect("encode empty result");
        let reader = StreamReader::try_new(Cursor::new(encoded), None).expect("open stream");

        assert_eq!(reader.schema().as_ref(), schema.as_ref());
        assert_eq!(reader.count(), 0);
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
            result.to_rust_json(),
            serde_json::json!([{
                "signed": i64::MIN,
                "unsigned": u64::MAX,
            }])
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
