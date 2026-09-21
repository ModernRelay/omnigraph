//! The three roles a physical node's operator can take. A `Source` yields
//! morsels; an `Operator` maps one chunk to zero or more chunks and flushes
//! once its source is exhausted; a `Sink` ends a pipeline and is where a
//! pipeline breaker materializes.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

use super::chunk::Chunk;
use super::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorResult {
    NeedMoreInput,
    /// The operator will emit nothing more; the executor stops the pipeline.
    Finished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkResult {
    NeedMoreInput,
    Finished,
}

#[async_trait]
pub trait Source: Send {
    /// The schema every morsel carries, known before the first one so a join
    /// can type the null side of an input that yields nothing.
    fn schema(&self) -> SchemaRef;

    /// The next morsel, or `None` once exhausted.
    async fn next(&mut self) -> Result<Option<RecordBatch>>;
}

#[async_trait]
pub trait Operator: Send {
    async fn execute(&mut self, chunk: Chunk, out: &mut Vec<Chunk>) -> Result<OperatorResult>;

    /// Flush what the operator held back, once the source is exhausted.
    async fn finish(&mut self, _out: &mut Vec<Chunk>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait Sink: Send {
    async fn sink(&mut self, chunk: Chunk) -> Result<SinkResult>;

    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }
}
