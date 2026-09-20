//! `Page`: the row and byte budget a page names for explain. The budget's
//! authority is the consumer, which charges each serialized change and ends
//! the page; the operator passes every chunk through unchanged.

use super::chunk::Chunk;
use super::error::Result;
use super::roles::{Operator, OperatorResult};

pub struct Page {
    pub rows: usize,
    pub bytes: u64,
}

#[async_trait::async_trait]
impl Operator for Page {
    async fn execute(&mut self, chunk: Chunk, out: &mut Vec<Chunk>) -> Result<OperatorResult> {
        out.push(chunk);
        Ok(OperatorResult::NeedMoreInput)
    }
}
