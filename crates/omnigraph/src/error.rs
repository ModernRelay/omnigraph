use thiserror::Error;

pub type Result<T> = std::result::Result<T, OmniError>;

#[derive(Debug, Clone)]
pub struct MergeConflict {
    pub table_key: String,
    pub row_id: Option<String>,
    pub kind: MergeConflictKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictKind {
    DivergentInsert,
    DivergentUpdate,
    DeleteVsUpdate,
    OrphanEdge,
    UniqueViolation,
    CardinalityViolation,
    ValueConstraintViolation,
}

#[derive(Debug, Error)]
pub enum OmniError {
    #[error("{0}")]
    Compiler(#[from] omnigraph_compiler::error::NanoError),
    #[error("storage: {0}")]
    Lance(String),
    #[error("query: {0}")]
    DataFusion(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Manifest(String),
    #[error("merge conflicts: {0:?}")]
    MergeConflicts(Vec<MergeConflict>),
}
