use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSpan {
    pub start: usize,
    pub end: usize,
}

impl SourceSpan {
    pub fn new(start: usize, end: usize) -> Self {
        Self {
            start,
            end: end.max(start.saturating_add(1)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseDiagnostic {
    pub message: String,
    pub span: Option<SourceSpan>,
}

impl ParseDiagnostic {
    pub fn new(message: String, span: Option<SourceSpan>) -> Self {
        Self { message, span }
    }
}

impl std::fmt::Display for ParseDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ParseDiagnostic {}

#[derive(Debug, Error)]
pub enum NanoError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error(
        "@unique constraint violation on {type_name}.{property}: duplicate value '{value}' at rows {first_row} and {second_row}"
    )]
    UniqueConstraint {
        type_name: String,
        property: String,
        value: String,
        first_row: usize,
        second_row: usize,
    },

    #[error("plan error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error(transparent)]
    DataFusion(#[from] datafusion_common::DataFusionError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("lance error: {0}")]
    Lance(String),

    #[error("manifest error: {0}")]
    Manifest(String),
}

pub type Result<T> = std::result::Result<T, NanoError>;
