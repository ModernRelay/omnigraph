use thiserror::Error;

pub type Result<T> = std::result::Result<T, RepoError>;

#[derive(Debug, Error)]
pub enum RepoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("schema parse error: {0}")]
    Schema(String),
    #[error("repo not found: {0}")]
    RepoNotFound(String),
    #[error("branch not found: {0}")]
    BranchNotFound(String),
    #[error("branch already exists: {0}")]
    BranchExists(String),
    #[error("invalid branch name: {0}")]
    InvalidBranchName(String),
    #[error("invalid repo: {0}")]
    InvalidRepo(String),
}
