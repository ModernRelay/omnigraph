use std::error::Error as StdError;

use crate::error::OmniError;

/// Every failure an operator can raise. `External` carries an engine-typed
/// error through a hook unchanged, so the engine gets its own variant back.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("resource limit exceeded for {resource}: actual {actual}, limit {limit}")]
    ResourceLimit {
        resource: String,
        limit: u64,
        actual: u64,
    },
    /// A one-pass sorted full-width scan was refused before its first row;
    /// the caller re-plans with late materialization only.
    #[error("one-pass scan refused before the first row: {resource}")]
    OnePassRefused { resource: String },
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    Lance(#[from] lance::Error),
    #[error("{0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("{0}")]
    External(Box<dyn StdError + Send + Sync + 'static>),
    #[error("{context}: {source}")]
    Context {
        context: String,
        #[source]
        source: Box<ExecError>,
    },
}

pub type Result<T> = std::result::Result<T, ExecError>;

/// A typed limit crosses as itself, so `resource()` keeps naming it; every
/// other engine error rides through as `External`.
impl From<OmniError> for ExecError {
    fn from(error: OmniError) -> Self {
        match error {
            OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } => Self::ResourceLimit {
                resource,
                limit,
                actual,
            },
            other => Self::external(other),
        }
    }
}

impl ExecError {
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(detail.into())
    }

    pub fn external(error: impl StdError + Send + Sync + 'static) -> Self {
        Self::External(Box::new(error))
    }

    pub fn with_context(self, context: impl std::fmt::Display) -> Self {
        Self::Context {
            context: context.to_string(),
            source: Box::new(self),
        }
    }

    /// The resource a typed limit names, at any depth of context.
    pub fn resource(&self) -> Option<&str> {
        match self {
            Self::ResourceLimit { resource, .. } | Self::OnePassRefused { resource } => {
                Some(resource)
            }
            Self::Context { source, .. } => source.resource(),
            _ => None,
        }
    }
}
