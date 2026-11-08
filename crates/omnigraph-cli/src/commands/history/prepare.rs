//! Pure history selections, prepared after the existing client-resolution boundary.

use omnigraph_api_types::{ChangeOp, EntityKind};

use super::args::{ChangeKindArg, ChangeOpArg};
use crate::client::ChangeFilterArgs;

/// An explicit token requests exactly one page and preserves the server's
/// default limit. Automatic traversal asks for the public maximum unless set.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Pagination {
    One { token: String, limit: Option<usize> },
    All { limit: usize },
}

impl Pagination {
    pub(super) fn from_args(token: Option<String>, limit: Option<usize>) -> Self {
        match token {
            Some(token) => Self::One { token, limit },
            None => Self::All {
                limit: limit.unwrap_or(omnigraph::changes::COMMIT_CHANGES_MAX_CHANGES),
            },
        }
    }

    pub(super) fn limit(&self) -> Option<usize> {
        match self {
            Self::One { limit, .. } => *limit,
            Self::All { limit } => Some(*limit),
        }
    }
}

pub(super) struct ChangeFilter {
    kinds: Vec<EntityKind>,
    types: Vec<String>,
    ops: Vec<ChangeOp>,
}

impl ChangeFilter {
    pub(super) fn from_args(
        kinds: Vec<ChangeKindArg>,
        types: Vec<String>,
        ops: Vec<ChangeOpArg>,
    ) -> Self {
        Self {
            kinds: kinds.into_iter().map(Into::into).collect(),
            types,
            ops: ops.into_iter().map(Into::into).collect(),
        }
    }

    pub(super) fn as_args(&self) -> ChangeFilterArgs<'_> {
        ChangeFilterArgs {
            kinds: &self.kinds,
            types: &self.types,
            ops: &self.ops,
        }
    }
}

pub(super) enum CommitListSelection {
    Published,
    Unpublished,
    Unattached,
}

impl CommitListSelection {
    pub(super) fn from_args(unpublished: bool, unattached: bool) -> Self {
        if unattached {
            Self::Unattached
        } else if unpublished {
            Self::Unpublished
        } else {
            Self::Published
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Pagination;

    #[test]
    fn explicit_page_preserves_server_default_while_automatic_uses_public_maximum() {
        assert_eq!(
            Pagination::from_args(Some("opaque".into()), None),
            Pagination::One {
                token: "opaque".into(),
                limit: None
            }
        );
        assert_eq!(
            Pagination::from_args(None, None).limit(),
            Some(omnigraph::changes::COMMIT_CHANGES_MAX_CHANGES)
        );
        for limit in [0, 17, usize::MAX] {
            assert_eq!(
                Pagination::from_args(Some("opaque".into()), Some(limit)).limit(),
                Some(limit)
            );
            assert_eq!(
                Pagination::from_args(None, Some(limit)).limit(),
                Some(limit)
            );
        }
    }
}
