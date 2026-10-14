pub(crate) mod candidate_scan;
pub(crate) mod enumerate;
pub(crate) mod feed;
pub mod model;
pub(crate) mod row_compare;
pub(crate) mod token;

pub use model::{
    BranchReadAnswer, CHANGE_FEED_DEFAULT_COMMITS_PER_POLL, CHANGE_FEED_MAX_COMMITS_PER_POLL,
    COMMIT_CHANGES_DEFAULT_BYTES, COMMIT_CHANGES_DEFAULT_CHANGES, COMMIT_CHANGES_MAX_BYTES,
    COMMIT_CHANGES_MAX_CHANGES, ChangeBaseline, ChangeCause, ChangeEntityKind,
    ChangeFeedContinuation, ChangeFeedPage, ChangeFeedPosition, ChangeFeedRequest, ChangeFeedScope,
    ChangeFeedStart, ChangeOpKind, CommitChangesPage, EntityEndpoints, EntityImage,
    GraphChangeBlock, GraphDiffPage, GraphDiffRequest, GraphDiffSides, GraphEntityChange,
    GraphTypeRef, MergeBase,
};

use std::collections::BTreeMap;

use crate::db::DatasetEntry;
use crate::db::Snapshot;
use crate::db::manifest::TableIdentity;

// ─── Types ──────────────────────────────────────────────────────────────────

/// The namespace of a graph entity: node or edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityKind {
    Node,
    Edge,
}

// ─── Table lifetimes ────────────────────────────────────────────────────────

fn parse_table_key(table_key: &str) -> (EntityKind, &str) {
    if let Some(name) = table_key.strip_prefix("node:") {
        (EntityKind::Node, name)
    } else if let Some(name) = table_key.strip_prefix("edge:") {
        (EntityKind::Edge, name)
    } else {
        (EntityKind::Node, table_key)
    }
}

/// One immutable table lifetime whose physical state differs between two
/// graph snapshots.
///
/// Identity, not alias, pairs the endpoints. A rename therefore stays one
/// interval (and is elided when its physical state did not move), while a
/// drop/re-add under the same public name remains two distinct lifetimes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TableChangeInterval<'a> {
    pub(crate) identity: TableIdentity,
    pub(crate) from: Option<&'a DatasetEntry>,
    pub(crate) to: Option<&'a DatasetEntry>,
}

impl<'a> TableChangeInterval<'a> {
    fn type_key(&self) -> &'a str {
        &self
            .to
            .or(self.from)
            .expect("a changed interval has at least one endpoint")
            .type_key
    }
}

/// Derive changed table lifetimes in stable graph-visible order: destination
/// alias (or source alias for a removal), then immutable identity.
///
/// This is the graph-commit CDC pruning layer: later row enumeration only
/// needs to inspect these exact endpoint pairs. It persists no parallel change
/// log and does not infer identity from an alias, path, or Lance version.
pub(crate) fn changed_table_intervals<'a>(
    from: &'a Snapshot,
    to: &'a Snapshot,
) -> Vec<TableChangeInterval<'a>> {
    let mut by_identity =
        BTreeMap::<TableIdentity, (Option<&'a DatasetEntry>, Option<&'a DatasetEntry>)>::new();
    for entry in from.datasets() {
        by_identity.entry(entry.identity).or_default().0 = Some(entry);
    }
    for entry in to.datasets() {
        by_identity.entry(entry.identity).or_default().1 = Some(entry);
    }

    let mut intervals = by_identity
        .into_iter()
        .filter_map(|(identity, (from, to))| {
            (!same_state(from, to)).then_some(TableChangeInterval { identity, from, to })
        })
        .collect::<Vec<_>>();
    intervals.sort_by(|left, right| {
        left.type_key()
            .cmp(right.type_key())
            .then_with(|| left.identity.cmp(&right.identity))
    });
    intervals
}

/// Whether two registrations name one table state, the pin included: two
/// branches staging detached on one dataset share `published_dataset_version`
/// while their pins differ (RFC "Detached-only tables", Branches).
fn same_state(a: Option<&DatasetEntry>, b: Option<&DatasetEntry>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a.published_dataset_version == b.published_dataset_version
                && a.native_dataset_branch == b.native_dataset_branch
                && a.version_metadata.staged_version() == b.version_metadata.staged_version()
                && a.version_metadata.transaction_uuid() == b.version_metadata.transaction_uuid()
        }
        _ => false,
    }
}
