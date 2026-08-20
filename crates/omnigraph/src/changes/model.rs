//! Public graph-vocabulary change model.
//!
//! These types are the deliberate SDK surface for per-commit entity changes
//! and the change feed. They speak graph vocabulary only: node/edge kind,
//! opaque graph type identity plus name, logical entity `id`, operation, and
//! exact logical before/after images with edge endpoints inside each image.
//! Physical vocabulary — table keys, incarnation numbers, Lance versions, row
//! addresses — never appears here.
//!
//! The `Serialize` derives define the canonical byte-accounting encoding used
//! to enforce page byte ceilings; wire DTOs live in `omnigraph-api-types` and
//! map from these types explicitly.

use serde::Serialize;

use crate::db::commit_graph::GraphCommit;

/// Default number of entity changes per bounded page.
pub const COMMIT_CHANGES_DEFAULT_ROWS: usize = 1_000;
/// Public ceiling on entity changes per bounded page.
pub const COMMIT_CHANGES_MAX_ROWS: usize = 8_192;
/// Default per-page serialized-byte PACKING target — how many small changes
/// share one page, not a wall a single change must fit under. A change whose two
/// images exceed the remaining budget is delivered on its own page (forward
/// progress; see `enumerate`), so this never makes a legal committed change
/// undeliverable.
pub const COMMIT_CHANGES_DEFAULT_BYTES: u64 = 4 * 1024 * 1024;
/// Public ceiling on the per-page byte packing target a caller may request.
/// This bounds how much a caller may pack per page; it is NOT a ceiling on a
/// single change's size — an `Update` serializes two entity images and managed
/// Blobs inline as base64 (~4/3), so one legal change can exceed this and is
/// still delivered solo. (Historically this equalled the write-path keyed
/// transaction budget, which wrongly implied a single change had to fit under it.)
pub const COMMIT_CHANGES_MAX_BYTES: u64 = 32 * 1024 * 1024;
/// Default number of graph commits one feed poll examines.
pub const CHANGE_FEED_DEFAULT_COMMITS_PER_POLL: usize = 128;
/// Public ceiling on graph commits examined by one feed poll.
pub const CHANGE_FEED_MAX_COMMITS_PER_POLL: usize = 512;

/// Engine mirror of the compiler's reserved Lance virtual column list. The
/// compiler's `is_reserved_storage_system_column` is crate-private there, so
/// this exact five-name list is pinned by a unit test below. Matching is
/// exact: a legal user property such as `_rowdy` or `_row_custom` is never
/// hidden by a prefix heuristic.
pub(crate) fn is_reserved_storage_system_column(name: &str) -> bool {
    matches!(
        name,
        "_rowid"
            | "_rowaddr"
            | "_rowoffset"
            | "_row_created_at_version"
            | "_row_last_updated_at_version"
    )
}

/// Logical entity namespace of one change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeEntityKind {
    Node,
    Edge,
}

impl ChangeEntityKind {
    /// Frozen ordering rank: nodes before edges within one commit block.
    pub(crate) fn rank(self) -> u8 {
        match self {
            Self::Node => 0,
            Self::Edge => 1,
        }
    }
}

impl From<super::EntityKind> for ChangeEntityKind {
    fn from(kind: super::EntityKind) -> Self {
        match kind {
            super::EntityKind::Node => Self::Node,
            super::EntityKind::Edge => Self::Edge,
        }
    }
}

/// Logical operation of one change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOpKind {
    Insert,
    Update,
    Delete,
}

impl ChangeOpKind {
    /// Frozen ordering rank, part of the page-token v1 contract.
    pub(crate) fn rank(self) -> u8 {
        match self {
            Self::Insert => 0,
            Self::Update => 1,
            Self::Delete => 2,
        }
    }
}

/// Opaque graph-scoped type identity plus the graph-schema name. The `id`
/// survives a supported rename and changes after drop/re-add; it is never a
/// retained table-key compatibility selector, dataset, path, or incarnation
/// identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GraphTypeRef {
    pub id: String,
    pub name: String,
}

/// Edge endpoints as graph references, using the public mutation/load
/// vocabulary. Endpoints belong to each image, so an endpoint-moving update
/// has distinct before and after endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EntityEndpoints {
    pub from: String,
    pub to: String,
}

/// One exact logical entity image, decoded with the commit-era physical
/// schema. `properties` carries user-schema keys verbatim; the logical `id`
/// and edge endpoint fields are hoisted out of it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EntityImage {
    pub properties: serde_json::Map<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<EntityEndpoints>,
}

/// One entity change. Cause is stated once on the enclosing block.
///
/// An insert has only `after`; an update has exact `before` and `after`; a
/// delete has only `before`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GraphEntityChange {
    pub kind: ChangeEntityKind,
    pub entity_type: GraphTypeRef,
    pub id: String,
    pub op: ChangeOpKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<EntityImage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<EntityImage>,
}

/// The commit cause of one change block, stated once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChangeCause {
    pub graph_commit_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_commit_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merged_parent_commit_id: Option<String>,
    /// Branch the commit originally landed on; `None` means main. The selected
    /// feed branch is request context and never rewrites this.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authored_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    /// Authorship time in Unix epoch microseconds. Minted before dataset
    /// effects and stable across retries and recovery — deliberately not
    /// labeled a commit or publication time.
    pub authored_at: i64,
}

impl From<&GraphCommit> for ChangeCause {
    fn from(commit: &GraphCommit) -> Self {
        Self {
            graph_commit_id: commit.graph_commit_id.clone(),
            parent_commit_id: commit.parent_commit_id.clone(),
            merged_parent_commit_id: commit.merged_parent_commit_id.clone(),
            authored_branch: commit.manifest_branch.clone(),
            actor_id: commit.actor_id.clone(),
            authored_at: commit.created_at,
        }
    }
}

/// All entity changes one graph commit made relative to its first parent.
/// A physical-only commit produces an empty block.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GraphChangeBlock {
    pub cause: ChangeCause,
    pub changes: Vec<GraphEntityChange>,
}

/// One bounded page of a finite commit entity diff. `next_page_token` is
/// absent exactly when the block is complete; there is no separate
/// completeness flag and never a feed cursor.
#[derive(Debug, Clone)]
pub struct CommitChangesPage {
    pub block: GraphChangeBlock,
    pub next_page_token: Option<String>,
}

/// Graph-vocabulary feed filter. Empty dimensions match everything; the
/// canonical digest of this scope binds page tokens and feed cursors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeFeedScope {
    pub kinds: Option<Vec<ChangeEntityKind>>,
    pub type_names: Option<Vec<String>>,
    pub ops: Option<Vec<ChangeOpKind>>,
}

impl ChangeFeedScope {
    pub(crate) fn wants_kind(&self, kind: ChangeEntityKind) -> bool {
        self.kinds
            .as_ref()
            .is_none_or(|kinds| kinds.contains(&kind))
    }

    pub(crate) fn wants_type_name(&self, name: &str) -> bool {
        self.type_names
            .as_ref()
            .is_none_or(|names| names.iter().any(|candidate| candidate == name))
    }

    pub(crate) fn wants_op(&self, op: ChangeOpKind) -> bool {
        self.ops.as_ref().is_none_or(|ops| ops.contains(&op))
    }
}

/// Explicit start mode for the first feed request. A missing cursor is never
/// an implicit alias for `Beginning`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeFeedStart {
    /// Capture the current head and position the cursor after it.
    Now,
    /// Begin after an exact commit on the captured branch's first-parent chain.
    AfterCommit(String),
    /// Begin before the chain's root, including inherited history on a named
    /// branch.
    Beginning,
}

/// Where one poll resumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeFeedPosition {
    /// First request: choose an explicit start mode.
    Start(ChangeFeedStart),
    /// Resume the durable feed after the cursor's last complete commit.
    Cursor(String),
    /// Continue one interrupted bounded poll at its captured cut.
    PageToken(String),
}

/// One feed poll request. `None` limits take the server-owned defaults; the
/// byte and commit ceilings exist for SDK callers and tests — transports do
/// not expose them.
#[derive(Debug, Clone)]
pub struct ChangeFeedRequest {
    /// Branch whose first-parent history is polled; `None` means main.
    pub branch: Option<String>,
    pub position: ChangeFeedPosition,
    pub scope: ChangeFeedScope,
    pub max_changes: Option<usize>,
    pub max_bytes: Option<u64>,
    pub max_commits: Option<usize>,
}

/// How one feed page ended.
#[derive(Debug, Clone)]
pub enum ChangeFeedContinuation {
    /// The page ended inside a block. Continue with the page token; no durable
    /// cursor is advanced, so a caller can never checkpoint a partial block.
    MidBlock { page_token: String },
    /// The page ended after a complete commit. The cursor is durable;
    /// `caught_up` is true when the captured cut was reached.
    AtBlockBoundary { cursor: String, caught_up: bool },
}

/// One bounded feed poll result: zero or more complete blocks, plus a trailing
/// partial block exactly when the continuation is `MidBlock`.
#[derive(Debug, Clone)]
pub struct ChangeFeedPage {
    pub blocks: Vec<GraphChangeBlock>,
    pub continuation: ChangeFeedContinuation,
}

/// The baseline handshake: one exact entity snapshot and the cursor that
/// resumes the feed immediately after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeBaseline {
    pub snapshot_commit_id: String,
    pub resume_cursor: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the exact five reserved Lance virtual column names. If Lance adds
    /// a sixth, the compiler's reserved list and this mirror must move
    /// together in one audited change.
    #[test]
    fn reserved_storage_system_columns_are_exactly_the_five_lance_names() {
        for name in [
            "_rowid",
            "_rowaddr",
            "_rowoffset",
            "_row_created_at_version",
            "_row_last_updated_at_version",
        ] {
            assert!(is_reserved_storage_system_column(name), "{name}");
        }
        for name in [
            "_row",
            "_rowdy",
            "_row_custom",
            "_row_created_at",
            "id",
            "rowid",
        ] {
            assert!(!is_reserved_storage_system_column(name), "{name}");
        }
    }

    #[test]
    fn operation_ranks_are_frozen() {
        assert_eq!(ChangeOpKind::Insert.rank(), 0);
        assert_eq!(ChangeOpKind::Update.rank(), 1);
        assert_eq!(ChangeOpKind::Delete.rank(), 2);
    }
}
