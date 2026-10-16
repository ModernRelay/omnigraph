use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::ir::QueryIR;
use serde::{Deserialize, Serialize};

/// Where a table's rows come from: a pinned dataset in the graph's store,
/// or the engine catalogue the running binary answers from memory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum TableSource {
    /// The dataset and its native Lance branch (RFC 0067 and detached-only
    /// tables), so a routed plan reads exactly the owner the executor reads.
    Dataset {
        dataset_path: String,
        native_branch: Option<String>,
    },
    /// A built-in `@` type (`omnigraph_compiler::catalogue`): rows derived
    /// from the binary's registries, never a dataset, never a snapshot.
    Catalogue,
}

impl TableSource {
    /// The explain spelling.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Dataset { .. } => "dataset",
            Self::Catalogue => "catalogue",
        }
    }
}

/// One table image: its type key and the source its rows come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    pub type_key: String,
    #[serde(flatten)]
    pub source: TableSource,
}

impl TableRef {
    pub fn dataset(
        type_key: impl Into<String>,
        dataset_path: impl Into<String>,
        native_branch: Option<String>,
    ) -> Self {
        Self {
            type_key: type_key.into(),
            source: TableSource::Dataset {
                dataset_path: dataset_path.into(),
                native_branch,
            },
        }
    }

    pub fn catalogue(type_key: impl Into<String>) -> Self {
        Self {
            type_key: type_key.into(),
            source: TableSource::Catalogue,
        }
    }

    /// The bare node type name of a `node:<Type>` table; `None` for any
    /// other table.
    pub fn node_type_name(&self) -> Option<&str> {
        self.type_key.strip_prefix("node:")
    }

    pub fn is_catalogue(&self) -> bool {
        matches!(self.source, TableSource::Catalogue)
    }

    /// The dataset path and native branch of a dataset-backed table.
    pub fn dataset_owner(&self) -> Option<(&str, Option<&str>)> {
        match &self.source {
            TableSource::Dataset {
                dataset_path,
                native_branch,
            } => Some((dataset_path.as_str(), native_branch.as_deref())),
            TableSource::Catalogue => None,
        }
    }
}

/// One side of a diff: a table at one pinned version, with the system-column
/// spellings that version's image carries.
#[derive(Debug, Clone)]
pub struct Side {
    pub table: TableRef,
    pub version: u64,
    pub columns: SystemColumns,
}

/// The row and serialized-byte budget of one page; a packing target the
/// engine enforces by charging each emitted change, never a memory bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PageBudgetSpec {
    pub rows: usize,
    pub bytes: u64,
}

/// Which change operations the caller wants emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ScopeSpec {
    pub inserts: bool,
    pub updates: bool,
    pub deletes: bool,
}

/// The gate's only input type. Each variant is one operation the engine may
/// hand to the planner. Read queries build engine plans directly; the
/// registry admits other operations by their resolved plan's shape.
#[derive(Debug, Clone)]
pub enum Operation {
    /// One commit pair's changed-table interval behind `commit changes` and
    /// `changes poll`.
    CommitDiff {
        parent: Side,
        child: Side,
        scope: ScopeSpec,
        resume: Option<String>,
        budget: PageBudgetSpec,
    },
    /// The graph diff's two endpoints (`Omnigraph::diff_page`), arbitrary and
    /// cross-branch.
    SnapshotDiff { from: Side, to: Side },
    /// The three merge cursors (milestone 2).
    MergeClassify {
        base: Side,
        source: Side,
        target: Side,
    },
    /// A compiled GQ query, lowered directly without registry admission.
    Query(Box<QueryIR>),
}

impl Operation {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::CommitDiff { .. } => "commit_diff",
            Self::SnapshotDiff { .. } => "snapshot_diff",
            Self::MergeClassify { .. } => "merge_classify",
            Self::Query(_) => "query",
        }
    }

    pub fn sides(&self) -> Vec<&Side> {
        match self {
            Self::CommitDiff { parent, child, .. } => vec![parent, child],
            Self::SnapshotDiff { from, to } => vec![from, to],
            Self::MergeClassify {
                base,
                source,
                target,
            } => vec![base, source, target],
            Self::Query(_) => Vec::new(),
        }
    }
}
