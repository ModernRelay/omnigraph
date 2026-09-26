use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::ir::QueryIR;
use serde::{Deserialize, Serialize};

/// The physical owner of one table image: the dataset, its native Lance
/// branch and the pinned version (RFC 0065), so a routed plan reads exactly
/// the owner the executor reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    pub type_key: String,
    pub dataset_path: String,
    pub native_branch: Option<String>,
}

impl TableRef {
    /// The bare node type name of a `node:<Type>` table; `None` for any
    /// other table.
    pub fn node_type_name(&self) -> Option<&str> {
        self.type_key.strip_prefix("node:")
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
    /// The unpaged `diff_commits` pair, arbitrary and cross-branch.
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
