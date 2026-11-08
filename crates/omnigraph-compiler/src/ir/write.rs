//! The write IR: one table a write touches, resolved under the transaction's
//! catalog, the planner's input behind `Operation::Write`. Lowering reads the
//! catalog and the write's shape (what it stages, whether it removes ids),
//! never a row, so planning a write costs what explaining it costs.

use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::query::ast::LoadTarget;

use super::load::{LoadCheck, checks_for};

/// The statement a write plan belongs to, as the explain's `operation` row
/// names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteKind {
    Load,
    Mutation,
    BlobCell,
    BranchMerge,
}

impl WriteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            WriteKind::Load => "load",
            WriteKind::Mutation => "mutation",
            WriteKind::BlobCell => "blob_cell",
            WriteKind::BranchMerge => "branch_merge",
        }
    }
}

/// What a write does to one table's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaClass {
    /// Rows the table has not held: a strict insert, an append.
    Insert,
    /// Rows present or absent before: a keyed upsert, a merge load, a staged
    /// mutation, a branch merge's effects on its target.
    Upsert,
    /// The table's whole new image; no committed row survives.
    Replace,
    /// No rows: the write only removes ids.
    Removal,
}

impl DeltaClass {
    pub fn as_str(self) -> &'static str {
        match self {
            DeltaClass::Insert => "insert",
            DeltaClass::Upsert => "upsert",
            DeltaClass::Replace => "replace",
            DeltaClass::Removal => "removal",
        }
    }

    /// Whether the committed image of the table is empty under this write.
    pub fn replaces(self) -> bool {
        self == DeltaClass::Replace
    }

    /// Whether the write stages rows the checks read.
    pub fn has_rows(self) -> bool {
        self != DeltaClass::Removal
    }
}

/// One table of a write as the planner judges it: the delta's class and size,
/// whether ids are removed, and the constraints in evaluation order, which is
/// the refusal precedence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTableIR {
    pub target: LoadTarget,
    pub delta: DeltaClass,
    /// The write removes ids from this table: a `delete`, an overwrite's
    /// replaced rows, a branch merge's deletions.
    pub deletes: bool,
    /// The staged rows, exact.
    pub rows: u64,
    pub checks: Vec<LoadCheck>,
}

/// Lower one table of a write under `catalog`. Infallible: a table the
/// catalog does not declare judges nothing.
pub fn lower_write_table(
    catalog: &Catalog,
    target: LoadTarget,
    delta: DeltaClass,
    deletes: bool,
    rows: u64,
) -> WriteTableIR {
    let checks = checks_for(catalog, &target, delta, deletes);
    WriteTableIR {
        target,
        delta,
        deletes,
        rows,
        checks,
    }
}
