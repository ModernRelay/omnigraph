//! Arguments for graph loads, exports and Blob transfers.
use clap::{Args, Subcommand, ValueEnum};
use omnigraph::loader::LoadMode;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Args)]
pub(crate) struct LoadArgs {
    #[arg(long)]
    pub(crate) data: PathBuf,
    /// Target branch (defaults to main). Without --from it must exist.
    #[arg(long)]
    pub(crate) branch: Option<String>,
    /// Base branch to fork --branch from when it doesn't exist yet.
    /// Without this flag a missing branch is an error, never a fork.
    #[arg(long)]
    pub(crate) from: Option<String>,
    /// How existing entities are handled: overwrite | append | merge.
    /// Required — overwrite is destructive, so there is no default.
    #[arg(long)]
    pub(crate) mode: CliLoadMode,
    /// Session setting for this invocation (repeatable): `name=value` in
    /// GQ spelling, e.g. `--set stage_write_concurrency=8`. Embedded
    /// stores only: no served load route carries a `settings` field.
    #[arg(long = "set", value_name = "NAME=VALUE")]
    pub(crate) settings: Vec<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ExportArgs {
    #[arg(long)]
    pub(crate) branch: Option<String>,
    #[arg(long = "type")]
    pub(crate) type_names: Vec<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum BlobEntityArg {
    Node,
    Edge,
}

impl From<BlobEntityArg> for omnigraph_api_types::BlobEntityKind {
    fn from(entity: BlobEntityArg) -> Self {
        match entity {
            BlobEntityArg::Node => Self::Node,
            BlobEntityArg::Edge => Self::Edge,
        }
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum BlobCommand {
    /// Stream one managed Blob to stdout or a file.
    Get {
        /// Logical entity namespace.
        #[arg(value_name = "ENTITY")]
        entity: BlobEntityArg,
        /// Accepted-schema node or edge type.
        #[arg(value_name = "TYPE")]
        type_name: String,
        /// Logical entity id.
        #[arg(value_name = "ID")]
        id: String,
        /// Blob property name.
        #[arg(value_name = "PROPERTY")]
        property: String,
        /// Read a named branch (defaults to main).
        #[arg(long, conflicts_with = "snapshot")]
        branch: Option<String>,
        /// Read an immutable graph snapshot.
        #[arg(long, conflicts_with = "branch")]
        snapshot: Option<String>,
        /// First byte to return.
        #[arg(long)]
        offset: Option<u64>,
        /// Number of bytes to return; must be greater than zero.
        #[arg(long)]
        length: Option<u64>,
        /// Write bytes to a file instead of stdout.
        #[arg(long, value_name = "PATH")]
        out: Option<PathBuf>,
    },
    /// Inspect one Blob descriptor without reading payload bytes.
    Stat {
        /// Logical entity namespace.
        #[arg(value_name = "ENTITY")]
        entity: BlobEntityArg,
        /// Accepted-schema node or edge type.
        #[arg(value_name = "TYPE")]
        type_name: String,
        /// Logical entity id.
        #[arg(value_name = "ID")]
        id: String,
        /// Blob property name.
        #[arg(value_name = "PROPERTY")]
        property: String,
        /// Read a named branch (defaults to main).
        #[arg(long, conflicts_with = "snapshot")]
        branch: Option<String>,
        /// Read an immutable graph snapshot.
        #[arg(long, conflicts_with = "branch")]
        snapshot: Option<String>,
        /// Emit stable JSON metadata.
        #[arg(long)]
        json: bool,
    },
    /// Replace one Blob value of an existing node or edge with raw bytes from
    /// a file or stdin, as one graph commit.
    Put {
        /// Logical entity namespace.
        #[arg(value_name = "ENTITY")]
        entity: BlobEntityArg,
        /// Accepted-schema node or edge type.
        #[arg(value_name = "TYPE")]
        type_name: String,
        /// Logical entity id; the entity must exist.
        #[arg(value_name = "ID")]
        id: String,
        /// Blob property name.
        #[arg(value_name = "PROPERTY")]
        property: String,
        /// Write a named branch (defaults to main).
        #[arg(long)]
        branch: Option<String>,
        /// Read the bytes from this file instead of stdin.
        #[arg(long, value_name = "PATH")]
        file: Option<PathBuf>,
        /// Write only while the cell's ETag is one of these entity tags, or,
        /// as `*`, while the cell holds any value.
        #[arg(long, value_name = "TAG")]
        if_match: Option<String>,
        /// Emit the receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Set one nullable Blob value of an existing node or edge to null, as one
    /// graph commit; a cell that is already null publishes nothing.
    Clear {
        /// Logical entity namespace.
        #[arg(value_name = "ENTITY")]
        entity: BlobEntityArg,
        /// Accepted-schema node or edge type.
        #[arg(value_name = "TYPE")]
        type_name: String,
        /// Logical entity id; the entity must exist.
        #[arg(value_name = "ID")]
        id: String,
        /// Blob property name.
        #[arg(value_name = "PROPERTY")]
        property: String,
        /// Write a named branch (defaults to main).
        #[arg(long)]
        branch: Option<String>,
        /// Write only while the cell's ETag is one of these entity tags, or,
        /// as `*`, while the cell holds any value.
        #[arg(long, value_name = "TAG")]
        if_match: Option<String>,
        /// Emit the receipt as JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CliLoadMode {
    Overwrite,
    Append,
    Merge,
}

impl From<CliLoadMode> for LoadMode {
    fn from(value: CliLoadMode) -> Self {
        match value {
            CliLoadMode::Overwrite => LoadMode::Overwrite,
            CliLoadMode::Append => LoadMode::Append,
            CliLoadMode::Merge => LoadMode::Merge,
        }
    }
}
impl CliLoadMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CliLoadMode::Overwrite => "overwrite",
            CliLoadMode::Append => "append",
            CliLoadMode::Merge => "merge",
        }
    }
}
