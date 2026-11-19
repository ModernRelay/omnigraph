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
    /// How records meet existing entities: insert (an existing key fails the
    /// load) | upsert (sets the named properties) | replace (writes the whole
    /// entity) | overwrite (each type becomes the batch's entities). Required:
    /// overwrite is destructive, so there is no default.
    #[arg(long, value_parser = parse_load_mode_arg)]
    pub(crate) mode: LoadModeArg,
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

impl From<BlobEntityArg> for omnigraph_api_types::EntityKind {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CliLoadMode {
    Overwrite,
    Insert,
    Upsert,
    Replace,
}

/// `--mode` as written: a mode, or edition 2's `append` or `merge`, which the
/// load refuses as a command failure (a coded error, JSON under `--json`)
/// rather than as a usage error.
#[derive(Debug, Clone, Copy)]
pub(crate) enum LoadModeArg {
    Mode(CliLoadMode),
    Removed {
        old: &'static str,
        new: &'static str,
    },
}

impl LoadModeArg {
    /// The mode, or the refusal of a removed one: `Q004` naming the mode
    /// that now means what it meant, before anything is read.
    pub(crate) fn resolve(self) -> Result<CliLoadMode, omnigraph::error::OmniError> {
        match self {
            Self::Mode(mode) => Ok(mode),
            Self::Removed { old, new } => {
                use omnigraph_compiler::query::codes::Q004;
                use omnigraph_compiler::{QueryDiagnostic, error::CompilerError};
                Err(omnigraph::error::OmniError::Compiler(CompilerError::query(
                    QueryDiagnostic::parse(
                        Q004,
                        omnigraph::loader::removed_mode_message(old, new),
                        None,
                    )
                    .with_fix(format!("--mode {new}")),
                )))
            }
        }
    }
}

fn parse_load_mode_arg(value: &str) -> Result<LoadModeArg, String> {
    Ok(match value {
        "append" => LoadModeArg::Removed {
            old: "append",
            new: "insert",
        },
        "merge" => LoadModeArg::Removed {
            old: "merge",
            new: "replace",
        },
        value => LoadModeArg::Mode(parse_cli_load_mode(value)?),
    })
}

/// `--mode`'s value, by the engine's names; edition 2's `append` and `merge`
/// are refused with the mode that now means what they meant.
fn parse_cli_load_mode(value: &str) -> Result<CliLoadMode, String> {
    Ok(match LoadMode::parse(value)? {
        LoadMode::Overwrite => CliLoadMode::Overwrite,
        LoadMode::Insert => CliLoadMode::Insert,
        LoadMode::Upsert => CliLoadMode::Upsert,
        LoadMode::Replace => CliLoadMode::Replace,
    })
}

impl From<CliLoadMode> for LoadMode {
    fn from(value: CliLoadMode) -> Self {
        match value {
            CliLoadMode::Overwrite => LoadMode::Overwrite,
            CliLoadMode::Insert => LoadMode::Insert,
            CliLoadMode::Upsert => LoadMode::Upsert,
            CliLoadMode::Replace => LoadMode::Replace,
        }
    }
}
impl CliLoadMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CliLoadMode::Overwrite => "overwrite",
            CliLoadMode::Insert => "insert",
            CliLoadMode::Upsert => "upsert",
            CliLoadMode::Replace => "replace",
        }
    }
}
