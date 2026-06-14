//! Declared CLI "planes" (RFC-010 Slice 1).
//!
//! Every subcommand belongs to exactly one plane. This classification is the
//! single source of truth the wrong-plane guard consumes — and that later
//! RFC-010 slices (the capability surface, plane-grouped help) will consume
//! too. The `command_plane` match is **exhaustive on purpose**: adding a
//! `Command` variant is a compile error until its plane is declared, so the
//! surface cannot silently drift from the command set.
//!
//! See [docs/dev/rfc-010-cli-planes-restructure.md].

use color_eyre::Result;
use color_eyre::eyre::bail;

use crate::cli::{Cli, Command, QueriesCommand, SchemaCommand};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plane {
    /// Runs against a graph, embedded **or** via `--server` (the `GraphClient`
    /// axis). The only plane on which the data-plane addressing flags
    /// (`--server`/`--graph`) apply.
    Data,
    /// Direct storage access; no server. Maintenance + local-only inspection
    /// that must work with the server down.
    Storage,
    /// Operates on a cluster directory, not a graph URI.
    Control,
    /// Touches no graph at all — session / config / local tooling.
    Session,
}

impl std::fmt::Display for Plane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Plane::Data => "data",
            Plane::Storage => "storage",
            Plane::Control => "control",
            Plane::Session => "session",
        })
    }
}

/// The plane a subcommand belongs to. Exhaustive — a new `Command` variant
/// will not compile until classified. Descends into the nested enums where
/// the plane differs per subcommand (`schema plan` is storage while `schema
/// show`/`apply` are data; `queries validate` opens the graph while `queries
/// list` only reads config).
pub(crate) fn command_plane(cmd: &Command) -> Plane {
    match cmd {
        Command::Query { .. }
        | Command::Mutate { .. }
        | Command::Load { .. }
        | Command::Ingest { .. }
        | Command::Branch { .. }
        | Command::Snapshot { .. }
        | Command::Export { .. }
        | Command::Commit { .. }
        | Command::Graphs { .. } => Plane::Data,
        Command::Schema {
            command: SchemaCommand::Show { .. } | SchemaCommand::Apply { .. },
        } => Plane::Data,
        Command::Schema {
            command: SchemaCommand::Plan { .. },
        } => Plane::Storage,
        Command::Queries {
            command: QueriesCommand::Validate { .. },
        } => Plane::Storage,
        Command::Queries {
            command: QueriesCommand::List { .. },
        } => Plane::Session,
        Command::Init { .. }
        | Command::Optimize { .. }
        | Command::Repair { .. }
        | Command::Cleanup { .. }
        | Command::Lint { .. } => Plane::Storage,
        Command::Cluster { .. } => Plane::Control,
        Command::Policy { .. }
        | Command::Embed(_)
        | Command::Login { .. }
        | Command::Logout { .. }
        | Command::Config { .. }
        | Command::Version => Plane::Session,
    }
}

/// User-facing label for a subcommand (descends one level for the nested
/// families so messages read `schema plan`, `queries validate`, etc.).
pub(crate) fn command_label(cmd: &Command) -> &'static str {
    match cmd {
        Command::Version => "version",
        Command::Login { .. } => "login",
        Command::Logout { .. } => "logout",
        Command::Config { .. } => "config",
        Command::Embed(_) => "embed",
        Command::Init { .. } => "init",
        Command::Load { .. } => "load",
        Command::Ingest { .. } => "ingest",
        Command::Branch { .. } => "branch",
        Command::Schema { command } => match command {
            SchemaCommand::Plan { .. } => "schema plan",
            SchemaCommand::Apply { .. } => "schema apply",
            SchemaCommand::Show { .. } => "schema show",
        },
        Command::Lint { .. } => "lint",
        Command::Queries { command } => match command {
            QueriesCommand::Validate { .. } => "queries validate",
            QueriesCommand::List { .. } => "queries list",
        },
        Command::Snapshot { .. } => "snapshot",
        Command::Export { .. } => "export",
        Command::Commit { .. } => "commit",
        Command::Query { .. } => "query",
        Command::Mutate { .. } => "mutate",
        Command::Policy { .. } => "policy",
        Command::Optimize { .. } => "optimize",
        Command::Repair { .. } => "repair",
        Command::Cleanup { .. } => "cleanup",
        Command::Cluster { .. } => "cluster",
        Command::Graphs { .. } => "graphs",
    }
}

/// Reject the data-plane addressing flags (`--server`/`--graph`) on any verb
/// that does not live on the data plane. This replaces the old silent-ignore
/// — e.g. `optimize --server prod` previously dropped `--server` and tried to
/// resolve a default target, failing (if at all) with an unrelated message.
/// Now it fails with one honest, declared error. RFC-010 Slice 1.
pub(crate) fn guard_addressing(cli: &Cli) -> Result<()> {
    if cli.server.is_none() && cli.graph.is_none() {
        return Ok(());
    }
    let plane = command_plane(&cli.command);
    if plane == Plane::Data {
        return Ok(());
    }
    let label = command_label(&cli.command);
    let how = match plane {
        // `init` is the one storage verb with no `--target` today (it takes a
        // required positional URI), so its remediation drops the `--target` half.
        Plane::Storage => match cli.command {
            Command::Init { .. } => "Pass a storage URI.",
            _ => "Use --target <name>, a storage URI, or --cluster <dir> --cluster-graph <id>.",
        },
        Plane::Control => "It operates on a cluster directory (pass --config <dir>).",
        Plane::Session => "It does not address a graph.",
        Plane::Data => unreachable!("data plane returned early"),
    };
    bail!(
        "`{label}` is a {plane}-plane command; --server/--graph address the data plane and do not apply. {how}"
    );
}
