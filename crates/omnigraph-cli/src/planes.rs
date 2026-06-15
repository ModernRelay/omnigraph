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

/// What a command *needs*, in the user-facing vocabulary (RFC-011). This is the
/// language CLI errors and `--help` speak; `Plane` stays the internal classifier
/// (`Capability` is derived from it, so the two cannot drift).
///
/// - `any` — graph-scoped data; served via a server scope, or direct against a
///   store scope. Accepts `--server`/`--graph`.
/// - `served` — requires a server. Accepts `--server`/`--graph`.
/// - `direct` — storage-native; opens storage directly, never through a server.
/// - `control` — operates on a cluster (control plane).
/// - `local` — addresses no graph at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Capability {
    Any,
    Served,
    Direct,
    Control,
    Local,
}

impl Capability {
    /// A human phrase for error messages (`` `optimize` is a {…} command ``).
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Capability::Any => "data",
            Capability::Served => "served",
            Capability::Direct => "direct (storage-native)",
            Capability::Control => "cluster control",
            Capability::Local => "local",
        }
    }

    /// `--server`/`--graph` are served-graph addressing: they apply only to the
    /// capabilities that reach a graph through a server.
    fn accepts_server_addressing(self) -> bool {
        matches!(self, Capability::Any | Capability::Served)
    }
}

/// The capability a subcommand needs, derived from its `Plane` (the exhaustive
/// classifier) plus the one Data→Served refinement: `graphs` is remote-only.
///
/// This reflects *current enforced behavior*, so messages stay truthful:
/// `queries list` is `Local` (reads config today) and `queries validate` is
/// `Direct` (opens a graph directly today). Both converge to the RFC end-state
/// (served / control) only when later slices re-route them.
pub(crate) fn command_capability(cmd: &Command) -> Capability {
    if let Command::Graphs { .. } = cmd {
        return Capability::Served;
    }
    match command_plane(cmd) {
        Plane::Data => Capability::Any,
        Plane::Storage => Capability::Direct,
        Plane::Control => Capability::Control,
        Plane::Session => Capability::Local,
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
    let capability = command_capability(&cli.command);
    if capability.accepts_server_addressing() {
        return Ok(());
    }
    let label = command_label(&cli.command);
    let how = match capability {
        Capability::Direct => match cli.command {
            Command::Init { .. } => "Pass a storage URI.",
            _ => "Pass a storage URI, or --cluster <dir> --cluster-graph <id>.",
        },
        Capability::Control => "It operates on a cluster (pass --config <dir>).",
        Capability::Local => "It does not address a graph.",
        Capability::Any | Capability::Served => {
            unreachable!("served-addressing capabilities returned early")
        }
    };
    bail!(
        "`{label}` is a {} command; --server/--graph address a served graph and do not apply. {how}",
        capability.describe()
    );
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn server_addressing_allowed_exactly_on_any_and_served() {
        // The behavior-preservation contract: `--server`/`--graph` apply to the
        // served-graph capabilities (`any`, `served`) and nothing else. This is
        // the old "Data plane only" allow set, re-expressed — graphs (the one
        // Data→Served verb) was already allowed.
        assert!(Capability::Any.accepts_server_addressing());
        assert!(Capability::Served.accepts_server_addressing());
        assert!(!Capability::Direct.accepts_server_addressing());
        assert!(!Capability::Control.accepts_server_addressing());
        assert!(!Capability::Local.accepts_server_addressing());
    }

    #[test]
    fn command_capability_classifies_representative_verbs() {
        let cap = |args: &[&str]| {
            command_capability(&Cli::try_parse_from(args).unwrap().command)
        };
        // The one Data→Served refinement — if the `graphs` guard were deleted,
        // every other assertion here would still pass.
        assert_eq!(cap(&["omnigraph", "graphs", "list"]), Capability::Served);
        assert_eq!(cap(&["omnigraph", "optimize", "graph.omni"]), Capability::Direct);
        assert_eq!(cap(&["omnigraph", "schema", "plan", "--schema", "s.pg", "graph.omni"]), Capability::Direct);
        assert_eq!(cap(&["omnigraph", "cluster", "status", "--config", "."]), Capability::Control);
        assert_eq!(cap(&["omnigraph", "version"]), Capability::Local);
        assert_eq!(cap(&["omnigraph", "queries", "list"]), Capability::Local);
    }

    #[test]
    fn every_capability_describes_distinctly() {
        let phrases = [
            Capability::Any.describe(),
            Capability::Served.describe(),
            Capability::Direct.describe(),
            Capability::Control.describe(),
            Capability::Local.describe(),
        ];
        for (i, a) in phrases.iter().enumerate() {
            assert!(!a.is_empty());
            for b in &phrases[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
