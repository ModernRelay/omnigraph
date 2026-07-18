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

use crate::cli::{BlobCommand, Cli, Command, QueriesCommand, SchemaCommand};

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
/// `queries`/`policy` read a cluster's applied state (`Control`).
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
/// show`/`apply` are data; `queries`/`policy` read cluster applied state).
pub(crate) fn command_plane(cmd: &Command) -> Plane {
    match cmd {
        Command::Query { .. }
        | Command::Mutate { .. }
        | Command::Load { .. }
        | Command::Ingest { .. }
        | Command::Branch { .. }
        | Command::Blob { .. }
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
        // `queries` and `policy` tooling now source their inputs from a
        // cluster's applied state (`--cluster`), so they live on the control
        // plane (RFC-011 — omnigraph.yaml excised from the CLI).
        Command::Queries { .. } => Plane::Control,
        Command::Policy { .. } => Plane::Control,
        Command::Init { .. }
        | Command::Optimize { .. }
        | Command::Repair { .. }
        | Command::Cleanup { .. }
        | Command::Lint { .. } => Plane::Storage,
        Command::Cluster { .. } => Plane::Control,
        Command::Alias { .. }
        | Command::Embed(_)
        | Command::Login { .. }
        | Command::Logout { .. }
        | Command::Profile { .. }
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
        Command::Profile { .. } => "profile",
        Command::Embed(_) => "embed",
        Command::Init { .. } => "init",
        Command::Load { .. } => "load",
        Command::Ingest { .. } => "ingest",
        Command::Branch { .. } => "branch",
        Command::Blob { command } => match command {
            BlobCommand::Get { .. } => "blob get",
            BlobCommand::Stat { .. } => "blob stat",
        },
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
        Command::Alias { .. } => "alias",
        Command::Policy { .. } => "policy",
        Command::Optimize { .. } => "optimize",
        Command::Repair { .. } => "repair",
        Command::Cleanup { .. } => "cleanup",
        Command::Cluster { .. } => "cluster",
        Command::Graphs { .. } => "graphs",
    }
}

/// The verbs that consume a cluster scope. Maintenance/lint select a graph with
/// `--cluster <root> --graph <id>`; policy/queries inspect the cluster's
/// applied control-plane state and may optionally use `--graph` to select one
/// bundle/registry. `init` is storage-plane too but *creates* a graph (cluster
/// graphs are born from `cluster apply`, not `init`), and `schema plan` takes a
/// positional URI, so the guard rejects `--cluster`/`--graph` there rather than
/// silently dropping the flag.
pub(crate) fn accepts_cluster_addressing(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Optimize { .. }
            | Command::Repair { .. }
            | Command::Cleanup { .. }
            // `lint` can type-check a `.gq` against a cluster graph's schema
            // (RFC-011): `--cluster <dir> --graph <id>`.
            | Command::Lint { .. }
            // The policy/queries tooling addresses a cluster's applied state
            // (RFC-011): `--cluster <dir>` selects the cluster, `--graph <id>`
            // picks a graph's bundle/registry within it.
            | Command::Policy { .. }
            | Command::Queries { .. }
    )
}

/// Reject a scope-addressing flag (`--server`/`--cluster`/`--graph`) on a verb
/// that cannot consume it, rather than silently dropping it (the old behavior:
/// e.g. `optimize --server prod` dropped `--server` and failed later with an
/// unrelated message). `alias` gets an extra guard because its binding owns all
/// addressing and several ignored globals sit outside this three-flag guard.
/// Each flag has a distinct valid surface:
/// - `--server` → served-graph scopes (`any`/`served`);
/// - `--cluster` → cluster-scoped direct/control verbs;
/// - `--graph` → any multi-graph scope: a served scope *or* a cluster one.
/// RFC-010 Slice 1, generalized for RFC-011 cluster addressing.
pub(crate) fn guard_addressing(cli: &Cli) -> Result<()> {
    if let Command::Alias { .. } = &cli.command {
        let mut flags = Vec::new();
        if cli.server.is_some() {
            flags.push("--server");
        }
        if cli.graph.is_some() {
            flags.push("--graph");
        }
        if cli.store.is_some() {
            flags.push("--store");
        }
        if cli.cluster.is_some() {
            flags.push("--cluster");
        }
        if cli.profile.is_some() {
            flags.push("--profile");
        }
        if cli.as_actor.is_some() {
            flags.push("--as");
        }
        if !flags.is_empty() {
            bail!(
                "`alias` uses the server, graph, and stored query declared in \
                 `aliases.<name>` in ~/.omnigraph/config.yaml; remove global scope \
                 flag(s): {}",
                flags.join(", ")
            );
        }
    }
    if cli.server.is_none() && cli.cluster.is_none() && cli.graph.is_none() {
        return Ok(());
    }
    let capability = command_capability(&cli.command);
    let label = command_label(&cli.command);
    let cluster_ok = accepts_cluster_addressing(&cli.command);

    if cli.server.is_some() && !capability.accepts_server_addressing() {
        bail!(
            "`{label}` is a {} command; --server addresses a served graph and does not apply.{}",
            capability.describe(),
            remediation(capability, &cli.command),
        );
    }
    if cli.cluster.is_some() && !cluster_ok {
        bail!(
            "`{label}` is a {} command; --cluster addresses a cluster-scoped command \
             and does not apply.{}",
            capability.describe(),
            remediation(capability, &cli.command),
        );
    }
    if cli.graph.is_some() && !(capability.accepts_server_addressing() || cluster_ok) {
        bail!(
            "`{label}` is a {} command; --graph selects a graph within a server or cluster \
             scope and does not apply.{}",
            capability.describe(),
            remediation(capability, &cli.command),
        );
    }
    Ok(())
}

/// The "what to do instead" tail for a wrong-address error, by capability.
/// Includes its own leading space when non-empty so the caller appends it
/// directly — an empty tail (the served-addressing capabilities, which only
/// reach this fn for a misplaced `--cluster`/`--graph`) leaves no trailing space.
fn remediation(capability: Capability, cmd: &Command) -> &'static str {
    match capability {
        Capability::Direct => match cmd {
            Command::Init { .. } => " Pass a storage URI.",
            Command::Optimize { .. } | Command::Repair { .. } | Command::Cleanup { .. } => {
                " Pass a storage URI, or --cluster <dir> --graph <id>."
            }
            _ => " Pass a storage URI.",
        },
        Capability::Control => match cmd {
            Command::Cluster { .. } => {
                " It operates on a cluster config directory (pass --config <dir>)."
            }
            Command::Policy { .. } | Command::Queries { .. } => {
                " It operates on a cluster (pass --cluster <dir|uri>, or select a cluster profile)."
            }
            _ => " It operates on a cluster.",
        },
        Capability::Local => " It does not address a graph.",
        Capability::Any | Capability::Served => "",
    }
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
        assert_eq!(cap(&["omnigraph", "alias", "who"]), Capability::Local);
        assert_eq!(cap(&["omnigraph", "blob", "stat", "Doc", "id1", "content"]), Capability::Any);
        assert_eq!(cap(&["omnigraph", "optimize", "graph.omni"]), Capability::Direct);
        assert_eq!(cap(&["omnigraph", "schema", "plan", "--schema", "s.pg", "graph.omni"]), Capability::Direct);
        assert_eq!(cap(&["omnigraph", "cluster", "status", "--config", "."]), Capability::Control);
        assert_eq!(cap(&["omnigraph", "version"]), Capability::Local);
        // `queries`/`policy` tooling reads cluster state now (control plane).
        assert_eq!(cap(&["omnigraph", "queries", "list"]), Capability::Control);
        assert_eq!(
            cap(&["omnigraph", "policy", "validate"]),
            Capability::Control
        );
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
