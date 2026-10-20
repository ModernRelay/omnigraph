//! Arguments for applied cluster tooling and graph registry access.

use std::path::PathBuf;

use clap::Subcommand;
use omnigraph_server::PolicyAction;

/// Operations on the graph registry of a multi-graph server (MR-668).
///
/// Registry scope (RFC-011): these address the server itself, not a graph
/// within it — a server or managed-cluster `--target` applies, while
/// `--graph` and `--as` are rejected by the addressing guard.
/// Use `cluster apply --target` to deploy graph additions and schema/query changes.
#[derive(Debug, Subcommand)]
pub(crate) enum GraphsCommand {
    /// List every graph registered with the multi-graph server.
    List {
        #[arg(long)]
        json: bool,
        /// Minimal authenticated graph existence; requires an identity credential.
        #[arg(long)]
        discovery: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum PolicyCommand {
    /// Compile and validate the Cedar policy bundle(s) applied in a cluster.
    ///
    /// Sources the bundle(s) from the cluster's applied policies
    /// (`--target <root>`); pass the global `--graph <id>` to pick one
    /// graph's bundle when several apply.
    Validate {},
    /// Run declarative policy tests against a cluster's applied bundle.
    ///
    /// The cluster model has no per-bundle tests file, so the cases are
    /// supplied explicitly with `--tests <file>` and checked against the
    /// bundle selected by `--target` (+ optional `--graph`).
    Test {
        /// Path to a policy.tests.yaml file.
        #[arg(long)]
        tests: PathBuf,
    },
    /// Explain one policy decision against a cluster's applied bundle.
    Explain {
        #[arg(long)]
        actor: String,
        #[arg(long)]
        action: PolicyAction,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long = "target-branch")]
        target_branch: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum QueriesCommand {
    /// Type-check a cluster's stored-query registry against its schemas.
    ///
    /// Distinct from `omnigraph lint` (which lints one `.gq` file): this
    /// validates the whole `queries:` registry of a cluster (`--target
    /// <dir>`, optional `--graph <id>`) by reading each graph's applied
    /// schema and confirming every stored query still type-checks. Exits
    /// non-zero on any breakage.
    Validate {
        #[arg(long)]
        json: bool,
    },
    /// List a cluster's registered stored queries (name, params).
    List {
        #[arg(long)]
        json: bool,
    },
}
