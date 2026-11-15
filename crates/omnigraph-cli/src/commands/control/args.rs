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
    /// List stored queries (name, params): a cluster root's registry, or one
    /// served graph's catalog.
    ///
    /// A direct root reads its applied state; `--graph` narrows it to one
    /// graph. A server target reads `GET /graphs/{id}/queries` for the graph
    /// `--graph` or the target's `default_graph` selects, and `--json` prints
    /// that response body.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Write query files in GQ's canonical spelling, never changing what a
    /// query means.
    ///
    /// Formats each named `.gq` file and every `.gq` file under a named
    /// directory in place, or reads stdin and writes stdout when no path or
    /// `-` is given. It writes `exists { … }` for `count { … } > 0`, a flat
    /// `union`, the ends of `<e>` and an alternation's members in name order,
    /// no `asc`, no alias equal to its column's name, call options in the
    /// registry's order without defaults, and `bm25` over a String for
    /// `terms(q, mode: any)`; a short `match` on one line, a long one an
    /// element per line. With `--schema`, `search`, `match_text` and `fuzzy`
    /// over a field whose index declares an analyzer become `match_terms`.
    /// No target or credential is read.
    Fmt {
        /// `.gq` files or directories; none, or `-`, reads stdin.
        paths: Vec<PathBuf>,
        /// Write nothing; exit 1 when a file is not in its canonical spelling.
        #[arg(long)]
        check: bool,
        /// The schema the queries run against (`.pg`), which decides where an
        /// older lexical call is `match_terms`.
        #[arg(long)]
        schema: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
}
