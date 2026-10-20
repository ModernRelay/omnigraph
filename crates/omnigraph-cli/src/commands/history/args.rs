//! Arguments owned by graph history and branch commands.

use clap::{Args, Subcommand, ValueEnum};

#[derive(Debug, Args)]
pub(crate) struct SnapshotArgs {
    /// Graph URI
    pub(crate) uri: Option<String>,
    #[arg(long)]
    pub(crate) branch: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct DiffArgs {
    /// Graph URI
    pub(crate) uri: Option<String>,
    /// Read target the diff starts from
    #[arg(long)]
    pub(crate) from: String,
    /// Read target the diff ends at
    #[arg(long)]
    pub(crate) to: String,
    /// Changes per page, at most 8192: 8192 when the CLI follows every
    /// page, 1000 for one `--page-token` fetch
    #[arg(long)]
    pub(crate) limit: Option<usize>,
    /// Fetch exactly one page starting at this opaque token
    /// (auto-paginates when omitted)
    #[arg(long)]
    pub(crate) page_token: Option<String>,
    /// Filter by entity kind (repeatable): node | edge
    #[arg(long = "kind", value_enum)]
    pub(crate) kinds: Vec<ChangeKindArg>,
    /// Filter by type name (repeatable)
    #[arg(long = "type")]
    pub(crate) types: Vec<String>,
    /// Filter by operation (repeatable): insert | update | delete
    #[arg(long = "op", value_enum)]
    pub(crate) ops: Vec<ChangeOpArg>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum BranchCommand {
    /// Create a new branch
    Create {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        /// Branch to fork; the new branch starts at its head. Defaults to
        /// `main`.
        #[arg(long)]
        from: Option<String>,
        /// A published graph commit id to start the branch at, with that
        /// commit's data and history, instead of a branch head.
        #[arg(long, conflicts_with = "from")]
        from_commit: Option<String>,
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// List branches
    List {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Delete a branch
    Delete {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Merge a source branch into a target branch
    Merge {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        source: String,
        #[arg(long)]
        into: Option<String>,
        /// Delete the source branch after a successful merge. Runs under its
        /// own branch_delete policy check; a refusal is reported as a warning
        /// and never fails the already-landed merge.
        #[arg(long)]
        delete_branch: bool,
        /// Merge into an unpublished commit (`branch merge … as commit`): the
        /// target head is unchanged until `omnigraph commit publish <id>`.
        /// Exclusive with --delete-branch, whose source the commit still holds.
        #[arg(long, conflicts_with = "delete_branch")]
        as_commit: bool,
        /// Run the merge through classification and validation and stop:
        /// report the outcome, or fail with the conflicts, and write nothing.
        /// A read of both branches.
        #[arg(long, conflicts_with_all = ["delete_branch", "as_commit"])]
        dry_run: bool,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
        /// `settings` field (the Session settings RFC).
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// What a branch changed since its merge base with a target branch: the
    /// net entity changes from the base to the branch head. Auto-paginates.
    Diff {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        source: String,
        /// Branch the source is compared against (default main)
        #[arg(long)]
        into: Option<String>,
        /// Changes per page, at most 8192: 8192 when the CLI follows every
        /// page, 1000 for one `--page-token` fetch
        #[arg(long)]
        limit: Option<usize>,
        /// Fetch exactly one page starting at this opaque token
        /// (auto-paginates when omitted)
        #[arg(long)]
        page_token: Option<String>,
        /// Filter by entity kind (repeatable): node | edge
        #[arg(long = "kind", value_enum)]
        kinds: Vec<ChangeKindArg>,
        /// Filter by type name (repeatable)
        #[arg(long = "type")]
        types: Vec<String>,
        /// Filter by operation (repeatable): insert | update | delete
        #[arg(long = "op", value_enum)]
        ops: Vec<ChangeOpArg>,
        #[arg(long)]
        json: bool,
    },
    /// The commit a merge of a source branch into a target branch would
    /// classify against
    MergeBase {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        source: String,
        /// Branch the source would merge into (default main)
        #[arg(long)]
        into: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]

pub(crate) enum CommitCommand {
    /// List graph commits
    List {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// List the unpublished commits whose recorded target is the branch
        /// (`as commit` writes awaiting `commit publish`) instead of its history
        #[arg(long)]
        unpublished: bool,
        /// List the live unattached snapshots whose recorded target is the
        /// branch (`as unattached branch` writes) instead of its history
        #[arg(long, conflicts_with = "unpublished")]
        unattached: bool,
        /// List one page of at most this many commits, 1 to 1000, and the
        /// token that continues it (the whole history when omitted)
        #[arg(long, conflicts_with_all = ["unpublished", "unattached"])]
        limit: Option<usize>,
        /// Continue a paged listing of the same branch with this opaque token,
        /// 100 commits per page unless `--limit` is set
        #[arg(long, conflicts_with_all = ["unpublished", "unattached"])]
        page_token: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Publish an unpublished commit onto its recorded target branch
    Publish {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        commit_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Drop an unpublished commit or an unattached snapshot: the id and every
    /// unattached descendant stop resolving and held merge inputs are released
    Drop {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        commit_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Show a graph commit
    Show {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        commit_id: String,
        #[arg(long)]
        json: bool,
    },
    /// List the entity changes one commit made relative to its first parent
    Changes {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        commit_id: String,
        /// Changes per page, at most 8192: 8192 when the CLI follows every
        /// page, 1000 for one `--page-token` fetch
        #[arg(long)]
        limit: Option<usize>,
        /// Fetch exactly one page starting at this opaque token
        /// (auto-paginates when omitted)
        #[arg(long)]
        page_token: Option<String>,
        /// Filter by entity kind (repeatable): node | edge
        #[arg(long = "kind", value_enum)]
        kinds: Vec<ChangeKindArg>,
        /// Filter by type name (repeatable)
        #[arg(long = "type")]
        types: Vec<String>,
        /// Filter by operation (repeatable): insert | update | delete
        #[arg(long = "op", value_enum)]
        ops: Vec<ChangeOpArg>,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
        /// `settings` field (the Session settings RFC).
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ChangeKindArg {
    Node,
    Edge,
}

impl From<ChangeKindArg> for omnigraph_api_types::EntityKindOutput {
    fn from(kind: ChangeKindArg) -> Self {
        match kind {
            ChangeKindArg::Node => Self::Node,
            ChangeKindArg::Edge => Self::Edge,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ChangeOpArg {
    Insert,
    Update,
    Delete,
}

impl From<ChangeOpArg> for omnigraph_api_types::ChangeOpOutput {
    fn from(op: ChangeOpArg) -> Self {
        match op {
            ChangeOpArg::Insert => Self::Insert,
            ChangeOpArg::Update => Self::Update,
            ChangeOpArg::Delete => Self::Delete,
        }
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum ChangesCommand {
    /// Poll the change feed of a branch. Auto-consumes page tokens and prints
    /// the durable cursor from the terminal page.
    Poll {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Durable cursor from a previous poll or baseline
        #[arg(long, conflicts_with = "start")]
        cursor: Option<String>,
        /// Start mode: now | beginning | after:<commit_id> (default now)
        #[arg(long, conflicts_with = "cursor")]
        start: Option<String>,
        /// Changes per page, at most 8192 (default 8192; the CLI follows
        /// every page)
        #[arg(long)]
        limit: Option<usize>,
        /// Filter by entity kind (repeatable): node | edge
        #[arg(long = "kind", value_enum)]
        kinds: Vec<ChangeKindArg>,
        /// Filter by type name (repeatable)
        #[arg(long = "type")]
        types: Vec<String>,
        /// Filter by operation (repeatable): insert | update | delete
        #[arg(long = "op", value_enum)]
        ops: Vec<ChangeOpArg>,
        /// Session setting for this invocation (repeatable): `name=value` in
        /// GQ spelling, e.g. `--set merge_lineage=off` — the request's
        /// `settings` field (the Session settings RFC).
        #[arg(long = "set", value_name = "NAME=VALUE")]
        settings: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Capture an exact entity snapshot plus its resume cursor (POSIX only).
    /// Non-POSIX platforms fail before capture until a durable write-through
    /// namespace replacement is available.
    Baseline {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Snapshot scope by entity kind (repeatable): node | edge
        #[arg(long = "kind", value_enum)]
        kinds: Vec<ChangeKindArg>,
        /// Snapshot scope by type name (repeatable)
        #[arg(long = "type")]
        types: Vec<String>,
        /// Feed scope by operation (repeatable); binds the resume cursor only
        #[arg(long = "op", value_enum)]
        ops: Vec<ChangeOpArg>,
        /// Write the NDJSON snapshot here; the handshake prints to stdout
        #[arg(long, value_name = "PATH")]
        out: std::path::PathBuf,
        #[arg(long)]
        json: bool,
    },
}
