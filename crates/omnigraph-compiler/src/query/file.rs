//! A parsed `.gq` file: its settings lines, then its declarations or the one
//! statement it holds (`branch`, `commit`, `load`, `show`, `explain`), and
//! the header of each declaration.

use std::fmt::Write as _;

use crate::settings::{SettingId, SettingValue};

/// A parsed `.gq` source: the `set` and `reset` lines at its head, then its
/// body, which is a list of `query` declarations, one branch statement, one
/// `show` statement, or one `explain` statement wrapping a read declaration
/// or a load statement.
#[derive(Debug, Clone)]
pub struct QueryFile {
    pub settings: Vec<SettingStmt>,
    pub body: FileBody,
}

/// What follows a file's settings prefix. `Queries` is empty for a file that
/// holds no statement at all, prefix or not.
#[derive(Debug, Clone)]
pub enum FileBody {
    Queries(Vec<QueryDecl>),
    Branch(BranchStmt),
    Commit(CommitWrite),
    Load(LoadStmt),
    Show(Option<SettingId>),
    Explain(QueryDecl),
    /// `explain load csv …`: the read doors answer the accepted load plan
    /// as explain rows, reading no payload byte.
    ExplainLoad(LoadStmt),
}

/// A top-level load statement: `load csv "<uri>" into node:Person mode
/// overwrite [delimiter "|"] [header] [columns (a, b)]`. The source is an
/// object the engine reads under the graph's external Blob policy; the
/// statement loads into the request's branch like a mutation declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadStmt {
    /// The URI as written, decoded from its string literal.
    pub source: String,
    pub target: LoadTarget,
    pub mode: LoadStmtMode,
    /// One ASCII byte; `,` when unspelled.
    pub delimiter: u8,
    /// Whether the first record names the columns.
    pub header: bool,
    /// Property names by column position, `from`/`to` for an edge file's
    /// endpoints; `None` when the header names the columns.
    pub columns: Option<Vec<String>>,
}

/// The declared type a load statement fills, as spelled: `node:Person` or
/// `edge:Knows`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadTarget {
    Node(String),
    Edge(String),
}

impl LoadTarget {
    /// The catalog table key, `node:<name>` or `edge:<name>`.
    pub fn table_key(&self) -> String {
        match self {
            LoadTarget::Node(name) => format!("node:{name}"),
            LoadTarget::Edge(name) => format!("edge:{name}"),
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            LoadTarget::Node(name) | LoadTarget::Edge(name) => name,
        }
    }
}

/// `mode overwrite | append | merge`, the load modes `omnigraph load` takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadStmtMode {
    Overwrite,
    Append,
    Merge,
}

impl LoadStmtMode {
    pub fn as_str(self) -> &'static str {
        match self {
            LoadStmtMode::Overwrite => "overwrite",
            LoadStmtMode::Append => "append",
            LoadStmtMode::Merge => "merge",
        }
    }
}

impl LoadStmt {
    /// The statement's two keywords.
    pub fn statement_name(&self) -> &'static str {
        "load csv"
    }

    /// The statement as the grammar reads it back: every spelled option in
    /// its canonical order, the default delimiter left unspelled.
    pub fn render(&self) -> String {
        let mut statement = format!(
            "load csv {} into {} mode {}",
            render_string_literal(&self.source),
            self.target.table_key(),
            self.mode.as_str()
        );
        if self.delimiter != b',' {
            let _ = write!(
                statement,
                " delimiter {}",
                render_string_literal(&(self.delimiter as char).to_string())
            );
        }
        if self.header {
            statement.push_str(" header");
        }
        if let Some(columns) = &self.columns {
            let _ = write!(statement, " columns ({})", columns.join(", "));
        }
        statement
    }

    /// Refusal text for a consumer that expected `query` declarations.
    pub fn not_a_declaration_message(&self) -> String {
        format!(
            "`{}` is a load statement, not a query declaration",
            self.statement_name()
        )
    }
}

/// `text` as a GQ string literal: quoted, with the backslash and the quote
/// escaped.
fn render_string_literal(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

/// A top-level control write on an unpublished commit:
/// `commit publish "<id>"` or `commit drop "<id>"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitWrite {
    Publish { graph_commit_id: String },
    Drop { graph_commit_id: String },
}

impl CommitWrite {
    /// The write's two keywords, `commit publish` or `commit drop`.
    pub fn statement_name(&self) -> &'static str {
        match self {
            CommitWrite::Publish { .. } => "commit publish",
            CommitWrite::Drop { .. } => "commit drop",
        }
    }

    pub fn graph_commit_id(&self) -> &str {
        match self {
            CommitWrite::Publish { graph_commit_id } | CommitWrite::Drop { graph_commit_id } => {
                graph_commit_id
            }
        }
    }

    /// Refusal text for a consumer that expected `query` declarations.
    pub fn not_a_declaration_message(&self) -> String {
        format!(
            "`{}` is a commit statement, not a query declaration",
            self.statement_name()
        )
    }
}

/// Which empty a source is: no statement at all, or a settings prefix with
/// nothing after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyFile {
    NoStatement,
    SettingsOnly,
}

/// One line of a file's settings prefix, its name and value already checked
/// against the settings definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingStmt {
    Set { id: SettingId, value: SettingValue },
    Reset { id: Option<SettingId> },
}

impl SettingStmt {
    /// `set` or `reset`, the keyword the line opens with.
    pub fn statement_name(&self) -> &'static str {
        match self {
            SettingStmt::Set { .. } => "set",
            SettingStmt::Reset { .. } => "reset",
        }
    }

    /// The setting a line names; `None` for `reset all`.
    pub fn id(&self) -> Option<SettingId> {
        match self {
            SettingStmt::Set { id, .. } => Some(*id),
            SettingStmt::Reset { id } => *id,
        }
    }
}

/// `show <name>` or `show all`, the spelling refusals quote.
pub fn show_statement_name(id: Option<SettingId>) -> String {
    format!("show {}", id.map_or("all", SettingId::name))
}

/// Refusal text for a consumer that expected `query` declarations and got
/// an `explain` statement; the branch statements carry theirs on
/// [`BranchStmt::not_a_declaration_message`].
const EXPLAIN_NOT_A_DECLARATION: &str = "`explain` is a statement, not a query declaration";

/// Refusal text for a read-declaration consumer that got `explain load csv`:
/// the read doors match the variant before selecting declarations and answer
/// the load plan themselves.
const EXPLAIN_LOAD_NOT_A_READ_DECLARATION: &str =
    "`explain load csv` is answered as a load plan by the read doors, not as a query declaration";

/// The statement name of `explain`, as the door refusals spell it.
pub const EXPLAIN_STATEMENT_NAME: &str = "explain";

impl FileBody {
    /// The declarations of a declaration file.
    ///
    /// # Errors
    ///
    /// The not-a-declaration message of a branch, `show` or `explain`
    /// statement.
    pub fn into_declarations(self) -> Result<Vec<QueryDecl>, String> {
        match self {
            FileBody::Queries(queries) => Ok(queries),
            FileBody::Branch(stmt) => Err(stmt.not_a_declaration_message()),
            FileBody::Commit(write) => Err(write.not_a_declaration_message()),
            FileBody::Load(stmt) => Err(stmt.not_a_declaration_message()),
            FileBody::Show(id) => Err(format!(
                "`{}` is a settings statement, not a query declaration",
                show_statement_name(id)
            )),
            FileBody::Explain(_) => Err(EXPLAIN_NOT_A_DECLARATION.to_string()),
            FileBody::ExplainLoad(_) => Err(EXPLAIN_NOT_A_DECLARATION.to_string()),
        }
    }

    /// The declarations a read door selects from: those of a declaration
    /// file, or the one read declaration under an `explain` statement.
    ///
    /// # Errors
    ///
    /// As [`FileBody::into_declarations`] for a branch or `show` statement,
    /// the `explain` refusal when the wrapped declaration holds mutations,
    /// and the load-plan refusal for `explain load csv`, which a read door
    /// answers itself before it selects declarations.
    pub fn into_read_declarations(self) -> Result<Vec<QueryDecl>, String> {
        match self {
            FileBody::Explain(decl) if decl.is_mutation() => Err(format!(
                "`explain` applies to a read query; '{}' contains mutations",
                decl.name
            )),
            FileBody::Explain(decl) => Ok(vec![decl]),
            FileBody::ExplainLoad(_) => Err(EXPLAIN_LOAD_NOT_A_READ_DECLARATION.to_string()),
            body => body.into_declarations(),
        }
    }

    /// Whether this body is an `explain` statement wrapping a query
    /// declaration; `explain load csv` answers no declaration's plan.
    pub fn is_explain(&self) -> bool {
        matches!(self, FileBody::Explain(_))
    }
}

impl QueryFile {
    /// Which empty this file is, `None` when its body carries a statement:
    /// the one reading of an empty `Queries` body every door shares.
    pub fn empty_kind(&self) -> Option<EmptyFile> {
        match &self.body {
            FileBody::Queries(queries) if queries.is_empty() => Some(if self.settings.is_empty() {
                EmptyFile::NoStatement
            } else {
                EmptyFile::SettingsOnly
            }),
            _ => None,
        }
    }

    /// [`FileBody::into_declarations`] of this file's body.
    ///
    /// # Errors
    ///
    /// As [`FileBody::into_declarations`].
    pub fn into_declarations(self) -> Result<Vec<QueryDecl>, String> {
        self.body.into_declarations()
    }

    /// The one declaration of a single-query file. Test support: production
    /// code matches `body` instead.
    ///
    /// # Panics
    ///
    /// Panics unless the file holds exactly one `query` declaration.
    #[doc(hidden)]
    #[track_caller]
    pub fn single_decl(&self) -> &QueryDecl {
        match &self.body {
            FileBody::Queries(queries) => match queries.as_slice() {
                [decl] => decl,
                other => panic!(
                    "expected exactly one query declaration, got {}",
                    other.len()
                ),
            },
            FileBody::Branch(stmt) => panic!("{}", stmt.not_a_declaration_message()),
            FileBody::Commit(write) => panic!("{}", write.not_a_declaration_message()),
            FileBody::Load(stmt) => panic!("{}", stmt.not_a_declaration_message()),
            FileBody::Show(id) => panic!("`{}` is not a declaration", show_statement_name(*id)),
            FileBody::Explain(_) | FileBody::ExplainLoad(_) => {
                panic!("{EXPLAIN_NOT_A_DECLARATION}")
            }
        }
    }
}

/// A top-level branch statement: a control write, `branch list`, or a
/// control read that compares or previews branches (RFC 0030 §8). Only a
/// control write changes a branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchStmt {
    Write(BranchWrite),
    List,
    Read(BranchRead),
}

/// A branch control read: it changes no branch and publishes nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchRead {
    /// `branch diff <source> [into <target>]` or `branch diff <from> to <to>`,
    /// one page of net entity changes.
    Diff {
        sides: BranchDiffSides,
        page: DiffPageClause,
    },
    /// `branch merge-base <source> [into <target>]`.
    MergeBase {
        source: String,
        into: Option<String>,
    },
    /// `branch merge <source> [into <target>] dry run`: the merge decided,
    /// nothing written.
    MergeDryRun {
        source: String,
        into: Option<String>,
    },
}

/// The two sides of `branch diff`. `into` is `None` when unspelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchDiffSides {
    /// What `source` changed since its merge base with `into`.
    Branch {
        source: String,
        into: Option<String>,
    },
    /// The net change from `from` to `to`, each a branch or a commit id.
    Between { from: String, to: String },
}

/// The paging clauses of `branch diff`: `after page "<token>"` continues a
/// previous answer, `limit <n>` bounds the page's changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiffPageClause {
    pub after_page: Option<String>,
    pub limit: Option<usize>,
}

impl BranchRead {
    /// The read's leading words.
    pub fn statement_name(&self) -> &'static str {
        match self {
            BranchRead::Diff { .. } => "branch diff",
            BranchRead::MergeBase { .. } => "branch merge-base",
            BranchRead::MergeDryRun { .. } => "branch merge dry run",
        }
    }
}

/// A control write: `branch create <name> [from <parent> | from commit "<id>"]`,
/// `branch delete <name>`, or `branch merge <source> [into <target>] [as commit]`.
/// `from` and `into` are `None` when unspelled; no default is filled here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchWrite {
    Create {
        name: String,
        from: Option<BranchCreateFrom>,
    },
    Delete {
        name: String,
    },
    Merge {
        source: String,
        into: Option<String>,
        /// `as commit`: the merge result is an unpublished commit.
        as_commit: bool,
    },
}

/// Where `branch create` starts the new branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchCreateFrom {
    /// `from <parent>`: the new branch starts at the parent's head.
    Branch(String),
    /// `from commit "<id>"`: the new branch starts at that published graph
    /// commit.
    Commit(String),
}

impl BranchWrite {
    /// The write's two keywords, `branch create` through `branch merge`.
    pub fn statement_name(&self) -> &'static str {
        match self {
            BranchWrite::Create { .. } => "branch create",
            BranchWrite::Delete { .. } => "branch delete",
            BranchWrite::Merge { .. } => "branch merge",
        }
    }
}

impl BranchStmt {
    /// `true` only for a control write.
    pub fn is_write(&self) -> bool {
        matches!(self, BranchStmt::Write(_))
    }

    /// The statement's leading words, `branch create` through `branch diff`.
    pub fn statement_name(&self) -> &'static str {
        match self {
            BranchStmt::Write(write) => write.statement_name(),
            BranchStmt::List => "branch list",
            BranchStmt::Read(read) => read.statement_name(),
        }
    }

    /// Refusal text for a consumer that expected `query` declarations.
    pub fn not_a_declaration_message(&self) -> String {
        format!(
            "`{}` is a branch statement, not a query declaration",
            self.statement_name()
        )
    }
}

/// One declaration as the front door hands it on: its header (name,
/// metadata, parameters, write mode), which every door reads to select and
/// describe it, and its parse tree, which the type checker reads.
#[derive(Debug, Clone)]
pub struct QueryDecl {
    pub name: String,
    pub description: Option<String>,
    pub instruction: Option<String>,
    pub params: Vec<Param>,
    /// The clause before the body: `Publish`, or `as commit` / `as
    /// unattached branch` on a write body.
    pub write_mode: MutationWriteMode,
    pub syntax: SyntaxDecl,
}

impl QueryDecl {
    /// A read named nothing with no parameters and an empty body: the
    /// declaration a harness holds for a source the parser refused.
    pub fn unnamed_read() -> Self {
        use super::syntax::{Body, Decl, DeclBody, ReadBody, SourceFile};
        let file = SourceFile {
            settings: Vec::new(),
            body: Body::Decls(vec![Decl {
                name: String::new(),
                params: Vec::new(),
                annotations: Vec::new(),
                write_mode: MutationWriteMode::Publish,
                body: DeclBody::Read(ReadBody {
                    clauses: Vec::new(),
                    distinct: false,
                    projections: Vec::new(),
                    order: Vec::new(),
                    limit: None,
                    spans: Default::default(),
                }),
            }]),
        };
        Self {
            name: String::new(),
            description: None,
            instruction: None,
            params: Vec::new(),
            write_mode: MutationWriteMode::Publish,
            syntax: SyntaxDecl::new(std::sync::Arc::new(file), 0),
        }
    }

    /// Whether the declaration writes: its body is a write body (its kind is
    /// the body's shape).
    pub fn is_mutation(&self) -> bool {
        self.syntax.is_write()
    }
}

/// One declaration and the file that declares it: a call resolves
/// against the declarations of its own source unit, so the type checker
/// needs the whole file.
#[derive(Debug, Clone)]
pub struct SyntaxDecl {
    file: std::sync::Arc<super::syntax::SourceFile>,
    index: usize,
}

impl SyntaxDecl {
    /// The declaration at `index` of `file`'s declarations, or its `explain`
    /// target when `file` is an `explain` statement.
    pub(crate) fn new(file: std::sync::Arc<super::syntax::SourceFile>, index: usize) -> Self {
        Self { file, index }
    }

    /// The file the declaration belongs to.
    pub fn file(&self) -> &super::syntax::SourceFile {
        &self.file
    }

    /// The declaration's parse tree.
    pub fn decl(&self) -> &super::syntax::Decl {
        match &self.file.body {
            super::syntax::Body::Decls(decls) => &decls[self.index],
            super::syntax::Body::Explain(decl) => decl,
            super::syntax::Body::Statement(_) => {
                unreachable!("a statement carries no declaration")
            }
        }
    }

    /// Whether the body is a write body.
    pub fn is_write(&self) -> bool {
        matches!(self.decl().body, super::syntax::DeclBody::Write(_))
    }
}

/// Where a mutation's result goes: the branch head (`Publish`), an
/// unpublished commit (`as commit`), or an unattached snapshot
/// (`as unattached branch [on <base>]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationWriteMode {
    Publish,
    AsCommit,
    Unattached { base: Option<UnattachedBase> },
}

/// The `on` operand of `as unattached branch`: the base snapshot's id as a
/// quoted literal, or the name (without `$`) of a `String` parameter that
/// carries it per invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnattachedBase {
    Literal(String),
    Param(String),
}

#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}
