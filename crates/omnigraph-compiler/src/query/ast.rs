pub const NOW_PARAM_NAME: &str = "__nanograph_now";

/// A parsed `.gq` source: a list of `query` declarations, or one branch
/// statement.
#[derive(Debug, Clone)]
pub enum QueryFile {
    Queries(Vec<QueryDecl>),
    Branch(BranchStmt),
}

impl QueryFile {
    /// The one declaration of a single-query file. Test support: production
    /// code matches `QueryFile` instead.
    ///
    /// # Panics
    ///
    /// Panics unless the file holds exactly one `query` declaration.
    #[doc(hidden)]
    #[track_caller]
    pub fn single_decl(&self) -> &QueryDecl {
        match self {
            QueryFile::Queries(queries) => match queries.as_slice() {
                [decl] => decl,
                other => panic!(
                    "expected exactly one query declaration, got {}",
                    other.len()
                ),
            },
            QueryFile::Branch(stmt) => panic!("{}", stmt.not_a_declaration_message()),
        }
    }
}

/// A top-level branch statement: a control write, or `branch list`, the
/// one statement that changes no branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchStmt {
    Write(BranchWrite),
    List,
}

/// A control write: `branch create <name> [from <parent>]`,
/// `branch delete <name>`, or `branch merge <source> [into <target>]`.
/// `from` and `into` are `None` when unspelled; no default is filled here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchWrite {
    Create {
        name: String,
        from: Option<String>,
    },
    Delete {
        name: String,
    },
    Merge {
        source: String,
        into: Option<String>,
    },
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
    /// `false` only for `branch list`.
    pub fn is_write(&self) -> bool {
        matches!(self, BranchStmt::Write(_))
    }

    /// The statement's two keywords, `branch create` through `branch list`.
    pub fn statement_name(&self) -> &'static str {
        match self {
            BranchStmt::Write(write) => write.statement_name(),
            BranchStmt::List => "branch list",
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

#[derive(Debug, Clone)]
pub struct QueryDecl {
    pub name: String,
    pub description: Option<String>,
    pub instruction: Option<String>,
    pub params: Vec<Param>,
    pub match_clause: Vec<Clause>,
    pub return_clause: Vec<Projection>,
    pub order_clause: Vec<Ordering>,
    pub limit: Option<u64>,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub enum Clause {
    Binding(Binding),
    Traversal(Traversal),
    Filter(Filter),
    Negation(Vec<Clause>),
}

#[derive(Debug, Clone)]
pub struct Binding {
    pub variable: String,
    pub type_name: String,
    pub prop_matches: Vec<PropMatch>,
}

#[derive(Debug, Clone)]
pub struct PropMatch {
    pub prop_name: String,
    pub value: MatchValue,
}

#[derive(Debug, Clone)]
pub enum MatchValue {
    Literal(Literal),
    Variable(String),
    Now,
}

#[derive(Debug, Clone)]
pub struct Traversal {
    pub src: String,
    pub edge_name: String,
    pub dst: String,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
    /// `$a <edge> $b` — match the edge in either direction (set semantics;
    /// same-endpoint-type edges only, enforced at typecheck).
    pub undirected: bool,
    /// Optional name for the matched edge (`$p $w:knows $f`), making the
    /// edge's own properties addressable as `$w.<prop>`.
    pub edge_binding: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub left: Expr,
    pub op: CompOp,
    pub right: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    /// The parser emits `Contains` for the `contains` keyword; typecheck
    /// accepts a list left operand (membership) or a scalar String left
    /// operand (substring), and lowering resolves the String form to
    /// `StringContains` so downstream consumers never re-derive types.
    Contains,
    /// Exact, case-sensitive prefix match on a scalar String property.
    StartsWith,
    /// Exact, case-sensitive substring match on a scalar String property.
    /// Never produced by the parser — lowering resolves it from `Contains`.
    StringContains,
}

impl std::fmt::Display for CompOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "!="),
            Self::Gt => write!(f, ">"),
            Self::Lt => write!(f, "<"),
            Self::Ge => write!(f, ">="),
            Self::Le => write!(f, "<="),
            Self::Contains | Self::StringContains => write!(f, "contains"),
            Self::StartsWith => write!(f, "starts_with"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Now,
    PropAccess {
        variable: String,
        property: String,
    },
    Nearest {
        variable: String,
        property: String,
        query: Box<Expr>,
    },
    Search {
        field: Box<Expr>,
        query: Box<Expr>,
    },
    Fuzzy {
        field: Box<Expr>,
        query: Box<Expr>,
        max_edits: Option<Box<Expr>>,
    },
    MatchText {
        field: Box<Expr>,
        query: Box<Expr>,
    },
    Bm25 {
        field: Box<Expr>,
        query: Box<Expr>,
    },
    Rrf {
        primary: Box<Expr>,
        secondary: Box<Expr>,
        k: Option<Box<Expr>>,
    },
    Variable(String),
    Literal(Literal),
    Aggregate {
        func: AggFunc,
        arg: Box<Expr>,
    },
    AliasRef(String),
}

/// Lance's search output columns, appended under the target's prefix
/// (`{var}._distance` for `nearest`, `{var}._score` for `bm25`) and reserved
/// by `schema::is_reserved_search_output_column`.
pub const DISTANCE_COLUMN: &str = "_distance";
/// See [`DISTANCE_COLUMN`].
pub const SCORE_COLUMN: &str = "_score";

impl Expr {
    /// The `(binding, column)` a projected rank expression reads: the score
    /// the executed retrieval wrote for that binding (RFC 0047 §Metric
    /// projection). `None` for every expression that is not a single-source
    /// rank expression over a property.
    pub fn score_column(&self) -> Option<(&str, &'static str)> {
        match self {
            Expr::Nearest { variable, .. } => Some((variable, DISTANCE_COLUMN)),
            Expr::Bm25 { field, .. } => match field.as_ref() {
                Expr::PropAccess { variable, .. } => Some((variable, SCORE_COLUMN)),
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count => write!(f, "count"),
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Date(String),
    DateTime(String),
    List(Vec<Literal>),
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Ordering {
    pub expr: Expr,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub enum Mutation {
    Insert(InsertMutation),
    Update(UpdateMutation),
    Delete(DeleteMutation),
}

#[derive(Debug, Clone)]
pub struct InsertMutation {
    pub type_name: String,
    pub assignments: Vec<MutationAssignment>,
}

#[derive(Debug, Clone)]
pub struct UpdateMutation {
    pub type_name: String,
    pub assignments: Vec<MutationAssignment>,
    pub predicate: MutationPredicate,
}

#[derive(Debug, Clone)]
pub struct DeleteMutation {
    pub type_name: String,
    pub predicate: MutationPredicate,
}

#[derive(Debug, Clone)]
pub struct MutationAssignment {
    pub property: String,
    pub value: MatchValue,
}

#[derive(Debug, Clone)]
pub struct MutationPredicate {
    pub property: String,
    pub op: CompOp,
    pub value: MatchValue,
}
