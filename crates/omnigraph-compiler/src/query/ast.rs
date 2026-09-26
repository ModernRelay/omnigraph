use std::fmt::Write as _;

use crate::settings::{SettingId, SettingValue};

pub const NOW_PARAM_NAME: &str = "__nanograph_now";

/// A parsed `.gq` source: the `set` and `reset` lines at its head, then its
/// body, which is a list of `query` declarations, one branch statement, one
/// `show` statement, or one `explain` statement wrapping a read declaration.
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
    Show(Option<SettingId>),
    Explain(QueryDecl),
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
            FileBody::Show(id) => Err(format!(
                "`{}` is a settings statement, not a query declaration",
                show_statement_name(id)
            )),
            FileBody::Explain(_) => Err(EXPLAIN_NOT_A_DECLARATION.to_string()),
        }
    }

    /// The declarations a read door selects from: those of a declaration
    /// file, or the one read declaration under an `explain` statement.
    ///
    /// # Errors
    ///
    /// As [`FileBody::into_declarations`] for a branch or `show` statement,
    /// and the `explain` refusal when the wrapped declaration holds mutations.
    pub fn into_read_declarations(self) -> Result<Vec<QueryDecl>, String> {
        match self {
            FileBody::Explain(decl) if !decl.mutations.is_empty() => Err(format!(
                "`explain` applies to a read query; '{}' contains mutations",
                decl.name
            )),
            FileBody::Explain(decl) => Ok(vec![decl]),
            body => body.into_declarations(),
        }
    }

    /// Whether this body is an `explain` statement.
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
            FileBody::Show(id) => panic!("`{}` is not a declaration", show_statement_name(*id)),
            FileBody::Explain(_) => panic!("{EXPLAIN_NOT_A_DECLARATION}"),
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
    /// A condition the type checker proves Boolean: a comparison today, any
    /// Boolean expression once the grammar admits one.
    Filter(Expr),
    Subquery(Subquery),
}

/// A correlated block: `not { … }`, `exists { … }`, `count { … } > 2`,
/// `sum($m.size) { … } > 100`. The block's clauses match per outer row; the
/// aggregate over those matches is compared with `right`.
#[derive(Debug, Clone)]
pub struct Subquery {
    /// The surface spelling, for error messages.
    pub keyword: BlockKeyword,
    pub clauses: Vec<Clause>,
    pub func: AggFunc,
    /// `None` counts the matched rows (`not`, `exists`, `count { … }`).
    pub arg: Option<Expr>,
    pub op: CompOp,
    pub right: Expr,
}

impl Subquery {
    /// `not { … }`: `count = 0`.
    pub fn not_block(clauses: Vec<Clause>) -> Self {
        Self::row_count(BlockKeyword::Not, clauses, CompOp::Eq)
    }

    /// `exists { … }`: `count > 0`.
    pub fn exists_block(clauses: Vec<Clause>) -> Self {
        Self::row_count(BlockKeyword::Exists, clauses, CompOp::Gt)
    }

    fn row_count(keyword: BlockKeyword, clauses: Vec<Clause>, op: CompOp) -> Self {
        Self {
            keyword,
            clauses,
            func: AggFunc::Count,
            arg: None,
            op,
            right: Expr::Literal(Literal::Integer(0)),
        }
    }

    /// The block as an error message names it: `negation`, `exists`, `count`, `sum`, …
    pub fn block_name(&self) -> String {
        match self.keyword {
            BlockKeyword::Not => "negation".to_string(),
            BlockKeyword::Exists => "exists".to_string(),
            BlockKeyword::Aggregate => self.func.to_string(),
        }
    }
}

/// How a correlated block was spelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKeyword {
    Not,
    Exists,
    /// `count`, `sum`, `avg`, `min` or `max`, followed by a comparison.
    Aggregate,
}

#[derive(Debug, Clone)]
pub struct Binding {
    pub variable: String,
    pub type_name: String,
    pub prop_matches: Vec<PropMatch>,
}

/// `{ name: "x" }` inside a binding: `value` is a constant, a `Literal`, a
/// `Variable` naming a declared parameter, or `Now`.
#[derive(Debug, Clone)]
pub struct PropMatch {
    pub prop_name: String,
    pub value: Expr,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// The operator of an [`Expr::Binary`] node: a comparison, or a Boolean
/// combination of two Boolean operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Compare(CompOp),
    And,
    Or,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compare(op) => write!(f, "{op}"),
            Self::And => f.write_str("and"),
            Self::Or => f.write_str("or"),
        }
    }
}

/// One expression type for every clause; the clause's type check decides
/// which node kinds it admits. Two trees are equal node by node, `Literal`
/// by its own rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Not(Box<Expr>),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
}

impl Expr {
    /// `left <op> right` as one comparison node.
    pub fn comparison(left: Expr, op: CompOp, right: Expr) -> Self {
        Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::Compare(op),
            right: Box::new(right),
        }
    }

    /// The operands and operator of a comparison-rooted expression; `None`
    /// for every other node.
    pub fn comparison_parts(&self) -> Option<(&Expr, CompOp, &Expr)> {
        match self {
            Expr::Binary {
                left,
                op: BinaryOp::Compare(op),
                right,
            } => Some((left, *op, right)),
            _ => None,
        }
    }

    /// The top-level `and` chain as its conjuncts, in written order; an
    /// `or`, a `not` or a comparison is one conjunct.
    pub fn conjuncts(&self) -> Vec<&Expr> {
        match self {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut conjuncts = left.conjuncts();
                conjuncts.extend(right.conjuncts());
                conjuncts
            }
            other => vec![other],
        }
    }

    /// Whether `self` is a `search`, `fuzzy` or `match_text` call.
    pub fn is_search_call(&self) -> bool {
        matches!(
            self,
            Expr::Search { .. } | Expr::Fuzzy { .. } | Expr::MatchText { .. }
        )
    }

    /// Whether `self` is a search predicate in the one shape it lowers to:
    /// a bare search call, or that call `= true`.
    pub fn is_search_predicate(&self) -> bool {
        self.is_search_call()
            || matches!(
                self.comparison_parts(),
                Some((call, CompOp::Eq, Expr::Literal(Literal::Bool(true)))) if call.is_search_call()
            )
    }

    /// The filter with every bare search call among its top-level conjuncts
    /// spelled `call = true`, so both spellings reach the type checker and the
    /// lowering as one shape.
    pub fn with_search_predicates_spelled(self) -> Expr {
        match self {
            call if call.is_search_call() => {
                Expr::comparison(call, CompOp::Eq, Expr::Literal(Literal::Bool(true)))
            }
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => Expr::Binary {
                left: Box::new(left.with_search_predicates_spelled()),
                op: BinaryOp::And,
                right: Box::new(right.with_search_predicates_spelled()),
            },
            other => other,
        }
    }
}

/// Binding strength of an expression's root, for printing with minimal
/// parentheses: `or` < `and` < `not` < comparison and null test < atom.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Precedence {
    Or,
    And,
    Not,
    Comparison,
    Atom,
}

impl Expr {
    fn precedence(&self) -> Precedence {
        match self {
            Expr::Binary {
                op: BinaryOp::Or, ..
            } => Precedence::Or,
            Expr::Binary {
                op: BinaryOp::And, ..
            } => Precedence::And,
            Expr::Not(_) => Precedence::Not,
            Expr::Binary {
                op: BinaryOp::Compare(_),
                ..
            }
            | Expr::IsNull { .. } => Precedence::Comparison,
            _ => Precedence::Atom,
        }
    }

    /// Print `self` as an operand of a node with `parent` precedence, in
    /// parentheses when it binds looser, when both are comparisons or null tests,
    /// or as the right operand of an `and`/`or` it repeats (left-nested prints bare).
    fn fmt_operand(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        parent: Precedence,
        right_operand: bool,
    ) -> std::fmt::Result {
        let own = self.precedence();
        let parenthesized =
            own < parent || (own == parent && (parent == Precedence::Comparison || right_operand));
        if parenthesized {
            write!(f, "({self})")
        } else {
            write!(f, "{self}")
        }
    }
}

/// The expression as GQ text, the spelling the parser accepts, with a system
/// field as written (`$p.@id`) and the minimal parentheses (`fmt_operand`),
/// never the user's; the text an error quotes back.
impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Binary { left, op, right } => {
                let precedence = self.precedence();
                left.fmt_operand(f, precedence, false)?;
                write!(f, " {op} ")?;
                right.fmt_operand(f, precedence, true)
            }
            Expr::Not(operand) => {
                f.write_str("not ")?;
                operand.fmt_operand(f, Precedence::Not, false)
            }
            Expr::IsNull { expr, negated } => {
                expr.fmt_operand(f, Precedence::Comparison, false)?;
                f.write_str(if *negated { " is not null" } else { " is null" })
            }
            Expr::Now => f.write_str("now()"),
            Expr::PropAccess { variable, property } => write!(f, "${variable}.{property}"),
            Expr::Nearest {
                variable,
                property,
                query,
            } => write!(f, "nearest(${variable}.{property}, {query})"),
            Expr::Search { field, query } => write!(f, "search({field}, {query})"),
            Expr::Fuzzy {
                field,
                query,
                max_edits: None,
            } => write!(f, "fuzzy({field}, {query})"),
            Expr::Fuzzy {
                field,
                query,
                max_edits: Some(max_edits),
            } => write!(f, "fuzzy({field}, {query}, {max_edits})"),
            Expr::MatchText { field, query } => write!(f, "match_text({field}, {query})"),
            Expr::Bm25 { field, query } => write!(f, "bm25({field}, {query})"),
            Expr::Rrf {
                primary,
                secondary,
                k: None,
            } => write!(f, "rrf({primary}, {secondary})"),
            Expr::Rrf {
                primary,
                secondary,
                k: Some(k),
            } => write!(f, "rrf({primary}, {secondary}, {k})"),
            Expr::Variable(name) => write!(f, "${name}"),
            Expr::Literal(literal) => write!(f, "{literal}"),
            Expr::Aggregate { func, arg } => write!(f, "{func}({arg})"),
            Expr::AliasRef(alias) => f.write_str(alias),
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone)]
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

/// Equality and hashing by value, with `Float` compared through
/// `f64::to_bits`: `0.0` and `-0.0` are distinct and `NaN` equals itself, so
/// the derived `Eq` and `Hash` of every expression type hold.
impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Null, Literal::Null) => true,
            (Literal::String(a), Literal::String(b)) => a == b,
            (Literal::Integer(a), Literal::Integer(b)) => a == b,
            (Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
            (Literal::Bool(a), Literal::Bool(b)) => a == b,
            (Literal::Date(a), Literal::Date(b)) => a == b,
            (Literal::DateTime(a), Literal::DateTime(b)) => a == b,
            (Literal::List(a), Literal::List(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Literal {}

impl std::hash::Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Literal::Null => {}
            Literal::String(text) | Literal::Date(text) | Literal::DateTime(text) => {
                text.hash(state)
            }
            Literal::Integer(value) => value.hash(state),
            Literal::Float(value) => value.to_bits().hash(state),
            Literal::Bool(value) => value.hash(state),
            Literal::List(items) => items.hash(state),
        }
    }
}

/// The literal as explain text. It is the spelling the parser accepts except
/// for `null`, a negative number and a non-finite float, which GQ has no
/// literal for. A float prints in fixed notation with a decimal point.
impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Null => f.write_str("null"),
            Literal::String(text) => write_gq_string(f, text),
            Literal::Integer(value) => write!(f, "{value}"),
            Literal::Float(value) => {
                let text = value.to_string();
                f.write_str(&text)?;
                if value.is_finite() && !text.contains('.') {
                    f.write_str(".0")?;
                }
                Ok(())
            }
            Literal::Bool(value) => write!(f, "{value}"),
            Literal::Date(text) => {
                f.write_str("date(")?;
                write_gq_string(f, text)?;
                f.write_str(")")
            }
            Literal::DateTime(text) => {
                f.write_str("datetime(")?;
                write_gq_string(f, text)?;
                f.write_str(")")
            }
            Literal::List(items) => {
                f.write_str("[")?;
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{item}")?;
                }
                f.write_str("]")
            }
        }
    }
}

/// A GQ string literal: double-quoted, with the escapes
/// [`crate::error::decode_string_literal`] decodes (`\"`, `\\`, `\n`, `\r`,
/// `\t`), so the text stays on one line.
fn write_gq_string(f: &mut std::fmt::Formatter<'_>, text: &str) -> std::fmt::Result {
    f.write_char('"')?;
    for character in text.chars() {
        match character {
            '"' => f.write_str("\\\"")?,
            '\\' => f.write_str("\\\\")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            '\t' => f.write_str("\\t")?,
            other => f.write_char(other)?,
        }
    }
    f.write_char('"')
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

/// `update T set { … } where <predicate>`. The predicate names the target's
/// properties as `Expr::PropAccess { variable: <type name>, property }`
/// (the parser spells a bare `name` that way; see [`Expr::mutation_property`]).
#[derive(Debug, Clone)]
pub struct UpdateMutation {
    pub type_name: String,
    pub assignments: Vec<MutationAssignment>,
    pub predicate: Expr,
}

/// `delete T where <predicate>`, the predicate shaped as in [`UpdateMutation`].
#[derive(Debug, Clone)]
pub struct DeleteMutation {
    pub type_name: String,
    pub predicate: Expr,
}

/// `property: value` in an insert or update; `value` is a constant as in
/// [`PropMatch`].
#[derive(Debug, Clone)]
pub struct MutationAssignment {
    pub property: String,
    pub value: Expr,
}

impl Expr {
    /// A bare property in a mutation `where`, bound to the mutation's target
    /// type: the type name stands where a read has a binding variable.
    pub fn mutation_property(type_name: &str, property: &str) -> Self {
        Expr::PropAccess {
            variable: type_name.to_string(),
            property: property.to_string(),
        }
    }
}
