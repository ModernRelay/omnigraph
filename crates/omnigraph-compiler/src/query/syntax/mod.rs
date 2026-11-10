//! The edition-3 parse tree (docs/rfcs/2026-11-07-gq-3.md, sections 3, 6 and
//! 7).
//!
//! A separate tree from [`super::ast`], so the edition-2 nodes, which the
//! planner and the GQT runner match exhaustively, are untouched. The front door hands each declaration on as
//! a [`super::ast::QueryDecl`] carrying this tree
//! ([`super::ast::SyntaxDecl`]); the edition-3 type checker
//! ([`super::check`]) reads it.
//!
//! Every node derives `PartialEq`, so a parse, print and parse round trip
//! compares whole trees ([`print`]). Parentheses leave no node.

use pest_derive::Parser;

use super::ast::{AggFunc, CompOp, FileBody, Literal, MutationWriteMode, SettingStmt, Traversal};
use super::diagnostic::Position;

pub mod edition;
pub mod edition_window;
pub mod foreign;
pub mod parse;
pub mod print;

#[cfg(test)]
mod tests;

/// The grammar of both editions: the edition-2 rules the edition window
/// reads, then the edition-3 rules (`grammar.pest`).
#[derive(Parser)]
#[grammar = "query/syntax/grammar.pest"]
pub(crate) struct QueryParser;

/// A source that opened with `gq 3;`.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceFile {
    pub settings: Vec<SettingStmt>,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    Decls(Vec<Decl>),
    /// `show`, `branch`, `commit` or `load`: the edition-2 statement nodes,
    /// whose grammar edition 3 keeps.
    Statement(Box<StatementBody>),
    /// `explain` over one edition-3 declaration.
    Explain(Box<Decl>),
}

/// Edition 2's statement bodies compare by their debug form: they do not
/// derive `PartialEq` and edition 3 changes none of them.
#[derive(Debug, Clone)]
pub struct StatementBody(pub FileBody);

impl PartialEq for StatementBody {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self.0) == format!("{:?}", other.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Decl {
    pub name: String,
    pub params: Vec<Param>,
    pub annotations: Vec<Annotation>,
    pub write_mode: MutationWriteMode,
    pub body: DeclBody,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    pub name: String,
    /// The type as edition 2 spells it in `ast::Param::type_name`.
    pub type_name: String,
    pub nullable: bool,
    pub default: Option<ParamDefault>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParamDefault {
    pub negative: bool,
    pub value: Literal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Annotation {
    /// Without the `@`.
    pub name: String,
    /// `None`: written without parentheses.
    pub args: Option<Vec<AnnotationArg>>,
    /// Where it was written, for the type checker's refusal of a name it
    /// does not read.
    pub at: SourceSpan,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AnnotationArg {
    Positional(AnnotationValue),
    Named(String, AnnotationValue),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AnnotationValue {
    Literal(Literal),
    Variable(String),
    MetaField(String),
    Ident(String),
}

/// The kind of a declaration is the shape of its body (A7).
#[derive(Debug, Clone, PartialEq)]
pub enum DeclBody {
    Read(ReadBody),
    Write(WriteBody),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadBody {
    pub clauses: Vec<Clause>,
    pub distinct: bool,
    pub projections: Vec<Projection>,
    pub order: Vec<Ordering>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Ordering {
    pub expr: Expr,
    /// `None`: no direction written.
    pub descending: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Limit {
    Literal(u64),
    Param(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriteBody {
    /// One or more groups: each a run of clauses, then the statements that
    /// write over its rows (RFC section 2, the write groups).
    pub groups: Vec<WriteGroup>,
}

impl WriteBody {
    /// Every group's clauses, in order.
    pub fn all_clauses(&self) -> Vec<&Clause> {
        self.groups.iter().flat_map(|g| g.clauses.iter()).collect()
    }

    /// Every group's statements, in order.
    pub fn all_statements(&self) -> Vec<&Stmt> {
        self.groups
            .iter()
            .flat_map(|g| g.statements.iter())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriteGroup {
    pub clauses: Vec<Clause>,
    pub statements: Vec<Stmt>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Insert {
        type_name: String,
        assignments: Vec<Assignment>,
    },
    Update {
        variable: String,
        assignments: Vec<Assignment>,
    },
    Delete {
        variable: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub property: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    /// `let $name = value`: a named value or a named relation.
    Let {
        name: String,
        value: LetValue,
    },
    /// `from rel`: rows drawn from a relation.
    From(RelExpr),
    Not(Vec<Clause>),
    Optional(Vec<Clause>),
    Binding {
        variable: String,
        type_name: String,
        prop_matches: Vec<(String, Expr)>,
    },
    Traversal(TraversalEq),
    Filter(Expr),
}

/// Edition 2's traversal node compared by its debug form.
#[derive(Debug, Clone)]
pub struct TraversalEq(pub Traversal);

impl PartialEq for TraversalEq {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self.0) == format!("{:?}", other.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LetValue {
    Relation(Box<ReadBody>),
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelExpr {
    Inline(Box<ReadBody>),
    Call(Call),
    Variable(String),
}

/// A correlated block: `count { … }`, `sum($x) { … }`, `exists { … }`.
#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    pub func: BlockFunc,
    pub arg: Option<Box<Expr>>,
    pub clauses: Vec<Clause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockFunc {
    Agg(AggFunc),
    Exists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl ArithOp {
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Rem => "%",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Variable(String),
    /// `$x.name` or `$x.@id`.
    Prop {
        variable: String,
        property: String,
    },
    /// A bare meta-field, `@id`.
    Meta(String),
    /// A bare identifier: a return alias in a read, a property in a
    /// mutation assignment, a field name as a ranking function's argument.
    Ident(String),
    Call(Call),
    /// `count(distinct x)`; any aggregate name parses, the type checker
    /// keeps today's T57 for the others.
    AggDistinct {
        func: AggFunc,
        arg: Box<Expr>,
    },
    Block(Box<Block>),
    Neg(Box<Expr>),
    Arith {
        op: ArithOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Compare {
        op: CompOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    In {
        needle: Box<Expr>,
        list: Box<Expr>,
        /// Where it was written: `$d in $x` over a node is a traversal over
        /// the reserved edge name, refused with the quoted form.
        at: SourceSpan,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    /// Edition 2's infix `contains` and `starts_with`. The type checker
    /// refuses it, since the left operand's type picks the fix: the call
    /// over a String, `in` over a list.
    InfixString {
        op: InfixStringOp,
        left: Box<Expr>,
        right: Box<Expr>,
        at: SourceSpan,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfixStringOp {
    Contains,
    StartsWith,
}

impl InfixStringOp {
    pub fn name(self) -> &'static str {
        match self {
            Self::Contains => "contains",
            Self::StartsWith => "starts_with",
        }
    }
}

/// Where a node was written: its byte range and position in the source, and
/// the source text of each operand, for a type-check refusal's suggestion.
/// It takes no part in tree equality, so a printed and reparsed tree still
/// compares equal.
#[derive(Debug, Clone)]
pub struct SourceSpan {
    pub start: usize,
    pub end: usize,
    pub position: Position,
    pub operands: Vec<String>,
}

impl Default for SourceSpan {
    /// A node built rather than parsed.
    fn default() -> Self {
        Self {
            start: 0,
            end: 0,
            position: Position {
                line: 1,
                column: 1,
                byte: 0,
            },
            operands: Vec::new(),
        }
    }
}

impl PartialEq for SourceSpan {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub name: String,
    pub args: Vec<Arg>,
    pub options: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Arg {
    Expr(Expr),
    Inline(Box<ReadBody>),
}
