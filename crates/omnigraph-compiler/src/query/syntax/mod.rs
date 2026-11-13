//! The surface tree (docs/rfcs/2026-11-07-gq-3.md, sections 3, 6 and 7):
//! what the parser builds and the type checker ([`super::check`]) reads.
//! The front door hands each declaration on as a
//! [`super::file::QueryDecl`] carrying this tree
//! ([`super::file::SyntaxDecl`]).
//!
//! Every node derives `PartialEq`, so a parse, print and parse round trip
//! compares whole trees ([`print`]). Parentheses leave no node.

use pest_derive::Parser;

use super::diagnostic::Position;
use super::file::{FileBody, MutationWriteMode, SettingStmt};
use super::value::{AggFunc, CompOp, Literal};

pub mod foreign;
pub mod parse;
pub mod print;
pub mod statement;

#[cfg(test)]
mod tests;

/// The GQ grammar (`grammar.pest`).
#[derive(Parser)]
#[grammar = "query/syntax/grammar.pest"]
pub(crate) struct QueryParser;

/// A parsed source: its settings lines and its body.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceFile {
    pub settings: Vec<SettingStmt>,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    Decls(Vec<Decl>),
    /// `show`, `branch`, `commit` or `load`: one statement.
    Statement(Box<StatementBody>),
    /// `explain` over one declaration.
    Explain(Box<Decl>),
}

/// A statement body compared by its debug form: the statement nodes do not
/// derive `PartialEq`.
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
    /// The type as a parameter declaration spells it.
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
    /// `match { … } where φ`.
    pub pattern: Box<Pattern>,
    pub distinct: bool,
    pub projections: Vec<Projection>,
    pub order: Vec<Ordering>,
    pub limit: Option<Limit>,
    /// Where each part was written.
    pub spans: Box<ReadSpans>,
}

/// The elements of a `match { … }` and the `where` after its braces, or of
/// a block's or an `optional`'s braces with the `where` inside them
/// (section 3): the elements produce rows, the predicate removes them.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Pattern {
    pub elements: Vec<Element>,
    /// The one Boolean expression of `where`, evaluated after every element.
    pub predicate: Option<Expr>,
    /// Where each part was written.
    pub spans: PatternSpans,
}

/// Where a pattern's parts were written, for a type-check refusal's position
/// and suggestion. Like [`SourceSpan`], it takes no part in tree equality; a
/// pattern built rather than parsed has none.
#[derive(Debug, Clone, Default)]
pub struct PatternSpans {
    /// One per element, in order.
    pub elements: Vec<SourceSpan>,
    /// The `where` expression.
    pub predicate: Option<SourceSpan>,
    /// The byte just after the closing brace of `match { … }`, where a
    /// `where` would open.
    pub close: Option<usize>,
}

impl PartialEq for PatternSpans {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

/// Where a read body's parts were written, for a type-check refusal's
/// position and suggestion. Like [`SourceSpan`], it takes no part in tree
/// equality; a body built rather than parsed has none.
#[derive(Debug, Clone, Default)]
pub struct ReadSpans {
    /// The text between the `match` braces.
    pub match_inner: Option<SourceSpan>,
    /// One per return item, the item without its alias.
    pub items: Vec<SourceSpan>,
    /// One per order key, its direction included.
    pub order: Vec<SourceSpan>,
    /// The `limit` value.
    pub limit: Option<SourceSpan>,
}

impl PartialEq for ReadSpans {
    fn eq(&self, _: &Self) -> bool {
        true
    }
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
    /// One or more groups: each a selection, `match { … } where φ` or none,
    /// then the statements that write over its rows (RFC section 2, the
    /// write groups).
    pub groups: Vec<WriteGroup>,
}

impl WriteBody {
    /// Every group's elements, in order.
    pub fn all_elements(&self) -> Vec<&Element> {
        self.groups
            .iter()
            .flat_map(|g| g.pattern.elements.iter())
            .collect()
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
    /// The group's `match { … } where φ`; empty for a group with no `match`.
    pub pattern: Pattern,
    pub statements: Vec<Stmt>,
    /// Where each statement was written; empty for a group built rather than
    /// parsed. No part of tree equality.
    pub spans: GroupSpans,
}

#[derive(Debug, Clone, Default)]
pub struct GroupSpans {
    pub statements: Vec<SourceSpan>,
}

impl PartialEq for GroupSpans {
    fn eq(&self, _: &Self) -> bool {
        true
    }
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

/// An element of `match { … }`: a form that produces rows.
#[derive(Debug, Clone, PartialEq)]
pub enum Element {
    /// `let $name = value`: a named value or a named relation.
    Let {
        name: String,
        value: LetValue,
    },
    /// `from rel`, or `from rel as $s` over a ranking: rows drawn from a
    /// relation; `score` names the score column a ranking adds.
    From {
        rel: RelExpr,
        score: Option<String>,
    },
    Optional(Pattern),
    /// `$x: T`, a node or an edge type.
    Binding {
        variable: String,
        type_name: String,
    },
    Traversal(Traversal),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Traversal {
    pub src: String,
    pub selector: EdgeSelector,
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

/// The source spelling of the edge types a traversal selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeSelector {
    Named(String),
    Alternation(Vec<String>),
    Wildcard,
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
    /// The block's elements and its own `where`, inside its braces.
    pub pattern: Pattern,
    /// Where the block was written, from its function word to its closing
    /// brace: where a fix that replaces the whole block writes.
    pub at: SourceSpan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockFunc {
    Agg(AggFunc),
    Exists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
        /// Where it was written: a literal operand the type checker refuses
        /// against the other operand's property is replaced at its range.
        at: SourceSpan,
    },
    In {
        needle: Box<Expr>,
        list: Box<Expr>,
        /// Where it was written: `$d in $x` over a node in `where` is a
        /// triple, refused with the `match` element as the fix.
        at: SourceSpan,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    /// An infix `contains` or `starts_with`. The type checker
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
    /// The byte range of each operand of a comparison or an `in`, in
    /// `operands` order, then, for an `in` over a written list, of each of
    /// its elements: where a refused literal operand is replaced. A
    /// triple's element span holds its two ends, source then destination,
    /// which its `T82` fix swaps; a `from`'s holds its relation, after which
    /// its `T85` fix writes or removes `as`. Empty for a node built rather
    /// than parsed and for the other nodes.
    pub ranges: Vec<(usize, usize)>,
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
            ranges: Vec::new(),
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
