//! The surface tree (the GQ 3 RFC, sections 3, 6 and 7):
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
pub mod recover;
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
    /// `explain` over one declaration, after the declarations its read may
    /// call.
    Explain {
        before: Vec<Decl>,
        decl: Box<Decl>,
    },
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
    /// Where the name was written: the position of a refusal of the whole
    /// declaration.
    pub at: SourceSpan,
    pub params: Vec<Param>,
    pub annotations: Vec<Annotation>,
    pub write_mode: MutationWriteMode,
    pub body: DeclBody,
    /// The non-canonical spellings the parser admitted in the declaration,
    /// each with its canonical text (section 6). No part of tree equality.
    pub respellings: Respellings,
}

/// A non-canonical spelling the parser admits and lowers to its canonical
/// form's typed IR (the GQ 3 RFC, sections 3 and 6): `case` for nested
/// `if`, `||` for `concat`, `<>` for `!=`, a single-quoted string, and a
/// keyword or registry name in another letter case. Lint names each one with
/// `fix` replacing `start..end` as its byte-range fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Respelling {
    /// The lint code (`L212` to `L216`).
    pub code: &'static str,
    pub start: usize,
    pub end: usize,
    pub position: Position,
    /// The text as written.
    pub written: String,
    /// The canonical text that replaces it.
    pub fix: String,
}

/// The respellings of one declaration, in source order. Like
/// [`SourceSpan`], they take no part in tree equality, so a respelled text
/// and its canonical form parse to equal trees.
#[derive(Debug, Clone, Default)]
pub struct Respellings(pub Vec<Respelling>);

impl PartialEq for Respellings {
    fn eq(&self, _: &Self) -> bool {
        true
    }
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
    /// One per return item, the item with its alias: where lint's fix of
    /// an alias equal to its column name writes.
    pub projections: Vec<SourceSpan>,
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
    /// `nulls first` (`Some(true)`) or `nulls last` (`Some(false)`); `None`:
    /// the fixed value order's placement, nulls first ascending.
    pub nulls_first: Option<bool>,
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

/// What a keyed statement does when an entity already has its key (the GQ 3
/// RFC, section 3, Keyed writes). On a new key every mode creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InsertMode {
    /// `insert`: an existing key is a `key_conflict`.
    Insert,
    /// `upsert`: sets the named properties and keeps the others.
    Upsert,
    /// `replace`: writes the whole row; an omitted nullable property becomes
    /// null.
    Replace,
}

impl InsertMode {
    /// The statement word.
    pub fn keyword(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Upsert => "upsert",
            Self::Replace => "replace",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Insert {
        mode: InsertMode,
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
    /// The property as written; an edge's endpoints are `@src` and `@dst`.
    pub property: String,
    pub value: Expr,
    /// Where the property's name was written: what a fix of the name
    /// replaces. No part of tree equality.
    pub at: SourceSpan,
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
    /// A second argument, `percentile($x.p, 0.5) { … }`'s fraction; the
    /// type checker refuses one for any other function.
    pub fraction: Option<Box<Expr>>,
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

/// `Clone`, `PartialEq` and `Debug` are written out rather than derived only
/// so that each level of a deep expression enters through
/// [`crate::stack::deep`]; they mean what the derived ones would.
pub enum Expr {
    Literal(Literal),
    Variable(String),
    /// A list literal holding a parameter, `[$a, 2]`: each element an
    /// `Expr::Literal` or an `Expr::Variable`. A list of literals alone is
    /// an `Expr::Literal`.
    List {
        elements: Vec<Expr>,
        /// Where it was written; `ranges` holds each element's byte range,
        /// where a refusal naming the element points.
        at: SourceSpan,
    },
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
    /// keeps today's T57 for the others. `fraction` is a second argument,
    /// `percentile(distinct x, p)`'s, which the type checker refuses with
    /// the rest of the call.
    AggDistinct {
        func: AggFunc,
        arg: Box<Expr>,
        fraction: Option<Box<Expr>>,
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

/// A deep tree is taken apart child by child onto a heap stack, so dropping
/// it never recurses once per level (see [`crate::ir::IRExpr`]'s `Drop`).
impl Drop for Expr {
    fn drop(&mut self) {
        let mut pending: Vec<Expr> = Vec::new();
        self.take_children(&mut pending);
        while let Some(mut child) = pending.pop() {
            child.take_children(&mut pending);
        }
    }
}

impl Expr {
    /// Every boxed child expression of the node moved into `out`, a leaf in
    /// its place. A block's or a call's own expressions stay: they are a
    /// scope of their own, whose drop enters each level through the stack
    /// guard of [`Expr`]'s parts.
    fn take_children(&mut self, out: &mut Vec<Expr>) {
        let mut take = |child: &mut Box<Expr>| {
            if !matches!(child.as_ref(), Expr::Literal(_) | Expr::Variable(_)) {
                out.push(std::mem::replace(
                    child.as_mut(),
                    Expr::Variable(String::new()),
                ));
            }
        };
        match self {
            Self::AggDistinct { arg, fraction, .. } => {
                take(arg);
                if let Some(p) = fraction {
                    take(p);
                }
            }
            Self::Neg(child) | Self::IsNull { expr: child, .. } | Self::Not(child) => take(child),
            Self::Arith { left, right, .. }
            | Self::Compare { left, right, .. }
            | Self::In {
                needle: left,
                list: right,
                ..
            }
            | Self::And(left, right)
            | Self::Or(left, right)
            | Self::InfixString { left, right, .. } => {
                take(left);
                take(right);
            }
            Self::Literal(_)
            | Self::Variable(_)
            | Self::List { .. }
            | Self::Prop { .. }
            | Self::Meta(_)
            | Self::Ident(_)
            | Self::Call(_)
            | Self::Block(_) => {}
        }
    }
}

impl Clone for Expr {
    fn clone(&self) -> Self {
        crate::stack::deep(|| match self {
            Self::Literal(f0) => Self::Literal(f0.clone()),
            Self::Variable(f0) => Self::Variable(f0.clone()),
            Self::List { elements, at } => Self::List {
                elements: elements.clone(),
                at: at.clone(),
            },
            Self::Prop { variable, property } => Self::Prop {
                variable: variable.clone(),
                property: property.clone(),
            },
            Self::Meta(f0) => Self::Meta(f0.clone()),
            Self::Ident(f0) => Self::Ident(f0.clone()),
            Self::Call(f0) => Self::Call(f0.clone()),
            Self::AggDistinct {
                func,
                arg,
                fraction,
            } => Self::AggDistinct {
                func: *func,
                arg: arg.clone(),
                fraction: fraction.clone(),
            },
            Self::Block(f0) => Self::Block(f0.clone()),
            Self::Neg(f0) => Self::Neg(f0.clone()),
            Self::Arith { op, left, right } => Self::Arith {
                op: *op,
                left: left.clone(),
                right: right.clone(),
            },
            Self::Compare {
                op,
                left,
                right,
                at,
            } => Self::Compare {
                op: *op,
                left: left.clone(),
                right: right.clone(),
                at: at.clone(),
            },
            Self::In { needle, list, at } => Self::In {
                needle: needle.clone(),
                list: list.clone(),
                at: at.clone(),
            },
            Self::IsNull { expr, negated } => Self::IsNull {
                expr: expr.clone(),
                negated: *negated,
            },
            Self::Not(f0) => Self::Not(f0.clone()),
            Self::And(f0, f1) => Self::And(f0.clone(), f1.clone()),
            Self::Or(f0, f1) => Self::Or(f0.clone(), f1.clone()),
            Self::InfixString {
                op,
                left,
                right,
                at,
            } => Self::InfixString {
                op: *op,
                left: left.clone(),
                right: right.clone(),
                at: at.clone(),
            },
        })
    }
}

impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        crate::stack::deep(|| match (self, other) {
            (Self::Literal(f01), Self::Literal(f02)) => f01 == f02,
            (Self::Variable(f01), Self::Variable(f02)) => f01 == f02,
            (
                Self::List {
                    elements: elements1,
                    at: at1,
                },
                Self::List {
                    elements: elements2,
                    at: at2,
                },
            ) => elements1 == elements2 && at1 == at2,
            (
                Self::Prop {
                    variable: variable1,
                    property: property1,
                },
                Self::Prop {
                    variable: variable2,
                    property: property2,
                },
            ) => variable1 == variable2 && property1 == property2,
            (Self::Meta(f01), Self::Meta(f02)) => f01 == f02,
            (Self::Ident(f01), Self::Ident(f02)) => f01 == f02,
            (Self::Call(f01), Self::Call(f02)) => f01 == f02,
            (
                Self::AggDistinct {
                    func: func1,
                    arg: arg1,
                    fraction: fraction1,
                },
                Self::AggDistinct {
                    func: func2,
                    arg: arg2,
                    fraction: fraction2,
                },
            ) => func1 == func2 && arg1 == arg2 && fraction1 == fraction2,
            (Self::Block(f01), Self::Block(f02)) => f01 == f02,
            (Self::Neg(f01), Self::Neg(f02)) => f01 == f02,
            (
                Self::Arith {
                    op: op1,
                    left: left1,
                    right: right1,
                },
                Self::Arith {
                    op: op2,
                    left: left2,
                    right: right2,
                },
            ) => op1 == op2 && left1 == left2 && right1 == right2,
            (
                Self::Compare {
                    op: op1,
                    left: left1,
                    right: right1,
                    at: at1,
                },
                Self::Compare {
                    op: op2,
                    left: left2,
                    right: right2,
                    at: at2,
                },
            ) => op1 == op2 && left1 == left2 && right1 == right2 && at1 == at2,
            (
                Self::In {
                    needle: needle1,
                    list: list1,
                    at: at1,
                },
                Self::In {
                    needle: needle2,
                    list: list2,
                    at: at2,
                },
            ) => needle1 == needle2 && list1 == list2 && at1 == at2,
            (
                Self::IsNull {
                    expr: expr1,
                    negated: negated1,
                },
                Self::IsNull {
                    expr: expr2,
                    negated: negated2,
                },
            ) => expr1 == expr2 && negated1 == negated2,
            (Self::Not(f01), Self::Not(f02)) => f01 == f02,
            (Self::And(f01, f11), Self::And(f02, f12)) => f01 == f02 && f11 == f12,
            (Self::Or(f01, f11), Self::Or(f02, f12)) => f01 == f02 && f11 == f12,
            (
                Self::InfixString {
                    op: op1,
                    left: left1,
                    right: right1,
                    at: at1,
                },
                Self::InfixString {
                    op: op2,
                    left: left2,
                    right: right2,
                    at: at2,
                },
            ) => op1 == op2 && left1 == left2 && right1 == right2 && at1 == at2,
            _ => false,
        })
    }
}

impl std::fmt::Debug for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::stack::deep(|| match self {
            Self::Literal(f0) => f.debug_tuple("Literal").field(f0).finish(),
            Self::Variable(f0) => f.debug_tuple("Variable").field(f0).finish(),
            Self::List { elements, at } => f
                .debug_struct("List")
                .field("elements", elements)
                .field("at", at)
                .finish(),
            Self::Prop { variable, property } => f
                .debug_struct("Prop")
                .field("variable", variable)
                .field("property", property)
                .finish(),
            Self::Meta(f0) => f.debug_tuple("Meta").field(f0).finish(),
            Self::Ident(f0) => f.debug_tuple("Ident").field(f0).finish(),
            Self::Call(f0) => f.debug_tuple("Call").field(f0).finish(),
            Self::AggDistinct {
                func,
                arg,
                fraction,
            } => f
                .debug_struct("AggDistinct")
                .field("func", func)
                .field("arg", arg)
                .field("fraction", fraction)
                .finish(),
            Self::Block(f0) => f.debug_tuple("Block").field(f0).finish(),
            Self::Neg(f0) => f.debug_tuple("Neg").field(f0).finish(),
            Self::Arith { op, left, right } => f
                .debug_struct("Arith")
                .field("op", op)
                .field("left", left)
                .field("right", right)
                .finish(),
            Self::Compare {
                op,
                left,
                right,
                at,
            } => f
                .debug_struct("Compare")
                .field("op", op)
                .field("left", left)
                .field("right", right)
                .field("at", at)
                .finish(),
            Self::In { needle, list, at } => f
                .debug_struct("In")
                .field("needle", needle)
                .field("list", list)
                .field("at", at)
                .finish(),
            Self::IsNull { expr, negated } => f
                .debug_struct("IsNull")
                .field("expr", expr)
                .field("negated", negated)
                .finish(),
            Self::Not(f0) => f.debug_tuple("Not").field(f0).finish(),
            Self::And(f0, f1) => f.debug_tuple("And").field(f0).field(f1).finish(),
            Self::Or(f0, f1) => f.debug_tuple("Or").field(f0).field(f1).finish(),
            Self::InfixString {
                op,
                left,
                right,
                at,
            } => f
                .debug_struct("InfixString")
                .field("op", op)
                .field("left", left)
                .field("right", right)
                .field("at", at)
                .finish(),
        })
    }
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
    /// The position of `end`: with `position`, the span a refusal reports.
    pub end_position: Position,
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
            end_position: Position {
                line: 1,
                column: 1,
                byte: 0,
            },
            operands: Vec::new(),
            ranges: Vec::new(),
        }
    }
}

impl SourceSpan {
    /// The span `start..end` of `input`, with no operands.
    pub fn of(input: &str, start: usize, end: usize) -> Self {
        Self {
            start,
            end,
            position: Position::at(input, start),
            end_position: Position::at(input, end),
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
    /// Where the call was written, from its name to its closing parenthesis:
    /// where a fix that replaces the whole call writes. No part of tree
    /// equality.
    pub at: SourceSpan,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Arg {
    Expr(Expr),
    Inline(Box<ReadBody>),
}
