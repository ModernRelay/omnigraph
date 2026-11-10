//! The typed core IR of GQ edition 3 (docs/rfcs/2026-11-07-gq-3.md,
//! section 2).
//!
//! The edition-3 type checker ([`crate::query::check`]) lowers every
//! declaration into one [`Term`] tree over the thirteen core operators, the
//! term former `Call` and the two leaves the operators need (`Unit`, the one
//! empty row, and `Env`, the one enclosing row of a correlated term). A write
//! is a list of [`WriteGroup`]s, each a term and its sinks.
//!
//! Today's [`super::QueryIR`] has no node for `Correlate`, `Union`, `Group`
//! over a derived relation, `Score`, `ApproxTopK` as a relation, `Fuse` over
//! relations, `Fix` or `Call`; this tree is the compiler's half, and the
//! planner does not lower it yet (`P006`), so nothing in edition 3 executes.
//!
//! Every node derives `PartialEq`: the desugaring-uniqueness test compares
//! whole terms. Names are deterministic (fresh names come from a counter per
//! declaration), so two spellings of one term compare equal.

use crate::query::ast::{AggFunc, CompOp, Literal, MutationWriteMode};
use crate::query::syntax::ArithOp;
use crate::query::typecheck::MutationTarget;
use crate::types::{Direction, PropType, ScalarType};

/// A value type with its nullability: a scalar or list (as the schema spells
/// it), a node reference or an edge reference.
#[derive(Debug, Clone, PartialEq)]
pub enum Ty {
    Value(PropType),
    /// A node reference; `type_name` may name an interface (a polymorphic
    /// column).
    Node {
        type_name: String,
        nullable: bool,
    },
    Edge {
        type_name: String,
        nullable: bool,
    },
}

impl Ty {
    pub fn scalar(scalar: ScalarType, nullable: bool) -> Self {
        Self::Value(PropType::scalar(scalar, nullable))
    }

    pub fn nullable(&self) -> bool {
        match self {
            Self::Value(p) => p.nullable,
            Self::Node { nullable, .. } | Self::Edge { nullable, .. } => *nullable,
        }
    }

    pub fn with_nullable(&self, nullable: bool) -> Self {
        let mut out = self.clone();
        match &mut out {
            Self::Value(p) => p.nullable = nullable,
            Self::Node { nullable: n, .. } | Self::Edge { nullable: n, .. } => *n = nullable,
        }
        out
    }

    pub fn is_reference(&self) -> bool {
        matches!(self, Self::Node { .. } | Self::Edge { .. })
    }

    pub fn as_value(&self) -> Option<&PropType> {
        match self {
            Self::Value(p) => Some(p),
            Self::Node { .. } | Self::Edge { .. } => None,
        }
    }

    /// Base-type equality, nullability ignored (`Union`'s rule).
    pub fn same_base(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Value(a), Self::Value(b)) => a.scalar == b.scalar && a.list == b.list,
            (Self::Node { type_name: a, .. }, Self::Node { type_name: b, .. })
            | (Self::Edge { type_name: a, .. }, Self::Edge { type_name: b, .. }) => a == b,
            _ => false,
        }
    }

    pub fn display(&self) -> String {
        match self {
            Self::Value(p) => p.display_name(),
            Self::Node {
                type_name,
                nullable,
            } => format!("node {type_name}{}", if *nullable { "?" } else { "" }),
            Self::Edge {
                type_name,
                nullable,
            } => format!("edge {type_name}{}", if *nullable { "?" } else { "" }),
        }
    }
}

/// One named column of a relation. Names are variable names without `$`.
#[derive(Debug, Clone, PartialEq)]
pub struct Col {
    pub name: String,
    pub ty: Ty,
}

/// `R[c1: t1, …, cn: tn]`: an ordered list of named typed columns.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RelTy {
    pub cols: Vec<Col>,
}

impl RelTy {
    pub fn get(&self, name: &str) -> Option<&Col> {
        self.cols.iter().find(|c| c.name == name)
    }

    pub fn names(&self) -> Vec<String> {
        self.cols.iter().map(|c| c.name.clone()).collect()
    }

    pub fn node_columns(&self) -> Vec<&Col> {
        self.cols
            .iter()
            .filter(|c| matches!(c.ty, Ty::Node { .. }))
            .collect()
    }
}

/// A typed scalar expression: the operand language of Filter, Project, Group
/// and Order.
#[derive(Debug, Clone, PartialEq)]
pub struct SExpr {
    pub kind: SKind,
    pub ty: Ty,
    /// Arithmetic and registry rows declared `can_fail`; the planner keeps the
    /// term behind its guards (A3).
    pub can_fail: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SKind {
    Lit(Literal),
    /// `null`, admitted only as an assignment value.
    Null,
    /// A declared parameter of the innermost declaration (a `Call` frame binds
    /// it; it is never textually substituted).
    Param(String),
    /// A column of the current row.
    Col(String),
    /// A property of a reference column.
    Prop {
        col: String,
        prop: String,
    },
    /// A meta-field (`@id`, `@src`, `@dst`, `@type`) of a reference column.
    Meta {
        col: String,
        field: String,
    },
    Arith {
        op: ArithOp,
        left: Box<SExpr>,
        right: Box<SExpr>,
    },
    Neg(Box<SExpr>),
    Cmp {
        op: CompOp,
        left: Box<SExpr>,
        right: Box<SExpr>,
    },
    In {
        needle: Box<SExpr>,
        list: Box<SExpr>,
    },
    IsNull {
        expr: Box<SExpr>,
        negated: bool,
    },
    Not(Box<SExpr>),
    And(Box<SExpr>, Box<SExpr>),
    Or(Box<SExpr>, Box<SExpr>),
    /// A registry scalar row, its options in registry order with defaults
    /// filled (so a spelled default and an omitted one are one term).
    Call {
        func: String,
        args: Vec<SExpr>,
        options: Vec<(String, SExpr)>,
    },
    /// An explicit numeric conversion (comparison casts stay explicit, as
    /// today; arithmetic widening is one too).
    Cast(Box<SExpr>),
}

impl SExpr {
    pub fn new(kind: SKind, ty: Ty) -> Self {
        let can_fail = match &kind {
            SKind::Arith { .. } | SKind::Neg(_) => true,
            SKind::Cmp { left, right, .. }
            | SKind::And(left, right)
            | SKind::Or(left, right)
            | SKind::In {
                needle: left,
                list: right,
            } => left.can_fail || right.can_fail,
            SKind::Not(e) | SKind::Cast(e) | SKind::IsNull { expr: e, .. } => e.can_fail,
            SKind::Call { args, options, .. } => {
                args.iter().any(|a| a.can_fail) || options.iter().any(|(_, a)| a.can_fail)
            }
            SKind::Lit(_)
            | SKind::Null
            | SKind::Param(_)
            | SKind::Col(_)
            | SKind::Prop { .. }
            | SKind::Meta { .. } => false,
        };
        Self { kind, ty, can_fail }
    }

    /// The columns the expression reads.
    pub fn columns(&self, out: &mut Vec<String>) {
        match &self.kind {
            SKind::Col(c) | SKind::Prop { col: c, .. } | SKind::Meta { col: c, .. } => {
                if !out.contains(c) {
                    out.push(c.clone());
                }
            }
            SKind::Arith { left, right, .. }
            | SKind::Cmp { left, right, .. }
            | SKind::And(left, right)
            | SKind::Or(left, right)
            | SKind::In {
                needle: left,
                list: right,
            } => {
                left.columns(out);
                right.columns(out);
            }
            SKind::Neg(e) | SKind::Not(e) | SKind::Cast(e) | SKind::IsNull { expr: e, .. } => {
                e.columns(out)
            }
            SKind::Call { args, options, .. } => {
                for a in args {
                    a.columns(out);
                }
                for (_, a) in options {
                    a.columns(out);
                }
            }
            SKind::Lit(_) | SKind::Null | SKind::Param(_) => {}
        }
    }

    pub fn is_reference_read(&self) -> bool {
        matches!(
            self.kind,
            SKind::Col(_) | SKind::Prop { .. } | SKind::Meta { .. }
        )
    }
}

/// One aggregate of a `Group`; `arg: None` counts rows.
#[derive(Debug, Clone, PartialEq)]
pub struct Agg {
    pub func: AggFunc,
    pub distinct: bool,
    pub arg: Option<SExpr>,
    pub ty: Ty,
}

/// A `Cut`'s or a window's row count: a literal or a parameter (`limit $k`).
#[derive(Debug, Clone, PartialEq)]
pub enum Count {
    Literal(u64),
    Param(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoreKind {
    /// The field's declared distance, `F32`, ascending.
    Distance,
    /// `bm25_v1` over the field corpus, `F64`, descending; keeps members only.
    Bm25,
}

/// The lexical query a `bm25` or `match_terms` consumes: `terms(text, mode,
/// max_edits)` with defaults filled; a bare String is `terms(q, mode: any)`.
#[derive(Debug, Clone, PartialEq)]
pub struct LexicalQuery {
    pub text: SExpr,
    pub mode_all: bool,
    pub max_edits: SExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScoreQuery {
    Vector(SExpr),
    Lexical(LexicalQuery),
}

/// The thirteen operators, `Call`, and the leaves.
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    /// The one empty row: the input of a write group with no clauses, and of
    /// a scope whose first clause is a filter.
    Unit,
    /// The one enclosing row of a correlated term, exposing exactly the outer
    /// columns the term reads (its free variables), in outer column order.
    Env(Vec<Col>),
    Scan {
        var: String,
        ty: Ty,
    },
    Expand {
        input: Box<Term>,
        src: String,
        dst: String,
        /// The node type `dst` takes.
        dst_type: String,
        /// The edge selector: (edge type, direction) pairs, sorted.
        selector: Vec<(String, Direction)>,
        min_hops: u32,
        max_hops: Option<u32>,
        edge_var: Option<String>,
    },
    Filter {
        input: Box<Term>,
        pred: SExpr,
    },
    /// `keep`: the input's columns are kept and `items` appended (the
    /// derived-forms table's `Extend` without an aggregate); otherwise the
    /// output is `items` alone.
    Project {
        input: Box<Term>,
        items: Vec<(String, SExpr)>,
        keep: bool,
    },
    Group {
        input: Box<Term>,
        keys: Vec<(String, SExpr)>,
        aggs: Vec<(String, Agg)>,
    },
    /// The outer columns, then the inner term's new columns.
    Correlate {
        outer: Box<Term>,
        inner: Box<Term>,
    },
    Union {
        left: Box<Term>,
        right: Box<Term>,
    },
    Order {
        input: Box<Term>,
        /// (key, descending)
        keys: Vec<(SExpr, bool)>,
    },
    Cut {
        input: Box<Term>,
        k: Count,
    },
    Score {
        input: Box<Term>,
        column: String,
        property: String,
        kind: ScoreKind,
        query: ScoreQuery,
        /// `distance` or `score`.
        out: String,
    },
    ApproxTopK {
        input: Box<Term>,
        column: String,
        property: String,
        query: SExpr,
        k: Count,
        /// `None`: the engine's policy *[as today]*.
        recall: Option<f64>,
        out: String,
    },
    Fuse {
        arms: Vec<Term>,
        target: String,
        k: u64,
        weights: Vec<f64>,
    },
    /// The least fixpoint of a declared read over itself; `body` reads the
    /// recursive relation through [`Term::RecRef`].
    Fix {
        name: String,
        body: Box<Term>,
        /// `@budget(rounds = n)`.
        rounds: Option<u64>,
        /// The column `min` or `max` orders, if any (the lattice columns).
        lattice: Vec<String>,
    },
    RecRef {
        name: String,
        ty: RelTy,
    },
    /// A declared read inlined with its scalar arguments bound to its
    /// parameters (evaluated in the caller's scope); `body` is closed.
    Call {
        name: String,
        bindings: Vec<(String, SExpr)>,
        body: Box<Term>,
    },
}

impl Term {
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    /// The number of nodes in the expanded tree (the plan budget counts this,
    /// not the shared graph).
    pub fn size(&self) -> usize {
        1 + match self {
            Self::Unit | Self::Env(_) | Self::Scan { .. } | Self::RecRef { .. } => 0,
            Self::Expand { input, .. }
            | Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Group { input, .. }
            | Self::Order { input, .. }
            | Self::Cut { input, .. }
            | Self::Score { input, .. }
            | Self::ApproxTopK { input, .. } => input.size(),
            Self::Correlate { outer, inner } => outer.size() + inner.size(),
            Self::Union { left, right } => left.size() + right.size(),
            Self::Fuse { arms, .. } => arms.iter().map(Term::size).sum(),
            Self::Fix { body, .. } | Self::Call { body, .. } => body.size(),
        }
    }

    /// The node's kind.
    pub fn node(&self) -> CoreNode {
        match self {
            Self::Unit => CoreNode::Unit,
            Self::Env(_) => CoreNode::Env,
            Self::Scan { .. } => CoreNode::Scan,
            Self::Expand { .. } => CoreNode::Expand,
            Self::Filter { .. } => CoreNode::Filter,
            Self::Project { .. } => CoreNode::Project,
            Self::Group { .. } => CoreNode::Group,
            Self::Correlate { .. } => CoreNode::Correlate,
            Self::Union { .. } => CoreNode::Union,
            Self::Order { .. } => CoreNode::Order,
            Self::Cut { .. } => CoreNode::Cut,
            Self::Score { .. } => CoreNode::Score,
            Self::ApproxTopK { .. } => CoreNode::ApproxTopK,
            Self::Fuse { .. } => CoreNode::Fuse,
            Self::Fix { .. } => CoreNode::Fix,
            Self::RecRef { .. } => CoreNode::RecRef,
            Self::Call { .. } => CoreNode::Call,
        }
    }

    /// The operator's name, as the closure table and the report spell it.
    pub fn operator(&self) -> &'static str {
        self.node().name()
    }

    /// The node's inputs, in the order a pre-order walk visits them.
    pub fn children(&self) -> Vec<&Term> {
        match self {
            Self::Unit | Self::Env(_) | Self::Scan { .. } | Self::RecRef { .. } => Vec::new(),
            Self::Expand { input, .. }
            | Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Group { input, .. }
            | Self::Order { input, .. }
            | Self::Cut { input, .. }
            | Self::Score { input, .. }
            | Self::ApproxTopK { input, .. } => vec![input],
            Self::Correlate { outer, inner } => vec![outer, inner],
            Self::Union { left, right } => vec![left, right],
            Self::Fuse { arms, .. } => arms.iter().collect(),
            Self::Fix { body, .. } | Self::Call { body, .. } => vec![body],
        }
    }

    /// Every node kind in the tree, pre-order.
    pub fn nodes(&self, out: &mut Vec<CoreNode>) {
        out.push(self.node());
        for child in self.children() {
            child.nodes(out);
        }
    }

    /// Every operator name in the tree, pre-order.
    pub fn operators(&self, out: &mut Vec<&'static str>) {
        let mut nodes = Vec::new();
        self.nodes(&mut nodes);
        out.extend(nodes.into_iter().map(CoreNode::name));
    }
}

/// The kind of a core node: the thirteen operators, the term former `Call`,
/// the three leaves and the three sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoreNode {
    Unit,
    Env,
    Scan,
    Expand,
    Filter,
    Project,
    Group,
    Correlate,
    Union,
    Order,
    Cut,
    Score,
    ApproxTopK,
    Fuse,
    Fix,
    RecRef,
    Call,
    Insert,
    Update,
    Delete,
}

impl CoreNode {
    /// The name the RFC's core gives the node.
    pub fn name(self) -> &'static str {
        match self {
            Self::Unit => "Unit",
            Self::Env => "Env",
            Self::Scan => "Scan",
            Self::Expand => "Expand",
            Self::Filter => "Filter",
            Self::Project => "Project",
            Self::Group => "Group",
            Self::Correlate => "Correlate",
            Self::Union => "Union",
            Self::Order => "Order",
            Self::Cut => "Cut",
            Self::Score => "Score",
            Self::ApproxTopK => "ApproxTopK",
            Self::Fuse => "Fuse",
            Self::Fix => "Fix",
            Self::RecRef => "RecRef",
            Self::Call => "Call",
            Self::Insert => "Insert",
            Self::Update => "Update",
            Self::Delete => "Delete",
        }
    }
}

/// A checked read: its parameters, its term and its result columns.
#[derive(Debug, Clone, PartialEq)]
pub struct CoreRead {
    pub name: String,
    pub params: Vec<(String, PropType)>,
    pub term: Term,
    pub result: RelTy,
    /// `@budget(plan = n)`, or the compiler's constant.
    pub plan_budget: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssignValue {
    Null,
    Expr(SExpr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Sink {
    Insert {
        target: MutationTarget,
        assignments: Vec<(String, AssignValue)>,
    },
    Update {
        var: String,
        target: MutationTarget,
        assignments: Vec<(String, AssignValue)>,
    },
    Delete {
        var: String,
        target: MutationTarget,
    },
}

/// One run of clauses and the sinks over its relation: every sink sees the
/// one snapshot the group's clauses read; a later group sees the effects of
/// the groups before it.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteGroup {
    pub input: Term,
    pub sinks: Vec<Sink>,
}

/// The per-write constructive-or-destructive split *[as today]*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteClass {
    Constructive,
    Destructive,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreWrite {
    pub name: String,
    pub params: Vec<(String, PropType)>,
    pub groups: Vec<WriteGroup>,
    pub class: WriteClass,
    pub write_mode: MutationWriteMode,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoreDecl {
    Read(Box<CoreRead>),
    Write(CoreWrite),
}

impl CoreDecl {
    pub fn name(&self) -> &str {
        match self {
            Self::Read(r) => &r.name,
            Self::Write(w) => &w.name,
        }
    }

    /// Every node of the declaration, pre-order: a read's term; for a write,
    /// each group's input term then its sinks, group by group.
    pub fn nodes(&self) -> Vec<CoreNode> {
        let mut out = Vec::new();
        match self {
            Self::Read(r) => r.term.nodes(&mut out),
            Self::Write(w) => {
                for group in &w.groups {
                    group.input.nodes(&mut out);
                    out.extend(group.sinks.iter().map(Sink::node));
                }
            }
        }
        out
    }
}

impl Sink {
    /// The sink's kind.
    pub fn node(&self) -> CoreNode {
        match self {
            Self::Insert { .. } => CoreNode::Insert,
            Self::Update { .. } => CoreNode::Update,
            Self::Delete { .. } => CoreNode::Delete,
        }
    }
}

/// The compiler-wide plan budget when a declaration declares none (the RFC
/// leaves its value open, Unresolved question 2).
pub const DEFAULT_PLAN_BUDGET: usize = 4096;

/// `rrf`'s defaults.
pub const RRF_DEFAULT_K: u64 = 60;
pub const RRF_MAX_K: u64 = 1_000_000;
pub const RRF_MIN_ARMS: usize = 2;
pub const RRF_MAX_ARMS: usize = 16;
