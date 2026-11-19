//! The typed core IR (the GQ 3 RFC, section 2).
//!
//! The type checker ([`crate::query::check`]) lowers every
//! declaration into one [`Term`] tree over the thirteen core operators, the
//! term former `Call` and the two leaves the operators need (`Unit`, the one
//! empty row, and `Env`, the one enclosing row of a correlated term). A write
//! is a list of [`WriteGroup`]s, each a term and its sinks.
//!
//! The planner reads only this tree. It lowers the forms the engine runs
//! (a pattern of scans, expansions, filters and correlated blocks, its
//! projection, grouping, order and cut, the rankings over a population, and
//! the write groups) to today's plans through [`crate::shape`], which the type
//! checker also runs: every other node is refused at type check with `T86`.
//!
//! Every scalar a term holds (a filter's predicate, a projected or grouped
//! item, an order key, an aggregate's argument, a score's query, a sink's
//! assignment) is the bound expression [`IRExpr`], the one the planner and
//! the engine evaluate: a reference column reads as a `Variable`, a value column as an
//! `AliasRef`, a property or meta field as a `PropAccess`.
//!
//! Every node derives `PartialEq`: the desugaring-uniqueness test compares
//! whole terms. Names are deterministic (fresh names come from a counter per
//! declaration, [`Fresh`]), so two spellings of one term compare equal.

use std::collections::HashMap;
use std::sync::Arc;

use super::expr::{IRExpr, IROrdering};
use crate::query::file::MutationWriteMode;
use crate::query::value::{AggFunc, Literal};
use crate::traversal::EdgeSelection;
use crate::types::{ExprType, PropType, ScalarType};

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

    /// The expression type of a value of this type: a reference's
    /// nullability is the column's, so the reference type carries none.
    pub fn expr_type(&self) -> ExprType {
        match self {
            Self::Value(p) => ExprType::from_prop(p),
            Self::Node { type_name, .. } => ExprType::Node {
                type_name: type_name.clone(),
            },
            Self::Edge { type_name, .. } => ExprType::Edge {
                type_name: type_name.clone(),
            },
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

/// One aggregate of a `Group`; `arg: None` counts rows.
#[derive(Debug, Clone, PartialEq)]
pub struct Agg {
    pub func: AggFunc,
    pub distinct: bool,
    pub arg: Option<IRExpr>,
    /// `percentile`'s `p`, a numeric literal in [0, 1] or a declared
    /// numeric parameter; `None` for every other aggregate.
    pub fraction: Option<IRExpr>,
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
    pub text: IRExpr,
    pub mode_all: bool,
    pub max_edits: IRExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScoreQuery {
    Vector(IRExpr),
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
        input: Arc<Term>,
        src: String,
        dst: String,
        /// The node type `src` holds.
        src_type: String,
        /// The node type `dst` takes.
        dst_type: String,
        /// The edges traversed from `src`: one type, an alternation or a
        /// wildcard, members sorted by name, each with its direction from
        /// `src`.
        edges: EdgeSelection,
        min_hops: u32,
        max_hops: Option<u32>,
        edge_var: Option<String>,
    },
    /// A `where` conjunct in its written position, over every element of
    /// its scope, or a closing triple's identity test.
    Filter {
        input: Arc<Term>,
        pred: IRExpr,
        /// The conjunct can fail where it is evaluated: its predicate or a
        /// block it reads (the type checker's one computation of it, A3).
        /// The planner moves a conjunct that cannot fail and keeps this one
        /// in written order.
        fails: bool,
    },
    /// `keep`: the input's columns are kept and `items` appended (the
    /// derived-forms table's `Extend` without an aggregate); otherwise the
    /// output is `items` alone.
    Project {
        input: Arc<Term>,
        items: Vec<(String, IRExpr)>,
        keep: bool,
        /// An item can fail where it is evaluated: a `let` that can fail
        /// runs on every row its input produces, and nothing moves below it.
        fails: bool,
    },
    Group {
        input: Arc<Term>,
        keys: Vec<(String, IRExpr)>,
        aggs: Vec<(String, Agg)>,
    },
    /// The outer columns, then the inner term's new columns.
    Correlate {
        outer: Arc<Term>,
        inner: Arc<Term>,
        /// The inner term can fail where it is evaluated (a `from`, an
        /// `optional` or a block over a term that can fail): it runs on every
        /// outer row, and nothing moves below it.
        fails: bool,
    },
    Union {
        left: Arc<Term>,
        right: Arc<Term>,
    },
    Order {
        input: Arc<Term>,
        /// The keys, each with its direction and written null placement.
        keys: Vec<IROrdering>,
    },
    Cut {
        input: Arc<Term>,
        k: Count,
    },
    Score {
        input: Arc<Term>,
        column: String,
        property: String,
        kind: ScoreKind,
        query: ScoreQuery,
        /// The score's column, named by the `from`'s `as`; `distance` or
        /// `score` in a relation function's argument, where it has no name.
        out: String,
    },
    ApproxTopK {
        input: Arc<Term>,
        column: String,
        property: String,
        query: IRExpr,
        k: Count,
        /// `None`: the engine's policy *[as today]*.
        recall: Option<f64>,
        out: String,
    },
    Fuse {
        arms: Vec<Term>,
        target: String,
        /// The fusion constant, `RRF_DEFAULT_K` when unwritten.
        k: Count,
        weights: Vec<f64>,
        /// The fused score's column, named by the `from`'s `as`.
        out: String,
    },
    /// The least fixpoint of a declared read over itself; `body` reads the
    /// recursive relation through [`Term::RecRef`].
    Fix {
        name: String,
        body: Arc<Term>,
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
        bindings: Vec<(String, IRExpr)>,
        body: Arc<Term>,
    },
}

impl Term {
    /// The term as a child of another: shared, so a pattern the core
    /// writes twice (an `optional`'s) is stored once.
    pub fn boxed(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// Whether an expression of the term can fail where it is evaluated:
    /// such a term keeps its place among the elements (A3). The type
    /// checker computes this once, when it builds the term, and records it
    /// on the `Filter`, `Project` and `Correlate` that hold such a term; a
    /// `Group`'s or an `Order`'s own expressions are read here, since one
    /// appears only inside a `Correlate` or at the read's top.
    pub fn can_fail(&self) -> bool {
        self.any_node(Self::fails_here)
    }

    /// Whether this node, its inputs apart, can fail where it is evaluated:
    /// [`Term::can_fail`] holds when one node of the term does.
    pub fn fails_here(&self) -> bool {
        match self {
            Self::Filter { fails, .. }
            | Self::Project { fails, .. }
            | Self::Correlate { fails, .. } => *fails,
            Self::Group { keys, aggs, .. } => {
                keys.iter().any(|(_, e)| e.can_fail())
                    || aggs
                        .iter()
                        .any(|(_, agg)| agg.arg.as_ref().is_some_and(IRExpr::can_fail))
            }
            Self::Order { keys, .. } => keys.iter().any(|k| k.expr.can_fail()),
            _ => false,
        }
    }

    /// The node's shared inputs (every input but a `Fuse`'s arms, which it
    /// holds in place).
    pub fn shared_children(&self) -> Vec<&Arc<Term>> {
        match self {
            Self::Unit
            | Self::Env(_)
            | Self::Scan { .. }
            | Self::RecRef { .. }
            | Self::Fuse { .. } => Vec::new(),
            Self::Expand { input, .. }
            | Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Group { input, .. }
            | Self::Order { input, .. }
            | Self::Cut { input, .. }
            | Self::Score { input, .. }
            | Self::ApproxTopK { input, .. } => vec![input],
            Self::Correlate { outer, inner, .. } => vec![outer, inner],
            Self::Union { left, right } => vec![left, right],
            Self::Fix { body, .. } | Self::Call { body, .. } => vec![body],
        }
    }

    /// Every stored node of the term once, pre-order: a subterm two parents
    /// share (an `optional`'s pattern) is listed where it is first reached.
    /// The walk keeps its own stack.
    pub fn stored_nodes(&self) -> Vec<&Term> {
        let mut seen: std::collections::HashSet<*const Term> = std::collections::HashSet::new();
        let mut out = Vec::new();
        let mut pending = vec![self];
        while let Some(node) = pending.pop() {
            if seen.insert(node as *const Term) {
                out.push(node);
                pending.extend(node.children().into_iter().rev());
            }
        }
        out
    }

    /// Whether `test` holds for a node of the term. Each stored node is
    /// tested once, so a subterm two parents share (an `optional`'s
    /// pattern) costs its size once, and the walk keeps its own stack.
    pub fn any_node(&self, mut test: impl FnMut(&Term) -> bool) -> bool {
        let mut seen: std::collections::HashSet<*const Term> = std::collections::HashSet::new();
        let mut pending = vec![self];
        while let Some(node) = pending.pop() {
            if !seen.insert(node as *const Term) {
                continue;
            }
            if test(node) {
                return true;
            }
            pending.extend(node.children());
        }
        false
    }

    /// The number of nodes the plan budget charges: the expanded tree, a
    /// `Call`'s body at each call site as the RFC's `Call` rule expands it,
    /// except that the subterm an `optional`'s desugaring writes twice (its
    /// pattern, which `Union(p, …¬ exists p…)` both returns and tests for
    /// absence) is stored once, shared by the `Union`'s two arms, and
    /// charged once, as the one expansion it is. Linear in the stored
    /// nodes, on a stack of its own.
    pub fn size(&self) -> usize {
        // Each pending node with the chain of `Union` left arms its
        // right-arm ancestors share: a node that is one of them is the
        // shared pattern, already charged under the left arm.
        let mut shared: Vec<(*const Term, Option<usize>)> = Vec::new();
        let mut pending: Vec<(&Term, Option<usize>)> = vec![(self, None)];
        let mut size = 0;
        while let Some((node, chain)) = pending.pop() {
            let mut at = chain;
            let mut charged = false;
            while let Some(i) = at {
                if std::ptr::eq(shared[i].0, node) {
                    charged = true;
                    break;
                }
                at = shared[i].1;
            }
            if charged {
                continue;
            }
            size += 1;
            match node {
                Self::Union { left, right } => {
                    shared.push((Arc::as_ptr(left), chain));
                    pending.push((right, Some(shared.len() - 1)));
                    pending.push((left, chain));
                }
                other => pending.extend(other.children().into_iter().map(|c| (c, chain))),
            }
        }
        size
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

    /// The term nodes and the expression nodes of the expanded tree, as
    /// [`Term::size`] counts it (a `Call`'s body at each call site, an
    /// `optional`'s shared pattern once), counted until together they pass
    /// `limit`: the walk costs at most `limit` steps past the nodes it
    /// counts, however large the expansion.
    pub fn weight(&self, limit: usize) -> (usize, usize) {
        enum Next<'t> {
            Visit(&'t Term),
            Mark(*const Term),
            Unmark(*const Term),
        }
        let mut shared: HashMap<*const Term, usize> = HashMap::new();
        let mut pending = vec![Next::Visit(self)];
        let (mut terms, mut exprs) = (0usize, 0usize);
        while let Some(next) = pending.pop() {
            let node = match next {
                Next::Visit(node) => node,
                Next::Mark(at) => {
                    *shared.entry(at).or_default() += 1;
                    continue;
                }
                Next::Unmark(at) => {
                    if let Some(n) = shared.get_mut(&at) {
                        *n -= 1;
                    }
                    continue;
                }
            };
            if shared.get(&(node as *const Term)).is_some_and(|n| *n > 0) {
                continue;
            }
            terms += 1;
            exprs += node.expr_nodes();
            if terms + exprs > limit {
                break;
            }
            match node {
                // The left arm is the pattern the right arm tests for
                // absence: counted once, under the left arm.
                Self::Union { left, right } => {
                    let at = Arc::as_ptr(left);
                    pending.push(Next::Unmark(at));
                    pending.push(Next::Visit(right));
                    pending.push(Next::Mark(at));
                    pending.push(Next::Visit(left));
                }
                other => pending.extend(other.children().into_iter().map(Next::Visit)),
            }
        }
        (terms, exprs)
    }

    /// The expression nodes this node holds, its inputs' apart: the plan
    /// budget's measure of what a phase copies of it ([`crate::budget`]).
    pub fn expr_nodes(&self) -> usize {
        match self {
            Self::Filter { pred, .. } => pred.nodes(),
            Self::Project { items, .. } => items.iter().map(|(_, e)| e.nodes()).sum(),
            Self::Group { keys, aggs, .. } => {
                keys.iter().map(|(_, e)| e.nodes()).sum::<usize>()
                    + aggs
                        .iter()
                        .map(|(_, a)| 1 + a.arg.as_ref().map_or(0, IRExpr::nodes))
                        .sum::<usize>()
            }
            Self::Order { keys, .. } => keys.iter().map(|k| k.expr.nodes()).sum(),
            Self::ApproxTopK { query, .. } => query.nodes(),
            Self::Score {
                query: ScoreQuery::Vector(query),
                ..
            } => query.nodes(),
            Self::Call { bindings, .. } => bindings.iter().map(|(_, e)| e.nodes()).sum(),
            Self::Unit
            | Self::Env(_)
            | Self::Scan { .. }
            | Self::Expand { .. }
            | Self::Correlate { .. }
            | Self::Union { .. }
            | Self::Cut { .. }
            | Self::Score { .. }
            | Self::Fuse { .. }
            | Self::Fix { .. }
            | Self::RecRef { .. } => 0,
        }
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
            Self::Correlate { outer, inner, .. } => vec![outer, inner],
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
    /// The declared default of each parameter that writes one, in
    /// declaration order: the value binding uses when the caller omits it.
    pub defaults: Vec<(String, Literal)>,
    pub term: Term,
    pub result: RelTy,
    /// `@budget(plan = n)`, or the compiler's constant.
    pub plan_budget: usize,
    /// The plan budget and the work typing charged to it, which shaping and
    /// planning go on charging ([`crate::budget`]).
    pub work: crate::budget::WorkSoFar,
    /// The names the type checker made up, with their roles.
    pub fresh: Fresh,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssignValue {
    Null,
    Expr(IRExpr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Sink {
    Insert {
        /// What the write does on an existing key (the GQ 3 RFC, section 3,
        /// Keyed writes).
        mode: crate::query::syntax::InsertMode,
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
    /// As [`CoreRead::defaults`].
    pub defaults: Vec<(String, Literal)>,
    pub groups: Vec<WriteGroup>,
    pub class: WriteClass,
    pub write_mode: MutationWriteMode,
    /// As [`CoreRead::fresh`].
    pub fresh: Fresh,
    /// As [`CoreRead::work`]: each group's selection read is shaped against
    /// it.
    pub work: crate::budget::WorkSoFar,
}

impl CoreWrite {
    /// Whether a group selects its rows from stored data: any group with a
    /// `match`, whose input is more than `Unit`. Such a write reveals what
    /// it read through its affected counts and data-dependent failures, so
    /// policy asks for `read` on the branch as well as `change`; an
    /// `insert` with no `match` runs once over `Unit` and reads nothing.
    pub fn selects_stored_rows(&self) -> bool {
        self.groups
            .iter()
            .any(|group| !matches!(group.input, Term::Unit))
    }
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
/// leaves its value open, Unresolved question 3), in the work units of
/// [`crate::budget`]. The largest charge of a read in the GQT corpus is
/// 2,506 (`optional_thirty_blocks_run.gqt`: typing, shaping and planning)
/// and of a generated read 960 (41,732 reads of the generated laws at their
/// nightly breadth), so the default is thirteen times the corpus's largest.
pub const DEFAULT_PLAN_BUDGET: usize = 32_768;

/// `rrf`'s defaults.
pub const RRF_DEFAULT_K: u64 = 60;
pub const RRF_MAX_K: u64 = 1_000_000;
pub const RRF_MIN_ARMS: usize = 2;
pub const RRF_MAX_ARMS: usize = 16;

/// The exact graph namespace selected for a mutation target.
///
/// Node and edge declarations may share a name, so consumers must carry the
/// resolved kind from type checking instead of re-deriving it from spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationTarget {
    Node { type_name: String },
    Edge { type_name: String },
}

impl MutationTarget {
    pub fn type_name(&self) -> &str {
        match self {
            Self::Node { type_name } | Self::Edge { type_name } => type_name,
        }
    }

    pub fn is_edge(&self) -> bool {
        matches!(self, Self::Edge { .. })
    }
}

/// What a name the type checker made up stands for. The planner and the
/// shaper read a made-up name's role here, never from its spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FreshRole {
    /// An anonymous triple end, `$_`.
    Anonymous,
    /// The fresh destination a triple whose ends are both bound expands to,
    /// tied to the bound end by an identity filter.
    Closing,
    /// The column a block read as a value fills (its `Correlate`'s `Group`).
    Block,
    /// The column a guarded block's guard is computed into.
    Guard,
    /// A return item's or a hidden aggregate's column beside the match
    /// columns, spelled `^name`, which no written name is.
    Item,
}

impl FreshRole {
    /// The leading word of a name of this role. It starts with an
    /// uppercase letter, which no written variable, alias, property or
    /// parameter does (section 7's `ident`), so a made-up name never
    /// collides with a written one.
    fn word(self) -> &'static str {
        match self {
            Self::Anonymous => "Anon",
            Self::Closing => "Closing",
            Self::Block => "Block",
            Self::Guard => "Guard",
            Self::Item => "^",
        }
    }
}

/// The names the type checker made up for one declaration, each with its
/// role: the typed record a consumer reads to tell a made-up column from a
/// written one, in place of a test of the name's spelling.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Fresh {
    roles: std::collections::BTreeMap<String, FreshRole>,
    count: u32,
}

impl Fresh {
    /// A new name of `role`, recorded.
    pub fn mint(&mut self, role: FreshRole) -> String {
        self.count += 1;
        let name = format!("{}{}", role.word(), self.count);
        self.roles.insert(name.clone(), role);
        name
    }

    /// Record `name`, which the type checker spelled itself, under `role`.
    pub fn record(&mut self, name: &str, role: FreshRole) {
        self.roles.insert(name.to_string(), role);
    }

    /// The role of `name`, when the type checker made it up.
    pub fn role(&self, name: &str) -> Option<FreshRole> {
        self.roles.get(name).copied()
    }

    /// Whether `name` was made up with `role`.
    pub fn is(&self, name: &str, role: FreshRole) -> bool {
        self.role(name) == Some(role)
    }

    /// Whether `name` was made up: the query declares no such binding, so
    /// its identity orders nothing a reader can see.
    pub fn made_up(&self, name: &str) -> bool {
        self.roles.contains_key(name)
    }

    /// Every made-up name.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.roles.keys().map(String::as_str)
    }
}
