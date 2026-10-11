//! The exact fragment and its checked derivation (RFC 0047, "Checked
//! rewrites for exact search"). Membership is a syntax-and-type check over
//! the declaration's typed lowering: one binding of one node type,
//! eligibility over Boolean, integer and String properties, literals and
//! parameters (a literal or parameter cast to the property's integer type
//! included), an optional leading `bm25()` over a String property, direct
//! property sort keys, a limit, and a projection of properties and the
//! selected score.
//!
//! For a member, the validator builds the canonical chain from the typed IR
//! (`Limit(n, Sort(K, Project(V, [Search(a)] Filter(p, Scan(C)))))`, leaf
//! first), applies the optimizer's recorded rule applications one by one,
//! each checked against its precondition and reconstructed by the validator,
//! and requires the result to equal the candidate plan node for node. A
//! derivation records rule ids, node references and typed substitutions;
//! the successors are the validator's to reconstruct, never the optimizer's
//! to assert. The rule catalogue is closed and versioned
//! ([`super::RULES_VERSION`]).

use std::collections::BTreeSet;

use omnigraph_compiler::catalog::NodeType;
use omnigraph_compiler::ir::{IRExpr, IROp, IROrdering, IRProjection, QueryIR};
use omnigraph_compiler::query::ast::{BinaryOp, CompOp, Literal};
use omnigraph_compiler::types::{ExprType, PropType, ScalarType};
use serde::{Deserialize, Serialize};

use super::budget::Budget;
use super::requirements::Requirements;
use super::{AcceptInput, ValidationError};
use crate::logical::{ColumnRef, IDENTITY_MEMBER, LogicalNode, LogicalPlan};
use crate::mirror::{ExprMirror, OrderingMirror, ProjectionMirror};
use crate::optimizer::ROW_ADDR;
use crate::physical::{
    Eligibility, HydratedColumn, PhysicalNode, PhysicalPlan, RankKind, RankScope, ScanInput,
};
use crate::source::FullTextCoverage;

/// A member of the exact fragment: its one binding and node type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Member {
    pub binding: String,
    pub type_name: String,
}

/// The scalar types the fragment's comparisons and keys admit.
fn admitted(property: &PropType) -> bool {
    !property.list && admitted_scalar(property.scalar)
}

fn admitted_scalar(scalar: ScalarType) -> bool {
    matches!(
        scalar,
        ScalarType::Bool
            | ScalarType::I32
            | ScalarType::I64
            | ScalarType::U32
            | ScalarType::U64
            | ScalarType::String
    )
}

/// The scalar of a non-list value type the fragment admits.
fn admitted_type(ty: &ExprType) -> Option<ScalarType> {
    match ty {
        ExprType::Value {
            scalar,
            list: false,
            ..
        } if admitted_scalar(*scalar) => Some(*scalar),
        _ => None,
    }
}

/// An operand's type as the fragment compares it. The lowering makes both
/// sides of a comparison one type (an explicit cast of a constant where
/// they differ), so every operand compares only with its own scalar type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operand {
    Scalar(ScalarType),
    Null,
}

impl Operand {
    fn integer(self) -> bool {
        matches!(
            self,
            Self::Scalar(ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64)
        )
    }

    fn string(self) -> bool {
        self == Self::Scalar(ScalarType::String)
    }

    fn same_type(self, other: Self) -> bool {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => true,
            (left, right) => left == right,
        }
    }
}

struct Fragment<'a> {
    binding: &'a str,
    node_type: &'a NodeType,
}

impl Fragment<'_> {
    fn property(&self, variable: &str, property: &str) -> Option<ScalarType> {
        if variable != self.binding {
            return None;
        }
        let declared = self.node_type.properties.get(property)?;
        admitted(declared).then_some(declared.scalar)
    }

    fn operand(&self, expr: &IRExpr) -> Option<Operand> {
        match expr {
            IRExpr::PropAccess {
                variable, property, ..
            } => self.property(variable, property).map(Operand::Scalar),
            IRExpr::Literal(Literal::Null, _) => Some(Operand::Null),
            IRExpr::Literal(Literal::Bool(_) | Literal::String(_) | Literal::Integer(_), ty)
            | IRExpr::Param(_, ty) => admitted_type(ty).map(Operand::Scalar),
            // A constant cast to an admitted type: the scan translates the
            // cast as the in-memory filter evaluates it.
            IRExpr::Cast { expr, ty }
                if matches!(
                    expr.as_ref(),
                    IRExpr::Literal(Literal::Integer(_), _) | IRExpr::Param(_, _)
                ) =>
            {
                admitted_type(ty).map(Operand::Scalar)
            }
            _ => None,
        }
    }

    /// Whether `expr` is an admitted eligibility predicate.
    fn predicate(&self, expr: &IRExpr) -> bool {
        match expr {
            IRExpr::Binary {
                left,
                op: BinaryOp::And | BinaryOp::Or,
                right,
                ..
            } => self.predicate(left) && self.predicate(right),
            IRExpr::Not(inner, _) => self.predicate(inner),
            IRExpr::IsNull { expr, .. } => self.operand(expr).is_some(),
            IRExpr::Binary {
                left,
                op: BinaryOp::Compare(op),
                right,
                ..
            } => {
                let (Some(left), Some(right)) = (self.operand(left), self.operand(right)) else {
                    return false;
                };
                left.same_type(right)
                    && match op {
                        CompOp::Eq | CompOp::Ne => true,
                        CompOp::Lt | CompOp::Le | CompOp::Gt | CompOp::Ge => {
                            (left.integer() || left.string() || left == Operand::Null)
                                && (right.integer() || right.string() || right == Operand::Null)
                        }
                        CompOp::Contains | CompOp::StartsWith | CompOp::StringContains => false,
                    }
            }
            other => self.operand(other) == Some(Operand::Scalar(ScalarType::Bool)),
        }
    }

    /// Whether `expr` is the leading `bm25()` the fragment admits: over a
    /// String property of the binding, with a String literal or parameter.
    fn bm25(&self, expr: &IRExpr) -> bool {
        let IRExpr::Bm25 { field, query, .. } = expr else {
            return false;
        };
        let IRExpr::PropAccess {
            variable, property, ..
        } = field.as_ref()
        else {
            return false;
        };
        self.property(variable, property) == Some(ScalarType::String)
            && self.operand(query) == Some(Operand::Scalar(ScalarType::String))
    }
}

/// The member the typed lowering of `input` is, or `None` for a query
/// outside the fragment.
pub(crate) fn membership(input: &AcceptInput<'_>) -> Option<Member> {
    let ir = input.ir;
    let mut pipeline = ir.pipeline.iter();
    let Some(IROp::NodeScan {
        variable,
        type_name,
        filters,
    }) = pipeline.next()
    else {
        return None;
    };
    let node_type = input.catalog.node_types.get(type_name)?;
    let fragment = Fragment {
        binding: variable,
        node_type,
    };
    if !filters.iter().all(|filter| fragment.predicate(filter)) {
        return None;
    }
    for op in pipeline {
        match op {
            IROp::Filter(filter) if fragment.predicate(filter) => {}
            _ => return None,
        }
    }
    let mut order = ir.order_by.iter();
    let leading = ir.order_by.first().map(|key| &key.expr);
    let ranked = leading.is_some_and(|lead| fragment.bm25(lead));
    if ranked {
        order.next();
    }
    for key in order {
        let IRExpr::PropAccess {
            variable, property, ..
        } = &key.expr
        else {
            return None;
        };
        fragment.property(variable, property)?;
    }
    // The lowering makes a projected `bm25()` its score column; the
    // declaration says which call it was.
    let decl = input.checked.decl();
    let written_lead = decl.order_clause.first().map(|key| &key.expr);
    for (index, projection) in ir.return_exprs.iter().enumerate() {
        let direct = match &projection.expr {
            IRExpr::PropAccess {
                variable, property, ..
            } if fragment.property(variable, property).is_some() => true,
            _ => {
                ranked
                    && decl
                        .return_clause
                        .get(index)
                        .is_some_and(|written| Some(&written.expr) == written_lead)
            }
        };
        if !direct {
            return None;
        }
    }
    Some(Member {
        binding: variable.clone(),
        type_name: type_name.clone(),
    })
}

/// The ranking a scan of the chain carries: `bm25` over `property` with
/// `query`, placing eligibility as declared.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainRanking {
    pub property: String,
    pub query: ExprMirror,
    pub eligibility: Eligibility,
}

/// One node of a member's chain, logical until a lowering rule marks it
/// physical (`lowered`). A filter with no conjunct is no node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum ChainNode {
    Scan {
        type_key: String,
        version: Option<u64>,
        binding: String,
        filter: Vec<ExprMirror>,
        projection: Option<Vec<String>>,
        ranked: Option<ChainRanking>,
        lowered: bool,
    },
    Filter {
        conjuncts: Vec<ExprMirror>,
        lowered: bool,
    },
    Search {
        property: String,
        query: ExprMirror,
    },
    Projection {
        exprs: Vec<ProjectionMirror>,
        lowered: bool,
    },
    Sort {
        keys: Vec<OrderingMirror>,
        fetch: Option<usize>,
        tiebreak: Vec<ColumnRef>,
        lowered: bool,
    },
    Limit {
        rows: usize,
        lowered: bool,
    },
    /// Return columns read by row address above the limit; physical from
    /// its creation.
    Hydrate {
        binding: String,
        type_key: String,
        columns: Vec<HydratedColumn>,
    },
}

/// A chain node's place, leaf first; a chain holds at most one of each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Role {
    Scan,
    Filter,
    Search,
    Projection,
    Sort,
    Limit,
    Hydrate,
}

impl ChainNode {
    pub(crate) fn role(&self) -> Role {
        match self {
            Self::Scan { .. } => Role::Scan,
            Self::Filter { .. } => Role::Filter,
            Self::Search { .. } => Role::Search,
            Self::Projection { .. } => Role::Projection,
            Self::Sort { .. } => Role::Sort,
            Self::Limit { .. } => Role::Limit,
            Self::Hydrate { .. } => Role::Hydrate,
        }
    }

    fn lowered(&self) -> bool {
        match self {
            Self::Scan { lowered, .. }
            | Self::Filter { lowered, .. }
            | Self::Projection { lowered, .. }
            | Self::Sort { lowered, .. }
            | Self::Limit { lowered, .. } => *lowered,
            Self::Hydrate { .. } => true,
            Self::Search { .. } => false,
        }
    }

    fn lower(&mut self) {
        match self {
            Self::Scan { lowered, .. }
            | Self::Filter { lowered, .. }
            | Self::Projection { lowered, .. }
            | Self::Sort { lowered, .. }
            | Self::Limit { lowered, .. } => *lowered = true,
            Self::Search { .. } | Self::Hydrate { .. } => {}
        }
    }
}

/// One rule of the closed catalogue, with its typed substitution. Each
/// application names the arena nodes it rewrites (`at`) and appends one
/// successor per named node, a removed node as an empty slot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum Rule {
    /// `Filter(p ∧ c, Scan(C, f))` → `Filter(p, Scan(C, f ∧ c))`, `at`
    /// `[filter, scan]`; `c` must be one of the filter's conjuncts and
    /// within the fragment's predicate forms, which the scan evaluates with
    /// the same typed semantics.
    AbsorbScanFilter { conjunct: ExprMirror },
    /// The scan reads `columns`, which must hold every column the nodes
    /// above it read and only columns of its table; `at` `[scan]`.
    PruneScanColumns { columns: Vec<String> },
    /// A logical node becomes its physical operator unchanged; `at` `[node]`.
    Lower,
    /// `Search(a, …Scan(C))` → a ranked physical scan of `C` by `a`,
    /// placing eligibility before scoring only under recorded full
    /// full-text coverage; `at` `[search, scan]`.
    RankBm25Scan { eligibility: Eligibility },
    /// The physical `Limit(n > 0, …Projection(V, …Scan(C)))` reads each of
    /// `columns`, a bare property of the binding that `V` returns at its
    /// position and no other node reads, by row address above the limit:
    /// the scan reads `_rowaddr` in their place, and a hydration over the
    /// limit places them; `at` `[scan]`, successors the scan and the
    /// hydration.
    HydrateReturnColumns { columns: Vec<HydratedColumn> },
    /// The physical scan also tests `conjunct`, the canonical identity of a
    /// written equality on its type's single String key, which the
    /// validator re-derives; `at` `[scan]`.
    NarrowKey { conjunct: ExprMirror },
}

/// One recorded rule application.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Step {
    pub at: Vec<usize>,
    #[serde(flatten)]
    pub rule: Rule,
}

/// The optimizer's record of how it rewrote a member's canonical chain:
/// rule applications over arena references. The arena starts with the
/// canonical chain, leaf first, and every application appends its
/// successors.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Derivation {
    pub steps: Vec<Step>,
}

/// The canonical chain's roles for a plan shape: the optimizer and the
/// validator number the arena the same way.
pub(crate) fn canonical_roles(filter: bool, search: bool, sort: bool, limit: bool) -> Vec<Role> {
    let mut roles = vec![Role::Scan];
    if filter {
        roles.push(Role::Filter);
    }
    if search {
        roles.push(Role::Search);
    }
    roles.push(Role::Projection);
    if sort {
        roles.push(Role::Sort);
    }
    if limit {
        roles.push(Role::Limit);
    }
    roles
}

/// The optimizer's side of a derivation: it numbers the arena as the
/// validator does and records each rule it applies. Inactive (the default)
/// on every plan that is no single-binding chain.
#[derive(Debug, Default)]
pub(crate) struct Tracer {
    derivation: Option<Derivation>,
    current: std::collections::HashMap<Role, usize>,
    next: usize,
    /// Conjuncts the chain's filter still holds; absorbing the last one
    /// removes the filter.
    conjuncts: usize,
}

impl Tracer {
    /// Start recording over the canonical chain of `plan`, a resolved
    /// logical plan, when it is one binding's chain:
    /// `[Limit] [Sort] Projection [TextSearch] Filter* TableScan`.
    pub(crate) fn for_plan(plan: &LogicalPlan) -> Self {
        let mut id = plan.root();
        let mut limit = false;
        let mut sort = false;
        let mut search = false;
        let mut conjuncts: Vec<&IRExpr> = Vec::new();
        let mut projection = false;
        loop {
            match plan.node(id) {
                Some(LogicalNode::Limit { input, .. }) if !sort && !projection && !limit => {
                    limit = true;
                    id = *input;
                }
                Some(LogicalNode::Sort { input, .. }) if !projection && !sort => {
                    sort = true;
                    id = *input;
                }
                Some(LogicalNode::Projection { input, .. }) if !projection => {
                    projection = true;
                    id = *input;
                }
                Some(LogicalNode::TextSearch { input, .. }) if projection && !search => {
                    search = true;
                    id = *input;
                }
                Some(LogicalNode::Filter {
                    input,
                    conjuncts: held,
                }) if projection => {
                    for conjunct in held {
                        if !conjuncts.contains(&conjunct) {
                            conjuncts.push(conjunct);
                        }
                    }
                    id = *input;
                }
                Some(LogicalNode::TableScan { input: None, spec })
                    if projection && spec.binding.is_some() =>
                {
                    break;
                }
                _ => return Self::default(),
            }
        }
        let roles = canonical_roles(!conjuncts.is_empty(), search, sort, limit);
        Self {
            derivation: Some(Derivation::default()),
            current: roles
                .iter()
                .enumerate()
                .map(|(index, role)| (*role, index))
                .collect(),
            next: roles.len(),
            conjuncts: conjuncts.len(),
        }
    }

    /// Record the placement pass moving `conjunct` from the chain's filter
    /// into its scan.
    pub(crate) fn absorb(&mut self, conjunct: &IRExpr) {
        self.conjuncts = self.conjuncts.saturating_sub(1);
        let removed: &[Role] = if self.conjuncts == 0 {
            &[Role::Filter]
        } else {
            &[]
        };
        self.record(
            &[Role::Filter, Role::Scan],
            Rule::AbsorbScanFilter {
                conjunct: mirror(conjunct),
            },
            removed,
        );
    }

    /// Record `rule` applied to the current nodes of `roles`; a role in
    /// `removed` has no successor.
    pub(crate) fn record(&mut self, roles: &[Role], rule: Rule, removed: &[Role]) {
        let Some(derivation) = self.derivation.as_mut() else {
            return;
        };
        let mut at = Vec::with_capacity(roles.len());
        for role in roles {
            let Some(index) = self.current.get(role).copied() else {
                self.derivation = None;
                return;
            };
            at.push(index);
        }
        for role in roles {
            if removed.contains(role) {
                self.current.remove(role);
            } else {
                self.current.insert(*role, self.next);
            }
            self.next += 1;
        }
        derivation.steps.push(Step { at, rule });
    }

    /// Record `rule` applied to the current nodes of `at`, whose successors
    /// are the current nodes of `successors` (a role it creates included).
    pub(crate) fn record_with(&mut self, at: &[Role], successors: &[Role], rule: Rule) {
        let Some(derivation) = self.derivation.as_mut() else {
            return;
        };
        let mut indices = Vec::with_capacity(at.len());
        for role in at {
            let Some(index) = self.current.get(role).copied() else {
                self.derivation = None;
                return;
            };
            indices.push(index);
        }
        for role in successors {
            self.current.insert(*role, self.next);
            self.next += 1;
        }
        derivation.steps.push(Step { at: indices, rule });
    }

    /// The derivation recorded so far.
    #[cfg(test)]
    pub(crate) fn derivation(&self) -> Option<Derivation> {
        self.derivation.clone()
    }

    pub(crate) fn finish(self) -> Option<Derivation> {
        self.derivation
    }
}

/// The checker's arena: every node the derivation reached, `None` where a
/// rule removed or replaced one. Only the current node of each role is kept,
/// so the arena's expressions partition the query's own and its memory is
/// linear in the query; a replaced node is released before its successor is
/// stored, and a rule naming it is refused like any other stale reference.
struct Arena {
    nodes: Vec<Option<ChainNode>>,
    current: std::collections::HashMap<Role, usize>,
}

impl Arena {
    fn node(&self, index: usize, role: Role) -> Result<&ChainNode, ValidationError> {
        let node = self.nodes.get(index).and_then(Option::as_ref);
        match node {
            Some(node) if node.role() == role && self.current.get(&role) == Some(&index) => {
                Ok(node)
            }
            _ => Err(invalid(format!(
                "a rule names node {index} as the current {role:?}, which it is not"
            ))),
        }
    }
}

fn invalid(detail: String) -> ValidationError {
    ValidationError::violated("exact subset", detail)
}

fn mirror(expr: &IRExpr) -> ExprMirror {
    ExprMirror::from(expr)
}

/// The physical scan's table and pinned version recorded for `type_key`.
fn pinned(plan: &PhysicalPlan, type_key: &str) -> Option<Option<u64>> {
    plan.assumptions()
        .datasets
        .get(type_key)
        .map(|pin| pin.as_ref().map(|pin| pin.version))
}

#[cfg(test)]
thread_local! {
    /// The nodes the last checked derivation's arena still held at its end.
    pub(super) static RETAINED_NODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Check `derivation` for `member` against the candidate `plan`.
pub(crate) fn check(
    derivation: &Derivation,
    plan: &PhysicalPlan,
    input: &AcceptInput<'_>,
    requirements: &Requirements,
    member: &Member,
    budget: &mut Budget,
) -> Result<(), ValidationError> {
    let _ = requirements;
    let ir = input.ir;
    let mut arena = canonical(ir, plan, member, input)?;
    for step in &derivation.steps {
        budget.step()?;
        apply(&mut arena, step, plan, member, input, budget)?;
    }
    let reached: Vec<&ChainNode> = [
        Role::Hydrate,
        Role::Limit,
        Role::Sort,
        Role::Projection,
        Role::Search,
        Role::Filter,
        Role::Scan,
    ]
    .iter()
    .filter_map(|role| arena.current.get(role))
    .filter_map(|index| arena.nodes[*index].as_ref())
    .filter(|node| !matches!(node, ChainNode::Filter { conjuncts, .. } if conjuncts.is_empty()))
    .collect();
    if let Some(node) = reached.iter().find(|node| !node.lowered()) {
        return Err(invalid(format!(
            "the derivation leaves the {:?} logical",
            node.role()
        )));
    }
    #[cfg(test)]
    RETAINED_NODES.with(|retained| retained.set(arena.nodes.iter().flatten().count()));
    let candidate = chain_of(plan, input)?;
    if reached.len() != candidate.len()
        || reached
            .iter()
            .zip(&candidate)
            .any(|(left, right)| *left != right)
    {
        return Err(invalid(format!(
            "the derivation reconstructs {reached:?}; the plan is {candidate:?}"
        )));
    }
    Ok(())
}

/// The canonical chain of a member, from its checked IR: the scan of the
/// pinned table, every conjunct in written order (a repeated one once), the
/// leading `bm25()`, the projection, the sort of the remaining keys with the
/// identity tie-break the comparator requires, and the limit.
fn canonical(
    ir: &QueryIR,
    plan: &PhysicalPlan,
    member: &Member,
    input: &AcceptInput<'_>,
) -> Result<Arena, ValidationError> {
    let type_key = format!("node:{}", member.type_name);
    let version = pinned(plan, &type_key)
        .ok_or_else(|| invalid(format!("the plan records no pin of `{type_key}`")))?;
    let mut conjuncts: Vec<IRExpr> = Vec::new();
    for op in &ir.pipeline {
        let filters: Vec<IRExpr> = match op {
            IROp::NodeScan { filters, .. } => filters.clone(),
            IROp::Filter(filter) => vec![filter.clone()],
            _ => Vec::new(),
        };
        for conjunct in filters.into_iter().flat_map(IRExpr::into_conjuncts) {
            if !conjuncts.contains(&conjunct) {
                conjuncts.push(conjunct);
            }
        }
    }
    let mut keys: &[IROrdering] = &ir.order_by;
    let search = match keys.first().map(|key| &key.expr) {
        Some(IRExpr::Bm25 { field, query, .. }) => match field.as_ref() {
            IRExpr::PropAccess { property, .. } => {
                keys = &keys[1..];
                Some(ChainNode::Search {
                    property: property.clone(),
                    query: mirror(query),
                })
            }
            _ => None,
        },
        _ => None,
    };
    let sorted = search.is_some() || !ir.order_by.is_empty();
    let mut nodes = vec![ChainNode::Scan {
        type_key,
        version,
        binding: member.binding.clone(),
        filter: Vec::new(),
        projection: None,
        ranked: None,
        lowered: false,
    }];
    if !conjuncts.is_empty() {
        nodes.push(ChainNode::Filter {
            conjuncts: conjuncts.iter().map(mirror).collect(),
            lowered: false,
        });
    }
    nodes.extend(search);
    nodes.push(ChainNode::Projection {
        exprs: ir.return_exprs.iter().map(ProjectionMirror::from).collect(),
        lowered: false,
    });
    if sorted {
        let written: Vec<&IROrdering> = keys
            .iter()
            .filter(|key| !matches!(key.expr, IRExpr::Literal(_, _)))
            .collect();
        nodes.push(ChainNode::Sort {
            keys: written
                .iter()
                .map(|key| OrderingMirror::from(*key))
                .collect(),
            fetch: ir.limit.and_then(|limit| usize::try_from(limit).ok()),
            tiebreak: canonical_tiebreak(ir, member, input),
            lowered: false,
        });
    }
    if let Some(limit) = ir.limit {
        nodes.push(ChainNode::Limit {
            rows: usize::try_from(limit).unwrap_or(usize::MAX),
            lowered: false,
        });
    }
    Ok(Arena {
        current: nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.role(), index))
            .collect(),
        nodes: nodes.into_iter().map(Some).collect(),
    })
}

/// The identity tie-break of the canonical sort: the binding's id unless
/// every returned expression is an order key (rows that tie are then
/// indistinguishable) or the id is a key itself.
fn canonical_tiebreak(ir: &QueryIR, member: &Member, input: &AcceptInput<'_>) -> Vec<ColumnRef> {
    let keys: Vec<String> = ir.order_by.iter().map(|key| key.expr.to_string()).collect();
    let covered = ir
        .return_exprs
        .iter()
        .all(|projection| keys.contains(&projection.expr.to_string()));
    let id = input.catalog.system_columns.id;
    let keyed = ir.order_by.iter().any(|key| {
        matches!(&key.expr, IRExpr::PropAccess { variable, property, .. }
            if *variable == member.binding && property == id)
    });
    if covered || keyed {
        Vec::new()
    } else {
        vec![ColumnRef::property(&member.binding, IDENTITY_MEMBER)]
    }
}

/// The columns the nodes above the scan read: returned and sorted
/// properties, residual conjuncts, and the id when a tie-break reads it.
fn demanded(arena: &Arena, member: &Member, input: &AcceptInput<'_>) -> BTreeSet<String> {
    let mut reads = Vec::new();
    for (role, index) in &arena.current {
        let Some(node) = arena.nodes[*index].as_ref() else {
            continue;
        };
        match (role, node) {
            (Role::Filter, ChainNode::Filter { conjuncts, .. }) => {
                for conjunct in conjuncts {
                    crate::optimizer::reads_of_expr(&IRExpr::from(conjunct.clone()), &mut reads);
                }
            }
            (Role::Projection, ChainNode::Projection { exprs, .. }) => {
                for projection in exprs {
                    let projection = IRProjection::from(projection.clone());
                    crate::optimizer::reads_of_expr(&projection.expr, &mut reads);
                }
            }
            (Role::Sort, ChainNode::Sort { keys, tiebreak, .. }) => {
                for key in keys {
                    let key = IROrdering::from(key.clone());
                    crate::optimizer::reads_of_expr(&key.expr, &mut reads);
                }
                if !tiebreak.is_empty() {
                    reads.push(ColumnRef::property(&member.binding, IDENTITY_MEMBER));
                }
            }
            _ => {}
        }
    }
    let id = input.catalog.system_columns.id;
    reads
        .into_iter()
        .filter(|read| read.binding == member.binding)
        .filter_map(|read| match read.property.as_deref() {
            Some(IDENTITY_MEMBER) => Some(id.to_string()),
            Some(property) => Some(property.to_string()),
            None => None,
        })
        .collect()
}

/// Apply one recorded rule: check its references and precondition, then
/// reconstruct its successors.
fn apply(
    arena: &mut Arena,
    step: &Step,
    plan: &PhysicalPlan,
    member: &Member,
    input: &AcceptInput<'_>,
    budget: &mut Budget,
) -> Result<(), ValidationError> {
    let successors: Vec<(Role, Option<ChainNode>)> = match (&step.rule, step.at.as_slice()) {
        (Rule::AbsorbScanFilter { conjunct }, [filter, scan]) => {
            let ChainNode::Filter { conjuncts, lowered } =
                arena.node(*filter, Role::Filter)?.clone()
            else {
                unreachable!("the arena checked the role");
            };
            let ChainNode::Scan {
                type_key,
                version,
                binding,
                mut filter,
                projection,
                ranked,
                lowered: scan_lowered,
            } = arena.node(*scan, Role::Scan)?.clone()
            else {
                unreachable!("the arena checked the role");
            };
            if lowered || scan_lowered || ranked.is_some() {
                return Err(invalid(
                    "a filter is absorbed only between logical nodes".into(),
                ));
            }
            budget.visit(u64::try_from(conjuncts.len() + filter.len()).unwrap_or(u64::MAX))?;
            let Some(position) = conjuncts.iter().position(|held| held == conjunct) else {
                return Err(invalid(format!(
                    "the filter holds no conjunct `{}` to absorb",
                    IRExpr::from(conjunct.clone())
                )));
            };
            if !translatable(&IRExpr::from(conjunct.clone()), member, input) {
                return Err(invalid(format!(
                    "`{}` has no admitted scan translation",
                    IRExpr::from(conjunct.clone())
                )));
            }
            let mut rest = conjuncts;
            let moved = rest.remove(position);
            filter.push(moved);
            let filter_node = (!rest.is_empty()).then_some(ChainNode::Filter {
                conjuncts: rest,
                lowered: false,
            });
            vec![
                (Role::Filter, filter_node),
                (
                    Role::Scan,
                    Some(ChainNode::Scan {
                        type_key,
                        version,
                        binding,
                        filter,
                        projection,
                        ranked,
                        lowered: false,
                    }),
                ),
            ]
        }
        (Rule::PruneScanColumns { columns }, [scan]) => {
            let mut node = arena.node(*scan, Role::Scan)?.clone();
            let schema = &input
                .catalog
                .node_types
                .get(&member.type_name)
                .ok_or_else(|| invalid("the member's type left the catalog".into()))?
                .arrow_schema;
            budget.visit(u64::try_from(columns.len()).unwrap_or(u64::MAX))?;
            if let Some(unknown) = columns
                .iter()
                .find(|column| schema.field_with_name(column).is_err())
            {
                return Err(invalid(format!("the scan cannot read `{unknown}`")));
            }
            let held: BTreeSet<&String> = columns.iter().collect();
            if let Some(missing) = demanded(arena, member, input)
                .into_iter()
                .find(|column| schema.field_with_name(column).is_ok() && !held.contains(column))
            {
                return Err(invalid(format!(
                    "the pruned scan drops `{missing}`, which a node above it reads"
                )));
            }
            if let ChainNode::Scan { projection, .. } = &mut node {
                *projection = Some(columns.clone());
            }
            vec![(Role::Scan, Some(node))]
        }
        (Rule::Lower, [at]) => {
            let role = arena
                .nodes
                .get(*at)
                .and_then(Option::as_ref)
                .map(ChainNode::role)
                .ok_or_else(|| invalid(format!("no node {at} to lower")))?;
            let mut node = arena.node(*at, role)?.clone();
            match (&mut node, role) {
                (ChainNode::Search { .. }, _) => {
                    return Err(invalid("a search lowers only into its ranked scan".into()));
                }
                (ChainNode::Sort { keys, .. }, Role::Sort) => {
                    if arena.current.contains_key(&Role::Search) {
                        return Err(invalid("the sort lowers after its ranking".into()));
                    }
                    let ranked = arena
                        .current
                        .get(&Role::Scan)
                        .and_then(|index| arena.nodes[*index].as_ref())
                        .and_then(|scan| match scan {
                            ChainNode::Scan { ranked, .. } => ranked.clone(),
                            _ => None,
                        });
                    if ranked.is_some() {
                        keys.insert(
                            0,
                            OrderingMirror::from(&IROrdering {
                                expr: crate::optimizer::rank_score(&member.binding, RankKind::Bm25),
                                descending: RankKind::Bm25.score().1,
                            }),
                        );
                    }
                }
                _ => {}
            }
            if node.lowered() {
                return Err(invalid(format!("the {role:?} is lowered twice")));
            }
            node.lower();
            vec![(role, Some(node))]
        }
        (Rule::RankBm25Scan { eligibility }, [search, scan]) => {
            let ChainNode::Search { property, query } = arena.node(*search, Role::Search)?.clone()
            else {
                unreachable!("the arena checked the role");
            };
            let mut node = arena.node(*scan, Role::Scan)?.clone();
            let ChainNode::Scan {
                type_key,
                ranked,
                lowered,
                ..
            } = &mut node
            else {
                unreachable!("the arena checked the role");
            };
            if !*lowered || ranked.is_some() {
                return Err(invalid("only a physical, unranked scan is ranked".into()));
            }
            let coverage =
                plan.assumptions()
                    .full_text
                    .get(&crate::physical::Assumptions::full_text_key(
                        type_key, &property,
                    ));
            if *eligibility == Eligibility::BeforeScoring
                && coverage != Some(&FullTextCoverage::Full)
            {
                return Err(invalid(format!(
                    "the bm25 scan filters before scoring under recorded coverage {coverage:?}"
                )));
            }
            *ranked = Some(ChainRanking {
                property,
                query,
                eligibility: *eligibility,
            });
            vec![(Role::Search, None), (Role::Scan, Some(node))]
        }
        (Rule::HydrateReturnColumns { columns }, [scan]) => {
            let mut node = arena.node(*scan, Role::Scan)?.clone();
            let ChainNode::Scan {
                type_key,
                filter,
                projection: Some(read),
                ranked: None,
                lowered: true,
                ..
            } = &mut node
            else {
                return Err(invalid(
                    "only a physical, unranked scan with a projection defers columns".into(),
                ));
            };
            let current = |role: Role| {
                arena
                    .current
                    .get(&role)
                    .and_then(|index| arena.nodes[*index].as_ref())
            };
            if current(Role::Hydrate).is_some()
                || !matches!(current(Role::Limit), Some(ChainNode::Limit { rows, lowered: true }) if *rows > 0)
            {
                return Err(invalid(
                    "return columns are hydrated once, above a nonzero physical limit".into(),
                ));
            }
            let Some(ChainNode::Projection {
                exprs,
                lowered: true,
            }) = current(Role::Projection)
            else {
                return Err(invalid(
                    "return columns are hydrated over a physical projection".into(),
                ));
            };
            let returns: Vec<IRProjection> =
                exprs.iter().cloned().map(IRProjection::from).collect();
            let node_type = input
                .catalog
                .node_types
                .get(&member.type_name)
                .ok_or_else(|| invalid("the member's type left the catalog".into()))?;
            budget.visit(u64::try_from(columns.len() + returns.len()).unwrap_or(u64::MAX))?;
            // Every read below the hydration that must still come from the
            // scan: filters, sort keys, and every return item not deferred.
            let mut reads = Vec::new();
            for conjunct in filter.iter() {
                crate::optimizer::reads_of_expr(&IRExpr::from(conjunct.clone()), &mut reads);
            }
            if let Some(ChainNode::Filter { conjuncts, .. }) = current(Role::Filter) {
                for conjunct in conjuncts {
                    crate::optimizer::reads_of_expr(&IRExpr::from(conjunct.clone()), &mut reads);
                }
            }
            if let Some(ChainNode::Sort { keys, .. }) = current(Role::Sort) {
                for key in keys {
                    crate::optimizer::reads_of_expr(
                        &IROrdering::from(key.clone()).expr,
                        &mut reads,
                    );
                }
            }
            for (position, item) in returns.iter().enumerate() {
                if !columns.iter().any(|column| column.position == position) {
                    crate::optimizer::reads_of_expr(&item.expr, &mut reads);
                }
            }
            for column in columns {
                let returned = returns.get(column.position).is_some_and(|item| {
                    item.column == column.output
                        && matches!(&item.expr, IRExpr::PropAccess { variable, property, .. }
                            if *variable == member.binding && *property == column.property)
                });
                let read_below = reads.iter().any(|read| {
                    read.binding == member.binding
                        && read.property.as_deref() == Some(column.property.as_str())
                });
                if !returned
                    || read_below
                    || !read.contains(&column.property)
                    || node_type
                        .key
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .any(|key| *key == column.property)
                    || column.property == input.catalog.system_columns.id
                {
                    return Err(invalid(format!(
                        "`${}.{}` at return position {} cannot be read by row address",
                        member.binding, column.property, column.position
                    )));
                }
            }
            read.retain(|name| !columns.iter().any(|column| column.property == *name));
            if !read.iter().any(|name| name == ROW_ADDR) {
                read.push(ROW_ADDR.to_string());
            }
            let hydrate = ChainNode::Hydrate {
                binding: member.binding.clone(),
                type_key: type_key.clone(),
                columns: columns.clone(),
            };
            vec![(Role::Scan, Some(node)), (Role::Hydrate, Some(hydrate))]
        }
        (Rule::NarrowKey { conjunct }, [scan]) => {
            let mut node = arena.node(*scan, Role::Scan)?.clone();
            let ChainNode::Scan {
                type_key,
                binding,
                filter,
                lowered: true,
                ..
            } = &mut node
            else {
                return Err(invalid("only a physical scan narrows its key".into()));
            };
            let derived = IRExpr::from(conjunct.clone());
            let written: Vec<IRExpr> = filter.iter().cloned().map(IRExpr::from).collect();
            budget.visit(u64::try_from(written.len()).unwrap_or(u64::MAX))?;
            if !super::requirements::narrowed_identity(input, type_key, binding, &written, &derived)
            {
                return Err(invalid(format!(
                    "`{derived}` is no canonical identity of a written key equality"
                )));
            }
            filter.push(conjunct.clone());
            vec![(Role::Scan, Some(node))]
        }
        (rule, at) => {
            return Err(invalid(format!("rule {rule:?} names {} nodes", at.len())));
        }
    };
    for (role, successor) in successors {
        budget.node()?;
        if let Some(replaced) = arena.current.get(&role).copied() {
            arena.nodes[replaced] = None;
        }
        let index = arena.nodes.len();
        match successor {
            Some(node) => {
                arena.nodes.push(Some(node));
                arena.current.insert(role, index);
            }
            None => {
                arena.nodes.push(None);
                arena.current.remove(&role);
            }
        }
    }
    Ok(())
}

/// Whether the scan evaluates `conjunct` with the same typed semantics as
/// the in-memory filter: the fragment's predicate forms over the member's
/// properties, literals and bound parameters, a constant cast included (the
/// scan translates a cast as the filter evaluates it).
fn translatable(conjunct: &IRExpr, member: &Member, input: &AcceptInput<'_>) -> bool {
    let Some(node_type) = input.catalog.node_types.get(&member.type_name) else {
        return false;
    };
    fn operand(expr: &IRExpr, member: &Member, node_type: &NodeType) -> bool {
        match expr {
            IRExpr::PropAccess {
                variable, property, ..
            } => {
                *variable == member.binding
                    && node_type.properties.get(property).is_some_and(admitted)
            }
            IRExpr::Literal(
                Literal::Bool(_) | Literal::String(_) | Literal::Integer(_) | Literal::Null,
                _,
            )
            | IRExpr::Param(_, _) => true,
            IRExpr::Cast { expr, ty } => {
                admitted_type(ty).is_some()
                    && matches!(
                        expr.as_ref(),
                        IRExpr::Literal(Literal::Integer(_), _) | IRExpr::Param(_, _)
                    )
            }
            _ => false,
        }
    }
    match conjunct {
        IRExpr::Binary {
            left,
            op: BinaryOp::And | BinaryOp::Or,
            right,
            ..
        } => translatable(left, member, input) && translatable(right, member, input),
        IRExpr::Not(inner, _) => translatable(inner, member, input),
        IRExpr::IsNull { expr, .. } => operand(expr, member, node_type),
        IRExpr::Binary {
            left,
            op:
                BinaryOp::Compare(
                    CompOp::Eq | CompOp::Ne | CompOp::Lt | CompOp::Le | CompOp::Gt | CompOp::Ge,
                ),
            right,
            ..
        } => operand(left, member, node_type) && operand(right, member, node_type),
        other => operand(other, member, node_type),
    }
}

/// The candidate plan as a chain, root first, or why it is none.
fn chain_of(
    plan: &PhysicalPlan,
    input: &AcceptInput<'_>,
) -> Result<Vec<ChainNode>, ValidationError> {
    let mut chain = Vec::new();
    let mut id = plan.root();
    let mut hydrated_table = None;
    loop {
        let node = plan
            .node(id)
            .ok_or_else(|| invalid(format!("node {id} is a tombstone")))?;
        let (converted, next) = match node {
            PhysicalNode::HydrateColumns { input, bindings } if chain.is_empty() => {
                let [hydrated] = bindings.as_slice() else {
                    return Err(invalid("a chain hydrates one binding".into()));
                };
                hydrated_table = Some(&hydrated.table);
                (
                    ChainNode::Hydrate {
                        binding: hydrated.binding.clone(),
                        type_key: hydrated.table.type_key.clone(),
                        columns: hydrated.columns.clone(),
                    },
                    Some(*input),
                )
            }
            PhysicalNode::Limit { input, rows } => (
                ChainNode::Limit {
                    rows: *rows,
                    lowered: true,
                },
                Some(*input),
            ),
            PhysicalNode::Sort {
                input,
                order_by,
                fetch,
                tiebreak,
            } => (
                ChainNode::Sort {
                    keys: order_by.iter().map(OrderingMirror::from).collect(),
                    fetch: *fetch,
                    tiebreak: tiebreak.clone(),
                    lowered: true,
                },
                Some(*input),
            ),
            PhysicalNode::Projection {
                input,
                return_exprs,
                ..
            } => (
                ChainNode::Projection {
                    exprs: return_exprs.iter().map(ProjectionMirror::from).collect(),
                    lowered: true,
                },
                Some(*input),
            ),
            PhysicalNode::Filter { input, filters } => (
                ChainNode::Filter {
                    conjuncts: filters.iter().map(mirror).collect(),
                    lowered: true,
                },
                Some(*input),
            ),
            PhysicalNode::Scan {
                source: ScanInput::Table,
                spec,
                ordered: false,
                keys_only: false,
                ranked,
            } => {
                let same_table = match plan.assumptions().datasets.get(&spec.table.type_key) {
                    Some(Some(pin)) => {
                        pin.dataset_path == spec.table.dataset_path
                            && pin.native_branch == spec.table.native_branch
                    }
                    Some(None) => spec.version.is_none(),
                    None => false,
                };
                if !same_table
                    || hydrated_table.is_some_and(|table| *table != spec.table)
                    || spec.fragments.is_some()
                    || spec.runtime_filter.is_some()
                    || spec.columns != input.catalog.system_columns
                {
                    return Err(invalid(format!(
                        "the scan of `{}` reads another table image than the one pinned",
                        spec.table.type_key
                    )));
                }
                let ranked = match ranked {
                    None => None,
                    Some(access)
                        if access.kind == RankKind::Bm25
                            && access.fetch.is_none()
                            && access.nprobes.is_none()
                            && access.scope == RankScope::Order
                            && access.overfetch.is_empty()
                            && access.prefilter.is_none() =>
                    {
                        Some(ChainRanking {
                            property: access.property.clone(),
                            query: mirror(&access.query),
                            eligibility: access.eligibility,
                        })
                    }
                    Some(_) => {
                        return Err(invalid(
                            "the scan's ranking is no exact bm25 retrieval".into(),
                        ));
                    }
                };
                (
                    ChainNode::Scan {
                        type_key: spec.table.type_key.clone(),
                        version: spec.version,
                        binding: spec.binding.clone().unwrap_or_default(),
                        filter: spec
                            .filter
                            .as_ref()
                            .map(|filter| filter.gq_filters().iter().map(mirror).collect())
                            .unwrap_or_default(),
                        projection: spec.projection.clone(),
                        ranked,
                        lowered: true,
                    },
                    None,
                )
            }
            other => {
                return Err(invalid(format!(
                    "the plan holds a {} the fragment's chain does not",
                    other.name()
                )));
            }
        };
        chain.push(converted);
        match next {
            Some(next) => id = next,
            None => return Ok(chain),
        }
    }
}
