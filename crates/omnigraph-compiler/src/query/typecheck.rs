use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::catalog::schema_ir::{SYSTEM_COLUMNS_META, SystemFieldRole};
use crate::catalog::{Catalog, EdgeType};
use crate::error::{CompilerError, Result};
use crate::traversal::{EDGE_TYPE_META, EdgeMember, EdgeSelection, common_edge_property};
use crate::types::{
    AggSignature, Direction, ExprType, PropType, ScalarType, check_date_literal,
    check_datetime_literal,
};

use super::ast::*;
use super::codes::*;
use super::diagnostic::QueryDiagnostic;

/// A variable in the query's single namespace, tagged by what it binds.
///
/// Node and edge bindings share one symbol table (GQ has one variable
/// namespace) but live in *separate type namespaces* — a node type and an edge
/// type may share a name. A type name therefore only means something once the
/// kind is known, so the kind is the discriminant: every consumer must match,
/// and a new kind is a compile error at each site.
#[derive(Debug, Clone)]
pub enum BoundVariable {
    Node { type_name: String },
    Edge { type_names: Vec<String> },
}

impl BoundVariable {
    /// Node type name of a traversal endpoint, or T23 if `self` is an edge
    /// binding. The type name is only reachable through this check, so no
    /// caller can compare endpoint types without having ruled the edge case
    /// out. `var` names the variable for the error message only.
    fn require_traversal_endpoint(&self, var: &str) -> Result<&str> {
        match self {
            Self::Node { type_name } => Ok(type_name),
            Self::Edge { .. } => Err(CompilerError::typed(
                T23,
                format!(
                    "edge binding `${var}` cannot be used as a traversal endpoint; traversal endpoints must be node bindings"
                ),
            )),
        }
    }
}

/// The checker's scope while it walks one read query: the variables bound
/// so far and the return aliases an `order` key may name. It never leaves
/// the checker; the completed scope is a [`CheckedScope`].
#[derive(Debug, Clone)]
struct TypeContext {
    bindings: HashMap<String, BoundVariable>,
    aliases: HashMap<String, ResolvedType>,
}

impl TypeContext {
    /// No bindings, no aliases: the read context of a mutation scope, where
    /// every name resolves through the scope instead.
    fn empty() -> Self {
        Self {
            bindings: HashMap::new(),
            aliases: HashMap::new(),
        }
    }
}

/// A read query after type checking: every expression occurrence resolved
/// once and retained with its type, the match scope completed, the
/// parameters parsed. Only [`typecheck_query`] and [`typecheck_query_decl`]
/// construct one; lowering and result-schema inference read it.
#[derive(Debug, Clone)]
pub struct CheckedReadQuery {
    pub(crate) name: String,
    pub(crate) params: Vec<CheckedParam>,
    pub(crate) scope: CheckedScope,
    pub(crate) returns: Vec<CheckedReturn>,
    /// `return distinct`: one row per distinct projected tuple.
    pub(crate) distinct: bool,
    pub(crate) orders: Vec<CheckedOrder>,
    pub(crate) limit: Option<u64>,
}

impl CheckedReadQuery {
    /// The bindings visible at the end of the top-level match scope.
    pub fn bindings(&self) -> &HashMap<String, BoundVariable> {
        &self.scope.bindings
    }

    /// The traversals of the top-level match scope, in source order;
    /// a correlated block's traversals belong to its own scope.
    pub fn traversals(&self) -> impl Iterator<Item = &ResolvedTraversal> {
        self.scope.traversals()
    }
}

/// A declared parameter and the type selected for its name (the last
/// declaration of a repeated name wins, as the checker's lookup does).
#[derive(Debug, Clone)]
pub(crate) struct CheckedParam {
    pub(crate) declaration: Param,
    pub(crate) ty: PropType,
}

/// A completed match scope: the bindings visible at its end and its clauses
/// in source order, each carrying what the checker resolved for it.
#[derive(Debug, Clone)]
pub(crate) struct CheckedScope {
    pub(crate) bindings: HashMap<String, BoundVariable>,
    pub(crate) clauses: Vec<CheckedClause>,
}

impl CheckedScope {
    pub(crate) fn traversals(&self) -> impl Iterator<Item = &ResolvedTraversal> {
        self.clauses.iter().filter_map(|clause| match clause {
            CheckedClause::Traversal(traversal) => Some(traversal),
            CheckedClause::Binding(_) | CheckedClause::Filter(_) | CheckedClause::Block(_) => None,
        })
    }

    pub(crate) fn blocks(&self) -> impl Iterator<Item = &CheckedBlock> {
        self.clauses.iter().filter_map(|clause| match clause {
            CheckedClause::Block(block) => Some(block.as_ref()),
            CheckedClause::Binding(_) | CheckedClause::Traversal(_) | CheckedClause::Filter(_) => {
                None
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CheckedClause {
    Binding(CheckedBinding),
    Traversal(ResolvedTraversal),
    /// A Boolean expression with every bare search call among its top-level
    /// conjuncts spelled `call = true`, the one shape lowering emits.
    Filter(TypedExpr),
    Block(Box<CheckedBlock>),
}

#[derive(Debug, Clone)]
pub(crate) struct CheckedBinding {
    pub(crate) variable: String,
    pub(crate) type_name: String,
    pub(crate) matches: Vec<CheckedPropMatch>,
}

/// An inline binding match: the matched property's declared type (a list
/// property matches by membership) and the constant checked against it.
#[derive(Debug, Clone)]
pub(crate) struct CheckedPropMatch {
    pub(crate) property: String,
    pub(crate) prop_type: PropType,
    pub(crate) value: TypedExpr,
}

/// A correlated block: the outer bindings it may read, its completed inner
/// scope, and the comparison of its aggregate with `right`.
#[derive(Debug, Clone)]
pub(crate) struct CheckedBlock {
    pub(crate) outer_bindings: HashMap<String, BoundVariable>,
    pub(crate) inner: CheckedScope,
    pub(crate) aggregate: CheckedBlockAggregate,
    pub(crate) op: CompOp,
    pub(crate) right: TypedExpr,
}

/// The aggregate side of a block comparison: the row count (`not`,
/// `exists`, `count { … }`, `count($node) { … }`) or a column aggregate.
#[derive(Debug, Clone)]
pub(crate) enum CheckedBlockAggregate {
    RowCount,
    Aggregate {
        func: AggFunc,
        arg: TypedExpr,
        signature: AggSignature,
    },
}

/// A return item: its typed expression, its alias, and the two names the
/// result carries (`projection_name` for the inferred schema and
/// descriptors, `executed_column_name` for the executed batch), computed
/// once from the source spelling.
#[derive(Debug, Clone)]
pub(crate) struct CheckedReturn {
    pub(crate) expr: TypedExpr,
    pub(crate) alias: Option<String>,
    pub(crate) inferred_name: String,
    pub(crate) executed_column: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CheckedOrder {
    pub(crate) expr: TypedExpr,
    pub(crate) descending: bool,
}

/// A mutation query after type checking: its parameters parsed once and
/// each statement retained with its target namespace and typed constants.
#[derive(Debug, Clone)]
pub struct CheckedMutationQuery {
    pub(crate) name: String,
    pub(crate) params: Vec<CheckedParam>,
    pub(crate) ops: Vec<CheckedMutationOp>,
    pub(crate) write_mode: MutationWriteMode,
}

#[derive(Debug, Clone)]
pub(crate) enum CheckedMutationOp {
    Insert {
        target: MutationTarget,
        assignments: Vec<CheckedAssignment>,
    },
    Update {
        target: MutationTarget,
        assignments: Vec<CheckedAssignment>,
        predicate: TypedExpr,
    },
    Delete {
        target: MutationTarget,
        predicate: TypedExpr,
    },
}

impl CheckedMutationOp {
    pub(crate) fn target(&self) -> &MutationTarget {
        match self {
            Self::Insert { target, .. }
            | Self::Update { target, .. }
            | Self::Delete { target, .. } => target,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CheckedAssignment {
    pub(crate) property: String,
    pub(crate) value: TypedExpr,
}

/// An expression after resolution: one node per [`Expr`] node, every child
/// retained, every node carrying the type the checker resolved for it. An
/// `Aggregate` carries its signature and derives its type from it.
#[derive(Debug, Clone)]
pub(crate) enum TypedExpr {
    Now {
        ty: ResolvedType,
    },
    PropAccess {
        variable: String,
        property: String,
        ty: ResolvedType,
    },
    Nearest {
        variable: String,
        property: String,
        query: Box<TypedExpr>,
        ty: ResolvedType,
    },
    Search {
        field: Box<TypedExpr>,
        query: Box<TypedExpr>,
        ty: ResolvedType,
    },
    Fuzzy {
        field: Box<TypedExpr>,
        query: Box<TypedExpr>,
        max_edits: Option<Box<TypedExpr>>,
        ty: ResolvedType,
    },
    MatchText {
        field: Box<TypedExpr>,
        query: Box<TypedExpr>,
        ty: ResolvedType,
    },
    Bm25 {
        field: Box<TypedExpr>,
        query: Box<TypedExpr>,
        ty: ResolvedType,
    },
    Rrf {
        primary: Box<TypedExpr>,
        secondary: Box<TypedExpr>,
        k: Option<Box<TypedExpr>>,
        ty: ResolvedType,
    },
    /// A declared parameter (`Scalar`) or a node binding (`Node`); the
    /// checker resolves a parameter name first, so the type says which.
    Variable {
        name: String,
        ty: ResolvedType,
    },
    Literal {
        literal: Literal,
        ty: ResolvedType,
    },
    Aggregate {
        func: AggFunc,
        distinct: bool,
        arg: Box<TypedExpr>,
        signature: AggSignature,
    },
    AliasRef {
        name: String,
        ty: ResolvedType,
    },
    Binary {
        left: Box<TypedExpr>,
        op: BinaryOp,
        right: Box<TypedExpr>,
        ty: ResolvedType,
    },
    Not {
        inner: Box<TypedExpr>,
        ty: ResolvedType,
    },
    IsNull {
        inner: Box<TypedExpr>,
        negated: bool,
        ty: ResolvedType,
    },
    In {
        needle: Box<TypedExpr>,
        list: Box<TypedExpr>,
        ty: ResolvedType,
    },
}

impl TypedExpr {
    /// The type the checker resolved for this node.
    pub(crate) fn ty(&self) -> ResolvedType {
        match self {
            Self::Now { ty }
            | Self::PropAccess { ty, .. }
            | Self::Nearest { ty, .. }
            | Self::Search { ty, .. }
            | Self::Fuzzy { ty, .. }
            | Self::MatchText { ty, .. }
            | Self::Bm25 { ty, .. }
            | Self::Rrf { ty, .. }
            | Self::Variable { ty, .. }
            | Self::Literal { ty, .. }
            | Self::AliasRef { ty, .. }
            | Self::Binary { ty, .. }
            | Self::Not { ty, .. }
            | Self::IsNull { ty, .. }
            | Self::In { ty, .. } => ty.clone(),
            Self::Aggregate { signature, .. } => ResolvedType::Aggregate(signature.result.clone()),
        }
    }

    /// The IR type of a completed node. A retained tree holds no
    /// `ForwardAlias` (`completed` refuses one at the root and every parent
    /// rule refuses one as a child), so the conversion is total.
    pub(crate) fn expr_type(&self) -> ExprType {
        match self.ty() {
            ResolvedType::Scalar(prop) => ExprType::from_prop(&prop),
            ResolvedType::Node(type_name) => ExprType::Node { type_name },
            ResolvedType::Aggregate(ty) => ty,
            ResolvedType::ForwardAlias => {
                unreachable!("a completed expression never retains a forward alias")
            }
        }
    }

    /// Replace the resolved type of a literal whose type another operand
    /// decided (a direct null, an empty or all-null list).
    fn set_ty(&mut self, resolved: ResolvedType) {
        match self {
            Self::Now { ty }
            | Self::PropAccess { ty, .. }
            | Self::Nearest { ty, .. }
            | Self::Search { ty, .. }
            | Self::Fuzzy { ty, .. }
            | Self::MatchText { ty, .. }
            | Self::Bm25 { ty, .. }
            | Self::Rrf { ty, .. }
            | Self::Variable { ty, .. }
            | Self::Literal { ty, .. }
            | Self::AliasRef { ty, .. }
            | Self::Binary { ty, .. }
            | Self::Not { ty, .. }
            | Self::IsNull { ty, .. }
            | Self::In { ty, .. } => *ty = resolved,
            Self::Aggregate { .. } => {
                unreachable!("an aggregate's type is its signature's result")
            }
        }
    }

    /// Whether `self` is a `search`, `fuzzy` or `match_text` call.
    pub(crate) fn is_search_call(&self) -> bool {
        matches!(
            self,
            Self::Search { .. } | Self::Fuzzy { .. } | Self::MatchText { .. }
        )
    }

    /// [`Expr::score_column`] over the typed tree.
    pub(crate) fn score_column(&self) -> Option<(&str, &'static str)> {
        match self {
            Self::Nearest { variable, .. } => Some((variable, DISTANCE_COLUMN)),
            Self::Bm25 { field, .. } => match field.as_ref() {
                Self::PropAccess { variable, .. } => Some((variable, SCORE_COLUMN)),
                _ => None,
            },
            Self::Rrf { primary, .. } => primary
                .score_column()
                .map(|(variable, _)| (variable, RRF_COLUMN)),
            _ => None,
        }
    }

    /// The scalar type of the column `score_column` names: Lance writes
    /// `F32` scores and distances, the fusion an `F64` sum.
    pub(crate) fn score_type(&self) -> ScalarType {
        match self {
            Self::Rrf { .. } => ScalarType::F64,
            _ => ScalarType::F32,
        }
    }

    /// The alias an unaliased meta-field projection needs: the executor keys
    /// a projection by its physical column, which is vintage-specific, so the
    /// logical `var.@id` rides as the alias and result columns read as the
    /// query wrote them.
    pub(crate) fn meta_field_result_key(&self) -> Option<String> {
        match self {
            Self::PropAccess {
                variable, property, ..
            } if property.starts_with('@') => Some(format!("{variable}.{property}")),
            Self::Aggregate { arg, .. } => arg.meta_field_result_key(),
            _ => None,
        }
    }

    /// The variable a `search`, `fuzzy` or `match_text` call reads, anywhere
    /// in the expression.
    pub(crate) fn text_search_subject(&self) -> Option<&str> {
        match self {
            Self::Search { field, .. }
            | Self::Fuzzy { field, .. }
            | Self::MatchText { field, .. } => match field.as_ref() {
                Self::PropAccess { variable, .. } => Some(variable.as_str()),
                _ => None,
            },
            Self::Binary { left, right, .. }
            | Self::In {
                needle: left,
                list: right,
                ..
            } => left
                .text_search_subject()
                .or_else(|| right.text_search_subject()),
            Self::Not { inner, .. } | Self::IsNull { inner, .. } => inner.text_search_subject(),
            _ => None,
        }
    }

    /// The first variable name the expression's leaves read, operand by
    /// operand in written order.
    pub(crate) fn first_variable(&self) -> Option<&str> {
        match self {
            Self::Now { .. } | Self::Literal { .. } | Self::AliasRef { .. } => None,
            Self::Binary { left, right, .. }
            | Self::In {
                needle: left,
                list: right,
                ..
            } => left.first_variable().or_else(|| right.first_variable()),
            Self::Not { inner, .. } | Self::IsNull { inner, .. } => inner.first_variable(),
            Self::PropAccess { variable, .. } | Self::Nearest { variable, .. } => Some(variable),
            Self::Variable { name, .. } => Some(name),
            Self::Search { field, query, .. }
            | Self::MatchText { field, query, .. }
            | Self::Bm25 { field, query, .. } => {
                field.first_variable().or_else(|| query.first_variable())
            }
            Self::Fuzzy {
                field,
                query,
                max_edits,
                ..
            } => field
                .first_variable()
                .or_else(|| query.first_variable())
                .or_else(|| max_edits.as_deref().and_then(Self::first_variable)),
            Self::Rrf {
                primary,
                secondary,
                k,
                ..
            } => primary
                .first_variable()
                .or_else(|| secondary.first_variable())
                .or_else(|| k.as_deref().and_then(Self::first_variable)),
            Self::Aggregate { arg, .. } => arg.first_variable(),
        }
    }
}

/// A resolved expression about to be retained by a clause: its root may not
/// be a forward alias. Every parent rule already refuses one as a child, so
/// this check completes the tree without walking it.
fn completed(expr: TypedExpr) -> Result<TypedExpr> {
    if matches!(expr.ty(), ResolvedType::ForwardAlias) {
        return Err(CompilerError::Plan(
            "unresolved forward alias retained by a checked clause".into(),
        ));
    }
    Ok(expr)
}

/// Where an expression stands. Every scope resolves through the one
/// `resolve_expr`; the scope decides what a bare name means and which
/// node kinds are refused, one binder per clause over one expression type.
#[derive(Clone, Copy)]
enum Scope<'a> {
    /// A read clause over the match bindings.
    Read,
    /// A `return` item or order key: a comparison may take an aggregate
    /// operand, evaluated per group after aggregation.
    Return,
    /// A mutation `where`: a bare name is a property of the target.
    MutationWhere(&'a MutationTarget),
    /// An assignment value or an inline binding match on `type_name`:
    /// constants only.
    Constant {
        clause: ConstantClause,
        type_name: &'a str,
    },
}

#[derive(Clone, Copy)]
enum ConstantClause {
    Assignment,
    BindingMatch,
}

impl Scope<'_> {
    /// A read over the match bindings: a filter, an order key or a return item.
    fn is_read(self) -> bool {
        matches!(self, Scope::Read | Scope::Return)
    }

    /// The refusal of an aggregate, search or ranking call in this scope, the
    /// call's keyword in front (T44); `None` in a read.
    fn call_refusal(self, keyword: &str) -> Option<CompilerError> {
        match self {
            Scope::Read | Scope::Return => None,
            Scope::MutationWhere(_) => Some(CompilerError::typed(
                T44,
                format!(
                    "`{keyword}` cannot appear in a mutation where; a where compares the row's own properties, parameters and now()"
                ),
            )),
            Scope::Constant { .. } => Some(CompilerError::typed(
                T44,
                format!(
                    "`{keyword}` cannot appear in an assignment value; assignments and binding matches are constants per invocation"
                ),
            )),
        }
    }

    /// The refusal of a variable that names no declared parameter outside a
    /// read: T14 in a mutation statement, T3 in a binding match.
    fn undeclared_parameter(self, name: &str) -> CompilerError {
        match self {
            Scope::Constant {
                clause: ConstantClause::BindingMatch,
                ..
            } => CompilerError::typed(
                T3,
                format!("match variable `${name}` must be a declared query parameter"),
            ),
            _ => CompilerError::typed(
                T14,
                format!("mutation variable `${name}` must be a declared query parameter"),
            ),
        }
    }
}

/// A property, `@id`, `@src` or `@dst` leaf where only a constant may stand
/// (T45); a bare name of the clause's own type prints bare.
fn constant_leaf_refusal(type_name: &str, variable: &str, property: &str) -> CompilerError {
    let leaf = if variable == type_name {
        property.to_string()
    } else {
        format!("${variable}.{property}")
    };
    CompilerError::typed(
        T45,
        format!(
            "`{leaf}` cannot appear in an assignment value; assignments and binding matches are constants per invocation"
        ),
    )
}

/// The keyword of an aggregate, search or ranking call; `None` for every
/// other node.
fn call_keyword(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Aggregate { func, .. } => Some(func.to_string()),
        Expr::Nearest { .. }
        | Expr::Search { .. }
        | Expr::Fuzzy { .. }
        | Expr::MatchText { .. }
        | Expr::Bm25 { .. }
        | Expr::Rrf { .. } => Some(rank_keyword(expr).to_string()),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedTraversal {
    pub src: String,
    pub dst: String,
    pub edges: EdgeSelection,
    pub src_type: String,
    pub dst_type: String,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
    /// Variable bound to the matched edge (`$p $w:knows $f`), if any;
    /// lowering uses it to carry edge properties through the expand.
    pub edge_binding: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedType {
    Scalar(PropType),
    Node(String),
    Aggregate(ExprType),
    ForwardAlias,
}

impl ResolvedType {
    fn display_name(&self) -> String {
        match self {
            Self::Scalar(prop) => prop.display_name(),
            Self::Node(type_name) => format!("node `{}`", type_name),
            Self::Aggregate(_) => "aggregate".to_string(),
            Self::ForwardAlias => "aggregate".to_string(),
        }
    }
}

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

/// A query after type checking, read or mutation. The two lowerers each
/// take their own kind, so a checked query cannot reach the wrong one.
#[derive(Debug, Clone)]
pub enum CheckedQuery {
    Read(CheckedReadQuery),
    Mutation(CheckedMutationQuery),
}

pub fn typecheck_query_decl(catalog: &Catalog, query: &QueryDecl) -> Result<CheckedQuery> {
    if !query.mutations.is_empty() {
        let (params, param_types) = checked_params(&query.params)?;
        typecheck_unattached_base(&query.write_mode, &param_types)?;
        let mut ops = Vec::with_capacity(query.mutations.len());
        for mutation in &query.mutations {
            ops.push(typecheck_mutation(catalog, mutation, &param_types)?);
        }
        Ok(CheckedQuery::Mutation(CheckedMutationQuery {
            name: query.name.clone(),
            params,
            ops,
            write_mode: query.write_mode.clone(),
        }))
    } else {
        Ok(CheckedQuery::Read(typecheck_read_query(catalog, query)?))
    }
}

pub fn typecheck_query(catalog: &Catalog, query: &QueryDecl) -> Result<CheckedReadQuery> {
    if !query.mutations.is_empty() {
        return Err(CompilerError::Plan(
            "mutation query cannot be typechecked with read-query API".to_string(),
        ));
    }
    typecheck_read_query(catalog, query)
}

/// The Arrow schema of a checked read's result, from the types its return
/// items retained; nothing is resolved again.
pub fn infer_query_result_schema(catalog: &Catalog, query: &CheckedReadQuery) -> Result<SchemaRef> {
    let fields = query
        .returns
        .iter()
        .map(|item| projection_field(catalog, &item.inferred_name, &item.expr.expr_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// `on $base` names a declared, non-nullable `String` parameter (T55).
fn typecheck_unattached_base(
    write_mode: &MutationWriteMode,
    param_types: &HashMap<String, PropType>,
) -> Result<()> {
    let MutationWriteMode::Unattached {
        base: Some(UnattachedBase::Param(name)),
    } = write_mode
    else {
        return Ok(());
    };
    match param_types.get(name) {
        Some(ty) if ty.scalar == ScalarType::String && !ty.nullable && !ty.list => Ok(()),
        Some(ty) => Err(CompilerError::typed(
            T55,
            format!(
                "`on $base` needs a declared `String` parameter; `${name}` is `{}`",
                ty.display_name()
            ),
        )),
        None => Err(CompilerError::typed(
            T55,
            format!("`on $base` needs a declared `String` parameter; `${name}` is not declared"),
        )),
    }
}

/// The declarations in source order, each with the type selected for its
/// name, and the name lookup the checker resolves variables through. A
/// repeated name keeps every declaration and the last one's type.
fn checked_params(params: &[Param]) -> Result<(Vec<CheckedParam>, HashMap<String, PropType>)> {
    let types = parse_declared_param_types(params)?;
    let checked = params
        .iter()
        .map(|declaration| CheckedParam {
            declaration: declaration.clone(),
            ty: types[&declaration.name].clone(),
        })
        .collect();
    Ok((checked, types))
}

fn parse_declared_param_types(params: &[Param]) -> Result<HashMap<String, PropType>> {
    let mut out = HashMap::with_capacity(params.len());
    for p in params {
        if p.name == NOW_PARAM_NAME {
            return Err(CompilerError::typed(
                T47,
                format!(
                    "parameter name `${}` is reserved for runtime timestamp injection",
                    NOW_PARAM_NAME
                ),
            ));
        }
        let prop_type =
            PropType::from_param_type_name(&p.type_name, p.nullable).ok_or_else(|| {
                CompilerError::typed(
                    T48,
                    format!("unknown parameter type `{}` for `${}`", p.type_name, p.name),
                )
            })?;
        out.insert(p.name.clone(), prop_type);
    }
    Ok(out)
}

/// Names beginning with `__` are the compiler's: lowering mints `__anon_N`
/// for anonymous traversal endpoints and `__temp_<var>_N` for cycle-closing
/// traversals, and the plan check refuses a name introduced twice, so a user
/// variable spelled like one would fail there with a message about the
/// plan. Refuse it here, once, with the reason.
fn refuse_reserved_variable_names(clauses: &[Clause]) -> Result<()> {
    fn check(name: &str) -> Result<()> {
        if name.starts_with("__") {
            return Err(CompilerError::typed(
                T49,
                format!(
                    "variable `${name}`: names beginning with `__` are reserved for the compiler"
                ),
            ));
        }
        Ok(())
    }
    for clause in clauses {
        match clause {
            Clause::Binding(binding) => check(&binding.variable)?,
            Clause::Traversal(traversal) => {
                check(&traversal.src)?;
                check(&traversal.dst)?;
                if let Some(edge) = &traversal.edge_binding {
                    check(edge)?;
                }
            }
            Clause::Filter(_) => {}
            Clause::Subquery(subquery) => refuse_reserved_variable_names(&subquery.clauses)?,
        }
    }
    Ok(())
}

fn typecheck_read_query(catalog: &Catalog, query: &QueryDecl) -> Result<CheckedReadQuery> {
    let mut ctx = TypeContext::empty();
    let (checked_params, params) = checked_params(&query.params)?;

    refuse_reserved_variable_names(&query.match_clause)?;

    // Typecheck match clauses
    let clauses = typecheck_clauses(
        catalog,
        &query.match_clause,
        &mut ctx,
        &params,
        &mut HashSet::new(),
    )?;

    // Typecheck return projections
    let mut result_columns: HashSet<String> = HashSet::new();
    let mut returns = Vec::with_capacity(query.return_clause.len());
    for proj in &query.return_clause {
        let alias = proj.alias.as_deref();
        let typed = resolve_expr(catalog, &proj.expr, &ctx, &params, Scope::Return)?;
        let resolved = typed.ty();
        reject_blob_read_value(&resolved, &proj.expr)?;
        check_projection(&proj.expr, alias, &query.order_clause)?;
        // T25: one result column per name. The executor emits a batch with
        // every projection's column under its executed name; two columns of
        // one name survive the batch (Arrow allows it) and every reader that
        // keys a row by column name keeps the last one, so the first value
        // is lost without an error.
        let column = executed_column_name(&proj.expr, alias);
        if !result_columns.insert(column.clone()) {
            return Err(CompilerError::typed(
                T25,
                format!(
                    "result column `{column}` is produced by more than one projection; give each projection its own alias"
                ),
            ));
        }
        if let Some(alias) = &proj.alias {
            ctx.aliases.insert(alias.clone(), resolved);
        }
        returns.push(CheckedReturn {
            expr: completed(typed)?,
            alias: proj.alias.clone(),
            inferred_name: projection_name(&proj.expr, alias),
            executed_column: column,
        });
    }

    let has_agg = query
        .return_clause
        .iter()
        .any(|p| expr_contains_aggregate(&p.expr));

    // Typecheck order expressions
    let mut orders = Vec::with_capacity(query.order_clause.len());
    for (index, ord) in query.order_clause.iter().enumerate() {
        let typed = resolve_expr(catalog, &ord.expr, &ctx, &params, Scope::Return)?;
        reject_blob_read_value(&typed.ty(), &ord.expr)?;
        let carried_meta = has_agg
            && matches!(typed, TypedExpr::PropAccess { .. })
            && typed.meta_field_result_key().is_some_and(|name| {
                returns.iter().any(|item| {
                    item.alias.is_none()
                        && item.expr.meta_field_result_key().as_deref() == Some(name.as_str())
                })
            });
        if !carried_meta {
            bind_order_key(
                index,
                &ord.expr,
                &query.return_clause,
                &ctx,
                query.distinct,
                has_agg,
            )?;
        }
        if matches!(ord.expr, Expr::Bm25 { .. } | Expr::Rrf { .. })
            && ord.explicit
            && !ord.descending
        {
            return Err(CompilerError::typed(
                T54,
                format!(
                    "`{}` ranks most relevant first; its order key does not accept the asc modifier",
                    rank_keyword(&ord.expr)
                ),
            ));
        }
        orders.push(CheckedOrder {
            expr: completed(typed)?,
            descending: ord.descending,
        });
    }

    let has_standalone_nearest = query
        .order_clause
        .iter()
        .any(|ord| expr_contains_standalone_nearest(&ord.expr));
    let has_rrf = query
        .order_clause
        .iter()
        .any(|ord| expr_contains_rrf(&ord.expr));
    if has_rrf && query.limit.is_none() {
        return Err(CompilerError::typed(
            T21,
            "rrf ordering requires a limit clause".to_string(),
        ));
    }
    if has_standalone_nearest && query.limit.is_none() {
        return Err(CompilerError::typed(
            T17,
            "nearest ordering requires a limit clause".to_string(),
        ));
    }

    // T9: If any return expression is an aggregate, non-aggregate expressions
    // must be valid group-by keys (PropAccess or Variable).
    if query.distinct && has_agg {
        return Err(CompilerError::typed(
            T56,
            "`distinct` on an aggregated return: the group values are already one row each; \
             drop `distinct` or the aggregate"
                .to_string(),
        ));
    }
    if has_agg {
        for proj in &query.return_clause {
            match &proj.expr {
                Expr::Aggregate { .. } | Expr::PropAccess { .. } | Expr::Variable(_) => {}
                Expr::Binary { .. } | Expr::Not(_) | Expr::IsNull { .. } | Expr::In { .. }
                    if expr_contains_aggregate(&proj.expr) =>
                {
                    check_aggregate_comparison_leaves(&proj.expr, &params)?;
                }
                _ => {
                    return Err(CompilerError::typed(
                        T9,
                        "non-aggregate expressions in an aggregate query must be \
                         property accesses or variables"
                            .to_string(),
                    ));
                }
            }
        }
    }

    Ok(CheckedReadQuery {
        name: query.name.clone(),
        params: checked_params,
        scope: CheckedScope {
            bindings: ctx.bindings,
            clauses,
        },
        returns,
        distinct: query.distinct,
        orders,
        limit: query.limit,
    })
}

/// T42 (RFC 2026-09-24-shared-expression-model, "Order keys bind against the
/// return list"): a property key has a hidden column only on ungrouped rows;
/// the leading rank key is the search node's; others bind to return.
fn bind_order_key(
    index: usize,
    key: &Expr,
    return_clause: &[Projection],
    ctx: &TypeContext,
    distinct: bool,
    aggregate: bool,
) -> Result<()> {
    let exempt = match key {
        Expr::PropAccess { variable, property } => {
            !distinct
                && (!aggregate
                    || (property == EDGE_TYPE_META
                        && matches!(ctx.bindings.get(variable),
                            Some(BoundVariable::Edge { type_names }) if type_names.len() == 1)))
        }
        Expr::Nearest { .. } | Expr::Bm25 { .. } | Expr::Rrf { .. } => index == 0,
        _ => false,
    };
    if exempt {
        return Ok(());
    }
    let bound = match key {
        Expr::AliasRef(name) => ctx.aliases.contains_key(name),
        other => return_clause.iter().any(|item| &item.expr == other),
    };
    if bound {
        return Ok(());
    }
    let fix = if distinct {
        "under `distinct` an order key is a projected value or its alias; add it to return"
    } else {
        "add it to return or order by its alias"
    };
    Err(CompilerError::typed(
        T42,
        format!("order key `{key}` does not appear in return; {fix}"),
    ))
}

fn typecheck_mutation(
    catalog: &Catalog,
    mutation: &Mutation,
    param_types: &HashMap<String, PropType>,
) -> Result<CheckedMutationOp> {
    refuse_catalogue_write(match mutation {
        Mutation::Insert(insert) => &insert.type_name,
        Mutation::Update(update) => &update.type_name,
        Mutation::Delete(delete) => &delete.type_name,
    })?;
    match mutation {
        Mutation::Insert(insert) => {
            if insert.assignments.is_empty() {
                return Err(CompilerError::typed(
                    T10,
                    "insert mutation requires at least one assignment".to_string(),
                ));
            }

            ensure_no_duplicate_assignment_names(&insert.assignments)?;

            if let Some(node_type) = catalog.node_types.get(&insert.type_name) {
                let mut assignments = Vec::with_capacity(insert.assignments.len());
                for assignment in &insert.assignments {
                    let prop_type =
                        node_type
                            .properties
                            .get(&assignment.property)
                            .ok_or_else(|| {
                                CompilerError::typed(
                                    T11,
                                    format!(
                                        "type `{}` has no property `{}`{}",
                                        insert.type_name,
                                        assignment.property,
                                        identity_assignment_hint(&assignment.property)
                                    ),
                                )
                            })?;
                    assignments.push(typecheck_assignment(
                        catalog,
                        assignment,
                        param_types,
                        prop_type,
                        &insert.type_name,
                    )?);
                }

                let assigned_props: HashSet<&str> = insert
                    .assignments
                    .iter()
                    .map(|assignment| assignment.property.as_str())
                    .collect();
                for (prop_name, prop_type) in &node_type.properties {
                    if prop_type.nullable {
                        continue;
                    }
                    if assigned_props.contains(prop_name.as_str()) {
                        continue;
                    }

                    if let Some(embed) = node_type.embed_sources.get(prop_name) {
                        if assigned_props.contains(embed.source.as_str()) {
                            continue;
                        }
                        return Err(CompilerError::typed(
                            T12,
                            format!(
                                "insert for `{}` must provide non-nullable property `{}` or @embed source `{}`",
                                insert.type_name, prop_name, embed.source
                            ),
                        ));
                    }

                    return Err(CompilerError::typed(
                        T12,
                        format!(
                            "insert for `{}` must provide non-nullable property `{}`",
                            insert.type_name, prop_name
                        ),
                    ));
                }
                return Ok(CheckedMutationOp::Insert {
                    target: MutationTarget::Node {
                        type_name: insert.type_name.clone(),
                    },
                    assignments,
                });
            }

            if let Some(edge_type) = catalog.edge_types.get(&insert.type_name) {
                let mut has_from = false;
                let mut has_to = false;
                let mut assignments = Vec::with_capacity(insert.assignments.len());

                for assignment in &insert.assignments {
                    let prop_type = match assignment.property.as_str() {
                        "from" => {
                            has_from = true;
                            meta_field_type()
                        }
                        "to" => {
                            has_to = true;
                            meta_field_type()
                        }
                        _ => edge_type
                            .properties
                            .get(&assignment.property)
                            .ok_or_else(|| {
                                CompilerError::typed(
                                    T11,
                                    format!(
                                        "type `{}` has no property `{}`{}",
                                        insert.type_name,
                                        assignment.property,
                                        identity_assignment_hint(&assignment.property)
                                    ),
                                )
                            })?
                            .clone(),
                    };
                    assignments.push(typecheck_assignment(
                        catalog,
                        assignment,
                        param_types,
                        &prop_type,
                        &insert.type_name,
                    )?);
                }

                if !has_from {
                    return Err(CompilerError::typed(
                        T12,
                        format!(
                            "insert for `{}` must provide required endpoint `from`",
                            insert.type_name
                        ),
                    ));
                }
                if !has_to {
                    return Err(CompilerError::typed(
                        T12,
                        format!(
                            "insert for `{}` must provide required endpoint `to`",
                            insert.type_name
                        ),
                    ));
                }

                for (prop_name, prop_type) in &edge_type.properties {
                    if prop_type.nullable {
                        continue;
                    }
                    if !insert.assignments.iter().any(|a| &a.property == prop_name) {
                        return Err(CompilerError::typed(
                            T12,
                            format!(
                                "insert for `{}` must provide non-nullable property `{}`",
                                insert.type_name, prop_name
                            ),
                        ));
                    }
                }
                return Ok(CheckedMutationOp::Insert {
                    target: MutationTarget::Edge {
                        type_name: insert.type_name.clone(),
                    },
                    assignments,
                });
            }

            Err(CompilerError::typed(
                T10,
                format!("unknown node/edge type `{}`", insert.type_name),
            ))
        }
        Mutation::Update(update) => {
            let node_type = if let Some(node_type) = catalog.node_types.get(&update.type_name) {
                node_type
            } else if catalog.edge_types.contains_key(&update.type_name) {
                return Err(CompilerError::typed(
                    T16,
                    format!(
                        "update mutation for edge type `{}` is not supported",
                        update.type_name
                    ),
                ));
            } else {
                return Err(CompilerError::typed(
                    T10,
                    format!("unknown node/edge type `{}`", update.type_name),
                ));
            };

            if update.assignments.is_empty() {
                return Err(CompilerError::typed(
                    T10,
                    "update mutation requires at least one assignment".to_string(),
                ));
            }
            ensure_no_duplicate_assignment_names(&update.assignments)?;

            let mut assignments = Vec::with_capacity(update.assignments.len());
            for assignment in &update.assignments {
                let prop_type =
                    node_type
                        .properties
                        .get(&assignment.property)
                        .ok_or_else(|| {
                            CompilerError::typed(
                                T11,
                                format!(
                                    "type `{}` has no property `{}`{}",
                                    update.type_name,
                                    assignment.property,
                                    identity_assignment_hint(&assignment.property)
                                ),
                            )
                        })?;
                assignments.push(typecheck_assignment(
                    catalog,
                    assignment,
                    param_types,
                    prop_type,
                    &update.type_name,
                )?);
            }

            let target = MutationTarget::Node {
                type_name: update.type_name.clone(),
            };
            let predicate =
                typecheck_mutation_where(catalog, &target, &update.predicate, param_types)?;
            Ok(CheckedMutationOp::Update {
                target,
                assignments,
                predicate,
            })
        }
        Mutation::Delete(delete) => {
            let target = if catalog.node_types.contains_key(&delete.type_name) {
                MutationTarget::Node {
                    type_name: delete.type_name.clone(),
                }
            } else if catalog.edge_types.contains_key(&delete.type_name) {
                MutationTarget::Edge {
                    type_name: delete.type_name.clone(),
                }
            } else {
                return Err(CompilerError::typed(
                    T10,
                    format!("unknown node/edge type `{}`", delete.type_name),
                ));
            };
            let predicate =
                typecheck_mutation_where(catalog, &target, &delete.predicate, param_types)?;
            Ok(CheckedMutationOp::Delete { target, predicate })
        }
    }
}

fn ensure_no_duplicate_assignment_names(assignments: &[MutationAssignment]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        if !seen.insert(&assignment.property) {
            return Err(CompilerError::typed(
                T13,
                format!(
                    "duplicate assignment for property `{}`",
                    assignment.property
                ),
            ));
        }
    }
    Ok(())
}

enum MetaField {
    System(SystemFieldRole),
    EdgeType,
}

/// A query meta-field, including the virtual edge type; `None` for a bare name.
fn meta_field_role(property: &str) -> Option<Option<MetaField>> {
    property.starts_with('@').then(|| match property {
        name if name == SYSTEM_COLUMNS_META.id => Some(MetaField::System(SystemFieldRole::Id)),
        name if name == SYSTEM_COLUMNS_META.src => Some(MetaField::System(SystemFieldRole::Src)),
        name if name == SYSTEM_COLUMNS_META.dst => Some(MetaField::System(SystemFieldRole::Dst)),
        EDGE_TYPE_META => Some(MetaField::EdgeType),
        _ => None,
    })
}

/// Every metadata value is a non-null string.
fn meta_field_type() -> PropType {
    PropType::scalar(ScalarType::String, false)
}

fn system_field_hint(property: &str, binding: Option<&str>, is_edge: bool) -> String {
    let meta = match property {
        "id" => SYSTEM_COLUMNS_META.id,
        "src" if is_edge => SYSTEM_COLUMNS_META.src,
        "dst" if is_edge => SYSTEM_COLUMNS_META.dst,
        _ => return String::new(),
    };
    let role = if property == "id" {
        "identity"
    } else {
        "endpoint"
    };
    match binding {
        Some(variable) => format!("; the system {role} is `${variable}.{meta}`"),
        None => format!("; the system {role} is `{meta}`"),
    }
}

fn identity_assignment_hint(property: &str) -> &'static str {
    if property == "id" {
        "; the engine assigns system identity; it cannot be set through a property assignment"
    } else {
        ""
    }
}

/// The type of a bare name in a mutation `where` on `target`: the legacy
/// `from`/`to` endpoints and the admitted meta-fields are the endpoint
/// strings; a user property keeps its declared type; Blob is refused.
fn mutation_property_type(
    catalog: &Catalog,
    target: &MutationTarget,
    variable: &str,
    property: &str,
) -> Result<PropType> {
    let type_name = target.type_name();
    if variable != type_name {
        return Err(CompilerError::typed(
            T14,
            format!("mutation variable `${variable}` must be a declared query parameter"),
        ));
    }
    let is_edge = target.is_edge();
    if is_edge && (property == "from" || property == "to") {
        return Ok(meta_field_type());
    }
    if let Some(role) = meta_field_role(property) {
        let admitted = match role {
            Some(MetaField::System(SystemFieldRole::Id)) => true,
            Some(MetaField::System(SystemFieldRole::Src | SystemFieldRole::Dst)) => is_edge,
            Some(MetaField::EdgeType) | None => false,
        };
        if !admitted {
            let known = if is_edge {
                "`@id`, `@src`, `@dst`"
            } else {
                "`@id`"
            };
            return Err(CompilerError::typed(
                T11,
                format!(
                    "type `{type_name}` has no meta-field `{property}`; the meta-fields of this type are {known}"
                ),
            ));
        }
        return Ok(meta_field_type());
    }
    let prop_type = declared_property(catalog, target, property).ok_or_else(|| {
        CompilerError::typed(
            T11,
            format!(
                "type `{type_name}` has no property `{property}`{}",
                system_field_hint(property, None, is_edge)
            ),
        )
    })?;
    if matches!(prop_type.scalar, ScalarType::Blob) {
        return Err(CompilerError::typed(
            T11,
            format!("blob property `{property}` cannot be used in WHERE predicates"),
        ));
    }
    Ok(prop_type.clone())
}

/// A catalogue type is read-only: no insert, update, delete or load targets
/// it, whether or not the name is a declared catalogue type.
pub(crate) fn refuse_catalogue_write(type_name: &str) -> Result<()> {
    if crate::catalog::catalogue::is_catalogue_type_name(type_name) {
        return Err(CompilerError::typed(
            T10,
            format!("catalogue type `{type_name}` is read-only; the engine owns its rows"),
        ));
    }
    Ok(())
}

/// The user property `property` of the mutation target's own namespace.
fn declared_property<'c>(
    catalog: &'c Catalog,
    target: &MutationTarget,
    property: &str,
) -> Option<&'c PropType> {
    let type_name = target.type_name();
    match target {
        MutationTarget::Node { .. } => catalog
            .node_types
            .get(type_name)
            .and_then(|node_type| node_type.properties.get(property)),
        MutationTarget::Edge { .. } => catalog
            .edge_types
            .get(type_name)
            .and_then(|edge_type| edge_type.properties.get(property)),
    }
}

/// A mutation `where` is an expression the checker proves Boolean under the
/// target's scope; a Boolean literal on a target that declares a property named
/// `true` or `false` is refused (T46), since the bare word can no longer reach it.
fn typecheck_mutation_where(
    catalog: &Catalog,
    target: &MutationTarget,
    predicate: &Expr,
    params: &HashMap<String, PropType>,
) -> Result<TypedExpr> {
    let shadowed: Vec<&str> = ["true", "false"]
        .into_iter()
        .filter(|name| declared_property(catalog, target, name).is_some())
        .collect();
    if let Some(literal) = first_boolean_literal(predicate)
        && let Some(first) = shadowed.first()
    {
        let word = if literal { "true" } else { "false" };
        let property = shadowed.iter().find(|name| **name == word).unwrap_or(first);
        return Err(CompilerError::typed(
            T46,
            format!(
                "`{word}` is a Boolean literal here; the property named `{property}` of `{}` cannot be named bare in a mutation `where`; rename it in the schema (`@rename_from`)",
                target.type_name()
            ),
        ));
    }
    let typed = resolve_expr(
        catalog,
        predicate,
        &TypeContext::empty(),
        params,
        Scope::MutationWhere(target),
    )?;
    let resolved = typed.ty();
    if boolean_scalar(&resolved).is_none() {
        return Err(CompilerError::typed(
            T41,
            format!(
                "a mutation `where` must be Boolean, got {}",
                resolved.display_name()
            ),
        ));
    }
    completed(typed)
}

/// The first bare `true` or `false` in written order, through the Boolean
/// structure; a list literal's elements are not bare words.
fn first_boolean_literal(expr: &Expr) -> Option<bool> {
    match expr {
        Expr::Literal(Literal::Bool(value)) => Some(*value),
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => first_boolean_literal(left).or_else(|| first_boolean_literal(right)),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => first_boolean_literal(inner),
        _ => None,
    }
}

fn typecheck_assignment(
    catalog: &Catalog,
    assignment: &MutationAssignment,
    params: &HashMap<String, PropType>,
    expected: &PropType,
    type_name: &str,
) -> Result<CheckedAssignment> {
    let value = typecheck_constant(
        catalog,
        &assignment.value,
        params,
        expected,
        &assignment.property,
        ConstantClause::Assignment,
        type_name,
    )?;
    Ok(CheckedAssignment {
        property: assignment.property.clone(),
        value,
    })
}

/// A constant against the property it is assigned to or matched with: the
/// scope refuses every non-constant leaf, then the clause types the result
/// (the property's type; a binding match on a list property, its element type).
fn typecheck_constant(
    catalog: &Catalog,
    value: &Expr,
    params: &HashMap<String, PropType>,
    expected: &PropType,
    property: &str,
    clause: ConstantClause,
    type_name: &str,
) -> Result<TypedExpr> {
    let scope = Scope::Constant { clause, type_name };
    let typed = resolve_expr(catalog, value, &TypeContext::empty(), params, scope)?;
    check_constant(&typed, expected, property, clause)?;
    completed(typed)
}

/// The clause rules over an already resolved constant: a literal against
/// the property by its retained literal type and payload, `now()` against
/// DateTime, a parameter by compatibility.
fn check_constant(
    value: &TypedExpr,
    expected: &PropType,
    property: &str,
    clause: ConstantClause,
) -> Result<()> {
    let resolved = value.ty();
    let ResolvedType::Scalar(actual) = &resolved else {
        return Err(CompilerError::typed(
            T7,
            format!(
                "the value for property `{property}` must be a scalar, got {}",
                resolved.display_name()
            ),
        ));
    };
    match (clause, value) {
        (ConstantClause::Assignment, TypedExpr::Literal { literal, .. }) => {
            check_literal_type(literal, actual, expected, property)
        }
        (ConstantClause::BindingMatch, TypedExpr::Literal { literal, .. }) => {
            check_binding_literal_type(literal, actual, expected, property)
        }
        (_, TypedExpr::Now { .. }) => check_now_match_value_type(expected, property),
        (ConstantClause::Assignment, _) => {
            if !assignment_compatible(actual, expected) {
                return Err(CompilerError::typed(
                    T7,
                    format!(
                        "cannot assign/compare {} with {} for property `{}`",
                        actual.display_name(),
                        expected.display_name(),
                        property
                    ),
                ));
            }
            Ok(())
        }
        (ConstantClause::BindingMatch, _) => {
            check_binding_variable_type(actual, expected, property)
        }
    }
}

fn check_now_match_value_type(expected: &PropType, property: &str) -> Result<()> {
    if expected.list || expected.scalar != ScalarType::DateTime {
        return Err(CompilerError::typed(
            T7,
            format!(
                "cannot assign/compare DateTime with {} for property `{}`",
                expected.display_name(),
                property
            ),
        ));
    }
    Ok(())
}

/// The clauses of one scope, checked in source order and retained with what
/// each resolved; binding matches are typed in a prepass over the bindings
/// (a traversal may name a binding declared after it) and moved in once.
fn typecheck_clauses(
    catalog: &Catalog,
    clauses: &[Clause],
    ctx: &mut TypeContext,
    params: &HashMap<String, PropType>,
    declared_nodes: &mut HashSet<String>,
) -> Result<Vec<CheckedClause>> {
    let mut declarations = TypeContext {
        bindings: ctx.bindings.clone(),
        ..TypeContext::empty()
    };
    let mut local_declared = declared_nodes.clone();
    let mut typed_matches = Vec::new();
    for clause in clauses {
        if let Clause::Binding(binding) = clause {
            typed_matches.push(typecheck_binding(
                catalog,
                binding,
                &mut declarations,
                params,
            )?);
            if binding.variable != "_" {
                local_declared.insert(binding.variable.clone());
            }
        }
    }
    let mut typed_matches = typed_matches.into_iter();
    let mut checked = Vec::with_capacity(clauses.len());
    for clause in clauses {
        match clause {
            Clause::Binding(binding) => {
                bind_node(binding, ctx)?;
                if binding.variable != "_" {
                    declared_nodes.insert(binding.variable.clone());
                }
                checked.push(CheckedClause::Binding(CheckedBinding {
                    variable: binding.variable.clone(),
                    type_name: binding.type_name.clone(),
                    matches: typed_matches
                        .next()
                        .expect("every binding clause was typed in the prepass"),
                }));
            }
            Clause::Traversal(t) => checked.push(CheckedClause::Traversal(typecheck_traversal(
                catalog,
                t,
                ctx,
                &local_declared,
                &declarations.bindings,
            )?)),
            Clause::Filter(f) => checked.push(CheckedClause::Filter(typecheck_filter(
                catalog, f, ctx, params,
            )?)),
            Clause::Subquery(subquery) => {
                let outer_vars: Vec<String> = ctx.bindings.keys().cloned().collect();
                let mut inner_ctx = TypeContext {
                    bindings: ctx.bindings.clone(),
                    ..TypeContext::empty()
                };
                let inner_clauses = typecheck_clauses(
                    catalog,
                    &subquery.clauses,
                    &mut inner_ctx,
                    params,
                    &mut declared_nodes.clone(),
                )?;
                if !block_references_outer(&subquery.clauses, &outer_vars) {
                    let rule = match subquery.keyword {
                        BlockKeyword::Not => T9,
                        BlockKeyword::Exists | BlockKeyword::Aggregate => T39,
                    };
                    return Err(CompilerError::typed(
                        rule,
                        format!(
                            "{} block must reference at least one outer-bound variable",
                            subquery.block_name()
                        ),
                    ));
                }
                let (aggregate, right) =
                    typecheck_subquery_predicate(catalog, subquery, &inner_ctx, ctx, params)?;
                checked.push(CheckedClause::Block(Box::new(CheckedBlock {
                    outer_bindings: ctx.bindings.clone(),
                    inner: CheckedScope {
                        bindings: inner_ctx.bindings,
                        clauses: inner_clauses,
                    },
                    aggregate,
                    op: subquery.op,
                    right,
                })));
            }
        }
    }
    validate_traversal_anchors(clauses, &declarations.bindings)?;
    Ok(checked)
}

fn validate_traversal_anchors(
    clauses: &[Clause],
    bindings: &HashMap<String, BoundVariable>,
) -> Result<()> {
    let mut reachable: HashSet<&str> = bindings
        .iter()
        .filter_map(|(name, binding)| {
            (name != "_" && matches!(binding, BoundVariable::Node { .. })).then_some(name.as_str())
        })
        .collect();
    let mut remaining: Vec<_> = clauses
        .iter()
        .filter_map(|clause| match clause {
            Clause::Traversal(traversal) => Some(traversal),
            Clause::Binding(_) | Clause::Filter(_) | Clause::Subquery(_) => None,
        })
        .collect();
    while !remaining.is_empty() {
        let count = remaining.len();
        remaining.retain(|traversal| {
            if !reachable.contains(traversal.src.as_str())
                && !reachable.contains(traversal.dst.as_str())
            {
                return true;
            }
            for endpoint in [&traversal.src, &traversal.dst] {
                if endpoint != "_" {
                    reachable.insert(endpoint.as_str());
                }
            }
            false
        });
        if remaining.len() == count {
            let traversal = remaining[0];
            return Err(CompilerError::typed(
                T5,
                format!(
                    "traversal from `${}` to `${}` requires an executable source or destination node binding",
                    traversal.src, traversal.dst
                ),
            ));
        }
    }
    Ok(())
}

/// Whether a block's own clauses read a variable bound outside it: the
/// correlation a `not { … }` or `count { … }` block needs.
fn block_references_outer(clauses: &[Clause], outer_vars: &[String]) -> bool {
    clauses.iter().any(|clause| match clause {
        Clause::Traversal(t) => outer_vars.contains(&t.src) || outer_vars.contains(&t.dst),
        Clause::Filter(f) => expr_references_any(f, outer_vars),
        Clause::Binding(b) => outer_vars.contains(&b.variable),
        Clause::Subquery(_) => false,
    })
}

/// The checked owner of a block aggregate. Counting a node binding uses the
/// row-count path, its argument validated once and then dropped; column
/// aggregates retain their argument and the ordinary nullable signature.
fn typecheck_block_aggregate(
    catalog: &Catalog,
    subquery: &Subquery,
    inner_ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<CheckedBlockAggregate> {
    match &subquery.arg {
        None if subquery.func == AggFunc::Count => Ok(CheckedBlockAggregate::RowCount),
        None => Err(CompilerError::typed(
            T40,
            "a block aggregate without an argument must count rows",
        )),
        Some(arg) => {
            let (arg, signature) =
                resolve_aggregate(catalog, subquery.func, arg, inner_ctx, params, true)?;
            if subquery.func == AggFunc::Count && matches!(signature.arg, ExprType::Node { .. }) {
                Ok(CheckedBlockAggregate::RowCount)
            } else {
                Ok(CheckedBlockAggregate::Aggregate {
                    func: subquery.func,
                    arg,
                    signature,
                })
            }
        }
    }
}

/// The comparison of a subquery predicate: the aggregate's result type from
/// the block's scope (the argument rule of a `return` aggregate), the right
/// operand from the outer scope, compatible under the filter rule.
fn typecheck_subquery_predicate(
    catalog: &Catalog,
    subquery: &Subquery,
    inner_ctx: &TypeContext,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<(CheckedBlockAggregate, TypedExpr)> {
    let func = subquery.func;
    if matches!(
        subquery.op,
        CompOp::Contains | CompOp::StringContains | CompOp::StartsWith
    ) {
        return Err(CompilerError::typed(
            T40,
            "a block comparison requires equality or ordering",
        ));
    }
    let aggregate = typecheck_block_aggregate(catalog, subquery, inner_ctx, params)?;
    let result = match &aggregate {
        CheckedBlockAggregate::RowCount => PropType::scalar(ScalarType::I64, false),
        CheckedBlockAggregate::Aggregate { signature, .. } => match &signature.result {
            ExprType::Value {
                scalar,
                list,
                nullable,
            } => PropType {
                scalar: *scalar,
                nullable: *nullable,
                list: *list,
                enum_values: None,
            },
            ExprType::Node { type_name } => {
                return Err(CompilerError::Plan(format!(
                    "{func} over a block has node result type `{type_name}`"
                )));
            }
            ExprType::ExactInteger { .. } => {
                return Err(CompilerError::Plan(
                    "block aggregate exposes an internal carrier".into(),
                ));
            }
        },
    };
    let bound = match &subquery.right {
        Expr::Literal(_) | Expr::Now => true,
        Expr::Variable(name) => params.contains_key(name),
        _ => false,
    };
    if !bound {
        return Err(CompilerError::typed(
            T40,
            format!("{func} over a block compares with a literal, now() or a parameter"),
        ));
    }
    let mut right = resolve_expr(catalog, &subquery.right, ctx, params, Scope::Read)?;
    if let Some(contextual) =
        contextual_literal_type(&right, &ResolvedType::Scalar(result.clone()), Some(false))
    {
        right.set_ty(contextual);
    }
    let right_type = right.ty();
    let ResolvedType::Scalar(r) = &right_type else {
        return Err(CompilerError::typed(
            T40,
            format!(
                "{func} over a block compares with a scalar, got {}",
                right_type.display_name()
            ),
        ));
    };
    if !types_compatible(&result, r) {
        return Err(CompilerError::typed(
            T40,
            format!(
                "cannot compare {func} over a block ({}) with {}",
                result.display_name(),
                r.display_name()
            ),
        ));
    }
    Ok((aggregate, completed(right)?))
}

/// T1 through T3 over one binding, then its declaration; returns the typed
/// value of each inline match, in source order.
fn typecheck_binding(
    catalog: &Catalog,
    binding: &Binding,
    ctx: &mut TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<Vec<CheckedPropMatch>> {
    // T1: binding type must exist in catalog
    let Some(node_type) = catalog.query_node_type(&binding.type_name) else {
        return Err(CompilerError::typed(
            T1,
            format!("unknown node type `{}`", binding.type_name),
        ));
    };

    // T2 + T3: property match fields must exist and have correct types
    let mut matches = Vec::with_capacity(binding.prop_matches.len());
    for pm in &binding.prop_matches {
        let prop = node_type.properties.get(&pm.prop_name).ok_or_else(|| {
            CompilerError::typed(
                T2,
                format!(
                    "type `{}` has no property `{}`{}",
                    binding.type_name,
                    pm.prop_name,
                    if pm.prop_name == "id" {
                        format!(
                            "; filter the system identity with `${}.@id = ...` in the match block",
                            binding.variable
                        )
                    } else {
                        String::new()
                    }
                ),
            )
        })?;

        if matches!(prop.scalar, ScalarType::Blob) {
            return Err(CompilerError::typed(
                T3,
                format!(
                    "blob property `{}.{}` cannot be used in match patterns",
                    binding.type_name, pm.prop_name
                ),
            ));
        }

        let value = typecheck_constant(
            catalog,
            &pm.value,
            params,
            prop,
            &pm.prop_name,
            ConstantClause::BindingMatch,
            &binding.type_name,
        )?;
        matches.push(CheckedPropMatch {
            property: pm.prop_name.clone(),
            prop_type: prop.clone(),
            value,
        });
    }

    bind_node(binding, ctx)?;
    Ok(matches)
}

fn bind_node(binding: &Binding, ctx: &mut TypeContext) -> Result<()> {
    // Don't overwrite if already bound to the same node type (re-binding the
    // same node var is OK). Node and edge namespaces are independent, so a
    // matching type name does not make a cross-kind rebind valid.
    if let Some(existing) = ctx.bindings.get(&binding.variable) {
        match existing {
            BoundVariable::Edge { type_names } => {
                return Err(CompilerError::typed(
                    T23,
                    format!(
                        "variable `${}` is an edge binding ({}) and cannot be rebound as node type `{}`",
                        binding.variable,
                        if type_names.is_empty() {
                            "empty selection".to_string()
                        } else {
                            type_names.join(" | ")
                        },
                        binding.type_name
                    ),
                ));
            }
            BoundVariable::Node { type_name } => {
                if *type_name != binding.type_name {
                    return Err(CompilerError::typed(
                        T50,
                        format!(
                            "variable `${}` already bound to type `{}`, cannot rebind to `{}`",
                            binding.variable, type_name, binding.type_name
                        ),
                    ));
                }
            }
        }
    }

    ctx.bindings.insert(
        binding.variable.clone(),
        BoundVariable::Node {
            type_name: binding.type_name.clone(),
        },
    );

    Ok(())
}

/// A literal matched against a property: a list property matches by
/// membership of a scalar literal. `lit_type` is the type the resolver
/// computed for `lit`.
fn check_binding_literal_type(
    lit: &Literal,
    lit_type: &PropType,
    expected: &PropType,
    property: &str,
) -> Result<()> {
    if expected.list {
        if lit_type.list {
            return Err(CompilerError::typed(
                T3,
                format!(
                    "list equality is not supported for property `{}`; use a scalar value to match list membership",
                    property
                ),
            ));
        }

        let expected_member = PropType::scalar(expected.scalar, expected.nullable);
        if !types_compatible(lit_type, &expected_member) {
            return Err(CompilerError::typed(
                T3,
                format!(
                    "property `{}` has type {} but membership match got {}",
                    property,
                    expected.display_name(),
                    lit_type.display_name()
                ),
            ));
        }
        return Ok(());
    }

    check_literal_type(lit, lit_type, expected, property)
}

fn check_binding_variable_type(
    actual: &PropType,
    expected: &PropType,
    property: &str,
) -> Result<()> {
    if expected.list {
        if actual.list {
            return Err(CompilerError::typed(
                T7,
                format!(
                    "list equality is not supported for property `{}`; use a scalar parameter for membership matching",
                    property
                ),
            ));
        }

        let expected_member = PropType::scalar(expected.scalar, expected.nullable);
        if !types_compatible(actual, &expected_member) {
            return Err(CompilerError::typed(
                T7,
                format!(
                    "cannot compare {} membership against {} for property `{}`",
                    actual.display_name(),
                    expected.display_name(),
                    property
                ),
            ));
        }
        return Ok(());
    }

    if !types_compatible(actual, expected) {
        return Err(CompilerError::typed(
            T7,
            format!(
                "cannot assign/compare {} with {} for property `{}`",
                actual.display_name(),
                expected.display_name(),
                property
            ),
        ));
    }
    Ok(())
}

fn typecheck_traversal(
    catalog: &Catalog,
    traversal: &Traversal,
    ctx: &mut TypeContext,
    declared_nodes: &HashSet<String>,
    declarations: &HashMap<String, BoundVariable>,
) -> Result<ResolvedTraversal> {
    if matches!(traversal.selector, EdgeSelector::Wildcard)
        && (!declared_nodes.contains(&traversal.src) || !declared_nodes.contains(&traversal.dst))
    {
        return Err(CompilerError::typed(
            T5,
            "wildcard traversal requires explicitly declared source and destination node bindings"
                .to_string(),
        ));
    }
    let resolved = resolve_traversal(catalog, traversal, &ctx.bindings, declarations)?;
    if let Some(binding) = &resolved.edge_binding {
        if binding == &traversal.src || binding == &traversal.dst {
            return Err(CompilerError::typed(
                T23,
                format!(
                    "edge binding `${binding}` cannot reuse a traversal endpoint name; edge bindings and node endpoints need distinct names"
                ),
            ));
        }
        if let Some(existing) = ctx.bindings.get(binding) {
            let kind = match existing {
                BoundVariable::Node { .. } => "node",
                BoundVariable::Edge { .. } => "edge",
            };
            return Err(CompilerError::typed(
                T23,
                format!(
                    "variable `${binding}` is already a {kind} binding; an edge binding needs a fresh name"
                ),
            ));
        }
        ctx.bindings.insert(
            binding.clone(),
            BoundVariable::Edge {
                type_names: resolved
                    .edges
                    .members()
                    .iter()
                    .map(|member| member.edge_type.clone())
                    .collect(),
            },
        );
    }
    for (var, type_name) in [
        (&traversal.src, &resolved.src_type),
        (&traversal.dst, &resolved.dst_type),
    ] {
        if var != "_" {
            ctx.bindings
                .entry(var.clone())
                .or_insert_with(|| BoundVariable::Node {
                    type_name: type_name.clone(),
                });
        }
    }
    Ok(resolved)
}

fn resolve_traversal(
    catalog: &Catalog,
    traversal: &Traversal,
    bindings: &HashMap<String, BoundVariable>,
    declarations: &HashMap<String, BoundVariable>,
) -> Result<ResolvedTraversal> {
    if traversal.min_hops == 0 {
        return Err(CompilerError::typed(
            T15,
            "traversal min hop bound must be >= 1".to_string(),
        ));
    }
    if let Some(max_hops) = traversal.max_hops {
        if max_hops < traversal.min_hops {
            return Err(CompilerError::typed(
                T15,
                format!(
                    "invalid traversal bounds {{{},{}}}; max must be >= min",
                    traversal.min_hops, max_hops
                ),
            ));
        }
    } else {
        return Err(CompilerError::typed(
            T15,
            "unbounded traversal is disabled; use bounded traversal {min,max}".to_string(),
        ));
    }
    if let Some(binding) = &traversal.edge_binding
        && (traversal.min_hops != 1 || traversal.max_hops != Some(1))
    {
        return Err(CompilerError::typed(
            T23,
            format!(
                "edge binding `${binding}` cannot be combined with traversal bounds; a multi-hop traversal matches a path of edges, not one edge"
            ),
        ));
    }
    let src = bindings
        .get(&traversal.src)
        .or_else(|| declarations.get(&traversal.src))
        .filter(|_| traversal.src != "_")
        .map(|binding| binding.require_traversal_endpoint(&traversal.src))
        .transpose()?;
    let dst = bindings
        .get(&traversal.dst)
        .or_else(|| declarations.get(&traversal.dst))
        .filter(|_| traversal.dst != "_")
        .map(|binding| binding.require_traversal_endpoint(&traversal.dst))
        .transpose()?;
    let (edges, src_type, dst_type) = match &traversal.selector {
        EdgeSelector::Named(name) => {
            let edge = lookup_traversal_edge(catalog, name)?;
            let (member, src_type, dst_type) = resolve_member(edge, traversal, src, dst)?;
            (EdgeSelection::Named(member), src_type, dst_type)
        }
        EdgeSelector::Alternation(names) => {
            let Some((first, rest)) = names.split_first() else {
                return Err(CompilerError::typed(
                    T4,
                    "edge alternation must name at least one edge type".to_string(),
                ));
            };
            let edge = lookup_traversal_edge(catalog, first)?;
            if src.is_none() && dst.is_none() {
                for name in rest {
                    let other = lookup_traversal_edge(catalog, name)?;
                    if edge.from_type != other.from_type || edge.to_type != other.to_type {
                        return Err(CompilerError::typed(T5, "an edge alternation with mixed endpoint orientations requires a declared endpoint type".to_string(),
                        ));
                    }
                }
            }
            let (member, src_type, dst_type) = resolve_member(edge, traversal, src, dst)?;
            let mut seen = HashSet::from([member.edge_type.clone()]);
            let mut members = vec![member];
            for name in rest {
                let edge = lookup_traversal_edge(catalog, name)?;
                if seen.insert(edge.name.clone()) {
                    let (member, _, _) =
                        resolve_member(edge, traversal, Some(&src_type), Some(&dst_type))?;
                    members.push(member);
                }
            }
            members.sort_by(|left, right| left.edge_type.cmp(&right.edge_type));
            (EdgeSelection::Alternation(members), src_type, dst_type)
        }
        EdgeSelector::Wildcard => {
            let (Some(src), Some(dst)) = (src, dst) else {
                return Err(CompilerError::typed(
                    T5,
                    "wildcard traversal requires known source and destination node types"
                        .to_string(),
                ));
            };
            if traversal.undirected && src != dst {
                return Err(CompilerError::typed(
                    T22,
                    "undirected wildcard traversal requires the same node type at both endpoints"
                        .to_string(),
                ));
            }
            let mut members = Vec::new();
            for edge in catalog.edge_types.values() {
                if (edge.from_type == src && edge.to_type == dst)
                    || (edge.to_type == src && edge.from_type == dst)
                {
                    let (member, _, _) = resolve_member(edge, traversal, Some(src), Some(dst))?;
                    members.push(member);
                }
            }
            members.sort_by(|left, right| left.edge_type.cmp(&right.edge_type));
            (
                EdgeSelection::Wildcard(members),
                src.to_string(),
                dst.to_string(),
            )
        }
    };
    if traversal.src == traversal.dst && traversal.src != "_" && src_type != dst_type {
        return Err(CompilerError::typed(
            T5,
            format!(
                "traversal endpoint `${}` cannot have both type `{src_type}` and type `{dst_type}`",
                traversal.src
            ),
        ));
    }
    // A hop ends on the destination type, and the next hop must start on the
    // source type, so a path across distinct endpoint types never reaches a
    // second hop; a bound that allows one is refused, not silently capped.
    if let Some(max_hops) = traversal.max_hops
        && max_hops > 1
        && src_type != dst_type
    {
        let (EdgeSelector::Named(name), Some(member)) = (&traversal.selector, edges.named()) else {
            return Err(CompilerError::typed(
                T5,
                "recursive edge selection requires the same node type at both endpoints"
                    .to_string(),
            ));
        };
        let edge = lookup_traversal_edge(catalog, &member.edge_type)?;
        // Dropping the bound keeps the rows of a range that includes hop one;
        // a range starting past it matched nothing, so that rewrite would not.
        let fix = if traversal.min_hops == 1 {
            format!(
                "follow one hop without a bound: `${} {name} ${}`",
                traversal.src, traversal.dst
            )
        } else {
            format!(
                "no `{}` path reaches hop {}, so this pattern matches nothing; to go further, start another traversal at the `{dst_type}` endpoint",
                edge.name, traversal.min_hops
            )
        };
        return Err(CompilerError::query(
            QueryDiagnostic::typecheck(
                T5,
                format!(
                    "multi-hop traversal `{name}{{{},{max_hops}}}` requires the same node type at both endpoints, but `{}: {} -> {}` connects different types, so no path continues past one hop",
                    traversal.min_hops, edge.name, edge.from_type, edge.to_type
                ),
            )
            .with_fix(fix),
        ));
    }
    Ok(ResolvedTraversal {
        src: traversal.src.clone(),
        dst: traversal.dst.clone(),
        edges,
        src_type,
        dst_type,
        min_hops: traversal.min_hops,
        max_hops: traversal.max_hops,
        edge_binding: traversal
            .edge_binding
            .as_deref()
            .filter(|name| *name != "_")
            .map(str::to_string),
    })
}

fn lookup_traversal_edge<'a>(catalog: &'a Catalog, name: &str) -> Result<&'a EdgeType> {
    catalog
        .lookup_edge_by_name(name)
        .ok_or_else(|| CompilerError::typed(T4, format!("unknown edge type `{name}`")))
}

fn resolve_member(
    edge: &EdgeType,
    traversal: &Traversal,
    src: Option<&str>,
    dst: Option<&str>,
) -> Result<(EdgeMember, String, String)> {
    if traversal.undirected && edge.from_type != edge.to_type {
        return Err(CompilerError::typed(
            T22,
            format!(
                "undirected traversal `<{}>` requires a same-endpoint-type edge, but `{}: {} -> {}` is asymmetric; use the directional form",
                edge.name, edge.name, edge.from_type, edge.to_type
            ),
        ));
    }
    let direction = if let Some(src) = src {
        if src == edge.from_type {
            Direction::Out
        } else if src == edge.to_type {
            Direction::In
        } else {
            return Err(endpoint_type_error(&traversal.src, src, edge));
        }
    } else if let Some(dst) = dst {
        if dst == edge.to_type {
            Direction::Out
        } else if dst == edge.from_type {
            Direction::In
        } else {
            return Err(endpoint_type_error(&traversal.dst, dst, edge));
        }
    } else {
        Direction::Out
    };
    let (src_type, dst_type) = match direction {
        Direction::Out | Direction::Both => (&edge.from_type, &edge.to_type),
        Direction::In => (&edge.to_type, &edge.from_type),
    };
    if let Some(dst) = dst
        && dst != dst_type
    {
        return Err(CompilerError::typed(
            T5,
            format!(
                "endpoint `${}` resolves to type `{dst}` but edge `{}` expects `{dst_type}`",
                traversal.dst, edge.name
            ),
        ));
    }
    Ok((
        EdgeMember {
            edge_type: edge.name.clone(),
            direction: if traversal.undirected {
                Direction::Both
            } else {
                direction
            },
        },
        src_type.clone(),
        dst_type.clone(),
    ))
}

fn endpoint_type_error(var: &str, type_name: &str, edge: &EdgeType) -> CompilerError {
    CompilerError::typed(
        T5,
        format!(
            "endpoint `${var}` resolves to type `{type_name}`, which is not an endpoint of edge `{}: {} -> {}`",
            edge.name, edge.from_type, edge.to_type
        ),
    )
}

/// A match filter is an expression the checker proves Boolean; a search
/// call stands only as a top-level conjunct, bare or `= true` (T38), and the
/// retained filter spells every bare call `call = true`, both non-null Bool.
fn typecheck_filter(
    catalog: &Catalog,
    filter: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<TypedExpr> {
    if filter
        .conjuncts()
        .into_iter()
        .any(|conjunct| !conjunct.is_search_predicate() && contains_search_call(conjunct))
    {
        return Err(CompilerError::typed(T38, SEARCH_PREDICATE_SHAPE));
    }
    let typed = resolve_expr(catalog, filter, ctx, params, Scope::Read)?;
    let resolved = typed.ty();
    if boolean_scalar(&resolved).is_none() {
        return Err(CompilerError::typed(
            T41,
            format!("a filter must be Boolean, got {}", resolved.display_name()),
        ));
    }
    Ok(spell_search_predicates(completed(typed)?))
}

/// The filter with every bare search call among its top-level conjuncts
/// spelled `call = true`.
fn spell_search_predicates(filter: TypedExpr) -> TypedExpr {
    let boolean = || ResolvedType::Scalar(PropType::scalar(ScalarType::Bool, false));
    match filter {
        call if call.is_search_call() => TypedExpr::Binary {
            left: Box::new(call),
            op: BinaryOp::Compare(CompOp::Eq),
            right: Box::new(TypedExpr::Literal {
                literal: Literal::Bool(true),
                ty: boolean(),
            }),
            ty: boolean(),
        },
        TypedExpr::Binary {
            left,
            op: BinaryOp::And,
            right,
            ty,
        } => TypedExpr::Binary {
            left: Box::new(spell_search_predicates(*left)),
            op: BinaryOp::And,
            right: Box::new(spell_search_predicates(*right)),
            ty,
        },
        other => other,
    }
}

const SEARCH_PREDICATE_SHAPE: &str =
    "search predicates require a standalone call or `= true`, alone or joined by and";

/// Whether a `search`, `fuzzy` or `match_text` call stands anywhere in `expr`.
fn contains_search_call(expr: &Expr) -> bool {
    expr.is_search_call()
        || match expr {
            Expr::Binary { left, right, .. }
            | Expr::In {
                needle: left,
                list: right,
            } => contains_search_call(left) || contains_search_call(right),
            Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => contains_search_call(inner),
            Expr::Aggregate { arg, .. } => contains_search_call(arg),
            Expr::Nearest { query, .. } => contains_search_call(query),
            Expr::Bm25 { field, query } => {
                contains_search_call(field) || contains_search_call(query)
            }
            Expr::Rrf {
                primary,
                secondary,
                k,
            } => {
                contains_search_call(primary)
                    || contains_search_call(secondary)
                    || k.as_deref().is_some_and(contains_search_call)
            }
            Expr::Now
            | Expr::PropAccess { .. }
            | Expr::Search { .. }
            | Expr::Fuzzy { .. }
            | Expr::MatchText { .. }
            | Expr::Variable(_)
            | Expr::Literal(_)
            | Expr::AliasRef(_) => false,
        }
}

/// The type when `resolved` is a scalar, non-list `Bool`.
fn boolean_scalar(resolved: &ResolvedType) -> Option<&PropType> {
    match resolved {
        ResolvedType::Scalar(t) if !t.list && t.scalar == ScalarType::Bool => Some(t),
        _ => None,
    }
}

/// Infer a type for direct nulls and empty/all-null literal lists from another operand.
fn contextual_literal_type(
    expr: &TypedExpr,
    other: &ResolvedType,
    list: Option<bool>,
) -> Option<ResolvedType> {
    let TypedExpr::Literal { literal, .. } = expr else {
        return None;
    };
    let nullable = match literal {
        Literal::Null => true,
        Literal::List(items) if items.iter().all(|item| matches!(item, Literal::Null)) => false,
        _ => return None,
    };
    let ResolvedType::Scalar(other) = other else {
        return None;
    };
    let mut prop = other.clone();
    prop.nullable = nullable;
    prop.list = matches!(literal, Literal::List(_)) || list.unwrap_or(other.list);
    prop.enum_values = None;
    Some(ResolvedType::Scalar(prop))
}

/// A direct null under a Boolean operator in a read is a nullable Bool.
fn contextual_boolean(expr: &mut TypedExpr, scope: Scope<'_>) {
    if scope.is_read()
        && matches!(
            expr,
            TypedExpr::Literal {
                literal: Literal::Null,
                ..
            }
        )
    {
        expr.set_ty(ResolvedType::Scalar(PropType::scalar(
            ScalarType::Bool,
            true,
        )));
    }
}

/// In a return item or order key an aggregate operand is its per-group value;
/// match filters retain the aggregate type T7 refuses.
fn aggregate_value_type(ty: ResolvedType, scope: Scope<'_>) -> ResolvedType {
    match ty {
        ResolvedType::Aggregate(ExprType::Value {
            scalar,
            list,
            nullable,
        }) if matches!(scope, Scope::Return) => ResolvedType::Scalar(PropType {
            scalar,
            nullable,
            list,
            enum_values: None,
        }),
        other => other,
    }
}

/// `left <op> right` over two resolved operands: the operand rules of a
/// comparison (T7, T38) and its type, `Bool`, nullable when an operand is; in
/// a mutation `where`, a target property against a constant keeps the T3/T7 texts.
fn typecheck_comparison(
    left: &mut TypedExpr,
    op: CompOp,
    right: &mut TypedExpr,
    scope: Scope<'_>,
) -> Result<PropType> {
    let mut left_type = aggregate_value_type(left.ty(), scope);
    let mut right_type = aggregate_value_type(right.ty(), scope);
    if scope.is_read() {
        let left_is_null = matches!(
            left,
            TypedExpr::Literal {
                literal: Literal::Null,
                ..
            }
        );
        let member = op == CompOp::Contains
            && !matches!(&left_type, ResolvedType::Scalar(PropType { scalar: ScalarType::String, list: false, .. })
                if !left_is_null
                    || matches!(&right_type, ResolvedType::Scalar(PropType { scalar: ScalarType::String, list: false, .. })));
        if let Some(contextual) = contextual_literal_type(left, &right_type, member.then_some(true))
        {
            left.set_ty(contextual.clone());
            left_type = contextual;
        }
        if let Some(contextual) =
            contextual_literal_type(right, &left_type, member.then_some(false))
        {
            right.set_ty(contextual.clone());
            right_type = contextual;
        }
    }

    if (left.is_search_call() || right.is_search_call())
        && !(left.is_search_call()
            && op == CompOp::Eq
            && matches!(
                right,
                TypedExpr::Literal {
                    literal: Literal::Bool(true),
                    ..
                }
            ))
    {
        return Err(CompilerError::typed(T38, SEARCH_PREDICATE_SHAPE));
    }

    if let Scope::MutationWhere(target) = scope
        && let TypedExpr::PropAccess {
            variable, property, ..
        } = &*left
        && variable == target.type_name()
        && !matches!(
            op,
            CompOp::Contains | CompOp::StartsWith | CompOp::StringContains
        )
        && matches!(
            right,
            TypedExpr::Literal { .. } | TypedExpr::Variable { .. } | TypedExpr::Now { .. }
        )
        && let (ResolvedType::Scalar(expected), ResolvedType::Scalar(actual)) =
            (&left_type, &right_type)
    {
        check_constant(right, expected, property, ConstantClause::Assignment)?;
        return Ok(PropType::scalar(
            ScalarType::Bool,
            expected.nullable || actual.nullable,
        ));
    }

    if let (ResolvedType::Scalar(l), ResolvedType::Scalar(r)) = (&left_type, &right_type) {
        let result = PropType::scalar(ScalarType::Bool, l.nullable || r.nullable);
        // Blob values never participate in `.gq` filters. Keep this ahead of
        // every operator-specific early return so public-AST callers cannot
        // bypass containment with a list-membership shape such as
        // `[Blob] contains Blob`.
        if matches!(l.scalar, ScalarType::Blob) || matches!(r.scalar, ScalarType::Blob) {
            return Err(CompilerError::typed(
                T7,
                "blob comparisons in filters are not supported".to_string(),
            ));
        }

        if op == CompOp::Contains {
            // Overloaded on the left operand: list → membership, scalar
            // String → exact substring. Lowering resolves the String form to
            // `StringContains` so execution never re-derives the dispatch.
            if !l.list && matches!(l.scalar, ScalarType::String) {
                if r.list || !matches!(r.scalar, ScalarType::String) {
                    return Err(CompilerError::typed(
                        T7,
                        format!(
                            "string contains requires a String right operand, got {}",
                            r.display_name()
                        ),
                    ));
                }
                return Ok(result);
            }
            if !l.list {
                return Err(CompilerError::typed(
                    T7,
                    format!(
                        "contains requires a list property (membership) or a String property (substring) on the left, got {}",
                        l.display_name()
                    ),
                ));
            }
            if r.list {
                return Err(CompilerError::typed(
                    T7,
                    "contains requires a scalar right operand".to_string(),
                ));
            }
            if matches!(l.scalar, ScalarType::Vector(_))
                || matches!(r.scalar, ScalarType::Vector(_))
            {
                return Err(CompilerError::typed(
                    T7,
                    "vector membership filters are not supported".to_string(),
                ));
            }

            let empty_list_literal = matches!(
                left,
                TypedExpr::Literal {
                    literal: Literal::List(items),
                    ..
                } if items.is_empty()
            );
            let expected_member = PropType::scalar(l.scalar, l.nullable);
            if !empty_list_literal && !types_compatible(&expected_member, r) {
                return Err(CompilerError::typed(
                    T7,
                    format!(
                        "cannot test membership of {} in {}",
                        r.display_name(),
                        l.display_name()
                    ),
                ));
            }
            return Ok(result);
        }

        if matches!(op, CompOp::StartsWith | CompOp::StringContains) {
            // Exact, case-sensitive string predicates: scalar String on both
            // sides. (`StringContains` only exists post-lowering, but the
            // check is written over both ops so re-typechecking IR-shaped
            // input stays consistent.)
            if l.list || !matches!(l.scalar, ScalarType::String) {
                return Err(CompilerError::typed(
                    T7,
                    format!(
                        "{} requires a String property on the left, got {}",
                        op,
                        l.display_name()
                    ),
                ));
            }
            if r.list || !matches!(r.scalar, ScalarType::String) {
                return Err(CompilerError::typed(
                    T7,
                    format!(
                        "{} requires a String right operand, got {}",
                        op,
                        r.display_name()
                    ),
                ));
            }
            return Ok(result);
        }

        // T7: check type compatibility
        if l.list || r.list {
            return Err(CompilerError::typed(
                T7,
                "list comparisons in filters are not supported; use `contains` for list membership"
                    .to_string(),
            ));
        }
        if matches!(l.scalar, ScalarType::Vector(_)) || matches!(r.scalar, ScalarType::Vector(_)) {
            return Err(CompilerError::typed(
                T7,
                "vector comparisons in filters are not supported".to_string(),
            ));
        }
        if !types_compatible(l, r) {
            return Err(CompilerError::typed(
                T7,
                format!(
                    "cannot compare {} with {}",
                    l.display_name(),
                    r.display_name()
                ),
            ));
        }
        Ok(result)
    } else {
        Err(CompilerError::typed(
            T7,
            format!(
                "filter comparisons require scalar operands, got {} and {}",
                left_type.display_name(),
                right_type.display_name()
            ),
        ))
    }
}

/// Search/rank filters are hoisted onto the field variable's NodeScan; an
/// edge binding has none, so accepting one here would silently drop the
/// filter. Reject at typecheck instead.
fn reject_edge_binding_search_field(ctx: &TypeContext, field: &Expr, func: &str) -> Result<()> {
    if let Expr::PropAccess { variable, property } = field
        && let Some(bv) = ctx.bindings.get(variable)
    {
        match bv {
            BoundVariable::Node { type_name } => {
                reject_catalogue_search(type_name, variable, func)?;
            }
            BoundVariable::Edge { .. } => {
                return Err(CompilerError::typed(
                    T23,
                    format!(
                        "{} cannot target edge property `${}.{}`; text/rank search runs on node properties — edge properties support comparison filters and projection",
                        func, variable, property
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// A catalogue type has no index: text and rank search over its rows is
/// refused at the check, before any storage planning. Scalar predicates,
/// projection, aggregation, ordering and joins stay available.
fn reject_catalogue_search(type_name: &str, variable: &str, func: &str) -> Result<()> {
    if crate::catalog::catalogue::is_catalogue_type_name(type_name) {
        return Err(CompilerError::typed(
            T23,
            format!(
                "{func} cannot target catalogue binding `${variable}: {type_name}`; catalogue rows have no search index"
            ),
        ));
    }
    Ok(())
}

/// `$variable.property` in a read: the meta-field's type by role, or the
/// declared property type of the node or edge the variable is bound to
/// (T6). Blob is the caller's decision: T24 as a value, T7 under `is null`.
fn read_property_type(
    catalog: &Catalog,
    ctx: &TypeContext,
    variable: &str,
    property: &str,
) -> Result<PropType> {
    let bv = ctx
        .bindings
        .get(variable)
        .ok_or_else(|| CompilerError::typed(T6, format!("variable `${variable}` is not bound")))?;

    if let Some(role) = meta_field_role(property) {
        let admitted = match (bv, role) {
            (_, Some(MetaField::System(SystemFieldRole::Id))) => true,
            (BoundVariable::Edge { .. }, Some(_)) => true,
            (BoundVariable::Node { .. }, Some(_)) | (_, None) => false,
        };
        if !admitted {
            let known = match bv {
                BoundVariable::Node { .. } => "`@id`".to_string(),
                BoundVariable::Edge { .. } => format!("`@id`, `@src`, `@dst`, `{EDGE_TYPE_META}`"),
            };
            return Err(CompilerError::typed(
                T6,
                format!(
                    "binding `${variable}` has no meta-field `{property}`; its meta-fields are {known}"
                ),
            ));
        }
        return Ok(meta_field_type());
    }

    let prop = match bv {
        BoundVariable::Node { type_name } => {
            let node_type = catalog.query_node_type(type_name).ok_or_else(|| {
                CompilerError::typed(T6, format!("type `{}` not found in catalog", type_name))
            })?;
            node_type.properties.get(property).ok_or_else(|| {
                CompilerError::typed(
                    T6,
                    format!(
                        "type `{}` has no property `{}`{}",
                        type_name,
                        property,
                        system_field_hint(property, Some(variable), false)
                    ),
                )
            })?
        }
        BoundVariable::Edge { type_names } => {
            return common_edge_property(catalog, type_names, property).ok_or_else(|| {
                let detail = if let [type_name] = type_names.as_slice() {
                    format!("edge `{type_name}` has no property `{property}`")
                } else if type_names.is_empty() {
                    format!("empty wildcard has no inferable property `{property}`")
                } else {
                    format!("property `{property}` must exist with compatible types on every selected edge ({})", type_names.join(" | "))
                };
                CompilerError::typed(T6, format!("{detail}{}", system_field_hint(property, Some(variable), true)))
            });
        }
    };
    Ok(prop.clone())
}

/// An aggregate's argument resolved once, with the signature derived from
/// its type (T8); the result is `nullable` as the caller's position demands.
fn resolve_aggregate(
    catalog: &Catalog,
    func: AggFunc,
    arg: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
    nullable: bool,
) -> Result<(TypedExpr, AggSignature)> {
    let typed_arg = resolve_expr(catalog, arg, ctx, params, Scope::Read)?;
    let resolved_arg = typed_arg.ty();
    reject_blob_read_value(&resolved_arg, arg)?;
    let arg = check_aggregate_argument(&func, arg, &resolved_arg)?;
    let scalar = func.result_type(&arg).ok_or_else(|| {
        CompilerError::Plan(format!(
            "{func} has no result type for argument {}",
            arg.spelling()
        ))
    })?;
    Ok((
        typed_arg,
        AggSignature {
            arg,
            result: ExprType::from_prop(&PropType::scalar(scalar, nullable)),
        },
    ))
}

/// The one recursive resolver: every expression occurrence passes through
/// here exactly once per checking invocation and comes back as a typed node
/// that retains its children; helpers read a child's type, never resolve it again.
fn resolve_expr(
    catalog: &Catalog,
    expr: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
    scope: Scope<'_>,
) -> Result<TypedExpr> {
    visits::expr(expr);
    if let Some(keyword) = call_keyword(expr)
        && let Some(refusal) = scope.call_refusal(&keyword)
    {
        return Err(refusal);
    }
    let scalar = |scalar: ScalarType, nullable: bool| {
        ResolvedType::Scalar(PropType::scalar(scalar, nullable))
    };
    match expr {
        Expr::Now => Ok(TypedExpr::Now {
            ty: scalar(ScalarType::DateTime, false),
        }),
        Expr::PropAccess { variable, property } => {
            let ty = match scope {
                Scope::Read | Scope::Return => {
                    let prop = read_property_type(catalog, ctx, variable, property)?;
                    if matches!(prop.scalar, ScalarType::Blob) {
                        return Err(CompilerError::typed(
                            T24,
                            format!(
                                "Blob property `${}.{}` is not available as a .gq read value; Blob values require a dedicated API",
                                variable, property
                            ),
                        ));
                    }
                    ResolvedType::Scalar(prop)
                }
                Scope::MutationWhere(target) => ResolvedType::Scalar(mutation_property_type(
                    catalog, target, variable, property,
                )?),
                Scope::Constant { type_name, .. } => {
                    return Err(constant_leaf_refusal(type_name, variable, property));
                }
            };
            Ok(TypedExpr::PropAccess {
                variable: variable.clone(),
                property: property.clone(),
                ty,
            })
        }
        Expr::Nearest {
            variable,
            property,
            query,
        } => {
            let node_type_name = match ctx.bindings.get(variable) {
                Some(BoundVariable::Node { type_name }) => type_name,
                Some(BoundVariable::Edge { .. }) => {
                    return Err(CompilerError::typed(
                        T23,
                        format!(
                            "nearest cannot target edge binding `${}`; vector search runs on node properties",
                            variable
                        ),
                    ));
                }
                None => {
                    return Err(CompilerError::typed(
                        T15,
                        format!("variable `${}` is not bound", variable),
                    ));
                }
            };
            reject_catalogue_search(node_type_name, variable, "nearest")?;
            let node_type = catalog.query_node_type(node_type_name).ok_or_else(|| {
                CompilerError::typed(
                    T15,
                    format!("type `{}` not found in catalog", node_type_name),
                )
            })?;
            let prop_type = node_type.properties.get(property).ok_or_else(|| {
                CompilerError::typed(
                    T15,
                    format!(
                        "type `{}` has no property `{}`{}",
                        node_type_name,
                        property,
                        system_field_hint(property, Some(variable), false)
                    ),
                )
            })?;
            let vector_dim = match prop_type.scalar {
                ScalarType::Vector(dim) => dim,
                _ => {
                    return Err(CompilerError::typed(
                        T15,
                        format!(
                            "nearest requires a Vector property, got {}.{}: {}",
                            node_type_name,
                            property,
                            prop_type.display_name()
                        ),
                    ));
                }
            };
            if prop_type.list {
                return Err(CompilerError::typed(
                    T15,
                    "nearest does not support list-wrapped vectors".to_string(),
                ));
            }

            let query = resolve_expr(catalog, query, ctx, params, scope)?;
            let literal_dim = match &query {
                TypedExpr::Literal { literal, .. } => numeric_vector_literal_dim(literal),
                _ => None,
            };
            if let Some(dim) = literal_dim {
                if dim != vector_dim {
                    return Err(CompilerError::typed(
                        T15,
                        format!(
                            "nearest vector dimension mismatch: property is Vector({}), query literal has {} elements",
                            vector_dim, dim
                        ),
                    ));
                }
            } else {
                match query.ty() {
                    ResolvedType::Scalar(s)
                        if matches!(s.scalar, ScalarType::Vector(_)) && !s.list =>
                    {
                        let qdim = match s.scalar {
                            ScalarType::Vector(dim) => dim,
                            _ => unreachable!(),
                        };
                        if qdim != vector_dim {
                            return Err(CompilerError::typed(
                                T15,
                                format!(
                                    "nearest vector dimension mismatch: property is Vector({}), query is Vector({})",
                                    vector_dim, qdim
                                ),
                            ));
                        }
                    }
                    ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                    ResolvedType::Scalar(s) => {
                        return Err(CompilerError::typed(
                            T15,
                            format!(
                                "nearest query must be Vector({}) or String, got {}",
                                vector_dim,
                                s.display_name()
                            ),
                        ));
                    }
                    _ => {
                        return Err(CompilerError::typed(
                            T15,
                            "nearest query must be a scalar expression".to_string(),
                        ));
                    }
                }
            }

            Ok(TypedExpr::Nearest {
                variable: variable.clone(),
                property: property.clone(),
                query: Box::new(query),
                ty: scalar(ScalarType::F32, false),
            })
        }
        Expr::Search { field, query } => {
            reject_edge_binding_search_field(ctx, field, "search")?;
            let field = resolve_expr(catalog, field, ctx, params, scope)?;
            match field.ty() {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(CompilerError::typed(
                        T19,
                        format!("search field must be String, got {}", s.display_name()),
                    ));
                }
                _ => {
                    return Err(CompilerError::typed(
                        T19,
                        "search field must be a scalar String expression".to_string(),
                    ));
                }
            }

            let query = resolve_expr(catalog, query, ctx, params, scope)?;
            match query.ty() {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(CompilerError::typed(
                        T19,
                        format!("search query must be String, got {}", s.display_name()),
                    ));
                }
                _ => {
                    return Err(CompilerError::typed(
                        T19,
                        "search query must be a scalar String expression".to_string(),
                    ));
                }
            }

            Ok(TypedExpr::Search {
                field: Box::new(field),
                query: Box::new(query),
                ty: scalar(ScalarType::Bool, false),
            })
        }
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            reject_edge_binding_search_field(ctx, field, "fuzzy")?;
            let field = resolve_expr(catalog, field, ctx, params, scope)?;
            match field.ty() {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(CompilerError::typed(
                        T19,
                        format!("fuzzy field must be String, got {}", s.display_name()),
                    ));
                }
                _ => {
                    return Err(CompilerError::typed(
                        T19,
                        "fuzzy field must be a scalar String expression".to_string(),
                    ));
                }
            }

            let query = resolve_expr(catalog, query, ctx, params, scope)?;
            match query.ty() {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(CompilerError::typed(
                        T19,
                        format!("fuzzy query must be String, got {}", s.display_name()),
                    ));
                }
                _ => {
                    return Err(CompilerError::typed(
                        T19,
                        "fuzzy query must be a scalar String expression".to_string(),
                    ));
                }
            }

            let mut max_edits_typed = None;
            if let Some(max_edits_expr) = max_edits {
                let max_edits = resolve_expr(catalog, max_edits_expr, ctx, params, scope)?;
                match max_edits.ty() {
                    ResolvedType::Scalar(s)
                        if !s.list
                            && matches!(
                                s.scalar,
                                ScalarType::I32
                                    | ScalarType::I64
                                    | ScalarType::U32
                                    | ScalarType::U64
                            ) => {}
                    ResolvedType::Scalar(s) => {
                        return Err(CompilerError::typed(
                            T19,
                            format!(
                                "fuzzy max_edits must be an integer scalar, got {}",
                                s.display_name()
                            ),
                        ));
                    }
                    _ => {
                        return Err(CompilerError::typed(
                            T19,
                            "fuzzy max_edits must be an integer scalar expression".to_string(),
                        ));
                    }
                }
                max_edits_typed = Some(Box::new(max_edits));
            }

            Ok(TypedExpr::Fuzzy {
                field: Box::new(field),
                query: Box::new(query),
                max_edits: max_edits_typed,
                ty: scalar(ScalarType::Bool, false),
            })
        }
        Expr::MatchText { field, query } => {
            reject_edge_binding_search_field(ctx, field, "match_text")?;
            let field = resolve_expr(catalog, field, ctx, params, scope)?;
            match field.ty() {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(CompilerError::typed(
                        T20,
                        format!("match_text field must be String, got {}", s.display_name()),
                    ));
                }
                _ => {
                    return Err(CompilerError::typed(
                        T20,
                        "match_text field must be a scalar String expression".to_string(),
                    ));
                }
            }

            let query = resolve_expr(catalog, query, ctx, params, scope)?;
            match query.ty() {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(CompilerError::typed(
                        T20,
                        format!("match_text query must be String, got {}", s.display_name()),
                    ));
                }
                _ => {
                    return Err(CompilerError::typed(
                        T20,
                        "match_text query must be a scalar String expression".to_string(),
                    ));
                }
            }

            Ok(TypedExpr::MatchText {
                field: Box::new(field),
                query: Box::new(query),
                ty: scalar(ScalarType::Bool, false),
            })
        }
        Expr::Bm25 { field, query } => {
            reject_edge_binding_search_field(ctx, field, "bm25")?;
            let field = resolve_expr(catalog, field, ctx, params, scope)?;
            match field.ty() {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(CompilerError::typed(
                        T20,
                        format!("bm25 field must be String, got {}", s.display_name()),
                    ));
                }
                _ => {
                    return Err(CompilerError::typed(
                        T20,
                        "bm25 field must be a scalar String expression".to_string(),
                    ));
                }
            }

            let query = resolve_expr(catalog, query, ctx, params, scope)?;
            match query.ty() {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(CompilerError::typed(
                        T20,
                        format!("bm25 query must be String, got {}", s.display_name()),
                    ));
                }
                _ => {
                    return Err(CompilerError::typed(
                        T20,
                        "bm25 query must be a scalar String expression".to_string(),
                    ));
                }
            }

            Ok(TypedExpr::Bm25 {
                field: Box::new(field),
                query: Box::new(query),
                ty: scalar(ScalarType::F32, false),
            })
        }
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => {
            if !matches!(primary.as_ref(), Expr::Nearest { .. } | Expr::Bm25 { .. }) {
                return Err(CompilerError::typed(
                    T21,
                    "rrf primary expression must be nearest(...) or bm25(...)".to_string(),
                ));
            }
            if !matches!(secondary.as_ref(), Expr::Nearest { .. } | Expr::Bm25 { .. }) {
                return Err(CompilerError::typed(
                    T21,
                    "rrf secondary expression must be nearest(...) or bm25(...)".to_string(),
                ));
            }

            let primary = resolve_expr(catalog, primary, ctx, params, scope)?;
            let secondary = resolve_expr(catalog, secondary, ctx, params, scope)?;

            for ty in [primary.ty(), secondary.ty()] {
                match ty {
                    ResolvedType::Scalar(s) if s.scalar == ScalarType::F32 && !s.list => {}
                    ResolvedType::Scalar(s) => {
                        return Err(CompilerError::typed(
                            T21,
                            format!(
                                "rrf rank expressions must evaluate to F32, got {}",
                                s.display_name()
                            ),
                        ));
                    }
                    _ => {
                        return Err(CompilerError::typed(
                            T21,
                            "rrf rank expressions must be scalar numeric expressions".to_string(),
                        ));
                    }
                }
            }

            let mut k_typed = None;
            if let Some(k_expr) = k {
                let k = resolve_expr(catalog, k_expr, ctx, params, scope)?;
                match k.ty() {
                    ResolvedType::Scalar(s)
                        if !s.list
                            && matches!(
                                s.scalar,
                                ScalarType::I32
                                    | ScalarType::I64
                                    | ScalarType::U32
                                    | ScalarType::U64
                            ) => {}
                    ResolvedType::Scalar(s) => {
                        return Err(CompilerError::typed(
                            T21,
                            format!("rrf k must be an integer scalar, got {}", s.display_name()),
                        ));
                    }
                    _ => {
                        return Err(CompilerError::typed(
                            T21,
                            "rrf k must be an integer scalar expression".to_string(),
                        ));
                    }
                }
                if let TypedExpr::Literal {
                    literal: Literal::Integer(v),
                    ..
                } = &k
                    && *v <= 0
                {
                    return Err(CompilerError::typed(
                        T21,
                        "rrf k must be greater than 0".to_string(),
                    ));
                }
                k_typed = Some(Box::new(k));
            }

            Ok(TypedExpr::Rrf {
                primary: Box::new(primary),
                secondary: Box::new(secondary),
                k: k_typed,
                ty: scalar(ScalarType::F64, false),
            })
        }
        Expr::Variable(name) => {
            // Could be a query parameter or a bound variable
            let ty = if let Some(prop_type) = params.get(name) {
                ResolvedType::Scalar(prop_type.clone())
            } else if !scope.is_read() {
                return Err(scope.undeclared_parameter(name));
            } else if let Some(bv) = ctx.bindings.get(name) {
                match bv {
                    BoundVariable::Node { type_name } => ResolvedType::Node(type_name.clone()),
                    BoundVariable::Edge { .. } => {
                        return Err(CompilerError::typed(
                            T23,
                            format!(
                                "edge binding `${}` cannot be used bare; access one of its properties (`${}.{{prop}}`)",
                                name, name
                            ),
                        ));
                    }
                }
            } else {
                return Err(CompilerError::typed(
                    T6,
                    format!("variable `${}` is not bound", name),
                ));
            };
            Ok(TypedExpr::Variable {
                name: name.clone(),
                ty,
            })
        }
        Expr::Literal(lit) => Ok(TypedExpr::Literal {
            literal: lit.clone(),
            ty: ResolvedType::Scalar(literal_type(lit)?),
        }),
        Expr::Aggregate {
            func,
            distinct,
            arg,
        } => {
            if *distinct && *func != AggFunc::Count {
                return Err(CompilerError::typed(
                    T57,
                    format!(
                        "`distinct` applies only to `count`; `{func}(distinct …)` is not supported"
                    ),
                ));
            }
            let (arg, signature) = resolve_aggregate(catalog, *func, arg, ctx, params, true)?;
            Ok(TypedExpr::Aggregate {
                func: *func,
                distinct: *distinct,
                arg: Box::new(arg),
                signature,
            })
        }
        Expr::AliasRef(name) => {
            let ty = match scope {
                Scope::Read | Scope::Return => ctx
                    .aliases
                    .get(name)
                    .cloned()
                    .unwrap_or(ResolvedType::ForwardAlias),
                Scope::MutationWhere(target) => ResolvedType::Scalar(mutation_property_type(
                    catalog,
                    target,
                    target.type_name(),
                    name,
                )?),
                Scope::Constant { type_name, .. } => {
                    return Err(constant_leaf_refusal(type_name, type_name, name));
                }
            };
            Ok(TypedExpr::AliasRef {
                name: name.clone(),
                ty,
            })
        }
        Expr::Binary {
            left,
            op: BinaryOp::Compare(op),
            right,
        } => {
            let mut left = resolve_expr(catalog, left, ctx, params, scope)?;
            let mut right = resolve_expr(catalog, right, ctx, params, scope)?;
            let ty = typecheck_comparison(&mut left, *op, &mut right, scope)?;
            Ok(TypedExpr::Binary {
                left: Box::new(left),
                op: BinaryOp::Compare(*op),
                right: Box::new(right),
                ty: ResolvedType::Scalar(ty),
            })
        }
        Expr::In { needle, list } => {
            if let Some(refusal) = membership_over_a_node(catalog, needle, list, ctx, params) {
                return Err(refusal);
            }
            let mut list = resolve_expr(catalog, list, ctx, params, scope)?;
            let list_type = list.ty();
            if !matches!(&list_type, ResolvedType::Scalar(list) if list.list)
                && !(scope.is_read()
                    && matches!(
                        list,
                        TypedExpr::Literal {
                            literal: Literal::Null,
                            ..
                        }
                    ))
            {
                return Err(CompilerError::typed(
                    T7,
                    format!(
                        "`in` needs a list on the right, got {}",
                        list_type.display_name()
                    ),
                ));
            }
            let mut needle = resolve_expr(catalog, needle, ctx, params, scope)?;
            let ty = typecheck_comparison(&mut list, CompOp::Contains, &mut needle, scope)?;
            Ok(TypedExpr::In {
                needle: Box::new(needle),
                list: Box::new(list),
                ty: ResolvedType::Scalar(ty),
            })
        }
        Expr::Binary { left, op, right } => {
            let mut left = resolve_expr(catalog, left, ctx, params, scope)?;
            contextual_boolean(&mut left, scope);
            let mut right = resolve_expr(catalog, right, ctx, params, scope)?;
            contextual_boolean(&mut right, scope);
            let (left_type, right_type) = (left.ty(), right.ty());
            let (Some(l), Some(r)) = (boolean_scalar(&left_type), boolean_scalar(&right_type))
            else {
                return Err(CompilerError::typed(
                    T41,
                    format!(
                        "`{op}` needs Bool operands, got {} and {}",
                        left_type.display_name(),
                        right_type.display_name()
                    ),
                ));
            };
            let ty = scalar(ScalarType::Bool, l.nullable || r.nullable);
            Ok(TypedExpr::Binary {
                left: Box::new(left),
                op: *op,
                right: Box::new(right),
                ty,
            })
        }
        Expr::Not(inner) => {
            let mut inner = resolve_expr(catalog, inner, ctx, params, scope)?;
            contextual_boolean(&mut inner, scope);
            let inner_type = inner.ty();
            let Some(b) = boolean_scalar(&inner_type) else {
                return Err(CompilerError::typed(
                    T41,
                    format!(
                        "`not` needs a Bool operand, got {}",
                        inner_type.display_name()
                    ),
                ));
            };
            let ty = scalar(ScalarType::Bool, b.nullable);
            Ok(TypedExpr::Not {
                inner: Box::new(inner),
                ty,
            })
        }
        Expr::IsNull {
            expr: inner,
            negated,
        } => {
            let inner = if scope.is_read()
                && let Expr::PropAccess { variable, property } = inner.as_ref()
            {
                visits::expr(inner);
                let prop = read_property_type(catalog, ctx, variable, property)?;
                if prop.scalar == ScalarType::Blob {
                    return Err(CompilerError::typed(
                        T7,
                        "blob comparisons in filters are not supported".to_string(),
                    ));
                }
                TypedExpr::PropAccess {
                    variable: variable.clone(),
                    property: property.clone(),
                    ty: ResolvedType::Scalar(prop),
                }
            } else {
                resolve_expr(catalog, inner, ctx, params, scope)?
            };
            let inner_type = aggregate_value_type(inner.ty(), scope);
            if !matches!(inner_type, ResolvedType::Scalar(_)) {
                return Err(CompilerError::typed(
                    T41,
                    format!(
                        "`is null` tests a scalar or list value, got {}",
                        inner_type.display_name()
                    ),
                ));
            }
            Ok(TypedExpr::IsNull {
                inner: Box::new(inner),
                negated: *negated,
                ty: scalar(ScalarType::Bool, false),
            })
        }
    }
}

fn reject_blob_read_value(resolved: &ResolvedType, expr: &Expr) -> Result<()> {
    if matches!(
        resolved,
        ResolvedType::Scalar(PropType {
            scalar: ScalarType::Blob,
            ..
        })
    ) {
        let subject = match expr {
            Expr::Variable(name) => format!("Blob parameter `${name}`"),
            Expr::AliasRef(name) => format!("Blob alias `{name}`"),
            _ => "Blob expression".to_string(),
        };
        return Err(CompilerError::typed(
            T24,
            format!(
                "{subject} is not available as a .gq read value; Blob values require a dedicated API"
            ),
        ));
    }
    Ok(())
}

/// Exhaustive over `Expr`, so a new variant fails to compile here instead of
/// reaching the executor's catch-all arm; a rank expression repeats the leading
/// `order` retrieval (T33), a Boolean expression carries its `alias` (T43).
fn check_projection(expr: &Expr, alias: Option<&str>, order_clause: &[Ordering]) -> Result<()> {
    match expr {
        Expr::Now | Expr::PropAccess { .. } | Expr::Variable(_) | Expr::Literal(_) => Ok(()),
        Expr::Aggregate { func, arg, .. } => match arg.as_ref() {
            Expr::Nearest { .. } | Expr::Bm25 { .. } | Expr::Rrf { .. } => {
                Err(CompilerError::typed(
                    T32,
                    format!(
                        "`{}` under `{func}` in `return`: a retrieval selects the rows an aggregate counts; state the filter instead",
                        rank_keyword(arg)
                    ),
                ))
            }
            inner => check_projection(inner, alias, order_clause),
        },
        Expr::Nearest { .. } | Expr::Bm25 { .. } | Expr::Rrf { .. } => {
            let executed = order_clause.first().is_some_and(|lead| &lead.expr == expr);
            if !executed {
                return Err(CompilerError::typed(
                    T33,
                    format!(
                        "`{}` in `return` must repeat the retrieval stated as the leading `order` key; the projection reads the score that ordering computed",
                        rank_keyword(expr)
                    ),
                ));
            }
            if expr.score_column().is_none() {
                return Err(CompilerError::typed(
                    T33,
                    format!(
                        "`{}` projects its score only over a property field; name the property the retrieval ranks",
                        rank_keyword(expr)
                    ),
                ));
            }
            Ok(())
        }
        Expr::Search { .. } | Expr::Fuzzy { .. } | Expr::MatchText { .. } => {
            Err(CompilerError::typed(
                T35,
                format!(
                    "`{}` cannot be projected in `return`; a search predicate belongs in `match`",
                    rank_keyword(expr)
                ),
            ))
        }
        Expr::AliasRef(name) => Err(CompilerError::typed(
            T36,
            format!(
                "`{name}` cannot be projected in `return`; an alias is resolved in `order`, not projected again"
            ),
        )),
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => {
            require_projection_alias(alias)?;
            check_projection(left, alias, order_clause)?;
            check_projection(right, alias, order_clause)
        }
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => {
            require_projection_alias(alias)?;
            check_projection(inner, alias, order_clause)
        }
    }
}

fn require_projection_alias(alias: Option<&str>) -> Result<()> {
    if alias.is_none() {
        return Err(CompilerError::typed(
            T43,
            "a comparison in return needs an alias; write `… as <name>`".to_string(),
        ));
    }
    Ok(())
}

fn rank_keyword(expr: &Expr) -> &'static str {
    match expr {
        Expr::Nearest { .. } => "nearest",
        Expr::Bm25 { .. } => "bm25",
        Expr::Rrf { .. } => "rrf",
        Expr::Search { .. } => "search",
        Expr::Fuzzy { .. } => "fuzzy",
        Expr::MatchText { .. } => "match_text",
        Expr::Now
        | Expr::PropAccess { .. }
        | Expr::Variable(_)
        | Expr::Literal(_)
        | Expr::Aggregate { .. }
        | Expr::AliasRef(_)
        | Expr::Binary { .. }
        | Expr::Not(_)
        | Expr::IsNull { .. }
        | Expr::In { .. } => "expression",
    }
}

/// The column name a projection carries in the executed result batch
/// (`exec/projection.rs`, `evaluate_projection` and the aggregate path): the
/// alias when given, else `var.prop` for a property, the variable or
/// parameter name for a bare variable, `literal` for a literal, and the
/// argument's executed name for an aggregate; every other expression keeps
/// `projection_name`'s spelling. `projection_name` is the inferred-schema
/// spelling and names an unaliased property by the property alone; the two
/// spellings drift for unaliased projections today, and this function
/// follows the executor because T25 guards the batch the executor builds.
pub fn executed_column_name(expr: &Expr, alias: Option<&str>) -> String {
    if let Some(alias) = alias {
        return alias.to_string();
    }
    match expr {
        Expr::PropAccess { variable, property } => format!("{variable}.{property}"),
        Expr::Variable(variable) => variable.clone(),
        Expr::Literal(_) => "literal".to_string(),
        Expr::Aggregate { arg, .. } => executed_column_name(arg, None),
        // `now()` lowers to the hidden parameter and is named after it.
        Expr::Now => crate::query::ast::NOW_PARAM_NAME.to_string(),
        other => projection_name(other, None),
    }
}

pub(crate) fn projection_name(expr: &Expr, alias: Option<&str>) -> String {
    if let Some(alias) = alias {
        return alias.to_string();
    }

    match expr {
        Expr::Now => "now".to_string(),
        Expr::PropAccess { property, .. } => property.clone(),
        Expr::Variable(variable) => variable.clone(),
        Expr::Literal(_) => "literal".to_string(),
        Expr::Nearest { .. } | Expr::Bm25 { .. } | Expr::Rrf { .. } => match expr.score_column() {
            Some((variable, column)) => format!("{variable}.{column}"),
            None => rank_keyword(expr).to_string(),
        },
        Expr::Search { .. } | Expr::Fuzzy { .. } | Expr::MatchText { .. } => {
            rank_keyword(expr).to_string()
        }
        Expr::Aggregate { func, .. } => func.to_string(),
        Expr::AliasRef(name) => name.clone(),
        Expr::Binary { .. } | Expr::Not(_) | Expr::IsNull { .. } | Expr::In { .. } => {
            "expression".to_string()
        }
    }
}

/// T8: `count` takes a scalar or a node, `sum`/`avg` a numeric, `min`/`max` an
/// orderable scalar; none takes an aggregate.
fn check_aggregate_argument(
    func: &AggFunc,
    arg: &Expr,
    arg_type: &ResolvedType,
) -> Result<ExprType> {
    match (func, arg_type) {
        (_, ResolvedType::Aggregate(_) | ResolvedType::ForwardAlias) => Err(CompilerError::typed(
            T8,
            format!("{func} cannot take an aggregate or a forward alias reference as its argument"),
        )),
        (AggFunc::Count, ResolvedType::Node(type_name)) => Ok(ExprType::Node {
            type_name: type_name.clone(),
        }),
        (_, ResolvedType::Node(_)) => {
            let subject = match arg {
                Expr::Variable(name) => format!("node binding `${name}`"),
                Expr::AliasRef(alias) => format!("node projection `{alias}`"),
                other => format!("node value `{other:?}`"),
            };
            Err(CompilerError::typed(
                T8,
                format!(
                    "{func} cannot take {subject} bare; access one of the node's properties (`$var.{{prop}}`)"
                ),
            ))
        }
        (AggFunc::Sum | AggFunc::Avg, ResolvedType::Scalar(s))
            if s.list || !s.scalar.is_numeric() =>
        {
            Err(CompilerError::typed(
                T8,
                format!("{} requires numeric type, got {}", func, s.display_name()),
            ))
        }
        (AggFunc::Min | AggFunc::Max, ResolvedType::Scalar(s))
            if s.list || !s.scalar.is_orderable() =>
        {
            Err(CompilerError::typed(
                T8,
                format!(
                    "{} requires a numeric, String, Bool, Date, or DateTime scalar, got {}",
                    func,
                    s.display_name()
                ),
            ))
        }
        (_, ResolvedType::Scalar(prop)) => Ok(ExprType::from_prop(prop)),
    }
}

/// Convert a stored return type to a field without inferring its expression again.
pub(crate) fn projection_field(catalog: &Catalog, name: &str, ty: &ExprType) -> Result<Field> {
    let (data_type, nullable) = match ty {
        ExprType::Value { nullable, .. } => (
            ty.to_arrow()
                .ok_or_else(|| CompilerError::Plan("value has no Arrow type".into()))?,
            *nullable,
        ),
        ExprType::ExactInteger { .. } => {
            return Err(CompilerError::Plan(
                "internal exact integer cannot be a result column".into(),
            ));
        }
        ExprType::Node { type_name } => {
            let node_type = catalog.query_node_type(type_name).ok_or_else(|| {
                CompilerError::typed(T51, format!("type `{}` not found in catalog", type_name))
            })?;
            let fields: Vec<Field> = node_type
                .node_object_members()
                .map(|(member, field)| {
                    Field::new(member, field.data_type().clone(), field.is_nullable())
                })
                .collect();
            (DataType::Struct(fields.into()), false)
        }
    };
    Ok(Field::new(name, data_type, nullable))
}

/// The refusal of `$a in $b` where `$a` is a node binding, the shape of a
/// traversal over an edge `In`: it names the quoted form when that edge exists.
fn membership_over_a_node(
    catalog: &Catalog,
    needle: &Expr,
    list: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Option<CompilerError> {
    let (Expr::Variable(node), Expr::Variable(other)) = (needle, list) else {
        return None;
    };
    let is_node = matches!(ctx.bindings.get(node), Some(BoundVariable::Node { .. }));
    if params.contains_key(node) || !is_node {
        return None;
    }
    let advice = match catalog.lookup_edge_by_name("in") {
        Some(edge) => format!(
            "to follow the edge `{}` write `${node} \"in\" ${other}`",
            edge.name
        ),
        None => "`in` takes a value on the left and a list on the right".to_string(),
    };
    Some(CompilerError::typed(
        T7,
        format!("`${node} in ${other}` tests membership and `${node}` is a node; {advice}"),
    ))
}

/// The type of a literal payload, computed once per occurrence (list
/// elements included) and retained on the typed node.
fn literal_type(lit: &Literal) -> Result<PropType> {
    visits::literal(lit);
    match lit {
        // Null is compatible with any nullable type; default to String for inference.
        Literal::Null => Ok(PropType::scalar(ScalarType::String, true)),
        Literal::String(_) => Ok(PropType::scalar(ScalarType::String, false)),
        Literal::Integer(_) => Ok(PropType::scalar(ScalarType::I64, false)),
        Literal::Float(value) if value.is_infinite() => Err(CompilerError::typed(
            T3,
            "float literal is outside the F64 range".to_string(),
        )),
        Literal::Float(_) => Ok(PropType::scalar(ScalarType::F64, false)),
        Literal::Bool(_) => Ok(PropType::scalar(ScalarType::Bool, false)),
        Literal::Date(value) => {
            check_date_literal(value)
                .map_err(|reason| CompilerError::typed(T3, reason.to_string()))?;
            Ok(PropType::scalar(ScalarType::Date, false))
        }
        Literal::DateTime(value) => {
            check_datetime_literal(value)
                .map_err(|reason| CompilerError::typed(T3, reason.to_string()))?;
            Ok(PropType::scalar(ScalarType::DateTime, false))
        }
        Literal::List(items) => {
            let mut scalar = None;
            for item in items {
                let item_type = literal_type(item)?;
                if matches!(item, Literal::Null) {
                    continue;
                }
                if item_type.list {
                    return Err(CompilerError::typed(
                        T52,
                        "nested list literals are not supported".to_string(),
                    ));
                }
                scalar = Some(match scalar {
                    None => item_type.scalar,
                    Some(previous) if previous == item_type.scalar => previous,
                    Some(ScalarType::I64 | ScalarType::F64)
                        if matches!(item_type.scalar, ScalarType::I64 | ScalarType::F64) =>
                    {
                        ScalarType::F64
                    }
                    Some(_) => {
                        return Err(CompilerError::typed(
                            T53,
                            "list literal elements must share a compatible scalar type".to_string(),
                        ));
                    }
                });
            }
            Ok(PropType::list_of(
                scalar.unwrap_or(ScalarType::String),
                false,
            ))
        }
    }
}

/// A literal against the property it is assigned to or matched with;
/// `lit_type` is the type the resolver computed for `lit`, so the payload
/// is inspected here (null, vector width, enum values) but never typed again.
fn check_literal_type(
    lit: &Literal,
    lit_type: &PropType,
    expected: &PropType,
    prop_name: &str,
) -> Result<()> {
    // Null is compatible with any nullable property type.
    if matches!(lit, Literal::Null) {
        return if expected.nullable {
            Ok(())
        } else {
            Err(CompilerError::typed(
                T3,
                format!("property `{}` is non-nullable but got null", prop_name),
            ))
        };
    }

    if !expected.list
        && let ScalarType::Vector(expected_dim) = expected.scalar
        && let Some(actual_dim) = numeric_vector_literal_dim(lit)
    {
        if actual_dim == expected_dim {
            return Ok(());
        }
        return Err(CompilerError::typed(
            T3,
            format!(
                "property `{}` has type Vector({}) but got vector literal with {} elements",
                prop_name, expected_dim, actual_dim
            ),
        ));
    }

    if !types_compatible(lit_type, expected) {
        return Err(CompilerError::typed(
            T3,
            format!(
                "property `{}` has type {} but got {}",
                prop_name,
                expected.display_name(),
                lit_type.display_name()
            ),
        ));
    }
    if expected.is_enum() {
        let allowed = expected.enum_values.as_ref().cloned().unwrap_or_default();
        match lit {
            Literal::String(v) if !allowed.contains(v) => {
                return Err(CompilerError::typed(
                    T3,
                    format!(
                        "property `{}` expects one of [{}], got '{}'",
                        prop_name,
                        allowed.join(", "),
                        v
                    ),
                ));
            }
            Literal::List(items) if expected.list => {
                for item in items {
                    match item {
                        Literal::String(v) if allowed.contains(v) => {}
                        Literal::String(v) => {
                            return Err(CompilerError::typed(
                                T3,
                                format!(
                                    "property `{}` expects one of [{}], got '{}'",
                                    prop_name,
                                    allowed.join(", "),
                                    v
                                ),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// `types_compatible`, plus a String parameter assigned to a Blob property
/// (the blob's URI).
fn assignment_compatible(actual: &PropType, expected: &PropType) -> bool {
    types_compatible(actual, expected)
        || (matches!(expected.scalar, ScalarType::Blob)
            && matches!(actual.scalar, ScalarType::String)
            && !actual.list)
}

fn types_compatible(a: &PropType, b: &PropType) -> bool {
    if a.list != b.list {
        return false;
    }
    if a.scalar == b.scalar {
        return true;
    }
    // Numeric types are mutually compatible for comparison
    if a.scalar.is_numeric() && b.scalar.is_numeric() {
        return true;
    }
    false
}

fn numeric_vector_literal_dim(lit: &Literal) -> Option<u32> {
    let items = match lit {
        Literal::List(items) => items,
        _ => return None,
    };
    if items.is_empty() {
        return None;
    }
    if items
        .iter()
        .all(|v| matches!(v, Literal::Integer(_) | Literal::Float(_)))
    {
        Some(items.len() as u32)
    } else {
        None
    }
}

fn expr_references_any(expr: &Expr, vars: &[String]) -> bool {
    match expr {
        Expr::PropAccess { variable, .. } => vars.contains(variable),
        Expr::Nearest {
            variable, query, ..
        } => vars.contains(variable) || expr_references_any(query, vars),
        Expr::Search { field, query } => {
            expr_references_any(field, vars) || expr_references_any(query, vars)
        }
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            expr_references_any(field, vars)
                || expr_references_any(query, vars)
                || max_edits
                    .as_deref()
                    .is_some_and(|m| expr_references_any(m, vars))
        }
        Expr::MatchText { field, query } => {
            expr_references_any(field, vars) || expr_references_any(query, vars)
        }
        Expr::Bm25 { field, query } => {
            expr_references_any(field, vars) || expr_references_any(query, vars)
        }
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => {
            expr_references_any(primary, vars)
                || expr_references_any(secondary, vars)
                || k.as_deref()
                    .is_some_and(|expr| expr_references_any(expr, vars))
        }
        Expr::Variable(v) => vars.contains(v),
        Expr::Aggregate { arg, .. } => expr_references_any(arg, vars),
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => expr_references_any(left, vars) || expr_references_any(right, vars),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => expr_references_any(inner, vars),
        _ => false,
    }
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => expr_contains_aggregate(left) || expr_contains_aggregate(right),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => expr_contains_aggregate(inner),
        _ => false,
    }
}

/// T58: a Boolean return item over an aggregate is evaluated after
/// aggregation, where only the group's aggregates and constants exist.
fn check_aggregate_comparison_leaves(
    expr: &Expr,
    params: &HashMap<String, PropType>,
) -> Result<()> {
    match expr {
        Expr::Aggregate { .. } | Expr::Literal(_) | Expr::Now => Ok(()),
        Expr::Variable(name) if params.contains_key(name) => Ok(()),
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => {
            check_aggregate_comparison_leaves(left, params)?;
            check_aggregate_comparison_leaves(right, params)
        }
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => {
            check_aggregate_comparison_leaves(inner, params)
        }
        other => Err(CompilerError::typed(
            T58,
            format!(
                "`{other}` beside an aggregate: a Boolean return item compares aggregates with constants only; project the group value on its own"
            ),
        )),
    }
}

/// T33 admits a projected rank expression only as a repeat of the leading
/// `order` key, so an alias never leads the order and the walk stops at
/// `AliasRef`; a `nearest` nested under `rrf` is T21's.
fn expr_contains_standalone_nearest(expr: &Expr) -> bool {
    match expr {
        Expr::Nearest { .. } => true,
        Expr::Aggregate { arg, .. } => expr_contains_standalone_nearest(arg),
        Expr::Search { field, query }
        | Expr::MatchText { field, query }
        | Expr::Bm25 { field, query } => {
            expr_contains_standalone_nearest(field) || expr_contains_standalone_nearest(query)
        }
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            expr_contains_standalone_nearest(field)
                || expr_contains_standalone_nearest(query)
                || max_edits
                    .as_deref()
                    .is_some_and(expr_contains_standalone_nearest)
        }
        Expr::Rrf { .. } => false,
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => expr_contains_standalone_nearest(left) || expr_contains_standalone_nearest(right),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => {
            expr_contains_standalone_nearest(inner)
        }
        _ => false,
    }
}

fn expr_contains_rrf(expr: &Expr) -> bool {
    match expr {
        Expr::Rrf { .. } => true,
        Expr::Aggregate { arg, .. } => expr_contains_rrf(arg),
        Expr::Search { field, query }
        | Expr::MatchText { field, query }
        | Expr::Bm25 { field, query } => expr_contains_rrf(field) || expr_contains_rrf(query),
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            expr_contains_rrf(field)
                || expr_contains_rrf(query)
                || max_edits.as_deref().is_some_and(expr_contains_rrf)
        }
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => expr_contains_rrf(left) || expr_contains_rrf(right),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => expr_contains_rrf(inner),
        _ => false,
    }
}

/// Test-only record of the resolution work one checking invocation does:
/// the address of every `Expr` the resolver enters and of every `Literal`
/// payload `literal_type` types. Thread-local, so a test owns its own run;
/// no production semantics.
#[cfg(test)]
pub(crate) mod visits {
    use std::cell::RefCell;

    use super::{Expr, Literal};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub(crate) enum Visit {
        Expr(usize),
        Literal(usize),
    }

    thread_local! {
        static RECORDER: RefCell<Option<Vec<Visit>>> = const { RefCell::new(None) };
    }

    /// Start recording on this thread, discarding any earlier record.
    pub(crate) fn start() {
        RECORDER.with(|recorder| *recorder.borrow_mut() = Some(Vec::new()));
    }

    /// Stop recording and return the visits in order.
    pub(crate) fn finish() -> Vec<Visit> {
        RECORDER.with(|recorder| recorder.borrow_mut().take().unwrap_or_default())
    }

    pub(super) fn expr(expr: &Expr) {
        record(Visit::Expr(expr as *const Expr as usize));
    }

    pub(super) fn literal(literal: &Literal) {
        record(Visit::Literal(literal as *const Literal as usize));
    }

    fn record(visit: Visit) {
        RECORDER.with(|recorder| {
            if let Some(visits) = recorder.borrow_mut().as_mut() {
                visits.push(visit);
            }
        });
    }
}

#[cfg(not(test))]
mod visits {
    pub(super) fn expr(_: &super::Expr) {}

    pub(super) fn literal(_: &super::Literal) {}
}

#[cfg(test)]
#[path = "typecheck_tests.rs"]
mod tests;
