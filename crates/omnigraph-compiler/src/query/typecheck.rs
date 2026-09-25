use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{SYSTEM_COLUMNS_META, SystemFieldRole};
use crate::error::{CompilerError, Result};
use crate::types::{Direction, PropType, ScalarType};

use super::ast::*;
use super::codes::*;

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
    Edge { type_name: String },
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

#[derive(Debug, Clone)]
pub struct TypeContext {
    pub bindings: HashMap<String, BoundVariable>,
    pub aliases: HashMap<String, ResolvedType>,
    pub traversals: Vec<ResolvedTraversal>,
}

impl TypeContext {
    /// No bindings, no aliases: the read context of a mutation scope, where
    /// every name resolves through the scope instead.
    fn empty() -> Self {
        Self {
            bindings: HashMap::new(),
            aliases: HashMap::new(),
            traversals: Vec::new(),
        }
    }
}

/// Where an expression stands. Every scope resolves through the one
/// `resolve_expr_type`; the scope decides what a bare name means and which
/// node kinds are refused, one binder per clause over one expression type.
#[derive(Clone, Copy)]
enum Scope<'a> {
    /// A read clause over the match bindings.
    Read,
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
    /// The refusal of an aggregate, search or ranking call in this scope, the
    /// call's keyword in front (T44); `None` in a read.
    fn call_refusal(self, keyword: &str) -> Option<CompilerError> {
        match self {
            Scope::Read => None,
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
    pub edge_type: String,
    pub direction: Direction,
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
    Aggregate,
}

impl ResolvedType {
    fn display_name(&self) -> String {
        match self {
            Self::Scalar(prop) => prop.display_name(),
            Self::Node(type_name) => format!("node `{}`", type_name),
            Self::Aggregate => "aggregate".to_string(),
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

#[derive(Debug, Clone)]
pub struct MutationTypeContext {
    pub targets: Vec<MutationTarget>,
}

#[derive(Debug, Clone)]
pub enum CheckedQuery {
    Read(TypeContext),
    Mutation(MutationTypeContext),
}

pub fn typecheck_query_decl(catalog: &Catalog, query: &QueryDecl) -> Result<CheckedQuery> {
    if !query.mutations.is_empty() {
        let mut targets = Vec::with_capacity(query.mutations.len());
        for mutation in &query.mutations {
            targets.push(typecheck_mutation(catalog, mutation, &query.params)?);
        }
        Ok(CheckedQuery::Mutation(MutationTypeContext { targets }))
    } else {
        Ok(CheckedQuery::Read(typecheck_read_query(catalog, query)?))
    }
}

pub fn typecheck_query(catalog: &Catalog, query: &QueryDecl) -> Result<TypeContext> {
    if !query.mutations.is_empty() {
        return Err(CompilerError::Plan(
            "mutation query cannot be typechecked with read-query API".to_string(),
        ));
    }
    typecheck_read_query(catalog, query)
}

pub fn infer_query_result_schema(
    catalog: &Catalog,
    query: &QueryDecl,
    ctx: &TypeContext,
) -> Result<SchemaRef> {
    let params = parse_declared_param_types(&query.params)?;
    let mut fields = Vec::with_capacity(query.return_clause.len());

    for projection in &query.return_clause {
        let field = infer_projection_field(
            catalog,
            &projection.expr,
            projection.alias.as_deref(),
            &query.order_clause,
            ctx,
            &params,
        )?;
        fields.push(field);
    }

    Ok(Arc::new(Schema::new(fields)))
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

fn typecheck_read_query(catalog: &Catalog, query: &QueryDecl) -> Result<TypeContext> {
    let mut ctx = TypeContext {
        bindings: HashMap::new(),
        aliases: HashMap::new(),
        traversals: Vec::new(),
    };
    let params = parse_declared_param_types(&query.params)?;

    refuse_reserved_variable_names(&query.match_clause)?;

    // Typecheck match clauses
    typecheck_clauses(catalog, &query.match_clause, &mut ctx, &params, false)?;

    // Typecheck return projections
    let mut result_columns: HashSet<String> = HashSet::new();
    for proj in &query.return_clause {
        let resolved = resolve_expr_type(catalog, &proj.expr, &ctx, &params, Scope::Read)?;
        reject_blob_read_value(&resolved, &proj.expr)?;
        check_projection(&proj.expr, proj.alias.as_deref(), &query.order_clause)?;
        // T25: one result column per name. The executor emits a batch with
        // every projection's column under its executed name; two columns of
        // one name survive the batch (Arrow allows it) and every reader that
        // keys a row by column name keeps the last one, so the first value
        // is lost without an error.
        let column = executed_column_name(&proj.expr, proj.alias.as_deref());
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
    }

    // Typecheck order expressions
    for (index, ord) in query.order_clause.iter().enumerate() {
        let resolved = resolve_expr_type(catalog, &ord.expr, &ctx, &params, Scope::Read)?;
        reject_blob_read_value(&resolved, &ord.expr)?;
        bind_order_key(index, &ord.expr, &query.return_clause, &ctx)?;
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
    if has_standalone_nearest
        && query
            .order_clause
            .iter()
            .any(|ord| matches!(ord.expr, Expr::AliasRef(_)))
    {
        return Err(CompilerError::typed(
            T18,
            "alias-based ordering is not supported together with nearest in phase 1".to_string(),
        ));
    }

    // T9: If any return expression is an aggregate, non-aggregate expressions
    // must be valid group-by keys (PropAccess or Variable).
    let has_agg = query
        .return_clause
        .iter()
        .any(|p| matches!(p.expr, Expr::Aggregate { .. }));
    if has_agg {
        for proj in &query.return_clause {
            if !matches!(proj.expr, Expr::Aggregate { .. }) {
                match &proj.expr {
                    Expr::PropAccess { .. } | Expr::Variable(_) => {}
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
    }

    Ok(ctx)
}

/// T42 (RFC 2026-09-24-shared-expression-model, "Order keys bind against the
/// return list"): a property key sorts through the hidden column, the leading
/// rank key is the search node's; every other key is a return alias or item.
fn bind_order_key(
    index: usize,
    key: &Expr,
    return_clause: &[Projection],
    ctx: &TypeContext,
) -> Result<()> {
    let exempt = match key {
        Expr::PropAccess { .. } => true,
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
    Err(CompilerError::typed(
        T42,
        format!(
            "order key `{key}` does not appear in return; add it to return or order by its alias"
        ),
    ))
}

fn typecheck_mutation(
    catalog: &Catalog,
    mutation: &Mutation,
    params: &[Param],
) -> Result<MutationTarget> {
    let param_types = parse_declared_param_types(params)?;

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
                    typecheck_assignment(
                        catalog,
                        assignment,
                        &param_types,
                        prop_type,
                        &insert.type_name,
                    )?;
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
                return Ok(MutationTarget::Node {
                    type_name: insert.type_name.clone(),
                });
            }

            if let Some(edge_type) = catalog.edge_types.get(&insert.type_name) {
                let mut has_from = false;
                let mut has_to = false;

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
                    typecheck_assignment(
                        catalog,
                        assignment,
                        &param_types,
                        &prop_type,
                        &insert.type_name,
                    )?;
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
                return Ok(MutationTarget::Edge {
                    type_name: insert.type_name.clone(),
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
                typecheck_assignment(
                    catalog,
                    assignment,
                    &param_types,
                    prop_type,
                    &update.type_name,
                )?;
            }

            let target = MutationTarget::Node {
                type_name: update.type_name.clone(),
            };
            typecheck_mutation_where(catalog, &target, &update.predicate, &param_types)?;
            Ok(target)
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
            typecheck_mutation_where(catalog, &target, &delete.predicate, &param_types)?;
            Ok(target)
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

/// The system role a meta-field spelling names (RFC 0040 Query language):
/// `@id` on any binding, `@src`/`@dst` on an edge; `None` for a bare name.
fn meta_field_role(property: &str) -> Option<Option<SystemFieldRole>> {
    property.starts_with('@').then(|| match property {
        name if name == SYSTEM_COLUMNS_META.id => Some(SystemFieldRole::Id),
        name if name == SYSTEM_COLUMNS_META.src => Some(SystemFieldRole::Src),
        name if name == SYSTEM_COLUMNS_META.dst => Some(SystemFieldRole::Dst),
        _ => None,
    })
}

/// The type of every meta-field: the system identity and endpoints are
/// non-null strings on both vintages.
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
            Some(SystemFieldRole::Id) => true,
            Some(SystemFieldRole::Src | SystemFieldRole::Dst) => is_edge,
            None => false,
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
) -> Result<()> {
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
    let resolved = resolve_expr_type(
        catalog,
        predicate,
        &TypeContext::empty(),
        params,
        Scope::MutationWhere(target),
    )?;
    if boolean_scalar(&resolved).is_none() {
        return Err(CompilerError::typed(
            T41,
            format!(
                "a mutation `where` must be Boolean, got {}",
                resolved.display_name()
            ),
        ));
    }
    Ok(())
}

/// The first bare `true` or `false` in written order, through the Boolean
/// structure; a list literal's elements are not bare words.
fn first_boolean_literal(expr: &Expr) -> Option<bool> {
    match expr {
        Expr::Literal(Literal::Bool(value)) => Some(*value),
        Expr::Binary { left, right, .. } => {
            first_boolean_literal(left).or_else(|| first_boolean_literal(right))
        }
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
) -> Result<()> {
    typecheck_constant(
        catalog,
        &assignment.value,
        params,
        expected,
        &assignment.property,
        ConstantClause::Assignment,
        type_name,
    )
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
) -> Result<()> {
    let scope = Scope::Constant { clause, type_name };
    let resolved = resolve_expr_type(catalog, value, &TypeContext::empty(), params, scope)?;
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
        (ConstantClause::Assignment, Expr::Literal(lit)) => {
            check_literal_type(lit, expected, property)
        }
        (ConstantClause::BindingMatch, Expr::Literal(lit)) => {
            check_binding_literal_type(lit, expected, property)
        }
        (_, Expr::Now) => check_now_match_value_type(expected, property),
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

fn typecheck_clauses(
    catalog: &Catalog,
    clauses: &[Clause],
    ctx: &mut TypeContext,
    params: &HashMap<String, PropType>,
    _in_negation: bool,
) -> Result<()> {
    for clause in clauses {
        match clause {
            Clause::Binding(b) => typecheck_binding(catalog, b, ctx, params)?,
            Clause::Traversal(t) => typecheck_traversal(catalog, t, ctx)?,
            Clause::Filter(f) => typecheck_filter(catalog, f, ctx, params)?,
            Clause::Subquery(subquery) => {
                let outer_vars: Vec<String> = ctx.bindings.keys().cloned().collect();
                let mut inner_ctx = ctx.clone();
                typecheck_clauses(catalog, &subquery.clauses, &mut inner_ctx, params, true)?;
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
                typecheck_subquery_predicate(catalog, subquery, &inner_ctx, ctx, params)?;
            }
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

/// The comparison of a subquery predicate: the aggregate's result type from
/// the block's scope (the argument rule of a `return` aggregate), the right
/// operand from the outer scope, compatible under the filter rule.
fn typecheck_subquery_predicate(
    catalog: &Catalog,
    subquery: &Subquery,
    inner_ctx: &TypeContext,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<()> {
    let func = subquery.func;
    let result = match &subquery.arg {
        None => PropType::scalar(ScalarType::I64, false),
        Some(arg) => {
            let arg_type = resolve_expr_type(catalog, arg, inner_ctx, params, Scope::Read)?;
            reject_blob_read_value(&arg_type, arg)?;
            check_aggregate_argument(&func, arg, &arg_type)?;
            match (func, &arg_type) {
                (AggFunc::Count, _) => PropType::scalar(ScalarType::I64, false),
                (AggFunc::Sum | AggFunc::Avg, _) => PropType::scalar(ScalarType::F64, false),
                (AggFunc::Min | AggFunc::Max, ResolvedType::Scalar(s)) => {
                    PropType::scalar(s.scalar, false)
                }
                (_, other) => {
                    return Err(CompilerError::typed(
                        T40,
                        format!(
                            "{func} over a block requires a scalar argument, got {}",
                            other.display_name()
                        ),
                    ));
                }
            }
        }
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
    let right = resolve_expr_type(catalog, &subquery.right, ctx, params, Scope::Read)?;
    let ResolvedType::Scalar(r) = &right else {
        return Err(CompilerError::typed(
            T40,
            format!(
                "{func} over a block compares with a scalar, got {}",
                right.display_name()
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
    Ok(())
}

fn typecheck_binding(
    catalog: &Catalog,
    binding: &Binding,
    ctx: &mut TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<()> {
    // T1: binding type must exist in catalog
    if !catalog.node_types.contains_key(&binding.type_name) {
        return Err(CompilerError::typed(
            T1,
            format!("unknown node type `{}`", binding.type_name),
        ));
    }

    let node_type = &catalog.node_types[&binding.type_name];

    // T2 + T3: property match fields must exist and have correct types
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

        typecheck_constant(
            catalog,
            &pm.value,
            params,
            prop,
            &pm.prop_name,
            ConstantClause::BindingMatch,
            &binding.type_name,
        )?;
    }

    // Don't overwrite if already bound to the same node type (re-binding the
    // same node var is OK). Node and edge namespaces are independent, so a
    // matching type name does not make a cross-kind rebind valid.
    if let Some(existing) = ctx.bindings.get(&binding.variable) {
        match existing {
            BoundVariable::Edge { type_name } => {
                return Err(CompilerError::typed(
                    T23,
                    format!(
                        "variable `${}` is bound to edge type `{}` and cannot be rebound as node type `{}`",
                        binding.variable, type_name, binding.type_name
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

fn check_binding_literal_type(lit: &Literal, expected: &PropType, property: &str) -> Result<()> {
    if expected.list {
        let lit_type = literal_type(lit)?;
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
        if !types_compatible(&lit_type, &expected_member) {
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

    check_literal_type(lit, expected, property)
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
) -> Result<()> {
    // T4: edge must exist
    let edge = catalog
        .lookup_edge_by_name(&traversal.edge_name)
        .ok_or_else(|| {
            CompilerError::typed(T4, format!("unknown edge type `{}`", traversal.edge_name))
        })?;

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

    // Undirected (`$a <edge> $b`): only meaningful when both orientations
    // carry the same endpoint types — for an asymmetric edge the pattern is
    // well-typed in at most one direction, so the undirected form is either
    // pointless or a type error; require the directional form instead.
    if traversal.undirected && edge.from_type != edge.to_type {
        return Err(CompilerError::typed(
            T22,
            format!(
                "undirected traversal `<{}>` requires a same-endpoint-type edge, but `{}: {} -> {}` is asymmetric; use the directional form",
                edge.name, edge.name, edge.from_type, edge.to_type
            ),
        ));
    }

    // T23: a {min,max} traversal matches a path of edges; there is no single
    // row for a binding to name.
    let edge_binding = traversal
        .edge_binding
        .as_deref()
        .filter(|binding| *binding != "_")
        .map(str::to_string);
    if traversal.edge_binding.is_some()
        && (traversal.min_hops != 1 || traversal.max_hops != Some(1))
    {
        return Err(CompilerError::typed(
            T23,
            format!(
                "edge binding `${}` cannot be combined with traversal bounds; a multi-hop traversal matches a path of edges, not one edge",
                traversal.edge_binding.as_deref().unwrap_or("_")
            ),
        ));
    }
    if let Some(binding) = &edge_binding {
        if binding == &traversal.src || binding == &traversal.dst {
            return Err(CompilerError::typed(
                T23,
                format!(
                    "edge binding `${binding}` cannot reuse a traversal endpoint name; edge bindings and node endpoints need distinct names"
                ),
            ));
        }
        if ctx.bindings.contains_key(binding) {
            return Err(CompilerError::typed(
                T23,
                format!(
                    "variable `${}` is already bound; an edge binding needs a fresh name",
                    binding
                ),
            ));
        }
        ctx.bindings.insert(
            binding.clone(),
            BoundVariable::Edge {
                type_name: edge.name.clone(),
            },
        );
    }

    // Determine direction based on bound variables and edge endpoints
    let src_bound = ctx.bindings.get(&traversal.src);
    let dst_bound = ctx.bindings.get(&traversal.dst);

    let mut direction;

    if let Some(src_bv) = src_bound {
        let src_type = src_bv.require_traversal_endpoint(&traversal.src)?;
        // T5: src type must match one endpoint of the edge
        if src_type == edge.from_type {
            direction = Direction::Out;
            // dst should be edge.to_type
            bind_traversal_endpoint(ctx, &traversal.dst, &edge.to_type, edge)?;
        } else if src_type == edge.to_type {
            direction = Direction::In;
            // dst should be edge.from_type
            bind_traversal_endpoint(ctx, &traversal.dst, &edge.from_type, edge)?;
        } else {
            return Err(CompilerError::typed(
                T5,
                format!(
                    "variable `${}` has type `{}`, which is not an endpoint of edge `{}: {} -> {}`",
                    traversal.src, src_type, edge.name, edge.from_type, edge.to_type
                ),
            ));
        }
    } else if let Some(dst_bv) = dst_bound {
        let dst_type = dst_bv.require_traversal_endpoint(&traversal.dst)?;
        // dst is bound, infer direction from it
        if dst_type == edge.to_type {
            direction = Direction::Out;
            bind_traversal_endpoint(ctx, &traversal.src, &edge.from_type, edge)?;
        } else if dst_type == edge.from_type {
            direction = Direction::In;
            bind_traversal_endpoint(ctx, &traversal.src, &edge.to_type, edge)?;
        } else {
            return Err(CompilerError::typed(
                T5,
                format!(
                    "variable `${}` has type `{}`, which is not an endpoint of edge `{}: {} -> {}`",
                    traversal.dst, dst_type, edge.name, edge.from_type, edge.to_type
                ),
            ));
        }
    } else {
        // Neither bound — default Out direction, bind both
        direction = Direction::Out;
        bind_traversal_endpoint(ctx, &traversal.src, &edge.from_type, edge)?;
        bind_traversal_endpoint(ctx, &traversal.dst, &edge.to_type, edge)?;
    }

    if traversal.undirected {
        // The orientation inference above is a no-op for a same-type edge
        // (both arms resolve identically); the user asked for both ways.
        direction = Direction::Both;
    }

    ctx.traversals.push(ResolvedTraversal {
        src: traversal.src.clone(),
        dst: traversal.dst.clone(),
        edge_type: edge.name.clone(),
        direction,
        min_hops: traversal.min_hops,
        max_hops: traversal.max_hops,
        edge_binding,
    });

    Ok(())
}

fn bind_traversal_endpoint(
    ctx: &mut TypeContext,
    var: &str,
    expected_type: &str,
    edge: &crate::catalog::EdgeType,
) -> Result<()> {
    if var == "_" {
        return Ok(()); // anonymous variable
    }
    if let Some(existing) = ctx.bindings.get(var) {
        let existing_type = existing.require_traversal_endpoint(var)?;
        if existing_type != expected_type {
            return Err(CompilerError::typed(
                T5,
                format!(
                    "variable `${}` has type `{}` but edge `{}` expects `{}`",
                    var, existing_type, edge.name, expected_type
                ),
            ));
        }
    } else {
        ctx.bindings.insert(
            var.to_string(),
            BoundVariable::Node {
                type_name: expected_type.to_string(),
            },
        );
    }
    Ok(())
}

/// A match filter is an expression the checker proves Boolean; a search
/// call stands only as a top-level conjunct, bare or `= true` (T38).
fn typecheck_filter(
    catalog: &Catalog,
    filter: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<()> {
    if filter
        .conjuncts()
        .into_iter()
        .any(|conjunct| !conjunct.is_search_predicate() && contains_search_call(conjunct))
    {
        return Err(CompilerError::typed(T38, SEARCH_PREDICATE_SHAPE));
    }
    let resolved = resolve_expr_type(catalog, filter, ctx, params, Scope::Read)?;
    if boolean_scalar(&resolved).is_none() {
        return Err(CompilerError::typed(
            T41,
            format!("a filter must be Boolean, got {}", resolved.display_name()),
        ));
    }
    Ok(())
}

const SEARCH_PREDICATE_SHAPE: &str =
    "search predicates require a standalone call or `= true`, alone or joined by and";

/// Whether a `search`, `fuzzy` or `match_text` call stands anywhere in `expr`.
fn contains_search_call(expr: &Expr) -> bool {
    expr.is_search_call()
        || match expr {
            Expr::Binary { left, right, .. } => {
                contains_search_call(left) || contains_search_call(right)
            }
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

/// `left <op> right`: the operand rules of a comparison (T7, T38) and its
/// type, `Bool`, nullable when an operand is; in a mutation `where`, a target
/// property against a literal, a parameter or `now()` keeps the T3/T7 texts.
fn typecheck_comparison(
    catalog: &Catalog,
    left: &Expr,
    op: CompOp,
    right: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
    scope: Scope<'_>,
) -> Result<PropType> {
    let left_type = resolve_expr_type(catalog, left, ctx, params, scope)?;
    let right_type = resolve_expr_type(catalog, right, ctx, params, scope)?;

    if (left.is_search_call() || right.is_search_call())
        && !(left.is_search_call()
            && op == CompOp::Eq
            && matches!(right, Expr::Literal(Literal::Bool(true))))
    {
        return Err(CompilerError::typed(T38, SEARCH_PREDICATE_SHAPE));
    }

    if let Scope::MutationWhere(target) = scope
        && let Expr::PropAccess { variable, property } = left
        && variable == target.type_name()
        && !matches!(
            op,
            CompOp::Contains | CompOp::StartsWith | CompOp::StringContains
        )
        && matches!(right, Expr::Literal(_) | Expr::Variable(_) | Expr::Now)
        && let (ResolvedType::Scalar(expected), ResolvedType::Scalar(actual)) =
            (&left_type, &right_type)
    {
        typecheck_constant(
            catalog,
            right,
            params,
            expected,
            property,
            ConstantClause::Assignment,
            target.type_name(),
        )?;
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

            let expected_member = PropType::scalar(l.scalar, l.nullable);
            if !types_compatible(&expected_member, r) {
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
            BoundVariable::Node { .. } => {}
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
            (_, Some(SystemFieldRole::Id)) => true,
            (BoundVariable::Edge { .. }, Some(_)) => true,
            (BoundVariable::Node { .. }, Some(_)) | (_, None) => false,
        };
        if !admitted {
            let known = match bv {
                BoundVariable::Node { .. } => "`@id`",
                BoundVariable::Edge { .. } => "`@id`, `@src`, `@dst`",
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
            let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
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
        BoundVariable::Edge { type_name } => {
            let edge_type = catalog.lookup_edge_by_name(type_name).ok_or_else(|| {
                CompilerError::typed(
                    T6,
                    format!("edge type `{}` not found in catalog", type_name),
                )
            })?;
            edge_type.properties.get(property).ok_or_else(|| {
                CompilerError::typed(
                    T6,
                    format!(
                        "edge `{}` has no property `{}`{}",
                        type_name,
                        property,
                        system_field_hint(property, Some(variable), true)
                    ),
                )
            })?
        }
    };
    Ok(prop.clone())
}

fn resolve_expr_type(
    catalog: &Catalog,
    expr: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
    scope: Scope<'_>,
) -> Result<ResolvedType> {
    if let Some(keyword) = call_keyword(expr)
        && let Some(refusal) = scope.call_refusal(&keyword)
    {
        return Err(refusal);
    }
    match expr {
        Expr::Now => Ok(ResolvedType::Scalar(PropType::scalar(
            ScalarType::DateTime,
            false,
        ))),
        Expr::PropAccess { variable, property } => match scope {
            Scope::Read => {
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
                Ok(ResolvedType::Scalar(prop))
            }
            Scope::MutationWhere(target) => {
                mutation_property_type(catalog, target, variable, property)
                    .map(ResolvedType::Scalar)
            }
            Scope::Constant { type_name, .. } => {
                Err(constant_leaf_refusal(type_name, variable, property))
            }
        },
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
            let node_type = catalog.node_types.get(node_type_name).ok_or_else(|| {
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

            if let Expr::Literal(lit) = query.as_ref()
                && let Some(dim) = numeric_vector_literal_dim(lit)
            {
                if dim != vector_dim {
                    return Err(CompilerError::typed(
                        T15,
                        format!(
                            "nearest vector dimension mismatch: property is Vector({}), query literal has {} elements",
                            vector_dim, dim
                        ),
                    ));
                }
                return Ok(ResolvedType::Scalar(PropType::scalar(
                    ScalarType::F32,
                    false,
                )));
            }

            let query_type = resolve_expr_type(catalog, query, ctx, params, scope)?;
            match query_type {
                ResolvedType::Scalar(s) if matches!(s.scalar, ScalarType::Vector(_)) && !s.list => {
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
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {
                    // query-time string embedding is supported by the runtime executor
                }
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

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::F32,
                false,
            )))
        }
        Expr::Search { field, query } => {
            reject_edge_binding_search_field(ctx, field, "search")?;
            let field_type = resolve_expr_type(catalog, field, ctx, params, scope)?;
            match field_type {
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

            let query_type = resolve_expr_type(catalog, query, ctx, params, scope)?;
            match query_type {
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

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::Bool,
                false,
            )))
        }
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            reject_edge_binding_search_field(ctx, field, "fuzzy")?;
            let field_type = resolve_expr_type(catalog, field, ctx, params, scope)?;
            match field_type {
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

            let query_type = resolve_expr_type(catalog, query, ctx, params, scope)?;
            match query_type {
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

            if let Some(max_edits_expr) = max_edits {
                let max_edits_type =
                    resolve_expr_type(catalog, max_edits_expr, ctx, params, scope)?;
                match max_edits_type {
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
            }

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::Bool,
                false,
            )))
        }
        Expr::MatchText { field, query } => {
            reject_edge_binding_search_field(ctx, field, "match_text")?;
            let field_type = resolve_expr_type(catalog, field, ctx, params, scope)?;
            match field_type {
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

            let query_type = resolve_expr_type(catalog, query, ctx, params, scope)?;
            match query_type {
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

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::Bool,
                false,
            )))
        }
        Expr::Bm25 { field, query } => {
            reject_edge_binding_search_field(ctx, field, "bm25")?;
            let field_type = resolve_expr_type(catalog, field, ctx, params, scope)?;
            match field_type {
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

            let query_type = resolve_expr_type(catalog, query, ctx, params, scope)?;
            match query_type {
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

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::F32,
                false,
            )))
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

            let primary_ty = resolve_expr_type(catalog, primary, ctx, params, scope)?;
            let secondary_ty = resolve_expr_type(catalog, secondary, ctx, params, scope)?;

            for ty in [primary_ty, secondary_ty] {
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

            if let Some(k_expr) = k {
                let k_type = resolve_expr_type(catalog, k_expr, ctx, params, scope)?;
                match k_type {
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
                if let Expr::Literal(Literal::Integer(v)) = k_expr.as_ref()
                    && *v <= 0
                {
                    return Err(CompilerError::typed(
                        T21,
                        "rrf k must be greater than 0".to_string(),
                    ));
                }
            }

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::F64,
                false,
            )))
        }
        Expr::Variable(name) => {
            // Could be a query parameter or a bound variable
            if let Some(prop_type) = params.get(name) {
                Ok(ResolvedType::Scalar(prop_type.clone()))
            } else if !matches!(scope, Scope::Read) {
                Err(scope.undeclared_parameter(name))
            } else if let Some(bv) = ctx.bindings.get(name) {
                match bv {
                    BoundVariable::Node { type_name } => Ok(ResolvedType::Node(type_name.clone())),
                    BoundVariable::Edge { .. } => Err(CompilerError::typed(
                        T23,
                        format!(
                            "edge binding `${}` cannot be used bare; access one of its properties (`${}.{{prop}}`)",
                            name, name
                        ),
                    )),
                }
            } else {
                Err(CompilerError::typed(
                    T6,
                    format!("variable `${}` is not bound", name),
                ))
            }
        }
        Expr::Literal(lit) => Ok(ResolvedType::Scalar(literal_type(lit)?)),
        Expr::Aggregate { func, arg } => {
            let arg_type = resolve_expr_type(catalog, arg, ctx, params, scope)?;
            reject_blob_read_value(&arg_type, arg)?;
            check_aggregate_argument(func, arg, &arg_type)?;

            Ok(ResolvedType::Aggregate)
        }
        Expr::AliasRef(name) => match scope {
            Scope::Read => Ok(ctx
                .aliases
                .get(name)
                .cloned()
                .unwrap_or(ResolvedType::Aggregate)),
            Scope::MutationWhere(target) => {
                mutation_property_type(catalog, target, target.type_name(), name)
                    .map(ResolvedType::Scalar)
            }
            Scope::Constant { type_name, .. } => {
                Err(constant_leaf_refusal(type_name, type_name, name))
            }
        },
        Expr::Binary {
            left,
            op: BinaryOp::Compare(op),
            right,
        } => Ok(ResolvedType::Scalar(typecheck_comparison(
            catalog, left, *op, right, ctx, params, scope,
        )?)),
        Expr::Binary { left, op, right } => {
            let left_type = resolve_expr_type(catalog, left, ctx, params, scope)?;
            let right_type = resolve_expr_type(catalog, right, ctx, params, scope)?;
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
            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::Bool,
                l.nullable || r.nullable,
            )))
        }
        Expr::Not(inner) => {
            let inner_type = resolve_expr_type(catalog, inner, ctx, params, scope)?;
            let Some(b) = boolean_scalar(&inner_type) else {
                return Err(CompilerError::typed(
                    T41,
                    format!(
                        "`not` needs a Bool operand, got {}",
                        inner_type.display_name()
                    ),
                ));
            };
            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::Bool,
                b.nullable,
            )))
        }
        Expr::IsNull { expr: inner, .. } => {
            if let Scope::Read = scope
                && let Expr::PropAccess { variable, property } = inner.as_ref()
                && read_property_type(catalog, ctx, variable, property)?.scalar == ScalarType::Blob
            {
                return Err(CompilerError::typed(
                    T7,
                    "blob comparisons in filters are not supported".to_string(),
                ));
            }
            let inner_type = resolve_expr_type(catalog, inner, ctx, params, scope)?;
            if !matches!(inner_type, ResolvedType::Scalar(_)) {
                return Err(CompilerError::typed(
                    T41,
                    format!(
                        "`is null` tests a scalar or list value, got {}",
                        inner_type.display_name()
                    ),
                ));
            }
            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::Bool,
                false,
            )))
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
        Expr::Aggregate { func, arg } => match arg.as_ref() {
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
        Expr::Nearest { .. } | Expr::Bm25 { .. } => {
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
        Expr::Rrf { .. } => Err(CompilerError::typed(
            T37,
            "`rrf` cannot be projected in `return`; order by `rrf(...)` and project plain columns"
                .to_string(),
        )),
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
        Expr::Binary { left, right, .. } => {
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
        | Expr::IsNull { .. } => "expression",
    }
}

fn infer_projection_field(
    catalog: &Catalog,
    expr: &Expr,
    alias: Option<&str>,
    order_clause: &[Ordering],
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<Field> {
    let name = projection_name(expr, alias);
    match expr {
        Expr::Aggregate { func, arg } => {
            // Keep result-schema inference fail-closed even when a caller has
            // not first passed through `typecheck_read_query`. In particular,
            // Count's output shape is fixed, but its argument may still be an
            // unsupported Blob value.
            let resolved_arg = resolve_expr_type(catalog, arg, ctx, params, Scope::Read)?;
            reject_blob_read_value(&resolved_arg, arg)?;
            check_aggregate_argument(func, arg, &resolved_arg)?;
            check_projection(expr, alias, order_clause)?;
            let (data_type, nullable) = match func {
                AggFunc::Count => (DataType::Int64, true),
                AggFunc::Avg | AggFunc::Sum => (DataType::Float64, true),
                AggFunc::Min | AggFunc::Max => {
                    let (data_type, _) = resolved_type_to_field_shape(catalog, &resolved_arg)?;
                    (data_type, true)
                }
            };
            Ok(Field::new(name, data_type, nullable))
        }
        Expr::Nearest { .. } | Expr::Bm25 { .. } => {
            resolve_expr_type(catalog, expr, ctx, params, Scope::Read)?;
            check_projection(expr, alias, order_clause)?;
            Ok(Field::new(name, DataType::Float32, false))
        }
        _ => {
            let resolved = resolve_expr_type(catalog, expr, ctx, params, Scope::Read)?;
            reject_blob_read_value(&resolved, expr)?;
            check_projection(expr, alias, order_clause)?;
            let (data_type, nullable) = resolved_type_to_field_shape(catalog, &resolved)?;
            Ok(Field::new(name, data_type, nullable))
        }
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

fn projection_name(expr: &Expr, alias: Option<&str>) -> String {
    if let Some(alias) = alias {
        return alias.to_string();
    }

    match expr {
        Expr::Now => "now".to_string(),
        Expr::PropAccess { property, .. } => property.clone(),
        Expr::Variable(variable) => variable.clone(),
        Expr::Literal(_) => "literal".to_string(),
        Expr::Nearest { .. } | Expr::Bm25 { .. } => match expr.score_column() {
            Some((variable, column)) => format!("{variable}.{column}"),
            None => rank_keyword(expr).to_string(),
        },
        Expr::Search { .. } | Expr::Fuzzy { .. } | Expr::MatchText { .. } | Expr::Rrf { .. } => {
            rank_keyword(expr).to_string()
        }
        Expr::Aggregate { func, .. } => func.to_string(),
        Expr::AliasRef(name) => name.clone(),
        Expr::Binary { .. } | Expr::Not(_) | Expr::IsNull { .. } => "expression".to_string(),
    }
}

/// T8: `count` takes a scalar or a node, `sum`/`avg` a numeric, `min`/`max` an
/// orderable scalar; none takes an aggregate.
fn check_aggregate_argument(func: &AggFunc, arg: &Expr, arg_type: &ResolvedType) -> Result<()> {
    match (func, arg_type) {
        (_, ResolvedType::Aggregate) => Err(CompilerError::typed(
            T8,
            format!("{func} cannot take an aggregate or a forward alias reference as its argument"),
        )),
        (AggFunc::Count, _) => Ok(()),
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
        _ => Ok(()),
    }
}

fn resolved_type_to_field_shape(
    catalog: &Catalog,
    resolved: &ResolvedType,
) -> Result<(DataType, bool)> {
    match resolved {
        ResolvedType::Scalar(prop_type) => Ok((prop_type.to_arrow(), prop_type.nullable)),
        ResolvedType::Node(type_name) => {
            let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
                CompilerError::typed(T51, format!("type `{}` not found in catalog", type_name))
            })?;
            let fields: Vec<Field> = node_type
                .node_object_members()
                .map(|(member, field)| {
                    Field::new(member, field.data_type().clone(), field.is_nullable())
                })
                .collect();
            Ok((DataType::Struct(fields.into()), false))
        }
        ResolvedType::Aggregate => Ok((DataType::Int64, true)),
    }
}

fn literal_type(lit: &Literal) -> Result<PropType> {
    match lit {
        // Null is compatible with any nullable type; default to String for inference.
        Literal::Null => Ok(PropType::scalar(ScalarType::String, true)),
        Literal::String(_) => Ok(PropType::scalar(ScalarType::String, false)),
        Literal::Integer(_) => Ok(PropType::scalar(ScalarType::I64, false)),
        Literal::Float(_) => Ok(PropType::scalar(ScalarType::F64, false)),
        Literal::Bool(_) => Ok(PropType::scalar(ScalarType::Bool, false)),
        Literal::Date(value) => {
            crate::types::check_date_literal(value)
                .map_err(|reason| CompilerError::typed(T3, reason.to_string()))?;
            Ok(PropType::scalar(ScalarType::Date, false))
        }
        Literal::DateTime(_) => Ok(PropType::scalar(ScalarType::DateTime, false)),
        Literal::List(items) => {
            if items.is_empty() {
                return Ok(PropType::list_of(ScalarType::String, false));
            }
            let first = literal_type(&items[0])?;
            if first.list {
                return Err(CompilerError::typed(
                    T52,
                    "nested list literals are not supported".to_string(),
                ));
            }
            for item in items.iter().skip(1) {
                let item_type = literal_type(item)?;
                if item_type.list || !types_compatible(&first, &item_type) {
                    return Err(CompilerError::typed(
                        T53,
                        "list literal elements must share a compatible scalar type".to_string(),
                    ));
                }
            }
            Ok(PropType::list_of(first.scalar, false))
        }
    }
}

fn check_literal_type(lit: &Literal, expected: &PropType, prop_name: &str) -> Result<()> {
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

    let lit_type = literal_type(lit)?;
    if !types_compatible(&lit_type, expected) {
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
        Expr::Binary { left, right, .. } => {
            expr_references_any(left, vars) || expr_references_any(right, vars)
        }
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => expr_references_any(inner, vars),
        _ => false,
    }
}

/// T33 admits a projected rank expression only as a repeat of the leading
/// `order` key, so an alias never leads the order and the walk stops at
/// `AliasRef` (T18 refuses an alias key beside `nearest`).
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
        // nearest() nested under rrf() is handled by T21 and should not trigger T17/T18 checks.
        Expr::Rrf { .. } => false,
        Expr::Binary { left, right, .. } => {
            expr_contains_standalone_nearest(left) || expr_contains_standalone_nearest(right)
        }
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
        Expr::Binary { left, right, .. } => expr_contains_rrf(left) || expr_contains_rrf(right),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => expr_contains_rrf(inner),
        _ => false,
    }
}

#[cfg(test)]
#[path = "typecheck_tests.rs"]
mod tests;
