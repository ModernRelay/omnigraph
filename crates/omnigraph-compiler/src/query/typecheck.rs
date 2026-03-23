use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::catalog::Catalog;
use crate::error::{NanoError, Result};
use crate::types::{Direction, PropType, ScalarType};

use super::ast::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    Node,
    Edge,
}

#[derive(Debug, Clone)]
pub struct BoundVariable {
    pub var_name: String,
    pub type_name: String,
    pub kind: BindingKind,
}

#[derive(Debug, Clone)]
pub struct TypeContext {
    pub bindings: HashMap<String, BoundVariable>,
    pub aliases: HashMap<String, ResolvedType>,
    pub traversals: Vec<ResolvedTraversal>,
}

#[derive(Debug, Clone)]
pub struct ResolvedTraversal {
    pub src: String,
    pub dst: String,
    pub edge_type: String,
    pub direction: Direction,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedType {
    Scalar(PropType),
    Node(String),
    Aggregate,
}

#[derive(Debug, Clone)]
pub struct MutationTypeContext {
    pub target_type: String,
}

#[derive(Debug, Clone)]
pub enum CheckedQuery {
    Read(TypeContext),
    Mutation(MutationTypeContext),
}

pub fn typecheck_query_decl(catalog: &Catalog, query: &QueryDecl) -> Result<CheckedQuery> {
    if let Some(mutation) = &query.mutation {
        let target_type = typecheck_mutation(catalog, mutation, &query.params)?;
        Ok(CheckedQuery::Mutation(MutationTypeContext { target_type }))
    } else {
        Ok(CheckedQuery::Read(typecheck_read_query(catalog, query)?))
    }
}

pub fn typecheck_query(catalog: &Catalog, query: &QueryDecl) -> Result<TypeContext> {
    if query.mutation.is_some() {
        return Err(NanoError::Type(
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
            return Err(NanoError::Type(format!(
                "parameter name `${}` is reserved for runtime timestamp injection",
                NOW_PARAM_NAME
            )));
        }
        let scalar = ScalarType::from_str_name(&p.type_name).ok_or_else(|| {
            NanoError::Type(format!(
                "unknown parameter type `{}` for `${}`",
                p.type_name, p.name
            ))
        })?;
        out.insert(p.name.clone(), PropType::scalar(scalar, p.nullable));
    }
    Ok(out)
}

fn typecheck_read_query(catalog: &Catalog, query: &QueryDecl) -> Result<TypeContext> {
    let mut ctx = TypeContext {
        bindings: HashMap::new(),
        aliases: HashMap::new(),
        traversals: Vec::new(),
    };
    let mut alias_exprs: HashMap<String, &Expr> = HashMap::new();

    let params = parse_declared_param_types(&query.params)?;

    // Typecheck match clauses
    typecheck_clauses(catalog, &query.match_clause, &mut ctx, &params, false)?;

    // Typecheck return projections
    for proj in &query.return_clause {
        let resolved = resolve_expr_type(catalog, &proj.expr, &ctx, &params)?;
        if let Some(alias) = &proj.alias {
            ctx.aliases.insert(alias.clone(), resolved);
            alias_exprs.insert(alias.clone(), &proj.expr);
        }
    }

    // Typecheck order expressions
    for ord in &query.order_clause {
        resolve_expr_type(catalog, &ord.expr, &ctx, &params)?;
    }

    let has_standalone_nearest = query
        .order_clause
        .iter()
        .any(|ord| expr_contains_standalone_nearest_with_aliases(&ord.expr, &alias_exprs));
    let has_rrf = query
        .order_clause
        .iter()
        .any(|ord| expr_contains_rrf_with_aliases(&ord.expr, &alias_exprs));
    if has_rrf && query.limit.is_none() {
        return Err(NanoError::Type(
            "T21: rrf ordering requires a limit clause".to_string(),
        ));
    }
    if has_standalone_nearest && query.limit.is_none() {
        return Err(NanoError::Type(
            "T17: nearest ordering requires a limit clause".to_string(),
        ));
    }
    if has_standalone_nearest
        && query
            .order_clause
            .iter()
            .any(|ord| matches!(ord.expr, Expr::AliasRef(_)))
    {
        return Err(NanoError::Type(
            "T18: alias-based ordering is not supported together with nearest in phase 1"
                .to_string(),
        ));
    }

    Ok(ctx)
}

fn typecheck_mutation(catalog: &Catalog, mutation: &Mutation, params: &[Param]) -> Result<String> {
    let param_types = parse_declared_param_types(params)?;

    match mutation {
        Mutation::Insert(insert) => {
            if insert.assignments.is_empty() {
                return Err(NanoError::Type(
                    "T10: insert mutation requires at least one assignment".to_string(),
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
                                NanoError::Type(format!(
                                    "T11: type `{}` has no property `{}`",
                                    insert.type_name, assignment.property
                                ))
                            })?;
                    check_match_value_type(
                        &assignment.value,
                        &param_types,
                        prop_type,
                        &assignment.property,
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

                    if let Some(source_prop) = node_type.embed_sources.get(prop_name) {
                        if assigned_props.contains(source_prop.as_str()) {
                            continue;
                        }
                        return Err(NanoError::Type(format!(
                            "T12: insert for `{}` must provide non-nullable property `{}` or @embed source `{}`",
                            insert.type_name, prop_name, source_prop
                        )));
                    }

                    return Err(NanoError::Type(format!(
                        "T12: insert for `{}` must provide non-nullable property `{}`",
                        insert.type_name, prop_name
                    )));
                }
                return Ok(insert.type_name.clone());
            }

            if let Some(edge_type) = catalog.edge_types.get(&insert.type_name) {
                let mut has_from = false;
                let mut has_to = false;

                for assignment in &insert.assignments {
                    match assignment.property.as_str() {
                        "from" => {
                            has_from = true;
                            check_match_value_type(
                                &assignment.value,
                                &param_types,
                                &PropType::scalar(ScalarType::String, false),
                                "from",
                            )?;
                        }
                        "to" => {
                            has_to = true;
                            check_match_value_type(
                                &assignment.value,
                                &param_types,
                                &PropType::scalar(ScalarType::String, false),
                                "to",
                            )?;
                        }
                        _ => {
                            let prop_type = edge_type
                                .properties
                                .get(&assignment.property)
                                .ok_or_else(|| {
                                    NanoError::Type(format!(
                                        "T11: type `{}` has no property `{}`",
                                        insert.type_name, assignment.property
                                    ))
                                })?;
                            check_match_value_type(
                                &assignment.value,
                                &param_types,
                                prop_type,
                                &assignment.property,
                            )?;
                        }
                    }
                }

                if !has_from {
                    return Err(NanoError::Type(format!(
                        "T12: insert for `{}` must provide required endpoint `from`",
                        insert.type_name
                    )));
                }
                if !has_to {
                    return Err(NanoError::Type(format!(
                        "T12: insert for `{}` must provide required endpoint `to`",
                        insert.type_name
                    )));
                }

                for (prop_name, prop_type) in &edge_type.properties {
                    if prop_type.nullable {
                        continue;
                    }
                    if !insert.assignments.iter().any(|a| &a.property == prop_name) {
                        return Err(NanoError::Type(format!(
                            "T12: insert for `{}` must provide non-nullable property `{}`",
                            insert.type_name, prop_name
                        )));
                    }
                }
                return Ok(insert.type_name.clone());
            }

            Err(NanoError::Type(format!(
                "T10: unknown node/edge type `{}`",
                insert.type_name
            )))
        }
        Mutation::Update(update) => {
            let node_type = if let Some(node_type) = catalog.node_types.get(&update.type_name) {
                node_type
            } else if catalog.edge_types.contains_key(&update.type_name) {
                return Err(NanoError::Type(format!(
                    "T16: update mutation for edge type `{}` is not supported",
                    update.type_name
                )));
            } else {
                return Err(NanoError::Type(format!(
                    "T10: unknown node/edge type `{}`",
                    update.type_name
                )));
            };

            if update.assignments.is_empty() {
                return Err(NanoError::Type(
                    "T10: update mutation requires at least one assignment".to_string(),
                ));
            }
            ensure_no_duplicate_assignment_names(&update.assignments)?;

            for assignment in &update.assignments {
                let prop_type =
                    node_type
                        .properties
                        .get(&assignment.property)
                        .ok_or_else(|| {
                            NanoError::Type(format!(
                                "T11: type `{}` has no property `{}`",
                                update.type_name, assignment.property
                            ))
                        })?;
                check_match_value_type(
                    &assignment.value,
                    &param_types,
                    prop_type,
                    &assignment.property,
                )?;
            }

            typecheck_mutation_predicate(
                &update.type_name,
                &update.predicate,
                node_type,
                &param_types,
            )?;
            Ok(update.type_name.clone())
        }
        Mutation::Delete(delete) => {
            if let Some(node_type) = catalog.node_types.get(&delete.type_name) {
                typecheck_mutation_predicate(
                    &delete.type_name,
                    &delete.predicate,
                    node_type,
                    &param_types,
                )?;
                Ok(delete.type_name.clone())
            } else if let Some(edge_type) = catalog.edge_types.get(&delete.type_name) {
                typecheck_edge_mutation_predicate(
                    &delete.type_name,
                    &delete.predicate,
                    edge_type,
                    &param_types,
                )?;
                Ok(delete.type_name.clone())
            } else {
                Err(NanoError::Type(format!(
                    "T10: unknown node/edge type `{}`",
                    delete.type_name
                )))
            }
        }
    }
}

fn ensure_no_duplicate_assignment_names(assignments: &[MutationAssignment]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        if !seen.insert(&assignment.property) {
            return Err(NanoError::Type(format!(
                "T13: duplicate assignment for property `{}`",
                assignment.property
            )));
        }
    }
    Ok(())
}

fn typecheck_mutation_predicate(
    type_name: &str,
    predicate: &MutationPredicate,
    node_type: &crate::catalog::NodeType,
    param_types: &HashMap<String, PropType>,
) -> Result<()> {
    let prop_type = node_type
        .properties
        .get(&predicate.property)
        .ok_or_else(|| {
            NanoError::Type(format!(
                "T11: type `{}` has no property `{}`",
                type_name, predicate.property
            ))
        })?;
    check_match_value_type(
        &predicate.value,
        param_types,
        prop_type,
        &predicate.property,
    )?;
    Ok(())
}

fn typecheck_edge_mutation_predicate(
    type_name: &str,
    predicate: &MutationPredicate,
    edge_type: &crate::catalog::EdgeType,
    param_types: &HashMap<String, PropType>,
) -> Result<()> {
    if predicate.property == "from" || predicate.property == "to" {
        return check_match_value_type(
            &predicate.value,
            param_types,
            &PropType::scalar(ScalarType::String, false),
            &predicate.property,
        );
    }

    let prop_type = edge_type
        .properties
        .get(&predicate.property)
        .ok_or_else(|| {
            NanoError::Type(format!(
                "T11: type `{}` has no property `{}`",
                type_name, predicate.property
            ))
        })?;
    check_match_value_type(
        &predicate.value,
        param_types,
        prop_type,
        &predicate.property,
    )?;
    Ok(())
}

fn check_match_value_type(
    value: &MatchValue,
    params: &HashMap<String, PropType>,
    expected: &PropType,
    property: &str,
) -> Result<()> {
    match value {
        MatchValue::Literal(lit) => check_literal_type(lit, expected, property),
        MatchValue::Variable(v) => {
            let Some(actual) = params.get(v) else {
                return Err(NanoError::Type(format!(
                    "T14: mutation variable `${}` must be a declared query parameter",
                    v
                )));
            };
            if !types_compatible(actual, expected) {
                return Err(NanoError::Type(format!(
                    "T7: cannot assign/compare {} with {} for property `{}`",
                    actual.display_name(),
                    expected.display_name(),
                    property
                )));
            }
            Ok(())
        }
        MatchValue::Now => check_now_match_value_type(expected, property),
    }
}

fn check_now_match_value_type(expected: &PropType, property: &str) -> Result<()> {
    if expected.list || expected.scalar != ScalarType::DateTime {
        return Err(NanoError::Type(format!(
            "T7: cannot assign/compare DateTime with {} for property `{}`",
            expected.display_name(),
            property
        )));
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
            Clause::Negation(inner) => {
                // T9: at least one variable in the negation block must be bound outside
                let outer_vars: Vec<String> = ctx.bindings.keys().cloned().collect();

                // Typecheck inner clauses in a copy of ctx
                let mut inner_ctx = ctx.clone();
                typecheck_clauses(catalog, inner, &mut inner_ctx, params, true)?;

                // Check T9
                let mut has_outer = false;
                for clause in inner {
                    match clause {
                        Clause::Traversal(t) => {
                            if outer_vars.contains(&t.src) || outer_vars.contains(&t.dst) {
                                has_outer = true;
                            }
                        }
                        Clause::Filter(f) => {
                            if expr_references_any(&f.left, &outer_vars)
                                || expr_references_any(&f.right, &outer_vars)
                            {
                                has_outer = true;
                            }
                        }
                        Clause::Binding(b) => {
                            if outer_vars.contains(&b.variable) {
                                has_outer = true;
                            }
                        }
                        _ => {}
                    }
                }
                if !has_outer {
                    return Err(NanoError::Type(
                        "T9: negation block must reference at least one outer-bound variable"
                            .to_string(),
                    ));
                }
            }
        }
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
        return Err(NanoError::Type(format!(
            "T1: unknown node type `{}`",
            binding.type_name
        )));
    }

    let node_type = &catalog.node_types[&binding.type_name];

    // T2 + T3: property match fields must exist and have correct types
    for pm in &binding.prop_matches {
        let prop = node_type.properties.get(&pm.prop_name).ok_or_else(|| {
            NanoError::Type(format!(
                "T2: type `{}` has no property `{}`",
                binding.type_name, pm.prop_name
            ))
        })?;

        // T3: check value type matches property type
        match &pm.value {
            MatchValue::Literal(lit) => {
                check_binding_literal_type(lit, prop, &pm.prop_name)?;
            }
            MatchValue::Variable(v) => {
                if let Some(actual) = params.get(v) {
                    check_binding_variable_type(actual, prop, &pm.prop_name)?;
                }
            }
            MatchValue::Now => check_now_match_value_type(prop, &pm.prop_name)?,
        }
    }

    // Don't overwrite if already bound to same type (re-binding same var is OK)
    if let Some(existing) = ctx.bindings.get(&binding.variable)
        && existing.type_name != binding.type_name
    {
        return Err(NanoError::Type(format!(
            "variable `${}` already bound to type `{}`, cannot rebind to `{}`",
            binding.variable, existing.type_name, binding.type_name
        )));
    }

    ctx.bindings.insert(
        binding.variable.clone(),
        BoundVariable {
            var_name: binding.variable.clone(),
            type_name: binding.type_name.clone(),
            kind: BindingKind::Node,
        },
    );

    Ok(())
}

fn check_binding_literal_type(lit: &Literal, expected: &PropType, property: &str) -> Result<()> {
    if expected.list {
        let lit_type = literal_type(lit)?;
        if lit_type.list {
            return Err(NanoError::Type(format!(
                "T3: list equality is not supported for property `{}`; use a scalar value to match list membership",
                property
            )));
        }

        let expected_member = PropType::scalar(expected.scalar, expected.nullable);
        if !types_compatible(&lit_type, &expected_member) {
            return Err(NanoError::Type(format!(
                "T3: property `{}` has type {} but membership match got {}",
                property,
                expected.display_name(),
                lit_type.display_name()
            )));
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
            return Err(NanoError::Type(format!(
                "T7: list equality is not supported for property `{}`; use a scalar parameter for membership matching",
                property
            )));
        }

        let expected_member = PropType::scalar(expected.scalar, expected.nullable);
        if !types_compatible(actual, &expected_member) {
            return Err(NanoError::Type(format!(
                "T7: cannot compare {} membership against {} for property `{}`",
                actual.display_name(),
                expected.display_name(),
                property
            )));
        }
        return Ok(());
    }

    if !types_compatible(actual, expected) {
        return Err(NanoError::Type(format!(
            "T7: cannot assign/compare {} with {} for property `{}`",
            actual.display_name(),
            expected.display_name(),
            property
        )));
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
            NanoError::Type(format!("T4: unknown edge type `{}`", traversal.edge_name))
        })?;

    if traversal.min_hops == 0 {
        return Err(NanoError::Type(
            "T15: traversal min hop bound must be >= 1".to_string(),
        ));
    }
    if let Some(max_hops) = traversal.max_hops {
        if max_hops < traversal.min_hops {
            return Err(NanoError::Type(format!(
                "T15: invalid traversal bounds {{{},{}}}; max must be >= min",
                traversal.min_hops, max_hops
            )));
        }
    } else {
        return Err(NanoError::Type(
            "T15: unbounded traversal is disabled; use bounded traversal {min,max}".to_string(),
        ));
    }

    // Determine direction based on bound variables and edge endpoints
    let src_bound = ctx.bindings.get(&traversal.src);
    let dst_bound = ctx.bindings.get(&traversal.dst);

    let direction;

    if let Some(src_bv) = src_bound {
        // T5: src type must match one endpoint of the edge
        if src_bv.type_name == edge.from_type {
            direction = Direction::Out;
            // dst should be edge.to_type
            bind_traversal_endpoint(ctx, &traversal.dst, &edge.to_type, edge)?;
        } else if src_bv.type_name == edge.to_type {
            direction = Direction::In;
            // dst should be edge.from_type
            bind_traversal_endpoint(ctx, &traversal.dst, &edge.from_type, edge)?;
        } else {
            return Err(NanoError::Type(format!(
                "T5: variable `${}` has type `{}`, which is not an endpoint of edge `{}: {} -> {}`",
                traversal.src, src_bv.type_name, edge.name, edge.from_type, edge.to_type
            )));
        }
    } else if let Some(dst_bv) = dst_bound {
        // dst is bound, infer direction from it
        if dst_bv.type_name == edge.to_type {
            direction = Direction::Out;
            bind_traversal_endpoint(ctx, &traversal.src, &edge.from_type, edge)?;
        } else if dst_bv.type_name == edge.from_type {
            direction = Direction::In;
            bind_traversal_endpoint(ctx, &traversal.src, &edge.to_type, edge)?;
        } else {
            return Err(NanoError::Type(format!(
                "T5: variable `${}` has type `{}`, which is not an endpoint of edge `{}: {} -> {}`",
                traversal.dst, dst_bv.type_name, edge.name, edge.from_type, edge.to_type
            )));
        }
    } else {
        // Neither bound — default Out direction, bind both
        direction = Direction::Out;
        bind_traversal_endpoint(ctx, &traversal.src, &edge.from_type, edge)?;
        bind_traversal_endpoint(ctx, &traversal.dst, &edge.to_type, edge)?;
    }

    ctx.traversals.push(ResolvedTraversal {
        src: traversal.src.clone(),
        dst: traversal.dst.clone(),
        edge_type: edge.name.clone(),
        direction,
        min_hops: traversal.min_hops,
        max_hops: traversal.max_hops,
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
        if existing.type_name != expected_type {
            return Err(NanoError::Type(format!(
                "T5: variable `${}` has type `{}` but edge `{}` expects `{}`",
                var, existing.type_name, edge.name, expected_type
            )));
        }
    } else {
        ctx.bindings.insert(
            var.to_string(),
            BoundVariable {
                var_name: var.to_string(),
                type_name: expected_type.to_string(),
                kind: BindingKind::Node,
            },
        );
    }
    Ok(())
}

fn typecheck_filter(
    catalog: &Catalog,
    filter: &Filter,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<()> {
    let left_type = resolve_expr_type(catalog, &filter.left, ctx, params)?;
    let right_type = resolve_expr_type(catalog, &filter.right, ctx, params)?;

    if let (ResolvedType::Scalar(l), ResolvedType::Scalar(r)) = (&left_type, &right_type) {
        if filter.op == CompOp::Contains {
            if !l.list {
                return Err(NanoError::Type(format!(
                    "T7: contains requires a list property on the left, got {}",
                    l.display_name()
                )));
            }
            if r.list {
                return Err(NanoError::Type(
                    "T7: contains requires a scalar right operand".to_string(),
                ));
            }
            if matches!(l.scalar, ScalarType::Vector(_))
                || matches!(r.scalar, ScalarType::Vector(_))
            {
                return Err(NanoError::Type(
                    "T7: vector membership filters are not supported".to_string(),
                ));
            }

            let expected_member = PropType::scalar(l.scalar, l.nullable);
            if !types_compatible(&expected_member, r) {
                return Err(NanoError::Type(format!(
                    "T7: cannot test membership of {} in {}",
                    r.display_name(),
                    l.display_name()
                )));
            }
            return Ok(());
        }

        // T7: check type compatibility
        if l.list || r.list {
            return Err(NanoError::Type(
                "T7: list comparisons in filters are not supported; use `contains` for list membership".to_string(),
            ));
        }
        if matches!(l.scalar, ScalarType::Vector(_)) || matches!(r.scalar, ScalarType::Vector(_)) {
            return Err(NanoError::Type(
                "T7: vector comparisons in filters are not supported".to_string(),
            ));
        }
        if !types_compatible(l, r) {
            return Err(NanoError::Type(format!(
                "T7: cannot compare {} with {}",
                l.display_name(),
                r.display_name()
            )));
        }
    }

    Ok(())
}

fn resolve_expr_type(
    catalog: &Catalog,
    expr: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<ResolvedType> {
    match expr {
        Expr::Now => Ok(ResolvedType::Scalar(PropType::scalar(
            ScalarType::DateTime,
            false,
        ))),
        Expr::PropAccess { variable, property } => {
            // T6: variable must be bound and property must exist
            let bv = ctx.bindings.get(variable).ok_or_else(|| {
                NanoError::Type(format!("T6: variable `${}` is not bound", variable))
            })?;

            let node_type = catalog.node_types.get(&bv.type_name).ok_or_else(|| {
                NanoError::Type(format!("T6: type `{}` not found in catalog", bv.type_name))
            })?;

            let prop = node_type.properties.get(property).ok_or_else(|| {
                NanoError::Type(format!(
                    "T6: type `{}` has no property `{}`",
                    bv.type_name, property
                ))
            })?;

            Ok(ResolvedType::Scalar(prop.clone()))
        }
        Expr::Nearest {
            variable,
            property,
            query,
        } => {
            let node_binding = ctx.bindings.get(variable).ok_or_else(|| {
                NanoError::Type(format!("T15: variable `${}` is not bound", variable))
            })?;
            let node_type = catalog
                .node_types
                .get(&node_binding.type_name)
                .ok_or_else(|| {
                    NanoError::Type(format!(
                        "T15: type `{}` not found in catalog",
                        node_binding.type_name
                    ))
                })?;
            let prop_type = node_type.properties.get(property).ok_or_else(|| {
                NanoError::Type(format!(
                    "T15: type `{}` has no property `{}`",
                    node_binding.type_name, property
                ))
            })?;
            let vector_dim = match prop_type.scalar {
                ScalarType::Vector(dim) => dim,
                _ => {
                    return Err(NanoError::Type(format!(
                        "T15: nearest requires a Vector property, got {}.{}: {}",
                        node_binding.type_name,
                        property,
                        prop_type.display_name()
                    )));
                }
            };
            if prop_type.list {
                return Err(NanoError::Type(
                    "T15: nearest does not support list-wrapped vectors".to_string(),
                ));
            }

            if let Expr::Literal(lit) = query.as_ref()
                && let Some(dim) = numeric_vector_literal_dim(lit)
            {
                if dim != vector_dim {
                    return Err(NanoError::Type(format!(
                        "T15: nearest vector dimension mismatch: property is Vector({}), query literal has {} elements",
                        vector_dim, dim
                    )));
                }
                return Ok(ResolvedType::Scalar(PropType::scalar(
                    ScalarType::F64,
                    false,
                )));
            }

            let query_type = resolve_expr_type(catalog, query, ctx, params)?;
            match query_type {
                ResolvedType::Scalar(s) if matches!(s.scalar, ScalarType::Vector(_)) && !s.list => {
                    let qdim = match s.scalar {
                        ScalarType::Vector(dim) => dim,
                        _ => unreachable!(),
                    };
                    if qdim != vector_dim {
                        return Err(NanoError::Type(format!(
                            "T15: nearest vector dimension mismatch: property is Vector({}), query is Vector({})",
                            vector_dim, qdim
                        )));
                    }
                }
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {
                    // query-time string embedding is supported in phase 3
                }
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T15: nearest query must be Vector({}) or String, got {}",
                        vector_dim,
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T15: nearest query must be a scalar expression".to_string(),
                    ));
                }
            }

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::F64,
                false,
            )))
        }
        Expr::Search { field, query } => {
            let field_type = resolve_expr_type(catalog, field, ctx, params)?;
            match field_type {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T19: search field must be String, got {}",
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T19: search field must be a scalar String expression".to_string(),
                    ));
                }
            }

            let query_type = resolve_expr_type(catalog, query, ctx, params)?;
            match query_type {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T19: search query must be String, got {}",
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T19: search query must be a scalar String expression".to_string(),
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
            let field_type = resolve_expr_type(catalog, field, ctx, params)?;
            match field_type {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T19: fuzzy field must be String, got {}",
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T19: fuzzy field must be a scalar String expression".to_string(),
                    ));
                }
            }

            let query_type = resolve_expr_type(catalog, query, ctx, params)?;
            match query_type {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T19: fuzzy query must be String, got {}",
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T19: fuzzy query must be a scalar String expression".to_string(),
                    ));
                }
            }

            if let Some(max_edits_expr) = max_edits {
                let max_edits_type = resolve_expr_type(catalog, max_edits_expr, ctx, params)?;
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
                        return Err(NanoError::Type(format!(
                            "T19: fuzzy max_edits must be an integer scalar, got {}",
                            s.display_name()
                        )));
                    }
                    _ => {
                        return Err(NanoError::Type(
                            "T19: fuzzy max_edits must be an integer scalar expression".to_string(),
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
            let field_type = resolve_expr_type(catalog, field, ctx, params)?;
            match field_type {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T20: match_text field must be String, got {}",
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T20: match_text field must be a scalar String expression".to_string(),
                    ));
                }
            }

            let query_type = resolve_expr_type(catalog, query, ctx, params)?;
            match query_type {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T20: match_text query must be String, got {}",
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T20: match_text query must be a scalar String expression".to_string(),
                    ));
                }
            }

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::Bool,
                false,
            )))
        }
        Expr::Bm25 { field, query } => {
            let field_type = resolve_expr_type(catalog, field, ctx, params)?;
            match field_type {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T20: bm25 field must be String, got {}",
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T20: bm25 field must be a scalar String expression".to_string(),
                    ));
                }
            }

            let query_type = resolve_expr_type(catalog, query, ctx, params)?;
            match query_type {
                ResolvedType::Scalar(s) if s.scalar == ScalarType::String && !s.list => {}
                ResolvedType::Scalar(s) => {
                    return Err(NanoError::Type(format!(
                        "T20: bm25 query must be String, got {}",
                        s.display_name()
                    )));
                }
                _ => {
                    return Err(NanoError::Type(
                        "T20: bm25 query must be a scalar String expression".to_string(),
                    ));
                }
            }

            Ok(ResolvedType::Scalar(PropType::scalar(
                ScalarType::F64,
                false,
            )))
        }
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => {
            if !matches!(primary.as_ref(), Expr::Nearest { .. } | Expr::Bm25 { .. }) {
                return Err(NanoError::Type(
                    "T21: rrf primary expression must be nearest(...) or bm25(...)".to_string(),
                ));
            }
            if !matches!(secondary.as_ref(), Expr::Nearest { .. } | Expr::Bm25 { .. }) {
                return Err(NanoError::Type(
                    "T21: rrf secondary expression must be nearest(...) or bm25(...)".to_string(),
                ));
            }

            let primary_ty = resolve_expr_type(catalog, primary, ctx, params)?;
            let secondary_ty = resolve_expr_type(catalog, secondary, ctx, params)?;

            for ty in [primary_ty, secondary_ty] {
                match ty {
                    ResolvedType::Scalar(s) if s.scalar == ScalarType::F64 && !s.list => {}
                    ResolvedType::Scalar(s) => {
                        return Err(NanoError::Type(format!(
                            "T21: rrf rank expressions must evaluate to F64, got {}",
                            s.display_name()
                        )));
                    }
                    _ => {
                        return Err(NanoError::Type(
                            "T21: rrf rank expressions must be scalar numeric expressions"
                                .to_string(),
                        ));
                    }
                }
            }

            if let Some(k_expr) = k {
                let k_type = resolve_expr_type(catalog, k_expr, ctx, params)?;
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
                        return Err(NanoError::Type(format!(
                            "T21: rrf k must be an integer scalar, got {}",
                            s.display_name()
                        )));
                    }
                    _ => {
                        return Err(NanoError::Type(
                            "T21: rrf k must be an integer scalar expression".to_string(),
                        ));
                    }
                }
                if let Expr::Literal(Literal::Integer(v)) = k_expr.as_ref()
                    && *v <= 0
                {
                    return Err(NanoError::Type(
                        "T21: rrf k must be greater than 0".to_string(),
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
            } else if let Some(bv) = ctx.bindings.get(name) {
                Ok(ResolvedType::Node(bv.type_name.clone()))
            } else {
                Err(NanoError::Type(format!(
                    "variable `${}` is not bound",
                    name
                )))
            }
        }
        Expr::Literal(lit) => Ok(ResolvedType::Scalar(literal_type(lit)?)),
        Expr::Aggregate { func, arg } => {
            let arg_type = resolve_expr_type(catalog, arg, ctx, params)?;

            // T8: sum/avg/min/max require numeric
            match func {
                AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
                    if let ResolvedType::Scalar(s) = &arg_type
                        && (s.list || !s.scalar.is_numeric())
                    {
                        return Err(NanoError::Type(format!(
                            "T8: {} requires numeric type, got {}",
                            func,
                            s.display_name()
                        )));
                    }
                }
                _ => {} // count works on any type
            }

            Ok(ResolvedType::Aggregate)
        }
        Expr::AliasRef(name) => {
            // Check if it's a known alias from return clause
            if let Some(resolved) = ctx.aliases.get(name) {
                Ok(resolved.clone())
            } else {
                // Might be an alias not yet registered (forward reference in order)
                Ok(ResolvedType::Aggregate)
            }
        }
    }
}

fn infer_projection_field(
    catalog: &Catalog,
    expr: &Expr,
    alias: Option<&str>,
    ctx: &TypeContext,
    params: &HashMap<String, PropType>,
) -> Result<Field> {
    let name = projection_name(expr, alias);
    match expr {
        Expr::Aggregate { func, arg } => {
            let (data_type, nullable) = match func {
                AggFunc::Count => (DataType::Int64, true),
                AggFunc::Avg => (DataType::Float64, true),
                _ => {
                    let resolved = resolve_expr_type(catalog, arg, ctx, params)?;
                    let (data_type, _) = resolved_type_to_field_shape(catalog, &resolved)?;
                    (data_type, true)
                }
            };
            Ok(Field::new(name, data_type, nullable))
        }
        _ => {
            let resolved = resolve_expr_type(catalog, expr, ctx, params)?;
            let (data_type, nullable) = resolved_type_to_field_shape(catalog, &resolved)?;
            Ok(Field::new(name, data_type, nullable))
        }
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
        Expr::Nearest { .. } => "nearest".to_string(),
        Expr::Search { .. } => "search".to_string(),
        Expr::Fuzzy { .. } => "fuzzy".to_string(),
        Expr::MatchText { .. } => "match_text".to_string(),
        Expr::Bm25 { .. } => "bm25".to_string(),
        Expr::Rrf { .. } => "rrf".to_string(),
        Expr::Aggregate { func, .. } => func.to_string(),
        Expr::AliasRef(name) => name.clone(),
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
                NanoError::Type(format!("type `{}` not found in catalog", type_name))
            })?;
            let fields: Vec<Field> = node_type
                .arrow_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect();
            Ok((DataType::Struct(fields.into()), false))
        }
        ResolvedType::Aggregate => Ok((DataType::Int64, true)),
    }
}

fn literal_type(lit: &Literal) -> Result<PropType> {
    match lit {
        Literal::String(_) => Ok(PropType::scalar(ScalarType::String, false)),
        Literal::Integer(_) => Ok(PropType::scalar(ScalarType::I64, false)),
        Literal::Float(_) => Ok(PropType::scalar(ScalarType::F64, false)),
        Literal::Bool(_) => Ok(PropType::scalar(ScalarType::Bool, false)),
        Literal::Date(_) => Ok(PropType::scalar(ScalarType::Date, false)),
        Literal::DateTime(_) => Ok(PropType::scalar(ScalarType::DateTime, false)),
        Literal::List(items) => {
            if items.is_empty() {
                return Ok(PropType::list_of(ScalarType::String, false));
            }
            let first = literal_type(&items[0])?;
            if first.list {
                return Err(NanoError::Type(
                    "nested list literals are not supported".to_string(),
                ));
            }
            for item in items.iter().skip(1) {
                let item_type = literal_type(item)?;
                if item_type.list || !types_compatible(&first, &item_type) {
                    return Err(NanoError::Type(
                        "list literal elements must share a compatible scalar type".to_string(),
                    ));
                }
            }
            Ok(PropType::list_of(first.scalar, false))
        }
    }
}

fn check_literal_type(lit: &Literal, expected: &PropType, prop_name: &str) -> Result<()> {
    if !expected.list
        && let ScalarType::Vector(expected_dim) = expected.scalar
        && let Some(actual_dim) = numeric_vector_literal_dim(lit)
    {
        if actual_dim == expected_dim {
            return Ok(());
        }
        return Err(NanoError::Type(format!(
            "T3: property `{}` has type Vector({}) but got vector literal with {} elements",
            prop_name, expected_dim, actual_dim
        )));
    }

    let lit_type = literal_type(lit)?;
    if !types_compatible(&lit_type, expected) {
        return Err(NanoError::Type(format!(
            "T3: property `{}` has type {} but got {}",
            prop_name,
            expected.display_name(),
            lit_type.display_name()
        )));
    }
    if expected.is_enum() {
        let allowed = expected.enum_values.as_ref().cloned().unwrap_or_default();
        match lit {
            Literal::String(v) => {
                if !allowed.contains(v) {
                    return Err(NanoError::Type(format!(
                        "T3: property `{}` expects one of [{}], got '{}'",
                        prop_name,
                        allowed.join(", "),
                        v
                    )));
                }
            }
            Literal::List(items) if expected.list => {
                for item in items {
                    match item {
                        Literal::String(v) if allowed.contains(v) => {}
                        Literal::String(v) => {
                            return Err(NanoError::Type(format!(
                                "T3: property `{}` expects one of [{}], got '{}'",
                                prop_name,
                                allowed.join(", "),
                                v
                            )));
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
        _ => false,
    }
}

fn expr_contains_standalone_nearest_with_aliases(
    expr: &Expr,
    alias_exprs: &HashMap<String, &Expr>,
) -> bool {
    expr_contains_standalone_nearest_inner(expr, alias_exprs, &mut HashSet::new())
}

fn expr_contains_standalone_nearest_inner(
    expr: &Expr,
    alias_exprs: &HashMap<String, &Expr>,
    seen_aliases: &mut HashSet<String>,
) -> bool {
    match expr {
        Expr::Nearest { .. } => true,
        Expr::Aggregate { arg, .. } => {
            expr_contains_standalone_nearest_inner(arg, alias_exprs, seen_aliases)
        }
        Expr::Search { field, query }
        | Expr::MatchText { field, query }
        | Expr::Bm25 { field, query } => {
            expr_contains_standalone_nearest_inner(field, alias_exprs, seen_aliases)
                || expr_contains_standalone_nearest_inner(query, alias_exprs, seen_aliases)
        }
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            expr_contains_standalone_nearest_inner(field, alias_exprs, seen_aliases)
                || expr_contains_standalone_nearest_inner(query, alias_exprs, seen_aliases)
                || max_edits.as_deref().is_some_and(|expr| {
                    expr_contains_standalone_nearest_inner(expr, alias_exprs, seen_aliases)
                })
        }
        Expr::AliasRef(name) => {
            if !seen_aliases.insert(name.clone()) {
                return false;
            }
            let found = alias_exprs.get(name).is_some_and(|expr| {
                expr_contains_standalone_nearest_inner(expr, alias_exprs, seen_aliases)
            });
            seen_aliases.remove(name);
            found
        }
        // nearest() nested under rrf() is handled by T21 and should not trigger T17/T18 checks.
        Expr::Rrf { .. } => false,
        _ => false,
    }
}

fn expr_contains_rrf_with_aliases(expr: &Expr, alias_exprs: &HashMap<String, &Expr>) -> bool {
    expr_contains_rrf_inner(expr, alias_exprs, &mut HashSet::new())
}

fn expr_contains_rrf_inner(
    expr: &Expr,
    alias_exprs: &HashMap<String, &Expr>,
    seen_aliases: &mut HashSet<String>,
) -> bool {
    match expr {
        Expr::Rrf { .. } => true,
        Expr::Aggregate { arg, .. } => expr_contains_rrf_inner(arg, alias_exprs, seen_aliases),
        Expr::Search { field, query }
        | Expr::MatchText { field, query }
        | Expr::Bm25 { field, query } => {
            expr_contains_rrf_inner(field, alias_exprs, seen_aliases)
                || expr_contains_rrf_inner(query, alias_exprs, seen_aliases)
        }
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            expr_contains_rrf_inner(field, alias_exprs, seen_aliases)
                || expr_contains_rrf_inner(query, alias_exprs, seen_aliases)
                || max_edits
                    .as_deref()
                    .is_some_and(|expr| expr_contains_rrf_inner(expr, alias_exprs, seen_aliases))
        }
        Expr::AliasRef(name) => {
            if !seen_aliases.insert(name.clone()) {
                return false;
            }
            let found = alias_exprs
                .get(name)
                .is_some_and(|expr| expr_contains_rrf_inner(expr, alias_exprs, seen_aliases));
            seen_aliases.remove(name);
            found
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::query::parser::parse_query;
    use crate::schema::parser::parse_schema;

    fn setup() -> Catalog {
        let schema = parse_schema(
            r#"
node Person {
    name: String
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company {
    title: String?
}
"#,
        )
        .unwrap();
        build_catalog(&schema).unwrap()
    }

    fn setup_vector() -> Catalog {
        let schema = parse_schema(
            r#"
node Doc {
    id_str: String
    embedding: Vector(3)
}
"#,
        )
        .unwrap();
        build_catalog(&schema).unwrap()
    }

    fn setup_list() -> Catalog {
        let schema = parse_schema(
            r#"
node Person {
    name: String
    tags: [String]?
}
"#,
        )
        .unwrap();
        build_catalog(&schema).unwrap()
    }

    fn setup_embed_vector() -> Catalog {
        let schema = parse_schema(
            r#"
node Doc {
    slug: String
    body: String?
    embedding: Vector(3) @embed(body)
}
"#,
        )
        .unwrap();
        build_catalog(&schema).unwrap()
    }

    #[test]
    fn test_basic_binding() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_t1_unknown_type() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Foo }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T1"));
    }

    #[test]
    fn test_t2_unknown_property_match() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person { salary: 100 } }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T2"));
    }

    #[test]
    fn test_t3_wrong_type_in_match() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person { age: "old" } }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T3"));
    }

    #[test]
    fn test_list_membership_match_accepts_scalar_literal() {
        let catalog = setup_list();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person { tags: "rust" } }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_list_membership_match_accepts_scalar_param() {
        let catalog = setup_list();
        let qf = parse_query(
            r#"
query q($tag: String) {
    match { $p: Person { tags: $tag } }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_list_equality_match_is_rejected() {
        let catalog = setup_list();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person { tags: ["rust"] } }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("list equality is not supported"));
        assert!(msg.contains("membership"));
    }

    #[test]
    fn test_contains_filter_accepts_list_membership() {
        let catalog = setup_list();
        let qf = parse_query(
            r#"
query q($tag: String) {
    match {
        $p: Person
        $p.tags contains $tag
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_contains_filter_requires_list_left_operand() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p.name contains "Al"
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(
            err.to_string()
                .contains("contains requires a list property on the left")
        );
    }

    #[test]
    fn test_contains_filter_rejects_list_right_operand() {
        let catalog = setup_list();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p.tags contains ["rust"]
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(
            err.to_string()
                .contains("contains requires a scalar right operand")
        );
    }

    #[test]
    fn test_t4_unknown_edge() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p likes $f
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T4"));
    }

    #[test]
    fn test_t5_bad_endpoints() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $c: Company
        $c knows $f
    }
    return { $c.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T5"));
    }

    #[test]
    fn test_t6_bad_property() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p.salary > 100
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T6"));
    }

    #[test]
    fn test_t7_bad_comparison() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p.age > "old"
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T7"));
    }

    #[test]
    fn test_nearest_requires_limit() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($q: Vector(3)) {
    match { $d: Doc }
    return { $d.id_str }
    order { nearest($d.embedding, $q) }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T17"));
    }

    #[test]
    fn test_nearest_vector_dim_mismatch() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($q: Vector(2)) {
    match { $d: Doc }
    return { $d.id_str }
    order { nearest($d.embedding, $q) }
    limit 3
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T15"));
    }

    #[test]
    fn test_nearest_vector_param_ok() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($q: Vector(3)) {
    match { $d: Doc }
    return { $d.id_str }
    order { nearest($d.embedding, $q) }
    limit 3
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("d"));
    }

    #[test]
    fn test_nearest_string_param_ok() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($q: String) {
    match { $d: Doc }
    return { $d.id_str }
    order { nearest($d.embedding, $q) }
    limit 3
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("d"));
    }

    #[test]
    fn test_search_string_param_ok() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($q: String) {
    match {
        $p: Person
        search($p.name, $q)
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_fuzzy_max_edits_param_ok() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($q: String, $m: I64) {
    match {
        $p: Person
        fuzzy($p.name, $q, $m)
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_fuzzy_rejects_non_integer_max_edits() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($q: String, $m: F64) {
    match {
        $p: Person
        fuzzy($p.name, $q, $m)
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T19"));
    }

    #[test]
    fn test_match_text_string_param_ok() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($q: String) {
    match {
        $p: Person
        match_text($p.name, $q)
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_bm25_string_param_ok() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($q: String) {
    match { $p: Person }
    return { $p.name, bm25($p.name, $q) as score }
    order { bm25($p.name, $q) desc }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_bm25_rejects_non_string_query() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($q: I64) {
    match { $p: Person }
    return { bm25($p.name, $q) as score }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T20"));
    }

    #[test]
    fn test_rrf_requires_limit_in_order() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($vq: Vector(3), $tq: String) {
    match { $d: Doc }
    return { $d.id_str }
    order { rrf(nearest($d.embedding, $vq), bm25($d.id_str, $tq), 60) desc }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T21"));
    }

    #[test]
    fn test_rrf_ordering_ok_with_limit() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($vq: Vector(3), $tq: String) {
    match { $d: Doc }
    return { $d.id_str }
    order { rrf(nearest($d.embedding, $vq), bm25($d.id_str, $tq), 60) desc }
    limit 5
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("d"));
    }

    #[test]
    fn test_rrf_with_nearest_allows_alias_ordering() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($vq: Vector(3), $tq: String) {
    match { $d: Doc }
    return {
        $d.id_str,
        rrf(nearest($d.embedding, $vq), bm25($d.id_str, $tq), 60) as score
    }
    order {
        rrf(nearest($d.embedding, $vq), bm25($d.id_str, $tq), 60) desc,
        score desc
    }
    limit 5
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("d"));
    }

    #[test]
    fn test_rrf_alias_ordering_requires_limit() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($vq: Vector(3), $tq: String) {
    match { $d: Doc }
    return {
        $d.id_str,
        rrf(nearest($d.embedding, $vq), bm25($d.id_str, $tq), 60) as score
    }
    order { score desc }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T21"));
    }

    #[test]
    fn test_rrf_alias_ordering_with_limit_is_valid() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($vq: Vector(3), $tq: String) {
    match { $d: Doc }
    return {
        $d.id_str,
        rrf(nearest($d.embedding, $vq), bm25($d.id_str, $tq), 60) as score
    }
    order { score desc }
    limit 5
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("d"));
    }

    #[test]
    fn test_standalone_nearest_with_alias_ordering_still_rejected() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($vq: Vector(3)) {
    match { $d: Doc }
    return {
        $d.id_str as score
    }
    order {
        nearest($d.embedding, $vq),
        score desc
    }
    limit 5
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T18"));
    }

    #[test]
    fn test_rrf_rejects_non_rank_expression_argument() {
        let parse = parse_query(
            r#"
query q($q: String) {
    match { $d: Doc }
    return { $d.id_str }
    order { rrf(bm25($d.id_str, $q), search($d.id_str, $q), 60) desc }
    limit 5
}
"#,
        );
        assert!(parse.is_err());
    }

    #[test]
    fn test_rrf_rejects_non_positive_k_literal() {
        let catalog = setup_vector();
        let qf = parse_query(
            r#"
query q($vq: Vector(3), $tq: String) {
    match { $d: Doc }
    return { $d.id_str }
    order { rrf(nearest($d.embedding, $vq), bm25($d.id_str, $tq), 0) desc }
    limit 5
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T21"));
    }

    #[test]
    fn test_t8_sum_on_string() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person }
    return { sum($p.name) as s }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T8"));
    }

    #[test]
    fn test_traversal_direction_out() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert_eq!(ctx.traversals[0].direction, Direction::Out);
        assert_eq!(ctx.bindings["f"].type_name, "Person");
    }

    #[test]
    fn test_traversal_direction_in() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $c: Company { name: "Acme" }
        $p worksAt $c
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        // $c is Company (to_type), $p is src — direction should be Out
        // because $p (Person=from_type) worksAt $c (Company=to_type) is forward
        assert_eq!(ctx.traversals[0].direction, Direction::Out);
    }

    #[test]
    fn test_bounded_traversal_typecheck() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows{1,3} $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert_eq!(ctx.traversals[0].min_hops, 1);
        assert_eq!(ctx.traversals[0].max_hops, Some(3));
    }

    #[test]
    fn test_bounded_traversal_invalid_bounds() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows{3,1} $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T15"));
    }

    #[test]
    fn test_unbounded_traversal_is_disabled() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows{1,} $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("unbounded traversal is disabled"));
    }

    #[test]
    fn test_negation_typecheck() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_aggregation_typecheck() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows $f
    }
    return {
        $p.name
        count($f) as friends
    }
}
"#,
        )
        .unwrap();
        typecheck_query(&catalog, &qf.queries[0]).unwrap();
    }

    #[test]
    fn test_valid_two_hop() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $mid
        $mid knows $fof
    }
    return { $fof.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("mid"));
        assert!(ctx.bindings.contains_key("fof"));
    }

    #[test]
    fn test_mutation_insert_typecheck_ok() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query add_person($name: String, $age: I32) {
    insert Person {
        name: $name
        age: $age
    }
}
"#,
        )
        .unwrap();
        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        match checked {
            CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_type, "Person"),
            _ => panic!("expected mutation typecheck result"),
        }
    }

    #[test]
    fn test_mutation_insert_missing_required_property() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query add_person($age: I32) {
    insert Person { age: $age }
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T12"));
    }

    #[test]
    fn test_mutation_insert_allows_embed_target_omission_when_source_present() {
        let catalog = setup_embed_vector();
        let qf = parse_query(
            r#"
query add_doc($slug: String, $body: String) {
    insert Doc {
        slug: $slug
        body: $body
    }
}
"#,
        )
        .unwrap();
        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        match checked {
            CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_type, "Doc"),
            _ => panic!("expected mutation typecheck result"),
        }
    }

    #[test]
    fn test_mutation_insert_requires_embed_source_when_target_omitted() {
        let catalog = setup_embed_vector();
        let qf = parse_query(
            r#"
query add_doc($slug: String) {
    insert Doc {
        slug: $slug
    }
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("T12"));
        assert!(msg.contains("embedding"));
        assert!(msg.contains("body"));
    }

    #[test]
    fn test_mutation_update_bad_property() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query update_person($name: String) {
    update Person set { salary: 100 } where name = $name
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T11"));
    }

    #[test]
    fn test_mutation_delete_bad_type() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query del($name: String) {
    delete Unknown where name = $name
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T10"));
    }

    #[test]
    fn test_mutation_insert_edge_typecheck_ok() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query add_knows($from: String, $to: String) {
    insert Knows {
        from: $from
        to: $to
    }
}
"#,
        )
        .unwrap();
        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        match checked {
            CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_type, "Knows"),
            _ => panic!("expected mutation typecheck result"),
        }
    }

    #[test]
    fn test_mutation_insert_edge_requires_from_and_to() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query add_knows($from: String) {
    insert Knows {
        from: $from
    }
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T12"));
    }

    #[test]
    fn test_mutation_delete_edge_typecheck_ok() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query del_knows($from: String) {
    delete Knows where from = $from
}
"#,
        )
        .unwrap();
        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        match checked {
            CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_type, "Knows"),
            _ => panic!("expected mutation typecheck result"),
        }
    }

    #[test]
    fn test_mutation_update_edge_not_supported() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query upd_knows($from: String) {
    update Knows set { since: 2000 } where from = $from
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T16"));
    }

    #[test]
    fn test_now_expression_typechecks_as_datetime() {
        let schema = parse_schema(
            r#"
node Event {
    slug: String @key
    at: DateTime
}
"#,
        )
        .unwrap();
        let catalog = build_catalog(&schema).unwrap();
        let qf = parse_query(
            r#"
query due() {
    match {
        $e: Event
        $e.at <= now()
    }
    return { now() as ts }
}
"#,
        )
        .unwrap();

        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        assert!(matches!(checked, CheckedQuery::Read(_)));
    }

    #[test]
    fn test_now_is_rejected_for_non_datetime_mutation_property() {
        let schema = parse_schema(
            r#"
node Event {
    slug: String @key
    on: Date
}
"#,
        )
        .unwrap();
        let catalog = build_catalog(&schema).unwrap();
        let qf = parse_query(
            r#"
query stamp() {
    update Event set { on: now() } where slug = "launch"
}
"#,
        )
        .unwrap();

        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("DateTime"));
        assert!(err.to_string().contains("property `on`"));
    }
}
