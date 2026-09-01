use std::collections::{HashMap, HashSet, VecDeque};

use crate::catalog::Catalog;
use crate::error::{CompilerError, Result};
use crate::query::ast::*;
use crate::query::typecheck::{BoundVariable, TypeContext};
use crate::types::{Direction, PropType, ScalarType};

use super::*;

pub fn lower_query(
    catalog: &Catalog,
    query: &QueryDecl,
    type_ctx: &TypeContext,
) -> Result<QueryIR> {
    if !query.mutations.is_empty() {
        return Err(crate::error::CompilerError::Plan(
            "cannot lower mutation query with read-query lowerer".to_string(),
        ));
    }
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();
    // Param types were validated during typecheck; unknown names simply
    // don't participate in `contains` overload resolution below.
    let param_types: HashMap<String, PropType> = query
        .params
        .iter()
        .filter_map(|p| {
            PropType::from_param_type_name(&p.type_name, p.nullable).map(|t| (p.name.clone(), t))
        })
        .collect();

    let mut pipeline = Vec::new();
    let mut bound_vars = HashSet::new();

    lower_clauses(
        catalog,
        &query.match_clause,
        type_ctx,
        &mut pipeline,
        &mut bound_vars,
        &param_names,
        &param_types,
    )?;

    let return_exprs: Vec<IRProjection> = query
        .return_clause
        .iter()
        .map(|p| IRProjection {
            expr: lower_expr(&p.expr, &param_names),
            alias: p.alias.clone(),
        })
        .collect();

    let order_by: Vec<IROrdering> = query
        .order_clause
        .iter()
        .map(|o| IROrdering {
            expr: lower_expr(&o.expr, &param_names),
            descending: o.descending,
        })
        .collect();

    let retrieval = lower_retrieval(&order_by, &return_exprs, query.limit)?;

    Ok(QueryIR {
        name: query.name.clone(),
        params: query.params.clone(),
        pipeline,
        return_exprs,
        order_by,
        limit: query.limit,
        retrieval,
    })
}

/// Multiplier on the query's limit for a capped BM25 scan; trades scan width
/// against how often the engine's uncapped under-fill retry is needed
/// (issue #563 — an unbounded ranked scan hydrates the whole matched corpus).
pub const BM25_SCAN_OVERFETCH_FACTOR: u64 = 4;

/// Row cap for a standalone `bm25()` scan, or `None` to scan every matching
/// document. `None` for a limitless query and for any aggregate return: an
/// aggregate's value is computed over the scanned rows, so a capped scan
/// would change the answer, not just the cost. Secondary order keys also
/// disqualify the cap: they rank WITHIN score ties, and a bounded scan
/// chooses which tied rows exist at all — the secondary sort over a
/// cap-arbitrary subset would be a silently wrong answer the engine's
/// under-fill retry cannot see (exactly `limit` rows still come back).
fn bm25_scan_cap(
    limit: Option<u64>,
    order_key_count: usize,
    return_exprs: &[IRProjection],
) -> Option<u64> {
    if return_exprs
        .iter()
        .any(|projection| matches!(&projection.expr, IRExpr::Aggregate { .. }))
    {
        return None;
    }
    if order_key_count > 1 {
        return None;
    }
    limit.map(|rows| rows.saturating_mul(BM25_SCAN_OVERFETCH_FACTOR))
}

/// Lower `order_by[0]` into the query's retrieval plan. A non-rank first
/// ordering (or no ordering) lowers no retrieval; the engine then runs a
/// plain scan-and-sort. Malformed rank shapes that typecheck already forbids
/// surface as `Plan` errors rather than silent plain modes.
fn lower_retrieval(
    order_by: &[IROrdering],
    return_exprs: &[IRProjection],
    limit: Option<u64>,
) -> Result<Option<RetrievalIR>> {
    let Some(first) = order_by.first() else {
        return Ok(None);
    };
    match &first.expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => Ok(Some(RetrievalIR::Nearest {
            variable: variable.clone(),
            property: property.clone(),
            query: query.clone(),
            k: limit,
        })),
        IRExpr::Bm25 { field, query } => Ok(Some(lower_bm25_leaf(
            field,
            query,
            bm25_scan_cap(limit, order_by.len(), return_exprs),
        )?)),
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => Ok(Some(RetrievalIR::FuseRrf {
            primary: Box::new(lower_rrf_arm(primary, limit)?),
            secondary: Box::new(lower_rrf_arm(secondary, limit)?),
            k: k.clone(),
            limit,
        })),
        _ => Ok(None),
    }
}

fn lower_rrf_arm(expr: &IRExpr, limit: Option<u64>) -> Result<RetrievalIR> {
    match expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => Ok(RetrievalIR::Nearest {
            variable: variable.clone(),
            property: property.clone(),
            query: query.clone(),
            // An arm's candidate count follows the fusion limit; 100 is the
            // long-standing fallback for the (typecheck-unreachable) case of
            // an rrf without a limit.
            k: Some(limit.unwrap_or(100)),
        }),
        // Never capped inside fusion — see `RetrievalIR::Bm25::scan_cap`.
        IRExpr::Bm25 { field, query } => lower_bm25_leaf(field, query, None),
        other => Err(CompilerError::Plan(format!(
            "rrf() arm must be nearest(...) or bm25(...), got {other:?}"
        ))),
    }
}

fn lower_bm25_leaf(field: &IRExpr, query: &IRExpr, scan_cap: Option<u64>) -> Result<RetrievalIR> {
    let IRExpr::PropAccess { variable, property } = field else {
        return Err(CompilerError::Plan(
            "bm25 field must be a property access".to_string(),
        ));
    };
    Ok(RetrievalIR::Bm25 {
        variable: variable.clone(),
        property: property.clone(),
        query: Box::new(query.clone()),
        scan_cap,
    })
}

/// Bindings that lowering DEFERS within one clause scope.
///
/// When multiple bindings in the same match clause are connected by
/// traversals, only the first-declared binding of each connected component
/// needs a NodeScan; the rest are introduced by Expand operations (making
/// them all NodeScans would trigger expensive cross-joins followed by
/// cycle-closing filters). Algorithm: build an undirected graph of variables
/// connected by traversals (the anonymous wildcard `_` never bridges
/// components), then walk components in binding declaration order — the
/// first binding in each component is the root; the rest are deferred.
pub(crate) fn deferred_binding_variables(clauses: &[Clause]) -> HashSet<String> {
    let mut bindings = Vec::new();
    let mut traversals = Vec::new();
    for clause in clauses {
        match clause {
            Clause::Binding(b) => bindings.push(b),
            Clause::Traversal(t) => traversals.push(t),
            Clause::Filter(_) | Clause::Negation(_) => {}
        }
    }

    let binding_set: HashSet<&str> = bindings.iter().map(|b| b.variable.as_str()).collect();

    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for t in &traversals {
        let src = t.src.as_str();
        let dst = t.dst.as_str();
        if src != "_" && dst != "_" {
            adj.entry(src).or_default().push(dst);
            adj.entry(dst).or_default().push(src);
        }
    }

    let mut deferred_set: HashSet<String> = HashSet::new();
    let mut component_visited: HashSet<&str> = HashSet::new();

    for binding in &bindings {
        if component_visited.contains(binding.variable.as_str()) {
            continue;
        }
        let mut queue = VecDeque::new();
        queue.push_back(binding.variable.as_str());
        let mut component_bindings: Vec<&str> = Vec::new();

        while let Some(var) = queue.pop_front() {
            if !component_visited.insert(var) {
                continue;
            }
            if binding_set.contains(var) {
                component_bindings.push(var);
            }
            if let Some(neighbours) = adj.get(var) {
                for &n in neighbours {
                    if !component_visited.contains(n) {
                        queue.push_back(n);
                    }
                }
            }
        }

        for var in component_bindings.into_iter().skip(1) {
            deferred_set.insert(var.to_string());
        }
    }
    deferred_set
}

/// Scan-rooted variables of one clause scope: the declared bindings lowering
/// gives a NodeScan (declared minus deferred). A search or rank function can
/// only run on a scan-rooted target — everything else is Expand-introduced
/// and has no scan to attach to. Consumed by typecheck's T26 rule.
pub(crate) fn scan_root_variables(clauses: &[Clause]) -> HashSet<String> {
    let deferred = deferred_binding_variables(clauses);
    clauses
        .iter()
        .filter_map(|clause| match clause {
            Clause::Binding(b) if !deferred.contains(b.variable.as_str()) => {
                Some(b.variable.clone())
            }
            _ => None,
        })
        .collect()
}

pub fn lower_mutation_query(query: &QueryDecl) -> Result<MutationIR> {
    if query.mutations.is_empty() {
        return Err(crate::error::CompilerError::Plan(
            "query does not contain a mutation body".to_string(),
        ));
    }
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();

    let ops = query
        .mutations
        .iter()
        .map(|m| lower_single_mutation(m, &param_names))
        .collect::<Result<Vec<_>>>()?;

    Ok(MutationIR {
        name: query.name.clone(),
        params: query.params.clone(),
        ops,
    })
}

fn lower_single_mutation(
    mutation: &Mutation,
    param_names: &HashSet<String>,
) -> Result<MutationOpIR> {
    match mutation {
        Mutation::Insert(insert) => Ok(MutationOpIR::Insert {
            type_name: insert.type_name.clone(),
            assignments: insert
                .assignments
                .iter()
                .map(|a| IRAssignment {
                    property: a.property.clone(),
                    value: lower_match_value(&a.value, param_names),
                })
                .collect(),
        }),
        Mutation::Update(update) => Ok(MutationOpIR::Update {
            type_name: update.type_name.clone(),
            assignments: update
                .assignments
                .iter()
                .map(|a| IRAssignment {
                    property: a.property.clone(),
                    value: lower_match_value(&a.value, param_names),
                })
                .collect(),
            predicate: IRMutationPredicate {
                property: update.predicate.property.clone(),
                op: update.predicate.op,
                value: lower_match_value(&update.predicate.value, param_names),
            },
        }),
        Mutation::Delete(delete) => Ok(MutationOpIR::Delete {
            type_name: delete.type_name.clone(),
            predicate: IRMutationPredicate {
                property: delete.predicate.property.clone(),
                op: delete.predicate.op,
                value: lower_match_value(&delete.predicate.value, param_names),
            },
        }),
    }
}

fn lower_clauses(
    catalog: &Catalog,
    clauses: &[Clause],
    type_ctx: &TypeContext,
    pipeline: &mut Vec<IROp>,
    bound_vars: &mut HashSet<String>,
    param_names: &HashSet<String>,
    param_types: &HashMap<String, PropType>,
) -> Result<()> {
    // Separate clause types for ordering: bindings first, then traversals, then filters
    let mut bindings = Vec::new();
    let mut traversals = Vec::new();
    let mut filters = Vec::new();
    let mut negations = Vec::new();

    for clause in clauses {
        match clause {
            Clause::Binding(b) => bindings.push(b),
            Clause::Traversal(t) => traversals.push(t),
            Clause::Filter(f) => filters.push(f),
            Clause::Negation(inner) => negations.push(inner),
        }
    }

    // ── Determine which bindings are "deferred" ─────────────────────────
    // The component-root computation is shared with typecheck's T26 pass
    // (`scan_root_variables`) so lowering and the compile-time rule cannot
    // drift; see `deferred_binding_variables` for the algorithm.
    let deferred_set = deferred_binding_variables(clauses);

    // Build deferred filters map for variables introduced by traversals
    let mut deferred_filters: HashMap<String, Vec<IRFilter>> = HashMap::new();

    // Lower bindings into NodeScan ops (skip deferred ones)
    for binding in &bindings {
        let node_type = catalog
            .node_types
            .get(&binding.type_name)
            .expect("binding type was validated during typecheck");

        let binding_filters = build_binding_filters(binding, node_type, param_names);

        if deferred_set.contains(&binding.variable) {
            // Save filters for emission after the Expand that introduces
            // this variable.
            if !binding_filters.is_empty() {
                deferred_filters.insert(binding.variable.clone(), binding_filters);
            }
            continue;
        }

        pipeline.push(IROp::NodeScan {
            variable: binding.variable.clone(),
            type_name: binding.type_name.clone(),
            filters: binding_filters,
        });
        bound_vars.insert(binding.variable.clone());
    }

    // Lower traversals into Expand ops.
    //
    // Traversals are processed iteratively rather than in a single pass
    // because deferred bindings mean a traversal's source might not be
    // bound until a prior traversal introduces it.  Each pass processes
    // every traversal that has at least one bound endpoint; this repeats
    // until all traversals are consumed.
    let mut remaining: Vec<&Traversal> = traversals.to_vec();
    while !remaining.is_empty() {
        let mut next_remaining = Vec::new();
        for traversal in &remaining {
            let src_bound = bound_vars.contains(&traversal.src);
            let dst_bound = bound_vars.contains(&traversal.dst);
            if !src_bound && !dst_bound {
                next_remaining.push(*traversal);
                continue;
            }

            let edge = catalog
                .lookup_edge_by_name(&traversal.edge_name)
                .ok_or_else(|| {
                    crate::error::CompilerError::Plan(format!(
                        "lowering traversal referenced missing edge '{}' after typecheck",
                        traversal.edge_name
                    ))
                })?;

            // Undirected is carried on the AST node itself — negation inners
            // are typechecked into a discarded context clone, so the
            // ResolvedTraversal lookup below cannot see their direction; the
            // syntax is the source of truth for Both.
            let direction = if traversal.undirected {
                Direction::Both
            } else {
                type_ctx
                    .traversals
                    .iter()
                    .find(|rt| {
                        rt.src == traversal.src
                            && rt.dst == traversal.dst
                            && rt.edge_type == edge.name
                    })
                    .map(|rt| rt.direction)
                    .unwrap_or(Direction::Out)
            };

            let dst_type = match direction {
                Direction::Out => edge.to_type.clone(),
                Direction::In => edge.from_type.clone(),
                // Undirected requires from_type == to_type (typecheck rule),
                // so either endpoint type is correct.
                Direction::Both => edge.to_type.clone(),
            };

            if src_bound && dst_bound {
                // Cycle closing: expand to a temp var, then filter temp.id = dst.id
                let temp_var = format!("__temp_{}", traversal.dst);
                pipeline.push(IROp::Expand {
                    src_var: traversal.src.clone(),
                    dst_var: temp_var.clone(),
                    edge_type: edge.name.clone(),
                    direction,
                    dst_type,
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: vec![],
                    edge_binding: traversal
                        .edge_binding
                        .as_deref()
                        .filter(|binding| *binding != "_")
                        .map(str::to_string),
                });
                pipeline.push(IROp::Filter(IRFilter {
                    left: IRExpr::PropAccess {
                        variable: temp_var,
                        property: "id".to_string(),
                    },
                    op: CompOp::Eq,
                    right: IRExpr::PropAccess {
                        variable: traversal.dst.clone(),
                        property: "id".to_string(),
                    },
                }));
            } else if !src_bound && dst_bound {
                // Reverse expand: dst is bound, src is not.
                let reverse_dir = match direction {
                    Direction::Out => Direction::In,
                    Direction::In => Direction::Out,
                    // Symmetric: reversing an undirected expand is a no-op.
                    Direction::Both => Direction::Both,
                };
                let src_type = match direction {
                    Direction::Out => edge.from_type.clone(),
                    Direction::In => edge.to_type.clone(),
                    Direction::Both => edge.from_type.clone(),
                };
                let introduced_filters =
                    deferred_filters.remove(&traversal.src).unwrap_or_default();
                pipeline.push(IROp::Expand {
                    src_var: traversal.dst.clone(),
                    dst_var: traversal.src.clone(),
                    edge_type: edge.name.clone(),
                    direction: reverse_dir,
                    dst_type: src_type,
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: introduced_filters,
                    edge_binding: traversal
                        .edge_binding
                        .as_deref()
                        .filter(|binding| *binding != "_")
                        .map(str::to_string),
                });
                if traversal.src != "_" {
                    bound_vars.insert(traversal.src.clone());
                }
            } else {
                // Normal expand: src is bound, dst is not.
                let introduced_filters =
                    deferred_filters.remove(&traversal.dst).unwrap_or_default();
                pipeline.push(IROp::Expand {
                    src_var: traversal.src.clone(),
                    dst_var: traversal.dst.clone(),
                    edge_type: edge.name.clone(),
                    direction,
                    dst_type,
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: introduced_filters,
                    edge_binding: traversal
                        .edge_binding
                        .as_deref()
                        .filter(|binding| *binding != "_")
                        .map(str::to_string),
                });
                if traversal.dst != "_" {
                    bound_vars.insert(traversal.dst.clone());
                }
            }
        }
        if next_remaining.len() == remaining.len() {
            break;
        }
        remaining = next_remaining;
    }

    // Clause-local variable types for filter-op resolution: negation inners
    // are typechecked into a discarded context clone (same asymmetry the
    // `direction` fallback above documents), so `type_ctx` alone cannot
    // resolve variables introduced inside `not { }`. Bindings declare their
    // type; traversal endpoints take the edge's declared endpoint types
    // (bindings win when both name a variable).
    let mut local_bindings: HashMap<&str, BoundVariable> = HashMap::new();
    for t in &traversals {
        if let Some(edge) = catalog.lookup_edge_by_name(&t.edge_name) {
            local_bindings
                .entry(t.src.as_str())
                .or_insert_with(|| BoundVariable::Node {
                    type_name: edge.from_type.clone(),
                });
            local_bindings
                .entry(t.dst.as_str())
                .or_insert_with(|| BoundVariable::Node {
                    type_name: edge.to_type.clone(),
                });
            // An edge binding (`$p $w:knows $f`) names the edge type, whose
            // String properties are addressable in filters (`$w.note contains …`).
            if let Some(eb) = &t.edge_binding {
                local_bindings
                    .entry(eb.as_str())
                    .or_insert_with(|| BoundVariable::Edge {
                        type_name: edge.name.clone(),
                    });
            }
        }
    }
    for b in &bindings {
        local_bindings.insert(
            b.variable.as_str(),
            BoundVariable::Node {
                type_name: b.type_name.clone(),
            },
        );
    }

    // Lower explicit filters
    for filter in &filters {
        pipeline.push(IROp::Filter(IRFilter {
            left: lower_expr(&filter.left, param_names),
            op: resolve_filter_op(catalog, type_ctx, param_types, &local_bindings, filter),
            right: lower_expr(&filter.right, param_names),
        }));
    }

    // Lower negations into AntiJoin ops
    for neg_clauses in &negations {
        // Find outer-bound variable referenced in the negation
        let outer_var = find_outer_var(neg_clauses, bound_vars);

        let mut inner_pipeline = Vec::new();
        let mut inner_bound = bound_vars.clone();
        lower_clauses(
            catalog,
            neg_clauses,
            type_ctx,
            &mut inner_pipeline,
            &mut inner_bound,
            param_names,
            param_types,
        )?;

        pipeline.push(IROp::AntiJoin {
            outer_var: outer_var.unwrap_or_default(),
            inner: inner_pipeline,
        });
    }

    Ok(())
}

/// Whether `binding.property` is a non-list scalar String. Node and edge type
/// namespaces are independent, so the binding discriminant selects the one
/// catalog namespace that may define the property.
fn is_scalar_string_property(catalog: &Catalog, binding: &BoundVariable, property: &str) -> bool {
    match binding {
        BoundVariable::Node { type_name } => catalog
            .node_types
            .get(type_name)
            .and_then(|nt| nt.properties.get(property)),
        BoundVariable::Edge { type_name } => catalog
            .lookup_edge_by_name(type_name)
            .and_then(|et| et.properties.get(property)),
    }
    .is_some_and(|p| !p.list && matches!(p.scalar, ScalarType::String))
}

/// Resolve the overloaded `contains` keyword to its String-substring form
/// (`StringContains`) when the left operand is a scalar String, so execution
/// dispatches on the IR op alone and never re-derives operand types.
///
/// Variable bindings come from `local_bindings` (this clause list's node and
/// edge bindings + traversal endpoints) first, then the outer `TypeContext`
/// — negation inners never reach the outer context, while outer variables
/// referenced inside a negation only exist there.
fn resolve_filter_op(
    catalog: &Catalog,
    type_ctx: &TypeContext,
    param_types: &HashMap<String, PropType>,
    local_bindings: &HashMap<&str, BoundVariable>,
    filter: &Filter,
) -> CompOp {
    if filter.op != CompOp::Contains {
        return filter.op;
    }
    let left_is_scalar_string = match &filter.left {
        Expr::PropAccess { variable, property } => local_bindings
            .get(variable.as_str())
            .or_else(|| type_ctx.bindings.get(variable))
            .is_some_and(|binding| is_scalar_string_property(catalog, binding, property)),
        Expr::Literal(Literal::String(_)) => true,
        Expr::Variable(v) => param_types
            .get(v)
            .is_some_and(|t| !t.list && matches!(t.scalar, ScalarType::String)),
        _ => false,
    };
    if left_is_scalar_string {
        CompOp::StringContains
    } else {
        CompOp::Contains
    }
}

/// Build IR filters from a binding's inline property matches.
fn build_binding_filters(
    binding: &Binding,
    node_type: &crate::catalog::NodeType,
    param_names: &HashSet<String>,
) -> Vec<IRFilter> {
    let mut filters = Vec::new();
    for pm in &binding.prop_matches {
        let prop = node_type
            .properties
            .get(&pm.prop_name)
            .expect("binding property was validated during typecheck");
        let op = if prop.list {
            CompOp::Contains
        } else {
            CompOp::Eq
        };
        let right = match &pm.value {
            MatchValue::Literal(lit) => IRExpr::Literal(lit.clone()),
            MatchValue::Now => IRExpr::Param(NOW_PARAM_NAME.to_string()),
            MatchValue::Variable(v) => {
                if param_names.contains(v) {
                    IRExpr::Param(v.clone())
                } else {
                    IRExpr::Variable(v.clone())
                }
            }
        };
        filters.push(IRFilter {
            left: IRExpr::PropAccess {
                variable: binding.variable.clone(),
                property: pm.prop_name.clone(),
            },
            op,
            right,
        });
    }
    filters
}

fn find_outer_var(clauses: &[Clause], outer_bound: &HashSet<String>) -> Option<String> {
    for clause in clauses {
        match clause {
            Clause::Traversal(t) => {
                if outer_bound.contains(&t.src) {
                    return Some(t.src.clone());
                }
                if outer_bound.contains(&t.dst) {
                    return Some(t.dst.clone());
                }
            }
            Clause::Filter(f) => {
                if let Some(v) = expr_var(&f.left)
                    && outer_bound.contains(&v)
                {
                    return Some(v);
                }
                if let Some(v) = expr_var(&f.right)
                    && outer_bound.contains(&v)
                {
                    return Some(v);
                }
            }
            Clause::Binding(b) if outer_bound.contains(&b.variable) => {
                return Some(b.variable.clone());
            }
            _ => {}
        }
    }
    None
}

fn expr_var(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Now => None,
        Expr::PropAccess { variable, .. } => Some(variable.clone()),
        Expr::Variable(v) => Some(v.clone()),
        Expr::Nearest { variable, .. } => Some(variable.clone()),
        Expr::Search { field, query } => expr_var(field).or_else(|| expr_var(query)),
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => expr_var(field)
            .or_else(|| expr_var(query))
            .or_else(|| max_edits.as_deref().and_then(expr_var)),
        Expr::MatchText { field, query } => expr_var(field).or_else(|| expr_var(query)),
        Expr::Bm25 { field, query } => expr_var(field).or_else(|| expr_var(query)),
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => expr_var(primary)
            .or_else(|| expr_var(secondary))
            .or_else(|| k.as_deref().and_then(expr_var)),
        Expr::Aggregate { arg, .. } => expr_var(arg),
        _ => None,
    }
}

fn lower_expr(expr: &Expr, param_names: &HashSet<String>) -> IRExpr {
    match expr {
        Expr::Now => IRExpr::Param(NOW_PARAM_NAME.to_string()),
        Expr::PropAccess { variable, property } => IRExpr::PropAccess {
            variable: variable.clone(),
            property: property.clone(),
        },
        Expr::Nearest {
            variable,
            property,
            query,
        } => IRExpr::Nearest {
            variable: variable.clone(),
            property: property.clone(),
            query: Box::new(lower_expr(query, param_names)),
        },
        Expr::Search { field, query } => IRExpr::Search {
            field: Box::new(lower_expr(field, param_names)),
            query: Box::new(lower_expr(query, param_names)),
        },
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => IRExpr::Fuzzy {
            field: Box::new(lower_expr(field, param_names)),
            query: Box::new(lower_expr(query, param_names)),
            max_edits: max_edits
                .as_ref()
                .map(|expr| Box::new(lower_expr(expr, param_names))),
        },
        Expr::MatchText { field, query } => IRExpr::MatchText {
            field: Box::new(lower_expr(field, param_names)),
            query: Box::new(lower_expr(query, param_names)),
        },
        Expr::Bm25 { field, query } => IRExpr::Bm25 {
            field: Box::new(lower_expr(field, param_names)),
            query: Box::new(lower_expr(query, param_names)),
        },
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => IRExpr::Rrf {
            primary: Box::new(lower_expr(primary, param_names)),
            secondary: Box::new(lower_expr(secondary, param_names)),
            k: k.as_ref()
                .map(|expr| Box::new(lower_expr(expr, param_names))),
        },
        Expr::Variable(v) => {
            if param_names.contains(v) {
                IRExpr::Param(v.clone())
            } else {
                IRExpr::Variable(v.clone())
            }
        }
        Expr::Literal(l) => IRExpr::Literal(l.clone()),
        Expr::Aggregate { func, arg } => IRExpr::Aggregate {
            func: *func,
            arg: Box::new(lower_expr(arg, param_names)),
        },
        Expr::AliasRef(name) => IRExpr::AliasRef(name.clone()),
    }
}

fn lower_match_value(value: &MatchValue, param_names: &HashSet<String>) -> IRExpr {
    match value {
        MatchValue::Now => IRExpr::Param(NOW_PARAM_NAME.to_string()),
        MatchValue::Literal(l) => IRExpr::Literal(l.clone()),
        MatchValue::Variable(v) => {
            if param_names.contains(v) {
                IRExpr::Param(v.clone())
            } else {
                IRExpr::Variable(v.clone())
            }
        }
    }
}

#[cfg(test)]
#[path = "lower_tests.rs"]
mod tests;
