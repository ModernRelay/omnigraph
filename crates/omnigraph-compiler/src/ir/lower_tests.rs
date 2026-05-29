use super::*;
use crate::catalog::build_catalog;
use crate::query::parser::parse_query;
use crate::query::typecheck::{CheckedQuery, typecheck_query, typecheck_query_decl};
use crate::schema::parser::parse_schema;

fn setup() -> Catalog {
    let schema = parse_schema(
        r#"
node Person { name: String  age: I32? }
node Company { name: String }
edge Knows: Person -> Person { since: Date? }
edge WorksAt: Person -> Company
"#,
    )
    .unwrap();
    build_catalog(&schema).unwrap()
}

#[test]
fn test_lower_basic() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q($name: String) {
match {
    $p: Person { name: $name }
    $p knows $f
}
return { $f.name, $f.age }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    assert_eq!(ir.pipeline.len(), 2); // NodeScan + Expand
    assert_eq!(ir.return_exprs.len(), 2);
}

#[test]
fn test_lower_negation() {
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
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    assert_eq!(ir.pipeline.len(), 2); // NodeScan + AntiJoin
    assert!(matches!(&ir.pipeline[1], IROp::AntiJoin { .. }));
}

#[test]
fn test_lower_mutation_update() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q($name: String, $age: I32) {
update Person set { age: $age } where name = $name
}
"#,
    )
    .unwrap();
    let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
    assert!(matches!(checked, CheckedQuery::Mutation(_)));

    let ir = lower_mutation_query(&qf.queries[0]).unwrap();
    match &ir.ops[0] {
        MutationOpIR::Update {
            type_name,
            assignments,
            predicate,
        } => {
            assert_eq!(type_name, "Person");
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].property, "age");
            assert_eq!(predicate.property, "name");
        }
        _ => panic!("expected update mutation op"),
    }
}

#[test]
fn test_lower_bounded_traversal() {
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
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();
    let expand = ir
        .pipeline
        .iter()
        .find_map(|op| match op {
            IROp::Expand {
                min_hops, max_hops, ..
            } => Some((*min_hops, *max_hops)),
            _ => None,
        })
        .expect("expected expand op");
    assert_eq!(expand.0, 1);
    assert_eq!(expand.1, Some(3));
}

#[test]
fn test_lower_now_uses_reserved_runtime_param() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query stamp() {
match { $p: Person }
return { now() as ts }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    assert!(matches!(
        ir.return_exprs[0].expr,
        IRExpr::Param(ref name) if name == NOW_PARAM_NAME
    ));
}

#[test]
fn test_lower_mutation_now_uses_reserved_runtime_param() {
    let catalog = build_catalog(
        &parse_schema(
            r#"
node Event {
slug: String @key
updated_at: DateTime?
}
"#,
        )
        .unwrap(),
    )
    .unwrap();
    let qf = parse_query(
        r#"
query stamp() {
update Event set { updated_at: now() } where updated_at = now()
}
"#,
    )
    .unwrap();
    let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
    assert!(matches!(checked, CheckedQuery::Mutation(_)));

    let ir = lower_mutation_query(&qf.queries[0]).unwrap();
    match &ir.ops[0] {
        MutationOpIR::Update {
            assignments,
            predicate,
            ..
        } => {
            assert!(matches!(
                assignments[0].value,
                IRExpr::Param(ref name) if name == NOW_PARAM_NAME
            ));
            assert!(matches!(
                predicate.value,
                IRExpr::Param(ref name) if name == NOW_PARAM_NAME
            ));
        }
        _ => panic!("expected update mutation op"),
    }
}

#[test]
fn test_lower_multi_mutation() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q($name: String, $age: I32, $friend: String) {
insert Person { name: $name, age: $age }
insert Knows { from: $name, to: $friend }
}
"#,
    )
    .unwrap();
    let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
    assert!(matches!(checked, CheckedQuery::Mutation(_)));

    let ir = lower_mutation_query(&qf.queries[0]).unwrap();
    assert_eq!(ir.ops.len(), 2);
    assert!(matches!(&ir.ops[0], MutationOpIR::Insert { type_name, .. } if type_name == "Person"));
    assert!(matches!(&ir.ops[1], MutationOpIR::Insert { type_name, .. } if type_name == "Knows"));
}

/// Destination binding is deferred: NodeScan + Expand + Filter (no cross-join).
#[test]
fn test_lower_traversal_with_destination_binding() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p worksAt $c
    $c: Company { name: "Acme" }
}
return { $p.name, $c.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // Should be: NodeScan($p) → Expand($p→$c, dst_filters=[name=="Acme"])
    // NOT:       NodeScan($p) → NodeScan($c) → cross-join → cycle-close
    assert_eq!(ir.pipeline.len(), 2);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "p" && dst_var == "c" && dst_filters.len() == 1
    ));
}

/// Multi-hop chain: all intermediate and final bindings are deferred.
#[test]
fn test_lower_chain_defers_all_intermediate_bindings() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person { name: "Alice" }
    $p knows $f
    $f: Person { name: "Bob" }
    $f worksAt $c
    $c: Company { name: "Acme" }
}
return { $c.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // Should be: NodeScan($p,[name=Alice]) → Expand($p→$f, [name==Bob])
    //            → Expand($f→$c, [name==Acme])
    assert_eq!(ir.pipeline.len(), 3);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "p" && dst_var == "f" && dst_filters.len() == 1
    ));
    assert!(matches!(
        &ir.pipeline[2],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "f" && dst_var == "c" && dst_filters.len() == 1
    ));
}

/// Reverse traversal: source binding is deferred when destination is the root.
#[test]
fn test_lower_reverse_traversal_defers_source_binding() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $c: Company { name: "Acme" }
    $p worksAt $c
    $p: Person { name: "Alice" }
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // $c is root (first declared). $p is deferred (connected via traversal).
    // Traversal $p worksAt $c: $c is bound, $p is not → reverse expand.
    // Pipeline: NodeScan($c,[name=Acme]) → Expand($c→$p, In, [name==Alice])
    assert_eq!(ir.pipeline.len(), 2);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "c"));
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "c" && dst_var == "p" && dst_filters.len() == 1
    ));
}

/// Independent bindings (no traversal) still cross-join.
#[test]
fn test_lower_independent_bindings_still_cross_join() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $c: Company
}
return { $p.name, $c.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // No traversal connecting them → both get NodeScans (cross-join at runtime)
    assert_eq!(ir.pipeline.len(), 2);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
    assert!(matches!(&ir.pipeline[1], IROp::NodeScan { variable, .. } if variable == "c"));
}

/// Destination binding without filters: no NodeScan, no post-expand filter.
#[test]
fn test_lower_destination_binding_without_filters() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p worksAt $c
    $c: Company
}
return { $p.name, $c.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // $c binding is deferred (no filters) → just NodeScan + Expand
    assert_eq!(ir.pipeline.len(), 2);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { src_var, dst_var, .. }
        if src_var == "p" && dst_var == "c"
    ));
}

/// Traversals declared in non-topological order are reordered automatically.
#[test]
fn test_lower_out_of_order_traversals() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $f worksAt $c
    $p knows $f
    $f: Person
    $c: Company { name: "Acme" }
}
return { $c.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // Even though "$f worksAt $c" is declared before "$p knows $f",
    // the iterative lowering processes "$p knows $f" first (because $p
    // is bound) and then "$f worksAt $c" (once $f is bound).
    assert_eq!(ir.pipeline.len(), 3);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
    // First expand: $p → $f (knows)
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { src_var, dst_var, .. }
        if src_var == "p" && dst_var == "f"
    ));
    // Second expand: $f → $c (worksAt), with filter from $c binding
    assert!(matches!(
        &ir.pipeline[2],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "f" && dst_var == "c" && dst_filters.len() == 1
    ));
}

/// Wildcard $_ must not bridge unrelated components in the adjacency graph.
#[test]
fn test_lower_wildcard_does_not_bridge_components() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p knows $_
    $c: Company
}
return { $p.name, $c.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // $p and $c are in separate components (connected only through $_).
    // Both must get their own NodeScan — $c must NOT be deferred.
    // Bindings are emitted first, then traversals.
    assert_eq!(ir.pipeline.len(), 3);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
    assert!(matches!(&ir.pipeline[1], IROp::NodeScan { variable, .. } if variable == "c"));
    // The expand for $p knows $_ (wildcard destination)
    assert!(matches!(
        &ir.pipeline[2],
        IROp::Expand { src_var, dst_var, .. }
        if src_var == "p" && dst_var == "_"
    ));
}

/// Fan-out: one root fans to two deferred destinations via different edges.
#[test]
fn test_lower_fan_out_topology() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person { name: "Alice" }
    $p knows $f
    $f: Person { name: "Bob" }
    $p worksAt $c
    $c: Company { name: "Acme" }
}
return { $f.name, $c.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // Root: $p. Deferred: $f, $c (both reachable from $p).
    assert_eq!(ir.pipeline.len(), 3);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "p" && dst_var == "f" && dst_filters.len() == 1
    ));
    assert!(matches!(
        &ir.pipeline[2],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "p" && dst_var == "c" && dst_filters.len() == 1
    ));
}

/// Fan-in: two sources converge on one destination; second source is
/// introduced via reverse expand from the shared destination.
#[test]
fn test_lower_fan_in_topology() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $a: Person { name: "Alice" }
    $a knows $c
    $b: Person { name: "Bob" }
    $b knows $c
    $c: Person
}
return { $a.name, $b.name, $c.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // Root: $a (first in component {a,b,c}). Deferred: $b, $c.
    // $a knows $c: expand(a→c). $b knows $c: reverse expand(c→b).
    assert_eq!(ir.pipeline.len(), 3);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "a"));
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "a" && dst_var == "c" && dst_filters.is_empty()
    ));
    assert!(matches!(
        &ir.pipeline[2],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "c" && dst_var == "b" && dst_filters.len() == 1
    ));
}

/// Genuine graph cycle: deferred binding is introduced by first traversal,
/// second traversal triggers cycle-closing.
#[test]
fn test_lower_cycle_with_deferred_binding() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $a: Person
    $a knows $b
    $b: Person { name: "Bob" }
    $b knows $a
}
return { $a.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // $b is deferred, introduced by first expand.
    // Second traversal ($b knows $a) is genuine cycle-closing.
    assert_eq!(ir.pipeline.len(), 4);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "a"));
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "a" && dst_var == "b" && dst_filters.len() == 1
    ));
    // Cycle-closing expand to __temp_a
    assert!(matches!(
        &ir.pipeline[2],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "b" && dst_var.starts_with("__temp_") && dst_filters.is_empty()
    ));
    // Cycle-closing filter: __temp_a.id == a.id
    assert!(matches!(&ir.pipeline[3], IROp::Filter(_)));
}

/// Multiple filters on a single deferred binding.
#[test]
fn test_lower_multiple_filters_on_deferred_binding() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p knows $f
    $f: Person { name: "Bob", age: 25 }
}
return { $f.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // Two prop_matches → two dst_filters on the Expand.
    assert_eq!(ir.pipeline.len(), 2);
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { dst_filters, .. }
        if dst_filters.len() == 2
    ));
}

/// Parameter in a deferred binding filter (unit test level).
#[test]
fn test_lower_param_filter_on_deferred_binding() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q($company: String) {
match {
    $p: Person
    $p worksAt $c
    $c: Company { name: $company }
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    assert_eq!(ir.pipeline.len(), 2);
    assert!(matches!(
        &ir.pipeline[1],
        IROp::Expand { dst_filters, .. }
        if dst_filters.len() == 1
    ));
    // The filter's right-hand side should be a Param, not a Literal
    if let IROp::Expand { dst_filters, .. } = &ir.pipeline[1] {
        assert!(matches!(&dst_filters[0].right, IRExpr::Param(name) if name == "company"));
    }
}

/// Negation with inner binding: inner binding is NOT deferred because
/// bound_vars (from outer scope) is not in binding_set for the inner call.
/// This documents current behavior — the inner pipeline uses a NodeScan +
/// cycle-closing, which is correct but less efficient than deferral.
#[test]
fn test_lower_negation_with_inner_binding() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    not {
        $p worksAt $c
        $c: Company { name: "Acme" }
    }
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    // Outer: NodeScan($p) + AntiJoin
    assert_eq!(ir.pipeline.len(), 2);
    assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
    let IROp::AntiJoin { inner, .. } = &ir.pipeline[1] else {
        panic!("expected AntiJoin");
    };
    // Inner pipeline: $c is NOT deferred (it's the only binding in the
    // inner scope), so it gets a NodeScan + cycle-closing (3 ops).
    assert_eq!(inner.len(), 3);
    assert!(matches!(&inner[0], IROp::NodeScan { variable, .. } if variable == "c"));
    assert!(matches!(&inner[1], IROp::Expand { .. }));
    assert!(matches!(&inner[2], IROp::Filter(_)));
}
