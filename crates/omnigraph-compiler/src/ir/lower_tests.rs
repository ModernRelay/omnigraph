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

fn lower(catalog: &Catalog, text: &str) -> QueryIR {
    let qf = parse_query(text).unwrap();
    let tc = typecheck_query(catalog, &qf.queries[0]).unwrap();
    lower_query(catalog, &qf.queries[0], &tc).unwrap()
}

fn expand_dsts(ir: &QueryIR) -> Vec<&str> {
    ir.pipeline
        .iter()
        .filter_map(|op| match op {
            IROp::Expand { dst_var, .. } => Some(dst_var.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn test_lower_anonymous_destinations_get_distinct_names() {
    let ir = lower(
        &setup(),
        "query q() { match { $p: Person  $p knows $_  $p knows $_ } return { $p.name } }",
    );
    let dsts = expand_dsts(&ir);
    assert_eq!(dsts.len(), 2);
    assert!(dsts.iter().all(|d| d.starts_with("__anon_")), "{dsts:?}");
    assert_ne!(dsts[0], dsts[1]);
}

#[test]
fn test_lower_anonymous_sources_get_distinct_names() {
    // Reverse expand: the bound end is the destination, `_` the source.
    let ir = lower(
        &setup(),
        "query q() { match { $p: Person  $_ knows $p  $_ knows $p } return { $p.name } }",
    );
    let dsts = expand_dsts(&ir);
    assert_eq!(dsts.len(), 2);
    assert!(dsts.iter().all(|d| d.starts_with("__anon_")), "{dsts:?}");
    assert_ne!(dsts[0], dsts[1]);
}

#[test]
fn test_lower_rebinding_a_scanned_variable_filters_instead_of_rescanning() {
    let ir = lower(
        &setup(),
        "query q() { match { $p: Person  $p: Person { name: \"x\" } } return { $p.name } }",
    );
    let scans = ir
        .pipeline
        .iter()
        .filter(|op| matches!(op, IROp::NodeScan { .. }))
        .count();
    let filters_on_p_name = ir
        .pipeline
        .iter()
        .filter(|op| {
            matches!(
                op,
                IROp::Filter(IRFilter {
                    left: IRExpr::PropAccess { variable, property },
                    ..
                }) if variable == "p" && property == "name"
            )
        })
        .count();
    assert_eq!((scans, filters_on_p_name), (1, 1), "{:?}", ir.pipeline);
}

#[test]
fn test_lower_repeated_deferred_binding_keeps_both_filter_sets() {
    let ir = lower(
        &setup(),
        "query q() { match { $p: Person  $p knows $f  $f: Person { age: 40 }  $f: Person { name: \"x\" } } return { $f.name } }",
    );
    let dst_filter_props: Vec<Vec<&str>> = ir
        .pipeline
        .iter()
        .filter_map(|op| match op {
            IROp::Expand { dst_filters, .. } => Some(
                dst_filters
                    .iter()
                    .map(|f| match &f.left {
                        IRExpr::PropAccess { variable, property } if variable == "f" => {
                            property.as_str()
                        }
                        other => panic!("unexpected filter operand {other:?}"),
                    })
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        dst_filter_props,
        vec![vec!["age", "name"]],
        "{:?}",
        ir.pipeline
    );
}

#[test]
fn test_lower_cycle_closing_temps_are_distinct() {
    let ir = lower(
        &setup(),
        "query q() { match { $p: Person  $p knows $p  $p knows $p } return { $p.name } }",
    );
    let dsts = expand_dsts(&ir);
    assert_eq!(dsts.len(), 2);
    assert!(dsts.iter().all(|d| d.starts_with("__temp_p_")), "{dsts:?}");
    assert_ne!(dsts[0], dsts[1]);
}

#[test]
fn test_lower_rebinding_an_outer_variable_inside_negation_keeps_its_filter() {
    // `$q` is outer-bound and, inside the negation, not the root of its
    // component; its inline filter must survive as a Filter in the inner
    // pipeline, never be deferred onto an Expand that will not introduce it.
    let ir = lower(
        &setup(),
        "query q() { match { $p: Person  $p knows $q  not { $r: Person  $r knows $q  $q: Person { name: \"zzz\" } } } return { $p.name } }",
    );
    let inner = ir
        .pipeline
        .iter()
        .find_map(|op| match op {
            IROp::AntiJoin { inner, .. } => Some(inner),
            _ => None,
        })
        .expect("an AntiJoin");
    let filters_on_q_name = inner
        .iter()
        .filter(|op| {
            matches!(
                op,
                IROp::Filter(IRFilter {
                    left: IRExpr::PropAccess { variable, property },
                    ..
                }) if variable == "q" && property == "name"
            )
        })
        .count();
    assert_eq!(filters_on_q_name, 1, "{inner:?}");
}

#[test]
fn test_typecheck_refuses_reserved_variable_prefix() {
    // A user variable spelled like a minted name would collide with it in
    // the plan check; the typechecker refuses the spelling with the reason.
    let catalog = setup();
    for text in [
        "query q() { match { $__anon_1: Person } return { $__anon_1.name } }",
        "query q() { match { $p: Person  $p knows $__temp_p_1 } return { $p.name } }",
        "query q() { match { $p: Person  not { $p knows $__x } } return { $p.name } }",
        "query q() { match { $p: Person  $__x knows $p } return { $p.name } }",
        "query q() { match { $p: Person  $p $__w:knows $q } return { $q.name } }",
    ] {
        let qf = parse_query(text).unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0])
            .expect_err("reserved prefix must be refused")
            .to_string();
        assert!(err.contains("reserved for the compiler"), "{err}");
    }
}

#[test]
fn test_lower_resolves_contains_overload_by_left_operand_type() {
    // `contains` on a scalar String left operand lowers to StringContains
    // (substring); on a list left operand it stays Contains (membership).
    let schema = parse_schema("node Person { name: String  tags: [String]? }").unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let qf = parse_query(
        r#"
query q($q: String) {
match {
    $p: Person
    $p.name contains $q
    $p.tags contains $q
    $p.name starts_with $q
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    let filter_ops: Vec<CompOp> = ir
        .pipeline
        .iter()
        .filter_map(|op| match op {
            IROp::Filter(f) => Some(f.op),
            _ => None,
        })
        .collect();
    assert_eq!(
        filter_ops,
        vec![CompOp::StringContains, CompOp::Contains, CompOp::StartsWith]
    );
}

#[test]
fn test_lower_resolves_contains_overload_on_edge_bound_property() {
    // Node and edge type namespaces are independent. These deliberately share
    // a type and property name but give that property different shapes, so
    // lowering must retain the binding kind instead of searching by name.
    let schema = parse_schema(
        "node Shared { text: [String]? }\nedge Shared: Shared -> Shared { text: String  tags: [String]? }",
    )
    .unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let qf = parse_query(
        r#"
query q() {
match {
    $a: Shared
    $a $w:shared $b
    $a.text contains "node member"
    $w.text contains "sub"
    $w.tags contains "member"
    not { $w.text contains "outer sub" }
}
return { $b.text }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

    let filter_ops: Vec<CompOp> = ir
        .pipeline
        .iter()
        .filter_map(|op| match op {
            IROp::Filter(f) => Some(f.op),
            _ => None,
        })
        .collect();
    // Edge String property -> substring; edge list property -> membership.
    assert_eq!(
        filter_ops,
        vec![CompOp::Contains, CompOp::StringContains, CompOp::Contains],
        "same-named node and edge bindings must resolve through separate namespaces"
    );

    let anti_join_inner = ir
        .pipeline
        .iter()
        .find_map(|op| match op {
            IROp::AntiJoin { inner, .. } => Some(inner),
            _ => None,
        })
        .expect("expected anti-join for negation");
    assert!(matches!(
        anti_join_inner.as_slice(),
        [IROp::Filter(IRFilter {
            op: CompOp::StringContains,
            ..
        })]
    ));
}

#[test]
fn test_lower_undirected_traversal_to_direction_both() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q($name: String) {
match {
    $p: Person { name: $name }
    $p <knows> $f
}
return { $f.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();
    match &ir.pipeline[1] {
        IROp::Expand { direction, .. } => assert_eq!(*direction, Direction::Both),
        op => panic!("expected Expand, got {op:?}"),
    }
}

// The discarded-context-clone regression: negation inners are typechecked into
// a clone that never reaches lowering's ResolvedTraversal lookup, so direction
// used to silently fall back to Out inside not{}. Undirectedness now travels
// on the AST node; this pins Both surviving into the AntiJoin's inner Expand.
#[test]
fn test_lower_undirected_inside_negation_keeps_direction_both() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    not { $p <knows> $_ }
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
    let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();
    let IROp::AntiJoin { inner, .. } = &ir.pipeline[1] else {
        panic!("expected AntiJoin, got {:?}", ir.pipeline[1]);
    };
    match &inner[0] {
        IROp::Expand { direction, .. } => assert_eq!(
            *direction,
            Direction::Both,
            "negation inner must not fall back to Out"
        ),
        op => panic!("expected inner Expand, got {op:?}"),
    }
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
        if src_var == "p" && dst_var.starts_with("__anon_")
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
    // Cycle-closing expand to __temp_a_1
    assert!(matches!(
        &ir.pipeline[2],
        IROp::Expand { src_var, dst_var, dst_filters, .. }
        if src_var == "b" && dst_var.starts_with("__temp_") && dst_filters.is_empty()
    ));
    // Cycle-closing filter: __temp_a_1.id == a.id
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
