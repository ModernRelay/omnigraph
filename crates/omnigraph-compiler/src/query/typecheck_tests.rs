use super::*;
use crate::catalog::build_catalog;
use crate::query::parser::parse_query;
use crate::schema::parser::parse_schema;

/// Node type name of a binding, panicking if it is an edge binding — the two
/// namespaces can share a type name (see `setup_same_named_node_and_edge`).
/// Indexing `ctx.bindings` covers the unbound case with its own panic.
fn node_type_of(binding: &BoundVariable) -> &str {
    match binding {
        BoundVariable::Node { type_name } => type_name,
        BoundVariable::Edge { type_name } => {
            panic!("expected a node binding, found edge type `{type_name}`")
        }
    }
}

/// Edge type name of a binding — the dual of `node_type_of`.
fn edge_type_of(binding: &BoundVariable) -> &str {
    match binding {
        BoundVariable::Edge { type_name } => type_name,
        BoundVariable::Node { type_name } => {
            panic!("expected an edge binding, found node type `{type_name}`")
        }
    }
}

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

fn setup_same_named_node_and_edge() -> Catalog {
    // Node and edge namespaces are independent. These deliberately share a
    // name so the typechecker cannot use `type_name` as a proxy for binding
    // kind when it validates rebinding and traversal endpoints.
    let schema = parse_schema(
        r#"
node Shared {
label: String
}
edge Shared: Shared -> Shared {
label: String?
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

#[test]
fn mutation_target_retains_node_namespace_when_an_edge_shares_its_name() {
    let catalog = setup_same_named_node_and_edge();
    let qf = parse_query(
        r#"
query insert_shared() {
insert Shared { label: "node" }
}
"#,
    )
    .unwrap();

    let checked = typecheck_query_decl(&catalog, qf.single_decl()).unwrap();
    match checked {
        CheckedQuery::Mutation(ctx) => assert_eq!(
            ctx.targets,
            vec![MutationTarget::Node {
                type_name: "Shared".to_string(),
            }]
        ),
        CheckedQuery::Read(_) => panic!("expected mutation typecheck result"),
    }
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

fn setup_blob() -> Catalog {
    let schema = parse_schema(
        r#"
node Document {
name: String
payload: Blob?
}
edge Attaches: Document -> Document {
label: String?
payload: Blob?
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains("T3"));
}

#[test]
fn test_t3_undeclared_variable_in_match_is_rejected() {
    let catalog = setup();
    for (query, variable) in [
        (
            r#"
query q() {
match { $p: Person { age: $missing } }
return { $p.name }
}
"#,
            "missing",
        ),
        (
            r#"
query q() {
match {
    $p: Person
    not {
        $p knows $friend
        $friend: Person { age: $missing }
    }
}
return { $p.name }
}
"#,
            "missing",
        ),
        (
            r#"
query q() {
match {
    $other: Person
    $p: Person { name: $other }
}
return { $p.name }
}
"#,
            "other",
        ),
    ] {
        let qf = parse_query(query).unwrap();
        let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "match variable `${variable}` must be a declared query parameter"
            )),
            "unexpected error for {query}: {err}"
        );
    }
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    assert!(ctx.bindings.contains_key("p"));
}

#[test]
fn test_declared_list_params_typecheck() {
    let catalog = setup_list();
    let qf = parse_query(
        r#"
query q($tags: [String], $days: [Date]?) {
match {
    $p: Person
    $p.tags contains "friend"
}
return { $p.tags, $tags, $days }
}
"#,
    )
    .unwrap();
    assert!(typecheck_query(&catalog, qf.single_decl()).is_ok());
}

#[test]
fn test_contains_filter_accepts_string_substring_overload() {
    // A scalar String left operand resolves the overload to exact substring
    // matching (previously a T7 error, so no existing query changes meaning).
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
    assert!(typecheck_query(&catalog, qf.single_decl()).is_ok());
}

#[test]
fn test_string_contains_requires_string_right_operand() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p.name contains 42
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(
        err.to_string()
            .contains("string contains requires a String right operand")
    );
}

#[test]
fn test_contains_filter_requires_list_or_string_left_operand() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p.age contains 3
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains(
        "contains requires a list property (membership) or a String property (substring)"
    ));
}

#[test]
fn test_starts_with_accepts_string_operands() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q($q: String) {
match {
    $p: Person
    $p.name starts_with $q
}
return { $p.name }
}
"#,
    )
    .unwrap();
    assert!(typecheck_query(&catalog, qf.single_decl()).is_ok());
}

#[test]
fn test_starts_with_rejects_non_string_left_operand() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p.age starts_with "4"
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(
        err.to_string()
            .contains("starts_with requires a String property on the left")
    );
}

#[test]
fn test_starts_with_rejects_non_string_right_operand() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p.name starts_with 4
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(
        err.to_string()
            .contains("starts_with requires a String right operand")
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains("T7"));
}

#[test]
fn test_t7_rejects_non_scalar_comparison() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p != 5
}
return { $p.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains("scalar operands"));
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    assert!(ctx.bindings.contains_key("p"));
}

#[test]
fn test_bm25_string_param_ok() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q($q: String) {
match { $p: Person }
return { $p.name }
order { bm25($p.name, $q) desc }
}
"#,
    )
    .unwrap();
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    assert!(ctx.bindings.contains_key("p"));
}

#[test]
fn test_bm25_rejects_non_string_query() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q($q: I64) {
match { $p: Person }
return { $p.name }
order { bm25($p.name, $q) desc }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    assert!(ctx.bindings.contains_key("d"));
}

#[test]
fn test_rrf_ordering_ok_with_string_nearest_limit() {
    let catalog = setup_vector();
    let qf = parse_query(
        r#"
query q($vq: String, $tq: String) {
match { $d: Doc }
return { $d.id_str }
order { rrf(nearest($d.embedding, $vq), bm25($d.id_str, $tq), 60) desc }
limit 5
}
"#,
    )
    .unwrap();
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains("T8"));
}

#[test]
fn test_undirected_traversal_resolves_both_on_same_type_edge() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person { name: "Alice" }
    $p <knows> $f
}
return { $f.name }
}
"#,
    )
    .unwrap();
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    assert_eq!(ctx.traversals[0].direction, Direction::Both);
    assert_eq!(node_type_of(&ctx.bindings["f"]), "Person");
}

#[test]
fn test_undirected_traversal_rejected_on_asymmetric_edge() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person { name: "Alice" }
    $p <worksAt> $c
}
return { $c.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("T22"), "expected T22, got: {msg}");
    assert!(msg.contains("WorksAt"), "names the edge type: {msg}");
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    assert_eq!(ctx.traversals[0].direction, Direction::Out);
    assert_eq!(node_type_of(&ctx.bindings["f"]), "Person");
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
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
    let checked = typecheck_query_decl(&catalog, qf.single_decl()).unwrap();
    match checked {
        CheckedQuery::Mutation(ctx) => assert_eq!(
            ctx.targets[0],
            MutationTarget::Node {
                type_name: "Person".to_string(),
            }
        ),
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
    let err = typecheck_query_decl(&catalog, qf.single_decl()).unwrap_err();
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
    let checked = typecheck_query_decl(&catalog, qf.single_decl()).unwrap();
    match checked {
        CheckedQuery::Mutation(ctx) => assert_eq!(
            ctx.targets[0],
            MutationTarget::Node {
                type_name: "Doc".to_string(),
            }
        ),
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
    let err = typecheck_query_decl(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query_decl(&catalog, qf.single_decl()).unwrap_err();
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
    let err = typecheck_query_decl(&catalog, qf.single_decl()).unwrap_err();
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
    let checked = typecheck_query_decl(&catalog, qf.single_decl()).unwrap();
    match checked {
        CheckedQuery::Mutation(ctx) => assert_eq!(
            ctx.targets[0],
            MutationTarget::Edge {
                type_name: "Knows".to_string(),
            }
        ),
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
    let err = typecheck_query_decl(&catalog, qf.single_decl()).unwrap_err();
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
    let checked = typecheck_query_decl(&catalog, qf.single_decl()).unwrap();
    match checked {
        CheckedQuery::Mutation(ctx) => assert_eq!(
            ctx.targets[0],
            MutationTarget::Edge {
                type_name: "Knows".to_string(),
            }
        ),
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
    let err = typecheck_query_decl(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains("T16"));
}

#[test]
fn test_mutation_multi_insert_typecheck_ok() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query add_and_link($name: String, $age: I32, $friend: String) {
insert Person { name: $name, age: $age }
insert Knows { from: $name, to: $friend }
}
"#,
    )
    .unwrap();
    let checked = typecheck_query_decl(&catalog, qf.single_decl()).unwrap();
    match checked {
        CheckedQuery::Mutation(ctx) => {
            assert_eq!(
                ctx.targets,
                vec![
                    MutationTarget::Node {
                        type_name: "Person".to_string(),
                    },
                    MutationTarget::Edge {
                        type_name: "Knows".to_string(),
                    },
                ]
            );
        }
        _ => panic!("expected mutation typecheck result"),
    }
}

#[test]
fn test_mutation_multi_second_stmt_error() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query bad($name: String, $age: I32) {
insert Person { name: $name, age: $age }
insert Unknown { foo: $name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query_decl(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains("T10"));
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

    let checked = typecheck_query_decl(&catalog, qf.single_decl()).unwrap();
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

    let err = typecheck_query_decl(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains("DateTime"));
    assert!(err.to_string().contains("property `on`"));
}

#[test]
fn test_edge_binding_prop_access_in_filter_and_return() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p $w:knows $f
    $w.since >= date("2026-01-01")
}
return { $f.name, $w.since }
}
"#,
    )
    .unwrap();
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    assert_eq!(edge_type_of(&ctx.bindings["w"]), "Knows");
    assert_eq!(
        ctx.traversals[0].edge_binding.as_deref(),
        Some("w"),
        "resolved traversal carries the binding for lowering"
    );
}

#[test]
fn test_edge_binding_unknown_property_rejected() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p $w:knows $f
}
return { $w.nonsense }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Knows"), "names the edge type: {msg}");
    assert!(
        msg.contains("nonsense"),
        "names the missing property: {msg}"
    );
}

#[test]
fn test_edge_binding_rejected_on_bounded_traversal() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p $w:knows{1,3} $f
}
return { $f.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("T23"), "dedicated code: {msg}");
    assert!(msg.contains("multi-hop"), "explains the restriction: {msg}");
    assert!(msg.contains("not one edge"), "uses graph vocabulary: {msg}");
}

#[test]
fn test_edge_binding_name_collision_rejected() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p $p:knows $f
}
return { $f.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    assert!(err.to_string().contains("T23"), "{err}");
}

#[test]
fn test_edge_binding_cannot_reuse_a_fresh_traversal_endpoint() {
    let catalog = setup_same_named_node_and_edge();

    for pattern in ["$w $w:shared $b", "$a $w:shared $w"] {
        let source = format!(
            r#"
query q() {{
match {{ {pattern} }}
return {{ $w.label }}
}}
"#
        );
        let qf = parse_query(&source).unwrap();
        let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("T23"), "dedicated edge-binding error: {msg}");
        assert!(
            msg.contains("endpoint") && msg.contains("distinct"),
            "explains the namespace collision: {msg}"
        );
    }
}

#[test]
fn test_edge_binding_cannot_be_rebound_as_same_named_node_type() {
    let catalog = setup_same_named_node_and_edge();
    let qf = parse_query(
        r#"
query q() {
match {
    $a: Shared
    $a $w:shared $b
    $w: Shared
}
return { $w.label }
}
"#,
    )
    .unwrap();

    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("T23"), "dedicated edge-binding error: {msg}");
    assert!(
        msg.contains("edge") && msg.contains("node"),
        "reports the cross-kind rebind: {msg}"
    );
}

#[test]
fn test_edge_binding_cannot_be_a_same_named_traversal_endpoint() {
    let catalog = setup_same_named_node_and_edge();

    for second_traversal in ["$w $x:shared $c", "$c $x:shared $w"] {
        let source = format!(
            r#"
query q() {{
match {{
    $a: Shared
    $a $w:shared $b
    {second_traversal}
}}
return {{ $c.label }}
}}
"#
        );
        let qf = parse_query(&source).unwrap();
        let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("T23"), "dedicated edge-binding error: {msg}");
        assert!(
            msg.contains("edge") && msg.contains("endpoint"),
            "reports the cross-kind endpoint use: {msg}"
        );
    }
}

#[test]
fn test_blob_read_values_are_rejected_for_nodes_and_edges() {
    let catalog = setup_blob();
    for (binding_kind, match_clause, variable, scalar_property) in [
        ("node", "$d: Document", "d", "name"),
        ("edge", "$a: Document\n    $a $e:attaches $b", "e", "label"),
    ] {
        let cases = [
            ("projection", format!("return {{ ${variable}.payload }}")),
            (
                "order",
                format!(
                    "return {{ ${variable}.{scalar_property} }}\norder {{ ${variable}.payload }}"
                ),
            ),
            ("count", format!("return {{ count(${variable}.payload) }}")),
            ("sum", format!("return {{ sum(${variable}.payload) }}")),
            ("avg", format!("return {{ avg(${variable}.payload) }}")),
            ("min", format!("return {{ min(${variable}.payload) }}")),
            ("max", format!("return {{ max(${variable}.payload) }}")),
        ];

        for (operation, tail) in cases {
            let source = format!("query q() {{\nmatch {{\n    {match_clause}\n}}\n{tail}\n}}");
            let qf = parse_query(&source).unwrap_or_else(|error| {
                panic!("{binding_kind} {operation} query must parse: {error}\n{source}")
            });
            let error = typecheck_query(&catalog, qf.single_decl())
                .expect_err(&format!("{binding_kind} {operation}"));
            assert_eq!(
                error.to_string(),
                format!(
                    "type error: T24: Blob property `${variable}.payload` is not available as a .gq read value; Blob values require a dedicated API"
                ),
                "{binding_kind} {operation}"
            );
        }
    }

    // The containment is property-type-specific; ordinary edge projections
    // continue to use the existing bound-edge scan.
    let scalar_edge = parse_query(
        r#"
query q() {
match {
    $a: Document
    $a $e:attaches $b
}
return { $e.label }
}
"#,
    )
    .unwrap();
    assert!(typecheck_query(&catalog, scalar_edge.single_decl()).is_ok());
}

#[test]
fn test_blob_count_cannot_bypass_result_schema_inference() {
    let catalog = setup_blob();
    let qf = parse_query(
        r#"
query q() {
match { $d: Document }
return { count($d.payload) }
}
"#,
    )
    .unwrap();
    let ctx = TypeContext {
        bindings: HashMap::from([(
            "d".to_string(),
            BoundVariable::Node {
                type_name: "Document".to_string(),
            },
        )]),
        aliases: HashMap::new(),
        traversals: Vec::new(),
    };
    let error = infer_query_result_schema(&catalog, qf.single_decl(), &ctx).unwrap_err();
    assert_eq!(
        error.to_string(),
        "type error: T24: Blob property `$d.payload` is not available as a .gq read value; Blob values require a dedicated API"
    );
}

#[test]
fn test_blob_parameters_are_rejected_as_read_values() {
    let catalog = setup_blob();
    let cases = [
        ("projection", "return { $payload }"),
        ("aliased projection", "return { $payload as copy }"),
        ("order", "return { $d.name }\norder { $payload }"),
        ("count", "return { count($payload) }"),
    ];

    for (operation, tail) in cases {
        let source = format!("query q($payload: Blob) {{\nmatch {{ $d: Document }}\n{tail}\n}}");
        let qf = parse_query(&source)
            .unwrap_or_else(|error| panic!("{operation} query must parse: {error}\n{source}"));
        let error = typecheck_query(&catalog, qf.single_decl()).expect_err(operation);
        assert_eq!(
            error.to_string(),
            "type error: T24: Blob parameter `$payload` is not available as a .gq read value; Blob values require a dedicated API",
            "{operation}"
        );
    }
}

#[test]
fn test_blob_match_and_comparison_refusals_remain_pinned() {
    let catalog = setup_blob();

    let matched = parse_query(
        r#"
query q($payload: Blob) {
match { $d: Document { payload: $payload } }
return { $d.name }
}
"#,
    )
    .unwrap();
    let error = typecheck_query(&catalog, matched.single_decl()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "type error: T3: blob property `Document.payload` cannot be used in match patterns"
    );

    let parameter_comparison = parse_query(
        r#"
query q($left: Blob, $right: Blob) {
match {
    $d: Document
    $left = $right
}
return { $d.name }
}
"#,
    )
    .unwrap();
    let error = typecheck_query(&catalog, parameter_comparison.single_decl()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "type error: T7: blob comparisons in filters are not supported"
    );

    // The textual grammar treats a bare `$xs contains $x` as traversal-like,
    // but the AST is a public compiler surface. Pin containment there too so
    // callers cannot route Blob membership around the ordinary comparison
    // guard.
    let direct_ast = QueryDecl {
        name: "blob_membership".to_string(),
        description: None,
        instruction: None,
        params: vec![
            Param {
                name: "xs".to_string(),
                type_name: "[Blob]".to_string(),
                nullable: false,
            },
            Param {
                name: "x".to_string(),
                type_name: "Blob".to_string(),
                nullable: false,
            },
        ],
        match_clause: vec![Clause::Filter(Expr::comparison(
            Expr::Variable("xs".to_string()),
            CompOp::Contains,
            Expr::Variable("x".to_string()),
        ))],
        return_clause: vec![Projection {
            expr: Expr::Literal(Literal::String("unreachable".to_string())),
            alias: None,
        }],
        order_clause: Vec::new(),
        limit: None,
        mutations: Vec::new(),
    };
    let error = typecheck_query(&catalog, &direct_ast).unwrap_err();
    assert_eq!(
        error.to_string(),
        "type error: T7: blob comparisons in filters are not supported"
    );

    for (kind, match_clause, variable) in [
        ("node", "$d: Document", "d"),
        ("edge", "$a: Document\n    $a $e:attaches $b", "e"),
    ] {
        let source = format!(
            "query q($payload: Blob) {{\nmatch {{\n    {match_clause}\n    ${variable}.payload = $payload\n}}\nreturn {{ $payload }}\n}}"
        );
        let qf = parse_query(&source)
            .unwrap_or_else(|error| panic!("{kind} comparison must parse: {error}\n{source}"));
        let error = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "type error: T24: Blob property `${variable}.payload` is not available as a .gq read value; Blob values require a dedicated API"
            ),
            "{kind} comparison"
        );
    }
}

#[test]
fn test_blob_mutation_predicates_are_rejected_for_nodes_and_edges() {
    let catalog = setup_blob();
    for (kind, target) in [("node", "Document"), ("edge", "Attaches")] {
        for param_type in ["Blob", "String"] {
            let source = format!(
                "query delete_target($payload: {param_type}) {{\ndelete {target} where payload = $payload\n}}"
            );
            let qf = parse_query(&source).unwrap();
            let error = typecheck_query_decl(&catalog, qf.single_decl())
                .expect_err("Blob predicates must never use assignment coercions");
            assert_eq!(
                error.to_string(),
                "type error: T11: blob property `payload` cannot be used in WHERE predicates",
                "{kind} {param_type} predicate"
            );
        }
    }
}

#[test]
fn test_blob_mutation_assignment_remains_supported() {
    let catalog = setup_blob();
    for param_type in ["Blob", "String"] {
        let source = format!(
            r#"
query update_payload($payload: {param_type}) {{
update Document set {{ payload: $payload }} where name = "doc"
}}
"#
        );
        let qf = parse_query(&source).unwrap();
        assert!(
            matches!(
                typecheck_query_decl(&catalog, qf.single_decl()),
                Ok(CheckedQuery::Mutation(_))
            ),
            "{param_type} assignment must remain available"
        );
    }
}

#[test]
fn test_edge_binding_aggregate_typechecks() {
    // The uniformity promise ("works wherever a node field does") includes
    // aggregates: count over an edge property, grouped by a node field.
    let catalog = setup();
    let qf = parse_query(
        r#"
query knows_counts() {
match {
    $p: Person
    $p $w:knows $f
}
return { $f.name, count($w.since) }
}
"#,
    )
    .unwrap();
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    assert!(matches!(&ctx.bindings["w"], BoundVariable::Edge { .. }));
}

#[test]
fn test_edge_binding_rejected_in_search_field() {
    // Would otherwise typecheck (title is a String edge prop) and then be
    // SILENTLY DROPPED by the engine's search-filter hoist, which targets a
    // NodeScan the edge binding does not have.
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p $w:worksAt $c
    search($w.title, "engineer")
}
return { $c.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("T23"), "{msg}");
    assert!(msg.contains("search"), "{msg}");
    assert!(msg.contains("node properties"), "{msg}");
}

#[test]
fn test_edge_binding_rejected_in_nearest() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p $w:worksAt $c
}
return { $c.name }
order { nearest($w.title, "x") }
limit 5
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("T23"),
        "clear edge-binding error, not a confusing catalog miss: {msg}"
    );
    assert!(msg.contains("node properties"), "{msg}");
}

#[test]
fn test_edge_binding_bare_use_rejected() {
    let catalog = setup();
    let qf = parse_query(
        r#"
query q() {
match {
    $p: Person
    $p $w:knows $f
}
return { $w }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(&catalog, qf.single_decl()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("T23"), "{msg}");
    assert!(msg.contains("propert"), "points at property access: {msg}");
}

/// Person with a Bool, a nullable String, a list and a property named `and`.
fn setup_expressions() -> Catalog {
    let schema = parse_schema(
        r#"
node Person {
name: String
email: String?
age: I32?
active: Bool
tags: [String]?
and: I32?
payload: Blob?
}
edge Knows: Person -> Person {
since: Date?
}
"#,
    )
    .unwrap();
    build_catalog(&schema).unwrap()
}

/// The error text of `query` under `setup_expressions`, read or mutation.
fn refusal(catalog: &Catalog, query: &str) -> String {
    let qf = parse_query(query).unwrap_or_else(|error| panic!("{query}: {error}"));
    typecheck_query_decl(catalog, qf.single_decl())
        .err()
        .unwrap_or_else(|| panic!("expected a refusal for {query}"))
        .to_string()
}

fn accepted(catalog: &Catalog, query: &str) {
    let qf = parse_query(query).unwrap_or_else(|error| panic!("{query}: {error}"));
    typecheck_query_decl(catalog, qf.single_decl())
        .unwrap_or_else(|error| panic!("{query}: {error}"));
}

fn read(filter: &str) -> String {
    format!(
        "query q($q: String, $n: I32, $flag: Bool) {{ match {{ $p: Person  {filter} }} return {{ $p.name }} }}"
    )
}

#[test]
fn test_boolean_operators_need_bool_operands() {
    let catalog = setup_expressions();
    for filter in [
        "$p.active and not $p.email is null",
        "($p.age > 30 or $p.name = $q) and $flag",
        "not $p.active or $p.tags contains \"x\"",
        "$p.age is null",
        "$p.tags is not null",
        "$p.active = true and $p.age > $n",
        "($p.age > 30) is null",
        "($p.age > 30) = $flag",
        "$p.and > 1",
    ] {
        accepted(&catalog, &read(filter));
    }
    assert_eq!(
        refusal(&catalog, &read("$p.age and $p.name")),
        "type error: T41: `and` needs Bool operands, got I32? and String"
    );
    assert_eq!(
        refusal(&catalog, &read("not $p.age")),
        "type error: T41: `not` needs a Bool operand, got I32?"
    );
    assert_eq!(
        refusal(&catalog, &read("$p")),
        "type error: T41: a filter must be Boolean, got node `Person`"
    );
    assert_eq!(
        refusal(&catalog, &read("$p.payload is null")),
        "type error: T7: blob comparisons in filters are not supported"
    );
    assert_eq!(
        refusal(&catalog, &read("$p.age > age")),
        "type error: T7: filter comparisons require scalar operands, got I32? and aggregate"
    );
    assert_eq!(
        refusal(&catalog, &read("@id = \"x\"")),
        "type error: T7: filter comparisons require scalar operands, got aggregate and String"
    );
    assert_eq!(
        refusal(&catalog, &read("count($p) > 1 and $p.active")),
        "type error: T7: filter comparisons require scalar operands, got aggregate and I64"
    );
}

#[test]
fn test_boolean_expressions_are_nullable_when_an_operand_is() {
    let catalog = setup_expressions();
    let qf = parse_query(
        "query q($n: I32) { match { $p: Person } return { $p.age > 30 as adult, $p.name = \"x\" as named, $p.email is null as unreachable, $p.active and $p.age > $n as both, not $p.active as idle } }",
    )
    .unwrap();
    let ctx = typecheck_query(&catalog, qf.single_decl()).unwrap();
    let schema = infer_query_result_schema(&catalog, qf.single_decl(), &ctx).unwrap();
    let shapes: Vec<(&str, bool)> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.is_nullable()))
        .collect();
    assert_eq!(
        shapes,
        vec![
            ("adult", true),
            ("named", false),
            ("unreachable", false),
            ("both", true),
            ("idle", false),
        ]
    );
    assert!(
        schema
            .fields()
            .iter()
            .all(|field| *field.data_type() == arrow_schema::DataType::Boolean)
    );
}

#[test]
fn test_boolean_projection_needs_an_alias() {
    let catalog = setup_expressions();
    for projection in ["not $p.active", "$p.email is null"] {
        assert_eq!(
            refusal(
                &catalog,
                &format!("query q() {{ match {{ $p: Person }} return {{ {projection} }} }}")
            ),
            "type error: T43: a comparison in return needs an alias; write `… as <name>`",
            "{projection}"
        );
    }
    assert!(
        refusal(
            &catalog,
            "query q($q: String) { match { $p: Person } return { search($p.name, $q) = true as hit } }"
        )
        .contains("T35")
    );
}

#[test]
fn test_search_predicate_stands_only_as_a_top_level_conjunct() {
    let catalog = setup_expressions();
    let expected = "type error: T38: search predicates require a standalone call or `= true`, alone or joined by and";
    for filter in [
        "not search($p.name, $q)",
        "search($p.name, $q) = false",
        "(search($p.name, $q) = true) or $p.active",
        "search($p.name, $q) is null",
        "$p.active and (fuzzy($p.name, $q) or $p.active)",
        "$p.active and not match_text($p.name, $q)",
    ] {
        assert_eq!(refusal(&catalog, &read(filter)), expected, "{filter}");
    }
    for filter in [
        "search($p.name, $q)",
        "search($p.name, $q) = true",
        "search($p.name, $q) and $p.active",
        "$p.active and fuzzy($p.name, $q) = true and match_text($p.name, $q)",
    ] {
        accepted(&catalog, &read(filter));
    }
}

#[test]
fn test_mutation_where_resolves_under_the_target_scope() {
    let catalog = setup_expressions();
    for query in [
        "query q() { delete Person where age > 30 and not name = \"x\" }",
        "query q($q: String) { delete Person where name contains $q or name starts_with \"a\" }",
        "query q() { delete Person where email is null or (active and age is not null) }",
        "query q() { delete Knows where @src = \"a\" and @dst = \"b\" }",
        "query q() { delete Knows where from = \"a\" and to = \"b\" and since is null }",
        "query q() { delete Person where @id = \"a\" or @id = \"b\" }",
        "query q($n: I32) { update Person set { active: false } where age > $n and email is not null }",
        "query q() { delete Person where tags contains \"rust\" }",
        "query q() { delete Person where name = \"x\" and now() > now() }",
    ] {
        accepted(&catalog, query);
    }
    assert_eq!(
        refusal(
            &catalog,
            "query q() { delete Person where active and $p.age > 3 }"
        ),
        "type error: T14: mutation variable `$p` must be a declared query parameter"
    );
    assert_eq!(
        refusal(&catalog, "query q() { delete Person where age > $n }"),
        "type error: T14: mutation variable `$n` must be a declared query parameter"
    );
    assert_eq!(
        refusal(&catalog, "query q() { delete Person where salary > 1 }"),
        "type error: T11: type `Person` has no property `salary`"
    );
    assert_eq!(
        refusal(&catalog, "query q() { delete Person where id = \"x\" }"),
        "type error: T11: type `Person` has no property `id`; the system identity is `@id`"
    );
    assert_eq!(
        refusal(&catalog, "query q() { delete Person where @src = \"x\" }"),
        "type error: T11: type `Person` has no meta-field `@src`; the meta-fields of this type are `@id`"
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { delete Person where payload is null }"
        ),
        "type error: T11: blob property `payload` cannot be used in WHERE predicates"
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q($n: I32) { delete Person where @id = $n }"
        ),
        "type error: T7: cannot assign/compare I32 with String for property `@id`"
    );
    assert_eq!(
        refusal(&catalog, "query q() { delete Person where age = \"old\" }"),
        "type error: T3: property `age` has type I32? but got String"
    );
    assert_eq!(
        refusal(&catalog, "query q() { delete Person where age and active }"),
        "type error: T41: `and` needs Bool operands, got I32? and Bool"
    );
}

#[test]
fn test_boolean_literal_refused_where_a_property_shadows_it() {
    let schema = parse_schema(
        "node Flag { slug: String  false: Bool  on: Bool }\nnode Switch { slug: String  flag: Bool }",
    )
    .unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let shadowed = |word: &str| {
        format!(
            "type error: T46: `{word}` is a Boolean literal here; the property named `false` of `Flag` cannot be named bare in a mutation `where`; rename it in the schema (`@rename_from`)"
        )
    };
    assert_eq!(
        refusal(&catalog, "query q() { delete Flag where false = false }"),
        shadowed("false")
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { update Flag set { on: true } where slug = \"a\" or not on = false }"
        ),
        shadowed("false")
    );
    assert_eq!(
        refusal(&catalog, "query q() { delete Flag where on = true }"),
        shadowed("true")
    );
    for query in [
        "query q() { delete Flag where slug = \"a\" }",
        "query q($on: Bool) { delete Flag where on = $on }",
        "query q() { update Flag set { false: true, on: false } where slug = \"a\" }",
        "query q() { delete Switch where flag = false }",
        "query q() { delete Switch where true }",
    ] {
        accepted(&catalog, query);
    }
}

#[test]
fn test_assignments_and_binding_matches_take_constants() {
    let catalog = setup_expressions();
    for query in [
        "query q($flag: Bool, $n: I32) { insert Person { name: \"x\", active: $flag or $n > 3 } }",
        "query q($n: I32) { insert Person { name: \"x\", active: not $n > 3 and true } }",
        "query q($n: I32) { update Person set { active: $n is null } where name = \"x\" }",
        "query q($flag: Bool, $n: I32) { match { $p: Person { active: $flag and $n > 3 } } return { $p.name } }",
        "query q() { match { $p: Person { active: 1 = 1, and: 2 } } return { $p.and } }",
        "query q() { insert Person { name: \"x\", active: true, and: 1 } }",
        "query q($t: String) { match { $p: Person { tags: $t } } return { $p.name } }",
    ] {
        accepted(&catalog, query);
    }
    let constants = "assignments and binding matches are constants per invocation";
    assert_eq!(
        refusal(
            &catalog,
            "query q() { insert Person { name: @id, active: true } }"
        ),
        format!("type error: T45: `@id` cannot appear in an assignment value; {constants}")
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { update Person set { name: $p.name } where active }"
        ),
        format!("type error: T45: `$p.name` cannot appear in an assignment value; {constants}")
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { match { $p: Person { name: $p.name } } return { $p.name } }"
        ),
        format!("type error: T45: `$p.name` cannot appear in an assignment value; {constants}")
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { match { $p: Person { name: search(1, 2) } } return { $p.name } }"
        ),
        format!("type error: T44: `search` cannot appear in an assignment value; {constants}")
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { insert Person { name: 1 = 1, active: true } }"
        ),
        "type error: T7: cannot assign/compare Bool with String for property `name`"
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { match { $p: Person { name: 1 = 1 } } return { $p.name } }"
        ),
        "type error: T7: cannot assign/compare Bool with String for property `name`"
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { match { $p: Person { tags: 1 = 1 } } return { $p.name } }"
        ),
        "type error: T7: cannot compare Bool membership against [String]? for property `tags`"
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { insert Person { name: $missing, active: true } }"
        ),
        "type error: T14: mutation variable `$missing` must be a declared query parameter"
    );
    assert_eq!(
        refusal(
            &catalog,
            "query q() { match { $p: Person { active: $missing or true } } return { $p.name } }"
        ),
        "type error: T3: match variable `$missing` must be a declared query parameter"
    );
}

#[test]
fn a_type_error_exposes_its_diagnostic_with_code_and_stage() {
    let catalog = setup();
    let file = parse_query("query q() { match { $x: Nowhere } return { $x.name } }").unwrap();
    let decl = &file.into_declarations().unwrap()[0];
    let err = typecheck_query_decl(&catalog, decl).unwrap_err();
    let diagnostic = err
        .diagnostic()
        .expect("a typecheck refusal carries its diagnostic");
    assert_eq!(diagnostic.code.as_str(), "T1");
    assert_eq!(
        diagnostic.stage.as_ref().map(|stage| stage.name),
        Some("typecheck")
    );
    assert!(diagnostic.position.is_none());
    assert_eq!(
        err.to_string(),
        format!("type error: T1: {}", diagnostic.message)
    );
    assert!(
        crate::query::codes::ALL
            .iter()
            .any(|code| code.as_str() == "T1")
    );
}
