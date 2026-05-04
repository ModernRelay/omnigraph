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
embedding: Vector @embed(body)
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
    assert!(typecheck_query(&catalog, &qf.queries[0]).is_ok());
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
    let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
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
        CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_types[0], "Person"),
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
        CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_types[0], "Doc"),
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
        CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_types[0], "Knows"),
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
        CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_types[0], "Knows"),
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
    let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
    match checked {
        CheckedQuery::Mutation(ctx) => {
            assert_eq!(ctx.target_types, vec!["Person", "Knows"]);
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
    let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
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
