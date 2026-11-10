//! One test per operator of section 2's table, each over a small graph and a
//! query the type checker lowers to that operator.

use std::collections::HashMap;

use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::ir::core::CoreDecl;
use omnigraph_compiler::query::check::check_source;
use omnigraph_compiler::query::value::Literal;
use omnigraph_compiler::schema::parser::parse_schema;
use serde_json::{Value as Json, json};

use super::*;

const SCHEMA: &str = r#"
node Person { name: String @key  age: I32?  city: String? }
node Doc { slug: String @key  text: String @index  year: I32  embedding: Vector(2)? @index }
edge Knows: Person -> Person { since: I64? }
edge Wrote: Person -> Doc
"#;

const DATA: &str = r#"{"type":"Person","data":{"name":"ann","age":30,"city":"Oslo"}}
{"type":"Person","data":{"name":"bob","age":40}}
{"type":"Person","data":{"name":"cyd","age":30,"city":"Rome"}}
{"type":"Doc","data":{"slug":"d1","text":"graph needle","year":2020,"embedding":[0.0,1.0]}}
{"type":"Doc","data":{"slug":"d2","text":"needle needle hay","year":2021,"embedding":[1.0,0.0]}}
{"type":"Doc","data":{"slug":"d3","text":"hay","year":2022,"embedding":[1.0,1.0]}}
{"edge":"Knows","id":"k1","from":"ann","to":"bob","data":{"since":2001}}
{"edge":"Knows","id":"k2","from":"bob","to":"cyd","data":{"since":2002}}
{"edge":"Wrote","id":"w1","from":"ann","to":"d1","data":{}}
{"edge":"Wrote","id":"w2","from":"ann","to":"d2","data":{}}
{"edge":"Wrote","id":"w3","from":"cyd","to":"d3","data":{}}"#;

fn catalog() -> Catalog {
    build_catalog(&parse_schema(SCHEMA).unwrap()).unwrap()
}

/// The rows of the one read in `src`, and whether they are ordered.
#[track_caller]
fn run(src: &str, params: &[(&str, Literal)]) -> (Vec<Json>, bool) {
    let catalog = catalog();
    let graph = Graph::from_export(&catalog, DATA).unwrap();
    let decls = check_source(&catalog, src).unwrap();
    let (_, outcome) = decls.into_iter().next().unwrap();
    let CoreDecl::Read(read) = outcome.unwrap().core else {
        panic!("a read")
    };
    let params: HashMap<String, Literal> = params
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Interpreter::new(&catalog, &graph, &params)
        .run(&read)
        .unwrap()
}

#[track_caller]
fn rows(src: &str) -> Vec<Json> {
    run(src, &[]).0
}

/// The rows as a bag: sorted by their cells in key order, whatever order
/// the JSON map keeps its keys in (serde_json's `preserve_order` is on when
/// a crate built alongside enables it).
fn sorted(mut rows: Vec<Json>) -> Vec<Json> {
    let key = |row: &Json| -> Vec<(String, String)> {
        let mut cells: Vec<(String, String)> = row
            .as_object()
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.to_string())).collect())
            .unwrap_or_default();
        cells.sort();
        cells
    };
    rows.sort_by_key(key);
    rows
}

#[test]
fn scan_reads_every_node_of_the_type() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person } return { $p.name } }"
        )),
        vec![
            json!({"p.name": "ann"}),
            json!({"p.name": "bob"}),
            json!({"p.name": "cyd"})
        ]
    );
}

#[test]
fn filter_keeps_rows_whose_predicate_is_true() {
    // A null `city` is not true: bob drops.
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person  $p.city != \"Rome\" } return { $p.name } }"
        )),
        vec![json!({"p.name": "ann"})]
    );
    assert_eq!(
        rows("query q() { match { $p: Person  $p.age > 35 } return { $p.name } }"),
        vec![json!({"p.name": "bob"})]
    );
}

#[test]
fn expand_follows_edges_and_binds_an_edge_variable() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $a: Person  $a knows $b } return { $a.name, $b.name } }"
        )),
        vec![
            json!({"a.name": "ann", "b.name": "bob"}),
            json!({"a.name": "bob", "b.name": "cyd"})
        ]
    );
    assert_eq!(
        rows(
            "query q() { match { $a: Person { name: \"ann\" }  $a $k:knows $b } return { $k.since } }"
        ),
        vec![json!({"k.since": 2001})]
    );
    // Two hops, set semantics over endpoint pairs.
    assert_eq!(
        rows(
            "query q() { match { $a: Person { name: \"ann\" }  $a knows{2,2} $c } return { $c.name } }"
        ),
        vec![json!({"c.name": "cyd"})]
    );
    // Against the edge: who knows bob.
    assert_eq!(
        rows(
            "query q() { match { $b: Person { name: \"bob\" }  $a knows $b } return { $a.name } }"
        ),
        vec![json!({"a.name": "ann"})]
    );
}

#[test]
fn project_computes_return_items() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person  $p.name = \"ann\" } return { $p.name, $p.age + 1 as next } }"
        )),
        vec![json!({"p.name": "ann", "next": 31})]
    );
}

#[test]
fn group_counts_per_key_and_once_over_no_keys() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person } return { $p.age, count($p) as n } }"
        )),
        vec![json!({"n": 1, "p.age": 40}), json!({"n": 2, "p.age": 30})]
    );
    assert_eq!(
        rows("query q() { match { $p: Person  $p.age > 99 } return { count($p) as n } }"),
        vec![json!({"n": 0})]
    );
}

#[test]
fn correlate_evaluates_a_block_per_outer_row() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person  count { $p wrote $d } > 0 } return { $p.name } }"
        )),
        vec![json!({"p.name": "ann"}), json!({"p.name": "cyd"})]
    );
    assert_eq!(
        rows("query q() { match { $p: Person  not { $p wrote $d } } return { $p.name } }"),
        vec![json!({"p.name": "bob"})]
    );
}

#[test]
fn union_concatenates_its_arms() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person { name: \"bob\" }  optional { $p wrote $d } } return { $p.name } }"
        )),
        vec![json!({"p.name": "bob"})]
    );
}

#[test]
fn order_and_cut_take_the_first_rows_by_the_keys_then_the_tie_rule() {
    let (out, ordered) = run(
        "query q() { match { $p: Person } return { $p.name } order { $p.age desc } limit 2 }",
        &[],
    );
    assert!(ordered);
    // bob first; ann and cyd tie on age and break by identity.
    assert_eq!(
        out,
        vec![json!({"p.name": "bob"}), json!({"p.name": "ann"})]
    );
}

#[test]
fn approx_top_k_keeps_the_k_nearest_targets() {
    let out = rows(
        "query q() { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, [0.0, 1.0], k: 2) } return { $d.slug, $distance as dist } order { $distance } limit 2 }",
    );
    assert_eq!(
        out,
        vec![
            json!({"d.slug": "d1", "dist": 0.0}),
            json!({"d.slug": "d3", "dist": 1.0})
        ]
    );
}

#[test]
fn score_keeps_matching_rows_with_their_bm25() {
    let out = rows(
        "query q() { match { from bm25({ match { $d: Doc } return { $d } }, $d.text, \"needle\") } return { $d.slug } order { $score desc } }",
    );
    assert_eq!(out, vec![json!({"d.slug": "d2"}), json!({"d.slug": "d1"})]);
}

#[test]
fn fuse_sums_reciprocal_ranks_over_the_arms() {
    let out = rows(
        "query q() { match { let $all = { match { $d: Doc } return { $d } }  from rrf(nearest($all, $d.embedding, [0.0, 1.0], k: 3), bm25($all, $d.text, \"needle\"), k: 60) } return { $d.slug } order { $score desc } limit 3 }",
    );
    // d1 is first in both arms; d2 is second in bm25 and third by distance.
    assert_eq!(out[0], json!({"d.slug": "d1"}));
    assert_eq!(out.len(), 3);
}

#[test]
fn a_parameter_reaches_its_leaves() {
    let (out, _) = run(
        "query q($n: String) { match { $p: Person { name: $n } } return { $p.age } }",
        &[("n", Literal::String("cyd".into()))],
    );
    assert_eq!(out, vec![json!({"p.age": 30})]);
}

#[test]
fn integer_overflow_is_the_e1_refusal() {
    let catalog = catalog();
    let graph = Graph::from_export(&catalog, DATA).unwrap();
    let decls = check_source(
        &catalog,
        "query q() { match { $p: Person { name: \"ann\" } } return { $p.age * 2147483647 as big } }",
    )
    .unwrap();
    let CoreDecl::Read(read) = decls.into_iter().next().unwrap().1.unwrap().core else {
        panic!()
    };
    let refused = Interpreter::new(&catalog, &graph, &HashMap::new())
        .run(&read)
        .unwrap_err();
    assert!(matches!(refused, Refusal::Runtime("E1", _)), "{refused:?}");
}

#[test]
fn a_bare_node_renders_as_its_object() {
    let out = rows("query q() { match { $p: Person { name: \"bob\" } } return { $p } }");
    assert_eq!(
        out,
        vec![json!({"p": {"@id": "bob", "name": "bob", "age": 40}})]
    );
}

/// The read named `name` in a source of several declarations.
#[track_caller]
fn rows_of(src: &str, name: &str, params: &[(&str, Literal)]) -> Vec<Json> {
    let catalog = catalog();
    let graph = Graph::from_export(&catalog, DATA).unwrap();
    let (_, outcome) = check_source(&catalog, src)
        .unwrap()
        .into_iter()
        .find(|(n, _)| n == name)
        .unwrap();
    let CoreDecl::Read(read) = outcome.unwrap().core else {
        panic!("a read")
    };
    let params: HashMap<String, Literal> = params
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Interpreter::new(&catalog, &graph, &params)
        .run(&read)
        .unwrap()
        .0
}

#[test]
fn fix_iterates_a_recursive_read_to_its_fixpoint() {
    let src = "query reachable($from: String) {
  match {
    from union(
      { match { $p: Person { name: $from }  $p knows $q } return { $q } },
      { match { from reachable($from)  $q knows $r } return { $r as q } })
  }
  return distinct { $q }
}";
    assert_eq!(
        sorted(rows_of(
            src,
            "reachable",
            &[("from", Literal::String("ann".into()))]
        )),
        vec![
            json!({"q": {"@id": "bob", "name": "bob", "age": 40}}),
            json!({"q": {"@id": "cyd", "name": "cyd", "age": 30, "city": "Rome"}})
        ]
    );
}

#[test]
fn call_evaluates_a_declared_read_with_its_arguments() {
    let src =
        "query friends($n: String) { match { $p: Person { name: $n }  $p knows $q } return { $q } }
query q() { match { from friends(\"ann\") } return { $q.name } }";
    assert_eq!(rows_of(src, "q", &[]), vec![json!({"q.name": "bob"})]);
}

/// The rows of the one read in `src` over `data` and the schema `schema`.
#[track_caller]
fn rows_over(schema: &str, data: &str, src: &str) -> (Vec<Json>, bool) {
    let catalog = build_catalog(&parse_schema(schema).unwrap()).unwrap();
    let graph = Graph::from_export(&catalog, data).unwrap();
    let (_, outcome) = check_source(&catalog, src)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let CoreDecl::Read(read) = outcome.unwrap().core else {
        panic!("a read")
    };
    Interpreter::new(&catalog, &graph, &HashMap::new())
        .run(&read)
        .unwrap()
}

const LOOPS: &str = "node P { name: String @key }\nedge Knows: P -> P { w: I64? }";
const LOOP_DATA: &str = r#"{"type":"P","data":{"name":"a"}}
{"type":"P","data":{"name":"b"}}
{"edge":"Knows","id":"ab","from":"a","to":"b","data":{"w":9007199254740993}}
{"edge":"Knows","id":"ba","from":"b","to":"a","data":{"w":-9007199254740992}}
{"edge":"Knows","id":"bb","from":"b","to":"b","data":{}}"#;

#[test]
fn expand_returns_its_source_only_through_a_self_loop() {
    // a reaches itself through b in two hops, which `Expand` never returns;
    // b's own loop is one hop, and is returned.
    let (out, _) = rows_over(
        LOOPS,
        LOOP_DATA,
        "query q() { match { $p: P  $p knows{1,2} $q } return { $p.name, $q.name } }",
    );
    assert_eq!(
        sorted(out),
        vec![
            json!({"p.name": "a", "q.name": "b"}),
            json!({"p.name": "b", "q.name": "a"}),
            json!({"p.name": "b", "q.name": "b"})
        ]
    );
    // Shortest distance: b is one hop from a, so not two.
    let (out, _) = rows_over(
        LOOPS,
        LOOP_DATA,
        "query q() { match { $p: P { name: \"a\" }  $p knows{2,2} $q } return { $q.name } }",
    );
    assert_eq!(out, Vec::<Json>::new());
}

#[test]
fn an_integer_sum_is_exact_and_rounds_once() {
    // In F64 the two weights cancel to 0; exactly they sum to 1.
    let (out, _) = rows_over(
        LOOPS,
        LOOP_DATA,
        "query q() { match { $p: P  $p $e:knows $q } return { sum($e.w) as total } }",
    );
    assert_eq!(out, vec![json!({"total": 1.0})]);
}

#[test]
fn the_tie_rule_reads_an_expand_target_before_its_edge() {
    // Every row ties on the key, and `R + [$q] + [$e]` breaks by `$q`
    // before `$e`: edge `z` reaches a and edge `y` reaches b.
    let data = r#"{"type":"P","data":{"name":"a"}}
{"type":"P","data":{"name":"b"}}
{"edge":"Knows","id":"z","from":"b","to":"a","data":{}}
{"edge":"Knows","id":"y","from":"b","to":"b","data":{}}"#;
    let (out, ordered) = rows_over(
        LOOPS,
        data,
        "query q() { match { $p: P { name: \"b\" }  $p $e:knows $q } return { $q.name, $e.@id as id } order { $e.@type } limit 1 }",
    );
    assert!(ordered);
    assert_eq!(out, vec![json!({"q.name": "a", "id": "z"})]);
}
