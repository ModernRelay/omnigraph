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
node Doc { slug: String @key  text: String @index(fulltext, analyzer = "standard_folded_v1")  title: String? @index(fulltext, analyzer = "standard_v1")  year: I32  embedding: Vector(2)? @index }
node Note { slug: String @key  body: String @index(fulltext) }
edge Knows: Person -> Person { since: I64? }
edge Wrote: Person -> Doc
"#;

const DATA: &str = r#"{"type":"Person","data":{"name":"ann","age":30,"city":"Oslo"}}
{"type":"Person","data":{"name":"bob","age":40}}
{"type":"Person","data":{"name":"cyd","age":30,"city":"Rome"}}
{"type":"Doc","data":{"slug":"d1","text":"graph needle","title":"Graphs and Café","year":2020,"embedding":[0.0,1.0]}}
{"type":"Doc","data":{"slug":"d2","text":"needle needle hay","year":2021,"embedding":[1.0,0.0]}}
{"type":"Doc","data":{"slug":"d3","text":"hay","year":2022,"embedding":[1.0,1.0]}}
{"type":"Note","data":{"slug":"n1","body":"graph"}}
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

/// The refusal of the one read in `src`.
#[track_caller]
fn run_refused(src: &str) -> Refusal {
    let catalog = catalog();
    let graph = Graph::from_export(&catalog, DATA).unwrap();
    let decls = check_source(&catalog, src).unwrap();
    let (_, outcome) = decls.into_iter().next().unwrap();
    let CoreDecl::Read(read) = outcome.unwrap().core else {
        panic!("a read")
    };
    Interpreter::new(&catalog, &graph, &HashMap::new())
        .run(&read)
        .unwrap_err()
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
            "query q() { match { $p: Person } where $p.city != \"Rome\" return { $p.name } }"
        )),
        vec![json!({"p.name": "ann"})]
    );
    assert_eq!(
        rows("query q() { match { $p: Person } where $p.age > 35 return { $p.name } }"),
        vec![json!({"p.name": "bob"})]
    );
}

#[test]
fn expand_follows_edges_and_binds_an_edge_variable() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $a: Person, $a knows $b } return { $a.name, $b.name } }"
        )),
        vec![
            json!({"a.name": "ann", "b.name": "bob"}),
            json!({"a.name": "bob", "b.name": "cyd"})
        ]
    );
    assert_eq!(
        rows(
            "query q() { match { $a: Person, $a $k:knows $b } where $a.name = \"ann\" return { $k.since } }"
        ),
        vec![json!({"k.since": 2001})]
    );
    // Two hops, set semantics over endpoint pairs.
    assert_eq!(
        rows(
            "query q() { match { $a: Person, $a knows{2,2} $c } where $a.name = \"ann\" return { $c.name } }"
        ),
        vec![json!({"c.name": "cyd"})]
    );
    // Against the edge: who knows bob.
    assert_eq!(
        rows(
            "query q() { match { $b: Person, $a knows $b } where $b.name = \"bob\" return { $a.name } }"
        ),
        vec![json!({"a.name": "ann"})]
    );
}

#[test]
fn project_computes_return_items() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person } where $p.name = \"ann\" return { $p.name, $p.age + 1 as next } }"
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
        rows("query q() { match { $p: Person } where $p.age > 99 return { count($p) as n } }"),
        vec![json!({"n": 0})]
    );
}

#[test]
fn correlate_evaluates_a_block_per_outer_row() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person } where count { $d: Doc, $p wrote $d } > 0 return { $p.name } }"
        )),
        vec![json!({"p.name": "ann"}), json!({"p.name": "cyd"})]
    );
    assert_eq!(
        rows(
            "query q() { match { $p: Person } where not exists { $d: Doc, $p wrote $d } return { $p.name } }"
        ),
        vec![json!({"p.name": "bob"})]
    );
}

#[test]
fn union_concatenates_its_arms() {
    // A bag: ann wrote two documents, so she is a row of the first arm
    // twice; cyd is a row of both arms.
    assert_eq!(
        sorted(rows(
            "query q() { match { from union({ match { $p: Person, $d: Doc, $p wrote $d } return { $p } }, { match { $a: Person, $q: Person, $a knows $q } return { $q as p } }) } return { $p.name } }"
        )),
        vec![
            json!({"p.name": "ann"}),
            json!({"p.name": "ann"}),
            json!({"p.name": "bob"}),
            json!({"p.name": "cyd"}),
            json!({"p.name": "cyd"})
        ]
    );
}

#[test]
fn optional_keeps_an_outer_row_with_no_match_once_with_nulls() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person, optional { $d: Doc, $p wrote $d } } return { $p.name, $d.slug } }"
        )),
        vec![
            json!({"d.slug": "d1", "p.name": "ann"}),
            json!({"d.slug": "d2", "p.name": "ann"}),
            json!({"d.slug": "d3", "p.name": "cyd"}),
            json!({"p.name": "bob"})
        ]
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
        "query q() { match { let $docs = { match { $d: Doc } return { $d } }, from nearest($docs, $d.embedding, [0.0, 1.0], k: 2) as $distance } return { $d.slug, $distance as dist } order { $distance } limit 2 }",
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
        "query q() { match { let $docs = { match { $d: Doc } return { $d } }, from bm25($docs, $d.text, \"needle\") as $score } return { $d.slug } order { $score desc } }",
    );
    assert_eq!(out, vec![json!({"d.slug": "d2"}), json!({"d.slug": "d1"})]);
}

#[test]
fn fuse_sums_reciprocal_ranks_over_the_arms() {
    let out = rows(
        "query q() { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, [0.0, 1.0], k: 3), nearest($all, $d.embedding, [0.0, 0.9], k: 3), k: 60) as $score } return { $d.slug } order { $score desc } limit 3 }",
    );
    // d1 is first in both arms, d3 second, d2 third.
    assert_eq!(
        out,
        vec![
            json!({"d.slug": "d1"}),
            json!({"d.slug": "d3"}),
            json!({"d.slug": "d2"})
        ]
    );
}

#[test]
fn a_parameter_reaches_its_leaves() {
    let (out, _) = run(
        "query q($n: String) { match { $p: Person } where $p.name = $n return { $p.age } }",
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
        "query q() { match { $p: Person } where $p.name = \"ann\" return { $p.age * 2147483647 as big } }",
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
    let out = rows("query q() { match { $p: Person } where $p.name = \"bob\" return { $p } }");
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

/// Replace the scan of `var` in `term` with the recursive relation `name`.
fn scan_to_rec_ref(term: &mut Term, var: &str, name: &str) {
    if matches!(term, Term::Scan { var: v, .. } if v == var) {
        *term = Term::RecRef {
            name: name.into(),
            ty: Default::default(),
        };
        return;
    }
    match term {
        Term::Expand { input, .. }
        | Term::Filter { input, .. }
        | Term::Project { input, .. }
        | Term::Group { input, .. }
        | Term::Order { input, .. }
        | Term::Cut { input, .. } => scan_to_rec_ref(input, var, name),
        Term::Correlate { outer, inner } => {
            scan_to_rec_ref(outer, var, name);
            scan_to_rec_ref(inner, var, name);
        }
        _ => {}
    }
}

#[test]
fn fix_iterates_a_recursive_read_to_its_fixpoint() {
    // The checker refuses a recursive read in this release (`T86`), so the
    // `Fix` is assembled from two checked reads: the base arm, and the step
    // with its scan of `$q` replaced by the recursive relation.
    let catalog = catalog();
    let graph = Graph::from_export(&catalog, DATA).unwrap();
    let src = "query base($from: String) { match { $p: Person, $p knows $q } where $p.name = $from return { $q } }
query step() { match { $q: Person, $q knows $r } return { $r as q } }";
    let mut terms = check_source(&catalog, src)
        .unwrap()
        .into_iter()
        .map(|(_, d)| {
            let CoreDecl::Read(read) = d.unwrap().core else {
                panic!("a read")
            };
            read
        });
    let base = terms.next().unwrap();
    let mut step = terms.next().unwrap().term;
    scan_to_rec_ref(&mut step, "q", "reachable");
    let mut read = (*base).clone();
    read.term = Term::Fix {
        name: "reachable".into(),
        body: Box::new(Term::Union {
            left: Box::new(base.term.clone()),
            right: Box::new(step),
        }),
        rounds: None,
        lattice: Vec::new(),
    };
    let params = HashMap::from([("from".to_string(), Literal::String("ann".into()))]);
    let (out, _) = Interpreter::new(&catalog, &graph, &params)
        .run(&read)
        .unwrap();
    assert_eq!(
        sorted(out),
        vec![
            json!({"q": {"@id": "bob", "name": "bob", "age": 40}}),
            json!({"q": {"@id": "cyd", "name": "cyd", "age": 30, "city": "Rome"}})
        ]
    );
}

#[test]
fn call_evaluates_a_declared_read_with_its_arguments() {
    let src =
        "query friends($n: String) { match { $p: Person, $p knows $q } where $p.name = $n return { $q } }
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
fn expand_returns_its_source_only_through_a_self_loop_of_the_single_hop() {
    // a reaches itself through b in two hops, which `Expand` never returns;
    // b's own loop returns b to the single hop, and to no range.
    let (out, _) = rows_over(
        LOOPS,
        LOOP_DATA,
        "query q() { match { $p: P, $p knows{1,2} $q } return { $p.name, $q.name } }",
    );
    assert_eq!(
        sorted(out),
        vec![
            json!({"p.name": "a", "q.name": "b"}),
            json!({"p.name": "b", "q.name": "a"})
        ]
    );
    let (out, _) = rows_over(
        LOOPS,
        LOOP_DATA,
        "query q() { match { $p: P, $p knows $q } where $p.name = \"b\" return { $q.name } }",
    );
    assert_eq!(
        sorted(out),
        vec![json!({"q.name": "a"}), json!({"q.name": "b"})]
    );
    // Shortest distance: b is one hop from a, so not two.
    let (out, _) = rows_over(
        LOOPS,
        LOOP_DATA,
        "query q() { match { $p: P, $p knows{2,2} $q } where $p.name = \"a\" return { $q.name } }",
    );
    assert_eq!(out, Vec::<Json>::new());
}

#[test]
fn a_comparison_treats_the_two_zeros_as_equal_and_the_value_order_does_not() {
    let schema = "node P { name: String @key score: F64? }";
    let data = r#"{"type":"P","data":{"name":"a","score":0.0}}
{"type":"P","data":{"name":"b","score":-0.0}}"#;
    let (out, _) = rows_over(
        schema,
        data,
        "query q() { match { $p: P } where $p.score = 0.0 return { $p.name } }",
    );
    assert_eq!(
        sorted(out),
        vec![json!({"p.name": "a"}), json!({"p.name": "b"})]
    );
    let (out, _) = rows_over(
        schema,
        data,
        "query q() { match { $p: P } return { $p.name } order { $p.score, $p.name } }",
    );
    assert_eq!(out, vec![json!({"p.name": "b"}), json!({"p.name": "a"})]);
}

#[test]
fn an_integer_mean_divides_the_exact_total_once() {
    // In F64 the two weights cancel to 0; exactly they sum to 1, over 2.
    let (out, _) = rows_over(
        LOOPS,
        LOOP_DATA,
        "query q() { match { $p: P, $p $e:knows $q } return { avg($e.w) as mean } }",
    );
    assert_eq!(out, vec![json!({"mean": 0.5})]);
}

#[test]
fn an_integer_sum_is_exact_and_rounds_once() {
    // In F64 the two weights cancel to 0; exactly they sum to 1.
    let (out, _) = rows_over(
        LOOPS,
        LOOP_DATA,
        "query q() { match { $p: P, $p $e:knows $q } return { sum($e.w) as total } }",
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
        "query q() { match { $p: P, $p $e:knows $q } where $p.name = \"b\" return { $q.name, $e.@id as id } order { $e.@type } limit 1 }",
    );
    assert!(ordered);
    assert_eq!(out, vec![json!({"q.name": "a", "id": "z"})]);
}

#[test]
fn union_names_its_columns_by_position_from_the_first_arm() {
    assert_eq!(
        sorted(rows(
            "query q() { match { from union({ match { $p: Person } where $p.name = \"ann\" return { $p.name as who } }, { match { $d: Doc } where $d.slug = \"d1\" return { $d.slug as what } }) } return { $who } }"
        )),
        vec![json!({"who": "ann"}), json!({"who": "d1"})]
    );
}

#[test]
fn a_call_binds_its_argument_from_the_callers_row() {
    let src = "query friends($n: String) { match { $p: Person, $p knows $q } where $p.name = $n return { $q } }
query q() { match { $a: Person, from friends($a.name) } return { $a.name, $q.name } }";
    assert_eq!(
        sorted(rows_of(src, "q", &[])),
        vec![
            json!({"a.name": "ann", "q.name": "bob"}),
            json!({"a.name": "bob", "q.name": "cyd"})
        ]
    );
}

#[test]
fn distance_scores_every_row_with_a_vector() {
    let (out, ordered) = run(
        "query q() { match { let $docs = { match { $d: Doc } return { $d } }, from distance($docs, $d.embedding, [0.0, 1.0]) as $dist } return { $d.slug, $dist } order { $dist } }",
        &[],
    );
    assert!(ordered);
    assert_eq!(
        out,
        vec![
            json!({"d.slug": "d1", "dist": 0.0}),
            json!({"d.slug": "d3", "dist": 1.0}),
            json!({"d.slug": "d2", "dist": 2.0})
        ]
    );
}

#[test]
fn an_edge_binding_scans_every_edge_of_its_type() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $k: Knows } return { $k.since, $k.@src } }"
        )),
        vec![
            json!({"k.@src": "ann", "k.since": 2001}),
            json!({"k.@src": "bob", "k.since": 2002})
        ]
    );
}

#[test]
fn limit_reads_its_count_from_a_parameter() {
    // No `order`: the cut takes the first rows under the tie rule.
    let (out, ordered) = run(
        "query q($n: I64) { match { $p: Person } return { $p.name } limit $n }",
        &[("n", Literal::Integer(2))],
    );
    assert!(ordered);
    assert_eq!(
        out,
        vec![json!({"p.name": "ann"}), json!({"p.name": "bob"})]
    );
}

#[test]
fn if_and_coalesce_evaluate_only_what_they_select() {
    // bob's age is 40: the division is the branch not taken.
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person, let $r = if($p.age = 40, 0, 100 / ($p.age - 40)) } return { $p.name, $r, coalesce($p.city, \"none\") as c } }"
        )),
        vec![
            json!({"c": "Oslo", "p.name": "ann", "r": -10}),
            json!({"c": "Rome", "p.name": "cyd", "r": -10}),
            json!({"c": "none", "p.name": "bob", "r": 0})
        ]
    );
}

#[test]
fn an_inline_read_groups_once_per_outer_row() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person, from { match { $d: Doc, $p wrote $d } return { count($d) as n } } } return { $p.name, $n } }"
        )),
        vec![
            json!({"n": 0, "p.name": "bob"}),
            json!({"n": 1, "p.name": "cyd"}),
            json!({"n": 2, "p.name": "ann"})
        ]
    );
}

#[test]
fn a_block_compares_with_a_scalar_of_the_outer_row() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person } where count { $d: Doc, $p wrote $d } >= $p.age - 29 return { $p.name } }"
        )),
        vec![json!({"p.name": "ann"}), json!({"p.name": "cyd"})]
    );
}

#[test]
fn fuse_yields_one_row_per_target_under_its_name() {
    let out = rows(
        "query q() { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, [0.0, 1.0], k: 3), nearest($all, $d.embedding, [1.0, 0.0], k: 3), k: 60) as $fused } return { $d.slug } }",
    );
    assert_eq!(
        sorted(out),
        vec![
            json!({"d.slug": "d1"}),
            json!({"d.slug": "d2"}),
            json!({"d.slug": "d3"})
        ]
    );
}

#[test]
fn lexical_predicates_match_any_term_and_a_null_document_never() {
    // `search` is any-term: d1 holds `graph`, d2 and d3 `hay`.
    assert_eq!(
        sorted(rows(
            "query q() { match { $d: Doc } where match_terms($d.text, terms(\"graph hay\", mode: any)) return { $d.slug } }"
        )),
        vec![
            json!({"d.slug": "d1"}),
            json!({"d.slug": "d2"}),
            json!({"d.slug": "d3"})
        ]
    );
    // Only d1 has a title; a null document does not match, so `not` keeps
    // d2 and d3.
    assert_eq!(
        sorted(rows(
            "query q() { match { $d: Doc } where not match_terms($d.title, terms(\"graphs\", mode: any)) return { $d.slug } }"
        )),
        vec![json!({"d.slug": "d2"}), json!({"d.slug": "d3"})]
    );
    // One edit reaches `needle`; none does not.
    assert_eq!(
        sorted(rows(
            "query q() { match { $d: Doc } where match_terms($d.text, terms(\"neadle\", mode: any, max_edits: 1)) return { $d.slug } }"
        )),
        vec![json!({"d.slug": "d1"}), json!({"d.slug": "d2"})]
    );
    assert_eq!(
        rows(
            "query q() { match { $d: Doc } where match_terms($d.text, terms(\"neadle\", mode: any)) return { $d.slug } }"
        ),
        Vec::<Json>::new()
    );
}

#[test]
fn a_profile_fixes_folding_and_the_token_length_filter() {
    let catalog = catalog();
    // `standard_v1`: lowercase alone, so `café` stays and `graphs` is not
    // stemmed.
    let standard = Analysis::of(&catalog, Some("Doc"), "title").unwrap();
    assert_eq!(
        standard.tokens("Graphs and Café").unwrap(),
        vec!["graphs", "and", "café"]
    );
    // `standard_folded_v1`: folded, and no profile filters tokens by length.
    let folded = Analysis::of(&catalog, Some("Doc"), "text").unwrap();
    let long = "x".repeat(40);
    assert_eq!(
        folded.tokens(&format!("Café {long} crème")).unwrap(),
        vec!["cafe".to_string(), long.clone(), "creme".to_string()]
    );
    // A letter outside the folding table is refused, not guessed.
    assert!(matches!(
        folded.tokens("Ωmega"),
        Err(Refusal::Unsupported(_))
    ));
    // `english_v1`, the profile of an index that names none, stems and
    // removes stop words, which this crate cannot do: refused.
    assert!(matches!(
        Analysis::of(&catalog, Some("Note"), "body"),
        Err(Refusal::Unsupported(_))
    ));
    assert!(matches!(
        run_refused(
            "query q() { match { $n: Note } where match_terms($n.body, terms(\"graph\", mode: any)) return { $n.slug } }"
        ),
        Refusal::Unsupported(_)
    ));
}

#[test]
fn bm25_v1_scores_by_the_rfc_formula() {
    let analysis = Analysis {
        fold: false,
        max_bytes: None,
    };
    let corpus = vec![
        vec!["a".to_string(), "b".to_string()],
        vec!["a".to_string(), "a".to_string(), "c".to_string()],
        vec!["c".to_string()],
    ];
    let scorer = Bm25::new(vec!["a".into(), "a".into()], false, 0, analysis, corpus);
    // N = 3, df(a) = 2, avg_len = 2; the second document: tf 2, len 3.
    let idf = ((3.0 - 2.0 + 0.5) / (2.0 + 0.5f64)).ln_1p();
    let norm = 1.2 * (0.25 + 0.75 * 3.0 / 2.0);
    let expected = idf * 2.2 * 2.0 / (2.0 + norm);
    let catalog = catalog();
    let graph = Graph::from_export(
        &catalog,
        r#"{"type":"Doc","data":{"slug":"x","text":"a a c","year":1}}"#,
    )
    .unwrap();
    let it = Interpreter::new(&catalog, &graph, &HashMap::new());
    let row = vec![(
        "d".to_string(),
        Value::Ref {
            edge: false,
            type_name: "Doc".into(),
            id: "x".into(),
        },
    )];
    let score = scorer.score(&it, &row, "d", "text").unwrap().unwrap();
    assert_eq!(score.to_bits(), expected.to_bits());
}

#[test]
fn a_scan_of_a_type_the_export_does_not_carry_is_refused() {
    let catalog = catalog();
    let graph = Graph::from_export(&catalog, DATA).unwrap();
    let mut read = match check_source(
        &catalog,
        "query q() { match { $p: Person } return { $p.name } }",
    )
    .unwrap()
    .remove(0)
    .1
    .unwrap()
    .core
    {
        CoreDecl::Read(read) => read,
        CoreDecl::Write(_) => panic!("a read"),
    };
    read.term = Term::Scan {
        var: "o".into(),
        ty: omnigraph_compiler::ir::core::Ty::Node {
            type_name: "@Operator".into(),
            nullable: false,
        },
    };
    let refused = Interpreter::new(&catalog, &graph, &HashMap::new())
        .run(&read)
        .unwrap_err();
    assert!(matches!(refused, Refusal::Unsupported(_)), "{refused:?}");
}
