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
node Reading { slug: String @key  x: F64  y: F32 }
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
{"type":"Reading","data":{"slug":"r1","x":0.0,"y":1.5}}
{"type":"Reading","data":{"slug":"r2","x":-0.0,"y":3.0e38}}
{"type":"Reading","data":{"slug":"r3","x":2.0,"y":0.5}}
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

/// `collect` gathers a group's non-null values in the fixed value order,
/// drops repeats under `distinct`, and is the empty list over empty input,
/// as a group's aggregate and as a block value.
#[test]
fn group_collects_non_null_values_in_the_fixed_order() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person } return { $p.age, collect($p.city) as cities } }"
        )),
        vec![
            json!({"cities": ["Oslo", "Rome"], "p.age": 30}),
            json!({"cities": [], "p.age": 40})
        ]
    );
    assert_eq!(
        rows("query q() { match { $p: Person } return { collect(distinct $p.age) as ages } }"),
        vec![json!({"ages": [30, 40]})]
    );
    assert_eq!(
        rows(
            "query q() { match { $p: Person } where $p.age > 99 return { collect($p.name) as names } }"
        ),
        vec![json!({"names": []})]
    );
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person } return { $p.name, collect($d.year) { $d: Doc, $p wrote $d } as years } }"
        )),
        vec![
            json!({"p.name": "ann", "years": [2020, 2021]}),
            json!({"p.name": "bob", "years": []}),
            json!({"p.name": "cyd", "years": [2022]})
        ]
    );
    // The total order on floats puts -0.0 before 0.0, and keeps both.
    let xs = rows("query q() { match { $r: Reading } return { collect(distinct $r.x) as xs } }");
    let xs = xs[0]["xs"].as_array().unwrap();
    let signs: Vec<bool> = xs
        .iter()
        .map(|x| x.as_f64().unwrap().is_sign_negative())
        .collect();
    assert_eq!(signs, vec![true, false, false]);
    assert_eq!(xs.len(), 3);
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

/// A key's null placement: by default nulls come first ascending and last
/// descending; `nulls first` and `nulls last` place them whatever the
/// direction, and the non-null values keep the key's direction.
#[test]
fn order_places_a_keys_nulls_as_written() {
    let names = |order: &str| -> Vec<Json> {
        let src = format!(
            "query q() {{ match {{ $p: Person }} return {{ $p.name }} order {{ {order} }} }}"
        );
        rows(&src)
            .into_iter()
            .map(|row| row["p.name"].clone())
            .collect()
    };
    assert_eq!(
        names("$p.city"),
        vec![json!("bob"), json!("ann"), json!("cyd")]
    );
    assert_eq!(
        names("$p.city desc"),
        vec![json!("cyd"), json!("ann"), json!("bob")]
    );
    assert_eq!(
        names("$p.city nulls last"),
        vec![json!("ann"), json!("cyd"), json!("bob")]
    );
    assert_eq!(
        names("$p.city nulls first"),
        vec![json!("bob"), json!("ann"), json!("cyd")]
    );
    assert_eq!(
        names("$p.city desc nulls first"),
        vec![json!("bob"), json!("cyd"), json!("ann")]
    );
    assert_eq!(
        names("$p.city desc nulls last"),
        vec![json!("cyd"), json!("ann"), json!("bob")]
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

/// A list literal holding a parameter (section 7's `list_elem`) is the list
/// of its elements' values, in order, whether tested for membership or
/// returned.
#[test]
fn a_list_holding_a_parameter_is_its_elements_values() {
    let (out, _) = run(
        "query q($a: I32, $c: String) { match { $p: Person } where $p.age in [$a, -1] return { $p.name, [$c, \"x\", $c] as l } }",
        &[
            ("a", Literal::Integer(40)),
            ("c", Literal::String("k".into())),
        ],
    );
    assert_eq!(out, vec![json!({"p.name": "bob", "l": ["k", "x", "k"]})]);
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

/// `and` and `or` are guards, left to right: the right side runs only
/// where the left does not decide, so its overflow never fires on a row the
/// left decides, and fires on a row it does not.
#[test]
fn and_and_or_evaluate_their_right_side_only_where_the_left_does_not_decide() {
    let big = "($p.age * 2147483647) > 0";
    assert_eq!(
        rows(&format!(
            "query q() {{ match {{ $p: Person }} where $p.name = \"ann\" return {{ $p.name, $p.name = \"ann\" or {big} as x }} }}"
        )),
        vec![json!({"p.name": "ann", "x": true})]
    );
    assert_eq!(
        rows(&format!(
            "query q() {{ match {{ $p: Person }} where $p.name = \"ann\" return {{ $p.name, $p.name != \"ann\" and {big} as x }} }}"
        )),
        vec![json!({"p.name": "ann", "x": false})]
    );
    for guard in ["$p.name = \"bob\" or", "$p.name = \"ann\" and"] {
        let refused = run_refused(&format!(
            "query q() {{ match {{ $p: Person }} where $p.name = \"ann\" return {{ {guard} {big} as x }} }}"
        ));
        assert!(matches!(refused, Refusal::Runtime("E1", _)), "{refused:?}");
    }
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
        | Term::Cut { input, .. } => scan_to_rec_ref(std::sync::Arc::make_mut(input), var, name),
        Term::Correlate { outer, inner, .. } => {
            scan_to_rec_ref(std::sync::Arc::make_mut(outer), var, name);
            scan_to_rec_ref(std::sync::Arc::make_mut(inner), var, name);
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
        body: std::sync::Arc::new(Term::Union {
            left: std::sync::Arc::new(base.term.clone()),
            right: std::sync::Arc::new(step),
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

/// `Union` matches its arms by column name, in the first arm's column
/// order: columns returned in another order keep their values.
#[test]
fn union_matches_its_arms_by_column_name() {
    assert_eq!(
        sorted(rows(
            "query q() { match { from union({ match { $p: Person } where $p.name = \"ann\" return { $p.name as who, $p.city as at } }, { match { $d: Doc } where $d.slug = \"d1\" return { $d.slug as at, concat($d.slug, \"!\") as who } }) } return { $who, $at } }"
        )),
        vec![
            json!({"at": "Oslo", "who": "ann"}),
            json!({"at": "d1", "who": "d1!"})
        ]
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

/// `stddev` is the sample deviation of a group's non-null values, null
/// below two; `percentile(x, p)` interpolates linearly between the values
/// around p · (n − 1) in the fixed value order, null over none. A fraction
/// parameter outside [0, 1] or null is `E5` before any row is read, over no
/// group too.
#[test]
fn stddev_and_percentile_over_a_group() {
    let one = |src: &str, params: &[(&str, Literal)]| -> Json {
        let rows = outcome(src, params).unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        rows[0].clone()
    };
    let row = one(
        "query q() { match { $p: Person } return { stddev($p.age) as sd, percentile($p.age, 0.5) as median, percentile($p.age, 0.75) as upper } }",
        &[],
    );
    // [30, 30, 40]: the squared differences from 100 / 3 sum to 200 / 3.
    let sd = row["sd"].as_f64().unwrap();
    assert!((sd - (100.0f64 / 3.0).sqrt()).abs() < 1e-12, "{sd}");
    assert_eq!(row["median"], json!(30.0));
    assert_eq!(row["upper"], json!(35.0));
    // One value per city: no deviation, the value itself at any fraction.
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person } return { $p.city, stddev($p.age) as sd, percentile($p.age, 0.3) as at } }"
        )),
        vec![
            json!({"at": 30.0, "p.city": "Oslo"}),
            json!({"at": 30.0, "p.city": "Rome"}),
            json!({"at": 40.0})
        ]
    );
    // The fixed value order puts -0.0 before 0.0.
    let row = one(
        "query q() { match { $r: Reading } return { percentile($r.x, 0) as low, percentile($r.x, 1) as high } }",
        &[],
    );
    assert_eq!(row["low"].as_f64().unwrap().to_bits(), (-0.0f64).to_bits());
    assert_eq!(row["high"], json!(2.0));
    // Over no row: one row with no value, or no row under keys.
    assert_eq!(
        rows(
            "query q() { match { $p: Person } where $p.age > 99 return { stddev($p.age) as sd, percentile($p.age, 0.5) as m } }"
        ),
        vec![json!({})]
    );
    let at = "query q($f: F64?) { match { $p: Person } where $p.age > 99 return { $p.city, percentile($p.age, $f) as m } }";
    assert_eq!(
        outcome(at, &[("f", Literal::Float(0.5))]).unwrap(),
        Vec::<Json>::new()
    );
    for f in [Literal::Float(1.5), Literal::Float(-0.25), Literal::Null] {
        match outcome(at, &[("f", f.clone())]) {
            Err(Refusal::Runtime("E5", _)) => {}
            other => panic!("{f:?}: {other:?}"),
        }
    }
}

/// The outcome of the one read in `src` under `params`.
#[track_caller]
fn outcome(src: &str, params: &[(&str, Literal)]) -> R<Vec<Json>> {
    let catalog = catalog();
    let graph = Graph::from_export(&catalog, DATA).unwrap();
    let (_, checked) = check_source(&catalog, src)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let CoreDecl::Read(read) = checked.unwrap().core else {
        panic!("a read")
    };
    let params: HashMap<String, Literal> = params
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Interpreter::new(&catalog, &graph, &params)
        .run(&read)
        .map(|(rows, _)| rows)
}

/// Binding: an omitted parameter takes its declared default, an omitted
/// nullable one without a default is null, and an omitted required one, a
/// null for a non-nullable one and a value outside the declared type are
/// refused.
#[test]
fn a_parameter_binds_at_its_declaration() {
    let count = |decl: &str, params: &[(&str, Literal)]| {
        outcome(
            &format!(
                "query q({decl}) {{ match {{ $p: Person }} where $p.age > $x return {{ count($p) as n }} }}"
            ),
            params,
        )
    };
    assert_eq!(count("$x: I32 = 30", &[]).unwrap(), vec![json!({"n": 1})]);
    assert_eq!(count("$x: I32? = 30", &[]).unwrap(), vec![json!({"n": 1})]);
    assert_eq!(count("$x: I32?", &[]).unwrap(), vec![json!({"n": 0})]);
    for (decl, params) in [
        ("$x: I32", vec![]),
        ("$x: I32", vec![("x", Literal::Null)]),
        (
            "$x: I32",
            vec![("x", Literal::Integer(i128::from(i32::MAX) + 1))],
        ),
    ] {
        assert!(
            matches!(count(decl, &params), Err(Refusal::Input(_))),
            "{decl} {params:?}"
        );
    }
    let f32_param = outcome(
        "query q($y: F32) { match { $r: Reading } where $r.y > $y return { $r.slug } }",
        &[("y", Literal::Float(1e39))],
    );
    assert!(matches!(f32_param, Err(Refusal::Input(_))), "{f32_param:?}");
}

/// A window or constant read from a parameter is refused outside its
/// range: a `limit` below zero, a `nearest` window below one, an `rrf`
/// constant outside `1..=RRF_MAX_K`; a null lexical query, and a value
/// bound to `now()`'s reserved name, are refused too.
#[test]
fn a_count_parameter_outside_its_range_is_refused() {
    let docs = "let $docs = { match { $d: Doc } return { $d } }";
    for (src, k) in [
        (
            "query q($k: I64) { match { $p: Person } return { $p.name } limit $k }".to_string(),
            -1,
        ),
        (
            format!(
                "query q($k: I64) {{ match {{ {docs}, from nearest($docs, $d.embedding, [0.0, 1.0], k: $k) as $s }} return {{ $d.slug }} }}"
            ),
            0,
        ),
        (
            format!(
                "query q($k: I64) {{ match {{ {docs}, from rrf(nearest($docs, $d.embedding, [0.0, 1.0], k: 2), bm25($docs, $d.text, \"needle\"), k: $k) as $s }} return {{ $d.slug }} }}"
            ),
            0,
        ),
        (
            format!(
                "query q($k: I64) {{ match {{ {docs}, from rrf(nearest($docs, $d.embedding, [0.0, 1.0], k: 2), bm25($docs, $d.text, \"needle\"), k: $k) as $s }} return {{ $d.slug }} }}"
            ),
            i128::from(RRF_MAX_K) + 1,
        ),
    ] {
        let refused = outcome(&src, &[("k", Literal::Integer(k))]);
        assert!(
            matches!(refused, Err(Refusal::Input(_))),
            "{src} k={k}: {refused:?}"
        );
    }
    let now = outcome(
        "query q() { match { $p: Person } return { $p.name } }",
        &[(
            NOW_PARAM_NAME,
            Literal::DateTime("2024-01-01T00:00:00Z".into()),
        )],
    );
    assert!(matches!(now, Err(Refusal::Input(_))), "{now:?}");
    let null_query = outcome(
        "query q($q: String?) { match { $d: Doc } where match_terms($d.text, terms($q, mode: any)) return { $d.slug } }",
        &[("q", Literal::Null)],
    );
    assert!(
        matches!(null_query, Err(Refusal::Input(_))),
        "{null_query:?}"
    );
}

/// A `DateTime` is its instant: spellings of one instant (zero padding,
/// another offset) are one value, rendered as the engine renders it; a
/// parameter with a non-zero digit past the millisecond is refused.
#[test]
fn a_datetime_is_its_instant() {
    for spelling in [
        "2024-01-01T00:00:00.123Z",
        "2024-01-01T00:00:00.123000Z",
        "2024-01-01T05:00:00.123000000+05:00",
    ] {
        assert_eq!(
            canonical_datetime(spelling).as_deref(),
            Some("2024-01-01T00:00:00.123")
        );
    }
    assert_eq!(
        canonical_datetime("1969-12-31T23:59:59Z").as_deref(),
        Some("1969-12-31T23:59:59")
    );
    let ty = PropType::scalar(ScalarType::DateTime, false);
    assert!(matches!(
        bind_param("t", Value::Str("2024-01-01T00:00:00.123456Z".into()), &ty),
        Err(Refusal::Input(_))
    ));
    assert_eq!(
        bind_param("t", Value::Str("2024-01-01T01:00:00+01:00".into()), &ty),
        Ok(Value::Str("2024-01-01T00:00:00".into()))
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

/// A `null` literal typed by its context: the other branch of `if`, the
/// other arguments of `coalesce`, is null on every row it is selected; a
/// null value leaves its row's object.
#[test]
fn a_typed_null_is_null_where_it_is_selected() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $p: Person, let $r = if($p.age > 35, 80 / $p.age, null) } return { $p.name, $r, coalesce(null, $p.city) as c } }"
        )),
        vec![
            json!({"c": "Oslo", "p.name": "ann"}),
            json!({"c": "Rome", "p.name": "cyd"}),
            json!({"p.name": "bob", "r": 2})
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
fn a_profile_fixes_its_whole_pipeline() {
    let catalog = catalog();
    // `standard_v1`: lowercase alone, so `café` stays and `graphs` is not
    // stemmed.
    let standard = Analysis::of(&catalog, Some("Doc"), "title").unwrap();
    assert_eq!(
        standard.tokens("Graphs and Café").unwrap(),
        vec!["graphs", "and", "café"]
    );
    // NFC comes before tokenization: a decomposed `é` is one letter.
    assert_eq!(
        standard.tokens("Cafe\u{301}").unwrap(),
        standard.tokens("Café").unwrap()
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
    // `english_v1`, the profile of an index that names none: the English
    // stemmer, then the stop list of each word and its stem, so `beings`
    // (`be`) is dropped and `only` (`onli`) too; folding after stemming
    // keeps `résumés` (`résumé`) and `resumes` (`resum`) apart.
    let english = Analysis::of(&catalog, Some("Note"), "body").unwrap();
    assert_eq!(
        english
            .tokens("Human beings only read Graphs, résumés and resumes")
            .unwrap(),
        vec!["human", "read", "graph", "resume", "resum"]
    );
    assert_eq!(
        rows(
            "query q() { match { $n: Note } where match_terms($n.body, terms(\"graphs\", mode: any)) return { $n.slug } }"
        ),
        vec![json!({"n.slug": "n1"})]
    );
}

#[test]
fn bm25_v1_scores_by_the_rfc_formula() {
    let analysis = Analysis {
        fold: false,
        max_bytes: None,
        stem: false,
        stop_words: None,
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

/// `-0.0` and `0.0` are one value to a comparison and two to grouping,
/// `distinct` and `count(distinct)`, as the engine evaluates them.
#[test]
fn negative_zero_compares_equal_and_groups_apart() {
    assert_eq!(
        sorted(rows(
            "query q() { match { $r: Reading } where $r.x = 0.0 return { $r.slug } }"
        )),
        vec![json!({"r.slug": "r1"}), json!({"r.slug": "r2"})]
    );
    assert_eq!(
        rows("query q() { match { $r: Reading } return distinct { $r.x } }").len(),
        3
    );
    assert_eq!(
        rows("query q() { match { $r: Reading } return { $r.x, count($r) as n } }").len(),
        3
    );
    assert_eq!(
        rows("query q() { match { $r: Reading } return { count(distinct $r.x) as n } }"),
        vec![json!({"n": 3})]
    );
}

/// A float zero divisor, `-0.0` included, is `E2`, never a non-finite `E3`.
#[test]
fn a_float_zero_divisor_is_e2() {
    for slug in ["r1", "r2"] {
        let refused = run_refused(&format!(
            "query q() {{ match {{ $r: Reading }} where $r.slug = \"{slug}\" return {{ 1.0 / $r.x as v }} }}"
        ));
        assert!(
            matches!(refused, Refusal::Runtime("E2", _)),
            "{slug}: {refused:?}"
        );
    }
}

/// `F32` arithmetic is computed in `F32`: a product finite in `F64`
/// overflows `F32` and is `E3`.
#[test]
fn f32_arithmetic_overflows_in_f32() {
    let refused = run_refused(
        "query q() { match { $r: Reading } where $r.slug = \"r2\" return { $r.y * $r.y as v } }",
    );
    assert!(matches!(refused, Refusal::Runtime("E3", _)), "{refused:?}");
    assert_eq!(
        rows(
            "query q() { match { $r: Reading } where $r.slug = \"r1\" return { $r.y * $r.y as v } }"
        ),
        vec![json!({"v": 2.25})]
    );
}

/// A distance from finite components that overflows `F32` is `E3` under
/// `distance` and `nearest` alike, never a row with an infinite score.
#[test]
fn an_overflowing_distance_is_the_e3_refusal() {
    for call in [
        "distance($docs, $d.embedding, [3.0e38, 3.0e38])",
        "nearest($docs, $d.embedding, [3.0e38, 3.0e38], k: 1)",
    ] {
        let refused = run_refused(&format!(
            "query q() {{ match {{ let $docs = {{ match {{ $d: Doc }} return {{ $d }} }}, from {call} as $dist }} return {{ $d.slug }} }}"
        ));
        assert!(
            matches!(refused, Refusal::Runtime("E3", _)),
            "{call}: {refused:?}"
        );
    }
}

/// A cast of a float to an integer is a form this interpreter does not
/// evaluate, never a float passed through as the integer.
#[test]
fn a_float_cast_to_an_integer_is_refused() {
    let ty = ExprType::from_prop(&omnigraph_compiler::types::PropType::scalar(
        ScalarType::I64,
        false,
    ));
    assert!(matches!(
        cast(Value::Float(1.5), &ty),
        Err(Refusal::Unsupported(_))
    ));
    assert!(matches!(cast(Value::Int(2), &ty), Ok(Value::Int(2))));
}
