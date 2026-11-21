//! Every refusal the type checker justifies by a claim about every graph
//! that conforms to the schema holds that claim.
//!
//! The refusals (`omnigraph_compiler::query::check::soundness::Claim`, which
//! lists them with the sentence each rests on):
//!
//! - `T7`, a comparison with `null`: null on every row;
//! - `T3`, a literal outside its property's domain, under `=` and `in`: the
//!   comparison returns no rows (section 5);
//! - `T82`, a reversed triple, or a selection none of whose members points
//!   along the written order: the written order is the direction, so no
//!   edge matches;
//! - `T5`, a hop range starting past one hop over an edge between two node
//!   types: no path reaches the hop, so the pattern matches nothing;
//! - `T7`, two references of different types: never equal.
//!
//! With one refusal let through (the checker's test-only switch), the
//! reference interpreter evaluates the term on generated conforming graphs
//! and must confirm the claim: no rows, or null in the column on every row.
//! The converse for `T82`: a selection it admits, one member along the
//! written order and one against it, returns rows on some generated graph.
//!
//! `T20`, a literal lexical query with no searchable character, is a type
//! error by its rule (section 4, "Empty text") and is not one of these: the
//! same text bound at run time, or a text of only stop words, matches no
//! document under either mode, a property of the lexical relation that its
//! own cases pin, not a refusal's claim.
//!
//! A counterexample is a wrongful refusal. `T3`'s reason is misspelling, not
//! emptiness: a literal no row can hold is refused wherever it is compared,
//! and under `!=`, `<` or `>` it would keep rows (section 5's literal-checking
//! bullet), so the law checks the emptiness claim for `=` and `in` only. The
//! refusal under every other operator is pinned by the
//! `t3_literal_checked_against_its_property.gqt` case.

use std::collections::{BTreeMap, HashMap};

use omnigraph_compiler::Literal;
use omnigraph_compiler::arbitrary::{
    Choices, GenGraph, GenSchema, PropTy, Sc, SchemaOptions, choices,
};
use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::ir::core::CoreDecl;
use omnigraph_compiler::query::check::check_source;
use omnigraph_compiler::query::check::soundness::{Claim, with_refusal_let_through};
use omnigraph_compiler::schema::parser::parse_schema;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

use crate::law_engine_reference::law_runner;

const CASES: u32 = 1_000;
const NIGHTLY: u32 = 50_000;

/// What the refusal's claim says a probe returns.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Expect {
    /// No row on any conforming graph.
    NoRows,
    /// Column `c` null on every row.
    NullColumn,
}

#[derive(Debug, Clone)]
struct Probe {
    claim: Claim,
    shape: &'static str,
    source: String,
    expect: Expect,
}

const OPS: [&str; 6] = ["=", "!=", "<", "<=", ">", ">="];

fn ty(schema: &GenSchema, t: usize) -> &str {
    &schema.nodes[t].name
}

/// The probes a schema admits, each a read the refusal on `claim` refuses.
fn probes(schema: &GenSchema, c: &mut Choices) -> Vec<Probe> {
    let mut out = Vec::new();
    let t = c.below(schema.nodes.len());
    let node = &schema.nodes[t];
    let tn = ty(schema, t);
    // T7: a comparison with `null`, in `return` and in `where`, either side.
    let scalars: Vec<_> = node
        .props
        .iter()
        .filter(|p| p.scalar().is_some() || matches!(p.ty, PropTy::Enum))
        .collect();
    if !scalars.is_empty() {
        let p = &scalars[c.below(scalars.len())];
        let op = *c.pick(&OPS);
        let ordered = matches!(op, "<" | "<=" | ">" | ">=");
        if !(ordered && matches!(p.ty, PropTy::Scalar(Sc::Bool) | PropTy::Enum)) {
            let cmp = if c.flag(50) {
                format!("$x.{} {op} null", p.name)
            } else {
                format!("null {op} $x.{}", p.name)
            };
            out.push(Probe {
                claim: Claim::NullComparison,
                shape: "null in return",
                source: format!(
                    "query q() {{ match {{ $x: {tn} }} return {{ $x.k as k, ({cmp}) as c }} }}"
                ),
                expect: Expect::NullColumn,
            });
            out.push(Probe {
                claim: Claim::NullComparison,
                shape: "null in where",
                source: format!(
                    "query q() {{ match {{ $x: {tn} }} where {cmp} return {{ $x.k }} }}"
                ),
                expect: Expect::NoRows,
            });
        }
    }
    // T3: a literal outside its property's domain, under `=` and `in`.
    for p in &node.props {
        let outside = match p.ty {
            PropTy::Enum => Some("\"purple\"".to_string()),
            PropTy::Scalar(Sc::I32) => {
                Some(["2147483648", "(-2147483649)", "9999999999"][c.below(3)].to_string())
            }
            PropTy::Scalar(Sc::U32) => Some(["(-1)", "4294967296"][c.below(2)].to_string()),
            _ => None,
        };
        let Some(lit) = outside else { continue };
        let (shape, cmp) = if c.flag(50) {
            (
                "outside the domain under =",
                format!("$x.{} = {lit}", p.name),
            )
        } else {
            (
                "outside the domain under in",
                format!("$x.{} in [{lit}]", p.name),
            )
        };
        out.push(Probe {
            claim: Claim::LiteralOutsideDomain,
            shape,
            source: format!("query q() {{ match {{ $x: {tn} }} where {cmp} return {{ $x.k }} }}"),
            expect: Expect::NoRows,
        });
    }
    // T82 and T5 over an edge between two types.
    for e in &schema.edges {
        if e.from == e.to {
            continue;
        }
        let (from, to) = (ty(schema, e.from), ty(schema, e.to));
        let name = e.surface();
        out.push(Probe {
            claim: Claim::ReversedTriple,
            shape: "reversed triple",
            source: format!(
                "query q() {{ match {{ $b: {to}, $a: {from}, $b {name} $a }} return {{ $a.k as a, $b.k as b }} }}"
            ),
            expect: Expect::NoRows,
        });
        let (lo, hi) = *c.pick(&[(2, 2), (2, 3), (3, 3)]);
        out.push(Probe {
            claim: Claim::HopPastDistinctEndpoints,
            shape: "hop range past one hop",
            source: format!(
                "query q() {{ match {{ $a: {from}, $a {name}{{{lo},{hi}}} $b }} return {{ $a.k as a, $b.k as b }} }}"
            ),
            expect: Expect::NoRows,
        });
        // Two types, two references: never equal.
        out.push(Probe {
            claim: Claim::ReferencesOfTwoTypes,
            shape: "references of two types under =",
            source: format!(
                "query q() {{ match {{ $a: {from}, $a {name} $b }} where $a = $b return {{ $a.k as a }} }}"
            ),
            expect: Expect::NoRows,
        });
    }
    // T82 over a selection none of whose members points along.
    for (i, e) in schema.edges.iter().enumerate() {
        for f in &schema.edges[i + 1..] {
            if e.from != e.to && e.from == f.from && e.to == f.to {
                let (from, to) = (ty(schema, e.from), ty(schema, e.to));
                out.push(Probe {
                    claim: Claim::ReversedTriple,
                    shape: "reversed selection",
                    source: format!(
                        "query q() {{ match {{ $b: {to}, $a: {from}, $b ({} | {}) $a }} return {{ $a.k as a, $b.k as b }} }}",
                        e.surface(),
                        f.surface()
                    ),
                    expect: Expect::NoRows,
                });
            }
        }
    }
    out
}

/// The probe's rows on `graph` with its refusal let through: `None` when
/// the checker still refuses it for another reason.
fn evaluate(
    catalog: &Catalog,
    graph: &omnigraph_reference::Graph,
    probe: &Probe,
) -> Result<Option<Vec<Value>>, String> {
    let refused = check_source(catalog, &probe.source)
        .map(|decls| decls.into_iter().find_map(|(_, r)| r.err()))
        .unwrap_or_else(Some);
    let Some(refusal) = refused else {
        return Err(format!("accepted without the switch: {}", probe.source));
    };
    let checked = with_refusal_let_through(probe.claim, || check_source(catalog, &probe.source));
    let read = match checked.map(|decls| decls.into_iter().find(|(n, _)| n == "q").map(|(_, r)| r))
    {
        Ok(Some(Ok(c))) => match c.core {
            CoreDecl::Read(read) => read,
            CoreDecl::Write(_) => return Err("a write".into()),
        },
        // Another refusal applies; this probe is not this claim's.
        _ => {
            let _ = refusal;
            return Ok(None);
        }
    };
    let params: HashMap<String, Literal> = HashMap::new();
    omnigraph_reference::Interpreter::new(catalog, graph, &params)
        .run(&read)
        .map(|(rows, _)| Some(rows))
        .map_err(|e| format!("the reference refused: {e}"))
}

#[derive(Default)]
struct Tally {
    /// (claim shape) → probes the reference evaluated.
    evaluated: BTreeMap<&'static str, usize>,
    /// … of which returned at least one row.
    with_rows: BTreeMap<&'static str, usize>,
}

static TALLY: std::sync::Mutex<Option<Tally>> = std::sync::Mutex::new(None);

fn check(s: &[u32], g: &[u32], p: &[u32]) -> Result<(), String> {
    let schema = GenSchema::generate(&mut Choices::new(s), SchemaOptions::default());
    let graph = GenGraph::generate(&mut Choices::new(g), &schema, 5, 8);
    let catalog = build_catalog(&parse_schema(&schema.text()).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let shared = graph.jsonl(&schema);
    let disjoint = disjoint_keys(&shared, &schema);
    let reference = omnigraph_reference::Graph::from_export(&catalog, &shared)
        .map_err(|e| format!("graph: {e}"))?;
    let typed = omnigraph_reference::Graph::from_export(&catalog, &disjoint)
        .map_err(|e| format!("graph: {e}"))?;
    for probe in probes(&schema, &mut Choices::new(p)) {
        // A claim that crosses two node types is evaluated on the graph
        // whose keys no two types share (see `disjoint_keys`).
        let crosses_types = matches!(
            probe.claim,
            Claim::ReversedTriple | Claim::HopPastDistinctEndpoints | Claim::ReferencesOfTwoTypes
        );
        let (graph, jsonl) = if crosses_types {
            (&typed, &disjoint)
        } else {
            (&reference, &shared)
        };
        let Some(rows) = evaluate(&catalog, graph, &probe)? else {
            continue;
        };
        {
            let mut tally = TALLY.lock().unwrap();
            let tally = tally.get_or_insert_with(Tally::default);
            *tally.evaluated.entry(probe.shape).or_default() += 1;
            if !rows.is_empty() {
                *tally.with_rows.entry(probe.shape).or_default() += 1;
            }
        }
        let holds = match probe.expect {
            Expect::NoRows => rows.is_empty(),
            // A row renders a null column by omitting it.
            Expect::NullColumn => rows.iter().all(|r| r.get("c").is_none_or(Value::is_null)),
        };
        if !holds {
            return Err(format!(
                "{:?} ({}): the refused read returns {}\n--- schema\n{}\n--- seed\n{}\n--- query\n{}",
                probe.claim,
                probe.shape,
                Value::Array(rows),
                schema.text(),
                jsonl,
                probe.source
            ));
        }
    }
    Ok(())
}

/// The graph with every key prefixed by its node type, so no two types
/// share one. The term a let-through refusal leaves in a claim crossing two
/// types is ill-typed (an `Expand` from a node over an edge whose source is
/// another type, an equality between references of two types), and the
/// reference interpreter, which only ever evaluates well-typed terms, finds
/// an edge's endpoint and compares a reference by key alone; on a graph
/// whose types share a key (which the generator draws on purpose), it would
/// read an `A` keyed `a` as the `B` keyed `a`. Conformance, which each of
/// those claims rests on, identifies a node by its type and its key.
fn disjoint_keys(jsonl: &str, schema: &GenSchema) -> String {
    let mut out = String::new();
    for line in jsonl.lines() {
        let Ok(mut row) = serde_json::from_str::<Value>(line) else {
            out.push_str(line);
            out.push('\n');
            continue;
        };
        if let Some(t) = row.get("type").and_then(Value::as_str).map(str::to_string)
            && let Some(k) = row.pointer_mut("/data/k")
            && let Some(key) = k.as_str()
        {
            *k = Value::String(format!("{t}:{key}"));
        }
        if let Some(e) = row.get("edge").and_then(Value::as_str).map(str::to_string)
            && let Some(edge) = schema.edges.iter().find(|x| x.name == e)
        {
            for (end, t) in [("from", edge.from), ("to", edge.to)] {
                if let Some(v) = row.get_mut(end)
                    && let Some(key) = v.as_str()
                {
                    *v = Value::String(format!("{}:{key}", schema.nodes[t].name));
                }
            }
        }
        out.push_str(&row.to_string());
        out.push('\n');
    }
    out
}

fn run_law(cases: u32) {
    let mut runner = law_runner(cases, "tests/regressions/law_refusal_soundness.txt");
    let result = runner.run(&(choices(64), choices(1024), choices(32)), |(s, g, p)| {
        check(&s, &g, &p).map_err(TestCaseError::fail)
    });
    if let Err(e) = result {
        panic!("{e}");
    }
    let tally = TALLY.lock().unwrap().take().unwrap_or_default();
    eprintln!("probes evaluated: {:?}", tally.evaluated);
    // Every claim is exercised, over graphs that hold rows of the pattern
    // the refused predicate filters, so no claim passes on empty graphs
    // alone.
    for shape in [
        "null in return",
        "null in where",
        "outside the domain under =",
        "reversed triple",
        "hop range past one hop",
        "references of two types under =",
    ] {
        assert!(
            tally.evaluated.get(shape).copied().unwrap_or(0) > 0,
            "no `{shape}` probe was evaluated: {:?}",
            tally.evaluated
        );
    }
    assert!(
        tally.with_rows.get("null in return").copied().unwrap_or(0) > 0,
        "no `null in return` probe saw a row"
    );
}

#[test]
fn every_refusal_resting_on_a_claim_about_every_graph_holds_it() {
    run_law(CASES);
}

#[test]
#[ignore = "nightly: 50,000 generated schemas and graphs; the gate runs 1,000"]
fn every_refusal_resting_on_a_claim_about_every_graph_holds_it_at_nightly_breadth() {
    run_law(NIGHTLY);
}

/// The converse for `T82`: a selection with one member along the written
/// order and one against it is admitted, and returns rows on some
/// generated graph, so admitting it is not admitting an empty read.
#[test]
fn an_admitted_selection_returns_rows_on_some_generated_graph() {
    let schema_text = "node A {\n    k: String @key\n}\nnode B {\n    k: String @key\n}\nedge Fwd: A -> B\nedge Back: B -> A\n";
    let catalog = build_catalog(&parse_schema(schema_text).unwrap()).unwrap();
    let source =
        "query q() { match { $a: A, $b: B, $a (fwd | back) $b } return { $a.k as a, $b.k as b } }";
    let checked = check_source(&catalog, source).unwrap();
    let Some((_, Ok(checked))) = checked.into_iter().find(|(n, _)| n == "q") else {
        panic!("the selection with one member along the written order is refused");
    };
    let CoreDecl::Read(read) = checked.core else {
        panic!("a write")
    };
    // The generator's graphs over the same schema shape.
    let schema = GenSchema {
        nodes: vec![
            omnigraph_compiler::arbitrary::NodeTy {
                name: "A".into(),
                props: vec![],
            },
            omnigraph_compiler::arbitrary::NodeTy {
                name: "B".into(),
                props: vec![],
            },
        ],
        edges: vec![
            omnigraph_compiler::arbitrary::EdgeTy {
                name: "Fwd".into(),
                from: 0,
                to: 1,
                props: vec![],
            },
            omnigraph_compiler::arbitrary::EdgeTy {
                name: "Back".into(),
                from: 1,
                to: 0,
                props: vec![],
            },
        ],
    };
    assert_eq!(schema.text().replace(' ', ""), schema_text.replace(' ', ""));
    let mut runner = law_runner(64, "tests/regressions/law_refusal_soundness_converse.txt");
    let found = std::sync::atomic::AtomicBool::new(false);
    let _ = runner.run(&choices(256), |g| {
        let graph = GenGraph::generate(&mut Choices::new(&g), &schema, 5, 8);
        let reference =
            omnigraph_reference::Graph::from_export(&catalog, &graph.jsonl(&schema)).unwrap();
        let (rows, _) =
            omnigraph_reference::Interpreter::new(&catalog, &reference, &HashMap::new())
                .run(&read)
                .unwrap();
        if !rows.is_empty() {
            found.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    });
    assert!(
        found.load(std::sync::atomic::Ordering::Relaxed),
        "no generated graph gave the admitted selection a row"
    );
    // Both members contribute: one generated graph holds a `Back` edge
    // whose row the selection returns, read from `$a` against `Back`'s
    // declared direction, as its end types fix.
    let seed = "{\"type\":\"A\",\"data\":{\"k\":\"a\"}}\n{\"type\":\"B\",\"data\":{\"k\":\"b\"}}\n{\"edge\":\"Back\",\"from\":\"b\",\"to\":\"a\"}\n";
    let reference = omnigraph_reference::Graph::from_export(&catalog, seed).unwrap();
    let (rows, _) = omnigraph_reference::Interpreter::new(&catalog, &reference, &HashMap::new())
        .run(&read)
        .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
}
