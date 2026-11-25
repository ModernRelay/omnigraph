//! GQ 2 and GQ 3 answer the reads both can express alike.
//!
//! The generator draws a read as a tree over a generated schema (p21's
//! `GenSchema` and `GenGraph`): node bindings, triples in their edges'
//! declared direction (a hop range on an edge between one type), filters
//! (comparisons with a literal the graph holds, null tests, `in` over a
//! list), a negated pattern, return items with and without aliases, a
//! count grouped by a key, and `order` over every returned column with an
//! optional `limit`. Two test-only printers write the same tree as
//! GQ 2 text (the clause list inside `match`, `not { }` with the
//! fresh name only in its triple) and as GQ 3 text (`match { … }`,
//! `where … and …`, `not exists { $z: T, … }`). Nothing here reads user
//! text: the printers start from generated trees, and they exist only in
//! this test.
//!
//! The law: the GQ 2 binary (`OMNIGRAPH_GQ_2_BIN`, the `omnigraph`
//! CLI of the pinned merge base, `GQ_2_SOURCE_COMMIT` in `ci.yml`) and
//! this tree's engine return equal rows under the read's order rule, or
//! both refuse. The tree avoids the shapes whose answers the GQ 3 RFC's
//! Compatibility section changes on purpose (a `limit` without a total
//! `order`, an integer compared with a float literal, full-text and vector
//! search, letter case), so every difference is a defect; the RFC's
//! sanctioned differences are the cross-version table's
//! (`crates/omnigraph-gqt/cross-version.tsv`).
//!
//! Without the binary the law prints a skip line; with
//! `OMNIGRAPH_REQUIRE_GQ_2=1`, as the `Cross-Version Comparison` job
//! sets, a missing binary fails it.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph_compiler::ParamMap;
use omnigraph_compiler::arbitrary::{
    Choices, GenGraph, GenSchema, PropTy, Sc, SchemaOptions, choices,
};
use proptest::test_runner::TestCaseError;
use serde_json::Value;

use crate::law_engine_reference::law_runner;

const CASES: u32 = 48;
const NIGHTLY: u32 = 1_500;

// ---------------------------------------------------------------------------
// The tree.

#[derive(Debug, Clone)]
struct Binding {
    var: String,
    ty: usize,
}

#[derive(Debug, Clone)]
struct Triple {
    src: String,
    edge: usize,
    dst: String,
    /// `{m,n}` on an edge between one type.
    hops: Option<(u32, u32)>,
}

#[derive(Debug, Clone)]
enum Filter {
    Cmp {
        var: String,
        prop: String,
        op: &'static str,
        lit: String,
    },
    Null {
        var: String,
        prop: String,
        negated: bool,
    },
    In {
        var: String,
        prop: String,
        list: Vec<String>,
    },
}

/// A negated pattern: no `edge` from `var` to a fresh node of type `ty`.
#[derive(Debug, Clone)]
struct Negation {
    var: String,
    edge: usize,
    fresh: String,
    ty: usize,
}

#[derive(Debug, Clone)]
enum Item {
    Prop {
        var: String,
        prop: String,
        alias: Option<String>,
    },
    Count {
        var: String,
        alias: String,
    },
}

#[derive(Debug, Clone)]
struct Read {
    bindings: Vec<Binding>,
    triples: Vec<Triple>,
    filters: Vec<Filter>,
    negation: Option<Negation>,
    items: Vec<Item>,
    order: Vec<(usize, bool)>,
    limit: Option<u32>,
}

impl Read {
    fn ordered(&self) -> bool {
        !self.order.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Generation over a schema and the graph it will run on.

/// A JSON scalar as a literal both versions read: numbers, `true`/`false`,
/// and a JSON string, whose escapes GQ reads alike. GQ 2 has no
/// exponent in a number, so `1.5e3` is written `1500.0`, and a number that
/// needs one has no literal there.
fn literal(json: &str) -> Option<String> {
    if !json.contains(['e', 'E']) || json.starts_with('"') {
        return Some(json.to_string());
    }
    let written = format!("{:?}", json.parse::<f64>().ok()?);
    (!written.contains(['e', 'E'])).then_some(written)
}

fn generate(c: &mut Choices, schema: &GenSchema, graph: &GenGraph) -> Read {
    let mut bindings = Vec::new();
    let mut triples = Vec::new();
    let t0 = c.below(schema.nodes.len());
    bindings.push(Binding {
        var: "$x".into(),
        ty: t0,
    });
    // A triple from `$x` along an edge leaving its type, in the declared
    // direction.
    let leaving: Vec<usize> = (0..schema.edges.len())
        .filter(|&e| schema.edges[e].from == t0)
        .collect();
    if !leaving.is_empty() && c.flag(60) {
        let e = leaving[c.below(leaving.len())];
        let edge = &schema.edges[e];
        let hops = (edge.from == edge.to && c.flag(30)).then(|| *c.pick(&[(1, 2), (2, 2), (1, 3)]));
        bindings.push(Binding {
            var: "$y".into(),
            ty: edge.to,
        });
        triples.push(Triple {
            src: "$x".into(),
            edge: e,
            dst: "$y".into(),
            hops,
        });
    }
    let mut filters = Vec::new();
    for _ in 0..c.below(3) {
        let b = &bindings[c.below(bindings.len())];
        let props: Vec<_> = schema.nodes[b.ty]
            .props
            .iter()
            .filter(|p| match p.ty {
                PropTy::Scalar(sc) => !matches!(sc, Sc::Date | Sc::DateTime),
                PropTy::Enum => true,
                _ => false,
            })
            .collect();
        if props.is_empty() {
            continue;
        }
        let p = props[c.below(props.len())];
        // GQ 2 has no negative literal (its grammar has no unary
        // minus), so a negative value stays out of its subset.
        let values: Vec<String> = graph.nodes[b.ty]
            .iter()
            .filter_map(|r| {
                r.values
                    .iter()
                    .find(|(n, _)| *n == p.name)
                    .map(|(_, v)| v.clone())
            })
            .filter(|v| !v.starts_with('-'))
            .collect();
        let pick = |c: &mut Choices| -> Option<String> {
            (!values.is_empty())
                .then(|| literal(&values[c.below(values.len())]))
                .flatten()
        };
        let orderable = !matches!(p.ty, PropTy::Scalar(Sc::Bool) | PropTy::Enum);
        match c.below(3) {
            0 if p.nullable => filters.push(Filter::Null {
                var: b.var.clone(),
                prop: p.name.clone(),
                negated: c.flag(50),
            }),
            1 => {
                let list: Vec<String> = (0..1 + c.below(2)).filter_map(|_| pick(c)).collect();
                if !list.is_empty() {
                    filters.push(Filter::In {
                        var: b.var.clone(),
                        prop: p.name.clone(),
                        list,
                    });
                }
            }
            _ => {
                if let Some(lit) = pick(c) {
                    let op = if orderable {
                        *c.pick(&["=", "!=", "<", "<=", ">", ">="])
                    } else {
                        *c.pick(&["=", "!="])
                    };
                    filters.push(Filter::Cmp {
                        var: b.var.clone(),
                        prop: p.name.clone(),
                        op,
                        lit,
                    });
                }
            }
        }
    }
    let negation = (!leaving.is_empty() && c.flag(25)).then(|| {
        let e = leaving[c.below(leaving.len())];
        Negation {
            var: "$x".into(),
            edge: e,
            fresh: "$z".into(),
            ty: schema.edges[e].to,
        }
    });
    let mut items = Vec::new();
    let grouped = bindings.len() > 1 && c.flag(25);
    if grouped {
        items.push(Item::Prop {
            var: "$x".into(),
            prop: "k".into(),
            alias: None,
        });
        items.push(Item::Count {
            var: "$y".into(),
            alias: "n".into(),
        });
    } else {
        for i in 0..1 + c.below(3) {
            let b = &bindings[c.below(bindings.len())];
            let props: Vec<String> = std::iter::once("k".to_string())
                .chain(
                    schema.nodes[b.ty]
                        .props
                        .iter()
                        .filter(|p| matches!(p.ty, PropTy::Scalar(_) | PropTy::Enum))
                        .map(|p| p.name.clone()),
                )
                .collect();
            let prop = props[c.below(props.len())].clone();
            let alias = c.flag(40).then(|| format!("c{i}"));
            // Two items with one column name are refused by both versions.
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("{}.{prop}", &b.var[1..]));
            if items.iter().any(|it| column(it) == name) {
                continue;
            }
            items.push(Item::Prop {
                var: b.var.clone(),
                prop,
                alias,
            });
        }
    }
    // `order` over every returned column, so the order is total over the
    // rows, and a `limit` only beneath it.
    let (order, limit) = if c.flag(50) {
        let order = (0..items.len()).map(|i| (i, c.flag(40))).collect();
        (order, c.flag(50).then(|| 1 + c.below(4) as u32))
    } else {
        (Vec::new(), None)
    };
    Read {
        bindings,
        triples,
        filters,
        negation,
        items,
        order,
        limit,
    }
}

fn column(item: &Item) -> String {
    match item {
        Item::Prop { alias: Some(a), .. } | Item::Count { alias: a, .. } => a.clone(),
        Item::Prop { var, prop, .. } => format!("{}.{prop}", &var[1..]),
    }
}

// ---------------------------------------------------------------------------
// The two printers.

fn filter_text(f: &Filter) -> String {
    match f {
        Filter::Cmp { var, prop, op, lit } => format!("{var}.{prop} {op} {lit}"),
        Filter::Null { var, prop, negated } => {
            format!("{var}.{prop} is {}null", if *negated { "not " } else { "" })
        }
        Filter::In { var, prop, list } => format!("{var}.{prop} in [{}]", list.join(", ")),
    }
}

fn triple_text(schema: &GenSchema, t: &Triple) -> String {
    let hops = t
        .hops
        .map(|(m, n)| format!("{{{m},{n}}}"))
        .unwrap_or_default();
    format!(
        "{} {}{hops} {}",
        t.src,
        schema.edges[t.edge].surface(),
        t.dst
    )
}

fn item_text(item: &Item) -> String {
    match item {
        Item::Prop { var, prop, alias } => match alias {
            Some(a) => format!("{var}.{prop} as {a}"),
            None => format!("{var}.{prop}"),
        },
        Item::Count { var, alias } => format!("count({var}) as {alias}"),
    }
}

fn tail(read: &Read) -> String {
    let mut s = format!(
        "  return {{ {} }}\n",
        read.items
            .iter()
            .map(item_text)
            .collect::<Vec<_>>()
            .join(", ")
    );
    if !read.order.is_empty() {
        let keys: Vec<String> = read
            .order
            .iter()
            .map(|(i, desc)| {
                let key = match &read.items[*i] {
                    Item::Prop {
                        var,
                        prop,
                        alias: None,
                    } => format!("{var}.{prop}"),
                    other => column(other),
                };
                format!("{key}{}", if *desc { " desc" } else { "" })
            })
            .collect();
        let _ = writeln!(s, "  order {{ {} }}", keys.join(", "));
    }
    if let Some(n) = read.limit {
        let _ = writeln!(s, "  limit {n}");
    }
    s
}

/// GQ 2: one clause per line inside `match`, `not { }` naming the fresh
/// node only in its triple.
fn gq_2(schema: &GenSchema, read: &Read) -> String {
    let mut clauses: Vec<String> = read
        .bindings
        .iter()
        .map(|b| format!("{}: {}", b.var, schema.nodes[b.ty].name))
        .collect();
    clauses.extend(read.triples.iter().map(|t| triple_text(schema, t)));
    clauses.extend(read.filters.iter().map(filter_text));
    if let Some(n) = &read.negation {
        clauses.push(format!(
            "not {{ {} {} {} }}",
            n.var,
            schema.edges[n.edge].surface(),
            n.fresh
        ));
    }
    format!(
        "query q() {{\n  match {{\n    {}\n  }}\n{}}}\n",
        clauses.join("\n    "),
        tail(read)
    )
}

/// GQ 3: the pattern in `match`, the filters and the negated pattern
/// in `where`, joined by `and` in written order.
fn gq_3(schema: &GenSchema, read: &Read) -> String {
    let mut elements: Vec<String> = read
        .bindings
        .iter()
        .map(|b| format!("{}: {}", b.var, schema.nodes[b.ty].name))
        .collect();
    elements.extend(read.triples.iter().map(|t| triple_text(schema, t)));
    let mut conjuncts: Vec<String> = read.filters.iter().map(filter_text).collect();
    if let Some(n) = &read.negation {
        conjuncts.push(format!(
            "not exists {{ {}: {}, {} {} {} }}",
            n.fresh,
            schema.nodes[n.ty].name,
            n.var,
            schema.edges[n.edge].surface(),
            n.fresh
        ));
    }
    let mut s = format!("query q() {{\n  match {{ {} }}\n", elements.join(", "));
    if !conjuncts.is_empty() {
        let _ = writeln!(s, "  where {}", conjuncts.join(" and "));
    }
    s.push_str(&tail(read));
    s.push_str("}\n");
    s
}

// ---------------------------------------------------------------------------
// Running both versions.

fn gq_2_binary() -> Option<PathBuf> {
    match std::env::var_os("OMNIGRAPH_GQ_2_BIN") {
        Some(path) => Some(PathBuf::from(path)),
        None if std::env::var("OMNIGRAPH_REQUIRE_GQ_2").as_deref() == Ok("1") => {
            panic!("OMNIGRAPH_GQ_2_BIN is unset and OMNIGRAPH_REQUIRE_GQ_2=1")
        }
        None => None,
    }
}

/// The GQ 2 binary over one graph: `init` and `load` once, then a
/// query per call.
struct Gq2 {
    binary: PathBuf,
    dir: tempfile::TempDir,
}

impl Gq2 {
    fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        command
            .env("OMNIGRAPH_HOME", self.dir.path().join("home"))
            .current_dir(self.dir.path());
        command
    }

    fn new(binary: &Path, schema: &str, jsonl: &str) -> Result<Self, String> {
        let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        std::fs::write(dir.path().join("s.pg"), schema).map_err(|e| e.to_string())?;
        std::fs::write(dir.path().join("d.jsonl"), jsonl).map_err(|e| e.to_string())?;
        let me = Self {
            binary: binary.to_path_buf(),
            dir,
        };
        let init = me
            .command()
            .args(["init", "--schema", "s.pg", "./g"])
            .output()
            .map_err(|e| format!("could not run the GQ 2 binary: {e}"))?;
        if !init.status.success() {
            return Err(format!(
                "GQ 2 init: {}",
                String::from_utf8_lossy(&init.stderr)
            ));
        }
        if !jsonl.is_empty() {
            let load = me
                .command()
                .args([
                    "load",
                    "--data",
                    "d.jsonl",
                    "--mode",
                    "overwrite",
                    "--target",
                    "./g",
                    "--json",
                ])
                .output()
                .map_err(|e| e.to_string())?;
            if !load.status.success() {
                return Err(format!(
                    "GQ 2 load: {}",
                    String::from_utf8_lossy(&load.stdout)
                ));
            }
        }
        Ok(me)
    }

    /// The rows, or the refusal's text.
    fn query(&self, source: &str) -> Result<Vec<Value>, String> {
        let out = self
            .command()
            .args(["query", "--target", "./g", "--json", "-e", source, "q"])
            .output()
            .map_err(|e| e.to_string())?;
        let doc: Value = serde_json::from_slice(&out.stdout)
            .map_err(|e| format!("undecodable GQ 2 output: {e}"))?;
        if let Some(error) = doc.get("error") {
            return Err(error.as_str().unwrap_or_default().to_string());
        }
        match doc.get("rows") {
            Some(Value::Array(rows)) => Ok(rows.clone()),
            other => Err(format!("GQ 2 output without rows: {other:?}")),
        }
    }
}

async fn gq_3_session(schema: &str, jsonl: &str) -> Result<(tempfile::TempDir, Session), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
        .await
        .map_err(|e| format!("init: {e}"))?;
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    if !jsonl.is_empty() {
        session
            .load_jsonl(jsonl, LoadMode::Overwrite)
            .await
            .map_err(|e| format!("load: {e}"))?;
    }
    Ok((dir, session))
}

fn canonical_rows(rows: &[Value], ordered: bool) -> Vec<String> {
    let mut out: Vec<String> = rows.iter().map(|r| canonical(r).to_string()).collect();
    if !ordered {
        out.sort();
    }
    out
}

/// A row with its object keys sorted and its numbers as `f64` (GQ 2
/// and GQ 3 may render one value `1` and `1.0`).
fn canonical(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), canonical(v)))
                .collect::<serde_json::Map<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(canonical).collect()),
        Value::Number(n) => serde_json::Number::from_f64(n.as_f64().unwrap_or(f64::NAN))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        other => other.clone(),
    }
}

/// The schema both versions accept: GQ 2 indexes no list property.
fn gq_2_schema(mut schema: GenSchema) -> GenSchema {
    for node in &mut schema.nodes {
        for p in &mut node.props {
            if matches!(p.ty, PropTy::ListStr) {
                p.indexed = false;
            }
        }
    }
    schema
}

static COMPARED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static WITH_ROWS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static BOTH_REFUSED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static SELF_LOOP: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The read with its hop range's start node excluded (`$x.k != $y.k`), when
/// it has a hop range other than the single hop.
fn without_start_node(read: &Read) -> Option<Read> {
    let t = read.triples.iter().find(|t| t.hops.is_some())?;
    let mut read = read.clone();
    read.filters.push(Filter::Cmp {
        var: t.src.clone(),
        prop: "k".into(),
        op: "!=",
        lit: format!("{}.k", t.dst),
    });
    // A `limit` cuts different rows once the start node's are gone.
    read.limit = None;
    Some(read)
}

fn run_law(cases: u32) {
    let Some(binary) = gq_2_binary() else {
        eprintln!("skipping the two-version law: OMNIGRAPH_GQ_2_BIN is unset");
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = law_runner(cases, "tests/regressions/law_two_versions.txt");
    let options = SchemaOptions {
        search: false,
        ..SchemaOptions::default()
    };
    let result = runner.run(&(choices(64), choices(1024), choices(64)), |(s, g, q)| {
        let schema = gq_2_schema(GenSchema::generate(&mut Choices::new(&s), options));
        let graph = GenGraph::generate(&mut Choices::new(&g), &schema, 5, 8);
        let read = generate(&mut Choices::new(&q), &schema, &graph);
        let (old, new) = (gq_2(&schema, &read), gq_3(&schema, &read));
        let jsonl = graph.jsonl(&schema);
        let describe = || {
            format!(
                "--- schema\n{}\n--- seed\n{jsonl}\n--- GQ 2\n{old}\n--- GQ 3\n{new}",
                schema.text()
            )
        };
        let gq2 = Gq2::new(&binary, &schema.text(), &jsonl)
            .map_err(|e| TestCaseError::fail(format!("{e}\n{}", describe())))?;
        let a = gq2.query(&old);
        let b = rt.block_on(async {
            let (_dir, session) = gq_3_session(&schema.text(), &jsonl).await?;
            session
                .query(ReadTarget::branch("main"), &new, "q", &ParamMap::new())
                .await
                .map_err(|e| e.to_string())
                .and_then(|r| match r.to_rust_json() {
                    Ok(Value::Array(rows)) => Ok(rows),
                    Ok(other) => Err(format!("rendered {other}")),
                    Err(e) => Err(e.to_string()),
                })
        });
        COMPARED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ordered = read.ordered();
        let verdict = match (&a, &b) {
            (Ok(x), Ok(y)) => {
                if !x.is_empty() {
                    WITH_ROWS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                if canonical_rows(x, ordered) == canonical_rows(y, ordered) {
                    Ok(())
                } else if let Some(without) = without_start_node(&read) {
                    // The difference the cross-version table cites as
                    // `results.hop-range-self-loop`: a hop range no longer
                    // returns its start node through a self-loop. With
                    // the start node excluded, the rows must agree.
                    let (old, new) = (gq_2(&schema, &without), gq_3(&schema, &without));
                    let a = gq2.query(&old);
                    let b = rt.block_on(async {
                        let (_dir, session) = gq_3_session(&schema.text(), &jsonl).await?;
                        session
                            .query(ReadTarget::branch("main"), &new, "q", &ParamMap::new())
                            .await
                            .map_err(|e| e.to_string())
                            .and_then(|r| r.to_rust_json().map_err(|e| e.to_string()))
                    });
                    match (a, b) {
                        (Ok(x), Ok(Value::Array(y)))
                            if canonical_rows(&x, ordered) == canonical_rows(&y, ordered) =>
                        {
                            SELF_LOOP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Ok(())
                        }
                        _ => Err("the rows differ, beyond a hop range's start node"),
                    }
                } else {
                    Err("the rows differ")
                }
            }
            (Err(_), Err(_)) => {
                BOTH_REFUSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            (Err(_), Ok(_)) => Err("GQ 2 refused, GQ 3 answered"),
            (Ok(_), Err(_)) => Err("GQ 2 answered, GQ 3 refused"),
        };
        verdict.map_err(|why| {
            TestCaseError::fail(format!(
                "{why}\n{}\n--- GQ 2 answer\n{a:?}\n--- GQ 3 answer\n{b:?}",
                describe()
            ))
        })
    });
    if let Err(e) = result {
        panic!("{e}");
    }
    let compared = COMPARED.load(std::sync::atomic::Ordering::Relaxed);
    let with_rows = WITH_ROWS.load(std::sync::atomic::Ordering::Relaxed);
    let refused = BOTH_REFUSED.load(std::sync::atomic::Ordering::Relaxed);
    let self_loop = SELF_LOOP.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "{compared} reads compared: {with_rows} with rows, {refused} refused by both, \
         {self_loop} differing only by a hop range's start node (results.hop-range-self-loop)"
    );
    assert!(
        with_rows * 3 >= compared,
        "only {with_rows} of {compared} reads returned rows"
    );
    assert!(
        refused * 10 <= compared,
        "{refused} of {compared} reads were refused by both"
    );
}

#[test]
fn gq_2_and_gq_3_answer_generated_reads_alike() {
    run_law(CASES);
}

#[test]
#[ignore = "nightly: 1,500 generated reads; the gate runs 48"]
fn gq_2_and_gq_3_answer_generated_reads_alike_at_nightly_breadth() {
    run_law(NIGHTLY);
}

/// The two printers write one tree: the GQ 3 text type-checks, and
/// both name the same bindings, triples and return items.
#[test]
fn the_printers_write_one_tree_in_two_versions() {
    let mut runner = law_runner(256, "tests/regressions/law_two_versions_printers.txt");
    let options = SchemaOptions {
        search: false,
        ..SchemaOptions::default()
    };
    runner
        .run(&(choices(64), choices(256), choices(64)), |(s, g, q)| {
            let schema = GenSchema::generate(&mut Choices::new(&s), options);
            let graph = GenGraph::generate(&mut Choices::new(&g), &schema, 5, 8);
            let read = generate(&mut Choices::new(&q), &schema, &graph);
            let new = gq_3(&schema, &read);
            let catalog = omnigraph_compiler::catalog::build_catalog(
                &omnigraph_compiler::schema::parser::parse_schema(&schema.text()).unwrap(),
            )
            .unwrap();
            let checked = omnigraph_compiler::query::check::check_source(&catalog, &new)
                .map_err(|d| TestCaseError::fail(format!("{}: {}\n{new}", d.code, d.message)))?;
            for (_, r) in checked {
                r.map_err(|d| TestCaseError::fail(format!("{}: {}\n{new}", d.code, d.message)))?;
            }
            Ok(())
        })
        .unwrap();
}
