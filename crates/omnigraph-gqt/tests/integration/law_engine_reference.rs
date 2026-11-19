//! Law 1: the engine equals the reference interpreter (the GQ 3 RFC,
//! section 10), over generated schemas, graphs and well-typed reads.
//!
//! The generator (`omnigraph_compiler::arbitrary`) draws a schema of one to
//! three node types and up to three edges, a graph over it (shared keys
//! across types, self-loops, parallel edges, nulls, the integer extremes),
//! and one read built type-directed over that schema: bindings, triples
//! (hop ranges, undirected and alternation selectors, edge bindings, closed
//! cycles), `optional`, correlated `union`, uncorrelated grouped inline
//! reads, `let` of values and of blocks, blocks as operands (`count`,
//! `sum`, `min`, `max`, `avg`, `exists`, `not exists`), the guards `if`,
//! `coalesce`, `and`, `or`, arithmetic with nulls over `I32`, `I64`, `F32`
//! and `F64`, comparisons over every scalar, `in` over list literals,
//! list and enum properties, parameters (nullable, omitted), `return`,
//! `return distinct`, groups with every aggregate and `count(distinct …)`,
//! the registry's scalar rows (`lower`, `upper`, `trim`, `ltrim`, `rtrim`,
//! `length`, `left`, `right`, `substring`, `abs`, `floor`, `ceil`, `round`
//! with and without `digits:`, `sqrt`, `power`, `exp`, `ln`, `log10`,
//! `size`, `cast` between the pairs it converts) over values with Unicode
//! whitespace, case mappings, negative zero and the texts `cast` reads or
//! refuses, `order` with `nulls first` and `nulls last`, `limit` (a literal
//! or a parameter), and the rankings `nearest`, `distance`, `bm25`, `rrf`
//! (with `weights`).
//!
//! The law: the engine's answer equals the reference's, as rows under the
//! result's order rule (ordered when the read has an `order`, as bags
//! otherwise) or as the same refusal, under every traversal mode (`csr`,
//! `indexed` and the planner's choice) and again after every declared index
//! is built. Writes are not generated: the reference evaluates reads only.
//! Two run-time refusals with different codes agree when the read can raise
//! each: the RFC fixes each row's refusal, not which one a read reports when
//! its rows fail in different ways ([`refused_in_two_ways`]).
//!
//! The metamorphic layer reruns each read rewritten to an equal one: a
//! tautology conjoined to its `where`, its top-level conjuncts reversed
//! when none can fail, and an `rrf`'s arms swapped with their weights (the
//! fusion is invariant under permuting its arms).
//!
//! A failure prints the shrunk read, the schema, the graph and both
//! answers. Seeds are deterministic; a failing case's seed is written to
//! `tests/regressions/law_engine_reference.txt`, which every run replays
//! first.

use std::sync::Arc;

use omnigraph::Session;
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;
use omnigraph::settings::{SessionSettings, Traversal};
use omnigraph_compiler::ParamMap;
use omnigraph_compiler::arbitrary::{
    Choices, GenGraph, GenRead, GenSchema, ReadOptions, SchemaOptions, choices,
};
use proptest::test_runner::{
    Config, FileFailurePersistence, RngAlgorithm, TestCaseError, TestRng, TestRunner,
};

/// The per-pull-request case count; the nightly test runs `NIGHTLY`.
const CASES: u32 = 64;
const NIGHTLY: u32 = 2_000;

/// A law's runner: `cases` cases (`OMNIGRAPH_LAW_CASES` overrides it for a
/// local hunt) from the deterministic seed, or from `OMNIGRAPH_LAW_SEED`
/// (a number) when set; a failing case is persisted to `regressions` and
/// replayed first by every later run.
pub(crate) fn law_runner(cases: u32, regressions: &'static str) -> TestRunner {
    let cases = std::env::var("OMNIGRAPH_LAW_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(cases);
    let config = Config {
        cases,
        max_shrink_iters: 4_000,
        max_shrink_time: 120_000,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(regressions))),
        ..Config::default()
    };
    let rng = match std::env::var("OMNIGRAPH_LAW_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&seed.to_le_bytes());
            TestRng::from_seed(RngAlgorithm::ChaCha, &bytes)
        }
        None => TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    };
    TestRunner::new_with_rng(config, rng)
}

/// One generated sample.
#[derive(Debug)]
pub(crate) struct Sample {
    pub schema: GenSchema,
    pub graph: GenGraph,
    pub read: GenRead,
}

pub(crate) fn sample(s: &[u32], g: &[u32], q: &[u32], read: ReadOptions) -> Sample {
    let schema = GenSchema::generate(&mut Choices::new(s), SchemaOptions::default());
    let graph = GenGraph::generate(&mut Choices::new(g), &schema, 5, 8);
    let read = GenRead::generate(&mut Choices::new(q), &schema, read);
    Sample {
        schema,
        graph,
        read,
    }
}

impl Sample {
    pub(crate) fn params(&self) -> ParamMap {
        self.read.params.iter().cloned().collect()
    }

    pub(crate) fn describe(&self) -> String {
        format!(
            "--- schema\n{}\n--- seed\n{}\n--- query\n{}\n--- params\n{:?}",
            self.schema.text(),
            self.graph.jsonl(&self.schema),
            self.read.source,
            self.read.params
        )
    }
}

/// A fresh graph holding the sample's schema and rows.
pub(crate) async fn load(sample: &Sample) -> Result<(tempfile::TempDir, Session), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, &sample.schema.text())
        .await
        .map_err(|e| format!("init failed: {e}"))?;
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    let jsonl = sample.graph.jsonl(&sample.schema);
    if !jsonl.is_empty() {
        session
            .load_jsonl(&jsonl, LoadMode::Overwrite)
            .await
            .map_err(|e| format!("load failed: {e}"))?;
    }
    Ok((dir, session))
}

fn with_traversal(session: &Session, mode: Traversal) -> Session {
    let settings = session.settings().clone().with_traversal(mode);
    Session::from_defaults(Arc::clone(session.db()), settings)
}

/// Judges `source` on `session`, failing with the whole sample printed;
/// true when the engine answered rows.
async fn judge(
    session: &Session,
    sample: &Sample,
    source: &str,
    label: &str,
) -> Result<bool, String> {
    let judgment = omnigraph_gqt::judge_read_against_reference(
        session,
        source,
        "q",
        &sample.params(),
        sample.read.ordered,
    )
    .await
    .map_err(|e| format!("{label}: {e}\n{}", sample.describe()))?;
    let rows = judgment.engine.starts_with("[{");
    if judgment.verdict.is_err() && refused_in_two_ways(source, &judgment) {
        MIXED_REFUSALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Ok(false);
    }
    judgment.verdict.map(|()| rows).map_err(|e| {
        format!(
            "{label}: {e}\n{}\n--- rewritten query\n{source}\n--- engine\n{}\n--- reference\n{}",
            sample.describe(),
            judgment.engine,
            judgment.reference
        )
    })
}

/// Reads both sides refused at run time with different codes, each one the
/// read's text can raise.
static MIXED_REFUSALS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Whether the engine and the reference refused at run time with two
/// different codes the read can each raise. The RFC fixes each row's
/// refusal but not which one a read reports when its rows fail in
/// different ways: the reference evaluates a row at a time and reports the
/// first row's, the engine an operator at a time over a batch and reports
/// the first operator's (`log10(abs(round($x.p, digits: (-1))))` refuses
/// with `E5` on a row holding `0` and with `E1` on a row whose rounding
/// leaves `I32`). A code the read cannot raise still disagrees, and so
/// does a refusal against rows.
fn refused_in_two_ways(source: &str, judgment: &omnigraph_gqt::ReferenceJudgment) -> bool {
    let engine = judgment
        .engine
        .strip_prefix("refused: Coded(\"")
        .and_then(|rest| rest.get(..2));
    let reference = judgment
        .reference
        .strip_prefix("refused: ")
        .and_then(|rest| rest.get(..2));
    let (Some(engine), Some(reference)) = (engine, reference) else {
        return false;
    };
    let markers: &[(&str, &[&str])] = &[
        (
            "E1",
            &[" + ", " - ", " * ", "(-", "abs(", "round(", "sum(", "avg("],
        ),
        ("E2", &[" / ", " % "]),
        (
            "E3",
            &[
                " / ", " + ", " - ", " * ", "(-", "exp(", "power(", "round(", "sqrt(", "ln(",
                "log10(", "sum(", "avg(",
            ],
        ),
        (
            "E5",
            &[
                "sqrt(",
                "ln(",
                "log10(",
                "left(",
                "right(",
                "substring(",
                "round(",
                "power(",
                "percentile(",
            ],
        ),
        ("E6", &["cast("]),
    ];
    let raises = |code: &str| {
        markers
            .iter()
            .find(|(c, _)| *c == code)
            .is_some_and(|(_, m)| m.iter().any(|m| source.contains(m)))
    };
    engine != reference && raises(engine) && raises(reference)
}

/// The read with `extra` conjoined to its `where` (one is added when it
/// has none) and, when `reverse`, its top-level conjuncts reversed.
fn rewritten(source: &str, extra: &str, reverse: bool) -> Option<String> {
    let lines: Vec<&str> = source.lines().collect();
    let mut out = Vec::new();
    let mut seen_where = false;
    for line in &lines {
        if let Some(pred) = line.strip_prefix("  where ") {
            seen_where = true;
            let mut conjuncts = split_conjuncts(pred);
            // Conjuncts filter in written order, and a conjunct that can
            // fail keeps its place (A3): only conjuncts that cannot fail
            // are independent.
            if reverse && conjuncts.iter().any(|c| can_fail(c)) {
                return None;
            }
            if reverse {
                conjuncts.reverse();
            }
            if !extra.is_empty() {
                conjuncts.push(extra.to_string());
            }
            out.push(format!("  where {}", conjuncts.join(" and ")));
        } else if line.starts_with("  return") && !seen_where {
            seen_where = true;
            if extra.is_empty() {
                return None;
            }
            out.push(format!("  where {extra}"));
            out.push(line.to_string());
        } else {
            out.push(line.to_string());
        }
    }
    Some(out.join("\n"))
}

/// The read with its `rrf`'s two arms swapped, their weights with them.
fn rrf_arms_swapped(source: &str) -> Option<String> {
    let start = source.find("from rrf(")? + "from rrf(".len();
    let end = start + source[start..].find(") as $s")?;
    let inner = &source[start..end];
    let split = inner.find(", bm25(")?;
    let (first, rest) = (&inner[..split], &inner[split + 2..]);
    let tail_at = rest.find(", k: ")?;
    let (second, tail) = (&rest[..tail_at], &rest[tail_at..]);
    let tail = match tail.split_once(", weights: [") {
        Some((k, weights)) => {
            let weights = weights.strip_suffix(']')?;
            let (a, b) = weights.split_once(", ")?;
            format!("{k}, weights: [{b}, {a}]")
        }
        None => tail.to_string(),
    };
    Some(format!(
        "{}{second}, {first}{tail}{}",
        &source[..start],
        &source[end..]
    ))
}

/// Whether a conjunct may hold a term that can fail: arithmetic, an
/// aggregate that can overflow, or a registry row that can fail
/// (`ScalarFunc::row_can_fail`).
fn can_fail(conjunct: &str) -> bool {
    [
        " / ",
        " % ",
        " * ",
        " + ",
        " - ",
        "(-",
        "sum(",
        "avg(",
        "left(",
        "right(",
        "substring(",
        "abs(",
        "round(",
        "sqrt(",
        "exp(",
        "ln(",
        "log10(",
        "power(",
        "cast(",
    ]
    .iter()
    .any(|op| conjunct.contains(op))
}

/// The top-level `and` conjuncts of a fully parenthesized predicate.
fn split_conjuncts(pred: &str) -> Vec<String> {
    let inner = pred
        .strip_prefix('(')
        .and_then(|p| p.strip_suffix(')'))
        .filter(|p| balanced(p));
    let Some(inner) = inner else {
        return vec![pred.to_string()];
    };
    let mut depth = 0i32;
    let bytes = inner.as_bytes();
    let mut in_str = false;
    for i in 0..bytes.len() {
        match bytes[i] {
            b'"' if i == 0 || bytes[i - 1] != b'\\' => in_str = !in_str,
            b'(' | b'{' | b'[' if !in_str => depth += 1,
            b')' | b'}' | b']' if !in_str => depth -= 1,
            _ => {}
        }
        if depth == 0 && !in_str && inner[i..].starts_with(" and ") {
            let (a, b) = (&inner[..i], &inner[i + 5..]);
            let mut left = split_conjuncts(a);
            left.extend(split_conjuncts(b));
            return left;
        }
    }
    vec![pred.to_string()]
}

fn balanced(s: &str) -> bool {
    let mut depth = 0i32;
    let mut in_str = false;
    let bytes = s.as_bytes();
    for i in 0..bytes.len() {
        match bytes[i] {
            b'"' if i == 0 || bytes[i - 1] != b'\\' => in_str = !in_str,
            b'(' if !in_str => depth += 1,
            b')' if !in_str => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

/// Samples whose read returned at least one row, so the law cannot pass
/// over empty answers alone.
static NONEMPTY: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static TOTAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

async fn check(sample: &Sample) -> Result<(), String> {
    TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (_dir, session) = load(sample).await?;
    let source = &sample.read.source;
    if judge(&session, sample, source, "planner's choice").await? {
        NONEMPTY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    } else if std::env::var_os("OMNIGRAPH_LAW_TRACE").is_some() {
        eprintln!("EMPTY rows={}\n{}", sample.graph.row_count(), source);
    }
    for mode in [Traversal::Csr, Traversal::Indexed] {
        match judge(
            &with_traversal(&session, mode),
            sample,
            source,
            &format!("traversal {mode:?}"),
        )
        .await
        {
            // A forced mode the plan cannot take (an edge selection under
            // `csr`) is refused by its setting, not by the read.
            Err(e) if e.contains("P004") && e.contains("do not support traversal") => {}
            other => {
                other?;
            }
        }
    }
    // Metamorphic: a tautology conjoined, and the conjuncts reversed.
    if let Some(tautology) = rewritten(source, "(1 = 1)", false) {
        judge(&session, sample, &tautology, "a tautology conjoined").await?;
    }
    if let Some(reversed) = rewritten(source, "", true) {
        judge(&session, sample, &reversed, "conjuncts reversed").await?;
    }
    // `rrf` is invariant under permuting its arms with their weights.
    if let Some(permuted) = rrf_arms_swapped(source) {
        judge(&session, sample, &permuted, "rrf arms swapped").await?;
    }
    // Every declared index built.
    session
        .ensure_indices_as(None)
        .await
        .map_err(|e| format!("ensure_indices failed: {e}\n{}", sample.describe()))?;
    judge(&session, sample, source, "indexes built").await?;
    Ok(())
}

fn run_law(cases: u32) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = law_runner(cases, "tests/regressions/law_engine_reference.txt");
    let result = runner.run(&(choices(64), choices(1024), choices(512)), |(s, g, q)| {
        let sample = sample(&s, &g, &q, ReadOptions::default());
        rt.block_on(check(&sample)).map_err(TestCaseError::fail)
    });
    if let Err(e) = result {
        panic!("{e}");
    }
    let nonempty = NONEMPTY.load(std::sync::atomic::Ordering::Relaxed);
    let total = TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "{nonempty} of {total} generated reads returned a row; {} judgments refused in two ways",
        MIXED_REFUSALS.load(std::sync::atomic::Ordering::Relaxed)
    );
    assert!(
        nonempty * 5 >= total,
        "only {nonempty} of {total} generated reads returned a row"
    );
}

/// Generated reads of the placement law (the GQ 3 RFC, section 2): a
/// `where` of two to five conjuncts that mix pure ones with ones that can
/// fail, in every written position, after elements that can fail.
const PLACEMENT_CASES: u32 = 64;
const PLACEMENT_NIGHTLY: u32 = 2_000;

/// The read with the conjuncts written before its first one that can fail
/// reversed: they are pure and total, so their order is the planner's to
/// choose and the answer does not change. `None` when fewer than two
/// precede it.
fn pure_prefix_reversed(source: &str) -> Option<String> {
    let mut out = Vec::new();
    let mut changed = false;
    for line in source.lines() {
        match line.strip_prefix("  where ") {
            Some(pred) => {
                let mut conjuncts = top_level_and(pred);
                let k = conjuncts
                    .iter()
                    .position(|c| can_fail(c))
                    .unwrap_or(conjuncts.len());
                if k >= 2 {
                    conjuncts[..k].reverse();
                    changed = true;
                }
                out.push(format!("  where {}", conjuncts.join(" and ")));
            }
            None => out.push(line.to_string()),
        }
    }
    changed.then(|| out.join("\n"))
}

/// The top-level `and` operands of an unparenthesized chain, each kept as
/// written.
fn top_level_and(pred: &str) -> Vec<String> {
    let bytes = pred.as_bytes();
    let (mut depth, mut in_str, mut start) = (0i32, false, 0);
    let mut out = Vec::new();
    for i in 0..bytes.len() {
        match bytes[i] {
            b'"' if i == 0 || bytes[i - 1] != b'\\' => in_str = !in_str,
            b'(' | b'{' | b'[' if !in_str => depth += 1,
            b')' | b'}' | b']' if !in_str => depth -= 1,
            _ => {}
        }
        if depth == 0 && !in_str && pred[i..].starts_with(" and ") {
            out.push(pred[start..i].to_string());
            start = i + 5;
        }
    }
    out.push(pred[start..].to_string());
    out
}

async fn check_placement(sample: &Sample) -> Result<(), String> {
    TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (_dir, session) = load(sample).await?;
    let source = &sample.read.source;
    if judge(&session, sample, source, "planner's choice").await? {
        NONEMPTY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    if let Some(reversed) = pure_prefix_reversed(source) {
        judge(&session, sample, &reversed, "pure prefix reversed").await?;
    }
    session
        .ensure_indices_as(None)
        .await
        .map_err(|e| format!("ensure_indices failed: {e}\n{}", sample.describe()))?;
    judge(&session, sample, source, "indexes built").await?;
    Ok(())
}

fn run_placement_law(cases: u32) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = law_runner(cases, "tests/regressions/law_placement.txt");
    let options = ReadOptions {
        placement: true,
        search: false,
        ..ReadOptions::default()
    };
    let result = runner.run(&(choices(64), choices(1024), choices(512)), |(s, g, q)| {
        let sample = sample(&s, &g, &q, options);
        rt.block_on(check_placement(&sample))
            .map_err(TestCaseError::fail)
    });
    if let Err(e) = result {
        panic!("{e}");
    }
    let nonempty = NONEMPTY.load(std::sync::atomic::Ordering::Relaxed);
    let total = TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("{nonempty} of {total} generated placement reads returned a row");
}

/// The placement law: a `where`'s conjuncts before the first one that can
/// fail move freely, that one and every one after it filter in written
/// order above every element, and nothing moves below an element that can
/// fail. The engine must answer each generated read as the reference does,
/// rows or the same refusal, before and after its pure prefix is reversed
/// and with every declared index built.
#[test]
fn engine_equals_reference_on_generated_placements() {
    run_placement_law(PLACEMENT_CASES);
}

#[test]
#[ignore = "nightly: 2,000 generated placement reads; the gate runs 64"]
fn engine_equals_reference_on_generated_placements_at_nightly_breadth() {
    run_placement_law(PLACEMENT_NIGHTLY);
}

#[test]
fn engine_equals_reference_on_generated_reads() {
    run_law(CASES);
}

#[test]
#[ignore = "nightly: 2,000 generated reads; the gate runs 64"]
fn engine_equals_reference_on_generated_reads_at_nightly_breadth() {
    run_law(NIGHTLY);
}

/// The shrunk case of a defect law 1 found: the return items are computed
/// beside every row before the cut (`Project(Cut(Order(Extend(R, items),
/// keys), k), items)`, the RFC's derived forms), so a zero divisor in a
/// return item refuses (`E2`) under `limit 0` too, as it does under
/// `limit 1`. A cut of zero rows executes nothing below it only when no
/// expression there can fail; otherwise it pulls its input to the end.
#[test]
fn limit_zero_evaluates_a_return_item_that_can_fail() {
    let schema = "node A {\n    k: String @key\n    p: I32?\n}\n";
    let seed = r#"{"type":"A","data":{"k":"a","p":0}}"#;
    let source = "query q() { match { $n1: A } return { (1 / $n1.p) as c0 } limit 0 }";
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap();
        let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
        session.load_jsonl(seed, LoadMode::Overwrite).await.unwrap();
        let judgment = omnigraph_gqt::judge_read_against_reference(
            &session,
            source,
            "q",
            &ParamMap::new(),
            false,
        )
        .await
        .unwrap();
        judgment.verdict.unwrap_or_else(|e| {
            panic!(
                "{e}\nengine: {}\nreference: {}",
                judgment.engine, judgment.reference
            )
        });
    });
}

/// The shrunk case of a defect the placement law found, which predated it:
/// after a correlated `union`, a triple from the union's binding and an
/// `optional` over its destination whose own `where` can fail, the engine
/// refused its own rows (`$n6.__id` held nulls against its non-null type)
/// where the reference returns none. The shaper placed a triple from a
/// correlated read's column after every `optional`, so the `optional` bound
/// the destination itself and padded it; such a triple now expands before
/// the `optional`s written after it (`optional_over_a_correlated_unions_destination.gqt`).
#[test]
fn an_optional_that_can_fail_after_a_correlated_union_keeps_its_destination() {
    let schema = r#"node A {
    k: String @key
    p0: F64
}
node B {
    k: String @key
    p0: I64?
    p1: String?
}
node C {
    k: String @key
    p0: F64?
    p1: I32
    p2: F32?
}
edge Rel0: C -> C
edge Rel1: C -> A
edge Rel2: B -> A
"#;
    let seed = r#"{"type":"A","data":{"k":"d","p0":1.5e3}}
{"type":"A","data":{"k":"a","p0":100.0}}
{"type":"A","data":{"k":"f","p0":1.5e3}}
{"type":"A","data":{"k":"c","p0":100.0}}
{"type":"A","data":{"k":"b","p0":-2.25}}
{"type":"B","data":{"k":"c","p0":1,"p1":""}}
{"type":"B","data":{"k":"e","p0":2147483647,"p1":""}}
{"type":"B","data":{"k":"a","p0":100,"p1":"graph search"}}
{"type":"C","data":{"k":"f","p0":0.0,"p1":10,"p2":100.0}}
{"edge":"Rel0","from":"f","to":"f","data":{}}
{"edge":"Rel0","from":"f","to":"f","data":{}}
{"edge":"Rel0","from":"f","to":"f","data":{}}
{"edge":"Rel0","from":"f","to":"f","data":{}}
{"edge":"Rel0","from":"f","to":"f","data":{}}
{"edge":"Rel1","from":"f","to":"c","data":{}}
{"edge":"Rel1","from":"f","to":"d","data":{}}
{"edge":"Rel1","from":"f","to":"c","data":{}}
{"edge":"Rel1","from":"f","to":"c","data":{}}
{"edge":"Rel2","from":"a","to":"c","data":{}}
{"edge":"Rel2","from":"e","to":"d","data":{}}
{"edge":"Rel2","from":"c","to":"b","data":{}}"#;
    let source = r#"query q() {
  match { $n1: A, from union({ match { $n3: B, $n3 $k2:rel2 $n1 } where ($n3.p0 >= $n1.p0) return { $n3 as u4 } }, { match { $n5: B } where if(false, true, true) return { $n5 as u4 } }), $u4 rel2 $n6, optional { $n7: C, $n7 rel1 $n6 where ((1 % $n7.p1) = 0) } }
  where if(($n7.p1 != 0), (((-1) / $n7.p1) > 2), false) and (($n7.p1 * 2147483647) > 10) and ($u4.p0 >= (-7)) and ($n7.p1 < 10)
  return { (coalesce(datetime("2023-12-31T23:59:59Z"), datetime("2024-01-01T00:00:00Z")) >= datetime("2023-12-31T23:59:59Z")) as c0 }
  order { c0 desc }
  limit 3
}"#;
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap();
        let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
        session.load_jsonl(seed, LoadMode::Overwrite).await.unwrap();
        let judgment = omnigraph_gqt::judge_read_against_reference(
            &session,
            source,
            "q",
            &ParamMap::new(),
            true,
        )
        .await
        .unwrap();
        judgment.verdict.unwrap_or_else(|e| {
            panic!(
                "{e}\nengine: {}\nreference: {}",
                judgment.engine, judgment.reference
            )
        });
    });
}

/// The shrunk case of a second defect the placement law found, which
/// predates it: the shaper's fixed element order places a triple from a
/// correlated read's column (`$m rel0 $x`, `$m` drawn by a `from` with its
/// own cut) after every later `from`, so a `from` written after the triple,
/// whose sum can fail, runs on rows the triple removes and refuses (`E2`)
/// where the reference, evaluating the elements in written order, returns
/// no row. The law first found it as a triple closing on an `optional`'s
/// node, a text `T90` now refuses. The structural lowering replaces the
/// fixed order; until then the placement generator draws no `optional`
/// before the elements that can fail.
#[test]
#[ignore = "hunt: a from that can fail runs before a triple written ahead of it from a correlated read's column"]
fn a_from_that_can_fail_runs_after_the_triples_written_before_it() {
    let schema = r#"node A {
    k: String @key
    p0: I64?
}
edge Rel0: A -> A
"#;
    let seed = r#"{"type":"A","data":{"k":"a","p0":5}}
{"type":"A","data":{"k":"b","p0":0}}
{"edge":"Rel0","from":"a","to":"b","data":{}}"#;
    let source = r#"query q() {
  match { $n1: A, from { match { $m: A, $n1 rel0 $m } return { $m } order { $m.k } limit 5 }, $m rel0 $x, from { match { $y: A, $n1 rel0{1,2} $y } return { sum((10 / $y.p0)) as s } } }
  where $n1.k = "a"
  return { $n1.k as c0 }
}"#;
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap();
        let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
        session.load_jsonl(seed, LoadMode::Overwrite).await.unwrap();
        let judgment = omnigraph_gqt::judge_read_against_reference(
            &session,
            source,
            "q",
            &ParamMap::new(),
            false,
        )
        .await
        .unwrap();
        judgment.verdict.unwrap_or_else(|e| {
            panic!(
                "{e}\nengine: {}\nreference: {}",
                judgment.engine, judgment.reference
            )
        });
    });
}

/// The shrunk case of a defect law 1 found, which predates the registry
/// rows it was hunting: a zero query vector on a `cosine` field is refused
/// before the read runs (`vector_distance_cosine_refuses_zero_vectors.gqt`),
/// but the reference interpreter refuses it only where it scores a row, so
/// over a population with no row it returns no row where the engine
/// refuses.
#[test]
#[ignore = "hunt: the reference does not refuse a zero cosine query vector over an empty population"]
fn a_zero_cosine_query_vector_is_refused_over_an_empty_population() {
    let schema = r#"node A {
    k: String @key
    v: Vector(3, distance = "cosine")?
    t: String? @index(fulltext, analyzer = "english_v1")
}
"#;
    let source = r#"query q($qv: Vector(3)) {
  match { let $pop = { match { $d: A } where false return { $d } }, from rrf(nearest($pop, $d.v, $qv, k: 1), bm25($pop, $d.t, "graph"), k: 1) as $s }
  return { 0 as c0 }
}"#;
    let params: ParamMap = [(
        "qv".to_string(),
        omnigraph_compiler::query::value::Literal::List(vec![
            omnigraph_compiler::query::value::Literal::Float(0.0);
            3
        ]),
    )]
    .into_iter()
    .collect();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap();
        let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
        let judgment =
            omnigraph_gqt::judge_read_against_reference(&session, source, "q", &params, false)
                .await
                .unwrap();
        judgment.verdict.unwrap_or_else(|e| {
            panic!(
                "{e}\nengine: {}\nreference: {}",
                judgment.engine, judgment.reference
            )
        });
    });
}

/// The shrunk case of a defect law 1 found once the registry rows let an
/// uncorrelated grouped `from`'s `where` fail; it predates them, as a
/// division shows: the elements evaluate in written order, so a `from`
/// whose `where` refuses (`E2`) refuses the read even where a triple
/// written after it matches no edge, but the engine joins the `from` with
/// that empty triple and never runs it. The generator's grouped `from`
/// draws no `where` that can fail until the element order is the written
/// one.
#[test]
#[ignore = "hunt: an uncorrelated from that can fail is not run beside a later triple that matches nothing"]
fn an_uncorrelated_from_that_can_fail_runs_beside_an_empty_triple() {
    let schema = "node A {\n    k: String @key\n    t: String?\n}\nedge Rel0: A -> A\n";
    let seed = r#"{"type":"A","data":{"k":"a"}}"#;
    let source = r#"query q($z: I64) {
  match { from { match { $g: A } where ((1 / $z) > 0) return { $g.t as t, count($g) as n } }, $a: A, $a rel0 $b }
  return { $a.k as c0 }
}"#;
    let params: ParamMap = [(
        "z".to_string(),
        omnigraph_compiler::query::value::Literal::Integer(0),
    )]
    .into_iter()
    .collect();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap();
        let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
        session.load_jsonl(seed, LoadMode::Overwrite).await.unwrap();
        let judgment =
            omnigraph_gqt::judge_read_against_reference(&session, source, "q", &params, false)
                .await
                .unwrap();
        judgment.verdict.unwrap_or_else(|e| {
            panic!(
                "{e}\nengine: {}\nreference: {}",
                judgment.engine, judgment.reference
            )
        });
    });
}

/// The shrunk case of a defect law 1 found once `cast` let a block's
/// aggregate fail; it predates the registry rows, as a division shows: a
/// `let` bound to a block whose aggregate refuses (`E2`) refuses the read
/// even where a triple written after the `let` matches no edge, but the
/// engine runs the triple first and never evaluates the block. A scalar
/// `let` keeps its place. The generator draws no term that can fail in a
/// block a `let` binds until the element order is the written one.
#[test]
#[ignore = "hunt: a block a let binds that can fail does not run before a later triple that matches nothing"]
fn a_let_block_that_can_fail_runs_before_a_later_empty_triple() {
    let schema =
        "node A {\n    k: String @key\n    z: I64?\n}\nedge Rel0: A -> A\nedge Rel1: A -> A\n";
    let seed = r#"{"type":"A","data":{"k":"a","z":0}}
{"edge":"Rel1","from":"a","to":"a","data":{}}"#;
    let source = r#"query q() {
  match { $a: A, let $s = (sum((1 / $b.z)) { $b: A, $a rel1 $b }), $a rel0 $c }
  return { $s as c0 }
}"#;
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap();
        let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
        session.load_jsonl(seed, LoadMode::Overwrite).await.unwrap();
        let judgment = omnigraph_gqt::judge_read_against_reference(
            &session,
            source,
            "q",
            &ParamMap::new(),
            false,
        )
        .await
        .unwrap();
        judgment.verdict.unwrap_or_else(|e| {
            panic!(
                "{e}\nengine: {}\nreference: {}",
                judgment.engine, judgment.reference
            )
        });
    });
}

/// The shrunk case of a defect the placement law found once `cast` let a
/// block in `return` fail; it predates the registry rows, as a division
/// shows: the `return` items are computed on the rows the `where` keeps,
/// so a block there that refuses (`E2`) on a row the `where` removes
/// leaves no row to refuse, but the engine computes the block first. The
/// generator draws no term that can fail in a block in `return` until the
/// block runs above the `where`.
#[test]
#[ignore = "hunt: a block in return that can fail runs on rows the where removes"]
fn a_return_block_that_can_fail_runs_on_the_rows_the_where_keeps() {
    let schema = "node A {\n    k: String @key\n    z: I64?\n}\nedge Rel0: A -> A\n";
    let seed = r#"{"type":"A","data":{"k":"a","z":0}}
{"edge":"Rel0","from":"a","to":"a","data":{}}"#;
    let source = r#"query q() {
  match { $a: A }
  where ($a.k = "b")
  return { (count { $b: A, $a rel0 $b where ((1 / $a.z) > 0) }) as c0 }
}"#;
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap();
        let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
        session.load_jsonl(seed, LoadMode::Overwrite).await.unwrap();
        let judgment = omnigraph_gqt::judge_read_against_reference(
            &session,
            source,
            "q",
            &ParamMap::new(),
            false,
        )
        .await
        .unwrap();
        judgment.verdict.unwrap_or_else(|e| {
            panic!(
                "{e}\nengine: {}\nreference: {}",
                judgment.engine, judgment.reference
            )
        });
    });
}
