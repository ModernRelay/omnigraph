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
//! `order`, `limit` (a literal or a parameter), and the rankings
//! `nearest`, `distance`, `bm25`, `rrf` (with `weights`).
//!
//! The law: the engine's answer equals the reference's, as rows under the
//! result's order rule (ordered when the read has an `order`, as bags
//! otherwise) or as the same refusal, under every traversal mode (`csr`,
//! `indexed` and the planner's choice) and again after every declared index
//! is built. Writes are not generated: the reference evaluates reads only.
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
    judgment.verdict.map(|()| rows).map_err(|e| {
        format!(
            "{label}: {e}\n{}\n--- rewritten query\n{source}\n--- engine\n{}\n--- reference\n{}",
            sample.describe(),
            judgment.engine,
            judgment.reference
        )
    })
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

/// Whether a conjunct may hold a term that can fail: arithmetic or an
/// aggregate that can overflow.
fn can_fail(conjunct: &str) -> bool {
    [" / ", " % ", " * ", " + ", " - ", "(-", "sum(", "avg("]
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
    eprintln!("{nonempty} of {total} generated reads returned a row");
    assert!(
        nonempty * 5 >= total,
        "only {nonempty} of {total} generated reads returned a row"
    );
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
/// `limit 1`; the engine's `LimitExec` and `SortExec` execute nothing below
/// a zero limit, so the item never runs. The fix drains the input of a zero
/// limit whose subtree holds a term that can fail; until then the generator
/// draws no `limit 0`.
#[test]
#[ignore = "hunt: limit 0 skips a return item that can fail, which the reference evaluates"]
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
