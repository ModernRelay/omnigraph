//! Law 6: search answers do not depend on index state, the edit relation is
//! the RFC's, and fusion is the RFC's formula under its tie rule (the GQ 3
//! RFC, section 4, "The lexical query", "Vector retrieval", "Fusion" and
//! "Populations and laws", law 5: index state never changes an exact
//! answer).
//!
//! The generator (`omnigraph_compiler::arbitrary::search`) draws one node
//! type declaring every index kind (a BTREE on an `I64`, a BITMAP on an
//! enum, a LABEL_LIST on a list, a BTREE and an NGRAM on a String, a
//! full-text index under each analyzer profile, a vector index under each
//! distance), its rows in three batches, and reads: filters on each indexed
//! column, `contains` over a column and over its `lower`, `match_terms` in
//! both modes with every edit budget, and the rankings `distance`,
//! `nearest`, `bm25` and `rrf` of two to four arms, with and without
//! `weights`, over a population with or without a `where`.
//!
//! - (a) Every read answers as the reference interpreter does, rows or the
//!   same refusal, in three states holding the same rows: no index built;
//!   every index built over every row; and every index built over the first
//!   batch before rows of new keys are added and earlier rows are replaced
//!   whole, so the indexes cover part of the table and some covered rows
//!   have moved to fragments no index covers. The indexed states must probe
//!   each scalar family (BTREE, BITMAP, LABEL_LIST, NGRAM) somewhere in the
//!   run, by the plans' `access` entries. Every read is exact, `nearest`
//!   included: the engine builds its vector index with one IVF partition,
//!   which every search reads, so the law holds `nearest` to the exact
//!   window, which the RFC's approximate contract contains. No query reads
//!   a full-text index (the exact lexical scan answers), so the full-text
//!   reads hold the RFC's `matches(indexed) = matches(unindexed) =
//!   matches(partially indexed)` by construction, and the law keeps them
//!   in every state for when one does.
//! - (b) A `match_terms` predicate alone under `standard_v1` or
//!   `standard_folded_v1` keeps exactly the rows a dynamic-programming edit
//!   distance over the analyzed terms admits (insertions, deletions and
//!   substitutions of Unicode scalar values, one each, so a transposition
//!   costs two); the engine's banded distance is held to the same table in
//!   its own unit law (`lexical_scan::tests`).
//! - (c) An `rrf` answers alike under every permutation of its arms, their
//!   weights moved with them: the same targets, each score equal up to the
//!   rounding of a sum taken in another order, and each permutation equal
//!   to the reference's. Its scores are the formula over each arm's own
//!   answer, ranked in the arm's direction with equal scores broken by the
//!   smaller key, a `nearest` arm's window being the `k` smallest
//!   distances under that order; and an `rrf` cut by `order { $s desc }`
//!   and `limit` keeps the largest scores, equal scores broken by the
//!   smaller key.
//!
//! Seeds are deterministic; a failing case's seed is written to
//! `tests/regressions/law_search.txt`, which every run replays first.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use omnigraph::Session;
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph_compiler::ParamMap;
use omnigraph_compiler::arbitrary::search::{Arm, Lexical, SearchCase, SearchRead, SearchRow};
use omnigraph_compiler::arbitrary::{Choices, choices};
use proptest::test_runner::TestCaseError;
use serde_json::Value;

use super::law_engine_reference::law_runner;

/// The per-pull-request case count; the nightly test runs `NIGHTLY`.
const CASES: u32 = 40;
const NIGHTLY: u32 = 2_000;
/// Reads drawn per case, each judged in every state.
const READS: usize = 4;

#[derive(Clone, Copy, Debug)]
enum State {
    NoIndex,
    Indexed,
    Partial,
}

const STATES: [State; 3] = [State::NoIndex, State::Indexed, State::Partial];

fn jsonl(rows: &[SearchRow]) -> String {
    rows.iter()
        .map(SearchRow::jsonl)
        .collect::<Vec<_>>()
        .join("\n")
}

fn describe(case: &SearchCase) -> String {
    format!(
        "--- schema\n{}\n--- base\n{}\n--- tail\n{}\n--- updates\n{}",
        case.schema(),
        jsonl(&case.base),
        jsonl(&case.tail),
        jsonl(&case.updates)
    )
}

/// A graph of the case's rows in `state`.
async fn graph(case: &SearchCase, state: State) -> Result<(tempfile::TempDir, Session), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let uri = dir.path().to_str().unwrap().to_string();
    let db = Omnigraph::init(&uri, &case.schema())
        .await
        .map_err(|e| format!("init failed: {e}"))?;
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    let load = |rows: String, mode: LoadMode| {
        let session = &session;
        async move {
            if rows.is_empty() {
                return Ok(());
            }
            session
                .load_jsonl(&rows, mode)
                .await
                .map(|_| ())
                .map_err(|e| format!("{state:?}: load failed: {e}"))
        }
    };
    let index = || async {
        session
            .ensure_indices_as(None)
            .await
            .map(|_| ())
            .map_err(|e| format!("{state:?}: ensure_indices failed: {e}"))
    };
    match state {
        State::NoIndex => load(jsonl(&case.rows()), LoadMode::Overwrite).await?,
        State::Indexed => {
            load(jsonl(&case.rows()), LoadMode::Overwrite).await?;
            index().await?;
        }
        State::Partial => {
            load(jsonl(&case.base), LoadMode::Overwrite).await?;
            index().await?;
            load(jsonl(&case.tail), LoadMode::Insert).await?;
            load(jsonl(&case.updates), LoadMode::Replace).await?;
        }
    }
    Ok((dir, session))
}

fn params(read: &SearchRead) -> ParamMap {
    read.params.iter().cloned().collect()
}

/// The engine's rows of `source`, judged against the reference.
async fn judge(
    session: &Session,
    case: &SearchCase,
    read: &SearchRead,
    source: &str,
    ordered: bool,
    label: &str,
) -> Result<Vec<Value>, String> {
    let judgment =
        omnigraph_gqt::judge_read_against_reference(session, source, "q", &params(read), ordered)
            .await
            .map_err(|e| format!("{label}: {e}\n--- query\n{source}\n{}", describe(case)))?;
    judgment.verdict.map_err(|e| {
        format!(
            "{label}: {e}\n--- query\n{source}\n--- params\n{:?}\n{}\n--- engine\n{}\n--- reference\n{}",
            read.params,
            describe(case),
            judgment.engine,
            judgment.reference
        )
    })?;
    match serde_json::from_str::<Value>(&judgment.engine) {
        Ok(Value::Array(rows)) => Ok(rows),
        _ => Ok(Vec::new()),
    }
}

// ---------------------------------------------------------------- (b)

/// The edit distance of `a` to `b` over Unicode scalar values, the full
/// dynamic-programming table: the oracle.
pub(crate) fn edit_distance(a: &str, b: &str) -> usize {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut table = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in table.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in table[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let substitute = table[i - 1][j - 1] + usize::from(a[i - 1] != b[j - 1]);
            table[i][j] = substitute.min(table[i - 1][j] + 1).min(table[i][j - 1] + 1);
        }
    }
    table[a.len()][b.len()]
}

/// The terms `profile` analyzes `text` to, for the profiles without a
/// stemmer: split at every character that is not alphanumeric, lower case,
/// and under `standard_folded_v1` the ASCII folding of the generator's
/// letters. `None` for a profile the oracle does not model.
fn analyzed(profile: &str, text: &str) -> Option<Vec<String>> {
    let folded = match profile {
        "standard_v1" => false,
        "standard_folded_v1" => true,
        _ => return None,
    };
    Some(
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .map(|w| {
                let lower = w.to_lowercase();
                if folded {
                    lower.replace('é', "e").replace('ß', "ss")
                } else {
                    lower
                }
            })
            .collect(),
    )
}

/// The keys a `match_terms` alone keeps, by the oracle.
fn lexical_oracle(case: &SearchCase, lexical: &Lexical) -> Option<BTreeSet<String>> {
    let query: BTreeSet<String> = analyzed(case.profile, &lexical.text)?.into_iter().collect();
    let mut keys = BTreeSet::new();
    for row in case.rows() {
        let Some(text) = &row.t else { continue };
        let doc = analyzed(case.profile, text)?;
        if doc.is_empty() || query.is_empty() {
            continue;
        }
        let hit = |q: &String| doc.iter().any(|t| edit_distance(q, t) <= lexical.max_edits);
        let matched = if lexical.all {
            query.iter().all(hit)
        } else {
            query.iter().any(hit)
        };
        if matched {
            keys.insert(row.key);
        }
    }
    Some(keys)
}

fn keys(rows: &[Value]) -> BTreeSet<String> {
    rows.iter()
        .filter_map(|r| r["x.k"].as_str().map(str::to_string))
        .collect()
}

// ---------------------------------------------------------------- (c)

/// The arm orders the law runs: every permutation of up to three arms; of
/// four, the rotations and their reversals.
fn permutations(n: usize) -> Vec<Vec<usize>> {
    if n <= 3 {
        let mut out = Vec::new();
        let mut current: Vec<usize> = (0..n).collect();
        heap(n, &mut current, &mut out);
        out
    } else {
        (0..n)
            .flat_map(|r| {
                let rotated: Vec<usize> = (0..n).map(|i| (i + r) % n).collect();
                let reversed: Vec<usize> = rotated.iter().rev().copied().collect();
                [rotated, reversed]
            })
            .collect()
    }
}

fn heap(k: usize, a: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
    if k <= 1 {
        out.push(a.clone());
        return;
    }
    for i in 0..k {
        heap(k - 1, a, out);
        let j = if k.is_multiple_of(2) { i } else { 0 };
        a.swap(j, k - 1);
    }
}

/// Each target's score.
fn scores(rows: &[Value]) -> Result<BTreeMap<String, Option<f64>>, String> {
    let mut out = BTreeMap::new();
    for row in rows {
        let key = row["x.k"]
            .as_str()
            .ok_or_else(|| format!("a row without its key: {row}"))?;
        if out.insert(key.to_string(), row["s"].as_f64()).is_some() {
            return Err(format!("two rows of one target: {key}"));
        }
    }
    Ok(out)
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-12 * a.abs().max(b.abs()).max(1.0)
}

/// The targets of an arm in rank order: its score in its direction, equal
/// scores by the smaller key.
fn ranked(scored: &BTreeMap<String, Option<f64>>, descending: bool) -> Option<Vec<String>> {
    let mut rows: Vec<(f64, &String)> = Vec::new();
    for (key, score) in scored {
        rows.push(((*score)?, key));
    }
    rows.sort_by(|a, b| {
        let by = if descending {
            b.0.total_cmp(&a.0)
        } else {
            a.0.total_cmp(&b.0)
        };
        by.then_with(|| a.1.cmp(b.1))
    });
    Some(rows.into_iter().map(|(_, key)| key.clone()).collect())
}

/// Reads whose `rrf` the formula oracle checked, and those it skipped
/// because an arm scored a null.
static FORMULA_CHECKED: AtomicUsize = AtomicUsize::new(0);
static FORMULA_SKIPPED: AtomicUsize = AtomicUsize::new(0);
static LEXICAL_CHECKED: AtomicUsize = AtomicUsize::new(0);
static NONEMPTY: AtomicUsize = AtomicUsize::new(0);
/// The access paths the plans of each state took, by the explain
/// document's `access` words.
static ACCESS: std::sync::Mutex<BTreeMap<String, usize>> = std::sync::Mutex::new(BTreeMap::new());

async fn note_access(session: &Session, read: &SearchRead, state: State) {
    let doc = session
        .explain_query(
            omnigraph::db::ReadTarget::branch("main"),
            &read.source,
            "q",
            &params(read),
        )
        .await;
    if std::env::var_os("OMNIGRAPH_LAW_TRACE").is_some() {
        eprintln!("EXPLAIN {state:?}\n{}\n{doc:?}", read.source);
    }
    let Ok(doc) = doc else { return };
    fn walk(value: &Value, state: State, out: &mut BTreeMap<String, usize>) {
        match value {
            Value::Object(map) => {
                if let Some(access) = map.get("access").and_then(Value::as_str) {
                    let index = map
                        .get("index")
                        .and_then(Value::as_array)
                        .map(|names| {
                            names
                                .iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join("+")
                        })
                        .unwrap_or_default();
                    *out.entry(format!("{state:?} {access} {index}"))
                        .or_insert(0) += 1;
                }
                map.values().for_each(|v| walk(v, state, out));
            }
            Value::Array(items) => items.iter().for_each(|v| walk(v, state, out)),
            _ => {}
        }
    }
    walk(&doc["physical_plan"], state, &mut ACCESS.lock().unwrap());
}
static TOTAL: AtomicUsize = AtomicUsize::new(0);

async fn check_rrf(
    session: &Session,
    case: &SearchCase,
    read: &SearchRead,
    rows: &[Value],
) -> Result<(), String> {
    let rrf = read.rrf.as_ref().expect("an rrf read");
    let fused = scores(rows)?;
    // Every permutation of the arms answers as the reference does and
    // fuses the same targets to the same scores.
    for order in permutations(rrf.arms.len()) {
        let source = read.rrf_source(&order).expect("an rrf read");
        let permuted = judge(session, case, read, &source, false, "rrf arms permuted").await?;
        let permuted = scores(&permuted)?;
        let same = permuted.len() == fused.len()
            && permuted.iter().all(|(key, s)| {
                fused.get(key).is_some_and(|f| match (f, s) {
                    (Some(a), Some(b)) => close(*a, *b),
                    (a, b) => a == b,
                })
            });
        if !same {
            return Err(format!(
                "rrf arms permuted {order:?} fuse to {permuted:?}, written order to {fused:?}\n--- query\n{}\n--- permuted\n{source}\n{}",
                read.source,
                describe(case)
            ));
        }
    }
    // The formula over each arm's own answer, under the tie rule.
    let mut ranks: Vec<Vec<String>> = Vec::new();
    for (i, arm) in rrf.arms.iter().enumerate() {
        let window = match arm {
            Arm::Nearest { param, k } => {
                let all = Arm::Distance {
                    param: param.clone(),
                };
                let source = read
                    .arm_source(i)
                    .expect("an rrf read")
                    .replace(&arm.call(), &all.call());
                Some((source, *k))
            }
            _ => None,
        };
        let source = match &window {
            Some((source, _)) => source.clone(),
            None => read.arm_source(i).expect("an rrf read"),
        };
        let arm_rows = judge(session, case, read, &source, false, &format!("rrf arm {i}")).await?;
        let Some(mut order) = ranked(&scores(&arm_rows)?, arm.descending()) else {
            FORMULA_SKIPPED.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };
        if let Some((_, k)) = window {
            order.truncate(k);
            // The `nearest` arm alone selects that window.
            let alone = read.arm_source(i).expect("an rrf read");
            let selected = judge(session, case, read, &alone, false, "nearest arm").await?;
            let selected = keys(&selected);
            let expected: BTreeSet<String> = order.iter().cloned().collect();
            if selected != expected {
                return Err(format!(
                    "nearest(k: {k}) selected {selected:?}, the exact window under the tie rule is {expected:?}\n--- query\n{alone}\n{}",
                    describe(case)
                ));
            }
        }
        ranks.push(order);
    }
    let mut oracle: Vec<(String, f64)> = Vec::new();
    for (i, order) in ranks.iter().enumerate() {
        let w = rrf.weights.as_ref().map_or(1.0, |w| w[i] as f64);
        for (rank, key) in order.iter().enumerate() {
            let add = w / (rrf.k as f64 + (rank + 1) as f64);
            match oracle.iter_mut().find(|(k, _)| k == key) {
                Some((_, s)) => *s += add,
                None => oracle.push((key.clone(), add)),
            }
        }
    }
    let oracle: BTreeMap<String, Option<f64>> =
        oracle.into_iter().map(|(k, s)| (k, Some(s))).collect();
    if oracle != fused {
        return Err(format!(
            "rrf fused {fused:?}, the formula over its arms under the tie rule gives {oracle:?}\n--- query\n{}\n--- params\n{:?}\n{}",
            read.source,
            read.params,
            describe(case)
        ));
    }
    // A cut keeps the largest scores, equal scores by the smaller key.
    let limit = 1 + oracle.len() / 2;
    let cut_source = read.source.replace(
        "  return { $x.k, $s }\n",
        &format!("  return {{ $x.k, $s }}\n  order {{ $s desc }}\n  limit {limit}\n"),
    );
    let cut = judge(session, case, read, &cut_source, true, "rrf cut").await?;
    let got: Vec<String> = cut
        .iter()
        .filter_map(|r| r["x.k"].as_str().map(str::to_string))
        .collect();
    let mut want: Vec<(f64, String)> = oracle
        .iter()
        .map(|(k, s)| (s.expect("scored"), k.clone()))
        .collect();
    want.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let want: Vec<String> = want.into_iter().take(limit).map(|(_, k)| k).collect();
    if got != want {
        return Err(format!(
            "the cut rrf kept {got:?}, the largest scores under the tie rule are {want:?}\n--- query\n{cut_source}\n{}",
            describe(case)
        ));
    }
    FORMULA_CHECKED.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

async fn check(case: &SearchCase) -> Result<(), String> {
    for state in STATES {
        let (_dir, session) = graph(case, state).await?;
        for read in &case.reads {
            TOTAL.fetch_add(1, Ordering::Relaxed);
            let label = format!("{state:?}");
            let rows = judge(&session, case, read, &read.source, read.ordered, &label).await?;
            note_access(&session, read, state).await;
            if !rows.is_empty() {
                NONEMPTY.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(lexical) = &read.lexical
                && let Some(want) = lexical_oracle(case, lexical)
            {
                LEXICAL_CHECKED.fetch_add(1, Ordering::Relaxed);
                let got = keys(&rows);
                if got != want {
                    return Err(format!(
                        "{label}: match_terms kept {got:?}, the edit oracle admits {want:?}\n--- query\n{}\n{}",
                        read.source,
                        describe(case)
                    ));
                }
            }
            if read.rrf.is_some() && matches!(state, State::NoIndex | State::Partial) {
                check_rrf(&session, case, read, &rows).await?;
            }
        }
    }
    Ok(())
}

fn run_law(cases: u32) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = law_runner(cases, "tests/regressions/law_search.txt");
    let result = runner.run(&choices(1024), |c| {
        let case = SearchCase::generate(&mut Choices::new(&c), READS);
        rt.block_on(check(&case)).map_err(TestCaseError::fail)
    });
    if let Err(e) = result {
        panic!("{e}");
    }
    let total = TOTAL.load(Ordering::Relaxed);
    let nonempty = NONEMPTY.load(Ordering::Relaxed);
    let lexical = LEXICAL_CHECKED.load(Ordering::Relaxed);
    let formula = FORMULA_CHECKED.load(Ordering::Relaxed);
    let skipped = FORMULA_SKIPPED.load(Ordering::Relaxed);
    eprintln!(
        "{nonempty} of {total} judged reads returned a row; {lexical} match_terms reads met the \
         edit oracle; {formula} rrf reads met the formula oracle, {skipped} skipped for a null arm score"
    );
    let access = ACCESS.lock().unwrap();
    eprintln!("access paths: {access:?}");
    assert!(nonempty * 2 >= total, "too few reads returned a row");
    for state in ["Indexed", "Partial"] {
        for index in ["n_idx", "s_bitmap", "tags_label_list", "name_ngram"] {
            assert!(
                access
                    .keys()
                    .any(|k| k.starts_with(&format!("{state} index_probe")) && k.contains(index)),
                "no read probed {index} in the {state} state: {access:?}"
            );
        }
    }
    assert!(lexical > 0 && formula > 0, "an oracle never ran");
}

#[test]
fn search_answers_hold_under_every_index_state_and_the_rrf_formula() {
    run_law(CASES);
}

#[test]
#[ignore = "nightly: law 6 over generated search cases at its nightly count"]
fn search_answers_hold_under_every_index_state_and_the_rrf_formula_at_nightly_breadth() {
    run_law(NIGHTLY);
}

#[test]
fn the_edit_oracle_counts_a_transposition_twice() {
    assert_eq!(edit_distance("ab", "ba"), 2);
    assert_eq!(edit_distance("é", "e"), 1);
    assert_eq!(edit_distance("", "中文"), 2);
    assert_eq!(edit_distance("kitten", "sitting"), 3);
}
