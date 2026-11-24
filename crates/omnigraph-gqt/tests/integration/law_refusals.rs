//! Law 5: the refusals. One table keyed by the code catalog,
//! `omnigraph_compiler::query::codes::ALL` (the GQ 3 RFC, section 5):
//!
//! - every code has at least one row whose source raises it first, or is
//!   listed in [`NOT_RAISED`] with the reason no query raises it;
//! - each row pins the start of the refusal's message and its printed fix,
//!   exactly, `None` where it prints none;
//! - where the refusal prints machine-applicable edits, one pass of them
//!   repairs the source to the row's intended text, compared as parse
//!   trees: the repair is the text the writer meant, not merely a text that
//!   parses. The same law runs over generated reads: a read `t` drawn by
//!   `omnigraph_compiler::arbitrary`, misprinted at one place
//!   (`arbitrary::misprint`), is repaired back to `t`;
//! - the printed fixes converge: applying every machine-applicable edit of
//!   one compile pass at once reparses, strictly shrinks the refusals, never
//!   undoes an earlier pass's edit and reaches a fixed point within three
//!   passes, over every row and every corpus step that expects a compile
//!   refusal;
//! - a refusal raised while the query runs (an `E` code) or by the planner
//!   (a `P` code) is raised by the engine, and the reference interpreter
//!   refuses a read where the engine does, with the same code
//!   ([`RUN_ROWS`]);
//! - no prefix of a parse refusal's source, and no such source with one
//!   character removed, panics the parser.

mod rows;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use omnigraph::Session;
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph_compiler::arbitrary::misprint::{Misprint, misprint};
use omnigraph_compiler::arbitrary::{
    Choices, GenRead, GenSchema, ReadOptions, SchemaOptions, choices,
};
use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::query::check::check_source_all;
use omnigraph_compiler::query::codes::ALL;
use omnigraph_compiler::query::syntax::parse::{apply_edits, parse_source, parse_source_all};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{Applicability, QueryDiagnostic, QueryDiagnosticKind, TextEdit};
use proptest::test_runner::TestCaseError;

use super::law_engine_reference::law_runner;
use rows::ROWS;

/// One refused source of the table.
pub(crate) struct Row {
    /// The code the source is refused with first.
    pub code: &'static str,
    /// What the source writes, in words.
    pub shape: &'static str,
    /// The schema the source is typed against.
    pub schema: &'static str,
    /// The source; [`NEST`] stands for an expression nested past the
    /// compiler's depth.
    pub source: &'static str,
    /// The start of the refusal's message; empty when the row pins the code
    /// and the fix alone.
    pub message: &'static str,
    /// The refusal's printed fix, exactly.
    pub fix: Option<&'static str>,
    /// The text one pass of the printed edits writes, machine-applicable or
    /// to review, compared with it as a parse tree; `None` when the pass
    /// prints no edit.
    pub intended: Option<&'static str>,
}

/// Where a row writes an expression nested one level past
/// `MAX_QUERY_NESTING`.
const NEST: &str = "⟨nest⟩";

impl Row {
    fn text(&self) -> String {
        if self.source.contains(NEST) {
            let depth = omnigraph_compiler::query::depth::MAX_QUERY_NESTING + 1;
            let nested = format!("{}true{}", "(".repeat(depth), ")".repeat(depth));
            self.source.replace(NEST, &nested)
        } else {
            self.source.to_string()
        }
    }
}

/// The codes no query raises, each with the reason.
const NOT_RAISED: &[(&str, &str)] = &[
    (
        "T51",
        "the result typer's lookup of a node type: the checker has resolved every type a read returns before the result is typed",
    ),
    (
        "P002",
        "the planner's refusal of an edge selection with no traversal allowance: a session always carries a finite `traversal_work_limit` (1,000,000 by default, never 0), so the engine never plans one without it",
    ),
    (
        "P003",
        "the planner's check of a captured traversal allowance: the settings layer refuses a `traversal_work_limit` outside 1..=i64::MAX before a plan is made, and a bound plan's replay carries the one it captured",
    ),
    (
        "P006",
        "the planner's assertion that a term arrived lowered: the checker refuses every term the planner does not lower (T86), so a query reaching it is a defect, never a refusal",
    ),
];

/// A refusal raised past the type checker, by the planner or while the
/// query runs: the engine raises it on a graph of [`RUN_SEED`]'s rows.
struct RunRow {
    code: &'static str,
    shape: &'static str,
    source: &'static str,
    /// Session settings the row runs under.
    settings: &'static [(&'static str, &'static str)],
    /// Whether the session pins traversal mode `csr`.
    csr: bool,
    kind: RunKind,
}

/// How a [`RunRow`] runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RunKind {
    /// A read the planner refuses: the reference has no plan to compare.
    Plan,
    /// A read refused while it runs, which the reference evaluates too.
    Read,
    /// A write, which the reference does not evaluate.
    Write,
}

/// The rows every [`RunRow`] runs over, under [`rows::COMMON`].
const RUN_SEED: &str = r#"{"type":"Person","data":{"handle":"ada","name":"Ada","age":2147483647}}
{"type":"Person","data":{"handle":"bob","name":"Bob","age":36}}
{"type":"Company","data":{"slug":"acme","name":"Acme"}}
{"edge":"WorksAt","from":"ada","to":"acme"}
{"edge":"WorksAt","from":"bob","to":"acme"}
{"edge":"Knows","from":"ada","to":"bob"}"#;

const RUN_ROWS: &[RunRow] = &[
    RunRow {
        code: "P004",
        shape: "an edge wildcard under traversal mode csr",
        source: "query q() { match { $a: Person, $b: Person, $a * $b } return { $a.name } }",
        settings: &[],
        csr: true,
        kind: RunKind::Plan,
    },
    RunRow {
        code: "T89",
        shape: "a declared plan budget above the session's ceiling",
        source: "query q() @budget(plan = 5000) { match { $p: Person } return { $p.name } }",
        settings: &[("plan_budget_limit", "1000")],
        csr: false,
        kind: RunKind::Plan,
    },
    RunRow {
        code: "E1",
        shape: "an integer sum past its type",
        source: "query q() { match { $p: Person } return { $p.age + $p.age as x } }",
        settings: &[],
        csr: false,
        kind: RunKind::Read,
    },
    RunRow {
        code: "E2",
        shape: "a stored value divided by zero",
        source: "query q() { match { $p: Person } return { $p.age / ($p.age - $p.age) as x } }",
        settings: &[],
        csr: false,
        kind: RunKind::Read,
    },
    RunRow {
        code: "E3",
        shape: "a float product past the finite range",
        source: "query q() { match { $p: Person } return { cast($p.age, F64) * 1.0e308 as x } }",
        settings: &[],
        csr: false,
        kind: RunKind::Read,
    },
    RunRow {
        code: "E4",
        shape: "one statement giving a property two values",
        source: "query w() { match { $p: Person, $p worksAt $c } update $c set { name: $p.name } }",
        settings: &[],
        csr: false,
        kind: RunKind::Write,
    },
    RunRow {
        code: "E5",
        shape: "a square root of a negative value",
        source: "query q() { match { $p: Person } return { sqrt(0.0 - cast($p.age, F64)) as x } }",
        settings: &[],
        csr: false,
        kind: RunKind::Read,
    },
    RunRow {
        code: "E6",
        shape: "a cast of a name to an integer",
        source: "query q() { match { $p: Person } return { cast($p.name, I64) as x } }",
        settings: &[],
        csr: false,
        kind: RunKind::Read,
    },
];

fn catalog(schema: &str) -> Catalog {
    build_catalog(&parse_schema(schema).expect("the row's schema parses"))
        .expect("the row's schema builds")
}

/// One catalog per schema of the table.
fn catalogs() -> HashMap<*const u8, Catalog> {
    let mut out = HashMap::new();
    for row in ROWS {
        out.entry(row.schema.as_ptr())
            .or_insert_with(|| catalog(row.schema));
    }
    out
}

/// The machine-applicable edits of one pass, or the two that overlap.
fn pass_edits(refusals: &[QueryDiagnostic]) -> Result<Vec<TextEdit>, String> {
    edits_of(refusals, false)
}

/// The edits of one pass, every suggestion's when `review` and the
/// machine-applicable ones' otherwise, or the two that overlap.
fn edits_of(refusals: &[QueryDiagnostic], review: bool) -> Result<Vec<TextEdit>, String> {
    let mut edits: Vec<TextEdit> = refusals
        .iter()
        .filter_map(|d| d.suggestion.as_deref())
        .filter(|s| review || s.applicability == Applicability::MachineApplicable)
        .flat_map(|s| s.edits.iter().cloned())
        .collect();
    edits.sort_by_key(|e| (e.start, e.end));
    edits.dedup();
    for pair in edits.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        let touching_inserts = a.start == a.end && b.start == b.end && a.start == b.start;
        if b.start < a.end || touching_inserts {
            return Err(format!("overlapping edits {a:?} and {b:?}"));
        }
    }
    Ok(edits)
}

/// Whether `repaired` parses to the tree `intended` does.
fn same_tree(repaired: &str, intended: &str) -> Result<(), String> {
    let got = parse_source(repaired)
        .map_err(|e| format!("the repair does not parse: {e}\n{repaired}"))?;
    let want = parse_source(intended)
        .map_err(|e| format!("the intended text does not parse: {e}\n{intended}"))?;
    if got != want {
        return Err(format!("repaired to\n{repaired}\nnot\n{intended}"));
    }
    Ok(())
}

/// Every code of the catalog has a row raising it, or a reason no query
/// does; every row's code is in the catalog.
#[test]
fn every_code_of_the_catalog_has_a_row_raising_it() {
    let catalog: BTreeSet<&str> = ALL.iter().map(|c| c.as_str()).collect();
    let mut raised: BTreeMap<&str, usize> = BTreeMap::new();
    for code in ROWS
        .iter()
        .map(|r| r.code)
        .chain(RUN_ROWS.iter().map(|r| r.code))
    {
        assert!(catalog.contains(code), "{code} is not in the catalog");
        *raised.entry(code).or_default() += 1;
    }
    for (code, reason) in NOT_RAISED {
        assert!(catalog.contains(code), "{code} is not in the catalog");
        assert!(!raised.contains_key(code), "{code} has a row: {reason}");
    }
    let missing: Vec<&str> = catalog
        .iter()
        .copied()
        .filter(|c| !raised.contains_key(c) && !NOT_RAISED.iter().any(|(n, _)| n == c))
        .collect();
    assert!(missing.is_empty(), "codes no row raises: {missing:?}");
}

/// Every row is refused first with its code, its message and its fix, and
/// one pass of its machine-applicable edits writes its intended text.
#[test]
fn every_row_is_refused_with_its_code_message_fix_and_repair() {
    let catalogs = catalogs();
    let mut failures = Vec::new();
    for row in ROWS {
        let cat = &catalogs[&row.schema.as_ptr()];
        let source = row.text();
        if let Err(failure) = holds(cat, row, &source) {
            failures.push(format!("{} {}: {failure}", row.code, row.shape));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} rows fail:\n{}",
        failures.len(),
        ROWS.len(),
        failures.join("\n\n")
    );
}

fn holds(cat: &Catalog, row: &Row, source: &str) -> Result<(), String> {
    let refusals = check_source_all(cat, source);
    let first = refusals.first().ok_or("accepted")?;
    if first.code.as_str() != row.code {
        return Err(format!("refused with {first}"));
    }
    if !first.message.starts_with(row.message) {
        return Err(format!("the message reads: {}", first.message));
    }
    if first.fix.as_deref() != row.fix {
        return Err(format!("the printed fix is {:?}", first.fix));
    }
    let edits = edits_of(&refusals, true)?;
    match (row.intended, edits.is_empty()) {
        (Some(intended), false) => same_tree(&apply_edits(source, &edits), intended),
        (Some(_), true) => Err("the pass prints no edit".into()),
        (None, false) => Err(format!(
            "the pass prints edits repairing it to\n{}\nstate the intended text",
            apply_edits(source, &edits)
        )),
        (None, true) => Ok(()),
    }
}

/// Prints every row's first refusal as one JSON line, to restate the
/// table after a deliberate message or fix change.
#[test]
#[ignore = "instrument: prints each row's refusal to restate the table"]
fn print_the_rows_refusals() {
    let catalogs = catalogs();
    for row in ROWS {
        let source = row.text();
        let refusals = check_source_all(&catalogs[&row.schema.as_ptr()], &source);
        let first = refusals.first();
        let repaired = edits_of(&refusals, true)
            .ok()
            .filter(|e| !e.is_empty())
            .map(|e| apply_edits(&source, &e));
        println!(
            "{}",
            serde_json::json!({
                "source": row.source,
                "code": first.map(|d| d.code.as_str()),
                "message": first.map(|d| d.message.clone()),
                "fix": first.and_then(|d| d.fix.clone()),
                "repaired": repaired,
            })
        );
    }
}

// ---------------------------------------------------------------- convergence

/// The passes within which the fixes reach a fixed point.
const PASSES: usize = 3;

/// Where each edit's replacement stands in the edited source, with the text
/// it replaced: what a later pass may not write back.
fn written(edits: &[TextEdit], source: &str) -> Vec<(usize, usize, String)> {
    let mut shift: isize = 0;
    let mut out = Vec::new();
    for e in edits {
        let start = (e.start as isize + shift) as usize;
        out.push((
            start,
            start + e.replacement.len(),
            source[e.start..e.end].to_string(),
        ));
        shift += e.replacement.len() as isize - (e.end - e.start) as isize;
    }
    out
}

/// The stage a pass reached: parsing, or name resolution and typing.
fn stage(refusals: &[QueryDiagnostic]) -> u8 {
    refusals
        .iter()
        .map(|d| match d.kind {
            QueryDiagnosticKind::Parse => 0,
            _ => 1,
        })
        .min()
        .unwrap_or(2)
}

fn render(refusals: &[QueryDiagnostic]) -> String {
    refusals
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The violation of the convergence rule for one source, if any; else the
/// passes that moved it and the text they repaired it to. A compile has two
/// refusing stages, the second reading what the first accepted: a pass
/// whose fixes clear its stage may reveal the next stage's refusals; every
/// other pass must leave fewer refusals than it found, and no pass may leave
/// an earlier stage's refusal than the one it found.
fn converge(cat: &Catalog, source: &str) -> Result<(usize, String), String> {
    let mut seen = vec![source.to_string()];
    let mut current = source.to_string();
    let mut refusals = check_source_all(cat, &current);
    let mut previous: Vec<(usize, usize, String)> = Vec::new();
    for pass in 0..=PASSES {
        let edits = pass_edits(&refusals).map_err(|e| format!("pass {pass}: {e}"))?;
        if refusals.is_empty() || edits.is_empty() {
            return Ok((pass, current));
        }
        if pass == PASSES {
            return Err(format!(
                "no fixed point within {PASSES} passes: {}",
                render(&refusals)
            ));
        }
        for e in &edits {
            if previous.iter().any(|(s, end, removed)| {
                e.start == *s && e.end == *end && e.replacement == *removed
            }) {
                return Err(format!("pass {pass} undoes an earlier edit: {e:?}"));
            }
        }
        let next = apply_edits(&current, &edits);
        if seen.contains(&next) {
            return Err(format!("pass {pass} returns to an earlier source"));
        }
        let after = check_source_all(cat, &next);
        let (was, now) = (stage(&refusals), stage(&after));
        if now < was {
            return Err(format!(
                "pass {pass} goes back a stage: {}\n{next}",
                render(&after)
            ));
        }
        if now == was && after.len() >= refusals.len() {
            return Err(format!(
                "pass {pass} does not shrink the refusals: {} -> {}\n{next}",
                render(&refusals),
                render(&after)
            ));
        }
        previous = written(&edits, &current);
        seen.push(next.clone());
        current = next;
        refusals = after;
    }
    unreachable!("the loop returns by its last pass")
}

fn cases_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("cases")
}

fn gqt_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            gqt_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "gqt") {
            out.push(path);
        }
    }
}

/// One corpus step expecting an error: the case, the schema and the source.
struct ErrorStep {
    origin: String,
    schema: String,
    source: String,
}

/// Every `--- query` or `--- mutate` section of the corpus whose
/// expectation (after an optional `--- params`) is `--- expect error`, in a
/// case with a schema.
fn error_steps() -> Vec<ErrorStep> {
    let mut files = Vec::new();
    gqt_files(&cases_root(), &mut files);
    files.sort();
    let mut out = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        let mut sections: Vec<(String, String)> = Vec::new();
        for line in text.lines() {
            if let Some(header) = line.strip_prefix("--- ") {
                sections.push((header.to_string(), String::new()));
            } else if let Some((_, body)) = sections.last_mut() {
                body.push_str(line);
                body.push('\n');
            }
        }
        let Some((_, schema)) = sections.iter().find(|(h, _)| h == "schema") else {
            continue;
        };
        let origin = file
            .strip_prefix(cases_root())
            .unwrap_or(&file)
            .display()
            .to_string();
        for (i, (header, body)) in sections.iter().enumerate() {
            let kind = header.split_whitespace().next().unwrap_or_default();
            if kind != "query" && kind != "mutate" {
                continue;
            }
            let mut next = sections[i + 1..].iter().map(|(h, _)| h.as_str());
            let expect = match next.next() {
                Some(h) if h.starts_with("params") => next.next(),
                other => other,
            };
            if expect.is_some_and(|h| h.starts_with("expect error")) {
                out.push(ErrorStep {
                    origin: format!("{origin} step {i}"),
                    schema: schema.clone(),
                    source: body.clone(),
                });
            }
        }
    }
    out
}

/// The text every refused corpus step that prints a machine-applicable fix
/// is repaired to, by its case and step: what its printed fixes intend. A
/// step that prints a fix and is not listed, or is listed and prints none,
/// fails the test. The table's rows state their own.
const INTENDED: &[(&str, &str)] = &[
    (
        "boolean_literal_refused_where_a_property_is_named_false.gqt step 3",
        r#"query shadowed_property() {
    match { $x: Flag } where false = false  delete $x
}"#,
    ),
    (
        "null_literal_takes_its_contexts_type.gqt step 18",
        r#"query equals_null() { match { $p: Person } where $p.city is null return { $p.handle } }"#,
    ),
    (
        "null_literal_takes_its_contexts_type.gqt step 20",
        r#"query differs_from_null() { match { $p: Person } where $p.city is not null return { $p.handle } }"#,
    ),
    (
        "issue_640_search_score_projection.gqt step 23",
        r#"query nearest_projected_without_retrieval($v: Vector(2)) {
    match { let $pop = { match { $p: Person } return { $p } }, from nearest($pop, $p.embedding, $v, k: 2) as $distance }
    return { $p.name, $distance as score }
    order { $p.name }
    limit 2
}"#,
    ),
    (
        "issue_659_edge_selection_contract.gqt step 33",
        r#"query single_name_group_is_not_edge_alternation() {
    match { $a: Person, $b: Person, $a knows $b }
    return { $a.name }
}"#,
    ),
    (
        "optional_keeps_rows_without_a_match.gqt step 12",
        "query friend_employers() { match { $p: Person, optional { $f: Person, $p knows $f, $c: Company, $f worksAt $c } } return { $p.handle, $f.handle, $c.slug } }",
    ),
    (
        "rank_key_asc_direction_refused.gqt step 6",
        r#"query bm25_asc() {
    match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.text, "needle") as $score }
    return { $d.slug }
    order { $score desc }
    limit 2
}"#,
    ),
    (
        "rank_key_asc_direction_refused.gqt step 8",
        r#"query rrf_asc($v: Vector(2), $w: Vector(2)) {
    match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $v, k: 2), nearest($all, $d.embedding, $w, k: 2), k: 60) as $score,  $d: Doc }
    return { $d.slug }
    order { $score desc }
    limit 2
}"#,
    ),
    (
        "traversal_from_an_optional_node.gqt step 3",
        r#"query q() {
  match { $n1: A, optional { $n2: A, $n1 rel0 $n2, $n5: A, $n2 $k4:rel0 $n5 where $n2.p0 is null } }
  return { $n1.k as a, $n2.k as b, $n5.k as c }
}"#,
    ),
    (
        "traversal_from_an_optional_node.gqt step 5",
        r#"query q() {
  match { $n1: A, optional { $n2: A, $n1 rel0 $n2, $n5: A, $n2 rel0{1,2} $n5 where $n2.p0 is null } }
  return { $n1.k as a, $n2.k as b, $n5.k as c }
}"#,
    ),
    (
        "traversal_from_an_optional_node.gqt step 7",
        r#"query q() {
  match { $n1: A, optional { $n2: A, $n1 rel0 $n2, $n5: A, $n5 rel0 $n2 where $n2.p0 is null } }
  return { $n1.k as a, $n2.k as b, $n5.k as c }
}"#,
    ),
    (
        "typed_search_options.gqt step 41",
        r#"query positional_window($q: Vector(2)) {
    match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 1) as $distance }
    return { $d.key }
}"#,
    ),
];

/// Every row and every corpus step the compiler refuses reaches a fixed
/// point within three passes; a corpus step whose fixes move it is listed in
/// [`INTENDED`] with the text they write, and the codes whose fixes are part
/// of the break's contract (the GQ 3 RFC, section 6, "Stored sources at
/// upgrade") each converge somewhere.
#[test]
fn every_refusal_reaches_a_fixed_point_within_three_passes() {
    let catalogs = catalogs();
    let mut violations = Vec::new();
    let mut contract: Vec<(&str, bool)> = ["T7", "T65", "T71", "Q008"]
        .into_iter()
        .map(|code| (code, false))
        .collect();
    let mut mark = |first: &[QueryDiagnostic]| {
        for d in first {
            let applicable = d
                .suggestion
                .as_deref()
                .is_some_and(|s| s.applicability == Applicability::MachineApplicable);
            if let Some(entry) = contract.iter_mut().find(|(c, _)| *c == d.code.as_str()) {
                entry.1 |= applicable;
            }
        }
    };
    for row in ROWS {
        let cat = &catalogs[&row.schema.as_ptr()];
        let source = row.text();
        match converge(cat, &source) {
            Ok((passes, _)) if passes > 0 => mark(&check_source_all(cat, &source)),
            Ok(_) => {}
            Err(violation) => violations.push(format!("{} {}: {violation}", row.code, row.shape)),
        }
    }
    let mut fixed = Vec::new();
    let mut refused = 0;
    for step in error_steps() {
        let Ok(schema) = parse_schema(&step.schema) else {
            continue;
        };
        let Ok(cat) = build_catalog(&schema) else {
            continue;
        };
        let first = check_source_all(&cat, &step.source);
        if first.is_empty() {
            continue;
        }
        refused += 1;
        let intended = INTENDED
            .iter()
            .find(|(origin, _)| *origin == step.origin)
            .map(|(_, text)| *text);
        match (converge(&cat, &step.source), intended) {
            (Ok((passes, repaired)), Some(intended)) if passes > 0 => {
                fixed.push(step.origin.clone());
                mark(&first);
                if repaired.trim_end() != intended {
                    violations.push(format!(
                        "{}: the fixes repair the source to\n{repaired}\nnot the intended\n{intended}",
                        step.origin
                    ));
                }
            }
            (Ok(_), Some(_)) => violations.push(format!(
                "{}: its intended repair is listed, but its refusal prints no machine-applicable edit",
                step.origin
            )),
            (Ok((passes, repaired)), None) if passes > 0 => violations.push(format!(
                "{}: its fixes repair the source to\n{repaired}\nlist the intended text in INTENDED, or move the step into the refusal table",
                step.origin
            )),
            (Ok(_), None) => {}
            (Err(violation), _) => {
                violations.push(format!("{}: {violation}\n{}", step.origin, step.source))
            }
        }
    }
    let stale: Vec<&str> = INTENDED
        .iter()
        .map(|(origin, _)| *origin)
        .filter(|origin| !fixed.iter().any(|f| f == origin))
        .collect();
    assert!(
        stale.is_empty(),
        "INTENDED lists steps no fix repaired: {stale:?}"
    );
    let untested: Vec<&str> = contract
        .iter()
        .filter(|(_, seen)| !seen)
        .map(|(code, _)| *code)
        .collect();
    assert!(
        untested.is_empty(),
        "no refusal of {untested:?} prints a machine-applicable fix that converges"
    );
    assert!(
        violations.is_empty(),
        "{} of {} refused sources break the fix-convergence rule:\n{}",
        violations.len(),
        ROWS.len() + refused,
        violations.join("\n")
    );
}

/// The block contexts a pattern can be written in: each `⟨⟩` is the
/// block's content. A `where` operand (`exists`, `count` and every
/// aggregate's block, negated or compared), or an element of the `match`
/// written the way edition 2 wrote one (a block compared among the elements,
/// `not { }`, `optional { }`).
const BLOCK_CONTEXTS: &[&str] = &[
    "match { $p: Person } where exists { ⟨⟩ }",
    "match { $p: Person } where not exists { ⟨⟩ }",
    "match { $p: Person } where count { ⟨⟩ } != 0",
    "match { $p: Person } where count($p.age) { ⟨⟩ } > 0",
    "match { $p: Person } where sum($p.age) { ⟨⟩ } > 1",
    "match { $p: Person } where avg($p.age) { ⟨⟩ } > 1",
    "match { $p: Person } where min($p.age) { ⟨⟩ } > 1",
    "match { $p: Person } where max($p.age) { ⟨⟩ } > 1",
    "match { $p: Person } where collect($p.age) { ⟨⟩ } = [1]",
    "match { $p: Person, exists { ⟨⟩ } }",
    "match { $p: Person  count { ⟨⟩ } != 0 }",
    "match { $p: Person, not { ⟨⟩ } }",
    "match { $p: Person, optional { ⟨⟩ } }",
    "match { $p: Person, exists { ⟨⟩ }, $q: Company }",
];

/// The contents a block holds in edition-2 text and in edition 3, refused
/// or not: filters alone, a re-mention of the outer binding, triples that
/// introduce a name, a block's own `where`, nested blocks.
const BLOCK_CONTENTS: &[&str] = &[
    "$p.age > 3",
    "$p.age > 3  $p.name = \"a\"",
    "where $p.age > 3",
    "$p.age > 3 where $p.name = \"a\"",
    "$p: Person { name: \"a\" }",
    "$p: Person where $p.age > 3",
    "$p: Person",
    "$p knows $q",
    "$p knows $q  $q.age > 3",
    "$q: Person, $p knows $q where $q.age > 3",
    "$p worksAt $q",
    "not { $p knows $q }",
    "exists { $p.age > 3 }",
    "count { $p.age > 3 } > 0",
];

/// The printed fixes reach one fixed point however two refusals of a block
/// meet: every block context of [`BLOCK_CONTEXTS`] holding every content of
/// [`BLOCK_CONTENTS`], alone or followed by another, either compiles, stops
/// at a refusal with no fix, or converges, with or without the fixes for
/// review; and `lint --fix` on what it wrote changes nothing. Two rules whose
/// fixes undo each other, one moving a filter into a block's `where` and the
/// other deleting a `where` that has no element before it, fail here. The
/// rows' stricter rule, that every pass leaves fewer refusals
/// ([`converge`]), does not hold of a compound: the pass that repairs an
/// element list can reveal a refusal of a block inside it, in the same
/// stage.
#[test]
fn printed_fixes_reach_one_fixed_point_in_every_block_context() {
    use omnigraph_compiler::{FixOptions, FixStatus, fix_source};
    let cat = catalog(rows::CHECKED);
    let mut contents: Vec<String> = BLOCK_CONTENTS.iter().map(|c| c.to_string()).collect();
    for a in BLOCK_CONTENTS {
        for b in BLOCK_CONTENTS {
            contents.push(format!("{a}  {b}"));
        }
    }
    let mut violations = Vec::new();
    let mut fixed = 0;
    for context in BLOCK_CONTEXTS {
        for content in &contents {
            let source = format!(
                "query q() {{ {} return {{ $p.name }} }}",
                context.replace("⟨⟩", content)
            );
            for review in [false, true] {
                let out = fix_source(&cat, &source, FixOptions { review });
                match out.report.status {
                    FixStatus::DoesNotConverge => {
                        violations.push(format!("{source}\n  does not converge: {:?}", out.report))
                    }
                    FixStatus::Fixed => {
                        fixed += 1;
                        let text = out.fixed.unwrap();
                        let again = fix_source(&cat, &text, FixOptions { review });
                        if again.report.status != FixStatus::Unchanged {
                            violations.push(format!(
                                "{source}\n  fixed to\n{text}\n  which the fixes change again: {:?}",
                                again.report
                            ));
                        }
                    }
                    FixStatus::Unchanged | FixStatus::Unfixed => {}
                }
            }
        }
    }
    assert!(fixed > 0, "no source was fixed");
    assert!(
        violations.is_empty(),
        "{} sources break the fixed-point rule:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

/// A fix does not reveal a refusal the pass did not report: `T84` (`$q`
/// bound nowhere in the inline read) leaves the inline read typed as its fix
/// would, so the same pass also reports `T68` (the inline read returns `$p`
/// into a scope that holds it), and one pass of fixes converges.
#[test]
fn a_fix_does_not_reveal_a_refusal_of_its_own_declaration() {
    let source = "query q() {\n  match { $p: Person, from { match { $p knows $q } return { $p } order { $q.age } } }\n  return { $p.name }\n}";
    let cat = catalog(rows::CHECKED);
    let codes: Vec<&str> = check_source_all(&cat, source)
        .iter()
        .map(|refusal| refusal.code.as_str())
        .collect();
    assert_eq!(codes, ["T84", "T68"], "one pass reports both");
    converge(&cat, source).unwrap();
}

/// No source reaches a slice or an `unwrap` in the parser: every prefix of
/// every parse refusal's source, and every such source with one character
/// removed, is refused or accepted, never a panic.
#[test]
fn no_cut_or_deletion_of_a_row_panics_the_parser() {
    for row in ROWS
        .iter()
        .filter(|r| r.code.starts_with('Q') && !r.source.contains(NEST))
    {
        let source = row.source;
        let mut variants: Vec<String> = source
            .char_indices()
            .map(|(i, _)| source[..i].to_string())
            .collect();
        variants.extend(source.char_indices().map(|(i, c)| {
            let mut s = source.to_string();
            s.replace_range(i..i + c.len_utf8(), "");
            s
        }));
        for variant in variants {
            let outcome = std::panic::catch_unwind(|| {
                let _ = parse_source(&variant);
                let _ = parse_source_all(&variant);
            });
            assert!(
                outcome.is_ok(),
                "{} {}: the parser panicked on\n{variant}",
                row.code,
                row.shape
            );
        }
    }
}

// ------------------------------------------------------------- run-time rows

/// A graph of [`RUN_SEED`]'s rows under [`rows::COMMON`].
async fn run_graph() -> (tempfile::TempDir, Arc<Omnigraph>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(dir.path().to_str().unwrap(), rows::COMMON)
        .await
        .unwrap();
    let db = Arc::new(db);
    Session::from_defaults(Arc::clone(&db), SessionSettings::default())
        .load_jsonl(RUN_SEED, LoadMode::Overwrite)
        .await
        .unwrap();
    (dir, db)
}

/// Every planner and run-time row is refused by the engine with its code,
/// and a read the same way by the reference interpreter.
#[test]
fn the_engine_and_the_reference_raise_every_run_time_code() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (_dir, db) = run_graph().await;
        let params = omnigraph_compiler::ParamMap::new();
        let mut failures = Vec::new();
        for row in RUN_ROWS {
            let mut settings = SessionSettings::default();
            for (name, value) in row.settings {
                settings = settings.with(name, value).unwrap();
            }
            if row.csr {
                settings = settings.with_traversal(omnigraph::settings::Traversal::Csr);
            }
            let session = Session::from_defaults(Arc::clone(&db), settings);
            let target = omnigraph::db::ReadTarget::branch("main");
            let refusal = match row.kind {
                RunKind::Write => session.mutate("main", row.source, "w", &params).await.err(),
                _ => session.query(target, row.source, "q", &params).await.err(),
            };
            let Some(refusal) = refusal else {
                failures.push(format!("{} {}: the engine answered", row.code, row.shape));
                continue;
            };
            let code = refusal.diagnostic().map(|d| d.code.as_str());
            if code != Some(row.code) {
                failures.push(format!(
                    "{} {}: the engine refused otherwise: {refusal}",
                    row.code, row.shape
                ));
                continue;
            }
            if row.kind == RunKind::Read {
                match omnigraph_gqt::judge_read_against_reference(
                    &session, row.source, "q", &params, false,
                )
                .await
                {
                    Ok(judgment) => {
                        if let Err(e) = judgment.verdict {
                            failures.push(format!("{} {}: {e}", row.code, row.shape));
                        }
                    }
                    Err(e) => failures.push(format!("{} {}: {e}", row.code, row.shape)),
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    });
}

// ------------------------------------------------------------ misprinted reads

/// The per-pull-request count of generated reads; nightly runs `NIGHTLY`.
const CASES: u32 = 2_000;
const NIGHTLY: u32 = 50_000;

static APPLIED: std::sync::Mutex<BTreeMap<Misprint, usize>> =
    std::sync::Mutex::new(BTreeMap::new());

/// `parse(apply_fixes(misprint(t))) == parse(t)` for a generated read `t`
/// misprinted at one place: the misprint is refused with its code and one
/// pass of the printed edits restores the read the writer meant.
fn misprint_holds(s: &[u32], q: &[u32], form: usize, pick: usize) -> Result<(), String> {
    let schema = GenSchema::generate(&mut Choices::new(s), SchemaOptions::default());
    let read = GenRead::generate(&mut Choices::new(q), &schema, ReadOptions::default());
    let form = Misprint::ALL[form % Misprint::ALL.len()];
    let Some(misprinted) = misprint(&read.source, form, pick) else {
        return Ok(());
    };
    let cat = build_catalog(&parse_schema(&schema.text()).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let refusals = check_source_all(&cat, &misprinted);
    let context = || {
        format!(
            "{form:?}\n--- schema\n{}\n--- read\n{}\n--- misprinted\n{misprinted}",
            schema.text(),
            read.source
        )
    };
    let Some(first) = refusals.first() else {
        return Err(format!("the misprint is accepted\n{}", context()));
    };
    if first.code.as_str() != form.code() {
        return Err(format!(
            "refused with {first}, not {}\n{}",
            form.code(),
            context()
        ));
    }
    let edits = pass_edits(&refusals).map_err(|e| format!("{e}\n{}", context()))?;
    if edits.is_empty() {
        return Err(format!(
            "no machine-applicable edit: {first}\n{}",
            context()
        ));
    }
    same_tree(&apply_edits(&misprinted, &edits), &read.source)
        .map_err(|e| format!("{e}\n{}", context()))?;
    *APPLIED.lock().unwrap().entry(form).or_default() += 1;
    Ok(())
}

fn run_misprint_law(cases: u32) {
    let mut runner = law_runner(cases, "tests/regressions/law_refusals.txt");
    let strategy = (choices(64), choices(256), 0..Misprint::ALL.len(), 0usize..8);
    runner
        .run(&strategy, |(s, q, form, pick)| {
            misprint_holds(&s, &q, form, pick).map_err(TestCaseError::fail)
        })
        .unwrap();
    let applied = APPLIED.lock().unwrap();
    eprintln!("misprints applied: {applied:?}");
    let unapplied: Vec<Misprint> = Misprint::ALL
        .into_iter()
        .filter(|m| !applied.contains_key(m))
        .collect();
    assert!(
        unapplied.is_empty(),
        "no generated read took the misprints {unapplied:?}"
    );
}

#[test]
fn every_misprinted_generated_read_is_repaired_to_the_read() {
    run_misprint_law(CASES);
}

#[test]
#[ignore = "nightly: 50,000 misprinted generated reads; the gate runs 2,000"]
fn every_misprinted_generated_read_is_repaired_to_the_read_at_nightly_breadth() {
    run_misprint_law(NIGHTLY);
}
