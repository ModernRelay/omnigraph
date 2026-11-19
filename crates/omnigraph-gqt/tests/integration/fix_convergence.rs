//! The printed fixes converge (the GQ 3 RFC, section 5):
//! for every step of the corpus that expects an error and whose source the
//! compiler refuses, applying every machine-applicable suggestion of one
//! compile pass at once reparses, strictly shrinks the refusals, never undoes
//! an earlier pass's edit, and reaches a fixed point within three passes.
//!
//! One pass is [`check_source_all`]: every independent refusal of the
//! source, each with its span. The suggestions of one pass may not overlap.
//! "Strictly shrinks" counts the refusals of the stage the pass reached. A
//! compile has two refusing stages, the second reading what the first
//! accepted: parsing, then name resolution and typing, where the lowering
//! admission (`T86`) and a repeated declaration name (`T83`) belong; lowering
//! refuses nothing. A pass whose fixes clear its stage may reveal the next
//! stage's refusals; every other pass must leave fewer refusals than it
//! found, and no pass may leave an earlier stage's refusal than the one it
//! found.
//!
//! A step whose refusal prints a machine-applicable fix is listed in
//! [`INTENDED`] with the text its fixes are meant to write, and the repaired
//! text must be that text: a fix that converges somewhere else, or a listed
//! step whose first pass prints no edit, fails.

use std::path::{Path, PathBuf};

use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::query::check::check_source_all;
use omnigraph_compiler::query::syntax::parse::apply_edits;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{Applicability, QueryDiagnostic, QueryDiagnosticKind, TextEdit};

/// The passes within which the fixes reach a fixed point.
const PASSES: usize = 3;

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

/// One step expecting an error: the case, the schema and the source.
struct ErrorStep {
    origin: String,
    schema: String,
    source: String,
}

/// Every `--- query` or `--- mutate` section whose expectation (after an
/// optional `--- params`) is `--- expect error`, in a case with a schema.
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

fn catalog(schema: &str) -> Option<Catalog> {
    build_catalog(&parse_schema(schema).ok()?).ok()
}

/// The machine-applicable edits of one pass, or the two that overlap.
fn pass_edits(refusals: &[QueryDiagnostic]) -> Result<Vec<TextEdit>, String> {
    let mut edits: Vec<TextEdit> = refusals
        .iter()
        .filter_map(|d| d.suggestion.as_deref())
        .filter(|s| s.applicability == Applicability::MachineApplicable)
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
/// passes that moved it and the text they repaired it to.
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

/// The text every refused corpus step that prints a machine-applicable fix
/// is repaired to, by its case and step: what its printed fixes intend,
/// reviewed when the step was added. A step that prints a fix and is not
/// listed, or is listed and prints none, fails the test.
const INTENDED: &[(&str, &str)] = &[
    (
        "boolean_literal_refused_where_a_property_is_named_false.gqt step 3",
        r#"query shadowed_property() {
    match { $x: Flag } where false = false  delete $x
}"#,
    ),
    (
        "diagnostics/q002_missing_parameter_list.gqt step 3",
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        "diagnostics/q003_refused_setting.gqt step 5",
        "query q() { match { $p: Person } return { $p.name } }",
    ),
    (
        "diagnostics/q004_load_mode_append_and_merge.gqt step 4",
        r#"load csv "people.csv" into node:Person mode insert header"#,
    ),
    (
        "diagnostics/q004_load_mode_append_and_merge.gqt step 6",
        r#"load csv "people.csv" into node:Person mode replace header"#,
    ),
    (
        "diagnostics/q007_arrow_traversal.gqt step 3",
        r#"query q() { match { $a: Person, $a knows $b } return { $b.name } }"#,
    ),
    (
        "diagnostics/q008_where_on_a_statement.gqt step 3",
        r#"query w($h: String) { match { $x: Person } where $x.handle = $h  delete $x }"#,
    ),
    (
        "diagnostics/q009_mutation_keyword.gqt step 3",
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }"#,
    ),
    (
        "diagnostics/q010_comparison_in_binding_braces.gqt step 3",
        r#"query q() { match { $d: Doc } where $d.year >= 2025 return { $d.title } }"#,
    ),
    (
        "diagnostics/q011_as_commit_on_a_read.gqt step 3",
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        "diagnostics/q012_annotation_with_empty_parentheses.gqt step 3",
        r#"query q() @deprecated { match { $p: Person } return { $p.name } }"#,
    ),
    (
        "diagnostics/q013_element_after_the_last_statement.gqt step 3",
        r#"query w($h: String) { match { $p: Person, $q: Person } where $p.handle = $h delete $p }"#,
    ),
    (
        "diagnostics/q016_missing_comma.gqt step 3",
        r#"query q() { match { $p: Person, $p knows $q } return { $q.name } }"#,
    ),
    (
        "diagnostics/q017_filter_inside_match.gqt step 3",
        r#"query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"#,
    ),
    (
        "diagnostics/q019_element_keyword_call.gqt step 3",
        r#"query q() { match { $p: Person } where $p.city is not null return { $p.name } }"#,
    ),
    (
        "diagnostics/q019_element_keyword_call.gqt step 7",
        r#"query q() { match { $p: Person } where $p.age > 30 and $p.city is null return { $p.name } }"#,
    ),
    (
        "diagnostics/q020_match_without_braces.gqt step 3",
        r#"query q() { match { $p: Person }  return { $p.name } }"#,
    ),
    (
        "diagnostics/q021_annotation_argument_with_colon.gqt step 3",
        r#"query q() @budget(plan = 10) { match { $p: Person } return { $p.name } }"#,
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
        "diagnostics/t11_edge_endpoint_written_from_and_to.gqt step 3",
        r#"query befriend() { insert Knows { @src: "ada", @dst: "ada" } }"#,
    ),
    (
        "diagnostics/t65_ranking_in_order.gqt step 3",
        r#"query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: 3) as $distance } return { $d.title } order { $distance } limit 3 }"#,
    ),
    (
        "diagnostics/t68_block_binds_an_outer_name.gqt step 3",
        r#"query not_bob() { match { $p: Person } where not ($p.name = "Bob") return { $p.name } }"#,
    ),
    (
        "diagnostics/t68_block_binds_an_outer_name.gqt step 5",
        r#"query knows_someone() { match { $p: Person } where exists { $q: Person, $p knows $q } return { $p.name } }"#,
    ),
    (
        "diagnostics/t68_block_binds_an_outer_name.gqt step 7",
        r#"query employers() { match { $p: Person, optional { $c: Company, $p worksAt $c } } return { $p.name, $c.name } }"#,
    ),
    (
        "diagnostics/t6_fuzzy_is_not_a_registry_row.gqt step 3",
        r#"query q() { match { $d: Doc } where match_terms($d.title, terms("graf", mode: any, max_edits: 2)) return { $d.slug } }"#,
    ),
    (
        "diagnostics/t6_match_text_is_not_a_registry_row.gqt step 3",
        r#"query q() { match { $d: Doc } where match_terms($d.title, terms("graph", mode: any)) return { $d.slug } }"#,
    ),
    (
        "diagnostics/t6_search_is_not_a_registry_row.gqt step 3",
        r#"query q() { match { $d: Doc } where match_terms($d.title, terms("graph", mode: any)) return { $d.slug } }"#,
    ),
    (
        "diagnostics/t71_positional_option.gqt step 3",
        r#"query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: 3) as $distance } return { $d.title } }"#,
    ),
    (
        "diagnostics/t7_infix_contains.gqt step 3",
        r#"query q() { match { $d: Doc } where "rust" in $d.tags return { $d.title } }"#,
    ),
    (
        "diagnostics/t82_triple_written_against_its_edge_direction.gqt step 3",
        r#"query staff($slug: String) { match { $c: Company, $p worksAt $c } where $c.slug = $slug return { $p.name } }"#,
    ),
    (
        "diagnostics/t82_triple_written_against_its_edge_direction.gqt step 5",
        r#"query ties() { match { $c: Company } where exists { $p: Person, $p $k:(worksAt | founded) $c } return { $c.name } }"#,
    ),
    (
        "diagnostics/t82_triple_written_against_its_edge_direction.gqt step 7",
        r#"query everyone() { match { $c: Company, $p: Person, $p * $c } return { $p.name } }"#,
    ),
    (
        "diagnostics/t84_triple_names_a_name_no_binding_introduces.gqt step 3",
        r#"query loners() { match { $p: Person } where not exists { $q: Person, $p knows $q } return { $p.name } }"#,
    ),
    (
        "diagnostics/t84_triple_names_a_name_no_binding_introduces.gqt step 5",
        r#"query headcounts() { match { from { match { $p: Person, $c: Company, $p worksAt $c } return { $c, count($p) as n } } } return { $c.name, $n } }"#,
    ),
    (
        "diagnostics/t84_triple_names_a_name_no_binding_introduces.gqt step 7",
        r#"query employers() { match { $p: Person, optional { $c: Company, $p worksAt $c } } return { $p.name, $c.name } }"#,
    ),
    (
        "diagnostics/t85_ranked_from_names_its_score.gqt step 3",
        r#"query near($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $distance } return { $d.slug, $distance } order { $distance } }"#,
    ),
    (
        "diagnostics/t85_ranked_from_names_its_score.gqt step 5",
        r#"query docs() { match { let $docs = { match { $d: Doc } return { $d } }, from $docs } return { $d.slug } }"#,
    ),
    (
        "diagnostics/t85_ranked_from_names_its_score.gqt step 7",
        r#"query near_by_name($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, let $near = nearest($pop, $d.embedding, $q, k: 2), from $near as $distance } return { $d.slug } }"#,
    ),
    (
        "diagnostics/t85_ranked_from_names_its_score.gqt step 9",
        r#"query wrapped($q: Vector(3)) { match { from { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $dist } return { $d, $dist } } } return { $d.slug } }"#,
    ),
    (
        "diagnostics/t90_triple_reads_a_reference_an_optional_introduced.gqt step 3",
        "query friends_employers() { match { $p: Person, optional { $f: Person, $p knows $f, $c: Company, $f worksAt $c } } return { $p.name, $c.name } }",
    ),
    (
        "diagnostics/t90_triple_reads_a_reference_an_optional_introduced.gqt step 5",
        "query admirers_of_staff() { match { $c: Company, optional { $p: Person, $p worksAt $c, $q: Person, $q knows $p } } return { $c.name, $q.name } }",
    ),
    (
        "diagnostics/t92_upsert_or_replace_without_a_key.gqt step 3",
        r#"query befriend() { insert Knows { @src: "ada", @dst: "ada" } }"#,
    ),
    (
        "diagnostics/t92_upsert_or_replace_without_a_key.gqt step 5",
        r#"query befriend_again() { insert Knows { @src: "ada", @dst: "ada" } }"#,
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

#[test]
fn every_expect_error_case_reaches_a_fixed_point_within_three_passes() {
    let steps = error_steps();
    // Every `cases/diagnostics/` case, one per code the parser and the type
    // checker raise, holds a step the compiler refuses.
    let mut diagnostics: Vec<PathBuf> = Vec::new();
    gqt_files(&cases_root().join("diagnostics"), &mut diagnostics);
    assert!(!diagnostics.is_empty());
    let mut refused_cases: Vec<String> = Vec::new();
    let mut fixed: Vec<String> = Vec::new();
    // The codes whose fixes are part of the break's contract (the GQ 3 RFC,
    // section 6, "Stored sources at upgrade"), each fixed somewhere.
    let mut contract: Vec<(&str, bool)> = ["T7", "T65", "T71", "Q008"]
        .into_iter()
        .map(|code| (code, false))
        .collect();
    let mut violations = Vec::new();
    for step in &steps {
        let Some(cat) = catalog(&step.schema) else {
            continue;
        };
        let first = check_source_all(&cat, &step.source);
        if first.is_empty() {
            continue;
        }
        refused_cases.push(step.origin.clone());
        let intended = INTENDED
            .iter()
            .find(|(origin, _)| *origin == step.origin)
            .map(|(_, text)| *text);
        match (converge(&cat, &step.source), intended) {
            (Ok((passes, repaired)), Some(intended)) if passes > 0 => {
                fixed.push(step.origin.clone());
                if repaired.trim_end() != intended {
                    violations.push(format!(
                        "{}: the fixes repair the source to\n{repaired}\nnot the intended\n{intended}",
                        step.origin
                    ));
                }
                for diagnostic in &first {
                    let applicable = diagnostic
                        .suggestion
                        .as_deref()
                        .is_some_and(|s| s.applicability == Applicability::MachineApplicable);
                    if let Some(entry) = contract
                        .iter_mut()
                        .find(|(code, _)| *code == diagnostic.code.as_str())
                    {
                        entry.1 |= applicable;
                    }
                }
            }
            (Ok((_, _)), Some(_)) => violations.push(format!(
                "{}: its intended repair is listed, but its refusal prints no machine-applicable edit\n{}",
                step.origin, step.source
            )),
            (Ok((passes, repaired)), None) if passes > 0 => violations.push(format!(
                "{}: its fixes repair the source to\n{repaired}\nlist the intended text in INTENDED",
                step.origin
            )),
            (Ok(_), None) => {}
            (Err(violation), _) => {
                violations.push(format!("{}: {violation}\n{}", step.origin, step.source))
            }
        }
    }
    for file in &diagnostics {
        let name = file
            .strip_prefix(cases_root())
            .unwrap_or(file)
            .display()
            .to_string();
        assert!(
            refused_cases
                .iter()
                .any(|origin| origin.starts_with(&format!("{name} "))),
            "{name}: no step the compiler refuses"
        );
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
        "no corpus refusal of {untested:?} prints a machine-applicable fix that converges"
    );
    assert!(
        violations.is_empty(),
        "{} of {} refused sources break the fix-convergence rule:\n{}",
        violations.len(),
        refused_cases.len(),
        violations.join("\n")
    );
}
