//! The printed fixes converge (docs/rfcs/2026-11-07-gq-3.md, section 5):
//! for every step of the corpus that expects an error and whose source the
//! compiler refuses, applying every machine-applicable suggestion of one
//! compile pass at once reparses, strictly shrinks the refusals, never undoes
//! an earlier pass's edit, and reaches a fixed point within three passes.
//!
//! One pass is [`check_source_all`]: every independent refusal of the
//! source, each with its span. The suggestions of one pass may not overlap.
//! "Strictly shrinks" counts the refusals of the stage the pass reached. A
//! compile has three stages, each reading what the one before accepted: the
//! grammar (a refused source holds one refusal), the typing rules, and the
//! lowering admission (`T86`), which reads a typed declaration. A pass whose
//! fixes clear its stage may reveal the next stage's refusals; every other
//! pass must leave fewer refusals than it found, and no pass may leave an
//! earlier stage's refusal than the one it found.

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

/// The stage a pass reached: the grammar, the typing rules, or the
/// lowering admission.
fn stage(refusals: &[QueryDiagnostic]) -> u8 {
    refusals
        .iter()
        .map(|d| match (d.kind, d.code.as_str()) {
            (QueryDiagnosticKind::Parse, _) => 0,
            (_, "T86") => 2,
            _ => 1,
        })
        .min()
        .unwrap_or(3)
}

fn render(refusals: &[QueryDiagnostic]) -> String {
    refusals
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The violation of the convergence rule for one source, if any.
fn converge(cat: &Catalog, source: &str) -> Result<usize, String> {
    let mut seen = vec![source.to_string()];
    let mut current = source.to_string();
    let mut refusals = check_source_all(cat, &current);
    let mut previous: Vec<(usize, usize, String)> = Vec::new();
    for pass in 0..=PASSES {
        let edits = pass_edits(&refusals).map_err(|e| format!("pass {pass}: {e}"))?;
        if refusals.is_empty() || edits.is_empty() {
            return Ok(pass);
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

#[test]
fn every_expect_error_case_reaches_a_fixed_point_within_three_passes() {
    let steps = error_steps();
    assert!(steps.len() > 300, "the corpus shrank: {}", steps.len());
    let mut refused = 0;
    let mut violations = Vec::new();
    for step in &steps {
        let Some(cat) = catalog(&step.schema) else {
            continue;
        };
        if check_source_all(&cat, &step.source).is_empty() {
            continue;
        }
        refused += 1;
        if let Err(violation) = converge(&cat, &step.source) {
            violations.push(format!("{}: {violation}\n{}", step.origin, step.source));
        }
    }
    assert!(refused > 100, "few compile refusals: {refused}");
    assert!(
        violations.is_empty(),
        "{} of {refused} refused sources break the fix-convergence rule:\n{}",
        violations.len(),
        violations.join("\n")
    );
}
