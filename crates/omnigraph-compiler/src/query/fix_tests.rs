use super::*;
use crate::build_catalog;
use crate::query::codes::{Q014, Q015};
use crate::query::diagnostic::Suggestion;
use crate::schema::parser::parse_schema;

fn catalog() -> Catalog {
    let schema = "node Person { name: String @key\n age: I32? }\nedge Knows: Person -> Person\n";
    build_catalog(&parse_schema(schema).unwrap()).unwrap()
}

fn codes(report: &FixReport) -> Vec<(&str, usize)> {
    report
        .applied
        .iter()
        .map(|(code, n)| (code.as_str(), *n))
        .collect()
}

/// An edition-2 source respelled over two passes: the refusals' fixes
/// first (one pass, several codes), then the canonical form the compiling
/// text names; the result compiles and names no canonical form.
#[test]
fn printed_fixes_converge_then_canonical_forms_apply() {
    let catalog = catalog();
    let source = "mutation m($n: String) { insert Person { name: $n } }\nquery q() { match { $p: Person  $p.age > 30 } return { $p.name  $p.age } order { $p.age asc } }";
    let out = fix_source(&catalog, source, FixOptions::default());
    assert_eq!(out.report.status, FixStatus::Fixed, "{:?}", out.report);
    assert_eq!(out.report.passes, 2);
    assert_eq!(
        codes(&out.report),
        [("L208", 1), ("Q009", 1), ("Q016", 1), ("Q017", 1)]
    );
    assert!(out.report.applied_for_review.is_empty());
    let fixed = out.fixed.unwrap();
    assert_eq!(
        fixed,
        "query m($n: String) { insert Person { name: $n } }\nquery q() { match { $p: Person } where $p.age > 30 return { $p.name,  $p.age } order { $p.age } }"
    );
    assert!(check_source_all(&catalog, &fixed).is_empty());
    // A fixed text is a fixed point.
    let again = fix_source(&catalog, &fixed, FixOptions::default());
    assert_eq!(again.report.status, FixStatus::Unchanged);
    assert_eq!((again.fixed, again.report.passes), (None, 0));
}

/// A refusal with no fix writes nothing, also when an earlier pass's fix
/// revealed it; the report lists it, positioned in the text of the pass
/// that found it, and keeps what the earlier passes applied.
#[test]
fn a_refusal_without_a_fix_writes_nothing() {
    let catalog = catalog();
    let out = fix_source(
        &catalog,
        "query g() { match { $p: Person } return { $p.nope } }",
        FixOptions::default(),
    );
    assert_eq!(out.report.status, FixStatus::Unfixed);
    assert_eq!(out.fixed, None);
    assert_eq!(out.report.stopped_at_pass, Some(0));
    assert_eq!(out.report.unfixed.len(), 1);
    assert_eq!(out.report.unfixed[0].code.as_str(), "T6");
    assert!(out.report.failed());

    let source = "mutation m() { insert Person { name: \"a\" } }\nquery g() { match { $p: Person } return { $p.nope } }";
    let out = fix_source(&catalog, source, FixOptions::default());
    assert_eq!(out.report.status, FixStatus::Unfixed);
    assert_eq!(out.fixed, None);
    assert_eq!(out.report.stopped_at_pass, Some(1));
    assert_eq!(codes(&out.report), [("Q009", 1)]);
    let unfixed = &out.report.unfixed[0];
    assert_eq!(unfixed.code.as_str(), "T6");
    // `mutation` became `query`: two bytes shorter than the original.
    let original = source.find("$p.nope").unwrap();
    assert_eq!(unfixed.position.unwrap().byte as usize, original - 3);
}

/// A fix printed for review stops the loop unless review fixes are asked
/// for; then it applies and is counted apart.
#[test]
fn a_fix_for_review_applies_only_when_asked() {
    let catalog = catalog();
    let source = "query q() { match { $p: Person } return { $p.name } offset 3 }";
    let out = fix_source(&catalog, source, FixOptions::default());
    assert_eq!(out.report.status, FixStatus::Unfixed);
    let unfixed = &out.report.unfixed[0];
    assert_eq!(unfixed.code, Q015);
    assert_eq!(
        unfixed.suggestion.as_ref().unwrap().applicability,
        Applicability::NeedsReview,
        "the report keeps the fix it did not apply"
    );

    let out = fix_source(&catalog, source, FixOptions { review: true });
    assert_eq!(out.report.status, FixStatus::Fixed);
    assert_eq!(
        out.fixed.as_deref(),
        Some("query q() { match { $p: Person } return { $p.name } }")
    );
    assert_eq!(codes(&out.report), [("Q015", 1)]);
    assert_eq!(out.report.applied_for_review.get("Q015"), Some(&1));
}

/// Canonical forms apply only to a compiling text, and the rows-equal
/// rewrites (`L204`, `L205`) never: they are the author's choice.
#[test]
fn canonical_forms_apply_once_nothing_is_refused() {
    let catalog = catalog();
    let out = fix_source(
        &catalog,
        "query q() { match { $p: Person } where $p.name == \"A\" return { $p.name } order { $p.age asc } }",
        FixOptions::default(),
    );
    // Pass 1 fixes the refusal alone, though the `asc` was there already.
    assert_eq!(out.report.passes, 2);
    assert_eq!(codes(&out.report), [("L208", 1), ("Q014", 1)]);

    for chosen in [
        "query q() { match { $p: Person } where count { $f: Person, $p knows $f } >= 1 return { $p.name } }",
        "query q() { match { $p: Person } where count { $f: Person, $p knows $f } = 0 return { $p.name } }",
    ] {
        assert!(check_source_all(&catalog, chosen).is_empty(), "{chosen}");
        let lint = crate::query::canonical::source_canonical_forms(chosen);
        assert!(
            lint.iter().any(|(_, f)| !f.same_ir),
            "lint names it: {chosen}"
        );
        let out = fix_source(&catalog, chosen, FixOptions::default());
        assert_eq!(out.report.status, FixStatus::Unchanged, "{chosen}");
        assert_eq!(out.fixed, None);
    }
}

fn edit(start: usize, end: usize, replacement: &str) -> TextEdit {
    TextEdit {
        start,
        end,
        replacement: replacement.to_string(),
    }
}

fn candidate(code: &str, edits: Vec<TextEdit>) -> Candidate {
    Candidate {
        code: code.to_string(),
        review: false,
        edits,
    }
}

/// Of two suggestions whose edits overlap, the first in source order
/// applies whole and the other not at all; two inserts at one point
/// conflict, an insert where a replaced range starts or ends does not, and
/// an edit two suggestions share applies once.
#[test]
fn a_pass_applies_whole_suggestions_that_do_not_overlap() {
    let text = "abcdefgh";
    let (out, applied) = apply_candidates(
        text,
        vec![
            // Second in source order; its second edit overlaps the first's.
            candidate("B", vec![edit(6, 7, "G"), edit(2, 4, "")]),
            candidate("A", vec![edit(1, 3, "BC")]),
            candidate("C", vec![edit(5, 5, "+"), edit(5, 6, "F")]),
            candidate("D", vec![edit(5, 5, "*")]),
            candidate("E", vec![edit(6, 6, "!"), edit(1, 3, "BC")]),
            // Outside the text: never applied.
            candidate("F", vec![edit(7, 99, "")]),
        ],
    );
    let codes: Vec<&str> = applied.iter().map(|(c, _)| c.as_str()).collect();
    assert_eq!(codes, ["A", "E", "C"]);
    assert_eq!(out, "aBCde+F!gh");
}

/// The suggestion a pass left out applies in the next pass, which compiles
/// the text the earlier pass wrote.
#[test]
fn an_overlapping_fix_applies_in_a_later_pass() {
    let refusal = |code, start, end, replacement: &str| {
        QueryDiagnostic::parse(code, "m", None).with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits: vec![edit(start, end, replacement)],
        })
    };
    // `ab` → `x` and `bc` → `y` overlap; once `ab` is `x`, `xc` → `z`.
    let out = fix_loop(
        "abc",
        FixOptions::default(),
        |text| match text {
            "abc" => vec![refusal(Q014, 0, 2, "x"), refusal(Q015, 1, 3, "y")],
            "xc" => vec![refusal(Q015, 0, 2, "z")],
            _ => Vec::new(),
        },
        |_| Vec::new(),
    );
    assert_eq!(out.fixed.as_deref(), Some("z"));
    assert_eq!(out.report.passes, 2);
    assert_eq!(codes(&out.report), [("Q014", 1), ("Q015", 1)]);
}

/// Fixes that write a text seen before, or never settle, do not converge,
/// and nothing is written.
#[test]
fn fixes_that_loop_or_never_settle_do_not_converge() {
    let swap = |text: &str| {
        let to = if text == "a" { "b" } else { "a" };
        vec![
            QueryDiagnostic::parse(Q014, "m", None).with_suggestion(Suggestion {
                applicability: Applicability::MachineApplicable,
                edits: vec![edit(0, 1, to)],
            }),
        ]
    };
    let out = fix_loop("a", FixOptions::default(), swap, |_| Vec::new());
    assert_eq!(out.report.status, FixStatus::DoesNotConverge);
    assert_eq!(out.fixed, None);
    assert_eq!(out.report.stopped_at_pass, Some(1));
    assert_eq!(
        out.report.unfixed.len(),
        1,
        "the standing refusal is listed"
    );

    // A canonical form whose fix keeps growing the text.
    let grow = |text: &str| vec![candidate("L208", vec![edit(text.len(), text.len(), "+")])];
    let out = fix_loop("a", FixOptions::default(), |_| Vec::new(), grow);
    assert_eq!(out.report.status, FixStatus::DoesNotConverge);
    assert_eq!(out.report.passes, MAX_FIX_PASSES);
    assert_eq!(out.report.stopped_at_pass, Some(MAX_FIX_PASSES));
    assert_eq!(out.fixed, None);
}

/// A refusal whose suggestion has no edits is a refusal with no fix.
#[test]
fn a_suggestion_without_edits_is_no_fix() {
    let out = fix_loop(
        "a",
        FixOptions { review: true },
        |_| {
            vec![
                QueryDiagnostic::parse(Q014, "m", None).with_suggestion(Suggestion {
                    applicability: Applicability::MachineApplicable,
                    edits: Vec::new(),
                }),
            ]
        },
        |_| Vec::new(),
    );
    assert_eq!(out.report.status, FixStatus::Unfixed);
    assert_eq!(out.report.unfixed.len(), 1);
}
