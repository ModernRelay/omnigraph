//! The diagnostics protocol (docs/rfcs/2026-11-07-gq-3.md, section 5): a
//! compile returns every independent refusal in one pass, in source order,
//! each with its span, and a refusal of an unknown name lists up to five
//! nearest names of the right kind with their types.

use super::catalog;
use crate::query::check::{check_source, check_source_all};
use crate::query::diagnostic::QueryDiagnostic;

/// The refusals of one pass over `src`.
#[track_caller]
fn pass(src: &str) -> Vec<QueryDiagnostic> {
    check_source_all(&catalog(), src)
}

/// The text a refusal's span covers.
#[track_caller]
fn spanned<'s>(src: &'s str, d: &QueryDiagnostic) -> &'s str {
    let start = d
        .position
        .unwrap_or_else(|| panic!("no position: {d}"))
        .byte as usize;
    let end = d.end.unwrap_or_else(|| panic!("no end: {d}")).byte as usize;
    &src[start..end]
}

/// Each refusal's code and the text its span covers.
#[track_caller]
fn codes_at(src: &str) -> Vec<(String, String)> {
    pass(src)
        .iter()
        .map(|d| (d.code.as_str().to_string(), spanned(src, d).to_string()))
        .collect()
}

/// A refusal in an element, two in separate conjuncts of `where` and one in
/// a return item are independent: one pass returns the four, in source
/// order, each at its construct. The return item naming the refused
/// element's binding depends on it and is not checked, nor is the order key,
/// which reads the return items.
#[test]
fn every_independent_refusal_is_returned_in_one_pass_with_its_span() {
    let src = r#"query q() { match { $p: Persn, $c: Company } where $c.nmae = "x" and $c.slug = 3 return { $p, $c.nme } order { $c.name } }"#;
    let got = codes_at(src);
    assert_eq!(
        got,
        [
            ("T1", "$p: Persn"),
            ("T6", r#"$c.nmae = "x""#),
            ("T7", "$c.slug = 3"),
            ("T6", "$c.nme"),
        ]
        .map(|(c, t)| (c.to_string(), t.to_string())),
        "{src}"
    );
    // The `Result` door keeps the first and carries the others.
    let first = check_source(&catalog(), src).unwrap()[0]
        .1
        .clone()
        .expect_err("refused");
    assert_eq!(first.code.as_str(), "T1");
    assert_eq!(first.further().len(), 3);
    assert_eq!(first.all(), pass(src));
    assert!(first.to_string().starts_with("type error: T1: "), "{first}");
}

/// A refused element's names are what its dependents read: a triple over a
/// refused binding is skipped, and so is everything naming its far end,
/// while a conjunct over another binding is still checked.
#[test]
fn a_construct_naming_what_a_refused_one_binds_is_not_checked() {
    let src = r#"query q() { match { $p: Persn, $p knows $f, $c: Company } where $f.name = "x" and $c.nme = "y" return { $f.name } }"#;
    assert_eq!(
        codes_at(src),
        [("T1", "$p: Persn"), ("T6", r#"$c.nme = "y""#)]
            .map(|(c, t)| (c.to_string(), t.to_string())),
    );
    // A refused `from` drew columns no one knows: what names one of them is
    // skipped, and what names a known binding is checked.
    let src = r#"query q() { match { $c: Company, from nearest_x($c) } where $c.nme = "y" and $zz.a = 1 return { $c } }"#;
    let got = codes_at(src);
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(got[1], ("T6".to_string(), r#"$c.nme = "y""#.to_string()));
}

/// Declarations are independent: each one's refusals are in the pass, and a
/// repeated name is refused at each later declaration beside the type
/// errors, whose calls resolve to the first.
#[test]
fn declarations_and_repeated_names_are_refused_in_one_pass() {
    let src = "query a() { match { $p: Persn } return { $p } }\nquery b() { match { $d: Doc } where $d.yr > 3 return { $d } }";
    assert_eq!(
        codes_at(src),
        [("T1", "$p: Persn"), ("T6", "$d.yr > 3")].map(|(c, t)| (c.to_string(), t.to_string())),
    );
    let src = "query a() { match { $p: Person } return { $p } }\nquery a() { match { $p: Person } return { $p.nme } }\nquery a() { match { $p: Person } return { $p } }";
    assert_eq!(
        codes_at(src),
        [("T83", "a"), ("T6", "$p.nme"), ("T83", "a")].map(|(c, t)| (c.to_string(), t.to_string())),
    );
}

/// The statements of a write are independent of each other.
#[test]
fn each_write_statement_is_checked() {
    let src = r#"query w($h: String) { match { $p: Person } where $p.handle = $h  update $p set { nmae: "x" }  update $p set { cty: "y" } }"#;
    let got = codes_at(src);
    assert_eq!(
        got.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(),
        ["T11", "T11"]
    );
    assert!(
        got[0].1.contains("nmae") && got[1].1.contains("cty"),
        "{got:?}"
    );
}

/// A parse refusal stops the pass at parsing: the type refusals of the
/// same text are not reported, since the stage that finds them reads what
/// parsing accepted.
#[test]
fn a_parse_refusal_stops_the_pass_at_parsing() {
    let src = "query q() { match { $p: Persn } where $p.name = 'x' return { $p.nme } }";
    let got = pass(src);
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(spanned(src, &got[0]), "where $p.name = 'x'");
}

/// The parser resumes at the next element, item, statement or declaration
/// boundary: independent parse refusals of one text, a grammar refusal
/// among them, are all reported in source order, each with its own edits.
#[test]
fn independent_parse_refusals_are_all_reported_in_source_order() {
    let src = "query q() { match { $p -knows-> $f, $c: Company } where $c.name == 'x' return { $p.name, distinct $c } }";
    let got = pass(src);
    assert_eq!(
        got.iter()
            .map(|d| (d.code.as_str(), spanned(src, d)))
            .collect::<Vec<_>>(),
        [
            ("Q007", "$p -knows-> $f"),
            ("Q014", "where $c.name == 'x'"),
            ("Q014", "return { $p.name, distinct $c }"),
        ],
        "{got:?}"
    );
    // Each refusal carries the edits of its own unit: applying all of them
    // reparses.
    let mut edits: Vec<_> = got
        .iter()
        .flat_map(|d| d.suggestion.iter().flat_map(|s| s.edits.clone()))
        .collect();
    edits.sort_by_key(|e| (e.start, e.end));
    let fixed = crate::query::syntax::parse::apply_edits(src, &edits);
    assert!(
        crate::query::syntax::parse::parse_source(&fixed).is_ok(),
        "{fixed}"
    );
    // A grammar refusal in one declaration leaves the next one parsed.
    let src = "query a() { match { $p: Person } return $p.name }\nquery b() { match { $x: Person, $x -knows-> $y } return { $x } }\nquery c() { match { $p: Person, } where $p.age = = 3 return { $p } }";
    let got = pass(src);
    assert_eq!(
        got.iter().map(|d| d.code.as_str()).collect::<Vec<_>>(),
        ["Q020", "Q007", "Q001"],
        "{got:?}"
    );
    let at = |d: &QueryDiagnostic| d.position.map(|p| p.byte);
    assert!(got.windows(2).all(|w| at(&w[0]) < at(&w[1])));
}

/// A refused binding gives its name the error type: the triple, the
/// conjunct, the return item and the order key reading it pass every check,
/// unreported, while an independent refusal in the other operand of the same
/// `or` is reported.
#[test]
fn a_refused_bindings_dependents_are_not_reported() {
    let src = r#"query q() { match { $p: Persn, $p knows $f, $c: Company } where $p.name = "x" or $c.nmae = 1 return { $p.name, $f, $c.slug } order { $p.age } }"#;
    assert_eq!(
        codes_at(src),
        [("T1", "$p: Persn"), ("T6", "$c.nmae = 1")].map(|(c, t)| (c.to_string(), t.to_string())),
    );
}

/// Inside a block every element and conjunct is checked: its independent
/// refusals are all reported, and the block, of the error type, adds none
/// to the conjunct holding it.
#[test]
fn independent_type_refusals_in_a_block_are_all_reported() {
    let src = r#"query q() { match { $p: Person } where not exists { $f: Person, $p knws $f, $f worksAt $c where $f.nmae = "a" and $f.agee > 3 } and $p.cty = "x" return { $p.name } }"#;
    assert_eq!(
        codes_at(src),
        [
            ("T4", "$p knws $f"),
            ("T84", "$f worksAt $c"),
            ("T6", r#"$f.nmae = "a""#),
            ("T6", "$f.agee > 3"),
            ("T6", r#"$p.cty = "x""#),
        ]
        .map(|(c, t)| (c.to_string(), t.to_string())),
    );
    // So in an inline read and an `optional`.
    let src = r#"query q() { match { $p: Person, optional { $q: Person, $p knows $q where $q.nmae = "a" }, from { match { $c: Company } where $c.nmae = 1 return { $c } } } where $c.slug = "x" return { $p } }"#;
    assert_eq!(
        codes_at(src),
        [("T6", r#"$q.nmae = "a""#), ("T6", "$c.nmae = 1")]
            .map(|(c, t)| (c.to_string(), t.to_string())),
    );
}

/// `T86` and `T81` are typing refusals: each is reported beside the other
/// type refusals of its declaration that it does not depend on.
#[test]
fn the_lowering_refusal_and_the_product_rule_stand_beside_other_type_refusals() {
    let src = r#"query q($h: String) { match { $p: Person, optional { $q: Person, $p knows $q } } where $p.hndle = $h return { $p.name, $q } }"#;
    assert_eq!(
        codes_at(src),
        [("T6", "$p.hndle = $h"), ("T86", "$q")].map(|(c, t)| (c.to_string(), t.to_string())),
    );
    // A `from` whose `as` alone is refused draws its relation, so the
    // ranking it holds, which this release does not lower, is refused too.
    let src = "query wrapped($q: Vector(3)) { match { from { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $dist } return { $d, $dist } } as $s } return { $d.title } }";
    assert_eq!(
        pass(src)
            .iter()
            .map(|d| d.code.as_str())
            .collect::<Vec<_>>(),
        ["T85", "T86"],
    );
    // The refused binding passes the product rule, which `$b` and `$c`
    // still break; a refused conjunct naming one of them would pass its
    // key-pin check.
    let src = r#"query q() { match { $a: Persn, $b: Company, $c: Doc } return { $b.nme, $c } }"#;
    assert_eq!(
        codes_at(src),
        [("T1", "$a: Persn"), ("T81", "$c: Doc"), ("T6", "$b.nme")]
            .map(|(c, t)| (c.to_string(), t.to_string())),
    );
}

/// An unknown name lists up to five names of its kind, nearest first, each
/// with its type, behind each code's own wording.
#[test]
fn an_unknown_name_lists_the_nearest_names_of_its_kind_with_their_types() {
    let message = |src: &str| pass(src)[0].message.clone();
    let m = message("query q() { match { $p: Person } return { $p.nmae } }");
    assert_eq!(
        m,
        "type `Person` has no property `nmae`; nearest: name (String), age (I32?), city (String?), handle (String)"
    );
    let m = message("query q() { match { $p: Persn } return { $p } }");
    assert!(
        m.starts_with("unknown node type `Persn`; nearest: Person (node), "),
        "{m}"
    );
    let m = message("query q() { match { $p: Person, $p knws $f } return { $f } }");
    assert!(
        m.starts_with("unknown edge type `knws`; nearest: knows (Person -> Person), "),
        "{m}"
    );
    let m = message("query q($h: String) { match { $p: Person } return { $q } }");
    assert_eq!(
        m,
        "variable `$q` is not bound; nearest: $h (String), $p (node Person)"
    );
    let m = message("query q() { match { $p: Person } return { $p.name as n } order { nn } }");
    assert_eq!(
        m,
        "`nn` in `order` names no return alias; nearest: n (String)"
    );
    // The grammar refuses an unknown parameter type, with the same listing.
    let m = message("query q($n: Str) { match { $p: Person } return { $p } }");
    assert!(m.ends_with("found `Str`; nearest: String (scalar), Date (scalar), F32 (scalar), F64 (scalar), I32 (scalar)"), "{m}");
    let m = message(r#"query w() { insert Person { handle: "a", nmae: "b" } }"#);
    assert!(
        m.starts_with("type `Person` has no property `nmae`; nearest: name (String), "),
        "{m}"
    );
    let m = message(
        "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, kk: 3) as $s } return { $d } }",
    );
    assert_eq!(
        m,
        "`nearest` has no option `kk`; it admits k (integer), recall (F64 in (0, 1])"
    );
}
