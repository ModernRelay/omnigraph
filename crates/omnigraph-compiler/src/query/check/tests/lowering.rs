//! The release's lowering admission (docs/rfcs/2026-11-07-gq-3.md, section
//! 5, "A read the type checker accepts, the planner lowers"): a term the
//! planner does not lower is `T86` at its construct, naming the stage that
//! adds it, and the closure table is the test. Every freeze-list row of
//! section 6 that the RFC marks as working is represented by texts that type
//! check and shape; a construct of a row this release does not lower yet is
//! `T86` naming its stage.

use super::{SCHEMA, catalog};
use crate::ir::core::CoreDecl;
use crate::query::check::check_source;
use crate::query::diagnostic::QueryDiagnostic;
use crate::shape::read::ReadShape;
use crate::shape::write::WriteShape;

/// What a representative text does in this release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// It type-checks and the shaper lowers it: no `T86`, no `P006`.
    Lowers,
    /// `T86`, naming this stage.
    Refused(&'static str),
}

use Outcome::{Lowers, Refused};

/// A freeze-list row: its surface and its representative texts. Every row
/// of the table says its surface works in this release; a refusal the row
/// itself states (a self-call, `T86` naming Rec) is its text's outcome.
struct Row {
    surface: &'static str,
    texts: &'static [(&'static str, Outcome)],
}

/// Texts of a row the RFC marks as working that this build does not lower
/// yet. Stage B ships in part: each is `T86` naming B, and the set must
/// shrink to empty before the release ships B whole.
const B_IN_PROGRESS: &[&str] = &[
    "edge binding in a read",
    "optional",
    "a declared read's call",
    "union",
    "distance",
    "limit over a parameter",
];

const CLOSURE: &[Row] = &[
    Row {
        surface: "Declaration kinds",
        texts: &[
            (
                "query q() { match { $p: Person } return { $p.name } }",
                Lowers,
            ),
            (
                "query q($h: String) { match { $p: Person } where $p.handle = $h delete $p }",
                Lowers,
            ),
        ],
    },
    Row {
        surface: "Read body",
        texts: &[(
            "query q() { match { $p: Person } where $p.age > 30 return { $p.name, $p.age } order { $p.age desc, $p.name } limit 3 }",
            Lowers,
        )],
    },
    Row {
        surface: "Elements of `match`, and `where`",
        texts: &[
            (
                "query q() { match { $p: Person, $p knows $q } return { $q.name } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person, let $n = $p.name } where $n = \"Ada\" return { $p.handle } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person, from { match { $c: Company, $p worksAt $c } return { $c } } } return { $p.name, $c.name } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person } where count { $q: Person, $p knows $q where $q.age > 30 } > 0 return { $p.name } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person } where not exists { $c: Company, $p worksAt $c } return { $p.name } }",
                Lowers,
            ),
            // "edge binding in a read"
            (
                "query q() { match { $k: Knows } return { $k.@src } }",
                Refused("B"),
            ),
            // "optional"
            (
                "query q() { match { $p: Person, optional { $p worksAt $c } } return { $p.name, $c.name } }",
                Refused("B"),
            ),
        ],
    },
    Row {
        surface: "Relation expressions",
        texts: &[
            (
                "query q() { match { from { match { $p: Person } where $p.age > 30 return { $p } } } return { $p.name } }",
                Lowers,
            ),
            // "a declared read's call"
            (
                "query adults() { match { $p: Person } where $p.age > 30 return { $p } }
                 query q() { match { from adults() } return { $p.name } }",
                Refused("B"),
            ),
            // "union"
            (
                "query q() { match { from union({ match { $p: Person } return { $p } }, { match { $q: Person } return { $q as p } }) } return { $p.name } }",
                Refused("B"),
            ),
            (
                "query q($s: String) { match { let $all = { match { $d: Doc } return { $d } }, from bm25($all, $d.body, $s) as $score } return { $d.title, $score } order { $score desc } limit 3 }",
                Lowers,
            ),
            // A monotone self-call types to `Fix`, which stage Rec evaluates.
            (super::REACHABLE, Refused("Rec")),
        ],
    },
    Row {
        surface: "Ranking",
        texts: &[
            (
                "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: 3) as $distance } return { $d.title, $distance } order { $distance } limit 3 }",
                Lowers,
            ),
            (
                "query q($s: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, terms($s)) as $score } return { $d.title } order { $score desc } limit 3 }",
                Lowers,
            ),
            (
                "query q($s: String, $v: Vector(3)) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $v, k: 3), bm25($all, $d.body, $s), k: 60) as $score } return { $d.title } order { $score desc } limit 3 }",
                Lowers,
            ),
            // "distance"
            (
                "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from distance($pop, $d.embedding, $v) as $distance } return { $d.title, $distance } }",
                Refused("B"),
            ),
        ],
    },
    Row {
        surface: "Mutation body",
        texts: &[
            (
                "query q($h: String) { insert Person { handle: $h, name: $h } }",
                Lowers,
            ),
            (
                "query q($h: String) { match { $p: Person } where $p.handle = $h update $p set { city: null } }",
                Lowers,
            ),
            (
                "query q($h: String) { match { $p: Person, $p worksAt $c } where $c.slug = $h update $p set { city: $c.name } }",
                Lowers,
            ),
        ],
    },
    Row {
        surface: "Traversal",
        texts: &[
            (
                "query q() { match { $p: Person, $p knows{1,3} $q } return { $q.name } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person, $p <knows> $q } return { $q.name } }",
                Lowers,
            ),
        ],
    },
    Row {
        surface: "Calls",
        texts: &[
            (
                "query q() { match { $p: Person } where starts_with($p.name, \"A\") return { $p.name } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person } return { count(distinct $p.city) as cities } }",
                Lowers,
            ),
        ],
    },
    Row {
        surface: "Expressions",
        texts: &[(
            "query q() { match { $p: Person } where $p.age + 1 > 30 and ($p.city is null or $p.city in [\"Berlin\"]) return { $p.name, -$p.age as neg, coalesce($p.city, \"none\") as city, if($p.age > 30, \"old\", \"young\") as band, concat($p.name, \"!\") as shout } }",
            Lowers,
        )],
    },
    Row {
        surface: "Parameters",
        texts: &[
            (
                "query q($n: I32? = 30) { match { $p: Person } where $p.age > $n return { $p.name } }",
                Lowers,
            ),
            // "limit over a parameter"
            (
                "query q($k: I64) { match { $p: Person } return { $p.name } limit $k }",
                Refused("B"),
            ),
        ],
    },
    Row {
        surface: "Annotations",
        texts: &[(
            "query q() @description(\"names\") @budget(plan = 100) { match { $p: Person } return { $p.name } }",
            Lowers,
        )],
    },
    Row {
        surface: "Meta-fields",
        texts: &[(
            "query q() { match { $p: Person, $p $w:knows $q } return { $p.@id, $w.@src, $w.@dst } }",
            Lowers,
        )],
    },
    // `C` and `@C`, the "Type references" row, are every row's `Person` and
    // this row's `@Operator`.
    Row {
        surface: "Catalogue names",
        texts: &[(
            "query q() { match { $o: @Operator } return { $o.name } }",
            Lowers,
        )],
    },
];

/// Every declaration of `text` checked through the release's front door, and
/// each accepted one shaped as the planner shapes it.
fn outcome(text: &str) -> Result<(), QueryDiagnostic> {
    let catalog = catalog();
    let decls = check_source(&catalog, text).unwrap_or_else(|e| panic!("parse: {e}\n{text}"));
    for (name, checked) in decls {
        let core = checked?.core;
        // An accepted declaration shapes: nothing the type checker admits
        // reaches the planner's `P006`.
        let shaped = match &core {
            CoreDecl::Read(read) => ReadShape::of(read, &catalog).map(|_| ()),
            CoreDecl::Write(write) => WriteShape::of(write, &catalog).map(|_| ()),
        };
        shaped.unwrap_or_else(|e| panic!("`{name}` was accepted and does not shape: {e}\n{text}"));
    }
    Ok(())
}

#[test]
fn every_row_marked_works_lowers_and_every_other_construct_names_its_stage() {
    let _ = SCHEMA;
    let mut pending = Vec::new();
    for row in CLOSURE {
        for (text, want) in row.texts {
            let got = match outcome(text) {
                Ok(()) => Lowers,
                Err(d) => {
                    assert_eq!(d.code.as_str(), "T86", "{}: {d}\n{text}", row.surface);
                    let stage = ["B", "Rec"]
                        .into_iter()
                        .find(|s| d.message.contains(&format!("stage {s} ")))
                        .unwrap_or_else(|| panic!("{}: no stage named: {d}", row.surface));
                    assert!(d.position.is_some(), "{}: no position: {d}", row.surface);
                    Refused(stage)
                }
            };
            assert_eq!(got, *want, "{}:\n{text}", row.surface);
            if got == Refused("B") {
                pending.push(*text);
            }
        }
    }
    // A row the RFC marks as working that this build refuses is stage B in
    // progress, and only the listed constructs are.
    assert_eq!(pending.len(), B_IN_PROGRESS.len(), "{pending:#?}");
}

/// `T86` points at the construct it refuses and prints the rewrite this
/// release lowers where one exists.
#[test]
fn t86_is_at_the_construct_with_its_repair() {
    use crate::query::syntax::parse::apply_edits;
    let refused = |text: &str| outcome(text).expect_err("refused");
    // A `let` whose value can fail: the refusal is at the `let`, and its fix
    // writes the value where it is read.
    let text = "query q() {\n  match { $p: Person, let $next = $p.age + 1 }\n  where $next > 30\n  return { $p.handle }\n}";
    let d = refused(text);
    assert_eq!(d.code.as_str(), "T86", "{d}");
    let at = d.position.expect("a position");
    assert_eq!((at.line, at.column), (2, 23), "{d}");
    assert!(
        d.fix.as_deref().is_some_and(|f| f.contains("`$p.age + 1`")),
        "{d}"
    );
    // The value written in `where` lowers.
    outcome("query q() { match { $p: Person } where $p.age + 1 > 30 return { $p.handle } }")
        .expect("the value in place lowers");
    // An order key no return item computes: the edit returns it.
    let text = "query q() { match { $p: Person } return { $p.handle } order { $p.age > 30 } }";
    let d = refused(text);
    assert_eq!(d.position.map(|p| p.column), Some(63), "{d}");
    let suggestion = d.suggestion.expect("an edit");
    let repaired = apply_edits(text, &suggestion.edits);
    outcome(&repaired).unwrap_or_else(|e| panic!("{repaired}: {e}"));
    // An `optional` is refused at the element.
    let d =
        refused("query q() { match { $p: Person, optional { $p knows $f } } return { $p.name } }");
    assert_eq!(d.position.map(|p| p.column), Some(33), "{d}");
    assert!(d.fix.is_none(), "{d}");
    // A block compared with a column is refused at the comparison.
    let d = refused(
        "query q() { match { $p: Person } where count { $f: Person, $p knows $f } >= $p.age return { $p.name } }",
    );
    assert_eq!(d.position.map(|p| p.column), Some(40), "{d}");
    // A write group whose selection reads a table an earlier group changes
    // is refused at that group's `match`.
    let d = refused(
        "query q() {\n  match { $p: Person } where $p.name = \"ada\"\n  update $p set { city: \"x\" }\n  match { $q: Person, $q worksAt $c }\n  update $c set { name: $q.name }\n}",
    );
    let at = d.position.expect("a position");
    assert_eq!((at.line, at.column), (4, 11), "{d}");
    // `limit` over a parameter is refused at the `limit`.
    let d = refused("query q($k: I64) { match { $p: Person } return { $p.name } limit $k }");
    assert_eq!(d.position.map(|p| p.column), Some(66), "{d}");
}
