//! The release's lowering admission (the GQ 3 RFC, section
//! 5, "A read the type checker accepts, the planner lowers"): a term the
//! planner does not lower is `T86` at its construct, naming the construct
//! and saying this release does not support it yet, and the closure table
//! is the test. Every freeze-list row of section 6 that the RFC marks as
//! working is represented by texts that type check and shape; a construct
//! of a row this release does not lower yet is `T86`. The stage that adds
//! each refused construct is recorded here ([`B_IN_PROGRESS`] for stage B,
//! [`LATER`] for later stages) and by the shaper's [`crate::shape::ProgrammeStage`], and
//! the two must agree; no message names a stage.

use super::SCHEMA;
use crate::catalog::{Catalog, build_catalog};
use crate::ir::core::CoreDecl;
use crate::query::check::check_source;
use crate::query::diagnostic::QueryDiagnostic;
use crate::schema::parser::parse_schema;
use crate::shape::ShapeError;
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
/// itself states (a self-call, `T86` of a stage Rec construct) is its
/// text's outcome.
struct Row {
    surface: &'static str,
    texts: &'static [(&'static str, Outcome)],
}

/// A construct of a row the RFC marks as working that this build does not
/// lower yet: `T86` of stage B, at the shaper's refusal whose term is
/// `site` (its text, a `format!` string's literal parts in order).
struct InProgress {
    construct: &'static str,
    site: &'static str,
    text: &'static str,
}

/// Every construct the RFC marks as working that this build refuses with
/// `T86`, each of stage B. Stage B ships in part: the list must shrink to
/// empty before the release ships B whole, and it is the acceptance test of
/// the planner's direct lowering of the core term, which reaches these
/// forms. Every refusal of the shapers of stage B has an entry here, one
/// per call site at least
/// ([`every_refusal_of_the_shapers_is_pinned`]); a term the type checker
/// never writes is a [`crate::shape::ShapeError::Internal`] defect, not a
/// refusal, and has none.
const B_IN_PROGRESS: &[InProgress] = &[
    // `ReadShape`: a ranking drawn as a read of its own.
    InProgress {
        construct: "an `rrf` correlated on an outer variable",
        site: "an `rrf` whose arms read the enclosing scope",
        text: "query q($s: String, $v: Vector(3)) { match { $p: Person, let $pop = { match { $d: Doc, $p authored $d } return { $d } }, from rrf(bm25($pop, $d.body, $s), nearest($pop, $d.embedding, $v, k: 3)) as $f } return { $p.name, $d.title, $f } }",
    },
    // `ReadShape`: return items and order keys.
    // `WriteShape`.
    InProgress {
        construct: "an assignment reading a Blob from a row",
        site: "an assignment that reads a Blob from a row (`{property}`)",
        text: "query q() { match { $f: File, $g: File } where $f.name = \"a\" and $g.name = \"b\" update $g set { data: $f.data } }",
    },
];

/// The shapers' refusals of a later stage, each with the stage, its term
/// and a text that reaches it.
const LATER: &[(&str, &str, &str)] =
    &[("Rec", "a read that calls itself (`Fix`)", super::REACHABLE)];

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
                "query q() { match { $p: Person, let $next = $p.age + 1, $p knows $q } where $next > 30 and $q.age > 3 return { $q.handle, $next } }",
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
            (
                "query q() { match { $p: Person } where count { $f: Person, $p knows $f } >= $p.age return { $p.name } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person } where count { $f: Person, $p knows $f } > count { $c: Company, $p worksAt $c } return { $p.name } }",
                Lowers,
            ),
            // A block's value in a `let`, as a return item, and under `or`
            // and `not`.
            (
                "query q() { match { $c: Company, let $n = count { $p: Person, $p worksAt $c } } where $n < 2 return { $c.name, $n, sum($p.age) { $p: Person, $p worksAt $c } as ages } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person } where exists { $f: Person, $p knows $f } or not (count { $c: Company, $p worksAt $c } > 0 and $p.age > 30) return { $p.name } }",
                Lowers,
            ),
            (
                "query q() { match { $k: Knows } return { $k.@src } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person, optional { $c: Company, $p worksAt $c } } return { $p.name, $c.name } }",
                Lowers,
            ),
            // An edge reference returned whole: its edge object.
            (
                "query q() { match { $p: Person, $q: Person, $p $k:knows $q } return { $p.name, $k } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person, $q: Person, $p $k:knows $q } return { $k.@id, $k.@src, $k.@dst } order { $k } }",
                Lowers,
            ),
            // A node an `optional` introduces returned whole: a nullable
            // node object.
            (
                "query q() { match { $p: Person, optional { $c: Company, $p worksAt $c } } return { $p.name, $c } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person, optional { $f: Person, $p knows $f, $c: Company, $f worksAt $c } } where $c.name = \"Acme\" return { $p.name, $f.name } }",
                Lowers,
            ),
            (
                "query q() { match { $p: Person, optional { $c: Company, $p worksAt $c, let $n = $c.name, let $one = 1 } } where coalesce($n, \"none\") != \"x\" return { $p.name, $n, $one } }",
                Lowers,
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
            (
                "query adults() { match { $p: Person } where $p.age > 30 return { $p } }
                 query q() { match { from adults() } return { $p.name } }",
                Lowers,
            ),
            (
                "query colleagues($h: String) { match { $p: Person, $p worksAt $c, $q worksAt $c } where $p.handle = $h return { $q } }
                 query q() { match { $p: Person, from colleagues($p.handle) } return { $p.name, $q.name } }",
                Lowers,
            ),
            (
                "query q() { match { from union({ match { $p: Person } return { $p } }, { match { $q: Person } return { $q as p } }) } return { $p.name } }",
                Lowers,
            ),
            (
                "query q() { match { from { match { $p: Person, $c: Company, $p worksAt $c } return { $c, count($p) as n } } } return { $c.name, $n } }",
                Lowers,
            ),
            (
                "query q() { match { from { match { $p: Person } return { $p } order { $p.age desc } limit 2 } } return { $p.name } }",
                Lowers,
            ),
            (
                "query q() { match { $c: Company, from { match { $p: Person, $p worksAt $c } return { count($p) as n } } } return { $c.name, $n } }",
                Lowers,
            ),
            (
                "query q() { match { $c: Company, from { match { $p: Person, $p worksAt $c } return { $p } order { $p.age desc } limit 1 } } return { $c.name, $p.name } }",
                Lowers,
            ),
            (
                "query q() { match { from { match { $p: Person } return distinct { $p.city as town } } } return { $town } }",
                Lowers,
            ),
            // A ranking inside an inline read with its own order and limit.
            (
                "query q($s: String) { match { from { match { let $all = { match { $d: Doc } return { $d } }, from bm25($all, $d.body, $s) as $score } return { $d } order { $score desc } limit 2 } } return { $d.title } }",
                Lowers,
            ),
            // A ranking over an inline read with an aggregate or `distinct`.
            (
                "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return distinct { $d } }, $d.embedding, $v, k: 1) as $x } return { $d.id } }",
                Lowers,
            ),
            // A ranking over an inline read with an `order`.
            (
                "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } order { $d.id } }, $d.embedding, $v, k: 1) as $x } return { $d.id } }",
                Lowers,
            ),
            // A ranking over an inline read with a `limit`.
            (
                "query q($v: Vector(3)) { match { from distance({ match { $d: Doc } return { $d } limit 3 }, $d.embedding, $v) as $x } return { $d.id } }",
                Lowers,
            ),
            // A ranking beside another pattern component.
            (
                "query q($s: String) { match { $p: Person, let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, $s) as $score } where $p.handle = \"p1\" return { $p.name, $d.title, $score } }",
                Lowers,
            ),
            // A ranking correlated on an outer variable.
            (
                "query q($v: Vector(3)) { match { $p: Person, from nearest({ match { $d: Doc, $p authored $d } return { $d } }, $d.embedding, $v, k: 3) as $s } return { $p.name, $d.title } }",
                Lowers,
            ),
            // Two rankings in one pattern.
            (
                "query q($s: String, $v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, $s) as $score, let $pop2 = { match { $e: Doc } return { $e } }, from nearest($pop2, $e.embedding, $v, k: 2) as $dist } return { $d.title, $e.title } }",
                Lowers,
            ),
            // A ranking inside an inline read without its own order.
            (
                "query q($v: Vector(3)) { match { from { match { let $all = { match { $d: Doc } return { $d } }, from nearest($all, $d.embedding, $v, k: 3) as $s } return { $d, $s } } } return { $d.title, $s } }",
                Lowers,
            ),
            // A ranking inside an `optional`.
            (
                "query q($v: Vector(3)) { match { $p: Person, optional { let $all = { match { $d: Doc } return { $d } }, from nearest($all, $d.embedding, $v, k: 3) as $s } } where $p.handle = \"a\" return { $p.name } }",
                Lowers,
            ),
            // A ranking as a `union` arm.
            (
                "query q($v: Vector(3)) { match { let $all = { match { $d: Doc } return { $d } }, from union(nearest($all, $d.embedding, $v, k: 3), { match { $d: Doc } return { $d } }) } return { $d.title } }",
                Lowers,
            ),
            (
                "query q() { match { from union({ match { $p: Person } return { $p, 1 as d } }, { match { $q: Person } return { $q as p, 2 as d } }) } return { $p.name, $d } }",
                Lowers,
            ),
            (
                "query q() { match { from union({ match { $p: Person } return { $p } order { $p.age } limit 1 }, { match { $q: Person } return { $q as p } }) } return { $p.name } }",
                Lowers,
            ),
            // A union arm returning an edge.
            (
                "query q() { match { from union({ match { $p: Person, $q: Person, $p $k:knows $q } return { $k } }, { match { $a: Person, $b: Person, $a $e:knows $b } return { $e as k } }) } return { $k.@src } }",
                Lowers,
            ),
            (
                "query q() { match { $c: Company, from union({ match { $p: Person, $p worksAt $c } return { $p } }, { match { $q: Person } return { $q as p } }) } return { $p.name } }",
                Lowers,
            ),
            // A union arm returning a value that reads the enclosing scope.
            (
                "query q() { match { $c: Company, from union({ match { $p: Person, $p worksAt $c } return { $p, 1 as d } }, { match { $q: Person } return { $q as p, 2 as d } }) } return { $p.name, $d } }",
                Lowers,
            ),
            // An inline read returning an outer variable under another name.
            (
                "query q() { match { $p: Person, from { match { $q: Person, $p knows $q } return { $p as r } } } return { $r.name } }",
                Lowers,
            ),
            // An inline read with an aggregate returning an outer variable under another name.
            (
                "query q() { match { $p: Person, from { match { $q: Person, $p knows $q } return { $p as r, count($q) as n } } } return { $r.name, $n } }",
                Lowers,
            ),
            // A called read's `limit $k` bound to a computed argument.
            (
                "query top($k: I64) { match { $q: Person } return { $q } order { $q.age } limit $k }
               query q() { match { $p: Person, from top(3 + 1) } return { $p.name, $q.name } }",
                Lowers,
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
                "query q($s: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, terms($s, mode: any)) as $score } return { $d.title } order { $score desc } limit 3 }",
                Lowers,
            ),
            // `bm25` over every term of a `terms(…)` value, scored by the
            // exact lexical scan.
            (
                "query q($s: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, terms($s)) as $score } return { $d.title } order { $score desc } limit 3 }",
                Lowers,
            ),
            (
                "query q($s: String, $v: Vector(3)) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $v, k: 3), bm25($all, $d.body, $s), k: 60) as $score } return { $d.title } order { $score desc } limit 3 }",
                Lowers,
            ),
            (
                "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from distance($pop, $d.embedding, $v) as $distance } return { $d.title, $distance } }",
                Lowers,
            ),
            (
                "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from distance($pop, $d.embedding, $v) as $dist } where $dist < 1.5 return { $d.title, $dist } order { $dist } limit 2 }",
                Lowers,
            ),
            (
                "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from distance($pop, $d.embedding, $v) as $dist } return { min($dist) as closest } }",
                Lowers,
            ),
            (
                "query q($s: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, $s) as $score } return { max($score) as best, count($d) as n } }",
                Lowers,
            ),
            // Two `nearest` arms with windows of their own.
            (
                "query q($v: Vector(3), $w: Vector(3)) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $v, k: 3), nearest($all, $d.embedding, $w, k: 5)) as $score } return { $d.title } order { $d.title } }",
                Lowers,
            ),
            // Three arms with weights.
            (
                "query q($v: Vector(3), $s: String, $t: String) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $v, k: 3), bm25($all, $d.body, $s), bm25($all, $d.body, $t), weights: [2.0, 1.0, 0.5]) as $score } return { $d.title } order { $score desc } limit 3 }",
                Lowers,
            ),
            // An `rrf` over a population whose pattern fans out or filters:
            // one row per target.
            (
                "query q($s: String, $v: Vector(3)) { match { let $pop = { match { $d: Doc, $p: Person, $p authored $d } where $p.age > 30 return { $d } }, from rrf(nearest($pop, $d.embedding, $v, k: 3), bm25($pop, $d.body, $s)) as $score, $p: Person, $p authored $d } return { $d.title, $p.name } order { $score desc } }",
                Lowers,
            ),
            // Arms over different populations.
            (
                "query q($s: String, $v: Vector(3)) { match { let $old = { match { $d: Doc, $p: Person, $p authored $d } where $p.age > 30 return { $d } }, let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($old, $d.embedding, $v, k: 3), bm25($all, $d.body, $s)) as $score } return { $d.title } order { $score desc } limit 3 }",
                Lowers,
            ),
            // An exact `distance` arm.
            (
                "query q($v: Vector(3), $s: String) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(distance($all, $d.embedding, $v), bm25($all, $d.body, $s), k: 60) as $score } return { $d.title } order { $score desc } limit 3 }",
                Lowers,
            ),
            // An inline read whose own `order` ranks its targets, and an
            // exact `bm25` arm on another type's declared field.
            (
                "query q($s: String) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(bm25($all, $d.body, $s), { match { $d: Doc } return { $d } order { $d.id } }) as $score } return { $d.title, $score } }",
                Lowers,
            ),
            (
                "query q($v: Vector(3), $s: String) { match { let $all = { match { $m: Memo } return { $m } }, from rrf(nearest($all, $m.embedding, $v, k: 3), bm25($all, $m.text, $s), k: 60) as $score } return { $m.slug } order { $score desc } limit 3 }",
                Lowers,
            ),
            // A lexical predicate is a Boolean operand like any other: under
            // `not` and `or`, compared with a value, inside `coalesce`, in a
            // filter over two bindings and in a block's `where`.
            (
                "query q($s: String) { match { $d: Doc, $p: Person, $p authored $d } where not match_terms($d.body, terms($s, mode: any)) or $p.age > 30 or match_terms($d.body, terms($s, mode: any, max_edits: 1)) = false or not coalesce(match_terms($d.body, terms($s, mode: any)), false) return { $d.title } }",
                Lowers,
            ),
            (
                "query q($s: String) { match { $p: Person } where not exists { $d: Doc, $p authored $d where not match_terms($d.body, terms($s, mode: any)) } return { $p.name } }",
                Lowers,
            ),
            // `match_terms` is the same operand, with every term required or
            // any one.
            (
                "query q($s: String) { match { $d: Doc, $p: Person, $p authored $d } where match_terms($d.body, terms($s)) or not match_terms($d.body, terms($s, mode: any, max_edits: 0)) or $p.age > 30 return { $d.title } }",
                Lowers,
            ),
            (
                "query q($s: String) { match { $p: Person } where count { $d: Doc, $p authored $d where match_terms($d.body, terms($s, mode: all)) } > 0 return { $p.name } }",
                Lowers,
            ),
            // An edit budget holds on every full-text field.
            (
                "query q($s: String) { match { $d: Doc } where match_terms($d.body, terms($s, mode: any, max_edits: 1)) return { $d.title } }",
                Lowers,
            ),
            // A full-text call read as a value.
            (
                "query q($s: String) { match { $d: Doc } return { $d.title, match_terms($d.body, terms($s, mode: any)) as hit } }",
                Lowers,
            ),
            // A full-text call as an order key.
            (
                "query q($s: String) { match { $d: Doc } return { $d.title } order { match_terms($d.body, terms($s, mode: any)) } }",
                Lowers,
            ),
            // A `let` inside an `optional` that binds no node.
            (
                "query q() { match { $p: Person, $q: Person, optional { $p knows $q, let $x = 1 } } where $p.handle = \"a\" and $q.handle = \"b\" return { $p.name, $x } }",
                Lowers,
            ),
            // A full-text call in a block's aggregate.
            (
                "query q($s: String) { match { $p: Person } where sum(if(match_terms($d.body, terms($s, mode: any)), 1, 0)) { $d: Doc, $p authored $d } > 0 return { $p.name } }",
                Lowers,
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
        texts: &[
            (
                "query q() { match { $p: Person } return { $p.handle } order { $p.age > 30 } }",
                Lowers,
            ),

            (
            "query q() { match { $p: Person } where $p.age + 1 > 30 and ($p.city is null or $p.city in [\"Berlin\"]) return { $p.name, -$p.age as neg, coalesce($p.city, \"none\") as city, if($p.age > 30, \"old\", \"young\") as band, concat($p.name, \"!\") as shout } }",
            Lowers,
        ),
        ],
    },
    Row {
        surface: "Parameters",
        texts: &[
            (
                "query q($n: I32? = 30) { match { $p: Person } where $p.age > $n return { $p.name } }",
                Lowers,
            ),
            (
                "query q($k: I64) { match { $p: Person } return { $p.name } limit $k }",
                Lowers,
            ),
            (
                "query q($k: I64, $v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: $k) as $distance } return { $d.title } order { $d.title } }",
                Lowers,
            ),
            (
                "query q($k: I64, $s: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, $s) as $score } return { $d.title } order { $score desc } limit $k }",
                Lowers,
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

/// The tests' schema with a node holding a `Blob`, which a write's
/// assignment may read from a row.
fn catalog() -> Catalog {
    let schema = format!(
        "{SCHEMA}\nnode File {{ name: String @key  data: Blob? }}\nnode Memo {{ slug: String @key  text: String @index(fulltext, analyzer = \"standard_v1\")  embedding: Vector(3) @index }}\n"
    );
    build_catalog(&parse_schema(&schema).expect("the schema parses")).unwrap()
}

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

/// The stage the shaper records for the first term of `text` it does not
/// lower: the code's record, which no message names.
fn shaper_stage(text: &str) -> &'static str {
    let catalog = catalog();
    let decls = crate::query::check::type_source(&catalog, text)
        .unwrap_or_else(|e| panic!("parse: {e}\n{text}"));
    for (_, checked) in decls {
        let core = checked
            .unwrap_or_else(|e| panic!("does not type: {e}\n{text}"))
            .core;
        let shaped = match &core {
            CoreDecl::Read(read) => ReadShape::of(read, &catalog).map(|_| ()),
            CoreDecl::Write(write) => WriteShape::of(write, &catalog).map(|_| ()),
        };
        if let Err(ShapeError::Unlowered(u)) = shaped {
            return u.stage.code();
        }
    }
    panic!("every declaration shapes:\n{text}")
}

/// A `T86`'s message: it names the construct, says this release does not
/// support it yet and names no stage; the stage is the shaper's record.
fn refused_stage(d: &QueryDiagnostic, text: &str) -> &'static str {
    assert_eq!(d.code.as_str(), "T86", "{d}\n{text}");
    assert!(
        d.message.contains("this release does not support") && d.message.ends_with(" yet"),
        "a T86 that does not say this release does not support its construct yet: {d}"
    );
    for stage in ["stage", " B ", "(B)", "Rec", "the break"] {
        assert!(
            !d.message.contains(stage),
            "a T86 naming a stage ({stage}): {d}"
        );
    }
    shaper_stage(text)
}

#[test]
fn every_row_marked_works_lowers_and_every_other_construct_names_its_stage() {
    let mut pending = Vec::new();
    for row in CLOSURE {
        for (text, want) in row.texts {
            let got = match outcome(text) {
                Ok(()) => Lowers,
                Err(d) => {
                    let stage = refused_stage(&d, text);
                    assert!(d.position.is_some(), "{}: no position: {d}", row.surface);
                    // A suggestion must keep the rows and the result
                    // columns; no refused term of this release has one.
                    assert!(
                        d.fix.is_none() && d.suggestion.is_none(),
                        "{}: a T86 that prints a rewrite: {d}",
                        row.surface
                    );
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
    // progress: a listed construct.
    for text in pending {
        assert!(
            B_IN_PROGRESS.iter().any(|entry| entry.text == text),
            "a stage B refusal that `B_IN_PROGRESS` does not list:\n{text}"
        );
    }
}

/// The term each refusal call of a shaper's source refuses, as written: a
/// string literal, a `format!` string or a `const` naming one. The calls
/// are `self.refuse(what)`, `self.refuse_at(what, …)`, `unsupported(name,
/// what, …)` and `Unlowered::new(name, what, …)`.
fn refusal_sites(source: &str) -> Vec<String> {
    // A Rust string literal starting at `text`'s first byte, unescaped.
    fn literal(text: &str) -> (String, usize) {
        assert!(
            text.starts_with('"'),
            "not a literal: {}",
            &text[..40.min(text.len())]
        );
        let mut out = String::new();
        let mut chars = text.char_indices().skip(1).peekable();
        while let Some((i, c)) = chars.next() {
            match c {
                '"' => return (out, i + 1),
                '\\' => match chars.next() {
                    Some((_, '\n')) => while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {},
                    Some((_, 'n')) => out.push('\n'),
                    Some((_, other)) => out.push(other),
                    None => break,
                },
                other => out.push(other),
            }
        }
        panic!("an unterminated literal");
    }
    let constant = |name: &str| -> String {
        let decl = format!("const {name}: &str = ");
        let at = source
            .find(&decl)
            .unwrap_or_else(|| panic!("no `const {name}` in the shaper"));
        literal(&source[at + decl.len()..]).0
    };
    let mut sites = Vec::new();
    for (call, skip_first) in [
        ("self.refuse(", false),
        ("self.refuse_at(", false),
        ("unsupported(", true),
        ("Unlowered::new(", true),
    ] {
        for (at, _) in source.match_indices(call) {
            let line_start = source[..at].rfind('\n').map_or(0, |i| i + 1);
            let line = source[line_start..at].trim_start();
            if line.starts_with("//") || line.ends_with("fn ") {
                continue;
            }
            let mut rest = source[at + call.len()..].trim_start();
            if skip_first {
                rest = rest[rest.find(',').expect("a second argument") + 1..].trim_start();
            }
            let what = if let Some(fmt) = rest.strip_prefix("format!(") {
                literal(fmt.trim_start()).0
            } else if rest.starts_with('"') {
                literal(rest).0
            } else {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                match name.as_str() {
                    // A helper passing its own argument through.
                    "what" => continue,
                    _ if name.chars().all(|c| c.is_ascii_uppercase() || c == '_') => {
                        constant(&name)
                    }
                    _ => panic!("a refusal whose term is not written at its call: `{call}{name}…`"),
                }
            };
            sites.push(what);
        }
    }
    sites
}

/// Whether the refusal message `message` states the term `site`: every
/// literal part of the `format!` string, in order.
fn names_site(message: &str, site: &str) -> bool {
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut chars = site.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                part.push('{');
            }
            '{' => {
                for c in chars.by_ref() {
                    if c == '}' {
                        break;
                    }
                }
                parts.push(std::mem::take(&mut part));
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                part.push('}');
            }
            other => part.push(other),
        }
    }
    parts.push(part);
    let mut rest = message;
    parts
        .iter()
        .filter(|p| !p.is_empty())
        .all(|part| match rest.find(part.as_str()) {
            Some(at) => {
                rest = &rest[at + part.len()..];
                true
            }
            None => false,
        })
}

/// Every refusal the shapers can make is pinned: a refusal of stage B by
/// the constructs of [`B_IN_PROGRESS`] that reach it, at least one per
/// call site, and one of a later stage by [`LATER`]. Each text type checks
/// to `T86` stating the site's term, and the shaper records the entry's
/// stage, so a new refusal without an entry, an entry whose refusal is gone,
/// an entry whose stage moved, or a text that no longer reaches its refusal
/// fails here.
#[test]
fn every_refusal_of_the_shapers_is_pinned() {
    let mut sites = refusal_sites(include_str!("../../../shape/read.rs"));
    sites.extend(refusal_sites(include_str!("../../../shape/write.rs")));
    // The read that calls itself (stage Rec) is refused whatever B lowers.
    assert!(
        sites.iter().any(|s| s.contains("`Fix`")),
        "the site reader found {sites:#?}"
    );
    let stage = |text: &str| -> (String, &'static str) {
        let d = outcome(text).expect_err(text);
        let stage = refused_stage(&d, text);
        (d.message, stage)
    };
    let mut unpinned = Vec::new();
    let mut distinct: Vec<&String> = sites.iter().collect();
    distinct.sort();
    distinct.dedup();
    for site in distinct {
        let calls = sites.iter().filter(|s| *s == site).count();
        let later = LATER.iter().filter(|(_, s, _)| s == site).count();
        let in_progress = B_IN_PROGRESS.iter().filter(|e| e.site == site).count();
        if later + in_progress < calls {
            unpinned.push(format!("{site} ({calls} calls)"));
        }
    }
    assert!(unpinned.is_empty(), "refusals with no entry: {unpinned:#?}");
    for entry in B_IN_PROGRESS {
        assert!(
            sites.iter().any(|s| s == entry.site),
            "`{}` names a refusal no shaper makes: {}",
            entry.construct,
            entry.site
        );
        let (message, named) = stage(entry.text);
        assert_eq!(named, "B", "{}: {message}", entry.construct);
        assert!(
            names_site(&message, entry.site),
            "{}: the refusal is not `{}`: {message}",
            entry.construct,
            entry.site
        );
    }
    for (want, site, text) in LATER {
        assert!(sites.iter().any(|s| s == site), "no shaper refuses {site}");
        let (message, named) = stage(text);
        assert_eq!(named, *want, "{message}");
        assert!(names_site(&message, site), "{message}");
    }
}

/// `T86` points at the construct it refuses.
#[test]
fn t86_is_at_the_construct() {
    let refused = |text: &str| outcome(text).expect_err("refused");
    // A list of references, a later stage's, is refused at its return
    // item.
    let text =
        "query q() { match { $p: Person, $p knows $f } return { $p.name, collect($f) as fs } }";
    let d = refused(text);
    let item = u32::try_from(text.rfind("collect").unwrap() + 1).unwrap();
    assert_eq!(d.position.map(|p| p.column), Some(item), "{d}");
}

/// Every group of a write selects on the write's one snapshot, so a group
/// whose selection reads a type an earlier group changes is `T91` at that
/// group's `match`, whether its selection is a filtered scan of its target
/// (the staged form) or a read the read planner shapes; a group reading only
/// types no earlier group changes is accepted.
#[test]
fn a_group_selecting_on_a_type_an_earlier_group_changes_is_t91() {
    let code = |text: &str| outcome(text).map(|_| ()).map_err(|d| d.code.as_str());
    for text in [
        // A selection read after an update of the type it reads.
        "query q() {\n  match { $p: Person } where $p.name = \"ada\"\n  update $p set { city: \"x\" }\n  match { $q: Person, $q worksAt $c }\n  update $c set { name: $q.name }\n}",
        // A staged scan after an update of its own target.
        "query q() {\n  match { $p: Person } where $p.name = \"ada\"\n  update $p set { city: \"x\" }\n  match { $q: Person } where $q.name = \"ada\"\n  update $q set { city: \"y\" }\n}",
    ] {
        let d = outcome(text).expect_err("refused");
        assert_eq!(d.code.as_str(), "T91", "{d}");
        let at = d.position.expect("a position");
        assert_eq!((at.line, at.column), (4, 11), "{d}");
        assert!(d.fix.is_some(), "{d}");
    }
    // A node delete changes its edges' tables too.
    assert_eq!(
        code(
            "query q() {\n  match { $p: Person } where $p.name = \"ada\"\n  delete $p\n  match { $q: Person, $q worksAt $c }\n  delete $c\n}"
        ),
        Err("T91")
    );
    // A later group reading a type no earlier group changes is accepted.
    assert_eq!(
        code(
            "query q() {\n  match { $p: Person } where $p.name = \"ada\"\n  update $p set { city: \"x\" }\n  match { $c: Company } where $c.name = \"acme\"\n  update $c set { name: \"acme2\" }\n}"
        ),
        Ok(())
    );
}

/// A `let` whose value can fail is evaluated where it was written: after the
/// steps of the elements before it and before those after it, and no
/// conjunct of `where` is placed below it, a conjunct over a binding written
/// before it included.
#[test]
fn a_let_that_can_fail_runs_before_where_and_the_elements_after_it() {
    use crate::shape::read::PatternStep;
    let catalog = catalog();
    let text = "query q() { match { $p: Person, let $next = $p.age + 1, $p knows $q } where $p.age > 3 and $next > 30 return { $q.handle } }";
    let decls = check_source(&catalog, text).expect("parse");
    let CoreDecl::Read(read) = decls.into_iter().next().unwrap().1.expect("accepted").core else {
        panic!("a read");
    };
    let shape = ReadShape::of(&read, &catalog).expect("shapes");
    let kinds: Vec<&str> = shape
        .pipeline
        .iter()
        .map(|step| match step {
            PatternStep::NodeScan { .. } => "scan",
            // The `let`'s column, of a binding the shaper made up for it.
            PatternStep::Let { name, value } => {
                assert!(name.ends_with("_next"), "{name}");
                assert!(value.can_fail(), "{value}");
                "let"
            }
            PatternStep::Expand { .. } => "expand",
            PatternStep::Filter { .. } => "filter",
            other => panic!("unexpected step {other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        ["scan", "let", "expand", "filter", "filter"],
        "{shape:#?}"
    );
}
