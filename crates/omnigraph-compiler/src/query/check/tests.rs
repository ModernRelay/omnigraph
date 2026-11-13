//! The type checker's tests (docs/rfcs/2026-11-07-gq-3.md, section
//! 10, "Type-check tests"), the RFC's examples type-checked, and one repair
//! per type-level refusal shape.

use super::{CheckedDecl, check_source};
use crate::catalog::{Catalog, build_catalog};
use crate::ir::core::*;
use crate::query::syntax::parse::parse_source;
use crate::schema::parser::parse_schema;

/// The schema of the RFC's section 8 in today's `.pg` spelling (a String's
/// full-text index is `@index`), with the section 6 example's revisions and
/// passages.
pub(crate) const SCHEMA: &str = r#"
interface Named { name: String }
node Person implements Named { handle: String @key  name: String  age: I32?  city: String? }
node Company implements Named { slug: String @key  name: String }
node Doc { id: String @key  title: String  body: String @index  year: I32  tags: [String]  embedding: Vector(3)? }
node Collection { slug: String @key  min_docs: I32 }
node SourceRevision { path: String @key  matter_number: String }
node Passage { locator: String @key  text: String @index }
edge WorksAt: Person -> Company @card(0..1)
edge Knows: Person -> Person
edge Authored: Person -> Doc
edge Cites: Doc -> Doc
edge In: Doc -> Collection
edge HasPassage: SourceRevision -> Passage
"#;

pub(crate) fn catalog() -> Catalog {
    build_catalog(&parse_schema(SCHEMA).expect("the schema parses")).unwrap()
}

/// Check `src` against [`SCHEMA`]; one
/// result per declaration.
#[track_caller]
pub(crate) fn check(
    src: &str,
) -> Vec<(
    String,
    Result<CheckedDecl, crate::query::diagnostic::QueryDiagnostic>,
)> {
    check_source(&catalog(), src).unwrap_or_else(|e| panic!("parse: {e}\n{src}"))
}

/// The code of the first refused declaration, or `ok`.
#[track_caller]
pub(crate) fn code(src: &str) -> String {
    for (_, r) in check(src) {
        if let Err(e) = r {
            return e.code.as_str().to_string();
        }
    }
    "ok".into()
}

#[track_caller]
pub(crate) fn ok(src: &str) -> CoreDecl {
    let mut out = None;
    for (n, r) in check(src) {
        match r {
            Ok(c) => out = Some(c.core),
            Err(e) => panic!("`{n}` refused: {e}\n{src}"),
        }
    }
    out.unwrap()
}

#[track_caller]
pub(crate) fn refused(src: &str, want: &str) -> String {
    for (_, r) in check(src) {
        if let Err(e) = r {
            assert_eq!(e.code.as_str(), want, "{e}\n{src}");
            return format!("{e}");
        }
    }
    panic!("accepted, expected {want}:\n{src}");
}

pub(crate) fn read_of(c: &CoreDecl) -> &CoreRead {
    match c {
        CoreDecl::Read(r) => r,
        CoreDecl::Write(_) => panic!("a write"),
    }
}

/// The RFC's examples (sections 3, 6 and 8), each type-checked as written.
#[test]
fn the_rfc_examples_type_check() {
    for text in [
        "query reachable($from: String) {
  match {
    from union(
      { match { $p: Person, $p knows $q } where $p.handle = $from return { $q } },
      { match { from reachable($from), $q knows $r } return { $r as q } }),
  }
  return distinct { $q }
}

query hops($from: String) @budget(rounds = 50) {
  match {
    from union(
      { match { $p: Person, $p knows $q } where $p.handle = $from return { $q, 1 as d } },
      { match { from hops($from), $q knows $r } return { $r as q, $d + 1 as d } }),
  }
  return { $q, min($d) as d }
}",
        "query close_company($slug: String) {
  match { $c: Company, $p $k:worksAt $c }
  where $c.slug = $slug
  delete $k
}

query relocate_staff($slug: String) {
  match { $c: Company, $p worksAt $c }
  where $c.slug = $slug
  update $p set { city: null }
}",
        "query passages($matter: String, $q: String, $k: I64 = 5) {
  match {
    from bm25(
      { match { $r: SourceRevision, $r hasPassage $p } where $r.matter_number = $matter and contains($p.text, \"tariff\")
        return { $r, $p } },
      $p.text, $q),
  }
  return { $r.path, $p.locator, $score }
  order { $score desc }
  limit $k
}",
        "query big_employers() {
  match {
    from { match { $p worksAt $c } return { $c, count($p) as headcount } },
  }
  where $headcount >= 100
  return { $c.name, $headcount }
  order { $headcount desc }
}",
        "query cited_near($coll: String, $q: Vector(3)) {
  match {
    from nearest(
      { match { $d in $x, $x: Collection } where $x.slug = $coll return { $d } },
      $d.embedding, $q, k: 20
    ),
    $d cites $other,
  }
  return { $d.title, $distance, count($other) as cites }
  order { $distance }
}",
        "query retire($h: String) as commit {
  match { $p: Person, $p $k:worksAt $c }
  where $p.handle = $h
  delete $k
}

query relocate($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p set { city: null }
}",
    ] {
        ok(text);
    }
}

/// A block correlates on the variables in scope where it is written
/// (section 3): in `where`, every element's, since the `where` is evaluated
/// after all of them; in a `let`, the elements before it.
#[test]
fn a_block_correlates_on_the_names_in_scope_where_it_is_written() {
    let c = ok(
        "query q() { match { $p: Person, $x: Person } where not exists { $p knows $x } and $x.handle = \"q\" return { $p } }",
    );
    let text = format!("{:?}", read_of(&c).term);
    assert!(
        text.contains("Col { name: \"x\", ty: Node"),
        "the block's Env carries `x`: {text}"
    );
    // Written before `$x` in a `let`, the block sees `$p` alone, and its
    // `$x` is its own.
    let c = ok(
        "query q() { match { $p: Person, let $n = count { $p knows $x }, $x: Person } where $x.handle = \"q\" return { $p, $n } }",
    );
    let text = format!("{:?}", read_of(&c).term);
    assert!(
        text.contains("Env([Col { name: \"p\"")
            && !text.contains("Env([Col { name: \"p\", ty: Node { type_name: \"Person\", nullable: false } }, Col { name: \"x\""),
        "the block's Env carries `p` alone: {text}"
    );
    // A block value compared with a later element's property correlates on
    // the names the block reads.
    let waiting = ok(
        "query q() { match { $p: Person, $c: Collection } where count { $p knows $x } >= $c.min_docs return { $p } }",
    );
    let text = format!("{:?}", read_of(&waiting).term);
    assert!(
        !text.contains("Env([Col { name: \"c\""),
        "the block does not correlate on `$c`: {text}"
    );
}

/// A block re-mentions an outer variable to correlate on it
/// (`not { $p: Person { name: "bob" } }` is "`$p` is not bob"), so
/// a block is never `T68`; a binding inside an inline read that reuses an
/// outer name is.
#[test]
fn a_block_re_mentions_an_outer_variable_to_correlate_on_it() {
    let src = "query q() { match { $p: Person } where not exists { $p: Person where $p.handle = \"bob\" } return { $p } }";
    let rebinding = ok(src);
    // The same term as the filter written inside the block.
    let filter = ok(
        "query q() { match { $p: Person } where not exists { $p: Person where $p.handle = \"bob\" } return { $p } }",
    );
    assert_eq!(read_of(&rebinding).term, read_of(&filter).term);
    // Another type is a rebinding, refused as today.
    refused(
        "query q() { match { $p: Person } where not exists { $p: Company } return { $p } }",
        "T50",
    );
    // `optional` correlates the same way.
    ok(
        "query q() { match { $p: Person, optional { $p: Person, $p worksAt $c where $p.city = \"x\" } } return { $p, $c } }",
    );
    // An inline read may not rebind an outer name.
    refused(
        "query q() { match { $p: Person, from { match { $p: Person } return { $p as q } } } return { $p } }",
        "T68",
    );
}

// ---------------------------------------------------------------------------
// Section 10, "Type-check tests", one test each.

fn cols(c: &CoreDecl) -> Vec<(String, String)> {
    read_of(c)
        .result
        .cols
        .iter()
        .map(|c| (c.name.clone(), c.ty.display()))
        .collect()
}

#[test]
fn t68_on_a_collision_with_each_kind_of_name_in_scope() {
    // A node binding, an edge binding, a `let` value, a `let` relation, a
    // column drawn by an earlier `from`, a parameter, and a ranking's own
    // output column.
    for (kind, src) in [
        (
            "node binding",
            "query q() { match { $c: Company, from { match { $x: Company } return { $x as c } } } return { $c } }",
        ),
        (
            "edge binding",
            "query q() { match { $p $k:worksAt $c, from { match { $x: Company } return { $x as k } } } return { $p } }",
        ),
        (
            "let value",
            "query q() { match { $p: Person, let $n = 1, from { match { $x: Company } return { $x as n } } } return { $p } }",
        ),
        (
            "let relation",
            "query q() { match { let $r = { match { $x: Company } return { $x } }, from { match { $y: Company } return { $y as r } } } return { $y } }",
        ),
        (
            "from column",
            "query q() { match { from { match { $x: Company } return { $x } }, from { match { $y: Company } return { $y as x } } } return { $x } }",
        ),
        (
            "parameter",
            "query q($h: String) { match { from { match { $p: Person } return { $p.handle as h } } } return { $h } }",
        ),
        (
            "ranking output",
            "query q($v: Vector(3)) { match { from distance({ match { $d: Doc } return { $d, $d.year as distance } }, $d.embedding, $v) } return { $d } }",
        ),
        (
            "let reusing a column",
            "query q() { match { $p: Person, let $p = 1 } return { $p } }",
        ),
    ] {
        let m = refused(src, "T68");
        assert!(!m.is_empty(), "{kind}");
    }
    // An inner binding that reuses an outer name: it would silently become a
    // filter on the outer row.
    let m = refused(
        "query q() { match { $p: Person, from { match { $p: Person, $p knows $q } return { $q } } } return { $p, $q } }",
        "T68",
    );
    assert!(m.contains("silently become a filter"), "{m}");
    // A `from` inside an inner read drawing a name the enclosing scope holds.
    refused(
        "query q($v: Vector(3)) { match { let $docs = { match { $d: Doc } return { $d } }, $d: Doc, from { match { from distance($docs, $d.embedding, $v) } return { $d as e } } } return { $e } }",
        "T68",
    );
    // The fix the code prints, an alias on the inner return, is accepted.
    ok(
        "query q() { match { $c: Company, from { match { $x: Company } return { $x as c2 } } } return { $c, $c2 } }",
    );
    // A traversal inside an inner read that names an outer variable is a
    // correlation, not a binding.
    ok(
        "query q() { match { $p: Person, from { match { $p authored $d } return { $d } order { $d.year desc } limit 3 } } return { $p.name, $d.title } }",
    );
}

#[test]
fn t69_both_ways() {
    // A relation where a scalar is read.
    for src in [
        "query q() { match { let $r = { match { $p: Person } return { $p } } } where $r = 1 return { 1 as one } }",
        "query q() { match { let $r = { match { $p: Person } return { $p } }, $x: Person } return { $r } }",
        "query c($h: String) { match { $p: Person } where $p.handle = $h return { $p } }\nquery q() { match { $x: Person } where c($x.handle) return { $x } }",
        "query q() { match { $x: Person } where union({ match { $a: Person } return { $a } }, { match { $b: Person } return { $b as a } }) return { $x } }",
    ] {
        let m = refused(src, "T69");
        assert!(m.contains("relation"), "{m}");
    }
    // A scalar where a relation is read.
    for src in [
        "query q() { match { $p: Person, from $p } return { $p } }",
        "query q() { match { let $v = 1, from $v } return { $v } }",
        "query q($h: String) { match { from $h } return { 1 as one } }",
        "query c($h: String) { match { $p: Person } where $p.handle = $h return { $p } }\nquery q() { match { from c({ match { $x: Person } return { $x.handle as h } }) } return { $p } }",
    ] {
        refused(src, "T69");
    }
}

/// Recursive reads.
pub(crate) const REACHABLE: &str = "query reachable($from: String) {
  match {
    from union(
      { match { $p: Person, $p knows $q } where $p.handle = $from return { $q } },
      { match { from reachable($from), $q knows $r } return { $r as q } }),
  }
  return distinct { $q }
}";

#[test]
fn t70_for_a_non_monotone_cycle_and_a_budget_overrun() {
    // A monotone self-call compiles to `Fix`.
    let r = ok(REACHABLE);
    assert!(matches!(read_of(&r).term, Term::Fix { ref name, .. } if name == "reachable"));
    // `exists` over the recursive rows is admitted, and is the term of
    // `count { } > 0`.
    for form in [
        "exists { from members($from), $m knows $q }",
        "count { from members($from), $m knows $q } > 0",
    ] {
        ok(&format!(
            "query members($from: String) {{ match {{ from union(
               {{ match {{ $p: Person }} where $p.handle = $from return {{ $p as m }} }},
               {{ match {{ $q: Person }} where {form} return {{ $q as m }} }}) }} return distinct {{ $m }} }}"
        ));
    }
    // A `not` over the recursive read.
    let m = refused(
        "query bad($from: String) { match { from union(
           { match { $p: Person } where $p.handle = $from return { $p as m } },
           { match { $q: Person } where not exists { from bad($from), $m knows $q } return { $q as m } }) } return distinct { $m } }",
        "T70",
    );
    assert!(m.contains("not") && m.contains("bad"), "{m}");
    // A `count` over it.
    let m = refused(
        "query bad($from: String) { match { from union(
           { match { $p: Person } where $p.handle = $from return { $p as m } },
           { match { $q: Person } where count { from bad($from), $m knows $q } > 2 return { $q as m } }) } return distinct { $m } }",
        "T70",
    );
    assert!(m.contains("count"), "{m}");
    // An invented value without `min`.
    let m = refused(
        "query dist($from: String) @budget(rounds = 50) { match { from union(
           { match { $p: Person, $p knows $q } where $p.handle = $from return { $q, 1 as d } },
           { match { from dist($from), $q knows $r } return { $r as q, $d + 1 as d } }) } return distinct { $q, $d } }",
        "T70",
    );
    assert!(m.contains("computed column") || m.contains("invent"), "{m}");
    // An invented value under `min` without a round budget.
    let m = refused(
        "query dist($from: String) { match { from union(
           { match { $p: Person, $p knows $q } where $p.handle = $from return { $q, 1 as d } },
           { match { from dist($from), $q knows $r } return { $r as q, $d + 1 as d } }) } return { $q, min($d) as d } }",
        "T70",
    );
    assert!(m.contains("round budget"), "{m}");
    // ... and with one, the RFC's `distance` example compiles.
    let d = ok("query dist($from: String) @budget(rounds = 50) { match { from union(
           { match { $p: Person, $p knows $q } where $p.handle = $from return { $q, 1 as d } },
           { match { from dist($from), $q knows $r } return { $r as q, $d + 1 as d } }) } return { $q, min($d) as d } }");
    assert!(
        matches!(read_of(&d).term, Term::Fix { rounds: Some(50), ref lattice, .. } if lattice == &vec!["d".to_string()])
    );
    // A filter on the `min` column inside the cycle.
    let m = refused(
        "query dist($from: String) @budget(rounds = 50) { match { from union(
           { match { $p: Person, $p knows $q } where $p.handle = $from return { $q, 1 as d } },
           { match { from dist($from), $q knows $r } where $d < 3 return { $r as q, $d + 1 as d } }) } return { $q, max($d) as d } }",
        "T70",
    );
    assert!(m.contains("filter reads the `min`/`max` column"), "{m}");
    // An order and a limit over the recursive rows.
    let m = refused(
        "query cut($from: String) { match { from union(
           { match { $p: Person } where $p.handle = $from return { $p as q } },
           { match { from { match { from cut($from) } return { $q } order { $q.handle } limit 1 }, $q knows $r } return { $r as q } }) } return distinct { $q } }",
        "T70",
    );
    assert!(m.contains("order") || m.contains("limit"), "{m}");
    // A computed recursive argument.
    let m = refused(
        "query down($n: I64) { match { from union(
           { match { $p: Person } return { $p } },
           { match { from down($n - 1) } return { $p } }) } return distinct { $p } }",
        "T70",
    );
    assert!(m.contains("computed argument"), "{m}");
    // A recursion with no base case.
    refused(
        "query loop($h: String) { match { from loop($h) } return distinct { $q } }",
        "T70",
    );
    // A recursion through two declared reads.
    let two = check(
        "query a($from: String) { match { from union(
           { match { $p: Person } where $p.handle = $from return { $p as q } },
           { match { from b($from), $q knows $r } return { $r as q } }) } return distinct { $q } }
         query b($x: String) { match { from a($x) } return distinct { $q } }",
    );
    assert!(two.iter().all(|(_, r)| r.is_ok()), "{two:?}");
    // A budget overrun names the chain.
    let m = refused(
        "query leaf() { match { $p: Person } return { $p } }
         query mid() { match { from union(leaf(), leaf()) } return { $p } }
         query top() @budget(plan = 20) { match { from union(mid(), mid()) } return { $p } }",
        "T70",
    );
    assert!(m.contains("plan budget") && m.contains("mid → leaf"), "{m}");
}

#[test]
fn a_relation_function_over_zero_and_two_node_columns() {
    // Zero node columns: the ranked column is not one.
    let m = refused(
        "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d.title as t } }, $d.embedding, $v, k: 3) } return { $t } }",
        "T79",
    );
    assert!(m.contains("none"), "{m}");
    // A scalar column named as the ranked one.
    refused(
        "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d.title as d } }, $d.embedding, $v, k: 3) } return { $d } }",
        "T79",
    );
    // Two node columns: the call names which (the RFC's `passages` shape);
    // the other rides as payload.
    let c = ok(
        "query q($s: String) { match { from bm25({ match { $p authored $d } return { $p, $d } }, $d.body, $s) } return { $p.name, $d.title, $score } }",
    );
    assert_eq!(
        cols(&c),
        vec![
            ("p.name".to_string(), "String".to_string()),
            ("d.title".to_string(), "String".to_string()),
            ("score".to_string(), "F32".to_string()),
        ]
    );
    // A bare field name is refused at its word, and the suggestion names the
    // one binding in scope.
    let e = parse_source(
        "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, embedding, $v, k: 3) } return { $d } }",
    )
    .unwrap_err();
    assert_eq!(e.code.as_str(), "Q018", "{e}");
    assert_eq!(e.fix.as_deref(), Some("$d.embedding"), "{e:?}");
}

#[test]
fn a_ranked_column_whose_property_is_not_declared() {
    refused(
        "query q($s: String) { match { from bm25({ match { $d: Doc } return { $d } }, $d.abstract, $s) } return { $d } }",
        "T27",
    );
    refused(
        "query q($v: Vector(3)) { match { from distance({ match { $d: Doc } return { $d } }, $d.vec, $v) } return { $d } }",
        "T15",
    );
    // A declared property without a full-text index.
    refused(
        "query q($s: String) { match { from bm25({ match { $d: Doc } return { $d } }, $d.title, $s) } return { $d } }",
        "T27",
    );
    // A query vector of the wrong dimension.
    refused(
        "query q() { match { from distance({ match { $d: Doc } return { $d } }, $d.embedding, [1.0, 2.0]) } return { $d } }",
        "T15",
    );
}

#[test]
fn nearest_without_k() {
    let m = refused(
        "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v) } return { $d } }",
        "T17",
    );
    assert!(m.contains("law 3"), "{m}");
    refused(
        "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v, k: 0) } return { $d } }",
        "T71",
    );
    refused(
        "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v, k: 5, recall: 1.5) } return { $d } }",
        "T71",
    );
    refused(
        "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v, k: 5, candidates: 50) } return { $d } }",
        "T71",
    );
    // `k` from a parameter is a window, never `limit`'s value.
    ok(
        "query q($v: Vector(3), $k: I64) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v, k: $k) } return { $d.title, $distance } limit 3 }",
    );
}

#[test]
fn t65_with_the_from_fix() {
    for (src, fix) in [
        (
            "query q($v: Vector(3)) { match { $d: Doc } return { $d.title } order { nearest($d.embedding, $v) } limit 3 }",
            "from nearest(",
        ),
        (
            "query q($s: String) { match { $d: Doc } return { $d.title, bm25($d.body, $s) as s } }",
            "from bm25(",
        ),
        (
            "query q($s: String, $v: Vector(3)) { match { $d: Doc } return { $d.title } order { rrf(nearest($d.embedding, $v), bm25($d.body, $s), 60) } limit 3 }",
            "from rrf(",
        ),
        (
            "query q($v: Vector(3)) { match { $d: Doc } where distance($d.embedding, $v) < 1.0 return { $d } }",
            "from distance(",
        ),
    ] {
        let e = check(src);
        let e = e[0].1.as_ref().unwrap_err();
        assert_eq!(e.code.as_str(), "T65", "{src}");
        assert!(e.fix.as_deref().unwrap().contains(fix), "{e:?}");
    }
}

#[test]
fn t40_lifted() {
    // A block value compares with any scalar in scope.
    let src = "query full_collections() { match { $x: Collection } where count { $d in $x } >= $x.min_docs return { $x.slug } }";
    ok(src);
    // It is a value: bound, returned and ordered.
    let c = ok(
        "query degrees($min: I64) { match { $p: Person, let $degree = count { $p knows $q } } where $degree >= $min return { $p.name, $degree } order { $degree desc } }",
    );
    assert_eq!(cols(&c)[1], ("degree".to_string(), "I64".to_string()));
    // `sum` over an empty block is null; `count` is not.
    let c = ok(
        "query s() { match { $p: Person, let $y = sum($d.year) { $p authored $d } } return { $y } }",
    );
    assert_eq!(cols(&c)[0].1, "F64?");
    // A block that reads no outer variable stays refused, as today.
    refused(
        "query q() { match { $p: Person } where count { $d: Doc } > 1 return { $p } }",
        "T39",
    );
}

#[test]
fn optional_served() {
    let c = ok(
        "query q() { match { $p: Person, optional { $p worksAt $c } } return { $p.name, $c.name, $c } }",
    );
    assert_eq!(
        cols(&c),
        vec![
            ("p.name".to_string(), "String".to_string()),
            ("c.name".to_string(), "String?".to_string()),
            ("c".to_string(), "node Company?".to_string()),
        ]
    );
    let mut ops = Vec::new();
    read_of(&c).term.operators(&mut ops);
    assert!(ops.contains(&"Union"), "{ops:?}");
}

#[test]
fn if_commit_and_valid_are_refused_with_the_writes_rfc_named() {
    for a in ["@if_commit($h)", "@valid($h)"] {
        let message = refused(
            &format!(
                "query r($h: String) {a} {{ match {{ $p: Person }} where $p.handle = $h delete $p }}"
            ),
            "T80",
        );
        assert!(message.contains("writes RFC"), "{message}");
    }
}

/// Section 10 says "an unknown annotation retained"; section 5 and the
/// freeze list refuse every name but the metadata allow-list, and the test
/// follows section 5.
#[test]
fn an_unknown_annotation_is_refused_and_metadata_is_retained() {
    ok(
        "query q() @description(\"d\") @example(\"e\") @rename_from(\"old\") { match { $p: Person } return { $p } }",
    );
    let m = refused(
        "query q() @cache(ttl = 5) { match { $p: Person } return { $p } }",
        "T80",
    );
    assert!(m.contains("@description"), "{m}");
    ok("query q() @budget(plan = 100) { match { $p: Person } return { $p } }");
    let m = refused(
        "query q() @budget(rows = 10) { match { $p: Person } return { $p } }",
        "T80",
    );
    assert!(m.contains("stage (N)"), "{m}");
}

// ---------------------------------------------------------------------------
// The rest of the brief's typing rules.

#[test]
fn arithmetic_widens_and_propagates_null() {
    let ty = |e: &str| {
        let c = ok(&format!(
            "query q($i: I32, $l: I64, $f: F32, $g: F64, $u: U32) {{ match {{ $p: Person }} return {{ {e} as v }} }}"
        ));
        cols(&c)[0].1.clone()
    };
    assert_eq!(ty("$i + $i"), "I32");
    assert_eq!(ty("$i + $l"), "I64");
    assert_eq!(
        ty("$i * 2"),
        "I32",
        "an integer literal takes the other operand's width"
    );
    assert_eq!(ty("$i + $f"), "F64");
    assert_eq!(ty("$f * $f"), "F32");
    assert_eq!(ty("$f + $g"), "F64");
    assert_eq!(
        ty("$f * 0.5"),
        "F64",
        "a float literal is F64, so it widens an F32"
    );
    assert_eq!(ty("$p.age + 1"), "I32?", "nullable when an operand is");
    assert_eq!(ty("-$l % 3"), "I64");
    assert_eq!(
        ty("$u + $i"),
        "I64",
        "mixed signedness: the RFC states no rule"
    );
    for e in ["$p.name + 1", "$p + 1", "-$p.name", "$p.age + true"] {
        refused(
            &format!("query q() {{ match {{ $p: Person }} return {{ {e} as v }} }}"),
            "T72",
        );
    }
    refused(
        "query q($u: U64, $i: I32) { match { $p: Person } return { $u + $i as v } }",
        "T72",
    );
    // The term carries `can_fail`, so the planner keeps it behind its guards.
    let c = ok("query q() { match { $p: Person } where $p.age * 2 >= 10 return { $p } }");
    fn find_filter(t: &Term) -> Option<&crate::ir::expr::IRExpr> {
        match t {
            Term::Filter { pred, .. } => Some(pred),
            Term::Project { input, .. } | Term::Order { input, .. } | Term::Cut { input, .. } => {
                find_filter(input)
            }
            _ => None,
        }
    }
    assert!(find_filter(&read_of(&c).term).unwrap().can_fail());
}

#[test]
fn the_edge_binding_scans_an_edge_type() {
    let c =
        ok("query q($h: String) { match { $k: WorksAt } where $k.@src = $h return { $k.@dst } }");
    let mut ops = Vec::new();
    read_of(&c).term.operators(&mut ops);
    assert_eq!(ops[..3], ["Project", "Project", "Filter"]);
    assert!(matches!(&read_of(&c).term, Term::Project { input, .. }
        if matches!(input.as_ref(), Term::Project { input, .. }
            if matches!(input.as_ref(), Term::Filter { input, .. }
                if matches!(input.as_ref(), Term::Scan { ty: Ty::Edge { .. }, .. })))));
}

#[test]
fn the_ranking_functions_type_their_result_columns() {
    let c = ok(
        "query q($v: Vector(3)) { match { from distance({ match { $d: Doc } return { $d } }, $d.embedding, $v) } return { $d, $distance } }",
    );
    assert_eq!(cols(&c)[1], ("distance".into(), "F32".into()));
    let c = ok(
        "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v, k: 5, recall: 0.9) } return { $distance } }",
    );
    assert_eq!(cols(&c)[0], ("distance".into(), "F32".into()));
    assert!(format!("{:?}", read_of(&c).term).contains("recall: Some(0.9)"));
    let c = ok(
        "query q($s: String) { match { from bm25({ match { $d: Doc } return { $d } }, $d.body, terms($s, mode: all, max_edits: 1)) } return { $score } }",
    );
    assert_eq!(cols(&c)[0], ("score".into(), "F32".into()));
    // `rrf` yields the target and `score` only: the population's other
    // columns are gone after fusion.
    let fused = "let $docs = { match { $p authored $d } return { $p, $d } },
                 from rrf(nearest($docs, $d.embedding, $v, k: 50), bm25($docs, $d.body, $s), k: 60, weights: [2.0, 1.0])";
    let c = ok(&format!(
        "query q($s: String, $v: Vector(3)) {{ match {{ {fused} }} return {{ $d.title, $score }} }}"
    ));
    assert_eq!(
        cols(&c),
        vec![
            ("d.title".into(), "String".into()),
            ("score".into(), "F64".into())
        ]
    );
    refused(
        &format!(
            "query q($s: String, $v: Vector(3)) {{ match {{ {fused} }} return {{ $p.name }} }}"
        ),
        "T6",
    );
    // ... and a correlated `from` over the population draws them again.
    ok(&format!(
        "query q($s: String, $v: Vector(3)) {{ match {{ {fused}, from {{ match {{ $p authored $d }} return {{ $p }} }} }} return {{ $p.name, $score }} }}"
    ));
}

#[test]
fn rrf_refusals() {
    let pop = "let $docs = { match { $d: Doc } return { $d } }";
    for (opts, arms) in [
        (
            "",
            "nearest($docs, $d.embedding, $v, k: 5), nearest($docs, $d.embedding, $v, k: 5)",
        ),
        ("", "nearest($docs, $d.embedding, $v, k: 5)"),
        (
            ", weights: [1.0]",
            "nearest($docs, $d.embedding, $v, k: 5), bm25($docs, $d.body, $s)",
        ),
        (
            "",
            "nearest($docs, $d.embedding, $v, k: 5), { match { $p: Person } return { $p } }",
        ),
        (
            "",
            "nearest($docs, $d.embedding, $v, k: 5), { match { $p: Person } return { $p } order { $p.age } }",
        ),
    ] {
        refused(
            &format!(
                "query q($s: String, $v: Vector(3)) {{ match {{ {pop}, from rrf({arms}{opts}) }} return {{ $d }} }}"
            ),
            "T75",
        );
    }
    for opts in [", k: 0", ", weights: [1.0, 0.0]", ", depth: 2"] {
        refused(
            &format!(
                "query q($s: String, $v: Vector(3)) {{ match {{ {pop}, from rrf(nearest($docs, $d.embedding, $v, k: 5), bm25($docs, $d.body, $s){opts}) }} return {{ $d }} }}"
            ),
            "T71",
        );
    }
    // Two arms over one population with different queries are distinct arms;
    // an ordered inline read is an arm.
    ok(&format!(
        "query q($s: String, $t: String, $v: Vector(3)) {{ match {{ {pop}, from rrf(bm25($docs, $d.body, $s), bm25($docs, $d.body, $t), {{ match {{ $d: Doc }} return {{ $d }} order {{ $d.year desc }} limit 10 }}) }} return {{ $d, $score }} }}"
    ));
}

#[test]
fn union_agrees_by_position_and_widens_nullability() {
    let c = ok(
        "query q() { match { from union({ match { $p: Person } return { $p.city as c } }, { match { $x: Company } return { $x.name as c } }) } return { $c } }",
    );
    assert_eq!(cols(&c)[0], ("c".into(), "String?".into()));
    let m = refused(
        "query q() { match { from union({ match { $p: Person } return { $p.age as c } }, { match { $x: Company } return { $x.name as c } }) } return { $c } }",
        "T73",
    );
    assert!(m.contains("arm 2 column 1"), "{m}");
    refused(
        "query q() { match { from union({ match { $p: Person } return { $p } }, { match { $x: Company } return { $x as p } }) } return { $p } }",
        "T73",
    );
    refused(
        "query q() { match { from union({ match { $p: Person } return { $p, $p.age as a } }, { match { $x: Person } return { $x as p } }) } return { $p } }",
        "T73",
    );
    // A flat union and a nested one are one term (the formatter flattens).
    let flat = ok(
        "query q() { match { from union({ match { $p: Person } return { $p } }, { match { $q: Person } return { $q as p } }, { match { $r: Person } return { $r as p } }) } return { $p } }",
    );
    let nested = ok(
        "query q() { match { from union(union({ match { $p: Person } return { $p } }, { match { $q: Person } return { $q as p } }), { match { $r: Person } return { $r as p } }) } return { $p } }",
    );
    assert_eq!(read_of(&flat).term, read_of(&nested).term);
}

#[test]
fn a_call_binds_its_arguments_and_never_substitutes() {
    // `friends_of`'s body binds `$p`; the caller passes `$p.handle` from a
    // scope that also binds `$p`. Substitution would make the body's binding
    // `$p: Person { handle: $p.handle }`, every person; binding keeps the
    // body closed.
    let unit = "query friends_of($h: String) { match { $p: Person, $p knows $q } where $p.handle = $h return { $q } }
                query q() { match { $p: Person, from friends_of($p.handle) } where $p.handle = \"erin\" return { $q } }";
    let all = check(unit);
    let CoreDecl::Read(r) = &all[1].1.as_ref().unwrap().core else {
        panic!()
    };
    let text = format!("{:?}", r.term);
    assert!(text.contains("Call { name: \"friends_of\", bindings: [(\"h\", PropAccess { variable: \"p\", property: \"handle\""), "{text}");
    assert!(
        text.contains("Param(\"h\", "),
        "the body reads its parameter, not the argument: {text}"
    );
    // A declared read may not shadow a registry name or a clause keyword.
    refused(
        "query nearest() { match { $p: Person } return { $p } }",
        "T74",
    );
    refused(
        "query count() { match { $p: Person } return { $p } }",
        "T74",
    );
    // A read cannot draw from a write (A7), and a write calls no declared
    // read until stage W2.
    refused(
        "query w($h: String) { insert Person { handle: $h, name: $h } }\nquery q() { match { from w(\"a\") } return { 1 as one } }",
        "T74",
    );
    let m = refused(
        "query r() { match { $p: Person } return { $p } }\nquery w() { match { from r() }  update $p set { city: null } }",
        "T74",
    );
    assert!(m.contains("W2"), "{m}");
    // Arity and argument types.
    refused(
        "query f($h: String) { match { $p: Person } where $p.handle = $h return { $p } }\nquery q() { match { from f() } return { $p } }",
        "T7",
    );
    refused(
        "query f($h: String) { match { $p: Person } where $p.handle = $h return { $p } }\nquery q() { match { from f(3) } return { $p } }",
        "T7",
    );
    // A default fills an omitted trailing argument.
    ok(
        "query f($h: String, $n: I64 = 3) { match { $p: Person } where $p.handle = $h return { $p } limit $n }\nquery q() { match { from f(\"a\") } return { $p } }",
    );
}

/// A call resolves against the declarations of its source unit,
/// case-sensitively, exactly one: two declarations of one name are `T83`,
/// refused before any declaration is checked, naming both and renaming the
/// later one.
#[test]
fn t83_names_both_declarations_and_renames_the_later() {
    use crate::query::diagnostic::Applicability;
    use crate::query::syntax::parse::apply_edits;
    let written = "query f($h: String) { match { $p: Person } where $p.handle = $h return { $p } }\n\
                   query q() { match { from f(\"a\") } return { $p.name } }\n\
                   query f($h: String) { match { $p: Person } where $p.name = $h return { $p } }";
    let d = check_source(&catalog(), written).expect_err("refused at the front door");
    assert_eq!(d.code.as_str(), "T83", "{d}");
    assert_eq!(
        d.to_string(),
        "type error: T83: `f` is declared twice in one source, at line 1, column 7 and at line 3, column 7; a call or a request resolves a name to exactly one declaration"
    );
    let at = d.position.expect("the later declaration's position");
    assert_eq!((at.line, at.column), (3, 7));
    assert_eq!(&written[at.byte as usize..at.byte as usize + 1], "f");
    assert_eq!(
        d.fix.as_deref(),
        Some("rename the later `f` (line 3, column 7) to `f_2`, or delete that declaration")
    );
    let suggestion = d.suggestion.expect("the rename");
    assert_eq!(suggestion.applicability, Applicability::NeedsReview);
    let repaired = apply_edits(written, &suggestion.edits);
    assert!(
        repaired.ends_with(
            "query f_2($h: String) { match { $p: Person } where $p.name = $h return { $p } }"
        ),
        "{repaired}"
    );
    // Renamed, the call resolves to the one `f` left.
    assert!(
        check(&repaired).iter().all(|(_, r)| r.is_ok()),
        "{repaired}"
    );

    // The fresh name is one no declaration of the source uses, and a third
    // declaration is refused at the first repeat.
    let d = check_source(
        &catalog(),
        "query g() { match { $p: Person } return { $p } }\nquery g_2() { match { $p: Person } return { $p } }\nquery g() { match { $p: Person } return { $p } }\nquery g() { match { $p: Person } return { $p } }",
    )
    .expect_err("refused");
    assert_eq!(d.position.map(|at| at.line), Some(3), "{d}");
    assert_eq!(d.suggestion.unwrap().edits[0].replacement, "g_3");

    // Names compare case-sensitively, as a call resolves them.
    ok(
        "query byName() { match { $p: Person } return { $p } }\nquery byname() { match { $p: Person } return { $p } }",
    );
}

#[test]
fn a_column_drawn_from_must_be_variable_shaped() {
    let e = check(
        "query q() { match { from { match { $p: Person } return { $p.name } } } return { 1 as one } }",
    );
    let e = e[0].1.as_ref().unwrap_err();
    assert_eq!(e.code.as_str(), "T74");
    assert!(
        e.message.contains("`p.name`") && e.fix.as_deref().unwrap().contains("as p_name"),
        "{e:?}"
    );
}

// ---------------------------------------------------------------------------
// Writes.

fn write_of(c: &CoreDecl) -> &CoreWrite {
    match c {
        CoreDecl::Write(w) => w,
        CoreDecl::Read(_) => panic!("a read"),
    }
}

#[test]
fn write_groups_type_their_sinks() {
    let w = ok(
        "query close_company($slug: String) { match { $c: Company, $p $k:worksAt $c } where $c.slug = $slug  delete $k }",
    );
    let w = write_of(&w);
    assert_eq!(w.class, WriteClass::Destructive);
    assert!(
        matches!(&w.groups[0].sinks[0], Sink::Delete { var, target } if var == "k" && target.is_edge())
    );
    let w = ok(
        "query relocate($h: String) { match { $p: Person } where $p.handle = $h  update $p set { city: null, age: $p.age + 1 } }",
    );
    let w = write_of(&w);
    assert!(
        matches!(&w.groups[0].sinks[0], Sink::Update { assignments, .. } if assignments[0].1 == AssignValue::Null)
    );
    // `insert` with no clauses runs once over the one empty row.
    let w = ok(
        "query hire($h: String, $slug: String) { insert Person { handle: $h, name: $h }  insert WorksAt { from: $h, to: $slug } }",
    );
    assert_eq!(write_of(&w).groups[0].input, Term::Unit);
    // An endpoint by bound reference, and the mismatch.
    ok(
        "query link($a: String, $b: String) { match { $x: Person, $y: Person } where $x.handle = $a and $y.handle = $b  insert Knows { from: $x, to: $y } }",
    );
    let m = refused(
        "query link($a: String, $b: String) { match { $x: Person, $d: Doc } where $x.handle = $a and $d.id = $b  insert Knows { from: $x, to: $d } }",
        "T78",
    );
    assert!(
        m.contains("`to` of `Knows` is a `Person`") && m.contains("node Doc"),
        "{m}"
    );
    // A write fed by a ranked read, a `let` and an `optional`.
    ok(
        "query tag_near($q: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $q, k: 20) }  update $d set { tags: [\"near\"] } }",
    );
    ok(
        "query w($h: String) { match { $p: Person, let $n = count { $p knows $q }, optional { $p worksAt $c } } where $p.handle = $h and $n > 1  update $p set { city: \"busy\" } }",
    );
    // Groups: a later group sees the earlier ones' effects; each has its own
    // clauses.
    let w = ok(
        "query two($h: String) { insert Person { handle: $h, name: $h }  match { $p: Person } where $p.handle = $h  update $p set { city: \"x\" } }",
    );
    assert_eq!(write_of(&w).groups.len(), 2);
    // `null` only where the property is nullable.
    refused(
        "query w($h: String) { match { $p: Person } where $p.handle = $h  update $p set { name: null } }",
        "T3",
    );
    // A statement's variable must be a bound reference.
    refused("query w() { update $p set { city: null } }", "T77");
    refused(
        "query w() { match { $p: Person, let $n = 1 }  delete $n }",
        "T77",
    );
    // Update of an edge is refused, as today (T16).
    refused(
        "query w() { match { $p $k:worksAt $c }  update $k set { since: 1 } }",
        "T16",
    );
}

#[test]
fn the_constructive_or_destructive_split_and_ordered_statements() {
    let m = refused(
        "query retire($h: String) { match { $p: Person, $p $k:worksAt $c } where $p.handle = $h  delete $k  update $p set { city: null } }",
        "T76",
    );
    assert!(m.contains("split it into separate mutations"), "{m}");
    // Split across groups too: the split is per write.
    refused(
        "query w($h: String) { match { $p: Person } where $p.handle = $h  delete $p  insert Person { handle: $h, name: $h } }",
        "T76",
    );
    // A group's statements run in order, the later effect winning: two
    // values for one entity across statements, and two upserts of one key,
    // are legal as today. A conflict through one statement's rows is found
    // only by running it (the `E` namespace).
    ok(
        "query w($h: String) { match { $p: Person } where $p.handle = $h  update $p set { city: \"a\" }  update $p set { city: \"b\" } }",
    );
    ok(
        "query w() { insert Person { handle: \"a\", name: \"x\" }  insert Person { handle: \"a\", name: \"y\" } }",
    );
}

#[test]
fn as_commit_is_admitted_only_on_a_write_body() {
    let w = ok(
        "query retire($h: String) as commit { match { $p: Person, $p $k:worksAt $c } where $p.handle = $h  delete $k }",
    );
    assert_eq!(
        write_of(&w).write_mode,
        crate::query::file::MutationWriteMode::AsCommit
    );
    let e = parse_source("query q() as commit { match { $p: Person } return { $p } }").unwrap_err();
    assert_eq!(e.code.as_str(), "Q011");
}

/// The probes the RFC's reference interpreter of the core was checked
/// against, with the verdict section 2 gives each.
#[test]
fn the_reference_interpreter_probes() {
    let dist = |ret: &str, budget: &str, filter: &str| {
        format!(
            "query dist($from: String) {budget} {{ match {{ from union(
               {{ match {{ $p: Person, $p knows $q }} where $p.handle = $from return {{ $q, 1 as d }} }},
               {{ match {{ from dist($from), $q knows $r }} {filter} return {{ $r as q, $d + 1 as d }} }}) }} return {{ {ret} }} }}"
        )
    };
    let probes: Vec<(&str, String, &str)> = vec![
        ("inner_rebinding_in_not", "query x() { match { $p: Person } where not exists { $p: Person where $p.handle = \"bob\" } return { $p.name } }".into(), "ok"),
        ("future_name_in_block", "query x() { match { $p: Person, let $n = count { $p knows $x }, $x: Person } where $x.handle = \"q\" and $n = 0 return { $p.name } }".into(), "ok"),
        ("from_collision", "query x() { match { $c: Company, from { match { $x: Company } return { $x as c } } } return { $c } }".into(), "T68"),
        ("union_mismatch", "query x() { match { from union({ match { $p: Person } return { $p.age as c } }, { match { $x: Company } return { $x.name as c } }) } return { $c } }".into(), "T73"),
        ("union_nullable", "query x() { match { from union({ match { $p: Person } return { $p.city as c } }, { match { $x: Company } return { $x.name as c } }) } return { $c } }".into(), "ok"),
        ("not_over_recursion", "query bad($from: String) { match { from union({ match { $p: Person } where $p.handle = $from return { $p as m } }, { match { $q: Person } where not exists { from bad($from), $m knows $q } return { $q as m } }) } return distinct { $m } }".into(), "T70"),
        ("exists_over_recursion", "query members($from: String) { match { from union({ match { $p: Person } where $p.handle = $from return { $p as m } }, { match { $q: Person } where exists { from members($from), $m knows $q } return { $q as m } }) } return distinct { $m } }".into(), "ok"),
        ("computed_recursive_arg", "query down($n: I64) { match { from union({ match { $p: Person } return { $p } }, { match { from down($n - 1) } return { $p } }) } return distinct { $p } }".into(), "T70"),
        ("invented_value_without_min", dist("$q, $d", "@budget(rounds = 50)", "").replace("return { $q, $d }", "return distinct { $q, $d }"), "T70"),
        ("min_without_rounds", dist("$q, min($d) as d", "", ""), "T70"),
        ("min_with_rounds", dist("$q, min($d) as d", "@budget(rounds = 50)", ""), "ok"),
        ("filter_on_max_column", dist("$q, max($d) as d", "@budget(rounds = 50)", "where $d < 3"), "T70"),
        ("age_times_two_into_age", "query w($h: String) { match { $p: Person } where $p.handle = $h  update $p set { age: $p.age * 2 } }".into(), "ok"),
        ("update_and_delete_one_write", "query w($h: String) { match { $p: Person, $p $k:worksAt $c } where $p.handle = $h  delete $k  update $p set { city: null } }".into(), "T76"),
    ];
    for (name, src, want) in probes {
        assert_eq!(code(&src), want, "{name}\n{src}");
    }
}

/// Section 3's refused respellings that need a type: `$d in $x` in `where`
/// with `$x` a node is a triple, written in `match` (`T7`); `$d in $list`
/// in `match` with a list on the right is the membership test, moved to
/// `where` (`T7`); `contains` over a list is refused with the `in` fix.
#[test]
fn in_over_the_wrong_kind_and_contains_over_a_list_are_type_errors() {
    let e = check("query q() { match { $x: Collection, $d: Doc } where $d in $x return { $d } }");
    let e = e[0].1.as_ref().unwrap_err();
    assert_eq!(e.code.as_str(), "T7");
    assert_eq!(e.fix.as_deref(), Some("match { …, $d in $x }"));
    ok("query q() { match { $x: Collection, $d in $x } return { $d } }");
    for (written, intended) in [
        (
            "query q($tags: [String]) { match { $d: Doc, $d in $tags } return { $d } }",
            "query q($tags: [String]) { match { $d: Doc } where $d in $tags return { $d } }",
        ),
        (
            "query q($tags: [String]) { match { $d in $tags, $d: Doc } where $d.year > 1 return { $d } }",
            "query q($tags: [String]) { match { $d: Doc } where $d.year > 1 and $d in $tags return { $d } }",
        ),
    ] {
        let d = refused_with(written, "T7");
        let edits = d.suggestion.expect("an edit").edits;
        let repaired = crate::query::syntax::parse::apply_edits(written, &edits);
        assert_eq!(
            parse_source(&repaired).unwrap(),
            parse_source(intended).unwrap(),
            "{repaired}"
        );
    }
    let e =
        check("query q() { match { $d: Doc } where contains($d.tags, \"rust\") return { $d } }");
    let e = e[0].1.as_ref().unwrap_err();
    assert_eq!(
        (e.code.as_str(), e.fix.as_deref()),
        ("T7", Some("<value> in <list>"))
    );
}

/// A population reused after its column name is bound collides (`T68`);
/// renaming it at `let` level, before `$d` is bound, is a legal spelling.
#[test]
fn weighted_type_checks_with_the_population_renamed_before_d_is_bound() {
    ok("query weighted($q: String, $v: Vector(3)) {
  match {
    let $docs = { match { $d: Doc } return { $d } },
    let $docs2 = { match { from $docs } return { $d as d2 } },
    from { match { from distance($docs, $d.embedding, $v) } return { $d, $distance as vec } },
    from { match { from bm25($docs2, $d2.body, $q) } return { $d2, $score as lex } },
  }
  where $d = $d2
  return { $d.title, 0.7 * (1.0 / (1.0 + $vec)) + 0.3 * $lex as s }
  order { s desc }
  limit 10
}");
}

/// `(shape, code, refused text, intended text)`: the type-level refusals
/// whose suggestion is an edit, each reparsing to the intended text.
const TYPE_REPAIRS: &[(&str, &str, &str, &str)] = &[
    (
        r#"in over a list inside match"#,
        "T7",
        r#"query q($names: [String]) {
  match { $p: Person, let $n = $p.name, $n in $names }
  return { $p.name }
}"#,
        r#"query q($names: [String]) { match { $p: Person, let $n = $p.name } where $n in $names return { $p.name } }"#,
    ),
    (
        r#"infix contains over a list"#,
        "T7",
        r#"query q() {
  match { $d: Doc } where $d.tags contains "rust"
  return { $d.title }
}"#,
        r#"query q() { match { $d: Doc } where "rust" in $d.tags return { $d.title } }"#,
    ),
    (
        r#"infix contains over a String"#,
        "T7",
        r#"query q() {
  match { $d: Doc } where $d.title contains "graph"
  return { $d.title }
}"#,
        r#"query q() { match { $d: Doc } where contains($d.title, "graph") return { $d.title } }"#,
    ),
    (
        r#"infix starts_with"#,
        "T7",
        r#"query q() {
  match { $p: Person } where $p.name starts_with "A"
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where starts_with($p.name, "A") return { $p.name } }"#,
    ),
    (
        r#"an unknown annotation"#,
        "T80",
        r#"query q() @owner("search-team") {
  match { $p: Person }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
];

#[test]
fn every_type_refusal_shape_prints_a_repair_that_reparses_to_the_intended_text() {
    for (shape, code, refused, intended) in TYPE_REPAIRS {
        let source = refused.to_string();
        let file = parse_source(&source).unwrap_or_else(|e| panic!("{shape}: {e}"));
        let diagnostic = super::check_file(&catalog(), &file)
            .into_iter()
            .find_map(|(_, r)| r.err())
            .unwrap_or_else(|| panic!("{shape}: accepted"));
        assert_eq!(diagnostic.code.as_str(), *code, "{shape}: {diagnostic}");
        let suggestion = diagnostic
            .suggestion
            .as_ref()
            .unwrap_or_else(|| panic!("{shape}: no suggestion: {diagnostic}"));
        let repaired = crate::query::syntax::parse::apply_edits(&source, &suggestion.edits);
        let got = parse_source(&repaired)
            .unwrap_or_else(|e| panic!("{shape}: the repair does not parse: {e}\n{repaired}"));
        let want = parse_source(intended).expect("the intended text parses");
        assert_eq!(got, want, "{shape}: repaired to\n{repaired}");
    }
}

// ---------------------------------------------------------------------------
// Diagnostics an agent repairs from: each names the construct, carries a fix
// that type-checks, and points at where the text was written.

/// The one refusal of `src`.
#[track_caller]
fn refused_with(src: &str, code: &str) -> crate::query::diagnostic::QueryDiagnostic {
    let mut out = check(src);
    let (_, outcome) = out.remove(0);
    let d = outcome.expect_err("refused");
    assert_eq!(d.code.as_str(), code, "{d}");
    d
}

/// `T74` names the return item that produced the column it refuses, and its
/// fix aliases that item.
#[test]
fn t74_names_the_return_item_behind_an_unshaped_column() {
    let d = refused_with(
        "query q() { match { from { match { $p: Person, $p worksAt $c } return { $c, count($p) } } } return { $c } }",
        "T74",
    );
    assert!(d.message.contains("`count($p)`"), "{d}");
    assert_eq!(
        d.fix.as_deref(),
        Some("alias it on the inner `return`: `count($p) as count_p`")
    );
    let fixed = "query q() { match { from { match { $p: Person, $p worksAt $c } return { $c, count($p) as count_p } } } return { $c, $count_p } }";
    ok(fixed);
}

/// `T8` for an aggregate in a filter prints the block form, which
/// type-checks; over the read's own rows it prints the `return` item.
#[test]
fn t8_prints_the_block_an_aggregate_filter_means() {
    let d = refused_with(
        "query q() { match { $p: Person, $p knows $f } where count($f) > 3 return { $p.name } }",
        "T8",
    );
    let fix = d.fix.expect("a fix");
    assert_eq!(
        fix,
        "match { $p: Person } where count($f) { $p knows $f } > 3"
    );
    ok(&format!("query q() {{ {fix} return {{ $p.name }} }}"));
    let d = refused_with(
        "query q() { match { $p: Person } where count($p) > 3 return { $p.name } }",
        "T8",
    );
    assert_eq!(d.fix.as_deref(), Some("return { count($p) as count_p }"));
}

/// `T65`'s suggestion draws the ranking with `from` over the read's own
/// pattern; applied, the text type-checks to the form the rewriter writes.
#[test]
fn t65_suggests_an_applicable_edit() {
    use crate::query::syntax::parse::apply_edits;
    for (written, intended) in [
        (
            "query q($v: Vector(3)) { match { $d: Doc } return { $d.title } order { nearest($d.embedding, $v) } limit 3 }",
            "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v, k: 3) } return { $d.title } order { $distance } limit 3 }",
        ),
        (
            "query q($v: Vector(3)) { match { $d: Doc } where $d.year > 2000 return { $d.title } order { nearest($d.embedding, $v) } limit 3 }",
            "query q($v: Vector(3)) { match { from nearest({ match { $d: Doc } where $d.year > 2000 return { $d } }, $d.embedding, $v, k: 3) } return { $d.title } order { $distance } limit 3 }",
        ),
        (
            "query q($s: String) { match { $p: Person, $p authored $d } return { $d.title, bm25($d.body, $s) } order { bm25($d.body, $s) } limit 3 }",
            "query q($s: String) { match { from bm25({ match { $p: Person, $p authored $d } return { $p, $d } }, $d.body, $s) } return { $d.title, $score as _score } order { $score desc } limit 3 }",
        ),
        (
            "query q($s: String, $v: Vector(3)) { match { $d: Doc } return { $d.title } order { rrf(nearest($d.embedding, $v), bm25($d.body, $s)) } limit 3 }",
            "query q($s: String, $v: Vector(3)) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $v, k: 3), bm25($all, $d.body, $s), k: 60), $d: Doc } return { $d.title } order { $score desc } limit 3 }",
        ),
    ] {
        let d = refused_with(written, "T65");
        let suggestion = d.suggestion.expect("an edit");
        assert_eq!(
            suggestion.applicability,
            crate::query::diagnostic::Applicability::MachineApplicable
        );
        let repaired = apply_edits(written, &suggestion.edits);
        assert_eq!(
            parse_source(&repaired).unwrap(),
            parse_source(intended).unwrap(),
            "{repaired}"
        );
        ok(&repaired);
        assert!(d.position.is_some(), "{written}");
    }
}

/// A literal compared with a property is checked against the property's
/// type in every scope that compares, either operand order, and in an `in`
/// list: `T3`, its edit writing the nearest allowed value where the literal
/// was, and the repaired text type-checks.
#[test]
fn t3_refuses_a_literal_outside_its_property_with_the_nearest_value() {
    use crate::query::diagnostic::Applicability;
    use crate::query::syntax::parse::apply_edits;
    const LITERAL_SCHEMA: &str = r#"
node Person { slug: String @key  status: enum(active, retired)  former: enum(active, retired)?  tags: [String]  age: I32  rank: U32  weight: F32  score: F64?  born: Date  seen: DateTime }
edge Knows: Person -> Person
"#;
    let catalog = build_catalog(&parse_schema(LITERAL_SCHEMA).unwrap()).unwrap();
    let outcome = |src: &str| {
        check_source(&catalog, src)
            .unwrap_or_else(|e| panic!("parse: {e}\n{src}"))
            .remove(0)
            .1
    };
    let read = |predicate: &str| {
        format!("query q() {{ match {{ $p: Person }} where {predicate} return {{ $p.slug }} }}")
    };
    let mut cases: Vec<(String, String, &str)> = [
        (
            "$p.status = \"Active\"",
            "$p.status = \"active\"",
            "expects one of [active, retired], got 'Active'",
        ),
        (
            "\"retird\" != $p.status",
            "\"retired\" != $p.status",
            "got 'retird'",
        ),
        (
            "$p.former = \"retird\"",
            "$p.former = \"retired\"",
            "got 'retird'",
        ),
        (
            "$p.status in [\"active\", \"retird\"]",
            "$p.status in [\"active\", \"retired\"]",
            "got 'retird'",
        ),
        (
            "$p.age < 3000000000",
            "$p.age < 2147483647",
            "has type I32 but got 3000000000, outside its range -2147483648 to 2147483647",
        ),
        (
            "$p.rank > -1",
            "$p.rank > 0",
            "has type U32 but got -1, outside its range 0 to 4294967295",
        ),
        (
            "$p.weight < 1.0e39",
            "$p.weight < 340282346638528860000000000000000000000.0",
            "has type F32 but got",
        ),
        (
            "$p.born > date(\"1970-13-32\")",
            "$p.born > date(\"1970-12-31\")",
            "has type Date but got the malformed date \"1970-13-32\"",
        ),
        (
            "$p.born = date(\"2023-02-30\")",
            "$p.born = date(\"2023-02-28\")",
            "malformed date",
        ),
    ]
    .into_iter()
    .map(|(written, intended, message)| (read(written), read(intended), message))
    .collect();
    // The same check in a block's, an `optional`'s and a write group's
    // `where` and in a `let`.
    for (written, intended) in [
        (
            "query q() { match { $p: Person } where exists { $p knows $q where $q.status = \"retird\" } return { $p.slug } }",
            "query q() { match { $p: Person } where exists { $p knows $q where $q.status = \"retired\" } return { $p.slug } }",
        ),
        (
            "query q() { match { $p: Person, optional { $p knows $q where $q.status = \"retird\" } } return { $p.slug } }",
            "query q() { match { $p: Person, optional { $p knows $q where $q.status = \"retired\" } } return { $p.slug } }",
        ),
        (
            "query q() { match { $p: Person, let $gone = $p.status = \"retird\" } return { $p.slug, $gone } }",
            "query q() { match { $p: Person, let $gone = $p.status = \"retired\" } return { $p.slug, $gone } }",
        ),
        (
            "query q() { match { $p: Person } where $p.status = \"retird\" update $p set { age: 1 } }",
            "query q() { match { $p: Person } where $p.status = \"retired\" update $p set { age: 1 } }",
        ),
    ] {
        cases.push((written.to_string(), intended.to_string(), "got 'retird'"));
    }
    for (written, intended, message) in &cases {
        let d = outcome(written).expect_err(written);
        assert_eq!(d.code.as_str(), "T3", "{written}: {d}");
        assert!(d.message.contains(message), "{written}: {d}");
        assert!(d.position.is_some(), "{written}");
        let suggestion = d.suggestion.unwrap_or_else(|| panic!("{written}: no edit"));
        assert_eq!(suggestion.applicability, Applicability::NeedsReview);
        let [edit] = suggestion.edits.as_slice() else {
            panic!("{written}: one edit")
        };
        assert_eq!(
            d.fix.as_deref(),
            Some(edit.replacement.as_str()),
            "{written}"
        );
        let repaired = apply_edits(written, &suggestion.edits);
        assert_eq!(&repaired, intended);
        outcome(&repaired).unwrap_or_else(|e| panic!("{repaired}: {e}"));
    }
    // A date with no nearest day, a malformed datetime and a string test no
    // enum value passes print the fix without an edit.
    for (written, fix) in [
        (
            read("$p.born < date(\"yesterday\")"),
            "date(\"YYYY-MM-DD\")",
        ),
        (
            read("$p.seen > datetime(\"2024-01-01T25:00:00Z\")"),
            "datetime(\"YYYY-MM-DDTHH:MM:SSZ\")",
        ),
        (read("starts_with($p.status, \"ret_\")"), "\"retired\""),
        (read("contains($p.status, \"xyz\")"), "\"active\""),
    ] {
        let d = outcome(&written).expect_err(&written);
        assert_eq!(
            (d.code.as_str(), d.fix.as_deref(), d.suggestion.is_none()),
            ("T3", Some(fix), true),
            "{written}: {d}"
        );
    }
    // Valid literals, an integer against a float and a float against an
    // integer (compared through a cast, which keeps the fraction), a
    // parameter (its declared type), and a string test some value passes
    // are admitted as before.
    for predicate in [
        "$p.status = \"active\" and \"retired\" != $p.status and $p.former = \"retired\"",
        "$p.status in [\"active\", \"retired\"] and \"anything\" in $p.tags",
        "$p.age > -2147483648 and $p.age < 2147483647 and $p.rank >= 0",
        "$p.weight > 1 and $p.score > 2 and $p.score < 2.5 and $p.weight < 0.5",
        "$p.age > 2.5 and $p.rank in [1, 2.5] and 9007199254740992.0 = $p.age",
        "$p.born > date(\"1970-01-01\") and $p.seen < datetime(\"2024-01-01T00:00:00Z\")",
        "starts_with($p.status, \"ret\") and contains($p.status, \"tiv\")",
    ] {
        let src = read(predicate);
        outcome(&src).unwrap_or_else(|e| panic!("{src}: {e}"));
    }
    outcome(
        "query q($s: String, $n: I64) { match { $p: Person } where $p.status = $s and $p.age < $n return { $p.slug } }",
    )
    .expect("a parameter keeps its declared type");
}

/// The checked-literal example of the diagnostics guide
/// (docs/user/queries/diagnostics.md), byte for byte.
#[test]
fn t3_checked_literal_matches_the_diagnostics_guide() {
    use crate::query::diagnostic::{Applicability, TextEdit};
    let catalog = build_catalog(
        &parse_schema("node PullRequest { title: String @key  status: enum(closed, open) }")
            .unwrap(),
    )
    .unwrap();
    let src = "query open_prs() { match { $pr: PullRequest } where $pr.status = \"Closed\" return { $pr.title } }";
    let d = check_source(&catalog, src)
        .unwrap()
        .remove(0)
        .1
        .unwrap_err();
    assert_eq!(
        (d.code.as_str(), d.message.as_str(), d.fix.as_deref()),
        (
            "T3",
            "property `status` expects one of [closed, open], got 'Closed'",
            Some("\"closed\"")
        )
    );
    let suggestion = d.suggestion.expect("an edit");
    assert_eq!(suggestion.applicability, Applicability::NeedsReview);
    assert_eq!(
        suggestion.edits,
        vec![TextEdit {
            start: 65,
            end: 73,
            replacement: "\"closed\"".into()
        }]
    );
}

/// A type error points at the clause, return item or order key it was
/// found in.
#[test]
fn a_type_error_carries_the_position_of_its_clause() {
    let d = refused_with(
        "query q() {\n  match { $p: Person }\n  where $p.age > \"x\"\n  return { $p.name }\n}",
        "T7",
    );
    let at = d.position.expect("a position");
    assert_eq!((at.line, at.column), (3, 9));
    let d = refused_with(
        "query q() { match { $p: Person } return { $p.name } order { $p.nope } }",
        "T6",
    );
    assert_eq!(d.position.map(|p| p.column), Some(61));
}

mod desugaring;
