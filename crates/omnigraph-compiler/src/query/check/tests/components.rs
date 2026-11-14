//! The product rule (docs/rfcs/2026-11-07-gq-3.md, the derived forms and
//! "Added: two elements of `match`"): two patterns that share no variable
//! multiply their rows by a product only when every component but one is
//! pinned to one row by a `@key` equality in `where`, and are otherwise
//! `T81`, whose fix joins the components through a triple or writes the
//! product as a named relation.

use super::{check, ok, refused};
use crate::catalog::build_catalog;
use crate::query::check::type_source;
use crate::query::diagnostic::{Applicability, QueryDiagnostic};
use crate::query::syntax::parse::{apply_edits, parse_source};
use crate::schema::parser::parse_schema;

/// The refusal of `src`, which is `T81`.
#[track_caller]
fn t81(src: &str) -> QueryDiagnostic {
    let d = check(src)
        .into_iter()
        .find_map(|(_, r)| r.err())
        .unwrap_or_else(|| panic!("accepted:\n{src}"));
    assert_eq!(d.code.as_str(), "T81", "{d}\n{src}");
    d
}

/// `src` with its refusal's suggestion applied, which parses to `intended`
/// and type-checks.
#[track_caller]
fn repaired(src: &str, applicability: Applicability, intended: &str) {
    let d = t81(src);
    let suggestion = d.suggestion.as_ref().expect("a suggestion");
    assert_eq!(suggestion.applicability, applicability, "{d}");
    let fixed = apply_edits(src, &suggestion.edits);
    assert_eq!(
        parse_source(&fixed).expect("the repair parses"),
        parse_source(intended).expect("the intended text parses"),
        "repaired to\n{fixed}"
    );
    ok(&fixed);
}

#[test]
fn a_key_pinned_product_is_accepted_and_an_unpinned_one_refused() {
    for pinned in [
        r#"query q() { match { $p: Person, $c: Company } where $c.slug = "acme" return { $p.name, $c.name } }"#,
        r#"query q($s: String) { match { $p: Person, $c: Company } where $s = $c.slug and $p.age > 3 return { $p.name } }"#,
        r#"query q() { match { $c: Company, $p: Person, $p knows $q } where $c.slug = "acme" return { $q.name } }"#,
        r#"query q() { match { $p: Person, $c: Company, $d: Doc } where $p.handle = "a" and $d.id = "d" return { $c.name } }"#,
        // `@id` pins as the key does, against a literal or a parameter.
        r#"query q() { match { $p: Person, $c: Company } where $c.@id = "acme" return { $p.name } }"#,
        r#"query q($i: String) { match { $p: Person, $d: Doc } where $i = $d.@id return { $p.name } }"#,
    ] {
        ok(pinned);
    }
    for unpinned in [
        "query q() { match { $p: Person, $c: Company } return { $p.name, $c.name } }",
        // A property that is not the key, and a key compared by another
        // operator, pin nothing.
        r#"query q() { match { $p: Person, $c: Company } where $c.name = "Acme" return { $p.name } }"#,
        r#"query q() { match { $p: Person, $c: Company } where $c.slug != "acme" return { $p.name } }"#,
        r#"query q() { match { $p: Person, $c: Company } where $c.slug in ["a", "b"] return { $p.name } }"#,
        // The key equated with another component's property is a join by
        // value, not a pin.
        "query q() { match { $p: Person, $c: Company } where $c.slug = $p.handle return { $p.name } }",
        // A pinned binding with a triple is a component of many rows.
        r#"query q() { match { $p: Person, $p knows $q, $c: Company } where $p.handle = "a" return { $q.name } }"#,
        // Under `or` a conjunct pins nothing.
        r#"query q() { match { $p: Person, $c: Company } where $c.slug = "a" or $p.age > 3 return { $p.name } }"#,
        // `@id` compared by another operator, or with another component's
        // column, pins nothing.
        r#"query q() { match { $p: Person, $c: Company } where $c.@id != "acme" return { $p.name } }"#,
        "query q() { match { $p: Person, $c: Company } where $c.@id = $p.handle return { $p.name } }",
        // A component of two elements is never pinned, whatever its
        // binding's key and `@id` equate.
        r#"query q() { match { $p: Person, $p knows $q, $d: Doc } where $p.@id = "a" and $p.handle = "a" return { $q.name } }"#,
        // A triple between two outer names is a component, not a pin.
        r#"query q() { match { $p: Person, $q: Person, $p knows $q } where count { $p knows $q, $d: Doc } > 1 return { $p.name } }"#,
    ] {
        t81(unpinned);
    }
}

#[test]
fn a_composite_key_pins_with_every_column() {
    let schema = "node Pair { first: String  last: String  @key(first, last) }\nnode Item { slug: String @key }";
    let catalog = build_catalog(&parse_schema(schema).unwrap()).unwrap();
    let code = |src: &str| -> String {
        type_source(&catalog, src)
            .unwrap()
            .into_iter()
            .find_map(|(_, r)| r.err())
            .map_or("ok".into(), |d| d.code.as_str().to_string())
    };
    assert_eq!(
        code(
            r#"query q() { match { $i: Item, $p: Pair } where $p.first = "ada" and $p.last = "byron" return { $i.slug } }"#
        ),
        "ok"
    );
    assert_eq!(
        code(
            r#"query q() { match { $i: Item, $p: Pair } where $p.first = "ada" return { $i.slug } }"#
        ),
        "T81"
    );
}

/// The refusal points at the second component and names both; with one edge
/// type from one to the other, the fix inserts the triple, source first.
#[test]
fn the_refusal_is_at_the_second_component_and_one_edge_joins_them() {
    let src = "query q() {\n  match { $c: Company, $p: Person }\n  return { $p.name, $c.name }\n}";
    let d = t81(src);
    let at = d.position.expect("a position");
    assert_eq!((at.line, at.column), (2, 24));
    assert_eq!(
        d.message,
        "`$p: Person` shares no variable with `$c: Company`, so the pattern multiplies their rows by a product; a product is written only when every component but one is key-pinned, one node binding whose every `@key` property, or whose `@id`, `where` equates with a literal or a parameter"
    );
    assert_eq!(
        d.fix.as_deref(),
        Some(
            "join them through the edge, `$p worksAt $c`, or write the intended product as a named relation, `let $ps = { match { $p: Person } return { $p } }, from $ps`"
        )
    );
    repaired(
        src,
        Applicability::MachineApplicable,
        "query q() { match { $c: Company, $p: Person, $p worksAt $c } return { $p.name, $c.name } }",
    );
}

/// Without exactly one triple between the components, the suggestion writes
/// the second component as a named relation, which the author reviews.
#[test]
fn without_one_connecting_triple_the_fix_is_the_named_relation() {
    repaired(
        "query q() { match { $p: Person, $c: Collection } return { $p.name, $c.slug } }",
        Applicability::NeedsReview,
        "query q() { match { $p: Person, let $cs = { match { $c: Collection } return { $c } }, from $cs } return { $p.name, $c.slug } }",
    );
    // Two orientations of one edge type: the fix names both.
    let src =
        "query q() { match { $p: Person, $q: Person } where $p.age > $q.age return { $p.name } }";
    let d = t81(src);
    assert_eq!(
        d.fix.as_deref(),
        Some(
            "join them through a triple, one of `$p knows $q`, `$q knows $p`, or write the intended product as a named relation, `let $qs = { match { $q: Person } return { $q } }, from $qs`"
        )
    );
    repaired(
        src,
        Applicability::NeedsReview,
        "query q() { match { $p: Person, let $qs = { match { $q: Person } return { $q } }, from $qs } where $p.age > $q.age return { $p.name } }",
    );
    // A component of several elements moves whole, a binding restating a
    // type it binds included.
    repaired(
        "query q() { match { $c: Collection, $p: Person, $p knows $q, $q: Person } return { $q.name } }",
        Applicability::NeedsReview,
        "query q() { match { $c: Collection, let $ps = { match { $p: Person, $p knows $q, $q: Person } return { $p, $q } }, from $ps } return { $q.name } }",
    );
}

/// The rule holds in every scope: a block, an `optional`, an inline read and
/// a write group's `match`.
#[test]
fn every_scope_applies_the_rule() {
    t81(
        "query q() { match { $p: Person } where exists { $q: Person, $p knows $q, $c: Company } return { $p.name } }",
    );
    t81("query q() { match { $p: Person, optional { $c: Company } } return { $p.name } }");
    t81(
        "query q() { match { from { match { $q: Person, $c: Company } return { $q, $c } } } return { $q.name } }",
    );
    t81("query w() { match { $p: Person, $c: Company } update $p set { age: 1 } }");
    ok(
        r#"query w() { match { $p: Person, $c: Company } where $c.slug = "acme" update $p set { age: 1 } }"#,
    );
    // A block reading the outer variable correlates on the outer row, which
    // is one row.
    ok(
        r#"query q() { match { $p: Person } where exists { $c: Company where $p.name = $c.name and $c.slug = "acme" } return { $p.name } }"#,
    );
}

/// What joins two components: an element that reads a variable of each. A
/// `from` correlated on a variable joins its component, and one that reads
/// nothing is the product written out; a `let` value, the `where` and a block
/// in it read rows rather than produce them, so they join nothing.
#[test]
fn only_an_element_that_produces_rows_joins_components() {
    ok(
        "query q() { match { $p: Person, from { match { $q: Person } where $q.age = $p.age return { $q } } } return { $q.name } }",
    );
    ok(
        "query q() { match { $p: Person, let $a = $p.age, from { match { $q: Person } where $q.age = $a return { $q } } } return { $q.name } }",
    );
    ok(
        "query q() { match { $p: Person, from { match { $c: Company } return { $c } } } return { $p.name, $c.name } }",
    );
    ok(
        "query q() { match { $p: Person, optional { $c: Company, $p worksAt $c } } return { $p.name, $c.name } }",
    );
    t81(
        "query q() { match { $p: Person, $c: Company, let $n = concat($p.name, $c.name) } return { $n } }",
    );
    t81(
        "query q() { match { $p: Person, $c: Company } where $p.name = $c.name return { $p.name } }",
    );
    let d = t81(
        "query q() { match { $p: Person, $c: Company } where exists { $p worksAt $c } return { $p.name } }",
    );
    assert!(
        d.fix
            .as_deref()
            .is_some_and(|f| f.contains("`$p worksAt $c`")),
        "{d}"
    );
    // A refusal elsewhere keeps its own code.
    refused(
        "query q() { match { $p: Person } return { $p.name, $p.name } }",
        "T25",
    );
}
