//! The desugaring-uniqueness test and the closure table as a test
//! (docs/rfcs/2026-11-07-gq-3.md, section 10).
//!
//! The first lowers every surface form of section 3 through the edition-3
//! checker and compares its core term with the term the derived-forms table
//! of section 2 names, built here directly; then it lowers every form and
//! fails on two forms that share a term unless the formatter list of section
//! 6 writes both one way, or A6's one exception (a literal and a reference to
//! the same value) covers them. The second is one case per
//! row of section 2's closure table: a derived relation in that input
//! position, accepted or refused with the stated reason.

use std::collections::{BTreeMap, BTreeSet};

use super::ast::{AggFunc, CompOp, Literal};
use super::typeck3::compile3;
use crate::catalog::{Catalog, build_catalog};
use crate::ir::core3::*;
use crate::schema::parser::parse_schema;
use crate::types::{Direction, PropType, ScalarType};

/// The checker tests' schema plus a second Person → Person edge, so that an
/// alternation has two members of one orientation.
fn catalog() -> Catalog {
    let schema = format!(
        "{}\nedge Likes: Person -> Person\n",
        super::typeck3_tests::SCHEMA
    );
    build_catalog(&parse_schema(&schema).unwrap()).unwrap()
}

#[track_caller]
fn lower(src: &str) -> CoreDecl {
    let out =
        compile3(&catalog(), &format!("gq 3;\n{src}")).unwrap_or_else(|e| panic!("{e}\n{src}"));
    out.into_iter()
        .last()
        .unwrap()
        .1
        .unwrap_or_else(|e| panic!("{e}\n{src}"))
        .core
}

#[track_caller]
fn refusal(src: &str) -> super::diagnostic::QueryDiagnostic {
    match compile3(&catalog(), &format!("gq 3;\n{src}")) {
        Err(e) => e,
        Ok(out) => out
            .into_iter()
            .find_map(|(_, r)| r.err())
            .unwrap_or_else(|| panic!("accepted:\n{src}")),
    }
}

fn term(c: &CoreDecl) -> &Term {
    match c {
        CoreDecl::Read(r) => &r.term,
        CoreDecl::Write(w) => &w.groups[0].input,
    }
}

/// The match term under a read's return desugaring:
/// `Project(Cut?(Order?(Extend(R, items))), items)`.
fn match_term(c: &CoreDecl) -> &Term {
    let mut t = term(c);
    if let Term::Project {
        input, keep: false, ..
    } = t
    {
        t = input;
    }
    if let Term::Cut { input, .. } = t {
        t = input;
    }
    if let Term::Order { input, .. } = t {
        t = input;
    }
    match t {
        Term::Project {
            input, keep: true, ..
        }
        | Term::Group { input, .. } => input,
        other => other,
    }
}

// Constructors for the expected terms.

fn node(t: &str) -> Ty {
    Ty::Node {
        type_name: t.into(),
        nullable: false,
    }
}
fn val(s: ScalarType, nullable: bool) -> Ty {
    Ty::scalar(s, nullable)
}
fn boolean(nullable: bool) -> Ty {
    val(ScalarType::Bool, nullable)
}
fn scan(v: &str, t: &str) -> Term {
    Term::Scan {
        var: v.into(),
        ty: node(t),
    }
}
fn expand(input: Term, src: &str, dst: &str, dst_type: &str, sel: &[(&str, Direction)]) -> Term {
    Term::Expand {
        input: input.boxed(),
        src: src.into(),
        dst: dst.into(),
        dst_type: dst_type.into(),
        selector: sel.iter().map(|(e, d)| (e.to_string(), *d)).collect(),
        min_hops: 1,
        max_hops: Some(1),
        edge_var: None,
    }
}
fn env(cols: &[(&str, Ty)]) -> Term {
    Term::Env(
        cols.iter()
            .map(|(n, t)| Col {
                name: n.to_string(),
                ty: t.clone(),
            })
            .collect(),
    )
}
fn filter(input: Term, pred: SExpr) -> Term {
    Term::Filter {
        input: input.boxed(),
        pred,
    }
}
fn corr(outer: Term, inner: Term) -> Term {
    Term::Correlate {
        outer: outer.boxed(),
        inner: inner.boxed(),
    }
}
fn col(n: &str, ty: Ty) -> SExpr {
    SExpr::new(SKind::Col(n.into()), ty)
}
fn prop(c: &str, p: &str, ty: Ty) -> SExpr {
    SExpr::new(
        SKind::Prop {
            col: c.into(),
            prop: p.into(),
        },
        ty,
    )
}
fn int(n: i128, s: ScalarType) -> SExpr {
    SExpr::new(SKind::Lit(Literal::Integer(n)), val(s, false))
}
fn cmp(op: CompOp, l: SExpr, r: SExpr) -> SExpr {
    let nullable = l.ty.nullable() || r.ty.nullable();
    SExpr::new(
        SKind::Cmp {
            op,
            left: Box::new(l),
            right: Box::new(r),
        },
        boolean(nullable),
    )
}
fn not(e: SExpr) -> SExpr {
    SExpr::new(SKind::Not(Box::new(e)), boolean(false))
}
fn count_rows() -> Agg {
    Agg {
        func: AggFunc::Count,
        distinct: false,
        arg: None,
        ty: val(ScalarType::I64, false),
    }
}
fn block(r: Term, inner: Term, c: &str) -> Term {
    corr(
        r,
        Term::Group {
            input: inner.boxed(),
            keys: Vec::new(),
            aggs: vec![(c.into(), count_rows())],
        },
    )
}
fn exists(c: &str) -> SExpr {
    cmp(
        CompOp::Gt,
        col(c, val(ScalarType::I64, false)),
        int(0, ScalarType::I64),
    )
}
fn inline_ret(input: Term, cols: &[(&str, Ty)]) -> Term {
    Term::Project {
        input: Term::Project {
            input: input.boxed(),
            items: cols
                .iter()
                .map(|(n, t)| (format!("^{n}"), col(n, t.clone())))
                .collect(),
            keep: true,
        }
        .boxed(),
        items: cols
            .iter()
            .map(|(n, t)| (n.to_string(), col(&format!("^{n}"), t.clone())))
            .collect(),
        keep: false,
    }
}

fn age() -> Ty {
    Ty::Value(PropType::scalar(ScalarType::I32, true))
}
fn knows_q_from_env() -> Term {
    expand(
        env(&[("p", node("Person"))]),
        "p",
        "q",
        "Person",
        &[("Knows", Direction::Out)],
    )
}

/// Section 2's derived-forms table, row by row: each surface form lowers to
/// the core term the table names.
#[test]
fn every_surface_form_lowers_to_the_term_the_table_names() {
    use CompOp::*;
    let p_knows_q = || {
        expand(
            scan("p", "Person"),
            "p",
            "q",
            "Person",
            &[("Knows", Direction::Out)],
        )
    };
    let rows: Vec<(&str, &str, Term)> = vec![
        (
            "match { … }: binders in written order, a filter after the last variable it names",
            "query f() { match { $p: Person  $q.age > 3  $p knows $q } return { $q } }",
            filter(
                p_knows_q(),
                cmp(Gt, prop("q", "age", age()), int(3, ScalarType::I32)),
            ),
        ),
        (
            "count { p } as a value: Correlate(R, Group(p, [], [agg])), the aggregate column",
            "query f() { match { $p: Person  let $n = count { $p knows $q } } return { $n } }",
            Term::Project {
                input: block(scan("p", "Person"), knows_q_from_env(), "_b1").boxed(),
                items: vec![("n".into(), col("_b1", val(ScalarType::I64, false)))],
                keep: true,
            },
        ),
        (
            "exists { p }: count { p } > 0",
            "query f() { match { $p: Person  exists { $p knows $q } } return { $p } }",
            filter(
                block(scan("p", "Person"), knows_q_from_env(), "_b1"),
                exists("_b1"),
            ),
        ),
        (
            "not { p }: Filter(¬ exists { p })",
            "query f() { match { $p: Person  not { $p knows $q } } return { $p } }",
            filter(
                block(scan("p", "Person"), knows_q_from_env(), "_b1"),
                not(exists("_b1")),
            ),
        ),
        (
            "optional { p }: Union(Correlate(R, p), Project(Filter(R, ¬ exists p), nulls))",
            "query f() { match { $p: Person  optional { $p worksAt $c } } return { $p, $c } }",
            {
                let works = || {
                    expand(
                        env(&[("p", node("Person"))]),
                        "p",
                        "c",
                        "Company",
                        &[("WorksAt", Direction::Out)],
                    )
                };
                Term::Union {
                    left: corr(scan("p", "Person"), works()).boxed(),
                    right: Term::Project {
                        input: filter(
                            block(scan("p", "Person"), works(), "_opt"),
                            not(exists("_opt")),
                        )
                        .boxed(),
                        items: vec![
                            ("p".into(), col("p", node("Person"))),
                            (
                                "c".into(),
                                SExpr::new(
                                    SKind::Null,
                                    Ty::Node {
                                        type_name: "Company".into(),
                                        nullable: true,
                                    },
                                ),
                            ),
                        ],
                        keep: false,
                    }
                    .boxed(),
                }
            },
        ),
        (
            "two patterns sharing no variable: Correlate with an inner term that has no free variables",
            "query f() { match { $p: Person  $c: Company } return { $p, $c } }",
            corr(scan("p", "Person"), scan("c", "Company")),
        ),
        (
            "$a (e1 | e2) $b: one Expand whose selector holds every member",
            "query f() { match { $p: Person  $p (knows | likes) $q } return { $q } }",
            expand(
                scan("p", "Person"),
                "p",
                "q",
                "Person",
                &[("Knows", Direction::Out), ("Likes", Direction::Out)],
            ),
        ),
        (
            "$a <e> $b: one Expand whose selector holds both orientations",
            "query f() { match { $p: Person  $p <knows> $q } return { $q } }",
            expand(
                scan("p", "Person"),
                "p",
                "q",
                "Person",
                &[("Knows", Direction::Both)],
            ),
        ),
        (
            "$k: E: Scan over the edge type",
            "query f() { match { $k: WorksAt } return { $k } }",
            Term::Scan {
                var: "k".into(),
                ty: Ty::Edge {
                    type_name: "WorksAt".into(),
                    nullable: false,
                },
            },
        ),
        (
            "let $v = expr: Project adding one named column",
            "query f() { match { $p: Person  let $a = $p.age } return { $a } }",
            Term::Project {
                input: scan("p", "Person").boxed(),
                items: vec![("a".into(), prop("p", "age", age()))],
                keep: true,
            },
        ),
        (
            "from rel: Correlate",
            "query f() { match { $p: Person  from { match { $c: Company } return { $c } } } return { $p, $c } }",
            corr(
                scan("p", "Person"),
                inline_ret(scan("c", "Company"), &[("c", node("Company"))]),
            ),
        ),
        (
            "union(rel, rel, …): Union, folded from the left",
            "query f() { match { from union({ match { $c: Company } return { $c } }, { match { $d: Company } return { $d as c } }, { match { $e: Company } return { $e as c } }) } return { $c } }",
            {
                let arm = |v: &str| Term::Project {
                    input: Term::Project {
                        input: scan(v, "Company").boxed(),
                        items: vec![("^c".into(), col(v, node("Company")))],
                        keep: true,
                    }
                    .boxed(),
                    items: vec![("c".into(), col("^c", node("Company")))],
                    keep: false,
                };
                Term::Union {
                    left: Term::Union {
                        left: inline_ret(scan("c", "Company"), &[("c", node("Company"))]).boxed(),
                        right: arm("d").boxed(),
                    }
                    .boxed(),
                    right: arm("e").boxed(),
                }
            },
        ),
        (
            "distance(rel, $x.f, $q): Score",
            "query f($v: Vector(3)) { match { from distance({ match { $d: Doc } return { $d } }, $d.embedding, $v) } return { $d } }",
            Term::Score {
                input: inline_ret(scan("d", "Doc"), &[("d", node("Doc"))]).boxed(),
                column: "d".into(),
                property: "embedding".into(),
                kind: ScoreKind::Distance,
                query: ScoreQuery::Vector(SExpr::new(
                    SKind::Param("v".into()),
                    Ty::Value(PropType::scalar(ScalarType::Vector(3), false)),
                )),
                out: "distance".into(),
            },
        ),
        (
            "bm25(rel, $x.f, q): Score, a String meaning terms(q, mode: any)",
            "query f($s: String) { match { from bm25({ match { $d: Doc } return { $d } }, $d.body, $s) } return { $d } }",
            Term::Score {
                input: inline_ret(scan("d", "Doc"), &[("d", node("Doc"))]).boxed(),
                column: "d".into(),
                property: "body".into(),
                kind: ScoreKind::Bm25,
                query: ScoreQuery::Lexical(LexicalQuery {
                    text: SExpr::new(SKind::Param("s".into()), val(ScalarType::String, false)),
                    mode_all: false,
                    max_edits: int(0, ScalarType::I64),
                }),
                out: "score".into(),
            },
        ),
        (
            "nearest(rel, $x.f, $q, k: n): ApproxTopK",
            "query f($v: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v, k: 5) } return { $d } }",
            Term::ApproxTopK {
                input: inline_ret(scan("d", "Doc"), &[("d", node("Doc"))]).boxed(),
                column: "d".into(),
                property: "embedding".into(),
                query: SExpr::new(
                    SKind::Param("v".into()),
                    Ty::Value(PropType::scalar(ScalarType::Vector(3), false)),
                ),
                k: Count::Literal(5),
                recall: None,
                out: "distance".into(),
            },
        ),
    ];
    for (row, src, want) in rows {
        let got = lower(src);
        assert_eq!(match_term(&got), &want, "row: {row}\n{src}");
    }

    // `return { … }`, `order`, `limit`:
    // Project(Cut(Order(Extend(R, items), keys), k), items).
    let got = lower(
        "query f() { match { $p: Person } return { $p.name } order { $p.age desc } limit 3 }",
    );
    let name = val(ScalarType::String, false);
    let want = Term::Project {
        input: Term::Cut {
            input: Term::Order {
                input: Term::Project {
                    input: scan("p", "Person").boxed(),
                    items: vec![("^p.name".into(), prop("p", "name", name.clone()))],
                    keep: true,
                }
                .boxed(),
                keys: vec![(prop("p", "age", age()), true)],
            }
            .boxed(),
            k: Count::Literal(3),
        }
        .boxed(),
        items: vec![("p.name".into(), col("^p.name", name))],
        keep: false,
    };
    assert_eq!(term(&got), &want);
    // `return distinct { … }`: Group with no aggregates.
    let got = lower("query f() { match { $p: Person } return distinct { $p.city } }");
    let city = Ty::Value(PropType::scalar(ScalarType::String, true));
    assert!(matches!(term(&got), Term::Project { input, .. }
        if matches!(input.as_ref(), Term::Group { keys, aggs, .. }
            if aggs.is_empty() && keys == &vec![("^p.city".to_string(), prop("p", "city", city.clone()))])));
    // `rrf(rel, …)`: Fuse; a named read: Call; a self-calling read: Fix.
    let got = lower(
        "query f($s: String, $v: Vector(3)) { match { let $docs = { match { $d: Doc } return { $d } }  from rrf(nearest($docs, $d.embedding, $v, k: 5), bm25($docs, $d.body, $s)) } return { $d } }",
    );
    assert!(
        matches!(match_term(&got), Term::Fuse { arms, k: 60, weights, .. } if arms.len() == 2 && weights == &vec![1.0, 1.0])
    );
    let got = lower(
        "query g($h: String) { match { $p: Person { handle: $h } } return { $p } }\nquery f() { match { from g(\"a\") } return { $p } }",
    );
    assert!(matches!(match_term(&got), Term::Call { name, .. } if name == "g"));
    let got = lower(super::typeck3_tests::REACHABLE);
    assert!(matches!(term(&got), Term::Fix { .. }));
    // A write body: one group per run of clauses and statements, the clauses'
    // term then one sink per statement.
    let got = lower(
        "query w($h: String) { $p: Person { handle: $h }  update $p set { city: null }  $c: Company { slug: $h }  update $c set { name: $h } }",
    );
    let CoreDecl::Write(w) = &got else { panic!() };
    assert_eq!(w.groups.len(), 2);
    assert_eq!(
        w.groups[0].input,
        filter(
            scan("p", "Person"),
            cmp(
                Eq,
                prop("p", "handle", name_ty()),
                SExpr::new(SKind::Param("h".into()), name_ty())
            )
        )
    );
    // Edition 2's `delete T where φ`, as the rewriter writes it:
    // Delete(Filter(Scan(T), φ), $x).
    let got = lower("query w() { $x: Person  $x.age > 3  delete $x }");
    let CoreDecl::Write(w) = &got else { panic!() };
    assert_eq!(
        w.groups[0].input,
        filter(
            scan("x", "Person"),
            cmp(Gt, prop("x", "age", age()), int(3, ScalarType::I32))
        )
    );
    assert!(matches!(&w.groups[0].sinks[0], Sink::Delete { var, .. } if var == "x"));
}

fn name_ty() -> Ty {
    val(ScalarType::String, false)
}

/// The forms of section 3, each a declaration; `meaning` groups the forms a
/// reader would call one query.
const FORMS: &[(&str, &str)] = &[
    (
        "braces",
        "query f($h: String) { match { $p: Person { handle: $h } } return { $p } }",
    ),
    (
        "braces_as_filter",
        "query f($h: String) { match { $p: Person  $p.handle = $h } return { $p } }",
    ),
    // With neither end typed, the written order is the schema order (as
    // today): `$c worksAt $p` alone makes `$c` the Person. The reversal the
    // formatter rewrites is of a triple whose ends are typed.
    (
        "triple_schema_order",
        "query f() { match { $c: Company  $p worksAt $c } return { $p, $c } }",
    ),
    (
        "triple_reversed",
        "query f() { match { $c: Company  $c worksAt $p } return { $p, $c } }",
    ),
    (
        "exists",
        "query f() { match { $p: Person  exists { $p knows $q } } return { $p } }",
    ),
    (
        "count_gt_0",
        "query f() { match { $p: Person  count { $p knows $q } > 0 } return { $p } }",
    ),
    (
        "count_ge_1",
        "query f() { match { $p: Person  count { $p knows $q } >= 1 } return { $p } }",
    ),
    (
        "not_block",
        "query f() { match { $p: Person  not { $p knows $q } } return { $p } }",
    ),
    (
        "not_exists_filter",
        "query f() { match { $p: Person  not exists { $p knows $q } } return { $p } }",
    ),
    (
        "count_eq_0",
        "query f() { match { $p: Person  count { $p knows $q } = 0 } return { $p } }",
    ),
    (
        "union_flat",
        "query f() { match { from union({ match { $c: Company } return { $c } }, { match { $d: Company } return { $d as c } }, { match { $e: Company } return { $e as c } }) } return { $c } }",
    ),
    (
        "union_nested",
        "query f() { match { from union(union({ match { $c: Company } return { $c } }, { match { $d: Company } return { $d as c } }), { match { $e: Company } return { $e as c } }) } return { $c } }",
    ),
    (
        "alternation",
        "query f() { match { $p: Person  $p (knows | likes) $q } return { $q } }",
    ),
    (
        "alternation_reordered",
        "query f() { match { $p: Person  $p (likes | knows) $q } return { $q } }",
    ),
    (
        "undirected",
        "query f() { match { $p: Person  $p <knows> $q } return { $q } }",
    ),
    (
        "undirected_ends_swapped",
        "query f() { match { $p: Person  $q <knows> $p } return { $q } }",
    ),
    (
        "order_asc",
        "query f() { match { $p: Person } return { $p } order { $p.age asc } }",
    ),
    (
        "order_default",
        "query f() { match { $p: Person } return { $p } order { $p.age } }",
    ),
    (
        "alias_same_name",
        "query f() { match { $p: Person } return { $p as p } }",
    ),
    (
        "no_alias",
        "query f() { match { $p: Person } return { $p } }",
    ),
    (
        "and_one_clause",
        "query f() { match { $p: Person  $p.age > 3 and $p.city = \"x\" } return { $p } }",
    ),
    (
        "and_two_clauses",
        "query f() { match { $p: Person  $p.age > 3  $p.city = \"x\" } return { $p } }",
    ),
    (
        "bm25_string",
        "query f($s: String) { match { from bm25({ match { $d: Doc } return { $d } }, $d.body, $s) } return { $d } }",
    ),
    (
        "bm25_terms_any",
        "query f($s: String) { match { from bm25({ match { $d: Doc } return { $d } }, $d.body, terms($s, mode: any)) } return { $d } }",
    ),
    (
        "bm25_terms_all",
        "query f($s: String) { match { from bm25({ match { $d: Doc } return { $d } }, $d.body, terms($s)) } return { $d } }",
    ),
    (
        "rrf_default_k",
        "query f($s: String, $v: Vector(3)) { match { let $docs = { match { $d: Doc } return { $d } }  from rrf(nearest($docs, $d.embedding, $v, k: 5), bm25($docs, $d.body, $s)) } return { $d } }",
    ),
    (
        "rrf_k_60",
        "query f($s: String, $v: Vector(3)) { match { let $docs = { match { $d: Doc } return { $d } }  from rrf(nearest($docs, $d.embedding, $v, k: 5), bm25($docs, $d.body, $s), k: 60) } return { $d } }",
    ),
    (
        "let_rel_then_from",
        "query f() { match { let $r = { match { $c: Company } return { $c } }  from $r } return { $c } }",
    ),
    (
        "from_inline",
        "query f() { match { from { match { $c: Company } return { $c } } } return { $c } }",
    ),
    (
        "filter_written_late",
        "query f() { match { $p: Person  $q: Person  $p.age > 3 } return { $p, $q } }",
    ),
    (
        "filter_written_early",
        "query f() { match { $p: Person  $p.age > 3  $q: Person } return { $p, $q } }",
    ),
    (
        "filter_before_binder",
        "query f() { match { $p.age > 3  $p: Person  $q: Person } return { $p, $q } }",
    ),
    (
        "two_bindings",
        "query f() { match { $p: Person  $q: Person } return { $p, $q } }",
    ),
    (
        "two_bindings_swapped",
        "query f() { match { $q: Person  $p: Person } return { $p, $q } }",
    ),
    (
        "let_value",
        "query f() { match { $p: Person  let $a = $p.age } return { $a } }",
    ),
    (
        "return_expression",
        "query f() { match { $p: Person } return { $p.age as a } }",
    ),
    (
        "fuzzy_default",
        "query f($s: String) { match { $d: Doc  fuzzy($d.body, $s) } return { $d } }",
    ),
    (
        "fuzzy_two_edits",
        "query f($s: String) { match { $d: Doc  fuzzy($d.body, $s, max_edits: 2) } return { $d } }",
    ),
];

/// The formatter's list (section 6): each pair is (non-canonical form,
/// canonical form), the rewrite `queries fmt` performs without changing the
/// typed IR.
const FORMATTER: &[(&str, &str)] = &[
    ("braces_as_filter", "braces"),
    ("triple_reversed", "triple_schema_order"),
    ("count_gt_0", "exists"),
    ("not_exists_filter", "not_block"),
    ("union_nested", "union_flat"),
    ("alternation_reordered", "alternation"),
    ("undirected_ends_swapped", "undirected"),
    ("order_asc", "order_default"),
    ("alias_same_name", "no_alias"),
    ("and_one_clause", "and_two_clauses"),
    ("bm25_terms_any", "bm25_string"),
    ("rrf_k_60", "rrf_default_k"),
    ("fuzzy_two_edits", "fuzzy_default"),
    // A filter moves to just after the binder of the last variable it names.
    ("filter_written_late", "filter_written_early"),
    ("filter_before_binder", "filter_written_early"),
];

/// Lint's rows-equal rewrites (section 5): equal rows, because a count is a
/// non-negative integer, but a different typed IR, so `fmt` does not apply
/// them.
const LINT_ONLY: &[(&str, &str)] = &[("count_ge_1", "exists"), ("count_eq_0", "not_block")];

/// A6's one standing exception: a literal and a reference to the same value.
const LITERAL_AND_REFERENCE: &[(&str, &str)] = &[("let_rel_then_from", "from_inline")];

/// The canonical form `fmt` writes for `id`.
fn canonical(id: &str) -> &str {
    let mut id = id;
    while let Some((_, to)) = FORMATTER
        .iter()
        .chain(LITERAL_AND_REFERENCE)
        .find(|(from, _)| *from == id)
    {
        id = to;
    }
    id
}

#[test]
fn two_forms_share_a_term_only_when_the_formatter_names_one() {
    let lowered: Vec<(&str, CoreDecl)> = FORMS.iter().map(|(id, src)| (*id, lower(src))).collect();
    let term = |id: &str| &lowered.iter().find(|(i, _)| *i == id).unwrap().1;
    // Every collision is two spellings the formatter writes one way (or A6's
    // literal and reference); a new one fails here.
    let mut unlisted = BTreeSet::new();
    for (i, (a, ta)) in lowered.iter().enumerate() {
        for (b, tb) in &lowered[..i] {
            if ta == tb && canonical(a) != canonical(b) {
                unlisted.insert((b.to_string(), a.to_string()));
            }
        }
    }
    assert_eq!(unlisted, BTreeSet::new(), "collisions no list names");
    // Every formatter row is a collision: the formatter never changes the
    // typed IR.
    for (from, to) in FORMATTER.iter().chain(LITERAL_AND_REFERENCE) {
        assert_eq!(term(from), term(to), "{from} -> {to}");
    }
    // Lint's rows are not: they change the IR, which is why they are lint's.
    for (from, to) in LINT_ONLY {
        assert_ne!(term(from), term(to), "{from} -> {to}");
    }
    // Forms a reader might call one query that are different terms: kept
    // apart, no A6 question (clause order of binders, `let` against a return
    // expression).
    for (a, b) in [
        ("two_bindings", "two_bindings_swapped"),
        ("let_value", "return_expression"),
    ] {
        let ta = &lowered.iter().find(|(i, _)| *i == a).unwrap().1;
        let tb = &lowered.iter().find(|(i, _)| *i == b).unwrap().1;
        assert_ne!(ta, tb, "{a} / {b}");
    }
}

// ---------------------------------------------------------------------------
// The closure table (section 2, Closure): one case per row.

/// A grouped relation: a derived relation for every row to consume.
const GROUPED: &str = "{ match { $p worksAt $c } return { $c, count($p) as n } }";

#[test]
fn the_closure_table() {
    let mut rows: BTreeMap<&str, String> = BTreeMap::new();
    let accept = |src: &str| -> String {
        let _ = lower(src);
        "accepted".into()
    };
    let refuse = |src: &str, code: &str, reason: &str| -> String {
        let e = refusal(src);
        assert_eq!(e.code.as_str(), code, "{e}\n{src}");
        assert!(e.message.contains(reason), "`{reason}` not in: {e}");
        format!("refused {code}")
    };
    rows.insert(
        "Correlate inner term",
        accept(&format!("query q() {{ match {{ $x: Company  from {{ match {{ from {GROUPED}  $c = $x }} return {{ $n }} }} }} return {{ $x, $n }} }}")),
    );
    rows.insert(
        "Union arms",
        accept(&format!("query q() {{ match {{ from union({{ match {{ from {GROUPED} }} return {{ $c, $n }} }}, {{ match {{ from {GROUPED} }} return {{ $c, $n }} }}) }} return {{ $c, $n }} }}")),
    );
    rows.insert(
        "Union arms, types disagree",
        refuse(
            &format!("query q() {{ match {{ from union({{ match {{ from {GROUPED} }} return {{ $c, $n }} }}, {{ match {{ $x: Company }} return {{ $x as c, $x.name as n }} }}) }} return {{ $c }} }}"),
            "T73",
            "arm 2 column 2",
        ),
    );
    rows.insert(
        "Group, Project, Filter, Order, Cut input",
        accept(&format!("query q() {{ match {{ from {GROUPED}  $n > 1 }} return {{ avg($n) as mean, max($n) as largest, count($c) as k }} order {{ mean desc }} limit 1 }}")),
    );
    rows.insert(
        "Expand source, a node column",
        accept(&format!(
            "query q() {{ match {{ from {GROUPED}  $x worksAt $c }} return {{ $x, $n }} }}"
        )),
    );
    rows.insert(
        "Expand source, no node column",
        refuse(
            "query q() { match { from { match { $p: Person } return { $p.handle as h } }  $h knows $x } return { $x } }",
            "T23",
            "nothing to traverse from",
        ),
    );
    rows.insert(
        "Score, ApproxTopK population, ranked column named",
        accept("query q($v: Vector(3)) { match { from nearest({ match { from { match { $p authored $d } return { $d, count($p) as authors } }  $authors > 1 } return { $d, $authors } }, $d.embedding, $v, k: 5) } return { $d, $authors, $distance } }"),
    );
    rows.insert(
        "Score population, ranked column not a node column",
        refuse(
            &format!("query q($v: Vector(3)) {{ match {{ from distance({GROUPED}, $n.embedding, $v) }} return {{ $c }} }}"),
            "T79",
            "a ranking ranks a node column",
        ),
    );
    rows.insert(
        "Fuse arms, scored, over one node column",
        accept("query q($s: String, $v: Vector(3)) { match { let $docs = { match { from { match { $p authored $d } return { $d, count($p) as a } } } return { $d } }  from rrf(nearest($docs, $d.embedding, $v, k: 5), bm25($docs, $d.body, $s)) } return { $d, $score } }"),
    );
    rows.insert(
        "Fuse arms over two node columns",
        refuse(
            "query q($s: String, $v: Vector(3)) { match { let $docs = { match { $p authored $d } return { $p, $d } }  from rrf(nearest($docs, $d.embedding, $v, k: 5), { match { from $docs } return { $p } order { $p.age } }) } return { $score } }",
            "T75",
            "the fused target would be a pair",
        ),
    );
    rows.insert(
        "Call argument, a relation",
        refuse(
            &format!("query g($c: String) {{ match {{ $x: Company {{ slug: $c }} }} return {{ $x }} }}\nquery q() {{ match {{ from g({GROUPED}) }} return {{ $x }} }}"),
            "T69",
            "an inline read is a relation",
        ),
    );
    rows.insert(
        "Call argument, a scalar",
        accept("query g($c: String) { match { $x: Company { slug: $c } } return { $x } }\nquery q() { match { $p: Person  from g($p.handle) } return { $x } }"),
    );
    rows.insert(
        "Fix body, monotone",
        accept(super::typeck3_tests::REACHABLE),
    );
    rows.insert(
        "Fix body, a negation over it",
        refuse(
            "query bad($from: String) { match { from union({ match { $p: Person { handle: $from } } return { $p as m } }, { match { $q: Person  not { from bad($from)  $m knows $q } } return { $q as m } }) } return distinct { $m } }",
            "T70",
            "not monotone",
        ),
    );
    rows.insert(
        "a column of a relation, a relation",
        refuse(
            &format!(
                "query q() {{ match {{ let $r = {GROUPED}  $x: Company }} return {{ $x, $r }} }}"
            ),
            "T69",
            "is a relation",
        ),
    );
    rows.insert(
        "a sink's input",
        accept(&format!(
            "query w() {{ from {GROUPED}  $n > 100  update $c set {{ name: \"big\" }} }}"
        )),
    );
    rows.insert(
        "a sink's input, a ranked relation",
        accept("query w($v: Vector(3)) { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $v, k: 20)  update $d set { tags: [\"near\"] } }"),
    );
    // A sink inside a read: never (A7). The grammar has no statement in a
    // read body, and a read cannot draw rows from a write.
    // A bare word is an operand nowhere in a `match`, so a statement there
    // fails at its word.
    rows.insert(
        "a sink inside a read, in the body",
        refuse(
            "query q() { match { $p: Person  delete $p } return { $p } }",
            "Q018",
            "`delete` is a bare name",
        ),
    );
    rows.insert(
        "a sink inside a read, through a call",
        refuse(
            "query w($h: String) { insert Person { handle: $h, name: $h } }\nquery q() { match { from w(\"a\") } return { 1 as one } }",
            "T74",
            "A7",
        ),
    );
    for (k, v) in &rows {
        eprintln!("{k:52} {v}");
    }
    assert_eq!(rows.len(), 19);
}
