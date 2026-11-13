//! The parser tests of docs/rfcs/2026-11-07-gq-3.md, section 10
//! ("Compiler parser tests"), the rule-set pin, and one repair per refusal
//! shape.

use super::parse::parse_source;
use super::*;
use crate::query::diagnostic::QueryDiagnostic;
use crate::query::file::{FileBody, MutationWriteMode};
use crate::query::parser::parse_query_diagnostic;
use crate::query::syntax::EdgeSelector;
use crate::query::value::CompOp;

pub(crate) fn parse_text(src: &str) -> Result<SourceFile, QueryDiagnostic> {
    parse_source(src)
}

#[track_caller]
pub(crate) fn parsed(src: &str) -> SourceFile {
    parse_text(src).unwrap_or_else(|e| panic!("refused:\n{src}\n{e:?}"))
}

#[track_caller]
fn refusal_of(src: &str) -> QueryDiagnostic {
    match parse_text(src) {
        Ok(f) => panic!("accepted:\n{src}\n{f:#?}"),
        Err(e) => e,
    }
}

#[track_caller]
fn code3(src: &str) -> &'static str {
    refusal_of(src).code.as_str()
}

#[track_caller]
fn decl(src: &str) -> Decl {
    match parsed(src).body {
        Body::Decls(mut d) => d.remove(0),
        other => panic!("not a declaration: {other:?}"),
    }
}

#[track_caller]
fn read(src: &str) -> ReadBody {
    match decl(src).body {
        DeclBody::Read(r) => r,
        other => panic!("not a read: {other:?}"),
    }
}

#[track_caller]
fn write(src: &str) -> WriteBody {
    match decl(src).body {
        DeclBody::Write(w) => w,
        other => panic!("not a write: {other:?}"),
    }
}

/// The elements of `query q() { match { <elements> } return { 1 as one } }`.
#[track_caller]
fn match_elements(elements: &str) -> Vec<Element> {
    read(&format!(
        "query q() {{ match {{ {elements} }} return {{ 1 as one }} }}"
    ))
    .pattern
    .elements
}

#[track_caller]
fn one_element(text: &str) -> Element {
    let mut c = match_elements(text);
    assert_eq!(c.len(), 1, "{text} gave {c:#?}");
    c.remove(0)
}

/// The `where` of `query q() { match { $x: X } where <text> return { 1 as
/// one } }`.
#[track_caller]
fn where_expr(text: &str) -> Expr {
    read(&format!(
        "query q() {{ match {{ $x: X }} where {text} return {{ 1 as one }} }}"
    ))
    .pattern
    .predicate
    .unwrap_or_else(|| panic!("{text}: no where"))
}

/// The refusal of `text` written as one element of a read's `match`.
#[track_caller]
fn element_refusal(text: &str) -> QueryDiagnostic {
    refusal_of(&format!(
        "query q() {{ match {{ {text} }} return {{ 1 as one }} }}"
    ))
}

fn int(n: i128) -> Expr {
    Expr::Literal(crate::query::value::Literal::Integer(n))
}
fn var(v: &str) -> Expr {
    Expr::Variable(v.to_string())
}
fn arith(op: ArithOp, l: Expr, r: Expr) -> Expr {
    Expr::Arith {
        op,
        left: Box::new(l),
        right: Box::new(r),
    }
}
fn cmp(op: CompOp, l: Expr, r: Expr) -> Expr {
    Expr::Compare {
        op,
        left: Box::new(l),
        right: Box::new(r),
        at: SourceSpan::default(),
    }
}

// ---------------------------------------------------------------------------
// `match` holds the elements, `where` the predicate (section 3).

#[test]
fn match_holds_the_elements_and_where_the_predicate() {
    let r = read(
        "query colleagues($h: String) { match { $p: Person, $p worksAt $c, $q worksAt $c } where $p.handle = $h and $q != $p and not exists { $q blocked $p } return { $q.name } }",
    );
    assert_eq!(r.pattern.elements.len(), 3);
    assert!(matches!(r.pattern.predicate, Some(Expr::And(..))));
    // A trailing comma, and elements one per line.
    assert_eq!(match_elements("$p: Person,").len(), 1);
    assert_eq!(match_elements("\n  $p: Person,\n  $p knows $q,\n").len(), 2);
    // A block holds elements and its own `where` inside its braces, and so
    // does `optional`.
    let Expr::Compare { left, .. } = where_expr("count { $x knows $q where $q.age > 30 } > 2")
    else {
        panic!()
    };
    let Expr::Block(b) = *left else { panic!() };
    assert_eq!(b.pattern.elements.len(), 1);
    assert!(b.pattern.predicate.is_some());
    let Element::Optional(o) = one_element("optional { $p worksAt $c where $c.name = \"x\" }")
    else {
        panic!()
    };
    assert!(o.predicate.is_some());
    // A write group selects with the same `match` and `where`.
    let w = write(
        "query relocate($slug: String) { match { $c: Company, $p worksAt $c } where $c.slug = $slug update $p set { city: null } }",
    );
    assert_eq!(w.groups[0].pattern.elements.len(), 2);
    assert!(w.groups[0].pattern.predicate.is_some());
}

/// Each repair of a predicate written among the elements is one suggestion
/// over the whole `match` and its `where`, which reparses to the intended
/// tree (section 3's table).
#[test]
fn a_predicate_inside_match_prints_the_where_after_the_braces() {
    for (src, code, intended) in [
        (
            "query q() { match { $p: Person, $p.age > 30 } return { $p } }",
            "Q017",
            "query q() { match { $p: Person } where $p.age > 30 return { $p } }",
        ),
        (
            "query q() { match { $p: Person where $p.age > 30 } return { $p } }",
            "Q017",
            "query q() { match { $p: Person } where $p.age > 30 return { $p } }",
        ),
        (
            "query q() { match { $p: Person, $p.age > 30 } where $p.age < 60 return { $p } }",
            "Q017",
            "query q() { match { $p: Person } where $p.age > 30 and $p.age < 60 return { $p } }",
        ),
        (
            "query q() { match { $p: Person, $p.a = 1 or $p.b = 2 } return { $p } }",
            "Q017",
            "query q() { match { $p: Person } where ($p.a = 1 or $p.b = 2) return { $p } }",
        ),
        (
            "query q($h: String) { match { $p: Person { handle: $h }, $p knows $q } return { $q } }",
            "Q010",
            "query q($h: String) { match { $p: Person, $p knows $q } where $p.handle = $h return { $q } }",
        ),
        (
            "query q() { match { $d: Doc { year >= 2025 } } return { $d } }",
            "Q010",
            "query q() { match { $d: Doc } where $d.year >= 2025 return { $d } }",
        ),
        (
            "query q() { match { $p: Person, not { $p knows $q } } return { $p } }",
            "Q012",
            "query q() { match { $p: Person } where not exists { $p knows $q } return { $p } }",
        ),
        (
            "query q() { match { $p: Person } where not { $p knows $q } return { $p } }",
            "Q012",
            "query q() { match { $p: Person } where not exists { $p knows $q } return { $p } }",
        ),
        (
            "query q() { match { $p: Person $p knows $q } return { $q } }",
            "Q016",
            "query q() { match { $p: Person, $p knows $q } return { $q } }",
        ),
        (
            "query q() { match { $p: Person } where count { $p knows $q $q.age > 3 } > 1 return { $p } }",
            "Q017",
            "query q() { match { $p: Person } where count { $p knows $q where $q.age > 3 } > 1 return { $p } }",
        ),
    ] {
        let e = refusal_of(src);
        assert_eq!(e.code.as_str(), code, "{src}: {e:?}");
        let fixed = super::parse::apply_edits(src, &e.suggestion.as_ref().unwrap().edits);
        assert_eq!(
            parse_source(&fixed).unwrap_or_else(|e| panic!("{src}\n{fixed}\n{e:?}")),
            parsed(intended),
            "{src}\n{fixed}"
        );
    }
}

#[test]
fn every_list_takes_commas_and_a_missing_one_prints_it() {
    for (src, intended) in [
        (
            "query q() { match { $p: Person } return { $p.name $p.age } }",
            "query q() { match { $p: Person } return { $p.name, $p.age } }",
        ),
        (
            "query q() { match { $p: Person } return { $p.name } order { $p.age desc $p.name } }",
            "query q() { match { $p: Person } return { $p.name } order { $p.age desc, $p.name } }",
        ),
        (
            "query q() { match { $p: Person } where contains($p.name \"a\") return { $p.name } }",
            "query q() { match { $p: Person } where contains($p.name, \"a\") return { $p.name } }",
        ),
        (
            "query w() { insert Person { handle: \"h\" name: \"n\" } }",
            "query w() { insert Person { handle: \"h\", name: \"n\" } }",
        ),
        (
            "query w() { match { $p: Person } update $p set {\n  age: 3\n  city: null\n} }",
            "query w() { match { $p: Person } update $p set { age: 3, city: null } }",
        ),
    ] {
        let e = refusal_of(src);
        assert_eq!(e.code.as_str(), "Q016", "{src}: {e:?}");
        let fixed = super::parse::apply_edits(src, &e.suggestion.as_ref().unwrap().edits);
        assert_eq!(parse_source(&fixed).unwrap(), parsed(intended), "{fixed}");
    }
    // A trailing comma in every list but a call's arguments.
    read("query q() { match { $p: Person, } return { $p.name, } order { $p.name, } }");
    write("query w() { insert Person { handle: \"h\", } }");
}

// ---------------------------------------------------------------------------
// Infix string predicates.

/// An infix `contains` is a tree node the type checker refuses, since the
/// left operand's type picks the fix (the call over a String, `in` over a
/// list).
#[test]
fn an_infix_string_predicate_is_a_node_the_checker_refuses() {
    let f = parse_query_diagnostic(
        "query q() { match { $d: Doc } where $d.t contains \"x\" return { $d.t } }",
    )
    .unwrap();
    let FileBody::Queries(decls) = f.body else {
        panic!("a declaration file")
    };
    let DeclBody::Read(body) = &decls[0].syntax.decl().body else {
        panic!("a read")
    };
    assert!(matches!(
        body.pattern.predicate,
        Some(Expr::InfixString { .. })
    ));
    // `gq` is an ordinary identifier.
    assert!(
        parse_query_diagnostic("query gq() { match { $p: P } where $p.gq = 3 return { $p.gq } }")
            .is_ok()
    );
}

// ---------------------------------------------------------------------------
// `let` and `from` in every element position.

const LET: &str = "let $v = $p.age";
const FROM: &str = "from { match { $p knows $q } return { $q } }";

#[test]
fn let_and_from_in_every_element_position() {
    for text in [LET, FROM] {
        let probe = |c: &Element| matches!(c, Element::Let { .. } | Element::From(_));
        // First, middle and last of a `match`.
        for elements in [
            format!("{text}, $p: Person"),
            format!("$p: Person, {text}, $p knows $q"),
            format!("$p: Person, {text}"),
        ] {
            let c = match_elements(&elements);
            assert_eq!(c.len(), elements.matches(", ").count() + 1, "{elements}");
            assert!(c.iter().any(probe), "{elements}: {c:#?}");
        }
        // Inside every block.
        for block in ["optional", "exists", "count"] {
            let pattern = if block == "optional" {
                match one_element(&format!("{block} {{ $p: Person, {text} }}")) {
                    Element::Optional(p) => p,
                    other => panic!("{block}: {other:?}"),
                }
            } else {
                let suffix = if block == "count" { " > 0" } else { "" };
                match where_expr(&format!("{block} {{ $p: Person, {text} }}{suffix}")) {
                    Expr::Block(b) => b.pattern,
                    Expr::Compare { left, .. } => match *left {
                        Expr::Block(b) => b.pattern,
                        other => panic!("{block}: {other:?}"),
                    },
                    other => panic!("{block}: {other:?}"),
                }
            };
            assert!(pattern.elements.iter().any(probe), "{block}: {pattern:#?}");
        }
        // Inside an inline read, as `from`'s and `let`'s relation.
        let c = one_element(&format!(
            "from {{ match {{ $p: Person, {text} }} return {{ $p }} }}"
        ));
        let Element::From(RelExpr::Inline(body)) = c else {
            panic!()
        };
        assert!(body.pattern.elements.iter().any(probe));
        // In a write group's `match`.
        let w = write(&format!(
            "query w() {{ match {{ $p: Person, {text} }} update $p set {{ age: 1 }} }}"
        ));
        assert!(
            w.all_elements().into_iter().any(probe),
            "{:#?}",
            w.all_elements()
        );
    }
}

#[test]
fn from_takes_an_inline_read_a_variable_and_a_call() {
    assert!(matches!(
        one_element("from { match { $p: Person } return { $p } }"),
        Element::From(RelExpr::Inline(_))
    ));
    assert!(matches!(
        one_element("from $docs"),
        Element::From(RelExpr::Variable(v)) if v == "docs"
    ));
    let Element::From(RelExpr::Call(c)) = one_element("from colleagues($x.handle)") else {
        panic!()
    };
    assert_eq!(c.name, "colleagues");
    // A call's name need not touch its parenthesis.
    let Element::From(RelExpr::Call(c)) = one_element("from colleagues ($x.handle)") else {
        panic!()
    };
    assert_eq!(c.name, "colleagues");
    let Element::From(RelExpr::Call(c)) = one_element(
        "from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $q, k: 20, recall: 0.9)",
    ) else {
        panic!()
    };
    assert_eq!((c.args.len(), c.options.len()), (3, 2));
    assert!(matches!(c.args[0], Arg::Inline(_)));
    // `from` followed by anything else is a parse error at the word, and so
    // is a bare name anywhere in a `match`.
    for text in ["from = 1", "from", "from (1) = 1"] {
        let e = element_refusal(text);
        assert!(["Q018", "Q019"].contains(&e.code.as_str()), "{text}: {e:?}");
        assert_eq!(e.position.unwrap().column, 21, "{text}: at the word");
    }
}

/// Inside `match` any word is an edge name, keywords included, and a quoted
/// name is still accepted (section 7).
#[test]
fn any_word_is_an_edge_name_inside_match() {
    for word in [
        "true", "false", "let", "from", "in", "and", "or", "not", "is", "null", "insert", "update",
        "delete", "where", "match", "return",
    ] {
        let Element::Traversal(t) = one_element(&format!("$a {word} $b")) else {
            panic!("{word} as an edge")
        };
        assert_eq!(t.selector, EdgeSelector::Named(word.into()), "{word}");
        let Element::Traversal(q) = one_element(&format!("$a \"{word}\" $b")) else {
            panic!("{word} quoted")
        };
        assert_eq!(q, t, "{word}: the quoted name is the same triple");
        // A bound edge of that name, with bounds, undirected, alternated.
        one_element(&format!("$a $k:{word} $b"));
        one_element(&format!("$a {word}{{1,2}} $b"));
        one_element(&format!("$a <{word}> $b"));
        one_element(&format!("$a ({word} | knows) $b"));
    }
    // In a write group, a statement word between two variables in `match`
    // is a triple, and the statement after the braces is a statement.
    let w = write("query w() { match { $a: Person, $a delete $b } delete $b }");
    assert_eq!((w.all_elements().len(), w.all_statements().len()), (2, 1));
    for word in ["let", "from"] {
        // A property after a dot, in `where` and in return.
        let r = read(&format!(
            "query q() {{ match {{ $p: Person }} where $p.{word} > 0 return {{ $p.{word} as {word} }} order {{ {word} }} }}"
        ));
        assert_eq!(r.projections[0].alias.as_deref(), Some(word));
        // A bare mutation property.
        let w = write(&format!(
            "query w() {{ insert Person {{ {word}: 1, handle: \"h\" }} }}"
        ));
        assert!(
            matches!(&w.all_statements()[0], Stmt::Insert { assignments, .. } if assignments[0].property == word)
        );
        // A registry row is never named an element keyword (section 5), so
        // `from(1)` and `let(1)` are refused, not calls.
        let e = refusal_of(&format!(
            "query q() {{ match {{ $x: X }} where $x.a = {word}(1) return {{ 1 as one }} }}"
        ));
        assert_eq!(e.code.as_str(), "Q019", "{word}");
    }
    // An edge insert: `from` and `to` as assignment names.
    let w =
        write("query hire($h: String, $slug: String) { insert WorksAt { from: $h, to: $slug } }");
    assert_eq!(w.all_statements().len(), 1);
}

// ---------------------------------------------------------------------------
// The arithmetic ladder.

#[test]
fn minus_is_arithmetic_and_the_arrow_is_refused() {
    let gt = |l| cmp(CompOp::Gt, l, int(0));
    assert_eq!(
        where_expr("$n - 1 > 0"),
        gt(arith(ArithOp::Sub, var("n"), int(1)))
    );
    // A negative number is unary minus over a literal, so `$n -1`, `$n- 1`
    // and `$n-1` are one subtraction (section 3).
    for spelled in ["$n -1 > 0", "$n- 1 > 0", "$n-1 > 0"] {
        assert_eq!(where_expr(spelled), where_expr("$n - 1 > 0"), "{spelled}");
    }
    assert_eq!(
        where_expr("$n - $m > 0"),
        gt(arith(ArithOp::Sub, var("n"), var("m")))
    );
    assert_eq!(where_expr("$n-$m > 0"), where_expr("$n - $m > 0"));
    // In `return`, items are separated by commas, so `$a -$b` is one item.
    let r = read("query q() { match { $p: Person } return { $p.age -1 as a, -$p.age as b } }");
    assert_eq!(r.projections.len(), 2);
    // `$a -knows- $b` is the arrow, refused with the triple as the fix.
    let e = element_refusal("$a -knows- $b");
    assert_eq!(e.code.as_str(), "Q007");
    assert_eq!(e.fix.as_deref(), Some("$a <knows> $b"));
    let e = element_refusal("$a -knows-> $b");
    assert_eq!(e.fix.as_deref(), Some("$a knows $b"));
    let e = element_refusal("$a <-knows- $b");
    assert_eq!(e.fix.as_deref(), Some("$b knows $a"));
    let e = element_refusal("$a -$k:knows{1,2}-> $b");
    assert_eq!(e.fix.as_deref(), Some("$a $k:knows{1,2} $b"));
    let e = element_refusal("$a -in-> $b");
    assert_eq!(e.fix.as_deref(), Some("$a in $b"));
    let e = element_refusal("$a -(knows | likes)-> $b");
    assert_eq!(e.fix.as_deref(), Some("$a (knows | likes) $b"));
}

#[test]
fn the_ladder_has_the_usual_precedence_and_associativity() {
    // `*` binds tighter than `+`; both are left-associative; unary minus
    // binds tightest; comparison is below arithmetic and above `not`.
    assert_eq!(
        where_expr("$a + $b * $c > 0"),
        cmp(
            CompOp::Gt,
            arith(
                ArithOp::Add,
                var("a"),
                arith(ArithOp::Mul, var("b"), var("c"))
            ),
            int(0)
        )
    );
    assert_eq!(
        where_expr("$a - $b - $c > 0"),
        cmp(
            CompOp::Gt,
            arith(
                ArithOp::Sub,
                arith(ArithOp::Sub, var("a"), var("b")),
                var("c")
            ),
            int(0)
        )
    );
    assert_eq!(
        where_expr("$a / $b % $c > 0"),
        cmp(
            CompOp::Gt,
            arith(
                ArithOp::Rem,
                arith(ArithOp::Div, var("a"), var("b")),
                var("c")
            ),
            int(0)
        )
    );
    assert_eq!(
        where_expr("0 < -$a * $b"),
        cmp(
            CompOp::Lt,
            int(0),
            arith(ArithOp::Mul, Expr::Neg(Box::new(var("a"))), var("b"))
        )
    );
    assert_eq!(
        where_expr("-$a > 0"),
        cmp(CompOp::Gt, Expr::Neg(Box::new(var("a"))), int(0))
    );
    assert_eq!(
        where_expr("(-$a) > 0"),
        cmp(CompOp::Gt, Expr::Neg(Box::new(var("a"))), int(0))
    );
    // A newline is not a separator: `$a.x` then `-$b.y` on the next line is
    // one subtraction.
    assert_eq!(
        where_expr("$a.x\n  -$b.y > 0"),
        where_expr("$a.x - $b.y > 0")
    );
    assert_eq!(
        where_expr("$a * ($b + $c) > --1"),
        cmp(
            CompOp::Gt,
            arith(
                ArithOp::Mul,
                var("a"),
                arith(ArithOp::Add, var("b"), var("c"))
            ),
            Expr::Neg(Box::new(Expr::Neg(Box::new(int(1)))))
        )
    );
    assert!(matches!(
        where_expr("not $a + 1 > 2 and $b"),
        Expr::And(l, _) if matches!(*l, Expr::Not(_))
    ));
    // Comparison stays non-associative.
    assert!(parse_text("query q() { match { $x: X } where $a < $b < $c return { $a } }").is_err());
    // Arithmetic in `return`, `order`, `let`, `where` and an assignment.
    let r = read(
        "query doubled($min: I32) { match { $p: Person, let $born = 2026 - $p.age } where $p.age = $min + 1 and $p.age * 2 >= $min return { $p.name, $p.age * 2 as twice, (2026 - $p.age) as born_about } order { -$p.age, $born % 10 desc } }",
    );
    assert_eq!(r.projections.len(), 3);
    let w = write(
        "query birthday($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: $p.age + 1 } }",
    );
    assert!(
        matches!(&w.all_statements()[0], Stmt::Update { assignments, .. } if matches!(assignments[0].value, Expr::Arith { .. }))
    );
}

#[test]
fn float_literals_take_an_exponent() {
    use crate::query::value::Literal;
    for (text, value) in [
        ("1.5e3", 1.5e3),
        ("2.0E-2", 2.0e-2),
        ("6.02e+23", 6.02e23),
        ("0.5", 0.5),
    ] {
        assert_eq!(
            where_expr(&format!("$x.f > {text}")),
            cmp(
                CompOp::Gt,
                Expr::Prop {
                    variable: "x".into(),
                    property: "f".into()
                },
                Expr::Literal(Literal::Float(value))
            ),
            "{text}"
        );
    }
    // A negative exponent float is unary minus over the literal.
    assert!(matches!(
        where_expr("$x.f > -1.0e-3"),
        Expr::Compare { right, .. } if matches!(*right, Expr::Neg(_))
    ));
    // `1e3` without a fraction is not a float. The RFC's `float_lit` admits
    // only `d.d[e…]`; with an unbounded integer rule, `1e3` would read as
    // `1` followed by the bare name `e3`. The word boundary on `integer`
    // makes it a parse error instead.
    assert!(parse_text("query q() { match { $x: X } return { 1e3 } }").is_err());
    assert!(parse_text("query q() { match { $x: X } return { 1.5e } }").is_err());
}

// ---------------------------------------------------------------------------
// The reserved set.

/// Every word the grammar spells as a keyword, other than the six
/// reserved ones.
const CONTEXTUAL: &[&str] = &[
    "query",
    "match",
    "return",
    "order",
    "limit",
    "distinct",
    "as",
    "asc",
    "desc",
    "let",
    "from",
    "optional",
    "exists",
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "insert",
    "update",
    "delete",
    "set",
    "where",
    "commit",
    "unattached",
    "branch",
    "on",
    "mutation",
    "gq",
    "true",
    "false",
    "date",
    "datetime",
    "now",
    "nearest",
    "bm25",
    "rrf",
    "l2",
    "union",
    "search",
    "fuzzy",
    "match_text",
    "terms",
    "match_terms",
    "contains",
    "starts_with",
    "if",
    "coalesce",
    "concat",
    "show",
    "reset",
    "all",
    "explain",
    "load",
    "csv",
    "into",
    "mode",
    "for",
    "unwind",
    "case",
    "having",
    "retrieve",
    "view",
    "batch",
    "returning",
    "recall",
    "k",
    "weights",
    "to",
    "merge",
    "diff",
    "list",
    "create",
];
const RESERVED: &[&str] = &["and", "or", "not", "is", "null", "in"];

#[test]
fn the_reserved_set_is_the_six_words() {
    for word in CONTEXTUAL {
        // A property after a dot, an edge, an alias and a bare mutation
        // property.
        let src = format!(
            "query q() {{ match {{ $p: Person, $p {word} $q }} where $p.{word} = 1 return {{ $p.{word} as {word} }} }}"
        );
        let r = read(&src);
        assert!(
            matches!(&r.pattern.elements[1], Element::Traversal(_)),
            "`{word}` as an edge: {:#?}",
            r.pattern.elements
        );
        assert_eq!(r.projections[0].alias.as_deref(), Some(*word), "{word}");
        let w = write(&format!("query w() {{ insert Person {{ {word}: 1 }} }}"));
        assert_eq!(w.all_statements().len(), 1, "{word}");
        let w = write(&format!(
            "query w() {{ match {{ $p: Person }} update $p set {{ {word}: 2 }} }}"
        ));
        assert_eq!(w.all_statements().len(), 1, "{word}");
    }
    for word in RESERVED {
        // Refused as an alias; a property after a dot and an edge name are
        // fine.
        assert!(
            parse_text(&format!(
                "query q() {{ match {{ $p: P }} return {{ $p.a as {word} }} }}"
            ))
            .is_err(),
            "{word} as an alias"
        );
        read(&format!(
            "query q() {{ match {{ $p: P }} where $p.{word} = 1 return {{ $p.{word} }} }}"
        ));
        assert!(
            matches!(one_element(&format!("$a {word} $b")), Element::Traversal(_)),
            "{word} as an edge"
        );
    }
}

/// `$d in $x` inside `match` is a triple over the edge named `in`; in
/// `where` it is the membership test, which the type checker refuses with
/// the triple's place as the fix when `$x` is a node, and inside `match`
/// with a list on the right it is `T7` with the `where` as the fix.
#[test]
fn in_is_an_edge_in_match_and_membership_in_where() {
    let Element::Traversal(t) = one_element("$d in $x") else {
        panic!()
    };
    assert_eq!(t.selector, EdgeSelector::Named("in".into()));
    assert_eq!(
        where_expr("$d in $x"),
        Expr::In {
            needle: Box::new(var("d")),
            list: Box::new(var("x")),
            at: SourceSpan::default(),
        }
    );
}

// ---------------------------------------------------------------------------
// Refused respellings with their fix.

#[test]
fn delete_and_update_with_where_print_the_group() {
    let e = refusal_of("query retire($h: String) { delete WorksAt where @src = $h }");
    assert_eq!(e.code.as_str(), "Q008");
    assert_eq!(
        e.fix.as_deref(),
        Some("match { $x: WorksAt } where $x.@src = $h  delete $x")
    );
    let e = refusal_of(
        "query retire($h: String) { update Person set { city: \"x\" } where handle = $h and age > 3 }",
    );
    assert_eq!(e.code.as_str(), "Q008");
    assert_eq!(
        e.fix.as_deref(),
        Some(
            "match { $x: Person } where $x.handle = $h and $x.age > 3  update $x set { city: \"x\" }"
        )
    );
    // Over a bound variable the predicate joins the group's `where`.
    for (src, intended) in [
        (
            "query q() { match { $x: Person } delete $x where $x.age > 3 }",
            "query q() { match { $x: Person } where $x.age > 3 delete $x }",
        ),
        (
            "query q() { match { $x: Person } where $x.age < 90 update $x set { a: 1 } where $x.age > 3 }",
            "query q() { match { $x: Person } where $x.age < 90 and $x.age > 3 update $x set { a: 1 } }",
        ),
    ] {
        let e = refusal_of(src);
        assert_eq!(e.code.as_str(), "Q008", "{src}");
        let fixed = super::parse::apply_edits(src, &e.suggestion.as_ref().unwrap().edits);
        assert_eq!(parse_source(&fixed).unwrap(), parsed(intended), "{fixed}");
    }
}

#[test]
fn the_mutation_keyword_is_refused_with_query_as_the_fix() {
    let e = refusal_of("mutation hire($h: String) { insert Person { handle: $h } }");
    assert_eq!(e.code.as_str(), "Q009");
    // The fix is the construct the diagnostic points at, the keyword, with
    // the suggestion applied.
    assert_eq!(e.fix.as_deref(), Some("query"));
    let edit = &e.suggestion.as_ref().unwrap().edits[0];
    assert_eq!(edit.replacement, "query");
    // A read body under `mutation` gets the same fix.
    assert_eq!(
        code3("mutation r() { match { $p: P } return { $p } }"),
        "Q009"
    );
    // `mutation` stays an identifier everywhere else.
    read(
        "query mutation() { match { $p: P } where $p.mutation = 1 return { $p.mutation as mutation } }",
    );
}

#[test]
fn as_commit_is_a_write_body_clause() {
    let d = decl(
        "query retire($h: String) as commit { match { $p: Person } where $p.handle = $h delete $p }",
    );
    assert_eq!(d.write_mode, MutationWriteMode::AsCommit);
    let d = decl(
        "query propose($h: String) as unattached branch on $h { insert Person { handle: $h, name: $h } }",
    );
    assert!(matches!(d.write_mode, MutationWriteMode::Unattached { .. }));
    let e = refusal_of("query r() as commit { match { $p: P } return { $p } }");
    assert_eq!(e.code.as_str(), "Q011");
    let e = refusal_of("query r() as unattached branch { match { $p: P } return { $p } }");
    assert_eq!(e.code.as_str(), "Q011");
}

#[test]
fn a_write_groups_elements_are_in_its_match() {
    // Elements written without `match { }`, before or after the statements.
    for (src, intended) in [
        (
            "query w($h: String) { $p: Person, $p.handle = $h delete $p }",
            "query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }",
        ),
        (
            "query w($h: String) { $p: Person { handle: $h } delete $p }",
            "query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }",
        ),
        (
            "query w() { delete $p  $p: Person }",
            "query w() { match { $p: Person } delete $p }",
        ),
        (
            "query w() { match { $p: Person } delete $p  $p.age > 3 }",
            "query w() { match { $p: Person } where $p.age > 3 delete $p }",
        ),
    ] {
        let e = refusal_of(src);
        assert_eq!(e.code.as_str(), "Q013", "{src}: {e:?}");
        let fixed = super::parse::apply_edits(src, &e.suggestion.as_ref().unwrap().edits);
        assert_eq!(
            parse_source(&fixed).unwrap_or_else(|e| panic!("{fixed}\n{e:?}")),
            parsed(intended),
            "{fixed}"
        );
    }
    // Several statements after one selection, and later groups opening with
    // their own `match`.
    let w = write(
        "query close_company($slug: String) { match { $c: Company, $p $k:worksAt $c } where $c.slug = $slug delete $k  update $p set { city: null } }",
    );
    assert_eq!((w.all_elements().len(), w.all_statements().len()), (2, 2));
    let w = write(
        "query two() { insert Person { handle: \"a\" } match { $p: Person } where $p.handle = \"a\" update $p set { age: 1 } match { $q: Person } update $q set { age: 2 } }",
    );
    assert_eq!(w.groups.len(), 3);
    assert!(w.groups[0].pattern.elements.is_empty());
    // `null` is an assignment value; in operand position it stays a
    // reserved word.
    assert!(
        matches!(&w.all_statements()[1], Stmt::Update { assignments, .. }
        if assignments[0].value == int(1))
    );
    let w = write("query w() { match { $p: P } update $p set { a: null, b: 1 } }");
    assert!(
        matches!(&w.all_statements()[0], Stmt::Update { assignments, .. }
        if assignments[0].value == Expr::Literal(crate::query::value::Literal::Null))
    );
    write("query w() { insert P { a: null,  b: 1 } }");
    assert!(parse_text("query w() { insert P { a: null + 1 } }").is_err());
    assert!(parse_text("query q() { match { $p: P } where $p.a = null return { $p } }").is_err());
}

#[test]
fn other_respellings_print_their_fix() {
    for (src, fix) in [
        ("$a || $b = \"x\"", "concat($a, $b)"),
        (
            "case when $d.year > 1 then 1 else 2 end = 1",
            "if($d.year > 1, 1, 2)",
        ),
    ] {
        let e = refusal_of(&format!(
            "query q() {{ match {{ $d: Doc }} where {src} return {{ $d }} }}"
        ));
        assert_eq!(e.code.as_str(), "Q012", "{src}");
        assert_eq!(e.fix.as_deref(), Some(fix), "{src}");
    }
    for (src, fix) in [
        ("let $x in $r", "from $r"),
        ("let ($a, $b) in $r", "from $r"),
    ] {
        let e = element_refusal(src);
        assert_eq!(e.code.as_str(), "Q012", "{src}");
        assert_eq!(e.fix.as_deref(), Some(fix), "{src}");
    }
    // Over a list there is no row form, and the parser cannot tell a list
    // from a relation: both readings are printed.
    for src in ["for $x in $list", "unwind $list as $x"] {
        let e = element_refusal(src);
        assert_eq!(e.code.as_str(), "Q012", "{src}");
        let fix = e.fix.unwrap();
        assert!(
            fix.contains("from $list") && fix.contains("in $list"),
            "{src}: {fix}"
        );
    }
    // Infix `contains` and `starts_with` parse to a node the type checker
    // refuses (`T7`), since the left operand's type picks the fix.
    for src in ["$d.title starts_with \"O\"", "$d.tags contains \"rust\""] {
        assert!(matches!(where_expr(src), Expr::InfixString { .. }));
    }
    // Every annotation parses; the type checker decides which names it reads
    // (`T80`), so a later typed annotation is additive.
    parse_text("query q() @if_commit($c) { match { $p: P } delete $p }").unwrap();
    parse_text("query q() @valid(from = 1) { match { $p: P } return { $p } }").unwrap();
    // `retrieve`, `having` and `view` are refused at the word, each naming
    // the GQ form (section 3's refused respellings).
    for (src, code) in [
        (
            "query q() { match { $d: Doc } retrieve { $d.e: nearest($q) } return { $d } }",
            "Q020",
        ),
        (
            "query q() { match { $d: Doc } return { $d } having { $d } }",
            "Q014",
        ),
        ("view v() { match { $d: Doc } return { $d } }", "Q009"),
    ] {
        assert_eq!(code3(src), code, "{src}");
    }
}

// ---------------------------------------------------------------------------
// Declarations.

#[test]
fn one_keyword_and_the_body_shape_is_the_kind() {
    assert!(matches!(
        decl("query r() { match { $p: P } return { $p } }").body,
        DeclBody::Read(_)
    ));
    assert!(matches!(
        decl("query w() { insert P { a: 1 } }").body,
        DeclBody::Write(_)
    ));
    // A write whose selection comes from a ranked read.
    let w = write(
        "query tag_near($q: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $q, k: 20) } update $d set { tags: [\"near\"] } }",
    );
    assert_eq!((w.all_elements().len(), w.all_statements().len()), (1, 1));
    // A statement keyword stays a property name, after a dot and as a key.
    write("query w() { match { $p: P } where $p.insert = 1 and $p.update = 2 delete $p }");
    // Parameters with defaults, `limit $k`, generic annotations.
    let d = decl(
        "query top_people($k: I64 = 10, $neg: I32 = -1, $s: String? = \"x\") @description(\"d\") @budget(plan = 1000) @example(\"e\") { match { $p: Person } return { $p.name } order { $p.age desc } limit $k }",
    );
    assert!(d.params[1].default.as_ref().unwrap().negative);
    assert_eq!(d.annotations.len(), 3);
    let DeclBody::Read(r) = d.body else { panic!() };
    assert_eq!(r.limit, Some(Limit::Param("k".into())));
    // Several declarations in one file.
    let f = parsed("query a() { match { $p: P } return { $p } }\nquery b() { insert P { a: 1 } }");
    assert!(matches!(f.body, Body::Decls(ref d) if d.len() == 2));
    // `explain` over a declaration.
    assert!(matches!(
        parsed("explain query a() { match { from $r } return { $p } }").body,
        Body::Explain(_)
    ));
    // The missing parameter list keeps its fix.
    let e = refusal_of("query a { match { $p: P } return { $p } }");
    assert_eq!(
        (e.code.as_str(), e.fix.as_deref()),
        ("Q002", Some("query a()"))
    );
}

#[test]
fn calls_take_positionals_then_options_and_one_distinct_form() {
    let Expr::Call(c) = where_expr("fuzzy($x.title, $q, max_edits: 2)") else {
        panic!()
    };
    assert_eq!((c.args.len(), c.options.len()), (2, 1));
    // An option before a positional is refused with the reordered arguments.
    let e = refusal_of("query q() { match { $x: X } where f(k: 1, $x) return { 1 } }");
    assert_eq!(e.code.as_str(), "Q005");
    assert_eq!(e.fix.as_deref(), Some("$x, k: 1"));
    // Options only.
    where_expr("f(k: 1, mode: any)");
    // `distinct` under any aggregate name (the type checker keeps T57).
    let r = read(
        "query q() { match { $p: P } return { count(distinct $p.city) as c, sum(distinct $p.age) as s } }",
    );
    assert!(matches!(r.projections[1].expr, Expr::AggDistinct { .. }));
    // `date("…")` is a literal everywhere, including inside a list.
    assert!(matches!(
        where_expr("$x.d in [date(\"2025-01-01\"), date(\"2025-01-02\")]"),
        Expr::In { .. }
    ));
    // Nested relation functions.
    one_element(
        "from rrf(nearest($docs, $d.embedding, $v, k: 50), bm25($docs, $d.body, $q), k: 60, weights: [0.7, 0.3])",
    );
    // A block value as an operand, compared with another binding's value.
    let e = where_expr("count { $d in $x } + 1 >= $x.min_docs");
    assert!(matches!(e, Expr::Compare { left, .. } if matches!(*left, Expr::Arith { .. })));
}

#[test]
fn a_block_is_an_operand() {
    // One tree for one text: `count { p } > 1 or $x` is the disjunction, as
    // the same text is anywhere else an expression stands, never
    // `count { p } > (1 or $x)`.
    assert!(matches!(
        where_expr("count { $a: P } > 1 or $x"),
        Expr::Or(..)
    ));
    assert!(matches!(where_expr("exists { $a: P }"), Expr::Block(_)));
    // A block written among the elements is a predicate, moved to `where`.
    assert_eq!(
        element_refusal("$x: X, exists { $a: P }").code.as_str(),
        "Q017"
    );
}

#[test]
fn a_product_of_two_variables_is_not_the_wildcard_triple() {
    // `$a * $b` is the wildcard triple in `match` and a product in an
    // expression; with predicates out of `match`, the two never meet.
    assert_eq!(
        where_expr("$n * $m > 10"),
        cmp(CompOp::Gt, arith(ArithOp::Mul, var("n"), var("m")), int(10))
    );
    assert!(matches!(where_expr("$n * $m or $ok"), Expr::Or(..)));
    assert!(matches!(one_element("$a * $b"), Element::Traversal(_)));
    assert!(matches!(one_element("$a *{1,3} $b"), Element::Traversal(_)));
    assert!(matches!(one_element("$a $k:* $b"), Element::Traversal(_)));
    let c = match_elements("$a * $b, $b: Person");
    assert!(matches!(c[0], Element::Traversal(_)), "{c:#?}");
    assert_eq!(c.len(), 2);
    // Followed by an operator, it is a filter written among the elements.
    assert_eq!(element_refusal("$a * $b > 3").code.as_str(), "Q017");
}

#[test]
fn a_bare_from_or_let_before_a_variable_is_the_element() {
    // The contextual keyword wins over a bare name at element start; a bare
    // name is never a value in a read.
    assert!(parse_text("query q() { match { from  $a: Person } return { 1 } }").is_err());
    assert!(matches!(one_element("let  $a = 1"), Element::Let { .. }));
}

#[test]
fn a_call_to_contains_is_a_call() {
    // The infix recognizer is word-bounded and does not touch `(`, so this
    // is a call, not `$p.active contains (…)`.
    assert!(matches!(
        where_expr("$p.active and contains($p.name, \"a\") and starts_with($p.name, \"b\")"),
        Expr::And(..)
    ));
    // The infix form with a space is still recognised, as the tree node the
    // type checker refuses.
    assert!(matches!(
        where_expr("$p.name contains \"a\""),
        Expr::InfixString { .. }
    ));
}

#[test]
fn a_date_call_is_the_date_literal() {
    // `date("…")` is the literal production, never a call to a function
    // named `date`, with or without a space before its parenthesis.
    for spelled in ["date(\"2025-01-01\")", "date (\"2025-01-01\")"] {
        assert_eq!(
            where_expr(&format!("$x.d = {spelled}")),
            cmp(
                CompOp::Eq,
                Expr::Prop {
                    variable: "x".into(),
                    property: "d".into()
                },
                Expr::Literal(crate::query::value::Literal::Date("2025-01-01".into()))
            )
        );
    }
    // With another argument shape it is a call.
    assert!(matches!(
        where_expr("$x.d = date($s)"),
        Expr::Compare { right, .. } if matches!(*right, Expr::Call(_))
    ));
}

#[test]
fn recognizers_shadow_only_ill_typed_text() {
    // The arrow recognizer takes `$a - x - $b`, `x` a bare name, a string or
    // a bare meta-field; in `where` such text is arithmetic over a bare name
    // (refused at the word) or never well typed.
    for middle in ["x", "\"s\"", "@id"] {
        assert_eq!(
            element_refusal(&format!("$a - {middle} - $b"))
                .code
                .as_str(),
            "Q007",
            "{middle}"
        );
    }
    // A number, a property or a parenthesised term in the middle is
    // arithmetic.
    for middle in ["1", "$x.n", "($x)"] {
        where_expr(&format!("$a - {middle} - $b > 0"));
    }
    // `let ($a) in …` is the refused respelling.
    assert_eq!(element_refusal("let($a) in [1]").code.as_str(), "Q012");
}

#[test]
fn reserved_words_in_where() {
    // `$a and $b` in `where` is the conjunction of two Boolean operands;
    // `is` and `null` between two variables are parse errors.
    assert!(matches!(where_expr("$a and $b"), Expr::And(..)));
    assert!(matches!(where_expr("$a or $b"), Expr::Or(..)));
    assert!(matches!(where_expr("$a in $b"), Expr::In { .. }));
    for word in ["is", "null"] {
        assert!(
            parse_text(&format!(
                "query q() {{ match {{ $a: Person }} where $a {word} $b return {{ 1 }} }}"
            ))
            .is_err(),
            "{word}"
        );
    }
}

// ---------------------------------------------------------------------------
// The rule-set pin (section 11, rule 1).

/// Every rule `grammar.pest` defines, sorted.
const SYNTAX_RULES: &[&str] = &[
    "BLOCK_COMMENT",
    "COMMENT",
    "LINE_COMMENT",
    "WHITESPACE",
    "agg_distinct",
    "agg_name",
    "alias",
    "alias_string_rec",
    "and_expr",
    "annotation",
    "annotation_arg",
    "annotation_colon_rec",
    "annotation_kwarg",
    "annotation_name",
    "annotation_value",
    "arrow_close",
    "arrow_open",
    "arrow_traversal",
    "as_commit_clause",
    "as_unattached_clause",
    "assignment",
    "assignments",
    "bare_elements_rec",
    "bare_name_rec",
    "base_type",
    "between_rec",
    "binding",
    "binding_brace_filter",
    "binding_braces_rec",
    "binding_lower_rec",
    "binding_paren_head",
    "binding_paren_rec",
    "block_body",
    "block_value",
    "blockless_rec",
    "bool_lit",
    "brace_body",
    "brace_colon_compare",
    "brace_compare",
    "brace_item",
    "brace_items_rec",
    "branch_create",
    "branch_delete",
    "branch_diff",
    "branch_list",
    "branch_merge",
    "branch_merge_base",
    "branch_name",
    "branch_stmt",
    "call",
    "call_arg",
    "call_args",
    "call_block_rec",
    "call_name",
    "call_option",
    "case_rec",
    "catalogue_edge",
    "chained_rec",
    "commit_drop",
    "commit_publish",
    "commit_stmt",
    "comp_op",
    "comparison",
    "concat_bars",
    "continuation",
    "count_star_rec",
    "create_from_commit",
    "create_rec",
    "cypher_hops",
    "cypher_name",
    "cypher_node",
    "cypher_pattern",
    "cypher_prop",
    "cypher_rel",
    "cypher_rel_body",
    "cypher_typed_node",
    "date_lit",
    "datetime_lit",
    "declaration",
    "delete_multi_rec",
    "delete_stmt",
    "delete_type_rec",
    "detach_rec",
    "diff_after_page",
    "diff_limit",
    "digits",
    "dir_first_rec",
    "distinct_item_rec",
    "dry_run_clause",
    "edge_alternation",
    "edge_alternatives",
    "edge_binding",
    "edge_group_rec",
    "edge_ident",
    "edge_name",
    "edge_sel",
    "edge_wildcard",
    "element",
    "elements",
    "eq_assign_rec",
    "exp_float_rec",
    "explain_stmt",
    "expr",
    "expr_ident",
    "filter_rec",
    "float_lit",
    "from_clause",
    "from_paren_rec",
    "fw_between",
    "fw_by",
    "fw_call",
    "fw_case",
    "fw_create",
    "fw_detach",
    "fw_else",
    "fw_end",
    "fw_ends",
    "fw_filter",
    "fw_group",
    "fw_having",
    "fw_like",
    "fw_match",
    "fw_offset",
    "fw_optional",
    "fw_retrieve",
    "fw_starts",
    "fw_then",
    "fw_when",
    "fw_with",
    "group_by_rec",
    "having_rec",
    "ident",
    "ident_chars",
    "in_paren_rec",
    "infix_string_op",
    "inline_read",
    "inner_where_rec",
    "insert_paren_rec",
    "insert_stmt",
    "integer",
    "keyword_call_rec",
    "kw_after",
    "kw_all",
    "kw_and",
    "kw_as",
    "kw_branch",
    "kw_columns",
    "kw_commit",
    "kw_create",
    "kw_csv",
    "kw_date",
    "kw_datetime",
    "kw_delete",
    "kw_delimiter",
    "kw_diff",
    "kw_distinct",
    "kw_drop",
    "kw_dry",
    "kw_exists",
    "kw_explain",
    "kw_for",
    "kw_from",
    "kw_header",
    "kw_in",
    "kw_insert",
    "kw_into",
    "kw_is",
    "kw_let",
    "kw_limit",
    "kw_list",
    "kw_load",
    "kw_match",
    "kw_merge",
    "kw_merge_base",
    "kw_mode",
    "kw_mutation",
    "kw_not",
    "kw_null",
    "kw_on",
    "kw_optional",
    "kw_or",
    "kw_order",
    "kw_page",
    "kw_publish",
    "kw_query",
    "kw_reset",
    "kw_return",
    "kw_run",
    "kw_set",
    "kw_show",
    "kw_to",
    "kw_unattached",
    "kw_unwind",
    "kw_update",
    "kw_view",
    "kw_where",
    "let_clause",
    "like_rec",
    "limit_clause",
    "limit_neg_rec",
    "list_lit",
    "list_to_rows",
    "list_type",
    "literal",
    "load_columns",
    "load_delimiter",
    "load_header",
    "load_kind",
    "load_mode",
    "load_mode_name",
    "load_option",
    "load_stmt",
    "load_target",
    "match_clause",
    "match_nobrace_rec",
    "match_prefix_rec",
    "matchless_read_rec",
    "meta_assign_rec",
    "meta_field",
    "missing_comma_rec",
    "missing_param_list",
    "mutation_body",
    "mutation_decl",
    "mutation_stmt",
    "neg_sign",
    "negation_rec",
    "not_block_rec",
    "not_expr",
    "not_in_rec",
    "null_compare_rec",
    "null_test",
    "null_value",
    "offset_rec",
    "operand",
    "optional_block",
    "optional_rec",
    "or_expr",
    "order_by_rec",
    "order_clause",
    "order_dir",
    "ordering",
    "param",
    "param_default",
    "param_item",
    "param_list",
    "param_untyped_rec",
    "positional",
    "prefix_rec",
    "product",
    "product_op",
    "projection",
    "prop_access",
    "prop_chain_rec",
    "prop_match",
    "prop_match_list",
    "prop_method_head",
    "prop_method_rec",
    "prop_star_rec",
    "qualified_assign_rec",
    "query_decl",
    "query_file",
    "read_body",
    "read_tail",
    "regex_rec",
    "relation_expr",
    "reserved",
    "reserved_alias",
    "reserved_property",
    "reset_stmt",
    "retrieve_rec",
    "return_clause",
    "return_nobrace_rec",
    "set_item",
    "set_rec",
    "set_stmt",
    "setting_name",
    "setting_stmt",
    "setting_value",
    "show_stmt",
    "sql_alias_rec",
    "star_item_rec",
    "starts_with_words_rec",
    "statement_trailer",
    "stmt_keyword",
    "stmt_var_rec",
    "stray_braces_rec",
    "string_char",
    "string_lit",
    "sum",
    "sum_op",
    "swapped_read_rec",
    "trailing_elements_rec",
    "trailing_match_rec",
    "traversal",
    "traversal_bounds",
    "type_name",
    "type_prop_rec",
    "type_ref",
    "unary",
    "unary_minus",
    "undirected_edge",
    "update_noset_rec",
    "update_stmt",
    "variable",
    "vector_type",
    "view_decl",
    "where_clause",
    "where_stmt",
    "with_item",
    "with_rec",
    "write_element",
    "write_group",
];

/// No new grammar rule without an amendment (section 11, rule 1): a rule
/// added to or removed from the grammar turns this red, and the fix is an
/// amendment to the GQ RFC that names the production, the token the grammar
/// refused where it starts, and the collision it designs around.
/// Recognizers (`_rec`, and the rules only they use) print a fix and accept
/// nothing section 7 refuses, but they are pinned too, so a recognizer that
/// starts accepting text is seen.
#[test]
fn the_rule_set_is_pinned() {
    let grammar = include_str!("grammar.pest");
    let mut rules: Vec<&str> = grammar
        .lines()
        .filter_map(|line| {
            let (name, _) = line.split_once('=')?;
            let name = name.trim();
            (!name.is_empty()
                && !line.starts_with(char::is_whitespace)
                && !line.starts_with("//")
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
            .then_some(name)
        })
        .collect();
    rules.sort_unstable();
    rules.dedup();
    assert_eq!(
        rules, SYNTAX_RULES,
        "the grammar's rule set moved; a grammar change is an amendment to the GQ RFC (section 11, rule 1)"
    );
}

// ---------------------------------------------------------------------------
// Repairs: one row per distinct refusal shape agents' texts reached, each the
// refused text, its code, and the text its suggestion must reparse to.

/// `(shape, code, refused text, intended text)`.
const PARSE_REPAIRS: &[(&str, &str, &str, &str)] = &[
    (
        r#"a declaration without its parameter list"#,
        "Q002",
        r#"query q {
  match { $p: Person }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"an exponent without a fraction"#,
        "Q005",
        r#"query q() {
  match { $d: Doc }
  where $d.year * 1.0 > 2e3
  return { $d.title }
}"#,
        r#"query q() { match { $d: Doc } where $d.year * 1.0 > 2000.0 return { $d.title } }"#,
    ),
    (
        r#"a reserved word as an alias"#,
        "Q005",
        r#"query q() {
  match { $p: Person }
  return { $p.age as in }
  order { in desc }
}"#,
        r#"query q() { match { $p: Person } return { $p.age as in_ } order { in_ desc } }"#,
    ),
    (
        r#"an option before a positional argument"#,
        "Q005",
        r#"query q($q: Vector(3)) {
  match { from nearest({ match { $d: Doc } return { $d } }, k: 20, $d.embedding, $q) }
  return { $d.title }
}"#,
        r#"query q($q: Vector(3)) { match { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $q, k: 20) } return { $d.title } }"#,
    ),
    (
        r#"an endpoint written as a meta-field"#,
        "Q005",
        r#"query hire($h: String, $slug: String) {
  insert WorksAt { @src: $h, @dst: $slug }
}"#,
        r#"query hire($h: String, $slug: String) { insert WorksAt { from: $h, to: $slug } }"#,
    ),
    (
        r#"an assignment with ="#,
        "Q005",
        r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p set { age = 3 }
}"#,
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }"#,
    ),
    (
        r#"a qualified assignment target"#,
        "Q005",
        r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p set { $p.age: 3 }
}"#,
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }"#,
    ),
    (
        r#"update without set"#,
        "Q005",
        r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p { age: 3 }
}"#,
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }"#,
    ),
    (
        r#"an untyped parameter"#,
        "Q005",
        r#"query q($h) {
  match { $p: Person } where $p.handle = $h
  return { $p.name }
}"#,
        r#"query q($h: String) { match { $p: Person } where $p.handle = $h return { $p.name } }"#,
    ),
    (
        r#"an outgoing arrow"#,
        "Q007",
        r#"query q() {
  match { $a: Person, $a -knows-> $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person, $a knows $b } return { $b.name } }"#,
    ),
    (
        r#"an incoming arrow"#,
        "Q007",
        r#"query q() {
  match { $a: Person, $a <-knows- $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person, $b knows $a } return { $b.name } }"#,
    ),
    (
        r#"an undirected arrow"#,
        "Q007",
        r#"query q() {
  match { $a: Person, $a -knows- $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person, $a <knows> $b } return { $b.name } }"#,
    ),
    (
        r#"an arrow with an edge binding and bounds"#,
        "Q007",
        r#"query q() {
  match { $a: Person, $a -$k:knows{1,2}-> $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person, $a $k:knows{1,2} $b } return { $b.name } }"#,
    ),
    (
        r#"Cypher's bracketed edge"#,
        "Q007",
        r#"query q() {
  match { $a: Person, $a -[:knows]-> $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person, $a knows $b } return { $b.name } }"#,
    ),
    (
        r#"delete T where"#,
        "Q008",
        r#"query w($h: String) {
  delete Person where handle = $h
}"#,
        r#"query w($h: String) { match { $x: Person } where $x.handle = $h delete $x }"#,
    ),
    (
        r#"update T set where"#,
        "Q008",
        r#"query w($h: String) {
  update Person set { age: 3 } where handle = $h
}"#,
        r#"query w($h: String) { match { $x: Person } where $x.handle = $h update $x set { age: 3 } }"#,
    ),
    (
        r#"delete $x where"#,
        "Q008",
        r#"query w() {
  match { $p: Person }
  delete $p where $p.age > 90
}"#,
        r#"query w() { match { $p: Person } where $p.age > 90 delete $p }"#,
    ),
    (
        r#"delete $x where beside the group's where"#,
        "Q008",
        r#"query w() {
  match { $p: Person } where $p.age < 100 or $p.age > 120
  delete $p where $p.age > 90 or $p.age < 3
}"#,
        r#"query w() { match { $p: Person } where ($p.age < 100 or $p.age > 120) and ($p.age > 90 or $p.age < 3) delete $p }"#,
    ),
    (
        r#"delete over an edge type where"#,
        "Q008",
        r#"query w($h: String) {
  delete WorksAt where @src = $h
}"#,
        r#"query w($h: String) { match { $x: WorksAt } where $x.@src = $h delete $x }"#,
    ),
    (
        r#"a where fix that keeps a parameter"#,
        "Q008",
        r#"query w($x: String) {
  delete Person where handle = $x
}"#,
        r#"query w($x: String) { match { $p: Person } where $p.handle = $x delete $p }"#,
    ),
    (
        r#"a where fix that qualifies an assignment"#,
        "Q008",
        r#"query w() {
  update Person set { name: handle } where age > 3
}"#,
        r#"query w() { match { $x: Person } where $x.age > 3 update $x set { name: $x.handle } }"#,
    ),
    (
        r#"the mutation keyword"#,
        "Q009",
        r#"mutation hire($h: String) {
  insert Person { handle: $h, name: $h }
}"#,
        r#"query hire($h: String) { insert Person { handle: $h, name: $h } }"#,
    ),
    (
        r#"the view keyword"#,
        "Q009",
        r#"view big() {
  match { $c: Company }
  return { $c.name }
}"#,
        r#"query big() { match { $c: Company } return { $c.name } }"#,
    ),
    (
        r#"a property map"#,
        "Q010",
        r#"query q($h: String) {
  match { $p: Person { handle: $h }, $p knows $q }
  return { $q.name }
}"#,
        r#"query q($h: String) { match { $p: Person, $p knows $q } where $p.handle = $h return { $q.name } }"#,
    ),
    (
        r#"a comparison inside binding braces"#,
        "Q010",
        r#"query q() {
  match { $p: Person { age > 30 } }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"#,
    ),
    (
        r#"a colon comparison inside binding braces"#,
        "Q010",
        r#"query q() {
  match { $p: Person { age: > 30 } }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"#,
    ),
    (
        r#"as commit on a read"#,
        "Q011",
        r#"query q() as commit {
  match { $p: Person }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"for over a relation"#,
        "Q012",
        r#"query q() {
  match { for $x in { match { $p: Person } return { $p } } }
  return { $x.name }
}"#,
        r#"query q() { match { from { match { $p: Person } return { $p } } } return { $x.name } }"#,
    ),
    (
        r#"let … in a relation"#,
        "Q012",
        r#"query q() {
  match { let $x in { match { $p: Person } return { $p } } }
  return { $x.name }
}"#,
        r#"query q() { match { from { match { $p: Person } return { $p } } } return { $x.name } }"#,
    ),
    (
        r#"let over a tuple"#,
        "Q012",
        r#"query q() {
  match { let ($a, $b) in { match { $p worksAt $c } return { $p, $c } } }
  return { $a.name }
}"#,
        r#"query q() { match { from { match { $p worksAt $c } return { $p, $c } } } return { $a.name } }"#,
    ),
    (
        r#"CASE … END"#,
        "Q012",
        r#"query q() {
  match { $p: Person }
  return { $p.name, CASE WHEN $p.age > 65 THEN "senior" ELSE "active" END as band }
}"#,
        r#"query q() { match { $p: Person } return { $p.name, if($p.age > 65, "senior", "active") as band } }"#,
    ),
    (
        r#"|| concatenation"#,
        "Q012",
        r#"query q() {
  match { $p: Person }
  return { $p.name || " " || $p.handle as label }
}"#,
        r#"query q() { match { $p: Person } return { concat($p.name, " ", $p.handle) as label } }"#,
    ),
    (
        r#"a chained comparison"#,
        "Q012",
        r#"query q() {
  match { $p: Person }
  where 18 <= $p.age < 65
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where 18 <= $p.age and $p.age < 65 return { $p.name } }"#,
    ),
    (
        r#"not { } among the elements"#,
        "Q012",
        r#"query q() {
  match { $p: Person, not { $p worksAt $c } }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where not exists { $p worksAt $c } return { $p.name } }"#,
    ),
    (
        r#"not { } in where"#,
        "Q012",
        r#"query q() {
  match { $p: Person }
  where $p.age > 3 and not { $p worksAt $c }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age > 3 and not exists { $p worksAt $c } return { $p.name } }"#,
    ),
    (
        r#"an element after the last statement"#,
        "Q013",
        r#"query w($h: String) {
  delete $p
  $p: Person
}"#,
        r#"query w($h: String) { match { $p: Person } delete $p }"#,
    ),
    (
        r#"a match after its statement"#,
        "Q013",
        r#"query w() {
  delete $p  match { $p: Person } where $p.age > 3
}"#,
        r#"query w() { match { $p: Person } where $p.age > 3 delete $p }"#,
    ),
    (
        r#"a write's elements without match"#,
        "Q013",
        r#"query w($h: String) {
  $p: Person, $p.handle = $h
  delete $p
}"#,
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }"#,
    ),
    (
        r#"a write's elements without match, with a property map"#,
        "Q013",
        r#"query w($h: String) {
  $p: Person { handle: $h }
  update $p set { age: 3 }
}"#,
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }"#,
    ),
    (
        r#"a Cypher pattern in match"#,
        "Q014",
        r#"query q() {
  match { (a:Person)-[:KNOWS]->(b) }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person, $a knows $b } return { $b.name } }"#,
    ),
    (
        r#"an uppercase WHERE inside match"#,
        "Q017",
        r#"query q() {
  match { $p: Person WHERE $p.age > 30 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"#,
    ),
    (
        r#"an uppercase WHERE after match"#,
        "Q014",
        r#"query q() {
  match { $p: Person } WHERE $p.age > 30
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"#,
    ),
    (
        r#"ORDER BY"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { $p.name }
  order by $p.age desc
}"#,
        r#"query q() { match { $p: Person } return { $p.name } order { $p.age desc } }"#,
    ),
    (
        r#"count(*)"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { count(*) as n }
}"#,
        r#"query q() { match { $p: Person } return { count($p) as n } }"#,
    ),
    (
        r#"Cypher's WITH"#,
        "Q014",
        r#"query q() {
  match { $p worksAt $c, with $c, count($p) as n }
  where $n > 3
  return { $c.name }
}"#,
        r#"query q() { match { from { match { $p worksAt $c } return { $c, count($p) as n } } } where $n > 3 return { $c.name } }"#,
    ),
    (
        r#"an uppercase COALESCE"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { COALESCE($p.city, "none") as c }
}"#,
        r#"query q() { match { $p: Person } return { coalesce($p.city, "none") as c } }"#,
    ),
    (
        r#"STARTS WITH"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.name STARTS WITH "A"
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where starts_with($p.name, "A") return { $p.name } }"#,
    ),
    (
        r#"CREATE"#,
        "Q014",
        r#"query w() {
  CREATE (p:Person {handle: "x", name: "x"})
}"#,
        r#"query w() { insert Person { handle: "x", name: "x" } }"#,
    ),
    (
        r#"SET"#,
        "Q014",
        r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  set $p.age = 3
}"#,
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }"#,
    ),
    (
        r#"DETACH DELETE"#,
        "Q014",
        r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  detach delete $p
}"#,
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }"#,
    ),
    (
        r#"OPTIONAL MATCH"#,
        "Q014",
        r#"query q() {
  match { $p: Person, optional match { $p worksAt $c } }
  return { $p.name, $c.name }
}"#,
        r#"query q() { match { $p: Person, optional { $p worksAt $c } } return { $p.name, $c.name } }"#,
    ),
    (
        r#"CALL { }"#,
        "Q014",
        r#"query q() {
  match { $p: Person, call { match { $p knows $q } return { $q } } }
  return { $q.name }
}"#,
        r#"query q() { match { $p: Person, from { match { $p knows $q } return { $q } } } return { $q.name } }"#,
    ),
    (
        r#"!= null"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.city != null
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.city is not null return { $p.name } }"#,
    ),
    (
        r#"<>"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.city <> "Berlin"
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.city != "Berlin" return { $p.name } }"#,
    ),
    (
        r#"=="#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.city == "Berlin"
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.city = "Berlin" return { $p.name } }"#,
    ),
    (
        r#"single quotes"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.city = 'Berlin'
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.city = "Berlin" return { $p.name } }"#,
    ),
    (
        r#"GROUP BY"#,
        "Q014",
        r#"query q() {
  match { $p worksAt $c }
  return { $c.name, count($p) as n }
  group by $c.name
}"#,
        r#"query q() { match { $p worksAt $c } return { $c.name, count($p) as n } }"#,
    ),
    (
        r#"LIKE"#,
        "Q014",
        r#"query q() {
  match { $d: Doc }
  where $d.title LIKE '%graph%'
  return { $d.title }
}"#,
        r#"query q() { match { $d: Doc } where contains($d.title, "graph") return { $d.title } }"#,
    ),
    (
        r#"BETWEEN"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.age BETWEEN 18 AND 65
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age >= 18 and $p.age <= 65 return { $p.name } }"#,
    ),
    (
        r#"in over a parenthesised list"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.age in (30, 40)
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age in [30, 40] return { $p.name } }"#,
    ),
    (
        r#"not in"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.city not in ["Berlin"]
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where not $p.city in ["Berlin"] return { $p.name } }"#,
    ),
    (
        r#"&&"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.age >= 18 && $p.age < 65
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age >= 18 and $p.age < 65 return { $p.name } }"#,
    ),
    (
        r#"! for not"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where !($p.age > 3)
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where not $p.age > 3 return { $p.name } }"#,
    ),
    (
        r#"a Cypher binding in parentheses"#,
        "Q014",
        r#"query q($h: String) {
  match { $p: Person(handle: $h) }
  return { $p.name }
}"#,
        r#"query q($h: String) { match { $p: Person } where $p.handle = $h return { $p.name } }"#,
    ),
    (
        r#"a lowercase type"#,
        "Q014",
        r#"query q() {
  match { $p: person }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"an uppercase variable"#,
        "Q014",
        r#"query q() {
  match { $P: Person }
  return { $P.name }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"a star projection"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { $p.* }
}"#,
        r#"query q() { match { $p: Person } return { $p } }"#,
    ),
    (
        r#"distinct inside return"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { distinct $p.city }
}"#,
        r#"query q() { match { $p: Person } return distinct { $p.city } }"#,
    ),
    (
        r#"a string alias"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { $p.name as "Name" }
}"#,
        r#"query q() { match { $p: Person } return { $p.name as name } }"#,
    ),
    (
        r#"a SQL comment"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where -- adults only
  $p.age > 18
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age > 18 return { $p.name } }"#,
    ),
    (
        r#"a semicolon after a declaration"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { $p.name }
};"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"optional without braces"#,
        "Q014",
        r#"query q() {
  match { $p: Person, optional $p worksAt $c }
  return { $p.name, $c.name }
}"#,
        r#"query q() { match { $p: Person, optional { $p worksAt $c } } return { $p.name, $c.name } }"#,
    ),
    (
        r#"not without braces"#,
        "Q014",
        r#"query q() {
  match { $p: Person, $q: Person, not $p knows $q }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person, $q: Person } where not exists { $p knows $q } return { $p.name } }"#,
    ),
    (
        r#"delete of two variables"#,
        "Q014",
        r#"query w() {
  match { $p knows $q }
  delete $p, $q
}"#,
        r#"query w() { match { $p knows $q } delete $p delete $q }"#,
    ),
    (
        r#"null in a read filter"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  where $p.city = null
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.city is null return { $p.name } }"#,
    ),
    (
        r#"a direction before its key"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { $p.name }
  order { desc $p.age }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } order { $p.age desc } }"#,
    ),
    (
        r#"a SQL alias = value"#,
        "Q014",
        r#"query q() {
  match { $p: Person }
  return { n = $p.name }
}"#,
        r#"query q() { match { $p: Person } return { $p.name as n } }"#,
    ),
    (
        r#"returning, a later stage"#,
        "Q015",
        r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  delete $p
  returning { @id }
}"#,
        r#"query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }"#,
    ),
    (
        r#"a namespaced type, a later stage"#,
        "Q015",
        r#"query q() {
  match { $o: crm::Person }
  return { $o.name }
}"#,
        r#"query q() { match { $o: Person } return { $o.name } }"#,
    ),
    (
        r#"offset, a later stage"#,
        "Q015",
        r#"query q() {
  match { $p: Person }
  return { $p.name }
  limit 10 offset 5
}"#,
        r#"query q() { match { $p: Person } return { $p.name } limit 10 }"#,
    ),
    (
        r#"a group of one edge name"#,
        "Q012",
        r#"query q() {
  match { $a: Person, $a (knows) $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person, $a knows $b } return { $b.name } }"#,
    ),
    (
        r#"a missing comma between elements"#,
        "Q016",
        r#"query q() {
  match {
    $p: Person
    $p knows $q
  }
  return { $q.name }
}"#,
        r#"query q() { match { $p: Person, $p knows $q } return { $q.name } }"#,
    ),
    (
        r#"a missing comma between return items"#,
        "Q016",
        r#"query q() {
  match { $p: Person }
  return {
    $p.name
    $p.age
  }
}"#,
        r#"query q() { match { $p: Person } return { $p.name, $p.age } }"#,
    ),
    (
        r#"a missing comma between order keys"#,
        "Q016",
        r#"query q() {
  match { $p: Person }
  return { $p.name }
  order { $p.age desc $p.name }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } order { $p.age desc, $p.name } }"#,
    ),
    (
        r#"a missing comma between arguments"#,
        "Q016",
        r#"query q() {
  match { $p: Person }
  where contains($p.name "a")
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where contains($p.name, "a") return { $p.name } }"#,
    ),
    (
        r#"a missing comma between assignments"#,
        "Q016",
        r#"query w() {
  insert Person { handle: "h" name: "n" }
}"#,
        r#"query w() { insert Person { handle: "h", name: "n" } }"#,
    ),
    (
        r#"a filter among the elements"#,
        "Q017",
        r#"query q() {
  match { $p: Person, $p.age > 30 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"#,
    ),
    (
        r#"a where inside the match braces"#,
        "Q017",
        r#"query q() {
  match { $p: Person where $p.age > 30 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"#,
    ),
    (
        r#"a filter among a block's elements"#,
        "Q017",
        r#"query q() {
  match { $p: Person }
  where exists { $p knows $q, $q.age > 3 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where exists { $p knows $q where $q.age > 3 } return { $p.name } }"#,
    ),
    (
        r#"a bare property name"#,
        "Q018",
        r#"query q() {
  match { $p: Person }
  where name = "x"
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.name = "x" return { $p.name } }"#,
    ),
    (
        r#"a bare parameter name"#,
        "Q018",
        r#"query q($h: String) {
  match { $p: Person }
  where $p.handle = h
  return { $p.name }
}"#,
        r#"query q($h: String) { match { $p: Person } where $p.handle = $h return { $p.name } }"#,
    ),
    (
        r#"from with parentheses"#,
        "Q019",
        r#"query q() {
  match { from ({ match { $p: Person } return { $p } }) }
  return { $p.name }
}"#,
        r#"query q() { match { from { match { $p: Person } return { $p } } } return { $p.name } }"#,
    ),
    (
        r#"exists as a function"#,
        "Q019",
        r#"query q() {
  match { $p: Person }
  where exists($p.city)
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } where $p.city is not null return { $p.name } }"#,
    ),
    (
        r#"an inline read without match"#,
        "Q020",
        r#"query q() {
  match { $p: Person, from { $p knows $q return { $p } order { $q.age } } }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person, from { match { $p knows $q } return { $p } order { $q.age } } } return { $p.name } }"#,
    ),
    (
        r#"return without braces"#,
        "Q020",
        r#"query q() {
  match { $p: Person }
  return $p.name
}"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"match without braces"#,
        "Q020",
        r#"query q() {
  match $p: Person
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"return before match"#,
        "Q020",
        r#"query q() {
  return { $p.name }
  match { $p: Person }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"limit before order"#,
        "Q020",
        r#"query q() {
  match { $p: Person }
  return { $p.name }
  limit 10
  order { $p.age }
}"#,
        r#"query q() { match { $p: Person } return { $p.name } order { $p.age } limit 10 }"#,
    ),
    (
        r#"an annotation argument with a colon"#,
        "Q021",
        r#"query q() @budget(plan: 1000) {
  match { $p: Person }
  return { $p.name }
}"#,
        r#"query q() @budget(plan = 1000) { match { $p: Person } return { $p.name } }"#,
    ),
];

/// `text` as a source.
pub(crate) fn as_source(text: &str) -> String {
    text.to_string()
}

#[test]
fn every_refusal_shape_prints_a_repair_that_reparses_to_the_intended_text() {
    for (shape, code, refused, intended) in PARSE_REPAIRS {
        let source = as_source(refused);
        let diagnostic = parse_query_diagnostic(&source)
            .err()
            .unwrap_or_else(|| panic!("{shape}: accepted\n{source}"));
        assert_eq!(diagnostic.code.as_str(), *code, "{shape}: {diagnostic}");
        let suggestion = diagnostic
            .suggestion
            .as_ref()
            .unwrap_or_else(|| panic!("{shape}: no suggestion: {diagnostic}"));
        let repaired = super::parse::apply_edits(&source, &suggestion.edits);
        let got = parse_source(&repaired)
            .unwrap_or_else(|e| panic!("{shape}: the repair does not parse: {e}\n{repaired}"));
        let want = parse_source(&as_source(intended)).expect("the intended text parses");
        assert_eq!(got, want, "{shape}: repaired to\n{repaired}");
    }
}

mod round_trip;

/// SQL's `union` between two reads is refused naming it, with the
/// `union` relation function as the fix.
#[test]
fn a_sql_union_between_reads_prints_the_relation_function() {
    let d = refusal_of(
        "query q() { match { $p: Person } return { $p.name } union match { $c: Company } return { $c.name } }",
    );
    assert_eq!(d.code.as_str(), "Q014", "{d}");
    assert!(d.message.contains("`union`"), "{d}");
    assert!(
        d.fix.as_deref().is_some_and(|f| f.contains("from union({")),
        "{d:?}"
    );
    assert_eq!(d.position.map(|p| p.column), Some(53));
}
