//! The edition-3 parser tests of docs/rfcs/2026-11-07-gq-3.md, section 10
//! ("Compiler parser tests"), the rule-set pin, and one repair per refusal
//! shape.

use super::parse::parse_source;
use super::*;
use crate::query::ast::{CompOp, EdgeSelector, FileBody, MutationWriteMode};
use crate::query::diagnostic::QueryDiagnostic;
use crate::query::parser::parse_query_diagnostic;

pub(crate) fn parse3(src: &str) -> Result<SourceFile, QueryDiagnostic> {
    parse_source(&format!("gq 3;\n{src}"))
}

#[track_caller]
pub(crate) fn ok3(src: &str) -> SourceFile {
    parse3(src).unwrap_or_else(|e| panic!("edition 3 refused:\n{src}\n{e:?}"))
}

#[track_caller]
fn err3(src: &str) -> QueryDiagnostic {
    match parse3(src) {
        Ok(f) => panic!("edition 3 accepted:\n{src}\n{f:#?}"),
        Err(e) => e,
    }
}

#[track_caller]
fn code3(src: &str) -> &'static str {
    err3(src).code.as_str()
}

#[track_caller]
fn decl(src: &str) -> Decl {
    match ok3(src).body {
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

/// The clauses of `query q() { match { <clauses> } return { 1 as one } }`.
#[track_caller]
fn match_clauses(clauses: &str) -> Vec<Clause> {
    read(&format!(
        "query q() {{ match {{ {clauses} }} return {{ 1 as one }} }}"
    ))
    .clauses
}

#[track_caller]
fn one_clause(text: &str) -> Clause {
    let mut c = match_clauses(text);
    assert_eq!(c.len(), 1, "{text} gave {c:#?}");
    c.remove(0)
}

#[track_caller]
fn filter_expr(text: &str) -> Expr {
    match one_clause(text) {
        Clause::Filter(e) => e,
        other => panic!("{text}: not a filter: {other:?}"),
    }
}

fn int(n: i128) -> Expr {
    Expr::Literal(crate::query::ast::Literal::Integer(n))
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
    }
}

// ---------------------------------------------------------------------------
// The edition line.

#[test]
fn the_edition_line_selects_the_grammar() {
    // No line: edition 2, exactly as before (infix `contains` is legal).
    let e2 = "query q() { match { $d: Doc  $d.t contains \"x\" } return { $d.t } }";
    assert!(parse_query_diagnostic(e2).is_ok());
    // `gq 3;`: the edition-3 front end reads it; the infix form is a tree
    // node the type checker refuses, since the left operand's type picks the
    // fix.
    let f = parse_query_diagnostic(&format!("gq 3;\n{e2}")).unwrap();
    let FileBody::Queries(decls) = f.body else {
        panic!("a declaration file")
    };
    let syntax = decls[0].syntax.as_ref().expect("an edition-3 declaration");
    let DeclBody::Read(body) = &syntax.decl().body else {
        panic!("a read")
    };
    assert!(matches!(
        body.clauses[1],
        Clause::Filter(Expr::InfixString { .. })
    ));
    // `gq 2;` is blanked so every position keeps its column.
    let e = parse_query_diagnostic("gq 2;\nquery q() { match { $d: Doc  $d.t = } return { $d } }")
        .unwrap_err();
    let direct =
        parse_query_diagnostic("     \nquery q() { match { $d: Doc  $d.t = } return { $d } }")
            .unwrap_err();
    assert_eq!(e.position, direct.position);
    // `gq 2;` selects edition 2 for one release.
    assert!(parse_query_diagnostic(&format!("gq 2;\n{e2}")).is_ok());
    // Any other edition is refused at parse.
    let e = parse_query_diagnostic(&format!("gq 4;\n{e2}")).unwrap_err();
    assert_eq!(e.code.as_str(), "Q022");
    // The line stands before the settings lines.
    assert!(
        parse_source("gq 3;\nset ann_nprobes = 4;\nquery q() { match { $p: P } return { $p } }")
            .is_ok()
    );
    assert!(
        parse_source("set ann_nprobes = 4;\ngq 3;\nquery q() { match { $p: P } return { $p } }")
            .is_err()
    );
    // A comment may precede it.
    assert!(
        parse_query_diagnostic("// header\ngq 3;\nquery q() { match { $p: P } return { $p } }")
            .is_ok()
    );
    // `gq` as an identifier elsewhere is unaffected.
    assert!(
        parse_query_diagnostic("query gq() { match { $p: P { gq: 3 } } return { $p.gq } }").is_ok()
    );
}

// ---------------------------------------------------------------------------
// `let` and `from` in every clause position.

const LET: &str = "let $v = $p.age";
const FROM: &str = "from { match { $p knows $q } return { $q } }";

#[test]
fn let_and_from_in_every_clause_position() {
    for text in [LET, FROM] {
        let probe = |c: &Clause| matches!(c, Clause::Let { .. } | Clause::From(_));
        // First, middle and last of a `match`.
        for clauses in [
            format!("{text}  $p: Person"),
            format!("$p: Person  {text}  $p.age > 1"),
            format!("$p: Person  {text}"),
        ] {
            let c = match_clauses(&clauses);
            assert_eq!(c.len(), clauses.matches("  ").count() + 1, "{clauses}");
            assert!(c.iter().any(probe), "{clauses}: {c:#?}");
        }
        // Inside every block.
        for block in ["not", "optional", "exists", "count"] {
            let suffix = if block == "count" { " > 0" } else { "" };
            let c = one_clause(&format!("{block} {{ $p: Person  {text} }}{suffix}"));
            let inner = match c {
                Clause::Not(c) | Clause::Optional(c) => c,
                Clause::Filter(Expr::Block(b)) => b.clauses,
                Clause::Filter(Expr::Compare { left, .. }) => match *left {
                    Expr::Block(b) => b.clauses,
                    other => panic!("{block}: {other:?}"),
                },
                other => panic!("{block}: {other:?}"),
            };
            assert!(inner.iter().any(probe), "{block}: {inner:#?}");
        }
        // As a block value's clause.
        let e = filter_expr(&format!("$x.n < count {{ $p: Person  {text} }}"));
        assert!(matches!(e, Expr::Compare { .. }));
        // Inside an inline read, as `from`'s and `let`'s relation.
        let c = one_clause(&format!(
            "from {{ match {{ $p: Person  {text} }} return {{ $p }} }}"
        ));
        let Clause::From(RelExpr::Inline(body)) = c else {
            panic!()
        };
        assert!(body.clauses.iter().any(probe));
        // In a write body, before a statement.
        let w = write(&format!(
            "query w() {{ $p: Person  {text}  update $p set {{ age: 1 }} }}"
        ));
        assert!(
            w.all_clauses().into_iter().any(probe),
            "{:#?}",
            w.all_clauses()
        );
        // As the only clause of a write body.
        let w = write(&format!("query w() {{ {text}  delete $p }}"));
        assert_eq!(w.all_clauses().len(), 1);
    }
}

#[test]
fn from_takes_an_inline_read_a_variable_and_a_call() {
    assert!(matches!(
        one_clause("from { match { $p: Person } return { $p } }"),
        Clause::From(RelExpr::Inline(_))
    ));
    assert!(matches!(
        one_clause("from $docs"),
        Clause::From(RelExpr::Variable(v)) if v == "docs"
    ));
    let Clause::From(RelExpr::Call(c)) = one_clause("from colleagues($x.handle)") else {
        panic!()
    };
    assert_eq!(c.name, "colleagues");
    let Clause::From(RelExpr::Call(c)) = one_clause(
        "from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $q, k: 20, recall: 0.9)",
    ) else {
        panic!()
    };
    assert_eq!((c.args.len(), c.options.len()), (3, 2));
    assert!(matches!(c.args[0], Arg::Inline(_)));
    // `from` followed by anything else is a parse error at the word (section
    // 7, as adopted on 2026-11-08: a bare filter named `from` is never well
    // typed), and so is a bare name anywhere in a `match`.
    for text in ["from = 1", "from", "from (1) = 1"] {
        let e = err3(&format!(
            "query q() {{ match {{ {text} }} return {{ 1 as one }} }}"
        ));
        assert!(["Q018", "Q019"].contains(&e.code.as_str()), "{text}: {e:?}");
        assert_eq!(e.position.unwrap().column, 21, "{text}: at the word");
    }
}

#[test]
fn let_and_from_are_property_edge_alias_and_assignment_names() {
    for word in ["true", "false"] {
        // The Boolean literals are not bare edge names either.
        assert!(matches!(
            one_clause(&format!("$a \"{word}\" $b")),
            Clause::Traversal(_)
        ));
        let c = match_clauses(&format!("$ok {word} $p: Person"));
        assert_eq!(c.len(), 3, "{word}: {c:#?}");
        // Between two lone variables, refused with the quoted edge.
        let e = err3(&format!(
            "query q() {{ match {{ $a {word} $b }} return {{ 1 as one }} }}"
        ));
        assert_eq!(e.code.as_str(), "Q017", "{word}");
    }
    for word in ["let", "from"] {
        // A property after a dot, in a binding's braces and in return.
        let r = read(&format!(
            "query q() {{ match {{ $p: Person {{ {word}: 1 }}  $p.{word} > 0 }} return {{ $p.{word} as {word} }} order {{ {word} }} }}"
        ));
        assert_eq!(r.projections[0].alias.as_deref(), Some(word));
        // An edge name: quoted, since `$ok  from $r` (a Boolean filter, then
        // a `from` clause) and `$ok from $r` (a triple over `from`) are the
        // same text.
        let Clause::Traversal(TraversalEq(t)) = one_clause(&format!("$a \"{word}\" $b")) else {
            panic!("{word} as an edge")
        };
        assert_eq!(t.selector, EdgeSelector::Named(word.into()));
        // A bound edge of that name, with bounds, undirected, alternated.
        one_clause(&format!("$a $k:\"{word}\" $b"));
        one_clause(&format!("$a \"{word}\"{{1,2}} $b"));
        one_clause(&format!("$a <\"{word}\"> $b"));
        one_clause(&format!("$a (\"{word}\" | knows) $b"));
        // The bare form between two lone variables is refused, with the
        // quoted edge as the fix (section 7, as adopted on 2026-11-08).
        let e = err3(&format!(
            "query q() {{ match {{ $ok {word} $r }} return {{ 1 as one }} }}"
        ));
        assert_eq!(e.code.as_str(), "Q017", "{word}");
        assert_eq!(
            e.fix.as_deref(),
            Some(format!("$ok \"{word}\" $r").as_str())
        );
        // A name that merely starts with the word is a bare edge.
        one_clause(&format!("$a {word}s $b"));
        // A bare mutation property.
        let w = write(&format!(
            "query w() {{ insert Person {{ {word}: 1, handle: \"h\" }} }}"
        ));
        assert!(
            matches!(&w.all_statements()[0], Stmt::Insert { assignments, .. } if assignments[0].property == word)
        );
        // A registry row is never named a clause keyword (section 5), so
        // `from(1)` and `let(1)` are refused, not calls.
        let e = err3(&format!(
            "query q() {{ match {{ $x.a = {word}(1) }} return {{ 1 as one }} }}"
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
        filter_expr("$n - 1 > 0"),
        gt(arith(ArithOp::Sub, var("n"), int(1)))
    );
    // A binary minus has whitespace on both sides or neither (section 3, as
    // adopted on 2026-11-08): `$n -1` and `$n- 1` are refused, and the
    // suggestion applied is `$n - 1`.
    for asym in ["$n -1 > 0", "$n- 1 > 0"] {
        let src = format!("query q() {{ match {{ {asym} }} return {{ 1 as one }} }}");
        let e = err3(&src);
        assert_eq!(e.code.as_str(), "Q016", "{asym}");
        let fixed = super::parse::apply_edits(
            &format!("gq 3;\n{src}"),
            &e.suggestion.as_ref().unwrap().edits,
        );
        assert_eq!(
            parse_source(&fixed).unwrap(),
            parse3("query q() { match { $n - 1 > 0 } return { 1 as one } }").unwrap(),
            "{asym}"
        );
    }
    assert_eq!(filter_expr("$n-1 > 0"), filter_expr("$n - 1 > 0"));
    assert_eq!(
        filter_expr("$n - $m > 0"),
        gt(arith(ArithOp::Sub, var("n"), var("m")))
    );
    // Without spaces too.
    assert_eq!(filter_expr("$n-$m > 0"), filter_expr("$n - $m > 0"));
    // `$a -knows- $b` is the arrow, refused with the triple as the fix.
    let e = err3("query q() { match { $a -knows- $b } return { $a } }");
    assert_eq!(e.code.as_str(), "Q007");
    assert_eq!(e.fix.as_deref(), Some("$a <knows> $b"));
    let e = err3("query q() { match { $a -knows-> $b } return { $a } }");
    assert_eq!(e.fix.as_deref(), Some("$a knows $b"));
    let e = err3("query q() { match { $a <-knows- $b } return { $a } }");
    assert_eq!(e.fix.as_deref(), Some("$b knows $a"));
    let e = err3("query q() { match { $a -$k:knows{1,2}-> $b } return { $a } }");
    assert_eq!(e.fix.as_deref(), Some("$a $k:knows{1,2} $b"));
    let e = err3("query q() { match { $a -\"in\"-> $b } return { $a } }");
    assert_eq!(e.fix.as_deref(), Some("$a \"in\" $b"));
    let e = err3("query q() { match { $a -(knows | likes)-> $b } return { $a } }");
    assert_eq!(e.fix.as_deref(), Some("$a (knows | likes) $b"));
}

#[test]
fn the_ladder_has_the_usual_precedence_and_associativity() {
    // `*` binds tighter than `+`; both are left-associative; unary minus
    // binds tightest; comparison is below arithmetic and above `not`.
    assert_eq!(
        filter_expr("$a + $b * $c > 0"),
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
        filter_expr("$a - $b - $c > 0"),
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
        filter_expr("$a / $b % $c > 0"),
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
        filter_expr("0 < -$a * $b"),
        cmp(
            CompOp::Lt,
            int(0),
            arith(ArithOp::Mul, Expr::Neg(Box::new(var("a"))), var("b"))
        )
    );
    // The leading-minus rule is gone (2026-11-08): a filter may begin with
    // `-`, because a `-` after whitespace and before a word is never binary.
    assert_eq!(
        filter_expr("-$a > 0"),
        cmp(CompOp::Gt, Expr::Neg(Box::new(var("a"))), int(0))
    );
    assert_eq!(
        filter_expr("(-$a) > 0"),
        cmp(CompOp::Gt, Expr::Neg(Box::new(var("a"))), int(0))
    );
    // Clauses have no separator: after an expression clause, `X  -Y` would
    // continue it, so it is refused (Q016) instead of silently merging.
    assert_eq!(
        err3("query q() { match { $a.x\n  -$b.y > 0 } return { 1 } }")
            .code
            .as_str(),
        "Q016"
    );
    assert_eq!(
        filter_expr("$a * ($b + $c) > --1"),
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
        filter_expr("not $a + 1 > 2 and $b"),
        Expr::And(l, _) if matches!(*l, Expr::Not(_))
    ));
    // Comparison stays non-associative.
    assert!(parse3("query q() { match { $a < $b < $c } return { $a } }").is_err());
    // Arithmetic in `return`, `order`, `let`, a binding value and an
    // assignment.
    let r = read(
        "query doubled($min: I32) { match { $p: Person { age: $min + 1 }  $p.age * 2 >= $min  let $born = 2026 - $p.age } return { $p.name, $p.age * 2 as twice, (2026 - $p.age) as born_about } order { -$p.age, $born % 10 desc } }",
    );
    assert_eq!(r.projections.len(), 3);
    let w = write(
        "query birthday($h: String) { $p: Person { handle: $h }  update $p set { age: $p.age + 1 } }",
    );
    assert!(
        matches!(&w.all_statements()[0], Stmt::Update { assignments, .. } if matches!(assignments[0].value, Expr::Arith { .. }))
    );
}

#[test]
fn float_literals_take_an_exponent() {
    use crate::query::ast::Literal;
    for (text, value) in [
        ("1.5e3", 1.5e3),
        ("2.0E-2", 2.0e-2),
        ("6.02e+23", 6.02e23),
        ("0.5", 0.5),
    ] {
        assert_eq!(
            filter_expr(&format!("$x.f > {text}")),
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
        filter_expr("$x.f > -1.0e-3"),
        Expr::Compare { right, .. } if matches!(*right, Expr::Neg(_))
    ));
    // `1e3` without a fraction is not a float. The RFC's `float_lit` admits
    // only `d.d[e…]`; with edition 2's unbounded `integer`, `1e3` would read
    // as `1` followed by the bare name `e3`. The word boundary on
    // `integer3` makes it a parse error instead.
    assert!(parse3("query q() { match { $x: X } return { 1e3 } }").is_err());
    assert!(parse3("query q() { match { $x: X } return { 1.5e } }").is_err());
}

// ---------------------------------------------------------------------------
// The reserved set.

/// Every word the edition-3 grammar spells as a keyword, other than the six
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
        // The statement words are quoted edges too.
        let edge = if matches!(
            *word,
            "let" | "from" | "true" | "false" | "insert" | "update" | "delete"
        ) {
            format!("\"{word}\"")
        } else {
            word.to_string()
        };
        let src = format!(
            "query q() {{ match {{ $p: Person  $p.{word} = 1  $p {edge} $q }} return {{ $p.{word} as {word} }} }}"
        );
        let r = read(&src);
        assert!(
            matches!(&r.clauses[2], Clause::Traversal(_)),
            "`{word}` as an edge: {:#?}",
            r.clauses
        );
        assert_eq!(r.projections[0].alias.as_deref(), Some(*word), "{word}");
        let w = write(&format!("query w() {{ insert Person {{ {word}: 1 }} }}"));
        assert_eq!(w.all_statements().len(), 1, "{word}");
        let w = write(&format!(
            "query w() {{ $p: Person  update $p set {{ {word}: 2 }} }}"
        ));
        assert_eq!(w.all_statements().len(), 1, "{word}");
        // A binding's match key.
        read(&format!(
            "query q() {{ match {{ $p: Person {{ {word}: 1 }} }} return {{ $p }} }}"
        ));
    }
    for word in RESERVED {
        // Refused as an alias and as a bare edge; a property after a dot is
        // fine.
        assert!(
            parse3(&format!(
                "query q() {{ match {{ $p: P }} return {{ $p.a as {word} }} }}"
            ))
            .is_err(),
            "{word} as an alias"
        );
        read(&format!(
            "query q() {{ match {{ $p: P  $p.{word} = 1 }} return {{ $p.{word} }} }}"
        ));
        let quoted = one_clause(&format!("$a \"{word}\" $b"));
        assert!(matches!(quoted, Clause::Traversal(_)), "{word} quoted");
        let bare = parse3(&format!(
            "query q() {{ match {{ $a {word} $b }} return {{ $a }} }}"
        ));
        match *word {
            // `$a in $b` and `$a is …` are filters, not traversals.
            "in" => assert!(matches!(bare.as_ref().map(|_| ()), Ok(()))),
            _ => assert!(
                bare.is_err()
                    || !matches!(
                        bare.as_ref().unwrap().body,
                        Body::Decls(ref d) if matches!(&d[0].body, DeclBody::Read(r) if matches!(r.clauses[0], Clause::Traversal(_)))
                    ),
                "{word} bare is a traversal"
            ),
        }
    }
}

#[test]
fn an_edge_named_in_is_quoted_and_the_bare_form_is_a_membership_filter() {
    let Clause::Traversal(TraversalEq(t)) = one_clause("$d \"in\" $x") else {
        panic!()
    };
    assert_eq!(t.selector, EdgeSelector::Named("in".into()));
    // `$d in $x` is a legal membership filter (the fix for `unwind` is
    // `$x in $list`), so the parser cannot refuse the bare edge: the type
    // checker, seeing a node on the right of `in`, prints the quoted name
    // (`T7`).
    assert_eq!(
        filter_expr("$d in $x"),
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
fn delete_and_update_with_where_print_the_binding_and_filter() {
    let e = err3("query retire($h: String) { delete WorksAt where @src = $h }");
    assert_eq!(e.code.as_str(), "Q008");
    assert_eq!(
        e.fix.as_deref(),
        Some("$x: WorksAt  $x.@src = $h  delete $x")
    );
    let e = err3(
        "query retire($h: String) { update Person set { city: \"x\" } where handle = $h and age > 3 }",
    );
    assert_eq!(e.code.as_str(), "Q008");
    assert_eq!(
        e.fix.as_deref(),
        Some("$x: Person  $x.handle = $h and $x.age > 3  update $x set { city: \"x\" }")
    );
    // `where_stmt` is tried first and takes a variable too, so
    // `delete $x where φ` prints the filter clause instead of reading `where`
    // as a bare name after the statement.
    let e = err3("query q() { $x: Person  delete $x where $x.age > 3 }");
    assert_eq!(e.code.as_str(), "Q008");
    assert_eq!(e.fix.as_deref(), Some("$x.age > 3  delete $x"));
    let e = err3("query q() { $x: Person  update $x set { a: 1 } where $x.age > 3 }");
    assert_eq!(e.fix.as_deref(), Some("$x.age > 3  update $x set { a: 1 }"));
}

#[test]
fn the_mutation_keyword_is_refused_with_query_as_the_fix() {
    let e = err3("mutation hire($h: String) { insert Person { handle: $h } }");
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
    read("query mutation() { match { $p: P { mutation: 1 } } return { $p.mutation as mutation } }");
}

#[test]
fn a_comparison_in_binding_braces_prints_the_filter_clause() {
    let e = err3("query q() { match { $d: Doc { year >= 2025 } } return { $d } }");
    assert_eq!(e.code.as_str(), "Q010");
    assert_eq!(e.fix.as_deref(), Some("$d: Doc  $d.year >= 2025"));
    // A legal match list is unaffected, with or without a trailing comma.
    read("query q() { match { $d: Doc { year: 2025, title: \"x\", } } return { $d } }");
}

#[test]
fn as_commit_is_a_write_body_clause() {
    let d = decl("query retire($h: String) as commit { $p: Person { handle: $h }  delete $p }");
    assert_eq!(d.write_mode, MutationWriteMode::AsCommit);
    let d = decl(
        "query propose($h: String) as unattached branch on $h { insert Person { handle: $h, name: $h } }",
    );
    assert!(matches!(d.write_mode, MutationWriteMode::Unattached { .. }));
    let e = err3("query r() as commit { match { $p: P } return { $p } }");
    assert_eq!(e.code.as_str(), "Q011");
    let e = err3("query r() as unattached branch { match { $p: P } return { $p } }");
    assert_eq!(e.code.as_str(), "Q011");
}

#[test]
fn a_clause_after_a_statement_prints_the_order() {
    let e = err3("query w() { $p: Person  delete $p  $q: Person }");
    assert_eq!(e.code.as_str(), "Q013");
    // Several statements after the clauses are fine.
    let w = write(
        "query close_company($slug: String) { $c: Company { slug: $slug }  $p $k:worksAt $c  delete $k  update $p set { city: null } }",
    );
    assert_eq!((w.all_clauses().len(), w.all_statements().len()), (2, 2));
    // `null` is an assignment value; in operand position it stays a
    // reserved word.
    assert!(
        matches!(&w.all_statements()[1], Stmt::Update { assignments, .. }
        if assignments[0].value == Expr::Literal(crate::query::ast::Literal::Null))
    );
    write("query w() { insert P { a: null, b: 1 } }");
    write("query w() { insert P { a: null  b: 1 } }");
    assert!(parse3("query w() { insert P { a: null + 1 } }").is_err());
    assert!(parse3("query q() { match { $p: P  $p.a = null } return { $p } }").is_err());
}

#[test]
fn other_respellings_print_their_fix() {
    for (src, fix) in [
        ("$a || $b = \"x\"", "concat($a, $b)"),
        (
            "case when $d.year > 1 then 1 else 2 end = 1",
            "if($d.year > 1, 1, 2)",
        ),
        ("for $x in $list", "$x in $list"),
        ("unwind $list as $x", "$x in $list"),
        ("let $x in $r", "from $r"),
        ("let ($a, $b) in $r", "from $r"),
    ] {
        let e = err3(&format!(
            "query q() {{ match {{ $d: Doc  {src} }} return {{ $d }} }}"
        ));
        assert_eq!(e.code.as_str(), "Q012", "{src}");
        assert_eq!(e.fix.as_deref(), Some(fix), "{src}");
    }
    // Infix `contains` and `starts_with` parse to a node the type checker
    // refuses (`T7`), since the left operand's type picks the fix.
    for src in ["$d.title starts_with \"O\"", "$d.tags contains \"rust\""] {
        assert!(matches!(filter_expr(src), Expr::InfixString { .. }));
    }
    // Every annotation parses; the type checker decides which names it reads
    // (`T80`), so a later typed annotation is additive.
    parse3("query q() @if_commit($c) { $p: P  delete $p }").unwrap();
    parse3("query q() @valid(from = 1) { match { $p: P } return { $p } }").unwrap();
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
    // A write whose clauses come from a ranked read.
    let w = write(
        "query tag_near($q: Vector(3)) { from nearest({ match { $d: Doc } return { $d } }, $d.embedding, $q, k: 20)  update $d set { tags: [\"near\"] } }",
    );
    assert_eq!((w.all_clauses().len(), w.all_statements().len()), (1, 1));
    // A write body's clauses are bare; a body opening with `match { }` is
    // refused with the wrapper removed (section 5).
    let e = err3("query w() { match { $p: Person } delete $p }");
    assert_eq!(e.code.as_str(), "Q005");
    assert_eq!(e.fix.as_deref(), Some("$p: Person"));
    // A statement keyword stays a property name, after a dot and as a key.
    write("query w() { $p: P { insert: 1 }  $p.update = 2  delete $p }");
    // Parameters with defaults, `limit $k`, generic annotations.
    let d = decl(
        "query top_people($k: I64 = 10, $neg: I32 = -1, $s: String? = \"x\") @description(\"d\") @budget(plan = 1000) @example(\"e\") { match { $p: Person } return { $p.name } order { $p.age desc } limit $k }",
    );
    assert!(d.params[1].default.as_ref().unwrap().negative);
    assert_eq!(d.annotations.len(), 3);
    let DeclBody::Read(r) = d.body else { panic!() };
    assert_eq!(r.limit, Some(Limit::Param("k".into())));
    // Several declarations in one file.
    let f = ok3("query a() { match { $p: P } return { $p } }\nquery b() { insert P { a: 1 } }");
    assert!(matches!(f.body, Body::Decls(ref d) if d.len() == 2));
    // `explain` over an edition-3 declaration.
    assert!(matches!(
        ok3("explain query a() { match { from $r } return { $p } }").body,
        Body::Explain(_)
    ));
    // The missing parameter list keeps its fix.
    let e = err3("query a { match { $p: P } return { $p } }");
    assert_eq!(
        (e.code.as_str(), e.fix.as_deref()),
        ("Q002", Some("query a()"))
    );
}

#[test]
fn calls_take_positionals_then_options_and_one_distinct_form() {
    let Expr::Call(c) = filter_expr("fuzzy($x.title, $q, max_edits: 2)") else {
        panic!()
    };
    assert_eq!((c.args.len(), c.options.len()), (2, 1));
    // An option before a positional is refused.
    assert!(parse3("query q() { match { f(k: 1, $x) } return { 1 } }").is_err());
    // Options only.
    filter_expr("f(k: 1, mode: any)");
    // `distinct` under any aggregate name (the type checker keeps T57).
    let r = read(
        "query q() { match { $p: P } return { count(distinct $p.city) as c, sum(distinct $p.age) as s } }",
    );
    assert!(matches!(r.projections[1].expr, Expr::AggDistinct { .. }));
    // `date("…")` is a literal everywhere, including inside a list.
    assert!(matches!(
        filter_expr("$x.d in [date(\"2025-01-01\"), date(\"2025-01-02\")]"),
        Expr::In { .. }
    ));
    // Nested relation functions.
    one_clause(
        "from rrf(nearest($docs, $d.embedding, $v, k: 50), bm25($docs, $d.body, $q), k: 60, weights: [0.7, 0.3])",
    );
    // A block value as an operand, compared with another binding's value.
    let e = filter_expr("count { $d \"in\" $x } + 1 >= $x.min_docs");
    assert!(matches!(e, Expr::Compare { left, .. } if matches!(*left, Expr::Arith { .. })));
}

#[test]
fn a_block_in_clause_position_is_a_filter() {
    // One tree for one text: `count { p } > 1 or $x` is the disjunction, as
    // the same text is anywhere else an expression stands; edition 2's
    // `subquery_predicate` would read `count { p } > (1 or $x)`.
    assert!(matches!(
        one_clause("count { $a: P } > 1 or $x"),
        Clause::Filter(Expr::Or(..))
    ));
    assert!(matches!(
        one_clause("exists { $a: P }"),
        Clause::Filter(Expr::Block(_))
    ));
}

#[test]
fn a_product_of_two_variables_is_not_the_wildcard_triple() {
    // `$a * $b` is the wildcard triple only when no operator follows
    // (section 7); followed by one it is a product.
    assert_eq!(
        filter_expr("$n * $m > 10"),
        cmp(CompOp::Gt, arith(ArithOp::Mul, var("n"), var("m")), int(10))
    );
    assert!(matches!(filter_expr("$n * $m or $ok"), Expr::Or(..)));
    // The triple stays a triple, alone, bounded, bound or followed by a
    // clause.
    assert!(matches!(one_clause("$a * $b"), Clause::Traversal(_)));
    assert!(matches!(one_clause("$a *{1,3} $b"), Clause::Traversal(_)));
    assert!(matches!(one_clause("$a $k:* $b"), Clause::Traversal(_)));
    let c = match_clauses("$a * $b  $b: Person  (-$x) > 0");
    assert!(matches!(c[0], Clause::Traversal(_)), "{c:#?}");
    assert_eq!(c.len(), 3);
}

#[test]
fn a_bare_from_or_let_filter_before_a_variable_is_the_clause() {
    // The contextual keyword wins over a bare-name filter at clause start;
    // a bare name is never a value in a read.
    assert!(parse3("query q() { match { from  $a: Person } return { 1 } }").is_err());
    assert!(matches!(one_clause("let  $a = 1"), Clause::Let { .. }));
}

#[test]
fn a_call_to_contains_after_a_boolean_filter_is_a_call() {
    // The infix recognizer is word-bounded and does not touch `(`, so the
    // second clause is a call, not `$p.active contains (…)`.
    let c = match_clauses("$p.active  contains($p.name, \"a\")  starts_with($p.name, \"b\")");
    assert_eq!(c.len(), 3, "{c:#?}");
    // The infix form with a space is still recognised, as the tree node the
    // type checker refuses.
    assert!(matches!(
        filter_expr("$p.name contains \"a\""),
        Expr::InfixString { .. }
    ));
    // A triple endpoint is never followed by `.`.
    assert!(matches!(filter_expr("$a * $a.n = 0"), Expr::Compare { .. }));
}

#[test]
fn a_date_call_is_the_date_literal() {
    // `date("…")` is the literal production, never a call to a function
    // named `date`.
    assert_eq!(
        filter_expr("$x.d = date(\"2025-01-01\")"),
        cmp(
            CompOp::Eq,
            Expr::Prop {
                variable: "x".into(),
                property: "d".into()
            },
            Expr::Literal(crate::query::ast::Literal::Date("2025-01-01".into()))
        )
    );
    // With another argument shape it is a call.
    assert!(matches!(
        filter_expr("$x.d = date($s)"),
        Expr::Compare { right, .. } if matches!(*right, Expr::Call(_))
    ));
}

#[test]
fn recognizers_shadow_only_ill_typed_text() {
    // The arrow recognizer takes `$a - x - $b`, `x` a bare name, a
    // string or a bare meta-field: legal grammar (a bare name is an operand),
    // never well-typed arithmetic. The RFC's claim holds at the type level.
    // Followed by a comparison, the recognized arrow leaves `> 0` with no
    // clause to start, so the refusal is the generic Q001 at `>`.
    for middle in ["x", "\"s\"", "@id"] {
        let src = format!("query q() {{ match {{ $a - {middle} - $b }} return {{ 1 }} }}");
        assert_eq!(code3(&src), "Q007", "{middle}");
        let src = format!("query q() {{ match {{ $a - {middle} - $b > 0 }} return {{ 1 }} }}");
        assert_eq!(code3(&src), "Q001", "{middle}");
    }
    // A number, a property or a parenthesised term in the middle is
    // arithmetic.
    for middle in ["1", "$x.n", "($x)"] {
        filter_expr(&format!("$a - {middle} - $b > 0"));
    }
    // `let ($a) in …` is the refused respelling, so a call to a function
    // named `let` with one variable, compared with `in`, is refused too.
    assert_eq!(
        code3("query q() { match { let($a) in [1] } return { 1 } }"),
        "Q012"
    );
}

#[test]
fn reserved_words_between_two_variables() {
    // Of the six reserved words, only `is` and `null`
    // between two variables are parse errors. `and`, `or`, `in` and `not`
    // there are legal expressions (`$a not $b` is two filters), so a bare
    // edge of that name can be refused only by the type checker.
    for word in ["and", "or", "in"] {
        let c = match_clauses(&format!("$a: Person  $a {word} $b"));
        assert!(matches!(c[1], Clause::Filter(_)), "{word}: {c:#?}");
    }
    assert_eq!(match_clauses("$a: Person  $a not $b").len(), 3);
    for word in ["is", "null"] {
        assert!(
            parse3(&format!(
                "query q() {{ match {{ $a: Person  $a {word} $b }} return {{ 1 }} }}"
            ))
            .is_err(),
            "{word}"
        );
    }
}

#[test]
fn a_date_literal_touches_its_parenthesis() {
    // `date("…")` is the literal, and like a call its name touches its `(`:
    // with a space it is the refused call spelling.
    assert_eq!(
        code3("query q() { match { $d: Doc  $d.at > date (\"2025-01-01\") } return { $d } }"),
        "Q019"
    );
    assert!(matches!(
        filter_expr("$d.at > date(\"2025-01-01\")"),
        Expr::Compare { right, .. } if matches!(*right, Expr::Literal(crate::query::ast::Literal::Date(_)))
    ));
}

// ---------------------------------------------------------------------------
// The rule-set pin (section 11, rule 1).

/// Every rule the edition-3 section of `grammar.pest` defines, sorted.
const SYNTAX_RULES: &[&str] = &[
    "agg_distinct",
    "agg_name",
    "alias3",
    "alias_string_rec",
    "and_expr3",
    "annotation3",
    "annotation_arg3",
    "annotation_colon_rec",
    "annotation_kwarg",
    "annotation_name",
    "annotation_value",
    "arrow_close",
    "arrow_open",
    "arrow_traversal",
    "assignment3",
    "bare_name_rec",
    "between_rec",
    "binding3",
    "binding_brace_filter",
    "binding_lower_rec",
    "binding_paren_head",
    "binding_paren_rec",
    "block_value",
    "blockless_rec",
    "brace_body",
    "brace_colon_compare",
    "brace_compare",
    "brace_item",
    "brace_items_rec",
    "call",
    "call_args",
    "call_block_rec",
    "call_head",
    "call_name",
    "call_option",
    "case_rec",
    "catalogue_edge",
    "chained_rec",
    "clause3",
    "comparison3",
    "concat_bars",
    "continuation",
    "count_star_rec",
    "create_rec",
    "cypher_hops",
    "cypher_name",
    "cypher_node",
    "cypher_pattern",
    "cypher_prop",
    "cypher_rel",
    "cypher_rel_body",
    "cypher_typed_node",
    "date_head3",
    "date_lit3",
    "datetime_head3",
    "datetime_lit3",
    "declaration3",
    "delete_multi_rec",
    "delete_stmt3",
    "delete_type_rec",
    "detach_rec",
    "dir_first_rec",
    "distinct_item_rec",
    "edge_alternation3",
    "edge_alternatives3",
    "edge_ident3",
    "edge_name3",
    "edge_sel3",
    "edition_line",
    "eq_assign_rec",
    "exp_float_rec",
    "explain_stmt3",
    "expr3",
    "filter3",
    "float_lit3",
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
    "fw_where",
    "fw_with",
    "group_by_rec",
    "having_rec",
    "in_paren_rec",
    "infix_string_op",
    "inline_read",
    "insert_paren_rec",
    "insert_stmt3",
    "integer3",
    "keyword_call_head",
    "keyword_call_rec",
    "kw_exists",
    "kw_for",
    "kw_gq",
    "kw_insert",
    "kw_let",
    "kw_match",
    "kw_mutation",
    "kw_optional",
    "kw_order",
    "kw_query",
    "kw_return",
    "kw_unwind",
    "kw_update",
    "kw_view",
    "kw_where",
    "let_clause",
    "like_rec",
    "limit_clause3",
    "limit_neg_rec",
    "list_lit3",
    "list_to_rows",
    "literal3",
    "match_block_rec",
    "match_clause3",
    "match_nobrace_rec",
    "match_prefix_rec",
    "matchless_read_rec",
    "meta_assign_rec",
    "missing_param_list3",
    "mixed_args_rec",
    "mutation_body3",
    "mutation_decl",
    "mutation_stmt3",
    "neg_sign",
    "negation3",
    "not_expr3",
    "not_in_rec",
    "null_compare_rec",
    "null_value",
    "offset_rec",
    "operand3",
    "optional_block",
    "optional_rec",
    "or_expr3",
    "order_by_rec",
    "order_clause3",
    "order_dir3",
    "ordering3",
    "param3",
    "param_default",
    "param_item3",
    "param_list3",
    "param_untyped_rec",
    "positional",
    "prefix_rec",
    "product3",
    "product_op",
    "projection3",
    "prop_access3",
    "prop_chain_rec",
    "prop_match3",
    "prop_match_list3",
    "prop_method_head",
    "prop_method_rec",
    "prop_star_rec",
    "qualified_assign_rec",
    "query_decl3",
    "query_file3",
    "read_body3",
    "read_clause3",
    "read_tail3",
    "regex_rec",
    "relation_expr",
    "retrieve_rec",
    "return_clause3",
    "return_nobrace_rec",
    "set_item",
    "set_rec",
    "spaced_call_head",
    "spaced_call_rec",
    "sql_alias_rec",
    "star_item_rec",
    "starts_with_words_rec",
    "statement_trailer3",
    "stmt_edge_rec",
    "stmt_keyword",
    "stmt_var_rec",
    "stmt_word",
    "stray_braces_rec",
    "subquery_block3",
    "sum3",
    "sum_op3",
    "swapped_read_rec",
    "trailing_clauses_rec",
    "traversal3",
    "type_prop_rec",
    "unary3",
    "unary_minus",
    "undirected_edge3",
    "update_noset_rec",
    "update_stmt3",
    "view_decl",
    "where_stmt",
    "with_item",
    "with_rec",
    "write_clause3",
    "write_group",
];

/// No new grammar rule without an amendment (section 11, rule 1): a rule
/// added to or removed from the edition-3 grammar turns this red, and the
/// fix is an amendment to the GQ 3 RFC that names the production, the token
/// edition 3 refused where it starts, and the collision it designs around.
/// Recognizers (`_rec`, and the rules only they use) print a fix and accept
/// nothing section 7 refuses, but they are pinned too, so a recognizer that
/// starts accepting text is seen.
#[test]
fn the_syntax_rule_set_is_pinned() {
    let grammar = include_str!("grammar.pest");
    let section = &grammar[grammar
        .find("// GQ edition 3")
        .expect("the edition-3 banner")..];
    let mut rules: Vec<&str> = section
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
        "the edition-3 rule set moved; a grammar change after the break is an amendment to the GQ 3 RFC (section 11, rule 1)"
    );
}

// ---------------------------------------------------------------------------
// Repairs: one row per distinct refusal shape agents' texts reached, each the
// refused text, its code, and the text its suggestion must reparse to.

/// `(shape, code, refused text, intended text)`. A text that opens with its
/// own edition or settings line is taken as written; every other one is
/// edition 3.
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
  match { $d: Doc  $d.year * 1.0 > 2e3 }
  return { $d.title }
}"#,
        r#"query q() { match { $d: Doc $d.year * 1.0 > 2000.0 } return { $d.title } }"#,
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
        r#"a write body wrapped in match"#,
        "Q005",
        r#"query w($h: String) {
  match { $p: Person { handle: $h } }
  delete $p
}"#,
        r#"query w($h: String) { $p: Person { handle: $h } delete $p }"#,
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
  $p: Person { handle: $h }
  update $p set { age = 3 }
}"#,
        r#"query w($h: String) { $p: Person { handle: $h } update $p set { age: 3 } }"#,
    ),
    (
        r#"a qualified assignment target"#,
        "Q005",
        r#"query w($h: String) {
  $p: Person { handle: $h }
  update $p set { $p.age: 3 }
}"#,
        r#"query w($h: String) { $p: Person { handle: $h } update $p set { age: 3 } }"#,
    ),
    (
        r#"update without set"#,
        "Q005",
        r#"query w($h: String) {
  $p: Person { handle: $h }
  update $p { age: 3 }
}"#,
        r#"query w($h: String) { $p: Person { handle: $h } update $p set { age: 3 } }"#,
    ),
    (
        r#"an untyped parameter"#,
        "Q005",
        r#"query q($h) {
  match { $p: Person { handle: $h } }
  return { $p.name }
}"#,
        r#"query q($h: String) { match { $p: Person { handle: $h } } return { $p.name } }"#,
    ),
    (
        r#"an edition other than 3"#,
        "Q022",
        r#"gq 4;
query q() {
  match { $p: Person }
  return { $p.name }
}"#,
        r#"gq 3; query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"an edition line without its semicolon"#,
        "Q022",
        r#"gq 3
query q() {
  match { $p: Person }
  return { $p.name }
}"#,
        r#"gq 3; query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"an uppercase edition line"#,
        "Q022",
        r#"GQ 3;
query q() {
  match { $p: Person }
  return { $p.name }
}"#,
        r#"gq 3; query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"a settings line before the edition line"#,
        "Q022",
        r#"set ann_nprobes = 20;
gq 3;
query q() {
  match { $p: Person }
  return { $p.name }
}"#,
        r#"gq 3; set ann_nprobes = 20; query q() { match { $p: Person } return { $p.name } }"#,
    ),
    (
        r#"an outgoing arrow"#,
        "Q007",
        r#"query q() {
  match { $a: Person  $a -knows-> $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person $a knows $b } return { $b.name } }"#,
    ),
    (
        r#"an incoming arrow"#,
        "Q007",
        r#"query q() {
  match { $a: Person  $a <-knows- $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person $b knows $a } return { $b.name } }"#,
    ),
    (
        r#"an undirected arrow"#,
        "Q007",
        r#"query q() {
  match { $a: Person  $a -knows- $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person $a <knows> $b } return { $b.name } }"#,
    ),
    (
        r#"an arrow with an edge binding and bounds"#,
        "Q007",
        r#"query q() {
  match { $a: Person  $a -$k:knows{1,2}-> $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person $a $k:knows{1,2} $b } return { $b.name } }"#,
    ),
    (
        r#"Cypher's bracketed edge"#,
        "Q007",
        r#"query q() {
  match { $a: Person  $a -[:knows]-> $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person $a knows $b } return { $b.name } }"#,
    ),
    (
        r#"delete T where"#,
        "Q008",
        r#"query w($h: String) {
  delete Person where handle = $h
}"#,
        r#"query w($h: String) { $x: Person $x.handle = $h delete $x }"#,
    ),
    (
        r#"update T set where"#,
        "Q008",
        r#"query w($h: String) {
  update Person set { age: 3 } where handle = $h
}"#,
        r#"query w($h: String) { $x: Person $x.handle = $h update $x set { age: 3 } }"#,
    ),
    (
        r#"delete $x where"#,
        "Q008",
        r#"query w() {
  $p: Person
  delete $p where $p.age > 90
}"#,
        r#"query w() { $p: Person $p.age > 90 delete $p }"#,
    ),
    (
        r#"delete over an edge type where"#,
        "Q008",
        r#"query w($h: String) {
  delete WorksAt where @src = $h
}"#,
        r#"query w($h: String) { $x: WorksAt $x.@src = $h delete $x }"#,
    ),
    (
        r#"a where fix that keeps a parameter"#,
        "Q008",
        r#"query w($x: String) {
  delete Person where handle = $x
}"#,
        r#"query w($x: String) { $p: Person $p.handle = $x delete $p }"#,
    ),
    (
        r#"a where fix that qualifies an assignment"#,
        "Q008",
        r#"query w() {
  update Person set { name: handle } where age > 3
}"#,
        r#"query w() { $x: Person $x.age > 3 update $x set { name: $x.handle } }"#,
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
        r#"a comparison inside binding braces"#,
        "Q010",
        r#"query q() {
  match { $p: Person { age > 30 } }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.age > 30 } return { $p.name } }"#,
    ),
    (
        r#"a colon comparison inside binding braces"#,
        "Q010",
        r#"query q() {
  match { $p: Person { age: > 30 } }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.age > 30 } return { $p.name } }"#,
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
        r#"unwind"#,
        "Q012",
        r#"query q($list: [String]) {
  match { unwind $list as $x  $p: Person { city: $x } }
  return { $p.name }
}"#,
        r#"query q($list: [String]) { match { $x in $list $p: Person { city: $x } } return { $p.name } }"#,
    ),
    (
        r#"for over a list"#,
        "Q012",
        r#"query q($list: [String]) {
  match { for $x in $list  $p: Person { city: $x } }
  return { $p.name }
}"#,
        r#"query q($list: [String]) { match { $x in $list $p: Person { city: $x } } return { $p.name } }"#,
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
  match { $p: Person  18 <= $p.age < 65 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person 18 <= $p.age and $p.age < 65 } return { $p.name } }"#,
    ),
    (
        r#"a clause after the last statement"#,
        "Q013",
        r#"query w($h: String) {
  delete $p
  $p: Person { handle: $h }
}"#,
        r#"query w($h: String) { $p: Person { handle: $h } delete $p }"#,
    ),
    (
        r#"a Cypher pattern in match"#,
        "Q014",
        r#"query q() {
  match { (a:Person)-[:KNOWS]->(b) }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person $a knows $b } return { $b.name } }"#,
    ),
    (
        r#"an uppercase WHERE"#,
        "Q014",
        r#"query q() {
  match { $p: Person WHERE $p.age > 30 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.age > 30 } return { $p.name } }"#,
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
  match { $p worksAt $c  with $c, count($p) as n  $n > 3 }
  return { $c.name }
}"#,
        r#"query q() { match { from { match { $p worksAt $c } return { $c, count($p) as n } } $n > 3 } return { $c.name } }"#,
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
  match { $p: Person  $p.name STARTS WITH "A" }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person starts_with($p.name, "A") } return { $p.name } }"#,
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
  $p: Person { handle: $h }
  set $p.age = 3
}"#,
        r#"query w($h: String) { $p: Person { handle: $h } update $p set { age: 3 } }"#,
    ),
    (
        r#"DETACH DELETE"#,
        "Q014",
        r#"query w($h: String) {
  $p: Person { handle: $h }
  detach delete $p
}"#,
        r#"query w($h: String) { $p: Person { handle: $h } delete $p }"#,
    ),
    (
        r#"OPTIONAL MATCH"#,
        "Q014",
        r#"query q() {
  match { $p: Person  optional match { $p worksAt $c } }
  return { $p.name, $c.name }
}"#,
        r#"query q() { match { $p: Person optional { $p worksAt $c } } return { $p.name, $c.name } }"#,
    ),
    (
        r#"CALL { }"#,
        "Q014",
        r#"query q() {
  match { $p: Person  call { match { $p knows $q } return { $q } } }
  return { $q.name }
}"#,
        r#"query q() { match { $p: Person from { match { $p knows $q } return { $q } } } return { $q.name } }"#,
    ),
    (
        r#"!= null"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.city != null }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.city is not null } return { $p.name } }"#,
    ),
    (
        r#"<>"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.city <> "Berlin" }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.city != "Berlin" } return { $p.name } }"#,
    ),
    (
        r#"=="#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.city == "Berlin" }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.city = "Berlin" } return { $p.name } }"#,
    ),
    (
        r#"single quotes"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.city = 'Berlin' }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.city = "Berlin" } return { $p.name } }"#,
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
  match { $d: Doc  $d.title LIKE '%graph%' }
  return { $d.title }
}"#,
        r#"query q() { match { $d: Doc contains($d.title, "graph") } return { $d.title } }"#,
    ),
    (
        r#"BETWEEN"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.age BETWEEN 18 AND 65 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.age >= 18 and $p.age <= 65 } return { $p.name } }"#,
    ),
    (
        r#"in over a parenthesised list"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.age in (30, 40) }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.age in [30, 40] } return { $p.name } }"#,
    ),
    (
        r#"not in"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.city not in ["Berlin"] }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person not $p.city in ["Berlin"] } return { $p.name } }"#,
    ),
    (
        r#"&&"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.age >= 18 && $p.age < 65 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.age >= 18 and $p.age < 65 } return { $p.name } }"#,
    ),
    (
        r#"! for not"#,
        "Q014",
        r#"query q() {
  match { $p: Person  !($p.age > 3) }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person not $p.age > 3 } return { $p.name } }"#,
    ),
    (
        r#"a Cypher binding in parentheses"#,
        "Q014",
        r#"query q($h: String) {
  match { $p: Person(handle: $h) }
  return { $p.name }
}"#,
        r#"query q($h: String) { match { $p: Person { handle: $h } } return { $p.name } }"#,
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
  match { $p: Person  -- adults only
  $p.age > 18 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.age > 18 } return { $p.name } }"#,
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
  match { $p: Person  optional $p worksAt $c }
  return { $p.name, $c.name }
}"#,
        r#"query q() { match { $p: Person optional { $p worksAt $c } } return { $p.name, $c.name } }"#,
    ),
    (
        r#"not without braces"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $q: Person  not $p knows $q }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $q: Person not { $p knows $q } } return { $p.name } }"#,
    ),
    (
        r#"delete of two variables"#,
        "Q014",
        r#"query w() {
  $p knows $q
  delete $p, $q
}"#,
        r#"query w() { $p knows $q delete $p delete $q }"#,
    ),
    (
        r#"null in a read filter"#,
        "Q014",
        r#"query q() {
  match { $p: Person  $p.city = null }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.city is null } return { $p.name } }"#,
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
  $p: Person { handle: $h }
  delete $p
  returning { @id }
}"#,
        r#"query w($h: String) { $p: Person { handle: $h } delete $p }"#,
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
        r#"$n -1 after one space"#,
        "Q016",
        r#"query q() {
  match { $p: Person  let $n = $p.age  $n -1 > 0 }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person let $n = $p.age $n - 1 > 0 } return { $p.name } }"#,
    ),
    (
        r#"a leading minus on a new line in return"#,
        "Q016",
        r#"query q() {
  match { $p: Person }
  return {
    $p.age
    -$p.age as neg
  }
}"#,
        r#"query q() { match { $p: Person } return { $p.age, -$p.age as neg } }"#,
    ),
    (
        r#"a leading minus after two spaces in match"#,
        "Q016",
        r#"query q() {
  match { $p: Person  let $x = $p.age  -1 < $x }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person let $x = $p.age (-1 < $x) } return { $p.name } }"#,
    ),
    (
        r#"from as a bare edge"#,
        "Q017",
        r#"query q() {
  match { $a: Person  $a from $b }
  return { $b.name }
}"#,
        r#"query q() { match { $a: Person $a "from" $b } return { $b.name } }"#,
    ),
    (
        r#"insert as a bare edge in a write"#,
        "Q017",
        r#"query w() {
  $a: Person  $a insert $b
  delete $b
}"#,
        r#"query w() { $a: Person $a "insert" $b delete $b }"#,
    ),
    (
        r#"a bare property name"#,
        "Q018",
        r#"query q() {
  match { $p: Person  name = "x" }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.name = "x" } return { $p.name } }"#,
    ),
    (
        r#"a bare parameter name"#,
        "Q018",
        r#"query q($h: String) {
  match { $p: Person { handle: h } }
  return { $p.name }
}"#,
        r#"query q($h: String) { match { $p: Person { handle: $h } } return { $p.name } }"#,
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
        r#"a space before a call parenthesis"#,
        "Q019",
        r#"query colleagues($of: String) {
  match { $p: Person { handle: $of }  $p worksAt $c  $q worksAt $c }
  return { $q }
}

query q($h: String) {
  match { from colleagues ($h) }
  return { $q.name }
}"#,
        r#"query colleagues($of: String) { match { $p: Person { handle: $of } $p worksAt $c $q worksAt $c } return { $q } } query q($h: String) { match { from colleagues($h) } return { $q.name } }"#,
    ),
    (
        r#"exists as a function"#,
        "Q019",
        r#"query q() {
  match { $p: Person  exists($p.city) }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person $p.city is not null } return { $p.name } }"#,
    ),
    (
        r#"an inline read without match"#,
        "Q020",
        r#"query q() {
  match { $p: Person  from { match { $p knows $q } order { $q.age } } }
  return { $p.name }
}"#,
        r#"query q() { match { $p: Person from { match { $p knows $q } return { $p } order { $q.age } } } return { $p.name } }"#,
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

/// `text` as a source: its own edition or settings line, or `gq 3;` before it.
pub(crate) fn as_source(text: &str) -> String {
    if text.starts_with("gq") || text.starts_with("GQ") || text.starts_with("set ") {
        text.to_string()
    } else {
        format!("gq 3;\n{text}")
    }
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
