//! Parse, print, parse again over generated edition-3 declarations
//! (docs/rfcs/2026-11-07-gq-3.md, section 10).
//!
//! The generator builds random read declarations from the edition-3 grammar:
//! bindings, triples in every traversal form, filters over the arithmetic
//! ladder and calls, `let`, `from` over inline reads, calls and `$r`, the
//! blocks as clauses and as values, `optional`, and `return`, `order` and
//! `limit`. The property is that printing a tree and parsing the text gives
//! the tree back. Separators between clauses and between `return` items are
//! drawn too, so the whitespace-separated forms the grammar admits are
//! exercised.
//!
//! Names are drawn from pools that include the contextual keywords (`let`,
//! `from`, `count`, `match`, …), because an identifier that is also a
//! keyword is where a grammar's lookahead is tested.
//!
//! The ordinary suite runs a few thousand cases; the `hunt:` test runs the
//! 100,000 the RFC names.

use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use super::ast::{AggFunc, CompOp, EdgeSelector, Literal, MutationWriteMode, Traversal};
use super::ast3::*;
use super::parser3::parse_query3;
use super::print3::{Style, print_decl};

const VARS: &[&str] = &["a", "b", "p", "q", "from", "let", "count", "x1"];
const PROPS: &[&str] = &[
    "name", "age", "from", "let", "in", "order", "match", "count", "limit", "insert", "not",
];
const EDGES: &[&str] = &[
    "knows", "worksAt", "from", "let", "limit", "optional", "in", "and", "count", "exists", "true",
    "delete", "update",
];
const TYPES: &[&str] = &["Person", "Doc", "@Operator"];
const ALIASES: &[&str] = &[
    "n", "total", "from", "let", "count", "order", "distinct", "limit",
];
/// Bare names, legal only as a whole `order` key (a return alias) and as a
/// whole option value (an enumerated word). Excludes the reserved six and the
/// two Boolean literals, which are not names.
const IDENTS: &[&str] = &[
    "n",
    "total",
    "from",
    "let",
    "count",
    "exists",
    "date",
    "embedding",
    "any",
    "refuse",
];
const CALLS: &[&str] = &[
    // Not `date`/`datetime`: with one string argument the call is the date
    // literal production.
    // Not `let`, `from`, `optional`, `exists`: a registry row is never named a
    // clause keyword (section 5).
    "f",
    "coalesce",
    "contains",
    "starts_with",
    "nearest",
    "bm25",
    "l2",
    "union",
    "rrf",
    "count",
    "distance",
];
const OPTIONS: &[&str] = &["k", "recall", "max_edits", "weights", "mode", "from"];

fn pick(pool: &'static [&'static str]) -> impl Strategy<Value = String> {
    proptest::sample::select(pool).prop_map(str::to_string)
}

fn literal() -> impl Strategy<Value = Literal> {
    let scalar = prop_oneof![
        (0i128..1000).prop_map(Literal::Integer),
        (0u32..100_000).prop_map(|n| Literal::Float(f64::from(n) / 100.0)),
        (1u32..1000, -30i32..30).prop_map(|(m, e)| Literal::Float(
            format!("{}.{}e{e}", m / 10, m % 10).parse().unwrap()
        )),
        "[a-z ]{0,6}".prop_map(Literal::String),
        any::<bool>().prop_map(Literal::Bool),
        Just(Literal::Date("2025-01-01".into())),
        Just(Literal::DateTime("2025-01-01T00:00:00Z".into())),
    ];
    prop_oneof![
        4 => scalar.clone(),
        1 => proptest::collection::vec(scalar, 0..3).prop_map(Literal::List),
    ]
}

fn leaf() -> impl Strategy<Value = Expr3> {
    prop_oneof![
        3 => literal().prop_map(Expr3::Literal),
        3 => pick(VARS).prop_map(Expr3::Variable),
        3 => (pick(VARS), pick(PROPS)).prop_map(|(variable, property)| Expr3::Prop { variable, property }),
        1 => (pick(VARS), Just("@id".to_string())).prop_map(|(variable, property)| Expr3::Prop { variable, property }),
    ]
}

fn arith_op() -> impl Strategy<Value = ArithOp> {
    proptest::sample::select(
        &[
            ArithOp::Add,
            ArithOp::Sub,
            ArithOp::Mul,
            ArithOp::Div,
            ArithOp::Rem,
        ][..],
    )
}

fn comp_op() -> impl Strategy<Value = CompOp> {
    proptest::sample::select(
        &[
            CompOp::Eq,
            CompOp::Ne,
            CompOp::Gt,
            CompOp::Lt,
            CompOp::Ge,
            CompOp::Le,
        ][..],
    )
}

fn agg() -> impl Strategy<Value = AggFunc> {
    proptest::sample::select(
        &[
            AggFunc::Count,
            AggFunc::Sum,
            AggFunc::Avg,
            AggFunc::Min,
            AggFunc::Max,
        ][..],
    )
}

/// Expressions over the whole ladder. `depth` bounds nesting; clause-level
/// recursion (blocks, inline reads) goes through [`clauses`].
fn expr(depth: u32) -> BoxedStrategy<Expr3> {
    if depth == 0 {
        return leaf().boxed();
    }
    let sub = || expr(depth - 1);
    prop_oneof![
        4 => leaf(),
        2 => (arith_op(), sub(), sub()).prop_map(|(op, l, r)| Expr3::Arith { op, left: Box::new(l), right: Box::new(r) }),
        1 => sub().prop_map(|e| Expr3::Neg(Box::new(e))),
        2 => (comp_op(), sub(), sub()).prop_map(|(op, l, r)| Expr3::Compare { op, left: Box::new(l), right: Box::new(r) }),
        1 => (sub(), sub()).prop_map(|(n, l)| Expr3::In { needle: Box::new(n), list: Box::new(l), at: SourceSpan::default() }),
        1 => (sub(), any::<bool>()).prop_map(|(e, negated)| Expr3::IsNull { expr: Box::new(e), negated }),
        1 => sub().prop_map(|e| Expr3::Not(Box::new(e))),
        1 => (sub(), sub()).prop_map(|(l, r)| Expr3::And(Box::new(l), Box::new(r))),
        1 => (sub(), sub()).prop_map(|(l, r)| Expr3::Or(Box::new(l), Box::new(r))),
        2 => call(depth - 1).prop_map(Expr3::Call),
        1 => (agg(), sub()).prop_map(|(func, arg)| Expr3::AggDistinct { func, arg: Box::new(arg) }),
        1 => block(depth - 1).prop_map(|b| Expr3::Block(Box::new(b))),
    ]
    .boxed()
}

fn call(depth: u32) -> BoxedStrategy<Call3> {
    let arg = prop_oneof![
        4 => expr(depth).prop_map(Arg3::Expr),
        1 => read_body(depth).prop_map(|b| Arg3::Inline(Box::new(b))),
    ];
    (
        pick(CALLS),
        proptest::collection::vec(arg, 0..3),
        proptest::collection::vec(
            (
                pick(OPTIONS),
                prop_oneof![4 => expr(depth), 1 => pick(IDENTS).prop_map(Expr3::Ident)],
            ),
            0..2,
        ),
    )
        .prop_map(|(name, args, options)| Call3 {
            name,
            args,
            options,
        })
        .boxed()
}

fn block(depth: u32) -> BoxedStrategy<Block3> {
    prop_oneof![
        (
            agg(),
            proptest::option::of(expr(depth)),
            clauses(depth, 1..3)
        )
            .prop_map(|(f, arg, clauses)| Block3 {
                func: BlockFunc3::Agg(f),
                arg: arg.map(Box::new),
                clauses,
            }),
        clauses(depth, 1..3).prop_map(|clauses| Block3 {
            func: BlockFunc3::Exists,
            arg: None,
            clauses,
        }),
    ]
    .boxed()
}

fn traversal() -> impl Strategy<Value = Clause3> {
    let selector = prop_oneof![
        4 => pick(EDGES).prop_map(EdgeSelector::Named),
        1 => Just(EdgeSelector::Named("@property".into())),
        1 => proptest::collection::vec(pick(EDGES), 2..4).prop_map(EdgeSelector::Alternation),
        1 => Just(EdgeSelector::Wildcard),
    ];
    let bounds = prop_oneof![
        4 => Just((1u32, Some(1u32))),
        1 => (0u32..3, 1u32..4).prop_map(|(m, n)| (m, Some(m + n))),
        1 => (0u32..3).prop_map(|m| (m, None)),
    ];
    (
        pick(VARS),
        proptest::option::of(pick(VARS)),
        selector,
        any::<bool>(),
        bounds,
        pick(VARS),
    )
        .prop_map(
            |(src, edge_binding, selector, undirected, (min_hops, max_hops), dst)| {
                Clause3::Traversal(TraversalEq(Traversal {
                    src,
                    selector,
                    dst,
                    min_hops,
                    max_hops,
                    undirected,
                    edge_binding,
                }))
            },
        )
}

fn relation(depth: u32) -> BoxedStrategy<RelExpr3> {
    prop_oneof![
        2 => read_body(depth).prop_map(|b| RelExpr3::Inline(Box::new(b))),
        2 => call(depth).prop_map(RelExpr3::Call),
        1 => pick(VARS).prop_map(RelExpr3::Variable),
    ]
    .boxed()
}

fn clause(depth: u32) -> BoxedStrategy<Clause3> {
    let binding = (
        pick(VARS),
        pick(TYPES),
        proptest::collection::vec((pick(PROPS), expr(depth.min(1))), 0..3),
    )
        .prop_map(|(variable, type_name, prop_matches)| Clause3::Binding {
            variable,
            type_name,
            prop_matches,
        });
    let base = prop_oneof![
        3 => binding,
        3 => traversal(),
        // A filter that is the bare name `from` or `let` and is followed by a
        // `$` clause is that keyword's clause: the generator does not write
        // that shape, which is never well typed. A product of two variables
        // alone, `$a * $b`, is the wildcard triple; as a filter it is never
        // Boolean.
        3 => expr(depth.min(2))
            // A lone name, string or bare meta-field as a whole filter
            // sits between a lone-variable filter and a `$` clause as the
            // edge of a triple; it is never Boolean.
            .prop_filter("lone non-Boolean leaf", |e| !matches!(e,
                Expr3::Ident(_) | Expr3::Meta(_) | Expr3::Literal(super::ast::Literal::String(_))))
            // ... and `not` over one is the brace-less `not` over that
            // triple, refused (Q014); `not` of a number is never Boolean.
            .prop_filter("a lone product of two variables", |e| {
                let product = |e: &Expr3| matches!(e,
                    Expr3::Arith { op: ArithOp::Mul, left, right }
                        if matches!(**left, Expr3::Variable(_)) && matches!(**right, Expr3::Variable(_)));
                !(product(e) || matches!(e, Expr3::Not(inner) if product(inner)))
            })
            .prop_map(Clause3::Filter),
    ];
    if depth == 0 {
        return base.boxed();
    }
    let d = depth - 1;
    prop_oneof![
        6 => base,
        2 => (pick(VARS), expr(d)).prop_map(|(name, e)| Clause3::Let { name, value: LetValue3::Expr(e) }),
        1 => (pick(VARS), read_body(d)).prop_map(|(name, b)| Clause3::Let { name, value: LetValue3::Relation(Box::new(b)) }),
        2 => relation(d).prop_map(Clause3::From),
        1 => clauses(d, 1..3).prop_map(Clause3::Not),
        1 => clauses(d, 1..3).prop_map(Clause3::Optional),
    ]
    .boxed()
}

/// A lone-variable filter, then `from $b`, or a Boolean literal and another
/// lone-variable filter, is refused (Q017): it is the shape of a triple over
/// an edge named `from`, `let`, `true` or `false`, which are written quoted.
fn bare_edge_shape(cs: &[Clause3]) -> bool {
    let lone = |c: &Clause3| matches!(c, Clause3::Filter(Expr3::Variable(_)));
    cs.windows(2)
        .any(|w| lone(&w[0]) && matches!(w[1], Clause3::From(RelExpr3::Variable(_))))
        || cs.windows(3).any(|w| {
            lone(&w[0])
                && matches!(
                    w[1],
                    Clause3::Filter(Expr3::Literal(super::ast::Literal::Bool(_)))
                )
                && lone(&w[2])
        })
}

fn clauses(depth: u32, n: std::ops::Range<usize>) -> BoxedStrategy<Vec<Clause3>> {
    proptest::collection::vec(clause(depth), n)
        .prop_filter("a bare-edge shape (Q017)", |cs| !bare_edge_shape(cs))
        .boxed()
}

fn read_body(depth: u32) -> BoxedStrategy<ReadBody3> {
    let projection = (expr(depth.min(2)), proptest::option::of(pick(ALIASES)))
        .prop_map(|(expr, alias)| Projection3 { expr, alias });
    let ordering = (
        prop_oneof![4 => expr(depth.min(1)), 1 => pick(IDENTS).prop_map(Expr3::Ident)],
        proptest::option::of(any::<bool>()),
    )
        .prop_map(|(expr, descending)| Ordering3 { expr, descending });
    let limit = proptest::option::of(prop_oneof![
        (0u64..100).prop_map(Limit3::Literal),
        pick(VARS).prop_map(Limit3::Param),
    ]);
    (
        clauses(depth, 1..4),
        any::<bool>(),
        proptest::collection::vec(projection, 1..4),
        proptest::collection::vec(ordering, 0..3),
        limit,
    )
        .prop_map(|(clauses, distinct, projections, order, limit)| ReadBody3 {
            clauses,
            distinct,
            projections,
            order,
            limit,
        })
        .boxed()
}

fn decl() -> impl Strategy<Value = Decl3> {
    let param = (
        pick(VARS),
        proptest::sample::select(&["String", "I64", "F64", "[String]", "Vector(3)"][..]),
        any::<bool>(),
        proptest::option::of((any::<bool>(), (0i128..100).prop_map(Literal::Integer))),
    )
        .prop_map(|(name, type_name, nullable, default)| Param3 {
            name,
            type_name: type_name.to_string(),
            nullable,
            default: default.map(|(negative, value)| ParamDefault { negative, value }),
        });
    let body = prop_oneof![
        3 => read_body(3).prop_map(DeclBody3::Read),
        1 => write_body().prop_map(DeclBody3::Write),
    ];
    (proptest::collection::vec(param, 0..3), body, any::<bool>()).prop_map(
        |(params, body, commit)| Decl3 {
            name: "q".into(),
            params,
            annotations: Vec::new(),
            write_mode: if commit && matches!(body, DeclBody3::Write(_)) {
                MutationWriteMode::AsCommit
            } else {
                MutationWriteMode::Publish
            },
            body,
        },
    )
}

fn statement() -> impl Strategy<Value = Stmt3> {
    let value = prop_oneof![
        4 => expr(1),
        1 => Just(Expr3::Literal(Literal::Null)),
    ];
    let assignments = proptest::collection::vec(
        (pick(PROPS), value).prop_map(|(property, value)| Assignment3 { property, value }),
        1..3,
    )
    .boxed();
    prop_oneof![
        (pick(TYPES), assignments.clone()).prop_map(|(type_name, assignments)| Stmt3::Insert {
            type_name,
            assignments
        }),
        (pick(VARS), assignments).prop_map(|(variable, assignments)| Stmt3::Update {
            variable,
            assignments
        }),
        pick(VARS).prop_map(|variable| Stmt3::Delete { variable }),
    ]
}

/// Write groups. A group after the first has at least one clause: two
/// adjacent groups with no clause between them print as one group with more
/// statements (section 7: consecutive statements are one group).
fn write_body() -> BoxedStrategy<WriteBody3> {
    let group = |min: usize| {
        (
            clauses(1, min..3),
            proptest::collection::vec(statement(), 1..3),
        )
            .prop_map(|(clauses, statements)| WriteGroup3 {
                clauses,
                statements,
            })
    };
    (group(0), proptest::collection::vec(group(1), 0..2))
        .prop_map(|(first, rest)| WriteBody3 {
            groups: std::iter::once(first).chain(rest).collect(),
        })
        .boxed()
}

fn style() -> impl Strategy<Value = Style> {
    (
        proptest::sample::select(&["  ", "\n", " "][..]),
        proptest::sample::select(&[", ", "  ", "\n"][..]),
    )
        .prop_map(|(clause_sep, projection_sep)| Style {
            clause_sep,
            projection_sep,
            assignment_sep: ", ",
        })
}

/// How one round trip ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Trip {
    Equal,
    /// The arrow recognizer took a generated filter of the shape
    /// `$a - x - $b`, `x` a bare name, a string or a bare meta-field: legal
    /// grammar, never well-typed arithmetic. Counted, not failed.
    ArrowShadow,
}

/// One round trip: `Ok` when the reparse is the tree (or a counted known
/// shadow), `Err` with the text otherwise.
pub(crate) fn round_trip(d: &Decl3, style: Style) -> Result<Trip, String> {
    let text = format!("gq 3;\n{}", print_decl(d, style));
    match parse_query3(&text) {
        Ok(File3 {
            body: Body3::Decls(decls),
            ..
        }) if decls.len() == 1 && decls[0] == *d => Ok(Trip::Equal),
        Ok(other) => Err(format!("reparsed to another tree\n{text}\n{other:#?}")),
        Err(e) if e.code.as_str() == "Q007" => Ok(Trip::ArrowShadow),
        Err(e) => Err(format!("refused: {e}\n{text}")),
    }
}

fn print_then_parse(cases: u32) {
    let mut runner = TestRunner::new(Config {
        cases,
        max_shrink_iters: 4096,
        failure_persistence: None,
        ..Config::default()
    });
    let shadows = std::sync::atomic::AtomicUsize::new(0);
    let result = runner.run(&(decl(), style()), |(d, s)| {
        match round_trip(&d, s).map_err(TestCaseError::fail)? {
            Trip::Equal => {}
            Trip::ArrowShadow => {
                shadows.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        Ok(())
    });
    if let Err(e) = result {
        panic!("{e}");
    }
    let shadows = shadows.into_inner();
    // The shadow must stay rare: it needs a bare name, a string or a bare
    // meta-field between two minus signs.
    assert!(shadows * 50 < cases as usize, "{shadows} of {cases}");
}

#[test]
fn print_then_parse_is_the_identity_on_generated_declarations() {
    print_then_parse(2_000);
}

#[test]
#[ignore = "hunt: the parse, print and parse property over 100,000 generated declarations"]
fn print_then_parse_hunt() {
    print_then_parse(100_000);
}
