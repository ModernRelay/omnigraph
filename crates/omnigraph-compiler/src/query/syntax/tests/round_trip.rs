//! Parse, print, parse again over generated declarations
//! (docs/rfcs/2026-11-07-gq-3.md, section 10).
//!
//! The generator builds random read and write declarations from the
//! grammar: bindings, triples in every traversal form, `let`, `from` over
//! inline reads, calls and `$r`, `optional` with its own `where`, a `where`
//! over the arithmetic ladder, calls and blocks (each with its elements and
//! its own `where`), `return`, `order` and `limit`, and write groups with
//! their statements. The property is that printing a tree and parsing the
//! text gives the tree back.
//!
//! Names are drawn from pools that include the contextual keywords (`let`,
//! `from`, `count`, `match`, …), because an identifier that is also a
//! keyword is where a grammar's lookahead is tested; inside `match` every
//! edge name in the pool is written bare.
//!
//! The suite runs 2,000 cases.

use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use crate::query::file::MutationWriteMode;
use crate::query::syntax::parse::parse_source;
use crate::query::syntax::print::print_decl;
use crate::query::syntax::*;
use crate::query::syntax::{EdgeSelector, Traversal};
use crate::query::value::{AggFunc, CompOp, Literal};

const VARS: &[&str] = &["a", "b", "p", "q", "from", "let", "count", "x1"];
const PROPS: &[&str] = &[
    "name", "age", "from", "let", "in", "order", "match", "count", "limit", "insert", "not",
];
const EDGES: &[&str] = &[
    "knows", "worksAt", "from", "let", "limit", "optional", "in", "and", "not", "is", "null",
    "count", "exists", "true", "delete", "update", "where", "match",
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
    // Not `let`, `from`, `optional`, `exists`: a registry row is never named an
    // element keyword (section 5).
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

fn leaf() -> impl Strategy<Value = Expr> {
    prop_oneof![
        3 => literal().prop_map(Expr::Literal),
        3 => pick(VARS).prop_map(Expr::Variable),
        3 => (pick(VARS), pick(PROPS)).prop_map(|(variable, property)| Expr::Prop { variable, property }),
        1 => (pick(VARS), Just("@id".to_string())).prop_map(|(variable, property)| Expr::Prop { variable, property }),
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

/// Expressions over the whole ladder. `depth` bounds nesting; pattern-level
/// recursion (blocks, inline reads) goes through [`pattern`].
fn expr(depth: u32) -> BoxedStrategy<Expr> {
    if depth == 0 {
        return leaf().boxed();
    }
    let sub = || expr(depth - 1);
    prop_oneof![
        4 => leaf(),
        2 => (arith_op(), sub(), sub()).prop_map(|(op, l, r)| Expr::Arith { op, left: Box::new(l), right: Box::new(r) }),
        1 => sub().prop_map(|e| Expr::Neg(Box::new(e))),
        2 => (comp_op(), sub(), sub()).prop_map(|(op, l, r)| Expr::Compare { op, left: Box::new(l), right: Box::new(r), at: SourceSpan::default() }),
        1 => (sub(), sub()).prop_map(|(n, l)| Expr::In { needle: Box::new(n), list: Box::new(l), at: SourceSpan::default() }),
        1 => (sub(), any::<bool>()).prop_map(|(e, negated)| Expr::IsNull { expr: Box::new(e), negated }),
        1 => sub().prop_map(|e| Expr::Not(Box::new(e))),
        1 => (sub(), sub()).prop_map(|(l, r)| Expr::And(Box::new(l), Box::new(r))),
        1 => (sub(), sub()).prop_map(|(l, r)| Expr::Or(Box::new(l), Box::new(r))),
        2 => call(depth - 1).prop_map(Expr::Call),
        1 => (agg(), sub()).prop_map(|(func, arg)| Expr::AggDistinct { func, arg: Box::new(arg) }),
        1 => block(depth - 1).prop_map(|b| Expr::Block(Box::new(b))),
    ]
    .boxed()
}

fn call(depth: u32) -> BoxedStrategy<Call> {
    let arg = prop_oneof![
        4 => expr(depth).prop_map(Arg::Expr),
        1 => read_body(depth).prop_map(|b| Arg::Inline(Box::new(b))),
    ];
    (
        pick(CALLS),
        proptest::collection::vec(arg, 0..3),
        proptest::collection::vec(
            (
                pick(OPTIONS),
                prop_oneof![4 => expr(depth), 1 => pick(IDENTS).prop_map(Expr::Ident)],
            ),
            0..2,
        ),
    )
        .prop_map(|(name, args, options)| Call {
            name,
            args,
            options,
        })
        .boxed()
}

fn block(depth: u32) -> BoxedStrategy<Block> {
    prop_oneof![
        (
            agg(),
            proptest::option::of(expr(depth)),
            pattern(depth, 1..3)
        )
            .prop_map(|(f, arg, pattern)| Block {
                func: BlockFunc::Agg(f),
                arg: arg.map(Box::new),
                pattern,
                at: SourceSpan::default(),
            }),
        pattern(depth, 1..3).prop_map(|pattern| Block {
            func: BlockFunc::Exists,
            arg: None,
            pattern,
            at: SourceSpan::default(),
        }),
    ]
    .boxed()
}

fn traversal() -> impl Strategy<Value = Element> {
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
                Element::Traversal(Traversal {
                    src,
                    selector,
                    dst,
                    min_hops,
                    max_hops,
                    undirected,
                    edge_binding,
                })
            },
        )
}

fn relation(depth: u32) -> BoxedStrategy<RelExpr> {
    prop_oneof![
        2 => read_body(depth).prop_map(|b| RelExpr::Inline(Box::new(b))),
        2 => call(depth).prop_map(RelExpr::Call),
        1 => pick(VARS).prop_map(RelExpr::Variable),
    ]
    .boxed()
}

fn element(depth: u32) -> BoxedStrategy<Element> {
    let binding = (pick(VARS), pick(TYPES)).prop_map(|(variable, type_name)| Element::Binding {
        variable,
        type_name,
    });
    let base = prop_oneof![3 => binding, 3 => traversal()];
    if depth == 0 {
        return base.boxed();
    }
    let d = depth - 1;
    prop_oneof![
        6 => base,
        2 => (pick(VARS), expr(d)).prop_map(|(name, e)| Element::Let { name, value: LetValue::Expr(e) }),
        1 => (pick(VARS), read_body(d)).prop_map(|(name, b)| Element::Let { name, value: LetValue::Relation(Box::new(b)) }),
        2 => relation(d).prop_map(Element::From),
        1 => pattern(d, 1..3).prop_map(Element::Optional),
    ]
    .boxed()
}

/// Elements and an optional `where`.
fn pattern(depth: u32, n: std::ops::Range<usize>) -> BoxedStrategy<Pattern> {
    (
        proptest::collection::vec(element(depth), n),
        proptest::option::of(expr(depth.min(2))),
    )
        .prop_map(|(elements, predicate)| Pattern {
            elements,
            predicate,
            spans: Default::default(),
        })
        .boxed()
}

fn read_body(depth: u32) -> BoxedStrategy<ReadBody> {
    let projection = (expr(depth.min(2)), proptest::option::of(pick(ALIASES)))
        .prop_map(|(expr, alias)| Projection { expr, alias });
    let ordering = (
        prop_oneof![4 => expr(depth.min(1)), 1 => pick(IDENTS).prop_map(Expr::Ident)],
        proptest::option::of(any::<bool>()),
    )
        .prop_map(|(expr, descending)| Ordering { expr, descending });
    let limit = proptest::option::of(prop_oneof![
        (0u64..100).prop_map(Limit::Literal),
        pick(VARS).prop_map(Limit::Param),
    ]);
    (
        pattern(depth, 1..4),
        any::<bool>(),
        proptest::collection::vec(projection, 1..4),
        proptest::collection::vec(ordering, 0..3),
        limit,
    )
        .prop_map(|(pattern, distinct, projections, order, limit)| ReadBody {
            spans: Default::default(),
            pattern: Box::new(pattern),
            distinct,
            projections,
            order,
            limit,
        })
        .boxed()
}

fn decl() -> impl Strategy<Value = Decl> {
    let param = (
        pick(VARS),
        proptest::sample::select(&["String", "I64", "F64", "[String]", "Vector(3)"][..]),
        any::<bool>(),
        proptest::option::of((any::<bool>(), (0i128..100).prop_map(Literal::Integer))),
    )
        .prop_map(|(name, type_name, nullable, default)| Param {
            name,
            type_name: type_name.to_string(),
            nullable,
            default: default.map(|(negative, value)| ParamDefault { negative, value }),
        });
    let body = prop_oneof![
        3 => read_body(3).prop_map(DeclBody::Read),
        1 => write_body().prop_map(DeclBody::Write),
    ];
    (proptest::collection::vec(param, 0..3), body, any::<bool>()).prop_map(
        |(params, body, commit)| Decl {
            name: "q".into(),
            params,
            annotations: Vec::new(),
            write_mode: if commit && matches!(body, DeclBody::Write(_)) {
                MutationWriteMode::AsCommit
            } else {
                MutationWriteMode::Publish
            },
            body,
        },
    )
}

fn statement() -> impl Strategy<Value = Stmt> {
    let value = prop_oneof![
        4 => expr(1),
        1 => Just(Expr::Literal(Literal::Null)),
    ];
    let assignments = proptest::collection::vec(
        (pick(PROPS), value).prop_map(|(property, value)| Assignment { property, value }),
        1..3,
    )
    .boxed();
    prop_oneof![
        (pick(TYPES), assignments.clone()).prop_map(|(type_name, assignments)| Stmt::Insert {
            type_name,
            assignments
        }),
        (pick(VARS), assignments).prop_map(|(variable, assignments)| Stmt::Update {
            variable,
            assignments
        }),
        pick(VARS).prop_map(|variable| Stmt::Delete { variable }),
    ]
}

/// Write groups. A group after the first opens with its `match`: two
/// adjacent groups with no `match` between them print as one group with more
/// statements (section 7: consecutive statements are one group). A group
/// with no `match` has no `where`.
fn write_body() -> BoxedStrategy<WriteBody> {
    let group = |min: usize| {
        (
            pattern(1, min..3),
            proptest::collection::vec(statement(), 1..3),
        )
            .prop_map(|(mut pattern, statements)| {
                if pattern.elements.is_empty() {
                    pattern.predicate = None;
                }
                WriteGroup {
                    spans: Default::default(),
                    pattern,
                    statements,
                }
            })
    };
    (group(0), proptest::collection::vec(group(1), 0..2))
        .prop_map(|(first, rest)| WriteBody {
            groups: std::iter::once(first).chain(rest).collect(),
        })
        .boxed()
}

/// One round trip: `Ok` when the reparse is the tree, `Err` with the text
/// otherwise.
pub(crate) fn round_trip(d: &Decl) -> Result<(), String> {
    let text = print_decl(d);
    match parse_source(&text) {
        Ok(SourceFile {
            body: Body::Decls(decls),
            ..
        }) if decls.len() == 1 && decls[0] == *d => Ok(()),
        Ok(other) => Err(format!("reparsed to another tree\n{text}\n{other:#?}")),
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
    let result = runner.run(&decl(), |d| {
        round_trip(&d).map_err(TestCaseError::fail)?;
        Ok(())
    });
    if let Err(e) = result {
        panic!("{e}");
    }
}

#[test]
fn print_then_parse_is_the_identity_on_generated_declarations() {
    print_then_parse(2_000);
}
