//! Parse, print, parse again over generated sources (the GQ 3 RFC,
//! section 10), and the bound expression's display read back through the
//! type checker.
//!
//! # The surface round trip
//!
//! The generator builds random source files of one to three query
//! declarations, or of declarations and an `explain` over the last one, and
//! covers every production of the declaration grammar that builds a tree:
//!
//! - the header: a name (keywords included), parameters of every base,
//!   list and vector type with or without `?` and with or without a default
//!   (any literal, a list among them, under an optional `-`), annotations
//!   with no parentheses or one argument at least, positional and `name = value`
//!   arguments over literals, `$params`, `@meta` fields and bare words
//!   (`@budget(plan = 3)` among them), and a write's `as commit` and
//!   `as unattached branch` with no base, a quoted base or a `$param` base;
//! - read bodies: `match { … }` and its `where`, `return` and
//!   `return distinct` with aliases, `order` with no direction, `asc` and
//!   `desc` (an expression or a bare alias as the key) and `limit` as a
//!   literal (up to `u64::MAX`) or a `$param`;
//! - elements: bindings (an `@` catalogue type among them), triples with an
//!   edge binding, a hop range (`{m,n}` and `{m,}`), a directed or
//!   undirected selector that is one name, a catalogue edge, an
//!   alternation or `*`; `let` of a value and of an inline read; `from` over
//!   an inline read, a call or a `$name`, with and without `as $score`;
//!   `optional { … }` with its own `where`;
//! - expressions: the whole logic and arithmetic ladder with unary minus,
//!   comparisons, `in`, `is [not] null`, infix `contains` and
//!   `starts_with`, `$x`, `$x.prop`, `$x.@meta`, a bare `@meta`, calls with
//!   positional arguments, inline reads and options (a bare word as an
//!   option's whole value), blocks (`count`, `sum($x)`, …, `exists`, each
//!   with its own `where`) and `count(distinct …)`;
//! - literals: `null` as an operand, integers to `i64::MAX`, floats over every finite bit
//!   pattern, infinity and the subnormals, Booleans, `date(…)` and
//!   `datetime(…)`, and lists, nested, holding negative numbers (a list is
//!   where the grammar reads `-` before a number as part of the literal),
//!   and lists holding a `$name` among those elements;
//!   strings holding every escape the decoder reads (`\"`, `\\`, `\n`, `\r`,
//!   `\t`), a backslash before an `n`, `//`, `/*` and `*/`, braces, raw
//!   control characters and non-ASCII text;
//! - writes: groups of `insert`, `update … set` and `delete`, each group
//!   after the first opening with its `match`, assignments of `null` and of
//!   expressions.
//!
//! Each tree is printed ([`print_file`]) and the text parsed back; the parse
//! must be the tree, and printing the parse must give the same text (a
//! fixed point). The text is then respelled and parsed again: comments,
//! line and block, replace some of the spaces between tokens, a space
//! follows some minus signs (a sign, a unary minus or a negative number in
//! a list), and a string's
//! `\n`, `\t` and `\r` escapes become the raw characters, which the grammar
//! reads too (a string may span lines). The respelled text must parse to
//! the same tree.
//!
//! Names are drawn from pools that include the contextual keywords (`let`,
//! `from`, `count`, `match`, …), because an identifier that is also a
//! keyword is where a grammar's lookahead is tested; inside `match` every
//! edge name in the pool is written bare.
//!
//! [`every_grammar_rule_the_generator_is_meant_to_cover_is_produced`] holds
//! the coverage: it parses the generated texts of a fixed seed and requires
//! every rule of `grammar.pest` to appear in some parse, except the rules
//! [`NOT_GENERATED`] lists, each with its reason (silent rules, rules inside
//! atomic tokens, recognizers that accept only refused text, and the
//! statement grammar: settings, `show`, `branch`, `commit` and `load`,
//! which [`print_file`] does not print). It checks rule names, not every
//! alternative inside a rule; the list above is the alternatives.
//!
//! The ordinary test runs 450 cases; the nightly one the RFC's 100,000,
//! split over the available cores.
//!
//! # The display round trip
//!
//! [`IRExpr`]'s `Display` writes GQ text. The second generator builds
//! well-typed predicates and return values over a fixed schema and
//! parameter list (integer, float, string, date, datetime and Boolean
//! operands; arithmetic with its widening casts, unary minus, comparisons,
//! null tests, `not`/`and`/`or`, `if`, `coalesce`, `concat`, `now()` and
//! `match_terms`), type-checks the query, displays every stored filter and
//! the returned value, puts the displayed text in the same query and
//! type-checks it again: the second declaration must equal the first, node
//! for node, except that a filter on `true` is dropped from both (the
//! checker drops a written `where true` and keeps a predicate it folds to
//! `true`). Display is lossy in two places, which the generator leaves out:
//! list membership (`$x in $l`), `contains(…)` and `starts_with(…)`
//! display as infix `contains` and `starts_with`, which the type checker
//! refuses (explain output, GQT plan expectations and the docs print that
//! spelling); and a non-finite float displays as `inf`, which is not a
//! literal. The generator writes no ranking, aggregate or bare node
//! variable, so `Nearest`, `Aggregate` and `Variable` are not displayed.
//!
//! The ordinary test runs 300 cases; the nightly one 20,000.

use std::collections::BTreeSet;

use pest::Parser as _;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

use crate::catalog::{Catalog, build_catalog};
use crate::ir::core::{CoreDecl, Term};
use crate::ir::expr::IRExpr;
use crate::query::check::check_source;
use crate::query::file::{MutationWriteMode, UnattachedBase};
use crate::query::syntax::parse::parse_source;
use crate::query::syntax::print::{literal as print_literal, print_file};
use crate::query::syntax::*;
use crate::query::syntax::{EdgeSelector, Traversal};
use crate::query::value::{AggFunc, CompOp, Literal};
use crate::schema::parser::parse_schema;

const VARS: &[&str] = &["a", "b", "p", "q", "from", "let", "count", "x1"];
const PROPS: &[&str] = &[
    "name", "age", "from", "let", "in", "order", "match", "count", "limit", "insert", "not",
];
const EDGES: &[&str] = &[
    "knows", "worksAt", "from", "let", "limit", "optional", "in", "and", "not", "is", "null",
    "count", "exists", "true", "delete", "update", "where", "match",
];
const TYPES: &[&str] = &["Person", "Doc", "@Operator"];
const META: &[&str] = &["id", "src", "dst", "type"];
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
    // Not a reserved word or a keyword (`let`, `from`, `where`, `match`,
    // `and`, …): a call is never named one (section 7), which
    // `a_keyword_is_never_a_call_name` holds.
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
    // A foreign word is a name in operand position.
    "with",
    // `||` and `case` respell these two (section 3).
    "concat",
    "if",
    "lower",
];
const OPTIONS: &[&str] = &["k", "recall", "max_edits", "weights", "mode", "from"];
/// Declaration names: distinct within a file, since a repeated name is
/// refused.
const DECL_NAMES: &[&str] = &[
    "q", "match", "from", "query", "explain", "return", "set", "x_1",
];
const ANNOTATIONS: &[&str] = &["budget", "description", "deprecated", "match", "x_1"];
const ANNOTATION_KEYS: &[&str] = &["plan", "rounds", "true", "in", "as"];
/// A bare word as an annotation value. Not `true` or `false`, which read
/// as the Boolean literals.
const ANNOTATION_WORDS: &[&str] = &["plan", "null", "date", "in", "any"];
const BASE_TYPES: &[&str] = &[
    "String", "Blob", "Bool", "I32", "I64", "U32", "U64", "F32", "F64", "DateTime", "Date",
];

fn pick(pool: &'static [&'static str]) -> impl Strategy<Value = String> {
    proptest::sample::select(pool).prop_map(str::to_string)
}

/// String contents: every character the printer escapes, a backslash
/// before an `n`, comment openers and closers, braces, raw control
/// characters and non-ASCII text.
fn text() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            6 => proptest::sample::select(&["a", "z", " ", "é", "→", "0", "$x"][..]),
            1 => proptest::sample::select(
                &[
                    "\"", "\\", "\n", "\t", "\r", "\\n", "//", "/*", "*/", "{", "}", "\u{0}",
                    "\u{1b}", "\u{7f}",
                ][..]
            ),
        ],
        0..6,
    )
    .prop_map(|parts| parts.concat())
}

/// A float; `signed` admits a negative one, which only a list writes.
fn float(signed: bool) -> impl Strategy<Value = f64> {
    let magnitude = prop_oneof![
        3 => (0u32..100_000).prop_map(|n| f64::from(n) / 100.0),
        2 => any::<u64>()
            .prop_map(|bits| f64::from_bits(bits).abs())
            .prop_filter("finite", |f| f.is_finite()),
        1 => proptest::sample::select(
            &[0.0, f64::MIN_POSITIVE, 5e-324, f64::MAX, f64::INFINITY, 1e21, 1e-7][..]
        ),
    ];
    (magnitude, any::<bool>()).prop_map(
        move |(f, negative)| {
            if signed && negative { -f } else { f }
        },
    )
}

/// A literal that is not a list. `signed`: a negative number, as a list's
/// element.
fn scalar(signed: bool) -> BoxedStrategy<Literal> {
    let integer = if signed {
        prop_oneof![(-1000i64..1000), any::<i64>()].boxed()
    } else {
        prop_oneof![(0i64..1000), (0i64..=i64::MAX)].boxed()
    };
    prop_oneof![
        3 => integer.prop_map(|n| Literal::Integer(i128::from(n))),
        3 => float(signed).prop_map(Literal::Float),
        3 => text().prop_map(Literal::String),
        1 => any::<bool>().prop_map(Literal::Bool),
        1 => text().prop_map(Literal::Date),
        1 => text().prop_map(Literal::DateTime),
    ]
    .boxed()
}

/// A list literal: negative numbers and nested lists among its elements.
fn list() -> impl Strategy<Value = Literal> {
    scalar(true)
        .prop_recursive(2, 8, 4, |inner| {
            proptest::collection::vec(prop_oneof![3 => scalar(true), 1 => inner], 0..4)
                .prop_map(Literal::List)
        })
        .prop_map(|l| match l {
            Literal::List(_) => l,
            other => Literal::List(vec![other]),
        })
}

/// A literal where an operand stands: no negative number, which the
/// grammar reads as unary minus over a literal.
fn literal() -> impl Strategy<Value = Literal> {
    prop_oneof![
        4 => scalar(false),
        1 => list(),
    ]
}

/// A list holding a parameter: literals (negative numbers and lists among
/// them) and `$name`s, one at least, which only the type checker sorts.
fn value_list() -> impl Strategy<Value = Expr> {
    let element = prop_oneof![
        2 => scalar(true).prop_map(Expr::Literal),
        1 => list().prop_map(Expr::Literal),
        2 => pick(VARS).prop_map(Expr::Variable),
    ];
    (
        proptest::collection::vec(element, 0..3),
        pick(VARS),
        any::<proptest::sample::Index>(),
    )
        .prop_map(|(mut elements, variable, at)| {
            elements.insert(at.index(elements.len() + 1), Expr::Variable(variable));
            Expr::List {
                elements,
                at: SourceSpan::default(),
            }
        })
}

fn leaf() -> impl Strategy<Value = Expr> {
    prop_oneof![
        3 => literal().prop_map(Expr::Literal),
        1 => value_list(),
        3 => pick(VARS).prop_map(Expr::Variable),
        3 => (pick(VARS), pick(PROPS)).prop_map(|(variable, property)| Expr::Prop { variable, property }),
        1 => (pick(VARS), pick(META)).prop_map(|(variable, m)| Expr::Prop { variable, property: format!("@{m}") }),
        1 => pick(META).prop_map(Expr::Meta),
        // `null`, an operand wherever its context fixes its type.
        1 => Just(Expr::Literal(Literal::Null)),
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
            AggFunc::Stddev,
            AggFunc::Percentile,
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
    let infix = proptest::sample::select(&[InfixStringOp::Contains, InfixStringOp::StartsWith][..]);
    prop_oneof![
        4 => leaf(),
        2 => (arith_op(), sub(), sub()).prop_map(|(op, l, r)| Expr::Arith { op, left: Box::new(l), right: Box::new(r) }),
        1 => sub().prop_map(|e| Expr::Neg(Box::new(e))),
        2 => (comp_op(), sub(), sub()).prop_map(|(op, l, r)| Expr::Compare { op, left: Box::new(l), right: Box::new(r), at: SourceSpan::default() }),
        1 => (sub(), sub()).prop_map(|(n, l)| Expr::In { needle: Box::new(n), list: Box::new(l), at: SourceSpan::default() }),
        1 => (infix, sub(), sub()).prop_map(|(op, l, r)| Expr::InfixString { op, left: Box::new(l), right: Box::new(r), at: SourceSpan::default() }),
        1 => (sub(), any::<bool>()).prop_map(|(e, negated)| Expr::IsNull { expr: Box::new(e), negated }),
        1 => sub().prop_map(|e| Expr::Not(Box::new(e))),
        1 => (sub(), sub()).prop_map(|(l, r)| Expr::And(Box::new(l), Box::new(r))),
        1 => (sub(), sub()).prop_map(|(l, r)| Expr::Or(Box::new(l), Box::new(r))),
        2 => call(depth - 1).prop_map(Expr::Call),
        1 => (agg(), sub()).prop_map(|(func, arg)| Expr::AggDistinct { func, arg: Box::new(arg), fraction: None }),
        1 => block(depth - 1).prop_map(|b| Expr::Block(Box::new(b))),
    ]
    .boxed()
}

fn call(depth: u32) -> BoxedStrategy<Call> {
    // `cast(x, T)`: a scalar type name is its second argument.
    let cast = (expr(depth), pick(CAST_TYPES)).prop_map(|(x, ty)| Call {
        name: "cast".into(),
        args: vec![Arg::Expr(x), Arg::Expr(Expr::Ident(ty))],
        options: Vec::new(),
        at: SourceSpan::default(),
    });
    let arg = prop_oneof![
        4 => expr(depth).prop_map(Arg::Expr),
        1 => read_body(depth).prop_map(|b| Arg::Inline(Box::new(b))),
    ];
    let general = (
        pick(CALLS),
        proptest::collection::vec(arg, 0..4),
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
            at: SourceSpan::default(),
        })
        .boxed();
    // `if(c, a, b)`, which `case` respells; `null` as its `else` half the
    // time, which a `case` with no `else` respells.
    let if3 = (
        expr(depth),
        expr(depth),
        prop_oneof![expr(depth), Just(Expr::Literal(Literal::Null))],
    )
        .prop_map(|(c, a, b)| Call {
            name: "if".into(),
            args: vec![Arg::Expr(c), Arg::Expr(a), Arg::Expr(b)],
            options: Vec::new(),
            at: SourceSpan::default(),
        });
    prop_oneof![8 => general, 1 => cast, 1 => if3].boxed()
}

/// The scalar types `cast` converts between.
const CAST_TYPES: &[&str] = &[
    "String", "Bool", "I32", "I64", "U32", "U64", "F32", "F64", "DateTime", "Date",
];

fn block(depth: u32) -> BoxedStrategy<Block> {
    prop_oneof![
        (
            agg(),
            proptest::option::of(expr(depth)),
            proptest::option::of(expr(depth)),
            pattern(depth, 1..3)
        )
            .prop_map(|(f, arg, fraction, pattern)| Block {
                func: BlockFunc::Agg(f),
                // A second argument is written only beside a first.
                fraction: fraction.filter(|_| arg.is_some()).map(Box::new),
                arg: arg.map(Box::new),
                pattern,
                at: SourceSpan::default(),
            }),
        pattern(depth, 1..3).prop_map(|pattern| Block {
            func: BlockFunc::Exists,
            arg: None,
            fraction: None,
            pattern,
            at: SourceSpan::default(),
        }),
    ]
    .boxed()
}

fn traversal() -> impl Strategy<Value = Element> {
    let member = prop_oneof![6 => pick(EDGES), 1 => Just("@member".to_string())];
    let selector = prop_oneof![
        4 => pick(EDGES).prop_map(EdgeSelector::Named),
        1 => Just(EdgeSelector::Named("@property".into())),
        1 => proptest::collection::vec(member, 2..4).prop_map(EdgeSelector::Alternation),
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
        2 => (relation(d), proptest::option::of(pick(VARS))).prop_map(|(rel, score)| Element::From { rel, score }),
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
        proptest::option::of(any::<bool>()),
    )
        .prop_map(|(expr, descending, nulls_first)| Ordering {
            expr,
            descending,
            nulls_first,
        });
    let limit = proptest::option::of(prop_oneof![
        3 => (0u64..100).prop_map(Limit::Literal),
        1 => any::<u64>().prop_map(Limit::Literal),
        2 => pick(VARS).prop_map(Limit::Param),
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

fn param() -> impl Strategy<Value = Param> {
    let type_name = prop_oneof![
        3 => pick(BASE_TYPES),
        2 => pick(BASE_TYPES).prop_map(|t| format!("[{t}]")),
        1 => (1u32..5000).prop_map(|n| format!("Vector({n})")),
    ];
    (
        pick(VARS),
        type_name,
        any::<bool>(),
        proptest::option::of((any::<bool>(), literal())),
    )
        .prop_map(|(name, type_name, nullable, default)| Param {
            name,
            type_name,
            nullable,
            default: default.map(|(negative, value)| ParamDefault { negative, value }),
        })
}

fn annotation() -> impl Strategy<Value = Annotation> {
    let value = prop_oneof![
        3 => literal().prop_map(AnnotationValue::Literal),
        1 => pick(VARS).prop_map(AnnotationValue::Variable),
        1 => pick(META).prop_map(AnnotationValue::MetaField),
        2 => pick(ANNOTATION_WORDS).prop_map(AnnotationValue::Ident),
    ];
    let arg = prop_oneof![
        1 => value.clone().prop_map(AnnotationArg::Positional),
        1 => (pick(ANNOTATION_KEYS), value).prop_map(|(k, v)| AnnotationArg::Named(k, v)),
    ];
    let budget = (0u32..100).prop_map(|n| Annotation {
        name: "budget".into(),
        args: Some(vec![AnnotationArg::Named(
            "plan".into(),
            AnnotationValue::Literal(Literal::Integer(i128::from(n))),
        )]),
        at: SourceSpan::default(),
    });
    prop_oneof![
        1 => budget,
        // An annotation without arguments takes no parentheses: `args` is
        // `None` or holds one argument at least.
        3 => (pick(ANNOTATIONS), proptest::option::of(proptest::collection::vec(arg, 1..3)))
            .prop_map(|(name, args)| Annotation { name, args, at: SourceSpan::default() }),
    ]
}

/// A write's mode. An unattached base is a commit id: not empty, no
/// surrounding space, no control character.
fn write_mode() -> impl Strategy<Value = MutationWriteMode> {
    let id = "[a-z0-9]([a-z0-9 \"\\\\-]{0,10}[a-z0-9])?";
    prop_oneof![
        2 => Just(MutationWriteMode::Publish),
        2 => Just(MutationWriteMode::AsCommit),
        1 => Just(MutationWriteMode::Unattached { base: None }),
        1 => id.prop_map(|s| MutationWriteMode::Unattached { base: Some(UnattachedBase::Literal(s)) }),
        1 => pick(VARS).prop_map(|p| MutationWriteMode::Unattached { base: Some(UnattachedBase::Param(p)) }),
    ]
}

/// A declaration named `q`; [`file`] renames it.
fn decl() -> impl Strategy<Value = Decl> {
    let body = prop_oneof![
        3 => read_body(3).prop_map(DeclBody::Read),
        1 => write_body().prop_map(DeclBody::Write),
    ];
    (
        proptest::collection::vec(param(), 0..3),
        proptest::collection::vec(annotation(), 0..3),
        body,
        write_mode(),
    )
        .prop_map(|(params, annotations, body, mode)| Decl {
            name: "q".into(),
            at: Default::default(),
            params,
            annotations,
            write_mode: if matches!(body, DeclBody::Write(_)) {
                mode
            } else {
                // A read takes no write clause (`Q011`).
                MutationWriteMode::Publish
            },
            body,
            respellings: Default::default(),
        })
}

fn statement() -> impl Strategy<Value = Stmt> {
    let value = prop_oneof![
        4 => expr(1),
        1 => Just(Expr::Literal(Literal::Null)),
    ];
    let assignments = proptest::collection::vec(
        (
            prop_oneof![9 => pick(PROPS), 1 => pick(&["@src", "@dst"])],
            value,
        )
            .prop_map(|(property, value)| Assignment {
                property,
                value,
                at: SourceSpan::default(),
            }),
        1..3,
    )
    .boxed();
    prop_oneof![
        (
            prop_oneof![
                Just(InsertMode::Insert),
                Just(InsertMode::Upsert),
                Just(InsertMode::Replace)
            ],
            pick(TYPES),
            assignments.clone()
        )
            .prop_map(|(mode, type_name, assignments)| Stmt::Insert {
                mode,
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

/// A source file: one to three declarations with distinct names, the last
/// one under `explain` or not; and the seed its respelling draws from.
fn file() -> impl Strategy<Value = (SourceFile, u64)> {
    (
        proptest::collection::vec(decl(), 1..=3),
        proptest::sample::subsequence(DECL_NAMES, 3).prop_shuffle(),
        any::<bool>(),
        any::<u64>(),
    )
        .prop_map(|(mut decls, names, explain, seed)| {
            for (d, name) in decls.iter_mut().zip(names) {
                d.name = name.to_string();
            }
            let body = if explain {
                let decl = Box::new(decls.pop().expect("one declaration at least"));
                Body::Explain {
                    before: decls,
                    decl,
                }
            } else {
                Body::Decls(decls)
            };
            (
                SourceFile {
                    settings: Vec::new(),
                    body,
                },
                seed,
            )
        })
}

/// `text` respelled without changing its tree: outside strings, some of
/// the spaces become a block comment, a line comment or a newline and a
/// tab; inside strings, some `\n`, `\t` and `\r` escapes become the raw
/// character.
fn respell(text: &str, seed: u64) -> String {
    let mut state = seed | 1;
    let mut next = move || {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut out = String::with_capacity(text.len() * 2);
    let mut chars = text.chars();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if in_string {
            match c {
                '\\' => {
                    let escaped = chars.next().expect("a printed escape is complete");
                    let raw = match escaped {
                        'n' => Some('\n'),
                        't' => Some('\t'),
                        'r' => Some('\r'),
                        _ => None,
                    };
                    match raw {
                        Some(raw) if next() % 2 == 0 => out.push(raw),
                        _ => {
                            out.push('\\');
                            out.push(escaped);
                        }
                    }
                }
                '"' => {
                    in_string = false;
                    out.push(c);
                }
                _ => out.push(c),
            }
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                out.push(c);
            }
            // A single-quoted string is copied as written.
            '\'' => {
                out.push(c);
                while let Some(c) = chars.next() {
                    out.push(c);
                    if c == '\\' {
                        out.extend(chars.next());
                    } else if c == '\'' {
                        break;
                    }
                }
            }
            // Whitespace may follow a minus sign, a unary minus and a
            // negative number's alike (section 7); not inside a float's
            // exponent, which is one token.
            '-' if !out.ends_with(['e', 'E']) && next() % 4 == 0 => out.push_str("- "),
            ' ' => match next() % 8 {
                0 => out.push_str(" /* a \"comment\" { // */ "),
                1 => out.push_str(" // a \"comment\" */ /* {\n"),
                2 => out.push_str("\n\t"),
                _ => out.push(' '),
            },
            _ => out.push(c),
        }
    }
    out
}

/// One round trip: `Ok` when the reparse of the printed text and of its
/// respelling are the tree and printing the reparse gives the text back,
/// `Err` with the text otherwise.
pub(crate) fn round_trip(file: &SourceFile, seed: u64) -> Result<(), String> {
    let text = print_file(file);
    let reparsed = match parse_source(&text) {
        Ok(reparsed) if reparsed == *file => reparsed,
        Ok(other) => return Err(format!("reparsed to another tree\n{text}\n{other:#?}")),
        Err(e) => return Err(format!("refused: {e}\n{text}")),
    };
    let again = print_file(&reparsed);
    if again != text {
        return Err(format!("printing is not a fixed point\n{text}\n{again}"));
    }
    let respelled = respell(
        &crate::arbitrary::respell_spellings(&text, seed, &crate::arbitrary::Spelling::ALL).0,
        seed,
    );
    match parse_source(&respelled) {
        Ok(other) if other == *file => Ok(()),
        Ok(other) => Err(format!(
            "the respelling reparsed to another tree\n{respelled}\n{other:#?}"
        )),
        Err(e) => Err(format!("the respelling was refused: {e}\n{respelled}")),
    }
}

/// `cases` round trips. The nightly breadth is split over the available
/// cores, to stay within the per-test bound of the `ci` nextest profile;
/// each thread builds its own strategy, which costs most of a second.
fn print_then_parse(cases: u32) {
    let threads = if cases < 10_000 {
        1
    } else {
        std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(8) as u32
    };
    std::thread::scope(|scope| {
        let runs: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(move || {
                    let mut runner = TestRunner::new(Config {
                        cases: cases.div_ceil(threads),
                        max_shrink_iters: 4096,
                        failure_persistence: None,
                        ..Config::default()
                    });
                    runner.run(&file(), |(f, seed)| {
                        round_trip(&f, seed).map_err(TestCaseError::fail)?;
                        Ok(())
                    })
                })
            })
            .collect();
        for run in runs {
            if let Err(e) = run.join().unwrap_or_else(|p| std::panic::resume_unwind(p)) {
                panic!("{e}");
            }
        }
    });
}

#[test]
fn print_then_parse_is_the_identity_on_generated_declarations() {
    print_then_parse(450);
}

#[test]
#[ignore = "nightly: the RFC's 100,000 generated sources; the gate runs 450"]
fn print_then_parse_is_the_identity_on_generated_declarations_at_nightly_breadth() {
    print_then_parse(100_000);
}

/// Grammar rules no generated text is meant to produce, by reason. Every
/// rule whose name ends in `_rec` is a recognizer (the grammar's header):
/// it accepts only text the builder refuses, so it is left out too, except
/// `bare_name_rec`, which is also how a bare alias in `order` and a bare
/// option value parse.
const NOT_GENERATED: &[(&str, &[&str])] = &[
    (
        "silent: a `_{ }` rule leaves no pair in the parse",
        &[
            "WHITESPACE",
            "COMMENT",
            "LINE_COMMENT",
            "BLOCK_COMMENT",
            "param_item",
            "read_tail",
            "edge_name",
            "call_arg",
            "write_element",
            "call_keyword",
        ],
    ),
    (
        "inside an atomic token, whose inner rules leave no pair",
        &["ident_chars", "string_char", "sq_string_char", "call_word"],
    ),
    (
        "only inside a lookahead, which leaves no pair",
        // `stmt_keyword` only bounds elements before a statement.
        &["continuation", "stmt_keyword"],
    ),
    (
        "recognizers without the `_rec` suffix: every text they accept is refused",
        &[
            // `mutation` and `view` are recognized only to print `query`.
            "mutation_decl",
            "view_decl",
            "kw_mutation",
            "kw_view",
            "missing_param_list",
            // A statement beside a declaration.
            "statement_trailer",
            // Foreign element forms: `for … in`, `unwind`, `let $x in`,
            // arrows, Cypher patterns, property maps and braces after a
            // binding, Cypher's star range, `WITH` items.
            "list_to_rows",
            "kw_for",
            "kw_unwind",
            "arrow_traversal",
            "arrow_open",
            "arrow_close",
            "cypher_pattern",
            "cypher_typed_node",
            "cypher_node",
            "cypher_prop",
            "cypher_rel",
            "cypher_rel_body",
            "cypher_hops",
            "cypher_name",
            "star_mark",
            // `$x.out(…)`, a method call on a variable.
            "prop_method_head",
            // A keyword or reserved word written as a call's name.
            "call_keyword_word",
            // Edition 2's statement `where`, refused with the group form.
            "where_stmt",
            "binding_brace_filter",
            "prop_match_list",
            "prop_match",
            "brace_item",
            "brace_colon_compare",
            "brace_compare",
            "brace_body",
            "binding_paren_head",
            "with_item",
            "set_item",
            // `||`, refused with `concat` as the fix.
            "concat_bars",
            // A reserved word as an alias or a bare operand.
            "reserved",
            "reserved_property",
            "reserved_alias",
            // Foreign words, recognized only to be refused.
            "fw_filter",
            "fw_with",
            "fw_call",
            "fw_match",
            "fw_optional",
            "fw_detach",
            "fw_create",
            "fw_by",
            "fw_offset",
            "fw_group",
            "fw_having",
            "fw_retrieve",
            "fw_between",
            "fw_like",
            "fw_starts",
            "fw_ends",
        ],
    ),
    (
        "the statement grammar: settings, `show`, `branch`, `commit` and `load` \
         have their own nodes, which `print_file` does not print",
        &[
            "setting_stmt",
            "set_stmt",
            "reset_stmt",
            "show_stmt",
            "setting_name",
            "setting_value",
            "branch_stmt",
            "commit_stmt",
            "load_stmt",
            "load_target",
            "load_kind",
            "load_option",
            "load_mode",
            "load_mode_name",
            "load_delimiter",
            "load_header",
            "load_columns",
            "kw_load",
            "kw_csv",
            "kw_mode",
            "kw_delimiter",
            "kw_header",
            "kw_columns",
            "branch_create",
            "create_from_commit",
            "branch_delete",
            "branch_merge",
            "branch_list",
            "branch_merge_base",
            "branch_diff",
            "diff_after_page",
            "diff_limit",
            "dry_run_clause",
            "branch_name",
            "commit_publish",
            "commit_drop",
            "kw_create",
            "kw_merge",
            "kw_merge_base",
            "kw_diff",
            "kw_to",
            "kw_after",
            "kw_page",
            "kw_dry",
            "kw_run",
            "kw_list",
            "kw_into",
            "kw_reset",
            "kw_show",
            "kw_all",
            "kw_publish",
            "kw_drop",
        ],
    ),
];

/// Every rule `grammar.pest` defines, read as the rule-set pin reads it.
fn grammar_rules() -> BTreeSet<&'static str> {
    include_str!("../grammar.pest")
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
        .collect()
}

/// The files the coverage test draws from its fixed seed.
const COVERAGE_FILES: usize = 200;

#[test]
fn every_grammar_rule_the_generator_is_meant_to_cover_is_produced() {
    let rules = grammar_rules();
    let excluded: BTreeSet<&str> = NOT_GENERATED
        .iter()
        .flat_map(|(_, names)| names.iter().copied())
        .collect();
    let unknown: Vec<&&str> = excluded.difference(&rules).collect();
    assert!(
        unknown.is_empty(),
        "excluded rules the grammar does not define: {unknown:?}"
    );
    let meant: BTreeSet<&str> = rules
        .iter()
        .copied()
        .filter(|r| !excluded.contains(r) && (!r.ends_with("_rec") || *r == "bare_name_rec"))
        .collect();

    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let strategy = file();
    let mut produced = BTreeSet::new();
    for _ in 0..COVERAGE_FILES {
        let (f, seed) = strategy
            .new_tree(&mut runner)
            .expect("the generator draws a file")
            .current();
        for text in [
            print_file(&f),
            respell(
                &crate::arbitrary::respell_spellings(
                    &print_file(&f),
                    seed,
                    &crate::arbitrary::Spelling::ALL,
                )
                .0,
                seed,
            ),
        ] {
            let pairs = QueryParser::parse(Rule::query_file, &text)
                .unwrap_or_else(|e| panic!("the grammar refuses a generated text: {e}\n{text}"));
            produced.extend(pairs.flatten().map(|p| format!("{:?}", p.as_rule())));
        }
    }
    let missing: Vec<&&str> = meant.iter().filter(|r| !produced.contains(**r)).collect();
    assert!(
        missing.is_empty(),
        "grammar rules the generator never produced over its fixed seed; generate them or \
         list them in `NOT_GENERATED` with the reason: {missing:?}"
    );
}

/// The words `call_keyword` names in `grammar.pest`: every reserved word
/// and declaration keyword a call is never named.
fn call_keywords() -> Vec<String> {
    let grammar = include_str!("../grammar.pest");
    let line = grammar
        .lines()
        .find(|l| l.starts_with("call_keyword "))
        .expect("the grammar defines `call_keyword`");
    let mut words = Vec::new();
    for kw in line.split('|').map(str::trim) {
        let kw = kw
            .trim_start_matches("call_keyword = _{")
            .trim()
            .trim_end_matches('}')
            .trim();
        if kw == "reserved" {
            words.extend(["and", "or", "not", "is", "null", "in"].map(String::from));
        } else if let Some(word) = kw.strip_prefix("kw_") {
            words.push(word.to_string());
        }
    }
    assert!(words.len() > 10, "{words:?}");
    words
}

/// Section 7: a reserved word or keyword is never a call name. Written as
/// one, in a `return` item, a `where` and an `order` key, it is `Q019`
/// with a printed fix; a word of the generator's call pool beside it
/// parses as a call.
#[test]
fn a_keyword_is_never_a_call_name() {
    for word in call_keywords() {
        for text in [
            format!("query q() {{ match {{ $p: Person }} return {{ {word}($p.a, $p.b) as x }} }}"),
            format!("query q() {{ match {{ $p: Person }} where {word}($p.a) return {{ $p }} }}"),
            format!(
                "query q() {{ match {{ $p: Person }} return {{ $p }} order {{ {word}($p.a) }} }}"
            ),
        ]
        .into_iter()
        // `not(x)` is the negation of a parenthesized operand; only a
        // second argument makes it a call.
        .filter(|text| word != "not" || text.contains(", "))
        {
            let d = parse_source(&text).expect_err(&text);
            assert_eq!(d.code.as_str(), "Q019", "{text}: {d}");
            assert!(d.fix.is_some(), "{text}: no fix printed: {d}");
            assert!(
                d.message.contains(&format!("`{word}`")),
                "{text}: the message does not name the word: {d}"
            );
        }
    }
    for word in CALLS {
        let text = format!("query q() {{ match {{ $p: Person }} return {{ {word}($p.a) as x }} }}");
        parse_source(&text).unwrap_or_else(|e| panic!("{text}: {e}"));
    }
    // `where` after the pattern is the clause, parenthesized or not.
    parse_source("query q() { match { $p: Person } where($p.a > 1) return { $p } }")
        .expect("`where (…)` is the clause");
    // An infix reserved word written as a call: the fix writes it infix.
    let d = parse_source("query q() { match { $p: Person } where and($p.a, $p.b) return { $p } }")
        .expect_err("refused");
    let edits = &d.suggestion.as_ref().expect("a machine fix").edits;
    assert_eq!(edits[0].replacement, "$p.a and $p.b", "{d}");
}

/// Section 7: an annotation without arguments takes no parentheses. The
/// generator writes `@name` and `@name(args)`; `@name()` is `Q012`, whose
/// machine fix removes the parentheses and gives the generator's form.
#[test]
fn an_annotation_without_arguments_takes_no_parentheses() {
    for name in ANNOTATIONS {
        let text = format!("query q() @{name}() {{ match {{ $p: Person }} return {{ $p }} }}");
        let d = parse_source(&text).expect_err(&text);
        assert_eq!(d.code.as_str(), "Q012", "{text}: {d}");
        let edits = &d.suggestion.as_ref().expect("a machine fix").edits;
        let mut fixed = text.clone();
        for e in edits.iter().rev() {
            fixed.replace_range(e.start..e.end, &e.replacement);
        }
        assert_eq!(
            fixed,
            format!("query q() @{name} {{ match {{ $p: Person }} return {{ $p }} }}"),
            "{d}"
        );
        parse_source(&fixed).unwrap_or_else(|e| panic!("{fixed}: {e}"));
    }
}

/// Section 7: whitespace may follow a unary minus, a parameter default's
/// sign and a list's negative number; each spelling is the tree without
/// it.
#[test]
fn whitespace_may_follow_a_minus_sign() {
    for (spaced, tight) in [
        (
            "query q($n: I64 = - 1) { match { $p: Person } where $p.a in [- 1, - 2.5] and - $p.b > - 3 return { $p } }",
            "query q($n: I64 = -1) { match { $p: Person } where $p.a in [-1, -2.5] and -$p.b > -3 return { $p } }",
        ),
        (
            "query q() { match { $p: Person } where $p.a in [-\n 1] return { $p } }",
            "query q() { match { $p: Person } where $p.a in [-1] return { $p } }",
        ),
    ] {
        assert_eq!(
            parse_source(spaced).unwrap_or_else(|e| panic!("{spaced}: {e}")),
            parse_source(tight).unwrap_or_else(|e| panic!("{tight}: {e}")),
            "{spaced}"
        );
    }
}

// ---------------------------------------------------------------------------
// The display round trip.

const SCHEMA: &str = r#"
node Person {
    name: String @key
    bio: String @index(fulltext)
    city: String?
    age: I32?
    rank: I64
    score: F64?
    born: Date?
    seen: DateTime?
    active: Bool?
}
"#;

/// Every parameter the generated expressions read.
const PARAMS: &str =
    "$n: I64, $i: I32?, $f: F64, $s: String, $t: String?, $b: Bool, $d: Date, $w: DateTime?";

fn ir_catalog() -> Catalog {
    build_catalog(&parse_schema(SCHEMA).expect("the schema parses")).expect("the catalog builds")
}

/// The operand domains the typed generator draws from.
#[derive(Clone, Copy, Debug)]
enum Dom {
    Int,
    Float,
    Str,
    Date,
    DateTime,
    Bool,
}

const DOMAINS: &[Dom] = &[
    Dom::Int,
    Dom::Float,
    Dom::Str,
    Dom::Date,
    Dom::DateTime,
    Dom::Bool,
];

fn select_str(pool: &'static [&'static str]) -> BoxedStrategy<String> {
    pick(pool).boxed()
}

/// A leaf of `dom`: a parameter, a property or a literal.
fn typed_leaf(dom: Dom) -> BoxedStrategy<String> {
    match dom {
        Dom::Int => prop_oneof![
            2 => select_str(&["$n", "$i", "$p.rank", "$p.age"]),
            1 => prop_oneof![0i64..1000, 0i64..=i64::MAX].prop_map(|n| n.to_string()),
        ]
        .boxed(),
        Dom::Float => prop_oneof![
            2 => select_str(&["$f", "$p.score"]),
            1 => float(false)
                .prop_filter("finite", |f| f.is_finite())
                .prop_map(|f| print_literal(&Literal::Float(f))),
        ]
        .boxed(),
        Dom::Str => prop_oneof![
            2 => select_str(&["$s", "$t", "$p.name", "$p.city", "$p.bio"]),
            1 => text().prop_map(|s| print_literal(&Literal::String(s))),
        ]
        .boxed(),
        Dom::Date => select_str(&["$d", "$p.born", "date(\"2024-02-29\")"]),
        Dom::DateTime => select_str(&[
            "$w",
            "$p.seen",
            "now()",
            "datetime(\"2024-02-29T12:00:00Z\")",
        ]),
        Dom::Bool => select_str(&["$b", "$p.active", "true", "false"]),
    }
}

/// A well-typed expression of `dom`, fully parenthesized: parentheses leave
/// no node, so the displayed text, with its minimal ones, must type to the
/// same tree.
fn typed(dom: Dom, depth: u32) -> BoxedStrategy<String> {
    if depth == 0 {
        return typed_leaf(dom);
    }
    let sub = move |d: Dom| typed(d, depth - 1);
    let conditional =
        (sub(Dom::Bool), sub(dom), sub(dom)).prop_map(|(c, a, b)| format!("if({c}, {a}, {b})"));
    let coalesce = (sub(dom), sub(dom)).prop_map(|(a, b)| format!("coalesce({a}, {b})"));
    let arith = |left: Dom, right: Dom| {
        (
            sub(left),
            proptest::sample::select(&["+", "-", "*", "/", "%"][..]),
            sub(right),
        )
            .prop_map(|(a, op, b)| format!("({a} {op} {b})"))
    };
    match dom {
        Dom::Int => prop_oneof![
            3 => typed_leaf(dom),
            3 => arith(Dom::Int, Dom::Int),
            1 => sub(Dom::Int).prop_map(|a| format!("(-{a})")),
            1 => conditional,
            1 => coalesce,
        ]
        .boxed(),
        Dom::Float => prop_oneof![
            3 => typed_leaf(dom),
            2 => arith(Dom::Float, Dom::Float),
            1 => arith(Dom::Int, Dom::Float),
            1 => arith(Dom::Float, Dom::Int),
            1 => sub(Dom::Float).prop_map(|a| format!("(-{a})")),
            1 => conditional,
            1 => coalesce,
        ]
        .boxed(),
        Dom::Str => prop_oneof![
            3 => typed_leaf(dom),
            1 => (sub(Dom::Str), sub(Dom::Str)).prop_map(|(a, b)| format!("concat({a}, {b})")),
            1 => conditional,
            1 => coalesce,
        ]
        .boxed(),
        Dom::Date | Dom::DateTime => prop_oneof![
            3 => typed_leaf(dom),
            1 => conditional,
            1 => coalesce,
        ]
        .boxed(),
        Dom::Bool => {
            let comparable = proptest::sample::select(
                &[
                    (Dom::Int, Dom::Int),
                    (Dom::Int, Dom::Float),
                    (Dom::Float, Dom::Float),
                    (Dom::Str, Dom::Str),
                    (Dom::Date, Dom::Date),
                    (Dom::DateTime, Dom::DateTime),
                    (Dom::Bool, Dom::Bool),
                ][..],
            );
            let compare = (comparable, comp_op()).prop_flat_map(move |((l, r), op)| {
                (sub(l), sub(r)).prop_map(move |(a, b)| format!("({a} {op} {b})"))
            });
            let null_test = (proptest::sample::select(DOMAINS), any::<bool>()).prop_flat_map(
                move |(d, negated)| {
                    sub(d).prop_map(move |a| {
                        format!("({a} is {}null)", if negated { "not " } else { "" })
                    })
                },
            );
            // `terms`' text is a String literal or parameter (`T19`).
            let query = prop_oneof![
                1 => select_str(&["$s", "$t"]),
                1 => text().prop_map(|s| print_literal(&Literal::String(s))),
            ];
            let terms = (
                query,
                proptest::sample::select(&["all", "any"][..]),
                0u32..3,
            )
                .prop_map(|(q, mode, edits)| {
                    format!("match_terms($p.bio, terms({q}, mode: {mode}, max_edits: {edits}))")
                });
            prop_oneof![
                2 => typed_leaf(dom),
                4 => compare,
                2 => null_test,
                1 => sub(Dom::Bool).prop_map(|a| format!("(not {a})")),
                2 => (sub(Dom::Bool), sub(Dom::Bool)).prop_map(|(a, b)| format!("({a} and {b})")),
                2 => (sub(Dom::Bool), sub(Dom::Bool)).prop_map(|(a, b)| format!("({a} or {b})")),
                1 => terms,
                1 => conditional,
                1 => coalesce,
            ]
            .boxed()
        }
    }
}

fn ir_query(predicate: Option<&str>, value: &str) -> String {
    let filter = predicate.map_or(String::new(), |p| format!(" where {p}"));
    format!("query q({PARAMS}) {{ match {{ $p: Person }}{filter} return {{ {value} as v }} }}")
}

/// The one declaration of `source`, checked; `None` when it is refused.
/// A filter on `true` is dropped: the checker drops a written `where true`
/// but keeps a predicate it folds to `true`, and both filter nothing.
fn checked(catalog: &Catalog, source: &str) -> Option<CoreDecl> {
    let mut decls = check_source(catalog, source).ok()?;
    let (_, decl) = decls.pop()?;
    let mut core = decl.ok()?.core;
    if let CoreDecl::Read(read) = &mut core {
        drop_true_filters(&mut read.term);
    }
    Some(core)
}

fn drop_true_filters(term: &mut Term) {
    if let Term::Filter {
        input,
        pred: IRExpr::Literal(Literal::Bool(true), _),
        ..
    } = term
    {
        *term = std::sync::Arc::unwrap_or_clone(std::mem::replace(input, Term::Unit.boxed()));
        return drop_true_filters(term);
    }
    match term {
        Term::Filter { input, .. }
        | Term::Project { input, .. }
        | Term::Order { input, .. }
        | Term::Cut { input, .. } => drop_true_filters(std::sync::Arc::make_mut(input)),
        _ => {}
    }
}

/// The stored filters of a read, outermost first, and its returned `v`.
fn filters_and_value(core: &CoreDecl) -> (Vec<IRExpr>, IRExpr) {
    let CoreDecl::Read(read) = core else {
        panic!("a read")
    };
    let mut filters = Vec::new();
    let mut value = None;
    let mut wanted = "v".to_string();
    let mut term = &read.term;
    loop {
        match term {
            Term::Filter { input, pred, .. } => {
                filters.push(pred.clone());
                term = input;
            }
            Term::Project { input, items, .. } => {
                if value.is_none() {
                    match items.iter().find(|(n, _)| *n == wanted) {
                        Some((_, IRExpr::AliasRef(inner, _))) if *inner != wanted => {
                            wanted = inner.clone();
                        }
                        Some((_, e)) => value = Some(e.clone()),
                        None => {}
                    }
                }
                term = input;
            }
            Term::Order { input, .. } | Term::Cut { input, .. } => term = input,
            _ => break,
        }
    }
    (filters, value.expect("the read returns `v`"))
}

/// One display round trip: `Ok(true)` when the source types and the
/// displayed text types to the same declaration, `Ok(false)` when the
/// source is refused (a fold of a constant can overflow or divide by zero).
fn display_round_trip(catalog: &Catalog, predicate: &str, value: &str) -> Result<bool, String> {
    let first = ir_query(Some(predicate), value);
    let Some(core) = checked(catalog, &first) else {
        return Ok(false);
    };
    let (filters, stored) = filters_and_value(&core);
    // The filters are one `where` split at its top-level `and`s, outermost
    // last.
    let displayed_filter = IRExpr::and_all(filters.into_iter().rev()).map(|f| f.to_string());
    let displayed_value = stored.to_string();
    let second = ir_query(displayed_filter.as_deref(), &displayed_value);
    match checked(catalog, &second) {
        Some(again) if again == core => Ok(true),
        Some(again) => Err(format!(
            "the displayed text types to another declaration\n{first}\n{second}\n{core:#?}\n{again:#?}"
        )),
        None => Err(format!(
            "the displayed text is refused: {:?}\n{first}\n{second}",
            check_source(catalog, &second)
                .map(|d| d.into_iter().map(|(_, r)| r.err()).collect::<Vec<_>>())
        )),
    }
}

fn display_then_check(cases: u32) {
    let catalog = ir_catalog();
    let strategy = (
        typed(Dom::Bool, 3),
        proptest::sample::select(DOMAINS).prop_flat_map(|d| typed(d, 2)),
    );
    let mut runner = TestRunner::new(Config {
        cases,
        max_shrink_iters: 4096,
        failure_persistence: None,
        ..Config::default()
    });
    let accepted = std::cell::Cell::new(0u32);
    let result = runner.run(&strategy, |(predicate, value)| {
        if display_round_trip(&catalog, &predicate, &value).map_err(TestCaseError::fail)? {
            accepted.set(accepted.get() + 1);
        }
        Ok(())
    });
    if let Err(e) = result {
        panic!("{e}");
    }
    // Most generated sources type; a generator that drifted into refused
    // text would pass vacuously.
    assert!(
        accepted.get() * 2 > cases,
        "only {} of {cases} generated sources typed",
        accepted.get()
    );
}

#[test]
fn a_displayed_expression_types_to_the_expression() {
    display_then_check(300);
}

#[test]
#[ignore = "nightly: 20,000 generated expressions; the gate runs 300"]
fn a_displayed_expression_types_to_the_expression_at_nightly_breadth() {
    display_then_check(20_000);
}
