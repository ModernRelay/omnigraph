//! The non-canonical forms lint names (the GQ 3 RFC,
//! section 6, "Canonical forms"), each with the canonical form as its fix.
//!
//! The list is the RFC's, and complete. Each canonical form has the typed IR
//! of the form it replaces:
//!
//! - `L203`: `count { P } > 0` is `exists { P }`;
//! - `L206`: a `union` nested as the first argument of a `union` is the flat
//!   one, since `union` folds its arms from the left;
//! - `L207`: the ends of `<e>` smaller name first, and the members of an
//!   alternation in name order;
//! - `L208`: `asc` dropped;
//! - `L209`: an alias equal to its item's column name dropped;
//! - `L210`: a call's options in the registry's order, those equal to their
//!   default dropped;
//! - `L211`: `bm25` over `terms(q, mode: any)` is `bm25` over the String `q`;
//! - `L212`: `case … end` is nested `if`, a missing `else` meaning `null`;
//! - `L213`: `a || b` is `concat(a, b)`;
//! - `L214`: `<>` is `!=`;
//! - `L215`: a single-quoted string is the double-quoted one;
//! - `L216`: a keyword or registry name in another letter case is the
//!   lowercase one.
//!
//! The parser records `L212` to `L216` as it lowers them
//! ([`crate::query::syntax::Respelling`]); the others are read from the tree.
//! Every form carries its fix as one byte-range edit of the source, which
//! `omnigraph lint --fix` applies.
//!
//! Two rewrites have the rows of their canonical form but another typed IR,
//! so they are the author's choice rather than canonical forms: `L204`,
//! `count { P } >= 1` for `exists { P }`, and `L205`, `count { P } = 0` for
//! `not exists { P }`.

use crate::query::check::{OptionDefault, REGISTRY_OPTIONS, item_name};
use crate::query::diagnostic::{Position, TextEdit};
use crate::query::syntax::print;
use crate::query::syntax::{
    Arg, Block, BlockFunc, Call, Decl, DeclBody, EdgeSelector, Element, Expr, LetValue, Pattern,
    ReadBody, RelExpr, SourceSpan, Stmt,
};
use crate::query::value::{AggFunc, CompOp, Literal};

/// One non-canonical form of a declaration: its code, where it was written
/// when the parse tree records it, the form as written and its fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalForm {
    pub code: &'static str,
    pub position: Option<Position>,
    pub written: String,
    pub fix: String,
    /// Whether the fix keeps the typed IR (a canonical form) or only the
    /// rows (`L204`, `L205`).
    pub same_ir: bool,
    /// The fix as a byte-range edit of the source; `None` for a tree built
    /// rather than parsed.
    pub edit: Option<TextEdit>,
}

/// Every non-canonical form of `decl`, in written order.
pub(crate) fn canonical_forms(decl: &Decl) -> Vec<CanonicalForm> {
    let mut out = Vec::new();
    for r in &decl.respellings.0 {
        out.push(CanonicalForm {
            code: r.code,
            position: Some(r.position),
            written: r.written.clone(),
            fix: r.fix.clone(),
            same_ir: true,
            edit: Some(TextEdit {
                start: r.start,
                end: r.end,
                replacement: r.fix.clone(),
            }),
        });
    }
    match &decl.body {
        DeclBody::Read(body) => read_body(body, &mut out),
        DeclBody::Write(body) => {
            for group in &body.groups {
                pattern(&group.pattern, &mut out);
                for stmt in &group.statements {
                    if let Stmt::Insert { assignments, .. } | Stmt::Update { assignments, .. } =
                        stmt
                    {
                        for a in assignments {
                            expr(&a.value, None, &mut out);
                        }
                    }
                }
            }
        }
    }
    out.sort_by_key(|f| {
        f.edit
            .as_ref()
            .map_or((usize::MAX, 0), |e| (e.start, e.end))
    });
    out
}

/// Every non-canonical form of every declaration of `source`, each with the
/// name of the declaration it stands in, in declaration order; nothing for a
/// source that does not parse.
pub(crate) fn source_canonical_forms(source: &str) -> Vec<(String, CanonicalForm)> {
    use crate::query::syntax::Body;
    let Ok(file) = crate::query::syntax::parse::parse_source(source) else {
        return Vec::new();
    };
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain { before, decl } => before.iter().chain([decl.as_ref()]).collect(),
        Body::Statement(_) => Vec::new(),
    };
    decls
        .into_iter()
        .flat_map(|decl| {
            canonical_forms(decl)
                .into_iter()
                .map(|form| (decl.name.clone(), form))
        })
        .collect()
}

/// The byte range a parsed node was written at; `None` for a node built
/// rather than parsed.
fn range(at: &SourceSpan) -> Option<(usize, usize)> {
    (at.end > at.start).then_some((at.start, at.end))
}

fn push(
    out: &mut Vec<CanonicalForm>,
    code: &'static str,
    position: Option<Position>,
    written: String,
    fix: String,
    same_ir: bool,
    at: Option<(usize, usize)>,
) {
    let edit = at.map(|(start, end)| TextEdit {
        start,
        end,
        replacement: fix.clone(),
    });
    out.push(CanonicalForm {
        code,
        position,
        written,
        fix,
        same_ir,
        edit,
    });
}

fn read_body(body: &ReadBody, out: &mut Vec<CanonicalForm>) {
    pattern(&body.pattern, out);
    for (i, p) in body.projections.iter().enumerate() {
        let at = body.spans.items.get(i).map(|s| s.position);
        expr(&p.expr, at, out);
        if let Some(alias) = &p.alias
            && *alias == item_name(&p.expr, None)
        {
            push(
                out,
                "L209",
                at,
                format!("{} as {alias}", print::expr(&p.expr)),
                print::expr(&p.expr),
                true,
                body.spans.projections.get(i).and_then(range),
            );
        }
    }
    for (i, o) in body.order.iter().enumerate() {
        let at = body.spans.order.get(i).map(|s| s.position);
        expr(&o.expr, at, out);
        if o.descending == Some(false) {
            let nulls = match o.nulls_first {
                None => "",
                Some(true) => " nulls first",
                Some(false) => " nulls last",
            };
            push(
                out,
                "L208",
                at,
                format!("{} asc{nulls}", print::expr(&o.expr)),
                format!("{}{nulls}", print::expr(&o.expr)),
                true,
                body.spans.order.get(i).and_then(range),
            );
        }
    }
}

fn pattern(p: &Pattern, out: &mut Vec<CanonicalForm>) {
    for (i, e) in p.elements.iter().enumerate() {
        let span = p.spans.elements.get(i);
        element(e, span.map(|s| s.position), span.and_then(range), out);
    }
    if let Some(w) = &p.predicate {
        expr(w, p.spans.predicate.as_ref().map(|s| s.position), out);
    }
}

fn element(
    e: &Element,
    at: Option<Position>,
    written_at: Option<(usize, usize)>,
    out: &mut Vec<CanonicalForm>,
) {
    crate::stack::deep(|| match e {
        Element::Let { value, .. } => match value {
            LetValue::Relation(body) => read_body(body, out),
            LetValue::Expr(e) => expr(e, at, out),
        },
        Element::From { rel, .. } => match rel {
            RelExpr::Inline(body) => read_body(body, out),
            RelExpr::Call(c) => call(c, at, out),
            RelExpr::Variable(_) => {}
        },
        Element::Optional(p) => pattern(p, out),
        Element::Binding { .. } => {}
        Element::Traversal(t) => {
            let mut canonical = t.clone();
            if canonical.undirected && canonical.dst < canonical.src {
                std::mem::swap(&mut canonical.src, &mut canonical.dst);
            }
            if let EdgeSelector::Alternation(names) = &mut canonical.selector {
                names.sort();
            }
            if canonical != *t {
                push(
                    out,
                    "L207",
                    at,
                    print::element(e),
                    print::element(&Element::Traversal(canonical)),
                    true,
                    written_at,
                );
            }
        }
    })
}

fn expr(e: &Expr, at: Option<Position>, out: &mut Vec<CanonicalForm>) {
    crate::stack::deep(|| {
        // A block's own position is more exact than its enclosing clause's.
        if let Expr::Compare {
            op,
            left,
            right,
            at: compare_at,
        } = e
            && let (Expr::Block(b), Expr::Literal(Literal::Integer(n))) =
                (left.as_ref(), right.as_ref())
            && b.func == BlockFunc::Agg(AggFunc::Count)
            && b.arg.is_none()
        {
            let exists = || {
                let mut block: Block = b.as_ref().clone();
                block.func = BlockFunc::Exists;
                print::expr(&Expr::Block(Box::new(block)))
            };
            let found = match (op, n) {
                (CompOp::Gt, 0) => Some(("L203", exists(), true)),
                (CompOp::Ge, 1) => Some(("L204", exists(), false)),
                (CompOp::Eq, 0) => Some(("L205", format!("not {}", exists()), false)),
                _ => None,
            };
            if let Some((code, fix, same_ir)) = found {
                push(
                    out,
                    code,
                    Some(b.at.position),
                    print::expr(e),
                    fix,
                    same_ir,
                    range(compare_at),
                );
            }
        }
        match e {
            Expr::Literal(_)
            | Expr::Variable(_)
            | Expr::List { .. }
            | Expr::Prop { .. }
            | Expr::Meta(_)
            | Expr::Ident(_) => {}
            Expr::Call(c) => call(c, at, out),
            Expr::AggDistinct { arg, fraction, .. } => {
                expr(arg, at, out);
                if let Some(p) = fraction {
                    expr(p, at, out);
                }
            }
            Expr::Block(b) => {
                for a in b.arg.iter().chain(&b.fraction) {
                    expr(a, Some(b.at.position), out);
                }
                pattern(&b.pattern, out);
            }
            Expr::Neg(inner) | Expr::Not(inner) => expr(inner, at, out),
            Expr::IsNull { expr: inner, .. } => expr(inner, at, out),
            Expr::Arith { left, right, .. }
            | Expr::Compare { left, right, .. }
            | Expr::InfixString { left, right, .. } => {
                expr(left, at, out);
                expr(right, at, out);
            }
            Expr::In { needle, list, .. } => {
                expr(needle, at, out);
                expr(list, at, out);
            }
            Expr::And(l, r) | Expr::Or(l, r) => {
                expr(l, at, out);
                expr(r, at, out);
            }
        }
    })
}

fn call(c: &Call, at: Option<Position>, out: &mut Vec<CanonicalForm>) {
    crate::stack::deep(|| {
        // `union` folds its arms from the left, so a `union` nested as its first
        // argument is the flat one.
        if c.name == "union"
            && c.options.is_empty()
            && matches!(c.args.first(), Some(Arg::Expr(Expr::Call(inner))) if inner.name == "union" && inner.options.is_empty())
        {
            let mut flat = c.clone();
            while let Some(Arg::Expr(Expr::Call(inner))) = flat.args.first()
                && inner.name == "union"
                && inner.options.is_empty()
            {
                let inner_args = inner.args.clone();
                flat.args.splice(0..1, inner_args);
            }
            push(
                out,
                "L206",
                at,
                print::call(c),
                print::call(&flat),
                true,
                range(&c.at),
            );
        }
        let ordered = canonical_options(c);
        if ordered.options != c.options {
            push(
                out,
                "L210",
                at,
                print::call(c),
                print::call(&ordered),
                true,
                range(&c.at),
            );
        }
        // `bm25` over `terms(q, mode: any)` is `bm25` over the String `q`.
        if c.name == "bm25"
            && let Some(Arg::Expr(Expr::Call(t))) = c.args.get(2)
            && t.name == "terms"
            && let [Arg::Expr(text)] = t.args.as_slice()
            && matches!(t.options.as_slice(), [(o, Expr::Ident(m))] if o == "mode" && m == "any")
        {
            let mut over_string = c.clone();
            over_string.args[2] = Arg::Expr(text.clone());
            push(
                out,
                "L211",
                at,
                print::call(c),
                print::call(&over_string),
                true,
                range(&c.at),
            );
        }
        for a in &c.args {
            match a {
                Arg::Expr(e) => expr(e, at, out),
                Arg::Inline(body) => read_body(body, out),
            }
        }
        for (_, v) in &c.options {
            expr(v, at, out);
        }
    })
}

/// `c` with its options in the registry's order and those equal to their
/// default dropped; an option the registry does not list stays after them,
/// in the order written.
fn canonical_options(c: &Call) -> Call {
    let mut c = c.clone();
    let Some((_, row)) = REGISTRY_OPTIONS.iter().find(|(name, _)| *name == c.name) else {
        return c;
    };
    let positional = c.args.len();
    c.options.retain(|(name, value)| {
        let default = row
            .iter()
            .find(|(n, _)| n == name)
            .map_or(OptionDefault::None, |(_, d)| *d);
        !is_default(default, value, positional)
    });
    let rank = |name: &str| {
        row.iter()
            .position(|(n, _)| *n == name)
            .unwrap_or(row.len())
    };
    c.options.sort_by_key(|(name, _)| rank(name));
    c
}

fn is_default(default: OptionDefault, value: &Expr, positional: usize) -> bool {
    match (default, value) {
        (OptionDefault::Integer(d), Expr::Literal(Literal::Integer(v))) => d == *v,
        (OptionDefault::Word(d), Expr::Ident(v)) => d == v,
        (OptionDefault::OnePerArgument, Expr::Literal(Literal::List(items))) => {
            items.len() == positional
                && items.iter().all(|i| match i {
                    Literal::Integer(1) => true,
                    Literal::Float(f) => *f == 1.0,
                    _ => false,
                })
        }
        _ => false,
    }
}
