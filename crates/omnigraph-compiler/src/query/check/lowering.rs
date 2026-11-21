//! The release's lowering admission (the GQ 3 RFC, section
//! 5): a declaration the type checker accepts must lower. The shapers in
//! [`crate::shape`] are the one statement of what lowers; a term they do not
//! lower is refused here with `T86` at the construct it was written as,
//! naming the construct; the stage that adds it is the shaper's record
//! ([`crate::shape::ProgrammeStage`]), never the message's. A suggestion would have to write a lowered
//! form with the same rows and the same result columns; no term refused in
//! this release has one, so `T86` prints none.

use crate::catalog::Catalog;
use crate::ir::core::CoreDecl;
use crate::query::codes::{T86, T91};
use crate::query::diagnostic::QueryDiagnostic;
use crate::query::syntax::{
    Decl, DeclBody, Element, Expr, LetValue, Pattern, ReadBody, RelExpr, SourceSpan,
};
use crate::shape::read::ReadShape;
use crate::shape::write::WriteShape;
use crate::shape::{Construct, ElementKind, ShapeError, Unlowered};

use super::AtSpan;

/// `T86` for the first term of `core` that this release's planner does not
/// lower, or `T70` when shaping it passes its plan budget. A shaping defect
/// is not the author's refusal: the planner reports it when the declaration
/// runs.
pub(super) fn admit(
    catalog: &Catalog,
    decl: &Decl,
    core: &CoreDecl,
) -> Result<(), QueryDiagnostic> {
    if let CoreDecl::Write(write) = core
        && let Some(change) = crate::shape::write::earlier_group_change(write, catalog)
    {
        return Err(earlier_group_change(decl, &write.name, &change));
    }
    let shaped = match core {
        CoreDecl::Read(read) => ReadShape::of(read, catalog).map(|_| ()),
        CoreDecl::Write(write) => WriteShape::of(write, catalog).map(|_| ()),
    };
    match shaped {
        Err(ShapeError::Unlowered(u)) => Err(refusal(decl, &u)),
        Err(e @ ShapeError::OverBudget { .. }) => Err(e
            .over_budget()
            .expect("an over-budget shaping is a refusal")
            .at_span_of(&decl.at)),
        Ok(()) | Err(ShapeError::Internal(_)) => Ok(()),
    }
}

/// `T91`: a write group's selection reads a table an earlier group of the
/// write changes. Every group selects on the write's one snapshot, so the
/// group would not see that change; the refusal sits at the group's
/// `match`, and no single edit keeps the author's meaning, so it prints a
/// fix and no suggestion.
fn earlier_group_change(
    decl: &Decl,
    name: &str,
    change: &crate::shape::write::EarlierGroupChange,
) -> QueryDiagnostic {
    let (kind, type_name) = change
        .table
        .split_once(':')
        .unwrap_or(("node", change.table.as_str()));
    let kind = if kind == "edge" { "edge type" } else { "type" };
    let mut d = QueryDiagnostic::typecheck(
        T91,
        format!(
            "`{name}`: group {} selects on {kind} `{type_name}`, which an earlier group of the \
             write changes; every group of a write selects on the snapshot the write started \
             from, so this selection would not see that change",
            change.group + 1
        ),
    );
    d.fix = Some(
        "run the group in a second write after this one publishes, or select every row the \
         statements need in one group and write its statements after that one `match`"
            .to_string(),
    );
    let span = group_pattern(decl, change.group)
        .and_then(|p| p.spans.elements.first())
        .and_then(parsed);
    d.position = span.map(|s| s.position);
    d.end = span.map(|s| s.end_position);
    d
}

/// The `T86` refusal of `u`, at the construct it names in `decl`.
fn refusal(decl: &Decl, u: &Unlowered) -> QueryDiagnostic {
    let mut d = QueryDiagnostic::typecheck(T86, u.message());
    let span = construct_span(decl, u);
    d.position = span.map(|s| s.position);
    d.end = span.map(|s| s.end_position);
    d
}

/// The body or the write group's pattern `u` points into.
fn read_body(decl: &Decl) -> Option<&ReadBody> {
    match &decl.body {
        DeclBody::Read(body) => Some(body),
        DeclBody::Write(_) => None,
    }
}

fn group_pattern(decl: &Decl, group: usize) -> Option<&Pattern> {
    match &decl.body {
        DeclBody::Write(w) => w.groups.get(group).map(|g| &g.pattern),
        DeclBody::Read(_) => None,
    }
}

/// A span that was parsed; a node built rather than parsed has none.
fn parsed(span: &SourceSpan) -> Option<&SourceSpan> {
    (span.end > span.start).then_some(span)
}

/// Where the construct `u` names was written.
fn construct_span<'d>(decl: &'d Decl, u: &Unlowered) -> Option<&'d SourceSpan> {
    let pattern = match u.group {
        Some(g) => group_pattern(decl, g),
        None => read_body(decl).map(|b| b.pattern.as_ref()),
    };
    let whole = || pattern.and_then(|p| p.spans.elements.first().and_then(parsed));
    let found = match &u.construct {
        Construct::Read => None,
        Construct::Limit => read_body(decl).and_then(|b| b.spans.limit.as_ref()),
        Construct::OrderKey(k) => read_body(decl).and_then(|b| b.spans.order.get(*k)),
        Construct::ReturnItem(i) => read_body(decl).and_then(|b| b.spans.items.get(*i)),
        Construct::Element(kinds, name) => {
            pattern.and_then(|p| element_span(p, kinds, name.as_deref()))
        }
        Construct::Block => pattern.and_then(block_span),
        Construct::Group(g) => group_pattern(decl, *g)
            .and_then(|p| p.spans.elements.first())
            .or_else(|| statement_span(decl, *g, 0)),
        Construct::Statement(g, s) | Construct::Assignment(g, s, _) => statement_span(decl, *g, *s),
    };
    found
        .and_then(parsed)
        .or_else(whole)
        .or_else(|| read_body(decl).and_then(|b| b.spans.match_inner.as_ref().and_then(parsed)))
}

fn statement_span(decl: &Decl, group: usize, statement: usize) -> Option<&SourceSpan> {
    match &decl.body {
        DeclBody::Write(w) => w.groups.get(group)?.spans.statements.get(statement),
        DeclBody::Read(_) => None,
    }
}

/// The patterns an element holds: an `optional`'s, an inline read's.
fn nested(element: &Element) -> Vec<&Pattern> {
    let bodies: Vec<&ReadBody> = match element {
        Element::Optional(p) => return vec![p],
        Element::From {
            rel: RelExpr::Inline(body),
            ..
        }
        | Element::Let {
            value: LetValue::Relation(body),
            ..
        } => vec![body],
        Element::From {
            rel: RelExpr::Call(call),
            ..
        } => call
            .args
            .iter()
            .filter_map(|a| match a {
                crate::query::syntax::Arg::Inline(body) => Some(body.as_ref()),
                crate::query::syntax::Arg::Expr(_) => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    bodies.into_iter().map(|b| b.pattern.as_ref()).collect()
}

fn element_matches(element: &Element, kinds: &[ElementKind], name: Option<&str>) -> bool {
    let (kind, binds) = match element {
        Element::Let { name, .. } => (ElementKind::Let, Some(name.as_str())),
        Element::From { .. } => (ElementKind::From, None),
        Element::Optional(_) => (ElementKind::Optional, None),
        Element::Binding { variable, .. } => (ElementKind::Binding, Some(variable.as_str())),
        Element::Traversal(_) => return false,
    };
    kinds.contains(&kind) && (name.is_none() || binds.is_none() || binds == name)
}

/// The first element of `kinds` binding `name` in `pattern`, in written
/// order, an element before the patterns it holds.
fn element_span<'p>(
    pattern: &'p Pattern,
    kinds: &[ElementKind],
    name: Option<&str>,
) -> Option<&'p SourceSpan> {
    crate::stack::deep(|| {
        for (i, element) in pattern.elements.iter().enumerate() {
            if element_matches(element, kinds, name) {
                return pattern.spans.elements.get(i);
            }
            if let Some(span) = nested(element)
                .into_iter()
                .find_map(|p| element_span(p, kinds, name))
            {
                return Some(span);
            }
        }
        None
    })
}

/// The comparison a block is an operand of, in `pattern`'s `where` or a
/// pattern it holds; the `where` itself when no comparison holds one.
fn block_span(pattern: &Pattern) -> Option<&SourceSpan> {
    if let Some(predicate) = &pattern.predicate
        && let Some(span) = compared_block(predicate).or(pattern.spans.predicate.as_ref())
    {
        return Some(span);
    }
    pattern
        .elements
        .iter()
        .flat_map(nested)
        .find_map(block_span)
}

fn compared_block(e: &Expr) -> Option<&SourceSpan> {
    crate::stack::deep(|| match e {
        Expr::Compare {
            left, right, at, ..
        } => {
            if matches!(left.as_ref(), Expr::Block(_)) || matches!(right.as_ref(), Expr::Block(_)) {
                Some(at)
            } else {
                compared_block(left).or_else(|| compared_block(right))
            }
        }
        Expr::Not(inner) | Expr::Neg(inner) => compared_block(inner),
        Expr::And(l, r)
        | Expr::Or(l, r)
        | Expr::Arith {
            left: l, right: r, ..
        } => compared_block(l).or_else(|| compared_block(r)),
        _ => None,
    })
}
