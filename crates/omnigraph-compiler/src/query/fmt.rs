//! `queries fmt`: the canonical spelling of a GQ source
//! (docs/rfcs/2026-11-07-gq-3.md, section 6).
//!
//! The formatter canonicalises the forms the grammar cannot forbid, and
//! nothing else, without changing the typed IR:
//!
//! - `count { P } > 0` is written `exists { P }`;
//! - a `union` nested as the first argument of a `union` is written flat;
//! - the ends of `<e>` are written smaller name first, and the members of
//!   an alternation in name order;
//! - `asc` is dropped;
//! - an alias equal to its item's column name is dropped;
//! - a call's options are written in the registry's order, and one equal to
//!   its default is dropped;
//! - `bm25` over `terms(q, mode: any)` is written over the String `q`;
//! - `search`, `match_text` and `fuzzy` are written
//!   `match_terms(f, terms(q, mode: any[, max_edits: n]))`, `fuzzy` with no
//!   budget being two edits, where a schema is supplied and the rewrite
//!   leaves the typed IR unchanged, which holds over a field whose index
//!   declares an analyzer.
//!
//! It prints a short `match` on one line and a long one an element per line
//! with a trailing comma; blocks, `optional`s, inline reads, `return`,
//! `order` and assignments follow the same rule. Comments between
//! declarations are kept; a declaration with a comment inside it is kept as
//! written, since the parse tree holds no comment to print.

use pest::Parser as _;

use crate::catalog::Catalog;
use crate::ir::core::CoreDecl;
use crate::query::check::{OptionDefault, REGISTRY_OPTIONS, check_file, item_name};
use crate::query::diagnostic::QueryDiagnostic;
use crate::query::syntax::parse::parse_source;
use crate::query::syntax::print::{self, Prec, prec};
use crate::query::syntax::{
    Arg, Assignment, Block, BlockFunc, Body, Call, Decl, DeclBody, EdgeSelector, Element, Expr,
    LetValue, Pattern, QueryParser, ReadBody, RelExpr, Rule, SourceFile, Stmt, WriteBody,
};
use crate::query::value::{AggFunc, CompOp, Literal};

/// The widest line a construct is printed on before it breaks.
const WIDTH: usize = 80;
/// One level of indentation.
const STEP: usize = 2;

/// The formatted source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Formatted {
    /// The canonical text.
    pub text: String,
    /// Whether it differs from the input.
    pub changed: bool,
    /// Declarations kept as written because a comment stands inside them,
    /// by name.
    pub kept: Vec<String>,
}

/// Format `source`. A source that does not parse is refused with its
/// diagnostic; a statement (`branch`, `commit`, `load`, `show`) is returned
/// as written. With `catalog`, the older lexical calls are written as
/// `match_terms` wherever that leaves every declaration's typed IR unchanged.
pub fn format_source(
    source: &str,
    catalog: Option<&Catalog>,
) -> Result<Formatted, QueryDiagnostic> {
    crate::query::depth::check_query_nesting(source)?;
    let file = parse_source(source)?;
    let unchanged = || Formatted {
        text: source.to_string(),
        changed: false,
        kept: Vec::new(),
    };
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain { before, decl } => before.iter().chain([decl.as_ref()]).collect(),
        Body::Statement(_) => return Ok(unchanged()),
    };
    let Some(items) = top_level_items(source) else {
        return Ok(unchanged());
    };
    let canonical = canonical_file(&file, catalog);
    let canonical_decls: Vec<&Decl> = match &canonical.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain { before, decl } => before.iter().chain([decl.as_ref()]).collect(),
        Body::Statement(_) => unreachable!("a statement body is returned above"),
    };
    debug_assert_eq!(decls.len(), canonical_decls.len());

    let mut out = String::new();
    let mut kept = Vec::new();
    let mut previous_end = 0usize;
    let mut decl_index = 0usize;
    let mut previous: Option<ItemKind> = None;
    for item in &items {
        let gap = &source[previous_end..item.start];
        let separate = !(previous == Some(ItemKind::Setting) && item.kind == ItemKind::Setting);
        push_gap(&mut out, gap, previous.is_none(), separate);
        let text = &source[item.start..item.end];
        match item.kind {
            ItemKind::Setting => out.push_str(text.trim()),
            ItemKind::Decl | ItemKind::Explain => {
                let decl = canonical_decls[decl_index];
                let original = decls[decl_index];
                decl_index += 1;
                if !comments(text).is_empty() {
                    kept.push(original.name.clone());
                    out.push_str(text.trim());
                } else {
                    if item.kind == ItemKind::Explain {
                        out.push_str("explain ");
                    }
                    out.push_str(&layout_decl(decl));
                }
            }
        }
        previous_end = item.end;
        previous = Some(item.kind);
    }
    push_trailer(&mut out, &source[previous_end..]);
    let changed = out != source;
    Ok(Formatted {
        text: out,
        changed,
        kept,
    })
}

// ---------------------------------------------------------------------------
// The text around the declarations.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemKind {
    Setting,
    Decl,
    Explain,
}

struct Item {
    kind: ItemKind,
    start: usize,
    end: usize,
}

/// The settings lines, declarations and `explain` of a source with their
/// byte ranges; `None` for a statement source.
fn top_level_items(source: &str) -> Option<Vec<Item>> {
    let file = QueryParser::parse(Rule::query_file, source).ok()?.next()?;
    let mut items = Vec::new();
    for pair in file.into_inner() {
        let span = pair.as_span();
        let kind = match pair.as_rule() {
            Rule::setting_stmt => ItemKind::Setting,
            Rule::declaration => ItemKind::Decl,
            Rule::explain_stmt => ItemKind::Explain,
            Rule::EOI => continue,
            _ => return None,
        };
        items.push(Item {
            kind,
            start: span.start(),
            end: span.end(),
        });
    }
    Some(items)
}

/// The byte ranges of the comments of `text`, outside string literals.
fn comments(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                let start = i;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                out.push((start, i));
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let start = i;
                i += 2;
                while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
                out.push((start, i));
            }
            _ => i += 1,
        }
    }
    out
}

/// The text between two items: its comments kept, one blank line at most
/// between paragraphs, and a blank line between two items when `separate`
/// or when comments stand between them. A comment on the line an item ends
/// stays on that line.
fn push_gap(out: &mut String, gap: &str, first: bool, separate: bool) {
    let mut rest = gap;
    if !first {
        let line_end = gap.find('\n').unwrap_or(gap.len());
        let same_line = gap[..line_end].trim();
        if !same_line.is_empty() {
            out.push(' ');
            out.push_str(same_line);
        }
        rest = &gap[line_end..];
        out.push('\n');
    }
    let lines = paragraph_lines(rest);
    if lines.is_empty() {
        if !first && separate {
            out.push('\n');
        }
        return;
    }
    if !first {
        out.push('\n');
    }
    for line in &lines {
        out.push_str(line);
        out.push('\n');
    }
    // A blank line between the comments and the item when one was written.
    let tail = &rest[rest.trim_end().len()..];
    if tail.matches('\n').count() >= 2 {
        out.push('\n');
    }
}

/// The lines of a gap, trailing spaces trimmed, a comment line's
/// indentation dropped, runs of blank lines collapsed to one and blank
/// lines at either end removed.
fn paragraph_lines(text: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    let mut in_block = false;
    for raw in text.lines() {
        let line = raw.trim_end();
        let shown = if in_block { line } else { line.trim_start() };
        if shown.is_empty() {
            if lines.last().is_some_and(|l| !l.is_empty()) {
                lines.push(String::new());
            }
            continue;
        }
        if !in_block && shown.starts_with("/*") {
            in_block = !shown.contains("*/");
        } else if in_block && shown.contains("*/") {
            in_block = false;
        }
        lines.push(shown.to_string());
    }
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

/// The text after the last item: its comments, then one newline.
fn push_trailer(out: &mut String, trailer: &str) {
    let line_end = trailer.find('\n').unwrap_or(trailer.len());
    let same_line = trailer[..line_end].trim();
    if !same_line.is_empty() {
        out.push(' ');
        out.push_str(same_line);
    }
    let lines = paragraph_lines(&trailer[line_end..]);
    out.push('\n');
    if !lines.is_empty() {
        out.push('\n');
        for line in lines {
            out.push_str(&line);
            out.push('\n');
        }
    }
}

// ---------------------------------------------------------------------------
// The canonical tree.

/// `file` with every declaration canonical: the syntactic rewrites, then,
/// with a catalog, each older lexical call that `match_terms` writes
/// without changing the typed IR of any declaration.
fn canonical_file(file: &SourceFile, catalog: Option<&Catalog>) -> SourceFile {
    let mut file = file.clone();
    for_each_decl(&mut file, canonical_decl);
    if let Some(catalog) = catalog {
        file = lexical_rewrites(catalog, &file).0;
    }
    file
}

/// An older lexical call `match_terms` writes: the declaration, the call as
/// written and the call `queries fmt` writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexicalRewrite {
    pub decl: String,
    pub from: String,
    pub to: String,
}

/// `file` with each older lexical call written as `match_terms` wherever
/// that leaves the typed IR of every declaration unchanged, which holds
/// over a field whose index declares an analyzer, and the rewrites made.
pub(crate) fn lexical_rewrites(
    catalog: &Catalog,
    file: &SourceFile,
) -> (SourceFile, Vec<LexicalRewrite>) {
    let mut file = file.clone();
    let mut made = Vec::new();
    let before = typed(catalog, &file);
    let decl_count = decls_mut(&mut file).len();
    for index in 0..decl_count {
        let mut site = 0;
        loop {
            let mut candidate = file.clone();
            let (name, rewrote) = {
                let decl = &mut decls_mut(&mut candidate)[index];
                (decl.name.clone(), rewrite_lexical(decl, site))
            };
            let Some((from, to)) = rewrote else {
                break;
            };
            if typed(catalog, &candidate) == before {
                file = candidate;
                made.push(LexicalRewrite {
                    decl: name,
                    from: print::call(&from),
                    to: print::call(&to),
                });
                // The call at `site` is gone; the next one took its place.
            } else {
                site += 1;
            }
        }
    }
    (file, made)
}

/// Each declaration's typed IR, or its refusal's code.
fn typed(catalog: &Catalog, file: &SourceFile) -> Vec<Result<CoreDecl, String>> {
    check_file(catalog, file)
        .into_iter()
        .map(|(_, r)| r.map(|c| c.core).map_err(|e| e.code.as_str().to_string()))
        .collect()
}

fn decls_mut(file: &mut SourceFile) -> Vec<&mut Decl> {
    match &mut file.body {
        Body::Decls(d) => d.iter_mut().collect(),
        Body::Explain { before, decl } => before
            .iter_mut()
            .chain(std::iter::once(decl.as_mut()))
            .collect(),
        Body::Statement(_) => Vec::new(),
    }
}

fn for_each_decl(file: &mut SourceFile, mut f: impl FnMut(&mut Decl)) {
    for d in decls_mut(file) {
        f(d);
    }
}

/// The syntactic rewrites of one declaration.
pub(crate) fn canonical_decl(decl: &mut Decl) {
    match &mut decl.body {
        DeclBody::Read(body) => canonical_body(body),
        DeclBody::Write(body) => canonical_write(body),
    }
}

fn canonical_write(body: &mut WriteBody) {
    for group in &mut body.groups {
        canonical_pattern(&mut group.pattern);
        for stmt in &mut group.statements {
            match stmt {
                Stmt::Insert { assignments, .. } | Stmt::Update { assignments, .. } => {
                    for a in assignments {
                        canonical_expr(&mut a.value);
                    }
                }
                Stmt::Delete { .. } => {}
            }
        }
    }
}

fn canonical_body(body: &mut ReadBody) {
    canonical_pattern(&mut body.pattern);
    for p in &mut body.projections {
        canonical_expr(&mut p.expr);
        if p.alias.as_deref() == Some(item_name(&p.expr, None).as_str()) {
            p.alias = None;
        }
    }
    for o in &mut body.order {
        canonical_expr(&mut o.expr);
        if o.descending == Some(false) {
            o.descending = None;
        }
    }
}

fn canonical_pattern(p: &mut Pattern) {
    for e in &mut p.elements {
        canonical_element(e);
    }
    if let Some(w) = &mut p.predicate {
        canonical_expr(w);
    }
}

fn canonical_element(e: &mut Element) {
    match e {
        Element::Let { value, .. } => match value {
            LetValue::Relation(body) => canonical_body(body),
            LetValue::Expr(e) => canonical_expr(e),
        },
        Element::From { rel, .. } => match rel {
            RelExpr::Inline(body) => canonical_body(body),
            RelExpr::Call(c) => canonical_call(c),
            RelExpr::Variable(_) => {}
        },
        Element::Optional(p) => canonical_pattern(p),
        Element::Binding { .. } => {}
        Element::Traversal(t) => {
            if t.undirected && t.dst < t.src {
                std::mem::swap(&mut t.src, &mut t.dst);
            }
            if let EdgeSelector::Alternation(names) = &mut t.selector {
                names.sort();
            }
        }
    }
}

fn canonical_expr(e: &mut Expr) {
    match e {
        Expr::Literal(_)
        | Expr::Variable(_)
        | Expr::Prop { .. }
        | Expr::Meta(_)
        | Expr::Ident(_) => {}
        Expr::Call(c) => canonical_call(c),
        Expr::AggDistinct { arg, .. } => canonical_expr(arg),
        Expr::Block(b) => {
            if let Some(a) = &mut b.arg {
                canonical_expr(a);
            }
            canonical_pattern(&mut b.pattern);
        }
        Expr::Neg(inner) | Expr::Not(inner) => canonical_expr(inner),
        Expr::IsNull { expr, .. } => canonical_expr(expr),
        Expr::Arith { left, right, .. }
        | Expr::Compare { left, right, .. }
        | Expr::InfixString { left, right, .. } => {
            canonical_expr(left);
            canonical_expr(right);
        }
        Expr::In { needle, list, .. } => {
            canonical_expr(needle);
            canonical_expr(list);
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            canonical_expr(l);
            canonical_expr(r);
        }
    }
    // `count { P } > 0` is `exists { P }`.
    if let Expr::Compare {
        op: CompOp::Gt,
        left,
        right,
        ..
    } = e
        && matches!(right.as_ref(), Expr::Literal(Literal::Integer(0)))
        && let Expr::Block(b) = left.as_ref()
        && b.func == BlockFunc::Agg(AggFunc::Count)
        && b.arg.is_none()
    {
        let mut block: Block = b.as_ref().clone();
        block.func = BlockFunc::Exists;
        *e = Expr::Block(Box::new(block));
    }
}

fn canonical_call(c: &mut Call) {
    for a in &mut c.args {
        match a {
            Arg::Expr(e) => canonical_expr(e),
            Arg::Inline(body) => canonical_body(body),
        }
    }
    for (_, v) in &mut c.options {
        canonical_expr(v);
    }
    // A `union` nested as the first argument of a `union` is the flat one:
    // `union` folds its arms from the left.
    if c.name == "union" {
        while let Some(Arg::Expr(Expr::Call(inner))) = c.args.first()
            && inner.name == "union"
            && inner.options.is_empty()
            && c.options.is_empty()
        {
            let Some(Arg::Expr(Expr::Call(inner))) = c.args.first().cloned() else {
                unreachable!()
            };
            c.args.splice(0..1, inner.args);
        }
    }
    canonical_options(c);
    // `bm25` over `terms(q, mode: any)` is `bm25` over the String `q`.
    if c.name == "bm25"
        && let Some(Arg::Expr(Expr::Call(t))) = c.args.get_mut(2)
        && t.name == "terms"
        && matches!(t.args.as_slice(), [Arg::Expr(_)])
        && matches!(t.options.as_slice(), [(o, Expr::Ident(m))] if o == "mode" && m == "any")
    {
        let Some(Arg::Expr(text)) = t.args.pop() else {
            unreachable!()
        };
        c.args[2] = Arg::Expr(text);
    }
}

/// A call's options in the registry's order, those equal to their default
/// dropped; an option the registry does not list stays after them, in the
/// order written.
fn canonical_options(c: &mut Call) {
    let Some((_, row)) = REGISTRY_OPTIONS.iter().find(|(name, _)| *name == c.name) else {
        return;
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

/// Rewrite the `target`-th older lexical call of `decl`, in walk order, to
/// `match_terms`: the call as written and as rewritten, when there is one.
fn rewrite_lexical(decl: &mut Decl, target: usize) -> Option<(Call, Call)> {
    let mut done = None;
    let mut n = 0;
    let mut visit = |c: &mut Call| {
        if done.is_some() || !matches!(c.name.as_str(), "search" | "match_text" | "fuzzy") {
            return;
        }
        if c.args.len() != 2 || c.args.iter().any(|a| matches!(a, Arg::Inline(_))) {
            return;
        }
        if n == target {
            let to = match_terms_of(c);
            done = Some((std::mem::replace(c, to.clone()), to));
        }
        n += 1;
    };
    visit_decl_calls(decl, &mut visit);
    done
}

/// `match_terms(f, terms(q, mode: any[, max_edits: n]))` for an older
/// lexical call: `fuzzy`'s budget, two when unwritten, zero dropped as the
/// default of `terms`.
fn match_terms_of(c: &Call) -> Call {
    let mut args = c.args.clone();
    let query = args.pop().expect("two arguments");
    let Arg::Expr(query) = query else {
        unreachable!("an inline argument is excluded")
    };
    let mut options = vec![("mode".to_string(), Expr::Ident("any".into()))];
    if c.name == "fuzzy" {
        let budget = c
            .options
            .iter()
            .find(|(o, _)| o == "max_edits")
            .map_or(Expr::Literal(Literal::Integer(2)), |(_, v)| v.clone());
        if !matches!(budget, Expr::Literal(Literal::Integer(0))) {
            options.push(("max_edits".to_string(), budget));
        }
    }
    args.push(Arg::Expr(Expr::Call(Call {
        name: "terms".into(),
        args: vec![Arg::Expr(query)],
        options,
    })));
    Call {
        name: "match_terms".into(),
        args,
        options: Vec::new(),
    }
}

/// Every call of `decl`, outermost first, in written order.
pub(crate) fn visit_decl_calls(decl: &mut Decl, f: &mut dyn FnMut(&mut Call)) {
    match &mut decl.body {
        DeclBody::Read(body) => visit_body_calls(body, f),
        DeclBody::Write(body) => {
            for group in &mut body.groups {
                visit_pattern_calls(&mut group.pattern, f);
                for stmt in &mut group.statements {
                    if let Stmt::Insert { assignments, .. } | Stmt::Update { assignments, .. } =
                        stmt
                    {
                        for a in assignments {
                            visit_expr_calls(&mut a.value, f);
                        }
                    }
                }
            }
        }
    }
}

fn visit_body_calls(body: &mut ReadBody, f: &mut dyn FnMut(&mut Call)) {
    visit_pattern_calls(&mut body.pattern, f);
    for p in &mut body.projections {
        visit_expr_calls(&mut p.expr, f);
    }
    for o in &mut body.order {
        visit_expr_calls(&mut o.expr, f);
    }
}

fn visit_pattern_calls(p: &mut Pattern, f: &mut dyn FnMut(&mut Call)) {
    for e in &mut p.elements {
        match e {
            Element::Let { value, .. } => match value {
                LetValue::Relation(body) => visit_body_calls(body, f),
                LetValue::Expr(e) => visit_expr_calls(e, f),
            },
            Element::From { rel, .. } => match rel {
                RelExpr::Inline(body) => visit_body_calls(body, f),
                RelExpr::Call(c) => visit_call(c, f),
                RelExpr::Variable(_) => {}
            },
            Element::Optional(p) => visit_pattern_calls(p, f),
            Element::Binding { .. } | Element::Traversal(_) => {}
        }
    }
    if let Some(w) = &mut p.predicate {
        visit_expr_calls(w, f);
    }
}

fn visit_call(c: &mut Call, f: &mut dyn FnMut(&mut Call)) {
    f(c);
    for a in &mut c.args {
        match a {
            Arg::Expr(e) => visit_expr_calls(e, f),
            Arg::Inline(body) => visit_body_calls(body, f),
        }
    }
    for (_, v) in &mut c.options {
        visit_expr_calls(v, f);
    }
}

fn visit_expr_calls(e: &mut Expr, f: &mut dyn FnMut(&mut Call)) {
    match e {
        Expr::Literal(_)
        | Expr::Variable(_)
        | Expr::Prop { .. }
        | Expr::Meta(_)
        | Expr::Ident(_) => {}
        Expr::Call(c) => visit_call(c, f),
        Expr::AggDistinct { arg, .. } => visit_expr_calls(arg, f),
        Expr::Block(b) => {
            if let Some(a) = &mut b.arg {
                visit_expr_calls(a, f);
            }
            visit_pattern_calls(&mut b.pattern, f);
        }
        Expr::Neg(inner) | Expr::Not(inner) => visit_expr_calls(inner, f),
        Expr::IsNull { expr, .. } => visit_expr_calls(expr, f),
        Expr::Arith { left, right, .. }
        | Expr::Compare { left, right, .. }
        | Expr::InfixString { left, right, .. } => {
            visit_expr_calls(left, f);
            visit_expr_calls(right, f);
        }
        Expr::In { needle, list, .. } => {
            visit_expr_calls(needle, f);
            visit_expr_calls(list, f);
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            visit_expr_calls(l, f);
            visit_expr_calls(r, f);
        }
    }
}

/// Every expression of `decl`, each before the expressions inside it, in
/// written order.
pub(crate) fn visit_decl_exprs(decl: &Decl, f: &mut dyn FnMut(&Expr)) {
    match &decl.body {
        DeclBody::Read(body) => visit_body_exprs(body, f),
        DeclBody::Write(body) => {
            for group in &body.groups {
                visit_pattern_exprs(&group.pattern, f);
                for stmt in &group.statements {
                    if let Stmt::Insert { assignments, .. } | Stmt::Update { assignments, .. } =
                        stmt
                    {
                        for a in assignments {
                            visit_expr(&a.value, f);
                        }
                    }
                }
            }
        }
    }
}

fn visit_body_exprs(body: &ReadBody, f: &mut dyn FnMut(&Expr)) {
    visit_pattern_exprs(&body.pattern, f);
    for p in &body.projections {
        visit_expr(&p.expr, f);
    }
    for o in &body.order {
        visit_expr(&o.expr, f);
    }
}

fn visit_pattern_exprs(p: &Pattern, f: &mut dyn FnMut(&Expr)) {
    for e in &p.elements {
        match e {
            Element::Let { value, .. } => match value {
                LetValue::Relation(body) => visit_body_exprs(body, f),
                LetValue::Expr(e) => visit_expr(e, f),
            },
            Element::From { rel, .. } => match rel {
                RelExpr::Inline(body) => visit_body_exprs(body, f),
                RelExpr::Call(c) => visit_call_exprs(c, f),
                RelExpr::Variable(_) => {}
            },
            Element::Optional(p) => visit_pattern_exprs(p, f),
            Element::Binding { .. } | Element::Traversal(_) => {}
        }
    }
    if let Some(w) = &p.predicate {
        visit_expr(w, f);
    }
}

fn visit_call_exprs(c: &Call, f: &mut dyn FnMut(&Expr)) {
    for a in &c.args {
        match a {
            Arg::Expr(e) => visit_expr(e, f),
            Arg::Inline(body) => visit_body_exprs(body, f),
        }
    }
    for (_, v) in &c.options {
        visit_expr(v, f);
    }
}

fn visit_expr(e: &Expr, f: &mut dyn FnMut(&Expr)) {
    f(e);
    match e {
        Expr::Literal(_)
        | Expr::Variable(_)
        | Expr::Prop { .. }
        | Expr::Meta(_)
        | Expr::Ident(_) => {}
        Expr::Call(c) => visit_call_exprs(c, f),
        Expr::AggDistinct { arg, .. } => visit_expr(arg, f),
        Expr::Block(b) => {
            if let Some(a) = &b.arg {
                visit_expr(a, f);
            }
            visit_pattern_exprs(&b.pattern, f);
        }
        Expr::Neg(inner) | Expr::Not(inner) => visit_expr(inner, f),
        Expr::IsNull { expr, .. } => visit_expr(expr, f),
        Expr::Arith { left, right, .. }
        | Expr::Compare { left, right, .. }
        | Expr::InfixString { left, right, .. } => {
            visit_expr(left, f);
            visit_expr(right, f);
        }
        Expr::In { needle, list, .. } => {
            visit_expr(needle, f);
            visit_expr(list, f);
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            visit_expr(l, f);
            visit_expr(r, f);
        }
    }
}

// ---------------------------------------------------------------------------
// The layout.

fn width(s: &str) -> usize {
    s.chars().count()
}

/// Whether `s` is one line that fits when it starts at column `col`.
fn fits(col: usize, s: &str) -> bool {
    !s.contains('\n') && col + width(s) <= WIDTH
}

/// The width of the last line of `s`.
fn last_line(s: &str) -> usize {
    width(s.rsplit('\n').next().unwrap_or(s))
}

fn pad(indent: usize) -> String {
    " ".repeat(indent)
}

/// `open`, the items, `close`: on one line when it fits from `col`,
/// otherwise one item per line at `indent + STEP`, each followed by
/// `separator` when `trailing` (the last one too) or between items
/// otherwise, `close` at `indent`. `items` lays each item out at an
/// indentation and a column.
fn list(
    open: &str,
    close: &str,
    indent: usize,
    col: usize,
    count: usize,
    trailing: bool,
    item: &dyn Fn(usize, usize, usize) -> String,
) -> String {
    let flat: Vec<String> = (0..count).map(|i| item(i, indent, 0)).collect();
    let pad_open = if open.ends_with('(') { "" } else { " " };
    let pad_close = if close == ")" { "" } else { " " };
    let one = if count == 0 {
        format!("{open}{close}")
    } else {
        format!("{open}{pad_open}{}{pad_close}{close}", flat.join(", "))
    };
    if fits(col, &one) {
        return one;
    }
    let inner = indent + STEP;
    let mut out = format!("{open}\n");
    for i in 0..count {
        out.push_str(&pad(inner));
        // One column for the separator after the item.
        out.push_str(&item(i, inner, inner + 1));
        if trailing || i + 1 < count {
            out.push(',');
        }
        out.push('\n');
    }
    out.push_str(&pad(indent));
    out.push_str(close);
    out
}

/// A declaration, its body one clause per line.
fn layout_decl(d: &Decl) -> String {
    let mut out = print::header(d);
    out.push_str(" {\n");
    let lines = match &d.body {
        DeclBody::Read(body) => read_lines(body, STEP),
        DeclBody::Write(body) => write_lines(body, STEP),
    };
    for line in lines {
        out.push_str(&pad(STEP));
        out.push_str(&line);
        out.push('\n');
    }
    out.push('}');
    out
}

/// A read body's clauses, each starting at `indent`, without that
/// indentation.
fn read_lines(body: &ReadBody, indent: usize) -> Vec<String> {
    let mut lines = vec![match_clause(&body.pattern.elements, indent)];
    if let Some(w) = &body.pattern.predicate {
        lines.push(format!("where {}", expr(w, indent, indent + 6)));
    }
    let head = if body.distinct {
        "return distinct"
    } else {
        "return"
    };
    let col = indent + width(head) + 1;
    lines.push(format!(
        "{head} {}",
        list(
            "{",
            "}",
            indent,
            col,
            body.projections.len(),
            true,
            &|i, ind, c| {
                let p = &body.projections[i];
                match &p.alias {
                    Some(a) => format!("{} as {a}", expr(&p.expr, ind, c)),
                    None => expr(&p.expr, ind, c),
                }
            },
        )
    ));
    if !body.order.is_empty() {
        lines.push(format!(
            "order {}",
            list(
                "{",
                "}",
                indent,
                indent + 6,
                body.order.len(),
                true,
                &|i, ind, c| {
                    let o = &body.order[i];
                    let key = expr(&o.expr, ind, c);
                    match o.descending {
                        Some(true) => format!("{key} desc"),
                        Some(false) => format!("{key} asc"),
                        None => key,
                    }
                },
            )
        ));
    }
    match &body.limit {
        None => {}
        Some(crate::query::syntax::Limit::Literal(n)) => lines.push(format!("limit {n}")),
        Some(crate::query::syntax::Limit::Param(p)) => lines.push(format!("limit ${p}")),
    }
    lines
}

/// A write body's groups: each group's `match`, with its `where` on the same
/// line when it fits, then its statements.
fn write_lines(body: &WriteBody, indent: usize) -> Vec<String> {
    let mut lines = Vec::new();
    for group in &body.groups {
        if !group.pattern.elements.is_empty() {
            let m = match_clause(&group.pattern.elements, indent);
            match &group.pattern.predicate {
                None => lines.push(m),
                Some(w) => {
                    let joined = format!("{m} where {}", print::expr(w));
                    if fits(indent, &joined) {
                        lines.push(joined);
                    } else {
                        lines.push(m);
                        lines.push(format!("where {}", expr(w, indent, indent + 6)));
                    }
                }
            }
        }
        for s in &group.statements {
            lines.push(statement(s, indent));
        }
    }
    lines
}

fn statement(s: &Stmt, indent: usize) -> String {
    let assignments = |head: String, a: &[Assignment]| {
        let col = indent + width(&head) + 1;
        format!(
            "{head} {}",
            list("{", "}", indent, col, a.len(), true, &|i, ind, c| {
                let a = &a[i];
                let at = c + width(&a.property) + 2;
                format!("{}: {}", a.property, expr(&a.value, ind, at))
            })
        )
    };
    match s {
        Stmt::Insert {
            type_name,
            assignments: a,
        } => assignments(format!("insert {type_name}"), a),
        Stmt::Update {
            variable,
            assignments: a,
        } => assignments(format!("update ${variable} set"), a),
        Stmt::Delete { variable } => format!("delete ${variable}"),
    }
}

/// `match { … }`: short on one line, long an element per line with a
/// trailing comma.
fn match_clause(elements: &[Element], indent: usize) -> String {
    format!(
        "match {}",
        list(
            "{",
            "}",
            indent,
            indent + 6,
            elements.len(),
            true,
            &|i, ind, c| element(&elements[i], ind, c),
        )
    )
}

fn element(e: &Element, indent: usize, col: usize) -> String {
    match e {
        Element::Let { name, value } => {
            let head = format!("let ${name} = ");
            let at = col + width(&head);
            match value {
                LetValue::Relation(body) => format!("{head}{}", inline(body, indent, at)),
                LetValue::Expr(e) => format!("{head}{}", expr(e, indent, at)),
            }
        }
        Element::From { rel, score } => {
            let suffix = score
                .as_ref()
                .map(|s| format!(" as ${s}"))
                .unwrap_or_default();
            let flat = format!("from {}{suffix}", print::relation(rel));
            if fits(col, &flat) {
                return flat;
            }
            let body = match rel {
                RelExpr::Inline(body) => inline(body, indent, col + 5),
                RelExpr::Call(c) => call(c, indent, col + 5),
                RelExpr::Variable(v) => format!("${v}"),
            };
            format!("from {body}{suffix}")
        }
        Element::Optional(p) => format!("optional {}", braced(p, indent, col + 9)),
        Element::Binding { .. } | Element::Traversal(_) => print::element(e),
    }
}

/// An inline read: on one line when it fits, otherwise its clauses one per
/// line inside the braces.
fn inline(body: &ReadBody, indent: usize, col: usize) -> String {
    let flat = print::inline(body);
    if fits(col, &flat) {
        return flat;
    }
    let inner = indent + STEP;
    let mut out = String::from("{\n");
    for line in read_lines(body, inner) {
        out.push_str(&pad(inner));
        out.push_str(&line);
        out.push('\n');
    }
    out.push_str(&pad(indent));
    out.push('}');
    out
}

/// A block's or an `optional`'s braces: the elements and the block's own
/// `where`, on one line when it fits, otherwise an element per line with a
/// trailing comma and the `where` after them.
fn braced(p: &Pattern, indent: usize, col: usize) -> String {
    let flat = print::braced(p);
    if fits(col, &flat) {
        return flat;
    }
    let inner = indent + STEP;
    let mut out = String::from("{\n");
    for e in &p.elements {
        out.push_str(&pad(inner));
        out.push_str(&element(e, inner, inner));
        out.push_str(",\n");
    }
    if let Some(w) = &p.predicate {
        out.push_str(&pad(inner));
        out.push_str("where ");
        out.push_str(&expr(w, inner, inner + 6));
        out.push('\n');
    }
    out.push_str(&pad(indent));
    out.push('}');
    out
}

/// A call: on one line when it fits, otherwise an argument per line, with
/// no trailing comma, which a call's argument list does not take.
fn call(c: &Call, indent: usize, col: usize) -> String {
    let count = c.args.len() + c.options.len();
    list(
        &format!("{}(", c.name),
        ")",
        indent,
        col,
        count,
        false,
        &|i, ind, at| match c.args.get(i) {
            Some(Arg::Expr(e)) => expr(e, ind, at),
            Some(Arg::Inline(body)) => inline(body, ind, at),
            None => {
                let (k, v) = &c.options[i - c.args.len()];
                format!("{k}: {}", expr(v, ind, at + width(k) + 2))
            }
        },
    )
}

/// `e` where the grammar requires at least `min`.
fn at(e: &Expr, min: Prec, indent: usize, col: usize) -> String {
    if prec(e) < min {
        format!("({})", expr(e, indent, col + 1))
    } else {
        expr(e, indent, col)
    }
}

/// An expression starting at column `col`, its blocks and calls broken
/// over lines when it does not fit.
fn expr(e: &Expr, indent: usize, col: usize) -> String {
    let flat = print::expr(e);
    if fits(col, &flat) {
        return flat;
    }
    let binary = |l: &Expr, lmin: Prec, op: &str, r: &Expr, rmin: Prec| {
        let left = at(l, lmin, indent, col);
        let next = if left.contains('\n') {
            last_line(&left)
        } else {
            col + width(&left)
        } + width(op)
            + 2;
        format!("{left} {op} {}", at(r, rmin, indent, next))
    };
    match e {
        Expr::Block(b) => {
            let head = match (b.func, &b.arg) {
                (BlockFunc::Exists, _) => "exists".to_string(),
                (BlockFunc::Agg(f), None) => print::agg(f).to_string(),
                (BlockFunc::Agg(f), Some(a)) => format!("{}({})", print::agg(f), print::expr(a)),
            };
            let at_col = col + width(&head) + 1;
            format!("{head} {}", braced(&b.pattern, indent, at_col))
        }
        Expr::Call(c) => call(c, indent, col),
        Expr::Not(inner) => format!("not {}", at(inner, Prec::Not, indent, col + 4)),
        // A long conjunction or disjunction: an operand per line, the
        // operator leading each line after the first.
        Expr::And(l, r) => logical(l, Prec::And, "and", r, Prec::Not, indent, col),
        Expr::Or(l, r) => logical(l, Prec::Or, "or", r, Prec::And, indent, col),
        Expr::Compare {
            op, left, right, ..
        } => binary(left, Prec::Sum, print::comp(*op), right, Prec::Sum),
        Expr::In { needle, list, .. } => binary(needle, Prec::Sum, "in", list, Prec::Sum),
        Expr::Arith { op, left, right } => {
            let own = prec(e);
            let right_min = if own == Prec::Sum {
                Prec::Product
            } else {
                Prec::Unary
            };
            binary(left, own, op.symbol(), right, right_min)
        }
        _ => flat,
    }
}

/// `l op r` with `r` on its own line at `indent + STEP`, led by `op`.
fn logical(
    l: &Expr,
    lmin: Prec,
    op: &str,
    r: &Expr,
    rmin: Prec,
    indent: usize,
    col: usize,
) -> String {
    let left = at(l, lmin, indent, col);
    let inner = indent + STEP;
    let right = at(r, rmin, inner, inner + width(op) + 1);
    format!("{left}\n{}{op} {right}", pad(inner))
}

#[cfg(test)]
#[path = "fmt_tests.rs"]
mod tests;
