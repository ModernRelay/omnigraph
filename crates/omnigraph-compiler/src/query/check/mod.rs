//! The type checker (the GQ 3 RFC, sections 2, 3 and 5).
//!
//! It reads the parse tree ([`crate::query::syntax`]) of one source unit and
//! types every declaration into the core IR ([`crate::ir::core`]), or
//! refuses it with a typed code. The derived-forms table of section 2 is the
//! lowering: each element of `match` becomes a step in written order, `let`
//! and `from` seeing only the elements before them, and each top-level
//! conjunct of `where` a filter after the last binder of a variable it names;
//! a block in `where` correlates on every element's names, a block in a
//! `let` on the elements before it. The steps fold into one term.
//!
//! Traversal resolution (`traversal`), literal typing and type
//! compatibility (`literal`) are the one implementation of those rules. Each
//! scope's elements form components, and two that share no variable are a
//! product the scope admits only when every one but one is pinned to a row
//! by its `@key` (`components`, `T81`). A
//! last pass (`bind`) puts every expression in its bound form: physical
//! columns, and the casts [`crate::ir::coerce`] chooses.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::budget::{Overrun, PlanWork, WorkKind};
use crate::catalog::Catalog;
use crate::error::CompilerError;
use crate::ir::core::MutationTarget;
use crate::ir::core::WriteGroup;
use crate::ir::core::*;
use crate::ir::expr::{IRExpr, IROrdering};
use crate::query::codes::*;
use crate::query::diagnostic::{
    Applicability, Position, QueryCode, QueryDiagnostic, Suggestion, TextEdit,
};
use crate::query::file::{MutationWriteMode, UnattachedBase};
use crate::query::syntax::*;
use crate::query::syntax::{EdgeSelector, Traversal};
use crate::query::value::{AggFunc, CompOp, Literal, NOW_PARAM_NAME};
use crate::traversal::EdgeSelection;
use crate::types::{Direction, ExprType, PropType, ScalarType};
use literal::{assignment_compatible, check_literal_type, literal_type, types_compatible};
use traversal::{BoundVariable, resolve_traversal};

/// The internal error: a refusal, the signal that a recursive read's type
/// is not yet known (the base pass of [`Ctx::read_decl`]), or the error
/// type: the construct was refused and its refusal recorded on the context
/// ([`Ctx::recover`]), or it reads what a refused construct would have
/// bound. A construct of the error type passes every check, so nothing that
/// depends on a refused construct is refused again.
#[derive(Debug, Clone)]
enum E {
    D(QueryDiagnostic),
    Pending(String),
    Recorded,
}

impl From<QueryDiagnostic> for E {
    fn from(d: QueryDiagnostic) -> Self {
        Self::D(d)
    }
}

impl From<CompilerError> for E {
    fn from(e: CompilerError) -> Self {
        Self::D(
            e.diagnostic()
                .cloned()
                .unwrap_or_else(|| QueryDiagnostic::typecheck(T6, e.to_string())),
        )
    }
}

type R<T> = Result<T, E>;

fn err(code: QueryCode, message: impl Into<String>) -> E {
    E::D(QueryDiagnostic::typecheck(code, message))
}

fn err_fix(code: QueryCode, message: impl Into<String>, fix: impl Into<String>) -> E {
    E::D(QueryDiagnostic::typecheck(code, message).with_fix(fix))
}

/// The result of checking one declaration.
#[derive(Debug, Clone)]
pub struct CheckedDecl {
    pub core: CoreDecl,
}

/// Type-check every declaration of a file against `catalog`. The
/// declarations of the file are the source unit a `Call` resolves against.
pub fn check_file(
    catalog: &Catalog,
    file: &SourceFile,
) -> Vec<(String, Result<CheckedDecl, QueryDiagnostic>)> {
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain { before, decl } => before.iter().chain([decl.as_ref()]).collect(),
        Body::Statement(_) => Vec::new(),
    };
    let unit = source_unit(&decls);
    decls
        .iter()
        .map(|d| (d.name.clone(), check_decl(catalog, &unit, d)))
        .collect()
}

/// Type-check the declaration the front door handed on, with the other
/// declarations of its file as the source unit its calls resolve in.
pub fn check_syntax_decl(
    catalog: &Catalog,
    syntax: &crate::query::file::SyntaxDecl,
) -> Result<CoreDecl, QueryDiagnostic> {
    let file = syntax.file();
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain { before, decl } => before.iter().chain([decl.as_ref()]).collect(),
        Body::Statement(_) => Vec::new(),
    };
    let unit = source_unit(&decls);
    check_decl(catalog, &unit, syntax.decl()).map(|checked| checked.core)
}

/// [`check_syntax_decl`] over a declaration's header, its refusal as a
/// compiler error.
pub fn check_query_decl(
    catalog: &Catalog,
    decl: &crate::query::file::QueryDecl,
) -> crate::error::Result<CoreDecl> {
    check_syntax_decl(catalog, &decl.syntax).map_err(CompilerError::query)
}

/// The plan budget `decl` declares with `@budget(plan = n)` and the
/// annotation declaring it; `None` when it declares none, or declares one
/// the type checker refuses as malformed.
fn declared_plan_budget(decl: &Decl) -> Option<(u64, &Annotation)> {
    decl.annotations.iter().find_map(|a| {
        if a.name != "budget" {
            return None;
        }
        a.args.as_deref()?.iter().find_map(|arg| match arg {
            AnnotationArg::Named(key, AnnotationValue::Literal(Literal::Integer(n)))
                if key == "plan" && *n > 0 =>
            {
                Some((u64::try_from(*n).unwrap_or(u64::MAX), a))
            }
            _ => None,
        })
    })
}

/// The plan budget a query declaration declares, `@budget(plan = n)`.
pub fn declared_plan(decl: &crate::query::file::QueryDecl) -> Option<u64> {
    declared_plan_budget(decl.syntax.decl()).map(|(n, _)| n)
}

/// Refuse (`T89`) a declaration whose `@budget(plan = n)` is above
/// `ceiling`, the session's `plan_budget_limit`, before it is typed: a
/// client never sets the server's resource bounds (invariant 11), so the
/// budget is refused rather than clamped, and the refused declaration costs
/// no expansion. A declaration declaring no plan budget compiles under the
/// compile-time default.
pub fn admit_plan_budget(
    decl: &crate::query::file::QueryDecl,
    ceiling: u64,
) -> Result<(), QueryDiagnostic> {
    match declared_plan_budget(decl.syntax.decl()) {
        Some((declared, a)) if declared > ceiling => {
            let mut d = QueryDiagnostic::typecheck(
                T89,
                format!(
                    "`{}` declares `@budget(plan = {declared})`, above this session's ceiling `plan_budget_limit` = {ceiling}; a declared budget is refused, never clamped",
                    decl.name
                ),
            )
            .with_fix(format!(
                "declare `@budget(plan = {ceiling})` or less, or have the operator raise `plan_budget_limit` (`OMNIGRAPH_PLAN_BUDGET_LIMIT`)"
            ));
            if a.at.end > a.at.start {
                d.position = Some(a.at.position);
                d.end = Some(a.at.end_position);
            }
            Err(d)
        }
        _ => Ok(()),
    }
}

/// Each declaration's name and its checked form or refusal.
pub type CheckedDecls = Vec<(String, Result<CheckedDecl, QueryDiagnostic>)>;

/// The declarations of a source unit by name, the first of a repeated name
/// kept: the later one is refused (`T83`), and renaming it is the fix.
fn source_unit<'d>(decls: &[&'d Decl]) -> HashMap<String, &'d Decl> {
    let mut unit = HashMap::new();
    for d in decls {
        unit.entry(d.name.clone()).or_insert(*d);
    }
    unit
}

/// Every refusal of a source's earliest refusing stage in one pass, in
/// source order, each with its span; empty when the source compiles. The
/// stages are parsing, then name resolution and typing. Parsing resumes at
/// the next element, item, statement or declaration boundary after a
/// refusal ([`crate::query::syntax::recover`]), so every independent parse
/// refusal is reported, and a source with one is not typed. Otherwise the
/// pass holds each repeated declaration name (`T83`) and every
/// declaration's independent type refusals: a declaration is checked on its
/// own, and in each of its scopes a refusal in one element, conjunct, return
/// item, order key, statement, assignment or operand is recorded and the
/// next is checked; a refused construct is of the error type, which passes
/// every check, so what depends on it is not reported. The product rule
/// (`T81`) and the lowering admission (`T86`) are typing rules of the same
/// pass.
pub fn check_source_all(catalog: &Catalog, source: &str) -> Vec<QueryDiagnostic> {
    crate::query::diagnostic::LineIndex::scope(source, || check_source_all_scoped(catalog, source))
}

fn check_source_all_scoped(catalog: &Catalog, source: &str) -> Vec<QueryDiagnostic> {
    if let Err(refused) = crate::query::depth::check_query_nesting(source) {
        return vec![refused];
    }
    let (file, mut refusals) = crate::query::syntax::parse::parse_source_all(source);
    let Some(file) = file else {
        return refusals;
    };
    for (_, checked) in check_file(catalog, &file) {
        if let Err(refused) = checked {
            refusals.extend(refused.all());
        }
    }
    refusals.sort_by_key(|d| d.position.map_or(0, |at| at.byte));
    refusals
}

/// Parse a source and type-check it.
pub fn check_source(catalog: &Catalog, source: &str) -> Result<CheckedDecls, QueryDiagnostic> {
    let file = crate::query::syntax::parse::parse_source(source)?;
    Ok(check_file(catalog, &file))
}

/// [`check_source`] by the typing rules alone ([`type_decl`]): the core term
/// of every declaration this release types, lowered or not.
#[cfg(test)]
pub(crate) fn type_source(
    catalog: &Catalog,
    source: &str,
) -> Result<CheckedDecls, QueryDiagnostic> {
    let file = crate::query::syntax::parse::parse_source(source)?;
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain { before, decl } => before.iter().chain([decl.as_ref()]).collect(),
        Body::Statement(_) => Vec::new(),
    };
    let unit = source_unit(&decls);
    Ok(decls
        .iter()
        .map(|d| (d.name.clone(), type_decl(catalog, &unit, d)))
        .collect())
}

/// Type-check one declaration of a source unit. A declaration this
/// release's planner does not lower is refused (`T86`): acceptance is per
/// release ([`lowering`]).
pub fn check_decl(
    catalog: &Catalog,
    unit: &HashMap<String, &Decl>,
    decl: &Decl,
) -> Result<CheckedDecl, QueryDiagnostic> {
    match typed_decl(catalog, unit, decl) {
        Typed::Checked(checked) => {
            lowering::admit(catalog, decl, &checked.core)?;
            Ok(checked)
        }
        Typed::Refused {
            mut refusals,
            partial,
        } => {
            // `T86` is a typing refusal like the others of the pass. The
            // term of what typed holds no refused construct, nor anything
            // depending on one: each is of the error type, which lowers as
            // nothing, and removing a construct removes structure, so it
            // never makes the rest unlowered. A `T86` found there names a
            // term the author wrote and the checker accepted, independent of
            // the other refusals.
            if let Some(core) = partial
                && let Err(unlowered) = lowering::admit(catalog, decl, &core)
                && unlowered.position.is_some()
            {
                refusals.push(unlowered);
            }
            Err(in_source_order(refusals, &decl.at).expect("a refused declaration holds a refusal"))
        }
    }
}

/// Type-check one declaration into its core term, lowered by this release
/// or not: the typing rules alone. A monotone self-call types to `Fix`
/// here, so every rule of a later stage's term is tested in this one.
#[cfg(test)]
pub(crate) fn type_decl(
    catalog: &Catalog,
    unit: &HashMap<String, &Decl>,
    decl: &Decl,
) -> Result<CheckedDecl, QueryDiagnostic> {
    match typed_decl(catalog, unit, decl) {
        Typed::Checked(checked) => Ok(checked),
        Typed::Refused { refusals, .. } => {
            Err(in_source_order(refusals, &decl.at).expect("a refused declaration holds a refusal"))
        }
    }
}

/// A declaration typed: its checked form, or its refusals with the term of
/// what typed beside them when there is one.
enum Typed {
    Checked(CheckedDecl),
    Refused {
        refusals: Vec<QueryDiagnostic>,
        partial: Option<CoreDecl>,
    },
}

fn typed_decl(catalog: &Catalog, unit: &HashMap<String, &Decl>, decl: &Decl) -> Typed {
    typed_decl_measured(
        catalog,
        unit,
        decl,
        &mut crate::budget::WorkRecord::default(),
        &mut None,
        &mut 0,
    )
}

/// The measured work of typing one declaration.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct TypingWork {
    /// The work typing charged to the plan budget, by kind.
    pub work: crate::budget::WorkSpent,
    /// The expressions typed, each written expression once per time it is
    /// typed: the measure the growth laws hold to the source's size.
    pub typed_exprs: usize,
    pub accepted: bool,
    /// The checked declaration, when it is accepted.
    pub core: Option<CoreDecl>,
    /// The refusals, when it is refused.
    pub refusals: Vec<QueryDiagnostic>,
    /// The charge that passed the plan budget, when one did.
    pub overrun: Option<crate::budget::Overrun>,
}

/// The work typing each declaration of `source` took, by name.
#[cfg(test)]
pub(crate) fn typing_work(catalog: &Catalog, source: &str) -> Vec<(String, TypingWork)> {
    let file = crate::query::syntax::parse::parse_source(source).expect("parses");
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain { before, decl } => before.iter().chain([decl.as_ref()]).collect(),
        Body::Statement(_) => Vec::new(),
    };
    let unit = source_unit(&decls);
    decls
        .iter()
        .map(|d| {
            let mut work = crate::budget::WorkRecord::default();
            let mut overrun = None;
            let mut typed_exprs = 0;
            let typed =
                typed_decl_measured(catalog, &unit, d, &mut work, &mut overrun, &mut typed_exprs);
            let (accepted, core, refusals) = match typed {
                Typed::Checked(checked) => (true, Some(checked.core), Vec::new()),
                Typed::Refused { refusals, .. } => (false, None, refusals),
            };
            (
                d.name.clone(),
                TypingWork {
                    work: work.spent,
                    typed_exprs,
                    accepted,
                    core,
                    refusals,
                    overrun,
                },
            )
        })
        .collect()
}

fn typed_decl_measured(
    catalog: &Catalog,
    unit: &HashMap<String, &Decl>,
    decl: &Decl,
    work: &mut crate::budget::WorkRecord,
    overrun: &mut Option<crate::budget::Overrun>,
    typed_exprs: &mut usize,
) -> Typed {
    let refused = |d: QueryDiagnostic| Typed::Refused {
        refusals: vec![d],
        partial: None,
    };
    let mut ctx = Ctx {
        catalog,
        unit,
        made_up: Fresh::default(),
        frames: Vec::new(),
        assume: HashMap::new(),
        rec_hits: HashSet::new(),
        in_write: false,
        at: None,
        element_span: None,
        refusals: Vec::new(),
        work: PlanWork::new(DEFAULT_PLAN_BUDGET as u64),
        overrun: std::cell::RefCell::new(None),
        typed_calls: HashMap::new(),
        fail_memo: FailMemo::default(),
        typed_exprs: 0,
        calls_below: Vec::new(),
        expanding: Vec::new(),
    };
    let params = match param_types(&decl.name, &decl.params) {
        Ok(p) => p,
        Err(E::D(d)) => return refused(d.at_span_of(&decl.at)),
        Err(E::Pending(_) | E::Recorded) => unreachable!(),
    };
    ctx.frames.push(Frame {
        decl: decl.name.clone(),
        bindings: params
            .iter()
            .map(|(p, t)| SExpr::new(SKind::Param(p.clone()), Ty::Value(t.clone())))
            .collect(),
        params,
    });
    ctx.in_write = matches!(decl.body, DeclBody::Write(_));
    let out = match &decl.body {
        DeclBody::Read(_) => ctx.read_decl(decl).map(|r| {
            CoreDecl::Read(Box::new(CoreRead {
                name: decl.name.clone(),
                params: r.params,
                defaults: param_defaults(&decl.params),
                term: r.rel.term,
                result: r.rel.ty,
                plan_budget: r.budget,
                work: crate::budget::WorkSoFar::new(ctx.work.record()),
                fresh: ctx.made_up.clone(),
            }))
        }),
        DeclBody::Write(w) => ctx.write_decl(decl, w).map(CoreDecl::Write),
    };
    *work = ctx.work.record();
    *overrun = ctx.work.overrun();
    *typed_exprs = ctx.typed_exprs;
    // Past the plan budget, the overrun alone: what else was refused before
    // it, or because the compile stopped, is not the author's to fix first.
    if let Some(d) = ctx.overrun.borrow_mut().take() {
        return refused(d.at_span_of(&decl.at));
    }
    // Every refusal recorded by a recovery point, then the one that ended
    // the pass, each at its span or at the declaration's name.
    let mut refusals = std::mem::take(&mut ctx.refusals);
    let mut partial = None;
    match out {
        Ok(core) if !refusals.is_empty() => partial = Some(core),
        Err(E::Recorded) if !refusals.is_empty() => {}
        Ok(mut core) => {
            return match bind::bind_decl(catalog, &mut core) {
                Ok(()) => Typed::Checked(CheckedDecl { core }),
                Err(e) => refused(
                    match e {
                        CompilerError::Query(d) => *d,
                        other => QueryDiagnostic::typecheck(T7, other.to_string()),
                    }
                    .at_span_of(&decl.at),
                ),
            };
        }
        Err(E::D(d)) => {
            let d = ctx.placed(d);
            refusals.push(d);
        }
        Err(E::Pending(name)) => refusals.push(QueryDiagnostic::typecheck(
            T70,
            format!(
                "`{name}` calls itself on every branch; a recursion needs a base case, a `union` arm that does not call it"
            ),
        )),
        Err(E::Recorded) => unreachable!("a recorded refusal is on the context"),
    }
    Typed::Refused { refusals, partial }
}

/// The refusals of one declaration in source order, each without a position
/// placed at the declaration's name, one of each repeated refusal kept; the
/// first carries the others.
fn in_source_order(refusals: Vec<QueryDiagnostic>, decl: &SourceSpan) -> Option<QueryDiagnostic> {
    let mut refusals: Vec<QueryDiagnostic> =
        refusals.into_iter().map(|d| d.at_span_of(decl)).collect();
    refusals.sort_by_key(|d| d.position.map_or(0, |at| at.byte));
    let mut kept: Vec<QueryDiagnostic> = Vec::new();
    for d in refusals {
        if !kept.contains(&d) {
            kept.push(d);
        }
    }
    QueryDiagnostic::first_of(kept)
}

/// A refusal without a position, placed at the comparison or membership
/// test it was found in.
fn at_written(e: E, written: &SourceSpan) -> E {
    match e {
        E::D(d) => E::D(d.at_span_of(written)),
        other => other,
    }
}

/// A refusal without a position from an operand of a comparison: placed at
/// the first span written inside the operand, or at the comparison.
fn at_operand(e: E, operand: &Expr, written: &SourceSpan) -> E {
    match first_span(operand) {
        Some(span) => at_written(at_written(e, &span), written),
        None => at_written(e, written),
    }
}

/// A diagnostic without a position, placed at `span`.
pub(super) trait AtSpan {
    fn at_span_of(self, span: &SourceSpan) -> Self;
}

impl AtSpan for QueryDiagnostic {
    fn at_span_of(mut self, span: &SourceSpan) -> Self {
        if self.position.is_none() && span.end > span.start {
            self.position = Some(span.position);
            self.end = Some(span.end_position);
        }
        self
    }
}

/// An infix `contains` or `starts_with`, refused here because the
/// left operand's type picks the fix: over a list the test is `in`, over a
/// String the registry call.
fn infix_string(op: InfixStringOp, at: &SourceSpan, list: bool) -> QueryDiagnostic {
    let (left, right) = (&at.operands[0], &at.operands[1]);
    let replacement = if list && op == InfixStringOp::Contains {
        format!("{right} in {left}")
    } else {
        format!("{}({left}, {right})", op.name())
    };
    let mut diagnostic = QueryDiagnostic::typecheck(
        T7,
        format!("`{}` is a function, not an infix operator", op.name()),
    )
    .with_fix(replacement.clone())
    .with_suggestion(Suggestion {
        applicability: Applicability::MachineApplicable,
        edits: vec![TextEdit {
            start: at.start,
            end: at.end,
            replacement,
        }],
    });
    diagnostic.position = Some(at.position);
    diagnostic.end = Some(at.end_position);
    diagnostic
}

/// `search`, `match_text` and `fuzzy`, which are not registry rows: each is
/// refused with the `match_terms` call that keeps its any-term membership,
/// `fuzzy`'s edit budget (two when unwritten) its `max_edits`
/// (the GQ 3 RFC, section 4, Migration). A call of two
/// arguments, with no option but `fuzzy`'s `max_edits`, gets the fix as an
/// edit over the call.
fn removed_lexical_call(c: &Call) -> QueryDiagnostic {
    use crate::query::syntax::print;
    let name = c.name.as_str();
    // The edit budget, written as edition 2's positional third argument of
    // `fuzzy` or as its option; a budget of 0 is exact membership.
    let budget_of = |v: &Expr| match v {
        Expr::Literal(Literal::Integer(0)) => None,
        v => Some(print::expr(v)),
    };
    let positional = match c.args.as_slice() {
        [_, _, Arg::Expr(n)] if name == "fuzzy" => Some(n),
        _ => None,
    };
    let budget = match (c.options.as_slice(), positional) {
        ([], None) if name == "fuzzy" => Some(Some("2".to_string())),
        ([], None) => Some(None),
        ([], Some(n)) => Some(budget_of(n)),
        ([(o, v)], None) if name == "fuzzy" && o == "max_edits" => Some(budget_of(v)),
        _ => None,
    };
    let operands = match c.args.as_slice() {
        [Arg::Expr(field), Arg::Expr(query)] | [Arg::Expr(field), Arg::Expr(query), _]
            if c.args.len() == 2 || positional.is_some() =>
        {
            Some((print::expr(field), print::expr(query)))
        }
        _ => None,
    };
    let message = format!(
        "`{name}` is not a registry row; the lexical predicate is `match_terms(field, terms(text, mode: any{}))`",
        if name == "fuzzy" {
            ", max_edits: n"
        } else {
            ""
        }
    );
    let (Some(budget), Some((field, query))) = (budget, operands) else {
        return QueryDiagnostic::typecheck(T6, message).with_fix(format!(
            "match_terms(<field>, terms(<text>, mode: any{}))",
            if name == "fuzzy" {
                ", max_edits: 2"
            } else {
                ""
            }
        ));
    };
    let replacement = match budget {
        Some(n) => format!("match_terms({field}, terms({query}, mode: any, max_edits: {n}))"),
        None => format!("match_terms({field}, terms({query}, mode: any))"),
    };
    let mut diagnostic = QueryDiagnostic::typecheck(T6, message)
        .with_fix(replacement.clone())
        .with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits: vec![TextEdit {
                start: c.at.start,
                end: c.at.end,
                replacement,
            }],
        });
    if c.at.end > c.at.start {
        diagnostic.position = Some(c.at.position);
        diagnostic.end = Some(c.at.end_position);
    }
    diagnostic
}

/// The score column a ranking term appends: the term is a ranking, drawn
/// directly or through a named relation.
/// The name `T85`'s fix gives a ranking function's score.
fn default_score_name(function: &str) -> &'static str {
    match function {
        "bm25" | "rrf" => "score",
        _ => "distance",
    }
}

/// `base`, or its first numbered variant, that no column of `ty` has.
fn unused_column(base: &str, ty: &RelTy) -> String {
    let mut name = base.to_string();
    let mut n = 2;
    while ty.get(&name).is_some() {
        name = format!("{base}{n}");
        n += 1;
    }
    name
}

/// A relation carrying an unnamed score, drawn by a `from` whose `as` names
/// it: the score column takes the written name, and the relation then
/// carries a named score.
fn rename_score(r: &mut Rel, name: &str) -> R<()> {
    let at = r
        .score
        .take()
        .expect("the relation carries an unnamed score");
    if r.ty.cols[at].name == name {
        return Ok(());
    }
    if r.ty.cols.iter().any(|c| c.name == name) {
        return Err(err_fix(
            T68,
            format!("the ranking already has a `{name}` column"),
            "name the score another way: `as $<name>`",
        ));
    }
    r.ty.cols[at].name = name.to_string();
    rename_score_term(&mut r.term, name);
    Ok(())
}

/// The term that computes a relation's unnamed score writes it under
/// `name`: the ranking itself, or the ranking a named relation is bound to.
fn rename_score_term(term: &mut Term, name: &str) {
    match term {
        Term::Score { out, .. } | Term::ApproxTopK { out, .. } | Term::Fuse { out, .. } => {
            *out = name.to_string()
        }
        other => unreachable!("an unnamed score is a ranking's: {other:?}"),
    }
}

/// A ranking in a relation function's argument: its score has no name, so
/// the relation keeps only its population's columns (an `rrf` arm is read
/// whole, since fusion keeps only the target and the fused score).
fn unnamed_score(r: Rel) -> Rel {
    let Some(at) = r.score else {
        return r;
    };
    let mut cols = r.ty.cols;
    cols.remove(at);
    let mut shaped = r.shaped;
    shaped.remove(at);
    Rel {
        term: Term::Project {
            input: r.term.boxed(),
            items: cols
                .iter()
                .map(|c| {
                    (
                        c.name.clone(),
                        SExpr::new(SKind::Col(c.name.clone()), c.ty.clone()).to_ir(),
                    )
                })
                .collect(),
            keep: false,
            fails: false,
        },
        ty: RelTy { cols },
        shaped,
        ranked: None,
        score: None,
        free: r.free,
    }
}

/// The name `T85`'s fix gives the unnamed score `term` computes.
fn ranking_function(term: &Term) -> &'static str {
    match term {
        Term::Score {
            kind: ScoreKind::Bm25,
            ..
        }
        | Term::Fuse { .. } => default_score_name("rrf"),
        _ => default_score_name("distance"),
    }
}

/// The relation a `from` draws, as a fix names it.
fn relation_text(rel: &RelExpr) -> String {
    match rel {
        RelExpr::Call(c) => format!("{}(…)", c.name),
        RelExpr::Variable(v) => format!("${v}"),
        RelExpr::Inline(_) => "{ … }".to_string(),
    }
}

/// `T85` for a `from` whose relation carries an unnamed score and that has
/// no `as`; the fix names the score `$distance` for a distance and `$score`
/// for a relevance or fused score, or a fresh variant of it.
fn unnamed_ranking(rel: &RelExpr, name: &str, span: Option<&SourceSpan>) -> QueryDiagnostic {
    let mut d = QueryDiagnostic::typecheck(
        T85,
        format!(
            "`from {}` ranks without naming its score: a ranked `from` names its score with `as`, and the score enters scope only under that name",
            relation_text(rel)
        ),
    )
    .with_fix(format!("from {} as ${name}", relation_text(rel)));
    if let Some(span) = span {
        d.position = Some(span.position);
        d.end = Some(span.end_position);
        if let [(_, end)] = span.ranges.as_slice() {
            d = d.with_suggestion(Suggestion {
                applicability: Applicability::MachineApplicable,
                edits: vec![TextEdit {
                    start: *end,
                    end: *end,
                    replacement: format!(" as ${name}"),
                }],
            });
        }
    }
    d
}

/// `T85` for an `as` on a `from` whose relation carries no unnamed score
/// (it ranks nothing, or returns its score named): its columns are named
/// by its relation, so the fix removes the `as`.
fn named_non_ranking(rel: &RelExpr, name: &str, span: Option<&SourceSpan>) -> QueryDiagnostic {
    let mut d = QueryDiagnostic::typecheck(
        T85,
        format!(
            "`from {} as ${name}`: `as` names an unnamed score, and this `from`'s relation carries none; its columns are named by its relation",
            relation_text(rel)
        ),
    )
    .with_fix(format!("from {}", relation_text(rel)));
    if let Some(span) = span {
        d.position = Some(span.position);
        d.end = Some(span.end_position);
        if let [(_, end)] = span.ranges.as_slice() {
            d = d.with_suggestion(Suggestion {
                applicability: Applicability::MachineApplicable,
                edits: vec![TextEdit {
                    start: *end,
                    end: span.end,
                    replacement: String::new(),
                }],
            });
        }
    }
    d
}

/// `T65`: a ranking written as a scalar call, in `order`, `return` or a
/// filter; the fix draws it with `from` and reads its score column.
fn ranking_as_scalar(name: &str) -> QueryDiagnostic {
    let fix = match name {
        "nearest" => {
            "from nearest(<population>, $x.field, $q, k: <limit>) as $distance  and  order { $distance }"
        }
        "bm25" => "from bm25(<population>, $x.field, $q) as $score  and  order { $score desc }",
        "rrf" => "from rrf(<arm>, <arm>, k: 60) as $score  and  order { $score desc }",
        _ => "from distance(<population>, $x.field, $q) as $distance  and  order { $distance }",
    };
    QueryDiagnostic::typecheck(
        T65,
        format!(
            "`{name}` is a relation function; a ranking is drawn with `from`, and its score is a column"
        ),
    )
    .with_fix(fix)
}

/// `T65` for a ranking written as a call in `order` or `return`, with the
/// edit that draws it with `from` over the read's own pattern when the read
/// gives every piece the edit needs: the spans the parser recorded, a
/// `limit` to size a `nearest` window, every occurrence the same call, no
/// aggregate, and for `rrf` the type of the ranked binding.
fn ranking_in_order_or_return(
    catalog: &Catalog,
    body: &ReadBody,
    sc: &Scope,
) -> Option<QueryDiagnostic> {
    let is_ranking = |e: &Expr| matches!(e, Expr::Call(c) if matches!(c.name.as_str(), "nearest" | "bm25" | "rrf" | "distance"));
    let (call, written) = body
        .order
        .iter()
        .map(|o| &o.expr)
        .zip(
            body.spans
                .order
                .iter()
                .map(Some)
                .chain(std::iter::repeat(None)),
        )
        .chain(
            body.projections.iter().map(|p| &p.expr).zip(
                body.spans
                    .items
                    .iter()
                    .map(Some)
                    .chain(std::iter::repeat(None)),
            ),
        )
        .find(|(e, _)| is_ranking(e))?;
    let Expr::Call(call) = call else {
        unreachable!("is_ranking holds")
    };
    let mut diagnostic = ranking_as_scalar(&call.name);
    diagnostic.position = written.map(|s| s.position);
    if let Some((edits, fix)) = ranking_edits(catalog, body, call, sc) {
        diagnostic = diagnostic.with_fix(fix).with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits,
        });
    }
    Some(diagnostic)
}

/// The edits, and the fix they write: the `match` clause and the `order`.
/// The canonical ranked read names its population with `let` before the
/// ranking, `$pop` (`$all` for an `rrf`'s type population) or a fresh
/// variant, and names the score with `as` as the column it was, `$distance`
/// or `$score`, or a fresh variant.
fn ranking_edits(
    catalog: &Catalog,
    body: &ReadBody,
    call: &Call,
    sc: &Scope,
) -> Option<(Vec<TextEdit>, String)> {
    use crate::query::syntax::print;
    let spans = &body.spans;
    let inner = spans.match_inner.as_ref()?;
    if body.projections.iter().any(|p| contains_agg(&p.expr)) || body.distinct {
        return None;
    }
    let predicate_end = match (&body.pattern.predicate, &body.pattern.spans.predicate) {
        (None, _) => None,
        (Some(_), Some(p)) => Some(p.end),
        (Some(_), None) => return None,
    };
    // Every ranking in `order` and `return` is this call.
    let ranking = |e: &Expr| matches!(e, Expr::Call(c) if matches!(c.name.as_str(), "nearest" | "bm25" | "rrf" | "distance"));
    if body
        .order
        .iter()
        .map(|o| &o.expr)
        .chain(body.projections.iter().map(|p| &p.expr))
        .any(|e| ranking(e) && *e != Expr::Call(call.clone()))
    {
        return None;
    }
    // The variables the pattern binds, in written order.
    let mut vars: Vec<String> = Vec::new();
    let mut push = |v: &String| {
        if !v.starts_with('_') && !vars.contains(v) {
            vars.push(v.clone());
        }
    };
    for c in &body.pattern.elements {
        match c {
            Element::Binding { variable, .. } => push(variable),
            Element::Traversal(t) => {
                push(&t.src);
                if let Some(e) = &t.edge_binding {
                    push(e);
                }
                push(&t.dst);
            }
            _ => {}
        }
    }
    // Every name the read mentions or its scope holds; the names the fix
    // introduces are fresh against them.
    let mut taken = Vec::new();
    pattern_vars(&body.pattern, &mut taken);
    for e in body
        .projections
        .iter()
        .map(|p| &p.expr)
        .chain(body.order.iter().map(|o| &o.expr))
    {
        expr_vars(e, &mut taken);
    }
    let fresh = |base: &str| {
        let mut name = base.to_string();
        let mut n = 2;
        while taken.contains(&name) || sc.in_scope(&name) {
            name = format!("{base}{n}");
            n += 1;
        }
        format!("${name}")
    };
    let args: Vec<String> = call
        .args
        .iter()
        .map(|a| match a {
            Arg::Expr(e) => Some(print::expr(e)),
            _ => None,
        })
        .collect::<Option<_>>()?;
    let limit = spans.limit.as_ref().and_then(|_| match &body.limit {
        Some(Limit::Literal(n)) => Some(n.to_string()),
        Some(Limit::Param(p)) => Some(format!("${p}")),
        None => None,
    });
    let returned = vars
        .iter()
        .map(|v| format!("${v}"))
        .collect::<Vec<_>>()
        .join(", ");
    let (prefix, suffix, score, ordered) = match call.name.as_str() {
        "nearest" | "distance" | "bm25" => {
            let [field, query] = args.as_slice() else {
                return None;
            };
            let window = match call.name.as_str() {
                "nearest" => format!(", k: {}", limit.clone()?),
                _ => String::new(),
            };
            let (score, ordered) = match call.name.as_str() {
                "bm25" => {
                    let s = fresh("score");
                    (s.clone(), format!("{s} desc"))
                }
                _ => {
                    let s = fresh("distance");
                    (s.clone(), s)
                }
            };
            // The population is the whole `match` and its `where`, named
            // before the ranking; with a `where` the population closes after
            // it, and the outer `match` closes there too.
            let pop = fresh("pop");
            let tail = format!(
                " return {{ {returned} }} }}, from {}({pop}, {field}, {query}{window}) as {score}",
                call.name
            );
            (
                format!(" let {pop} = {{ match {{"),
                match predicate_end {
                    Some(_) => format!("{tail} }}"),
                    None => format!("}}{tail} "),
                },
                score,
                ordered,
            )
        }
        "rrf" => {
            // The arms rank the type of the ranked binding; the pattern
            // follows, correlated on it.
            let all = fresh("all");
            let mut arms = Vec::new();
            let mut ranked: Option<String> = None;
            for a in &call.args {
                let Arg::Expr(Expr::Call(arm)) = a else {
                    return None;
                };
                let [
                    Arg::Expr(field @ Expr::Prop { variable, .. }),
                    Arg::Expr(query),
                ] = arm.args.as_slice()
                else {
                    return None;
                };
                if ranked.get_or_insert_with(|| variable.clone()) != variable {
                    return None;
                }
                let window = match arm.name.as_str() {
                    "nearest" => format!(", k: {}", limit.clone().unwrap_or_else(|| "100".into())),
                    "bm25" => String::new(),
                    _ => return None,
                };
                arms.push(format!(
                    "{}({all}, {}, {}{window})",
                    arm.name,
                    print::expr(field),
                    print::expr(query)
                ));
            }
            let ranked = ranked?;
            let type_name = body.pattern.elements.iter().find_map(|c| match c {
                Element::Binding {
                    variable,
                    type_name,
                } if *variable == ranked => Some(type_name.clone()),
                _ => None,
            })?;
            let k = call
                .options
                .iter()
                .find(|(o, _)| o == "k")
                .map(|(_, v)| print::expr(v))
                .unwrap_or_else(|| "60".into());
            let score = fresh("score");
            (
                format!(
                    " let {all} = {{ match {{ ${ranked}: {type_name} }} return {{ ${ranked} }} }}, from rrf({}, k: {k}) as {score}, ",
                    arms.join(", ")
                ),
                String::new(),
                score.clone(),
                format!("{score} desc"),
            )
        }
        _ => return None,
    };
    let mut edits = vec![TextEdit {
        start: inner.start,
        end: inner.start,
        replacement: prefix,
    }];
    if !suffix.is_empty() {
        let at = predicate_end.unwrap_or(inner.end);
        edits.push(TextEdit {
            start: at,
            end: at,
            replacement: suffix.clone(),
        });
    }
    let this = Expr::Call(call.clone());
    for (o, span) in body.order.iter().zip(&spans.order) {
        if o.expr == this {
            edits.push(TextEdit {
                start: span.start,
                end: span.end,
                replacement: ordered.clone(),
            });
        }
    }
    if spans.order.len() != body.order.len() || spans.items.len() != body.projections.len() {
        return None;
    }
    for (p, span) in body.projections.iter().zip(&spans.items) {
        if p.expr == this {
            let alias = match (p.alias.is_none(), call.name.as_str()) {
                (true, "nearest" | "distance") => " as _distance",
                (true, _) => " as _score",
                _ => "",
            };
            edits.push(TextEdit {
                start: span.start,
                end: span.end,
                replacement: format!("{score}{alias}"),
            });
        }
    }
    // The population is an inline read, which binds every name its triples
    // introduce (`T84`).
    let fresh = match call.name.as_str() {
        "rrf" => Vec::new(),
        _ => end_bindings(catalog, &body.pattern.elements, &HashMap::new()),
    };
    let element_spans = &body.pattern.spans.elements;
    if !fresh.is_empty() && element_spans.len() != body.pattern.elements.len() {
        return None;
    }
    for (i, binds) in &fresh {
        let at = element_spans[*i].start;
        edits.push(TextEdit {
            start: at,
            end: at,
            replacement: binds.iter().map(|(n, t)| format!("${n}: {t}, ")).collect(),
        });
    }
    let elements = body
        .pattern
        .elements
        .iter()
        .enumerate()
        .flat_map(|(i, c)| {
            let binds = fresh.iter().find(|(j, _)| *j == i).map(|(_, b)| b.clone());
            binds
                .unwrap_or_default()
                .into_iter()
                .map(|(n, t)| format!("${n}: {t}"))
                .chain(std::iter::once(print::element(c)))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let filter = body
        .pattern
        .predicate
        .as_ref()
        .map(|p| format!(" where {}", print::expr(p)))
        .unwrap_or_default();
    let prefix = &edits[0].replacement;
    let fix = match (suffix.is_empty(), predicate_end) {
        (true, _) => format!("match {{{prefix}{elements} }}{filter}  order {{ {ordered} }}"),
        (false, None) => format!("match {{{prefix} {elements} {suffix}}}  order {{ {ordered} }}"),
        (false, Some(_)) => {
            format!("match {{{prefix} {elements} }}{filter}{suffix}  order {{ {ordered} }}")
        }
    };
    edits.sort_by_key(|e| (e.start, e.end));
    Some((edits, fix))
}

// ---------------------------------------------------------------------------
// Registry.

/// Relation functions (kind `relation`).
const RELATION_ROWS: &[&str] = &["distance", "nearest", "bm25", "rrf", "union"];
/// Scalar rows, today's and the break's.
const SCALAR_ROWS: &[&str] = &[
    "contains",
    "starts_with",
    "match_terms",
    "terms",
    "now",
    "if",
    "coalesce",
    "concat",
    "lower",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "length",
    "left",
    "right",
    "substring",
    "abs",
    "floor",
    "ceil",
    "round",
    "sqrt",
    "power",
    "exp",
    "ln",
    "log10",
    "size",
    "cast",
];
const AGGREGATES: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "collect",
    "stddev",
    "percentile",
];

/// Whether `name` (in lower case) names a registry row: a relation, scalar
/// or aggregate row, which the parser reads in any letter case.
pub(crate) fn is_registry_name(name: &str) -> bool {
    RELATION_ROWS.contains(&name) || SCALAR_ROWS.contains(&name) || AGGREGATES.contains(&name)
}
/// The default an omitted option of a registry row takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OptionDefault {
    /// No default: the option is required, or its absence means something
    /// no written value does.
    None,
    Integer(i128),
    /// An enumerated word, written bare.
    Word(&'static str),
    /// A list of one `1` per positional argument (`rrf`'s `weights`).
    OnePerArgument,
}

/// The options of each registry row that takes options, in the registry's
/// order, with the default each takes when it is omitted: lint's canonical
/// form of a call writes its options in this order and drops an option
/// equal to its default (`L210`, section 6).
pub(crate) const REGISTRY_OPTIONS: &[(&str, &[(&str, OptionDefault)])] = &[
    (
        "nearest",
        &[("k", OptionDefault::None), ("recall", OptionDefault::None)],
    ),
    (
        "rrf",
        &[
            ("k", OptionDefault::Integer(RRF_DEFAULT_K as i128)),
            ("weights", OptionDefault::OnePerArgument),
        ],
    ),
    (
        "terms",
        &[
            ("mode", OptionDefault::Word("all")),
            ("max_edits", OptionDefault::Integer(0)),
        ],
    ),
];

/// Words a registry row may never be named (section 5).
const CLAUSE_KEYWORDS: &[&str] = &["let", "from", "optional", "exists", "not"];
/// Annotations retained as pure metadata.
const METADATA_ANNOTATIONS: &[&str] = &["description", "instruction", "example", "rename_from"];

// ---------------------------------------------------------------------------
// Context, frames and scopes.

struct Ctx<'a> {
    catalog: &'a Catalog,
    unit: &'a HashMap<String, &'a Decl>,
    /// The names the pass made up, with their roles ([`Fresh`]).
    made_up: Fresh,
    /// The declared reads being inlined, outermost first.
    frames: Vec<Frame>,
    /// The assumed type of a recursive read during its fixpoint passes.
    assume: HashMap<String, RelTy>,
    /// Reads referenced while on the frame stack (they compile to `Fix`).
    rec_hits: HashSet<String>,
    /// Checking a write body: a declared read is not callable there until
    /// stage W2.
    in_write: bool,
    /// Where the element, `where`, return item, order key or statement being checked
    /// was written, start and end: the span of a refusal that names none of
    /// its own.
    at: Option<(Position, Position)>,
    /// The span of the element being checked, when the parser recorded one
    /// per element: where a triple's `T82` fix swaps its two ends.
    element_span: Option<SourceSpan>,
    /// The refusals the recovery points of the declaration recorded
    /// ([`Ctx::recover`]).
    refusals: Vec<QueryDiagnostic>,
    /// The plan budget of the declaration being checked and the work
    /// charged to it ([`crate::budget`]): every term node and expression
    /// node built and every expression typed, so a compile stops as soon as
    /// its work passes the budget ([`Ctx::spend`]).
    work: PlanWork,
    /// The refusal of the charge that passed the budget: once the work
    /// passed it, no refusal is recorded and the compile ends with this one.
    overrun: std::cell::RefCell<Option<QueryDiagnostic>>,
    /// Each declared read the declaration calls, typed once: its body's
    /// term, type and column shapes. A body that reached a recursive
    /// reference depends on the call frames and is typed at each call.
    typed_calls: HashMap<String, TypedCall>,
    /// The verdicts of [`Term::can_fail`] over the terms built so far.
    fail_memo: FailMemo,
    /// How many expressions the pass typed, each written expression once
    /// per time it is typed.
    typed_exprs: usize,
    /// The calls the last call typed or expanded runs through, itself first:
    /// what a refusal names when the work passes the budget inside it.
    calls_below: Vec<String>,
    /// The calls a shared body being charged at a later call site runs
    /// through ([`Ctx::overrun_refusal`]).
    expanding: Vec<String>,
}

/// A declared read typed once: its body's term, type and column shapes, and
/// the calls its body runs through.
type TypedCall = (Term, RelTy, Vec<Option<String>>, Vec<String>);

struct Frame {
    decl: String,
    params: Vec<(String, PropType)>,
    /// The arguments, typed in the caller's scope, by position.
    bindings: Vec<SExpr>,
}

/// A relation produced by a relation expression.
#[derive(Debug, Clone)]
struct Rel {
    term: Term,
    ty: RelTy,
    /// Per column, `None` when it is variable-shaped (a bare variable or an
    /// alias) and so may be drawn into scope by `from`; otherwise the return
    /// item as written, which the refusal names.
    shaped: Vec<Option<String>>,
    ranked: Option<Ranked>,
    /// The column of the unnamed score the relation carries: a ranking
    /// function's result, and so a named relation bound to one. A `from`
    /// drawing it names the score with `as` (`T85`), and a relation
    /// function's argument drops it. Any other relation, an inline read
    /// returning a named score included, carries none.
    score: Option<usize>,
    /// The outer columns the relation reads.
    free: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct Ranked {
    /// The node column the ranking ranks.
    column: String,
    /// What identifies the arm for the duplicate-arm refusal: the producer,
    /// the population term, the field and the query.
    identity: Option<(Term, String, ScoreQueryId)>,
}

#[derive(Debug, Clone, PartialEq)]
enum ScoreQueryId {
    Vector(SExpr),
    Lexical(LexicalQuery),
}

/// One step of a scope; the steps fold into its term in order.
#[derive(Debug, Clone)]
enum Step {
    /// A leaf or an independent term, correlated with what came before.
    Leaf(Term),
    Expand {
        src: String,
        dst: String,
        src_type: String,
        dst_type: String,
        edges: EdgeSelection,
        min_hops: u32,
        max_hops: Option<u32>,
        edge_var: Option<String>,
        /// Both endpoints were bound: the expansion targets a fresh column
        /// and a filter ties it to `dst`.
        semi: Option<String>,
    },
    /// Correlated block aggregates (each the inner `Group`, and the column
    /// that receives it), then the filter that reads them.
    Filter { blocks: Vec<BlockAgg>, pred: SExpr },
    /// `let $v = expr`.
    Let {
        blocks: Vec<BlockAgg>,
        name: String,
        value: SExpr,
    },
    /// `optional { p }`: the inner term, its new columns, and the outer
    /// columns it reads (its `Env`, which the padding arm keeps).
    Optional {
        inner: Term,
        new_cols: Vec<Col>,
        env: Vec<Col>,
    },
}

#[derive(Clone)]
struct Scope {
    params: HashMap<String, PropType>,
    /// Columns of enclosing scopes visible here (correlation).
    outer: Vec<Col>,
    /// The first of `outer` with each name, by position: a lookup is one
    /// probe, not a scan of the enclosing scopes' columns.
    outer_at: HashMap<String, usize>,
    cols: Vec<Col>,
    /// The first of `cols` with each name, by position ([`Scope::push_col`]).
    col_at: HashMap<String, usize>,
    rels: HashMap<String, Rel>,
    free: Vec<String>,
    /// The binder step index of each local column.
    bound_at: HashMap<String, usize>,
    steps: Vec<(usize, Step)>,
    /// Binder count so far.
    binders: usize,
    /// Lookahead: the type every binding of this scope declares.
    declared: HashMap<String, BoundVariable>,
    declared_nodes: HashSet<String>,
    /// Names this scope's elements will bind (lookahead for block
    /// correlation).
    future: HashSet<String>,
    /// Block aggregates produced while typing the current expression.
    block_buf: Vec<BlockAgg>,
    /// The guards around the expression being typed, each typed once where
    /// it is written: an operand of `and`, `or`, `if` or `coalesce` is
    /// evaluated only where these hold (A3), so a block under them that can
    /// fail is evaluated only there too ([`BlockAgg::guard`]).
    guards: Vec<SExpr>,
    /// Per guard, the column that holds it and every guard before it, once
    /// a block has needed it: the next block reads that column and adds the
    /// guards after it, so no guard is copied into a second block's.
    guard_cols: Vec<Option<String>>,
    pending: Vec<Pending>,
    /// True inside an inline read or a called body: a binding reusing an
    /// outer name is `T68`, since it would silently become a filter.
    inner: bool,
    /// True while a block whose only element re-mentions an outer variable
    /// is probed for its filter's nullability: the re-mention correlates.
    correlates: bool,
    /// True inside a block or an `optional`. Each, like an inline read, is
    /// an inner scope: it correlates on an outer name by reading it, a
    /// binding of an outer name is `T68`, and a name new to it is introduced
    /// by a binding element, so a triple naming one it does not bind is
    /// `T84`.
    block: bool,
    /// True inside an `optional`, whose new names enter the enclosing scope
    /// nullable; it names the scope in a refusal.
    optional: bool,
    /// While a filter written earlier is placed: the names bound before it,
    /// the only outer names a block inside it correlates on.
    block_visible: Option<std::rc::Rc<HashSet<String>>>,
    /// Every column read since an element began, local or outer: what the
    /// element's inner term reads, for the product rule (`components`).
    reads: Vec<String>,
    /// A scope of the declaration being checked, its top scope or an inner
    /// one (a block, an `optional`, an inline read): a refusal in one
    /// element, conjunct, return item, order key, statement or operand is
    /// recorded and the next one is checked ([`Ctx::recover`]). A called
    /// read's body and a probe stop at their first refusal.
    recover: bool,
    /// The names of the error type: what a refused construct of this scope
    /// or an enclosing one would have bound. A construct reading one is of
    /// the error type itself, so it passes every check unreported.
    poisoned: HashSet<String>,
    /// A refused `from` drew columns no one knows: a name the scope cannot
    /// resolve is of the error type too.
    opaque: bool,
    /// A construct of this scope was refused or is of the error type: an
    /// inner scope is then of the error type as a whole.
    refused: bool,
    /// The variables a refused or error-typed conjunct of the `where` names:
    /// it passes the key-pin check of the product rule, so `T81` does not
    /// follow from it.
    loose_pins: HashSet<String>,
}

#[derive(Clone)]
enum Pending {
    /// A conjunct of `where` and the names bound before it: a block inside it
    /// correlates on those alone.
    /// And where the `where` was written, start and end.
    Filter(
        Expr,
        std::rc::Rc<HashSet<String>>,
        Option<(Position, Position)>,
    ),
}

impl Scope {
    fn new(params: HashMap<String, PropType>, outer: Vec<Col>, rels: HashMap<String, Rel>) -> Self {
        let inner = !outer.is_empty();
        let mut outer_at = HashMap::with_capacity(outer.len());
        for (i, c) in outer.iter().enumerate() {
            outer_at.entry(c.name.clone()).or_insert(i);
        }
        Self {
            params,
            outer,
            outer_at,
            cols: Vec::new(),
            col_at: HashMap::new(),
            rels,
            free: Vec::new(),
            bound_at: HashMap::new(),
            steps: Vec::new(),
            binders: 0,
            declared: HashMap::new(),
            declared_nodes: HashSet::new(),
            future: HashSet::new(),
            block_buf: Vec::new(),
            guards: Vec::new(),
            guard_cols: Vec::new(),
            pending: Vec::new(),
            inner,
            correlates: false,
            block: false,
            optional: false,
            block_visible: None,
            reads: Vec::new(),
            recover: false,
            poisoned: HashSet::new(),
            opaque: false,
            refused: false,
            loose_pins: HashSet::new(),
        }
    }

    /// `guard` holds for the operands typed until it is popped.
    fn push_guard(&mut self, guard: SExpr) {
        self.guards.push(guard);
        self.guard_cols.push(None);
    }

    /// The guards back to the first `len`.
    fn truncate_guards(&mut self, len: usize) {
        self.guards.truncate(len);
        self.guard_cols.truncate(len);
    }

    /// An inner scope over `outer`: it recovers when this scope does, and
    /// the names of the error type stay of the error type inside it.
    fn inner_scope(&self, outer: Vec<Col>) -> Scope {
        let mut inner = Scope::new(self.params.clone(), outer, self.rels.clone());
        inner.recover = self.recover;
        inner.poisoned = self.poisoned.clone();
        inner.opaque = self.opaque;
        inner
    }

    /// Whether `name`, which nothing in scope resolves, is of the error
    /// type.
    fn is_error(&self, name: &str) -> bool {
        !self.in_scope(name)
            && (self.poisoned.contains(name) || (self.opaque && !self.future.contains(name)))
    }

    /// Whether a construct naming `names` depends on a refused one.
    fn depends_on_refused<'n>(&self, names: impl IntoIterator<Item = &'n String>) -> bool {
        names.into_iter().any(|n| self.is_error(n))
    }

    /// Record that a refused construct would have bound `names`: those the
    /// scope does not already hold.
    fn poison<'n>(&mut self, names: impl IntoIterator<Item = &'n String>) {
        for n in names {
            if !self.is_bound(n) {
                self.poisoned.insert(n.clone());
            }
        }
    }

    /// Every name in scope with its type, as a refusal of an unknown one
    /// lists them: the columns, the parameters and the named relations.
    fn names_in_scope(&self, made_up: &Fresh) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .visible()
            .into_iter()
            .filter(|c| !made_up.made_up(&c.name))
            .map(|c| (format!("${}", c.name), c.ty.display()))
            .collect();
        out.extend(
            self.params
                .iter()
                .map(|(p, t)| (format!("${p}"), t.display_name())),
        );
        out.extend(
            self.rels
                .keys()
                .map(|r| (format!("${r}"), "relation".into())),
        );
        out
    }

    /// Every column visible here, local first then outer, for an inner
    /// scope's `outer`.
    fn visible(&self) -> Vec<Col> {
        let mut out = self.cols.clone();
        out.extend(
            self.outer
                .iter()
                .enumerate()
                .filter(|(i, c)| {
                    !self.col_at.contains_key(&c.name) && self.outer_at.get(&c.name) == Some(i)
                })
                .map(|(_, c)| c.clone()),
        );
        out
    }

    fn local(&self, name: &str) -> Option<&Col> {
        self.col_at.get(name).map(|i| &self.cols[*i])
    }

    fn outer_col(&self, name: &str) -> Option<&Col> {
        self.outer_at.get(name).map(|i| &self.outer[*i])
    }

    /// Add `c` to the scope's columns.
    fn push_col(&mut self, c: Col) {
        self.col_at.entry(c.name.clone()).or_insert(self.cols.len());
        self.cols.push(c);
    }

    /// Remove the columns from `len` on, and return them.
    fn truncate_cols(&mut self, len: usize) -> Vec<Col> {
        let removed: Vec<Col> = self.cols.drain(len..).collect();
        for c in &removed {
            if self.col_at.get(&c.name).is_some_and(|i| *i >= len) {
                self.col_at.remove(&c.name);
            }
        }
        removed
    }

    /// The type of a column visible here, marking an outer one free.
    fn col(&mut self, name: &str) -> Option<Ty> {
        if let Some(c) = self.local(name) {
            let ty = c.ty.clone();
            self.reads.push(name.to_string());
            return Some(ty);
        }
        let ty = self.outer_col(name)?.ty.clone();
        self.reads.push(name.to_string());
        if !self.free.iter().any(|f| f == name) {
            self.free.push(name.to_string());
        }
        Some(ty)
    }

    fn is_bound(&self, name: &str) -> bool {
        self.local(name).is_some() || self.outer_col(name).is_some()
    }

    /// Any name in scope: a column, a parameter or a named relation.
    fn in_scope(&self, name: &str) -> bool {
        self.is_bound(name) || self.params.contains_key(name) || self.rels.contains_key(name)
    }

    fn push_binder(&mut self, step: Step, binds: &[Col]) {
        let at = self.binders;
        self.binders += 1;
        self.steps.push((at * 2 + 1, step));
        for c in binds {
            self.bound_at.insert(c.name.clone(), at);
            self.push_col(c.clone());
        }
    }

    /// A filter placed after binder `after` (0: before every binder, which
    /// the fold reads as after the first leaf, or over `Env`).
    fn push_filter(&mut self, after: usize, step: Step) {
        self.steps.push((after * 2, step));
    }
}

/// An identifier for a return item as written: its words joined by `_`
/// (`count($p)` is `count_p`, `$p.name` is `p_name`).
fn alias_for(written: &str) -> String {
    written
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

/// `e` with every subexpression a group key computes read as that key's
/// column.
fn key_columns(e: IRExpr, keys: &[(String, IRExpr)]) -> IRExpr {
    crate::stack::deep(|| {
        if let Some((name, key)) = keys.iter().find(|(_, k)| *k == e) {
            return IRExpr::AliasRef(name.clone(), key.ty().clone());
        }
        let mut e = e;
        let b = |x: &mut IRExpr| *x = key_columns(IRExpr::take(x), keys);
        match &mut e {
            IRExpr::Binary { left, right, .. } => {
                b(left);
                b(right);
            }
            IRExpr::Not(inner, _)
            | IRExpr::IsNull { expr: inner, .. }
            | IRExpr::Cast { expr: inner, .. }
            | IRExpr::Neg { expr: inner, .. } => b(inner),
            IRExpr::Call { args, .. } => args.iter_mut().for_each(b),
            _ => {}
        }
        e
    })
}

/// The reason an assignment cannot name a system column.
fn system_assignment_hint(property: &str) -> &'static str {
    match property {
        "id" => {
            "; the engine assigns system identity; it cannot be set through a property assignment"
        }
        _ => "",
    }
}

fn param_types(decl: &str, params: &[Param]) -> R<Vec<(String, PropType)>> {
    let mut out = Vec::new();
    for p in params {
        if p.name == NOW_PARAM_NAME {
            return Err(err(
                T47,
                format!(
                    "parameter name `${NOW_PARAM_NAME}` is reserved for runtime timestamp injection"
                ),
            ));
        }
        let Some(ty) = PropType::from_param_type_name(&p.type_name, p.nullable) else {
            let written = p.type_name.trim_start_matches('[').trim_end_matches(']');
            return Err(err(
                T48,
                format!(
                    "unknown parameter type `{}` for `${}`{}",
                    p.type_name,
                    p.name,
                    nearest::listing(written, nearest::scalar_types())
                ),
            ));
        };
        if let Some(default) = &p.default {
            let mut lit = default.value.clone();
            if default.negative {
                lit = match lit {
                    Literal::Integer(n) => Literal::Integer(-n),
                    Literal::Float(f) => Literal::Float(-f),
                    _ => {
                        return Err(err(
                            T3,
                            format!("the default of `${}` negates a non-number", p.name),
                        ));
                    }
                };
            }
            // A default is a value of the declared type, in its range; a
            // `[]` takes the declared list type.
            let untyped_list = matches!(&lit, Literal::List(items) if items.iter().all(|i| matches!(i, Literal::Null)));
            if !(untyped_list && ty.list) {
                let lt = literal_type(&lit)?;
                check_literal_type(&lit, &lt, &ty, &p.name)?;
            }
        }
        out.push((p.name.clone(), ty));
    }
    // Arguments bind by position, so a parameter a call may omit is one
    // every later parameter may omit too: a defaulted parameter is followed
    // only by defaulted ones. The fix moves the defaulted parameters after
    // the others, each group in written order; a caller's arguments then
    // bind other parameters, so it needs review.
    if let Some(first) = params.iter().position(|p| p.default.is_some())
        && let Some(after) = params[first..].iter().find(|p| p.default.is_none())
    {
        let (required, defaulted): (Vec<&Param>, Vec<&Param>) =
            params.iter().partition(|p| p.default.is_none());
        let moved: Vec<String> = required
            .into_iter()
            .chain(defaulted)
            .map(crate::query::syntax::print::param)
            .collect();
        return Err(err_fix(
            T88,
            format!(
                "`{decl}`'s parameter `${}` has a default and `${}` after it has none; arguments bind by position, so a parameter with a default is followed only by parameters with defaults",
                params[first].name, after.name
            ),
            format!("({})", moved.join(", ")),
        ));
    }
    Ok(out)
}

/// The value a parameter's default writes, its sign applied.
pub(crate) fn default_value(default: &ParamDefault) -> Literal {
    match (&default.value, default.negative) {
        (Literal::Integer(n), true) => Literal::Integer(-n),
        (Literal::Float(f), true) => Literal::Float(-f),
        (value, _) => value.clone(),
    }
}

/// The declared defaults of `params`, each typed by [`param_types`] against
/// its declaration first.
fn param_defaults(params: &[Param]) -> Vec<(String, Literal)> {
    params
        .iter()
        .filter_map(|p| Some((p.name.clone(), default_value(p.default.as_ref()?))))
        .collect()
}

struct ReadOut {
    params: Vec<(String, PropType)>,
    rel: Rel,
    budget: usize,
}

struct Budget {
    plan: usize,
    rounds: Option<u64>,
}

/// The annotations of a declaration (section 5): the metadata allow-list is
/// retained unread, `@budget(plan = n, rounds = n)` is typed, and every other
/// name is refused; a name a later stage types is refused as not supported
/// yet, never naming the stage.
fn annotations(decl: &Decl) -> R<Budget> {
    let mut budget = Budget::default();
    for a in &decl.annotations {
        placed_annotation(a, &mut budget)?;
    }
    Ok(budget)
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            plan: DEFAULT_PLAN_BUDGET,
            rounds: None,
        }
    }
}

/// [`annotation`], a refusal placed at the annotation.
fn placed_annotation(a: &Annotation, budget: &mut Budget) -> R<()> {
    annotation(a, budget).map_err(|e| match e {
        // The suggestion removes the annotation; whether the text still
        // means what its author wanted is theirs to judge.
        E::D(mut d) => {
            d.position = Some(a.at.position);
            d.end = Some(a.at.end_position);
            E::D(d.with_suggestion(Suggestion {
                applicability: Applicability::NeedsReview,
                edits: vec![TextEdit {
                    start: a.at.start,
                    end: a.at.end,
                    replacement: String::new(),
                }],
            }))
        }
        other => other,
    })
}

fn annotation(a: &Annotation, budget: &mut Budget) -> R<()> {
    {
        match a.name.as_str() {
            name if METADATA_ANNOTATIONS.contains(&name) => return Ok(()),
            "budget" => {}
            "if_commit" | "valid" => {
                return Err(err(
                    T80,
                    format!("this release does not support `@{}` yet", a.name),
                ));
            }
            other => {
                return Err(err(
                    T80,
                    format!(
                        "`@{other}` is not an annotation the language reads; the metadata annotations are @{}, and `@budget(plan = n, rounds = n)` is typed",
                        METADATA_ANNOTATIONS.join(", @")
                    ),
                ));
            }
        }
        let Some(args) = a.args.as_deref().filter(|args| !args.is_empty()) else {
            return Err(err(
                T80,
                "`@budget` takes `plan = n` and `rounds = n`, each a positive integer",
            ));
        };
        for arg in args {
            let AnnotationArg::Named(key, value) = arg else {
                return Err(err(
                    T80,
                    "`@budget` takes `plan = n` and `rounds = n`, each a positive integer",
                ));
            };
            match key.as_str() {
                "plan" | "rounds" => {}
                "rows" | "ms" | "on_exceed" => {
                    return Err(err(
                        T80,
                        format!("this release does not support `@budget({key} = …)` yet"),
                    ));
                }
                other => {
                    return Err(err(
                        T80,
                        format!("`@budget` has no `{other}`; it takes `plan` and `rounds`"),
                    ));
                }
            }
            let AnnotationValue::Literal(Literal::Integer(n)) = value else {
                return Err(err(
                    T80,
                    format!("`@budget({key} = …)` takes a positive integer"),
                ));
            };
            if *n <= 0 {
                return Err(err(T80, format!("`@budget({key} = {n})` must be positive")));
            }
            match key.as_str() {
                "plan" => budget.plan = usize::try_from(*n).unwrap_or(usize::MAX),
                _ => budget.rounds = Some(u64::try_from(*n).unwrap_or(u64::MAX)),
            }
        }
    }
    Ok(())
}

impl Ctx<'_> {
    /// A name no declaration can collide with: [`Fresh::mint`] spells it
    /// with a leading uppercase letter, which no written name has, and
    /// records its role.
    fn fresh(&mut self, role: FreshRole) -> String {
        self.made_up.mint(role)
    }

    /// `^name`: a return item's or an aggregate's column beside the match
    /// columns, recorded as one.
    fn item(&mut self, name: &str) -> String {
        let item = format!("^{name}");
        self.made_up.record(&item, FreshRole::Item);
        item
    }

    /// A recovery point: a refusal is recorded at its span
    /// (the construct's, when it names none of its own) and `None` returned,
    /// so the caller checks the next construct; a construct abandoned after
    /// recording is `None` too. A recursive read's pending type is not a
    /// refusal and propagates.
    fn recover<T>(&mut self, result: R<T>) -> R<Option<T>> {
        // Past the plan budget the compile ends: nothing more is checked.
        if let Some(d) = self.overrun.borrow().as_ref() {
            return result.map(Some).map_err(|_| E::D(d.clone()));
        }
        match result {
            Ok(v) => Ok(Some(v)),
            Err(E::D(d)) => {
                let d = self.placed(d);
                self.refusals.push(d);
                Ok(None)
            }
            Err(E::Recorded) => Ok(None),
            Err(E::Pending(name)) => Err(E::Pending(name)),
        }
    }

    /// The annotations of the top declaration, each refusal recorded.
    fn recorded_annotations(&mut self, decl: &Decl) -> R<Budget> {
        let mut budget = Budget::default();
        for a in &decl.annotations {
            let typed = placed_annotation(a, &mut budget);
            self.recover(typed)?;
        }
        Ok(budget)
    }

    /// [`Self::recover`] in a scope that recovers; elsewhere (a called
    /// read's body, a probe) the refusal propagates.
    fn guard<T>(&mut self, top: bool, result: R<T>) -> R<Option<T>> {
        if top {
            self.recover(result)
        } else {
            result.map(Some)
        }
    }

    /// `d` placed at the construct being checked when it names no position
    /// of its own, or given that construct's end when it starts there.
    fn placed(&self, mut d: QueryDiagnostic) -> QueryDiagnostic {
        if let Some((start, end)) = self.at {
            if d.position.is_none() {
                d.position = Some(start);
                d.end = Some(end);
            } else if d.end.is_none() && d.position == Some(start) {
                d.end = Some(end);
            }
        }
        d
    }

    /// The errors of two operands that both failed. An operand of the error
    /// type makes every check over it pass, so the construct holding it is
    /// of the error type too; but a refusal in the first operand does not
    /// hide an independent one in the second: it is recorded, and the
    /// second error stands for the construct.
    fn join(&mut self, sc: &Scope, first: E, second: E) -> E {
        match (first, second) {
            (E::Pending(name), _) | (_, E::Pending(name)) => E::Pending(name),
            (E::D(d), second) if sc.recover => {
                let d = self.placed(d);
                self.refusals.push(d);
                second
            }
            (E::Recorded, second) => second,
            (first, _) => first,
        }
    }

    /// The error so far of a list of operands, joined with the next one's.
    fn join_failed(&mut self, sc: &Scope, so_far: Option<E>, next: E) -> E {
        match so_far {
            None => next,
            Some(earlier) => self.join(sc, earlier, next),
        }
    }

    /// Two operands, typed: both, or the error of the construct holding
    /// them ([`Self::join`]).
    fn both(&mut self, sc: &Scope, a: R<SExpr>, b: R<SExpr>) -> R<(SExpr, SExpr)> {
        match (a, b) {
            (Ok(a), Ok(b)) => Ok((a, b)),
            (Err(e), Ok(_)) | (Ok(_), Err(e)) => Err(e),
            (Err(first), Err(second)) => Err(self.join(sc, first, second)),
        }
    }

    /// The guard a block that can fail runs under, typed where it is
    /// written: every guard around it, as a column of the outer row. The
    /// guards a block written before it in this expression already computed
    /// are read from their column; each guard after them adds a column that
    /// reads the one before it, so a chain of guarded blocks computes each
    /// guard once.
    fn block_guard(&mut self, sc: &mut Scope) -> Option<BlockGuard> {
        let top = sc.guards.len().checked_sub(1)?;
        let start = (0..=top)
            .rev()
            .find(|level| sc.guard_cols[*level].is_some())
            .map_or(0, |level| level + 1);
        let boolean = Ty::scalar(ScalarType::Bool, false);
        let mut columns = Vec::new();
        for level in start..=top {
            let guard = sc.guards[level].clone();
            let guard = match level
                .checked_sub(1)
                .and_then(|below| sc.guard_cols[below].clone())
            {
                None => guard,
                Some(below) => SExpr::new(
                    SKind::And(
                        SExpr::new(SKind::Col(below), boolean.clone()).boxed(),
                        guard.boxed(),
                    ),
                    boolean.clone(),
                ),
            };
            let name = self.fresh(FreshRole::Guard);
            columns.push((name.clone(), guard));
            sc.guard_cols[level] = Some(name);
        }
        let column = sc.guard_cols[top].clone()?;
        Some(BlockGuard { columns, column })
    }

    /// Charge `n` units of `kind` to the declaration's plan budget
    /// ([`crate::budget`]); past it, `T70`, which no recovery point records
    /// past: the compile ends with it.
    fn spend(&self, kind: WorkKind, n: usize) -> R<()> {
        if let Some(d) = self.overrun.borrow().as_ref() {
            return Err(E::D(d.clone()));
        }
        let Err(overrun) = self.work.charge(kind, n) else {
            return Ok(());
        };
        let d = self.overrun_refusal(&overrun);
        *self.overrun.borrow_mut() = Some(d.clone());
        Err(E::D(d))
    }

    /// `T70` for `overrun`, naming the chain of calls being expanded.
    fn overrun_refusal(&self, overrun: &Overrun) -> QueryDiagnostic {
        let name = self.frames.first().map_or("", |f| f.decl.as_str());
        let chain: Vec<&str> = self
            .frames
            .iter()
            .skip(1)
            .map(|f| f.decl.as_str())
            .chain(self.expanding.iter().map(String::as_str))
            .collect();
        overrun.refusal(name, &chain)
    }

    /// Charge the expanded tree of `term`, built once and shared again
    /// (a declared read's body at a later call site, a named relation at
    /// each draw), walking no further than the budget left
    /// ([`Term::weight`]).
    fn charge_expansion(&self, term: &Term) -> R<()> {
        let remaining = usize::try_from(self.work.remaining()).unwrap_or(usize::MAX);
        let (terms, exprs) = term.weight(remaining);
        self.charge(Built {
            terms,
            exprs,
            steps: 0,
        })
    }

    /// Charge `term`'s top node, just built over a term already charged,
    /// and the expressions it holds.
    fn charge_top(&self, term: &Term) -> R<()> {
        self.charge(Built {
            terms: 1,
            exprs: term.expr_nodes(),
            steps: 0,
        })
    }

    /// An inner scope of `sc` over `outer`, its environment charged as it
    /// is copied: the outer columns, the parameters, the named relations and
    /// the names of the error type. A scope nested `d` deep copies what the
    /// scopes around it hold, so this charge grows with nesting and with a
    /// long run of `optional`s or blocks, as the copying does.
    fn inner_scope(&self, sc: &Scope, outer: Vec<Col>) -> R<Scope> {
        self.spend(
            WorkKind::Typing,
            outer.len() + sc.params.len() + sc.rels.len() + sc.poisoned.len(),
        )?;
        Ok(sc.inner_scope(outer))
    }

    /// Charge the term nodes and the expression nodes `nodes` adds, and
    /// the term nodes it rewrote.
    fn charge(&self, nodes: Built) -> R<()> {
        self.spend(WorkKind::TermNodes, nodes.terms)?;
        self.spend(WorkKind::ExprNodes, nodes.exprs)?;
        self.spend(WorkKind::Typing, nodes.steps)
    }

    /// The written construct `span` holds, as a refusal names it: work
    /// charged while it is checked is charged to it, unless an enclosing
    /// construct is already being charged ([`PlanWork::enter`]).
    fn enter(&self, what: &str, span: Option<&SourceSpan>) -> bool {
        self.work.enter(|| match span {
            Some(s) => format!("{what} at {}:{}", s.position.line, s.position.column),
            None => what.to_string(),
        })
    }

    /// The construct being checked was written at `span`.
    fn set_at(&mut self, span: &SourceSpan) {
        self.at = Some((span.position, span.end_position));
    }

    // -----------------------------------------------------------------------
    // Declarations.

    fn read_decl(&mut self, decl: &Decl) -> R<ReadOut> {
        // The declaration being checked records an independent refusal and
        // checks on; a refusal of the name or the annotations is independent
        // of the body. A called read's body stops at its first refusal.
        let top = self.frames.len() == 1;
        let mark = self.refusals.len();
        if is_registry_name(&decl.name) || CLAUSE_KEYWORDS.contains(&decl.name.as_str()) {
            let refused = err(
                T74,
                format!(
                    "a declared read may not be named `{}`, which is a registry row or an element keyword",
                    decl.name
                ),
            );
            if !top {
                return Err(refused);
            }
            self.set_at(&decl.at);
            self.recover::<()>(Err(refused))?;
        }
        let DeclBody::Read(body) = &decl.body else {
            return Err(err(
                T74,
                format!(
                    "`{}` is a write; a read draws rows only from reads, never from a write",
                    decl.name
                ),
            ));
        };
        let params = param_types(&decl.name, &decl.params)?;
        let budget = if top {
            let budget = self.recorded_annotations(decl)?;
            // Nothing is charged before the budget is known.
            self.work = PlanWork::new(budget.plan as u64);
            self.work.phase(format!("the body of `{}`", decl.name));
            budget
        } else {
            annotations(decl)?
        };
        let name = decl.name.clone();
        self.rec_hits.remove(&name);
        self.assume.remove(&name);
        let pmap: HashMap<String, PropType> = params.iter().cloned().collect();
        let mut sc = Scope::new(pmap.clone(), Vec::new(), HashMap::new());
        sc.recover = top;
        let mut rel = self.read_body(body, sc)?;
        if self.refusals.len() > mark {
            // The term of what typed, for the lowering admission; a
            // recursive read's fixpoint and its plan budget read the whole
            // term, so they wait for a declaration with no refusal.
            if self.rec_hits.contains(&name) || !top {
                return Err(E::Recorded);
            }
            return Ok(ReadOut {
                params,
                rel,
                budget: budget.plan,
            });
        }
        if self.rec_hits.contains(&name) {
            // The fixpoint of the read's type: start from the base arms'
            // type, retype under it until it is stable.
            for _ in 0..4 {
                self.assume.insert(name.clone(), rel.ty.clone());
                let again =
                    self.read_body(body, Scope::new(pmap.clone(), Vec::new(), HashMap::new()))?;
                let stable = again.ty == rel.ty;
                rel = again;
                if stable {
                    break;
                }
            }
            self.assume.remove(&name);
            let lattice = monotone(&name, &rel.term, budget.rounds)?;
            rel.term = Term::Fix {
                name: name.clone(),
                body: rel.term.boxed(),
                rounds: budget.rounds,
                lattice,
            };
        }
        // Every node of the expanded tree was charged as it was built, so
        // a tree over the budget stopped the compile there.
        Ok(ReadOut {
            params,
            rel,
            budget: budget.plan,
        })
    }

    fn write_decl(&mut self, decl: &Decl, body: &WriteBody) -> R<CoreWrite> {
        let params = param_types(&decl.name, &decl.params)?;
        if let MutationWriteMode::Unattached {
            base: Some(UnattachedBase::Param(name)),
        } = &decl.write_mode
        {
            let refused = match params.iter().find(|(n, _)| n == name) {
                Some((_, ty)) if ty.scalar == ScalarType::String && !ty.nullable && !ty.list => {
                    None
                }
                Some((_, ty)) => Some(err(
                    T55,
                    format!(
                        "`on $base` needs a declared `String` parameter; `${name}` is `{}`",
                        ty.display_name()
                    ),
                )),
                None => Some(err(
                    T55,
                    format!(
                        "`on $base` needs a declared `String` parameter; `${name}` is not declared"
                    ),
                )),
            };
            if let Some(refused) = refused {
                self.set_at(&decl.at);
                self.recover::<()>(Err(refused))?;
            }
        }
        let plan = self.recorded_annotations(decl)?.plan;
        self.work = PlanWork::new(plan as u64);
        self.work.phase(format!("the body of `{}`", decl.name));
        let pmap: HashMap<String, PropType> = params.iter().cloned().collect();
        let mut groups = Vec::new();
        let mut class: Option<(WriteClass, String)> = None;
        // A group's statements run in order, each keyed effect seeing the
        // ones before it, and across statements the later effect wins; two
        // values for one entity reached through the rows of one statement
        // are found only by running it, so no conflict is refused here. Each
        // group is a top scope: a refusal in one element, conjunct or
        // statement is recorded and the next is checked.
        for (g, group) in body.groups.iter().enumerate() {
            let mut sc = Scope::new(pmap.clone(), Vec::new(), HashMap::new());
            sc.recover = true;
            let typed = self.pattern(&mut sc, &group.pattern);
            if self.recover(typed)?.is_none() {
                continue;
            }
            let finished = self.finish(&mut sc);
            let Some(input) = self.recover(finished)? else {
                continue;
            };
            let input = input.unwrap_or(Term::Unit);
            let mut sinks = Vec::new();
            for (i, s) in group.statements.iter().enumerate() {
                if let Some(span) = group.spans.statements.get(i) {
                    self.set_at(span);
                }
                if sc.depends_on_refused(&statement_names(s)) {
                    continue;
                }
                let typed = self.statement(&mut sc, s, group.spans.statements.get(i));
                let Some((sink, this)) = self.recover(typed)? else {
                    continue;
                };
                let label = format!("statement {} of group {}", i + 1, g + 1);
                match &class {
                    Some((c, first)) if *c != this => {
                        self.recover::<()>(Err(err(
                            T76,
                            format!(
                                "{label} is {} but {first} is {}; a write mixes no `delete` with an `insert` or `update`: split it into separate mutations",
                                class_word(this),
                                class_word(*c)
                            ),
                        )))?;
                    }
                    Some(_) => {}
                    None => class = Some((this, label.clone())),
                }
                sinks.push(sink);
            }
            groups.push(WriteGroup { input, sinks });
        }
        Ok(CoreWrite {
            name: decl.name.clone(),
            params,
            defaults: param_defaults(&decl.params),
            groups,
            class: class.map_or(WriteClass::Constructive, |(c, _)| c),
            write_mode: decl.write_mode.clone(),
            fresh: self.made_up.clone(),
            work: crate::budget::WorkSoFar::new(self.work.record()),
        })
    }

    // -----------------------------------------------------------------------
    // Reads.

    /// A read body in `sc` (top level: no outer columns; inline: the
    /// enclosing scope's visible columns).
    fn read_body(&mut self, body: &ReadBody, mut sc: Scope) -> R<Rel> {
        let top = sc.recover;
        let mark = self.refusals.len();
        if let Some(refusal) = ranking_in_order_or_return(self.catalog, body, &sc) {
            if !top {
                return Err(E::D(refusal));
            }
            // The return or order holds the ranking: the `match` is checked
            // alone.
            self.recover::<()>(Err(E::D(refusal)))?;
            let typed = self.pattern(&mut sc, &body.pattern);
            self.recover(typed)?;
            return Err(E::Recorded);
        }
        let typed = self.pattern(&mut sc, &body.pattern);
        if self.guard(top, typed)?.is_none() {
            // A refusal of the whole pattern: nothing it binds is known.
            return Err(E::Recorded);
        }
        // Return items, typed beside every match column.
        let mut items = Vec::new();
        let mut aggs = Vec::new();
        let mut post: Vec<(String, SExpr)> = Vec::new();
        let mut shaped = Vec::new();
        // The columns of the items typed, in written order.
        let mut names: Vec<String> = Vec::new();
        // Every item's column, for the duplicate check.
        let mut seen: Vec<String> = Vec::new();
        // The items refused or of the error type: their columns are of the
        // error type, so an order key naming one, or repeating one, is too.
        let mut refused_items: Vec<&Projection> = Vec::new();
        let mut refused_columns: Vec<String> = Vec::new();
        for (i, p) in body.projections.iter().enumerate() {
            if let Some(span) = body.spans.items.get(i) {
                self.set_at(span);
            }
            let name = item_name(&p.expr, p.alias.as_deref());
            if seen.contains(&name) {
                let refused = Err(err(
                    T25,
                    format!("two return items produce the column `{name}`; alias one"),
                ));
                self.guard::<()>(top, refused)?;
                sc.refused = true;
                continue;
            }
            seen.push(name.clone());
            let entered = self.enter("the return item", body.spans.items.get(i));
            let typed = self.return_item(&mut sc, p, &name, &mut items, &mut aggs, &mut post);
            self.work.leave(entered);
            if self.guard(top, typed)?.is_none() {
                sc.block_buf.clear();
                sc.refused = true;
                refused_items.push(p);
                refused_columns.push(name);
                continue;
            }
            names.push(name);
            shaped.push(
                (p.alias.is_none() && !matches!(p.expr, Expr::Variable(_)))
                    .then(|| crate::query::syntax::print::expr(&p.expr)),
            );
        }
        // An order key of the error type: it names a refused item's column
        // or repeats a refused item.
        let key_refused = |o: &Ordering| {
            matches!(&o.expr, Expr::Ident(alias) if refused_columns.contains(alias))
                || refused_items.iter().any(|p| p.expr == o.expr)
        };
        // Blocks in return items come before the extension.
        let blocks = std::mem::take(&mut sc.block_buf);
        let at = sc.binders;
        if !blocks.is_empty() {
            sc.push_filter(
                at,
                Step::Filter {
                    blocks,
                    pred: SExpr::new(
                        SKind::Lit(Literal::Bool(true)),
                        Ty::scalar(ScalarType::Bool, false),
                    ),
                },
            );
        }
        let mut term = match self.finish(&mut sc)? {
            Some(term) => term,
            None => {
                self.charge(Built {
                    terms: 1,
                    ..Built::default()
                })?;
                Term::Unit
            }
        };
        // A refused item holding an aggregate grouped the return too.
        let grouped = !aggs.is_empty()
            || !post.is_empty()
            || body.distinct
            || refused_items.iter().any(|p| contains_agg(&p.expr));
        if body.distinct && !aggs.is_empty() {
            self.guard::<()>(
                top,
                Err(err(
                    T56,
                    "`return distinct` does not apply to an aggregated return",
                )),
            )?;
        }
        let mut out_cols: Vec<Col> = Vec::new();
        let ext_names: Vec<String>;
        if grouped {
            let keys: Vec<(String, SExpr)> = items.iter().flatten().cloned().collect();
            for (i, it) in items.iter().enumerate() {
                let ty = match it {
                    Some((_, e)) => e.ty.clone(),
                    None => {
                        let n = self.item(&names[i]);
                        match aggs.iter().find(|(a, _)| *a == n) {
                            Some((_, a)) => a.ty.clone(),
                            None => post.iter().find(|(a, _)| *a == n).unwrap().1.ty.clone(),
                        }
                    }
                };
                out_cols.push(Col {
                    name: names[i].clone(),
                    ty,
                });
            }
            ext_names = names.iter().map(|n| self.item(n)).collect();
            // Order keys over the groups: a key item's expression, or an
            // alias.
            let mut order = Vec::new();
            for (k, o) in body.order.iter().enumerate() {
                if let Some(span) = body.spans.order.get(k) {
                    self.set_at(span);
                }
                if key_refused(o) {
                    continue;
                }
                let key = self.grouped_order_key(&mut sc, body, o, &names, &out_cols, &aggs, &keys);
                let Some(k) = self.guard(top, key)? else {
                    sc.refused = true;
                    continue;
                };
                order.push((k, o.descending.unwrap_or(false), o.nulls_first));
            }
            let keys: Vec<(String, IRExpr)> =
                keys.into_iter().map(|(n, k)| (n, k.to_ir())).collect();
            // An expression over aggregates runs after the `Group`, where
            // only its keys and aggregates exist: a match column it reads is
            // the key that returns it, and one no key returns is refused.
            let mut post_items = Vec::new();
            for (n, e) in post {
                let e = key_columns(e.to_ir(), &keys);
                let mut read = Vec::new();
                e.columns(&mut read);
                if let Some(c) = read.iter().find(|c| {
                    !self.made_up.is(c, FreshRole::Item) && !keys.iter().any(|(k, _)| k == *c)
                }) {
                    // A refused item may be the key that groups by it.
                    sc.refused = true;
                    if !refused_items.is_empty() {
                        continue;
                    }
                    let refused = Err(err(
                        T9,
                        format!(
                            "`{}` reads `${c}` beside an aggregate, and no return item groups by it; return the column as an item, or aggregate it",
                            n.trim_start_matches('^')
                        ),
                    ));
                    self.guard::<()>(top, refused)?;
                    continue;
                }
                post_items.push((n, e));
            }
            term = Term::Group {
                input: term.boxed(),
                keys: keys.clone(),
                aggs,
            };
            self.charge_top(&term)?;
            if !post_items.is_empty() {
                term = Term::Project {
                    input: term.boxed(),
                    fails: post_items.iter().any(|(_, e)| e.can_fail()),
                    items: post_items,
                    keep: true,
                };
                self.charge_top(&term)?;
            }
            if !order.is_empty() {
                term = Term::Order {
                    input: term.boxed(),
                    keys: order
                        .into_iter()
                        .map(|(k, descending, nulls_first)| IROrdering {
                            expr: k.to_ir(),
                            descending,
                            nulls_first,
                        })
                        .collect(),
                };
                self.charge_top(&term)?;
            }
        } else {
            let ext: Vec<(String, SExpr)> = items.into_iter().flatten().collect();
            for (n, e) in &ext {
                out_cols.push(Col {
                    name: n[1..].to_string(),
                    ty: e.ty.clone(),
                });
            }
            ext_names = ext.iter().map(|(n, _)| n.clone()).collect();
            let mut order = Vec::new();
            for (k, o) in body.order.iter().enumerate() {
                if let Some(span) = body.spans.order.get(k) {
                    self.set_at(span);
                }
                if key_refused(o) {
                    continue;
                }
                let key = match &o.expr {
                    Expr::Ident(alias) => match ext.iter().find(|(n, _)| n[1..] == **alias) {
                        Some((n, e)) => Ok(SExpr::new(SKind::Col(n.clone()), e.ty.clone())),
                        None => Err(err(
                            T42,
                            format!(
                                "`{alias}` in `order` names no return alias{}",
                                nearest::listing(
                                    alias,
                                    ext.iter()
                                        .map(|(n, e)| (n[1..].to_string(), e.ty.display()))
                                        .collect()
                                )
                            ),
                        )),
                    },
                    e => self.order_key(&mut sc, e),
                };
                let Some(k) = self.guard(top, key)? else {
                    sc.refused = true;
                    continue;
                };
                order.push((k, o.descending.unwrap_or(false), o.nulls_first));
            }
            term = Term::Project {
                input: term.boxed(),
                fails: ext.iter().any(|(_, e)| e.can_fail),
                items: ext.into_iter().map(|(n, e)| (n, e.to_ir())).collect(),
                keep: true,
            };
            self.charge_top(&term)?;
            if !order.is_empty() {
                term = Term::Order {
                    input: term.boxed(),
                    keys: order
                        .into_iter()
                        .map(|(k, descending, nulls_first)| IROrdering {
                            expr: k.to_ir(),
                            descending,
                            nulls_first,
                        })
                        .collect(),
                };
                self.charge_top(&term)?;
            }
        }
        if let Some(limit) = &body.limit {
            if let Some(span) = &body.spans.limit {
                self.set_at(span);
            }
            let k = match limit {
                Limit::Literal(n) => Ok(Count::Literal(*n)),
                Limit::Param(p) => match sc.params.get(p) {
                    None => Err(err(T6, format!("`limit ${p}` names no parameter"))),
                    Some(t) if t.list || !is_integer(t.scalar) => Err(err(
                        T7,
                        format!(
                            "`limit ${p}` needs an integer parameter, got {}",
                            t.display_name()
                        ),
                    )),
                    Some(_) => Ok(Count::Param(p.clone())),
                },
            };
            if let Some(k) = self.guard(top, k)? {
                term = Term::Cut {
                    input: term.boxed(),
                    k,
                };
                self.charge_top(&term)?;
            }
        }
        // An inline read holding a refusal is of the error type; the top
        // read body keeps the term of what it typed, which the lowering
        // admission reads ([`check_decl`]).
        if sc.inner && (self.refusals.len() > mark || sc.refused) {
            return Err(E::Recorded);
        }
        term = Term::Project {
            input: term.boxed(),
            items: ext_names
                .iter()
                .zip(&out_cols)
                .map(|(n, c)| {
                    (
                        c.name.clone(),
                        SExpr::new(SKind::Col(n.clone()), c.ty.clone()).to_ir(),
                    )
                })
                .collect(),
            keep: false,
            fails: false,
        };
        self.charge_top(&term)?;
        let ordered_ranked = if body.order.is_empty() {
            None
        } else {
            let nodes: Vec<&Col> = out_cols
                .iter()
                .filter(|c| matches!(c.ty, Ty::Node { .. }))
                .collect();
            match nodes.as_slice() {
                [one] => Some(Ranked {
                    column: one.name.clone(),
                    identity: None,
                }),
                _ => None,
            }
        };
        Ok(Rel {
            term,
            ty: RelTy { cols: out_cols },
            shaped,
            ranked: ordered_ranked,
            score: None,
            free: sc.free.clone(),
        })
    }

    /// One return item: an aggregate, an expression over aggregates, or a
    /// value, pushed where [`Ctx::read_body`] reads it.
    #[allow(clippy::too_many_arguments)]
    fn return_item(
        &mut self,
        sc: &mut Scope,
        p: &Projection,
        name: &str,
        items: &mut Vec<Option<(String, SExpr)>>,
        aggs: &mut Vec<(String, Agg)>,
        post: &mut Vec<(String, SExpr)>,
    ) -> R<()> {
        if let Some(agg) = self.aggregate(sc, &p.expr)? {
            aggs.push((self.item(name), agg));
            items.push(None);
        } else if contains_agg(&p.expr) {
            // An expression over aggregates (`count($p) > 3 as big`): the
            // aggregates are hidden `Group` columns, the expression a
            // projection after the group.
            let mut hidden = Vec::new();
            let rewritten = self.extract_aggs(sc, &p.expr, &mut hidden)?;
            for (n, a) in &hidden {
                sc.push_col(Col {
                    name: n.clone(),
                    ty: a.ty.clone(),
                });
            }
            let e = self.expr(sc, &rewritten, At::Value)?;
            if p.alias.is_none() {
                return Err(err(
                    T43,
                    "an expression over aggregates in return needs an alias",
                ));
            }
            aggs.extend(hidden);
            post.push((self.item(name), e));
            items.push(None);
        } else {
            let e = self.expr(sc, &p.expr, At::Value)?;
            if matches!(e.ty, Ty::Value(ref t) if t.scalar == ScalarType::Blob) {
                return Err(err(T24, format!("a Blob is not a read value (`{name}`)")));
            }
            if matches!(e.ty, Ty::Value(ref t) if t.scalar == ScalarType::Bool)
                && p.alias.is_none()
                && !e.is_reference_read()
                && !matches!(e.kind, SKind::Col(_) | SKind::Lit(_))
            {
                return Err(err(
                    T43,
                    "a comparison in return needs an alias; write `… as <name>`",
                ));
            }
            items.push(Some((self.item(name), e)));
        }
        Ok(())
    }

    /// An order key over grouped rows: a return item's alias, a return item's
    /// expression, or an aggregate one repeats.
    #[allow(clippy::too_many_arguments)]
    fn grouped_order_key(
        &mut self,
        sc: &mut Scope,
        body: &ReadBody,
        o: &Ordering,
        names: &[String],
        out_cols: &[Col],
        aggs: &[(String, Agg)],
        keys: &[(String, SExpr)],
    ) -> R<SExpr> {
        let same_item = body.projections.iter().position(|p| p.expr == o.expr);
        Ok(match &o.expr {
            Expr::Ident(alias) if names.contains(alias) => {
                let i = names.iter().position(|n| n == alias).unwrap();
                SExpr::new(SKind::Col(self.item(alias)), out_cols[i].ty.clone())
            }
            _ if same_item.is_some() => {
                let i = same_item.unwrap();
                SExpr::new(SKind::Col(self.item(&names[i])), out_cols[i].ty.clone())
            }
            e if self.probe_aggregate(sc, e) => {
                let a = self.aggregate(sc, e)?.unwrap();
                match aggs.iter().find(|(_, x)| *x == a) {
                    Some((n, x)) => SExpr::new(SKind::Col(n.clone()), x.ty.clone()),
                    None => {
                        return Err(err(
                            T42,
                            "an aggregate order key must repeat a return item; order by its alias",
                        ));
                    }
                }
            }
            e if contains_agg(e) => {
                return Err(err(
                    T42,
                    "an aggregate order key must repeat a return item; order by its alias",
                ));
            }
            e => {
                let typed = self.order_key(sc, e)?;
                match keys.iter().find(|(_, k)| *k == typed) {
                    Some((n, k)) => SExpr::new(SKind::Col(n.clone()), k.ty.clone()),
                    None => {
                        return Err(err(
                            T42,
                            "an order key over grouped rows must be a return item or name its alias",
                        ));
                    }
                }
            }
        })
    }

    fn order_key(&mut self, sc: &mut Scope, e: &Expr) -> R<SExpr> {
        let k = self.expr(sc, e, At::Value)?;
        if !sc.block_buf.is_empty() {
            return Err(err(
                T42,
                "a block cannot be an order key; bind it with `let`",
            ));
        }
        Ok(k)
    }

    /// `e` with every aggregate call replaced by a hidden column it pushes to
    /// `hidden`.
    fn extract_aggs(
        &mut self,
        sc: &mut Scope,
        e: &Expr,
        hidden: &mut Vec<(String, Agg)>,
    ) -> R<Expr> {
        crate::stack::deep(|| {
            if let Some(a) = self.aggregate(sc, e)? {
                let n = self.fresh(FreshRole::Item);
                hidden.push((n.clone(), a));
                return Ok(Expr::Variable(n));
            }
            let b = |x: &Expr,
                     this: &mut Self,
                     sc: &mut Scope,
                     h: &mut Vec<(String, Agg)>|
             -> R<Box<Expr>> { Ok(Box::new(this.extract_aggs(sc, x, h)?)) };
            Ok(match e {
                Expr::Compare {
                    op,
                    left,
                    right,
                    at,
                } => Expr::Compare {
                    op: *op,
                    left: b(left, self, sc, hidden)?,
                    right: b(right, self, sc, hidden)?,
                    at: at.clone(),
                },
                Expr::Arith { op, left, right } => Expr::Arith {
                    op: *op,
                    left: b(left, self, sc, hidden)?,
                    right: b(right, self, sc, hidden)?,
                },
                Expr::And(l, r) => Expr::And(b(l, self, sc, hidden)?, b(r, self, sc, hidden)?),
                Expr::Or(l, r) => Expr::Or(b(l, self, sc, hidden)?, b(r, self, sc, hidden)?),
                Expr::Not(x) => Expr::Not(b(x, self, sc, hidden)?),
                Expr::Neg(x) => Expr::Neg(b(x, self, sc, hidden)?),
                Expr::IsNull { expr, negated } => Expr::IsNull {
                    expr: b(expr, self, sc, hidden)?,
                    negated: *negated,
                },
                Expr::In { needle, list, at } => Expr::In {
                    needle: b(needle, self, sc, hidden)?,
                    list: b(list, self, sc, hidden)?,
                    at: at.clone(),
                },
                Expr::Call(c) => {
                    let mut c = c.clone();
                    for a in &mut c.args {
                        if let Arg::Expr(x) = a {
                            *x = self.extract_aggs(sc, x, hidden)?;
                        }
                    }
                    Expr::Call(c)
                }
                other => other.clone(),
            })
        })
    }

    /// Whether `e` types as an aggregate, recording nothing.
    fn probe_aggregate(&mut self, sc: &mut Scope, e: &Expr) -> bool {
        let mark = self.refusals.len();
        let recover = std::mem::replace(&mut sc.recover, false);
        let found = self.aggregate(sc, e).is_ok_and(|a| a.is_some());
        sc.recover = recover;
        self.refusals.truncate(mark);
        found
    }

    /// An aggregate return item, or `None`.
    fn aggregate(&mut self, sc: &mut Scope, e: &Expr) -> R<Option<Agg>> {
        let (func, distinct, arg, fraction) = match e {
            Expr::Call(c) if AGGREGATES.contains(&c.name.as_str()) => {
                let func = agg_func(&c.name);
                let arity = if func.takes_fraction() { 2 } else { 1 };
                if !c.options.is_empty() || c.args.len() != arity {
                    return Err(err(
                        T8,
                        if func.takes_fraction() {
                            format!("`{func}` takes two arguments: `{func}($x.p, 0.5)`")
                        } else {
                            format!("`{}` takes one argument", c.name)
                        },
                    ));
                }
                let [Arg::Expr(a), rest @ ..] = c.args.as_slice() else {
                    return Err(err(
                        T69,
                        format!(
                            "`{}` aggregates a column; an inline read is a relation",
                            c.name
                        ),
                    ));
                };
                let fraction = match rest {
                    [] => None,
                    [Arg::Expr(p)] => Some(p),
                    _ => return Err(err(T8, format!("`{func}`'s fraction is a number"))),
                };
                (func, false, a, fraction)
            }
            Expr::AggDistinct {
                func,
                arg,
                fraction,
            } => (*func, true, arg.as_ref(), fraction.as_deref()),
            _ => return Ok(None),
        };
        if distinct && !matches!(func, AggFunc::Count | AggFunc::Collect) {
            return Err(err(T57, "distinct applies only to count and collect"));
        }
        if contains_agg(arg) {
            return Err(err(T8, format!("{func} cannot take an aggregate")));
        }
        let a = self.expr(sc, arg, At::Value)?;
        let ty = agg_type(func, &a.ty)?;
        let fraction = self.agg_fraction(sc, func, fraction)?;
        Ok(Some(Agg {
            func,
            distinct,
            arg: Some(a.to_ir()),
            fraction,
            ty,
        }))
    }

    /// `percentile`'s fraction `p`: a numeric literal in [0, 1], kept as an
    /// `F64` literal, or a declared numeric parameter, whose value the
    /// engine checks against [0, 1] before the read reads a row (`E5`). Any
    /// other aggregate takes none.
    fn agg_fraction(
        &mut self,
        sc: &mut Scope,
        func: AggFunc,
        written: Option<&Expr>,
    ) -> R<Option<IRExpr>> {
        let p = match (func.takes_fraction(), written) {
            (false, None) => return Ok(None),
            (false, Some(_)) => return Err(err(T8, format!("`{func}` takes one argument"))),
            (true, None) => {
                return Err(err(
                    T8,
                    format!("`{func}` takes two arguments: `{func}($x.p, 0.5)`"),
                ));
            }
            (true, Some(p)) => p,
        };
        let typed = self.expr(sc, p, At::Value)?;
        let numeric = matches!(&typed.ty, Ty::Value(t) if !t.list && t.scalar.is_numeric());
        let literal = |kind: &SKind| match kind {
            SKind::Lit(Literal::Integer(n)) => Some(*n as f64),
            SKind::Lit(Literal::Float(f)) => Some(*f),
            _ => None,
        };
        let value = match &typed.kind {
            SKind::Param(_) if numeric => return Ok(Some(typed.to_ir())),
            SKind::Neg(inner) => literal(&inner.kind).map(|v| -v),
            kind => literal(kind),
        };
        match value {
            // `-0` is `0`: the fraction is a value, not a spelling.
            Some(v) if (0.0..=1.0).contains(&v) => Ok(Some(IRExpr::Literal(
                Literal::Float(if v == 0.0 { 0.0 } else { v }),
                crate::types::ExprType::Value {
                    scalar: ScalarType::F64,
                    list: false,
                    nullable: false,
                },
            ))),
            Some(v) => Err(err(
                T8,
                format!("`{func}`'s fraction p is in [0, 1], got {v}"),
            )),
            None => Err(err(
                T8,
                format!(
                    "`{func}`'s fraction p is a number literal in [0, 1] or a numeric parameter, got {}",
                    typed.ty.display()
                ),
            )),
        }
    }

    /// Fold a scope's steps into its term, after the last pending filters.
    fn finish(&mut self, sc: &mut Scope) -> R<Option<Term>> {
        self.flush(sc, true)?;
        let mut steps = std::mem::take(&mut sc.steps);
        steps.sort_by_key(|(k, _)| *k);
        let env = if sc.free.is_empty() {
            None
        } else {
            let free: HashSet<&str> = sc.free.iter().map(String::as_str).collect();
            Some(Term::Env(
                sc.outer
                    .iter()
                    .filter(|c| free.contains(c.name.as_str()))
                    .cloned()
                    .collect(),
            ))
        };
        if env.is_some() {
            self.charge(Built {
                terms: 1,
                ..Built::default()
            })?;
        }
        let mut acc = env;
        // Every step's nodes are charged as it is applied, counting only the
        // nodes the step builds: a term it holds was charged when built.
        // Filters placed before every binder wait for the first leaf when
        // there is no `Env` to sit on.
        let mut early = Vec::new();
        for (k, step) in steps {
            if acc.is_none() && k == 0 && !matches!(step, Step::Leaf(_)) {
                early.push(step);
                continue;
            }
            acc = Some(self.charged(acc, step)?);
            if !early.is_empty() {
                for s in std::mem::take(&mut early) {
                    acc = Some(self.charged(acc, s)?);
                }
            }
        }
        for s in early {
            if acc.is_none() {
                self.charge(Built {
                    terms: 1,
                    ..Built::default()
                })?;
            }
            acc = Some(self.charged(acc.or(Some(Term::Unit)), s)?);
        }
        Ok(acc)
    }

    /// `step` applied to `acc`, and the nodes it built charged
    /// ([`Self::charge`]): neither `acc` nor a term the step holds is walked
    /// again.
    fn charged(&mut self, acc: Option<Term>, step: Step) -> R<Term> {
        let (term, built) = apply_counted(acc, step, &mut self.fail_memo);
        self.charge(built)?;
        Ok(term)
    }

    /// The elements of one scope in written order, then its `where`, one
    /// pending filter per top-level conjunct, each placed after the last
    /// binder it reads. A block in the `where` correlates on every element's
    /// names (section 3).
    fn pattern(&mut self, sc: &mut Scope, pattern: &Pattern) -> R<()> {
        // A scope reads its whole pattern, the patterns nested in it
        // included, a bounded number of times before typing it (the
        // lookahead, the product and membership checks, each filter's
        // names): charged here, since a pattern nested `d` deep is read
        // again at each level.
        let left = usize::try_from(self.work.remaining()).unwrap_or(usize::MAX);
        self.spend(WorkKind::Typing, SyntaxSize::of(pattern, left))?;
        if let Some(fix) = aggregate_filter_fix(self.catalog, pattern, self.in_write) {
            return Err(fix);
        }
        if let Some(refusal) = membership_triple(pattern, sc) {
            return Err(E::D(refusal));
        }
        lookahead(&pattern.elements, sc);
        let mut parts = components::Parts::default();
        let mut moves = OptionalMoves::default();
        // Each triple of an inner scope and the ends it introduced.
        let mut introduced: Vec<(usize, Vec<String>)> = Vec::new();
        for (i, c) in pattern.elements.iter().enumerate() {
            if let Some(span) = pattern.spans.elements.get(i) {
                self.set_at(span);
            }
            if sc.recover && sc.depends_on_refused(&element_names(c)) {
                // It names what a refused element would have bound: it is
                // of the error type.
                refused_element(sc, &mut parts, i, c);
                continue;
            }
            self.element_span = (pattern.spans.elements.len() == pattern.elements.len())
                .then(|| pattern.spans.elements.get(i).cloned())
                .flatten();
            if sc.block
                && let Element::Binding {
                    variable,
                    type_name,
                } = c
                && sc.local(variable).is_none()
                && let Some(outer) = sc.outer_col(variable)
            {
                return Err(E::D(block_rebinding(
                    pattern,
                    i,
                    sc.optional,
                    variable,
                    type_name,
                    &outer.ty,
                    self.binding_type(type_name).ok().as_ref(),
                )));
            }
            let before = sc.cols.len();
            let (steps, binders) = (sc.steps.len(), sc.binders);
            sc.reads.clear();
            let entered = self.enter(element_word(c), pattern.spans.elements.get(i));
            let typed = match c {
                Element::Traversal(t) => {
                    match optional_triple(self.catalog, sc, pattern, i, t, &mut moves) {
                        Some(d) => Err(E::D(d)),
                        None => self.element(sc, c),
                    }
                }
                _ => self.element(sc, c),
            };
            self.work.leave(entered);
            if self.guard(sc.recover, typed)?.is_none() {
                sc.block_buf.clear();
                for col in sc.truncate_cols(before) {
                    sc.bound_at.remove(&col.name);
                }
                sc.steps.truncate(steps);
                sc.binders = binders;
                refused_element(sc, &mut parts, i, c);
                continue;
            }
            let bound: Vec<String> = sc.cols[before..].iter().map(|c| c.name.clone()).collect();
            if let Element::Traversal(t) = c {
                let ends: Vec<String> = [&t.src, &t.dst]
                    .into_iter()
                    .filter(|end| *end != "_" && bound.contains(end))
                    .cloned()
                    .collect();
                introduced.push((i, ends));
            }
            parts.element(i, c, bound, std::mem::take(&mut sc.reads));
            self.flush(sc, false)?;
        }
        // A refused binding still declares its name, so `T84` names only
        // what no binding element introduces; a refused `from` drew columns
        // no one knows, any of which may be the name.
        if (sc.block || sc.inner) && !sc.opaque {
            let checked = unbound_triple_names(pattern, sc, &introduced);
            if self.guard(sc.recover, checked)?.is_none() {
                sc.refused = true;
            }
        }
        if let Some(predicate) = &pattern.predicate {
            let written = pattern
                .spans
                .predicate
                .as_ref()
                .map(|s| (s.position, s.end_position));
            if written.is_some() {
                self.at = written;
            }
            // One set of the names bound before the `where`, shared by its
            // conjuncts.
            let before: std::rc::Rc<HashSet<String>> =
                std::rc::Rc::new(sc.visible().into_iter().map(|c| c.name).collect());
            for conj in conjuncts(predicate) {
                sc.pending.push(Pending::Filter(
                    conj.clone(),
                    std::rc::Rc::clone(&before),
                    written,
                ));
            }
            let entered = self.enter("the `where`", pattern.spans.predicate.as_ref());
            let flushed = self.flush(sc, false);
            self.work.leave(entered);
            flushed?;
        }
        // The product rule is a typing rule: a refused element is of the
        // error type and passes it, joining the components it names, and a
        // refused conjunct passes the key-pin check.
        if let Some(refusal) = parts.refusal(self.catalog, sc, pattern, &self.made_up) {
            self.guard::<()>(sc.recover, Err(E::D(refusal)))?;
        }
        Ok(())
    }

    /// Place every pending filter whose variables are bound (all of them at
    /// the end of a scope).
    fn flush(&mut self, sc: &mut Scope, all: bool) -> R<()> {
        // A filter's reads belong to no element: `where` connects no
        // component.
        let reads = std::mem::take(&mut sc.reads);
        let pending = std::mem::take(&mut sc.pending);
        for p in pending {
            let vars = pending_vars(&p, sc);
            let ready = vars
                .iter()
                .all(|v| sc.is_bound(v) || (!sc.future.contains(v)));
            if !(ready || all) {
                sc.pending.push(p);
                continue;
            }
            // A conjunct is written after every element of its scope, and
            // there it stays: the planner's placement law moves the ones
            // that may move (A3).
            let at = sc.binders;
            match p {
                Pending::Filter(e, before, written) => {
                    // An error in a scope that does not recover abandons it, so
                    // the restriction needs no reset on that path.
                    sc.block_visible = Some(before);
                    for conj in conjuncts(&e) {
                        // A conjunct is placed at its own first span, the
                        // `where` when it has none.
                        match first_span(conj) {
                            Some(span) => self.set_at(&span),
                            None if written.is_some() => self.at = written,
                            None => {}
                        }
                        let typed = self
                            .expr(sc, conj, At::Filter)
                            .and_then(|pred| require_bool(&pred, "a filter").map(|()| pred));
                        let Some(pred) = self.guard(sc.recover, typed)? else {
                            sc.block_buf.clear();
                            sc.refused = true;
                            sc.loose_pins.extend(expr_names(conj));
                            continue;
                        };
                        let blocks = std::mem::take(&mut sc.block_buf);
                        sc.push_filter(at, Step::Filter { blocks, pred });
                    }
                    sc.block_visible = None;
                }
            }
        }
        sc.reads = reads;
        Ok(())
    }

    fn element(&mut self, sc: &mut Scope, c: &Element) -> R<()> {
        let written: Vec<&str> = match c {
            Element::Let { name, .. } => vec![name],
            Element::Binding { variable, .. } => vec![variable],
            Element::Traversal(t) => [Some(&t.src), Some(&t.dst), t.edge_binding.as_ref()]
                .into_iter()
                .flatten()
                .map(String::as_str)
                .collect(),
            _ => Vec::new(),
        };
        if let Some(name) = written.into_iter().find(|n| n.starts_with("__")) {
            return Err(err(
                T49,
                format!(
                    "variable `${name}`: names beginning with `__` are reserved for the compiler"
                ),
            ));
        }
        match c {
            Element::Binding {
                variable,
                type_name,
            } => self.binding(sc, variable, type_name),
            Element::Traversal(t) => self.traversal(sc, t),
            Element::Optional(pattern) => self.optional(sc, pattern),
            Element::Let { name, value } => self.let_clause(sc, name, value),
            Element::From { rel, score } => {
                self.flush(sc, false)?;
                let r = self.drawn_rel(sc, rel, score.as_deref())?;
                self.draw(sc, r)
            }
        }
    }

    fn binding(&mut self, sc: &mut Scope, var: &str, type_name: &str) -> R<()> {
        let ty = self.binding_type(type_name)?;
        if let Some(existing) = sc.local(var) {
            let same = existing.ty.same_base(&ty);
            if !same {
                return Err(err(
                    if matches!(existing.ty, Ty::Edge { .. }) || matches!(ty, Ty::Edge { .. }) {
                        T23
                    } else {
                        T50
                    },
                    format!(
                        "variable `${var}` is already bound to {}, cannot rebind to `{type_name}`",
                        existing.ty.display()
                    ),
                ));
            }
            return Ok(());
        }
        if sc.correlates
            && let Some(outer) = sc.outer_col(var).cloned()
        {
            // The probed block re-mentions an outer variable to correlate on it.
            if !outer.ty.same_base(&ty) {
                return Err(err(
                    if matches!(outer.ty, Ty::Edge { .. }) || matches!(ty, Ty::Edge { .. }) {
                        T23
                    } else {
                        T50
                    },
                    format!(
                        "variable `${var}` is bound to {} outside the block, cannot rebind to `{type_name}`",
                        outer.ty.display()
                    ),
                ));
            }
            sc.col(var);
            return Ok(());
        }
        if sc.inner && sc.outer_col(var).is_some() {
            return Err(err_fix(
                T68,
                format!(
                    "`${var}` is bound inside an inner read, but the enclosing scope already holds `${var}`; the inner binding would silently become a filter on the outer row"
                ),
                format!("bind another name inside, or read the outer `${var}` without binding it"),
            ));
        }
        if sc.params.contains_key(var) || sc.rels.contains_key(var) {
            return Err(err(
                T68,
                format!("`${var}` is already a parameter or a named relation"),
            ));
        }
        // A leaf is charged where it is built; the scope's fold charges
        // only the nodes over it.
        self.charge(Built {
            terms: 1,
            ..Built::default()
        })?;
        sc.push_binder(
            Step::Leaf(Term::Scan {
                var: var.to_string(),
                ty: ty.clone(),
            }),
            &[Col {
                name: var.to_string(),
                ty,
            }],
        );
        Ok(())
    }

    fn binding_type(&self, type_name: &str) -> R<Ty> {
        if self.catalog.interfaces.contains_key(type_name) {
            return Err(err(
                T1,
                format!(
                    "`{type_name}` is an interface; a binding names a node type, and this release does not support an interface population yet"
                ),
            ));
        }
        if self.catalog.query_node_type(type_name).is_some() {
            return Ok(Ty::Node {
                type_name: type_name.to_string(),
                nullable: false,
            });
        }
        if let Some(e) = self.catalog.lookup_edge_by_name(type_name)
            && e.name == type_name
        {
            return Ok(Ty::Edge {
                type_name: type_name.to_string(),
                nullable: false,
            });
        }
        Err(err(
            T1,
            format!(
                "unknown node type `{type_name}`{}",
                nearest::listing(type_name, nearest::binding_types(self.catalog))
            ),
        ))
    }

    fn traversal(&mut self, sc: &mut Scope, t: &Traversal) -> R<()> {
        let written = t;
        // Anonymous endpoints get fresh names.
        let mut t = t.clone();
        for end in [&mut t.src, &mut t.dst] {
            if end == "_" {
                *end = self.fresh(FreshRole::Anonymous);
            }
        }
        // `<e>` matches either direction, so its ends are read in one order,
        // the smaller written name first, whichever end is bound: the typed
        // term never depends on the order they were written in; lint names
        // the other order (`L207`, section 6).
        if t.undirected && written.dst < written.src {
            std::mem::swap(&mut t.src, &mut t.dst);
        }
        for end in [&t.src, &t.dst] {
            if let Some(c) = sc.local(end).or_else(|| sc.outer_col(end))
                && matches!(c.ty, Ty::Value(_))
            {
                return Err(err(
                    T23,
                    format!(
                        "`${end}` is a {} column; a relation with no node column has nothing to traverse from",
                        c.ty.display()
                    ),
                ));
            }
        }
        // The two ends as the scope binds them: a lookup each, never a
        // scan of every column in scope, so a pattern's triples are typed
        // in time linear in their number.
        let mut bindings: HashMap<String, BoundVariable> = HashMap::new();
        for end in [&t.src, &t.dst] {
            match sc.local(end).or_else(|| sc.outer_col(end)).map(|c| &c.ty) {
                Some(Ty::Node { type_name, .. }) => {
                    bindings.insert(
                        end.clone(),
                        BoundVariable::Node {
                            type_name: type_name.clone(),
                        },
                    );
                }
                Some(Ty::Edge { .. }) => {
                    bindings.insert(end.clone(), BoundVariable::Edge);
                }
                Some(Ty::Value(_)) | None => {}
            }
        }
        let catalogue = match &t.selector {
            EdgeSelector::Named(n) => is_catalogue(n),
            EdgeSelector::Alternation(ns) => ns.iter().any(|n| is_catalogue(n)),
            EdgeSelector::Wildcard => false,
        };
        if catalogue {
            return Err(err(
                T4,
                "this release does not support a catalogue edge (`@property`, `@from`, …) yet",
            ));
        }
        let declared_node = |end: &String| {
            sc.declared_nodes.contains(end)
                || matches!(
                    sc.local(end).or_else(|| sc.outer_col(end)).map(|c| &c.ty),
                    Some(Ty::Node { .. })
                )
        };
        if matches!(t.selector, EdgeSelector::Wildcard)
            && (!declared_node(&t.src) || !declared_node(&t.dst))
        {
            return Err(err(
                T5,
                "wildcard traversal requires explicitly declared source and destination node bindings",
            ));
        }
        let resolved = resolve_traversal(self.catalog, &t, &bindings, &sc.declared)?;
        let mut resolved = resolved;
        if let Some(d) =
            reversed_triple(self.catalog, written, &resolved, self.element_span.as_ref())
        {
            if soundness::refuses(soundness::Claim::ReversedTriple) {
                return Err(E::D(d));
            }
            // Let through (a test-only switch): the written order is the
            // direction, so every member traverses from the written source.
            resolved.edges = resolved.edges.reversed();
        }
        if let Some(k) = &resolved.edge_binding {
            if *k == t.src || *k == t.dst {
                return Err(err(
                    T23,
                    format!(
                        "edge binding `${k}` cannot reuse a traversal endpoint name; edge bindings and node endpoints need distinct names"
                    ),
                ));
            }
            if sc.is_bound(k) {
                if (sc.inner || sc.block) && sc.outer_col(k).is_some() && sc.local(k).is_none() {
                    let scope = if sc.optional {
                        "an `optional`"
                    } else if sc.block {
                        "a block"
                    } else {
                        "an inner read"
                    };
                    return Err(err_fix(
                        T68,
                        format!("edge binding `${k}` inside {scope} reuses an outer name"),
                        "bind the edge under another name",
                    ));
                }
                return Err(err(
                    T23,
                    format!("variable `${k}` is already bound; an edge binding needs a fresh name"),
                ));
            }
        }
        let src_bound = sc.is_bound(&t.src);
        let dst_bound = sc.is_bound(&t.dst);
        if src_bound {
            sc.col(&t.src);
        }
        if dst_bound {
            sc.col(&t.dst);
        }
        let members: Vec<(String, Direction)> = resolved
            .edges
            .members()
            .iter()
            .map(|m| (m.edge_type.clone(), m.direction))
            .collect();
        let forward = resolved.edges.clone();
        let reverse = resolved.edges.reversed();
        let edge_col = resolved.edge_binding.clone().map(|k| Col {
            name: k,
            ty: Ty::Edge {
                type_name: members
                    .iter()
                    .map(|(e, _)| e.clone())
                    .collect::<Vec<_>>()
                    .join("|"),
                nullable: false,
            },
        });
        // Orientation: from the bound end; with neither bound, from the
        // edge's source for a directed member, the smaller name for `<e>`.
        let (from, to, to_type, sel, from_bound) = if src_bound {
            (
                t.src.clone(),
                t.dst.clone(),
                resolved.dst_type.clone(),
                forward,
                true,
            )
        } else if dst_bound {
            (
                t.dst.clone(),
                t.src.clone(),
                resolved.src_type.clone(),
                reverse.clone(),
                true,
            )
        } else {
            let flip = match members.as_slice() {
                [(_, Direction::In)] => true,
                [(_, Direction::Both)] => t.dst < t.src,
                _ => false,
            };
            if flip {
                (
                    t.dst.clone(),
                    t.src.clone(),
                    resolved.src_type.clone(),
                    reverse,
                    false,
                )
            } else {
                (
                    t.src.clone(),
                    t.dst.clone(),
                    resolved.dst_type.clone(),
                    forward,
                    false,
                )
            }
        };
        let from_type = if from == t.src {
            resolved.src_type.clone()
        } else {
            resolved.dst_type.clone()
        };
        let both = src_bound && dst_bound;
        let semi = both.then(|| self.fresh(FreshRole::Closing));
        let mut binds = Vec::new();
        if !both {
            binds.push(Col {
                name: to.clone(),
                ty: Ty::Node {
                    type_name: to_type.clone(),
                    nullable: false,
                },
            });
        }
        if let Some(e) = &edge_col {
            binds.push(e.clone());
        }
        let expand = Step::Expand {
            src: from.clone(),
            dst: to.clone(),
            src_type: from_type.clone(),
            dst_type: to_type.clone(),
            edges: sel,
            min_hops: resolved.min_hops,
            max_hops: resolved.max_hops,
            edge_var: resolved.edge_binding.clone(),
            semi,
        };
        if from_bound {
            sc.push_binder(expand, &binds);
        } else {
            // Neither end bound: an independent pattern, correlated in.
            let (leaf, mut built) = apply_counted(
                Some(Term::Scan {
                    var: from.clone(),
                    ty: Ty::Node {
                        type_name: from_type.clone(),
                        nullable: false,
                    },
                }),
                expand,
                &mut self.fail_memo,
            );
            built.terms += 1;
            self.charge(built)?;
            binds.insert(
                0,
                Col {
                    name: from,
                    ty: Ty::Node {
                        type_name: from_type,
                        nullable: false,
                    },
                },
            );
            sc.push_binder(Step::Leaf(leaf), &binds);
        }
        Ok(())
    }

    fn optional(&mut self, sc: &mut Scope, pattern: &Pattern) -> R<()> {
        self.flush(sc, false)?;
        let before: Vec<String> = sc.visible().iter().map(|c| c.name.clone()).collect();
        let mut inner = self.inner_scope(sc, sc.visible())?;
        inner.inner = false;
        inner.block = true;
        inner.optional = true;
        let mark = self.refusals.len();
        self.pattern(&mut inner, pattern)?;
        // An inner scope holding a refusal is of the error type: what reads
        // its names depends on it.
        if self.refusals.len() > mark || inner.refused {
            return Err(E::Recorded);
        }
        let new_cols: Vec<Col> = inner
            .cols
            .iter()
            .filter(|c| !before.contains(&c.name))
            .cloned()
            .collect();
        let free = inner.free.clone();
        // The inner term's `Env`, as `finish` builds it.
        let env: Vec<Col> = inner
            .outer
            .iter()
            .filter(|c| free.contains(&c.name))
            .cloned()
            .collect();
        let term = self.finish(&mut inner)?.unwrap_or(Term::Unit);
        for f in &free {
            sc.col(f);
        }
        let nullable: Vec<Col> = new_cols
            .iter()
            .map(|c| Col {
                name: c.name.clone(),
                ty: c.ty.with_nullable(true),
            })
            .collect();
        sc.push_binder(
            Step::Optional {
                inner: term,
                new_cols: new_cols.clone(),
                env,
            },
            &nullable,
        );
        Ok(())
    }

    fn let_clause(&mut self, sc: &mut Scope, name: &str, value: &LetValue) -> R<()> {
        self.flush(sc, false)?;
        if sc.in_scope(name) {
            return Err(err_fix(
                T68,
                format!("`let ${name}` names a value already in scope"),
                "choose another name",
            ));
        }
        let rel = match value {
            LetValue::Relation(body) => Some(self.rel_expr(sc, &RelArg::Inline(body))?),
            LetValue::Expr(e) if is_relation_expr(e, sc, self.unit) => {
                Some(self.rel_expr(sc, &RelArg::Expr(e))?)
            }
            LetValue::Expr(_) => None,
        };
        if let Some(r) = rel {
            // A bound relation was charged as it was built, drawn or not.
            sc.rels.insert(name.to_string(), r);
            return Ok(());
        }
        let LetValue::Expr(e) = value else {
            unreachable!()
        };
        let v = self.expr(sc, e, At::Value)?;
        let blocks = std::mem::take(&mut sc.block_buf);
        let ty = v.ty.clone();
        sc.push_binder(
            Step::Let {
                blocks,
                name: name.to_string(),
                value: v,
            },
            &[Col {
                name: name.to_string(),
                ty,
            }],
        );
        Ok(())
    }

    /// `from rel`: the relation's columns enter scope by name.
    fn draw(&mut self, sc: &mut Scope, r: Rel) -> R<()> {
        for (c, written) in r.ty.cols.iter().zip(&r.shaped) {
            if let Some(written) = written {
                return Err(err_fix(
                    T74,
                    format!(
                        "the return item `{written}` produces the column `{}`, which is not variable-shaped and cannot be drawn into scope",
                        c.name
                    ),
                    format!(
                        "alias it on the inner `return`: `{written} as {}`",
                        alias_for(written)
                    ),
                ));
            }
            if sc.in_scope(&c.name) {
                return Err(err_fix(
                    T68,
                    format!(
                        "`from` draws the column `{0}` into a scope that already holds `${0}`",
                        c.name
                    ),
                    format!("alias it on the inner `return`: `${0} as {0}2`", c.name),
                ));
            }
        }
        for f in &r.free {
            sc.col(f);
        }
        let cols = r.ty.cols.clone();
        sc.push_binder(Step::Leaf(r.term), &cols);
        Ok(())
    }

    /// `T68` for a block whose only element binds a name the scope where the
    /// block is written holds, with the same type: a re-mention, which once
    /// correlated the block on the outer row and so filtered it. An `exists` block of that shape is the
    /// test that its `where` is true, so the fix writes the predicate in the
    /// block's place (`not exists { $p: Person where $p.name = "bob" }` is
    /// `not ($p.name = "bob")`). Where the predicate can be null the block's
    /// test was false and the conjunct would be null, so `not exists` kept a
    /// row that `not ( … )` drops; the fix writes `coalesce(…, false)` there,
    /// which keeps every row. The block is typed with the re-mention
    /// correlating to learn that; a refusal inside it is returned first,
    /// since it is the block's own. The lexical predicate `match_terms` is a
    /// Boolean operand like any other, so a block filtering on one is written
    /// the same way, `not (match_terms($d.title, terms($q, mode: any)))`.
    fn re_mention_only(&mut self, sc: &Scope, block: &Block, outer: &[Col]) -> R<()> {
        let pattern = &block.pattern;
        let [
            Element::Binding {
                variable,
                type_name,
            },
        ] = pattern.elements.as_slice()
        else {
            return Ok(());
        };
        let Some(outer_ty) = outer.iter().find(|c| c.name == *variable).map(|c| &c.ty) else {
            return Ok(());
        };
        match self.binding_type(type_name) {
            Ok(ty) if outer_ty.same_base(&ty) => {}
            _ => return Ok(()),
        }
        let first = pattern.spans.elements.first();
        let at = first.map(|s| s.position);
        let at_end = first.map(|s| s.end_position);
        let mut d = QueryDiagnostic::typecheck(
            T68,
            format!(
                "`${variable}` is bound inside a block, but the scope where the block is written already holds `${variable}` ({}); a block reads an outer name to correlate on it, so a block that only binds one to filter its row is that filter",
                outer_ty.display()
            ),
        );
        d.position = at;
        d.end = at_end;
        if block.func != BlockFunc::Exists {
            return Err(E::D(d.with_fix(format!(
                "read the outer `${variable}` without binding it inside the block, or write the filter as a conjunct of the `where`"
            ))));
        }
        let mut probe = self.inner_scope(sc, outer.to_vec())?;
        probe.recover = false;
        probe.inner = false;
        probe.correlates = true;
        self.pattern(&mut probe, pattern)?;
        self.flush(&mut probe, true)?;
        let nullable = probe.steps.iter().any(|(_, s)| match s {
            Step::Filter { pred, .. } => pred.ty.nullable(),
            _ => false,
        });
        self.at = at.zip(at_end);
        let span = &block.at;
        let (open, close) = if nullable {
            ("coalesce(", ", false)")
        } else {
            ("(", ")")
        };
        let (fix, edits) = match (&pattern.predicate, &pattern.spans.predicate) {
            (Some(p), Some(pspan)) if span.end > span.start => (
                format!("`{open}{}{close}`", crate::query::syntax::print::expr(p)),
                vec![
                    TextEdit {
                        start: span.start,
                        end: pspan.start,
                        replacement: open.to_string(),
                    },
                    TextEdit {
                        start: pspan.end,
                        end: span.end,
                        replacement: close.to_string(),
                    },
                ],
            ),
            (None, _) if span.end > span.start => (
                "`true`, since the outer row always exists".to_string(),
                vec![TextEdit {
                    start: span.start,
                    end: span.end,
                    replacement: "true".to_string(),
                }],
            ),
            _ => return Err(E::D(d.with_fix("write the block's `where` in its place"))),
        };
        let note = if nullable {
            ", which keeps the block's answer where the predicate is null"
        } else {
            ""
        };
        Err(E::D(
            d.with_fix(format!("write {fix} in the block's place{note}"))
                .with_suggestion(Suggestion {
                    applicability: Applicability::MachineApplicable,
                    edits,
                }),
        ))
    }

    /// A block's inner scope: its term (starting from `Env` when it reads an
    /// outer column) and its free variables.
    /// A block's inner scope, with its aggregate argument typed against the
    /// block's own bindings.
    fn block_scope_arg(
        &mut self,
        sc: &mut Scope,
        block: &Block,
    ) -> R<(Term, Vec<String>, Option<SExpr>)> {
        let (pattern, arg) = (&block.pattern, block.arg.as_deref());
        let outer: Vec<Col> = match &sc.block_visible {
            Some(before) => sc
                .visible()
                .into_iter()
                .filter(|c| before.contains(&c.name))
                .collect(),
            None => sc.visible(),
        };
        self.re_mention_only(sc, block, &outer)?;
        let mut inner = self.inner_scope(sc, outer)?;
        inner.inner = false;
        inner.block = true;
        let mark = self.refusals.len();
        self.pattern(&mut inner, pattern)?;
        self.flush(&mut inner, true)?;
        let arg = match arg {
            Some(a) => {
                let typed = self.expr(&mut inner, a, At::Value);
                self.guard(inner.recover, typed)?
            }
            None => None,
        };
        // A block holding a refusal is of the error type.
        if self.refusals.len() > mark || inner.refused || (block.arg.is_some() && arg.is_none()) {
            return Err(E::Recorded);
        }
        // A block in the argument correlates on the block's own rows: it
        // is placed in the block's term, as a return item's block is in its
        // read's, before the aggregate reads its column.
        let blocks = std::mem::take(&mut inner.block_buf);
        if !blocks.is_empty() {
            let at = inner.binders;
            inner.push_filter(
                at,
                Step::Filter {
                    blocks,
                    pred: SExpr::new(
                        SKind::Lit(Literal::Bool(true)),
                        Ty::scalar(ScalarType::Bool, false),
                    ),
                },
            );
        }
        let free = inner.free.clone();
        let term = self.finish(&mut inner)?.unwrap_or(Term::Unit);
        for f in &free {
            sc.col(f);
        }
        Ok((term, free, arg))
    }

    // -----------------------------------------------------------------------
    // Relation expressions.

    fn rel_expr(&mut self, sc: &mut Scope, arg: &RelArg<'_>) -> R<Rel> {
        match arg {
            RelArg::Rel(RelExpr::Inline(_)) | RelArg::Inline(_) => {
                let body: &ReadBody = match arg {
                    RelArg::Rel(RelExpr::Inline(b)) => b,
                    RelArg::Inline(b) => b,
                    _ => unreachable!(),
                };
                let mut inner = self.inner_scope(sc, sc.visible())?;
                inner.inner = true;
                let r = self.read_body(body, inner)?;
                for f in &r.free {
                    sc.col(f);
                }
                Ok(r)
            }
            RelArg::Rel(RelExpr::Variable(v)) => self.rel_var(sc, v),
            RelArg::Rel(RelExpr::Call(c)) => self.rel_call(sc, c),
            RelArg::Expr(Expr::Variable(v)) => self.rel_var(sc, v),
            RelArg::Expr(Expr::Call(c)) => self.rel_call(sc, c),
            RelArg::Expr(_) => Err(err_fix(
                T69,
                "a scalar is read where a relation is expected",
                "an inline read `{ match { … } return { … } }`, a named relation, or a relation call",
            )),
        }
    }

    /// The relation a `from` draws. A ranking's score enters scope under the
    /// name its `as` writes, and only there: `as` is required on a `from`
    /// whose relation's type carries an unnamed score, a ranking function's
    /// result or a named relation bound to one, and refused on any other
    /// (`T85`), an inline read that returns a named score included.
    fn drawn_rel(&mut self, sc: &mut Scope, rel: &RelExpr, score: Option<&str>) -> R<Rel> {
        // Typing the relation checks its own elements, so the `from`'s span
        // is taken first.
        let span = self.element_span.clone();
        // A ranking function written in the `from` names its score as it
        // ranks.
        let mut named_here = false;
        let mut r = match rel {
            RelExpr::Call(c) if matches!(c.name.as_str(), "distance" | "bm25" | "nearest") => {
                named_here = score.is_some();
                self.ranking(sc, c, score)?
            }
            RelExpr::Call(c) if c.name == "rrf" => {
                named_here = score.is_some();
                self.rrf(sc, c, score)?
            }
            _ => self.rel_expr(sc, &RelArg::Rel(rel))?,
        };
        let span = span.as_ref();
        match (r.score, score) {
            (None, None) => {}
            (None, Some(_)) if named_here => {}
            (Some(_), Some(name)) => rename_score(&mut r, name)?,
            (Some(at), None) => {
                let base = ranking_function(&r.term);
                let mut name = base.to_string();
                let mut n = 2;
                while sc.in_scope(&name)
                    || sc.future.contains(&name)
                    || r.ty
                        .cols
                        .iter()
                        .enumerate()
                        .any(|(i, c)| i != at && c.name == name)
                {
                    name = format!("{base}{n}");
                    n += 1;
                }
                // The relation typed: only its score's name is refused, so
                // it is drawn under the name the fix writes, and what it
                // holds is still checked.
                let refused = Err(E::D(unnamed_ranking(rel, &name, span)));
                self.guard::<()>(sc.recover, refused)?;
                sc.refused = true;
                rename_score(&mut r, &name)?;
            }
            (None, Some(name)) => {
                // The relation typed and the fix removes the `as`: the name
                // is of the error type.
                let refused = Err(E::D(named_non_ranking(rel, name, span)));
                self.guard::<()>(sc.recover, refused)?;
                sc.refused = true;
                sc.poisoned.insert(name.to_string());
            }
        }
        Ok(r)
    }

    fn rel_var(&mut self, sc: &mut Scope, v: &str) -> R<Rel> {
        if let Some(r) = sc.rels.get(v).cloned() {
            // The named relation's tree, charged at each draw: the expanded
            // tree holds it once per draw.
            self.charge_expansion(&r.term)?;
            for f in &r.free {
                sc.col(f);
            }
            return Ok(r);
        }
        if sc.is_error(v) {
            return Err(E::Recorded);
        }
        if sc.is_bound(v) || sc.params.contains_key(v) {
            return Err(err_fix(
                T69,
                format!("`${v}` is a scalar or a reference; `from` and a ranking read a relation"),
                format!("bind a relation: `let ${v}s = {{ match {{ … }} return {{ … }} }}`"),
            ));
        }
        Err(err(
            T6,
            format!(
                "`${v}` names no relation in scope{}",
                nearest::listing(
                    &format!("${v}"),
                    sc.rels
                        .keys()
                        .map(|r| (format!("${r}"), "relation".into()))
                        .collect()
                )
            ),
        ))
    }

    fn rel_call(&mut self, sc: &mut Scope, c: &Call) -> R<Rel> {
        match c.name.as_str() {
            "union" => self.union(sc, c),
            "distance" | "bm25" | "nearest" => self.ranking(sc, c, None),
            "rrf" => self.rrf(sc, c, None),
            name if self.unit.contains_key(name) => self.call(sc, c),
            name if is_registry_name(name) => Err(err_fix(
                T69,
                format!("`{name}` is a scalar registry row; a relation is expected here"),
                "an inline read, a declared read or a relation function",
            )),
            name => Err(err(
                T6,
                format!(
                    "`{name}` is neither a relation function nor a declared read of this source"
                ),
            )),
        }
    }

    fn arg_rel(&mut self, sc: &mut Scope, a: &Arg) -> R<Rel> {
        match a {
            Arg::Inline(body) => self.rel_expr(sc, &RelArg::Inline(body)),
            Arg::Expr(e) => self.rel_expr(sc, &RelArg::Expr(e)),
        }
    }

    fn union(&mut self, sc: &mut Scope, c: &Call) -> R<Rel> {
        if let Some((o, _)) = c.options.first() {
            return Err(err(T71, format!("`union` takes no option; got `{o}`")));
        }
        if c.args.len() < 2 {
            return Err(err(T73, "`union` takes two or more relations"));
        }
        let mut arms = Vec::new();
        let mut pending = None;
        for a in &c.args {
            match self.arg_rel(sc, a) {
                Ok(r) => arms.push(unnamed_score(r)),
                Err(E::Pending(n)) => pending = Some(n),
                Err(e) => return Err(e),
            }
        }
        if arms.is_empty() {
            return Err(E::Pending(pending.unwrap()));
        }
        // The arms match by column name (the GQ 3 RFC, section 3): the
        // same set of names, base types agreeing per name, a column
        // nullable when any arm's is, column order from the first arm.
        let mut acc = arms.remove(0);
        for (i, arm) in arms.into_iter().enumerate() {
            let has = |rel: &Rel, name: &str| rel.ty.cols.iter().any(|c| c.name == name);
            let missing: Vec<&str> = acc
                .ty
                .cols
                .iter()
                .map(|c| c.name.as_str())
                .filter(|n| !has(&arm, n))
                .collect();
            let extra: Vec<&str> = arm
                .ty
                .cols
                .iter()
                .map(|c| c.name.as_str())
                .filter(|n| !has(&acc, n))
                .collect();
            if !missing.is_empty() || !extra.is_empty() {
                let list = |names: &[&str]| {
                    names
                        .iter()
                        .map(|n| format!("`{n}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let mut message = format!("union arm {}", i + 2);
                if !missing.is_empty() {
                    message.push_str(&format!(" has no column {}", list(&missing)));
                }
                if !extra.is_empty() {
                    message.push_str(&format!(
                        "{} a column {} the first arm does not have",
                        if missing.is_empty() { " has" } else { " and" },
                        list(&extra)
                    ));
                }
                message.push_str("; the arms of a `union` match by column name");
                // One unmatched column each way pairs up: alias it.
                let fix = if missing.len() == extra.len() {
                    missing
                        .iter()
                        .zip(&extra)
                        .map(|(want, have)| format!("return `{have}` as `{want}`: `… as {want}`"))
                        .collect::<Vec<_>>()
                        .join("; ")
                } else {
                    format!(
                        "return the columns {} from every arm, aliasing with `as`",
                        list(
                            &acc.ty
                                .cols
                                .iter()
                                .map(|c| c.name.as_str())
                                .collect::<Vec<_>>()
                        )
                    )
                };
                return Err(err_fix(T73, message, fix));
            }
            let mut cols = Vec::new();
            for a in &acc.ty.cols {
                let Some(b) = arm.ty.cols.iter().find(|c| c.name == a.name) else {
                    unreachable!("both arms hold every name (checked above)")
                };
                if !a.ty.same_base(&b.ty) {
                    return Err(err(
                        T73,
                        format!(
                            "union arm {}'s column `{}` is {}, the first arm's is {}",
                            i + 2,
                            a.name,
                            b.ty.display(),
                            a.ty.display()
                        ),
                    ));
                }
                cols.push(Col {
                    name: a.name.clone(),
                    ty: a.ty.with_nullable(a.ty.nullable() || b.ty.nullable()),
                });
            }
            let mut free = acc.free.clone();
            for f in arm.free {
                if !free.contains(&f) {
                    free.push(f);
                }
            }
            let term = Term::Union {
                left: acc.term.boxed(),
                right: arm.term.boxed(),
            };
            self.charge(Built {
                terms: 1,
                ..Built::default()
            })?;
            acc = Rel {
                term,
                ty: RelTy { cols },
                shaped: acc.shaped,
                ranked: None,
                score: None,
                free,
            };
        }
        acc.ranked = None;
        Ok(acc)
    }

    /// `distance`, `bm25` and `nearest`; `named` is the score's name when
    /// a `from` draws the ranking, `None` in a relation function's argument.
    fn ranking(&mut self, sc: &mut Scope, c: &Call, named: Option<&str>) -> R<Rel> {
        let name = c.name.as_str();
        let (positional, admitted): (usize, &[&str]) = match name {
            "nearest" => (3, &["k (integer)", "recall (F64 in (0, 1])"]),
            _ => (3, &[]),
        };
        let admitted_names: Vec<&str> = admitted
            .iter()
            .map(|a| a.split(' ').next().unwrap_or(a))
            .collect();
        for (o, _) in &c.options {
            if !admitted_names.contains(&o.as_str()) {
                return Err(err(
                    T71,
                    format!(
                        "`{name}` has no option `{o}`; it admits {}",
                        if admitted.is_empty() {
                            "none".to_string()
                        } else {
                            admitted.join(", ")
                        }
                    ),
                ));
            }
        }
        // `nearest`'s window written as a fourth positional argument: the
        // fix names it `k:` in place.
        if name == "nearest"
            && c.args.len() == positional + 1
            && c.at.end > c.at.start
            && !c.options.iter().any(|(o, _)| o == "k")
            && let Some(Arg::Expr(window)) = c.args.last()
        {
            let mut fixed = c.clone();
            fixed.args.pop();
            fixed.options.insert(0, ("k".to_string(), window.clone()));
            let replacement = crate::query::syntax::print::call(&fixed);
            let mut diagnostic = QueryDiagnostic::typecheck(
                T71,
                "`nearest`'s window is the option `k:`, not a fourth positional argument",
            )
            .with_fix(replacement.clone())
            .with_suggestion(Suggestion {
                applicability: Applicability::MachineApplicable,
                edits: vec![TextEdit {
                    start: c.at.start,
                    end: c.at.end,
                    replacement,
                }],
            });
            diagnostic.position = Some(c.at.position);
            diagnostic.end = Some(c.at.end_position);
            return Err(E::D(diagnostic));
        }
        if c.args.len() != positional {
            let fix = match name {
                "nearest" => "nearest(<population>, $x.field, $q, k: n)",
                "bm25" => "bm25(<population>, $x.field, $q)",
                _ => "distance(<population>, $x.field, $q)",
            };
            return Err(err_fix(
                if c.args.len() > positional { T71 } else { T15 },
                format!(
                    "`{name}` takes a population, a ranked column and a query{}",
                    if name == "nearest" { ", then `k:`" } else { "" }
                ),
                fix,
            ));
        }
        let pop = unnamed_score(self.arg_rel(sc, &c.args[0])?);
        let (column, property) = match &c.args[1] {
            Arg::Expr(Expr::Prop { variable, property }) => (variable.clone(), property.clone()),
            Arg::Expr(Expr::Ident(field)) => {
                let col = pop
                    .ty
                    .node_columns()
                    .first()
                    .map_or("x".to_string(), |c| c.name.clone());
                return Err(err_fix(
                    if name == "bm25" { T27 } else { T15 },
                    format!(
                        "`{name}` names its ranked column as a property access on a node column of its population, not a bare field `{field}`"
                    ),
                    format!("${col}.{field}"),
                ));
            }
            _ => {
                return Err(err(
                    T79,
                    format!(
                        "`{name}`'s second argument is a property access, `$x.field`, on a node column of the population"
                    ),
                ));
            }
        };
        let Some(col) = pop.ty.get(&column).cloned() else {
            let nodes: Vec<String> = pop
                .ty
                .node_columns()
                .iter()
                .map(|c| format!("${}", c.name))
                .collect();
            return Err(err(
                T79,
                format!(
                    "`${column}` is not a column of `{name}`'s population; its node columns are {}",
                    if nodes.is_empty() {
                        "none".to_string()
                    } else {
                        nodes.join(", ")
                    }
                ),
            ));
        };
        let Ty::Node { type_name, .. } = &col.ty else {
            return Err(err(
                T79,
                format!(
                    "`${column}` is {} in `{name}`'s population; a ranking ranks a node column",
                    col.ty.display()
                ),
            ));
        };
        let type_name = type_name.clone();
        let prop = match self
            .node_props(&type_name)
            .and_then(|p| p.get(&property).cloned())
        {
            Some(p) => p,
            None => {
                return Err(err(
                    if name == "bm25" { T27 } else { T15 },
                    format!(
                        "type `{type_name}` has no property `{property}` for `{name}` to rank{}",
                        self.node_props(&type_name)
                            .map(|p| nearest::listing(&property, nearest::properties(p)))
                            .unwrap_or_default()
                    ),
                ));
            }
        };
        let query_arg = match &c.args[2] {
            Arg::Expr(e) => e,
            Arg::Inline(_) => {
                return Err(err(
                    T69,
                    format!("`{name}`'s query is a scalar, not a relation"),
                ));
            }
        };
        let kind = match name {
            "bm25" => ScoreKind::Bm25,
            _ => ScoreKind::Distance,
        };
        // An unnamed score is read by no expression, so its column takes a
        // name no population column has.
        let out = match named {
            Some(out) => {
                if pop.ty.get(out).is_some() {
                    return Err(err_fix(
                        T68,
                        format!(
                            "`as ${out}` names the score after a column `{name}`'s population already has"
                        ),
                        "name the score another way: `as $<name>`",
                    ));
                }
                out.to_string()
            }
            None => unused_column(default_score_name(name), &pop.ty),
        };
        let out = out.as_str();
        let query = if name == "bm25" {
            if !matches!(prop.scalar, ScalarType::String) || prop.list {
                return Err(err(
                    T27,
                    format!(
                        "`bm25` ranks a String with a full-text index; `{type_name}.{property}` is {}",
                        prop.display_name()
                    ),
                ));
            }
            if !self
                .catalog
                .query_node_type(&type_name)
                .is_some_and(|n| n.is_lexical_field(&property))
            {
                return Err(err_fix(
                    T27,
                    format!("`bm25` needs a full-text index on `{type_name}.{property}`"),
                    format!("declare `{property}: String @index(fulltext)` on `{type_name}`"),
                ));
            }
            if self
                .catalog
                .query_node_type(&type_name)
                .is_some_and(|n| !n.full_text_scorer(&property).ranks())
            {
                return Err(err_fix(
                    T87,
                    format!(
                        "`bm25` ranks `{type_name}.{property}`, whose full-text index declares `scorer = \"none\"`: the field matches but does not rank"
                    ),
                    format!(
                        "match without a score, `match_terms(${}.{property}, terms(…))` in `where`, or declare `scorer = \"bm25_v1\"` on the index",
                        column
                    ),
                ));
            }
            ScoreQueryId::Lexical(self.lexical(sc, query_arg)?)
        } else {
            let ScalarType::Vector(dim) = prop.scalar else {
                return Err(err(
                    T15,
                    format!(
                        "`{name}` ranks a Vector field; `{type_name}.{property}` is {}",
                        prop.display_name()
                    ),
                ));
            };
            let q = self.expr(sc, query_arg, At::Value)?;
            match &q.ty {
                Ty::Value(t) if t.scalar == ScalarType::Vector(dim) && !t.list => {}
                Ty::Value(t) if t.list && matches!(q.kind, SKind::Lit(_)) => {
                    let SKind::Lit(Literal::List(items)) = &q.kind else {
                        unreachable!()
                    };
                    if items.len() as u32 != dim {
                        return Err(err(
                            T15,
                            format!(
                                "`{type_name}.{property}` is Vector({dim}); the query vector has {} elements",
                                items.len()
                            ),
                        ));
                    }
                }
                // A list holding parameters counts its elements as a
                // literal list does.
                Ty::Value(t)
                    if t.list
                        && t.scalar.is_numeric()
                        && matches!(&q.kind, SKind::Call { func, .. } if func == "list") =>
                {
                    let SKind::Call { args, .. } = &q.kind else {
                        unreachable!()
                    };
                    if args.len() as u32 != dim {
                        return Err(err(
                            T15,
                            format!(
                                "`{type_name}.{property}` is Vector({dim}); the query vector has {} elements",
                                args.len()
                            ),
                        ));
                    }
                }
                // A String is embedded with the field's recipe, or, on a
                // field without `@embed`, the default one, as today (the
                // RFC leaves the default recipe's `.pg` spelling open).
                Ty::Value(t) if t.scalar == ScalarType::String && !t.list => {}
                other => {
                    return Err(err(
                        T15,
                        format!(
                            "`{name}`'s query must be a Vector({dim}) or an embedded String, got {}",
                            other.display()
                        ),
                    ));
                }
            }
            ScoreQueryId::Vector(q)
        };
        // `distance` is `F32`; `bm25` is `bm25_v1`, `F64` (section 4).
        let out_ty = Ty::scalar(
            match kind {
                ScoreKind::Bm25 => ScalarType::F64,
                ScoreKind::Distance => ScalarType::F32,
            },
            false,
        );
        let score_at = pop.ty.cols.len();
        let mut cols = pop.ty.cols.clone();
        cols.push(Col {
            name: out.to_string(),
            ty: out_ty,
        });
        let mut shaped = pop.shaped.clone();
        shaped.push(None);
        let identity = Some((
            pop.term.clone(),
            format!("{column}.{property}"),
            query.clone(),
        ));
        let term = if name == "nearest" {
            let Some((_, k)) = c.options.iter().find(|(o, _)| o == "k") else {
                return Err(err_fix(
                    T17,
                    "`nearest` needs its window `k:`; no default couples it to `limit` (law 3)",
                    "nearest(…, k: n)",
                ));
            };
            let k = self.count_option(sc, k, "k", 1, None)?;
            let recall = match c.options.iter().find(|(o, _)| o == "recall") {
                None => None,
                Some((_, Expr::Literal(Literal::Float(r)))) if *r > 0.0 && *r <= 1.0 => Some(*r),
                Some((_, Expr::Literal(Literal::Integer(1)))) => Some(1.0),
                Some(_) => {
                    return Err(err(T71, "`recall:` is a literal in (0, 1]"));
                }
            };
            let ScoreQueryId::Vector(q) = query else {
                unreachable!()
            };
            Term::ApproxTopK {
                input: pop.term.boxed(),
                column: column.clone(),
                property,
                query: q.to_ir(),
                k,
                recall,
                out: out.to_string(),
            }
        } else {
            Term::Score {
                input: pop.term.boxed(),
                column: column.clone(),
                property,
                kind,
                query: match query {
                    ScoreQueryId::Vector(q) => ScoreQuery::Vector(q.to_ir()),
                    ScoreQueryId::Lexical(l) => ScoreQuery::Lexical(l),
                },
                out: out.to_string(),
            }
        };
        Ok(Rel {
            term,
            ty: RelTy { cols },
            shaped,
            ranked: Some(Ranked { column, identity }),
            score: named.is_none().then_some(score_at),
            free: pop.free,
        })
    }

    fn count_option(
        &mut self,
        sc: &mut Scope,
        e: &Expr,
        what: &str,
        min: i128,
        max: Option<i128>,
    ) -> R<Count> {
        match e {
            Expr::Literal(Literal::Integer(n)) if *n >= min && max.is_none_or(|m| *n <= m) => {
                Ok(Count::Literal(*n as u64))
            }
            Expr::Variable(p)
                if sc
                    .params
                    .get(p)
                    .is_some_and(|t| !t.list && is_integer(t.scalar)) =>
            {
                Ok(Count::Param(p.clone()))
            }
            _ => Err(err(
                T71,
                format!(
                    "`{what}:` is an integer literal or parameter{}",
                    match max {
                        Some(m) => format!(" in {min}..={m}"),
                        None => format!(" of at least {min}"),
                    }
                ),
            )),
        }
    }

    /// A String query (`terms(q, mode: any)`) or a `terms(…)` value.
    fn lexical(&mut self, sc: &mut Scope, e: &Expr) -> R<LexicalQuery> {
        crate::stack::deep(|| {
            if let Expr::Call(c) = e
                && c.name == "terms"
            {
                return Ok(self.terms(sc, c)?.lexical());
            }
            let text = self.expr(sc, e, At::Value)?;
            lexical_text(&text)?;
            Ok(LexicalQuery {
                text: text.to_ir(),
                mode_all: false,
                max_edits: int_lit(0).to_ir(),
            })
        })
    }

    fn terms(&mut self, sc: &mut Scope, c: &Call) -> R<Terms> {
        let [Arg::Expr(t)] = c.args.as_slice() else {
            return Err(err(T19, "`terms` takes one text argument"));
        };
        if !matches!(t, Expr::Literal(Literal::String(_)) | Expr::Variable(_)) {
            return Err(err(T19, "`terms`' text is a String literal or parameter"));
        }
        let text = self.expr(sc, t, At::Value)?;
        lexical_text(&text)?;
        let mut mode_all = true;
        let mut max_edits = int_lit(0);
        for (o, v) in &c.options {
            match (o.as_str(), v) {
                ("mode", Expr::Ident(m)) if m == "all" => mode_all = true,
                ("mode", Expr::Ident(m)) if m == "any" => mode_all = false,
                ("max_edits", Expr::Literal(Literal::Integer(n))) if (0..=2).contains(n) => {
                    max_edits = int_lit(*n)
                }
                ("max_edits", Expr::Literal(Literal::Integer(n))) => {
                    return Err(err(T71, format!("`max_edits: {n}` is outside 0..=2")));
                }
                ("max_edits", Expr::Variable(p))
                    if sc.params.get(p).is_some_and(|t| is_integer(t.scalar)) =>
                {
                    max_edits = self.expr(sc, v, At::Value)?
                }
                ("mode", _) => return Err(err(T71, "`mode:` is `all` or `any`")),
                (other, _) => {
                    return Err(err(
                        T71,
                        format!(
                            "`terms` has no option `{other}`; it admits mode (`all` or `any`), max_edits (integer)"
                        ),
                    ));
                }
            }
        }
        Ok(Terms {
            text,
            mode_all,
            max_edits,
        })
    }

    /// `rrf`; `named` is the fused score's name when a `from` draws it.
    fn rrf(&mut self, sc: &mut Scope, c: &Call, named: Option<&str>) -> R<Rel> {
        let n = c.args.len();
        if !(RRF_MIN_ARMS..=RRF_MAX_ARMS).contains(&n) {
            return Err(err(T75, format!("`rrf` takes 2 to 16 arms, got {n}")));
        }
        let mut arms = Vec::new();
        for a in &c.args {
            let r = self.arg_rel(sc, a)?;
            let Some(ranked) = r.ranked.clone() else {
                return Err(err_fix(
                    T75,
                    "an `rrf` arm is a ranking function or an inline read with an `order` over one node column",
                    "nearest(…), bm25(…), distance(…), or { match { … } return { $x } order { … } }",
                ));
            };
            arms.push((r, ranked));
        }
        let (first, first_ranked) = (&arms[0].0, &arms[0].1);
        let target_ty = first.ty.get(&first_ranked.column).unwrap().ty.clone();
        for (i, (r, rk)) in arms.iter().enumerate().skip(1) {
            let ty = &r.ty.get(&rk.column).unwrap().ty;
            if rk.column != first_ranked.column || !ty.same_base(&target_ty) {
                return Err(err(
                    T75,
                    format!(
                        "`rrf` arm {} ranks `${}` ({}), arm 1 ranks `${}` ({}); the fused target would be a pair",
                        i + 1,
                        rk.column,
                        ty.display(),
                        first_ranked.column,
                        target_ty.display()
                    ),
                ));
            }
        }
        for i in 0..arms.len() {
            for j in 0..i {
                if arms[i].1.identity.is_some() && arms[i].1.identity == arms[j].1.identity {
                    return Err(err(
                        T75,
                        format!(
                            "`rrf` arms {} and {} rank the same population, field and query; `weights` expresses emphasis",
                            j + 1,
                            i + 1
                        ),
                    ));
                }
            }
        }
        let mut k = Count::Literal(RRF_DEFAULT_K);
        let mut weights = vec![1.0; n];
        for (o, v) in &c.options {
            match (o.as_str(), v) {
                ("k", Expr::Literal(Literal::Integer(x))) if *x >= 1 && *x <= RRF_MAX_K as i128 => {
                    k = Count::Literal(*x as u64)
                }
                // An integer parameter: the engine refuses a value outside
                // the range as it runs.
                ("k", Expr::Variable(p))
                    if sc
                        .params
                        .get(p)
                        .is_some_and(|t| !t.list && is_integer(t.scalar)) =>
                {
                    k = Count::Param(p.clone())
                }
                ("k", _) => {
                    return Err(err(
                        T71,
                        "`rrf`'s `k:` is an integer literal in 1..=1000000 or an integer parameter",
                    ));
                }
                ("weights", Expr::Literal(Literal::List(ws))) => {
                    if ws.len() != n {
                        return Err(err(
                            T75,
                            format!("`weights` has {} entries for {n} arms", ws.len()),
                        ));
                    }
                    weights = Vec::new();
                    for w in ws {
                        let w = match w {
                            Literal::Float(f) => *f,
                            Literal::Integer(i) => *i as f64,
                            _ => return Err(err(T71, "a weight is a number")),
                        };
                        if !(w > 0.0 && w <= 1_000_000.0) {
                            return Err(err(T71, format!("a weight is in (0, 1000000], got {w}")));
                        }
                        weights.push(w);
                    }
                }
                ("weights", _) => return Err(err(T71, "`weights:` is a list literal of numbers")),
                (other, _) => {
                    return Err(err(
                        T71,
                        format!(
                            "`rrf` has no option `{other}`; it admits k (integer), weights (a list of numbers)"
                        ),
                    ));
                }
            }
        }
        let target = first_ranked.column.clone();
        let out = match named {
            Some(out) if out == target => {
                return Err(err_fix(
                    T68,
                    format!("`rrf`'s score is named `${out}`, its target column's name"),
                    "name the score another way: `as $<name>`",
                ));
            }
            Some(out) => out.to_string(),
            None => unused_column(
                default_score_name("rrf"),
                &RelTy {
                    cols: vec![Col {
                        name: target.clone(),
                        ty: target_ty.clone(),
                    }],
                },
            ),
        };
        let mut free = Vec::new();
        for (r, _) in &arms {
            for f in &r.free {
                if !free.contains(f) {
                    free.push(f.clone());
                }
            }
        }
        Ok(Rel {
            term: Term::Fuse {
                arms: arms.into_iter().map(|(r, _)| r.term).collect(),
                target: target.clone(),
                k,
                weights,
                out: out.clone(),
            },
            ty: RelTy {
                cols: vec![
                    Col {
                        name: target.clone(),
                        ty: target_ty.with_nullable(false),
                    },
                    Col {
                        name: out,
                        ty: Ty::scalar(ScalarType::F64, false),
                    },
                ],
            },
            shaped: vec![None, None],
            ranked: Some(Ranked {
                column: target,
                identity: None,
            }),
            score: named.is_none().then_some(1),
            free,
        })
    }

    /// `name(args)`: a declared read of the same source unit, inlined with
    /// its arguments bound.
    fn call(&mut self, sc: &mut Scope, c: &Call) -> R<Rel> {
        let decl = *self.unit.get(&c.name).unwrap();
        if self.in_write {
            return Err(err(
                T74,
                format!(
                    "a write body calls the declared read `{}`; this release does not support a declared read called from a write yet",
                    c.name
                ),
            ));
        }
        if let DeclBody::Write(_) = decl.body {
            return Err(err(
                T74,
                format!(
                    "`{}` is a write; a read draws rows only from reads, never from a write",
                    c.name
                ),
            ));
        }
        if let Some((o, _)) = c.options.first() {
            return Err(err(
                T71,
                format!("a declared read takes positional arguments; got `{o}:`"),
            ));
        }
        let params = param_types(&decl.name, &decl.params)?;
        // A defaulted parameter is followed only by defaulted ones
        // (`param_types`, `T88`), so the parameters a call may omit are the
        // last ones: every parameter past the last argument has a default.
        let required = decl.params.iter().filter(|p| p.default.is_none()).count();
        if c.args.len() < required || c.args.len() > params.len() {
            let takes = if required == params.len() {
                format!(
                    "{} argument{}",
                    params.len(),
                    if params.len() == 1 { "" } else { "s" }
                )
            } else {
                format!("{required} to {} arguments", params.len())
            };
            let missing = decl
                .params
                .get(c.args.len())
                .filter(|p| p.default.is_none())
                .map(|p| format!("; `${}` has no default", p.name))
                .unwrap_or_default();
            return Err(err(
                T7,
                format!("`{}` takes {takes}, got {}{missing}", c.name, c.args.len()),
            ));
        }
        let mut bindings = Vec::new();
        for (i, (pname, pty)) in params.iter().enumerate() {
            let v = match c.args.get(i) {
                Some(Arg::Inline(_)) => {
                    return Err(err_fix(
                        T69,
                        format!(
                            "`{}`'s parameter `${pname}` is a scalar; an inline read is a relation",
                            c.name
                        ),
                        "pass a scalar, and draw the relation with `from` inside the read",
                    ));
                }
                Some(Arg::Expr(e)) => {
                    if is_relation_expr(e, sc, self.unit) {
                        return Err(err(
                            T69,
                            format!(
                                "`{}`'s parameter `${pname}` is a scalar; a declared read takes scalar arguments only",
                                c.name
                            ),
                        ));
                    }
                    let v = self.expr(sc, e, At::Value)?;
                    if !sc.block_buf.is_empty() {
                        return Err(err(
                            T69,
                            "a block is not a call argument; bind it with `let`",
                        ));
                    }
                    let v = adapt(v, &Ty::Value(pty.clone()));
                    match &v.ty {
                        // A nullable value, a nullable parameter of the
                        // caller's included, is no value of a parameter
                        // that is not.
                        Ty::Value(t)
                            if types_compatible(t, pty) && (pty.nullable || !t.nullable) => {}
                        other => {
                            return Err(err(
                                T7,
                                format!(
                                    "`{}`'s parameter `${pname}` is {}, got {}",
                                    c.name,
                                    pty.display_name(),
                                    other.display()
                                ),
                            ));
                        }
                    }
                    v
                }
                None => {
                    let Some(d) = decl.params[i].default.as_ref() else {
                        unreachable!("a parameter past the arguments has a default (checked above)")
                    };
                    SExpr::new(SKind::Lit(default_value(d)), Ty::Value(pty.clone()))
                }
            };
            bindings.push(v);
        }
        // A recursive reference: the callee is being typed further out.
        if let Some(target) = self.frames.iter().position(|f| f.decl == c.name) {
            self.recursive_args(c, &bindings, target)?;
            self.rec_hits.insert(c.name.clone());
            let Some(ty) = self.assume.get(&c.name).cloned() else {
                return Err(E::Pending(c.name.clone()));
            };
            let n = ty.cols.len();
            return Ok(Rel {
                term: Term::RecRef {
                    name: c.name.clone(),
                    ty: ty.clone(),
                },
                ty,
                shaped: vec![None; n],
                ranked: None,
                score: None,
                free: Vec::new(),
            });
        }
        if self.frames.len() > 64 {
            return Err(err(
                T70,
                format!(
                    "the call chain through `{}` is deeper than 64 reads",
                    c.name
                ),
            ));
        }
        // The body of a read typed once per declaration, so a read called
        // many times (each caller calling the next twice) is not typed
        // again at each call.
        // The expanded tree is charged at every call site (the RFC's `Call`
        // rule): the body's nodes are charged as it is typed, and its tree
        // again at each later site that shares it.
        let (term, ty, shaped) = match self.typed_calls.get(&c.name) {
            Some((term, ty, shaped, below)) if self.assume.is_empty() => {
                let typed = (term.clone(), ty.clone(), shaped.clone());
                let below = below.clone();
                self.expanding = std::iter::once(c.name.clone())
                    .chain(below.iter().cloned())
                    .collect();
                self.charge_expansion(&typed.0)?;
                self.expanding.clear();
                self.calls_below = std::iter::once(c.name.clone()).chain(below).collect();
                typed
            }
            _ => {
                self.calls_below.clear();
                self.frames.push(Frame {
                    decl: c.name.clone(),
                    params: params.clone(),
                    bindings: bindings.clone(),
                });
                let out = self.read_decl(decl);
                self.frames.pop();
                let Rel {
                    term, ty, shaped, ..
                } = out?.rel;
                if self.assume.is_empty() && !reaches_recursion(&term) {
                    self.typed_calls.insert(
                        c.name.clone(),
                        (
                            term.clone(),
                            ty.clone(),
                            shaped.clone(),
                            self.calls_below.clone(),
                        ),
                    );
                }
                self.calls_below.insert(0, c.name.clone());
                (term, ty, shaped)
            }
        };
        let bindings: Vec<(String, IRExpr)> = params
            .iter()
            .map(|(p, _)| p.clone())
            .zip(bindings.iter().map(SExpr::to_ir))
            .collect();
        self.charge(Built {
            terms: 1,
            exprs: bindings.iter().map(|(_, e)| ir_nodes(e)).sum(),
            steps: 0,
        })?;
        let term = Term::Call {
            name: c.name.clone(),
            bindings,
            body: term.boxed(),
        };
        Ok(Rel {
            term,
            ty,
            shaped,
            ranked: None,
            score: None,
            free: Vec::new(),
        })
    }

    /// A recursive call passes the parameters through unchanged: each
    /// argument, followed back through the call frames, is the target's own
    /// parameter in the same position.
    fn recursive_args(&self, c: &Call, args: &[SExpr], target: usize) -> R<()> {
        let target_params = &self.frames[target].params;
        for (i, a) in args.iter().enumerate() {
            let mut cur = a.clone();
            let mut f = self.frames.len() - 1;
            let ok = loop {
                let SKind::Param(name) = &cur.kind else {
                    break false;
                };
                if f == target {
                    break target_params.get(i).is_some_and(|(p, _)| p == name);
                }
                let frame = &self.frames[f];
                let Some(pos) = frame.params.iter().position(|(p, _)| p == name) else {
                    break false;
                };
                cur = frame.bindings[pos].clone();
                f -= 1;
            };
            if !ok {
                return Err(err_fix(
                    T70,
                    format!(
                        "the recursive call `{}(…)` passes a computed argument in position {}; a recursion passes its parameters through unchanged (a computed argument is value invention with no fixpoint)",
                        c.name,
                        i + 1
                    ),
                    format!(
                        "pass `${}` through, and filter or compute inside the body",
                        target_params.get(i).map_or("p", |(p, _)| p.as_str())
                    ),
                ));
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Write statements.

    /// Whether `insert T { … }` writes the edge type `T` rather than the node
    /// type `T`. A node type and an edge type may share a name, and the
    /// statement's form decides between them: an edge write assigns its
    /// endpoints as `@src` and `@dst`, which no node type declares. Edition
    /// 2's `from` and `to` select the edge too where the node type declares
    /// no such property, so the edge's refusal prints the `@src`/`@dst` fix.
    fn insert_targets_edge(&self, type_name: &str, assignments: &[Assignment]) -> bool {
        let node = self.catalog.node_types.get(type_name);
        let edge = self.catalog.edge_types.get(type_name);
        let (Some(node), Some(_)) = (node, edge) else {
            return node.is_none() && edge.is_some();
        };
        assignments.iter().any(|a| match a.property.as_str() {
            "@src" | "@dst" => true,
            end @ ("from" | "to") => !node.properties.contains_key(end),
            _ => false,
        })
    }

    /// `T92`: `upsert` or `replace` names an existing entity by its key, so a
    /// type with no `@key` has none to name; the fix writes `insert`, which
    /// always creates.
    fn keyless_mode(
        &self,
        mode: InsertMode,
        type_name: &str,
        keyed: bool,
        at: Option<&SourceSpan>,
    ) -> Option<E> {
        if mode == InsertMode::Insert || keyed {
            return None;
        }
        let word = mode.keyword();
        let mut d = QueryDiagnostic::typecheck(
            T92,
            format!(
                "`{word} {type_name}`: `{type_name}` declares no `@key`, so `{word}` has no existing entity to name; `insert` creates one"
            ),
        )
        .with_fix(format!("insert {type_name} {{ … }}"));
        if let Some(at) = at.filter(|at| at.end > at.start) {
            d.position = Some(at.position);
            d.end = Some(at.end_position);
            d = d.with_suggestion(Suggestion {
                applicability: Applicability::MachineApplicable,
                edits: vec![TextEdit {
                    start: at.start,
                    end: at.start + word.len(),
                    replacement: "insert".to_string(),
                }],
            });
        }
        Some(E::D(d))
    }

    fn statement(
        &mut self,
        sc: &mut Scope,
        s: &Stmt,
        at: Option<&SourceSpan>,
    ) -> R<(Sink, WriteClass)> {
        match s {
            Stmt::Insert {
                mode,
                type_name,
                assignments,
            } => {
                let mode = *mode;
                let word = mode.keyword();
                if crate::catalog::catalogue::is_catalogue_type_name(type_name) {
                    return Err(err(
                        T10,
                        format!(
                            "catalogue type `{type_name}` is read-only; the engine owns its rows"
                        ),
                    ));
                }
                if assignments.is_empty() {
                    return Err(err(T10, format!("{word} requires at least one assignment")));
                }
                dup_assignments(assignments)?;
                let edge_insert = self.insert_targets_edge(type_name, assignments);
                if let Some(node) = self
                    .catalog
                    .node_types
                    .get(type_name)
                    .filter(|_| !edge_insert)
                {
                    if let Some(e) = self.keyless_mode(mode, type_name, node.key.is_some(), at) {
                        return Err(e);
                    }
                    let key: HashSet<&str> =
                        node.key.iter().flatten().map(String::as_str).collect();
                    let mut out = Vec::new();
                    // Each assignment is checked; an unknown property may be
                    // the misspelled required one, so the required check
                    // waits for none.
                    let mut failed: Option<E> = None;
                    let mut unknown = false;
                    for a in assignments {
                        let typed = match node.properties.get(&a.property).cloned() {
                            None if matches!(a.property.as_str(), "@src" | "@dst") => {
                                unknown = true;
                                Err(err(
                                    T11,
                                    format!(
                                        "node type `{type_name}` has no endpoints; `{}` is assigned on an edge",
                                        a.property
                                    ),
                                ))
                            }
                            None => {
                                unknown = true;
                                Err(err(
                                    T11,
                                    format!(
                                        "type `{type_name}` has no property `{}`{}{}",
                                        a.property,
                                        system_assignment_hint(&a.property),
                                        nearest::listing(
                                            &a.property,
                                            nearest::properties(&node.properties)
                                        )
                                    ),
                                ))
                            }
                            Some(pt) => self.assign(sc, &a.value, &pt, &a.property),
                        };
                        match typed {
                            Ok(v) => out.push((a.property.clone(), v)),
                            Err(e) => failed = Some(self.join_failed(sc, failed.take(), e)),
                        }
                    }
                    let assigned: HashSet<&str> =
                        assignments.iter().map(|a| a.property.as_str()).collect();
                    let mut missing = None;
                    for (p, t) in &node.properties {
                        if t.nullable || assigned.contains(p.as_str()) {
                            continue;
                        }
                        // `upsert` on an existing key keeps every property
                        // it omits, so only its key is required here; a
                        // new key without a required property is refused
                        // when it runs.
                        if mode == InsertMode::Upsert && !key.contains(p.as_str()) {
                            continue;
                        }
                        if let Some(e) = node.embed_sources.get(p)
                            && assigned.contains(e.source.as_str())
                        {
                            continue;
                        }
                        missing = Some(err(
                            T12,
                            format!(
                                "{word} for `{type_name}` must provide non-nullable property `{p}`"
                            ),
                        ));
                        break;
                    }
                    match (failed, missing) {
                        (Some(e), Some(m)) if !unknown => return Err(self.join(sc, e, m)),
                        (Some(e), _) => return Err(e),
                        (None, Some(m)) => return Err(m),
                        (None, None) => {}
                    }
                    return Ok((
                        Sink::Insert {
                            mode,
                            target: MutationTarget::Node {
                                type_name: type_name.clone(),
                            },
                            assignments: out,
                        },
                        WriteClass::Constructive,
                    ));
                }
                if let Some(edge) = self.catalog.edge_types.get(type_name) {
                    if let Some(e) = self.keyless_mode(mode, type_name, edge.key.is_some(), at) {
                        return Err(e);
                    }
                    let mut out = Vec::new();
                    let (mut src, mut dst) = (false, false);
                    let mut failed: Option<E> = None;
                    for a in assignments {
                        let v = (|| -> R<(String, AssignValue)> {
                            Ok(match a.property.as_str() {
                                end @ ("@src" | "@dst") => {
                                    // The IR names an endpoint by the edge
                                    // table's logical `from` and `to`.
                                    let (endpoint, name) = if end == "@src" {
                                        src = true;
                                        (&edge.from_type, "from")
                                    } else {
                                        dst = true;
                                        (&edge.to_type, "to")
                                    };
                                    (
                                        name.to_string(),
                                        self.endpoint(sc, &a.value, endpoint, end, type_name)?,
                                    )
                                }
                                end @ ("from" | "to") => {
                                    return Err(endpoint_respelling(end, &a.at));
                                }
                                p => {
                                    let Some(pt) = edge.properties.get(p).cloned() else {
                                        let mut names = nearest::properties(&edge.properties);
                                        names.push(("@src".into(), edge.from_type.clone()));
                                        names.push(("@dst".into(), edge.to_type.clone()));
                                        return Err(err(
                                            T11,
                                            format!(
                                                "type `{type_name}` has no property `{p}`{}{}",
                                                system_assignment_hint(p),
                                                nearest::listing(p, names)
                                            ),
                                        ));
                                    };
                                    (p.to_string(), self.assign(sc, &a.value, &pt, p)?)
                                }
                            })
                        })();
                        match v {
                            Ok(v) => out.push(v),
                            Err(e) => failed = Some(self.join_failed(sc, failed.take(), e)),
                        }
                    }
                    if let Some(e) = failed {
                        return Err(e);
                    }
                    if !src || !dst {
                        return Err(err(
                            T12,
                            format!(
                                "{word} for `{type_name}` must provide required endpoint `{}`",
                                if src { "@dst" } else { "@src" }
                            ),
                        ));
                    }
                    for (p, t) in &edge.properties {
                        if !t.nullable
                            && mode != InsertMode::Upsert
                            && !assignments.iter().any(|a| &a.property == p)
                        {
                            return Err(err(
                                T12,
                                format!(
                                    "{word} for `{type_name}` must provide non-nullable property `{p}`"
                                ),
                            ));
                        }
                    }
                    return Ok((
                        Sink::Insert {
                            mode,
                            target: MutationTarget::Edge {
                                type_name: type_name.clone(),
                            },
                            assignments: out,
                        },
                        WriteClass::Constructive,
                    ));
                }
                Err(err(
                    T10,
                    format!(
                        "unknown node/edge type `{type_name}`{}",
                        nearest::listing(type_name, nearest::binding_types(self.catalog))
                    ),
                ))
            }
            Stmt::Update {
                variable,
                assignments,
            } => {
                let target = self.statement_target(sc, variable, "update")?;
                if assignments.is_empty() {
                    return Err(err(T10, "update requires at least one assignment"));
                }
                dup_assignments(assignments)?;
                // A key property and an edge's endpoints are its identity:
                // `update` sets the others, and moving an edge is a delete
                // and an insert.
                let (type_name, props, key) = match &target {
                    MutationTarget::Node { type_name } => {
                        let node = self.catalog.node_types.get(type_name);
                        (
                            type_name,
                            node.map(|n| n.properties.clone()).unwrap_or_default(),
                            node.and_then(|n| n.key.clone()).unwrap_or_default(),
                        )
                    }
                    MutationTarget::Edge { type_name } => {
                        let edge = self.catalog.edge_types.get(type_name);
                        (
                            type_name,
                            edge.map(|e| e.properties.clone()).unwrap_or_default(),
                            edge.and_then(|e| e.key.clone()).unwrap_or_default(),
                        )
                    }
                };
                let edge = matches!(target, MutationTarget::Edge { .. });
                let mut out = Vec::new();
                // Each assignment is checked.
                let mut failed: Option<E> = None;
                for a in assignments {
                    let p = a.property.as_str();
                    let identity = if edge && matches!(p, "@src" | "@dst" | "from" | "to") {
                        Some("an endpoint")
                    } else if key.iter().any(|k| k == p) {
                        Some("a key property")
                    } else {
                        None
                    };
                    let typed = match (identity, props.get(p).cloned()) {
                        (Some(what), _) => Err(err_fix(
                            T93,
                            format!(
                                "`update ${variable}` assigns `{p}`, {what} of `{type_name}`; a key and an edge's endpoints are its identity, which `update` does not change"
                            ),
                            if edge {
                                format!(
                                    "delete the edge and insert it with its new endpoints, or leave `{p}` out of the `set`"
                                )
                            } else {
                                format!(
                                    "insert an entity with the new key and delete the old one, or leave `{p}` out of the `set`"
                                )
                            },
                        )),
                        (None, None) => Err(err(
                            T11,
                            format!(
                                "type `{type_name}` has no property `{p}`{}{}",
                                system_assignment_hint(p),
                                nearest::listing(p, nearest::properties(&props))
                            ),
                        )),
                        (None, Some(pt)) => self.assign(sc, &a.value, &pt, p),
                    };
                    match typed {
                        Ok(v) => out.push((a.property.clone(), v)),
                        Err(e) => failed = Some(self.join_failed(sc, failed.take(), e)),
                    }
                }
                if let Some(e) = failed {
                    return Err(e);
                }
                Ok((
                    Sink::Update {
                        var: variable.clone(),
                        target,
                        assignments: out,
                    },
                    WriteClass::Constructive,
                ))
            }
            Stmt::Delete { variable } => {
                let target = self.statement_target(sc, variable, "delete")?;
                Ok((
                    Sink::Delete {
                        var: variable.clone(),
                        target,
                    },
                    WriteClass::Destructive,
                ))
            }
        }
    }

    fn statement_target(&mut self, sc: &mut Scope, var: &str, what: &str) -> R<MutationTarget> {
        match sc.col(var) {
            Some(Ty::Node { type_name, .. }) => {
                if self.catalog.interfaces.contains_key(&type_name) {
                    return Err(err(
                        T77,
                        format!(
                            "`{what} ${var}`: `${var}` is an interface binding; a write targets a declared type"
                        ),
                    ));
                }
                if crate::catalog::catalogue::is_catalogue_type_name(&type_name) {
                    return Err(err(
                        T10,
                        format!("catalogue type `{type_name}` is read-only"),
                    ));
                }
                Ok(MutationTarget::Node { type_name })
            }
            Some(Ty::Edge { type_name, .. }) if !type_name.contains('|') => {
                Ok(MutationTarget::Edge { type_name })
            }
            Some(other) => Err(err(
                T77,
                format!(
                    "`{what} ${var}`: `${var}` is {}, not a node or edge reference of one type",
                    other.display()
                ),
            )),
            None => Err(err_fix(
                T77,
                format!("`{what} ${var}`: `${var}` is not bound by the group's `match`"),
                format!("bind it in the group's `match`: `match {{ ${var}: <Type> }} where …`"),
            )),
        }
    }

    fn assign(
        &mut self,
        sc: &mut Scope,
        value: &Expr,
        pt: &PropType,
        prop: &str,
    ) -> R<AssignValue> {
        if matches!(value, Expr::Literal(Literal::Null)) {
            if !pt.nullable {
                return Err(err(
                    T3,
                    format!("property `{prop}` is non-nullable but got null"),
                ));
            }
            return Ok(AssignValue::Null);
        }
        if is_untyped_list(value) {
            // The property fixes the element type of a `[]`.
            if !pt.list {
                return Err(err(
                    T3,
                    format!(
                        "property `{prop}` has type {} but got a list",
                        pt.display_name()
                    ),
                ));
            }
            return Ok(AssignValue::Expr(
                untyped_list_as(value, &Ty::Value(pt.clone()))?.to_ir(),
            ));
        }
        // A negative number is a literal of the property's type, checked
        // against its range as any literal is: `balance: -5`.
        let negated = match value {
            Expr::Neg(inner) => match inner.as_ref() {
                Expr::Literal(Literal::Integer(n)) => Some(Literal::Integer(-*n)),
                Expr::Literal(Literal::Float(f)) => Some(Literal::Float(-*f)),
                _ => None,
            },
            _ => None,
        };
        if let Some(l) = negated.as_ref().or(match value {
            Expr::Literal(l) => Some(l),
            _ => None,
        }) {
            let lt = literal_type(l)?;
            check_literal_type(l, &lt, pt, prop)?;
            let mut want = pt.clone();
            want.nullable = false;
            return Ok(AssignValue::Expr(
                SExpr::new(SKind::Lit(l.clone()), Ty::Value(want)).to_ir(),
            ));
        }
        let v = if is_untyped_null(value) {
            // The property fixes the type of a `null` nothing else does.
            self.null_as(sc, value, pt)?
        } else {
            self.expr(sc, value, At::Value)?
        };
        if !sc.block_buf.is_empty() {
            return Err(err(
                T44,
                "a block is not an assignment value; bind it with `let` in the group's `match`",
            ));
        }
        let v = adapt(v, &Ty::Value(pt.clone()));
        let ok = match &v.ty {
            Ty::Value(t) => {
                assignment_compatible(t, pt)
                    && (!t.scalar.is_numeric() || numeric_fits(t.scalar, pt.scalar))
                    // A nullable value for a non-null property is checked as
                    // the write runs (a constant can evaluate to non-null).
                    && (pt.nullable || !t.nullable || v.to_ir().columns_empty())
            }
            _ => false,
        };
        if !ok {
            return Err(err(
                T3,
                format!(
                    "property `{prop}` has type {} but the value is {}",
                    pt.display_name(),
                    v.ty.display()
                ),
            ));
        }
        Ok(AssignValue::Expr(v.to_ir()))
    }

    fn endpoint(
        &mut self,
        sc: &mut Scope,
        value: &Expr,
        endpoint: &str,
        end: &str,
        edge: &str,
    ) -> R<AssignValue> {
        let key_ty = self
            .catalog
            .node_types
            .get(endpoint)
            .and_then(|n| n.key_property().and_then(|k| n.properties.get(k)).cloned())
            .unwrap_or_else(|| PropType::scalar(ScalarType::String, false));
        if let Expr::Variable(v) = value
            && let Some(ty) = sc.col(v)
        {
            return match ty {
                Ty::Node { type_name, .. } if type_name == endpoint => Ok(AssignValue::Expr(
                    SExpr::new(
                        SKind::Col(v.clone()),
                        Ty::Node {
                            type_name,
                            nullable: false,
                        },
                    )
                    .to_ir(),
                )),
                other => Err(err(
                    T78,
                    format!(
                        "`{end}` of `{edge}` is a `{endpoint}`; `${v}` is {}",
                        other.display()
                    ),
                )),
            };
        }
        let v = self.expr(sc, value, At::Value)?;
        let v = adapt(v, &Ty::Value(key_ty.clone()));
        match &v.ty {
            // A String names any key, as today (`from: "0"` on an I64 key).
            Ty::Value(t)
                if !t.list && (types_compatible(t, &key_ty) || t.scalar == ScalarType::String) =>
            {
                Ok(AssignValue::Expr(v.to_ir()))
            }
            other => Err(err(
                T78,
                format!(
                    "`{end}` of `{edge}` is a key of `{endpoint}` ({}) or a bound `{endpoint}` reference; got {}",
                    key_ty.display_name(),
                    other.display()
                ),
            )),
        }
    }

    // -----------------------------------------------------------------------
    // Scalar expressions.

    fn node_props(&self, type_name: &str) -> Option<&HashMap<String, PropType>> {
        self.catalog
            .query_node_type(type_name)
            .map(|n| &n.properties)
            .or_else(|| {
                self.catalog
                    .interfaces
                    .get(type_name)
                    .map(|i| &i.properties)
            })
    }

    /// The type of `prop` on a reference of type `ty`, and its owner's name.
    fn property(&self, ty: &Ty, prop: &str, var: &str) -> R<(PropType, String)> {
        match ty {
            Ty::Node {
                type_name,
                nullable,
            } => {
                let Some(props) = self.node_props(type_name) else {
                    return Err(err(T6, format!("type `{type_name}` not found")));
                };
                let Some(p) = props.get(prop) else {
                    let hint = match prop {
                        "id" => format!("; the system identity is `${var}.@id`"),
                        _ => String::new(),
                    };
                    return Err(err(
                        T6,
                        format!(
                            "type `{type_name}` has no property `{prop}`{hint}{}",
                            nearest::listing(prop, nearest::properties(props))
                        ),
                    ));
                };
                let mut p = p.clone();
                p.nullable |= *nullable;
                Ok((p, type_name.clone()))
            }
            Ty::Edge {
                type_name,
                nullable,
            } => {
                let names: Vec<String> = type_name.split('|').map(str::to_string).collect();
                let Some(mut p) =
                    crate::traversal::common_edge_property(self.catalog, &names, prop)
                else {
                    let what = if type_name.is_empty() {
                        format!(
                            "`${var}` selects no edge: an empty wildcard has no inferable property `{prop}`"
                        )
                    } else if names.len() > 1 {
                        format!(
                            "edge `{type_name}` has no common property `{prop}`: it must exist with compatible types on every selected edge"
                        )
                    } else {
                        let hint = match prop {
                            "id" => format!("; the system identity is `${var}.@id`"),
                            "src" | "dst" => format!("; the system endpoint is `${var}.@{prop}`"),
                            _ => String::new(),
                        };
                        let near = self
                            .catalog
                            .lookup_edge_by_name(type_name)
                            .map(|e| nearest::listing(prop, nearest::properties(&e.properties)))
                            .unwrap_or_default();
                        format!("edge `{type_name}` has no property `{prop}`{hint}{near}")
                    };
                    return Err(err(T6, what));
                };
                p.nullable |= *nullable;
                Ok((p, type_name.clone()))
            }
            Ty::Value(t) => Err(err(
                T6,
                format!(
                    "`${var}` is {}; only a node or edge reference has properties",
                    t.display_name()
                ),
            )),
        }
    }

    /// A list literal holding a parameter (section 7's `list_elem`): every
    /// parameter element is a declared non-null scalar parameter, all of one
    /// declared type, which is the list's element type; a literal element is
    /// a value of that type. No element is cast. The typed list is the
    /// internal `list` row, built when the parameters are bound.
    fn list_value(
        &mut self,
        sc: &mut Scope,
        elements: &[Expr],
        written: &SourceSpan,
        at: At,
    ) -> R<SExpr> {
        let refuse = |code: QueryCode, message: String| {
            let mut d = QueryDiagnostic::typecheck(code, message);
            if written.end > written.start {
                d.position = Some(written.position);
                d.end = Some(written.end_position);
            }
            E::D(d)
        };
        const ELEMENT: &str = "a list element is a literal or a declared non-null scalar parameter";
        let mut element: Option<(String, PropType)> = None;
        for e in elements {
            let Expr::Variable(v) = e else { continue };
            if let Some(c) = sc.local(v).or_else(|| sc.outer_col(v)) {
                let what = match c.ty {
                    Ty::Node { .. } | Ty::Edge { .. } => "a binding",
                    Ty::Value(_) => "a value bound by `let` or a block",
                };
                return Err(refuse(
                    T7,
                    format!("the list element `${v}` is {what}, not a parameter; {ELEMENT}"),
                ));
            }
            let Some(t) = sc.params.get(v).cloned() else {
                if sc.rels.contains_key(v) {
                    return Err(refuse(
                        T7,
                        format!(
                            "the list element `${v}` is a relation, not a parameter; {ELEMENT}"
                        ),
                    ));
                }
                // An unknown name is refused as it is anywhere else.
                return self.expr(sc, e, at);
            };
            if t.list || t.nullable || matches!(t.scalar, ScalarType::Vector(_) | ScalarType::Blob)
            {
                return Err(refuse(
                    T7,
                    format!(
                        "the list element `${v}` is a parameter of type {}; {ELEMENT}",
                        t.display_name()
                    ),
                ));
            }
            match &element {
                None => element = Some((v.clone(), t)),
                Some((first, ft)) if ft.scalar != t.scalar => {
                    return Err(refuse(
                        T53,
                        format!(
                            "the list element `${v}` is {}, but `${first}` is {}; every parameter element of a list has one type, and no element is cast",
                            t.display_name(),
                            ft.display_name()
                        ),
                    ));
                }
                Some(_) => {}
            }
        }
        let Some((first, ty)) = element else {
            // A list of literals alone is a literal list.
            let items = elements
                .iter()
                .map(|e| match e {
                    Expr::Literal(l) => Ok(l.clone()),
                    other => self.expr(sc, other, at).map(|_| Literal::Null),
                })
                .collect::<R<Vec<_>>>()?;
            return self.expr(sc, &Expr::Literal(Literal::List(items)), at);
        };
        let mut args = Vec::with_capacity(elements.len());
        for e in elements {
            args.push(match e {
                Expr::Variable(v) => SExpr::new(SKind::Param(v.clone()), Ty::Value(ty.clone())),
                Expr::Literal(l) => {
                    let lt = literal_type(l)?;
                    let fits = match (l, ty.scalar) {
                        (Literal::Integer(_), s) => s.is_numeric(),
                        (Literal::Float(_), s) => matches!(s, ScalarType::F32 | ScalarType::F64),
                        (Literal::Null, _) => false,
                        _ => !lt.list && lt.scalar == ty.scalar,
                    };
                    let outside = if fits {
                        literal::out_of_range(l, &ty).map(|r| format!(" ({r})"))
                    } else {
                        None
                    };
                    if !fits || outside.is_some() {
                        return Err(refuse(
                            T53,
                            format!(
                                "the list element `{}` is not a value of the list's element type {}, the type of `${first}`{}; no element is cast",
                                crate::query::syntax::print::literal(l),
                                ty.display_name(),
                                outside.unwrap_or_default()
                            ),
                        ));
                    }
                    SExpr::new(SKind::Lit(l.clone()), Ty::Value(ty.clone()))
                }
                other => {
                    return Err(refuse(
                        T7,
                        format!(
                            "the list element `{}` is an expression; {ELEMENT}",
                            crate::query::syntax::print::expr(other)
                        ),
                    ));
                }
            });
        }
        Ok(SExpr::new(
            SKind::Call {
                func: "list".into(),
                args,
                options: Vec::new(),
            },
            Ty::Value(PropType::list_of(ty.scalar, false)),
        ))
    }

    fn expr(&mut self, sc: &mut Scope, e: &Expr, at: At) -> R<SExpr> {
        crate::stack::deep(|| {
            self.typed_exprs += 1;
            self.spend(WorkKind::Typing, 1)?;
            let sub = at;
            let boolean = Ty::scalar(ScalarType::Bool, false);
            Ok(match e {
                Expr::InfixString {
                    op,
                    left,
                    right: _,
                    at: written,
                } => {
                    return Err(E::D(infix_string(
                        *op,
                        written,
                        self.expr(sc, left, sub)?
                            .ty
                            .as_value()
                            .is_some_and(|p| p.list),
                    )));
                }
                Expr::Literal(Literal::Null) => return Err(untyped_null_refusal()),
                e if is_untyped_list(e) => return Err(untyped_list_refusal()),
                Expr::Literal(l) => SExpr::new(SKind::Lit(l.clone()), Ty::Value(literal_type(l)?)),
                Expr::List {
                    elements,
                    at: written,
                } => self.list_value(sc, elements, written, sub)?,
                Expr::Variable(v) => {
                    if let Some(ty) = sc.col(v) {
                        SExpr::new(SKind::Col(v.clone()), ty)
                    } else if let Some(t) = sc.params.get(v) {
                        SExpr::new(SKind::Param(v.clone()), Ty::Value(t.clone()))
                    } else if sc.is_error(v) {
                        return Err(E::Recorded);
                    } else if sc.rels.contains_key(v) {
                        return Err(err_fix(
                            T69,
                            format!("`${v}` is a relation; a scalar is read here"),
                            format!(
                                "draw it with `from ${v}`, or aggregate over it: `count {{ from ${v} }}`"
                            ),
                        ));
                    } else {
                        return Err(err(
                            T6,
                            format!(
                                "variable `${v}` is not bound{}",
                                nearest::listing(
                                    &format!("${v}"),
                                    sc.names_in_scope(&self.made_up)
                                )
                            ),
                        ));
                    }
                }
                Expr::Prop { variable, property } => {
                    let Some(ty) = sc.col(variable) else {
                        if sc.is_error(variable) {
                            return Err(E::Recorded);
                        }
                        if sc.params.contains_key(variable) {
                            return Err(err(
                                T6,
                                format!("`${variable}` is a parameter and has no properties"),
                            ));
                        }
                        return Err(err(
                            T6,
                            format!(
                                "variable `${variable}` is not bound{}",
                                nearest::listing(
                                    &format!("${variable}"),
                                    sc.names_in_scope(&self.made_up)
                                )
                            ),
                        ));
                    };
                    if let Some(field) = property.strip_prefix('@') {
                        let admitted = match &ty {
                            Ty::Node { .. } => field == "id",
                            Ty::Edge { .. } => matches!(field, "id" | "src" | "dst" | "type"),
                            Ty::Value(_) => false,
                        };
                        if !admitted {
                            let fields = match &ty {
                                Ty::Node { .. } => "its meta-fields are `@id`",
                                Ty::Edge { .. } => {
                                    "its meta-fields are `@id`, `@src`, `@dst` and `@type`"
                                }
                                Ty::Value(_) => "a value has no meta-fields",
                            };
                            return Err(err(
                                T6,
                                format!(
                                    "binding `${variable}` has no meta-field `{property}`; {fields}"
                                ),
                            ));
                        }
                        SExpr::new(
                            SKind::Meta {
                                col: variable.clone(),
                                field: property.clone(),
                            },
                            Ty::scalar(ScalarType::String, ty.nullable()),
                        )
                    } else {
                        let (p, _) = self.property(&ty, property, variable)?;
                        if p.scalar == ScalarType::Blob && !matches!(at, At::Value) {
                            return Err(err(
                                T24,
                                format!(
                                    "Blob property `{variable}.{property}` is not a read value"
                                ),
                            ));
                        }
                        SExpr::new(
                            SKind::Prop {
                                col: variable.clone(),
                                prop: property.clone(),
                            },
                            Ty::Value(p),
                        )
                    }
                }
                Expr::Meta(m) => {
                    return Err(err(
                        T6,
                        format!("a bare meta-field `@{m}` names no binding; write `$x.@{m}`"),
                    ));
                }
                Expr::Ident(i) => {
                    return Err(err(
                        T6,
                        format!("a bare identifier `{i}` is not a value in a read"),
                    ));
                }
                Expr::Call(c) => return self.call_scalar(sc, c, at),
                Expr::AggDistinct { func, .. } => {
                    return Err(err(
                        T8,
                        format!("`{func}(distinct …)` is an aggregate; it belongs in `return`"),
                    ));
                }
                Expr::Block(b) => {
                    let (term, free, arg) = self.block_scope_arg(sc, b)?;
                    if free.is_empty() {
                        return Err(err(
                            T39,
                            "an exists or aggregate block must reference an outer-bound variable",
                        ));
                    }
                    let col = self.fresh(FreshRole::Block);
                    // The block evaluates its inner term and, over its rows, its
                    // aggregate's argument; either can fail.
                    let can_fail = term.can_fail() || arg.as_ref().is_some_and(|a| a.can_fail);
                    let (agg, value) = match b.func {
                        BlockFunc::Exists => (count_rows(), exists_col(&col)),
                        BlockFunc::Agg(func) => {
                            let agg = match (arg, func) {
                                (None, AggFunc::Count) => count_rows(),
                                (None, f) => {
                                    return Err(err(
                                        T40,
                                        format!(
                                            "`{f} {{ }}` needs its argument: `{f}($x.p) {{ … }}`"
                                        ),
                                    ));
                                }
                                (Some(arg), f) => {
                                    let ty = agg_type(f, &arg.ty)?;
                                    let ty = ty.with_nullable(!f.defined_on_empty());
                                    let fraction =
                                        self.agg_fraction(sc, f, b.fraction.as_deref())?;
                                    Agg {
                                        func: f,
                                        distinct: false,
                                        arg: Some(arg.to_ir()),
                                        fraction,
                                        ty,
                                    }
                                }
                            };
                            let ty = agg.ty.clone();
                            (agg, SExpr::new(SKind::Col(col.clone()), ty))
                        }
                    };
                    // A block that can fail, written under a guard, runs only on
                    // the outer rows the guard selects (A3): the guard, typed
                    // once where it was written, is computed as a column of the
                    // outer row and the block's input keeps the rows where it
                    // holds.
                    let guard = if can_fail { self.block_guard(sc) } else { None };
                    sc.block_buf.push(BlockAgg {
                        inner: term,
                        col: col.clone(),
                        agg,
                        guard,
                    });
                    // The block's column is visible to the expression that reads it.
                    value
                }
                Expr::Neg(inner) => {
                    // `-9223372036854775808`: the literal is above the `I64`
                    // range and its negation is the smallest `I64`, which no
                    // negation of an `I64` reaches, so it is that literal.
                    if let Expr::Literal(Literal::Integer(n)) = inner.as_ref()
                        && i64::try_from(*n).is_err()
                        && i64::try_from(-*n).is_ok()
                    {
                        return Ok(SExpr::new(
                            SKind::Lit(Literal::Integer(-*n)),
                            Ty::scalar(ScalarType::I64, false),
                        ));
                    }
                    let v = self.expr(sc, inner, sub)?;
                    let ty = numeric(&v, "unary `-`")?;
                    if matches!(&ty, Ty::Value(t) if matches!(t.scalar, ScalarType::U32 | ScalarType::U64))
                    {
                        return Err(err(
                            T72,
                            format!(
                                "unary `-` needs a signed integer or a float, got {}",
                                ty.display()
                            ),
                        ));
                    }
                    SExpr::new(SKind::Neg(v.boxed()), ty)
                }
                Expr::Arith { op, left, right } => {
                    let l = self.expr(sc, left, sub);
                    let r = self.expr(sc, right, sub);
                    let (l, r) = self.both(sc, l, r)?;
                    arith(*op, l, r)?
                }
                Expr::Compare {
                    op,
                    left,
                    right,
                    at: written,
                } if (is_null_literal(left) || is_null_literal(right))
                    && soundness::refuses(soundness::Claim::NullComparison) =>
                {
                    return Err(null_comparison(*op, left, right, written));
                }
                Expr::Compare {
                    op, left, right, ..
                } if is_null_literal(left) || is_null_literal(right) => {
                    // The refusal let through (a test-only switch): the `null`
                    // takes the other operand's type, as the refusal's claim
                    // reads it.
                    let other_is_left = !is_null_literal(left);
                    let other = if other_is_left { left } else { right };
                    let other = self.expr(sc, other, sub)?;
                    let Ty::Value(t) = &other.ty else {
                        return Err(err(T7, "a comparison with `null` over a reference"));
                    };
                    let null = SExpr::new(
                        SKind::Null,
                        Ty::Value(PropType {
                            nullable: true,
                            ..t.clone()
                        }),
                    );
                    if other_is_left {
                        compare(*op, other, null)?
                    } else {
                        compare(*op, null, other)?
                    }
                }
                Expr::Compare {
                    op,
                    left,
                    right,
                    at: written,
                } if is_untyped_list(left) != is_untyped_list(right) => {
                    // A `[]` takes the other operand's type.
                    let list_is_left = is_untyped_list(left);
                    let (list, other) = if list_is_left {
                        (left, right)
                    } else {
                        (right, left)
                    };
                    let other = self
                        .expr(sc, other, sub)
                        .map_err(|e| at_operand(e, other, written))?;
                    let list =
                        untyped_list_as(list, &other.ty).map_err(|e| at_written(e, written))?;
                    let compared = if list_is_left {
                        compare(*op, list, other)
                    } else {
                        compare(*op, other, list)
                    };
                    compared.map_err(|e| at_written(e, written))?
                }
                Expr::Compare {
                    op,
                    left,
                    right,
                    at: written,
                } => {
                    // A refusal inside a comparison is placed at it.
                    let l = self
                        .expr(sc, left, sub)
                        .map_err(|e| at_operand(e, left, written));
                    let r = self
                        .expr(sc, right, sub)
                        .map_err(|e| at_operand(e, right, written));
                    let (l, r) = self.both(sc, l, r)?;
                    if let Some(d) = list_equality_membership(*op, &l, &r, left, right, written) {
                        return Err(E::D(d));
                    }
                    let compared = literal_against_property(&l, &r, written, 1)
                        .and_then(|()| literal_against_property(&r, &l, written, 0))
                        .and_then(|()| compare(*op, l, r));
                    compared.map_err(|e| at_written(e, written))?
                }
                Expr::In {
                    needle,
                    list,
                    at: written,
                } => {
                    // `$d in $x` over a node is a triple over the edge named
                    // `in`, which is written in `match`; refused before the
                    // needle is typed, since only a triple would bind it.
                    if let (Expr::Variable(a), Expr::Variable(b)) = (needle.as_ref(), list.as_ref())
                        && !sc.is_error(a)
                        && !sc.is_error(b)
                    {
                        // A node on the right, or an unbound name beside a node
                        // when the schema has an edge named `in`.
                        let node = matches!(sc.col(b), Some(Ty::Node { .. }));
                        let unbound_edge = sc.col(b).is_none()
                            && matches!(sc.col(a), Some(Ty::Node { .. }))
                            && self.catalog.lookup_edge_by_name("in").is_some();
                        if node || unbound_edge {
                            let mut d = QueryDiagnostic::typecheck(
                            T7,
                            if node {
                                format!(
                                    "`${b}` is a node, so `${a} in ${b}` is a triple over the edge named `in`, an element of `match {{ … }}`, not a predicate"
                                )
                            } else {
                                format!(
                                    "`${b}` is bound by nothing but the edge named `in`, so `${a} in ${b}` is a triple over it, an element of `match {{ … }}`, not a predicate"
                                )
                            },
                        )
                        .with_fix(format!("match {{ …, ${a} in ${b} }}"));
                            d.position = Some(written.position);
                            d.end = Some(written.end_position);
                            return Err(E::D(d));
                        }
                    }
                    let n = self
                        .expr(sc, needle, sub)
                        .map_err(|e| at_operand(e, needle, written));
                    let l = if is_untyped_list(list) {
                        // A `[]` takes the needle's type as its element type.
                        match &n {
                            Ok(SExpr {
                                ty: Ty::Value(t), ..
                            }) if !t.list => untyped_list_as(
                                list,
                                &Ty::Value(PropType::list_of(t.scalar, false)),
                            ),
                            Ok(_) => Err(untyped_list_refusal()),
                            // The needle's own refusal is reported; this
                            // stand-in is never read.
                            Err(_) => Ok(SExpr::new(
                                SKind::Lit(untyped_list_literal(list)),
                                Ty::Value(PropType::list_of(ScalarType::String, false)),
                            )),
                        }
                    } else {
                        self.expr(sc, list, sub)
                            .map_err(|e| at_operand(e, list, written))
                    };
                    let (n, l) = self.both(sc, n, l)?;
                    literal_in_against_property(&n, &l, written)?;
                    let l = match &n.ty {
                        Ty::Value(t) if !t.list => {
                            adapt(l, &Ty::Value(PropType::list_of(t.scalar, false)))
                        }
                        _ => l,
                    };
                    let n = adapt(n, &l.ty.clone());
                    match &l.ty {
                        Ty::Node { .. } => {
                            return Err(err(T7, "`in` needs a list on the right, not a node"));
                        }
                        Ty::Value(t) if !t.list => {
                            return Err(err_fix(
                                T7,
                                format!(
                                    "`in` needs a list on the right; the right side is {}",
                                    t.display_name()
                                ),
                                "contains(<string>, <substring>) for a substring test",
                            ));
                        }
                        Ty::Value(t) => {
                            let mut elem = t.clone();
                            elem.list = false;
                            let n = adapt(n, &Ty::Value(elem.clone()));
                            match &n.ty {
                                Ty::Value(nt) if !nt.list && types_compatible(nt, &elem) => {}
                                other => {
                                    return Err(err(
                                        T7,
                                        format!(
                                            "`in` tests membership in {}: the needle must be {}, got {}",
                                            t.display_name(),
                                            elem.display_name(),
                                            other.display()
                                        ),
                                    ));
                                }
                            }
                            let nullable = n.ty.nullable() || l.ty.nullable();
                            SExpr::new(
                                SKind::In {
                                    needle: n.boxed(),
                                    list: l.boxed(),
                                },
                                Ty::scalar(ScalarType::Bool, nullable),
                            )
                        }
                        Ty::Edge { .. } => return Err(err(T7, "`in` reads a list, not an edge")),
                    }
                }
                Expr::IsNull { expr, negated } => {
                    let v = self.expr(sc, expr, sub)?;
                    SExpr::new(
                        SKind::IsNull {
                            expr: v.boxed(),
                            negated: *negated,
                        },
                        boolean,
                    )
                }
                Expr::Not(inner) => {
                    // `not exists { … }` that reads no outer name excludes every
                    // row or none.
                    let v = match inner.as_ref() {
                    Expr::Block(b) if b.func == BlockFunc::Exists => {
                        self.expr(sc, inner, sub).map_err(|e| match e {
                            E::D(d) if d.code == T39 => err(
                                T9,
                                "a `not exists { }` block must reference an outer-bound variable",
                            ),
                            other => other,
                        })?
                    }
                    _ => self.expr(sc, inner, sub)?,
                };
                    require_bool(&v, "`not`")?;
                    let ty = v.ty.clone();
                    SExpr::new(SKind::Not(v.boxed()), ty)
                }
                Expr::And(..) | Expr::Or(..) => {
                    let and = matches!(e, Expr::And(..));
                    // The chain of this connective as written, `((d1 op d2) op
                    // d3) …`: its operands in written order.
                    let mut operands = Vec::new();
                    let mut at = e;
                    loop {
                        match (at, and) {
                            (Expr::And(l, r), true) | (Expr::Or(l, r), false) => {
                                operands.push(r.as_ref());
                                at = l;
                            }
                            _ => {
                                operands.push(at);
                                break;
                            }
                        }
                    }
                    operands.reverse();
                    // Each node of the chain is an expression typed.
                    self.typed_exprs += operands.len().saturating_sub(2);
                    self.spend(WorkKind::Typing, operands.len().saturating_sub(2))?;
                    let what = if and { "`and`" } else { "`or`" };
                    // `a and b` evaluates `b` only where `a` is not false, `a or
                    // b` only where `a` is not true: each operand guards every
                    // operand after it, as one guard of its own, so a later
                    // operand's guard copies no earlier one. A chain that
                    // failed to type guards nothing after the failure.
                    let depth = sc.guards.len();
                    let mut chain: Option<R<SExpr>> = None;
                    for (i, operand) in operands.iter().enumerate() {
                        let typed = self.expr(sc, operand, sub);
                        let guard = typed.as_ref().ok().map(|t| lazy_guard(t, and, true));
                        chain = Some(match chain {
                            None => typed,
                            Some(left) => match self.both(sc, left, typed) {
                                Err(e) => Err(e),
                                Ok((a, b)) => {
                                    match (require_bool(&a, what), require_bool(&b, what)) {
                                        (Err(e), _) | (_, Err(e)) => Err(e),
                                        (Ok(()), Ok(())) => {
                                            let ty = Ty::scalar(
                                                ScalarType::Bool,
                                                a.ty.nullable() || b.ty.nullable(),
                                            );
                                            Ok(if and {
                                                SExpr::new(SKind::And(a.boxed(), b.boxed()), ty)
                                            } else {
                                                SExpr::new(SKind::Or(a.boxed(), b.boxed()), ty)
                                            })
                                        }
                                    }
                                }
                            },
                        });
                        let chained = matches!(chain, Some(Ok(_)));
                        match guard {
                            Some(guard) if chained && i + 1 < operands.len() => {
                                sc.push_guard(guard)
                            }
                            _ => sc.truncate_guards(depth),
                        }
                    }
                    sc.truncate_guards(depth);
                    chain.expect("a chain has two operands")?
                }
            })
        })
    }

    /// `e`, a `null` whatever it is typed as ([`is_untyped_null`]), typed
    /// as `want`, nullable: the literal, or an `if` whose branches or a
    /// `coalesce` whose arguments are all such, each typed the same way.
    fn null_as(&mut self, sc: &mut Scope, e: &Expr, want: &PropType) -> R<SExpr> {
        crate::stack::deep(|| {
            let mut want = want.clone();
            want.nullable = true;
            let ty = Ty::Value(want.clone());
            let Expr::Call(c) = e else {
                return Ok(SExpr::new(SKind::Null, ty));
            };
            let mut args = Vec::with_capacity(c.args.len());
            for (position, a) in c.args.iter().enumerate() {
                let Arg::Expr(arg) = a else {
                    unreachable!("`is_untyped_null` admits expression arguments only")
                };
                args.push(if c.name == "if" && position == 0 {
                    let cnd = self.expr(sc, arg, At::Value)?;
                    require_bool(&cnd, "`if`'s condition")?;
                    cnd
                } else {
                    self.null_as(sc, arg, &want)?
                });
            }
            Ok(SExpr::new(
                SKind::Call {
                    func: c.name.clone(),
                    args,
                    options: Vec::new(),
                },
                ty,
            ))
        })
    }

    fn call_scalar(&mut self, sc: &mut Scope, c: &Call, at: At) -> R<SExpr> {
        crate::stack::deep(|| {
            let name = c.name.as_str();
            if matches!(name, "nearest" | "bm25" | "rrf" | "distance") {
                return Err(E::D(ranking_as_scalar(name)));
            }
            if name == "union" || self.unit.contains_key(name) {
                return Err(err_fix(
                    T69,
                    format!("`{name}(…)` is a relation; a scalar is read here"),
                    format!("draw it with `from {name}(…)`, or aggregate over it in a block"),
                ));
            }
            if matches!(name, "search" | "match_text" | "fuzzy") {
                return Err(E::D(removed_lexical_call(c)));
            }
            if AGGREGATES.contains(&name) {
                return Err(err(
                    T8,
                    format!(
                        "`{name}(…)` is an aggregate; it belongs in `return`, or as a block `{name}($x.p) {{ … }}`"
                    ),
                ));
            }
            let mut args = Vec::new();
            // Each argument as typed, by position, for the guards of the ones
            // after it.
            let mut typed_at: Vec<Option<SExpr>> = Vec::new();
            // Every argument is typed, so a refusal in one does not hide an
            // independent refusal in another.
            let mut failed: Option<E> = None;
            // A `null` takes the type its context fixes (the GQ 3 RFC,
            // section 5): an argument of `if`'s branches or of `coalesce` that
            // is a `null` whatever it is typed as is typed after the others, as
            // their common type.
            let deferred: Vec<usize> = c
                .args
                .iter()
                .enumerate()
                .filter(|(position, a)| {
                    matches!(a, Arg::Expr(e) if is_untyped_null(e) || is_untyped_list(e))
                        && match name {
                            "if" => *position > 0,
                            "coalesce" => true,
                            _ => false,
                        }
                })
                .map(|(position, _)| position)
                .collect();
            let coalesce_depth = sc.guards.len();
            let (mut coalesce_guarding, mut coalesce_guarded) = (true, 0);
            for (position, a) in c.args.iter().enumerate() {
                match a {
                    Arg::Expr(_) if deferred.contains(&position) => typed_at.push(None),
                    Arg::Expr(e) => {
                        if matches!(e, Expr::Call(t) if t.name == "terms")
                            && matches!(name, "match_terms")
                        {
                            continue;
                        }
                        // `cast`'s target is a type name, read by its row.
                        if name == "cast" && position == 1 {
                            continue;
                        }
                        // The lazy rows evaluate an argument only where the ones
                        // before it select it: `if`'s branches under their
                        // condition, `coalesce`'s later arguments where every
                        // earlier one is null. Each guard reads the arguments as
                        // they were typed; nothing is typed again.
                        let earlier = |i: usize| typed_at.get(i).cloned().flatten();
                        let guard = match (name, position) {
                            ("if", 1) => earlier(0).map(|cnd| lazy_guard(&cnd, true, false)),
                            ("if", 2) => earlier(0).map(|cnd| lazy_guard(&cnd, false, true)),
                            _ => None,
                        };
                        // `coalesce`'s argument is evaluated where every earlier
                        // one is null: one guard per earlier argument, kept for
                        // the arguments after it. An earlier argument that is a
                        // `null` is null on every row: it guards nothing; one
                        // that failed to type leaves the rest unguarded.
                        if name == "coalesce" && coalesce_guarding {
                            for i in coalesce_guarded..position {
                                if deferred.contains(&i) {
                                    continue;
                                }
                                match earlier(i) {
                                    Some(before) => sc.push_guard(SExpr::new(
                                        SKind::IsNull {
                                            expr: before.boxed(),
                                            negated: false,
                                        },
                                        Ty::scalar(ScalarType::Bool, false),
                                    )),
                                    None => {
                                        sc.truncate_guards(coalesce_depth);
                                        coalesce_guarding = false;
                                        break;
                                    }
                                }
                            }
                            coalesce_guarded = position;
                        }
                        let depth = sc.guards.len();
                        if let Some(guard) = guard {
                            sc.push_guard(guard);
                        }
                        let typed = self.expr(sc, e, At::Value);
                        sc.truncate_guards(depth);
                        // Kept only where a later argument's guard reads it:
                        // `if`'s condition, `coalesce`'s arguments before
                        // its last. Copying every argument would copy a
                        // nested `if`'s whole else branch at each level.
                        let read_later = match name {
                            "if" => position == 0,
                            "coalesce" => position + 1 < c.args.len(),
                            _ => false,
                        };
                        typed_at.push(read_later.then(|| typed.as_ref().ok().cloned()).flatten());
                        match typed {
                            Ok(v) => args.push(v),
                            Err(e) => {
                                failed = Some(match failed.take() {
                                    None => e,
                                    Some(earlier) => self.join(sc, earlier, e),
                                })
                            }
                        }
                    }
                    Arg::Inline(_) => {
                        sc.truncate_guards(coalesce_depth);
                        return Err(err(
                            T69,
                            format!("`{name}` reads scalars; an inline read is a relation"),
                        ));
                    }
                }
            }
            sc.truncate_guards(coalesce_depth);
            if let Some(e) = failed {
                return Err(e);
            }
            if !deferred.is_empty() {
                // The type the other branch or arguments fix: the first of them,
                // which the row's own typing below joins with the rest.
                let fixed = match name {
                    "if" => args.get(1).map(|other| other.ty.clone()),
                    _ => args.first().map(|other| other.ty.clone()),
                };
                let Some(Ty::Value(want)) = fixed else {
                    return Err(untyped_null_refusal());
                };
                for &position in &deferred {
                    let Some(Arg::Expr(e)) = c.args.get(position) else {
                        unreachable!("a deferred argument is an expression")
                    };
                    let typed = if is_untyped_list(e) {
                        untyped_list_as(e, &Ty::Value(want.clone()))?
                    } else {
                        self.null_as(sc, e, &want)?
                    };
                    args.insert(position.min(args.len()), typed);
                }
            }
            let bool_ty = |nullable| Ty::scalar(ScalarType::Bool, nullable);
            let string =
                |t: &Ty| matches!(t, Ty::Value(p) if p.scalar == ScalarType::String && !p.list);
            let no_options = |c: &Call| -> R<()> {
                match c.options.first() {
                    Some((o, _)) => Err(err(
                        T71,
                        format!("`{}` has no option `{o}`; it admits none", c.name),
                    )),
                    None => Ok(()),
                }
            };
            let search_field = |this: &Self, args: &[SExpr]| -> R<()> {
                let Some(SExpr {
                    kind: SKind::Prop { col, prop },
                    ..
                }) = args.first()
                else {
                    return Err(err(
                        T19,
                        format!("`{name}`'s first argument is a property of a node binding"),
                    ));
                };
                let ty = sc
                    .local(col)
                    .or_else(|| sc.outer_col(col))
                    .map(|c| c.ty.clone());
                match ty {
                    Some(Ty::Node { type_name, .. }) => {
                        if !this
                            .catalog
                            .query_node_type(&type_name)
                            .is_some_and(|n| n.is_lexical_field(prop))
                        {
                            return Err(err(
                                T27,
                                format!("`{name}` needs a full-text index on `{type_name}.{prop}`"),
                            ));
                        }
                        Ok(())
                    }
                    _ => Err(err(T19, format!("`{name}` searches a node property"))),
                }
            };
            let out = match name {
                "contains" | "starts_with" => {
                    no_options(c)?;
                    let [a, b] = args.as_slice() else {
                        return Err(err(T7, format!("`{name}` takes two Strings")));
                    };
                    if let Ty::Value(t) = &a.ty
                        && t.list
                        && name == "contains"
                    {
                        return Err(err_fix(
                            T7,
                            "`contains` tests a substring of a String; a list is matched with `in`",
                            "<value> in <list>",
                        ));
                    }
                    if !string(&a.ty) || !string(&b.ty) {
                        return Err(err(
                            T7,
                            format!(
                                "`{name}` takes two Strings, got {} and {}",
                                a.ty.display(),
                                b.ty.display()
                            ),
                        ));
                    }
                    if let (
                        SKind::Prop { prop, .. },
                        Ty::Value(expected),
                        SKind::Lit(Literal::String(value)),
                    ) = (&a.kind, &a.ty, &b.kind)
                        && let Some(mismatch) =
                            literal::tested_enum_literal(name, value, expected, prop)
                    {
                        return Err(literal_refusal(mismatch, None, None));
                    }
                    let ty = bool_ty(a.ty.nullable() || b.ty.nullable());
                    SExpr::new(
                        SKind::Call {
                            func: name.into(),
                            args,
                            options: Vec::new(),
                        },
                        ty,
                    )
                }
                "match_terms" => {
                    no_options(c)?;
                    search_field(self, &args)?;
                    let Some(Arg::Expr(Expr::Call(t))) = c.args.get(1) else {
                        return Err(err(
                            T19,
                            "`match_terms(field, terms(…))` takes a `terms` value",
                        ));
                    };
                    if t.name != "terms" {
                        return Err(err(
                            T19,
                            "`match_terms(field, terms(…))` takes a `terms` value",
                        ));
                    }
                    let q = self.terms(sc, t)?;
                    let mut args = args;
                    args.push(q.text);
                    SExpr::new(
                        SKind::Call {
                            func: "match_terms".into(),
                            args,
                            options: vec![
                                (
                                    "mode".into(),
                                    SExpr::new(
                                        SKind::Lit(Literal::String(
                                            if q.mode_all { "all" } else { "any" }.into(),
                                        )),
                                        Ty::scalar(ScalarType::String, false),
                                    ),
                                ),
                                ("max_edits".into(), q.max_edits),
                            ],
                        },
                        bool_ty(false),
                    )
                }
                "terms" => {
                    return Err(err(
                        T19,
                        "`terms(…)` is a lexical query; it is read by `match_terms` and `bm25` only",
                    ));
                }
                "now" => {
                    no_options(c)?;
                    if !args.is_empty() {
                        return Err(err(T7, "`now()` takes no argument"));
                    }
                    SExpr::new(
                        SKind::Call {
                            func: "now".into(),
                            args,
                            options: Vec::new(),
                        },
                        Ty::scalar(ScalarType::DateTime, false),
                    )
                }
                "if" => {
                    no_options(c)?;
                    // The arguments are moved, never copied: a nested `if`'s
                    // else branch holds the rest of the chain.
                    let Ok([cnd, a, b]) = <[SExpr; 3]>::try_from(args) else {
                        return Err(err(T7, "`if(c, a, b)` takes three arguments"));
                    };
                    require_bool(&cnd, "`if`'s condition")?;
                    let ty = common(&a, &b, "if")?;
                    let (a, b) = (fit(a, &ty), fit(b, &ty));
                    SExpr::new(
                        SKind::Call {
                            func: "if".into(),
                            args: vec![cnd, a, b],
                            options: Vec::new(),
                        },
                        ty,
                    )
                }
                "coalesce" => {
                    no_options(c)?;
                    let Some(first) = args.first() else {
                        return Err(err(T7, "`coalesce` takes one or more arguments"));
                    };
                    // The first argument meets the second as itself, so a literal
                    // there takes the other argument's type.
                    let mut ty = first.ty.clone();
                    for (i, a) in args.iter().enumerate().skip(1) {
                        let null;
                        let so_far = if i == 1 {
                            first
                        } else {
                            null = SExpr::new(SKind::Null, ty.clone());
                            &null
                        };
                        ty = common(so_far, a, "coalesce")?;
                    }
                    let all_null = args.iter().all(|a| a.ty.nullable());
                    let ty = ty.with_nullable(all_null);
                    let args = args.into_iter().map(|a| fit(a, &ty)).collect();
                    SExpr::new(
                        SKind::Call {
                            func: "coalesce".into(),
                            args,
                            options: Vec::new(),
                        },
                        ty,
                    )
                }
                "concat" => {
                    no_options(c)?;
                    if args.is_empty() || !args.iter().all(|a| string(&a.ty)) {
                        return Err(err(T7, "`concat` takes one or more Strings"));
                    }
                    let nullable = args.iter().any(|a| a.ty.nullable());
                    SExpr::new(
                        SKind::Call {
                            func: "concat".into(),
                            args,
                            options: Vec::new(),
                        },
                        Ty::scalar(ScalarType::String, nullable),
                    )
                }
                "lower" | "upper" | "trim" | "ltrim" | "rtrim" => {
                    no_options(c)?;
                    let [x] = one_string(name, args)?;
                    let ty = x.ty.clone();
                    call(name, vec![x], ty)
                }
                "length" => {
                    no_options(c)?;
                    if let [x] = args.as_slice()
                        && matches!(&x.ty, Ty::Value(t) if t.list)
                    {
                        return Err(err_fix(
                            T7,
                            "`length` counts a String's characters; a list's elements are `size`",
                            "size(<list>)",
                        ));
                    }
                    let [x] = one_string(name, args)?;
                    let nullable = x.ty.nullable();
                    call(name, vec![x], Ty::scalar(ScalarType::I64, nullable))
                }
                "left" | "right" => {
                    no_options(c)?;
                    let Ok([x, n]) = <[SExpr; 2]>::try_from(args) else {
                        return Err(err(
                            T7,
                            format!("`{name}(s, n)` takes a String and a count"),
                        ));
                    };
                    let [x] = one_string(name, vec![x])?;
                    let n = integer_arg(name, "count", n)?;
                    let nullable = x.ty.nullable() || n.ty.nullable();
                    call(name, vec![x, n], Ty::scalar(ScalarType::String, nullable))
                }
                "substring" => {
                    no_options(c)?;
                    let Ok([x, start, len]) = <[SExpr; 3]>::try_from(args) else {
                        return Err(err(
                            T7,
                            "`substring(s, start, len)` takes a String, a 1-based start and a length",
                        ));
                    };
                    let [x] = one_string(name, vec![x])?;
                    let start = integer_arg(name, "start", start)?;
                    let len = integer_arg(name, "length", len)?;
                    let nullable = x.ty.nullable() || start.ty.nullable() || len.ty.nullable();
                    call(
                        name,
                        vec![x, start, len],
                        Ty::scalar(ScalarType::String, nullable),
                    )
                }
                "abs" | "floor" | "ceil" => {
                    no_options(c)?;
                    let Ok([x]) = <[SExpr; 1]>::try_from(args) else {
                        return Err(err(T7, format!("`{name}` takes one number")));
                    };
                    let ty = numeric(&x, &format!("`{name}`"))?;
                    call(name, vec![x], ty)
                }
                "round" => {
                    let Ok([x]) = <[SExpr; 1]>::try_from(args) else {
                        return Err(err(T7, "`round(x, digits: n)` takes one number"));
                    };
                    let ty = numeric(&x, "`round`")?;
                    let mut digits = None;
                    for (o, v) in &c.options {
                        if o != "digits" || digits.is_some() {
                            return Err(err(
                                T71,
                                format!("`round` has no option `{o}`; it admits `digits`"),
                            ));
                        }
                        digits = Some(integer_arg(name, "`digits`", self.expr(sc, v, At::Value)?)?);
                    }
                    let digits = digits.unwrap_or_else(|| {
                        SExpr::new(
                            SKind::Lit(Literal::Integer(0)),
                            Ty::scalar(ScalarType::I64, false),
                        )
                    });
                    let ty = ty.with_nullable(ty.nullable() || digits.ty.nullable());
                    SExpr::new(
                        SKind::Call {
                            func: name.into(),
                            args: vec![x],
                            options: vec![("digits".into(), digits)],
                        },
                        ty,
                    )
                }
                "sqrt" | "exp" | "ln" | "log10" => {
                    no_options(c)?;
                    let Ok([x]) = <[SExpr; 1]>::try_from(args) else {
                        return Err(err(T7, format!("`{name}` takes one number")));
                    };
                    numeric(&x, &format!("`{name}`"))?;
                    let nullable = x.ty.nullable();
                    let x = widen(x, ScalarType::F64);
                    call(name, vec![x], Ty::scalar(ScalarType::F64, nullable))
                }
                "power" => {
                    no_options(c)?;
                    let Ok([b, e]) = <[SExpr; 2]>::try_from(args) else {
                        return Err(err(T7, "`power(base, exponent)` takes two numbers"));
                    };
                    let b = adapt(b, &Ty::scalar(ScalarType::F64, false));
                    let e = adapt(e, &Ty::scalar(ScalarType::F64, false));
                    numeric(&b, "`power`'s base")?;
                    numeric(&e, "`power`'s exponent")?;
                    let nullable = b.ty.nullable() || e.ty.nullable();
                    let (b, e) = (widen(b, ScalarType::F64), widen(e, ScalarType::F64));
                    call(name, vec![b, e], Ty::scalar(ScalarType::F64, nullable))
                }
                "size" => {
                    no_options(c)?;
                    let Ok([x]) = <[SExpr; 1]>::try_from(args) else {
                        return Err(err(T7, "`size` takes one list"));
                    };
                    if !matches!(&x.ty, Ty::Value(t) if t.list) {
                        return Err(err(
                            T7,
                            format!("`size` counts a list's elements, got {}", x.ty.display()),
                        ));
                    }
                    let nullable = x.ty.nullable();
                    call(name, vec![x], Ty::scalar(ScalarType::I64, nullable))
                }
                "cast" => {
                    no_options(c)?;
                    let target = match c.args.get(1) {
                        Some(Arg::Expr(Expr::Ident(t))) => crate::ir::expr::cast_target(t)
                            .ok_or_else(|| {
                                err(T7, format!("`cast` converts to a scalar type, not `{t}`"))
                            })?,
                        // `Vector(3)` reads as a call of a capitalized name.
                        Some(Arg::Expr(Expr::Call(t)))
                            if t.name.starts_with(|ch: char| ch.is_ascii_uppercase()) =>
                        {
                            return Err(err(
                                T7,
                                format!(
                                    "`cast` converts to a scalar type, not `{}`",
                                    crate::query::syntax::print::expr(&Expr::Call(t.clone()))
                                ),
                            ));
                        }
                        _ => {
                            return Err(err(
                                T7,
                                "`cast(x, T)` takes a value and a scalar type name",
                            ));
                        }
                    };
                    let Ok([x]) = <[SExpr; 1]>::try_from(args) else {
                        return Err(err(T7, "`cast(x, T)` takes a value and a scalar type name"));
                    };
                    let from = match &x.ty {
                        Ty::Value(t) if !t.list => t.scalar,
                        other => {
                            return Err(err(
                                T7,
                                format!("`cast` converts a scalar, got {}", other.display()),
                            ));
                        }
                    };
                    // An integer literal is a number of the target's kind.
                    let x = adapt(x, &Ty::scalar(target, false));
                    let from = match &x.ty {
                        Ty::Value(t) => t.scalar,
                        _ => from,
                    };
                    if !crate::ir::expr::cast_converts(from, target) {
                        return Err(err(
                            T7,
                            format!(
                                "`cast` does not convert {} to {}",
                                Ty::scalar(from, false).display(),
                                Ty::scalar(target, false).display()
                            ),
                        ));
                    }
                    let nullable = x.ty.nullable();
                    call(name, vec![x], Ty::scalar(target, nullable))
                }
                other => {
                    return Err(err(
                        T6,
                        format!("`{other}` is not a registry row or a declared read"),
                    ));
                }
            };
            let _ = at;
            Ok(out)
        })
    }
}

#[derive(Clone, Copy)]
enum At {
    Filter,
    Value,
}

enum RelArg<'a> {
    Rel(&'a RelExpr),
    Inline(&'a ReadBody),
    Expr(&'a Expr),
}

// ---------------------------------------------------------------------------
// Folding steps into terms.

/// The term nodes and expression nodes a construction built, the work the
/// plan budget charges for it ([`crate::budget`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Built {
    terms: usize,
    exprs: usize,
    /// Term nodes rewritten rather than built, charged as typing work.
    steps: usize,
}

impl Built {
    fn node(&mut self, exprs: usize) {
        self.terms += 1;
        self.exprs += exprs;
    }
}

/// [`Term::can_fail`] over the terms one compile builds, each shared
/// subterm walked once: a term built over earlier ones (a scope's fold, a
/// block over its inner read) reads their verdicts instead of walking them
/// again. It keeps every subterm it has a verdict for alive, so no address
/// it holds is reused for another term.
#[derive(Default)]
struct FailMemo {
    known: HashMap<*const Term, bool>,
    alive: Vec<Arc<Term>>,
}

impl FailMemo {
    fn term(&mut self, t: &Term) -> bool {
        crate::stack::deep(|| {
            t.fails_here()
                || match t {
                    Term::Fuse { arms, .. } => arms.iter().any(|a| self.term(a)),
                    other => other.shared_children().into_iter().any(|c| self.shared(c)),
                }
        })
    }

    fn shared(&mut self, t: &Arc<Term>) -> bool {
        let at = Arc::as_ptr(t);
        if let Some(fails) = self.known.get(&at) {
            return *fails;
        }
        let fails = self.term(t);
        self.known.insert(at, fails);
        self.alive.push(Arc::clone(t));
        fails
    }
}

/// The nodes of a bound expression.
fn ir_nodes(e: &IRExpr) -> usize {
    let mut n = 0;
    e.visit_nodes(|_| n += 1);
    n
}

/// [`apply`], and the nodes it built: a term the step holds (a leaf, an
/// `optional`'s pattern, a block's inner term) was charged when it was
/// built and is not counted again.
fn apply_counted(acc: Option<Term>, step: Step, memo: &mut FailMemo) -> (Term, Built) {
    let mut built = Built::default();
    let correlate = |acc: Option<Term>, t: Term, built: &mut Built, memo: &mut FailMemo| match acc {
        None => t,
        Some(a) => {
            built.node(0);
            let inner = t.boxed();
            Term::Correlate {
                outer: a.boxed(),
                fails: memo.shared(&inner),
                inner,
            }
        }
    };
    let term = match step {
        Step::Leaf(t) => correlate(acc, t, &mut built, memo),
        Step::Expand {
            src,
            dst,
            src_type,
            dst_type,
            edges,
            min_hops,
            max_hops,
            edge_var,
            semi,
        } => {
            let target = semi.clone().unwrap_or_else(|| dst.clone());
            built.node(0);
            let e = Term::Expand {
                input: acc.unwrap_or(Term::Unit).boxed(),
                src,
                dst: target.clone(),
                src_type,
                dst_type: dst_type.clone(),
                edges,
                min_hops,
                max_hops,
                edge_var,
            };
            match semi {
                None => e,
                Some(t) => {
                    let pred = SExpr::new(
                        SKind::Cmp {
                            op: CompOp::Eq,
                            left: SExpr::new(
                                SKind::Col(t),
                                Ty::Node {
                                    type_name: dst_type.clone(),
                                    nullable: false,
                                },
                            )
                            .boxed(),
                            right: SExpr::new(
                                SKind::Col(dst),
                                Ty::Node {
                                    type_name: dst_type,
                                    nullable: false,
                                },
                            )
                            .boxed(),
                        },
                        Ty::scalar(ScalarType::Bool, false),
                    )
                    .to_ir();
                    built.node(ir_nodes(&pred));
                    Term::Filter {
                        input: e.boxed(),
                        pred,
                        fails: false,
                    }
                }
            }
        }
        Step::Filter { blocks, pred } => {
            // The conjunct can fail where its predicate or a block it reads
            // can: the one computation of it the planner reads (A3).
            let fails = pred.can_fail || blocks.iter().any(|b| b.can_fail(memo));
            let t = with_blocks(acc.unwrap_or(Term::Unit), blocks, &mut built, memo);
            if matches!(pred.kind, SKind::Lit(Literal::Bool(true))) {
                return (t, built);
            }
            let pred = pred.to_ir();
            built.node(ir_nodes(&pred));
            Term::Filter {
                input: t.boxed(),
                pred,
                fails,
            }
        }
        Step::Let {
            blocks,
            name,
            value,
        } => {
            let t = with_blocks(acc.unwrap_or(Term::Unit), blocks, &mut built, memo);
            let value_ir = value.to_ir();
            built.node(ir_nodes(&value_ir));
            Term::Project {
                input: t.boxed(),
                fails: value.can_fail,
                items: vec![(name, value_ir)],
                keep: true,
            }
        }
        Step::Optional {
            inner,
            new_cols,
            env,
        } => {
            // Correlate(R, Union(p, Project(Filter(Env, ¬ exists p), nulls))):
            // for each outer row, p's rows, or, when p has none, one row that
            // keeps the outer columns p reads and is null in each column p
            // adds. R is written once, so a run of optionals grows the tree
            // linearly, and p is stored once, shared by the two arms, so
            // nesting optionals does too.
            let inner = inner.boxed();
            let col = "_opt".to_string();
            let row = if env.is_empty() {
                Term::Unit
            } else {
                Term::Env(env.clone())
            };
            // The row, the `Correlate` and `Group` over the pattern, the
            // `Filter` on its absence, the padding `Project` and the `Union`;
            // the `Correlate` with the outer rows is counted where it is
            // built.
            built.terms += 6;
            built.exprs += 1;
            let unmatched = Term::Filter {
                input: Term::Correlate {
                    outer: row.boxed(),
                    fails: memo.shared(&inner),
                    inner: Term::Group {
                        input: Arc::clone(&inner),
                        keys: Vec::new(),
                        aggs: vec![(col.clone(), count_rows())],
                    }
                    .boxed(),
                }
                .boxed(),
                pred: {
                    let pred = not_exists(&col).to_ir();
                    built.exprs += ir_nodes(&pred);
                    pred
                },
                fails: false,
            };
            // The arms of a `Union` match by name, so the unmatched row
            // holds every column `p` holds, in `p`'s order: the outer
            // columns it reads, and null in each column it adds, the names
            // it binds and the ones its lowering made up alike.
            // Each column is looked up, never searched for, so the row
            // costs what it holds.
            let types = column_types(&inner);
            let first = |cols: &[Col]| -> HashMap<String, Ty> {
                let mut at = HashMap::with_capacity(cols.len());
                for c in cols {
                    at.entry(c.name.clone()).or_insert_with(|| c.ty.clone());
                }
                at
            };
            let (env_at, new_at) = (first(&env), first(&new_cols));
            let items: Vec<(String, IRExpr)> = output_columns(&inner)
                .into_iter()
                .filter_map(|name| {
                    if let Some(ty) = env_at.get(&name) {
                        let read = SExpr::new(SKind::Col(name.clone()), ty.clone());
                        return Some((name, read.to_ir()));
                    }
                    let ty = match new_at.get(&name) {
                        Some(ty) => ty.with_nullable(true).expr_type(),
                        None => nullable_type(types.get(&name)?.clone()),
                    };
                    Some((name, IRExpr::Literal(Literal::Null, ty)))
                })
                .collect();
            built.exprs += items.len();
            correlate(
                Some(acc.unwrap_or(Term::Unit)),
                Term::Union {
                    left: inner,
                    right: Term::Project {
                        input: unmatched.boxed(),
                        items,
                        keep: false,
                        fails: false,
                    }
                    .boxed(),
                },
                &mut built,
                memo,
            )
        }
    };
    (term, built)
}

/// A block typed in an expression: its inner term, the column its aggregate
/// fills, the aggregate, and, for a block that can fail written under a
/// guard, the column its guard is.
#[derive(Debug, Clone)]
struct BlockAgg {
    inner: Term,
    col: String,
    agg: Agg,
    guard: Option<BlockGuard>,
}

/// The guard a block runs under, as a column of the outer row: `column`,
/// computed by the last of `columns`, each a guard column the block computes
/// first, in order. Each holds one guard as typed where it was written and
/// reads the column before it for the guards around that one, so a guard
/// another block already computed is read, never copied.
#[derive(Debug, Clone)]
struct BlockGuard {
    columns: Vec<(String, SExpr)>,
    column: String,
}

impl BlockAgg {
    /// The block can fail where it is evaluated: its inner term or its
    /// aggregate's argument.
    fn can_fail(&self, memo: &mut FailMemo) -> bool {
        memo.term(&self.inner) || self.agg.arg.as_ref().is_some_and(IRExpr::can_fail)
    }
}

/// Each block over `t`: `Correlate(t, Group(inner, [], [agg]))`, one row per
/// outer row. A guarded block's guard is computed once as a column of the
/// outer row (`Project`) and its inner term's `Env` row is kept only where
/// that column holds, so on an outer row the guard rejects the block reads
/// nothing and evaluates nothing: `count` 0, every other aggregate null.
fn with_blocks(mut t: Term, blocks: Vec<BlockAgg>, built: &mut Built, memo: &mut FailMemo) -> Term {
    for BlockAgg {
        inner,
        col,
        agg,
        guard,
    } in blocks
    {
        let inner = match guard {
            None => inner,
            Some(BlockGuard { columns, column }) => {
                for (name, guard) in columns {
                    let guard_ir = guard.to_ir();
                    built.node(ir_nodes(&guard_ir));
                    t = Term::Project {
                        input: t.boxed(),
                        fails: guard.can_fail,
                        items: vec![(name, guard_ir)],
                        keep: true,
                    };
                }
                condition_env(inner, &column, built)
            }
        };
        // The `Group` with its aggregate and the `Correlate`.
        built.node(1 + agg.arg.as_ref().map_or(0, ir_nodes));
        built.node(0);
        let inner = Term::Group {
            input: inner.boxed(),
            keys: Vec::new(),
            aggs: vec![(col, agg)],
        }
        .boxed();
        t = Term::Correlate {
            outer: t.boxed(),
            fails: memo.shared(&inner),
            inner,
        };
    }
    t
}

/// `t` with its `Env` row (the leaf its own steps were built on) kept only
/// where the outer Boolean column `guard` holds; the `Env` exposes `guard`
/// beside the columns it already read.
fn condition_env(t: Term, guard: &str, built: &mut Built) -> Term {
    crate::stack::deep(|| {
        let boolean = Ty::scalar(ScalarType::Bool, false);
        // Each node on the way down to the `Env` is rebuilt: a step of the
        // type checker's work.
        built.steps += 1;
        let mut step =
            |input: Arc<Term>| Arc::new(condition_env(Arc::unwrap_or_clone(input), guard, built));
        match t {
            Term::Env(mut cols) => {
                built.node(1);
                cols.push(Col {
                    name: guard.to_string(),
                    ty: boolean.clone(),
                });
                Term::Filter {
                    input: Term::Env(cols).boxed(),
                    pred: SExpr::new(SKind::Col(guard.to_string()), boolean).to_ir(),
                    fails: false,
                }
            }
            Term::Expand {
                input,
                src,
                dst,
                src_type,
                dst_type,
                edges,
                min_hops,
                max_hops,
                edge_var,
            } => Term::Expand {
                input: step(input),
                src,
                dst,
                src_type,
                dst_type,
                edges,
                min_hops,
                max_hops,
                edge_var,
            },
            Term::Filter { input, pred, fails } => Term::Filter {
                input: step(input),
                pred,
                fails,
            },
            Term::Project {
                input,
                items,
                keep,
                fails,
            } => Term::Project {
                input: step(input),
                items,
                keep,
                fails,
            },
            Term::Group { input, keys, aggs } => Term::Group {
                input: step(input),
                keys,
                aggs,
            },
            Term::Correlate {
                outer,
                inner,
                fails,
            } => Term::Correlate {
                outer: step(outer),
                inner,
                fails,
            },
            Term::Order { input, keys } => Term::Order {
                input: step(input),
                keys,
            },
            Term::Cut { input, k } => Term::Cut {
                input: step(input),
                k,
            },
            other => other,
        }
    })
}

/// The guard a lazy row's later operand is evaluated under, over the
/// earlier operand `e` as it was typed: the rows where `e` is `when`, a null
/// `e` selected when `on_null`. `b` in `a and b` runs where `a` is true or
/// null, in `a or b` where it is false or null; `if`'s branch where its
/// condition is true, its else branch where it is false or null.
fn lazy_guard(e: &SExpr, when: bool, on_null: bool) -> SExpr {
    let boolean = Ty::scalar(ScalarType::Bool, false);
    let lit = |b: bool| SExpr::new(SKind::Lit(Literal::Bool(b)), boolean.clone());
    let fallback = if when { on_null } else { !on_null };
    let coalesced = SExpr::new(
        SKind::Call {
            func: "coalesce".into(),
            args: vec![e.clone(), lit(fallback)],
            options: Vec::new(),
        },
        boolean.clone(),
    );
    if when {
        coalesced
    } else {
        SExpr::new(SKind::Not(coalesced.boxed()), boolean)
    }
}

/// Whether `t` holds a recursive reference or a fixpoint, whose typing
/// depends on the call frames it was typed under.
fn reaches_recursion(t: &Term) -> bool {
    t.any_node(|node| matches!(node, Term::RecRef { .. } | Term::Fix { .. }))
}

/// The column names a term produces (types are not needed by the fold).
/// The type of the column `name` that `t` outputs, from the node that
/// adds it.
/// The type of each column `t` adds, by name: [`output_columns`]'s names,
/// each with the type of the node that adds it, the first such node where
/// two do (an item over its input, an outer column over an inner one), in
/// one walk.
fn column_types(t: &Term) -> HashMap<String, ExprType> {
    crate::stack::deep(|| {
        // `first` holds the names that take precedence over `then`'s.
        let over = |mut then: HashMap<String, ExprType>, first: HashMap<String, ExprType>| {
            then.extend(first);
            then
        };
        let listed = |pairs: Vec<(String, ExprType)>| {
            let mut out = HashMap::with_capacity(pairs.len());
            for (n, ty) in pairs {
                out.entry(n).or_insert(ty);
            }
            out
        };
        let value = |scalar| ExprType::Value {
            scalar,
            list: false,
            nullable: false,
        };
        match t {
            Term::Unit => HashMap::new(),
            Term::Env(cols) => listed(
                cols.iter()
                    .map(|c| (c.name.clone(), c.ty.expr_type()))
                    .collect(),
            ),
            Term::Scan { var, ty } => HashMap::from([(var.clone(), ty.expr_type())]),
            Term::Expand {
                input,
                dst,
                dst_type,
                edges,
                edge_var,
                ..
            } => {
                let mut own = Vec::new();
                own.push((
                    dst.clone(),
                    ExprType::Node {
                        type_name: dst_type.clone(),
                    },
                ));
                if let Some(e) = edge_var {
                    own.push((
                        e.clone(),
                        ExprType::Edge {
                            type_name: edges
                                .members()
                                .iter()
                                .map(|m| m.edge_type.as_str())
                                .collect::<Vec<_>>()
                                .join("|"),
                        },
                    ));
                }
                over(column_types(input), listed(own))
            }
            Term::Filter { input, .. } | Term::Order { input, .. } | Term::Cut { input, .. } => {
                column_types(input)
            }
            Term::Project {
                input, items, keep, ..
            } => {
                let own = listed(
                    items
                        .iter()
                        .map(|(n, e)| (n.clone(), e.ty().clone()))
                        .collect(),
                );
                if *keep {
                    over(column_types(input), own)
                } else {
                    own
                }
            }
            Term::Group { keys, aggs, .. } => over(
                listed(
                    aggs.iter()
                        .map(|(n, a)| (n.clone(), a.ty.expr_type()))
                        .collect(),
                ),
                listed(
                    keys.iter()
                        .map(|(n, e)| (n.clone(), e.ty().clone()))
                        .collect(),
                ),
            ),
            Term::Correlate { outer, inner, .. } => over(column_types(inner), column_types(outer)),
            Term::Union { left, .. } => column_types(left),
            Term::Score {
                input, kind, out, ..
            } => over(
                column_types(input),
                HashMap::from([(
                    out.clone(),
                    value(match kind {
                        ScoreKind::Distance => ScalarType::F32,
                        ScoreKind::Bm25 => ScalarType::F64,
                    }),
                )]),
            ),
            Term::ApproxTopK { input, out, .. } => over(
                column_types(input),
                HashMap::from([(out.clone(), value(ScalarType::F32))]),
            ),
            Term::Fuse {
                arms, target, out, ..
            } => {
                let mut own = HashMap::from([(out.clone(), value(ScalarType::F64))]);
                if target != out
                    && let Some(ty) = arms.iter().find_map(|a| column_types(a).remove(target))
                {
                    own.insert(target.clone(), ty);
                }
                own
            }
            Term::Fix { body, .. } | Term::Call { body, .. } => column_types(body),
            Term::RecRef { ty, .. } => listed(
                ty.cols
                    .iter()
                    .map(|c| (c.name.clone(), c.ty.expr_type()))
                    .collect(),
            ),
        }
    })
}

/// `ty`, admitting null: a value type's nullable form; a reference column
/// is nullable as it is.
fn nullable_type(ty: ExprType) -> ExprType {
    match ty {
        ExprType::Value { scalar, list, .. } => ExprType::Value {
            scalar,
            list,
            nullable: true,
        },
        ExprType::ExactInteger { list, .. } => ExprType::ExactInteger {
            list,
            nullable: true,
        },
        other => other,
    }
}

fn output_columns(t: &Term) -> Vec<String> {
    crate::stack::deep(|| match t {
        Term::Unit => Vec::new(),
        Term::Env(cols) => cols.iter().map(|c| c.name.clone()).collect(),
        Term::Scan { var, .. } => vec![var.clone()],
        Term::Expand {
            input,
            dst,
            edge_var,
            ..
        } => {
            let mut v = output_columns(input);
            v.push(dst.clone());
            if let Some(e) = edge_var {
                v.push(e.clone());
            }
            v
        }
        Term::Filter { input, .. } | Term::Order { input, .. } | Term::Cut { input, .. } => {
            output_columns(input)
        }
        Term::Project {
            input, items, keep, ..
        } => {
            let mut v = if *keep {
                output_columns(input)
            } else {
                Vec::new()
            };
            v.extend(items.iter().map(|(n, _)| n.clone()));
            v
        }
        Term::Group { keys, aggs, .. } => keys
            .iter()
            .map(|(n, _)| n.clone())
            .chain(aggs.iter().map(|(n, _)| n.clone()))
            .collect(),
        Term::Correlate { outer, inner, .. } => {
            let mut v = output_columns(outer);
            let mut seen: HashSet<String> = v.iter().cloned().collect();
            for c in output_columns(inner) {
                if seen.insert(c.clone()) {
                    v.push(c);
                }
            }
            v
        }
        Term::Union { left, .. } => output_columns(left),
        Term::Score { input, out, .. } | Term::ApproxTopK { input, out, .. } => {
            let mut v = output_columns(input);
            v.push(out.clone());
            v
        }
        Term::Fuse { target, out, .. } => vec![target.clone(), out.clone()],
        Term::Fix { body, .. } | Term::Call { body, .. } => output_columns(body),
        Term::RecRef { ty, .. } => ty.names(),
    })
}

// ---------------------------------------------------------------------------
// Monotonicity (section 2, Fix; section 3, Recursion).

/// Check the body of a self-calling read against the monotonicity rules and
/// return its lattice columns (those its own `return` takes `min` or `max`
/// of).
fn monotone(name: &str, body: &Term, rounds: Option<u64>) -> R<Vec<String>> {
    // The read's own return: Project(Cut?(Order?(Group | Project))).
    let mut own = body;
    loop {
        match own {
            Term::Project {
                input, keep: false, ..
            } => own = input,
            Term::Cut { input, .. } | Term::Order { input, .. } => {
                if depends(input, name) {
                    return Err(t70(name, "an `order` or `limit` over the recursive rows"));
                }
                own = input;
            }
            _ => break,
        }
    }
    let (input, lattice): (&Term, Vec<String>) = match own {
        Term::Group { input, keys, aggs } => {
            let mut lat = Vec::new();
            for (n, a) in aggs {
                match a.func {
                    AggFunc::Min | AggFunc::Max => lat.push(n[1..].to_string()),
                    f => {
                        return Err(t70(
                            name,
                            &format!(
                                "the aggregate `{f}` in its return; a recursive read returns `distinct` or `min`/`max`"
                            ),
                        ));
                    }
                }
            }
            for (_, k) in keys {
                let mut cols = Vec::new();
                k.columns(&mut cols);
                let _ = cols;
            }
            (input, lat)
        }
        Term::Project { .. } => {
            return Err(t70(
                name,
                "a return that is neither `distinct` nor `min`/`max`; over a bag the fixpoint does not exist",
            ));
        }
        other => (other, Vec::new()),
    };
    // Lattice columns as the recursive relation names them (its result
    // columns are the `return` names).
    let mut walker = Walk {
        name,
        lattice: lattice.clone(),
        blocks: Vec::new(),
    };
    let computed = walker.term(input)?;
    // A column the read computes rather than reads must be under min or max,
    // and a round budget declared.
    if let Term::Group { keys, aggs, .. } = own {
        for (n, k) in keys {
            if reads_any(k, &computed) || !k.is_column_read() && !matches!(k, IRExpr::Param(..)) {
                return Err(t70(
                    name,
                    &format!(
                        "the computed column `{}` in its return without `min` or `max`; that is value invention with no fixpoint",
                        &n[1..]
                    ),
                ));
            }
        }
        for (n, a) in aggs {
            if let Some(arg) = &a.arg
                && reads_any(arg, &computed)
                && rounds.is_none()
            {
                return Err(E::D(
                    QueryDiagnostic::typecheck(
                        T70,
                        format!(
                            "`{name}` invents the value `{}` (a computed column under `{}`) without a round budget",
                            &n[1..],
                            a.func
                        ),
                    )
                    .with_fix(format!("declare `@budget(rounds = n)` on `{name}`")),
                ));
            }
        }
    }
    Ok(lattice)
}

fn t70(name: &str, construct: &str) -> E {
    err(
        T70,
        format!("`{name}` calls itself, but its cycle is not monotone: {construct}"),
    )
}

fn depends(t: &Term, name: &str) -> bool {
    t.any_node(|node| matches!(node, Term::RecRef { name: n, .. } if n == name))
}

fn reads_any(e: &IRExpr, cols: &HashSet<String>) -> bool {
    let mut v = Vec::new();
    e.columns(&mut v);
    v.iter().any(|c| cols.contains(c))
}

struct Walk<'a> {
    name: &'a str,
    lattice: Vec<String>,
    /// Block columns aggregating the recursive rows: only an `exists` reading
    /// (`> 0`, `>= 1`) is monotone.
    blocks: Vec<String>,
}

impl Walk<'_> {
    /// Walk a term inside the cycle; return the columns it computes (not
    /// reads), refusing a non-monotone construct over the recursive rows.
    /// Lattice-derived columns are tracked by name through `Project`.
    fn term(&mut self, t: &Term) -> R<HashSet<String>> {
        crate::stack::deep(|| {
            Ok(match t {
                Term::RecRef { .. } => HashSet::new(),
                Term::Unit | Term::Env(_) | Term::Scan { .. } => HashSet::new(),
                Term::Expand { input, .. } => self.term(input)?,
                Term::Filter { input, pred, .. } => {
                    let c = self.term(input)?;
                    if depends(input, self.name) {
                        let mut cols = Vec::new();
                        pred.columns(&mut cols);
                        if let Some(l) = cols.iter().find(|c| self.lattice.contains(c)) {
                            return Err(t70(
                                self.name,
                                &format!(
                                    "a filter reads the `min`/`max` column `{l}` inside the cycle"
                                ),
                            ));
                        }
                        if let Some(b) = cols.iter().find(|c| self.blocks.contains(c)) {
                            if !is_exists_of(pred, b) {
                                return Err(t70(
                                    self.name,
                                    "a `not` or a `count` over the recursive rows; only `exists { }` over them is monotone",
                                ));
                            }
                        }
                    }
                    c
                }
                Term::Project { input, items, .. } => {
                    let mut c = self.term(input)?;
                    for (_, e) in items {
                        let mut cols = Vec::new();
                        e.columns(&mut cols);
                        if cols.iter().any(|x| self.blocks.contains(x)) {
                            return Err(t70(
                                self.name,
                                "a block value over the recursive rows; only `exists { }` as a filter is monotone",
                            ));
                        }
                    }
                    let derived: Vec<String> = items
                        .iter()
                        .filter(|(_, e)| {
                            let mut cols = Vec::new();
                            e.columns(&mut cols);
                            cols.iter().any(|x| self.lattice.contains(x))
                        })
                        .map(|(n, _)| n.trim_start_matches('^').to_string())
                        .collect();
                    for (n, e) in items {
                        let n = n.trim_start_matches('^').to_string();
                        let computed = !(e.is_column_read() || matches!(e, IRExpr::Param(..)))
                            || reads_any(e, &c);
                        if computed {
                            c.insert(n.clone());
                            c.insert(format!("^{n}"));
                        } else {
                            c.remove(&n);
                        }
                    }
                    for d in derived {
                        if !self.lattice.contains(&d) {
                            self.lattice.push(d);
                        }
                    }
                    c
                }
                Term::Group { input, keys, aggs } => {
                    let dep = depends(input, self.name);
                    let c = self.term(input)?;
                    if dep {
                        // A block over the recursive rows: only `exists`, a
                        // count compared `> 0` or `>= 1`, is monotone; the walk
                        // over the enclosing filter decides (see `Correlate`).
                        // `distinct` (a Group with no aggregate) is a projection
                        // over sets and monotone.
                        let block = keys.is_empty()
                            && aggs.len() == 1
                            && aggs[0].1.func == AggFunc::Count
                            && aggs[0].1.arg.is_none();
                        if !aggs.is_empty() && !block {
                            return Err(t70(
                                self.name,
                                "an aggregate over the recursive rows below its own return",
                            ));
                        }
                        for (_, k) in keys {
                            let mut cols = Vec::new();
                            k.columns(&mut cols);
                            if cols.iter().any(|x| self.lattice.contains(x)) {
                                return Err(t70(
                                    self.name,
                                    "a group key reads a `min`/`max` column inside the cycle",
                                ));
                            }
                        }
                    }
                    c
                }
                Term::Correlate { outer, inner, .. } => {
                    let mut c = self.term(outer)?;
                    if let Term::Group { input, keys, aggs } = inner.as_ref()
                        && depends(input, self.name)
                        && keys.is_empty()
                        && aggs.len() == 1
                    {
                        // Recorded; the filter above decides whether it is an
                        // `exists` (admitted) or a count or `not` (refused).
                        self.blocks.push(aggs[0].0.clone());
                    }
                    c.extend(self.term(inner)?);
                    c
                }
                Term::Union { left, right } => {
                    let mut c = self.term(left)?;
                    let r = self.term(right)?;
                    // The arms match by name: a column the right arm computes is
                    // the union's column of that name.
                    let ln = output_columns(left);
                    for n in output_columns(right) {
                        if r.contains(&n) && ln.contains(&n) {
                            c.insert(n);
                        }
                    }
                    c
                }
                Term::Order { input, keys } => {
                    if depends(input, self.name) {
                        return Err(t70(self.name, "an `order` over the recursive rows"));
                    }
                    let _ = keys;
                    self.term(input)?
                }
                Term::Cut { input, .. } => {
                    if depends(input, self.name) {
                        return Err(t70(self.name, "a `limit` over the recursive rows"));
                    }
                    self.term(input)?
                }
                Term::ApproxTopK { input, .. } => {
                    if depends(input, self.name) {
                        return Err(t70(self.name, "a `nearest` window over the recursive rows"));
                    }
                    self.term(input)?
                }
                Term::Fuse { arms, .. } => {
                    if arms.iter().any(|a| depends(a, self.name)) {
                        return Err(t70(self.name, "an `rrf` over the recursive rows"));
                    }
                    HashSet::new()
                }
                Term::Score { input, .. } => self.term(input)?,
                Term::Fix { body, .. } | Term::Call { body, .. } => self.term(body)?,
            })
        })
    }
}

/// `col > 0` or `col >= 1`: the `exists` reading of a block count.
fn is_exists_of(pred: &IRExpr, col: &str) -> bool {
    let Some((left, op, right)) = pred.comparison_parts() else {
        return false;
    };
    matches!(left, IRExpr::AliasRef(c, _) if c == col)
        && matches!(
            (op, right),
            (CompOp::Gt, IRExpr::Literal(Literal::Integer(0), _))
                | (CompOp::Ge, IRExpr::Literal(Literal::Integer(1), _))
        )
}

// ---------------------------------------------------------------------------
// Helpers.

impl SExpr {
    fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

fn class_word(c: WriteClass) -> &'static str {
    match c {
        WriteClass::Constructive => "constructive",
        WriteClass::Destructive => "destructive",
    }
}

/// Edition 2's `from:` or `to:` in an edge write: an edge's endpoints are
/// assigned as `@src` and `@dst`, the names a read and a `where` use, so
/// `from` has one meaning in edition 3. A node type may declare a property
/// named `from`, so only the type checker, which knows the target is an
/// edge, can refuse the spelling.
fn endpoint_respelling(end: &str, at: &SourceSpan) -> E {
    let fixed = if end == "from" { "@src" } else { "@dst" };
    let mut d = QueryDiagnostic::typecheck(
        T11,
        format!("an edge's endpoints are assigned as `@src` and `@dst`; `{end}:` is edition 2's spelling of `{fixed}:`"),
    )
    .with_fix(format!("{fixed}: <endpoint>"));
    if at.end > at.start {
        d.position = Some(at.position);
        d.end = Some(at.end_position);
        d = d.with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits: vec![TextEdit {
                start: at.start,
                end: at.end,
                replacement: fixed.to_string(),
            }],
        });
    }
    E::D(d)
}

fn dup_assignments(a: &[Assignment]) -> R<()> {
    let mut seen = HashSet::new();
    for x in a {
        if !seen.insert(&x.property) {
            return Err(err(
                T13,
                format!("duplicate assignment for property `{}`", x.property),
            ));
        }
    }
    Ok(())
}

fn is_catalogue(name: &str) -> bool {
    name.starts_with('@')
}

fn is_integer(s: ScalarType) -> bool {
    matches!(
        s,
        ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64
    )
}

fn int_lit(n: i128) -> SExpr {
    SExpr::new(
        SKind::Lit(Literal::Integer(n)),
        Ty::scalar(ScalarType::I64, false),
    )
}

fn count_rows() -> Agg {
    Agg {
        func: AggFunc::Count,
        distinct: false,
        arg: None,
        fraction: None,
        ty: Ty::scalar(ScalarType::I64, false),
    }
}

fn exists_col(col: &str) -> SExpr {
    SExpr::new(
        SKind::Cmp {
            op: CompOp::Gt,
            left: SExpr::new(
                SKind::Col(col.to_string()),
                Ty::scalar(ScalarType::I64, false),
            )
            .boxed(),
            right: int_lit(0).boxed(),
        },
        Ty::scalar(ScalarType::Bool, false),
    )
}

fn not_exists(col: &str) -> SExpr {
    SExpr::new(
        SKind::Not(exists_col(col).boxed()),
        Ty::scalar(ScalarType::Bool, false),
    )
}

fn agg_func(name: &str) -> AggFunc {
    match name {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "collect" => AggFunc::Collect,
        "stddev" => AggFunc::Stddev,
        "percentile" => AggFunc::Percentile,
        _ => AggFunc::Max,
    }
}

/// An aggregate's result type: `count` is a non-null `I64`; `sum`, `avg`,
/// `stddev` and `percentile` are `F64` over a numeric argument; `min` and `max` keep the argument's type; all but
/// `count` and `collect` are nullable over an empty input. `collect` is a
/// list of its argument's type, the empty list over empty input.
fn agg_type(func: AggFunc, arg: &Ty) -> R<Ty> {
    match (func, arg) {
        // An aggregate's value is nullable in its position, `count`'s too
        // (the result contract the plan validates).
        (AggFunc::Count, _) => Ok(Ty::scalar(ScalarType::I64, true)),
        (_, Ty::Value(t)) if t.scalar == ScalarType::Blob => Err(err(
            T24,
            format!("a Blob is not a read value, so {func} cannot take it"),
        )),
        // A list of references waits for stage Path, which admits reference
        // elements in a `List`; a property of the reference is the lowered
        // form.
        (AggFunc::Collect, Ty::Node { .. } | Ty::Edge { .. }) => Err(err(
            T86,
            "`collect` of a reference: this release does not support a list of references yet; collect one of its properties",
        )),
        (AggFunc::Collect, Ty::Value(t)) if t.list || !t.scalar.is_list_element() => Err(err(
            T8,
            format!(
                "collect gathers values a list can hold, got {}",
                t.display_name()
            ),
        )),
        (AggFunc::Collect, Ty::Value(t)) => Ok(Ty::Value(PropType {
            scalar: t.scalar,
            nullable: true,
            list: true,
            enum_values: None,
        })),
        (_, Ty::Node { .. } | Ty::Edge { .. }) => Err(err(
            T8,
            format!("{func} cannot take a reference bare; access one of its properties"),
        )),
        (AggFunc::Sum | AggFunc::Avg | AggFunc::Stddev | AggFunc::Percentile, Ty::Value(t))
            if t.list || !t.scalar.is_numeric() =>
        {
            Err(err(
                T8,
                format!("{func} requires numeric type, got {}", t.display_name()),
            ))
        }
        (AggFunc::Sum | AggFunc::Avg | AggFunc::Stddev | AggFunc::Percentile, Ty::Value(_)) => {
            Ok(Ty::scalar(ScalarType::F64, true))
        }
        (_, Ty::Value(t)) if t.list || !t.scalar.is_orderable() => Err(err(
            T8,
            format!(
                "{func} requires an orderable scalar, got {}",
                t.display_name()
            ),
        )),
        (_, Ty::Value(t)) => Ok(Ty::scalar(t.scalar, true)),
    }
}

fn require_bool(e: &SExpr, what: &str) -> R<()> {
    match &e.ty {
        Ty::Value(t) if t.scalar == ScalarType::Bool && !t.list => Ok(()),
        other => Err(err(
            T41,
            format!("{what} needs a Bool, got {}", other.display()),
        )),
    }
}

/// An integer literal takes the integer type of the operand it meets when it
/// fits, and `F64` beside a float; a float literal stays `F64`. A negated
/// integer literal is one too: `-3` beside an `I32` is the negation of an
/// `I32` literal, where both the literal and its negation fit.
fn adapt(e: SExpr, other: &Ty) -> SExpr {
    if let (SKind::Neg(inner), Ty::Value(t)) = (&e.kind, other)
        && let SKind::Lit(Literal::Integer(n)) = inner.kind
        && !t.list
    {
        let scalar = match t.scalar {
            ScalarType::I32 | ScalarType::I64 if fits(n, t.scalar) && fits(-n, t.scalar) => {
                t.scalar
            }
            ScalarType::F32 | ScalarType::F64 => ScalarType::F64,
            _ => return e,
        };
        let literal = SExpr::new(inner.kind.clone(), Ty::scalar(scalar, false));
        return SExpr::new(SKind::Neg(literal.boxed()), Ty::scalar(scalar, false));
    }
    match (&e.kind, other) {
        (SKind::Lit(Literal::Integer(n)), Ty::Value(t))
            if !t.list && is_integer(t.scalar) && fits(*n, t.scalar) =>
        {
            SExpr::new(e.kind.clone(), Ty::scalar(t.scalar, false))
        }
        (SKind::Lit(Literal::Integer(_)), Ty::Value(t))
            if !t.list && matches!(t.scalar, ScalarType::F32 | ScalarType::F64) =>
        {
            SExpr::new(e.kind.clone(), Ty::scalar(ScalarType::F64, false))
        }
        // A list literal meeting a list: an empty list takes its type, and
        // integer elements take an integer element type (contextual, as today).
        (SKind::Lit(Literal::List(items)), Ty::Value(t)) if t.list => {
            let all_int = items
                .iter()
                .all(|i| matches!(i, Literal::Integer(_) | Literal::Null));
            if items.is_empty() || (all_int && t.scalar.is_numeric()) {
                SExpr::new(
                    e.kind.clone(),
                    Ty::Value(PropType::list_of(t.scalar, false)),
                )
            } else {
                e
            }
        }
        _ => e,
    }
}

/// The type [`adapt`] gives `e` against `other`, read without copying `e`:
/// only a literal, or a negated one, takes its context's type, and copying
/// any other operand would copy a nested branch at each level.
fn adapted_type(e: &SExpr, other: &Ty) -> Ty {
    let literal = match &e.kind {
        SKind::Lit(_) => true,
        SKind::Neg(inner) => matches!(inner.kind, SKind::Lit(_)),
        _ => false,
    };
    if literal {
        adapt(e.clone(), other).ty.clone()
    } else {
        e.ty.clone()
    }
}

fn fits(n: i128, s: ScalarType) -> bool {
    match s {
        ScalarType::I32 => i32::try_from(n).is_ok(),
        ScalarType::I64 => i64::try_from(n).is_ok(),
        ScalarType::U32 => u32::try_from(n).is_ok(),
        ScalarType::U64 => u64::try_from(n).is_ok(),
        _ => false,
    }
}

fn width(s: ScalarType) -> u8 {
    match s {
        ScalarType::I32 | ScalarType::U32 | ScalarType::F32 => 32,
        _ => 64,
    }
}

/// Whether a numeric value of type `from` may be assigned to a property of
/// type `to` without a narrowing the RFC never states.
fn numeric_fits(from: ScalarType, to: ScalarType) -> bool {
    if !to.is_numeric() {
        return true;
    }
    let float = |s| matches!(s, ScalarType::F32 | ScalarType::F64);
    match (float(from), float(to)) {
        (false, false) => width(from) <= width(to),
        (false, true) => true,
        (true, false) => false,
        (true, true) => width(from) <= width(to),
    }
}

fn numeric(e: &SExpr, what: &str) -> R<Ty> {
    match &e.ty {
        Ty::Value(t) if t.scalar.is_numeric() && !t.list => Ok(e.ty.clone()),
        other => Err(err(
            T72,
            format!("{what} needs a number, got {}", other.display()),
        )),
    }
}

/// `+ - * / %`: one width keeps it, a narrower integer widens to the wider,
/// an integer with a float is `F64`, `F32` beside `F64` is `F64`; the result
/// is nullable when either operand is. Each operand of another type is
/// widened by an explicit `Cast` that keeps its nullability.
fn arith(op: ArithOp, l: SExpr, r: SExpr) -> R<SExpr> {
    let l = adapt(l, &r.ty);
    let r = adapt(r, &l.ty);
    let lt = numeric(&l, &format!("`{}`'s left operand", op.symbol()))?;
    let rt = numeric(&r, &format!("`{}`'s right operand", op.symbol()))?;
    let (Ty::Value(a), Ty::Value(b)) = (&lt, &rt) else {
        unreachable!()
    };
    let Some(scalar) = numeric_join(a.scalar, b.scalar) else {
        return Err(err(
            T72,
            format!(
                "`{}` mixes U64 with a signed integer, which have no common type",
                op.symbol()
            ),
        ));
    };
    let ty = Ty::scalar(scalar, a.nullable || b.nullable);
    Ok(SExpr::new(
        SKind::Arith {
            op,
            left: widen(l, scalar).boxed(),
            right: widen(r, scalar).boxed(),
        },
        ty,
    ))
}

/// The common type of two numeric scalars: one type keeps it, an integer
/// with a float is `F64` (as is `F32` beside `F64`), a narrower integer of
/// one signedness widens to the wider, `U32` with a signed integer is `I64`,
/// and `U64` with a signed integer has none. This is the join of a value
/// that becomes a column (arithmetic, `if`, `coalesce`); a comparison of
/// `U64` with a signed integer meets in the internal exact carrier instead
/// (`ir::coerce::common_numeric`), which never appears in a result.
fn numeric_join(a: ScalarType, b: ScalarType) -> Option<ScalarType> {
    let float = |s| matches!(s, ScalarType::F32 | ScalarType::F64);
    let signed = |s| matches!(s, ScalarType::I32 | ScalarType::I64);
    Some(if a == b {
        a
    } else if float(a) || float(b) {
        ScalarType::F64
    } else if signed(a) == signed(b) {
        if width(a) >= width(b) { a } else { b }
    } else if matches!(a, ScalarType::U64) || matches!(b, ScalarType::U64) {
        return None;
    } else {
        ScalarType::I64
    })
}

/// A scalar numeric operand of another type, widened to `scalar` by an
/// explicit `Cast` that keeps its nullability.
fn widen(e: SExpr, scalar: ScalarType) -> SExpr {
    match &e.ty {
        Ty::Value(t) if !t.list && t.scalar.is_numeric() && t.scalar != scalar => {
            let nullable = t.nullable;
            SExpr::new(SKind::Cast(e.boxed()), Ty::scalar(scalar, nullable))
        }
        _ => e,
    }
}

/// A comparison: numeric operands of different types meet through an
/// explicit `Cast` to the wider; references compare by identity with `=`
/// and `!=` only.
fn compare(op: CompOp, l: SExpr, r: SExpr) -> R<SExpr> {
    let l = adapt(l, &r.ty);
    let r = adapt(r, &l.ty);
    let nullable = l.ty.nullable() || r.ty.nullable();
    let ordering = matches!(op, CompOp::Lt | CompOp::Le | CompOp::Gt | CompOp::Ge);
    let (l, r) = match (&l.ty, &r.ty) {
        (Ty::Node { type_name: a, .. }, Ty::Node { type_name: b, .. })
        | (Ty::Edge { type_name: a, .. }, Ty::Edge { type_name: b, .. }) => {
            if ordering || !matches!(op, CompOp::Eq | CompOp::Ne) {
                return Err(err(T7, "references compare with `=` and `!=` only"));
            }
            if a != b && soundness::refuses(soundness::Claim::ReferencesOfTwoTypes) {
                return Err(err(
                    T7,
                    format!("`{a}` and `{b}` references are never equal"),
                ));
            }
            (l, r)
        }
        (Ty::Value(a), Ty::Value(b)) => {
            if matches!(op, CompOp::Contains | CompOp::StartsWith) {
                return Err(err(T7, "infix string operators are calls"));
            }
            if a.list && b.list && types_compatible(a, b) {
                return Err(err_fix(
                    T7,
                    format!(
                        "list equality is not supported: cannot compare {} with {}",
                        a.display_name(),
                        b.display_name()
                    ),
                    "test membership of one value: `<value> in <list>`",
                ));
            }
            if !types_compatible(a, b) || a.list || b.list {
                return Err(err(
                    T7,
                    format!(
                        "cannot compare {} with {}",
                        a.display_name(),
                        b.display_name()
                    ),
                ));
            }
            if matches!(a.scalar, ScalarType::Vector(_) | ScalarType::Blob) {
                return Err(err(
                    T7,
                    format!("{} values do not compare", a.display_name()),
                ));
            }
            if ordering && !a.scalar.is_orderable() {
                return Err(err(
                    T7,
                    format!("{} values have no order", a.display_name()),
                ));
            }
            if a.scalar != b.scalar {
                let wide = if width(a.scalar) >= width(b.scalar)
                    && !(matches!(b.scalar, ScalarType::F32 | ScalarType::F64)
                        && !matches!(a.scalar, ScalarType::F32 | ScalarType::F64))
                {
                    a.scalar
                } else {
                    b.scalar
                };
                let cast = |e: SExpr| {
                    if matches!(&e.ty, Ty::Value(t) if t.scalar != wide) {
                        let n = e.ty.nullable();
                        SExpr::new(SKind::Cast(e.boxed()), Ty::scalar(wide, n))
                    } else {
                        e
                    }
                };
                (cast(l), cast(r))
            } else {
                (l, r)
            }
        }
        (a, b) => {
            return Err(err(
                T7,
                format!("cannot compare {} with {}", a.display(), b.display()),
            ));
        }
    };
    Ok(SExpr::new(
        SKind::Cmp {
            op,
            left: l.boxed(),
            right: r.boxed(),
        },
        Ty::scalar(ScalarType::Bool, nullable),
    ))
}

/// A literal compared with a property is checked against the property's
/// type wherever it is written (a read's `where`, a block's or an
/// `optional`'s `where`, a write group's `where`, a `let`), since every one
/// types its comparisons here: `lit` set against `prop`, its byte range the
/// `index`th of the comparison's operands. A parameter keeps its declared
/// type, and an operand that is not a property read is left as it is.
fn literal_against_property(
    prop: &SExpr,
    lit: &SExpr,
    written: &SourceSpan,
    index: usize,
) -> R<()> {
    if !soundness::refuses(soundness::Claim::LiteralOutsideDomain) {
        return Ok(());
    }
    let (SKind::Prop { prop: name, .. }, Ty::Value(expected)) = (&prop.kind, &prop.ty) else {
        return Ok(());
    };
    let Some(value) = written_literal(lit) else {
        return Ok(());
    };
    match literal::compared_literal(&value, expected, name) {
        Some(mismatch) => Err(literal_refusal(
            mismatch,
            written.ranges.get(index),
            Some(written.position),
        )),
        None => Ok(()),
    }
}

/// The same check for `in`: a literal needle against a list property's
/// element type, and each element of a written list against a property
/// needle.
fn literal_in_against_property(needle: &SExpr, list: &SExpr, written: &SourceSpan) -> R<()> {
    if !soundness::refuses(soundness::Claim::LiteralOutsideDomain) {
        return Ok(());
    }
    if let Ty::Value(t) = &list.ty
        && t.list
        && matches!(list.kind, SKind::Prop { .. })
    {
        let mut element_type = t.clone();
        element_type.list = false;
        let element = SExpr::new(list.kind.clone(), Ty::Value(element_type));
        return literal_against_property(&element, needle, written, 0);
    }
    let (SKind::Prop { prop: name, .. }, Ty::Value(expected)) = (&needle.kind, &needle.ty) else {
        return Ok(());
    };
    // A written list's literal elements, by position: a list holding a
    // parameter checks its literals as a literal list does.
    let items: Vec<Option<&Literal>> = match &list.kind {
        SKind::Lit(Literal::List(items)) => items.iter().map(Some).collect(),
        SKind::Call { func, args, .. } if func == "list" => args
            .iter()
            .map(|a| match &a.kind {
                SKind::Lit(l) => Some(l),
                _ => None,
            })
            .collect(),
        _ => return Ok(()),
    };
    for (i, item) in items.into_iter().enumerate() {
        let Some(item) = item else { continue };
        if let Some(mismatch) = literal::compared_literal(item, expected, name) {
            return Err(literal_refusal(
                mismatch,
                written.ranges.get(2 + i),
                Some(written.position),
            ));
        }
    }
    Ok(())
}

/// The literal an operand was written as, a negated number included.
fn written_literal(e: &SExpr) -> Option<Literal> {
    match &e.kind {
        SKind::Lit(l) => Some(l.clone()),
        SKind::Neg(inner) => match &inner.kind {
            SKind::Lit(Literal::Integer(n)) => Some(Literal::Integer(-n)),
            SKind::Lit(Literal::Float(f)) => Some(Literal::Float(-f)),
            _ => None,
        },
        _ => None,
    }
}

/// `T3` for a literal outside its property's domain, the nearest allowed
/// value printed as the fix and, where the literal's range was recorded,
/// as the edit that writes it in place.
fn literal_refusal(
    mismatch: literal::LiteralMismatch,
    range: Option<&(usize, usize)>,
    position: Option<Position>,
) -> E {
    let mut d = QueryDiagnostic::typecheck(T3, mismatch.message).with_fix(mismatch.fix);
    if let (Some(replacement), Some(&(start, end))) = (mismatch.replacement, range)
        && start < end
    {
        d = d.with_suggestion(Suggestion {
            applicability: Applicability::NeedsReview,
            edits: vec![TextEdit {
                start,
                end,
                replacement,
            }],
        });
    }
    // A parsed comparison records its operands' ranges; a built one has no
    // position of its own, and the clause's is filled in instead.
    if range.is_some() {
        d.position = position;
    }
    E::D(d)
}

/// A branch of `if` or an argument of `coalesce` in the call's common type:
/// a literal takes it, and another numeric value is widened to it.
/// A registry row's call node: its arguments and result type; `can_fail`
/// as the row declares it ([`crate::ir::expr::ScalarFunc::row_can_fail`])
/// or as an argument does.
fn call(name: &str, args: Vec<SExpr>, ty: Ty) -> SExpr {
    SExpr::new(
        SKind::Call {
            func: name.into(),
            args,
            options: Vec::new(),
        },
        ty,
    )
}

/// The one String argument of a string row.
fn one_string(name: &str, args: Vec<SExpr>) -> R<[SExpr; 1]> {
    let Ok([x]) = <[SExpr; 1]>::try_from(args) else {
        return Err(err(T7, format!("`{name}` takes one String")));
    };
    if !matches!(&x.ty, Ty::Value(t) if t.scalar == ScalarType::String && !t.list) {
        return Err(err(
            T7,
            format!("`{name}` takes a String, got {}", x.ty.display()),
        ));
    }
    Ok([x])
}

/// An integer argument of a row, an `I64` by an explicit `Cast` where it is
/// a narrower integer; a float or a `U64` is refused, since no `Cast`
/// reaches `I64` without a value it cannot hold.
fn integer_arg(name: &str, what: &str, e: SExpr) -> R<SExpr> {
    let e = adapt(e, &Ty::scalar(ScalarType::I64, false));
    match &e.ty {
        Ty::Value(t)
            if !t.list
                && matches!(
                    t.scalar,
                    ScalarType::I32 | ScalarType::I64 | ScalarType::U32
                ) =>
        {
            Ok(widen(e, ScalarType::I64))
        }
        other => Err(err(
            T7,
            format!(
                "`{name}`'s {what} is an I64, I32 or U32, got {}",
                other.display()
            ),
        )),
    }
}

fn fit(e: SExpr, ty: &Ty) -> SExpr {
    let e = adapt(e, ty);
    match ty {
        Ty::Value(t) if !t.list && t.scalar.is_numeric() => widen(e, t.scalar),
        _ => e,
    }
}

/// The common type of two branches (`if`, `coalesce`).
fn is_null_literal(e: &Expr) -> bool {
    matches!(e, Expr::Literal(Literal::Null))
}

/// Whether `e` is null on every row whatever type it is given, so only its
/// context can fix its type: the `null` literal, an `if` whose branches
/// and a `coalesce` whose arguments are all such.
fn is_untyped_null(e: &Expr) -> bool {
    crate::stack::deep(|| {
        let all = |args: &[Arg]| {
            args.iter()
                .all(|a| matches!(a, Arg::Expr(x) if is_untyped_null(x)))
        };
        match e {
            Expr::Literal(Literal::Null) => true,
            Expr::Call(c) if c.options.is_empty() && c.name == "if" && c.args.len() == 3 => {
                all(&c.args[1..])
            }
            Expr::Call(c) if c.options.is_empty() && c.name == "coalesce" && !c.args.is_empty() => {
                all(&c.args)
            }
            _ => false,
        }
    })
}

/// Whether `e` is a list literal with no non-null element, `[]`, whose
/// element type only its context can fix, as a list literal's elements fix
/// theirs: the property it is assigned to, the other operand of a
/// comparison, the needle of `in`, the other branch of `if` or another
/// argument of `coalesce`.
fn is_untyped_list(e: &Expr) -> bool {
    match e {
        Expr::List { elements, .. } => elements
            .iter()
            .all(|x| matches!(x, Expr::Literal(Literal::Null))),
        Expr::Literal(Literal::List(items)) => items.iter().all(|x| matches!(x, Literal::Null)),
        _ => false,
    }
}

/// The literal an [`is_untyped_list`] expression writes.
fn untyped_list_literal(e: &Expr) -> Literal {
    match e {
        Expr::List { elements, .. } => Literal::List(vec![Literal::Null; elements.len()]),
        Expr::Literal(l) => l.clone(),
        _ => unreachable!("`is_untyped_list` admits list literals only"),
    }
}

/// `e`, an [`is_untyped_list`] literal, as a value of the list type `want`
/// its context fixes; a context that is not a list refuses it.
fn untyped_list_as(e: &Expr, want: &Ty) -> R<SExpr> {
    match want {
        Ty::Value(t) if t.list => Ok(SExpr::new(
            SKind::Lit(untyped_list_literal(e)),
            Ty::Value(PropType::list_of(t.scalar, false)),
        )),
        other => Err(err(
            T7,
            format!(
                "an empty list is not a value of {}, the type its context reads",
                other.display()
            ),
        )),
    }
}

/// An empty list whose element type nothing fixes (the GQ 3 RFC, section 5,
/// and section 7's list rule).
fn untyped_list_refusal() -> E {
    err_fix(
        T7,
        "nothing fixes the element type of this empty list: `[]` is a value where the property it is assigned to, the other operand of a comparison or `in`, the other branch of `if` or another argument of `coalesce` gives it a type",
        "a list parameter of the intended type, `$xs: [<Type>]`, bound to `[]`",
    )
}

/// A `null` whose type nothing fixes (the GQ 3 RFC, section 5).
fn untyped_null_refusal() -> E {
    err_fix(
        T7,
        "nothing fixes the type of this `null`: a `null` is a value where the other branch of `if`, another argument of `coalesce` or the property it is assigned to gives it a type",
        "if(<condition>, <value>, null), or coalesce(<value>, …)",
    )
}

/// `T7` for a list compared with `=` or `!=` to a value of its element type,
/// which edition 2's property brace on a list meant as membership
/// (`$p: Person { tags: "rust" }`, whose brace fix writes `$p.tags =
/// "rust"`): the fix is the membership test, `"rust" in $p.tags`, negated
/// under `!=`.
fn list_equality_membership(
    op: CompOp,
    l: &SExpr,
    r: &SExpr,
    left: &Expr,
    right: &Expr,
    written: &SourceSpan,
) -> Option<QueryDiagnostic> {
    if !matches!(op, CompOp::Eq | CompOp::Ne) {
        return None;
    }
    let (Ty::Value(a), Ty::Value(b)) = (&l.ty, &r.ty) else {
        return None;
    };
    let (list, item, list_ty, item_ty) = match (a.list, b.list) {
        (true, false) => (left, right, a, b),
        (false, true) => (right, left, b, a),
        _ => return None,
    };
    let mut element = list_ty.clone();
    element.list = false;
    if !types_compatible(item_ty, &element) && !types_compatible(&element, item_ty) {
        return None;
    }
    let print = crate::query::syntax::print::expr;
    let membership = format!("{} in {}", print(item), print(list));
    let replacement = match op {
        CompOp::Ne => format!("not ({membership})"),
        _ => membership,
    };
    let mut d = QueryDiagnostic::typecheck(
        T7,
        format!(
            "cannot compare {} with {}: a list holds a value, which `in` tests",
            a.display_name(),
            b.display_name()
        ),
    )
    .with_fix(replacement.clone());
    if written.end > written.start {
        d = d.with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits: vec![TextEdit {
                start: written.start,
                end: written.end,
                replacement,
            }],
        });
        d.position = Some(written.position);
        d.end = Some(written.end_position);
    }
    Some(d)
}

/// A comparison with the `null` literal, null on every row: the test is
/// `is null`, or `is not null` for `!=`, which the fix writes.
fn null_comparison(op: CompOp, left: &Expr, right: &Expr, written: &SourceSpan) -> E {
    let other = if is_null_literal(left) { right } else { left };
    let test = match op {
        CompOp::Ne => "is not null",
        _ => "is null",
    };
    let replacement = format!("{} {test}", crate::query::syntax::print::expr(other));
    let mut d = QueryDiagnostic::typecheck(
        T7,
        format!("a comparison with `null` is null on every row; a null test is `{test}`"),
    )
    .with_fix(replacement.clone());
    if matches!(op, CompOp::Eq | CompOp::Ne) && written.end > written.start {
        d = d.with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits: vec![TextEdit {
                start: written.start,
                end: written.end,
                replacement,
            }],
        });
    }
    if written.end > written.start {
        d.position = Some(written.position);
        d.end = Some(written.end_position);
    }
    E::D(d)
}

fn common(a: &SExpr, b: &SExpr, what: &str) -> R<Ty> {
    let a_ty = adapted_type(a, &b.ty);
    let b_ty = adapted_type(b, &a_ty);
    let nullable = a_ty.nullable() || b_ty.nullable();
    match (&a_ty, &b_ty) {
        (Ty::Value(x), Ty::Value(y)) if x.scalar == y.scalar && x.list == y.list => {
            Ok(a_ty.with_nullable(nullable))
        }
        (Ty::Value(x), Ty::Value(y))
            if x.scalar.is_numeric() && y.scalar.is_numeric() && !x.list && !y.list =>
        {
            let s = numeric_join(x.scalar, y.scalar).ok_or_else(|| {
                err(
                    T7,
                    format!(
                        "`{what}`'s branches are {} and {}, which have no common type",
                        x.display_name(),
                        y.display_name()
                    ),
                )
            })?;
            Ok(Ty::scalar(s, nullable))
        }
        (x, y) if x.same_base(y) => Ok(x.with_nullable(nullable)),
        (x, y) => Err(err(
            T7,
            format!(
                "`{what}`'s branches are {} and {}",
                x.display(),
                y.display()
            ),
        )),
    }
}

fn lexical_text(text: &SExpr) -> R<()> {
    match (&text.kind, &text.ty) {
        (SKind::Lit(Literal::String(s)), _)
            if s.split(|c: char| !c.is_alphanumeric()).all(str::is_empty) =>
        {
            Err(err(T20, "the lexical query has no searchable terms"))
        }
        (_, Ty::Value(t)) if t.scalar == ScalarType::String && !t.list => Ok(()),
        (_, other) => Err(err(
            T20,
            format!(
                "a lexical query is a String or `terms(…)`, got {}",
                other.display()
            ),
        )),
    }
}

fn contains_agg(e: &Expr) -> bool {
    crate::stack::deep(|| match e {
        Expr::Call(c) if AGGREGATES.contains(&c.name.as_str()) => true,
        Expr::AggDistinct { .. } => true,
        Expr::Call(c) => c
            .args
            .iter()
            .any(|a| matches!(a, Arg::Expr(x) if contains_agg(x))),
        Expr::Compare { left, right, .. }
        | Expr::Arith { left, right, .. }
        | Expr::InfixString { left, right, .. }
        | Expr::And(left, right)
        | Expr::Or(left, right)
        | Expr::In {
            needle: left,
            list: right,
            ..
        } => contains_agg(left) || contains_agg(right),
        Expr::Not(x) | Expr::Neg(x) => contains_agg(x),
        Expr::IsNull { expr, .. } => contains_agg(expr),
        _ => false,
    })
}

/// `T8` for an aggregate call in a `where`, with the block form as the fix:
/// the elements that bind the aggregate's argument move into the block, and
/// the call becomes the block's value (`match { $p knows $f } where
/// count($f) > 3` is `where count($f) { $p knows $f } > 3`).
fn aggregate_filter_fix(catalog: &Catalog, pattern: &Pattern, in_write: bool) -> Option<E> {
    use crate::query::syntax::print;
    let filter = pattern.predicate.as_ref()?;
    let call = first_aggregate_call(filter)?;
    let Some(Arg::Expr(arg)) = call.args.first() else {
        return None;
    };
    let mut vars = Vec::new();
    expr_vars(arg, &mut vars);
    let binds = |c: &Element| match c {
        Element::Binding { variable, .. } => vars.contains(variable),
        Element::Traversal(t) => {
            vars.contains(&t.dst)
                || vars.contains(&t.src)
                || t.edge_binding.as_ref().is_some_and(|e| vars.contains(e))
        }
        _ => false,
    };
    let moved: Vec<Element> = pattern
        .elements
        .iter()
        .filter(|c| binds(c))
        .cloned()
        .collect();
    let written = print::expr(&Expr::Call(call.clone()));
    // A block correlates on a name the remaining elements bind; with none
    // left, the count is a grouped value of the whole read.
    let mut kept_vars = Vec::new();
    for c in pattern.elements.iter().filter(|c| !binds(c)) {
        match c {
            Element::Binding { variable, .. } => kept_vars.push(variable.clone()),
            Element::Traversal(t) => kept_vars.extend([t.src.clone(), t.dst.clone()]),
            _ => {}
        }
    }
    let correlated = moved.iter().any(|c| match c {
        Element::Traversal(t) => kept_vars.contains(&t.src) || kept_vars.contains(&t.dst),
        _ => false,
    });
    if !correlated {
        // A write has no `return`; the general refusal stands.
        if in_write {
            return None;
        }
        return Some(err_fix(
            T8,
            format!("`{written}` is an aggregate over the read's own rows; it belongs in `return`"),
            format!("return {{ {written} as {} }}", alias_for(&written)),
        ));
    }
    // The block binds every name its triples introduce (`T84`).
    let mut outer: HashMap<String, BoundVariable> = HashMap::new();
    for c in pattern.elements.iter().filter(|c| !binds(c)) {
        if let Element::Binding {
            variable,
            type_name,
        } = c
        {
            outer.insert(
                variable.clone(),
                BoundVariable::Node {
                    type_name: type_name.clone(),
                },
            );
        }
    }
    let kept_elements: Vec<Element> = pattern
        .elements
        .iter()
        .filter(|c| !binds(c))
        .cloned()
        .collect();
    for (_, introduced) in end_bindings(catalog, &kept_elements, &HashMap::new()) {
        for (variable, type_name) in introduced {
            outer.insert(variable, BoundVariable::Node { type_name });
        }
    }
    let fresh = end_bindings(catalog, &moved, &outer);
    let moved: Vec<Element> = moved
        .into_iter()
        .enumerate()
        .flat_map(|(i, c)| {
            let binds = fresh.iter().find(|(j, _)| *j == i).map(|(_, b)| b.clone());
            binds
                .unwrap_or_default()
                .into_iter()
                .map(|(variable, type_name)| Element::Binding {
                    variable,
                    type_name,
                })
                .chain(std::iter::once(c))
        })
        .collect();
    let func = agg_func(&call.name);
    let block = Expr::Block(Box::new(Block {
        func: BlockFunc::Agg(func),
        arg: Some(Box::new(arg.clone())),
        fraction: match call.args.get(1) {
            Some(Arg::Expr(p)) => Some(Box::new(p.clone())),
            _ => None,
        },
        pattern: Pattern {
            elements: moved,
            ..Pattern::default()
        },
        at: SourceSpan::default(),
    }));
    let rewritten = replace_call(filter, call, &block);
    let kept: Vec<String> = pattern
        .elements
        .iter()
        .filter(|c| !binds(c))
        .map(print::element)
        .collect();
    Some(err_fix(
        T8,
        format!(
            "`{written}` is an aggregate; in a filter it is a block over the elements it counts"
        ),
        format!(
            "match {{ {} }} where {}",
            kept.join(", "),
            print::expr(&rewritten)
        ),
    ))
}

/// `T68` for a binding inside a block of a name the scope where the block is
/// written holds. A block correlates on an outer name by reading it, so the
/// fix drops the binding when its type is the outer name's; another type
/// meant a name of the block's own, and the fix is a fresh name.
fn block_rebinding(
    pattern: &Pattern,
    index: usize,
    optional: bool,
    var: &str,
    type_name: &str,
    outer: &Ty,
    written: Option<&Ty>,
) -> QueryDiagnostic {
    let (scope, noun) = if optional {
        ("an `optional`", "the `optional`")
    } else {
        ("a block", "the block")
    };
    let mut d = QueryDiagnostic::typecheck(
        T68,
        format!(
            "`${var}` is bound inside {scope}, but the scope where {noun} is written already holds `${var}` ({}); {scope} reads an outer name to correlate on it, and binds only a name new to it",
            outer.display()
        ),
    );
    let spans = &pattern.spans.elements;
    d.position = spans.get(index).map(|s| s.position);
    let same = written.is_some_and(|ty| ty.same_base(outer));
    if !same {
        return d.with_fix(format!(
            "bind another name inside {scope}: `$x: {type_name}`"
        ));
    }
    if optional && pattern.elements.len() == 1 {
        // An `optional` whose one element re-mentions an outer name keeps
        // every outer row once, matched or not, and introduces no name.
        return d.with_fix(format!(
            "read the outer `${var}` without binding it; an `optional` whose only element is `${var}: {type_name}` keeps every row and introduces no name"
        ));
    }
    d = d.with_fix(format!(
        "read the outer `${var}` without binding it: drop `${var}: {type_name}`"
    ));
    let range = match (index, spans.get(index)) {
        (0, Some(this)) => spans.get(1).map(|next| (this.start, next.start)),
        (i, Some(this)) => spans.get(i - 1).map(|prev| (prev.end, this.end)),
        _ => None,
    };
    match range {
        Some((start, end)) => d.with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits: vec![TextEdit {
                start,
                end,
                replacement: String::new(),
            }],
        }),
        None => d,
    }
}

/// The bindings an inner scope written from `elements` needs (`T84`): for
/// each triple, the ends it names first that no binding element of
/// `elements` binds and the enclosing scope (`outer`) does not hold, each
/// with the type the edge declares for that end, by the triple's resolution
/// against the bindings before it. A triple that does not resolve
/// contributes none, and the refusal it meets is the one its text earns.
fn end_bindings(
    catalog: &Catalog,
    elements: &[Element],
    outer: &HashMap<String, BoundVariable>,
) -> Vec<(usize, Vec<(String, String)>)> {
    let mut declared: HashMap<String, BoundVariable> = HashMap::new();
    for c in elements {
        if let Element::Binding {
            variable,
            type_name,
        } = c
            && catalog.query_node_type(type_name).is_some()
        {
            declared.insert(
                variable.clone(),
                BoundVariable::Node {
                    type_name: type_name.clone(),
                },
            );
        }
    }
    let mut known = declared.clone();
    known.extend(outer.iter().map(|(k, v)| (k.clone(), v.clone())));
    let mut out = Vec::new();
    for (i, c) in elements.iter().enumerate() {
        let Element::Traversal(t) = c else {
            continue;
        };
        let Ok(resolved) = resolve_traversal(catalog, t, &known, &declared) else {
            continue;
        };
        let mut here = Vec::new();
        for (end, ty) in [(&t.src, &resolved.src_type), (&t.dst, &resolved.dst_type)] {
            if end == "_" || known.contains_key(end) {
                continue;
            }
            known.insert(
                end.clone(),
                BoundVariable::Node {
                    type_name: ty.clone(),
                },
            );
            here.push((end.clone(), ty.clone()));
        }
        if !here.is_empty() {
            out.push((i, here));
        }
    }
    out
}

/// `T82` for a triple written against its edge's declared direction, or a
/// selection, `(a | b)` or `*`, none of whose members points along the
/// written order: every edge it selects resolved from the written ends to the
/// reverse of its declared `from -> to`. A selection matches each member in
/// the direction its end types fix (the written order where both ends have
/// one type), so one member along the written order admits it, whatever the
/// others; a wildcard's members are every edge between its two end types,
/// either way. The fix swaps the two ends, keeping the edge binding, the hop
/// bounds and the selector, which reads every member in its declared
/// orientation as the end types did. `<e>` reads either direction and is
/// never refused, and a selection with no member has no order to restore.
fn reversed_triple(
    catalog: &Catalog,
    written: &Traversal,
    resolved: &traversal::ResolvedTraversal,
    span: Option<&SourceSpan>,
) -> Option<QueryDiagnostic> {
    if written.undirected {
        return None;
    }
    let members = resolved.edges.members();
    if members.is_empty() || members.iter().any(|m| m.direction != Direction::In) {
        return None;
    }
    let declared = members
        .iter()
        .filter_map(|m| catalog.lookup_edge_by_name(&m.edge_type))
        .map(|e| format!("`{}: {} -> {}`", e.name, e.from_type, e.to_type))
        .collect::<Vec<_>>()
        .join(", ");
    let selector = match &written.selector {
        EdgeSelector::Named(name) => name.clone(),
        EdgeSelector::Alternation(names) => format!("({})", names.join(" | ")),
        EdgeSelector::Wildcard => "*".to_string(),
    };
    let edge = match &written.edge_binding {
        Some(k) => format!("${k}:{selector}"),
        None => selector,
    };
    let bounds = match (written.min_hops, written.max_hops) {
        (1, Some(1)) => String::new(),
        (min, Some(max)) => format!("{{{min},{max}}}"),
        (min, None) => format!("{{{min},}}"),
    };
    let triple = |a: &str, b: &str| format!("${a} {edge}{bounds} ${b}");
    let message = if matches!(written.selector, EdgeSelector::Named(_)) {
        format!(
            "`{}` is written against the declared direction of {declared}: a triple is written in its edge's declared order, source first, so its written order is its direction",
            triple(&written.src, &written.dst)
        )
    } else {
        format!(
            "no member of `{}` points along its written order, which is against the declared direction of {declared}: a selection matches each member in the direction its end types fix, and at least one member is written in its declared order, source first",
            triple(&written.src, &written.dst)
        )
    };
    let mut d = QueryDiagnostic::typecheck(T82, message).with_fix(format!(
        "write the source first: `{}`",
        triple(&written.dst, &written.src)
    ));
    if let Some(span) = span {
        d.position = Some(span.position);
        d.end = Some(span.end_position);
        if let [(ss, se), (ds, de)] = span.ranges.as_slice() {
            d = d.with_suggestion(Suggestion {
                applicability: Applicability::MachineApplicable,
                edits: vec![
                    TextEdit {
                        start: *ss,
                        end: *se,
                        replacement: format!("${}", written.dst),
                    },
                    TextEdit {
                        start: *ds,
                        end: *de,
                        replacement: format!("${}", written.src),
                    },
                ],
            });
        }
    }
    Some(d)
}

/// Whether `end` is a reference an `optional` introduced into this scope,
/// nullable outside it: a nullable node column of the scope's own. An inner
/// scope reads an enclosing scope's reference by correlating on it: a later
/// `optional` pads, and a block or an inline read is evaluated once per
/// outer row, so none of them is a triple of the scope the name entered.
fn optional_reference(sc: &Scope, end: &str) -> bool {
    end != "_"
        && sc
            .local(end)
            .is_some_and(|c| matches!(c.ty, Ty::Node { nullable: true, .. }))
}

/// What a pattern's `T90` refusals have already moved: one suggestion per
/// `optional` moves every triple bound for it, so two refusals never insert
/// at one place.
#[derive(Default)]
struct OptionalMoves {
    /// The `optional` elements a suggestion already fills.
    targets: HashSet<usize>,
    /// The triples a suggestion already moves.
    elements: HashSet<usize>,
}

/// `T90` for a triple, outside an `optional`, that reads a reference the
/// `optional` introduced (the GQ 3 RFC, section 3): the reference is null on
/// the rows where the `optional` matched nothing, and a triple yields no row
/// for a null end, so the outer join would silently become an inner one.
/// The fix moves the triple inside the `optional`, with a binding for each
/// name it introduces, and with it every later triple that reads one of those
/// names; where only the rows the `optional` matches were meant, the other
/// fix drops the `optional`.
fn optional_triple(
    catalog: &Catalog,
    sc: &Scope,
    pattern: &Pattern,
    i: usize,
    t: &Traversal,
    moves: &mut OptionalMoves,
) -> Option<QueryDiagnostic> {
    use crate::query::syntax::print;
    let mut ends: Vec<&String> = Vec::new();
    for end in [&t.src, &t.dst] {
        if optional_reference(sc, end) && !ends.contains(&end) {
            ends.push(end);
        }
    }
    if ends.is_empty() {
        return None;
    }
    let written = print::element(&pattern.elements[i]);
    let named = ends
        .iter()
        .map(|e| format!("`${e}`"))
        .collect::<Vec<_>>()
        .join(" and ");
    let (pronoun, verb) = if ends.len() == 1 {
        ("it", "is")
    } else {
        ("them", "are")
    };
    let mut d = QueryDiagnostic::typecheck(
        T90,
        format!(
            "`{written}` reads {named} outside the `optional` that introduces {pronoun}, where {named} {verb} null on the rows the `optional` matched nothing for: a triple yields no row for a null end, so it would silently turn that outer join into an inner one"
        ),
    )
    .with_fix(format!(
        "move `{written}` inside the `optional`, or drop the `optional` if only the rows it matches were meant"
    ));
    let spans = &pattern.spans.elements;
    if let Some(span) = spans.get(i) {
        d.position = Some(span.position);
        d.end = Some(span.end_position);
    }
    if moves.elements.contains(&i) {
        return Some(d);
    }
    if let Some(edits) = optional_move(catalog, sc, pattern, i, moves) {
        d = d.with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits,
        });
    }
    Some(d)
}

/// The edits that move the triple at `i`, and the later triples reading the
/// names it introduces, inside the `optional` of this pattern that
/// introduced its references; none when that `optional` is not an element
/// of this pattern (a nullable reference a `from` drew), when the moved
/// triples read names bound after that `optional`, or when a later binding,
/// `let` or `from` would read what they introduce.
fn optional_move(
    catalog: &Catalog,
    sc: &Scope,
    pattern: &Pattern,
    i: usize,
    moves: &mut OptionalMoves,
) -> Option<Vec<TextEdit>> {
    use crate::query::syntax::print;
    let spans = &pattern.spans.elements;
    if spans.len() != pattern.elements.len() {
        return None;
    }
    // The `optional` element before `i` that introduced `name`: the last one
    // whose pattern names it.
    let introduced_by = |name: &str| {
        (0..i).rev().find(|&j| match &pattern.elements[j] {
            Element::Optional(p) => {
                let mut vars = Vec::new();
                pattern_vars(p, &mut vars);
                vars.iter().any(|v| v == name)
            }
            _ => false,
        })
    };
    let mut known: HashMap<String, BoundVariable> = HashMap::new();
    for c in sc.visible() {
        match c.ty {
            Ty::Node { type_name, .. } => {
                known.insert(c.name, BoundVariable::Node { type_name });
            }
            Ty::Edge { .. } => {
                known.insert(c.name, BoundVariable::Edge);
            }
            Ty::Value(_) => {}
        }
    }
    // The names the moved triples introduce, each with the `optional` it
    // moves into.
    let mut introduced: HashMap<String, usize> = HashMap::new();
    let mut moved: Vec<(usize, Vec<(String, String)>)> = Vec::new();
    let mut target: Option<usize> = None;
    for k in i..pattern.elements.len() {
        match &pattern.elements[k] {
            Element::Traversal(u) => {
                let mut into: Vec<usize> = Vec::new();
                for end in [&u.src, &u.dst] {
                    if let Some(j) = introduced.get(end) {
                        into.push(*j);
                    } else if k == i && optional_reference(sc, end) {
                        into.push(introduced_by(end)?);
                    }
                }
                let Some(&j) = into.iter().max() else {
                    continue;
                };
                if into.iter().any(|x| *x != j) || target.is_some_and(|t| t != j) {
                    return None;
                }
                target = Some(j);
                let resolved = resolve_traversal(catalog, u, &known, &sc.declared).ok()?;
                let mut bindings = Vec::new();
                for (end, ty) in [(&u.src, &resolved.src_type), (&u.dst, &resolved.dst_type)] {
                    if end == "_" || known.contains_key(end) {
                        continue;
                    }
                    if sc.declared.contains_key(end) {
                        return None;
                    }
                    known.insert(
                        end.clone(),
                        BoundVariable::Node {
                            type_name: ty.clone(),
                        },
                    );
                    introduced.insert(end.clone(), j);
                    bindings.push((end.clone(), ty.clone()));
                }
                if let Some(k_name) = &u.edge_binding {
                    introduced.insert(k_name.clone(), j);
                }
                moved.push((k, bindings));
            }
            Element::Optional(_) => {}
            Element::Binding { variable, .. } => {
                if introduced.contains_key(variable) {
                    return None;
                }
            }
            Element::Let { .. } | Element::From { .. } => {
                if !introduced.is_empty() {
                    return None;
                }
            }
        }
    }
    let j = target?;
    if moves.targets.contains(&j) {
        return None;
    }
    // A moved triple reads, inside the `optional`, only names bound before
    // it: one bound between the `optional` and the triple would be new there.
    let mut after: Vec<String> = Vec::new();
    for c in &pattern.elements[j + 1..i] {
        after.extend(element_binds(c));
    }
    for (k, _) in &moved {
        if let Element::Traversal(u) = &pattern.elements[*k]
            && [&u.src, &u.dst]
                .into_iter()
                .any(|end| after.contains(end) && !introduced.contains_key(end))
        {
            return None;
        }
    }
    let Element::Optional(inner) = &pattern.elements[j] else {
        return None;
    };
    if inner.spans.elements.len() != inner.elements.len() {
        return None;
    }
    let at = inner.spans.elements.last()?.end;
    let mut text = String::new();
    let mut edits = Vec::new();
    for (k, bindings) in &moved {
        for (name, ty) in bindings {
            text.push_str(&format!(", ${name}: {ty}"));
        }
        text.push_str(", ");
        text.push_str(&print::element(&pattern.elements[*k]));
        edits.push(TextEdit {
            start: spans[k - 1].end,
            end: spans[*k].end,
            replacement: String::new(),
        });
    }
    edits.insert(
        0,
        TextEdit {
            start: at,
            end: at,
            replacement: text,
        },
    );
    moves.targets.insert(j);
    moves.elements.extend(moved.iter().map(|(k, _)| *k));
    Some(edits)
}

/// `T84` for the names an inner scope's triples introduced that no binding
/// element of the scope binds, at the first triple naming one. A name new
/// to a block or an inline read is introduced by a binding; the fix inserts
/// one before each triple that first names such a name, with the type the
/// edge declares for that end, which is the type the triple gave it.
fn unbound_triple_names(
    pattern: &Pattern,
    sc: &Scope,
    introduced: &[(usize, Vec<String>)],
) -> R<()> {
    let mut seen: Vec<String> = Vec::new();
    let mut found: Vec<(usize, Vec<(String, String)>)> = Vec::new();
    for (i, ends) in introduced {
        let mut here = Vec::new();
        for end in ends {
            if sc.declared.contains_key(end) || seen.contains(end) {
                continue;
            }
            let Some(Ty::Node { type_name, .. }) = sc.local(end).map(|c| &c.ty) else {
                continue;
            };
            seen.push(end.clone());
            here.push((end.clone(), type_name.clone()));
        }
        if !here.is_empty() {
            found.push((*i, here));
        }
    }
    let Some((first, _)) = found.first() else {
        return Ok(());
    };
    let scope = if sc.optional {
        "`optional`"
    } else if sc.block {
        "block"
    } else {
        "inline read"
    };
    let names = seen
        .iter()
        .map(|n| format!("`${n}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let bindings = found
        .iter()
        .flat_map(|(_, b)| b.iter())
        .map(|(n, t)| format!("`${n}: {t}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let verb = if seen.len() == 1 { "is" } else { "are" };
    let spans = &pattern.spans.elements;
    let mut d = QueryDiagnostic::typecheck(
        T84,
        format!(
            "{names} {verb} named in a triple of this {scope} but bound nowhere: a name new to a block, an `optional` or an inline read is introduced by a binding element inside it, and a name read from the enclosing scope correlates"
        ),
    )
    .with_fix(format!(
        "bind {} in the {scope}: {bindings}",
        if seen.len() == 1 { "it" } else { "them" }
    ));
    d.position = spans.get(*first).map(|s| s.position);
    let edits: Option<Vec<TextEdit>> = found
        .iter()
        .map(|(i, b)| {
            spans.get(*i).map(|span| TextEdit {
                start: span.start,
                end: span.start,
                replacement: b
                    .iter()
                    .map(|(n, t)| format!("${n}: {t}, "))
                    .collect::<String>(),
            })
        })
        .collect();
    if let Some(edits) = edits.filter(|e| !e.is_empty() && spans.len() == pattern.elements.len()) {
        d = d.with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits,
        });
    }
    Err(E::D(d))
}

/// `T7` for `$d in $list` in `match`: a triple over the edge named `in` whose
/// right end is a list parameter, which is the membership test written in
/// the wrong place; the fix moves it to the `where`.
fn membership_triple(pattern: &Pattern, sc: &Scope) -> Option<QueryDiagnostic> {
    let (i, t) = pattern
        .elements
        .iter()
        .enumerate()
        .find_map(|(i, c)| match c {
            Element::Traversal(t)
                if t.selector == EdgeSelector::Named("in".into())
                    && t.edge_binding.is_none()
                    && !t.undirected
                    && !sc.is_bound(&t.dst)
                    && sc.params.get(&t.dst).is_some_and(|p| p.list) =>
            {
                Some((i, t))
            }
            _ => None,
        })?;
    let test = format!("${} in ${}", t.src, t.dst);
    let mut d = QueryDiagnostic::typecheck(
        T7,
        format!(
            "`${}` is a list, so `{test}` is a membership test, which belongs in `where`; inside `match`, `in` is an edge name",
            t.dst
        ),
    )
    .with_fix(format!("where {test}"));
    let spans = &pattern.spans.elements;
    if let Some(span) = spans.get(i) {
        d.position = Some(span.position);
        d.end = Some(span.end_position);
    }
    // Remove the element with one separator, then add the test to the
    // `where` (or open one after the braces).
    let removal = match (
        spans.get(i),
        spans.get(i + 1),
        i.checked_sub(1).and_then(|p| spans.get(p)),
    ) {
        (Some(this), Some(next), _) => Some((this.start, next.start)),
        (Some(this), None, Some(prev)) => Some((prev.end, this.end)),
        _ => None,
    };
    let insertion = match (&pattern.spans.predicate, pattern.spans.close) {
        (Some(p), _) => Some((p.end, format!(" and {test}"))),
        (None, Some(close)) => Some((close, format!(" where {test}"))),
        _ => None,
    };
    if let (Some((start, end)), Some((at, text))) = (removal, insertion) {
        d = d.with_suggestion(Suggestion {
            applicability: Applicability::MachineApplicable,
            edits: vec![
                TextEdit {
                    start,
                    end,
                    replacement: String::new(),
                },
                TextEdit {
                    start: at,
                    end: at,
                    replacement: text,
                },
            ],
        });
    }
    Some(d)
}

fn first_aggregate_call(e: &Expr) -> Option<&Call> {
    crate::stack::deep(|| match e {
        Expr::Call(c) if AGGREGATES.contains(&c.name.as_str()) => Some(c),
        Expr::Compare { left, right, .. }
        | Expr::Arith { left, right, .. }
        | Expr::And(left, right)
        | Expr::Or(left, right) => {
            first_aggregate_call(left).or_else(|| first_aggregate_call(right))
        }
        Expr::Not(x) | Expr::Neg(x) => first_aggregate_call(x),
        _ => None,
    })
}

/// `e` with the call `call` replaced by `with`.
fn replace_call(e: &Expr, call: &Call, with: &Expr) -> Expr {
    crate::stack::deep(|| {
        let r = |x: &Expr| Box::new(replace_call(x, call, with));
        match e {
            Expr::Call(c) if c == call => with.clone(),
            Expr::Compare {
                op,
                left,
                right,
                at,
            } => Expr::Compare {
                op: *op,
                left: r(left),
                right: r(right),
                at: at.clone(),
            },
            Expr::Arith { op, left, right } => Expr::Arith {
                op: *op,
                left: r(left),
                right: r(right),
            },
            Expr::And(a, b) => Expr::And(r(a), r(b)),
            Expr::Or(a, b) => Expr::Or(r(a), r(b)),
            Expr::Not(x) => Expr::Not(r(x)),
            Expr::Neg(x) => Expr::Neg(r(x)),
            other => other.clone(),
        }
    })
}

/// The conjuncts of a top-level `and`: each is placed after the last binder
/// it reads, so a conjunct is pushed down as far as its own names allow.
fn conjuncts(e: &Expr) -> Vec<&Expr> {
    crate::stack::deep(|| match e {
        Expr::And(l, r) => {
            let mut v = conjuncts(l);
            v.extend(conjuncts(r));
            v
        }
        other => vec![other],
    })
}

/// The name a return item produces: its alias, `var.prop` for a property,
/// the variable for a reference.
pub(crate) fn item_name(e: &Expr, alias: Option<&str>) -> String {
    crate::stack::deep(|| {
        if let Some(a) = alias {
            return a.to_string();
        }
        match e {
            Expr::Prop { variable, property } => format!("{variable}.{property}"),
            Expr::Variable(v) => v.clone(),
            Expr::Literal(_) => "literal".into(),
            Expr::Call(c) if AGGREGATES.contains(&c.name.as_str()) => match c.args.first() {
                Some(Arg::Expr(a)) => item_name(a, None),
                _ => c.name.clone(),
            },
            Expr::AggDistinct { arg, .. } => item_name(arg, None),
            Expr::Call(c) if c.name == "now" => crate::query::value::NOW_PARAM_NAME.to_string(),
            Expr::Call(c) => c.name.clone(),
            _ => "expression".into(),
        }
    })
}

fn is_relation_expr(e: &Expr, sc: &Scope, unit: &HashMap<String, &Decl>) -> bool {
    match e {
        Expr::Variable(v) => sc.rels.contains_key(v),
        Expr::Call(c) => RELATION_ROWS.contains(&c.name.as_str()) || unit.contains_key(&c.name),
        _ => false,
    }
}

/// Lookahead over a scope's elements: the type each binding declares, and
/// every name the scope will bind.
fn lookahead(elements: &[Element], sc: &mut Scope) {
    crate::stack::deep(|| {
        for c in elements {
            match c {
                Element::Binding {
                    variable,
                    type_name,
                } => {
                    sc.declared.insert(
                        variable.clone(),
                        BoundVariable::Node {
                            type_name: type_name.clone(),
                        },
                    );
                    sc.declared_nodes.insert(variable.clone());
                    sc.future.insert(variable.clone());
                }
                Element::Traversal(t) => {
                    sc.future.insert(t.src.clone());
                    sc.future.insert(t.dst.clone());
                    if let Some(k) = &t.edge_binding {
                        sc.future.insert(k.clone());
                    }
                }
                Element::Let { name, .. } => {
                    sc.future.insert(name.clone());
                }
                Element::From {
                    rel: RelExpr::Inline(body),
                    ..
                } => {
                    for p in &body.projections {
                        match (&p.alias, &p.expr) {
                            (Some(a), _) => {
                                sc.future.insert(a.clone());
                            }
                            (None, Expr::Variable(v)) => {
                                sc.future.insert(v.clone());
                            }
                            _ => {}
                        }
                    }
                }
                Element::Optional(inner) => {
                    let mut tmp = Scope::new(HashMap::new(), Vec::new(), HashMap::new());
                    lookahead(&inner.elements, &mut tmp);
                    sc.future.extend(tmp.future);
                }
                Element::From { .. } => {}
            }
        }
    })
}

/// The scope variables a pending filter names: the variables it mentions
/// that the scope binds or can see, a block's own bindings excluded.
fn pending_vars(p: &Pending, sc: &Scope) -> Vec<String> {
    let mut out = Vec::new();
    match p {
        Pending::Filter(e, before, _) => filter_vars(e, before, &mut out),
    }
    out.retain(|v| {
        (sc.future.contains(v) || sc.is_bound(v))
            && !(sc.params.contains_key(v) && !sc.is_bound(v) && !sc.future.contains(v))
    });
    out.sort();
    out.dedup();
    out
}

/// The variables a filter waits for: those it names outside its blocks, and
/// those a block names that were bound before the filter was written (the
/// rest are the block's own).
fn filter_vars(e: &Expr, before: &HashSet<String>, out: &mut Vec<String>) {
    let mut all = Vec::new();
    expr_vars(e, &mut all);
    let mut in_blocks = Vec::new();
    block_vars(e, &mut in_blocks);
    let mut outside = Vec::new();
    expr_vars_outside_blocks(e, &mut outside);
    out.extend(
        all.into_iter()
            .filter(|v| outside.contains(v) || (in_blocks.contains(v) && before.contains(v))),
    );
}

/// The variables named inside the blocks of `e`.
fn block_vars(e: &Expr, out: &mut Vec<String>) {
    crate::stack::deep(|| match e {
        Expr::Block(b) => {
            pattern_vars(&b.pattern, out);
            for a in b.arg.iter().chain(&b.fraction) {
                expr_vars(a, out);
            }
        }
        other => for_each_child(other, &mut |child| block_vars(child, out)),
    })
}

/// The variables `e` names outside its blocks.
fn expr_vars_outside_blocks(e: &Expr, out: &mut Vec<String>) {
    crate::stack::deep(|| match e {
        Expr::Block(_) => {}
        Expr::Variable(v) | Expr::Prop { variable: v, .. } => out.push(v.clone()),
        other => for_each_child(other, &mut |child| expr_vars_outside_blocks(child, out)),
    })
}

/// Every direct operand of an expression that is not a block.
/// The syntax nodes of a pattern, counted until they pass `limit`: its
/// elements, every expression node, and the patterns, blocks and inline
/// reads nested in it. The count stops at the limit, so reading a pattern
/// for its size costs at most the budget left.
struct SyntaxSize {
    n: usize,
    limit: usize,
}

impl SyntaxSize {
    fn of(p: &Pattern, limit: usize) -> usize {
        let mut size = Self { n: 0, limit };
        size.pattern(p);
        size.n
    }

    /// Count `k` nodes; whether to go on.
    fn add(&mut self, k: usize) -> bool {
        self.n = self.n.saturating_add(k);
        self.n <= self.limit
    }

    fn pattern(&mut self, p: &Pattern) -> bool {
        crate::stack::deep(|| {
            for e in &p.elements {
                let more = self.add(1)
                    && match e {
                        Element::Optional(q) => self.pattern(q),
                        Element::Let {
                            value: LetValue::Expr(x),
                            ..
                        } => self.expr(x),
                        Element::Let {
                            value: LetValue::Relation(body),
                            ..
                        }
                        | Element::From {
                            rel: RelExpr::Inline(body),
                            ..
                        } => self.body(body),
                        Element::From {
                            rel: RelExpr::Call(c),
                            ..
                        } => self.call(c),
                        Element::From { .. } | Element::Binding { .. } | Element::Traversal(_) => {
                            true
                        }
                    };
                if !more {
                    return false;
                }
            }
            p.predicate.as_ref().is_none_or(|w| self.expr(w))
        })
    }

    fn body(&mut self, b: &ReadBody) -> bool {
        self.pattern(&b.pattern)
            && b.projections.iter().all(|p| self.expr(&p.expr))
            && b.order.iter().all(|o| self.expr(&o.expr))
    }

    fn expr(&mut self, e: &Expr) -> bool {
        crate::stack::deep(|| {
            if !self.add(1) {
                return false;
            }
            match e {
                Expr::Block(b) => {
                    self.pattern(&b.pattern)
                        && b.arg.as_deref().is_none_or(|a| self.expr(a))
                        && b.fraction.as_deref().is_none_or(|p| self.expr(p))
                }
                Expr::Call(c) => self.call(c),
                other => {
                    let mut more = true;
                    for_each_child(other, &mut |child| more = more && self.expr(child));
                    more
                }
            }
        })
    }

    fn call(&mut self, c: &Call) -> bool {
        self.add(1)
            && c.args.iter().all(|a| match a {
                Arg::Expr(e) => self.expr(e),
                Arg::Inline(body) => self.body(body),
            })
            && c.options.iter().all(|(_, e)| self.expr(e))
    }
}

fn for_each_child(e: &Expr, f: &mut dyn FnMut(&Expr)) {
    match e {
        Expr::Literal(_)
        | Expr::Variable(_)
        | Expr::Prop { .. }
        | Expr::Meta(_)
        | Expr::Ident(_)
        | Expr::Block(_) => {}
        Expr::Call(c) => {
            for a in &c.args {
                if let Arg::Expr(e) = a {
                    f(e);
                }
            }
            for (_, e) in &c.options {
                f(e);
            }
        }
        Expr::AggDistinct { arg, fraction, .. } => {
            f(arg);
            if let Some(p) = fraction {
                f(p);
            }
        }
        Expr::Neg(arg) | Expr::Not(arg) => f(arg),
        Expr::IsNull { expr, .. } => f(expr),
        Expr::Arith { left, right, .. }
        | Expr::Compare { left, right, .. }
        | Expr::InfixString { left, right, .. }
        | Expr::And(left, right)
        | Expr::Or(left, right) => {
            f(left);
            f(right);
        }
        Expr::In { needle, list, .. } => {
            f(needle);
            f(list);
        }
        Expr::List { elements, .. } => elements.iter().for_each(f),
    }
}

/// The variable a write statement targets: a statement targeting one of
/// the error type is of the error type; its values read their names where
/// they are typed.
fn statement_names(s: &Stmt) -> Vec<String> {
    match s {
        Stmt::Insert { .. } => Vec::new(),
        Stmt::Update { variable, .. } | Stmt::Delete { variable } => vec![variable.clone()],
    }
}

/// The variables an element names outside its expressions and inner
/// scopes: what it binds, and the named relation a `from` draws. An element
/// naming one of the error type is of the error type; a name read inside an
/// expression or an inner scope is of the error type where it is read, so
/// the rest of that expression or scope is still checked.
/// How a refusal names the element `c`.
fn element_word(c: &Element) -> &'static str {
    match c {
        Element::Let { .. } => "the `let`",
        Element::From { .. } => "the `from`",
        Element::Optional(_) => "the `optional`",
        Element::Binding { .. } => "the binding",
        Element::Traversal(_) => "the traversal",
    }
}

fn element_names(c: &Element) -> Vec<String> {
    let mut out = match c {
        Element::Optional(_) => Vec::new(),
        Element::From { score, .. } => score.iter().cloned().collect(),
        _ => element_binds(c),
    };
    if let Element::From {
        rel: RelExpr::Variable(v),
        ..
    } = c
    {
        out.push(v.clone());
    }
    out
}

/// Element `index` of a scope was refused, or is of the error type: what it
/// would have bound is of the error type, the columns of a `from` that is
/// not an inline read are unknown, and it joins the components it names.
fn refused_element(sc: &mut Scope, parts: &mut components::Parts, index: usize, c: &Element) {
    let binds = element_binds(c);
    sc.poison(&binds);
    if matches!(c, Element::From { rel, .. } if !matches!(rel, RelExpr::Inline(_))) {
        sc.opaque = true;
    }
    sc.refused = true;
    let mut names = element_names(c);
    names.extend(binds);
    parts.refused(index, names);
}

/// The names an element would bind.
fn element_binds(c: &Element) -> Vec<String> {
    match c {
        Element::Let { name, .. } => vec![name.clone()],
        Element::Binding { variable, .. } => vec![variable.clone()],
        Element::Traversal(t) => [Some(&t.src), Some(&t.dst), t.edge_binding.as_ref()]
            .into_iter()
            .flatten()
            .filter(|n| *n != "_")
            .cloned()
            .collect(),
        Element::Optional(p) => {
            let mut out = Vec::new();
            pattern_vars(p, &mut out);
            out
        }
        Element::From { rel, score } => {
            let mut out: Vec<String> = score.iter().cloned().collect();
            if let RelExpr::Inline(body) = rel {
                for p in &body.projections {
                    match (&p.alias, &p.expr) {
                        (Some(a), _) => out.push(a.clone()),
                        (None, Expr::Variable(v)) => out.push(v.clone()),
                        _ => {}
                    }
                }
            }
            out
        }
    }
}

fn pattern_vars(pattern: &Pattern, out: &mut Vec<String>) {
    crate::stack::deep(|| {
        for c in &pattern.elements {
            match c {
                Element::Binding { variable, .. } => out.push(variable.clone()),
                Element::Traversal(t) => {
                    out.push(t.src.clone());
                    out.push(t.dst.clone());
                }
                Element::Optional(p) => pattern_vars(p, out),
                Element::Let { value, .. } => {
                    if let LetValue::Expr(e) = value {
                        expr_vars(e, out)
                    }
                }
                Element::From { score, .. } => out.extend(score.iter().cloned()),
            }
        }
        if let Some(p) = &pattern.predicate {
            expr_vars(p, out);
        }
    })
}

/// The first span written inside `e`, in source order: where a conjunct of
/// a `where` is placed.
fn first_span(e: &Expr) -> Option<SourceSpan> {
    crate::stack::deep(|| {
        let own = match e {
            Expr::Compare { at, .. } | Expr::In { at, .. } | Expr::InfixString { at, .. } => {
                Some(at)
            }
            Expr::Block(b) => Some(&b.at),
            _ => None,
        };
        let mut found = own.filter(|at| at.end > at.start).cloned();
        for_each_child(e, &mut |child| {
            if let Some(span) = first_span(child)
                && found.as_ref().is_none_or(|f| span.start < f.start)
            {
                found = Some(span);
            }
        });
        found
    })
}

/// Every variable `e` names, inside its blocks too.
fn expr_names(e: &Expr) -> Vec<String> {
    let mut out = Vec::new();
    expr_vars(e, &mut out);
    out
}

fn expr_vars(e: &Expr, out: &mut Vec<String>) {
    crate::stack::deep(|| match e {
        Expr::Variable(v) | Expr::Prop { variable: v, .. } => out.push(v.clone()),
        Expr::Literal(_) | Expr::Meta(_) | Expr::Ident(_) => {}
        Expr::Call(c) => {
            for a in &c.args {
                if let Arg::Expr(e) = a {
                    expr_vars(e, out);
                }
            }
            for (_, e) in &c.options {
                expr_vars(e, out);
            }
        }
        Expr::AggDistinct { arg, fraction, .. } => {
            expr_vars(arg, out);
            if let Some(p) = fraction {
                expr_vars(p, out);
            }
        }
        Expr::Neg(arg) | Expr::Not(arg) => expr_vars(arg, out),
        Expr::IsNull { expr, .. } => expr_vars(expr, out),
        Expr::Block(b) => {
            pattern_vars(&b.pattern, out);
            for a in b.arg.iter().chain(&b.fraction) {
                expr_vars(a, out);
            }
        }
        Expr::Arith { left, right, .. }
        | Expr::Compare { left, right, .. }
        | Expr::InfixString { left, right, .. }
        | Expr::And(left, right)
        | Expr::Or(left, right) => {
            expr_vars(left, out);
            expr_vars(right, out);
        }
        Expr::In { needle, list, .. } => {
            expr_vars(needle, out);
            expr_vars(list, out);
        }
        Expr::List { elements, .. } => elements.iter().for_each(|e| expr_vars(e, out)),
    })
}

mod bind;
mod components;
mod literal;
mod lowering;
pub(crate) mod nearest;
mod result;
pub use result::{projection_field, result_schema};
mod scalar;
pub mod soundness;
mod traversal;
pub use scalar::{SExpr, SKind, Terms};

#[cfg(test)]
mod tests;
