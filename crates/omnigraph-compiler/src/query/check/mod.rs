//! The edition-3 type checker (docs/rfcs/2026-11-07-gq-3.md, sections 2, 3
//! and 5).
//!
//! It reads the edition-3 parse tree ([`crate::query::syntax`]) of one source unit and
//! types every declaration into the core IR ([`crate::ir::core`]), or
//! refuses it with a typed code. The derived-forms table of section 2 is the
//! lowering: each clause becomes a step, and the steps fold into one term in
//! clause order (binders in written order, a filter after the last binder of
//! a variable it names, a block, `let` and `from` seeing only earlier text,
//! so a block correlates on the names bound before it).
//!
//! Rules that are *[as today]* reuse the edition-2 checker's functions
//! (traversal resolution, literal typing, type compatibility), so the two
//! editions agree on the shapes they share; the tests compare the two on the
//! repository's corpus.

use std::collections::{HashMap, HashSet};

use crate::catalog::Catalog;
use crate::error::CompilerError;
use crate::ir::core::WriteGroup;
use crate::ir::core::*;
use crate::query::ast::{AggFunc, CompOp, EdgeSelector, Literal, Traversal};
use crate::query::codes::*;
use crate::query::diagnostic::{Applicability, QueryCode, QueryDiagnostic, Suggestion, TextEdit};
use crate::query::syntax::*;
use crate::query::typecheck::{
    BoundVariable, MutationTarget, assignment_compatible, check_literal_type, literal_type,
    resolve_traversal, types_compatible,
};
use crate::types::{Direction, PropType, ScalarType};

/// The internal error: a refusal, or the signal that a recursive read's type
/// is not yet known (the base pass of [`Ctx::read_decl`]).
#[derive(Debug, Clone)]
enum E {
    D(QueryDiagnostic),
    Pending(String),
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

/// Type-check every declaration of an edition-3 file against `catalog`. The
/// declarations of the file are the source unit a `Call` resolves against.
pub fn check_file(
    catalog: &Catalog,
    file: &SourceFile,
) -> Vec<(String, Result<CheckedDecl, QueryDiagnostic>)> {
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain(d) => vec![d.as_ref()],
        Body::Statement(_) => Vec::new(),
    };
    let unit: HashMap<String, &Decl> = decls.iter().map(|d| (d.name.clone(), *d)).collect();
    decls
        .iter()
        .map(|d| (d.name.clone(), check_decl(catalog, &unit, d)))
        .collect()
}

/// Type-check the edition-3 declaration the front door handed on, with the
/// other declarations of its file as the source unit its calls resolve in.
pub fn check_syntax_decl(
    catalog: &Catalog,
    syntax: &crate::query::ast::SyntaxDecl,
) -> Result<CoreDecl, QueryDiagnostic> {
    let file = syntax.file();
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain(d) => vec![d.as_ref()],
        Body::Statement(_) => Vec::new(),
    };
    let unit: HashMap<String, &Decl> = decls.iter().map(|d| (d.name.clone(), *d)).collect();
    check_decl(catalog, &unit, syntax.decl()).map(|checked| checked.core)
}

/// Each declaration's name and its checked form or refusal.
pub type CheckedDecls = Vec<(String, Result<CheckedDecl, QueryDiagnostic>)>;

/// Parse an edition-3 source (it must open with `gq 3;`) and type-check it.
pub fn check_source(catalog: &Catalog, source: &str) -> Result<CheckedDecls, QueryDiagnostic> {
    let file = crate::query::syntax::parse::parse_source(source)?;
    Ok(check_file(catalog, &file))
}

/// Type-check one declaration of a source unit.
pub fn check_decl(
    catalog: &Catalog,
    unit: &HashMap<String, &Decl>,
    decl: &Decl,
) -> Result<CheckedDecl, QueryDiagnostic> {
    let mut ctx = Ctx {
        catalog,
        unit,
        fresh: 0,
        frames: Vec::new(),
        assume: HashMap::new(),
        rec_hits: HashSet::new(),
        in_write: false,
    };
    let params = match param_types(&decl.params) {
        Ok(p) => p,
        Err(E::D(d)) => return Err(d),
        Err(E::Pending(_)) => unreachable!(),
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
                term: r.rel.term,
                result: r.rel.ty,
                plan_budget: r.budget,
            }))
        }),
        DeclBody::Write(w) => ctx.write_decl(decl, w).map(CoreDecl::Write),
    };
    match out {
        Ok(core) => Ok(CheckedDecl { core }),
        Err(E::D(d)) => Err(d),
        Err(E::Pending(name)) => Err(QueryDiagnostic::typecheck(
            T70,
            format!(
                "`{name}` calls itself on every branch; a recursion needs a base case, a `union` arm that does not call it"
            ),
        )),
    }
}

/// Edition 2's infix `contains` or `starts_with`, refused here because the
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
        format!(
            "`{}` is a function in edition 3, not an infix operator",
            op.name()
        ),
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
    diagnostic
}

/// `T65`: a ranking written as a scalar call, in `order`, `return` or a
/// filter; the fix draws it with `from` and reads its score column.
fn ranking_as_scalar(name: &str) -> QueryDiagnostic {
    let fix = match name {
        "nearest" => {
            "from nearest(<population>, $x.field, $q, k: <limit>)  and  order { $distance }"
        }
        "bm25" => "from bm25(<population>, $x.field, $q)  and  order { $score desc }",
        "rrf" => "from rrf(<arm>, <arm>, k: 60)  and  order { $score desc }",
        _ => "from distance(<population>, $x.field, $q)  and  order { $distance }",
    };
    QueryDiagnostic::typecheck(
        T65,
        format!(
            "`{name}` is a relation function in edition 3; a ranking is drawn with `from`, and its score is a column"
        ),
    )
    .with_fix(fix)
}

// ---------------------------------------------------------------------------
// Registry.

/// Relation functions (kind `relation`).
const RELATION_ROWS: &[&str] = &["distance", "nearest", "bm25", "rrf", "union"];
/// Scalar rows, today's and the break's.
const SCALAR_ROWS: &[&str] = &[
    "contains",
    "starts_with",
    "search",
    "fuzzy",
    "match_text",
    "match_terms",
    "terms",
    "now",
    "if",
    "coalesce",
    "concat",
];
const AGGREGATES: &[&str] = &["count", "sum", "avg", "min", "max"];
/// Words a registry row may never be named (section 5).
const CLAUSE_KEYWORDS: &[&str] = &["let", "from", "optional", "exists", "not"];
/// Annotations retained as pure metadata.
const METADATA_ANNOTATIONS: &[&str] = &["description", "instruction", "example", "rename_from"];

fn is_registry_name(name: &str) -> bool {
    RELATION_ROWS.contains(&name) || SCALAR_ROWS.contains(&name) || AGGREGATES.contains(&name)
}

// ---------------------------------------------------------------------------
// Context, frames and scopes.

struct Ctx<'a> {
    catalog: &'a Catalog,
    unit: &'a HashMap<String, &'a Decl>,
    fresh: u32,
    /// The declared reads being inlined, outermost first.
    frames: Vec<Frame>,
    /// The assumed type of a recursive read during its fixpoint passes.
    assume: HashMap<String, RelTy>,
    /// Reads referenced while on the frame stack (they compile to `Fix`).
    rec_hits: HashSet<String>,
    /// Checking a write body: a declared read is not callable there until
    /// stage W2.
    in_write: bool,
}

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
    /// Whether each column is variable-shaped (a bare variable or an alias)
    /// and so may be drawn into scope by `from`.
    shaped: Vec<bool>,
    ranked: Option<Ranked>,
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
        dst_type: String,
        selector: Vec<(String, Direction)>,
        min_hops: u32,
        max_hops: Option<u32>,
        edge_var: Option<String>,
        /// Both endpoints were bound: the expansion targets a fresh column
        /// and a filter ties it to `dst`.
        semi: Option<String>,
    },
    /// Correlated block aggregates (each the inner `Group`, and the column
    /// that receives it), then the filter that reads them.
    Filter {
        blocks: Vec<(Term, String, Agg)>,
        pred: SExpr,
    },
    /// `let $v = expr`.
    Let {
        blocks: Vec<(Term, String, Agg)>,
        name: String,
        value: SExpr,
    },
    /// `optional { p }`: the inner term, its new columns, and the columns
    /// visible before it (for the padding arm's types).
    Optional {
        inner: Term,
        new_cols: Vec<Col>,
        before: Vec<Col>,
    },
}

#[derive(Clone)]
struct Scope {
    params: HashMap<String, PropType>,
    /// Columns of enclosing scopes visible here (correlation).
    outer: Vec<Col>,
    cols: Vec<Col>,
    rels: HashMap<String, Rel>,
    free: Vec<String>,
    /// The binder step index of each local column.
    bound_at: HashMap<String, usize>,
    steps: Vec<(usize, Step)>,
    /// Binder count so far.
    binders: usize,
    /// Lookahead: the type every binding clause of this scope declares.
    declared: HashMap<String, BoundVariable>,
    declared_nodes: HashSet<String>,
    /// Names this scope's clauses will bind (lookahead for block
    /// correlation).
    future: HashSet<String>,
    /// Block aggregates produced while typing the current expression.
    block_buf: Vec<(Term, String, Agg)>,
    pending: Vec<Pending>,
    /// True inside an inline read or a called body: a binding reusing an
    /// outer name is `T68`, since it would silently become a filter.
    inner: bool,
    /// True inside a block or `optional`: a binding that re-mentions an
    /// outer variable correlates on it, as edition 2 reads it.
    correlates: bool,
    /// While a filter written earlier is placed: the names bound before it,
    /// the only outer names a block inside it correlates on.
    block_visible: Option<Vec<String>>,
}

#[derive(Clone)]
enum Pending {
    /// A filter clause and the names bound before it: a block inside it
    /// correlates on those alone.
    Filter(Expr, Vec<String>),
    Braces {
        var: String,
        prop: String,
        value: Expr,
    },
}

impl Scope {
    fn new(params: HashMap<String, PropType>, outer: Vec<Col>, rels: HashMap<String, Rel>) -> Self {
        let inner = !outer.is_empty();
        Self {
            params,
            outer,
            cols: Vec::new(),
            rels,
            free: Vec::new(),
            bound_at: HashMap::new(),
            steps: Vec::new(),
            binders: 0,
            declared: HashMap::new(),
            declared_nodes: HashSet::new(),
            future: HashSet::new(),
            block_buf: Vec::new(),
            pending: Vec::new(),
            inner,
            correlates: false,
            block_visible: None,
        }
    }

    /// Every column visible here, local first then outer, for an inner
    /// scope's `outer`.
    fn visible(&self) -> Vec<Col> {
        let mut out = self.cols.clone();
        for c in &self.outer {
            if !out.iter().any(|o| o.name == c.name) {
                out.push(c.clone());
            }
        }
        out
    }

    fn local(&self, name: &str) -> Option<&Col> {
        self.cols.iter().find(|c| c.name == name)
    }

    fn outer_col(&self, name: &str) -> Option<&Col> {
        self.outer.iter().find(|c| c.name == name)
    }

    /// The type of a column visible here, marking an outer one free.
    fn col(&mut self, name: &str) -> Option<Ty> {
        if let Some(c) = self.local(name) {
            return Some(c.ty.clone());
        }
        let ty = self.outer_col(name)?.ty.clone();
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

    /// The step index after which a value naming `vars` may be placed: the
    /// last binder of a local variable, or before every binder.
    fn position(&self, vars: &[String]) -> usize {
        vars.iter()
            .filter_map(|v| self.bound_at.get(v).map(|i| i + 1))
            .max()
            .unwrap_or(0)
    }

    fn push_binder(&mut self, step: Step, binds: &[Col]) {
        let at = self.binders;
        self.binders += 1;
        self.steps.push((at * 2 + 1, step));
        for c in binds {
            self.bound_at.insert(c.name.clone(), at);
            self.cols.push(c.clone());
        }
    }

    /// A filter placed after binder `after` (0: before every binder, which
    /// the fold reads as after the first leaf, or over `Env`).
    fn push_filter(&mut self, after: usize, step: Step) {
        self.steps.push((after * 2, step));
    }
}

fn param_types(params: &[Param]) -> R<Vec<(String, PropType)>> {
    let mut out = Vec::new();
    for p in params {
        let Some(ty) = PropType::from_param_type_name(&p.type_name, p.nullable) else {
            return Err(err(
                T48,
                format!("unknown parameter type `{}` for `${}`", p.type_name, p.name),
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
            let lt = literal_type(&lit)?;
            check_literal_type(&lit, &lt, &ty, &p.name)?;
        }
        out.push((p.name.clone(), ty));
    }
    Ok(out)
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
/// name is refused, naming the stage that types it where one does.
fn annotations(decl: &Decl) -> R<Budget> {
    let mut budget = Budget {
        plan: DEFAULT_PLAN_BUDGET,
        rounds: None,
    };
    for a in &decl.annotations {
        annotation(a, &mut budget).map_err(|e| match e {
            // The suggestion removes the annotation; whether the text still
            // means what its author wanted is theirs to judge.
            E::D(mut d) => {
                d.position = Some(a.at.position);
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
        })?;
    }
    Ok(budget)
}

fn annotation(a: &Annotation, budget: &mut Budget) -> R<()> {
    {
        match a.name.as_str() {
            name if METADATA_ANNOTATIONS.contains(&name) => return Ok(()),
            "budget" => {}
            "if_commit" | "valid" => {
                return Err(err(
                    T80,
                    format!(
                        "`@{}` belongs to the writes RFC (stage W1) and is not read in this release",
                        a.name
                    ),
                ));
            }
            other => {
                return Err(err(
                    T80,
                    format!(
                        "`@{other}` is not an annotation edition 3 reads; the metadata annotations are @{}, and `@budget(plan = n, rounds = n)` is typed",
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
                        format!(
                            "`@budget({key} = …)` is typed by the agent-interaction stage (N), not by this release"
                        ),
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
    fn fresh(&mut self, prefix: &str) -> String {
        self.fresh += 1;
        format!("_{prefix}{}", self.fresh)
    }

    // -----------------------------------------------------------------------
    // Declarations.

    fn read_decl(&mut self, decl: &Decl) -> R<ReadOut> {
        if is_registry_name(&decl.name) || CLAUSE_KEYWORDS.contains(&decl.name.as_str()) {
            return Err(err(
                T74,
                format!(
                    "a declared read may not be named `{}`, which is a registry row or a clause keyword",
                    decl.name
                ),
            ));
        }
        let DeclBody::Read(body) = &decl.body else {
            return Err(err(
                T74,
                format!(
                    "`{}` is a write; a read cannot draw rows from a write (A7)",
                    decl.name
                ),
            ));
        };
        let params = param_types(&decl.params)?;
        let budget = annotations(decl)?;
        let name = decl.name.clone();
        self.rec_hits.remove(&name);
        self.assume.remove(&name);
        let pmap: HashMap<String, PropType> = params.iter().cloned().collect();
        let mut rel = self.read_body(body, Scope::new(pmap.clone(), Vec::new(), HashMap::new()))?;
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
        let size = rel.term.size();
        if size > budget.plan {
            let mut chain = Vec::new();
            call_chain(&rel.term, &mut chain);
            return Err(err_fix(
                T70,
                format!(
                    "`{name}` expands to {size} plan nodes, over its plan budget of {}; the expansion runs through {}",
                    budget.plan,
                    if chain.is_empty() {
                        "no declared read".to_string()
                    } else {
                        chain.join(" → ")
                    }
                ),
                format!(
                    "declare `@budget(plan = {size})` on `{name}`, or share a relation with `let`"
                ),
            ));
        }
        Ok(ReadOut {
            params,
            rel,
            budget: budget.plan,
        })
    }

    fn write_decl(&mut self, decl: &Decl, body: &WriteBody) -> R<CoreWrite> {
        let params = param_types(&decl.params)?;
        annotations(decl)?;
        let pmap: HashMap<String, PropType> = params.iter().cloned().collect();
        let mut groups = Vec::new();
        let mut class: Option<(WriteClass, String)> = None;
        // A group's statements run in order, each keyed effect seeing the
        // ones before it, and across statements the later effect wins; two
        // values for one entity reached through the rows of one statement
        // are found only by running it, so no conflict is refused here.
        for (g, group) in body.groups.iter().enumerate() {
            let mut sc = Scope::new(pmap.clone(), Vec::new(), HashMap::new());
            self.clauses(&mut sc, &group.clauses)?;
            let input = self.finish(&mut sc)?.unwrap_or(Term::Unit);
            let mut sinks = Vec::new();
            for (i, s) in group.statements.iter().enumerate() {
                let (sink, this) = self.statement(&mut sc, s)?;
                let label = format!("statement {} of group {}", i + 1, g + 1);
                match &class {
                    Some((c, first)) if *c != this => {
                        return Err(err(
                            T76,
                            format!(
                                "{label} is {} but {first} is {}; a write mixes no `delete` with an `insert` or `update` (the constructive-or-destructive split, as today)",
                                class_word(this),
                                class_word(*c)
                            ),
                        ));
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
            groups,
            class: class.map_or(WriteClass::Constructive, |(c, _)| c),
            write_mode: decl.write_mode.clone(),
        })
    }

    // -----------------------------------------------------------------------
    // Reads.

    /// A read body in `sc` (top level: no outer columns; inline: the
    /// enclosing scope's visible columns).
    fn read_body(&mut self, body: &ReadBody, mut sc: Scope) -> R<Rel> {
        self.clauses(&mut sc, &body.clauses)?;
        // Return items, typed beside every match column.
        let mut items = Vec::new();
        let mut aggs = Vec::new();
        let mut post: Vec<(String, SExpr)> = Vec::new();
        let mut shaped = Vec::new();
        let mut names: Vec<String> = Vec::new();
        for p in &body.projections {
            let name = item_name(&p.expr, p.alias.as_deref());
            if names.contains(&name) {
                return Err(err(
                    T25,
                    format!("two return items produce the column `{name}`; alias one"),
                ));
            }
            names.push(name.clone());
            shaped.push(p.alias.is_some() || matches!(p.expr, Expr::Variable(_)));
            if let Some(agg) = self.aggregate(&mut sc, &p.expr)? {
                aggs.push((format!("^{name}"), agg));
                items.push(None);
            } else if contains_agg(&p.expr) {
                // An expression over aggregates (`count($p) > 3 as big`): the
                // aggregates are hidden `Group` columns, the expression a
                // projection after the group.
                let mut hidden = Vec::new();
                let rewritten = self.extract_aggs(&mut sc, &p.expr, &mut hidden)?;
                for (n, a) in &hidden {
                    sc.cols.push(Col {
                        name: n.clone(),
                        ty: a.ty.clone(),
                    });
                }
                let e = self.expr(&mut sc, &rewritten, At::Value)?;
                if p.alias.is_none() {
                    return Err(err(
                        T43,
                        "an expression over aggregates in return needs an alias",
                    ));
                }
                aggs.extend(hidden);
                post.push((format!("^{name}"), e));
                items.push(None);
            } else {
                let e = self.expr(&mut sc, &p.expr, At::Value)?;
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
                items.push(Some((format!("^{name}"), e)));
            }
        }
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
        let mut term = self.finish(&mut sc)?.unwrap_or(Term::Unit);
        let grouped = !aggs.is_empty() || !post.is_empty() || body.distinct;
        if body.distinct && !aggs.is_empty() {
            return Err(err(
                T56,
                "`return distinct` does not apply to an aggregated return",
            ));
        }
        let mut out_cols: Vec<Col> = Vec::new();
        let ext_names: Vec<String>;
        if grouped {
            let keys: Vec<(String, SExpr)> = items.iter().flatten().cloned().collect();
            for (i, it) in items.iter().enumerate() {
                let ty = match it {
                    Some((_, e)) => e.ty.clone(),
                    None => {
                        let n = format!("^{}", names[i]);
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
            ext_names = names.iter().map(|n| format!("^{n}")).collect();
            // Order keys over the groups: a key item's expression, or an
            // alias.
            let mut order = Vec::new();
            for o in &body.order {
                let same_item = body.projections.iter().position(|p| p.expr == o.expr);
                let k = match &o.expr {
                    Expr::Ident(alias) if names.contains(alias) => {
                        let i = names.iter().position(|n| n == alias).unwrap();
                        SExpr::new(SKind::Col(format!("^{alias}")), out_cols[i].ty.clone())
                    }
                    _ if same_item.is_some() => {
                        let i = same_item.unwrap();
                        SExpr::new(SKind::Col(format!("^{}", names[i])), out_cols[i].ty.clone())
                    }
                    e if self.aggregate(&mut sc, e).is_ok_and(|a| a.is_some()) => {
                        let a = self.aggregate(&mut sc, e)?.unwrap();
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
                    e => {
                        let typed = self.order_key(&mut sc, e)?;
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
                };
                order.push((k, o.descending.unwrap_or(false)));
            }
            term = Term::Group {
                input: term.boxed(),
                keys,
                aggs,
            };
            if !post.is_empty() {
                term = Term::Project {
                    input: term.boxed(),
                    items: post,
                    keep: true,
                };
            }
            if !order.is_empty() {
                term = Term::Order {
                    input: term.boxed(),
                    keys: order,
                };
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
            for o in &body.order {
                let k = match &o.expr {
                    Expr::Ident(alias) => match ext.iter().find(|(n, _)| n[1..] == **alias) {
                        Some((n, e)) => SExpr::new(SKind::Col(n.clone()), e.ty.clone()),
                        None => {
                            return Err(err(
                                T42,
                                format!("`{alias}` in `order` names no return alias"),
                            ));
                        }
                    },
                    e => self.order_key(&mut sc, e)?,
                };
                order.push((k, o.descending.unwrap_or(false)));
            }
            term = Term::Project {
                input: term.boxed(),
                items: ext,
                keep: true,
            };
            if !order.is_empty() {
                term = Term::Order {
                    input: term.boxed(),
                    keys: order,
                };
            }
        }
        if let Some(limit) = &body.limit {
            let k = match limit {
                Limit::Literal(n) => Count::Literal(*n),
                Limit::Param(p) => {
                    let Some(t) = sc.params.get(p) else {
                        return Err(err(T6, format!("`limit ${p}` names no parameter")));
                    };
                    if t.list || !is_integer(t.scalar) {
                        return Err(err(
                            T7,
                            format!(
                                "`limit ${p}` needs an integer parameter, got {}",
                                t.display_name()
                            ),
                        ));
                    }
                    Count::Param(p.clone())
                }
            };
            term = Term::Cut {
                input: term.boxed(),
                k,
            };
        }
        term = Term::Project {
            input: term.boxed(),
            items: ext_names
                .iter()
                .zip(&out_cols)
                .map(|(n, c)| {
                    (
                        c.name.clone(),
                        SExpr::new(SKind::Col(n.clone()), c.ty.clone()),
                    )
                })
                .collect(),
            keep: false,
        };
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
            free: sc.free.clone(),
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
        if let Some(a) = self.aggregate(sc, e)? {
            self.fresh += 1;
            let n = format!("^_ag{}", self.fresh);
            hidden.push((n.clone(), a));
            return Ok(Expr::Variable(n));
        }
        let b = |x: &Expr,
                 this: &mut Self,
                 sc: &mut Scope,
                 h: &mut Vec<(String, Agg)>|
         -> R<Box<Expr>> { Ok(Box::new(this.extract_aggs(sc, x, h)?)) };
        Ok(match e {
            Expr::Compare { op, left, right } => Expr::Compare {
                op: *op,
                left: b(left, self, sc, hidden)?,
                right: b(right, self, sc, hidden)?,
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
    }

    /// An aggregate return item, or `None`.
    fn aggregate(&mut self, sc: &mut Scope, e: &Expr) -> R<Option<Agg>> {
        let (func, distinct, arg) = match e {
            Expr::Call(c) if AGGREGATES.contains(&c.name.as_str()) => {
                if !c.options.is_empty() || c.args.len() != 1 {
                    return Err(err(T8, format!("`{}` takes one argument", c.name)));
                }
                let Arg::Expr(a) = &c.args[0] else {
                    return Err(err(
                        T69,
                        format!(
                            "`{}` aggregates a column; an inline read is a relation",
                            c.name
                        ),
                    ));
                };
                (agg_func(&c.name), false, a)
            }
            Expr::AggDistinct { func, arg } => (*func, true, arg.as_ref()),
            _ => return Ok(None),
        };
        if distinct && func != AggFunc::Count {
            return Err(err(T57, "distinct applies only to count"));
        }
        let a = self.expr(sc, arg, At::Value)?;
        let ty = agg_type(func, &a.ty)?;
        Ok(Some(Agg {
            func,
            distinct,
            arg: Some(a),
            ty,
        }))
    }

    /// Fold a scope's steps into its term, after the last pending filters.
    fn finish(&mut self, sc: &mut Scope) -> R<Option<Term>> {
        self.flush(sc, true)?;
        let mut steps = std::mem::take(&mut sc.steps);
        steps.sort_by_key(|(k, _)| *k);
        let env = if sc.free.is_empty() {
            None
        } else {
            Some(Term::Env(
                sc.outer
                    .iter()
                    .filter(|c| sc.free.contains(&c.name))
                    .cloned()
                    .collect(),
            ))
        };
        let mut acc = env;
        // Filters placed before every binder wait for the first leaf when
        // there is no `Env` to sit on.
        let mut early = Vec::new();
        for (k, step) in steps {
            if acc.is_none() && k == 0 && !matches!(step, Step::Leaf(_)) {
                early.push(step);
                continue;
            }
            acc = Some(apply(acc, step, &mut self.fresh));
            if !early.is_empty() {
                for s in std::mem::take(&mut early) {
                    acc = Some(apply(acc, s, &mut self.fresh));
                }
            }
        }
        for s in early {
            acc = Some(apply(acc.or(Some(Term::Unit)), s, &mut self.fresh));
        }
        Ok(acc)
    }

    /// Type every clause of a scope.
    fn clauses(&mut self, sc: &mut Scope, clauses: &[Clause]) -> R<()> {
        lookahead(clauses, sc);
        for c in clauses {
            self.clause(sc, c)?;
            self.flush(sc, false)?;
        }
        Ok(())
    }

    /// Place every pending filter whose variables are bound (all of them at
    /// the end of a scope).
    fn flush(&mut self, sc: &mut Scope, all: bool) -> R<()> {
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
            let at = sc.position(&vars);
            match p {
                Pending::Filter(e, before) => {
                    // An error abandons the scope, so the restriction needs
                    // no reset on that path.
                    sc.block_visible = Some(before);
                    for conj in conjuncts(&e) {
                        let pred = self.expr(sc, conj, At::Filter { top: true })?;
                        require_bool(&pred, "a filter")?;
                        let blocks = std::mem::take(&mut sc.block_buf);
                        sc.push_filter(at, Step::Filter { blocks, pred });
                    }
                    sc.block_visible = None;
                }
                Pending::Braces { var, prop, value } => {
                    let pred = self.braces(sc, &var, &prop, &value)?;
                    sc.push_filter(
                        at,
                        Step::Filter {
                            blocks: Vec::new(),
                            pred,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn clause(&mut self, sc: &mut Scope, c: &Clause) -> R<()> {
        match c {
            Clause::Binding {
                variable,
                type_name,
                prop_matches,
            } => {
                self.binding(sc, variable, type_name)?;
                for (prop, value) in prop_matches {
                    sc.pending.push(Pending::Braces {
                        var: variable.clone(),
                        prop: prop.clone(),
                        value: value.clone(),
                    });
                }
                Ok(())
            }
            Clause::Traversal(TraversalEq(t)) => self.traversal(sc, t),
            Clause::Filter(e) => {
                let before = sc.visible().into_iter().map(|c| c.name).collect();
                sc.pending.push(Pending::Filter(e.clone(), before));
                Ok(())
            }
            // A block sees only the text before it, so it correlates on the
            // names bound so far and is placed after the last of those it
            // reads.
            Clause::Not(clauses) => {
                let (term, free) = self.block_scope(sc, clauses)?;
                if free.is_empty() {
                    return Err(err(
                        T9,
                        "a `not { }` block must reference an outer-bound variable",
                    ));
                }
                let at = sc.position(&free);
                let col = self.fresh("b");
                let pred = not_exists(&col);
                sc.push_filter(
                    at,
                    Step::Filter {
                        blocks: vec![(term, col, count_rows())],
                        pred,
                    },
                );
                Ok(())
            }
            Clause::Optional(clauses) => self.optional(sc, clauses),
            Clause::Let { name, value } => self.let_clause(sc, name, value),
            Clause::From(rel) => {
                self.flush(sc, false)?;
                let r = self.rel_expr(sc, &RelArg::Rel(rel))?;
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
            // A block re-mentions an outer variable to correlate on it.
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
                    "`{type_name}` is an interface; a binding names a node type (an interface population is the polymorphism stage, Poly), as today"
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
        Err(err(T1, format!("unknown node type `{type_name}`")))
    }

    fn braces(&mut self, sc: &mut Scope, var: &str, prop: &str, value: &Expr) -> R<SExpr> {
        let Some(ty) = sc.col(var) else {
            return Err(err(T6, format!("`${var}` is not bound")));
        };
        let (prop_ty, owner) = self.property(&ty, prop, var)?;
        if prop_ty.scalar == ScalarType::Blob {
            return Err(err(
                T3,
                format!("blob property `{owner}.{prop}` cannot be used in match patterns"),
            ));
        }
        let mut named = Vec::new();
        expr_vars(value, &mut named);
        if let Some(v) = named
            .iter()
            .find(|v| !sc.params.contains_key(*v) || sc.is_bound(v))
        {
            return Err(err(
                T45,
                format!(
                    "the value of `{prop}` in `${var}`'s braces must be a constant; `${v}` is not a parameter"
                ),
            ));
        }
        let field = SExpr::new(
            SKind::Prop {
                col: var.to_string(),
                prop: prop.to_string(),
            },
            Ty::Value(prop_ty.clone()),
        );
        let v = self.expr(sc, value, At::Value)?;
        let v = adapt(v, &Ty::Value(prop_ty.clone()));
        if let Expr::Literal(l) = value {
            let lt = literal_type(l)?;
            // A list property matches a scalar literal by membership.
            if prop_ty.list && !lt.list {
                let mut elem = prop_ty.clone();
                elem.list = false;
                check_literal_type(l, &lt, &elem, prop)?;
                return Ok(SExpr::new(
                    SKind::In {
                        needle: v.boxed(),
                        list: field.boxed(),
                    },
                    Ty::scalar(ScalarType::Bool, false),
                ));
            }
            check_literal_type(l, &lt, &prop_ty, prop)?;
        } else if let Ty::Value(vt) = &v.ty
            && prop_ty.list
            && !vt.list
        {
            let mut elem = prop_ty.clone();
            elem.list = false;
            if !types_compatible(vt, &elem) {
                return Err(err(
                    T3,
                    format!(
                        "property `{prop}` holds {} but got {}",
                        elem.display_name(),
                        vt.display_name()
                    ),
                ));
            }
            return Ok(SExpr::new(
                SKind::In {
                    needle: v.boxed(),
                    list: field.boxed(),
                },
                Ty::scalar(ScalarType::Bool, false),
            ));
        } else if let Ty::Value(vt) = &v.ty
            && !types_compatible(vt, &prop_ty)
        {
            return Err(err(
                T3,
                format!(
                    "property `{prop}` has type {} but got {}",
                    prop_ty.display_name(),
                    vt.display_name()
                ),
            ));
        }
        compare(CompOp::Eq, field, v)
    }

    fn traversal(&mut self, sc: &mut Scope, t: &Traversal) -> R<()> {
        // Anonymous endpoints get fresh names.
        let mut t = t.clone();
        for end in [&mut t.src, &mut t.dst] {
            if end == "_" {
                *end = self.fresh("a");
            }
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
        let mut bindings: HashMap<String, BoundVariable> = HashMap::new();
        for c in sc.visible() {
            match &c.ty {
                Ty::Node { type_name, .. } => {
                    bindings.insert(
                        c.name.clone(),
                        BoundVariable::Node {
                            type_name: type_name.clone(),
                        },
                    );
                }
                Ty::Edge { type_name, .. } => {
                    bindings.insert(
                        c.name.clone(),
                        BoundVariable::Edge {
                            type_names: vec![type_name.clone()],
                        },
                    );
                }
                Ty::Value(_) => {}
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
                "a catalogue edge (`@property`, `@from`, …) is the schema catalogue's (stage S); this release has none",
            ));
        }
        let mut declared_nodes = sc.declared_nodes.clone();
        for c in sc.visible() {
            if matches!(c.ty, Ty::Node { .. }) {
                declared_nodes.insert(c.name.clone());
            }
        }
        if matches!(t.selector, EdgeSelector::Wildcard)
            && (!declared_nodes.contains(&t.src) || !declared_nodes.contains(&t.dst))
        {
            return Err(err(
                T5,
                "wildcard traversal requires explicitly declared source and destination node bindings",
            ));
        }
        let resolved = resolve_traversal(self.catalog, &t, &bindings, &sc.declared)?;
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
                if sc.inner && sc.outer_col(k).is_some() && sc.local(k).is_none() {
                    return Err(err(
                        T68,
                        format!("edge binding `${k}` inside an inner read reuses an outer name"),
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
        let reverse = |sel: &[(String, Direction)]| -> Vec<(String, Direction)> {
            let mut out: Vec<_> = sel
                .iter()
                .map(|(e, d)| {
                    (
                        e.clone(),
                        match d {
                            Direction::Out => Direction::In,
                            Direction::In => Direction::Out,
                            Direction::Both => Direction::Both,
                        },
                    )
                })
                .collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            out
        };
        let mut forward = members.clone();
        forward.sort_by(|a, b| a.0.cmp(&b.0));
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
                reverse(&members),
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
                    reverse(&members),
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
        let semi = both.then(|| self.fresh("t"));
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
            dst_type: to_type.clone(),
            selector: sel,
            min_hops: resolved.min_hops,
            max_hops: resolved.max_hops,
            edge_var: resolved.edge_binding.clone(),
            semi,
        };
        if from_bound {
            sc.push_binder(expand, &binds);
        } else {
            // Neither end bound: an independent pattern, correlated in.
            let leaf = apply(
                Some(Term::Scan {
                    var: from.clone(),
                    ty: Ty::Node {
                        type_name: from_type.clone(),
                        nullable: false,
                    },
                }),
                expand,
                &mut self.fresh,
            );
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

    fn optional(&mut self, sc: &mut Scope, clauses: &[Clause]) -> R<()> {
        self.flush(sc, false)?;
        let before: Vec<String> = sc.visible().iter().map(|c| c.name.clone()).collect();
        let mut inner = Scope::new(sc.params.clone(), sc.visible(), sc.rels.clone());
        inner.inner = false;
        inner.correlates = true;
        self.clauses(&mut inner, clauses)?;
        let new_cols: Vec<Col> = inner
            .cols
            .iter()
            .filter(|c| !before.contains(&c.name))
            .cloned()
            .collect();
        let free = inner.free.clone();
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
        let before_cols = sc.visible();
        sc.push_binder(
            Step::Optional {
                inner: term,
                new_cols: new_cols.clone(),
                before: before_cols,
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
        for (c, shaped) in r.ty.cols.iter().zip(&r.shaped) {
            if !shaped {
                return Err(err_fix(
                    T74,
                    format!(
                        "the column `{}` is not variable-shaped and cannot be drawn into scope",
                        c.name
                    ),
                    format!(
                        "alias it on the inner `return`: `… as {}`",
                        c.name.replace(['.', '@'], "_")
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

    /// A block's inner scope: its term (starting from `Env` when it reads an
    /// outer column) and its free variables.
    fn block_scope(&mut self, sc: &mut Scope, clauses: &[Clause]) -> R<(Term, Vec<String>)> {
        let (t, f, _) = self.block_scope_arg(sc, clauses, None)?;
        Ok((t, f))
    }

    /// A block's inner scope, with its aggregate argument typed against the
    /// block's own bindings.
    fn block_scope_arg(
        &mut self,
        sc: &mut Scope,
        clauses: &[Clause],
        arg: Option<&Expr>,
    ) -> R<(Term, Vec<String>, Option<SExpr>)> {
        let outer = match &sc.block_visible {
            Some(before) => sc
                .visible()
                .into_iter()
                .filter(|c| before.contains(&c.name))
                .collect(),
            None => sc.visible(),
        };
        let mut inner = Scope::new(sc.params.clone(), outer, sc.rels.clone());
        inner.inner = false;
        inner.correlates = true;
        self.clauses(&mut inner, clauses)?;
        self.flush(&mut inner, true)?;
        let arg = match arg {
            Some(a) => Some(self.expr(&mut inner, a, At::Value)?),
            None => None,
        };
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
                let mut inner = Scope::new(sc.params.clone(), sc.visible(), sc.rels.clone());
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

    fn rel_var(&mut self, sc: &mut Scope, v: &str) -> R<Rel> {
        if let Some(r) = sc.rels.get(v).cloned() {
            for f in &r.free {
                sc.col(f);
            }
            return Ok(r);
        }
        if sc.is_bound(v) || sc.params.contains_key(v) {
            return Err(err_fix(
                T69,
                format!("`${v}` is a scalar or a reference; `from` and a ranking read a relation"),
                format!("bind a relation: `let ${v}s = {{ match {{ … }} return {{ … }} }}`"),
            ));
        }
        Err(err(T6, format!("`${v}` names no relation in scope")))
    }

    fn rel_call(&mut self, sc: &mut Scope, c: &Call) -> R<Rel> {
        match c.name.as_str() {
            "union" => self.union(sc, c),
            "distance" | "bm25" | "nearest" => self.ranking(sc, c),
            "rrf" => self.rrf(sc, c),
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
                Ok(r) => arms.push(r),
                Err(E::Pending(n)) => pending = Some(n),
                Err(e) => return Err(e),
            }
        }
        if arms.is_empty() {
            return Err(E::Pending(pending.unwrap()));
        }
        let mut acc = arms.remove(0);
        for (i, arm) in arms.into_iter().enumerate() {
            if arm.ty.cols.len() != acc.ty.cols.len() {
                return Err(err(
                    T73,
                    format!(
                        "union arm {} has {} columns, the first has {}",
                        i + 2,
                        arm.ty.cols.len(),
                        acc.ty.cols.len()
                    ),
                ));
            }
            let mut cols = Vec::new();
            for (j, (a, b)) in acc.ty.cols.iter().zip(&arm.ty.cols).enumerate() {
                if !a.ty.same_base(&b.ty) {
                    return Err(err(
                        T73,
                        format!(
                            "union arm {} column {} (`{}`) is {}, the first arm's `{}` is {}",
                            i + 2,
                            j + 1,
                            b.name,
                            b.ty.display(),
                            a.name,
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
            acc = Rel {
                term: Term::Union {
                    left: acc.term.boxed(),
                    right: arm.term.boxed(),
                },
                ty: RelTy { cols },
                shaped: acc.shaped,
                ranked: None,
                free,
            };
        }
        acc.ranked = None;
        Ok(acc)
    }

    /// `distance`, `bm25` and `nearest`.
    fn ranking(&mut self, sc: &mut Scope, c: &Call) -> R<Rel> {
        let name = c.name.as_str();
        let (positional, admitted): (usize, &[&str]) = match name {
            "nearest" => (3, &["k", "recall"]),
            _ => (3, &[]),
        };
        for (o, _) in &c.options {
            if !admitted.contains(&o.as_str()) {
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
        let pop = self.arg_rel(sc, &c.args[0])?;
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
                    format!("type `{type_name}` has no property `{property}` for `{name}` to rank"),
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
        let (kind, out) = match name {
            "bm25" => (ScoreKind::Bm25, "score"),
            _ => (ScoreKind::Distance, "distance"),
        };
        if pop.ty.get(out).is_some() {
            return Err(err_fix(
                T68,
                format!("`{name}`'s population already has a `{out}` column"),
                format!("alias it on the population's `return`: `${out} as {out}0`"),
            ));
        }
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
                .is_some_and(|n| n.has_full_text_index(&property))
            {
                return Err(err_fix(
                    T27,
                    format!("`bm25` needs a full-text index on `{type_name}.{property}`"),
                    format!("declare `{property}: String @index(fulltext)` on `{type_name}`"),
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
                Ty::Value(t) if t.scalar == ScalarType::String && !t.list => {
                    let embedded = self
                        .catalog
                        .query_node_type(&type_name)
                        .is_some_and(|n| n.embed_sources.contains_key(&property));
                    if !embedded {
                        return Err(err(
                            T15,
                            format!(
                                "a String query needs `@embed` on `{type_name}.{property}`; pass a Vector({dim})"
                            ),
                        ));
                    }
                }
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
        let out_ty = match kind {
            ScoreKind::Bm25 => Ty::scalar(ScalarType::F64, false),
            ScoreKind::Distance => Ty::scalar(ScalarType::F32, false),
        };
        let mut cols = pop.ty.cols.clone();
        cols.push(Col {
            name: out.to_string(),
            ty: out_ty,
        });
        let mut shaped = pop.shaped.clone();
        shaped.push(true);
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
                query: q,
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
                    ScoreQueryId::Vector(q) => ScoreQuery::Vector(q),
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
        if let Expr::Call(c) = e
            && c.name == "terms"
        {
            return self.terms(sc, c);
        }
        let text = self.expr(sc, e, At::Value)?;
        lexical_text(&text)?;
        Ok(LexicalQuery {
            text,
            mode_all: false,
            max_edits: int_lit(0),
        })
    }

    fn terms(&mut self, sc: &mut Scope, c: &Call) -> R<LexicalQuery> {
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
                        format!("`terms` has no option `{other}`; it admits mode, max_edits"),
                    ));
                }
            }
        }
        Ok(LexicalQuery {
            text,
            mode_all,
            max_edits,
        })
    }

    fn rrf(&mut self, sc: &mut Scope, c: &Call) -> R<Rel> {
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
        let mut k = RRF_DEFAULT_K;
        let mut weights = vec![1.0; n];
        for (o, v) in &c.options {
            match (o.as_str(), v) {
                ("k", Expr::Literal(Literal::Integer(x))) if *x >= 1 && *x <= RRF_MAX_K as i128 => {
                    k = *x as u64
                }
                ("k", _) => {
                    return Err(err(
                        T71,
                        "`rrf`'s `k:` is an integer literal in 1..=1000000",
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
                        format!("`rrf` has no option `{other}`; it admits k, weights"),
                    ));
                }
            }
        }
        let target = first_ranked.column.clone();
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
            },
            ty: RelTy {
                cols: vec![
                    Col {
                        name: target.clone(),
                        ty: target_ty.with_nullable(false),
                    },
                    Col {
                        name: "score".into(),
                        ty: Ty::scalar(ScalarType::F64, false),
                    },
                ],
            },
            shaped: vec![true, true],
            ranked: Some(Ranked {
                column: target,
                identity: None,
            }),
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
                    "a write body calls the declared read `{}`; a declared read called from a write is stage W2 (section 11), after the writes RFC",
                    c.name
                ),
            ));
        }
        if let DeclBody::Write(_) = decl.body {
            return Err(err(
                T74,
                format!(
                    "`{}` is a write; a read draws rows only from reads (A7)",
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
        let params = param_types(&decl.params)?;
        let required = decl.params.iter().filter(|p| p.default.is_none()).count();
        if c.args.len() < required || c.args.len() > params.len() {
            return Err(err(
                T7,
                format!(
                    "`{}` takes {} argument{}, got {}",
                    c.name,
                    params.len(),
                    if params.len() == 1 { "" } else { "s" },
                    c.args.len()
                ),
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
                                "`{}`'s parameter `${pname}` is a scalar; relation arguments are refused (the closure table)",
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
                        Ty::Value(t)
                            if types_compatible(t, pty)
                                && (pty.nullable
                                    || !t.nullable
                                    || matches!(v.kind, SKind::Param(_))) => {}
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
                    let d = decl.params[i].default.as_ref().unwrap();
                    let mut lit = d.value.clone();
                    if d.negative {
                        lit = match lit {
                            Literal::Integer(n) => Literal::Integer(-n),
                            Literal::Float(f) => Literal::Float(-f),
                            other => other,
                        };
                    }
                    SExpr::new(SKind::Lit(lit), Ty::Value(pty.clone()))
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
                shaped: vec![true; n],
                ranked: None,
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
        self.frames.push(Frame {
            decl: c.name.clone(),
            params: params.clone(),
            bindings: bindings.clone(),
        });
        let out = self.read_decl(decl);
        self.frames.pop();
        let out = out?;
        let Rel {
            term, ty, shaped, ..
        } = out.rel;
        Ok(Rel {
            term: Term::Call {
                name: c.name.clone(),
                bindings: params
                    .iter()
                    .map(|(p, _)| p.clone())
                    .zip(bindings)
                    .collect(),
                body: term.boxed(),
            },
            ty,
            shaped,
            ranked: None,
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

    fn statement(&mut self, sc: &mut Scope, s: &Stmt) -> R<(Sink, WriteClass)> {
        match s {
            Stmt::Insert {
                type_name,
                assignments,
            } => {
                if crate::catalog::catalogue::is_catalogue_type_name(type_name) {
                    return Err(err(
                        T10,
                        format!(
                            "catalogue type `{type_name}` is read-only; the engine owns its rows"
                        ),
                    ));
                }
                if assignments.is_empty() {
                    return Err(err(T10, "insert requires at least one assignment"));
                }
                dup_assignments(assignments)?;
                if let Some(node) = self.catalog.node_types.get(type_name) {
                    let mut out = Vec::new();
                    for a in assignments {
                        let Some(pt) = node.properties.get(&a.property).cloned() else {
                            return Err(err(
                                T11,
                                format!("type `{type_name}` has no property `{}`", a.property),
                            ));
                        };
                        out.push((
                            a.property.clone(),
                            self.assign(sc, &a.value, &pt, &a.property)?,
                        ));
                    }
                    let assigned: HashSet<&str> =
                        assignments.iter().map(|a| a.property.as_str()).collect();
                    for (p, t) in &node.properties {
                        if t.nullable || assigned.contains(p.as_str()) {
                            continue;
                        }
                        if let Some(e) = node.embed_sources.get(p)
                            && assigned.contains(e.source.as_str())
                        {
                            continue;
                        }
                        return Err(err(
                            T12,
                            format!(
                                "insert for `{type_name}` must provide non-nullable property `{p}`"
                            ),
                        ));
                    }
                    return Ok((
                        Sink::Insert {
                            target: MutationTarget::Node {
                                type_name: type_name.clone(),
                            },
                            assignments: out,
                        },
                        WriteClass::Constructive,
                    ));
                }
                if let Some(edge) = self.catalog.edge_types.get(type_name) {
                    let mut out = Vec::new();
                    let (mut from, mut to) = (false, false);
                    for a in assignments {
                        let v = match a.property.as_str() {
                            end @ ("from" | "to") => {
                                if end == "from" {
                                    from = true
                                } else {
                                    to = true
                                }
                                let endpoint = if end == "from" {
                                    &edge.from_type
                                } else {
                                    &edge.to_type
                                };
                                self.endpoint(sc, &a.value, endpoint, end, type_name)?
                            }
                            p => {
                                let Some(pt) = edge.properties.get(p).cloned() else {
                                    return Err(err(
                                        T11,
                                        format!("type `{type_name}` has no property `{p}`"),
                                    ));
                                };
                                self.assign(sc, &a.value, &pt, p)?
                            }
                        };
                        out.push((a.property.clone(), v));
                    }
                    if !from || !to {
                        return Err(err(
                            T12,
                            format!(
                                "insert for `{type_name}` must provide required endpoint `{}`",
                                if from { "to" } else { "from" }
                            ),
                        ));
                    }
                    for (p, t) in &edge.properties {
                        if !t.nullable && !assignments.iter().any(|a| &a.property == p) {
                            return Err(err(
                                T12,
                                format!(
                                    "insert for `{type_name}` must provide non-nullable property `{p}`"
                                ),
                            ));
                        }
                    }
                    return Ok((
                        Sink::Insert {
                            target: MutationTarget::Edge {
                                type_name: type_name.clone(),
                            },
                            assignments: out,
                        },
                        WriteClass::Constructive,
                    ));
                }
                Err(err(T10, format!("unknown node/edge type `{type_name}`")))
            }
            Stmt::Update {
                variable,
                assignments,
            } => {
                let target = self.statement_target(sc, variable, "update")?;
                let MutationTarget::Node { type_name } = &target else {
                    return Err(err(
                        T16,
                        format!("update of the edge `${variable}` is not supported (as today)"),
                    ));
                };
                if assignments.is_empty() {
                    return Err(err(T10, "update requires at least one assignment"));
                }
                dup_assignments(assignments)?;
                let props = self.node_props(type_name).cloned().unwrap_or_default();
                let mut out = Vec::new();
                for a in assignments {
                    let Some(pt) = props.get(&a.property).cloned() else {
                        return Err(err(
                            T11,
                            format!("type `{type_name}` has no property `{}`", a.property),
                        ));
                    };
                    out.push((
                        a.property.clone(),
                        self.assign(sc, &a.value, &pt, &a.property)?,
                    ));
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
                format!("`{what} ${var}`: `${var}` is not bound by the group's clauses"),
                format!("bind it before the statement: `${var}: <Type> {{ … }}`"),
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
        if let Expr::Literal(l) = value {
            let lt = literal_type(l)?;
            check_literal_type(l, &lt, pt, prop)?;
            let mut want = pt.clone();
            want.nullable = false;
            return Ok(AssignValue::Expr(SExpr::new(
                SKind::Lit(l.clone()),
                Ty::Value(want),
            )));
        }
        let v = self.expr(sc, value, At::Value)?;
        if !sc.block_buf.is_empty() {
            return Err(err(
                T44,
                "a block is not an assignment value; bind it with `let` in the clauses",
            ));
        }
        let v = adapt(v, &Ty::Value(pt.clone()));
        let ok = match &v.ty {
            Ty::Value(t) => {
                assignment_compatible(t, pt)
                    && (!t.scalar.is_numeric() || numeric_fits(t.scalar, pt.scalar))
                    && (pt.nullable || !t.nullable || matches!(v.kind, SKind::Param(_)))
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
        Ok(AssignValue::Expr(v))
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
                Ty::Node { type_name, .. } if type_name == endpoint => {
                    Ok(AssignValue::Expr(SExpr::new(
                        SKind::Col(v.clone()),
                        Ty::Node {
                            type_name,
                            nullable: false,
                        },
                    )))
                }
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
                Ok(AssignValue::Expr(v))
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
                    return Err(err(
                        T6,
                        format!("type `{type_name}` has no property `{prop}`"),
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
                    return Err(err(
                        T6,
                        format!("edge `{type_name}` has no property `{prop}`"),
                    ));
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

    fn expr(&mut self, sc: &mut Scope, e: &Expr, at: At) -> R<SExpr> {
        let top = matches!(at, At::Filter { top: true });
        let sub = if matches!(at, At::Filter { .. }) {
            At::Filter { top: false }
        } else {
            at
        };
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
            Expr::Literal(Literal::Null) => {
                return Err(err(
                    T7,
                    "`null` is not a value in a read; test it with `is null`",
                ));
            }
            Expr::Literal(l) => SExpr::new(SKind::Lit(l.clone()), Ty::Value(literal_type(l)?)),
            Expr::Variable(v) => {
                if let Some(ty) = sc.col(v) {
                    SExpr::new(SKind::Col(v.clone()), ty)
                } else if let Some(t) = sc.params.get(v) {
                    SExpr::new(SKind::Param(v.clone()), Ty::Value(t.clone()))
                } else if sc.rels.contains_key(v) {
                    return Err(err_fix(
                        T69,
                        format!("`${v}` is a relation; a scalar is read here"),
                        format!(
                            "draw it with `from ${v}`, or aggregate over it: `count {{ from ${v} }}`"
                        ),
                    ));
                } else {
                    return Err(err(T6, format!("variable `${v}` is not bound")));
                }
            }
            Expr::Prop { variable, property } => {
                let Some(ty) = sc.col(variable) else {
                    if sc.params.contains_key(variable) {
                        return Err(err(
                            T6,
                            format!("`${variable}` is a parameter and has no properties"),
                        ));
                    }
                    return Err(err(T6, format!("variable `${variable}` is not bound")));
                };
                if let Some(field) = property.strip_prefix('@') {
                    let admitted = match &ty {
                        Ty::Node { .. } => field == "id",
                        Ty::Edge { .. } => matches!(field, "id" | "src" | "dst" | "type"),
                        Ty::Value(_) => false,
                    };
                    if !admitted {
                        return Err(err(
                            T6,
                            format!("`${variable}` has no meta-field `{property}`"),
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
                            format!("Blob property `{variable}.{property}` is not a read value"),
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
            Expr::Call(c) => return self.call_scalar(sc, c, at, top),
            Expr::AggDistinct { func, .. } => {
                return Err(err(
                    T8,
                    format!("`{func}(distinct …)` is an aggregate; it belongs in `return`"),
                ));
            }
            Expr::Block(b) => {
                let (term, free, arg) = self.block_scope_arg(sc, &b.clauses, b.arg.as_deref())?;
                if free.is_empty() {
                    return Err(err(
                        T39,
                        "an exists or aggregate block must reference an outer-bound variable",
                    ));
                }
                let col = self.fresh("b");
                let (agg, value) = match b.func {
                    BlockFunc::Exists => (count_rows(), exists_col(&col)),
                    BlockFunc::Agg(func) => {
                        let agg = match (arg, func) {
                            (None, AggFunc::Count) => count_rows(),
                            (None, f) => {
                                return Err(err(
                                    T40,
                                    format!("`{f} {{ }}` needs its argument: `{f}($x.p) {{ … }}`"),
                                ));
                            }
                            (Some(arg), f) => {
                                let ty = agg_type(f, &arg.ty)?;
                                let ty = ty.with_nullable(f != AggFunc::Count);
                                Agg {
                                    func: f,
                                    distinct: false,
                                    arg: Some(arg),
                                    ty,
                                }
                            }
                        };
                        let ty = agg.ty.clone();
                        (agg, SExpr::new(SKind::Col(col.clone()), ty))
                    }
                };
                sc.block_buf.push((term, col.clone(), agg));
                // The block's column is visible to the expression that reads it.
                value
            }
            Expr::Neg(inner) => {
                let v = self.expr(sc, inner, sub)?;
                let ty = numeric(&v, "unary `-`")?;
                SExpr::new(SKind::Neg(v.boxed()), ty)
            }
            Expr::Arith { op, left, right } => {
                let l = self.expr(sc, left, sub)?;
                let r = self.expr(sc, right, sub)?;
                arith(*op, l, r)?
            }
            Expr::Compare { op, left, right } => {
                let l = self.expr(sc, left, sub)?;
                let r = self.expr(sc, right, sub)?;
                compare(*op, l, r)?
            }
            Expr::In {
                needle,
                list,
                at: written,
            } => {
                // `$d in $x` over a node is a traversal over the edge named
                // `in`, whose bare name is reserved; refused before the
                // needle is typed, since a traversal would bind it.
                if let (Expr::Variable(a), Expr::Variable(b)) = (needle.as_ref(), list.as_ref()) {
                    if matches!(sc.col(b), Some(Ty::Node { .. })) {
                        let replacement = format!("${a} \"in\" ${b}");
                        let mut d = QueryDiagnostic::typecheck(
                            T7,
                            format!(
                                "`${b}` is a node, so `${a} in ${b}` is a traversal over the edge named `in`, whose bare name is reserved"
                            ),
                        )
                        .with_fix(replacement.clone())
                        .with_suggestion(Suggestion {
                            applicability: Applicability::MachineApplicable,
                            edits: vec![TextEdit {
                                start: written.start,
                                end: written.end,
                                replacement,
                            }],
                        });
                        d.position = Some(written.position);
                        return Err(E::D(d));
                    }
                }
                let n = self.expr(sc, needle, sub)?;
                let l = self.expr(sc, list, sub)?;
                let l = match &n.ty {
                    Ty::Value(t) if !t.list => {
                        adapt(l, &Ty::Value(PropType::list_of(t.scalar, false)))
                    }
                    _ => l,
                };
                let n = adapt(n, &l.ty.clone());
                match &l.ty {
                    Ty::Node { .. } => return Err(err(T7, "`in` reads a list, not a node")),
                    Ty::Value(t) if !t.list => {
                        return Err(err_fix(
                            T7,
                            format!("`in` reads a list; the right side is {}", t.display_name()),
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
                                        "`in` over {} needs a {} needle, got {}",
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
                let v = self.expr(sc, inner, sub)?;
                require_bool(&v, "`not`")?;
                let ty = v.ty.clone();
                SExpr::new(SKind::Not(v.boxed()), ty)
            }
            Expr::And(l, r) | Expr::Or(l, r) => {
                let a = self.expr(sc, l, sub)?;
                let b = self.expr(sc, r, sub)?;
                require_bool(&a, "`and`/`or`")?;
                require_bool(&b, "`and`/`or`")?;
                let ty = Ty::scalar(ScalarType::Bool, a.ty.nullable() || b.ty.nullable());
                if matches!(e, Expr::And(..)) {
                    SExpr::new(SKind::And(a.boxed(), b.boxed()), ty)
                } else {
                    SExpr::new(SKind::Or(a.boxed(), b.boxed()), ty)
                }
            }
        })
    }

    fn call_scalar(&mut self, sc: &mut Scope, c: &Call, at: At, top: bool) -> R<SExpr> {
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
        if AGGREGATES.contains(&name) {
            return Err(err(
                T8,
                format!(
                    "`{name}(…)` is an aggregate; it belongs in `return`, or as a block `{name}($x.p) {{ … }}`"
                ),
            ));
        }
        let mut args = Vec::new();
        for a in &c.args {
            match a {
                Arg::Expr(e) => {
                    if matches!(e, Expr::Call(t) if t.name == "terms")
                        && matches!(name, "match_terms")
                    {
                        continue;
                    }
                    args.push(self.expr(sc, e, At::Value)?)
                }
                Arg::Inline(_) => {
                    return Err(err(
                        T69,
                        format!("`{name}` reads scalars; an inline read is a relation"),
                    ));
                }
            }
        }
        let bool_ty = |nullable| Ty::scalar(ScalarType::Bool, nullable);
        let string =
            |t: &Ty| matches!(t, Ty::Value(p) if p.scalar == ScalarType::String && !p.list);
        let no_options = |c: &Call| -> R<()> {
            match c.options.first() {
                Some((o, _)) => Err(err(T71, format!("`{}` has no option `{o}`", c.name))),
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
                        .is_some_and(|n| n.has_full_text_index(prop))
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
            "search" | "match_text" | "fuzzy" => {
                if !top {
                    return Err(err(
                        T38,
                        format!(
                            "`{name}` is a search predicate; it must stand alone as a filter clause"
                        ),
                    ));
                }
                if args.len() != 2 {
                    return Err(err(
                        T71,
                        format!(
                            "`{name}` takes a field and a query; an edit budget is the option `max_edits:`"
                        ),
                    ));
                }
                search_field(self, &args)?;
                if !string(&args[1].ty) {
                    return Err(err(T19, format!("`{name}`'s query is a String")));
                }
                let mut options = Vec::new();
                for (o, v) in &c.options {
                    if name == "fuzzy" && o == "max_edits" {
                        if let Expr::Literal(Literal::Integer(n)) = v
                            && !(0..=2).contains(n)
                        {
                            return Err(err(T71, format!("`max_edits: {n}` is outside 0..=2")));
                        }
                        options.push((o.clone(), self.expr(sc, v, At::Value)?));
                    } else {
                        return Err(err(T71, format!("`{name}` has no option `{o}`")));
                    }
                }
                if name == "fuzzy" && options.is_empty() {
                    // `fuzzy(f, q)` with no budget is two edits, as today.
                    options.push(("max_edits".into(), int_lit(2)));
                }
                SExpr::new(
                    SKind::Call {
                        func: name.into(),
                        args,
                        options,
                    },
                    bool_ty(false),
                )
            }
            "match_terms" => {
                if !top {
                    return Err(err(
                        T38,
                        "`match_terms` is a predicate; it must stand alone as a filter clause",
                    ));
                }
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
                let [cnd, a, b] = args.as_slice() else {
                    return Err(err(T7, "`if(c, a, b)` takes three arguments"));
                };
                require_bool(cnd, "`if`'s condition")?;
                let ty = common(a, b, "if")?;
                let (a, b) = (adapt(a.clone(), &ty), adapt(b.clone(), &ty));
                SExpr::new(
                    SKind::Call {
                        func: "if".into(),
                        args: vec![cnd.clone(), a, b],
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
                let mut ty = first.ty.clone();
                for a in &args[1..] {
                    ty = common(&SExpr::new(SKind::Null, ty.clone()), a, "coalesce")?;
                }
                let all_null = args.iter().all(|a| a.ty.nullable());
                let ty = ty.with_nullable(all_null);
                let args = args.into_iter().map(|a| adapt(a, &ty)).collect();
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
            other => {
                return Err(err(
                    T6,
                    format!("`{other}` is not a registry row or a declared read"),
                ));
            }
        };
        let _ = at;
        Ok(out)
    }
}

#[derive(Clone, Copy)]
enum At {
    Filter { top: bool },
    Value,
}

enum RelArg<'a> {
    Rel(&'a RelExpr),
    Inline(&'a ReadBody),
    Expr(&'a Expr),
}

// ---------------------------------------------------------------------------
// Folding steps into terms.

fn apply(acc: Option<Term>, step: Step, _fresh: &mut u32) -> Term {
    let correlate = |acc: Option<Term>, t: Term| match acc {
        None => t,
        Some(a) => Term::Correlate {
            outer: a.boxed(),
            inner: t.boxed(),
        },
    };
    match step {
        Step::Leaf(t) => correlate(acc, t),
        Step::Expand {
            src,
            dst,
            dst_type,
            selector,
            min_hops,
            max_hops,
            edge_var,
            semi,
        } => {
            let target = semi.clone().unwrap_or_else(|| dst.clone());
            let e = Term::Expand {
                input: acc.unwrap_or(Term::Unit).boxed(),
                src,
                dst: target.clone(),
                dst_type: dst_type.clone(),
                selector,
                min_hops,
                max_hops,
                edge_var,
            };
            match semi {
                None => e,
                Some(t) => Term::Filter {
                    input: e.boxed(),
                    pred: SExpr::new(
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
                    ),
                },
            }
        }
        Step::Filter { blocks, pred } => {
            let mut t = acc.unwrap_or(Term::Unit);
            for (inner, col, agg) in blocks {
                t = Term::Correlate {
                    outer: t.boxed(),
                    inner: Term::Group {
                        input: inner.boxed(),
                        keys: Vec::new(),
                        aggs: vec![(col, agg)],
                    }
                    .boxed(),
                };
            }
            if matches!(pred.kind, SKind::Lit(Literal::Bool(true))) {
                return t;
            }
            Term::Filter {
                input: t.boxed(),
                pred,
            }
        }
        Step::Let {
            blocks,
            name,
            value,
        } => {
            let mut t = acc.unwrap_or(Term::Unit);
            for (inner, col, agg) in blocks {
                t = Term::Correlate {
                    outer: t.boxed(),
                    inner: Term::Group {
                        input: inner.boxed(),
                        keys: Vec::new(),
                        aggs: vec![(col, agg)],
                    }
                    .boxed(),
                };
            }
            Term::Project {
                input: t.boxed(),
                items: vec![(name, value)],
                keep: true,
            }
        }
        Step::Optional {
            inner,
            new_cols,
            before,
        } => {
            // Union(Correlate(R, p), Project(Filter(R, ¬ exists p), nulls)).
            let r = acc.unwrap_or(Term::Unit);
            let matched = Term::Correlate {
                outer: r.clone().boxed(),
                inner: inner.clone().boxed(),
            };
            let col = "_opt".to_string();
            let unmatched = Term::Filter {
                input: Term::Correlate {
                    outer: r.clone().boxed(),
                    inner: Term::Group {
                        input: inner.boxed(),
                        keys: Vec::new(),
                        aggs: vec![(col.clone(), count_rows())],
                    }
                    .boxed(),
                }
                .boxed(),
                pred: not_exists(&col),
            };
            let mut items = Vec::new();
            for c in output_columns(&r) {
                // A hidden column (a block's count) has no declared type here;
                // every one is an `I64` count or a reference.
                let ty = before
                    .iter()
                    .find(|b| b.name == c)
                    .map_or(Ty::scalar(ScalarType::I64, false), |b| b.ty.clone());
                items.push((c.clone(), SExpr::new(SKind::Col(c), ty)));
            }
            for c in &new_cols {
                items.push((
                    c.name.clone(),
                    SExpr::new(SKind::Null, c.ty.with_nullable(true)),
                ));
            }
            Term::Union {
                left: matched.boxed(),
                right: Term::Project {
                    input: unmatched.boxed(),
                    items,
                    keep: false,
                }
                .boxed(),
            }
        }
    }
}

/// The column names a term produces (types are not needed by the fold).
fn output_columns(t: &Term) -> Vec<String> {
    match t {
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
        Term::Project { input, items, keep } => {
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
        Term::Correlate { outer, inner } => {
            let mut v = output_columns(outer);
            for c in output_columns(inner) {
                if !v.contains(&c) {
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
        Term::Fuse { target, .. } => vec![target.clone(), "score".into()],
        Term::Fix { body, .. } | Term::Call { body, .. } => output_columns(body),
        Term::RecRef { ty, .. } => ty.names(),
    }
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
            if reads_any(k, &computed)
                || !k.is_reference_read() && !matches!(k.kind, SKind::Param(_))
            {
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
    match t {
        Term::RecRef { name: n, .. } => n == name,
        Term::Unit | Term::Env(_) | Term::Scan { .. } => false,
        Term::Expand { input, .. }
        | Term::Filter { input, .. }
        | Term::Project { input, .. }
        | Term::Group { input, .. }
        | Term::Order { input, .. }
        | Term::Cut { input, .. }
        | Term::Score { input, .. }
        | Term::ApproxTopK { input, .. } => depends(input, name),
        Term::Correlate { outer, inner } => depends(outer, name) || depends(inner, name),
        Term::Union { left, right } => depends(left, name) || depends(right, name),
        Term::Fuse { arms, .. } => arms.iter().any(|a| depends(a, name)),
        Term::Fix { body, .. } | Term::Call { body, .. } => depends(body, name),
    }
}

fn reads_any(e: &SExpr, cols: &HashSet<String>) -> bool {
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
        Ok(match t {
            Term::RecRef { .. } => HashSet::new(),
            Term::Unit | Term::Env(_) | Term::Scan { .. } => HashSet::new(),
            Term::Expand { input, .. } => self.term(input)?,
            Term::Filter { input, pred } => {
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
                    let computed = !(e.is_reference_read() || matches!(e.kind, SKind::Param(_)))
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
            Term::Correlate { outer, inner } => {
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
                // Positional: the right arm's computed columns are the left
                // arm's names.
                let ln = output_columns(left);
                let rn = output_columns(right);
                for (i, n) in rn.iter().enumerate() {
                    if r.contains(n)
                        && let Some(l) = ln.get(i)
                    {
                        c.insert(l.clone());
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
    }
}

/// `col > 0` or `col >= 1`: the `exists` reading of a block count.
fn is_exists_of(pred: &SExpr, col: &str) -> bool {
    let SKind::Cmp { op, left, right } = &pred.kind else {
        return false;
    };
    matches!(&left.kind, SKind::Col(c) if c == col)
        && matches!(
            (op, &right.kind),
            (CompOp::Gt, SKind::Lit(Literal::Integer(0)))
                | (CompOp::Ge, SKind::Lit(Literal::Integer(1)))
        )
}

fn call_chain(t: &Term, out: &mut Vec<String>) {
    match t {
        Term::Call { name, body, .. } => {
            if !out.contains(name) {
                out.push(name.clone());
            }
            call_chain(body, out);
        }
        Term::Unit | Term::Env(_) | Term::Scan { .. } | Term::RecRef { .. } => {}
        Term::Expand { input, .. }
        | Term::Filter { input, .. }
        | Term::Project { input, .. }
        | Term::Group { input, .. }
        | Term::Order { input, .. }
        | Term::Cut { input, .. }
        | Term::Score { input, .. }
        | Term::ApproxTopK { input, .. } => call_chain(input, out),
        Term::Correlate { outer, inner } => {
            call_chain(outer, out);
            call_chain(inner, out);
        }
        Term::Union { left, right } => {
            call_chain(left, out);
            call_chain(right, out);
        }
        Term::Fuse { arms, .. } => arms.iter().for_each(|a| call_chain(a, out)),
        Term::Fix { body, .. } => call_chain(body, out),
    }
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
        _ => AggFunc::Max,
    }
}

/// An aggregate's result type: `count` is a non-null `I64`; `sum` and `avg`
/// are `F64` as today; `min` and `max` keep the argument's type; all but
/// `count` are nullable over an empty input.
fn agg_type(func: AggFunc, arg: &Ty) -> R<Ty> {
    match (func, arg) {
        (AggFunc::Count, _) => Ok(Ty::scalar(ScalarType::I64, false)),
        (_, Ty::Node { .. } | Ty::Edge { .. }) => Err(err(
            T8,
            format!("{func} cannot take a reference bare; access one of its properties"),
        )),
        (AggFunc::Sum | AggFunc::Avg, Ty::Value(t)) if t.list || !t.scalar.is_numeric() => {
            Err(err(
                T8,
                format!("{func} requires numeric type, got {}", t.display_name()),
            ))
        }
        (AggFunc::Sum | AggFunc::Avg, Ty::Value(_)) => Ok(Ty::scalar(ScalarType::F64, true)),
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

/// An integer literal takes the integer type of the operand it meets; a
/// float literal stays `F64` (the RFC's rules leave literals unstated; this
/// is edition 2's contextual literal typing).
fn adapt(e: SExpr, other: &Ty) -> SExpr {
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
/// is nullable when either operand is.
fn arith(op: ArithOp, l: SExpr, r: SExpr) -> R<SExpr> {
    let l = adapt(l, &r.ty);
    let r = adapt(r, &l.ty);
    let lt = numeric(&l, &format!("`{}`'s left operand", op.symbol()))?;
    let rt = numeric(&r, &format!("`{}`'s right operand", op.symbol()))?;
    let (Ty::Value(a), Ty::Value(b)) = (&lt, &rt) else {
        unreachable!()
    };
    let float = |s| matches!(s, ScalarType::F32 | ScalarType::F64);
    let signed = |s| matches!(s, ScalarType::I32 | ScalarType::I64);
    let scalar = if a.scalar == b.scalar {
        a.scalar
    } else if float(a.scalar) || float(b.scalar) {
        if a.scalar == ScalarType::F32 && b.scalar == ScalarType::F32 {
            ScalarType::F32
        } else {
            ScalarType::F64
        }
    } else if signed(a.scalar) == signed(b.scalar) {
        if width(a.scalar) >= width(b.scalar) {
            a.scalar
        } else {
            b.scalar
        }
    } else if matches!(a.scalar, ScalarType::U64) || matches!(b.scalar, ScalarType::U64) {
        return Err(err(
            T72,
            format!(
                "`{}` mixes U64 with a signed integer; the RFC states no common type",
                op.symbol()
            ),
        ));
    } else {
        ScalarType::I64
    };
    let ty = Ty::scalar(scalar, a.nullable || b.nullable);
    let cast = |e: SExpr| {
        if matches!(&e.ty, Ty::Value(t) if t.scalar != scalar) && !matches!(e.kind, SKind::Lit(_)) {
            SExpr::new(SKind::Cast(e.boxed()), Ty::scalar(scalar, false))
        } else {
            e
        }
    };
    Ok(SExpr::new(
        SKind::Arith {
            op,
            left: cast(l).boxed(),
            right: cast(r).boxed(),
        },
        ty,
    ))
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
            if a != b {
                return Err(err(
                    T7,
                    format!("`{a}` and `{b}` references are never equal"),
                ));
            }
            (l, r)
        }
        (Ty::Value(a), Ty::Value(b)) => {
            if matches!(op, CompOp::Contains | CompOp::StartsWith) {
                return Err(err(T7, "infix string operators are calls in edition 3"));
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

/// The common type of two branches (`if`, `coalesce`).
fn common(a: &SExpr, b: &SExpr, what: &str) -> R<Ty> {
    let a2 = adapt(a.clone(), &b.ty);
    let b2 = adapt(b.clone(), &a2.ty);
    let nullable = a2.ty.nullable() || b2.ty.nullable();
    match (&a2.ty, &b2.ty) {
        (Ty::Value(x), Ty::Value(y)) if x.scalar == y.scalar && x.list == y.list => {
            Ok(a2.ty.with_nullable(nullable))
        }
        (Ty::Value(x), Ty::Value(y))
            if x.scalar.is_numeric() && y.scalar.is_numeric() && !x.list && !y.list =>
        {
            let float = |s| matches!(s, ScalarType::F32 | ScalarType::F64);
            let s = if float(x.scalar) || float(y.scalar) {
                ScalarType::F64
            } else if width(x.scalar) >= width(y.scalar) {
                x.scalar
            } else {
                y.scalar
            };
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
    match e {
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
    }
}

/// The conjuncts of a top-level `and` (the formatter splits them into
/// clauses without changing the term, so the lowering splits them too).
fn conjuncts(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::And(l, r) => {
            let mut v = conjuncts(l);
            v.extend(conjuncts(r));
            v
        }
        other => vec![other],
    }
}

/// The name a return item produces (edition 2's executed spelling).
fn item_name(e: &Expr, alias: Option<&str>) -> String {
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
        Expr::Call(c) if c.name == "now" => crate::query::ast::NOW_PARAM_NAME.to_string(),
        Expr::Call(c) => c.name.clone(),
        _ => "expression".into(),
    }
}

fn is_relation_expr(e: &Expr, sc: &Scope, unit: &HashMap<String, &Decl>) -> bool {
    match e {
        Expr::Variable(v) => sc.rels.contains_key(v),
        Expr::Call(c) => RELATION_ROWS.contains(&c.name.as_str()) || unit.contains_key(&c.name),
        _ => false,
    }
}

/// Lookahead over a scope's clauses: the type each binding declares, and
/// every name the scope will bind.
fn lookahead(clauses: &[Clause], sc: &mut Scope) {
    for c in clauses {
        match c {
            Clause::Binding {
                variable,
                type_name,
                ..
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
            Clause::Traversal(TraversalEq(t)) => {
                sc.future.insert(t.src.clone());
                sc.future.insert(t.dst.clone());
                if let Some(k) = &t.edge_binding {
                    sc.future.insert(k.clone());
                }
            }
            Clause::Let { name, .. } => {
                sc.future.insert(name.clone());
            }
            Clause::From(RelExpr::Inline(body)) => {
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
            Clause::Optional(inner) => {
                let mut tmp = Scope::new(HashMap::new(), Vec::new(), HashMap::new());
                lookahead(inner, &mut tmp);
                sc.future.extend(tmp.future);
            }
            Clause::From(_) | Clause::Filter(_) | Clause::Not(_) => {}
        }
    }
}

/// The scope variables a pending filter names: the variables it mentions
/// that the scope binds or can see, a block's own bindings excluded.
fn pending_vars(p: &Pending, sc: &Scope) -> Vec<String> {
    let mut out = Vec::new();
    match p {
        Pending::Filter(e, before) => filter_vars(e, before, &mut out),
        Pending::Braces { var, value, .. } => {
            out.push(var.clone());
            expr_vars(value, &mut out);
        }
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
fn filter_vars(e: &Expr, before: &[String], out: &mut Vec<String>) {
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
    match e {
        Expr::Block(b) => {
            clauses_vars(&b.clauses, out);
            if let Some(a) = &b.arg {
                expr_vars(a, out);
            }
        }
        other => for_each_child(other, &mut |child| block_vars(child, out)),
    }
}

/// The variables `e` names outside its blocks.
fn expr_vars_outside_blocks(e: &Expr, out: &mut Vec<String>) {
    match e {
        Expr::Block(_) => {}
        Expr::Variable(v) | Expr::Prop { variable: v, .. } => out.push(v.clone()),
        other => for_each_child(other, &mut |child| expr_vars_outside_blocks(child, out)),
    }
}

/// Every direct operand of an expression that is not a block.
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
        Expr::AggDistinct { arg, .. } | Expr::Neg(arg) | Expr::Not(arg) => f(arg),
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
    }
}

fn clauses_vars(clauses: &[Clause], out: &mut Vec<String>) {
    for c in clauses {
        match c {
            Clause::Binding {
                variable,
                prop_matches,
                ..
            } => {
                out.push(variable.clone());
                for (_, v) in prop_matches {
                    expr_vars(v, out);
                }
            }
            Clause::Traversal(TraversalEq(t)) => {
                out.push(t.src.clone());
                out.push(t.dst.clone());
            }
            Clause::Filter(e) => expr_vars(e, out),
            Clause::Not(c) | Clause::Optional(c) => clauses_vars(c, out),
            Clause::Let { value, .. } => {
                if let LetValue::Expr(e) = value {
                    expr_vars(e, out)
                }
            }
            Clause::From(_) => {}
        }
    }
}

fn expr_vars(e: &Expr, out: &mut Vec<String>) {
    match e {
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
        Expr::AggDistinct { arg, .. } | Expr::Neg(arg) | Expr::Not(arg) => expr_vars(arg, out),
        Expr::IsNull { expr, .. } => expr_vars(expr, out),
        Expr::Block(b) => {
            clauses_vars(&b.clauses, out);
            if let Some(a) = &b.arg {
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
    }
}

#[cfg(test)]
mod tests;
