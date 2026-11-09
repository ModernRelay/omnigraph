//! The edition-3 front end (docs/rfcs/2026-11-07-gq-3.md, sections 5, 6 and
//! 7).
//!
//! [`parse_query3`] parses a source that opens with `gq 3;` into the
//! edition-3 tree ([`super::ast3`]). Every refusal is a [`Refusal3`]: a
//! diagnostic whose `suggestion` holds byte-range edits of the original
//! source and whose `fix` is derived from them (the construct the diagnostic
//! points at, with the edits applied), plus the construct's span. The
//! recognizers of the grammar (rules ending `_rec`, `arrow_traversal`,
//! `where_stmt`, …) are refused here with their edits; a lexical pass over a
//! refused source ([`super::habits3`]) adds the habits a grammar rule cannot
//! hold (single quotes, `<>`, `==`, `&&`, `!`, uppercase keywords, `?x`,
//! `--` and `#` comments) and composes their edits with the structural
//! refusal of the source they produce.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::Arc;

use pest::Parser;
use pest::error::{ErrorVariant, InputLocation};
use pest::iterators::Pair;

use crate::error::CompilerError;

use super::ast::{
    AggFunc, CompOp, EdgeSelector, Edition3Decl, FileBody, Literal, MutationWriteMode, Param,
    QueryDecl, QueryFile, Traversal,
};
use super::ast3::*;
use super::codes::{
    Q001, Q002, Q004, Q005, Q006, Q007, Q008, Q009, Q010, Q011, Q012, Q013, Q014, Q015, Q016, Q017,
    Q018, Q019, Q020, Q021,
};
use super::diagnostic::{
    Applicability, Position, QueryCode, QueryDiagnostic, Suggestion, TextEdit,
};
use super::habits3;
use super::parser::{
    QueryParser, Rule, compiler_error_to_diagnostic, diagnostic_at, parse_branch_stmt,
    parse_commit_stmt, parse_edge_name, parse_load_stmt, parse_setting_stmt, parse_setting_target,
    parse_string_lit, parse_traversal_bounds, parse_unattached_base, position_of, skip_trivia,
};

/// A refusal: the diagnostic an agent reads, and the span of the construct it
/// points at (the span the derived `fix` text replaces).
#[derive(Debug, Clone)]
pub struct Refusal3 {
    pub diagnostic: QueryDiagnostic,
    pub span: Option<(usize, usize)>,
}

impl From<QueryDiagnostic> for Refusal3 {
    fn from(diagnostic: QueryDiagnostic) -> Self {
        Self {
            diagnostic,
            span: None,
        }
    }
}

impl Refusal3 {
    pub fn edits(&self) -> &[TextEdit] {
        self.diagnostic
            .suggestion
            .as_ref()
            .map_or(&[], |s| s.edits.as_slice())
    }
}

type R<T> = std::result::Result<T, Refusal3>;

const MACHINE: Applicability = Applicability::MachineApplicable;
const REVIEW: Applicability = Applicability::NeedsReview;

/// A refusal with edits; `fix` is the construct `span` with the edits that
/// fall inside it applied, followed by each edit outside it.
pub(crate) fn refusal(
    input: &str,
    code: QueryCode,
    message: impl Into<String>,
    span: (usize, usize),
    mut edits: Vec<TextEdit>,
    applicability: Applicability,
) -> Refusal3 {
    let mut d = QueryDiagnostic::parse(code, message, Some(Position::at(input, span.0)));
    // A pest span may end after implicit whitespace; the construct ends at
    // its last token, and covers every edit that touches it.
    let mut span = (span.0, input[..span.1].trim_end().len().max(span.0));
    for e in &edits {
        if e.start <= span.1 && span.0 <= e.end {
            span = (span.0.min(e.start), span.1.max(e.end));
        }
    }
    if !edits.is_empty() {
        edits.sort_by_key(|e| (e.start, e.end));
        d = d
            .with_fix(derive_fix(input, span, &edits))
            .with_suggestion(Suggestion {
                applicability,
                edits,
            });
    }
    Refusal3 {
        diagnostic: d,
        span: Some(span),
    }
}

/// A refusal with no mechanical fix: the note says what to write instead.
pub(crate) fn refusal_note(
    input: &str,
    code: QueryCode,
    message: impl Into<String>,
    span: (usize, usize),
    note: impl Into<String>,
) -> Refusal3 {
    Refusal3 {
        diagnostic: QueryDiagnostic::parse(code, message, Some(Position::at(input, span.0)))
            .with_fix(note),
        span: Some(span),
    }
}

/// The construct's text with the edits inside it applied; an edit outside it
/// is printed after it.
pub(crate) fn derive_fix(input: &str, span: (usize, usize), edits: &[TextEdit]) -> String {
    let (inside, outside): (Vec<&TextEdit>, Vec<&TextEdit>) = edits
        .iter()
        .partition(|e| e.start >= span.0 && e.end <= span.1);
    let mut text = input[span.0..span.1].to_string();
    let mut sorted = inside;
    sorted.sort_by_key(|e| std::cmp::Reverse(e.start));
    for e in sorted {
        text.replace_range(e.start - span.0..e.end - span.0, &e.replacement);
    }
    if text.trim().is_empty() {
        text = format!("remove `{}`", input[span.0..span.1].trim());
    }
    for e in outside {
        text.push_str(&format!(
            "; and `{}` for `{}`",
            e.replacement,
            &input[e.start..e.end]
        ));
    }
    text
}

/// Apply non-overlapping edits to `input`.
pub fn apply_edits(input: &str, edits: &[TextEdit]) -> String {
    let mut out = input.to_string();
    let mut sorted: Vec<&TextEdit> = edits.iter().collect();
    sorted.sort_by_key(|e| std::cmp::Reverse((e.start, e.end)));
    for e in sorted {
        out.replace_range(e.start..e.end, &e.replacement);
    }
    out
}

fn edit(start: usize, end: usize, replacement: impl Into<String>) -> TextEdit {
    TextEdit {
        start,
        end,
        replacement: replacement.into(),
    }
}

fn span_of(p: &Pair<Rule>) -> (usize, usize) {
    (p.as_span().start(), p.as_span().end())
}

fn input_of<'i>(p: &Pair<'i, Rule>) -> &'i str {
    p.as_span().get_input()
}

/// `[start, end)` widened over the whitespace that follows it.
fn with_trailing_ws(input: &str, start: usize, end: usize) -> (usize, usize) {
    let rest = &input[end..];
    let ws = rest.len() - rest.trim_start_matches([' ', '\t', '\r', '\n']).len();
    (start, end + ws)
}

/// The per-declaration facts a fix needs: the parameter names, the node and
/// edge bindings in written order, every variable name used, and whether a
/// bare name is admitted (inside an edition-2 `where` predicate only).
#[derive(Default)]
struct Ctx {
    params: Vec<String>,
    bindings: Vec<(String, String)>,
    vars: BTreeSet<String>,
    allow_bare: bool,
    /// The expression being built is a whole `return` item or a whole
    /// clause starting at this byte: where a `$n -1` may be meant as two.
    top: Option<(Top, usize)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Top {
    Item,
    Clause,
}

fn at_top<T>(top: Top, start: usize, f: impl FnOnce() -> T) -> T {
    let saved = with_ctx(|c| c.top.replace((top, start)));
    let out = f();
    with_ctx(|c| c.top = saved);
    out
}

thread_local! {
    static CTX: RefCell<Ctx> = RefCell::new(Ctx::default());
}

fn with_ctx<T>(f: impl FnOnce(&mut Ctx) -> T) -> T {
    CTX.with(|c| f(&mut c.borrow_mut()))
}

fn first_binding() -> Option<String> {
    with_ctx(|c| c.bindings.first().map(|(v, _)| v.clone()))
}

fn binding_of_type(ty: &str) -> Option<String> {
    with_ctx(|c| {
        c.bindings
            .iter()
            .find(|(_, t)| t == ty)
            .map(|(v, _)| v.clone())
    })
}

/// A variable name the declaration does not use: `x`, then the type's
/// initial, then `x1`, `x2`, ….
fn fresh_name(type_name: &str) -> String {
    with_ctx(|c| {
        let initial = type_name
            .trim_start_matches('@')
            .chars()
            .next()
            .map(|ch| ch.to_ascii_lowercase().to_string())
            .unwrap_or_else(|| "y".into());
        for candidate in ["x".to_string(), initial] {
            if !c.vars.contains(&candidate) {
                return candidate;
            }
        }
        (1..)
            .map(|n| format!("x{n}"))
            .find(|n| !c.vars.contains(n))
            .unwrap()
    })
}

/// The edition a source's first line selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditionLine {
    /// No `gq N;` line.
    Absent,
    /// `gq N;` spanning `start..end` in the source.
    Present {
        edition: u64,
        start: usize,
        end: usize,
    },
}

/// Read the edition line: `gq`, word-bounded, an integer and `;`, after
/// leading whitespace and comments. Anything else is `Absent`.
pub fn edition_line(input: &str) -> EditionLine {
    let rest = skip_trivia(input);
    let start = input.len() - rest.len();
    let Some(after) = rest.strip_prefix("gq") else {
        return EditionLine::Absent;
    };
    if after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
        return EditionLine::Absent;
    }
    let after = skip_trivia(after);
    let digits = after.len() - after.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    if digits == 0 {
        return EditionLine::Absent;
    }
    let Ok(edition) = after[..digits].parse::<u64>() else {
        return EditionLine::Absent;
    };
    let after = skip_trivia(&after[digits..]);
    let Some(tail) = after.strip_prefix(';') else {
        return EditionLine::Absent;
    };
    EditionLine::Present {
        edition,
        start,
        end: input.len() - tail.len(),
    }
}

/// A malformed edition line (section 5): `gq 3` without `;`, `GQ 3;`, or a
/// settings line before it. Refused with `gq 3;` as the fix. `None` when the
/// source has no edition line at all, or a well-formed one.
pub fn edition_near_miss(input: &str) -> Option<Refusal3> {
    if !matches!(edition_line(input), EditionLine::Absent) {
        return None;
    }
    // The candidate line, after any settings lines.
    let mut cursor = input.len() - skip_trivia(input).len();
    let first = cursor;
    let mut settings_end = None;
    loop {
        let rest = &input[cursor..];
        let word_end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        let word = &rest[..word_end];
        if word == "set" || word == "reset" {
            let semi = rest.find(';')?;
            cursor += semi + 1;
            settings_end = Some(cursor);
            cursor = input.len() - skip_trivia(&input[cursor..]).len();
            continue;
        }
        break;
    }
    let rest = &input[cursor..];
    if !rest.get(..2).is_some_and(|w| w.eq_ignore_ascii_case("gq")) {
        return None;
    }
    let after = &rest[2..];
    if after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    let ws = after.len() - after.trim_start().len();
    let digits = after[ws..].len()
        - after[ws..]
            .trim_start_matches(|c: char| c.is_ascii_digit())
            .len();
    if digits == 0 {
        return None;
    }
    let mut end = cursor + 2 + ws + digits;
    let tail = &input[end..];
    let tail_ws = tail.len() - tail.trim_start_matches([' ', '\t']).len();
    let has_semi = tail[tail_ws..].starts_with(';');
    if has_semi {
        end += tail_ws + 1;
    }
    let line = &input[cursor..end];
    let number = &line[2 + ws..2 + ws + digits];
    let fixed = format!("gq {number};");
    let (message, edits, span) = match settings_end {
        Some(_) => (
            "the edition line comes first, before every settings line".to_string(),
            vec![edit(first, first, format!("{fixed}\n")), {
                // Delete the misplaced line and the whitespace before it.
                let before = input[..cursor].trim_end_matches([' ', '\t', '\r', '\n']);
                edit(before.len(), end, "")
            }],
            (first, end),
        ),
        None => (
            if has_semi {
                format!(
                    "`{line}` is not the edition line; it is lowercase `gq`, the edition and `;`"
                )
            } else {
                format!(
                    "`{line}` is missing its `;`; the edition line is `gq`, the edition and `;`"
                )
            },
            vec![edit(cursor, end, fixed)],
            (cursor, end),
        ),
    };
    Some(refusal(input, Q006, message, span, edits, MACHINE))
}

/// The edition-3 file as the front door hands it on: the settings, an
/// edition-2 statement body unchanged, and one [`QueryDecl`] per declaration
/// carrying its edition-3 tree. The edition-2 body fields of such a
/// declaration stay empty; its name, metadata, parameters and write mode are
/// filled so every door can select and describe it.
pub(crate) fn into_query_file(file: File3) -> QueryFile {
    let settings = file.settings.clone();
    let file = Arc::new(file);
    let header = |index: usize| {
        let edition3 = Edition3Decl::new(Arc::clone(&file), index);
        let decl = edition3.decl();
        let metadata = |name: &str| {
            decl.annotations
                .iter()
                .find_map(|a| match a.args.as_deref() {
                    Some(
                        [
                            AnnotationArg3::Positional(AnnotationValue3::Literal(Literal::String(
                                s,
                            ))),
                        ],
                    ) if a.name == name => Some(s.clone()),
                    _ => None,
                })
        };
        QueryDecl {
            name: decl.name.clone(),
            description: metadata("description"),
            instruction: metadata("instruction"),
            params: decl
                .params
                .iter()
                .map(|p| Param {
                    name: p.name.clone(),
                    type_name: p.type_name.clone(),
                    nullable: p.nullable,
                })
                .collect(),
            match_clause: Vec::new(),
            return_clause: Vec::new(),
            distinct: false,
            order_clause: Vec::new(),
            limit: None,
            mutations: Vec::new(),
            write_mode: decl.write_mode.clone(),
            edition3: Some(edition3.clone()),
        }
    };
    let body = match &file.body {
        Body3::Decls(decls) => FileBody::Queries((0..decls.len()).map(header).collect()),
        Body3::Explain(_) => FileBody::Explain(header(0)),
        Body3::Statement(statement) => statement.0.clone(),
    };
    QueryFile { settings, body }
}

pub fn parse_query3(input: &str) -> std::result::Result<File3, QueryDiagnostic> {
    parse_query3_detailed(input).map_err(|r| r.diagnostic)
}

/// [`parse_query3`] keeping the construct span of a refusal.
pub fn parse_query3_detailed(input: &str) -> R<File3> {
    match structural(input) {
        Ok(file) => Ok(file),
        Err(first) => Err(habits3::with_habits(input, first, structural_refusal)),
    }
}

/// The structural parse alone, for the habit pass to reparse a candidate.
fn structural_refusal(input: &str) -> Option<Refusal3> {
    structural(input).err()
}

fn structural(input: &str) -> R<File3> {
    let pairs =
        QueryParser::parse(Rule::gq3_file, input).map_err(|error| grammar_error(input, error))?;
    let file = pairs.into_iter().next().expect("grammar: one gq3_file");
    let mut settings = Vec::new();
    let mut decls = Vec::new();
    let mut statement: Option<Body3> = None;
    for inner in file.into_inner() {
        match inner.as_rule() {
            Rule::edition_line => {
                let n = inner
                    .clone()
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::integer)
                    .expect("grammar: edition_line holds an integer");
                if n.as_str() != "3" {
                    let (s, e) = span_of(&n);
                    return Err(refusal(
                        input,
                        Q006,
                        format!("edition {} is not edition 3", n.as_str()),
                        span_of(&inner),
                        vec![edit(s, e, "3")],
                        REVIEW,
                    ));
                }
            }
            Rule::setting_stmt => settings.push(parse_setting_stmt(inner)?),
            Rule::branch_stmt => {
                statement = Some(Body3::Statement(Box::new(StatementBody(FileBody::Branch(
                    parse_branch_stmt(inner)?,
                )))))
            }
            Rule::commit_stmt => {
                statement = Some(Body3::Statement(Box::new(StatementBody(FileBody::Commit(
                    parse_commit_stmt(inner)?,
                )))))
            }
            Rule::load_stmt => {
                statement = Some(Body3::Statement(Box::new(StatementBody(FileBody::Load(
                    parse_load_stmt(inner)?,
                )))))
            }
            Rule::show_stmt => {
                statement = Some(Body3::Statement(Box::new(StatementBody(FileBody::Show(
                    parse_setting_target(inner)?,
                )))))
            }
            Rule::explain_stmt3 => {
                let wrapped = inner
                    .into_inner()
                    .find(|p| matches!(p.as_rule(), Rule::query_decl3 | Rule::load_stmt))
                    .expect("grammar: explain_stmt3 wraps one declaration or load");
                statement = Some(match wrapped.as_rule() {
                    Rule::load_stmt => Body3::Statement(Box::new(StatementBody(
                        FileBody::ExplainLoad(parse_load_stmt(wrapped)?),
                    ))),
                    _ => Body3::Explain(Box::new(decl(wrapped)?)),
                });
            }
            Rule::statement_trailer3 => {
                return Err(
                    diagnostic_at(Q004, "a statement stands alone in its file", &inner).into(),
                );
            }
            Rule::missing_param_list3 => {
                let name = inner
                    .clone()
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::ident)
                    .expect("grammar: missing_param_list3 holds an ident");
                let byte = name.as_span().end();
                return Err(refusal(
                    input,
                    Q002,
                    "expected `(`: a query declares its parameters even when it has none",
                    (inner.as_span().start(), byte),
                    vec![edit(byte, byte, "()")],
                    MACHINE,
                ));
            }
            Rule::declaration3 => {
                let d = inner.into_inner().next().expect("grammar: one declaration");
                match d.as_rule() {
                    Rule::query_decl3 => decls.push(decl(d)?),
                    Rule::mutation_decl => return Err(declaration_keyword(input, d, "mutation")),
                    Rule::view_decl => return Err(declaration_keyword(input, d, "view")),
                    other => unreachable!("grammar: declaration3 holds no {other:?}"),
                }
            }
            _ => {}
        }
    }
    Ok(File3 {
        settings,
        body: statement.unwrap_or(Body3::Decls(decls)),
    })
}

/// The construct a grammar rule stands for, as a parse error names it.
fn construct_name(rule: Rule) -> Option<&'static str> {
    use Rule as X;
    Some(match rule {
        X::EOI => "the end of the source",
        X::declaration3 | X::query_decl3 | X::kw_query => "a declaration `query name(…) { … }`",
        X::param_list3 => "a parameter list `($name: Type, …)`",
        X::param3 | X::variable => "a variable `$name`",
        X::type_ref | X::base_type | X::list_type | X::vector_type => {
            "a parameter type (`String`, `I64`, `[String]`, `Vector(3)`, …)"
        }
        X::annotation3 | X::annotation_name => "an annotation `@name(…)`",
        X::read_body3 | X::match_clause3 | X::kw_match => "`match { … }`",
        X::return_clause3 | X::kw_return => "`return { … }`",
        X::projection3 => "a return item",
        X::kw_as | X::alias3 => "`as <alias>`",
        X::order_clause3 | X::kw_order => "`order { … }`",
        X::ordering3 => "an order key",
        X::order_dir3 => "`asc` or `desc`",
        X::limit_clause3 | X::kw_limit => "`limit <n>`",
        X::clause3
        | X::let_clause
        | X::from_clause
        | X::binding3
        | X::traversal3
        | X::filter3
        | X::negation3
        | X::optional_block => {
            "a clause (a binding `$x: Type`, a traversal `$a edge $b`, a filter, `let`, `from`, `not { }` or `optional { }`)"
        }
        X::relation_expr | X::inline_read => {
            "a relation (`{ match { … } return { … } }`, a call or `$r`)"
        }
        X::mutation_body3 | X::write_group | X::mutation_stmt3 => {
            "a statement (`insert T { … }`, `update $x set { … }` or `delete $x`)"
        }
        X::insert_stmt3 | X::kw_insert => "`insert T { … }`",
        X::update_stmt3 | X::kw_update => "`update $x set { … }`",
        X::delete_stmt3 | X::kw_delete => "`delete $x`",
        X::kw_set => "`set { … }`",
        X::assignment3 => "an assignment `property: value`",
        X::prop_match_list3 | X::prop_match3 => "a property match `property: value`",
        X::type_name => "a type name (`Person`)",
        X::ident | X::expr_ident | X::edge_ident3 | X::edge_ident => "a name",
        X::edge_sel3 | X::undirected_edge3 | X::edge_alternation3 => "an edge name",
        X::traversal_bounds => "hop bounds `{m,n}`",
        X::expr3
        | X::or_expr3
        | X::and_expr3
        | X::not_expr3
        | X::comparison3
        | X::sum3
        | X::product3
        | X::unary3
        | X::operand3
        | X::literal3
        | X::call
        | X::prop_access3
        | X::block_value
        | X::agg_distinct
        | X::positional
        | X::string_lit
        | X::integer3
        | X::float_lit3
        | X::bool_lit
        | X::list_lit3
        | X::meta_field => "an expression",
        X::call_args | X::call_option => "a call argument or `name: value` option",
        X::comp_op | X::null_test | X::kw_in | X::kw_is => "a comparison",
        X::sum_op3 | X::product_op => "an operator",
        X::kw_and | X::kw_or | X::kw_not => "`and`, `or` or `not`",
        X::kw_distinct => "`distinct`",
        X::setting_stmt => "a settings line",
        X::edition_line | X::kw_gq => "the edition line `gq 3;`",
        X::as_commit_clause | X::as_unattached_clause => "`as commit` or `as unattached branch`",
        _ => return None,
    })
}

/// A source the grammar refuses with no recognizer: a parse error naming the
/// constructs expected there and the token found.
fn grammar_error(input: &str, err: pest::error::Error<Rule>) -> Refusal3 {
    let byte = match err.location {
        InputLocation::Pos(pos) => pos,
        InputLocation::Span((start, _)) => start,
    };
    let mut names: Vec<&'static str> = Vec::new();
    if let ErrorVariant::ParsingError { positives, .. } = &err.variant {
        for rule in positives {
            if let Some(n) = construct_name(*rule) {
                if !names.contains(&n) {
                    names.push(n);
                }
            }
        }
    }
    let found = token_at(input, byte);
    // After an expression the ladder offers every operator; name the two
    // constructs the reader can act on.
    let clause = "a clause (a binding `$x: Type`, a traversal `$a edge $b`, a filter, `let`, `from`, `not { }` or `optional { }`)";
    if names.contains(&clause) && names.len() > 2 {
        names = vec!["an operator that continues the expression", clause];
    }
    let expected = match names.len() {
        0 => String::new(),
        1 => format!("expected {}", names[0]),
        _ => {
            let (last, rest) = names.split_last().unwrap();
            format!("expected {} or {}", rest.join(", "), last)
        }
    };
    let message = match (expected.is_empty(), found) {
        (true, Some(t)) => format!("`{t}` is not GQ here"),
        (true, None) => "the source ends early".to_string(),
        (false, Some(t)) => format!("{expected}; found `{t}`"),
        (false, None) => format!("{expected}; the source ends here"),
    };
    QueryDiagnostic::parse(Q001, message, Some(Position::at(input, byte))).into()
}

/// The token at `byte`, for a message.
fn token_at(input: &str, byte: usize) -> Option<&str> {
    let rest = input.get(byte..)?;
    let first = rest.chars().next()?;
    let len = if first.is_ascii_alphanumeric() || first == '_' || first == '$' || first == '@' {
        1 + rest[first.len_utf8()..]
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len() - first.len_utf8())
    } else {
        let two = rest.get(..2).unwrap_or("");
        if ["<>", "==", "&&", "||", "!=", ">=", "<=", "->", "<-", "=~"].contains(&two) {
            2
        } else {
            first.len_utf8()
        }
    };
    Some(&rest[..len.min(rest.len())])
}

/// `mutation name(…) { … }` and `view name(…) { … }`: a parse error with
/// `query` as the fix, as a machine-applicable edit of the keyword.
fn declaration_keyword(input: &str, d: Pair<Rule>, word: &str) -> Refusal3 {
    let keyword = d
        .clone()
        .into_inner()
        .next()
        .expect("grammar: the declaration opens with its keyword");
    let (s, e) = span_of(&keyword);
    let message = if word == "view" {
        "`view` is not a declaration keyword: a named read is a `query`, and `from name(…)` draws its rows"
    } else {
        "`mutation` is not a declaration keyword: a write is a `query` whose body ends in statements"
    };
    refusal(
        input,
        Q009,
        message,
        (s, e),
        vec![edit(s, e, "query")],
        MACHINE,
    )
}

/// Collect the facts a declaration's fixes need.
fn collect_ctx(pair: &Pair<Rule>) -> Ctx {
    let mut ctx = Ctx::default();
    for p in pair.clone().into_inner().flatten() {
        match p.as_rule() {
            Rule::param3 | Rule::param_untyped_rec => {
                if let Some(v) = p.clone().into_inner().next() {
                    ctx.params.push(v.as_str()[1..].to_string());
                }
            }
            Rule::binding3 | Rule::binding_brace_filter | Rule::binding_paren_rec => {
                let mut inner = p.clone().into_inner();
                let v = inner.next().map(|v| v.as_str()[1..].to_string());
                let t = inner.find_map(|t| match t.as_rule() {
                    Rule::type_name => Some(t.as_str().to_string()),
                    Rule::binding_paren_head => {
                        t.into_inner().next().map(|tn| tn.as_str().to_string())
                    }
                    _ => None,
                });
                if let (Some(v), Some(t)) = (v, t) {
                    ctx.bindings.push((v, t));
                }
            }
            Rule::variable => {
                ctx.vars.insert(p.as_str()[1..].to_string());
            }
            _ => {}
        }
    }
    ctx
}

fn decl(pair: Pair<Rule>) -> R<Decl3> {
    let input = input_of(&pair);
    let ctx = collect_ctx(&pair);
    with_ctx(|c| *c = ctx);
    let mut name = None;
    let mut params = Vec::new();
    let mut annotations = Vec::new();
    let mut write_clause = None;
    let mut body = None;
    for item in pair.into_inner() {
        match item.as_rule() {
            Rule::kw_query => {}
            Rule::ident => name = Some(item.as_str().to_string()),
            Rule::param_list3 => {
                for p in item.into_inner() {
                    params.push(param(p)?);
                }
            }
            Rule::annotation3 => annotations.push(annotation(item)?),
            Rule::as_commit_clause | Rule::as_unattached_clause => write_clause = Some(item),
            Rule::read_body3 => body = Some(DeclBody3::Read(read_body(item)?)),
            Rule::swapped_read_rec => {
                let input = input_of(&item);
                let ret = item.clone().into_inner().next().unwrap();
                let mat = item.clone().into_inner().nth(1).unwrap();
                let (rs, _) = span_of(&ret);
                let (_, me) = span_of(&mat);
                return Err(refusal(
                    input,
                    Q020,
                    "a read is `match { … }`, then `return { … }`, then `order` and `limit`",
                    (rs, me),
                    vec![edit(
                        rs,
                        me,
                        format!("{}\n  {}", mat.as_str().trim(), ret.as_str().trim()),
                    )],
                    MACHINE,
                ));
            }
            Rule::mutation_body3 => body = Some(DeclBody3::Write(write_body(item)?)),
            other => unreachable!("grammar: query_decl3 holds no {other:?}"),
        }
    }
    let name = name.expect("grammar: query_decl3 names itself");
    let body = body.expect("grammar: query_decl3 has a body");
    let write_mode = match write_clause {
        None => MutationWriteMode::Publish,
        Some(clause) => {
            if matches!(body, DeclBody3::Read(_)) {
                let spelled = match clause.as_rule() {
                    Rule::as_commit_clause => "as commit",
                    _ => "as unattached branch",
                };
                // The kind is in the parse tree, so this is a parse error
                // (section 5); the fix deletes the clause.
                let (s, e) = span_of(&clause);
                let (_, e2) = with_trailing_ws(input, s, e);
                return Err(refusal(
                    input,
                    Q011,
                    format!(
                        "`{spelled}` applies to a write; the body of `{name}` ends in `return`, so it is a read with no statement to commit"
                    ),
                    (s, e),
                    vec![edit(s, e2, "")],
                    MACHINE,
                ));
            }
            match clause.as_rule() {
                Rule::as_commit_clause => MutationWriteMode::AsCommit,
                _ => MutationWriteMode::Unattached {
                    base: parse_unattached_base(clause)?,
                },
            }
        }
    };
    Ok(Decl3 {
        name,
        params,
        annotations,
        write_mode,
        body,
    })
}

fn param(pair: Pair<Rule>) -> R<Param3> {
    let input = input_of(&pair);
    if pair.as_rule() == Rule::param_untyped_rec {
        let (s, e) = span_of(&pair);
        return Err(refusal(
            input,
            Q005,
            format!(
                "the parameter `{}` has no type; a parameter is `$name: Type`",
                pair.as_str()
            ),
            (s, e),
            vec![edit(e, e, ": String")],
            REVIEW,
        ));
    }
    let mut inner = pair.into_inner();
    let var = inner.next().unwrap().as_str();
    let name = var[1..].to_string();
    let type_ref = inner.next().unwrap();
    let nullable = type_ref.as_str().trim_end().ends_with('?');
    let core = type_ref.into_inner().next().unwrap();
    let type_name = match core.as_rule() {
        Rule::base_type => core.as_str().to_string(),
        Rule::list_type => format!("[{}]", core.into_inner().next().unwrap().as_str().trim()),
        Rule::vector_type => format!("Vector({})", core.into_inner().next().unwrap().as_str()),
        other => unreachable!("grammar: type_ref holds no {other:?}"),
    };
    let default = match inner.next() {
        None => None,
        Some(d) => {
            let mut negative = false;
            let mut value = None;
            for part in d.into_inner() {
                match part.as_rule() {
                    Rule::neg_sign => negative = true,
                    _ => value = Some(literal(part)?),
                }
            }
            Some(ParamDefault {
                negative,
                value: value.expect("grammar: param_default holds a literal"),
            })
        }
    };
    Ok(Param3 {
        name,
        type_name,
        nullable,
        default,
    })
}

/// An annotation is parsed whatever its name; the type checker decides
/// which names are read (`T80`), so a later typed annotation is additive.
fn annotation(pair: Pair<Rule>) -> R<Annotation3> {
    let input = input_of(&pair);
    let whole = span_of(&pair);
    let mut inner = pair.clone().into_inner();
    let name = inner.next().unwrap().as_str()[1..].to_string();
    let text = pair.as_str();
    let mut args = Vec::new();
    for arg in inner {
        let a = arg.into_inner().next().unwrap();
        args.push(match a.as_rule() {
            Rule::annotation_kwarg => {
                let mut kv = a.into_inner();
                let key = kv.next().unwrap().as_str().to_string();
                AnnotationArg3::Named(key, annotation_value(kv.next().unwrap())?)
            }
            Rule::annotation_colon_rec => {
                let mut kv = a.clone().into_inner();
                let key = kv.next().unwrap();
                let value = kv.next().unwrap();
                let (_, ke) = span_of(&key);
                let (vs, _) = span_of(&value);
                return Err(refusal(
                    input,
                    Q021,
                    "an annotation's named argument is written `name = value`, as in `.pg`",
                    whole,
                    vec![edit(ke, vs, " = ")],
                    MACHINE,
                ));
            }
            _ => AnnotationArg3::Positional(annotation_value(a)?),
        });
    }
    let (start, end) = with_trailing_ws(input, whole.0, whole.1);
    Ok(Annotation3 {
        name,
        args: text.contains('(').then_some(args),
        at: SourceSpan {
            start,
            end,
            position: Position::at(input, whole.0),
            operands: Vec::new(),
        },
    })
}

fn annotation_value(pair: Pair<Rule>) -> R<AnnotationValue3> {
    let v = pair.into_inner().next().unwrap();
    Ok(match v.as_rule() {
        Rule::literal3 => AnnotationValue3::Literal(literal(v)?),
        Rule::variable => AnnotationValue3::Variable(v.as_str()[1..].to_string()),
        Rule::meta_field => AnnotationValue3::MetaField(v.as_str()[1..].to_string()),
        _ => AnnotationValue3::Ident(v.as_str().to_string()),
    })
}

fn read_body(pair: Pair<Rule>) -> R<ReadBody3> {
    let input = input_of(&pair);
    let mut body = ReadBody3 {
        clauses: Vec::new(),
        distinct: false,
        projections: Vec::new(),
        order: Vec::new(),
        limit: None,
    };
    let mut seen_return = false;
    let mut match_end = 0;
    let mut tail: Vec<Pair<Rule>> = Vec::new();
    for section in pair.clone().into_inner() {
        match section.as_rule() {
            Rule::match_clause3 => {
                match_end = section.as_span().end();
                body.clauses = clause_list(
                    section
                        .into_inner()
                        .filter(|c| matches!(c.as_rule(), Rule::clause3 | Rule::stmt_edge_rec))
                        .collect(),
                )?;
            }
            Rule::match_nobrace_rec => {
                let kw = section.clone().into_inner().next().unwrap();
                let (s, e) = span_of(&section);
                let e = input[..e].trim_end().len();
                let (_, ke) = span_of(&kw);
                return Err(refusal(
                    input,
                    Q020,
                    "`match` takes its clauses in braces",
                    (s, e),
                    vec![edit(ke, ke, " {"), edit(e, e, " }")],
                    MACHINE,
                ));
            }
            Rule::retrieve_rec => {
                return Err(refusal_note(
                    input,
                    Q020,
                    "`retrieve` was withdrawn: a ranking is a relation function drawn with `from`",
                    span_of(&section),
                    "match { from nearest({ match { … } return { $d } }, $d.embedding, $q, k: 10) }",
                ));
            }
            Rule::return_clause3 => {
                seen_return = true;
                let mut distinct_items = Vec::new();
                let return_span = span_of(&section);
                let mut keyword_end = 0;
                for p in section.into_inner() {
                    match p.as_rule() {
                        Rule::kw_return => keyword_end = p.as_span().end(),
                        Rule::kw_distinct => {
                            body.distinct = true;
                            keyword_end = p.as_span().end();
                        }
                        Rule::projection3 => {
                            if let Some(item) = p
                                .clone()
                                .into_inner()
                                .find(|i| i.as_rule() == Rule::distinct_item_rec)
                            {
                                distinct_items.push(item);
                            } else if distinct_items.is_empty() {
                                body.projections.push(projection(p)?);
                            }
                        }
                        _ => {}
                    }
                }
                if !distinct_items.is_empty() {
                    let mut edits: Vec<TextEdit> = distinct_items
                        .iter()
                        .map(|item| {
                            let kw = item.clone().into_inner().next().unwrap();
                            let (s, e) =
                                with_trailing_ws(input, kw.as_span().start(), kw.as_span().end());
                            edit(s, e, "")
                        })
                        .collect();
                    if !body.distinct {
                        edits.push(edit(keyword_end, keyword_end, " distinct"));
                    }
                    return Err(refusal(
                        input,
                        Q014,
                        "`distinct` belongs to the `return`, not to an item: `return distinct { … }`",
                        return_span,
                        edits,
                        MACHINE,
                    ));
                }
            }
            Rule::return_nobrace_rec => {
                let mut kw_end = 0;
                for p in section.clone().into_inner() {
                    if matches!(p.as_rule(), Rule::kw_return | Rule::kw_distinct) {
                        kw_end = p.as_span().end();
                    }
                }
                let (s, e) = span_of(&section);
                let e = input[..e]
                    .trim_end_matches([' ', '\t', '\r', '\n', ','])
                    .len()
                    .max(kw_end);
                return Err(refusal(
                    input,
                    Q020,
                    "`return` takes its items in braces",
                    (s, e),
                    vec![edit(kw_end, kw_end, " {"), edit(e, e, " }")],
                    MACHINE,
                ));
            }
            _ => tail.push(section),
        }
    }
    if !seen_return {
        let var = first_binding();
        let span = span_of(&pair);
        return Err(match var {
            Some(v) => refusal(
                input,
                Q020,
                "a read ends in `return { … }`; a body that writes ends in statements",
                span,
                vec![edit(match_end, match_end, format!("\n  return {{ ${v} }}"))],
                REVIEW,
            ),
            None => refusal_note(
                input,
                Q020,
                "a read ends in `return { … }`; a body that writes ends in statements",
                span,
                "return { <items> }",
            ),
        });
    }
    read_tail(input, &mut body, tail)?;
    Ok(body)
}

/// `order` then `limit`, each at most once; the recognised tails refused.
fn read_tail(input: &str, body: &mut ReadBody3, tail: Vec<Pair<Rule>>) -> R<()> {
    let mut order_pair: Option<Pair<Rule>> = None;
    let mut limit_pair: Option<Pair<Rule>> = None;
    for section in tail {
        let span = span_of(&section);
        match section.as_rule() {
            Rule::order_clause3 => {
                if let Some(l) = &limit_pair {
                    let (ls, le) = span_of(l);
                    return Err(refusal(
                        input,
                        Q020,
                        "`order` comes before `limit`",
                        (ls, span.1),
                        vec![edit(
                            ls,
                            span.1,
                            format!("{}\n  {}", section.as_str(), &input[ls..le]),
                        )],
                        MACHINE,
                    ));
                }
                if order_pair.is_some() {
                    return Err(refusal_note(
                        input,
                        Q020,
                        "a read has one `order`; list every key in it",
                        span,
                        "order { <key>, <key> }",
                    ));
                }
                for o in section.clone().into_inner() {
                    if o.as_rule() == Rule::ordering3 {
                        body.order.push(ordering(o)?);
                    }
                }
                order_pair = Some(section);
            }
            Rule::limit_clause3 => {
                if limit_pair.is_some() {
                    return Err(refusal_note(
                        input,
                        Q020,
                        "a read has one `limit`",
                        span,
                        "limit <n>",
                    ));
                }
                let value = section
                    .clone()
                    .into_inner()
                    .find(|p| p.as_rule() != Rule::kw_limit)
                    .unwrap();
                body.limit = Some(match value.as_rule() {
                    Rule::variable => Limit3::Param(value.as_str()[1..].to_string()),
                    _ => Limit3::Literal(value.as_str().parse::<u64>().map_err(|e| {
                        Refusal3::from(diagnostic_at(Q005, format!("invalid limit: {e}"), &value))
                    })?),
                });
                limit_pair = Some(section);
            }
            Rule::limit_neg_rec => {
                return Err(refusal_note(
                    input,
                    Q020,
                    "a limit is a non-negative integer or a parameter",
                    span,
                    "limit <n>",
                ));
            }
            Rule::order_by_rec => {
                let keys: Vec<&str> = section
                    .clone()
                    .into_inner()
                    .filter(|p| p.as_rule() == Rule::ordering3)
                    .map(|p| p.as_str().trim())
                    .collect();
                return Err(refusal(
                    input,
                    Q014,
                    "`order by` is SQL's; GQ writes the keys in braces, `order { … }`",
                    span,
                    vec![edit(
                        span.0,
                        span.1,
                        format!("order {{ {} }}", keys.join(", ")),
                    )],
                    MACHINE,
                ));
            }
            Rule::offset_rec => {
                let (s, e) = with_trailing_ws(input, span.0, span.1);
                return Err(refusal(
                    input,
                    Q015,
                    format!(
                        "`{}` is paging, which stage P adds as a continuation token over the total order; this release has `limit` only",
                        section
                            .as_str()
                            .split_whitespace()
                            .next()
                            .unwrap_or("offset")
                    ),
                    span,
                    vec![edit(s, e, "")],
                    REVIEW,
                ));
            }
            Rule::group_by_rec => {
                let (s, e) = with_trailing_ws(input, span.0, span.1);
                return Err(refusal(
                    input,
                    Q014,
                    "`group by` is SQL's; a `return` with an aggregate groups by its other items",
                    span,
                    vec![edit(s, e, "")],
                    MACHINE,
                ));
            }
            Rule::having_rec => {
                return Err(refusal_note(
                    input,
                    Q014,
                    "`having` is SQL's; a grouped result is filtered by drawing it with `from` and filtering its rows",
                    span,
                    "match { from { match { … } return { $c, count($p) as n } }  $n > 3 } return { … }",
                ));
            }
            other => unreachable!("grammar: read_tail3 holds no {other:?}"),
        }
    }
    Ok(())
}

fn ordering(pair: Pair<Rule>) -> R<Ordering3> {
    let input = input_of(&pair);
    let first = pair.clone().into_inner().next().unwrap();
    if first.as_rule() == Rule::dir_first_rec {
        let mut parts = first.clone().into_inner();
        let dir = parts.next().unwrap();
        let key = parts.next().unwrap();
        let span = span_of(&first);
        return Err(refusal(
            input,
            Q014,
            "the direction follows its key: `order { <key> desc }`",
            span,
            vec![edit(
                span.0,
                span.1,
                format!("{} {}", key.as_str().trim(), dir.as_str()),
            )],
            MACHINE,
        ));
    }
    let mut parts = pair.into_inner();
    let key = parts.next().unwrap();
    // A whole key that is a bare name is a return alias.
    let expr = match lone(&key, Rule::bare_name_rec) {
        Some(name) => Expr3::Ident(name.as_str().to_string()),
        None => expr(key)?,
    };
    let descending = parts.next().map(|d| d.as_str() == "desc");
    Ok(Ordering3 { expr, descending })
}

fn projection(pair: Pair<Rule>) -> R<Projection3> {
    let input = input_of(&pair);
    let mut inner = pair.clone().into_inner();
    let first = inner.next().unwrap();
    match first.as_rule() {
        Rule::sql_alias_rec => {
            let mut parts = first.clone().into_inner();
            let name = parts.next().unwrap();
            let value = parts.next().unwrap();
            let span = span_of(&first);
            let sep = if input[name.as_span().end()..value.as_span().start()].contains(':') {
                "GraphQL's `alias: value`"
            } else {
                "SQL's `alias = value`"
            };
            return Err(refusal(
                input,
                Q014,
                format!("{sep} is written `value as alias` in GQ"),
                span,
                vec![edit(
                    span.0,
                    span.1,
                    format!("{} as {}", value.as_str().trim(), name.as_str()),
                )],
                MACHINE,
            ));
        }
        Rule::star_item_rec => {
            let vars: Vec<String> =
                with_ctx(|c| c.bindings.iter().map(|(v, _)| format!("${v}")).collect());
            let span = span_of(&first);
            return Err(if vars.is_empty() {
                refusal_note(
                    input,
                    Q014,
                    "`*` is not GQ: a return lists its items",
                    span,
                    "return { $x }",
                )
            } else {
                refusal(
                    input,
                    Q014,
                    "`*` is not GQ: a return lists its items; returning a node returns its properties",
                    span,
                    vec![edit(span.0, span.1, vars.join(", "))],
                    REVIEW,
                )
            });
        }
        _ => {}
    }
    let start = first.as_span().start();
    let e = at_top(Top::Item, start, || expr(first))?;
    let mut alias = None;
    for part in inner {
        if part.as_rule() != Rule::alias3 {
            continue;
        }
        let name = part.clone().into_inner().nth(1).unwrap();
        match name.as_rule() {
            Rule::reserved_alias => {
                let (s, e) = span_of(&name);
                let word = name.as_str();
                let renamed = format!("{word}_");
                let mut edits = vec![edit(s, e, renamed.clone())];
                // The alias's uses as whole `order` keys are renamed too.
                if let Some(order) = input[e..].find("order").map(|k| e + k) {
                    if let (Some(open), Some(close)) =
                        (input[order..].find('{'), input[order..].find('}'))
                    {
                        let (open, close) = (order + open, order + close);
                        for key in input[open + 1..close].split(',') {
                            let key_start = open
                                + 1
                                + (key.as_ptr() as usize - input[open + 1..].as_ptr() as usize);
                            let trimmed = key.trim_start();
                            let at = key_start + (key.len() - trimmed.len());
                            let first = trimmed.split_whitespace().next().unwrap_or("");
                            if first == word {
                                edits.push(edit(at, at + word.len(), renamed.clone()));
                            }
                        }
                    }
                }
                return Err(refusal(
                    input,
                    Q005,
                    format!(
                        "`{word}` is one of the six reserved words and cannot be a return alias"
                    ),
                    span_of(&part),
                    edits,
                    REVIEW,
                ));
            }
            Rule::alias_string_rec => {
                let lit = name.as_str();
                let snake: String = lit[1..lit.len() - 1]
                    .trim()
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() {
                            c.to_ascii_lowercase()
                        } else {
                            '_'
                        }
                    })
                    .collect();
                let snake = if snake.starts_with(|c: char| c.is_ascii_digit()) || snake.is_empty() {
                    format!("_{snake}")
                } else {
                    snake
                };
                let (s, e) = span_of(&name);
                return Err(refusal(
                    input,
                    Q014,
                    "an alias is a name, not a string",
                    span_of(&part),
                    vec![edit(s, e, snake)],
                    MACHINE,
                ));
            }
            _ => alias = Some(name.as_str().to_string()),
        }
    }
    Ok(Projection3 { expr: e, alias })
}

fn write_body(pair: Pair<Rule>) -> R<WriteBody3> {
    let input = input_of(&pair);
    let mut groups: Vec<WriteGroup3> = Vec::new();
    let mut last_stmts: Option<(usize, usize)> = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::write_group => {
                let mut clause_pairs: Vec<Pair<Rule>> = Vec::new();
                let mut statements = Vec::new();
                let mut stmt_span: Option<(usize, usize)> = None;
                for p in part.into_inner() {
                    match p.as_rule() {
                        Rule::mutation_stmt3
                            if p.clone().into_inner().next().unwrap().as_rule()
                                == Rule::stmt_var_rec =>
                        {
                            let rec = p.clone().into_inner().next().unwrap();
                            let mut parts = rec.clone().into_inner();
                            let word = parts.next().unwrap().as_str();
                            let b_pair = parts.next().unwrap();
                            let b = b_pair.as_str();
                            if let Some(a) = clause_pairs.last().and_then(lone_var_filter) {
                                // The rec's span ends after the whitespace its
                                // lookahead skipped; the triple ends at `b`.
                                let span = (
                                    clause_pairs.last().unwrap().as_span().start(),
                                    b_pair.as_span().end(),
                                );
                                return Err(bare_edge(input, span, a.as_str(), word, b));
                            }
                            return Err(refusal_note(
                                input,
                                Q005,
                                format!(
                                    "`{word}` takes a type (`insert T {{ … }}`) or a bound variable and `set` (`update $x set {{ … }}`)"
                                ),
                                span_of(&rec),
                                if word == "insert" {
                                    "insert <Type> { <property>: <value> }"
                                } else {
                                    "update $x set { <property>: <value> }"
                                },
                            ));
                        }
                        Rule::mutation_stmt3 => {
                            let s = span_of(&p);
                            stmt_span = Some(match stmt_span {
                                None => s,
                                Some((a, _)) => (a, s.1),
                            });
                            statements.push(statement(p)?);
                        }
                        _ => clause_pairs.push(p),
                    }
                }
                last_stmts = stmt_span;
                groups.push(WriteGroup3 {
                    clauses: clause_list(clause_pairs)?,
                    statements,
                });
            }
            Rule::trailing_clauses_rec => {
                let (ts, te) = span_of(&part);
                let (ss, se) = last_stmts.expect("grammar: a write group precedes its trailer");
                let te = input[..te].trim_end().len();
                return Err(refusal(
                    input,
                    Q013,
                    "a clause follows a statement; a group's clauses come before its statements",
                    (ss, te),
                    vec![edit(
                        ss,
                        te,
                        format!("{}\n  {}", input[ts..te].trim(), input[ss..se].trim()),
                    )],
                    MACHINE,
                ));
            }
            _ => {}
        }
    }
    Ok(WriteBody3 { groups })
}

fn assigns<'i>(p: &Pair<'i, Rule>) -> Vec<Pair<'i, Rule>> {
    p.clone()
        .into_inner()
        .filter(|a| a.as_rule() == Rule::assignment3)
        .collect()
}

fn assignments_text(pairs: &[Pair<Rule>]) -> String {
    pairs
        .iter()
        .map(|p| p.as_str().trim().trim_end_matches(',').trim())
        .collect::<Vec<_>>()
        .join(", ")
}

fn statement(pair: Pair<Rule>) -> R<Stmt3> {
    let input = input_of(&pair);
    let s = pair.into_inner().next().unwrap();
    let span = span_of(&s);
    match s.as_rule() {
        Rule::insert_stmt3 => {
            let mut inner = s
                .clone()
                .into_inner()
                .filter(|p| p.as_rule() != Rule::kw_insert);
            let type_name = inner.next().unwrap().as_str().to_string();
            let assignments = assignment_list(input, &assigns(&s))?;
            Ok(Stmt3::Insert {
                type_name,
                assignments,
            })
        }
        Rule::update_stmt3 => {
            let var = s
                .clone()
                .into_inner()
                .find(|p| p.as_rule() == Rule::variable)
                .unwrap();
            let assignments = assignment_list(input, &assigns(&s))?;
            Ok(Stmt3::Update {
                variable: var.as_str()[1..].to_string(),
                assignments,
            })
        }
        Rule::delete_stmt3 => {
            let var = s
                .into_inner()
                .find(|p| p.as_rule() == Rule::variable)
                .unwrap();
            Ok(Stmt3::Delete {
                variable: var.as_str()[1..].to_string(),
            })
        }
        Rule::where_stmt => Err(where_statement(s)),
        Rule::create_rec => {
            let word = s
                .clone()
                .into_inner()
                .next()
                .unwrap()
                .as_str()
                .to_ascii_lowercase();
            let body = s.clone().into_inner().nth(1).unwrap();
            let replacement = match body.as_rule() {
                Rule::cypher_node => {
                    let mut ty = None;
                    let mut props = Vec::new();
                    for p in body.into_inner() {
                        match p.as_rule() {
                            Rule::cypher_name => ty = Some(p.as_str().to_string()),
                            Rule::cypher_prop => props.push(p.as_str().trim().to_string()),
                            _ => {}
                        }
                    }
                    ty.map(|t| format!("insert {t} {{ {} }}", props.join(", ")))
                }
                _ => {
                    let ty = body.as_str();
                    Some(format!(
                        "insert {ty} {{ {} }}",
                        assignments_text(&assigns(&s))
                    ))
                }
            };
            let message = format!(
                "`{word}` is Cypher's; GQ's keyed `insert T {{ … }}` creates the node, or replaces the one with the same key"
            );
            Err(match replacement {
                Some(r) => refusal(
                    input,
                    Q014,
                    message,
                    span,
                    vec![edit(span.0, span.1, r)],
                    MACHINE,
                ),
                None => refusal_note(
                    input,
                    Q014,
                    message,
                    span,
                    "insert <Type> { <property>: <value> }",
                ),
            })
        }
        Rule::set_rec => {
            let items: Vec<Pair<Rule>> = s
                .clone()
                .into_inner()
                .filter(|p| p.as_rule() == Rule::set_item)
                .collect();
            let mut var = None;
            let mut parts = Vec::new();
            for item in &items {
                let mut kv = item.clone().into_inner();
                let target = kv.next().unwrap();
                let value = kv.next().unwrap();
                let mut tv = target.into_inner();
                let v = tv.next().unwrap().as_str().to_string();
                let prop = tv.next().unwrap().as_str().to_string();
                if var.as_ref().is_some_and(|w| w != &v) {
                    return Err(refusal_note(
                        input,
                        Q014,
                        "Cypher's `set` writes several entities; GQ's `update $x set { … }` writes one",
                        span,
                        "update $x set { <property>: <value> }  update $y set { … }",
                    ));
                }
                var = Some(v);
                parts.push(format!("{prop}: {}", value.as_str().trim()));
            }
            Err(refusal(
                input,
                Q014,
                "Cypher's `set $x.p = v` is `update $x set { p: v }` in GQ",
                span,
                vec![edit(
                    span.0,
                    span.1,
                    format!("update {} set {{ {} }}", var.unwrap(), parts.join(", ")),
                )],
                MACHINE,
            ))
        }
        Rule::delete_multi_rec => {
            let vars: Vec<String> = s
                .clone()
                .into_inner()
                .filter(|p| p.as_rule() == Rule::variable)
                .map(|p| format!("delete {}", p.as_str()))
                .collect();
            Err(refusal(
                input,
                Q014,
                "`delete` names one bound variable; one statement per entity",
                span,
                vec![edit(span.0, span.1, vars.join("  "))],
                MACHINE,
            ))
        }
        Rule::delete_type_rec => {
            let ty = s.clone().into_inner().nth(1).unwrap().as_str();
            let v = fresh_name(ty);
            Err(refusal(
                input,
                Q008,
                format!(
                    "`delete {ty}` names a type; a statement names a bound variable, and a binding selects the rows"
                ),
                span,
                vec![edit(span.0, span.1, format!("${v}: {ty}  delete ${v}"))],
                REVIEW,
            ))
        }
        Rule::insert_paren_rec => {
            let ty = s.clone().into_inner().nth(1).unwrap().as_str();
            Err(refusal(
                input,
                Q014,
                "`insert T { … }` takes its assignments in braces alone",
                span,
                vec![edit(
                    span.0,
                    span.1,
                    format!("insert {ty} {{ {} }}", assignments_text(&assigns(&s))),
                )],
                MACHINE,
            ))
        }
        Rule::update_noset_rec => {
            let var = s.clone().into_inner().nth(1).unwrap();
            let e = var.as_span().end();
            Err(refusal(
                input,
                Q005,
                "`update $x` takes `set { … }`",
                span,
                vec![edit(e, e, " set")],
                MACHINE,
            ))
        }
        other => unreachable!("grammar: mutation_stmt3 holds no {other:?}"),
    }
}

/// Assignments, with the recognised respellings refused together so one
/// suggestion fixes every one of them.
fn assignment_list(input: &str, pairs: &[Pair<Rule>]) -> R<Vec<Assignment3>> {
    let mut edits = Vec::new();
    let mut first: Option<(&'static str, (usize, usize))> = None;
    let mut note = None;
    for p in pairs {
        let a = p.clone().into_inner().next().unwrap();
        let span = span_of(&a);
        match a.as_rule() {
            Rule::eq_assign_rec => {
                let mut kv = a.clone().into_inner();
                let key = kv.next().unwrap();
                let value = kv.next().unwrap();
                edits.push(edit(key.as_span().end(), value.as_span().start(), ": "));
                first.get_or_insert((
                    "an assignment is `property: value`, not `property = value`",
                    span,
                ));
            }
            Rule::qualified_assign_rec => {
                let mut kv = a.clone().into_inner();
                let target = kv.next().unwrap();
                let value = kv.next().unwrap();
                let prop = target.clone().into_inner().nth(1).unwrap();
                edits.push(edit(
                    target.as_span().start(),
                    value.as_span().start(),
                    format!("{}: ", prop.as_str()),
                ));
                first.get_or_insert((
                    "an assignment names the property alone; the statement names the variable",
                    span,
                ));
            }
            Rule::meta_assign_rec => {
                let meta = a.clone().into_inner().next().unwrap();
                let name = match meta.as_str() {
                    "@src" => Some("from"),
                    "@dst" => Some("to"),
                    _ => None,
                };
                match name {
                    Some(n) => {
                        edits.push(edit(meta.as_span().start(), meta.as_span().end(), n));
                        first.get_or_insert((
                            "an edge's endpoints are assigned as `from:` and `to:`",
                            span,
                        ));
                    }
                    None => note = Some(span),
                }
            }
            _ => {}
        }
    }
    if let Some(span) = note {
        return Err(refusal_note(
            input,
            Q005,
            "a meta-field is not assigned; `@id` is the key property",
            span,
            "<key property>: <value>",
        ));
    }
    if let Some((message, span)) = first {
        let whole = (
            pairs.first().unwrap().as_span().start(),
            input[..pairs.last().unwrap().as_span().end()]
                .trim_end()
                .len(),
        );
        let _ = span;
        return Err(refusal(input, Q005, message, whole, edits, MACHINE));
    }
    pairs.iter().map(|p| assignment(p.clone())).collect()
}

/// `delete T where φ` / `update T set { … } where φ`: the fix binds the type
/// with a name the declaration does not use and writes `φ` as a filter
/// clause, each bare property of `φ` and of the assignments qualified
/// (section 5, and the rewriter's row of section 6).
fn where_statement(s: Pair<Rule>) -> Refusal3 {
    let input = input_of(&s);
    let span = span_of(&s);
    let mut kind = "";
    let mut type_name = "";
    let mut bound = None;
    let mut set: Vec<Pair<Rule>> = Vec::new();
    let mut predicate = None;
    for part in s.into_inner() {
        match part.as_rule() {
            Rule::kw_update => kind = "update",
            Rule::kw_delete => kind = "delete",
            Rule::type_name => type_name = part.as_str(),
            Rule::variable => bound = Some(part.as_str()),
            Rule::assignment3 => set.push(part),
            Rule::expr3 => predicate = Some(part),
            _ => {}
        }
    }
    let predicate = predicate.expect("grammar: where_stmt holds its predicate");
    if let Some(var) = bound {
        let name = &var[1..];
        let statement = match kind {
            "update" => format!(
                "update {var} set {{ {} }}",
                set.iter()
                    .map(|a| qualify_bare_names(a, name))
                    .map(|t| t.trim().trim_end_matches(',').trim().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            _ => format!("delete {var}"),
        };
        return refusal(
            input,
            Q008,
            format!(
                "a statement takes no `where`; the clauses before it select the rows `{var}` names"
            ),
            span,
            vec![edit(
                span.0,
                span.1,
                format!(
                    "{}  {statement}",
                    qualify_bare_names(&predicate, name).trim()
                ),
            )],
            MACHINE,
        );
    }
    let v = fresh_name(type_name);
    let filter = qualify_bare_names(&predicate, &v);
    let statement = match kind {
        "update" => format!(
            "update ${v} set {{ {} }}",
            set.iter()
                .map(|a| qualify_bare_names(a, &v))
                .map(|t| t.trim().trim_end_matches(',').trim().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => format!("delete ${v}"),
    };
    refusal(
        input,
        Q008,
        format!(
            "`{kind} {type_name} … where …` is edition 2's form; in edition 3 a statement names a bound variable and the clauses select its rows"
        ),
        span,
        vec![edit(
            span.0,
            span.1,
            format!("${v}: {type_name}  {}  {statement}", filter.trim()),
        )],
        MACHINE,
    )
}

/// The text of `pair` with each bare name and bare meta-field in operand
/// position written as a property of `$var`; the endpoint names `from` and
/// `to` become `$var.@src` and `$var.@dst`.
fn qualify_bare_names(pair: &Pair<Rule>, var: &str) -> String {
    let source = pair.as_str();
    let base = pair.as_span().start();
    let mut edits: Vec<(usize, usize, String)> = pair
        .clone()
        .into_inner()
        .flatten()
        .filter(|p| p.as_rule() == Rule::operand3)
        .filter_map(|p| p.into_inner().next())
        .filter_map(|p| {
            let (s, e) = (p.as_span().start() - base, p.as_span().end() - base);
            match p.as_rule() {
                Rule::bare_name_rec => Some(match p.as_str() {
                    "from" => (s, e, format!("${var}.@src")),
                    "to" => (s, e, format!("${var}.@dst")),
                    name => (s, e, format!("${var}.{name}")),
                }),
                Rule::meta_field => Some((s, e, format!("${var}.{}", p.as_str()))),
                _ => None,
            }
        })
        .collect();
    edits.sort_by_key(|e| std::cmp::Reverse(e.0));
    let mut out = source.to_string();
    for (s, e, r) in edits {
        out.replace_range(s..e, &r);
    }
    out
}

fn assignment(pair: Pair<Rule>) -> R<Assignment3> {
    let mut inner = pair.into_inner();
    let property = inner.next().unwrap().as_str().to_string();
    let value = inner.next().unwrap();
    let value = match value.as_rule() {
        Rule::null_value => Expr3::Literal(Literal::Null),
        _ => expr(value)?,
    };
    Ok(Assignment3 { property, value })
}

/// Descend through single-child expression rules to a pair of `rule`.
fn lone<'i>(pair: &Pair<'i, Rule>, rule: Rule) -> Option<Pair<'i, Rule>> {
    let mut p = pair.clone();
    loop {
        if p.as_rule() == rule {
            return Some(p);
        }
        let mut inner = p.clone().into_inner();
        let child = inner.next()?;
        if inner.next().is_some() {
            return None;
        }
        if !matches!(
            p.as_rule(),
            Rule::clause3
                | Rule::filter3
                | Rule::expr3
                | Rule::or_expr3
                | Rule::and_expr3
                | Rule::not_expr3
                | Rule::comparison3
                | Rule::sum3
                | Rule::product3
                | Rule::unary3
                | Rule::operand3
                | Rule::literal3
                | Rule::relation_expr
        ) {
            return None;
        }
        p = child;
    }
}

/// The clause kind under a `clause3` pair.
fn kind<'i>(p: &Pair<'i, Rule>) -> Option<Pair<'i, Rule>> {
    if p.as_rule() == Rule::clause3 {
        p.clone().into_inner().next()
    } else {
        Some(p.clone())
    }
}

fn lone_var_filter<'i>(p: &Pair<'i, Rule>) -> Option<Pair<'i, Rule>> {
    let k = kind(p)?;
    (k.as_rule() == Rule::filter3)
        .then(|| lone(&k, Rule::variable))
        .flatten()
}

/// A clause list, with the shapes that span two or three clauses checked
/// first: a contextual keyword or Boolean literal written as a bare edge name
/// between two lone variables (`$a from $b`, `$a let $b`, `$a true $b`), and
/// Cypher's `with`, whose fix takes the clauses before it.
fn clause_list(pairs: Vec<Pair<Rule>>) -> R<Vec<Clause3>> {
    for i in 0..pairs.len() {
        let input = input_of(&pairs[i]);
        if let Some(a) = lone_var_filter(&pairs[i]) {
            // `$a from $b`: a lone-variable filter, then `from $b`.
            if let Some(next) = pairs.get(i + 1).and_then(kind) {
                if next.as_rule() == Rule::from_clause {
                    let rel = next
                        .clone()
                        .into_inner()
                        .find(|r| r.as_rule() == Rule::relation_expr);
                    if let Some(b) = rel.as_ref().and_then(|r| lone(r, Rule::variable)) {
                        let span = (pairs[i].as_span().start(), next.as_span().end());
                        return Err(bare_edge(input, span, a.as_str(), "from", b.as_str()));
                    }
                }
            }
            // `$a let $b`, `$a true $b`, `$a false $b`.
            if let (Some(mid), Some(b)) = (
                pairs.get(i + 1).and_then(kind),
                pairs.get(i + 2).and_then(lone_var_filter),
            ) {
                if mid.as_rule() == Rule::filter3 {
                    let word = lone(&mid, Rule::bare_name_rec)
                        .map(|w| w.as_str().to_string())
                        .or_else(|| lone(&mid, Rule::bool_lit).map(|w| w.as_str().to_string()));
                    if let Some(word) =
                        word.filter(|w| ["let", "true", "false"].contains(&w.as_str()))
                    {
                        let span = (pairs[i].as_span().start(), pairs[i + 2].as_span().end());
                        return Err(bare_edge(input, span, a.as_str(), &word, b.as_str()));
                    }
                }
            }
        }
        if let Some(k) = kind(&pairs[i]) {
            if k.as_rule() == Rule::with_rec {
                return Err(with_clause(input, &pairs[..i], &k));
            }
        }
    }
    pairs.into_iter().map(clause).collect()
}

fn bare_edge(input: &str, span: (usize, usize), a: &str, word: &str, b: &str) -> Refusal3 {
    refusal(
        input,
        Q017,
        format!(
            "`{word}` is never a bare edge name: between two variables it would read as a clause or a literal; an edge named `{word}` is quoted"
        ),
        span,
        vec![edit(span.0, span.1, format!("{a} \"{word}\" {b}"))],
        REVIEW,
    )
}

/// Cypher's `… WITH x, agg AS n …`: the clauses before it become an inline
/// read whose `return` is the `with` items, drawn with `from`.
fn with_clause(input: &str, before: &[Pair<Rule>], with: &Pair<Rule>) -> Refusal3 {
    let items: Vec<&str> = with
        .clone()
        .into_inner()
        .filter(|p| p.as_rule() == Rule::with_item)
        .map(|p| p.as_str().trim())
        .collect();
    let message = "`with` is Cypher's; GQ draws an intermediate result with `from { match { … } return { … } }`";
    let Some(first) = before.first() else {
        return refusal_note(
            input,
            Q014,
            message,
            span_of(with),
            "from { match { … } return { … } }",
        );
    };
    let start = first.as_span().start();
    let clauses = input[start..with.as_span().start()].trim();
    let span = (start, with.as_span().end());
    refusal(
        input,
        Q014,
        message,
        span,
        vec![edit(
            span.0,
            span.1,
            format!(
                "from {{ match {{ {clauses} }} return {{ {} }} }}",
                items.join(", ")
            ),
        )],
        MACHINE,
    )
}

/// Merge an inner refusal's edits into an outer recognizer's edits, so one
/// suggestion repairs both (the outer construct is the span).
fn compose_inner(outer: Vec<TextEdit>, inner: Result<Clause3, Refusal3>) -> R<Vec<TextEdit>> {
    match inner {
        Ok(_) => Ok(outer),
        Err(r) if !r.edits().is_empty() => {
            let mut all = outer;
            for e in r.edits() {
                if !all.iter().any(|o| o.start < e.end && e.start < o.end) {
                    all.push(e.clone());
                }
            }
            Ok(all)
        }
        Err(r) => Err(r),
    }
}

fn inner_clauses<'i>(p: &Pair<'i, Rule>) -> Vec<Pair<'i, Rule>> {
    p.clone()
        .into_inner()
        .filter(|c| matches!(c.as_rule(), Rule::clause3 | Rule::stmt_edge_rec))
        .collect()
}

fn clause(pair: Pair<Rule>) -> R<Clause3> {
    let input = input_of(&pair);
    let c = kind(&pair).unwrap();
    let span = span_of(&c);
    match c.as_rule() {
        Rule::let_clause => {
            let mut inner = c.into_inner().filter(|p| p.as_rule() != Rule::kw_let);
            let name = inner.next().unwrap().as_str()[1..].to_string();
            let value = inner.next().unwrap();
            let value = match value.as_rule() {
                Rule::inline_read => LetValue3::Relation(Box::new(inline_read(value)?)),
                _ => {
                    let start = value.as_span().start();
                    LetValue3::Expr(at_top(Top::Clause, start, || expr(value))?)
                }
            };
            Ok(Clause3::Let { name, value })
        }
        Rule::from_clause => {
            let rel = c
                .into_inner()
                .find(|p| p.as_rule() == Rule::relation_expr)
                .unwrap();
            Ok(Clause3::From(relation(rel)?))
        }
        Rule::from_paren_rec => {
            let rel = c
                .clone()
                .into_inner()
                .find(|p| p.as_rule() == Rule::relation_expr)
                .unwrap();
            Err(refusal(
                input,
                Q019,
                "`from` takes the relation itself, not in parentheses",
                span,
                vec![edit(
                    span.0,
                    span.1,
                    format!("from {}", rel.as_str().trim()),
                )],
                MACHINE,
            ))
        }
        Rule::negation3 => Ok(Clause3::Not(clause_list(inner_clauses(&c))?)),
        Rule::optional_block => Ok(Clause3::Optional(clause_list(inner_clauses(&c))?)),
        Rule::binding3 => {
            let mut inner = c.into_inner();
            let variable = inner.next().unwrap().as_str()[1..].to_string();
            let type_name = inner.next().unwrap().as_str().to_string();
            let mut prop_matches = Vec::new();
            if let Some(list) = inner.next() {
                for pm in list.into_inner() {
                    let mut kv = pm.into_inner();
                    let key = kv.next().unwrap().as_str().to_string();
                    prop_matches.push((key, expr(kv.next().unwrap())?));
                }
            }
            Ok(Clause3::Binding {
                variable,
                type_name,
                prop_matches,
            })
        }
        Rule::binding_brace_filter => Err(brace_filter(c)),
        Rule::binding_paren_rec => {
            let mut inner = c.clone().into_inner();
            let var = inner.next().unwrap().as_str();
            let head = inner.next().unwrap();
            let ty = head.as_str().trim_end_matches('(').trim();
            let args = inner.next();
            Err(match args.as_ref().map(|a| a.as_rule()) {
                Some(Rule::call_args) => {
                    let name = lower_first(ty);
                    refusal(
                        input,
                        Q014,
                        "the view binding `$x: View(args)` was withdrawn: a declared read is drawn with `from name(args)`",
                        span,
                        vec![edit(
                            span.0,
                            span.1,
                            format!("from {name}({})", args.unwrap().as_str().trim()),
                        )],
                        REVIEW,
                    )
                }
                _ => refusal(
                    input,
                    Q014,
                    "a binding's property matches are in braces, `$x: Type { property: value }`",
                    span,
                    vec![edit(
                        span.0,
                        span.1,
                        format!(
                            "{var}: {ty} {{ {} }}",
                            args.map(|a| a.as_str().trim().to_string())
                                .unwrap_or_default()
                        ),
                    )],
                    MACHINE,
                ),
            })
        }
        Rule::binding_lower_rec => {
            let ident = c.clone().into_inner().nth(1).unwrap();
            let (s, e) = span_of(&ident);
            Err(refusal(
                input,
                Q014,
                format!(
                    "a type name starts with an uppercase letter; `{}` is a property or edge name",
                    ident.as_str()
                ),
                span,
                vec![edit(s, e, upper_first(ident.as_str()))],
                REVIEW,
            ))
        }
        Rule::traversal3 => Ok(Clause3::Traversal(TraversalEq(traversal(c)?))),
        Rule::stmt_edge_rec => {
            let mut parts = c.clone().into_inner();
            let a = parts.next().unwrap().as_str();
            let word = parts.next().unwrap().as_str();
            let b = parts.next().unwrap().as_str();
            Err(bare_edge(input, span, a, word, b))
        }
        Rule::arrow_traversal => Err(arrow(c)),
        Rule::cypher_pattern => Err(cypher_pattern(c)),
        Rule::list_to_rows => Err(list_to_rows(c)),
        Rule::prefix_rec | Rule::match_prefix_rec => {
            let word = c.clone().into_inner().next().unwrap();
            let inner = c.clone().into_inner().nth(1).unwrap();
            let (ws, we) = with_trailing_ws(input, word.as_span().start(), word.as_span().end());
            let edits = compose_inner(vec![edit(ws, we, "")], clause(inner))?;
            let w = word.as_str().to_ascii_lowercase();
            let message = if w == "match" {
                "`match` wraps a read's clauses once; inside it, and in a write body, clauses are written bare".to_string()
            } else {
                format!("`{w}` is not GQ: a filter is a clause of its own, written bare")
            };
            Err(refusal(input, Q014, message, span, edits, MACHINE))
        }
        Rule::with_rec => unreachable!("clause_list refuses `with` first"),
        Rule::call_block_rec => {
            let word = c.clone().into_inner().next().unwrap();
            let (s, e) = span_of(&word);
            Err(refusal(
                input,
                Q014,
                "`call { … }` is Cypher's; GQ draws the rows of an inline read with `from { … }`",
                span,
                vec![edit(s, e, "from")],
                MACHINE,
            ))
        }
        Rule::optional_rec => {
            let mut edits = Vec::new();
            let mut parts = c.clone().into_inner().peekable();
            let word = parts.next().unwrap();
            if word.as_str() != "optional" {
                edits.push(edit(
                    word.as_span().start(),
                    word.as_span().end(),
                    "optional",
                ));
            }
            let rest: Vec<Pair<Rule>> = parts.collect();
            if let Some(m) = rest.iter().find(|p| p.as_rule() == Rule::fw_match) {
                let (s, e) = with_trailing_ws(input, m.as_span().start(), m.as_span().end());
                edits.push(edit(s, e, ""));
            }
            let clauses: Vec<&Pair<Rule>> = rest
                .iter()
                .filter(|p| p.as_rule() == Rule::clause3)
                .collect();
            let braced = input[word.as_span().end()..].trim_start().starts_with('{')
                || rest.iter().any(|p| p.as_rule() == Rule::fw_match)
                    && input[rest[0].as_span().end()..]
                        .trim_start()
                        .starts_with('{');
            if !braced {
                if let Some(cl) = clauses.first() {
                    let (s, e) = span_of(cl);
                    edits.push(edit(s, s, "{ "));
                    edits.push(edit(e, e, " }"));
                }
            }
            let mut all = edits;
            for cl in clauses {
                all = compose_inner(all, clause(cl.clone()))?;
            }
            Err(refusal(
                input,
                Q014,
                "`optional` takes its clauses in braces: `optional { … }`",
                span,
                all,
                MACHINE,
            ))
        }
        Rule::blockless_rec => {
            let word = c.clone().into_inner().next().unwrap();
            let inner = c.clone().into_inner().nth(1).unwrap();
            let (s, e) = span_of(&inner);
            let edits = vec![edit(s, s, "{ "), edit(e, e, " }")];
            let inner_clause = match inner.as_rule() {
                Rule::traversal3 => {
                    traversal(inner.clone()).map(|t| Clause3::Traversal(TraversalEq(t)))
                }
                Rule::arrow_traversal => Err(arrow(inner.clone())),
                Rule::cypher_pattern => Err(cypher_pattern(inner.clone())),
                _ => Ok(Clause3::Filter(Expr3::Literal(Literal::Bool(true)))),
            };
            let edits = compose_inner(edits, inner_clause)?;
            Err(refusal(
                input,
                Q014,
                format!(
                    "`{}` over a pattern takes it in braces: `{} {{ … }}`",
                    word.as_str(),
                    word.as_str()
                ),
                span,
                edits,
                MACHINE,
            ))
        }
        Rule::match_block_rec => {
            let kw = c.clone().into_inner().next().unwrap();
            let first = c.clone().into_inner().nth(1).unwrap();
            let open_end = first.as_span().start();
            let close = input[..span.1 - 1].trim_end().len();
            Err(refusal(
                input,
                Q005,
                "a write body's clauses are written bare, not inside `match { }`",
                span,
                vec![
                    edit(kw.as_span().start(), open_end, ""),
                    edit(close, span.1, ""),
                ],
                MACHINE,
            ))
        }
        Rule::detach_rec => {
            let (s, e) = with_trailing_ws(input, span.0, span.1);
            Err(refusal(
                input,
                Q014,
                "`detach` is Cypher's; GQ's `delete` of a node removes its edges",
                span,
                vec![edit(s, e, "")],
                MACHINE,
            ))
        }
        Rule::filter3 => {
            let e = c.into_inner().next().unwrap();
            let start = e.as_span().start();
            Ok(Clause3::Filter(at_top(Top::Clause, start, || {
                filter_expr(e)
            })?))
        }
        Rule::stray_braces_rec => Err(refusal_note(
            input,
            Q010,
            "property matches belong in a binding's braces, `$x: Type { property: value }`; a clause does not start with `{`",
            span,
            "$x: Type { <property>: <value> }",
        )),
        other => unreachable!("grammar: clause3 holds no {other:?}"),
    }
}

/// A filter clause; a lone bare name at clause start that is a clause
/// keyword gets that keyword's message.
fn filter_expr(e: Pair<Rule>) -> R<Expr3> {
    let input = input_of(&e);
    if let Some(word) = lone(&e, Rule::bare_name_rec) {
        let w = word.as_str();
        let span = span_of(&word);
        let note = match w {
            "from" => Some((
                "`from` takes a relation: an inline read `{ match { … } return { … } }`, `$r`, or a call whose name touches its `(`",
                "from { match { … } return { … } }",
            )),
            "let" => Some((
                "`let` names a value: `let $name = <value>`",
                "let $name = <value>",
            )),
            "optional" | "exists" | "not" => Some((
                "this word takes a block of clauses in braces",
                "optional { … }",
            )),
            "match" => {
                let (s, e2) = with_trailing_ws(input, span.0, span.1);
                return Err(refusal(
                    input,
                    Q005,
                    "a write body's clauses are written bare; `match` belongs to a read",
                    span,
                    vec![edit(s, e2, "")],
                    MACHINE,
                ));
            }
            _ => None,
        };
        if let Some((message, note)) = note {
            return Err(refusal_note(input, Q018, message, span, note));
        }
    }
    expr(e)
}

fn lower_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_ascii_lowercase().to_string() + c.as_str(),
        None => String::new(),
    }
}

fn upper_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_ascii_uppercase().to_string() + c.as_str(),
        None => String::new(),
    }
}

/// A Cypher relationship type as a GQ edge name: `KNOWS` → `knows`,
/// `WORKS_AT` → `worksAt`, `Knows` → `knows`.
pub(crate) fn gq_edge_name(cypher: &str) -> String {
    if cypher.chars().any(|c| c.is_ascii_lowercase()) {
        return lower_first(cypher);
    }
    let mut out = String::new();
    for (i, part) in cypher.split('_').filter(|p| !p.is_empty()).enumerate() {
        let lower = part.to_ascii_lowercase();
        if i == 0 {
            out.push_str(&lower);
        } else {
            out.push_str(&upper_first(&lower));
        }
    }
    out
}

fn cypher_hops(p: &Pair<Rule>) -> String {
    let text = p.as_str().trim_start_matches('*');
    let (lo, hi) = match text.split_once("..") {
        Some((a, b)) => (a.trim(), b.trim()),
        None => (text.trim(), text.trim()),
    };
    let lo = if lo.is_empty() { "1" } else { lo };
    format!("{{{lo},{hi}}}")
}

/// `$a -e-> $b`, `$a <-e- $b`, `$a -e- $b` and Cypher's `-[:E]->`: a parse
/// error whose fix is the triple.
fn arrow(c: Pair<Rule>) -> Refusal3 {
    let input = input_of(&c);
    let span = span_of(&c);
    let mut vars = Vec::new();
    let mut open = "";
    let mut close = "";
    let mut edge = String::new();
    let mut bounds = String::new();
    let mut binding = String::new();
    for part in c.into_inner() {
        match part.as_rule() {
            Rule::variable => vars.push(part.as_str()),
            Rule::arrow_open => open = part.as_str(),
            Rule::arrow_close => close = part.as_str(),
            Rule::edge_binding => binding = part.as_str().replace(' ', ""),
            Rule::edge_sel3 => edge = part.as_str().to_string(),
            Rule::traversal_bounds => bounds = part.as_str().to_string(),
            Rule::cypher_rel_body => {
                for q in part.into_inner() {
                    match q.as_rule() {
                        Rule::ident => binding = format!("${}:", q.as_str()),
                        Rule::cypher_name => edge = gq_edge_name(q.as_str()),
                        Rule::cypher_hops => bounds = cypher_hops(&q),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    let (a, b) = (vars[0], vars[1]);
    let fix = match (open, close) {
        ("-", "->") => format!("{a} {binding}{edge}{bounds} {b}"),
        ("<-", "-") => format!("{b} {binding}{edge}{bounds} {a}"),
        _ => format!("{a} {binding}<{edge}>{bounds} {b}"),
    };
    if edge.is_empty() {
        return refusal_note(
            input,
            Q007,
            "a traversal is a triple: the source, the edge and the target, with no arrow",
            span,
            format!("{a} <edge> {b}"),
        );
    }
    refusal(
        input,
        Q007,
        "a traversal is a triple: the source, the edge and the target, with no arrow",
        span,
        vec![edit(span.0, span.1, fix)],
        MACHINE,
    )
}

/// Cypher's `(a:Person)-[:KNOWS]->(b)`: the bindings and triples it means.
fn cypher_pattern(c: Pair<Rule>) -> Refusal3 {
    let input = input_of(&c);
    let span = span_of(&c);
    let mut out: Vec<String> = Vec::new();
    let mut nodes: Vec<String> = Vec::new();
    let mut rels: Vec<(String, String, String, String)> = Vec::new();
    let mut anon = 0;
    for part in c.into_inner() {
        match part.as_rule() {
            Rule::cypher_node | Rule::cypher_typed_node => {
                let mut name = None;
                let mut ty = None;
                let mut props = Vec::new();
                for q in part.into_inner() {
                    match q.as_rule() {
                        Rule::ident => name = Some(q.as_str().to_string()),
                        Rule::cypher_name => ty = Some(q.as_str().to_string()),
                        Rule::cypher_prop => props.push(q.as_str().trim().to_string()),
                        _ => {}
                    }
                }
                let name = name.unwrap_or_else(|| {
                    anon += 1;
                    format!("n{anon}")
                });
                match (ty, props.is_empty()) {
                    (Some(t), true) => out.push(format!("${name}: {t}")),
                    (Some(t), false) => {
                        out.push(format!("${name}: {t} {{ {} }}", props.join(", ")))
                    }
                    (None, _) => {}
                }
                nodes.push(name);
            }
            Rule::cypher_rel => {
                let mut open = String::new();
                let mut close = String::new();
                let mut edge = String::new();
                let mut hops = String::new();
                for q in part.into_inner() {
                    match q.as_rule() {
                        Rule::arrow_open => open = q.as_str().to_string(),
                        Rule::arrow_close => close = q.as_str().to_string(),
                        Rule::cypher_rel_body => {
                            for r in q.into_inner() {
                                match r.as_rule() {
                                    Rule::cypher_name => edge = gq_edge_name(r.as_str()),
                                    Rule::cypher_hops => hops = cypher_hops(&r),
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
                rels.push((open, close, edge, hops));
            }
            _ => {}
        }
    }
    let message = "a graph pattern in GQ is bindings and triples over `$` variables: `$a: Person  $a knows $b`";
    for (i, (open, close, edge, hops)) in rels.iter().enumerate() {
        if edge.is_empty() {
            return refusal_note(input, Q014, message, span, "$a: Type  $a <edge> $b");
        }
        let (a, b) = (&nodes[i], &nodes[i + 1]);
        out.push(match (open.as_str(), close.as_str()) {
            ("-", "->") => format!("${a} {edge}{hops} ${b}"),
            ("<-", "-") => format!("${b} {edge}{hops} ${a}"),
            _ => format!("${a} <{edge}>{hops} ${b}"),
        });
    }
    refusal(
        input,
        Q014,
        message,
        span,
        vec![edit(span.0, span.1, out.join("  "))],
        MACHINE,
    )
}

fn list_to_rows(c: Pair<Rule>) -> Refusal3 {
    let input = input_of(&c);
    let span = span_of(&c);
    let first = c.clone().into_inner().next().unwrap();
    let written = match first.as_rule() {
        Rule::kw_for => "`for $x in …`",
        Rule::kw_unwind => "`unwind … as $x`",
        _ => "`let … in …`",
    };
    let message = format!(
        "{written} is not GQ: `let` names one value and `from` draws rows from a relation; a list has no row form"
    );
    let parts: Vec<Pair<Rule>> = c.clone().into_inner().collect();
    // `let … in rel` is always over a relation (section 3: the fix is
    // `from rel`); `for $x in` over an inline read too.
    let rel = parts
        .iter()
        .find(|p| p.as_rule() == Rule::inline_read)
        .or_else(|| {
            (first.as_rule() == Rule::kw_let)
                .then(|| parts.iter().rev().find(|p| p.as_rule() == Rule::expr3))
                .flatten()
        });
    if let Some(rel) = rel {
        return refusal(
            input,
            Q012,
            message,
            span,
            vec![edit(
                span.0,
                span.1,
                format!("from {}", rel.as_str().trim()),
            )],
            REVIEW,
        );
    }
    // Over a list there is no row form: the RFC's fix tests membership, and
    // the variable must then be bound by a clause of its own.
    let var = parts.iter().find(|p| p.as_rule() == Rule::variable);
    let list = parts.iter().find(|p| p.as_rule() == Rule::expr3);
    match (var, list) {
        (Some(v), Some(l)) => refusal(
            input,
            Q012,
            message,
            span,
            vec![edit(
                span.0,
                span.1,
                format!("{} in {}", v.as_str(), l.as_str().trim()),
            )],
            REVIEW,
        ),
        _ => refusal_note(input, Q012, message, span, "from <relation>"),
    }
}

/// `$d: Doc { year >= 2025 }`: the equalities stay in the braces, each
/// comparison becomes a filter clause on the bound variable.
fn brace_filter(c: Pair<Rule>) -> Refusal3 {
    let input = input_of(&c);
    let span = span_of(&c);
    let mut inner = c.clone().into_inner();
    let var = inner.next().unwrap().as_str();
    let ty = inner.next().unwrap().as_str();
    let body = inner.next().unwrap();
    let message =
        "the braces of a binding hold `property: value` matches; a comparison is a filter clause";
    if body.as_rule() != Rule::brace_items_rec {
        return refusal_note(
            input,
            Q010,
            "the braces of a binding hold `property: value` matches, each value a literal, parameter or expression",
            span,
            format!("{var}: {ty} {{ <property>: <value> }}"),
        );
    }
    let mut kept = Vec::new();
    let mut filters = Vec::new();
    for item in body.into_inner() {
        let it = item.into_inner().next().unwrap();
        match it.as_rule() {
            Rule::prop_match3 => kept.push(it.as_str().trim().to_string()),
            Rule::brace_compare | Rule::brace_colon_compare => {
                let mut parts = it.into_inner();
                let key = parts.next().unwrap().as_str();
                let op = parts.next().unwrap().as_str();
                let value = parts.next().unwrap().as_str().trim();
                filters.push(format!("{var}.{key} {op} {value}"));
            }
            _ => {}
        }
    }
    let binding = if kept.is_empty() {
        format!("{var}: {ty}")
    } else {
        format!("{var}: {ty} {{ {} }}", kept.join(", "))
    };
    refusal(
        input,
        Q010,
        message,
        span,
        vec![edit(
            span.0,
            span.1,
            format!("{binding}  {}", filters.join("  ")),
        )],
        MACHINE,
    )
}

fn traversal(pair: Pair<Rule>) -> R<Traversal> {
    let mut inner = pair.into_inner();
    let src = inner.next().unwrap().as_str()[1..].to_string();
    let mut next = inner.next().unwrap();
    let mut edge_binding = None;
    if next.as_rule() == Rule::edge_binding {
        edge_binding = Some(next.into_inner().next().unwrap().as_str()[1..].to_string());
        next = inner.next().unwrap();
    }
    let (selector, undirected) = match next.as_rule() {
        Rule::undirected_edge3 => (edge_selector(next.into_inner().next().unwrap())?, true),
        _ => (edge_selector(next)?, false),
    };
    let mut min_hops = 1;
    let mut max_hops = Some(1);
    let mut next = inner.next().unwrap();
    if next.as_rule() == Rule::traversal_bounds {
        let (min, max) = parse_traversal_bounds(next).map_err(into_diagnostic)?;
        min_hops = min;
        max_hops = max;
        next = inner.next().unwrap();
    }
    Ok(Traversal {
        src,
        selector,
        dst: next.as_str()[1..].to_string(),
        min_hops,
        max_hops,
        undirected,
        edge_binding,
    })
}

fn edge_selector(pair: Pair<Rule>) -> R<EdgeSelector> {
    match pair.as_rule() {
        Rule::edge_sel3 | Rule::edge_alternation3 => {
            edge_selector(pair.into_inner().next().unwrap())
        }
        Rule::edge_wildcard => Ok(EdgeSelector::Wildcard),
        Rule::edge_alternatives3 => Ok(EdgeSelector::Alternation(
            pair.into_inner().map(edge_name).collect::<R<Vec<_>>>()?,
        )),
        _ => Ok(EdgeSelector::Named(edge_name(pair)?)),
    }
}

fn edge_name(pair: Pair<Rule>) -> R<String> {
    match pair.as_rule() {
        Rule::catalogue_edge | Rule::edge_ident3 => Ok(pair.as_str().to_string()),
        _ => parse_edge_name(pair).map_err(into_diagnostic),
    }
}

fn into_diagnostic(error: CompilerError) -> Refusal3 {
    compiler_error_to_diagnostic(error, Position::at("", 0)).into()
}

fn inline_read(pair: Pair<Rule>) -> R<ReadBody3> {
    let input = input_of(&pair);
    let body = pair.into_inner().next().unwrap();
    if body.as_rule() == Rule::matchless_read_rec {
        let clauses: Vec<Pair<Rule>> = body
            .clone()
            .into_inner()
            .filter(|p| p.as_rule() == Rule::clause3)
            .collect();
        let start = clauses.first().unwrap().as_span().start();
        let end = clauses.last().unwrap().as_span().end();
        return Err(refusal(
            input,
            Q020,
            "an inline read begins with `match { … }`, as every read does",
            span_of(&body),
            vec![edit(start, start, "match { "), edit(end, end, " }")],
            MACHINE,
        ));
    }
    read_body(body)
}

fn relation(pair: Pair<Rule>) -> R<RelExpr3> {
    let r = pair.into_inner().next().unwrap();
    match r.as_rule() {
        Rule::inline_read => Ok(RelExpr3::Inline(Box::new(inline_read(r)?))),
        Rule::call => Ok(RelExpr3::Call(call(r)?)),
        Rule::spaced_call_rec => Err(spaced_call(r)),
        Rule::variable => Ok(RelExpr3::Variable(r.as_str()[1..].to_string())),
        other => unreachable!("grammar: relation_expr holds no {other:?}"),
    }
}

/// `name (…)`: the name touches its parenthesis.
fn spaced_call(r: Pair<Rule>) -> Refusal3 {
    let input = input_of(&r);
    let head = r.clone().into_inner().next().unwrap();
    let name = head.clone().into_inner().next().unwrap();
    let ne = name.as_span().end();
    let paren = head.as_span().end() - 1;
    refusal(
        input,
        Q019,
        format!(
            "a call's name touches its parenthesis: `{}(`; with a space, `{}` reads as a bare name",
            name.as_str(),
            name.as_str()
        ),
        span_of(&r),
        vec![edit(ne, paren, "")],
        MACHINE,
    )
}

fn call(pair: Pair<Rule>) -> R<Call3> {
    let mut name = String::new();
    let mut args = Vec::new();
    let mut options = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::call_head => {
                name = part.into_inner().next().unwrap().as_str().to_string();
            }
            Rule::call_args => {
                if let Some(mixed) = part
                    .clone()
                    .into_inner()
                    .find(|a| a.as_rule() == Rule::mixed_args_rec)
                {
                    return Err(mixed_args(mixed));
                }
                for a in part.into_inner() {
                    match a.as_rule() {
                        Rule::positional => {
                            let v = a.into_inner().next().unwrap();
                            args.push(match v.as_rule() {
                                Rule::inline_read => Arg3::Inline(Box::new(inline_read(v)?)),
                                _ => Arg3::Expr(expr(v)?),
                            });
                        }
                        Rule::call_option => {
                            let mut kv = a.into_inner();
                            let key = kv.next().unwrap().as_str().to_string();
                            let value = kv.next().unwrap();
                            // A whole option value may be a bare word, the
                            // option's enumerated value (`terms($q, mode: any)`,
                            // section 7's `option`).
                            let value = match lone(&value, Rule::bare_name_rec) {
                                Some(word) => Expr3::Ident(word.as_str().to_string()),
                                None => expr(value)?,
                            };
                            options.push((key, value));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(Call3 {
        name,
        args,
        options,
    })
}

/// An option before a positional argument: the positionals first, in order,
/// then the options.
fn mixed_args(mixed: Pair<Rule>) -> Refusal3 {
    let input = input_of(&mixed);
    let span = span_of(&mixed);
    let (pos, opts): (Vec<Pair<Rule>>, Vec<Pair<Rule>>) = mixed
        .clone()
        .into_inner()
        .partition(|p| p.as_rule() == Rule::positional);
    let text: Vec<&str> = pos
        .iter()
        .chain(opts.iter())
        .map(|p| p.as_str().trim())
        .collect();
    refusal(
        input,
        Q005,
        "a call's positional arguments come first, then its `name: value` options",
        span,
        vec![edit(span.0, span.1, text.join(", "))],
        MACHINE,
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

fn comp_op(op: &str) -> CompOp {
    match op {
        "=" => CompOp::Eq,
        "!=" => CompOp::Ne,
        ">" => CompOp::Gt,
        "<" => CompOp::Lt,
        ">=" => CompOp::Ge,
        _ => CompOp::Le,
    }
}

fn expr(pair: Pair<Rule>) -> R<Expr3> {
    let input = input_of(&pair);
    match pair.as_rule() {
        Rule::expr3 => expr(pair.into_inner().next().unwrap()),
        Rule::or_expr3 | Rule::and_expr3 => {
            let is_or = pair.as_rule() == Rule::or_expr3;
            let mut operands = pair
                .into_inner()
                .filter(|p| !matches!(p.as_rule(), Rule::kw_and | Rule::kw_or));
            let first = expr(operands.next().unwrap())?;
            operands.try_fold(first, |left, right| {
                let right = Box::new(expr(right)?);
                Ok(if is_or {
                    Expr3::Or(Box::new(left), right)
                } else {
                    Expr3::And(Box::new(left), right)
                })
            })
        }
        Rule::not_expr3 => {
            let mut parts = pair.into_inner();
            let first = parts.next().unwrap();
            if first.as_rule() == Rule::kw_not {
                Ok(Expr3::Not(Box::new(expr(parts.next().unwrap())?)))
            } else {
                expr(first)
            }
        }
        Rule::comparison3 => comparison(pair),
        Rule::sum3 | Rule::product3 => arithmetic(input, pair),
        Rule::unary3 => {
            let mut parts = pair.into_inner();
            let first = parts.next().unwrap();
            if first.as_rule() == Rule::unary_minus {
                Ok(Expr3::Neg(Box::new(expr(parts.next().unwrap())?)))
            } else {
                expr(first)
            }
        }
        Rule::operand3 => operand(pair),
        other => unreachable!("grammar: no expression rule {other:?}"),
    }
}

fn comparison(pair: Pair<Rule>) -> R<Expr3> {
    let input = input_of(&pair);
    let span = span_of(&pair);
    let parts: Vec<Pair<Rule>> = pair.into_inner().collect();
    let left_pair = parts[0].clone();
    let left_source = left_pair.as_str().trim().to_string();
    let Some(op) = parts.get(1).cloned() else {
        return expr(left_pair);
    };
    let whole = |text: String, message: &str, code: QueryCode, app: Applicability| -> R<Expr3> {
        Err(refusal(
            input,
            code,
            message.to_string(),
            span,
            vec![edit(span.0, span.1, text)],
            app,
        ))
    };
    match op.as_rule() {
        Rule::comp_op => {
            let right_pair = parts[2].clone();
            if let Some(chain) = parts.get(3).filter(|p| p.as_rule() == Rule::chained_rec) {
                let mut terms = vec![format!(
                    "{left_source} {} {}",
                    op.as_str(),
                    right_pair.as_str().trim()
                )];
                let mut prev = right_pair.as_str().trim().to_string();
                let items: Vec<Pair<Rule>> = chain.clone().into_inner().collect();
                for pair in items.chunks(2) {
                    let r = pair[1].as_str().trim().to_string();
                    terms.push(format!("{prev} {} {r}", pair[0].as_str()));
                    prev = r;
                }
                return whole(
                    terms.join(" and "),
                    "a comparison is not chained; write each with `and` (a chained `<` … `>` would read as an undirected traversal)",
                    Q012,
                    MACHINE,
                );
            }
            Ok(Expr3::Compare {
                op: comp_op(op.as_str()),
                left: Box::new(expr(left_pair)?),
                right: Box::new(expr(right_pair)?),
            })
        }
        Rule::kw_in => {
            let list = parts[2].clone();
            if list.as_rule() == Rule::in_paren_rec {
                let items: Vec<&str> = list
                    .clone()
                    .into_inner()
                    .map(|p| p.as_str().trim())
                    .collect();
                let (s, e) = span_of(&list);
                return Err(refusal(
                    input,
                    Q014,
                    "a list is written in brackets, `[a, b]`",
                    span,
                    vec![edit(s, e, format!("[{}]", items.join(", ")))],
                    MACHINE,
                ));
            }
            let list_source = list.as_str().trim().to_string();
            Ok(Expr3::In {
                needle: Box::new(expr(left_pair)?),
                list: Box::new(expr(list)?),
                at: SourceSpan {
                    start: span.0,
                    end: span.0 + input[span.0..span.1].trim_end().len(),
                    position: Position::at(input, span.0),
                    operands: vec![left_source.clone(), list_source],
                },
            })
        }
        Rule::null_compare_rec => {
            let negated = op.as_str().starts_with("!=");
            whole(
                format!("{left_source} is {}null", if negated { "not " } else { "" }),
                "a read has no null literal; a null test is `is null` or `is not null`",
                Q014,
                MACHINE,
            )
        }
        Rule::regex_rec => Err(refusal_note(
            input,
            Q014,
            "`=~` is not GQ; the string registry rows are `contains`, `starts_with` and `match_text`",
            span,
            format!("contains({left_source}, \"…\")"),
        )),
        Rule::not_in_rec => {
            let right = op.clone().into_inner().nth(2).unwrap();
            whole(
                format!("not {left_source} in {}", right.as_str().trim()),
                "`not` comes before the comparison: `not x in list`",
                Q014,
                MACHINE,
            )
        }
        Rule::between_rec => {
            let mut b = op.clone().into_inner();
            let lo = b.nth(1).unwrap().as_str().trim().to_string();
            let hi = b.nth(1).unwrap().as_str().trim().to_string();
            whole(
                format!("{left_source} >= {lo} and {left_source} <= {hi}"),
                "`between` is SQL's; GQ writes the two comparisons",
                Q014,
                MACHINE,
            )
        }
        Rule::like_rec => {
            let pat = op.clone().into_inner().nth(1).unwrap();
            let text = pat.as_str().trim();
            let inner = text.strip_prefix('"').and_then(|t| t.strip_suffix('"'));
            let fix = inner.and_then(|t| {
                let body = t.trim_start_matches('%').trim_end_matches('%');
                if body.contains('%') || body.contains('_') {
                    return None;
                }
                match (t.starts_with('%'), t.ends_with('%')) {
                    (true, true) => Some(format!("contains({left_source}, \"{body}\")")),
                    (false, true) => Some(format!("starts_with({left_source}, \"{body}\")")),
                    _ => None,
                }
            });
            match fix {
                Some(f) => whole(
                    f,
                    "`like` is SQL's; the string registry rows are `contains` and `starts_with`",
                    Q014,
                    MACHINE,
                ),
                None => Err(refusal_note(
                    input,
                    Q014,
                    "`like` is SQL's; the string registry rows are `contains` and `starts_with`",
                    span,
                    format!("contains({left_source}, \"…\")"),
                )),
            }
        }
        Rule::starts_with_words_rec => {
            let mut w = op.clone().into_inner();
            let word = w.next().unwrap().as_str().to_ascii_lowercase();
            let right = w.nth(1).unwrap().as_str().trim().to_string();
            let func = if word == "ends" {
                "ends_with"
            } else {
                "starts_with"
            };
            whole(
                format!("{func}({left_source}, {right})"),
                &format!("`{word} with` is Cypher's; GQ calls `{func}`"),
                Q014,
                if func == "ends_with" { REVIEW } else { MACHINE },
            )
        }
        Rule::infix_string_op => Ok(Expr3::InfixString {
            op: if op.as_str() == "contains" {
                InfixStringOp::Contains
            } else {
                InfixStringOp::StartsWith
            },
            left: Box::new(expr(left_pair)?),
            right: Box::new(expr(parts[2].clone())?),
            at: SourceSpan {
                start: span.0,
                end: span.0 + input[span.0..span.1].trim_end().len(),
                position: Position::at(input, span.0),
                operands: vec![
                    left_source.to_string(),
                    parts[2].as_str().trim().to_string(),
                ],
            },
        }),
        _ => Ok(Expr3::IsNull {
            expr: Box::new(expr(left_pair)?),
            negated: op.into_inner().any(|k| k.as_rule() == Rule::kw_not),
        }),
    }
}

fn arithmetic(input: &str, pair: Pair<Rule>) -> R<Expr3> {
    let span = span_of(&pair);
    let parts: Vec<Pair<Rule>> = pair.into_inner().collect();
    // `a || b || c`: one concat call over the flattened chain.
    if parts.iter().any(|p| p.as_str() == "||") {
        let ops_all_bars = parts.iter().skip(1).step_by(2).all(|p| p.as_str() == "||");
        let operands: Vec<&str> = parts.iter().step_by(2).map(|p| p.as_str().trim()).collect();
        return Err(if ops_all_bars {
            refusal(
                input,
                Q012,
                "`||` is not an operator: concatenation is the registry call `concat`",
                span,
                vec![edit(
                    span.0,
                    span.1,
                    format!("concat({})", operands.join(", ")),
                )],
                MACHINE,
            )
        } else {
            refusal_note(
                input,
                Q012,
                "`||` is not an operator: concatenation is the registry call `concat`",
                span,
                "concat(a, b, …)",
            )
        });
    }
    let top = with_ctx(|c| c.top).filter(|(_, start)| *start == span.0);
    let mut it = parts.into_iter();
    let first = it.next().unwrap();
    // A pest span can end after the implicit whitespace a trailing
    // repetition skipped; the operand ends at its last token.
    let trimmed_end = |p: &Pair<Rule>| input[..p.as_span().end()].trim_end().len();
    let mut left_end = trimmed_end(&first);
    let mut left = expr(first)?;
    while let Some(op) = it.next() {
        let right_pair = it.next().unwrap();
        let right_end = trimmed_end(&right_pair);
        let right = expr(right_pair)?;
        let symbol = op.as_str();
        if symbol == "-" {
            let at = op.as_span().start();
            let gap = &input[left_end..at];
            let before = input[..at]
                .chars()
                .next_back()
                .is_some_and(char::is_whitespace);
            let after = input[at + 1..]
                .chars()
                .next()
                .is_some_and(char::is_whitespace);
            if before != after {
                // A gap that separates clauses (a line break or two spaces)
                // before a `-` touching its operand reads as a new item or
                // clause; one space as a subtraction (section 3's gap rule).
                let separator = gap.contains('\n') || gap.len() >= 2;
                let edits = match (before, separator, top) {
                    (true, true, Some((Top::Item, _))) => vec![edit(left_end, left_end, ",")],
                    (true, true, Some((Top::Clause, _))) => {
                        vec![edit(at, at, "("), edit(right_end, right_end, ")")]
                    }
                    (true, _, _) => vec![edit(at + 1, at + 1, " ")],
                    (false, _, _) => vec![edit(at, at, " ")],
                };
                return Err(refusal(
                    input,
                    Q016,
                    "a binary `-` has whitespace on both sides or neither; `$n -1` would otherwise merge two items or two clauses, or read as a sign depending on the clause before it",
                    span,
                    edits,
                    REVIEW,
                ));
            }
        }
        left_end = right_end;
        let op = match symbol {
            "+" => ArithOp::Add,
            "-" => ArithOp::Sub,
            "*" => ArithOp::Mul,
            "/" => ArithOp::Div,
            _ => ArithOp::Rem,
        };
        left = Expr3::Arith {
            op,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

/// A bare name in operand position: refused at the word, with a guess at
/// what was meant when the declaration gives one.
fn bare_name(word: Pair<Rule>) -> Refusal3 {
    let input = input_of(&word);
    let span = span_of(&word);
    let w = word.as_str();
    let fixed: Option<(&str, &str)> = match w {
        "where" | "filter" => Some(("drop it: a filter is a clause of its own", "")),
        "distinct" => None,
        _ => None,
    };
    if let Some((message, replacement)) = fixed {
        let (s, e) = with_trailing_ws(input, span.0, span.1);
        return refusal(
            input,
            Q014,
            format!("`{w}` is not GQ; {message}"),
            span,
            vec![edit(s, e, replacement)],
            MACHINE,
        );
    }
    if w == "as" {
        return refusal_note(
            input,
            Q014,
            "`as` names a return item's column (`<item> as name`); a binding names its variable with `$`, `$p: Person`",
            span,
            "remove it",
        );
    }
    let stage = match w {
        "offset" | "skip" => Some("paging, stage P"),
        "returning" => Some("the writes RFC, stage W1"),
        _ => None,
    };
    if let Some(stage) = stage {
        return refusal_note(
            input,
            Q015,
            format!("`{w}` is {stage}; it has no meaning in this release"),
            span,
            "remove it",
        );
    }
    let (param, single) = with_ctx(|c| {
        (
            c.params.iter().any(|p| p == w),
            (c.bindings.len() == 1).then(|| c.bindings[0].0.clone()),
        )
    });
    let message = format!(
        "`{w}` is a bare name, which is not a value in a read: a property is `$x.{w}`, a parameter `${w}`, a string `\"{w}\"`; a bare name is a return alias in `order` only"
    );
    if param {
        return refusal(
            input,
            Q018,
            message,
            span,
            vec![edit(span.0, span.0, "$")],
            REVIEW,
        );
    }
    if let Some(v) = single {
        return refusal(
            input,
            Q018,
            message,
            span,
            vec![edit(span.0, span.0, format!("${v}."))],
            REVIEW,
        );
    }
    refusal_note(input, Q018, message, span, format!("$x.{w}"))
}

fn operand(pair: Pair<Rule>) -> R<Expr3> {
    let input = input_of(&pair);
    let inner = pair.into_inner().next().unwrap();
    let span = span_of(&inner);
    match inner.as_rule() {
        Rule::expr3 => expr(inner),
        Rule::block_value => {
            let mut func = None;
            let mut arg = None;
            let mut clauses = Vec::new();
            for part in inner.into_inner() {
                match part.as_rule() {
                    Rule::agg_name => func = Some(BlockFunc3::Agg(agg_func(part.as_str()))),
                    Rule::kw_exists => func = Some(BlockFunc3::Exists),
                    Rule::expr3 => arg = Some(Box::new(expr(part)?)),
                    Rule::subquery_block3 => clauses = clause_list(inner_clauses(&part))?,
                    _ => {}
                }
            }
            Ok(Expr3::Block(Box::new(Block3 {
                func: func.unwrap(),
                arg,
                clauses,
            })))
        }
        Rule::agg_distinct => {
            let mut parts = inner.into_inner();
            let func = agg_func(parts.next().unwrap().as_str());
            let arg = parts.find(|p| p.as_rule() == Rule::expr3).unwrap();
            Ok(Expr3::AggDistinct {
                func,
                arg: Box::new(expr(arg)?),
            })
        }
        Rule::count_star_rec => {
            let name = inner.clone().into_inner().next().unwrap();
            Err(match first_binding() {
                Some(v) => refusal(
                    input,
                    Q014,
                    format!(
                        "`{}(*)` is SQL's; an aggregate names what it counts",
                        name.as_str()
                    ),
                    span,
                    vec![edit(span.0, span.1, format!("{}(${v})", name.as_str()))],
                    REVIEW,
                ),
                None => refusal_note(
                    input,
                    Q014,
                    "`count(*)` is SQL's; an aggregate names what it counts",
                    span,
                    "count($x)",
                ),
            })
        }
        Rule::case_rec => {
            let mut whens: Vec<(String, String)> = Vec::new();
            let mut otherwise = None;
            let parts: Vec<Pair<Rule>> = inner.clone().into_inner().collect();
            let mut i = 0;
            while i < parts.len() {
                match parts[i].as_rule() {
                    Rule::fw_when => {
                        whens.push((
                            parts[i + 1].as_str().trim().to_string(),
                            parts[i + 3].as_str().trim().to_string(),
                        ));
                        i += 4;
                    }
                    Rule::fw_else => {
                        otherwise = Some(parts[i + 1].as_str().trim().to_string());
                        i += 2;
                    }
                    _ => i += 1,
                }
            }
            let message =
                "`CASE` is not GQ: a conditional is the registry call `if(condition, then, else)`";
            match otherwise {
                Some(e) => {
                    let text = whens
                        .iter()
                        .rev()
                        .fold(e, |acc, (c, t)| format!("if({c}, {t}, {acc})"));
                    Err(refusal(
                        input,
                        Q012,
                        message,
                        span,
                        vec![edit(span.0, span.1, text)],
                        MACHINE,
                    ))
                }
                None => Err(refusal_note(
                    input,
                    Q012,
                    message,
                    span,
                    "if(<condition>, <then>, <else>)",
                )),
            }
        }
        Rule::literal3 => Ok(Expr3::Literal(literal(inner)?)),
        Rule::type_prop_rec => {
            let mut parts = inner.clone().into_inner();
            let ty = parts.next().unwrap().as_str();
            let prop = parts.next().unwrap().as_str();
            let message = format!(
                "`{ty}.{prop}` reads a property through a type; GQ reads it through the bound variable"
            );
            Err(match binding_of_type(ty) {
                Some(v) => refusal(
                    input,
                    Q014,
                    message,
                    span,
                    vec![edit(span.0, span.1, format!("${v}.{prop}"))],
                    MACHINE,
                ),
                None => refusal_note(input, Q014, message, span, format!("$x: {ty} … $x.{prop}")),
            })
        }
        Rule::call => Ok(Expr3::Call(call(inner)?)),
        Rule::keyword_call_rec => {
            let head = inner.clone().into_inner().next().unwrap();
            let word = head.as_str().trim_end_matches('(');
            let args: Vec<Pair<Rule>> = inner
                .clone()
                .into_inner()
                .filter(|p| p.as_rule() == Rule::call_args)
                .flat_map(|a| a.into_inner())
                .collect();
            if word == "exists" && args.len() == 1 && args[0].as_rule() == Rule::positional {
                return Err(refusal(
                    input,
                    Q019,
                    "`exists(x)` is Cypher's property test; a registry row is never named `exists`, and the test is `x is not null`",
                    span,
                    vec![edit(
                        span.0,
                        span.1,
                        format!("{} is not null", args[0].as_str().trim()),
                    )],
                    MACHINE,
                ));
            }
            Err(refusal_note(
                input,
                Q019,
                format!("`{word}` is a clause keyword, and a registry row is never named one"),
                span,
                format!("{word} {{ … }}"),
            ))
        }
        Rule::spaced_call_rec => Err(spaced_call(inner)),
        Rule::prop_method_rec => {
            let head = inner.clone().into_inner().next().unwrap();
            let mut hp = head.clone().into_inner();
            let var = hp.next().unwrap().as_str();
            let method = hp.next().unwrap().as_str();
            let edge = inner
                .clone()
                .into_inner()
                .filter(|p| p.as_rule() == Rule::call_args)
                .flat_map(|a| a.into_inner())
                .collect::<Vec<_>>();
            let message = "a traversal is a triple, `$a edge $b`, not a method call on a variable";
            let edge_name = match edge.as_slice() {
                [one] => lone(
                    &one.clone().into_inner().next().unwrap(),
                    Rule::bare_name_rec,
                )
                .map(|w| w.as_str().to_string())
                .or_else(|| {
                    let t = one.as_str().trim();
                    (t.starts_with('"') && t.ends_with('"') && t.len() > 2)
                        .then(|| gq_edge_name(&t[1..t.len() - 1]))
                }),
                _ => None,
            };
            let to = fresh_name("x");
            let triple = edge_name.and_then(|e| match method {
                "out" | "outE" => Some(format!("{var} {e} ${to}")),
                "in" | "inE" => Some(format!("${to} {e} {var}")),
                "both" | "bothE" => Some(format!("{var} <{e}> ${to}")),
                _ => None,
            });
            Err(match triple {
                Some(t) => refusal(
                    input,
                    Q014,
                    message,
                    span,
                    vec![edit(span.0, span.1, t)],
                    REVIEW,
                ),
                None => refusal_note(input, Q014, message, span, "$a <edge> $b"),
            })
        }
        Rule::prop_chain_rec => Err(refusal_note(
            input,
            Q014,
            "a property path through an edge is a traversal clause, then a property of its target",
            span,
            "match { … $p <edge> $q } return { $q.<property> }",
        )),
        Rule::prop_star_rec => {
            let var = inner.clone().into_inner().next().unwrap();
            Err(refusal(
                input,
                Q014,
                "`$x.*` is not GQ; returning the node returns its properties",
                span,
                vec![edit(span.0, span.1, var.as_str())],
                MACHINE,
            ))
        }
        Rule::prop_access3 => {
            let mut parts = inner.into_inner();
            let variable = parts.next().unwrap().as_str()[1..].to_string();
            let property = parts.next().unwrap().as_str().to_string();
            Ok(Expr3::Prop { variable, property })
        }
        Rule::meta_field => Ok(Expr3::Meta(inner.as_str()[1..].to_string())),
        Rule::variable => Ok(Expr3::Variable(inner.as_str()[1..].to_string())),
        Rule::reserved_property if inner.as_str() == "null" => Err(refusal_note(
            input,
            Q005,
            "a read has no `null` literal; a null test is `x is null` or `x is not null`, and `null` is an assignment value only",
            span,
            "$x.<property> is null",
        )),
        Rule::reserved_property => Err(diagnostic_at(
            Q005,
            format!(
                "`{}` is a reserved word; a property of that name is written `$p.{}`",
                inner.as_str(),
                inner.as_str()
            ),
            &inner,
        )
        .into()),
        Rule::bare_name_rec => {
            if with_ctx(|c| c.allow_bare) {
                Ok(Expr3::Ident(inner.as_str().to_string()))
            } else {
                Err(bare_name(inner))
            }
        }
        other => unreachable!("grammar: operand3 holds no {other:?}"),
    }
}

fn literal(pair: Pair<Rule>) -> R<Literal> {
    let input = input_of(&pair);
    let inner = pair.into_inner().next().unwrap();
    let parse_err = |e: CompilerError, at: &Pair<Rule>| {
        Refusal3::from(compiler_error_to_diagnostic(e, position_of(at)))
    };
    Ok(match inner.as_rule() {
        Rule::string_lit => {
            Literal::String(parse_string_lit(&inner).map_err(|e| parse_err(e, &inner))?)
        }
        Rule::integer3 => {
            Literal::Integer(i128::from(inner.as_str().parse::<i64>().map_err(|e| {
                Refusal3::from(diagnostic_at(Q005, format!("invalid integer: {e}"), &inner))
            })?))
        }
        Rule::exp_float_rec => {
            let text = inner.as_str();
            let at = text.find(['e', 'E']).unwrap();
            let (s, _) = span_of(&inner);
            return Err(refusal(
                input,
                Q005,
                "an exponent needs a fraction: `2.0e3`, not `2e3`",
                span_of(&inner),
                vec![edit(s + at, s + at, ".0")],
                MACHINE,
            ));
        }
        Rule::float_lit3 => Literal::Float(inner.as_str().parse::<f64>().map_err(|e| {
            Refusal3::from(diagnostic_at(Q005, format!("invalid float: {e}"), &inner))
        })?),
        Rule::bool_lit => Literal::Bool(inner.as_str() == "true"),
        Rule::date_lit3 | Rule::datetime_lit3 => {
            let is_date = inner.as_rule() == Rule::date_lit3;
            let s = inner
                .clone()
                .into_inner()
                .find(|p| p.as_rule() == Rule::string_lit)
                .expect("grammar: a date literal holds a string");
            let text = parse_string_lit(&s).map_err(|e| parse_err(e, &s))?;
            if is_date {
                Literal::Date(text)
            } else {
                Literal::DateTime(text)
            }
        }
        Rule::list_lit3 => Literal::List(inner.into_inner().map(literal).collect::<R<Vec<_>>>()?),
        other => unreachable!("grammar: literal3 holds no {other:?}"),
    })
}
