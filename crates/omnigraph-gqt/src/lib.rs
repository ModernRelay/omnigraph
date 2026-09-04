//! GQ logic tests: the `.gqt` corpus under `cases/` and the runner that
//! executes one case against a fresh temporary store (init, load, index,
//! then the steps in order). The file format, refusal set, comparison
//! semantics, and bless workflow are specified in
//! `docs/rfcs/0045-gq-logic-tests.md`.
//!
//! The test target `tests/gq_logic_tests.rs` registers every case file as
//! its own test (`datatest-stable`); how cases are selected, listed, and
//! run concurrently is documented there. Every case runs under a per-case
//! wall-time budget (`OMNIGRAPH_GQ_CASE_TIMEOUT_SECS=<n>`, default 10) via
//! [`run_case_bounded`]. `OMNIGRAPH_GQ_BLESS=1` rewrites the failing
//! step's `--- expect` rows in place.
//!
//! Layout of the `run_query_step` future (an engine query under the traversal
//! task-local, the timeout, and `catch_unwind`) exceeds the default
//! `recursion_limit` on Linux CI; the same raise the other engine
//! integration tests carry.
#![recursion_limit = "512"]

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::FutureExt as _;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes, with_traversal_mode};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::query::ast::{Clause, Expr, Literal, Param, QueryDecl};
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::schema::ast::{Annotation, PropDecl, SchemaDecl};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{JsonParamMode, json_params_to_param_map};
use serde_json::Value;

pub const CASE_TIMEOUT_ENV: &str = "OMNIGRAPH_GQ_CASE_TIMEOUT_SECS";
pub const DEFAULT_CASE_TIMEOUT_SECS: u64 = 10;
pub const BLESS_ENV: &str = "OMNIGRAPH_GQ_BLESS";

#[derive(Debug)]
struct Case {
    schema: String,
    seed: String,
    /// The `# traversal:` pin; `None` runs the production path unscoped.
    traversal: Option<&'static str>,
    items: Vec<Item>,
    needs_indices: bool,
}

impl Case {
    fn has_loops(&self) -> bool {
        self.items.iter().any(|i| matches!(i, Item::Loop { .. }))
    }
}

#[derive(Debug)]
enum Item {
    Step(Step),
    Loop {
        var: String,
        values: Vec<String>,
        steps: Vec<Step>,
    },
}

#[derive(Debug)]
enum Step {
    Query(QueryStep),
    Mutate(MutateStep),
    Restart { ordinal: usize },
}

#[derive(Debug)]
struct QueryStep {
    ordinal: usize,
    source: String,
    name: String,
    ast_params: Vec<Param>,
    params_raw: Option<String>,
    expect: QueryExpect,
    /// The match clause carries an unbound traversal, so a successful run
    /// must show at least one Expand on the pinned path.
    expects_expand: bool,
}

#[derive(Debug)]
struct MutateStep {
    ordinal: usize,
    source: String,
    name: String,
    ast_params: Vec<Param>,
    params_raw: Option<String>,
    expect: MutateExpect,
}

#[derive(Debug)]
enum QueryExpect {
    Rows {
        ordered: bool,
        body_raw: String,
        span: BodySpan,
    },
    Error {
        needle: String,
    },
}

#[derive(Debug)]
enum MutateExpect {
    Ok,
    Affected { nodes: usize, edges: usize },
    Error { needle: String },
}

/// Line span of an expect section's body in the case file, for bless splicing.
#[derive(Debug, Clone, Copy)]
struct BodySpan {
    start_line: usize,
    len: usize,
}

#[derive(Debug)]
struct Header {
    issue: IssueRef,
    traversal: Option<&'static str>,
}

#[derive(Debug, PartialEq)]
enum IssueRef {
    None,
    Num(u64),
}

/// The four header keys, in the spelling the canonical form requires.
const HEADER_KEYS: [&str; 4] = ["issue", "red_on", "notes", "traversal"];

/// The one accepted spelling of a header line. A line is accepted exactly
/// when it equals this for some key in `HEADER_KEYS` and a value with no
/// leading or trailing whitespace: no continuation lines exist (a
/// multi-line note repeats `# notes:`), so a misspelled key has no prose
/// branch to fall into and is refused with the others.
fn canonical_header_line(key: &str, value: &str) -> String {
    format!("# {key}: {value}")
}

/// Splits a header line into its key and value, or names why it is not
/// canonical. `# ` and `: ` are matched literally, so the key cannot carry
/// whitespace; the value is refused when it does at either end. A line that
/// passes prints back to itself through `canonical_header_line`.
fn split_header_line(line: &str) -> Result<(&str, &str), String> {
    let Some(rest) = line.strip_prefix("# ") else {
        return Err("header line is not `# <key>: <value>`".into());
    };
    let Some((key, value)) = rest.split_once(": ") else {
        return Err("header line is not `# <key>: <value>`".into());
    };
    if !HEADER_KEYS.contains(&key) {
        return Err(format!(
            "unknown header key `{key}`; keys are {}",
            HEADER_KEYS.join(", ")
        ));
    }
    if value.trim().is_empty() {
        return Err(format!("`# {key}:` needs a value"));
    }
    if value.trim() != value {
        return Err(format!(
            "header value carries leading or trailing whitespace; write `{}`",
            canonical_header_line(key, value.trim())
        ));
    }
    debug_assert_eq!(canonical_header_line(key, value), line);
    Ok((key, value))
}

fn parse_header(lines: &[&str]) -> Result<Header, String> {
    let mut issue: Option<IssueRef> = None;
    let mut red_on = false;
    let mut traversal: Option<&'static str> = None;
    for (idx, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        if !line.starts_with('#') {
            return Err(format!(
                "line {}: only `#` header lines may precede the first section",
                idx + 1
            ));
        }
        let (key, value) = split_header_line(line).map_err(|e| format!("line {}: {e}", idx + 1))?;
        let duplicate = match key {
            "issue" => issue.is_some(),
            "red_on" => red_on,
            "traversal" => traversal.is_some(),
            _ => false,
        };
        if duplicate {
            return Err(format!("line {}: duplicate `# {key}:` header", idx + 1));
        }
        match key {
            "issue" => {
                issue = Some(if value == "none" {
                    IssueRef::None
                } else {
                    let n = value.parse::<u64>().map_err(|_| {
                        format!("line {}: `# issue:` takes a number or `none`", idx + 1)
                    })?;
                    if value != n.to_string() {
                        return Err(format!(
                            "line {}: `# issue:` number carries no sign or leading zeros",
                            idx + 1
                        ));
                    }
                    IssueRef::Num(n)
                });
            }
            "red_on" => red_on = true,
            "notes" => {}
            "traversal" => {
                traversal = Some(match value {
                    "indexed" => "indexed",
                    "csr" => "csr",
                    other => {
                        return Err(format!(
                            "line {}: `# traversal:` takes `indexed` or `csr`, got `{other}`",
                            idx + 1
                        ));
                    }
                });
            }
            other => unreachable!("split_header_line admits only HEADER_KEYS, got `{other}`"),
        }
    }
    let Some(issue) = issue else {
        return Err("missing required `# issue:` header".into());
    };
    if matches!(issue, IssueRef::Num(_)) && !red_on {
        return Err("`# red_on:` is required when `# issue:` names a number".into());
    }
    Ok(Header { issue, traversal })
}

fn check_file_name(stem: &str, issue: &IssueRef) -> Result<(), String> {
    if let Some(rest) = stem.strip_prefix("issue_") {
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        let tail = &rest[digits.len()..];
        let short = tail.strip_prefix('_').unwrap_or("");
        if digits.is_empty()
            || short.is_empty()
            || !short
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err("file name must be `issue_<N>_<short_name>.gqt`".into());
        }
        let n: u64 = digits
            .parse()
            .map_err(|_| "file name issue number does not parse".to_string())?;
        if digits != n.to_string() {
            return Err("file name issue number must not carry leading zeros".into());
        }
        if *issue != IssueRef::Num(n) {
            return Err(format!(
                "file name says issue {n} but the `# issue:` header disagrees"
            ));
        }
    } else {
        if stem.is_empty()
            || !stem
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err("feature case file names are `<short_name>.gqt` over [a-z0-9_]".into());
        }
        if let IssueRef::Num(n) = issue {
            return Err(format!(
                "`# issue: {n}` requires the file name `issue_{n}_<short_name>.gqt`"
            ));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Section<'a> {
    name: String,
    header_line: usize,
    body: Vec<(usize, &'a str)>,
}

fn split_sections<'a>(lines: &[&'a str]) -> (Vec<&'a str>, Vec<Section<'a>>) {
    let mut starts: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("--- "))
        .map(|(i, _)| i)
        .collect();
    let header_end = starts.first().copied().unwrap_or(lines.len());
    let header = lines[..header_end].to_vec();
    starts.push(lines.len());
    let mut sections = Vec::new();
    for pair in starts.windows(2) {
        let (start, end) = (pair[0], pair[1]);
        if start >= lines.len() {
            break;
        }
        sections.push(Section {
            name: lines[start]["--- ".len()..].trim_end().to_string(),
            header_line: start,
            body: (start + 1..end).map(|i| (i, lines[i])).collect(),
        });
    }
    (header, sections)
}

#[derive(Debug)]
enum ExpectHeader {
    Unordered,
    Ordered,
    Ok,
    Error(String),
    Affected { nodes: usize, edges: usize },
}

fn parse_expect_header(rest: &str) -> Result<ExpectHeader, String> {
    let rest = rest.trim();
    if rest.is_empty() {
        return Err("a bare `--- expect` is refused; give a mode word".into());
    }
    if rest == "unordered" {
        return Ok(ExpectHeader::Unordered);
    }
    if rest == "ordered" {
        return Ok(ExpectHeader::Ordered);
    }
    if rest == "ok" {
        return Ok(ExpectHeader::Ok);
    }
    if let Some(needle) = rest.strip_prefix("error:") {
        let needle = needle.trim();
        if needle.is_empty() {
            return Err(
                "`expect error:` needs a substring; a bare any-error expectation is refused".into(),
            );
        }
        return Ok(ExpectHeader::Error(needle.to_string()));
    }
    if let Some(counts) = rest.strip_prefix("affected:") {
        let parts: Vec<&str> = counts.split_whitespace().collect();
        let parsed = match parts.as_slice() {
            [n, e] => n
                .strip_prefix("nodes=")
                .zip(e.strip_prefix("edges="))
                .and_then(|(n, e)| parse_plain_count(n).zip(parse_plain_count(e))),
            _ => None,
        };
        let Some((nodes, edges)) = parsed else {
            return Err(
                "`expect affected:` must be exactly `affected: nodes=<N> edges=<M>`".into(),
            );
        };
        return Ok(ExpectHeader::Affected { nodes, edges });
    }
    Err(format!("unknown expect mode `{rest}`"))
}

/// Exact-spelling numeric token: digits only, no sign, no leading zeros.
fn parse_plain_count(token: &str) -> Option<usize> {
    let n: usize = token.parse().ok()?;
    (n.to_string() == token).then_some(n)
}

fn parse_loop_var(token: &str) -> Result<String, String> {
    let Some(name) = token.strip_prefix('$') else {
        return Err(format!("loop variable `{token}` must start with `$`"));
    };
    let mut chars = name.chars();
    let head_ok = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    if !head_ok
        || !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(format!(
            "loop variable `{token}` must match $[a-z][a-z0-9_]*"
        ));
    }
    Ok(name.to_string())
}

fn parse_loop_header(rest: &str) -> Result<(String, Vec<String>), String> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    let [var, start, end] = parts.as_slice() else {
        return Err("`--- loop` must be `loop $var <start> <end>`".into());
    };
    let var = parse_loop_var(var)?;
    for bound in [start, end] {
        if bound.starts_with('-') {
            return Err("loop bounds must be non-negative".into());
        }
    }
    let start = parse_plain_bound(start)?;
    let end = parse_plain_bound(end)?;
    if start >= end {
        return Err("empty loop range is refused; zero iterations assert nothing".into());
    }
    if end - start > 10_000 {
        return Err(format!(
            "loop range of {} iterations exceeds the 10000 cap; a case needing more stays a Rust test",
            end - start
        ));
    }
    Ok((var, (start..end).map(|i| i.to_string()).collect()))
}

fn parse_plain_bound(token: &str) -> Result<u64, String> {
    let n: u64 = token
        .parse()
        .map_err(|_| "loop bounds must be plain decimal integers".to_string())?;
    if n.to_string() != token {
        return Err("loop bounds must be plain decimal integers".into());
    }
    Ok(n)
}

fn parse_foreach_header(rest: &str) -> Result<(String, Vec<String>), String> {
    let mut parts = rest.split_whitespace();
    let Some(var) = parts.next() else {
        return Err("`--- foreach` must be `foreach $var <v1> [<v2> ...]`".into());
    };
    let var = parse_loop_var(var)?;
    let values: Vec<String> = parts.map(str::to_string).collect();
    if values.is_empty() {
        return Err(
            "a `--- foreach` with no values is refused; zero iterations assert nothing".into(),
        );
    }
    for v in &values {
        if !v
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
        {
            return Err(format!(
                "foreach value `{v}` is outside [A-Za-z0-9_.-]; a value needing more stays a Rust test"
            ));
        }
    }
    Ok((var, values))
}

fn refuse_comment_lines(body: &[(usize, &str)], section: &str) -> Result<(), String> {
    for (idx, line) in body {
        if line.trim_start().starts_with('#') {
            return Err(format!(
                "line {}: `#` lines are refused inside the {section} section; comments live in the header",
                idx + 1
            ));
        }
    }
    Ok(())
}

fn refuse_nonempty_body(body: &[(usize, &str)], what: &str) -> Result<(), String> {
    for (idx, line) in body {
        if !line.trim().is_empty() {
            return Err(format!("line {}: {what} carries no body", idx + 1));
        }
    }
    Ok(())
}

fn validate_subst_tokens(body: &str, loop_var: Option<&str>) -> Result<(), String> {
    let mut rest = body;
    while let Some(pos) = rest.find("${") {
        let after = &rest[pos + 2..];
        let Some(close) = after.find('}') else {
            return Err("unterminated `${` substitution".into());
        };
        let name = &after[..close];
        match loop_var {
            None => {
                return Err("`${` in a params or expect body is refused outside a loop".into());
            }
            Some(var) if name == var => {}
            Some(var) => {
                return Err(format!(
                    "`${{{name}}}` does not name the enclosing loop's variable `${var}`"
                ));
            }
        }
        rest = &after[close + 1..];
    }
    Ok(())
}

/// Walks one expression for the index decision and the string-`nearest`
/// refusal. Exhaustive over `Expr` so a newly added construct is a compile
/// error, never a silent skip.
fn walk_expr(expr: &Expr, params: &[Param], needs_indices: &mut bool) -> Result<(), String> {
    match expr {
        Expr::Now
        | Expr::PropAccess {
            variable: _,
            property: _,
        }
        | Expr::Variable(_)
        | Expr::Literal(_)
        | Expr::AliasRef(_) => {}
        Expr::Aggregate { func: _, arg } => walk_expr(arg, params, needs_indices)?,
        Expr::Search { field, query }
        | Expr::MatchText { field, query }
        | Expr::Bm25 { field, query } => {
            *needs_indices = true;
            walk_expr(field, params, needs_indices)?;
            walk_expr(query, params, needs_indices)?;
        }
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            *needs_indices = true;
            walk_expr(field, params, needs_indices)?;
            walk_expr(query, params, needs_indices)?;
            if let Some(max_edits) = max_edits {
                walk_expr(max_edits, params, needs_indices)?;
            }
        }
        Expr::Nearest {
            variable: _,
            property: _,
            query,
        } => {
            *needs_indices = true;
            refuse_string_nearest(query, params)?;
            walk_expr(query, params, needs_indices)?;
        }
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => {
            *needs_indices = true;
            walk_expr(primary, params, needs_indices)?;
            walk_expr(secondary, params, needs_indices)?;
            if let Some(k) = k {
                walk_expr(k, params, needs_indices)?;
            }
        }
    }
    Ok(())
}

fn refuse_string_nearest(query: &Expr, params: &[Param]) -> Result<(), String> {
    let is_vector = match query {
        Expr::Literal(Literal::List(_)) => true,
        Expr::Variable(name) => {
            let name = name.trim_start_matches('$');
            params
                .iter()
                .any(|p| p.name.trim_start_matches('$') == name && p.type_name != "String")
        }
        _ => false,
    };
    if is_vector {
        return Ok(());
    }
    Err(
        "`nearest` takes an explicit vector literal or vector parameter; a string argument \
         resolves an embedding provider from process environment and stays a Rust test"
            .into(),
    )
}

fn walk_clauses(
    clauses: &[Clause],
    params: &[Param],
    needs_indices: &mut bool,
) -> Result<(), String> {
    for clause in clauses {
        match clause {
            Clause::Binding(_) | Clause::Traversal(_) => {}
            Clause::Filter(f) => {
                walk_expr(&f.left, params, needs_indices)?;
                walk_expr(&f.right, params, needs_indices)?;
            }
            Clause::Negation(inner) => walk_clauses(inner, params, needs_indices)?,
        }
    }
    Ok(())
}

/// Why `expect ordered` is refused for this declaration, if it is. The
/// engine's order is total only where `apply_ordering` appends the `<var>.id`
/// tie-breaks (RFC 0045, Comparison semantics): no `order` clause, an
/// `rrf()`-led one (fusion sorts by score alone), or an aggregate in the
/// `return` list (group rows carry no `<var>.id`) each fail that condition.
/// The tie-break is stable within a run only (ids are minted per load), so
/// a case's `order` keys must be total over its rows: an authoring rule the
/// parser cannot check.
fn ordered_refusal(decl: &QueryDecl) -> Option<String> {
    if decl.order_clause.is_empty() {
        return Some("`expect ordered` is refused for a query without an `order` clause".into());
    }
    if matches!(
        decl.order_clause.first().map(|o| &o.expr),
        Some(Expr::Rrf { .. })
    ) {
        return Some(
            "`expect ordered` is refused for an `order` clause led by `rrf()`; fusion sorts by \
             score alone, with no tie-break"
                .into(),
        );
    }
    if decl
        .return_clause
        .iter()
        .any(|p| matches!(p.expr, Expr::Aggregate { .. }))
    {
        return Some(
            "`expect ordered` is refused for a query with an aggregate in its `return` list; \
             group rows carry no `<var>.id` tie-break, and a search-led aggregate query is \
             not ordered at all"
                .into(),
        );
    }
    None
}

fn inspect_decl(decl: &QueryDecl, needs_indices: &mut bool) -> Result<(), String> {
    walk_clauses(&decl.match_clause, &decl.params, needs_indices)?;
    for projection in &decl.return_clause {
        walk_expr(&projection.expr, &decl.params, needs_indices)?;
    }
    for ordering in &decl.order_clause {
        walk_expr(&ordering.expr, &decl.params, needs_indices)?;
    }
    Ok(())
}

fn props_use_embed(props: &[PropDecl]) -> bool {
    props.iter().any(|p| anns_use_embed(&p.annotations))
}

fn anns_use_embed(annotations: &[Annotation]) -> bool {
    annotations.iter().any(|a| a.name == "embed")
}

fn refuse_embed_schema(schema: &str, start_line: usize) -> Result<(), String> {
    let file = parse_schema(schema)
        .map_err(|e| format!("schema section starting at line {start_line} does not parse: {e}"))?;
    let uses_embed = file.declarations.iter().any(|decl| match decl {
        SchemaDecl::Interface(i) => props_use_embed(&i.properties),
        SchemaDecl::Node(n) => anns_use_embed(&n.annotations) || props_use_embed(&n.properties),
        SchemaDecl::Edge(e) => anns_use_embed(&e.annotations) || props_use_embed(&e.properties),
    });
    if uses_embed {
        return Err(
            "schemas using `@embed` resolve an embedding provider from process environment \
             and stay Rust tests"
                .into(),
        );
    }
    Ok(())
}

/// A query or mutate section parsed and classified, awaiting its expect.
struct PendingStep {
    is_mutation: bool,
    ordinal: usize,
    source: String,
    name: String,
    ast_params: Vec<Param>,
    ordered_refusal: Option<String>,
    expects_expand: bool,
    params_raw: Option<String>,
}

fn parse_case(stem: &str, text: &str) -> Result<Case, String> {
    if text.contains('\r') {
        return Err("case files are UTF-8 with `\\n` line endings; `\\r` found".into());
    }
    let lines: Vec<&str> = text.lines().collect();
    let (header_lines, sections) = split_sections(&lines);
    let header = parse_header(&header_lines)?;
    check_file_name(stem, &header.issue)?;

    if sections.first().map(|s| s.name.as_str()) != Some("schema") {
        return Err("the first section must be `--- schema`".into());
    }
    if sections.get(1).map(|s| s.name.as_str()) != Some("seed") {
        return Err("the second section must be `--- seed`".into());
    }
    let schema: String = sections[0]
        .body
        .iter()
        .map(|(_, l)| *l)
        .collect::<Vec<_>>()
        .join("\n");
    refuse_comment_lines(&sections[1].body, "seed")?;
    let seed: String = sections[1]
        .body
        .iter()
        .map(|(_, l)| *l)
        .filter(|l| !l.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    refuse_embed_schema(&schema, sections[0].header_line + 1)?;

    // An explicit `# traversal:` pin makes the traversal path the case's
    // subject, so the indexed executor must run covered, not on its fallback.
    let mut needs_indices = header.traversal.is_some();
    let mut items: Vec<Item> = Vec::new();
    let mut open_loop: Option<(String, Vec<String>, Vec<Step>)> = None;
    let mut pending: Option<PendingStep> = None;
    let mut ordinal = 0usize;
    let mut qm_steps = 0usize;
    let mut substitutable_lines: HashSet<usize> = HashSet::new();

    fn push_step(
        items: &mut Vec<Item>,
        open_loop: &mut Option<(String, Vec<String>, Vec<Step>)>,
        step: Step,
    ) {
        match open_loop {
            Some((_, _, steps)) => steps.push(step),
            None => items.push(Item::Step(step)),
        }
    }

    for section in &sections[2..] {
        let (kind, rest) = match section.name.split_once(' ') {
            Some((k, rest)) => (k, rest),
            None => (section.name.as_str(), ""),
        };
        match kind {
            "schema" | "seed" => {
                return Err(format!(
                    "line {}: `--- {kind}` is out of position; schema then seed lead the file, once each",
                    section.header_line + 1
                ));
            }
            "query" | "mutate" => {
                if !rest.is_empty() {
                    return Err(format!("`--- {kind}` takes no arguments"));
                }
                if pending.is_some() {
                    return Err(format!(
                        "line {}: the previous step is missing its `--- expect`",
                        section.header_line + 1
                    ));
                }
                let source: String = section
                    .body
                    .iter()
                    .map(|(_, l)| *l)
                    .collect::<Vec<_>>()
                    .join("\n");
                let file = parse_query(&source).map_err(|e| {
                    format!(
                        "`--- {kind}` section starting at line {} does not parse: {e}",
                        section.header_line + 1
                    )
                })?;
                let [decl] = file.queries.as_slice() else {
                    return Err(format!(
                        "a `--- {kind}` section must hold exactly one declaration, got {}",
                        file.queries.len()
                    ));
                };
                let is_mutation = !decl.mutations.is_empty();
                if kind == "query" && is_mutation {
                    return Err(
                        "a mutation declaration under `--- query` is refused; use `--- mutate`"
                            .into(),
                    );
                }
                if kind == "mutate" && !is_mutation {
                    return Err(
                        "a read declaration under `--- mutate` is refused; use `--- query`".into(),
                    );
                }
                inspect_decl(decl, &mut needs_indices)?;
                ordinal += 1;
                qm_steps += 1;
                pending = Some(PendingStep {
                    is_mutation,
                    ordinal,
                    source: source.clone(),
                    name: decl.name.clone(),
                    ast_params: decl.params.clone(),
                    ordered_refusal: ordered_refusal(decl),
                    expects_expand: expects_expand(&decl.match_clause),
                    params_raw: None,
                });
            }
            "params" => {
                if !rest.is_empty() {
                    return Err(format!("unknown section `--- {}`", section.name));
                }
                let Some(step) = pending.as_mut() else {
                    return Err(format!(
                        "line {}: `--- params` must directly follow a query or mutate section",
                        section.header_line + 1
                    ));
                };
                if step.params_raw.is_some() {
                    return Err(format!(
                        "line {}: a second `--- params` for one step is refused",
                        section.header_line + 1
                    ));
                }
                let body: String = section
                    .body
                    .iter()
                    .map(|(_, l)| *l)
                    .collect::<Vec<_>>()
                    .join("\n");
                validate_subst_tokens(&body, open_loop.as_ref().map(|(v, _, _)| v.as_str()))?;
                substitutable_lines.extend(section.body.iter().map(|(i, _)| *i));
                step.params_raw = Some(body);
            }
            "expect" => {
                let Some(step) = pending.take() else {
                    return Err(format!(
                        "line {}: `--- expect` has no query or mutate step to bind to",
                        section.header_line + 1
                    ));
                };
                let mode = parse_expect_header(rest)?;
                let completed = match (&mode, step.is_mutation) {
                    (ExpectHeader::Unordered | ExpectHeader::Ordered, true) => {
                        return Err(
                            "a mutate step takes `ok`, `affected:`, or `error:`; mutation results carry no rows"
                                .into(),
                        );
                    }
                    (ExpectHeader::Ok | ExpectHeader::Affected { .. }, false) => {
                        return Err("a query step takes `unordered`, `ordered`, or `error:`".into());
                    }
                    (ExpectHeader::Unordered | ExpectHeader::Ordered, false) => {
                        let ordered = matches!(mode, ExpectHeader::Ordered);
                        if ordered {
                            if let Some(reason) = step.ordered_refusal {
                                return Err(reason);
                            }
                        }
                        refuse_comment_lines(&section.body, "expect")?;
                        let body: String = section
                            .body
                            .iter()
                            .map(|(_, l)| *l)
                            .collect::<Vec<_>>()
                            .join("\n");
                        validate_subst_tokens(
                            &body,
                            open_loop.as_ref().map(|(v, _, _)| v.as_str()),
                        )?;
                        substitutable_lines.extend(section.body.iter().map(|(i, _)| *i));
                        Step::Query(QueryStep {
                            ordinal: step.ordinal,
                            source: step.source,
                            name: step.name,
                            ast_params: step.ast_params,
                            params_raw: step.params_raw,
                            expects_expand: step.expects_expand,
                            expect: QueryExpect::Rows {
                                ordered,
                                body_raw: body,
                                span: BodySpan {
                                    start_line: section.header_line + 1,
                                    len: section.body.len(),
                                },
                            },
                        })
                    }
                    (ExpectHeader::Error(needle), is_mutation) => {
                        refuse_nonempty_body(&section.body, "an `expect error:` section")?;
                        if is_mutation {
                            Step::Mutate(MutateStep {
                                ordinal: step.ordinal,
                                source: step.source,
                                name: step.name,
                                ast_params: step.ast_params,
                                params_raw: step.params_raw,
                                expect: MutateExpect::Error {
                                    needle: needle.clone(),
                                },
                            })
                        } else {
                            Step::Query(QueryStep {
                                ordinal: step.ordinal,
                                source: step.source,
                                name: step.name,
                                ast_params: step.ast_params,
                                params_raw: step.params_raw,
                                expects_expand: step.expects_expand,
                                expect: QueryExpect::Error {
                                    needle: needle.clone(),
                                },
                            })
                        }
                    }
                    (ExpectHeader::Ok, true) => {
                        refuse_nonempty_body(&section.body, "an `expect ok` section")?;
                        Step::Mutate(MutateStep {
                            ordinal: step.ordinal,
                            source: step.source,
                            name: step.name,
                            ast_params: step.ast_params,
                            params_raw: step.params_raw,
                            expect: MutateExpect::Ok,
                        })
                    }
                    (ExpectHeader::Affected { nodes, edges }, true) => {
                        refuse_nonempty_body(&section.body, "an `expect affected:` section")?;
                        Step::Mutate(MutateStep {
                            ordinal: step.ordinal,
                            source: step.source,
                            name: step.name,
                            ast_params: step.ast_params,
                            params_raw: step.params_raw,
                            expect: MutateExpect::Affected {
                                nodes: *nodes,
                                edges: *edges,
                            },
                        })
                    }
                };
                push_step(&mut items, &mut open_loop, completed);
            }
            "restart" => {
                if !rest.is_empty() {
                    return Err("`--- restart` takes no arguments".into());
                }
                if pending.is_some() {
                    return Err(format!(
                        "line {}: the previous step is missing its `--- expect`",
                        section.header_line + 1
                    ));
                }
                refuse_nonempty_body(&section.body, "`--- restart`")?;
                ordinal += 1;
                push_step(&mut items, &mut open_loop, Step::Restart { ordinal });
            }
            "loop" | "foreach" => {
                if pending.is_some() {
                    return Err(format!(
                        "line {}: the previous step is missing its `--- expect`",
                        section.header_line + 1
                    ));
                }
                if open_loop.is_some() {
                    return Err("loops may not nest".into());
                }
                refuse_nonempty_body(&section.body, "a loop header")?;
                let (var, values) = if kind == "loop" {
                    parse_loop_header(rest)?
                } else {
                    parse_foreach_header(rest)?
                };
                open_loop = Some((var, values, Vec::new()));
            }
            "endloop" => {
                if !rest.is_empty() {
                    return Err("`--- endloop` takes no arguments".into());
                }
                if pending.is_some() {
                    return Err(format!(
                        "line {}: the previous step is missing its `--- expect`",
                        section.header_line + 1
                    ));
                }
                refuse_nonempty_body(&section.body, "`--- endloop`")?;
                let Some((var, values, steps)) = open_loop.take() else {
                    return Err("`--- endloop` without an open loop".into());
                };
                if steps.is_empty() {
                    return Err("a loop enclosing no steps is refused".into());
                }
                items.push(Item::Loop { var, values, steps });
            }
            _ => return Err(format!("unknown section `--- {}`", section.name)),
        }
    }
    if pending.is_some() {
        return Err("the final step is missing its `--- expect`".into());
    }
    if open_loop.is_some() {
        return Err("a loop is not closed with `--- endloop`".into());
    }
    if qm_steps == 0 {
        return Err(
            "a case needs at least one query or mutate step; nothing would be asserted".into(),
        );
    }
    for (idx, line) in lines.iter().enumerate() {
        if line.contains("${") && !substitutable_lines.contains(&idx) {
            return Err(format!(
                "line {}: `${{` may appear only inside a params or expect body",
                idx + 1
            ));
        }
    }
    Ok(Case {
        schema,
        seed,
        traversal: header.traversal,
        items,
        needs_indices,
    })
}

fn normalize_number(n: &serde_json::Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    let f = n
        .as_f64()
        .expect("invariant: serde_json numbers are i64, u64, or f64");
    let mut s = format!("{f:.12}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s == "-0" { "0".to_string() } else { s }
}

/// One canonical string per row: object keys sorted, every number rewritten to
/// a scale-12 decimal with trailing zeros trimmed (integer-shaped numbers
/// never route through f64), null cells explicit.
fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => {
            let _ = write!(out, "{b}");
        }
        Value::Number(n) => out.push_str(&normalize_number(n)),
        Value::String(s) => {
            out.push_str(&Value::String(s.clone()).to_string());
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut pairs: Vec<(&String, &Value)> = map.iter().collect();
            pairs.sort_by_key(|(k, _)| k.as_str());
            out.push('{');
            for (i, (k, v)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                write_canonical(v, out);
            }
            out.push('}');
        }
    }
}

fn parse_expect_rows(body: &str) -> Result<Vec<Value>, String> {
    let mut rows = Vec::new();
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .map_err(|e| format!("expected row is not valid JSON: {e}: {line}"))?;
        if !value.is_object() {
            return Err(format!("expected row must be a JSON object: {line}"));
        }
        rows.push(value);
    }
    Ok(rows)
}

/// Compares normalized rows; returns the actual rows in the order bless would
/// write them alongside the mismatch message.
fn compare_rows(
    expected: &[Value],
    actual: &[Value],
    ordered: bool,
) -> Result<(), (String, Vec<String>)> {
    let mut expected: Vec<String> = expected.iter().map(canonical_json).collect();
    let mut actual: Vec<String> = actual.iter().map(canonical_json).collect();
    if !ordered {
        expected.sort();
        actual.sort();
    }
    if expected == actual {
        return Ok(());
    }
    let mut msg = if expected.len() == actual.len() {
        format!("row mismatch ({} rows)\nexpected:\n", expected.len())
    } else {
        format!(
            "row mismatch: expected {} rows, got {}\nexpected:\n",
            expected.len(),
            actual.len()
        )
    };
    for row in &expected {
        let _ = writeln!(msg, "  {row}");
    }
    msg.push_str("actual:\n");
    for row in &actual {
        let _ = writeln!(msg, "  {row}");
    }
    Err((msg, actual))
}

struct StepFail {
    label: String,
    message: String,
    bless_rows: Option<(BodySpan, Vec<String>)>,
}

fn step_label(ordinal: usize, kind: &str, binding: Option<(&str, &str)>) -> String {
    match binding {
        Some((var, value)) => format!("step {ordinal} ({kind}, ${var}={value})"),
        None => format!("step {ordinal} ({kind})"),
    }
}

fn substitute(text: &str, binding: Option<(&str, &str)>) -> String {
    match binding {
        Some((var, value)) => text.replace(&format!("${{{var}}}"), value),
        None => text.to_string(),
    }
}

fn build_params(
    params_raw: Option<&String>,
    ast_params: &[Param],
    binding: Option<(&str, &str)>,
) -> Result<omnigraph_compiler::ParamMap, String> {
    let json = match params_raw {
        Some(raw) => {
            let substituted = substitute(raw, binding);
            Some(
                serde_json::from_str::<Value>(&substituted)
                    .map_err(|e| format!("params are not valid JSON: {e}"))?,
            )
        }
        None => None,
    };
    json_params_to_param_map(json.as_ref(), ast_params, JsonParamMode::Standard)
        .map_err(|e| format!("params rejected: {e}"))
}

/// Expand executions observed while a pinned step ran, by path.
struct PathCounts {
    indexed: Arc<AtomicU64>,
    csr: Arc<AtomicU64>,
}

/// Runs `fut` under the case's `# traversal:` pin with expand-path probes
/// attached, or unscoped on the production path when the case pins
/// nothing. A pinned step gets its observed path counts back so the caller
/// can check the pin took effect: the pin is a task-local override, and a
/// step whose rows match its expect proves nothing about which path ran.
async fn under_traversal<F: Future>(
    mode: Option<&'static str>,
    fut: F,
) -> (F::Output, Option<PathCounts>) {
    match mode {
        Some(mode) => {
            let counts = PathCounts {
                indexed: Arc::new(AtomicU64::new(0)),
                csr: Arc::new(AtomicU64::new(0)),
            };
            let probes = QueryIoProbes {
                expand_indexed_runs: Arc::clone(&counts.indexed),
                expand_csr_runs: Arc::clone(&counts.csr),
                ..Default::default()
            };
            let out = with_traversal_mode(mode, with_query_io_probes(probes, fut)).await;
            (out, Some(counts))
        }
        None => (fut.await, None),
    }
}

/// Why a pinned step's observed expand paths violate its pin, if they do.
/// Any expand on the other path means the pinned mode was not honored. When
/// the step is known to expand (`require_expand`: its match clause carries
/// an unbound traversal and the query succeeded), zero expands on the pinned
/// path is a violation too: the pin and the probes are both task-locals, so
/// a boundary that drops the pin drops the probes with it and would
/// otherwise read as a clean 0/0.
fn pin_violation(mode: &str, indexed: u64, csr: u64, require_expand: bool) -> Option<String> {
    let (other, ran, pinned) = match mode {
        "indexed" => ("csr", csr, indexed),
        "csr" => ("indexed", indexed, csr),
        _ => return None,
    };
    if ran > 0 {
        return Some(format!(
            "pinned `{mode}`, ran `{other}` on {ran} expand(s); the pinned mode was not honored"
        ));
    }
    if require_expand && pinned == 0 {
        return Some(format!(
            "pinned `{mode}`, but no expand ran on it; the pin was lost before the \
             executor or the traversal did not execute"
        ));
    }
    None
}

/// The pin check for one finished step: `None` when the step was unpinned
/// or ran only, and at least once when required, on its pinned path.
fn check_pin(
    mode: Option<&'static str>,
    counts: &Option<PathCounts>,
    require_expand: bool,
) -> Option<String> {
    let (mode, counts) = mode.zip(counts.as_ref())?;
    pin_violation(
        mode,
        counts.indexed.load(Ordering::Relaxed),
        counts.csr.load(Ordering::Relaxed),
        require_expand,
    )
}

/// Whether a match clause list runs at least one Expand: a traversal
/// without an edge binding, outside `not { }`. A bound edge scans the edge
/// dataset on a path of its own that no mode pins, and a single-hop
/// negation runs as a CSR existence check that never reaches the expand
/// dispatch; a multi-hop negation does expand, and the other-path check
/// still covers it.
fn expects_expand(clauses: &[Clause]) -> bool {
    clauses.iter().any(|c| match c {
        Clause::Traversal(t) => t.edge_binding.is_none(),
        Clause::Negation(_) | Clause::Binding(_) | Clause::Filter(_) => false,
    })
}

async fn run_query_step(
    db: &Omnigraph,
    mode: Option<&'static str>,
    step: &QueryStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "query", binding);
    let fail = |message: String| StepFail {
        label: label.clone(),
        message,
        bless_rows: None,
    };
    // A params refusal is one of the ways "the query must fail": route it
    // into an `error:` expectation instead of always failing the step.
    let params = match build_params(step.params_raw.as_ref(), &step.ast_params, binding) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                QueryExpect::Error { needle } if e.contains(needle) => Ok(()),
                QueryExpect::Error { needle } => {
                    Err(fail(format!("error does not contain \"{needle}\": {e}")))
                }
                QueryExpect::Rows { .. } => Err(fail(e)),
            };
        }
    };
    let (outcome, counts) = under_traversal(
        mode,
        db.query(
            ReadTarget::branch("main"),
            &step.source,
            &step.name,
            &params,
        ),
    )
    .await;
    let require_expand =
        step.expects_expand && outcome.is_ok() && matches!(step.expect, QueryExpect::Rows { .. });
    if let Some(violation) = check_pin(mode, &counts, require_expand) {
        return Err(fail(violation));
    }
    match &step.expect {
        QueryExpect::Rows {
            ordered,
            body_raw,
            span,
        } => {
            let result = outcome.map_err(|e| fail(format!("query failed: {e}")))?;
            let Value::Array(actual) = result.to_rust_json() else {
                return Err(fail("engine returned a non-array row set".into()));
            };
            let expected = parse_expect_rows(&substitute(body_raw, binding)).map_err(&fail)?;
            compare_rows(&expected, &actual, *ordered).map_err(|(message, rows)| StepFail {
                label: label.clone(),
                message,
                bless_rows: Some((*span, rows)),
            })
        }
        QueryExpect::Error { needle } => match outcome {
            Ok(_) => Err(fail(format!(
                "expected an error containing \"{needle}\", but the query succeeded"
            ))),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains(needle) {
                    Ok(())
                } else {
                    Err(fail(format!("error does not contain \"{needle}\": {msg}")))
                }
            }
        },
    }
}

async fn run_mutate_step(
    db: &Omnigraph,
    mode: Option<&'static str>,
    step: &MutateStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "mutate", binding);
    let fail = |message: String| StepFail {
        label: label.clone(),
        message,
        bless_rows: None,
    };
    let params = match build_params(step.params_raw.as_ref(), &step.ast_params, binding) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                MutateExpect::Error { needle } if e.contains(needle) => Ok(()),
                MutateExpect::Error { needle } => {
                    Err(fail(format!("error does not contain \"{needle}\": {e}")))
                }
                MutateExpect::Ok | MutateExpect::Affected { .. } => Err(fail(e)),
            };
        }
    };
    let (outcome, counts) =
        under_traversal(mode, db.mutate("main", &step.source, &step.name, &params)).await;
    if let Some(violation) = check_pin(mode, &counts, false) {
        return Err(fail(violation));
    }
    match &step.expect {
        MutateExpect::Ok => outcome
            .map(|_| ())
            .map_err(|e| fail(format!("mutation failed: {e}"))),
        MutateExpect::Affected { nodes, edges } => {
            let result = outcome.map_err(|e| fail(format!("mutation failed: {e}")))?;
            if result.affected_nodes == *nodes && result.affected_edges == *edges {
                Ok(())
            } else {
                Err(fail(format!(
                    "affected counts mismatch: expected nodes={nodes} edges={edges}, got nodes={} edges={}",
                    result.affected_nodes, result.affected_edges
                )))
            }
        }
        MutateExpect::Error { needle } => match outcome {
            Ok(_) => Err(fail(format!(
                "expected an error containing \"{needle}\", but the mutation succeeded"
            ))),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains(needle) {
                    Ok(())
                } else {
                    Err(fail(format!("error does not contain \"{needle}\": {msg}")))
                }
            }
        },
    }
}

/// A fresh store for one case: init from the schema, seed, and build indices
/// when the case needs them. The tempdir rides along so the store outlives
/// the call.
async fn open_case_store(case: &Case) -> Result<(Omnigraph, String, tempfile::TempDir), String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir failed: {e}"))?;
    let uri = dir
        .path()
        .to_str()
        .ok_or_else(|| "temp path is not utf-8".to_string())?
        .to_string();
    let db = Omnigraph::init(&uri, &case.schema)
        .await
        .map_err(|e| format!("init failed: {e}"))?;
    if !case.seed.trim().is_empty() {
        load_jsonl(&db, &case.seed, LoadMode::Overwrite)
            .await
            .map_err(|e| format!("seed load failed: {e}"))?;
    }
    if case.needs_indices {
        db.ensure_indices()
            .await
            .map_err(|e| format!("ensure_indices failed: {e}"))?;
    }
    Ok((db, uri, dir))
}

async fn execute_case(case: &Case, path: &Path, bless: bool) -> Result<(), String> {
    let (mut db, uri, _dir) = open_case_store(case).await?;

    let mut first_fail: Option<StepFail> = None;
    'run: for item in &case.items {
        let (values, var, steps): (Vec<Option<&str>>, Option<&str>, Vec<&Step>) = match item {
            Item::Step(step) => (vec![None], None, vec![step]),
            Item::Loop { var, values, steps } => (
                values.iter().map(|v| Some(v.as_str())).collect(),
                Some(var.as_str()),
                steps.iter().collect(),
            ),
        };
        for value in values {
            let binding = var.zip(value);
            for step in &steps {
                let outcome = match step {
                    Step::Query(q) => run_query_step(&db, case.traversal, q, binding).await,
                    Step::Mutate(m) => run_mutate_step(&db, case.traversal, m, binding).await,
                    Step::Restart { ordinal } => {
                        drop(db);
                        db = Omnigraph::open(&uri).await.map_err(|e| {
                            format!(
                                "{}: reopen failed: {e}",
                                step_label(*ordinal, "restart", binding)
                            )
                        })?;
                        Ok(())
                    }
                };
                if let Err(fail) = outcome {
                    first_fail = Some(fail);
                    break 'run;
                }
            }
        }
    }
    let Some(fail) = first_fail else {
        return Ok(());
    };
    let mut detail = format!("{}: {}", fail.label, fail.message);
    if bless {
        if let Some((span, rows)) = &fail.bless_rows {
            if case.has_loops() {
                detail.push_str("\nbless: refused, the case contains loops");
            } else {
                bless_rewrite(path, *span, rows)?;
                let _ = write!(
                    detail,
                    "\nbless: expect rewritten in place ({} rows), re-run to confirm",
                    rows.len()
                );
            }
        }
    }
    Err(detail)
}

fn splice_lines(original: &str, span: BodySpan, rows: &[String]) -> String {
    let lines: Vec<&str> = original.lines().collect();
    let body = &lines[span.start_line..span.start_line + span.len];
    let trailing_blanks = body
        .iter()
        .rev()
        .take_while(|l| l.trim().is_empty())
        .count();
    let mut out: Vec<&str> = lines[..span.start_line].to_vec();
    out.extend(rows.iter().map(String::as_str));
    out.extend(std::iter::repeat_n("", trailing_blanks));
    out.extend(&lines[span.start_line + span.len..]);
    let mut joined = out.join("\n");
    joined.push('\n');
    joined
}

fn bless_rewrite(path: &Path, span: BodySpan, rows: &[String]) -> Result<(), String> {
    let original =
        std::fs::read_to_string(path).map_err(|e| format!("bless: cannot re-read case: {e}"))?;
    std::fs::write(path, splice_lines(&original, span, rows))
        .map_err(|e| format!("bless: cannot write case: {e}"))
}

/// Parses and executes one case file; `bless` rewrites a failing step's
/// `--- expect` rows in place and still reports the step (`expect
/// rewritten`), so the run stays red until the re-run confirms.
pub async fn run_case(path: PathBuf, bless: bool) -> Result<(), String> {
    let stem = stem_of(&path);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("cannot read case file: {e}"))?;
    let case = parse_case(&stem, &text).map_err(|e| format!("refused: {e}"))?;
    execute_case(&case, &path, bless).await
}

/// The corpus directory, `cases/` beside this crate's manifest: the same
/// compile-time root the test target's `datatest_stable::harness!` resolves
/// `root = "cases"` against.
pub fn corpus_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("cases")
}

/// `OMNIGRAPH_GQ_BLESS=1` turns bless on; unset, empty, or `0` leaves it off.
///
/// # Panics
///
/// On any other value: the knob is refused, not ignored.
pub fn bless_from_env() -> bool {
    match std::env::var(BLESS_ENV) {
        Err(_) => false,
        Ok(v) if v == "1" => true,
        Ok(v) if v == "0" || v.is_empty() => false,
        Ok(v) => panic!("{BLESS_ENV} takes 1 (or 0/unset), got `{v}`"),
    }
}

/// The per-case wall-time budget: `OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` or the
/// default.
pub fn case_budget_from_env() -> Duration {
    Duration::from_secs(env_positive(CASE_TIMEOUT_ENV).unwrap_or(DEFAULT_CASE_TIMEOUT_SECS))
}

/// Splits the corpus dir into `.gqt` case files and foreign entries; a
/// foreign entry is a mis-renamed, nested, symlinked, dot-prefixed, or
/// non-UTF-8-named case that would otherwise silently never run. The rule
/// that RUNS a case is the test target's `datatest_stable::harness!` pattern
/// (`tests/gq_logic_tests.rs`): a regular file (symlinks are not followed)
/// with a UTF-8 name that ends in `.gqt` and does not start with `.`. This
/// function mirrors that rule so `corpus_layout` refuses what the target
/// would skip; `scripts/check-fix-regression.py` (`corpus_case`) mirrors
/// the name half, and both self-tests walk one name battery. Dot-prefixed
/// entries without a `.gqt` extension (`.DS_Store`, `.gitkeep`, and a file
/// named exactly `.gqt`, which has no extension) are neither cases nor
/// foreign: they are skipped, as the target skips every hidden file.
pub fn list_cases(root: &Path) -> (Vec<PathBuf>, Vec<String>) {
    let mut files = Vec::new();
    let mut foreign = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                foreign.push(entry.file_name().to_string_lossy().into_owned());
                continue;
            };
            let is_regular_file = entry.file_type().map(|t| t.is_file()).unwrap_or(false);
            let is_gqt =
                is_regular_file && path.extension().and_then(|s| s.to_str()) == Some("gqt");
            if name.starts_with('.') && !is_gqt {
                continue;
            }
            if is_gqt && !name.starts_with('.') {
                files.push(path);
            } else {
                foreign.push(name);
            }
        }
    }
    files.sort();
    foreign.sort();
    (files, foreign)
}

/// The case name: the file stem, or `<non-utf8>` for a name the corpus
/// rule refuses anyway.
pub fn stem_of(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("<non-utf8>")
        .to_string()
}

/// A positive-integer environment override; unset or empty means none.
///
/// # Panics
///
/// On a value that is not a positive integer: the knob is refused, not
/// ignored.
pub fn env_positive(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    if value.trim().is_empty() {
        return None;
    }
    match value.trim().parse::<u64>() {
        Ok(n) if n > 0 => Some(n),
        _ => panic!("{name} takes a positive integer, got `{value}`"),
    }
}

/// The production traversal path consults `OMNIGRAPH_TRAVERSAL_MODE`, so a set
/// variable would silently decide which path an unpinned case exercises.
pub fn traversal_override_refusal(value: Option<&OsStr>) -> Option<String> {
    value.map(|v| {
        format!(
            "OMNIGRAPH_TRAVERSAL_MODE={} is set; logic tests run the production traversal \
             path, unset it (a case that must run one path pins it with `# traversal:`)",
            v.to_string_lossy()
        )
    })
}

pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// What one case produced: its stem, wall time from the moment it started,
/// and the verdict (the error text carries the failing step's diff, the
/// refusal, the panic message, or the budget overrun).
#[derive(Debug)]
pub struct CaseOutcome {
    pub stem: String,
    pub elapsed: Duration,
    pub result: Result<(), String>,
}

/// Runs `case` (the future for one case, named `stem`) under `budget` of
/// wall time, timed from its first poll; a case over budget is dropped,
/// store included. A panic or a timeout is an ordinary failed case, so a
/// corpus run always reaches every case.
pub async fn run_bounded<F>(stem: &str, budget: Duration, case: F) -> CaseOutcome
where
    F: Future<Output = Result<(), String>>,
{
    let started = Instant::now();
    let case = AssertUnwindSafe(case).catch_unwind();
    let result = match tokio::time::timeout(budget, case).await {
        Ok(Ok(result)) => result,
        Ok(Err(payload)) => Err(format!(
            "case panicked: {}",
            panic_message(payload.as_ref())
        )),
        Err(_) => Err(format!(
            "case exceeded its budget of {:.2}s ({CASE_TIMEOUT_ENV} overrides the default of \
             {DEFAULT_CASE_TIMEOUT_SECS}s; libtest's --test-threads sets how many cases run \
             concurrently; a case over budget belongs in a `heavy-repro:` `#[ignore]`d test, \
             not the corpus)",
            budget.as_secs_f64()
        )),
    };
    CaseOutcome {
        stem: stem.to_string(),
        elapsed: started.elapsed(),
        result,
    }
}

/// [`run_bounded`] over [`run_case`] for the file at `path`.
pub async fn run_case_bounded(path: PathBuf, budget: Duration, bless: bool) -> CaseOutcome {
    let stem = stem_of(&path);
    run_bounded(&stem, budget, run_case(path, bless)).await
}

#[cfg(test)]
mod tests;
