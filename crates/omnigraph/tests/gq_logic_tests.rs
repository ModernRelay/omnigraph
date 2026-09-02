//! GQ logic tests: walks `tests/gq_logic_tests/*.gqt` and runs each case
//! against a fresh temporary store (init, load, index, then the steps in
//! order). The file format, refusal set, comparison semantics, and bless
//! workflow are specified in `docs/rfcs/0045-gq-logic-tests.md`.
//!
//! To libtest the whole walker is one test; case concurrency comes from a
//! `JoinSet` bounded by a semaphore (`OMNIGRAPH_GQ_JOBS=<n>` overrides the
//! default of the machine's available parallelism), and every case runs under
//! a per-case budget (`OMNIGRAPH_GQ_CASE_TIMEOUT_SECS=<n>`, default 10).
//! `OMNIGRAPH_GQ_LOGIC_TESTS=<substr>[,<substr>]` restricts the run to
//! matching case files; `OMNIGRAPH_GQ_BLESS=1` rewrites the failing step's
//! `--- expect` rows in place.
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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const CASE_TIMEOUT_ENV: &str = "OMNIGRAPH_GQ_CASE_TIMEOUT_SECS";
const DEFAULT_CASE_TIMEOUT_SECS: u64 = 10;
const JOBS_ENV: &str = "OMNIGRAPH_GQ_JOBS";

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

async fn run_case(path: PathBuf, bless: bool) -> Result<(), String> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| "case file name is not utf-8".to_string())?
        .to_string();
    let text = std::fs::read_to_string(&path).map_err(|e| format!("cannot read case file: {e}"))?;
    let case = parse_case(&stem, &text).map_err(|e| format!("refused: {e}"))?;
    execute_case(&case, &path, bless).await
}

fn corpus_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .join("tests")
        .join("gq_logic_tests")
}

/// Splits the corpus dir into `.gqt` case files and foreign entries; a
/// foreign entry is a mis-renamed, nested, or dot-prefixed case that would
/// otherwise silently never run. A case is a top-level file whose name ends
/// in `.gqt` and does not start with `.`; `scripts/check-fix-regression.py`
/// (`corpus_case`) applies the same rule, and both self-tests walk one name
/// battery. Dot-prefixed entries without a `.gqt` extension (`.DS_Store`,
/// `.gitkeep`, and a file named exactly `.gqt`, which has no extension)
/// are neither cases nor foreign: they are skipped.
fn list_cases(root: &Path) -> (Vec<PathBuf>, Vec<String>) {
    let mut files = Vec::new();
    let mut foreign = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let is_gqt = path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("gqt");
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

fn stem_of(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("<non-utf8>")
        .to_string()
}

/// A positive-integer environment override; unset or empty means none.
fn env_positive(name: &str) -> Option<u64> {
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
fn traversal_override_refusal(value: Option<&OsStr>) -> Option<String> {
    value.map(|v| {
        format!(
            "OMNIGRAPH_TRAVERSAL_MODE={} is set; logic tests run the production traversal \
             path, unset it (a case that must run one path pins it with `# traversal:`)",
            v.to_string_lossy()
        )
    })
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

type CaseFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
type CaseRunner = Arc<dyn Fn(PathBuf) -> CaseFuture + Send + Sync>;

#[derive(Debug)]
struct CaseOutcome {
    stem: String,
    elapsed: Duration,
    result: Result<(), String>,
}

/// Runs every case as its own task with at most `permits` in flight (each
/// case holds a store and may build indexes) and `budget` of wall time per
/// case, timed from the moment it holds a permit; a case over budget is
/// dropped, store included, before its permit is released. A panic or a
/// timeout is an ordinary failed case, so the whole corpus always runs.
/// `report` sees each outcome as it completes; the returned list is sorted
/// by case name.
async fn run_bounded(
    cases: Vec<(String, PathBuf)>,
    permits: usize,
    budget: Duration,
    run: CaseRunner,
    report: &(dyn Fn(&CaseOutcome) + Sync),
) -> Vec<CaseOutcome> {
    let semaphore = Arc::new(Semaphore::new(permits));
    let mut set: JoinSet<CaseOutcome> = JoinSet::new();
    for (stem, path) in cases {
        let semaphore = Arc::clone(&semaphore);
        let run = Arc::clone(&run);
        set.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("the case semaphore is never closed");
            let started = Instant::now();
            let case = AssertUnwindSafe(run(path)).catch_unwind();
            let result = match tokio::time::timeout(budget, case).await {
                Ok(Ok(result)) => result,
                Ok(Err(payload)) => Err(format!(
                    "case panicked: {}",
                    panic_message(payload.as_ref())
                )),
                Err(_) => Err(format!(
                    "case exceeded its budget of {:.2}s while up to {permits} cases ran \
                     concurrently ({CASE_TIMEOUT_ENV} overrides the default of \
                     {DEFAULT_CASE_TIMEOUT_SECS}s, {JOBS_ENV} the concurrency; a case over \
                     budget belongs in a `heavy-repro:` `#[ignore]`d test, not the corpus)",
                    budget.as_secs_f64()
                )),
            };
            CaseOutcome {
                stem,
                elapsed: started.elapsed(),
                result,
            }
        });
    }
    let mut outcomes = Vec::new();
    while let Some(joined) = set.join_next().await {
        // The case itself is caught by `catch_unwind`; the task around it
        // holds nothing that can panic.
        let outcome = joined.expect("a case task never panics");
        report(&outcome);
        outcomes.push(outcome);
    }
    outcomes.sort_by(|a, b| a.stem.cmp(&b.stem));
    outcomes
}

#[tokio::test(flavor = "multi_thread")]
async fn gq_logic_tests() {
    let root = corpus_root();
    let (mut files, foreign) = list_cases(&root);
    assert!(
        foreign.is_empty(),
        "foreign entries under {}: {}; a mis-renamed, nested, or dot-prefixed case must never silently skip",
        root.display(),
        foreign.join(", ")
    );
    assert!(
        !files.is_empty(),
        "no .gqt cases found under {}; a broken checkout must never read as green",
        root.display()
    );
    if let Ok(filter) = std::env::var("OMNIGRAPH_GQ_LOGIC_TESTS") {
        let needles: Vec<String> = filter
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if !needles.is_empty() {
            files.retain(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|name| needles.iter().any(|n| name.contains(n.as_str())))
            });
            assert!(
                !files.is_empty(),
                "OMNIGRAPH_GQ_LOGIC_TESTS={filter} matched no cases"
            );
        }
    }
    let bless = match std::env::var("OMNIGRAPH_GQ_BLESS") {
        Err(_) => false,
        Ok(v) if v == "1" => true,
        Ok(v) if v == "0" || v.is_empty() => false,
        Ok(v) => panic!("OMNIGRAPH_GQ_BLESS takes 1 (or 0/unset), got `{v}`"),
    };

    if let Some(reason) =
        traversal_override_refusal(std::env::var_os("OMNIGRAPH_TRAVERSAL_MODE").as_deref())
    {
        panic!("{reason}");
    }
    let permits = env_positive(JOBS_ENV)
        .map(|n| {
            usize::try_from(n)
                .unwrap_or(usize::MAX)
                .min(Semaphore::MAX_PERMITS)
        })
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1)
        });
    let budget =
        Duration::from_secs(env_positive(CASE_TIMEOUT_ENV).unwrap_or(DEFAULT_CASE_TIMEOUT_SECS));

    let total = files.len();
    let cases = files
        .into_iter()
        .map(|path| (stem_of(&path), path))
        .collect();
    let runner: CaseRunner = Arc::new(move |path| Box::pin(run_case(path, bless)));
    let report = |outcome: &CaseOutcome| {
        let secs = outcome.elapsed.as_secs_f64();
        match &outcome.result {
            Ok(()) => println!("ok {} {secs:.2}s", outcome.stem),
            Err(_) => println!("FAIL {} {secs:.2}s", outcome.stem),
        }
    };
    let outcomes = run_bounded(cases, permits, budget, runner, &report).await;

    let mut failures: Vec<(String, String)> = outcomes
        .into_iter()
        .filter_map(|outcome| {
            let CaseOutcome { stem, result, .. } = outcome;
            result.err().map(|detail| (stem, detail))
        })
        .collect();

    if !failures.is_empty() {
        failures.sort();
        let mut msg = format!(
            "{} of {total} gq logic test cases failed:\n",
            failures.len()
        );
        for (stem, detail) in &failures {
            let _ = write!(msg, "\n{stem}:\n  {}\n", detail.replace('\n', "\n  "));
        }
        panic!("{msg}");
    }
}

const HDR: &str = "# issue: none\n";
const SCHEMA: &str = "--- schema\nnode Person {\n    name: String @key\n}\n";
const SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n";
const QUERY: &str =
    "--- query\nquery all() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
const EXPECT: &str = "--- expect unordered\n{\"p.name\": \"alice\"}\n";
const MUTATE: &str = "--- mutate\nquery ins($n: String) {\n    insert Person { name: $n }\n}\n";
const PARAMS: &str = "--- params\n{\"n\": \"bob\"}\n";
const EXPECT_OK: &str = "--- expect ok\n";

fn refusal(stem: &str, text: &str) -> String {
    parse_case(stem, text).expect_err("expected the case to be refused")
}

#[test]
fn parses_a_minimal_case() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}");
    let case = parse_case("minimal", &text).unwrap();
    assert_eq!(case.items.len(), 1);
    assert!(!case.needs_indices);
    assert_eq!(case.traversal, None);
}

#[test]
fn header_notes_repeat_and_continuation_lines_are_refused() {
    let text = format!(
        "# issue: 7\n# red_on: 2026-01-01, the run\n# notes: returned 8,\n# notes: not 20.\n{SCHEMA}{SEED}{QUERY}{EXPECT}"
    );
    parse_case("issue_7_notes", &text).unwrap();
    let text = format!(
        "# issue: 7\n# red_on: 2026-01-01, the run\n#   returned 8: not 20.\n{SCHEMA}{SEED}{QUERY}{EXPECT}"
    );
    assert!(refusal("issue_7_x", &text).contains("unknown header key"));
    // The three misspellings that the old prose branch swallowed silently.
    for typo in [
        "# Traversal: indexed",
        "# traversal : indexed",
        "# traversal=csr",
    ] {
        let text = format!("{HDR}{typo}\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
        let reason = refusal("x", &text);
        assert!(
            reason.contains("unknown header key") || reason.contains("not `# <key>: <value>`"),
            "{typo}: {reason}"
        );
    }
}

/// Bounded exhaustive walk of the header-line typo space: key spelling,
/// separator, leading and trailing whitespace, and the gap before the value.
/// A line is accepted exactly when it equals the canonical
/// `# traversal: indexed`, so a future key inherits the same proof.
#[test]
fn header_lines_are_accepted_only_in_canonical_form() {
    let keys = [
        "traversal",
        "Traversal",
        "TRAVERSAL",
        "traversa1",
        "traversal_",
        " traversal",
    ];
    let seps = [":", " :", "=", "", "::"];
    let leads = ["# ", "#", "#  ", " # "];
    let gaps = [" ", "", "  ", "\t"];
    let trails = ["", " ", "\t"];
    let canonical = canonical_header_line("traversal", "indexed");
    // Distinct lines: two lead/key pairs collide (`#` + ` traversal` = `# ` +
    // `traversal`, and `# ` + ` traversal` = `#  ` + `traversal`), 120
    // duplicates over the 1440 grid points, so the set is what gets walked.
    let mut lines = std::collections::BTreeSet::new();
    for lead in leads {
        for key in keys {
            for sep in seps {
                for gap in gaps {
                    for trail in trails {
                        lines.insert(format!("{lead}{key}{sep}{gap}indexed{trail}"));
                    }
                }
            }
        }
    }
    assert_eq!(lines.len(), 1320, "the typo space is 1320 distinct lines");
    let mut accepted = 0;
    for line in &lines {
        let text = format!("{HDR}{line}\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
        match parse_case("x", &text) {
            Ok(case) => {
                assert_eq!(line, &canonical, "accepted a non-canonical line");
                assert_eq!(case.traversal, Some("indexed"));
                accepted += 1;
            }
            Err(e) => assert_ne!(line, &canonical, "refused the canonical line: {e}"),
        }
    }
    assert_eq!(accepted, 1);
}

#[test]
fn refuses_missing_issue_header() {
    let text = format!("# notes: no anchor\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("# issue:"));
}

#[test]
fn refuses_numbered_issue_without_red_on() {
    let text = format!("# issue: 7\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7_x", &text).contains("red_on"));
}

#[test]
fn refuses_header_line_without_a_key() {
    let text = format!("# stray prose\n{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("not `# <key>: <value>`"));
}

#[test]
fn refuses_unknown_header_key() {
    let text = format!("{HDR}# owner: me\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("unknown header key"));
}

#[test]
fn refuses_bad_traversal_mode() {
    let text = format!("{HDR}# traversal: bogus\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("indexed"));
}

#[test]
fn refuses_non_header_line_before_first_section() {
    let text = format!("{HDR}stray\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("precede the first section"));
}

#[test]
fn refuses_comment_line_in_seed() {
    let text = format!("{HDR}{SCHEMA}--- seed\n# a comment\n{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("seed"));
}

#[test]
fn refuses_comment_line_in_expect_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n# nope\n");
    assert!(refusal("x", &text).contains("expect"));
}

#[test]
fn refuses_seed_before_schema() {
    let text = format!("{HDR}{SEED}{SCHEMA}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("first section"));
}

#[test]
fn refuses_missing_seed_section() {
    let text = format!("{HDR}{SCHEMA}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("second section"));
}

#[test]
fn refuses_case_without_a_query_or_mutate_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}");
    assert!(refusal("x", &text).contains("at least one query or mutate step"));
}

#[test]
fn refuses_restart_only_step_list() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- restart\n");
    assert!(refusal("x", &text).contains("at least one query or mutate step"));
}

#[test]
fn refuses_second_declaration_in_one_section() {
    let two = "--- query\nquery a() {\n    match { $p: Person }\n    return { $p.name }\n}\nquery b() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{two}{EXPECT}");
    assert!(refusal("x", &text).contains("exactly one declaration"));
}

#[test]
fn refuses_mutation_declaration_under_query() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- query\nquery ins($n: String) {{\n    insert Person {{ name: $n }}\n}}\n{EXPECT}"
    );
    assert!(refusal("x", &text).contains("use `--- mutate`"));
}

#[test]
fn refuses_read_declaration_under_mutate() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- mutate\nquery all() {{\n    match {{ $p: Person }}\n    return {{ $p.name }}\n}}\n{EXPECT_OK}"
    );
    assert!(refusal("x", &text).contains("use `--- query`"));
}

#[test]
fn refuses_bare_expect() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect\n");
    assert!(refusal("x", &text).contains("mode word"));
}

#[test]
fn refuses_error_expect_without_substring() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect error:\n");
    assert!(refusal("x", &text).contains("substring"));
}

#[test]
fn refuses_error_expect_with_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect error: boom\nbody\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_affected_expect_missing_a_count() {
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}--- expect affected: nodes=1\n");
    assert!(refusal("x", &text).contains("nodes=<N> edges=<M>"));
}

#[test]
fn refuses_unknown_expect_mode() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect sorted\n");
    assert!(refusal("x", &text).contains("unknown expect mode"));
}

#[test]
fn refuses_row_expect_on_a_mutate_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}{EXPECT}");
    assert!(refusal("x", &text).contains("carry no rows"));
}

#[test]
fn refuses_ok_expect_on_a_query_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT_OK}");
    assert!(refusal("x", &text).contains("a query step takes"));
}

#[test]
fn refuses_query_step_without_expect() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}");
    assert!(refusal("x", &text).contains("missing its `--- expect`"));
}

#[test]
fn refuses_expect_with_no_step_to_bind_to() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- restart\n{EXPECT}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("no query or mutate step to bind to"));
}

#[test]
fn refuses_params_without_a_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}{PARAMS}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("directly follow"));
}

#[test]
fn refuses_second_params_for_one_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}{PARAMS}{EXPECT_OK}");
    assert!(refusal("x", &text).contains("second `--- params`"));
}

#[test]
fn refuses_restart_with_a_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- restart\nstray\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_unknown_section() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- teardown\n");
    assert!(refusal("x", &text).contains("unknown section"));
}

#[test]
fn refuses_schema_out_of_position() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}{SCHEMA}");
    assert!(refusal("x", &text).contains("out of position"));
}

#[test]
fn refuses_negative_loop_bound() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i -1 2\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("non-negative"));
}

#[test]
fn refuses_empty_loop_range() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 3 3\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("empty loop range"));
}

#[test]
fn refuses_foreach_without_values() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- foreach $x\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("no values"));
}

#[test]
fn refuses_foreach_value_outside_charset() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- foreach $x a\"b\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("[A-Za-z0-9_.-]"));
}

#[test]
fn refuses_bad_loop_variable_name() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $I 0 2\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("$[a-z][a-z0-9_]*"));
}

#[test]
fn refuses_nested_loops() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- loop $i 0 2\n--- loop $j 0 2\n{QUERY}{EXPECT}--- endloop\n--- endloop\n"
    );
    assert!(refusal("x", &text).contains("may not nest"));
}

#[test]
fn refuses_endloop_without_a_loop() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("without an open loop"));
}

#[test]
fn refuses_unclosed_loop() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 2\n{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("not closed"));
}

#[test]
fn refuses_loop_enclosing_no_steps() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 2\n--- endloop\n{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("enclosing no steps"));
}

#[test]
fn refuses_substitution_marker_in_query_body() {
    let query = "--- query\nquery all() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    let text =
        format!("{HDR}{SCHEMA}{SEED}{query}{EXPECT}").replace("$p.name }", "$p.name } // ${i}");
    assert!(refusal("x", &text).contains("only inside a params or expect body"));
}

#[test]
fn refuses_substitution_marker_in_seed() {
    let text = format!(
        "{HDR}{SCHEMA}--- seed\n{{\"type\":\"Person\",\"data\":{{\"name\":\"${{i}}\"}}}}\n{QUERY}{EXPECT}"
    );
    assert!(refusal("x", &text).contains("only inside a params or expect body"));
}

#[test]
fn refuses_substitution_outside_a_loop() {
    let text =
        format!("{HDR}{SCHEMA}{SEED}{MUTATE}--- params\n{{\"n\": \"${{who}}\"}}\n{EXPECT_OK}");
    assert!(refusal("x", &text).contains("outside a loop"));
}

#[test]
fn refuses_substitution_naming_the_wrong_variable() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- foreach $who bob\n{MUTATE}--- params\n{{\"n\": \"${{other}}\"}}\n{EXPECT_OK}--- endloop\n"
    );
    assert!(refusal("x", &text).contains("enclosing loop's variable"));
}

#[test]
fn refuses_unterminated_substitution() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- foreach $who bob\n{MUTATE}--- params\n{{\"n\": \"${{who\n{EXPECT_OK}--- endloop\n"
    );
    assert!(refusal("x", &text).contains("unterminated"));
}

#[test]
fn refuses_file_name_disagreeing_with_issue_header() {
    let text = format!("# issue: 7\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_8_wrong", &text).contains("disagrees"));
}

#[test]
fn refuses_issue_prefix_without_number_or_short_name() {
    let text = format!("# issue: 7\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7", &text).contains("issue_<N>_<short_name>"));
    assert!(refusal("issue_x", &text).contains("issue_<N>_<short_name>"));
}

#[test]
fn refuses_feature_name_with_numbered_issue_header() {
    let text = format!("# issue: 7\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("feature_name", &text).contains("issue_7_<short_name>"));
}

#[test]
fn refuses_file_name_outside_charset() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("Bad-Name", &text).contains("[a-z0-9_]"));
}

#[test]
fn refuses_ordered_expect_without_an_order_clause() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect ordered\n{{\"p.name\": \"alice\"}}\n");
    assert!(refusal("x", &text).contains("order` clause"));
}

#[test]
fn refuses_embed_schema() {
    let schema = "--- schema\nnode Doc {\n    slug: String @key\n    text: String\n    vec: Vector(4) @embed(\"text\")\n}\n";
    let seed = "--- seed\n";
    let text = format!("{HDR}{schema}{seed}{QUERY}{EXPECT}")
        .replace("$p: Person", "$p: Doc")
        .replace("$p.name", "$p.slug");
    assert!(refusal("x", &text).contains("@embed"));
}

#[test]
fn refuses_nearest_over_a_string_literal() {
    let query = "--- query\nquery q() {\n    match { $p: Person }\n    return { $p.name }\n    order { nearest($p.name, \"alpha\") }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n");
    assert!(refusal("x", &text).contains("string argument"));
}

#[test]
fn refuses_nearest_over_a_string_param() {
    let query = "--- query\nquery q($q: String) {\n    match { $p: Person }\n    return { $p.name }\n    order { nearest($p.name, $q) }\n}\n";
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{query}--- params\n{{\"q\": \"alpha\"}}\n--- expect unordered\n"
    );
    assert!(refusal("x", &text).contains("string argument"));
}

#[test]
fn accepts_empty_expect_body_as_empty_result_assertion() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n");
    parse_case("x", &text).unwrap();
}

#[test]
fn search_construct_sets_the_index_decision() {
    let schema = "--- schema\nnode Doc {\n    slug: String @key\n    text: String @index\n}\n";
    let query = "--- query\nquery q($q: String) {\n    match {\n        $d: Doc\n        search($d.text, $q)\n    }\n    return { $d.slug }\n}\n";
    let text = format!(
        "{HDR}{schema}--- seed\n{query}--- params\n{{\"q\": \"needle\"}}\n--- expect unordered\n"
    );
    let case = parse_case("x", &text).unwrap();
    assert!(case.needs_indices);
}

#[test]
fn normalization_equates_integer_and_float_spellings() {
    let a: Value = serde_json::from_str("{\"total\": 2}").unwrap();
    let b: Value = serde_json::from_str("{\"total\": 2.0}").unwrap();
    assert_eq!(canonical_json(&a), canonical_json(&b));
}

#[test]
fn normalization_does_not_collapse_large_integers() {
    let a: Value = serde_json::from_str("{\"n\": 9007199254740993}").unwrap();
    let b: Value = serde_json::from_str("{\"n\": 9007199254740992}").unwrap();
    assert_ne!(canonical_json(&a), canonical_json(&b));
}

#[test]
fn normalization_ignores_noise_below_scale_12() {
    let a: Value = serde_json::from_str("{\"x\": 0.1000000000000001}").unwrap();
    let b: Value = serde_json::from_str("{\"x\": 0.1}").unwrap();
    assert_eq!(canonical_json(&a), canonical_json(&b));
}

#[test]
fn canonical_form_sorts_object_keys_and_recurses() {
    let a: Value = serde_json::from_str("{\"b\": [{\"z\": 1, \"a\": 2}], \"a\": null}").unwrap();
    assert_eq!(canonical_json(&a), "{\"a\":null,\"b\":[{\"a\":2,\"z\":1}]}");
}

#[test]
fn unordered_comparison_is_multiset_equality() {
    let rows = |s: &str| -> Vec<Value> {
        s.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    };
    let expected = rows("{\"n\": 1}\n{\"n\": 1}\n{\"n\": 2}");
    let actual = rows("{\"n\": 2}\n{\"n\": 1}\n{\"n\": 1}");
    compare_rows(&expected, &actual, false).unwrap();
    let missing_dup = rows("{\"n\": 1}\n{\"n\": 2}");
    assert!(compare_rows(&expected, &missing_dup, false).is_err());
}

#[test]
fn ordered_comparison_is_positional() {
    let rows = |s: &str| -> Vec<Value> {
        s.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    };
    let expected = rows("{\"n\": 1}\n{\"n\": 2}");
    let swapped = rows("{\"n\": 2}\n{\"n\": 1}");
    assert!(compare_rows(&expected, &swapped, true).is_err());
    compare_rows(&expected, &expected.clone(), true).unwrap();
}

#[tokio::test]
async fn execution_reports_a_row_mismatch() {
    let text =
        format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n");
    let case = parse_case("mismatch", &text).unwrap();
    let err = execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap_err();
    assert!(err.contains("row mismatch"), "got: {err}");
    assert!(err.contains("step 1 (query)"), "got: {err}");
}

#[tokio::test]
async fn bless_rewrites_the_failing_expect_and_converges() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bless_case.gqt");
    let text =
        format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n");
    std::fs::write(&path, &text).unwrap();
    let case = parse_case("bless_case", &text).unwrap();
    let err = execute_case(&case, &path, true).await.unwrap_err();
    assert!(err.contains("expect rewritten"), "got: {err}");

    let blessed = std::fs::read_to_string(&path).unwrap();
    assert!(blessed.contains("{\"p.name\":\"alice\"}"), "got: {blessed}");
    let case = parse_case("bless_case", &blessed).unwrap();
    execute_case(&case, &path, false).await.unwrap();
}

#[test]
fn refuses_duplicate_issue_header() {
    let text = format!("{HDR}# issue: none\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("duplicate"));
}

#[test]
fn refuses_empty_red_on_value() {
    let text = format!("# issue: 7\n# red_on: \n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7_x", &text).contains("needs a value"));
    let text = format!("# issue: 7\n# red_on:\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7_x", &text).contains("not `# <key>: <value>`"));
}

#[test]
fn refuses_noncanonical_issue_header_number() {
    let text = format!("# issue: 0563\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_563_x", &text).contains("no sign or leading zeros"));
    let text = format!("# issue: +563\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_563_x", &text).contains("no sign or leading zeros"));
}

#[test]
fn refuses_leading_zero_issue_digits() {
    let text = format!("# issue: 7\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_007_x", &text).contains("leading zeros"));
}

#[test]
fn refuses_arguments_on_bare_sections() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- restart now\n");
    assert!(refusal("x", &text).contains("takes no arguments"));
    let junk_query =
        "--- query fast\nquery all() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{junk_query}{EXPECT}");
    assert!(refusal("x", &text).contains("takes no arguments"));
}

#[test]
fn refuses_crlf_line_endings() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}").replace('\n', "\r\n");
    assert!(refusal("x", &text).contains("line endings"));
}

#[test]
fn refuses_loop_range_over_the_cap() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 10001\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("10000 cap"));
}

#[test]
fn refuses_signed_or_padded_numeric_tokens() {
    let text =
        format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}--- expect affected: nodes=+1 edges=0\n");
    assert!(refusal("x", &text).contains("nodes=<N> edges=<M>"));
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 00 2\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("plain decimal"));
}

#[test]
fn refuses_ok_expect_with_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}--- expect ok\nstray\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_affected_expect_with_body() {
    let text =
        format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}--- expect affected: nodes=1 edges=0\nstray\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_schema_inside_a_loop() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- foreach $x a\n{SCHEMA}{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("out of position"));
}

#[test]
fn refuses_loop_headers_with_a_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 2\nstray\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("carries no body"));
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 2\n{QUERY}{EXPECT}--- endloop\nstray\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_nearest_over_a_string_property() {
    let query = "--- query\nquery q() {\n    match { $p: Person }\n    return { $p.name }\n    order { nearest($p.name, $p.name) }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n");
    assert!(refusal("x", &text).contains("vector parameter"));
}

#[test]
fn traversal_header_forces_index_builds() {
    let text = format!("{HDR}# traversal: indexed\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    let case = parse_case("x", &text).unwrap();
    assert!(case.needs_indices);
    assert_eq!(case.traversal, Some("indexed"));
}

#[test]
fn pin_violation_names_the_path_that_ran() {
    assert_eq!(pin_violation("indexed", 3, 0, true), None);
    assert_eq!(pin_violation("csr", 0, 2, true), None);
    assert_eq!(pin_violation("indexed", 0, 0, false), None);
    let v = pin_violation("indexed", 1, 2, false).unwrap();
    assert!(
        v.contains("pinned `indexed`, ran `csr` on 2 expand(s)"),
        "{v}"
    );
    let v = pin_violation("csr", 4, 0, false).unwrap();
    assert!(
        v.contains("pinned `csr`, ran `indexed` on 4 expand(s)"),
        "{v}"
    );
    // A step that must expand and shows nothing on the pinned path: the
    // pin and the probes were dropped together.
    let v = pin_violation("indexed", 0, 0, true).unwrap();
    assert!(v.contains("no expand ran on it"), "{v}");
    let v = pin_violation("csr", 0, 0, true).unwrap();
    assert!(v.contains("no expand ran on it"), "{v}");
}

#[test]
fn expects_expand_ignores_bound_edges_and_plain_bindings() {
    let unbound = parse_query(TRAVERSAL_QUERY.trim_start_matches("--- query\n")).unwrap();
    assert!(expects_expand(&unbound.queries[0].match_clause));
    let bound = "query f($n: String) {\n    match {\n        $a: Person\n        $a.name = $n\n        \
                 $a $k:knows $b\n    }\n    return { $b.name }\n}\n";
    let bound = parse_query(bound).unwrap();
    assert!(!expects_expand(&bound.queries[0].match_clause));
    let plain = parse_query(QUERY.trim_start_matches("--- query\n")).unwrap();
    assert!(!expects_expand(&plain.queries[0].match_clause));
    let negated = "query f() {\n    match {\n        $a: Person\n        not { $a knows $x }\n    }\n    \
                   return { $a.name }\n}\n";
    let negated = parse_query(negated).unwrap();
    assert!(!expects_expand(&negated.queries[0].match_clause));
}

/// A two-node, one-edge graph with a one-hop traversal, for the pin tests.
const TRAVERSAL_SCHEMA: &str = "--- schema\nnode Person {\n    name: String @key\n}\n\n\
                                edge Knows: Person -> Person {\n    since: I64\n}\n";
const TRAVERSAL_SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n\
                              {\"type\":\"Person\",\"data\":{\"name\":\"bob\"}}\n\
                              {\"edge\":\"Knows\",\"from\":\"alice\",\"to\":\"bob\",\"data\":{\"id\":\"k-1\",\"since\":2020}}\n";
const TRAVERSAL_QUERY: &str = "--- query\nquery friends($n: String) {\n    match {\n        $a: Person\n        \
                               $a.name = $n\n        $a knows $b\n    }\n    return { $b.name }\n}\n";
const TRAVERSAL_PARAMS: &str = "--- params\n{\"n\": \"alice\"}\n";
const TRAVERSAL_EXPECT: &str = "--- expect unordered\n{\"b.name\": \"bob\"}\n";

/// The pin reaches the executor on both paths: a pinned step runs its
/// expands on the pinned path only, and the probes see them (a zero count
/// on both paths would make the check vacuous).
#[tokio::test]
async fn pinned_step_runs_only_its_pinned_path() {
    for (mode, expect_indexed) in [("indexed", true), ("csr", false)] {
        let text = format!(
            "{HDR}# traversal: {mode}\n{TRAVERSAL_SCHEMA}{TRAVERSAL_SEED}{TRAVERSAL_QUERY}{TRAVERSAL_PARAMS}{TRAVERSAL_EXPECT}"
        );
        let case = parse_case("pinned", &text).unwrap();
        execute_case(&case, Path::new("unused.gqt"), false)
            .await
            .unwrap_or_else(|e| panic!("{mode}: {e}"));

        let (db, _uri, _dir) = open_case_store(&case).await.unwrap();
        let Some(Item::Step(Step::Query(step))) = case.items.first() else {
            panic!("first item is the query step");
        };
        let params = build_params(step.params_raw.as_ref(), &step.ast_params, None).unwrap();
        let (outcome, counts) = under_traversal(
            Some(mode),
            db.query(
                ReadTarget::branch("main"),
                &step.source,
                &step.name,
                &params,
            ),
        )
        .await;
        outcome.unwrap();
        let counts = counts.unwrap();
        let (indexed, csr) = (
            counts.indexed.load(Ordering::Relaxed),
            counts.csr.load(Ordering::Relaxed),
        );
        if expect_indexed {
            assert!(
                indexed >= 1 && csr == 0,
                "{mode}: indexed={indexed} csr={csr}"
            );
        } else {
            assert!(
                csr >= 1 && indexed == 0,
                "{mode}: indexed={indexed} csr={csr}"
            );
        }
    }
}

#[test]
fn refuses_ordered_expect_on_an_rrf_led_order() {
    let query = "--- query\nquery q($v: Vector(4), $t: String) {\n    match { $p: Person }\n    \
                 return { $p.name }\n    order { rrf(nearest($p.vec, $v), bm25($p.name, $t)) }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect ordered\n");
    let message = refusal("x", &text);
    assert!(message.contains("led by `rrf()`"), "{message}");
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n");
    parse_case("x", &text).unwrap();
}

#[test]
fn refuses_ordered_expect_with_an_aggregate_in_return() {
    let query = "--- query\nquery q($t: String) {\n    match { $p: Person\n        search($p.name, $t) }\n    \
                 return { count($p) as total }\n    order { bm25($p.name, $t) }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect ordered\n");
    assert!(refusal("x", &text).contains("aggregate in its `return` list"));
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n");
    parse_case("x", &text).unwrap();
}

fn synthetic_cases(names: &[&str]) -> Vec<(String, PathBuf)> {
    names
        .iter()
        .map(|n| ((*n).to_string(), PathBuf::from(format!("{n}.gqt"))))
        .collect()
}

fn no_report(_: &CaseOutcome) {}

#[tokio::test(flavor = "multi_thread")]
async fn walker_bounds_cases_in_flight() {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let names: Vec<String> = (0..12).map(|i| format!("c{i:02}")).collect();
    let cases = synthetic_cases(&names.iter().map(String::as_str).collect::<Vec<_>>());
    // Every permit holder waits at a 3-party barrier, so the three are in
    // flight together (max == 3) or the walker admitted fewer than three
    // and the barrier never releases: the outer timeout turns that hang
    // into a failure instead of a stall. Twelve cases = four generations.
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let (counter, max) = (Arc::clone(&in_flight), Arc::clone(&max_seen));
    let runner: CaseRunner = Arc::new(move |_| {
        let (counter, max, barrier) =
            (Arc::clone(&counter), Arc::clone(&max), Arc::clone(&barrier));
        Box::pin(async move {
            let now = counter.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(now, Ordering::SeqCst);
            barrier.wait().await;
            counter.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        })
    });
    let outcomes = tokio::time::timeout(
        Duration::from_secs(30),
        run_bounded(cases, 3, Duration::from_secs(10), runner, &no_report),
    )
    .await
    .expect("fewer than three cases in flight: the barrier never released");
    assert_eq!(outcomes.len(), 12);
    assert!(outcomes.iter().all(|o| o.result.is_ok()), "{outcomes:?}");
    assert_eq!(max_seen.load(Ordering::SeqCst), 3);
    assert_eq!(in_flight.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn walker_reports_in_completion_order_and_returns_sorted() {
    // Completion is forced into the order c2, c1, c0 by hand-offs, not by
    // sleep lengths: c2 returns at once; c1 waits until c2 is reported;
    // c0 waits until c1 is reported.
    let gates: Arc<[tokio::sync::Notify; 2]> =
        Arc::new([tokio::sync::Notify::new(), tokio::sync::Notify::new()]);
    let runner_gates = Arc::clone(&gates);
    let runner: CaseRunner = Arc::new(move |path| {
        let gates = Arc::clone(&runner_gates);
        let idx: usize = path.file_stem().unwrap().to_str().unwrap()[1..]
            .parse()
            .unwrap();
        Box::pin(async move {
            if idx < 2 {
                gates[idx].notified().await;
            }
            Ok(())
        })
    });
    let reported = std::sync::Mutex::new(Vec::new());
    let report = |o: &CaseOutcome| {
        reported.lock().unwrap().push(o.stem.clone());
        match o.stem.as_str() {
            "c2" => gates[1].notify_one(),
            "c1" => gates[0].notify_one(),
            _ => {}
        }
    };
    let cases = synthetic_cases(&["c0", "c1", "c2"]);
    let outcomes = tokio::time::timeout(
        Duration::from_secs(30),
        run_bounded(cases, 3, Duration::from_secs(10), runner, &report),
    )
    .await
    .expect("a hand-off never arrived");
    let returned: Vec<&str> = outcomes.iter().map(|o| o.stem.as_str()).collect();
    assert_eq!(returned, ["c0", "c1", "c2"]);
    assert_eq!(*reported.lock().unwrap(), ["c2", "c1", "c0"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn walker_budget_starts_at_the_permit_not_at_spawn() {
    let runner: CaseRunner = Arc::new(|_| {
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(())
        })
    });
    let cases = synthetic_cases(&["a", "b", "c"]);
    // One permit: the third case waits ~600 ms in the queue, past the
    // 500 ms budget that its own 300 ms of work stays under; the margins
    // are wide because libtest runs this beside the corpus walker.
    let outcomes = run_bounded(cases, 1, Duration::from_millis(500), runner, &no_report).await;
    assert!(outcomes.iter().all(|o| o.result.is_ok()), "{outcomes:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn walker_fails_a_case_over_its_budget_and_runs_the_rest() {
    let runner: CaseRunner = Arc::new(|path| {
        Box::pin(async move {
            if path.starts_with("slow.gqt") {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Ok(())
        })
    });
    let cases = synthetic_cases(&["slow", "quick"]);
    let outcomes = run_bounded(cases, 1, Duration::from_millis(50), runner, &no_report).await;
    let quick = &outcomes[0];
    assert_eq!(quick.stem, "quick");
    assert!(quick.result.is_ok(), "{quick:?}");
    let slow = &outcomes[1];
    assert_eq!(slow.stem, "slow");
    assert!(
        slow.elapsed < Duration::from_secs(5),
        "timeout did not cut the case short"
    );
    let err = slow.result.as_ref().unwrap_err();
    assert!(err.contains("budget of 0.05s"), "{err}");
    assert!(err.contains("up to 1 cases"), "{err}");
    assert!(err.contains(CASE_TIMEOUT_ENV), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn walker_records_a_panicking_case_and_runs_the_rest() {
    let runner: CaseRunner = Arc::new(|path| {
        Box::pin(async move {
            if path.starts_with("p.gqt") {
                panic!("boom");
            }
            Ok(())
        })
    });
    let cases = synthetic_cases(&["p", "q"]);
    let outcomes = run_bounded(cases, 1, Duration::from_secs(10), runner, &no_report).await;
    let err = outcomes[0].result.as_ref().unwrap_err();
    assert!(err.starts_with("case panicked: boom"), "{err}");
    assert!(outcomes[1].result.is_ok(), "{:?}", outcomes[1]);
}

#[test]
fn walker_refuses_a_process_traversal_override() {
    assert!(traversal_override_refusal(None).is_none());
    let reason = traversal_override_refusal(Some(OsStr::new("csr"))).unwrap();
    assert!(reason.contains("OMNIGRAPH_TRAVERSAL_MODE=csr"), "{reason}");
    assert!(reason.contains("# traversal:"), "{reason}");
}

/// Same name battery as `scripts/check-fix-regression.py --self-test`
/// (`corpus_case`): the walker and the gate must agree on what a case is.
#[test]
fn walker_flags_foreign_corpus_entries() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.gqt"), "x").unwrap();
    std::fs::write(dir.path().join("b.txt"), "x").unwrap();
    std::fs::write(dir.path().join(".hidden.gqt"), "x").unwrap();
    std::fs::write(dir.path().join(".DS_Store"), "x").unwrap();
    std::fs::write(dir.path().join("c.GQT"), "x").unwrap();
    std::fs::create_dir(dir.path().join("nested")).unwrap();
    std::fs::write(dir.path().join("nested").join("d.gqt"), "x").unwrap();
    let (files, foreign) = list_cases(dir.path());
    assert_eq!(files, vec![dir.path().join("a.gqt")]);
    assert_eq!(
        foreign,
        vec![
            ".hidden.gqt".to_string(),
            "b.txt".to_string(),
            "c.GQT".to_string(),
            "nested".to_string()
        ]
    );
}

#[tokio::test]
async fn bless_refuses_cases_containing_loops() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bless_loop_case.gqt");
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- foreach $x a b\n{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n--- endloop\n"
    );
    std::fs::write(&path, &text).unwrap();
    let case = parse_case("bless_loop_case", &text).unwrap();
    let err = execute_case(&case, &path, true).await.unwrap_err();
    assert!(err.contains("bless: refused"), "got: {err}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
}

#[tokio::test]
async fn bless_never_rewrites_on_a_kind_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bless_kind_case.gqt");
    let query = "--- query\nquery q() {\n    match { $p: Person }\n    return { $p.nope }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n{{\"p.nope\": \"x\"}}\n");
    std::fs::write(&path, &text).unwrap();
    let case = parse_case("bless_kind_case", &text).unwrap();
    let err = execute_case(&case, &path, true).await.unwrap_err();
    assert!(err.contains("query failed"), "got: {err}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
}

#[tokio::test]
async fn params_refusal_satisfies_an_error_expect() {
    let query = "--- query\nquery q($q: String) {\n    match {\n        $p: Person\n        $p.name = $q\n    }\n    return { $p.name }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect error: q\n");
    let case = parse_case("x", &text).unwrap();
    execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap();
}

#[test]
fn bless_splice_preserves_the_trailing_blank_separator() {
    let original = "--- expect unordered\nold\n\n--- restart\n";
    let span = BodySpan {
        start_line: 1,
        len: 2,
    };
    let rows = vec!["{\"n\":1}".to_string()];
    let out = splice_lines(original, span, &rows);
    assert_eq!(out, "--- expect unordered\n{\"n\":1}\n\n--- restart\n");
}

#[test]
fn bless_splice_replaces_only_the_expect_body() {
    let original = "--- query\nq\n--- expect unordered\nold row\nold row 2\n--- restart\n";
    let span = BodySpan {
        start_line: 3,
        len: 2,
    };
    let rows = vec!["{\"n\":1}".to_string()];
    let out = splice_lines(original, span, &rows);
    assert_eq!(
        out,
        "--- query\nq\n--- expect unordered\n{\"n\":1}\n--- restart\n"
    );
}

#[test]
fn bless_splice_inserts_into_an_empty_expect_body() {
    let original = "--- expect unordered\n--- restart\n";
    let span = BodySpan {
        start_line: 1,
        len: 0,
    };
    let rows = vec!["{\"n\":1}".to_string()];
    let out = splice_lines(original, span, &rows);
    assert_eq!(out, "--- expect unordered\n{\"n\":1}\n--- restart\n");
}
