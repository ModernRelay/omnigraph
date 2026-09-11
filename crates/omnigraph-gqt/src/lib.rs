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

use std::collections::{BTreeMap, HashSet};
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::FutureExt as _;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes, with_traversal_mode};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::query::ast::{
    BranchStmt, BranchWrite, Clause, Expr, Literal, Param, QueryDecl, QueryFile,
};
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::query::typecheck::{
    executed_column_name, infer_query_result_schema, typecheck_query,
};
use omnigraph_compiler::schema::ast::{Annotation, PropDecl, SchemaDecl};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{JsonParamMode, QueryResult, json_params_to_param_map};
use serde_json::Value;

mod dst_runner;
mod runner_config;
pub use dst_runner::{
    replay_report, report_cli_refusal, run_corpus_case, run_selected, run_worker_if_requested,
};
use omnigraph::storage::StorageAdapter;
use runner_config::{
    Execution, Fault, KnownFailure, RunnerConfig, parse_fault, parse_known_failure, parse_runner,
};

mod shape;
use shape::{ShapeExpect, bless_shape_lines, parse_shape_body, shape_mismatch};

pub const CASE_TIMEOUT_ENV: &str = "OMNIGRAPH_GQ_CASE_TIMEOUT_SECS";
pub const DEFAULT_CASE_TIMEOUT_SECS: u64 = 10;
pub const BLESS_ENV: &str = "OMNIGRAPH_GQ_BLESS";

#[derive(Debug)]
struct Case {
    input_text: String,
    runner: RunnerConfig,
    known_failure: Option<KnownFailure>,
    faults: BTreeMap<usize, Fault>,
    source_lines: BTreeMap<usize, usize>,
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
    Control(ControlStep),
    List(ListStep),
    Restart { ordinal: usize },
}

impl Step {
    fn ordinal(&self) -> usize {
        match self {
            Self::Query(s) => s.ordinal,
            Self::Mutate(s) => s.ordinal,
            Self::Control(s) => s.ordinal,
            Self::List(s) => s.ordinal,
            Self::Restart { ordinal } => *ordinal,
        }
    }

    /// The rows-or-error expect of a read step (`--- query`), which is the
    /// one kind that carries a shape section.
    fn read_expect(&self) -> Option<&QueryExpect> {
        match self {
            Step::Query(step) => Some(&step.expect),
            Step::List(step) => Some(&step.expect),
            Step::Mutate(_) | Step::Control(_) | Step::Restart { .. } => None,
        }
    }

    fn read_expect_mut(&mut self) -> Option<&mut QueryExpect> {
        match self {
            Step::Query(step) => Some(&mut step.expect),
            Step::List(step) => Some(&mut step.expect),
            Step::Mutate(_) | Step::Control(_) | Step::Restart { .. } => None,
        }
    }
}

#[derive(Debug)]
struct QueryStep {
    ordinal: usize,
    source: String,
    name: String,
    /// The `branch: <name>` header argument; `main` when unspelled.
    branch: String,
    decl: Box<QueryDecl>,
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
    /// The `branch: <name>` header argument, as `QueryStep::branch`.
    branch: String,
    ast_params: Vec<Param>,
    params_raw: Option<String>,
    expect: MutateExpect,
}

/// A `--- mutate` step holding a control write. The expect lives inside
/// the write so `outcome:` is spellable on a merge and on nothing else.
#[derive(Debug)]
struct ControlStep {
    ordinal: usize,
    /// The statement's two words, from `BranchWrite::statement_name`.
    name: &'static str,
    write: ControlWrite,
}

#[derive(Debug)]
enum ControlWrite {
    Create {
        name: String,
        from: Option<String>,
        expect: WriteExpect,
    },
    Delete {
        name: String,
        expect: WriteExpect,
    },
    Merge {
        source: String,
        into: Option<String>,
        expect: MergeExpect,
    },
}

#[derive(Debug)]
enum WriteExpect {
    Ok,
    Error { needle: String },
}

/// A `branch merge`'s expect: what any control write takes, plus the
/// `outcome:` word only a merge has an answer for.
#[derive(Debug)]
enum MergeExpect {
    Write(WriteExpect),
    Outcome(MergeOutcome),
}

/// A `--- query` step holding `branch list`: one `name` column, rows in
/// byte order, so its expect is a read expect like a declaration's.
#[derive(Debug)]
struct ListStep {
    ordinal: usize,
    expect: QueryExpect,
}

/// `main`, the default wherever a branch is unspelled.
const MAIN_BRANCH: &str = "main";

/// The `outcome:` word of each `MergeOutcome`, the spelling
/// `BranchMergeOutcome` carries on the wire (`omnigraph-api-types`).
fn merge_outcome_word(outcome: MergeOutcome) -> &'static str {
    match outcome {
        MergeOutcome::AlreadyUpToDate => "already_up_to_date",
        MergeOutcome::FastForward => "fast_forward",
        MergeOutcome::Merged => "merged",
    }
}

const MERGE_OUTCOMES: [MergeOutcome; 3] = [
    MergeOutcome::AlreadyUpToDate,
    MergeOutcome::FastForward,
    MergeOutcome::Merged,
];

/// The `outcome:` words as a refusal spells them, off `MERGE_OUTCOMES`, so a
/// fourth variant widens every message with the match.
fn merge_outcome_words() -> String {
    let words = MERGE_OUTCOMES.map(|o| format!("`{}`", merge_outcome_word(o)));
    let (last, head) = words
        .split_last()
        .expect("invariant: MERGE_OUTCOMES names every MergeOutcome");
    if head.is_empty() {
        return last.clone();
    }
    format!("{}, or {last}", head.join(", "))
}

#[derive(Debug)]
enum QueryExpect {
    Rows {
        ordered: bool,
        body_raw: String,
        span: BodySpan,
        shape: ShapeExpect,
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
    Outcome(MergeOutcome),
}

/// The one argument a `--- query` or `--- mutate` header takes,
/// `branch: <name>` (a word, a colon, the trimmed remainder, as
/// `--- expect error: <substring>`); `None` when the header is bare.
fn parse_step_branch(kind: &str, rest: &str) -> Result<Option<String>, String> {
    let rest = rest.trim();
    if rest.is_empty() {
        return Ok(None);
    }
    let Some(name) = rest.strip_prefix("branch:") else {
        return Err(format!(
            "`--- {kind}` takes no arguments but `branch: <name>`, got `{rest}`"
        ));
    };
    let name = name.trim();
    if name.is_empty() {
        return Err(format!("`--- {kind} branch:` needs a branch name"));
    }
    Ok(Some(name.to_string()))
}

fn parse_expect_header(rest: &str) -> Result<ExpectHeader, String> {
    let rest = rest.trim();
    if rest.is_empty() {
        return Err("a bare `--- expect` is refused; give a mode word".into());
    }
    if let Some(word) = rest.strip_prefix("outcome:") {
        let word = word.trim();
        if word.is_empty() {
            return Err(format!(
                "`expect outcome:` needs a word: {}",
                merge_outcome_words()
            ));
        }
        let Some(outcome) = MERGE_OUTCOMES
            .into_iter()
            .find(|o| merge_outcome_word(*o) == word)
        else {
            return Err(format!(
                "`expect outcome:` takes {}, got `{word}`",
                merge_outcome_words()
            ));
        };
        return Ok(ExpectHeader::Outcome(outcome));
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
enum Pending {
    Decl(PendingStep),
    List { ordinal: usize },
    Control { ordinal: usize, write: BranchWrite },
}

struct PendingStep {
    is_mutation: bool,
    ordinal: usize,
    source: String,
    name: String,
    branch: String,
    decl: Box<QueryDecl>,
    ordered_refusal: Option<String>,
    expects_expand: bool,
    params_raw: Option<String>,
}

/// The rows expect of a read step: the body under its substitution rule,
/// with the empty shape the mandatory `--- expect shape` section fills in.
fn rows_expect(
    ordered: bool,
    section: &Section<'_>,
    loop_var: Option<&str>,
) -> Result<QueryExpect, String> {
    refuse_comment_lines(&section.body, "expect")?;
    let body: String = section
        .body
        .iter()
        .map(|(_, l)| *l)
        .collect::<Vec<_>>()
        .join("\n");
    validate_subst_tokens(&body, loop_var)?;
    Ok(QueryExpect::Rows {
        ordered,
        body_raw: body,
        span: BodySpan {
            start_line: section.header_line + 1,
            len: section.body.len(),
        },
        shape: ShapeExpect {
            lines: Vec::new(),
            span: BodySpan {
                start_line: 0,
                len: 0,
            },
        },
    })
}

/// The step a `branch list` section becomes under `mode`. A rows expect
/// needs a shape section next, as for a declaration.
fn complete_list_step(
    ordinal: usize,
    mode: &ExpectHeader,
    section: &Section<'_>,
    loop_var: Option<&str>,
) -> Result<ListStep, String> {
    let expect = match mode {
        ExpectHeader::Unordered | ExpectHeader::Ordered => {
            rows_expect(matches!(mode, ExpectHeader::Ordered), section, loop_var)?
        }
        ExpectHeader::Error(needle) => {
            refuse_nonempty_body(&section.body, "an `expect error:` section")?;
            QueryExpect::Error {
                needle: needle.clone(),
            }
        }
        ExpectHeader::Ok | ExpectHeader::Affected { .. } | ExpectHeader::Outcome(_) => {
            return Err("`branch list` takes `unordered`, `ordered`, or `error:`".into());
        }
    };
    Ok(ListStep { ordinal, expect })
}

fn affected_refusal(name: &str) -> String {
    format!("`expect affected:` is refused on a control write; `{name}` carries no counts")
}

fn rows_refusal(name: &str) -> String {
    format!("a control write takes `ok` or `error:`; `{name}` returns no rows")
}

/// The expect of a `branch create` or `branch delete` step under `mode`,
/// or why the mode is refused for it.
fn complete_write_expect(
    name: &str,
    mode: &ExpectHeader,
    section: &Section<'_>,
) -> Result<WriteExpect, String> {
    match mode {
        ExpectHeader::Ok => {
            refuse_nonempty_body(&section.body, "an `expect ok` section")?;
            Ok(WriteExpect::Ok)
        }
        ExpectHeader::Error(needle) => {
            refuse_nonempty_body(&section.body, "an `expect error:` section")?;
            Ok(WriteExpect::Error {
                needle: needle.clone(),
            })
        }
        ExpectHeader::Outcome(_) => {
            Err("`expect outcome:` is accepted on a `branch merge` step only".into())
        }
        ExpectHeader::Affected { .. } => Err(affected_refusal(name)),
        ExpectHeader::Unordered | ExpectHeader::Ordered => Err(rows_refusal(name)),
    }
}

/// The expect of a `branch merge` step: `complete_write_expect`'s modes,
/// plus the `outcome:` word only a merge has an answer for.
fn complete_merge_expect(
    mode: &ExpectHeader,
    section: &Section<'_>,
) -> Result<MergeExpect, String> {
    if let ExpectHeader::Outcome(outcome) = mode {
        refuse_nonempty_body(&section.body, "an `expect outcome:` section")?;
        return Ok(MergeExpect::Outcome(*outcome));
    }
    Ok(MergeExpect::Write(complete_write_expect(
        "branch merge",
        mode,
        section,
    )?))
}

/// The step a control write becomes under `mode`.
fn complete_control_step(
    ordinal: usize,
    write: BranchWrite,
    mode: &ExpectHeader,
    section: &Section<'_>,
) -> Result<ControlStep, String> {
    let name = write.statement_name();
    let write = match write {
        BranchWrite::Create { name: branch, from } => ControlWrite::Create {
            name: branch,
            from,
            expect: complete_write_expect(name, mode, section)?,
        },
        BranchWrite::Delete { name: branch } => ControlWrite::Delete {
            name: branch,
            expect: complete_write_expect(name, mode, section)?,
        },
        BranchWrite::Merge { source, into } => ControlWrite::Merge {
            source,
            into,
            expect: complete_merge_expect(mode, section)?,
        },
    };
    Ok(ControlStep {
        ordinal,
        name,
        write,
    })
}

/// The step a declaration section becomes under `mode`, or why the mode is
/// refused for it.
fn complete_decl_step(
    step: PendingStep,
    mode: &ExpectHeader,
    section: &Section<'_>,
    loop_var: Option<&str>,
) -> Result<Step, String> {
    match (mode, step.is_mutation) {
        (ExpectHeader::Outcome(_), _) => {
            Err("`expect outcome:` is accepted on a `branch merge` step only".into())
        }
        (ExpectHeader::Unordered | ExpectHeader::Ordered, true) => Err(
            "a mutate step takes `ok`, `affected:`, or `error:`; mutation results carry no rows"
                .into(),
        ),
        (ExpectHeader::Ok | ExpectHeader::Affected { .. }, false) => {
            Err("a query step takes `unordered`, `ordered`, or `error:`".into())
        }
        (ExpectHeader::Unordered | ExpectHeader::Ordered, false) => {
            let ordered = matches!(mode, ExpectHeader::Ordered);
            if ordered && let Some(reason) = step.ordered_refusal {
                return Err(reason);
            }
            Ok(Step::Query(QueryStep {
                ordinal: step.ordinal,
                source: step.source,
                name: step.name,
                branch: step.branch,
                decl: step.decl,
                params_raw: step.params_raw,
                expects_expand: step.expects_expand,
                expect: rows_expect(ordered, section, loop_var)?,
            }))
        }
        (ExpectHeader::Error(needle), is_mutation) => {
            refuse_nonempty_body(&section.body, "an `expect error:` section")?;
            let needle = needle.clone();
            Ok(if is_mutation {
                Step::Mutate(MutateStep {
                    ordinal: step.ordinal,
                    source: step.source,
                    name: step.name,
                    branch: step.branch,
                    ast_params: step.decl.params,
                    params_raw: step.params_raw,
                    expect: MutateExpect::Error { needle },
                })
            } else {
                Step::Query(QueryStep {
                    ordinal: step.ordinal,
                    source: step.source,
                    name: step.name,
                    branch: step.branch,
                    decl: step.decl,
                    params_raw: step.params_raw,
                    expects_expand: step.expects_expand,
                    expect: QueryExpect::Error { needle },
                })
            })
        }
        (ExpectHeader::Ok, true) => {
            refuse_nonempty_body(&section.body, "an `expect ok` section")?;
            Ok(Step::Mutate(MutateStep {
                ordinal: step.ordinal,
                source: step.source,
                name: step.name,
                branch: step.branch,
                ast_params: step.decl.params,
                params_raw: step.params_raw,
                expect: MutateExpect::Ok,
            }))
        }
        (ExpectHeader::Affected { nodes, edges }, true) => {
            refuse_nonempty_body(&section.body, "an `expect affected:` section")?;
            Ok(Step::Mutate(MutateStep {
                ordinal: step.ordinal,
                source: step.source,
                name: step.name,
                branch: step.branch,
                ast_params: step.decl.params,
                params_raw: step.params_raw,
                expect: MutateExpect::Affected {
                    nodes: *nodes,
                    edges: *edges,
                },
            }))
        }
    }
}

fn parse_case(stem: &str, text: &str) -> Result<Case, String> {
    if text.contains('\r') {
        return Err("case files are UTF-8 with `\\n` line endings; `\\r` found".into());
    }
    let lines: Vec<&str> = text.lines().collect();
    let (header_lines, sections) = split_sections(&lines);
    let header = parse_header(&header_lines)?;
    check_file_name(stem, &header.issue)?;

    let (runner, sections) = if sections.first().is_some_and(|s| s.name == "runner") {
        let body = sections[0]
            .body
            .iter()
            .map(|(_, line)| *line)
            .collect::<Vec<_>>()
            .join("\n");
        (parse_runner(&body)?, &sections[1..])
    } else {
        return Err(
            "invalid_case: the first section must be `--- runner` with explicit configuration"
                .into(),
        );
    };
    let (known_failure, sections) = if sections.first().is_some_and(|s| s.name == "known_failure") {
        let body = sections[0]
            .body
            .iter()
            .map(|(_, line)| *line)
            .collect::<Vec<_>>()
            .join("\n");
        (Some(parse_known_failure(&body)?), &sections[1..])
    } else {
        (None, sections)
    };
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
    let mut pending: Option<Pending> = None;
    let mut awaiting_shape: Option<Step> = None;
    let mut ordinal = 0usize;
    let mut faults = BTreeMap::new();
    let mut source_lines = BTreeMap::new();
    let mut awaiting_fault_step = false;
    let mut qm_steps = 0usize;
    let mut substitutable_lines: HashSet<usize> = HashSet::new();

    /// The refusal for a rows step whose `--- expect shape` did not arrive
    /// next: any other section, or the end of the file, ends the case here.
    fn missing_shape(step: &Step) -> String {
        let line = match step.read_expect() {
            Some(QueryExpect::Rows { span, .. }) => span.start_line,
            Some(QueryExpect::Error { .. }) | None => 0,
        };
        format!(
            "line {line}: the rows expect needs an `--- expect shape` section directly after it; write it from the .pg schema, or fill it with OMNIGRAPH_GQ_BLESS=1 and review the diff"
        )
    }

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
        let (kind, rest) = match section.name.split_once([' ', '\t']) {
            Some((k, rest)) => (k, rest),
            None => (section.name.as_str(), ""),
        };
        if let Some(waiting) = &awaiting_shape
            && !(kind == "expect" && rest.trim().starts_with("shape"))
        {
            return Err(missing_shape(waiting));
        }
        if awaiting_fault_step && !matches!(kind, "query" | "mutate") {
            return Err(
                "invalid_case: a fault must directly precede its query or mutate step".into(),
            );
        }
        if matches!(kind, "query" | "mutate" | "restart") {
            source_lines.insert(ordinal + 1, section.header_line + 1);
            awaiting_fault_step = false;
        }
        match kind {
            "fault" => {
                if open_loop.is_some() {
                    return Err(
                        "invalid_case: fault directives inside loops are not supported".into(),
                    );
                }
                if faults.len() >= 16 {
                    return Err("invalid_case: a case admits at most 16 fault directives".into());
                }
                if !rest.is_empty() || pending.is_some() {
                    return Err("invalid_case: fault takes no header arguments and must precede a complete step".into());
                }
                let body = section
                    .body
                    .iter()
                    .map(|(_, line)| *line)
                    .collect::<Vec<_>>()
                    .join("\n");
                faults.insert(ordinal + 1, parse_fault(&body)?);
                awaiting_fault_step = true;
            }
            "runner" | "schema" | "seed" => {
                return Err(format!(
                    "line {}: `--- {kind}` is out of position; schema then seed lead the file, once each",
                    section.header_line + 1
                ));
            }
            "query" | "mutate" => {
                let branch = parse_step_branch(kind, rest)?;
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
                let decls = match file {
                    QueryFile::Queries(decls) => decls,
                    QueryFile::Branch(stmt) => {
                        if kind == "query" && stmt.is_write() {
                            return Err(
                                "a control write under `--- query` is refused; use `--- mutate`"
                                    .into(),
                            );
                        }
                        if kind == "mutate" && !stmt.is_write() {
                            return Err(
                                "`branch list` under `--- mutate` is refused; use `--- query`"
                                    .into(),
                            );
                        }
                        if branch.is_some() {
                            return Err(
                                "a branch statement names its branches itself; drop the `branch:` argument"
                                    .into(),
                            );
                        }
                        ordinal += 1;
                        qm_steps += 1;
                        pending = Some(match stmt {
                            BranchStmt::List => Pending::List { ordinal },
                            BranchStmt::Write(write) => Pending::Control { ordinal, write },
                        });
                        continue;
                    }
                };
                let [decl] = decls.as_slice() else {
                    return Err(format!(
                        "a `--- {kind}` section must hold exactly one declaration, got {}",
                        decls.len()
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
                pending = Some(Pending::Decl(PendingStep {
                    is_mutation,
                    ordinal,
                    source: source.clone(),
                    name: decl.name.clone(),
                    branch: branch.unwrap_or_else(|| MAIN_BRANCH.to_string()),
                    decl: Box::new(decl.clone()),
                    ordered_refusal: ordered_refusal(decl),
                    expects_expand: expects_expand(&decl.match_clause),
                    params_raw: None,
                }));
            }
            "params" => {
                if !rest.is_empty() {
                    return Err(format!("unknown section `--- {}`", section.name));
                }
                let step = match pending.as_mut() {
                    Some(Pending::Decl(step)) => step,
                    Some(Pending::List { .. } | Pending::Control { .. }) => {
                        return Err("a branch statement takes no params".into());
                    }
                    None => {
                        return Err(format!(
                            "line {}: `--- params` must directly follow a query or mutate section",
                            section.header_line + 1
                        ));
                    }
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
                if rest.trim() == "shape" {
                    let Some(mut step) = awaiting_shape.take() else {
                        return Err(format!(
                            "line {}: `--- expect shape` must directly follow a query step's unordered or ordered expect",
                            section.header_line + 1
                        ));
                    };
                    let lines = parse_shape_body(&section.body)?;
                    let Some(QueryExpect::Rows { shape, .. }) = step.read_expect_mut() else {
                        return Err(format!(
                            "line {}: internal: the step awaiting a shape section carries no rows expect",
                            section.header_line + 1
                        ));
                    };
                    *shape = ShapeExpect {
                        lines,
                        span: BodySpan {
                            start_line: section.header_line + 1,
                            len: section.body.len(),
                        },
                    };
                    push_step(&mut items, &mut open_loop, step);
                    continue;
                }
                let mode = parse_expect_header(rest)?;
                let loop_var = open_loop.as_ref().map(|(v, _, _)| v.as_str());
                let completed = match pending.take() {
                    Some(Pending::Decl(step)) => {
                        complete_decl_step(step, &mode, section, loop_var)?
                    }
                    Some(Pending::List { ordinal }) => {
                        Step::List(complete_list_step(ordinal, &mode, section, loop_var)?)
                    }
                    Some(Pending::Control { ordinal, write }) => {
                        Step::Control(complete_control_step(ordinal, write, &mode, section)?)
                    }
                    None => {
                        return Err(format!(
                            "line {}: `--- expect` has no query or mutate step to bind to",
                            section.header_line + 1
                        ));
                    }
                };
                if matches!(completed.read_expect(), Some(QueryExpect::Rows { .. })) {
                    substitutable_lines.extend(section.body.iter().map(|(i, _)| *i));
                    awaiting_shape = Some(completed);
                } else {
                    push_step(&mut items, &mut open_loop, completed);
                }
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
    if awaiting_fault_step {
        return Err("invalid_case: fault has no following operation".into());
    }
    if pending.is_some() {
        return Err("the final step is missing its `--- expect`".into());
    }
    if let Some(waiting) = &awaiting_shape {
        return Err(missing_shape(waiting));
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
    let case = Case {
        input_text: text.into(),
        runner,
        known_failure,
        faults,
        source_lines,
        schema,
        seed,
        traversal: header.traversal,
        items,
        needs_indices,
    };
    dst_runner::validate_known_failure(&case)?;
    Ok(case)
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
    bless_lines: Option<(BodySpan, Vec<String>)>,
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

/// Why the executed result disagrees with the schema the compiler inferred
/// for `decl`, if it does: the column count, then per position the executed
/// name, the Arrow type, and nulls in a column the compiler inferred non-null.
fn schema_drift(decl: &QueryDecl, inferred: &Schema, result: &QueryResult) -> Option<String> {
    let executed = result.schema();
    if executed.fields().len() != inferred.fields().len() {
        return Some(format!(
            "result schema mismatch: the compiler inferred {} column(s), the executor returned {}",
            inferred.fields().len(),
            executed.fields().len()
        ));
    }
    for (i, (want, got)) in inferred.fields().iter().zip(executed.fields()).enumerate() {
        let proj = &decl.return_clause[i];
        let name = executed_column_name(&proj.expr, proj.alias.as_deref());
        if got.name() != &name {
            return Some(format!(
                "result schema mismatch at column {i}: expected name `{name}`, the executor returned `{}`",
                got.name()
            ));
        }
        if got.data_type() != want.data_type() {
            let hint = if matches!(want.data_type(), DataType::Struct(_)) {
                "; a bare node projection executes as the id column today and has no green shape until the engine returns the node object"
            } else {
                "; the compiler and the executor disagree: an engine defect to file, not a case error"
            };
            return Some(format!(
                "result schema mismatch at column {i} `{name}`: the compiler inferred {:?}, the executor returned {:?}{hint}",
                want.data_type(),
                got.data_type()
            ));
        }
        if !want.is_nullable() {
            let nulls = shape::null_cells(result, i);
            if nulls > 0 {
                return Some(format!(
                    "result schema mismatch at column {i} `{name}`: the compiler inferred it non-nullable, the executor returned {nulls} null(s)"
                ));
            }
        }
    }
    None
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
        bless_lines: None,
    };
    // A params refusal is one of the ways "the query must fail": route it
    // into an `error:` expectation instead of always failing the step.
    let params = match build_params(step.params_raw.as_ref(), &step.decl.params, binding) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                QueryExpect::Error { needle } if e.contains(needle) => {
                    dst_runner::record("parameter_error", serde_json::json!({"message": e}));
                    Ok(())
                }
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
            ReadTarget::branch(&step.branch),
            &step.source,
            &step.name,
            &params,
        ),
    )
    .await;
    dst_runner::observe_query(
        &outcome,
        matches!(step.expect, QueryExpect::Rows { ordered: true, .. }),
    );
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
            shape,
        } => {
            let result = outcome.map_err(|e| fail(format!("query failed: {e}")))?;
            let catalog = db.catalog();
            let inferred = typecheck_query(&catalog, &step.decl)
                .and_then(|ctx| infer_query_result_schema(&catalog, &step.decl, &ctx))
                .map_err(|e| fail(format!("result schema inference failed: {e}")))?;
            let drift = schema_drift(&step.decl, &inferred, &result);
            if let Some(mismatch) = shape_mismatch(&shape.lines, &result, &inferred, &catalog) {
                let (message, bless_lines) = match (&drift, bless_shape_lines(&result, &catalog)) {
                    (Some(drift), _) => (
                        format!(
                            "{mismatch}\nthe executor disagrees with the compiler's schema, so bless does not rewrite the shape: {drift}"
                        ),
                        None,
                    ),
                    (None, Ok(lines)) => (mismatch, Some((shape.span, lines))),
                    (None, Err(unspellable)) => (format!("{mismatch}\n{unspellable}"), None),
                };
                return Err(StepFail {
                    label: label.clone(),
                    message,
                    bless_lines,
                });
            }
            if let Some(drift) = drift {
                return Err(fail(drift));
            }
            check_rows(&label, &result, *ordered, body_raw, *span, binding)
        }
        QueryExpect::Error { needle } => {
            check_error_expect(needle, outcome, "the query succeeded").map_err(fail)
        }
    }
}

/// Compares the executed rows with the expect body; a mismatch carries the
/// actual rows as the bless target.
fn check_rows(
    label: &str,
    result: &QueryResult,
    ordered: bool,
    body_raw: &str,
    span: BodySpan,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let fail = |message: String| StepFail {
        label: label.to_string(),
        message,
        bless_lines: None,
    };
    let rows = result
        .to_rust_json()
        .map_err(|e| fail(format!("query rows failed to render as JSON: {e}")))?;
    let Value::Array(actual) = rows else {
        return Err(fail("engine returned a non-array row set".into()));
    };
    dst_runner::observe(|| {
        let mut rows = actual.iter().map(Value::to_string).collect::<Vec<_>>();
        if !ordered {
            rows.sort();
        }
        format!("{label} rows: {rows:?}")
    });
    let expected = parse_expect_rows(&substitute(body_raw, binding)).map_err(&fail)?;
    compare_rows(&expected, &actual, ordered).map_err(|(message, rows)| StepFail {
        label: label.to_string(),
        message,
        bless_lines: Some((span, rows)),
    })
}

/// Holds an `error: <needle>` expect against the step's outcome: the step
/// must fail, and the rendered error must contain the needle.
fn check_error_expect<T, E: std::fmt::Display>(
    needle: &str,
    outcome: Result<T, E>,
    succeeded: &str,
) -> Result<(), String> {
    match outcome {
        Ok(_) => Err(format!(
            "expected an error containing \"{needle}\", but {succeeded}"
        )),
        Err(e) => {
            let msg = e.to_string();
            dst_runner::observe(|| format!("error: {msg}"));
            if msg.contains(needle) {
                Ok(())
            } else {
                Err(format!("error does not contain \"{needle}\": {msg}"))
            }
        }
    }
}

/// `branch list`'s answer as a result: one non-null `Utf8` column `name`,
/// rows in byte order, the shape a `--- expect shape` holds against.
fn list_result(mut names: Vec<String>) -> Result<QueryResult, String> {
    names.sort();
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(names))],
    )
    .map_err(|e| format!("`branch list` rows failed to build: {e}"))?;
    Ok(QueryResult::new(schema, vec![batch]))
}

async fn run_list_step(
    db: &Omnigraph,
    step: &ListStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "branch list", binding);
    let fail = |message: String| StepFail {
        label: label.clone(),
        message,
        bless_lines: None,
    };
    let outcome = match db.branch_list().await {
        Ok(names) => Ok(list_result(names).map_err(&fail)?),
        Err(error) => Err(error),
    };
    dst_runner::observe_query(
        &outcome,
        matches!(step.expect, QueryExpect::Rows { ordered: true, .. }),
    );
    match &step.expect {
        QueryExpect::Rows {
            ordered,
            body_raw,
            span,
            shape,
        } => {
            let result = outcome.map_err(|e| fail(format!("`branch list` failed: {e}")))?;
            let catalog = db.catalog();
            if let Some(mismatch) = shape_mismatch(&shape.lines, &result, result.schema(), &catalog)
            {
                let (message, bless_lines) = match bless_shape_lines(&result, &catalog) {
                    Ok(lines) => (mismatch, Some((shape.span, lines))),
                    Err(unspellable) => (format!("{mismatch}\n{unspellable}"), None),
                };
                return Err(StepFail {
                    label: label.clone(),
                    message,
                    bless_lines,
                });
            }
            check_rows(&label, &result, *ordered, body_raw, *span, binding)
        }
        QueryExpect::Error { needle } => {
            check_error_expect(needle, outcome, "`branch list` succeeded").map_err(fail)
        }
    }
}

fn check_write_expect<T, E: std::fmt::Display>(
    name: &str,
    expect: &WriteExpect,
    outcome: Result<T, E>,
) -> Result<(), String> {
    match expect {
        WriteExpect::Ok => outcome
            .map(|_| ())
            .map_err(|e| format!("`{name}` failed: {e}")),
        WriteExpect::Error { needle } => {
            check_error_expect(needle, outcome, &format!("`{name}` succeeded"))
        }
    }
}

/// Runs a control write against the handle. A `branch delete` returns at
/// the manifest flip and reclaims the branch's forks in a background task,
/// so the step joins those before the next step runs.
async fn run_control_step(
    db: &Omnigraph,
    step: &ControlStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let name = step.name;
    let label = step_label(step.ordinal, name, binding);
    let fail = |message: String| StepFail {
        label: label.clone(),
        message,
        bless_lines: None,
    };
    match &step.write {
        ControlWrite::Create {
            name: branch,
            from,
            expect,
        } => {
            let parent = from.as_deref().unwrap_or(MAIN_BRANCH);
            let outcome = db
                .branch_create_from_as(ReadTarget::branch(parent), branch, None)
                .await;
            dst_runner::observe(|| format!("actual control: {outcome:?}"));
            check_write_expect(name, expect, outcome).map_err(fail)
        }
        ControlWrite::Delete {
            name: branch,
            expect,
        } => {
            let outcome = db.branch_delete_as(branch, None).await;
            dst_runner::observe(|| format!("actual control: {outcome:?}"));
            check_write_expect(name, expect, outcome).map_err(fail)
        }
        ControlWrite::Merge {
            source,
            into,
            expect,
        } => {
            let target = into.as_deref().unwrap_or(MAIN_BRANCH);
            let outcome = db
                .branch_merge_as(source, target, None)
                .await
                .inspect_err(dst_runner::observe_fault);
            dst_runner::observe(|| format!("actual merge: {outcome:?}"));
            match expect {
                MergeExpect::Write(expect) => {
                    dst_runner::observe(|| format!("actual control: {outcome:?}"));
                    check_write_expect(name, expect, outcome).map_err(fail)
                }
                MergeExpect::Outcome(want) => match outcome {
                    Ok(got) if got == *want => Ok(()),
                    Ok(got) => Err(fail(format!(
                        "merge outcome mismatch: expected `{}`, got `{}`",
                        merge_outcome_word(*want),
                        merge_outcome_word(got)
                    ))),
                    Err(e) => Err(fail(format!("`{name}` failed: {e}"))),
                },
            }
        }
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
        bless_lines: None,
    };
    let params = match build_params(step.params_raw.as_ref(), &step.ast_params, binding) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                MutateExpect::Error { needle } if e.contains(needle) => {
                    dst_runner::record("parameter_error", serde_json::json!({"message": e}));
                    Ok(())
                }
                MutateExpect::Error { needle } => {
                    Err(fail(format!("error does not contain \"{needle}\": {e}")))
                }
                MutateExpect::Ok | MutateExpect::Affected { .. } => Err(fail(e)),
            };
        }
    };
    let (outcome, counts) = under_traversal(
        mode,
        db.mutate(&step.branch, &step.source, &step.name, &params),
    )
    .await;
    let outcome = outcome.inspect_err(dst_runner::observe_fault);
    dst_runner::record(
        "mutation_result",
        match &outcome {
            Ok(result) => {
                serde_json::json!({"nodes": result.affected_nodes, "edges": result.affected_edges})
            }
            Err(error) => serde_json::json!({"error": error.to_string()}),
        },
    );
    dst_runner::observe(|| match &outcome {
        Ok(result) => format!(
            "actual affected: nodes={} edges={}",
            result.affected_nodes, result.affected_edges
        ),
        Err(error) => format!("actual mutation error: {error}"),
    });
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
        MutateExpect::Error { needle } => {
            check_error_expect(needle, outcome, "the mutation succeeded").map_err(fail)
        }
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
    seed_case(&db, case).await?;
    Ok((db, uri, dir))
}

async fn seed_case(db: &Omnigraph, case: &Case) -> Result<(), String> {
    if !case.seed.trim().is_empty() {
        load_jsonl(db, &case.seed, LoadMode::Overwrite)
            .await
            .map_err(|e| format!("seed load failed: {e}"))?;
    }
    if case.needs_indices {
        db.ensure_indices()
            .await
            .map_err(|e| format!("ensure_indices failed: {e}"))?;
    }
    Ok(())
}

// Keep the complete case state machine off its callers' stack. In particular,
// direct lib tests can execute multiple cases on the default Tokio test stack.
// This synchronous wrapper finishes constructing the boxed future before polling.
fn execute_case<'a>(
    case: &'a Case,
    path: &'a Path,
    bless: bool,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    execute_case_inner(case, path, bless).boxed()
}

async fn execute_case_inner(case: &Case, path: &Path, bless: bool) -> Result<(), String> {
    let (db, uri, _dir) = open_case_store(case).await?;
    execute_steps(case, path, bless, db, &uri, None).await
}

#[cfg(tokio_unstable)]
async fn execute_case_with_storage(
    case: &Case,
    path: &Path,
    uri: &str,
    storage: Arc<dyn StorageAdapter>,
) -> Result<(), String> {
    let db = Omnigraph::init_with_storage(uri, &case.schema, storage.clone(), Default::default())
        .await
        .map_err(|e| format!("init failed: {e}"))?;
    seed_case(&db, case).await?;
    execute_steps(case, path, false, db, uri, Some(storage)).await
}

fn execute_steps<'a>(
    case: &'a Case,
    path: &'a Path,
    bless: bool,
    db: Omnigraph,
    uri: &'a str,
    storage: Option<Arc<dyn StorageAdapter>>,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    execute_steps_inner(case, path, bless, db, uri, storage).boxed()
}

async fn execute_steps_inner(
    case: &Case,
    path: &Path,
    bless: bool,
    mut db: Omnigraph,
    uri: &str,
    storage: Option<Arc<dyn StorageAdapter>>,
) -> Result<(), String> {
    let mut first_fail: Option<StepFail> = None;
    let mut generation = 0usize;
    dst_runner::observe(|| "lifetime: initialized generation 0".into());
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
                let ordinal = step.ordinal();
                dst_runner::observe(|| {
                    format!(
                        "operation: ordinal={ordinal} line={:?} binding={binding:?} generation={generation} expected={step:?}",
                        case.source_lines.get(&ordinal)
                    )
                });
                dst_runner::begin_operation(
                    serde_json::json!({"ordinal": ordinal, "source_line": case.source_lines.get(&ordinal), "loop_binding": binding, "generation": generation}),
                );
                dst_runner::record(
                    "expectation",
                    match step {
                        Step::Query(q) => read_expect_evidence(&q.expect),
                        Step::List(l) => read_expect_evidence(&l.expect),
                        Step::Mutate(m) => match &m.expect {
                            MutateExpect::Ok => serde_json::json!({"kind": "ok"}),
                            MutateExpect::Affected { nodes, edges } => {
                                serde_json::json!({"kind": "affected", "nodes": nodes, "edges": edges})
                            }
                            MutateExpect::Error { needle } => {
                                serde_json::json!({"kind": "error", "contains": needle})
                            }
                        },
                        Step::Control(c) => {
                            serde_json::json!({"control": c.name, "expectation": format!("{:?}", c.write)})
                        }
                        Step::Restart { .. } => {
                            serde_json::json!({"kind": "restart", "storage": "preserved"})
                        }
                    },
                );
                let fault = case.faults.get(&ordinal);
                let guard = dst_runner::arm_fault(fault)?;
                let lifetime_before = dst_runner::lifetime_counts();
                let outcome = match step {
                    Step::Query(q) => run_query_step(&db, case.traversal, q, binding).await,
                    Step::Mutate(m) => run_mutate_step(&db, case.traversal, m, binding).await,
                    Step::Control(c) => run_control_step(&db, c, binding).await,
                    Step::List(l) => run_list_step(&db, l, binding).await,
                    Step::Restart { ordinal } => {
                        drop(db);
                        generation += 1;
                        dst_runner::observe(|| format!("lifetime: reopen generation {generation}"));
                        let reopened = match &storage {
                            Some(storage) => {
                                Omnigraph::open_with_storage(uri, storage.clone()).await
                            }
                            None => Omnigraph::open(uri).await,
                        };
                        db = reopened.map_err(|e| {
                            format!(
                                "{}: reopen failed: {e}",
                                step_label(*ordinal, "restart", binding)
                            )
                        })?;
                        Ok(())
                    }
                };
                #[cfg(tokio_unstable)]
                drop(guard);
                #[cfg(not(tokio_unstable))]
                let _ = guard;
                let lifetime_after = dst_runner::lifetime_counts();
                dst_runner::record(
                    "engine_lifetime",
                    serde_json::json!({"before": lifetime_before, "after": lifetime_after}),
                );
                if let (Some(before), Some(after)) = (lifetime_before, lifetime_after) {
                    let opens = u64::from(matches!(step, Step::Restart { .. }));
                    if after != [before[0], before[1] + opens] {
                        return Err(format!(
                            "worker_failed: unexpected engine init/open call during step {ordinal}: {before:?} -> {after:?}"
                        ));
                    }
                }
                let fault_result = dst_runner::finish_fault(fault);
                dst_runner::record(
                    "assertion",
                    match &outcome {
                        Ok(()) => serde_json::json!({"status": "passed"}),
                        Err(error) => {
                            serde_json::json!({"status": "failed", "code": "assertion_failed", "message": error.message})
                        }
                    },
                );
                dst_runner::observe(|| {
                    format!(
                        "operation result: {:?}",
                        outcome.as_ref().map_err(|f| (&f.label, &f.message))
                    )
                });
                if let Err(error) = fault_result {
                    return Err(format!(
                        "{error}; operation result: {:?}",
                        outcome.as_ref().map_err(|f| (&f.label, &f.message))
                    ));
                }
                if storage.is_some() {
                    let snapshot = db
                        .resolve_snapshot("main")
                        .await
                        .map_err(|e| format!("observe main snapshot: {e}"))?;
                    dst_runner::observe(|| format!("main snapshot: {snapshot}"));
                }
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
        if let Some((span, lines)) = &fail.bless_lines {
            if case.has_loops() {
                detail.push_str("\nbless: refused, the case contains loops");
            } else {
                bless_rewrite(path, *span, lines, &case.input_text)?;
                let _ = write!(
                    detail,
                    "\nbless: expect rewritten in place ({} lines), re-run to confirm",
                    lines.len()
                );
            }
        }
    }
    Err(detail)
}

fn read_expect_evidence(expect: &QueryExpect) -> Value {
    match expect {
        QueryExpect::Rows {
            ordered,
            body_raw,
            shape,
            ..
        } => {
            serde_json::json!({"kind": "rows", "ordered": ordered, "rows_jsonl": body_raw, "shape": format!("{:?}", shape.lines)})
        }
        QueryExpect::Error { needle } => serde_json::json!({"kind": "error", "contains": needle}),
    }
}

fn splice_lines(original: &str, span: BodySpan, lines: &[String]) -> String {
    let original_lines: Vec<&str> = original.lines().collect();
    let body = &original_lines[span.start_line..span.start_line + span.len];
    let trailing_blanks = body
        .iter()
        .rev()
        .take_while(|l| l.trim().is_empty())
        .count();
    let mut out: Vec<&str> = original_lines[..span.start_line].to_vec();
    out.extend(lines.iter().map(String::as_str));
    out.extend(std::iter::repeat_n("", trailing_blanks));
    out.extend(&original_lines[span.start_line + span.len..]);
    let mut joined = out.join("\n");
    joined.push('\n');
    joined
}

fn bless_rewrite(
    path: &Path,
    span: BodySpan,
    lines: &[String],
    expected: &str,
) -> Result<(), String> {
    let original =
        std::fs::read_to_string(path).map_err(|e| format!("bless: cannot re-read case: {e}"))?;
    if original != expected {
        return Err("environment_changed: case changed before bless".into());
    }
    std::fs::write(path, splice_lines(&original, span, lines))
        .map_err(|e| format!("bless: cannot write case: {e}"))
}

/// Parses and executes one case file; `bless` rewrites a failing step's
/// `--- expect` rows or shape lines in place and still reports the step
/// (`expect rewritten`), so the run stays red until the re-run confirms.
pub async fn run_case(path: PathBuf, bless: bool) -> Result<(), String> {
    let stem = stem_of(&path);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("cannot read case file: {e}"))?;
    let case = parse_case(&stem, &text).map_err(|e| format!("refused: {e}"))?;
    if case.runner.environments.len() != 1
        || !matches!(
            case.runner.environments[0].execution,
            Execution::Engine { .. }
        )
    {
        return Err("DST cases require the file dispatcher; the async normal runner cannot execute mode: dst".into());
    }
    case.runner.environments[0].admit(!case.faults.is_empty())?;
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
