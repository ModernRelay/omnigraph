//! GQT parsing, expectation checking, and execution over a supplied session.
#![recursion_limit = "512"]

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::FutureExt as _;
use omnigraph::Session;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, capture_query_io_probes, with_query_io_probes};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::query::ast::{
    BranchStmt, BranchWrite, Clause, EmptyFile, Expr, FileBody, Literal, Param, QueryDecl,
    SettingStmt, show_statement_name,
};
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::query::typecheck::{
    executed_column_name, infer_query_result_schema, typecheck_query,
};
use omnigraph_compiler::schema::ast::{Annotation, PropDecl, SchemaDecl};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::settings::{Engine, SessionSettings, SettingId, SettingRow, Traversal};
use omnigraph_compiler::{
    JsonParamMode, PropType, QueryResult, ScalarType, json_params_to_param_map,
};
use serde_json::Value;

pub mod concurrent;
mod generate;
mod host;
pub mod runner_config;
mod yaml;
pub use concurrent::{ConcurrentStep, SessionExpect, SessionKind, SessionOp};
pub use generate::{Generated, Seed};
pub use host::{ExecutionHost, PlainHost};
use omnigraph::storage::StorageAdapter;
pub use runner_config::{Execution, RunnerConfig, SeamDirective, parse_runner, parse_seam};

mod plan;
mod report;
mod shape;
use plan::{PlanExpect, parse_plan_body, plan_mismatch, validate_plan_columns};

use shape::{
    ShapeExpect, ShapeLine, ShapeType, bless_shape_lines, parse_shape_body, shape_mismatch,
    spell_shape_line,
};

#[derive(Debug)]
pub struct Case {
    input_text: String,
    pub runner: RunnerConfig,
    pub seams: BTreeMap<usize, Vec<SeamDirective>>,
    pub source_lines: BTreeMap<usize, usize>,
    pub fixture: Option<Fixture>,
    /// The `# traversal:` pin: the harness-only traversal field on the case
    /// session plus the expand-path check; `None` leaves it at `auto`.
    traversal: Option<&'static str>,
    pub items: Vec<Item>,
    pub needs_indices: bool,
}

#[derive(Debug)]
pub struct Fixture {
    pub schema: String,
    pub seed: Seed,
}

impl Case {
    pub fn admit_store(&self, store: Option<&str>) -> Result<(), String> {
        match (&self.fixture, store) {
            (None, None) => {
                Err("invalid_case: a case without schema and seed requires --store <URI>".into())
            }
            (Some(_), Some(_)) => {
                Err("invalid_case: --store cannot be combined with schema and seed".into())
            }
            (Some(_), None) | (None, Some(_)) => Ok(()),
        }
    }

    fn has_loops(&self) -> bool {
        self.items.iter().any(|i| matches!(i, Item::Loop { .. }))
    }

    /// Whether the case needs the DST runner: a seam directive or a
    /// concurrent block, neither of which the direct engine can host.
    pub fn needs_dst(&self) -> bool {
        !self.seams.is_empty()
            || self
                .items
                .iter()
                .any(|i| matches!(i, Item::Step(Step::Concurrent(_))))
    }
}

/// The step's kind as the measure report names it.
pub fn step_kind(step: &Step) -> &'static str {
    match step {
        Step::Query(_) => "query",
        Step::Mutate(_) => "mutate",
        Step::Load(_) => "load",
        Step::Control(_) => "control",
        Step::List(_) => "list",
        Step::Settings(_) => "settings",
        Step::Show(_) => "show",
        Step::Restart { .. } => "restart",
        Step::Concurrent(_) => "concurrent",
    }
}

#[derive(Debug)]
pub enum Item {
    Step(Step),
    Loop {
        var: String,
        values: Vec<String>,
        steps: Vec<Step>,
    },
}

#[derive(Debug)]
pub enum Step {
    Query(QueryStep),
    Mutate(MutateStep),
    Load(LoadStep),
    Control(ControlStep),
    List(ListStep),
    Settings(SettingsStep),
    Show(ShowStep),
    Restart { ordinal: usize },
    Concurrent(ConcurrentStep),
}

impl Step {
    pub fn ordinal(&self) -> usize {
        match self {
            Self::Query(s) => s.ordinal,
            Self::Mutate(s) => s.ordinal,
            Self::Load(s) => s.ordinal,
            Self::Control(s) => s.ordinal,
            Self::List(s) => s.ordinal,
            Self::Settings(s) => s.ordinal,
            Self::Show(s) => s.ordinal,
            Self::Restart { ordinal } => *ordinal,
            Self::Concurrent(s) => s.ordinal,
        }
    }

    /// The rows-or-error expect of a read step (`--- query`), which is the
    /// one kind that carries a shape section.
    fn read_expect(&self) -> Option<&QueryExpect> {
        match self {
            Step::Query(step) => Some(&step.expect),
            Step::List(step) => Some(&step.expect),
            Step::Show(step) => Some(&step.expect),
            Step::Mutate(_)
            | Step::Load(_)
            | Step::Control(_)
            | Step::Settings(_)
            | Step::Restart { .. }
            | Step::Concurrent(_) => None,
        }
    }

    fn read_expect_mut(&mut self) -> Option<&mut QueryExpect> {
        match self {
            Step::Query(step) => Some(&mut step.expect),
            Step::List(step) => Some(&mut step.expect),
            Step::Show(step) => Some(&mut step.expect),
            Step::Mutate(_)
            | Step::Load(_)
            | Step::Control(_)
            | Step::Settings(_)
            | Step::Restart { .. }
            | Step::Concurrent(_) => None,
        }
    }
}

#[derive(Debug)]
pub struct LoadStep {
    ordinal: usize,
    pub branch: String,
    mode: LoadMode,
    generated: Generated,
    expect: WriteExpect,
}

impl LoadStep {
    pub fn call_count(&self) -> u64 {
        self.generated.call_count()
    }
}

#[derive(Debug)]
pub struct QueryStep {
    ordinal: usize,
    pub source: String,
    pub name: String,
    /// The `branch: <name>` header argument; `main` when unspelled.
    pub branch: String,
    decl: Box<QueryDecl>,
    params_raw: Option<String>,
    expect: QueryExpect,
    /// The match clause carries an unbound traversal, so a successful run
    /// must show at least one Expand on the pinned path.
    expects_expand: bool,
    /// The `--- expect plan` section, checked against the engine's explain
    /// document before the rows.
    plan: Option<PlanExpect>,
    /// `--- expect same as v1`: the rows must also equal the reference
    /// engine's rows for the same query, compared as `expect` compares.
    same_as_v1: bool,
}

#[derive(Debug)]
pub struct MutateStep {
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
pub struct ControlStep {
    ordinal: usize,
    /// The statement's two words, from `BranchWrite::statement_name`.
    name: &'static str,
    /// The section's `set` and `reset` lines, scoped to this one write.
    prefix: Vec<SettingStmt>,
    pub write: ControlWrite,
}

#[derive(Debug)]
pub enum ControlWrite {
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
pub enum WriteExpect {
    Ok,
    Error { needle: String },
}

/// A `branch merge`'s expect: what any control write takes, plus the
/// `outcome:` word only a merge has an answer for.
#[derive(Debug)]
pub enum MergeExpect {
    Write(WriteExpect),
    Outcome(MergeOutcome),
}

/// A `--- query` step holding `branch list`: one `name` column, rows in
/// byte order, so its expect is a read expect like a declaration's.
#[derive(Debug)]
pub struct ListStep {
    ordinal: usize,
    expect: QueryExpect,
}

/// A `--- mutate` step of only `set` and `reset` lines: applied to the case
/// session for the steps that follow, across a `--- restart`.
#[derive(Debug)]
pub struct SettingsStep {
    ordinal: usize,
    statements: Vec<SettingStmt>,
}

/// A `--- query` step holding `show`: the five `String` columns of
/// `SettingRow::COLUMNS`, rows in definition order, so its expect is a read
/// expect like a declaration's. The prefix is scoped to this one step.
#[derive(Debug)]
pub struct ShowStep {
    ordinal: usize,
    id: Option<SettingId>,
    prefix: Vec<SettingStmt>,
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

/// Canonical spelling for one header key and its trimmed value.
fn canonical_header_line(key: &str, value: &str) -> String {
    format!("# {key}: {value}")
}

/// Splits a canonical header line, rejecting unknown keys and edge whitespace.
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

fn yaml_body(body: &[(usize, &str)]) -> String {
    let mut text = body
        .iter()
        .map(|(_, line)| *line)
        .collect::<Vec<_>>()
        .join("\n");
    text.push('\n');
    text
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
fn walk_expr(
    expr: &Expr,
    params: &[Param],
    needs_indices: &mut bool,
    reject_edge_type: bool,
) -> Result<(), String> {
    match expr {
        Expr::PropAccess { property, .. }
            if reject_edge_type && property == omnigraph_compiler::traversal::EDGE_TYPE_META =>
        {
            return Err(
                "expect same as v1: the reference engine does not support edge @type access".into(),
            );
        }
        Expr::Now
        | Expr::PropAccess { .. }
        | Expr::Variable(_)
        | Expr::Literal(_)
        | Expr::AliasRef(_) => {}
        Expr::Aggregate { func: _, arg } => {
            walk_expr(arg, params, needs_indices, reject_edge_type)?
        }
        Expr::Binary { left, op: _, right }
        | Expr::In {
            needle: left,
            list: right,
        } => {
            walk_expr(left, params, needs_indices, reject_edge_type)?;
            walk_expr(right, params, needs_indices, reject_edge_type)?;
        }
        Expr::Not(inner)
        | Expr::IsNull {
            expr: inner,
            negated: _,
        } => walk_expr(inner, params, needs_indices, reject_edge_type)?,
        Expr::Search { field, query }
        | Expr::MatchText { field, query }
        | Expr::Bm25 { field, query } => {
            *needs_indices = true;
            walk_expr(field, params, needs_indices, reject_edge_type)?;
            walk_expr(query, params, needs_indices, reject_edge_type)?;
        }
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            *needs_indices = true;
            walk_expr(field, params, needs_indices, reject_edge_type)?;
            walk_expr(query, params, needs_indices, reject_edge_type)?;
            if let Some(max_edits) = max_edits {
                walk_expr(max_edits, params, needs_indices, reject_edge_type)?;
            }
        }
        Expr::Nearest {
            variable: _,
            property: _,
            query,
        } => {
            *needs_indices = true;
            refuse_string_nearest(query, params)?;
            walk_expr(query, params, needs_indices, reject_edge_type)?;
        }
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => {
            *needs_indices = true;
            walk_expr(primary, params, needs_indices, reject_edge_type)?;
            walk_expr(secondary, params, needs_indices, reject_edge_type)?;
            if let Some(k) = k {
                walk_expr(k, params, needs_indices, reject_edge_type)?;
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
    reject_edge_type: bool,
) -> Result<(), String> {
    for clause in clauses {
        match clause {
            Clause::Binding(binding) => {
                for property in &binding.prop_matches {
                    walk_expr(&property.value, params, needs_indices, reject_edge_type)?;
                }
            }
            Clause::Traversal(_) => {}
            Clause::Filter(f) => walk_expr(f, params, needs_indices, reject_edge_type)?,
            Clause::Subquery(subquery) => {
                walk_clauses(&subquery.clauses, params, needs_indices, reject_edge_type)?;
                if let Some(arg) = &subquery.arg {
                    walk_expr(arg, params, needs_indices, reject_edge_type)?;
                }
                walk_expr(&subquery.right, params, needs_indices, reject_edge_type)?;
            }
        }
    }
    Ok(())
}

/// Why `expect ordered` is refused for this declaration, if it is: the order
/// is total only where a sort appends `<var>.id` tie-breaks (RFC 0045), so no
/// `order`, an `rrf()`-led one, or an aggregate `return` each refuse it.
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
             ranked identity and downstream metadata; the harness does not promise a total order for every fusion shape"
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
    inspect_decl_with_policy(decl, needs_indices, false)
}

fn inspect_decl_with_policy(
    decl: &QueryDecl,
    needs_indices: &mut bool,
    reject_edge_type: bool,
) -> Result<(), String> {
    walk_clauses(
        &decl.match_clause,
        &decl.params,
        needs_indices,
        reject_edge_type,
    )?;
    for projection in &decl.return_clause {
        walk_expr(
            &projection.expr,
            &decl.params,
            needs_indices,
            reject_edge_type,
        )?;
    }
    for ordering in &decl.order_clause {
        walk_expr(
            &ordering.expr,
            &decl.params,
            needs_indices,
            reject_edge_type,
        )?;
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
    Load {
        ordinal: usize,
        branch: String,
        mode: LoadMode,
        generated: Generated,
    },
    List {
        ordinal: usize,
    },
    Control {
        ordinal: usize,
        prefix: Vec<SettingStmt>,
        write: BranchWrite,
    },
    Settings {
        ordinal: usize,
        statements: Vec<SettingStmt>,
    },
    Show {
        ordinal: usize,
        id: Option<SettingId>,
        prefix: Vec<SettingStmt>,
    },
    /// A `--- concurrent` block awaiting the bare `--- expect` that names
    /// each session's outcome.
    Concurrent(ConcurrentStep),
}

/// Whether `text` holds the statement `<statement> <name>` at any position:
/// a `set` or `reset` that follows an earlier statement on the same line is
/// still its own statement, and its line is the one to report.
fn line_holds_statement(text: &str, statement: &str, name: &str) -> bool {
    text.match_indices(statement)
        .any(|(at, _)| text[at + statement.len()..].trim_start().starts_with(name))
}

/// The `process` scope rule at the runner (the Session settings RFC, Logic tests): a case
/// body may `set` or `reset` a `request` setting only; the refusal names
/// the offending statement's line.
fn refuse_process_settings(
    statements: &[SettingStmt],
    body: &[(usize, &str)],
) -> Result<(), String> {
    for stmt in statements {
        let Some(id) = stmt.id() else {
            continue;
        };
        let Err(error) = id.refuse_from_request() else {
            continue;
        };
        let line = body
            .iter()
            .find(|(_, text)| line_holds_statement(text, stmt.statement_name(), id.name()))
            .or(body.first())
            .map_or(0, |(idx, _)| idx + 1);
        return Err(format!("line {line}: {error}"));
    }
    Ok(())
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

/// The shape of `show`'s rows: the five [`SettingRow::COLUMNS`], each a
/// non-null `String`. It is derived, so a `show` step carries no
/// `--- expect shape` section and none is blessed into one.
fn derived_show_shape() -> ShapeExpect {
    ShapeExpect {
        lines: SettingRow::COLUMNS
            .map(|name| ShapeLine {
                name: name.to_string(),
                shape_type: ShapeType::Scalar(PropType::scalar(ScalarType::String, false)),
            })
            .to_vec(),
        span: BodySpan {
            start_line: 0,
            len: 0,
        },
    }
}

/// A `show` step's explicit `--- expect shape`: the only shape `show` rows
/// can have is the derived one, so any other spelling is refused and the
/// refusal names the derivable shape.
fn refuse_unequal_show_shape(lines: &[ShapeLine], line: usize) -> Result<(), String> {
    let want: Vec<String> = derived_show_shape()
        .lines
        .iter()
        .map(spell_shape_line)
        .collect();
    if lines.iter().map(spell_shape_line).eq(want.iter().cloned()) {
        return Ok(());
    }
    Err(format!(
        "line {line}: `show` derives its shape; drop the `--- expect shape` section, or write exactly `{}`",
        want.join("`, `")
    ))
}

/// The step a `show` section becomes under `mode`. The rows are total in
/// definition order and the statement cannot fail, so `error:` is refused
/// with the mutate modes. The shape comes from [`derived_show_shape`].
fn complete_show_step(
    ordinal: usize,
    id: Option<SettingId>,
    prefix: Vec<SettingStmt>,
    mode: &ExpectHeader,
    section: &Section<'_>,
    loop_var: Option<&str>,
) -> Result<ShowStep, String> {
    let expect = match mode {
        ExpectHeader::Unordered | ExpectHeader::Ordered => {
            rows_expect(matches!(mode, ExpectHeader::Ordered), section, loop_var)?
        }
        ExpectHeader::Error(_)
        | ExpectHeader::Ok
        | ExpectHeader::Affected { .. }
        | ExpectHeader::Outcome(_) => {
            return Err(format!(
                "`{}` takes `unordered` or `ordered`; it returns the settings rows and does not fail",
                show_statement_name(id)
            ));
        }
    };
    let mut step = ShowStep {
        ordinal,
        id,
        prefix,
        expect,
    };
    if let QueryExpect::Rows { shape, .. } = &mut step.expect {
        *shape = derived_show_shape();
    }
    Ok(step)
}

/// The step a section of only `set` and `reset` lines becomes under `mode`.
fn complete_settings_step(
    ordinal: usize,
    statements: Vec<SettingStmt>,
    mode: &ExpectHeader,
    section: &Section<'_>,
) -> Result<SettingsStep, String> {
    match mode {
        ExpectHeader::Ok => {
            refuse_nonempty_body(&section.body, "an `expect ok` section")?;
            Ok(SettingsStep {
                ordinal,
                statements,
            })
        }
        ExpectHeader::Unordered
        | ExpectHeader::Ordered
        | ExpectHeader::Error(_)
        | ExpectHeader::Affected { .. }
        | ExpectHeader::Outcome(_) => {
            Err("a settings step takes `ok`; `set` and `reset` return no rows and no counts".into())
        }
    }
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
    prefix: Vec<SettingStmt>,
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
        prefix,
        write,
    })
}

/// The expect word that adds the reference-engine comparison to a query
/// step's rows expect.
const SAME_AS_V1: &str = "same as v1";

/// Why a `--- expect same as v1` section has no step to attach to; a mutate
/// step, pending or just completed, gets its own refusal.
fn same_as_v1_misplaced(
    line: usize,
    pending: Option<&Pending>,
    items: &[Item],
    open_loop: Option<&(String, Vec<String>, Vec<Step>)>,
) -> String {
    let last = match open_loop {
        Some((_, _, steps)) => steps.last(),
        None => match items.last() {
            Some(Item::Step(step)) => Some(step),
            Some(Item::Loop { .. }) | None => None,
        },
    };
    let on_mutate = matches!(pending, Some(Pending::Decl(step)) if step.is_mutation)
        || (pending.is_none() && matches!(last, Some(Step::Mutate(_))));
    if on_mutate {
        return format!(
            "invalid_case: line {line}: `--- expect same as v1` is refused on a mutate step; the reference engine only reads, and a mutation returns no rows to compare"
        );
    }
    format!(
        "line {line}: `--- expect same as v1` must directly follow a query step's `--- expect shape` or `--- expect plan`; it adds the reference-engine comparison to the step's own rows expect"
    )
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
                plan: None,
                same_as_v1: false,
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
                    plan: None,
                    same_as_v1: false,
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

pub fn parse_case(stem: &str, text: &str) -> Result<Case, String> {
    if text.contains('\r') {
        return Err("case files are UTF-8 with `\\n` line endings; `\\r` found".into());
    }
    let lines: Vec<&str> = text.lines().collect();
    let (header_lines, sections) = split_sections(&lines);
    let header = parse_header(&header_lines)?;
    check_file_name(stem, &header.issue)?;

    let (runner, sections) = if sections.first().is_some_and(|s| s.name == "runner") {
        (parse_runner(&yaml_body(&sections[0].body))?, &sections[1..])
    } else {
        return Err(
            "invalid_case: the first section must be `--- runner` with explicit configuration"
                .into(),
        );
    };
    let (fixture, sections) = match sections.first().map(|s| s.name.as_str()) {
        Some("schema") => {
            if !sections
                .get(1)
                .is_some_and(|s| s.name == "seed" || s.name.starts_with("seed "))
            {
                return Err(
                    "schema and seed are optional together; the second section must be `--- seed`"
                        .into(),
                );
            }
            let schema = sections[0]
                .body
                .iter()
                .map(|(_, l)| *l)
                .collect::<Vec<_>>()
                .join("\n");
            if sections[1].name == "seed" {
                refuse_comment_lines(&sections[1].body, "seed")?;
            }
            let seed = Seed::parse(&sections[1].name, yaml_body(&sections[1].body))?;
            refuse_embed_schema(&schema, sections[0].header_line + 1)?;
            (Some(Fixture { schema, seed }), &sections[2..])
        }
        Some(name) if name == "seed" || name.starts_with("seed ") => {
            return Err(
                "schema and seed are optional together; `--- schema` must precede `--- seed`"
                    .into(),
            );
        }
        _ => (None, sections),
    };

    let mut needs_indices = header.traversal.is_some();
    let mut items: Vec<Item> = Vec::new();
    let mut open_loop: Option<(String, Vec<String>, Vec<Step>)> = None;
    let mut pending: Option<Pending> = None;
    let mut awaiting_shape: Option<Step> = None;
    let mut awaiting_plan: Option<Step> = None;
    let mut awaiting_same_as_v1: Option<Step> = None;
    let mut ordinal = 0usize;
    let mut seams = BTreeMap::new();
    let mut source_lines = BTreeMap::new();
    let mut awaiting_seam_step = false;
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

    /// A rows step whose `--- expect shape` did not arrive next: a `show`
    /// step derives its shape and is complete, any other rows step is
    /// refused.
    fn settle_shape(
        items: &mut Vec<Item>,
        open_loop: &mut Option<(String, Vec<String>, Vec<Step>)>,
        awaiting: Option<Step>,
    ) -> Result<(), String> {
        match awaiting {
            Some(step) if matches!(step, Step::Show(_)) => {
                push_step(items, open_loop, step);
                Ok(())
            }
            Some(step) => Err(missing_shape(&step)),
            None => Ok(()),
        }
    }

    for section in sections {
        let (kind, rest) = match section.name.split_once([' ', '\t']) {
            Some((k, rest)) => (k, rest),
            None => (section.name.as_str(), ""),
        };
        let expect_word = if kind == "expect" { rest.trim() } else { "" };
        if !expect_word.starts_with("shape") {
            settle_shape(&mut items, &mut open_loop, awaiting_shape.take())?;
        }
        if let Some(step) = awaiting_plan.take_if(|_| !matches!(expect_word, "plan" | SAME_AS_V1)) {
            push_step(&mut items, &mut open_loop, step);
        }
        if let Some(step) = awaiting_same_as_v1.take_if(|_| expect_word != SAME_AS_V1) {
            push_step(&mut items, &mut open_loop, step);
        }
        if awaiting_seam_step && !matches!(kind, "mutate" | "load" | "seam") {
            return Err(
                "invalid_case: a seam must directly precede its mutate step (a GQ mutation or a branch statement) or load step; no seam is crossed by a query step yet".into(),
            );
        }
        if matches!(kind, "query" | "mutate" | "load" | "restart" | "concurrent") {
            source_lines.insert(ordinal + 1, section.header_line + 1);
            awaiting_seam_step = false;
        }
        match kind {
            "seam" => {
                if open_loop.is_some() {
                    return Err(
                        "invalid_case: seam directives inside loops are not supported".into(),
                    );
                }
                if seams.values().map(Vec::len).sum::<usize>() >= 16 {
                    return Err("invalid_case: a case admits at most 16 seam directives".into());
                }
                if !rest.is_empty() || pending.is_some() {
                    return Err("invalid_case: seam takes no header arguments and must precede a complete step".into());
                }
                let seam = parse_seam(&yaml_body(&section.body))?;
                let step_seams: &mut Vec<SeamDirective> = seams.entry(ordinal + 1).or_default();
                if step_seams.iter().any(|earlier| earlier.at == seam.at) {
                    return Err(format!(
                        "invalid_case: seam {} is declared twice before one step; one directive per seam per step",
                        seam.at
                    ));
                }
                step_seams.push(seam);
                awaiting_seam_step = true;
            }
            "fault" => {
                return Err(format!(
                    "line {}: there is no `--- fault` section; a code seam is a `--- seam` with the same `at`, `occurrence` and `scope` and `action: fail` (was `return_error`) or `action: skip`, and a store fault is a `--- seam` naming a store place with `subject:` or a decision seam with a store action",
                    section.header_line + 1
                ));
            }
            "runner" | "schema" | "seed" => {
                return Err(format!(
                    "line {}: `--- {kind}` is out of position; schema then seed lead the file, once each",
                    section.header_line + 1
                ));
            }
            "load" => {
                if pending.is_some() {
                    return Err("the previous step is missing its `--- expect`".into());
                }
                let (seed, mode, branch) = generate::parse_arguments(rest, true)?;
                let generated = Generated::parse(seed, &yaml_body(&section.body))?;
                if generated.call_count() == 0 {
                    return Err("a generated load requires at least one nonempty batch; empty recipes are admitted only as seeds".into());
                }
                ordinal += 1;
                pending = Some(Pending::Load {
                    ordinal,
                    branch,
                    mode,
                    generated,
                });
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
                refuse_process_settings(&file.settings, &section.body)?;
                let empty = file.empty_kind();
                let decls = match file.body {
                    FileBody::Queries(_) if matches!(empty, Some(EmptyFile::SettingsOnly)) => {
                        if kind == "query" {
                            return Err(
                                "a settings step is a `--- mutate` step; use `--- mutate`".into()
                            );
                        }
                        if branch.is_some() {
                            return Err(
                                "a settings step changes the case session, not a branch; drop the `branch:` argument"
                                    .into(),
                            );
                        }
                        ordinal += 1;
                        pending = Some(Pending::Settings {
                            ordinal,
                            statements: file.settings,
                        });
                        continue;
                    }
                    FileBody::Queries(decls) => decls,
                    FileBody::Show(id) => {
                        if kind == "mutate" {
                            return Err(format!(
                                "`{}` under `--- mutate` is refused; use `--- query`",
                                show_statement_name(id)
                            ));
                        }
                        if branch.is_some() {
                            return Err(
                                "`show` reads the case session, not a branch; drop the `branch:` argument"
                                    .into(),
                            );
                        }
                        ordinal += 1;
                        pending = Some(Pending::Show {
                            ordinal,
                            id,
                            prefix: file.settings,
                        });
                        continue;
                    }
                    FileBody::Branch(stmt) => {
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
                        pending = Some(match stmt {
                            BranchStmt::List => Pending::List { ordinal },
                            BranchStmt::Write(write) => Pending::Control {
                                ordinal,
                                prefix: file.settings,
                                write,
                            },
                        });
                        continue;
                    }
                    FileBody::Explain(_) => {
                        return Err(format!(
                            "an `explain` statement under `--- {kind}` is refused; assert the plan with `--- expect plan`"
                        ));
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
            "concurrent" => {
                let line = section.header_line + 1;
                if !rest.is_empty() {
                    return Err(format!("line {line}: `--- concurrent` takes no arguments"));
                }
                if open_loop.is_some() {
                    return Err(format!(
                        "line {line}: invalid_case: a concurrent block inside a loop is not supported"
                    ));
                }
                if pending.is_some() {
                    return Err(format!(
                        "line {line}: the previous step is missing its `--- expect`"
                    ));
                }
                let (lines, order) = concurrent::parse_block(&section.body).map_err(|e| {
                    if e.starts_with("line ") {
                        e
                    } else {
                        format!("line {line}: {e}")
                    }
                })?;
                let mut sessions = Vec::with_capacity(lines.len());
                for session in lines {
                    let file = parse_query(&session.text).map_err(|e| {
                        format!(
                            "line {}: session `{}` does not parse: {e}",
                            session.line, session.label
                        )
                    })?;
                    if !file.settings.is_empty() {
                        return Err(format!(
                            "line {}: session `{}` carries `set` or `reset` lines; a block session is one statement",
                            session.line, session.label
                        ));
                    }
                    let FileBody::Queries(decls) = file.body else {
                        return Err(format!(
                            "line {}: session `{}` must be a query or mutation declaration; show, branch and explain statements are not admitted in a block",
                            session.line, session.label
                        ));
                    };
                    let [decl] = decls.as_slice() else {
                        return Err(format!(
                            "line {}: session `{}` holds exactly one declaration, got {}",
                            session.line,
                            session.label,
                            decls.len()
                        ));
                    };
                    if !decl.params.is_empty() {
                        return Err(format!(
                            "line {}: session `{}` declares parameters; a block session takes none",
                            session.line, session.label
                        ));
                    }
                    inspect_decl(decl, &mut needs_indices)?;
                    let kind = if decl.mutations.is_empty() {
                        SessionKind::Query
                    } else {
                        SessionKind::Mutation
                    };
                    sessions.push(SessionOp {
                        label: session.label,
                        branch: session.branch,
                        source: session.text,
                        name: decl.name.clone(),
                        kind,
                        expect: SessionExpect::Ok,
                    });
                }
                ordinal += 1;
                pending = Some(Pending::Concurrent(ConcurrentStep {
                    ordinal,
                    sessions,
                    order,
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
                    Some(Pending::Settings { .. } | Pending::Show { .. }) => {
                        return Err("a settings statement takes no params".into());
                    }
                    Some(Pending::Concurrent(_)) => {
                        return Err("a concurrent block takes no params; its sessions are literal statements".into());
                    }
                    Some(Pending::Load { .. }) => {
                        return Err("a generated load takes no params".into());
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
                if let Some(Pending::Concurrent(mut step)) =
                    pending.take_if(|p| matches!(p, Pending::Concurrent(_)))
                {
                    let line = section.header_line + 1;
                    if !rest.trim().is_empty() {
                        return Err(format!(
                            "line {line}: after a concurrent block, `--- expect` is bare and its body names each session: `<label>: ok` or `<label>: error: <needle>`"
                        ));
                    }
                    refuse_comment_lines(&section.body, "expect")?;
                    let labels: Vec<&str> =
                        step.sessions.iter().map(|s| s.label.as_str()).collect();
                    let expects =
                        concurrent::parse_expect_body(&section.body, &labels).map_err(|e| {
                            if e.starts_with("line ") {
                                e
                            } else {
                                format!("line {line}: {e}")
                            }
                        })?;
                    for (op, expect) in step.sessions.iter_mut().zip(expects) {
                        op.expect = expect;
                    }
                    push_step(&mut items, &mut open_loop, Step::Concurrent(step));
                    continue;
                }
                if rest.trim() == "shape" {
                    let Some(mut step) = awaiting_shape.take() else {
                        return Err(format!(
                            "line {}: `--- expect shape` must directly follow a query step's unordered or ordered expect",
                            section.header_line + 1
                        ));
                    };
                    let lines = parse_shape_body(&section.body)?;
                    if matches!(step, Step::Show(_)) {
                        refuse_unequal_show_shape(&lines, section.header_line + 1)?;
                        push_step(&mut items, &mut open_loop, step);
                        continue;
                    }
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
                    awaiting_plan = Some(step);
                    continue;
                }
                if rest.trim() == "plan" {
                    let Some(mut step) = awaiting_plan.take() else {
                        return Err(format!(
                            "line {}: `--- expect plan` must directly follow a query step's `--- expect shape`",
                            section.header_line + 1
                        ));
                    };
                    let lines = parse_plan_body(&section.body)
                        .map_err(|e| format!("line {}: {e}", section.header_line + 1))?;
                    let Step::Query(query) = &mut step else {
                        return Err(format!(
                            "line {}: `--- expect plan` is supported only on query steps",
                            section.header_line + 1
                        ));
                    };
                    query.plan = Some(PlanExpect { lines });
                    awaiting_same_as_v1 = Some(step);
                    continue;
                }
                if rest.trim() == SAME_AS_V1 {
                    let line = section.header_line + 1;
                    refuse_nonempty_body(&section.body, "an `expect same as v1` section")?;
                    let Some(mut step) =
                        awaiting_plan.take().or_else(|| awaiting_same_as_v1.take())
                    else {
                        return Err(same_as_v1_misplaced(
                            line,
                            pending.as_ref(),
                            &items,
                            open_loop.as_ref(),
                        ));
                    };
                    let Step::Query(query) = &mut step else {
                        return Err(format!(
                            "line {line}: `--- expect same as v1` is supported only on a query declaration step"
                        ));
                    };
                    query.same_as_v1 = true;
                    push_step(&mut items, &mut open_loop, step);
                    continue;
                }
                let mode = parse_expect_header(rest)?;
                let loop_var = open_loop.as_ref().map(|(v, _, _)| v.as_str());
                let completed = match pending.take() {
                    Some(Pending::Load {
                        ordinal,
                        branch,
                        mode: load_mode,
                        generated,
                    }) => Step::Load(LoadStep {
                        ordinal,
                        branch,
                        mode: load_mode,
                        generated,
                        expect: complete_write_expect("load", &mode, section)?,
                    }),
                    Some(Pending::Decl(step)) => {
                        complete_decl_step(step, &mode, section, loop_var)?
                    }
                    Some(Pending::List { ordinal }) => {
                        Step::List(complete_list_step(ordinal, &mode, section, loop_var)?)
                    }
                    Some(Pending::Control {
                        ordinal,
                        prefix,
                        write,
                    }) => Step::Control(complete_control_step(
                        ordinal, prefix, write, &mode, section,
                    )?),
                    Some(Pending::Settings {
                        ordinal,
                        statements,
                    }) => {
                        Step::Settings(complete_settings_step(ordinal, statements, &mode, section)?)
                    }
                    Some(Pending::Show {
                        ordinal,
                        id,
                        prefix,
                    }) => Step::Show(complete_show_step(
                        ordinal, id, prefix, &mode, section, loop_var,
                    )?),
                    Some(Pending::Concurrent(_)) => {
                        unreachable!("a pending concurrent block is completed above")
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
    if awaiting_seam_step {
        return Err("invalid_case: seam has no following operation".into());
    }
    if pending.is_some() {
        return Err("the final step is missing its `--- expect`".into());
    }
    settle_shape(&mut items, &mut open_loop, awaiting_shape.take())?;
    if let Some(step) = awaiting_plan.take().or_else(|| awaiting_same_as_v1.take()) {
        push_step(&mut items, &mut open_loop, step);
    }
    if open_loop.is_some() {
        return Err("a loop is not closed with `--- endloop`".into());
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
        seams,
        source_lines,
        fixture,
        traversal: header.traversal,
        items,
        needs_indices,
    };
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
pub fn canonical_json(value: &Value) -> String {
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

pub struct StepFail {
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

/// The path a step is pinned to: the session's harness-only traversal field,
/// which only the `# traversal:` header pin sets, `None` for `auto`.
fn pinned_mode(session: &Session) -> Option<&'static str> {
    match session.settings().traversal() {
        Traversal::Indexed => Some("indexed"),
        Traversal::Csr => Some("csr"),
        Traversal::Auto => None,
    }
}

/// Runs `fut` with expand-path probes attached when the case's `# traversal:`
/// header pins a path (`pinned_mode`), so the caller can check the pin took
/// effect; unprobed when it pins nothing.
async fn under_traversal<F: Future>(
    mode: Option<&'static str>,
    fut: F,
) -> (F::Output, Option<PathCounts>) {
    match mode {
        Some(_) => {
            let counts = PathCounts {
                indexed: Arc::new(AtomicU64::new(0)),
                csr: Arc::new(AtomicU64::new(0)),
            };
            let probes = QueryIoProbes {
                expand_indexed_runs: Arc::clone(&counts.indexed),
                expand_csr_runs: Arc::clone(&counts.csr),
                ..capture_query_io_probes().unwrap_or_default()
            };
            let out = with_query_io_probes(probes, fut).await;
            (out, Some(counts))
        }
        None => (fut.await, None),
    }
}

/// Rejects an expand on the wrong path, or no observed expand when one is required.
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

/// Whether a match clause list runs at least one Expand: an unbound traversal
/// outside a correlated block (a bound edge and a block take paths of their
/// own that the other-path check covers).
fn expects_expand(clauses: &[Clause]) -> bool {
    clauses.iter().any(|c| match c {
        Clause::Traversal(t) => t.edge_binding.is_none(),
        Clause::Subquery(_) | Clause::Binding(_) | Clause::Filter(_) => false,
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

/// What a v2 query step's `Executed` holds beyond its result: the explain
/// document rendered from the bound plan the run executed, and its report
/// rows.
struct Inspection {
    explain: Value,
    rows: Result<Vec<report::Row>, String>,
}

async fn run_query_step(
    host: &impl ExecutionHost,
    session: &Session,
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
    let inspected = match session.effective(&step.source) {
        Ok(_) => true,
        Err(error) if step.plan.is_some() => {
            return Err(fail(format!("query settings failed: {error}")));
        }
        Err(_) => false,
    };
    let params = match build_params(step.params_raw.as_ref(), &step.decl.params, binding) {
        Ok(params) => params,
        Err(e) => {
            return match &step.expect {
                QueryExpect::Error { needle } if e.contains(needle) => {
                    host.record("parameter_error", || serde_json::json!({"message": e}));
                    Ok(())
                }
                QueryExpect::Error { needle } => {
                    Err(fail(format!("error does not contain \"{needle}\": {e}")))
                }
                QueryExpect::Rows { .. } => Err(fail(e)),
            };
        }
    };
    let target = ReadTarget::branch(&step.branch);
    let (outcome, counts, inspection) = if inspected {
        let door = session.query_inspected(target, &step.source, &step.name, &params);
        let (outcome, counts) = under_traversal(mode, operation(host, step.ordinal, door)).await;
        let outcome = outcome.map_err(&fail)?;
        match outcome {
            Ok(run) => {
                let inspection = Inspection {
                    explain: run.explain.to_value(),
                    rows: report::report_rows(&run.report),
                };
                (Ok(run.result), counts, Some(inspection))
            }
            Err(error) => (Err(error), counts, None),
        }
    } else {
        let query = session.query(target, &step.source, &step.name, &params);
        let (outcome, counts) = under_traversal(mode, operation(host, step.ordinal, query)).await;
        let outcome = outcome.map_err(&fail)?;
        (outcome, counts, None)
    };
    host.observe_query(
        &outcome,
        matches!(step.expect, QueryExpect::Rows { ordered: true, .. }),
    );
    let require_expand =
        step.expects_expand && outcome.is_ok() && matches!(step.expect, QueryExpect::Rows { .. });
    if let Some(violation) = check_pin(mode, &counts, require_expand) {
        return Err(fail(violation));
    }
    if let Some(run) = &inspection {
        run.rows.as_ref().map_err(|error| fail(error.clone()))?;
    }
    match &step.expect {
        QueryExpect::Rows {
            ordered,
            body_raw,
            span,
            shape,
        } => {
            let result = outcome.map_err(|e| fail(format!("query failed: {e}")))?;
            if let (Some(plan), Some(Inspection { explain, rows })) = (&step.plan, &inspection) {
                validate_plan_columns(&plan.lines, &session.catalog()).map_err(&fail)?;
                let report = rows.as_ref().ok().map(Vec::as_slice);
                if let Some(mismatch) = plan_mismatch(&plan.lines, explain, report) {
                    return Err(fail(format!(
                        "{mismatch}\nexplain document:\n{}",
                        serde_json::to_string_pretty(explain).unwrap_or_default()
                    )));
                }
            }
            let catalog = session.catalog();
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
            check_rows(host, &label, &result, *ordered, body_raw, *span, binding)?;
            if step.same_as_v1 && !host.active() {
                check_same_as_v1(host, session, step, &params, &result, *ordered)
                    .await
                    .map_err(&fail)?;
            }
            Ok(())
        }
        QueryExpect::Error { needle } => {
            check_error_expect(host, needle, outcome, "the query succeeded").map_err(fail)
        }
    }
}

/// `--- expect same as v1`: the step's query again on a copy of the case
/// session whose reads run on the reference engine; any v1 error, a gate
/// refusal included, or a row difference fails the step.
async fn check_same_as_v1(
    host: &impl ExecutionHost,
    session: &Session,
    step: &QueryStep,
    params: &omnigraph_compiler::ParamMap,
    v2: &QueryResult,
    ordered: bool,
) -> Result<(), String> {
    inspect_decl_with_policy(&step.decl, &mut false, true)?;
    let v1 = host
        .reference_query(session, step, params)
        .await
        .map_err(|e| format!("expect same as v1: v2 returned rows, v1 failed: {e}"))?;
    let rows = |result: &QueryResult, engine: &str| match result.to_rust_json() {
        Ok(Value::Array(rows)) => Ok(rows),
        Ok(_) => Err(format!(
            "expect same as v1: {engine} returned a non-array row set"
        )),
        Err(e) => Err(format!(
            "expect same as v1: {engine} rows failed to render as JSON: {e}"
        )),
    };
    compare_rows(&rows(&v1, "v1")?, &rows(v2, "v2")?, ordered).map_err(|(message, _)| {
        format!("expect same as v1: the engines disagree (expected = v1, actual = v2): {message}")
    })
}

/// Compares the executed rows with the expect body; a mismatch carries the
/// actual rows as the bless target.
fn check_rows(
    host: &impl ExecutionHost,
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
    host.observe(|| {
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
    host: &impl ExecutionHost,
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
            host.observe(|| format!("error: {msg}"));
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

async fn run_load_step(
    host: &impl ExecutionHost,
    session: &Session,
    step: &LoadStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "load", binding);
    let fail = |message: String| StepFail::new(label.clone(), message);
    let outcome = if let Some(batch) = step.generated.single_batch().map_err(&fail)? {
        operation(
            host,
            step.ordinal,
            session.load(&step.branch, &batch, step.mode),
        )
        .await
        .map_err(&fail)?
        .inspect_err(|error| host.observe_fault(error))
        .map(|_| ())
        .map_err(|error| error.to_string())
    } else {
        operation(
            host,
            step.ordinal,
            step.generated
                .load_observed(session, &step.branch, step.mode, |error| {
                    host.observe_fault(error)
                }),
        )
        .await
        .map_err(&fail)?
    };
    match &step.expect {
        WriteExpect::Ok => outcome.map_err(fail),
        WriteExpect::Error { needle } => {
            check_error_expect(host, needle, outcome, "load succeeded").map_err(fail)
        }
    }
}

async fn run_list_step(
    host: &impl ExecutionHost,
    session: &Session,
    step: &ListStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "branch list", binding);
    let fail = |message: String| StepFail {
        label: label.clone(),
        message,
        bless_lines: None,
    };
    let outcome = match operation(host, step.ordinal, session.branch_list())
        .await
        .map_err(&fail)?
    {
        Ok(names) => Ok(list_result(names).map_err(&fail)?),
        Err(error) => Err(error),
    };
    check_synthetic_expect(
        host,
        session,
        &label,
        &step.expect,
        outcome,
        "branch list",
        binding,
    )
}

/// Holds a read expect against a result the runner built itself (`branch
/// list`, `show`): the result's own schema is the inferred one, so the
/// shape check has no compiler to disagree with.
fn check_synthetic_expect<E: std::fmt::Display>(
    host: &impl ExecutionHost,
    session: &Session,
    label: &str,
    expect: &QueryExpect,
    outcome: Result<QueryResult, E>,
    statement: &str,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let fail = |message: String| StepFail {
        label: label.to_string(),
        message,
        bless_lines: None,
    };
    host.observe_query(
        &outcome,
        matches!(expect, QueryExpect::Rows { ordered: true, .. }),
    );
    match expect {
        QueryExpect::Rows {
            ordered,
            body_raw,
            span,
            shape,
        } => {
            let result = outcome.map_err(|e| fail(format!("`{statement}` failed: {e}")))?;
            let catalog = session.catalog();
            if let Some(mismatch) = shape_mismatch(&shape.lines, &result, result.schema(), &catalog)
            {
                let (message, bless_lines) = match bless_shape_lines(&result, &catalog) {
                    Ok(lines) => (mismatch, Some((shape.span, lines))),
                    Err(unspellable) => (format!("{mismatch}\n{unspellable}"), None),
                };
                return Err(StepFail {
                    label: label.to_string(),
                    message,
                    bless_lines,
                });
            }
            check_rows(host, label, &result, *ordered, body_raw, *span, binding)
        }
        QueryExpect::Error { needle } => {
            check_error_expect(host, needle, outcome, &format!("`{statement}` succeeded"))
                .map_err(fail)
        }
    }
}

/// `show`'s answer as a result: the five non-null `Utf8` columns of
/// `SettingRow::COLUMNS`, rows in definition order, the shape a
/// `--- expect shape` holds against.
fn show_result(rows: &[SettingRow]) -> Result<QueryResult, String> {
    let schema = Arc::new(Schema::new(
        SettingRow::COLUMNS
            .map(|name| Field::new(name, DataType::Utf8, false))
            .to_vec(),
    ));
    let column = |cell: fn(&SettingRow) -> &str| -> ArrayRef {
        Arc::new(StringArray::from(rows.iter().map(cell).collect::<Vec<_>>()))
    };
    let columns = vec![
        column(|row| row.name),
        column(|row| row.value.as_str()),
        column(|row| row.default),
        column(|row| row.source.as_str()),
        column(|row| row.scope.as_str()),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|e| format!("`show` rows failed to build: {e}"))?;
    Ok(QueryResult::new(schema, vec![batch]))
}

/// The session one step runs under: the case session with the step's
/// `set` and `reset` prefix applied to a copy, as the engine does for a
/// declaration's prefix. The case session is unchanged.
fn scoped_session(session: &Session, prefix: &[SettingStmt]) -> Result<Session, String> {
    session.with_prefix(prefix).map_err(|e| e.to_string())
}

fn run_show_step(
    host: &impl ExecutionHost,
    session: &Session,
    step: &ShowStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let name = show_statement_name(step.id);
    let label = step_label(step.ordinal, &name, binding);
    let outcome =
        scoped_session(session, &step.prefix).and_then(|scoped| show_result(&scoped.show(step.id)));
    check_synthetic_expect(host, session, &label, &step.expect, outcome, &name, binding)
}

/// Applies a settings step to the case session: every statement in order,
/// for the steps that follow.
fn run_settings_step(
    host: &impl ExecutionHost,
    session: &mut Session,
    step: &SettingsStep,
    binding: Option<(&str, &str)>,
) -> Result<(), StepFail> {
    let label = step_label(step.ordinal, "settings", binding);
    for stmt in &step.statements {
        session.apply(stmt).map_err(|e| StepFail {
            label: label.clone(),
            message: format!("`{}` failed: {e}", stmt.statement_name()),
            bless_lines: None,
        })?;
    }
    host.observe(|| format!("actual settings: {:?}", session.settings()));
    Ok(())
}

fn check_write_expect<T, E: std::fmt::Display>(
    host: &impl ExecutionHost,
    name: &str,
    expect: &WriteExpect,
    outcome: Result<T, E>,
) -> Result<(), String> {
    match expect {
        WriteExpect::Ok => outcome
            .map(|_| ())
            .map_err(|e| format!("`{name}` failed: {e}")),
        WriteExpect::Error { needle } => {
            check_error_expect(host, needle, outcome, &format!("`{name}` succeeded"))
        }
    }
}

/// Runs a control write under the step's scoped session (the case session
/// plus the section's prefix); a `branch delete` reclaims the branch's forks
/// in a background task the step joins before the next step runs.
async fn run_control_step(
    host: &impl ExecutionHost,
    session: &Session,
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
    let db = scoped_session(session, &step.prefix).map_err(&fail)?;
    match &step.write {
        ControlWrite::Create {
            name: branch,
            from,
            expect,
        } => {
            let parent = from.as_deref().unwrap_or(MAIN_BRANCH);
            let outcome = operation(
                host,
                step.ordinal,
                db.branch_create_from_as(ReadTarget::branch(parent), branch, None),
            )
            .await
            .map_err(&fail)?;
            host.observe(|| format!("actual control: {outcome:?}"));
            check_write_expect(host, name, expect, outcome).map_err(fail)
        }
        ControlWrite::Delete {
            name: branch,
            expect,
        } => {
            let outcome = operation(host, step.ordinal, db.branch_delete_as(branch, None))
                .await
                .map_err(&fail)?;
            host.observe(|| format!("actual control: {outcome:?}"));
            check_write_expect(host, name, expect, outcome).map_err(fail)
        }
        ControlWrite::Merge {
            source,
            into,
            expect,
        } => {
            let target = into.as_deref().unwrap_or(MAIN_BRANCH);
            let outcome = operation(host, step.ordinal, db.branch_merge_as(source, target, None))
                .await
                .map_err(&fail)?
                .map(|result| result.outcome)
                .inspect_err(|error| host.observe_fault(error));
            host.observe(|| format!("actual merge: {outcome:?}"));
            match expect {
                MergeExpect::Write(expect) => {
                    host.observe(|| format!("actual control: {outcome:?}"));
                    check_write_expect(host, name, expect, outcome).map_err(fail)
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
    host: &impl ExecutionHost,
    session: &Session,
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
                    host.record("parameter_error", || serde_json::json!({"message": e}));
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
        operation(
            host,
            step.ordinal,
            session.mutate(&step.branch, &step.source, &step.name, &params),
        ),
    )
    .await;
    let outcome = outcome
        .map_err(&fail)?
        .inspect_err(|error| host.observe_fault(error));
    host.record("mutation_result", || match &outcome {
        Ok(result) => {
            serde_json::json!({"nodes": result.affected_nodes, "edges": result.affected_edges})
        }
        Err(error) => serde_json::json!({"error": error.to_string()}),
    });
    host.observe(|| match &outcome {
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
            check_error_expect(host, needle, outcome, "the mutation succeeded").map_err(fail)
        }
    }
}

/// One session of a block: the statement on its branch through the case
/// session under the case's traversal pin, compared with the session's expect.
pub async fn run_session(
    host: &impl ExecutionHost,
    session: &Session,
    op: &SessionOp,
) -> Result<(), String> {
    let params = build_params(None, &[], None)?;
    let mode = pinned_mode(session);
    match op.kind {
        SessionKind::Mutation => {
            let (outcome, counts) = under_traversal(
                mode,
                session.mutate(&op.branch, &op.source, &op.name, &params),
            )
            .await;
            let outcome = outcome.inspect_err(|error| host.observe_fault(error));
            host.record(
                "mutation_result",
                || match &outcome {
                    Ok(result) => {
                        serde_json::json!({"nodes": result.affected_nodes, "edges": result.affected_edges})
                    }
                    Err(error) => serde_json::json!({"error": error.to_string()}),
                },
            );
            host.observe(|| match &outcome {
                Ok(result) => format!(
                    "session `{}` actual affected: nodes={} edges={}",
                    op.label, result.affected_nodes, result.affected_edges
                ),
                Err(error) => format!("session `{}` actual mutation error: {error}", op.label),
            });
            if let Some(violation) = check_pin(mode, &counts, false) {
                return Err(violation);
            }
            match &op.expect {
                SessionExpect::Ok => outcome
                    .map(|_| ())
                    .map_err(|e| format!("mutation failed: {e}")),
                SessionExpect::Error { needle } => {
                    check_error_expect(host, needle, outcome, "the mutation succeeded")
                }
            }
        }
        SessionKind::Query => {
            let query = session.query(
                ReadTarget::branch(&op.branch),
                &op.source,
                &op.name,
                &params,
            );
            let (outcome, counts) = under_traversal(mode, query).await;
            host.observe_query(&outcome, false);
            if let Some(violation) = check_pin(mode, &counts, false) {
                return Err(violation);
            }
            match &op.expect {
                SessionExpect::Ok => outcome
                    .map(|_| ())
                    .map_err(|e| format!("query failed: {e}")),
                SessionExpect::Error { needle } => {
                    check_error_expect(host, needle, outcome, "the query succeeded")
                }
            }
        }
    }
}

/// The case session: the definition's defaults over the fresh handle, the
/// runner's `engine` (`OMNIGRAPH_GQ_ENGINE`) as the baseline `engine` row,
/// then the `# traversal:` pin on the harness-only field no `show` reports.
pub fn case_session(db: Omnigraph, case: &Case, engine: Engine) -> Result<Session, String> {
    let mut settings = SessionSettings::default()
        .with("engine", engine.as_str())
        .map_err(|e| format!("invalid_case: {e}"))?;
    if let Some(mode) = case.traversal {
        let pinned = Traversal::from_spelling(mode).ok_or_else(|| {
            format!("`# traversal: {mode}` refused: expected one of auto, indexed, csr")
        })?;
        settings = settings.with_traversal(pinned);
    }
    Ok(Session::from_defaults(Arc::new(db), settings))
}

pub async fn seed_case(session: &Session, seed: &Seed, needs_indices: bool) -> Result<(), String> {
    seed.load(session).await?;
    if needs_indices {
        session
            .ensure_indices()
            .await
            .map_err(|e| format!("ensure_indices failed: {e}"))?;
    }
    Ok(())
}

/// Executes the parsed steps and returns the current session, including any restart replacement.
pub fn execute_steps<'a, H: ExecutionHost>(
    case: &'a Case,
    path: &'a Path,
    bless: bool,
    session: Session,
    uri: &'a str,
    storage: Option<Arc<dyn StorageAdapter>>,
    host: &'a H,
) -> futures::future::BoxFuture<'a, Result<Session, String>> {
    execute_steps_inner(case, path, bless, session, uri, storage, host).boxed()
}

async fn execute_steps_inner<H: ExecutionHost>(
    case: &Case,
    path: &Path,
    bless: bool,
    mut session: Session,
    uri: &str,
    storage: Option<Arc<dyn StorageAdapter>>,
    host: &H,
) -> Result<Session, String> {
    host.admit_case(case)?;
    let mut first_fail: Option<StepFail> = None;
    let mut generation = 0usize;
    host.observe(|| {
        if case.fixture.is_some() {
            "lifetime: initialized generation 0".into()
        } else {
            "lifetime: opened generation 0".into()
        }
    });
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
                host.observe(|| {
                    format!(
                        "operation: ordinal={ordinal} line={:?} binding={binding:?} generation={generation} expected={step:?}",
                        case.source_lines.get(&ordinal)
                    )
                });
                host.begin_operation(||
                    serde_json::json!({"ordinal": ordinal, "source_line": case.source_lines.get(&ordinal), "loop_binding": binding, "generation": generation}),
                );
                host.record(
                    "expectation",
                    || match step {
                        Step::Query(q) => {
                            let mut evidence = read_expect_evidence(&q.expect);
                            if let Some(plan) = &q.plan {
                                evidence["plan"] = serde_json::json!(plan.lines);
                            }
                            evidence
                        }
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
                        Step::Load(step) => serde_json::json!({"kind": "load", "expectation": format!("{:?}", step.expect)}),
                        Step::Control(c) => {
                            serde_json::json!({"control": c.name, "expectation": format!("{:?}", c.write)})
                        }
                        Step::Settings(s) => {
                            serde_json::json!({"kind": "settings", "statements": format!("{:?}", s.statements)})
                        }
                        Step::Show(s) => read_expect_evidence(&s.expect),
                        Step::Restart { .. } => {
                            serde_json::json!({"kind": "restart", "storage": "preserved"})
                        }
                        Step::Concurrent(c) => serde_json::json!({
                            "kind": "concurrent",
                            "sessions": c.sessions.iter().map(|s| serde_json::json!({"label": s.label, "branch": s.branch, "kind": s.kind.name(), "expect": format!("{:?}", s.expect)})).collect::<Vec<_>>(),
                            "order": c.order.iter().map(|e| format!("{} {}", c.sessions[e.session].label, e.event)).collect::<Vec<_>>(),
                        }),
                    },
                );
                let seams = case.seams.get(&ordinal).map_or(&[][..], Vec::as_slice);
                let armed = host.arm_seams(seams, step)?;
                let lifetime_before = host.lifetime_counts();
                host.measure_step_begin(
                    ordinal as u64,
                    case.source_lines.get(&ordinal).map(|line| *line as u64),
                    step_kind(step),
                );
                let outcome = match step {
                    Step::Query(q) => {
                        run_query_step(host, &session, pinned_mode(&session), q, binding).await
                    }
                    Step::Mutate(m) => {
                        run_mutate_step(host, &session, pinned_mode(&session), m, binding).await
                    }
                    Step::Load(step) => run_load_step(host, &session, step, binding).await,
                    Step::Control(c) => run_control_step(host, &session, c, binding).await,
                    Step::List(l) => run_list_step(host, &session, l, binding).await,
                    Step::Settings(s) => run_settings_step(host, &mut session, s, binding),
                    Step::Show(s) => run_show_step(host, &session, s, binding),
                    Step::Concurrent(c) => host.concurrent_step(&session, case, c).await,
                    Step::Restart { ordinal } => {
                        generation += 1;
                        host.observe(|| format!("lifetime: reopen generation {generation}"));
                        let detached = session.detach();
                        host.operation_started(*ordinal)?;
                        let reopened = host.reopen(uri, storage.clone()).await;
                        host.operation_finished(*ordinal)?;
                        session = match reopened {
                            Ok(db) => detached.attach(Arc::new(db)),
                            Err(error) => {
                                host.observe_fault(&error);
                                let lifetime_after = host.lifetime_counts();
                                host.record(
                                    "engine_lifetime",
                                    || serde_json::json!({"before": lifetime_before, "after": lifetime_after}),
                                );
                                if let (Some(before), Some(after)) =
                                    (lifetime_before, lifetime_after)
                                    && (after[0] != before[0] || after[1] > before[1] + 1)
                                {
                                    return Err(format!(
                                        "worker_failed: unexpected engine init/open call during step {ordinal}: {before:?} -> {after:?}"
                                    ));
                                }
                                let fail = StepFail {
                                    label: step_label(*ordinal, "restart", binding),
                                    message: format!("reopen failed: {error}"),
                                    bless_lines: None,
                                };
                                host.record(
                                    "assertion",
                                    || serde_json::json!({"status": "failed", "code": "assertion_failed", "message": fail.message}),
                                );
                                host.observe(|| {
                                    format!(
                                        "operation result: Err({:?})",
                                        (&fail.label, &fail.message)
                                    )
                                });
                                return Err(format!("{}: {}", fail.label, fail.message));
                            }
                        };
                        Ok(())
                    }
                };
                let seam_result = host.finish_seams(armed);
                host.measure_step_end(ordinal as u64);
                let lifetime_after = host.lifetime_counts();
                host.record(
                    "engine_lifetime",
                    || serde_json::json!({"before": lifetime_before, "after": lifetime_after}),
                );
                if let (Some(before), Some(after)) = (lifetime_before, lifetime_after) {
                    let opens = u64::from(matches!(step, Step::Restart { .. }));
                    if after != [before[0], before[1] + opens] {
                        return Err(format!(
                            "worker_failed: unexpected engine init/open call during step {ordinal}: {before:?} -> {after:?}"
                        ));
                    }
                }
                host.record(
                    "assertion",
                    || match &outcome {
                        Ok(()) => serde_json::json!({"status": "passed"}),
                        Err(error) => {
                            serde_json::json!({"status": "failed", "code": "assertion_failed", "message": error.message})
                        }
                    },
                );
                host.observe(|| {
                    format!(
                        "operation result: {:?}",
                        outcome.as_ref().map_err(|f| (&f.label, &f.message))
                    )
                });
                if let Err(error) = seam_result {
                    return Err(format!(
                        "{error}; operation result: {:?}",
                        outcome.as_ref().map_err(|f| (&f.label, &f.message))
                    ));
                }
                if host.observe_snapshot() {
                    let snapshot = session
                        .resolve_snapshot("main")
                        .await
                        .map_err(|e| format!("observe main snapshot: {e}"))?;
                    host.observe(|| format!("main snapshot: {snapshot}"));
                }
                if let Err(fail) = outcome {
                    first_fail = Some(fail);
                    break 'run;
                }
            }
        }
    }
    let Some(fail) = first_fail else {
        return Ok(session);
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

impl Step {
    pub fn kind(&self) -> &'static str {
        step_kind(self)
    }

    pub fn source(&self) -> Option<&str> {
        match self {
            Self::Query(step) => Some(&step.source),
            Self::Mutate(step) => Some(&step.source),
            _ => None,
        }
    }
}

impl StepFail {
    pub fn new(label: String, message: String) -> Self {
        Self {
            label,
            message,
            bless_lines: None,
        }
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}

async fn operation<F: Future>(
    host: &impl ExecutionHost,
    ordinal: usize,
    future: F,
) -> Result<F::Output, String> {
    host.operation_started(ordinal)?;
    let result = future.await;
    host.operation_finished(ordinal)?;
    Ok(result)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepKind {
    Query,
    Mutate,
    Load,
    BranchCreate,
    BranchDelete,
    BranchMerge,
    BranchList,
    Settings,
    Show,
    Restart,
    Concurrent,
}

#[derive(Clone, Debug)]
pub struct StepDescriptor {
    pub ordinal: usize,
    pub kind: StepKind,
    pub source: String,
    pub in_loop: bool,
}

impl Case {
    pub fn source(&self) -> &str {
        &self.input_text
    }

    pub fn steps(&self) -> Vec<StepDescriptor> {
        let lines: Vec<&str> = self.input_text.lines().collect();
        let (_, sections) = split_sections(&lines);
        self.items
            .iter()
            .flat_map(|item| {
                let (steps, in_loop) = match item {
                    Item::Step(step) => (std::slice::from_ref(step), false),
                    Item::Loop { steps, .. } => (steps.as_slice(), true),
                };
                steps.iter().map(move |step| (step, in_loop))
            })
            .map(|(step, in_loop)| {
                let section = self.source_lines.get(&step.ordinal()).and_then(|line| {
                    sections
                        .iter()
                        .find(|section| section.header_line + 1 == *line)
                });
                let source = section.map_or_else(String::new, |section| {
                    std::iter::once(lines[section.header_line])
                        .chain(section.body.iter().map(|(_, line)| *line))
                        .collect::<Vec<_>>()
                        .join("\n")
                        .trim()
                        .to_string()
                });
                StepDescriptor {
                    ordinal: step.ordinal(),
                    kind: step.operation_kind(),
                    source,
                    in_loop,
                }
            })
            .collect()
    }
}

impl Step {
    pub fn operation_kind(&self) -> StepKind {
        match self {
            Self::Query(_) => StepKind::Query,
            Self::Mutate(_) => StepKind::Mutate,
            Self::Load(_) => StepKind::Load,
            Self::Control(step) => match &step.write {
                ControlWrite::Create { .. } => StepKind::BranchCreate,
                ControlWrite::Delete { .. } => StepKind::BranchDelete,
                ControlWrite::Merge { .. } => StepKind::BranchMerge,
            },
            Self::List(_) => StepKind::BranchList,
            Self::Settings(_) => StepKind::Settings,
            Self::Show(_) => StepKind::Show,
            Self::Restart { .. } => StepKind::Restart,
            Self::Concurrent(_) => StepKind::Concurrent,
        }
    }
}

#[cfg(test)]
mod tests;
