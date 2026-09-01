//! GQ logic tests: walks `tests/gq_logic_tests/*.gqt` and runs each case
//! against a fresh temporary store (init, load, index, then the steps in
//! order). The file format, refusal set, comparison semantics, and bless
//! workflow are specified in `docs/rfcs/0045-gq-logic-tests.md`.
//!
//! To libtest the whole walker is one test; case concurrency comes from a
//! `JoinSet`. `OMNIGRAPH_GQ_LOGIC_TESTS=<substr>[,<substr>]` restricts the run
//! to matching case files; `OMNIGRAPH_GQ_BLESS=1` rewrites the failing step's
//! `--- expect` rows in place.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::with_traversal_mode;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::query::ast::{Clause, Expr, Literal, Param, QueryDecl};
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::schema::ast::{Annotation, PropDecl, SchemaDecl};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{JsonParamMode, json_params_to_param_map};
use serde_json::Value;
use tokio::task::JoinSet;

#[derive(Debug)]
struct Case {
    schema: String,
    seed: String,
    traversal: &'static str,
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

fn parse_header(lines: &[&str]) -> Result<Header, String> {
    let mut issue: Option<IssueRef> = None;
    let mut red_on = false;
    let mut notes_seen = false;
    let mut traversal: Option<&'static str> = None;
    let mut have_key = false;
    for (idx, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let Some(rest) = line.strip_prefix('#') else {
            return Err(format!(
                "line {}: only `#` header lines may precede the first section",
                idx + 1
            ));
        };
        let content = rest.trim_start();
        let key = content
            .split_once(':')
            .map(|(k, _)| k)
            .filter(|k| !k.is_empty() && k.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
        let Some(key) = key else {
            if !have_key {
                return Err(format!(
                    "line {}: the first header line must start a key (`# issue:`, `# red_on:`, `# notes:`, `# traversal:`)",
                    idx + 1
                ));
            }
            continue;
        };
        let value = content[key.len() + 1..].trim();
        let duplicate = match key {
            "issue" => issue.is_some(),
            "red_on" => red_on,
            "notes" => notes_seen,
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
                    IssueRef::Num(n)
                });
            }
            "red_on" => {
                if value.is_empty() {
                    return Err(format!("line {}: `# red_on:` needs a value", idx + 1));
                }
                red_on = true;
            }
            "notes" => notes_seen = true,
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
            other => return Err(format!("line {}: unknown header key `# {other}:`", idx + 1)),
        }
        have_key = true;
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
    has_order_clause: bool,
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
                    has_order_clause: !decl.order_clause.is_empty(),
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
                        if ordered && !step.has_order_clause {
                            return Err(
                                "`expect ordered` is refused for a query without an `order` clause"
                                    .into(),
                            );
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
        traversal: header.traversal.unwrap_or("indexed"),
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

async fn run_query_step(
    db: &Omnigraph,
    mode: &'static str,
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
    let outcome = with_traversal_mode(
        mode,
        db.query(
            ReadTarget::branch("main"),
            &step.source,
            &step.name,
            &params,
        ),
    )
    .await;
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
    mode: &'static str,
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
    let outcome =
        with_traversal_mode(mode, db.mutate("main", &step.source, &step.name, &params)).await;
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

async fn execute_case(case: &Case, path: &Path, bless: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir failed: {e}"))?;
    let uri = dir
        .path()
        .to_str()
        .ok_or_else(|| "temp path is not utf-8".to_string())?
        .to_string();
    let mut db = Omnigraph::init(&uri, &case.schema)
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

/// Splits the corpus dir into `.gqt` case files and foreign entries (anything
/// else except dotfiles); a foreign entry is a mis-renamed or nested case that
/// would otherwise silently never run.
fn list_cases(root: &Path) -> (Vec<PathBuf>, Vec<String>) {
    let mut files = Vec::new();
    let mut foreign = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue;
            }
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("gqt") {
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

#[tokio::test(flavor = "multi_thread")]
async fn gq_logic_tests() {
    let root = corpus_root();
    let (mut files, foreign) = list_cases(&root);
    assert!(
        foreign.is_empty(),
        "non-.gqt entries under {}: {}; a mis-renamed or nested case must never silently skip",
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

    let total = files.len();
    let mut names: std::collections::HashMap<tokio::task::Id, String> =
        std::collections::HashMap::new();
    let mut set: JoinSet<(String, Result<(), String>)> = JoinSet::new();
    for path in files {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("<non-utf8>")
            .to_string();
        let task_stem = stem.clone();
        let handle = set.spawn(async move {
            let result = run_case(path, bless).await;
            (task_stem, result)
        });
        names.insert(handle.id(), stem);
    }

    let mut failures: Vec<(String, String)> = Vec::new();
    while let Some(joined) = set.join_next_with_id().await {
        match joined {
            Ok((_, (stem, Ok(())))) => println!("ok {stem}"),
            Ok((_, (stem, Err(detail)))) => {
                println!("FAIL {stem}");
                failures.push((stem, detail));
            }
            Err(join_err) => {
                let stem = names
                    .get(&join_err.id())
                    .cloned()
                    .unwrap_or_else(|| "<unknown case>".to_string());
                println!("FAIL {stem}");
                failures.push((stem, format!("case panicked: {join_err}")));
            }
        }
    }

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
    assert_eq!(case.traversal, "indexed");
}

#[test]
fn header_continuation_lines_extend_the_previous_key() {
    let text = format!(
        "# issue: 7\n# red_on: 2026-01-01, the run\n#   returned 8: not 20.\n{SCHEMA}{SEED}{QUERY}{EXPECT}"
    );
    parse_case("issue_7_continued", &text).unwrap();
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
fn refuses_first_header_line_without_a_key() {
    let text = format!("# stray continuation\n{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("first header line"));
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
    let text = format!("# issue: 7\n# red_on:\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7_x", &text).contains("needs a value"));
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
}

#[test]
fn walker_flags_foreign_corpus_entries() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.gqt"), "x").unwrap();
    std::fs::write(dir.path().join("b.txt"), "x").unwrap();
    std::fs::write(dir.path().join(".hidden"), "x").unwrap();
    std::fs::create_dir(dir.path().join("nested")).unwrap();
    let (files, foreign) = list_cases(dir.path());
    assert_eq!(files.len(), 1);
    assert_eq!(foreign, vec!["b.txt".to_string(), "nested".to_string()]);
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
