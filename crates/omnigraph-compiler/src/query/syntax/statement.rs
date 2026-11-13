//! The statements a source may hold instead of declarations (`branch`,
//! `commit`, `load`, `show` and the settings lines), and the helpers every
//! builder of the parse tree shares: positions, diagnostics, string
//! literals, edge names and traversal bounds.

use super::Rule;
use crate::error::{CompilerError, Result, decode_string_literal};
use crate::query::codes::{Q003, Q004, Q005};
use crate::query::diagnostic::{Position, QueryDiagnostic};
use crate::query::file::*;
use crate::settings::{SessionSettings, SessionSettingsError, SettingId, SettingValue};

pub(crate) fn position_of(pair: &pest::iterators::Pair<Rule>) -> Position {
    let span = pair.as_span();
    Position::at(span.get_input(), span.start())
}

pub(crate) fn diagnostic_at(
    code: crate::query::diagnostic::QueryCode,
    message: impl Into<String>,
    at: &pest::iterators::Pair<Rule>,
) -> QueryDiagnostic {
    QueryDiagnostic::parse(code, message, Some(position_of(at)))
}

pub(crate) fn query_error_at(
    message: impl Into<String>,
    at: &pest::iterators::Pair<Rule>,
) -> CompilerError {
    CompilerError::query(diagnostic_at(Q005, message, at))
}

pub(crate) fn parse_setting_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<SettingStmt, QueryDiagnostic> {
    let form = pair
        .into_inner()
        .next()
        .expect("grammar: setting_stmt holds one form");
    match form.as_rule() {
        Rule::set_stmt => {
            let mut parts = form
                .into_inner()
                .filter(|inner| inner.as_rule() != Rule::kw_set);
            let name = parts
                .next()
                .expect("grammar: set_stmt holds a setting_name");
            let id = parse_setting_id(&name)?;
            let value_pair = parts
                .next()
                .expect("grammar: set_stmt holds a setting_value");
            let value = parse_setting_value(id, &value_pair)?;
            SessionSettings::default()
                .set(id, &value)
                .map_err(|error| setting_diagnostic(error, &value_pair))?;
            Ok(SettingStmt::Set { id, value })
        }
        Rule::reset_stmt => Ok(SettingStmt::Reset {
            id: parse_setting_target(form)?,
        }),
        other => unreachable!("grammar: setting_stmt admits no {other:?}"),
    }
}

/// The `<name>` or `all` after `reset` or `show`.
pub(crate) fn parse_setting_target(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<Option<SettingId>, QueryDiagnostic> {
    let target = pair
        .into_inner()
        .find(|inner| matches!(inner.as_rule(), Rule::kw_all | Rule::setting_name))
        .expect("grammar: reset_stmt and show_stmt hold `all` or a setting_name");
    match target.as_rule() {
        Rule::kw_all => Ok(None),
        Rule::setting_name => parse_setting_id(&target).map(Some),
        other => unreachable!("grammar: a setting target admits no {other:?}"),
    }
}

fn parse_setting_id(
    name: &pest::iterators::Pair<Rule>,
) -> std::result::Result<SettingId, QueryDiagnostic> {
    SettingId::parse(name.as_str()).map_err(|error| setting_diagnostic(error, name))
}

fn parse_setting_value(
    id: SettingId,
    pair: &pest::iterators::Pair<Rule>,
) -> std::result::Result<SettingValue, QueryDiagnostic> {
    let token = pair
        .clone()
        .into_inner()
        .next()
        .expect("grammar: setting_value wraps an integer, an ident or a string_lit");
    match token.as_rule() {
        Rule::digits => token
            .as_str()
            .parse::<i64>()
            .map(SettingValue::Integer)
            .map_err(|_| {
                setting_diagnostic(
                    SessionSettingsError::OutOfRange {
                        setting: id,
                        got: token.as_str().to_string(),
                    },
                    pair,
                )
            }),
        Rule::ident => Ok(SettingValue::Ident(token.as_str().to_string())),
        Rule::string_lit => decode_string_literal(token.as_str())
            .map(SettingValue::Str)
            .map_err(|error| diagnostic_at(Q003, parse_error_message(error), &token)),
        other => unreachable!("grammar: setting_value admits no {other:?}"),
    }
}

fn setting_diagnostic(
    error: SessionSettingsError,
    at: &pest::iterators::Pair<Rule>,
) -> QueryDiagnostic {
    diagnostic_at(Q003, error.to_string(), at)
}

pub(crate) fn parse_branch_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<BranchStmt, QueryDiagnostic> {
    let form = pair
        .into_inner()
        .find(|inner| inner.as_rule() != Rule::kw_branch)
        .expect("grammar: branch_stmt holds one form after kw_branch");
    let rule = form.as_rule();
    let has = |wanted: Rule| {
        form.clone()
            .into_inner()
            .any(|inner| inner.as_rule() == wanted)
    };
    let as_commit = has(Rule::as_commit_clause);
    let dry_run = has(Rule::dry_run_clause);
    let between = has(Rule::kw_to);
    let page = parse_diff_page_clause(&form)?;
    let from_commit = form
        .clone()
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::create_from_commit)
        .map(|pair| {
            let literal = pair
                .into_inner()
                .find(|inner| inner.as_rule() == Rule::string_lit)
                .expect("grammar: create_from_commit names one string literal");
            decode_graph_commit_id(&literal)
        })
        .transpose()?;
    let names = form
        .into_inner()
        .filter(|inner| inner.as_rule() == Rule::branch_name)
        .map(parse_branch_name)
        .collect::<std::result::Result<Vec<_>, QueryDiagnostic>>()?;
    let mut names = names.into_iter();
    match (rule, names.next(), names.next()) {
        (Rule::branch_create, Some(name), from) => Ok(BranchStmt::Write(BranchWrite::Create {
            name,
            from: from_commit
                .map(BranchCreateFrom::Commit)
                .or(from.map(BranchCreateFrom::Branch)),
        })),
        (Rule::branch_delete, Some(name), None) => {
            Ok(BranchStmt::Write(BranchWrite::Delete { name }))
        }
        (Rule::branch_merge, Some(source), into) if dry_run => {
            Ok(BranchStmt::Read(BranchRead::MergeDryRun { source, into }))
        }
        (Rule::branch_merge, Some(source), into) => Ok(BranchStmt::Write(BranchWrite::Merge {
            source,
            into,
            as_commit,
        })),
        (Rule::branch_list, None, None) => Ok(BranchStmt::List),
        (Rule::branch_merge_base, Some(source), into) => {
            Ok(BranchStmt::Read(BranchRead::MergeBase { source, into }))
        }
        (Rule::branch_diff, Some(from), Some(to)) if between => {
            Ok(BranchStmt::Read(BranchRead::Diff {
                sides: BranchDiffSides::Between { from, to },
                page,
            }))
        }
        (Rule::branch_diff, Some(source), into) => Ok(BranchStmt::Read(BranchRead::Diff {
            sides: BranchDiffSides::Branch { source, into },
            page,
        })),
        (other, _, _) => {
            unreachable!("grammar: every branch form fixes its name arity, got {other:?}")
        }
    }
}

/// The `after page "<token>"` and `limit <n>` clauses of `branch diff`; both
/// absent on every other form. A limit is at least one (`Q004` otherwise).
fn parse_diff_page_clause(
    form: &pest::iterators::Pair<Rule>,
) -> std::result::Result<DiffPageClause, QueryDiagnostic> {
    let mut page = DiffPageClause::default();
    for clause in form.clone().into_inner() {
        match clause.as_rule() {
            Rule::diff_after_page => {
                let literal = clause
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::string_lit)
                    .expect("grammar: `after page` names one string literal");
                let token = decode_string_literal(literal.as_str())
                    .map_err(|error| diagnostic_at(Q004, parse_error_message(error), &literal))?;
                page.after_page = Some(token);
            }
            Rule::diff_limit => {
                let integer = clause
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::digits)
                    .expect("grammar: `limit` names one integer");
                let limit = integer
                    .as_str()
                    .parse::<usize>()
                    .ok()
                    .filter(|limit| *limit > 0)
                    .ok_or_else(|| {
                        diagnostic_at(
                            Q004,
                            format!("diff limit {} must be a positive integer", integer.as_str()),
                            &integer,
                        )
                    })?;
                page.limit = Some(limit);
            }
            _ => {}
        }
    }
    Ok(page)
}

pub(crate) fn parse_commit_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<CommitWrite, QueryDiagnostic> {
    let form = pair
        .into_inner()
        .find(|inner| inner.as_rule() != Rule::kw_commit)
        .expect("grammar: commit_stmt holds one form after kw_commit");
    let rule = form.as_rule();
    let literal = form
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::string_lit)
        .expect("grammar: every commit form names one string literal");
    let graph_commit_id = decode_graph_commit_id(&literal)?;
    match rule {
        Rule::commit_publish => Ok(CommitWrite::Publish { graph_commit_id }),
        Rule::commit_drop => Ok(CommitWrite::Drop { graph_commit_id }),
        other => unreachable!("grammar: commit_stmt admits no {other:?}"),
    }
}

pub(crate) fn parse_load_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<LoadStmt, QueryDiagnostic> {
    let at = pair.clone();
    let mut source = None;
    let mut target = None;
    let mut mode = None;
    let mut delimiter = None;
    let mut header = None;
    let mut columns = None;
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::string_lit => {
                let decoded = decode_string_literal(inner.as_str())
                    .map_err(|error| diagnostic_at(Q004, parse_error_message(error), &inner))?;
                if decoded.is_empty() || decoded.chars().any(char::is_control) {
                    return Err(diagnostic_at(
                        Q004,
                        "a load source is a non-empty URI without control characters",
                        &inner,
                    ));
                }
                source = Some(decoded);
            }
            Rule::load_target => {
                let mut parts = inner.clone().into_inner();
                let kind = parts
                    .next()
                    .expect("grammar: load_target opens with load_kind");
                let name = parts
                    .next()
                    .expect("grammar: load_target closes with type_name")
                    .as_str()
                    .to_string();
                target = Some(match kind.as_str() {
                    "node" => LoadTarget::Node(name),
                    "edge" => LoadTarget::Edge(name),
                    other => unreachable!("grammar: load_kind admits no {other}"),
                });
            }
            Rule::load_option => {
                let option = inner
                    .clone()
                    .into_inner()
                    .next()
                    .expect("grammar: load_option holds one form");
                match option.as_rule() {
                    Rule::load_mode => {
                        refuse_repeated_load_option(mode.is_some(), "mode", &option)?;
                        let name = option
                            .into_inner()
                            .find(|part| part.as_rule() == Rule::load_mode_name)
                            .expect("grammar: load_mode names its mode");
                        mode = Some(match name.as_str() {
                            "overwrite" => LoadStmtMode::Overwrite,
                            "append" => LoadStmtMode::Append,
                            "merge" => LoadStmtMode::Merge,
                            other => unreachable!("grammar: load_mode_name admits no {other}"),
                        });
                    }
                    Rule::load_delimiter => {
                        refuse_repeated_load_option(delimiter.is_some(), "delimiter", &option)?;
                        let literal = option
                            .into_inner()
                            .find(|part| part.as_rule() == Rule::string_lit)
                            .expect("grammar: load_delimiter holds one string literal");
                        let decoded = decode_string_literal(literal.as_str()).map_err(|error| {
                            diagnostic_at(Q004, parse_error_message(error), &literal)
                        })?;
                        let bytes = decoded.as_bytes();
                        if bytes.len() != 1 || !bytes[0].is_ascii() || bytes[0].is_ascii_control() {
                            return Err(diagnostic_at(
                                Q004,
                                "a load delimiter is one printable ASCII character",
                                &literal,
                            ));
                        }
                        delimiter = Some(bytes[0]);
                    }
                    Rule::load_header => {
                        refuse_repeated_load_option(header.is_some(), "header", &option)?;
                        header = Some(true);
                    }
                    Rule::load_columns => {
                        refuse_repeated_load_option(columns.is_some(), "columns", &option)?;
                        let mut names: Vec<String> = Vec::new();
                        for part in option
                            .into_inner()
                            .filter(|part| part.as_rule() == Rule::ident)
                        {
                            let name = part.as_str().to_string();
                            if names.contains(&name) {
                                return Err(diagnostic_at(
                                    Q004,
                                    format!("column `{name}` is named twice"),
                                    &part,
                                ));
                            }
                            names.push(name);
                        }
                        columns = Some(names);
                    }
                    other => unreachable!("grammar: load_option admits no {other:?}"),
                }
            }
            _ => {}
        }
    }
    let Some(mode) = mode else {
        return Err(diagnostic_at(
            Q004,
            "a load statement names its mode: `mode overwrite`, `mode append` or `mode merge`",
            &at,
        ));
    };
    let header = header.unwrap_or(false);
    if !header && columns.is_none() {
        return Err(diagnostic_at(
            Q004,
            "a load without `header` names its columns: `columns (a, b, …)`",
            &at,
        ));
    }
    Ok(LoadStmt {
        source: source.expect("grammar: load_stmt names one source literal"),
        target: target.expect("grammar: load_stmt names one target"),
        mode,
        delimiter: delimiter.unwrap_or(b','),
        header,
        columns,
    })
}

fn refuse_repeated_load_option(
    seen: bool,
    option: &str,
    at: &pest::iterators::Pair<Rule>,
) -> std::result::Result<(), QueryDiagnostic> {
    if seen {
        return Err(diagnostic_at(
            Q004,
            format!("`{option}` is given twice in one load statement"),
            at,
        ));
    }
    Ok(())
}

/// A quoted graph commit id operand: decoded, non-empty, untrimmed-equal
/// and free of control characters (`Q004` otherwise).
fn decode_graph_commit_id(
    literal: &pest::iterators::Pair<Rule>,
) -> std::result::Result<String, QueryDiagnostic> {
    let graph_commit_id = decode_string_literal(literal.as_str())
        .map_err(|error| diagnostic_at(Q004, parse_error_message(error), literal))?;
    if graph_commit_id.is_empty()
        || graph_commit_id.trim() != graph_commit_id
        || graph_commit_id.chars().any(char::is_control)
    {
        return Err(diagnostic_at(
            Q004,
            format!("graph commit id {graph_commit_id:?} is not a commit id"),
            literal,
        ));
    }
    Ok(graph_commit_id)
}

fn parse_branch_name(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<String, QueryDiagnostic> {
    let position = position_of(&pair);
    let token = pair
        .into_inner()
        .next()
        .expect("grammar: branch_name wraps an ident or a string_lit");
    let name = match token.as_rule() {
        Rule::string_lit => decode_string_literal(token.as_str())
            .map_err(|error| diagnostic_at(Q004, parse_error_message(error), &token))?,
        Rule::ident => token.as_str().to_string(),
        other => unreachable!("grammar: branch_name admits no {other:?}"),
    };
    if name.chars().any(char::is_control) {
        return Err(QueryDiagnostic::parse(
            Q004,
            format!("branch name {name:?} contains a control character"),
            Some(position),
        ));
    }
    if name.trim() != name {
        return Err(QueryDiagnostic::parse(
            Q004,
            format!("branch name {name:?} has leading or trailing whitespace"),
            Some(position),
        ));
    }
    if name.is_empty() {
        return Err(QueryDiagnostic::parse(
            Q004,
            format!("branch name {name:?} cannot be empty"),
            Some(position),
        ));
    }
    Ok(name)
}

/// A declaration body the hand-written parser refused: its bare message
/// names the construct and `Display` adds the one `parse error:` prefix; a
/// diagnostic it already carries passes through.
pub(crate) fn compiler_error_to_diagnostic(
    err: CompilerError,
    position: Position,
) -> QueryDiagnostic {
    match err {
        CompilerError::Query(diagnostic) => *diagnostic,
        other => QueryDiagnostic::parse(Q005, parse_error_message(other), Some(position)),
    }
}

fn parse_error_message(error: CompilerError) -> String {
    match error {
        CompilerError::Parse(message) => message,
        other => other.to_string(),
    }
}

pub(crate) fn parse_unattached_base(
    clause: pest::iterators::Pair<Rule>,
) -> std::result::Result<Option<UnattachedBase>, QueryDiagnostic> {
    let Some(operand) = clause
        .into_inner()
        .find(|inner| matches!(inner.as_rule(), Rule::string_lit | Rule::variable))
    else {
        return Ok(None);
    };
    match operand.as_rule() {
        Rule::variable => Ok(Some(UnattachedBase::Param(
            operand.as_str()[1..].to_string(),
        ))),
        Rule::string_lit => Ok(Some(UnattachedBase::Literal(decode_graph_commit_id(
            &operand,
        )?))),
        other => unreachable!("grammar: as_unattached_clause admits no operand {other:?}"),
    }
}

pub(crate) fn parse_edge_name(pair: pest::iterators::Pair<Rule>) -> Result<String> {
    if pair.as_rule() != Rule::string_lit {
        return Ok(pair.as_str().to_string());
    }
    let name = parse_string_lit(&pair)?;
    let mut chars = name.chars();
    let starts = chars
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == '_');
    if starts && chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(name)
    } else {
        Err(query_error_at(
            format!(
                "`{}` is not an edge name; a quoted edge is its bare name in quotes, as in `$a \"in\" $b`",
                pair.as_str()
            ),
            &pair,
        ))
    }
}

pub(crate) fn parse_traversal_bounds(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(u32, Option<u32>)> {
    let mut inner = pair.into_inner();
    let min_pair = inner
        .next()
        .ok_or_else(|| CompilerError::Parse("traversal bound missing min hop".to_string()))?;
    let min = min_pair
        .as_str()
        .parse::<u32>()
        .map_err(|e| query_error_at(format!("invalid traversal min bound: {}", e), &min_pair))?;
    let max = inner
        .next()
        .map(|p| {
            p.as_str()
                .parse::<u32>()
                .map_err(|e| query_error_at(format!("invalid traversal max bound: {}", e), &p))
        })
        .transpose()?;
    Ok((min, max))
}

pub(crate) fn parse_string_lit(at: &pest::iterators::Pair<Rule>) -> Result<String> {
    decode_string_literal(at.as_str())
        .map_err(|error| query_error_at(parse_error_message(error), at))
}
