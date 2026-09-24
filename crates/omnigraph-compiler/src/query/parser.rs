use pest::Parser;
use pest::error::InputLocation;
use pest_derive::Parser;

use crate::error::{
    CompilerError, ParseDiagnostic, Result, SourceSpan, decode_string_literal, render_span,
};
use crate::settings::{SessionSettings, SessionSettingsError, SettingId, SettingValue};

use super::ast::*;

#[derive(Parser)]
#[grammar = "query/query.pest"]
struct QueryParser;

pub fn parse_query(input: &str) -> Result<QueryFile> {
    parse_query_diagnostic(input).map_err(|e| CompilerError::Parse(e.to_string()))
}

/// Whether `input` opens with a settings statement: `set` or `reset` as the
/// first token after leading whitespace and comments, closed by a word
/// boundary as the grammar's keywords are. The gate a caller that needs only
/// the prefix takes before `parse_query`, so a source without one is never
/// parsed for it.
pub fn has_settings_prefix(input: &str) -> bool {
    let rest = skip_trivia(input);
    ["set", "reset"].iter().any(|keyword| {
        rest.strip_prefix(keyword).is_some_and(|after| {
            !after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
        })
    })
}

/// `input` after the grammar's `WHITESPACE` and `COMMENT`: an unterminated
/// block comment consumes the rest, as it does in the parser.
fn skip_trivia(mut input: &str) -> &str {
    loop {
        let trimmed = input.trim_start_matches([' ', '\t', '\r', '\n']);
        if let Some(rest) = trimmed.strip_prefix("//") {
            input = rest.split_once('\n').map_or("", |(_, tail)| tail);
        } else if let Some(rest) = trimmed.strip_prefix("/*") {
            input = rest.split_once("*/").map_or("", |(_, tail)| tail);
        } else {
            return trimmed;
        }
    }
}

pub fn parse_query_diagnostic(input: &str) -> std::result::Result<QueryFile, ParseDiagnostic> {
    let pairs = QueryParser::parse(Rule::query_file, input).map_err(pest_error_to_diagnostic)?;

    let mut settings = Vec::new();
    let mut queries = Vec::new();
    let mut statement = None;
    for pair in pairs {
        if let Rule::query_file = pair.as_rule() {
            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::setting_stmt => settings.push(parse_setting_stmt(inner)?),
                    Rule::branch_stmt => {
                        statement = Some(FileBody::Branch(parse_branch_stmt(inner)?));
                    }
                    Rule::show_stmt => {
                        statement = Some(FileBody::Show(parse_setting_target(inner)?));
                    }
                    Rule::explain_stmt => {
                        statement = Some(FileBody::Explain(parse_explain_stmt(inner)?));
                    }
                    Rule::statement_trailer => {
                        let subject = match statement {
                            Some(FileBody::Show(_)) => "a show statement",
                            Some(FileBody::Explain(_)) => "an `explain` statement",
                            _ => "a branch statement",
                        };
                        return Err(ParseDiagnostic::new(
                            format!("{subject} stands alone in its file"),
                            Some(pair_span(&inner)),
                        ));
                    }
                    Rule::query_decl => {
                        queries
                            .push(parse_query_decl(inner).map_err(compiler_error_to_diagnostic)?);
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(QueryFile {
        settings,
        body: statement.unwrap_or(FileBody::Queries(queries)),
    })
}

fn parse_explain_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<QueryDecl, ParseDiagnostic> {
    let decl = pair
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::query_decl)
        .expect("grammar: explain_stmt holds one query_decl after kw_explain");
    parse_query_decl(decl).map_err(compiler_error_to_diagnostic)
}

fn pair_span(pair: &pest::iterators::Pair<Rule>) -> SourceSpan {
    let span = pair.as_span();
    render_span(SourceSpan::new(span.start(), span.end()))
}

fn parse_setting_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<SettingStmt, ParseDiagnostic> {
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
fn parse_setting_target(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<Option<SettingId>, ParseDiagnostic> {
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
) -> std::result::Result<SettingId, ParseDiagnostic> {
    SettingId::parse(name.as_str()).map_err(|error| setting_diagnostic(error, name))
}

fn parse_setting_value(
    id: SettingId,
    pair: &pest::iterators::Pair<Rule>,
) -> std::result::Result<SettingValue, ParseDiagnostic> {
    let token = pair
        .clone()
        .into_inner()
        .next()
        .expect("grammar: setting_value wraps an integer, an ident or a string_lit");
    match token.as_rule() {
        Rule::integer => token
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
        Rule::string_lit => parse_string_lit(token.as_str())
            .map(SettingValue::Str)
            .map_err(compiler_error_to_diagnostic),
        other => unreachable!("grammar: setting_value admits no {other:?}"),
    }
}

fn setting_diagnostic(
    error: SessionSettingsError,
    at: &pest::iterators::Pair<Rule>,
) -> ParseDiagnostic {
    ParseDiagnostic::new(error.to_string(), Some(pair_span(at)))
}

fn parse_branch_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<BranchStmt, ParseDiagnostic> {
    let form = pair
        .into_inner()
        .find(|inner| inner.as_rule() != Rule::kw_branch)
        .expect("grammar: branch_stmt holds one form after kw_branch");
    let rule = form.as_rule();
    let names = form
        .into_inner()
        .filter(|inner| inner.as_rule() == Rule::branch_name)
        .map(parse_branch_name)
        .collect::<std::result::Result<Vec<_>, ParseDiagnostic>>()?;
    let mut names = names.into_iter();
    match (rule, names.next(), names.next()) {
        (Rule::branch_create, Some(name), from) => {
            Ok(BranchStmt::Write(BranchWrite::Create { name, from }))
        }
        (Rule::branch_delete, Some(name), None) => {
            Ok(BranchStmt::Write(BranchWrite::Delete { name }))
        }
        (Rule::branch_merge, Some(source), into) => {
            Ok(BranchStmt::Write(BranchWrite::Merge { source, into }))
        }
        (Rule::branch_list, None, None) => Ok(BranchStmt::List),
        (other, _, _) => {
            unreachable!("grammar: every branch form fixes its name arity, got {other:?}")
        }
    }
}

fn parse_branch_name(
    pair: pest::iterators::Pair<Rule>,
) -> std::result::Result<String, ParseDiagnostic> {
    let span = pair_span(&pair);
    let token = pair
        .into_inner()
        .next()
        .expect("grammar: branch_name wraps an ident or a string_lit");
    let name = match token.as_rule() {
        Rule::string_lit => {
            parse_string_lit(token.as_str()).map_err(compiler_error_to_diagnostic)?
        }
        Rule::ident => token.as_str().to_string(),
        other => unreachable!("grammar: branch_name admits no {other:?}"),
    };
    if name.chars().any(char::is_control) {
        return Err(ParseDiagnostic::new(
            format!("branch name {name:?} contains a control character"),
            Some(span),
        ));
    }
    if name.trim() != name {
        return Err(ParseDiagnostic::new(
            format!("branch name {name:?} has leading or trailing whitespace"),
            Some(span),
        ));
    }
    if name.is_empty() {
        return Err(ParseDiagnostic::new(
            format!("branch name {name:?} cannot be empty"),
            Some(span),
        ));
    }
    Ok(name)
}

fn pest_error_to_diagnostic(err: pest::error::Error<Rule>) -> ParseDiagnostic {
    let span = match err.location {
        InputLocation::Pos(pos) => Some(render_span(SourceSpan::new(pos, pos))),
        InputLocation::Span((start, end)) => Some(render_span(SourceSpan::new(start, end))),
    };
    ParseDiagnostic::new(err.to_string(), span)
}

fn compiler_error_to_diagnostic(err: CompilerError) -> ParseDiagnostic {
    ParseDiagnostic::new(err.to_string(), None)
}

fn parse_query_decl(pair: pest::iterators::Pair<Rule>) -> Result<QueryDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut description = None;
    let mut instruction = None;
    let mut params = Vec::new();
    let mut match_clause = Vec::new();
    let mut return_clause = Vec::new();
    let mut order_clause = Vec::new();
    let mut limit = None;
    let mut mutations = Vec::new();

    for item in inner {
        match item.as_rule() {
            Rule::param_list => {
                for p in item.into_inner() {
                    if let Rule::param = p.as_rule() {
                        params.push(parse_param(p)?);
                    }
                }
            }
            Rule::query_annotation => {
                let (annotation_name, value) = parse_query_annotation(item)?;
                match annotation_name {
                    "description" => {
                        if description.replace(value).is_some() {
                            return Err(CompilerError::Parse(format!(
                                "query `{}` cannot include duplicate @description annotations",
                                name
                            )));
                        }
                    }
                    "instruction" => {
                        if instruction.replace(value).is_some() {
                            return Err(CompilerError::Parse(format!(
                                "query `{}` cannot include duplicate @instruction annotations",
                                name
                            )));
                        }
                    }
                    other => {
                        return Err(CompilerError::Parse(format!(
                            "unsupported query annotation: @{}",
                            other
                        )));
                    }
                }
            }
            Rule::query_body => {
                let body = item.into_inner().next().ok_or_else(|| {
                    CompilerError::Parse("query body cannot be empty".to_string())
                })?;
                match body.as_rule() {
                    Rule::read_query_body => {
                        for section in body.into_inner() {
                            match section.as_rule() {
                                Rule::match_clause => {
                                    for c in section.into_inner() {
                                        if let Rule::clause = c.as_rule() {
                                            match_clause.push(parse_clause(c)?);
                                        }
                                    }
                                }
                                Rule::return_clause => {
                                    for proj in section.into_inner() {
                                        if let Rule::projection = proj.as_rule() {
                                            return_clause.push(parse_projection(proj)?);
                                        }
                                    }
                                }
                                Rule::order_clause => {
                                    for ord in section.into_inner() {
                                        if let Rule::ordering = ord.as_rule() {
                                            order_clause.push(parse_ordering(ord)?);
                                        }
                                    }
                                }
                                Rule::limit_clause => {
                                    let int_pair = section.into_inner().next().unwrap();
                                    limit =
                                        Some(int_pair.as_str().parse::<u64>().map_err(|e| {
                                            CompilerError::Parse(format!("invalid limit: {}", e))
                                        })?);
                                }
                                _ => {}
                            }
                        }
                    }
                    Rule::mutation_body => {
                        for mutation_pair in body.into_inner() {
                            if let Rule::mutation_stmt = mutation_pair.as_rule() {
                                let stmt = mutation_pair.into_inner().next().ok_or_else(|| {
                                    CompilerError::Parse(
                                        "mutation statement cannot be empty".to_string(),
                                    )
                                })?;
                                mutations.push(parse_mutation_stmt(stmt)?);
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    Ok(QueryDecl {
        name,
        description,
        instruction,
        params,
        match_clause,
        return_clause,
        order_clause,
        limit,
        mutations,
    })
}

fn parse_query_annotation(pair: pest::iterators::Pair<Rule>) -> Result<(&'static str, String)> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| CompilerError::Parse("query annotation cannot be empty".to_string()))?;
    match inner.as_rule() {
        Rule::description_annotation => {
            let value = inner
                .into_inner()
                .next()
                .ok_or_else(|| {
                    CompilerError::Parse("@description requires a string literal".to_string())
                })
                .map(|value| parse_string_lit(value.as_str()))??;
            Ok(("description", value))
        }
        Rule::instruction_annotation => {
            let value = inner
                .into_inner()
                .next()
                .ok_or_else(|| {
                    CompilerError::Parse("@instruction requires a string literal".to_string())
                })
                .map(|value| parse_string_lit(value.as_str()))??;
            Ok(("instruction", value))
        }
        other => Err(CompilerError::Parse(format!(
            "unexpected query annotation rule: {:?}",
            other
        ))),
    }
}

fn parse_param(pair: pest::iterators::Pair<Rule>) -> Result<Param> {
    let mut inner = pair.into_inner();
    let var = inner.next().unwrap().as_str();
    let name = var.strip_prefix('$').unwrap_or(var).to_string();
    let type_ref = inner.next().unwrap();
    let nullable = type_ref.as_str().trim_end().ends_with('?');
    let mut type_inner = type_ref.into_inner();
    let core = type_inner
        .next()
        .ok_or_else(|| CompilerError::Parse("parameter type is missing".to_string()))?;
    let base =
        match core.as_rule() {
            Rule::base_type => core.as_str().to_string(),
            Rule::list_type => {
                let inner = core.into_inner().next().ok_or_else(|| {
                    CompilerError::Parse("list type missing item type".to_string())
                })?;
                format!("[{}]", inner.as_str().trim())
            }
            Rule::vector_type => {
                let vector = core.into_inner().next().ok_or_else(|| {
                    CompilerError::Parse("Vector type missing dimension".to_string())
                })?;
                format!("Vector({})", vector.as_str().trim())
            }
            other => {
                return Err(CompilerError::Parse(format!(
                    "unexpected param type rule: {:?}",
                    other
                )));
            }
        };

    Ok(Param {
        name,
        type_name: base,
        nullable,
    })
}

fn parse_clause(pair: pest::iterators::Pair<Rule>) -> Result<Clause> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::binding => Ok(Clause::Binding(parse_binding(inner)?)),
        Rule::traversal => Ok(Clause::Traversal(parse_traversal(inner)?)),
        Rule::filter => Ok(Clause::Filter(parse_filter(inner)?)),
        Rule::text_search_clause => Ok(parse_text_search_clause(inner)?),
        Rule::negation => Ok(Clause::Subquery(Subquery::not_block(parse_block_clauses(
            inner,
        )?))),
        Rule::subquery_predicate => Ok(Clause::Subquery(parse_subquery_predicate(inner)?)),
        Rule::exists_block => {
            let block = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::subquery_block)
                .ok_or_else(|| CompilerError::Parse("exists block is empty".to_string()))?;
            Ok(Clause::Subquery(Subquery::exists_block(
                parse_block_clauses(block)?,
            )))
        }
        _ => Err(CompilerError::Parse(format!(
            "unexpected clause rule: {:?}",
            inner.as_rule()
        ))),
    }
}

/// The `clause` children of a braced block (`not { … }`, `count { … }`).
fn parse_block_clauses(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Clause>> {
    let mut clauses = Vec::new();
    for c in pair.into_inner() {
        if let Rule::clause = c.as_rule() {
            clauses.push(parse_clause(c)?);
        }
    }
    Ok(clauses)
}

fn parse_subquery_predicate(pair: pest::iterators::Pair<Rule>) -> Result<Subquery> {
    let mut func = None;
    let mut arg = None;
    let mut clauses = None;
    let mut op = None;
    let mut right = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::agg_func => func = Some(parse_agg_func(part.as_str())?),
            Rule::expr if clauses.is_none() => arg = Some(parse_expr(part)?),
            Rule::expr => right = Some(parse_expr(part)?),
            Rule::subquery_block => clauses = Some(parse_block_clauses(part)?),
            Rule::comp_op => op = Some(parse_comp_op(part)?),
            other => {
                return Err(CompilerError::Parse(format!(
                    "unexpected subquery predicate rule: {:?}",
                    other
                )));
            }
        }
    }
    let missing =
        |what: &str| CompilerError::Parse(format!("subquery predicate is missing its {what}"));
    Ok(Subquery {
        keyword: BlockKeyword::Aggregate,
        clauses: clauses.ok_or_else(|| missing("block"))?,
        func: func.ok_or_else(|| missing("aggregate"))?,
        arg,
        op: op.ok_or_else(|| missing("comparison"))?,
        right: right.ok_or_else(|| missing("right operand"))?,
    })
}

fn parse_agg_func(name: &str) -> Result<AggFunc> {
    match name {
        "count" => Ok(AggFunc::Count),
        "sum" => Ok(AggFunc::Sum),
        "avg" => Ok(AggFunc::Avg),
        "min" => Ok(AggFunc::Min),
        "max" => Ok(AggFunc::Max),
        other => Err(CompilerError::Parse(format!(
            "unknown aggregate: {}",
            other
        ))),
    }
}

fn parse_text_search_clause(pair: pest::iterators::Pair<Rule>) -> Result<Clause> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| CompilerError::Parse("text search clause cannot be empty".to_string()))?;
    let expr = match inner.as_rule() {
        Rule::search_call => parse_search_call(inner)?,
        Rule::fuzzy_call => parse_fuzzy_call(inner)?,
        Rule::match_text_call => parse_match_text_call(inner)?,
        other => {
            return Err(CompilerError::Parse(format!(
                "unexpected text search clause rule: {:?}",
                other
            )));
        }
    };

    Ok(Clause::Filter(Filter {
        left: expr,
        op: CompOp::Eq,
        right: Expr::Literal(Literal::Bool(true)),
    }))
}

fn parse_binding(pair: pest::iterators::Pair<Rule>) -> Result<Binding> {
    let mut inner = pair.into_inner();
    let var = inner.next().unwrap().as_str();
    let variable = var.strip_prefix('$').unwrap_or(var).to_string();
    let type_name = inner.next().unwrap().as_str().to_string();

    let mut prop_matches = Vec::new();
    for item in inner {
        if let Rule::prop_match_list = item.as_rule() {
            for pm in item.into_inner() {
                if let Rule::prop_match = pm.as_rule() {
                    prop_matches.push(parse_prop_match(pm)?);
                }
            }
        }
    }

    Ok(Binding {
        variable,
        type_name,
        prop_matches,
    })
}

fn parse_prop_match(pair: pest::iterators::Pair<Rule>) -> Result<PropMatch> {
    let mut inner = pair.into_inner();
    let prop_name = inner.next().unwrap().as_str().to_string();
    let value_pair = inner.next().unwrap();
    let value = parse_match_value(value_pair)?;

    Ok(PropMatch { prop_name, value })
}

fn parse_mutation_stmt(pair: pest::iterators::Pair<Rule>) -> Result<Mutation> {
    match pair.as_rule() {
        Rule::insert_stmt => parse_insert_mutation(pair).map(Mutation::Insert),
        Rule::update_stmt => parse_update_mutation(pair).map(Mutation::Update),
        Rule::delete_stmt => parse_delete_mutation(pair).map(Mutation::Delete),
        other => Err(CompilerError::Parse(format!(
            "unexpected mutation statement rule: {:?}",
            other
        ))),
    }
}

fn parse_insert_mutation(pair: pest::iterators::Pair<Rule>) -> Result<InsertMutation> {
    let mut inner = pair.into_inner();
    let type_name = inner.next().unwrap().as_str().to_string();
    let mut assignments = Vec::new();
    for item in inner {
        if let Rule::mutation_assignment = item.as_rule() {
            assignments.push(parse_mutation_assignment(item)?);
        }
    }
    Ok(InsertMutation {
        type_name,
        assignments,
    })
}

fn parse_update_mutation(pair: pest::iterators::Pair<Rule>) -> Result<UpdateMutation> {
    let mut inner = pair.into_inner();
    let type_name = inner.next().unwrap().as_str().to_string();

    let mut assignments = Vec::new();
    let mut predicate = None;

    for item in inner {
        match item.as_rule() {
            Rule::mutation_assignment => assignments.push(parse_mutation_assignment(item)?),
            Rule::mutation_predicate => predicate = Some(parse_mutation_predicate(item)?),
            _ => {}
        }
    }

    let predicate = predicate.ok_or_else(|| {
        CompilerError::Parse("update mutation requires a where predicate".to_string())
    })?;

    Ok(UpdateMutation {
        type_name,
        assignments,
        predicate,
    })
}

fn parse_delete_mutation(pair: pest::iterators::Pair<Rule>) -> Result<DeleteMutation> {
    let mut inner = pair.into_inner();
    let type_name = inner.next().unwrap().as_str().to_string();
    let predicate = inner
        .next()
        .ok_or_else(|| {
            CompilerError::Parse("delete mutation requires a where predicate".to_string())
        })
        .and_then(parse_mutation_predicate)?;
    Ok(DeleteMutation {
        type_name,
        predicate,
    })
}

fn parse_mutation_assignment(pair: pest::iterators::Pair<Rule>) -> Result<MutationAssignment> {
    let mut inner = pair.into_inner();
    let property = inner.next().unwrap().as_str().to_string();
    let value = parse_match_value(inner.next().unwrap())?;
    Ok(MutationAssignment { property, value })
}

fn parse_mutation_predicate(pair: pest::iterators::Pair<Rule>) -> Result<MutationPredicate> {
    let mut inner = pair.into_inner();
    let property = inner.next().unwrap().as_str().to_string();
    let op = parse_comp_op(inner.next().unwrap())?;
    let value = parse_match_value(inner.next().unwrap())?;
    Ok(MutationPredicate {
        property,
        op,
        value,
    })
}

fn parse_match_value(pair: pest::iterators::Pair<Rule>) -> Result<MatchValue> {
    let value_inner = pair.into_inner().next().unwrap();
    match value_inner.as_rule() {
        Rule::variable => {
            let v = value_inner.as_str();
            Ok(MatchValue::Variable(
                v.strip_prefix('$').unwrap_or(v).to_string(),
            ))
        }
        Rule::now_call => Ok(MatchValue::Now),
        Rule::literal => Ok(MatchValue::Literal(parse_literal(value_inner)?)),
        _ => Err(CompilerError::Parse(format!(
            "unexpected match value: {:?}",
            value_inner.as_rule()
        ))),
    }
}

fn parse_traversal(pair: pest::iterators::Pair<Rule>) -> Result<Traversal> {
    let mut inner = pair.into_inner();
    let src_var = inner.next().unwrap().as_str();
    let src = src_var.strip_prefix('$').unwrap_or(src_var).to_string();
    let mut next = inner.next().unwrap();
    let edge_binding = if let Rule::edge_binding = next.as_rule() {
        let var = next.into_inner().next().unwrap().as_str();
        let binding = var.strip_prefix('$').unwrap_or(var).to_string();
        next = inner.next().unwrap();
        Some(binding)
    } else {
        None
    };
    let edge_pair = next;
    let (edge_name, undirected) = match edge_pair.as_rule() {
        // `<edge>` — the inner edge_ident carries the name.
        Rule::undirected_edge => (
            edge_pair.into_inner().next().unwrap().as_str().to_string(),
            true,
        ),
        _ => (edge_pair.as_str().to_string(), false),
    };
    let mut min_hops = 1u32;
    let mut max_hops = Some(1u32);

    let next = inner.next().unwrap();
    let dst_pair = if let Rule::traversal_bounds = next.as_rule() {
        let (min, max) = parse_traversal_bounds(next)?;
        min_hops = min;
        max_hops = max;
        inner.next().ok_or_else(|| {
            CompilerError::Parse("traversal missing destination variable".to_string())
        })?
    } else {
        next
    };

    let dst_var = dst_pair.as_str();
    let dst = dst_var.strip_prefix('$').unwrap_or(dst_var).to_string();

    Ok(Traversal {
        src,
        edge_name,
        dst,
        min_hops,
        max_hops,
        undirected,
        edge_binding,
    })
}

fn parse_traversal_bounds(pair: pest::iterators::Pair<Rule>) -> Result<(u32, Option<u32>)> {
    let mut inner = pair.into_inner();
    let min = inner
        .next()
        .ok_or_else(|| CompilerError::Parse("traversal bound missing min hop".to_string()))?
        .as_str()
        .parse::<u32>()
        .map_err(|e| CompilerError::Parse(format!("invalid traversal min bound: {}", e)))?;
    let max = inner
        .next()
        .map(|p| {
            p.as_str()
                .parse::<u32>()
                .map_err(|e| CompilerError::Parse(format!("invalid traversal max bound: {}", e)))
        })
        .transpose()?;
    Ok((min, max))
}

fn parse_filter(pair: pest::iterators::Pair<Rule>) -> Result<Filter> {
    let mut inner = pair.into_inner();
    let left = parse_expr(inner.next().unwrap())?;
    let op = parse_filter_op(inner.next().unwrap())?;
    let right = parse_expr(inner.next().unwrap())?;

    Ok(Filter { left, op, right })
}

fn parse_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::now_call => Ok(Expr::Now),
        Rule::prop_access => {
            let mut parts = inner.into_inner();
            let var = parts.next().unwrap().as_str();
            let variable = var.strip_prefix('$').unwrap_or(var).to_string();
            let property = parts.next().unwrap().as_str().to_string();
            Ok(Expr::PropAccess { variable, property })
        }
        Rule::variable => {
            let v = inner.as_str();
            Ok(Expr::Variable(v.strip_prefix('$').unwrap_or(v).to_string()))
        }
        Rule::literal => Ok(Expr::Literal(parse_literal(inner)?)),
        Rule::agg_call => {
            let mut parts = inner.into_inner();
            let func = parse_agg_func(parts.next().unwrap().as_str())?;
            let arg = parse_expr(parts.next().unwrap())?;
            Ok(Expr::Aggregate {
                func,
                arg: Box::new(arg),
            })
        }
        Rule::search_call => parse_search_call(inner),
        Rule::fuzzy_call => parse_fuzzy_call(inner),
        Rule::match_text_call => parse_match_text_call(inner),
        Rule::nearest_ordering => parse_nearest_ordering(inner),
        Rule::bm25_call => parse_bm25_call(inner),
        Rule::rrf_call => parse_rrf_call(inner),
        Rule::ident => Ok(Expr::AliasRef(inner.as_str().to_string())),
        _ => Err(CompilerError::Parse(format!(
            "unexpected expr rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_search_call(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let mut args = pair.into_inner();
    let field = args
        .next()
        .ok_or_else(|| CompilerError::Parse("search() missing field argument".to_string()))?;
    let query = args
        .next()
        .ok_or_else(|| CompilerError::Parse("search() missing query argument".to_string()))?;
    if args.next().is_some() {
        return Err(CompilerError::Parse(
            "search() accepts exactly 2 arguments".to_string(),
        ));
    }
    Ok(Expr::Search {
        field: Box::new(parse_expr(field)?),
        query: Box::new(parse_expr(query)?),
    })
}

fn parse_fuzzy_call(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let mut args = pair.into_inner();
    let field = args
        .next()
        .ok_or_else(|| CompilerError::Parse("fuzzy() missing field argument".to_string()))?;
    let query = args
        .next()
        .ok_or_else(|| CompilerError::Parse("fuzzy() missing query argument".to_string()))?;
    let max_edits = args.next().map(parse_expr).transpose()?.map(Box::new);
    if args.next().is_some() {
        return Err(CompilerError::Parse(
            "fuzzy() accepts at most 3 arguments".to_string(),
        ));
    }
    Ok(Expr::Fuzzy {
        field: Box::new(parse_expr(field)?),
        query: Box::new(parse_expr(query)?),
        max_edits,
    })
}

fn parse_match_text_call(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let mut args = pair.into_inner();
    let field = args
        .next()
        .ok_or_else(|| CompilerError::Parse("match_text() missing field argument".to_string()))?;
    let query = args
        .next()
        .ok_or_else(|| CompilerError::Parse("match_text() missing query argument".to_string()))?;
    if args.next().is_some() {
        return Err(CompilerError::Parse(
            "match_text() accepts exactly 2 arguments".to_string(),
        ));
    }
    Ok(Expr::MatchText {
        field: Box::new(parse_expr(field)?),
        query: Box::new(parse_expr(query)?),
    })
}

fn parse_bm25_call(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let mut args = pair.into_inner();
    let field = args
        .next()
        .ok_or_else(|| CompilerError::Parse("bm25() missing field argument".to_string()))?;
    let query = args
        .next()
        .ok_or_else(|| CompilerError::Parse("bm25() missing query argument".to_string()))?;
    if args.next().is_some() {
        return Err(CompilerError::Parse(
            "bm25() accepts exactly 2 arguments".to_string(),
        ));
    }
    Ok(Expr::Bm25 {
        field: Box::new(parse_expr(field)?),
        query: Box::new(parse_expr(query)?),
    })
}

fn parse_rank_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let inner = if pair.as_rule() == Rule::rank_expr {
        pair.into_inner()
            .next()
            .ok_or_else(|| CompilerError::Parse("rank expression cannot be empty".to_string()))?
    } else {
        pair
    };
    match inner.as_rule() {
        Rule::nearest_ordering => parse_nearest_ordering(inner),
        Rule::bm25_call => parse_bm25_call(inner),
        other => Err(CompilerError::Parse(format!(
            "rrf() rank expression must be nearest(...) or bm25(...), got {:?}",
            other
        ))),
    }
}

fn parse_rrf_call(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let mut args = pair.into_inner();
    let primary = args
        .next()
        .ok_or_else(|| CompilerError::Parse("rrf() missing primary rank expression".to_string()))?;
    let secondary = args.next().ok_or_else(|| {
        CompilerError::Parse("rrf() missing secondary rank expression".to_string())
    })?;
    let k = args.next().map(parse_expr).transpose()?.map(Box::new);
    if args.next().is_some() {
        return Err(CompilerError::Parse(
            "rrf() accepts at most 3 arguments".to_string(),
        ));
    }
    Ok(Expr::Rrf {
        primary: Box::new(parse_rank_expr(primary)?),
        secondary: Box::new(parse_rank_expr(secondary)?),
        k,
    })
}

fn parse_comp_op(pair: pest::iterators::Pair<Rule>) -> Result<CompOp> {
    match pair.as_str() {
        "=" => Ok(CompOp::Eq),
        "!=" => Ok(CompOp::Ne),
        ">" => Ok(CompOp::Gt),
        "<" => Ok(CompOp::Lt),
        ">=" => Ok(CompOp::Ge),
        "<=" => Ok(CompOp::Le),
        other => Err(CompilerError::Parse(format!("unknown operator: {}", other))),
    }
}

fn parse_filter_op(pair: pest::iterators::Pair<Rule>) -> Result<CompOp> {
    match pair.as_str() {
        "contains" => Ok(CompOp::Contains),
        "starts_with" => Ok(CompOp::StartsWith),
        _ => parse_comp_op(pair),
    }
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> Result<Literal> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::string_lit => Ok(Literal::String(parse_string_lit(inner.as_str())?)),
        Rule::integer => {
            let n: i64 = inner
                .as_str()
                .parse()
                .map_err(|e| CompilerError::Parse(format!("invalid integer: {}", e)))?;
            Ok(Literal::Integer(n))
        }
        Rule::float_lit => {
            let f: f64 = inner
                .as_str()
                .parse()
                .map_err(|e| CompilerError::Parse(format!("invalid float: {}", e)))?;
            Ok(Literal::Float(f))
        }
        Rule::bool_lit => {
            let b = match inner.as_str() {
                "true" => true,
                "false" => false,
                other => {
                    return Err(CompilerError::Parse(format!(
                        "invalid boolean literal: {}",
                        other
                    )));
                }
            };
            Ok(Literal::Bool(b))
        }
        Rule::date_lit => {
            let date_str = inner
                .into_inner()
                .next()
                .map(|s| parse_string_lit(s.as_str()))
                .ok_or_else(|| {
                    CompilerError::Parse("date literal requires a string".to_string())
                })?;
            Ok(Literal::Date(date_str?))
        }
        Rule::datetime_lit => {
            let dt_str = inner
                .into_inner()
                .next()
                .map(|s| parse_string_lit(s.as_str()))
                .ok_or_else(|| {
                    CompilerError::Parse("datetime literal requires a string".to_string())
                })?;
            Ok(Literal::DateTime(dt_str?))
        }
        Rule::list_lit => {
            let mut items = Vec::new();
            for item in inner.into_inner() {
                if item.as_rule() == Rule::literal {
                    items.push(parse_literal(item)?);
                }
            }
            Ok(Literal::List(items))
        }
        _ => Err(CompilerError::Parse(format!(
            "unexpected literal: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_string_lit(raw: &str) -> Result<String> {
    decode_string_literal(raw)
}

fn parse_projection(pair: pest::iterators::Pair<Rule>) -> Result<Projection> {
    let mut inner = pair.into_inner();
    let expr = parse_expr(inner.next().unwrap())?;
    let alias = inner.next().map(|p| p.as_str().to_string());

    Ok(Projection { expr, alias })
}

fn parse_ordering(pair: pest::iterators::Pair<Rule>) -> Result<Ordering> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| CompilerError::Parse("ordering cannot be empty".to_string()))?;
    let (expr, descending) = match first.as_rule() {
        Rule::nearest_ordering => (parse_nearest_ordering(first)?, false),
        Rule::expr => {
            let expr = parse_expr(first)?;
            let direction = inner.next().map(|p| p.as_str().to_string());
            if matches!(expr, Expr::Nearest { .. }) && direction.is_some() {
                return Err(CompilerError::Parse(
                    "nearest() ordering does not accept asc/desc modifiers".to_string(),
                ));
            }
            let descending = matches!(direction.as_deref(), Some("desc"));
            (expr, descending)
        }
        other => {
            return Err(CompilerError::Parse(format!(
                "unexpected ordering rule: {:?}",
                other
            )));
        }
    };

    Ok(Ordering { expr, descending })
}

fn parse_nearest_ordering(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let prop = inner
        .next()
        .ok_or_else(|| CompilerError::Parse("nearest() missing property".to_string()))?;
    let mut prop_parts = prop.into_inner();
    let var = prop_parts
        .next()
        .ok_or_else(|| CompilerError::Parse("nearest() missing variable".to_string()))?
        .as_str();
    let variable = var.strip_prefix('$').unwrap_or(var).to_string();
    let property = prop_parts
        .next()
        .ok_or_else(|| CompilerError::Parse("nearest() missing property name".to_string()))?
        .as_str()
        .to_string();

    let query = inner
        .next()
        .ok_or_else(|| CompilerError::Parse("nearest() missing query expression".to_string()))?;
    Ok(Expr::Nearest {
        variable,
        property,
        query: Box::new(parse_expr(query)?),
    })
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod tests;
