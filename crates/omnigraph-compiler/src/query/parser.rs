use pest::Parser;
use pest::error::InputLocation;
use pest_derive::Parser;

use crate::error::{
    NanoError, ParseDiagnostic, Result, SourceSpan, decode_string_literal, render_span,
};

use super::ast::*;

#[derive(Parser)]
#[grammar = "query/query.pest"]
struct QueryParser;

pub fn parse_query(input: &str) -> Result<QueryFile> {
    parse_query_diagnostic(input).map_err(|e| NanoError::Parse(e.to_string()))
}

pub fn parse_query_diagnostic(input: &str) -> std::result::Result<QueryFile, ParseDiagnostic> {
    let pairs = QueryParser::parse(Rule::query_file, input).map_err(pest_error_to_diagnostic)?;

    let mut queries = Vec::new();
    for pair in pairs {
        if let Rule::query_file = pair.as_rule() {
            for inner in pair.into_inner() {
                if let Rule::query_decl = inner.as_rule() {
                    queries.push(parse_query_decl(inner).map_err(nano_error_to_diagnostic)?);
                }
            }
        }
    }
    Ok(QueryFile { queries })
}

fn pest_error_to_diagnostic(err: pest::error::Error<Rule>) -> ParseDiagnostic {
    let span = match err.location {
        InputLocation::Pos(pos) => Some(render_span(SourceSpan::new(pos, pos))),
        InputLocation::Span((start, end)) => Some(render_span(SourceSpan::new(start, end))),
    };
    ParseDiagnostic::new(err.to_string(), span)
}

fn nano_error_to_diagnostic(err: NanoError) -> ParseDiagnostic {
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
                            return Err(NanoError::Parse(format!(
                                "query `{}` cannot include duplicate @description annotations",
                                name
                            )));
                        }
                    }
                    "instruction" => {
                        if instruction.replace(value).is_some() {
                            return Err(NanoError::Parse(format!(
                                "query `{}` cannot include duplicate @instruction annotations",
                                name
                            )));
                        }
                    }
                    other => {
                        return Err(NanoError::Parse(format!(
                            "unsupported query annotation: @{}",
                            other
                        )));
                    }
                }
            }
            Rule::query_body => {
                let body = item
                    .into_inner()
                    .next()
                    .ok_or_else(|| NanoError::Parse("query body cannot be empty".to_string()))?;
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
                                            NanoError::Parse(format!("invalid limit: {}", e))
                                        })?);
                                }
                                _ => {}
                            }
                        }
                    }
                    Rule::mutation_body => {
                        for mutation_pair in body.into_inner() {
                            if let Rule::mutation_stmt = mutation_pair.as_rule() {
                                let stmt =
                                    mutation_pair.into_inner().next().ok_or_else(|| {
                                        NanoError::Parse(
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
        .ok_or_else(|| NanoError::Parse("query annotation cannot be empty".to_string()))?;
    match inner.as_rule() {
        Rule::description_annotation => {
            let value = inner
                .into_inner()
                .next()
                .ok_or_else(|| {
                    NanoError::Parse("@description requires a string literal".to_string())
                })
                .map(|value| parse_string_lit(value.as_str()))??;
            Ok(("description", value))
        }
        Rule::instruction_annotation => {
            let value = inner
                .into_inner()
                .next()
                .ok_or_else(|| {
                    NanoError::Parse("@instruction requires a string literal".to_string())
                })
                .map(|value| parse_string_lit(value.as_str()))??;
            Ok(("instruction", value))
        }
        other => Err(NanoError::Parse(format!(
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
        .ok_or_else(|| NanoError::Parse("parameter type is missing".to_string()))?;
    let base = match core.as_rule() {
        Rule::base_type => core.as_str().to_string(),
        Rule::list_type => {
            let inner = core
                .into_inner()
                .next()
                .ok_or_else(|| NanoError::Parse("list type missing item type".to_string()))?;
            format!("[{}]", inner.as_str().trim())
        }
        Rule::vector_type => {
            let vector = core
                .into_inner()
                .next()
                .ok_or_else(|| NanoError::Parse("Vector type missing dimension".to_string()))?;
            format!("Vector({})", vector.as_str().trim())
        }
        other => {
            return Err(NanoError::Parse(format!(
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
        Rule::negation => {
            let mut clauses = Vec::new();
            for c in inner.into_inner() {
                if let Rule::clause = c.as_rule() {
                    clauses.push(parse_clause(c)?);
                }
            }
            Ok(Clause::Negation(clauses))
        }
        _ => Err(NanoError::Parse(format!(
            "unexpected clause rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_text_search_clause(pair: pest::iterators::Pair<Rule>) -> Result<Clause> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| NanoError::Parse("text search clause cannot be empty".to_string()))?;
    let expr = match inner.as_rule() {
        Rule::search_call => parse_search_call(inner)?,
        Rule::fuzzy_call => parse_fuzzy_call(inner)?,
        Rule::match_text_call => parse_match_text_call(inner)?,
        other => {
            return Err(NanoError::Parse(format!(
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
        other => Err(NanoError::Parse(format!(
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
        NanoError::Parse("update mutation requires a where predicate".to_string())
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
        .ok_or_else(|| NanoError::Parse("delete mutation requires a where predicate".to_string()))
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
        _ => Err(NanoError::Parse(format!(
            "unexpected match value: {:?}",
            value_inner.as_rule()
        ))),
    }
}

fn parse_traversal(pair: pest::iterators::Pair<Rule>) -> Result<Traversal> {
    let mut inner = pair.into_inner();
    let src_var = inner.next().unwrap().as_str();
    let src = src_var.strip_prefix('$').unwrap_or(src_var).to_string();
    let edge_name = inner.next().unwrap().as_str().to_string();
    let mut min_hops = 1u32;
    let mut max_hops = Some(1u32);

    let next = inner.next().unwrap();
    let dst_pair = if let Rule::traversal_bounds = next.as_rule() {
        let (min, max) = parse_traversal_bounds(next)?;
        min_hops = min;
        max_hops = max;
        inner
            .next()
            .ok_or_else(|| NanoError::Parse("traversal missing destination variable".to_string()))?
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
    })
}

fn parse_traversal_bounds(pair: pest::iterators::Pair<Rule>) -> Result<(u32, Option<u32>)> {
    let mut inner = pair.into_inner();
    let min = inner
        .next()
        .ok_or_else(|| NanoError::Parse("traversal bound missing min hop".to_string()))?
        .as_str()
        .parse::<u32>()
        .map_err(|e| NanoError::Parse(format!("invalid traversal min bound: {}", e)))?;
    let max = inner
        .next()
        .map(|p| {
            p.as_str()
                .parse::<u32>()
                .map_err(|e| NanoError::Parse(format!("invalid traversal max bound: {}", e)))
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
            let func = match parts.next().unwrap().as_str() {
                "count" => AggFunc::Count,
                "sum" => AggFunc::Sum,
                "avg" => AggFunc::Avg,
                "min" => AggFunc::Min,
                "max" => AggFunc::Max,
                other => return Err(NanoError::Parse(format!("unknown aggregate: {}", other))),
            };
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
        _ => Err(NanoError::Parse(format!(
            "unexpected expr rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_search_call(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let mut args = pair.into_inner();
    let field = args
        .next()
        .ok_or_else(|| NanoError::Parse("search() missing field argument".to_string()))?;
    let query = args
        .next()
        .ok_or_else(|| NanoError::Parse("search() missing query argument".to_string()))?;
    if args.next().is_some() {
        return Err(NanoError::Parse(
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
        .ok_or_else(|| NanoError::Parse("fuzzy() missing field argument".to_string()))?;
    let query = args
        .next()
        .ok_or_else(|| NanoError::Parse("fuzzy() missing query argument".to_string()))?;
    let max_edits = args.next().map(parse_expr).transpose()?.map(Box::new);
    if args.next().is_some() {
        return Err(NanoError::Parse(
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
        .ok_or_else(|| NanoError::Parse("match_text() missing field argument".to_string()))?;
    let query = args
        .next()
        .ok_or_else(|| NanoError::Parse("match_text() missing query argument".to_string()))?;
    if args.next().is_some() {
        return Err(NanoError::Parse(
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
        .ok_or_else(|| NanoError::Parse("bm25() missing field argument".to_string()))?;
    let query = args
        .next()
        .ok_or_else(|| NanoError::Parse("bm25() missing query argument".to_string()))?;
    if args.next().is_some() {
        return Err(NanoError::Parse(
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
            .ok_or_else(|| NanoError::Parse("rank expression cannot be empty".to_string()))?
    } else {
        pair
    };
    match inner.as_rule() {
        Rule::nearest_ordering => parse_nearest_ordering(inner),
        Rule::bm25_call => parse_bm25_call(inner),
        other => Err(NanoError::Parse(format!(
            "rrf() rank expression must be nearest(...) or bm25(...), got {:?}",
            other
        ))),
    }
}

fn parse_rrf_call(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let mut args = pair.into_inner();
    let primary = args
        .next()
        .ok_or_else(|| NanoError::Parse("rrf() missing primary rank expression".to_string()))?;
    let secondary = args
        .next()
        .ok_or_else(|| NanoError::Parse("rrf() missing secondary rank expression".to_string()))?;
    let k = args.next().map(parse_expr).transpose()?.map(Box::new);
    if args.next().is_some() {
        return Err(NanoError::Parse(
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
        other => Err(NanoError::Parse(format!("unknown operator: {}", other))),
    }
}

fn parse_filter_op(pair: pest::iterators::Pair<Rule>) -> Result<CompOp> {
    match pair.as_str() {
        "contains" => Ok(CompOp::Contains),
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
                .map_err(|e| NanoError::Parse(format!("invalid integer: {}", e)))?;
            Ok(Literal::Integer(n))
        }
        Rule::float_lit => {
            let f: f64 = inner
                .as_str()
                .parse()
                .map_err(|e| NanoError::Parse(format!("invalid float: {}", e)))?;
            Ok(Literal::Float(f))
        }
        Rule::bool_lit => {
            let b = match inner.as_str() {
                "true" => true,
                "false" => false,
                other => {
                    return Err(NanoError::Parse(format!(
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
                .ok_or_else(|| NanoError::Parse("date literal requires a string".to_string()))?;
            Ok(Literal::Date(date_str?))
        }
        Rule::datetime_lit => {
            let dt_str = inner
                .into_inner()
                .next()
                .map(|s| parse_string_lit(s.as_str()))
                .ok_or_else(|| {
                    NanoError::Parse("datetime literal requires a string".to_string())
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
        _ => Err(NanoError::Parse(format!(
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
        .ok_or_else(|| NanoError::Parse("ordering cannot be empty".to_string()))?;
    let (expr, descending) = match first.as_rule() {
        Rule::nearest_ordering => (parse_nearest_ordering(first)?, false),
        Rule::expr => {
            let expr = parse_expr(first)?;
            let direction = inner.next().map(|p| p.as_str().to_string());
            if matches!(expr, Expr::Nearest { .. }) && direction.is_some() {
                return Err(NanoError::Parse(
                    "nearest() ordering does not accept asc/desc modifiers".to_string(),
                ));
            }
            let descending = matches!(direction.as_deref(), Some("desc"));
            (expr, descending)
        }
        other => {
            return Err(NanoError::Parse(format!(
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
        .ok_or_else(|| NanoError::Parse("nearest() missing property".to_string()))?;
    let mut prop_parts = prop.into_inner();
    let var = prop_parts
        .next()
        .ok_or_else(|| NanoError::Parse("nearest() missing variable".to_string()))?
        .as_str();
    let variable = var.strip_prefix('$').unwrap_or(var).to_string();
    let property = prop_parts
        .next()
        .ok_or_else(|| NanoError::Parse("nearest() missing property name".to_string()))?
        .as_str()
        .to_string();

    let query = inner
        .next()
        .ok_or_else(|| NanoError::Parse("nearest() missing query expression".to_string()))?;
    Ok(Expr::Nearest {
        variable,
        property,
        query: Box::new(parse_expr(query)?),
    })
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod tests;
