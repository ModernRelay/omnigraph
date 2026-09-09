//! RFC 0048 syntax/scope experiment, not a second production compiler.
//! Reuses the real graph parser and typechecker. Prefix queries below are
//! typed AST values used only for scope checks; execution stages stay separate.
//! Representation certification, parameter admission, physical lowering and
//! per-group syntax are not implemented by this experiment.

use std::collections::{BTreeMap, BTreeSet};

use pest::iterators::Pair;

use super::*;
use crate::catalog::{Catalog, build_catalog};
use crate::query::typecheck::{BoundVariable, ResolvedType, TypeContext, typecheck_query};
use crate::schema::parser::parse_schema;
use crate::types::{PropType, ScalarType};

type Options = BTreeMap<String, Expr>;
type ProbeResult<T> = std::result::Result<T, String>;

#[derive(Debug, Clone)]
struct Terms {
    text: Expr,
    options: Options,
}

#[derive(Debug, Clone)]
enum MatchItem {
    Graph(Clause),
    Terms { field: Expr, query: Terms },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    Lexical,
    Knn,
    Ann,
    Rrf,
}

#[derive(Debug, Clone)]
enum Source {
    Lexical {
        field: Expr,
        query: Terms,
    },
    Vector {
        kind: SourceKind,
        field: Expr,
        query: Expr,
    },
    Fusion(Vec<(String, Options)>),
}

#[derive(Debug, Clone)]
struct Declaration {
    alias: String,
    source: Source,
    options: Options,
}

#[derive(Debug, Clone)]
enum Stage {
    Match(Vec<MatchItem>),
    Rank {
        target: String,
        declarations: Vec<Declaration>,
    },
}

#[derive(Debug, Clone)]
enum Value {
    Core(Expr),
    Identity(String),
    Metric { source: String, field: String },
    Aggregate { function: String, value: Box<Value> },
}

#[derive(Debug)]
struct Query {
    header: QueryDecl,
    stages: Vec<Stage>,
    projections: Vec<(Value, Option<String>)>,
    order: Vec<(Value, bool)>,
}

fn core(pair: Pair<Rule>) -> ProbeResult<Expr> {
    parse_expr(pair).map_err(|e| e.to_string())
}

fn options<'i>(pairs: impl Iterator<Item = Pair<'i, Rule>>) -> ProbeResult<Options> {
    let mut options = Options::new();
    for pair in pairs {
        let mut parts = pair.into_inner();
        let name = parts.next().unwrap().as_str().to_string();
        let value = core(parts.next().unwrap())?;
        if options.insert(name.clone(), value).is_some() {
            return Err(format!("duplicate option `{name}`"));
        }
    }
    Ok(options)
}

fn terms(pair: Pair<Rule>) -> ProbeResult<Terms> {
    let mut parts = pair.into_inner();
    Ok(Terms {
        text: core(parts.next().unwrap())?,
        options: options(parts)?,
    })
}

fn declaration(pair: Pair<Rule>) -> ProbeResult<Declaration> {
    let mut parts = pair.into_inner();
    let call = parts.next().unwrap();
    let alias = parts.next().unwrap().as_str().to_string();
    let rule = call.as_rule();
    let mut args = call.into_inner();
    let source = match rule {
        Rule::probe_lexical => Source::Lexical {
            field: core(args.next().unwrap())?,
            query: terms(args.next().unwrap())?,
        },
        Rule::probe_vector => Source::Vector {
            kind: match args.next().unwrap().as_str() {
                "knn" => SourceKind::Knn,
                _ => SourceKind::Ann,
            },
            field: core(args.next().unwrap())?,
            query: core(args.next().unwrap())?,
        },
        Rule::probe_fusion => {
            let mut arms = Vec::new();
            while args.peek().is_some_and(|p| p.as_rule() == Rule::probe_arm) {
                let mut arm = args.next().unwrap().into_inner();
                arms.push((arm.next().unwrap().as_str().to_string(), options(arm)?));
            }
            Source::Fusion(arms)
        }
        _ => unreachable!(),
    };
    Ok(Declaration {
        alias,
        source,
        options: options(args)?,
    })
}

fn value(pair: Pair<Rule>) -> ProbeResult<Value> {
    let item = pair.into_inner().next().unwrap();
    match item.as_rule() {
        Rule::expr => Ok(Value::Core(core(item)?)),
        Rule::probe_identity => Ok(Value::Identity(
            item.into_inner().next().unwrap().as_str()[1..].into(),
        )),
        Rule::probe_metric => {
            let mut parts = item.into_inner();
            Ok(Value::Metric {
                source: parts.next().unwrap().as_str().into(),
                field: parts.next().unwrap().as_str().into(),
            })
        }
        Rule::probe_aggregate => {
            let mut parts = item.into_inner();
            Ok(Value::Aggregate {
                function: parts.next().unwrap().as_str().into(),
                value: Box::new(value(parts.next().unwrap())?),
            })
        }
        _ => unreachable!(),
    }
}

fn parse(input: &str) -> ProbeResult<Query> {
    let file = QueryParser::parse(Rule::probe_file, input)
        .map_err(|e| e.to_string())?
        .next()
        .unwrap();
    let mut items = file.into_inner().next().unwrap().into_inner();
    let mut query = Query {
        header: QueryDecl {
            name: items.next().unwrap().as_str().into(),
            description: None,
            instruction: None,
            params: Vec::new(),
            match_clause: Vec::new(),
            return_clause: Vec::new(),
            order_clause: Vec::new(),
            limit: None,
            mutations: Vec::new(),
        },
        stages: Vec::new(),
        projections: Vec::new(),
        order: Vec::new(),
    };
    for item in items {
        match item.as_rule() {
            Rule::param_list => {
                query.header.params = item
                    .into_inner()
                    .map(parse_param)
                    .collect::<Result<_>>()
                    .map_err(|e| e.to_string())?
            }
            Rule::query_annotation => {
                let (name, text) = parse_query_annotation(item).map_err(|e| e.to_string())?;
                let slot = match name {
                    "description" => &mut query.header.description,
                    _ => &mut query.header.instruction,
                };
                if slot.replace(text).is_some() {
                    return Err(format!("duplicate @{name}"));
                }
            }
            Rule::probe_match => {
                let mut clauses = Vec::new();
                for c in item.into_inner() {
                    clauses.push(if c.as_rule() == Rule::clause {
                        MatchItem::Graph(parse_clause(c).map_err(|e| e.to_string())?)
                    } else {
                        let mut parts = c.into_inner();
                        MatchItem::Terms {
                            field: core(parts.next().unwrap())?,
                            query: terms(parts.next().unwrap())?,
                        }
                    });
                }
                query.stages.push(Stage::Match(clauses));
            }
            Rule::probe_rank => {
                let mut parts = item.into_inner();
                let target = parts.next().unwrap().as_str()[1..].to_string();
                query.stages.push(Stage::Rank {
                    target,
                    declarations: parts.map(declaration).collect::<ProbeResult<_>>()?,
                });
            }
            Rule::probe_return => {
                for p in item.into_inner() {
                    let mut parts = p.into_inner();
                    query.projections.push((
                        value(parts.next().unwrap())?,
                        parts.next().map(|p| p.as_str().into()),
                    ));
                }
            }
            Rule::probe_order => {
                for o in item.into_inner() {
                    let mut parts = o.into_inner();
                    query.order.push((
                        value(parts.next().unwrap())?,
                        parts.next().is_some_and(|p| p.as_str() == "desc"),
                    ));
                }
            }
            Rule::limit_clause => {
                query.header.limit = Some(
                    item.into_inner()
                        .next()
                        .unwrap()
                        .as_str()
                        .parse()
                        .map_err(|e| format!("limit: {e}"))?,
                )
            }
            _ => unreachable!(),
        }
    }
    Ok(query)
}

// IDs are semantic references within one typed plan; names are just lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SourceId {
    block: usize,
    ordinal: usize,
}

#[derive(Debug, Clone, PartialEq)]
enum Bound {
    Literal(u64),
    Parameter {
        name: String,
        min: u64,
        max: Option<u64>,
    },
}

#[derive(Debug, Clone)]
struct CheckedSource {
    id: SourceId,
    input_stage: usize,
    target: String,
    kind: SourceKind,
    candidates: Bound,
    arms: Vec<SourceId>,
    // Keep the typed syntax as well: this probe must not discard predicates,
    // query operands, weights or options while merely validating their shape.
    declaration: Declaration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricDomain {
    Rank,
    Bm25Score,
    RrfScore,
    Distance,
}

#[derive(Debug, Clone, PartialEq)]
enum ValueType {
    Core(ResolvedType),
    Identity(String),
    Metric {
        source: SourceId,
        domain: MetricDomain,
    },
    Reduced {
        function: AggFunc,
        input: Box<ValueType>,
    },
}

impl ValueType {
    fn metric_origins(&self) -> Vec<SourceId> {
        match self {
            Self::Metric { source, .. } => vec![*source],
            Self::Reduced { input, .. } => input.metric_origins(),
            _ => Vec::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum OutputOrder {
    Ranked(SourceId),
    Explicit(Vec<(ValueType, bool)>),
    Unordered,
}

#[derive(Debug, PartialEq)]
enum OutputScope {
    Bindings,
    Groups { key_projections: Vec<usize> },
}

#[derive(Debug)]
struct Plan {
    query: Query,
    sources: BTreeMap<String, CheckedSource>,
    scopes: Vec<BTreeMap<String, String>>,
    projection_types: Vec<ValueType>,
    output_order: OutputOrder,
    output_scope: OutputScope,
}

fn context(catalog: &Catalog, prefix: &QueryDecl) -> ProbeResult<TypeContext> {
    typecheck_query(catalog, prefix).map_err(|e| e.to_string())
}

fn core_type(catalog: &Catalog, prefix: &QueryDecl, expr: &Expr) -> ProbeResult<ResolvedType> {
    reject_legacy(expr)?;
    let mut probe = prefix.clone();
    probe.return_clause = vec![Projection {
        expr: expr.clone(),
        alias: Some("probe_value".into()),
    }];
    Ok(context(catalog, &probe)?
        .aliases
        .remove("probe_value")
        .unwrap())
}

fn reject_legacy(expr: &Expr) -> ProbeResult<()> {
    match expr {
        Expr::Nearest { .. }
        | Expr::Search { .. }
        | Expr::Fuzzy { .. }
        | Expr::MatchText { .. }
        | Expr::Bm25 { .. }
        | Expr::Rrf { .. } => {
            Err("removed retrieval expression; use a rank stage or match_terms".into())
        }
        Expr::Aggregate { arg, .. } => reject_legacy(arg),
        _ => Ok(()),
    }
}

fn reject_legacy_clause(clause: &Clause) -> ProbeResult<()> {
    match clause {
        Clause::Filter(f) => {
            reject_legacy(&f.left)?;
            reject_legacy(&f.right)
        }
        Clause::Negation(clauses) => clauses.iter().try_for_each(reject_legacy_clause),
        _ => Ok(()),
    }
}

fn parameter(prefix: &QueryDecl, name: &str) -> ProbeResult<PropType> {
    let param = prefix
        .params
        .iter()
        .rev()
        .find(|p| p.name == name)
        .ok_or_else(|| format!("`${name}` is not a declared query parameter"))?;
    PropType::from_param_type_name(&param.type_name, param.nullable)
        .ok_or_else(|| format!("invalid parameter type for `${name}`"))
}

fn bound(prefix: &QueryDecl, expr: &Expr, min: u64, max: Option<u64>) -> ProbeResult<Bound> {
    match expr {
        Expr::Literal(Literal::Integer(value)) => {
            let value = u64::try_from(*value).map_err(|_| "negative bound")?;
            if value < min || max.is_some_and(|max| value > max) {
                return Err("bound out of range".into());
            }
            Ok(Bound::Literal(value))
        }
        Expr::Variable(name) => {
            let ty = parameter(prefix, name)?;
            if ty.nullable
                || ty.list
                || !matches!(
                    ty.scalar,
                    ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64
                )
            {
                return Err("bound parameter must be a non-null integer".into());
            }
            Ok(Bound::Parameter {
                name: name.clone(),
                min,
                max,
            })
        }
        _ => Err("bound must be an integer literal or query parameter".into()),
    }
}

fn allowed(options: &Options, names: &[&str]) -> ProbeResult<()> {
    if let Some(key) = options.keys().find(|key| !names.contains(&key.as_str())) {
        return Err(format!("unknown option `{key}`"));
    }
    Ok(())
}

fn constant_input(prefix: &QueryDecl, input: &Expr, dimension: Option<u32>) -> ProbeResult<()> {
    match input {
        Expr::Literal(Literal::String(_)) => Ok(()),
        Expr::Literal(Literal::List(values))
            if dimension.and_then(|dim| usize::try_from(dim).ok()) == Some(values.len())
                && values
                    .iter()
                    .all(|v| matches!(v, Literal::Integer(_) | Literal::Float(_))) =>
        {
            Ok(())
        }
        Expr::Variable(name) => {
            let ty = parameter(prefix, name)?;
            if !ty.nullable
                && !ty.list
                && (ty.scalar == ScalarType::String
                    || matches!(ty.scalar, ScalarType::Vector(dim) if Some(dim) == dimension))
            {
                Ok(())
            } else {
                Err("incompatible or nullable query input".into())
            }
        }
        _ => Err(
            "query input must be a constant String or compatible vector literal/parameter".into(),
        ),
    }
}

fn field_type(
    catalog: &Catalog,
    prefix: &QueryDecl,
    field: &Expr,
    target: Option<&str>,
) -> ProbeResult<PropType> {
    let Expr::PropAccess { variable, .. } = field else {
        return Err("source requires a target property".into());
    };
    if target.is_some_and(|target| target != variable) {
        return Err("source property belongs to a different target".into());
    }
    match core_type(catalog, prefix, field)? {
        ResolvedType::Scalar(ty) => Ok(ty),
        _ => Err("source property must be scalar".into()),
    }
}

fn check_terms(
    catalog: &Catalog,
    prefix: &QueryDecl,
    field: &Expr,
    terms: &Terms,
    target: Option<&str>,
) -> ProbeResult<()> {
    let field = field_type(catalog, prefix, field, target)?;
    if field.list || field.scalar != ScalarType::String {
        return Err("terms requires a scalar String property".into());
    }
    constant_input(prefix, &terms.text, None)?;
    allowed(&terms.options, &["mode", "max_edits"])?;
    if let Some(mode) = terms.options.get("mode")
        && !matches!(mode, Expr::AliasRef(name) if name == "all" || name == "any")
    {
        return Err("terms mode must be all or any".into());
    }
    if let Some(edits) = terms.options.get("max_edits") {
        bound(prefix, edits, 0, Some(2))?;
    }
    Ok(())
}

fn positive_number(prefix: &QueryDecl, expr: &Expr) -> ProbeResult<()> {
    match expr {
        Expr::Literal(Literal::Integer(n)) if *n > 0 => Ok(()),
        Expr::Literal(Literal::Float(n)) if n.is_finite() && *n > 0.0 => Ok(()),
        Expr::Variable(name) => {
            let ty = parameter(prefix, name)?;
            if !ty.nullable && !ty.list && ty.scalar.is_numeric() {
                Ok(())
            } else {
                Err("weight parameter must be non-null numeric".into())
            }
        }
        _ => Err("weight must be positive and finite".into()),
    }
}

fn check_source(
    catalog: &Catalog,
    prefix: &QueryDecl,
    declaration: &Declaration,
    id: SourceId,
    target: &str,
    sources: &BTreeMap<String, CheckedSource>,
) -> ProbeResult<CheckedSource> {
    let candidates = bound(
        prefix,
        declaration
            .options
            .get("candidates")
            .ok_or("missing candidates window")?,
        1,
        Some(10_000),
    )?;
    let mut arms = Vec::new();
    let kind = match &declaration.source {
        Source::Lexical { field, query } => {
            allowed(&declaration.options, &["candidates", "scoring"])?;
            if let Some(scoring) = declaration.options.get("scoring")
                && !matches!(scoring, Expr::AliasRef(name) if name == "bm25_v1")
            {
                return Err("unknown lexical scoring policy".into());
            }
            check_terms(catalog, prefix, field, query, Some(target))?;
            SourceKind::Lexical
        }
        Source::Vector { kind, field, query } => {
            allowed(
                &declaration.options,
                if *kind == SourceKind::Ann {
                    &["candidates", "oversample"]
                } else {
                    &["candidates"]
                },
            )?;
            let field = field_type(catalog, prefix, field, Some(target))?;
            let ScalarType::Vector(dimension) = field.scalar else {
                return Err("vector source requires a Vector property".into());
            };
            if field.list {
                return Err("vector source does not accept a list of vectors".into());
            }
            constant_input(prefix, query, Some(dimension))?;
            if let Some(effort) = declaration.options.get("oversample") {
                bound(prefix, effort, 1, None)?;
            }
            *kind
        }
        Source::Fusion(inputs) => {
            allowed(&declaration.options, &["candidates", "k"])?;
            if !(2..=16).contains(&inputs.len()) {
                return Err("fusion requires 2..16 arms".into());
            }
            if let Some(k) = declaration.options.get("k") {
                positive_number(prefix, k)?;
            }
            let mut seen = BTreeSet::new();
            for (alias, options) in inputs {
                allowed(options, &["weight"])?;
                if let Some(weight) = options.get("weight") {
                    positive_number(prefix, weight)?;
                }
                let source = sources
                    .get(alias)
                    .ok_or_else(|| format!("unknown or forward arm `{alias}`"))?;
                if source.id.block != id.block {
                    return Err("fusion arm must belong to the current rank block".into());
                }
                if !seen.insert(source.id) {
                    return Err("duplicate fusion arm".into());
                }
                arms.push(source.id);
            }
            SourceKind::Rrf
        }
    };
    Ok(CheckedSource {
        id,
        input_stage: id.block - 1,
        target: target.into(),
        kind,
        candidates,
        arms,
        declaration: declaration.clone(),
    })
}

fn value_type(
    catalog: &Catalog,
    prefix: &QueryDecl,
    value: &Value,
    sources: &BTreeMap<String, CheckedSource>,
    aliases: &BTreeMap<String, ValueType>,
) -> ProbeResult<ValueType> {
    match value {
        Value::Core(Expr::AliasRef(alias)) => aliases
            .get(alias)
            .cloned()
            .ok_or_else(|| format!("unknown result alias `{alias}`")),
        Value::Core(expr) => Ok(ValueType::Core(core_type(catalog, prefix, expr)?)),
        Value::Identity(variable) => {
            if !context(catalog, prefix)?.bindings.contains_key(variable) {
                return Err(format!("unbound identity `${variable}`"));
            }
            Ok(ValueType::Identity(variable.clone()))
        }
        Value::Metric { source, field } => {
            let source = sources.get(source).ok_or("unknown metric source")?;
            let domain = match (source.kind, field.as_str()) {
                (_, "rank") => MetricDomain::Rank,
                (SourceKind::Lexical, "score") => MetricDomain::Bm25Score,
                (SourceKind::Rrf, "score") => MetricDomain::RrfScore,
                (SourceKind::Knn | SourceKind::Ann, "distance") => MetricDomain::Distance,
                _ => return Err("metric is not defined by this source".into()),
            };
            // SourceId retains query, target and population through aliases.
            // Presence/nullability and resolved representation fingerprints
            // are separate qualification gates, not inferred from this tag.
            Ok(ValueType::Metric {
                source: source.id,
                domain,
            })
        }
        Value::Aggregate { function, value } => {
            if matches!(value.as_ref(), Value::Aggregate { .. }) {
                return Err("nested aggregates are not supported".into());
            }
            let input = value_type(catalog, prefix, value, sources, aliases)?;
            if matches!(input, ValueType::Reduced { .. }) {
                return Err("nested aggregates through aliases are not supported".into());
            }
            let function = match function.as_str() {
                "count" => AggFunc::Count,
                "min" => AggFunc::Min,
                "max" => AggFunc::Max,
                // Adding or averaging ranks or uncalibrated scores needs an
                // explicit domain contract, not the underlying float type.
                "sum" | "avg" if matches!(&input, ValueType::Core(ResolvedType::Scalar(ty)) if !ty.list && ty.scalar.is_numeric()) => {
                    if function == "sum" {
                        AggFunc::Sum
                    } else {
                        AggFunc::Avg
                    }
                }
                _ => return Err("prototype has no domain contract for this aggregate".into()),
            };
            let orderable = match &input {
                ValueType::Metric { .. } => true,
                ValueType::Core(ResolvedType::Scalar(ty)) => !ty.list && ty.scalar.is_orderable(),
                _ => false,
            };
            if function != AggFunc::Count && !orderable {
                return Err("min/max requires an orderable scalar or metric".into());
            }
            Ok(ValueType::Reduced {
                function,
                input: Box::new(input),
            })
        }
    }
}

fn check(catalog: &Catalog, query: Query) -> ProbeResult<Plan> {
    let mut prefix = query.header.clone();
    let mut sources = BTreeMap::new();
    let mut scopes = Vec::new();
    let mut active_order = None;
    for (index, stage) in query.stages.iter().enumerate() {
        match stage {
            Stage::Match(clauses) => {
                for clause in clauses {
                    match clause {
                        MatchItem::Graph(c) => {
                            reject_legacy_clause(c)?;
                            prefix.match_clause.push(c.clone());
                            context(catalog, &prefix)?;
                        }
                        MatchItem::Terms { field, query } => {
                            check_terms(catalog, &prefix, field, query, None)?
                        }
                    }
                }
            }
            Stage::Rank {
                target,
                declarations,
            } => {
                if target == "_" {
                    return Err("rank target must be named; anonymous `$_` has no reusable binding identity".into());
                }
                if !context(catalog, &prefix)?.bindings.contains_key(target) {
                    return Err(format!(
                        "rank target `${target}` is not bound before this stage"
                    ));
                }
                for (ordinal, declaration) in declarations.iter().enumerate() {
                    if sources.contains_key(&declaration.alias) {
                        return Err("duplicate stage alias".into());
                    }
                    let id = SourceId {
                        block: index,
                        ordinal,
                    };
                    let source = check_source(catalog, &prefix, declaration, id, target, &sources)?;
                    sources.insert(declaration.alias.clone(), source);
                    active_order = Some(id);
                }
            }
        }
        scopes.push(
            context(catalog, &prefix)?
                .bindings
                .into_iter()
                .map(|(name, binding)| {
                    let kind = match binding {
                        BoundVariable::Node { type_name } => format!("Node<{type_name}>"),
                        BoundVariable::Edge { type_name } => format!("Edge<{type_name}>"),
                    };
                    (name, kind)
                })
                .collect(),
        );
    }
    let aggregate = query
        .projections
        .iter()
        .any(|(value, _)| matches!(value, Value::Aggregate { .. }));
    let mut aliases = BTreeMap::new();
    let mut projection_types = Vec::new();
    for (value, alias) in &query.projections {
        let ty = value_type(catalog, &prefix, value, &sources, &aliases)?;
        if let Some(alias) = alias {
            if aliases.insert(alias.clone(), ty.clone()).is_some() {
                return Err("duplicate result alias".into());
            }
        }
        projection_types.push(ty);
    }
    let mut explicit_order = Vec::new();
    for (value, descending) in &query.order {
        if aggregate && !matches!(value, Value::Core(Expr::AliasRef(_))) {
            return Err(
                "aggregate ordering must reference a projected result alias in this prototype"
                    .into(),
            );
        }
        explicit_order.push((
            value_type(catalog, &prefix, value, &sources, &aliases)?,
            *descending,
        ));
    }
    if aggregate {
        active_order = None;
    }
    let output_order = if !explicit_order.is_empty() {
        OutputOrder::Explicit(explicit_order)
    } else if let Some(source) = active_order {
        OutputOrder::Ranked(source)
    } else {
        OutputOrder::Unordered
    };
    let output_scope = if aggregate {
        OutputScope::Groups {
            key_projections: projection_types
                .iter()
                .enumerate()
                .filter_map(|(index, ty)| {
                    (!matches!(ty, ValueType::Reduced { .. })).then_some(index)
                })
                .collect(),
        }
    } else {
        OutputScope::Bindings
    };
    Ok(Plan {
        query,
        sources,
        scopes,
        projection_types,
        output_order,
        output_scope,
    })
}

fn catalog() -> Catalog {
    build_catalog(
        &parse_schema(
            r#"
node Organization { slug: String @key name: String embedding: Vector(3) }
node Incident { slug: String @key title: String severity: I32 embedding: Vector(3) }
edge HasIncident: Organization -> Incident { note: String }
"#,
        )
        .unwrap(),
    )
    .unwrap()
}

const STAGED: &str = r#"
query staged($q: String, $vector: Vector(3), $window: I64) {
  match { $o: Organization }
  rank $o {
    lexical($o.name, terms($q, max_edits: 1), candidates: $window) as words
    knn($o.embedding, $vector, candidates: 100) as meaning
    rrf(arm(words), arm(meaning, weight: 1.5), candidates: 20) as combined
  }
  match { $o hasIncident $i $i.title contains "outage" }
  rank $i { lexical($i.title, terms($q), candidates: 5) as incidents }
  return { $o.@id, $i.slug, metric(words, rank) as words, metric(incidents, score) as score }
  order { words asc, score desc, $i.@id }
  limit 3
}
"#;

#[test]
fn staged_probe_preserves_scopes_populations_and_metric_origins() {
    let plan = check(&catalog(), parse(STAGED).unwrap()).unwrap();
    assert_eq!(
        plan.scopes
            .iter()
            .map(|scope| scope.keys().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        [vec!["o"], vec!["o"], vec!["i", "o"], vec!["i", "o"]]
    );
    let words = &plan.sources["words"];
    assert_eq!((words.input_stage, words.target.as_str()), (0, "o"));
    assert_eq!(
        words.candidates,
        Bound::Parameter {
            name: "window".into(),
            min: 1,
            max: Some(10_000)
        }
    );
    assert_eq!(plan.sources["meaning"].input_stage, 0);
    assert_eq!(
        plan.sources["combined"].arms,
        [words.id, plan.sources["meaning"].id]
    );
    assert_eq!(plan.sources["incidents"].input_stage, 2);
    assert_eq!(
        plan.projection_types,
        [
            ValueType::Identity("o".into()),
            ValueType::Core(ResolvedType::Scalar(PropType::scalar(
                ScalarType::String,
                false
            ))),
            ValueType::Metric {
                source: words.id,
                domain: MetricDomain::Rank
            },
            ValueType::Metric {
                source: plan.sources["incidents"].id,
                domain: MetricDomain::Bm25Score
            }
        ]
    );
    let OutputOrder::Explicit(order) = &plan.output_order else {
        panic!("final order overrides stage order");
    };
    assert_eq!(order[0], (plan.projection_types[2].clone(), false));
    assert_eq!(order[1], (plan.projection_types[3].clone(), true));
    assert_eq!(order[0].0.metric_origins(), [words.id]);
    assert_eq!(order[1].0.metric_origins(), [plan.sources["incidents"].id]);
    assert_eq!(plan.query.header.limit, Some(3));
    assert_eq!(plan.output_scope, OutputScope::Bindings);
    assert_eq!(plan.sources["incidents"].declaration.alias, "incidents");
    assert!(
        parse_query(STAGED).is_err(),
        "production root must keep rejecting staged syntax"
    );
}

#[test]
fn staged_probe_rejects_invalid_scope_operands_and_references() {
    let catalog = catalog();
    let cases = [
        ("rank $o {", "rank $i {", "not bound before this stage"),
        ("lexical($o.name", "lexical($i.title", "different target"),
        (
            "terms($q, max_edits: 1)",
            "terms($o.name)",
            "constant String",
        ),
        (
            "$vector: Vector(3)",
            "$vector: Vector(4)",
            "incompatible or nullable query input",
        ),
        (
            "$q: String",
            "$q: String?",
            "incompatible or nullable query input",
        ),
        ("$window: I64", "$window: I64?", "non-null integer"),
        ("max_edits: 1", "max_edits: 3", "bound out of range"),
        ("max_edits: 1", "mode: maybe", "mode must be all or any"),
        (
            "max_edits: 1",
            "max_edits: 1, max_edits: 2",
            "duplicate option",
        ),
        ("candidates: $window", "candidates: 0", "bound out of range"),
        (
            "candidates: $window",
            "candidates: 10001",
            "bound out of range",
        ),
        (
            "candidates: $window",
            "candidates: 1.5",
            "integer literal or query parameter",
        ),
        (
            "candidates: $window",
            "candidates: $missing",
            "not a declared query parameter",
        ),
        (
            "candidates: $window",
            "typo: 3, candidates: 10",
            "unknown option",
        ),
        (
            "candidates: 100) as meaning",
            "oversample: 4, candidates: 100) as meaning",
            "unknown option",
        ),
        ("arm(words)", "arm(combined)", "unknown or forward arm"),
        (
            "arm(meaning, weight: 1.5)",
            "arm(words)",
            "duplicate fusion arm",
        ),
        ("weight: 1.5", "weight: 0", "positive and finite"),
        ("as incidents", "as words", "duplicate stage alias"),
        (
            "metric(words, rank)",
            "metric(words, distance)",
            "metric is not defined",
        ),
        (
            "metric(words, rank)",
            "metric(absent, rank)",
            "unknown metric source",
        ),
        (
            "as words, metric(incidents, score) as score",
            "as score, metric(incidents, score) as score",
            "duplicate result alias",
        ),
        (
            "order { words asc, score desc, $i.@id }",
            "order { nearest($i.embedding, $vector) }",
            "removed retrieval expression",
        ),
        ("$o hasIncident $i", "$o: Incident", "cannot rebind"),
    ];
    for (from, to, expected) in cases {
        assert!(STAGED.contains(from));
        let error = parse(&STAGED.replace(from, to))
            .and_then(|query| check(&catalog, query))
            .unwrap_err();
        assert!(error.contains(expected), "{from} -> {to}: {error}");
    }
}

#[test]
fn staged_probe_does_not_leak_negation_bindings_and_can_rank_edges() {
    let catalog = catalog();
    let anonymous = STAGED.replace("$o", "$_");
    assert!(
        check(&catalog, parse(&anonymous).unwrap())
            .unwrap_err()
            .contains("rank target must be named")
    );
    let hidden = STAGED
        .replace(
            "match { $o: Organization }",
            "match { $o: Organization not { $o hasIncident $hidden } }",
        )
        .replace("rank $o {", "rank $hidden {");
    assert!(
        check(&catalog, parse(&hidden).unwrap())
            .unwrap_err()
            .contains("not bound before this stage")
    );
    let edge = r#"
query edge_search($q: String) {
  match { $o: Organization $o $link:hasIncident $i }
  rank $link { lexical($link.note, terms($q), candidates: 5) as edges }
  return { $link.@id, $i.slug, metric(edges, rank) as rank }
}
"#;
    let plan = check(&catalog, parse(edge).unwrap()).unwrap();
    assert_eq!(plan.scopes[0]["link"], "Edge<HasIncident>");
    assert_eq!(plan.sources["edges"].target, "link");
    assert_eq!(
        plan.output_order,
        OutputOrder::Ranked(plan.sources["edges"].id)
    );
}

#[test]
fn staged_probe_cross_block_metrics_do_not_reopen_previous_populations() {
    let input = r#"
query repeated($q: String) {
  match { $o: Organization }
  rank $o { lexical($o.name, terms($q), candidates: 10) as original }
  match { $o.name contains "selected" }
  rank $o { lexical($o.name, terms($q), candidates: 5) as later }
  return { $o.slug, metric(original, rank) as original_rank, metric(later, rank) as later_rank }
}
"#;
    let catalog = catalog();
    let plan = check(&catalog, parse(input).unwrap()).unwrap();
    assert_eq!(plan.sources["original"].input_stage, 0);
    assert_eq!(plan.sources["later"].input_stage, 2);
    assert_ne!(plan.projection_types[1], plan.projection_types[2]);
    let invalid = input.replace(
        "as later }",
        "as later rrf(arm(original), arm(later), candidates: 5) as reopened }",
    );
    assert!(
        check(&catalog, parse(&invalid).unwrap())
            .unwrap_err()
            .contains("current rank block")
    );
}

#[test]
fn staged_probe_aggregation_establishes_a_new_output_scope() {
    let input = r#"
query grouped($q: String) {
  match { $o: Organization $o hasIncident $i }
  rank $i { lexical($i.title, terms($q), candidates: 10) as hits }
  return { $o.slug as organization, min(metric(hits, rank)) as best, count($i) as n }
  order { best asc, organization asc }
}
"#;
    let catalog = catalog();
    let plan = check(&catalog, parse(input).unwrap()).unwrap();
    assert_eq!(
        plan.output_scope,
        OutputScope::Groups {
            key_projections: vec![0]
        }
    );
    assert_eq!(
        plan.projection_types[1],
        ValueType::Reduced {
            function: AggFunc::Min,
            input: Box::new(ValueType::Metric {
                source: plan.sources["hits"].id,
                domain: MetricDomain::Rank
            }),
        }
    );
    let OutputOrder::Explicit(order) = plan.output_order else {
        panic!("expected aggregate order");
    };
    assert_eq!(order[0].0, plan.projection_types[1]);
    let unordered = input.replace("order { best asc, organization asc }", "");
    assert_eq!(
        check(&catalog, parse(&unordered).unwrap())
            .unwrap()
            .output_order,
        OutputOrder::Unordered
    );
    let discarded = input.replace(
        "order { best asc, organization asc }",
        "order { metric(hits, rank) }",
    );
    assert!(
        check(&catalog, parse(&discarded).unwrap())
            .unwrap_err()
            .contains("projected result alias")
    );
    let nested = input.replace("min(metric(hits, rank))", "min(count($i))");
    assert!(
        check(&catalog, parse(&nested).unwrap())
            .unwrap_err()
            .contains("nested aggregates")
    );
    let scalar_sum = input.replace("count($i) as n", "sum($i.severity) as total");
    let summed = check(&catalog, parse(&scalar_sum).unwrap()).unwrap();
    assert!(matches!(
        summed.projection_types[2],
        ValueType::Reduced {
            function: AggFunc::Sum,
            ..
        }
    ));
    let rank_sum = input.replace("min(metric(hits, rank))", "sum(metric(hits, rank))");
    assert!(
        check(&catalog, parse(&rank_sum).unwrap())
            .unwrap_err()
            .contains("no domain contract")
    );
    // Ordinary non-aggregate return items are grouping keys. A projected
    // metric may be such a key, but that does not retain its old row order.
    let grouped_rank = input.replace("min(metric(hits, rank))", "metric(hits, rank)");
    assert_eq!(
        check(&catalog, parse(&grouped_rank).unwrap())
            .unwrap()
            .output_scope,
        OutputScope::Groups {
            key_projections: vec![0, 1]
        }
    );
}

#[test]
fn staged_probe_final_projection_and_limit_do_not_resize_sources() {
    let catalog = catalog();
    let before = check(&catalog, parse(STAGED).unwrap()).unwrap();
    let changed = STAGED
        .replace("limit 3", "limit 999")
        .replace("$o.@id, $i.slug,", "$o.slug, $i.title,");
    let after = check(&catalog, parse(&changed).unwrap()).unwrap();
    for (alias, source) in &before.sources {
        let other = &after.sources[alias];
        assert_eq!(source.id, other.id);
        assert_eq!(source.input_stage, other.input_stage);
        assert_eq!(source.target, other.target);
        assert_eq!(source.candidates, other.candidates);
        assert_eq!(source.arms, other.arms);
    }
}

#[test]
fn staged_probe_checks_the_rfc_examples_directly() {
    let rfc = include_str!("../../../../docs/rfcs/0048-search-contracts.md");
    let catalog = catalog();
    let examples: Vec<_> = rfc
        .split("```gq\n")
        .skip(1)
        .map(|part| part.split_once("```").unwrap().0)
        .collect();
    assert_eq!(
        examples.len(),
        2,
        "review new examples when extending the prototype"
    );
    for example in examples {
        let plan = check(&catalog, parse(example).unwrap()).unwrap();
        assert!(!plan.query.projections.is_empty());
        assert!(
            parse_query(example).is_err(),
            "RFC syntax is not shipped by this prototype"
        );
    }
}
