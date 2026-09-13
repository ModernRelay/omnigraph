//! RFC 0048 syntax/scope experiment, not a second production compiler.
//! Reuses the real graph parser and typechecker. Prefix queries below are
//! typed AST values used only for scope checks; execution stages stay separate.
//! Representation certification, parameter admission and physical lowering
//! are not implemented by this experiment.

use std::collections::{BTreeMap, BTreeSet};

use pest::iterators::Pair;

use super::*;
use crate::catalog::{Catalog, build_catalog};
use crate::query::typecheck::{BoundVariable, ResolvedType, TypeContext, typecheck_query};
use crate::schema::parser::parse_schema;
use crate::types::{Direction, PropType, ScalarType};

#[path = "staged_probe/plan.rs"]
mod plan;

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
    Filter(Value),
    Negation(Vec<MatchItem>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    Lexical,
    Knn,
    Ann,
    Rrf,
    LexicalFeature,
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
        output: String,
    },
    Take(Take),
    Group {
        keys: Vec<(Value, Option<String>)>,
        reductions: Vec<(Value, Option<String>)>,
    },
    Let(Vec<(Value, Option<String>)>),
    Select {
        order: Vec<OrderingSpec<Value>>,
        count: Expr,
    },
    Score {
        target: String,
        declarations: Vec<Declaration>,
    },
    Nested {
        kind: NestedKind,
        imports: Vec<Expr>,
        alias: String,
        query: Box<Query>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NestedKind {
    Collect,
    Optional,
}

#[derive(Debug, Clone)]
struct Take {
    target: String,
    keys: Vec<Value>,
    order: Vec<OrderingSpec<Value>>,
    count: Expr,
}

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Core(Expr),
    Identity(String),
    Metric {
        source: String,
        field: String,
    },
    Aggregate {
        function: String,
        value: Box<Value>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Value>,
        right: Box<Value>,
    },
    Not(Box<Value>),
    IsNull(Box<Value>),
    Feature(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    And,
    Or,
    Compare(CompOp),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReduceOp {
    Count,
    CountIf,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

impl Value {
    fn contains_aggregate(&self) -> bool {
        match self {
            Self::Aggregate { .. } => true,
            Self::Binary { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::Not(value) | Self::IsNull(value) => value.contains_aggregate(),
            _ => false,
        }
    }

    fn uses_only_output_values(&self) -> bool {
        match self {
            Self::Core(Expr::AliasRef(_) | Expr::Literal(_)) => true,
            Self::Binary { left, right, .. } => {
                left.uses_only_output_values() && right.uses_only_output_values()
            }
            Self::Not(value) | Self::IsNull(value) => value.uses_only_output_values(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OrderingSpec<T> {
    value: T,
    descending: bool,
    nulls_first: bool,
}

#[derive(Debug, Clone)]
struct Query {
    header: QueryDecl,
    stages: Vec<Stage>,
    projections: Vec<(Value, Option<String>)>,
    order: Vec<OrderingSpec<Value>>,
    limit: Option<Expr>,
}

fn core(pair: Pair<Rule>) -> ProbeResult<Expr> {
    if pair.as_rule() == Rule::probe_value {
        return match value(pair)? {
            Value::Core(expr) => Ok(expr),
            _ => Err("this operand requires a literal, parameter or property".into()),
        };
    }
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
    let item = match pair.as_rule() {
        Rule::probe_value | Rule::probe_primary => return value(pair.into_inner().next().unwrap()),
        Rule::probe_or
        | Rule::probe_and
        | Rule::probe_comparison
        | Rule::probe_sum
        | Rule::probe_product => {
            let mut parts = pair.into_inner();
            let mut left = value(parts.next().unwrap())?;
            while let Some(op) = parts.next() {
                let op = match op.as_str() {
                    "+" => BinaryOp::Add,
                    "-" => BinaryOp::Subtract,
                    "*" => BinaryOp::Multiply,
                    "/" => BinaryOp::Divide,
                    "and" => BinaryOp::And,
                    "or" => BinaryOp::Or,
                    "=" => BinaryOp::Compare(CompOp::Eq),
                    "!=" => BinaryOp::Compare(CompOp::Ne),
                    ">" => BinaryOp::Compare(CompOp::Gt),
                    "<" => BinaryOp::Compare(CompOp::Lt),
                    ">=" => BinaryOp::Compare(CompOp::Ge),
                    "<=" => BinaryOp::Compare(CompOp::Le),
                    "contains" => BinaryOp::Compare(CompOp::Contains),
                    "starts_with" => BinaryOp::Compare(CompOp::StartsWith),
                    _ => unreachable!(),
                };
                left = Value::Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(value(parts.next().unwrap())?),
                };
            }
            return Ok(left);
        }
        Rule::probe_not => {
            let mut parts = pair.into_inner();
            let mut result = value(parts.next_back().unwrap())?;
            for _ in parts {
                result = Value::Not(Box::new(result));
            }
            return Ok(result);
        }
        _ => pair,
    };
    match item.as_rule() {
        Rule::probe_feature => Ok(Value::Feature(
            item.into_inner().next().unwrap().as_str().into(),
        )),
        Rule::probe_alias => Ok(Value::Core(Expr::AliasRef(item.as_str().into()))),
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
        Rule::probe_is_null => Ok(Value::IsNull(Box::new(value(
            item.into_inner().next().unwrap(),
        )?))),
        _ => unreachable!(),
    }
}

fn match_item(pair: Pair<Rule>) -> ProbeResult<MatchItem> {
    let item = pair.into_inner().next().unwrap();
    match item.as_rule() {
        Rule::binding => Ok(MatchItem::Graph(Clause::Binding(
            parse_binding(item).map_err(|e| e.to_string())?,
        ))),
        Rule::traversal => Ok(MatchItem::Graph(Clause::Traversal(
            parse_traversal(item).map_err(|e| e.to_string())?,
        ))),
        Rule::probe_negation => Ok(MatchItem::Negation(
            item.into_inner()
                .skip(1)
                .map(match_item)
                .collect::<ProbeResult<_>>()?,
        )),
        Rule::probe_filter => Ok(MatchItem::Filter(value(item.into_inner().next().unwrap())?)),
        Rule::probe_match_terms => {
            let mut parts = item.into_inner();
            Ok(MatchItem::Terms {
                field: core(parts.next().unwrap())?,
                query: terms(parts.next().unwrap())?,
            })
        }
        _ => unreachable!(),
    }
}

fn order(pair: Pair<Rule>) -> ProbeResult<Vec<OrderingSpec<Value>>> {
    pair.into_inner()
        .map(|ordering| {
            let mut parts = ordering.into_inner();
            let value = value(parts.next().unwrap())?;
            let mut descending = false;
            let mut explicit_nulls = None;
            for part in parts {
                match part.as_rule() {
                    Rule::order_dir => descending = part.as_str() == "desc",
                    Rule::probe_nulls => {
                        explicit_nulls = Some(part.into_inner().next().unwrap().as_str() == "first")
                    }
                    _ => unreachable!(),
                }
            }
            // Preserve current GQ ordering defaults when no modifier is given.
            Ok(OrderingSpec {
                value,
                descending,
                nulls_first: explicit_nulls.unwrap_or(!descending),
            })
        })
        .collect()
}

fn projection(pair: Pair<Rule>) -> ProbeResult<(Value, Option<String>)> {
    let mut parts = pair.into_inner();
    Ok((
        value(parts.next().unwrap())?,
        parts.next().map(|p| p.as_str().into()),
    ))
}

fn parse(input: &str) -> ProbeResult<Query> {
    let file = QueryParser::parse(Rule::probe_file, input)
        .map_err(|e| e.to_string())?
        .next()
        .unwrap();
    let mut items = file.into_inner().next().unwrap().into_inner();
    let query = Query {
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
        limit: None,
    };
    parse_items(query, items)
}

fn parse_items(mut query: Query, items: pest::iterators::Pairs<Rule>) -> ProbeResult<Query> {
    for item in items {
        match item.as_rule() {
            Rule::probe_body => query = parse_items(query, item.into_inner())?,
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
                let clauses = item
                    .into_inner()
                    .map(match_item)
                    .collect::<ProbeResult<_>>()?;
                query.stages.push(Stage::Match(clauses));
            }
            Rule::probe_rank => {
                let mut parts = item.into_inner();
                let target = parts.next().unwrap().as_str()[1..].to_string();
                let output = parts
                    .next_back()
                    .unwrap()
                    .into_inner()
                    .nth(1)
                    .unwrap()
                    .as_str()
                    .into();
                query.stages.push(Stage::Rank {
                    target,
                    declarations: parts.map(declaration).collect::<ProbeResult<_>>()?,
                    output,
                });
            }
            Rule::probe_score => {
                let mut parts = item.into_inner();
                let target = parts.next().unwrap().as_str()[1..].to_string();
                query.stages.push(Stage::Score {
                    target,
                    declarations: parts.map(declaration).collect::<ProbeResult<_>>()?,
                });
            }
            Rule::probe_nested => {
                let mut parts = item.into_inner();
                let kind = match parts.next().unwrap().as_str() {
                    "collect" => NestedKind::Collect,
                    _ => NestedKind::Optional,
                };
                let imports = parts
                    .next()
                    .unwrap()
                    .into_inner()
                    .map(|item| {
                        if item.as_rule() == Rule::variable {
                            Expr::Variable(item.as_str()[1..].into())
                        } else {
                            Expr::AliasRef(item.as_str().into())
                        }
                    })
                    .collect();
                let alias = parts.next().unwrap().as_str().to_string();
                let child = Query {
                    header: query.header.clone(),
                    stages: Vec::new(),
                    projections: Vec::new(),
                    order: Vec::new(),
                    limit: None,
                };
                let child = parse_items(child, parts.next().unwrap().into_inner())?;
                query.stages.push(Stage::Nested {
                    kind,
                    imports,
                    alias,
                    query: Box::new(child),
                });
            }
            Rule::probe_take => {
                let mut parts = item.into_inner();
                let target = parts.next().unwrap().as_str()[1..].to_string();
                let keys = parts
                    .next()
                    .unwrap()
                    .into_inner()
                    .map(value)
                    .collect::<ProbeResult<_>>()?;
                let next = parts.next().unwrap();
                let (order, count) = if next.as_rule() == Rule::probe_order {
                    (order(next)?, core(parts.next().unwrap())?)
                } else {
                    (Vec::new(), core(next)?)
                };
                query.stages.push(Stage::Take(Take {
                    target,
                    keys,
                    order,
                    count,
                }));
            }
            Rule::probe_return => {
                query.projections = item
                    .into_inner()
                    .map(projection)
                    .collect::<ProbeResult<_>>()?;
            }
            Rule::probe_let => query.stages.push(Stage::Let(
                item.into_inner()
                    .map(projection)
                    .collect::<ProbeResult<_>>()?,
            )),
            Rule::probe_group => {
                // The two braced projection lists have distinct typed roles.
                let mut parts = item.into_inner();
                let keys = parts
                    .next()
                    .unwrap()
                    .into_inner()
                    .map(projection)
                    .collect::<ProbeResult<_>>()?;
                let reductions = parts
                    .next()
                    .unwrap()
                    .into_inner()
                    .map(projection)
                    .collect::<ProbeResult<_>>()?;
                query.stages.push(Stage::Group { keys, reductions });
            }
            Rule::probe_select => {
                let mut parts = item.into_inner();
                let order = order(parts.next().unwrap())?;
                let count = core(parts.next().unwrap().into_inner().next().unwrap())?;
                query.stages.push(Stage::Select { order, count });
            }
            Rule::probe_order => {
                query.order = order(item)?;
            }
            Rule::probe_limit => query.limit = Some(core(item.into_inner().next().unwrap())?),
            _ => unreachable!(),
        }
    }
    Ok(query)
}

// IDs are semantic references within one typed plan; names are just lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SourceId {
    scope: usize,
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
    candidates: Option<Bound>,
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
        function: ReduceOp,
        input: Box<ValueType>,
    },
    Materialized(Box<ValueType>),
    Object {
        fields: Vec<(String, ValueType)>,
        nullable: bool,
    },
    Collection(Box<ValueType>),
    Computed {
        scalar: PropType,
        inputs: Vec<ValueType>,
    },
}

impl ValueType {
    fn scalar(&self) -> Option<PropType> {
        match self {
            Self::Core(ResolvedType::Scalar(ty)) | Self::Computed { scalar: ty, .. } => {
                Some(ty.clone())
            }
            Self::Reduced {
                function: ReduceOp::Count | ReduceOp::CountIf | ReduceOp::CountDistinct,
                ..
            } => Some(PropType::scalar(ScalarType::I64, false)),
            Self::Materialized(value) => value.scalar(),
            _ => None,
        }
    }

    fn contains_reduction(&self) -> bool {
        match self {
            Self::Reduced { .. } => true,
            Self::Computed { inputs, .. } => inputs.iter().any(Self::contains_reduction),
            _ => false,
        }
    }

    fn metric_origins(&self) -> Vec<SourceId> {
        match self {
            Self::Metric { source, .. } => vec![*source],
            Self::Reduced { input, .. } | Self::Materialized(input) => input.metric_origins(),
            Self::Computed { inputs, .. } => inputs.iter().flat_map(Self::metric_origins).collect(),
            Self::Object { fields, .. } => fields
                .iter()
                .flat_map(|(_, ty)| ty.metric_origins())
                .collect(),
            Self::Collection(item) => item.metric_origins(),
            _ => Vec::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum OutputOrder {
    Ranked(SourceId),
    Explicit(Vec<OrderingSpec<ValueType>>),
    Unordered,
}

#[derive(Debug, PartialEq)]
enum OutputScope {
    Bindings,
    Groups { key_projections: Vec<usize> },
}

#[derive(Debug)]
struct CheckedTake {
    input_stage: usize,
    target: String,
    key_types: Vec<ValueType>,
    order: Vec<OrderingSpec<ValueType>>,
    count: Bound,
    // Bindings whose identity is proved constant within each target/key pair.
    determined: BTreeSet<String>,
}

#[derive(Debug)]
struct CheckedGroup {
    input_stage: usize,
    key_types: Vec<ValueType>,
    output_values: BTreeMap<String, ValueType>,
}

#[derive(Debug)]
struct CheckedSelect {
    input_stage: usize,
    order: Vec<OrderingSpec<ValueType>>,
    count: Bound,
}

#[derive(Debug)]
struct CheckedNested {
    kind: NestedKind,
    imports: BTreeMap<String, ValueType>,
    plan: Box<Plan>,
}

#[derive(Debug)]
struct Plan {
    scope: usize,
    query: Query,
    sources: BTreeMap<String, CheckedSource>,
    rank_outputs: BTreeMap<usize, SourceId>,
    selections: BTreeMap<usize, CheckedTake>,
    scopes: Vec<BTreeMap<String, String>>,
    projection_types: Vec<ValueType>,
    output_order: OutputOrder,
    output_scope: OutputScope,
    final_limit: Option<Bound>,
    groups: BTreeMap<usize, CheckedGroup>,
    row_selections: BTreeMap<usize, CheckedSelect>,
    value_scopes: Vec<BTreeMap<String, ValueType>>,
    nested: BTreeMap<usize, CheckedNested>,
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
        candidates: Some(candidates),
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
            if source.kind == SourceKind::LexicalFeature {
                return Err("scorer has no retrieval membership; use feature".into());
            }
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
        Value::Feature(name) => {
            let source = sources.get(name).ok_or("unknown feature")?;
            if source.kind != SourceKind::LexicalFeature {
                return Err("feature requires a scorer, not a retriever".into());
            }
            Ok(ValueType::Metric {
                source: source.id,
                domain: MetricDomain::Bm25Score,
            })
        }
        Value::Aggregate { function, value } => {
            if value.contains_aggregate() {
                return Err("nested aggregates are not supported".into());
            }
            let input = value_type(catalog, prefix, value, sources, aliases)?;
            if input.contains_reduction() {
                return Err("nested aggregates through aliases are not supported".into());
            }
            let function = match function.as_str() {
                "count" => ReduceOp::Count,
                "count_if"
                    if input
                        .scalar()
                        .is_some_and(|ty| !ty.list && ty.scalar == ScalarType::Bool) =>
                {
                    ReduceOp::CountIf
                }
                "count_distinct"
                    if orderable(&input)
                        || matches!(input, ValueType::Core(ResolvedType::Node(_))) =>
                {
                    ReduceOp::CountDistinct
                }
                "min" => ReduceOp::Min,
                "max" => ReduceOp::Max,
                // Adding or averaging ranks or uncalibrated scores needs an
                // explicit domain contract, not the underlying float type.
                "sum" | "avg"
                    if input
                        .scalar()
                        .is_some_and(|ty| !ty.list && ty.scalar.is_numeric()) =>
                {
                    if function == "sum" {
                        ReduceOp::Sum
                    } else {
                        ReduceOp::Avg
                    }
                }
                _ => return Err("prototype has no domain contract for this aggregate".into()),
            };
            let orderable = match &input {
                ValueType::Metric { .. } => true,
                _ if input
                    .scalar()
                    .is_some_and(|ty| !ty.list && ty.scalar.is_orderable()) =>
                {
                    true
                }
                _ => false,
            };
            if !matches!(
                function,
                ReduceOp::Count | ReduceOp::CountIf | ReduceOp::CountDistinct
            ) && !orderable
            {
                return Err("min/max requires an orderable scalar or metric".into());
            }
            Ok(ValueType::Reduced {
                function,
                input: Box::new(input),
            })
        }
        Value::IsNull(value) => Ok(ValueType::Computed {
            scalar: PropType::scalar(ScalarType::Bool, false),
            inputs: vec![value_type(catalog, prefix, value, sources, aliases)?],
        }),
        Value::Not(value) => {
            let input = value_type(catalog, prefix, value, sources, aliases)?;
            let ty = input
                .scalar()
                .filter(|t| !t.list && t.scalar == ScalarType::Bool)
                .ok_or("not requires Bool")?;
            Ok(ValueType::Computed {
                scalar: ty,
                inputs: vec![input],
            })
        }
        Value::Binary { op, left, right } => {
            let l = value_type(catalog, prefix, left, sources, aliases)?;
            let r = value_type(catalog, prefix, right, sources, aliases)?;
            let scalar = binary_type(catalog, *op, &l, &r)?;
            Ok(ValueType::Computed {
                scalar,
                inputs: vec![l, r],
            })
        }
    }
}

fn binary_type(
    catalog: &Catalog,
    op: BinaryOp,
    left: &ValueType,
    right: &ValueType,
) -> ProbeResult<PropType> {
    let scalar_pair = left.scalar().zip(right.scalar());
    if let BinaryOp::Compare(comparison) = op {
        if let Some((l, r)) = scalar_pair {
            // Reuse production comparison/containment type rules with typed
            // parameters; these synthetic bindings never enter a query plan.
            let query = QueryDecl {
                name: "comparison_type_probe".into(),
                description: None,
                instruction: None,
                params: [("left", l.clone()), ("right", r.clone())]
                    .into_iter()
                    .map(|(name, ty)| Param {
                        name: name.into(),
                        type_name: ty.display_name().trim_end_matches('?').into(),
                        nullable: ty.nullable,
                    })
                    .collect(),
                match_clause: vec![Clause::Filter(Filter {
                    left: Expr::Variable("left".into()),
                    op: comparison,
                    right: Expr::Variable("right".into()),
                })],
                return_clause: Vec::new(),
                order_clause: Vec::new(),
                limit: None,
                mutations: Vec::new(),
            };
            context(catalog, &query)?;
            return Ok(PropType::scalar(ScalarType::Bool, l.nullable || r.nullable));
        }
        if matches!(
            comparison,
            CompOp::Contains | CompOp::StartsWith | CompOp::StringContains
        ) {
            return Err("text/list predicate requires ordinary scalar operands".into());
        }
        let threshold = |metric: &ValueType, scalar: &ValueType| {
            let ValueType::Metric { domain, .. } = metric else {
                return false;
            };
            scalar.scalar().is_some_and(|ty| {
                !ty.list
                    && match domain {
                        MetricDomain::Rank => matches!(
                            ty.scalar,
                            ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64
                        ),
                        _ => ty.scalar.is_numeric(),
                    }
            })
        };
        if (matches!(left, ValueType::Metric { .. }) && left == right)
            || threshold(left, right)
            || threshold(right, left)
        {
            return Ok(PropType::scalar(ScalarType::Bool, true));
        }
        if matches!(comparison, CompOp::Eq | CompOp::Ne)
            && matches!((left, right), (ValueType::Identity(l), ValueType::Identity(r)) if l == r)
        {
            return Ok(PropType::scalar(ScalarType::Bool, false));
        }
        return Err(
            "comparison requires compatible metric domains/origins or scalar operands".into(),
        );
    }
    let (l, r) = scalar_pair.ok_or("arithmetic/Boolean operators cannot erase a metric domain")?;
    if l.list || r.list {
        return Err("operator requires scalar operands".into());
    }
    let scalar = match op {
        BinaryOp::And | BinaryOp::Or if l.scalar == ScalarType::Bool && r.scalar == ScalarType::Bool => ScalarType::Bool,
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide
            if l.scalar.is_numeric() && l.scalar == r.scalar => {
                if op == BinaryOp::Divide && !matches!(l.scalar, ScalarType::F32 | ScalarType::F64) {
                    return Err("integer division requires an explicit future numeric policy".into());
                }
                l.scalar
            }
        _ => return Err("operator requires compatible numeric or Bool operands; implicit numeric casts are not qualified".into()),
    };
    Ok(PropType::scalar(scalar, l.nullable || r.nullable))
}

fn orderable(ty: &ValueType) -> bool {
    match ty {
        ValueType::Identity(_) | ValueType::Metric { .. } => true,
        ValueType::Core(ResolvedType::Scalar(ty)) => !ty.list && ty.scalar.is_orderable(),
        ValueType::Computed { scalar, .. } => !scalar.list && scalar.scalar.is_orderable(),
        ValueType::Reduced {
            function: ReduceOp::Count | ReduceOp::CountIf | ReduceOp::CountDistinct,
            ..
        } => true,
        ValueType::Reduced { input, .. } | ValueType::Materialized(input) => orderable(input),
        _ => false,
    }
}

fn pair_bindings(catalog: &Catalog, ctx: &TypeContext, take: &Take) -> BTreeSet<String> {
    let mut determined = BTreeSet::from([take.target.clone()]);
    let mut grouped_properties: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for key in &take.keys {
        match key {
            Value::Identity(variable) => {
                determined.insert(variable.clone());
            }
            Value::Core(Expr::PropAccess { variable, property }) => {
                grouped_properties
                    .entry(variable)
                    .or_default()
                    .insert(property);
            }
            _ => {}
        }
    }
    // A complete non-null key/unique tuple determines the entity too. A
    // partial composite key or nullable unique tuple does not: the null bucket
    // can contain several identities even when all non-null values are unique.
    for (variable, properties) in grouped_properties {
        let (key, unique, schema) = match &ctx.bindings[variable] {
            BoundVariable::Node { type_name } => {
                let node = &catalog.node_types[type_name];
                (&node.key, &node.unique_constraints, &node.properties)
            }
            BoundVariable::Edge { type_name } => {
                let edge = catalog.lookup_edge_by_name(type_name).unwrap();
                (&edge.key, &edge.unique_constraints, &edge.properties)
            }
        };
        if key.iter().chain(unique.iter()).any(|members| {
            !members.is_empty()
                && members.iter().all(|member| {
                    properties.contains(member.as_str())
                        && schema.get(member).is_some_and(|ty| !ty.nullable)
                })
        }) {
            determined.insert(variable.to_string());
        }
    }
    // Close over proven one-hop relationships, using native edge direction.
    // An undirected edge identity alone does not fix the orientation of two
    // same-type endpoint variables, and a hop range can have several targets.
    loop {
        let before = determined.len();
        for traversal in &ctx.traversals {
            if traversal.min_hops != 1 || traversal.max_hops != Some(1) {
                continue;
            }
            let (from, to) = match traversal.direction {
                Direction::Out => (&traversal.src, &traversal.dst),
                Direction::In => (&traversal.dst, &traversal.src),
                Direction::Both => continue,
            };
            if traversal
                .edge_binding
                .as_ref()
                .is_some_and(|edge| determined.contains(edge))
            {
                determined.insert(from.clone());
                determined.insert(to.clone());
            }
            let edge = catalog.lookup_edge_by_name(&traversal.edge_type).unwrap();
            if edge.cardinality.max == Some(1) && determined.contains(from) {
                determined.insert(to.clone());
                if let Some(binding) = &traversal.edge_binding {
                    determined.insert(binding.clone());
                }
            }
        }
        if determined.len() == before {
            return determined;
        }
    }
}

fn pair_constant(
    value: &Value,
    prefix: &QueryDecl,
    take: &Take,
    determined: &BTreeSet<String>,
    sources: &BTreeMap<String, CheckedSource>,
) -> bool {
    if take.keys.contains(value) {
        return true;
    }
    match value {
        Value::Core(Expr::Literal(_) | Expr::Now) => true,
        Value::Core(Expr::Variable(name)) => prefix.params.iter().any(|p| p.name == *name),
        Value::Core(Expr::PropAccess { variable, .. }) | Value::Identity(variable) => {
            determined.contains(variable)
        }
        Value::Metric { source, .. } => determined.contains(&sources[source].target),
        Value::Feature(source) => determined.contains(&sources[source].target),
        Value::Aggregate { .. } => true, // value_type already validates the explicit per-pair reduction.
        Value::Binary { left, right, .. } => {
            pair_constant(left, prefix, take, determined, sources)
                && pair_constant(right, prefix, take, determined, sources)
        }
        Value::Not(value) | Value::IsNull(value) => {
            pair_constant(value, prefix, take, determined, sources)
        }
        _ => false,
    }
}

fn check_take(
    catalog: &Catalog,
    prefix: &QueryDecl,
    index: usize,
    take: &Take,
    active_order: Option<SourceId>,
    sources: &BTreeMap<String, CheckedSource>,
) -> ProbeResult<CheckedTake> {
    let ctx = context(catalog, prefix)?;
    if take.target == "_" || !ctx.bindings.contains_key(&take.target) {
        return Err("take requires an existing named node or edge target".into());
    }
    let aliases = BTreeMap::new();
    let mut key_types = Vec::new();
    for key in &take.keys {
        let ty = value_type(catalog, prefix, key, sources, &aliases)?;
        if !orderable(&ty) || ty.contains_reduction() {
            return Err(
                "group key requires an identity or orderable scalar, without aggregation".into(),
            );
        }
        key_types.push(ty);
    }
    let determined = pair_bindings(catalog, &ctx, take);
    let raw_order = if take.order.is_empty() {
        let source =
            active_order.ok_or("take needs an explicit order when no ranking order is in scope")?;
        let alias = sources
            .iter()
            .find(|(_, s)| s.id == source)
            .unwrap()
            .0
            .clone();
        vec![OrderingSpec {
            value: Value::Metric {
                source: alias,
                field: "rank".into(),
            },
            descending: false,
            nulls_first: false,
        }]
    } else {
        take.order.clone()
    };
    let mut order = Vec::new();
    for comparator in raw_order {
        let ty = value_type(catalog, prefix, &comparator.value, sources, &aliases)?;
        if !orderable(&ty) {
            return Err("take comparator must be orderable".into());
        }
        if !pair_constant(&comparator.value, prefix, take, &determined, sources) {
            return Err("cannot prove comparator constant within each target/group pair; request an explicit reduction".into());
        }
        order.push(OrderingSpec {
            value: ty,
            descending: comparator.descending,
            nulls_first: comparator.nulls_first,
        });
    }
    order.push(OrderingSpec {
        value: ValueType::Identity(take.target.clone()),
        descending: false,
        nulls_first: false,
    });
    Ok(CheckedTake {
        input_stage: index - 1,
        target: take.target.clone(),
        key_types,
        order,
        count: bound(prefix, &take.count, 0, None)?,
        determined,
    })
}

fn check_match(
    catalog: &Catalog,
    prefix: &mut QueryDecl,
    clauses: &[MatchItem],
    sources: &BTreeMap<String, CheckedSource>,
    values: &BTreeMap<String, ValueType>,
) -> ProbeResult<()> {
    // Graph patterns are declarative inside a block. Resolve all graph
    // bindings before typing predicates, without moving them across stages.
    for clause in clauses {
        if let MatchItem::Graph(c) = clause {
            reject_legacy_clause(c)?;
            prefix.match_clause.push(c.clone());
        }
    }
    context(catalog, prefix)?;
    for clause in clauses {
        match clause {
            MatchItem::Graph(_) => {}
            MatchItem::Terms { field, query } => check_terms(catalog, prefix, field, query, None)?,
            MatchItem::Filter(value) => {
                if value.contains_aggregate() {
                    return Err("aggregate is not a row predicate".into());
                }
                let ty = value_type(catalog, prefix, value, sources, values)?;
                if !ty
                    .scalar()
                    .is_some_and(|t| !t.list && t.scalar == ScalarType::Bool)
                {
                    return Err("match predicate requires Bool".into());
                }
            }
            MatchItem::Negation(inner) => {
                check_match(catalog, &mut prefix.clone(), inner, sources, values)?
            }
        }
    }
    Ok(())
}

fn valid_group_expression(
    value: &Value,
    keys: &[(Value, Option<String>)],
    prefix: &QueryDecl,
) -> bool {
    if keys.iter().any(|(key, _)| key == value) {
        return true;
    }
    match value {
        Value::Aggregate { .. } | Value::Core(Expr::Literal(_)) => true,
        Value::Core(Expr::Variable(name)) => prefix.params.iter().any(|p| &p.name == name),
        Value::Binary { left, right, .. } => {
            valid_group_expression(left, keys, prefix)
                && valid_group_expression(right, keys, prefix)
        }
        Value::Not(value) | Value::IsNull(value) => valid_group_expression(value, keys, prefix),
        _ => false,
    }
}

fn check_group(
    catalog: &Catalog,
    prefix: &mut QueryDecl,
    index: usize,
    keys: &[(Value, Option<String>)],
    reductions: &[(Value, Option<String>)],
    sources: &BTreeMap<String, CheckedSource>,
    values: &mut BTreeMap<String, ValueType>,
) -> ProbeResult<CheckedGroup> {
    let mut bindings = Vec::new();
    let mut key_types = Vec::new();
    let mut output_values = BTreeMap::new();
    let mut retained = BTreeSet::new();
    for (key, alias) in keys {
        if key.contains_aggregate() {
            return Err("group key cannot contain an aggregate".into());
        }
        let ty = value_type(catalog, prefix, key, sources, values)?;
        if let (
            Value::Core(Expr::Variable(variable)),
            ValueType::Core(ResolvedType::Node(type_name)),
        ) = (key, &ty)
        {
            if alias.is_some() {
                return Err(
                    "entity group key retains its binding; project a new result name later".into(),
                );
            }
            if !retained.insert(variable.clone()) {
                return Err("duplicate entity group key".into());
            }
            bindings.push(Clause::Binding(Binding {
                variable: variable.clone(),
                type_name: type_name.clone(),
                prop_matches: Vec::new(),
            }));
        } else {
            if !orderable(&ty) {
                return Err("group key must have qualified equality".into());
            }
            let name = alias
                .as_ref()
                .ok_or("scalar group key requires an output alias")?;
            if output_values
                .insert(name.clone(), ValueType::Materialized(Box::new(ty.clone())))
                .is_some()
            {
                return Err("duplicate group output".into());
            }
        }
        key_types.push(ty);
    }
    for (expression, alias) in reductions {
        if !expression.contains_aggregate() || !valid_group_expression(expression, keys, prefix) {
            return Err("group output requires reductions or explicit group keys; member values cannot escape".into());
        }
        let name = alias
            .as_ref()
            .ok_or("group reduction requires an output alias")?;
        let ty = value_type(catalog, prefix, expression, sources, values)?;
        if output_values
            .insert(name.clone(), ValueType::Materialized(Box::new(ty)))
            .is_some()
        {
            return Err("duplicate group output".into());
        }
    }
    prefix.match_clause = bindings;
    *values = output_values.clone();
    Ok(CheckedGroup {
        input_stage: index - 1,
        key_types,
        output_values,
    })
}

fn check(catalog: &Catalog, query: Query) -> ProbeResult<Plan> {
    check_scope(catalog, query, Vec::new(), BTreeMap::new(), 0, &mut 1)
}

fn check_scope(
    catalog: &Catalog,
    query: Query,
    imported_bindings: Vec<Clause>,
    mut row_values: BTreeMap<String, ValueType>,
    scope: usize,
    next_scope: &mut usize,
) -> ProbeResult<Plan> {
    let mut prefix = query.header.clone();
    prefix.match_clause = imported_bindings;
    let mut sources = BTreeMap::new();
    let mut all_sources = BTreeMap::new();
    let mut rank_outputs = BTreeMap::new();
    let mut selections = BTreeMap::new();
    let mut scopes = Vec::new();
    let mut active_order = None;
    let mut row_order = None;
    let mut value_scopes = Vec::new();
    let mut groups = BTreeMap::new();
    let mut row_selections = BTreeMap::new();
    let mut nested = BTreeMap::new();
    for (index, stage) in query.stages.iter().enumerate() {
        match stage {
            Stage::Match(clauses) => {
                check_match(catalog, &mut prefix, clauses, &sources, &row_values)?;
            }
            Stage::Rank {
                target,
                declarations,
                output,
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
                    if all_sources.contains_key(&declaration.alias) {
                        return Err("duplicate stage alias".into());
                    }
                    let id = SourceId {
                        scope,
                        block: index,
                        ordinal,
                    };
                    let source = check_source(catalog, &prefix, declaration, id, target, &sources)?;
                    all_sources.insert(declaration.alias.clone(), source.clone());
                    sources.insert(declaration.alias.clone(), source);
                }
                let selected = sources.get(output).ok_or("unknown rank output")?;
                if selected.id.block != index {
                    return Err("output must belong to the current rank block".into());
                }
                rank_outputs.insert(index, selected.id);
                active_order = Some(selected.id);
                row_order = None;
            }
            Stage::Take(take) => {
                selections.insert(
                    index,
                    check_take(catalog, &prefix, index, take, active_order, &sources)?,
                );
            }
            Stage::Let(items) => {
                let mut additions = BTreeMap::new();
                for (expression, alias) in items {
                    if expression.contains_aggregate() {
                        return Err("let preserves rows; use group for reductions".into());
                    }
                    let name = alias.as_ref().ok_or("let requires an output alias")?;
                    let ty = value_type(catalog, &prefix, expression, &sources, &row_values)?;
                    if row_values.contains_key(name)
                        || additions
                            .insert(name.clone(), ValueType::Materialized(Box::new(ty)))
                            .is_some()
                    {
                        return Err("duplicate computed binding".into());
                    }
                }
                row_values.extend(additions);
            }
            Stage::Group { keys, reductions } => {
                groups.insert(
                    index,
                    check_group(
                        catalog,
                        &mut prefix,
                        index,
                        keys,
                        reductions,
                        &sources,
                        &mut row_values,
                    )?,
                );
                sources.clear();
                active_order = None;
                row_order = None;
            }
            Stage::Select { order, count } => {
                let mut checked = Vec::new();
                for item in order {
                    if item.value.contains_aggregate() {
                        return Err(
                            "select uses the current row scope; reduce before selecting".into()
                        );
                    }
                    let ty = value_type(catalog, &prefix, &item.value, &sources, &row_values)?;
                    if !orderable(&ty) {
                        return Err("select requires orderable values".into());
                    }
                    checked.push(OrderingSpec {
                        value: ty,
                        descending: item.descending,
                        nulls_first: item.nulls_first,
                    });
                }
                active_order = None;
                row_order = Some(checked.clone());
                row_selections.insert(
                    index,
                    CheckedSelect {
                        input_stage: index - 1,
                        order: checked,
                        count: bound(&prefix, count, 0, None)?,
                    },
                );
            }
            Stage::Score {
                target,
                declarations,
            } => {
                if target == "_" || !context(catalog, &prefix)?.bindings.contains_key(target) {
                    return Err("score requires a bound target".into());
                }
                for (ordinal, declaration) in declarations.iter().enumerate() {
                    if all_sources.contains_key(&declaration.alias) {
                        return Err("duplicate stage alias".into());
                    }
                    let Source::Lexical { field, query } = &declaration.source else {
                        return Err("this scorer prototype requires lexical input".into());
                    };
                    allowed(&declaration.options, &["scoring"])?;
                    if declaration
                        .options
                        .get("scoring")
                        .is_some_and(|v| !matches!(v, Expr::AliasRef(name) if name == "bm25_v1"))
                    {
                        return Err("unknown lexical scoring policy".into());
                    }
                    check_terms(catalog, &prefix, field, query, Some(target))?;
                    let source = CheckedSource {
                        id: SourceId {
                            scope,
                            block: index,
                            ordinal,
                        },
                        input_stage: index - 1,
                        target: target.clone(),
                        kind: SourceKind::LexicalFeature,
                        candidates: None,
                        arms: Vec::new(),
                        declaration: declaration.clone(),
                    };
                    all_sources.insert(declaration.alias.clone(), source.clone());
                    sources.insert(declaration.alias.clone(), source);
                }
                // A feature evaluates the incoming targets; it neither selects
                // membership nor changes the active comparator.
            }
            Stage::Nested {
                kind,
                imports,
                alias,
                query: child,
            } => {
                if row_values.contains_key(alias) {
                    return Err("duplicate nested output".into());
                }
                let ctx = context(catalog, &prefix)?;
                let mut imported_bindings = Vec::new();
                let mut imported_values = BTreeMap::new();
                let mut imported_types = BTreeMap::new();
                for import in imports {
                    let (name, ty) = match import {
                        Expr::Variable(name) => {
                            let Some(BoundVariable::Node { type_name }) = ctx.bindings.get(name)
                            else {
                                return Err(
                                    "nested binding import requires a bound node in this prototype"
                                        .into(),
                                );
                            };
                            imported_bindings.push(Clause::Binding(Binding {
                                variable: name.clone(),
                                type_name: type_name.clone(),
                                prop_matches: Vec::new(),
                            }));
                            (
                                format!("${name}"),
                                ValueType::Core(ResolvedType::Node(type_name.clone())),
                            )
                        }
                        Expr::AliasRef(name) => {
                            let ty = row_values
                                .get(name)
                                .ok_or("unknown nested value import")?
                                .clone();
                            imported_values.insert(name.clone(), ty.clone());
                            (name.clone(), ty)
                        }
                        _ => unreachable!(),
                    };
                    if imported_types.insert(name, ty).is_some() {
                        return Err("duplicate nested import".into());
                    }
                }
                let child_scope = *next_scope;
                *next_scope += 1;
                let child_plan = check_scope(
                    catalog,
                    *child.clone(),
                    imported_bindings,
                    imported_values,
                    child_scope,
                    next_scope,
                )?;
                for bindings in &child_plan.scopes {
                    for name in bindings.keys() {
                        if ctx.bindings.contains_key(name)
                            && !imported_types.contains_key(&format!("${name}"))
                        {
                            return Err("outer binding requires an explicit nested import; implicit shadowing is not admitted".into());
                        }
                    }
                }
                if *kind == NestedKind::Collect
                    && (child_plan.final_limit.is_none()
                        || child_plan.output_order == OutputOrder::Unordered)
                {
                    return Err(
                        "collection requires an explicit output limit and local order".into(),
                    );
                }
                let fields = child_plan
                    .query
                    .projections
                    .iter()
                    .zip(&child_plan.projection_types)
                    .map(|((_, alias), ty)| {
                        Ok((
                            alias.clone().ok_or("nested output fields require names")?,
                            ty.clone(),
                        ))
                    })
                    .collect::<ProbeResult<Vec<_>>>()?;
                let object = ValueType::Object {
                    fields,
                    nullable: *kind == NestedKind::Optional,
                };
                let output = match kind {
                    NestedKind::Collect => ValueType::Collection(Box::new(object)),
                    NestedKind::Optional => object,
                };
                row_values.insert(alias.clone(), output);
                nested.insert(
                    index,
                    CheckedNested {
                        kind: *kind,
                        imports: imported_types,
                        plan: Box::new(child_plan),
                    },
                );
            }
        }
        value_scopes.push(row_values.clone());
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
        .any(|(value, _)| value.contains_aggregate());
    let mut aliases = BTreeMap::new();
    let mut projection_types = Vec::new();
    for (value, alias) in &query.projections {
        let ty = value_type(catalog, &prefix, value, &sources, &row_values)?;
        if let Some(alias) = alias {
            if aliases.insert(alias.clone(), ty.clone()).is_some() {
                return Err("duplicate result alias".into());
            }
        }
        projection_types.push(ty);
    }
    let mut explicit_order = Vec::new();
    if !aggregate {
        for (name, ty) in &row_values {
            aliases.entry(name.clone()).or_insert_with(|| ty.clone());
        }
    }
    for ordering in &query.order {
        if aggregate && !ordering.value.uses_only_output_values() {
            return Err(
                "aggregate ordering must reference a projected result alias in this prototype"
                    .into(),
            );
        }
        let ty = value_type(catalog, &prefix, &ordering.value, &sources, &aliases)?;
        if !orderable(&ty) {
            return Err("order requires orderable values".into());
        }
        explicit_order.push(OrderingSpec {
            value: ty,
            descending: ordering.descending,
            nulls_first: ordering.nulls_first,
        });
    }
    if aggregate {
        active_order = None;
        row_order = None;
    }
    let output_order = if !explicit_order.is_empty() {
        OutputOrder::Explicit(explicit_order)
    } else if let Some(source) = active_order {
        OutputOrder::Ranked(source)
    } else if let Some(order) = row_order {
        OutputOrder::Explicit(order)
    } else {
        OutputOrder::Unordered
    };
    let output_scope = if aggregate {
        OutputScope::Groups {
            key_projections: projection_types
                .iter()
                .enumerate()
                .filter_map(|(index, ty)| (!ty.contains_reduction()).then_some(index))
                .collect(),
        }
    } else {
        OutputScope::Bindings
    };
    let final_limit = query
        .limit
        .as_ref()
        .map(|expr| bound(&query.header, expr, 0, None))
        .transpose()?;
    Ok(Plan {
        scope,
        query,
        sources: all_sources,
        rank_outputs,
        selections,
        scopes,
        projection_types,
        output_order,
        output_scope,
        final_limit,
        groups,
        row_selections,
        value_scopes,
        nested,
    })
}

const SCHEMA: &str = r#"
node Organization { slug: String @key name: String category: String? embedding: Vector(3) }
node Incident { slug: String @key title: String severity: I32 labels: [String] embedding: Vector(3) }
edge HasIncident: Organization -> Incident { note: String }
"#;

fn catalog() -> Catalog {
    build_catalog(&parse_schema(SCHEMA).unwrap()).unwrap()
}

const STAGED: &str = r#"
query staged($q: String, $vector: Vector(3), $window: I64) {
  match { $o: Organization }
  rank $o {
    lexical($o.name, terms($q, max_edits: 1), candidates: $window) as words
    knn($o.embedding, $vector, candidates: 100) as meaning
    rrf(arm(words), arm(meaning, weight: 1.5), candidates: 20) as combined
    yield combined
  }
  match { $o hasIncident $i $i.title contains "outage" }
  rank $i { lexical($i.title, terms($q), candidates: 5) as incidents
    yield incidents
  }
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
        Some(Bound::Parameter {
            name: "window".into(),
            min: 1,
            max: Some(10_000)
        })
    );
    assert_eq!(plan.sources["meaning"].input_stage, 0);
    assert_eq!(
        plan.sources["combined"].arms,
        [words.id, plan.sources["meaning"].id]
    );
    assert_eq!(plan.sources["incidents"].input_stage, 2);
    assert_eq!(
        plan.rank_outputs,
        BTreeMap::from([
            (1, plan.sources["combined"].id),
            (3, plan.sources["incidents"].id)
        ])
    );
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
    assert_eq!(
        order[0],
        OrderingSpec {
            value: plan.projection_types[2].clone(),
            descending: false,
            nulls_first: true
        }
    );
    assert_eq!(
        order[1],
        OrderingSpec {
            value: plan.projection_types[3].clone(),
            descending: true,
            nulls_first: false
        }
    );
    assert_eq!(order[0].value.metric_origins(), [words.id]);
    assert_eq!(
        order[1].value.metric_origins(),
        [plan.sources["incidents"].id]
    );
    assert_eq!(plan.final_limit, Some(Bound::Literal(3)));
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
  rank $link { lexical($link.note, terms($q), candidates: 5) as edges
    yield edges
  }
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
  rank $o { lexical($o.name, terms($q), candidates: 10) as original
    yield original
  }
  match { $o.name contains "selected" }
  rank $o { lexical($o.name, terms($q), candidates: 5) as later
    yield later
  }
  return { $o.slug, metric(original, rank) as original_rank, metric(later, rank) as later_rank }
}
"#;
    let catalog = catalog();
    let plan = check(&catalog, parse(input).unwrap()).unwrap();
    assert_eq!(plan.sources["original"].input_stage, 0);
    assert_eq!(plan.sources["later"].input_stage, 2);
    assert_ne!(plan.projection_types[1], plan.projection_types[2]);
    let invalid = input.replace(
        "as later\n    yield later\n  }",
        "as later rrf(arm(original), arm(later), candidates: 5) as reopened yield reopened }",
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
  rank $i { lexical($i.title, terms($q), candidates: 10) as hits
    yield hits
  }
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
            function: ReduceOp::Min,
            input: Box::new(ValueType::Metric {
                source: plan.sources["hits"].id,
                domain: MetricDomain::Rank
            }),
        }
    );
    let OutputOrder::Explicit(order) = plan.output_order else {
        panic!("expected aggregate order");
    };
    assert_eq!(order[0].value, plan.projection_types[1]);
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
            function: ReduceOp::Sum,
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
fn staged_probe_take_preserves_population_metrics_and_global_order() {
    let input = STAGED.replace(
        "  return {",
        "  take $i { per { $o.slug, $o.category } limit $window }\n  return {",
    );
    let plan = check(&catalog(), parse(&input).unwrap()).unwrap();
    let selection = &plan.selections[&4];
    assert_eq!(selection.input_stage, 3);
    assert_eq!(selection.target, "i");
    assert_eq!(
        selection.determined,
        BTreeSet::from(["i".into(), "o".into()])
    );
    assert_eq!(
        selection.key_types,
        [false, true].map(|nullable| {
            ValueType::Core(ResolvedType::Scalar(PropType::scalar(
                ScalarType::String,
                nullable,
            )))
        })
    );
    assert_eq!(
        selection.count,
        Bound::Parameter {
            name: "window".into(),
            min: 0,
            max: None
        }
    );
    assert_eq!(
        selection.order,
        [
            OrderingSpec {
                value: ValueType::Metric {
                    source: plan.sources["incidents"].id,
                    domain: MetricDomain::Rank
                },
                descending: false,
                nulls_first: false,
            },
            OrderingSpec {
                value: ValueType::Identity("i".into()),
                descending: false,
                nulls_first: false
            },
        ]
    );
    assert_eq!(plan.scopes[4], plan.scopes[3]);
    assert_eq!(
        plan.sources["incidents"].candidates,
        Some(Bound::Literal(5))
    );
    assert_eq!(plan.final_limit, Some(Bound::Literal(3)));
    assert_eq!(plan.output_scope, OutputScope::Bindings);

    // A local comparator chooses winners, without becoming a global order.
    // The full organization key makes its earlier metric constant per pair.
    let explicit = input
        .replace(
            "limit $window }",
            "order { metric(words, rank) asc nulls last } limit $window }",
        )
        .replace("  order { words asc, score desc, $i.@id }", "");
    let explicit_plan = check(&catalog(), parse(&explicit).unwrap()).unwrap();
    assert_eq!(
        explicit_plan.selections[&4].order[0].value.metric_origins(),
        [plan.sources["words"].id]
    );
    assert!(!explicit_plan.selections[&4].order[0].nulls_first);
    assert_eq!(
        explicit_plan.output_order,
        OutputOrder::Ranked(plan.sources["incidents"].id)
    );

    let after = input.replace(
        "  return {",
        "  rank $i { lexical($i.title, terms($q), candidates: 2) as after_take yield after_take }\n  return {",
    );
    let after_plan = check(&catalog(), parse(&after).unwrap()).unwrap();
    assert_eq!(after_plan.sources["after_take"].input_stage, 4);
    assert_ne!(
        after_plan.sources["after_take"].id,
        plan.sources["incidents"].id
    );
}

#[test]
fn staged_probe_take_validates_pairs_and_explicit_reductions() {
    let input = STAGED.replace("  return {", "  take $o { per { $o.category } order { min(metric(incidents, rank)) asc nulls last } limit 2 }\n  return {");
    let plan = check(&catalog(), parse(&input).unwrap()).unwrap();
    assert_eq!(
        plan.selections[&4].order[0].value,
        ValueType::Reduced {
            function: ReduceOp::Min,
            input: Box::new(ValueType::Metric {
                source: plan.sources["incidents"].id,
                domain: MetricDomain::Rank
            }),
        }
    );
    for (from, to, error) in [
        (
            "min(metric(incidents, rank))",
            "metric(incidents, rank)",
            "cannot prove comparator constant",
        ),
        (
            "order { min(metric(incidents, rank)) asc nulls last }",
            "",
            "cannot prove comparator constant",
        ),
        ("take $o", "take $unknown", "existing named"),
        ("take $o", "take $_", "existing named"),
        ("per { $o.category }", "per { $o }", "group key requires"),
        (
            "per { $o.category }",
            "per { $i.embedding }",
            "group key requires",
        ),
        (
            "per { $o.category }",
            "per { $i.labels }",
            "group key requires",
        ),
        (
            "per { $o.category }",
            "per { count($i) }",
            "group key requires",
        ),
        ("limit 2 }", "limit -1 }", "expected probe_not"),
        (
            "limit 2 }",
            "limit 1.5 }",
            "integer literal or query parameter",
        ),
        ("limit 2 }", "limit $q }", "non-null integer"),
        (
            "limit 2 }",
            "limit $i.severity }",
            "integer literal or query parameter",
        ),
        (
            "min(metric(incidents, rank))",
            "min(count($i))",
            "nested aggregates",
        ),
        (
            "min(metric(incidents, rank))",
            "sum(metric(incidents, rank))",
            "no domain contract",
        ),
    ] {
        let changed = input.replace(from, to);
        assert_ne!(changed, input);
        let actual = parse(&changed)
            .and_then(|query| check(&catalog(), query))
            .unwrap_err();
        assert!(actual.contains(error), "{from} -> {to}: {actual}");
    }
    let nullable = input
        .replace("$window: I64", "$window: I64?")
        .replace("limit 2 }", "limit $window }");
    assert!(
        check(&catalog(), parse(&nullable).unwrap())
            .unwrap_err()
            .contains("non-null integer")
    );
}

#[test]
fn staged_probe_take_uses_complete_nonnull_keys_and_directed_cardinality() {
    let input = STAGED.replace(
        "  return {",
        "  take $i { per { $o.slug } order { metric(words, rank) } limit 2 }\n  return {",
    );
    let composite = SCHEMA.replace(
        "slug: String @key name:",
        "slug: String region: String @key(region, slug) name:",
    );
    let composite_catalog = build_catalog(&parse_schema(&composite).unwrap()).unwrap();
    assert!(
        check(&composite_catalog, parse(&input).unwrap())
            .unwrap_err()
            .contains("cannot prove comparator constant")
    );
    let full = input.replace("per { $o.slug }", "per { $o.region, $o.slug }");
    assert!(
        check(&composite_catalog, parse(&full).unwrap())
            .unwrap()
            .selections[&4]
            .determined
            .contains("o")
    );

    for nullable in [false, true] {
        let schema = SCHEMA.replace(
            "category: String?",
            if nullable {
                "category: String? @unique"
            } else {
                "category: String @unique"
            },
        );
        let catalog = build_catalog(&parse_schema(&schema).unwrap()).unwrap();
        let by_unique = input.replace("per { $o.slug }", "per { $o.category }");
        let checked = check(&catalog, parse(&by_unique).unwrap());
        assert_eq!(
            checked.is_ok(),
            !nullable,
            "nullable unique bucket may contain multiple identities"
        );
    }

    let schema = SCHEMA.replace("-> Incident {", "-> Incident @card(0..1) {");
    let cardinality_catalog = build_catalog(&parse_schema(&schema).unwrap()).unwrap();
    let by_source = input
        .replace("take $i", "take $o")
        .replace("per { $o.slug }", "per { $o.category }")
        .replace(
            "order { metric(words, rank) }",
            "order { metric(incidents, rank) }",
        );
    let plan = check(&cardinality_catalog, parse(&by_source).unwrap()).unwrap();
    assert!(plan.selections[&4].determined.contains("i"));
    let reverse = input.replace("per { $o.slug }", "per { $i.slug }");
    assert!(
        check(&cardinality_catalog, parse(&reverse).unwrap())
            .unwrap_err()
            .contains("cannot prove comparator constant")
    );

    // A directed, bound edge fixes its endpoints even without a max-one edge.
    let edge = input
        .replace("$o hasIncident $i", "$o $link:hasIncident $i")
        .replace("take $i", "take $link")
        .replace("per { $o.slug }", "per { $o.category }");
    assert_eq!(
        check(&catalog(), parse(&edge).unwrap()).unwrap().selections[&4].determined,
        BTreeSet::from(["i".into(), "link".into(), "o".into()])
    );
}

#[test]
fn staged_probe_take_graph_only_zero_quota_and_null_placement() {
    let input = r#"
query graph_only() {
  match { $o: Organization $o hasIncident $i }
  take $i { per { $o.category } order { $i.severity desc nulls first } limit 0 }
  return { $i.slug }
}
"#;
    let plan = check(&catalog(), parse(input).unwrap()).unwrap();
    assert!(plan.sources.is_empty());
    assert_eq!(plan.output_order, OutputOrder::Unordered);
    assert_eq!(plan.selections[&1].count, Bound::Literal(0));
    assert!(plan.selections[&1].order[0].nulls_first);
    let missing = input.replace("order { $i.severity desc nulls first }", "");
    assert!(
        check(&catalog(), parse(&missing).unwrap())
            .unwrap_err()
            .contains("explicit order when no ranking")
    );
    let final_order = input.replace(
        "return { $i.slug }",
        "return { $i.slug } order { $o.category asc nulls last }",
    );
    let OutputOrder::Explicit(order) = check(&catalog(), parse(&final_order).unwrap())
        .unwrap()
        .output_order
    else {
        panic!("expected explicit order")
    };
    assert!(!order[0].nulls_first);
    assert!(!order[0].descending);
    assert!(parse_query(input).is_err());
}

#[test]
fn staged_probe_rank_output_is_explicit_and_independent_of_unused_sources() {
    let input = r#"
query chosen($q: String, $v: Vector(3)) {
  match { $o: Organization }
  rank $o {
    lexical($o.name, terms($q), candidates: 7) as words
    knn($o.embedding, $v, candidates: 11) as meaning
    yield words
  }
  return { $o.slug, metric(words, rank) as rank }
}
"#;
    let catalog = catalog();
    let before = check(&catalog, parse(input).unwrap()).unwrap();
    let changed = input.replace(
        "yield words",
        "lexical($o.name, terms($q, max_edits: 1), candidates: 2) as extra yield words",
    );
    let after = check(&catalog, parse(&changed).unwrap()).unwrap();
    for plan in [&before, &after] {
        assert_eq!(
            plan.rank_outputs,
            BTreeMap::from([(1, plan.sources["words"].id)])
        );
        assert_eq!(
            plan.output_order,
            OutputOrder::Ranked(plan.sources["words"].id)
        );
        assert_eq!(plan.sources["words"].candidates, Some(Bound::Literal(7)));
        assert!(plan.sources.values().all(|source| source.input_stage == 0));
    }
    assert_eq!(before.projection_types, after.projection_types);
    assert_eq!(before.scopes, after.scopes);
    assert_eq!(before.output_scope, after.output_scope);
    assert!(parse_query(input).is_err());

    let reordered = input.replace(
        "lexical($o.name, terms($q), candidates: 7) as words\n    knn($o.embedding, $v, candidates: 11) as meaning",
        "knn($o.embedding, $v, candidates: 11) as meaning\n    lexical($o.name, terms($q), candidates: 7) as words",
    );
    assert_ne!(input, reordered);
    let plan = check(&catalog, parse(&reordered).unwrap()).unwrap();
    assert_eq!(plan.rank_outputs[&1], plan.sources["words"].id);
    assert_eq!(plan.sources["words"].candidates, Some(Bound::Literal(7)));

    // The source alias may itself be a contextual keyword.
    let keyword = input.replace("words", "yield");
    let plan = check(&catalog, parse(&keyword).unwrap()).unwrap();
    assert_eq!(
        plan.output_order,
        OutputOrder::Ranked(plan.sources["yield"].id)
    );

    for replacement in [
        "",
        "yield missing",
        "yield words yield meaning",
        "yieldwords",
    ] {
        let invalid = input.replace("yield words", replacement);
        assert!(
            parse(&invalid).and_then(|q| check(&catalog, q)).is_err(),
            "{invalid}"
        );
    }
    // Appending an unused source still validates its operands and bounds.
    let invalid = changed.replace("candidates: 2) as extra", "candidates: 0) as extra");
    assert!(
        check(&catalog, parse(&invalid).unwrap())
            .unwrap_err()
            .contains("bound out of range")
    );
    let prior_block = input.replace(
        "return {",
        "rank $o { lexical($o.name, terms($q), candidates: 3) as later yield words } return {",
    );
    assert!(
        check(&catalog, parse(&prior_block).unwrap())
            .unwrap_err()
            .contains("output must belong to the current rank block")
    );
}

#[test]
fn staged_probe_shared_expressions_preserve_types_origins_and_precedence() {
    let input = r#"
query expressions($q: String, $threshold: I64, $missing: Bool?) {
  match { $o: Organization }
  rank $o {
    lexical($o.name, terms($q), candidates: 10) as words
    yield words
  }
  match { metric(words, rank) <= $threshold and not($missing) }
  return {
    1 + 2 * 3 as amount,
    is_null(metric(words, rank)) as absent,
    metric(words, rank) as rank
  }
  order { amount + 1 desc, rank }
}
"#;
    let plan = check(&catalog(), parse(input).unwrap()).unwrap();
    assert_eq!(
        plan.projection_types[0].scalar(),
        Some(PropType::scalar(ScalarType::I64, false))
    );
    assert_eq!(
        plan.projection_types[1].scalar(),
        Some(PropType::scalar(ScalarType::Bool, false))
    );
    assert_eq!(
        plan.projection_types[1].metric_origins(),
        [plan.sources["words"].id]
    );
    let Value::Binary { op, right, .. } = &plan.query.projections[0].0 else {
        panic!("expected additive expression");
    };
    assert_eq!(*op, BinaryOp::Add);
    assert!(matches!(right.as_ref(), Value::Binary { op, .. } if *op == BinaryOp::Multiply));
    for (params, expression, scalar, nullable) in [
        ("$a: I64?, $b: I64", "$a - $b", ScalarType::I64, true),
        ("$a: F64, $b: F64", "$a / $b", ScalarType::F64, false),
        ("$a: Bool?, $b: Bool", "$a and $b", ScalarType::Bool, true),
    ] {
        let input = format!(
            "query numeric({params}) {{ match {{ $o: Organization }} return {{ {expression} as value }} }}"
        );
        let plan = check(&catalog(), parse(&input).unwrap()).unwrap();
        assert_eq!(
            plan.projection_types[0].scalar(),
            Some(PropType::scalar(scalar, nullable))
        );
    }
    let grouped = input.replace("1 + 2 * 3", "(1 + 2) * 3");
    let grouped = check(&catalog(), parse(&grouped).unwrap()).unwrap();
    assert!(
        matches!(&grouped.query.projections[0].0, Value::Binary { op, .. } if *op == BinaryOp::Multiply)
    );
    let chain = input.replace("1 + 2 * 3", "9 - 3 - 1");
    let chain = check(&catalog(), parse(&chain).unwrap()).unwrap();
    assert!(
        matches!(&chain.query.projections[0].0, Value::Binary { op, left, .. }
        if *op == BinaryOp::Subtract && matches!(left.as_ref(), Value::Binary { op, .. } if *op == BinaryOp::Subtract))
    );
    for (from, to) in [
        ("1 + 2 * 3", "metric(words, rank) + 1"),
        ("<= $threshold", "<= metric(words, score)"),
        ("and not($missing)", "and $q"),
        ("<= $threshold", "<= $threshold <= 3"),
        (
            "metric(words, rank) <= $threshold",
            "min(metric(words, rank)) <= $threshold",
        ),
        ("is_null(metric(words, rank))", "is_null($unknown.name)"),
        ("1 + 2 * 3", "unknown(1)"),
        ("1 + 2 * 3", "1 + 2.0"),
        ("1 + 2 * 3", "3 / 2"),
        ("is_null(metric(words, rank))", "amount"),
    ] {
        let invalid = input.replace(from, to);
        assert_ne!(input, invalid);
        assert!(
            parse(&invalid).and_then(|q| check(&catalog(), q)).is_err(),
            "{invalid}"
        );
    }
    assert!(parse_query(input).is_err());

    let parameter_limit = input.replace(
        "order { amount + 1 desc, rank }",
        "order { amount + 1 desc, rank } limit $threshold",
    );
    let bounded = check(&catalog(), parse(&parameter_limit).unwrap()).unwrap();
    assert_eq!(
        bounded.final_limit,
        Some(Bound::Parameter {
            name: "threshold".into(),
            min: 0,
            max: None
        })
    );
    for invalid in [
        parameter_limit.replace("$threshold: I64", "$threshold: I64?"),
        parameter_limit.replace("limit $threshold", "limit $q"),
        parameter_limit.replace("limit $threshold", "limit $o.name"),
    ] {
        assert!(parse(&invalid).and_then(|q| check(&catalog(), q)).is_err());
    }
    // A filter's textual position within a graph block does not change scope.
    let before_binding = input.replace(
        "match { $o: Organization }",
        "match { $o.name contains $q $o: Organization }",
    );
    assert!(check(&catalog(), parse(&before_binding).unwrap()).is_ok());
    // Graph absence has its own local bindings; scalar negation only takes a value.
    let negation = input.replace("match { metric(words, rank) <= $threshold and not($missing) }", "match { not { $o hasIncident $hidden metric(words, rank) <= $threshold and $hidden.severity > 2 } }");
    let absent = check(&catalog(), parse(&negation).unwrap()).unwrap();
    assert!(!absent.scopes.last().unwrap().contains_key("hidden"));
    let leaking = negation.replace("1 + 2 * 3", "$hidden.severity");
    assert!(check(&catalog(), parse(&leaking).unwrap()).is_err());
    // Contextual operators/constructors remain usable as result aliases.
    for alias in [
        "yield",
        "not",
        "and",
        "or",
        "rank",
        "take",
        "is_null",
        "true_value",
    ] {
        let keyword = format!(
            "query aliases() {{ match {{ $o: Organization }} return {{ $o.name as {alias} }} order {{ {alias} asc }} }}"
        );
        check(&catalog(), parse(&keyword).unwrap()).unwrap();
    }
    let grouped = input.replace("1 + 2 * 3 as amount", "count($o) + 1 as amount");
    let grouped = check(&catalog(), parse(&grouped).unwrap()).unwrap();
    assert_eq!(
        grouped.output_scope,
        OutputScope::Groups {
            key_projections: vec![1, 2]
        }
    );
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
        7,
        "review new examples when extending the prototype"
    );
    for example in examples {
        let query = parse(example).unwrap();
        let selected_catalog = if query.header.name.starts_with("composition_") {
            composition_catalog()
        } else {
            catalog.clone()
        };
        let plan = check(&selected_catalog, query).unwrap();
        assert!(!plan.query.projections.is_empty());
        assert!(
            parse_query(example).is_err(),
            "RFC syntax is not shipped by this prototype"
        );
    }
}

fn composition_catalog() -> Catalog {
    build_catalog(
        &parse_schema(
            r#"
node Service { slug: String @key }
node Incident { slug: String @key period: String }
node Passage { slug: String @key text: String embedding: Vector(3) }
node Project { slug: String @key }
node Person { slug: String @key name: String }
edge HasIncident: Service -> Incident
edge HasReport: Service -> Passage
edge InProject: Passage -> Project
edge OwnedBy: Service -> Person @card(0..1)
"#,
        )
        .unwrap(),
    )
    .unwrap()
}

fn composition_example(name: &str) -> &'static str {
    include_str!("../../../../docs/rfcs/0048-search-contracts.md")
        .split("```gq\n")
        .skip(1)
        .map(|part| part.split_once("```").unwrap().0)
        .find(|source| source.starts_with(&format!("query {name}(")))
        .unwrap()
}

#[test]
fn composition_grouping_exports_entities_and_values_without_member_scope() {
    let catalog = composition_catalog();
    let input = composition_example("composition_c1");
    let plan = check(&catalog, parse(input).unwrap()).unwrap();
    plan::assert_golden(&plan, include_str!("staged_probe/composition_c1.json"));
    assert_eq!(plan.groups[&1].input_stage, 0);
    assert_eq!(
        plan.groups[&1].key_types,
        [ValueType::Core(ResolvedType::Node("Service".into()))]
    );
    assert_eq!(
        plan.scopes[1],
        BTreeMap::from([("s".into(), "Node<Service>".into())])
    );
    assert_eq!(
        plan.groups[&1]
            .output_values
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["current_count", "prior_count"]
    );
    assert_eq!(
        plan.value_scopes[2]["increase"].scalar(),
        Some(PropType::scalar(ScalarType::I64, false))
    );
    assert_eq!(plan.row_selections[&3].input_stage, 2);
    assert_eq!(plan.row_selections[&3].count, Bound::Literal(1));
    assert_eq!(
        plan.row_selections[&3].order[1].value,
        ValueType::Identity("s".into())
    );
    assert_eq!(plan.sources["reports"].input_stage, 4);
    assert_eq!(plan.sources["reports"].candidates, Some(Bound::Literal(2)));
    assert_eq!(plan.value_scopes[5], plan.value_scopes[3]);
    for (from, to) in [
        ("current_count - prior_count", "$i.period"),
        ("per { $s }", "per { $s.@id as service_id }"),
        ("count_if($i.period = \"prior\")", "$i.period"),
        ("count_if($i.period = \"prior\")", "count_if($i.period)"),
        (
            "let { current_count - prior_count as increase }",
            "let { count($s) as increase }",
        ),
        (
            "let { current_count - prior_count as increase }",
            "let { current_count - prior_count as increase, increase + 1 as extra }",
        ),
    ] {
        let invalid = input.replace(from, to);
        assert_ne!(input, invalid);
        assert!(
            parse(&invalid).and_then(|q| check(&catalog, q)).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn composition_retrieval_then_group_preserves_population_and_reduction_origin() {
    let catalog = composition_catalog();
    let input = composition_example("composition_c2");
    let plan = check(&catalog, parse(input).unwrap()).unwrap();
    plan::assert_golden(&plan, include_str!("staged_probe/composition_c2.json"));
    assert_eq!(plan.sources["candidates"].input_stage, 0);
    assert_eq!(plan.groups[&3].input_stage, 2);
    assert_eq!(
        plan.scopes[3],
        BTreeMap::from([("project".into(), "Node<Project>".into())])
    );
    for name in ["binding_rows", "passages"] {
        assert_eq!(
            plan.value_scopes[3][name].scalar(),
            Some(PropType::scalar(ScalarType::I64, false))
        );
    }
    assert_eq!(
        plan.value_scopes[3]["best_rank"].metric_origins(),
        [plan.sources["candidates"].id]
    );
    let without_order = input.replace("order { $project.@id asc }", "");
    assert_eq!(
        check(&catalog, parse(&without_order).unwrap())
            .unwrap()
            .output_order,
        OutputOrder::Unordered
    );
    for (from, to) in [
        (
            "best_rank as best_rank",
            "metric(candidates, rank) as best_rank",
        ),
        ("passages as passages", "$p.slug as passages"),
        (
            "min(metric(candidates, rank))",
            "count($p) + metric(candidates, rank)",
        ),
        ("count_distinct($p.@id)", "count_distinct($p.embedding)"),
    ] {
        let invalid = input.replace(from, to);
        assert!(
            parse(&invalid).and_then(|q| check(&catalog, q)).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn composition_scoring_adds_a_feature_without_source_membership_or_selection() {
    let catalog = composition_catalog();
    let input = composition_example("composition_c3");
    let plan = check(&catalog, parse(input).unwrap()).unwrap();
    plan::assert_golden(&plan, include_str!("staged_probe/composition_c3.json"));
    assert_eq!(
        plan.output_order,
        OutputOrder::Ranked(plan.sources["dense"].id)
    );
    assert_eq!(
        plan.rank_outputs,
        BTreeMap::from([(1, plan.sources["dense"].id)])
    );
    assert_eq!(
        plan.sources["words_feature"].kind,
        SourceKind::LexicalFeature
    );
    assert_eq!(plan.sources["words_feature"].candidates, None);
    assert_eq!(plan.sources["words_feature"].input_stage, 1);
    assert_eq!(plan.scopes[1], plan.scopes[2]);
    assert_ne!(plan.projection_types[2], plan.projection_types[3]);
    assert_eq!(
        plan.projection_types[3].metric_origins(),
        [plan.sources["words_feature"].id]
    );
    for (from, to) in [
        ("feature(words_feature)", "metric(words_feature, rank)"),
        ("feature(words_feature)", "feature(words)"),
        ("scoring: bm25_v1", "candidates: 1"),
        ("score $p", "score $unknown"),
        (
            "feature(words_feature)",
            "feature(words_feature) + metric(words, score)",
        ),
    ] {
        let invalid = input.replace(from, to);
        assert!(
            parse(&invalid).and_then(|q| check(&catalog, q)).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn composition_nested_results_have_explicit_imports_and_independent_source_scopes() {
    let catalog = composition_catalog();
    let input = composition_example("composition_c4");
    let plan = check(&catalog, parse(input).unwrap()).unwrap();
    plan::assert_golden(&plan, include_str!("staged_probe/composition_c4.json"));
    assert_eq!(plan.nested.len(), 2);
    let owner = &plan.nested[&4];
    let reports = &plan.nested[&5];
    assert_eq!(owner.kind, NestedKind::Optional);
    assert_eq!(reports.kind, NestedKind::Collect);
    assert_eq!(owner.imports, reports.imports);
    assert_eq!(owner.imports.keys().collect::<Vec<_>>(), ["$s"]);
    assert_eq!(plan.scopes[3], plan.scopes[5]);
    assert_eq!(
        plan.value_scopes[3]["increase"],
        plan.value_scopes[5]["increase"]
    );
    assert_eq!(
        plan.projection_types[4],
        ValueType::Object {
            fields: vec![(
                "person".into(),
                ValueType::Core(ResolvedType::Node("Person".into()))
            )],
            nullable: true,
        }
    );
    assert_eq!(
        plan.projection_types[5],
        ValueType::Collection(Box::new(ValueType::Object {
            fields: vec![
                ("id".into(), ValueType::Identity("p".into())),
                (
                    "text".into(),
                    ValueType::Core(ResolvedType::Scalar(PropType::scalar(
                        ScalarType::String,
                        false
                    )))
                ),
            ],
            nullable: false,
        }))
    );
    assert_eq!(reports.plan.final_limit, Some(Bound::Literal(2)));
    assert!(plan.sources.is_empty());
    assert_eq!(reports.plan.sources["relevant"].id.scope, 2);
    assert!(matches!(plan.output_order, OutputOrder::Explicit(_)));

    let imported = input
        .replace("collect ($s)", "collect ($s, increase)")
        .replace("$p.text as text", "increase as increase");
    check(&catalog, parse(&imported).unwrap()).unwrap();

    let child = input
        .split_once("  collect ($s)")
        .unwrap()
        .1
        .split_once("\n  return {")
        .unwrap()
        .0;
    let with_sibling = input.replacen(
        "\n  return {\n    $s as service",
        &format!(
            "\n  collect ($s){}\n  return {{\n    $s as service",
            child.replacen("as reports", "as more_reports", 1)
        ),
        1,
    );
    assert_ne!(with_sibling, input);
    let sibling_plan = check(&catalog, parse(&with_sibling).unwrap()).unwrap();
    assert_ne!(
        sibling_plan.nested[&5].plan.sources["relevant"].id,
        sibling_plan.nested[&6].plan.sources["relevant"].id
    );

    for (from, to) in [
        ("optional ($s)", "optional ()"),
        ("collect ($s)", "collect ()"),
        ("collect ($s)", "collect ($s, $s)"),
        ("collect ($s)", "collect ($s, missing)"),
        ("$p.text as text", "increase as text"),
        ("    limit 2\n", ""),
        ("$p.@id as id, $p.text as text", "$p.@id, $p.text as text"),
        ("reports as reports", "$p.text as reports"),
        ("reports as reports", "metric(relevant, rank) as reports"),
        ("as reports {", "as owner {"),
        ("metric(relevant, rank) asc, $p.@id asc", "$p asc"),
    ] {
        let invalid = input.replace(from, to);
        assert_ne!(invalid, input);
        assert!(
            parse(&invalid).and_then(|q| check(&catalog, q)).is_err(),
            "{invalid}"
        );
    }
}
