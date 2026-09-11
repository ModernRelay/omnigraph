//! Canonical explanation of the checked logical stages. This is a diagnostic
//! view, not an executable IR or a proposed serialized query-plan format.
use super::*;
use serde_json::{Value as Json, json};

pub(super) fn assert_golden(plan: &Plan, expected: &str) {
    let actual = serde_json::to_string_pretty(&explain(plan, None)).unwrap();
    assert_eq!(actual, expected.trim());
}

fn source_id(id: SourceId) -> String {
    format!("s{}.{}.{}", id.scope, id.block, id.ordinal)
}

fn expression(expr: &Expr) -> String {
    match expr {
        Expr::Variable(name) => format!("${name}"),
        Expr::PropAccess { variable, property } => format!("${variable}.{property}"),
        Expr::AliasRef(name) => name.clone(),
        Expr::Literal(Literal::String(text)) => serde_json::to_string(text).unwrap(),
        Expr::Literal(Literal::Integer(value)) => value.to_string(),
        Expr::Literal(Literal::Float(value)) => value.to_string(),
        Expr::Literal(Literal::Bool(value)) => value.to_string(),
        _ => format!("{expr:?}"),
    }
}

fn value(item: &Value, plan: &Plan) -> String {
    match item {
        Value::Core(expr) => expression(expr),
        Value::Identity(binding) => format!("${binding}.@id"),
        Value::Metric { source, field } => {
            format!("metric({}, {field})", source_id(plan.sources[source].id))
        }
        Value::Feature(source) => format!("feature({})", source_id(plan.sources[source].id)),
        Value::Aggregate { function, value: v } => format!("{function}({})", value(v, plan)),
        Value::Not(v) => format!("not({})", value(v, plan)),
        Value::IsNull(v) => format!("is_null({})", value(v, plan)),
        Value::Binary { op, left, right } => {
            let op = match op {
                BinaryOp::Add => "+".into(),
                BinaryOp::Subtract => "-".into(),
                BinaryOp::Multiply => "*".into(),
                BinaryOp::Divide => "/".into(),
                BinaryOp::And => "and".into(),
                BinaryOp::Or => "or".into(),
                BinaryOp::Compare(op) => op.to_string(),
            };
            format!("({} {op} {})", value(left, plan), value(right, plan))
        }
    }
}

fn value_type(ty: &ValueType) -> String {
    match ty {
        ValueType::Core(ResolvedType::Scalar(ty)) => ty.display_name(),
        ValueType::Core(ty) => format!("{ty:?}"),
        ValueType::Identity(binding) => format!("Identity<${binding}>"),
        ValueType::Metric { source, domain } => format!("{domain:?}<{}>?", source_id(*source)),
        ValueType::Reduced { function, input } => format!(
            "{} <- {function:?}<{}>",
            ty.scalar()
                .map(|t| t.display_name())
                .unwrap_or_else(|| value_type(input)),
            value_type(input)
        ),
        ValueType::Materialized(ty) => value_type(ty),
        ValueType::Computed { scalar, inputs } => format!(
            "{} from [{}]",
            scalar.display_name(),
            inputs.iter().map(value_type).collect::<Vec<_>>().join(", ")
        ),
        ValueType::Object { fields, nullable } => format!(
            "{{{}}}{}",
            fields
                .iter()
                .map(|(name, ty)| format!("{name}: {}", value_type(ty)))
                .collect::<Vec<_>>()
                .join(", "),
            if *nullable { "?" } else { "" }
        ),
        ValueType::Collection(ty) => format!("[{}]", value_type(ty)),
    }
}

fn bound(bound: &Bound) -> Json {
    match bound {
        Bound::Literal(n) => json!(n),
        Bound::Parameter { name, min, max } => json!({"parameter": name, "min": min, "max": max}),
    }
}

fn ordering(order: &[OrderingSpec<Value>], plan: &Plan) -> Json {
    json!(
        order
            .iter()
            .map(|o| json!({
                "value": value(&o.value, plan),
                "direction": if o.descending { "desc" } else { "asc" },
                "nulls": if o.nulls_first { "first" } else { "last" },
            }))
            .collect::<Vec<_>>()
    )
}

fn projections(items: &[(Value, Option<String>)], plan: &Plan) -> Json {
    json!(
        items
            .iter()
            .map(|(v, alias)| json!({"value": value(v, plan), "alias": alias}))
            .collect::<Vec<_>>()
    )
}

fn source(source: &CheckedSource) -> Json {
    let declaration = &source.declaration;
    let mut out = json!({
        "id": source_id(source.id), "alias": declaration.alias,
        "kind": format!("{:?}", source.kind), "target": source.target,
        "eligible_input_stage": source.input_stage,
        "candidates": source.candidates.as_ref().map(bound),
        "options": declaration.options.iter().map(|(k,v)| (k.clone(), expression(v))).collect::<BTreeMap<_,_>>(),
    });
    match &declaration.source {
        Source::Lexical { field, query } => {
            out["field"] = json!(expression(field));
            out["query"] = json!(expression(&query.text));
            out["terms_options"] = json!(
                query
                    .options
                    .iter()
                    .map(|(k, v)| (k.clone(), expression(v)))
                    .collect::<BTreeMap<_, _>>()
            );
            out["statistics"] =
                json!("snapshot-visible field corpus, independent of eligible input");
        }
        Source::Vector { field, query, .. } => {
            out["field"] = json!(expression(field));
            out["query"] = json!(expression(query));
        }
        Source::Fusion(arms) => {
            out["arms"] = json!(arms.iter().zip(&source.arms).map(|((_,options),id)| json!({
                "source": source_id(*id),
                "options": options.iter().map(|(k,v)| (k.clone(),expression(v))).collect::<BTreeMap<_,_>>(),
            })).collect::<Vec<_>>());
        }
    }
    out
}

fn explain(plan: &Plan, parent: Option<Json>) -> Json {
    let relation = |stage: usize| format!("r{}.{}", plan.scope, stage);
    let mut operators = Vec::new();
    for (index, stage) in plan.query.stages.iter().enumerate() {
        let input = index
            .checked_sub(1)
            .map(&relation)
            .unwrap_or_else(|| format!("r{}.input", plan.scope));
        let mut op = json!({"output":relation(index), "input":input});
        match stage {
            Stage::Match(items) => {
                op["operator"] = json!("GraphMatch");
                op["clauses"] = json!(
                    items
                        .iter()
                        .map(|item| match item {
                            MatchItem::Filter(v) => value(v, plan),
                            _ => format!("{item:?}"),
                        })
                        .collect::<Vec<_>>()
                );
                op["multiplicity"] = json!("binding rows; preserve graph paths");
            }
            Stage::Rank {
                target,
                declarations,
                ..
            } => {
                op["operator"] = json!("Rank");
                op["target"] = json!(target);
                op["sources"] = json!(
                    declarations
                        .iter()
                        .map(|d| source(&plan.sources[&d.alias]))
                        .collect::<Vec<_>>()
                );
                op["yield"] = json!(source_id(plan.rank_outputs[&index]));
                op["selection"] =
                    json!("distinct target identities; reattach all incoming bindings of winners");
            }
            Stage::Group { keys, reductions } => {
                op["operator"] = json!("Group");
                op["keys"] = projections(keys, plan);
                op["reductions"] = projections(reductions, plan);
                op["population"] = json!(relation(plan.groups[&index].input_stage));
                op["identity"] =
                    json!("group-key tuple; retained entity keys keep entity identity");
                op["active_order"] = json!("discarded");
            }
            Stage::Let(items) => {
                op["operator"] = json!("Compute");
                op["values"] = projections(items, plan);
                op["semantics"] =
                    json!("all expressions read incoming scope; preserve rows and order");
            }
            Stage::Select { order, .. } => {
                op["operator"] = json!("SelectRows");
                op["order"] = ordering(order, plan);
                op["limit"] = bound(&plan.row_selections[&index].count);
                op["units"] = json!("input rows");
            }
            Stage::Take(take) => {
                op["operator"] = json!("TakePairs");
                op["target"] = json!(take.target);
                op["keys"] = json!(take.keys.iter().map(|v| value(v, plan)).collect::<Vec<_>>());
                op["order"] = ordering(&take.order, plan);
                op["limit"] = bound(&plan.selections[&index].count);
                op["units"] = json!(
                    "distinct target/group pairs; preserve selected bindings and input order"
                );
            }
            Stage::Score {
                target,
                declarations,
            } => {
                op["operator"] = json!("ScoreExistingTargets");
                op["target"] = json!(target);
                op["sources"] = json!(
                    declarations
                        .iter()
                        .map(|d| source(&plan.sources[&d.alias]))
                        .collect::<Vec<_>>()
                );
                op["semantics"] = json!(
                    "preserve target membership, binding rows and input order; create no retrieval rank"
                );
            }
            Stage::Nested { kind, alias, .. } => {
                let nested = &plan.nested[&index];
                op["operator"] = json!(format!("{kind:?}"));
                op["alias"] = json!(alias);
                op["child"] = explain(
                    &nested.plan,
                    Some(json!({
                        "input_relation": input,
                        "correlation": "parent row identity, not only imported entity identity",
                        "imports": nested.imports.iter().map(|(k,v)| (k.clone(),value_type(v))).collect::<BTreeMap<_,_>>(),
                    })),
                );
                op["cardinality"] = json!(match kind {
                    NestedKind::Optional =>
                        "0: null object; 1: present object; >1: typed cardinality refusal",
                    NestedKind::Collect => "ordered binding-row list; zero rows: typed empty list",
                });
                op["semantics"] = json!(
                    "one output per parent row; preserve parent facts and order; share snapshot and budget"
                );
            }
        }
        op["entity_scope"] = json!(plan.scopes[index]);
        op["value_scope"] = json!(
            plan.value_scopes[index]
                .iter()
                .map(|(k, v)| (k.clone(), value_type(v)))
                .collect::<BTreeMap<_, _>>()
        );
        operators.push(op);
    }
    json!({
        "qualification":"checked logical stages; physical execution and resource ownership are not proved by this diagnostic",
        "scope":plan.scope, "correlated_input":parent,
        "snapshot":"inherited accepted snapshot", "budget":"shared whole-query context; never reset per stage or parent",
        "operators":operators,
        "result":{
            "input":relation(plan.query.stages.len()-1),
            "projection":projections(&plan.query.projections,plan),
            "types":plan.projection_types.iter().map(value_type).collect::<Vec<_>>(),
            "explicit_order":ordering(&plan.query.order,plan),
            "active_order":format!("{:?}",plan.output_order),
            "scope":format!("{:?}",plan.output_scope),
            "limit":plan.final_limit.as_ref().map(bound),
        }
    })
}
