use std::collections::HashMap;

use pest::Parser;
use pest::error::InputLocation;
use pest_derive::Parser;

use crate::error::{
    NanoError, ParseDiagnostic, Result, SourceSpan, decode_string_literal, render_span,
};
use crate::types::{PropType, ScalarType};

use super::ast::*;

#[derive(Parser)]
#[grammar = "schema/schema.pest"]
struct SchemaParser;

pub fn parse_schema(input: &str) -> Result<SchemaFile> {
    parse_schema_diagnostic(input).map_err(|e| NanoError::Parse(e.to_string()))
}

pub fn parse_schema_diagnostic(input: &str) -> std::result::Result<SchemaFile, ParseDiagnostic> {
    let pairs = SchemaParser::parse(Rule::schema_file, input).map_err(pest_error_to_diagnostic)?;

    let mut declarations = Vec::new();
    for pair in pairs {
        if pair.as_rule() == Rule::schema_file {
            for inner in pair.into_inner() {
                if let Rule::schema_decl = inner.as_rule() {
                    declarations.push(parse_schema_decl(inner).map_err(nano_error_to_diagnostic)?);
                }
            }
        }
    }

    // Collect interfaces for resolution (clone to avoid borrow conflict)
    let interfaces: Vec<InterfaceDecl> = declarations
        .iter()
        .filter_map(|d| match d {
            SchemaDecl::Interface(i) => Some(i.clone()),
            _ => None,
        })
        .collect();

    // Resolve implements clauses on nodes
    let iface_refs: Vec<&InterfaceDecl> = interfaces.iter().collect();
    for decl in &mut declarations {
        if let SchemaDecl::Node(node) = decl {
            resolve_interfaces(node, &iface_refs).map_err(nano_error_to_diagnostic)?;
        }
    }

    let schema = SchemaFile { declarations };
    validate_schema_annotations(&schema).map_err(nano_error_to_diagnostic)?;
    validate_constraints(&schema).map_err(nano_error_to_diagnostic)?;
    Ok(schema)
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

fn parse_schema_decl(pair: pest::iterators::Pair<Rule>) -> Result<SchemaDecl> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::interface_decl => Ok(SchemaDecl::Interface(parse_interface_decl(inner)?)),
        Rule::node_decl => Ok(SchemaDecl::Node(parse_node_decl(inner)?)),
        Rule::edge_decl => Ok(SchemaDecl::Edge(parse_edge_decl(inner)?)),
        _ => Err(NanoError::Parse(format!(
            "unexpected rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_interface_decl(pair: pest::iterators::Pair<Rule>) -> Result<InterfaceDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut properties = Vec::new();
    for item in inner {
        if let Rule::prop_decl = item.as_rule() {
            properties.push(parse_prop_decl(item)?);
        }
    }

    Ok(InterfaceDecl { name, properties })
}

fn parse_node_decl(pair: pest::iterators::Pair<Rule>) -> Result<NodeDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut annotations = Vec::new();
    let mut implements = Vec::new();
    let mut properties = Vec::new();
    let mut constraints = Vec::new();

    for item in inner {
        match item.as_rule() {
            Rule::annotation => {
                annotations.push(parse_annotation(item)?);
            }
            Rule::implements_clause => {
                for iface in item.into_inner() {
                    if iface.as_rule() == Rule::type_name {
                        implements.push(iface.as_str().to_string());
                    }
                }
            }
            Rule::prop_decl => {
                properties.push(parse_prop_decl(item)?);
            }
            Rule::body_constraint => {
                constraints.push(parse_body_constraint(item)?);
            }
            _ => {}
        }
    }

    // Desugar property-level @key/@unique/@index annotations into constraints
    desugar_property_constraints(&properties, &mut constraints);

    Ok(NodeDecl {
        name,
        annotations,
        implements,
        properties,
        constraints,
    })
}

fn parse_edge_decl(pair: pest::iterators::Pair<Rule>) -> Result<EdgeDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let from_type = inner.next().unwrap().as_str().to_string();
    let to_type = inner.next().unwrap().as_str().to_string();

    let mut cardinality = Cardinality::default();
    let mut annotations = Vec::new();
    let mut properties = Vec::new();
    let mut constraints = Vec::new();

    for item in inner {
        match item.as_rule() {
            Rule::cardinality => {
                cardinality = parse_cardinality(item)?;
            }
            Rule::annotation => annotations.push(parse_annotation(item)?),
            Rule::prop_decl => properties.push(parse_prop_decl(item)?),
            Rule::body_constraint => constraints.push(parse_body_constraint(item)?),
            _ => {}
        }
    }

    // Desugar property-level @unique/@index on edge properties
    desugar_property_constraints(&properties, &mut constraints);

    Ok(EdgeDecl {
        name,
        from_type,
        to_type,
        cardinality,
        annotations,
        properties,
        constraints,
    })
}

fn parse_cardinality(pair: pest::iterators::Pair<Rule>) -> Result<Cardinality> {
    let mut inner = pair.into_inner();
    let min_str = inner.next().unwrap().as_str();
    let min = min_str
        .parse::<u32>()
        .map_err(|_| NanoError::Parse(format!("invalid cardinality min: {}", min_str)))?;
    let max = if let Some(max_pair) = inner.next() {
        let max_str = max_pair.as_str();
        Some(
            max_str
                .parse::<u32>()
                .map_err(|_| NanoError::Parse(format!("invalid cardinality max: {}", max_str)))?,
        )
    } else {
        None
    };

    if let Some(max_val) = max {
        if min > max_val {
            return Err(NanoError::Parse(format!(
                "cardinality min ({}) exceeds max ({})",
                min, max_val
            )));
        }
    }

    Ok(Cardinality { min, max })
}

fn parse_body_constraint(pair: pest::iterators::Pair<Rule>) -> Result<Constraint> {
    let mut inner = pair.into_inner();
    let name_pair = inner.next().unwrap();
    let constraint_name = name_pair.as_str();
    let args_pair = inner.next().unwrap();
    let args: Vec<pest::iterators::Pair<Rule>> = args_pair.into_inner().collect();

    match constraint_name {
        "key" => {
            let names: Vec<String> = args
                .into_iter()
                .filter(|a| a.as_rule() == Rule::ident || a.as_rule() == Rule::constraint_arg)
                .map(|a| extract_ident_from_constraint_arg(a))
                .collect::<Result<Vec<_>>>()?;
            if names.is_empty() {
                return Err(NanoError::Parse(
                    "@key constraint requires at least one property name".to_string(),
                ));
            }
            Ok(Constraint::Key(names))
        }
        "unique" => {
            let names = extract_ident_list_from_args(args)?;
            if names.is_empty() {
                return Err(NanoError::Parse(
                    "@unique constraint requires at least one property name".to_string(),
                ));
            }
            Ok(Constraint::Unique(names))
        }
        "index" => {
            let names = extract_ident_list_from_args(args)?;
            if names.is_empty() {
                return Err(NanoError::Parse(
                    "@index constraint requires at least one property name".to_string(),
                ));
            }
            Ok(Constraint::Index(names))
        }
        "range" => {
            // @range(prop, min..max)
            if args.len() < 2 {
                return Err(NanoError::Parse(
                    "@range requires property name and bounds: @range(prop, min..max)".to_string(),
                ));
            }
            let property = extract_ident_from_constraint_arg(args[0].clone())?;
            // The second arg should be a range_bound
            let (min, max) = extract_range_bounds(&args[1])?;
            Ok(Constraint::Range { property, min, max })
        }
        "check" => {
            // @check(prop, "regex")
            if args.len() < 2 {
                return Err(NanoError::Parse(
                    "@check requires property name and pattern: @check(prop, \"regex\")"
                        .to_string(),
                ));
            }
            let property = extract_ident_from_constraint_arg(args[0].clone())?;
            let pattern = extract_string_from_constraint_arg(&args[1])?;
            Ok(Constraint::Check { property, pattern })
        }
        other => Err(NanoError::Parse(format!("unknown constraint: @{}", other))),
    }
}

fn extract_ident_from_constraint_arg(pair: pest::iterators::Pair<Rule>) -> Result<String> {
    if pair.as_rule() == Rule::ident {
        return Ok(pair.as_str().to_string());
    }
    // constraint_arg wraps ident or literal
    if let Some(inner) = pair.into_inner().next() {
        if inner.as_rule() == Rule::ident {
            return Ok(inner.as_str().to_string());
        }
    }
    Err(NanoError::Parse(
        "expected property name in constraint".to_string(),
    ))
}

fn extract_ident_list_from_args(args: Vec<pest::iterators::Pair<Rule>>) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for arg in args {
        names.push(extract_ident_from_constraint_arg(arg)?);
    }
    Ok(names)
}

fn extract_string_from_constraint_arg(pair: &pest::iterators::Pair<Rule>) -> Result<String> {
    // Navigate into constraint_arg -> literal -> string_lit
    fn find_string(pair: &pest::iterators::Pair<Rule>) -> Result<Option<String>> {
        if pair.as_rule() == Rule::string_lit {
            return decode_string_literal(pair.as_str()).map(Some);
        }
        for inner in pair.clone().into_inner() {
            if let Some(s) = find_string(&inner)? {
                return Ok(Some(s));
            }
        }
        Ok(None)
    }

    find_string(pair)?
        .ok_or_else(|| NanoError::Parse("expected string argument in constraint".to_string()))
}

fn extract_range_bounds(
    pair: &pest::iterators::Pair<Rule>,
) -> Result<(Option<ConstraintBound>, Option<ConstraintBound>)> {
    // Find the range_bound node inside the constraint_arg
    let range_pair = if pair.as_rule() == Rule::range_bound {
        pair.clone()
    } else {
        let mut found = None;
        for inner in pair.clone().into_inner() {
            if inner.as_rule() == Rule::range_bound {
                found = Some(inner);
                break;
            }
        }
        found.ok_or_else(|| {
            NanoError::Parse("expected range bounds (min..max) in @range constraint".to_string())
        })?
    };

    let mut min = None;
    let mut max = None;
    let mut seen_bound = false;

    for child in range_pair.into_inner() {
        if child.as_rule() == Rule::literal
            || child.as_rule() == Rule::integer
            || child.as_rule() == Rule::float_lit
            || child.as_rule() == Rule::signed_integer
            || child.as_rule() == Rule::signed_float
        {
            let bound = parse_constraint_bound(&child)?;
            if !seen_bound {
                min = Some(bound);
                seen_bound = true;
            } else {
                max = Some(bound);
            }
        }
    }

    Ok((min, max))
}

fn parse_constraint_bound(pair: &pest::iterators::Pair<Rule>) -> Result<ConstraintBound> {
    let text = pair.as_str();

    // Try as integer first
    if let Ok(n) = text.parse::<i64>() {
        return Ok(ConstraintBound::Integer(n));
    }
    // Try as float
    if let Ok(f) = text.parse::<f64>() {
        return Ok(ConstraintBound::Float(f));
    }

    // Navigate into literal -> integer/float_lit
    for inner in pair.clone().into_inner() {
        let s = inner.as_str();
        if let Ok(n) = s.parse::<i64>() {
            return Ok(ConstraintBound::Integer(n));
        }
        if let Ok(f) = s.parse::<f64>() {
            return Ok(ConstraintBound::Float(f));
        }
    }

    Err(NanoError::Parse(format!(
        "invalid constraint bound: {}",
        text
    )))
}

/// Desugar property-level @key/@unique/@index annotations into body-level constraints.
fn desugar_property_constraints(properties: &[PropDecl], constraints: &mut Vec<Constraint>) {
    for prop in properties {
        for ann in &prop.annotations {
            match ann.name.as_str() {
                "key" if ann.value.is_none() => {
                    constraints.push(Constraint::Key(vec![prop.name.clone()]));
                }
                "unique" if ann.value.is_none() => {
                    constraints.push(Constraint::Unique(vec![prop.name.clone()]));
                }
                "index" if ann.value.is_none() => {
                    constraints.push(Constraint::Index(vec![prop.name.clone()]));
                }
                _ => {}
            }
        }
    }
}

/// Resolve interface implements clauses — verify properties exist or inject them.
fn resolve_interfaces(node: &mut NodeDecl, interfaces: &[&InterfaceDecl]) -> Result<()> {
    let interface_map: HashMap<&str, &InterfaceDecl> =
        interfaces.iter().map(|i| (i.name.as_str(), *i)).collect();

    for iface_name in &node.implements {
        let iface = interface_map.get(iface_name.as_str()).ok_or_else(|| {
            NanoError::Parse(format!(
                "node {} implements unknown interface '{}'",
                node.name, iface_name
            ))
        })?;

        for iface_prop in &iface.properties {
            if let Some(existing) = node.properties.iter().find(|p| p.name == iface_prop.name) {
                // Property exists — verify type compatibility
                if existing.prop_type != iface_prop.prop_type {
                    return Err(NanoError::Parse(format!(
                        "node {} property '{}' has type {} but interface {} declares it as {}",
                        node.name,
                        iface_prop.name,
                        existing.prop_type.display_name(),
                        iface_name,
                        iface_prop.prop_type.display_name()
                    )));
                }
            } else {
                // Property missing — inject it from the interface
                node.properties.push(iface_prop.clone());
                // Also desugar any constraint annotations from the injected property
                desugar_property_constraints(
                    std::slice::from_ref(iface_prop),
                    &mut node.constraints,
                );
            }
        }
    }

    Ok(())
}

fn parse_prop_decl(pair: pest::iterators::Pair<Rule>) -> Result<PropDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let type_ref = inner.next().unwrap();
    let prop_type = parse_type_ref(type_ref)?;

    let mut annotations = Vec::new();
    for item in inner {
        if let Rule::annotation = item.as_rule() {
            annotations.push(parse_annotation(item)?);
        }
    }

    Ok(PropDecl {
        name,
        prop_type,
        annotations,
    })
}

fn parse_type_ref(pair: pest::iterators::Pair<Rule>) -> Result<PropType> {
    let text = pair.as_str();
    let nullable = text.ends_with('?');

    let mut inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| NanoError::Parse("type reference is missing core type".to_string()))?;
    if inner.as_rule() == Rule::core_type {
        inner = inner
            .into_inner()
            .next()
            .ok_or_else(|| NanoError::Parse("type reference is missing core type".to_string()))?;
    }

    match inner.as_rule() {
        Rule::base_type => {
            let scalar = ScalarType::from_str_name(inner.as_str())
                .ok_or_else(|| NanoError::Parse(format!("unknown type: {}", inner.as_str())))?;
            Ok(PropType::scalar(scalar, nullable))
        }
        Rule::vector_type => {
            let dim_text = inner
                .into_inner()
                .next()
                .ok_or_else(|| NanoError::Parse("Vector type missing dimension".to_string()))?
                .as_str();
            let dim = dim_text
                .parse::<u32>()
                .map_err(|e| NanoError::Parse(format!("invalid Vector dimension: {}", e)))?;
            if dim == 0 {
                return Err(NanoError::Parse(
                    "Vector dimension must be greater than zero".to_string(),
                ));
            }
            if dim > i32::MAX as u32 {
                return Err(NanoError::Parse(format!(
                    "Vector dimension {} exceeds maximum supported {}",
                    dim,
                    i32::MAX
                )));
            }
            Ok(PropType::scalar(ScalarType::Vector(dim), nullable))
        }
        Rule::list_type => {
            let element = inner
                .into_inner()
                .next()
                .ok_or_else(|| NanoError::Parse("list type missing element type".to_string()))?;
            let scalar = ScalarType::from_str_name(element.as_str()).ok_or_else(|| {
                NanoError::Parse(format!("unknown list element type: {}", element.as_str()))
            })?;
            if matches!(scalar, ScalarType::Blob) {
                return Err(NanoError::Parse(
                    "list of Blob is not supported".to_string(),
                ));
            }
            Ok(PropType::list_of(scalar, nullable))
        }
        Rule::enum_type => {
            let mut values = Vec::new();
            for value in inner.into_inner() {
                if value.as_rule() == Rule::enum_value {
                    values.push(value.as_str().to_string());
                }
            }
            if values.is_empty() {
                return Err(NanoError::Parse(
                    "enum type must include at least one value".to_string(),
                ));
            }
            let mut dedup = values.clone();
            dedup.sort();
            dedup.dedup();
            if dedup.len() != values.len() {
                return Err(NanoError::Parse(
                    "enum type cannot include duplicate values".to_string(),
                ));
            }
            Ok(PropType::enum_type(values, nullable))
        }
        other => Err(NanoError::Parse(format!(
            "unexpected type rule: {:?}",
            other
        ))),
    }
}

fn parse_annotation(pair: pest::iterators::Pair<Rule>) -> Result<Annotation> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let value = inner
        .next()
        .map(|p| decode_string_literal(p.as_str()))
        .transpose()?;

    Ok(Annotation { name, value })
}

fn validate_string_annotation(
    annotations: &[Annotation],
    annotation: &str,
    target: &str,
) -> Result<()> {
    let mut seen = false;
    for ann in annotations {
        if ann.name != annotation {
            continue;
        }
        if seen {
            return Err(NanoError::Parse(format!(
                "{} declares @{} multiple times",
                target, annotation
            )));
        }
        let value = ann.value.as_deref().ok_or_else(|| {
            NanoError::Parse(format!(
                "@{} on {} requires a non-empty value",
                annotation, target
            ))
        })?;
        if value.trim().is_empty() {
            return Err(NanoError::Parse(format!(
                "@{} on {} requires a non-empty value",
                annotation, target
            )));
        }
        seen = true;
    }
    Ok(())
}

// ─── Annotation Validation (metadata only) ───────────────────────────────────

fn validate_schema_annotations(schema: &SchemaFile) -> Result<()> {
    for decl in &schema.declarations {
        match decl {
            SchemaDecl::Interface(_) => {} // Interfaces have no type-level annotations
            SchemaDecl::Node(node) => {
                // Reject constraint annotations on node level (must be on properties or as body constraints)
                for ann in &node.annotations {
                    if ann.name == "key"
                        || ann.name == "unique"
                        || ann.name == "index"
                        || ann.name == "embed"
                    {
                        return Err(NanoError::Parse(format!(
                            "@{} is only supported on node properties or as body constraint (node {})",
                            ann.name, node.name
                        )));
                    }
                }
                validate_string_annotation(
                    &node.annotations,
                    "description",
                    &format!("node {}", node.name),
                )?;
                validate_string_annotation(
                    &node.annotations,
                    "instruction",
                    &format!("node {}", node.name),
                )?;

                // Validate property-level annotations
                for prop in &node.properties {
                    validate_property_annotations(prop, &node.name, &node.properties, false)?;
                }
            }
            SchemaDecl::Edge(edge) => {
                for ann in &edge.annotations {
                    if ann.name == "key"
                        || ann.name == "unique"
                        || ann.name == "index"
                        || ann.name == "embed"
                    {
                        return Err(NanoError::Parse(format!(
                            "@{} is not supported on edges (edge {})",
                            ann.name, edge.name
                        )));
                    }
                }
                validate_string_annotation(
                    &edge.annotations,
                    "description",
                    &format!("edge {}", edge.name),
                )?;
                validate_string_annotation(
                    &edge.annotations,
                    "instruction",
                    &format!("edge {}", edge.name),
                )?;

                for prop in &edge.properties {
                    validate_property_annotations(prop, &edge.name, &edge.properties, true)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_property_annotations(
    prop: &PropDecl,
    type_name: &str,
    all_properties: &[PropDecl],
    is_edge: bool,
) -> Result<()> {
    let is_vector = matches!(prop.prop_type.scalar, ScalarType::Vector(_));
    let is_blob = matches!(prop.prop_type.scalar, ScalarType::Blob);

    validate_string_annotation(
        &prop.annotations,
        "description",
        &format!("property {}.{}", type_name, prop.name),
    )?;

    let mut key_seen = false;
    let mut unique_seen = false;
    let mut index_seen = false;
    let mut embed_seen = false;

    for ann in &prop.annotations {
        // List/vector/blob restrictions on property-level annotations
        if prop.prop_type.list
            && (ann.name == "key"
                || ann.name == "unique"
                || ann.name == "index"
                || ann.name == "embed")
        {
            return Err(NanoError::Parse(format!(
                "@{} is not supported on list property {}.{}",
                ann.name, type_name, prop.name
            )));
        }
        if is_vector && (ann.name == "key" || ann.name == "unique") {
            return Err(NanoError::Parse(format!(
                "@{} is not supported on vector property {}.{}",
                ann.name, type_name, prop.name
            )));
        }
        if is_blob
            && (ann.name == "key"
                || ann.name == "unique"
                || ann.name == "index"
                || ann.name == "embed")
        {
            return Err(NanoError::Parse(format!(
                "@{} is not supported on blob property {}.{}",
                ann.name, type_name, prop.name
            )));
        }
        if ann.name == "instruction" {
            return Err(NanoError::Parse(format!(
                "@instruction is only supported on node and edge types (property {}.{})",
                type_name, prop.name
            )));
        }

        // Edge-specific restrictions
        if is_edge && (ann.name == "key" || ann.name == "embed") {
            return Err(NanoError::Parse(format!(
                "@{} is not supported on edge properties (edge {}.{})",
                ann.name, type_name, prop.name
            )));
        }

        // Arity checks
        match ann.name.as_str() {
            "key" => {
                if ann.value.is_some() {
                    return Err(NanoError::Parse(format!(
                        "@key on {}.{} does not accept a value",
                        type_name, prop.name
                    )));
                }
                if key_seen {
                    return Err(NanoError::Parse(format!(
                        "property {}.{} declares @key multiple times",
                        type_name, prop.name
                    )));
                }
                key_seen = true;
            }
            "unique" => {
                if ann.value.is_some() {
                    return Err(NanoError::Parse(format!(
                        "@unique on {}.{} does not accept a value",
                        type_name, prop.name
                    )));
                }
                if unique_seen {
                    return Err(NanoError::Parse(format!(
                        "property {}.{} declares @unique multiple times",
                        type_name, prop.name
                    )));
                }
                unique_seen = true;
            }
            "index" => {
                if ann.value.is_some() {
                    return Err(NanoError::Parse(format!(
                        "@index on {}.{} does not accept a value",
                        type_name, prop.name
                    )));
                }
                if index_seen {
                    return Err(NanoError::Parse(format!(
                        "property {}.{} declares @index multiple times",
                        type_name, prop.name
                    )));
                }
                index_seen = true;
            }
            "embed" => {
                if embed_seen {
                    return Err(NanoError::Parse(format!(
                        "property {}.{} declares @embed multiple times",
                        type_name, prop.name
                    )));
                }
                embed_seen = true;

                if !is_vector {
                    return Err(NanoError::Parse(format!(
                        "@embed is only supported on vector properties ({}.{})",
                        type_name, prop.name
                    )));
                }

                let source_prop = ann.value.as_deref().ok_or_else(|| {
                    NanoError::Parse(format!(
                        "@embed on {}.{} requires a source property name",
                        type_name, prop.name
                    ))
                })?;
                if source_prop.trim().is_empty() {
                    return Err(NanoError::Parse(format!(
                        "@embed on {}.{} requires a non-empty source property name",
                        type_name, prop.name
                    )));
                }

                let source_decl = all_properties
                    .iter()
                    .find(|p| p.name == source_prop)
                    .ok_or_else(|| {
                        NanoError::Parse(format!(
                            "@embed on {}.{} references unknown source property {}",
                            type_name, prop.name, source_prop
                        ))
                    })?;
                if source_decl.prop_type.list || source_decl.prop_type.scalar != ScalarType::String
                {
                    return Err(NanoError::Parse(format!(
                        "@embed source property {}.{} must be String",
                        type_name, source_prop
                    )));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

// ─── Constraint Validation ───────────────────────────────────────────────────

fn validate_constraints(schema: &SchemaFile) -> Result<()> {
    for decl in &schema.declarations {
        match decl {
            SchemaDecl::Interface(_) => {}
            SchemaDecl::Node(node) => {
                validate_type_constraints(&node.constraints, &node.properties, &node.name, false)?;
            }
            SchemaDecl::Edge(edge) => {
                validate_type_constraints(&edge.constraints, &edge.properties, &edge.name, true)?;
            }
        }
    }
    Ok(())
}

fn validate_type_constraints(
    constraints: &[Constraint],
    properties: &[PropDecl],
    type_name: &str,
    is_edge: bool,
) -> Result<()> {
    let prop_names: HashMap<&str, &PropDecl> =
        properties.iter().map(|p| (p.name.as_str(), p)).collect();

    let mut key_count = 0usize;

    for constraint in constraints {
        match constraint {
            Constraint::Key(cols) => {
                if is_edge {
                    return Err(NanoError::Parse(format!(
                        "@key constraint is not supported on edges (edge {})",
                        type_name
                    )));
                }
                key_count += 1;
                if key_count > 1 {
                    return Err(NanoError::Parse(format!(
                        "node type {} has multiple @key constraints; only one is supported",
                        type_name
                    )));
                }
                for col in cols {
                    let prop = prop_names.get(col.as_str()).ok_or_else(|| {
                        NanoError::Parse(format!(
                            "@key on {} references unknown property '{}'",
                            type_name, col
                        ))
                    })?;
                    if prop.prop_type.nullable {
                        return Err(NanoError::Parse(format!(
                            "@key property {}.{} cannot be nullable",
                            type_name, col
                        )));
                    }
                    if prop.prop_type.list {
                        return Err(NanoError::Parse(format!(
                            "@key is not supported on list property {}.{}",
                            type_name, col
                        )));
                    }
                    if matches!(prop.prop_type.scalar, ScalarType::Vector(_)) {
                        return Err(NanoError::Parse(format!(
                            "@key is not supported on vector property {}.{}",
                            type_name, col
                        )));
                    }
                    if matches!(prop.prop_type.scalar, ScalarType::Blob) {
                        return Err(NanoError::Parse(format!(
                            "@key is not supported on blob property {}.{}",
                            type_name, col
                        )));
                    }
                }
            }
            Constraint::Unique(cols) => {
                for col in cols {
                    // Allow "src" and "dst" as implicit edge columns
                    if is_edge && (col == "src" || col == "dst") {
                        continue;
                    }
                    if !prop_names.contains_key(col.as_str()) {
                        return Err(NanoError::Parse(format!(
                            "@unique on {} references unknown property '{}'",
                            type_name, col
                        )));
                    }
                }
            }
            Constraint::Index(cols) => {
                for col in cols {
                    if is_edge && (col == "src" || col == "dst") {
                        continue;
                    }
                    let prop = prop_names.get(col.as_str()).ok_or_else(|| {
                        NanoError::Parse(format!(
                            "@index on {} references unknown property '{}'",
                            type_name, col
                        ))
                    })?;
                    if matches!(prop.prop_type.scalar, ScalarType::Blob) {
                        return Err(NanoError::Parse(format!(
                            "@index is not supported on blob property {}.{}",
                            type_name, col
                        )));
                    }
                }
            }
            Constraint::Range { property, .. } => {
                if is_edge {
                    return Err(NanoError::Parse(format!(
                        "@range constraint is not supported on edges (edge {})",
                        type_name
                    )));
                }
                let prop = prop_names.get(property.as_str()).ok_or_else(|| {
                    NanoError::Parse(format!(
                        "@range on {} references unknown property '{}'",
                        type_name, property
                    ))
                })?;
                if !prop.prop_type.scalar.is_numeric() {
                    return Err(NanoError::Parse(format!(
                        "@range on {}.{} requires a numeric type, got {}",
                        type_name,
                        property,
                        prop.prop_type.display_name()
                    )));
                }
            }
            Constraint::Check { property, .. } => {
                if is_edge {
                    return Err(NanoError::Parse(format!(
                        "@check constraint is not supported on edges (edge {})",
                        type_name
                    )));
                }
                let prop = prop_names.get(property.as_str()).ok_or_else(|| {
                    NanoError::Parse(format!(
                        "@check on {} references unknown property '{}'",
                        type_name, property
                    ))
                })?;
                if prop.prop_type.scalar != ScalarType::String {
                    return Err(NanoError::Parse(format!(
                        "@check on {}.{} requires String type, got {}",
                        type_name,
                        property,
                        prop.prop_type.display_name()
                    )));
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod tests;
