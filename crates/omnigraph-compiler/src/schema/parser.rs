use pest::Parser;
use pest::error::InputLocation;
use pest_derive::Parser;

use crate::error::{NanoError, ParseDiagnostic, Result, SourceSpan};
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
    let schema = SchemaFile { declarations };
    validate_schema_annotations(&schema).map_err(nano_error_to_diagnostic)?;
    Ok(schema)
}

fn pest_error_to_diagnostic(err: pest::error::Error<Rule>) -> ParseDiagnostic {
    let span = match err.location {
        InputLocation::Pos(pos) => Some(SourceSpan::new(pos, pos)),
        InputLocation::Span((start, end)) => Some(SourceSpan::new(start, end)),
    };
    ParseDiagnostic::new(err.to_string(), span)
}

fn nano_error_to_diagnostic(err: NanoError) -> ParseDiagnostic {
    ParseDiagnostic::new(err.to_string(), None)
}

fn parse_schema_decl(pair: pest::iterators::Pair<Rule>) -> Result<SchemaDecl> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::node_decl => Ok(SchemaDecl::Node(parse_node_decl(inner)?)),
        Rule::edge_decl => Ok(SchemaDecl::Edge(parse_edge_decl(inner)?)),
        _ => Err(NanoError::Parse(format!(
            "unexpected rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_node_decl(pair: pest::iterators::Pair<Rule>) -> Result<NodeDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut annotations = Vec::new();
    let mut parent = None;
    let mut properties = Vec::new();

    for item in inner {
        match item.as_rule() {
            Rule::annotation => {
                annotations.push(parse_annotation(item)?);
            }
            Rule::type_name => {
                parent = Some(item.as_str().to_string());
            }
            Rule::prop_decl => {
                properties.push(parse_prop_decl(item)?);
            }
            _ => {}
        }
    }

    Ok(NodeDecl {
        name,
        annotations,
        parent,
        properties,
    })
}

fn parse_edge_decl(pair: pest::iterators::Pair<Rule>) -> Result<EdgeDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let from_type = inner.next().unwrap().as_str().to_string();
    let to_type = inner.next().unwrap().as_str().to_string();

    let mut annotations = Vec::new();
    let mut properties = Vec::new();
    for item in inner {
        match item.as_rule() {
            Rule::annotation => annotations.push(parse_annotation(item)?),
            Rule::prop_decl => properties.push(parse_prop_decl(item)?),
            _ => {}
        }
    }

    Ok(EdgeDecl {
        name,
        from_type,
        to_type,
        annotations,
        properties,
    })
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
    let value = inner.next().map(|p| {
        let s = p.as_str();
        s.strip_prefix('"')
            .and_then(|inner| inner.strip_suffix('"'))
            .unwrap_or(s)
            .to_string()
    });

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

fn validate_schema_annotations(schema: &SchemaFile) -> Result<()> {
    for decl in &schema.declarations {
        match decl {
            SchemaDecl::Node(node) => {
                for ann in &node.annotations {
                    if ann.name == "key"
                        || ann.name == "unique"
                        || ann.name == "index"
                        || ann.name == "embed"
                    {
                        return Err(NanoError::Parse(format!(
                            "@{} is only supported on node properties (node {})",
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

                let mut key_count = 0usize;
                for prop in &node.properties {
                    let mut key_seen = false;
                    let mut unique_seen = false;
                    let mut index_seen = false;
                    let mut embed_seen = false;
                    let is_vector = matches!(prop.prop_type.scalar, ScalarType::Vector(_));
                    validate_string_annotation(
                        &prop.annotations,
                        "description",
                        &format!("property {}.{}", node.name, prop.name),
                    )?;
                    for ann in &prop.annotations {
                        if prop.prop_type.list
                            && (ann.name == "key"
                                || ann.name == "unique"
                                || ann.name == "index"
                                || ann.name == "embed")
                        {
                            return Err(NanoError::Parse(format!(
                                "@{} is not supported on list property {}.{}",
                                ann.name, node.name, prop.name
                            )));
                        }
                        if is_vector && (ann.name == "key" || ann.name == "unique") {
                            return Err(NanoError::Parse(format!(
                                "@{} is not supported on vector property {}.{}",
                                ann.name, node.name, prop.name
                            )));
                        }
                        if ann.name == "instruction" {
                            return Err(NanoError::Parse(format!(
                                "@instruction is only supported on node and edge types (property {}.{})",
                                node.name, prop.name
                            )));
                        }
                        if ann.name == "key" {
                            if ann.value.is_some() {
                                return Err(NanoError::Parse(format!(
                                    "@key on {}.{} does not accept a value",
                                    node.name, prop.name
                                )));
                            }
                            if key_seen {
                                return Err(NanoError::Parse(format!(
                                    "property {}.{} declares @key multiple times",
                                    node.name, prop.name
                                )));
                            }
                            key_seen = true;
                            key_count += 1;
                        } else if ann.name == "unique" {
                            if ann.value.is_some() {
                                return Err(NanoError::Parse(format!(
                                    "@unique on {}.{} does not accept a value",
                                    node.name, prop.name
                                )));
                            }
                            if unique_seen {
                                return Err(NanoError::Parse(format!(
                                    "property {}.{} declares @unique multiple times",
                                    node.name, prop.name
                                )));
                            }
                            unique_seen = true;
                        } else if ann.name == "index" {
                            if ann.value.is_some() {
                                return Err(NanoError::Parse(format!(
                                    "@index on {}.{} does not accept a value",
                                    node.name, prop.name
                                )));
                            }
                            if index_seen {
                                return Err(NanoError::Parse(format!(
                                    "property {}.{} declares @index multiple times",
                                    node.name, prop.name
                                )));
                            }
                            index_seen = true;
                        } else if ann.name == "embed" {
                            if embed_seen {
                                return Err(NanoError::Parse(format!(
                                    "property {}.{} declares @embed multiple times",
                                    node.name, prop.name
                                )));
                            }
                            embed_seen = true;

                            if !is_vector {
                                return Err(NanoError::Parse(format!(
                                    "@embed is only supported on vector properties ({}.{})",
                                    node.name, prop.name
                                )));
                            }

                            let source_prop = ann.value.as_deref().ok_or_else(|| {
                                NanoError::Parse(format!(
                                    "@embed on {}.{} requires a source property name",
                                    node.name, prop.name
                                ))
                            })?;
                            if source_prop.trim().is_empty() {
                                return Err(NanoError::Parse(format!(
                                    "@embed on {}.{} requires a non-empty source property name",
                                    node.name, prop.name
                                )));
                            }

                            let source_decl = node
                                .properties
                                .iter()
                                .find(|p| p.name == source_prop)
                                .ok_or_else(|| {
                                    NanoError::Parse(format!(
                                        "@embed on {}.{} references unknown source property {}",
                                        node.name, prop.name, source_prop
                                    ))
                                })?;
                            if source_decl.prop_type.list
                                || source_decl.prop_type.scalar != ScalarType::String
                            {
                                return Err(NanoError::Parse(format!(
                                    "@embed source property {}.{} must be String",
                                    node.name, source_prop
                                )));
                            }
                        }
                    }
                }

                if key_count > 1 {
                    return Err(NanoError::Parse(format!(
                        "node type {} has multiple @key properties; only one is currently supported",
                        node.name
                    )));
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
                    validate_string_annotation(
                        &prop.annotations,
                        "description",
                        &format!("property {}.{}", edge.name, prop.name),
                    )?;
                    for ann in &prop.annotations {
                        if ann.name == "key"
                            || ann.name == "unique"
                            || ann.name == "index"
                            || ann.name == "embed"
                        {
                            return Err(NanoError::Parse(format!(
                                "@{} is not supported on edge properties (edge {}.{})",
                                ann.name, edge.name, prop.name
                            )));
                        }
                        if ann.name == "instruction" {
                            return Err(NanoError::Parse(format!(
                                "@instruction is only supported on node and edge types (property {}.{})",
                                edge.name, prop.name
                            )));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_schema() {
        let input = r#"
node Person {
    name: String
    age: I32?
}

node Company {
    name: String
}

edge Knows: Person -> Person {
    since: Date?
}

edge WorksAt: Person -> Company {
    title: String?
}
"#;
        let schema = parse_schema(input).unwrap();
        assert_eq!(schema.declarations.len(), 4);

        // Check Person node
        match &schema.declarations[0] {
            SchemaDecl::Node(n) => {
                assert_eq!(n.name, "Person");
                assert!(n.annotations.is_empty());
                assert!(n.parent.is_none());
                assert_eq!(n.properties.len(), 2);
                assert_eq!(n.properties[0].name, "name");
                assert!(!n.properties[0].prop_type.nullable);
                assert_eq!(n.properties[1].name, "age");
                assert!(n.properties[1].prop_type.nullable);
            }
            _ => panic!("expected Node"),
        }

        // Check Knows edge
        match &schema.declarations[2] {
            SchemaDecl::Edge(e) => {
                assert_eq!(e.name, "Knows");
                assert_eq!(e.from_type, "Person");
                assert_eq!(e.to_type, "Person");
                assert!(e.annotations.is_empty());
                assert_eq!(e.properties.len(), 1);
            }
            _ => panic!("expected Edge"),
        }
    }

    #[test]
    fn test_parse_inheritance() {
        let input = r#"
node Person {
    name: String
}
node Employee : Person {
    employee_id: String
}
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[1] {
            SchemaDecl::Node(n) => {
                assert_eq!(n.name, "Employee");
                assert_eq!(n.parent.as_deref(), Some("Person"));
            }
            _ => panic!("expected Node"),
        }
    }

    #[test]
    fn test_parse_annotation() {
        let input = r#"
node Person {
    name: String @unique
    id: U64 @key
    handle: String @index
}
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Node(n) => {
                assert_eq!(n.properties[0].annotations.len(), 1);
                assert_eq!(n.properties[0].annotations[0].name, "unique");
                assert_eq!(n.properties[1].annotations[0].name, "key");
                assert_eq!(n.properties[2].annotations[0].name, "index");
            }
            _ => panic!("expected Node"),
        }
    }

    #[test]
    fn test_parse_embed_annotation_identifier_arg() {
        let input = r#"
node Doc {
    title: String
    embedding: Vector(3) @embed(title)
}
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Node(n) => {
                assert_eq!(n.properties[1].annotations.len(), 1);
                assert_eq!(n.properties[1].annotations[0].name, "embed");
                assert_eq!(
                    n.properties[1].annotations[0].value.as_deref(),
                    Some("title")
                );
            }
            _ => panic!("expected Node"),
        }
    }

    #[test]
    fn test_parse_edge_no_body() {
        let input = "edge WorksAt: Person -> Company\n";
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Edge(e) => {
                assert_eq!(e.name, "WorksAt");
                assert!(e.annotations.is_empty());
                assert!(e.properties.is_empty());
            }
            _ => panic!("expected Edge"),
        }
    }

    #[test]
    fn test_parse_type_rename_annotation() {
        let input = r#"
node Account @rename_from("User") {
    full_name: String @rename_from("name")
}

edge ConnectedTo: Account -> Account @rename_from("Knows")
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Node(n) => {
                assert_eq!(n.name, "Account");
                assert_eq!(n.annotations.len(), 1);
                assert_eq!(n.annotations[0].name, "rename_from");
                assert_eq!(n.annotations[0].value.as_deref(), Some("User"));
                assert_eq!(n.properties[0].annotations[0].name, "rename_from");
                assert_eq!(
                    n.properties[0].annotations[0].value.as_deref(),
                    Some("name")
                );
            }
            _ => panic!("expected Node"),
        }
        match &schema.declarations[1] {
            SchemaDecl::Edge(e) => {
                assert_eq!(e.name, "ConnectedTo");
                assert_eq!(e.annotations.len(), 1);
                assert_eq!(e.annotations[0].name, "rename_from");
                assert_eq!(e.annotations[0].value.as_deref(), Some("Knows"));
            }
            _ => panic!("expected Edge"),
        }
    }

    #[test]
    fn test_reject_multiple_node_keys() {
        let input = r#"
node Person {
    id: U64 @key
    ext_id: String @key
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("multiple @key properties"));
    }

    #[test]
    fn test_reject_unique_with_value() {
        let input = r#"
node Person {
    email: String @unique("x")
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("@unique"));
        assert!(err.to_string().contains("does not accept a value"));
    }

    #[test]
    fn test_reject_index_with_value() {
        let input = r#"
node Person {
    email: String @index("x")
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("@index"));
        assert!(err.to_string().contains("does not accept a value"));
    }

    #[test]
    fn test_reject_unique_on_node_annotation() {
        let input = r#"
node Person @unique {
    email: String
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(
            err.to_string()
                .contains("only supported on node properties")
        );
    }

    #[test]
    fn test_reject_index_on_node_annotation() {
        let input = r#"
node Person @index {
    email: String
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(
            err.to_string()
                .contains("only supported on node properties")
        );
    }

    #[test]
    fn test_reject_unique_on_edge_property() {
        let input = r#"
node Person { name: String }
edge Knows: Person -> Person {
    weight: I32 @unique
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("edge properties"));
    }

    #[test]
    fn test_reject_index_on_edge_property() {
        let input = r#"
node Person { name: String }
edge Knows: Person -> Person {
    weight: I32 @index
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("edge properties"));
    }

    #[test]
    fn test_reject_embed_without_source_property() {
        let input = r#"
node Doc {
    title: String
    embedding: Vector(3) @embed
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("requires a source property name"));
    }

    #[test]
    fn test_reject_embed_on_non_vector_property() {
        let input = r#"
node Doc {
    title: String @embed(title)
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(
            err.to_string()
                .contains("only supported on vector properties")
        );
    }

    #[test]
    fn test_reject_embed_unknown_source_property() {
        let input = r#"
node Doc {
    title: String
    embedding: Vector(3) @embed(body)
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(
            err.to_string()
                .contains("references unknown source property")
        );
    }

    #[test]
    fn test_reject_embed_source_not_string() {
        let input = r#"
node Doc {
    body: I32
    embedding: Vector(3) @embed(body)
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("must be String"));
    }

    #[test]
    fn test_reject_embed_on_edge_property() {
        let input = r#"
node Doc { title: String }
edge Linked: Doc -> Doc {
    embedding: Vector(3) @embed(title)
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("edge properties"));
    }

    #[test]
    fn test_parse_enum_and_list_types() {
        let input = r#"
node Ticket {
    status: enum(open, closed, blocked)
    tags: [String]
}
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Node(n) => {
                let status = &n.properties[0].prop_type;
                assert!(status.is_enum());
                assert!(!status.list);
                assert_eq!(
                    status.enum_values.as_ref().unwrap(),
                    &vec![
                        "blocked".to_string(),
                        "closed".to_string(),
                        "open".to_string()
                    ]
                );

                let tags = &n.properties[1].prop_type;
                assert!(tags.list);
                assert!(!tags.is_enum());
                assert_eq!(tags.scalar, ScalarType::String);
            }
            _ => panic!("expected Node"),
        }
    }

    #[test]
    fn test_reject_duplicate_enum_values() {
        let input = r#"
node Ticket {
    status: enum(open, closed, open)
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("duplicate values"));
    }

    #[test]
    fn test_parse_description_and_instruction_annotations() {
        let input = r#"
node Task @description("Tracked work item") @instruction("Prefer querying by slug") {
    slug: String @key @description("Stable external identifier")
}
edge DependsOn: Task -> Task @description("Hard dependency") @instruction("Use only for blockers")
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Node(node) => {
                assert_eq!(
                    node.annotations
                        .iter()
                        .find(|ann| ann.name == "description")
                        .and_then(|ann| ann.value.as_deref()),
                    Some("Tracked work item")
                );
                assert_eq!(
                    node.annotations
                        .iter()
                        .find(|ann| ann.name == "instruction")
                        .and_then(|ann| ann.value.as_deref()),
                    Some("Prefer querying by slug")
                );
                assert_eq!(
                    node.properties[0]
                        .annotations
                        .iter()
                        .find(|ann| ann.name == "description")
                        .and_then(|ann| ann.value.as_deref()),
                    Some("Stable external identifier")
                );
            }
            _ => panic!("expected node"),
        }
        match &schema.declarations[1] {
            SchemaDecl::Edge(edge) => {
                assert_eq!(
                    edge.annotations
                        .iter()
                        .find(|ann| ann.name == "description")
                        .and_then(|ann| ann.value.as_deref()),
                    Some("Hard dependency")
                );
                assert_eq!(
                    edge.annotations
                        .iter()
                        .find(|ann| ann.name == "instruction")
                        .and_then(|ann| ann.value.as_deref()),
                    Some("Use only for blockers")
                );
            }
            _ => panic!("expected edge"),
        }
    }

    #[test]
    fn test_reject_duplicate_description_annotations() {
        let input = r#"
node Task @description("a") @description("b") {
    slug: String @key
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(
            err.to_string()
                .contains("declares @description multiple times")
        );
    }

    #[test]
    fn test_reject_instruction_on_property() {
        let input = r#"
node Task {
    slug: String @instruction("bad")
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(
            err.to_string()
                .contains("@instruction is only supported on node and edge types")
        );
    }

    #[test]
    fn test_reject_key_on_list_property() {
        let input = r#"
node Ticket {
    tags: [String] @key
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("list property"));
    }

    #[test]
    fn test_parse_vector_type() {
        let input = r#"
node Doc {
    embedding: Vector(3)
}
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Node(n) => match n.properties[0].prop_type.scalar {
                ScalarType::Vector(dim) => assert_eq!(dim, 3),
                other => panic!("expected vector type, got {:?}", other),
            },
            _ => panic!("expected node"),
        }
    }

    #[test]
    fn test_reject_zero_vector_dimension() {
        let input = r#"
node Doc {
    embedding: Vector(0)
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("Vector dimension"));
    }

    #[test]
    fn test_reject_vector_dimension_larger_than_arrow_bound() {
        let input = r#"
node Doc {
    embedding: Vector(2147483648)
}
"#;
        let err = parse_schema(input).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum supported"));
    }

    #[test]
    fn test_parse_error() {
        let input = "node { }"; // missing type name
        assert!(parse_schema(input).is_err());
    }

    #[test]
    fn test_parse_error_diagnostic_has_span() {
        let input = "node { }";
        let err = parse_schema_diagnostic(input).unwrap_err();
        assert!(err.span.is_some());
    }
}
