use super::*;
use crate::schema::ast::{EdgeDecl, NodeDecl};
use crate::schema::parser::parse_schema;
use crate::types::PropType;

fn test_schema() -> &'static str {
    r#"
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
"#
}

#[test]
fn test_build_catalog() {
    let schema = parse_schema(test_schema()).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    assert_eq!(catalog.node_types.len(), 2);
    assert_eq!(catalog.edge_types.len(), 2);
    assert!(catalog.node_types.contains_key("Person"));
    assert!(catalog.node_types.contains_key("Company"));
}

#[test]
fn test_catalog_tracks_embedding_config_and_specs() {
    let schema = parse_schema(
        r#"
config {
embedding_model: "gemini-embedding-2-preview"
}

node Doc {
title: String
embedding: Vector @embed(title) @index
}
"#,
    )
    .unwrap();
    let catalog = build_catalog(&schema).unwrap();
    assert_eq!(catalog.config.embedding_model, "gemini-embedding-2-preview");
    let doc = &catalog.node_types["Doc"];
    let spec = &doc.embedding_specs["embedding"];
    assert_eq!(spec.source_prop, "title");
    assert_eq!(spec.model, "gemini-embedding-2-preview");
    assert_eq!(spec.dimensions, 3072);
    assert_eq!(doc.embed_sources["embedding"], "title");
}

#[test]
fn test_edge_lookup() {
    let schema = parse_schema(test_schema()).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let edge = catalog.lookup_edge_by_name("knows").unwrap();
    assert_eq!(edge.from_type, "Person");
    assert_eq!(edge.to_type, "Person");
    let upper = catalog.lookup_edge_by_name("KNOWS").unwrap();
    assert_eq!(upper.name, "Knows");
}

#[test]
fn test_node_arrow_schema() {
    let schema = parse_schema(test_schema()).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let person = &catalog.node_types["Person"];
    assert_eq!(person.arrow_schema.fields().len(), 3); // id, name, age
}

#[test]
fn test_duplicate_node_error() {
    let input = r#"
node Person { name: String }
node Person { age: I32 }
"#;
    let schema = parse_schema(input).unwrap();
    assert!(build_catalog(&schema).is_err());
}

#[test]
fn test_bad_edge_endpoint() {
    let input = r#"
node Person { name: String }
edge Knows: Person -> Alien
"#;
    let schema = parse_schema(input).unwrap();
    assert!(build_catalog(&schema).is_err());
}

#[test]
fn test_id_fields_are_utf8() {
    let schema = parse_schema(test_schema()).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let person = &catalog.node_types["Person"];
    assert_eq!(
        person
            .arrow_schema
            .field_with_name("id")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    let knows = &catalog.edge_types["Knows"];
    assert_eq!(
        knows
            .arrow_schema
            .field_with_name("id")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        knows
            .arrow_schema
            .field_with_name("src")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        knows
            .arrow_schema
            .field_with_name("dst")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
}

#[test]
fn test_key_property_tracking() {
    let input = r#"
node Signal {
slug: String @key
title: String
}
node Person {
name: String
}
edge Emits: Person -> Signal
"#;
    let schema = parse_schema(input).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    assert_eq!(catalog.node_types["Signal"].key_property(), Some("slug"));
    assert_eq!(catalog.node_types["Person"].key_property(), None);
}

#[test]
fn test_edge_lookup_handles_non_ascii_leading_character() {
    let schema = SchemaFile {
        config: Default::default(),
        declarations: vec![
            SchemaDecl::Node(NodeDecl {
                name: "Person".to_string(),
                annotations: vec![],
                implements: vec![],
                properties: vec![crate::schema::ast::PropDecl {
                    name: "name".to_string(),
                    prop_type: PropType::scalar(ScalarType::String, false),
                    annotations: vec![],
                }],
                constraints: vec![],
            }),
            SchemaDecl::Edge(EdgeDecl {
                name: "Édges".to_string(),
                from_type: "Person".to_string(),
                to_type: "Person".to_string(),
                cardinality: Default::default(),
                annotations: vec![],
                properties: vec![],
                constraints: vec![],
            }),
        ],
    };
    let catalog = build_catalog(&schema).unwrap();
    assert!(catalog.lookup_edge_by_name("édges").is_some());
}

#[test]
fn test_edge_lookup_rejects_case_fold_collisions() {
    let input = r#"
node Person { name: String }
edge Knows: Person -> Person
edge KNOWS: Person -> Person
"#;
    let schema = parse_schema(input).unwrap();
    let err = build_catalog(&schema).unwrap_err();
    assert!(err.to_string().contains("case folding"));
}

#[test]
fn test_catalog_composite_unique() {
    let input = r#"
node Person {
first: String
last: String
@unique(first, last)
}
"#;
    let schema = parse_schema(input).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let person = &catalog.node_types["Person"];
    assert!(
        person
            .unique_constraints
            .contains(&vec!["first".to_string(), "last".to_string()])
    );
}

#[test]
fn test_catalog_composite_index() {
    let input = r#"
node Event {
category: String
date: Date
@index(category, date)
}
"#;
    let schema = parse_schema(input).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let event = &catalog.node_types["Event"];
    assert!(
        event
            .indices
            .contains(&vec!["category".to_string(), "date".to_string()])
    );
}

#[test]
fn test_catalog_edge_cardinality() {
    let input = r#"
node Person { name: String }
node Company { name: String }
edge WorksAt: Person -> Company @card(0..1)
"#;
    let schema = parse_schema(input).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let edge = &catalog.edge_types["WorksAt"];
    assert_eq!(edge.cardinality.min, 0);
    assert_eq!(edge.cardinality.max, Some(1));
}

#[test]
fn test_catalog_interfaces_stored() {
    let input = r#"
interface Named {
name: String
}
node Person implements Named {
age: I32?
}
"#;
    let schema = parse_schema(input).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    assert!(catalog.interfaces.contains_key("Named"));
    assert!(catalog.interfaces["Named"].properties.contains_key("name"));
}

#[test]
fn test_catalog_node_implements() {
    let input = r#"
interface Named {
name: String
}
node Person implements Named {
age: I32?
}
"#;
    let schema = parse_schema(input).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    assert_eq!(catalog.node_types["Person"].implements, vec!["Named"]);
}

#[test]
fn test_key_implies_index() {
    let input = r#"
node Signal {
slug: String @key
title: String
}
"#;
    let schema = parse_schema(input).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let signal = &catalog.node_types["Signal"];
    assert!(signal.indices.contains(&vec!["slug".to_string()]));
}
