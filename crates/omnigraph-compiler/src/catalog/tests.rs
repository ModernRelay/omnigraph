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
fn test_embed_source_records_model_kwarg() {
    let schema = parse_schema(
        r#"
node Doc {
title: String
embedding: Vector(3) @embed("title", model="openai/text-embedding-3-large")
plain: Vector(3) @embed("title")
}
"#,
    )
    .unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let doc = catalog.node_types.get("Doc").unwrap();

    let embedding = doc.embed_sources.get("embedding").unwrap();
    assert_eq!(embedding.source, "title");
    assert_eq!(
        embedding.model.as_deref(),
        Some("openai/text-embedding-3-large")
    );

    let plain = doc.embed_sources.get("plain").unwrap();
    assert_eq!(plain.source, "title");
    assert_eq!(plain.model, None);
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
    let system = catalog.system_columns;
    assert_eq!(system.id, "__id");
    let person = &catalog.node_types["Person"];
    assert_eq!(
        person
            .arrow_schema
            .field_with_name(system.id)
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    let knows = &catalog.edge_types["Knows"];
    for column in [system.id, system.src, system.dst] {
        assert_eq!(
            knows
                .arrow_schema
                .field_with_name(column)
                .unwrap()
                .data_type(),
            &DataType::Utf8
        );
    }
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
        declarations: vec![
            SchemaDecl::Node(NodeDecl {
                name: "Person".to_string(),
                annotations: vec![],
                implements: vec![],
                properties: vec![crate::schema::ast::PropDecl {
                    name: "name".to_string(),
                    prop_type: PropType::scalar(ScalarType::String, false),
                    annotations: vec![],
                    distance: None,
                }],
                constraints: vec![],
                source_constraints: vec![],
                property_origins: Default::default(),
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

/// What a String's one-column declaration builds: a `@key` alone a BTREE, an
/// explicit `@index` (with a `@key` or in the body form) an NGRAM and a
/// BTREE, and only `@index(fulltext, ...)` a full-text index beside them.
/// Every one is a lexical field. Another type builds the same kinds whatever
/// the declaration. The catalog built from accepted IR agrees.
#[test]
fn string_index_kinds_follow_the_declaration() {
    use crate::catalog::schema_ir::{SchemaIdentityDomain, initialize_schema_ir};
    use crate::catalog::schema_shape::compile_schema_shape;
    use crate::types::PropIndexKind::{Bitmap, Btree, FullText, NGram};
    let input = r#"
node Doc {
slug: String @key
title: String @index
body: String @index(fulltext)
note: String
kind: enum(a, b) @index
@index(note)
}
node Tag {
n: I32 @index
code: String @key @index
}
"#;
    let schema = parse_schema(input).unwrap();
    let domain = SchemaIdentityDomain::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    let ir = initialize_schema_ir(domain, &compile_schema_shape(&schema).unwrap())
        .unwrap()
        .schema_ir;
    for catalog in [
        build_catalog(&schema).unwrap(),
        build_catalog_from_ir(&ir).unwrap(),
    ] {
        let doc = &catalog.node_types["Doc"];
        assert_eq!(doc.property_index_kinds("slug"), &[Btree]);
        assert_eq!(doc.property_index_kinds("title"), &[NGram, Btree]);
        assert_eq!(doc.property_index_kinds("body"), &[FullText, NGram, Btree]);
        assert_eq!(doc.property_index_kinds("note"), &[NGram, Btree]);
        assert_eq!(doc.property_index_kinds("kind"), &[Bitmap]);
        for property in ["slug", "title", "body", "note"] {
            assert!(doc.is_lexical_field(property), "{property}");
            assert_eq!(doc.builds_full_text_index(property), property == "body");
        }
        assert!(!doc.is_lexical_field("kind"));
        let tag = &catalog.node_types["Tag"];
        assert_eq!(tag.property_index_kinds("code"), &[NGram, Btree]);
        assert!(tag.is_lexical_field("code") && !tag.builds_full_text_index("code"));
        assert_eq!(tag.property_index_kinds("n"), &[Btree]);
    }
}
