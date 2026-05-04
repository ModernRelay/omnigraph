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

    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert_eq!(n.name, "Person");
            assert!(n.annotations.is_empty());
            assert!(n.implements.is_empty());
            assert_eq!(n.properties.len(), 2);
            assert_eq!(n.properties[0].name, "name");
            assert!(!n.properties[0].prop_type.nullable);
            assert_eq!(n.properties[1].name, "age");
            assert!(n.properties[1].prop_type.nullable);
        }
        _ => panic!("expected Node"),
    }

    match &schema.declarations[2] {
        SchemaDecl::Edge(e) => {
            assert_eq!(e.name, "Knows");
            assert_eq!(e.from_type, "Person");
            assert_eq!(e.to_type, "Person");
            assert!(e.annotations.is_empty());
            assert_eq!(e.properties.len(), 1);
            assert!(e.cardinality.is_default());
        }
        _ => panic!("expected Edge"),
    }
}

#[test]
fn test_parse_interface_basic() {
    let input = r#"
interface Named {
name: String
}
node Person implements Named {
age: I32?
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Interface(i) => {
            assert_eq!(i.name, "Named");
            assert_eq!(i.properties.len(), 1);
            assert_eq!(i.properties[0].name, "name");
        }
        _ => panic!("expected Interface"),
    }
    match &schema.declarations[1] {
        SchemaDecl::Node(n) => {
            assert_eq!(n.name, "Person");
            assert_eq!(n.implements, vec!["Named"]);
            // "name" injected from interface + "age" declared locally
            assert_eq!(n.properties.len(), 2);
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_implements_multiple() {
    let input = r#"
interface Slugged {
slug: String @key
}
interface Described {
title: String
description: String?
}
node Signal implements Slugged, Described {
strength: F64
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[2] {
        SchemaDecl::Node(n) => {
            assert_eq!(n.name, "Signal");
            assert_eq!(n.implements, vec!["Slugged", "Described"]);
            // slug + title + description + strength
            assert_eq!(n.properties.len(), 4);
            // @key from Slugged should be desugared into constraints
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Key(v) if v == &["slug"]))
            );
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_reject_implements_unknown_interface() {
    let input = r#"
node Person implements Unknown {
name: String
}
"#;
    let err = parse_schema(input).unwrap_err();
    assert!(err.to_string().contains("unknown interface"));
}

#[test]
fn test_reject_interface_property_type_conflict() {
    let input = r#"
interface Named {
name: I32
}
node Person implements Named {
name: String
}
"#;
    let err = parse_schema(input).unwrap_err();
    assert!(err.to_string().contains("type") || err.to_string().contains("interface"));
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
            // Annotations are desugared into constraints
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Unique(_)))
            );
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Key(_)))
            );
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Index(_)))
            );
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_property_level_key_desugars_to_constraint() {
    let input = r#"
node Person {
name: String @key
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Key(v) if v == &["name"]))
            );
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_body_constraint_key() {
    let input = r#"
node Person {
name: String
@key(name)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Key(v) if v == &["name"]))
            );
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_body_constraint_unique_composite() {
    let input = r#"
node Person {
first: String
last: String
@unique(first, last)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Unique(v) if v == &["first", "last"]))
            );
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_body_constraint_index_composite() {
    let input = r#"
node Event {
category: String
date: Date
@index(category, date)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Index(v) if v == &["category", "date"]))
            );
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_body_constraint_range() {
    let input = r#"
node Person {
age: I32?
@range(age, 0..200)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(
                n.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Range { property, .. } if property == "age"))
            );
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_range_float_bounds() {
    let input = r#"
node Measurement {
name: String @key
temperature: F64?
@range(temperature, 0.0..100.0)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(n.constraints.iter().any(|c| matches!(
                c,
                Constraint::Range { property, min, max }
                if property == "temperature"
                    && matches!(min, Some(ConstraintBound::Float(f)) if *f == 0.0)
                    && matches!(max, Some(ConstraintBound::Float(f)) if *f == 100.0)
            )));
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_range_negative_float_bounds() {
    let input = r#"
node Measurement {
name: String @key
temperature: F64?
@range(temperature, -40.0..60.0)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(n.constraints.iter().any(|c| matches!(
                c,
                Constraint::Range { property, min, max }
                if property == "temperature"
                    && matches!(min, Some(ConstraintBound::Float(f)) if *f == -40.0)
                    && matches!(max, Some(ConstraintBound::Float(f)) if *f == 60.0)
            )));
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_range_negative_integer_bounds() {
    let input = r#"
node Account {
name: String @key
balance: I64?
@range(balance, -1000..1000)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(n.constraints.iter().any(|c| matches!(
                c,
                Constraint::Range { property, min, max }
                if property == "balance"
                    && matches!(min, Some(ConstraintBound::Integer(n)) if *n == -1000)
                    && matches!(max, Some(ConstraintBound::Integer(n)) if *n == 1000)
            )));
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_parse_body_constraint_check() {
    let input = r#"
node Order {
code: String
@check(code, "[A-Z]{3}-[0-9]+")
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => {
            assert!(n.constraints.iter().any(|c| matches!(c, Constraint::Check { property, pattern } if property == "code" && pattern == "[A-Z]{3}-[0-9]+")));
        }
        _ => panic!("expected Node"),
    }
}

#[test]
fn test_reject_range_on_string() {
    let input = r#"
node Person {
name: String
@range(name, 0..100)
}
"#;
    let err = parse_schema(input).unwrap_err();
    assert!(err.to_string().contains("numeric"));
}

#[test]
fn test_reject_check_on_integer() {
    let input = r#"
node Person {
age: I32
@check(age, "[0-9]+")
}
"#;
    let err = parse_schema(input).unwrap_err();
    assert!(err.to_string().contains("String"));
}

#[test]
fn test_parse_edge_cardinality() {
    let input = r#"
node Person { name: String }
node Company { name: String }
edge WorksAt: Person -> Company @card(0..1)
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[2] {
        SchemaDecl::Edge(e) => {
            assert_eq!(e.cardinality.min, 0);
            assert_eq!(e.cardinality.max, Some(1));
        }
        _ => panic!("expected Edge"),
    }
}

#[test]
fn test_parse_edge_cardinality_unbounded() {
    let input = r#"
node Person { name: String }
node Paper { title: String }
edge Authored: Person -> Paper @card(1..)
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[2] {
        SchemaDecl::Edge(e) => {
            assert_eq!(e.cardinality.min, 1);
            assert_eq!(e.cardinality.max, None);
        }
        _ => panic!("expected Edge"),
    }
}

#[test]
fn test_parse_edge_default_cardinality() {
    let input = r#"
node Person { name: String }
edge Knows: Person -> Person
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[1] {
        SchemaDecl::Edge(e) => {
            assert!(e.cardinality.is_default());
        }
        _ => panic!("expected Edge"),
    }
}

#[test]
fn test_parse_edge_unique_src_dst() {
    let input = r#"
node Person { name: String }
edge Knows: Person -> Person {
@unique(src, dst)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[1] {
        SchemaDecl::Edge(e) => {
            assert!(
                e.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Unique(v) if v == &["src", "dst"]))
            );
        }
        _ => panic!("expected Edge"),
    }
}

#[test]
fn test_parse_edge_property_index() {
    let input = r#"
node Person { name: String }
node Company { name: String }
edge WorksAt: Person -> Company {
since: Date? @index
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[2] {
        SchemaDecl::Edge(e) => {
            // @index on since is desugared to Constraint::Index
            assert!(
                e.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Index(v) if v == &["since"]))
            );
        }
        _ => panic!("expected Edge"),
    }
}

#[test]
fn test_parse_embed_annotation_identifier_arg() {
    let input = r#"
node Doc {
title: String
embedding: Vector @embed(title)
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
    assert!(err.to_string().contains("multiple @key"));
}

#[test]
fn test_reject_unique_with_value() {
    // @unique("x") is now a parse error — the grammar parses it as a body_constraint
    // which expects ident args, not string literals as the sole argument
    let input = r#"
node Person {
email: String @unique("x")
}
"#;
    assert!(parse_schema(input).is_err());
}

#[test]
fn test_reject_index_with_value() {
    // @index("x") is now a parse error — same reason as above
    let input = r#"
node Person {
email: String @index("x")
}
"#;
    assert!(parse_schema(input).is_err());
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
fn test_allow_unique_on_edge_property() {
    let input = r#"
node Person { name: String }
edge Knows: Person -> Person {
weight: I32 @unique
}
"#;
    // Should now succeed (edge property @unique is allowed)
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[1] {
        SchemaDecl::Edge(e) => {
            assert!(
                e.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Unique(v) if v == &["weight"]))
            );
        }
        _ => panic!("expected Edge"),
    }
}

#[test]
fn test_allow_index_on_edge_property() {
    let input = r#"
node Person { name: String }
edge Knows: Person -> Person {
weight: I32 @index
}
"#;
    // Should now succeed (edge property @index is allowed)
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[1] {
        SchemaDecl::Edge(e) => {
            assert!(
                e.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Index(v) if v == &["weight"]))
            );
        }
        _ => panic!("expected Edge"),
    }
}

#[test]
fn test_reject_embed_without_source_property() {
    let input = r#"
node Doc {
title: String
embedding: Vector @embed
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
embedding: Vector @embed(body)
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
embedding: Vector @embed(body)
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
embedding: Vector(3072) @embed(title)
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
fn test_parse_annotation_decodes_escapes() {
    let input = r#"
node Task @description("Tracked\n\"work\"\\item") {
slug: String @key @description("Stable\tidentifier")
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(node) => {
            assert_eq!(
                node.annotations[0].value.as_deref(),
                Some("Tracked\n\"work\"\\item")
            );
            assert_eq!(
                node.properties[0].annotations[1].value.as_deref(),
                Some("Stable\tidentifier")
            );
        }
        _ => panic!("expected node"),
    }
}

#[test]
fn test_parse_annotation_rejects_unknown_escape() {
    let input = r#"
node Task @description("Tracked\q") {
slug: String @key
}
"#;
    let err = parse_schema(input).unwrap_err();
    assert!(err.to_string().contains("unsupported escape sequence"));
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
fn test_parse_schema_config_embedding_model() {
    let input = r#"
config {
embedding_model: "gemini-embedding-2-preview"
}

node Doc {
title: String
}
"#;
    let schema = parse_schema(input).unwrap();
    assert_eq!(schema.config.embedding_model, "gemini-embedding-2-preview");
}

#[test]
fn test_bare_vector_embed_infers_configured_model_dimension() {
    let input = r#"
config {
embedding_model: "gemini-embedding-2-preview"
}

node Doc {
title: String
embedding: Vector @embed(title)
}
"#;
    let schema = parse_schema(input).unwrap();
    match &schema.declarations[0] {
        SchemaDecl::Node(n) => match n.properties[1].prop_type.scalar {
            ScalarType::Vector(dim) => assert_eq!(dim, 3072),
            other => panic!("expected inferred vector type, got {:?}", other),
        },
        _ => panic!("expected node"),
    }
}

#[test]
fn test_reject_unknown_embedding_model() {
    let input = r#"
config {
embedding_model: "not-a-model"
}

node Doc {
title: String
embedding: Vector @embed(title)
}
"#;
    let err = parse_schema(input).unwrap_err();
    assert!(err.to_string().contains("unsupported embedding_model"));
}

#[test]
fn test_reject_bare_vector_without_embed() {
    let input = r#"
node Doc {
embedding: Vector
}
"#;
    let err = parse_schema(input).unwrap_err();
    assert!(err.to_string().contains("bare Vector"));
}

#[test]
fn test_reject_embed_vector_dimension_mismatching_model() {
    let input = r#"
node Doc {
title: String
embedding: Vector(3) @embed(title)
}
"#;
    let err = parse_schema(input).unwrap_err();
    assert!(err.to_string().contains("produces 3072 dimensions"));
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
