use super::*;
use crate::build_catalog;
use crate::query::descriptor::{
    QueryGraphFact, QueryGraphFactKind, QueryResultFieldDescriptor, QueryValueKind,
};
use crate::schema::parser::parse_schema;

fn catalog(schema: &str) -> Catalog {
    let schema = parse_schema(schema).unwrap();
    build_catalog(&schema).unwrap()
}

#[test]
fn parse_failure_returns_structured_error_output() {
    let output = lint_query_file(
        &catalog("node Person { name: String }"),
        "query broken(",
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(output.status, QueryLintStatus::Error);
    assert_eq!(output.queries_processed, 0);
    assert_eq!(output.errors, 1);
    assert!(output.results.is_empty());
    assert_eq!(output.findings.len(), 1);
    assert_eq!(output.findings[0].severity, QueryLintSeverity::Error);
    assert_eq!(output.findings[0].code, PARSE_ERROR_CODE);
}

#[test]
fn mixed_valid_and_invalid_queries_preserve_per_query_results() {
    let output = lint_query_file(
        &catalog(
            r#"
node Person {
slug: String @key
name: String?
}
"#,
        ),
        r#"
query list_people($slug: String) {
match { $p: Person { slug: $slug } }
return { $p.name }
}

query bad_update($slug: String) {
update Person set { missing: "nope" } where slug = $slug
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(output.queries_processed, 2);
    assert_eq!(output.results[0].name, "list_people");
    assert_eq!(output.results[0].status, QueryLintStatus::Ok);
    let operation = output.results[0].operation.as_ref().unwrap();
    assert_eq!(
        operation.result,
        vec![QueryResultFieldDescriptor {
            name: "name".to_string(),
            kind: QueryValueKind::String,
            item_kind: None,
            vector_dim: None,
            nullable: true,
        }]
    );
    assert_eq!(
        operation.reads,
        vec![QueryGraphFact {
            kind: QueryGraphFactKind::Node,
            type_name: "Person".to_string(),
        }]
    );
    assert!(operation.writes.is_empty());
    assert!(output.results[1].operation.is_none());
    assert_eq!(output.results[1].name, "bad_update");
    assert_eq!(output.results[1].status, QueryLintStatus::Error);
    assert!(
        output.results[1]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("has no property")
    );
    let serialized = serde_json::to_value(&output).unwrap();
    assert!(
        serialized["results"][1].get("operation").is_none(),
        "failed queries must omit the descriptor rather than serialize a partial value"
    );
}

#[test]
fn hardcoded_mutation_warning_only_fires_for_mutation_queries() {
    let output = lint_query_file(
        &catalog(
            r#"
node Person {
slug: String @key
name: String?
}
edge Knows: Person -> Person @card(0..1)
"#,
        ),
        r#"
query list_people() {
match { $p: Person }
return { $p.name }
}

query insert_person() {
insert Person { slug: "p1", name: "P1" }
}

query insert_knows($from: String, $to: String) {
insert Knows { from: $from, to: $to }
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert!(output.results[0].warnings.is_empty());
    assert_eq!(
        output.results[1].warnings,
        vec![HARDCODED_MUTATION_WARNING.to_string()]
    );
    assert_eq!(output.warnings, 1);
    let mutation = output.results[1].operation.as_ref().unwrap();
    assert!(mutation.result.is_empty());
    assert_eq!(
        mutation.reads,
        vec![QueryGraphFact {
            kind: QueryGraphFactKind::Node,
            type_name: "Person".to_string(),
        }]
    );
    assert_eq!(
        mutation.writes,
        vec![QueryGraphFact {
            kind: QueryGraphFactKind::Node,
            type_name: "Person".to_string(),
        }]
    );
    let constrained_edge = output.results[2].operation.as_ref().unwrap();
    assert_eq!(
        constrained_edge.reads,
        vec![
            QueryGraphFact {
                kind: QueryGraphFactKind::Node,
                type_name: "Person".to_string(),
            },
            QueryGraphFact {
                kind: QueryGraphFactKind::Edge,
                type_name: "Knows".to_string(),
            },
        ]
    );
    assert_eq!(
        constrained_edge.writes,
        vec![QueryGraphFact {
            kind: QueryGraphFactKind::Edge,
            type_name: "Knows".to_string(),
        }]
    );
}

#[test]
fn l201_warns_for_nullable_uncovered_update_fields() {
    let output = lint_query_file(
        &catalog(
            r#"
node Policy {
slug: String @key
name: String?
effectiveTo: DateTime?
}
"#,
        ),
        r#"
query update_policy($slug: String, $name: String) {
update Policy set { name: $name } where slug = $slug
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(output.findings.len(), 1);
    assert_eq!(output.findings[0].code, L201_CODE);
    assert_eq!(
        output.findings[0].message,
        "Policy.effectiveTo exists in schema but no update query sets it"
    );
    assert_eq!(output.findings[0].query_names, vec!["update_policy"]);
}

#[test]
fn l201_does_not_fire_without_valid_update_queries() {
    let output = lint_query_file(
        &catalog(
            r#"
node Policy {
slug: String @key
effectiveTo: DateTime?
}
"#,
        ),
        r#"
query insert_policy($slug: String) {
insert Policy { slug: $slug }
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert!(output.findings.is_empty());
}

#[test]
fn l201_excludes_embed_target_properties() {
    let output = lint_query_file(
        &catalog(
            r#"
node Doc {
slug: String @key
body: String?
summary: String?
embedding: Vector(3)? @embed(body)
}
"#,
        ),
        r#"
query update_doc($slug: String, $body: String) {
update Doc set { body: $body } where slug = $slug
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(output.findings.len(), 1);
    assert_eq!(output.findings[0].property.as_deref(), Some("summary"));
}

#[test]
fn l201_excludes_key_properties_even_if_catalog_is_modified() {
    let mut catalog = catalog(
        r#"
node Policy {
slug: String @key
name: String?
}
"#,
    );
    catalog
        .node_types
        .get_mut("Policy")
        .unwrap()
        .properties
        .get_mut("slug")
        .unwrap()
        .nullable = true;

    let output = lint_query_file(
        &catalog,
        r#"
query update_policy($slug: String, $name: String) {
update Policy set { name: $name } where slug = $slug
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert!(
        output
            .findings
            .iter()
            .all(|finding| finding.property.as_deref() != Some("slug"))
    );
}

#[test]
fn findings_and_query_names_are_deterministic() {
    let output = lint_query_file(
        &catalog(
            r#"
node Policy {
slug: String @key
c_field: String?
b_field: String?
a_field: String?
}
"#,
        ),
        r#"
query update_b($slug: String) {
update Policy set { a_field: "x" } where slug = $slug
}

query update_a($slug: String) {
update Policy set { a_field: "x" } where slug = $slug
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(output.findings.len(), 2);
    assert_eq!(output.findings[0].property.as_deref(), Some("b_field"));
    assert_eq!(output.findings[1].property.as_deref(), Some("c_field"));
    assert_eq!(output.findings[0].query_names, vec!["update_a", "update_b"]);
    assert_eq!(output.findings[1].query_names, vec!["update_a", "update_b"]);
}

#[test]
fn negated_subplans_contribute_recursive_read_facts() {
    let output = lint_query_file(
        &catalog(
            r#"
node Person { name: String }
node Company { name: String }
edge WorksAt: Person -> Company
"#,
        ),
        r#"
query unemployed() {
match {
    $p: Person
    not { $p worksAt $_ }
}
return { $p.name }
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(
        output.results[0].operation.as_ref().unwrap().reads,
        vec![
            QueryGraphFact {
                kind: QueryGraphFactKind::Node,
                type_name: "Company".to_string(),
            },
            QueryGraphFact {
                kind: QueryGraphFactKind::Node,
                type_name: "Person".to_string(),
            },
            QueryGraphFact {
                kind: QueryGraphFactKind::Edge,
                type_name: "WorksAt".to_string(),
            },
        ]
    );
}

#[test]
fn node_delete_reports_every_incident_edge_as_read_and_possible_write() {
    let output = lint_query_file(
        &catalog(
            r#"
node Person { slug: String @key }
node Company { slug: String @key }
edge Knows: Person -> Person
edge WorksAt: Person -> Company
edge Partners: Company -> Company
"#,
        ),
        r#"
query delete_person($slug: String) {
delete Person where slug = $slug
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    let operation = output.results[0].operation.as_ref().unwrap();
    let expected = vec![
        QueryGraphFact {
            kind: QueryGraphFactKind::Node,
            type_name: "Person".to_string(),
        },
        QueryGraphFact {
            kind: QueryGraphFactKind::Edge,
            type_name: "Knows".to_string(),
        },
        QueryGraphFact {
            kind: QueryGraphFactKind::Edge,
            type_name: "WorksAt".to_string(),
        },
    ];
    assert_eq!(operation.reads, expected);
    assert_eq!(operation.writes, expected);
}

#[test]
fn same_named_node_and_edge_mutation_keeps_the_typechecked_namespace() {
    let output = lint_query_file(
        &catalog(
            r#"
node Shared { slug: String @key }
edge Shared: Shared -> Shared
"#,
        ),
        r#"
query insert_shared($slug: String) {
insert Shared { slug: $slug }
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    let operation = output.results[0].operation.as_ref().unwrap();
    let node = QueryGraphFact {
        kind: QueryGraphFactKind::Node,
        type_name: "Shared".to_string(),
    };
    assert_eq!(operation.reads, vec![node.clone()]);
    assert_eq!(operation.writes, vec![node]);
}

#[test]
fn result_descriptor_uses_closed_structured_kinds() {
    let output = lint_query_file(
        &catalog(
            r#"
node Doc {
slug: String @key
count: I32
total: I64
score: F64
tags: [Date]?
embedding: Vector(3)
createdAt: DateTime?
}
"#,
        ),
        r#"
query describe_doc() {
match { $d: Doc }
return {
    $d.slug
    $d.count
    $d.total
    $d.score
    $d.tags
    $d.embedding
    $d.createdAt
    $d
}
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    let result = &output.results[0].operation.as_ref().unwrap().result;
    let shapes = result
        .iter()
        .map(|field| {
            (
                field.name.as_str(),
                field.kind,
                field.item_kind,
                field.vector_dim,
                field.nullable,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        shapes,
        vec![
            ("slug", QueryValueKind::String, None, None, false),
            ("count", QueryValueKind::Int, None, None, false),
            ("total", QueryValueKind::BigInt, None, None, false),
            ("score", QueryValueKind::Float, None, None, false),
            (
                "tags",
                QueryValueKind::List,
                Some(QueryValueKind::Date),
                None,
                true,
            ),
            ("embedding", QueryValueKind::Vector, None, Some(3), false,),
            ("createdAt", QueryValueKind::DateTime, None, None, true,),
            ("d", QueryValueKind::Object, None, None, false),
        ]
    );

    let serialized = serde_json::to_value(&output).unwrap();
    assert_eq!(
        serialized["results"][0]["operation"]["result"][4],
        serde_json::json!({
            "name": "tags",
            "kind": "list",
            "item_kind": "date",
            "nullable": true
        })
    );
    assert_eq!(
        serialized["results"][0]["operation"]["result"][5],
        serde_json::json!({
            "name": "embedding",
            "kind": "vector",
            "vector_dim": 3,
            "nullable": false
        })
    );
}
