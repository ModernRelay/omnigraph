use super::*;
use crate::build_catalog;
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
query list_people() {
match { $p: Person }
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
    assert_eq!(output.results[1].name, "bad_update");
    assert_eq!(output.results[1].status, QueryLintStatus::Error);
    assert!(
        output.results[1]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("has no property")
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
embedding: Vector? @embed(body)
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
        &r#"
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
