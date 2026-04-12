use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::catalog::Catalog;
use crate::query::ast::{Mutation, QueryDecl};
use crate::query::parser::parse_query;
use crate::query::typecheck::typecheck_query_decl;

const PARSE_ERROR_CODE: &str = "Q000";
const L201_CODE: &str = "L201";
const HARDCODED_MUTATION_WARNING: &str =
    "mutation declares no params; hardcoded mutations are easy to miss";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryLintStatus {
    Ok,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryLintSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryLintQueryKind {
    Read,
    Mutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryLintSchemaSourceKind {
    File,
    Repo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryLintSchemaSource {
    pub kind: QueryLintSchemaSourceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

impl QueryLintSchemaSource {
    pub fn file(path: impl Into<String>) -> Self {
        Self {
            kind: QueryLintSchemaSourceKind::File,
            path: Some(path.into()),
            uri: None,
        }
    }

    pub fn repo(uri: impl Into<String>) -> Self {
        Self {
            kind: QueryLintSchemaSourceKind::Repo,
            path: None,
            uri: Some(uri.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryLintQueryResult {
    pub name: String,
    pub kind: QueryLintQueryKind,
    pub status: QueryLintStatus,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryLintFinding {
    pub severity: QueryLintSeverity,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub query_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryLintOutput {
    pub status: QueryLintStatus,
    pub schema_source: QueryLintSchemaSource,
    pub query_path: String,
    pub queries_processed: usize,
    pub errors: usize,
    pub warnings: usize,
    pub infos: usize,
    pub results: Vec<QueryLintQueryResult>,
    pub findings: Vec<QueryLintFinding>,
}

#[derive(Debug, Default)]
struct UpdateCoverage {
    query_names: BTreeSet<String>,
    assigned_properties: BTreeSet<String>,
}

pub fn lint_query_file(
    catalog: &Catalog,
    query_source: &str,
    query_path: impl Into<String>,
    schema_source: QueryLintSchemaSource,
) -> QueryLintOutput {
    let query_path = query_path.into();
    match parse_query(query_source) {
        Ok(parsed) => {
            let queries_processed = parsed.queries.len();
            let mut results = Vec::with_capacity(queries_processed);
            let mut coverage = BTreeMap::<String, UpdateCoverage>::new();

            for query in &parsed.queries {
                let kind = query_kind(query);
                let warnings = per_query_warnings(query);
                match typecheck_query_decl(catalog, query) {
                    Ok(_) => {
                        collect_update_coverage(query, &mut coverage);
                        results.push(QueryLintQueryResult {
                            name: query.name.clone(),
                            kind,
                            status: QueryLintStatus::Ok,
                            warnings,
                            error: None,
                        });
                    }
                    Err(err) => {
                        results.push(QueryLintQueryResult {
                            name: query.name.clone(),
                            kind,
                            status: QueryLintStatus::Error,
                            warnings,
                            error: Some(err.to_string()),
                        });
                    }
                }
            }

            let mut findings = lint_update_coverage(catalog, &coverage);
            findings.sort_by(findings_cmp);

            let errors = results
                .iter()
                .filter(|result| result.status == QueryLintStatus::Error)
                .count()
                + findings
                    .iter()
                    .filter(|finding| finding.severity == QueryLintSeverity::Error)
                    .count();
            let warnings = results
                .iter()
                .map(|result| result.warnings.len())
                .sum::<usize>()
                + findings
                    .iter()
                    .filter(|finding| finding.severity == QueryLintSeverity::Warning)
                    .count();
            let infos = findings
                .iter()
                .filter(|finding| finding.severity == QueryLintSeverity::Info)
                .count();

            QueryLintOutput {
                status: if errors == 0 {
                    QueryLintStatus::Ok
                } else {
                    QueryLintStatus::Error
                },
                schema_source,
                query_path,
                queries_processed,
                errors,
                warnings,
                infos,
                results,
                findings,
            }
        }
        Err(err) => QueryLintOutput {
            status: QueryLintStatus::Error,
            schema_source,
            query_path,
            queries_processed: 0,
            errors: 1,
            warnings: 0,
            infos: 0,
            results: Vec::new(),
            findings: vec![QueryLintFinding {
                severity: QueryLintSeverity::Error,
                code: PARSE_ERROR_CODE.to_string(),
                message: err.to_string(),
                type_name: None,
                property: None,
                query_names: Vec::new(),
            }],
        },
    }
}

fn query_kind(query: &QueryDecl) -> QueryLintQueryKind {
    if query.mutations.is_empty() {
        QueryLintQueryKind::Read
    } else {
        QueryLintQueryKind::Mutation
    }
}

fn per_query_warnings(query: &QueryDecl) -> Vec<String> {
    if query.mutations.is_empty() || !query.params.is_empty() {
        return Vec::new();
    }
    vec![HARDCODED_MUTATION_WARNING.to_string()]
}

fn collect_update_coverage(query: &QueryDecl, coverage: &mut BTreeMap<String, UpdateCoverage>) {
    for mutation in &query.mutations {
        if let Mutation::Update(update) = mutation {
            let entry = coverage.entry(update.type_name.clone()).or_default();
            entry.query_names.insert(query.name.clone());
            for assignment in &update.assignments {
                entry
                    .assigned_properties
                    .insert(assignment.property.clone());
            }
        }
    }
}

fn lint_update_coverage(
    catalog: &Catalog,
    coverage: &BTreeMap<String, UpdateCoverage>,
) -> Vec<QueryLintFinding> {
    let mut type_names = catalog.node_types.keys().cloned().collect::<Vec<_>>();
    type_names.sort();

    let mut findings = Vec::new();
    for type_name in type_names {
        let Some(type_coverage) = coverage.get(&type_name) else {
            continue;
        };
        if type_coverage.query_names.is_empty() {
            continue;
        }

        let node_type = &catalog.node_types[&type_name];
        let key_properties = node_type.key.clone().unwrap_or_default();

        let mut property_names = node_type.properties.keys().cloned().collect::<Vec<_>>();
        property_names.sort();

        for property_name in property_names {
            let property = &node_type.properties[&property_name];
            if !property.nullable {
                continue;
            }
            if key_properties.iter().any(|key| key == &property_name) {
                continue;
            }
            if node_type.embed_sources.contains_key(&property_name) {
                continue;
            }
            if type_coverage.assigned_properties.contains(&property_name) {
                continue;
            }

            findings.push(QueryLintFinding {
                severity: QueryLintSeverity::Warning,
                code: L201_CODE.to_string(),
                message: format!(
                    "{}.{} exists in schema but no update query sets it",
                    type_name, property_name
                ),
                type_name: Some(type_name.clone()),
                property: Some(property_name),
                query_names: type_coverage.query_names.iter().cloned().collect(),
            });
        }
    }
    findings
}

fn findings_cmp(left: &QueryLintFinding, right: &QueryLintFinding) -> std::cmp::Ordering {
    severity_rank(left.severity)
        .cmp(&severity_rank(right.severity))
        .then_with(|| left.type_name.cmp(&right.type_name))
        .then_with(|| left.property.cmp(&right.property))
        .then_with(|| left.message.cmp(&right.message))
}

fn severity_rank(severity: QueryLintSeverity) -> u8 {
    match severity {
        QueryLintSeverity::Error => 0,
        QueryLintSeverity::Warning => 1,
        QueryLintSeverity::Info => 2,
    }
}

#[cfg(test)]
mod tests {
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
}
