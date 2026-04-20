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
#[path = "lint_tests.rs"]
mod tests;
