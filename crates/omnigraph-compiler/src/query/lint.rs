use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::catalog::Catalog;
use crate::ir::core::{CoreDecl, MutationTarget, Sink};
use crate::query::codes::Q004;
use crate::query::descriptor::{QueryOperationDescriptor, describe_query_operation};
use crate::query::diagnostic::{Applicability, QueryDiagnostic, QueryDiagnosticKind, Suggestion};
use crate::query::file::QueryDecl;
use crate::query::parser::parse_query_diagnostic;

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
    Graph,
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

    pub fn graph(uri: impl Into<String>) -> Self {
        Self {
            kind: QueryLintSchemaSourceKind::Graph,
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
    /// Every refusal of the query's compile pass, in source order, each
    /// with its span, fix and suggestion; the first is `error`.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub diagnostics: Vec<QueryDiagnostic>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<QueryOperationDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryLintFinding {
    pub severity: QueryLintSeverity,
    pub code: String,
    pub message: String,
    /// The text that replaces the finding's construct, for a finding that
    /// names a canonical form.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub query_names: Vec<String>,
    /// The fix as byte-range edits of the source, which `lint --fix`
    /// applies: a refusal's printed suggestion, or a canonical form's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<Suggestion>,
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
    let parsed = match parse_query_diagnostic(query_source) {
        Ok(file) => file
            .body
            .into_read_declarations()
            .map_err(|message| vec![(Q004.as_str().to_string(), message, None, None)]),
        Err(diagnostic) => Err(diagnostic
            .all()
            .into_iter()
            .map(front_door_finding)
            .collect()),
    };
    match parsed {
        Ok(queries) => {
            let queries_processed = queries.len();
            let mut results = Vec::with_capacity(queries_processed);
            let mut coverage = BTreeMap::<String, UpdateCoverage>::new();

            for query in &queries {
                let kind = query_kind(query);
                let warnings = per_query_warnings(query);
                match describe_query_operation(catalog, query) {
                    Ok(operation) => {
                        collect_update_coverage(catalog, query, &mut coverage);
                        results.push(QueryLintQueryResult {
                            name: query.name.clone(),
                            kind,
                            status: QueryLintStatus::Ok,
                            warnings,
                            error: None,
                            diagnostics: Vec::new(),
                            operation: Some(operation),
                        });
                    }
                    Err(err) => {
                        results.push(QueryLintQueryResult {
                            name: query.name.clone(),
                            kind,
                            status: QueryLintStatus::Error,
                            warnings,
                            error: Some(err.to_string()),
                            diagnostics: err
                                .diagnostic()
                                .map(QueryDiagnostic::all)
                                .unwrap_or_default(),
                            operation: None,
                        });
                    }
                }
            }

            let mut findings = lint_update_coverage(catalog, &coverage);
            findings.extend(lint_canonical_forms(query_source));
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
        Err(refusals) => QueryLintOutput {
            status: QueryLintStatus::Error,
            schema_source,
            query_path,
            queries_processed: 0,
            errors: refusals.len(),
            warnings: 0,
            infos: 0,
            results: Vec::new(),
            findings: refusals
                .into_iter()
                .map(|(code, message, fix, suggestion)| QueryLintFinding {
                    severity: QueryLintSeverity::Error,
                    code,
                    message,
                    fix,
                    type_name: None,
                    property: None,
                    query_names: Vec::new(),
                    suggestion,
                })
                .collect(),
        },
    }
}

/// The finding of a refusal of the front door: its code and its message
/// with the position, `line <n>, column <c>: <message>`, so a refused `set`
/// line is reported where it stands; a spanless diagnostic keeps its
/// message. A parse refusal keeps its code and its message; a refusal the
/// front door raises for the whole source unit (`T83`, one per repeated
/// name) keeps its code and its rendered form. Its fix and the fix's
/// byte-range edits ride along, as the diagnostic carries them.
fn front_door_finding(
    diagnostic: QueryDiagnostic,
) -> (String, String, Option<String>, Option<Suggestion>) {
    let suggestion = diagnostic.suggestion.as_deref().cloned();
    let fix = diagnostic.fix.clone();
    let (code, message) = match diagnostic.kind {
        QueryDiagnosticKind::Parse => (
            diagnostic.code.as_str().to_string(),
            diagnostic.message.clone(),
        ),
        _ => (diagnostic.code.as_str().to_string(), diagnostic.to_string()),
    };
    let message = match diagnostic.position {
        Some(at) => format!("line {}, column {}: {message}", at.line, at.column),
        None => message,
    };
    (code, message, fix, suggestion)
}

fn query_kind(query: &QueryDecl) -> QueryLintQueryKind {
    if !query.is_mutation() {
        QueryLintQueryKind::Read
    } else {
        QueryLintQueryKind::Mutation
    }
}

fn per_query_warnings(query: &QueryDecl) -> Vec<String> {
    if !query.is_mutation() || !query.params.is_empty() {
        return Vec::new();
    }
    vec![HARDCODED_MUTATION_WARNING.to_string()]
}

fn collect_update_coverage(
    catalog: &Catalog,
    query: &QueryDecl,
    coverage: &mut BTreeMap<String, UpdateCoverage>,
) {
    let Ok(CoreDecl::Write(write)) = crate::query::check::check_query_decl(catalog, query) else {
        return;
    };
    for sink in write.groups.iter().flat_map(|group| &group.sinks) {
        if let Sink::Update {
            target: MutationTarget::Node { type_name },
            assignments,
            ..
        } = sink
        {
            let entry = coverage.entry(type_name.clone()).or_default();
            entry.query_names.insert(query.name.clone());
            for (property, _) in assignments {
                entry.assigned_properties.insert(property.clone());
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
                fix: None,
                type_name: Some(type_name.clone()),
                property: Some(property_name),
                query_names: type_coverage.query_names.iter().cloned().collect(),
                suggestion: None,
            });
        }
    }
    findings
}

/// The non-canonical forms of every declaration of the source
/// (the GQ 3 RFC, section 6), each with its canonical form
/// as the fix: `L203` and `L206` to `L211` keep the typed IR, `L204` and
/// `L205` only the rows.
fn lint_canonical_forms(query_source: &str) -> Vec<QueryLintFinding> {
    crate::query::canonical::source_canonical_forms(query_source)
        .into_iter()
        .map(|(decl, form)| {
            let position = form
                .position
                .map(|at| format!("line {}, column {}: ", at.line, at.column))
                .unwrap_or_default();
            let message = if form.same_ir {
                format!(
                    "{position}`{}` is `{}`, the same typed IR",
                    form.written, form.fix
                )
            } else {
                format!(
                    "{position}`{}` has the rows of `{}`, which says it directly",
                    form.written, form.fix
                )
            };
            QueryLintFinding {
                severity: QueryLintSeverity::Info,
                code: form.code.to_string(),
                message,
                fix: Some(form.fix),
                type_name: None,
                property: None,
                query_names: vec![decl],
                suggestion: form.edit.map(|edit| Suggestion {
                    applicability: Applicability::MachineApplicable,
                    edits: vec![edit],
                }),
            }
        })
        .collect()
}

fn findings_cmp(left: &QueryLintFinding, right: &QueryLintFinding) -> std::cmp::Ordering {
    // A finding about a query rather than a schema type keeps the source
    // order it was found in (the sort is stable).
    let about_query = left.type_name.is_none() && right.type_name.is_none();
    severity_rank(left.severity)
        .cmp(&severity_rank(right.severity))
        .then_with(|| left.type_name.cmp(&right.type_name))
        .then_with(|| left.property.cmp(&right.property))
        .then_with(|| {
            if about_query {
                std::cmp::Ordering::Equal
            } else {
                left.message.cmp(&right.message)
            }
        })
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
