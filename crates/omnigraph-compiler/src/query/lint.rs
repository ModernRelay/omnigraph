use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::catalog::Catalog;
use crate::ir::core::{CoreDecl, MutationTarget, Sink};
use crate::query::codes::Q004;
use crate::query::descriptor::{QueryOperationDescriptor, describe_query_operation};
use crate::query::diagnostic::{QueryDiagnostic, QueryDiagnosticKind};
use crate::query::file::QueryDecl;
use crate::query::parser::parse_query_diagnostic;
use crate::query::value::CompOp;

const L201_CODE: &str = "L201";
const L202_CODE: &str = "L202";
const L203_CODE: &str = "L203";
const L204_CODE: &str = "L204";
const L205_CODE: &str = "L205";
const L206_CODE: &str = "L206";
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
    let parsed = match parse_query_diagnostic(query_source) {
        Ok(file) => file
            .body
            .into_read_declarations()
            .map_err(|message| vec![(Q004.as_str().to_string(), message)]),
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
            findings.extend(lint_undeclared_analyzers(catalog));
            findings.extend(lint_rewrites(catalog, query_source));
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
                .map(|(code, message)| QueryLintFinding {
                    severity: QueryLintSeverity::Error,
                    code,
                    message,
                    type_name: None,
                    property: None,
                    query_names: Vec::new(),
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
/// name) keeps its code and its rendered form.
fn front_door_finding(diagnostic: QueryDiagnostic) -> (String, String) {
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
    (code, message)
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
                type_name: Some(type_name.clone()),
                property: Some(property_name),
                query_names: type_coverage.query_names.iter().cloned().collect(),
            });
        }
    }
    findings
}

/// `L202`: a node property's full-text index declares no analyzer, so it keeps
/// edition 2's index analysis (the substrate's default) rather than a profile
/// the language versions. The finding names the profile to declare,
/// `english_v1` being the closest to that analysis
/// (docs/rfcs/2026-11-07-gq-3.md section 4, Schema: Text).
fn lint_undeclared_analyzers(catalog: &Catalog) -> Vec<QueryLintFinding> {
    use crate::schema::analysis::AnalyzerProfile;
    let mut findings = Vec::new();
    let mut type_names = catalog.node_types.keys().collect::<Vec<_>>();
    type_names.sort();
    for type_name in type_names {
        let node_type = &catalog.node_types[type_name];
        let mut property_names = node_type.properties.keys().collect::<Vec<_>>();
        property_names.sort();
        for property_name in property_names {
            if !node_type.has_full_text_index(property_name)
                || node_type.full_text_profile(property_name).is_some()
                || !declares_index(node_type, property_name)
            {
                continue;
            }
            let closest = AnalyzerProfile::CLOSEST_TO_EDITION_2;
            findings.push(QueryLintFinding {
                severity: QueryLintSeverity::Info,
                code: L202_CODE.to_string(),
                message: format!(
                    "{type_name}.{property_name}'s full-text index declares no analyzer and keeps edition 2's index analysis; declare a profile, `@index(fulltext, analyzer = \"{closest}\")` being the closest (it differs by the 40-byte token filter and NFC)"
                ),
                type_name: Some(type_name.clone()),
                property: Some(property_name.clone()),
                query_names: Vec::new(),
            });
        }
    }
    findings
}

/// The rewrites lint names (docs/rfcs/2026-11-07-gq-3.md, section 5):
///
/// - `L203`: `count { P } > 0`, which `queries fmt` writes `exists { P }`;
/// - `L204` and `L205`: `count { P } >= 1` and `count { P } = 0`, with the
///   rows of `exists { P }` and `not exists { P }` but another typed IR, so
///   the formatter does not apply them;
/// - `L206`: a `search`, `match_text` or `fuzzy` call over a field whose
///   index declares an analyzer, which is the `match_terms` call
///   `queries fmt` writes there.
fn lint_rewrites(catalog: &Catalog, query_source: &str) -> Vec<QueryLintFinding> {
    use crate::query::syntax::{Body, Decl};
    let Ok(file) = crate::query::syntax::parse::parse_source(query_source) else {
        return Vec::new();
    };
    let decls: Vec<&Decl> = match &file.body {
        Body::Decls(d) => d.iter().collect(),
        Body::Explain { before, decl } => before.iter().chain([decl.as_ref()]).collect(),
        Body::Statement(_) => Vec::new(),
    };
    let finding = |code: &str, message: String, decl: &str| QueryLintFinding {
        severity: QueryLintSeverity::Info,
        code: code.to_string(),
        message,
        type_name: None,
        property: None,
        query_names: vec![decl.to_string()],
    };
    let (_, rewrites) = crate::query::fmt::lexical_rewrites(catalog, &file);
    let mut findings = Vec::new();
    for decl in decls {
        for (at, operator, bound) in count_comparisons(decl) {
            let position = format!("line {}, column {}", at.line, at.column);
            let (code, message) = match (operator, bound) {
                (CompOp::Gt, 0) => (
                    L203_CODE,
                    format!(
                        "{position}: `count {{ … }} > 0` is `exists {{ … }}`, the same typed IR; `queries fmt` writes it"
                    ),
                ),
                (CompOp::Ge, 1) => (
                    L204_CODE,
                    format!(
                        "{position}: `count {{ … }} >= 1` has the rows of `exists {{ … }}`, which says it directly"
                    ),
                ),
                (CompOp::Eq, 0) => (
                    L205_CODE,
                    format!(
                        "{position}: `count {{ … }} = 0` has the rows of `not exists {{ … }}`, which says it directly"
                    ),
                ),
                _ => continue,
            };
            findings.push(finding(code, message, &decl.name));
        }
        for rewrite in rewrites.iter().filter(|r| r.decl == decl.name) {
            findings.push(finding(
            L206_CODE,
            format!(
                "`{}` is `{}` over a field whose index declares an analyzer; `queries fmt --schema` writes it",
                rewrite.from, rewrite.to
            ),
            &rewrite.decl,
            ));
        }
    }
    findings
}

/// Every `count { P } <op> <integer>` of `decl`, a block count with no
/// argument compared with an integer literal: where it was written, the
/// operator and the integer.
fn count_comparisons(
    decl: &crate::query::syntax::Decl,
) -> Vec<(crate::query::diagnostic::Position, CompOp, i128)> {
    use crate::query::syntax::{BlockFunc, Expr};
    use crate::query::value::{AggFunc, Literal};
    let mut out = Vec::new();
    crate::query::fmt::visit_decl_exprs(decl, &mut |e| {
        if let Expr::Compare {
            op, left, right, ..
        } = e
            && let (Expr::Block(b), Expr::Literal(Literal::Integer(n))) =
                (left.as_ref(), right.as_ref())
            && b.func == BlockFunc::Agg(AggFunc::Count)
            && b.arg.is_none()
        {
            out.push((b.at.position, *op, *n));
        }
    });
    out
}

/// Whether `property` carries a written one-column `@index`, beyond the
/// index its one-column `@key` implies: the declaration an analyzer belongs to.
fn declares_index(node_type: &crate::catalog::NodeType, property: &str) -> bool {
    let single =
        |columns: &Vec<String>| matches!(columns.as_slice(), [column] if column == property);
    let written = node_type
        .indices
        .iter()
        .filter(|columns| single(columns))
        .count();
    let implied = usize::from(node_type.key.as_ref().is_some_and(single));
    written > implied
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
