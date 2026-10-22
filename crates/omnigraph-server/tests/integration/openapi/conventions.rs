//! HTTP convention checks share the generated-spec owner and its fixtures.

use omnigraph_server::api::ErrorCode;
use serde_json::Value;
use std::collections::BTreeSet;

use super::openapi_json;

const GUIDE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/dev/http-api.md"
));

#[derive(Clone, Copy)]
struct Convention<'a> {
    check: &'a str,
    scope: &'a str,
    value: &'a str,
}

fn conventions() -> Vec<Convention<'static>> {
    let mut seen = BTreeSet::new();
    GUIDE
        .lines()
        .filter(|line| line.starts_with("| `"))
        .map(|line| {
            let cells: Vec<_> = line
                .trim_matches('|')
                .split('|')
                .map(|cell| cell.trim().trim_matches('`'))
                .collect();
            assert_eq!(cells.len(), 4, "four convention columns: {line}");
            assert!(cells.iter().all(|cell| !cell.is_empty()), "{line}");
            assert!(seen.insert((cells[0], cells[1])), "duplicate row: {line}");
            Convention {
                check: cells[0],
                scope: cells[1],
                value: cells[2],
            }
        })
        .collect()
}

fn convention<'a>(rows: &'a [Convention<'a>], check: &str) -> &'a str {
    let matching: Vec<_> = rows.iter().filter(|row| row.check == check).collect();
    assert_eq!(matching.len(), 1, "one convention for {check}");
    matching[0].value
}

fn exception<'a>(
    rows: &[Convention<'a>],
    used: &mut BTreeSet<(&'a str, &'a str)>,
    check: &str,
    scope: &str,
) -> bool {
    rows.iter()
        .find(|row| row.check == check && row.scope == scope)
        .is_some_and(|row| {
            used.insert((row.check, row.scope));
            true
        })
}

fn words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|word| !word.is_empty())
}

fn named_schemas(value: &Value, names: &mut BTreeSet<String>) {
    match value {
        Value::Object(object) => {
            if let Some(name) = object.get("$ref").and_then(Value::as_str) {
                names.insert(name.rsplit('/').next().unwrap().to_string());
            }
            for value in object.values() {
                named_schemas(value, names);
            }
        }
        Value::Array(values) => {
            for value in values {
                named_schemas(value, names);
            }
        }
        _ => {}
    }
}

fn check_header(name: &str, location: &str, errors: &mut Vec<String>) {
    if !name.split('-').all(|part| {
        part.starts_with(|character: char| character.is_ascii_uppercase())
            && part
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
    }) {
        errors.push(format!("{location}: header {name} must use Title-Case"));
    }
}

fn convention_errors(doc: &Value) -> Vec<String> {
    let rows = conventions();
    let mut errors = Vec::new();
    let mut used = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut request_schemas = BTreeSet::new();
    let mut response_schemas = BTreeSet::new();
    let mut flat_bindings = BTreeSet::new();
    let tags: BTreeSet<_> = doc["tags"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tag| tag["name"].as_str())
        .collect();
    let graph_prefix = convention(&rows, "graph-prefix");
    let path_parameters: BTreeSet<_> = convention(&rows, "path-parameters")
        .split_whitespace()
        .collect();
    // Fixed checks deliberately have no expression language or plugins.
    for (check, spelling) in [
        ("operation-id", "lowerCamelCase"),
        ("query-parameters", "snake_case"),
        ("parameter-descriptions", "required"),
        ("header-names", "Title-Case"),
        ("limit-bounds", "minimum maximum default"),
        ("operation-tags", "declared"),
    ] {
        assert_eq!(convention(&rows, check), spelling, "unsupported {check}");
    }
    for (path, item) in doc["paths"].as_object().unwrap() {
        for (method, operation) in item.as_object().unwrap() {
            let Some(id) = operation["operationId"].as_str() else {
                errors.push(format!("{method} {path}: missing operationId"));
                continue;
            };
            if !ids.insert(id) {
                errors.push(format!("duplicate operationId {id}"));
            }
            if path.starts_with(graph_prefix) {
                let camel = id.starts_with(|character: char| character.is_ascii_lowercase())
                    && id
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric());
                if !camel && !exception(&rows, &mut used, "operation-id-exception", id) {
                    errors.push(format!("{id}: operationId must use lowerCamelCase"));
                }
            } else if let Some(row) = rows
                .iter()
                .find(|row| row.check == "flat-path" && row.scope == path)
            {
                let binding = format!("{method}={id}");
                if !row
                    .value
                    .split_whitespace()
                    .any(|expected| expected == binding)
                {
                    errors.push(format!("{path}: unlisted flat operation {binding}"));
                }
                flat_bindings.insert((path.as_str(), binding));
            } else {
                errors.push(format!("{path}: no declared path class"));
            }
            let operation_tags = operation["tags"].as_array();
            if !operation_tags.is_some_and(|operation_tags| {
                !operation_tags.is_empty()
                    && operation_tags
                        .iter()
                        .all(|tag| tag.as_str().is_some_and(|tag| tags.contains(tag)))
            }) {
                errors.push(format!("{id}: every operation tag must be declared"));
            }
            for parameter in operation["parameters"].as_array().into_iter().flatten() {
                let name = parameter["name"].as_str().unwrap();
                if !parameter["description"]
                    .as_str()
                    .is_some_and(|description| !description.trim().is_empty())
                {
                    errors.push(format!("{id}: parameter {name} needs a description"));
                }
                match parameter["in"].as_str().unwrap() {
                    "path" => {
                        let legacy = rows.iter().find(|row| {
                            row.check == "path-parameter-exception"
                                && row.scope == path
                                && row.value == name
                        });
                        if let Some(row) = legacy {
                            used.insert((row.check, row.scope));
                        } else if !path_parameters.contains(name) {
                            errors.push(format!("{id}: unqualified path parameter {name}"));
                        }
                        if !path.contains(&format!("{{{name}}}")) {
                            errors.push(format!("{id}: path has no placeholder for {name}"));
                        }
                    }
                    "query" => {
                        if !name.starts_with(|character: char| character.is_ascii_lowercase())
                            || !name.chars().all(|character| {
                                character.is_ascii_lowercase()
                                    || character.is_ascii_digit()
                                    || character == '_'
                            })
                        {
                            errors
                                .push(format!("{id}: query parameter {name} must use snake_case"));
                        }
                    }
                    "header" => check_header(name, id, &mut errors),
                    _ => {}
                }
                if name == "limit" {
                    let minimum = parameter["schema"]["minimum"].as_u64();
                    let maximum = parameter["schema"]["maximum"].as_u64();
                    let description = parameter["description"].as_str().unwrap_or_default();
                    if !matches!((minimum, maximum), (Some(min), Some(max)) if min > 0 && max >= min)
                        || !description.to_ascii_lowercase().contains("default")
                        || !maximum.is_some_and(|max| {
                            words(description).any(|word| word == max.to_string())
                        })
                    {
                        errors.push(format!("{id}: limit needs positive bounds and its default and ceiling described"));
                    }
                    match parameter["schema"]["default"].as_u64() {
                        Some(default) => {
                            if !matches!((minimum, maximum), (Some(min), Some(max)) if min <= default && default <= max)
                                || !words(description).any(|word| word == default.to_string())
                            {
                                errors.push(format!(
                                    "{id}: limit default must fit its bounds and description"
                                ));
                            }
                        }
                        None if parameter["schema"].get("default").is_none()
                            && exception(&rows, &mut used, "limit-default-exception", path) => {}
                        None => errors.push(format!("{id}: limit needs an integer schema default")),
                    }
                }
            }
            named_schemas(&operation["requestBody"]["content"], &mut request_schemas);
            for (status, response) in operation["responses"].as_object().unwrap() {
                named_schemas(&response["content"], &mut response_schemas);
                for name in response["headers"]
                    .as_object()
                    .into_iter()
                    .flatten()
                    .map(|(name, _)| name)
                {
                    check_header(name, id, &mut errors);
                }
                if let Ok(status) = status.parse::<u16>()
                    && status >= 400
                {
                    let description = response["description"].as_str().unwrap_or_default();
                    for code in ErrorCode::ALL {
                        if words(description).any(|word| word == code.as_str())
                            && !code.http_statuses().contains(&status)
                        {
                            errors
                                .push(format!("{id}: {status} cannot describe {}", code.as_str()));
                        }
                    }
                }
            }
        }
    }
    for row in rows.iter().filter(|row| row.check == "flat-path") {
        for binding in row.value.split_whitespace() {
            if !flat_bindings.contains(&(row.scope, binding.to_string())) {
                errors.push(format!(
                    "missing declared flat operation {} {binding}",
                    row.scope
                ));
            }
        }
    }
    for name in &request_schemas {
        if !name.ends_with(convention(&rows, "request-schema-suffix")) {
            errors.push(format!("request schema {name} needs its request suffix"));
        }
    }
    for name in &response_schemas {
        if !name.ends_with(convention(&rows, "response-schema-suffix"))
            && !exception(&rows, &mut used, "response-schema-exception", name)
        {
            errors.push(format!("response schema {name} needs its output suffix"));
        }
        let schema = &doc["components"]["schemas"][name];
        if let Some(properties) = schema["properties"].as_object()
            && properties.contains_key(convention(&rows, "page-envelope"))
            && !properties
                .values()
                .any(|property| property["type"] == "array")
        {
            errors.push(format!(
                "paged response {name} must carry a named items array"
            ));
        }
    }
    for (name, schema) in doc["components"]["schemas"].as_object().unwrap() {
        if schema["enum"].is_array()
            && convention(&rows, "enum-schema-suffixes")
                .split_whitespace()
                .any(|suffix| name.ends_with(suffix))
            && !exception(&rows, &mut used, "enum-schema-exception", name)
        {
            errors.push(format!("enum {name} must not have a transport suffix"));
        }
    }
    for row in rows.iter().filter(|row| row.check.ends_with("-exception")) {
        if !used.contains(&(row.check, row.scope)) {
            errors.push(format!("unused {} for {}", row.check, row.scope));
        }
    }
    errors
}

#[test]
fn spec_follows_the_api_conventions() {
    let errors = convention_errors(&openapi_json());
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn change_page_limits_match_engine_bounds() {
    let doc = openapi_json();
    for path in [
        "/graphs/{graph_id}/changes",
        "/graphs/{graph_id}/commits/{commit_id}/changes",
        "/graphs/{graph_id}/diff",
        "/graphs/{graph_id}/branches/diff",
    ] {
        let limit = doc["paths"][path]["get"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .find(|parameter| parameter["name"] == "limit")
            .unwrap();
        assert_eq!(limit["schema"]["minimum"], 1, "{path}");
        assert_eq!(
            limit["schema"]["maximum"],
            serde_json::json!(omnigraph::changes::COMMIT_CHANGES_MAX_CHANGES),
            "{path}"
        );
        assert_eq!(
            limit["schema"]["default"],
            serde_json::json!(omnigraph::changes::COMMIT_CHANGES_DEFAULT_CHANGES),
            "{path}"
        );
    }
}

#[test]
fn conventions_reject_new_deviations() {
    let doc = openapi_json();
    assert!(convention_errors(&doc).is_empty());
    for defect in ["operationId", "tag", "status", "limit", "limit-default"] {
        let mut changed = doc.clone();
        let expected = match defect {
            "operationId" => {
                changed["paths"]["/graphs/{graph_id}/query"]["post"]["operationId"] =
                    "another_snake_case_id".into();
                "operationId must use lowerCamelCase"
            }
            "tag" => {
                changed["paths"]["/graphs/{graph_id}/query"]["post"]["tags"] =
                    serde_json::json!(["undeclared"]);
                "every operation tag must be declared"
            }
            "status" => {
                changed["paths"]["/graphs/{graph_id}/query"]["post"]["responses"]["400"]["description"] =
                    ErrorCode::OutcomeUnknown.as_str().into();
                "400 cannot describe outcome_unknown"
            }
            "limit" => {
                let limit = changed["paths"]["/graphs/{graph_id}/commits"]["get"]["parameters"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|parameter| parameter["name"] == "limit")
                    .unwrap();
                limit["schema"].as_object_mut().unwrap().remove("maximum");
                "limit needs positive bounds"
            }
            "limit-default" => {
                let limit = changed["paths"]["/graphs/{graph_id}/changes"]["get"]["parameters"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|parameter| parameter["name"] == "limit")
                    .unwrap();
                limit["schema"].as_object_mut().unwrap().remove("default");
                "limit needs an integer schema default"
            }
            _ => unreachable!(),
        };
        let errors = convention_errors(&changed);
        assert!(
            errors.iter().any(|error| error.contains(expected)),
            "{defect}: {errors:?}"
        );
    }
}

// Existing handler capabilities, until the shared operation inventory replaces
// these assertions. A POST may read, and a stored query may write.
const OWNED_WRITE_OPERATIONS: &[&str] = &[
    "cluster_putBlob",
    "cluster_clearBlob",
    "cluster_mutate",
    "cluster_mutate_if_graph_commit",
    "cluster_invoke_query",
    "cluster_invoke_query_if_graph_commit",
    "cluster_load",
    "cluster_loadNdjson",
    "cluster_createBranch",
    "cluster_deleteBranch",
    "cluster_mergeBranches",
    "cluster_publishCommit",
    "cluster_dropCommit",
    "deployment_apply",
];

fn required_response_errors(doc: &Value, names: &[&str], code: ErrorCode) -> Vec<String> {
    let mut errors = Vec::new();
    for name in names {
        let operation = doc["paths"]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|item| item.as_object().unwrap().values())
            .find(|operation| operation["operationId"] == *name);
        let Some(operation) = operation else {
            errors.push(format!("missing required operation {name}"));
            continue;
        };
        for status in code.http_statuses() {
            let response = &operation["responses"][status.to_string()];
            if !response["description"]
                .as_str()
                .is_some_and(|description| words(description).any(|word| word == code.as_str()))
            {
                errors.push(format!("{name} must document {status} {}", code.as_str()));
            }
        }
    }
    errors
}

fn required_write_response_errors(doc: &Value) -> Vec<String> {
    required_response_errors(doc, OWNED_WRITE_OPERATIONS, ErrorCode::OutcomeUnknown)
}

#[test]
fn owned_writes_document_unknown_outcomes() {
    let errors = required_write_response_errors(&openapi_json());
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn handlers_document_internal_failures() {
    let doc = openapi_json();
    let mut errors = required_response_errors(&doc, OWNED_WRITE_OPERATIONS, ErrorCode::Internal);
    // These reads can fail internally too. Discovery and deployment lookup
    // have narrower refusal contracts; neither is inferred from its method.
    errors.extend(required_response_errors(
        &doc,
        &[
            "cluster_getAccess",
            "cluster_getBlob",
            "cluster_headBlob",
            "cluster_listBranches",
            "cluster_getBranchDiff",
            "cluster_getMergeBase",
            "cluster_pollChanges",
            "cluster_captureChangeBaseline",
            "cluster_listCommits",
            "cluster_getCommit",
            "cluster_getCommitChanges",
            "cluster_getDiff",
            "cluster_export",
            "cluster_list_queries",
            "cluster_query",
            "cluster_getSchema",
            "cluster_getSnapshot",
            "listGraphs",
            "deployment_status",
            "deployment_plan",
        ],
        ErrorCode::Internal,
    ));
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn required_write_coverage_detects_a_missing_response_or_code() {
    let doc = openapi_json();
    assert!(required_write_response_errors(&doc).is_empty());
    let status = ErrorCode::OutcomeUnknown.http_statuses()[0].to_string();
    for remove_response in [true, false] {
        let mut changed = doc.clone();
        let responses = changed["paths"]["/graphs/{graph_id}/mutate"]["post"]["responses"]
            .as_object_mut()
            .unwrap();
        if remove_response {
            responses.remove(&status);
        } else {
            let description = responses[&status]["description"]
                .as_str()
                .unwrap()
                .replace(ErrorCode::OutcomeUnknown.as_str(), "");
            responses.get_mut(&status).unwrap()["description"] = Value::String(description);
        }
        let errors = required_write_response_errors(&changed);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("cluster_mutate"));
    }
}
