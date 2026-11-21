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
    assert_eq!(output.findings[0].code, "Q001");
}

/// A file declaring one name twice is refused whole, and the finding keeps
/// the code `T83` at the later declaration, one finding for each later
/// declaration.
#[test]
fn a_repeated_declaration_name_is_a_t83_finding() {
    let output = lint_query_file(
        &catalog("node Person { name: String }"),
        "query q() { match { $p: Person } return { $p.name } }\nquery q() { match { $p: Person } return { $p } }\nquery q() { match { $p: Person } return { $p } }\n",
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );
    assert_eq!(output.status, QueryLintStatus::Error);
    assert_eq!(output.queries_processed, 0);
    assert_eq!(output.findings.len(), 2);
    assert_eq!(output.errors, 2);
    assert!(
        output.findings[1].message.starts_with("line 3, column 7: "),
        "{}",
        output.findings[1].message
    );
    assert_eq!(output.findings[0].code, "T83");
    assert!(
        output.findings[0]
            .message
            .starts_with("line 2, column 7: type error: T83: `q` is declared twice"),
        "{}",
        output.findings[0].message
    );
}

#[test]
fn branch_statement_is_refused_as_a_parse_family_finding() {
    let output = lint_query_file(
        &catalog("node Person { name: String }"),
        "branch merge b0 into main",
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(output.status, QueryLintStatus::Error);
    assert_eq!(output.queries_processed, 0);
    assert_eq!(output.errors, 1);
    assert!(output.results.is_empty());
    assert_eq!(output.findings.len(), 1);
    assert_eq!(output.findings[0].code, "Q004");
    assert_eq!(
        output.findings[0].message,
        "`branch merge` is a branch statement, not a query declaration"
    );
}

#[test]
fn explain_statement_lints_its_wrapped_declaration() {
    let schema = catalog("node Person { name: String }");
    let output = lint_query_file(
        &schema,
        "explain query names() { match { $p: Person } return { $p.name } }",
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );
    assert_eq!(output.status, QueryLintStatus::Ok, "{:?}", output.findings);
    assert_eq!(output.queries_processed, 1);
    assert_eq!(output.results[0].name, "names");

    let output = lint_query_file(
        &schema,
        "explain query add() { insert Person { name: \"Ada\" } }",
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );
    assert_eq!(output.status, QueryLintStatus::Error);
    assert_eq!(
        output.findings[0].message,
        "`explain` applies to a read query; 'add' contains mutations"
    );
}

#[test]
fn refused_set_line_is_reported_with_its_position() {
    let output = lint_query_file(
        &catalog("node Person { name: String }"),
        "set merge_lineage = v3;\n",
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(output.status, QueryLintStatus::Error);
    assert_eq!(output.queries_processed, 0);
    assert_eq!(output.findings.len(), 1);
    assert_eq!(output.findings[0].code, "Q003");
    let message = &output.findings[0].message;
    assert!(message.starts_with("line 1, column "), "{message}");
    assert!(message.contains("unknown value `v3`"), "{message}");
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
match { $p: Person } where $p.slug = $slug
return { $p.name }
}

query bad_update($slug: String) {
match { $x: Person } where $x.slug = $slug
update $x set { missing: "nope" }
update $x set { nmae: "x" }
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
            name: "p.name".to_string(),
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
    // Every refusal of the query's pass, each where it stands.
    let refusals = &output.results[1].diagnostics;
    assert_eq!(refusals.len(), 2, "{refusals:?}");
    assert!(
        refusals[1]
            .message
            .contains("`nmae`; nearest: name (String?)")
    );
    assert_eq!(refusals[1].position.map(|at| at.line), Some(10));
    let serialized = serde_json::to_value(&output).unwrap();
    assert!(serialized["results"][0].get("diagnostics").is_none());
    assert_eq!(serialized["results"][1]["diagnostics"][1]["code"], "T11");
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
insert Knows { @src: $from, @dst: $to }
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
match { $x: Policy } where $x.slug = $slug
update $x set { name: $name }
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
match { $x: Doc } where $x.slug = $slug
update $x set { body: $body }
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
match { $x: Policy } where $x.slug = $slug
update $x set { name: $name }
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
match { $x: Policy } where $x.slug = $slug
update $x set { a_field: "x" }
}

query update_a($slug: String) {
match { $x: Policy } where $x.slug = $slug
update $x set { a_field: "x" }
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
match { $p: Person }
where not exists { $p worksAt $_ }
return { $p.name }
}


"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );

    assert_eq!(output.results.len(), 1);
    for result in &output.results {
        assert_eq!(
            result.operation.as_ref().unwrap().reads,
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
match { $x: Person } where $x.slug = $slug
delete $x
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
fn same_named_node_and_edge_mutation_keeps_the_typechecked_namespace_issue_921() {
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

    // Its endpoint assignments make an insert of the same name an edge insert.
    let output = lint_query_file(
        &catalog(
            r#"
node Shared { slug: String @key }
edge Shared: Shared -> Shared
"#,
        ),
        r#"
query link_shared($from: String, $to: String) {
insert Shared { @src: $from, @dst: $to }
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );
    let operation = output.results[0].operation.as_ref().unwrap();
    let edge = QueryGraphFact {
        kind: QueryGraphFactKind::Edge,
        type_name: "Shared".to_string(),
    };
    assert_eq!(operation.writes, vec![edge]);
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
    $d.slug,
    $d.count,
    $d.total,
    $d.score,
    $d.tags,
    $d.embedding,
    $d.createdAt,
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
            ("d.slug", QueryValueKind::String, None, None, false),
            ("d.count", QueryValueKind::Int, None, None, false),
            ("d.total", QueryValueKind::BigInt, None, None, false),
            ("d.score", QueryValueKind::Float, None, None, false),
            (
                "d.tags",
                QueryValueKind::List,
                Some(QueryValueKind::Date),
                None,
                true,
            ),
            ("d.embedding", QueryValueKind::Vector, None, Some(3), false,),
            ("d.createdAt", QueryValueKind::DateTime, None, None, true,),
            ("d", QueryValueKind::Object, None, None, false),
        ]
    );

    let serialized = serde_json::to_value(&output).unwrap();
    assert_eq!(
        serialized["results"][0]["operation"]["result"][4],
        serde_json::json!({
            "name": "d.tags",
            "kind": "list",
            "item_kind": "date",
            "nullable": true
        })
    );
    assert_eq!(
        serialized["results"][0]["operation"]["result"][5],
        serde_json::json!({
            "name": "d.embedding",
            "kind": "vector",
            "vector_dim": 3,
            "nullable": false
        })
    );
}

#[test]
fn selected_scopes_contribute_every_read_fact_issue_659() {
    let schema = catalog(
        "node Person { name: String } node Company { name: String } edge WorksAt: Person -> Company edge Employs: Company -> Person",
    );
    for selector in ["(worksAt | employs)", "*"] {
        let output = lint_query_file(
            &schema,
            &format!(
                "query q() {{ match {{ $p: Person }} where not exists {{ $p {selector} $c, $c: Company }} return {{ $p.name }} }}"
            ),
            "/tmp/queries.gq",
            QueryLintSchemaSource::file("/tmp/schema.pg"),
        );
        assert_eq!(
            output.results[0].operation.as_ref().unwrap().reads,
            vec![
                QueryGraphFact {
                    kind: QueryGraphFactKind::Node,
                    type_name: "Company".into()
                },
                QueryGraphFact {
                    kind: QueryGraphFactKind::Node,
                    type_name: "Person".into()
                },
                QueryGraphFact {
                    kind: QueryGraphFactKind::Edge,
                    type_name: "Employs".into()
                },
                QueryGraphFact {
                    kind: QueryGraphFactKind::Edge,
                    type_name: "WorksAt".into()
                },
            ]
        );
    }
}

/// The count-block rewrites lint names (the GQ 3 RFC,
/// sections 5 and 6), each at the block's position and with its fix: `L203`
/// for `count { } > 0`, a canonical form whose fix keeps the typed IR, and
/// `L204` and `L205` for the rows-equal `count { } >= 1` and
/// `count { } = 0`, whose fix changes the IR. A count compared with another
/// bound draws none. Findings about queries keep source order.
#[test]
fn l203_to_l205_name_the_count_block_rewrites_with_their_fixes() {
    let output = lint_query_file(
        &catalog(
            r#"
node Person { name: String @key }
edge Knows: Person -> Person
"#,
        ),
        r#"query some() {
  match { $p: Person }
  where count { $q: Person, $p knows $q } > 0
  return { $p.name }
}
query at_least_one() {
  match { $p: Person }
  where count { $q: Person, $p knows $q } >= 1
  return { $p.name }
}
query none() {
  match { $p: Person }
  where count { $q: Person, $p knows $q } = 0 and count { $q: Person, $q knows $p } > 1
  return { $p.name }
}
"#,
        "/tmp/queries.gq",
        QueryLintSchemaSource::file("/tmp/schema.pg"),
    );
    let findings = output
        .findings
        .iter()
        .map(|f| {
            (
                f.code.as_str(),
                f.query_names.join(","),
                f.message.as_str(),
                f.fix.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        findings,
        vec![
            (
                "L203",
                "some".to_string(),
                "line 3, column 9: `count { $q: Person, $p knows $q } > 0` is `exists { $q: Person, $p knows $q }`, the same typed IR",
                Some("exists { $q: Person, $p knows $q }"),
            ),
            (
                "L204",
                "at_least_one".to_string(),
                "line 8, column 9: `count { $q: Person, $p knows $q } >= 1` has the rows of `exists { $q: Person, $p knows $q }`, which says it directly",
                Some("exists { $q: Person, $p knows $q }"),
            ),
            (
                "L205",
                "none".to_string(),
                "line 13, column 9: `count { $q: Person, $p knows $q } = 0` has the rows of `not exists { $q: Person, $p knows $q }`, which says it directly",
                Some("not exists { $q: Person, $p knows $q }"),
            ),
        ]
    );
    assert!(
        output
            .findings
            .iter()
            .all(|f| f.severity == QueryLintSeverity::Info)
    );
    assert_eq!(output.status, QueryLintStatus::Ok);
}

/// Every diagnostic `lint` reports carries its fix and, where the fix is
/// machine-applicable, its byte-range edits: a parse refusal of an edition-2
/// form, a type refusal of a removed call and a canonical form alike. The
/// edits are exactly what `lint --fix` (`fix_source`) applies: applying the
/// reported edits gives the text `fix_source` writes.
#[test]
fn every_reported_fix_carries_the_edits_lint_fix_applies() {
    use crate::query::diagnostic::TextEdit;
    use crate::query::fix::{FixOptions, fix_source};
    let catalog = catalog("node Person { name: String @key\n age: I32? }\n");
    for (label, source, code) in [
        (
            "an edition-2 filter in match",
            "query q() { match { $p: Person  $p.age > 30 } return { $p.name } }",
            "Q017",
        ),
        (
            "fuzzy with a positional budget",
            "query q($s: String) { match { $p: Person } where fuzzy($p.name, $s, 1) return { $p.name } }",
            "T6",
        ),
        (
            "a canonical form",
            "query q() { match { $p: Person } return { $p.name } order { $p.name asc } }",
            "L208",
        ),
    ] {
        let out = lint_query_file(
            &catalog,
            source,
            "q.gq",
            QueryLintSchemaSource::file("s.pg"),
        );
        let mut reported: Vec<(String, Option<String>, Vec<TextEdit>)> = out
            .findings
            .iter()
            .map(|f| {
                (
                    f.code.clone(),
                    f.fix.clone(),
                    f.suggestion
                        .as_ref()
                        .map(|s| s.edits.clone())
                        .unwrap_or_default(),
                )
            })
            .collect();
        reported.extend(out.results.iter().flat_map(|r| &r.diagnostics).map(|d| {
            (
                d.code.as_str().to_string(),
                d.fix.clone(),
                d.suggestion
                    .as_deref()
                    .map(|s| s.edits.clone())
                    .unwrap_or_default(),
            )
        }));
        assert_eq!(reported.len(), 1, "{label}: {reported:?}");
        let (reported_code, fix, mut edits) = reported.remove(0);
        assert_eq!(reported_code, code, "{label}");
        assert!(fix.is_some(), "{label}: no fix");
        assert!(!edits.is_empty(), "{label}: no edits");
        edits.sort_by_key(|e| std::cmp::Reverse(e.start));
        let mut text = source.to_string();
        for edit in edits {
            text.replace_range(edit.start..edit.end, &edit.replacement);
        }
        assert_eq!(
            fix_source(&catalog, source, FixOptions::default()).fixed,
            Some(text),
            "{label}"
        );
    }
}
