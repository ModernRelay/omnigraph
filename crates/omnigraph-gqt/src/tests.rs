use super::*;

const HDR: &str = "# issue: none\n";
const SCHEMA: &str = "--- schema\nnode Person {\n    name: String @key\n}\n";
const SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n";
const QUERY: &str =
    "--- query\nquery all() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
const EXPECT: &str = "--- expect unordered\n{\"p.name\": \"alice\"}\n";
const MUTATE: &str = "--- mutate\nquery ins($n: String) {\n    insert Person { name: $n }\n}\n";
const PARAMS: &str = "--- params\n{\"n\": \"bob\"}\n";
const EXPECT_OK: &str = "--- expect ok\n";

fn refusal(stem: &str, text: &str) -> String {
    parse_case(stem, text).expect_err("expected the case to be refused")
}

#[test]
fn parses_a_minimal_case() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}");
    let case = parse_case("minimal", &text).unwrap();
    assert_eq!(case.items.len(), 1);
    assert!(!case.needs_indices);
    assert_eq!(case.traversal, None);
}

#[test]
fn header_notes_repeat_and_continuation_lines_are_refused() {
    let text = format!(
        "# issue: 7\n# red_on: 2026-01-01, the run\n# notes: returned 8,\n# notes: not 20.\n{SCHEMA}{SEED}{QUERY}{EXPECT}"
    );
    parse_case("issue_7_notes", &text).unwrap();
    let text = format!(
        "# issue: 7\n# red_on: 2026-01-01, the run\n#   returned 8: not 20.\n{SCHEMA}{SEED}{QUERY}{EXPECT}"
    );
    assert!(refusal("issue_7_x", &text).contains("unknown header key"));
    // The three misspellings that the old prose branch swallowed silently.
    for typo in [
        "# Traversal: indexed",
        "# traversal : indexed",
        "# traversal=csr",
    ] {
        let text = format!("{HDR}{typo}\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
        let reason = refusal("x", &text);
        assert!(
            reason.contains("unknown header key") || reason.contains("not `# <key>: <value>`"),
            "{typo}: {reason}"
        );
    }
}

/// Bounded exhaustive walk of the header-line typo space: key spelling,
/// separator, leading and trailing whitespace, and the gap before the value.
/// A line is accepted exactly when it equals the canonical
/// `# traversal: indexed`, so a future key inherits the same proof.
#[test]
fn header_lines_are_accepted_only_in_canonical_form() {
    let keys = [
        "traversal",
        "Traversal",
        "TRAVERSAL",
        "traversa1",
        "traversal_",
        " traversal",
    ];
    let seps = [":", " :", "=", "", "::"];
    let leads = ["# ", "#", "#  ", " # "];
    let gaps = [" ", "", "  ", "\t"];
    let trails = ["", " ", "\t"];
    let canonical = canonical_header_line("traversal", "indexed");
    // Distinct lines: two lead/key pairs collide (`#` + ` traversal` = `# ` +
    // `traversal`, and `# ` + ` traversal` = `#  ` + `traversal`), 120
    // duplicates over the 1440 grid points, so the set is what gets walked.
    let mut lines = std::collections::BTreeSet::new();
    for lead in leads {
        for key in keys {
            for sep in seps {
                for gap in gaps {
                    for trail in trails {
                        lines.insert(format!("{lead}{key}{sep}{gap}indexed{trail}"));
                    }
                }
            }
        }
    }
    assert_eq!(lines.len(), 1320, "the typo space is 1320 distinct lines");
    let mut accepted = 0;
    for line in &lines {
        let text = format!("{HDR}{line}\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
        match parse_case("x", &text) {
            Ok(case) => {
                assert_eq!(line, &canonical, "accepted a non-canonical line");
                assert_eq!(case.traversal, Some("indexed"));
                accepted += 1;
            }
            Err(e) => assert_ne!(line, &canonical, "refused the canonical line: {e}"),
        }
    }
    assert_eq!(accepted, 1);
}

#[test]
fn refuses_missing_issue_header() {
    let text = format!("# notes: no anchor\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("# issue:"));
}

#[test]
fn refuses_numbered_issue_without_red_on() {
    let text = format!("# issue: 7\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7_x", &text).contains("red_on"));
}

#[test]
fn refuses_header_line_without_a_key() {
    let text = format!("# stray prose\n{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("not `# <key>: <value>`"));
}

#[test]
fn refuses_unknown_header_key() {
    let text = format!("{HDR}# owner: me\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("unknown header key"));
}

#[test]
fn refuses_bad_traversal_mode() {
    let text = format!("{HDR}# traversal: bogus\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("indexed"));
}

#[test]
fn refuses_non_header_line_before_first_section() {
    let text = format!("{HDR}stray\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("precede the first section"));
}

#[test]
fn refuses_comment_line_in_seed() {
    let text = format!("{HDR}{SCHEMA}--- seed\n# a comment\n{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("seed"));
}

#[test]
fn refuses_comment_line_in_expect_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n# nope\n");
    assert!(refusal("x", &text).contains("expect"));
}

#[test]
fn refuses_seed_before_schema() {
    let text = format!("{HDR}{SEED}{SCHEMA}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("first section"));
}

#[test]
fn refuses_missing_seed_section() {
    let text = format!("{HDR}{SCHEMA}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("second section"));
}

#[test]
fn refuses_case_without_a_query_or_mutate_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}");
    assert!(refusal("x", &text).contains("at least one query or mutate step"));
}

#[test]
fn refuses_restart_only_step_list() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- restart\n");
    assert!(refusal("x", &text).contains("at least one query or mutate step"));
}

#[test]
fn refuses_second_declaration_in_one_section() {
    let two = "--- query\nquery a() {\n    match { $p: Person }\n    return { $p.name }\n}\nquery b() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{two}{EXPECT}");
    assert!(refusal("x", &text).contains("exactly one declaration"));
}

#[test]
fn refuses_mutation_declaration_under_query() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- query\nquery ins($n: String) {{\n    insert Person {{ name: $n }}\n}}\n{EXPECT}"
    );
    assert!(refusal("x", &text).contains("use `--- mutate`"));
}

#[test]
fn refuses_read_declaration_under_mutate() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- mutate\nquery all() {{\n    match {{ $p: Person }}\n    return {{ $p.name }}\n}}\n{EXPECT_OK}"
    );
    assert!(refusal("x", &text).contains("use `--- query`"));
}

#[test]
fn refuses_bare_expect() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect\n");
    assert!(refusal("x", &text).contains("mode word"));
}

#[test]
fn refuses_error_expect_without_substring() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect error:\n");
    assert!(refusal("x", &text).contains("substring"));
}

#[test]
fn refuses_error_expect_with_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect error: boom\nbody\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_affected_expect_missing_a_count() {
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}--- expect affected: nodes=1\n");
    assert!(refusal("x", &text).contains("nodes=<N> edges=<M>"));
}

#[test]
fn refuses_unknown_expect_mode() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect sorted\n");
    assert!(refusal("x", &text).contains("unknown expect mode"));
}

#[test]
fn refuses_row_expect_on_a_mutate_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}{EXPECT}");
    assert!(refusal("x", &text).contains("carry no rows"));
}

#[test]
fn refuses_ok_expect_on_a_query_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT_OK}");
    assert!(refusal("x", &text).contains("a query step takes"));
}

#[test]
fn refuses_query_step_without_expect() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}");
    assert!(refusal("x", &text).contains("missing its `--- expect`"));
}

#[test]
fn refuses_expect_with_no_step_to_bind_to() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- restart\n{EXPECT}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("no query or mutate step to bind to"));
}

#[test]
fn refuses_params_without_a_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}{PARAMS}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("directly follow"));
}

#[test]
fn refuses_second_params_for_one_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}{PARAMS}{EXPECT_OK}");
    assert!(refusal("x", &text).contains("second `--- params`"));
}

#[test]
fn refuses_restart_with_a_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- restart\nstray\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_unknown_section() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- teardown\n");
    assert!(refusal("x", &text).contains("unknown section"));
}

#[test]
fn refuses_schema_out_of_position() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}{SCHEMA}");
    assert!(refusal("x", &text).contains("out of position"));
}

#[test]
fn refuses_negative_loop_bound() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i -1 2\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("non-negative"));
}

#[test]
fn refuses_empty_loop_range() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 3 3\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("empty loop range"));
}

#[test]
fn refuses_foreach_without_values() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- foreach $x\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("no values"));
}

#[test]
fn refuses_foreach_value_outside_charset() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- foreach $x a\"b\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("[A-Za-z0-9_.-]"));
}

#[test]
fn refuses_bad_loop_variable_name() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $I 0 2\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("$[a-z][a-z0-9_]*"));
}

#[test]
fn refuses_nested_loops() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- loop $i 0 2\n--- loop $j 0 2\n{QUERY}{EXPECT}--- endloop\n--- endloop\n"
    );
    assert!(refusal("x", &text).contains("may not nest"));
}

#[test]
fn refuses_endloop_without_a_loop() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("without an open loop"));
}

#[test]
fn refuses_unclosed_loop() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 2\n{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("not closed"));
}

#[test]
fn refuses_loop_enclosing_no_steps() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 2\n--- endloop\n{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("enclosing no steps"));
}

#[test]
fn refuses_substitution_marker_in_query_body() {
    let query = "--- query\nquery all() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    let text =
        format!("{HDR}{SCHEMA}{SEED}{query}{EXPECT}").replace("$p.name }", "$p.name } // ${i}");
    assert!(refusal("x", &text).contains("only inside a params or expect body"));
}

#[test]
fn refuses_substitution_marker_in_seed() {
    let text = format!(
        "{HDR}{SCHEMA}--- seed\n{{\"type\":\"Person\",\"data\":{{\"name\":\"${{i}}\"}}}}\n{QUERY}{EXPECT}"
    );
    assert!(refusal("x", &text).contains("only inside a params or expect body"));
}

#[test]
fn refuses_substitution_outside_a_loop() {
    let text =
        format!("{HDR}{SCHEMA}{SEED}{MUTATE}--- params\n{{\"n\": \"${{who}}\"}}\n{EXPECT_OK}");
    assert!(refusal("x", &text).contains("outside a loop"));
}

#[test]
fn refuses_substitution_naming_the_wrong_variable() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- foreach $who bob\n{MUTATE}--- params\n{{\"n\": \"${{other}}\"}}\n{EXPECT_OK}--- endloop\n"
    );
    assert!(refusal("x", &text).contains("enclosing loop's variable"));
}

#[test]
fn refuses_unterminated_substitution() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- foreach $who bob\n{MUTATE}--- params\n{{\"n\": \"${{who\n{EXPECT_OK}--- endloop\n"
    );
    assert!(refusal("x", &text).contains("unterminated"));
}

#[test]
fn refuses_file_name_disagreeing_with_issue_header() {
    let text = format!("# issue: 7\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_8_wrong", &text).contains("disagrees"));
}

#[test]
fn refuses_issue_prefix_without_number_or_short_name() {
    let text = format!("# issue: 7\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7", &text).contains("issue_<N>_<short_name>"));
    assert!(refusal("issue_x", &text).contains("issue_<N>_<short_name>"));
}

#[test]
fn refuses_feature_name_with_numbered_issue_header() {
    let text = format!("# issue: 7\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("feature_name", &text).contains("issue_7_<short_name>"));
}

#[test]
fn refuses_file_name_outside_charset() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("Bad-Name", &text).contains("[a-z0-9_]"));
}

#[test]
fn refuses_ordered_expect_without_an_order_clause() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect ordered\n{{\"p.name\": \"alice\"}}\n");
    assert!(refusal("x", &text).contains("order` clause"));
}

#[test]
fn refuses_embed_schema() {
    let schema = "--- schema\nnode Doc {\n    slug: String @key\n    text: String\n    vec: Vector(4) @embed(\"text\")\n}\n";
    let seed = "--- seed\n";
    let text = format!("{HDR}{schema}{seed}{QUERY}{EXPECT}")
        .replace("$p: Person", "$p: Doc")
        .replace("$p.name", "$p.slug");
    assert!(refusal("x", &text).contains("@embed"));
}

#[test]
fn refuses_nearest_over_a_string_literal() {
    let query = "--- query\nquery q() {\n    match { $p: Person }\n    return { $p.name }\n    order { nearest($p.name, \"alpha\") }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n");
    assert!(refusal("x", &text).contains("string argument"));
}

#[test]
fn refuses_nearest_over_a_string_param() {
    let query = "--- query\nquery q($q: String) {\n    match { $p: Person }\n    return { $p.name }\n    order { nearest($p.name, $q) }\n}\n";
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{query}--- params\n{{\"q\": \"alpha\"}}\n--- expect unordered\n"
    );
    assert!(refusal("x", &text).contains("string argument"));
}

#[test]
fn accepts_empty_expect_body_as_empty_result_assertion() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n");
    parse_case("x", &text).unwrap();
}

#[test]
fn search_construct_sets_the_index_decision() {
    let schema = "--- schema\nnode Doc {\n    slug: String @key\n    text: String @index\n}\n";
    let query = "--- query\nquery q($q: String) {\n    match {\n        $d: Doc\n        search($d.text, $q)\n    }\n    return { $d.slug }\n}\n";
    let text = format!(
        "{HDR}{schema}--- seed\n{query}--- params\n{{\"q\": \"needle\"}}\n--- expect unordered\n"
    );
    let case = parse_case("x", &text).unwrap();
    assert!(case.needs_indices);
}

#[test]
fn normalization_equates_integer_and_float_spellings() {
    let a: Value = serde_json::from_str("{\"total\": 2}").unwrap();
    let b: Value = serde_json::from_str("{\"total\": 2.0}").unwrap();
    assert_eq!(canonical_json(&a), canonical_json(&b));
}

#[test]
fn normalization_does_not_collapse_large_integers() {
    let a: Value = serde_json::from_str("{\"n\": 9007199254740993}").unwrap();
    let b: Value = serde_json::from_str("{\"n\": 9007199254740992}").unwrap();
    assert_ne!(canonical_json(&a), canonical_json(&b));
}

#[test]
fn normalization_ignores_noise_below_scale_12() {
    let a: Value = serde_json::from_str("{\"x\": 0.1000000000000001}").unwrap();
    let b: Value = serde_json::from_str("{\"x\": 0.1}").unwrap();
    assert_eq!(canonical_json(&a), canonical_json(&b));
}

#[test]
fn canonical_form_sorts_object_keys_and_recurses() {
    let a: Value = serde_json::from_str("{\"b\": [{\"z\": 1, \"a\": 2}], \"a\": null}").unwrap();
    assert_eq!(canonical_json(&a), "{\"a\":null,\"b\":[{\"a\":2,\"z\":1}]}");
}

#[test]
fn unordered_comparison_is_multiset_equality() {
    let rows = |s: &str| -> Vec<Value> {
        s.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    };
    let expected = rows("{\"n\": 1}\n{\"n\": 1}\n{\"n\": 2}");
    let actual = rows("{\"n\": 2}\n{\"n\": 1}\n{\"n\": 1}");
    compare_rows(&expected, &actual, false).unwrap();
    let missing_dup = rows("{\"n\": 1}\n{\"n\": 2}");
    assert!(compare_rows(&expected, &missing_dup, false).is_err());
}

#[test]
fn ordered_comparison_is_positional() {
    let rows = |s: &str| -> Vec<Value> {
        s.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    };
    let expected = rows("{\"n\": 1}\n{\"n\": 2}");
    let swapped = rows("{\"n\": 2}\n{\"n\": 1}");
    assert!(compare_rows(&expected, &swapped, true).is_err());
    compare_rows(&expected, &expected.clone(), true).unwrap();
}

#[tokio::test]
async fn execution_reports_a_row_mismatch() {
    let text =
        format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n");
    let case = parse_case("mismatch", &text).unwrap();
    let err = execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap_err();
    assert!(err.contains("row mismatch"), "got: {err}");
    assert!(err.contains("step 1 (query)"), "got: {err}");
}

#[tokio::test]
async fn bless_rewrites_the_failing_expect_and_converges() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bless_case.gqt");
    let text =
        format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n");
    std::fs::write(&path, &text).unwrap();
    let case = parse_case("bless_case", &text).unwrap();
    let err = execute_case(&case, &path, true).await.unwrap_err();
    assert!(err.contains("expect rewritten"), "got: {err}");

    let blessed = std::fs::read_to_string(&path).unwrap();
    assert!(blessed.contains("{\"p.name\":\"alice\"}"), "got: {blessed}");
    let case = parse_case("bless_case", &blessed).unwrap();
    execute_case(&case, &path, false).await.unwrap();
}

#[test]
fn refuses_duplicate_issue_header() {
    let text = format!("{HDR}# issue: none\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("duplicate"));
}

#[test]
fn refuses_empty_red_on_value() {
    let text = format!("# issue: 7\n# red_on: \n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7_x", &text).contains("needs a value"));
    let text = format!("# issue: 7\n# red_on:\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_7_x", &text).contains("not `# <key>: <value>`"));
}

#[test]
fn refuses_noncanonical_issue_header_number() {
    let text = format!("# issue: 0563\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_563_x", &text).contains("no sign or leading zeros"));
    let text = format!("# issue: +563\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_563_x", &text).contains("no sign or leading zeros"));
}

#[test]
fn refuses_leading_zero_issue_digits() {
    let text = format!("# issue: 7\n# red_on: 2026-01-01, red.\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    assert!(refusal("issue_007_x", &text).contains("leading zeros"));
}

#[test]
fn refuses_arguments_on_bare_sections() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- restart now\n");
    assert!(refusal("x", &text).contains("takes no arguments"));
    let junk_query =
        "--- query fast\nquery all() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{junk_query}{EXPECT}");
    assert!(refusal("x", &text).contains("takes no arguments"));
}

#[test]
fn refuses_crlf_line_endings() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}").replace('\n', "\r\n");
    assert!(refusal("x", &text).contains("line endings"));
}

#[test]
fn refuses_loop_range_over_the_cap() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 10001\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("10000 cap"));
}

#[test]
fn refuses_signed_or_padded_numeric_tokens() {
    let text =
        format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}--- expect affected: nodes=+1 edges=0\n");
    assert!(refusal("x", &text).contains("nodes=<N> edges=<M>"));
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 00 2\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("plain decimal"));
}

#[test]
fn refuses_ok_expect_with_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}--- expect ok\nstray\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_affected_expect_with_body() {
    let text =
        format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}--- expect affected: nodes=1 edges=0\nstray\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_schema_inside_a_loop() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- foreach $x a\n{SCHEMA}{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("out of position"));
}

#[test]
fn refuses_loop_headers_with_a_body() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 2\nstray\n{QUERY}{EXPECT}--- endloop\n");
    assert!(refusal("x", &text).contains("carries no body"));
    let text = format!("{HDR}{SCHEMA}{SEED}--- loop $i 0 2\n{QUERY}{EXPECT}--- endloop\nstray\n");
    assert!(refusal("x", &text).contains("carries no body"));
}

#[test]
fn refuses_nearest_over_a_string_property() {
    let query = "--- query\nquery q() {\n    match { $p: Person }\n    return { $p.name }\n    order { nearest($p.name, $p.name) }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n");
    assert!(refusal("x", &text).contains("vector parameter"));
}

#[test]
fn traversal_header_forces_index_builds() {
    let text = format!("{HDR}# traversal: indexed\n{SCHEMA}{SEED}{QUERY}{EXPECT}");
    let case = parse_case("x", &text).unwrap();
    assert!(case.needs_indices);
    assert_eq!(case.traversal, Some("indexed"));
}

#[test]
fn pin_violation_names_the_path_that_ran() {
    assert_eq!(pin_violation("indexed", 3, 0, true), None);
    assert_eq!(pin_violation("csr", 0, 2, true), None);
    assert_eq!(pin_violation("indexed", 0, 0, false), None);
    let v = pin_violation("indexed", 1, 2, false).unwrap();
    assert!(
        v.contains("pinned `indexed`, ran `csr` on 2 expand(s)"),
        "{v}"
    );
    let v = pin_violation("csr", 4, 0, false).unwrap();
    assert!(
        v.contains("pinned `csr`, ran `indexed` on 4 expand(s)"),
        "{v}"
    );
    // A step that must expand and shows nothing on the pinned path: the
    // pin and the probes were dropped together.
    let v = pin_violation("indexed", 0, 0, true).unwrap();
    assert!(v.contains("no expand ran on it"), "{v}");
    let v = pin_violation("csr", 0, 0, true).unwrap();
    assert!(v.contains("no expand ran on it"), "{v}");
}

#[test]
fn expects_expand_ignores_bound_edges_and_plain_bindings() {
    let unbound = parse_query(TRAVERSAL_QUERY.trim_start_matches("--- query\n")).unwrap();
    assert!(expects_expand(&unbound.queries[0].match_clause));
    let bound = "query f($n: String) {\n    match {\n        $a: Person\n        $a.name = $n\n        \
                 $a $k:knows $b\n    }\n    return { $b.name }\n}\n";
    let bound = parse_query(bound).unwrap();
    assert!(!expects_expand(&bound.queries[0].match_clause));
    let plain = parse_query(QUERY.trim_start_matches("--- query\n")).unwrap();
    assert!(!expects_expand(&plain.queries[0].match_clause));
    let negated = "query f() {\n    match {\n        $a: Person\n        not { $a knows $x }\n    }\n    \
                   return { $a.name }\n}\n";
    let negated = parse_query(negated).unwrap();
    assert!(!expects_expand(&negated.queries[0].match_clause));
}

/// A two-node, one-edge graph with a one-hop traversal, for the pin tests.
const TRAVERSAL_SCHEMA: &str = "--- schema\nnode Person {\n    name: String @key\n}\n\n\
                                edge Knows: Person -> Person {\n    since: I64\n}\n";
const TRAVERSAL_SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n\
                              {\"type\":\"Person\",\"data\":{\"name\":\"bob\"}}\n\
                              {\"edge\":\"Knows\",\"from\":\"alice\",\"to\":\"bob\",\"data\":{\"id\":\"k-1\",\"since\":2020}}\n";
const TRAVERSAL_QUERY: &str = "--- query\nquery friends($n: String) {\n    match {\n        $a: Person\n        \
                               $a.name = $n\n        $a knows $b\n    }\n    return { $b.name }\n}\n";
const TRAVERSAL_PARAMS: &str = "--- params\n{\"n\": \"alice\"}\n";
const TRAVERSAL_EXPECT: &str = "--- expect unordered\n{\"b.name\": \"bob\"}\n";

/// The pin reaches the executor on both paths: a pinned step runs its
/// expands on the pinned path only, and the probes see them (a zero count
/// on both paths would make the check vacuous).
#[tokio::test]
async fn pinned_step_runs_only_its_pinned_path() {
    for (mode, expect_indexed) in [("indexed", true), ("csr", false)] {
        let text = format!(
            "{HDR}# traversal: {mode}\n{TRAVERSAL_SCHEMA}{TRAVERSAL_SEED}{TRAVERSAL_QUERY}{TRAVERSAL_PARAMS}{TRAVERSAL_EXPECT}"
        );
        let case = parse_case("pinned", &text).unwrap();
        execute_case(&case, Path::new("unused.gqt"), false)
            .await
            .unwrap_or_else(|e| panic!("{mode}: {e}"));

        let (db, _uri, _dir) = open_case_store(&case).await.unwrap();
        let Some(Item::Step(Step::Query(step))) = case.items.first() else {
            panic!("first item is the query step");
        };
        let params = build_params(step.params_raw.as_ref(), &step.ast_params, None).unwrap();
        let (outcome, counts) = under_traversal(
            Some(mode),
            db.query(
                ReadTarget::branch("main"),
                &step.source,
                &step.name,
                &params,
            ),
        )
        .await;
        outcome.unwrap();
        let counts = counts.unwrap();
        let (indexed, csr) = (
            counts.indexed.load(Ordering::Relaxed),
            counts.csr.load(Ordering::Relaxed),
        );
        if expect_indexed {
            assert!(
                indexed >= 1 && csr == 0,
                "{mode}: indexed={indexed} csr={csr}"
            );
        } else {
            assert!(
                csr >= 1 && indexed == 0,
                "{mode}: indexed={indexed} csr={csr}"
            );
        }
    }
}

#[test]
fn refuses_ordered_expect_on_an_rrf_led_order() {
    let query = "--- query\nquery q($v: Vector(4), $t: String) {\n    match { $p: Person }\n    \
                 return { $p.name }\n    order { rrf(nearest($p.vec, $v), bm25($p.name, $t)) }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect ordered\n");
    let message = refusal("x", &text);
    assert!(message.contains("led by `rrf()`"), "{message}");
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n");
    parse_case("x", &text).unwrap();
}

#[test]
fn refuses_ordered_expect_with_an_aggregate_in_return() {
    let query = "--- query\nquery q($t: String) {\n    match { $p: Person\n        search($p.name, $t) }\n    \
                 return { count($p) as total }\n    order { bm25($p.name, $t) }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect ordered\n");
    assert!(refusal("x", &text).contains("aggregate in its `return` list"));
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n");
    parse_case("x", &text).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_fails_a_case_over_its_budget() {
    let slow = run_bounded("slow", Duration::from_millis(50), async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(())
    })
    .await;
    assert_eq!(slow.stem, "slow");
    assert!(
        slow.elapsed < Duration::from_secs(5),
        "timeout did not cut the case short"
    );
    let err = slow.result.as_ref().unwrap_err();
    assert!(err.contains("budget of 0.05s"), "{err}");
    assert!(err.contains(CASE_TIMEOUT_ENV), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_records_a_panicking_case() {
    let out = run_bounded("p", Duration::from_secs(10), async {
        if std::hint::black_box(true) {
            panic!("boom");
        }
        Ok(())
    })
    .await;
    assert_eq!(out.stem, "p");
    let err = out.result.as_ref().unwrap_err();
    assert!(err.starts_with("case panicked: boom"), "{err}");
}

/// The checked-in corpus itself: at least one case, and no foreign entry (a
/// mis-renamed, nested, symlinked, or dot-prefixed case would otherwise
/// silently never run: the test target registers what its
/// `datatest_stable::harness!` pattern matches, and `list_cases` mirrors
/// that rule so this test refuses what the target would skip).
#[test]
fn corpus_layout() {
    let root = corpus_root();
    let (files, foreign) = list_cases(&root);
    assert!(
        foreign.is_empty(),
        "foreign entries under {}: {}",
        root.display(),
        foreign.join(", ")
    );
    assert!(
        !files.is_empty(),
        "no .gqt cases found under {}; a broken checkout must never read as green",
        root.display()
    );
}

#[test]
fn runner_refuses_a_process_traversal_override() {
    assert!(traversal_override_refusal(None).is_none());
    let reason = traversal_override_refusal(Some(OsStr::new("csr"))).unwrap();
    assert!(reason.contains("OMNIGRAPH_TRAVERSAL_MODE=csr"), "{reason}");
    assert!(reason.contains("# traversal:"), "{reason}");
}

/// Same name battery as `scripts/check-fix-regression.py --self-test`
/// (`corpus_case`): the runner and the gate must agree on what a case is.
/// The symlink and non-UTF-8 rows exist only here: the gate sees path
/// strings, and the test target's walk (`datatest-stable` over `walkdir`,
/// links not followed, names that are not UTF-8 dropped) would skip both.
#[test]
fn corpus_flags_foreign_entries() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.gqt"), "x").unwrap();
    std::fs::write(dir.path().join("b.txt"), "x").unwrap();
    std::fs::write(dir.path().join(".hidden.gqt"), "x").unwrap();
    std::fs::write(dir.path().join(".DS_Store"), "x").unwrap();
    std::fs::write(dir.path().join("c.GQT"), "x").unwrap();
    std::fs::create_dir(dir.path().join("nested")).unwrap();
    std::fs::write(dir.path().join("nested").join("d.gqt"), "x").unwrap();
    let mut expected = vec![
        ".hidden.gqt".to_string(),
        "b.txt".to_string(),
        "c.GQT".to_string(),
        "nested".to_string(),
    ];
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("a.gqt", dir.path().join("link.gqt")).unwrap();
        expected.push("link.gqt".to_string());
    }
    // APFS refuses a name that is not valid UTF-8 (EILSEQ), so this row runs
    // where the file system takes it.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let bad = std::ffi::OsStr::from_bytes(b"bad\xff.gqt");
        std::fs::write(dir.path().join(bad), "x").unwrap();
        expected.push(bad.to_string_lossy().into_owned());
    }
    expected.sort();
    let (files, foreign) = list_cases(dir.path());
    assert_eq!(files, vec![dir.path().join("a.gqt")]);
    assert_eq!(foreign, expected);
}

#[tokio::test]
async fn bless_refuses_cases_containing_loops() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bless_loop_case.gqt");
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- foreach $x a b\n{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n--- endloop\n"
    );
    std::fs::write(&path, &text).unwrap();
    let case = parse_case("bless_loop_case", &text).unwrap();
    let err = execute_case(&case, &path, true).await.unwrap_err();
    assert!(err.contains("bless: refused"), "got: {err}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
}

#[tokio::test]
async fn bless_never_rewrites_on_a_kind_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bless_kind_case.gqt");
    let query = "--- query\nquery q() {\n    match { $p: Person }\n    return { $p.nope }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n{{\"p.nope\": \"x\"}}\n");
    std::fs::write(&path, &text).unwrap();
    let case = parse_case("bless_kind_case", &text).unwrap();
    let err = execute_case(&case, &path, true).await.unwrap_err();
    assert!(err.contains("query failed"), "got: {err}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
}

#[tokio::test]
async fn params_refusal_satisfies_an_error_expect() {
    let query = "--- query\nquery q($q: String) {\n    match {\n        $p: Person\n        $p.name = $q\n    }\n    return { $p.name }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect error: q\n");
    let case = parse_case("x", &text).unwrap();
    execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap();
}

#[test]
fn bless_splice_preserves_the_trailing_blank_separator() {
    let original = "--- expect unordered\nold\n\n--- restart\n";
    let span = BodySpan {
        start_line: 1,
        len: 2,
    };
    let rows = vec!["{\"n\":1}".to_string()];
    let out = splice_lines(original, span, &rows);
    assert_eq!(out, "--- expect unordered\n{\"n\":1}\n\n--- restart\n");
}

#[test]
fn bless_splice_replaces_only_the_expect_body() {
    let original = "--- query\nq\n--- expect unordered\nold row\nold row 2\n--- restart\n";
    let span = BodySpan {
        start_line: 3,
        len: 2,
    };
    let rows = vec!["{\"n\":1}".to_string()];
    let out = splice_lines(original, span, &rows);
    assert_eq!(
        out,
        "--- query\nq\n--- expect unordered\n{\"n\":1}\n--- restart\n"
    );
}

#[test]
fn bless_splice_inserts_into_an_empty_expect_body() {
    let original = "--- expect unordered\n--- restart\n";
    let span = BodySpan {
        start_line: 1,
        len: 0,
    };
    let rows = vec!["{\"n\":1}".to_string()];
    let out = splice_lines(original, span, &rows);
    assert_eq!(out, "--- expect unordered\n{\"n\":1}\n--- restart\n");
}
