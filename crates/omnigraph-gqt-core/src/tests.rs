use super::*;

const HDR: &str = "# issue: none\n";
const SCHEMA: &str = "--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n--- schema\nnode Person {\n    name: String @key\n}\n";
const SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n";
const QUERY: &str =
    "--- query\nquery all() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
const EXPECT: &str =
    "--- expect unordered\n{\"p.name\": \"alice\"}\n--- expect shape\np.name: String\n";
const SHAPE: &str = "--- expect shape\np.name: String\n";
const MUTATE: &str = "--- mutate\nquery ins($n: String) {\n    insert Person { name: $n }\n}\n";
const PARAMS: &str = "--- params\n{\"n\": \"bob\"}\n";
const EXPECT_OK: &str = "--- expect ok\n";

const GENERATED_PERSON: &str = "tables:\n  - kind: node\n    name: Person\n    rows: 1\n    commits: 1\n    columns:\n      name: {kind: key, prefix: person-, width: 3}\n";

#[test]
fn generated_seed_and_load_headers_are_strict_and_load_is_a_write_step() {
    let text = format!(
        "{HDR}{SCHEMA}--- seed generate: v1 seed: 7\n{GENERATED_PERSON}--- load generate: v1 seed: 7 mode: merge branch: main\n{GENERATED_PERSON}{EXPECT_OK}"
    );
    let case = parse_case("generated", &text).unwrap();
    assert!(matches!(
        case.fixture.as_ref().unwrap().seed,
        Seed::Generated(_)
    ));
    assert_eq!(case.steps()[0].kind, StepKind::Load);
    assert!(
        case.steps()[0]
            .source
            .starts_with("--- load generate: v1 seed: 7 mode: merge branch: main")
    );
    let [Item::Step(Step::Load(load))] = case.items.as_slice() else {
        panic!("expected generated load");
    };
    assert_eq!(load.call_count(), 1);
    for (from, to) in [
        ("generate: v1", "generate: v2"),
        ("seed: 7", "seed: -1"),
        ("seed: 7", "seed: 7 seed: 8"),
        ("mode: merge", "mode: overwrite"),
        ("mode: merge", "mode: unknown"),
        ("mode: merge", ""),
        ("--- expect ok", "--- expect affected: nodes=1 edges=0"),
        ("--- expect ok", "--- params\n{}\n--- expect ok"),
    ] {
        assert!(
            parse_case("generated", &text.replace(from, to)).is_err(),
            "{from} -> {to}"
        );
    }
    assert!(parse_case("generated", &text.replace("--- expect ok\n", "")).is_err());
    for recipe in [
        GENERATED_PERSON.replace("name: Person", "name: Person\n    name: Person"),
        GENERATED_PERSON.replace("name: Person", "name: &name Person"),
        GENERATED_PERSON.replace(
            "      name: {kind: key, prefix: person-, width: 3}",
            "      name: {kind: literal, value: alice}\n      name: {kind: literal, value: bob}",
        ),
    ] {
        for source in [
            format!("{HDR}{SCHEMA}--- seed generate: v1 seed: 7\n{recipe}"),
            format!(
                "{HDR}{SCHEMA}--- seed\n--- load generate: v1 seed: 7 mode: append\n{recipe}{EXPECT_OK}"
            ),
        ] {
            assert!(
                parse_case("invalid_generated_yaml", &source).is_err(),
                "{source}"
            );
        }
    }
}

/// A `--- params generate` recipe supplies a step's parameters: its values
/// reach the engine as a literal body's would, a Blob value included. The
/// header takes `generate: v1` and a seed only, the recipe takes no `${`
/// substitution, and its JSON bound is checked when the case parses.
#[tokio::test]
async fn generated_params_supply_a_step_and_are_refused_outside_their_contract() {
    const GENERATED: &str =
        "--- params generate: v1 seed: 7\nparams:\n  n: {kind: key, prefix: gen-, width: 3}\n";
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{MUTATE}{GENERATED}--- expect affected: nodes=1 edges=0\n\
         --- query\nquery named() {{\n    match {{ $p: Person {{ name: \"gen-000\" }} }}\n    return {{ $p.name }}\n}}\n\
         --- expect unordered\n{{\"p.name\": \"gen-000\"}}\n{SHAPE}"
    );
    let case = parse_case("generated_params", &text).unwrap();
    execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap();

    let blob = format!(
        "{HDR}--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n\
         --- schema\nnode Doc {{\n    slug: String @key\n    body: Blob?\n}}\n--- seed\n\
         --- mutate\nquery put($slug: String, $body: Blob) {{\n    insert Doc {{ slug: $slug, body: $body }}\n}}\n\
         --- params generate: v1 seed: 0\nparams:\n  slug: {{kind: literal, value: doc}}\n  body: {{kind: blob, byte: 1, length: 3}}\n\
         --- expect affected: nodes=1 edges=0\n"
    );
    let case = parse_case("generated_blob_param", &blob).unwrap();
    let [Item::Step(Step::Mutate(step))] = case.items.as_slice() else {
        panic!("expected one mutate step");
    };
    let Some(ParamsInput::Generated(generated)) = step.params_raw.as_ref() else {
        panic!("expected generated params");
    };
    assert_eq!(
        generated.generate().unwrap(),
        serde_json::json!({"slug": "doc", "body": "base64:AQEB"})
    );
    execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap();

    for (from, to) in [
        ("generate: v1", "generate: v2"),
        (" seed: 7", ""),
        ("seed: 7", "seed: 7 mode: merge"),
        ("seed: 7", "seed: 7 branch: main"),
        (
            "{kind: key, prefix: gen-, width: 3}",
            "{kind: key, prefix: gen-, width: 3, extra: 1}",
        ),
        (
            "{kind: key, prefix: gen-, width: 3}",
            "{kind: literal, value: \"${n}\"}",
        ),
        ("params:\n  n:", "params: {}\n  n:"),
        (
            "{kind: key, prefix: gen-, width: 3}",
            "{kind: blob, byte: 0, length: 50331648}",
        ),
        ("--- expect affected", "--- params\n{}\n--- expect affected"),
    ] {
        let changed = text.replacen(from, to, 1);
        assert_ne!(changed, text, "{from} must occur in the case");
        assert!(
            parse_case("generated_params", &changed).is_err(),
            "{from} -> {to}"
        );
    }
    assert!(
        parse_case(
            "generated_params",
            &format!(
                "{HDR}{SCHEMA}--- seed\n--- load generate: v1 seed: 7 mode: append\n{GENERATED_PERSON}{GENERATED}{EXPECT_OK}"
            )
        )
        .is_err(),
        "a generated load takes no params"
    );
}

#[test]
fn empty_generated_seeds_are_admitted_but_empty_loads_are_refused() {
    for recipe in [
        "tables: []\n".to_string(),
        GENERATED_PERSON
            .replace("rows: 1", "rows: 0")
            .replace("commits: 1", "commits: 0"),
    ] {
        let seed = format!("{HDR}{SCHEMA}--- seed generate: v1 seed: 7\n{recipe}");
        assert!(parse_case("empty_seed", &seed).is_ok());
        let load = format!(
            "{HDR}{SCHEMA}--- seed\n--- load generate: v1 seed: 7 mode: append branch: absent\n{recipe}{EXPECT_OK}"
        );
        assert!(refusal("empty_load", &load).contains("at least one nonempty batch"));
    }
}

#[tokio::test]
async fn generated_load_calls_the_host_once_before_checking_its_expectation() {
    let text = format!(
        "{HDR}{SCHEMA}--- seed\n--- load generate: v1 seed: 7 mode: append\n{GENERATED_PERSON}--- expect error: deliberately false\n"
    );
    let case = parse_case("generated", &text).unwrap();
    let (session, uri, _dir) = open_case_store(&case, Engine::V2).await.unwrap();
    let host = RecordingHost::default();
    let result = execute_steps(
        &case,
        Path::new("unused.gqt"),
        false,
        session,
        &uri,
        None,
        &host,
    )
    .await;
    assert!(result.err().unwrap().contains("load succeeded"));
    assert_eq!(
        *host.events.lock().unwrap(),
        ["started 1", "finished 1", "assertion \"failed\""]
    );
}

#[tokio::test]
async fn generated_append_partitions_publish_the_declared_number_of_commits() {
    for (rows, commits, batch) in [(5, 4, ""), (4097, 2, "    batch_rows: 4096\n")] {
        let recipe = GENERATED_PERSON
            .replace("rows: 1", &format!("rows: {rows}"))
            .replace("commits: 1\n", &format!("commits: {commits}\n{batch}"));
        let text = format!("{HDR}{SCHEMA}--- seed generate: v1 seed: 7\n{recipe}");
        let case = parse_case("generated", &text).unwrap();
        let (session, _, _dir) = open_case_store(&case, Engine::V2).await.unwrap();
        assert_eq!(
            session.list_commits(Some("main")).await.unwrap().len(),
            commits + 1
        );
        let result = session
            .query(
                ReadTarget::branch("main"),
                QUERY.trim_start_matches("--- query\n"),
                "all",
                &omnigraph_compiler::ParamMap::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.num_rows(), rows);
    }
}

#[tokio::test]
async fn generated_load_repeats_the_same_recipe_in_a_loop() {
    let text = format!(
        "{HDR}{SCHEMA}--- seed\n--- loop $i 0 3\n--- load generate: v1 seed: 7 mode: merge\n{GENERATED_PERSON}{EXPECT_OK}--- endloop\n"
    );
    let case = parse_case("generated", &text).unwrap();
    let (session, uri, _dir) = open_case_store(&case, Engine::V2).await.unwrap();
    let host = RecordingHost::default();
    let session = execute_steps(
        &case,
        Path::new("unused.gqt"),
        false,
        session,
        &uri,
        None,
        &host,
    )
    .await
    .unwrap();
    assert_eq!(
        host.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.as_str() == "started 1")
            .count(),
        3
    );
    let result = session
        .query(
            ReadTarget::branch("main"),
            QUERY.trim_start_matches("--- query\n"),
            "all",
            &omnigraph_compiler::ParamMap::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert!(
        parse_case(
            "generated",
            &text.replace("prefix: person-", "prefix: '${i}'")
        )
        .is_err()
    );
}

#[tokio::test]
async fn generated_load_observes_typed_errors_for_single_and_multiple_calls() {
    for count in [1, 2] {
        let recipe = GENERATED_PERSON
            .replace("rows: 1", &format!("rows: {count}"))
            .replace("commits: 1", &format!("commits: {count}"));
        let text = format!(
            "{HDR}{SCHEMA}--- seed\n--- load generate: v1 seed: 7 mode: append branch: absent\n{recipe}--- expect error: absent\n"
        );
        let case = parse_case("generated", &text).unwrap();
        let (session, uri, _dir) = open_case_store(&case, Engine::V2).await.unwrap();
        let host = RecordingHost::default();
        execute_steps(
            &case,
            Path::new("unused.gqt"),
            false,
            session,
            &uri,
            None,
            &host,
        )
        .await
        .unwrap();
        assert_eq!(host.faults.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}

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
fn a_fault_section_is_refused() {
    let fault =
        "--- fault\nat: recovery.sidecar_write\noccurrence: 1\naction: fail\nscope: next_step\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{fault}{QUERY}{EXPECT}");
    assert!(refusal("x", &text).contains("there is no `--- fault` section"));
}

const DST_RUNNER: &str = "--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0]\n\n--- schema\nnode Person {\n    name: String @key\n}\n";
const BLOCK: &str = "--- concurrent\nw1: query add() { insert Person { name: \"bob\" } }\nr1: query all() { match { $p: Person } return { $p.name } }\norder: w1 put __manifest/, r1, w1\n";
const BLOCK_EXPECT: &str = "--- expect\nw1: ok\nr1: ok\n";

#[test]
fn a_concurrent_block_parses_with_its_bare_expect() {
    let text = format!("{HDR}{DST_RUNNER}{SEED}{BLOCK}{BLOCK_EXPECT}");
    let case = parse_case("block", &text).unwrap();
    assert_eq!(case.items.len(), 1);
    assert!(case.needs_dst());
    let Item::Step(Step::Concurrent(step)) = &case.items[0] else {
        panic!("expected a concurrent step");
    };
    assert_eq!(step.ordinal, 1);
    assert_eq!(step.sessions.len(), 2);
    assert_eq!(step.sessions[0].kind, SessionKind::Mutation);
    assert_eq!(step.sessions[1].kind, SessionKind::Query);
    assert_eq!(step.sessions[0].name, "add");
    assert_eq!(step.order.len(), 3);
    assert_eq!(case.source_lines.get(&1), Some(&15));
}

#[test]
fn a_concurrent_block_refuses_what_it_cannot_run() {
    let refused = |block: &str, expect: &str| {
        refusal("block", &format!("{HDR}{DST_RUNNER}{SEED}{block}{expect}"))
    };
    assert!(refused(BLOCK, "--- expect ok\n").contains("`--- expect` is bare"));
    assert!(
        refused(BLOCK, "--- expect\n# note\nw1: ok\nr1: ok\n")
            .contains("`#` lines are refused inside the expect section")
    );
    assert!(
        refused(BLOCK, "--- params\n{}\n--- expect\nw1: ok\nr1: ok\n").contains("takes no params")
    );
    assert!(
        refused(
            "--- concurrent\nw1: query add($n: String) { insert Person { name: $n } }\nr1: query all() { match { $p: Person } return { $p.name } }\norder: w1, r1\n",
            BLOCK_EXPECT
        )
        .contains("declares parameters")
    );
    assert!(
        refused(
            "--- concurrent\nw1: branch create feature\nr1: query all() { match { $p: Person } return { $p.name } }\norder: w1, r1\n",
            BLOCK_EXPECT
        )
        .contains("must be a query or mutation declaration")
    );
    let in_loop = refused(
        &format!("--- loop $x 1 2\n{BLOCK}"),
        &format!("{BLOCK_EXPECT}--- endloop\n"),
    );
    assert!(in_loop.contains("inside a loop"), "{in_loop}");
    assert!(
        refused(
            "--- concurrent extra\nw1: query a() {}\norder: w1\n",
            BLOCK_EXPECT
        )
        .contains("takes no arguments")
    );
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
fn admits_case_without_a_query_or_mutate_step() {
    let text = format!("{HDR}{SCHEMA}{SEED}");
    assert!(parse_case("x", &text).unwrap().items.is_empty());
}

#[test]
fn admits_restart_only_step_list() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- restart\n");
    assert_eq!(parse_case("x", &text).unwrap().items.len(), 1);
}

#[test]
fn schema_and_seed_are_optional_together() {
    let runner = SCHEMA.split_once("--- schema").unwrap().0;
    for steps in ["".to_string(), format!("{QUERY}{EXPECT}")] {
        assert!(parse_case("external", &format!("{HDR}{runner}{steps}")).is_ok());
    }
    for sections in [
        SEED.to_string(),
        format!("{QUERY}{EXPECT}--- schema\nnode Person {{ name: String @key }}\n{SEED}"),
        format!("{QUERY}{EXPECT}{SEED}"),
    ] {
        assert!(parse_case("external", &format!("{HDR}{runner}{sections}")).is_err());
    }
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

const CREATE: &str = "--- mutate\nbranch create b0\n--- expect ok\n";
const LIST: &str = "--- query\nbranch list\n--- expect unordered\n{\"name\": \"b0\"}\n{\"name\": \"main\"}\n--- expect shape\nname: String\n";

/// The four refusals a statement step carries: the wrong section word for a
/// control write and for `branch list`, the `branch:` argument, `--- params`.
/// One input per arm; `branch_statement_lifecycle.gqt` runs the accepted ones.
#[test]
fn statement_step_refusals() {
    for (step, want) in [
        (
            format!("--- query\nbranch create b0\n{EXPECT}"),
            "a control write under `--- query` is refused; use `--- mutate`",
        ),
        (
            format!("--- mutate\nbranch list\n{EXPECT_OK}"),
            "`branch list` under `--- mutate` is refused; use `--- query`",
        ),
        (
            format!("--- mutate branch: main\nbranch create b0\n{EXPECT_OK}"),
            "a branch statement names its branches itself; drop the `branch:` argument",
        ),
        (
            format!("--- mutate\nbranch create b0\n{PARAMS}{EXPECT_OK}"),
            "a branch statement takes no params",
        ),
    ] {
        let text = format!("{HDR}{SCHEMA}{SEED}{step}");
        assert_eq!(refusal("x", &text), want, "{step}");
    }
}

/// The section word ends at the first space or tab, so a tab-separated
/// argument parses as the space-separated one does.
#[test]
fn step_header_word_ends_at_a_space_or_a_tab() {
    for sep in [' ', '\t'] {
        let text = format!(
            "{HDR}{SCHEMA}{SEED}--- query{sep}branch: main\n{}{EXPECT}",
            QUERY.trim_start_matches("--- query\n")
        );
        let case = parse_case("x", &text).unwrap_or_else(|e| panic!("{sep:?}: {e}"));
        match &case.items[0] {
            Item::Step(Step::Query(step)) => assert_eq!(step.branch, "main", "{sep:?}"),
            other => panic!("{sep:?}: {other:?}"),
        }
    }
}

/// The header argument's grammar: bare, or exactly `branch: <name>` with
/// `rest` trimmed and the trimmed remainder as the name; any other `rest`
/// is refused with the grammar, and a `branch:` with nothing after it too.
#[test]
fn step_header_takes_only_a_branch_argument() {
    for (rest, want) in [
        ("", None),
        ("branch: b0", Some("b0")),
        ("branch:b0", Some("b0")),
        (" branch: b0", Some("b0")),
        ("branch:   review/x  ", Some("review/x")),
    ] {
        assert_eq!(
            parse_step_branch("query", rest).unwrap().as_deref(),
            want,
            "{rest:?}"
        );
    }
    assert_eq!(
        parse_step_branch("mutate", "branches: b0").unwrap_err(),
        "`--- mutate` takes no arguments but `branch: <name>`, got `branches: b0`"
    );
    assert_eq!(
        parse_step_branch("query", "branch:").unwrap_err(),
        "`--- query branch:` needs a branch name"
    );
}

#[test]
fn refuses_outcome_on_any_step_but_a_merge() {
    let outcome = "--- expect outcome: merged\n";
    for (step, want) in [
        (
            "--- query\nbranch list\n",
            "`branch list` takes `unordered`, `ordered`, or `error:`",
        ),
        (
            "--- mutate\nbranch create b0\n",
            "`expect outcome:` is accepted on a `branch merge` step only",
        ),
        (
            QUERY,
            "`expect outcome:` is accepted on a `branch merge` step only",
        ),
    ] {
        let text = format!("{HDR}{SCHEMA}{SEED}{step}{outcome}");
        assert_eq!(refusal("x", &text), want, "{step}");
    }
}

#[test]
fn outcome_takes_the_three_merge_words() {
    for (word, want) in [
        ("already_up_to_date", MergeOutcome::AlreadyUpToDate),
        ("fast_forward", MergeOutcome::FastForward),
        ("merged", MergeOutcome::Merged),
    ] {
        let text = format!(
            "{HDR}{SCHEMA}{SEED}--- mutate\nbranch merge b0 into main\n--- expect outcome: {word}\n"
        );
        let case = parse_case("x", &text).unwrap();
        let [Item::Step(Step::Control(step))] = case.items.as_slice() else {
            panic!("expected one control step, got {:?}", case.items);
        };
        let ControlWrite::Merge {
            source,
            into,
            expect: MergeExpect::Outcome(got),
        } = &step.write
        else {
            panic!(
                "expected a merge with an outcome expect, got {:?}",
                step.write
            );
        };
        assert_eq!(
            (source.as_str(), into.as_deref(), *got),
            ("b0", Some("main"), want)
        );
    }
    let text =
        format!("{HDR}{SCHEMA}{SEED}--- mutate\nbranch merge b0\n--- expect outcome: conflicted\n");
    assert_eq!(
        refusal("x", &text),
        "`expect outcome:` takes `already_up_to_date`, `fast_forward`, or `merged`, got `conflicted`"
    );
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- mutate\nbranch merge b0\n--- expect outcome: merged\nbody\n"
    );
    assert!(refusal("x", &text).contains("carries no body"));
    let text = format!("{HDR}{SCHEMA}{SEED}--- mutate\nbranch merge b0\n--- expect outcome:\n");
    assert_eq!(
        refusal("x", &text),
        "`expect outcome:` needs a word: `already_up_to_date`, `fast_forward`, or `merged`"
    );
}

#[test]
fn refuses_affected_and_rows_on_a_control_write() {
    for (stmt, name) in [
        ("branch create b0", "branch create"),
        ("branch merge b0", "branch merge"),
    ] {
        let text = format!(
            "{HDR}{SCHEMA}{SEED}--- mutate\n{stmt}\n--- expect affected: nodes=0 edges=0\n"
        );
        assert_eq!(
            refusal("x", &text),
            format!("`expect affected:` is refused on a control write; `{name}` carries no counts")
        );
        let text = format!("{HDR}{SCHEMA}{SEED}--- mutate\n{stmt}\n--- expect unordered\n");
        assert_eq!(
            refusal("x", &text),
            format!("a control write takes `ok` or `error:`; `{name}` returns no rows")
        );
    }
}

#[test]
fn branch_list_takes_rows_or_error_and_needs_a_shape() {
    for mode in ["--- expect ok\n", "--- expect affected: nodes=0 edges=0\n"] {
        let text = format!("{HDR}{SCHEMA}{SEED}--- query\nbranch list\n{mode}");
        assert_eq!(
            refusal("x", &text),
            "`branch list` takes `unordered`, `ordered`, or `error:`",
            "{mode}"
        );
    }
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- query\nbranch list\n--- expect ordered\n{{\"name\": \"main\"}}\n"
    );
    assert!(refusal("x", &text).contains("needs an `--- expect shape` section"));
    let text = format!("{HDR}{SCHEMA}{SEED}--- query\nbranch list\n--- expect error: boom\n");
    let case = parse_case("x", &text).unwrap();
    assert!(matches!(
        case.items.as_slice(),
        [Item::Step(Step::List(ListStep {
            expect: QueryExpect::Error { .. },
            ..
        }))]
    ));
    let text = format!("{HDR}{SCHEMA}{SEED}{CREATE}{LIST}");
    let case = parse_case("x", &text).unwrap();
    assert_eq!(case.items.len(), 2);
    let text = format!("{HDR}{SCHEMA}{SEED}{LIST}--- expect plan\npass projection_pushdown\n");
    let error = refusal("x", &text);
    assert!(
        error.contains("`--- expect plan` is supported only on query steps"),
        "{error}"
    );
}

/// A quoted statement name carries `${` past the compiler (`string_char`
/// admits everything but `"` and `\`), so the runner's own `${` fence is
/// what refuses it, naming the line.
#[test]
fn refuses_substitution_marker_in_a_statement_body() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- foreach $i a b\n--- mutate\nbranch create \"${{i}}\"\n{EXPECT_OK}--- endloop\n"
    );
    assert_eq!(
        refusal("x", &text),
        "line 16: `${` may appear only inside a params or expect body"
    );
}

/// The `branch:` argument routes a declaration step to its branch and a
/// `branch list` rows step is compared and shape-checked as a read step;
/// what this pins is the `outcome:` mismatch, which names both words.
#[tokio::test]
async fn statement_steps_run_against_the_embedded_handle() {
    let on_b0 = QUERY.replace("--- query", "--- query branch: b0");
    let ins_b0 = MUTATE.replace("--- mutate", "--- mutate branch: b0");
    let both = "--- expect unordered\n{\"p.name\": \"alice\"}\n{\"p.name\": \"bob\"}\n--- expect shape\np.name: String\n";
    let list_ordered = LIST.replace("unordered", "ordered");
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{CREATE}{ins_b0}{PARAMS}--- expect affected: nodes=1 edges=0\n{on_b0}{both}{QUERY}{EXPECT}{list_ordered}\
         --- mutate\nbranch merge b0\n--- expect outcome: already_up_to_date\n"
    );
    let case = parse_case("x", &text).unwrap();
    let err = execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap_err();
    assert_eq!(
        err,
        "step 6 (branch merge): merge outcome mismatch: expected `already_up_to_date`, got `fast_forward`"
    );
}

#[tokio::test]
async fn branch_list_shape_and_rows_are_blessed_like_a_read_step() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bless_list.gqt");
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{CREATE}--- query\nbranch list\n--- expect unordered\n--- expect shape\n"
    );
    std::fs::write(&path, &text).unwrap();
    let case = parse_case("bless_list", &text).unwrap();
    let err = execute_case(&case, &path, true).await.unwrap_err();
    assert!(
        err.contains("names 0 column(s), the executor returned 1"),
        "got: {err}"
    );
    let blessed = std::fs::read_to_string(&path).unwrap();
    assert!(
        blessed.ends_with("--- expect shape\nname: String\n"),
        "got: {blessed}"
    );
    let case = parse_case("bless_list", &blessed).unwrap();
    let err = execute_case(&case, &path, true).await.unwrap_err();
    assert!(err.contains("row mismatch"), "got: {err}");
    let blessed = std::fs::read_to_string(&path).unwrap();
    assert!(
        blessed.contains("{\"name\":\"b0\"}\n{\"name\":\"main\"}\n"),
        "got: {blessed}"
    );
    let case = parse_case("bless_list", &blessed).unwrap();
    execute_case(&case, &path, false).await.unwrap();
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

const SAME: &str = "--- expect same as v1\n";

#[test]
fn same_as_v1_follows_a_query_steps_shape_or_plan() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}{SAME}{QUERY}{EXPECT}");
    let case = parse_case("x", &text).unwrap();
    let flags: Vec<bool> = case
        .items
        .iter()
        .map(|item| match item {
            Item::Step(Step::Query(step)) => step.same_as_v1,
            other => panic!("expected query steps, got {other:?}"),
        })
        .collect();
    assert_eq!(flags, [true, false]);
    let after_plan = format!(
        "{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}--- expect plan\npass projection_pushdown\n{SAME}"
    );
    let case = parse_case("x", &after_plan).unwrap();
    let Some(Item::Step(Step::Query(step))) = case.items.first() else {
        panic!("first item is the query step");
    };
    assert!(step.same_as_v1 && step.plan.is_some());
}

#[test]
fn same_as_v1_is_refused_off_a_query_rows_expect() {
    for text in [
        format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}{SAME}"),
        format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}{EXPECT_OK}{SAME}"),
    ] {
        let refused = refusal("x", &text);
        assert!(
            refused.starts_with("invalid_case: ") && refused.contains("refused on a mutate step"),
            "{refused}"
        );
    }
    let rows = "--- expect unordered\n{\"p.name\": \"alice\"}\n";
    for (text, needle) in [
        (
            format!("{HDR}{SCHEMA}{SEED}{QUERY}{SAME}{SHAPE}"),
            "must directly follow",
        ),
        (
            format!("{HDR}{SCHEMA}{SEED}{QUERY}{rows}{SAME}{SHAPE}"),
            "needs an `--- expect shape`",
        ),
        (
            format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect error: boom\n{SAME}"),
            "must directly follow",
        ),
        (
            format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}{SAME}{SAME}"),
            "must directly follow",
        ),
        (
            format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}{SAME}{{\"p.name\": \"alice\"}}\n"),
            "carries no body",
        ),
        (
            format!("{HDR}{SCHEMA}{SEED}{CREATE}{LIST}{SAME}"),
            "only on a query declaration step",
        ),
    ] {
        let refused = refusal("x", &text);
        assert!(refused.contains(needle), "{needle}: {refused}");
    }
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
    let schema = "--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n--- schema\nnode Doc {\n    slug: String @key\n    text: String\n    vec: Vector(4) @embed(\"text\")\n}\n";
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
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{SHAPE}");
    parse_case("x", &text).unwrap();
}

#[test]
fn search_construct_sets_the_index_decision() {
    let schema = "--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n--- schema\nnode Doc {\n    slug: String @key\n    text: String @index\n}\n";
    let query = "--- query\nquery q($q: String) {\n    match {\n        $d: Doc\n        search($d.text, $q)\n    }\n    return { $d.slug }\n}\n";
    let text = format!(
        "{HDR}{schema}--- seed\n{query}--- params\n{{\"q\": \"needle\"}}\n--- expect unordered\n--- expect shape\nd.slug: String\n"
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

mod schema_drift {
    use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field};

    use super::*;

    const AGG_QUERY: &str =
        "query q() {\n    match { $p: Person }\n    return { count($p) as n, min($p.age) as m }\n}";
    const NAME_QUERY: &str = "query q() {\n    match { $p: Person }\n    return { $p.name }\n}";

    fn decl(source: &str) -> QueryDecl {
        parse_query(source).unwrap().single_decl().clone()
    }

    fn agg_inferred() -> Schema {
        Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("m", DataType::Int32, true),
        ])
    }

    fn result(fields: Vec<Field>, columns: Vec<ArrayRef>) -> QueryResult {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        QueryResult::new(schema, vec![batch])
    }

    #[test]
    fn names_the_column_whose_type_differs_from_the_inferred_field() {
        let executed = result(
            vec![
                Field::new("n", DataType::Int64, true),
                Field::new("m", DataType::Float64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Float64Array::from(vec![None::<f64>])),
            ],
        );
        let msg = schema_drift(&decl(AGG_QUERY), &agg_inferred(), &executed).unwrap();
        assert!(msg.contains("column 1 `m`"), "got: {msg}");
        assert!(msg.contains("inferred Int32"), "got: {msg}");
        assert!(msg.contains("returned Float64"), "got: {msg}");
    }

    #[test]
    fn accepts_a_matching_schema_whatever_the_executor_declares_nullable() {
        let executed = result(
            vec![
                Field::new("n", DataType::Int64, false),
                Field::new("m", DataType::Int32, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![Some(7)])),
            ],
        );
        assert_eq!(
            schema_drift(&decl(AGG_QUERY), &agg_inferred(), &executed),
            None
        );
    }

    #[test]
    fn reports_a_column_count_mismatch() {
        let executed = result(
            vec![Field::new("n", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![0]))],
        );
        let msg = schema_drift(&decl(AGG_QUERY), &agg_inferred(), &executed).unwrap();
        assert!(msg.contains("inferred 2 column(s)"), "got: {msg}");
        assert!(msg.contains("returned 1"), "got: {msg}");
    }

    #[test]
    fn reports_an_executed_name_off_the_compiler_rule() {
        let inferred = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let executed = result(
            vec![Field::new("name", DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(vec!["alice"]))],
        );
        let msg = schema_drift(&decl(NAME_QUERY), &inferred, &executed).unwrap();
        assert!(msg.contains("expected name `p.name`"), "got: {msg}");
        assert!(msg.contains("returned `name`"), "got: {msg}");
    }

    #[test]
    fn reports_nulls_in_a_column_inferred_non_nullable() {
        let inferred = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let executed = result(
            vec![Field::new("p.name", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec![Some("alice"), None]))],
        );
        let msg = schema_drift(&decl(NAME_QUERY), &inferred, &executed).unwrap();
        assert!(msg.contains("column 0 `p.name`"), "got: {msg}");
        assert!(msg.contains("returned 1 null(s)"), "got: {msg}");
    }
}

mod result_contract {
    use arrow_array::{Float64Array, Int32Array, Int64Array, StructArray};
    use omnigraph_compiler::ir::{IRExpr, IRProjection};
    use omnigraph_compiler::{ExprType, PropType, ScalarType};
    use omnigraph_planner::{Estimate, NodeObjectType, PhysicalNode, PhysicalPlan, Properties};

    use super::*;

    fn node_fields() -> arrow_schema::Fields {
        vec![
            Field::new("@id", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]
        .into()
    }

    fn plan(nullable: bool) -> BoundPlan {
        let mut plan = PhysicalPlan::new();
        let input = plan.add(PhysicalNode::OuterReference {
            outer_var: "p".into(),
        });
        let root = plan.add(PhysicalNode::Projection {
            input,
            return_exprs: vec![
                IRProjection {
                    expr: IRExpr::Literal(
                        Literal::Float(1.0),
                        ExprType::from_prop(&PropType::scalar(ScalarType::F64, nullable)),
                    ),
                    alias: Some("total".into()),
                    column: "total".into(),
                    ty: ExprType::from_prop(&PropType::scalar(ScalarType::F64, nullable)),
                },
                IRProjection {
                    expr: IRExpr::Variable(
                        "p".into(),
                        omnigraph_compiler::types::ExprType::Node {
                            type_name: "Person".into(),
                        },
                    ),
                    alias: Some("person".into()),
                    column: "person".into(),
                    ty: ExprType::Node {
                        type_name: "Person".into(),
                    },
                },
            ],
            node_objects: vec![NodeObjectType {
                type_name: "Person".into(),
                fields: node_fields(),
            }],
        });
        plan.set_properties(
            root,
            Properties {
                schema: Arc::new(Schema::new(vec![
                    Field::new("total", DataType::Float64, nullable),
                    Field::new("person", DataType::Struct(node_fields()), false),
                ])),
                ordering: None,
                rows: Estimate::Unknown,
                work_bytes: Estimate::Unknown,
                retained_limit: None,
                sources: vec![],
            },
        );
        plan.set_root(root);
        BoundPlan {
            plan,
            values: Default::default(),
        }
    }

    fn decl() -> QueryDecl {
        parse_query(
            "query q() { match { $p: Person } return { sum($p.age) as total, $p as person } }",
        )
        .unwrap()
        .single_decl()
        .clone()
    }

    fn result(total: ArrayRef, age: ArrayRef) -> QueryResult {
        let fields = vec![
            Field::new("@id", DataType::Utf8, false),
            Field::new("age", age.data_type().clone(), true),
        ]
        .into();
        let person: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(StringArray::from(vec!["alice"])), age],
            None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("total", total.data_type().clone(), true),
            Field::new("person", person.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![total, person]).unwrap();
        QueryResult::new(schema, vec![batch])
    }

    fn matching() -> QueryResult {
        result(
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Int32Array::from(vec![30])),
        )
    }

    #[test]
    fn planned_results_require_order_names_and_full_node_types() {
        let bound = plan(true);
        assert_eq!(check_planned_results(&bound, &matching()), Ok(()));
        let wrong = result(
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![30])),
        );
        assert!(
            check_planned_results(&bound, &wrong)
                .unwrap_err()
                .contains("`total`")
        );
        let wrong = result(
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Int64Array::from(vec![30])),
        );
        assert!(
            check_planned_results(&bound, &wrong)
                .unwrap_err()
                .contains("`person`")
        );
        let reversed = matching().batches()[0].project(&[1, 0]).unwrap();
        let reversed = QueryResult::new(reversed.schema(), vec![reversed]);
        assert!(check_planned_results(&bound, &reversed).is_err());
        let mut wrong_name = plan(true);
        let root = wrong_name.plan.root();
        let mut properties = wrong_name.plan.properties(root).unwrap().clone();
        properties.schema = Arc::new(Schema::new(vec![
            Field::new("other", DataType::Float64, true),
            Field::new("person", DataType::Struct(node_fields()), false),
        ]));
        wrong_name.plan.set_properties(root, properties);
        assert!(check_planned_results(&wrong_name, &matching()).is_err());
    }

    #[test]
    fn planned_nullability_checks_cells_and_inference_checks_flags() {
        assert_eq!(check_planned_results(&plan(false), &matching()), Ok(()));
        let null = result(
            Arc::new(Float64Array::from(vec![None::<f64>])),
            Arc::new(Int32Array::from(vec![30])),
        );
        assert!(
            check_planned_results(&plan(false), &null)
                .unwrap_err()
                .contains("returned 1 null(s)")
        );
        assert_eq!(check_planned_results(&plan(true), &null), Ok(()));
        let bound = plan(true);
        assert_eq!(
            check_inferred_results(&decl(), declared_result_schema(&bound).unwrap(), &bound),
            Ok(())
        );
        assert!(
            check_inferred_results(
                &decl(),
                declared_result_schema(&plan(false)).unwrap(),
                &bound
            )
            .is_err()
        );
    }

    #[test]
    fn round_trip_checks_stored_shapes_even_when_explain_is_equal() {
        let original = plan(true);
        let mut restored = original.clone();
        let root = restored.plan.root();
        let mut properties = restored.plan.properties(root).unwrap().clone();
        properties.schema = Arc::new(Schema::new(vec![
            Field::new("total", DataType::Float64, false),
            Field::new("person", DataType::Struct(node_fields()), false),
        ]));
        restored.plan.set_properties(root, properties);
        assert_eq!(original.plan.to_json(), restored.plan.to_json());
        assert!(
            check_restored_plan(&original, &restored)
                .unwrap_err()
                .contains("schema")
        );
        let mut restored = original.clone();
        let Some(PhysicalNode::Projection { node_objects, .. }) = restored.plan.node_mut(root)
        else {
            panic!("projection");
        };
        node_objects[0].fields = vec![
            Field::new("@id", DataType::Int64, false),
            Field::new("age", DataType::Int32, true),
        ]
        .into();
        assert_eq!(original.plan.to_json(), restored.plan.to_json());
        assert!(
            check_restored_plan(&original, &restored)
                .unwrap_err()
                .contains("typed declarations")
        );
    }

    #[test]
    fn planner_typed_renderer_and_gqt_validator_agree_on_every_expression_kind() {
        use omnigraph_compiler::query::ast::{AggFunc, CompOp};
        use omnigraph_compiler::types::AggSignature;
        let ty = |scalar, nullable| ExprType::from_prop(&PropType::scalar(scalar, nullable));
        let text = || {
            IRExpr::Literal(
                Literal::String("needle".into()),
                ty(ScalarType::String, false),
            )
        };
        let property = || IRExpr::PropAccess {
            variable: "p".into(),
            property: "name".into(),
            ty: ty(ScalarType::String, true),
        };
        let node = || {
            IRExpr::Variable(
                "p".into(),
                ExprType::Node {
                    type_name: "Person".into(),
                },
            )
        };
        let rank = || IRExpr::Bm25 {
            field: Box::new(property()),
            query: Box::new(text()),
            ty: ty(ScalarType::F32, false),
        };
        let expressions = vec![
            property(),
            text(),
            node(),
            IRExpr::Param("term".into(), ty(ScalarType::String, false)),
            IRExpr::AliasRef("term".into(), ty(ScalarType::String, false)),
            IRExpr::Nearest {
                variable: "p".into(),
                property: "embedding".into(),
                query: Box::new(text()),
                ty: ty(ScalarType::F32, false),
            },
            IRExpr::Search {
                field: Box::new(property()),
                query: Box::new(text()),
                ty: ty(ScalarType::Bool, false),
            },
            IRExpr::Fuzzy {
                field: Box::new(property()),
                query: Box::new(text()),
                max_edits: Some(Box::new(IRExpr::Literal(
                    Literal::Integer(1),
                    ty(ScalarType::I64, false),
                ))),
                ty: ty(ScalarType::Bool, false),
            },
            IRExpr::MatchText {
                field: Box::new(property()),
                query: Box::new(text()),
                ty: ty(ScalarType::Bool, false),
            },
            rank(),
            IRExpr::Rrf {
                primary: Box::new(rank()),
                secondary: Box::new(rank()),
                k: None,
                ty: ty(ScalarType::F64, false),
            },
            IRExpr::Aggregate {
                func: AggFunc::Count,
                arg: Box::new(node()),
                signature: AggSignature {
                    arg: ExprType::Node {
                        type_name: "Person".into(),
                    },
                    result: ty(ScalarType::I64, true),
                },
            },
            IRExpr::comparison(property(), CompOp::Eq, text()),
            IRExpr::Not(
                Box::new(IRExpr::Param("flag".into(), ty(ScalarType::Bool, true))),
                ty(ScalarType::Bool, true),
            ),
            IRExpr::IsNull {
                expr: Box::new(property()),
                negated: false,
                ty: ty(ScalarType::Bool, false),
            },
            IRExpr::Cast {
                expr: Box::new(IRExpr::Literal(
                    Literal::Integer(1),
                    ty(ScalarType::I64, false),
                )),
                ty: ty(ScalarType::F64, false),
            },
        ];
        for expression in expressions {
            let mut bound = plan(false);
            let root = bound.plan.root();
            let Some(PhysicalNode::Projection { return_exprs, .. }) = bound.plan.node_mut(root)
            else {
                panic!("projection");
            };
            return_exprs[0].ty = expression.ty().clone();
            return_exprs[0].expr = expression;
            let rendered = bound.plan.to_json();
            assert_eq!(plan::validate_typed_plan(&rendered), Ok(()), "{rendered}");
            assert_eq!(check_bound_plan_round_trip(&bound), Ok(()), "{rendered}");
        }
    }

    #[test]
    fn block_typed_trees_and_specs_survive_every_row_round_trip_guard() {
        use omnigraph_compiler::AggSignature;
        use omnigraph_compiler::ir::{BlockAggregateExpr, SubqueryPredicate};
        use omnigraph_compiler::query::ast::{AggFunc, CompOp};
        let ty = |scalar, nullable| ExprType::from_prop(&PropType::scalar(scalar, nullable));
        let leaf = BlockAggregateExpr::Aggregate {
            func: AggFunc::Sum,
            arg: Box::new(IRExpr::PropAccess {
                variable: "p".into(),
                property: "age".into(),
                ty: ty(ScalarType::I64, true),
            }),
            signature: AggSignature {
                arg: ty(ScalarType::I64, true),
                result: ty(ScalarType::F64, true),
            },
        };
        let cases = vec![
            (
                leaf,
                IRExpr::Literal(Literal::Float(0.0), ty(ScalarType::F64, false)),
            ),
            (
                BlockAggregateExpr::CountRows {
                    ty: ty(ScalarType::I64, false),
                },
                IRExpr::Literal(Literal::Integer(0), ty(ScalarType::I64, false)),
            ),
            (
                BlockAggregateExpr::Cast {
                    expr: Box::new(BlockAggregateExpr::CountRows {
                        ty: ty(ScalarType::I64, false),
                    }),
                    ty: ty(ScalarType::F64, false),
                },
                IRExpr::Param("bound".into(), ty(ScalarType::F64, false)),
            ),
        ];
        for (left, right) in cases {
            let mut bound = plan(false);
            let input = bound.plan.root();
            let aggregate = omnigraph_planner::plan_block_aggregate(&left).unwrap();
            let id = bound.plan.add(PhysicalNode::AntiJoin {
                input,
                inner: input,
                outer_var: "p".into(),
                aggregate,
                predicate: SubqueryPredicate {
                    left,
                    op: CompOp::Gt,
                    right,
                },
            });
            bound.plan.set_root(id);
            assert_eq!(check_bound_plan_round_trip(&bound), Ok(()));
            let mut wrong = bound.clone();
            let Some(PhysicalNode::AntiJoin { aggregate, .. }) = wrong.plan.node_mut(id) else {
                panic!("block");
            };
            *aggregate = Some(omnigraph_planner::AggregateSpec {
                accumulator: omnigraph_planner::Accumulator::Float64,
                overflow: omnigraph_planner::Overflow::Error,
            });
            assert!(check_restored_plan(&bound, &wrong).is_err());
        }
    }

    #[test]
    fn round_trip_detects_cast_child_type_loss_with_unchanged_gq() {
        let mut original = plan(false);
        let root = original.plan.root();
        let Some(PhysicalNode::Projection { return_exprs, .. }) = original.plan.node_mut(root)
        else {
            panic!("projection");
        };
        return_exprs[0].expr = IRExpr::Cast {
            expr: Box::new(IRExpr::Literal(
                Literal::Integer(1),
                ExprType::from_prop(&PropType::scalar(ScalarType::I64, false)),
            )),
            ty: return_exprs[0].ty.clone(),
        };
        assert_eq!(check_bound_plan_round_trip(&original), Ok(()));
        let mut restored = original.clone();
        let Some(PhysicalNode::Projection { return_exprs, .. }) = restored.plan.node_mut(root)
        else {
            panic!("projection");
        };
        let IRExpr::Cast { expr, .. } = &mut return_exprs[0].expr else {
            panic!("cast");
        };
        let IRExpr::Literal(_, ty) = expr.as_mut() else {
            panic!("literal");
        };
        *ty = ExprType::from_prop(&PropType::scalar(ScalarType::I32, false));
        assert_eq!(
            original.plan.to_json()["exprs"],
            restored.plan.to_json()["exprs"]
        );
        assert!(
            check_restored_plan(&original, &restored)
                .unwrap_err()
                .contains("explain")
        );
    }

    #[test]
    fn bound_plan_round_trip_preserves_return_types_and_node_declarations() {
        let bound = plan(true);
        omnigraph_planner::validate_output_schemas(&bound.plan).unwrap();
        assert_eq!(check_bound_plan_round_trip(&bound), Ok(()));
        let restored: BoundPlan =
            serde_json::from_slice(&serde_json::to_vec(&bound).unwrap()).unwrap();
        assert_eq!(restored, bound);
        omnigraph_planner::validate_output_schemas(&restored.plan).unwrap();
        assert_eq!(
            bound.plan.to_json()["columns"],
            serde_json::json!(["total: F64?", "person: Person"])
        );
    }
}

mod shape_section {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Fields};

    use omnigraph_compiler::catalog::{Catalog, build_catalog};

    use super::*;
    use crate::shape::{ShapeLine, ShapeType, bless_shape_lines, parse_shape_body, shape_mismatch};

    fn age_catalog() -> Catalog {
        build_catalog(
            &parse_schema("node Person {\n    name: String @key\n    age: I32?\n}\n").unwrap(),
        )
        .unwrap()
    }

    fn mismatch(shape: &[ShapeLine], result: &QueryResult) -> Option<String> {
        shape_mismatch(shape, result, &Schema::empty(), &age_catalog())
    }

    fn person_struct() -> (Field, ArrayRef) {
        let fields = Fields::from(vec![
            Field::new("@id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["alice"])),
            Arc::new(StringArray::from(vec!["alice"])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ];
        let array = StructArray::new(fields.clone(), columns, None);
        (
            Field::new("p", DataType::Struct(fields), false),
            Arc::new(array),
        )
    }

    #[test]
    fn a_node_type_name_spells_a_bare_node_projection() {
        let parsed = lines("p: Person");
        assert!(matches!(&parsed[0].shape_type, ShapeType::Node(name) if name == "Person"));
        assert!(shape_refusal("p: Person?").contains("never null"));
        let (field, column) = person_struct();
        let executed = result(vec![field], vec![column]);
        assert_eq!(mismatch(&lines("p: Person"), &executed), None);
        assert!(
            mismatch(&lines("p: Company"), &executed)
                .unwrap()
                .contains("not a node type")
        );
        assert!(
            mismatch(&lines("p: String"), &executed)
                .unwrap()
                .contains("expected String")
        );
        assert_eq!(
            bless_shape_lines(&executed, &age_catalog()).unwrap(),
            vec!["p: Person".to_string()]
        );
    }

    const AGE_SCHEMA: &str = "--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n--- schema\nnode Person {\n    name: String @key\n    age: I32?\n}\n";
    const AGE_SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n{\"type\":\"Person\",\"data\":{\"name\":\"bob\"}}\n";
    const AGE_QUERY: &str =
        "--- query\nquery all() {\n    match { $p: Person }\n    return { $p.name, $p.age }\n}\n";
    const AGE_ROWS: &str =
        "--- expect unordered\n{\"p.name\": \"alice\", \"p.age\": 30}\n{\"p.name\": \"bob\"}\n";

    fn lines(body: &str) -> Vec<ShapeLine> {
        let owned: Vec<(usize, &str)> = body.lines().enumerate().collect();
        parse_shape_body(&owned).unwrap()
    }

    fn shape_refusal(body: &str) -> String {
        let owned: Vec<(usize, &str)> = body.lines().enumerate().collect();
        parse_shape_body(&owned).expect_err("expected the shape body to be refused")
    }

    fn result(fields: Vec<Field>, columns: Vec<ArrayRef>) -> QueryResult {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        QueryResult::new(schema, vec![batch])
    }

    #[test]
    fn a_rows_expect_without_a_shape_section_is_refused() {
        let text =
            format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"alice\"}}\n");
        let reason = refusal("x", &text);
        assert!(
            reason.contains("line 19: the rows expect needs an `--- expect shape` section"),
            "{reason}"
        );
        assert!(reason.contains("OMNIGRAPH_GQ_BLESS=1"), "{reason}");
        let text = format!(
            "{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"alice\"}}\n{QUERY}{EXPECT}"
        );
        assert!(
            refusal("x", &text).contains("the rows expect needs an `--- expect shape` section")
        );
    }

    #[test]
    fn a_shape_section_must_directly_follow_a_rows_expect() {
        let placement = "must directly follow a query step's unordered or ordered expect";
        let text = format!("{HDR}{SCHEMA}{SEED}{SHAPE}{QUERY}{EXPECT}");
        assert!(refusal("x", &text).contains(placement));
        let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{EXPECT_OK}{SHAPE}");
        assert!(refusal("x", &text).contains(placement));
        let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{SHAPE}");
        assert!(refusal("x", &text).contains(placement));
        let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}{SHAPE}");
        assert!(refusal("x", &text).contains(placement));
        let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}--- expect error: T1\n{SHAPE}");
        assert!(refusal("x", &text).contains(placement));
    }

    #[test]
    fn shape_lines_accept_plain_and_dotted_names_and_every_pg_type_ref() {
        let parsed = lines(
            "p.name: String\ntotal: I64?\np.tags: [I32]?\np.emb: Vector(2)\n__nanograph_now: DateTime\n\n",
        );
        let spelled: Vec<String> = parsed
            .iter()
            .map(|l| match &l.shape_type {
                ShapeType::Scalar(t) => format!("{}: {}", l.name, t.display_name()),
                ShapeType::Node(n) => format!("{}: {n}", l.name),
            })
            .collect();
        assert_eq!(
            spelled,
            [
                "p.name: String",
                "total: I64?",
                "p.tags: [I32]?",
                "p.emb: Vector(2)",
                "__nanograph_now: DateTime",
            ]
        );
        assert!(lines("").is_empty());
    }

    #[test]
    fn shape_body_refusals_name_the_line() {
        assert!(shape_refusal("# nope").contains("comments are refused"));
        assert!(shape_refusal("// nope").contains("comments are refused"));
        assert!(
            shape_refusal("p.name String").contains("line 1: not a `<name>: <type>` shape line")
        );
        assert!(
            shape_refusal("p.name: String\n1x: I64").contains("line 2: `1x` is not a column name")
        );
        assert!(shape_refusal("a.b.c: I64").contains("is not a column name"));
        assert!(
            shape_refusal("p.name: String @key")
                .contains("annotations and body constraints are not allowed")
        );
        assert!(shape_refusal("p: 123").contains("line 1: unknown type `123`"));
        assert!(shape_refusal("Name: String").contains("`Name` is not a column name"));
        assert!(shape_refusal("p.name: String // note").contains("comments are refused"));
        assert!(shape_refusal("p.name: String /* note */").contains("comments are refused"));
        assert!(shape_refusal("p.name: string").contains("unknown type `string`"));
        assert!(shape_refusal("kind: enum(a, b)").contains("`enum(...)` is refused"));
        assert!(shape_refusal("doc: Blob").contains("`Blob` is refused"));
    }

    #[test]
    fn substitution_markers_are_refused_inside_a_shape_body() {
        let text = format!(
            "{HDR}{SCHEMA}{SEED}--- foreach $x a b\n{QUERY}--- expect unordered\n--- expect shape\np.${{x}}: String\n--- endloop\n"
        );
        assert!(refusal("x", &text).contains("`${` is refused in a shape body"));
    }

    #[test]
    fn mismatch_names_the_column_and_spells_both_types_in_pg() {
        let shape = lines("n: I64?\nm: I32?");
        let executed = result(
            vec![
                Field::new("n", DataType::Int64, true),
                Field::new("m", DataType::Float64, true),
            ],
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![0])),
                Arc::new(arrow_array::Float64Array::from(vec![None::<f64>])),
            ],
        );
        let msg = mismatch(&shape, &executed).unwrap();
        assert_eq!(
            msg,
            "result shape mismatch at column 1 `m`: expected I32, the executor returned F64"
        );
        let msg = mismatch(&lines("n: I64?"), &executed).unwrap();
        assert!(
            msg.contains("names 1 column(s), the executor returned 2"),
            "{msg}"
        );
        let msg = mismatch(&lines("n: I64?\nmm: I32?"), &executed).unwrap();
        assert!(
            msg.contains("expected name `mm`, the executor returned `m`"),
            "{msg}"
        );
    }

    #[test]
    fn a_null_cell_fails_a_column_written_without_the_marker() {
        let executed = result(
            vec![Field::new("p.age", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![Some(30), None]))],
        );
        let msg = mismatch(&lines("p.age: I32"), &executed).unwrap();
        assert!(
            msg.contains("written without `?`, the executor returned 1 null(s)"),
            "{msg}"
        );
        assert_eq!(mismatch(&lines("p.age: I32?"), &executed), None);
        let no_nulls = result(
            vec![Field::new("p.age", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![Some(30)]))],
        );
        assert_eq!(mismatch(&lines("p.age: I32"), &no_nulls), None);
        assert_eq!(mismatch(&lines("p.age: I32?"), &no_nulls), None);
    }

    #[test]
    fn bless_spells_the_executed_type_and_marks_only_columns_holding_a_null() {
        let executed = result(
            vec![
                Field::new("p.name", DataType::Utf8, true),
                Field::new("p.age", DataType::Int32, false),
            ],
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), None])),
                Arc::new(Int32Array::from(vec![30, 31])),
            ],
        );
        assert_eq!(
            bless_shape_lines(&executed, &age_catalog()).unwrap(),
            ["p.name: String?", "p.age: I32"]
        );
    }

    #[test]
    fn a_shape_header_with_arguments_is_an_unknown_mode() {
        let text = format!(
            "{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"alice\"}}\n--- expect shape foo\np.name: String\n"
        );
        assert!(refusal("x", &text).contains("unknown expect mode `shape foo`"));
    }

    #[test]
    fn an_empty_shape_count_mismatch_names_the_bless_route() {
        let executed = result(
            vec![Field::new("n", DataType::Int64, true)],
            vec![Arc::new(arrow_array::Int64Array::from(vec![0]))],
        );
        let msg = mismatch(&lines(""), &executed).unwrap();
        assert!(
            msg.contains(
                "names 0 column(s), the executor returned 1; fill it with OMNIGRAPH_GQ_BLESS=1"
            ),
            "{msg}"
        );
    }

    #[test]
    fn a_type_mismatch_the_compiler_sides_with_names_the_executor_as_wrong() {
        let executed = result(
            vec![Field::new("m", DataType::Float64, true)],
            vec![Arc::new(arrow_array::Float64Array::from(vec![None::<f64>]))],
        );
        let inferred = Schema::new(vec![Field::new("m", DataType::Int32, true)]);
        let msg = shape_mismatch(&lines("m: I32?"), &executed, &inferred, &age_catalog()).unwrap();
        assert!(msg.ends_with("expected I32, the executor returned F64; the compiler infers I32 too, so the executor is wrong, not the shape line"), "{msg}");
        let disagreeing = Schema::new(vec![Field::new("m", DataType::Float64, true)]);
        let msg =
            shape_mismatch(&lines("m: I32?"), &executed, &disagreeing, &age_catalog()).unwrap();
        assert!(
            msg.ends_with("expected I32, the executor returned F64"),
            "{msg}"
        );
    }

    #[test]
    fn bless_refuses_a_column_name_the_shape_grammar_cannot_spell() {
        let executed = result(
            vec![Field::new("?", DataType::Int64, true)],
            vec![Arc::new(arrow_array::Int64Array::from(vec![0]))],
        );
        let err = bless_shape_lines(&executed, &age_catalog()).unwrap_err();
        assert!(
            err.contains("column `?` is not a column name the shape section can spell"),
            "{err}"
        );
    }

    #[test]
    fn bless_refuses_an_arrow_type_with_no_pg_spelling() {
        let inner = Fields::from(vec![Field::new("id", DataType::Utf8, false)]);
        let column: ArrayRef = Arc::new(StructArray::new(
            inner.clone(),
            vec![Arc::new(StringArray::from(vec!["alice"])) as ArrayRef],
            None,
        ));
        let executed = result(
            vec![Field::new("p", DataType::Struct(inner), false)],
            vec![column],
        );
        let err = bless_shape_lines(&executed, &age_catalog()).unwrap_err();
        assert!(
            err.contains("column `p` is a struct that is no node type's object"),
            "{err}"
        );
        let msg = mismatch(&lines("p: String"), &executed).unwrap();
        assert!(
            msg.contains("expected String, the executor returned Struct"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn the_shape_is_checked_before_the_rows() {
        let text = format!(
            "{HDR}{AGE_SCHEMA}{AGE_SEED}{AGE_QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n--- expect shape\np.name: String\np.age: I64?\n"
        );
        let case = parse_case("x", &text).unwrap();
        let err = execute_case(&case, Path::new("unused.gqt"), false)
            .await
            .unwrap_err();
        assert!(err.contains("step 1 (query): result shape mismatch at column 1 `p.age`: expected I64, the executor returned I32"), "got: {err}");
        assert!(!err.contains("row mismatch"), "got: {err}");
    }

    #[tokio::test]
    async fn a_nullable_property_without_a_null_cell_may_be_written_strict() {
        let seed = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\",\"age\":30}}\n";
        let text = format!(
            "{HDR}{AGE_SCHEMA}{seed}{AGE_QUERY}--- expect unordered\n{{\"p.name\": \"alice\", \"p.age\": 30}}\n--- expect shape\np.name: String\np.age: I32\n"
        );
        let case = parse_case("x", &text).unwrap();
        execute_case(&case, Path::new("unused.gqt"), false)
            .await
            .unwrap();
        let text = format!(
            "{HDR}{AGE_SCHEMA}{AGE_SEED}{AGE_QUERY}{AGE_ROWS}--- expect shape\np.name: String\np.age: I32\n"
        );
        let case = parse_case("x", &text).unwrap();
        let err = execute_case(&case, Path::new("unused.gqt"), false)
            .await
            .unwrap_err();
        assert!(
            err.contains("`p.age`: written without `?`, the executor returned 1 null(s)"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn bless_fills_an_empty_shape_section_from_the_data_and_converges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bless_shape.gqt");
        let text = format!("{HDR}{AGE_SCHEMA}{AGE_SEED}{AGE_QUERY}{AGE_ROWS}--- expect shape\n");
        std::fs::write(&path, &text).unwrap();
        let case = parse_case("bless_shape", &text).unwrap();
        let err = execute_case(&case, &path, true).await.unwrap_err();
        assert!(
            err.contains("names 0 column(s), the executor returned 2"),
            "got: {err}"
        );
        assert!(
            err.contains("expect rewritten in place (2 lines)"),
            "got: {err}"
        );

        let blessed = std::fs::read_to_string(&path).unwrap();
        assert!(
            blessed.ends_with("--- expect shape\np.name: String\np.age: I32?\n"),
            "got: {blessed}"
        );
        let case = parse_case("bless_shape", &blessed).unwrap();
        execute_case(&case, &path, false).await.unwrap();
    }

    #[tokio::test]
    async fn bless_never_rewrites_a_shape_over_a_row_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bless_rows_only.gqt");
        let text = format!(
            "{HDR}{AGE_SCHEMA}{AGE_SEED}{AGE_QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n--- expect shape\np.name: String\np.age: I32?\n"
        );
        std::fs::write(&path, &text).unwrap();
        let case = parse_case("bless_rows_only", &text).unwrap();
        let err = execute_case(&case, &path, true).await.unwrap_err();
        assert!(err.contains("row mismatch"), "got: {err}");
        let blessed = std::fs::read_to_string(&path).unwrap();
        assert!(
            blessed.ends_with("--- expect shape\np.name: String\np.age: I32?\n"),
            "got: {blessed}"
        );
        assert!(
            blessed.contains("{\"p.age\":30,\"p.name\":\"alice\"}"),
            "got: {blessed}"
        );
    }
}

#[tokio::test]
async fn execution_reports_a_row_mismatch() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n{SHAPE}"
    );
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
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n{SHAPE}"
    );
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
    let v = pin_violation("indexed", 0, 0, true).unwrap();
    assert!(v.contains("no expand ran on it"), "{v}");
    let v = pin_violation("csr", 0, 0, true).unwrap();
    assert!(v.contains("no expand ran on it"), "{v}");
}

#[test]
fn expects_expand_ignores_bound_edges_and_plain_bindings() {
    let unbound = parse_query(TRAVERSAL_QUERY.trim_start_matches("--- query\n")).unwrap();
    assert!(expects_expand(&unbound.single_decl().match_clause));
    let bound = "query f($n: String) {\n    match {\n        $a: Person\n        $a.name = $n\n        \
                 $a $k:knows $b\n    }\n    return { $b.name }\n}\n";
    let bound = parse_query(bound).unwrap();
    assert!(!expects_expand(&bound.single_decl().match_clause));
    let plain = parse_query(QUERY.trim_start_matches("--- query\n")).unwrap();
    assert!(!expects_expand(&plain.single_decl().match_clause));
    let negated = "query f() {\n    match {\n        $a: Person\n        not { $a knows $x }\n    }\n    \
                   return { $a.name }\n}\n";
    let negated = parse_query(negated).unwrap();
    assert!(!expects_expand(&negated.single_decl().match_clause));
}

/// A two-node, one-edge graph with a one-hop traversal, for the pin tests.
const TRAVERSAL_SCHEMA: &str = "--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n--- schema\nnode Person {\n    name: String @key\n}\n\n\
                                edge Knows: Person -> Person {\n    since: I64\n}\n";
const TRAVERSAL_SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n\
                              {\"type\":\"Person\",\"data\":{\"name\":\"bob\"}}\n\
                              {\"edge\":\"Knows\",\"id\":\"k-1\",\"from\":\"alice\",\"to\":\"bob\",\"data\":{\"since\":2020}}\n";
const TRAVERSAL_QUERY: &str = "--- query\nquery friends($n: String) {\n    match {\n        $a: Person\n        \
                               $a.name = $n\n        $a knows $b\n    }\n    return { $b.name }\n}\n";
const TRAVERSAL_PARAMS: &str = "--- params\n{\"n\": \"alice\"}\n";
const TRAVERSAL_EXPECT: &str =
    "--- expect unordered\n{\"b.name\": \"bob\"}\n--- expect shape\nb.name: String\n";

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

        let (session, _uri, _dir) = open_case_store(&case, Engine::V2).await.unwrap();
        assert_eq!(session.settings().traversal().as_str(), mode);
        let Some(Item::Step(Step::Query(step))) = case.items.first() else {
            panic!("first item is the query step");
        };
        let params = build_params(step.params_raw.as_ref(), &step.decl.params, None).unwrap();
        let (outcome, counts) = under_traversal(
            Some(mode),
            session.query(
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

/// A reset that zeroed the pin to `auto` would switch the pin check off
/// silently; `pinned_mode` returning `None` drops the probes, and the
/// stage fails loudly instead.
#[tokio::test]
async fn traversal_pin_survives_settings_steps_and_rebind() {
    let text = format!(
        "{HDR}# traversal: csr\n{TRAVERSAL_SCHEMA}{TRAVERSAL_SEED}{TRAVERSAL_QUERY}{TRAVERSAL_PARAMS}{TRAVERSAL_EXPECT}\
         --- mutate\nset merge_lineage = off;\n\n--- expect ok\n\n\
         --- mutate\nreset all;\n\n--- expect ok\n\n\
         --- restart\n"
    );
    let case = parse_case("pinned", &text).unwrap();
    let (mut session, uri, _dir) = open_case_store(&case, Engine::V2).await.unwrap();
    let Some(Item::Step(Step::Query(query))) = case.items.first() else {
        panic!("first item is the query step");
    };
    assert_csr_pin_runs(&session, query, "baseline").await;

    let baseline_lineage = SessionSettings::default().get(SettingId::MergeLineage);
    let (mut settings_steps, mut reopens) = (0, 0);
    for item in &case.items[1..] {
        let Item::Step(step) = item else {
            panic!("the case has no loops");
        };
        let stage = match step {
            Step::Settings(s) => {
                run_settings_step(&PlainHost, &mut session, s, None)
                    .unwrap_or_else(|f| panic!("{}: {}", f.label, f.message));
                settings_steps += 1;
                let name = s.statements[0].statement_name();
                let lineage = session.settings().get(SettingId::MergeLineage);
                match name {
                    "set" => assert_eq!(lineage, "off"),
                    "reset" => assert_eq!(lineage, baseline_lineage),
                    other => panic!("unexpected statement `{other}`"),
                }
                format!("after `{name}`")
            }
            Step::Restart { .. } => {
                let detached = session.detach();
                let db = Omnigraph::open(&uri).await.unwrap();
                session = detached.attach(Arc::new(db));
                reopens += 1;
                "after reopen".to_string()
            }
            other => panic!("unexpected step {other:?}"),
        };
        assert_csr_pin_runs(&session, query, &stage).await;
    }
    assert_eq!((settings_steps, reopens), (2, 1));
}

/// The session still pins `csr`, and a run of `step` through the runner's
/// own pin plumbing expands on the csr path only.
async fn assert_csr_pin_runs(session: &Session, step: &QueryStep, stage: &str) {
    assert_eq!(session.settings().traversal(), Traversal::Csr, "{stage}");
    let params = build_params(step.params_raw.as_ref(), &step.decl.params, None).unwrap();
    let (outcome, counts) = under_traversal(
        pinned_mode(session),
        session.query(
            ReadTarget::branch("main"),
            &step.source,
            &step.name,
            &params,
        ),
    )
    .await;
    outcome.unwrap_or_else(|e| panic!("{stage}: {e}"));
    let counts = counts.unwrap_or_else(|| panic!("{stage}: the pin dropped, no probes attached"));
    let (indexed, csr) = (
        counts.indexed.load(Ordering::Relaxed),
        counts.csr.load(Ordering::Relaxed),
    );
    assert!(
        csr >= 1 && indexed == 0,
        "{stage}: indexed={indexed} csr={csr}"
    );
}

#[test]
fn refuses_ordered_expect_on_an_rrf_led_order() {
    let query = "--- query\nquery q($v: Vector(4), $t: String) {\n    match { $p: Person }\n    \
                 return { $p.name }\n    order { rrf(nearest($p.vec, $v), bm25($p.name, $t)) }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect ordered\n");
    let message = refusal("x", &text);
    assert!(message.contains("led by `rrf()`"), "{message}");
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n{SHAPE}");
    parse_case("x", &text).unwrap();
}

#[test]
fn refuses_ordered_expect_with_an_aggregate_in_return() {
    let query = "--- query\nquery q($t: String) {\n    match { $p: Person\n        search($p.name, $t) }\n    \
                 return { count($p) as total }\n    order { bm25($p.name, $t) }\n}\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{query}--- expect ordered\n");
    assert!(refusal("x", &text).contains("aggregate in its `return` list"));
    let text =
        format!("{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n--- expect shape\ntotal: I64?\n");
    parse_case("x", &text).unwrap();
}

const SET_MERGE_OFF: &str = "--- mutate\nset merge_lineage = off;\n--- expect ok\n";
const SHOW_MERGE_LINEAGE: &str = "--- query\nshow merge_lineage;\n--- expect unordered\n{\"name\": \"merge_lineage\", \"value\": \"off\", \"default\": \"on\", \"source\": \"file\", \"scope\": \"request\"}\n--- expect shape\nname: String\nvalue: String\ndefault: String\nsource: String\nscope: String\n";

/// The settings statements the compiler and the scope rule refuse, each with
/// the definition's message (the Session settings RFC, The statements): the runner refuses
/// the case at parse time, so no case can carry these as expectations.
#[test]
fn refuses_settings_statements_the_definition_refuses() {
    let text = format!("{HDR}{SCHEMA}{SEED}--- mutate\nset merge_lineage = v3;\n--- expect ok\n");
    let reason = refusal("x", &text);
    assert!(reason.contains("unknown value `v3`"), "{reason}");
    let text = format!("{HDR}{SCHEMA}{SEED}--- query\nshow traversal;\n--- expect unordered\n");
    let reason = refusal("x", &text);
    assert!(reason.contains("unknown setting `traversal`"), "{reason}");
}

/// A `process` setting in a case body is refused, with the line of the
/// statement, and `reset all` names no setting so the scope rule accepts it.
#[test]
fn refuses_a_process_setting_anywhere_in_a_case_body() {
    let needle = "`rrf_plan` is a process setting";
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- mutate\nset merge_lineage = off;\nset merge_lineage = on; set rrf_plan = force_prefilter;\n--- expect ok\n"
    );
    let reason = refusal("x", &text);
    assert!(reason.contains(needle), "{reason}");
    assert!(
        reason.starts_with("line 16: "),
        "the line is the offending statement's own, though it shares line 16 with an earlier `set`: {reason}"
    );
    let text =
        format!("{HDR}{SCHEMA}{SEED}--- mutate\nreset all;\n--- expect ok\n{SHOW_MERGE_LINEAGE}");
    let case = parse_case("x", &text).unwrap_or_else(|e| {
        panic!("`reset all` names no setting, so the scope rule has nothing to refuse: {e}")
    });
    assert_eq!(case.items.len(), 2);
}

#[test]
fn settings_step_and_show_take_their_section_and_expect() {
    let text = format!("{HDR}{SCHEMA}{SEED}{SET_MERGE_OFF}{SHOW_MERGE_LINEAGE}");
    let case = parse_case("x", &text).unwrap();
    let [
        Item::Step(Step::Settings(settings)),
        Item::Step(Step::Show(show)),
    ] = case.items.as_slice()
    else {
        panic!("a settings step then a show step, got {:?}", case.items);
    };
    assert_eq!(settings.statements.len(), 1);
    assert_eq!(show.id, Some(SettingId::MergeLineage));
    assert!(show.prefix.is_empty());
    assert!(matches!(
        show.expect,
        QueryExpect::Rows { ordered: false, .. }
    ));

    let text = format!("{HDR}{SCHEMA}{SEED}--- query\nset merge_lineage = off;\n--- expect ok\n");
    assert!(
        refusal("x", &text).contains("a settings step is a `--- mutate` step; use `--- mutate`")
    );
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- mutate branch: b0\nset merge_lineage = off;\n--- expect ok\n"
    );
    assert!(refusal("x", &text).contains("drop the `branch:` argument"));
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- mutate\nset merge_lineage = off;\n--- expect affected: nodes=0 edges=0\n"
    );
    assert!(refusal("x", &text).contains("a settings step takes `ok`"));
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- mutate\nset merge_lineage = off;\n--- params\n{{}}\n--- expect ok\n"
    );
    assert!(refusal("x", &text).contains("a settings statement takes no params"));

    let text = format!("{HDR}{SCHEMA}{SEED}--- mutate\nshow merge_lineage;\n--- expect ok\n");
    assert!(
        refusal("x", &text)
            .contains("`show merge_lineage` under `--- mutate` is refused; use `--- query`")
    );
    let text = format!("{HDR}{SCHEMA}{SEED}--- query branch: b0\nshow all;\n--- expect ordered\n");
    assert!(refusal("x", &text).contains("drop the `branch:` argument"));
    let text = format!("{HDR}{SCHEMA}{SEED}--- query\nshow all;\n--- expect error: nope\n");
    assert!(refusal("x", &text).contains("`show all` takes `unordered` or `ordered`"));
    let text = format!("{HDR}{SCHEMA}{SEED}--- query\nshow all;\n--- expect ordered\n");
    let case = parse_case("x", &text).unwrap();
    assert!(
        matches!(case.items.as_slice(), [Item::Step(Step::Show(_))]),
        "a `show` step derives its shape and needs no `--- expect shape` section"
    );
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- query\nshow all;\n--- expect ordered\n--- expect shape\nname: String\n"
    );
    assert!(
        refusal("x", &text).contains("`show` derives its shape"),
        "a shape section that is not the derived shape is refused"
    );

    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- mutate\nset merge_lineage = off;\nbranch merge b0\n--- expect ok\n--- query\nset merge_lineage = on;\nbranch list\n--- expect unordered\n{{\"name\": \"main\"}}\n--- expect shape\nname: String\n"
    );
    let case = parse_case("x", &text).unwrap();
    let [
        Item::Step(Step::Control(merge)),
        Item::Step(Step::List(list)),
    ] = case.items.as_slice()
    else {
        panic!("a merge then a list, got {:?}", case.items);
    };
    assert_eq!(
        merge.prefix.len(),
        1,
        "a prefix before a control write rides on the step; before `branch list` it selects nothing and is accepted"
    );
    assert_eq!(
        list.ordinal, 2,
        "`ListStep` carries no prefix: the parser validates the prefix and then drops it, because `branch list` selects nothing for a setting to steer"
    );
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- query\nset merge_lineage = nope;\nbranch list\n--- expect unordered\n{{\"name\": \"main\"}}\n--- expect shape\nname: String\n"
    );
    assert!(
        refusal("x", &text).contains("unknown value `nope`"),
        "a prefix on `branch list` is validated at parse time and then dropped: `ListStep` carries none"
    );
}

#[tokio::test]
async fn bless_refuses_cases_containing_loops() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bless_loop_case.gqt");
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- foreach $x a b\n{QUERY}--- expect unordered\n{{\"p.name\": \"nobody\"}}\n{SHAPE}--- endloop\n"
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
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{query}--- expect unordered\n{{\"p.nope\": \"x\"}}\n--- expect shape\np.nope: String\n"
    );
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

#[test]
fn runner_is_required_and_cannot_be_repeated() {
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}");
    let case = parse_case("normal", &text).unwrap();
    assert!(
        case.runner.environments[0].matches(Some("omnigraph-engine"), Some("local-filesystem"))
    );
    let runner_end = text.find("--- schema").unwrap();
    let missing = format!("{HDR}{}", &text[runner_end..]);
    assert!(refusal("normal", &missing).contains("--- runner"));
    let duplicate = text.replacen(
        "--- schema",
        &format!("{}--- schema", &text[HDR.len()..runner_end]),
        1,
    );
    assert!(parse_case("normal", &duplicate).is_err());
}

#[test]
fn fault_limits_and_loop_scope_are_enforced() {
    let fault = "--- seam\nat: mutation.post_sidecar_pre_fork\noccurrence: 1\naction: fail\nscope: next_step\n";
    let operation = "--- mutate\nquery add() { insert Person { name: \"bob\" } }\n--- expect error: injected failpoint\n";
    let prefix = format!("{HDR}{SCHEMA}{SEED}");
    assert!(
        parse_case(
            "fault_limit",
            &format!("{prefix}{}", format!("{fault}{operation}").repeat(16))
        )
        .is_ok()
    );
    assert!(
        refusal(
            "fault_limit",
            &format!("{prefix}{}", format!("{fault}{operation}").repeat(17))
        )
        .contains("at most 16")
    );
    assert!(
        refusal(
            "fault_loop",
            &format!("{prefix}--- loop $i 0 1\n{fault}{operation}--- endloop\n")
        )
        .contains("inside loops")
    );
}

async fn open_case_store(
    case: &Case,
    engine: Engine,
) -> Result<(Session, String, tempfile::TempDir), String> {
    let fixture = case
        .fixture
        .as_ref()
        .ok_or("invalid_case: a case without schema and seed requires --store <URI>")?;
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir failed: {e}"))?;
    let uri = dir
        .path()
        .to_str()
        .ok_or_else(|| "temp path is not utf-8".to_string())?
        .to_string();
    let db = Omnigraph::init(&uri, &fixture.schema)
        .await
        .map_err(|e| format!("init failed: {e}"))?;
    let session = case_session(db, case, engine)?;
    seed_case(&session, &fixture.seed, case.needs_indices).await?;
    Ok((session, uri, dir))
}

fn execute_case<'a>(
    case: &'a Case,
    path: &'a Path,
    bless: bool,
) -> futures::future::BoxFuture<'a, Result<(), String>> {
    async move {
        let (session, uri, _dir) = open_case_store(case, Engine::V2).await?;
        execute_steps(case, path, bless, session, &uri, None, &PlainHost)
            .await
            .map(|_| ())
    }
    .boxed()
}

#[test]
fn core_manifest_has_no_runner_or_test_feature_dependency() {
    let manifest = include_str!("../Cargo.toml");
    for forbidden in [
        "omnigraph-dst",
        "omnigraph-reference-engine",
        "test-util",
        "[build-dependencies]",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "core dependency boundary: {forbidden}"
        );
    }
    assert!(
        !Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("build.rs")
            .exists()
    );
}

#[test]
fn step_descriptors_preserve_source_and_loop_membership() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}--- loop $i 0 1\n{QUERY}{EXPECT}--- endloop\n--- restart\n{QUERY}{EXPECT}"
    );
    let case = parse_case("descriptors", &text).unwrap();
    let steps = case.steps();
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].kind, StepKind::Query);
    assert!(steps[0].in_loop);
    assert_eq!(steps[0].source, QUERY.trim());
    assert_eq!(steps[1].kind, StepKind::Restart);
    assert!(!steps[1].in_loop);
    assert_eq!(steps[2].ordinal, 3);
    assert!(!steps[2].in_loop);
}

#[derive(Default)]
struct RecordingHost {
    events: std::sync::Mutex<Vec<String>>,
    faults: std::sync::atomic::AtomicUsize,
    storage: Option<Arc<dyn StorageAdapter>>,
    refuse_start: bool,
    refuse_finish: bool,
}

impl ExecutionHost for RecordingHost {
    type StepGuard = ();

    fn arm_seams(&self, seams: &[SeamDirective], step: &Step) -> Result<(), String> {
        PlainHost.arm_seams(seams, step)
    }

    fn finish_seams(&self, guard: ()) -> Result<(), String> {
        PlainHost.finish_seams(guard)
    }

    fn observe_fault(&self, _error: &omnigraph::error::OmniError) {
        self.faults
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn operation_started(&self, ordinal: usize) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push(format!("started {ordinal}"));
        if self.refuse_start {
            Err("before engine operation".into())
        } else {
            Ok(())
        }
    }

    fn operation_finished(&self, ordinal: usize) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push(format!("finished {ordinal}"));
        if self.refuse_finish {
            Err("after engine operation".into())
        } else {
            Ok(())
        }
    }

    fn record(&self, kind: &str, value: impl FnOnce() -> Value) {
        if kind == "assertion" {
            self.events
                .lock()
                .unwrap()
                .push(format!("assertion {}", value()["status"]));
        }
    }

    fn reopen<'a>(
        &'a self,
        uri: &'a str,
        storage: Option<Arc<dyn StorageAdapter>>,
    ) -> futures::future::BoxFuture<'a, Result<Omnigraph, omnigraph::error::OmniError>> {
        assert!(Arc::ptr_eq(
            self.storage.as_ref().unwrap(),
            storage.as_ref().unwrap()
        ));
        self.events.lock().unwrap().push("reopen".into());
        async move { PlainHost.reopen(uri, storage).await }.boxed()
    }
}

#[tokio::test]
async fn host_refusal_stops_at_the_operation_boundary() {
    let polled = std::sync::atomic::AtomicBool::new(false);
    let host = RecordingHost {
        refuse_start: true,
        ..Default::default()
    };
    let result = operation(&host, 1, async {
        polled.store(true, Ordering::SeqCst);
    })
    .await;
    assert_eq!(result.unwrap_err(), "before engine operation");
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(*host.events.lock().unwrap(), ["started 1"]);

    let host = RecordingHost {
        refuse_finish: true,
        ..Default::default()
    };
    let result = operation(&host, 2, async {
        polled.store(true, Ordering::SeqCst);
    })
    .await;
    assert_eq!(result.unwrap_err(), "after engine operation");
    assert!(polled.load(Ordering::SeqCst));
    assert_eq!(*host.events.lock().unwrap(), ["started 2", "finished 2"]);
}

#[tokio::test]
async fn engine_operation_finishes_before_failed_expectation() {
    let text = format!(
        "{HDR}{SCHEMA}{SEED}{QUERY}{}",
        EXPECT.replace("alice", "bob")
    );
    let case = parse_case("failed_expectation", &text).unwrap();
    let (session, uri, _dir) = open_case_store(&case, Engine::V2).await.unwrap();
    let host = RecordingHost::default();
    let result = execute_steps(
        &case,
        Path::new("unused.gqt"),
        false,
        session,
        &uri,
        None,
        &host,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        *host.events.lock().unwrap(),
        ["started 1", "finished 1", "assertion \"failed\""]
    );
}

#[tokio::test]
async fn restart_preserves_supplied_storage_and_returns_current_session() {
    let both = "--- expect unordered\n{\"p.name\":\"alice\"}\n{\"p.name\":\"bob\"}\n--- expect shape\np.name: String\n";
    let text = format!("{HDR}{SCHEMA}{SEED}{MUTATE}{PARAMS}{EXPECT_OK}--- restart\n{QUERY}{both}");
    let case = parse_case("restart_storage", &text).unwrap();
    let (session, uri, _dir) = open_case_store(&case, Engine::V2).await.unwrap();
    let storage: Arc<dyn StorageAdapter> =
        Arc::new(omnigraph::storage::ObjectStorageAdapter::local());
    let host = RecordingHost {
        storage: Some(storage.clone()),
        ..Default::default()
    };
    let session = execute_steps(
        &case,
        Path::new("unused.gqt"),
        false,
        session,
        &uri,
        Some(storage),
        &host,
    )
    .await
    .unwrap();
    let result = session
        .query(
            ReadTarget::branch("main"),
            QUERY.trim_start_matches("--- query\n"),
            "all",
            &omnigraph_compiler::ParamMap::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(
        *host.events.lock().unwrap(),
        [
            "started 1",
            "finished 1",
            "assertion \"passed\"",
            "started 2",
            "reopen",
            "finished 2",
            "assertion \"passed\"",
            "started 3",
            "finished 3",
            "assertion \"passed\""
        ]
    );
}

#[test]
fn descriptor_echo_distinguishes_branches_with_identical_queries() {
    let query_on_branch = QUERY.replacen("--- query", "--- query branch: b0", 1);
    let text = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}{query_on_branch}{EXPECT}");
    let case = parse_case("branch_echo", &text).unwrap();
    let steps = case.steps();
    assert_eq!(steps[0].source, QUERY.trim());
    assert_eq!(steps[1].source, query_on_branch.trim());
    assert_ne!(steps[0].source, steps[1].source);
}

#[tokio::test]
async fn traversal_pin_preserves_the_hosts_query_io_probes() {
    let inherited = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let probes = omnigraph::instrumentation::QueryIoProbes {
        probe_count: inherited.clone(),
        ..Default::default()
    };
    omnigraph::instrumentation::with_query_io_probes(probes, async {
        let (_, counts) = super::under_traversal(Some("indexed"), async {
            let current = omnigraph::instrumentation::capture_query_io_probes().unwrap();
            current
                .probe_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            current
                .expand_indexed_runs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
        .await;
        assert_eq!(
            counts
                .unwrap()
                .indexed
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    })
    .await;
    assert_eq!(inherited.load(std::sync::atomic::Ordering::Relaxed), 1);
}
