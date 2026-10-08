use super::*;

const HDR: &str = "# issue: none\n";
const SCHEMA: &str = "--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n--- schema\nnode Person {\n    name: String @key\n}\n";
const SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n";
const QUERY: &str =
    "--- query\nquery all() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
const EXPECT: &str =
    "--- expect unordered\n{\"p.name\": \"alice\"}\n--- expect shape\np.name: String\n";
const SAME: &str = "--- expect same as v1\n";
const TRAVERSAL_SCHEMA: &str = "--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n--- schema\nnode Person {\n    name: String @key\n}\n\n\
                                edge Knows: Person -> Person {\n    since: I64\n}\n";
const TRAVERSAL_SEED: &str = "--- seed\n{\"type\":\"Person\",\"data\":{\"name\":\"alice\"}}\n\
                              {\"type\":\"Person\",\"data\":{\"name\":\"bob\"}}\n\
                              {\"edge\":\"Knows\",\"id\":\"k-1\",\"from\":\"alice\",\"to\":\"bob\",\"data\":{\"since\":2020}}\n";

#[tokio::test]
async fn reference_comparison_refuses_named_type_access_in_every_read_position() {
    for (clauses, returns, order, rows, shape) in [
        (
            "$a: Person $a $e:knows $b",
            "$e.@type as kind",
            "",
            "{\"kind\":\"Knows\"}",
            "kind: String",
        ),
        (
            "$a: Person $a $e:knows $b $e.@type = \"Knows\"",
            "$b.name as name",
            "",
            "{\"name\":\"bob\"}",
            "name: String",
        ),
        (
            "$a: Person $a $e:knows $b",
            "count($e.@type) as n",
            "",
            "{\"n\":1}",
            "n: I64",
        ),
        (
            "$a: Person $a $e:knows $b",
            "$b.name as name",
            "order { $e.@type }",
            "{\"name\":\"bob\"}",
            "name: String",
        ),
        (
            "$a: Person exists { $a $e:knows $b $e.@type = \"Knows\" }",
            "$a.name as name",
            "",
            "{\"name\":\"alice\"}",
            "name: String",
        ),
    ] {
        let text = format!(
            "{HDR}{TRAVERSAL_SCHEMA}{TRAVERSAL_SEED}--- query\nquery q() {{ match {{ {clauses} }} return {{ {returns} }} {order} }}\n--- expect unordered\n{rows}\n--- expect shape\n{shape}\n--- expect same as v1\n"
        );
        let case = parse_case("reference_type_admission", &text).unwrap();
        let error = execute_case(&case, Path::new("unused.gqt"), false)
            .await
            .unwrap_err();
        assert!(
            error.contains("reference engine does not support edge @type access"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn same_as_v1_fails_on_a_v1_error_or_a_row_difference() {
    let equal = format!("{HDR}{SCHEMA}{SEED}{QUERY}{EXPECT}{SAME}");
    let case = parse_case("x", &equal).unwrap();
    execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap();

    let compound = "--- query\nquery either() {\n    match { $p: Person  $p.name = \"alice\" or $p.name = \"bob\" }\n    return { $p.name }\n}\n";
    let refused = format!("{HDR}{SCHEMA}{SEED}{compound}{EXPECT}{SAME}");
    let case = parse_case("x", &refused).unwrap();
    let err = execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap_err();
    assert!(
        err.contains("expect same as v1: v2 returned rows, v1 failed")
            && err.contains("compound predicates")
            && err.contains("drop `--- expect same as v1` from this step"),
        "{err}"
    );

    let differing = "# issue: none\n--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n\n\
         --- schema\nnode P {\n    k: String @key\n    age: I64\n    score: F64\n}\n\
         --- seed\n{\"type\":\"P\",\"data\":{\"k\":\"k\",\"age\":7,\"score\":7.5}}\n\
         --- query\nquery kept() {\n    match { $p: P  not { $p.age < $p.score } }\n    return { $p.k }\n}\n\
         --- expect unordered\n--- expect shape\np.k: String\n--- expect same as v1\n";
    let case = parse_case("x", differing).unwrap();
    let err = execute_case(&case, Path::new("unused.gqt"), false)
        .await
        .unwrap_err();
    assert!(
        err.contains("the engines disagree (expected = v1, actual = v2)")
            && err.contains("{\"p.k\":\"k\"}"),
        "{err}"
    );
}

#[tokio::test]
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

#[tokio::test]
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
fn runner_refuses_a_process_settings_override() {
    assert!(settings_override_refusal(|_| None).is_none());
    let one = |set: &'static str, value: &'static str| {
        settings_override_refusal(move |name| (name == set).then(|| OsString::from(value)))
    };
    let reason = one("OMNIGRAPH_TRAVERSAL_MODE", "csr").unwrap();
    assert!(reason.contains("OMNIGRAPH_TRAVERSAL_MODE=csr"), "{reason}");
    assert!(
        reason.contains("names no setting any more and decides nothing"),
        "{reason}"
    );
    let reason = one("OMNIGRAPH_MERGE_LINEAGE", "off").unwrap();
    assert!(reason.contains("OMNIGRAPH_MERGE_LINEAGE=off"), "{reason}");
    assert!(
        reason.contains("`set merge_lineage = <value>;`"),
        "{reason}"
    );
    for spec in DEFINITIONS {
        assert!(one(spec.env, "x").is_some(), "{} is not refused", spec.env);
    }
    assert!(one("OMNIGRAPH_GQ_UNRELATED", "x").is_none());
}

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
    std::fs::create_dir_all(dir.path().join("v2/planner")).unwrap();
    std::fs::write(dir.path().join("v2/planner/plan.gqt"), "x").unwrap();
    std::fs::write(dir.path().join("v2/planner/.hidden.gqt"), "x").unwrap();
    std::fs::write(dir.path().join("v2/planner/notes.txt"), "x").unwrap();
    std::fs::create_dir(dir.path().join(".cache")).unwrap();
    std::fs::write(dir.path().join(".cache/skipped.gqt"), "x").unwrap();
    let mut expected = vec![
        ".hidden.gqt".to_string(),
        "b.txt".to_string(),
        "c.GQT".to_string(),
        "v2/planner/.hidden.gqt".to_string(),
        "v2/planner/notes.txt".to_string(),
    ];
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("a.gqt", dir.path().join("link.gqt")).unwrap();
        expected.push("link.gqt".to_string());
        std::os::unix::fs::symlink("planner", dir.path().join("v2/link")).unwrap();
        expected.push("v2/link".to_string());
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
    assert_eq!(
        files,
        vec![
            dir.path().join("a.gqt"),
            dir.path().join("nested/d.gqt"),
            dir.path().join("v2/planner/plan.gqt"),
        ]
    );
    assert_eq!(foreign, expected);
}
