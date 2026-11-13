use super::*;

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
