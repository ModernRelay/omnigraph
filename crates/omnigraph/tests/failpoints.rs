#![cfg(feature = "failpoints")]

mod helpers;

use fail::FailScenario;
use omnigraph::db::Omnigraph;
use omnigraph::failpoints::ScopedFailPoint;

use helpers::{MUTATION_QUERIES, mixed_params, mutate_main};

const SCHEMA_V1: &str = "node Person { name: String @key }\n";
const SCHEMA_V2_ADDED_TYPE: &str =
    "node Person { name: String @key }\nnode Company { name: String @key }\n";

#[tokio::test]
async fn branch_create_failpoint_triggers() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap();
    let _failpoint = ScopedFailPoint::new("branch_create.after_manifest_branch_create", "return");

    let err = db.branch_create("feature").await.unwrap_err();
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: branch_create.after_manifest_branch_create")
    );
}

#[tokio::test]
async fn graph_publish_failpoint_triggers_before_commit_append() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
        .await
        .unwrap();
    let _failpoint = ScopedFailPoint::new("graph_publish.before_commit_append", "return");

    let err = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: graph_publish.before_commit_append")
    );
}

// Atomic schema apply: schema apply writes staging files first, then commits
// the manifest, then renames staging → final. Tests below inject crashes at
// the two boundaries and assert that reopening the repo yields a consistent
// state.

#[tokio::test]
async fn schema_apply_recovers_pre_commit_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let mut db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        let _failpoint = ScopedFailPoint::new("schema_apply.after_staging_write", "return");
        let err = db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_staging_write"),
            "got: {}",
            err
        );
    }

    // Reopen — recovery sweep should delete staging files and keep the
    // original schema, since the manifest commit never happened.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(db.schema_source(), SCHEMA_V1);
    assert_no_staging_files(dir.path());
}

#[tokio::test]
async fn schema_apply_recovers_post_commit_crash() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let mut db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        let _failpoint = ScopedFailPoint::new("schema_apply.after_manifest_commit", "return");
        let err = db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected failpoint triggered: schema_apply.after_manifest_commit"),
            "got: {}",
            err
        );
    }

    // Reopen — manifest is at the new version, so recovery sweep should
    // complete the rename and the live schema matches v2.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(db.schema_source(), SCHEMA_V2_ADDED_TYPE);
    assert_no_staging_files(dir.path());
}

#[tokio::test]
async fn schema_apply_recovers_partial_rename() {
    // Construct a partial-rename state: _schema.pg has been renamed in
    // (matching v2), but _schema.ir.json.staging and __schema_state.json.staging
    // were never renamed. Recovery should detect that the live source matches
    // the staging state's hash and complete the remaining renames.
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let mut db = Omnigraph::init(&uri, SCHEMA_V1).await.unwrap();
        db.apply_schema(SCHEMA_V2_ADDED_TYPE).await.unwrap();
    }

    // Simulate: one of the renames (the IR or state file) didn't complete by
    // copying the live ir/state files back to their staging names.
    std::fs::copy(
        dir.path().join("_schema.ir.json"),
        dir.path().join("_schema.ir.json.staging"),
    )
    .unwrap();
    std::fs::copy(
        dir.path().join("__schema_state.json"),
        dir.path().join("__schema_state.json.staging"),
    )
    .unwrap();

    // Reopen — recovery should complete the rename (overwriting final files
    // with identical staging content) and remove the staging files.
    let db = Omnigraph::open(&uri).await.unwrap();
    assert_eq!(db.schema_source(), SCHEMA_V2_ADDED_TYPE);
    assert_no_staging_files(dir.path());
}

fn assert_no_staging_files(repo: &std::path::Path) {
    for name in [
        "_schema.pg.staging",
        "_schema.ir.json.staging",
        "__schema_state.json.staging",
    ] {
        let path = repo.join(name);
        assert!(
            !path.exists(),
            "staging file {} still exists after recovery",
            path.display()
        );
    }
}
