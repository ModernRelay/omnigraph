//! Engine-layer policy enforcement (MR-722 chassis core, PR #2 + PR #3).
//!
//! These tests exercise `Omnigraph::with_policy()` + every `_as` writer
//! via the SDK directly — *no HTTP layer involved*. They're the proof
//! that engine-layer enforcement works for embedded callers and CLI
//! direct-engine writes, not just server requests.
//!
//! PR #2 wired `apply_schema_as`. PR #3 fans the same `enforce()` call
//! out to the remaining six writers — `mutate_as`, `load_as`,
//! `ingest_as`, `branch_create_as` / `branch_create_from_as`,
//! `branch_delete_as`, `branch_merge_as`. Each writer pair below
//! covers allow + deny via the engine-side gate; the allow case proves
//! the enforce call is correctly scoped (i.e. doesn't reject a legit
//! actor), the deny case proves it actually denies an unauthorized
//! actor — and both together pin the action × scope shape to match the
//! HTTP-layer authorize_request convention so engine and HTTP fire the
//! same Cedar decision.

mod helpers;

use std::fs;
use std::path::Path;
use std::sync::Arc;

use omnigraph::db::{Omnigraph, ReadTarget, SchemaApplyOptions};
use omnigraph::loader::LoadMode;
use omnigraph::error::OmniError;
use omnigraph_policy::{PolicyChecker, PolicyEngine};

use helpers::*;

/// Cedar policy: `act-allowed` may do every write; `act-denied` is in
/// the known-actors set (so Cedar evaluates the policy and doesn't
/// reject as unknown) but has no permit rule and is therefore implicitly
/// denied for every action.
///
/// The rule split mirrors the per-action scope convention: Change uses
/// `branch_scope`; SchemaApply, BranchCreate, BranchDelete, BranchMerge
/// use `target_branch_scope` (see `PolicyAction::uses_branch_scope` and
/// `uses_target_branch_scope` in `omnigraph-policy`).
const POLICY_YAML: &str = r#"
version: 1
groups:
  writers: [act-allowed]
  readers: [act-denied]
protected_branches: [main]
rules:
  - id: writers-data
    allow:
      actors: { group: writers }
      actions: [change]
      branch_scope: any
  - id: writers-branches-schema
    allow:
      actors: { group: writers }
      actions: [schema_apply, branch_create, branch_delete, branch_merge]
      target_branch_scope: any
"#;

fn additive_schema() -> String {
    helpers::TEST_SCHEMA.replace("    age: I32?\n}", "    age: I32?\n    nickname: String?\n}")
}

fn install_policy(db: Omnigraph, dir_path: &Path) -> (Omnigraph, Arc<PolicyEngine>) {
    let policy_path = dir_path.join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let engine = PolicyEngine::load_graph(&policy_path, dir_path.to_str().unwrap()).unwrap();
    let engine = Arc::new(engine);
    let db = db.with_policy(Arc::clone(&engine) as Arc<dyn PolicyChecker>);
    (db, engine)
}

async fn init_with_policy(dir: &tempfile::TempDir) -> (Omnigraph, Arc<PolicyEngine>) {
    let db = init_and_load(dir).await;
    install_policy(db, dir.path())
}

/// Variant for tests that need a pre-created feature branch (branch_delete /
/// branch_merge setup). Create the branch BEFORE wrapping with policy so the
/// setup itself doesn't need to satisfy BranchCreate.
async fn init_with_policy_and_feature_branch(
    dir: &tempfile::TempDir,
    branch: &str,
) -> (Omnigraph, Arc<PolicyEngine>) {
    let db = init_and_load(dir).await;
    db.branch_create_from(ReadTarget::branch("main"), branch)
        .await
        .expect("setup: create feature branch before installing policy");
    install_policy(db, dir.path())
}

// `MUTATION_QUERIES` from helpers/mod.rs already defines `insert_person($name, $age)`
// — reuse it rather than redefining one here, so this test exercises the
// same surface the engine integration tests do.

/// One JSONL record for `load_as` / `ingest_as` exercises.
const ONE_PERSON_JSONL: &str = r#"{"type": "Person", "data": {"name": "Eve"}}"#;

fn assert_denied(result: Result<impl std::fmt::Debug, OmniError>, what: &str) {
    match result {
        Err(OmniError::Policy(msg)) => {
            assert!(
                msg.contains("denied"),
                "{what}: expected denial message, got: {msg}"
            );
        }
        Err(other) => panic!("{what}: expected OmniError::Policy, got: {other:?}"),
        Ok(value) => panic!("{what}: expected denial, got Ok({value:?})"),
    }
}

#[tokio::test]
async fn apply_schema_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let desired = additive_schema();
    let result = db
        .apply_schema_as(&desired, SchemaApplyOptions::default(), Some("act-denied"))
        .await;

    match result {
        Err(OmniError::Policy(msg)) => {
            assert!(
                msg.contains("denied"),
                "expected denial message, got: {msg}"
            );
        }
        Err(other) => panic!("expected OmniError::Policy, got: {other:?}"),
        Ok(_) => panic!("expected denial — act-denied should not be able to SchemaApply"),
    }
}

#[tokio::test]
async fn apply_schema_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let desired = additive_schema();
    let result = db
        .apply_schema_as(&desired, SchemaApplyOptions::default(), Some("act-allowed"))
        .await
        .expect("act-allowed should be able to SchemaApply");
    assert!(result.applied);
}

#[tokio::test]
async fn apply_schema_without_actor_when_policy_is_installed_denies() {
    // MR-722 footgun guard: if a PolicyChecker is installed AND the
    // call site forgets to pass an actor, enforce() fails hard. Silent
    // bypass via "I forgot the actor" is exactly what the gate is
    // here to prevent.
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let desired = additive_schema();
    // `apply_schema(...)` is the no-actor variant — delegates to
    // apply_schema_as with actor=None.
    let result = db.apply_schema(&desired).await;

    match result {
        Err(OmniError::Policy(msg)) => {
            assert!(
                msg.contains("no actor"),
                "expected 'no actor' message, got: {msg}"
            );
        }
        Err(other) => panic!("expected OmniError::Policy('no actor ...'), got: {other:?}"),
        Ok(_) => panic!("expected denial — policy is installed but no actor was threaded"),
    }
}

#[tokio::test]
async fn apply_schema_without_policy_still_works() {
    // Baseline: when no policy is installed (the embedded/dev default),
    // apply_schema and apply_schema_as both work regardless of whether
    // an actor is passed. The enforce() gate is a strict no-op in this
    // shape — proves PR #2 doesn't regress the no-policy path.
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let desired = additive_schema();
    // No-actor variant.
    db.apply_schema(&desired)
        .await
        .expect("no policy → no enforcement → apply succeeds");
}

// ─── PR #3 writer fan-out ─────────────────────────────────────────────────
//
// One allow + one deny test per newly-wired writer. The allow case
// proves the enforce scope is correctly shaped (i.e. doesn't reject a
// legit actor whose policy permit matches the engine-side scope). The
// deny case proves the gate actually fires for an unauthorized actor.
// Footgun-guard (no-actor + policy-installed) is already proved by
// `apply_schema_without_actor_when_policy_is_installed_denies` and
// applies identically to every `_as` variant — duplicating it per
// writer would be redundant.

#[tokio::test]
async fn mutate_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let params = mixed_params(&[("$name", "Eve")], &[("$age", 22)]);
    let result = db
        .mutate_as(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &params,
            Some("act-denied"),
        )
        .await;
    assert_denied(result, "mutate_as");
}

#[tokio::test]
async fn mutate_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let params = mixed_params(&[("$name", "Eve")], &[("$age", 22)]);
    db.mutate_as(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &params,
        Some("act-allowed"),
    )
    .await
    .expect("act-allowed should be able to Change on main");
}

#[tokio::test]
async fn load_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let result = db
        .load_as("main", ONE_PERSON_JSONL, LoadMode::Merge, Some("act-denied"))
        .await;
    assert_denied(result, "load_as");
}

#[tokio::test]
async fn load_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    db.load_as(
        "main",
        ONE_PERSON_JSONL,
        LoadMode::Merge,
        Some("act-allowed"),
    )
    .await
    .expect("act-allowed should be able to load on main");
}

#[tokio::test]
async fn load_file_as_denies_when_policy_rejects_actor() {
    // `load_file_as` was added in PR #104 as the actor-aware mirror of
    // `load_file`, used by the CLI's `omnigraph load`. Tested
    // indirectly via CLI integration; this test closes the direct-SDK
    // gap so a regression in the file-read path doesn't ride through
    // unnoticed.
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;
    let data_path = dir.path().join("one-person.jsonl");
    fs::write(&data_path, ONE_PERSON_JSONL).unwrap();

    let result = db
        .load_file_as(
            "main",
            data_path.to_str().unwrap(),
            LoadMode::Merge,
            Some("act-denied"),
        )
        .await;
    assert_denied(result, "load_file_as");
}

#[tokio::test]
async fn load_file_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;
    let data_path = dir.path().join("one-person.jsonl");
    fs::write(&data_path, ONE_PERSON_JSONL).unwrap();

    db.load_file_as(
        "main",
        data_path.to_str().unwrap(),
        LoadMode::Merge,
        Some("act-allowed"),
    )
    .await
    .expect("act-allowed should be able to load_file_as on main");
}

#[tokio::test]
async fn ingest_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let result = db
        .ingest_as(
            "main",
            Some("main"),
            ONE_PERSON_JSONL,
            LoadMode::Merge,
            Some("act-denied"),
        )
        .await;
    assert_denied(result, "ingest_as");
}

#[tokio::test]
async fn ingest_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    db.ingest_as(
        "main",
        Some("main"),
        ONE_PERSON_JSONL,
        LoadMode::Merge,
        Some("act-allowed"),
    )
    .await
    .expect("act-allowed should be able to ingest on main");
}

#[tokio::test]
async fn branch_create_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let result = db.branch_create_as("feature", Some("act-denied")).await;
    assert_denied(result, "branch_create_as");
}

#[tokio::test]
async fn branch_create_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    db.branch_create_as("feature", Some("act-allowed"))
        .await
        .expect("act-allowed should be able to BranchCreate");
}

#[tokio::test]
async fn branch_create_from_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let result = db
        .branch_create_from_as(ReadTarget::branch("main"), "feature", Some("act-denied"))
        .await;
    assert_denied(result, "branch_create_from_as");
}

#[tokio::test]
async fn branch_create_from_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    db.branch_create_from_as(ReadTarget::branch("main"), "feature", Some("act-allowed"))
        .await
        .expect("act-allowed should be able to BranchCreate from main");
}

#[tokio::test]
async fn branch_delete_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy_and_feature_branch(&dir, "feature").await;

    let result = db.branch_delete_as("feature", Some("act-denied")).await;
    assert_denied(result, "branch_delete_as");
}

#[tokio::test]
async fn branch_delete_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy_and_feature_branch(&dir, "feature").await;

    db.branch_delete_as("feature", Some("act-allowed"))
        .await
        .expect("act-allowed should be able to BranchDelete");
}

#[tokio::test]
async fn branch_merge_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy_and_feature_branch(&dir, "feature").await;

    let result = db
        .branch_merge_as("feature", "main", Some("act-denied"))
        .await;
    assert_denied(result, "branch_merge_as");
}

#[tokio::test]
async fn branch_merge_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy_and_feature_branch(&dir, "feature").await;

    // No diverging writes on feature → merge is a no-op fast-forward,
    // but it still goes through enforce(BranchMerge, ...). That's the
    // path under test; the actual merge outcome is incidental.
    db.branch_merge_as("feature", "main", Some("act-allowed"))
        .await
        .expect("act-allowed should be able to BranchMerge");
}
