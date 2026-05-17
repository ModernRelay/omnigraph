//! Engine-layer policy enforcement (MR-722 chassis core, PR #2).
//!
//! These tests exercise `Omnigraph::with_policy()` + `apply_schema_as()`
//! via the SDK directly — *no HTTP layer involved*. They're the proof
//! that engine-layer enforcement works for embedded callers and CLI
//! direct-engine writes, not just server requests.
//!
//! `apply_schema_as` is the only writer wired in PR #2; PR #3 fans the
//! `enforce()` call out to the other six (`mutate_as`, `load`,
//! `ingest_as`, `branch_create_from`, `branch_delete`, `branch_merge`).

mod helpers;

use std::fs;
use std::sync::Arc;

use omnigraph::db::{Omnigraph, SchemaApplyOptions};
use omnigraph::error::OmniError;
use omnigraph_policy::{PolicyChecker, PolicyEngine};

use helpers::*;

/// Cedar policy: `act-allowed` may SchemaApply; `act-denied` is in the
/// known-actors set (so Cedar evaluates the policy, doesn't reject as
/// unknown) but has no permit rule.
const POLICY_YAML: &str = r#"
version: 1
groups:
  schema-writers: [act-allowed]
  readers: [act-denied]
protected_branches: [main]
rules:
  - id: writers-schema-apply
    allow:
      actors: { group: schema-writers }
      actions: [schema_apply]
      target_branch_scope: any
"#;

fn additive_schema() -> String {
    helpers::TEST_SCHEMA.replace("    age: I32?\n}", "    age: I32?\n    nickname: String?\n}")
}

async fn init_with_policy(dir: &tempfile::TempDir) -> (Omnigraph, Arc<PolicyEngine>) {
    let db = init_and_load(dir).await;
    let policy_path = dir.path().join("policy.yaml");
    fs::write(&policy_path, POLICY_YAML).unwrap();
    let engine = PolicyEngine::load(&policy_path, dir.path().to_str().unwrap()).unwrap();
    let engine = Arc::new(engine);
    let db = db.with_policy(Arc::clone(&engine) as Arc<dyn PolicyChecker>);
    (db, engine)
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
