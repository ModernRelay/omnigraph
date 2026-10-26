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
//! `branch_delete_as`, `branch_merge_as`. The maintenance entry points —
//! `optimize_as`, `ensure_indices_as` / `ensure_indices_on_as`,
//! `rebuild_full_text_indices_on_as`, `repair_report_as` (`maintain`) and
//! `cleanup_as` (`reclaim`) — are covered the same way. Each writer pair below
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

use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget, SnapshotId};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::loader::LoadMode;
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
    helpers::TEST_SCHEMA.replace(
        "    age: I32?\n}",
        "    age: I32?\n    nickname: String?\n}",
    )
}

fn install_policy(db: Omnigraph, dir_path: &Path) -> (Session, Arc<PolicyEngine>) {
    install_policy_source(db, dir_path, POLICY_YAML)
}

fn install_policy_source(
    db: Omnigraph,
    dir_path: &Path,
    source: &str,
) -> (Session, Arc<PolicyEngine>) {
    let policy_path = dir_path.join("policy.yaml");
    fs::write(&policy_path, source).unwrap();
    let engine = PolicyEngine::load_graph(&policy_path, dir_path.to_str().unwrap()).unwrap();
    let engine = Arc::new(engine);
    let db = helpers::session(db.with_policy(Arc::clone(&engine) as Arc<dyn PolicyChecker>));
    (db, engine)
}

async fn init_with_policy(dir: &tempfile::TempDir) -> (Session, Arc<PolicyEngine>) {
    drop(init_and_load(dir).await);
    let db = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    install_policy(db, dir.path())
}

/// Variant for tests that need a pre-created feature branch (branch_delete /
/// branch_merge setup). Create the branch BEFORE wrapping with policy so the
/// setup itself doesn't need to satisfy BranchCreate.
async fn init_with_policy_and_feature_branch(
    dir: &tempfile::TempDir,
    branch: &str,
) -> (Session, Arc<PolicyEngine>) {
    let db = init_and_load(dir).await;
    db.branch_create_from(ReadTarget::branch("main"), branch)
        .await
        .expect("setup: create feature branch before installing policy");
    drop(db);
    let db = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    install_policy(db, dir.path())
}

// `MUTATION_QUERIES` from helpers/mod.rs already defines `insert_person($name, $age)`
// — reuse it rather than redefining one here, so this test exercises the
// same surface the engine integration tests do.

/// One JSONL record for `load_as` / `ingest_as` exercises.
const ONE_PERSON_JSONL: &str = r#"{"type": "Person", "data": {"name": "Eve"}}"#;

/// Every directory and file under `root`, with file bytes: what a refused
/// call must leave as it found it.
fn physical_tree(root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, Option<Vec<u8>>> {
    fn visit(
        root: &Path,
        path: &Path,
        entries: &mut std::collections::BTreeMap<std::path::PathBuf, Option<Vec<u8>>>,
    ) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            if entry.file_type().unwrap().is_dir() {
                entries.insert(relative, None);
                visit(root, &path, entries);
            } else {
                entries.insert(relative, Some(fs::read(path).unwrap()));
            }
        }
    }
    let mut entries = std::collections::BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

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
    let before = db.list_commits(None).await.unwrap();
    assert_denied(
        db.prepare_schema_apply_as(&desired, Some("act-denied"))
            .await,
        "prepare_schema_apply_as",
    );
    let result = db.apply_schema_as(&desired, Some("act-denied")).await;

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
    assert_eq!(db.list_commits(None).await.unwrap(), before);
}

#[tokio::test]
async fn apply_schema_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let desired = additive_schema();
    let before = db.list_commits(None).await.unwrap();
    let prepared = db
        .prepare_schema_apply_as(&desired, Some("act-allowed"))
        .await
        .unwrap();
    assert_denied(
        db.apply_prepared_schema_as(&prepared, Some("act-denied"))
            .await,
        "apply_prepared_schema_as",
    );
    assert_denied(
        db.reconcile_schema_apply_as(&prepared, Some("act-denied"))
            .await,
        "reconcile_schema_apply_as",
    );

    // Persisted intent is not a policy capability. Reopening under a policy
    // that revokes this actor must refuse even though preparation was allowed.
    let revoked_source = POLICY_YAML
        .replace("writers: [act-allowed]", "writers: [act-other]")
        .replace(
            "readers: [act-denied]",
            "readers: [act-denied, act-allowed]",
        );
    let (revoked, _revoked_engine) = install_policy_source(
        Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap(),
        dir.path(),
        &revoked_source,
    );
    assert_denied(
        revoked
            .apply_prepared_schema_as(&prepared, Some("act-allowed"))
            .await,
        "apply_prepared_schema_as after revocation",
    );
    assert_denied(
        revoked
            .reconcile_schema_apply_as(&prepared, Some("act-allowed"))
            .await,
        "reconcile_schema_apply_as after revocation",
    );
    assert_eq!(db.list_commits(None).await.unwrap(), before);
    assert_eq!(db.schema_source().as_str(), TEST_SCHEMA);
    let result = db
        .apply_prepared_schema_as(&prepared, Some("act-allowed"))
        .await
        .expect("act-allowed should be able to SchemaApply");
    assert!(result.applied);
    assert_eq!(
        result.commit.as_ref().unwrap().actor_id.as_deref(),
        Some("act-allowed")
    );
    assert_eq!(
        result.commit.as_ref().unwrap().parent_commit_id.as_deref(),
        Some(before[0].graph_commit_id.as_str())
    );
    assert!(
        !db.apply_schema_as(&desired, Some("act-allowed"))
            .await
            .unwrap()
            .applied
    );
}

#[tokio::test]
async fn runtime_binding_views_share_engine_authority_and_keep_policy_immutable() {
    let dir = tempfile::tempdir().unwrap();
    let (original, _) = init_with_policy(&dir).await;
    let replacement_source = POLICY_YAML.replace("act-allowed", "act-replacement");
    let policy = Arc::new(
        PolicyEngine::load_graph_from_source(&replacement_source, dir.path().to_str().unwrap())
            .unwrap(),
    );
    let replacement = original
        .with_runtime_bindings(
            Some(policy as Arc<dyn PolicyChecker>),
            None,
            omnigraph::ExternalBlobPolicy::Deny,
        )
        .unwrap();
    assert!(original.shares_runtime_owner(&replacement));
    let before = original.list_commits(None).await.unwrap();
    let desired = additive_schema();
    assert_denied(
        replacement
            .prepare_schema_apply_as(&desired, Some("act-allowed"))
            .await,
        "replacement revokes original actor",
    );
    original
        .prepare_schema_apply_as(&desired, Some("act-allowed"))
        .await
        .unwrap();
    assert_eq!(original.list_commits(None).await.unwrap(), before);
    replacement
        .apply_schema_as(&desired, Some("act-replacement"))
        .await
        .unwrap();
    assert_eq!(
        original.schema_contract_digest(),
        replacement.schema_contract_digest()
    );
    assert_eq!(
        original.list_commits(None).await.unwrap(),
        replacement.list_commits(None).await.unwrap()
    );
    let reopened = Omnigraph::open_read_only(dir.path().to_str().unwrap())
        .await
        .unwrap();
    assert!(!original.shares_runtime_owner(&reopened));
    let unprotected = replacement
        .with_runtime_bindings(None, None, omnigraph::ExternalBlobPolicy::Deny)
        .unwrap();
    assert!(replacement.shares_runtime_owner(&unprotected));
    unprotected
        .prepare_schema_apply_as(&desired, None)
        .await
        .unwrap();
    assert_denied(
        replacement
            .prepare_schema_apply_as(&desired, Some("act-allowed"))
            .await,
        "removing policy from a new view leaves predecessor immutable",
    );
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

/// A Blob put or clear enforces `change` on its branch for the supplied actor
/// before it opens anything, like every other Mutation-protocol writer.
#[tokio::test]
async fn blob_put_and_clear_enforce_change_for_the_actor() {
    const SCHEMA: &str = "node Document { title: String @key content: Blob? }\n";
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    {
        let db = helpers::session(Omnigraph::init(uri, SCHEMA).await.unwrap());
        db.load_jsonl(
            r#"{"type":"Document","data":{"title":"doc"}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    }
    let (db, _engine) = install_policy(Omnigraph::open(uri).await.unwrap(), dir.path());
    let cell = || node_blob_cell("Document", "doc", "content");
    let bytes = || bytes::Bytes::from_static(b"payload");

    assert_denied(
        db.put_blob_at_as("main", cell(), bytes(), None, Some("act-denied"))
            .await,
        "put_blob_at_as",
    );
    assert_denied(
        db.clear_blob_at_as("main", cell(), None, Some("act-denied"))
            .await,
        "clear_blob_at_as",
    );
    let no_actor = db.put_blob_at_as("main", cell(), bytes(), None, None).await;
    assert!(
        matches!(no_actor, Err(OmniError::Policy(_))),
        "an installed policy refuses a write without an actor: {no_actor:?}"
    );
    db.put_blob_at_as("main", cell(), bytes(), None, Some("act-allowed"))
        .await
        .expect("act-allowed may Change on main");
    db.clear_blob_at_as("main", cell(), None, Some("act-allowed"))
        .await
        .expect("act-allowed may Change on main");
    assert_non_canonical_branch(
        db.put_blob_at_as(" main", cell(), bytes(), None, Some("act-allowed"))
            .await,
        "put_blob_at_as on a padded branch name",
    );
    assert_non_canonical_branch(
        db.clear_blob_at_as("main ", cell(), None, Some("act-allowed"))
            .await,
        "clear_blob_at_as on a padded branch name",
    );
}

#[tokio::test]
async fn load_as_denies_when_policy_rejects_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    let result = db
        .load_as(
            "main",
            None,
            ONE_PERSON_JSONL,
            LoadMode::Merge,
            Some("act-denied"),
        )
        .await;
    assert_denied(result, "load_as");
}

#[tokio::test]
async fn load_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    db.load_as(
        "main",
        None,
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
            None,
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
        None,
        data_path.to_str().unwrap(),
        LoadMode::Merge,
        Some("act-allowed"),
    )
    .await
    .expect("act-allowed should be able to load_file_as on main");
}

#[tokio::test]
#[allow(deprecated)]
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
#[allow(deprecated)]
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

    // A commit start is checked on the branch that wrote the commit, and a
    // denial is the not-found of an unknown id: the id discloses nothing.
    let head = db.list_commits(Some("main")).await.unwrap()[0]
        .graph_commit_id
        .clone();
    let result = db
        .branch_create_from_as(
            ReadTarget::snapshot(SnapshotId::new(head)),
            "feature",
            Some("act-denied"),
        )
        .await;
    assert!(
        matches!(&result, Err(OmniError::Manifest(error)) if error.kind == ManifestErrorKind::NotFound),
        "{result:?}"
    );
    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);
}

#[tokio::test]
async fn branch_create_from_as_allows_when_policy_permits_actor() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _engine) = init_with_policy(&dir).await;

    db.branch_create_from_as(ReadTarget::branch("main"), "feature", Some("act-allowed"))
        .await
        .expect("act-allowed should be able to BranchCreate from main");

    let head = db.list_commits(Some("main")).await.unwrap()[0]
        .graph_commit_id
        .clone();
    let created = db
        .branch_create_from_as(
            ReadTarget::snapshot(SnapshotId::new(head.clone())),
            "at-head",
            Some("act-allowed"),
        )
        .await
        .expect("act-allowed should be able to BranchCreate at a commit of main");
    assert_eq!(created.from_commit_id, head);
    assert_eq!(created.from, "main");
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

fn assert_non_canonical_branch(result: Result<impl std::fmt::Debug, OmniError>, what: &str) {
    match result {
        Err(OmniError::Manifest(error))
            if error.kind == ManifestErrorKind::BadRequest
                && (error.message.contains("leading or trailing whitespace")
                    || error.message.contains("contains a control character")) => {}
        other => panic!("{what}: expected a non-canonical branch name refusal, got {other:?}"),
    }
}

/// The head commit of each protected branch and the live branch names: what a
/// refused writer must leave as it found them.
async fn protected_state(db: &Session) -> (String, String, Vec<String>) {
    let head = |commits: Vec<omnigraph::db::GraphCommit>| commits[0].graph_commit_id.clone();
    let mut branches = db.branch_list().await.unwrap();
    branches.sort();
    (
        head(db.list_commits(None).await.unwrap()),
        head(db.list_commits(Some("release")).await.unwrap()),
        branches,
    )
}

/// The policy judges a branch name as written, and the engine acts on the
/// name it is given. An actor allowed to write and control only unprotected
/// branches therefore cannot reach a protected one through a spelling the
/// engine could read as it: every writer refuses the spelling as a typed bad
/// request before policy or effects, and both protected heads stay put.
#[tokio::test]
async fn non_canonical_branch_names_cannot_reach_a_protected_branch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    {
        let db = init_and_load(&dir).await;
        db.branch_create("feature").await.unwrap();
        db.branch_create("release").await.unwrap();
        let params = mixed_params(&[("$name", "Feature")], &[("$age", 30)]);
        db.mutate("feature", MUTATION_QUERIES, "insert_person", &params)
            .await
            .unwrap();
    }
    let policy = POLICY_YAML
        .replace(
            "protected_branches: [main]",
            "protected_branches: [main, release]",
        )
        .replace("      branch_scope: any", "      branch_scope: unprotected")
        .replace(
            "      target_branch_scope: any",
            "      target_branch_scope: unprotected",
        );
    let (db, _engine) =
        install_policy_source(Omnigraph::open(&uri).await.unwrap(), dir.path(), &policy);
    let actor = Some("act-allowed");
    let params = mixed_params(&[("$name", "Eve")], &[("$age", 22)]);
    // The canonical spellings are judged as the policy says.
    assert_denied(
        db.mutate_as("main", MUTATION_QUERIES, "insert_person", &params, actor)
            .await,
        "mutate_as on protected main",
    );
    assert_denied(
        db.branch_merge_as("feature", "main", actor).await,
        "branch_merge_as into protected main",
    );
    assert_denied(
        db.branch_delete_as("release", actor).await,
        "branch_delete_as of protected release",
    );
    let before = protected_state(&db).await;

    let check =
        |what: String, result: Result<(), OmniError>, after: (String, String, Vec<String>)| {
            assert_eq!(after, before, "{what} reached a protected branch");
            assert_non_canonical_branch(result, &what);
        };
    for main in [" main", "main ", " main ", "\tmain", "main\u{3000}"] {
        let result = db
            .mutate_as(main, MUTATION_QUERIES, "insert_person", &params, actor)
            .await
            .map(drop);
        check(
            format!("mutate_as({main:?})"),
            result,
            protected_state(&db).await,
        );
        let result = db
            .load_as(main, None, ONE_PERSON_JSONL, LoadMode::Merge, actor)
            .await
            .map(drop);
        check(
            format!("load_as({main:?})"),
            result,
            protected_state(&db).await,
        );
        let result = db.branch_merge_as("feature", main, actor).await.map(drop);
        check(
            format!("branch_merge_as(feature -> {main:?})"),
            result,
            protected_state(&db).await,
        );
        let result = db
            .rebuild_full_text_indices_on_as(main, actor)
            .await
            .map(drop);
        check(
            format!("rebuild_full_text_indices_on_as({main:?})"),
            result,
            protected_state(&db).await,
        );
        let result = db
            .load_as(
                "fresh",
                Some(main),
                ONE_PERSON_JSONL,
                LoadMode::Merge,
                actor,
            )
            .await
            .map(drop);
        check(
            format!("load_as(fresh from {main:?})"),
            result,
            protected_state(&db).await,
        );
        let result = db
            .branch_create_from_as(ReadTarget::branch(main), "fresh", actor)
            .await
            .map(drop);
        check(
            format!("branch_create_from_as({main:?} -> fresh)"),
            result,
            protected_state(&db).await,
        );
    }
    for release in [" release", "release "] {
        let result = db.branch_delete_as(release, actor).await;
        check(
            format!("branch_delete_as({release:?})"),
            result,
            protected_state(&db).await,
        );
        let result = db.branch_create_as(release, actor).await;
        check(
            format!("branch_create_as({release:?})"),
            result,
            protected_state(&db).await,
        );
    }
}

#[tokio::test]
async fn full_text_rebuild_enforces_change_or_maintain_on_the_selected_branch_and_records_actor() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    drop(db);
    // `act-allowed` can change unprotected feature, `act-maintainer` can
    // maintain it, and neither reaches protected main. A hardcoded main scope
    // or an action other than `change` or `maintain` must fail.
    let policy = format!(
        "{}  - id: maintainers\n    allow:\n      actors: {{ group: maintainers }}\n      actions: [maintain]\n      branch_scope: unprotected\n",
        POLICY_YAML
            .replace("      branch_scope: any", "      branch_scope: unprotected")
            .replace(
                "  readers: [act-denied]\n",
                "  readers: [act-denied]\n  maintainers: [act-maintainer]\n",
            )
    );
    let db = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    let (db, _engine) = install_policy_source(db, dir.path(), &policy);
    let main_before = db.list_commits(None).await.unwrap();
    let feature_before = db.list_commits(Some("feature")).await.unwrap();
    let files_before = physical_tree(dir.path());

    for (branch, actor) in [
        ("feature", "act-denied"),
        ("main", "act-allowed"),
        ("main", "act-maintainer"),
    ] {
        let result = db
            .rebuild_full_text_indices_on_as(branch, Some(actor))
            .await;
        // Either grant would admit the rebuild, so the refusal names both.
        assert!(
            matches!(&result, Err(OmniError::Policy(message))
                if message.contains("'maintain'") && message.contains("'change'")),
            "{actor} on {branch}: {result:?}"
        );
        assert_denied(result, "rebuild_full_text_indices_on_as");
        assert_eq!(
            physical_tree(dir.path()),
            files_before,
            "policy refusal must not write index artifacts, native refs, sidecars, or publications"
        );
    }
    let error = db
        .rebuild_full_text_indices_on_as("feature", None)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, OmniError::Policy(message) if message.contains("no actor")),
        "{error}"
    );
    assert_eq!(
        physical_tree(dir.path()),
        files_before,
        "a missing actor must not bypass policy"
    );

    let mut parent = feature_before[0].graph_commit_id.clone();
    for (actor, grant) in [("act-allowed", "change"), ("act-maintainer", "maintain")] {
        let result = db
            .rebuild_full_text_indices_on_as("feature", Some(actor))
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{grant} on the selected unprotected branch must permit a real rebuild: {error}"
                )
            });
        assert_eq!(result.branch, "feature");
        assert_eq!(result.rebuilt_indexes.len(), 2);
        let feature_after = db.list_commits(Some("feature")).await.unwrap();
        assert_eq!(
            result.graph_commit_id.as_deref(),
            Some(feature_after[0].graph_commit_id.as_str())
        );
        assert_eq!(feature_after[0].actor_id.as_deref(), Some(actor));
        assert_eq!(
            feature_after[0].parent_commit_id.as_deref(),
            Some(parent.as_str())
        );
        parent = feature_after[0].graph_commit_id.clone();
    }
    assert_eq!(
        db.list_commits(Some("feature")).await.unwrap().len(),
        feature_before.len() + 2
    );
    let main_after = db.list_commits(None).await.unwrap();
    assert_eq!(main_after.len(), main_before.len());
    assert_eq!(
        main_after[0].graph_commit_id,
        main_before[0].graph_commit_id
    );
}

/// Maintenance grants, apart from data writes: `act-maintainer` may maintain
/// any branch and `act-branch-maintainer` only unprotected ones;
/// `act-reclaimer` may reclaim; `act-writer` holds `change` everywhere and no
/// maintenance grant; `act-denied` holds nothing.
const MAINTENANCE_POLICY_YAML: &str = r#"
version: 1
groups:
  maintainers: [act-maintainer]
  branch-maintainers: [act-branch-maintainer]
  reclaimers: [act-reclaimer]
  writers: [act-writer]
  readers: [act-denied]
protected_branches: [main]
rules:
  - id: maintainers
    allow:
      actors: { group: maintainers }
      actions: [maintain]
      branch_scope: any
  - id: branch-maintainers
    allow:
      actors: { group: branch-maintainers }
      actions: [maintain]
      branch_scope: unprotected
  - id: reclaimers
    allow:
      actors: { group: reclaimers }
      actions: [reclaim]
  - id: writers
    allow:
      actors: { group: writers }
      actions: [change]
      branch_scope: any
"#;

/// A graph with a `feature` branch and two extra Person fragments on main, so
/// optimize has a compaction to publish, under [`MAINTENANCE_POLICY_YAML`].
async fn init_with_maintenance_policy(dir: &tempfile::TempDir) -> Session {
    let db = init_and_load(dir).await;
    db.branch_create("feature").await.unwrap();
    for (name, age) in [("Eve", 40), ("Frank", 41)] {
        let params = mixed_params(&[("$name", name)], &[("$age", age)]);
        db.mutate("main", MUTATION_QUERIES, "insert_person", &params)
            .await
            .unwrap();
    }
    drop(db);
    let db = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    install_policy_source(db, dir.path(), MAINTENANCE_POLICY_YAML).0
}

fn assert_no_actor(result: Result<impl std::fmt::Debug, OmniError>, what: &str) {
    match result {
        Err(OmniError::Policy(message)) if message.contains("no actor") => {}
        other => panic!("{what}: expected the no-actor refusal, got {other:?}"),
    }
}

/// Optimize, index builds and the repair report require `maintain` on the
/// branch they maintain, before any effect: `change` does not imply it, a
/// grant on unprotected branches does not reach main, and a missing actor is
/// refused. An allowed optimize records its actor on the commit it publishes.
#[tokio::test]
async fn maintenance_entry_points_enforce_maintain_on_their_branch_before_effects() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_with_maintenance_policy(&dir).await;
    let main_before = db.list_commits(None).await.unwrap();
    let files_before = physical_tree(dir.path());

    for actor in [
        "act-denied",
        "act-writer",
        "act-reclaimer",
        "act-branch-maintainer",
    ] {
        let actor = Some(actor);
        assert_denied(db.optimize_as(actor).await, "optimize_as");
        assert_denied(db.ensure_indices_as(actor).await, "ensure_indices_as");
        assert_denied(
            db.ensure_indices_on_as("main", actor).await,
            "ensure_indices_on_as(main)",
        );
        assert_denied(db.repair_report_as(actor).await, "repair_report_as");
    }
    for actor in ["act-denied", "act-writer", "act-reclaimer"] {
        assert_denied(
            db.ensure_indices_on_as("feature", Some(actor)).await,
            "ensure_indices_on_as(feature)",
        );
    }
    assert_no_actor(db.optimize_as(None).await, "optimize_as");
    assert_no_actor(db.ensure_indices_as(None).await, "ensure_indices_as");
    assert_no_actor(
        db.ensure_indices_on_as("feature", None).await,
        "ensure_indices_on_as",
    );
    assert_no_actor(db.repair_report_as(None).await, "repair_report_as");
    assert_eq!(
        physical_tree(dir.path()),
        files_before,
        "a refused maintenance call must not write index artifacts, compactions or publications"
    );
    assert_eq!(db.list_commits(None).await.unwrap(), main_before);

    db.ensure_indices_on_as("feature", Some("act-branch-maintainer"))
        .await
        .expect("maintain on an unprotected branch admits its index builds");
    let report = db
        .repair_report_as(Some("act-maintainer"))
        .await
        .expect("maintain on main admits the repair report");
    assert!(!report.datasets.is_empty());
    db.ensure_indices_as(Some("act-maintainer"))
        .await
        .expect("maintain on main admits index builds");

    let head_before_optimize = db.list_commits(None).await.unwrap()[0]
        .graph_commit_id
        .clone();
    let stats = db
        .optimize_as(Some("act-maintainer"))
        .await
        .expect("maintain on main admits optimize");
    assert!(
        stats
            .iter()
            .any(|stat| stat.type_key == "node:Person" && stat.committed),
        "{stats:?}"
    );
    let main_after = db.list_commits(None).await.unwrap();
    assert_eq!(
        main_after[0].parent_commit_id.as_deref(),
        Some(head_before_optimize.as_str())
    );
    assert_eq!(main_after[0].actor_id.as_deref(), Some("act-maintainer"));
}

/// Cleanup requires `reclaim`, which no other grant implies, before any
/// effect.
#[tokio::test]
async fn cleanup_as_enforces_reclaim_before_effects() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_with_maintenance_policy(&dir).await;
    let keep_one = || omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    };
    let files_before = physical_tree(dir.path());
    for actor in ["act-denied", "act-writer", "act-maintainer"] {
        assert_denied(db.cleanup_as(keep_one(), Some(actor)).await, "cleanup_as");
    }
    assert_no_actor(db.cleanup_as(keep_one(), None).await, "cleanup_as");
    assert_eq!(
        physical_tree(dir.path()),
        files_before,
        "a refused cleanup must not delete anything"
    );

    let rows = db
        .cleanup_as(keep_one(), Some("act-reclaimer"))
        .await
        .expect("reclaim admits cleanup");
    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
    assert_ne!(
        physical_tree(dir.path()),
        files_before,
        "the admitted cleanup reclaims the history keep 1 does not retain"
    );
}

#[tokio::test]
async fn schema_settlement_authorizes_current_executor_and_preserves_original_authorship() {
    use omnigraph::db::{SchemaApplySettlement, SchemaNonPublicationProof};
    for original_wins in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (db, _) = init_with_policy(&dir).await;
        let original = db
            .prepare_schema_apply_as(&additive_schema(), Some("act-allowed"))
            .await
            .unwrap();
        let fence = db
            .prepare_schema_settlement_as(&original, Some("act-allowed"))
            .await
            .unwrap();
        assert_denied(
            db.prepare_schema_settlement_as(&original, Some("act-denied"))
                .await,
            "prepare_schema_settlement_as",
        );
        assert_denied(
            db.settle_prepared_schema_as(&original, &fence, Some("act-denied"))
                .await,
            "settle_prepared_schema_as",
        );
        if original_wins {
            db.apply_prepared_schema_as(&original, Some("act-allowed"))
                .await
                .unwrap();
        }
        let source = POLICY_YAML
            .replace("writers: [act-allowed]", "writers: [act-other]")
            .replace(
                "readers: [act-denied]",
                "readers: [act-denied, act-allowed]",
            );
        let (recovery, _) = install_policy_source(
            Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap(),
            dir.path(),
            &source,
        );
        let before = recovery.list_commits(None).await.unwrap();
        assert_denied(
            recovery
                .settle_prepared_schema_as(&original, &fence, Some("act-allowed"))
                .await,
            "authored fence is not a capability after revocation",
        );
        assert_eq!(recovery.list_commits(None).await.unwrap(), before);
        let result = recovery
            .settle_prepared_schema_as(&original, &fence, Some("act-other"))
            .await
            .unwrap();
        let commit = match result {
            SchemaApplySettlement::Committed { commit, .. } if original_wins => commit,
            SchemaApplySettlement::NotPublished {
                proof: SchemaNonPublicationProof::Fence { commit, .. },
            } if !original_wins => commit,
            other => panic!("wrong authorized settlement: {other:?}"),
        };
        assert_eq!(
            commit.actor_id.as_deref(),
            Some("act-allowed"),
            "adoption must not rewrite authored lineage"
        );
        assert_eq!(
            commit.graph_manifest_version,
            original.base_manifest_version() + 1
        );
    }
}
