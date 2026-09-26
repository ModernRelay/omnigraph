//! What is left of open-time recovery after RFC 0067.
//!
//! No writer arms a recovery sidecar: table effects are detached commits
//! published as pins, and a staged schema contract names the graph commit that
//! publishes it. These tests pin the remaining contracts of the `__recovery/`
//! directory and of the read-only open: a clean open creates nothing, a
//! sidecar left by a build that predates detached commits refuses a read-write
//! open while reads keep working, index maintenance writes no sidecar, and a
//! read-only open never touches schema staging.

use std::path::Path;

use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;

mod helpers;
use helpers::snapshot_main;

const TEST_SCHEMA: &str = include_str!("fixtures/test.pg");

fn list_recovery_dir(graph_root: &Path) -> Vec<String> {
    let dir = graph_root.join("__recovery");
    if !dir.exists() {
        return Vec::new();
    }
    std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|d| d.file_name().to_string_lossy().to_string()))
        .collect()
}

#[tokio::test]
async fn recovery_does_not_run_on_clean_open() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    drop(_db);

    // Reopen: `__recovery/` does not exist and nothing creates it.
    let _db = Omnigraph::open(uri).await.unwrap();
    assert!(
        !dir.path().join("__recovery").exists(),
        "a clean open must not create __recovery/"
    );
}

/// A sidecar can only come from a build that predates detached table commits.
/// This build cannot interpret one: a read-write open refuses, naming it,
/// until the build that wrote it has resolved it. Reads stay pinned to
/// published manifest versions, so a read-only open serves the graph.
#[tokio::test]
async fn legacy_sidecar_refuses_a_read_write_open_and_not_a_read_only_one() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = helpers::init_and_load(&dir).await;
    let rows = helpers::count_rows(&db, "node:Person").await;
    drop(db);

    let recovery = dir.path().join("__recovery");
    std::fs::create_dir(&recovery).unwrap();
    std::fs::write(recovery.join("01LEGACYSIDECAR.json"), "{}").unwrap();

    let read_only = Omnigraph::open_read_only(uri)
        .await
        .expect("a read-only open never looks at __recovery/");
    assert_eq!(helpers::count_rows(&read_only, "node:Person").await, rows);
    drop(read_only);

    let error = Omnigraph::open(uri)
        .await
        .err()
        .expect("a read-write open must refuse a sidecar it cannot interpret");
    assert!(
        matches!(&error, OmniError::RecoveryRequired { operation_id, .. } if operation_id == "01LEGACYSIDECAR"),
        "{error}"
    );
    assert!(
        recovery.join("01LEGACYSIDECAR.json").exists(),
        "the refusal leaves the sidecar for the build that wrote it"
    );

    std::fs::remove_file(recovery.join("01LEGACYSIDECAR.json")).unwrap();
    let db = Omnigraph::open(uri)
        .await
        .expect("the graph opens once the sidecar is resolved");
    assert_eq!(helpers::count_rows(&db, "node:Person").await, rows);
}

/// `ensure_indices` must only touch tables that actually need new index
/// work. Steady state: when nothing needs indexing, the pass is a pure
/// no-op — it publishes no graph commit, republishes no table pin, and
/// (RFC 0067) leaves no `__recovery/` residue. The sibling test
/// `recovery_ensure_indices_handles_empty_tables` covers the empty-table
/// case, where the `if row_count > 0 { build_indices(...) }` guard means
/// empty tables produce zero commits.
#[tokio::test]
async fn recovery_ensure_indices_steady_state_no_sidecar() {
    use omnigraph::loader::LoadMode;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let db = helpers::session(Omnigraph::init(uri, TEST_SCHEMA).await.unwrap());
    let test_data = r#"{"type":"Person","data":{"name":"alice","age":30}}
{"type":"Company","data":{"name":"acme"}}
"#;
    db.load_jsonl(test_data, LoadMode::Append).await.unwrap();
    db.ensure_indices().await.unwrap();
    drop(db);

    let db = Omnigraph::open(uri).await.unwrap();
    let before = snapshot_main(&db).await.unwrap();
    db.ensure_indices().await.unwrap();
    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before.graph_manifest_version()
    );
    for key in ["node:Person", "node:Company", "edge:Knows", "edge:WorksAt"] {
        assert_eq!(
            after.dataset(key).unwrap().published_dataset_version,
            before.dataset(key).unwrap().published_dataset_version
        );
    }
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "steady-state ensure_indices must not leave a sidecar (no tables need work)"
    );
}

/// Empty tables (zero rows) bypass `build_indices_on_dataset` because
/// `ensure_indices_for_branch` has `if row_count > 0 { build_indices(...) }`.
/// The `needs_index_work_*` helpers must match this — pinning an empty
/// table means recovery classifies it as `NoMovement` (no commits ever
/// ran) and rolls back any sibling table's legitimate index work.
///
/// Integration verification: after a real init + ensure_indices on a
/// graph where every table is empty, the recovery sweep must complete
/// cleanly (no leftover sidecar) AND the next ensure_indices must also
/// leave no sidecar — proving the empty-table-scoping behavior lets
/// steady-state runs incur zero sidecar I/O. The
/// `count_rows == 0 → return false` short-circuit in `needs_index_work_*`
/// is what makes this work.
///
/// A stronger assertion that captured the sidecar after arming but before the
/// first index effect could inspect the persisted pin set directly. That needs
/// a dedicated pre-effect EnsureIndices rendezvous; the current failpoint is
/// after its effects, so this remains an end-to-end behavioral assertion.
#[tokio::test]
async fn recovery_ensure_indices_handles_empty_tables() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    // Don't load any data — every table is empty.
    let before = snapshot_main(&db).await.unwrap();
    db.ensure_indices().await.unwrap();
    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before.graph_manifest_version()
    );
    for key in ["node:Person", "node:Company", "edge:Knows", "edge:WorksAt"] {
        assert_eq!(
            after.dataset(key).unwrap().published_dataset_version,
            before.dataset(key).unwrap().published_dataset_version
        );
    }
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "ensure_indices on an all-empty graph must not leave a sidecar"
    );
    // Reopen + ensure_indices — still steady state, still no sidecar.
    drop(db);
    let db = Omnigraph::open(uri).await.unwrap();
    let before = snapshot_main(&db).await.unwrap();
    db.ensure_indices().await.unwrap();
    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before.graph_manifest_version()
    );
    for key in ["node:Person", "node:Company", "edge:Knows", "edge:WorksAt"] {
        assert_eq!(
            after.dataset(key).unwrap().published_dataset_version,
            before.dataset(key).unwrap().published_dataset_version
        );
    }
    assert!(
        list_recovery_dir(dir.path()).is_empty(),
        "second ensure_indices on an all-empty graph must also not leave a sidecar"
    );
}

/// `OpenMode::ReadOnly` must NOT run `recover_schema_state_files`,
/// which can delete or rename schema-staging files. Read-only consumers
/// may run with read-only object-store credentials, and silent open-time
/// mutations violate the contract.
///
/// This test drops a schema-staging file (which the recovery sweep
/// would normally delete) then opens with ReadOnly mode. The staging
/// file must remain untouched.
#[tokio::test]
async fn read_only_open_skips_schema_state_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let _ = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Drop a leftover schema-staging file. The schema-state recovery
    // sweep would normally tidy this on open (either delete or rename
    // depending on whether it matches the live schema). ReadOnly must
    // skip that work.
    let staging_path = dir.path().join("_schema.pg.staging");
    std::fs::write(&staging_path, "node Person { name: String @key }\n").unwrap();
    assert!(staging_path.exists());

    let _db = Omnigraph::open_read_only(uri).await.unwrap();

    // Staging file must be untouched.
    assert!(
        staging_path.exists(),
        "ReadOnly open must not delete schema-staging files (no object-store mutations)"
    );
    let content = std::fs::read_to_string(&staging_path).unwrap();
    assert_eq!(
        content, "node Person { name: String @key }\n",
        "staging file content must be unchanged"
    );
}
