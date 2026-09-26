//! Helpers for the detached-only collector's fixtures (RFC "Detached-only
//! tables", `blocked_on` 2). `cleanup_plan` is report-only, so the fixtures
//! assert the plan and the counts it puts on the `cleanup` rows, never a
//! deletion of the plan's own.

use std::collections::{BTreeMap, BTreeSet};

use omnigraph::Session;
use omnigraph::db::{
    CleanupPolicyOptions, CollectorReport, Omnigraph, ReadTarget, RetainedManifestVersions,
    TableCollectionPlan,
};
use omnigraph::loader::LoadMode;

use super::{
    MUTATION_QUERIES, count_rows_branch, mixed_params, open_dataset_head_exact, params, session,
};

pub const KEY_SCORE_SCHEMA: &str = r#"
node Person {
    name: String @key
    score: I32
}
"#;

const SCORE_MUTATIONS: &str = r#"
query insert_scored($name: String) {
    insert Person { name: $name, score: 2 }
}
"#;

pub fn keep_one() -> CleanupPolicyOptions {
    CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    }
}

/// Filesystem uri of a node table's main-branch incarnation, from its
/// registration (physical paths are identity-derived).
pub async fn table_uri(db: &Omnigraph, type_name: &str) -> String {
    let table_key = format!("node:{type_name}");
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table_path = &snapshot
        .dataset(&table_key)
        .unwrap_or_else(|| panic!("live manifest has no registration for {table_key}"))
        .dataset_path;
    format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        table_path.trim_start_matches('/')
    )
}

pub async fn detached_versions(table_uri: &str) -> BTreeSet<u64> {
    open_dataset_head_exact(table_uri, None)
        .await
        .list_detached_manifests()
        .await
        .unwrap()
        .into_iter()
        .map(|manifest| manifest.version)
        .collect()
}

/// The one detached manifest a write added under `table_uri` since `before`.
pub async fn staged_since(table_uri: &str, before: &BTreeSet<u64>) -> u64 {
    let added: Vec<u64> = (&detached_versions(table_uri).await - before)
        .into_iter()
        .collect();
    assert_eq!(
        added.len(),
        1,
        "one write stages one detached manifest, found {added:?}"
    );
    added[0]
}

/// One Person insert on `branch` under the test schema (`name`, `age`).
pub async fn insert_person(db: &Session, branch: &str, name: &str) {
    db.mutate(
        branch,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", name)], &[("$age", 30)]),
    )
    .await
    .expect("insert");
}

/// One Person insert on main under [`KEY_SCORE_SCHEMA`].
pub async fn insert_scored(db: &Session, name: &str) {
    db.mutate(
        "main",
        SCORE_MUTATIONS,
        "insert_scored",
        &params(&[("$name", name)]),
    )
    .await
    .expect("insert");
}

/// The plan of `table_key` at its main location (forks trace separately).
pub fn main_plan<'a>(report: &'a CollectorReport, table_key: &str) -> &'a TableCollectionPlan {
    report
        .tables
        .iter()
        .find(|plan| plan.table_key == table_key && plan.location == plan.full_path)
        .unwrap_or_else(|| {
            panic!(
                "no plan for {table_key} at its main location among {:?}",
                report
                    .tables
                    .iter()
                    .map(|plan| (&plan.table_key, &plan.location))
                    .collect::<Vec<_>>()
            )
        })
}

pub fn retained_on<'a>(
    report: &'a CollectorReport,
    branch: Option<&str>,
) -> &'a RetainedManifestVersions {
    report
        .branches
        .iter()
        .find(|row| row.branch.as_deref() == branch)
        .unwrap_or_else(|| {
            panic!(
                "{branch:?} is not among the live branches: {:?}",
                report.branches
            )
        })
}

/// Merge a branch whose Person delta spans three keyed-write chunks, so the
/// merge stages a three-link detached chain on main's Person location with
/// only its tip pinned; the branch is deleted after the merge, since its own
/// registrations would keep every link a root while it lives. Returns the
/// session, the Person uri and the chain, oldest link first.
pub async fn merge_three_chunk_chain(dir: &tempfile::TempDir) -> (Session, String, Vec<u64>) {
    const CHUNK_ROWS: usize = 8192;
    let uri = dir.path().to_str().unwrap().to_string();
    let db = session(Omnigraph::init(&uri, KEY_SCORE_SCHEMA).await.unwrap());
    db.load_jsonl(
        r#"{"type":"Person","data":{"name":"base","score":0}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();
    let person_uri = table_uri(&db, "Person").await;
    let before_loads = detached_versions(&person_uri).await;
    for (first, count) in [
        (0, CHUNK_ROWS),
        (CHUNK_ROWS, CHUNK_ROWS),
        (2 * CHUNK_ROWS, 1),
    ] {
        let mut rows = String::with_capacity(count * 70);
        for row in first..first + count {
            rows.push_str(&format!(
                "{{\"type\":\"Person\",\"data\":{{\"name\":\"merge-row-{row}\",\"score\":1}}}}\n"
            ));
        }
        db.load("feature", &rows, LoadMode::Append).await.unwrap();
    }
    assert_eq!(
        count_rows_branch(&db, "feature", "node:Person").await,
        2 * CHUNK_ROWS + 2,
        "three loads under the loader's keyed-entity limit; the merge chunks the delta by the same bound"
    );
    db.branch_merge("feature", "main").await.unwrap();
    let before = before_loads;
    let chunks = &detached_versions(&person_uri).await - &before;
    assert_eq!(
        chunks.len(),
        3,
        "three detached commits form the chain: the branch's three loads on main's location, adopted by the merge: {chunks:?}"
    );
    db.branch_delete("feature").await.unwrap();

    let raw = open_dataset_head_exact(&person_uri, None).await;
    let mut link_of = BTreeMap::new();
    for version in &chunks {
        let transaction = raw
            .checkout_version(*version)
            .await
            .unwrap()
            .read_transaction()
            .await
            .unwrap()
            .expect("a detached commit records its transaction");
        link_of.insert(*version, transaction.read_version);
    }
    let mut next = chunks
        .iter()
        .copied()
        .find(|version| !chunks.contains(&link_of[version]))
        .expect("the oldest link is staged from outside the chain");
    let mut chain = vec![next];
    while let Some(successor) = chunks
        .iter()
        .copied()
        .find(|version| link_of[version] == next)
    {
        chain.push(successor);
        next = successor;
    }
    assert_eq!(chain.len(), 3, "the three chunks link into one chain");
    (db, person_uri, chain)
}
