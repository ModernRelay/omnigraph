// Cross-path validator wire-up tests: each validator (enum, intra-batch
// unique, range, edge cardinality) must reject invalid data on every write
// path — JSONL load, mutation insert (node + edge where applicable),
// mutation update.

mod helpers;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::{mutate_main, params};

const ENUM_SCHEMA: &str = r#"
node Person {
    name: String @key
    role: enum(admin, guest, member)
}
"#;

const ENUM_VALID_SEED: &str = r#"{"type":"Person","data":{"name":"Alice","role":"admin"}}"#;

const ENUM_MUTATIONS: &str = r#"
query insert_person($name: String, $role: String) {
    insert Person { name: $name, role: $role }
}

query set_role($name: String, $role: String) {
    update Person set { role: $role } where name = $name
}
"#;

const RANGE_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
    @range(age, 0..120)
}
"#;

const RANGE_MUTATIONS: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
"#;

const UNIQUE_SCHEMA: &str = r#"
node User {
    name: String @key
    email: String?
    @unique(email)
}
"#;

const CARDINALITY_SCHEMA: &str = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company @card(0..1)
"#;

const CARDINALITY_SEED: &str = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}
{"type":"Company","data":{"name":"Beta"}}"#;

const CARDINALITY_MUTATIONS: &str = r#"
query add_employment($person: String, $company: String) {
    insert WorksAt { from: $person, to: $company }
}
"#;

async fn init_with(schema: &str, data: &str) -> (tempfile::TempDir, Omnigraph) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    if !data.is_empty() {
        load_jsonl(&mut db, data, LoadMode::Overwrite)
            .await
            .unwrap();
    }
    (dir, db)
}

// ─── Enum validation ─────────────────────────────────────────────────────────

#[tokio::test]
async fn enum_rejected_on_jsonl_load() {
    let (_dir, mut db) = init_with(ENUM_SCHEMA, "").await;
    let bad = r#"{"type":"Person","data":{"name":"Alice","role":"superadmin"}}"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Overwrite)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("invalid enum value 'superadmin'"),
        "got: {}",
        err
    );
}

#[tokio::test]
async fn enum_rejected_on_mutation_insert() {
    let (_dir, mut db) = init_with(ENUM_SCHEMA, ENUM_VALID_SEED).await;
    let err = mutate_main(
        &mut db,
        ENUM_MUTATIONS,
        "insert_person",
        &params(&[("$name", "Bob"), ("$role", "superadmin")]),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("invalid enum value 'superadmin'"),
        "got: {}",
        err
    );
}

#[tokio::test]
async fn enum_rejected_on_mutation_update() {
    let (_dir, mut db) = init_with(ENUM_SCHEMA, ENUM_VALID_SEED).await;
    let err = mutate_main(
        &mut db,
        ENUM_MUTATIONS,
        "set_role",
        &params(&[("$name", "Alice"), ("$role", "superadmin")]),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("invalid enum value 'superadmin'"),
        "got: {}",
        err
    );
}

// ─── Range validation ────────────────────────────────────────────────────────

#[tokio::test]
async fn range_rejected_on_jsonl_load() {
    let (_dir, mut db) = init_with(RANGE_SCHEMA, "").await;
    let bad = r#"{"type":"Person","data":{"name":"Alice","age":250}}"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Overwrite)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("@range violation"), "got: {}", err);
}

#[tokio::test]
async fn range_rejected_on_mutation_insert() {
    let (_dir, mut db) = init_with(
        RANGE_SCHEMA,
        r#"{"type":"Person","data":{"name":"Alice","age":30}}"#,
    )
    .await;
    let err = mutate_main(
        &mut db,
        RANGE_MUTATIONS,
        "insert_person",
        &helpers::mixed_params(&[("$name", "Bob")], &[("$age", 250)]),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("@range violation"), "got: {}", err);
}

#[tokio::test]
async fn range_rejected_on_mutation_update() {
    let (_dir, mut db) = init_with(
        RANGE_SCHEMA,
        r#"{"type":"Person","data":{"name":"Alice","age":30}}"#,
    )
    .await;
    let err = mutate_main(
        &mut db,
        RANGE_MUTATIONS,
        "set_age",
        &helpers::mixed_params(&[("$name", "Alice")], &[("$age", 250)]),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("@range violation"), "got: {}", err);
}

// ─── Intra-batch unique validation ───────────────────────────────────────────

#[tokio::test]
async fn intra_batch_unique_rejected_on_jsonl_load() {
    let (_dir, mut db) = init_with(UNIQUE_SCHEMA, "").await;
    let bad = r#"{"type":"User","data":{"name":"Alice","email":"dup@example.com"}}
{"type":"User","data":{"name":"Bob","email":"dup@example.com"}}"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Overwrite)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("@unique violation on User.email"),
        "got: {}",
        err
    );
}

// Note: single-row mutation insert can't violate intra-batch uniqueness
// (only one row in the batch). Cross-batch uniqueness against committed rows
// is out of scope for this wire-up — see the unified write-validator effort.

// ─── Edge cardinality ────────────────────────────────────────────────────────

#[tokio::test]
async fn cardinality_rejected_on_mutation_insert_edge() {
    let (_dir, mut db) = init_with(CARDINALITY_SCHEMA, CARDINALITY_SEED).await;

    // First WorksAt edge — within @card(0..1).
    mutate_main(
        &mut db,
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Acme")]),
    )
    .await
    .unwrap();

    // Second WorksAt for the same source — exceeds max=1.
    let err = mutate_main(
        &mut db,
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Beta")]),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("cardinality")
            || err.to_string().to_lowercase().contains("@card"),
        "got: {}",
        err
    );
}

#[tokio::test]
async fn cardinality_rejected_on_jsonl_load() {
    // Already covered by existing loader Phase 3 logic but assert the
    // same error surface as the mutation path so a regression is caught
    // even if only one path changes.
    let (_dir, mut db) = init_with(CARDINALITY_SCHEMA, CARDINALITY_SEED).await;
    let bad = r#"{"edge":"WorksAt","from":"Alice","to":"Acme"}
{"edge":"WorksAt","from":"Alice","to":"Beta"}"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Append)
        .await
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("cardinality")
            || err.to_string().to_lowercase().contains("@card"),
        "got: {}",
        err
    );
}
