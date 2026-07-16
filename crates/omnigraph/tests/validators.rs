// Cross-path validator wire-up tests: each validator (enum, intra-batch
// unique, range, edge cardinality) must reject invalid data on every write
// path — JSONL load, mutation insert (node + edge where applicable),
// mutation update.

mod helpers;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::{count_rows, mutate_main, params};

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

const UNIQUE_MUTATIONS: &str = r#"
query insert_user($name: String, $email: String) {
    insert User { name: $name, email: $email }
}
"#;

// A non-String `@unique` column: the committed cross-version probe must build a
// typed literal, not a stringified key, or it compares a Date32 column to a Utf8
// value (a DataFusion coercion error that breaks every write to the table).
const DATE_UNIQUE_SCHEMA: &str = r#"
node Task {
    name: String @key
    due: Date @unique
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

// A non-zero @card min so a move that vacates a src can drop it below the floor.
const CARD_MIN_SCHEMA: &str = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company @card(1..)
"#;

const CARD_MIN_DELETE_MUTATIONS: &str = r#"
query drop_employment($person: String) {
    delete WorksAt where from = $person
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

// ─── Vector element validation (loader surface) ─────────────────────────────

const VECTOR_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    embedding: Vector(3)
}
"#;

/// iss-loader-vector-element-coercion: the loader must reject non-numeric
/// vector elements loudly (parity with the mutation path's "vector elements
/// must be numeric"), never coerce them to 0.0 — a NaN serialized as JSON
/// null (JSON has no NaN) or a string element silently zeroed the vector's
/// direction while passing every dimension check.
#[tokio::test]
async fn non_numeric_vector_element_rejected_on_jsonl_load() {
    let (_dir, mut db) = init_with(VECTOR_SCHEMA, "").await;

    // A null element (what json! produces for a non-finite float).
    let bad_null = r#"{"type":"Doc","data":{"slug":"d1","embedding":[0.1,null,0.3]}}"#;
    let err = load_jsonl(&mut db, bad_null, LoadMode::Overwrite)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("elements must be numeric"),
        "null element must be rejected, got: {}",
        err
    );

    // A string element.
    let bad_str = r#"{"type":"Doc","data":{"slug":"d2","embedding":[0.1,"0.2",0.3]}}"#;
    let err = load_jsonl(&mut db, bad_str, LoadMode::Overwrite)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("elements must be numeric"),
        "string element must be rejected, got: {}",
        err
    );

    // Pin the adjacent (existing) dimension check while we are here — no
    // loader vector validation had any coverage before this test.
    let bad_dim = r#"{"type":"Doc","data":{"slug":"d3","embedding":[0.1,0.2]}}"#;
    let err = load_jsonl(&mut db, bad_dim, LoadMode::Overwrite)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("expects 3 dimensions, got 2"),
        "dimension mismatch must be rejected, got: {}",
        err
    );

    // A valid row still loads.
    let good = r#"{"type":"Doc","data":{"slug":"d4","embedding":[0.1,0.2,0.3]}}"#;
    load_jsonl(&mut db, good, LoadMode::Overwrite).await.unwrap();
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

// Single-row mutation insert can't violate INTRA-BATCH uniqueness (one row).
// CROSS-VERSION uniqueness against already-committed rows IS now enforced on the
// mutation path via the unified evaluator (#1/#2); the loader path's
// cross-version check lands with the loader migration.

/// Cross-version uniqueness, closed by the write-path evaluator migration:
/// two SEPARATE mutations inserting distinct rows with the same `@unique` value —
/// the second is rejected against the committed first (previously a gap).
#[tokio::test]
async fn cross_version_unique_rejected_on_mutation_insert() {
    let (_dir, mut db) = init_with(UNIQUE_SCHEMA, "").await;
    mutate_main(
        &mut db,
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Bob"), ("$email", "dup@example.com")]),
    )
    .await
    .unwrap();
    let err = mutate_main(
        &mut db,
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Carol"), ("$email", "dup@example.com")]),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("@unique violation on User.email"),
        "got: {}",
        err
    );
}

/// The cross-version unique check must NOT flag a row updating itself: an upsert
/// of an existing `@key` (same id) is an update, not a duplicate. Re-inserting
/// the same key with its own `@unique` value must succeed (the evaluator excludes
/// the committed same-id holder).
#[tokio::test]
async fn reinsert_existing_key_is_upsert_not_unique_violation() {
    let (_dir, mut db) = init_with(UNIQUE_SCHEMA, "").await;
    mutate_main(
        &mut db,
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Alice"), ("$email", "alice@example.com")]),
    )
    .await
    .unwrap();
    mutate_main(
        &mut db,
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Alice"), ("$email", "alice@example.com")]),
    )
    .await
    .expect("re-inserting an existing @key upserts; it is not a unique violation");
}

// ─── Cross-version uniqueness + RI on the LOADER path (Slice 3) ───────────────

const RI_SCHEMA: &str = r#"
node Person { name: String @key }
edge Knows: Person -> Person
"#;

/// Cross-version uniqueness is now enforced on the bulk-load path too: a second
/// Append load duplicating a committed `@unique` value is rejected. The second
/// load carries a MULTI-ROW batch (one colliding row + one clean row) so it
/// exercises the batched committed probe (one scan for the whole key set, the
/// colliding key found in the returned map) and load atomicity (the clean row
/// must not survive the rejected load).
#[tokio::test]
async fn cross_version_unique_rejected_on_append_load() {
    let (_dir, mut db) = init_with(UNIQUE_SCHEMA, "").await;
    load_jsonl(
        &mut db,
        r#"{"type":"User","data":{"name":"Bob","email":"dup@example.com"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let err = load_jsonl(
        &mut db,
        r#"{"type":"User","data":{"name":"Carol","email":"dup@example.com"}}
{"type":"User","data":{"name":"Dave","email":"dave@example.com"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("@unique violation on User.email"),
        "got: {}",
        err
    );
    // Atomicity: the clean sibling row was rejected with the batch.
    assert_eq!(
        count_rows(&db, "node:User").await,
        1,
        "rejected load must not commit its clean sibling row"
    );
}

/// Fix D: the cross-version `@unique` probe must use a typed literal on a
/// non-String column. A second-version row colliding with a committed `Date`
/// value must surface a proper `@unique` violation — not a Date32-vs-Utf8
/// coercion error (the red symptom before the fix).
#[tokio::test]
async fn cross_version_unique_rejected_on_date_column() {
    let (_dir, mut db) = init_with(DATE_UNIQUE_SCHEMA, "").await;
    load_jsonl(
        &mut db,
        r#"{"type":"Task","data":{"name":"T1","due":"2026-06-29"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    let err = load_jsonl(
        &mut db,
        r#"{"type":"Task","data":{"name":"T2","due":"2026-06-29"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("@unique violation on Task.due"),
        "got: {}",
        err
    );
}

/// Fix D companion: a non-colliding write to a `Date @unique` table must
/// succeed. Before the fix the committed probe raised a coercion error for
/// ANY second write (it compared Date32 to a Utf8 literal regardless of a
/// match), so this happy path failed too.
#[tokio::test]
async fn noncolliding_write_to_date_unique_column_succeeds() {
    let (_dir, mut db) = init_with(DATE_UNIQUE_SCHEMA, "").await;
    load_jsonl(
        &mut db,
        r#"{"type":"Task","data":{"name":"T1","due":"2026-06-29"}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();
    load_jsonl(
        &mut db,
        r#"{"type":"Task","data":{"name":"T2","due":"2026-07-01"}}"#,
        LoadMode::Append,
    )
    .await
    .expect("a distinct Date value must not collide and must not raise a coercion error");
    assert_eq!(count_rows(&db, "node:Task").await, 2);
}

/// Fix B: a Merge-load that MOVES an edge to a new src must recount the OLD
/// src. Moving Alice's only WorksAt to Bob drops Alice to zero, below
/// @card(1..). Before the fix only the new src (Bob) was in the affected set,
/// so Alice's underflow was missed and the load silently succeeded.
#[tokio::test]
async fn merge_load_edge_src_move_rechecks_vacated_src_cardinality() {
    let seed = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}
{"type":"Company","data":{"name":"Acme"}}
{"edge":"WorksAt","from":"Alice","to":"Acme","data":{"id":"E1"}}"#;
    let (_dir, mut db) = init_with(CARD_MIN_SCHEMA, seed).await;

    let err = load_jsonl(
        &mut db,
        r#"{"edge":"WorksAt","from":"Bob","to":"Acme","data":{"id":"E1"}}"#,
        LoadMode::Merge,
    )
    .await
    .expect_err("moving Alice's only edge to Bob drops Alice below @card(1..)");
    assert!(
        err.to_string().contains("@card violation") && err.to_string().contains("Alice"),
        "got: {}",
        err
    );
}

/// Fix A: a Merge-load batch listing the same edge id twice with different srcs
/// must be counted ONCE (commit dedupes by id, last-wins). Alice keeps her one
/// committed edge and Bob gets the (deduped) E1, both within @card(0..1), so the
/// load must succeed. Before the fix E1 was counted under both srcs, giving
/// Alice a phantom second edge and a spurious max violation.
#[tokio::test]
async fn merge_load_duplicate_edge_id_counts_once_per_card() {
    let seed = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}
{"type":"Company","data":{"name":"Acme"}}
{"type":"Company","data":{"name":"Beta"}}
{"edge":"WorksAt","from":"Alice","to":"Acme","data":{"id":"E0"}}"#;
    let (_dir, mut db) = init_with(CARDINALITY_SCHEMA, seed).await;

    // Same edge id E1 under two srcs in one batch: commit keeps the last
    // (Bob->Beta). Alice stays at her one committed edge (E0).
    let batch = r#"{"edge":"WorksAt","from":"Alice","to":"Beta","data":{"id":"E1"}}
{"edge":"WorksAt","from":"Bob","to":"Beta","data":{"id":"E1"}}"#;
    load_jsonl(&mut db, batch, LoadMode::Merge)
        .await
        .expect("a deduped edge id must not double-count Alice into a @card(0..1) violation");
    assert_eq!(count_rows(&db, "edge:WorksAt").await, 2);
}

/// A direct edge DELETE must recount the source it empties. Deleting Alice's
/// only WorksAt drops her to zero, below @card(1..), and must be rejected.
/// Deletes stage as predicates (absent from the constructive change-set), so
/// before the fix the mutation committed without any cardinality check — while
/// the merge path, which carries deleted_ids, would have caught it.
#[tokio::test]
async fn mutation_delete_edge_below_card_min_rejected() {
    let seed = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}
{"edge":"WorksAt","from":"Alice","to":"Acme","data":{"id":"E1"}}"#;
    let (_dir, mut db) = init_with(CARD_MIN_SCHEMA, seed).await;

    let err = mutate_main(
        &mut db,
        CARD_MIN_DELETE_MUTATIONS,
        "drop_employment",
        &params(&[("$person", "Alice")]),
    )
    .await
    .expect_err("deleting Alice's only WorksAt drops her below @card(1..)");
    assert!(
        err.to_string().contains("@card violation") && err.to_string().contains("Alice"),
        "got: {}",
        err
    );
    assert_eq!(
        count_rows(&db, "edge:WorksAt").await,
        1,
        "the rejected delete must not have removed the edge"
    );
}

/// A Merge load re-upserting an existing `@key` with its own `@unique` value is
/// an update, not a duplicate — it must NOT false-trigger the cross-version check.
#[tokio::test]
async fn merge_load_reupsert_existing_key_is_not_unique_violation() {
    let (_dir, mut db) = init_with(UNIQUE_SCHEMA, "").await;
    let row = r#"{"type":"User","data":{"name":"Alice","email":"alice@example.com"}}"#;
    load_jsonl(&mut db, row, LoadMode::Merge).await.unwrap();
    load_jsonl(&mut db, row, LoadMode::Merge)
        .await
        .expect("merge-load re-upserting an existing @key is not a unique violation");
}

/// `Overwrite` replaces the touched tables, so edge RI must validate against the
/// NEW batch image, not the replaced committed one. An edge to a node that exists
/// only in the new batch loads cleanly (regression against using the old image).
#[tokio::test]
async fn overwrite_load_validates_ri_against_new_image() {
    let (_dir, mut db) = init_with(RI_SCHEMA, r#"{"type":"Person","data":{"name":"Alice"}}"#).await;
    let batch = r#"{"type":"Person","data":{"name":"Carol"}}
{"edge":"Knows","from":"Carol","to":"Carol"}"#;
    load_jsonl(&mut db, batch, LoadMode::Overwrite)
        .await
        .expect("Overwrite RI validates against the new batch image, not the replaced committed");
}

/// And an Append load whose edge references a non-existent node is still rejected
/// (edge-RI enforced via the evaluator).
#[tokio::test]
async fn append_load_rejects_orphan_edge() {
    let (_dir, mut db) = init_with(RI_SCHEMA, r#"{"type":"Person","data":{"name":"Alice"}}"#).await;
    let err = load_jsonl(
        &mut db,
        r#"{"edge":"Knows","from":"Alice","to":"Ghost"}"#,
        LoadMode::Append,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("not found"),
        "orphan edge must be rejected, got: {}",
        err
    );
}

/// Finding 1: overwriting a NODE table can strand a retained edge in a
/// non-overwritten table. Seed Alice, Bob + Knows(Alice->Bob); Overwrite-load
/// node:Person with only Alice (Bob removed), leaving edge:Knows untouched ->
/// Knows(Alice->Bob) is now an orphan and must be rejected. The overwrite-removed
/// Bob is not expressed as a deleted_id, so edge-RI path-b never runs.
#[tokio::test]
async fn overwrite_node_removal_rejects_retained_orphan_edge() {
    let seed = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}
{"edge":"Knows","from":"Alice","to":"Bob"}"#;
    let (_dir, mut db) = init_with(RI_SCHEMA, seed).await;

    let err = load_jsonl(
        &mut db,
        r#"{"type":"Person","data":{"name":"Alice"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .expect_err("removing Bob via overwrite while Knows(Alice->Bob) is retained orphans the edge");
    assert!(
        err.to_string().contains("not found"),
        "retained edge to an overwrite-removed node must be rejected, got: {}",
        err
    );
}

/// Finding 2: uniqueness must evaluate the final coalesced image per id, not
/// accumulate superseded keys. One mutation updates Alice.email temp -> final,
/// then inserts Carol.email = temp. The final state (Alice=final, Carol=temp) is
/// valid, but the validator retains the stale Alice->temp and false-rejects Carol.
#[tokio::test]
async fn chained_unique_update_then_reuse_freed_value_is_not_a_violation() {
    let (_dir, mut db) = init_with(
        UNIQUE_SCHEMA,
        r#"{"type":"User","data":{"name":"Alice","email":"orig"}}"#,
    )
    .await;
    const Q: &str = r#"
query reassign() {
    update User set { email: "temp" } where name = "Alice"
    update User set { email: "final" } where name = "Alice"
    insert User { name: "Carol", email: "temp" }
}
"#;
    mutate_main(&mut db, Q, "reassign", &params(&[]))
        .await
        .expect("Alice ends at 'final' and Carol takes the freed 'temp' — final image has no collision");
}

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

/// RFC-013 step 3b regression guard (cursor High / codex P1 on #298): edge `@card`
/// validation must scan LIVE committed HEAD, not the pinned `txn.base`. Collapse #1
/// skips the edge accumulation open, so a non-strict edge insert under a `WriteTxn`
/// reopens for the cardinality scan — and that scan must observe edges a concurrent
/// writer committed after this mutation captured its base, or a `@card` max is
/// silently exceeded (invariant 9). The residual validate→commit TOCTOU is the §7.1
/// gap (step 4); this only un-widens what 3b widened (live HEAD vs mutation-start base).
///
/// Deterministic — no failpoint: handle B's coordinator is stale by construction
/// (the write path does not probe the manifest version, unlike the read path). B MUST
/// NOT read between A's commit and B's insert — a read refreshes B's coordinator and
/// masks the bug (the same caveat as the served stale-view repro in `writes.rs`).
#[tokio::test]
async fn cardinality_rejected_for_stale_handle_after_concurrent_edge_commit() {
    let (dir, mut db_a) = init_with(CARDINALITY_SCHEMA, CARDINALITY_SEED).await;
    let uri = dir.path().to_str().unwrap();

    // Handle B opens the same graph at the seed version (no edges yet); it then
    // never reads again, so its in-memory coordinator stays pinned at the seed.
    let mut db_b = Omnigraph::open(uri).await.unwrap();

    // Handle A commits WorksAt(Alice -> Acme): Alice is now at the @card(0..1) max.
    // This advances the on-disk manifest; B's coordinator is now stale.
    mutate_main(
        &mut db_a,
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Acme")]),
    )
    .await
    .unwrap();

    // Handle B (stale, never read since A committed) inserts a second WorksAt for
    // Alice. B is non-strict + under a WriteTxn, so collapse #1 skips the open and the
    // cardinality scan reopens: it MUST read live HEAD (Alice has 1) → reject (1+1 > 1),
    // not the stale base (Alice has 0) → which would wrongly pass and commit a 2nd edge.
    let err = mutate_main(
        &mut db_b,
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Beta")]),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("cardinality")
            || err.to_string().to_lowercase().contains("@card"),
        "a stale-handle edge insert must be rejected by @card against live HEAD, got: {}",
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
