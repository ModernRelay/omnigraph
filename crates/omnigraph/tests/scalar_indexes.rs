//! Coverage for `build_indices_on_dataset_for_catalog`'s per-property index
//! dispatch: which scalar/vector index each `@index`/`@key` column gets.
//!
//! The observable signal is `SnapshotTable::index_coverage`, which
//! reports `Indexed` only when a BTREE covers the column (the same helper the
//! traversal chooser uses). Enums and orderable scalars must get a BTREE so
//! `=`/range/IN/IS NULL are index-accelerated; free-text Strings keep FTS
//! (which `index_coverage` does not count as a BTREE, by design).

mod helpers;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::IndexCoverage;

use helpers::*;

const SCHEMA: &str = r#"
node Item {
    slug: String @key
    status: enum(active, archived) @index
    published: DateTime @index
    rank: I32 @index
    title: String @index
    note: String?
}
"#;

const DATA: &str = r#"{"type":"Item","data":{"slug":"a","status":"active","published":"2024-06-01T00:00:00Z","rank":1,"title":"alpha","note":"n1"}}
{"type":"Item","data":{"slug":"b","status":"archived","published":"2023-01-01T00:00:00Z","rank":2,"title":"beta","note":"n2"}}
{"type":"Item","data":{"slug":"c","status":"active","published":"2025-02-02T00:00:00Z","rank":3,"title":"gamma","note":"n3"}}"#;

// Enums and orderable scalars (DateTime, numeric) get a BTREE from the index
// reconciler, so a `=`/range filter on them uses the index. Free-text
// String `@index` keeps FTS (no BTREE), and an un-annotated column has no
// scalar index — both report `Degraded`, which is the negative control that
// keeps this test from being vacuously green.
#[tokio::test]
async fn node_scalar_and_enum_index_columns_get_btree() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&mut db, DATA, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices().await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Item").await.unwrap();

    for col in ["status", "published", "rank"] {
        let cov = ds.index_coverage(col).await.unwrap();
        assert_eq!(
            cov,
            IndexCoverage::Indexed,
            "column '{col}' (enum/DateTime/numeric @index) must get a BTREE, got {cov:?}"
        );
    }

    // Free-text String @index -> FTS, which is not a BTREE -> Degraded.
    let title_cov = ds.index_coverage("title").await.unwrap();
    assert!(
        matches!(title_cov, IndexCoverage::Degraded { .. }),
        "free-text String @index should keep FTS (no BTREE), got {title_cov:?}"
    );

    // No @index annotation -> no scalar index at all -> Degraded.
    let note_cov = ds.index_coverage("note").await.unwrap();
    assert!(
        matches!(note_cov, IndexCoverage::Degraded { .. }),
        "un-annotated column should have no scalar index, got {note_cov:?}"
    );
}

const TOUCH_NOTE: &str = r#"
query touch_note($slug: String, $note: String) {
    update Item set { note: $note } where slug = $slug
}
"#;

// Field-level-update acceptance cell (red → green): updating an UN-indexed property must
// not degrade index coverage on any OTHER column. The whole-row update merge
// marks every column modified, so Lance prunes the touched fragments from every
// index (`prune_updated_fields_from_indices`) and the rewritten rows land
// outside coverage; the partial-schema update (fields_modified = assigned
// columns only, in-place column patch on the same fragment) leaves the id/enum/
// scalar BTREEs intact. This is the erosion class measured in the field-level-
// updates investigation: keyed operations degrade ~+1 read/fragment after
// whole-row updates until an optimize.
#[tokio::test]
async fn update_of_unindexed_property_preserves_other_index_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&mut db, DATA, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices().await.unwrap();

    db.mutate(
        "main",
        TOUCH_NOTE,
        "touch_note",
        &params(&[("$slug", "a"), ("$note", "touched")]),
    )
    .await
    .unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Item").await.unwrap();
    for col in ["status", "published", "rank"] {
        let cov = ds.index_coverage(col).await.unwrap();
        assert_eq!(
            cov,
            IndexCoverage::Indexed,
            "updating un-indexed 'note' must not degrade the index on '{col}', got {cov:?}"
        );
    }

    // Known residual (tripwire, not the goal state): the partial merge source
    // must carry the join key (`id`) for matching, and Lance's column patcher
    // counts every source column as modified — including the ON column whose
    // values are equal by definition of the match — so the id BTREE alone
    // loses the patched fragment until the reconciler folds it back. Parity
    // with the whole-row path for `id`, strictly better for every other index.
    // When upstream excludes ON columns from column patches this assertion
    // goes red — flip it to `Indexed` and delete this comment.
    let id_cov = ds.index_coverage("id").await.unwrap();
    assert!(
        matches!(id_cov, IndexCoverage::Degraded { .. }),
        "id-BTREE currently loses patched fragments (join key rides the source); \
         if this is now Indexed, upstream fixed ON-column patching — tighten this test"
    );
}

const COMPOSITE_SCHEMA: &str = r#"
node Slot {
    name: String @key
    room: String?
    hour: I32 @index
    @unique(room, hour)
}
"#;

const COMPOSITE_DATA: &str = r#"{"type":"Slot","data":{"name":"s1","room":"roomA","hour":9}}
{"type":"Slot","data":{"name":"s2","room":"roomB","hour":9}}"#;

const MOVE_SLOT: &str = r#"
query move_slot($name: String, $room: String) {
    update Slot set { room: $room } where name = $name
}
"#;

// A `@unique`-group completion column is a VALIDATION input, not a merge
// input: an update assigning only `room` must not patch `hour` (its value is
// unchanged), so `hour`'s index keeps the fragment. The staged merge source is
// (id + assigned); the completion column rides only the validation change-set.
#[tokio::test]
async fn completion_column_index_survives_partial_update() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, COMPOSITE_SCHEMA).await.unwrap();
    load_jsonl(&mut db, COMPOSITE_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();

    db.mutate(
        "main",
        MOVE_SLOT,
        "move_slot",
        &params(&[("$name", "s2"), ("$room", "roomC")]),
    )
    .await
    .unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Slot").await.unwrap();
    let cov = ds.index_coverage("hour").await.unwrap();
    assert_eq!(
        cov,
        IndexCoverage::Indexed,
        "completion column 'hour' was not assigned — its index must keep coverage, got {cov:?}"
    );
}
