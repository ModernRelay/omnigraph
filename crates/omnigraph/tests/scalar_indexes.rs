//! Coverage for `build_indices_on_dataset_for_catalog`'s per-property index
//! dispatch: which scalar/vector index each `@index`/`@key` column gets.
//!
//! The observable signal is `SnapshotTable::index_coverage`, which
//! reports `Indexed` only when a BTREE covers the column (the same helper the
//! traversal chooser uses). Enums and orderable scalars must get a BTREE so
//! `=`/range/IN/IS NULL are index-accelerated; free-text Strings get FTS
//! *plus* an explicitly-named companion BTREE (equality + `starts_with`).

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
// reconciler, so a `=`/range filter on them uses the index. A free-text
// String `@index` gets FTS plus a companion BTREE (so `=` and `starts_with`
// are index-accelerated too); an un-annotated column has no scalar index and
// reports `Degraded`, the negative control that keeps this test from being
// vacuously green. A second `ensure_indices` run must be a no-op (the
// dual-index check matches by column + type, not name).
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

    // Free-text String @index -> FTS plus a companion BTREE; both must stand.
    let title_cov = ds.index_coverage("title").await.unwrap();
    assert_eq!(
        title_cov,
        IndexCoverage::Indexed,
        "free-text String @index must get a companion BTREE alongside FTS, got {title_cov:?}"
    );
    assert!(
        ds.has_fts_index("title").await.unwrap(),
        "the companion BTREE must not replace the FTS index (explicit-name contract)"
    );

    // No @index annotation -> no scalar index at all -> Degraded.
    let note_cov = ds.index_coverage("note").await.unwrap();
    assert!(
        matches!(note_cov, IndexCoverage::Degraded { .. }),
        "un-annotated column should have no scalar index, got {note_cov:?}"
    );

    // Idempotence: a second run finds every declared index present and
    // publishes nothing (dual indexes must not re-plan as missing forever).
    let version_before = ds.version();
    db.ensure_indices().await.unwrap();
    let snap2 = snapshot_main(&db).await.unwrap();
    let ds2 = snap2.open("node:Item").await.unwrap();
    assert_eq!(
        version_before,
        ds2.version(),
        "second ensure_indices must be a no-op on a fully-indexed graph"
    );
}

// The companion BTREE's name must be impossible to collide with any DEFAULT
// index name (`{column}_idx`): a column literally named after another
// column's companion would otherwise produce two indexes with one name,
// which the staged batch rejects — leaving ensure_indices permanently
// failing for a legal schema. All defaults end in `_idx`; the companion
// suffix deliberately does not.
#[tokio::test]
async fn companion_btree_name_cannot_collide_with_default_index_names() {
    const COLLIDING_SCHEMA: &str = r#"
node Thing {
    slug: String @key
    text: String @index
    text_btree: I32 @index
}
"#;
    const ROWS: &str = r#"{"type":"Thing","data":{"slug":"t1","text":"hello world","text_btree":1}}
{"type":"Thing","data":{"slug":"t2","text":"beta ray","text_btree":2}}"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, COLLIDING_SCHEMA).await.unwrap();
    load_jsonl(&mut db, ROWS, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices()
        .await
        .expect("companion names must not collide with another column's default index name");

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("node:Thing").await.unwrap();
    for col in ["text", "text_btree"] {
        assert_eq!(
            ds.index_coverage(col).await.unwrap(),
            IndexCoverage::Indexed,
            "both '{col}' BTREEs must land despite the adversarial column name"
        );
    }
    assert!(ds.has_fts_index("text").await.unwrap());

    // And the second run converges instead of the two same-named indexes
    // replacing each other forever.
    let version_before = ds.version();
    db.ensure_indices().await.unwrap();
    let snap2 = snapshot_main(&db).await.unwrap();
    let ds2 = snap2.open("node:Thing").await.unwrap();
    assert_eq!(version_before, ds2.version(), "second run must be a no-op");
}
