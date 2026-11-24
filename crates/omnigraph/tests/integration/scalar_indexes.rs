//! Coverage for `build_indices_on_dataset_for_catalog`'s per-property index
//! dispatch: which scalar/vector indexes each `@index`/`@key` column gets
//! (the GQ 3 RFC, section 4, "Scalar indexes").
//!
//! Orderable scalars get a BTREE, so `SnapshotDataset::index_coverage` (the
//! helper the traversal chooser uses) reports `Indexed`. An enum or `Bool`
//! gets a BITMAP, a list a LABEL_LIST, and a free-text String a BTREE for a
//! `@key` alone, an NGRAM and a BTREE for an `@index`, and a full-text index
//! beside them only for `@index(fulltext)`; each is checked by its index
//! family.

use omnigraph::IndexCoverage;
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;

use crate::helpers::*;

const SCHEMA: &str = r#"
node Item {
    slug: String @key
    status: enum(active, archived) @index
    published: DateTime @index
    rank: I32 @index
    title: String @index
    body: String? @index(fulltext)
    active: Bool @index
    tags: [String] @index
    note: String?
}

node Code {
    code: String @key @index
}
"#;

const DATA: &str = r#"{"type":"Item","data":{"slug":"a","status":"active","published":"2024-06-01T00:00:00Z","rank":1,"title":"alpha","body":"alpha text","active":true,"tags":["x"],"note":"n1"}}
{"type":"Item","data":{"slug":"b","status":"archived","published":"2023-01-01T00:00:00Z","rank":2,"title":"beta","body":"beta text","active":false,"tags":[],"note":"n2"}}
{"type":"Item","data":{"slug":"c","status":"active","published":"2025-02-02T00:00:00Z","rank":3,"title":"gamma","body":"gamma text","active":true,"tags":["x","y"],"note":"n3"}}
{"type":"Code","data":{"code":"c-1"}}"#;

/// The index families on `column`, by the message name of each index's
/// details (`BTree`, `Bitmap`, `LabelList`, `NGram`, `Inverted`), sorted.
async fn families(ds: &omnigraph::db::SnapshotDataset, column: &str) -> Vec<String> {
    let field = ds.schema().field(column).unwrap().id;
    let mut families: Vec<String> = ds
        .load_indices()
        .await
        .unwrap()
        .iter()
        .filter(|index| index.fields == [field])
        .filter_map(|index| index.index_details.as_ref())
        .filter_map(|details| details.type_url.rsplit('.').next())
        .map(|message| message.trim_end_matches("IndexDetails").to_string())
        .collect();
    families.sort();
    families
}

// Orderable scalars (DateTime, numeric) get a BTREE from the index
// reconciler, so a `=`/range filter on them uses the index. An enum and a
// `Bool` get a BITMAP and no BTREE, a list a LABEL_LIST, and a free-text
// String a BTREE (`@key` alone), an NGRAM and a BTREE (`@index`, also beside a
// `@key`), and a full-text index beside them only for `@index(fulltext)`, so a
// plain `@index` or a `@key` builds no full-text index. An un-annotated column
// has no scalar index and reports `Degraded`, the negative control that keeps
// this test from being vacuously green. A second `ensure_indices` run must be
// a no-op.
#[tokio::test]
async fn node_index_columns_get_the_index_kinds_their_type_serves() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, SCHEMA).await.unwrap());
    db.load_jsonl(DATA, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices_as(None).await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Item").await.unwrap();

    for col in ["published", "rank"] {
        let cov = ds.index_coverage(col).await.unwrap();
        assert_eq!(
            cov,
            IndexCoverage::Indexed,
            "column '{col}' (DateTime/numeric @index) must get a BTREE, got {cov:?}"
        );
    }
    for (col, expected) in [
        ("published", &["BTree"][..]),
        ("rank", &["BTree"][..]),
        ("status", &["Bitmap"][..]),
        ("active", &["Bitmap"][..]),
        ("tags", &["LabelList"][..]),
        ("slug", &["BTree"][..]),
        ("title", &["BTree", "NGram"][..]),
        ("body", &["BTree", "Inverted", "NGram"][..]),
        ("note", &[][..]),
    ] {
        assert_eq!(families(&ds, col).await, expected, "the indexes of `{col}`");
    }
    assert!(ds.has_fts_index("body").await.unwrap());
    for col in ["slug", "title"] {
        assert!(
            !ds.has_fts_index(col).await.unwrap(),
            "`{col}` builds no full text"
        );
    }
    let code = snap.open_dataset("node:Code").await.unwrap();
    assert_eq!(
        families(&code, "code").await,
        ["BTree", "NGram"],
        "a `@key` with an explicit `@index` follows the `@index` rule"
    );

    // No @index annotation -> no scalar index at all -> Degraded.
    let note_cov = ds.index_coverage("note").await.unwrap();
    assert!(
        matches!(note_cov, IndexCoverage::Degraded { .. }),
        "un-annotated column should have no scalar index, got {note_cov:?}"
    );

    // Idempotence: a second run finds every declared index present and
    // publishes nothing (dual indexes must not re-plan as missing forever).
    let version_before = ds.published_dataset_version();
    db.ensure_indices_as(None).await.unwrap();
    let snap2 = snapshot_main(&db).await.unwrap();
    let ds2 = snap2.open_dataset("node:Item").await.unwrap();
    assert_eq!(
        version_before,
        ds2.published_dataset_version(),
        "second ensure_indices must be a no-op on a fully-indexed graph"
    );
}

/// A BITMAP, LABEL_LIST or NGRAM whose coverage lags an append is rebuilt
/// whole in its own family, by an index pass and by optimize, never as a
/// BTREE under its name, and then covers every fragment: the appended row is
/// found through each.
#[tokio::test]
async fn a_lagging_bitmap_label_list_or_ngram_is_rebuilt_in_its_own_family() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, SCHEMA).await.unwrap());
    db.load_jsonl(DATA, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices_as(None).await.unwrap();
    let append = |slug: &'static str| {
        let db = &db;
        async move {
            db.load_jsonl(
                &format!(
                    r#"{{"type":"Item","data":{{"slug":"{slug}","status":"archived","published":"2025-03-03T00:00:00Z","rank":9,"title":"zeta","active":false,"tags":["z"],"note":"n"}}}}"#
                ),
                LoadMode::Replace,
            )
            .await
            .unwrap();
        }
    };
    let assert_rebuilt = |step: &'static str| {
        let db = &db;
        async move {
            let snap = snapshot_main(db).await.unwrap();
            let ds = snap.open_dataset("node:Item").await.unwrap();
            for (col, expected) in [
                ("status", &["Bitmap"][..]),
                ("active", &["Bitmap"][..]),
                ("tags", &["LabelList"][..]),
                ("title", &["BTree", "NGram"][..]),
                ("body", &["BTree", "Inverted", "NGram"][..]),
            ] {
                assert_eq!(families(&ds, col).await, expected, "{step}: `{col}`");
            }
            let pinned = open_pinned_dataset_for_test(db, "main", "node:Item").await;
            let current: std::collections::BTreeSet<u32> = pinned
                .get_fragments()
                .iter()
                .map(|fragment| fragment.id() as u32)
                .collect();
            let rebuilt = ["_bitmap", "_label_list", "_ngram", "_btree"];
            for index in ds.load_indices().await.unwrap().iter() {
                if !rebuilt.iter().any(|suffix| index.name.ends_with(suffix)) {
                    continue;
                }
                let covered: std::collections::BTreeSet<u32> = index
                    .fragment_bitmap
                    .as_ref()
                    .map(|bitmap| bitmap.iter().collect())
                    .unwrap_or_default();
                assert!(
                    current.is_subset(&covered),
                    "{step}: index '{}' covers {covered:?} of {current:?}",
                    index.name
                );
            }
        }
    };

    append("d").await;
    db.ensure_indices_as(None).await.unwrap();
    assert_rebuilt("index pass").await;

    append("e").await;
    db.optimize_as(None, omnigraph::db::OptimizeRequest::default())
        .await
        .unwrap();
    assert_rebuilt("optimize").await;
}

// #1385: `ensure_indices` builds independent BTREE indexes concurrently, both
// within one table and across tables, with at most four in Lance's builder at
// once. The fixtures index numeric and temporal columns only: an enum or a
// free-text String also builds a BITMAP or an NGRAM, which runs alone. The probe's rendezvous parks the first two builds until both have
// entered, so a serial build loop times out instead of passing on result
// equivalence alone.
const WIDE_SCHEMA: &str = r#"
node Item {
    slug: String @key
    level: I32 @index
    published: DateTime @index
    rank: I32 @index
    weight: I64 @index
    born: Date @index
}
"#;

const WIDE_DATA: &str = r#"{"type":"Item","data":{"slug":"a","level":1,"published":"2024-06-01T00:00:00Z","rank":1,"weight":10,"born":"2020-01-01"}}
{"type":"Item","data":{"slug":"b","level":2,"published":"2023-01-01T00:00:00Z","rank":2,"weight":20,"born":"2021-01-01"}}
{"type":"Item","data":{"slug":"c","level":1,"published":"2025-02-02T00:00:00Z","rank":3,"weight":30,"born":"2022-01-01"}}"#;

const TWO_TABLE_SCHEMA: &str = r#"
node Left {
    name: String
}
node Right {
    name: String
}
"#;

const TWO_TABLE_DATA: &str = r#"{"type":"Left","data":{"name":"l1"}}
{"type":"Left","data":{"name":"l2"}}
{"type":"Right","data":{"name":"r1"}}
{"type":"Right","data":{"name":"r2"}}"#;

async fn ensure_indices_under_rendezvous(db: &Omnigraph) -> (u64, u64) {
    use omnigraph::instrumentation::{IndexBuildProbes, with_index_build_probes};

    let probes = IndexBuildProbes::rendezvous(2);
    let observed = probes.clone();
    let build = with_index_build_probes(probes, Box::pin(db.ensure_indices_as(None)));
    tokio::time::timeout(std::time::Duration::from_secs(30), build)
        .await
        .expect("index build ran its BTREE builds one at a time")
        .unwrap();
    (observed.entered(), observed.peak_in_flight())
}

// Two tables, each with its id, key and four `@index` BTREEs: twelve builds
// an `optimize` run stages, up to `rewrite` tables at once.
const TWO_WIDE_TABLES_SCHEMA: &str = r#"
node Left {
    slug: String @key
    level: I32 @index
    rank: I32 @index
    weight: I64 @index
    born: Date @index
}
node Right {
    slug: String @key
    level: I32 @index
    rank: I32 @index
    weight: I64 @index
    born: Date @index
}
"#;

const TWO_WIDE_TABLES_DATA: &str = r#"{"type":"Left","data":{"slug":"a","level":1,"rank":1,"weight":10,"born":"2020-01-01"}}
{"type":"Left","data":{"slug":"b","level":2,"rank":2,"weight":20,"born":"2021-01-01"}}
{"type":"Right","data":{"slug":"c","level":1,"rank":3,"weight":30,"born":"2022-01-01"}}
{"type":"Right","data":{"slug":"d","level":2,"rank":4,"weight":40,"born":"2023-01-01"}}"#;

#[tokio::test]
async fn index_build_overlaps_btree_builds_of_one_table_issue_1385() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, WIDE_SCHEMA).await.unwrap());
    db.load_jsonl(WIDE_DATA, LoadMode::Overwrite).await.unwrap();

    let (entered, peak) = ensure_indices_under_rendezvous(&db).await;
    assert!(
        entered >= 6,
        "the table needs its id, key and five @index BTREEs, saw {entered} builds"
    );
    assert!(
        (2..=4).contains(&peak),
        "independent BTREE builds must overlap, at most four at once; peak was {peak}"
    );

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Item").await.unwrap();
    for col in ["level", "published", "rank", "weight", "born"] {
        assert_eq!(
            ds.index_coverage(col).await.unwrap(),
            IndexCoverage::Indexed,
            "column '{col}' must be covered by its BTREE after a concurrent build"
        );
    }
    let version_before = ds.published_dataset_version();
    db.ensure_indices_as(None).await.unwrap();
    let ds2 = snapshot_main(&db)
        .await
        .unwrap()
        .open_dataset("node:Item")
        .await
        .unwrap();
    assert_eq!(
        version_before,
        ds2.published_dataset_version(),
        "a second index build after a concurrent one must find nothing to do"
    );
}

#[tokio::test]
async fn index_build_overlaps_tables_issue_1385() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, TWO_TABLE_SCHEMA).await.unwrap());
    db.load_jsonl(TWO_TABLE_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let (entered, peak) = ensure_indices_under_rendezvous(&db).await;
    assert_eq!(
        entered, 2,
        "each table needs exactly its id BTREE, so two builds"
    );
    assert_eq!(peak, 2, "the two tables' builds must overlap");

    let snap = snapshot_main(&db).await.unwrap();
    for table in ["node:Left", "node:Right"] {
        let ds = snap.open_dataset(table).await.unwrap();
        assert!(
            ds.has_btree_index("__id").await.unwrap(),
            "{table} must have its id BTREE"
        );
    }
}

/// `optimize` stages several tables at once (`rewrite`, 8 on the direct
/// path), and the run owns one build budget for all of them: its BTREE builds
/// overlap, but never more than `INDEX_BUILD_CONCURRENCY` at once. With a
/// budget per table, two tables reached eight builds in flight.
#[tokio::test]
async fn optimize_shares_one_index_build_budget_across_tables_issue_1385() {
    use omnigraph::instrumentation::{IndexBuildProbes, with_index_build_probes};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, TWO_WIDE_TABLES_SCHEMA).await.unwrap());
    db.load_jsonl(TWO_WIDE_TABLES_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    // The first four builds wait until four are inside the builder, so the
    // run must overlap up to the whole budget to finish.
    let probes = IndexBuildProbes::rendezvous(4);
    let observed = probes.clone();
    let run = with_index_build_probes(
        probes,
        Box::pin(db.optimize_as(None, omnigraph::db::OptimizeRequest::default())),
    );
    tokio::time::timeout(std::time::Duration::from_secs(30), run)
        .await
        .expect("optimize did not overlap four BTREE builds")
        .unwrap();
    let (entered, peak) = (observed.entered(), observed.peak_in_flight());
    assert_eq!(
        entered, 12,
        "each table builds its id, key and four @index BTREEs"
    );
    assert!(
        peak == 4,
        "one budget per run: four BTREE builds at once across both tables, never more; peak was {peak}"
    );

    let snap = snapshot_main(&db).await.unwrap();
    for table in ["node:Left", "node:Right"] {
        let ds = snap.open_dataset(table).await.unwrap();
        for col in ["level", "rank", "weight", "born"] {
            assert_eq!(
                ds.index_coverage(col).await.unwrap(),
                IndexCoverage::Indexed,
                "{table}.{col} must be covered after optimize"
            );
        }
    }
}

/// An index is the declared (column, family) the engine builds, not the name
/// it gives it. An earlier binary that knows no NGRAM rebuilds a lagging
/// `body_ngram` as a BTREE under that name. The next index pass then lacks
/// the NGRAM and finds the BTREE lagging: the NGRAM's build replaces every
/// segment of the name, and the BTREE is not also folded under it, which
/// published the name twice and refused every later pass. The pass leaves
/// one NGRAM named `body_ngram` beside the declared BTREE, covering every
/// fragment, and a second pass has nothing to do.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread")]
async fn an_earlier_binarys_btree_under_the_ngram_name_is_replaced_by_the_ngram() {
    use lance::index::DatasetIndexExt;
    use lance_index::scalar::ScalarIndexParams;

    const DOC: &str = "node Doc {\n    k: String @key\n    body: String @index\n}\n";
    let rows = |from: usize, to: usize, word: &str| {
        (from..to)
            .map(|i| format!(r#"{{"type":"Doc","data":{{"k":"d{i}","body":"{word} {i}"}}}}"#))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = session(Omnigraph::init(uri, DOC).await.unwrap());
    db.load_jsonl(&rows(0, 20, "graph"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices_as(None).await.unwrap();
    db.load_jsonl(&rows(20, 30, "river"), LoadMode::Replace)
        .await
        .unwrap();

    // The earlier binary's fold: `body_ngram` rebuilt as a BTREE.
    forge_linear_head_from_pin(&db, "main", "node:Doc", 0).await;
    let table_path = snapshot_main(&db)
        .await
        .unwrap()
        .dataset("node:Doc")
        .unwrap()
        .dataset_path
        .clone();
    let table_uri = format!("{}/{}", uri.trim_end_matches('/'), table_path);
    // forbidden-api-allow: test plants the index an earlier binary's fold leaves.
    let mut raw = lance::Dataset::open(&table_uri).await.unwrap();
    raw.create_index(
        &["body"],
        lance_index::IndexType::BTree,
        Some("body_ngram".to_string()),
        &ScalarIndexParams::default(),
        true,
    )
    .await
    .unwrap();
    db.failpoint_publish_table_head_without_index_rebuild_for_test("main", "node:Doc", None)
        .await
        .unwrap();
    let planted = snapshot_main(&db)
        .await
        .unwrap()
        .open_dataset("node:Doc")
        .await
        .unwrap();
    assert_eq!(
        families(&planted, "body").await,
        ["BTree", "BTree"],
        "the earlier binary left two BTREEs and no NGRAM"
    );

    db.load_jsonl(&rows(30, 40, "harbor"), LoadMode::Replace)
        .await
        .unwrap();
    db.ensure_indices_as(None).await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open_dataset("node:Doc").await.unwrap();
    assert_eq!(families(&ds, "body").await, ["BTree", "NGram"]);
    let ngram: Vec<_> = ds
        .load_indices()
        .await
        .unwrap()
        .iter()
        .filter(|index| index.name == "body_ngram")
        .map(|index| {
            index
                .index_details
                .as_ref()
                .map(|details| details.type_url.clone())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(ngram.len(), 1, "one segment named `body_ngram`: {ngram:?}");
    assert!(ngram[0].ends_with("NGramIndexDetails"), "{ngram:?}");
    let pinned = open_pinned_dataset_for_test(&db, "main", "node:Doc").await;
    let current: std::collections::BTreeSet<u32> = pinned
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect();
    for index in ds.load_indices().await.unwrap().iter() {
        if !index.name.starts_with("body_") {
            continue;
        }
        let covered: std::collections::BTreeSet<u32> = index
            .fragment_bitmap
            .as_ref()
            .map(|bitmap| bitmap.iter().collect())
            .unwrap_or_default();
        assert!(
            current.is_subset(&covered),
            "index '{}' covers {covered:?} of {current:?}",
            index.name
        );
    }
    let version = ds.published_dataset_version();
    db.ensure_indices_as(None).await.unwrap();
    let again = snapshot_main(&db)
        .await
        .unwrap()
        .open_dataset("node:Doc")
        .await
        .unwrap();
    assert_eq!(
        version,
        again.published_dataset_version(),
        "a second pass is a no-op"
    );
}
