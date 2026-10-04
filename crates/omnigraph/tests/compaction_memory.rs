//! Peak allocation of Blob-table compaction and schema apply.
//!
//! Lance 11 compaction materializes every managed Blob payload of one scanner
//! batch, and a batch reads up to a whole fragment by default. `optimize`
//! derives each compaction task's batch size from that task's largest row,
//! its managed Blob bytes summed over the Blob columns, so one batch holds at
//! most 32 MiB of managed payload unless a single row exceeds it. This
//! instrument counts heap bytes with a global allocator and reports the peak
//! of a stock Lance compaction at the default and at the derived batch size,
//! then asserts the engine's `optimize` peaks within the budget plus the two
//! named allowances on fragments twice as wide as the derived batch. Every
//! value is 1 MiB, a packed placement. A second instrument measures schema
//! apply's peak over Blob tables of two sizes: its column changes are
//! metadata-only, so the peak must not grow with the table's Blob bytes.
//! Both are ignored by default: the allocator counts the whole process, so a
//! measurement needs the test binary to itself (`--exact`).

mod helpers;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::{WriteMode, WriteParams};
use lance::{BlobArrayBuilder, Dataset};
use lance_file::version::LanceFileVersion;
use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;

struct CountingAllocator;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if new_size >= layout.size() {
                let grown = new_size - layout.size();
                let now = CURRENT.fetch_add(grown, Ordering::Relaxed) + grown;
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const MIB: usize = 1024 * 1024;
/// The engine's compaction byte budget (`COMPACTION_BLOB_BATCH_BYTES`).
const BUDGET: usize = 32 * MIB;
/// The batch the engine derives for 1 MiB rows: 32 MiB / 1 MiB.
const DERIVED_ROWS: usize = 32;
/// Rows in the widest fragment each measurement compacts.
const WIDE_FRAGMENT_ROWS: usize = 2 * DERIVED_ROWS;
/// One 1 MiB value in flight beside a full batch: its read buffer and its
/// copy into the batch.
const IN_FLIGHT_VALUE_ALLOWANCE: usize = 2 * MIB;
/// Everything `optimize` allocates that is not Blob payload: scan, writer,
/// index and manifest state. An allowance, not a derived figure.
const NON_PAYLOAD_ALLOWANCE: usize = 8 * MIB;

/// Heap bytes allocated above the level at entry, at the peak of `run`.
async fn peak_above_baseline<T>(run: impl std::future::Future<Output = T>) -> (T, usize) {
    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let output = run.await;
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
    (output, peak)
}

fn payload(row: usize) -> Vec<u8> {
    vec![u8::try_from(row % 251).unwrap(); MIB]
}

/// A V2.2 Blob dataset of two `WIDE_FRAGMENT_ROWS`-row fragments of 1 MiB
/// values, the shape an earlier compaction leaves behind.
async fn wide_fragment_dataset(uri: &str) -> Dataset {
    let rows = 2 * WIDE_FRAGMENT_ROWS;
    let mut content = BlobArrayBuilder::new(rows);
    for row in 0..rows {
        content.push_bytes(payload(row)).unwrap();
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        lance::blob::blob_field("content", true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..rows as i32)),
            content.finish().unwrap(),
        ],
    )
    .unwrap();
    let dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            max_rows_per_file: WIDE_FRAGMENT_ROWS,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(dataset.get_fragments().len(), 2);
    dataset
}

async fn load_rows(db: &omnigraph::Session, rows: std::ops::Range<usize>, mode: LoadMode) {
    let lines = rows
        .map(|row| {
            serde_json::json!({
                "type": "Doc",
                "data": {
                    "slug": format!("d{row:03}"),
                    "content": format!(
                        "base64:{}",
                        base64::engine::general_purpose::STANDARD.encode(payload(row))
                    ),
                },
            })
            .to_string()
        })
        .collect::<Vec<_>>();
    db.load_jsonl(&lines.join("\n"), mode).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "instrument: compaction peak allocation on Blob tables"]
async fn compaction_peak_allocation_on_blob_tables() {
    let dir = tempfile::tempdir().unwrap();
    let mut peaks = Vec::new();
    for (label, batch_size) in [("default", None), ("derived", Some(DERIVED_ROWS))] {
        let uri = dir.path().join(format!("{label}.lance"));
        let mut dataset = wide_fragment_dataset(uri.to_str().unwrap()).await;
        let (metrics, peak) = peak_above_baseline(compact_files(
            &mut dataset,
            CompactionOptions {
                batch_size,
                ..Default::default()
            },
            None,
        ))
        .await;
        assert_eq!(metrics.unwrap().fragments_removed, 2);
        eprintln!(
            "lance compact_files, 2 x {WIDE_FRAGMENT_ROWS} rows of 1 MiB, batch {label}: \
             peak {:.1} MiB",
            peak as f64 / MIB as f64
        );
        peaks.push(peak);
    }
    assert!(
        peaks[1] < peaks[0],
        "the derived batch must lower the stock compaction peak: {peaks:?}"
    );

    // A load decodes at most 32 MiB and Lance compacts only fragments of equal
    // index coverage, so earlier optimizes build the two wide fragments.
    let graph = tempfile::tempdir().unwrap();
    let uri = graph.path().to_str().unwrap();
    let db = helpers::session(
        Omnigraph::init(
            uri,
            "node Doc {\n    slug: String @key\n    content: Blob?\n}\n",
        )
        .await
        .unwrap(),
    );
    for load in 0..8 {
        let rows = load * 16..(load + 1) * 16;
        let mode = if load == 0 {
            LoadMode::Overwrite
        } else {
            LoadMode::Merge
        };
        load_rows(&db, rows, mode).await;
        if load % 4 == 3 {
            db.optimize().await.unwrap();
        }
    }
    let fragment_rows = helpers::open_pinned_dataset_for_test(&db, "main", "node:Doc")
        .await
        .get_fragments()
        .iter()
        .map(|fragment| fragment.metadata().physical_rows.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        fragment_rows,
        vec![WIDE_FRAGMENT_ROWS; 2],
        "test precondition"
    );
    // Optimize leaves the new fragment outside the full-text index; the rebuild evens coverage.
    db.rebuild_full_text_indices_on("main").await.unwrap();

    let (stats, peak) = peak_above_baseline(db.optimize()).await;
    let doc = stats
        .unwrap()
        .into_iter()
        .find(|stat| stat.type_key == "node:Doc")
        .unwrap();
    assert!(doc.committed);
    assert_eq!(doc.fragments_removed, 2, "the measured optimize compacts");
    let bound = BUDGET + IN_FLIGHT_VALUE_ALLOWANCE + NON_PAYLOAD_ALLOWANCE;
    eprintln!(
        "engine optimize, 2 x {WIDE_FRAGMENT_ROWS} rows of 1 MiB: \
         peak {:.1} MiB (bound {:.1} MiB)",
        peak as f64 / MIB as f64,
        bound as f64 / MIB as f64
    );
    assert!(
        peak <= bound,
        "optimize allocated {peak} bytes at its peak, above the {bound}-byte bound"
    );
}

/// Schema apply on a Doc table of `rows` 1 MiB Blob values: one apply adds a
/// nullable property and renames another, a second drops it. Returns the peak
/// heap above the level at entry across both applies.
async fn schema_apply_peak(rows: usize) -> usize {
    let graph = tempfile::tempdir().unwrap();
    let uri = graph.path().to_str().unwrap();
    let db = helpers::session(
        Omnigraph::init(
            uri,
            "node Doc {\n    slug: String @key\n    content: Blob?\n    label: String?\n}\n",
        )
        .await
        .unwrap(),
    );
    for (load, start) in (0..rows).step_by(16).enumerate() {
        let mode = if load == 0 {
            LoadMode::Overwrite
        } else {
            LoadMode::Merge
        };
        load_rows(&db, start..(start + 16).min(rows), mode).await;
    }
    let (applied, peak) = peak_above_baseline(async {
        db.apply_schema(
            "node Doc {\n    slug: String @key\n    content: Blob?\n    \
             name: String? @rename_from(\"label\")\n    note: String?\n}\n",
        )
        .await?;
        db.apply_schema("node Doc {\n    slug: String @key\n    content: Blob?\n}\n")
            .await
    })
    .await;
    assert!(applied.unwrap().applied);
    peak
}

/// Everything schema apply allocates that is not table data: plan, catalog,
/// manifest reads and publication. An allowance, not a derived figure.
const SCHEMA_APPLY_ALLOWANCE: usize = 16 * MIB;

#[tokio::test(flavor = "current_thread")]
#[ignore = "instrument: schema apply peak allocation is flat in a table's Blob bytes"]
async fn schema_apply_peak_allocation_is_flat_in_blob_bytes() {
    let small_rows = 2 * DERIVED_ROWS;
    let large_rows = 4 * DERIVED_ROWS;
    let small = schema_apply_peak(small_rows).await;
    let large = schema_apply_peak(large_rows).await;
    for (rows, peak) in [(small_rows, small), (large_rows, large)] {
        eprintln!(
            "schema apply (add + rename, then drop) on {rows} rows of 1 MiB Blob: \
             peak {:.1} MiB (allowance {:.1} MiB)",
            peak as f64 / MIB as f64,
            SCHEMA_APPLY_ALLOWANCE as f64 / MIB as f64
        );
    }
    assert!(
        large <= SCHEMA_APPLY_ALLOWANCE,
        "schema apply allocated {large} bytes at its peak over {large_rows} MiB of Blob values"
    );
    assert!(
        large <= small + 4 * MIB,
        "schema apply's peak grew with the table's Blob bytes: {small} -> {large}"
    );
}
