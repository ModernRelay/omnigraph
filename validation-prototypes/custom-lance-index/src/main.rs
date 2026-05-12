//! MR-925 Experiment 1.2 — custom Lance index plugin from outside the lance crate.
//!
//! Goal: probe what a third-party crate (us) can and *cannot* do when shipping
//! a "custom index" against the public Lance 4.0.0 surface. Produces a
//! compatibility matrix the writeup at `.context/experiments/custom-lance-index.md`
//! consumes.
//!
//! Probes:
//!
//!   P1. Construct an `IndexMetadata` with a non-standard `index_details`
//!       protobuf and commit it via `Operation::CreateIndex`.
//!   P2. Reopen the dataset; verify `load_indices()` returns our row (or filters
//!       it out).
//!   P3. Append fragments; observe whether the index's `fragment_bitmap` is
//!       updated automatically (it should not be — that's the engine's job).
//!   P4. Run a `Scanner` with a filter; observe whether Lance attempts to open
//!       our index. We expect failure: `SCALAR_INDEX_PLUGIN_REGISTRY` is a
//!       `pub(crate)` static with no setter as of 4.0.0
//!       (lance/src/index/scalar.rs:223 carries the TODO).
//!   P5. Run `compact_files` (Rewrite). Observe whether our `IndexMetadata`
//!       survives the rewrite or is dropped.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, UInt64Builder};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::transaction::Operation;
use lance::dataset::WriteParams;
use lance::session::Session;
use lance_index::DatasetIndexExt;
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;
use tempfile::TempDir;
use uuid::Uuid;

use prost_types::Any as ProstAny;

const TYPE_URL: &str = "omnigraph.v0.NeighborIndexDetails";

fn make_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(n: u64, key_base: u64) -> RecordBatch {
    let schema = make_schema();
    let mut keys = UInt64Builder::with_capacity(n as usize);
    let mut payloads = StringBuilder::new();
    for i in 0..n {
        keys.append_value(key_base + i);
        payloads.append_value(format!("p_{:06}", key_base + i));
    }
    RecordBatch::try_new(
        schema,
        vec![Arc::new(keys.finish()), Arc::new(payloads.finish())],
    )
    .expect("build batch")
}

async fn write_initial(uri: &str) -> Result<Dataset> {
    let schema = make_schema();
    let batches = vec![Ok(build_batch(1000, 0))];
    let reader = RecordBatchIterator::new(batches.into_iter(), schema.clone());
    Dataset::write(reader, uri, Some(WriteParams::default()))
        .await
        .context("initial write")
}

async fn append_more(ds: &mut Dataset) -> Result<()> {
    let schema = make_schema();
    let batches = vec![Ok(build_batch(500, 10_000))];
    let reader = RecordBatchIterator::new(batches.into_iter(), schema.clone());
    ds.append(reader, None).await.context("append")?;
    Ok(())
}

/// Construct our custom-index metadata. The bytes payload mimics what a
/// real index plugin would carry: a serialized BTreeMap<u64, u64> (key →
/// row_addr). We don't read this back here — we just want to prove that
/// Lance round-trips it through the manifest unchanged.
fn make_index_metadata(uuid: Uuid, frag_ids: &[u64], dataset_version: u64) -> IndexMetadata {
    let payload_bytes: Vec<u8> = b"omnigraph::neighbor_index v0 (1000 entries)".to_vec();
    let any = ProstAny {
        type_url: TYPE_URL.to_string(),
        value: payload_bytes,
    };

    let mut bitmap = RoaringBitmap::new();
    for f in frag_ids {
        bitmap.insert(*f as u32);
    }

    IndexMetadata {
        uuid,
        fields: vec![0], // 0 = "key" by schema position
        name: "neighbor_idx".to_string(),
        dataset_version,
        fragment_bitmap: Some(bitmap),
        index_details: Some(Arc::new(any)),
        index_version: 0,
        created_at: None,
        base_id: None,
        files: None,
    }
}

async fn commit_index(ds: &Dataset, idx: IndexMetadata) -> Result<Dataset> {
    let op = Operation::CreateIndex {
        new_indices: vec![idx],
        removed_indices: vec![],
    };
    let new = Dataset::commit(
        ds.uri(),
        op,
        Some(ds.manifest().version),
        None,
        None,
        Arc::new(Session::default()),
        false,
    )
    .await
    .context("commit CreateIndex")?;
    Ok(new)
}

#[derive(Default)]
struct Matrix {
    rows: Vec<Row>,
}

struct Row {
    probe: &'static str,
    outcome: String,
    notes: String,
}

impl Matrix {
    fn add(&mut self, probe: &'static str, outcome: impl Into<String>, notes: impl Into<String>) {
        self.rows.push(Row {
            probe,
            outcome: outcome.into(),
            notes: notes.into(),
        });
    }

    fn print(&self) {
        println!("\n{:-^120}", " custom-lance-index compatibility matrix ");
        println!("{:<32} {:<14} {}", "probe", "outcome", "notes");
        println!("{:-<120}", "");
        for r in &self.rows {
            println!("{:<32} {:<14} {}", r.probe, r.outcome, r.notes);
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let tmp = TempDir::new().context("tmpdir")?;
    let uri = format!("file://{}", tmp.path().join("ds").display());
    println!("dataset uri: {uri}");

    let mut matrix = Matrix::default();

    // P1: build a dataset, then construct + commit our custom index.
    let ds = write_initial(&uri).await?;
    let frag_ids: Vec<u64> = ds
        .get_fragments()
        .iter()
        .map(|f| f.id() as u64)
        .collect();
    println!("initial fragments: {frag_ids:?}");

    let our_uuid = Uuid::new_v4();
    let idx = make_index_metadata(our_uuid, &frag_ids, ds.manifest().version);
    let mut ds = match commit_index(&ds, idx).await {
        Ok(d) => {
            matrix.add(
                "P1 construct+commit",
                "OK",
                format!(
                    "Operation::CreateIndex accepted custom type_url '{TYPE_URL}'; commit v{}",
                    d.manifest().version
                ),
            );
            d
        }
        Err(e) => {
            matrix.add("P1 construct+commit", "FAIL", format!("{e:#}"));
            matrix.print();
            return Ok(());
        }
    };

    // P2: load indices.
    let indices = ds.load_indices().await.context("load_indices")?;
    let ours: Vec<&IndexMetadata> = indices
        .iter()
        .filter(|i| i.uuid == our_uuid)
        .collect();
    if ours.len() == 1 {
        let our_idx = ours[0];
        let detail_url = our_idx
            .index_details
            .as_ref()
            .map(|a| a.type_url.clone())
            .unwrap_or_default();
        let frag_count = our_idx
            .fragment_bitmap
            .as_ref()
            .map(|b| b.len())
            .unwrap_or(0);
        matrix.add(
            "P2 load_indices (round-trip)",
            "OK",
            format!(
                "type_url='{detail_url}' fragment_bitmap.len={frag_count} survives retain_supported_indices"
            ),
        );
    } else {
        matrix.add(
            "P2 load_indices (round-trip)",
            "FAIL",
            format!(
                "expected 1 row matching uuid {our_uuid}, found {} (retain_supported_indices likely dropped it)",
                ours.len()
            ),
        );
    }

    // P3: append more rows; the index's fragment_bitmap should NOT
    // auto-update — that's the plugin's job. Verify the dataset still
    // reports the same (stale) bitmap.
    append_more(&mut ds).await?;
    let indices_after_append = ds.load_indices().await?;
    let ours_after_append: Vec<&IndexMetadata> = indices_after_append
        .iter()
        .filter(|i| i.uuid == our_uuid)
        .collect();
    if let Some(idx) = ours_after_append.first() {
        let frags_now: Vec<u32> = idx
            .fragment_bitmap
            .as_ref()
            .map(|b| b.iter().collect())
            .unwrap_or_default();
        matrix.add(
            "P3 append-row coverage",
            if frags_now.len() == frag_ids.len() {
                "STALE_AS_EXPECTED"
            } else {
                "UNEXPECTED_AUTO_UPDATE"
            },
            format!(
                "fragment_bitmap={frags_now:?} (expected {frag_ids:?}); new fragments not auto-covered"
            ),
        );
    } else {
        matrix.add("P3 append-row coverage", "DROPPED", "index disappeared after append");
    }

    // P4: try to scan with a predicate; observe whether Lance tries to open
    // our index. With the closed plugin registry, `open_scalar_index` should
    // never even be invoked on our type_url because the predicate is on
    // `key` — but a different index over `key` does not exist in any builtin
    // type. We assert here that scanning still works (Lance falls back to
    // full-scan) and does NOT panic on our metadata being present.
    let mut scanner = ds.scan();
    scanner
        .filter("key = 42")
        .context("filter")?
        .project(&["key"])
        .context("project")?;
    let stream = scanner.try_into_stream().await.context("scan stream")?;
    let batches: Vec<_> = futures::stream::TryStreamExt::try_collect(stream)
        .await
        .context("scan collect")?;
    let scanned_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    matrix.add(
        "P4 scan with filter on indexed col",
        if scanned_rows == 1 { "FULL_SCAN_FALLBACK" } else { "UNEXPECTED" },
        format!(
            "rows={scanned_rows} (expected 1); SCALAR_INDEX_PLUGIN_REGISTRY refuses unknown type_url '{TYPE_URL}' so scanner falls back to full scan"
        ),
    );

    // P5: run compact_files (Rewrite). Observe whether our IndexMetadata
    // survives the rewrite. The Operation::Rewrite path remaps row addresses
    // for *recognized* indices (BTreeMap of `rewritten_indices`) — our index
    // is not recognized, so we expect Lance to either (a) leave the
    // IndexMetadata in place with stale fragment_bitmap, or (b) drop it.
    let pre_compact_indices = ds.load_indices().await?.len();
    let metrics = compact_files(&mut ds, CompactionOptions::default(), None)
        .await
        .context("compact_files")?;
    let post_compact_indices = ds.load_indices().await?;
    let ours_after_compact: Vec<&IndexMetadata> = post_compact_indices
        .iter()
        .filter(|i| i.uuid == our_uuid)
        .collect();

    let frags_after: Vec<u64> = ds
        .get_fragments()
        .iter()
        .map(|f| f.id() as u64)
        .collect();

    if let Some(idx) = ours_after_compact.first() {
        let bitmap: Vec<u32> = idx
            .fragment_bitmap
            .as_ref()
            .map(|b| b.iter().collect())
            .unwrap_or_default();
        let outcome = if frags_after.iter().all(|f| bitmap.contains(&(*f as u32))) {
            "REMAPPED"
        } else if bitmap.is_empty() {
            "EMPTIED"
        } else {
            "STALE_BITMAP"
        };
        matrix.add(
            "P5 compact_files (Rewrite)",
            outcome,
            format!(
                "before={pre_compact_indices} indices; after={} indices; rewritten files={}; new fragments={frags_after:?}; idx.fragment_bitmap={bitmap:?}",
                post_compact_indices.len(),
                metrics.files_added
            ),
        );
    } else {
        matrix.add(
            "P5 compact_files (Rewrite)",
            "DROPPED",
            format!(
                "index dropped during compaction; before={pre_compact_indices} indices, after={} indices; files_added={}",
                post_compact_indices.len(),
                metrics.files_added
            ),
        );
    }

    matrix.print();

    // Final commentary printed for the writeup.
    println!("\n[note] Lance 4.0.0 has a private static `SCALAR_INDEX_PLUGIN_REGISTRY` (see");
    println!("       lance/src/index/scalar.rs:223). The `// TODO: Allow users to register their own plugins`");
    println!("       comment confirms this surface is not yet pluggable. We can write");
    println!("       custom IndexMetadata, but the Lance scanner cannot dispatch to a custom plugin.");

    Ok(())
}
