#![cfg(feature = "failpoints")]

//! RFC-026 Phase-B1 cost evidence.
//!
//! Keep the terms separate.  A warm already-claimed acknowledgement, a cold
//! claim/replay, one selected flushed-generation scan, retained shard-manifest
//! metadata, and the shared graph-manifest publisher are different costs with
//! different scaling variables.  Combining them into one latency number would
//! hide exactly the regressions this instrument exists to expose.

mod helpers;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fail::FailScenario;
use futures::TryStreamExt;
use lance::dataset::mem_wal::write::StoredBatch;
use lance::dataset::mem_wal::{ShardManifestStore, schema_with_tombstone};
use lance_core::utils::bloomfilter::sbbf::Sbbf;
use lance_index::mem_wal::{FlushedGeneration, ShardId, ShardManifest, ShardStatus};
use lance_io::utils::tracking_store::{IOTracker, IoStats};
use object_store::path::Path as ObjectPath;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph::instrumentation::{CountingStorageAdapter, StorageReadCounts};
use omnigraph::storage::storage_for_uri;
use serial_test::serial;

use helpers::cost::with_raw_io_trackers;

const STREAM_SCHEMA: &str = r#"
node Person { score: I32 }
node History { tick: I32 }
"#;
const PAYLOAD_SCHEMA: &str = "node Person { payload: String }\n";
const TABLE: &str = "node:Person";
const ADD_HISTORY: &str = r#"
query add_history($tick: I32) {
    insert History { tick: $tick }
}
"#;

const WIDEST_ROWS: usize = 8_192;
const WIDEST_PAYLOAD_BYTES: usize = 4_032;
const HARD_ARROW_BYTES: usize = 32 * 1024 * 1024;
const FOLD_RSS_DELTA_REMEASURE_BYTES: u64 = 384 * 1024 * 1024;
const NO_AUTO_ROLL_BYTES: usize = 1024 * 1024 * 1024;
const NO_AUTO_ROLL_ROWS: usize = 8_193;
const NO_AUTO_ROLL_BATCHES: usize = 8_193;

const RSS_CHILD_ENV: &str = "OMNIGRAPH_MEMWAL_COST_CHILD";
const SURVEYED_LANCE_REV: &str = "cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MemWalObjectKind {
    Wal,
    ShardManifestVersion,
    ShardManifestHint,
    GenerationData,
    GenerationManifest,
    GenerationTransaction,
    GenerationDeletion,
    GenerationPkSidecar,
    GenerationBloom,
    GenerationUserIndex,
    Unknown,
}

#[derive(Debug, Clone, Default)]
struct CurrentMemWalInventory {
    objects: BTreeMap<String, (MemWalObjectKind, u64)>,
    generation_roots: BTreeSet<String>,
    referenced_generation_roots: BTreeSet<String>,
}

impl CurrentMemWalInventory {
    fn object_count(&self, kind: MemWalObjectKind) -> usize {
        self.objects
            .values()
            .filter(|(candidate, _)| *candidate == kind)
            .count()
    }

    fn bytes(&self, kind: MemWalObjectKind) -> u64 {
        self.objects
            .values()
            .filter(|(candidate, _)| *candidate == kind)
            .map(|(_, bytes)| *bytes)
            .sum()
    }

    fn immutable_object_bytes(&self) -> u64 {
        self.objects
            .values()
            .filter(|(kind, _)| *kind != MemWalObjectKind::ShardManifestHint)
            .map(|(_, bytes)| *bytes)
            .sum()
    }

    fn unknown_paths(&self) -> Vec<&str> {
        self.objects
            .iter()
            .filter_map(|(path, (kind, _))| {
                (*kind == MemWalObjectKind::Unknown).then_some(path.as_str())
            })
            .collect()
    }

    fn generation_subtree_objects(&self) -> BTreeMap<String, (MemWalObjectKind, u64)> {
        self.objects
            .iter()
            .filter(|(_, (kind, _))| {
                matches!(
                    kind,
                    MemWalObjectKind::GenerationData
                        | MemWalObjectKind::GenerationManifest
                        | MemWalObjectKind::GenerationTransaction
                        | MemWalObjectKind::GenerationDeletion
                        | MemWalObjectKind::GenerationPkSidecar
                        | MemWalObjectKind::GenerationBloom
                        | MemWalObjectKind::GenerationUserIndex
                )
            })
            .map(|(path, value)| (path.clone(), *value))
            .collect()
    }

    fn assert_path_class_size_retained_by(&self, later: &Self) {
        for (path, (kind, bytes)) in &self.objects {
            if *kind == MemWalObjectKind::ShardManifestHint {
                continue;
            }
            assert_eq!(
                later.objects.get(path),
                Some(&(*kind, *bytes)),
                "retain-all evidence lost or changed the class/size of listed object {path}"
            );
        }
    }
}

fn is_generation_root(component: &str) -> bool {
    let Some((hash, generation)) = component.split_once("_gen_") else {
        return false;
    };
    let Ok(generation_number) = generation.parse::<u64>() else {
        return false;
    };
    hash.len() == 8
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && generation_number > 0
        && generation_number.to_string() == generation
}

fn parse_positive_bit_reversed_filename(filename: &str, extension: &str) -> Option<u64> {
    let Some(stem) = filename.strip_suffix(&format!(".{extension}")) else {
        return None;
    };
    if stem.len() != 64 || !stem.bytes().all(|byte| matches!(byte, b'0' | b'1')) {
        return None;
    }
    let reversed = u64::from_str_radix(stem, 2).ok()?;
    let value = reversed.reverse_bits();
    (value > 0).then_some(value)
}

fn parse_canonical_shard_id(value: &str) -> Option<ShardId> {
    let shard_id = ShardId::parse_str(value).ok()?;
    (shard_id.as_hyphenated().to_string() == value).then_some(shard_id)
}

fn join_object_path(base: &str, suffix: &str) -> String {
    if base.is_empty() {
        suffix.to_string()
    } else {
        format!("{base}/{suffix}")
    }
}

fn classify_mem_wal_object(
    components: &[&str],
    mem_wal_index: usize,
) -> (MemWalObjectKind, Option<String>) {
    let tail = &components[mem_wal_index + 1..];
    if tail.len() < 3 || parse_canonical_shard_id(tail[0]).is_none() {
        return (MemWalObjectKind::Unknown, None);
    }

    match tail[1] {
        "wal"
            if tail.len() == 3
                && parse_positive_bit_reversed_filename(tail[2], "arrow").is_some() =>
        {
            (MemWalObjectKind::Wal, None)
        }
        "manifest" if tail.len() == 3 && tail[2] == "version_hint.json" => {
            (MemWalObjectKind::ShardManifestHint, None)
        }
        "manifest"
            if tail.len() == 3
                && parse_positive_bit_reversed_filename(tail[2], "binpb").is_some() =>
        {
            (MemWalObjectKind::ShardManifestVersion, None)
        }
        generation if is_generation_root(generation) => {
            let root = components[..mem_wal_index + 3].join("/");
            let kind = match tail.get(2).copied() {
                Some("bloom_filter.bin") if tail.len() == 3 => MemWalObjectKind::GenerationBloom,
                Some("data") if tail.len() >= 4 => MemWalObjectKind::GenerationData,
                Some("_versions") if tail.len() >= 4 => MemWalObjectKind::GenerationManifest,
                Some("_transactions") if tail.len() >= 4 => MemWalObjectKind::GenerationTransaction,
                Some("_deletions") if tail.len() >= 4 => MemWalObjectKind::GenerationDeletion,
                Some("_pk_index") if tail.len() >= 4 => MemWalObjectKind::GenerationPkSidecar,
                Some("_indices") if tail.len() >= 4 => MemWalObjectKind::GenerationUserIndex,
                _ => MemWalObjectKind::Unknown,
            };
            (kind, Some(root))
        }
        _ => (MemWalObjectKind::Unknown, None),
    }
}

fn validated_referenced_generation_roots(
    table_base: &str,
    directory_shard_id: ShardId,
    manifest: &ShardManifest,
    listed_generation_roots: &BTreeSet<String>,
    listed_manifest_versions: &BTreeSet<u64>,
) -> BTreeSet<String> {
    assert_eq!(
        manifest.shard_id, directory_shard_id,
        "Gate R0 must fail closed when decoded shard authority disagrees with its directory"
    );
    assert!(
        manifest.version > 0,
        "Gate R0 must fail closed on a zero shard-manifest version"
    );
    let latest_listed_version = listed_manifest_versions
        .last()
        .copied()
        .expect("Gate R0 must fail closed when a shard has no listed manifest version");
    assert_eq!(
        latest_listed_version, manifest.version,
        "Gate R0 must fail closed when the latest shard-manifest filename disagrees with its body"
    );
    let expected_manifest_version_count = usize::try_from(manifest.version)
        .expect("Gate R0 must fail closed when shard-manifest authority exceeds usize");
    assert_eq!(
        listed_manifest_versions.len(),
        expected_manifest_version_count,
        "Gate R0 must fail closed on a gapped or noncanonical shard-manifest version chain"
    );
    assert!(
        manifest.current_generation > 0,
        "Gate R0 must fail closed on zero next-generation allocation authority"
    );

    let shard_prefix = join_object_path(
        table_base,
        &format!("_mem_wal/{}", directory_shard_id.as_hyphenated()),
    );
    let mut generations = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut roots = BTreeSet::new();
    for flushed in &manifest.flushed_generations {
        assert!(
            flushed.generation > 0 && generations.insert(flushed.generation),
            "Gate R0 must fail closed on a zero or duplicate flushed generation"
        );
        assert!(
            paths.insert(flushed.path.clone()),
            "Gate R0 must fail closed on a duplicate flushed-generation path"
        );
        assert!(
            is_generation_root(&flushed.path),
            "Gate R0 must fail closed on a noncanonical flushed-generation path"
        );
        let (_, path_generation) = flushed
            .path
            .split_once("_gen_")
            .expect("validated generation root must contain _gen_");
        assert_eq!(
            path_generation.parse::<u64>().unwrap(),
            flushed.generation,
            "Gate R0 must fail closed when generation authority disagrees with its path"
        );
        assert!(
            manifest.current_generation > flushed.generation,
            "Gate R0 must fail closed when next-generation allocation authority has not advanced past a flushed generation"
        );
        let root = join_object_path(&shard_prefix, &flushed.path);
        assert!(
            listed_generation_roots.contains(&root),
            "Gate R0 must fail closed when shard authority references a missing listed generation"
        );
        assert!(
            roots.insert(root),
            "Gate R0 must fail closed on duplicate generation authority"
        );
    }
    roots
}

/// Strict current-object inventory for the part of retained storage that stock
/// Lance exposes through ordinary LIST. This deliberately does not pretend to
/// see incomplete multipart uploads, superseded provider versions, delete
/// markers, local staged temp files, or billed storage.
async fn current_mem_wal_inventory(uri: &str) -> CurrentMemWalInventory {
    let (store, root_path) = lance_io::object_store::ObjectStore::from_uri(uri)
        .await
        .expect("Gate R0 fixture URI must resolve");
    let listed = store
        .inner
        .list(Some(&root_path))
        .try_collect::<Vec<_>>()
        .await
        .expect("Gate R0 inventory must fail closed on a listing error");

    let mut inventory = CurrentMemWalInventory::default();
    let mut shards = BTreeSet::new();
    let mut manifest_versions = BTreeMap::<(String, ShardId), BTreeSet<u64>>::new();
    for metadata in listed {
        let path = metadata.location.as_ref().to_string();
        let components = path.split('/').collect::<Vec<_>>();
        let Some(mem_wal_index) = components
            .iter()
            .position(|component| *component == "_mem_wal")
        else {
            continue;
        };
        let (kind, generation_root) = classify_mem_wal_object(&components, mem_wal_index);
        if let Some(root) = generation_root {
            inventory.generation_roots.insert(root);
        }
        let bytes = u64::try_from(metadata.size).expect("object size must fit u64");
        assert!(
            inventory
                .objects
                .insert(path.clone(), (kind, bytes))
                .is_none(),
            "object listing returned duplicate path {path}"
        );

        let tail = &components[mem_wal_index + 1..];
        if let Some(shard) = tail
            .first()
            .and_then(|value| parse_canonical_shard_id(value))
        {
            let table_base = components[..mem_wal_index].join("/");
            shards.insert((table_base.clone(), shard));
            if kind == MemWalObjectKind::ShardManifestVersion {
                let version = parse_positive_bit_reversed_filename(tail[2], "binpb")
                    .expect("classified shard-manifest version must decode");
                assert!(
                    manifest_versions
                        .entry((table_base, shard))
                        .or_default()
                        .insert(version),
                    "object listing returned duplicate decoded shard-manifest version"
                );
            }
        }
    }

    for (table_base, shard_id) in shards {
        let table_path =
            ObjectPath::parse(&table_base).expect("listed table base must remain a valid path");
        let manifest_store = ShardManifestStore::new(Arc::clone(&store), &table_path, shard_id, 2);
        let manifest = manifest_store
            .read_latest()
            .await
            .expect("Gate R0 must fail closed on an unreadable shard manifest")
            .expect("Gate R0 must fail closed when a listed shard has no latest manifest");
        let listed_manifest_versions = manifest_versions
            .get(&(table_base.clone(), shard_id))
            .expect("Gate R0 must fail closed when a shard has no listed manifest version");
        inventory
            .referenced_generation_roots
            .extend(validated_referenced_generation_roots(
                &table_base,
                shard_id,
                &manifest,
                &inventory.generation_roots,
                listed_manifest_versions,
            ));
    }

    inventory
}

async fn current_recovery_sidecars(uri: &str) -> Vec<String> {
    let (store, root_path) = lance_io::object_store::ObjectStore::from_uri(uri)
        .await
        .expect("Gate R0 fixture URI must resolve");
    let mut paths = store
        .inner
        .list(Some(&root_path))
        .try_collect::<Vec<_>>()
        .await
        .expect("Gate R0 recovery inventory must fail closed on a listing error")
        .into_iter()
        .map(|metadata| metadata.location.as_ref().to_string())
        .filter(|path| path.split('/').any(|component| component == "__recovery"))
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[derive(Debug, Clone, Copy, Default)]
struct PathIo {
    total_reads: u64,
    total_read_bytes: u64,
    total_writes: u64,
    total_written_bytes: u64,
    wal_reads: u64,
    wal_writes: u64,
    shard_manifest_reads: u64,
    shard_manifest_writes: u64,
    generation_reads: u64,
    generation_writes: u64,
    generation_data_reads: u64,
    generation_data_writes: u64,
    pk_sidecar_reads: u64,
    pk_sidecar_writes: u64,
}

impl PathIo {
    fn from_stats(stats: &IoStats) -> Self {
        let mut out = Self {
            total_reads: stats.read_iops,
            total_read_bytes: stats.read_bytes,
            total_writes: stats.write_iops,
            total_written_bytes: stats.written_bytes,
            ..Self::default()
        };
        for request in &stats.requests {
            let path = request.path.as_ref();
            let write = matches!(
                request.method,
                "put_opts" | "put_part" | "copy" | "rename" | "delete"
            );
            let read = !write;
            let memwal = path.contains("_mem_wal/");
            let wal = memwal && (path.contains("/wal/") || path.ends_with("/wal"));
            let shard_manifest =
                memwal && (path.contains("/manifest/") || path.ends_with("/manifest"));
            let generation = memwal && path.contains("_gen_");
            let generation_data = generation
                && (path.contains("/data/") || path.ends_with("/data") || path.ends_with(".lance"));
            let pk_sidecar = generation && path.contains("/_pk_index/");
            if read && wal {
                out.wal_reads += 1;
            }
            if write && wal {
                out.wal_writes += 1;
            }
            if read && shard_manifest {
                out.shard_manifest_reads += 1;
            }
            if write && shard_manifest {
                out.shard_manifest_writes += 1;
            }
            if read && generation {
                out.generation_reads += 1;
            }
            if write && generation {
                out.generation_writes += 1;
            }
            if read && generation_data {
                out.generation_data_reads += 1;
            }
            if write && generation_data {
                out.generation_data_writes += 1;
            }
            if read && pk_sidecar {
                out.pk_sidecar_reads += 1;
            }
            if write && pk_sidecar {
                out.pk_sidecar_writes += 1;
            }
        }
        out
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AdapterIo {
    read_text: u64,
    read_text_if_exists: u64,
    exists: u64,
    read_text_versioned: u64,
    list_dir: u64,
    write_text: u64,
    delete: u64,
}

impl AdapterIo {
    fn snapshot(counts: &StorageReadCounts) -> Self {
        Self {
            read_text: counts.read_text(),
            read_text_if_exists: counts.read_text_if_exists(),
            exists: counts.exists(),
            read_text_versioned: counts.read_text_versioned(),
            list_dir: counts.list_dir(),
            write_text: counts.write_text(),
            delete: counts.delete(),
        }
    }

    fn since(self, before: Self) -> Self {
        Self {
            read_text: self.read_text.saturating_sub(before.read_text),
            read_text_if_exists: self
                .read_text_if_exists
                .saturating_sub(before.read_text_if_exists),
            exists: self.exists.saturating_sub(before.exists),
            read_text_versioned: self
                .read_text_versioned
                .saturating_sub(before.read_text_versioned),
            list_dir: self.list_dir.saturating_sub(before.list_dir),
            write_text: self.write_text.saturating_sub(before.write_text),
            delete: self.delete.saturating_sub(before.delete),
        }
    }

    fn operations(self) -> u64 {
        self.read_text
            + self.read_text_if_exists
            + self.exists
            + self.read_text_versioned
            + self.list_dir
            + self.write_text
            + self.delete
    }
}

#[derive(Debug)]
struct HistorySample {
    depth: u64,
    ack_elapsed_us: u128,
    ack_table: PathIo,
    ack_manifest: PathIo,
    ack_adapter: AdapterIo,
    fold_elapsed_us: u128,
    fold_table: PathIo,
    fold_manifest: PathIo,
}

#[derive(Debug)]
struct ColdReplaySample {
    retained_batches: u64,
    elapsed_us: u128,
    table: PathIo,
}

#[derive(Debug)]
struct FoldRowsSample {
    rows: usize,
    elapsed_us: u128,
    table: PathIo,
    physical_generation_data_bytes: u64,
}

#[derive(Debug)]
struct RetainedMetadataSample {
    merged_generations: u64,
    serialized_manifest_bytes: u64,
    cold_elapsed_us: u128,
    table: PathIo,
    inventory: CurrentMemWalInventory,
}

#[derive(Debug)]
struct WidestRetainedGrowthSample {
    before: CurrentMemWalInventory,
    after_ack: CurrentMemWalInventory,
    after_fold: CurrentMemWalInventory,
    table_ack: PathIo,
    table_fold: PathIo,
    manifest_version_before: u64,
    manifest_version_after_ack: u64,
    manifest_version_after_fold: u64,
    table_version_before: u64,
    table_version_after_ack: u64,
    table_version_after_fold: u64,
    visible_rows: usize,
    sampled_payloads: Vec<(String, String)>,
    recovery_sidecars_after_fold: Vec<String>,
}

async fn initialize_history(uri: &str, depth: u64, compact_before_enrollment: bool) {
    let db = Omnigraph::init(uri, STREAM_SCHEMA).await.unwrap();
    for tick in 0..depth {
        db.mutate(
            "main",
            ADD_HISTORY,
            "add_history",
            &helpers::int_params(&[("$tick", tick as i64)]),
        )
        .await
        .unwrap();
    }
    if compact_before_enrollment {
        db.optimize()
            .await
            .expect("normalize internal/data fragments before the flat ack measurement");
    }
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
}

async fn init_enrolled(uri: &str, schema: &str) {
    let db = Omnigraph::init(uri, schema).await.unwrap();
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
}

async fn table_schema(db: &Omnigraph) -> Arc<Schema> {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(TABLE).await.unwrap();
    Arc::new(Schema::from(table.schema()))
}

fn score_batch(schema: Arc<Schema>, id_prefix: &str, rows: usize) -> RecordBatch {
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "score"]
    );
    let ids = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("{id_prefix}-{row:05}")),
    )) as ArrayRef;
    let scores = Arc::new(Int32Array::from_iter_values(
        (0..rows).map(|row| i32::try_from(row).unwrap()),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, scores]).unwrap()
}

async fn payload_batch(db: &Omnigraph, rows: usize, payload_bytes: usize) -> RecordBatch {
    let schema = table_schema(db).await;
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "payload"]
    );
    let ids = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("rss-{row:05}")),
    )) as ArrayRef;
    let payload = "x".repeat(payload_bytes);
    let values = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| payload.as_str()),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, values]).unwrap()
}

async fn history_sample_at_uri(
    uri: &str,
    depth: u64,
    compact_before_enrollment: bool,
) -> HistorySample {
    initialize_history(uri, depth, compact_before_enrollment).await;

    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let (adapter, adapter_counts) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
        let db = Arc::new(Omnigraph::open_with_storage(uri, adapter).await.unwrap());
        let schema = table_schema(&db).await;
        let first = score_batch(Arc::clone(&schema), &format!("d{depth}-warmup"), 1);
        let measured = score_batch(schema, &format!("d{depth}-measured"), 1);

        // This first put performs the cold claim.  The tracker is deliberately
        // persistent across it and the measured put: the warm writer carries
        // the object-store wrapper it acquired when it was opened.
        db.failpoint_stream_b1_for_test(TABLE, Some(first), 0)
            .await
            .unwrap();
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();
        let adapter_before = AdapterIo::snapshot(&adapter_counts);

        let ack_started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, Some(measured), 1)
            .await
            .unwrap();
        let ack_elapsed_us = ack_started.elapsed().as_micros();
        let ack_table = PathIo::from_stats(&table_tracker.incremental_stats());
        let ack_manifest = PathIo::from_stats(&manifest_tracker.incremental_stats());
        let ack_adapter = AdapterIo::snapshot(&adapter_counts).since(adapter_before);

        let fold_started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .unwrap();
        let fold_elapsed_us = fold_started.elapsed().as_micros();
        let fold_table = PathIo::from_stats(&table_tracker.incremental_stats());
        let fold_manifest = PathIo::from_stats(&manifest_tracker.incremental_stats());

        HistorySample {
            depth,
            ack_elapsed_us,
            ack_table,
            ack_manifest,
            ack_adapter,
            fold_elapsed_us,
            fold_table,
            fold_manifest,
        }
    })
    .await
}

async fn local_history_sample(depth: u64, compact: bool) -> HistorySample {
    let dir = tempfile::tempdir().unwrap();
    history_sample_at_uri(dir.path().to_str().unwrap(), depth, compact).await
}

fn assert_warm_ack_evidence(samples: &[HistorySample], backend: &str) {
    assert!(samples.len() >= 2);
    for sample in samples {
        eprintln!(
            "B1 warm ack {backend} depth={}: elapsed_us={} table_reads={} table_read_bytes={} table_writes={} table_written_bytes={} wal_writes={} manifest_reads={} adapter_ops={}",
            sample.depth,
            sample.ack_elapsed_us,
            sample.ack_table.total_reads,
            sample.ack_table.total_read_bytes,
            sample.ack_table.total_writes,
            sample.ack_table.total_written_bytes,
            sample.ack_table.wal_writes,
            sample.ack_manifest.total_reads,
            sample.ack_adapter.operations(),
        );
        assert!(
            sample.ack_table.wal_writes > 0,
            "warm ack recorded no WAL write; detached-task probe propagation regressed: {sample:?}"
        );
        assert_eq!(
            sample.ack_table.generation_writes, 0,
            "ack must not flush/roll a generation"
        );
        assert_eq!(
            sample.ack_manifest.total_writes, 0,
            "ack must not publish graph visibility"
        );
    }
    let shallow = &samples[0];
    let deep = &samples[samples.len() - 1];
    assert!(
        deep.ack_table.total_reads <= shallow.ack_table.total_reads + 2,
        "warm ack table reads grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_table.total_writes <= shallow.ack_table.total_writes + 2,
        "warm ack table writes grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_manifest.total_reads <= shallow.ack_manifest.total_reads + 4,
        "warm ack graph-authority reads grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_adapter.operations() <= shallow.ack_adapter.operations() + 2,
        "warm ack schema/sidecar adapter operations grew with graph history: {samples:#?}"
    );
}

#[tokio::test]
#[serial]
async fn warm_already_claimed_ack_is_flat_in_compacted_graph_history_local() {
    let mut samples = Vec::new();
    for depth in [8, 80] {
        samples.push(local_history_sample(depth, true).await);
    }
    assert_warm_ack_evidence(&samples, "local");
}

#[tokio::test]
#[serial]
async fn warm_already_claimed_ack_is_measured_on_configured_rustfs() {
    let Some(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok() else {
        eprintln!("SKIP: OMNIGRAPH_S3_TEST_BUCKET not set; RustFS B1 ack cost not measured");
        return;
    };

    let mut samples = Vec::new();
    for depth in [8, 80] {
        let uri = format!(
            "s3://{bucket}/cost-tests/memwal-b1-ack-{}-{depth}",
            ulid::Ulid::new()
        );
        let sample = history_sample_at_uri(&uri, depth, true).await;
        storage_for_uri(&uri)
            .unwrap()
            .delete_prefix(&uri)
            .await
            .expect("configured RustFS cleanup failed");
        samples.push(sample);
    }
    assert_warm_ack_evidence(&samples, "rustfs");
}

async fn cold_replay_sample(retained_batches: u64) -> ColdReplaySample {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    init_enrolled(uri, STREAM_SCHEMA).await;

    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let (adapter, _) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
        let mut db = Arc::new(
            Omnigraph::open_with_storage(uri, Arc::clone(&adapter))
                .await
                .unwrap(),
        );
        let schema = table_schema(&db).await;
        for ordinal in 0..retained_batches {
            let batch = score_batch(
                Arc::clone(&schema),
                &format!("cold-{retained_batches}-{ordinal}"),
                1,
            );
            if ordinal + 1 == retained_batches {
                let error = {
                    let _after_durable =
                        ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
                    db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                        .await
                        .expect_err("the final durable entry retires the warm writer")
                };
                assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
            } else {
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                    .await
                    .unwrap();
            }
        }

        // Reopen the graph, then reset: the measured interval starts at the
        // stream's cold claim/replay, not at generic graph open.
        drop(db);
        db = Arc::new(Omnigraph::open_with_storage(uri, adapter).await.unwrap());
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        let started = Instant::now();
        let error = {
            let _before_seal = ScopedFailPoint::new(names::STREAM_B1_BEFORE_FORCE_SEAL, "return");
            db.failpoint_stream_b1_for_test(TABLE, None, 0)
                .await
                .expect_err("stop after cold claim/replay and before generation output")
        };
        assert!(
            error
                .to_string()
                .contains(names::STREAM_B1_BEFORE_FORCE_SEAL),
            "{error:?}"
        );
        let elapsed_us = started.elapsed().as_micros();
        let table = PathIo::from_stats(&table_tracker.incremental_stats());

        // Complete the same durable tail so each fixture leaves no live worker.
        let _ = manifest_tracker.incremental_stats();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .unwrap();
        ColdReplaySample {
            retained_batches,
            elapsed_us,
            table,
        }
    })
    .await
}

#[tokio::test]
#[serial]
async fn cold_claim_replay_is_reported_separately_by_retained_wal_depth() {
    let _scenario = FailScenario::setup();
    let mut samples = Vec::new();
    for retained_batches in [1, 8, 32] {
        let sample = cold_replay_sample(retained_batches).await;
        eprintln!(
            "B1 cold replay retained_batches={}: elapsed_us={} wal_reads={} table_reads={} aggregate_table_read_bytes={} generation_writes={}",
            sample.retained_batches,
            sample.elapsed_us,
            sample.table.wal_reads,
            sample.table.total_reads,
            sample.table.total_read_bytes,
            sample.table.generation_writes,
        );
        assert!(
            sample.table.wal_reads > 0,
            "cold claim did not observe retained WAL: {sample:?}"
        );
        assert_eq!(
            sample.table.generation_writes, 0,
            "pre-seal cold replay measurement must exclude generation output"
        );
        samples.push(sample);
    }
    assert!(
        samples.last().unwrap().table.total_read_bytes
            > samples.first().unwrap().table.total_read_bytes,
        "aggregate cold claim/replay bytes must expose retained-WAL depth: {samples:#?}"
    );
}

async fn fold_rows_sample(rows: usize) -> FoldRowsSample {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    init_enrolled(uri, STREAM_SCHEMA).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let db = Arc::new(Omnigraph::open(uri).await.unwrap());
        let schema = table_schema(&db).await;
        let batch = score_batch(schema, &format!("fold-{rows}"), rows);
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .unwrap();
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        let started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .unwrap();
        let elapsed_us = started.elapsed().as_micros();
        let table = PathIo::from_stats(&table_tracker.incremental_stats());
        assert_eq!(
            helpers::count_rows(&db, TABLE).await,
            rows,
            "the measured fold must select exactly this generation's rows"
        );
        FoldRowsSample {
            rows,
            elapsed_us,
            table,
            physical_generation_data_bytes: generation_data_file_bytes(dir.path()),
        }
    })
    .await
}

#[tokio::test]
#[serial]
async fn one_generation_fold_data_scan_tracks_selected_rows() {
    let mut samples = Vec::new();
    for rows in [1, 4_096] {
        let sample = fold_rows_sample(rows).await;
        eprintln!(
            "B1 fold selected_rows={}: elapsed_us={} generation_data_reads={} generation_reads={} observed_range_read_bytes={} physical_generation_data_bytes={} pk_sidecar_reads={}",
            sample.rows,
            sample.elapsed_us,
            sample.table.generation_data_reads,
            sample.table.generation_reads,
            sample.table.total_read_bytes,
            sample.physical_generation_data_bytes,
            sample.table.pk_sidecar_reads,
        );
        assert!(
            sample.table.generation_data_reads > 0,
            "fold recorded no selected-generation data read; detached-task probe propagation regressed: {sample:?}"
        );
        samples.push(sample);
    }
    assert!(
        samples[1].physical_generation_data_bytes > samples[0].physical_generation_data_bytes,
        "the selected generation's physical data must expose row volume: {samples:#?}"
    );
}

#[tokio::test]
#[serial]
async fn fold_keeps_known_uncompacted_graph_manifest_history_term_visible() {
    let mut samples = Vec::new();
    for depth in [8, 80] {
        let sample = local_history_sample(depth, false).await;
        eprintln!(
            "B1 fold uncompacted_history={}: elapsed_us={} graph_manifest_reads={} graph_manifest_read_bytes={} generation_data_reads={}",
            sample.depth,
            sample.fold_elapsed_us,
            sample.fold_manifest.total_reads,
            sample.fold_manifest.total_read_bytes,
            sample.fold_table.generation_data_reads,
        );
        assert!(sample.fold_table.generation_data_reads > 0, "{sample:?}");
        samples.push(sample);
    }
    assert!(
        samples[1].fold_manifest.total_reads > samples[0].fold_manifest.total_reads
            || samples[1].fold_manifest.total_read_bytes
                > samples[0].fold_manifest.total_read_bytes,
        "the shared fold authority/publisher's known uncompacted graph-manifest term disappeared; do not replace it with a false history-flat claim: {samples:#?}"
    );
}

fn max_shard_manifest_bytes(root: &Path) -> u64 {
    fn walk(path: &Path, max_bytes: &mut u64) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(&path, max_bytes);
                continue;
            }
            let is_shard_manifest = path.extension().is_some_and(|ext| ext == "binpb")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "_mem_wal")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "manifest");
            if is_shard_manifest {
                *max_bytes = (*max_bytes).max(entry.metadata().unwrap().len());
            }
        }
    }

    let mut max_bytes = 0;
    walk(root, &mut max_bytes);
    assert!(
        max_bytes > 0,
        "no local shard manifest found under {root:?}"
    );
    max_bytes
}

fn generation_data_file_bytes(root: &Path) -> u64 {
    fn walk(path: &Path, total: &mut u64) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(&path, total);
                continue;
            }
            let in_generation = path
                .components()
                .any(|component| component.as_os_str().to_string_lossy().contains("_gen_"));
            let in_data = path
                .components()
                .any(|component| component.as_os_str() == "data");
            if in_generation && in_data {
                *total = total.saturating_add(entry.metadata().unwrap().len());
            }
        }
    }

    let mut total = 0;
    walk(root, &mut total);
    assert!(total > 0, "no flushed-generation data found under {root:?}");
    total
}

#[tokio::test]
#[serial]
async fn gate_r0_retain_all_current_object_growth_is_swept_explicitly() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    init_enrolled(uri, STREAM_SCHEMA).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();

    let samples = with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let db = Arc::new(Omnigraph::open(uri).await.unwrap());
        let schema = table_schema(&db).await;
        let mut completed = 0u64;
        let mut ordinal = 0u64;
        let mut samples = Vec::new();

        for checkpoint in [1u64, 4, 8] {
            while completed < checkpoint {
                let batch = score_batch(
                    Arc::clone(&schema),
                    &format!("metadata-normal-{completed}"),
                    1,
                );
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                    .await
                    .unwrap();
                ordinal += 1;
                db.failpoint_stream_b1_for_test(TABLE, None, 0)
                    .await
                    .unwrap();
                completed += 1;
            }

            let serialized_manifest_bytes = max_shard_manifest_bytes(dir.path());
            let inventory = current_mem_wal_inventory(uri).await;
            let pending = score_batch(
                Arc::clone(&schema),
                &format!("metadata-measured-{checkpoint}"),
                1,
            );
            let error = {
                let _after_durable =
                    ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
                db.failpoint_stream_b1_for_test(TABLE, Some(pending), ordinal)
                    .await
                    .expect_err("retire the writer so the metadata probe is a cold claim")
            };
            assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
            ordinal += 1;
            let _ = table_tracker.incremental_stats();
            let _ = manifest_tracker.incremental_stats();

            let started = Instant::now();
            let error = {
                let _before_seal =
                    ScopedFailPoint::new(names::STREAM_B1_BEFORE_FORCE_SEAL, "return");
                db.failpoint_stream_b1_for_test(TABLE, None, 0)
                    .await
                    .expect_err("measure retained metadata before generation output")
            };
            assert!(
                error
                    .to_string()
                    .contains(names::STREAM_B1_BEFORE_FORCE_SEAL),
                "{error:?}"
            );
            samples.push(RetainedMetadataSample {
                merged_generations: checkpoint,
                serialized_manifest_bytes,
                cold_elapsed_us: started.elapsed().as_micros(),
                table: PathIo::from_stats(&table_tracker.incremental_stats()),
                inventory,
            });

            let _ = manifest_tracker.incremental_stats();
            db.failpoint_stream_b1_for_test(TABLE, None, 0)
                .await
                .unwrap();
            completed += 1;
        }
        samples
    })
    .await;

    for (index, sample) in samples.iter().enumerate() {
        eprintln!(
            "B1 retained merged_generations={}: largest_retained_manifest_bytes={} current_listed_immutable_bytes={} current_listed_objects={} generation_roots={} manifest_version_objects={} cold_elapsed_us={} shard_manifest_reads={} aggregate_table_read_bytes={}",
            sample.merged_generations,
            sample.serialized_manifest_bytes,
            sample.inventory.immutable_object_bytes(),
            sample.inventory.objects.len(),
            sample.inventory.generation_roots.len(),
            sample
                .inventory
                .object_count(MemWalObjectKind::ShardManifestVersion),
            sample.cold_elapsed_us,
            sample.table.shard_manifest_reads,
            sample.table.total_read_bytes,
        );
        assert!(sample.table.shard_manifest_reads > 0, "{sample:?}");
        assert_eq!(
            sample.inventory.generation_roots.len(),
            usize::try_from(sample.merged_generations).unwrap(),
            "one no-roll B1 fold must retain one generation root"
        );
        assert_eq!(
            sample.inventory.generation_roots, sample.inventory.referenced_generation_roots,
            "the success-only sweep must not confuse referenced generations with crash orphans"
        );
        assert!(
            sample.inventory.unknown_paths().is_empty(),
            "current-object census missed paths: {:?}",
            sample.inventory.unknown_paths()
        );
        if index > 0 {
            samples[index - 1]
                .inventory
                .assert_path_class_size_retained_by(&sample.inventory);
        }
    }
    assert!(
        samples.last().unwrap().serialized_manifest_bytes
            > samples.first().unwrap().serialized_manifest_bytes,
        "B1 intentionally retains merged generation entries; the serialized shard metadata must expose that growth: {samples:#?}"
    );
    assert!(
        samples.last().unwrap().inventory.immutable_object_bytes()
            > samples.first().unwrap().inventory.immutable_object_bytes(),
        "retain-all physical objects must expose cumulative growth: {samples:#?}"
    );
    assert!(
        samples
            .last()
            .unwrap()
            .inventory
            .object_count(MemWalObjectKind::ShardManifestVersion)
            > samples
                .first()
                .unwrap()
                .inventory
                .object_count(MemWalObjectKind::ShardManifestVersion),
        "versioned shard authority is retained growth too: {samples:#?}"
    );
}

fn classify_test_path(path: &str) -> MemWalObjectKind {
    let components = path.split('/').collect::<Vec<_>>();
    let mem_wal_index = components
        .iter()
        .position(|component| *component == "_mem_wal")
        .expect("test path must contain _mem_wal");
    classify_mem_wal_object(&components, mem_wal_index).0
}

fn bit_reversed_stem(value: u64) -> String {
    format!("{:064b}", value.reverse_bits())
}

fn gate_r0_manifest(
    shard_id: ShardId,
    flushed_generations: Vec<FlushedGeneration>,
) -> ShardManifest {
    ShardManifest {
        shard_id,
        version: 1,
        shard_spec_id: 0,
        shard_field_values: Default::default(),
        writer_epoch: 1,
        replay_after_wal_entry_position: 0,
        wal_entry_position_last_seen: 0,
        current_generation: flushed_generations
            .iter()
            .map(|generation| generation.generation)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .expect("Gate R0 fixture generation must leave successor authority"),
        flushed_generations,
        status: ShardStatus::Active,
    }
}

#[test]
fn gate_r0_current_inventory_path_classifier_is_canonical_and_fail_closed() {
    let shard = "abcdefab-cdef-abcd-efab-cdefabcdefab";
    let bits = bit_reversed_stem(1);
    assert_eq!(
        classify_test_path(&format!("table/_mem_wal/{shard}/wal/{bits}.arrow")),
        MemWalObjectKind::Wal
    );
    assert_eq!(
        classify_test_path(&format!("table/_mem_wal/{shard}/manifest/{bits}.binpb")),
        MemWalObjectKind::ShardManifestVersion
    );
    assert_eq!(
        classify_test_path(&format!(
            "table/_mem_wal/{shard}/abcdef12_gen_1/data/file.lance"
        )),
        MemWalObjectKind::GenerationData
    );

    let malformed = [
        format!("table/_mem_wal/{shard}/wal/{}.arrow", "0".repeat(63)),
        format!("table/_mem_wal/{shard}/wal/{}.arrow", bit_reversed_stem(0)),
        format!("table/_mem_wal/{shard}/wal/{bits}.arrow.extra"),
        format!("table/_mem_wal/{shard}/manifest/not-a-version.binpb"),
        format!(
            "table/_mem_wal/{}/wal/{bits}.arrow",
            shard.to_ascii_uppercase()
        ),
        format!("table/_mem_wal/{shard}/ABCDEF12_gen_1/data/file.lance"),
        format!("table/_mem_wal/{shard}/abcdef12_gen_0/data/file.lance"),
        format!("table/_mem_wal/{shard}/abcdef12_gen_01/data/file.lance"),
    ];
    for path in malformed {
        assert_eq!(
            classify_test_path(&path),
            MemWalObjectKind::Unknown,
            "malformed path must fail closed: {path}"
        );
    }
}

#[test]
#[should_panic(expected = "decoded shard authority disagrees with its directory")]
fn gate_r0_current_inventory_refuses_manifest_for_another_shard() {
    let directory_shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let foreign_shard = ShardId::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
    let manifest = gate_r0_manifest(foreign_shard, Vec::new());
    let _ = validated_referenced_generation_roots(
        "table",
        directory_shard,
        &manifest,
        &BTreeSet::new(),
        &BTreeSet::from([1]),
    );
}

#[test]
#[should_panic(expected = "zero or duplicate flushed generation")]
fn gate_r0_current_inventory_refuses_duplicate_generation_authority() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let generation = FlushedGeneration {
        generation: 1,
        path: "abcdef12_gen_1".to_string(),
    };
    let manifest = gate_r0_manifest(shard, vec![generation.clone(), generation]);
    let listed = BTreeSet::from([format!(
        "table/_mem_wal/{}/abcdef12_gen_1",
        shard.as_hyphenated()
    )]);
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &listed,
        &BTreeSet::from([1]),
    );
}

#[test]
#[should_panic(expected = "zero next-generation allocation authority")]
fn gate_r0_current_inventory_refuses_zero_current_generation() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let mut manifest = gate_r0_manifest(shard, Vec::new());
    manifest.current_generation = 0;
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &BTreeSet::new(),
        &BTreeSet::from([1]),
    );
}

#[test]
#[should_panic(expected = "allocation authority has not advanced past a flushed generation")]
fn gate_r0_current_inventory_refuses_non_advanced_current_generation() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let generation = FlushedGeneration {
        generation: 1,
        path: "abcdef12_gen_1".to_string(),
    };
    let mut manifest = gate_r0_manifest(shard, vec![generation]);
    manifest.current_generation = 1;
    let listed = BTreeSet::from([format!(
        "table/_mem_wal/{}/abcdef12_gen_1",
        shard.as_hyphenated()
    )]);
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &listed,
        &BTreeSet::from([1]),
    );
}

#[test]
#[should_panic(expected = "latest shard-manifest filename disagrees with its body")]
fn gate_r0_current_inventory_refuses_manifest_filename_body_version_mismatch() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let manifest = gate_r0_manifest(shard, Vec::new());
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &BTreeSet::new(),
        &BTreeSet::from([2]),
    );
}

#[test]
#[should_panic(expected = "gapped or noncanonical shard-manifest version chain")]
fn gate_r0_current_inventory_refuses_gapped_manifest_version_chain() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let mut manifest = gate_r0_manifest(shard, Vec::new());
    manifest.version = 3;
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &BTreeSet::new(),
        &BTreeSet::from([1, 3]),
    );
}

#[tokio::test]
#[should_panic(expected = "listed shard has no latest manifest")]
async fn gate_r0_current_inventory_refuses_listed_shard_without_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let shard = "abcdefab-cdef-abcd-efab-cdefabcdefab";
    let wal_dir = dir.path().join("_mem_wal").join(shard).join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::write(
        wal_dir.join(format!("{}.arrow", bit_reversed_stem(1))),
        b"x",
    )
    .unwrap();
    let _ = current_mem_wal_inventory(dir.path().to_str().unwrap()).await;
}

#[test]
fn gate_r0_source_audit_revision_tripwire_matches_surveyed_lance() {
    let expected_source = format!(
        "git+https://github.com/lance-format/lance?rev={SURVEYED_LANCE_REV}#{SURVEYED_LANCE_REV}"
    );
    let expected_lance_repository_packages = BTreeSet::from([
        "fsst",
        "lance",
        "lance-arrow",
        "lance-arrow-scalar",
        "lance-arrow-stats",
        "lance-bitpacking",
        "lance-core",
        "lance-datafusion",
        "lance-datagen",
        "lance-derive",
        "lance-encoding",
        "lance-file",
        "lance-index",
        "lance-index-core",
        "lance-io",
        "lance-linalg",
        "lance-namespace",
        "lance-namespace-impls",
        "lance-select",
        "lance-table",
        "lance-tokenizer",
    ]);
    let mut observed = BTreeMap::<&str, Vec<&str>>::new();
    for package in include_str!("../../../Cargo.lock")
        .split("[[package]]")
        .skip(1)
    {
        let value = |key: &str| {
            package.lines().find_map(|line| {
                line.strip_prefix(&format!("{key} = \""))
                    .and_then(|value| value.strip_suffix('"'))
            })
        };
        let Some(name) = value("name") else {
            continue;
        };
        let Some(source) = value("source") else {
            continue;
        };
        if source.starts_with("git+https://github.com/lance-format/lance?") {
            assert_eq!(
                source, expected_source,
                "Lance-repository package {name} moved or became mixed; Gate R0's source audit must be rerun"
            );
            observed.entry(name).or_default().push(source);
        }
    }
    assert_eq!(
        observed.keys().copied().collect::<BTreeSet<_>>(),
        expected_lance_repository_packages,
        "the exact audited Lance-repository package family changed; Gate R0's source audit must be rerun"
    );
    for package in &expected_lance_repository_packages {
        assert_eq!(
            observed.get(package).map(Vec::as_slice),
            Some([expected_source.as_str()].as_slice()),
            "audited Lance-repository package {package} moved, disappeared, or became mixed; Gate R0's source audit must be rerun"
        );
    }
    let assert_direct_manifest_pin = |manifest: &str, package: &str| {
        let prefix = format!("{package} = ");
        let lines = manifest
            .lines()
            .filter(|line| line.starts_with(&prefix))
            .collect::<Vec<_>>();
        assert_eq!(
            lines.len(),
            1,
            "Gate R0 must find exactly one direct source declaration for {package}"
        );
        assert!(
            lines[0].contains("git = \"https://github.com/lance-format/lance\"")
                && lines[0].contains(&format!("rev = \"{SURVEYED_LANCE_REV}\"")),
            "direct Lance manifest pin moved without rerunning Gate R0: {}",
            lines[0]
        );
    };
    let workspace_manifest = include_str!("../../../Cargo.toml");
    for package in [
        "lance",
        "lance-core",
        "lance-select",
        "lance-datafusion",
        "lance-file",
        "lance-index",
        "lance-linalg",
        "lance-namespace",
        "lance-namespace-impls",
        "lance-table",
    ] {
        assert_direct_manifest_pin(workspace_manifest, package);
    }
    assert_direct_manifest_pin(include_str!("../Cargo.toml"), "lance-io");
}

#[tokio::test]
#[serial]
async fn gate_r0_referenced_cut_retry_reuses_the_same_generation_root_local() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    init_enrolled(uri, STREAM_SCHEMA).await;
    let db = Arc::new(Omnigraph::open(uri).await.unwrap());
    let batch = score_batch(table_schema(&db).await, "r0-retry", 1);
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _after_drain =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("stop after Lance published the generation cut")
    };
    assert!(
        error
            .to_string()
            .contains(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR),
        "{error:?}"
    );
    let after_failed_fold = current_mem_wal_inventory(uri).await;
    assert_eq!(after_failed_fold.generation_roots.len(), 1);
    assert_eq!(
        after_failed_fold.generation_roots,
        after_failed_fold.referenced_generation_roots
    );
    assert!(
        after_failed_fold.unknown_paths().is_empty(),
        "{after_failed_fold:#?}"
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    let after_retry = current_mem_wal_inventory(uri).await;
    assert_eq!(
        after_retry.generation_roots, after_failed_fold.generation_roots,
        "a retry after the referenced cut must fold-only, not materialize another random root"
    );
    assert!(after_retry.unknown_paths().is_empty(), "{after_retry:#?}");
    assert_eq!(
        after_retry.generation_roots,
        after_retry.referenced_generation_roots
    );
    assert_eq!(
        after_retry.generation_subtree_objects(),
        after_failed_fold.generation_subtree_objects(),
        "fold-only retry must not add, replace, or resize any generation-subtree object"
    );
    after_failed_fold.assert_path_class_size_retained_by(&after_retry);
    assert_eq!(helpers::count_rows(&db, TABLE).await, 1);
}

fn deterministic_high_entropy_ascii(row: usize, len: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = (row as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut bytes = Vec::with_capacity(len);
    for ordinal in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state = state.wrapping_add((ordinal as u64).wrapping_mul(0xd6e8_feb8_6659_fd93));
        bytes.push(ALPHABET[(state as usize) & (ALPHABET.len() - 1)]);
    }
    String::from_utf8(bytes).unwrap()
}

async fn widest_high_entropy_payload_batch(db: &Omnigraph) -> RecordBatch {
    let schema = table_schema(db).await;
    let ids = Arc::new(StringArray::from_iter_values(
        (0..WIDEST_ROWS).map(|row| format!("rss-{row:05}")),
    )) as ArrayRef;
    let values =
        Arc::new(StringArray::from_iter_values((0..WIDEST_ROWS).map(|row| {
            deterministic_high_entropy_ascii(row, WIDEST_PAYLOAD_BYTES)
        }))) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![ids, values]).unwrap();
    let mut stored_columns = batch.columns().to_vec();
    stored_columns.push(Arc::new(BooleanArray::from(vec![false; WIDEST_ROWS])) as ArrayRef);
    let stored = RecordBatch::try_new(
        schema_with_tombstone(batch.schema().as_ref()),
        stored_columns,
    )
    .unwrap();
    assert!(
        stored.get_array_memory_size() > 31 * 1024 * 1024
            && stored.get_array_memory_size() <= HARD_ARROW_BYTES,
        "Gate R0 fixture must remain a concrete near-cap legal batch"
    );
    batch
}

async fn widest_retained_growth_at_uri(uri: &str) -> WidestRetainedGrowthSample {
    init_enrolled(uri, PAYLOAD_SCHEMA).await;
    let before = current_mem_wal_inventory(uri).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let db = Arc::new(Omnigraph::open(uri).await.unwrap());
        let batch = widest_high_entropy_payload_batch(&db).await;
        let before_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let manifest_version_before = before_snapshot.version();
        let table_version_before = before_snapshot.entry(TABLE).unwrap().table_version;
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .unwrap();
        let table_ack = PathIo::from_stats(&table_tracker.incremental_stats());
        let _ = manifest_tracker.incremental_stats();
        let after_ack = current_mem_wal_inventory(uri).await;
        let after_ack_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let manifest_version_after_ack = after_ack_snapshot.version();
        let table_version_after_ack = after_ack_snapshot.entry(TABLE).unwrap().table_version;

        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect("every admitted near-cap generation must close");
        let table_fold = PathIo::from_stats(&table_tracker.incremental_stats());
        let after_fold = current_mem_wal_inventory(uri).await;
        let after_fold_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let manifest_version_after_fold = after_fold_snapshot.version();
        let table_version_after_fold = after_fold_snapshot.entry(TABLE).unwrap().table_version;

        let wanted = BTreeSet::from([
            "rss-00000".to_string(),
            format!("rss-{:05}", WIDEST_ROWS / 2),
            format!("rss-{:05}", WIDEST_ROWS - 1),
        ]);
        let visible = helpers::read_table(&db, TABLE).await;
        let visible_rows = visible.iter().map(RecordBatch::num_rows).sum();
        let mut sampled_payloads = Vec::new();
        for batch in &visible {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("folded id must remain Utf8");
            let payloads = batch
                .column_by_name("payload")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("folded payload must remain Utf8");
            for row in 0..batch.num_rows() {
                if wanted.contains(ids.value(row)) {
                    sampled_payloads
                        .push((ids.value(row).to_string(), payloads.value(row).to_string()));
                }
            }
        }
        sampled_payloads.sort_by(|left, right| left.0.cmp(&right.0));

        WidestRetainedGrowthSample {
            before,
            after_ack,
            after_fold,
            table_ack,
            table_fold,
            manifest_version_before,
            manifest_version_after_ack,
            manifest_version_after_fold,
            table_version_before,
            table_version_after_ack,
            table_version_after_fold,
            visible_rows,
            sampled_payloads,
            recovery_sidecars_after_fold: current_recovery_sidecars(uri).await,
        }
    })
    .await
}

fn assert_widest_retained_growth(sample: &WidestRetainedGrowthSample, backend: &str) {
    eprintln!(
        "Gate R0 widest {backend}: fold=closed visible_rows={} before_immutable_bytes={} after_ack_immutable_bytes={} after_fold_immutable_bytes={} wal_bytes={} generation_data_bytes={} generation_pk_bytes={} generation_bloom_bytes={} ack_writes={} fold_generation_writes={} manifest_versions={}/{}/{} table_versions={}/{}/{}",
        sample.visible_rows,
        sample.before.immutable_object_bytes(),
        sample.after_ack.immutable_object_bytes(),
        sample.after_fold.immutable_object_bytes(),
        sample.after_fold.bytes(MemWalObjectKind::Wal),
        sample.after_fold.bytes(MemWalObjectKind::GenerationData),
        sample
            .after_fold
            .bytes(MemWalObjectKind::GenerationPkSidecar),
        sample.after_fold.bytes(MemWalObjectKind::GenerationBloom),
        sample.table_ack.total_writes,
        sample.table_fold.generation_writes,
        sample.manifest_version_before,
        sample.manifest_version_after_ack,
        sample.manifest_version_after_fold,
        sample.table_version_before,
        sample.table_version_after_ack,
        sample.table_version_after_fold,
    );
    sample
        .before
        .assert_path_class_size_retained_by(&sample.after_ack);
    sample
        .after_ack
        .assert_path_class_size_retained_by(&sample.after_fold);
    assert!(sample.before.unknown_paths().is_empty(), "{sample:#?}");
    assert!(sample.after_ack.unknown_paths().is_empty(), "{sample:#?}");
    assert!(sample.after_fold.unknown_paths().is_empty(), "{sample:#?}");
    assert_eq!(sample.table_ack.generation_writes, 0, "{sample:#?}");
    assert!(
        sample.after_ack.bytes(MemWalObjectKind::Wal)
            >= u64::try_from(HARD_ARROW_BYTES * 9 / 10).unwrap(),
        "high-entropy WAL evidence compressed below its remeasurement tripwire"
    );
    assert!(sample.after_ack.generation_roots.is_empty());
    assert_eq!(sample.after_fold.generation_roots.len(), 1);
    assert_eq!(
        sample.after_fold.generation_roots,
        sample.after_fold.referenced_generation_roots
    );
    assert!(
        sample.after_fold.bytes(MemWalObjectKind::GenerationData)
            >= u64::try_from(HARD_ARROW_BYTES * 9 / 10).unwrap(),
        "high-entropy generation evidence compressed below its remeasurement tripwire"
    );
    assert!(
        sample
            .after_fold
            .bytes(MemWalObjectKind::GenerationPkSidecar)
            > 0
    );
    assert!(sample.after_fold.bytes(MemWalObjectKind::GenerationBloom) > 0);
    assert!(sample.after_fold.immutable_object_bytes() > sample.after_ack.immutable_object_bytes());
    assert_eq!(
        sample.manifest_version_after_ack, sample.manifest_version_before,
        "durable acknowledgement must not publish the graph"
    );
    assert_eq!(
        sample.table_version_after_ack, sample.table_version_before,
        "durable acknowledgement must not advance the base table"
    );
    assert_eq!(
        sample.manifest_version_after_fold,
        sample.manifest_version_before + 1,
        "one fold must have exactly one graph visibility point"
    );
    assert_eq!(
        sample.table_version_after_fold,
        sample.table_version_before + 1,
        "one fold must commit exactly one Lance table effect"
    );
    assert_eq!(sample.visible_rows, WIDEST_ROWS);
    let expected_samples = [0, WIDEST_ROWS / 2, WIDEST_ROWS - 1]
        .into_iter()
        .map(|row| {
            (
                format!("rss-{row:05}"),
                deterministic_high_entropy_ascii(row, WIDEST_PAYLOAD_BYTES),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(sample.sampled_payloads, expected_samples);
    assert!(
        sample.recovery_sidecars_after_fold.is_empty(),
        "successful closure must retire its recovery sidecar: {:?}",
        sample.recovery_sidecars_after_fold
    );
}

#[tokio::test]
#[serial]
async fn gate_r0_widest_generation_closes_and_records_retain_all_growth_local() {
    let dir = tempfile::tempdir().unwrap();
    let sample = widest_retained_growth_at_uri(dir.path().to_str().unwrap()).await;
    assert_widest_retained_growth(&sample, "local");
}

#[tokio::test]
#[serial]
async fn gate_r0_widest_generation_closes_and_records_retain_all_growth_on_configured_rustfs() {
    let Some(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok() else {
        eprintln!("SKIP: OMNIGRAPH_S3_TEST_BUCKET not set; RustFS Gate R0 closure not measured");
        return;
    };
    let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let uri = format!(
        "s3://{bucket}/{prefix}/memwal-gate-r0/{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    );
    let sample = widest_retained_growth_at_uri(&uri).await;
    storage_for_uri(&uri)
        .unwrap()
        .delete_prefix(&uri)
        .await
        .expect("configured RustFS Gate R0 cleanup failed");
    assert_widest_retained_growth(&sample, "rustfs");
}

fn widest_accounting_batch(payload_bytes: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids = Arc::new(StringArray::from_iter_values(
        (0..WIDEST_ROWS).map(|row| format!("rss-{row:05}")),
    )) as ArrayRef;
    let payload = "x".repeat(payload_bytes);
    let values = Arc::new(StringArray::from_iter_values(
        (0..WIDEST_ROWS).map(|_| payload.as_str()),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, values]).unwrap()
}

fn widest_generation_estimates(payload_bytes: usize) -> (usize, usize, usize) {
    let caller = widest_accounting_batch(payload_bytes);
    let mut columns = caller.columns().to_vec();
    columns.push(Arc::new(BooleanArray::from(vec![false; WIDEST_ROWS])) as ArrayRef);
    let stored =
        RecordBatch::try_new(schema_with_tombstone(caller.schema().as_ref()), columns).unwrap();
    let arrow_reservation = stored.get_array_memory_size();
    let batch_store_estimate = StoredBatch::new(stored, 0, 0).estimated_size;
    let bloom_estimate = Sbbf::with_ndv_fpp(8_192, 0.00057)
        .unwrap()
        .estimated_memory_size();
    (arrow_reservation, batch_store_estimate, bloom_estimate)
}

fn fragmented_generation_estimates() -> (usize, usize, usize, usize) {
    fn one_batch(payload_bytes: usize, row: usize) -> (usize, usize) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let caller = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![format!("rss-{row:05}")])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x".repeat(payload_bytes)])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut columns = caller.columns().to_vec();
        columns.push(Arc::new(BooleanArray::from(vec![false])) as ArrayRef);
        let stored =
            RecordBatch::try_new(schema_with_tombstone(caller.schema().as_ref()), columns).unwrap();
        let arrow = stored.get_array_memory_size();
        let batch_store = StoredBatch::new(stored, row as u64, row).estimated_size;
        (arrow, batch_store)
    }

    // Every id is fixed-width (`rss-00000` .. `rss-08191`). Tune the largest
    // uniform one-row payload whose exact post-tombstone accounting remains a
    // legal 32-MiB generation. Then construct and sum all 8,192 exact
    // `StoredBatch` estimates rather than extrapolating one monolithic batch.
    let mut low = 0usize;
    let mut high = 4_096usize;
    while low < high {
        let candidate = low + (high - low).div_ceil(2);
        let (one_arrow, _) = one_batch(candidate, 0);
        if one_arrow.saturating_mul(WIDEST_ROWS) <= HARD_ARROW_BYTES {
            low = candidate;
        } else {
            high = candidate - 1;
        }
    }
    let payload_bytes = low;
    let mut arrow_sum = 0usize;
    let mut batch_store_sum = 0usize;
    for row in 0..WIDEST_ROWS {
        let (arrow, batch_store) = one_batch(payload_bytes, row);
        arrow_sum = arrow_sum.checked_add(arrow).unwrap();
        batch_store_sum = batch_store_sum.checked_add(batch_store).unwrap();
    }
    let bloom_estimate = Sbbf::with_ndv_fpp(8_192, 0.00057)
        .unwrap()
        .estimated_memory_size();
    (payload_bytes, arrow_sum, batch_store_sum, bloom_estimate)
}

#[cfg(unix)]
fn normalized_peak_rss_bytes(rusage: &libc::rusage) -> u64 {
    #[cfg(target_os = "macos")]
    let peak = rusage.ru_maxrss as u64;
    #[cfg(not(target_os = "macos"))]
    let peak = (rusage.ru_maxrss as u64) * 1024;
    peak
}

#[cfg(unix)]
fn wait4_rusage(pid: i32) -> (i64, u64) {
    let mut status: libc::c_int = 0;
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    let reaped = loop {
        let result = unsafe { libc::wait4(pid, &mut status, 0, &mut rusage) };
        if result != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            break result;
        }
    };
    assert_eq!(reaped, pid, "wait4 reaped an unexpected process");
    let exit = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status) as i64
    } else if libc::WIFSIGNALED(status) {
        -(libc::WTERMSIG(status) as i64)
    } else {
        i64::MIN
    };
    (exit, normalized_peak_rss_bytes(&rusage))
}

#[cfg(unix)]
fn run_rss_child(mode: &str) -> u64 {
    use std::process::{Command, Stdio};

    let child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("widest_legal_generation_cost_child")
        .arg("--ignored")
        .arg("--nocapture")
        .env(RSS_CHILD_ENV, mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = i32::try_from(child.id()).unwrap();
    let (exit, peak) = wait4_rusage(pid);
    // `wait4` reaped the child; dropping the handle only closes its pipes.
    drop(child);
    assert_eq!(exit, 0, "{mode} widest-generation RSS child failed");
    peak
}

#[test]
fn widest_legal_generation_records_no_roll_estimates_and_peak_rss() {
    let (arrow_reservation, batch_store_estimate, bloom_estimate) =
        widest_generation_estimates(WIDEST_PAYLOAD_BYTES);
    let (one_byte_over_arrow_reservation, _, _) =
        widest_generation_estimates(WIDEST_PAYLOAD_BYTES + 1);
    let rc_trigger_estimate = batch_store_estimate + bloom_estimate;
    let (
        fragmented_payload_bytes,
        fragmented_arrow_reservation,
        fragmented_batch_store_estimate,
        fragmented_bloom_estimate,
    ) = fragmented_generation_estimates();
    let fragmented_rc_trigger_estimate =
        fragmented_batch_store_estimate + fragmented_bloom_estimate;
    assert!(
        arrow_reservation > 31 * 1024 * 1024 && arrow_reservation <= HARD_ARROW_BYTES,
        "fixture must be a concrete near-cap legal generation, got {arrow_reservation} bytes"
    );
    assert!(
        one_byte_over_arrow_reservation > HARD_ARROW_BYTES,
        "the next uniform payload byte must cross the exact Arrow cap; legal={} next={one_byte_over_arrow_reservation}",
        arrow_reservation,
    );
    assert!(WIDEST_ROWS < NO_AUTO_ROLL_ROWS);
    assert!(1 < NO_AUTO_ROLL_BATCHES);
    assert!(rc_trigger_estimate < NO_AUTO_ROLL_BYTES);
    assert!(
        fragmented_arrow_reservation > 31 * 1024 * 1024
            && fragmented_arrow_reservation <= HARD_ARROW_BYTES,
        "fragmented fixture must also be a concrete near-cap legal generation"
    );
    assert!(WIDEST_ROWS < NO_AUTO_ROLL_BATCHES);
    assert!(fragmented_rc_trigger_estimate < NO_AUTO_ROLL_BYTES);

    #[cfg(unix)]
    {
        let baseline_peak_rss = run_rss_child("baseline");
        let widest_peak_rss = run_rss_child("widest");
        let baseline_fold_peak_rss = run_rss_child("baseline-fold");
        let widest_fold_peak_rss = run_rss_child("widest-fold");
        let signed_process_delta = i128::from(widest_peak_rss) - i128::from(baseline_peak_rss);
        let fold_process_delta = widest_fold_peak_rss.saturating_sub(baseline_fold_peak_rss);
        eprintln!(
            "B1 widest legal generation: rows={WIDEST_ROWS} one_batch_payload_bytes_per_row={WIDEST_PAYLOAD_BYTES} one_batch_post_tombstone_arrow_reservation={arrow_reservation} one_batch_batch_store_estimate={batch_store_estimate} rc_pk_bloom_estimate={bloom_estimate} one_batch_rc_roll_trigger_estimate={rc_trigger_estimate} fragmented_batches={WIDEST_ROWS} fragmented_payload_bytes_per_row={fragmented_payload_bytes} fragmented_post_tombstone_arrow_reservation={fragmented_arrow_reservation} fragmented_batch_store_estimate={fragmented_batch_store_estimate} fragmented_rc_roll_trigger_estimate={fragmented_rc_trigger_estimate} no_roll_bytes={NO_AUTO_ROLL_BYTES} rss_shape=one_batch baseline_peak_rss={baseline_peak_rss} widest_peak_rss={widest_peak_rss} signed_whole_process_delta={signed_process_delta} baseline_fold_peak_rss={baseline_fold_peak_rss} widest_fold_peak_rss={widest_fold_peak_rss} fold_process_delta={fold_process_delta} fold_delta_remeasure_bytes={FOLD_RSS_DELTA_REMEASURE_BYTES}"
        );
        assert!(
            widest_peak_rss > baseline_peak_rss,
            "the isolated widest generation should have a visible whole-process RSS cost"
        );
        assert!(
            widest_fold_peak_rss > baseline_fold_peak_rss,
            "the isolated widest fold should have a visible whole-process RSS cost"
        );
        assert!(
            fold_process_delta <= FOLD_RSS_DELTA_REMEASURE_BYTES,
            "the dense fold path crossed its measured RSS-delta envelope; remeasure before changing the admission or compaction shape"
        );
    }

    #[cfg(not(unix))]
    eprintln!(
        "B1 widest legal generation: rows={WIDEST_ROWS} one_batch_payload_bytes_per_row={WIDEST_PAYLOAD_BYTES} one_batch_post_tombstone_arrow_reservation={arrow_reservation} one_batch_batch_store_estimate={batch_store_estimate} rc_pk_bloom_estimate={bloom_estimate} one_batch_rc_roll_trigger_estimate={rc_trigger_estimate} fragmented_batches={WIDEST_ROWS} fragmented_payload_bytes_per_row={fragmented_payload_bytes} fragmented_post_tombstone_arrow_reservation={fragmented_arrow_reservation} fragmented_batch_store_estimate={fragmented_batch_store_estimate} fragmented_rc_roll_trigger_estimate={fragmented_rc_trigger_estimate}; peak RSS unavailable on this platform"
    );
}

/// Subprocess half of `widest_legal_generation_records_no_roll_estimates_and_peak_rss`.
/// Keep it non-`serial`: the parent waits for this process and must not share an
/// interprocess serial-test lock with it.
#[test]
#[ignore = "subprocess helper; exercised by the widest-generation cost cell"]
fn widest_legal_generation_cost_child() {
    let Some(mode) = std::env::var_os(RSS_CHILD_ENV) else {
        return;
    };
    let mode = mode.to_string_lossy().into_owned();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().to_str().unwrap();
            init_enrolled(uri, PAYLOAD_SCHEMA).await;
            let db = Arc::new(Omnigraph::open(uri).await.unwrap());
            let (batch, fold) = match mode.as_str() {
                "baseline" => (payload_batch(&db, 1, 1).await, false),
                "widest" => (
                    payload_batch(&db, WIDEST_ROWS, WIDEST_PAYLOAD_BYTES).await,
                    false,
                ),
                "baseline-fold" => (payload_batch(&db, 1, 1).await, true),
                "widest-fold" => (widest_high_entropy_payload_batch(&db).await, true),
                other => panic!("unknown RSS child mode '{other}'"),
            };
            let expected_rows = batch.num_rows();
            let table_tracker = IOTracker::default();
            let manifest_tracker = IOTracker::default();
            with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
                let _ = table_tracker.incremental_stats();
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
                    .await
                    .unwrap();
                let table = PathIo::from_stats(&table_tracker.incremental_stats());
                assert!(table.wal_writes > 0, "{table:?}");
                assert_eq!(table.generation_writes, 0, "{table:?}");
                if fold {
                    db.failpoint_stream_b1_for_test(TABLE, None, 0)
                        .await
                        .expect("RSS fold child must close its admitted generation");
                    let fold_table = PathIo::from_stats(&table_tracker.incremental_stats());
                    assert!(fold_table.generation_writes > 0, "{fold_table:?}");
                    assert_eq!(
                        helpers::count_rows(&db, TABLE).await,
                        expected_rows,
                        "RSS fold child must publish every admitted row"
                    );
                }
            })
            .await;
        });
}
