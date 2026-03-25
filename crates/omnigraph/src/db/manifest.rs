use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};

/// Immutable point-in-time view of the database.
///
/// Cheap to create (no storage I/O). All reads within a query go through one
/// Snapshot to guarantee cross-type consistency.
#[derive(Debug, Clone)]
pub struct Snapshot {
    root_uri: String,
    version: u64,
    entries: HashMap<String, SubTableEntry>,
}

impl Snapshot {
    /// Open a sub-table dataset at its pinned version.
    pub async fn open(&self, table_key: &str) -> Result<Dataset> {
        let entry = self
            .entries
            .get(table_key)
            .ok_or_else(|| OmniError::Manifest(format!("no manifest entry for {}", table_key)))?;
        let full_path = format!("{}/{}", self.root_uri, entry.table_path);
        let ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let ds = match &entry.table_branch {
            Some(branch) => ds
                .checkout_branch(branch)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?,
            None => ds,
        };
        let ds = ds
            .checkout_version(entry.table_version)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(ds)
    }

    /// Manifest version this snapshot was taken from.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Look up a sub-table entry by key.
    pub fn entry(&self, table_key: &str) -> Option<&SubTableEntry> {
        self.entries.get(table_key)
    }

    pub fn entries(&self) -> impl Iterator<Item = &SubTableEntry> {
        self.entries.values()
    }
}

/// Schema for the manifest Lance table.
fn manifest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table_key", DataType::Utf8, false),
        Field::new("table_path", DataType::Utf8, false),
        Field::new("table_version", DataType::UInt64, false),
        Field::new("table_branch", DataType::Utf8, true),
        Field::new("row_count", DataType::UInt64, false),
    ]))
}

/// Deterministic hash of a type name, used as a stable directory name.
fn type_name_hash(name: &str) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for byte in name.as_bytes() {
        h ^= *byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", h)
}

/// An entry in the manifest table representing one sub-table (node or edge type).
#[derive(Debug, Clone)]
pub struct SubTableEntry {
    pub table_key: String,
    pub table_path: String,
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
}

/// An update to apply to the manifest via `commit`.
#[derive(Debug, Clone)]
pub struct SubTableUpdate {
    pub table_key: String,
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
}

#[derive(Debug, Clone)]
struct ManifestState {
    pub version: u64,
    pub entries: Vec<SubTableEntry>,
}

/// Coordinates cross-dataset state through a Lance manifest table.
///
/// The manifest table has one row per sub-table (one per node/edge type).
/// A repo version is a manifest table version. Atomic commits to the manifest
/// are the single coordination point.
pub struct ManifestCoordinator {
    root_uri: String,
    dataset: Dataset,
    known_state: ManifestState,
    active_branch: Option<String>,
}

impl ManifestCoordinator {
    /// Create a new repo at `root_uri` from a catalog.
    ///
    /// Creates per-type Lance datasets and the `_manifest.lance` table.
    pub async fn init(root_uri: &str, catalog: &Catalog) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let mut entries = Vec::new();

        // Create per-type Lance datasets
        for (name, node_type) in &catalog.node_types {
            let hash = type_name_hash(name);
            let table_path = format!("nodes/{}", hash);
            let full_path = format!("{}/{}", root, table_path);

            create_empty_dataset(&full_path, &node_type.arrow_schema).await?;

            entries.push(SubTableEntry {
                table_key: format!("node:{}", name),
                table_path,
                table_version: 1,
                table_branch: None,
                row_count: 0,
            });
        }

        for (name, edge_type) in &catalog.edge_types {
            let hash = type_name_hash(name);
            let table_path = format!("edges/{}", hash);
            let full_path = format!("{}/{}", root, table_path);

            create_empty_dataset(&full_path, &edge_type.arrow_schema).await?;

            entries.push(SubTableEntry {
                table_key: format!("edge:{}", name),
                table_path,
                table_version: 1,
                table_branch: None,
                row_count: 0,
            });
        }

        // Create the manifest Lance table
        let manifest_uri = format!("{}/_manifest.lance", root);
        let batch = entries_to_batch(&entries)?;
        let schema = manifest_schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let dataset = Dataset::write(reader, &manifest_uri as &str, Some(params))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let known_state = ManifestState {
            version: dataset.version().version,
            entries: entries.clone(),
        };

        Ok(Self {
            root_uri: root.to_string(),
            dataset,
            known_state,
            active_branch: None,
        })
    }

    /// Open an existing repo's manifest.
    pub async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let manifest_uri = format!("{}/_manifest.lance", root);
        let dataset = Dataset::open(&manifest_uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let known_state = read_manifest_state(&dataset).await?;
        Ok(Self {
            root_uri: root.to_string(),
            dataset,
            known_state,
            active_branch: None,
        })
    }

    /// Open an existing repo's manifest at a specific branch.
    pub async fn open_at_branch(root_uri: &str, branch: &str) -> Result<Self> {
        if branch == "main" {
            return Self::open(root_uri).await;
        }

        let root = root_uri.trim_end_matches('/');
        let manifest_uri = format!("{}/_manifest.lance", root);
        let dataset = Dataset::open(&manifest_uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let dataset = dataset
            .checkout_branch(branch)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let known_state = read_manifest_state(&dataset).await?;
        Ok(Self {
            root_uri: root.to_string(),
            dataset,
            known_state,
            active_branch: Some(branch.to_string()),
        })
    }

    pub async fn snapshot_at(
        root_uri: &str,
        branch: Option<&str>,
        version: u64,
    ) -> Result<Snapshot> {
        let root = root_uri.trim_end_matches('/');
        let manifest_uri = format!("{}/_manifest.lance", root);
        let dataset = Dataset::open(&manifest_uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let dataset = match branch {
            Some(branch) if branch != "main" => dataset
                .checkout_branch(branch)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?,
            _ => dataset,
        };
        let dataset = dataset
            .checkout_version(version)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let state = read_manifest_state(&dataset).await?;
        Ok(Snapshot {
            root_uri: root.to_string(),
            version: state.version,
            entries: state
                .entries
                .into_iter()
                .map(|entry| (entry.table_key.clone(), entry))
                .collect(),
        })
    }

    /// Return a Snapshot from the known manifest state. No storage I/O.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            root_uri: self.root_uri.clone(),
            version: self.known_state.version,
            entries: self
                .known_state
                .entries
                .iter()
                .map(|e| (e.table_key.clone(), e.clone()))
                .collect(),
        }
    }

    /// Re-read manifest from storage to see other writers' commits.
    pub async fn refresh(&mut self) -> Result<()> {
        let manifest_uri = format!("{}/_manifest.lance", self.root_uri);
        self.dataset = Dataset::open(&manifest_uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        if let Some(branch) = &self.active_branch {
            self.dataset = self
                .dataset
                .checkout_branch(branch)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        self.known_state = read_manifest_state(&self.dataset).await?;
        Ok(())
    }

    /// Read manifest state from storage. Used internally by `commit()`.
    async fn state(&self) -> Result<ManifestState> {
        read_manifest_state(&self.dataset).await
    }

    /// Commit updated sub-table versions to the manifest.
    ///
    /// Uses `merge_insert` keyed by `table_key` to update only the affected rows.
    /// Returns the new manifest version.
    pub async fn commit(&mut self, updates: &[SubTableUpdate]) -> Result<u64> {
        if updates.is_empty() {
            return Ok(self.version());
        }

        // Read current state to get table_path for each key
        let current = self.state().await?;
        let path_map: HashMap<String, String> = current
            .entries
            .into_iter()
            .map(|e| (e.table_key, e.table_path))
            .collect();

        // Build update batch
        let mut keys = Vec::with_capacity(updates.len());
        let mut paths = Vec::with_capacity(updates.len());
        let mut versions = Vec::with_capacity(updates.len());
        let mut branches: Vec<Option<String>> = Vec::with_capacity(updates.len());
        let mut row_counts = Vec::with_capacity(updates.len());

        for u in updates {
            let table_path = path_map.get(&u.table_key).ok_or_else(|| {
                OmniError::Manifest(format!("unknown table_key in commit: {}", u.table_key))
            })?;
            keys.push(u.table_key.as_str());
            paths.push(table_path.as_str());
            versions.push(u.table_version);
            branches.push(u.table_branch.clone());
            row_counts.push(u.row_count);
        }

        let schema = manifest_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(paths)),
                Arc::new(UInt64Array::from(versions)),
                Arc::new(StringArray::from(branches)),
                Arc::new(UInt64Array::from(row_counts)),
            ],
        )
        .map_err(|e| OmniError::Lance(e.to_string()))?;

        let ds = Arc::new(self.dataset.clone());
        let job = lance::dataset::MergeInsertBuilder::try_new(ds, vec!["table_key".to_string()])
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .when_matched(lance::dataset::WhenMatched::UpdateAll)
            .when_not_matched(lance::dataset::WhenNotMatched::InsertAll)
            .try_build()
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let reader = RecordBatchIterator::new(vec![Ok(batch)], manifest_schema());
        let (new_ds, _stats) = job
            .execute(lance_datafusion::utils::reader_to_stream(Box::new(reader)))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        self.dataset = new_ds.as_ref().clone();
        self.known_state = read_manifest_state(&self.dataset).await?;
        Ok(self.version())
    }

    /// Current manifest version.
    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    pub fn active_branch(&self) -> Option<&str> {
        self.active_branch.as_deref()
    }

    pub async fn create_branch(&mut self, name: &str) -> Result<()> {
        let mut ds = self.dataset.clone();
        ds.create_branch(name, self.version(), None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(())
    }

    pub async fn list_branches(&self) -> Result<Vec<String>> {
        let branches = self
            .dataset
            .list_branches()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let mut names: Vec<String> = branches.into_keys().collect();
        names.sort();
        let mut all = vec!["main".to_string()];
        all.extend(names);
        Ok(all)
    }

    /// Root URI of the repo.
    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }
}

/// Read manifest state from a Lance dataset.
async fn read_manifest_state(dataset: &Dataset) -> Result<ManifestState> {
    let version = dataset.version().version;
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let mut entries = Vec::new();
    for batch in &batches {
        let keys = batch
            .column_by_name("table_key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let paths = batch
            .column_by_name("table_path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let versions = batch
            .column_by_name("table_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let branches = batch
            .column_by_name("table_branch")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let row_counts = batch
            .column_by_name("row_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            entries.push(SubTableEntry {
                table_key: keys.value(i).to_string(),
                table_path: paths.value(i).to_string(),
                table_version: versions.value(i),
                table_branch: if branches.is_null(i) {
                    None
                } else {
                    Some(branches.value(i).to_string())
                },
                row_count: row_counts.value(i),
            });
        }
    }

    Ok(ManifestState { version, entries })
}

/// Create an empty Lance dataset with the given schema.
async fn create_empty_dataset(uri: &str, schema: &SchemaRef) -> Result<Dataset> {
    // Write an empty batch to create the dataset
    let batch = RecordBatch::new_empty(schema.clone());
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        allow_external_blob_outside_bases: true,
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params))
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Convert manifest entries to a RecordBatch.
fn entries_to_batch(entries: &[SubTableEntry]) -> Result<RecordBatch> {
    let keys: Vec<&str> = entries.iter().map(|e| e.table_key.as_str()).collect();
    let paths: Vec<&str> = entries.iter().map(|e| e.table_path.as_str()).collect();
    let versions: Vec<u64> = entries.iter().map(|e| e.table_version).collect();
    let branches: Vec<Option<&str>> = entries.iter().map(|e| e.table_branch.as_deref()).collect();
    let row_counts: Vec<u64> = entries.iter().map(|e| e.row_count).collect();

    RecordBatch::try_new(
        manifest_schema(),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(paths)),
            Arc::new(UInt64Array::from(versions)),
            Arc::new(StringArray::from(branches)),
            Arc::new(UInt64Array::from(row_counts)),
        ],
    )
    .map_err(|e| OmniError::Lance(e.to_string()))
}

use futures::TryStreamExt;

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::schema::parser::parse_schema;

    fn test_schema_source() -> &'static str {
        r#"
node Person {
    name: String
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company {
    title: String?
}
"#
    }

    fn build_test_catalog() -> Catalog {
        let schema = parse_schema(test_schema_source()).unwrap();
        build_catalog(&schema).unwrap()
    }

    #[tokio::test]
    async fn test_init_creates_manifest_and_sub_tables() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let catalog = build_test_catalog();

        let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
        let snap = mc.snapshot();

        assert!(snap.entry("node:Person").is_some());
        assert!(snap.entry("node:Company").is_some());
        assert!(snap.entry("edge:Knows").is_some());
        assert!(snap.entry("edge:WorksAt").is_some());

        // All at version 1, 0 rows
        for key in &["node:Person", "node:Company", "edge:Knows", "edge:WorksAt"] {
            let entry = snap.entry(key).unwrap();
            assert_eq!(entry.table_version, 1);
            assert_eq!(entry.row_count, 0);
            assert!(entry.table_branch.is_none());
        }
    }

    #[tokio::test]
    async fn test_open_reads_existing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let catalog = build_test_catalog();

        ManifestCoordinator::init(uri, &catalog).await.unwrap();

        // Re-open
        let mc = ManifestCoordinator::open(uri).await.unwrap();
        let snap = mc.snapshot();
        assert!(snap.entry("node:Person").is_some());
        assert!(snap.entry("edge:Knows").is_some());
    }

    #[tokio::test]
    async fn test_commit_advances_version() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let catalog = build_test_catalog();

        let mut mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
        let v1 = mc.version();

        let new_version = mc
            .commit(&[SubTableUpdate {
                table_key: "node:Person".to_string(),
                table_version: 2,
                table_branch: None,
                row_count: 10,
            }])
            .await
            .unwrap();

        assert!(new_version > v1);

        let snap = mc.snapshot();
        let person = snap.entry("node:Person").unwrap();
        assert_eq!(person.table_version, 2);
        assert_eq!(person.row_count, 10);

        // Other entries unchanged
        let company = snap.entry("node:Company").unwrap();
        assert_eq!(company.table_version, 1);
        assert_eq!(company.row_count, 0);
    }

    #[tokio::test]
    async fn test_snapshot_open_sub_table() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let catalog = build_test_catalog();

        let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
        let snap = mc.snapshot();
        let person_ds = snap.open("node:Person").await.unwrap();

        // Should have the correct schema (id + name + age = 3 fields)
        assert_eq!(person_ds.schema().fields.len(), 3);
        assert_eq!(person_ds.count_rows(None).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_version_is_manifest_version() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let catalog = build_test_catalog();

        let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
        let snap = mc.snapshot();
        assert_eq!(mc.version(), snap.version());
    }
}
