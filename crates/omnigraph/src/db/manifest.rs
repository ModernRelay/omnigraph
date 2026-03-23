use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance_file::version::LanceFileVersion;
use omnigraph_compiler::catalog::Catalog;

use crate::error::{OmniError, Result};

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

/// Snapshot of the full manifest state.
#[derive(Debug, Clone)]
pub struct ManifestState {
    pub version: u64,
    pub entries: Vec<SubTableEntry>,
}

impl ManifestState {
    pub fn entry(&self, table_key: &str) -> Option<&SubTableEntry> {
        self.entries.iter().find(|e| e.table_key == table_key)
    }

    pub fn entries_map(&self) -> HashMap<String, &SubTableEntry> {
        self.entries
            .iter()
            .map(|e| (e.table_key.clone(), e))
            .collect()
    }
}

/// Coordinates cross-dataset state through a Lance manifest table.
///
/// The manifest table has one row per sub-table (one per node/edge type).
/// A repo version is a manifest table version. Atomic commits to the manifest
/// are the single coordination point.
pub struct ManifestCoordinator {
    root_uri: String,
    dataset: Dataset,
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

        Ok(Self {
            root_uri: root.to_string(),
            dataset,
        })
    }

    /// Open an existing repo's manifest.
    pub async fn open(root_uri: &str) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let manifest_uri = format!("{}/_manifest.lance", root);
        let dataset = Dataset::open(&manifest_uri)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(Self {
            root_uri: root.to_string(),
            dataset,
        })
    }

    /// Read the current manifest state.
    pub async fn state(&self) -> Result<ManifestState> {
        let version = self.dataset.version().version;
        let batches: Vec<RecordBatch> = self
            .dataset
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
        let job = lance::dataset::MergeInsertBuilder::try_new(
            ds,
            vec!["table_key".to_string()],
        )
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
        Ok(self.version())
    }

    /// Open a sub-table dataset by resolving its path relative to the repo root.
    pub async fn open_sub_table(&self, entry: &SubTableEntry) -> Result<Dataset> {
        let full_path = format!("{}/{}", self.root_uri, entry.table_path);
        let ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        // Pin to the version recorded in the manifest
        let ds = ds
            .checkout_version(entry.table_version)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(ds)
    }

    /// Current manifest version.
    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    /// Root URI of the repo.
    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }
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
    let branches: Vec<Option<&str>> = entries
        .iter()
        .map(|e| e.table_branch.as_deref())
        .collect();
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
        let state = mc.state().await.unwrap();

        // Should have 4 entries: 2 node types + 2 edge types
        assert_eq!(state.entries.len(), 4);
        assert!(state.entry("node:Person").is_some());
        assert!(state.entry("node:Company").is_some());
        assert!(state.entry("edge:Knows").is_some());
        assert!(state.entry("edge:WorksAt").is_some());

        // All at version 1, 0 rows
        for entry in &state.entries {
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
        let state = mc.state().await.unwrap();
        assert_eq!(state.entries.len(), 4);
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

        let state = mc.state().await.unwrap();
        let person = state.entry("node:Person").unwrap();
        assert_eq!(person.table_version, 2);
        assert_eq!(person.row_count, 10);

        // Other entries unchanged
        let company = state.entry("node:Company").unwrap();
        assert_eq!(company.table_version, 1);
        assert_eq!(company.row_count, 0);
    }

    #[tokio::test]
    async fn test_open_sub_table() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let catalog = build_test_catalog();

        let mc = ManifestCoordinator::init(uri, &catalog).await.unwrap();
        let state = mc.state().await.unwrap();

        let person_entry = state.entry("node:Person").unwrap();
        let person_ds = mc.open_sub_table(person_entry).await.unwrap();

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
        let state = mc.state().await.unwrap();
        assert_eq!(mc.version(), state.version);
    }
}
