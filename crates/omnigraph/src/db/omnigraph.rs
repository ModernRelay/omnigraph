use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use lance::Dataset;
use lance_index::scalar::ScalarIndexParams;
use lance_index::{DatasetIndexExt, IndexType};
use omnigraph_compiler::build_catalog;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::schema::parser::parse_schema;

use crate::error::{OmniError, Result};
use crate::graph_index::GraphIndex;

use super::manifest::{ManifestCoordinator, Snapshot};

const SCHEMA_FILENAME: &str = "_schema.pg";

/// Top-level handle to an Omnigraph database.
///
/// An Omnigraph is a Lance-native graph database with git-style branching.
/// It stores typed property graphs as per-type Lance datasets coordinated
/// through a Lance manifest table.
pub struct Omnigraph {
    root_uri: String,
    manifest: ManifestCoordinator,
    catalog: Catalog,
    schema_source: String,
    cached_graph_index: Option<Arc<GraphIndex>>,
}

impl Omnigraph {
    /// Create a new repo at `uri` from schema source.
    ///
    /// Creates `_schema.pg`, per-type Lance datasets, and `_manifest.lance`.
    pub async fn init(uri: &str, schema_source: &str) -> Result<Self> {
        let root = uri.trim_end_matches('/');

        // Parse and validate schema
        let schema_ast = parse_schema(schema_source)?;
        let catalog = build_catalog(&schema_ast)?;

        // Write _schema.pg
        let schema_path = format!("{}/{}", root, SCHEMA_FILENAME);
        write_file(&schema_path, schema_source)?;

        // Create manifest + per-type datasets
        let manifest = ManifestCoordinator::init(root, &catalog).await?;

        Ok(Self {
            root_uri: root.to_string(),
            manifest,
            catalog,
            schema_source: schema_source.to_string(),
            cached_graph_index: None,
        })
    }

    /// Open an existing repo.
    ///
    /// Reads `_schema.pg`, parses it, builds the catalog, and opens the manifest.
    pub async fn open(uri: &str) -> Result<Self> {
        let root = uri.trim_end_matches('/');

        // Read _schema.pg
        let schema_path = format!("{}/{}", root, SCHEMA_FILENAME);
        let schema_source = read_file(&schema_path)?;

        // Parse and validate schema
        let schema_ast = parse_schema(&schema_source)?;
        let catalog = build_catalog(&schema_ast)?;

        // Open manifest
        let manifest = ManifestCoordinator::open(root).await?;

        Ok(Self {
            root_uri: root.to_string(),
            manifest,
            catalog,
            schema_source,
            cached_graph_index: None,
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn schema_source(&self) -> &str {
        &self.schema_source
    }

    pub fn uri(&self) -> &str {
        &self.root_uri
    }

    pub fn version(&self) -> u64 {
        self.manifest.version()
    }

    /// Return an immutable Snapshot from the known manifest state. No storage I/O.
    pub fn snapshot(&self) -> Snapshot {
        self.manifest.snapshot()
    }

    /// Re-read manifest from storage to see other writers' commits.
    pub async fn refresh(&mut self) -> Result<()> {
        self.manifest.refresh().await
    }

    /// Get or build the graph index for the current snapshot.
    pub async fn graph_index(&mut self) -> Result<Arc<GraphIndex>> {
        // For now, simple invalidation: rebuild if None.
        // Future: key by edge sub-table versions.
        if let Some(ref idx) = self.cached_graph_index {
            return Ok(Arc::clone(idx));
        }

        let snapshot = self.snapshot();
        let edge_types: HashMap<String, (String, String)> = self
            .catalog
            .edge_types
            .iter()
            .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
            .collect();

        let idx = Arc::new(GraphIndex::build(&snapshot, &edge_types).await?);
        self.cached_graph_index = Some(Arc::clone(&idx));
        Ok(idx)
    }

    /// Ensure BTree scalar indices exist on key columns.
    /// Idempotent — Lance skips if index already exists.
    pub async fn ensure_indices(&self) -> Result<()> {
        let snapshot = self.snapshot();
        let params = ScalarIndexParams::default();

        for type_name in self.catalog.node_types.keys() {
            let table_key = format!("node:{}", type_name);
            let Some(entry) = snapshot.entry(&table_key) else {
                continue;
            };
            let full_path = format!("{}/{}", self.root_uri, entry.table_path);
            let mut ds = Dataset::open(&full_path)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            if ds.count_rows(None).await.unwrap_or(0) > 0 {
                let _ = ds
                    .create_index_builder(&["id"], IndexType::BTree, &params)
                    .replace(true)
                    .await;
            }
        }

        for edge_name in self.catalog.edge_types.keys() {
            let table_key = format!("edge:{}", edge_name);
            let Some(entry) = snapshot.entry(&table_key) else {
                continue;
            };
            let full_path = format!("{}/{}", self.root_uri, entry.table_path);
            let mut ds = Dataset::open(&full_path)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            if ds.count_rows(None).await.unwrap_or(0) > 0 {
                let _ = ds
                    .create_index_builder(&["src"], IndexType::BTree, &params)
                    .replace(true)
                    .await;
                let _ = ds
                    .create_index_builder(&["dst"], IndexType::BTree, &params)
                    .replace(true)
                    .await;
            }
        }

        Ok(())
    }

    pub fn manifest_mut(&mut self) -> &mut ManifestCoordinator {
        &mut self.manifest
    }
}

/// Write a file to a local path (extracted from a URI).
fn write_file(uri: &str, content: &str) -> Result<()> {
    // For now, only local paths. S3 support comes later via object_store.
    let path = Path::new(uri);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content)?;
    Ok(())
}

/// Read a file from a local path (extracted from a URI).
fn read_file(uri: &str) -> Result<String> {
    let path = Path::new(uri);
    if !path.exists() {
        return Err(OmniError::Manifest(format!(
            "repo not found: {} (missing {})",
            uri, SCHEMA_FILENAME
        )));
    }
    Ok(std::fs::read_to_string(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#;

    #[tokio::test]
    async fn test_init_creates_repo() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        // Schema file written
        assert!(dir.path().join("_schema.pg").exists());

        // Manifest created with correct entries
        let snap = db.snapshot();
        assert!(snap.entry("node:Person").is_some());
        assert!(snap.entry("node:Company").is_some());
        assert!(snap.entry("edge:Knows").is_some());
        assert!(snap.entry("edge:WorksAt").is_some());

        // Catalog is correct
        assert_eq!(db.catalog().node_types.len(), 2);
        assert_eq!(db.catalog().edge_types.len(), 2);
        assert_eq!(
            db.catalog().node_types["Person"].key_property.as_deref(),
            Some("name")
        );
    }

    #[tokio::test]
    async fn test_open_reads_existing_repo() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        // Re-open
        let db = Omnigraph::open(uri).await.unwrap();
        assert_eq!(db.catalog().node_types.len(), 2);
        assert_eq!(db.catalog().edge_types.len(), 2);
        let snap = db.snapshot();
        assert!(snap.entry("node:Person").is_some());
        assert!(snap.entry("edge:Knows").is_some());
    }

    #[tokio::test]
    async fn test_open_nonexistent_fails() {
        let result = Omnigraph::open("/tmp/nonexistent_omnigraph_test_xyz").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_snapshot_version_is_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        // Take snapshot before any writes
        let snap1 = db.snapshot();
        let v1 = snap1.version();

        // Load data — advances manifest version
        crate::loader::load_jsonl(
            &mut db,
            r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#,
            crate::loader::LoadMode::Overwrite,
        )
        .await
        .unwrap();

        // Snapshot from handle sees new version
        let snap2 = db.snapshot();
        assert!(snap2.version() > v1);

        // But the old snapshot is still pinned
        assert_eq!(snap1.version(), v1);
    }
}
