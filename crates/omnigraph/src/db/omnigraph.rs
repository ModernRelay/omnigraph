use std::path::Path;

use omnigraph_compiler::build_catalog;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::schema::parser::parse_schema;

use crate::error::{OmniError, Result};

use super::manifest::{ManifestCoordinator, ManifestState};

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

    pub async fn state(&self) -> Result<ManifestState> {
        self.manifest.state().await
    }

    pub fn manifest(&self) -> &ManifestCoordinator {
        &self.manifest
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
        let state = db.state().await.unwrap();
        assert_eq!(state.entries.len(), 4);
        assert!(state.entry("node:Person").is_some());
        assert!(state.entry("edge:Knows").is_some());

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
        let state = db.state().await.unwrap();
        assert_eq!(state.entries.len(), 4);
    }

    #[tokio::test]
    async fn test_open_nonexistent_fails() {
        let result = Omnigraph::open("/tmp/nonexistent_omnigraph_test_xyz").await;
        assert!(result.is_err());
    }
}
