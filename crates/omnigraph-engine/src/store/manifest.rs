use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{NanoError, Result};

const MANIFEST_FILENAME: &str = "graph.manifest.json";
const MANIFEST_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphManifest {
    pub format_version: u32,
    pub db_version: u64,
    pub last_tx_id: String,
    pub committed_at: String,
    pub schema_ir_hash: String,
    pub next_node_id: u64,
    pub next_edge_id: u64,
    pub next_type_id: u32,
    pub next_prop_id: u32,
    pub schema_identity_version: u32,
    pub datasets: Vec<DatasetEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetEntry {
    pub type_id: u32,
    pub type_name: String,
    pub kind: String,
    pub dataset_path: String,
    pub dataset_version: u64,
    pub row_count: u64,
}

impl GraphManifest {
    pub fn new(schema_ir_hash: String) -> Self {
        GraphManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            db_version: 0,
            last_tx_id: "init".to_string(),
            committed_at: "0".to_string(),
            schema_ir_hash,
            next_node_id: 0,
            next_edge_id: 0,
            next_type_id: 0,
            next_prop_id: 0,
            schema_identity_version: 1,
            datasets: Vec::new(),
        }
    }

    /// Write atomically: write .tmp → fsync → rename.
    pub fn write_atomic(&self, db_dir: &Path) -> Result<()> {
        let path = db_dir.join(MANIFEST_FILENAME);
        let tmp_path = db_dir.join(format!("{}.tmp", MANIFEST_FILENAME));

        let json = serde_json::to_string_pretty(self)
            .map_err(|e| NanoError::Manifest(format!("serialize error: {}", e)))?;

        // Write + fsync on the same handle. File::open() is read-only which
        // causes sync_all() to fail on Windows (FlushFileBuffers needs write access).
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(json.as_bytes())?;
            file.sync_all()?;
        }

        std::fs::rename(&tmp_path, &path)?;
        Ok(())
    }

    pub fn read(db_dir: &Path) -> Result<Self> {
        let path = db_dir.join(MANIFEST_FILENAME);
        let data = std::fs::read_to_string(&path)?;
        let manifest: GraphManifest = serde_json::from_str(&data)
            .map_err(|e| NanoError::Manifest(format!("parse error: {}", e)))?;
        if manifest.format_version != MANIFEST_FORMAT_VERSION {
            return Err(NanoError::Manifest(format!(
                "unsupported manifest format_version {} (expected {})",
                manifest.format_version, MANIFEST_FORMAT_VERSION
            )));
        }
        Ok(manifest)
    }
}

/// Simple FNV-1a hash of a string -> hex.
pub(crate) fn hash_string(s: &str) -> String {
    let mut hash: u64 = 14695981039346656037;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    format!("{:016x}", hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        let mut manifest = GraphManifest::new("abc123".to_string());
        manifest.next_node_id = 42;
        manifest.next_edge_id = 10;
        manifest.datasets.push(DatasetEntry {
            type_id: 100,
            type_name: "Person".to_string(),
            kind: "node".to_string(),
            dataset_path: "nodes/00000064".to_string(),
            dataset_version: 7,
            row_count: 5,
        });

        manifest.write_atomic(path).unwrap();
        let loaded = GraphManifest::read(path).unwrap();

        assert_eq!(loaded.format_version, 2);
        assert_eq!(loaded.db_version, 0);
        assert_eq!(loaded.schema_ir_hash, "abc123");
        assert_eq!(loaded.next_node_id, 42);
        assert_eq!(loaded.next_edge_id, 10);
        assert_eq!(loaded.datasets.len(), 1);
        assert_eq!(loaded.datasets[0].type_name, "Person");
        assert_eq!(loaded.datasets[0].dataset_path, "nodes/00000064");
        assert_eq!(loaded.datasets[0].dataset_version, 7);
    }

    #[test]
    fn test_atomic_write_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        let manifest = GraphManifest::new("def456".to_string());
        manifest.write_atomic(path).unwrap();

        assert!(path.join("graph.manifest.json").exists());
        // tmp file should be gone (renamed)
        assert!(!path.join("graph.manifest.json.tmp").exists());
    }
}
