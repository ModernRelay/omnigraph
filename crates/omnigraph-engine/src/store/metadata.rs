use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{SchemaIR, build_catalog_from_ir};
use crate::error::{NanoError, Result};
use crate::store::manifest::{DatasetEntry, GraphManifest, hash_string};
use crate::store::migration::reconcile_migration_sidecars;
use crate::store::txlog::reconcile_logs_to_manifest;

pub const SCHEMA_IR_FILENAME: &str = "schema.ir.json";

#[derive(Debug, Clone)]
pub struct DatasetLocator {
    pub dataset_path: PathBuf,
    pub dataset_version: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone)]
pub struct DatabaseMetadata {
    path: PathBuf,
    schema_ir: SchemaIR,
    catalog: Catalog,
    manifest: GraphManifest,
    dataset_map: HashMap<(String, String), DatasetEntry>,
}

impl DatabaseMetadata {
    pub fn open(db_path: &Path) -> Result<Self> {
        reconcile_migration_sidecars(db_path)?;
        if !db_path.exists() {
            return Err(NanoError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("database not found: {}", db_path.display()),
            )));
        }

        let ir_json = std::fs::read_to_string(db_path.join(SCHEMA_IR_FILENAME))?;
        let schema_ir: SchemaIR = serde_json::from_str(&ir_json)
            .map_err(|e| NanoError::Manifest(format!("parse IR error: {}", e)))?;
        let catalog = build_catalog_from_ir(&schema_ir)?;
        let manifest = GraphManifest::read(db_path)?;

        let computed_hash = hash_string(&ir_json);
        if computed_hash != manifest.schema_ir_hash {
            return Err(NanoError::Manifest(format!(
                "schema mismatch: schema.ir.json has been modified since last load \
                 (expected hash {}, got {}). Re-run 'nanograph load' to update.",
                &manifest.schema_ir_hash[..8.min(manifest.schema_ir_hash.len())],
                &computed_hash[..8.min(computed_hash.len())]
            )));
        }
        reconcile_logs_to_manifest(db_path, manifest.db_version)?;

        let dataset_map = manifest
            .datasets
            .iter()
            .cloned()
            .map(|entry| ((entry.kind.clone(), entry.type_name.clone()), entry))
            .collect();

        Ok(Self {
            path: db_path.to_path_buf(),
            schema_ir,
            catalog,
            manifest,
            dataset_map,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn schema_ir(&self) -> &SchemaIR {
        &self.schema_ir
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn manifest(&self) -> &GraphManifest {
        &self.manifest
    }

    pub fn dataset_entry(&self, kind: &str, type_name: &str) -> Option<&DatasetEntry> {
        self.dataset_map
            .get(&(kind.to_string(), type_name.to_string()))
    }

    pub fn dataset_locator(&self, kind: &str, type_name: &str) -> Option<DatasetLocator> {
        self.dataset_entry(kind, type_name)
            .map(|entry| DatasetLocator {
                dataset_path: self.path.join(&entry.dataset_path),
                dataset_version: entry.dataset_version,
                row_count: entry.row_count,
            })
    }

    pub fn node_dataset_locator(&self, type_name: &str) -> Option<DatasetLocator> {
        self.dataset_locator("node", type_name)
    }

    pub fn edge_dataset_locator(&self, type_name: &str) -> Option<DatasetLocator> {
        self.dataset_locator("edge", type_name)
    }
}
