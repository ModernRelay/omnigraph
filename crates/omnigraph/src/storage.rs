use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;

pub trait StorageAdapter: Debug + Send + Sync {
    fn read_text(&self, uri: &str) -> Result<String>;
    fn write_text(&self, uri: &str, contents: &str) -> Result<()>;
    fn exists(&self, uri: &str) -> Result<bool>;
}

#[derive(Debug, Default)]
pub struct LocalStorageAdapter;

impl StorageAdapter for LocalStorageAdapter {
    fn read_text(&self, uri: &str) -> Result<String> {
        Ok(std::fs::read_to_string(uri)?)
    }

    fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        std::fs::write(uri, contents)?;
        Ok(())
    }

    fn exists(&self, uri: &str) -> Result<bool> {
        Ok(Path::new(uri).exists())
    }
}

pub fn default_storage() -> Arc<dyn StorageAdapter> {
    Arc::new(LocalStorageAdapter)
}

pub fn normalize_root_uri(uri: &str) -> String {
    uri.trim_end_matches('/').to_string()
}

pub fn join_uri(root_uri: &str, relative_path: &str) -> String {
    format!(
        "{}/{}",
        root_uri.trim_end_matches('/'),
        relative_path.trim_start_matches('/')
    )
}
