use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait StorageAdapter: Debug + Send + Sync {
    async fn read_text(&self, uri: &str) -> Result<String>;
    async fn write_text(&self, uri: &str, contents: &str) -> Result<()>;
    async fn exists(&self, uri: &str) -> Result<bool>;
}

#[derive(Debug, Default)]
pub struct LocalStorageAdapter;

#[async_trait]
impl StorageAdapter for LocalStorageAdapter {
    async fn read_text(&self, uri: &str) -> Result<String> {
        Ok(tokio::fs::read_to_string(uri).await?)
    }

    async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        tokio::fs::write(uri, contents).await?;
        Ok(())
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
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
