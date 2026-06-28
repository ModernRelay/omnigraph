//! A fault-injecting `StorageAdapter` wrapper (SlateDB-style). Wrap the base
//! adapter and inject seeded faults on the storage-adapter conditional-write
//! seam, then open the graph via `Omnigraph::open_with_storage`.
//!
//! The fault is a spurious CAS-lost on `write_text_if_match` — the
//! StorageAdapter conditional TEXT-OBJECT write (recovery sidecars, schema
//! staging, cluster state). SCOPE NOTE: this is **not** the Lance
//! manifest-publish CAS — graph data commits publish `__manifest` through Lance
//! `MergeInsertBuilder`/`ManifestBatchPublisher`, which bypasses this adapter,
//! so a pure-data op walk exercises few/no `write_text_if_match` calls and the
//! manifest-publish fault path is covered SEPARATELY by the failpoint recovery
//! cells (`dst_recovery`), not here. Where a conditional write IS lost, the
//! engine must surface/retry it (never silently drop it); `check_counts`
//! (count==model) catches a swallowed loss as a NOVEL violation.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use omnigraph::error::Result;
use omnigraph::storage::StorageAdapter;

#[derive(Debug)]
pub struct FaultAdapter {
    inner: Arc<dyn StorageAdapter>,
    cas_conflict_pct: u8,
    rng: Mutex<u64>,
}

impl FaultAdapter {
    /// Wrap `inner` (e.g. `storage_for_uri(uri)?`). `cas_conflict_pct` is the
    /// percentage of conditional writes that spuriously report CAS-lost.
    pub fn new(
        inner: Arc<dyn StorageAdapter>,
        seed: u64,
        cas_conflict_pct: u8,
    ) -> Arc<dyn StorageAdapter> {
        Arc::new(Self {
            inner,
            cas_conflict_pct,
            rng: Mutex::new(seed ^ 0x9E37_79B9_7F4A_7C15),
        })
    }

    fn roll(&self, pct: u8) -> bool {
        let mut g = self.rng.lock().unwrap();
        let mut x = *g;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *g = x;
        (x.wrapping_mul(0x2545_F491_4F6C_DD1D) % 100) < pct as u64
    }
}

#[async_trait]
impl StorageAdapter for FaultAdapter {
    async fn read_text(&self, uri: &str) -> Result<String> {
        self.inner.read_text(uri).await
    }
    async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        self.inner.write_text(uri, contents).await
    }
    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool> {
        self.inner.write_text_if_absent(uri, contents).await
    }
    async fn exists(&self, uri: &str) -> Result<bool> {
        self.inner.exists(uri).await
    }
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
        self.inner.rename_text(from_uri, to_uri).await
    }
    async fn delete(&self, uri: &str) -> Result<()> {
        self.inner.delete(uri).await
    }
    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
        self.inner.list_dir(dir_uri).await
    }
    async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)> {
        self.inner.read_text_versioned(uri).await
    }
    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> Result<Option<String>> {
        // Inject a spurious CAS-lost (precondition failed) — a concurrent-writer
        // illusion. The engine must surface/retry this; a swallowed loss is
        // caught downstream by count==model.
        if self.roll(self.cas_conflict_pct) {
            return Ok(None);
        }
        self.inner
            .write_text_if_match(uri, contents, expected_version)
            .await
    }
    async fn delete_prefix(&self, prefix_uri: &str) -> Result<()> {
        self.inner.delete_prefix(prefix_uri).await
    }
}
