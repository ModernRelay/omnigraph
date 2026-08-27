//! Write firewall for the pre-measurement read-write engine open.
//!
//! A local read-write bind deliberately creates and deletes one empty
//! `__create_if_absent_probe_*` object to prove the mount's atomic primitive.
//! Clean fixture recovery must perform no other write. This adapter permits
//! exactly that balanced capability probe while the runner prepares caches,
//! then opens the full mutation surface only at the measurement gate.

use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use omnigraph::error::OmniError;
use omnigraph::storage::{ListDirBounds, StorageAdapter};

const PROBE_PREFIX: &str = "__create_if_absent_probe_";
const MAX_PROBE_ATTEMPTS: u32 = 4;

#[derive(Debug, Default)]
struct PreparationState {
    measuring: bool,
    attempts: u32,
    successful_claims: u32,
    deletes: u32,
    claimed_uri: Option<String>,
}

/// Shared authority that validates preparation and opens the measurement gate.
#[derive(Debug, Clone)]
pub struct PreparationWriteGate {
    root: String,
    state: Arc<Mutex<PreparationState>>,
}

impl PreparationWriteGate {
    /// Prove preparation performed exactly one balanced capability probe
    /// without granting mutation authority.
    pub fn validate_preparation(&self) -> Result<(), String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "pre-measurement storage gate was poisoned".to_string())?;
        validate_preparation_state(&state)
    }

    /// Require exactly one balanced local capability probe, then permit the
    /// measured mutation. Calling this twice is an error.
    pub fn begin_measurement(&self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pre-measurement storage gate was poisoned".to_string())?;
        if state.measuring {
            return Err("pre-measurement storage gate was opened twice".to_string());
        }
        validate_preparation_state(&state)?;
        state.measuring = true;
        Ok(())
    }

    fn valid_probe_uri(&self, uri: &str) -> bool {
        let root = self.root.trim_end_matches('/');
        let Some(name) = uri
            .strip_prefix(root)
            .and_then(|suffix| suffix.strip_prefix('/'))
        else {
            return false;
        };
        name.starts_with(PROBE_PREFIX) && name.len() > PROBE_PREFIX.len() && !name.contains('/')
    }
}

fn validate_preparation_state(state: &PreparationState) -> Result<(), String> {
    if state.attempts == 0
        || state.attempts > MAX_PROBE_ATTEMPTS
        || state.successful_claims != 1
        || state.deletes != 1
        || state.claimed_uri.is_some()
    {
        return Err(format!(
            "read-write open did not perform exactly one balanced local capability probe (attempts={}, successful_claims={}, deletes={}, outstanding={:?})",
            state.attempts, state.successful_claims, state.deletes, state.claimed_uri
        ));
    }
    Ok(())
}

/// Wrap `inner` with the preparation firewall.
pub fn guard_preparation_writes(
    inner: Arc<dyn StorageAdapter>,
    root: &str,
) -> (Arc<dyn StorageAdapter>, PreparationWriteGate) {
    let gate = PreparationWriteGate {
        root: root.trim_end_matches('/').to_string(),
        state: Arc::new(Mutex::new(PreparationState::default())),
    };
    let adapter: Arc<dyn StorageAdapter> = Arc::new(PreparationStorageAdapter {
        inner,
        gate: gate.clone(),
    });
    (adapter, gate)
}

struct PreparationStorageAdapter {
    inner: Arc<dyn StorageAdapter>,
    gate: PreparationWriteGate,
}

impl fmt::Debug for PreparationStorageAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparationStorageAdapter")
            .field("root", &self.gate.root)
            .finish_non_exhaustive()
    }
}

impl PreparationStorageAdapter {
    fn measuring(&self) -> omnigraph::error::Result<bool> {
        self.gate
            .state
            .lock()
            .map(|state| state.measuring)
            .map_err(|_| OmniError::manifest_internal("pre-measurement storage gate was poisoned"))
    }

    fn denied(&self, operation: &str, uri: &str) -> OmniError {
        OmniError::manifest_internal(format!(
            "pre-measurement storage firewall denied {operation} at {uri}"
        ))
    }
}

#[async_trait]
impl StorageAdapter for PreparationStorageAdapter {
    async fn read_text(&self, uri: &str) -> omnigraph::error::Result<String> {
        self.inner.read_text(uri).await
    }

    async fn read_text_if_exists(&self, uri: &str) -> omnigraph::error::Result<Option<String>> {
        self.inner.read_text_if_exists(uri).await
    }

    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> omnigraph::error::Result<Option<String>> {
        self.inner.read_text_if_exists_bounded(uri, max_bytes).await
    }

    async fn write_text(&self, uri: &str, contents: &str) -> omnigraph::error::Result<()> {
        if !self.measuring()? {
            return Err(self.denied("write_text", uri));
        }
        self.inner.write_text(uri, contents).await
    }

    async fn write_text_if_absent(
        &self,
        uri: &str,
        contents: &str,
    ) -> omnigraph::error::Result<bool> {
        if self.measuring()? {
            return self.inner.write_text_if_absent(uri, contents).await;
        }
        if !contents.is_empty() || !self.gate.valid_probe_uri(uri) {
            return Err(self.denied("write_text_if_absent", uri));
        }
        {
            let mut state = self.gate.state.lock().map_err(|_| {
                OmniError::manifest_internal("pre-measurement storage gate was poisoned")
            })?;
            state.attempts = state.attempts.checked_add(1).ok_or_else(|| {
                OmniError::manifest_internal("capability-probe attempt counter overflowed")
            })?;
            if state.attempts > MAX_PROBE_ATTEMPTS || state.claimed_uri.is_some() {
                return Err(self.denied("extra write_text_if_absent", uri));
            }
        }
        let created = self.inner.write_text_if_absent(uri, contents).await?;
        if created {
            let mut state = self.gate.state.lock().map_err(|_| {
                OmniError::manifest_internal("pre-measurement storage gate was poisoned")
            })?;
            state.successful_claims = state.successful_claims.checked_add(1).ok_or_else(|| {
                OmniError::manifest_internal("capability-probe claim counter overflowed")
            })?;
            if state.successful_claims != 1 || state.claimed_uri.replace(uri.to_string()).is_some()
            {
                return Err(self.denied("extra capability-probe claim", uri));
            }
        }
        Ok(created)
    }

    async fn exists(&self, uri: &str) -> omnigraph::error::Result<bool> {
        self.inner.exists(uri).await
    }

    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> omnigraph::error::Result<()> {
        if !self.measuring()? {
            return Err(self.denied("rename_text", from_uri));
        }
        self.inner.rename_text(from_uri, to_uri).await
    }

    async fn delete(&self, uri: &str) -> omnigraph::error::Result<()> {
        if self.measuring()? {
            return self.inner.delete(uri).await;
        }
        {
            let state = self.gate.state.lock().map_err(|_| {
                OmniError::manifest_internal("pre-measurement storage gate was poisoned")
            })?;
            if state.claimed_uri.as_deref() != Some(uri) {
                return Err(self.denied("delete", uri));
            }
        }
        self.inner.delete(uri).await?;
        let mut state = self.gate.state.lock().map_err(|_| {
            OmniError::manifest_internal("pre-measurement storage gate was poisoned")
        })?;
        if state.claimed_uri.as_deref() != Some(uri) {
            return Err(self.denied("raced capability-probe delete", uri));
        }
        state.claimed_uri = None;
        state.deletes = state.deletes.checked_add(1).ok_or_else(|| {
            OmniError::manifest_internal("capability-probe delete counter overflowed")
        })?;
        Ok(())
    }

    async fn list_dir(&self, dir_uri: &str) -> omnigraph::error::Result<Vec<String>> {
        self.inner.list_dir(dir_uri).await
    }

    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: ListDirBounds,
    ) -> omnigraph::error::Result<Vec<String>> {
        self.inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await
    }

    async fn read_text_versioned(&self, uri: &str) -> omnigraph::error::Result<(String, String)> {
        self.inner.read_text_versioned(uri).await
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> omnigraph::error::Result<Option<String>> {
        if !self.measuring()? {
            return Err(self.denied("write_text_if_match", uri));
        }
        self.inner
            .write_text_if_match(uri, contents, expected_version)
            .await
    }

    async fn delete_prefix(&self, prefix_uri: &str) -> omnigraph::error::Result<()> {
        if !self.measuring()? {
            return Err(self.denied("delete_prefix", prefix_uri));
        }
        self.inner.delete_prefix(prefix_uri).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn preparation_allows_one_balanced_capability_probe_then_opens() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().to_str().unwrap();
        let inner = omnigraph::storage::storage_for_uri(root).unwrap();
        let (guarded, gate) = guard_preparation_writes(inner, root);
        let probe = format!("{root}/{PROBE_PREFIX}one");

        assert!(guarded.write_text_if_absent(&probe, "").await.unwrap());
        guarded.delete(&probe).await.unwrap();
        gate.begin_measurement().unwrap();
        guarded
            .write_text(&format!("{root}/measured"), "value")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn preparation_denies_every_non_probe_write() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().to_str().unwrap();
        let inner = omnigraph::storage::storage_for_uri(root).unwrap();
        let (guarded, _gate) = guard_preparation_writes(inner, root);

        let error = guarded
            .write_text(&format!("{root}/not-a-probe"), "value")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("firewall denied write_text"));
    }
}
