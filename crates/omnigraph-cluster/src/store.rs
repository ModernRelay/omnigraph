//! The cluster's storage layer: every stored byte (state ledger, lock,
//! recovery sidecars, approval artifacts, catalog payloads) goes through the
//! engine's `StorageAdapter`, so `file://` and `s3://` are one code path
//! (RFC-006). Declared configuration — `cluster.yaml` and the schema/query/
//! policy sources it references — deliberately does NOT live here: config is
//! read from the operator's working tree (Terraform's config-local /
//! state-remote split).
//!
//! Raw `fs::*` for cluster state outside this module is a deny-list entry.

use std::path::Path;
use std::process;
use std::sync::Arc;

use omnigraph::storage::{StorageAdapter, StorageKind, storage_for_uri, storage_kind_for_uri};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

use crate::{
    ApprovalArtifact, CLUSTER_APPROVALS_DIR, CLUSTER_LOCK_FILE, CLUSTER_RECOVERIES_DIR,
    CLUSTER_RESOURCES_DIR, CLUSTER_STATE_FILE, ClusterState, Diagnostic, RecoverySidecar,
    ResourceKind, StateLockFile, StateObservations, sha256_hex,
};

#[derive(Debug, Clone)]
pub(crate) struct ClusterStore {
    adapter: Arc<dyn StorageAdapter>,
    /// Normalized storage-root URI, no trailing slash: `file:///abs/dir`
    /// (the default config-dir layout) or `s3://bucket/prefix`.
    root: String,
    /// What observations/diagnostics display for stored locations: the plain
    /// local path for `file://` roots (byte-compatible with the pre-store
    /// outputs), the URI otherwise.
    display_root: String,
}

#[derive(Debug)]
pub(crate) struct StateSnapshot {
    pub(crate) state: Option<ClusterState>,
    /// Content identity (`sha256:<hex>`) — the public CAS vocabulary.
    pub(crate) state_cas: Option<String>,
}

#[derive(Debug)]
pub(crate) struct StateLockGuard {
    adapter: Arc<dyn StorageAdapter>,
    uri: String,
    kind: StorageKind,
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
        match self.kind {
            // Deterministic release on the file backend (tests assert the
            // lock is gone the moment a command returns).
            StorageKind::Local => {
                let path = self.uri.trim_start_matches("file://");
                let _ = std::fs::remove_file(path);
            }
            // Object stores need an async delete, and it must COMPLETE
            // before a short-lived CLI process exits — a spawned task dies
            // with the runtime and leaks the lock (caught by the s3 smoke
            // test: import's lock survived into the next command). On the
            // multi-thread runtime (the CLI and the gated s3 tests),
            // block_in_place waits for the delete; on a current-thread
            // runtime that's not allowed, so fall back to a spawn —
            // best-effort, with `force-unlock` as the documented recovery,
            // same as a crash.
            StorageKind::S3 => {
                let adapter = Arc::clone(&self.adapter);
                let uri = self.uri.clone();
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                        tokio::task::block_in_place(move || {
                            handle.block_on(async move {
                                let _ = adapter.delete(&uri).await;
                            });
                        });
                    } else {
                        handle.spawn(async move {
                            let _ = adapter.delete(&uri).await;
                        });
                    }
                }
            }
        }
    }
}

impl ClusterStore {
    /// The default layout: storage root = the config directory itself
    /// (`file://<abs config dir>`), byte-compatible with every pre-existing
    /// cluster on disk.
    pub(crate) fn for_config_dir(config_dir: &Path) -> Self {
        let absolute =
            std::path::absolute(config_dir).unwrap_or_else(|_| config_dir.to_path_buf());
        let display_root = absolute
            .to_string_lossy()
            .trim_end_matches('/')
            .to_string();
        let root = format!("file://{display_root}");
        let adapter = storage_for_uri(&root)
            .expect("local storage adapter construction is infallible for file:// roots");
        Self {
            adapter,
            root,
            display_root,
        }
    }

    /// An explicit `storage:` root. `file://` URIs and plain paths normalize
    /// to the local backend; `s3://bucket/prefix` to the S3 backend (env-
    /// driven credentials/endpoint — the same contract as graph storage).
    pub(crate) fn for_storage_root(root_uri: &str) -> Result<Self, Diagnostic> {
        let trimmed = root_uri.trim_end_matches('/');
        if storage_kind_for_uri(trimmed) == StorageKind::Local {
            let path = trimmed.trim_start_matches("file://");
            return Ok(Self::for_config_dir(Path::new(path)));
        }
        let adapter = storage_for_uri(trimmed).map_err(|err| {
            Diagnostic::error(
                "storage_root_invalid",
                "storage",
                format!("could not initialize storage for '{root_uri}': {err}"),
            )
        })?;
        Ok(Self {
            adapter,
            root: trimmed.to_string(),
            display_root: trimmed.to_string(),
        })
    }

    pub(crate) fn kind(&self) -> StorageKind {
        storage_kind_for_uri(&self.root)
    }

    fn uri(&self, relative: &str) -> String {
        format!("{}/{}", self.root, relative)
    }

    fn display(&self, relative: &str) -> String {
        format!("{}/{}", self.display_root, relative)
    }

    /// Derived graph root for `<id>`: `<storage>/graphs/<id>.omni`. A plain
    /// local path for `file://` roots (byte-compatible, directly usable by
    /// the engine); the S3 URI the engine opens natively otherwise.
    pub(crate) fn graph_root(&self, graph_id: &str) -> String {
        match self.kind() {
            StorageKind::Local => format!("{}/graphs/{graph_id}.omni", self.display_root),
            StorageKind::S3 => format!("{}/graphs/{graph_id}.omni", self.root),
        }
    }

    /// Display-form storage root (plain local path for `file://`, URI for S3).
    pub(crate) fn display_root(&self) -> &str {
        &self.display_root
    }

    /// Whether this root holds the cluster state ledger (`__cluster/state.json`)
    /// — i.e. is an actual cluster, not just any directory. Probed via the
    /// adapter (`file://` or `s3://`), failures read as "not a cluster".
    pub(crate) async fn has_state(&self) -> bool {
        self.adapter
            .exists(&self.uri(CLUSTER_STATE_FILE))
            .await
            .unwrap_or(false)
    }

    /// `read_text_versioned`, returning None for a missing object (probed
    /// via `exists` — the engine error type doesn't discriminate NotFound).
    async fn read_versioned_opt(&self, uri: &str) -> Result<Option<(String, String)>, String> {
        match self.adapter.exists(uri).await {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(err) => return Err(err.to_string()),
        }
        self.adapter
            .read_text_versioned(uri)
            .await
            .map(Some)
            .map_err(|err| err.to_string())
    }

    /// JSON object write. Atomic visibility is the storage adapter's
    /// contract on every backend (staged temp + rename on the filesystem,
    /// a single atomic PUT on object stores) — no torn JSON after a crash,
    /// no per-backend branch needed here.
    async fn put_json(&self, relative: &str, payload: &str) -> Result<(), String> {
        let target = self.uri(relative);
        self.adapter
            .write_text(&target, payload)
            .await
            .map_err(|err| err.to_string())
    }

    /// Shared list-and-parse for the sidecar/approval directories: id
    /// (filename) order; unparseable objects warn and stay for the operator.
    async fn list_json_dir<T: serde::de::DeserializeOwned>(
        &self,
        dir: &str,
        diagnostics: &mut Vec<Diagnostic>,
        list_error_code: &'static str,
        parse_error_code: &'static str,
        version_ok: impl Fn(&T) -> bool,
        version_error_code: &'static str,
    ) -> Vec<(String, T)> {
        let dir_uri = self.uri(dir);
        let mut uris = match self.adapter.list_dir(&dir_uri).await {
            Ok(uris) => uris,
            Err(err) => {
                diagnostics.push(Diagnostic::warning(
                    list_error_code,
                    dir,
                    format!("could not list '{dir}': {err}"),
                ));
                return Vec::new();
            }
        };
        uris.retain(|uri| uri.ends_with(".json"));
        uris.sort();
        let mut out = Vec::new();
        for uri in uris {
            match self.adapter.read_text(&uri).await {
                Ok(text) => match serde_json::from_str::<T>(&text) {
                    Ok(value) if version_ok(&value) => out.push((uri, value)),
                    Ok(_) => diagnostics.push(Diagnostic::warning(
                        version_error_code,
                        uri.clone(),
                        "unsupported schema version; leaving it in place".to_string(),
                    )),
                    Err(err) => diagnostics.push(Diagnostic::warning(
                        parse_error_code,
                        uri.clone(),
                        format!("could not parse ({err}); leaving it in place"),
                    )),
                },
                Err(err) => diagnostics.push(Diagnostic::warning(
                    parse_error_code,
                    uri.clone(),
                    format!("could not read ({err}); leaving it in place"),
                )),
            }
        }
        out
    }

    /// Best-effort object removal (sidecar retirement after a CAS lands,
    /// lock cleanup) — failures are recoverable by the next sweep.
    pub(crate) async fn delete_object(&self, uri: &str) {
        let _ = self.adapter.delete(uri).await;
    }

    /// Recursive prefix delete for graph roots (approved deletes). Idempotent;
    /// S3 non-atomicity is tolerated by the delete protocol's retry shape.
    pub(crate) async fn delete_graph_root(&self, graph_uri: &str) -> Result<(), String> {
        self.adapter
            .delete_prefix(graph_uri)
            .await
            .map_err(|err| err.to_string())
    }

    /// Existence probe for graph roots in sweep classification. A bare local
    /// path or any URI works — resolved through the same adapter machinery
    /// the engine uses.
    pub(crate) async fn graph_root_exists(&self, graph_uri: &str) -> bool {
        match storage_kind_for_uri(graph_uri) {
            StorageKind::Local => Path::new(graph_uri.trim_start_matches("file://")).exists(),
            StorageKind::S3 => match storage_for_uri(graph_uri) {
                Ok(adapter) => !adapter
                    .list_dir(graph_uri)
                    .await
                    .map(|entries| entries.is_empty())
                    .unwrap_or(true),
                Err(_) => false,
            },
        }
    }

    // ---- approvals ----

    pub(crate) async fn list_approval_artifacts(
        &self,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<(String, ApprovalArtifact)> {
        self.list_json_dir(
            CLUSTER_APPROVALS_DIR,
            diagnostics,
            "approval_read_error",
            "invalid_approval_artifact",
            |artifact: &ApprovalArtifact| artifact.schema_version == 1,
            "unsupported_approval_version",
        )
        .await
    }

    pub(crate) async fn write_approval_artifact(
        &self,
        artifact: &ApprovalArtifact,
    ) -> Result<String, Diagnostic> {
        let relative = format!("{CLUSTER_APPROVALS_DIR}/{}.json", artifact.approval_id);
        let mut payload = serde_json::to_string_pretty(artifact).map_err(|err| {
            Diagnostic::error(
                "approval_write_error",
                self.display(&relative),
                format!("could not encode approval artifact: {err}"),
            )
        })?;
        payload.push('\n');
        self.put_json(&relative, &payload).await.map_err(|err| {
            Diagnostic::error(
                "approval_write_error",
                self.display(&relative),
                format!("could not write approval artifact: {err}"),
            )
        })?;
        Ok(self.uri(&relative))
    }

    // ---- recovery sidecars ----

    pub(crate) async fn list_recovery_sidecar_locations(
        &self,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<String> {
        let dir_uri = self.uri(CLUSTER_RECOVERIES_DIR);
        let mut uris = match self.adapter.list_dir(&dir_uri).await {
            Ok(uris) => uris,
            Err(err) => {
                diagnostics.push(Diagnostic::warning(
                    "recovery_sidecar_read_error",
                    CLUSTER_RECOVERIES_DIR,
                    format!("could not list '{CLUSTER_RECOVERIES_DIR}': {err}"),
                ));
                return Vec::new();
            }
        };
        uris.retain(|uri| uri.ends_with(".json"));
        uris.sort();
        uris.into_iter()
            .map(|uri| match uri.rsplit('/').next() {
                Some(name) => format!("{}/{name}", self.display(CLUSTER_RECOVERIES_DIR)),
                None => uri,
            })
            .collect()
    }

    pub(crate) async fn list_recovery_sidecars(
        &self,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<(String, RecoverySidecar)> {
        self.list_json_dir(
            CLUSTER_RECOVERIES_DIR,
            diagnostics,
            "recovery_sidecar_read_error",
            "invalid_recovery_sidecar",
            |sidecar: &RecoverySidecar| sidecar.schema_version == 1,
            "unsupported_recovery_sidecar_version",
        )
        .await
    }

    pub(crate) async fn write_recovery_sidecar(
        &self,
        sidecar: &RecoverySidecar,
    ) -> Result<String, Diagnostic> {
        let relative = format!("{CLUSTER_RECOVERIES_DIR}/{}.json", sidecar.operation_id);
        let mut payload = serde_json::to_string_pretty(sidecar).map_err(|err| {
            Diagnostic::error(
                "recovery_sidecar_write_error",
                self.display(&relative),
                format!("could not encode recovery sidecar: {err}"),
            )
        })?;
        payload.push('\n');
        self.put_json(&relative, &payload).await.map_err(|err| {
            Diagnostic::error(
                "recovery_sidecar_write_error",
                self.display(&relative),
                format!("could not write recovery sidecar: {err}"),
            )
        })?;
        Ok(self.uri(&relative))
    }

    // ---- catalog payloads ----

    /// Content-addressed catalog location for a query/policy payload
    /// (extensions fixed per kind, same as the pre-port layout).
    pub(crate) fn payload_relative(kind: &ResourceKind, digest: &str) -> Option<String> {
        match kind {
            ResourceKind::Query { graph, name } => Some(format!(
                "{CLUSTER_RESOURCES_DIR}/query/{graph}/{name}/{digest}.gq"
            )),
            ResourceKind::Policy(name) => Some(format!(
                "{CLUSTER_RESOURCES_DIR}/policy/{name}/{digest}.yaml"
            )),
            _ => None,
        }
    }

    pub(crate) fn payload_display(&self, kind: &ResourceKind, digest: &str) -> Option<String> {
        Self::payload_relative(kind, digest).map(|relative| self.display(&relative))
    }

    pub(crate) async fn payload_exists(&self, kind: &ResourceKind, digest: &str) -> bool {
        let Some(relative) = Self::payload_relative(kind, digest) else {
            return false;
        };
        self.adapter
            .exists(&self.uri(&relative))
            .await
            .unwrap_or(false)
    }

    /// Raw payload read: `Ok(None)` for a missing blob, `Err` for transport
    /// failures — callers classify (verify loops need the three-way split).
    pub(crate) async fn read_payload(
        &self,
        kind: &ResourceKind,
        digest: &str,
    ) -> Result<Option<String>, String> {
        let Some(relative) = Self::payload_relative(kind, digest) else {
            return Ok(None);
        };
        let uri = self.uri(&relative);
        match self.adapter.exists(&uri).await {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(err) => return Err(err.to_string()),
        }
        self.adapter
            .read_text(&uri)
            .await
            .map(Some)
            .map_err(|err| {
                format!(
                    "could not read catalog payload '{}': {err}",
                    self.display(&relative)
                )
            })
    }

    /// Idempotent content-addressed write: a payload already present at its
    /// digest is by definition identical.
    pub(crate) async fn write_payload(
        &self,
        kind: &ResourceKind,
        digest: &str,
        content: &str,
    ) -> Result<(), String> {
        let Some(relative) = Self::payload_relative(kind, digest) else {
            return Err("resource kind has no payload".to_string());
        };
        if self
            .adapter
            .exists(&self.uri(&relative))
            .await
            .map_err(|err| err.to_string())?
        {
            return Ok(());
        }
        self.put_json(&relative, content).await
    }

    /// Read a catalog payload and verify it against its recorded digest.
    pub(crate) async fn read_verified_payload(
        &self,
        kind: &ResourceKind,
        digest: &str,
        address: &str,
    ) -> Result<String, Diagnostic> {
        let Some(relative) = Self::payload_relative(kind, digest) else {
            return Err(Diagnostic::error(
                "catalog_payload_missing",
                address,
                "resource kind has no payload",
            ));
        };
        let uri = self.uri(&relative);
        let text = self.adapter.read_text(&uri).await.map_err(|err| {
            Diagnostic::error(
                "catalog_payload_missing",
                address,
                format!(
                    "catalog blob '{}' unreadable ({err}); run `cluster refresh` then `cluster apply`, and restart",
                    self.display(&relative)
                ),
            )
        })?;
        if sha256_hex(text.as_bytes()) != digest {
            return Err(Diagnostic::error(
                "catalog_payload_digest_mismatch",
                address,
                format!(
                    "catalog blob '{}' does not match its recorded digest; run `cluster refresh` then `cluster apply`, and restart",
                    self.display(&relative)
                ),
            ));
        }
        Ok(text)
    }

    // ---- observations ----

    pub(crate) fn observations(&self) -> StateObservations {
        StateObservations {
            state_path: self.display(CLUSTER_STATE_FILE),
            lock_path: self.display(CLUSTER_LOCK_FILE),
            state_found: false,
            applied_config_digest: None,
            state_revision: 0,
            state_cas: None,
            resource_count: 0,
            locked: false,
            lock_id: None,
            lock_acquired: false,
            acquired_lock_id: None,
            lock_operation: None,
            lock_created_at: None,
            lock_pid: None,
            lock_age_seconds: None,
        }
    }

    // ---- state ledger ----

    pub(crate) async fn read_state(
        &self,
        observations: &mut StateObservations,
    ) -> Result<StateSnapshot, Diagnostic> {
        let state_uri = self.uri(CLUSTER_STATE_FILE);
        let (text, _version) = match self.read_versioned_opt(&state_uri).await {
            Ok(Some(read)) => read,
            Ok(None) => {
                return Ok(StateSnapshot {
                    state: None,
                    state_cas: None,
                });
            }
            Err(err) => {
                return Err(Diagnostic::error(
                    "state_read_error",
                    CLUSTER_STATE_FILE,
                    format!("could not read state file: {err}"),
                ));
            }
        };

        observations.state_found = true;
        let state_cas = format!("sha256:{}", sha256_hex(text.as_bytes()));
        observations.state_cas = Some(state_cas.clone());

        let state = serde_json::from_str::<ClusterState>(&text).map_err(|err| {
            Diagnostic::error(
                "invalid_state_json",
                CLUSTER_STATE_FILE,
                format!("could not parse state JSON: {err}"),
            )
        })?;

        if state.version != 1 {
            return Err(Diagnostic::error(
                "unsupported_state_version",
                "state.version",
                format!(
                    "unsupported cluster state version {}; this build supports version 1",
                    state.version
                ),
            ));
        }

        observations.applied_config_digest = state.applied_revision.config_digest.clone();
        observations.state_revision = state.state_revision;
        observations.resource_count = state.applied_revision.resources.len();

        Ok(StateSnapshot {
            state: Some(state),
            state_cas: Some(state_cas),
        })
    }

    /// CAS-guarded ledger replace. The public contract stays content-level
    /// (`expected_cas` = `sha256:<hex>` from the snapshot the command read);
    /// the physical swap is token-conditioned on a fresh read, so a writer
    /// that raced us between the fresh read and the put loses with
    /// `state_cas_mismatch` — never a silent overwrite. On S3 the token is
    /// the object's ETag and the put is conditional (If-Match); locally it
    /// is a content token over the same temp+rename flow as before the port.
    pub(crate) async fn write_state(
        &self,
        state: &ClusterState,
        expected_cas: Option<&str>,
        observations: &mut StateObservations,
    ) -> Result<(), Diagnostic> {
        let state_uri = self.uri(CLUSTER_STATE_FILE);
        let current = self.read_versioned_opt(&state_uri).await.map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not read state file before write: {err}"),
            )
        })?;
        let current_cas = current
            .as_ref()
            .map(|(text, _)| format!("sha256:{}", sha256_hex(text.as_bytes())));
        if current_cas.as_deref() != expected_cas {
            return Err(state_cas_mismatch());
        }

        let mut payload = serde_json::to_string_pretty(state).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not encode state JSON: {err}"),
            )
        })?;
        payload.push('\n');

        let written = match current {
            None => self
                .adapter
                .write_text_if_absent(&state_uri, &payload)
                .await
                .map_err(|err| {
                    Diagnostic::error(
                        "state_write_error",
                        CLUSTER_STATE_FILE,
                        format!("could not create state.json: {err}"),
                    )
                })?,
            Some((_, version)) => self
                .adapter
                .write_text_if_match(&state_uri, &payload, &version)
                .await
                .map_err(|err| {
                    Diagnostic::error(
                        "state_write_error",
                        CLUSTER_STATE_FILE,
                        format!("could not replace state.json: {err}"),
                    )
                })?
                .is_some(),
        };
        if !written {
            return Err(state_cas_mismatch());
        }

        observations.state_found = true;
        observations.applied_config_digest = state.applied_revision.config_digest.clone();
        observations.state_revision = state.state_revision;
        observations.state_cas = Some(format!("sha256:{}", sha256_hex(payload.as_bytes())));
        observations.resource_count = state.applied_revision.resources.len();
        Ok(())
    }

    // ---- lock ----

    pub(crate) async fn acquire_lock(
        &self,
        operation: &str,
        observations: &mut StateObservations,
    ) -> Result<StateLockGuard, Diagnostic> {
        let lock_uri = self.uri(CLUSTER_LOCK_FILE);
        let lock_id = Ulid::new().to_string();
        let lock = StateLockFile {
            version: 1,
            lock_id: lock_id.clone(),
            operation: operation.to_string(),
            created_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string()),
            pid: process::id(),
        };
        let payload = serde_json::to_string_pretty(&lock).map_err(|err| {
            Diagnostic::error(
                "state_lock_error",
                CLUSTER_LOCK_FILE,
                format!("could not encode state lock: {err}"),
            )
        })?;

        match self.adapter.write_text_if_absent(&lock_uri, &payload).await {
            Ok(true) => {
                observations.lock_acquired = true;
                observations.acquired_lock_id = Some(lock_id);
                Ok(StateLockGuard {
                    adapter: Arc::clone(&self.adapter),
                    uri: lock_uri,
                    kind: self.kind(),
                })
            }
            Ok(false) => {
                self.observe_lock_metadata_lossy(observations).await;
                Err(Diagnostic::error(
                    "state_lock_held",
                    CLUSTER_LOCK_FILE,
                    state_lock_held_message(observations),
                ))
            }
            Err(err) => Err(Diagnostic::error(
                "state_lock_error",
                CLUSTER_LOCK_FILE,
                format!("could not write state lock: {err}"),
            )),
        }
    }

    pub(crate) async fn force_unlock(
        &self,
        lock_id: &str,
        observations: &mut StateObservations,
    ) -> Result<(), Diagnostic> {
        let lock_uri = self.uri(CLUSTER_LOCK_FILE);
        let text = match self.read_versioned_opt(&lock_uri).await {
            Ok(Some((text, _))) => text,
            Ok(None) => {
                return Err(Diagnostic::error(
                    "state_lock_missing",
                    CLUSTER_LOCK_FILE,
                    "no cluster state lock is present",
                ));
            }
            Err(err) => {
                return Err(Diagnostic::error(
                    "state_lock_read_error",
                    CLUSTER_LOCK_FILE,
                    format!("could not read state lock: {err}"),
                ));
            }
        };
        let lock = parse_lock_file_for_unlock(&text)?;
        observations.observe_lock_metadata(&lock);
        observations.locked = true;
        if lock.lock_id != lock_id {
            return Err(Diagnostic::error(
                "state_lock_id_mismatch",
                CLUSTER_LOCK_FILE,
                format!(
                    "lock id mismatch: held lock is {}, refusing to remove (pass the exact id from `cluster status`)",
                    lock.lock_id
                ),
            ));
        }
        self.adapter.delete(&lock_uri).await.map_err(|err| {
            Diagnostic::error(
                "state_lock_error",
                CLUSTER_LOCK_FILE,
                format!("could not remove state lock: {err}"),
            )
        })?;
        observations.locked = false;
        Ok(())
    }

    pub(crate) async fn observe_lock(
        &self,
        observations: &mut StateObservations,
        diagnostics: &mut Vec<Diagnostic>,
    ) {
        let lock_uri = self.uri(CLUSTER_LOCK_FILE);
        match self.read_versioned_opt(&lock_uri).await {
            Ok(Some((text, _))) => {
                observations.locked = true;
                match serde_json::from_str::<StateLockFile>(&text) {
                    Ok(lock) if lock.version == 1 => observations.observe_lock_metadata(&lock),
                    Ok(lock) => diagnostics.push(Diagnostic::warning(
                        "unsupported_state_lock_version",
                        CLUSTER_LOCK_FILE,
                        format!("unsupported cluster state lock version {}", lock.version),
                    )),
                    Err(err) => diagnostics.push(Diagnostic::warning(
                        "invalid_state_lock",
                        CLUSTER_LOCK_FILE,
                        format!("could not parse state lock: {err}"),
                    )),
                }
            }
            Ok(None) => {}
            Err(err) => diagnostics.push(Diagnostic::warning(
                "state_lock_read_error",
                CLUSTER_LOCK_FILE,
                format!("could not read state lock: {err}"),
            )),
        }
    }

    pub(crate) async fn observe_lock_metadata_lossy(
        &self,
        observations: &mut StateObservations,
    ) {
        observations.locked = true;
        let lock_uri = self.uri(CLUSTER_LOCK_FILE);
        if let Ok(Some((text, _))) = self.read_versioned_opt(&lock_uri).await {
            if let Ok(lock) = serde_json::from_str::<StateLockFile>(&text) {
                if lock.version == 1 {
                    observations.observe_lock_metadata(&lock);
                }
            }
        }
    }
}

fn state_cas_mismatch() -> Diagnostic {
    Diagnostic::error(
        "state_cas_mismatch",
        CLUSTER_STATE_FILE,
        "state.json changed while the command was running; re-run the command against the latest state",
    )
}

pub(crate) fn parse_lock_file_for_unlock(text: &str) -> Result<StateLockFile, Diagnostic> {
    let lock = serde_json::from_str::<StateLockFile>(text).map_err(|err| {
        Diagnostic::error(
            "invalid_state_lock",
            CLUSTER_LOCK_FILE,
            format!("could not parse state lock: {err}"),
        )
    })?;
    if lock.version != 1 {
        return Err(Diagnostic::error(
            "unsupported_state_lock_version",
            CLUSTER_LOCK_FILE,
            format!("unsupported cluster state lock version {}", lock.version),
        ));
    }
    Ok(lock)
}

pub(crate) fn state_lock_held_message(observations: &StateObservations) -> String {
    match observations.lock_id.as_deref() {
        Some(lock_id) => format!(
            "cluster state lock already exists (lock id {lock_id}); run `omnigraph cluster force-unlock {lock_id}` only after confirming no cluster operation is active"
        ),
        None => "cluster state lock already exists; remove it only after confirming no cluster operation is active".to_string(),
    }
}
