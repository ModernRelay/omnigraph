//! The cluster's storage layer: every stored byte (state ledger, lock,
//! recovery sidecars, approval artifacts, catalog payloads) goes through the
//! shared `StorageAdapter`, so `file://`, `s3://`, and `az://` are one code path
//! (RFC-006). Declared configuration — `cluster.yaml` and the schema/query/
//! policy sources it references — deliberately does NOT live here: config is
//! read from the operator's working tree (Terraform's config-local /
//! state-remote split).
//!
//! Raw `fs::*` for cluster state outside this module and the exact lock-release
//! guard in `state_lock` is a deny-list entry.

use std::path::Path;
use std::sync::Arc;

use omnigraph_storage::{
    StorageAdapter, StorageError, StorageHandle, StorageKind, normalize_root_uri,
    redacted_storage_uri, storage_handle_for_uri, storage_kind_for_uri,
};

use crate::deployment::{DeploymentBundle, MAX_BUNDLE_BYTES, MAX_LEDGER_BYTES, validate_state};
use crate::state_lock::{StateLockAcquire, StateLockError, StateLockGuard, acquire_state_lock};
use crate::{
    CLUSTER_LOCK_FILE, CLUSTER_RECOVERIES_DIR, CLUSTER_RESOURCES_DIR, CLUSTER_STATE_FILE,
    ClusterState, Diagnostic, RecoverySidecar, ResourceKind, StateLockFile, StateObservations,
    sha256_hex,
};

#[derive(Debug, Clone)]
pub(crate) struct ClusterStore {
    adapter: Arc<dyn StorageAdapter>,
    /// Concrete backend evidence used by lock/authority factories. Unlike the
    /// public adapter trait, callers cannot implement this handle.
    storage: StorageHandle,
    /// Normalized storage-root URI, no trailing slash: `file:///abs/dir`
    /// (the default config-dir layout), `s3://bucket/prefix`, or
    /// `az://container/prefix`.
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

/// Only explicit stopped-ledger conversion may remove obsolete runtime fields.
/// The remaining ledger is decoded by the ordinary strict types and validator.
fn decode_ledger(text: &str, upgrade: bool) -> Result<(ClusterState, bool), Diagnostic> {
    let invalid =
        |message: String| Diagnostic::error("invalid_state_json", CLUSTER_STATE_FILE, message);
    let strict_error = match serde_json::from_str::<ClusterState>(text) {
        Ok(state) => return Ok((state, false)),
        Err(error) => error,
    };
    if !upgrade {
        // Reuse the bounded converter only to recognize a qualified prior
        // receipt shape. Ordinary reads never consume the converted state.
        if matches!(decode_ledger(text, true), Ok((_, true))) {
            return Err(Diagnostic::error(
                "ledger_upgrade_required",
                CLUSTER_STATE_FILE,
                "completed receipts contain obsolete runtime fields; stop serving, writers and maintenance, establish prior I/O quiescence, then run `omnigraph --cluster <cluster-root> cluster upgrade-ledger --writers-stopped`",
            ));
        }
        return Err(invalid(strict_error.to_string()));
    }
    let mut value =
        omnigraph::loader::parse_unique_json(text).map_err(|error| invalid(error.to_string()))?;
    if value.get("version").and_then(serde_json::Value::as_u64) != Some(2) {
        return Err(invalid(strict_error.to_string()));
    }
    if value
        .get("outstanding")
        .is_some_and(|pending| !pending.is_null())
    {
        return Err(Diagnostic::error(
            "ledger_upgrade_pending",
            CLUSTER_STATE_FILE,
            "complete the outstanding deployment with its originating build before ledger conversion",
        ));
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PriorActivation {
        process_incarnation: String,
        result_revision: u64,
        config_digest: String,
    }
    let mut converted = false;
    let results = value
        .get_mut("deployment_results")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| invalid("missing deployment results".into()))?;
    let results_bytes = serde_json::to_vec(&results).map_err(|error| invalid(error.to_string()))?;
    if results_bytes.len() > crate::deployment::MAX_RESULTS_BYTES {
        return Err(invalid("obsolete results exceed the receipt bound".into()));
    }
    for result in results {
        if serde_json::to_vec(&result)
            .map_err(|error| invalid(error.to_string()))?
            .len()
            > crate::deployment::MAX_RESULT_BYTES
        {
            return Err(invalid("obsolete result exceeds the receipt bound".into()));
        }
        let fields = result
            .as_object_mut()
            .ok_or_else(|| invalid("invalid deployment result".into()))?;
        let restart = fields.remove("restart_required");
        let activation = fields.remove("activation");
        if restart.is_none() && activation.is_none() {
            continue;
        }
        let restart = restart
            .and_then(|value| value.as_bool())
            .ok_or_else(|| invalid("obsolete receipt requires boolean restart_required".into()))?;
        let activation = activation
            .filter(|value| !value.is_null())
            .map(serde_json::from_value::<PriorActivation>)
            .transpose()
            .map_err(|error| invalid(error.to_string()))?;
        let receipt: crate::DeploymentResult =
            serde_json::from_value(result.clone()).map_err(|error| invalid(error.to_string()))?;
        if let Some(active) = activation {
            if !active
                .process_incarnation
                .parse::<ulid::Ulid>()
                .is_ok_and(|id| id.to_string() == active.process_incarnation)
                || active.result_revision != receipt.result_revision
                || receipt.config_digest.as_deref() != Some(active.config_digest.as_str())
                || !receipt.converged
                || restart
            {
                return Err(invalid("inconsistent obsolete activation witness".into()));
            }
        } else if !restart {
            return Err(invalid(
                "obsolete receipt claims activation without a witness".into(),
            ));
        }
        converted = true;
    }
    let state = serde_json::from_value(value).map_err(|error| invalid(error.to_string()))?;
    validate_state(&state)?;
    Ok((state, converted))
}

impl ClusterStore {
    /// The default layout: storage root = the config directory itself
    /// (`file://<abs config dir>`), byte-compatible with every pre-existing
    /// cluster on disk.
    pub(crate) fn for_config_dir(config_dir: &Path) -> Self {
        let absolute = std::path::absolute(config_dir).unwrap_or_else(|_| config_dir.to_path_buf());
        let display_root = absolute.to_string_lossy().trim_end_matches('/').to_string();
        let root = format!("file://{display_root}");
        let storage = storage_handle_for_uri(&root)
            .expect("local storage adapter construction is infallible for file:// roots");
        let adapter = storage.adapter();
        Self {
            adapter,
            storage,
            root,
            display_root,
        }
    }

    /// An explicit `storage:` root. `file://` URIs and plain paths normalize
    /// to the local backend; `s3://bucket/prefix` and
    /// `az://container/prefix` use their object-store backends (env-driven
    /// credentials/endpoint — the same contract as graph storage).
    pub(crate) fn for_storage_root(root_uri: &str) -> Result<Self, Diagnostic> {
        let diagnostic_root = redacted_storage_uri(root_uri);
        let normalized = normalize_root_uri(root_uri).map_err(|err| {
            Diagnostic::error(
                "storage_root_invalid",
                "storage",
                format!("could not initialize storage for '{diagnostic_root}': {err}"),
            )
        })?;
        let kind = storage_kind_for_uri(&normalized).map_err(|err| {
            Diagnostic::error(
                "storage_root_invalid",
                "storage",
                format!("could not initialize storage for '{diagnostic_root}': {err}"),
            )
        })?;
        if kind == StorageKind::Local {
            return Ok(Self::for_config_dir(Path::new(&normalized)));
        }
        let storage = storage_handle_for_uri(&normalized).map_err(|err| {
            Diagnostic::error(
                "storage_root_invalid",
                "storage",
                format!("could not initialize storage for '{diagnostic_root}': {err}"),
            )
        })?;
        Ok(Self {
            adapter: storage.adapter(),
            storage,
            root: normalized.clone(),
            display_root: normalized,
        })
    }

    pub(crate) fn kind(&self) -> StorageKind {
        self.storage.kind()
    }

    /// Canonical identity of the same store used for the serving snapshot.
    /// This is metadata only; it does not reread the config or ledger.
    pub(crate) fn canonical_root(&self) -> Result<String, Diagnostic> {
        if self.kind() == StorageKind::Local {
            let path = std::fs::canonicalize(&self.display_root).map_err(|err| {
                Diagnostic::error("storage_root_invalid", "storage", err.to_string())
            })?;
            Ok(format!("file://{}", path.to_string_lossy()))
        } else {
            Ok(self.root.clone())
        }
    }

    fn uri(&self, relative: &str) -> String {
        format!("{}/{}", self.root, relative)
    }

    fn display(&self, relative: &str) -> String {
        format!("{}/{}", self.display_root, relative)
    }

    /// Derived graph root for `<id>`: `<storage>/graphs/<id>.omni`. A plain
    /// local path for `file://` roots (byte-compatible, directly usable by
    /// the engine); the remote URI the engine opens natively otherwise.
    pub(crate) fn graph_root(&self, graph_id: &str) -> String {
        match self.kind() {
            StorageKind::Local => format!("{}/graphs/{graph_id}.omni", self.display_root),
            StorageKind::S3 | StorageKind::Azure => {
                format!("{}/graphs/{graph_id}.omni", self.root)
            }
        }
    }

    /// Refuse symlink/alias substitution before accepting or resuming deletion.
    /// The immutable intent stores this exact layout identity, never a caller's
    /// arbitrary prefix or the target of a graph-directory symlink.
    pub(crate) fn canonical_managed_graph_root(&self, graph: &str) -> Result<String, Diagnostic> {
        let mut diagnostics = Vec::new();
        crate::config::validate_id("graph", "graph", graph, &mut diagnostics);
        if let Some(error) = diagnostics.into_iter().next() {
            return Err(error);
        }
        let expected = format!(
            "{}/graphs/{graph}.omni",
            self.canonical_root()?.trim_start_matches("file://")
        );
        let actual = crate::admission::canonical_graph_uri(&self.graph_root(graph))?;
        if actual != expected {
            return Err(Diagnostic::error(
                "cluster_graph_root_mismatch",
                graph,
                "graph root is outside the admitted cluster's canonical graph layout",
            ));
        }
        Ok(expected)
    }

    pub(crate) async fn delete_managed_graph_root(
        &self,
        graph: &str,
        recorded_root: &str,
    ) -> Result<(), Diagnostic> {
        if self.canonical_managed_graph_root(graph)? != recorded_root {
            return Err(Diagnostic::error(
                "cluster_graph_root_mismatch",
                graph,
                "graph deletion root differs from its accepted identity",
            ));
        }
        self.adapter
            .delete_prefix(recorded_root)
            .await
            .map_err(|error| {
                Diagnostic::error(
                    "deployment_outcome_unknown",
                    graph,
                    format!("graph root deletion remains outstanding: {error}"),
                )
            })?;
        if self
            .graph_root_exists(recorded_root)
            .await
            .map_err(|error| {
                Diagnostic::error("deployment_outcome_unknown", graph, error.to_string())
            })?
        {
            return Err(Diagnostic::error(
                "deployment_outcome_unknown",
                graph,
                "graph root remains present after deletion",
            ));
        }
        Ok(())
    }

    /// Display-form storage root (plain local path for `file://`, URI for
    /// remote object stores).
    pub(crate) fn display_root(&self) -> &str {
        &self.display_root
    }

    /// A local store whose storage root reads as `display_root`. Serving
    /// compares applied server-safe external Blob bases, which are `s3://`
    /// only, with this root; a test can therefore reach that comparison
    /// through the real snapshot reader without an object store.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn with_display_root(mut self, display_root: &str) -> Self {
        self.display_root = display_root.to_string();
        self
    }

    /// Whether this root holds the cluster state ledger (`__cluster/state.json`)
    /// — i.e. is an actual cluster, not just any directory. Probed via the
    /// the backend (`file://`, `s3://`, or `az://`). Only a successful negative
    /// probe means "not a cluster"; filesystem, transport, and authorization
    /// failures stay loud so callers cannot bypass cluster ownership checks.
    pub(crate) async fn has_state(&self) -> omnigraph_storage::Result<bool> {
        match self.kind() {
            StorageKind::Local => Path::new(&self.display_root)
                .join(CLUSTER_STATE_FILE)
                .try_exists()
                .map_err(StorageError::from),
            StorageKind::S3 | StorageKind::Azure => {
                self.adapter.exists(&self.uri(CLUSTER_STATE_FILE)).await
            }
        }
    }

    /// One bounded GET supplies both the exact bytes and their backend CAS
    /// token. Missing objects are not probed separately.
    async fn read_versioned_opt(&self, uri: &str) -> Result<Option<(String, String)>, String> {
        self.adapter
            .read_text_versioned_if_exists_bounded(
                uri,
                if uri == self.uri(CLUSTER_LOCK_FILE) {
                    64 * 1024
                } else {
                    MAX_LEDGER_BYTES as u64
                },
            )
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

    /// Exact birth cleanup can leave directory skeletons on local filesystems.
    /// Admit only a bounded tree of real directories: object-store listings
    /// follow symlinks and hide broken ones, so their zero-object result alone
    /// cannot prove this local exception safe. Engine preparation still checks
    /// the target before minting creation authority. Cloud markers never qualify.
    pub(crate) fn graph_root_is_empty_local_directory(
        &self,
        graph_uri: &str,
    ) -> omnigraph_storage::Result<bool> {
        if storage_kind_for_uri(graph_uri)? != StorageKind::Local {
            return Ok(false);
        }
        let root = Path::new(graph_uri.trim_start_matches("file://"));
        if !std::fs::symlink_metadata(root)?.file_type().is_dir() {
            return Ok(false);
        }
        let mut pending = vec![root.to_path_buf()];
        let mut directories = 1usize;
        let mut path_bytes = root.as_os_str().len();
        while let Some(directory) = pending.pop() {
            if path_bytes > 64 * 1024 {
                return Ok(false);
            }
            for entry in std::fs::read_dir(directory)? {
                let entry = entry?;
                directories += 1;
                if directories > 64 || !entry.file_type()?.is_dir() {
                    return Ok(false);
                }
                let path = entry.path();
                path_bytes = path_bytes.saturating_add(path.as_os_str().len());
                if path_bytes > 64 * 1024 {
                    return Ok(false);
                }
                pending.push(path);
            }
        }
        Ok(true)
    }

    /// Existence probe before graph creation or read-only observation. A bare local
    /// path or any URI works — resolved through the same adapter machinery
    /// the engine uses.
    pub(crate) async fn graph_root_exists(
        &self,
        graph_uri: &str,
    ) -> omnigraph_storage::Result<bool> {
        match storage_kind_for_uri(graph_uri)? {
            StorageKind::Local => Path::new(graph_uri.trim_start_matches("file://"))
                .try_exists()
                .map_err(StorageError::from),
            // `exists` falls back from an exact-object HEAD to a recursive,
            // bounded-to-the-first-result prefix listing. A partially deleted
            // graph whose root contains only nested Lance objects therefore
            // remains present, while list/authorization failures stay loud.
            StorageKind::S3 | StorageKind::Azure => self.adapter.exists(graph_uri).await,
        }
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
            .map(|uri| {
                let name = uri.rsplit_once('/').map_or(uri.as_str(), |(_, name)| name);
                format!("{}/{name}", self.display(CLUSTER_RECOVERIES_DIR))
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

    pub(crate) async fn read_payload(
        &self,
        kind: &ResourceKind,
        digest: &str,
    ) -> Result<Option<String>, String> {
        let Some(relative) = Self::payload_relative(kind, digest) else {
            return Ok(None);
        };
        let uri = self.uri(&relative);
        self.adapter
            .read_text_versioned_if_exists_bounded(
                &uri,
                crate::config::MAX_CONFIG_SOURCE_BYTES as u64,
            )
            .await
            .map(|read| read.map(|(text, _)| text))
            .map_err(|err| {
                format!(
                    "could not read catalog payload '{}': {err}",
                    self.display(&relative)
                )
            })
    }

    /// Immutable content-addressed write. Existing bytes must verify against
    /// the expected content, not merely occupy its digest-named path.
    pub(crate) async fn write_payload(
        &self,
        kind: &ResourceKind,
        digest: &str,
        content: &str,
    ) -> Result<(), String> {
        let Some(relative) = Self::payload_relative(kind, digest) else {
            return Err("resource kind has no payload".to_string());
        };
        self.write_content_addressed(
            &relative,
            digest,
            content,
            crate::config::MAX_CONFIG_SOURCE_BYTES,
        )
        .await
    }

    async fn write_content_addressed(
        &self,
        relative: &str,
        digest: &str,
        content: &str,
        max_bytes: usize,
    ) -> Result<(), String> {
        if content.len() > max_bytes {
            return Err(format!("content exceeds encoded byte limit {max_bytes}"));
        }
        if sha256_hex(content.as_bytes()) != digest {
            return Err("content does not match its declared digest".to_string());
        }
        let uri = self.uri(relative);
        if self
            .adapter
            .write_text_if_absent(&uri, content)
            .await
            .map_err(|err| err.to_string())?
        {
            return Ok(());
        }
        let Some((existing, _)) = self
            .adapter
            .read_text_versioned_if_exists_bounded(&uri, max_bytes as u64)
            .await
            .map_err(|err| err.to_string())?
        else {
            return Err("existing immutable content disappeared during verification".to_string());
        };
        if sha256_hex(existing.as_bytes()) != digest || existing != content {
            return Err(
                "existing immutable content does not match its recorded digest".to_string(),
            );
        }
        Ok(())
    }

    pub(crate) async fn write_deployment_bundle(
        &self,
        bundle: &DeploymentBundle,
    ) -> Result<String, Diagnostic> {
        let text = encode_json_bounded(bundle, MAX_BUNDLE_BYTES, false).map_err(|err| {
            Diagnostic::error("deployment_bundle_bounds", CLUSTER_RESOURCES_DIR, err)
        })?;
        let digest = sha256_hex(text.as_bytes());
        let relative = format!("{CLUSTER_RESOURCES_DIR}/deployment/{digest}.json");
        self.write_content_addressed(&relative, &digest, &text, MAX_BUNDLE_BYTES)
            .await
            .map_err(|err| {
                Diagnostic::error("deployment_bundle_write", CLUSTER_RESOURCES_DIR, err)
            })?;
        Ok(digest)
    }

    pub(crate) async fn read_deployment_bundle(
        &self,
        digest: &str,
    ) -> Result<DeploymentBundle, Diagnostic> {
        if !crate::authorization::valid_digest(digest) {
            return Err(Diagnostic::error(
                "deployment_bundle_digest",
                CLUSTER_RESOURCES_DIR,
                "invalid bundle digest",
            ));
        }
        let relative = format!("{CLUSTER_RESOURCES_DIR}/deployment/{digest}.json");
        let text = self
            .adapter
            .read_text_if_exists_bounded(&self.uri(&relative), MAX_BUNDLE_BYTES as u64)
            .await
            .map_err(|err| {
                Diagnostic::error(
                    "deployment_bundle_read",
                    CLUSTER_RESOURCES_DIR,
                    err.to_string(),
                )
            })?
            .ok_or_else(|| {
                Diagnostic::error(
                    "deployment_bundle_missing",
                    CLUSTER_RESOURCES_DIR,
                    "immutable bundle is absent",
                )
            })?;
        if sha256_hex(text.as_bytes()) != digest {
            return Err(Diagnostic::error(
                "deployment_bundle_digest",
                CLUSTER_RESOURCES_DIR,
                "immutable bundle does not match its recorded digest",
            ));
        }
        serde_json::from_str(&text).map_err(|err| {
            Diagnostic::error(
                "deployment_bundle_invalid",
                CLUSTER_RESOURCES_DIR,
                format!("could not decode immutable bundle: {err}"),
            )
        })
    }

    /// Read a catalog payload and verify it against its recorded digest.
    pub(crate) async fn read_verified_payload(
        &self,
        kind: &ResourceKind,
        digest: &str,
        address: &str,
    ) -> Result<String, Diagnostic> {
        self.read_verified_payload_with_limit(kind, digest, address, None)
            .await
    }

    pub(crate) async fn read_verified_payload_bounded(
        &self,
        kind: &ResourceKind,
        digest: &str,
        address: &str,
        max_bytes: usize,
    ) -> Result<String, Diagnostic> {
        self.read_verified_payload_with_limit(kind, digest, address, Some(max_bytes))
            .await
    }

    async fn read_verified_payload_with_limit(
        &self,
        kind: &ResourceKind,
        digest: &str,
        address: &str,
        max_bytes: Option<usize>,
    ) -> Result<String, Diagnostic> {
        let Some(relative) = Self::payload_relative(kind, digest) else {
            return Err(Diagnostic::error(
                "catalog_payload_missing",
                address,
                "resource kind has no payload",
            ));
        };
        let uri = self.uri(&relative);
        let text = match max_bytes {
            Some(max_bytes) => self.adapter.read_text_if_exists_bounded(&uri, max_bytes as u64).await,
            None => self.adapter.read_text(&uri).await.map(Some),
        }.map_err(|err| {
            Diagnostic::error(
                "catalog_payload_missing",
                address,
                format!(
                    "catalog blob '{}' unreadable ({err}); restore access to the verified payload or restore its bytes from a trusted copy before deploying",
                    self.display(&relative)
                ),
            )
        })?.ok_or_else(|| Diagnostic::error("catalog_payload_missing", address, "applied catalog payload is absent"))?;
        if sha256_hex(text.as_bytes()) != digest {
            return Err(Diagnostic::error(
                "catalog_payload_digest_mismatch",
                address,
                format!(
                    "catalog blob '{}' does not match its recorded digest; restore access to the verified payload or restore its bytes from a trusted copy before deploying",
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
        self.read_state_inner(observations, false)
            .await
            .map(|(snapshot, _)| snapshot)
    }

    pub(crate) async fn read_state_for_ledger_upgrade(
        &self,
    ) -> Result<(StateSnapshot, bool), Diagnostic> {
        self.read_state_inner(&mut self.observations(), true).await
    }

    async fn read_state_inner(
        &self,
        observations: &mut StateObservations,
        upgrade: bool,
    ) -> Result<(StateSnapshot, bool), Diagnostic> {
        let state_uri = self.uri(CLUSTER_STATE_FILE);
        let (text, _version) = match self.read_versioned_opt(&state_uri).await {
            Ok(Some(read)) => read,
            Ok(None) => {
                return Ok((
                    StateSnapshot {
                        state: None,
                        state_cas: None,
                    },
                    false,
                ));
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

        let (mut state, converted) = decode_ledger(&text, upgrade)?;

        validate_state(&state)?;

        if !converted {
            canonicalize_observation_coordinates(&mut state)?;
        }

        observations.applied_config_digest = state.applied_revision.config_digest.clone();
        observations.state_revision = state.state_revision;
        observations.resource_count = state.applied_revision.resources.len();

        Ok((
            StateSnapshot {
                state: Some(state),
                state_cas: Some(state_cas),
            },
            converted,
        ))
    }

    /// CAS-guarded ledger replace. The public contract stays content-level
    /// (`expected_cas` = `sha256:<hex>` from the snapshot the command read);
    /// the physical swap is token-conditioned on a fresh read, so a writer
    /// that raced us between the fresh read and the put loses with
    /// `state_cas_mismatch` — never a silent overwrite. On S3 and Azure the
    /// token is the object's ETag and the put is conditional (If-Match);
    /// locally it is a content token over the same temp+rename flow as before
    /// the port.
    pub(crate) async fn write_state(
        &self,
        state: &ClusterState,
        expected_cas: Option<&str>,
        observations: &mut StateObservations,
    ) -> Result<(), Diagnostic> {
        self.write_state_inner(state, expected_cas, observations, false)
            .await
    }

    pub(crate) async fn write_state_for_ledger_upgrade(
        &self,
        state: &ClusterState,
        expected_cas: &str,
    ) -> Result<(), Diagnostic> {
        self.write_state_inner(state, Some(expected_cas), &mut self.observations(), true)
            .await
    }

    async fn write_state_inner(
        &self,
        state: &ClusterState,
        expected_cas: Option<&str>,
        observations: &mut StateObservations,
        upgrade: bool,
    ) -> Result<(), Diagnostic> {
        validate_state(state)?;
        if state.version != 2 {
            return Err(Diagnostic::error(
                "ledger_upgrade_required",
                CLUSTER_STATE_FILE,
                "v1 ledgers are read only; explicit conversion must publish v2",
            ));
        }
        // Every operational ledger uses the same compact bounded encoding.
        let payload = encode_json_bounded(state, MAX_LEDGER_BYTES, false)
            .map_err(|err| Diagnostic::error("state_write_error", CLUSTER_STATE_FILE, err))?;
        let state_uri = self.uri(CLUSTER_STATE_FILE);
        let current = self.read_versioned_opt(&state_uri).await.map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not read state file before write: {err}"),
            )
        })?;
        if let Some((text, _)) = &current {
            let (previous, _) = decode_ledger(text, upgrade)?;
            validate_state(&previous)?;
            if previous.version == 2 && state.version != 2 {
                return Err(Diagnostic::error(
                    "unsupported_state_version",
                    CLUSTER_STATE_FILE,
                    "ledger v2 cannot be downgraded",
                ));
            }
        }
        let current_cas = current
            .as_ref()
            .map(|(text, _)| format!("sha256:{}", sha256_hex(text.as_bytes())));
        if current_cas.as_deref() != expected_cas {
            return Err(state_cas_mismatch());
        }

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
        match acquire_state_lock(&self.storage, &lock_uri, operation).await {
            Ok(StateLockAcquire::Acquired(guard)) => {
                observations.lock_acquired = true;
                observations.acquired_lock_id = Some(guard.lock_id().to_string());
                Ok(guard)
            }
            Ok(StateLockAcquire::Held) => {
                self.observe_lock_metadata_lossy(observations).await;
                Err(Diagnostic::error(
                    "state_lock_held",
                    CLUSTER_LOCK_FILE,
                    state_lock_held_message(observations),
                ))
            }
            Err(StateLockError::LockEncode(err)) => Err(Diagnostic::error(
                "state_lock_error",
                CLUSTER_LOCK_FILE,
                format!("could not encode state lock: {err}"),
            )),
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
        if lock.lock_id() != lock_id {
            return Err(Diagnostic::error(
                "state_lock_id_mismatch",
                CLUSTER_LOCK_FILE,
                format!(
                    "lock id mismatch: held lock is {}, refusing to remove (pass the exact id from `cluster status`)",
                    lock.lock_id()
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
                match StateLockFile::parse(&text) {
                    Ok(lock) => observations.observe_lock_metadata(&lock),
                    Err(StateLockError::LockVersion(version)) => {
                        diagnostics.push(Diagnostic::warning(
                            "unsupported_state_lock_version",
                            CLUSTER_LOCK_FILE,
                            format!("unsupported cluster state lock version {version}"),
                        ))
                    }
                    Err(StateLockError::LockParse(err)) => diagnostics.push(Diagnostic::warning(
                        "invalid_state_lock",
                        CLUSTER_LOCK_FILE,
                        format!("could not parse state lock: {err}"),
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

    pub(crate) async fn observe_lock_metadata_lossy(&self, observations: &mut StateObservations) {
        observations.locked = true;
        let lock_uri = self.uri(CLUSTER_LOCK_FILE);
        if let Ok(Some((text, _))) = self.read_versioned_opt(&lock_uri).await {
            if let Ok(lock) = StateLockFile::parse(&text) {
                observations.observe_lock_metadata(&lock);
            }
        }
    }
}

/// Refuse during serialization, before an oversized encoded body is collected.
/// Pretty mode includes v1's historical trailing newline in the same cap.
fn encode_json_bounded<T: serde::Serialize>(
    value: &T,
    max_bytes: usize,
    pretty: bool,
) -> Result<String, String> {
    struct BoundedJson {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl std::io::Write for BoundedJson {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit - self.bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::FileTooLarge,
                    format!("encoded control object exceeds {} bytes", self.limit),
                ));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BoundedJson {
        bytes: Vec::new(),
        limit: max_bytes,
    };
    if pretty {
        serde_json::to_writer_pretty(&mut writer, value)
    } else {
        serde_json::to_writer(&mut writer, value)
    }
    .map_err(|err| err.to_string())?;
    if pretty {
        std::io::Write::write_all(&mut writer, b"\n").map_err(|err| err.to_string())?;
    }
    String::from_utf8(writer.bytes).map_err(|err| err.to_string())
}

fn canonicalize_observation_coordinates(state: &mut ClusterState) -> Result<(), Diagnostic> {
    for (resource, observation) in &mut state.observations {
        let Some(object) = observation.as_object_mut() else {
            continue;
        };
        let Some(legacy_version) = object.remove("manifest_version") else {
            continue;
        };
        if let Some(graph_manifest_version) = object.get("graph_manifest_version") {
            if graph_manifest_version != &legacy_version {
                return Err(Diagnostic::error(
                    "conflicting_graph_manifest_observation",
                    format!("state.observations.{resource}"),
                    format!(
                        "cluster observation '{resource}' contains conflicting legacy and canonical graph-manifest versions"
                    ),
                ));
            }
        } else {
            object.insert("graph_manifest_version".to_string(), legacy_version);
        }
    }
    Ok(())
}

fn state_cas_mismatch() -> Diagnostic {
    Diagnostic::error(
        "state_cas_mismatch",
        CLUSTER_STATE_FILE,
        "state.json changed while the command was running; re-run the command against the latest state",
    )
}

pub(crate) fn parse_lock_file_for_unlock(text: &str) -> Result<StateLockFile, Diagnostic> {
    match StateLockFile::parse(text) {
        Ok(lock) => Ok(lock),
        Err(StateLockError::LockVersion(version)) => Err(Diagnostic::error(
            "unsupported_state_lock_version",
            CLUSTER_LOCK_FILE,
            format!("unsupported cluster state lock version {version}"),
        )),
        Err(StateLockError::LockParse(err)) => Err(Diagnostic::error(
            "invalid_state_lock",
            CLUSTER_LOCK_FILE,
            format!("could not parse state lock: {err}"),
        )),
        Err(err) => Err(Diagnostic::error(
            "invalid_state_lock",
            CLUSTER_LOCK_FILE,
            format!("could not parse state lock: {err}"),
        )),
    }
}

pub(crate) fn state_lock_held_message(observations: &StateObservations) -> String {
    match observations.lock_id.as_deref() {
        Some(lock_id) => format!(
            "cluster state lock already exists (lock id {lock_id}); run `omnigraph cluster force-unlock {lock_id}` only after confirming no cluster operation is active"
        ),
        None => "cluster state lock already exists; remove it only after confirming no cluster operation is active".to_string(),
    }
}
