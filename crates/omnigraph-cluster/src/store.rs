//! The cluster's storage backend: state ledger, lock, recovery
//! sidecars, approval artifacts (moved verbatim from lib.rs in the
//! modularization). The object-storage port (RFC-006) lands here as a
//! follow-up — this module is the single home for stored-state I/O.

use super::*;

#[derive(Debug)]
pub(crate) struct LocalStateBackend {
    state_dir: PathBuf,
    state_path: PathBuf,
    lock_path: PathBuf,
    recoveries_dir: PathBuf,
    approvals_dir: PathBuf,
}

#[derive(Debug)]
pub(crate) struct StateSnapshot {
    pub(crate) state: Option<ClusterState>,
    pub(crate) state_cas: Option<String>,
}

#[derive(Debug)]
pub(crate) struct StateLockGuard {
    path: PathBuf,
}

impl LocalStateBackend {
    pub(crate) fn new(config_dir: &Path) -> Self {
        let state_dir = config_dir.join(CLUSTER_STATE_DIR);
        Self {
            state_path: config_dir.join(CLUSTER_STATE_FILE),
            lock_path: config_dir.join(CLUSTER_LOCK_FILE),
            recoveries_dir: config_dir.join(CLUSTER_RECOVERIES_DIR),
            approvals_dir: config_dir.join(CLUSTER_APPROVALS_DIR),
            state_dir,
        }
    }

    /// List approval artifacts in ULID (filename) order; unparseable files
    /// warn and stay on disk for the operator.
    pub(crate) fn list_approval_artifacts(
        &self,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<(PathBuf, ApprovalArtifact)> {
        let mut paths = Vec::new();
        match fs::read_dir(&self.approvals_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "json") {
                        paths.push(path);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => diagnostics.push(Diagnostic::warning(
                "approval_read_error",
                CLUSTER_APPROVALS_DIR,
                format!("could not list approval artifacts: {err}"),
            )),
        }
        paths.sort();
        let mut artifacts = Vec::new();
        for path in paths {
            match fs::read_to_string(&path)
                .map_err(|err| err.to_string())
                .and_then(|text| {
                    serde_json::from_str::<ApprovalArtifact>(&text).map_err(|err| err.to_string())
                }) {
                Ok(artifact) if artifact.schema_version == 1 => artifacts.push((path, artifact)),
                Ok(artifact) => diagnostics.push(Diagnostic::warning(
                    "unsupported_approval_version",
                    display_path(&path),
                    format!(
                        "unsupported approval artifact version {}; leaving it in place",
                        artifact.schema_version
                    ),
                )),
                Err(err) => diagnostics.push(Diagnostic::warning(
                    "invalid_approval_artifact",
                    display_path(&path),
                    format!("could not parse approval artifact ({err}); leaving it in place"),
                )),
            }
        }
        artifacts
    }

    /// Atomically write (or rewrite, e.g. on consumption) an approval artifact.
    pub(crate) fn write_approval_artifact(&self, artifact: &ApprovalArtifact) -> Result<PathBuf, Diagnostic> {
        fs::create_dir_all(&self.approvals_dir).map_err(|err| {
            Diagnostic::error(
                "approval_write_error",
                CLUSTER_APPROVALS_DIR,
                format!("could not create approvals directory: {err}"),
            )
        })?;
        let target = self
            .approvals_dir
            .join(format!("{}.json", artifact.approval_id));
        let mut payload = serde_json::to_string_pretty(artifact).map_err(|err| {
            Diagnostic::error(
                "approval_write_error",
                display_path(&target),
                format!("could not encode approval artifact: {err}"),
            )
        })?;
        payload.push('\n');
        let tmp_path = self
            .approvals_dir
            .join(format!("{}.json.tmp.{}", artifact.approval_id, Ulid::new()));
        fs::write(&tmp_path, payload.as_bytes()).map_err(|err| {
            Diagnostic::error(
                "approval_write_error",
                display_path(&tmp_path),
                format!("could not write approval artifact: {err}"),
            )
        })?;
        if let Err(err) = fs::rename(&tmp_path, &target) {
            let _ = fs::remove_file(&tmp_path);
            return Err(Diagnostic::error(
                "approval_write_error",
                display_path(&target),
                format!("could not move approval artifact into place: {err}"),
            ));
        }
        Ok(target)
    }

    /// List recovery sidecars in ULID (filename) order. Unparseable files are
    /// reported as warnings and skipped — they stay on disk for the operator.
    pub(crate) fn list_recovery_sidecars(
        &self,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<(PathBuf, RecoverySidecar)> {
        let mut paths = Vec::new();
        match fs::read_dir(&self.recoveries_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "json") {
                        paths.push(path);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                diagnostics.push(Diagnostic::warning(
                    "recovery_sidecar_read_error",
                    CLUSTER_RECOVERIES_DIR,
                    format!("could not list recovery sidecars: {err}"),
                ));
            }
        }
        paths.sort();
        let mut sidecars = Vec::new();
        for path in paths {
            match fs::read_to_string(&path)
                .map_err(|err| err.to_string())
                .and_then(|text| {
                    serde_json::from_str::<RecoverySidecar>(&text).map_err(|err| err.to_string())
                }) {
                Ok(sidecar) if sidecar.schema_version == 1 => sidecars.push((path, sidecar)),
                Ok(sidecar) => diagnostics.push(Diagnostic::warning(
                    "unsupported_recovery_sidecar_version",
                    display_path(&path),
                    format!(
                        "unsupported recovery sidecar version {}; leaving it in place",
                        sidecar.schema_version
                    ),
                )),
                Err(err) => diagnostics.push(Diagnostic::warning(
                    "invalid_recovery_sidecar",
                    display_path(&path),
                    format!("could not parse recovery sidecar ({err}); leaving it in place"),
                )),
            }
        }
        sidecars
    }

    /// Atomically write (or rewrite) a recovery sidecar; returns its path.
    pub(crate) fn write_recovery_sidecar(&self, sidecar: &RecoverySidecar) -> Result<PathBuf, Diagnostic> {
        fs::create_dir_all(&self.recoveries_dir).map_err(|err| {
            Diagnostic::error(
                "recovery_sidecar_write_error",
                CLUSTER_RECOVERIES_DIR,
                format!("could not create recoveries directory: {err}"),
            )
        })?;
        let target = self
            .recoveries_dir
            .join(format!("{}.json", sidecar.operation_id));
        let mut payload = serde_json::to_string_pretty(sidecar).map_err(|err| {
            Diagnostic::error(
                "recovery_sidecar_write_error",
                display_path(&target),
                format!("could not encode recovery sidecar: {err}"),
            )
        })?;
        payload.push('\n');
        let tmp_path = self
            .recoveries_dir
            .join(format!("{}.json.tmp.{}", sidecar.operation_id, Ulid::new()));
        fs::write(&tmp_path, payload.as_bytes()).map_err(|err| {
            Diagnostic::error(
                "recovery_sidecar_write_error",
                display_path(&tmp_path),
                format!("could not write recovery sidecar: {err}"),
            )
        })?;
        if let Err(err) = fs::rename(&tmp_path, &target) {
            let _ = fs::remove_file(&tmp_path);
            return Err(Diagnostic::error(
                "recovery_sidecar_write_error",
                display_path(&target),
                format!("could not move recovery sidecar into place: {err}"),
            ));
        }
        Ok(target)
    }

    pub(crate) fn observations(&self) -> StateObservations {
        StateObservations {
            state_path: display_path(&self.state_path),
            lock_path: display_path(&self.lock_path),
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

    pub(crate) fn read_state(
        &self,
        observations: &mut StateObservations,
    ) -> Result<StateSnapshot, Diagnostic> {
        let text = match fs::read_to_string(&self.state_path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
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

    pub(crate) fn write_state(
        &self,
        state: &ClusterState,
        expected_cas: Option<&str>,
        observations: &mut StateObservations,
    ) -> Result<(), Diagnostic> {
        fs::create_dir_all(&self.state_dir).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_DIR,
                format!("could not create cluster state directory: {err}"),
            )
        })?;

        let current_cas = self.current_state_cas()?;
        if current_cas.as_deref() != expected_cas {
            return Err(Diagnostic::error(
                "state_cas_mismatch",
                CLUSTER_STATE_FILE,
                "state.json changed while the command was running; re-run the command against the latest state",
            ));
        }

        let mut payload = serde_json::to_string_pretty(state).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not encode state JSON: {err}"),
            )
        })?;
        payload.push('\n');

        let tmp_path = self
            .state_dir
            .join(format!("state.json.tmp.{}", Ulid::new()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|err| {
                Diagnostic::error(
                    "state_write_error",
                    display_path(&tmp_path),
                    format!("could not create temporary state file: {err}"),
                )
            })?;
        file.write_all(payload.as_bytes()).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                display_path(&tmp_path),
                format!("could not write temporary state file: {err}"),
            )
        })?;
        file.sync_all().map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                display_path(&tmp_path),
                format!("could not sync temporary state file: {err}"),
            )
        })?;
        drop(file);

        if let Err(err) = fs::rename(&tmp_path, &self.state_path) {
            let _ = fs::remove_file(&tmp_path);
            return Err(Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not replace state.json atomically: {err}"),
            ));
        }

        let written = fs::read_to_string(&self.state_path).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not read state.json after write: {err}"),
            )
        })?;
        observations.state_found = true;
        observations.applied_config_digest = state.applied_revision.config_digest.clone();
        observations.state_revision = state.state_revision;
        observations.state_cas = Some(format!("sha256:{}", sha256_hex(written.as_bytes())));
        observations.resource_count = state.applied_revision.resources.len();

        Ok(())
    }

    pub(crate) fn current_state_cas(&self) -> Result<Option<String>, Diagnostic> {
        match fs::read(&self.state_path) {
            Ok(bytes) => Ok(Some(format!("sha256:{}", sha256_hex(&bytes)))),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(Diagnostic::error(
                "state_read_error",
                CLUSTER_STATE_FILE,
                format!("could not read state file for CAS check: {err}"),
            )),
        }
    }

    pub(crate) fn acquire_lock(
        &self,
        operation: &str,
        observations: &mut StateObservations,
    ) -> Result<StateLockGuard, Diagnostic> {
        fs::create_dir_all(&self.state_dir).map_err(|err| {
            Diagnostic::error(
                "state_lock_error",
                CLUSTER_STATE_DIR,
                format!("could not create cluster state directory: {err}"),
            )
        })?;

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

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.lock_path)
        {
            Ok(mut file) => {
                if let Err(err) = file.write_all(payload.as_bytes()) {
                    // No guard exists yet, so clean up the create-new file here
                    // instead of leaving a stale partial lock for the next run.
                    drop(file);
                    let _ = fs::remove_file(&self.lock_path);
                    return Err(Diagnostic::error(
                        "state_lock_error",
                        CLUSTER_LOCK_FILE,
                        format!("could not write state lock: {err}"),
                    ));
                }
                observations.lock_acquired = true;
                observations.acquired_lock_id = Some(lock_id.clone());
                Ok(StateLockGuard {
                    path: self.lock_path.clone(),
                })
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                self.observe_lock_metadata_lossy(observations);
                Err(Diagnostic::error(
                    "state_lock_held",
                    CLUSTER_LOCK_FILE,
                    state_lock_held_message(observations),
                ))
            }
            Err(err) => Err(Diagnostic::error(
                "state_lock_error",
                CLUSTER_LOCK_FILE,
                format!("could not acquire state lock: {err}"),
            )),
        }
    }

    pub(crate) fn force_unlock(
        &self,
        requested_lock_id: &str,
        observations: &mut StateObservations,
    ) -> Result<(), Diagnostic> {
        let text = match fs::read_to_string(&self.lock_path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Err(Diagnostic::error(
                    "state_lock_missing",
                    CLUSTER_LOCK_FILE,
                    "cluster state lock is not present; nothing was unlocked",
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
        observations.locked = true;
        let lock = parse_lock_file_for_unlock(&text)?;
        observations.observe_lock_metadata(&lock);

        if lock.lock_id != requested_lock_id {
            return Err(Diagnostic::error(
                "state_lock_id_mismatch",
                CLUSTER_LOCK_FILE,
                format!(
                    "cluster state lock id is {}; refusing to unlock with requested id {requested_lock_id}",
                    lock.lock_id
                ),
            ));
        }

        fs::remove_file(&self.lock_path).map_err(|err| {
            Diagnostic::error(
                "state_unlock_error",
                CLUSTER_LOCK_FILE,
                format!("could not remove state lock: {err}"),
            )
        })
    }

    pub(crate) fn observe_lock(
        &self,
        observations: &mut StateObservations,
        diagnostics: &mut Vec<Diagnostic>,
    ) {
        if self.lock_path.exists() {
            observations.locked = true;
            match fs::read_to_string(&self.lock_path) {
                Ok(text) => match serde_json::from_str::<StateLockFile>(&text) {
                    Ok(lock) if lock.version == 1 => {
                        observations.observe_lock_metadata(&lock);
                    }
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
                },
                Err(err) => diagnostics.push(Diagnostic::warning(
                    "state_lock_read_error",
                    CLUSTER_LOCK_FILE,
                    format!("could not read state lock: {err}"),
                )),
            }
        }
    }

    pub(crate) fn observe_lock_metadata_lossy(&self, observations: &mut StateObservations) {
        observations.locked = true;
        if let Ok(text) = fs::read_to_string(&self.lock_path) {
            if let Ok(lock) = serde_json::from_str::<StateLockFile>(&text) {
                if lock.version == 1 {
                    observations.observe_lock_metadata(&lock);
                }
            }
        }
    }
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
