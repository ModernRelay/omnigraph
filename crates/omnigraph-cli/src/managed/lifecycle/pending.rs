//! Local retry records are identity hints, never service authority.
use super::*;
use sha2::{Digest as _, Sha256};
use std::fs::File;

const PENDING: &str = "pending-lifecycle.json";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    version: u32,
    api: String,
    path: String,
    request_digest: String,
    principal_id: String,
    account_id: String,
    pub(super) idempotency_key: String,
    #[serde(skip)]
    pub(super) fresh: bool,
}

fn failure() -> Failure {
    Failure::new(
        "lifecycle_record_failed",
        "could not safely persist or read the local lifecycle recovery record",
        1,
    )
}

fn open(path: &Path, write: bool) -> Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .write(write)
        .create(write)
        .truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .mode(0o600);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink() || !m.is_file()) {
        return Err(failure());
    }
    let file = options.open(path).map_err(|_| failure())?;
    if !file.metadata().map_err(|_| failure())?.is_file() {
        return Err(failure());
    }
    Ok(file)
}

/// One local submission at a time. This is not a cluster or graph-writer fence.
pub(super) fn lock(config: &Path) -> Result<File> {
    read_context(config)?;
    if !config.is_dir() {
        return Err(Failure::refused(
            "config_directory_required",
            "create the configuration directory before creating a managed cluster",
        ));
    }
    let dir = config.join(".omnigraph");
    match std::fs::create_dir(&dir) {
        Ok(()) => crate::sync_dir(config).map_err(|_| failure())?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(failure()),
    }
    read_context(config)?;
    let file = open(&dir.join("lifecycle.lock"), true)?;
    file.try_lock().map_err(|_| {
        Failure::refused(
            "lifecycle_submission_busy",
            "another process is submitting from this folder; retry after it returns",
        )
    })?;
    Ok(file)
}

pub(super) fn prepare(
    config: &Path,
    api: &str,
    path: &str,
    body: &Value,
    principal: &PrincipalIdentity,
    explicit: Option<&str>,
    bound_create: bool,
) -> Result<Pending> {
    let request_digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(body).map_err(|_| failure())?)
    );
    let location = config.join(".omnigraph").join(PENDING);
    match std::fs::symlink_metadata(&location) {
        Ok(_) => {
            let file = open(&location, false)?;
            let mut bytes = Vec::new();
            file.take(MAX_CONTEXT + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| failure())?;
            if bytes.len() as u64 > MAX_CONTEXT {
                return Err(failure());
            }
            let existing: Pending = serde_json::from_slice(&bytes).map_err(|_| failure())?;
            if existing.version != 1
                || existing.api != api
                || existing.path != path
                || existing.request_digest != request_digest
                || existing.principal_id != principal.principal_id
                || existing.account_id != principal.account_id
                || explicit.is_some_and(|key| key != existing.idempotency_key)
            {
                return Err(Failure::refused(
                    "pending_lifecycle_conflict",
                    "another exact lifecycle request or principal is pending; recover it with the original principal before submitting a different request or key",
                ));
            }
            idempotency_key(Some(&existing.idempotency_key))?;
            Ok(existing)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if bound_create {
                return Err(Failure::refused(
                    "context_already_bound",
                    "cluster create requires an unbound folder; its existing context was preserved",
                ));
            }
            let pending = Pending {
                version: 1,
                api: api.into(),
                path: path.into(),
                request_digest,
                principal_id: principal.principal_id.clone(),
                account_id: principal.account_id.clone(),
                idempotency_key: idempotency_key(explicit)?,
                fresh: true,
            };
            save(
                config,
                PENDING,
                &serde_json::to_vec(&pending).map_err(|_| failure())?,
            )?;
            Ok(pending)
        }
        Err(_) => Err(failure()),
    }
}

fn save(config: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    read_context(config)?;
    let dir = config.join(".omnigraph");
    let mut file = tempfile::NamedTempFile::new_in(&dir).map_err(|_| failure())?;
    file.write_all(bytes).map_err(|_| failure())?;
    file.as_file().sync_all().map_err(|_| failure())?;
    file.persist(dir.join(name)).map_err(|_| failure())?;
    crate::sync_dir(&dir).map_err(|_| failure())
}

pub(super) fn record(config: &Path, api: &str, identity: &Identity) -> Result<()> {
    let mut record = serde_json::to_value(identity).map_err(|_| failure())?;
    record["version"] = json!(1);
    record["api"] = json!(api);
    save(
        config,
        "last-lifecycle.json",
        &serde_json::to_vec(&record).map_err(|_| failure())?,
    )
}

pub(super) fn bind(config: &Path, context: &Context) -> Result<()> {
    if let Some(existing) = read_context(config)? {
        if existing.api == context.api && existing.cluster == context.cluster {
            return Ok(());
        }
        return Err(Failure::refused(
            "context_already_bound",
            "another context appeared during creation; it was preserved",
        ));
    }
    let dir = config.join(".omnigraph");
    let mut file = tempfile::NamedTempFile::new_in(&dir).map_err(|_| failure())?;
    file.write_all(
        serde_yaml::to_string(context)
            .map_err(|_| failure())?
            .as_bytes(),
    )
    .map_err(|_| failure())?;
    file.as_file().sync_all().map_err(|_| failure())?;
    file.persist_noclobber(dir.join("context")).map_err(|_| {
        Failure::refused(
            "context_already_bound",
            "another context appeared during creation; it was preserved",
        )
    })?;
    crate::sync_dir(&dir).map_err(|_| failure())
}

pub(super) fn clear(config: &Path) -> Result<()> {
    std::fs::remove_file(config.join(".omnigraph").join(PENDING)).map_err(|_| failure())?;
    crate::sync_dir(&config.join(".omnigraph")).map_err(|_| failure())
}
