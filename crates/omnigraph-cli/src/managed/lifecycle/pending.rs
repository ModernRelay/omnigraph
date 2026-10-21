//! Bounded operator-home custody for exact lifecycle retry identities.
//! These records never authorize a service operation or select a destination.
use super::*;
use sha2::{Digest as _, Sha256};
use std::fs::File;
use std::path::PathBuf;

const PENDING: &str = "pending-lifecycle.json";
const ACCEPTED: &str = "accepted-lifecycle.json";
const MAX_ACCEPTED: usize = 128;
const MAX_RECORD_BYTES: u64 = 1024 * 1024;

fn owner_profile() -> Option<String> {
    crate::operator_routing::journal_owner()
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    version: u32,
    api: String,
    path: String,
    request_digest: String,
    principal_id: String,
    account_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner_profile: Option<String>,
    pub(super) idempotency_key: String,
    #[serde(skip)]
    pub(super) fresh: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Accepted {
    request: Pending,
    identity: Identity,
}

impl Pending {
    fn valid(&self) -> bool {
        self.version == 1
            && canonical_origin(&self.api).is_ok()
            && organization_id(&self.account_id).is_ok()
            && !self.principal_id.is_empty()
            && self.principal_id.len() <= 1024
            && self.principal_id.chars().all(|c| !c.is_control())
            && self.request_digest.len() == 64
            && self.request_digest.bytes().all(|c| c.is_ascii_hexdigit())
            && !self.idempotency_key.is_empty()
            && self.idempotency_key.len() <= 256
            && self.idempotency_key.bytes().all(|c| c.is_ascii_graphic())
            && (self.path == "/v1/clusters"
                || self.path.starts_with("/v1/clusters/")
                    && (self.path.ends_with(":delete") || self.path.ends_with(":undo")))
    }
}

fn accepted_records(dir: &Path) -> Result<Vec<Accepted>> {
    let records = read::<Vec<Accepted>>(dir, ACCEPTED)?.unwrap_or_default();
    if records.len() > MAX_ACCEPTED
        || records.iter().any(|record| {
            !record.request.valid()
                || [
                    &record.identity.cluster_id,
                    &record.identity.incarnation,
                    &record.identity.operation_id,
                ]
                .into_iter()
                .any(|id| identifier(id).is_err())
                || !matches!(record.identity.kind.as_str(), "create" | "delete" | "undo")
        })
    {
        return Err(failure());
    }
    let mut keys = std::collections::HashSet::new();
    if records
        .iter()
        .any(|record| !keys.insert(&record.request.idempotency_key))
    {
        return Err(failure());
    }
    Ok(records)
}

fn failure() -> Failure {
    Failure::new(
        "lifecycle_record_failed",
        "could not safely persist or read the operator lifecycle recovery record",
        1,
    )
}

fn ensure_directory(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(failure()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                ensure_directory(parent)?;
            }
            match std::fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(failure()),
            }
            let metadata = std::fs::symlink_metadata(path).map_err(|_| failure())?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(failure());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                    .map_err(|_| failure())?;
            }
            if let Some(parent) = path.parent() {
                crate::sync_dir(parent).map_err(|_| failure())?;
            }
            Ok(())
        }
        Err(_) => Err(failure()),
    }
}

pub(super) fn directory(
    input: &crate::operator::InvocationConfig,
    binding: &Binding,
) -> Result<PathBuf> {
    let cwd = std::env::current_dir().map_err(|_| failure())?;
    directory_at(input, binding, &cwd)
}

fn directory_at(
    input: &crate::operator::InvocationConfig,
    binding: &Binding,
    cwd: &Path,
) -> Result<PathBuf> {
    let home = input.directory().ok_or_else(|| {
        Failure::refused(
            "operator_home_required",
            "managed lifecycle custody requires OMNIGRAPH_HOME or a user home directory",
        )
    })?;
    let dir = account_directory(home, &binding.api, binding.organization()?)?;
    // A selected target cannot bypass an old uncertain submission in this checkout.
    for name in [PENDING, "last-lifecycle.json"] {
        let evidence = cwd.join(".omnigraph").join(name);
        match std::fs::symlink_metadata(&evidence) {
            Ok(_) => {
                return Err(Failure::refused(
                    "legacy_lifecycle_custody",
                    "legacy lifecycle evidence requires config recover-operations --profile NAME --source DIR before another lifecycle submission; preserve the project .omnigraph directory",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(failure()),
        }
    }
    verify_retired_custody(cwd, home)?;
    ensure_directory(&dir)?;
    Ok(dir)
}

fn account_directory(home: &Path, api: &str, organization: &str) -> Result<PathBuf> {
    let identity = serde_json::to_vec(&(api, organization)).map_err(|_| failure())?;
    Ok(home
        .join("lifecycle")
        .join(format!("{:x}", Sha256::digest(identity))))
}

/// A retired source record is still recovery evidence in every operator home.
/// The marker identifies its verified account; it does not select a backend.
fn verify_retired_custody(source: &Path, home: &Path) -> Result<()> {
    let Some(evidence) = legacy_evidence(source)? else {
        return Ok(());
    };
    let destination = account_directory(home, &evidence.api, &evidence.organization)?;
    let missing = || {
        Failure::refused(
            "legacy_lifecycle_custody",
            "retired lifecycle evidence has not been preserved in this operator home; run config recover-operations --profile NAME --source DIR before another lifecycle submission",
        )
    };
    if let Some(pending) = evidence.pending {
        let expected = serde_json::to_value(&pending).map_err(|_| failure())?;
        let copied = read::<Pending>(&destination, PENDING)?
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| failure())?;
        if copied.as_ref() != Some(&expected)
            && !accepted_records(&destination)?.iter().any(|record| {
                serde_json::to_value(&record.request).is_ok_and(|value| value == expected)
            })
        {
            return Err(missing());
        }
    }
    if let Some(accepted) = evidence.accepted {
        let expected = serde_json::to_value(&accepted).map_err(|_| failure())?;
        if !legacy_accepted_records(&destination, &evidence.api)?
            .iter()
            .any(|record| serde_json::to_value(record).is_ok_and(|value| value == expected))
        {
            return Err(missing());
        }
    }
    Ok(())
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
        options.custom_flags(0x0020_0000);
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

fn read<T: serde::de::DeserializeOwned>(dir: &Path, name: &str) -> Result<Option<T>> {
    let path = dir.join(name);
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(failure()),
        Ok(_) => {}
    }
    let mut bytes = Vec::new();
    open(&path, false)?
        .take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| failure())?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(failure());
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| failure())
}

/// One local submission at a time. This is not a cluster or graph-writer fence.
pub(super) fn lock(dir: &Path) -> Result<File> {
    ensure_directory(dir)?;
    let file = open(&dir.join("lifecycle.lock"), true)?;
    file.try_lock().map_err(|_| {
        Failure::refused(
            "lifecycle_submission_busy",
            "another process is submitting for this account; retry after it returns",
        )
    })?;
    Ok(file)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare(
    dir: &Path,
    api: &str,
    path: &str,
    body: &Value,
    principal: &PrincipalIdentity,
    explicit: Option<&str>,
    stderr: &mut dyn Write,
) -> Result<Pending> {
    let request_digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(body).map_err(|_| failure())?)
    );
    let same_request = |existing: &Pending| {
        existing.version == 1
            && existing.api == api
            && existing.path == path
            && existing.request_digest == request_digest
            && existing.principal_id == principal.principal_id
            && existing.account_id == principal.account_id
    };
    let matches = |existing: &Pending| {
        same_request(existing)
            && (existing.owner_profile == owner_profile()
                || existing.owner_profile.is_none()
                    && explicit == Some(existing.idempotency_key.as_str()))
    };
    if let Some(existing) = read::<Pending>(dir, PENDING)? {
        if !matches(&existing) || explicit.is_some_and(|key| key != existing.idempotency_key) {
            return Err(Failure::refused(
                "pending_lifecycle_conflict",
                "another exact lifecycle request or principal is pending; recover it with the original principal before submitting a different request or key",
            ));
        }
        idempotency_key(Some(&existing.idempotency_key), stderr)?;
        return Ok(existing);
    }
    let legacy = legacy_accepted_records(dir, api)?;
    if !legacy.is_empty() && explicit.is_none() {
        return Err(Failure::refused(
            "accepted_lifecycle_requires_key",
            "this account has imported accepted lifecycle history; observe the original operation or supply an explicit idempotency key",
        ));
    }
    let accepted = accepted_records(dir)?;
    if accepted.len() > MAX_ACCEPTED {
        return Err(failure());
    }
    for record in &accepted {
        if record.request.api != api || record.request.account_id != principal.account_id {
            return Err(failure());
        }
        if same_request(&record.request) && explicit.is_none() {
            let mut error = Failure::refused(
                "accepted_lifecycle_requires_key",
                "this exact lifecycle request was already accepted; observe its operation or supply its original idempotency key explicitly",
            );
            error.body["accepted_operation"] =
                serde_json::to_value(&record.identity).map_err(|_| failure())?;
            error.body["idempotency_key"] = json!(record.request.idempotency_key);
            return Err(error);
        }
        if explicit.is_some_and(|key| key == record.request.idempotency_key) {
            if !matches(&record.request) {
                return Err(Failure::refused(
                    "pending_lifecycle_conflict",
                    "the idempotency key belongs to a different accepted request or principal",
                ));
            }
            idempotency_key(Some(&record.request.idempotency_key), stderr)?;
            save(dir, PENDING, &record.request)?;
            return Ok(record.request.clone());
        }
    }
    if accepted.len() == MAX_ACCEPTED {
        return Err(Failure::refused(
            "lifecycle_custody_full",
            "the bounded lifecycle recovery history is full; preserve and migrate the operator home before another submission",
        ));
    }
    let pending = Pending {
        version: 1,
        api: api.into(),
        path: path.into(),
        request_digest,
        principal_id: principal.principal_id.clone(),
        account_id: principal.account_id.clone(),
        owner_profile: owner_profile(),
        idempotency_key: idempotency_key(explicit, stderr)?,
        fresh: true,
    };
    save(dir, PENDING, &pending)?;
    Ok(pending)
}

fn save(dir: &Path, name: &str, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| failure())?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(failure());
    }
    let mut file = tempfile::NamedTempFile::new_in(dir).map_err(|_| failure())?;
    file.write_all(&bytes).map_err(|_| failure())?;
    file.as_file().sync_all().map_err(|_| failure())?;
    file.persist(dir.join(name)).map_err(|_| failure())?;
    crate::sync_dir(dir).map_err(|_| failure())
}

pub(super) fn record(dir: &Path, request: &Pending, identity: &Identity) -> Result<()> {
    let mut records = accepted_records(dir)?;
    if let Some(existing) = records
        .iter()
        .find(|record| record.request.idempotency_key == request.idempotency_key)
    {
        if existing.request.api != request.api
            || existing.request.path != request.path
            || existing.request.request_digest != request.request_digest
            || existing.request.principal_id != request.principal_id
            || existing.request.owner_profile != request.owner_profile
            || existing.request.account_id != request.account_id
            || existing.identity != *identity
        {
            return Err(failure());
        }
        return Ok(());
    }
    if records.len() >= MAX_ACCEPTED {
        return Err(failure());
    }
    records.push(Accepted {
        request: request.clone(),
        identity: identity.clone(),
    });
    save(dir, ACCEPTED, &records)
}

pub(super) fn clear(dir: &Path) -> Result<()> {
    std::fs::remove_file(dir.join(PENDING)).map_err(|_| failure())?;
    crate::sync_dir(dir).map_err(|_| failure())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyContext {
    version: u8,
    api: String,
    organization_id: String,
    cluster: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyAccepted {
    version: u8,
    api: String,
    cluster_id: String,
    incarnation: String,
    operation_id: String,
    kind: String,
}

impl LegacyAccepted {
    fn valid(&self) -> bool {
        self.version == 1
            && canonical_origin(&self.api).is_ok()
            && [&self.cluster_id, &self.incarnation, &self.operation_id]
                .into_iter()
                .all(|id| identifier(id).is_ok())
            && matches!(self.kind.as_str(), "create" | "delete" | "undo")
    }
}

fn legacy_accepted_records(dir: &Path, api: &str) -> Result<Vec<LegacyAccepted>> {
    let records = read::<Vec<LegacyAccepted>>(dir, "legacy-accepted.json")?.unwrap_or_default();
    if records.len() > MAX_ACCEPTED
        || records
            .iter()
            .any(|record| !record.valid() || record.api != api)
    {
        return Err(failure());
    }
    Ok(records)
}

fn legacy(
    dir: &Path,
) -> Result<(
    Option<Pending>,
    Option<LegacyAccepted>,
    Option<LegacyContext>,
)> {
    let pending =
        read::<Pending>(dir, PENDING)?.or(read::<Pending>(dir, "pending-lifecycle.json.retired")?);
    let accepted = read::<LegacyAccepted>(dir, "last-lifecycle.json")?
        .or(read::<LegacyAccepted>(dir, "last-lifecycle.json.retired")?);
    if pending.is_none() && accepted.is_none() {
        return Ok((None, None, None));
    }
    let path = dir.join("context");
    let context = match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(failure()),
        Ok(_) => {
            let mut bytes = Vec::new();
            open(&path, false)?
                .take(16 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| failure())?;
            if bytes.len() > 16 * 1024 {
                return Err(failure());
            }
            Some(yaml_serde::from_slice::<LegacyContext>(&bytes).map_err(|_| failure())?)
        }
    };
    if pending.as_ref().is_some_and(|p| !p.valid())
        || context.as_ref().is_some_and(|c| {
            c.version != 2
                || canonical_origin(&c.api).is_err()
                || organization_id(&c.organization_id).is_err()
                || identifier(&c.cluster).is_err()
        })
        || accepted.as_ref().is_some_and(|a| !a.valid())
    {
        return Err(failure());
    }
    Ok((pending, accepted, context))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CustodyMigration {
    version: u8,
    api: String,
    organization_id: String,
}

struct LegacyEvidence {
    api: String,
    organization: String,
    pending: Option<Pending>,
    accepted: Option<LegacyAccepted>,
}

fn legacy_evidence(source: &Path) -> Result<Option<LegacyEvidence>> {
    let directory = source.join(".omnigraph");
    let (pending, accepted, context) = legacy(&directory)?;
    let marker = read::<CustodyMigration>(&directory, "target-custody-migrated.json")?;
    if marker.as_ref().is_some_and(|marker| {
        marker.version != 1
            || canonical_origin(&marker.api).is_err()
            || organization_id(&marker.organization_id).is_err()
    }) {
        return Err(failure());
    }
    if pending.is_none() && accepted.is_none() {
        return if marker.is_none() {
            Ok(None)
        } else {
            Err(failure())
        };
    }
    let binding = pending
        .as_ref()
        .map(|p| (p.api.clone(), p.account_id.clone()))
        .or_else(|| {
            context
                .as_ref()
                .map(|c| (c.api.clone(), c.organization_id.clone()))
        })
        .or_else(|| {
            marker
                .as_ref()
                .map(|m| (m.api.clone(), m.organization_id.clone()))
        });
    let (api, organization) = binding.ok_or_else(|| Failure::refused("legacy_lifecycle_identity_required", "accepted-only lifecycle evidence requires its original version 2 folder context or verified migration marker to identify the organization"))?;
    if pending
        .as_ref()
        .is_some_and(|p| p.api != api || p.account_id != organization)
        || context
            .as_ref()
            .is_some_and(|c| c.api != api || c.organization_id != organization)
        || marker
            .as_ref()
            .is_some_and(|m| m.api != api || m.organization_id != organization)
        || accepted.as_ref().is_some_and(|a| {
            a.api != api || context.as_ref().is_some_and(|c| a.cluster_id != c.cluster)
        })
    {
        return Err(failure());
    }
    Ok(Some(LegacyEvidence {
        api,
        organization,
        pending,
        accepted,
    }))
}

pub(crate) fn legacy_binding(source: &Path) -> color_eyre::Result<Option<(String, String)>> {
    legacy_evidence(source)
        .map(|evidence| evidence.map(|evidence| (evidence.api, evidence.organization)))
        .map_err(|error| color_eyre::eyre::eyre!("{}", error.body))
}

pub(crate) fn import(source: &Path, home: &Path, binding: &Binding) -> color_eyre::Result<()> {
    let expected = legacy_binding(source)?;
    let result = (|| -> Result<()> {
        let Some((api, organization)) = expected else {
            return Ok(());
        };
        if api != binding.api || Some(organization.as_str()) != binding.organization_id.as_deref() {
            return Err(Failure::refused(
                "legacy_lifecycle_identity_mismatch",
                "the selected migration target does not own the legacy lifecycle evidence",
            ));
        }
        let source_dir = source.join(".omnigraph");
        let _source_lock = lock(&source_dir)?;
        if legacy_binding(source).map_err(|_| failure())?
            != Some((api.clone(), organization.clone()))
        {
            return Err(failure());
        }
        let destination = account_directory(home, &api, &organization)?;
        let _destination_lock = lock(&destination)?;
        let (pending, accepted, _) = legacy(&source_dir)?;
        if let Some(pending) = pending {
            if let Some(existing) = read::<Pending>(&destination, PENDING)? {
                if serde_json::to_value(&existing).map_err(|_| failure())?
                    != serde_json::to_value(&pending).map_err(|_| failure())?
                {
                    return Err(Failure::refused(
                        "pending_lifecycle_conflict",
                        "the operator home already holds a different pending request; preserve both recovery records",
                    ));
                }
            } else {
                let accepted = accepted_records(&destination)?;
                if !accepted.iter().any(|record| {
                    serde_json::to_value(&record.request).ok()
                        == serde_json::to_value(&pending).ok()
                }) {
                    save(&destination, PENDING, &pending)?;
                }
            }
        }
        if let Some(accepted) = accepted {
            let mut records = legacy_accepted_records(&destination, &api)?;
            let value = serde_json::to_value(&accepted).map_err(|_| failure())?;
            if !records
                .iter()
                .any(|record| serde_json::to_value(record).is_ok_and(|record| record == value))
            {
                if records.len() >= MAX_ACCEPTED {
                    return Err(Failure::refused(
                        "lifecycle_custody_full",
                        "the operator home's legacy recovery history is full; all original evidence was preserved",
                    ));
                }
                records.push(accepted);
                save(&destination, "legacy-accepted.json", &records)?;
            }
        }
        // Copies are durable before originals retire. A crash here repeats the
        // same exact copy. Older executables must remain excluded after migration.
        for name in [PENDING, "last-lifecycle.json"] {
            let old = source_dir.join(name);
            if std::fs::symlink_metadata(&old).is_ok() {
                let retired = source_dir.join(format!("{name}.retired"));
                if std::fs::symlink_metadata(&retired).is_ok() {
                    // Never discard either copy if a second migration changed bytes.
                    let old_bytes: Value = read(&source_dir, name)?.ok_or_else(failure)?;
                    let retired_bytes: Value =
                        read(&source_dir, &format!("{name}.retired"))?.ok_or_else(failure)?;
                    if old_bytes != retired_bytes {
                        return Err(failure());
                    }
                    std::fs::remove_file(&old).map_err(|_| failure())?;
                } else {
                    std::fs::rename(&old, &retired).map_err(|_| failure())?;
                }
                crate::sync_dir(&source_dir).map_err(|_| failure())?;
            }
        }
        save(
            &source_dir,
            "target-custody-migrated.json",
            &CustodyMigration {
                version: 1,
                api,
                organization_id: organization,
            },
        )?;
        Ok(())
    })();
    result.map_err(|error| color_eyre::eyre::eyre!("{}", error.body))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal() -> PrincipalIdentity {
        PrincipalIdentity {
            principal_id: "principal-one".into(),
            account_id: "org_one".into(),
        }
    }
    fn identity() -> Identity {
        Identity {
            cluster_id: "cluster-one".into(),
            incarnation: "inc-one".into(),
            operation_id: "op-one".into(),
            kind: "create".into(),
        }
    }
    fn binding() -> Binding {
        Binding {
            api: "https://control.example".into(),
            organization_id: Some("org_one".into()),
            cluster: None,
            control_auth: ControlAuth::HumanDevice,
        }
    }
    #[test]
    fn accepted_requests_reuse_exact_keys_and_never_regenerate_after_clearing_pending() {
        let dir = tempfile::tempdir().unwrap();
        let _lock = lock(dir.path()).unwrap();
        let body = json!({"name":"one"});
        let prepare_request = |key| {
            prepare(
                dir.path(),
                "https://control.example",
                "/v1/clusters",
                &body,
                &principal(),
                key,
                &mut Vec::new(),
            )
        };
        let first = prepare_request(Some("key-one")).unwrap();
        assert!(first.fresh);
        assert!(!prepare_request(None).unwrap().fresh);
        assert!(prepare_request(Some("different-key")).is_err());
        record(dir.path(), &first, &identity()).unwrap();
        clear(dir.path()).unwrap();
        let durable = std::fs::read(dir.path().join(ACCEPTED)).unwrap();
        for (pointer, invalid) in [
            ("/0/request/version", json!(99)),
            ("/0/request/request_digest", json!("malformed")),
            ("/0/request/api", json!("https://different.example")),
            ("/0/identity/operation_id", json!("../other")),
        ] {
            let mut corrupt: Value = serde_json::from_slice(&durable).unwrap();
            *corrupt.pointer_mut(pointer).unwrap() = invalid;
            save(dir.path(), ACCEPTED, &corrupt).unwrap();
            let error = prepare_request(Some("new-key")).err().unwrap();
            assert_eq!(error.body["type"], "lifecycle_record_failed", "{pointer}");
            assert!(!dir.path().join(PENDING).exists());
        }
        std::fs::write(dir.path().join(ACCEPTED), durable).unwrap();
        let refused = prepare_request(None).err().unwrap();
        assert_eq!(refused.body["type"], "accepted_lifecycle_requires_key");
        assert_eq!(refused.body["accepted_operation"]["operation_id"], "op-one");
        let retry = prepare_request(Some("key-one")).unwrap();
        assert_eq!(retry.idempotency_key, first.idempotency_key);
        assert!(!retry.fresh);
        record(dir.path(), &retry, &identity()).unwrap();
        assert_eq!(
            read::<Vec<Accepted>>(dir.path(), ACCEPTED)
                .unwrap()
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn accepted_history_capacity_refuses_without_evicting_recovery_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let _lock = lock(dir.path()).unwrap();
        for i in 0..MAX_ACCEPTED {
            let pending = prepare(
                dir.path(),
                "https://control.example",
                "/v1/clusters",
                &json!({"name":i}),
                &principal(),
                Some(&format!("key-{i}")),
                &mut Vec::new(),
            )
            .unwrap();
            record(dir.path(), &pending, &identity()).unwrap();
            clear(dir.path()).unwrap();
        }
        let before = std::fs::read(dir.path().join(ACCEPTED)).unwrap();
        let error = prepare(
            dir.path(),
            "https://control.example",
            "/v1/clusters",
            &json!({"name":"new"}),
            &principal(),
            Some("new-key"),
            &mut Vec::new(),
        )
        .err()
        .unwrap();
        assert_eq!(error.body["type"], "lifecycle_custody_full");
        assert_eq!(std::fs::read(dir.path().join(ACCEPTED)).unwrap(), before);
        assert!(!dir.path().join(PENDING).exists());
    }
    #[test]
    fn legacy_pending_import_is_identity_checked_durable_and_idempotent() {
        let source = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let original = source.path().join(".omnigraph");
        let original_lock = lock(&original).unwrap();
        let request = prepare(
            &original,
            "https://control.example",
            "/v1/clusters",
            &json!({"name":"one"}),
            &principal(),
            Some("original-key"),
            &mut Vec::new(),
        )
        .unwrap();
        drop(original_lock);
        let before = std::fs::read(original.join(PENDING)).unwrap();
        assert_eq!(
            legacy_binding(source.path()).unwrap(),
            Some((binding().api.clone(), "org_one".into()))
        );
        let mut other = binding();
        other.organization_id = Some("org_other".into());
        assert!(import(source.path(), home.path(), &other).is_err());
        assert_eq!(std::fs::read(original.join(PENDING)).unwrap(), before);
        import(source.path(), home.path(), &binding()).unwrap();
        assert!(!original.join(PENDING).exists());
        assert_eq!(
            std::fs::read(original.join(format!("{PENDING}.retired"))).unwrap(),
            before
        );
        import(source.path(), home.path(), &binding()).unwrap();
        let root = home.path().join("lifecycle");
        let destination = std::fs::read_dir(root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let copied = read::<Pending>(&destination, PENDING).unwrap().unwrap();
        assert_eq!(copied.idempotency_key, request.idempotency_key);
        let input = crate::operator::InvocationConfig::at(Some(home.path().into()), None);
        assert_eq!(
            directory_at(&input, &binding(), source.path()).unwrap(),
            destination
        );
        let other_home = tempfile::tempdir().unwrap();
        let other_input =
            crate::operator::InvocationConfig::at(Some(other_home.path().into()), None);
        let error = directory_at(&other_input, &binding(), source.path()).unwrap_err();
        assert_eq!(error.body["type"], "legacy_lifecycle_custody");
        assert!(!other_home.path().join("lifecycle").exists());
        import(source.path(), other_home.path(), &binding()).unwrap();
        let transferred = directory_at(&other_input, &binding(), source.path()).unwrap();
        assert_eq!(
            read::<Pending>(&transferred, PENDING)
                .unwrap()
                .unwrap()
                .idempotency_key,
            request.idempotency_key
        );
        record(&destination, &copied, &identity()).unwrap();
        clear(&destination).unwrap();
        assert!(directory_at(&input, &binding(), source.path()).is_ok());
        import(source.path(), home.path(), &binding()).unwrap();
        assert!(
            !destination.join(PENDING).exists(),
            "reimport must not resurrect completed pending custody"
        );
    }
    #[test]
    fn accepted_only_legacy_import_keeps_a_no_key_guard() {
        let source = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let original = source.path().join(".omnigraph");
        let original_lock = lock(&original).unwrap();
        save(&original, "last-lifecycle.json", &json!({"version":1,"api":"https://control.example","cluster_id":"cluster-one","incarnation":"inc-one","operation_id":"op-one","kind":"create"})).unwrap();
        drop(original_lock);
        assert!(
            legacy_binding(source.path()).is_err(),
            "accepted identity alone does not identify its organization"
        );
        std::fs::write(original.join("context"), "version: 2\napi: https://control.example\norganization_id: org_one\ncluster: cluster-one\n").unwrap();
        import(source.path(), home.path(), &binding()).unwrap();
        let destination = std::fs::read_dir(home.path().join("lifecycle"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let error = prepare(
            &destination,
            "https://control.example",
            "/v1/clusters",
            &json!({"name":"possibly-original"}),
            &principal(),
            None,
            &mut Vec::new(),
        )
        .err()
        .unwrap();
        assert_eq!(error.body["type"], "accepted_lifecycle_requires_key");
        assert!(original.join("last-lifecycle.json.retired").is_file());
        let input = crate::operator::InvocationConfig::at(Some(home.path().into()), None);
        assert_eq!(
            directory_at(&input, &binding(), source.path()).unwrap(),
            destination
        );
        let other_home = tempfile::tempdir().unwrap();
        let other_input =
            crate::operator::InvocationConfig::at(Some(other_home.path().into()), None);
        let error = directory_at(&other_input, &binding(), source.path()).unwrap_err();
        assert_eq!(error.body["type"], "legacy_lifecycle_custody");
        assert!(!other_home.path().join("lifecycle").exists());
        std::fs::rename(original.join("context"), original.join("context.retired")).unwrap();
        import(source.path(), other_home.path(), &binding()).unwrap();
        assert!(directory_at(&other_input, &binding(), source.path()).is_ok());
        assert!(directory_at(&input, &binding(), source.path()).is_ok());
        import(source.path(), other_home.path(), &binding()).unwrap();
        let marker = std::fs::read(original.join("target-custody-migrated.json")).unwrap();
        save(
            &original,
            "target-custody-migrated.json",
            &json!({"version":99,"api":"https://control.example","organization_id":"org_one"}),
        )
        .unwrap();
        let error = directory_at(&other_input, &binding(), source.path()).unwrap_err();
        assert_eq!(error.body["type"], "lifecycle_record_failed");
        std::fs::write(original.join("target-custody-migrated.json"), marker).unwrap();
        let retired = original.join("last-lifecycle.json.retired");
        let evidence = std::fs::read(&retired).unwrap();
        std::fs::remove_file(&retired).unwrap();
        let error = directory_at(&other_input, &binding(), source.path()).unwrap_err();
        assert_eq!(error.body["type"], "lifecycle_record_failed");
        std::fs::write(&retired, evidence).unwrap();
        let retained = std::fs::read(destination.join("legacy-accepted.json")).unwrap();
        for (pointer, invalid) in [
            ("/0/version", json!(99)),
            ("/0/api", json!("https://different.example")),
            ("/0/operation_id", json!("../other")),
        ] {
            let mut corrupt: Value = serde_json::from_slice(&retained).unwrap();
            *corrupt.pointer_mut(pointer).unwrap() = invalid;
            save(&destination, "legacy-accepted.json", &corrupt).unwrap();
            let error = prepare(
                &destination,
                "https://control.example",
                "/v1/clusters",
                &json!({"name":"new"}),
                &principal(),
                Some("explicit-key"),
                &mut Vec::new(),
            )
            .err()
            .unwrap();
            assert_eq!(error.body["type"], "lifecycle_record_failed", "{pointer}");
            assert!(!destination.join(PENDING).exists());
        }
    }
}
