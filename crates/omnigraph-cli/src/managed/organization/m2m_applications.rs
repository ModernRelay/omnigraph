//! Organization M2M applications: the identities unattended workloads sign in
//! as. A client secret the service discloses reaches only the file the
//! administrator names; graph access remains in applied Cedar
//! (RFC 2026-11-25-organization-m2m-applications).
use super::*;
use crate::cli::{
    OrganizationM2mApplicationSecretCommand as Secrets,
    OrganizationM2mApplicationsCommand as Applications,
};
use crate::managed::custody as files;
use crate::operator_schema::{AuthProfile, ClientSecretSource};
use std::path::{Path, PathBuf};

const ROOT: &str = "/v1/organization/m2m-applications";
const RECORDS: &str = "m2m-applications.json";
const MAX_RECORDS: usize = 128;
const MAX_BYTES: u64 = 1024 * 1024;
/// Room for the step suffix each derived write key carries.
const MAX_KEY: usize = 200;
/// Listing follows at most ten pages of 100, as invitation lookup does.
const MAX_PAGES: usize = 10;
const MAX_PERMISSIONS: usize = 5;

/// The request identity a key retains; flags that only consent to replacing
/// local files are not part of it.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Create {
        name: String,
        permissions: Vec<String>,
        client_secret_file: PathBuf,
        profile: Option<String>,
    },
    Rotate {
        profile: String,
        revoke: Vec<String>,
    },
    Delete {
        selector: String,
    },
}

/// What one request has durably established, written before the next write.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum Progress {
    Create {
        token_endpoint: Option<String>,
        application: Option<Application>,
        /// How far this request has gone in installing its profile.
        profile: Option<ProfileStep>,
        /// The client secret this request wrote to its file.
        delivered: Option<Delivered>,
    },
    Rotate {
        client_id: String,
        token_endpoint: String,
        client_secret_file: PathBuf,
        /// Client secret IDs chosen from those that existed before the new one.
        revoke: Vec<String>,
        delivered: Option<Delivered>,
    },
    Delete {
        client_id: String,
        actor_id: String,
        name: String,
    },
}

/// The profile step of a create. Consent to replace a profile covers the
/// first attempt only: a retry after an attempt accepts its own profile or an
/// absent one, never another, and an installed profile is never written again.
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProfileStep {
    Attempted,
    Installed,
}

impl Progress {
    fn matches(&self, request: &Request) -> bool {
        matches!(
            (self, request),
            (Self::Create { .. }, Request::Create { .. })
                | (Self::Rotate { .. }, Request::Rotate { .. })
                | (Self::Delete { .. }, Request::Delete { .. })
        )
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Application {
    client_id: String,
    organization_id: String,
    name: String,
    permissions: Vec<String>,
    /// The `principal:<id>` a cluster's policy names.
    actor_id: String,
    created_at: String,
    updated_at: String,
}

#[derive(Deserialize)]
struct ApplicationPage {
    organization_id: String,
    applications: Vec<Value>,
    #[serde(deserialize_with = "Option::deserialize")]
    after: Option<String>,
    grantable_permissions: Vec<String>,
    token_endpoint: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct SecretSummary {
    id: String,
    secret_hint: String,
    created_at: String,
    updated_at: String,
}

#[derive(Deserialize)]
struct SecretPage {
    client_id: String,
    secrets: Vec<SecretSummary>,
    #[serde(deserialize_with = "Option::deserialize")]
    after: Option<String>,
}

#[derive(Deserialize)]
struct Generated {
    client_id: String,
    secret: SecretSummary,
}

/// A disclosed client secret. It implements no Debug, Display or Serialize,
/// so the only thing code can do with it is write it to the client secret file.
struct ClientSecret(String);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u8,
    api: String,
    organization_id: String,
    principal_id: String,
    owner_profile: String,
    idempotency_key: String,
    request: Request,
    progress: Progress,
}

fn provider_id(value: &str) -> bool {
    crate::helpers::valid_managed_identifier(value, 512)
}

fn application_name(value: &str) -> bool {
    plain(value, 256) && value.trim() == value
}

fn permission(value: &str) -> bool {
    crate::helpers::valid_managed_identifier(value, 128)
}

fn distinct_permissions(permissions: &[String], max: usize) -> bool {
    let mut seen = BTreeSet::new();
    permissions.len() <= max
        && permissions
            .iter()
            .all(|value| permission(value) && seen.insert(value))
}

fn timestamp(value: &str) -> bool {
    provider_timestamp(value).is_ok()
}

fn validate_application(application: &Application, organization: &str) -> Result<()> {
    if application.organization_id != organization
        || !provider_id(&application.client_id)
        || !application
            .actor_id
            .strip_prefix("principal:")
            .is_some_and(provider_id)
        || !application_name(&application.name)
        || !distinct_permissions(&application.permissions, 256)
        || !timestamp(&application.created_at)
        || !timestamp(&application.updated_at)
    {
        return Err(Failure::protocol());
    }
    Ok(())
}

fn validate_secret(secret: &SecretSummary) -> Result<()> {
    if !provider_id(&secret.id)
        || !plain(&secret.secret_hint, 64)
        || !timestamp(&secret.created_at)
        || !timestamp(&secret.updated_at)
    {
        return Err(Failure::protocol());
    }
    Ok(())
}

fn validate_token_endpoint(endpoint: &str) -> Result<()> {
    crate::credential_sources::validate_client_credentials(endpoint, "client", None)
        .map_err(|_| Failure::protocol())
}

fn validate_key(key: &str) -> Result<()> {
    if key.len() > MAX_KEY {
        return Err(Failure::refused(
            "idempotency_key_invalid",
            "M2M application idempotency keys contain 1–200 printable ASCII characters; the CLI derives one key per write from it",
        ));
    }
    Ok(())
}

/// One key per write. A multi-step request derives each step's key from the
/// retained key, so an identical retry repeats every write exactly.
fn step_key(key: &str, step: &str) -> String {
    format!("{key}:{step}")
}

fn page_path(base: &str, after: Option<&str>) -> String {
    match after {
        Some(after) => format!(
            "{base}?{}",
            url::form_urlencoded::Serializer::new(String::new())
                .append_pair("after", after)
                .finish()
        ),
        None => base.to_owned(),
    }
}

/// Send one request; responses name their organization in different places,
/// so each caller validates its own body.
async fn send(
    api: &Api,
    method: Method,
    path: &str,
    body: Option<&Value>,
    key: Option<&str>,
) -> Result<Value> {
    send_preserving(api, method, path, body, key, None).await
}

/// The transport strips every `client_secret` field from responses. Only
/// secret generation asks it to keep the one it discloses.
async fn send_preserving(
    api: &Api,
    method: Method,
    path: &str,
    body: Option<&Value>,
    key: Option<&str>,
    preserved: Option<&'static str>,
) -> Result<Value> {
    let response = api
        .raw_bounded(
            method,
            path,
            body,
            key,
            ResponseLimits {
                timeout: TEAM_TIMEOUT,
                max_body: MAX_BODY,
                preserved,
                auth_verification_status: false,
            },
        )
        .await?;
    if !response.status.is_success() {
        return Err(Failure {
            body: response.body,
            response_status: Some(response.status),
            exit: if response.status.is_client_error() {
                2
            } else {
                1
            },
        });
    }
    Ok(response.body)
}

/// Every application page, combined into one envelope with a null cursor.
async fn list_applications(api: &Api, organization: &str) -> Result<(Value, Vec<Application>)> {
    let mut cursor: Option<String> = None;
    let mut cursors = BTreeSet::new();
    let mut clients = BTreeSet::new();
    let mut raw = Vec::new();
    let mut applications = Vec::new();
    let mut first: Option<(Value, ApplicationPage)> = None;
    for _ in 0..MAX_PAGES {
        let body = send(
            api,
            Method::GET,
            &page_path(ROOT, cursor.as_deref()),
            None,
            None,
        )
        .await?;
        let page: ApplicationPage = parse(&body["data"])?;
        if page.organization_id != organization
            || page.applications.len() > 100
            || !distinct_permissions(&page.grantable_permissions, 256)
            || page
                .after
                .as_deref()
                .is_some_and(|after| !plain(after, 1024))
        {
            return Err(Failure::protocol());
        }
        validate_token_endpoint(&page.token_endpoint)?;
        if let Some((_, head)) = &first
            && (head.grantable_permissions != page.grantable_permissions
                || head.token_endpoint != page.token_endpoint)
        {
            return Err(Failure::protocol());
        }
        for value in &page.applications {
            let application: Application = parse(value)?;
            validate_application(&application, organization)?;
            if !clients.insert(application.client_id.clone()) {
                return Err(Failure::protocol());
            }
            raw.push(value.clone());
            applications.push(application);
        }
        let after = page.after.clone();
        if first.is_none() {
            first = Some((body["meta"].clone(), page));
        }
        match after {
            None => {
                let (meta, head) = first.expect("first page");
                let envelope = json!({"data":{
                    "organization_id": organization,
                    "applications": raw,
                    "after": null,
                    "grantable_permissions": head.grantable_permissions,
                    "token_endpoint": head.token_endpoint,
                },"meta": meta});
                return Ok((envelope, applications));
            }
            Some(after) if cursors.insert(after.clone()) => cursor = Some(after),
            Some(_) => return Err(Failure::protocol()),
        }
    }
    Err(Failure::refused(
        "m2m_application_list_incomplete",
        "the M2M application list exceeds ten pages; nothing was changed",
    ))
}

async fn list_secrets(api: &Api, client: &str) -> Result<Vec<SecretSummary>> {
    let base = format!("{ROOT}/{client}/secrets");
    let mut cursor: Option<String> = None;
    let mut secrets = Vec::new();
    let mut ids = BTreeSet::new();
    for _ in 0..MAX_PAGES {
        let body = send(
            api,
            Method::GET,
            &page_path(&base, cursor.as_deref()),
            None,
            None,
        )
        .await?;
        let page: SecretPage = parse(&body["data"])?;
        if page.client_id != client || page.secrets.len() > 100 {
            return Err(Failure::protocol());
        }
        for secret in page.secrets {
            validate_secret(&secret)?;
            if !ids.insert(secret.id.clone()) {
                return Err(Failure::protocol());
            }
            secrets.push(secret);
        }
        match page.after {
            None => return Ok(secrets),
            // Secret pages continue after the last ID, in ascending order.
            Some(after) if provider_id(&after) && cursor.as_deref() < Some(after.as_str()) => {
                cursor = Some(after)
            }
            Some(_) => return Err(Failure::protocol()),
        }
    }
    Err(Failure::refused(
        "m2m_application_secret_list_incomplete",
        "the client secret list exceeds ten pages; nothing was changed",
    ))
}

/// Generate one client secret. Its response is the only one that keeps one, and
/// [`take_secret`] removes it before anything else reads the body.
async fn generate(api: &Api, organization: &str, client: &str, key: &str) -> Result<Value> {
    send_preserving(
        api,
        Method::POST,
        &format!("{ROOT}/{client}/secrets"),
        Some(&json!({"organization_id":organization})),
        Some(key),
        Some("client_secret"),
    )
    .await
}

/// Remove the disclosed client secret from a generate response before anything else
/// reads the body, then validate the remaining receipt.
fn take_secret(body: &mut Value, client: &str) -> Result<(SecretSummary, Option<ClientSecret>)> {
    let data = body
        .get_mut("data")
        .and_then(Value::as_object_mut)
        .ok_or_else(Failure::protocol)?;
    let secret = match data.remove("client_secret") {
        Some(Value::String(value)) => Some(ClientSecret(value)),
        Some(Value::Null) => None,
        _ => return Err(Failure::protocol()),
    };
    let generated: Generated = parse(&body["data"])?;
    validate_secret(&generated.secret)?;
    // A replay discloses nothing: its client_secret is null.
    if generated.client_id != client
        || secret.as_ref().is_some_and(|ClientSecret(value)| {
            value.is_empty()
                || value.len() > crate::credential_sources::MAX_CREDENTIAL_BYTES
                || value.chars().any(char::is_control)
        })
    {
        return Err(Failure::protocol());
    }
    Ok((generated.secret, secret))
}

/// Whether this platform can create a file only its owner can read. Unix
/// permission bits do; elsewhere the CLI refuses to write a client secret
/// rather than leave it to whatever access the directory grants.
const OWNER_ONLY_FILES: bool = cfg!(unix);

/// The client secret's destination: a file name in an existing directory.
/// Construction is the one place a destination is accepted, so every command
/// and every collision check sees the same normalized path.
struct ClientSecretFile {
    path: PathBuf,
    directory: PathBuf,
}

impl ClientSecretFile {
    fn new(path: &Path) -> Result<Self> {
        Self::new_on(path, OWNER_ONLY_FILES)
    }

    fn new_on(path: &Path, owner_only_files: bool) -> Result<Self> {
        let invalid =
            |detail: &str| Failure::refused("m2m_application_secret_file_invalid", detail);
        if !owner_only_files {
            return Err(Failure::refused(
                "m2m_application_secret_file_unsupported",
                "this platform cannot create a client secret file only its owner can read; create or rotate the client secret on Linux or macOS, or in the managed console's Credentials page; nothing was changed",
            ));
        }
        let absolute = std::path::absolute(path)
            .map_err(|_| invalid("the client secret file path cannot be made absolute"))?;
        // `file_name` ignores a final separator and `.`, and `absolute` drops
        // a final `.`, so a path such as `dir/secret/` would pass here and fail
        // only at the rename, after the client secret exists. The path as
        // written must end in its file name.
        let raw = path.as_os_str().as_encoded_bytes();
        let last = raw
            .rsplit(|byte| std::path::is_separator(char::from(*byte)))
            .next()
            .unwrap_or_default();
        let ends_in_name = !last.is_empty() && last != b"." && last != b"..";
        let (Some(name), Some(parent)) = (absolute.file_name(), absolute.parent()) else {
            return Err(invalid("the client secret file path must name a file"));
        };
        if !ends_in_name {
            return Err(invalid(
                "the client secret file path must end in a file name, without a trailing separator or `.`",
            ));
        }
        if !parent.is_dir() {
            return Err(invalid(
                "the client secret file's directory must already exist",
            ));
        }
        Ok(Self {
            path: parent.join(name),
            directory: parent.to_owned(),
        })
    }

    /// Where the destination resolves: through the directory's links, since
    /// the file itself is never a link. Two outputs collide when these match.
    fn location(&self) -> PathBuf {
        resolved_location(&self.path)
    }

    /// An existing file must be a regular file, never a link the write would
    /// follow or replace; `replace` consents to overwriting it.
    fn check(&self, replace: bool) -> Result<()> {
        match std::fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(Failure::refused(
                "m2m_application_secret_file_invalid",
                "the client secret file cannot be inspected",
            )),
            Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
                Err(Failure::refused(
                    "m2m_application_secret_file_invalid",
                    "the client secret file exists and is not a regular file; the CLI writes regular files only",
                ))
            }
            Ok(_) if replace => Ok(()),
            Ok(_) => Err(Failure::refused(
                "m2m_application_secret_file_exists",
                "the client secret file already exists; choose another path or pass --replace-client-secret-file",
            )),
        }
    }

    /// Create the owner-only temporary file before any remote write, so a
    /// directory that cannot hold it refuses before a client secret exists.
    /// A replacement keeps the owner and group of the file it replaces: a
    /// rename installs the new inode, so without this a rotation run by
    /// another account would leave the workload unable to read its secret.
    fn stage(&self) -> Result<tempfile::NamedTempFile> {
        #[cfg(unix)]
        let owner = std::fs::symlink_metadata(&self.path).ok().map(|metadata| {
            use std::os::unix::fs::MetadataExt;
            (metadata.uid(), metadata.gid())
        });
        #[cfg(not(unix))]
        let owner = None;
        self.stage_owned(owner)
    }

    fn stage_owned(&self, owner: Option<(u32, u32)>) -> Result<tempfile::NamedTempFile> {
        let mut builder = tempfile::Builder::new();
        builder.prefix(".omnigraph-secret-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o600));
        }
        let staged = builder.tempfile_in(&self.directory).map_err(|_| {
            Failure::refused(
                "m2m_application_secret_file_invalid",
                "cannot create a file in the client secret file's directory; nothing was changed",
            )
        })?;
        #[cfg(unix)]
        if let Some((uid, gid)) = owner {
            use std::os::unix::fs::MetadataExt;
            let current = staged.as_file().metadata().map_err(|_| {
                Failure::refused(
                    "m2m_application_secret_file_invalid",
                    "the staged client secret file cannot be inspected; nothing was changed",
                )
            })?;
            if (current.uid(), current.gid()) != (uid, gid)
                && std::os::unix::fs::fchown(staged.as_file(), Some(uid), Some(gid)).is_err()
            {
                return Err(Failure::refused(
                    "m2m_application_secret_file_owner",
                    &format!(
                        "the client secret file belongs to user {uid} and group {gid}, and this account cannot give its replacement that owner; run the command as that user or as root; nothing was changed"
                    ),
                ));
            }
        }
        #[cfg(not(unix))]
        let _ = owner;
        Ok(staged)
    }

    fn deliver(
        &self,
        mut staged: tempfile::NamedTempFile,
        ClientSecret(secret): &ClientSecret,
        replace: bool,
    ) -> std::io::Result<()> {
        use std::io::Write as _;
        staged.write_all(secret.as_bytes())?;
        staged.as_file().sync_all()?;
        if replace {
            staged.persist(&self.path).map_err(|error| error.error)?;
        } else {
            staged
                .persist_noclobber(&self.path)
                .map_err(|error| error.error)?;
        }
        crate::sync_dir(&self.directory)
    }

    /// The client secret the file holds now, read as the provider reads it.
    fn read(
        &self,
    ) -> std::result::Result<ClientSecret, crate::credential_sources::CredentialError> {
        crate::credential_sources::read_client_secret(&ClientSecretSource::File {
            file: self.path.clone(),
        })
        .map(ClientSecret)
    }
}

/// A path as the file system resolves it: the whole path when it exists,
/// otherwise its directory resolved and its name appended.
fn resolved_location(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_owned());
    if let Ok(resolved) = std::fs::canonicalize(&absolute) {
        return resolved;
    }
    match (absolute.parent(), absolute.file_name()) {
        (Some(parent), Some(name)) => std::fs::canonicalize(parent)
            .map(|parent| parent.join(name))
            .unwrap_or(absolute),
        _ => absolute,
    }
}

/// Which delivered client secret a file holds, without keeping its value: a
/// salted SHA-256 of the value this request wrote. A retry compares the file
/// against it before it proves or revokes anything.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Delivered {
    secret_id: String,
    salt: String,
    digest: String,
}

impl Delivered {
    fn of(secret_id: &str, ClientSecret(value): &ClientSecret) -> Self {
        let salt = uuid::Uuid::new_v4().simple().to_string();
        Self {
            secret_id: secret_id.into(),
            digest: Self::digest(&salt, value),
            salt,
        }
    }

    fn digest(salt: &str, value: &str) -> String {
        use sha2::{Digest as _, Sha256};
        format!(
            "{:x}",
            Sha256::digest(format!("{salt}\0{value}").as_bytes())
        )
    }

    fn holds(&self, ClientSecret(value): &ClientSecret) -> bool {
        Self::digest(&self.salt, value) == self.digest
    }
}

fn undelivered(client: &str, secret: &str, reason: &str, recovery: &str) -> Failure {
    let mut failure = Failure::new(
        "m2m_application_secret_undelivered",
        &format!(
            "client secret {secret} of M2M application {client} was generated, but {reason}; its value cannot be recovered. Revoke client secret {secret} and generate another: {recovery}"
        ),
        1,
    );
    failure.body["client_id"] = json!(client);
    failure.body["secret_id"] = json!(secret);
    failure
}

struct Journal {
    _organization: custody::Journal,
    directory: PathBuf,
    records: Vec<Record>,
    identity: [String; 4],
}

fn custody_failure() -> Failure {
    Failure::new(
        "organization_record_failed",
        "could not safely read or persist M2M application request custody; preserve this operator home",
        1,
    )
}

impl Journal {
    /// M2M application records share the organization custody directory and its lock,
    /// in their own file, which older CLIs never read.
    fn open(
        input: &crate::operator::InvocationConfig,
        binding: &Binding,
        api: &Api,
    ) -> Result<Self> {
        let organization = custody::Journal::open(input, binding, api)?;
        let directory = organization.directory().to_owned();
        let identity = organization.identity().map(str::to_owned);
        let records: Vec<Record> = files::read(&directory, RECORDS, MAX_BYTES)
            .map_err(|_| custody_failure())?
            .unwrap_or_default();
        let mut keys = BTreeSet::new();
        if records.len() > MAX_RECORDS
            || records.iter().any(|record| {
                record.version != 1
                    || canonical_origin(&record.api).is_err()
                    || organization_id(&record.organization_id).is_err()
                    || !plain(&record.principal_id, 1024)
                    || !plain(&record.owner_profile, 1024)
                    || record.idempotency_key.is_empty()
                    || record.idempotency_key.len() > MAX_KEY
                    || !record.idempotency_key.bytes().all(|b| b.is_ascii_graphic())
                    || !keys.insert(&record.idempotency_key)
                    || !record.progress.matches(&record.request)
            })
        {
            return Err(custody_failure());
        }
        Ok(Self {
            _organization: organization,
            directory,
            records,
            identity,
        })
    }

    fn retained(&self, key: &str, request: &Request) -> Result<Option<Progress>> {
        let Some(record) = self.records.iter().find(|r| r.idempotency_key == key) else {
            if self.records.len() >= MAX_RECORDS {
                return Err(Failure::refused(
                    "organization_custody_full",
                    "M2M application request custody is full; exact same-key retries remain available; no records were evicted",
                ));
            }
            return Ok(None);
        };
        let [api, organization, principal, owner] = &self.identity;
        if record.request != *request
            || &record.api != api
            || &record.organization_id != organization
            || &record.principal_id != principal
            || &record.owner_profile != owner
        {
            return Err(Failure::refused(
                "organization_request_conflict",
                "this key belongs to different arguments, principal or profile custody; retain the original request and identity",
            ));
        }
        Ok(Some(record.progress.clone()))
    }

    fn save(&mut self, key: &str, request: &Request, progress: &Progress) -> Result<()> {
        let [api, organization, principal, owner] = self.identity.clone();
        match self.records.iter_mut().find(|r| r.idempotency_key == key) {
            Some(record) => record.progress = progress.clone(),
            None => self.records.push(Record {
                version: 1,
                api,
                organization_id: organization,
                principal_id: principal,
                owner_profile: owner,
                idempotency_key: key.into(),
                request: request.clone(),
                progress: progress.clone(),
            }),
        }
        files::save(&self.directory, RECORDS, &self.records, MAX_BYTES)
            .map_err(|_| custody_failure())
    }
}

/// The directories under the operator home that hold the CLI's own state.
const HOME_STATE: &[&str] = &["organization", "lifecycle", "auth-state"];

/// The client secret file `command` would write: the create flag, or the
/// rotated profile's `{file: PATH}`. `None` when the command writes none, or
/// when its destination is invalid, which the command itself refuses.
fn destination(
    command: &Applications,
    input: &crate::operator::InvocationConfig,
) -> Option<ClientSecretFile> {
    let path = match command {
        Applications::Create {
            client_secret_file, ..
        } => client_secret_file.clone(),
        Applications::Secret {
            command:
                Secrets::Rotate {
                    application_profile,
                    ..
                },
        } => {
            let profile = input
                .get(&mut std::io::sink())
                .ok()?
                .schema
                .as_ref()?
                .profiles
                .get(application_profile)?;
            match &profile.auth {
                AuthProfile::OauthClientCredentials {
                    client_secret: ClientSecretSource::File { file },
                    ..
                } => file.clone(),
                _ => return None,
            }
        }
        Applications::List | Applications::Delete { .. } => return None,
    };
    ClientSecretFile::new(&path).ok()
}

/// A refusal when the client secret file would coincide with another output
/// of this invocation (`--stats`) or with the CLI's own files: the operator
/// config, credential binding state, or request custody. The caller checks
/// before anything writes, so no output can replace a delivered secret.
pub(super) fn output_collision(
    command: &Applications,
    input: &crate::operator::InvocationConfig,
    stats: Option<&Path>,
) -> Option<Failure> {
    let destination = destination(command, input)?.location();
    let mut files: Vec<(&str, PathBuf)> = Vec::new();
    if let Some(stats) = stats {
        files.push(("the --stats file", stats.to_owned()));
    }
    if let Some(config) = input.config_path() {
        files.push(("the operator configuration", config));
    }
    if let Ok(state) = input.auth_state_path() {
        files.push(("the credential binding state", state));
    }
    let directories = input
        .directory()
        .into_iter()
        .flat_map(|home| HOME_STATE.iter().map(move |name| home.join(name)));
    let collision = files
        .into_iter()
        .find(|(_, path)| resolved_location(path) == destination)
        .map(|(what, _)| what.to_owned())
        .or_else(|| {
            directories
                .map(|directory| resolved_location(&directory))
                .find(|directory| destination.starts_with(directory))
                .map(|directory| {
                    format!("inside the CLI's state directory {}", directory.display())
                })
        })?;
    Some(Failure::refused(
        "m2m_application_secret_file_collision",
        &format!(
            "the client secret file {} is {collision}, which this command also writes; choose another path; nothing was changed",
            destination.display()
        ),
    ))
}

/// The invocation facts every M2M application command shares.
pub(super) struct Invocation<'a, 'i> {
    pub input: &'a crate::operator::InvocationConfig,
    pub binding: &'a Binding,
    pub api: &'a Api,
    pub admin_profile: Option<&'a str>,
    pub yes: bool,
    pub json: bool,
    pub prompt: &'a mut crate::input::Input<'i>,
    pub stderr: &'a mut dyn Write,
}

/// The retained key of an M2M application write, before any validation.
pub(super) fn key(command: &Applications) -> Option<&str> {
    match command {
        Applications::Create {
            idempotency_key, ..
        }
        | Applications::Delete {
            idempotency_key, ..
        }
        | Applications::Secret {
            command: Secrets::Rotate {
                idempotency_key, ..
            },
        } => Some(idempotency_key),
        Applications::List => None,
    }
}

pub(super) async fn dispatch(
    run: Invocation<'_, '_>,
    command: &Applications,
) -> Result<(Value, String, i32)> {
    if let Some(key) = key(command) {
        validate_key(key)?;
    }
    let organization = run.api.account_id()?.to_owned();
    match command {
        Applications::List => {
            let (body, applications) = list_applications(run.api, &organization).await?;
            let human = list_human(&body["data"], &applications, &organization);
            Ok((body, human, 0))
        }
        Applications::Create {
            name,
            permissions,
            client_secret_file,
            replace_client_secret_file,
            create_profile,
            replace_profile,
            idempotency_key,
        } => {
            create(
                run,
                &organization,
                CreateArgs {
                    name,
                    permissions,
                    client_secret_file,
                    replace_client_secret_file: *replace_client_secret_file,
                    profile: create_profile.as_deref(),
                    replace_profile: *replace_profile,
                    key: idempotency_key,
                },
            )
            .await
        }
        Applications::Secret {
            command:
                Secrets::Rotate {
                    application_profile,
                    revoke,
                    idempotency_key,
                },
        } => {
            rotate(
                run,
                &organization,
                application_profile,
                revoke,
                idempotency_key,
            )
            .await
        }
        Applications::Delete {
            selector,
            idempotency_key,
        } => delete(run, &organization, selector, idempotency_key).await,
    }
}

fn list_human(data: &Value, applications: &[Application], organization: &str) -> String {
    let mut human = format!("Organization {organization} M2M applications\n");
    if applications.is_empty() {
        human.push_str("No M2M applications.\n");
    } else {
        let rows: Vec<[String; 5]> = std::iter::once([
            "NAME".to_owned(),
            "CLIENT ID".to_owned(),
            "ACTOR ID".to_owned(),
            "CREATED".to_owned(),
            "PERMISSIONS".to_owned(),
        ])
        .chain(applications.iter().map(|application| {
            [
                application.name.clone(),
                application.client_id.clone(),
                application.actor_id.clone(),
                application.created_at.clone(),
                if application.permissions.is_empty() {
                    "none".into()
                } else {
                    application.permissions.join(", ")
                },
            ]
        }))
        .collect();
        let widths: Vec<usize> = (0..4)
            .map(|column| {
                rows.iter()
                    .map(|row| row[column].chars().count())
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        for row in &rows {
            for (column, width) in widths.iter().enumerate() {
                write!(human, "{:<width$}  ", row[column]).unwrap();
            }
            writeln!(human, "{}", row[4]).unwrap();
        }
    }
    writeln!(
        human,
        "\nToken endpoint: {}",
        data["token_endpoint"].as_str().unwrap_or_default()
    )
    .unwrap();
    let allowed: Vec<&str> = data["grantable_permissions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    writeln!(
        human,
        "Permissions you can grant: {}",
        if allowed.is_empty() {
            "none".to_owned()
        } else {
            allowed.join(", ")
        }
    )
    .unwrap();
    human.push_str(GRAPH_BOUNDARY);
    human
}

const GRAPH_BOUNDARY: &str =
    "An M2M application has no graph access until a cluster's policy names its actor ID.\n";

struct CreateArgs<'a> {
    name: &'a str,
    permissions: &'a [String],
    client_secret_file: &'a Path,
    replace_client_secret_file: bool,
    profile: Option<&'a str>,
    replace_profile: bool,
    key: &'a str,
}

fn validate_create_permissions(permissions: &[String]) -> Result<Vec<String>> {
    let mut sorted = permissions.to_vec();
    sorted.sort();
    if !distinct_permissions(&sorted, MAX_PERMISSIONS)
        || sorted.iter().any(|s| s == "members_manage")
    {
        return Err(Failure::refused(
            "m2m_application_permissions_invalid",
            "select at most five distinct permissions you hold, excluding members_manage; each contains 1–128 ASCII letters, digits, underscores or hyphens",
        ));
    }
    Ok(sorted)
}

fn schema<'a>(
    input: &'a crate::operator::InvocationConfig,
    stderr: &mut dyn Write,
) -> Result<&'a crate::operator_schema::OperatorConfig> {
    input
        .get(stderr)
        .ok()
        .and_then(|config| config.schema.as_ref())
        .ok_or_else(|| {
            Failure::refused(
                "operator_config_invalid",
                "M2M application profiles require a readable version 1 operator configuration",
            )
        })
}

/// The administrator's own profile, as the invocation selected it.
fn admin_profile<'a>(
    config: &'a crate::operator_schema::OperatorConfig,
    selected: Option<&'a str>,
) -> Result<(&'a str, &'a crate::operator_schema::Profile)> {
    let name = selected
        .or(config.defaults.profile.as_deref())
        .ok_or_else(|| Failure::refused("profile_required", "select --profile NAME"))?;
    let profile = config
        .profiles
        .get(name)
        .ok_or_else(|| Failure::refused("profile_required", "select --profile NAME"))?;
    Ok((name, profile))
}

fn application_profile(
    admin: &crate::operator_schema::Profile,
    token_endpoint: &str,
    client_id: &str,
    client_secret_file: &Path,
) -> crate::operator_schema::Profile {
    crate::operator_schema::Profile {
        api: admin.api.clone(),
        organization_id: admin.organization_id.clone(),
        auth: AuthProfile::OauthClientCredentials {
            token_endpoint: token_endpoint.into(),
            client_id: client_id.into(),
            client_secret: ClientSecretSource::File {
                file: client_secret_file.to_owned(),
            },
            scope: None,
        },
        transport: admin.transport.clone(),
    }
}

/// Add or replace one profile under the operator config's edit lock. An equal
/// profile, written by an earlier attempt of the same request, is accepted.
fn write_profile(
    input: &crate::operator::InvocationConfig,
    name: &str,
    profile: crate::operator_schema::Profile,
    replace: bool,
) -> Result<()> {
    let mut exists = false;
    crate::commands::targets::edit(input, |config| {
        match config.profiles.get(name) {
            Some(current) if *current == profile => {}
            Some(_) if !replace => {
                exists = true;
                color_eyre::eyre::bail!("profile exists");
            }
            _ => {
                config.profiles.insert(name.into(), profile);
            }
        }
        Ok(())
    })
    .map_err(|error| {
        if exists {
            Failure::refused(
                "m2m_application_profile_exists",
                "the profile already exists; choose another name or pass --replace-profile",
            )
        } else {
            Failure::new(
                "m2m_application_profile_write_failed",
                &format!("could not write the M2M application profile: {error}"),
                1,
            )
        }
    })
}

async fn create(
    run: Invocation<'_, '_>,
    organization: &str,
    args: CreateArgs<'_>,
) -> Result<(Value, String, i32)> {
    if !application_name(args.name) {
        return Err(Failure::refused(
            "m2m_application_name_invalid",
            "M2M application names contain 1–256 bytes without surrounding whitespace or control characters",
        ));
    }
    let permissions = validate_create_permissions(args.permissions)?;
    let file = ClientSecretFile::new(args.client_secret_file)?;
    let config = schema(run.input, run.stderr)?;
    let (admin_name, admin) = admin_profile(config, run.admin_profile)?;
    if let Some(profile) = args.profile
        && (!crate::targets::valid_name(profile) || profile == admin_name)
    {
        return Err(Failure::refused(
            "m2m_application_profile_invalid",
            "the M2M application profile needs a name that starts with an ASCII letter and differs from the administrator's profile",
        ));
    }
    let request = Request::Create {
        name: args.name.into(),
        permissions: permissions.clone(),
        client_secret_file: file.path.clone(),
        profile: args.profile.map(str::to_owned),
    };
    let mut journal = Journal::open(run.input, run.binding, run.api)?;
    let retained = journal.retained(args.key, &request)?;
    if retained.is_none() {
        // A first attempt refuses existing local destinations before any write.
        file.check(args.replace_client_secret_file)?;
        if let Some(profile) = args.profile
            && config.profiles.contains_key(profile)
            && !args.replace_profile
        {
            return Err(Failure::refused(
                "m2m_application_profile_exists",
                "the profile already exists; choose another name or pass --replace-profile",
            ));
        }
    }
    let Progress::Create {
        mut token_endpoint,
        mut application,
        profile: mut profile_step,
        mut delivered,
    } = retained.unwrap_or(Progress::Create {
        token_endpoint: None,
        application: None,
        profile: None,
        delivered: None,
    })
    else {
        return Err(custody_failure());
    };
    let save = |journal: &mut Journal,
                token_endpoint: &Option<String>,
                application: &Option<Application>,
                profile_step: Option<ProfileStep>,
                delivered: &Option<Delivered>| {
        journal.save(
            args.key,
            &request,
            &Progress::Create {
                token_endpoint: token_endpoint.clone(),
                application: application.clone(),
                profile: profile_step,
                delivered: delivered.clone(),
            },
        )
    };
    if args.profile.is_some() && token_endpoint.is_none() {
        let body = send(run.api, Method::GET, ROOT, None, None).await?;
        let page: ApplicationPage = parse(&body["data"])?;
        if page.organization_id != organization {
            return Err(Failure::protocol());
        }
        validate_token_endpoint(&page.token_endpoint)?;
        token_endpoint = Some(page.token_endpoint);
    }
    save(
        &mut journal,
        &token_endpoint,
        &application,
        profile_step,
        &delivered,
    )?;
    let staged = delivered.is_none().then(|| file.stage()).transpose()?;
    let created = match application.clone() {
        Some(created) => created,
        None => {
            let body = send(
                run.api,
                Method::POST,
                ROOT,
                Some(&json!({"organization_id":organization,"name":args.name,"permissions":permissions})),
                Some(&step_key(args.key, "application")),
            )
            .await?;
            let created: Application = parse(&body["data"]["application"])?;
            validate_application(&created, organization)?;
            let mut granted = created.permissions.clone();
            granted.sort();
            if created.name != args.name || granted != permissions {
                return Err(Failure::protocol());
            }
            application = Some(created.clone());
            save(
                &mut journal,
                &token_endpoint,
                &application,
                profile_step,
                &delivered,
            )?;
            created
        }
    };
    let client = created.client_id.as_str();
    // The profile precedes the client secret, so `secret rotate
    // --application-profile` recovers one that could not be delivered. It is
    // written at most once: an installed profile is never rewritten, so a
    // replay cannot overwrite a profile a later request changed.
    if let (Some(profile), Some(endpoint)) = (args.profile, token_endpoint.as_deref())
        && profile_step != Some(ProfileStep::Installed)
    {
        let replace = args.replace_profile && profile_step.is_none();
        profile_step = Some(ProfileStep::Attempted);
        save(
            &mut journal,
            &token_endpoint,
            &application,
            profile_step,
            &delivered,
        )?;
        write_profile(
            run.input,
            profile,
            application_profile(admin, endpoint, client, &file.path),
            replace,
        )?;
        profile_step = Some(ProfileStep::Installed);
        save(
            &mut journal,
            &token_endpoint,
            &application,
            profile_step,
            &delivered,
        )?;
    }
    let secret_id = match (delivered.clone(), staged) {
        (Some(delivered), _) => delivered.secret_id,
        (None, staged) => {
            let mut body =
                generate(run.api, organization, client, &step_key(args.key, "secret")).await?;
            let (summary, secret) = take_secret(&mut body, client)?;
            let recovery = match args.profile {
                Some(profile) => format!(
                    "`omnigraph organization m2m-applications secret rotate --application-profile {profile} --idempotency-key NEW_KEY` revokes it and writes a new client secret"
                ),
                None => "use the managed console's Credentials page".to_owned(),
            };
            let Some(secret) = secret else {
                return Err(undelivered(
                    client,
                    &summary.id,
                    "an earlier attempt of this request did not record its delivery",
                    &recovery,
                ));
            };
            let staged = staged.ok_or_else(custody_failure)?;
            if file
                .deliver(staged, &secret, args.replace_client_secret_file)
                .is_err()
            {
                return Err(undelivered(
                    client,
                    &summary.id,
                    "it could not be written to the client secret file",
                    &recovery,
                ));
            }
            delivered = Some(Delivered::of(&summary.id, &secret));
            drop(secret);
            save(
                &mut journal,
                &token_endpoint,
                &application,
                profile_step,
                &delivered,
            )?;
            summary.id
        }
    };
    let body = json!({"data":{
        "organization_id": organization,
        "application": created,
        "secret_id": secret_id,
        "client_secret_file": file.path,
        "profile": args.profile,
    },"meta":{}});
    let mut human = format!(
        "M2M application {} created\nClient ID: {client}\nActor ID: {}\nClient secret {secret_id} written to {}\n",
        created.name,
        created.actor_id,
        file.path.display()
    );
    if let Some(profile) = args.profile {
        writeln!(human, "Profile: {profile}").unwrap();
    }
    human.push_str(GRAPH_BOUNDARY);
    Ok((body, human, 0))
}

/// Which client secrets a rotation revokes: the explicit selection, or the
/// only one that existed before it. More than one needs an explicit choice.
fn revocation_targets(existing: &[SecretSummary], explicit: &[String]) -> Result<Vec<String>> {
    let candidates = || {
        existing
            .iter()
            .map(|secret| {
                format!(
                    "{} (hint {}, created {})",
                    secret.id, secret.secret_hint, secret.created_at
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    };
    if !explicit.is_empty() {
        if let Some(missing) = explicit
            .iter()
            .find(|id| !existing.iter().any(|secret| &secret.id == *id))
        {
            return Err(Failure::refused(
                "m2m_application_secret_not_found",
                &format!(
                    "client secret {missing} is not one of this M2M application's client secrets: {}; nothing was changed",
                    candidates()
                ),
            ));
        }
        return Ok(explicit.to_vec());
    }
    match existing {
        [] => Ok(Vec::new()),
        [only] => Ok(vec![only.id.clone()]),
        _ => Err(Failure::refused(
            "m2m_application_secret_ambiguous",
            &format!(
                "this M2M application has {} client secrets and the CLI cannot tell which one the profile's file holds; repeat with --revoke SECRET_ID for each client secret to revoke: {}; nothing was changed",
                existing.len(),
                candidates()
            ),
        )),
    }
}

/// One client-credentials exchange through the provider's acquisition code,
/// with the value read back from the file and checked against the delivery.
/// The token is dropped unused.
async fn prove(
    profile: &str,
    organization: &str,
    token_endpoint: &str,
    client: &str,
    ClientSecret(value): &ClientSecret,
    scope: Option<&str>,
) -> std::result::Result<(), crate::credential_sources::CredentialError> {
    use crate::credential_sources::{CredentialRequest, OwnerBinding, OwnerKind, Purpose};
    let owner = OwnerBinding::new(
        OwnerKind::Profile,
        "m2m-application-secret-rotation".into(),
        profile.into(),
        token_endpoint.into(),
        Some(organization.into()),
    )?;
    let request =
        CredentialRequest::new(owner, Purpose::Control, token_endpoint.into(), None, None)?;
    crate::credential_sources::exchange_client_credentials(
        &request,
        token_endpoint,
        client,
        value,
        scope,
    )
    .await
    .map(drop)
}

async fn rotate(
    run: Invocation<'_, '_>,
    organization: &str,
    profile_name: &str,
    revoke: &[String],
    key: &str,
) -> Result<(Value, String, i32)> {
    let mut explicit = revoke.to_vec();
    explicit.sort();
    if explicit.iter().any(|id| !provider_id(id)) || explicit.windows(2).any(|w| w[0] == w[1]) {
        return Err(Failure::refused(
            "m2m_application_secret_id_invalid",
            "--revoke takes distinct exact client secret IDs",
        ));
    }
    let config = schema(run.input, run.stderr)?;
    let (admin_name, _) = admin_profile(config, run.admin_profile)?;
    let invalid = |detail: &str| Failure::refused("m2m_application_profile_invalid", detail);
    let profile = config
        .profiles
        .get(profile_name)
        .filter(|_| profile_name != admin_name)
        .ok_or_else(|| {
            invalid("--application-profile names no profile other than the administrator's")
        })?;
    let AuthProfile::OauthClientCredentials {
        token_endpoint,
        client_id,
        client_secret: ClientSecretSource::File {
            file: client_secret_file,
        },
        scope,
    } = &profile.auth
    else {
        return Err(invalid(
            "the M2M application profile must use oauth_client_credentials with a {file: PATH} client_secret",
        ));
    };
    if profile.api.trim_end_matches('/') != run.binding.api
        || profile.organization_id != organization
        || !provider_id(client_id)
        || !client_secret_file.is_absolute()
    {
        return Err(invalid(
            "the M2M application profile must name this profile's API and organization, an exact client ID and an absolute client secret file",
        ));
    }
    let file = ClientSecretFile::new(client_secret_file)?;
    // Rotation replaces the profile's own file; it must still be a regular file.
    file.check(true)?;
    let request = Request::Rotate {
        profile: profile_name.into(),
        revoke: explicit.clone(),
    };
    let mut journal = Journal::open(run.input, run.binding, run.api)?;
    let progress = match journal.retained(key, &request)? {
        Some(progress) => progress,
        None => {
            let existing = list_secrets(run.api, client_id).await?;
            let progress = Progress::Rotate {
                client_id: client_id.clone(),
                token_endpoint: token_endpoint.clone(),
                client_secret_file: file.path.clone(),
                revoke: revocation_targets(&existing, &explicit)?,
                delivered: None,
            };
            journal.save(key, &request, &progress)?;
            progress
        }
    };
    let Progress::Rotate {
        client_id: retained_client,
        token_endpoint: retained_endpoint,
        client_secret_file: retained_file,
        revoke: targets,
        delivered,
    } = progress
    else {
        return Err(custody_failure());
    };
    if &retained_client != client_id
        || &retained_endpoint != token_endpoint
        || retained_file != file.path
    {
        return Err(Failure::refused(
            "organization_request_conflict",
            "the M2M application profile changed after this key was first used; restore it or start a new request with a new key",
        ));
    }
    let client = client_id.as_str();
    let delivered = match delivered {
        Some(delivered) => delivered,
        None => {
            let staged = file.stage()?;
            let mut body =
                generate(run.api, organization, client, &step_key(key, "secret")).await?;
            let (summary, secret) = take_secret(&mut body, client)?;
            let mut revoke: Vec<&str> = vec![&summary.id];
            revoke.extend(targets.iter().map(String::as_str));
            let recovery = format!(
                "the file and the earlier client secrets are unchanged; `omnigraph organization m2m-applications secret rotate --application-profile {profile_name} {} --idempotency-key NEW_KEY` revokes it after writing a new one",
                revoke
                    .iter()
                    .map(|id| format!("--revoke {id}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            let Some(secret) = secret else {
                return Err(undelivered(
                    client,
                    &summary.id,
                    "an earlier attempt of this request did not record its delivery",
                    &recovery,
                ));
            };
            if file.deliver(staged, &secret, true).is_err() {
                return Err(undelivered(
                    client,
                    &summary.id,
                    "it could not be written to the client secret file",
                    &recovery,
                ));
            }
            let delivered = Delivered::of(&summary.id, &secret);
            drop(secret);
            journal.save(
                key,
                &request,
                &Progress::Rotate {
                    client_id: client_id.clone(),
                    token_endpoint: token_endpoint.clone(),
                    client_secret_file: file.path.clone(),
                    revoke: targets.clone(),
                    delivered: Some(delivered.clone()),
                },
            )?;
            delivered
        }
    };
    let secret_id = delivered.secret_id.clone();
    // Prove and revoke only while the file holds the client secret this
    // request delivered. A file restored to an earlier client secret after a
    // failed proof would otherwise prove that one and then revoke it.
    let current = match file.read() {
        Ok(current) if delivered.holds(&current) => current,
        _ => {
            let mut failure = Failure::refused(
                "m2m_application_secret_file_changed",
                &format!(
                    "{} no longer holds client secret {secret_id}, which this request wrote, so nothing was proven or revoked; the new and earlier client secrets both remain active. If you restored an earlier client secret on purpose, revoke {secret_id}, which no file holds; otherwise start a new rotation with a new key",
                    file.path.display()
                ),
            );
            failure.body["client_id"] = json!(client);
            failure.body["secret_id"] = json!(secret_id);
            return Err(failure);
        }
    };
    let proof = prove(
        profile_name,
        organization,
        token_endpoint,
        client,
        &current,
        scope.as_deref(),
    )
    .await;
    drop(current);
    if let Err(error) = proof {
        let mut failure = Failure::new(
            "m2m_application_secret_unproven",
            &format!(
                "the new client secret {secret_id} is in {}, but a token exchange with it failed: {error}. Nothing was revoked; the new and earlier client secrets both remain active. A new client secret can take a moment to reach the identity provider: repeat this command with the same key to prove it again and then revoke {}. To roll back instead, restore the earlier client secret value to the file and revoke {secret_id}",
                file.path.display(),
                if targets.is_empty() {
                    "nothing".to_owned()
                } else {
                    targets.join(", ")
                }
            ),
            1,
        );
        failure.body["client_id"] = json!(client);
        failure.body["secret_id"] = json!(secret_id);
        return Err(failure);
    }
    for (index, target) in targets.iter().enumerate() {
        let body = send(
            run.api,
            Method::POST,
            &format!("{ROOT}/{client}/secrets/{target}:revoke"),
            Some(&json!({"organization_id":organization})),
            Some(&step_key(key, &format!("revoke-{index}"))),
        )
        .await;
        let confirmed = body.as_ref().is_ok_and(|body| {
            body["data"]["client_id"] == json!(client)
                && body["data"]["secret_id"] == json!(target)
                && body["data"]["revoked"] == json!(true)
        });
        if !confirmed {
            write_diagnostic(
                run.stderr,
                format_args!(
                    "The new client secret {secret_id} is in {} and its token exchange succeeded; revoking client secret {target} did not complete.\n",
                    file.path.display()
                ),
            )?;
            return Err(body.err().unwrap_or_else(Failure::protocol));
        }
    }
    let body = json!({"data":{
        "organization_id": organization,
        "client_id": client,
        "secret_id": secret_id,
        "client_secret_file": file.path,
        "profile": profile_name,
        "proved": true,
        "revoked": targets,
    },"meta":{}});
    let human = format!(
        "Client secret {secret_id} of M2M application {client} written to {}\nToken exchange: succeeded\nRevoked: {}\nTokens issued with a revoked client secret stay valid until they expire.\n",
        file.path.display(),
        if targets.is_empty() {
            "none".to_owned()
        } else {
            targets.join(", ")
        }
    );
    Ok((body, human, 0))
}

async fn delete(
    run: Invocation<'_, '_>,
    organization: &str,
    selector: &str,
    key: &str,
) -> Result<(Value, String, i32)> {
    if !application_name(selector) {
        return Err(Failure::refused(
            "identifier_invalid",
            "provide an M2M application name or exact client ID without surrounding whitespace or control characters",
        ));
    }
    let request = Request::Delete {
        selector: selector.into(),
    };
    let mut journal = Journal::open(run.input, run.binding, run.api)?;
    let retained = journal.retained(key, &request)?;
    let progress = match retained.clone() {
        Some(progress) => progress,
        None => {
            let (_, applications) = list_applications(run.api, organization).await?;
            let by_client: Vec<&Application> = applications
                .iter()
                .filter(|a| a.client_id == selector)
                .collect();
            let matches = if by_client.is_empty() {
                applications.iter().filter(|a| a.name == selector).collect()
            } else {
                by_client
            };
            let application = match matches.as_slice() {
                [application] => *application,
                [] => {
                    return Err(Failure::refused(
                        "m2m_application_not_found",
                        "no M2M application in the selected organization has this name or client ID",
                    ));
                }
                _ => {
                    return Err(Failure::refused(
                        "m2m_application_ambiguous",
                        "more than one M2M application has this name; use its exact client ID",
                    ));
                }
            };
            Progress::Delete {
                client_id: application.client_id.clone(),
                actor_id: application.actor_id.clone(),
                name: application.name.clone(),
            }
        }
    };
    let Progress::Delete {
        client_id,
        actor_id,
        name,
    } = &progress
    else {
        return Err(custody_failure());
    };
    crate::helpers::confirm_destructive(
        &format!("organization m2m-applications delete {name} ({client_id})"),
        &run.binding.api,
        run.yes,
        run.json,
        run.prompt,
        run.stderr,
    )
    .map_err(|error| Failure::refused("confirmation_required", &error.to_string()))?;
    if retained.is_none() {
        journal.save(key, &request, &progress)?;
    }
    let body = send(
        run.api,
        Method::POST,
        &format!("{ROOT}/{client_id}:delete"),
        Some(&json!({"organization_id":organization})),
        Some(key),
    )
    .await?;
    if body["data"]["client_id"] != json!(client_id) || body["data"]["deleted"] != json!(true) {
        return Err(Failure::protocol());
    }
    let human = format!(
        "M2M application {name} ({client_id}) deleted.\nTokens it already holds stay valid until they expire. A cluster's policy that names actor ID {actor_id} is unchanged; remove it through the configuration workflow.\n"
    );
    Ok((body, human, 0))
}

#[cfg(test)]
mod tests;
