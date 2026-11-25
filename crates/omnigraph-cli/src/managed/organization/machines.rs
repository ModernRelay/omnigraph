//! Organization machine applications: unattended client identities. A secret
//! the service discloses reaches only the file the administrator names; graph
//! access remains in applied Cedar (RFC 2026-11-25-organization-machine-applications).
use super::*;
use crate::cli::{
    OrganizationMachineSecretCommand as Secrets, OrganizationMachinesCommand as Machines,
};
use crate::managed::custody as files;
use crate::operator_schema::{AuthProfile, ClientSecretSource};
use std::path::{Path, PathBuf};

const ROOT: &str = "/v1/organization/machines";
const RECORDS: &str = "machines.json";
const MAX_RECORDS: usize = 128;
const MAX_BYTES: u64 = 1024 * 1024;
/// Room for the step suffix each derived write key carries.
const MAX_KEY: usize = 200;
/// Listing follows at most ten pages of 100, as invitation lookup does.
const MAX_PAGES: usize = 10;
const MAX_SCOPES: usize = 5;

/// The request identity a key retains; flags that only consent to replacing
/// local files are not part of it.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Create {
        name: String,
        scopes: Vec<String>,
        secret_file: PathBuf,
        profile: Option<String>,
    },
    Rotate {
        profile: String,
        revoke: Vec<String>,
    },
    Revoke {
        selector: String,
    },
}

/// What one request has durably established, written before the next write.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum Progress {
    Create {
        token_endpoint: Option<String>,
        machine: Option<Machine>,
        /// The secret ID whose value this request wrote to its file.
        delivered: Option<String>,
    },
    Rotate {
        client_id: String,
        token_endpoint: String,
        secret_file: PathBuf,
        /// Secret IDs chosen from those that existed before the new secret.
        revoke: Vec<String>,
        delivered: Option<String>,
    },
    Revoke {
        client_id: String,
        actor_id: String,
        name: String,
    },
}

impl Progress {
    fn matches(&self, request: &Request) -> bool {
        matches!(
            (self, request),
            (Self::Create { .. }, Request::Create { .. })
                | (Self::Rotate { .. }, Request::Rotate { .. })
                | (Self::Revoke { .. }, Request::Revoke { .. })
        )
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Machine {
    application_id: String,
    client_id: String,
    organization_id: String,
    name: String,
    scopes: Vec<String>,
    principal_id: String,
    actor_id: String,
    created_at: String,
    updated_at: String,
}

#[derive(Deserialize)]
struct MachinePage {
    organization_id: String,
    machines: Vec<Value>,
    #[serde(deserialize_with = "Option::deserialize")]
    after: Option<String>,
    allowed_scopes: Vec<String>,
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
    secret_available: bool,
}

/// A disclosed machine secret. It implements no Debug, Display or Serialize,
/// so the only thing code can do with it is write it to the secret file.
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

fn machine_name(value: &str) -> bool {
    plain(value, 256) && value.trim() == value
}

fn scope(value: &str) -> bool {
    crate::helpers::valid_managed_identifier(value, 128)
}

fn distinct_scopes(scopes: &[String], max: usize) -> bool {
    let mut seen = BTreeSet::new();
    scopes.len() <= max
        && scopes
            .iter()
            .all(|value| scope(value) && seen.insert(value))
}

fn timestamp(value: &str) -> bool {
    provider_timestamp(value).is_ok()
}

fn validate_machine(machine: &Machine, organization: &str) -> Result<()> {
    if machine.organization_id != organization
        || !provider_id(&machine.application_id)
        || !provider_id(&machine.client_id)
        || !provider_id(&machine.principal_id)
        || machine.actor_id != format!("principal:{}", machine.principal_id)
        || !machine_name(&machine.name)
        || !distinct_scopes(&machine.scopes, 256)
        || !timestamp(&machine.created_at)
        || !timestamp(&machine.updated_at)
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
            "machine idempotency keys contain 1–200 printable ASCII characters; the CLI derives one key per write from it",
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

/// Every machine page, combined into one envelope with a null cursor.
async fn list_machines(api: &Api, organization: &str) -> Result<(Value, Vec<Machine>)> {
    let mut cursor: Option<String> = None;
    let mut cursors = BTreeSet::new();
    let mut clients = BTreeSet::new();
    let mut raw = Vec::new();
    let mut machines = Vec::new();
    let mut first: Option<(Value, MachinePage)> = None;
    for _ in 0..MAX_PAGES {
        let body = send(
            api,
            Method::GET,
            &page_path(ROOT, cursor.as_deref()),
            None,
            None,
        )
        .await?;
        let page: MachinePage = parse(&body["data"])?;
        if page.organization_id != organization
            || page.machines.len() > 100
            || !distinct_scopes(&page.allowed_scopes, 256)
            || page
                .after
                .as_deref()
                .is_some_and(|after| !plain(after, 1024))
        {
            return Err(Failure::protocol());
        }
        validate_token_endpoint(&page.token_endpoint)?;
        if let Some((_, head)) = &first
            && (head.allowed_scopes != page.allowed_scopes
                || head.token_endpoint != page.token_endpoint)
        {
            return Err(Failure::protocol());
        }
        for value in &page.machines {
            let machine: Machine = parse(value)?;
            validate_machine(&machine, organization)?;
            if !clients.insert(machine.client_id.clone()) {
                return Err(Failure::protocol());
            }
            raw.push(value.clone());
            machines.push(machine);
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
                    "machines": raw,
                    "after": null,
                    "allowed_scopes": head.allowed_scopes,
                    "token_endpoint": head.token_endpoint,
                },"meta": meta});
                return Ok((envelope, machines));
            }
            Some(after) if cursors.insert(after.clone()) => cursor = Some(after),
            Some(_) => return Err(Failure::protocol()),
        }
    }
    Err(Failure::refused(
        "machine_list_incomplete",
        "the machine list exceeds ten pages; nothing was changed",
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
        "machine_secret_list_incomplete",
        "the secret list exceeds ten pages; nothing was changed",
    ))
}

/// Generate one secret. Its response is the only one that keeps a secret, and
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

/// Remove the disclosed secret from a generate response before anything else
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
    if generated.client_id != client
        || generated.secret_available != secret.is_some()
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

/// The secret's destination: an absolute path in an existing directory.
struct SecretFile {
    path: PathBuf,
    directory: PathBuf,
}

impl SecretFile {
    fn new(path: &Path) -> Result<Self> {
        let invalid = |detail: &str| Failure::refused("secret_file_invalid", detail);
        let path = std::path::absolute(path)
            .map_err(|_| invalid("the secret file path cannot be made absolute"))?;
        if path.file_name().is_none() {
            return Err(invalid("the secret file path must name a file"));
        }
        let directory = path
            .parent()
            .filter(|parent| parent.is_dir())
            .ok_or_else(|| invalid("the secret file's directory must already exist"))?
            .to_owned();
        Ok(Self { path, directory })
    }

    /// An existing file must be a regular file, never a link the write would
    /// follow or replace; `replace` consents to overwriting it.
    fn check(&self, replace: bool) -> Result<()> {
        match std::fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(Failure::refused(
                "secret_file_invalid",
                "the secret file cannot be inspected",
            )),
            Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
                Err(Failure::refused(
                    "secret_file_invalid",
                    "the secret file exists and is not a regular file; the CLI writes regular files only",
                ))
            }
            Ok(_) if replace => Ok(()),
            Ok(_) => Err(Failure::refused(
                "secret_file_exists",
                "the secret file already exists; choose another path or pass --replace-secret-file",
            )),
        }
    }

    /// Create the owner-only temporary file before any remote write, so a
    /// directory that cannot hold it refuses before a secret exists.
    fn stage(&self) -> Result<tempfile::NamedTempFile> {
        let mut builder = tempfile::Builder::new();
        builder.prefix(".omnigraph-secret-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o600));
        }
        builder.tempfile_in(&self.directory).map_err(|_| {
            Failure::refused(
                "secret_file_invalid",
                "cannot create a file in the secret file's directory; nothing was changed",
            )
        })
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
}

fn undelivered(client: &str, secret: &str, reason: &str, recovery: &str) -> Failure {
    let mut failure = Failure::new(
        "machine_secret_undelivered",
        &format!(
            "secret {secret} of machine {client} was generated, but {reason}; its value cannot be recovered. Revoke secret {secret} and generate another: {recovery}"
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
        "could not safely read or persist machine request custody; preserve this operator home",
        1,
    )
}

impl Journal {
    /// Machine records share the organization custody directory and its lock,
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
                    "machine request custody is full; exact same-key retries remain available; no records were evicted",
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

/// The invocation facts every machine command shares.
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

/// The retained key of a machine write, before any validation.
pub(super) fn key(command: &Machines) -> Option<&str> {
    match command {
        Machines::Create {
            idempotency_key, ..
        }
        | Machines::Revoke {
            idempotency_key, ..
        }
        | Machines::Secret {
            command: Secrets::Rotate {
                idempotency_key, ..
            },
        } => Some(idempotency_key),
        Machines::List => None,
    }
}

pub(super) async fn dispatch(
    run: Invocation<'_, '_>,
    command: &Machines,
) -> Result<(Value, String, i32)> {
    if let Some(key) = key(command) {
        validate_key(key)?;
    }
    let organization = run.api.account_id()?.to_owned();
    match command {
        Machines::List => {
            let (body, machines) = list_machines(run.api, &organization).await?;
            let human = list_human(&body["data"], &machines, &organization);
            Ok((body, human, 0))
        }
        Machines::Create {
            name,
            scopes,
            secret_file,
            replace_secret_file,
            create_profile,
            replace_profile,
            idempotency_key,
        } => {
            create(
                run,
                &organization,
                CreateArgs {
                    name,
                    scopes,
                    secret_file,
                    replace_secret_file: *replace_secret_file,
                    profile: create_profile.as_deref(),
                    replace_profile: *replace_profile,
                    key: idempotency_key,
                },
            )
            .await
        }
        Machines::Secret {
            command:
                Secrets::Rotate {
                    machine_profile,
                    revoke,
                    idempotency_key,
                },
        } => rotate(run, &organization, machine_profile, revoke, idempotency_key).await,
        Machines::Revoke {
            selector,
            idempotency_key,
        } => retire(run, &organization, selector, idempotency_key).await,
    }
}

fn list_human(data: &Value, machines: &[Machine], organization: &str) -> String {
    let mut human = format!("Organization {organization} machines\n");
    if machines.is_empty() {
        human.push_str("No machines.\n");
    } else {
        let rows: Vec<[String; 5]> = std::iter::once([
            "NAME".to_owned(),
            "CLIENT ID".to_owned(),
            "CEDAR PRINCIPAL".to_owned(),
            "CREATED".to_owned(),
            "SCOPES".to_owned(),
        ])
        .chain(machines.iter().map(|machine| {
            [
                machine.name.clone(),
                machine.client_id.clone(),
                machine.actor_id.clone(),
                machine.created_at.clone(),
                if machine.scopes.is_empty() {
                    "none".into()
                } else {
                    machine.scopes.join(", ")
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
    let allowed: Vec<&str> = data["allowed_scopes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    writeln!(
        human,
        "Scopes you can grant: {}",
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
    "A machine has no graph access until applied Cedar names its principal.\n";

struct CreateArgs<'a> {
    name: &'a str,
    scopes: &'a [String],
    secret_file: &'a Path,
    replace_secret_file: bool,
    profile: Option<&'a str>,
    replace_profile: bool,
    key: &'a str,
}

fn validate_create_scopes(scopes: &[String]) -> Result<Vec<String>> {
    let mut sorted = scopes.to_vec();
    sorted.sort();
    if !distinct_scopes(&sorted, MAX_SCOPES) || sorted.iter().any(|s| s == "members_manage") {
        return Err(Failure::refused(
            "scopes_invalid",
            "select at most five distinct service permissions you hold, excluding members_manage; each contains 1–128 ASCII letters, digits, underscores or hyphens",
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
                "machine profiles require a readable version 1 operator configuration",
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

fn machine_profile(
    admin: &crate::operator_schema::Profile,
    token_endpoint: &str,
    client_id: &str,
    secret_file: &Path,
) -> crate::operator_schema::Profile {
    crate::operator_schema::Profile {
        api: admin.api.clone(),
        organization_id: admin.organization_id.clone(),
        auth: AuthProfile::OauthClientCredentials {
            token_endpoint: token_endpoint.into(),
            client_id: client_id.into(),
            client_secret: ClientSecretSource::File {
                file: secret_file.to_owned(),
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
                "profile_exists",
                "the profile already exists; choose another name or pass --replace-profile",
            )
        } else {
            Failure::new(
                "profile_write_failed",
                &format!("could not write the machine profile: {error}"),
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
    if !machine_name(args.name) {
        return Err(Failure::refused(
            "machine_name_invalid",
            "machine names contain 1–256 bytes without surrounding whitespace or control characters",
        ));
    }
    let scopes = validate_create_scopes(args.scopes)?;
    let file = SecretFile::new(args.secret_file)?;
    let config = schema(run.input, run.stderr)?;
    let (admin_name, admin) = admin_profile(config, run.admin_profile)?;
    if let Some(profile) = args.profile
        && (!crate::targets::valid_name(profile) || profile == admin_name)
    {
        return Err(Failure::refused(
            "profile_invalid",
            "the machine profile needs a name that starts with an ASCII letter and differs from the administrator's profile",
        ));
    }
    let request = Request::Create {
        name: args.name.into(),
        scopes: scopes.clone(),
        secret_file: file.path.clone(),
        profile: args.profile.map(str::to_owned),
    };
    let mut journal = Journal::open(run.input, run.binding, run.api)?;
    let retained = journal.retained(args.key, &request)?;
    if retained.is_none() {
        // A first attempt refuses existing local destinations before any write.
        file.check(args.replace_secret_file)?;
        if let Some(profile) = args.profile
            && config.profiles.contains_key(profile)
            && !args.replace_profile
        {
            return Err(Failure::refused(
                "profile_exists",
                "the profile already exists; choose another name or pass --replace-profile",
            ));
        }
    }
    let Progress::Create {
        mut token_endpoint,
        mut machine,
        mut delivered,
    } = retained.unwrap_or(Progress::Create {
        token_endpoint: None,
        machine: None,
        delivered: None,
    })
    else {
        return Err(custody_failure());
    };
    let save = |journal: &mut Journal,
                token_endpoint: &Option<String>,
                machine: &Option<Machine>,
                delivered: &Option<String>| {
        journal.save(
            args.key,
            &request,
            &Progress::Create {
                token_endpoint: token_endpoint.clone(),
                machine: machine.clone(),
                delivered: delivered.clone(),
            },
        )
    };
    if args.profile.is_some() && token_endpoint.is_none() {
        let body = send(run.api, Method::GET, ROOT, None, None).await?;
        let page: MachinePage = parse(&body["data"])?;
        if page.organization_id != organization {
            return Err(Failure::protocol());
        }
        validate_token_endpoint(&page.token_endpoint)?;
        token_endpoint = Some(page.token_endpoint);
    }
    save(&mut journal, &token_endpoint, &machine, &delivered)?;
    let staged = delivered.is_none().then(|| file.stage()).transpose()?;
    let created = match machine.clone() {
        Some(created) => created,
        None => {
            let body = send(
                run.api,
                Method::POST,
                ROOT,
                Some(&json!({"organization_id":organization,"name":args.name,"scopes":scopes})),
                Some(&step_key(args.key, "machine")),
            )
            .await?;
            let created: Machine = parse(&body["data"]["machine"])?;
            validate_machine(&created, organization)?;
            let mut granted = created.scopes.clone();
            granted.sort();
            if created.name != args.name || granted != scopes {
                return Err(Failure::protocol());
            }
            machine = Some(created.clone());
            save(&mut journal, &token_endpoint, &machine, &delivered)?;
            created
        }
    };
    let client = created.client_id.as_str();
    // The profile precedes the secret, so `secret rotate --machine-profile`
    // recovers a secret that could not be delivered.
    if let (Some(profile), Some(endpoint)) = (args.profile, token_endpoint.as_deref()) {
        write_profile(
            run.input,
            profile,
            machine_profile(admin, endpoint, client, &file.path),
            args.replace_profile,
        )?;
    }
    let secret_id = match (delivered.clone(), staged) {
        (Some(secret_id), _) => secret_id,
        (None, staged) => {
            let mut body =
                generate(run.api, organization, client, &step_key(args.key, "secret")).await?;
            let (summary, secret) = take_secret(&mut body, client)?;
            let recovery = match args.profile {
                Some(profile) => format!(
                    "`omnigraph organization machines secret rotate --machine-profile {profile} --idempotency-key NEW_KEY` revokes it and writes a new secret"
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
                .deliver(staged, &secret, args.replace_secret_file)
                .is_err()
            {
                return Err(undelivered(
                    client,
                    &summary.id,
                    "it could not be written to the secret file",
                    &recovery,
                ));
            }
            drop(secret);
            delivered = Some(summary.id.clone());
            save(&mut journal, &token_endpoint, &machine, &delivered)?;
            summary.id
        }
    };
    let body = json!({"data":{
        "organization_id": organization,
        "machine": created,
        "secret_id": secret_id,
        "secret_file": file.path,
        "profile": args.profile,
    },"meta":{}});
    let mut human = format!(
        "Machine {} created\nClient ID: {client}\nCedar principal: {}\nSecret {secret_id} written to {}\n",
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

/// Which secrets a rotation revokes: the explicit selection, or the only
/// secret that existed before it. More than one needs an explicit choice.
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
                "machine_secret_not_found",
                &format!(
                    "secret {missing} is not one of this machine's secrets: {}; nothing was changed",
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
            "machine_secret_ambiguous",
            &format!(
                "this machine has {} secrets and the CLI cannot tell which one the profile's file holds; repeat with --revoke SECRET_ID for each secret to retire: {}; nothing was changed",
                existing.len(),
                candidates()
            ),
        )),
    }
}

/// One client-credentials exchange through the provider's acquisition code,
/// reading the secret back from its file. The token is dropped unused.
async fn prove(
    profile: &str,
    organization: &str,
    token_endpoint: &str,
    client: &str,
    secret_file: &Path,
    scope: Option<&str>,
) -> std::result::Result<(), crate::credential_sources::CredentialError> {
    use crate::credential_sources::{CredentialRequest, OwnerBinding, OwnerKind, Purpose};
    let owner = OwnerBinding::new(
        OwnerKind::Profile,
        "machine-secret-rotation".into(),
        profile.into(),
        token_endpoint.into(),
        Some(organization.into()),
    )?;
    let request =
        CredentialRequest::new(owner, Purpose::Control, token_endpoint.into(), None, None)?;
    crate::credential_sources::acquire_client_credentials(
        &request,
        token_endpoint,
        client,
        &ClientSecretSource::File {
            file: secret_file.to_owned(),
        },
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
            "secret_id_invalid",
            "--revoke takes distinct exact secret IDs",
        ));
    }
    let config = schema(run.input, run.stderr)?;
    let (admin_name, _) = admin_profile(config, run.admin_profile)?;
    let invalid = |detail: &str| Failure::refused("machine_profile_invalid", detail);
    let profile = config
        .profiles
        .get(profile_name)
        .filter(|_| profile_name != admin_name)
        .ok_or_else(|| {
            invalid("--machine-profile names no machine profile other than the administrator's")
        })?;
    let AuthProfile::OauthClientCredentials {
        token_endpoint,
        client_id,
        client_secret: ClientSecretSource::File { file: secret_file },
        scope,
    } = &profile.auth
    else {
        return Err(invalid(
            "the machine profile must use oauth_client_credentials with a {file: PATH} client_secret",
        ));
    };
    if profile.api.trim_end_matches('/') != run.binding.api
        || profile.organization_id != organization
        || !provider_id(client_id)
        || !secret_file.is_absolute()
    {
        return Err(invalid(
            "the machine profile must name this profile's API and organization, an exact client ID and an absolute secret file",
        ));
    }
    let file = SecretFile::new(secret_file)?;
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
                secret_file: file.path.clone(),
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
        secret_file: retained_file,
        revoke: targets,
        mut delivered,
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
            "the machine profile changed after this key was first used; restore it or start a new request with a new key",
        ));
    }
    let client = client_id.as_str();
    let secret_id = match delivered.clone() {
        Some(secret_id) => secret_id,
        None => {
            let staged = file.stage()?;
            let mut body =
                generate(run.api, organization, client, &step_key(key, "secret")).await?;
            let (summary, secret) = take_secret(&mut body, client)?;
            let mut retire: Vec<&str> = vec![&summary.id];
            retire.extend(targets.iter().map(String::as_str));
            let recovery = format!(
                "the file and the earlier secrets are unchanged; `omnigraph organization machines secret rotate --machine-profile {profile_name} {} --idempotency-key NEW_KEY` revokes it after writing a new one",
                retire
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
                    "it could not be written to the secret file",
                    &recovery,
                ));
            }
            drop(secret);
            delivered = Some(summary.id.clone());
            journal.save(
                key,
                &request,
                &Progress::Rotate {
                    client_id: client_id.clone(),
                    token_endpoint: token_endpoint.clone(),
                    secret_file: file.path.clone(),
                    revoke: targets.clone(),
                    delivered: delivered.clone(),
                },
            )?;
            summary.id
        }
    };
    if let Err(error) = prove(
        profile_name,
        organization,
        token_endpoint,
        client,
        &file.path,
        scope.as_deref(),
    )
    .await
    {
        let mut failure = Failure::new(
            "machine_secret_unproven",
            &format!(
                "the new secret {secret_id} is in {}, but a token exchange with it failed: {error}. Nothing was revoked; the new and earlier secrets both remain active. A new secret can take a moment to reach the identity provider: repeat this command with the same key to prove it again and then revoke {}. To roll back instead, restore the earlier secret value to the file and revoke {secret_id}",
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
                    "The new secret {secret_id} is in {} and its token exchange succeeded; revoking secret {target} did not complete.\n",
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
        "secret_file": file.path,
        "profile": profile_name,
        "proved": true,
        "revoked": targets,
    },"meta":{}});
    let human = format!(
        "Secret {secret_id} of machine {client} written to {}\nToken exchange: succeeded\nRevoked: {}\nTokens issued with a revoked secret stay valid until they expire.\n",
        file.path.display(),
        if targets.is_empty() {
            "none".to_owned()
        } else {
            targets.join(", ")
        }
    );
    Ok((body, human, 0))
}

async fn retire(
    run: Invocation<'_, '_>,
    organization: &str,
    selector: &str,
    key: &str,
) -> Result<(Value, String, i32)> {
    if !machine_name(selector) {
        return Err(Failure::refused(
            "identifier_invalid",
            "provide a machine name or exact client ID without surrounding whitespace or control characters",
        ));
    }
    let request = Request::Revoke {
        selector: selector.into(),
    };
    let mut journal = Journal::open(run.input, run.binding, run.api)?;
    let retained = journal.retained(key, &request)?;
    let progress = match retained.clone() {
        Some(progress) => progress,
        None => {
            let (_, machines) = list_machines(run.api, organization).await?;
            let by_client: Vec<&Machine> = machines
                .iter()
                .filter(|m| m.client_id == selector)
                .collect();
            let matches = if by_client.is_empty() {
                machines.iter().filter(|m| m.name == selector).collect()
            } else {
                by_client
            };
            let machine = match matches.as_slice() {
                [machine] => *machine,
                [] => {
                    return Err(Failure::refused(
                        "machine_not_found",
                        "no machine in the selected organization has this name or client ID",
                    ));
                }
                _ => {
                    return Err(Failure::refused(
                        "machine_ambiguous",
                        "more than one machine has this name; use its exact client ID",
                    ));
                }
            };
            Progress::Revoke {
                client_id: machine.client_id.clone(),
                actor_id: machine.actor_id.clone(),
                name: machine.name.clone(),
            }
        }
    };
    let Progress::Revoke {
        client_id,
        actor_id,
        name,
    } = &progress
    else {
        return Err(custody_failure());
    };
    crate::helpers::confirm_destructive(
        &format!("organization machines revoke {name} ({client_id})"),
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
        &format!("{ROOT}/{client_id}:revoke"),
        Some(&json!({"organization_id":organization})),
        Some(key),
    )
    .await?;
    if body["data"]["client_id"] != json!(client_id) || body["data"]["revoked"] != json!(true) {
        return Err(Failure::protocol());
    }
    let human = format!(
        "Machine {name} ({client_id}) retired.\nTokens it already holds stay valid until they expire. Applied Cedar still names {actor_id}; remove it through the configuration workflow.\n"
    );
    Ok((body, human, 0))
}

#[cfg(test)]
mod tests;
