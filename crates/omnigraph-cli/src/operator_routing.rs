use crate::credential_sources::{
    self as credentials, CredentialRequest, OwnerBinding, OwnerKind, Purpose,
};
use crate::operator_schema::{self as schema, AuthProfile, Target, TokenSource};
use crate::targets::{StorageProvider, TargetAuth, TargetEntry, TargetKind, TargetScope};
use color_eyre::eyre::{Result, bail, eyre};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) fn parse(text: &str, base: &Path) -> Result<crate::operator::OperatorConfig> {
    let source = schema::parse(text, base)?;
    let mut targets = BTreeMap::new();
    for (name, target) in &source.targets {
        let mut entry = TargetEntry {
            kind: TargetKind::Direct,
            scope: None,
            uri: None,
            provider: None,
            endpoint: None,
            credential: None,
            auth: None,
            default_graph: None,
            api: None,
            organization_id: None,
            cluster_id: None,
            cluster_incarnation: None,
            control_auth: None,
        };
        match target {
            Target::Direct {
                scope,
                uri,
                default_graph,
            } => {
                entry.scope = Some(match scope {
                    schema::DirectScope::Graph => TargetScope::Graph,
                    schema::DirectScope::Root => TargetScope::Root,
                });
                entry.uri = Some(uri.clone());
                entry.provider = Some(StorageProvider::Local);
                entry.default_graph = default_graph.clone();
            }
            Target::Server {
                endpoint,
                auth,
                default_graph,
                ..
            } => {
                entry.kind = TargetKind::Server;
                entry.endpoint = Some(endpoint.clone());
                entry.default_graph = default_graph.clone();
                if matches!(auth, AuthProfile::Anonymous) {
                    entry.auth = Some(TargetAuth::Anonymous);
                } else {
                    entry.credential = Some(name.clone());
                }
            }
            Target::Managed {
                profile,
                cluster_id,
                cluster_incarnation,
                endpoint,
                default_graph,
                ..
            } => {
                let owner = &source.profiles[profile];
                entry.kind = TargetKind::Managed;
                entry.scope = Some(TargetScope::Cluster);
                entry.api = Some(owner.api.trim_end_matches('/').to_owned());
                entry.organization_id = Some(owner.organization_id.clone());
                entry.cluster_id = Some(cluster_id.clone());
                entry.cluster_incarnation = Some(cluster_incarnation.clone());
                entry.endpoint = endpoint.clone();
                entry.default_graph = default_graph.clone();
                entry.control_auth = Some(crate::managed::ControlAuth::HumanDevice);
            }
        }
        targets.insert(name.clone(), entry);
    }
    let aliases = source
        .aliases
        .iter()
        .map(|(name, alias)| {
            Ok((
                name.clone(),
                crate::operator::OperatorAlias {
                    target: alias.target.clone(),
                    graph: alias.graph.clone(),
                    query: alias.query.clone(),
                    params: yaml_serde::to_value(&alias.params)?
                        .as_mapping()
                        .cloned()
                        .ok_or_else(|| eyre!("invalid alias parameters"))?,
                    format: None,
                },
            ))
        })
        .collect::<Result<_>>()?;
    let output = match source.defaults.output {
        schema::OutputFormat::Table => crate::read_format::ReadOutputFormat::Table,
        schema::OutputFormat::Json => crate::read_format::ReadOutputFormat::Json,
        schema::OutputFormat::Jsonl => crate::read_format::ReadOutputFormat::Jsonl,
        schema::OutputFormat::Csv => crate::read_format::ReadOutputFormat::Csv,
        schema::OutputFormat::Kv => crate::read_format::ReadOutputFormat::Kv,
    };
    Ok(crate::operator::OperatorConfig {
        targets,
        aliases,
        base_dir: base.to_owned(),
        operator: Default::default(),
        defaults: crate::operator::OperatorDefaults {
            target: source.defaults.target.clone(),
            output: Some(output),
            ..Default::default()
        },
        schema: Some(source),
    })
}

#[derive(Debug)]
pub(crate) struct Authentication {
    pub(crate) owner: OwnerBinding,
    pub(crate) profile: AuthProfile,
    pub(crate) name: String,
    pub(crate) request: CredentialRequest,
    pub(crate) derived_credentials: crate::managed::data::InvocationCredentials,
    custody: String,
    ids: BindingIds,
    credential: tokio::sync::OnceCell<credentials::AcquiredCredential>,
}

impl Authentication {
    async fn acquire_stored_with<G, S: credentials::SecretStore>(
        &self,
        store: &S,
        lock: impl std::future::Future<Output = Result<G>>,
    ) -> Result<credentials::AcquiredCredential> {
        let _lock = lock.await?;
        self.ensure_current()?;
        Ok(credentials::acquire_stored(&self.request, store)?)
    }

    pub(crate) fn ensure_current(&self) -> Result<()> {
        let _lock = lock_state(&self.ids.path)?;
        check_rename_pending(&self.ids.config_path)?;
        check_state_rename_pending(&self.ids.path)?;
        let (text, state) = binding_state(&self.ids.config_path, &self.ids.path)?;
        if text.is_none() {
            bail!("profile binding disappeared");
        }
        let current = state
            .owners
            .values()
            .find(|owner| owner.id == self.ids.owner)
            .and_then(|owner| {
                owner
                    .profiles
                    .values()
                    .find(|profile| profile.id == self.ids.profile)
            });
        if !current.is_some_and(|profile| profile.epoch == self.ids.epoch) {
            bail!("selected credential generation changed; retry with a fresh invocation");
        }
        Ok(())
    }
    pub(crate) fn invalidate(&self) -> Result<()> {
        let _lock = lock_state(&self.ids.path)?;
        check_rename_pending(&self.ids.config_path)?;
        check_state_rename_pending(&self.ids.path)?;
        let (text, mut state) = binding_state(&self.ids.config_path, &self.ids.path)?;
        if text.is_none() {
            bail!("owner/profile binding metadata disappeared");
        }
        let profile = state
            .owners
            .get_mut(&self.ids.key)
            .and_then(|owner| owner.profiles.get_mut(&self.name))
            .ok_or_else(|| eyre!("selected profile binding disappeared"))?;
        if profile.id != self.ids.profile {
            bail!("selected profile binding changed");
        }
        profile.epoch = profile
            .epoch
            .checked_add(1)
            .ok_or_else(|| eyre!("credential generation exhausted"))?;
        crate::targets::replace_file(&self.ids.path, &serde_json::to_string(&state)?)
    }
    /// The selected source's credential, read from it now.
    async fn acquire(&self) -> Result<credentials::AcquiredCredential> {
        Ok(match &self.profile {
            AuthProfile::Token {
                source: TokenSource::Env { env },
            } => credentials::acquire_environment(&self.request, env)?,
            AuthProfile::Token {
                source: TokenSource::File { file },
            } => credentials::acquire_file(&self.request, file)?,
            AuthProfile::Token {
                source: TokenSource::Store { .. },
            } => {
                self.acquire_stored_with(
                    &credentials::OsSecretStore,
                    crate::managed::credential_lock(),
                )
                .await?
            }
            AuthProfile::Exec { command, timeout } => {
                credentials::acquire_exec(
                    &self.request,
                    &credentials::ExecSource::new(command.clone(), timeout)?,
                )
                .await?
            }
            _ => bail!("selected provider does not supply a static bearer"),
        })
    }
    pub(crate) async fn token(&self) -> Result<Option<String>> {
        self.acquire_credential()
            .await?
            .map(|credential| {
                Ok(credential
                    .bearer_for(&self.request, time::OffsetDateTime::now_utc())?
                    .expose()
                    .to_owned())
            })
            .transpose()
    }

    pub(crate) async fn acquire_credential(
        &self,
    ) -> Result<Option<credentials::AcquiredCredential>> {
        if matches!(
            self.profile,
            AuthProfile::Anonymous | AuthProfile::Omnigraph
        ) {
            return Ok(None);
        }
        let credential = self.credential.get_or_try_init(|| self.acquire()).await?;
        credential.bearer_for(&self.request, time::OffsetDateTime::now_utc())?;
        Ok(Some(credential.clone()))
    }
    /// The selected credential read from its source again, for a long-lived
    /// client whose server refused the one it holds. The credential this
    /// invocation acquired is unchanged. A stored credential replaced since
    /// the invocation began is of another generation, and refuses.
    pub(crate) async fn reacquire(&self, endpoint: &str) -> Result<Option<String>> {
        if endpoint != self.owner.route() {
            bail!("credential destination differs from the frozen owner");
        }
        if matches!(
            self.profile,
            AuthProfile::Anonymous | AuthProfile::Omnigraph
        ) {
            return Ok(None);
        }
        Ok(Some(
            self.acquire()
                .await?
                .bearer_for(&self.request, time::OffsetDateTime::now_utc())?
                .expose()
                .to_owned(),
        ))
    }
    pub(crate) fn cached_token(&self, endpoint: &str) -> Result<Option<String>> {
        self.cached_credential(endpoint)?
            .map(|credential| {
                Ok(credential
                    .bearer_for(&self.request, time::OffsetDateTime::now_utc())?
                    .expose()
                    .to_owned())
            })
            .transpose()
    }

    pub(crate) fn cached_credential(
        &self,
        endpoint: &str,
    ) -> Result<Option<credentials::AcquiredCredential>> {
        if endpoint != self.owner.route() {
            bail!("credential destination differs from the frozen owner");
        }
        if matches!(self.profile, AuthProfile::Anonymous) {
            return Ok(None);
        }
        let acquired = self
            .credential
            .get()
            .ok_or_else(|| eyre!("selected credential has not been acquired"))?;
        acquired.bearer_for(&self.request, time::OffsetDateTime::now_utc())?;
        Ok(Some(acquired.clone()))
    }
}

tokio::task_local! { pub(crate) static AUTH: Option<std::sync::Arc<Authentication>>; }
tokio::task_local! { pub(crate) static TRANSPORTS: Transports; }

/// Explicit fixture trust for unit tests using the real HTTP clients. Routes
/// are exact (including prefixes); this never installs ambient/global trust.
/// Nested scopes retain their parent's routes for control-to-data journeys.
#[cfg(test)]
pub(crate) fn test_transport_scope<F: std::future::Future>(
    endpoints: &[&str],
    future: F,
) -> impl std::future::Future<Output = F::Output> + use<F> {
    TRANSPORTS.scope(test_transports(endpoints), future)
}

/// Capture a client before concurrent request futures start their deadlines.
#[cfg(test)]
pub(crate) fn test_transport_scope_sync<T>(endpoints: &[&str], setup: impl FnOnce() -> T) -> T {
    TRANSPORTS.sync_scope(test_transports(endpoints), setup)
}

#[cfg(test)]
fn test_transports(endpoints: &[&str]) -> Transports {
    let mut routes = TRANSPORTS
        .try_with(|parent| parent.routes.clone())
        .unwrap_or_default();
    let configuration = schema::Transport {
        tls: Some(schema::Tls {
            ca: Some(schema::Ca {
                mode: schema::CaMode::Replace,
                file: crate::tls_gateway::ca_path().to_owned(),
            }),
        }),
        proxy: schema::Proxy::Direct,
    };
    for endpoint in endpoints {
        let url = url::Url::parse(endpoint).unwrap();
        assert_eq!(url.scheme(), "https", "fixture transport requires TLS");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        let captured = crate::operator_transport::CapturedTransport::capture(
            endpoint,
            Some(&configuration),
            &BTreeMap::new(),
        )
        .unwrap();
        for control in [false, true] {
            routes.insert(
                (control, endpoint.trim_end_matches('/').to_owned()),
                (
                    Some(configuration.clone()),
                    std::sync::OnceLock::from(Ok(captured.clone())),
                ),
            );
        }
    }
    Transports {
        environment: BTreeMap::new(),
        routes,
    }
}

type TransportRoute = (
    Option<schema::Transport>,
    std::sync::OnceLock<std::result::Result<crate::operator_transport::CapturedTransport, String>>,
);

pub(crate) struct Transports {
    environment: BTreeMap<String, std::ffi::OsString>,
    routes: BTreeMap<(bool, String), TransportRoute>,
}

#[cfg(test)]
pub(crate) fn test_broken_control_transport_scope<F: std::future::Future>(
    endpoint: &str,
    missing_ca: std::path::PathBuf,
    proxy_conflict: bool,
    future: F,
) -> impl std::future::Future<Output = F::Output> + use<F> {
    let mut routes = TRANSPORTS
        .try_with(|parent| parent.routes.clone())
        .unwrap_or_default();
    let configuration = if proxy_conflict {
        schema::Transport {
            proxy: schema::Proxy::Environment,
            ..Default::default()
        }
    } else {
        schema::Transport {
            tls: Some(schema::Tls {
                ca: Some(schema::Ca {
                    mode: schema::CaMode::Append,
                    file: missing_ca,
                }),
            }),
            ..Default::default()
        }
    };
    routes.insert(
        (true, endpoint.trim_end_matches('/').to_owned()),
        (Some(configuration), Default::default()),
    );
    TRANSPORTS.scope(
        Transports {
            routes,
            environment: BTreeMap::from([
                (
                    "https_proxy".into(),
                    std::ffi::OsString::from("http://first.example:8080"),
                ),
                (
                    "HTTPS_PROXY".into(),
                    std::ffi::OsString::from("http://second.example:8080"),
                ),
            ]),
        },
        future,
    )
}

fn proxy_environment() -> BTreeMap<String, std::ffi::OsString> {
    [
        "https_proxy",
        "HTTPS_PROXY",
        "http_proxy",
        "no_proxy",
        "NO_PROXY",
    ]
    .into_iter()
    .filter_map(|name| std::env::var_os(name).map(|value| (name.into(), value)))
    .collect()
}

impl Transports {
    pub(crate) fn capture(
        input: &crate::operator::InvocationConfig,
        target: Option<&crate::targets::ResolvedTarget>,
    ) -> Result<Self> {
        let mut routes = BTreeMap::new();
        if let Some(target) = target {
            let config = input.get(&mut std::io::sink())?;
            if let Some(source) = &config.schema {
                if matches!(&target.binding, crate::targets::TargetBinding::Managed(binding) if binding.cluster.is_none())
                {
                    if let Some(owner) = source.profiles.get(target.label()) {
                        routes.insert(
                            (true, owner.api.trim_end_matches('/').to_owned()),
                            (owner.transport.clone(), Default::default()),
                        );
                    }
                    let environment = proxy_environment();
                    return Ok(Self {
                        routes,
                        environment,
                    });
                }
                match source.targets.get(target.label()) {
                    Some(Target::Server {
                        endpoint,
                        transport,
                        ..
                    }) => {
                        routes.insert(
                            (false, endpoint.trim_end_matches('/').to_owned()),
                            (transport.clone(), Default::default()),
                        );
                    }
                    Some(Target::Managed {
                        profile,
                        endpoint,
                        transport,
                        ..
                    }) => {
                        let owner = &source.profiles[profile];
                        routes.insert(
                            (true, owner.api.trim_end_matches('/').to_owned()),
                            (owner.transport.clone(), Default::default()),
                        );
                        if let Some(endpoint) = endpoint {
                            routes.insert(
                                (false, endpoint.trim_end_matches('/').to_owned()),
                                (transport.clone(), Default::default()),
                            );
                        }
                    }
                    _ => {
                        if let Some(owner) = source.profiles.get(target.label()) {
                            routes.insert(
                                (true, owner.api.trim_end_matches('/').to_owned()),
                                (owner.transport.clone(), Default::default()),
                            );
                        }
                    }
                }
            }
        }
        let environment = proxy_environment();
        Ok(Self {
            routes,
            environment,
        })
    }
}

pub(crate) fn transport(
    endpoint: &str,
) -> Result<Option<crate::operator_transport::CapturedTransport>> {
    capture_route(endpoint, false)
}

fn capture_route(
    endpoint: &str,
    control: bool,
) -> Result<Option<crate::operator_transport::CapturedTransport>> {
    TRANSPORTS
        .try_with(|transports| {
            let Some((configuration, captured)) = transports
                .routes
                .get(&(control, endpoint.trim_end_matches('/').to_owned()))
            else {
                return Ok(None);
            };
            captured
                .get_or_init(|| {
                    let mut environment = BTreeMap::new();
                    if configuration.as_ref().is_some_and(|transport| {
                        matches!(transport.proxy, schema::Proxy::Environment)
                    }) {
                        let names: &[&str] = if endpoint.starts_with("https:") {
                            &["https_proxy", "HTTPS_PROXY", "no_proxy", "NO_PROXY"]
                        } else {
                            &["http_proxy", "no_proxy", "NO_PROXY"]
                        };
                        for name in names {
                            if let Some(value) = transports.environment.get(*name) {
                                environment.insert(
                                    (*name).to_owned(),
                                    value
                                        .to_str()
                                        .ok_or_else(|| {
                                            "selected proxy environment must be valid UTF-8"
                                                .to_owned()
                                        })?
                                        .to_owned(),
                                );
                            }
                        }
                    }
                    crate::operator_transport::CapturedTransport::capture(
                        endpoint,
                        configuration.as_ref(),
                        &environment,
                    )
                    .map_err(|error| error.to_string())
                })
                .as_ref()
                .cloned()
                .map(Some)
                .map_err(|error| eyre!("{error}"))
        })
        .unwrap_or(Ok(None))
}

pub(crate) fn http_builder(endpoint: &str) -> Result<reqwest::ClientBuilder> {
    let builder = reqwest::Client::builder()
        .no_proxy()
        .tls_built_in_webpki_certs(false)
        .tls_built_in_native_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never());
    match capture_route(endpoint, true)? {
        Some(transport) => transport.apply(builder),
        None => Ok(builder),
    }
}

pub(crate) fn freeze_transports(
    cli: &crate::cli::Cli,
    target: Option<&crate::targets::ResolvedTarget>,
) -> Result<()> {
    use crate::cli::Command;
    use crate::planes::Requirement;
    use crate::targets::TargetBinding;
    if matches!(&cli.command, Command::Config { .. }) {
        return Ok(());
    }
    if matches!(
        &cli.command,
        Command::Cluster {
            command: crate::cli::ClusterCommand::Token { clear: true, .. }
        }
    ) {
        return Ok(());
    }
    if matches!(&cli.command, Command::Credential { command } if !matches!(command, crate::commands::targets::CredentialCommand::Export))
    {
        return Ok(());
    }
    let Some(target) = target else {
        return Ok(());
    };
    match &target.binding {
        TargetBinding::Managed(binding) => {
            if matches!(&cli.command, Command::Logout { .. }) {
                let _ = capture_route(&binding.api, true);
                return Ok(());
            }
            if matches!(
                crate::planes::requirement(&cli.command),
                Requirement::Graph { .. }
                    | Requirement::Registry
                    | Requirement::Alias
                    | Requirement::Credential
            ) {
                let _ = capture_route(&binding.api, true);
                if let Some(endpoint) = binding
                    .cluster
                    .as_ref()
                    .and_then(|cluster| cluster.endpoint.as_deref())
                {
                    capture_route(endpoint, false)?;
                } else {
                    capture_route(&binding.api, true)?;
                }
            } else {
                capture_route(&binding.api, true)?;
            }
        }
        TargetBinding::Server { endpoint, .. } => {
            capture_route(endpoint, false)?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn namespace(key: &str) -> String {
    AUTH.try_with(|auth| {
        auth.as_ref()
            .map(|auth| format!("profile:{}:{key}", auth.custody))
    })
    .ok()
    .flatten()
    .unwrap_or_else(|| key.to_owned())
}

pub(crate) fn data_namespace(key: &str) -> String {
    AUTH.try_with(|auth| {
        auth.as_ref().map(|auth| {
            format!(
                "owner:{}:profile:{}:binding:{}:generation:{}:credential:{key}",
                auth.ids.owner, auth.ids.profile, auth.custody, auth.ids.epoch
            )
        })
    })
    .ok()
    .flatten()
    .unwrap_or_else(|| key.to_owned())
}

pub(crate) fn data_profile_namespace() -> Option<String> {
    AUTH.try_with(|auth| {
        auth.as_ref()
            .map(|auth| format!("owner:{}:profile:{}", auth.ids.owner, auth.ids.profile))
    })
    .ok()
    .flatten()
}

pub(crate) fn authentication(
    cli: &crate::cli::Cli,
    input: &crate::operator::InvocationConfig,
    target: Option<&crate::targets::ResolvedTarget>,
) -> Result<Option<std::sync::Arc<Authentication>>> {
    let Some(target) = target else {
        return Ok(None);
    };
    if matches!(
        target.binding,
        crate::targets::TargetBinding::DirectGraph { .. }
            | crate::targets::TargetBinding::DirectRoot { .. }
            | crate::targets::TargetBinding::DirectCatalogue
    ) {
        return Ok(None);
    }
    let config = input.get(&mut std::io::sink())?;
    let source = config
        .schema
        .as_ref()
        .ok_or_else(|| eyre!("operator configuration requires version: 1"))?;
    let (kind, name, route, org, auth_name, profile) = match &target.binding {
        crate::targets::TargetBinding::Managed(binding) => {
            let name = if binding.cluster.is_none() {
                cli.profile
                    .as_deref()
                    .or(source.defaults.profile.as_deref())
            } else {
                if cli.profile.is_some() {
                    bail!(
                        "a managed target uses its configured profile; --profile cannot override it"
                    );
                }
                match source.targets.get(target.label()) {
                    Some(Target::Managed { profile, .. }) => Some(profile.as_str()),
                    _ => None,
                }
            }
            .ok_or_else(|| eyre!("select --profile NAME"))?;
            let owner = source
                .profiles
                .get(name)
                .ok_or_else(|| eyre!("unknown profile '{name}'"))?;
            (
                OwnerKind::Profile,
                name,
                &owner.api,
                Some(owner.organization_id.clone()),
                "auth",
                &owner.auth,
            )
        }
        crate::targets::TargetBinding::Server { .. } => match source.targets.get(target.label()) {
            Some(Target::Server { endpoint, auth, .. }) => (
                OwnerKind::Server,
                target.label(),
                endpoint,
                None,
                // Reserved outside the operator-name grammar: a flat target
                // never adopts a legacy named-auth credential by coincidence.
                "@target",
                auth,
            ),
            _ => bail!("served access requires a configured server target"),
        },
        _ => return Ok(None),
    };
    if matches!(profile, AuthProfile::Anonymous) {
        return Ok(None);
    }
    let route = if kind == OwnerKind::Profile {
        route.trim_end_matches('/').to_owned()
    } else {
        route.clone()
    };
    let ids = owner_ids(input, kind, name, auth_name)?;
    let custody = custody(&ids.owner, &ids.profile, &route, profile)?;
    let owner = OwnerBinding::new(
        kind,
        ids.owner.clone(),
        ids.profile.clone(),
        route.clone(),
        org,
    )?;
    let request = CredentialRequest::new(
        owner.clone(),
        if kind == OwnerKind::Profile {
            Purpose::Control
        } else {
            Purpose::Data
        },
        route.clone(),
        None,
        None,
    )?;
    Ok(Some(std::sync::Arc::new(Authentication {
        owner,
        profile: profile.clone(),
        name: auth_name.to_owned(),
        request,
        derived_credentials: Default::default(),
        custody,
        ids,
        credential: tokio::sync::OnceCell::new(),
    })))
}

fn custody(
    identity: &str,
    profile_name: &str,
    route: &str,
    profile: &AuthProfile,
) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(
            identity,
            profile_name,
            route,
            profile
        ))?)
    ))
}

pub(crate) fn registration_auth(
    input: &crate::operator::InvocationConfig,
    name: &str,
    route: &str,
) -> Result<std::sync::Arc<Authentication>> {
    let ids = owner_ids(input, OwnerKind::Profile, name, "auth")?;
    let custody = custody(&ids.owner, &ids.profile, route, &AuthProfile::Omnigraph)?;
    let owner = OwnerBinding::new(
        OwnerKind::Profile,
        ids.owner.clone(),
        ids.profile.clone(),
        route.into(),
        Some("pending".into()),
    )?;
    let request =
        CredentialRequest::new(owner.clone(), Purpose::Control, route.into(), None, None)?;
    Ok(std::sync::Arc::new(Authentication {
        owner,
        request,
        name: "auth".into(),
        derived_credentials: Default::default(),
        profile: AuthProfile::Omnigraph,
        custody,
        ids,
        credential: tokio::sync::OnceCell::new(),
    }))
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerRecord {
    id: String,
    profiles: BTreeMap<String, ProfileRecord>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileRecord {
    id: String,
    epoch: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingState {
    version: u32,
    // Old metadata may remain beside a read-only document. Its captured bytes
    // never become a second writer: later changes must be reconciled explicitly.
    legacy_sha256: Option<String>,
    owners: BTreeMap<String, OwnerRecord>,
}

fn parse_binding_state(text: &str, legacy: Option<&str>) -> Result<BindingState> {
    let state: BindingState = serde_json::from_str(text)?;
    if state.version != 1
        || state.legacy_sha256.as_ref().is_some_and(|digest| {
            digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        bail!("invalid credential binding state");
    }
    if let Some(legacy) = legacy {
        let digest = format!("{:x}", Sha256::digest(legacy.as_bytes()));
        if state.legacy_sha256.as_ref() != Some(&digest) {
            bail!(
                "legacy credential bindings conflict with migrated local state; preserve both files and reconcile them before continuing"
            );
        }
    }
    Ok(state)
}

fn binding_state(config: &Path, path: &Path) -> Result<(Option<String>, BindingState)> {
    let text = crate::operator::read_operator_file(path, "credential binding state")?;
    let legacy = crate::operator::read_operator_file(
        &adjacent(config, ".bindings.json"),
        "legacy credential bindings",
    )?;
    let state = match &text {
        Some(text) => parse_binding_state(text, legacy.as_deref())?,
        None => BindingState {
            version: 1,
            legacy_sha256: legacy
                .as_ref()
                .map(|text| format!("{:x}", Sha256::digest(text.as_bytes()))),
            owners: legacy
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?
                .unwrap_or_default(),
        },
    };
    Ok((text, state))
}

fn lock_state(path: &Path) -> Result<crate::targets::ConfigEditLock> {
    let directory = path
        .parent()
        .ok_or_else(|| eyre!("credential state path has no parent"))?;
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(directory)?;
    let metadata = std::fs::symlink_metadata(directory)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        bail!("credential state requires a private local directory");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
            bail!("credential state directory must be owned by the current user and private");
        }
    }
    crate::targets::ConfigEditLock::acquire(path)
}

pub(crate) fn lock_auth_state(
    input: &crate::operator::InvocationConfig,
) -> Result<crate::targets::ConfigEditLock> {
    lock_state(&input.auth_state_path()?)
}

#[derive(Debug)]
struct BindingIds {
    config_path: std::path::PathBuf,
    owner: String,
    profile: String,
    epoch: u64,
    path: std::path::PathBuf,
    key: String,
}

pub(crate) fn journal_owner() -> Option<String> {
    AUTH.try_with(|auth| {
        auth.as_ref()
            .map(|auth| format!("profile:{}", auth.custody))
    })
    .ok()
    .flatten()
}

fn owner_ids(
    input: &crate::operator::InvocationConfig,
    kind: OwnerKind,
    name: &str,
    profile: &str,
) -> Result<BindingIds> {
    let config_path = input
        .config_path()
        .ok_or_else(|| eyre!("owner binding requires an operator configuration path"))?;
    let path = input.auth_state_path()?;
    let _lock = lock_auth_state(input)?;
    input.check_rename_pending()?;
    let captured = input.get(&mut std::io::sink())?;
    let current = crate::operator::load_operator_config_at(&config_path)?;
    if current.schema != captured.schema {
        bail!("configuration changed after invocation capture; retry with a fresh binding");
    }
    let (text, mut state) = binding_state(&config_path, &path)?;
    let key = format!(
        "{}:{name}",
        if kind == OwnerKind::Profile {
            "profile"
        } else {
            "server"
        }
    );
    let record = state
        .owners
        .entry(key.clone())
        .or_insert_with(|| OwnerRecord {
            id: uuid::Uuid::new_v4().to_string(),
            profiles: BTreeMap::new(),
        });
    let profile = record
        .profiles
        .entry(profile.into())
        .or_insert_with(|| ProfileRecord {
            id: uuid::Uuid::new_v4().to_string(),
            epoch: 0,
        })
        .clone();
    let owner = record.id.clone();
    if uuid::Uuid::parse_str(&owner).is_err() || uuid::Uuid::parse_str(&profile.id).is_err() {
        bail!("invalid owner/profile binding metadata");
    }
    let after = serde_json::to_string(&state)?;
    if text.as_deref() != Some(&after) {
        crate::targets::replace_file(&path, &after)?;
    }
    Ok(BindingIds {
        config_path,
        owner,
        profile: profile.id,
        epoch: profile.epoch,
        path,
        key,
    })
}

pub(crate) enum Rename {
    Profile,
    Target,
}

fn adjacent(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

pub(crate) fn check_rename_pending(path: &Path) -> Result<()> {
    check_journal_pending(&adjacent(path, ".rename.json"))
}

pub(crate) fn check_state_rename_pending(path: &Path) -> Result<()> {
    check_journal_pending(&adjacent(path, ".rename.json"))
}

fn check_journal_pending(path: &Path) -> Result<()> {
    if crate::operator::read_operator_file(path, "rename journal")?.is_some() {
        bail!(
            "configuration rename is incomplete; run config recover-rename with this --operator-config"
        );
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameJournal {
    before_config: String,
    after_config: String,
    before_bindings: Option<String>,
    after_bindings: String,
}

fn move_entry<T>(entries: &mut BTreeMap<String, T>, from: &str, to: &str) -> Result<()> {
    if entries.contains_key(to) {
        bail!("rename destination already exists");
    }
    let value = entries
        .remove(from)
        .ok_or_else(|| eyre!("rename source does not exist"))?;
    entries.insert(to.into(), value);
    Ok(())
}

fn rename_default(selected: &mut Option<String>, from: &str, to: &str) {
    if selected.as_deref() == Some(from) {
        *selected = Some(to.into());
    }
}

pub(crate) fn rename(
    input: &crate::operator::InvocationConfig,
    kind: Rename,
    from: &str,
    to: &str,
) -> Result<()> {
    if !crate::targets::valid_name(from) || !crate::targets::valid_name(to) || from == to {
        bail!("rename requires two distinct valid names");
    }
    let path = input
        .config_path()
        .ok_or_else(|| eyre!("no operator configuration path"))?;
    let _config_lock = crate::targets::ConfigEditLock::acquire(&path)?;
    let _bindings_lock = lock_auth_state(input)?;
    input.check_rename_pending()?;
    let bindings_path = input.auth_state_path()?;
    let before_config = crate::operator::read_operator_file(&path, "operator configuration")?
        .ok_or_else(|| eyre!("operator configuration is missing"))?;
    let mut config = schema::parse(&before_config, path.parent().unwrap())?;
    let (before_bindings, mut state) = binding_state(&path, &bindings_path)?;
    let owner_move = match kind {
        Rename::Profile => {
            move_entry(&mut config.profiles, from, to)?;
            rename_default(&mut config.defaults.profile, from, to);
            for target in config.targets.values_mut() {
                if let Target::Managed { profile, .. } = target {
                    if profile == from {
                        *profile = to.into();
                    }
                }
            }
            Some((format!("profile:{from}"), format!("profile:{to}")))
        }
        Rename::Target => {
            let server = matches!(config.targets.get(from), Some(Target::Server { .. }));
            move_entry(&mut config.targets, from, to)?;
            rename_default(&mut config.defaults.target, from, to);
            for alias in config.aliases.values_mut() {
                if alias.target == from {
                    alias.target = to.into();
                }
            }
            server.then(|| (format!("server:{from}"), format!("server:{to}")))
        }
    };
    if let Some((from, to)) = owner_move {
        if state.owners.contains_key(&to) {
            bail!("destination owner has retained custody; choose another name");
        }
        if state.owners.contains_key(&from) {
            move_entry(&mut state.owners, &from, &to)?;
        }
    }
    let after_config = yaml_serde::to_string(&config)?;
    schema::parse(&after_config, path.parent().unwrap())?;
    let journal = RenameJournal {
        before_config,
        after_config,
        before_bindings,
        after_bindings: serde_json::to_string(&state)?,
    };
    let journal_path = adjacent(&bindings_path, ".rename.json");
    crate::targets::replace_file(&journal_path, &serde_json::to_string(&journal)?)?;
    finish_rename(&path, &bindings_path, &journal_path, &journal, false)
}

fn finish_rename(
    path: &Path,
    bindings_path: &Path,
    journal_path: &Path,
    journal: &RenameJournal,
    legacy: bool,
) -> Result<()> {
    let config = crate::operator::read_operator_file(path, "operator configuration")?;
    let bindings = crate::operator::read_operator_file(bindings_path, "owner/profile bindings")?;
    if ![
        Some(journal.before_config.as_str()),
        Some(journal.after_config.as_str()),
    ]
    .contains(&config.as_deref())
        || ![
            journal.before_bindings.as_deref(),
            Some(journal.after_bindings.as_str()),
        ]
        .contains(&bindings.as_deref())
    {
        bail!(
            "rename recovery conflicts with a subsequent edit; preserved journal requires review"
        );
    }
    schema::parse(&journal.after_config, path.parent().unwrap())?;
    if legacy {
        let _: BTreeMap<String, OwnerRecord> = serde_json::from_str(&journal.after_bindings)?;
    } else {
        let old = crate::operator::read_operator_file(
            &adjacent(path, ".bindings.json"),
            "legacy credential bindings",
        )?;
        parse_binding_state(&journal.after_bindings, old.as_deref())?;
    }
    crate::targets::replace_file(bindings_path, &journal.after_bindings)?;
    crate::targets::replace_file(path, &journal.after_config)?;
    std::fs::remove_file(journal_path)?;
    crate::sync_dir(
        journal_path
            .parent()
            .ok_or_else(|| eyre!("rename journal has no parent"))?,
    )?;
    Ok(())
}

pub(crate) fn recover_rename(input: &crate::operator::InvocationConfig) -> Result<()> {
    let path = input
        .config_path()
        .ok_or_else(|| eyre!("no operator configuration path"))?;
    let _config_lock = crate::targets::ConfigEditLock::acquire(&path)?;
    let _bindings_lock = lock_auth_state(input)?;
    let state_path = input.auth_state_path()?;
    let journal_path = adjacent(&state_path, ".rename.json");
    let text = crate::operator::read_operator_file(&journal_path, "rename journal")?;
    let old_journal_path = adjacent(&path, ".rename.json");
    let old = crate::operator::read_operator_file(&old_journal_path, "legacy rename journal")?;
    match (text, old) {
        (Some(_), Some(_)) => {
            bail!("legacy and current rename journals conflict; preserve both before recovery")
        }
        (Some(text), None) => finish_rename(
            &path,
            &state_path,
            &journal_path,
            &serde_json::from_str(&text)?,
            false,
        ),
        (None, Some(text)) => {
            if crate::operator::read_operator_file(&state_path, "credential binding state")?
                .is_some()
            {
                bail!(
                    "legacy rename conflicts with migrated credential state; preserve both before recovery"
                );
            }
            let journal: RenameJournal = serde_json::from_str(&text)?;
            schema::parse(&journal.after_config, path.parent().unwrap()).map_err(|_| {
                eyre!(
                    "legacy rename journal contains configuration this CLI no longer accepts; run config recover-rename with the previous CLI and the same --operator-config before flattening the YAML; configuration, credential bindings, and journal were preserved"
                )
            })?;
            let old_bindings = adjacent(&path, ".bindings.json");
            let _old_lock = crate::targets::ConfigEditLock::acquire(&old_bindings)?;
            finish_rename(&path, &old_bindings, &old_journal_path, &journal, true)
        }
        (None, None) => bail!("no pending rename"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_credentials_accept_readonly_operator_config_issue_1115() {
        let executable = std::env::current_exe().unwrap();
        for provider in [
            serde_json::json!({"provider":"token","source":{"env":"OMNIGRAPH_READONLY_TOKEN"}}),
            serde_json::json!({"provider":"token","source":{"file":"./token"}}),
            serde_json::json!({"provider":"exec","command":[executable]}),
        ] {
            let configuration = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let path = configuration.path().join("config.yaml");
            let document = serde_json::json!({
                "version":1,
                "targets":{"prod":{
                    "kind":"server",
                    "endpoint":"https://example.test/",
                    "default_graph":"knowledge",
                    "auth":provider
                }}
            });
            std::fs::write(&path, document.to_string()).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
                std::fs::set_permissions(
                    configuration.path(),
                    std::fs::Permissions::from_mode(0o555),
                )
                .unwrap();
            }
            let input = crate::operator::InvocationConfig::at(Some(state.path().to_owned()), None)
                .with_path(Some(path.to_str().unwrap()), configuration.path());
            let cli =
                crate::invocation::parse_from(["omnigraph", "query", "report", "--target", "prod"])
                    .unwrap()
                    .cli;
            let result = (|| -> Result<()> {
                let target = crate::scope::resolve(
                    &cli,
                    &input,
                    configuration.path(),
                    false,
                    &mut std::io::sink(),
                )?;
                let auth = authentication(&cli, &input, target.as_ref())?.unwrap();
                auth.ensure_current()
            })();
            let names = std::fs::read_dir(configuration.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    configuration.path(),
                    std::fs::Permissions::from_mode(0o755),
                )
                .unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            }
            assert!(result.is_ok(), "provider {provider}: {result:?}");
            assert_eq!(names, vec![std::ffi::OsString::from("config.yaml")]);
            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                document.to_string()
            );
        }
    }

    #[test]
    fn legacy_bindings_migrate_once_without_losing_custody_or_generations() {
        let configuration = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let path = configuration.path().join("config.yaml");
        std::fs::write(&path, "version: 1\nprofiles:\n  work:\n    api: https://api.example\n    organization_id: org_test\n    auth: {provider: omnigraph}\ntargets:\n  retained:\n    kind: server\n    endpoint: https://data.example\n    default_graph: knowledge\n    auth: {provider: token, source: {store: {}}}\n").unwrap();
        let owner = uuid::Uuid::new_v4().to_string();
        let profile = uuid::Uuid::new_v4().to_string();
        let server_owner = uuid::Uuid::new_v4().to_string();
        let old_server_profile = uuid::Uuid::new_v4().to_string();
        let legacy = serde_json::json!({
            "profile:work":{"id":owner,"profiles":{"auth":{"id":profile,"epoch":7}}},
            "server:retained":{"id":server_owner,"profiles":{
                "old-name":{"id":old_server_profile,"epoch":2}
            }}
        })
        .to_string();
        let legacy_path = adjacent(&path, ".bindings.json");
        std::fs::write(&legacy_path, &legacy).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for file in [&path, &legacy_path] {
                std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o444)).unwrap();
            }
            std::fs::set_permissions(configuration.path(), std::fs::Permissions::from_mode(0o555))
                .unwrap();
        }
        let input = || {
            crate::operator::InvocationConfig::at(Some(home.path().to_owned()), None)
                .with_path(Some(path.to_str().unwrap()), configuration.path())
        };
        let before = registration_auth(&input(), "work", "https://api.example");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(configuration.path(), std::fs::Permissions::from_mode(0o755))
                .unwrap();
            for file in [&path, &legacy_path] {
                std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o644)).unwrap();
            }
        }
        let before = before.unwrap();
        assert_eq!(before.ids.owner, owner);
        assert_eq!(before.ids.profile, profile);
        assert_eq!(before.ids.epoch, 7);
        assert_eq!(std::fs::read_to_string(&legacy_path).unwrap(), legacy);
        let server_input = input();
        let cli =
            crate::invocation::parse_from(["omnigraph", "query", "report", "--target", "retained"])
                .unwrap()
                .cli;
        let target = crate::scope::resolve(
            &cli,
            &server_input,
            configuration.path(),
            false,
            &mut std::io::sink(),
        )
        .unwrap();
        let server = authentication(&cli, &server_input, target.as_ref())
            .unwrap()
            .unwrap();
        assert_eq!(server.ids.owner, server_owner);
        assert_eq!(server.name, "@target");
        assert_ne!(
            server.ids.profile, old_server_profile,
            "flattening must not adopt a legacy named credential"
        );
        assert_eq!(server.ids.epoch, 0);
        before.invalidate().unwrap();
        assert!(before.ensure_current().is_err());
        let after = registration_auth(&input(), "work", "https://api.example").unwrap();
        assert_eq!(after.owner, before.owner);
        assert_eq!(after.ids.epoch, 8);
        rename(&input(), Rename::Profile, "work", "renamed").unwrap();
        let renamed = registration_auth(&input(), "renamed", "https://api.example").unwrap();
        assert_eq!(renamed.owner, before.owner);
        assert_eq!(renamed.ids.epoch, 8);
        let state_path = input().auth_state_path().unwrap();
        let saved = std::fs::read_to_string(&state_path).unwrap();
        let state: serde_json::Value = serde_json::from_str(&saved).unwrap();
        assert!(state["owners"].get("profile:work").is_none());
        assert_eq!(state["owners"]["profile:renamed"]["id"], owner);
        assert_eq!(
            state["owners"]["server:retained"]["profiles"]["@target"]["id"],
            server.ids.profile
        );
        assert_eq!(
            state["owners"]["server:retained"]["profiles"]["old-name"]["id"],
            old_server_profile
        );
        assert_eq!(
            state["owners"]["server:retained"]["profiles"]["old-name"]["epoch"],
            2
        );
        assert_eq!(std::fs::read_to_string(&legacy_path).unwrap(), legacy);
        // A later old-client write cannot roll generations back or replace the
        // migrated authority, even when it still has well-formed UUIDs.
        let changed = legacy.replace("\"epoch\":7", "\"epoch\":9");
        assert_ne!(changed, legacy);
        std::fs::write(&legacy_path, changed).unwrap();
        assert!(
            registration_auth(&input(), "renamed", "https://api.example")
                .unwrap_err()
                .to_string()
                .contains("conflict")
        );
        assert_eq!(std::fs::read_to_string(&state_path).unwrap(), saved);
    }

    struct InterleavedStore {
        token: std::cell::RefCell<Option<String>>,
        reads: std::cell::Cell<usize>,
    }

    impl credentials::SecretStore for InterleavedStore {
        fn get(&self, _: &OwnerBinding) -> credentials::Result<Option<credentials::BearerSecret>> {
            self.reads.set(self.reads.get() + 1);
            self.token
                .borrow()
                .clone()
                .map(credentials::BearerSecret::new)
                .transpose()
        }
        fn put(
            &self,
            _: &OwnerBinding,
            secret: &credentials::BearerSecret,
        ) -> credentials::Result<()> {
            *self.token.borrow_mut() = Some(secret.expose().to_owned());
            Ok(())
        }
        fn remove(&self, _: &OwnerBinding) -> credentials::Result<()> {
            *self.token.borrow_mut() = None;
            Ok(())
        }
    }

    #[tokio::test]
    async fn stored_acquisition_serializes_with_replacement_and_removal_after_invalidation() {
        use credentials::SecretStore;
        for remove in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            std::fs::write(directory.path().join("config.yaml"), "version: 1\nprofiles:\n  work:\n    api: https://control.example\n    organization_id: org_test\n    auth: {provider: token, source: {store: {}}}\n").unwrap();
            let cli = crate::invocation::parse_from([
                "omnigraph",
                "credentials",
                "status",
                "--profile",
                "work",
            ])
            .unwrap()
            .cli;
            let binding = || {
                let input =
                    crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
                let target = crate::scope::resolve(
                    &cli,
                    &input,
                    directory.path(),
                    true,
                    &mut std::io::sink(),
                )
                .unwrap();
                authentication(&cli, &input, target.as_ref())
                    .unwrap()
                    .unwrap()
            };
            let before = binding();
            let store = InterleavedStore {
                token: std::cell::RefCell::new(Some("old-token".into())),
                reads: std::cell::Cell::new(0),
            };
            let lock = tokio::sync::Mutex::new(());
            let editing = lock.lock().await;
            let stale_acquisition =
                before.acquire_stored_with(&store, async { Ok(lock.lock().await) });
            tokio::pin!(stale_acquisition);
            assert!(futures::poll!(&mut stale_acquisition).is_pending());
            before.invalidate().unwrap();
            let during = binding();
            let acquisition = during.acquire_stored_with(&store, async { Ok(lock.lock().await) });
            tokio::pin!(acquisition);
            assert!(futures::poll!(&mut acquisition).is_pending());
            assert_eq!(store.reads.get(), 0);
            if remove {
                store.remove(&before.owner).unwrap();
            } else {
                store
                    .put(
                        &before.owner,
                        &credentials::BearerSecret::new("replacement-token".into()).unwrap(),
                    )
                    .unwrap();
            }
            drop(editing);
            assert!(stale_acquisition.await.is_err());
            assert_eq!(store.reads.get(), 0);
            let acquired = acquisition.await;
            if remove {
                assert!(acquired.is_err());
            } else {
                let acquired = acquired.unwrap();
                assert_eq!(
                    acquired
                        .bearer_for(&during.request, time::OffsetDateTime::now_utc())
                        .unwrap()
                        .expose(),
                    "replacement-token"
                );
            }
            assert_eq!(store.reads.get(), 1);
            assert!(
                before
                    .acquire_stored_with(&store, async { Ok(lock.lock().await) })
                    .await
                    .is_err()
            );
            assert_eq!(store.reads.get(), 1);
            during.invalidate().unwrap();
            assert!(during.ensure_current().is_err());
        }
    }

    #[tokio::test]
    async fn canonical_basepath_preserves_independent_control_and_data_transports() {
        let route = "https://example.test/service";
        let configured = schema::Transport {
            proxy: schema::Proxy::Url {
                url: "http://proxy.test:8080".into(),
            },
            ..Default::default()
        };
        let transports = Transports {
            environment: BTreeMap::new(),
            routes: BTreeMap::from([
                ((true, route.into()), (Some(configured), Default::default())),
                ((false, route.into()), (None, Default::default())),
            ]),
        };
        TRANSPORTS
            .scope(transports, async {
                assert_eq!(
                    capture_route(&format!("{route}/"), true)
                        .unwrap()
                        .unwrap()
                        .proxy_url(),
                    Some("http://proxy.test:8080/")
                );
                assert!(transport(route).unwrap().unwrap().proxy_url().is_none());
                assert!(transport(&format!("{route}/sibling")).unwrap().is_none());
            })
            .await;

        // Fixture trust has the same exact-route boundary and ends with its
        // task-local scope; nested control/data setup must retain both routes.
        let fixture = crate::managed_http_fixture::IntentApiFixture::new(vec![]);
        let prefix = format!("{}/service", fixture.origin);
        assert!(transport(&fixture.origin).unwrap().is_none());
        test_transport_scope(&[&fixture.origin], async {
            assert!(transport(&fixture.origin).unwrap().is_some());
            assert!(capture_route(&fixture.origin, true).unwrap().is_some());
            assert!(transport(&prefix).unwrap().is_none());
            test_transport_scope(&[&prefix], async {
                assert!(transport(&fixture.origin).unwrap().is_some());
                assert!(transport(&prefix).unwrap().is_some());
                assert!(transport(&format!("{prefix}/sibling")).unwrap().is_none());
            })
            .await;
            assert!(transport(&prefix).unwrap().is_none());
        })
        .await;
        assert!(transport(&fixture.origin).unwrap().is_none());
        assert!(fixture.requests().is_empty());
        fixture.assert_complete();
    }

    #[tokio::test]
    async fn unused_data_ca_does_not_block_control_and_selected_failure_is_frozen() {
        let directory = tempfile::tempdir().unwrap();
        let ca = directory.path().join("missing.pem");
        let data = schema::Transport {
            tls: Some(schema::Tls {
                ca: Some(schema::Ca {
                    mode: schema::CaMode::Append,
                    file: ca.clone(),
                }),
            }),
            ..Default::default()
        };
        let transports = Transports {
            environment: BTreeMap::new(),
            routes: BTreeMap::from([
                (
                    (true, "https://api.test/prefix".into()),
                    (None, Default::default()),
                ),
                (
                    (false, "https://data.test/prefix".into()),
                    (Some(data), Default::default()),
                ),
            ]),
        };
        TRANSPORTS
            .scope(transports, async {
                assert!(capture_route("https://api.test/prefix/", true).is_ok());
                let failure = transport("https://data.test/prefix/")
                    .unwrap_err()
                    .to_string();
                std::fs::write(&ca, "invalid certificate").unwrap();
                assert_eq!(
                    transport("https://data.test/prefix")
                        .unwrap_err()
                        .to_string(),
                    failure
                );
            })
            .await;
    }

    #[test]
    fn interrupted_rename_refuses_normal_use_and_recovers_both_files() {
        for legacy in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("config.yaml");
            let input =
                crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
            let bindings_path = if legacy {
                adjacent(&path, ".bindings.json")
            } else {
                input.auth_state_path().unwrap()
            };
            let journal_path = if legacy {
                adjacent(&path, ".rename.json")
            } else {
                adjacent(&bindings_path, ".rename.json")
            };
            if !legacy {
                drop(lock_auth_state(&input).unwrap());
            }
            let journal = RenameJournal {
                before_config: "version: 1\n".into(),
                after_config: "version: 1\ndefaults: {output: json}\n".into(),
                before_bindings: None,
                after_bindings: if legacy {
                    "{}".into()
                } else {
                    serde_json::to_string(&BindingState {
                        version: 1,
                        legacy_sha256: None,
                        owners: BTreeMap::new(),
                    })
                    .unwrap()
                },
            };
            std::fs::write(&path, &journal.before_config).unwrap();
            std::fs::write(&journal_path, serde_json::to_string(&journal).unwrap()).unwrap();
            std::fs::write(&bindings_path, &journal.after_bindings).unwrap();
            assert!(input.get(&mut std::io::sink()).is_err());
            recover_rename(&input).unwrap();
            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                journal.after_config
            );
            let input =
                crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
            assert!(input.get(&mut std::io::sink()).is_ok());
            std::fs::write(&journal_path, serde_json::to_string(&journal).unwrap()).unwrap();
            std::fs::write(&path, "version: 1\ndefaults: {output: csv}\n").unwrap();
            assert!(recover_rename(&input).is_err());
            assert!(journal_path.exists());
        }

        // A real old-format server journal must be finished by the previous
        // CLI before its named authentication map is manually flattened.
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.yaml");
        let bindings_path = adjacent(&path, ".bindings.json");
        let journal_path = adjacent(&path, ".rename.json");
        let document = |name: &str| {
            serde_json::json!({
                "version": 1,
                "targets": {(name): {
                    "kind": "server", "endpoint": "https://data.example",
                    "default_graph": "knowledge", "default_auth": "deploy",
                    "auth": {"deploy": {"provider": "token", "source": {"store": {}}}}
                }}
            })
            .to_string()
        };
        let owner = uuid::Uuid::new_v4().to_string();
        let profile = uuid::Uuid::new_v4().to_string();
        let bindings = |name: &str| {
            serde_json::json!({
                (format!("server:{name}")): {"id": owner, "profiles": {
                    "deploy": {"id": profile, "epoch": 3}
                }}
            })
            .to_string()
        };
        let journal = RenameJournal {
            before_config: document("old"),
            after_config: document("renamed"),
            before_bindings: Some(bindings("old")),
            after_bindings: bindings("renamed"),
        };
        let journal_text = serde_json::to_string(&journal).unwrap();
        std::fs::write(&path, &journal.before_config).unwrap();
        std::fs::write(&bindings_path, &journal.after_bindings).unwrap();
        std::fs::write(&journal_path, &journal_text).unwrap();
        let input = crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
        let error = recover_rename(&input).unwrap_err().to_string();
        assert!(error.contains("previous CLI"), "{error}");
        assert!(error.contains("before flattening"), "{error}");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            journal.before_config
        );
        assert_eq!(
            std::fs::read_to_string(&bindings_path).unwrap(),
            journal.after_bindings
        );
        assert_eq!(
            std::fs::read_to_string(&journal_path).unwrap(),
            journal_text
        );
        assert!(!input.auth_state_path().unwrap().exists());
    }
}
