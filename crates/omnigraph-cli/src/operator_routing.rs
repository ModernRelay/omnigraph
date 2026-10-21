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
                default_auth,
                default_graph,
                ..
            } => {
                entry.kind = TargetKind::Server;
                entry.endpoint = Some(endpoint.clone());
                entry.default_graph = default_graph.clone();
                let anonymous = choose(auth, default_auth.as_deref(), None)
                    .is_ok_and(|(_, p)| matches!(p, AuthProfile::Anonymous));
                if anonymous {
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

pub(crate) fn choose<'a>(
    profiles: &'a BTreeMap<String, AuthProfile>,
    default: Option<&'a str>,
    explicit: Option<&'a str>,
) -> Result<(&'a str, &'a AuthProfile)> {
    let name = explicit.or(default).ok_or_else(|| {
        eyre!("select --auth PROFILE or configure default_auth on the selected owner")
    })?;
    Ok((
        name,
        profiles
            .get(name)
            .ok_or_else(|| eyre!("unknown auth profile '{name}' on the selected owner"))?,
    ))
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
        let _config_lock = crate::targets::ConfigEditLock::acquire(&self.ids.config_path)?;
        check_rename_pending(&self.ids.config_path)?;
        let _lock = crate::targets::ConfigEditLock::acquire(&self.ids.path)?;
        let text =
            crate::operator::read_operator_file(&self.ids.path, "owner/profile binding metadata")?
                .ok_or_else(|| eyre!("profile binding disappeared"))?;
        let records: BTreeMap<String, OwnerRecord> = serde_json::from_str(&text)?;
        let current = records
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
        let _config_lock = crate::targets::ConfigEditLock::acquire(&self.ids.config_path)?;
        check_rename_pending(&self.ids.config_path)?;
        let _lock = crate::targets::ConfigEditLock::acquire(&self.ids.path)?;
        let text =
            crate::operator::read_operator_file(&self.ids.path, "owner/profile binding metadata")?
                .ok_or_else(|| eyre!("owner/profile binding metadata disappeared"))?;
        let mut records: BTreeMap<String, OwnerRecord> = serde_json::from_str(&text)?;
        let profile = records
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
        crate::targets::replace_file(&self.ids.path, &serde_json::to_string(&records)?)
    }
    pub(crate) async fn token(&self) -> Result<Option<String>> {
        if matches!(
            self.profile,
            AuthProfile::Anonymous | AuthProfile::Omnigraph
        ) {
            return Ok(None);
        }
        let credential = self
            .credential
            .get_or_try_init(|| async {
                Ok::<_, color_eyre::Report>(match &self.profile {
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
            })
            .await?;
        Ok(Some(
            credential
                .bearer_for(&self.request, time::OffsetDateTime::now_utc())?
                .expose()
                .to_owned(),
        ))
    }
    pub(crate) fn cached_token(&self, endpoint: &str) -> Result<Option<String>> {
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
        Ok(Some(
            acquired
                .bearer_for(&self.request, time::OffsetDateTime::now_utc())?
                .expose()
                .to_owned(),
        ))
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
        if cli.auth.is_some() {
            bail!("direct targets do not accept --auth");
        }
        return Ok(None);
    }
    let config = input.get(&mut std::io::sink())?;
    let source = config
        .schema
        .as_ref()
        .ok_or_else(|| eyre!("operator configuration requires version: 1"))?;
    let (kind, name, route, org, auth_name, profile) = match &target.binding {
        crate::targets::TargetBinding::Managed(binding) => {
            if cli.auth.is_some() {
                bail!(
                    "managed profiles own one authentication configuration; select --profile instead of --auth"
                );
            }
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
            Some(Target::Server {
                endpoint,
                auth,
                default_auth,
                ..
            }) => {
                let (auth_name, profile) =
                    choose(auth, default_auth.as_deref(), cli.auth.as_deref())?;
                (
                    OwnerKind::Server,
                    target.label(),
                    endpoint,
                    None,
                    auth_name,
                    profile,
                )
            }
            _ => bail!("served access requires a configured server target"),
        },
        _ => {
            if cli.auth.is_some() {
                bail!("direct targets do not accept --auth");
            }
            return Ok(None);
        }
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
    let _config_lock = crate::targets::ConfigEditLock::acquire(&config_path)?;
    check_rename_pending(&config_path)?;
    let captured = input.get(&mut std::io::sink())?;
    let current = crate::operator::load_operator_config_at(&config_path)?;
    if current.schema != captured.schema {
        bail!("configuration changed after invocation capture; retry with a fresh binding");
    }
    let mut path = input
        .config_path()
        .ok_or_else(|| eyre!("owner binding requires an operator configuration path"))?
        .into_os_string();
    path.push(".bindings.json");
    let path = std::path::PathBuf::from(path);
    let _lock = crate::targets::ConfigEditLock::acquire(&path)?;
    let text = crate::operator::read_operator_file(&path, "owner/profile binding metadata")?;
    let mut records: BTreeMap<String, OwnerRecord> = match &text {
        Some(text) => serde_json::from_str(text)?,
        None => BTreeMap::new(),
    };
    let key = format!(
        "{}:{name}",
        if kind == OwnerKind::Profile {
            "profile"
        } else {
            "server"
        }
    );
    let record = records.entry(key.clone()).or_insert_with(|| OwnerRecord {
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
    let after = serde_json::to_string(&records)?;
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
    Auth { owner: String },
}

fn adjacent(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

pub(crate) fn check_rename_pending(path: &Path) -> Result<()> {
    if crate::operator::read_operator_file(&adjacent(path, ".rename.json"), "rename journal")?
        .is_some()
    {
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
    check_rename_pending(&path)?;
    let bindings_path = adjacent(&path, ".bindings.json");
    let _bindings_lock = crate::targets::ConfigEditLock::acquire(&bindings_path)?;
    let before_config = crate::operator::read_operator_file(&path, "operator configuration")?
        .ok_or_else(|| eyre!("operator configuration is missing"))?;
    let mut config = schema::parse(&before_config, path.parent().unwrap())?;
    let before_bindings =
        crate::operator::read_operator_file(&bindings_path, "owner/profile bindings")?;
    let mut records: BTreeMap<String, OwnerRecord> = before_bindings
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or_default();
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
        Rename::Auth { owner } => {
            let (profiles, default) = match config.targets.get_mut(&owner) {
                Some(Target::Server {
                    auth, default_auth, ..
                }) => (auth, default_auth),
                _ => bail!("auth rename requires a standalone server owner"),
            };
            move_entry(profiles, from, to)?;
            rename_default(default, from, to);
            let key = format!("server:{owner}");
            if let Some(record) = records.get_mut(&key) {
                if record.profiles.contains_key(to) {
                    bail!("destination profile has retained custody; choose another name");
                }
                if record.profiles.contains_key(from) {
                    move_entry(&mut record.profiles, from, to)?;
                }
            }
            None
        }
    };
    if let Some((from, to)) = owner_move {
        if records.contains_key(&to) {
            bail!("destination owner has retained custody; choose another name");
        }
        if records.contains_key(&from) {
            move_entry(&mut records, &from, &to)?;
        }
    }
    let after_config = yaml_serde::to_string(&config)?;
    schema::parse(&after_config, path.parent().unwrap())?;
    let journal = RenameJournal {
        before_config,
        after_config,
        before_bindings,
        after_bindings: serde_json::to_string(&records)?,
    };
    crate::targets::replace_file(
        &adjacent(&path, ".rename.json"),
        &serde_json::to_string(&journal)?,
    )?;
    finish_rename(&path, &journal)
}

fn finish_rename(path: &Path, journal: &RenameJournal) -> Result<()> {
    let config = crate::operator::read_operator_file(path, "operator configuration")?;
    let bindings_path = adjacent(path, ".bindings.json");
    let bindings = crate::operator::read_operator_file(&bindings_path, "owner/profile bindings")?;
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
    let _: BTreeMap<String, OwnerRecord> = serde_json::from_str(&journal.after_bindings)?;
    crate::targets::replace_file(&bindings_path, &journal.after_bindings)?;
    crate::targets::replace_file(path, &journal.after_config)?;
    std::fs::remove_file(adjacent(path, ".rename.json"))?;
    Ok(())
}

pub(crate) fn recover_rename(input: &crate::operator::InvocationConfig) -> Result<()> {
    let path = input
        .config_path()
        .ok_or_else(|| eyre!("no operator configuration path"))?;
    let _config_lock = crate::targets::ConfigEditLock::acquire(&path)?;
    let _bindings_lock =
        crate::targets::ConfigEditLock::acquire(&adjacent(&path, ".bindings.json"))?;
    let text =
        crate::operator::read_operator_file(&adjacent(&path, ".rename.json"), "rename journal")?
            .ok_or_else(|| eyre!("no pending rename"))?;
    finish_rename(&path, &serde_json::from_str(&text)?)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.yaml");
        let journal = RenameJournal {
            before_config: "version: 1\n".into(),
            after_config: "version: 1\ndefaults: {output: json}\n".into(),
            before_bindings: None,
            after_bindings: "{}".into(),
        };
        std::fs::write(&path, &journal.before_config).unwrap();
        std::fs::write(
            adjacent(&path, ".rename.json"),
            serde_json::to_string(&journal).unwrap(),
        )
        .unwrap();
        std::fs::write(adjacent(&path, ".bindings.json"), &journal.after_bindings).unwrap();
        assert!(crate::operator::load_operator_config_at(&path).is_err());
        let input = crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
        recover_rename(&input).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            journal.after_config
        );
        assert!(crate::operator::load_operator_config_at(&path).is_ok());
        std::fs::write(
            adjacent(&path, ".rename.json"),
            serde_json::to_string(&journal).unwrap(),
        )
        .unwrap();
        std::fs::write(&path, "version: 1\ndefaults: {output: csv}\n").unwrap();
        assert!(recover_rename(&input).is_err());
        assert!(adjacent(&path, ".rename.json").exists());
    }
}
