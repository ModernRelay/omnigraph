//! Complete target bindings and conservative operator-file administration.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::graph_http::RequestTarget;
use crate::operator::{OperatorConfig, token_env_name};

#[path = "target_import.rs"]
pub(crate) mod import;

pub(crate) const TARGET_CONFIG_VERSION: u32 = 1;
pub(crate) const TARGET_PROTOCOL: u32 = 1;
pub(crate) const CREDENTIAL_PROTOCOL: u32 = 1;
pub(crate) const ACCESS_PROTOCOL: u32 = 1;

#[derive(Debug)]
pub(crate) struct TargetError {
    pub(crate) code: &'static str,
    message: String,
}
impl std::fmt::Display for TargetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for TargetError {}
pub(crate) fn refusal(code: &'static str, message: impl Into<String>) -> color_eyre::Report {
    TargetError {
        code,
        message: message.into(),
    }
    .into()
}
fn invalid(name: &str, why: impl std::fmt::Display) -> color_eyre::Report {
    refusal(
        "target_invalid",
        format!("target '{name}' is not a complete binding: {why}"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetKind {
    Direct,
    Server,
    Managed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetScope {
    Graph,
    Root,
    Catalogue,
    Account,
    Cluster,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetAuth {
    Anonymous,
}

/// Destination options only. Authentication continues through provider chains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StorageProvider {
    Local,
    S3 {
        region: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        virtual_hosted_style: bool,
    },
    Azure {
        account_name: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        emulator: bool,
    },
}

impl StorageProvider {
    pub(crate) fn validate(&self, uri: &str) -> Result<()> {
        match (self, uri.split_once("://").map(|(scheme, _)| scheme)) {
            (Self::Local, None | Some("file")) => Ok(()),
            (
                Self::S3 {
                    region, endpoint, ..
                },
                Some("s3"),
            ) => {
                if region.is_empty()
                    || !region
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                {
                    bail!("S3 provider requires a nonempty region");
                }
                if let Some(endpoint) = endpoint {
                    validate_http_endpoint(endpoint)?;
                }
                Ok(())
            }
            (
                Self::Azure {
                    account_name,
                    endpoint,
                    emulator,
                },
                Some("az"),
            ) => {
                if account_name.is_empty()
                    || !account_name
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                {
                    bail!("Azure provider requires a lowercase account_name");
                }
                if let Some(endpoint) = endpoint {
                    validate_http_endpoint(endpoint)?;
                    if !emulator && !endpoint.starts_with("https://") {
                        bail!("Azure provider endpoint requires HTTPS outside emulator mode");
                    }
                }
                Ok(())
            }
            _ => bail!("provider kind does not match the storage URI"),
        }
    }
    /// Inline cloud addresses deliberately capture ambient destination settings
    /// once. Registered targets never call this function.
    pub(crate) fn capture(uri: &str) -> Result<Self> {
        Self::capture_from(uri, &std::env::vars().collect())
    }

    fn capture_from(uri: &str, environment: &BTreeMap<String, String>) -> Result<Self> {
        let nonempty = |names: &[&str]| {
            names.iter().find_map(|name| {
                environment
                    .get(*name)
                    .filter(|v| !v.trim().is_empty())
                    .cloned()
            })
        };
        let aliases = |names: &[&str]| -> Result<Option<String>> {
            let mut selected: Option<String> = None;
            for name in names {
                if let Some(value) = environment.get(*name).filter(|v| !v.trim().is_empty()) {
                    if selected.as_ref().is_some_and(|old| old != value) {
                        bail!("conflicting destination aliases: {}", names.join(", "));
                    }
                    selected = Some(value.clone());
                }
            }
            Ok(selected)
        };
        let boolean = |names: &[&str]| -> Result<bool> {
            match aliases(names)?
                .as_deref()
                .map(str::to_ascii_lowercase)
                .as_deref()
            {
                None | Some("false" | "0") => Ok(false),
                Some("true" | "1") => Ok(true),
                Some(_) => bail!("{} must be true or false", names[0]),
            }
        };
        let refuse_aliases = |names: &[&str]| -> Result<()> {
            if let Some(name) = names.iter().find(|name| environment.contains_key(**name)) {
                bail!(
                    "unprefixed destination alias {name} is unsupported; use the documented provider-prefixed setting"
                );
            }
            Ok(())
        };
        let provider = if uri.starts_with("s3://") {
            refuse_aliases(&[
                "REGION",
                "DEFAULT_REGION",
                "ENDPOINT",
                "ENDPOINT_URL",
                "VIRTUAL_HOSTED_STYLE_REQUEST",
                "S3_EXPRESS",
                "BUCKET",
                "BUCKET_NAME",
            ])?;
            if boolean(&["AWS_S3_EXPRESS"])? {
                bail!("S3 Express destination selection is not supported by target bindings");
            }
            let endpoint = nonempty(&["AWS_ENDPOINT_URL_S3"])
                .or(aliases(&["AWS_ENDPOINT_URL", "AWS_ENDPOINT"])?);
            if nonempty(&["AWS_PROFILE"]).is_some() && endpoint.is_none() {
                bail!(
                    "inline S3 with AWS_PROFILE needs an explicit AWS_ENDPOINT_URL_S3 so its endpoint can be captured; alternatively register a target with provider settings"
                );
            }
            Self::S3 {
                region: nonempty(&["AWS_REGION", "AWS_DEFAULT_REGION"]).ok_or_else(|| {
                    eyre!("inline S3 target requires AWS_REGION or AWS_DEFAULT_REGION")
                })?,
                endpoint,
                virtual_hosted_style: if boolean(&["AWS_S3_FORCE_PATH_STYLE"])? {
                    false
                } else {
                    boolean(&["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"])?
                },
            }
        } else if uri.starts_with("az://") {
            refuse_aliases(&[
                "ACCOUNT_NAME",
                "ENDPOINT",
                "USE_EMULATOR",
                "USE_FABRIC_ENDPOINT",
                "CONTAINER_NAME",
            ])?;
            if boolean(&["AZURE_USE_FABRIC_ENDPOINT"])? {
                bail!("Azure Fabric destination selection is not supported by target bindings");
            }
            let emulator = boolean(&["AZURE_STORAGE_USE_EMULATOR"])?;
            let endpoint = aliases(&["AZURE_STORAGE_ENDPOINT", "AZURE_ENDPOINT"])?;
            if emulator && endpoint.is_some() {
                bail!(
                    "Azure emulator mode conflicts with an explicit Azure endpoint; use AZURITE_BLOB_STORAGE_URL"
                );
            }
            Self::Azure {
                account_name: nonempty(&["AZURE_STORAGE_ACCOUNT_NAME"]).ok_or_else(|| {
                    eyre!("inline Azure target requires AZURE_STORAGE_ACCOUNT_NAME")
                })?,
                endpoint: if emulator {
                    nonempty(&["AZURITE_BLOB_STORAGE_URL"])
                        .or_else(|| Some("http://127.0.0.1:10000".into()))
                } else {
                    endpoint
                },
                emulator,
            }
        } else {
            Self::Local
        };
        provider.validate(uri)?;
        Ok(provider)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetEntry {
    pub(crate) kind: TargetKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<TargetScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider: Option<StorageProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) credential: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) auth: Option<TargetAuth>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) default_graph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) api: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) organization_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cluster_incarnation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) control_auth: Option<crate::managed::ControlAuth>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetBinding {
    DirectGraph {
        uri: String,
        provider: StorageProvider,
    },
    DirectRoot {
        uri: String,
        provider: StorageProvider,
        default_graph: Option<String>,
    },
    DirectCatalogue,
    Server {
        endpoint: String,
        auth: ServerAuth,
        default_graph: Option<String>,
    },
    Managed(crate::managed::Binding),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerAuth {
    Token { owner: String },
    Anonymous,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetProvenance {
    Explicit { name: String },
    Default { name: String },
    Inline { address: String },
}
#[derive(Debug, Clone)]
pub(crate) struct ResolvedTarget {
    pub(crate) binding: TargetBinding,
    pub(crate) provenance: TargetProvenance,
    storage: std::sync::OnceLock<
        std::result::Result<omnigraph::storage::CapturedStorageBinding, String>,
    >,
}
impl ResolvedTarget {
    pub(crate) fn new(binding: TargetBinding, provenance: TargetProvenance) -> Self {
        Self {
            binding,
            provenance,
            storage: std::sync::OnceLock::new(),
        }
    }
    pub(crate) fn storage(&self) -> Result<omnigraph::storage::CapturedStorageBinding> {
        self.storage
            .get_or_init(|| self.capture_storage().map_err(|error| error.to_string()))
            .as_ref()
            .cloned()
            .map_err(|message| refusal("target_invalid", message))
    }
    fn capture_storage(&self) -> Result<omnigraph::storage::CapturedStorageBinding> {
        use omnigraph::storage::{CapturedStorageBinding, StorageDestination};
        let (uri, provider) = match &self.binding {
            TargetBinding::DirectGraph { uri, provider }
            | TargetBinding::DirectRoot { uri, provider, .. } => (uri, provider),
            _ => bail!("this operation requires direct storage"),
        };
        let destination = match provider {
            StorageProvider::Local => StorageDestination::Local,
            StorageProvider::S3 {
                region,
                endpoint,
                virtual_hosted_style,
            } => StorageDestination::S3 {
                region: region.clone(),
                endpoint: endpoint.clone(),
                virtual_hosted_style: *virtual_hosted_style,
            },
            StorageProvider::Azure {
                account_name,
                endpoint,
                emulator,
            } => StorageDestination::Azure {
                account_name: account_name.clone(),
                endpoint: endpoint.clone(),
                emulator: *emulator,
            },
        };
        Ok(CapturedStorageBinding::capture(uri, &destination)?)
    }
    pub(crate) fn is_explicit(&self) -> bool {
        !matches!(self.provenance, TargetProvenance::Default { .. })
    }
    pub(crate) fn label(&self) -> &str {
        match &self.provenance {
            TargetProvenance::Explicit { name } | TargetProvenance::Default { name } => name,
            TargetProvenance::Inline { address } => address,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialOwner<'a> {
    Keyed(&'a str),
    Anonymous { target: &'a str },
}

pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
        && name != "."
        && name != ".."
        && name != "og_catalog"
}

pub(crate) fn validate_http_endpoint(endpoint: &str) -> Result<()> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| eyre!("endpoint is not a valid URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none_or(str::is_empty)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("HTTP endpoint requires http(s), a host, and no userinfo, query or fragment");
    }
    Ok(())
}
pub(crate) fn validate_server_endpoint(endpoint: &str) -> Result<()> {
    if crate::graph_http::server_scheme(endpoint) == Some("unix") {
        crate::graph_http::ServerTransport::parse(endpoint)?;
        let url = reqwest::Url::parse(endpoint)?;
        if url.query().is_some() || url.fragment().is_some() {
            bail!("Unix endpoint takes no query or fragment");
        }
        Ok(())
    } else {
        validate_http_endpoint(endpoint)
    }
}

pub(crate) fn normalize_storage_uri(uri: &str, base_dir: &Path) -> Result<String> {
    if uri.is_empty() {
        bail!("storage URI must not be empty");
    }
    if let Some((scheme, _)) = uri.split_once("://") {
        if !matches!(scheme, "file" | "s3" | "az") {
            bail!("unsupported storage URI scheme; use a path, file://, s3:// or az://");
        }
        let url = reqwest::Url::parse(uri).map_err(|_| eyre!("invalid storage URI"))?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!("storage URI carries no userinfo, query or fragment");
        }
        if scheme != "file" && url.host_str().is_none_or(str::is_empty) {
            bail!("cloud storage URI requires a bucket or container");
        }
        return omnigraph::storage::normalize_root_uri(uri).map_err(Into::into);
    }
    if let Some((prefix, suffix)) = uri.split_once(':') {
        let windows_drive = cfg!(windows)
            && prefix.len() == 1
            && (suffix.starts_with('/') || suffix.starts_with('\\'));
        let looks_like_scheme = prefix.starts_with(|c: char| c.is_ascii_alphabetic())
            && prefix
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'+' | b'-' | b'.'));
        if looks_like_scheme && !windows_drive {
            bail!(
                "malformed storage URI; use file://, s3:// or az://, or prefix a literal path containing ':' with './'"
            );
        }
    }
    let path = crate::operator::expand_tilde(uri);
    let path = if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    };
    let path = std::path::absolute(path)?;
    omnigraph::storage::normalize_root_uri(&path.to_string_lossy()).map_err(Into::into)
}

impl TargetEntry {
    #[cfg(test)]
    pub(crate) fn binding(&self, name: &str) -> Result<TargetBinding> {
        self.binding_at(name, Path::new("."))
    }
    pub(crate) fn binding_at(&self, name: &str, base_dir: &Path) -> Result<TargetBinding> {
        let forbid = |field: &str, present: bool| {
            if present {
                Err(invalid(
                    name,
                    format!("`{field}` does not apply to this kind/scope"),
                ))
            } else {
                Ok(())
            }
        };
        if let Some(graph) = &self.default_graph {
            crate::helpers::require_graph_segment(graph).map_err(|e| invalid(name, e))?;
        }
        if self.kind != TargetKind::Managed {
            forbid("api", self.api.is_some())?;
            forbid("organization_id", self.organization_id.is_some())?;
            forbid("cluster_id", self.cluster_id.is_some())?;
            forbid("cluster_incarnation", self.cluster_incarnation.is_some())?;
            forbid("control_auth", self.control_auth.is_some())?;
        }
        match self.kind {
            TargetKind::Direct => {
                forbid("endpoint", self.endpoint.is_some())?;
                forbid("credential", self.credential.is_some())?;
                forbid("auth", self.auth.is_some())?;
                if self.scope == Some(TargetScope::Catalogue) {
                    forbid("uri", self.uri.is_some())?;
                    forbid("provider", self.provider.is_some())?;
                    forbid("default_graph", self.default_graph.is_some())?;
                    return Ok(TargetBinding::DirectCatalogue);
                }
                let raw = self
                    .uri
                    .as_deref()
                    .ok_or_else(|| invalid(name, "direct targets require uri"))?;
                let uri = normalize_storage_uri(raw, base_dir).map_err(|e| invalid(name, e))?;
                let provider = self.provider.clone().unwrap_or(StorageProvider::Local);
                provider.validate(&uri).map_err(|e| invalid(name, e))?;
                match self.scope {
                    Some(TargetScope::Graph) => {
                        forbid("default_graph", self.default_graph.is_some())?;
                        Ok(TargetBinding::DirectGraph { uri, provider })
                    }
                    Some(TargetScope::Root) => Ok(TargetBinding::DirectRoot {
                        uri,
                        provider,
                        default_graph: self.default_graph.clone(),
                    }),
                    _ => Err(invalid(
                        name,
                        "direct targets require scope graph, root or catalogue",
                    )),
                }
            }
            TargetKind::Server => {
                forbid("scope", self.scope.is_some())?;
                forbid("uri", self.uri.is_some())?;
                forbid("provider", self.provider.is_some())?;
                let endpoint = self
                    .endpoint
                    .clone()
                    .ok_or_else(|| invalid(name, "server targets require endpoint"))?;
                validate_server_endpoint(&endpoint).map_err(|e| invalid(name, e))?;
                let auth = match (&self.credential, self.auth) {
                    (Some(owner), None) if valid_name(owner) => ServerAuth::Token {
                        owner: owner.clone(),
                    },
                    (None, Some(TargetAuth::Anonymous)) => ServerAuth::Anonymous,
                    _ => {
                        return Err(invalid(
                            name,
                            "server target requires exactly one credential owner or auth: anonymous",
                        ));
                    }
                };
                Ok(TargetBinding::Server {
                    endpoint,
                    auth,
                    default_graph: self.default_graph.clone(),
                })
            }
            TargetKind::Managed => {
                forbid("uri", self.uri.is_some())?;
                forbid("provider", self.provider.is_some())?;
                forbid("credential", self.credential.is_some())?;
                forbid("auth", self.auth.is_some())?;
                let api = self
                    .api
                    .clone()
                    .ok_or_else(|| invalid(name, "managed targets require api"))?;
                validate_http_endpoint(&api).map_err(|e| invalid(name, e))?;
                let url = reqwest::Url::parse(&api)?;
                if !matches!(url.path(), "" | "/") {
                    return Err(invalid(
                        name,
                        "managed api must be an origin without a path",
                    ));
                }
                let control_auth = self.control_auth.ok_or_else(|| {
                    invalid(
                        name,
                        "managed targets require control_auth: human-device or automation-env",
                    )
                })?;
                if self
                    .organization_id
                    .as_deref()
                    .is_some_and(|v| v.trim().is_empty())
                {
                    return Err(invalid(name, "organization_id must be nonempty"));
                }
                let cluster = match self.scope {
                    Some(TargetScope::Account) => {
                        forbid("cluster_id", self.cluster_id.is_some())?;
                        forbid("cluster_incarnation", self.cluster_incarnation.is_some())?;
                        forbid("endpoint", self.endpoint.is_some())?;
                        forbid("default_graph", self.default_graph.is_some())?;
                        if self.organization_id.is_none()
                            && control_auth != crate::managed::ControlAuth::HumanDevice
                        {
                            return Err(invalid(
                                name,
                                "only human-device account login may omit organization_id",
                            ));
                        }
                        None
                    }
                    Some(TargetScope::Cluster) => {
                        if self.organization_id.is_none() {
                            return Err(invalid(name, "cluster targets require organization_id"));
                        }
                        let required = |value: &Option<String>, field| {
                            value
                                .clone()
                                .filter(|v| !v.trim().is_empty())
                                .ok_or_else(|| {
                                    invalid(name, format!("cluster targets require {field}"))
                                })
                        };
                        if let Some(endpoint) = &self.endpoint {
                            validate_http_endpoint(endpoint).map_err(|e| invalid(name, e))?;
                        }
                        Some(crate::managed::ClusterBinding {
                            cluster_id: required(&self.cluster_id, "cluster_id")?,
                            cluster_incarnation: required(
                                &self.cluster_incarnation,
                                "cluster_incarnation",
                            )?,
                            endpoint: self.endpoint.clone(),
                            default_graph: self.default_graph.clone(),
                        })
                    }
                    _ => {
                        return Err(invalid(
                            name,
                            "managed targets require scope account or cluster",
                        ));
                    }
                };
                Ok(TargetBinding::Managed(crate::managed::Binding {
                    api: url.origin().ascii_serialization(),
                    organization_id: self.organization_id.clone(),
                    cluster,
                    control_auth,
                }))
            }
        }
    }
}

impl TargetBinding {
    pub(crate) fn digest(&self) -> Result<String> {
        let endpoint_identity = |endpoint: &str| -> Result<serde_json::Value> {
            Ok(RequestTarget::of(endpoint, endpoint)?.binding_identity())
        };
        let identity = match self {
            Self::DirectGraph { uri, provider } => {
                serde_json::json!({"kind":"direct","scope":"graph","uri":uri,"provider":provider})
            }
            Self::DirectRoot { uri, provider, .. } => {
                serde_json::json!({"kind":"direct","scope":"root","uri":uri,"provider":provider})
            }
            Self::DirectCatalogue => serde_json::json!({"kind":"direct","scope":"catalogue"}),
            Self::Server { endpoint, auth, .. } => {
                serde_json::json!({"kind":"server","endpoint":endpoint_identity(endpoint)?,"credential":match auth {ServerAuth::Token{owner}=>Some(owner),ServerAuth::Anonymous=>None}})
            }
            Self::Managed(binding) => {
                let cluster = match &binding.cluster {
                    Some(cluster) => Some(serde_json::json!({
                        "id": cluster.cluster_id,
                        "incarnation": cluster.cluster_incarnation,
                        "endpoint": cluster.endpoint.as_deref().map(endpoint_identity).transpose()?,
                    })),
                    None => None,
                };
                serde_json::json!({"kind":"managed","api":endpoint_identity(&binding.api)?,"organization_id":binding.organization_id,"cluster":cluster,"control_auth":binding.control_auth})
            }
        };
        Ok(format!(
            "sha256:{:x}",
            Sha256::digest(identity.to_string().as_bytes())
        ))
    }
}

pub(crate) fn validate_config(config: &OperatorConfig) -> Result<()> {
    if config.target_config_version != Some(TARGET_CONFIG_VERSION) {
        return Err(refusal(
            "target_invalid",
            "unsupported target_config_version; this CLI reads version 1",
        ));
    }
    for name in config.targets.keys() {
        if !valid_name(name) {
            return Err(invalid(
                name,
                "name must be one ASCII word of letters, digits, -, _ and .; og_catalog is reserved",
            ));
        }
    }
    validate_credential_owners(config)
}

struct ServerBinding<'a> {
    label: &'a str,
    owner: CredentialOwner<'a>,
    base: RequestTarget,
}
fn server_bindings(config: &OperatorConfig) -> Vec<ServerBinding<'_>> {
    config
        .targets
        .iter()
        .filter_map(|(name, entry)| {
            if entry.kind != TargetKind::Server {
                return None;
            }
            // Credential boundaries do not depend on unrelated graph defaults
            // or the completeness needed to execute this entry as a target.
            let endpoint = entry.endpoint.as_deref()?;
            let owner = match (entry.credential.as_deref(), entry.auth) {
                (Some(owner), None) if valid_name(owner) => CredentialOwner::Keyed(owner),
                (None, Some(TargetAuth::Anonymous)) => CredentialOwner::Anonymous { target: name },
                // An incomplete auth declaration claims a boundary but grants
                // no bearer authority: parent tokens must not fall through.
                _ => CredentialOwner::Anonymous { target: name },
            };
            Some(ServerBinding {
                label: name,
                owner,
                base: RequestTarget::of(endpoint, endpoint).ok()?,
            })
        })
        .collect()
}
/// Immutable credential ownership for every composed request of one client.
#[derive(Debug, Clone)]
pub(crate) struct ServerRequestScope {
    root: RequestTarget,
    auth: ServerAuth,
    boundaries: Vec<(RequestTarget, Option<String>)>,
}
impl ServerRequestScope {
    pub(crate) fn validate(&self, request: &RequestTarget) -> Result<()> {
        let declared = self
            .boundaries
            .iter()
            .filter(|(base, _)| request.is_under(base))
            .max_by_key(|(base, _)| base.depth())
            .and_then(|(_, owner)| owner.as_deref());
        let selected = match &self.auth {
            ServerAuth::Token { owner } => Some(owner.as_str()),
            ServerAuth::Anonymous => None,
        };
        if !request.is_under(&self.root) || declared != selected {
            return Err(refusal(
                "credential_scope_mismatch",
                "request leaves the captured server credential scope",
            ));
        }
        Ok(())
    }
}

pub(crate) fn capture_server_scope(
    input: &crate::operator::InvocationConfig,
    auth: &ServerAuth,
    endpoint: &str,
) -> Result<ServerRequestScope> {
    let config = input.get(&mut std::io::sink())?;
    let scope = ServerRequestScope {
        root: RequestTarget::of(endpoint, endpoint)?,
        auth: auth.clone(),
        boundaries: server_bindings(config)
            .into_iter()
            .map(|binding| {
                let owner = match binding.owner {
                    CredentialOwner::Keyed(owner) => Some(owner.to_owned()),
                    CredentialOwner::Anonymous { .. } => None,
                };
                (binding.base, owner)
            })
            .collect(),
    };
    scope.validate(&scope.root)?;
    Ok(scope)
}

fn validate_credential_owners(config: &OperatorConfig) -> Result<()> {
    let bindings = server_bindings(config);
    let mut variables = BTreeMap::new();
    for binding in &bindings {
        if let CredentialOwner::Keyed(owner) = binding.owner {
            if let Some(other) = variables.insert(token_env_name(owner), owner) {
                if other != owner {
                    return Err(refusal(
                        "target_invalid",
                        format!(
                            "credential owners '{owner}' and '{other}' share an environment token variable"
                        ),
                    ));
                }
            }
        }
        for other in &bindings {
            let owner = |binding: &ServerBinding<'_>| match binding.owner {
                CredentialOwner::Keyed(value) => Some(value.to_owned()),
                CredentialOwner::Anonymous { .. } => None,
            };
            let same_owner = owner(binding) == owner(other);
            if (binding.base == other.base && !same_owner)
                || (matches!(binding.owner, CredentialOwner::Keyed(_))
                    && same_owner
                    && binding.base != other.base)
            {
                return Err(refusal(
                    "target_invalid",
                    format!(
                        "targets '{}' and '{}' conflict: a credential owner binds exactly one endpoint scope",
                        binding.label, other.label
                    ),
                ));
            }
        }
    }
    Ok(())
}
impl OperatorConfig {
    pub(crate) fn default_target(&self) -> Option<&str> {
        self.defaults.target.as_deref()
    }
    pub(crate) fn target(&self, name: &str) -> Result<&TargetEntry> {
        self.targets.get(name).ok_or_else(|| refusal("target_unknown", format!("unknown target '{name}'; register it with `omnigraph target add {name}`, or write `./{name}` for a relative path")))
    }
    pub(crate) fn binding(&self, name: &str) -> Result<TargetBinding> {
        self.target(name)?.binding_at(name, &self.base_dir)
    }
    pub(crate) fn credential_owner_for(
        &self,
        request: &RequestTarget,
    ) -> Option<CredentialOwner<'_>> {
        server_bindings(self)
            .into_iter()
            .filter(|b| request.is_under(&b.base))
            .max_by_key(|b| b.base.depth())
            .map(|b| b.owner)
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct TargetListOutput {
    pub(crate) protocol: u32,
    pub(crate) default_target: Option<String>,
    pub(crate) targets: Vec<TargetOutput>,
}
#[derive(Debug, Serialize)]
pub(crate) struct TargetOutput {
    pub(crate) name: String,
    #[serde(flatten)]
    pub(crate) entry: TargetEntry,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) binding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}
impl TargetOutput {
    pub(crate) fn of(name: &str, entry: &TargetEntry, base: &Path) -> Self {
        let binding = entry
            .binding_at(name, base)
            .and_then(|binding| binding.digest());
        let mut safe_entry = entry.clone();
        for raw in [
            &mut safe_entry.uri,
            &mut safe_entry.endpoint,
            &mut safe_entry.api,
        ]
        .into_iter()
        .flatten()
        {
            *raw = omnigraph::storage::redacted_storage_uri(raw);
        }
        if let Some(
            StorageProvider::S3 { endpoint, .. } | StorageProvider::Azure { endpoint, .. },
        ) = &mut safe_entry.provider
        {
            if let Some(raw) = endpoint {
                *raw = omnigraph::storage::redacted_storage_uri(raw);
            }
        }
        Self {
            name: name.into(),
            entry: safe_entry,
            binding: binding.as_ref().ok().cloned(),
            error: binding.err().map(|e| e.to_string()),
        }
    }
}
pub(crate) fn list_output(config: &OperatorConfig) -> TargetListOutput {
    TargetListOutput {
        protocol: TARGET_PROTOCOL,
        default_target: config.defaults.target.clone(),
        targets: config
            .targets
            .iter()
            .map(|(name, entry)| TargetOutput::of(name, entry, &config.base_dir))
            .collect(),
    }
}
pub(crate) fn write_target_line(
    target: &TargetOutput,
    default: bool,
    out: &mut dyn Write,
) -> Result<()> {
    write!(
        out,
        "{}{}  {:?}",
        target.name,
        if default { " (default)" } else { "" },
        target.entry.kind
    )?;
    if let Some(scope) = target.entry.scope {
        write!(out, " {scope:?}")?;
    }
    if let Some(address) = target
        .entry
        .uri
        .as_ref()
        .or(target.entry.endpoint.as_ref())
        .or(target.entry.api.as_ref())
    {
        write!(out, "  {address}")?;
    }
    if let Some(error) = &target.error {
        write!(out, "  invalid: {error}")?;
    }
    writeln!(out)?;
    Ok(())
}

// ---- editing ----

/// Why an edit cannot be made while keeping the rest of the file's text.
fn not_editable(why: impl std::fmt::Display) -> color_eyre::Report {
    refusal(
        "config_not_editable",
        format!("the operator config cannot be edited safely: {why}; edit it by hand"),
    )
}

/// The `targets:` block of a block-style YAML document: the index of its key
/// line and of the first line after the block, and the indentation of its
/// entries.
struct TargetsBlock {
    key_line: usize,
    end: usize,
    indent: usize,
    /// The key line was `targets: {}`, an empty flow mapping.
    empty_flow: bool,
}

fn find_targets_block(lines: &[&str]) -> Result<Option<TargetsBlock>> {
    let mut found = None;
    for (index, line) in lines.iter().enumerate() {
        if let Some(rest) = line.strip_prefix("targets:") {
            if found.is_some() {
                return Err(not_editable("`targets` appears twice"));
            }
            let rest = rest.trim();
            // A trailing comment is not a value.
            let rest = if rest.starts_with('#') {
                ""
            } else {
                rest.split(" #").next().unwrap_or_default().trim()
            };
            let empty_flow = match rest {
                "" => false,
                "{}" => true,
                _ => return Err(not_editable("`targets` is not a block mapping")),
            };
            found = Some((index, empty_flow));
        }
    }
    let Some((key_line, empty_flow)) = found else {
        return Ok(None);
    };
    let mut end = key_line + 1;
    let mut indent = None;
    while end < lines.len() {
        let line = lines[end];
        let trimmed = line.trim_start();
        if trimmed.is_empty() || (trimmed.starts_with('#') && line.starts_with(' ')) {
            end += 1;
            continue;
        }
        if !line.starts_with(' ') {
            if line.starts_with('\t') {
                return Err(not_editable("the file indents with tabs"));
            }
            break;
        }
        let width = line.len() - trimmed.len();
        if indent.is_none() {
            indent = Some(width);
        }
        end += 1;
    }
    // Trailing blank or comment lines belong after the block.
    while end > key_line + 1 && lines[end - 1].trim().is_empty() {
        end -= 1;
    }
    if empty_flow && indent.is_some() {
        return Err(not_editable("`targets: {}` is followed by indented lines"));
    }
    Ok(Some(TargetsBlock {
        key_line,
        end,
        indent: indent.unwrap_or(2),
        empty_flow,
    }))
}

/// The lines of one entry, `name:` at `indent` and its fields below it.
fn render_entry(name: &str, entry: &TargetEntry, indent: usize) -> Result<Vec<String>> {
    let body = yaml_serde::to_string(entry)?;
    let pad = " ".repeat(indent);
    let child = " ".repeat(indent * 2);
    let mut lines = vec![format!("{pad}{name}:")];
    for line in body.lines() {
        if line.trim().is_empty() || line == "---" {
            continue;
        }
        lines.push(format!("{child}{line}"));
    }
    Ok(lines)
}

/// The file's text with `name: entry` added under `targets:`.
pub(crate) fn add_entry_text(text: &str, name: &str, entry: &TargetEntry) -> Result<String> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out: Vec<String> = Vec::with_capacity(lines.len() + 8);
    let has_version = lines
        .iter()
        .any(|line| line.starts_with("target_config_version:"));
    match find_targets_block(&lines)? {
        Some(block) => {
            for (index, line) in lines.iter().enumerate() {
                if index == block.key_line {
                    if !has_version {
                        out.push(format!("target_config_version: {TARGET_CONFIG_VERSION}"));
                    }
                    out.push(if block.empty_flow {
                        "targets:".to_string()
                    } else {
                        (*line).to_string()
                    });
                    if block.empty_flow {
                        out.extend(render_entry(name, entry, block.indent)?);
                    }
                    continue;
                }
                if index == block.end && !block.empty_flow {
                    out.extend(render_entry(name, entry, block.indent)?);
                }
                out.push((*line).to_string());
            }
            if block.end == lines.len() && !block.empty_flow {
                out.extend(render_entry(name, entry, block.indent)?);
            }
        }
        None => {
            out.extend(lines.iter().map(|line| (*line).to_string()));
            if out.last().is_some_and(|line| !line.trim().is_empty()) {
                out.push(String::new());
            }
            if !has_version {
                out.push(format!("target_config_version: {TARGET_CONFIG_VERSION}"));
            }
            out.push("targets:".to_string());
            out.extend(render_entry(name, entry, 2)?);
        }
    }
    let mut text = out.join("\n");
    text.push('\n');
    Ok(text)
}

/// The file's text with `name`'s entry removed from `targets:`.
pub(crate) fn remove_entry_text(text: &str, name: &str) -> Result<String> {
    let lines: Vec<&str> = text.lines().collect();
    let block = find_targets_block(&lines)?
        .ok_or_else(|| not_editable("no `targets:` block holds the entry"))?;
    let key = format!("{}{name}:", " ".repeat(block.indent));
    let start = (block.key_line + 1..block.end)
        .find(|index| {
            let line = lines[*index];
            line == key || line.starts_with(&format!("{key} "))
        })
        .ok_or_else(|| not_editable(format!("`{name}` is not a plain key of `targets:`")))?;
    let mut stop = start + 1;
    while stop < block.end {
        let line = lines[stop];
        let trimmed = line.trim_start();
        let width = line.len() - trimmed.len();
        if !trimmed.is_empty() && width <= block.indent {
            break;
        }
        stop += 1;
    }
    let remaining_entries = (block.key_line + 1..block.end)
        .filter(|index| !(start..stop).contains(index))
        .any(|index| {
            let line = lines[index];
            let trimmed = line.trim_start();
            !trimmed.is_empty() && !trimmed.starts_with('#')
        });
    let mut out: Vec<String> = Vec::with_capacity(lines.len());
    for (index, line) in lines.iter().enumerate() {
        if (start..stop).contains(&index) {
            continue;
        }
        if index == block.key_line && !remaining_entries {
            out.push("targets: {}".to_string());
            continue;
        }
        out.push((*line).to_string());
    }
    let mut text = out.join("\n");
    text.push('\n');
    Ok(text)
}

/// The parsed document without `targets` and `target_config_version`, to
/// prove an edit changed nothing else.
fn document_without_targets(text: &str) -> Result<yaml_serde::Value> {
    let mut value: yaml_serde::Value = if text.trim().is_empty() {
        yaml_serde::Value::Mapping(yaml_serde::Mapping::new())
    } else {
        yaml_serde::from_str(text)?
    };
    match &mut value {
        yaml_serde::Value::Mapping(mapping) => {
            mapping.remove("targets");
            mapping.remove("target_config_version");
        }
        yaml_serde::Value::Null => {
            value = yaml_serde::Value::Mapping(yaml_serde::Mapping::new());
        }
        _ => return Err(not_editable("the file is not a mapping")),
    }
    Ok(value)
}

/// Check an edit: the new text parses into a valid configuration whose
/// targets are `expected` and whose every other key is unchanged.
pub(crate) fn verify_edit(
    before: &str,
    after: &str,
    expected: &BTreeMap<String, TargetEntry>,
) -> Result<OperatorConfig> {
    // A binding the configuration refuses is the entry's fault, not the edit's.
    let config = match crate::operator::parse_operator_config(after) {
        Ok(config) => config,
        Err(error) if error.downcast_ref::<TargetError>().is_some() => return Err(error),
        Err(error) => {
            return Err(not_editable(format!(
                "the edited file would not load: {error}"
            )));
        }
    };
    if &config.targets != expected {
        return Err(not_editable(
            "the edit did not produce the expected targets",
        ));
    }
    if document_without_targets(before)? != document_without_targets(after)? {
        return Err(not_editable("the edit would change other keys"));
    }
    Ok(config)
}

/// Replace the file by writing a sibling and renaming it over, keeping its
/// permissions.
pub(crate) fn replace_file(path: &Path, text: &str) -> Result<()> {
    if text.len() > crate::operator::OPERATOR_FILE_MAX_BYTES {
        return Err(refusal(
            "target_config_invalid",
            "operator config exceeds 1 MiB",
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| eyre!("operator config path has no parent directory"))?;
    std::fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    if let Ok(metadata) = std::fs::metadata(path) {
        file.as_file().set_permissions(metadata.permissions())?;
    }
    file.write_all(text.as_bytes())?;
    file.as_file().sync_all()?;
    file.persist(path)
        .map_err(|error| eyre!("could not replace operator config: {}", error.error))?;
    crate::sync_dir(parent)?;
    Ok(())
}

/// An exclusive advisory lock on `config.yaml.lock`, held across one edit's
/// read, check and replace, so two edits never lose each other's entry. The
/// lock file is a stable sibling, never removed. Unix only; elsewhere an edit
/// refuses rather than run without the lock.
#[cfg_attr(not(unix), allow(dead_code))]
pub(crate) struct ConfigEditLock {
    #[cfg(unix)]
    _file: std::fs::File,
}

impl ConfigEditLock {
    pub(crate) fn acquire(config: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            if let Some(parent) = config.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut lock_path = config.as_os_str().to_os_string();
            lock_path.push(".lock");
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(std::path::PathBuf::from(lock_path))?;
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(Self { _file: file })
        }
        #[cfg(not(unix))]
        {
            let _ = config;
            Err(refusal(
                "config_not_editable",
                "the operator config cannot be edited safely here: this platform has no file lock to hold across the edit; edit the file by hand on this platform",
            ))
        }
    }
}

#[cfg(test)]
#[path = "targets_tests.rs"]
mod tests;

/// Acquire only the credential already bound to the captured endpoint.
pub(crate) fn resolve_server_token(
    input: &crate::operator::InvocationConfig,
    auth: &ServerAuth,
    endpoint: &str,
) -> Result<Option<String>> {
    resolve_server_token_for(input, auth, endpoint, endpoint)
}

/// Resolve the deepest configured credential boundary for the actual request,
/// keeping the transport root separate so Unix socket paths stay unambiguous.
pub(crate) fn resolve_server_token_for(
    input: &crate::operator::InvocationConfig,
    auth: &ServerAuth,
    server_root: &str,
    request_url: &str,
) -> Result<Option<String>> {
    let ServerAuth::Token { owner } = auth else {
        return Ok(None);
    };
    let config = input.get(&mut std::io::sink())?;
    let request = RequestTarget::of(server_root, request_url)?;
    if config.credential_owner_for(&request) != Some(CredentialOwner::Keyed(owner)) {
        return Err(refusal(
            "credential_scope_mismatch",
            "the credential owner is not bound to the selected endpoint",
        ));
    }
    if let Ok(token) = std::env::var(token_env_name(owner)) {
        if !token.trim().is_empty() {
            return Ok(Some(token.trim().into()));
        }
    }
    if let Some(path) = input.credentials_path() {
        if let Some(saved) = crate::operator::read_bound_credential_at(&path, owner)? {
            let saved_endpoint = saved.endpoint.as_deref().ok_or_else(|| refusal("credential_scope_missing", "saved credential has no endpoint binding; run target import or login for this target"))?;
            let scope = RequestTarget::of(saved_endpoint, saved_endpoint)?;
            if !request.is_under(&scope) {
                return Err(refusal(
                    "credential_scope_mismatch",
                    "saved credential belongs to a different endpoint; log in for the selected target",
                ));
            }
            return Ok(Some(saved.token));
        }
    }
    Err(refusal(
        "credential_missing",
        format!(
            "no token for credential '{owner}'; run login for this target or set {}",
            token_env_name(owner)
        ),
    ))
}

pub(crate) fn write_server_credential(
    input: &crate::operator::InvocationConfig,
    owner: &str,
    endpoint: &str,
    token: Option<&str>,
) -> Result<()> {
    validate_server_endpoint(endpoint)?;
    let config = input.get(&mut std::io::sink())?;
    let target = RequestTarget::of(endpoint, endpoint)?;
    if config.credential_owner_for(&target) != Some(CredentialOwner::Keyed(owner)) {
        return Err(refusal(
            "credential_scope_mismatch",
            "credential owner is not bound to this endpoint",
        ));
    }
    let path = input
        .credentials_path()
        .ok_or_else(|| eyre!("no operator home; set OMNIGRAPH_HOME"))?;
    crate::operator::rewrite_bound_credentials_at(&path, owner, Some(endpoint), token)
}
