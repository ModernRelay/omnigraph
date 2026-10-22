//! Complete target bindings and conservative operator-file administration.

#[cfg(test)]
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use serde::{Deserialize, Serialize};

use crate::graph_http::RequestTarget;
use crate::operator::OperatorConfig;

// Compatibility generations across published binaries, independent of the YAML
// document version. Incompatible configuration/command changes bump these.
pub(crate) const TARGET_PROTOCOL: u32 = 2;
pub(crate) const CREDENTIAL_PROTOCOL: u32 = 2;
pub(crate) const ACCESS_PROTOCOL: u32 = 2;

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
    #[cfg(test)]
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
    let mut bytes = name.bytes();
    bytes.next().is_some_and(|c| c.is_ascii_alphabetic())
        && bytes.all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
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
                    api: url.as_str().trim_end_matches('/').to_owned(),
                    organization_id: self.organization_id.clone(),
                    cluster,
                    control_auth,
                }))
            }
        }
    }
}

struct ServerBinding<'a> {
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
    let root = RequestTarget::of(endpoint, endpoint)?;
    let mut boundaries = vec![(
        root.clone(),
        match auth {
            ServerAuth::Token { owner } => Some(owner.clone()),
            ServerAuth::Anonymous => None,
        },
    )];
    boundaries.extend(
        server_bindings(input.get(&mut std::io::sink())?)
            .into_iter()
            .filter(|binding| binding.base.is_under(&root) && binding.base.depth() > root.depth())
            .map(|binding| {
                let owner = match binding.owner {
                    CredentialOwner::Keyed(owner) => Some(owner.to_owned()),
                    CredentialOwner::Anonymous { .. } => None,
                };
                (binding.base, owner)
            }),
    );
    let scope = ServerRequestScope {
        root,
        auth: auth.clone(),
        boundaries,
    };
    scope.validate(&scope.root)?;
    Ok(scope)
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
/// lock file is a stable sibling, never removed. Unsupported file locking
/// refuses rather than allowing an uncoordinated edit.
pub(crate) struct ConfigEditLock {
    file: std::fs::File,
}

impl Drop for ConfigEditLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl ConfigEditLock {
    pub(crate) fn acquire(config: &Path) -> Result<Self> {
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
        file.try_lock()
            .map_err(|error| eyre!("could not lock operator configuration: {error}"))?;
        Ok(Self { file })
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
    _input: &crate::operator::InvocationConfig,
    auth: &ServerAuth,
    server_root: &str,
    request_url: &str,
) -> Result<Option<String>> {
    match frozen_server_credential(auth, server_root, request_url)? {
        Some(selected) => selected.cached_token(server_root),
        None => Ok(None),
    }
}

/// The credential of [`resolve_server_token_for`] read from its source
/// again, for a long-lived client whose server refused it. Only the client
/// holds the new token; the invocation's own credential is unchanged.
pub(crate) async fn reread_server_credential_for(
    auth: &ServerAuth,
    server_root: &str,
    request_url: &str,
) -> Result<Option<crate::credential_sources::AcquiredCredential>> {
    match frozen_server_credential(auth, server_root, request_url)? {
        Some(selected) => selected.reacquire(server_root).await,
        None => Ok(None),
    }
}

/// The invocation's frozen owner/profile binding for a request under the
/// captured endpoint, or none for anonymous access.
fn frozen_server_credential(
    auth: &ServerAuth,
    server_root: &str,
    request_url: &str,
) -> Result<Option<std::sync::Arc<crate::operator_routing::Authentication>>> {
    let ServerAuth::Token { .. } = auth else {
        return Ok(None);
    };
    let request = RequestTarget::of(server_root, request_url)?;
    if !request.is_under(&RequestTarget::of(server_root, server_root)?) {
        return Err(refusal(
            "credential_scope_mismatch",
            "the credential owner is not bound to the selected endpoint",
        ));
    }
    crate::operator_routing::AUTH
        .try_with(Clone::clone)
        .ok()
        .flatten()
        .map(Some)
        .ok_or_else(|| refusal("credential_missing", "no frozen owner/profile binding"))
}
