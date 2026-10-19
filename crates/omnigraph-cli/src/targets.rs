//! Named targets in the operator configuration.
//!
//! A target is one complete destination binding: a direct graph or cluster
//! root in storage, or a server with its authentication. `--target NAME`, else
//! `defaults.target`, selects one for a command, and the scope resolver turns it
//! into the same addressing a profile or an explicit flag yields. A server
//! target's credential owner keys the existing token chain and is bound to the
//! target's endpoint: its token goes only to URLs under that endpoint.
//!
//! The `target` command group lists the targets and edits one entry at a time.
//! An edit changes only that entry's lines, keeps the rest of the file's text,
//! proves by parsing the result that nothing else changed, and replaces the
//! file by rename.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use color_eyre::Result;
use color_eyre::eyre::eyre;
use omnigraph_api_types::ErrorCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::graph_http::RequestTarget;
use crate::helpers::CliRefusal;
use crate::operator::{OperatorConfig, token_env_name};

/// The targets format this CLI reads and writes, named by
/// `target_config_version`.
pub(crate) const TARGET_CONFIG_VERSION: u32 = 1;

/// The version of the `target` and `credential` commands' machine output.
pub(crate) const TARGET_PROTOCOL: u32 = 1;

/// The version of the `access` report, a contract of its own that changes
/// apart from the `target` and `credential` envelopes.
pub(crate) const ACCESS_PROTOCOL: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetKind {
    /// Storage the CLI opens itself.
    Direct,
    /// A server the CLI reaches over HTTP.
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetScope {
    /// One standalone graph.
    Graph,
    /// A cluster's storage root.
    Root,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetAuth {
    /// Requests carry no credential.
    Anonymous,
}

/// One entry under `targets:`, as the file holds it. Unknown fields refuse the
/// file; whether the fields make a complete binding is checked when the entry
/// is used ([`TargetEntry::binding`]), so an unrelated incomplete entry does
/// not block another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetEntry {
    pub(crate) kind: TargetKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<TargetScope>,
    /// A direct target's storage: an absolute path or a `file://` URI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) uri: Option<String>,
    /// A server target's `http(s)://` URL or `unix:///absolute/path.sock`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint: Option<String>,
    /// The credential owner whose token a server target's requests carry:
    /// `OMNIGRAPH_TOKEN_<OWNER>`, else `[<owner>]` in the credentials file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) credential: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) auth: Option<TargetAuth>,
    /// The graph a server or cluster-root target addresses when `--graph`
    /// names none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) default_graph: Option<String>,
}

/// A target entry checked into one complete binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetBinding {
    DirectGraph {
        uri: String,
    },
    DirectRoot {
        uri: String,
        default_graph: Option<String>,
    },
    Server {
        endpoint: String,
        auth: ServerAuth,
        default_graph: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerAuth {
    Token { owner: String },
    Anonymous,
}

/// Who holds the credential for a request: a keyed owner, or an anonymous
/// target, whose requests carry none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialOwner<'a> {
    Keyed(&'a str),
    Anonymous { target: &'a str },
}

/// A target or credential-owner name: one ASCII word, so it is a plain YAML
/// key and a credentials-file section.
pub(crate) fn valid_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|first| first.is_ascii_alphanumeric())
        && name.len() <= 64
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn invalid(name: &str, why: impl std::fmt::Display) -> color_eyre::Report {
    CliRefusal::report(
        ErrorCode::TargetInvalid,
        format!("target '{name}' is not a complete binding: {why}"),
    )
}

impl TargetEntry {
    /// The complete binding this entry names, or `target_invalid`.
    pub(crate) fn binding(&self, name: &str) -> Result<TargetBinding> {
        let forbid = |field: &str, present: bool, kind: &str| {
            if present {
                Err(invalid(name, format!("a {kind} target takes no `{field}`")))
            } else {
                Ok(())
            }
        };
        if let Some(graph) = &self.default_graph {
            crate::helpers::require_graph_segment(graph).map_err(|error| invalid(name, error))?;
        }
        match self.kind {
            TargetKind::Direct => {
                forbid("endpoint", self.endpoint.is_some(), "direct")?;
                forbid("credential", self.credential.is_some(), "direct")?;
                forbid("auth", self.auth.is_some(), "direct")?;
                let uri = self
                    .uri
                    .clone()
                    .ok_or_else(|| invalid(name, "a direct target needs `uri`"))?;
                validate_storage_uri(&uri).map_err(|why| invalid(name, why))?;
                match self.scope {
                    Some(TargetScope::Graph) => {
                        forbid("default_graph", self.default_graph.is_some(), "graph")?;
                        Ok(TargetBinding::DirectGraph { uri })
                    }
                    Some(TargetScope::Root) => Ok(TargetBinding::DirectRoot {
                        uri,
                        default_graph: self.default_graph.clone(),
                    }),
                    None => Err(invalid(
                        name,
                        "a direct target needs `scope: graph` or `scope: root`",
                    )),
                }
            }
            TargetKind::Server => {
                forbid("scope", self.scope.is_some(), "server")?;
                forbid("uri", self.uri.is_some(), "server")?;
                let endpoint = self
                    .endpoint
                    .clone()
                    .ok_or_else(|| invalid(name, "a server target needs `endpoint`"))?;
                crate::graph_http::ServerTransport::parse_endpoint(&endpoint)
                    .map_err(|error| invalid(name, error))?;
                let auth = match (&self.credential, self.auth) {
                    (Some(owner), None) => {
                        if !valid_name(owner) {
                            return Err(invalid(
                                name,
                                format!(
                                    "credential owner `{owner}` must be ASCII letters, digits, `-`, `_` or `.`"
                                ),
                            ));
                        }
                        ServerAuth::Token {
                            owner: owner.clone(),
                        }
                    }
                    (None, Some(TargetAuth::Anonymous)) => ServerAuth::Anonymous,
                    (Some(_), Some(_)) => {
                        return Err(invalid(
                            name,
                            "a server target takes `credential` or `auth: anonymous`, not both",
                        ));
                    }
                    (None, None) => {
                        return Err(invalid(
                            name,
                            "a server target names its `credential` owner or sets `auth: anonymous`",
                        ));
                    }
                };
                Ok(TargetBinding::Server {
                    endpoint,
                    auth,
                    default_graph: self.default_graph.clone(),
                })
            }
        }
    }
}

/// A direct target's storage is written out in full, so the same entry names
/// the same storage from every working directory and from any environment.
/// Cloud storage is refused: its endpoint and account come from the
/// environment when it is opened, and this format cannot pin them in the
/// binding, so the entry would not be the complete address it claims.
fn validate_storage_uri(uri: &str) -> std::result::Result<(), String> {
    if let Some((scheme, _)) = uri.split_once("://") {
        return match scheme {
            "file" => {
                let url = reqwest::Url::parse(uri)
                    .map_err(|_| "`uri` is not a valid file:// URI".to_string())?;
                if url.query().is_some() || url.fragment().is_some() {
                    return Err("a file:// `uri` carries no query or fragment".to_string());
                }
                omnigraph::storage::normalize_root_uri(uri)
                    .map(|_| ())
                    .map_err(|_| {
                        "a file:// `uri` names a local path, with no host other than localhost"
                            .to_string()
                    })
            }
            "s3" | "az" => Err(format!(
                "a cloud target ({scheme}://) needs its storage endpoint and account pinned in \
                 the binding, which this version cannot record, so the environment would still \
                 choose where it points; address cloud storage directly with --store <uri> or \
                 --cluster <uri>"
            )),
            other => Err(format!(
                "`uri` must be an absolute path or a file:// URI, not {other}://"
            )),
        };
    }
    if Path::new(uri).is_absolute() {
        Ok(())
    } else {
        Err(format!(
            "`uri` must be an absolute path or a file:// URI, not `{uri}`"
        ))
    }
}

impl TargetBinding {
    /// A digest of where the binding points and how it authenticates, which a
    /// client compares to notice that a target it remembered now names
    /// something else. It holds no secret. A direct target's storage is
    /// digested in the normalized spelling the engine opens, so `/data/g` and
    /// `file:///data/g` are one store.
    pub(crate) fn digest(&self) -> String {
        let storage =
            |uri: &str| omnigraph::storage::normalize_root_uri(uri).unwrap_or_else(|_| uri.into());
        let identity = match self {
            Self::DirectGraph { uri } => {
                serde_json::json!({"kind":"direct","scope":"graph","uri":storage(uri)})
            }
            Self::DirectRoot { uri, .. } => {
                serde_json::json!({"kind":"direct","scope":"root","uri":storage(uri)})
            }
            // An endpoint is digested as requests are matched to it, so
            // `https://h`, `https://h/`, `https://h:443` and `https://H` are
            // one binding.
            Self::Server { endpoint, auth, .. } => serde_json::json!({
                "kind":"server",
                "endpoint": RequestTarget::of(endpoint, endpoint)
                    .map(|target| target.canonical())
                    .unwrap_or_else(|_| endpoint.clone()),
                "credential": match auth {
                    ServerAuth::Token { owner } => serde_json::Value::String(owner.clone()),
                    ServerAuth::Anonymous => serde_json::Value::Null,
                },
            }),
        };
        let digest = Sha256::digest(identity.to_string().as_bytes());
        format!("sha256:{digest:x}")
    }
}

/// Checks that hold across the whole file, run whenever it is read: the
/// format version, one default, and one credential owner per endpoint.
pub(crate) fn validate_config(config: &OperatorConfig) -> Result<()> {
    let declares_targets = !config.targets.is_empty() || config.defaults.target.is_some();
    match config.target_config_version {
        Some(TARGET_CONFIG_VERSION) => {}
        Some(other) => {
            return Err(CliRefusal::report(
                ErrorCode::TargetInvalid,
                format!(
                    "operator config `target_config_version: {other}` is not supported; this CLI reads version {TARGET_CONFIG_VERSION}"
                ),
            ));
        }
        None if declares_targets => {
            return Err(CliRefusal::report(
                ErrorCode::TargetInvalid,
                format!(
                    "operator config declares targets; add `target_config_version: {TARGET_CONFIG_VERSION}`"
                ),
            ));
        }
        None => {}
    }
    for name in config.targets.keys() {
        if !valid_name(name) {
            return Err(CliRefusal::report(
                ErrorCode::TargetInvalid,
                format!(
                    "target name `{name}` must start with an ASCII letter or digit and use only letters, digits, `-`, `_` or `.`"
                ),
            ));
        }
    }
    if config.defaults.target.is_some()
        && (config.defaults.server.is_some()
            || config.defaults.store.is_some()
            || config.defaults.default_graph.is_some())
    {
        return Err(CliRefusal::report(
            ErrorCode::TargetInvalid,
            "operator config `defaults` sets `target` beside `server`, `store` or `default_graph`; a target carries its own default graph, so keep one default",
        ));
    }
    validate_credential_owners(config)
}

/// One credential binding: who holds the credential for requests under
/// `base`, and the entry that declares it.
struct ServerBinding<'a> {
    label: String,
    owner: CredentialOwner<'a>,
    base: RequestTarget,
    address: String,
}

/// Every credential binding the file declares: each `servers:` entry and
/// each server target that validates. The ownership checks and credential
/// lookup read this one list, so an entry that fails validation never
/// supplies a credential.
fn server_bindings(config: &OperatorConfig) -> Vec<ServerBinding<'_>> {
    let mut bindings = Vec::new();
    for (name, server) in &config.servers {
        if let Ok(base) = RequestTarget::of(&server.url, &server.url) {
            bindings.push(ServerBinding {
                label: format!("server `{name}`"),
                owner: CredentialOwner::Keyed(name),
                base,
                address: server.url.clone(),
            });
        }
    }
    for (name, entry) in &config.targets {
        let Ok(TargetBinding::Server { endpoint, .. }) = entry.binding(name) else {
            continue;
        };
        let owner = match (entry.credential.as_deref(), entry.auth) {
            (Some(owner), None) => CredentialOwner::Keyed(owner),
            (None, Some(TargetAuth::Anonymous)) => CredentialOwner::Anonymous { target: name },
            _ => continue,
        };
        let Ok(base) = RequestTarget::of(&endpoint, &endpoint) else {
            continue;
        };
        bindings.push(ServerBinding {
            label: format!("target `{name}`"),
            owner,
            base,
            address: endpoint,
        });
    }
    bindings
}

/// One credential binds to one endpoint scope: an owner never serves two
/// endpoints, an endpoint never has two owners, and an anonymous target never
/// shares an endpoint with a keyed one. Owners also share the token variable
/// namespace with `servers:` names.
fn validate_credential_owners(config: &OperatorConfig) -> Result<()> {
    let conflict = |message: String| CliRefusal::report(ErrorCode::TargetInvalid, message);
    let bindings: Vec<(String, Option<String>, RequestTarget, String)> = server_bindings(config)
        .into_iter()
        .map(|binding| {
            let owner = match binding.owner {
                CredentialOwner::Keyed(owner) => Some(owner.to_string()),
                CredentialOwner::Anonymous { .. } => None,
            };
            (binding.label, owner, binding.base, binding.address)
        })
        .collect();
    let mut owners: BTreeMap<String, (&str, &RequestTarget, &str)> = BTreeMap::new();
    let mut variables: BTreeMap<String, &str> = BTreeMap::new();
    for (label, owner, base, address) in &bindings {
        if let Some(owner) = owner {
            match owners.get(owner) {
                Some((other, other_base, other_address)) if *other_base != base => {
                    return Err(conflict(format!(
                        "{label} and {other} both use credential `{owner}` at different endpoints ({address} and {other_address}); a credential is bound to one endpoint"
                    )));
                }
                Some(_) => {}
                None => {
                    owners.insert(owner.clone(), (label.as_str(), base, address.as_str()));
                }
            }
            if let Some(other) = variables.insert(token_env_name(owner), owner.as_str()) {
                if other != owner {
                    return Err(conflict(format!(
                        "credentials `{other}` and `{owner}` share the token variable {}; rename one",
                        token_env_name(owner)
                    )));
                }
            }
        }
        for (other_label, other_owner, other_base, _) in &bindings {
            if other_label != label && other_base == base && other_owner != owner {
                return Err(conflict(format!(
                    "{label} and {other_label} address the same endpoint ({address}) with different credentials"
                )));
            }
        }
    }
    Ok(())
}

impl OperatorConfig {
    /// The `defaults.target` name, if set.
    pub(crate) fn default_target(&self) -> Option<&str> {
        self.defaults.target.as_deref()
    }

    /// A named target's entry, or `target_unknown` listing the known ones.
    pub(crate) fn target(&self, name: &str) -> Result<&TargetEntry> {
        self.targets.get(name).ok_or_else(|| {
            let known = self
                .targets
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            CliRefusal::report(
                ErrorCode::TargetUnknown,
                format!(
                    "unknown target '{name}' — targets in the operator config: [{known}] (add one with `omnigraph target add {name}`)"
                ),
            )
        })
    }

    /// Whether a token stored under `owner` applies anywhere: `owner` names a
    /// `servers:` entry or is the credential owner of a server target.
    pub(crate) fn declares_credential_owner(&self, owner: &str) -> bool {
        self.servers.contains_key(owner)
            || self.targets.iter().any(|(name, entry)| {
                matches!(
                    entry.binding(name),
                    Ok(TargetBinding::Server {
                        auth: ServerAuth::Token { owner: bound },
                        ..
                    }) if bound == owner
                )
            })
    }

    /// Who holds the credential for a request: the deepest `servers:` entry
    /// or server target whose endpoint the request lies under. A request
    /// under no binding has no owner.
    pub(crate) fn credential_owner_for(
        &self,
        request: &RequestTarget,
    ) -> Option<CredentialOwner<'_>> {
        let mut best: Option<(CredentialOwner<'_>, usize)> = None;
        for binding in server_bindings(self) {
            if request.is_under(&binding.base)
                && best.is_none_or(|(_, depth)| binding.base.depth() > depth)
            {
                best = Some((binding.owner, binding.base.depth()));
            }
        }
        best.map(|(owner, _)| owner)
    }
}

// ---- listing ----

/// `target list --json`. Aliases are not listed: they still name `servers:`
/// entries, not targets.
#[derive(Debug, Serialize)]
pub(crate) struct TargetListOutput {
    pub(crate) protocol: u32,
    pub(crate) default_target: Option<String>,
    pub(crate) targets: Vec<TargetOutput>,
}

/// One target as `target list` and `target show` print it. It never carries a
/// token. An entry that is not a complete binding is listed with its `error`.
#[derive(Debug, Serialize)]
pub(crate) struct TargetOutput {
    pub(crate) name: String,
    pub(crate) kind: TargetKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<TargetScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) default_graph: Option<String>,
    /// `token` or `anonymous`, for a server target.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auth: Option<&'static str>,
    /// The credential owner whose stored token a server target's requests
    /// carry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) credential: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) binding: Option<String>,
    /// A direct target's live local servers, filled from their records;
    /// absent when the records cannot be read safely.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) local_servers: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

impl TargetOutput {
    pub(crate) fn of(name: &str, entry: &TargetEntry) -> Self {
        let mut output = Self {
            name: name.to_string(),
            kind: entry.kind,
            scope: entry.scope,
            root: entry.uri.clone(),
            endpoint: entry.endpoint.clone(),
            default_graph: entry.default_graph.clone(),
            auth: None,
            credential: entry.credential.clone(),
            binding: None,
            local_servers: (entry.kind == TargetKind::Direct).then(Vec::new),
            error: None,
        };
        match entry.binding(name) {
            Ok(binding) => {
                output.binding = Some(binding.digest());
                if let TargetBinding::Server { auth, .. } = &binding {
                    output.auth = Some(match auth {
                        ServerAuth::Token { .. } => "token",
                        ServerAuth::Anonymous => "anonymous",
                    });
                }
            }
            Err(error) => {
                // A binding refuses an address that holds a secret, so only
                // an incomplete entry's address can hold one: print it
                // without userinfo, query or fragment.
                let redact = |value: &Option<String>| {
                    value
                        .as_deref()
                        .map(omnigraph::storage::redacted_storage_uri)
                };
                output.root = redact(&entry.uri);
                output.endpoint = redact(&entry.endpoint);
                output.error = Some(error.to_string());
            }
        }
        output
    }
}

pub(crate) fn list_output(config: &OperatorConfig) -> TargetListOutput {
    TargetListOutput {
        protocol: TARGET_PROTOCOL,
        default_target: config.defaults.target.clone(),
        targets: config
            .targets
            .iter()
            .map(|(name, entry)| TargetOutput::of(name, entry))
            .collect(),
    }
}

/// The one-line human form of a target.
pub(crate) fn write_target_line(
    target: &TargetOutput,
    default: bool,
    out: &mut dyn Write,
) -> Result<()> {
    let address = target
        .endpoint
        .as_deref()
        .or(target.root.as_deref())
        .unwrap_or("-");
    let kind = match (target.kind, target.scope) {
        (TargetKind::Server, _) => "server".to_string(),
        (TargetKind::Direct, Some(TargetScope::Graph)) => "direct graph".to_string(),
        (TargetKind::Direct, Some(TargetScope::Root)) => "direct root".to_string(),
        (TargetKind::Direct, None) => "direct".to_string(),
    };
    write!(
        out,
        "{}{}  {kind}  {address}",
        target.name,
        if default { " (default)" } else { "" }
    )?;
    if let Some(graph) = &target.default_graph {
        write!(out, "  graph {graph}")?;
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
    CliRefusal::report(
        ErrorCode::ConfigNotEditable,
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
        Err(error) if error.downcast_ref::<CliRefusal>().is_some() => return Err(error),
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
    let parent = path
        .parent()
        .ok_or_else(|| eyre!("operator config path has no parent directory"))?;
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension(format!("yaml.tmp.{}", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        if let Ok(metadata) = std::fs::metadata(path) {
            std::fs::set_permissions(&tmp, metadata.permissions())?;
        }
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)?;
        crate::command_outcome::local_effect();
        crate::sync_dir(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
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
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(Self { _file: file })
        }
        #[cfg(not(unix))]
        {
            let _ = config;
            Err(CliRefusal::report(
                ErrorCode::ConfigNotEditable,
                "the operator config cannot be edited safely here: this platform has no file lock to hold across the edit; edit the file by hand on this platform",
            ))
        }
    }
}

#[cfg(test)]
#[path = "targets_tests.rs"]
mod tests;
