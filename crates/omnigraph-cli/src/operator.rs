//! The operator config surface (RFC-007): `~/.omnigraph/config.yaml` — who
//! the operator IS (identity, ergonomics), never what the system is (that's
//! cluster config) and never a project file (nothing here arrives with a
//! repo checkout).
//!
//! PR-1 scope: `operator.actor` + `defaults.output`. Unknown keys WARN and
//! are preserved-by-ignoring — a file written for a newer CLI (servers,
//! aliases, credentials keys from later slices) must load cleanly on this
//! one. Contrast with `cluster.yaml`, where unknown keys are fatal because
//! they change what a plan means.
//!
//! This module is CLI-only by design: the server never reads operator
//! config (server-side identity comes from bearer auth — invariant 11
//! holds by construction).

use std::collections::BTreeMap;
use std::env;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use serde::Deserialize;

use crate::read_format::{ReadOutputFormat, TableCellLayout};

pub(crate) const OPERATOR_HOME_ENV: &str = "OMNIGRAPH_HOME";
pub(crate) const OPERATOR_DIR: &str = ".omnigraph";
pub(crate) const OPERATOR_CONFIG_FILE: &str = "config.yaml";

#[derive(Debug, Default, Deserialize)]
pub(crate) struct OperatorConfig {
    /// The targets format version; required once `targets:` or
    /// `defaults.target` is present.
    #[serde(default)]
    pub(crate) target_config_version: Option<u32>,
    /// Named targets: each one complete destination binding, selected by
    /// `--target` or `defaults.target`. No tokens in this file, ever.
    #[serde(default)]
    pub(crate) targets: BTreeMap<String, crate::targets::TargetEntry>,
    #[serde(default)]
    pub(crate) operator: OperatorIdentity,
    #[serde(default)]
    pub(crate) defaults: OperatorDefaults,
    /// Operator-owned endpoint definitions (RFC-007 §D2/§D4): name → url.
    /// The name keys the credential chain; nothing a repo checkout supplies
    /// can redefine an entry here. No tokens in this file, ever.
    #[serde(default)]
    pub(crate) servers: BTreeMap<String, OperatorServer>,
    /// Personal alias bindings (RFC-007 PR 3); see OperatorAlias.
    #[serde(default)]
    pub(crate) aliases: BTreeMap<String, OperatorAlias>,
    /// Named scope bundles (RFC-011): each binds exactly one of
    /// {server, cluster, store} plus an optional default graph. Config data,
    /// not state — selecting one (`--profile`/`OMNIGRAPH_PROFILE`) fills in a
    /// command's omitted addressing; it never puts you "in" a mode.
    #[serde(default)]
    pub(crate) profiles: BTreeMap<String, OperatorProfile>,
    /// Managed-cluster storage roots (RFC-011): name → root URI. The ONLY
    /// place a storage root appears in operator config — admin-only and
    /// opt-in; a normal operator's file has none.
    #[serde(default)]
    pub(crate) clusters: BTreeMap<String, OperatorCluster>,
    /// Everything this CLI version doesn't know. Warned once at load,
    /// otherwise ignored (forward compatibility within the operator layer).
    #[serde(flatten)]
    unknown: yaml_serde::Mapping,
}

/// A personal alias: a pure BINDING to a stored query on a named server —
/// never content, never a file (RFC-007 §D2 "Aliases are bindings, not
/// content"). The stored query is the team's contract; the alias, its
/// defaults, and its name are the operator's.
#[derive(Debug, Deserialize)]
pub(crate) struct OperatorAlias {
    /// Names an entry under `servers:`.
    pub(crate) server: String,
    /// Graph id for multi-graph servers (appends `/graphs/<id>`).
    pub(crate) graph: Option<String>,
    /// The STORED query's name on that server.
    pub(crate) query: String,
    /// Positional CLI args bind to these param names, in order.
    #[serde(default)]
    pub(crate) args: Vec<String>,
    /// Fixed default params; positionals and `--params` override per key.
    #[serde(default)]
    pub(crate) params: yaml_serde::Mapping,
    pub(crate) format: Option<ReadOutputFormat>,
    #[serde(flatten)]
    unknown: yaml_serde::Mapping,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OperatorServer {
    pub(crate) url: String,
    #[serde(flatten)]
    unknown: yaml_serde::Mapping,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct OperatorIdentity {
    /// Default actor for every `--as` cascade (CLI direct-engine writes and
    /// cluster commands alike): `--as` > this > none.
    pub(crate) actor: Option<String>,
    #[serde(flatten)]
    unknown: yaml_serde::Mapping,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct OperatorDefaults {
    /// Default read output format, below every more-specific source.
    pub(crate) output: Option<ReadOutputFormat>,
    /// Table rendering preferences for `--format table`.
    pub(crate) table_max_column_width: Option<usize>,
    pub(crate) table_cell_layout: Option<TableCellLayout>,
    /// Default server scope (RFC-011): the everyday addressing when no
    /// `--profile` / primitive / legacy address is given. Names an entry
    /// under `servers:`. Mutually exclusive with `store` — a scope binds one
    /// entity.
    pub(crate) server: Option<String>,
    /// Default **store** scope (RFC-011): a `file://` / `s3://` / `az://` graph storage
    /// URI used as the zero-flag local default for graph commands when no
    /// `--profile` / primitive address is given. The local-dev counterpart of
    /// `server`; mutually exclusive with it.
    pub(crate) store: Option<String>,
    /// Default graph selected within a server/cluster scope when no
    /// `--graph` is passed (RFC-011).
    pub(crate) default_graph: Option<String>,
    /// Default target: names an entry under `targets:`, used when a command
    /// names no address. Exclusive with `server`, `store` and
    /// `default_graph`.
    pub(crate) target: Option<String>,
    #[serde(flatten)]
    unknown: yaml_serde::Mapping,
}

/// A named scope bundle (RFC-011): exactly one of {server, cluster, store}
/// plus an optional default graph. Validated on use (`binding()`), not at
/// parse time, so an unknown CLI's profile still loads.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct OperatorProfile {
    /// Names an entry under `servers:` — a served scope.
    pub(crate) server: Option<String>,
    /// Names an entry under `clusters:` — a privileged direct cluster scope.
    pub(crate) cluster: Option<String>,
    /// A single graph's storage URI — a direct store scope.
    pub(crate) store: Option<String>,
    /// Default graph within a server/cluster scope (ignored for a store,
    /// which is already one graph).
    pub(crate) default_graph: Option<String>,
    #[serde(flatten)]
    unknown: yaml_serde::Mapping,
}

/// A managed-cluster storage root (RFC-011).
#[derive(Debug, Default, Deserialize)]
pub(crate) struct OperatorCluster {
    /// The cluster's storage-root URI (`file://` / `s3://` / `az://`).
    pub(crate) root: String,
    #[serde(flatten)]
    unknown: yaml_serde::Mapping,
}

/// The one entity a profile (or flat default) binds. Exactly one variant —
/// the scope resolver consumes this; "exactly one of server/cluster/store"
/// is enforced when producing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScopeBinding {
    /// Served scope: a server name (resolved against `servers:`) or a literal URL.
    Server(String),
    /// Direct cluster scope: a cluster name (resolved against `clusters:`) or a
    /// literal root URI.
    Cluster(String),
    /// Direct store scope: a single graph's storage URI.
    Store(String),
}

impl OperatorConfig {
    pub(crate) fn actor(&self) -> Option<&str> {
        self.operator.actor.as_deref()
    }

    pub(crate) fn output(&self) -> Option<ReadOutputFormat> {
        self.defaults.output
    }

    /// A named profile, if defined (RFC-011).
    pub(crate) fn profile(&self, name: &str) -> Option<&OperatorProfile> {
        self.profiles.get(name)
    }

    /// The storage root of a named cluster, if defined (RFC-011).
    pub(crate) fn cluster_root(&self, name: &str) -> Option<&str> {
        self.clusters.get(name).map(|c| c.root.as_str())
    }

    /// The flat-default server scope name, if set (RFC-011).
    pub(crate) fn default_server(&self) -> Option<&str> {
        self.defaults.server.as_deref()
    }

    /// The flat-default store scope URI, if set (RFC-011) — the zero-flag
    /// local-dev default.
    pub(crate) fn default_store(&self) -> Option<&str> {
        self.defaults.store.as_deref()
    }

    /// The flat-default graph within a server/cluster scope, if set (RFC-011).
    pub(crate) fn default_graph(&self) -> Option<&str> {
        self.defaults.default_graph.as_deref()
    }

    /// A scope binds one entity (Decision 6): `defaults.server` and
    /// `defaults.store` are mutually exclusive, and a `store` (already a single
    /// graph) cannot carry a `default_graph`. Both are refused loudly rather
    /// than silently dropped.
    fn validate_defaults(&self) -> Result<()> {
        if self.defaults.server.is_some() && self.defaults.store.is_some() {
            bail!(
                "operator config `defaults` sets both `server` and `store` — a default scope \
                 binds one entity; keep one (use a `profile` if you need both)"
            );
        }
        if self.defaults.store.is_some() && self.defaults.default_graph.is_some() {
            bail!(
                "operator config `defaults` sets both `store` and `default_graph` — a store is \
                 already a single graph; drop `default_graph` (it applies only to a server/cluster scope)"
            );
        }
        Ok(())
    }

    /// Each server's token variable is its own: `a-b` and `a_b`, or `prod`
    /// and `PROD`, spell one `OMNIGRAPH_TOKEN_<NAME>`, which would bind one
    /// token to both servers. Such names are refused loudly, naming both.
    fn validate_server_token_names(&self) -> Result<()> {
        let mut owners: BTreeMap<String, &str> = BTreeMap::new();
        for name in self.servers.keys() {
            if let Some(other) = owners.insert(token_env_name(name), name) {
                bail!(
                    "operator config `servers` entries `{other}` and `{name}` share the token \
                     variable {}; rename one so each server has its own credential",
                    token_env_name(name)
                );
            }
        }
        Ok(())
    }
}

impl OperatorProfile {
    /// The single entity this profile binds, or a loud error if it binds zero
    /// or more than one of {server, cluster, store} (Decision 6: a scope binds
    /// exactly one entity). Validated here, on use, rather than at parse time.
    pub(crate) fn binding(&self, profile_name: &str) -> Result<ScopeBinding> {
        let set: Vec<&str> = [
            self.server.as_ref().map(|_| "server"),
            self.cluster.as_ref().map(|_| "cluster"),
            self.store.as_ref().map(|_| "store"),
        ]
        .into_iter()
        .flatten()
        .collect();
        match set.as_slice() {
            ["server"] => Ok(ScopeBinding::Server(self.server.clone().unwrap())),
            ["cluster"] => Ok(ScopeBinding::Cluster(self.cluster.clone().unwrap())),
            ["store"] => Ok(ScopeBinding::Store(self.store.clone().unwrap())),
            [] => Err(eyre!(
                "profile '{profile_name}' binds no scope; set exactly one of \
                 `server`, `cluster`, or `store`"
            )),
            many => Err(eyre!(
                "profile '{profile_name}' binds {} scopes ({}); a profile must \
                 bind exactly one of `server`, `cluster`, or `store`",
                many.len(),
                many.join(", ")
            )),
        }
    }
}

/// The operator dir: `$OMNIGRAPH_HOME` if set (tilde-expanded), else
/// `~/.omnigraph`. Returns None when no home directory is resolvable
/// (degenerate environments — the layer is simply absent).
pub(crate) fn operator_dir() -> Option<PathBuf> {
    if let Some(home_override) = env::var_os(OPERATOR_HOME_ENV) {
        let raw = home_override.to_string_lossy().into_owned();
        return Some(expand_tilde(&raw));
    }
    env::home_dir().map(|home| home.join(OPERATOR_DIR))
}

/// Configuration inputs selected once for this invocation. Reading the file is
/// lazy so wrong-mode and argument refusals still precede configuration access.
/// Only routing/configuration is retained; managed token renewal stays with its
/// existing credential owner.
#[derive(Debug)]
pub(crate) struct InvocationConfig {
    directory: Option<PathBuf>,
    /// `--target`, captured with the command line.
    target: Option<String>,
    profile: Option<String>,
    profile_present: bool,
    captured: OnceLock<CapturedOperator>,
    warnings: OnceLock<std::result::Result<(), (io::ErrorKind, String)>>,
}

#[derive(Debug)]
struct CapturedOperator {
    config: std::result::Result<OperatorConfig, CapturedRefusal>,
    warnings: Vec<u8>,
}

/// A load failure kept for every later read of the capture, with the code a
/// typed refusal carried, so each read reports it as the load did.
#[derive(Debug)]
struct CapturedRefusal {
    code: Option<omnigraph_api_types::ErrorCode>,
    message: String,
}

impl InvocationConfig {
    pub(crate) fn capture() -> Self {
        let profile = env::var_os(crate::scope::PROFILE_ENV);
        let mut input = Self::at(
            operator_dir(),
            profile
                .as_ref()
                .and_then(|value| value.to_str())
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
        );
        // Managed ambiguity detection also rejects a non-Unicode ambient
        // profile, while the ordinary profile resolver cannot select that name.
        input.profile_present = profile.is_some_and(|value| !value.is_empty());
        input
    }

    pub(crate) fn at(directory: Option<PathBuf>, profile: Option<String>) -> Self {
        Self {
            directory,
            target: None,
            profile_present: profile.as_ref().is_some_and(|value| !value.is_empty()),
            profile,
            captured: OnceLock::new(),
            warnings: OnceLock::new(),
        }
    }

    /// Select `--target` for this invocation.
    pub(crate) fn with_target(mut self, target: Option<String>) -> Self {
        self.target = target;
        self
    }

    pub(crate) fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }

    pub(crate) fn has_profile(&self) -> bool {
        self.profile_present
    }

    pub(crate) fn profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    pub(crate) fn config_path(&self) -> Option<PathBuf> {
        self.directory
            .as_ref()
            .map(|dir| dir.join(OPERATOR_CONFIG_FILE))
    }

    pub(crate) fn credentials_path(&self) -> Option<PathBuf> {
        self.directory
            .as_ref()
            .map(|dir| dir.join(CREDENTIALS_FILE))
    }

    fn captured(&self, stderr: &mut dyn Write) -> Result<&CapturedOperator> {
        let captured = self.captured.get_or_init(|| {
            let mut warnings = Vec::new();
            let config = match &self.directory {
                Some(dir) => load_operator_config_at_with_writer(
                    &dir.join(OPERATOR_CONFIG_FILE),
                    &mut warnings,
                ),
                None => Ok(OperatorConfig::default()),
            }
            .map_err(|error| CapturedRefusal {
                code: error
                    .downcast_ref::<crate::helpers::CliRefusal>()
                    .map(|refusal| refusal.code),
                message: error.to_string(),
            });
            CapturedOperator { config, warnings }
        });
        let warnings = self.warnings.get_or_init(|| {
            if captured.warnings.is_empty() {
                return Ok(());
            }
            stderr
                .write_all(&captured.warnings)
                .and_then(|()| stderr.flush())
                .map_err(|error| (error.kind(), error.to_string()))
        });
        if let Err((kind, message)) = warnings {
            return Err(io::Error::new(*kind, message.clone()).into());
        }
        Ok(captured)
    }

    pub(crate) fn get(&self, stderr: &mut dyn Write) -> Result<&OperatorConfig> {
        self.captured(stderr)?
            .config
            .as_ref()
            .map_err(|refusal| match refusal.code {
                Some(code) => crate::helpers::CliRefusal::report(code, refusal.message.clone()),
                None => eyre!(refusal.message.clone()),
            })
    }

    /// Existing managed query presentation tolerates an invalid optional config.
    /// Diagnostic delivery failures still propagate; they are not config absence.
    pub(crate) fn optional_get(&self, stderr: &mut dyn Write) -> Result<Option<&OperatorConfig>> {
        Ok(self.captured(stderr)?.config.as_ref().ok())
    }
}

#[cfg(test)]
pub(crate) fn load_operator_config_at(path: &Path) -> Result<OperatorConfig> {
    load_operator_config_at_with_writer(path, &mut io::sink())
}

/// Load the operator layer. Absent file (or unresolvable home) is an empty
/// layer, never an error; a present-but-malformed file is a loud error (the
/// operator owns it and can fix it); unknown keys warn to stderr once.
fn load_operator_config_at_with_writer(
    path: &Path,
    stderr: &mut dyn Write,
) -> Result<OperatorConfig> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(OperatorConfig::default());
        }
        Err(err) => {
            return Err(eyre!(
                "could not read operator config '{}': {err}",
                path.display()
            ));
        }
    };
    let config: OperatorConfig = yaml_serde::from_str(&text).map_err(|err| {
        eyre!(
            "could not parse operator config '{}': {err}",
            path.display()
        )
    })?;
    for warning in config.unknown_key_warnings() {
        writeln!(
            stderr,
            "warning: {warning} in operator config '{}'",
            path.display()
        )?;
    }
    config.validate()?;
    Ok(config)
}

/// Parse and check operator config text, as loading the file does, without
/// its unknown-key warnings.
pub(crate) fn parse_operator_config(text: &str) -> Result<OperatorConfig> {
    let config: OperatorConfig = if text.trim().is_empty() {
        OperatorConfig::default()
    } else {
        yaml_serde::from_str(text)?
    };
    config.validate()?;
    Ok(config)
}

impl OperatorConfig {
    fn validate(&self) -> Result<()> {
        self.validate_defaults()?;
        self.validate_server_token_names()?;
        crate::targets::validate_config(self)
    }

    fn unknown_key_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        let mut collect = |mapping: &yaml_serde::Mapping, prefix: &str| {
            for key in mapping.keys() {
                if let Some(name) = key.as_str() {
                    warnings.push(format!(
                        "unknown key `{prefix}{name}` (newer CLI feature or typo); ignored"
                    ));
                }
            }
        };
        collect(&self.unknown, "");
        collect(&self.operator.unknown, "operator.");
        collect(&self.defaults.unknown, "defaults.");
        for (name, server) in &self.servers {
            collect(&server.unknown, &format!("servers.{name}."));
        }
        for (name, alias) in &self.aliases {
            collect(&alias.unknown, &format!("aliases.{name}."));
        }
        for (name, profile) in &self.profiles {
            collect(&profile.unknown, &format!("profiles.{name}."));
        }
        for (name, cluster) in &self.clusters {
            collect(&cluster.unknown, &format!("clusters.{name}."));
        }
        warnings
    }
}

// ---- keyed credentials (RFC-007 §D4) ----

pub(crate) const CREDENTIALS_FILE: &str = "credentials";
const TOKEN_ENV_PREFIX: &str = "OMNIGRAPH_TOKEN_";

/// `intel-dev` → `OMNIGRAPH_TOKEN_INTEL_DEV`.
pub(crate) fn token_env_name(server: &str) -> String {
    let mut name = String::from(TOKEN_ENV_PREFIX);
    for c in server.chars() {
        name.push(match c {
            '-' => '_',
            other => other.to_ascii_uppercase(),
        });
    }
    name
}

/// The keyed token chain for a named server (§D4 steps 1–2):
/// `OMNIGRAPH_TOKEN_<NAME>` env → `[<name>]` in the credentials file.
/// `Ok(None)` means "no keyed token": the request carries no credential. A
/// present-but-unreadable/over-permissive credentials file is a loud error,
/// never a silent skip.
pub(crate) fn resolve_keyed_token_at(server: &str, path: Option<&Path>) -> Result<Option<String>> {
    if let Ok(token) = env::var(token_env_name(server)) {
        let token = token.trim();
        if !token.is_empty() {
            return Ok(Some(token.to_string()));
        }
    }
    let Some(path) = path else {
        return Ok(None);
    };
    read_credential_at(path, server)
}

pub(crate) fn read_credential_at(path: &Path, server: &str) -> Result<Option<String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(eyre!(
                "could not read credentials file '{}': {err}",
                path.display()
            ));
        }
    };
    refuse_over_permissive(path)?;
    let mut in_section = false;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(section) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            in_section = section.trim() == server;
            continue;
        }
        if in_section {
            if let Some((key, value)) = line.split_once('=') {
                if key.trim() == "token" {
                    let value = unquote(value.trim());
                    if value.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(value.to_string()));
                }
            }
        }
    }
    Ok(None)
}

pub(crate) fn rewrite_credentials_at(path: &Path, server: &str, token: Option<&str>) -> Result<()> {
    let existing = match std::fs::read_to_string(path) {
        Ok(text) => {
            refuse_over_permissive(path)?;
            text
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(err) => {
            return Err(eyre!(
                "could not read credentials file '{}': {err}",
                path.display()
            ));
        }
    };

    // Drop the target section (if present), keep everything else verbatim.
    let mut out = String::new();
    let mut in_target = false;
    for line in existing.lines() {
        let trimmed = line.trim();
        if let Some(section) = trimmed.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            in_target = section.trim() == server;
            if in_target {
                continue;
            }
        }
        if !in_target {
            out.push_str(line);
            out.push('\n');
        }
    }
    if let Some(token) = token {
        if !out.is_empty() && !out.ends_with("\n\n") {
            out.push('\n');
        }
        out.push_str(&format!("[{server}]\ntoken = {token}\n"));
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    write_owner_only(&tmp, &out)?;
    std::fs::rename(&tmp, path).map_err(|err| {
        let _ = std::fs::remove_file(&tmp);
        eyre!(
            "could not move credentials file into place '{}': {err}",
            path.display()
        )
    })?;
    Ok(())
}

#[cfg(unix)]
fn write_owner_only(path: &Path, content: &str) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(content.as_bytes())?;
    Ok(())
}

#[cfg(not(unix))]
fn write_owner_only(path: &Path, content: &str) -> Result<()> {
    std::fs::write(path, content)?;
    Ok(())
}

/// Secrets are operator-private: refuse a credentials file other accounts
/// can read (the chain errs loudly rather than using a leaked secret).
#[cfg(unix)]
fn refuse_over_permissive(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)?.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(eyre!(
            "credentials file '{}' is group/world-accessible (mode {:o}); run `chmod 600 {}`",
            path.display(),
            mode & 0o777,
            path.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn refuse_over_permissive(_path: &Path) -> Result<()> {
    Ok(())
}

fn unquote(value: &str) -> &str {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

/// Expand a leading `~` / `~/` to the home directory (PR #139 finding 9:
/// a literal `./~/…` path silently created a directory named `~`).
pub(crate) fn expand_tilde(raw: &str) -> PathBuf {
    if raw == "~" {
        return env::home_dir().unwrap_or_else(|| PathBuf::from(raw));
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = env::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_http::RequestTarget;
    use std::fs;

    #[test]
    fn absent_file_is_an_empty_layer() {
        let dir = tempfile::tempdir().unwrap();
        let config = load_operator_config_at(&dir.path().join("config.yaml")).unwrap();
        assert!(config.actor().is_none());
        assert!(config.output().is_none());
    }

    #[test]
    fn parses_identity_and_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(
            &path,
            "operator:\n  actor: act-andrew\ndefaults:\n  output: json\n",
        )
        .unwrap();
        let config = load_operator_config_at(&path).unwrap();
        assert_eq!(config.actor(), Some("act-andrew"));
        assert_eq!(config.output(), Some(ReadOutputFormat::Json));
    }

    #[test]
    fn defaults_store_parses_and_is_accessible() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, "defaults:\n  store: file:///tmp/dev.omni\n").unwrap();
        let config = load_operator_config_at(&path).unwrap();
        assert_eq!(config.default_store(), Some("file:///tmp/dev.omni"));
        assert_eq!(config.default_server(), None);
    }

    #[test]
    fn defaults_server_and_store_together_is_a_loud_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(
            &path,
            "defaults:\n  server: prod\n  store: file:///tmp/dev.omni\n",
        )
        .unwrap();
        let err = load_operator_config_at(&path).unwrap_err().to_string();
        assert!(err.contains("binds one entity"), "{err}");
    }

    #[test]
    fn defaults_store_with_default_graph_is_a_loud_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(
            &path,
            "defaults:\n  store: file:///tmp/dev.omni\n  default_graph: knowledge\n",
        )
        .unwrap();
        let err = load_operator_config_at(&path).unwrap_err().to_string();
        assert!(err.contains("already a single graph"), "{err}");
    }

    #[test]
    fn invocation_config_is_lazy_and_retains_a_failed_snapshot() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(OPERATOR_CONFIG_FILE);
        fs::write(&path, "servers: [\n").unwrap();
        let input = InvocationConfig::at(Some(home.path().to_owned()), Some("selected".into()));
        assert!(input.captured.get().is_none());
        assert_eq!(
            input.credentials_path(),
            Some(home.path().join(CREDENTIALS_FILE))
        );
        assert_eq!(input.profile(), Some("selected"));
        assert!(input.has_profile());
        let error = input.get(&mut Vec::new()).unwrap_err().to_string();
        fs::write(&path, "defaults: { store: graph.omni }\n").unwrap();
        assert_eq!(input.get(&mut Vec::new()).unwrap_err().to_string(), error);
        assert!(input.optional_get(&mut Vec::new()).unwrap().is_none());
        let fresh = InvocationConfig::at(Some(home.path().to_owned()), None);
        assert_eq!(
            fresh.get(&mut Vec::new()).unwrap().default_store(),
            Some("graph.omni")
        );
    }

    #[test]
    fn invocation_warning_failure_is_not_config_absence_or_retried_delivery() {
        struct FailedWarning {
            flush: bool,
            writes: usize,
        }
        impl Write for FailedWarning {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.writes += 1;
                if self.flush {
                    Ok(bytes.len())
                } else {
                    Err(io::ErrorKind::BrokenPipe.into())
                }
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
        }
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(OPERATOR_CONFIG_FILE);
        for flush in [false, true] {
            fs::write(&path, "future: true\ndefaults: { store: original.omni }\n").unwrap();
            let input = InvocationConfig::at(Some(home.path().to_owned()), None);
            let mut stderr = FailedWarning { flush, writes: 0 };
            let error = input.get(&mut stderr).unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(stderr.writes, 1);
            fs::write(&path, "defaults: { store: replacement.omni }\n").unwrap();
            let mut retry = Vec::new();
            let error = input.optional_get(&mut retry).unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert!(retry.is_empty());
            assert_eq!(
                input
                    .captured
                    .get()
                    .unwrap()
                    .config
                    .as_ref()
                    .unwrap()
                    .default_store(),
                Some("original.omni")
            );
        }
    }

    #[test]
    fn unknown_keys_warn_but_load() {
        // A file written for a later slice (servers/aliases) must load
        // cleanly today — warn-only forward compatibility.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(
            &path,
            "operator:\n  actor: act-a\n  color: green\nservers:\n  prod:\n    url: https://example.com\naliases: {}\n",
        )
        .unwrap();
        let config = load_operator_config_at(&path).unwrap();
        assert_eq!(config.actor(), Some("act-a"));
        let warnings = config.unknown_key_warnings();
        // `servers` (PR 2) and `aliases` (PR 3) are known keys now.
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings.iter().any(|w| w.contains("`operator.color`")));
        assert_eq!(config.servers["prod"].url, "https://example.com");
    }

    #[test]
    fn parses_profiles_clusters_and_scope_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        let yaml = "\
defaults:
  server: prod
  default_graph: knowledge
servers:
  prod:
    url: https://example.com
clusters:
  brain:
    root: s3://acme/clusters/brain
profiles:
  staging:
    server: staging
    default_graph: knowledge
  brain-admin:
    cluster: brain
    default_graph: knowledge
";
        fs::write(&path, yaml).unwrap();
        let config = load_operator_config_at(&path).unwrap();
        assert_eq!(config.default_server(), Some("prod"));
        assert_eq!(config.default_graph(), Some("knowledge"));
        assert_eq!(
            config.cluster_root("brain"),
            Some("s3://acme/clusters/brain")
        );
        assert_eq!(
            config
                .profile("staging")
                .unwrap()
                .binding("staging")
                .unwrap(),
            ScopeBinding::Server("staging".into())
        );
        assert_eq!(
            config
                .profile("brain-admin")
                .unwrap()
                .binding("brain-admin")
                .unwrap(),
            ScopeBinding::Cluster("brain".into())
        );
        // No unknown-key warnings for the new blocks.
        assert!(
            config.unknown_key_warnings().is_empty(),
            "{:?}",
            config.unknown_key_warnings()
        );
    }

    #[test]
    fn profile_binding_rejects_zero_or_multiple_entities() {
        let none = OperatorProfile::default();
        let err = none.binding("p").unwrap_err().to_string();
        assert!(err.contains("binds no scope"), "{err}");

        let two = OperatorProfile {
            server: Some("prod".into()),
            store: Some("graph.omni".into()),
            ..Default::default()
        };
        let err = two.binding("p").unwrap_err().to_string();
        assert!(err.contains("binds 2 scopes"), "{err}");
        assert!(err.contains("server") && err.contains("store"), "{err}");
    }

    #[test]
    fn unknown_keys_in_a_profile_warn() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(
            &path,
            "profiles:\n  p:\n    server: prod\n    flavour: spicy\n",
        )
        .unwrap();
        let config = load_operator_config_at(&path).unwrap();
        let warnings = config.unknown_key_warnings();
        assert!(
            warnings.iter().any(|w| w.contains("`profiles.p.flavour`")),
            "{warnings:?}"
        );
    }

    #[test]
    fn malformed_yaml_is_a_loud_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, "operator: [not, a, mapping\n").unwrap();
        let err = load_operator_config_at(&path).unwrap_err();
        assert!(err.to_string().contains("could not parse operator config"));
    }

    #[test]
    fn servers_sharing_a_token_variable_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        for (first, second) in [("a-b", "a_b"), ("prod", "PROD")] {
            fs::write(
                &path,
                format!(
                    "servers:\n  {first}:\n    url: http://h:1\n  {second}:\n    url: http://h:2\n"
                ),
            )
            .unwrap();
            let err = load_operator_config_at(&path).unwrap_err().to_string();
            assert!(
                err.contains(&format!(
                    "share the token variable {}",
                    token_env_name(first)
                )),
                "{err}"
            );
            assert!(err.contains(first) && err.contains(second), "{err}");
        }
        fs::write(
            &path,
            "servers:\n  a-b:\n    url: http://h:1\n  ab:\n    url: http://h:2\n",
        )
        .unwrap();
        load_operator_config_at(&path).unwrap();
    }

    fn keyed(owner: crate::targets::CredentialOwner<'_>) -> Option<&str> {
        match owner {
            crate::targets::CredentialOwner::Keyed(name) => Some(name),
            crate::targets::CredentialOwner::Anonymous { .. } => None,
        }
    }

    #[test]
    fn find_server_for_target_matches_where_the_request_is_sent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(
            &path,
            "servers:\n  dev:\n    url: http://h:8080\n  dev-spike:\n    url: http://h:8080/graphs/spike\n  \
               tenant:\n    url: https://t.example/tenant-a/\n  og:\n    url: unix:///tmp/og/og.sock\n",
        )
        .unwrap();
        let config = load_operator_config_at(&path).unwrap();
        let server = |url: &str| {
            let target = RequestTarget::of(url, url).unwrap();
            config.credential_owner_for(&target).and_then(keyed)
        };
        assert_eq!(server("http://h:8080"), Some("dev"));
        assert_eq!(server("http://H:8080/graphs/other"), Some("dev"));
        // The deepest base wins.
        assert_eq!(
            server("http://h:8080/graphs/spike/queries/q"),
            Some("dev-spike")
        );
        assert_eq!(server("http://h:8080/graphs/spike/../other"), Some("dev"));
        assert_eq!(
            server("http://h:8080/graphs/x/../spike/queries/q"),
            Some("dev-spike")
        );
        // A different host, port or scheme is a different server; a port
        // that does not parse addresses nothing.
        assert!(RequestTarget::of("http://h:8080-evil/x", "http://h:8080-evil/x").is_err());
        assert_eq!(server("http://h-evil:8080/x"), None);
        assert_eq!(server("http://other:9999"), None);
        assert_eq!(server("https://h:8080"), None);
        // The default port is the same endpoint; a sibling prefix is not.
        assert_eq!(
            server("https://t.example:443/tenant-a/graphs/g"),
            Some("tenant")
        );
        assert_eq!(server("https://t.example/tenant-ab/graphs/g"), None);
        assert_eq!(server("https://t.example/tenant-a/../tenant-b"), None);
        assert_eq!(server("https://t.example/tenant-a/%2e%2e/tenant-b"), None);
        // A socket is matched by the path the transport connects to.
        #[cfg(unix)]
        {
            let socket = |root: &str| {
                let target = RequestTarget::of(root, &format!("{root}/graphs/g")).unwrap();
                config.credential_owner_for(&target).and_then(keyed)
            };
            assert_eq!(socket("unix:///tmp/og/og.sock"), Some("og"));
            assert_eq!(socket("unix:///tmp/og/./og.sock"), Some("og"));
            assert_eq!(socket("unix:///tmp/og/og.sock/../evil.sock"), None);
        }
    }

    #[test]
    fn server_lookup_supports_targeting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(
            &path,
            "servers:\n  intel-dev:\n    url: http://127.0.0.1:8080/\n",
        )
        .unwrap();
        let config = load_operator_config_at(&path).unwrap();
        // the --server resolution shape: bare url and graph-scoped url
        let base = config.servers["intel-dev"].url.trim_end_matches('/');
        assert_eq!(base, "http://127.0.0.1:8080");
        assert_eq!(
            format!("{base}/graphs/spike"),
            "http://127.0.0.1:8080/graphs/spike"
        );
    }

    #[test]
    fn token_env_name_uppercases_and_underscores() {
        assert_eq!(token_env_name("intel-dev"), "OMNIGRAPH_TOKEN_INTEL_DEV");
        assert_eq!(token_env_name("prod"), "OMNIGRAPH_TOKEN_PROD");
    }

    #[test]
    fn credentials_roundtrip_rotate_remove_preserving_other_sections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");

        rewrite_credentials_at(&path, "prod", Some("tok-1")).unwrap();
        rewrite_credentials_at(&path, "dev", Some("tok-dev")).unwrap();
        assert_eq!(
            read_credential_at(&path, "prod").unwrap().as_deref(),
            Some("tok-1")
        );

        // rotate prod; dev preserved
        rewrite_credentials_at(&path, "prod", Some("tok-2")).unwrap();
        assert_eq!(
            read_credential_at(&path, "prod").unwrap().as_deref(),
            Some("tok-2")
        );
        assert_eq!(
            read_credential_at(&path, "dev").unwrap().as_deref(),
            Some("tok-dev")
        );

        // remove prod; dev preserved; removal is idempotent
        rewrite_credentials_at(&path, "prod", None).unwrap();
        rewrite_credentials_at(&path, "prod", None).unwrap();
        assert_eq!(read_credential_at(&path, "prod").unwrap(), None);
        assert_eq!(
            read_credential_at(&path, "dev").unwrap().as_deref(),
            Some("tok-dev")
        );
    }

    #[cfg(unix)]
    #[test]
    fn credentials_written_0600_and_over_permissive_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials");
        rewrite_credentials_at(&path, "prod", Some("tok")).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "written {:o}", mode & 0o777);

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let err = read_credential_at(&path, "prod").unwrap_err();
        assert!(err.to_string().contains("chmod 600"), "{err}");
    }

    #[test]
    fn expand_tilde_resolves_home_prefix() {
        let home = env::home_dir().unwrap();
        assert_eq!(expand_tilde("~"), home);
        assert_eq!(expand_tilde("~/x/y"), home.join("x/y"));
        assert_eq!(expand_tilde("/abs/path"), PathBuf::from("/abs/path"));
        assert_eq!(expand_tilde("rel/path"), PathBuf::from("rel/path"));
    }
}
