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
use std::path::{Path, PathBuf};

use color_eyre::Result;
use color_eyre::eyre::eyre;
use serde::Deserialize;

use omnigraph_server::config::ReadOutputFormat;

pub(crate) const OPERATOR_HOME_ENV: &str = "OMNIGRAPH_HOME";
pub(crate) const OPERATOR_DIR: &str = ".omnigraph";
pub(crate) const OPERATOR_CONFIG_FILE: &str = "config.yaml";

#[derive(Debug, Default, Deserialize)]
pub(crate) struct OperatorConfig {
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
    /// Everything this CLI version doesn't know. Warned once at load,
    /// otherwise ignored (forward compatibility within the operator layer).
    #[serde(flatten)]
    unknown: serde_yaml::Mapping,
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
    pub(crate) params: serde_yaml::Mapping,
    pub(crate) format: Option<ReadOutputFormat>,
    #[serde(flatten)]
    unknown: serde_yaml::Mapping,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OperatorServer {
    pub(crate) url: String,
    #[serde(flatten)]
    unknown: serde_yaml::Mapping,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct OperatorIdentity {
    /// Default actor for every `--as` cascade (CLI direct-engine writes and
    /// cluster commands alike): `--as` > legacy config actor (RFC-008
    /// window) > this > none.
    pub(crate) actor: Option<String>,
    #[serde(flatten)]
    unknown: serde_yaml::Mapping,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct OperatorDefaults {
    /// Default read output format, below every more-specific source.
    pub(crate) output: Option<ReadOutputFormat>,
    #[serde(flatten)]
    unknown: serde_yaml::Mapping,
}

impl OperatorConfig {
    pub(crate) fn actor(&self) -> Option<&str> {
        self.operator.actor.as_deref()
    }

    pub(crate) fn output(&self) -> Option<ReadOutputFormat> {
        self.defaults.output
    }

    /// The gh-host model: which operator server (if any) does this request
    /// URL belong to? Longest-prefix match after trailing-slash
    /// normalization, so `url: http://h:8080` matches
    /// `http://h:8080/graphs/spike` but never `http://h:8080-evil`.
    pub(crate) fn find_server_for_url(&self, request_url: &str) -> Option<&str> {
        let request = request_url.trim_end_matches('/');
        let mut best: Option<(&str, usize)> = None;
        for (name, server) in &self.servers {
            let base = server.url.trim_end_matches('/');
            let matches = request == base
                || request
                    .strip_prefix(base)
                    .is_some_and(|rest| rest.starts_with('/'));
            if matches && best.is_none_or(|(_, len)| base.len() > len) {
                best = Some((name, base.len()));
            }
        }
        best.map(|(name, _)| name)
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

/// Load the operator layer. Absent file (or unresolvable home) is an empty
/// layer, never an error; a present-but-malformed file is a loud error (the
/// operator owns it and can fix it); unknown keys warn to stderr once.
pub(crate) fn load_operator_config() -> Result<OperatorConfig> {
    let Some(dir) = operator_dir() else {
        return Ok(OperatorConfig::default());
    };
    load_operator_config_at(&dir.join(OPERATOR_CONFIG_FILE))
}

pub(crate) fn load_operator_config_at(path: &Path) -> Result<OperatorConfig> {
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
    let config: OperatorConfig = serde_yaml::from_str(&text).map_err(|err| {
        eyre!(
            "could not parse operator config '{}': {err}",
            path.display()
        )
    })?;
    for warning in config.unknown_key_warnings() {
        eprintln!("warning: {warning} in operator config '{}'", path.display());
    }
    Ok(config)
}

impl OperatorConfig {
    fn unknown_key_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        let mut collect = |mapping: &serde_yaml::Mapping, prefix: &str| {
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
        warnings
    }
}

// ---- keyed credentials (RFC-007 §D4) ----

pub(crate) const CREDENTIALS_FILE: &str = "credentials";
const TOKEN_ENV_PREFIX: &str = "OMNIGRAPH_TOKEN_";

pub(crate) fn credentials_path() -> Option<PathBuf> {
    operator_dir().map(|dir| dir.join(CREDENTIALS_FILE))
}

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
/// `Ok(None)` means "no keyed token" — callers fall through to the legacy
/// chain; a present-but-unreadable/over-permissive credentials file is a
/// loud error, never a silent skip.
pub(crate) fn resolve_keyed_token(server: &str) -> Result<Option<String>> {
    if let Ok(token) = env::var(token_env_name(server)) {
        let token = token.trim();
        if !token.is_empty() {
            return Ok(Some(token.to_string()));
        }
    }
    let Some(path) = credentials_path() else {
        return Ok(None);
    };
    read_credential_at(&path, server)
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

/// Write (or rotate) one server's token, preserving every other section.
/// Temp file + rename (#139 finding 7), created 0600.
pub(crate) fn write_credential(server: &str, token: &str) -> Result<PathBuf> {
    let path = credentials_path()
        .ok_or_else(|| eyre!("no home directory resolvable for the credentials file"))?;
    rewrite_credentials_at(&path, server, Some(token))?;
    Ok(path)
}

/// Remove one server's section. Idempotent: absent file or section is fine.
pub(crate) fn remove_credential(server: &str) -> Result<PathBuf> {
    let path = credentials_path()
        .ok_or_else(|| eyre!("no home directory resolvable for the credentials file"))?;
    rewrite_credentials_at(&path, server, None)?;
    Ok(path)
}

pub(crate) fn rewrite_credentials_at(
    path: &Path,
    server: &str,
    token: Option<&str>,
) -> Result<()> {
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
    fn malformed_yaml_is_a_loud_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(&path, "operator: [not, a, mapping\n").unwrap();
        let err = load_operator_config_at(&path).unwrap_err();
        assert!(err.to_string().contains("could not parse operator config"));
    }

    #[test]
    fn find_server_for_url_longest_prefix_no_substring_traps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        fs::write(
            &path,
            "servers:\n  dev:\n    url: http://h:8080\n  dev-spike:\n    url: http://h:8080/graphs/spike\n",
        )
        .unwrap();
        let config = load_operator_config_at(&path).unwrap();
        assert_eq!(config.find_server_for_url("http://h:8080"), Some("dev"));
        assert_eq!(
            config.find_server_for_url("http://h:8080/graphs/other"),
            Some("dev")
        );
        // longest prefix wins
        assert_eq!(
            config.find_server_for_url("http://h:8080/graphs/spike/queries/q"),
            Some("dev-spike")
        );
        // no substring trap: a different port/host must not match
        assert_eq!(config.find_server_for_url("http://h:8080-evil/x"), None);
        assert_eq!(config.find_server_for_url("http://other:9999"), None);
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
