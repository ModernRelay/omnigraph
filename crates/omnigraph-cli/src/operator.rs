//! Versioned operator configuration and endpoint-bound local credentials.
//! Routing is captured lazily once per invocation. Legacy routing is accepted
//! only by the explicit, offline `target import` migration.

use std::collections::BTreeMap;
use std::env;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use serde::{Deserialize, Serialize};

use crate::read_format::{ReadOutputFormat, TableCellLayout};

pub(crate) const OPERATOR_HOME_ENV: &str = "OMNIGRAPH_HOME";
pub(crate) const OPERATOR_DIR: &str = ".omnigraph";
pub(crate) const OPERATOR_CONFIG_FILE: &str = "config.yaml";
pub(crate) const OPERATOR_FILE_MAX_BYTES: usize = 1024 * 1024;
pub(crate) const RETIRED_PROFILE_ENV: &str = "OMNIGRAPH_PROFILE";

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorConfig {
    pub(crate) target_config_version: Option<u32>,
    #[serde(default)]
    pub(crate) targets: BTreeMap<String, crate::targets::TargetEntry>,
    #[serde(default)]
    pub(crate) operator: OperatorIdentity,
    #[serde(default)]
    pub(crate) defaults: OperatorDefaults,
    #[serde(default)]
    pub(crate) aliases: BTreeMap<String, OperatorAlias>,
    #[serde(skip)]
    pub(crate) base_dir: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorAlias {
    pub(crate) target: String,
    pub(crate) graph: Option<String>,
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    #[serde(default)]
    pub(crate) params: yaml_serde::Mapping,
    pub(crate) format: Option<ReadOutputFormat>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorIdentity {
    pub(crate) actor: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorDefaults {
    pub(crate) output: Option<ReadOutputFormat>,
    pub(crate) table_max_column_width: Option<usize>,
    pub(crate) table_cell_layout: Option<TableCellLayout>,
    pub(crate) target: Option<String>,
}

impl OperatorConfig {
    pub(crate) fn alias(&self, name: &str) -> Result<&OperatorAlias> {
        self.aliases.get(name).ok_or_else(|| {
            let defined = self.aliases.keys().map(String::as_str).collect::<Vec<_>>().join(", ");
            crate::targets::refusal("alias_unknown", format!(
                "unknown alias '{name}'; defined aliases: [{defined}] (add it under `aliases:` in the operator config)"
            ))
        })
    }
    pub(crate) fn actor(&self) -> Option<&str> {
        self.operator.actor.as_deref()
    }
    pub(crate) fn output(&self) -> Option<ReadOutputFormat> {
        self.defaults.output
    }
}

pub(crate) fn operator_dir() -> Option<PathBuf> {
    if let Some(home_override) = env::var_os(OPERATOR_HOME_ENV) {
        return Some(expand_tilde(&home_override.to_string_lossy()));
    }
    env::home_dir().map(|home| home.join(OPERATOR_DIR))
}

#[derive(Debug)]
pub(crate) struct InvocationConfig {
    directory: Option<PathBuf>,
    retired_profile_present: bool,
    captured: OnceLock<std::result::Result<OperatorConfig, String>>,
}

impl InvocationConfig {
    pub(crate) fn capture() -> Self {
        let mut input = Self::at(operator_dir(), None);
        input.retired_profile_present =
            env::var_os(RETIRED_PROFILE_ENV).is_some_and(|v| !v.is_empty());
        input
    }
    pub(crate) fn at(directory: Option<PathBuf>, retired_profile: Option<String>) -> Self {
        Self {
            directory,
            retired_profile_present: retired_profile.is_some_and(|v| !v.is_empty()),
            captured: OnceLock::new(),
        }
    }
    pub(crate) fn has_profile(&self) -> bool {
        self.retired_profile_present
    }
    pub(crate) fn directory(&self) -> Option<&Path> {
        self.directory.as_deref()
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
    pub(crate) fn get(&self, _stderr: &mut dyn Write) -> Result<&OperatorConfig> {
        self.captured
            .get_or_init(|| {
                match self.config_path() {
                    Some(path) => load_operator_config_at(&path),
                    None => Ok(OperatorConfig::default()),
                }
                .map_err(|error| error.to_string())
            })
            .as_ref()
            .map_err(|message| crate::targets::refusal("target_config_invalid", message.clone()))
    }
}

/// Bounded operator-owned text input, including config, credentials and backups.
pub(crate) fn read_operator_file(path: &Path, description: &str) -> Result<Option<String>> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => bail!("could not read {description} '{}': {error}", path.display()),
    };
    let mut text = String::new();
    file.take(OPERATOR_FILE_MAX_BYTES as u64 + 1)
        .read_to_string(&mut text)
        .map_err(|error| eyre!("could not read {description} '{}': {error}", path.display()))?;
    if text.len() > OPERATOR_FILE_MAX_BYTES {
        bail!("{description} exceeds 1 MiB");
    }
    Ok(Some(text))
}

pub(crate) fn load_operator_config_at(path: &Path) -> Result<OperatorConfig> {
    let Some(text) = read_operator_file(path, "operator config")? else {
        return Ok(OperatorConfig::default());
    };
    if text.trim().is_empty() {
        bail!(
            "operator config '{}' has no target_config_version; run `omnigraph target import`",
            path.display()
        );
    }
    let mut config = parse_operator_config(&text)
        .map_err(|err| eyre!("operator config '{}': {err}", path.display()))?;
    config.base_dir = path.parent().unwrap_or(Path::new(".")).to_owned();
    Ok(config)
}

pub(crate) fn parse_operator_config(text: &str) -> Result<OperatorConfig> {
    if text.trim().is_empty() {
        return Ok(OperatorConfig::default());
    }
    // Value's mapping deserializer refuses duplicate keys before typed decoding.
    let value: yaml_serde::Value = yaml_serde::from_str(text)?;
    let mapping = value
        .as_mapping()
        .ok_or_else(|| eyre!("operator configuration must be a mapping"))?;
    if ["servers", "profiles", "clusters"]
        .iter()
        .any(|key| mapping.contains_key(*key))
        || mapping
            .get("defaults")
            .and_then(yaml_serde::Value::as_mapping)
            .is_some_and(|defaults| {
                ["server", "store", "default_graph"]
                    .iter()
                    .any(|key| defaults.contains_key(*key))
            })
        || mapping
            .get("aliases")
            .and_then(yaml_serde::Value::as_mapping)
            .is_some_and(|aliases| {
                aliases.values().any(|alias| {
                    alias
                        .as_mapping()
                        .is_some_and(|entry| entry.contains_key("server"))
                })
            })
    {
        bail!(
            "legacy operator routing is removed; run `omnigraph target import` to convert this file before using it"
        );
    }
    if !mapping.contains_key("target_config_version") {
        bail!(
            "legacy operator format is removed; run `omnigraph target import` (target files require `target_config_version: 1`)"
        );
    }
    let config: OperatorConfig = yaml_serde::from_value(value)?;
    crate::targets::validate_config(&config)?;
    Ok(config)
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

/// A stored token and the endpoint authority it was issued for.
pub(crate) struct StoredCredential {
    pub(crate) token: String,
    pub(crate) endpoint: Option<String>,
}

impl std::fmt::Debug for StoredCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredCredential")
            .field("token", &"<redacted>")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[cfg(test)]
pub(crate) fn read_credential_at(path: &Path, owner: &str) -> Result<Option<String>> {
    Ok(read_bound_credential_at(path, owner)?.map(|entry| entry.token))
}

pub(crate) fn read_bound_credential_at(
    path: &Path,
    owner: &str,
) -> Result<Option<StoredCredential>> {
    let Some(text) = read_operator_file(path, "credentials file")? else {
        return Ok(None);
    };
    refuse_over_permissive(path)?;
    let mut sections: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut selected = None;
    for line in text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            if !crate::targets::valid_name(name) || sections.contains_key(name) {
                bail!("credentials file contains an invalid or duplicate owner");
            }
            sections.insert(name.into(), BTreeMap::new());
            selected = Some(name.to_owned());
        } else {
            let name = selected
                .as_deref()
                .ok_or_else(|| eyre!("credential value outside an owner section"))?;
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| eyre!("malformed credential entry"))?;
            let key = key.trim();
            if !matches!(key, "token" | "endpoint")
                || sections
                    .get_mut(name)
                    .unwrap()
                    .insert(key.into(), unquote(value.trim()).into())
                    .is_some()
            {
                bail!("credentials file contains an unknown or duplicate field");
            }
        }
    }
    let Some(mut entry) = sections.remove(owner) else {
        return Ok(None);
    };
    let token = entry.remove("token").unwrap_or_default();
    if token.is_empty() {
        return Ok(None);
    }
    Ok(Some(StoredCredential {
        token,
        endpoint: entry.remove("endpoint"),
    }))
}

#[cfg(test)]
pub(crate) fn rewrite_credentials_at(path: &Path, owner: &str, token: Option<&str>) -> Result<()> {
    rewrite_bound_credentials_at(path, owner, None, token)
}

pub(crate) fn rewrite_bound_credentials_at(
    path: &Path,
    server: &str,
    endpoint: Option<&str>,
    token: Option<&str>,
) -> Result<()> {
    if !crate::targets::valid_name(server) {
        bail!("invalid credential owner");
    }
    if token.is_some_and(|token| token.contains(['\n', '\r'])) {
        bail!("a bearer token must occupy one line");
    }
    let _lock = crate::targets::ConfigEditLock::acquire(path)?;
    let existing = match read_operator_file(path, "credentials file")? {
        Some(text) => {
            refuse_over_permissive(path)?;
            text
        }
        None => String::new(),
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
        if let Some(endpoint) = endpoint {
            out.push_str(&format!("endpoint = {endpoint}\n"));
        }
    }

    if out.len() > OPERATOR_FILE_MAX_BYTES {
        bail!("credentials file exceeds 1 MiB");
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let parent = path
        .parent()
        .ok_or_else(|| eyre!("credential path has no parent directory"))?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(out.as_bytes())?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|error| {
        eyre!(
            "could not replace credentials file '{}': {}",
            path.display(),
            error.error
        )
    })?;
    crate::sync_dir(parent)?;
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
        let original = fs::read_to_string(&path).unwrap();
        for length in [OPERATOR_FILE_MAX_BYTES, OPERATOR_FILE_MAX_BYTES + 1] {
            let padded = format!("{original}#{}", "x".repeat(length - original.len() - 1));
            fs::write(&path, padded).unwrap();
            let result = read_credential_at(&path, "dev");
            if length == OPERATOR_FILE_MAX_BYTES {
                assert_eq!(result.unwrap().as_deref(), Some("tok-dev"));
            } else {
                assert!(result.unwrap_err().to_string().contains("exceeds 1 MiB"));
            }
        }
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
