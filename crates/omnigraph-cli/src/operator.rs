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
    /// Everything this CLI version doesn't know. Warned once at load,
    /// otherwise ignored (forward compatibility within the operator layer).
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
        warnings
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
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        assert!(warnings.iter().any(|w| w.contains("`servers`")));
        assert!(warnings.iter().any(|w| w.contains("`aliases`")));
        assert!(warnings.iter().any(|w| w.contains("`operator.color`")));
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
    fn expand_tilde_resolves_home_prefix() {
        let home = env::home_dir().unwrap();
        assert_eq!(expand_tilde("~"), home);
        assert_eq!(expand_tilde("~/x/y"), home.join("x/y"));
        assert_eq!(expand_tilde("/abs/path"), PathBuf::from("/abs/path"));
        assert_eq!(expand_tilde("rel/path"), PathBuf::from("rel/path"));
    }
}
