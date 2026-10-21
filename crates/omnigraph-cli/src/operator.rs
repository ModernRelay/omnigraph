//! One captured operator configuration per invocation.

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

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorConfig {
    #[serde(skip)]
    pub(crate) schema: Option<crate::operator_schema::OperatorConfig>,
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
    explicit_path: Option<PathBuf>,
    captured: OnceLock<std::result::Result<OperatorConfig, String>>,
}

impl InvocationConfig {
    pub(crate) fn capture() -> Self {
        Self::at(operator_dir(), None)
    }
    pub(crate) fn at(directory: Option<PathBuf>, _retired_profile: Option<String>) -> Self {
        Self {
            directory,
            explicit_path: None,
            captured: OnceLock::new(),
        }
    }
    pub(crate) fn directory(&self) -> Option<&Path> {
        self.directory.as_deref()
    }
    pub(crate) fn config_path(&self) -> Option<PathBuf> {
        if let Some(path) = &self.explicit_path {
            return Some(path.clone());
        }
        self.directory
            .as_ref()
            .map(|dir| dir.join(OPERATOR_CONFIG_FILE))
    }
    pub(crate) fn with_path(mut self, path: Option<&str>, cwd: &Path) -> Self {
        if let Some(path) = path {
            self.explicit_path = Some(cwd.join(expand_tilde(path)));
            self.captured = OnceLock::new();
        }
        self
    }
    pub(crate) fn get(&self, _stderr: &mut dyn Write) -> Result<&OperatorConfig> {
        self.captured
            .get_or_init(|| {
                match self.config_path() {
                    Some(path) => {
                        if self.explicit_path.is_some()
                            && !path.try_exists().map_err(|error| error.to_string())?
                        {
                            return Err(format!(
                                "explicit operator configuration '{}' does not exist",
                                path.display()
                            ));
                        }
                        load_operator_config_at(&path)
                    }
                    None => crate::operator_routing::parse(
                        "version: 1\n",
                        &env::current_dir().map_err(|error| error.to_string())?,
                    ),
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
    crate::operator_routing::check_rename_pending(path)?;
    let Some(text) = read_operator_file(path, "operator config")? else {
        return crate::operator_routing::parse(
            "version: 1\n",
            path.parent().unwrap_or(Path::new(".")),
        );
    };
    let mut config = crate::operator_routing::parse(&text, path.parent().unwrap_or(Path::new(".")))
        .map_err(|err| eyre!("operator config '{}': {err}", path.display()))?;
    config.base_dir = path.parent().unwrap_or(Path::new(".")).to_owned();
    Ok(config)
}

#[cfg(test)]
pub(crate) fn parse_operator_config(text: &str) -> Result<OperatorConfig> {
    crate::operator_routing::parse(text, &std::env::current_dir()?)
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
    #[test]
    fn absent_default_config_is_an_empty_version_one_document_but_explicit_missing_refuses() {
        let directory = tempfile::tempdir().unwrap();
        for input in [
            InvocationConfig::at(None, None),
            InvocationConfig::at(Some(directory.path().to_owned()), None),
        ] {
            let config = input.get(&mut std::io::sink()).unwrap();
            let schema = config.schema.as_ref().unwrap();
            assert_eq!(schema.version, 1);
            assert!(schema.profiles.is_empty());
            assert!(schema.targets.is_empty());
        }
        let explicit = InvocationConfig::at(Some(directory.path().to_owned()), None)
            .with_path(Some("absent.yaml"), directory.path());
        assert!(
            explicit
                .get(&mut std::io::sink())
                .unwrap_err()
                .to_string()
                .contains("does not exist")
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
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
