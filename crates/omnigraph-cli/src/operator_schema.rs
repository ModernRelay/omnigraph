use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

mod validation;
mod yaml;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod schema_tests;

pub(crate) const MAX_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorConfig {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) profiles: BTreeMap<String, Profile>,
    #[serde(default)]
    pub(crate) targets: BTreeMap<String, Target>,
    #[serde(default)]
    pub(crate) aliases: BTreeMap<String, ReadAlias>,
    #[serde(default)]
    pub(crate) defaults: Defaults,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Profile {
    pub(crate) api: String,
    pub(crate) organization_id: String,
    pub(crate) auth: AuthProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) transport: Option<Transport>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Target {
    Managed {
        profile: String,
        cluster_id: String,
        cluster_incarnation: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transport: Option<Transport>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_graph: Option<String>,
    },
    Server {
        endpoint: String,
        auth: BTreeMap<String, AuthProfile>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_auth: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transport: Option<Transport>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_graph: Option<String>,
    },
    Direct {
        scope: DirectScope,
        uri: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_graph: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DirectScope {
    Graph,
    Root,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    from = "StrictAuthProfile",
    tag = "provider",
    rename_all = "snake_case"
)]
pub(crate) enum AuthProfile {
    Omnigraph,
    Token {
        source: TokenSource,
    },
    Exec {
        command: Vec<String>,
        timeout: String,
    },
    Anonymous,
}

#[derive(Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
enum StrictAuthProfile {
    Omnigraph {},
    Token {
        source: TokenSource,
    },
    Exec {
        command: Vec<String>,
        #[serde(default = "default_timeout")]
        timeout: String,
    },
    Anonymous {},
}

impl From<StrictAuthProfile> for AuthProfile {
    fn from(value: StrictAuthProfile) -> Self {
        match value {
            StrictAuthProfile::Omnigraph {} => Self::Omnigraph,
            StrictAuthProfile::Token { source } => Self::Token { source },
            StrictAuthProfile::Exec { command, timeout } => Self::Exec { command, timeout },
            StrictAuthProfile::Anonymous {} => Self::Anonymous,
        }
    }
}

fn default_timeout() -> String {
    "30s".to_owned()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub(crate) enum TokenSource {
    Store { store: Empty },
    Env { env: String },
    File { file: PathBuf },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Empty {}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Transport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tls: Option<Tls>,
    #[serde(default)]
    pub(crate) proxy: Proxy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tls {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) ca: Option<Ca>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Ca {
    pub(crate) mode: CaMode,
    pub(crate) file: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CaMode {
    Append,
    Replace,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(from = "StrictProxy", tag = "mode", rename_all = "snake_case")]
pub(crate) enum Proxy {
    #[default]
    Direct,
    Url {
        url: String,
    },
    Environment,
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum StrictProxy {
    Direct {},
    Url { url: String },
    Environment {},
}

impl From<StrictProxy> for Proxy {
    fn from(value: StrictProxy) -> Self {
        match value {
            StrictProxy::Direct {} => Self::Direct,
            StrictProxy::Url { url } => Self::Url { url },
            StrictProxy::Environment {} => Self::Environment,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Defaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<String>,
    #[serde(default)]
    pub(crate) output: OutputFormat,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutputFormat {
    #[default]
    Table,
    Json,
    Jsonl,
    Csv,
    Kv,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadAlias {
    pub(crate) target: String,
    pub(crate) query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) graph: Option<String>,
    #[serde(default)]
    pub(crate) params: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConfigError(pub(crate) String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

pub(crate) fn parse(text: &str, base_dir: &Path) -> Result<OperatorConfig, ConfigError> {
    if text.len() > MAX_CONFIG_BYTES {
        return Err(ConfigError(
            "operator configuration exceeds the 1 MiB limit".into(),
        ));
    }
    let value = yaml::parse(text)?;
    if value.as_mapping().is_some_and(|mapping| {
        mapping.contains_key(yaml_serde::Value::String("target_config_version".into()))
    }) {
        return Err(ConfigError(
            "legacy target_config_version is unsupported; write a version: 1 operator document using docs/user/cli/targets.md#upgrade-from-the-old-format; there is no automatic importer".into(),
        ));
    }
    let mut config: OperatorConfig = yaml_serde::from_value(value)
        .map_err(|error| ConfigError(format!("invalid operator configuration: {error}")))?;
    validation::validate(&mut config, base_dir)?;
    Ok(config)
}
