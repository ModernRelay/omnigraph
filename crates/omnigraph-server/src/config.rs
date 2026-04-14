use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use color_eyre::eyre::{Result, bail};
use serde::{Deserialize, Serialize};
pub const DEFAULT_CONFIG_FILE: &str = "omnigraph.yaml";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    pub uri: String,
    pub bearer_token_env: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ReadOutputFormat {
    #[default]
    Table,
    Kv,
    Csv,
    Jsonl,
    Json,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TableCellLayout {
    #[default]
    Truncate,
    Wrap,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CliDefaults {
    #[serde(rename = "graph")]
    pub graph: Option<String>,
    pub branch: Option<String>,
    pub output_format: Option<ReadOutputFormat>,
    pub table_max_column_width: Option<usize>,
    pub table_cell_layout: Option<TableCellLayout>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerDefaults {
    #[serde(rename = "graph")]
    pub graph: Option<String>,
    pub bind: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthDefaults {
    pub env_file: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryDefaults {
    #[serde(default)]
    pub roots: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicySettings {
    pub file: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasCommand {
    Read,
    Change,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasConfig {
    pub command: AliasCommand,
    pub query: String,
    pub name: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(rename = "graph")]
    pub graph: Option<String>,
    pub branch: Option<String>,
    pub format: Option<ReadOutputFormat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OmnigraphConfig {
    #[serde(default)]
    pub project: ProjectConfig,
    #[serde(default, rename = "graphs")]
    pub graphs: BTreeMap<String, TargetConfig>,
    #[serde(default)]
    pub server: ServerDefaults,
    #[serde(default)]
    pub auth: AuthDefaults,
    #[serde(default)]
    pub cli: CliDefaults,
    #[serde(default)]
    pub query: QueryDefaults,
    #[serde(default)]
    pub aliases: BTreeMap<String, AliasConfig>,
    #[serde(default)]
    pub policy: PolicySettings,
    #[serde(skip)]
    base_dir: PathBuf,
}

impl Default for OmnigraphConfig {
    fn default() -> Self {
        Self {
            project: ProjectConfig::default(),
            graphs: BTreeMap::new(),
            server: ServerDefaults::default(),
            auth: AuthDefaults::default(),
            cli: CliDefaults::default(),
            query: QueryDefaults::default(),
            aliases: BTreeMap::new(),
            policy: PolicySettings::default(),
            base_dir: PathBuf::new(),
        }
    }
}

impl OmnigraphConfig {
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn cli_branch(&self) -> &str {
        self.cli.branch.as_deref().unwrap_or("main")
    }

    pub fn cli_output_format(&self) -> ReadOutputFormat {
        self.cli.output_format.unwrap_or_default()
    }

    pub fn table_max_column_width(&self) -> usize {
        self.cli.table_max_column_width.unwrap_or(80)
    }

    pub fn table_cell_layout(&self) -> TableCellLayout {
        self.cli.table_cell_layout.unwrap_or_default()
    }

    pub fn cli_graph_name(&self) -> Option<&str> {
        self.cli.graph.as_deref()
    }

    pub fn server_graph_name(&self) -> Option<&str> {
        self.server.graph.as_deref()
    }

    pub fn server_bind(&self) -> &str {
        self.server.bind.as_deref().unwrap_or("127.0.0.1:8080")
    }

    pub fn resolve_target_name<'a>(
        &self,
        explicit_uri: Option<&str>,
        explicit_target: Option<&'a str>,
        default_target: Option<&'a str>,
    ) -> Option<&'a str> {
        explicit_target.or_else(|| {
            if explicit_uri.is_some() {
                None
            } else {
                default_target
            }
        })
    }

    pub fn graph_bearer_token_env(
        &self,
        explicit_uri: Option<&str>,
        explicit_target: Option<&str>,
        default_target: Option<&str>,
    ) -> Option<&str> {
        let target_name =
            self.resolve_target_name(explicit_uri, explicit_target, default_target)?;
        self.graphs
            .get(target_name)
            .and_then(|target| target.bearer_token_env.as_deref())
    }

    pub fn resolve_auth_env_file(&self) -> Option<PathBuf> {
        let path = self.auth.env_file.as_deref()?;
        let path = Path::new(path);
        Some(if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.base_dir.join(path)
        })
    }

    pub fn resolve_policy_file(&self) -> Option<PathBuf> {
        let path = self.policy.file.as_deref()?;
        let path = Path::new(path);
        Some(if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.base_dir.join(path)
        })
    }

    pub fn resolve_policy_tests_file(&self) -> Option<PathBuf> {
        let policy_file = self.resolve_policy_file()?;
        Some(policy_file.with_file_name("policy.tests.yaml"))
    }

    pub fn alias(&self, name: &str) -> Result<&AliasConfig> {
        self.aliases
            .get(name)
            .ok_or_else(|| color_eyre::eyre::eyre!("alias '{}' not found", name))
    }

    pub fn resolve_target_uri(
        &self,
        explicit_uri: Option<String>,
        explicit_target: Option<&str>,
        default_target: Option<&str>,
    ) -> Result<String> {
        if let Some(uri) = explicit_uri {
            return Ok(uri);
        }

        let target_name = explicit_target.or(default_target).ok_or_else(|| {
            color_eyre::eyre::eyre!("URI must be provided via <URI>, --target, or config")
        })?;
        let target = self.graphs.get(target_name).ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "graph '{}' not found in {}",
                target_name,
                DEFAULT_CONFIG_FILE
            )
        })?;
        Ok(self.resolve_config_uri(&target.uri))
    }

    pub fn resolve_query_path(&self, query: &Path) -> Result<PathBuf> {
        if query.is_absolute() {
            return Ok(query.to_path_buf());
        }

        let direct = self.base_dir.join(query);
        if direct.exists() {
            return Ok(direct);
        }

        for root in &self.query.roots {
            let candidate = self.base_dir.join(root).join(query);
            if candidate.exists() {
                return Ok(candidate);
            }
        }

        bail!("query file '{}' not found", query.display());
    }

    fn resolve_config_uri(&self, value: &str) -> String {
        if value.contains("://") {
            return value.to_string();
        }

        let path = Path::new(value);
        if path.is_absolute() {
            value.to_string()
        } else {
            self.base_dir.join(path).to_string_lossy().to_string()
        }
    }
}

pub fn default_config_path() -> PathBuf {
    PathBuf::from(DEFAULT_CONFIG_FILE)
}

pub fn load_config(config_path: Option<&PathBuf>) -> Result<OmnigraphConfig> {
    load_config_in(&env::current_dir()?, config_path)
}

fn load_config_in(cwd: &Path, config_path: Option<&PathBuf>) -> Result<OmnigraphConfig> {
    let explicit_path = config_path.cloned();
    let config_path = explicit_path.or_else(|| {
        let default_path = cwd.join(DEFAULT_CONFIG_FILE);
        default_path.exists().then_some(default_path)
    });

    let mut config = if let Some(path) = &config_path {
        serde_yaml::from_str::<OmnigraphConfig>(&fs::read_to_string(path)?)?
    } else {
        OmnigraphConfig::default()
    };

    config.base_dir = if let Some(path) = config_path {
        absolute_base_dir(cwd, &path)?
    } else {
        cwd.to_path_buf()
    };

    Ok(config)
}

fn absolute_base_dir(cwd: &Path, path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    Ok(path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| cwd.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use tempfile::tempdir;

    use super::{ReadOutputFormat, TableCellLayout, load_config_in};

    #[test]
    fn load_config_reads_yaml_defaults_from_current_dir() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            r#"
graphs:
  local:
    uri: ./demo.omni
    bearer_token_env: DEMO_TOKEN
auth:
  env_file: .env.omni
cli:
  graph: local
  branch: main
  output_format: kv
  table_max_column_width: 40
  table_cell_layout: wrap
policy: {}
"#,
        )
        .unwrap();

        let config = load_config_in(temp.path(), None).unwrap();
        assert_eq!(config.cli_graph_name(), Some("local"));
        assert_eq!(config.cli_branch(), "main");
        assert_eq!(config.cli_output_format(), ReadOutputFormat::Kv);
        assert_eq!(config.table_max_column_width(), 40);
        assert_eq!(config.table_cell_layout(), TableCellLayout::Wrap);
        assert_eq!(
            config.graph_bearer_token_env(None, None, config.cli_graph_name()),
            Some("DEMO_TOKEN")
        );
        assert_eq!(
            config.resolve_auth_env_file().unwrap(),
            temp.path().join(".env.omni")
        );
        assert_eq!(
            PathBuf::from(
                config
                    .resolve_target_uri(None, None, config.cli_graph_name())
                    .unwrap()
            ),
            temp.path().join("./demo.omni")
        );
    }

    #[test]
    fn load_config_does_not_walk_parent_directories() {
        let temp = tempdir().unwrap();
        let child = temp.path().join("child");
        fs::create_dir_all(&child).unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "graphs:\n  local:\n    uri: ./demo.omni\n",
        )
        .unwrap();

        let config = load_config_in(&child, None).unwrap();
        assert!(config.graphs.is_empty());
    }

    #[test]
    fn resolve_query_path_searches_config_roots() {
        let temp = tempdir().unwrap();
        fs::create_dir_all(temp.path().join("queries")).unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "query:\n  roots:\n    - queries\npolicy: {}\n",
        )
        .unwrap();
        fs::write(
            temp.path().join("queries").join("test.gq"),
            "query q { return {} }",
        )
        .unwrap();

        let config = load_config_in(temp.path(), None).unwrap();
        let resolved = config.resolve_query_path(Path::new("test.gq")).unwrap();
        assert_eq!(resolved, temp.path().join("queries").join("test.gq"));
    }

    #[test]
    fn resolve_query_path_prefers_config_base_dir_over_ambient_cwd() {
        let workspace = tempdir().unwrap();
        let config_dir = workspace.path().join("config");
        let ambient_dir = workspace.path().join("ambient");
        fs::create_dir_all(&config_dir).unwrap();
        fs::create_dir_all(&ambient_dir).unwrap();
        fs::write(config_dir.join("omnigraph.yaml"), "policy: {}\n").unwrap();
        fs::write(config_dir.join("local.gq"), "query local { return {} }").unwrap();
        fs::write(ambient_dir.join("local.gq"), "query ambient { return {} }").unwrap();

        let config =
            load_config_in(&ambient_dir, Some(&config_dir.join("omnigraph.yaml"))).unwrap();
        let resolved = config.resolve_query_path(Path::new("local.gq")).unwrap();

        assert_eq!(resolved, config_dir.join("local.gq"));
    }

    #[test]
    fn policy_block_accepts_non_empty_mapping() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "policy:\n  file: ./policy.yaml\n",
        )
        .unwrap();

        let config = load_config_in(temp.path(), None).unwrap();
        assert_eq!(
            config.resolve_policy_file().unwrap(),
            temp.path().join("policy.yaml")
        );
    }

    #[test]
    fn scoped_auth_env_ignores_default_target_when_uri_is_explicit() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            r#"
graphs:
  demo:
    uri: https://example.com
    bearer_token_env: DEMO_TOKEN
cli:
  graph: demo
"#,
        )
        .unwrap();

        let config = load_config_in(temp.path(), None).unwrap();
        assert_eq!(
            config.graph_bearer_token_env(
                Some("https://override.example.com"),
                None,
                config.cli_graph_name()
            ),
            None
        );
        assert_eq!(
            config.graph_bearer_token_env(
                Some("https://override.example.com"),
                Some("demo"),
                config.cli_graph_name()
            ),
            Some("DEMO_TOKEN")
        );
    }
}
