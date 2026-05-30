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
    /// Per-graph Cedar policy file (MR-668). In single-graph mode this
    /// field is unused — the top-level `policy.file` applies. In
    /// multi-graph mode, each `graphs.<id>.policy.file` governs that
    /// graph's HTTP-layer Cedar enforcement.
    #[serde(default)]
    pub policy: PolicySettings,
    /// Per-graph stored-query registry: an inline `name -> entry`
    /// map. Mirrors the per-graph `policy` shape — each
    /// `graphs.<id>.queries` declares that graph's stored queries. Absent
    /// (or empty) = no stored queries for the graph. v1 is inline-only;
    /// an external `queries.yaml` manifest indirection is a deferred
    /// convenience.
    #[serde(default)]
    pub queries: BTreeMap<String, QueryEntry>,
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
    /// Default actor identity for CLI direct-engine writes (MR-722).
    /// Used when `policy.file` is configured and the operator hasn't
    /// passed `--as <actor>` on the command line. With policy configured
    /// and neither this nor `--as` set, the engine-layer footgun guard
    /// fires (no silent bypass).
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerDefaults {
    #[serde(rename = "graph")]
    pub graph: Option<String>,
    pub bind: Option<String>,
    /// Server-level Cedar policy (MR-668). Governs management endpoints
    /// — currently `GET /graphs`; future runtime add/remove endpoints
    /// will plug in here too. In single-graph mode this is unused — the
    /// top-level `policy.file` covers the single graph.
    #[serde(default)]
    pub policy: PolicySettings,
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

/// One stored-query registry entry. The map **key** is the query's
/// identity — it must equal the `query <name>` symbol declared inside
/// the referenced `.gq` file (asserted at load, C2). Renaming the key
/// (or the symbol) is a breaking change to callers, by design.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEntry {
    /// Path to the `.gq` file (relative to the config's `base_dir`). The
    /// file may declare several queries; the registry selects the one
    /// whose symbol matches the map key.
    pub file: String,
    #[serde(default)]
    pub mcp: McpSettings,
}

/// MCP exposure for a stored query. A *deployment* concern (the same
/// `.gq` may be exposed in one graph and hidden in another), so it lives
/// in YAML rather than in the `.gq` source. Default `expose: false` —
/// a query is HTTP-callable but absent from the MCP tool catalog unless
/// the operator opts in. The catalog projection lands in a later slice;
/// v1 round-trips these fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct McpSettings {
    #[serde(default)]
    pub expose: bool,
    pub tool_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasCommand {
    /// Read alias (canonical: `query`). The legacy spelling `read` is
    /// kept as the variant name for back-compat with serialized configs
    /// and external SDK callers; `query` is accepted on the wire via the
    /// serde alias.
    #[serde(alias = "query")]
    Read,
    /// Mutation alias (canonical: `mutate`). The legacy spelling `change`
    /// is kept as the variant name for back-compat; `mutate` is accepted
    /// on the wire via the serde alias.
    #[serde(alias = "mutate")]
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
    /// Top-level stored-query registry, used in single-graph
    /// mode — mirrors how the top-level `policy` applies to the single
    /// graph. In multi-graph mode this is unused; each graph's
    /// `graphs.<id>.queries` applies instead.
    #[serde(default)]
    pub queries: BTreeMap<String, QueryEntry>,
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
            queries: BTreeMap::new(),
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
        self.auth
            .env_file
            .as_deref()
            .map(|path| self.resolve_config_path(path))
    }

    pub fn resolve_policy_file(&self) -> Option<PathBuf> {
        self.policy
            .file
            .as_deref()
            .map(|path| self.resolve_config_path(path))
    }

    /// Resolve the per-graph policy file path for the named target,
    /// relative to the config file's `base_dir`. Returns `None` if the
    /// target is unknown or no per-graph `policy.file` is set.
    pub fn resolve_target_policy_file(&self, target_name: &str) -> Option<PathBuf> {
        let target = self.graphs.get(target_name)?;
        target
            .policy
            .file
            .as_deref()
            .map(|path| self.resolve_config_path(path))
    }

    /// The top-level stored-query registry entries (single-graph mode).
    pub fn query_entries(&self) -> &BTreeMap<String, QueryEntry> {
        &self.queries
    }

    /// The per-graph stored-query registry entries for a named target
    /// (multi-graph mode). Returns `None` if the target is unknown.
    pub fn target_query_entries(
        &self,
        target_name: &str,
    ) -> Option<&BTreeMap<String, QueryEntry>> {
        self.graphs.get(target_name).map(|target| &target.queries)
    }

    /// Resolve a stored-query `.gq` file path (from a registry entry),
    /// relative to the config's `base_dir`. Mirrors policy-file
    /// resolution; the registry loader (C2) calls this to turn each
    /// entry's `file:` value into an absolute path.
    pub fn resolve_query_file(&self, value: &str) -> PathBuf {
        self.resolve_config_path(value)
    }

    /// Resolve the server-level policy file path (used by management
    /// endpoints). Returns `None` if `server.policy.file` is not set.
    pub fn resolve_server_policy_file(&self) -> Option<PathBuf> {
        self.server
            .policy
            .file
            .as_deref()
            .map(|path| self.resolve_config_path(path))
    }

    /// Resolve a raw config-supplied URI (which may be relative) to its
    /// absolute form. URIs containing `://` are passed through as-is;
    /// relative paths are joined with the config file's `base_dir`.
    pub fn resolve_uri_value(&self, value: &str) -> String {
        self.resolve_config_uri(value)
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

    fn resolve_config_path(&self, value: &str) -> PathBuf {
        let path = Path::new(value);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.base_dir.join(path)
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
    fn queries_block_round_trips_inline_and_per_graph() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            r#"
graphs:
  prod:
    uri: s3://bucket/prod
    queries:
      find_user:
        file: ./queries/find_user.gq
        mcp:
          expose: true
          tool_name: lookup_user
      internal_audit:
        file: ./queries/audit.gq
queries:
  single_mode_q:
    file: ./q.gq
"#,
        )
        .unwrap();

        let config = load_config_in(temp.path(), None).unwrap();

        // Per-graph registry (multi-graph mode).
        let prod = config.target_query_entries("prod").unwrap();
        assert_eq!(prod.len(), 2);
        let find_user = &prod["find_user"];
        assert_eq!(find_user.file, "./queries/find_user.gq");
        assert!(find_user.mcp.expose);
        assert_eq!(find_user.mcp.tool_name.as_deref(), Some("lookup_user"));
        // Default exposure is false (safe by default) and tool_name absent.
        let audit = &prod["internal_audit"];
        assert!(!audit.mcp.expose);
        assert!(audit.mcp.tool_name.is_none());

        // Top-level registry (single-graph mode).
        assert_eq!(config.query_entries().len(), 1);

        // Path resolution joins against base_dir, like policy files.
        assert_eq!(
            config.resolve_query_file(&find_user.file),
            temp.path().join("./queries/find_user.gq")
        );
    }

    #[test]
    fn queries_block_absent_yields_empty_registry() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "graphs:\n  local:\n    uri: ./demo.omni\n",
        )
        .unwrap();

        let config = load_config_in(temp.path(), None).unwrap();
        // Additive: no `queries:` anywhere → empty registries everywhere.
        assert!(config.query_entries().is_empty());
        assert!(
            config
                .target_query_entries("local")
                .unwrap()
                .is_empty()
        );
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
