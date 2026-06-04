use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use color_eyre::eyre::{Result, bail};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CONFIG_FILE: &str = "omnigraph.yaml";

pub fn graph_resource_id_for_selection(
    selected_graph: Option<&str>,
    normalized_uri: &str,
) -> String {
    selected_graph.unwrap_or(normalized_uri).to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub name: Option<String>,
}

// Spans both schemas (legacy `uri:` and v1 `storage:`/`server:`), so unknown-key
// strictness is gated on `version` in `load_config_in` rather than pinned here as
// a struct attribute: legacy tolerates unknown keys, `version: 1` rejects them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    /// Embedded base URI. Legacy spelling (`uri:`) for an object-store
    /// location; populated post-load from `storage`/`server` so every existing
    /// reader sees a resolved base URI. Empty until `storage`/`server`/`uri`
    /// is provided and `normalize_graphs` runs.
    #[serde(default)]
    pub uri: String,
    /// Embedded storage location (string or block) — RFC-002 §2. Mutually
    /// exclusive with `server`; the bare-string form desugars to `{ uri }`.
    pub storage: Option<Storage>,
    /// Remote: the name of a `servers:` entry hosting this graph. Mutually
    /// exclusive with `storage`/`uri`.
    pub server: Option<String>,
    /// Remote: the graph's id on `server` (defaults to the entry key).
    pub graph_id: Option<String>,
    /// Default branch for this graph (sub-state).
    pub branch: Option<String>,
    /// Optional read-pinned snapshot version.
    pub snapshot: Option<u64>,
    pub bearer_token_env: Option<String>,
    /// Per-graph Cedar policy file (MR-668). Valid only on embedded graphs; a
    /// remote graph's policy is owned by the server it targets.
    #[serde(default)]
    pub policy: PolicySettings,
    /// Per-graph stored-query registry (embedded graphs only; remote queries
    /// are server-owned and discovered).
    #[serde(default)]
    pub queries: BTreeMap<String, QueryEntry>,
}

/// Embedded storage location: a bare URI string, or a block carrying per-graph
/// object-store options. `region`/`endpoint` are parsed but **not yet threaded
/// to the engine** (that lands with the V2 remote/route work); `profile` is
/// intentionally absent (`AWS_PROFILE` is env-only in Lance and our store).
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Storage {
    Bare(String),
    Block(StorageBlock),
}

impl<'de> Deserialize<'de> for Storage {
    /// String-or-block, hand-rolled rather than `#[serde(untagged)]` on the
    /// deserialize side so a bad key in the block form surfaces the precise
    /// `unknown field` error instead of a generic "did not match any variant"
    /// (the honored-or-rejected DX rule, RFC-002 §1.2).
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        if let serde_yaml::Value::String(s) = value {
            Ok(Storage::Bare(s))
        } else {
            serde_yaml::from_value::<StorageBlock>(value)
                .map(Storage::Block)
                .map_err(serde::de::Error::custom)
        }
    }
}

// `storage:` (block form) is v1-only syntax with no legacy form to be lenient
// about, and the hand-rolled `Storage` deserialize above reaches it through an
// opaque `from_value` the version-gated loader pass cannot see into — so this
// block is always strict via its own `deny_unknown_fields`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageBlock {
    pub uri: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

impl Storage {
    pub fn uri(&self) -> &str {
        match self {
            Storage::Bare(uri) => uri,
            Storage::Block(block) => &block.uri,
        }
    }
}

/// A named remote server — an endpoint a client targets. Secret-free: bearer
/// credentials are resolved out of band (the auth model is a later change).
// `servers:` is v1-only syntax (no legacy form), so this typed block is always
// strict — the version gate governs only the legacy-spanning structs
// (`OmnigraphConfig`, `TargetConfig`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerEntry {
    pub endpoint: String,
}

/// A resolved graph address (RFC-002 §1.1/§2) — replaces scheme-sniffing on a
/// `uri` string with a typed embedded-XOR-remote locator.
#[derive(Debug, Clone)]
pub enum GraphLocator {
    Embedded {
        /// Object-store URI, resolved against the config `base_dir`.
        uri: String,
        region: Option<String>,
        endpoint: Option<String>,
        branch: Option<String>,
        snapshot: Option<u64>,
        policy_file: Option<PathBuf>,
        /// Cedar resource identity (graph name, or normalized URI for a
        /// positional `<URI>`).
        graph_id: String,
        /// The selected graph name, if from `graphs:` (None for a positional).
        selected: Option<String>,
    },
    Remote {
        endpoint: String,
        server: String,
        graph_id: String,
        branch: Option<String>,
        snapshot: Option<u64>,
    },
}

impl GraphLocator {
    pub fn is_remote(&self) -> bool {
        matches!(self, GraphLocator::Remote { .. })
    }
}

/// Scheme classification for a base URI: `http(s)://` is remote, everything
/// else (`file://`, `s3://`, a path) is embedded. The single place this sniff
/// survives, fed only the positional `<URI>` and legacy `uri:` entries.
fn uri_is_remote(uri: &str) -> bool {
    uri.starts_with("http://") || uri.starts_with("https://")
}

/// Split a legacy remote `uri:` into `(endpoint, graph_id)`: strip a trailing
/// `/graphs/{gid}` (the pre-typed-locator multi-graph hack) so the resolved
/// address is clean; otherwise the whole URI is the endpoint and `graph_id`
/// falls back to the entry key.
fn split_legacy_remote(uri: &str, key: &str) -> (String, String) {
    let trimmed = uri.trim_end_matches('/');
    if let Some((endpoint, gid)) = trimmed.rsplit_once("/graphs/") {
        if !gid.is_empty() && !gid.contains('/') {
            return (endpoint.to_string(), gid.to_string());
        }
    }
    (trimmed.to_string(), key.to_string())
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
/// the referenced `.gq` file (asserted when the registry loads).
/// Renaming the key (or the symbol) is a breaking change to callers, by
/// design.
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
/// in YAML rather than in the `.gq` source. **Default `expose: true`** —
/// declaring a query in the manifest *is* the opt-in, so it appears in the
/// MCP tool catalog (`GET /queries`) by default; set `expose: false` to
/// keep a query HTTP/service-callable but hidden from the agent tool list.
/// `expose` governs catalog membership only — it is **not** an
/// authorization gate (invocation is gated by `invoke_query`), so a hidden
/// query is still invocable by name with the right permission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpSettings {
    #[serde(default = "mcp_expose_default")]
    pub expose: bool,
    pub tool_name: Option<String>,
}

fn mcp_expose_default() -> bool {
    true
}

impl Default for McpSettings {
    fn default() -> Self {
        Self {
            expose: mcp_expose_default(),
            tool_name: None,
        }
    }
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

// Top-level schema, spanning legacy and v1 — unknown-key strictness is gated on
// `version` in `load_config_in` (see the `version` field), not pinned here as a
// struct attribute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OmnigraphConfig {
    /// Schema version — the strictness discriminator. Absent ⇒ legacy schema:
    /// unknown YAML keys are tolerated (lenient). `Some(1)` ⇒ typed schema:
    /// unknown keys are rejected at any depth (honored-or-rejected). Decided in
    /// `load_config_in`, which also rejects unsupported versions.
    #[serde(default)]
    pub version: Option<u32>,
    #[serde(default)]
    pub project: ProjectConfig,
    /// Named remote servers (endpoints) referenced by remote graph entries
    /// (`graphs.<name>.server`) and `server/graph_id` addressing — RFC-002 §1.
    #[serde(default)]
    pub servers: BTreeMap<String, ServerEntry>,
    #[serde(default, rename = "graphs")]
    pub graphs: BTreeMap<String, TargetConfig>,
    #[serde(default)]
    pub server: ServerDefaults,
    #[serde(default)]
    pub auth: AuthDefaults,
    /// CLI/client defaults (`defaults:`) — RFC-002. The legacy spelling `cli:`
    /// is accepted via a serde alias and honored under the legacy schema; under
    /// `version: 1` it is rejected at load (see [`legacy_top_level_keys`]). All
    /// reads go through the `default_*` accessors.
    #[serde(default, alias = "cli")]
    pub defaults: CliDefaults,
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
    /// Whether a config file was actually loaded (vs the built-in default).
    /// Gates the no-`version:` deprecation notice — there is nothing to migrate
    /// when no `omnigraph.yaml` exists.
    #[serde(skip)]
    loaded_from_file: bool,
    /// Top-level legacy keys present in the loaded file that v1 renames or
    /// removes (see [`legacy_top_level_keys`]). Populated by `load_config_in`
    /// from a raw scan; drives the legacy deprecation warnings. Empty under
    /// `version: 1` (those keys are rejected at load) and for the built-in default.
    #[serde(skip)]
    legacy_keys: Vec<String>,
}

impl Default for OmnigraphConfig {
    fn default() -> Self {
        Self {
            version: None,
            project: ProjectConfig::default(),
            servers: BTreeMap::new(),
            graphs: BTreeMap::new(),
            server: ServerDefaults::default(),
            auth: AuthDefaults::default(),
            defaults: CliDefaults::default(),
            query: QueryDefaults::default(),
            aliases: BTreeMap::new(),
            policy: PolicySettings::default(),
            queries: BTreeMap::new(),
            base_dir: PathBuf::new(),
            loaded_from_file: false,
            legacy_keys: Vec::new(),
        }
    }
}

impl OmnigraphConfig {
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn default_branch(&self) -> &str {
        self.defaults.branch.as_deref().unwrap_or("main")
    }

    pub fn default_output_format(&self) -> ReadOutputFormat {
        self.defaults.output_format.unwrap_or_default()
    }

    pub fn table_max_column_width(&self) -> usize {
        self.defaults.table_max_column_width.unwrap_or(80)
    }

    pub fn table_cell_layout(&self) -> TableCellLayout {
        self.defaults.table_cell_layout.unwrap_or_default()
    }

    pub fn default_graph_name(&self) -> Option<&str> {
        self.defaults.graph.as_deref()
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
    pub fn target_query_entries(&self, target_name: &str) -> Option<&BTreeMap<String, QueryEntry>> {
        self.graphs.get(target_name).map(|target| &target.queries)
    }

    /// The stored-query registry entries that apply for a graph
    /// selection — the single definition of "which `queries:` block
    /// governs graph X", shared by server boot and the CLI so the two
    /// can't drift. A named graph present in `graphs:` uses its
    /// per-graph block; everything else (no selection, or a name that is
    /// not a known graph, e.g. a bare URI) falls back to the top-level
    /// block (single-graph mode).
    pub fn query_entries_for(&self, graph: Option<&str>) -> &BTreeMap<String, QueryEntry> {
        match graph {
            Some(name) if self.graphs.contains_key(name) => &self.graphs[name].queries,
            _ => &self.queries,
        }
    }

    /// The single CLI gate that turns a raw graph selection into a *validated*
    /// one — the fallible counterpart to the infallible
    /// [`OmnigraphConfig::query_entries_for`]. Both `queries` subcommands route
    /// their selection through here so neither can skip a check the other (or
    /// server boot) applies:
    /// * a known name passes through, but only after the same coherence check
    ///   server boot enforces
    ///   ([`OmnigraphConfig::ensure_top_level_blocks_honored`]) — a named graph
    ///   with a populated top-level block is rejected;
    /// * an unknown name errors with the **same** message
    ///   [`OmnigraphConfig::resolve_target_uri`] produces, so a command that
    ///   opens no URI rejects an unknown `--graph` exactly like the
    ///   URI-resolving commands do;
    /// * an anonymous selection (`None`, e.g. a bare URI) stays anonymous,
    ///   resolving to the top-level registry downstream (top-level honored).
    pub fn resolve_graph_selection<'a>(&self, graph: Option<&'a str>) -> Result<Option<&'a str>> {
        match graph {
            Some(name) if self.graphs.contains_key(name) => {
                self.ensure_top_level_blocks_honored(Some(name))?;
                Ok(Some(name))
            }
            Some(name) => bail!("graph '{}' not found in {}", name, DEFAULT_CONFIG_FILE),
            None => Ok(None),
        }
    }

    pub fn resolve_policy_tooling_graph_selection(&self) -> Result<Option<&str>> {
        self.resolve_graph_selection(
            self.default_graph_name()
                .or_else(|| self.server_graph_name()),
        )
    }

    /// The policy file that applies for a graph selection — the policy
    /// sibling of [`OmnigraphConfig::query_entries_for`], so policy and
    /// queries resolve by the same identity rule. A named graph in
    /// `graphs:` uses its per-graph `policy.file` with **no** top-level
    /// fallback (a named graph with no per-graph policy has no policy —
    /// that keeps the boot-time coherence check meaningful); anything else
    /// (no selection, or a bare URI) uses the top-level `policy.file`.
    pub fn resolve_policy_file_for(&self, graph: Option<&str>) -> Option<PathBuf> {
        match graph {
            Some(name) if self.graphs.contains_key(name) => self.resolve_target_policy_file(name),
            _ => self.resolve_policy_file(),
        }
    }

    /// Names of any top-level config blocks (`policy.file`, `queries:`)
    /// that are populated. Used by the boot-time coherence check: when a
    /// **named** graph is served (single-mode by name, or multi-mode),
    /// the top-level blocks are not honored, so a populated one is a
    /// configuration error rather than a silent no-op.
    pub fn populated_top_level_blocks(&self) -> Vec<&'static str> {
        let mut blocks = Vec::new();
        if self.policy.file.is_some() {
            blocks.push("policy.file");
        }
        if !self.queries.is_empty() {
            blocks.push("queries");
        }
        blocks
    }

    /// A named graph uses its own `graphs.<name>` block, so a populated
    /// top-level block would be silently ignored — a config error. The single
    /// definition of that rule, shared by server boot and the CLI selection
    /// gate ([`OmnigraphConfig::resolve_graph_selection`]) so the two can't
    /// drift. An anonymous selection (`None`, e.g. a bare URI) legitimately
    /// honors the top-level blocks, so it is never rejected here.
    pub fn ensure_top_level_blocks_honored(&self, selected: Option<&str>) -> Result<()> {
        if let Some(name) = selected {
            let unhonored = self.populated_top_level_blocks();
            if !unhonored.is_empty() {
                bail!(
                    "named graph '{name}' uses its own `graphs.{name}.…` block, but top-level {} \
                     {} set and would be ignored. Move it to `graphs.{name}` (e.g. \
                     `graphs.{name}.policy.file`, `graphs.{name}.queries`).",
                    unhonored.join(" and "),
                    if unhonored.len() == 1 { "is" } else { "are" },
                );
            }
        }
        Ok(())
    }

    /// Resolve a stored-query `.gq` file path (from a registry entry),
    /// relative to the config's `base_dir`. Mirrors policy-file
    /// resolution; the registry loader calls this to turn each entry's
    /// `file:` value into an absolute path.
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

    /// Load-time deprecation notices (RFC-002 §Migration), computed purely so the
    /// CLI/server can print them — the config crate stays stdio-agnostic. The
    /// no-`version:` notice is gated on `loaded_from_file` (nothing to migrate when
    /// no config file exists); the per-graph notice flags a legacy `uri:` entry.
    pub fn deprecation_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if self.loaded_from_file && self.version.is_none() {
            warnings.push(
                "omnigraph.yaml has no `version:`; the legacy schema is deprecated — add \
                 `version: 1` so unknown keys error instead of being silently ignored."
                    .to_string(),
            );
        }
        // Legacy-only notices for top-level keys that `version: 1` rejects at
        // load (so a populated `legacy_keys` only ever occurs under the lenient
        // legacy schema). Shares the migration hints with the v1 rejection.
        for key in &self.legacy_keys {
            if let Some(hint) = legacy_key_migration_hint(key) {
                warnings.push(format!("`{key}:` — {hint}"));
            }
        }
        for (name, entry) in &self.graphs {
            if entry.storage.is_none() && entry.server.is_none() && !entry.uri.is_empty() {
                warnings.push(format!(
                    "graph '{name}' uses the legacy `uri:` key; use `storage:` for an embedded \
                     graph or `server:` for a remote one."
                ));
            }
        }
        warnings
    }

    /// Resolve a graph selection to a typed [`GraphLocator`] (RFC-002 §1): an
    /// explicit positional `<URI>` (scheme-sniffed), else a local `graphs:`
    /// alias, else a `server/graph_id` qualified name against `servers:`. A
    /// bare selection falls back to `cli.graph`.
    pub fn resolve_graph(
        &self,
        explicit_uri: Option<&str>,
        name: Option<&str>,
    ) -> Result<GraphLocator> {
        if let Some(uri) = explicit_uri {
            // Anonymous positional <URI> — the one surviving scheme sniff.
            return Ok(if uri_is_remote(uri) {
                let endpoint = uri.trim_end_matches('/').to_string();
                GraphLocator::Remote {
                    graph_id: endpoint.clone(),
                    server: "<positional>".to_string(),
                    endpoint,
                    branch: None,
                    snapshot: None,
                }
            } else {
                let resolved = self.resolve_config_uri(uri);
                GraphLocator::Embedded {
                    graph_id: graph_resource_id_for_selection(None, &resolved),
                    uri: resolved,
                    region: None,
                    endpoint: None,
                    branch: None,
                    snapshot: None,
                    policy_file: self.resolve_policy_file(),
                    selected: None,
                }
            });
        }

        let name = name.or_else(|| self.default_graph_name()).ok_or_else(|| {
            color_eyre::eyre::eyre!("URI must be provided via <URI>, --graph, or config")
        })?;

        if let Some(entry) = self.graphs.get(name) {
            return self.locator_from_entry(name, entry);
        }
        if let Some((server_name, graph_id)) = name.split_once('/') {
            if let Some(server) = self.servers.get(server_name) {
                return Ok(GraphLocator::Remote {
                    endpoint: server.endpoint.trim_end_matches('/').to_string(),
                    server: server_name.to_string(),
                    graph_id: graph_id.to_string(),
                    branch: None,
                    snapshot: None,
                });
            }
        }
        bail!("graph '{}' not found in {}", name, DEFAULT_CONFIG_FILE)
    }

    fn locator_from_entry(&self, name: &str, entry: &TargetConfig) -> Result<GraphLocator> {
        let remote = entry.server.is_some() || uri_is_remote(&entry.uri);
        if remote {
            let (endpoint, graph_id, server) = match &entry.server {
                Some(server) => (
                    entry.uri.trim_end_matches('/').to_string(),
                    entry.graph_id.clone().unwrap_or_else(|| name.to_string()),
                    server.clone(),
                ),
                None => {
                    let (endpoint, graph_id) = split_legacy_remote(&entry.uri, name);
                    (endpoint, graph_id, name.to_string())
                }
            };
            Ok(GraphLocator::Remote {
                endpoint,
                server,
                graph_id,
                branch: entry.branch.clone(),
                snapshot: entry.snapshot,
            })
        } else {
            let (region, endpoint) = match &entry.storage {
                Some(Storage::Block(block)) => (block.region.clone(), block.endpoint.clone()),
                _ => (None, None),
            };
            Ok(GraphLocator::Embedded {
                uri: self.resolve_config_uri(&entry.uri),
                region,
                endpoint,
                branch: entry.branch.clone(),
                snapshot: entry.snapshot,
                policy_file: self.resolve_target_policy_file(name),
                graph_id: name.to_string(),
                selected: Some(name.to_string()),
            })
        }
    }

    /// Fill each graph entry's `uri` from `storage`/`server` and validate the
    /// locator shape (RFC-002 §1.2/§3), so existing `uri`-readers see a
    /// resolved base URI and invalid combinations fail at load.
    fn normalize_graphs(&mut self) -> Result<()> {
        let endpoints: BTreeMap<String, String> = self
            .servers
            .iter()
            .map(|(name, entry)| (name.clone(), entry.endpoint.clone()))
            .collect();
        for (key, entry) in self.graphs.iter_mut() {
            match (entry.storage.is_some(), entry.server.is_some()) {
                (true, true) => {
                    bail!("graph '{key}': set exactly one of `storage` or `server`, not both")
                }
                (true, false) => {
                    entry.uri = entry.storage.as_ref().unwrap().uri().to_string();
                }
                (false, true) => {
                    if entry.policy.file.is_some() || !entry.queries.is_empty() {
                        bail!(
                            "graph '{key}': a remote graph (`server:`) cannot define `policy` or \
                             `queries` — those are owned by the server it targets"
                        );
                    }
                    let server = entry.server.as_deref().unwrap();
                    let endpoint = endpoints.get(server).ok_or_else(|| {
                        color_eyre::eyre::eyre!(
                            "graph '{key}': server '{server}' is not defined in `servers:`"
                        )
                    })?;
                    entry.uri = endpoint.clone();
                    if entry.graph_id.is_none() {
                        entry.graph_id = Some(key.clone());
                    }
                }
                (false, false) => {
                    if entry.uri.is_empty() {
                        bail!("graph '{key}': set `storage:` (embedded) or `server:` (remote)");
                    }
                    // Legacy `uri:` — kept as-is; classified by scheme downstream.
                }
            }
        }
        Ok(())
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
            color_eyre::eyre::eyre!("URI must be provided via <URI>, --graph, or config")
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
    let loaded_from_file = config_path.is_some();

    let mut config = if let Some(path) = &config_path {
        let text = fs::read_to_string(path)?;
        let mut unknown: Vec<String> = Vec::new();
        let de = serde_yaml::Deserializer::from_str(&text);
        let mut config: OmnigraphConfig =
            serde_ignored::deserialize(de, |key| unknown.push(key.to_string()))?;
        // Strictness is a function of the version, decided here — the one place
        // the loader holds both the parsed version and the set of ignored fields.
        // Legacy (no `version:`) tolerates unknown keys; `version: 1` rejects them
        // at any depth (honored-or-rejected, RFC-002 §3). The v1-only typed blocks
        // (`storage:`/`servers:`) enforce their own `deny_unknown_fields`.
        match config.version {
            Some(v) if v != 1 => bail!(
                "unsupported config version {v}; this build supports version 1 \
                 (omit `version:` for the legacy schema)"
            ),
            Some(1) if !unknown.is_empty() => {
                unknown.sort();
                bail!(
                    "unknown config field(s) under `version: 1`: {} \
                     (omit `version:` for the legacy lenient schema)",
                    unknown.join(", ")
                )
            }
            _ => {}
        }
        // Known-but-legacy top-level keys (renamed/removed by v1) are invisible
        // to `serde_ignored` because they stay parseable for the legacy schema,
        // so scan the raw text for them: reject under v1, record for the legacy
        // deprecation warnings otherwise.
        let legacy_keys = legacy_top_level_keys(&text);
        if config.version == Some(1) && !legacy_keys.is_empty() {
            let offenders = legacy_keys
                .iter()
                .map(|key| {
                    format!(
                        "`{key}:` — {}",
                        legacy_key_migration_hint(key).unwrap_or("")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n  ");
            bail!(
                "invalid key(s) under `version: 1`:\n  {offenders}\n(omit `version:` for the legacy lenient schema)"
            );
        }
        config.legacy_keys = legacy_keys;
        config
    } else {
        OmnigraphConfig::default()
    };

    config.base_dir = if let Some(path) = config_path {
        absolute_base_dir(cwd, &path)?
    } else {
        cwd.to_path_buf()
    };
    config.loaded_from_file = loaded_from_file;

    config.normalize_graphs()?;

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

/// Migration hint for a top-level YAML key that the RFC-002 v1 schema renames or
/// removes. `Some(hint)` ⇒ the key is invalid under `version: 1`; `None` ⇒ the
/// key is fine. These keys stay as known struct fields (honored under the legacy
/// schema), so `serde_ignored` cannot surface them — [`reject_legacy_top_level_keys_under_v1`]
/// scans for them explicitly instead (RFC-002 §3: honored-or-rejected, never
/// silently ignored). The set grows as each rename/removal lands.
fn legacy_key_migration_hint(key: &str) -> Option<&'static str> {
    match key {
        "project" => Some("remove it; it has no effect under `version: 1`"),
        "cli" => Some("rename to `defaults:`"),
        _ => None,
    }
}

/// The top-level keys present in the raw config text that v1 renames or removes
/// (those with a [`legacy_key_migration_hint`]), sorted. Scans the raw mapping
/// rather than the typed config because these keys are honored under the legacy
/// schema (so they stay parseable) — detection keys on the *presence* of the
/// key (an empty `cli: {}`/`policy: {}` under v1 is a silent no-op and must
/// error too), which only a raw scan can see. Drives both the `version: 1`
/// rejection and the legacy deprecation warnings.
fn legacy_top_level_keys(text: &str) -> Vec<String> {
    let mapping: serde_yaml::Mapping = serde_yaml::from_str(text).unwrap_or_default();
    let mut keys: Vec<String> = mapping
        .keys()
        .filter_map(serde_yaml::Value::as_str)
        .filter(|key| legacy_key_migration_hint(key).is_some())
        .map(str::to_string)
        .collect();
    keys.sort();
    keys
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use tempfile::tempdir;

    use super::{
        GraphLocator, ReadOutputFormat, TableCellLayout, graph_resource_id_for_selection,
        load_config_in,
    };

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
        assert_eq!(config.default_graph_name(), Some("local"));
        assert_eq!(config.default_branch(), "main");
        assert_eq!(config.default_output_format(), ReadOutputFormat::Kv);
        assert_eq!(config.table_max_column_width(), 40);
        assert_eq!(config.table_cell_layout(), TableCellLayout::Wrap);
        assert_eq!(
            config.graph_bearer_token_env(None, None, config.default_graph_name()),
            Some("DEMO_TOKEN")
        );
        assert_eq!(
            config.resolve_auth_env_file().unwrap(),
            temp.path().join(".env.omni")
        );
        assert_eq!(
            PathBuf::from(
                config
                    .resolve_target_uri(None, None, config.default_graph_name())
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
    fn graph_resource_id_for_selection_uses_name_or_anonymous_uri() {
        assert_eq!(
            graph_resource_id_for_selection(Some("local"), "/tmp/graph.omni"),
            "local"
        );
        assert_eq!(
            graph_resource_id_for_selection(None, "/tmp/graph.omni"),
            "/tmp/graph.omni"
        );
    }

    #[test]
    fn resolve_graph_selection_validates_membership_and_coherence() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "graphs:\n  local:\n    uri: ./demo.omni\n",
        )
        .unwrap();
        let config = load_config_in(temp.path(), None).unwrap();

        // A known graph passes through unchanged.
        assert_eq!(
            config.resolve_graph_selection(Some("local")).unwrap(),
            Some("local")
        );
        // An anonymous selection stays anonymous (→ top-level registry downstream).
        assert_eq!(config.resolve_graph_selection(None).unwrap(), None);
        // An unknown name errors, naming the graph (matching resolve_target_uri).
        let err = config
            .resolve_graph_selection(Some("ghost"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("ghost") && err.contains("not found"),
            "unknown graph must error naming it: {err}"
        );

        // Coherence: a named graph plus a populated top-level block is the
        // config server boot refuses, so the gate rejects it too (shared rule
        // via ensure_top_level_blocks_honored). An anonymous selection still
        // passes — top-level is honored when no graph is named.
        let temp2 = tempdir().unwrap();
        fs::write(
            temp2.path().join("omnigraph.yaml"),
            "graphs:\n  local:\n    uri: ./demo.omni\npolicy:\n  file: ./top.yaml\n",
        )
        .unwrap();
        let incoherent = load_config_in(temp2.path(), None).unwrap();
        let err = incoherent
            .resolve_graph_selection(Some("local"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("local") && err.contains("policy.file"),
            "named graph + populated top-level block must be rejected, naming both: {err}"
        );
        assert_eq!(
            incoherent.resolve_graph_selection(None).unwrap(),
            None,
            "anonymous selection still honors top-level"
        );
    }

    #[test]
    fn policy_tooling_graph_selection_prefers_cli_then_server_and_validates() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "graphs:\n  local:\n    uri: ./local.omni\n  prod:\n    uri: ./prod.omni\n\
             server:\n  graph: local\ncli:\n  graph: prod\n",
        )
        .unwrap();
        let config = load_config_in(temp.path(), None).unwrap();
        assert_eq!(
            config.resolve_policy_tooling_graph_selection().unwrap(),
            Some("prod")
        );

        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "graphs:\n  local:\n    uri: ./local.omni\nserver:\n  graph: local\n",
        )
        .unwrap();
        let config = load_config_in(temp.path(), None).unwrap();
        assert_eq!(
            config.resolve_policy_tooling_graph_selection().unwrap(),
            Some("local")
        );

        let temp = tempdir().unwrap();
        fs::write(temp.path().join("omnigraph.yaml"), "policy: {}\n").unwrap();
        let config = load_config_in(temp.path(), None).unwrap();
        assert_eq!(
            config.resolve_policy_tooling_graph_selection().unwrap(),
            None
        );

        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "graphs:\n  local:\n    uri: ./local.omni\nserver:\n  graph: ghost\n",
        )
        .unwrap();
        let config = load_config_in(temp.path(), None).unwrap();
        let err = config
            .resolve_policy_tooling_graph_selection()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("ghost") && err.contains("not found"),
            "unknown server.graph must use graph-selection validation: {err}"
        );
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
        // Default exposure is true (the manifest entry is the opt-in); tool_name absent.
        let audit = &prod["internal_audit"];
        assert!(audit.mcp.expose);
        assert!(audit.mcp.tool_name.is_none());

        // Top-level registry (single-graph mode).
        assert_eq!(config.query_entries().len(), 1);

        // The shared selector resolves the same blocks the server boot
        // and the CLI use: a known graph → its per-graph block; no
        // selection or an unknown name → the top-level block (the latter
        // pins the behavior of the CLI's now-deleted fallback arm).
        assert_eq!(config.query_entries_for(Some("prod")).len(), 2);
        assert_eq!(config.query_entries_for(None).len(), 1);
        assert_eq!(config.query_entries_for(Some("nonexistent")).len(), 1);

        // Path resolution joins against base_dir, like policy files.
        assert_eq!(
            config.resolve_query_file(&find_user.file),
            temp.path().join("./queries/find_user.gq")
        );
    }

    #[test]
    fn resolve_policy_file_for_follows_identity() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "policy:\n  file: ./top.yaml\ngraphs:\n  prod:\n    uri: s3://b/prod\n    \
             policy:\n      file: ./prod.yaml\n  bare:\n    uri: s3://b/bare\n",
        )
        .unwrap();
        let config = load_config_in(temp.path(), None).unwrap();

        // Named graph with its own policy → per-graph (not top-level).
        assert!(
            config
                .resolve_policy_file_for(Some("prod"))
                .unwrap()
                .ends_with("prod.yaml")
        );
        // Named graph with NO per-graph policy → None (no top-level fallback;
        // load-bearing for the boot coherence check).
        assert!(config.resolve_policy_file_for(Some("bare")).is_none());
        // Anonymous (bare URI) or an unknown name → top-level.
        assert!(
            config
                .resolve_policy_file_for(None)
                .unwrap()
                .ends_with("top.yaml")
        );
        assert!(
            config
                .resolve_policy_file_for(Some("nope"))
                .unwrap()
                .ends_with("top.yaml")
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
        assert!(config.target_query_entries("local").unwrap().is_empty());
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
                config.default_graph_name()
            ),
            None
        );
        assert_eq!(
            config.graph_bearer_token_env(
                Some("https://override.example.com"),
                Some("demo"),
                config.default_graph_name()
            ),
            Some("DEMO_TOKEN")
        );
    }

    #[test]
    fn version_one_parses_like_legacy() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "version: 1\ngraphs:\n  local:\n    uri: ./demo.omni\ndefaults:\n  graph: local\n",
        )
        .unwrap();
        let config = load_config_in(temp.path(), None).unwrap();
        assert_eq!(config.default_graph_name(), Some("local"));
        assert_eq!(
            PathBuf::from(
                config
                    .resolve_target_uri(None, None, config.default_graph_name())
                    .unwrap()
            ),
            temp.path().join("./demo.omni")
        );
    }

    #[test]
    fn unsupported_config_version_errors() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            "version: 2\ngraphs:\n  local:\n    uri: ./demo.omni\n",
        )
        .unwrap();
        let err = load_config_in(temp.path(), None).unwrap_err().to_string();
        assert!(
            err.contains("unsupported config version 2"),
            "config version > 1 must be rejected: {err}"
        );
    }

    #[test]
    fn legacy_config_tolerates_unknown_fields() {
        // No `version:` ⇒ legacy schema: an unrecognized key is tolerated, not
        // rejected (RFC-002: a missing `version:` is the lenient legacy shape).
        // Pins the contract that `version` — not an unconditional struct
        // attribute — gates strictness; a `version: 1` config rejects the same
        // key (see `version_one_rejects_unknown_nested_field`). A top-level key
        // is used deliberately: that is the level a struct-wide
        // `deny_unknown_fields` catches today, so this reproduces the bug.
        let config = load_yaml(
            "graphs:\n  local:\n    uri: ./demo.omni\n\
             future_top_level_key: whatever\ncli:\n  graph: local\n",
        );
        assert_eq!(config.default_graph_name(), Some("local"));
    }

    #[test]
    fn version_one_rejects_unknown_nested_field() {
        // `version: 1` is strict at ALL depths via serde_ignored, not only at the
        // structs that carry their own `deny_unknown_fields`: a typo inside a
        // nested block such as `defaults:` must error, naming the offending key
        // (honored-or-rejected, RFC-002 §3).
        let err = load_yaml_err(
            "version: 1\ngraphs:\n  local:\n    uri: ./demo.omni\n\
             defaults:\n  graph: local\n  outout_format: kv\n",
        );
        assert!(
            err.contains("outout_format") || err.contains("unknown config field"),
            "v1 must reject an unknown nested field, naming it: {err}"
        );
    }

    #[test]
    fn version_one_rejects_legacy_cli_key() {
        // `cli:` is renamed to `defaults:` under v1; writing it is a hard error
        // that points at the new spelling (a known struct field via the `cli`
        // alias, so only the raw key scan catches it).
        let err = load_yaml_err(
            "version: 1\ngraphs:\n  local:\n    storage: ./demo.omni\ncli:\n  graph: local\n",
        );
        assert!(
            err.contains("cli") && err.contains("rename to `defaults:`"),
            "v1 must reject `cli:` and point at `defaults:`: {err}"
        );
    }

    #[test]
    fn version_one_honors_defaults_block() {
        let config = load_yaml(
            "version: 1\ngraphs:\n  local:\n    storage: ./demo.omni\n\
             defaults:\n  graph: local\n  branch: dev\n  output_format: kv\n",
        );
        assert_eq!(config.default_graph_name(), Some("local"));
        assert_eq!(config.default_branch(), "dev");
        assert_eq!(config.default_output_format(), ReadOutputFormat::Kv);
        assert!(
            config.deprecation_warnings().is_empty(),
            "clean v1 `defaults:` config must not warn: {:?}",
            config.deprecation_warnings()
        );
    }

    #[test]
    fn legacy_cli_key_honored_via_alias_and_warned() {
        // No `version:` ⇒ the legacy `cli:` spelling is accepted (serde alias of
        // `defaults`) and still honored, but flagged for migration.
        let config = load_yaml("graphs:\n  local:\n    uri: ./demo.omni\ncli:\n  graph: local\n");
        assert_eq!(config.default_graph_name(), Some("local"));
        assert!(
            config
                .deprecation_warnings()
                .iter()
                .any(|w| w.contains("cli") && w.contains("defaults")),
            "legacy `cli:` must warn to migrate: {:?}",
            config.deprecation_warnings()
        );
    }

    #[test]
    fn version_one_rejects_legacy_project_key() {
        // `version: 1` removes the `project:` block (no consumer). Writing it
        // under v1 is a hard error that names the key (honored-or-rejected,
        // RFC-002 §3) — `serde_ignored` cannot catch it because `project` is a
        // known struct field kept for legacy parsing.
        let err = load_yaml_err(
            "version: 1\nproject:\n  name: x\ngraphs:\n  local:\n    storage: ./demo.omni\n",
        );
        assert!(
            err.contains("project"),
            "v1 must reject the legacy `project:` key, naming it: {err}"
        );
    }

    #[test]
    fn legacy_project_key_tolerated_without_version() {
        // No `version:` ⇒ legacy schema: `project:` still parses (it has no
        // effect, but is not rejected) — only `version: 1` is strict about it.
        let config = load_yaml("project:\n  name: x\ngraphs:\n  local:\n    uri: ./demo.omni\n");
        assert!(config.graphs.contains_key("local"));
    }

    #[test]
    fn deprecation_warnings_flag_legacy_schema_and_uri() {
        let config = load_yaml("graphs:\n  local:\n    uri: ./demo.omni\n");
        let warnings = config.deprecation_warnings();
        assert!(
            warnings.iter().any(|w| w.contains("version:")),
            "expected no-version notice: {warnings:?}"
        );
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("local") && w.contains("uri:")),
            "expected legacy-uri notice: {warnings:?}"
        );
    }

    #[test]
    fn deprecation_warnings_silent_for_clean_v1_config() {
        let config = load_yaml("version: 1\ngraphs:\n  local:\n    storage: ./demo.omni\n");
        assert!(
            config.deprecation_warnings().is_empty(),
            "clean v1 config must not warn: {:?}",
            config.deprecation_warnings()
        );
    }

    #[test]
    fn deprecation_warnings_flag_legacy_uri_under_v1() {
        // `version: 1` but still using `uri:` → only the per-graph notice (no
        // no-version notice, since `version:` is present).
        let config = load_yaml("version: 1\ngraphs:\n  local:\n    uri: ./demo.omni\n");
        let warnings = config.deprecation_warnings();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("local") && warnings[0].contains("uri:"));
    }

    #[test]
    fn deprecation_warnings_empty_without_config_file() {
        // No `omnigraph.yaml` → default config → nothing to migrate, no notice.
        let temp = tempdir().unwrap();
        let config = load_config_in(temp.path(), None).unwrap();
        assert!(config.deprecation_warnings().is_empty());
    }

    fn load_yaml(yaml: &str) -> super::OmnigraphConfig {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("omnigraph.yaml"), yaml).unwrap();
        load_config_in(temp.path(), None).unwrap()
    }

    fn load_yaml_err(yaml: &str) -> String {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("omnigraph.yaml"), yaml).unwrap();
        load_config_in(temp.path(), None).unwrap_err().to_string()
    }

    #[test]
    fn storage_bare_resolves_embedded() {
        let config = load_yaml("version: 1\ngraphs:\n  local:\n    storage: ./demo.omni\n");
        match config.resolve_graph(None, Some("local")).unwrap() {
            GraphLocator::Embedded {
                uri,
                selected,
                graph_id,
                ..
            } => {
                assert!(uri.ends_with("demo.omni"), "uri: {uri}");
                assert_eq!(selected.as_deref(), Some("local"));
                assert_eq!(graph_id, "local");
            }
            other => panic!("expected embedded, got {other:?}"),
        }
    }

    #[test]
    fn storage_block_carries_region_and_endpoint() {
        let config = load_yaml(
            "version: 1\ngraphs:\n  prod:\n    storage:\n      uri: s3://b/prod.omni\n      \
             region: eu-west-1\n      endpoint: https://minio.local\n",
        );
        match config.resolve_graph(None, Some("prod")).unwrap() {
            GraphLocator::Embedded {
                uri,
                region,
                endpoint,
                ..
            } => {
                assert_eq!(uri, "s3://b/prod.omni");
                assert_eq!(region.as_deref(), Some("eu-west-1"));
                assert_eq!(endpoint.as_deref(), Some("https://minio.local"));
            }
            other => panic!("expected embedded, got {other:?}"),
        }
    }

    #[test]
    fn remote_server_resolves_graph_id_defaulting_to_key() {
        let config = load_yaml(
            "version: 1\nservers:\n  prod:\n    endpoint: https://og.internal:8080\n\
             graphs:\n  reports:\n    server: prod\n",
        );
        match config.resolve_graph(None, Some("reports")).unwrap() {
            GraphLocator::Remote {
                endpoint,
                server,
                graph_id,
                ..
            } => {
                assert_eq!(endpoint, "https://og.internal:8080");
                assert_eq!(server, "prod");
                assert_eq!(graph_id, "reports", "graph_id defaults to the entry key");
            }
            other => panic!("expected remote, got {other:?}"),
        }
    }

    #[test]
    fn qualified_server_slash_graph_resolves_without_an_alias() {
        let config =
            load_yaml("version: 1\nservers:\n  prod:\n    endpoint: https://og.internal:8080\n");
        match config.resolve_graph(None, Some("prod/production")).unwrap() {
            GraphLocator::Remote {
                endpoint,
                server,
                graph_id,
                ..
            } => {
                assert_eq!(endpoint, "https://og.internal:8080");
                assert_eq!(server, "prod");
                assert_eq!(graph_id, "production");
            }
            other => panic!("expected remote, got {other:?}"),
        }
    }

    #[test]
    fn storage_xor_server_both_set_is_rejected() {
        let err = load_yaml_err(
            "version: 1\nservers:\n  p:\n    endpoint: https://h\n\
             graphs:\n  bad:\n    storage: s3://b/x\n    server: p\n",
        );
        assert!(
            err.contains("exactly one of `storage` or `server`"),
            "{err}"
        );
    }

    #[test]
    fn graph_with_neither_storage_nor_server_is_rejected() {
        let err = load_yaml_err("version: 1\ngraphs:\n  bad:\n    branch: main\n");
        assert!(
            err.contains("set `storage:`") && err.contains("bad"),
            "{err}"
        );
    }

    #[test]
    fn remote_graph_with_policy_is_rejected() {
        let err = load_yaml_err(
            "version: 1\nservers:\n  p:\n    endpoint: https://h\n\
             graphs:\n  g:\n    server: p\n    policy:\n      file: ./p.yaml\n",
        );
        assert!(err.contains("cannot define `policy`"), "{err}");
    }

    #[test]
    fn remote_graph_referencing_unknown_server_is_rejected() {
        let err = load_yaml_err("version: 1\ngraphs:\n  g:\n    server: ghost\n");
        assert!(
            err.contains("ghost") && err.contains("not defined in `servers:`"),
            "{err}"
        );
    }

    #[test]
    fn unknown_graph_name_errors() {
        let config = load_yaml("version: 1\ngraphs:\n  local:\n    storage: ./demo.omni\n");
        let err = config
            .resolve_graph(None, Some("ghost"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ghost") && err.contains("not found"), "{err}");
    }

    #[test]
    fn storage_block_rejects_unknown_field() {
        // `profile` is intentionally unsupported; deny_unknown_fields catches it.
        let err = load_yaml_err(
            "version: 1\ngraphs:\n  g:\n    storage:\n      uri: s3://b/x\n      profile: default\n",
        );
        assert!(
            err.to_lowercase().contains("profile") || err.contains("unknown field"),
            "{err}"
        );
    }

    #[test]
    fn legacy_remote_uri_splits_graphs_suffix() {
        // No `version:` (legacy); a remote uri with the /graphs/{gid} hack splits.
        let config = load_yaml("graphs:\n  prod:\n    uri: https://host:8080/graphs/production\n");
        match config.resolve_graph(None, Some("prod")).unwrap() {
            GraphLocator::Remote {
                endpoint, graph_id, ..
            } => {
                assert_eq!(endpoint, "https://host:8080");
                assert_eq!(graph_id, "production");
            }
            other => panic!("expected remote, got {other:?}"),
        }
    }

    #[test]
    fn legacy_embedded_uri_still_resolves_embedded() {
        let config = load_yaml("graphs:\n  local:\n    uri: ./demo.omni\n");
        assert!(matches!(
            config.resolve_graph(None, Some("local")).unwrap(),
            GraphLocator::Embedded { .. }
        ));
    }
}
