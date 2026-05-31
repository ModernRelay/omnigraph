//! Stored-query registry.
//!
//! A server-side registry of named, parameter-typed `.gq` queries that
//! operators declare in `omnigraph.yaml` (per-graph, or top-level in
//! single mode) and the server loads at startup. Each entry is parsed
//! and its identity asserted here (`load`); type-checking against the
//! live schema happens separately (a `check` pass) so the loader stays
//! callable without an open engine (the CLI's offline `queries check`).
//!
//! Identity is the query **name**: the manifest key must equal the
//! `query <name>` symbol declared in the referenced `.gq` file. The two
//! are asserted equal at load — one name, two places that must agree.
//! Renaming either is a breaking change to callers, by design.

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::query::ast::QueryDecl;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::query::typecheck::typecheck_query_decl;
use omnigraph_compiler::types::{PropType, ScalarType};

use crate::config::{OmnigraphConfig, QueryEntry};

/// One loaded stored query. `source` is the full `.gq` file text — the
/// invocation handler hands it to `run_query` / `run_mutate` verbatim,
/// which reuse the same parse/IR/exec path as the inline routes (no
/// parallel implementation).
#[derive(Debug, Clone)]
pub struct StoredQuery {
    /// Identity: manifest key == `query <name>` symbol.
    pub name: String,
    /// Full `.gq` source text the query was selected from.
    pub source: Arc<str>,
    /// Parsed declaration (params, mutations, description, …).
    pub decl: QueryDecl,
    /// Whether this query is listed in the MCP tool catalog (`GET /queries`).
    /// Default `true` (the manifest entry is the opt-in); `expose: false`
    /// keeps it HTTP/service-callable but hidden from the agent tool list.
    /// Catalog membership only — not an authorization gate.
    pub expose: bool,
    /// Optional MCP tool-name override; defaults to `name`.
    pub tool_name: Option<String>,
}

impl StoredQuery {
    /// `true` if the selected declaration contains insert/update/delete
    /// statements — drives read-vs-mutate routing at invocation time.
    pub fn is_mutation(&self) -> bool {
        !self.decl.mutations.is_empty()
    }

    /// The MCP tool name this query is catalogued under: the explicit
    /// `tool_name` override, else the query `name`. The catalog key —
    /// enforced unique across exposed queries at load. Server-side
    /// consumers (the uniqueness check, the future catalog projection) read
    /// this; the CLI `queries list` resolves the same rule on its own DTO.
    pub fn effective_tool_name(&self) -> &str {
        self.tool_name.as_deref().unwrap_or(&self.name)
    }
}

/// A loaded, identity-checked stored-query registry for one graph.
#[derive(Debug, Clone, Default)]
pub struct QueryRegistry {
    by_name: BTreeMap<String, StoredQuery>,
}

/// In-memory registry entry before file I/O. Used by [`QueryRegistry::load`]
/// (after reading each `.gq` from disk) and directly by tests.
#[derive(Debug, Clone)]
pub struct RegistrySpec {
    pub name: String,
    pub source: String,
    pub expose: bool,
    pub tool_name: Option<String>,
}

/// A single registry load failure. Collected (not fail-fast) so a bad
/// `omnigraph.yaml` surfaces every broken entry at once, matching the
/// bad-policy-YAML posture.
#[derive(Debug, Clone)]
pub struct LoadError {
    /// The offending query name, when the failure is entry-scoped.
    pub query: Option<String>,
    pub message: String,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.query {
            Some(name) => write!(f, "stored query '{name}': {}", self.message),
            None => write!(f, "stored query registry: {}", self.message),
        }
    }
}

impl QueryRegistry {
    /// Build a registry from in-memory specs: parse each source, select
    /// the declaration whose symbol equals the manifest key, and assert
    /// they agree. Collects every failure. No schema type-checking here
    /// — that is [`check`].
    pub fn from_specs(specs: Vec<RegistrySpec>) -> Result<Self, Vec<LoadError>> {
        let mut by_name = BTreeMap::new();
        let mut errors = Vec::new();

        for spec in specs {
            match parse_query(&spec.source) {
                Ok(file) => {
                    match file.queries.into_iter().find(|q| q.name == spec.name) {
                        Some(decl) => {
                            by_name.insert(
                                spec.name.clone(),
                                StoredQuery {
                                    name: spec.name,
                                    source: Arc::from(spec.source),
                                    decl,
                                    expose: spec.expose,
                                    tool_name: spec.tool_name,
                                },
                            );
                        }
                        None => errors.push(LoadError {
                            query: Some(spec.name.clone()),
                            message: format!(
                                "no `query {}` declaration found in its `.gq` file \
                                 (the registry key must match the query symbol)",
                                spec.name
                            ),
                        }),
                    }
                }
                Err(err) => errors.push(LoadError {
                    query: Some(spec.name),
                    message: format!("parse error: {err}"),
                }),
            }
        }

        // Exposed queries are catalogued under their effective tool name;
        // two claiming one name is an MCP-namespace collision. Refuse it at
        // load (collected, not fail-fast), naming the loser and the winner.
        // Iterating the `BTreeMap` makes the winner deterministic (the
        // lexicographically-first query name; config is a map, so YAML
        // declaration order isn't preserved anyway) and the error order
        // stable. Scoped to a block so these borrows of `by_name` end
        // before it is moved into `Self`.
        {
            let mut claimed: BTreeMap<&str, &str> = BTreeMap::new();
            for query in by_name.values().filter(|q| q.expose) {
                let tool = query.effective_tool_name();
                if let Some(winner) = claimed.insert(tool, &query.name) {
                    errors.push(LoadError {
                        query: Some(query.name.clone()),
                        message: format!(
                            "MCP tool name '{tool}' already claimed by exposed query '{winner}'"
                        ),
                    });
                }
            }
        }

        if errors.is_empty() {
            Ok(Self { by_name })
        } else {
            Err(errors)
        }
    }

    /// Read each registry entry's `.gq` file from disk and build the
    /// registry. `entries` is either the top-level `queries` map (single
    /// mode) or a graph's `queries` map (multi mode); `config` resolves
    /// each entry's relative `file:` path against `base_dir`.
    pub fn load(
        config: &OmnigraphConfig,
        entries: &BTreeMap<String, QueryEntry>,
    ) -> Result<Self, Vec<LoadError>> {
        let mut specs = Vec::with_capacity(entries.len());
        let mut errors = Vec::new();
        for (name, entry) in entries {
            let path = config.resolve_query_file(&entry.file);
            match fs::read_to_string(&path) {
                Ok(source) => specs.push(RegistrySpec {
                    name: name.clone(),
                    source,
                    expose: entry.mcp.expose,
                    tool_name: entry.mcp.tool_name.clone(),
                }),
                Err(err) => errors.push(LoadError {
                    query: Some(name.clone()),
                    message: format!("cannot read '{}': {err}", path.display()),
                }),
            }
        }

        if !errors.is_empty() {
            return Err(errors);
        }
        Self::from_specs(specs)
    }

    pub fn lookup(&self, name: &str) -> Option<&StoredQuery> {
        self.by_name.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &StoredQuery> {
        self.by_name.values()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_name.len()
    }
}

/// A stored query that fails to type-check against the live schema —
/// e.g. it references a node/edge type or property that was renamed or
/// removed by a migration. Breakages **block server boot** (same posture
/// as bad policy YAML), surfacing schema drift at the deploy boundary
/// rather than silently at invocation time.
#[derive(Debug, Clone)]
pub struct Breakage {
    pub query: String,
    pub message: String,
}

/// A non-blocking advisory found during validation. Logged at boot;
/// never blocks startup. Currently: an MCP-exposed query that declares a
/// parameter an agent cannot realistically supply.
#[derive(Debug, Clone)]
pub struct Warning {
    pub query: String,
    pub message: String,
}

/// Outcome of validating a registry against a schema. Breakages are
/// fatal (boot refuses); warnings are advisory.
#[derive(Debug, Clone, Default)]
pub struct CheckReport {
    pub breakages: Vec<Breakage>,
    pub warnings: Vec<Warning>,
}

impl CheckReport {
    pub fn has_breakages(&self) -> bool {
        !self.breakages.is_empty()
    }

    pub fn is_clean(&self) -> bool {
        self.breakages.is_empty() && self.warnings.is_empty()
    }
}

/// Validate a loaded registry against the live schema.
///
/// Pure over `(registry, catalog)` — takes an already-parsed registry and
/// a catalog, so it is callable both at server boot (with the engine's
/// `catalog()`) and offline from the CLI (`omnigraph queries check`),
/// without coupling to server config or an open engine connection.
///
/// Every query is type-checked via the same `typecheck_query_decl` the
/// engine runs for inline queries — no parallel implementation. Failures
/// are **collected, not fail-fast**, so an operator sees every broken
/// query in one pass.
///
/// Advisory lint (warn, never block): an `mcp.expose: true` query that
/// declares a `Vector(N)` parameter. An LLM cannot supply a raw embedding
/// vector; such a query should take a `String` parameter and let the
/// engine embed it server-side at query time. Service-to-service callers
/// may legitimately pass vectors, so this warns rather than rejects.
pub fn check(registry: &QueryRegistry, catalog: &Catalog) -> CheckReport {
    let mut report = CheckReport::default();
    for query in registry.iter() {
        if let Err(err) = typecheck_query_decl(catalog, &query.decl) {
            report.breakages.push(Breakage {
                query: query.name.clone(),
                message: err.to_string(),
            });
        }
        if query.expose {
            for param in &query.decl.params {
                // Resolve to the structured type via the compiler's own
                // resolver rather than string-matching `Vector(` — one
                // canonical definition of "is a vector", so this lint can't
                // drift from how the parser/type system spells the type.
                let is_vector = PropType::from_param_type_name(&param.type_name, param.nullable)
                    .is_some_and(|pt| matches!(pt.scalar, ScalarType::Vector(_)));
                if is_vector {
                    report.warnings.push(Warning {
                        query: query.name.clone(),
                        message: format!(
                            "MCP-exposed query declares a `{}` parameter `${}` that agents \
                             cannot supply; use a `String` parameter for server-side embedding",
                            param.type_name, param.name
                        ),
                    });
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, source: &str, expose: bool) -> RegistrySpec {
        RegistrySpec {
            name: name.to_string(),
            source: source.to_string(),
            expose,
            tool_name: None,
        }
    }

    fn spec_tool(name: &str, source: &str, expose: bool, tool_name: &str) -> RegistrySpec {
        RegistrySpec {
            name: name.to_string(),
            source: source.to_string(),
            expose,
            tool_name: Some(tool_name.to_string()),
        }
    }

    #[test]
    fn key_equal_symbol_loads() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "find_user",
            "query find_user($id: String) { match { $u: User } return { $u.name } }",
            true,
        )])
        .unwrap();
        let q = reg.lookup("find_user").unwrap();
        assert_eq!(q.name, "find_user");
        assert!(q.expose);
        assert_eq!(q.decl.params.len(), 1);
        assert!(!q.is_mutation());
        // No override → the effective tool name is the query name.
        assert_eq!(q.effective_tool_name(), "find_user");

        // An explicit override is what the catalog keys on.
        let with_tool = QueryRegistry::from_specs(vec![spec_tool(
            "find_user",
            "query find_user($id: String) { match { $u: User } return { $u.name } }",
            true,
            "lookup_user",
        )])
        .unwrap();
        assert_eq!(
            with_tool.lookup("find_user").unwrap().effective_tool_name(),
            "lookup_user"
        );
    }

    #[test]
    fn key_mismatch_is_an_identity_error() {
        let errors = QueryRegistry::from_specs(vec![spec(
            "find_user",
            // symbol is `lookup`, key is `find_user` — must be rejected.
            "query lookup($id: String) { match { $u: User } return { $u.name } }",
            false,
        )])
        .unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].query.as_deref(), Some("find_user"));
        assert!(errors[0].message.contains("must match the query symbol"));
    }

    #[test]
    fn multi_query_file_selects_the_matching_symbol() {
        let source = "query a($x: I64) { match { $u: User } return { $u.name } }\n\
                      query b($y: String) { match { $u: User } return { $u.name } }";
        let reg = QueryRegistry::from_specs(vec![spec("b", source, false)]).unwrap();
        let q = reg.lookup("b").unwrap();
        assert_eq!(q.name, "b");
        assert_eq!(q.decl.params[0].name, "y");
        assert!(reg.lookup("a").is_none(), "only the selected symbol is registered");
    }

    #[test]
    fn duplicate_exposed_tool_name_is_a_load_error() {
        // Two MCP-exposed queries claiming one tool name is an ambiguity in
        // the catalog key space — refused at load, naming both queries and
        // the contested tool.
        let errors = QueryRegistry::from_specs(vec![
            spec_tool("a", "query a() { match { $u: User } return { $u.name } }", true, "dup"),
            spec_tool("b", "query b() { match { $u: User } return { $u.name } }", true, "dup"),
        ])
        .unwrap_err();
        assert_eq!(errors.len(), 1);
        let msg = errors[0].to_string();
        assert!(msg.contains("'dup'"), "names the contested tool: {msg}");
        assert!(msg.contains("'a'"), "names the winning query: {msg}");
        assert!(msg.contains("'b'"), "names the losing query: {msg}");
    }

    #[test]
    fn duplicate_tool_name_among_unexposed_is_allowed() {
        // Unexposed queries have no MCP tool, so a shared effective tool
        // name is inert — must not error (pins the exposed-only scope).
        let reg = QueryRegistry::from_specs(vec![
            spec_tool("a", "query a() { match { $u: User } return { $u.name } }", false, "dup"),
            spec_tool("b", "query b() { match { $u: User } return { $u.name } }", false, "dup"),
        ])
        .unwrap();
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn parse_error_surfaces_per_entry() {
        let errors =
            QueryRegistry::from_specs(vec![spec("broken", "query broken( {{ not valid", false)])
                .unwrap_err();
        assert_eq!(errors[0].query.as_deref(), Some("broken"));
        assert!(errors[0].message.contains("parse error"));
    }

    #[test]
    fn errors_collect_rather_than_fail_fast() {
        let errors = QueryRegistry::from_specs(vec![
            spec("good", "query good() { match { $u: User } return { $u.name } }", false),
            spec("mismatch", "query other() { match { $u: User } return { $u.name } }", false),
            spec("broken", "query broken(", false),
        ])
        .unwrap_err();
        // `good` loads cleanly; only the mismatch and the parse error are
        // reported, and both surface in one pass (not fail-fast).
        assert_eq!(errors.len(), 2);
    }

    #[test]
    fn mutation_body_classifies_as_mutation() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "add_user",
            "query add_user($name: String) { insert User { name: $name } }",
            false,
        )])
        .unwrap();
        assert!(reg.lookup("add_user").unwrap().is_mutation());
    }

    // --- check(registry, catalog) ---

    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::schema::parser::parse_schema;

    fn test_catalog() -> Catalog {
        let schema = parse_schema(
            r#"
node User {
name: String
age: I32?
embedding: Vector(4)
}
"#,
        )
        .unwrap();
        build_catalog(&schema).unwrap()
    }

    #[test]
    fn check_passes_for_valid_query() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "find_user",
            "query find_user($name: String) { match { $u: User { name: $name } } return { $u.age } }",
            false,
        )])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert!(report.is_clean(), "unexpected: {:?}", report);
    }

    #[test]
    fn check_reports_unknown_type_as_breakage() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "ghost",
            // `Widget` is not in the schema.
            "query ghost() { match { $w: Widget } return { $w.name } }",
            false,
        )])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert!(report.has_breakages());
        assert_eq!(report.breakages[0].query, "ghost");
    }

    #[test]
    fn check_reports_unknown_property_as_breakage() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "bad_prop",
            // `User` exists but has no `nickname`.
            "query bad_prop() { match { $u: User } return { $u.nickname } }",
            false,
        )])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert!(report.has_breakages());
        assert_eq!(report.breakages[0].query, "bad_prop");
    }

    #[test]
    fn check_collects_every_breakage_not_fail_fast() {
        let reg = QueryRegistry::from_specs(vec![
            spec("a", "query a() { match { $w: Widget } return { $w.x } }", false),
            spec("b", "query b() { match { $g: Gadget } return { $g.y } }", false),
            spec(
                "ok",
                "query ok() { match { $u: User } return { $u.name } }",
                false,
            ),
        ])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert_eq!(report.breakages.len(), 2, "both bad queries reported: {:?}", report);
    }

    #[test]
    fn vector_param_on_exposed_query_warns() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "vec_search",
            "query vec_search($q: Vector(4)) { match { $u: User } return { $u.name } \
             order { nearest($u.embedding, $q) } limit 3 }",
            true, // mcp.expose
        )])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert!(!report.has_breakages(), "valid query: {:?}", report);
        assert_eq!(report.warnings.len(), 1);
        assert_eq!(report.warnings[0].query, "vec_search");
    }

    #[test]
    fn vector_param_on_unexposed_query_is_silent() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "vec_search",
            "query vec_search($q: Vector(4)) { match { $u: User } return { $u.name } \
             order { nearest($u.embedding, $q) } limit 3 }",
            false, // not exposed — vector param is fine for service-to-service callers
        )])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert!(report.is_clean(), "unexpected: {:?}", report);
    }

    #[test]
    fn non_vector_param_on_exposed_query_does_not_warn() {
        // The recommended `String` alternative on an exposed query does not
        // resolve to a Vector, so the embedding advisory stays silent. Guards
        // the structured type check against a false positive (and pins that
        // only `Vector(_)` triggers the warning).
        let reg = QueryRegistry::from_specs(vec![spec(
            "search",
            "query search($name: String) { match { $u: User { name: $name } } return { $u.name } }",
            true,
        )])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert!(report.is_clean(), "no breakage or warning expected: {:?}", report);
    }

    // --- catalog projection (api::query_catalog_entry) ---

    #[test]
    fn catalog_entry_projects_every_param_kind() {
        use crate::api::{self, ParamKind};
        let reg = QueryRegistry::from_specs(vec![spec_tool(
            "all_types",
            "query all_types($s: String, $i: I32, $big: I64, $u: U64, $f: F64, $b: Bool, \
             $d: Date, $dt: DateTime, $blob: Blob, $opt: String?, $list: [I32], $vec: Vector(4)) \
             { match { $x: User } return { $x.name } }",
            true,
            "all",
        )])
        .unwrap();
        let entry = api::query_catalog_entry(reg.lookup("all_types").unwrap());
        assert_eq!(entry.name, "all_types");
        assert_eq!(entry.tool_name, "all");
        assert!(!entry.mutation);

        let by: std::collections::HashMap<_, _> =
            entry.params.iter().map(|p| (p.name.as_str(), p)).collect();
        assert_eq!(by["s"].kind, ParamKind::String);
        assert_eq!(by["i"].kind, ParamKind::Int);
        assert_eq!(by["big"].kind, ParamKind::BigInt, "I64 → bigint (string on the wire)");
        assert_eq!(by["u"].kind, ParamKind::BigInt, "U64 → bigint");
        assert_eq!(by["f"].kind, ParamKind::Float);
        assert_eq!(by["b"].kind, ParamKind::Bool);
        assert_eq!(by["d"].kind, ParamKind::Date);
        assert_eq!(by["dt"].kind, ParamKind::DateTime);
        assert_eq!(by["blob"].kind, ParamKind::Blob);
        assert!(!by["s"].nullable);
        assert!(by["opt"].nullable, "String? → nullable");
        assert_eq!(by["list"].kind, ParamKind::List);
        assert_eq!(by["list"].item_kind, Some(ParamKind::Int), "[I32] → list of int");
        assert_eq!(by["vec"].kind, ParamKind::Vector);
        assert_eq!(by["vec"].vector_dim, Some(4));
    }

    #[test]
    fn catalog_entry_flags_mutation_and_empty_params() {
        use crate::api;
        let reg = QueryRegistry::from_specs(vec![spec(
            "add_user",
            "query add_user($name: String) { insert User { name: $name } }",
            true,
        )])
        .unwrap();
        let entry = api::query_catalog_entry(reg.lookup("add_user").unwrap());
        assert!(entry.mutation, "insert body → mutation flag");

        let reg2 = QueryRegistry::from_specs(vec![spec(
            "no_params",
            "query no_params() { match { $u: User } return { $u.name } }",
            true,
        )])
        .unwrap();
        let entry2 = api::query_catalog_entry(reg2.lookup("no_params").unwrap());
        assert!(entry2.params.is_empty(), "no declared params → empty list");
    }
}
