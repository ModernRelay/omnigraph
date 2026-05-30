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

use omnigraph_compiler::query::ast::QueryDecl;
use omnigraph_compiler::query::parser::parse_query;

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
    /// Whether this query is listed in the MCP tool catalog. Default
    /// `false`: HTTP-callable but not MCP-enumerated until the operator
    /// opts in. Consulted by the catalog projection (a later slice).
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
}
