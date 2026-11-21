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
use std::sync::Arc;

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::query::check::check_query_decl as typecheck_query_decl;
use omnigraph_compiler::query::file::QueryDecl;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::settings::STORED_QUERY_CARRIES_NO_SETTINGS;
use omnigraph_compiler::types::{PropType, ScalarType};

/// One loaded stored query. `source` is the full `.gq` file text — the
/// invocation handler hands it to `run_query` / `run_mutate` verbatim,
/// which reuse the same parse/IR/exec path as the inline routes (no
/// parallel implementation).
#[derive(Debug, Clone)]
pub struct StoredQuery {
    /// Identity: manifest key == `query <name>` symbol.
    pub name: String,
    /// The `.gq` file the query was selected from, every declaration the
    /// compiler refuses blanked (byte offsets kept): what an invocation runs.
    pub source: Arc<str>,
    /// Parsed declaration (params, mutations, description, …).
    pub decl: QueryDecl,
    /// The declarations of its file it calls: when one fails to type-check,
    /// this query is unavailable naming it.
    pub callees: Vec<QueryDecl>,
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
        self.decl.is_mutation()
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

/// A stored query this server does not serve: its source fails to compile
/// under this release (a parse or a type refusal, or a declaration the
/// registry key does not name). The graph stays served; this query alone is
/// unavailable, listed with its refusal, and invoking it answers that
/// refusal (the GQ 3 RFC, Compatibility).
#[derive(Debug, Clone)]
pub struct UnavailableQuery {
    pub name: String,
    /// Full `.gq` source text the query was registered with.
    pub source: Arc<str>,
    /// The refusal's one-line rendering.
    pub message: String,
    /// The compiler's diagnostic (code, position, fix, byte edits) behind
    /// the message, when the refusal carries one.
    pub diagnostic: Option<omnigraph_compiler::QueryDiagnostic>,
}

impl UnavailableQuery {
    /// The refusal as the upgrade contract states it ([`refusal_text`]).
    pub fn refusal(&self) -> String {
        refusal_text(&self.message, self.diagnostic.as_ref())
    }
}

/// A loaded, identity-checked stored-query registry for one graph.
///
/// A registry is strict or per-query. A strict one ([`Self::from_specs`])
/// refuses as a whole: one entry that fails to load or to type-check
/// refuses the registry, and with it the graph. A per-query one
/// ([`Self::from_specs_per_query`], the server's default) keeps every entry
/// that loads and type-checks and holds each other one as an
/// [`UnavailableQuery`].
#[derive(Debug, Clone, Default)]
pub struct QueryRegistry {
    by_name: BTreeMap<String, StoredQuery>,
    unavailable: BTreeMap<String, UnavailableQuery>,
    strict: bool,
}

/// In-memory registry spec: a query's name + already-read `.gq` source. The
/// input to [`QueryRegistry::from_specs`] — built by the server's cluster boot
/// and by the CLI's `queries` tooling from a cluster serving snapshot.
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
    pub diagnostic: Option<Box<omnigraph_compiler::QueryDiagnostic>>,
}

impl std::fmt::Display for LoadError {
    /// The refusal as the upgrade contract states it: the query, the code,
    /// where it fails, the message, the fix and its byte-range edits, so an
    /// agent or a person rewrites a stored source from the boot log alone.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.query {
            Some(name) => write!(f, "stored query '{name}': ")?,
            None => write!(f, "stored query registry: ")?,
        }
        write!(
            f,
            "{}",
            refusal_text(&self.message, self.diagnostic.as_deref())
        )
    }
}

/// A stored query's refusal as the upgrade contract states it: its code,
/// where it fails, the message, the fix and the fix's byte-range edits; the
/// bare message when the refusal carries no diagnostic.
pub fn refusal_text(
    message: &str,
    diagnostic: Option<&omnigraph_compiler::QueryDiagnostic>,
) -> String {
    let Some(diagnostic) = diagnostic else {
        return message.to_string();
    };
    let mut out = format!("error[{}]", diagnostic.code);
    if let Some(at) = &diagnostic.position {
        out.push_str(&format!(" at line {}, column {}", at.line, at.column));
    }
    out.push_str(&format!(": {}", diagnostic.message));
    if let Some(fix) = &diagnostic.fix {
        out.push_str(&format!("; fix: {fix}"));
    }
    if let Some(suggestion) = &diagnostic.suggestion {
        let edits: Vec<String> = suggestion
            .edits
            .iter()
            .map(|edit| {
                format!(
                    "bytes {}..{} -> {:?}",
                    edit.start, edit.end, edit.replacement
                )
            })
            .collect();
        if !edits.is_empty() {
            out.push_str(&format!("; edits: {}", edits.join(", ")));
        }
    }
    out
}

impl QueryRegistry {
    /// Build a registry from in-memory specs: parse each source, select
    /// the declaration whose symbol equals the manifest key, and assert
    /// they agree. Collects every failure. No schema type-checking here
    /// — that is [`check`]. The registry is strict: one failure refuses it.
    pub fn from_specs(specs: Vec<RegistrySpec>) -> Result<Self, Vec<LoadError>> {
        let registered: std::collections::BTreeSet<String> =
            specs.iter().map(|spec| spec.name.clone()).collect();
        let sources: std::collections::BTreeSet<String> =
            specs.iter().map(|spec| spec.source.clone()).collect();
        let (mut registry, mut errors) = Self::load(specs);
        // Strict refuses for any declaration of a stored file, registered
        // or only called.
        for source in &sources {
            for (name, refusal) in FileUnits::compile(source).refused() {
                if !registered.contains(name) {
                    errors.push(LoadError {
                        query: Some(name.to_string()),
                        message: refusal.0.clone(),
                        diagnostic: refusal.1.clone(),
                    });
                }
            }
        }
        if errors.is_empty() {
            registry.strict = true;
            Ok(registry)
        } else {
            Err(errors)
        }
    }

    /// [`Self::from_specs`], per query: an entry that fails to load is held
    /// as an [`UnavailableQuery`] with its refusal, and the others load. A
    /// failure no entry owns refuses the registry.
    pub fn from_specs_per_query(specs: Vec<RegistrySpec>) -> Result<Self, Vec<LoadError>> {
        let sources: BTreeMap<String, Arc<str>> = specs
            .iter()
            .map(|spec| (spec.name.clone(), Arc::from(spec.source.as_str())))
            .collect();
        let (mut registry, errors) = Self::load(specs);
        let mut registry_errors = Vec::new();
        for error in errors {
            let Some(name) = error.query.clone() else {
                registry_errors.push(error);
                continue;
            };
            registry.by_name.remove(&name);
            registry.unavailable.insert(
                name.clone(),
                UnavailableQuery {
                    source: sources.get(&name).cloned().unwrap_or_else(|| Arc::from("")),
                    name,
                    message: error.message,
                    diagnostic: error.diagnostic.map(|d| *d),
                },
            );
        }
        if registry_errors.is_empty() {
            Ok(registry)
        } else {
            Err(registry_errors)
        }
    }

    /// Whether one failing entry refuses the registry, and its graph.
    pub fn is_strict(&self) -> bool {
        self.strict
    }

    /// Hold each query `report` names as broken as an [`UnavailableQuery`]
    /// with its refusal, out of the queries served.
    pub fn withdraw_breakages(&mut self, report: &CheckReport) {
        for breakage in &report.breakages {
            let Some(query) = self.by_name.remove(&breakage.query) else {
                continue;
            };
            self.unavailable.insert(
                query.name.clone(),
                UnavailableQuery {
                    name: query.name,
                    source: query.source,
                    message: breakage.message.clone(),
                    diagnostic: breakage.diagnostic.clone(),
                },
            );
        }
    }

    /// The queries this server does not serve, by name.
    pub fn unavailable(&self) -> impl Iterator<Item = &UnavailableQuery> {
        self.unavailable.values()
    }

    /// The unavailable query of this name.
    pub fn lookup_unavailable(&self, name: &str) -> Option<&UnavailableQuery> {
        self.unavailable.get(name)
    }

    /// Every entry loaded, and every failure an entry or the registry
    /// owns. Each declaration of a file compiles on its own ([`FileUnits`]):
    /// one the compiler refuses is unavailable with its own diagnostic, one
    /// that calls an unavailable declaration of its file is unavailable
    /// naming it, and the others load.
    fn load(specs: Vec<RegistrySpec>) -> (Self, Vec<LoadError>) {
        let mut by_name = BTreeMap::new();
        let mut errors = Vec::new();
        let mut files: BTreeMap<String, FileUnits> = BTreeMap::new();

        for spec in specs {
            let file = files
                .entry(spec.source.clone())
                .or_insert_with(|| FileUnits::compile(&spec.source));
            match file.select(&spec.name) {
                Ok((decl, callees)) => {
                    by_name.insert(
                        spec.name.clone(),
                        StoredQuery {
                            name: spec.name,
                            source: Arc::clone(&file.served),
                            decl,
                            callees,
                            expose: spec.expose,
                            tool_name: spec.tool_name,
                        },
                    );
                }
                Err((message, diagnostic)) => errors.push(LoadError {
                    query: Some(spec.name.clone()),
                    message,
                    diagnostic,
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
                        diagnostic: None,
                    });
                }
            }
        }

        (
            Self {
                by_name,
                ..Self::default()
            },
            errors,
        )
    }

    pub fn lookup(&self, name: &str) -> Option<&StoredQuery> {
        self.by_name.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &StoredQuery> {
        self.by_name.values()
    }

    /// No query, served or unavailable.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty() && self.unavailable.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_name.len()
    }
}

/// A refusal: its message and, when it carries one, the compiler's
/// diagnostic.
type Refusal = (String, Option<Box<omnigraph_compiler::QueryDiagnostic>>);

/// One stored-query file, each declaration compiled on its own.
///
/// The file is split into its declarations by the compiler's lexical scan
/// (`declaration_units`), which needs no parse tree. Each declaration
/// compiles with every other declaration blanked (bytes kept, so positions
/// stay those of the file); a statement or settings line with no declaration
/// name stays in every compile, so its refusal holds for every declaration.
/// A declaration that calls a refused one of its file is refused naming it.
/// What is served is the file with every refused declaration blanked.
struct FileUnits {
    /// Each named declaration's own outcome.
    outcomes: BTreeMap<String, Result<(), Refusal>>,
    /// The declarations each named declaration calls, within the file.
    calls: BTreeMap<String, Vec<String>>,
    /// The file with every refused declaration blanked.
    served: Arc<str>,
    /// The served file's declarations, or its refusal.
    declarations: Result<Vec<QueryDecl>, Refusal>,
}

impl FileUnits {
    fn compile(source: &str) -> Self {
        use omnigraph_compiler::query::syntax::recover::declaration_units;
        let units = declaration_units(source);
        let names: std::collections::BTreeSet<&str> =
            units.iter().filter_map(|u| u.name.as_deref()).collect();
        let only = |keep: &[usize]| {
            let mut bytes = source.as_bytes().to_vec();
            for (index, unit) in units.iter().enumerate() {
                if unit.name.is_some() && !keep.contains(&index) {
                    for byte in &mut bytes[unit.start..unit.end] {
                        if *byte != b'\n' {
                            *byte = b' ';
                        }
                    }
                }
            }
            // Whole units of ASCII spaces over token boundaries.
            String::from_utf8(bytes).expect("blanking whole units keeps UTF-8")
        };
        let mut outcomes = BTreeMap::new();
        let mut calls = BTreeMap::new();
        for (index, unit) in units.iter().enumerate() {
            let Some(name) = &unit.name else { continue };
            let alone = declarations_of(&only(&[index])).map(|_| ());
            // A name declared twice keeps its first outcome; the served
            // file refuses the repetition for both.
            outcomes.entry(name.clone()).or_insert(alone);
            calls.entry(name.clone()).or_insert_with(|| {
                unit.called
                    .iter()
                    .filter(|called| *called != name && names.contains(called.as_str()))
                    .cloned()
                    .collect::<Vec<_>>()
            });
        }
        // A caller of a refused declaration is refused naming it, to the
        // fixed point.
        loop {
            let refused: Vec<(String, String)> = calls
                .iter()
                .filter(|(name, _)| outcomes.get(*name).is_some_and(Result::is_ok))
                .filter_map(|(name, called)| {
                    called
                        .iter()
                        .find(|c| outcomes.get(*c).is_some_and(Result::is_err))
                        .map(|c| (name.clone(), c.clone()))
                })
                .collect();
            if refused.is_empty() {
                break;
            }
            for (name, called) in refused {
                outcomes.insert(
                    name,
                    Err((
                        format!("calls `{called}`, a declaration of its file that is unavailable"),
                        None,
                    )),
                );
            }
        }
        let keep: Vec<usize> = units
            .iter()
            .enumerate()
            .filter(|(_, unit)| {
                unit.name
                    .as_ref()
                    .is_some_and(|name| outcomes.get(name).is_some_and(Result::is_ok))
            })
            .map(|(index, _)| index)
            .collect();
        let served = only(&keep);
        let declarations = declarations_of(&served);
        Self {
            outcomes,
            calls,
            served: Arc::from(served),
            declarations,
        }
    }

    /// Every refused declaration of the file, by name.
    fn refused(&self) -> impl Iterator<Item = (&str, &Refusal)> {
        self.outcomes
            .iter()
            .filter_map(|(name, outcome)| outcome.as_ref().err().map(|r| (name.as_str(), r)))
    }

    /// The declaration named `name` and the declarations it calls, or its
    /// refusal.
    fn select(&self, name: &str) -> Result<(QueryDecl, Vec<QueryDecl>), Refusal> {
        match self.outcomes.get(name) {
            Some(Err(refusal)) => return Err(refusal.clone()),
            Some(Ok(())) => {}
            // No declaration carries the name: a refusal of the file as a
            // whole (a statement, a settings line) is the reason.
            None if self.declarations.is_err() => {
                return Err(self.declarations.clone().unwrap_err());
            }
            None => {
                return Err((
                    format!(
                        "no `query {name}` declaration found in its `.gq` file \
                         (the registry key must match the query symbol)"
                    ),
                    None,
                ));
            }
        }
        let declarations = self.declarations.as_ref().map_err(Clone::clone)?;
        let find = |wanted: &str| declarations.iter().find(|q| q.name == wanted).cloned();
        let decl = find(name).ok_or_else(|| {
            (
                format!(
                    "no `query {name}` declaration found in its `.gq` file \
                     (the registry key must match the query symbol)"
                ),
                None,
            )
        })?;
        let callees = self
            .calls
            .get(name)
            .into_iter()
            .flatten()
            .filter_map(|called| find(called))
            .collect();
        Ok((decl, callees))
    }
}

/// The declarations of `source`, or its refusal.
fn declarations_of(source: &str) -> Result<Vec<QueryDecl>, Refusal> {
    let file = parse_query(source)
        .map_err(|err| (err.to_string(), err.diagnostic().cloned().map(Box::new)))?;
    if !file.settings.is_empty() {
        return Err((STORED_QUERY_CARRIES_NO_SETTINGS.to_string(), None));
    }
    file.into_declarations().map_err(|message| (message, None))
}

/// A stored query that fails to type-check against the live schema —
/// e.g. it references a node/edge type or property that was renamed or
/// removed by a migration, or written in a syntax this release refuses. In a
/// strict registry breakages **block the graph's startup** (same posture as
/// bad policy YAML); in a per-query one each broken query alone is
/// withdrawn ([`QueryRegistry::withdraw_breakages`]), surfacing the drift at
/// the deploy boundary rather than silently at invocation time.
#[derive(Debug, Clone)]
pub struct Breakage {
    pub query: String,
    pub message: String,
    /// The compiler's diagnostic (code, stage, expectation, fix) behind the
    /// message, when the refusal carries one.
    pub diagnostic: Option<omnigraph_compiler::QueryDiagnostic>,
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
            // A query whose own refusal comes from a declaration it calls is
            // unavailable naming that declaration.
            let refused_callee = query
                .callees
                .iter()
                .find(|callee| typecheck_query_decl(catalog, callee).is_err());
            report.breakages.push(match refused_callee {
                Some(callee) => Breakage {
                    query: query.name.clone(),
                    message: format!(
                        "calls `{}`, a declaration of its file that is unavailable",
                        callee.name
                    ),
                    diagnostic: None,
                },
                None => Breakage {
                    query: query.name.clone(),
                    message: err.to_string(),
                    diagnostic: err.diagnostic().cloned(),
                },
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

/// Format every breakage in a registry check report into a multi-line
/// operator-facing message, naming each offending query.
pub fn format_check_breakages(label: &str, report: &CheckReport) -> String {
    let joined = report
        .breakages
        .iter()
        .map(|b| {
            format!(
                "query '{}': {}",
                b.query,
                refusal_text(&b.message, b.diagnostic.as_ref())
            )
        })
        .collect::<Vec<_>>()
        .join("\n  ");
    format!(
        "graph '{label}': {} stored quer{} failed the schema check:\n  {joined}",
        report.breakages.len(),
        if report.breakages.len() == 1 {
            "y"
        } else {
            "ies"
        }
    )
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
        assert!(
            reg.lookup("a").is_none(),
            "only the selected symbol is registered"
        );
    }

    #[test]
    fn duplicate_exposed_tool_name_is_a_load_error() {
        // Two MCP-exposed queries claiming one tool name is an ambiguity in
        // the catalog key space — refused at load, naming both queries and
        // the contested tool.
        let errors = QueryRegistry::from_specs(vec![
            spec_tool(
                "a",
                "query a() { match { $u: User } return { $u.name } }",
                true,
                "dup",
            ),
            spec_tool(
                "b",
                "query b() { match { $u: User } return { $u.name } }",
                true,
                "dup",
            ),
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
            spec_tool(
                "a",
                "query a() { match { $u: User } return { $u.name } }",
                false,
                "dup",
            ),
            spec_tool(
                "b",
                "query b() { match { $u: User } return { $u.name } }",
                false,
                "dup",
            ),
        ])
        .unwrap();
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn parse_error_surfaces_per_entry() {
        let source = "query broken { match { $u: User } return { $u.name } }";
        let errors = QueryRegistry::from_specs(vec![spec("broken", source, false)]).unwrap_err();
        assert_eq!(errors[0].query.as_deref(), Some("broken"));
        let message = &errors[0].message;
        assert!(message.starts_with("parse error: "), "{message}");
        assert_eq!(message.matches("parse error:").count(), 1, "{message}");
        let diagnostic = errors[0].diagnostic.as_deref().unwrap();
        assert_eq!(
            diagnostic,
            parse_query(source).unwrap_err().diagnostic().unwrap()
        );
        assert_eq!(diagnostic.code.as_str(), "Q002");
        assert_eq!(
            diagnostic.suggestion.as_ref().unwrap().edits[0].replacement,
            "()"
        );
    }

    #[test]
    fn a_name_declared_twice_is_refused_per_entry() {
        let source = "query twice() { match { $u: User } return { $u.name } }\n\
                      query twice() { match { $u: User } return { $u } }";
        let errors = QueryRegistry::from_specs(vec![spec("twice", source, false)]).unwrap_err();
        assert_eq!(errors[0].query.as_deref(), Some("twice"));
        let diagnostic = errors[0].diagnostic.as_deref().unwrap();
        assert_eq!(diagnostic.code.as_str(), "T83");
        assert!(
            errors[0]
                .message
                .starts_with("type error: T83: `twice` is declared twice"),
            "{}",
            errors[0].message
        );
    }

    #[test]
    fn branch_statement_is_refused_per_entry() {
        let errors =
            QueryRegistry::from_specs(vec![spec("b0", "branch create b0", false)]).unwrap_err();
        assert_eq!(errors[0].query.as_deref(), Some("b0"));
        assert!(
            errors[0].message.contains("branch statement"),
            "{}",
            errors[0].message
        );
    }

    #[test]
    fn errors_collect_rather_than_fail_fast() {
        let errors = QueryRegistry::from_specs(vec![
            spec(
                "good",
                "query good() { match { $u: User } return { $u.name } }",
                false,
            ),
            spec(
                "mismatch",
                "query other() { match { $u: User } return { $u.name } }",
                false,
            ),
            spec("broken", "query broken(", false),
        ])
        .unwrap_err();
        // `good` loads cleanly; only the mismatch and the parse error are
        // reported, and both surface in one pass (not fail-fast).
        assert_eq!(errors.len(), 2);
    }

    /// A file of four declarations: `fine` compiles, `old_form` is refused
    /// by the parser, `caller` calls it, and `helper_user` calls `fine`.
    const MIXED_FILE: &str = "query fine() {\n  match { $u: User }\n  return { $u }\n}\n\nquery old_form() {\n  match { $u: User { name: \"a\" } }\n  return { $u.name }\n}\n\nquery caller() {\n  match { from old_form() }\n  return { $u.name }\n}\n\nquery helper_user() {\n  match { from fine() }\n  return { $u.name }\n}\n";

    #[test]
    fn each_declaration_of_a_file_is_available_on_its_own() {
        let specs = ["fine", "old_form", "caller", "helper_user"]
            .map(|name| spec(name, MIXED_FILE, true))
            .to_vec();
        let registry = QueryRegistry::from_specs_per_query(specs.clone()).unwrap();
        let served: Vec<_> = registry.iter().map(|q| q.name.as_str()).collect();
        assert_eq!(served, ["fine", "helper_user"]);

        // The refused declaration carries its own diagnostic, at its own
        // line of the file.
        let old_form = registry.lookup_unavailable("old_form").unwrap();
        let diagnostic = old_form.diagnostic.as_ref().unwrap();
        assert_eq!(diagnostic.code.as_str(), "Q010");
        assert_eq!(diagnostic.position.as_ref().unwrap().line, 7);
        assert_eq!(&*old_form.source, MIXED_FILE);
        // Its caller is unavailable naming it, with no diagnostic of its own.
        let caller = registry.lookup_unavailable("caller").unwrap();
        assert!(caller.diagnostic.is_none());
        assert!(caller.message.contains("`old_form`"), "{}", caller.message);
        assert_eq!(registry.unavailable().count(), 2);

        // What is served is the file with the refused declarations blanked,
        // byte for byte where they were.
        let fine = registry.lookup("fine").unwrap();
        assert_eq!(fine.source.len(), MIXED_FILE.len());
        assert!(!fine.source.contains("old_form"));
        assert!(fine.source.contains("query helper_user()"));
        assert_eq!(registry.lookup("helper_user").unwrap().callees.len(), 1);

        // The strict registry refuses on any refused declaration.
        let strict = QueryRegistry::from_specs(specs).unwrap_err();
        let refused: Vec<_> = strict.iter().map(|e| e.query.as_deref().unwrap()).collect();
        assert_eq!(refused, ["old_form", "caller"]);
        // Also for one only called, never registered.
        let strict = QueryRegistry::from_specs(vec![spec("fine", MIXED_FILE, true)]).unwrap_err();
        let refused: Vec<_> = strict.iter().map(|e| e.query.as_deref().unwrap()).collect();
        assert_eq!(refused, ["caller", "old_form"]);
    }

    #[test]
    fn a_caller_of_a_declaration_that_fails_its_type_check_is_unavailable_naming_it() {
        let source = "query helper() { match { $g: Ghost } return { $g } }\n\
                      query uses_helper() { match { from helper() } return { $g.name } }\n\
                      query plain() { match { $u: User } return { $u.name } }\n";
        let mut registry = QueryRegistry::from_specs_per_query(
            ["helper", "uses_helper", "plain"]
                .map(|name| spec(name, source, false))
                .to_vec(),
        )
        .unwrap();
        let report = check(&registry, &test_catalog());
        let mut broken: Vec<_> = report
            .breakages
            .iter()
            .map(|b| (b.query.as_str(), b.diagnostic.is_some()))
            .collect();
        broken.sort();
        assert_eq!(broken, [("helper", true), ("uses_helper", false)]);
        let uses = report
            .breakages
            .iter()
            .find(|b| b.query == "uses_helper")
            .unwrap();
        assert!(uses.message.contains("`helper`"), "{}", uses.message);
        registry.withdraw_breakages(&report);
        let served: Vec<_> = registry.iter().map(|q| q.name.as_str()).collect();
        assert_eq!(served, ["plain"]);
    }

    #[test]
    fn mutation_body_classifies_as_mutation() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "add_user",
            "query add_user($name: String) { replace User { name: $name } }",
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
            "query find_user($name: String) { match { $u: User } where $u.name = $name return { $u.age } }",
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
            spec(
                "a",
                "query a() { match { $w: Widget } return { $w.x } }",
                false,
            ),
            spec(
                "b",
                "query b() { match { $g: Gadget } return { $g.y } }",
                false,
            ),
            spec(
                "ok",
                "query ok() { match { $u: User } return { $u.name } }",
                false,
            ),
        ])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert_eq!(
            report.breakages.len(),
            2,
            "both bad queries reported: {:?}",
            report
        );
    }

    #[test]
    fn vector_param_on_exposed_query_warns() {
        let reg = QueryRegistry::from_specs(vec![spec(
            "vec_search",
            "query vec_search($q: Vector(4)) { match { let $pop = { match { $u: User } return { $u } }, \
             from nearest($pop, $u.embedding, $q, k: 3) as $distance } return { $u.name } \
             order { $distance } limit 3 }",
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
            "query vec_search($q: Vector(4)) { match { let $pop = { match { $u: User } return { $u } }, \
             from nearest($pop, $u.embedding, $q, k: 3) as $distance } return { $u.name } \
             order { $distance } limit 3 }",
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
            "find_user",
            "query find_user($name: String) { match { $u: User } where $u.name = $name return { $u.name } }",
            true,
        )])
        .unwrap();
        let report = check(&reg, &test_catalog());
        assert!(
            report.is_clean(),
            "no breakage or warning expected: {:?}",
            report
        );
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
        assert_eq!(
            by["big"].kind,
            ParamKind::BigInt,
            "I64 → bigint (string on the wire)"
        );
        assert_eq!(by["u"].kind, ParamKind::BigInt, "U64 → bigint");
        assert_eq!(by["f"].kind, ParamKind::Float);
        assert_eq!(by["b"].kind, ParamKind::Bool);
        assert_eq!(by["d"].kind, ParamKind::Date);
        assert_eq!(by["dt"].kind, ParamKind::DateTime);
        assert_eq!(by["blob"].kind, ParamKind::Blob);
        assert!(!by["s"].nullable);
        assert!(by["opt"].nullable, "String? → nullable");
        assert_eq!(by["list"].kind, ParamKind::List);
        assert_eq!(
            by["list"].item_kind,
            Some(ParamKind::Int),
            "[I32] → list of int"
        );
        assert_eq!(by["vec"].kind, ParamKind::Vector);
        assert_eq!(by["vec"].vector_dim, Some(4));
    }

    #[test]
    fn catalog_entry_flags_mutation_and_empty_params() {
        use crate::api;
        let reg = QueryRegistry::from_specs(vec![spec(
            "add_user",
            "query add_user($name: String) { replace User { name: $name } }",
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
