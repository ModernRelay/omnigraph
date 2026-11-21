//! HTTP wire DTOs. The types and their engine-result -> DTO mappings live
//! in the shared `omnigraph-api-types` crate (RFC-009 Phase 2) so the CLI
//! and server share one definition; re-exported here so every
//! `omnigraph_server::api::*` path (handlers, the OpenApi schema list,
//! CLI imports) keeps resolving unchanged. Only `query_catalog_entry`
//! stays — it maps the server's runtime `StoredQuery` (not a wire type)
//! into the shared `QueryCatalogEntry` DTO.

pub use omnigraph_api_types::*;

use crate::queries::{StoredQuery, UnavailableQuery};

/// Project a stored query the server does not serve into its listing
/// entry: its name and its refusal.
pub fn unavailable_query_output(query: &UnavailableQuery) -> UnavailableQueryOutput {
    UnavailableQueryOutput {
        name: query.name.clone(),
        error: query.refusal(),
        diagnostic: query.diagnostic.as_ref().map(DiagnosticOutput::from),
    }
}

/// Every stored query of a served graph that fails to compile under this
/// release, by name.
pub fn unavailable_queries(handle: &crate::GraphHandle) -> Vec<UnavailableQueryOutput> {
    handle
        .queries
        .as_ref()
        .map(|registry| {
            registry
                .unavailable()
                .map(unavailable_query_output)
                .collect()
        })
        .unwrap_or_default()
}

/// Project a loaded stored query into its catalog entry (typed params,
/// MCP tool name, read/mutate flag, description/instruction).
pub fn query_catalog_entry(query: &StoredQuery) -> QueryCatalogEntry {
    QueryCatalogEntry {
        name: query.name.clone(),
        tool_name: query.effective_tool_name().to_string(),
        description: query.decl.description.clone(),
        instruction: query.decl.instruction.clone(),
        mutation: query.is_mutation(),
        params: query.decl.params.iter().map(param_descriptor).collect(),
    }
}
