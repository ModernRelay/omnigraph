//! HTTP wire DTOs. The types and their engine-result -> DTO mappings live
//! in the shared `omnigraph-api-types` crate (RFC-009 Phase 2) so the CLI
//! and server share one definition; re-exported here so every
//! `omnigraph_server::api::*` path (handlers, the OpenApi schema list,
//! CLI imports) keeps resolving unchanged. Only `query_catalog_entry`
//! stays — it maps the server's runtime `StoredQuery` (not a wire type)
//! into the shared `QueryCatalogEntry` DTO.

pub use omnigraph_api_types::*;

use crate::queries::StoredQuery;

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
