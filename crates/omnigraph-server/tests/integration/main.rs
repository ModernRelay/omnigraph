//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file; the binaries the CI selects by name stay in tests/.

mod auth_policy;
mod data_routes;
mod mcp;
mod openapi;
mod schema_routes;
mod stored_queries;
#[path = "../support/mod.rs"]
mod support;
