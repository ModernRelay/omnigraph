//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file. A CI job selects a module's test as
//! `--test integration <module>::<name>`; `boot_settings` (re-executes its own binary) and
//! `multi_graph` (exhausts the process-wide export budget) stay their own binaries under tests/.

mod auth_policy;
mod data_routes;
mod mcp;
mod openapi;
mod s3;
mod schema_routes;
mod stored_queries;
#[path = "../support/mod.rs"]
mod support;
