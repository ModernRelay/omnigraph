//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file; the binaries the CI selects by name stay in tests/.

mod cli_data;
mod cli_queries;
mod cli_schema_config;
mod parity_matrix;
#[path = "../support/mod.rs"]
mod support;
