//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file. A CI job selects a module's test as
//! `--test integration <module>::<name>`.

mod cli_cluster;
mod cli_cluster_e2e;
mod cli_data;
#[cfg(unix)]
mod cli_local_servers;
#[cfg(unix)]
mod cli_mcp;
mod cli_queries;
mod cli_schema_config;
mod crossversion_upgrade;
mod gqt_served_smoke;
mod parity_matrix;
#[path = "../support/mod.rs"]
mod support;
mod system_local;
mod system_remote;
