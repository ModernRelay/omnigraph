//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file; the binaries the CI selects by name stay in tests/.
#![recursion_limit = "512"]

mod aggregation;
mod branch_control_cost;
mod branching;
mod changes;
mod changes_cost;
mod checkpoint_retention_cost;
mod composite_flow;
mod consistency;
mod durable_head_lookup_cost;
mod end_to_end;
mod engine_v2;
mod engine_v2_memory;
mod engine_v2_plan_replay;
mod export;
#[path = "../helpers/mod.rs"]
mod helpers;
mod legacy_columns;
mod lifecycle;
mod lineage_projection;
mod literal_filters;
mod maintenance;
mod merge_cleanup;
mod merge_cost;
mod merge_net_zero;
mod merge_projection_cache;
mod merge_truth_table;
mod ordering;
mod point_in_time;
mod recovery;
mod recovery_probe;
mod rrf_prefilter_gate;
mod scalar_indexes;
mod traversal_adaptive;
mod traversal_indexed;
mod validators;
mod warm_read_cost;
mod write_cost;
