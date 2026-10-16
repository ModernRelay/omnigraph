//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file. A CI job selects a module's test as
//! `--test integration <module>::<name>`; a file stays its own binary under tests/ only
//! when its tests need their own process (failpoints armed process-wide, `current_exe`
//! re-execution, environment mutation).
#![recursion_limit = "512"]

mod aggregation;
mod anti_join_cost;
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
mod lance_version_columns;
mod legacy_columns;
mod lifecycle;
mod lineage_projection;
mod literal_filters;
mod maintenance;
mod merge_cleanup;
mod merge_cost;
mod merge_fast_forward;
mod merge_net_zero;
mod merge_projection_cache;
mod merge_truth_table;
mod ordering;
mod point_in_time;
mod proptest_equivalence;
mod recovery;
mod recovery_probe;
mod repro_issue_563;
mod repro_issue_703;
mod repro_issue_723;
mod rrf_prefilter_gate;
mod s3_storage;
mod scalar_indexes;
mod traversal_adaptive;
mod traversal_indexed;
mod validators;
mod warm_read_cost;
mod write_cost;
mod write_cost_s3;
