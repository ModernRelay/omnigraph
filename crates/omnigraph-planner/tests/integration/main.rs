//! One test binary for this crate's loose integration tests: each file below is a module,
//! so the crate links once instead of once per file; the binaries the CI selects by name stay in tests/.

use omnigraph_planner::Bounds;
mod bound_plan;
#[path = "../support/bounds.rs"]
mod fixture_bounds;
mod lower_walk;
mod query_plan;
mod registry;
mod scan_access;
