//! The traversal-mode cost model and the row-count estimate behind it. The
//! optimizer decides here, from `PlanSource` statistics, whether an `Expand`
//! walks the BTREE-indexed edge scan or the in-memory CSR, and records the
//! decision on `PhysicalNode::Expand`; the engine executes the recorded mode
//! and keeps two runtime corrections (degraded index coverage, the mid-flight
//! switch of issue #533), both computed with the functions below.

use omnigraph_compiler::ir::{IRExpr, IRFilter};
use omnigraph_compiler::query::ast::CompOp;
use omnigraph_compiler::types::Direction;
use serde::{Deserialize, Serialize};

use crate::logical::{LogicalId, LogicalNode, LogicalPlan, ScanSpec};
use crate::source::PlanSource;

/// The two Expand execution paths the cost model dispatches between.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpandMode {
    /// Per-hop neighbor lookup via the persisted src/dst BTREE. Work scales
    /// with the frontier, not |E|.
    IndexedScan,
    /// Whole-graph in-memory CSR, built once and reused.
    Csr,
}

impl ExpandMode {
    /// The serde spelling as a static word, for the engine's operator display.
    pub fn word(self) -> &'static str {
        match self {
            Self::IndexedScan => "indexed_scan",
            Self::Csr => "csr",
        }
    }
}

/// What an `Expand` may do at run time beside the mode the plan recorded:
/// nothing when the session pinned the mode or the source held no edge
/// statistics, or re-decide with the recorded cost inputs (before the first
/// hop against the probed index coverage, the observed frontier and a warm
/// CSR; between input batches and at every later hop with
/// `should_switch_to_csr`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum ExpandPolicy {
    /// The session's traversal pin chose the mode; the run takes no other.
    Pinned,
    /// No edge statistics: the mode is `Csr` and the run takes no other.
    Uncosted,
    /// The cost model chose the mode from `inputs`; the run may take the
    /// other mode by the same model.
    Costed { inputs: ExpandCostInputs },
}

impl ExpandPolicy {
    /// The modes the run may switch to from `mode`.
    pub fn alternatives(&self, mode: ExpandMode) -> Vec<ExpandMode> {
        match self {
            Self::Pinned | Self::Uncosted => Vec::new(),
            Self::Costed { .. } => vec![match mode {
                ExpandMode::IndexedScan => ExpandMode::Csr,
                ExpandMode::Csr => ExpandMode::IndexedScan,
            }],
        }
    }

    /// The cost inputs of a `Costed` policy.
    pub fn cost(&self) -> Option<&ExpandCostInputs> {
        match self {
            Self::Costed { inputs } => Some(inputs),
            Self::Pinned | Self::Uncosted => None,
        }
    }
}

/// How a dependent scan reaches its destination rows: one Lance read per
/// input batch (`id IN (batch ids)`), or one read of the whole destination
/// table as the build side of a hash join the traversal probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessPath {
    /// The per-batch id lookup: work scales with the frontier.
    IdLookup,
    /// The destination table read once, hashed on its id, probed by the
    /// traversal pairs in their order: work scales with the table.
    HashJoin,
}

/// Reading the destination table once beats per-id lookups once the
/// frontier is at least this fraction (one over the ratio) of the table.
pub const HASH_JOIN_RATIO: u64 = 8;

/// The hash join holds its build side twice plus the hash table, so the build
/// side may take at most one part in this many of the memory pool.
pub const HASH_JOIN_POOL_DIVISOR: u64 = 4;

/// The access path of a dependent scan. `HashJoin` needs both: the table
/// holds at most `HASH_JOIN_RATIO` rows per frontier row, and the build
/// side's estimated bytes are known and at most `build_bytes_budget`.
/// `None` when either row count is unknown (the model did not decide).
pub fn choose_access_path(
    frontier_rows: Option<u64>,
    table_rows: Option<u64>,
    build_bytes: Option<u64>,
    build_bytes_budget: u64,
) -> Option<AccessPath> {
    let (frontier, rows) = (frontier_rows?, table_rows?);
    let few_rows = rows <= frontier.saturating_mul(HASH_JOIN_RATIO);
    let fits = build_bytes.is_some_and(|bytes| bytes <= build_bytes_budget);
    Some(if few_rows && fits {
        AccessPath::HashJoin
    } else {
        AccessPath::IdLookup
    })
}

/// Whether the per-hop `key_col IN (...)` scan is served by the BTREE
/// (`Indexed`) or silently falls back to a full scan (`Degraded`). The planner
/// assumes `Indexed`; the engine probes the dataset and corrects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexCoverage {
    Indexed,
    Degraded,
}

/// Building the in-memory CSR costs more than a bare edge scan: it scans every
/// edge AND allocates + groups the adjacency. This factor expresses that
/// overhead so a one-off degraded single-hop scan can still edge out a full CSR
/// build. The crossover is insensitive to its exact value.
pub const CSR_BUILD_FACTOR: f64 = 1.5;

/// Cardinality inputs for the (pure, IO-free) traversal-mode cost model. Every
/// field is a manifest-resident count or an already-in-hand value; the chooser
/// performs no scans.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpandCostInputs {
    /// The frontier the decision is made for: the planner's row-count
    /// estimate of the Expand's input (`u64::MAX` when it has none).
    pub frontier_rows: u64,
    /// |E| for the edge type (manifest `entity_count`).
    pub edge_count: u64,
    /// |V_src|, the node count of the keyed endpoint type.
    pub src_node_count: u64,
    /// Effective max hop count for this Expand (`cost_effective_hops`).
    pub effective_max_hops: u32,
    /// Hard ceiling above which the indexed path is never used (resolved
    /// `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS`).
    pub max_hops_cap: u32,
    /// Hard ceiling above which the indexed path is never used (resolved
    /// `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`).
    pub max_frontier_cap: u64,
    pub coverage: IndexCoverage,
    /// Whether the query's CSR is already realized (an earlier Expand or bulk
    /// AntiJoin built it), making the CSR path ≈ free.
    pub csr_cached: bool,
    /// Endpoint probes the indexed path issues per hop: 1 for a directed
    /// traversal, 2 for undirected (`Direction::Both` scans BOTH the src-keyed
    /// and dst-keyed orientations).
    pub probe_factor: f64,
}

/// Pure cost-based traversal-mode chooser. Compares an estimate of the indexed
/// path's frontier-relative work against the cost of building (or reusing) the
/// whole-graph CSR, and picks the cheaper.
///
/// Under `Indexed` coverage and a cold CSR the decision reduces to a clean
/// selectivity ratio: indexed wins when `hops * frontier * probe_factor <
/// CSR_BUILD_FACTOR * |V_src|`, i.e. when the frontier is a small fraction of
/// the source vertex set, which is independent of |E|.
pub fn choose_expand_mode(i: &ExpandCostInputs) -> ExpandMode {
    if i.effective_max_hops > i.max_hops_cap || i.frontier_rows > i.max_frontier_cap {
        return ExpandMode::Csr;
    }

    let hops = i.effective_max_hops.max(1) as f64;
    let frontier = i.frontier_rows as f64;
    let edges = i.edge_count as f64;
    let src = i.src_node_count.max(1) as f64;
    let fanout = edges / src;

    let indexed_cost = match i.coverage {
        IndexCoverage::Indexed => hops * frontier * fanout * i.probe_factor,
        IndexCoverage::Degraded => hops * edges * i.probe_factor,
    };
    let csr_cost = if i.csr_cached {
        0.0
    } else {
        CSR_BUILD_FACTOR * edges
    };

    if indexed_cost < csr_cost {
        ExpandMode::IndexedScan
    } else {
        ExpandMode::Csr
    }
}

/// Mid-traversal re-decision (issue #533): asked by the engine at the top of
/// every indexed hop after the first with the OBSERVED union frontier, where
/// the plan-time decision only saw an estimate of the initial one.
///
/// Two triggers, either sufficient:
///
/// 1. **The hard frontier ceiling.** The plan enforces it only against the
///    estimate; here it becomes an execution bound.
/// 2. **Remaining-work estimate.** Projects the frontier forward over the
///    remaining hops using the OBSERVED per-hop growth ratio when it exceeds
///    the manifest's average fanout (heavy-tailed graphs blow through the
///    average, and the observed ratio is the only estimator that sees hubs).
///    Each projected frontier saturates at |V_src|. Switch when that estimate
///    exceeds what the CSR path still costs (a build when cold, ~nothing when
///    warm): the switch continues from carried state, so switching pays
///    `csr_cost` once while staying pays the whole estimate.
pub fn should_switch_to_csr(
    observed_frontier: u64,
    prev_frontier: u64,
    remaining_hops: u32,
    csr_ready: bool,
    i: &ExpandCostInputs,
) -> bool {
    if observed_frontier > i.max_frontier_cap {
        return true;
    }
    let edges = i.edge_count as f64;
    let src = i.src_node_count.max(1) as f64;
    let fanout = edges / src;
    let observed_growth = if prev_frontier > 0 {
        observed_frontier as f64 / prev_frontier as f64
    } else {
        fanout
    };
    let growth = observed_growth.max(fanout).max(1.0);

    let mut remaining_cost = 0.0;
    let mut frontier = observed_frontier as f64;
    for _ in 0..remaining_hops {
        remaining_cost += frontier * fanout * i.probe_factor;
        frontier = (frontier * growth).min(src);
    }
    let csr_cost = if csr_ready {
        0.0
    } else {
        CSR_BUILD_FACTOR * edges
    };
    remaining_cost > csr_cost
}

/// Hops the indexed path will actually run. A cross-type edge cannot chain, so
/// the engine caps it at one hop regardless of the requested range; the cost
/// model must use that, or it over-estimates the indexed cost of a cross-type
/// variable-length expand and skews toward CSR.
pub fn cost_effective_hops(requested_max_hops: u32, same_type: bool) -> u32 {
    if same_type {
        requested_max_hops
    } else {
        requested_max_hops.min(1)
    }
}

/// The hops an `Expand` runs for the cost model: its `max_hops`, or its
/// `min_hops` (at least one) when unbounded, through `cost_effective_hops`.
pub fn executed_hops(min_hops: u32, max_hops: Option<u32>, same_type: bool) -> u32 {
    cost_effective_hops(max_hops.unwrap_or(min_hops.max(1)), same_type)
}

/// Per-hop probe multiplier for the indexed path: one scan for a directed
/// traversal and two for an undirected one.
pub fn direction_probe_factor(direction: Direction) -> f64 {
    match direction {
        Direction::Out | Direction::In => 1.0,
        Direction::Both => 2.0,
    }
}

/// The rows one query scan reads: the node type's row count, or at most one
/// row when the pushed filter equates every `@key` column, or one unique
/// property, of the scan's binding with a literal or a parameter. Any other
/// pushed filter leaves the row count as it is. `None` when the source holds
/// no row count for the type.
pub fn scan_row_estimate(spec: &ScanSpec, source: &dyn PlanSource) -> Option<u64> {
    let node_type = source.node_type(spec.table.node_type_name()?).ok()?;
    let rows = node_type.row_count?;
    let binding = spec.binding.as_deref()?;
    let filters = spec
        .filter
        .as_ref()
        .map(|predicate| predicate.gq_filters())
        .unwrap_or_default();
    let equated: Vec<&str> = filters
        .iter()
        .filter_map(|filter| equated_property(filter, binding))
        .collect();
    let whole_key = !node_type.key.is_empty()
        && node_type
            .key
            .iter()
            .all(|key| equated.contains(&key.as_str()));
    let unique = equated
        .iter()
        .any(|property| source.is_unique_property(&spec.table.type_key, property));
    Some(if whole_key || unique {
        rows.min(1)
    } else {
        rows
    })
}

/// The property of `binding` a filter equates with a literal or a parameter.
fn equated_property<'a>(filter: &'a IRFilter, binding: &str) -> Option<&'a str> {
    let IRFilter { left, op, right } = filter;
    if *op != CompOp::Eq {
        return None;
    }
    let constant = |expr: &IRExpr| matches!(expr, IRExpr::Literal(_) | IRExpr::Param(_));
    match (left, right) {
        (IRExpr::PropAccess { variable, property }, other)
        | (other, IRExpr::PropAccess { variable, property })
            if variable == binding && constant(other) =>
        {
            Some(property.as_str())
        }
        _ => None,
    }
}

/// The cardinality estimate of one logical node from the node types' row
/// counts and the edge statistics the source holds: an `Expand` multiplies
/// its input by the edge type's average fanout per hop and saturates at the
/// destination type's row count, so a hub can exceed it. `None` where the
/// source holds no statistic for a node the estimate depends on.
pub fn estimate_rows(plan: &LogicalPlan, node: LogicalId, source: &dyn PlanSource) -> Option<u64> {
    match plan.node(node)? {
        LogicalNode::TableScan { input, spec } => {
            let rows = scan_row_estimate(spec, source)?;
            match input {
                Some(input) => Some(estimate_rows(plan, *input, source)?.min(rows)),
                None => Some(rows),
            }
        }
        LogicalNode::MetadataCount { .. } => Some(1),
        LogicalNode::Expand {
            input,
            edge_type,
            direction,
            min_hops,
            max_hops,
            ..
        } => {
            let input_rows = estimate_rows(plan, *input, source)?;
            let stats = source.expand_statistics(edge_type, *direction)?;
            let fanout = stats
                .edge_count
                .div_ceil(stats.src_node_count.max(1))
                .max(1);
            let hops = executed_hops(*min_hops, *max_hops, stats.same_type);
            let mut rows = input_rows;
            for _ in 0..hops.max(1) {
                rows = rows.saturating_mul(fanout).min(stats.dst_node_count);
            }
            Some(rows)
        }
        LogicalNode::Filter { input, .. }
        | LogicalNode::Projection { input, .. }
        | LogicalNode::Sort { input, .. }
        | LogicalNode::Ordered { input, .. }
        | LogicalNode::RowDiff { input, .. }
        | LogicalNode::TextSearch { input, .. }
        | LogicalNode::RankFuse { input, .. }
        | LogicalNode::AntiJoin { input, .. }
        | LogicalNode::Aggregate { input, .. } => estimate_rows(plan, *input, source),
        LogicalNode::Limit { input, rows, .. } | LogicalNode::Page { input, rows, .. } => {
            Some(estimate_rows(plan, *input, source)?.min(*rows as u64))
        }
        LogicalNode::Nearest { input, k, .. } => {
            let input_rows = estimate_rows(plan, *input, source)?;
            Some(k.map_or(input_rows, |k| k.min(input_rows)))
        }
        LogicalNode::Join { left, right, .. } | LogicalNode::CrossJoin { left, right } => Some(
            estimate_rows(plan, *left, source)?
                .saturating_mul(estimate_rows(plan, *right, source)?),
        ),
        LogicalNode::OuterReference { .. } | LogicalNode::MergeClassify { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cost inputs with generous hard caps, so the cost comparison (not a
    /// ceiling) is what an assertion exercises unless it sets one on purpose.
    fn inputs(
        frontier_rows: u64,
        edge_count: u64,
        src_node_count: u64,
        effective_max_hops: u32,
        coverage: IndexCoverage,
    ) -> ExpandCostInputs {
        ExpandCostInputs {
            frontier_rows,
            edge_count,
            src_node_count,
            effective_max_hops,
            max_hops_cap: 6,
            max_frontier_cap: 1024,
            coverage,
            csr_cached: false,
            probe_factor: 1.0,
        }
    }

    /// `hops * frontier < CSR_BUILD_FACTOR * |V_src|` keeps the indexed path:
    /// 1 hop over |V_src| 100 and |E| 1,000, a frontier of 100 costs 1,000
    /// (< 1,500), a frontier of 160 costs 1,600 and flips to CSR.
    #[test]
    fn crossover_indexed_below_csr_above_caps_force_csr_warm_csr_always_wins() {
        assert_eq!(
            choose_expand_mode(&inputs(100, 1_000, 100, 1, IndexCoverage::Indexed)),
            ExpandMode::IndexedScan
        );
        assert_eq!(
            choose_expand_mode(&inputs(160, 1_000, 100, 1, IndexCoverage::Indexed)),
            ExpandMode::Csr
        );
        assert_eq!(
            choose_expand_mode(&inputs(150, 1_000, 100, 1, IndexCoverage::Indexed)),
            ExpandMode::Csr,
            "equal costs choose CSR"
        );
        assert_eq!(
            choose_expand_mode(&inputs(1, 1_000, 100, 6, IndexCoverage::Indexed)),
            ExpandMode::IndexedScan,
            "the hop cap is inclusive"
        );
        assert_eq!(
            choose_expand_mode(&inputs(
                1_024,
                1_000_000,
                1_000_000,
                1,
                IndexCoverage::Indexed
            )),
            ExpandMode::IndexedScan,
            "the frontier cap is inclusive"
        );
        assert_eq!(
            choose_expand_mode(&inputs(1, 1_000, 100, 7, IndexCoverage::Indexed)),
            ExpandMode::Csr
        );
        assert_eq!(
            choose_expand_mode(&inputs(
                1_025,
                1_000_000,
                1_000_000,
                1,
                IndexCoverage::Indexed
            )),
            ExpandMode::Csr
        );
        assert_eq!(
            choose_expand_mode(&inputs(u64::MAX, 1_000, 100, 1, IndexCoverage::Indexed)),
            ExpandMode::Csr
        );
        let mut warm = inputs(1, 1_000, 100, 1, IndexCoverage::Indexed);
        warm.csr_cached = true;
        assert_eq!(choose_expand_mode(&warm), ExpandMode::Csr);
        assert_eq!(
            choose_expand_mode(&inputs(1, 1_000, 100, 1, IndexCoverage::Degraded)),
            ExpandMode::IndexedScan
        );
        assert_eq!(
            choose_expand_mode(&inputs(1, 1_000, 100, 2, IndexCoverage::Degraded)),
            ExpandMode::Csr
        );
    }

    /// `rows <= frontier * HASH_JOIN_RATIO` reads the table once: 20,000
    /// rows under a 20,000 frontier and 6 under 6 join; 20 rows under a
    /// frontier of 1 (8 < 20) look ids up; an unknown count decides nothing.
    #[test]
    fn access_path_joins_when_the_table_is_at_most_eight_frontiers() {
        let path = |frontier, rows| choose_access_path(frontier, rows, Some(0), 0);
        assert_eq!(path(Some(20_000), Some(20_000)), Some(AccessPath::HashJoin));
        assert_eq!(path(Some(6), Some(6)), Some(AccessPath::HashJoin));
        assert_eq!(path(Some(1), Some(8)), Some(AccessPath::HashJoin));
        assert_eq!(path(Some(1), Some(20)), Some(AccessPath::IdLookup));
        assert_eq!(
            path(Some(u64::MAX), Some(u64::MAX)),
            Some(AccessPath::HashJoin)
        );
        assert_eq!(path(None, Some(20)), None);
        assert_eq!(path(Some(1), None), None);
    }

    /// A build side of exactly the budget joins; one byte more, or unknown
    /// bytes, looks ids up; the row-ratio rule holds whatever the bytes.
    #[test]
    fn access_path_joins_only_when_the_build_side_fits_the_budget() {
        let budget = 150 * 1024 * 1024 / HASH_JOIN_POOL_DIVISOR;
        let path = |bytes| choose_access_path(Some(20_000), Some(20_000), bytes, budget);
        assert_eq!(path(Some(budget)), Some(AccessPath::HashJoin));
        assert_eq!(path(Some(budget + 1)), Some(AccessPath::IdLookup));
        assert_eq!(path(None), Some(AccessPath::IdLookup));
        assert_eq!(
            choose_access_path(Some(1), Some(20), Some(0), budget),
            Some(AccessPath::IdLookup)
        );
    }

    #[test]
    fn a_costed_policy_declares_the_other_mode_and_a_pin_declares_none() {
        let costed = ExpandPolicy::Costed {
            inputs: inputs(1, 1_000, 100, 1, IndexCoverage::Indexed),
        };
        assert_eq!(
            costed.alternatives(ExpandMode::IndexedScan),
            [ExpandMode::Csr]
        );
        assert_eq!(
            costed.alternatives(ExpandMode::Csr),
            [ExpandMode::IndexedScan]
        );
        assert!(costed.cost().is_some());
        for policy in [ExpandPolicy::Pinned, ExpandPolicy::Uncosted] {
            assert!(policy.alternatives(ExpandMode::Csr).is_empty());
            assert!(policy.cost().is_none());
        }
        assert_eq!(
            serde_json::json!(ExpandPolicy::Pinned),
            serde_json::json!({"policy": "pinned"})
        );
    }

    #[test]
    fn expand_mode_word_is_its_serde_spelling() {
        for mode in [ExpandMode::IndexedScan, ExpandMode::Csr] {
            assert_eq!(serde_json::json!(mode), mode.word());
        }
    }

    /// The hard ceiling becomes an execution bound: observed 2000 > 1024
    /// switches regardless of the cost estimate.
    #[test]
    fn hop_policy_switches_on_observed_frontier_over_cap() {
        let i = inputs(1, 10_000_000, 1_000_000, 4, IndexCoverage::Indexed);
        assert!(should_switch_to_csr(2000, 100, 2, false, &i));
    }

    /// Undirected (probe factor 2), fanout ≈ 6.5, hop-2 frontier 238 from 1:
    /// the projection saturates at |V| and the 3 remaining hops dwarf the CSR
    /// build; directed, hop 2 stays and hop 3 crosses the 1024 ceiling.
    #[test]
    fn hop_policy_switches_on_projected_growth() {
        let mut i = inputs(1, 2_500_000, 388_000, 4, IndexCoverage::Indexed);
        i.probe_factor = 2.0;
        assert!(should_switch_to_csr(238, 1, 3, false, &i));

        i.probe_factor = 1.0;
        assert!(!should_switch_to_csr(238, 1, 3, false, &i));
        assert!(should_switch_to_csr(5_418, 238, 2, false, &i));
    }

    /// With the CSR already built this query, any nonzero remaining indexed
    /// work loses to ~free reuse.
    #[test]
    fn hop_policy_switches_cheaply_onto_a_warm_csr() {
        let i = inputs(1, 10_000_000, 1_000_000, 4, IndexCoverage::Indexed);
        assert!(!should_switch_to_csr(40, 20, 2, false, &i));
        assert!(should_switch_to_csr(40, 20, 2, true, &i));
    }
}
