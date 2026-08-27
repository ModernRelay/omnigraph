//! Deterministic local fixture construction and correctness verification for
//! `branch-merge-v1` benchmark cases.
//!
//! The frozen fixture produced here already contains the two diverged branches.
//! A runner must restore that state to the same stable active path for every
//! repetition and measure only [`Omnigraph::branch_merge`]. Nothing in this
//! module varies with a repetition number.

use std::error::Error;
use std::fmt::{Display, Formatter, Write as _};

use arrow_array::{Array, Int32Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::Schema as ArrowSchema;
use futures::TryStreamExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{compile_schema_shape, schema_shape_from_ir, schema_shape_json};
use sha2::{Digest, Sha256};

use crate::ValidatedCase;
use crate::case::{
    Aging, Arrival, Backend, ColumnShape, CompactionRecency, Contention, DataProvenance,
    DeletionHistory, Execution, FixtureBuilderKind, NetworkPosition, ReadWriteMix, ResetMode,
    SYNTHETIC_BRANCH_MERGE_BUILDER_VERSION, Scenario, Schedule, TopologySkew,
    branch_merge_change_mix,
};

pub const SOURCE_BRANCH: &str = "bench-source";
pub const TARGET_BRANCH: &str = "bench-target";

const MAIN_BRANCH: &str = "main";
const BUILDER_VERSION: u32 = SYNTHETIC_BRANCH_MERGE_BUILDER_VERSION;
const SUPPORTED_SEED: u64 = 0;
const UPDATE_VALUE: i32 = i32::MAX;
const NEW_COHORT: &str = "new";
const LOGICAL_FIXTURE_DIGEST_DOMAIN: &[u8] = b"omnigraph-bench-logical-fixture-v1\0";

/// The engine's keyed-write limits. Chunks stay at or below half of either
/// limit so JSON framing and future small schema additions cannot place a
/// multi-row chunk on the admission boundary.
const KEYED_WRITE_MAX_ROWS: usize = 8_192;
const KEYED_WRITE_MAX_BYTES: usize = 32 * 1024 * 1024;
// Edge JSON carries an explicit id plus both endpoint keys. Use the larger
// row shape for every chunk and byte-budget proof so neither the node nor edge
// generator can cross the engine's keyed-write admission boundary.
const MAX_ROW_OVERHEAD_BYTES: usize = 256;
const WARM_SCAN_TARGET_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RUNNER_TABLES: usize = 256;
const MAX_RUNNER_BASE_ROWS: usize = 10_000_000;
const MAX_RUNNER_GENERATED_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_RUNNER_ESTIMATED_ENTRIES: u64 = 750_000;
const MAX_RUNNER_HISTORY_DEPTH: u64 = 100_000;
const ESTIMATED_ENTRIES_PER_PUBLICATION: u64 = 64;
const ESTIMATED_ENTRIES_PER_DATASET: u64 = 64;
const ESTIMATED_FIXED_ENTRIES: u64 = 1_024;
const SCRATCH_AMPLIFICATION: u64 = 16;
const SCRATCH_FIXED_BYTES: u64 = 1024 * 1024 * 1024;

pub type BranchMergeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchMergeErrorKind {
    Unsupported,
    InvalidPlan,
    Fixture,
    Verification,
}

/// Scenario-owned failure with a stable broad class and an actionable detail.
/// Engine and Arrow errors remain their native sources through
/// [`BranchMergeResult`].
#[derive(Debug)]
pub struct BranchMergeError {
    kind: BranchMergeErrorKind,
    message: String,
}

impl BranchMergeError {
    pub fn kind(&self) -> BranchMergeErrorKind {
        self.kind
    }
}

impl Display for BranchMergeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl Error for BranchMergeError {}

fn scenario_error(
    kind: BranchMergeErrorKind,
    message: impl Into<String>,
) -> Box<dyn Error + Send + Sync> {
    Box::new(BranchMergeError {
        kind,
        message: message.into(),
    })
}

fn unsupported(path: &str, detail: impl Display) -> Box<dyn Error + Send + Sync> {
    scenario_error(
        BranchMergeErrorKind::Unsupported,
        format!("unsupported branch-merge-v1 axis at {path}: {detail}"),
    )
}

fn invalid_plan(detail: impl Display) -> Box<dyn Error + Send + Sync> {
    scenario_error(BranchMergeErrorKind::InvalidPlan, detail.to_string())
}

fn fixture_error(detail: impl Display) -> Box<dyn Error + Send + Sync> {
    scenario_error(BranchMergeErrorKind::Fixture, detail.to_string())
}

fn verification_error(detail: impl Display) -> Box<dyn Error + Send + Sync> {
    scenario_error(BranchMergeErrorKind::Verification, detail.to_string())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangeSplit {
    pub updates: usize,
    pub deletes: usize,
    pub inserts: usize,
}

impl ChangeSplit {
    pub fn total(self) -> usize {
        self.updates + self.deletes + self.inserts
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableDelta {
    pub source: ChangeSplit,
    pub target: ChangeSplit,
}

/// Fully checked execution data for one declarative benchmark point.
///
/// Repetition count is deliberately absent: it is acquisition quantity on
/// `ResolvedRun`, not scenario or fixture identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchMergePlan {
    pub tables: usize,
    pub rows_per_table: usize,
    pub payload_bytes: usize,
    pub diverged_tables: usize,
    pub delta_rows_per_side: usize,
    pub requested_history_depth: u64,
    pub compaction_recency: CompactionRecency,
    pub table_deltas: Vec<TableDelta>,
    source_update_cohort: String,
    source_delete_cohort: String,
    target_update_cohort: String,
    target_delete_cohort: String,
}

/// Bounded construction recipe checked before any fixture bytes are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct FixturePreflight {
    pub base_rows: u64,
    pub estimated_generated_bytes: u64,
    pub base_load_commits: u64,
    pub divergence_commits_per_branch: u64,
    pub optimize_commits: u64,
    pub expected_history_depth: u64,
    pub estimated_max_entries: u64,
    pub required_scratch_bytes: u64,
}

impl TryFrom<&ValidatedCase> for BranchMergePlan {
    type Error = Box<dyn Error + Send + Sync>;

    fn try_from(validated: &ValidatedCase) -> Result<Self, Self::Error> {
        let case = &validated.definition;
        if case.scenario != Scenario::BranchMergeV1 {
            return Err(unsupported(
                "scenario",
                "only branch-merge-v1 is executable",
            ));
        }
        if case.fixture.builder.kind != FixtureBuilderKind::SyntheticBranchMerge {
            return Err(unsupported(
                "fixture.builder.kind",
                "only synthetic-branch-merge is executable",
            ));
        }
        if case.fixture.builder.version != BUILDER_VERSION {
            return Err(unsupported(
                "fixture.builder.version",
                format!(
                    "builder version {} was requested; this runner implements version {BUILDER_VERSION}",
                    case.fixture.builder.version
                ),
            ));
        }
        if case.fixture.builder.seed != SUPPORTED_SEED {
            return Err(unsupported(
                "fixture.builder.seed",
                format!(
                    "seed {} was requested; deterministic builder v2 currently defines only seed {SUPPORTED_SEED}",
                    case.fixture.builder.seed
                ),
            ));
        }
        if case.fixture.data.provenance != DataProvenance::Synthetic {
            return Err(unsupported(
                "fixture.data.provenance",
                "builder v2 requires synthetic data",
            ));
        }
        if case.fixture.data.column_shape != ColumnShape::Scalars {
            return Err(unsupported(
                "fixture.data.column_shape",
                "builder v2 implements the scalar schema only",
            ));
        }
        if case.fixture.data.topology_skew != TopologySkew::Uniform {
            return Err(unsupported(
                "fixture.data.topology_skew",
                "builder v2 implements uniform topology only",
            ));
        }
        if case.fixture.state.aging != Aging::BulkLoaded {
            return Err(unsupported(
                "fixture.state.aging",
                "the local runner currently implements bulk-loaded fixtures only",
            ));
        }
        if !case.fixture.state.indexes.is_empty() {
            return Err(unsupported(
                "fixture.state.indexes",
                "the local runner currently implements unindexed fixtures only",
            ));
        }
        if case.fixture.state.deletion_history != DeletionHistory::None {
            return Err(unsupported(
                "fixture.state.deletion_history",
                "the local runner currently implements no pre-existing deletion history",
            ));
        }
        if case.workload.arrival != Arrival::UnscheduledSingleShot {
            return Err(unsupported(
                "workload.arrival",
                "branch-merge-v1 executes one unscheduled operation",
            ));
        }
        if case.workload.clients != 1 {
            return Err(unsupported(
                "workload.clients",
                "this runner slice executes exactly one embedded client",
            ));
        }
        if case.workload.read_write_mix != ReadWriteMix::WriteHeavy {
            return Err(unsupported(
                "workload.read_write_mix",
                "branch-merge-v1 requires write-heavy",
            ));
        }
        if case.workload.contention != Contention::DistinctKey {
            return Err(unsupported(
                "workload.contention",
                "builder v2 creates disjoint source and target edge cohorts",
            ));
        }
        if !matches!(&case.environment.backend, Backend::LocalFs { .. }) {
            return Err(unsupported(
                "environment.backend",
                "this runner slice supports local-fs only",
            ));
        }
        if case.environment.network_position != NetworkPosition::SameHost {
            return Err(unsupported(
                "environment.network_position",
                "local fixture execution requires same-host",
            ));
        }
        if case.environment.execution != Execution::Embedded {
            return Err(unsupported(
                "environment.execution",
                "this runner slice supports embedded execution only",
            ));
        }
        if !matches!(
            case.protocol.reset,
            ResetMode::PlainCopy | ResetMode::LocalClonefile
        ) {
            return Err(unsupported(
                "protocol.reset",
                "this runner slice implements local identical-state reset only",
            ));
        }
        if case.protocol.schedule != Schedule::Manual {
            return Err(unsupported(
                "protocol.schedule",
                "this runner slice executes manually scheduled suites only",
            ));
        }
        let tables = checked_usize("fixture.data.tables", case.fixture.data.tables)?;
        let rows_per_table = checked_usize(
            "fixture.data.rows_per_table",
            case.fixture.data.rows_per_table,
        )?;
        let payload_bytes = checked_usize(
            "fixture.data.payload_bytes",
            case.fixture.data.payload_bytes,
        )?;
        let diverged_tables =
            checked_usize("workload.diverged_tables", case.workload.diverged_tables)?;
        let delta_rows_per_side = checked_usize(
            "workload.delta_rows_per_side",
            case.workload.delta_rows_per_side,
        )?;

        if tables < 2 || !tables.is_multiple_of(2) || rows_per_table == 0 {
            return Err(invalid_plan(
                "builder v2 requires an even total table count >= 2 and rows_per_table >= 1",
            ));
        }
        let edge_tables = tables / 2;
        if diverged_tables == 0 || diverged_tables > edge_tables {
            return Err(invalid_plan(format!(
                "builder v2 diverges edge tables and requires diverged_tables in 1..={edge_tables}, got {diverged_tables}"
            )));
        }
        if delta_rows_per_side == 0 {
            return Err(invalid_plan("delta_rows_per_side must be >= 1"));
        }
        let row_bytes = payload_bytes
            .checked_add(MAX_ROW_OVERHEAD_BYTES)
            .ok_or_else(|| invalid_plan("payload plus JSON row overhead overflowed usize"))?;
        if row_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(unsupported(
                "fixture.data.payload_bytes",
                format!(
                    "one generated row is approximately {row_bytes} bytes, above the engine's {KEYED_WRITE_MAX_BYTES}-byte keyed-write cap"
                ),
            ));
        }

        let side_split = split_delta(delta_rows_per_side)?;
        let table_deltas = (0..diverged_tables)
            .map(|table| {
                let split = ChangeSplit {
                    updates: share(side_split.updates, diverged_tables, table),
                    deletes: share(side_split.deletes, diverged_tables, table),
                    inserts: share(side_split.inserts, diverged_tables, table),
                };
                TableDelta {
                    source: split,
                    target: split,
                }
            })
            .collect::<Vec<_>>();
        require_update_bearing_tables(&table_deltas, delta_rows_per_side, diverged_tables)?;
        let planned_source = table_deltas
            .iter()
            .map(|table| table.source.total())
            .sum::<usize>();
        let planned_target = table_deltas
            .iter()
            .map(|table| table.target.total())
            .sum::<usize>();
        if planned_source != delta_rows_per_side || planned_target != delta_rows_per_side {
            return Err(invalid_plan(format!(
                "delta distribution did not conserve the declared per-side total {delta_rows_per_side}: source={planned_source}, target={planned_target}"
            )));
        }

        for (table, delta) in table_deltas.iter().enumerate() {
            let tagged = delta
                .source
                .updates
                .checked_add(delta.source.deletes)
                .and_then(|count| count.checked_add(delta.target.updates))
                .and_then(|count| count.checked_add(delta.target.deletes))
                .ok_or_else(|| invalid_plan(format!("table {table} cohort size overflowed")))?;
            if tagged > rows_per_table {
                return Err(invalid_plan(format!(
                    "table {table} needs {tagged} disjoint update/delete cohort rows, but rows_per_table is {rows_per_table}"
                )));
            }
            for (side, inserts) in [
                ("source", delta.source.inserts),
                ("target", delta.target.inserts),
            ] {
                if inserts > i32::MAX as usize {
                    return Err(unsupported(
                        "workload.delta_rows_per_side",
                        format!(
                            "edge table {table} has {inserts} {side} inserts; builder v2's exact I32 payload can represent at most {} insert ordinals",
                            i32::MAX
                        ),
                    ));
                }
            }
        }

        Ok(Self {
            tables,
            rows_per_table,
            payload_bytes,
            diverged_tables,
            delta_rows_per_side,
            requested_history_depth: case.fixture.state.history_depth,
            compaction_recency: case.fixture.state.compaction_recency,
            table_deltas,
            source_update_cohort: format!("d{delta_rows_per_side}_src_upd"),
            source_delete_cohort: format!("d{delta_rows_per_side}_src_del"),
            target_update_cohort: format!("d{delta_rows_per_side}_tgt_upd"),
            target_delete_cohort: format!("d{delta_rows_per_side}_tgt_del"),
        })
    }
}

impl BranchMergePlan {
    fn node_tables(&self) -> usize {
        self.tables / 2
    }

    fn edge_tables(&self) -> usize {
        self.tables / 2
    }

    /// Exact total target rows after both sides' effects are merged.
    pub(crate) fn expected_merged_rows(&self) -> BranchMergeResult<u64> {
        let node_rows = self
            .node_tables()
            .checked_mul(self.rows_per_table)
            .ok_or_else(|| verification_error("expected node row total overflowed usize"))?;
        let mut total = u64::try_from(node_rows)
            .map_err(|_| verification_error("expected node row total does not fit u64"))?;
        for table in 0..self.edge_tables() {
            let rows = expected_edge_rows(self, table, BranchState::Merged)?;
            total = total
                .checked_add(u64::try_from(rows).map_err(|_| {
                    verification_error(format!(
                        "expected merged rows do not fit u64 for table {table}"
                    ))
                })?)
                .ok_or_else(|| verification_error("expected merged row total overflowed u64"))?;
        }
        Ok(total)
    }
}

fn checked_usize(path: &str, value: u64) -> BranchMergeResult<usize> {
    usize::try_from(value).map_err(|_| {
        unsupported(
            path,
            format!("value {value} does not fit this host's usize"),
        )
    })
}

fn split_delta(delta: usize) -> BranchMergeResult<ChangeSplit> {
    if delta == 0 {
        return Err(invalid_plan("delta must be >= 1"));
    }
    let change_mix = branch_merge_change_mix(
        u64::try_from(delta).map_err(|_| invalid_plan("delta does not fit u64"))?,
    );
    Ok(ChangeSplit {
        updates: usize::try_from(change_mix.updates)
            .map_err(|_| invalid_plan("update count does not fit usize"))?,
        deletes: usize::try_from(change_mix.deletes)
            .map_err(|_| invalid_plan("delete count does not fit usize"))?,
        inserts: usize::try_from(change_mix.inserts)
            .map_err(|_| invalid_plan("insert count does not fit usize"))?,
    })
}

fn share(total: usize, parts: usize, index: usize) -> usize {
    debug_assert!(parts > 0);
    debug_assert!(index < parts);
    total / parts + usize::from(index < total % parts)
}

fn minimum_update_bearing_delta(diverged_tables: usize) -> BranchMergeResult<usize> {
    diverged_tables
        .checked_mul(3)
        .and_then(|value| value.checked_sub(2))
        .ok_or_else(|| invalid_plan("minimum update-bearing delta overflowed usize"))
}

fn require_update_bearing_tables(
    table_deltas: &[TableDelta],
    delta_rows_per_side: usize,
    diverged_tables: usize,
) -> BranchMergeResult<()> {
    if let Some(table) = table_deltas
        .iter()
        .position(|delta| delta.source.updates == 0 || delta.target.updates == 0)
    {
        let minimum_delta = minimum_update_bearing_delta(diverged_tables)?;
        return Err(unsupported(
            "workload.delta_rows_per_side",
            format!(
                "delta {delta_rows_per_side} leaves declared diverged edge table {table} without an update on both sides, so it cannot guarantee the general TableWalk route; {diverged_tables} diverged edge tables require delta_rows_per_side >= {minimum_delta}"
            ),
        ));
    }
    Ok(())
}

fn load_chunk_rows(payload_bytes: usize) -> BranchMergeResult<usize> {
    let row_bytes = payload_bytes
        .checked_add(MAX_ROW_OVERHEAD_BYTES)
        .ok_or_else(|| invalid_plan("payload plus JSON row overhead overflowed usize"))?;
    if row_bytes > KEYED_WRITE_MAX_BYTES {
        return Err(invalid_plan(format!(
            "one generated row is approximately {row_bytes} bytes, above the keyed-write cap"
        )));
    }
    let rows_by_bytes = (KEYED_WRITE_MAX_BYTES / 2 / row_bytes).max(1);
    Ok(rows_by_bytes.min(KEYED_WRITE_MAX_ROWS / 2))
}

fn chunk_count(rows: usize, chunk_rows: usize) -> usize {
    if rows == 0 {
        0
    } else {
        rows.div_ceil(chunk_rows)
    }
}

impl BranchMergePlan {
    /// Derive and bound the exact builder-v2 publication recipe plus
    /// conservative local scratch requirements before initialization.
    pub fn preflight(&self) -> BranchMergeResult<FixturePreflight> {
        if self.tables > MAX_RUNNER_TABLES {
            return Err(unsupported(
                "fixture.data.tables",
                format!(
                    "runner-v1 materializes at most {MAX_RUNNER_TABLES} local tables, got {}",
                    self.tables
                ),
            ));
        }
        let base_rows = self
            .tables
            .checked_mul(self.rows_per_table)
            .ok_or_else(|| invalid_plan("runner base-row count overflowed usize"))?;
        if base_rows > MAX_RUNNER_BASE_ROWS {
            return Err(unsupported(
                "fixture.data",
                format!(
                    "runner-v1 materializes at most {MAX_RUNNER_BASE_ROWS} local base rows, got {base_rows}"
                ),
            ));
        }

        let chunk_rows = load_chunk_rows(self.payload_bytes)?;
        let chunks_per_table = chunk_count(self.rows_per_table, chunk_rows);
        if self.compaction_recency == CompactionRecency::Optimized && chunks_per_table < 2 {
            return Err(unsupported(
                "fixture.state.compaction_recency",
                format!(
                    "builder-v2 optimized fixtures require at least two base fragments per table for productive compaction; rows_per_table={} and chunk_rows={chunk_rows} produce {chunks_per_table}",
                    self.rows_per_table
                ),
            ));
        }
        let base_load_commits = self
            .tables
            .checked_mul(chunks_per_table)
            .ok_or_else(|| invalid_plan("base-load publication count overflowed usize"))?;
        let divergence_commits = |side: Side| -> BranchMergeResult<usize> {
            self.table_deltas.iter().try_fold(0usize, |total, delta| {
                let split = match side {
                    Side::Source => delta.source,
                    Side::Target => delta.target,
                };
                let publications = chunk_count(split.updates, chunk_rows)
                    .checked_add(usize::from(split.deletes > 0))
                    .and_then(|value| value.checked_add(chunk_count(split.inserts, chunk_rows)))
                    .ok_or_else(|| invalid_plan("divergence publication count overflowed"))?;
                total
                    .checked_add(publications)
                    .ok_or_else(|| invalid_plan("divergence publication total overflowed"))
            })
        };
        let source_divergence_commits = divergence_commits(Side::Source)?;
        let target_divergence_commits = divergence_commits(Side::Target)?;
        if source_divergence_commits != target_divergence_commits {
            return Err(invalid_plan(format!(
                "builder v2 requires symmetric branch publication recipes, got source={source_divergence_commits}, target={target_divergence_commits}"
            )));
        }
        let optimize_commits = usize::from(self.compaction_recency == CompactionRecency::Optimized);
        let expected_history_depth = 1usize
            .checked_add(base_load_commits)
            .and_then(|value| value.checked_add(optimize_commits))
            .and_then(|value| value.checked_add(source_divergence_commits))
            .ok_or_else(|| invalid_plan("expected history depth overflowed usize"))?;
        let expected_history_depth = u64::try_from(expected_history_depth)
            .map_err(|_| invalid_plan("expected history depth does not fit u64"))?;
        if expected_history_depth > MAX_RUNNER_HISTORY_DEPTH {
            return Err(unsupported(
                "fixture.state.history_depth",
                format!(
                    "runner-v1 permits at most {MAX_RUNNER_HISTORY_DEPTH} reachable commits per local branch, recipe requires {expected_history_depth}"
                ),
            ));
        }
        if self.requested_history_depth != expected_history_depth {
            return Err(unsupported(
                "fixture.state.history_depth",
                format!(
                    "builder-v2 recipe requires exactly {expected_history_depth} reachable commits per frozen branch (genesis 1 + base loads {base_load_commits} + optimize {optimize_commits} + divergence {source_divergence_commits}), but the case declares {}",
                    self.requested_history_depth
                ),
            ));
        }

        let divergent_input_rows = self.table_deltas.iter().try_fold(0usize, |total, delta| {
            total
                .checked_add(delta.source.updates)
                .and_then(|value| value.checked_add(delta.target.updates))
                .and_then(|value| value.checked_add(delta.source.inserts))
                .and_then(|value| value.checked_add(delta.target.inserts))
                .ok_or_else(|| invalid_plan("generated divergence-row count overflowed"))
        })?;
        let generated_rows = base_rows
            .checked_add(divergent_input_rows)
            .ok_or_else(|| invalid_plan("generated row count overflowed usize"))?;
        let row_bytes = self
            .payload_bytes
            .checked_add(MAX_ROW_OVERHEAD_BYTES)
            .ok_or_else(|| invalid_plan("estimated generated row bytes overflowed usize"))?;
        let estimated_generated_bytes = u64::try_from(generated_rows)
            .ok()
            .and_then(|rows| {
                u64::try_from(row_bytes)
                    .ok()
                    .and_then(|bytes| rows.checked_mul(bytes))
            })
            .ok_or_else(|| invalid_plan("estimated generated bytes overflowed u64"))?;
        if estimated_generated_bytes > MAX_RUNNER_GENERATED_BYTES {
            return Err(unsupported(
                "fixture.data",
                format!(
                    "runner-v1 permits at most {MAX_RUNNER_GENERATED_BYTES} estimated generated bytes locally, recipe requires {estimated_generated_bytes}"
                ),
            ));
        }

        let total_publications = 1usize
            .checked_add(base_load_commits)
            .and_then(|value| value.checked_add(optimize_commits))
            .and_then(|value| value.checked_add(source_divergence_commits))
            .and_then(|value| value.checked_add(target_divergence_commits))
            .ok_or_else(|| invalid_plan("total publication count overflowed usize"))?;
        let estimated_max_entries = u64::try_from(total_publications)
            .ok()
            .and_then(|publications| publications.checked_mul(ESTIMATED_ENTRIES_PER_PUBLICATION))
            .and_then(|entries| {
                u64::try_from(self.tables + 1)
                    .ok()
                    .and_then(|datasets| datasets.checked_mul(ESTIMATED_ENTRIES_PER_DATASET))
                    .and_then(|datasets| entries.checked_add(datasets))
            })
            .and_then(|entries| entries.checked_add(ESTIMATED_FIXED_ENTRIES))
            .ok_or_else(|| invalid_plan("estimated fixture entry count overflowed u64"))?;
        if estimated_max_entries > MAX_RUNNER_ESTIMATED_ENTRIES {
            return Err(unsupported(
                "fixture.data",
                format!(
                    "runner-v1 permits at most {MAX_RUNNER_ESTIMATED_ENTRIES} conservatively estimated local fixture entries, recipe requires {estimated_max_entries}"
                ),
            ));
        }
        let required_scratch_bytes = estimated_generated_bytes
            .checked_mul(SCRATCH_AMPLIFICATION)
            .and_then(|bytes| bytes.checked_add(SCRATCH_FIXED_BYTES))
            .ok_or_else(|| invalid_plan("required scratch byte estimate overflowed u64"))?;

        Ok(FixturePreflight {
            base_rows: u64::try_from(base_rows)
                .map_err(|_| invalid_plan("base rows do not fit u64"))?,
            estimated_generated_bytes,
            base_load_commits: u64::try_from(base_load_commits)
                .map_err(|_| invalid_plan("base-load commits do not fit u64"))?,
            divergence_commits_per_branch: u64::try_from(source_divergence_commits)
                .map_err(|_| invalid_plan("divergence commits do not fit u64"))?,
            optimize_commits: u64::try_from(optimize_commits)
                .map_err(|_| invalid_plan("optimize commits do not fit u64"))?,
            expected_history_depth,
            estimated_max_entries,
            required_scratch_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CohortRanges {
    source_updates_end: usize,
    source_deletes_end: usize,
    target_updates_end: usize,
    target_deletes_end: usize,
}

impl BranchMergePlan {
    fn ranges(&self, table: usize) -> Option<CohortRanges> {
        let delta = self.table_deltas.get(table)?;
        let source_updates_end = delta.source.updates;
        let source_deletes_end = source_updates_end + delta.source.deletes;
        let target_updates_end = source_deletes_end + delta.target.updates;
        let target_deletes_end = target_updates_end + delta.target.deletes;
        Some(CohortRanges {
            source_updates_end,
            source_deletes_end,
            target_updates_end,
            target_deletes_end,
        })
    }

    fn base_cohort<'a>(&'a self, table: usize, row: usize) -> BaseCohort<'a> {
        let Some(ranges) = self.ranges(table) else {
            return BaseCohort::Keep;
        };
        if row < ranges.source_updates_end {
            BaseCohort::SourceUpdate(&self.source_update_cohort)
        } else if row < ranges.source_deletes_end {
            BaseCohort::SourceDelete(&self.source_delete_cohort)
        } else if row < ranges.target_updates_end {
            BaseCohort::TargetUpdate(&self.target_update_cohort)
        } else if row < ranges.target_deletes_end {
            BaseCohort::TargetDelete(&self.target_delete_cohort)
        } else {
            BaseCohort::Keep
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaseCohort<'a> {
    SourceUpdate(&'a str),
    SourceDelete(&'a str),
    TargetUpdate(&'a str),
    TargetDelete(&'a str),
    Keep,
}

impl<'a> BaseCohort<'a> {
    fn label(self) -> &'a str {
        match self {
            Self::SourceUpdate(label)
            | Self::SourceDelete(label)
            | Self::TargetUpdate(label)
            | Self::TargetDelete(label) => label,
            Self::Keep => "keep",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchState {
    Main,
    Source,
    Target,
    Merged,
}

impl BranchState {
    fn has_source_effects(self) -> bool {
        matches!(self, Self::Source | Self::Merged)
    }

    fn has_target_effects(self) -> bool {
        matches!(self, Self::Target | Self::Merged)
    }
}

/// Observable facts from a completed, exactly verified fixture build.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FixtureBuildSummary {
    pub base_load_commits: usize,
    pub optimized_user_tables: usize,
    pub source_history_depth: u64,
    pub target_history_depth: u64,
    /// Digest of the exactly verified user-visible schema, physically proven
    /// empty index inventory, and rows on every frozen branch. Unlike the
    /// physical tree digest, this remains stable across Lance ids, timestamps,
    /// compaction layout, and encoding.
    pub logical_content_sha256: String,
}

/// Initialize, fully diverge, and verify a local fixture at `root_uri`.
///
/// The returned future drops its engine handle before returning. Callers may
/// then digest and freeze the directory without opening the frozen original
/// again.
pub async fn initialize_local_fixture(
    root_uri: &str,
    plan: &BranchMergePlan,
) -> BranchMergeResult<FixtureBuildSummary> {
    let preflight = plan.preflight()?;
    if root_uri.contains("://") && !root_uri.starts_with("file://") {
        return Err(unsupported(
            "invocation.root_uri",
            format!("local fixture initialization cannot use {root_uri:?}"),
        ));
    }
    let schema = schema_source(plan.tables);
    let db = Omnigraph::init(root_uri, &schema)
        .await
        .map_err(|error| fixture_error(format!("initialize fixture at {root_uri}: {error}")))?;
    let base_load_commits = load_base(&db, plan).await?;
    if u64::try_from(base_load_commits).ok() != Some(preflight.base_load_commits) {
        return Err(fixture_error(format!(
            "builder-v2 base-load recipe drifted: preflight declared {} publications, execution produced {base_load_commits}",
            preflight.base_load_commits
        )));
    }
    let optimized_user_tables = match plan.compaction_recency {
        CompactionRecency::Optimized => {
            let outcomes = db
                .optimize()
                .await
                .map_err(|error| fixture_error(format!("optimize fixture main branch: {error}")))?;
            let intended_keys = (0..plan.node_tables())
                .map(node_table_key)
                .chain((0..plan.edge_tables()).map(edge_table_key));
            for key in intended_keys {
                let outcome = outcomes
                    .iter()
                    .find(|outcome| outcome.type_key == key)
                    .ok_or_else(|| {
                        fixture_error(format!(
                            "optimized fixture returned no outcome for intended user table {key}"
                        ))
                    })?;
                if outcome.skipped.is_some()
                    || !outcome.committed
                    || outcome.fragments_removed == 0
                    || outcome.fragments_added == 0
                {
                    return Err(fixture_error(format!(
                        "optimized fixture did not productively compact {key}: committed={}, fragments_removed={}, fragments_added={}, skipped={:?}",
                        outcome.committed,
                        outcome.fragments_removed,
                        outcome.fragments_added,
                        outcome.skipped
                    )));
                }
            }
            plan.tables
        }
        CompactionRecency::NotOptimized => 0,
    };

    db.branch_create_from(ReadTarget::branch(MAIN_BRANCH), SOURCE_BRANCH)
        .await
        .map_err(|error| fixture_error(format!("create {SOURCE_BRANCH}: {error}")))?;
    db.branch_create_from(ReadTarget::branch(MAIN_BRANCH), TARGET_BRANCH)
        .await
        .map_err(|error| fixture_error(format!("create {TARGET_BRANCH}: {error}")))?;

    let queries = mutation_queries(plan.diverged_tables);
    diverge(&db, SOURCE_BRANCH, Side::Source, plan, &queries).await?;
    diverge(&db, TARGET_BRANCH, Side::Target, plan, &queries).await?;

    let schema_shape = verified_schema_shape_json(&db, plan)?;
    let mut logical_digest = Sha256::new();
    logical_digest.update(LOGICAL_FIXTURE_DIGEST_DOMAIN);
    hash_logical_field(
        &mut logical_digest,
        b"schema-shape",
        schema_shape.as_bytes(),
    );
    // Builder v2 declares no secondary indexes. The per-branch verification
    // below proves that every node and edge manifest has an empty physical
    // index inventory before this declared empty inventory is certified in the
    // logical digest. Compaction layout and encoding remain derived state.
    hash_logical_field(&mut logical_digest, b"logical-index-inventory", b"[]");
    verify_branch(
        &db,
        MAIN_BRANCH,
        plan,
        BranchState::Main,
        Some(&mut logical_digest),
    )
    .await?;
    verify_branch(
        &db,
        SOURCE_BRANCH,
        plan,
        BranchState::Source,
        Some(&mut logical_digest),
    )
    .await?;
    verify_branch(
        &db,
        TARGET_BRANCH,
        plan,
        BranchState::Target,
        Some(&mut logical_digest),
    )
    .await?;
    let source_history_depth = u64::try_from(
        db.list_commits(Some(SOURCE_BRANCH))
            .await
            .map_err(|error| fixture_error(format!("list {SOURCE_BRANCH} commits: {error}")))?
            .len(),
    )
    .map_err(|_| fixture_error("source history depth does not fit u64"))?;
    let target_history_depth = u64::try_from(
        db.list_commits(Some(TARGET_BRANCH))
            .await
            .map_err(|error| fixture_error(format!("list {TARGET_BRANCH} commits: {error}")))?
            .len(),
    )
    .map_err(|_| fixture_error("target history depth does not fit u64"))?;
    if source_history_depth != plan.requested_history_depth
        || target_history_depth != plan.requested_history_depth
    {
        return Err(unsupported(
            "fixture.state.history_depth",
            format!(
                "requested exactly {} reachable commits per branch, but deterministic construction produced {source_history_depth} on {SOURCE_BRANCH} and {target_history_depth} on {TARGET_BRANCH}; builder v2 does not silently pad or squash history — declare the observed depth or revise the versioned deterministic builder contract",
                plan.requested_history_depth
            ),
        ));
    }
    let logical_content_sha256 = format!("{:x}", logical_digest.finalize());

    drop(db);
    Ok(FixtureBuildSummary {
        base_load_commits,
        optimized_user_tables,
        source_history_depth,
        target_history_depth,
        logical_content_sha256,
    })
}

fn verified_schema_shape_json(db: &Omnigraph, plan: &BranchMergePlan) -> BranchMergeResult<String> {
    let catalog = db.catalog();
    let accepted_ir = catalog.bound_schema_ir().ok_or_else(|| {
        verification_error("fixture catalog is not bound to the accepted schema identity")
    })?;
    let observed = schema_shape_from_ir(accepted_ir).map_err(|error| {
        verification_error(format!("project accepted fixture schema shape: {error}"))
    })?;
    let expected_source = schema_source(plan.tables);
    let expected_ast = parse_schema(&expected_source)
        .map_err(|error| verification_error(format!("parse builder-v2 schema: {error}")))?;
    let expected = compile_schema_shape(&expected_ast)
        .map_err(|error| verification_error(format!("compile builder-v2 schema shape: {error}")))?;
    if observed != expected {
        return Err(verification_error(
            "accepted fixture schema differs from the complete canonical builder-v2 schema shape",
        ));
    }
    schema_shape_json(&observed)
        .map_err(|error| verification_error(format!("serialize fixture schema shape: {error}")))
}

fn logical_node_row_sha256(
    ty: &str,
    id: &str,
    name: &str,
    cohort: &str,
    value: i32,
    payload: &str,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"node-row\0");
    for (label, bytes) in [
        (b"type".as_slice(), ty.as_bytes()),
        (b"id".as_slice(), id.as_bytes()),
        (b"name".as_slice(), name.as_bytes()),
        (b"cohort".as_slice(), cohort.as_bytes()),
        (b"payload".as_slice(), payload.as_bytes()),
    ] {
        hash_logical_field(&mut digest, label, bytes);
    }
    hash_logical_field(&mut digest, b"val-i32-le", &value.to_le_bytes());
    digest.finalize().into()
}

fn logical_edge_row_sha256(
    ty: &str,
    id: &str,
    src: &str,
    dst: &str,
    cohort: &str,
    value: i32,
    payload: &str,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"edge-row\0");
    for (label, bytes) in [
        (b"type".as_slice(), ty.as_bytes()),
        (b"id".as_slice(), id.as_bytes()),
        (b"src".as_slice(), src.as_bytes()),
        (b"dst".as_slice(), dst.as_bytes()),
        (b"cohort".as_slice(), cohort.as_bytes()),
        (b"payload".as_slice(), payload.as_bytes()),
    ] {
        hash_logical_field(&mut digest, label, bytes);
    }
    hash_logical_field(&mut digest, b"val-i32-le", &value.to_le_bytes());
    digest.finalize().into()
}

/// Hash deterministic node rows only after the physical scan proved exact
/// equality with the builder-v2 model.
fn hash_verified_node_rows(
    digest: &mut Sha256,
    table: usize,
    plan: &BranchMergePlan,
    payload: &str,
) -> BranchMergeResult<()> {
    let ty = node_type_name(table);
    for index in 0..plan.rows_per_table {
        let value = i32::try_from(index).map_err(|_| {
            verification_error(format!(
                "node table {table}: canonical ordinal {index} does not fit I32"
            ))
        })?;
        let name = node_name(table, index);
        let row = logical_node_row_sha256(&ty, &name, &name, "keep", value, payload);
        hash_logical_field(digest, b"row-sha256", &row);
    }
    Ok(())
}

/// Hash deterministic edge rows only after the physical scan proved exact
/// equality with the builder-v2 model.
fn hash_verified_edge_rows(
    digest: &mut Sha256,
    table: usize,
    plan: &BranchMergePlan,
    state: BranchState,
    base_payload: &str,
    insert_payload: &str,
) -> BranchMergeResult<()> {
    let ty = edge_type_name(table);
    for index in 0..plan.rows_per_table {
        let cohort = plan.base_cohort(table, index);
        let present = match cohort {
            BaseCohort::SourceDelete(_) => !state.has_source_effects(),
            BaseCohort::TargetDelete(_) => !state.has_target_effects(),
            _ => true,
        };
        if !present {
            continue;
        }
        let value = match cohort {
            BaseCohort::SourceUpdate(_) if state.has_source_effects() => UPDATE_VALUE,
            BaseCohort::TargetUpdate(_) if state.has_target_effects() => UPDATE_VALUE,
            _ => i32::try_from(index).map_err(|_| {
                verification_error(format!(
                    "edge table {table}: canonical base ordinal {index} does not fit I32"
                ))
            })?,
        };
        let id = base_edge_id(table, index);
        let (src, dst) = edge_endpoints(plan, table, index);
        let row =
            logical_edge_row_sha256(&ty, &id, &src, &dst, cohort.label(), value, base_payload);
        hash_logical_field(digest, b"row-sha256", &row);
    }
    for (side, enabled, count) in [
        (
            Side::Source,
            state.has_source_effects(),
            delta_for(plan, table).source.inserts,
        ),
        (
            Side::Target,
            state.has_target_effects(),
            delta_for(plan, table).target.inserts,
        ),
    ] {
        if !enabled {
            continue;
        }
        for index in 0..count {
            let value = i32::try_from(index).map_err(|_| {
                verification_error(format!(
                    "edge table {table}: canonical insert ordinal {index} does not fit I32"
                ))
            })?;
            let id = insert_edge_id(side, table, index);
            let (src, dst) = edge_endpoints(plan, table, index);
            let row =
                logical_edge_row_sha256(&ty, &id, &src, &dst, NEW_COHORT, value, insert_payload);
            hash_logical_field(digest, b"row-sha256", &row);
        }
    }
    Ok(())
}
fn hash_logical_field(digest: &mut Sha256, label: &[u8], value: &[u8]) {
    digest.update((label.len() as u64).to_le_bytes());
    digest.update(label);
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

/// Execute the fixed, read-only `branch-merge-read-set-v1` warm-up program.
///
/// One iteration reads `main`, [`SOURCE_BRANCH`], and [`TARGET_BRANCH`] in that
/// order. For each branch it consumes the reachable commit listing, resolves
/// one coherent snapshot, then fully consumes `id`, `name`, `cohort`, `val`,
/// and `payload` for every declared diverged edge table. Scan batches use the same
/// payload-derived row bound as fixture generation. It scans every diverged
/// edge plus the union of those edges' endpoint node tables, matching the
/// validation read set of the measured merge. The program performs no writes;
/// `reopened-after-program` callers drop and reopen the handle only after it
/// returns.
pub async fn warm_read_set(
    db: &Omnigraph,
    plan: &BranchMergePlan,
    iterations: u32,
) -> BranchMergeResult<()> {
    if iterations == 0 {
        return Err(unsupported(
            "environment.cache_condition.iterations",
            "branch-merge-read-set-v1 requires at least one iteration",
        ));
    }
    let batch_rows = load_chunk_rows(plan.payload_bytes)?;
    for iteration in 0..iterations {
        for branch in [MAIN_BRANCH, SOURCE_BRANCH, TARGET_BRANCH] {
            let commits = db.list_commits(Some(branch)).await.map_err(|error| {
                fixture_error(format!(
                    "warm-up iteration {iteration}: list {branch} commits: {error}"
                ))
            })?;
            if commits.is_empty() {
                return Err(fixture_error(format!(
                    "warm-up iteration {iteration}: branch {branch} has no reachable commits"
                )));
            }
            let snapshot = db
                .snapshot_of(ReadTarget::branch(branch))
                .await
                .map_err(|error| {
                    fixture_error(format!(
                        "warm-up iteration {iteration}: snapshot {branch}: {error}"
                    ))
                })?;
            let mut branch_rows = 0u64;
            let mut endpoint_nodes = vec![false; plan.node_tables()];
            for table in 0..plan.diverged_tables {
                endpoint_nodes[table % plan.node_tables()] = true;
                endpoint_nodes[(table + 1) % plan.node_tables()] = true;
            }
            let mut read_set = endpoint_nodes
                .iter()
                .enumerate()
                .filter(|(_, included)| **included)
                .map(|(table, _)| {
                    (
                        node_table_key(table),
                        &["id", "name", "cohort", "val", "payload"][..],
                    )
                })
                .collect::<Vec<_>>();
            read_set.extend((0..plan.diverged_tables).map(|table| {
                (
                    edge_table_key(table),
                    &["id", "src", "dst", "cohort", "val", "payload"][..],
                )
            }));
            for (table_key, projection) in read_set {
                let dataset = snapshot.open_dataset(&table_key).await.map_err(|error| {
                    fixture_error(format!(
                        "warm-up iteration {iteration}: open {table_key} on {branch}: {error}"
                    ))
                })?;
                let mut scanner = dataset.scan();
                scanner
                    .project(projection)?
                    .batch_size(batch_rows)
                    .batch_size_bytes(WARM_SCAN_TARGET_BYTES)
                    .strict_batch_size(true);
                let mut stream = scanner.try_into_stream().await?;
                while let Some(batch) = stream.try_next().await? {
                    branch_rows = branch_rows
                        .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                            fixture_error(format!(
                                "warm-up iteration {iteration}: batch row count does not fit u64"
                            ))
                        })?)
                        .ok_or_else(|| {
                            fixture_error(format!(
                                "warm-up iteration {iteration}: row count overflow on {branch}"
                            ))
                        })?;
                }
            }
            if branch_rows == 0 {
                return Err(fixture_error(format!(
                    "warm-up iteration {iteration}: read set for {branch} was empty"
                )));
            }
        }
    }
    Ok(())
}

/// Frozen heads that a target-directed merge must not publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtectedBranchHeads {
    source: String,
    main: String,
}

/// Capture the source and main graph heads before measurement.
pub async fn capture_protected_branch_heads(
    db: &Omnigraph,
) -> BranchMergeResult<ProtectedBranchHeads> {
    Ok(ProtectedBranchHeads {
        source: exact_branch_head(db, SOURCE_BRANCH).await?,
        main: exact_branch_head(db, MAIN_BRANCH).await?,
    })
}

/// Verify every table on target, source, and main after a successful merge and
/// prove that the non-target graph heads did not move.
///
/// This must run after the measured window and after storage counters have
/// been captured.
pub async fn verify_merged_graph(
    db: &Omnigraph,
    plan: &BranchMergePlan,
    protected: &ProtectedBranchHeads,
) -> BranchMergeResult<MergeVerificationSummary> {
    let target = verify_branch(db, TARGET_BRANCH, plan, BranchState::Merged, None).await?;
    verify_branch(db, SOURCE_BRANCH, plan, BranchState::Source, None).await?;
    verify_branch(db, MAIN_BRANCH, plan, BranchState::Main, None).await?;
    let source_head = exact_branch_head(db, SOURCE_BRANCH).await?;
    let main_head = exact_branch_head(db, MAIN_BRANCH).await?;
    if source_head != protected.source || main_head != protected.main {
        return Err(verification_error(format!(
            "merge moved a protected graph head: source {} -> {}, main {} -> {}",
            protected.source, source_head, protected.main, main_head
        )));
    }
    Ok(MergeVerificationSummary {
        target,
        source_exact_content: true,
        main_exact_content: true,
        protected_heads_unchanged: true,
    })
}

async fn exact_branch_head(db: &Omnigraph, branch: &str) -> BranchMergeResult<String> {
    let snapshot = db
        .snapshot_of(ReadTarget::branch(branch))
        .await
        .map_err(|error| verification_error(format!("snapshot branch {branch}: {error}")))?;
    let head_key = (branch != MAIN_BRANCH).then_some(branch);
    snapshot
        .graph_head(head_key)
        .map(str::to_string)
        .ok_or_else(|| verification_error(format!("branch {branch} has no exact graph head")))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MergeVerificationSummary {
    pub target: VerificationSummary,
    pub source_exact_content: bool,
    pub main_exact_content: bool,
    pub protected_heads_unchanged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VerificationSummary {
    pub branch: String,
    pub tables: usize,
    pub rows: u64,
}

fn schema_source(tables: usize) -> String {
    debug_assert!(tables >= 2 && tables.is_multiple_of(2));
    let node_tables = tables / 2;
    let edge_tables = tables / 2;
    let mut source = String::new();
    for table in 0..node_tables {
        writeln!(
            source,
            "node {} {{\n    name: String @key\n    cohort: String?\n    val: I32?\n    payload: String?\n}}\n",
            node_type_name(table)
        )
        .expect("writing to String cannot fail");
    }
    for table in 0..edge_tables {
        writeln!(
            source,
            "edge {}: {} -> {} {{\n    cohort: String?\n    val: I32?\n    payload: String?\n}}\n",
            edge_type_name(table),
            node_type_name(table % node_tables),
            node_type_name((table + 1) % node_tables),
        )
        .expect("writing to String cannot fail");
    }
    source
}

fn mutation_queries(diverged_tables: usize) -> String {
    let mut source = String::new();
    for table in 0..diverged_tables {
        let ty = edge_type_name(table);
        writeln!(
            source,
            "query del_{table}($c: String) {{\n    delete {ty} where cohort = $c\n}}\n"
        )
        .expect("writing to String cannot fail");
    }
    source
}

fn node_type_name(table: usize) -> String {
    format!("BenchN{table:03}")
}

fn edge_type_name(table: usize) -> String {
    format!("BenchE{table:03}")
}

fn node_table_key(table: usize) -> String {
    format!("node:{}", node_type_name(table))
}

fn edge_table_key(table: usize) -> String {
    format!("edge:{}", edge_type_name(table))
}

fn node_name(table: usize, row: usize) -> String {
    format!("n{table:03}_r{row:07}")
}

fn base_edge_id(table: usize, row: usize) -> String {
    format!("e{table:03}_r{row:07}")
}

fn insert_edge_id(side: Side, table: usize, row: usize) -> String {
    format!("{}_e{table:03}_n{row:07}", side.tag())
}

fn jsonl_node_row(ty: &str, name: &str, cohort: &str, val: i32, payload: &str) -> String {
    serde_json::json!({
        "type": ty,
        "data": {
            "name": name,
            "cohort": cohort,
            "val": val,
            "payload": payload,
        }
    })
    .to_string()
}

fn jsonl_edge_row(
    ty: &str,
    id: &str,
    from: &str,
    to: &str,
    cohort: &str,
    val: i32,
    payload: &str,
) -> String {
    serde_json::json!({
        "edge": ty,
        "from": from,
        "to": to,
        "data": {
            "id": id,
            "cohort": cohort,
            "val": val,
            "payload": payload,
        }
    })
    .to_string()
}

fn edge_endpoints(plan: &BranchMergePlan, table: usize, row: usize) -> (String, String) {
    let nodes = plan.node_tables();
    let ordinal = row % plan.rows_per_table;
    (
        node_name(table % nodes, ordinal),
        node_name((table + 1) % nodes, ordinal),
    )
}

async fn load_base(db: &Omnigraph, plan: &BranchMergePlan) -> BranchMergeResult<usize> {
    let payload = "x".repeat(plan.payload_bytes);
    let chunk_rows = load_chunk_rows(plan.payload_bytes)?;
    let mut commits = 0usize;
    for table in 0..plan.node_tables() {
        let ty = node_type_name(table);
        let mut start = 0usize;
        while start < plan.rows_per_table {
            let end = start.saturating_add(chunk_rows).min(plan.rows_per_table);
            let mut chunk = String::new();
            for row in start..end {
                let val = i32::try_from(row).map_err(|_| {
                    invalid_plan(format!(
                        "base node ordinal {row} cannot be represented by builder v2's I32 val"
                    ))
                })?;
                chunk.push_str(&jsonl_node_row(
                    &ty,
                    &node_name(table, row),
                    "keep",
                    val,
                    &payload,
                ));
                chunk.push('\n');
            }
            db.load(MAIN_BRANCH, &chunk, LoadMode::Append)
                .await
                .map_err(|error| {
                    fixture_error(format!(
                        "load base node table {table} rows {start}..{end}: {error}"
                    ))
                })?;
            commits += 1;
            start = end;
        }
    }
    for table in 0..plan.edge_tables() {
        let ty = edge_type_name(table);
        let mut start = 0usize;
        while start < plan.rows_per_table {
            let end = start.saturating_add(chunk_rows).min(plan.rows_per_table);
            let mut chunk = String::new();
            for row in start..end {
                let val = i32::try_from(row).map_err(|_| {
                    invalid_plan(format!(
                        "base edge ordinal {row} cannot be represented by builder v2's I32 val"
                    ))
                })?;
                let (from, to) = edge_endpoints(plan, table, row);
                chunk.push_str(&jsonl_edge_row(
                    &ty,
                    &base_edge_id(table, row),
                    &from,
                    &to,
                    plan.base_cohort(table, row).label(),
                    val,
                    &payload,
                ));
                chunk.push('\n');
            }
            db.load(MAIN_BRANCH, &chunk, LoadMode::Append)
                .await
                .map_err(|error| {
                    fixture_error(format!(
                        "load base edge table {table} rows {start}..{end}: {error}"
                    ))
                })?;
            commits += 1;
            start = end;
        }
    }
    Ok(commits)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Source,
    Target,
}

impl Side {
    fn tag(self) -> &'static str {
        match self {
            Self::Source => "src",
            Self::Target => "tgt",
        }
    }
}

async fn diverge(
    db: &Omnigraph,
    branch: &str,
    side: Side,
    plan: &BranchMergePlan,
    queries: &str,
) -> BranchMergeResult<()> {
    let base_payload = "x".repeat(plan.payload_bytes);
    let chunk_rows = load_chunk_rows(plan.payload_bytes)?;
    for (table, delta) in plan.table_deltas.iter().enumerate() {
        let split = match side {
            Side::Source => delta.source,
            Side::Target => delta.target,
        };
        let (update_cohort, delete_cohort) = match side {
            Side::Source => (&plan.source_update_cohort, &plan.source_delete_cohort),
            Side::Target => (&plan.target_update_cohort, &plan.target_delete_cohort),
        };
        if split.updates > 0 {
            // The public mutation grammar intentionally refuses edge updates.
            // A Merge load with the same explicit edge id is the supported
            // keyed replacement path, and produces the same ordinary table
            // history the measured branch merge consumes.
            let start = match side {
                Side::Source => 0,
                Side::Target => delta.source.updates + delta.source.deletes,
            };
            let update_end = start + split.updates;
            let ty = edge_type_name(table);
            let mut chunk_start = start;
            while chunk_start < update_end {
                let chunk_end = chunk_start.saturating_add(chunk_rows).min(update_end);
                let mut chunk = String::new();
                for row in chunk_start..chunk_end {
                    let (from, to) = edge_endpoints(plan, table, row);
                    chunk.push_str(&jsonl_edge_row(
                        &ty,
                        &base_edge_id(table, row),
                        &from,
                        &to,
                        update_cohort,
                        UPDATE_VALUE,
                        &base_payload,
                    ));
                    chunk.push('\n');
                }
                db.load(branch, &chunk, LoadMode::Merge)
                    .await
                    .map_err(|error| {
                        fixture_error(format!(
                            "apply {} Merge update to edge table {table} rows {chunk_start}..{chunk_end} on {branch}: {error}",
                            side.tag()
                        ))
                    })?;
                chunk_start = chunk_end;
            }
        }
        if split.deletes > 0 {
            let mut params = ParamMap::new();
            params.insert("c".to_string(), Literal::String(delete_cohort.clone()));
            db.mutate(branch, queries, &format!("del_{table}"), &params)
                .await
                .map_err(|error| {
                    fixture_error(format!(
                        "apply {} delete to edge table {table} on {branch}: {error}",
                        side.tag()
                    ))
                })?;
        }
    }

    let payload = "y".repeat(plan.payload_bytes);
    for (table, delta) in plan.table_deltas.iter().enumerate() {
        let inserts = match side {
            Side::Source => delta.source.inserts,
            Side::Target => delta.target.inserts,
        };
        let ty = edge_type_name(table);
        let mut start = 0usize;
        while start < inserts {
            let end = start.saturating_add(chunk_rows).min(inserts);
            let mut chunk = String::new();
            for row in start..end {
                let val = i32::try_from(row).map_err(|_| {
                    invalid_plan(format!(
                        "edge insert ordinal {row} cannot be represented by builder v2's I32 val"
                    ))
                })?;
                let (from, to) = edge_endpoints(plan, table, row);
                chunk.push_str(&jsonl_edge_row(
                    &ty,
                    &insert_edge_id(side, table, row),
                    &from,
                    &to,
                    NEW_COHORT,
                    val,
                    &payload,
                ));
                chunk.push('\n');
            }
            db.load(branch, &chunk, LoadMode::Append)
                .await
                .map_err(|error| {
                    fixture_error(format!(
                        "load {} inserts into edge table {table} rows {start}..{end} on {branch}: {error}",
                        side.tag()
                    ))
                })?;
            start = end;
        }
    }
    Ok(())
}

async fn verify_branch(
    db: &Omnigraph,
    branch: &str,
    plan: &BranchMergePlan,
    state: BranchState,
    mut logical_digest: Option<&mut Sha256>,
) -> BranchMergeResult<VerificationSummary> {
    let snapshot = db
        .snapshot_of(ReadTarget::branch(branch))
        .await
        .map_err(|error| verification_error(format!("snapshot branch {branch}: {error}")))?;
    let mut total_rows = 0u64;
    let base_payload = "x".repeat(plan.payload_bytes);
    let insert_payload = "y".repeat(plan.payload_bytes);
    if let Some(digest) = logical_digest.as_deref_mut() {
        hash_logical_field(digest, b"branch", branch.as_bytes());
    }
    let catalog = db.catalog();
    for table in 0..plan.node_tables() {
        let ty = node_type_name(table);
        let dataset = snapshot
            .open_dataset(&node_table_key(table))
            .await
            .map_err(|error| {
                verification_error(format!("open node table {table} on {branch}: {error}"))
            })?;
        let expected_schema = catalog.node_types.get(&ty).ok_or_else(|| {
            verification_error(format!(
                "accepted fixture catalog has no expected node type {ty}"
            ))
        })?;
        let observed_schema = ArrowSchema::from(dataset.schema());
        if observed_schema != *expected_schema.arrow_schema {
            return Err(verification_error(format!(
                "table {table} on {branch} has a physical schema that differs from the complete accepted schema for {ty}"
            )));
        }
        if dataset.has_raw_index_section() {
            return Err(verification_error(format!(
                "node table {table} on {branch} carries a raw Lance index-metadata section, but builder v2 declares indexes: []"
            )));
        }
        if let Some(digest) = logical_digest.as_deref_mut() {
            hash_logical_field(digest, b"type", ty.as_bytes());
        }
        let mut scanner = dataset.scan();
        scanner.project(&["id", "name", "cohort", "val", "payload"])?;
        let mut stream = scanner.try_into_stream().await?;
        let mut seen_base = SeenBits::new(plan.rows_per_table)?;
        let mut actual_rows = 0usize;
        while let Some(batch) = stream.try_next().await? {
            verify_node_batch(&batch, branch, table, plan, &base_payload, &mut seen_base)?;
            actual_rows = actual_rows.checked_add(batch.num_rows()).ok_or_else(|| {
                verification_error(format!(
                    "row count overflow on node table {table} of {branch}"
                ))
            })?;
        }
        if actual_rows != plan.rows_per_table {
            return Err(verification_error(format!(
                "node table {table} on {branch} has {actual_rows} exact-valid rows, expected {}; at least one expected key is missing",
                plan.rows_per_table
            )));
        }
        total_rows = checked_add_verified_rows(total_rows, actual_rows, "node", table)?;
        if let Some(digest) = logical_digest.as_deref_mut() {
            let rows = u64::try_from(actual_rows).map_err(|_| {
                verification_error(format!("node row count does not fit u64 on table {table}"))
            })?;
            hash_logical_field(digest, b"row-count-u64-be", &rows.to_be_bytes());
            hash_verified_node_rows(digest, table, plan, &base_payload)?;
        }
    }

    for table in 0..plan.edge_tables() {
        let ty = edge_type_name(table);
        let dataset = snapshot
            .open_dataset(&edge_table_key(table))
            .await
            .map_err(|error| {
                verification_error(format!("open edge table {table} on {branch}: {error}"))
            })?;
        let expected_schema = catalog.edge_types.get(&ty).ok_or_else(|| {
            verification_error(format!(
                "accepted fixture catalog has no expected edge type {ty}"
            ))
        })?;
        let observed_schema = ArrowSchema::from(dataset.schema());
        if observed_schema != *expected_schema.arrow_schema {
            return Err(verification_error(format!(
                "edge table {table} on {branch} has a physical schema that differs from the complete accepted schema for {ty}"
            )));
        }
        if dataset.has_raw_index_section() {
            return Err(verification_error(format!(
                "edge table {table} on {branch} carries a raw Lance index-metadata section, but builder v2 declares indexes: []"
            )));
        }
        if let Some(digest) = logical_digest.as_deref_mut() {
            hash_logical_field(digest, b"type", ty.as_bytes());
        }
        let mut scanner = dataset.scan();
        scanner.project(&["id", "src", "dst", "cohort", "val", "payload"])?;
        let mut stream = scanner.try_into_stream().await?;
        let mut seen_base = SeenBits::new(plan.rows_per_table)?;
        let delta = plan.table_deltas.get(table).copied().unwrap_or(TableDelta {
            source: ChangeSplit::default(),
            target: ChangeSplit::default(),
        });
        let mut seen_source_inserts = SeenBits::new(delta.source.inserts)?;
        let mut seen_target_inserts = SeenBits::new(delta.target.inserts)?;
        let mut actual_rows = 0usize;
        while let Some(batch) = stream.try_next().await? {
            verify_edge_batch(
                &batch,
                branch,
                table,
                plan,
                state,
                &base_payload,
                &insert_payload,
                &mut seen_base,
                &mut seen_source_inserts,
                &mut seen_target_inserts,
            )?;
            actual_rows = actual_rows.checked_add(batch.num_rows()).ok_or_else(|| {
                verification_error(format!("row count overflow on table {table} of {branch}"))
            })?;
        }

        let expected_rows = expected_edge_rows(plan, table, state)?;
        if actual_rows != expected_rows {
            return Err(verification_error(format!(
                "edge table {table} on {branch} has {actual_rows} exact-valid rows, expected {expected_rows}; at least one expected id is missing"
            )));
        }
        let actual_rows_u64 = u64::try_from(actual_rows).map_err(|_| {
            verification_error(format!("edge row count does not fit u64 on table {table}"))
        })?;
        total_rows = checked_add_verified_rows(total_rows, actual_rows, "edge", table)?;
        if let Some(digest) = logical_digest.as_deref_mut() {
            hash_logical_field(digest, b"row-count-u64-be", &actual_rows_u64.to_be_bytes());
            hash_verified_edge_rows(digest, table, plan, state, &base_payload, &insert_payload)?;
        }
    }
    Ok(VerificationSummary {
        branch: branch.to_string(),
        tables: plan.tables,
        rows: total_rows,
    })
}

fn checked_add_verified_rows(
    total: u64,
    rows: usize,
    kind: &str,
    table: usize,
) -> BranchMergeResult<u64> {
    total
        .checked_add(u64::try_from(rows).map_err(|_| {
            verification_error(format!(
                "row count does not fit u64 on {kind} table {table}"
            ))
        })?)
        .ok_or_else(|| verification_error("verified row total overflowed u64"))
}

fn verify_node_batch(
    batch: &RecordBatch,
    branch: &str,
    table: usize,
    plan: &BranchMergePlan,
    base_payload: &str,
    seen_base: &mut SeenBits,
) -> BranchMergeResult<()> {
    let id = required_column(batch, "id", branch, table)?;
    let name = required_column(batch, "name", branch, table)?;
    let cohort = required_column(batch, "cohort", branch, table)?;
    let val = required_column(batch, "val", branch, table)?;
    let payload = required_column(batch, "payload", branch, table)?;
    let val = required_i32(val.as_ref(), branch, table)?;
    for row in 0..batch.num_rows() {
        let id = required_string(id.as_ref(), row, "id", branch, table)?;
        let name = required_string(name.as_ref(), row, "name", branch, table)?;
        let cohort = required_string(cohort.as_ref(), row, "cohort", branch, table)?;
        let payload = required_string(payload.as_ref(), row, "payload", branch, table)?;
        let value = required_i32_value(val, row, branch, table)?;
        let Some(index) = parse_node_name(name, table) else {
            return Err(verification_error(format!(
                "node table {table} on {branch}: unknown key {name:?}"
            )));
        };
        if id != name || index >= plan.rows_per_table || !seen_base.insert(index) {
            return Err(verification_error(format!(
                "node table {table} on {branch}: invalid, duplicate, or out-of-range id/name {id:?}/{name:?}"
            )));
        }
        let expected_val = i32::try_from(index).map_err(|_| {
            verification_error(format!(
                "node table {table}: ordinal {index} does not fit I32"
            ))
        })?;
        verify_values(
            branch,
            table,
            name,
            cohort,
            "keep",
            value,
            expected_val,
            payload,
            base_payload,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn verify_edge_batch(
    batch: &RecordBatch,
    branch: &str,
    table: usize,
    plan: &BranchMergePlan,
    state: BranchState,
    base_payload: &str,
    insert_payload: &str,
    seen_base: &mut SeenBits,
    seen_source_inserts: &mut SeenBits,
    seen_target_inserts: &mut SeenBits,
) -> BranchMergeResult<()> {
    let id = required_column(batch, "id", branch, table)?;
    let src = required_column(batch, "src", branch, table)?;
    let dst = required_column(batch, "dst", branch, table)?;
    let cohort = required_column(batch, "cohort", branch, table)?;
    let val = required_column(batch, "val", branch, table)?;
    let payload = required_column(batch, "payload", branch, table)?;
    let val = required_i32(val.as_ref(), branch, table)?;

    for row in 0..batch.num_rows() {
        let id = required_string(id.as_ref(), row, "id", branch, table)?;
        let src = required_string(src.as_ref(), row, "src", branch, table)?;
        let dst = required_string(dst.as_ref(), row, "dst", branch, table)?;
        let cohort = required_string(cohort.as_ref(), row, "cohort", branch, table)?;
        let payload = required_string(payload.as_ref(), row, "payload", branch, table)?;
        let val = required_i32_value(val, row, branch, table)?;

        if let Some(index) = parse_base_edge_id(id, table) {
            if index >= plan.rows_per_table {
                return Err(verification_error(format!(
                    "edge table {table} on {branch}: base id {id:?} has out-of-range ordinal {index}"
                )));
            }
            if !seen_base.insert(index) {
                return Err(verification_error(format!(
                    "edge table {table} on {branch}: duplicate base id {id:?}"
                )));
            }
            let base_cohort = plan.base_cohort(table, index);
            let present = match base_cohort {
                BaseCohort::SourceDelete(_) => !state.has_source_effects(),
                BaseCohort::TargetDelete(_) => !state.has_target_effects(),
                _ => true,
            };
            if !present {
                return Err(verification_error(format!(
                    "edge table {table} on {branch}: deleted base id {id:?} is still present"
                )));
            }
            let expected_val = match base_cohort {
                BaseCohort::SourceUpdate(_) if state.has_source_effects() => UPDATE_VALUE,
                BaseCohort::TargetUpdate(_) if state.has_target_effects() => UPDATE_VALUE,
                _ => i32::try_from(index).map_err(|_| {
                    verification_error(format!(
                        "edge table {table}: expected base ordinal {index} does not fit I32"
                    ))
                })?,
            };
            verify_values(
                branch,
                table,
                id,
                cohort,
                base_cohort.label(),
                val,
                expected_val,
                payload,
                base_payload,
            )?;
            verify_edge_endpoints(branch, table, id, src, dst, plan, index)?;
            continue;
        }

        if let Some(index) = parse_insert_edge_id(id, Side::Source, table) {
            verify_insert(
                branch,
                table,
                id,
                index,
                state.has_source_effects(),
                delta_for(plan, table).source.inserts,
                cohort,
                val,
                payload,
                insert_payload,
                seen_source_inserts,
            )?;
            verify_edge_endpoints(branch, table, id, src, dst, plan, index)?;
            continue;
        }
        if let Some(index) = parse_insert_edge_id(id, Side::Target, table) {
            verify_insert(
                branch,
                table,
                id,
                index,
                state.has_target_effects(),
                delta_for(plan, table).target.inserts,
                cohort,
                val,
                payload,
                insert_payload,
                seen_target_inserts,
            )?;
            verify_edge_endpoints(branch, table, id, src, dst, plan, index)?;
            continue;
        }
        return Err(verification_error(format!(
            "edge table {table} on {branch}: unknown id {id:?}"
        )));
    }
    Ok(())
}

fn verify_edge_endpoints(
    branch: &str,
    table: usize,
    id: &str,
    src: &str,
    dst: &str,
    plan: &BranchMergePlan,
    row: usize,
) -> BranchMergeResult<()> {
    let (expected_src, expected_dst) = edge_endpoints(plan, table, row);
    if src != expected_src || dst != expected_dst {
        return Err(verification_error(format!(
            "edge table {table} on {branch}, id {id:?}: endpoints {src:?}->{dst:?}, expected {expected_src:?}->{expected_dst:?}"
        )));
    }
    Ok(())
}

fn required_i32<'a>(
    array: &'a dyn Array,
    branch: &str,
    table: usize,
) -> BranchMergeResult<&'a Int32Array> {
    array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        verification_error(format!(
            "table {table} on {branch} has {:?} val, expected non-null Int32",
            array.data_type()
        ))
    })
}

fn required_i32_value(
    array: &Int32Array,
    row: usize,
    branch: &str,
    table: usize,
) -> BranchMergeResult<i32> {
    if array.is_null(row) {
        return Err(verification_error(format!(
            "table {table} on {branch}, row {row}: val is null"
        )));
    }
    Ok(array.value(row))
}

fn required_column<'a>(
    batch: &'a RecordBatch,
    column: &str,
    branch: &str,
    table: usize,
) -> BranchMergeResult<&'a arrow_array::ArrayRef> {
    batch.column_by_name(column).ok_or_else(|| {
        verification_error(format!(
            "table {table} on {branch} is missing projected column {column}"
        ))
    })
}

fn required_string<'a>(
    array: &'a dyn Array,
    row: usize,
    column: &str,
    branch: &str,
    table: usize,
) -> BranchMergeResult<&'a str> {
    if array.is_null(row) {
        return Err(verification_error(format!(
            "table {table} on {branch}, row {row}: {column} is null"
        )));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(array.value(row));
    }
    Err(verification_error(format!(
        "table {table} on {branch}: {column} has {:?}, expected Utf8, LargeUtf8, or Utf8View",
        array.data_type()
    )))
}

#[allow(clippy::too_many_arguments)]
fn verify_values(
    branch: &str,
    table: usize,
    key: &str,
    cohort: &str,
    expected_cohort: &str,
    val: i32,
    expected_val: i32,
    payload: &str,
    expected_payload: &str,
) -> BranchMergeResult<()> {
    if cohort != expected_cohort || val != expected_val || payload != expected_payload {
        return Err(verification_error(format!(
            "table {table} on {branch}, key {key:?}: got cohort={cohort:?}, val={val}, payload_bytes={}; expected cohort={expected_cohort:?}, val={expected_val}, payload_bytes={}",
            payload.len(),
            expected_payload.len()
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn verify_insert(
    branch: &str,
    table: usize,
    key: &str,
    index: usize,
    expected_present: bool,
    expected_count: usize,
    cohort: &str,
    val: i32,
    payload: &str,
    expected_payload: &str,
    seen: &mut SeenBits,
) -> BranchMergeResult<()> {
    if !expected_present || index >= expected_count {
        return Err(verification_error(format!(
            "table {table} on {branch}: unexpected insert key {key:?}"
        )));
    }
    if !seen.insert(index) {
        return Err(verification_error(format!(
            "table {table} on {branch}: duplicate insert key {key:?}"
        )));
    }
    let expected_val = i32::try_from(index).map_err(|_| {
        verification_error(format!(
            "table {table}: expected insert ordinal {index} does not fit I32"
        ))
    })?;
    verify_values(
        branch,
        table,
        key,
        cohort,
        NEW_COHORT,
        val,
        expected_val,
        payload,
        expected_payload,
    )
}

fn expected_edge_rows(
    plan: &BranchMergePlan,
    table: usize,
    state: BranchState,
) -> BranchMergeResult<usize> {
    let delta = delta_for(plan, table);
    let mut rows = plan.rows_per_table;
    if state.has_source_effects() {
        rows = rows
            .checked_sub(delta.source.deletes)
            .and_then(|rows| rows.checked_add(delta.source.inserts))
            .ok_or_else(|| {
                verification_error(format!("source row count overflow on table {table}"))
            })?;
    }
    if state.has_target_effects() {
        rows = rows
            .checked_sub(delta.target.deletes)
            .and_then(|rows| rows.checked_add(delta.target.inserts))
            .ok_or_else(|| {
                verification_error(format!("target row count overflow on table {table}"))
            })?;
    }
    Ok(rows)
}

fn delta_for(plan: &BranchMergePlan, table: usize) -> TableDelta {
    plan.table_deltas.get(table).copied().unwrap_or(TableDelta {
        source: ChangeSplit::default(),
        target: ChangeSplit::default(),
    })
}

fn parse_node_name(name: &str, table: usize) -> Option<usize> {
    let remainder = name.strip_prefix('n')?;
    let (found_table, row) = remainder.split_once("_r")?;
    let found_table = found_table.parse::<usize>().ok()?;
    let row = row.parse::<usize>().ok()?;
    (found_table == table && node_name(table, row) == name).then_some(row)
}

fn parse_base_edge_id(id: &str, table: usize) -> Option<usize> {
    let remainder = id.strip_prefix('e')?;
    let (found_table, row) = remainder.split_once("_r")?;
    let found_table = found_table.parse::<usize>().ok()?;
    let row = row.parse::<usize>().ok()?;
    (found_table == table && base_edge_id(table, row) == id).then_some(row)
}

fn parse_insert_edge_id(id: &str, side: Side, table: usize) -> Option<usize> {
    let remainder = id.strip_prefix(side.tag())?.strip_prefix("_e")?;
    let (found_table, row) = remainder.split_once("_n")?;
    let found_table = found_table.parse::<usize>().ok()?;
    let row = row.parse::<usize>().ok()?;
    (found_table == table && insert_edge_id(side, table, row) == id).then_some(row)
}

#[derive(Debug)]
struct SeenBits {
    words: Vec<u64>,
}

impl SeenBits {
    fn new(bits: usize) -> BranchMergeResult<Self> {
        let words = bits
            .checked_add(63)
            .ok_or_else(|| verification_error("verification bitset length overflowed"))?
            / 64;
        Ok(Self {
            words: vec![0; words],
        })
    }

    /// Returns true exactly once for every in-range bit.
    fn insert(&mut self, bit: usize) -> bool {
        let word = bit / 64;
        let mask = 1u64 << (bit % 64);
        let Some(entry) = self.words.get_mut(word) else {
            return false;
        };
        let fresh = *entry & mask == 0;
        *entry |= mask;
        fresh
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(rows: usize, diverged_tables: usize, delta: usize) -> BranchMergePlan {
        let split = split_delta(delta).unwrap();
        let table_deltas = (0..diverged_tables)
            .map(|table| {
                let split = ChangeSplit {
                    updates: share(split.updates, diverged_tables, table),
                    deletes: share(split.deletes, diverged_tables, table),
                    inserts: share(split.inserts, diverged_tables, table),
                };
                TableDelta {
                    source: split,
                    target: split,
                }
            })
            .collect();
        BranchMergePlan {
            tables: diverged_tables * 2,
            rows_per_table: rows,
            payload_bytes: 64,
            diverged_tables,
            delta_rows_per_side: delta,
            requested_history_depth: 1,
            compaction_recency: CompactionRecency::Optimized,
            table_deltas,
            source_update_cohort: format!("d{delta}_src_upd"),
            source_delete_cohort: format!("d{delta}_src_del"),
            target_update_cohort: format!("d{delta}_tgt_upd"),
            target_delete_cohort: format!("d{delta}_tgt_del"),
        }
    }

    #[test]
    fn delta_split_is_total_and_always_update_bearing() {
        for delta in [1, 2, 3, 50, 5_000] {
            let split = split_delta(delta).unwrap();
            assert_eq!(split.total(), delta);
            assert!(split.updates >= 1);
        }
    }

    #[test]
    fn table_shares_sum_to_the_declared_one_side_delta() {
        for (tables, delta) in [(1, 1), (4, 50), (7, 5_000)] {
            let plan = plan(10_000, tables, delta);
            let source = plan
                .table_deltas
                .iter()
                .map(|table| table.source.total())
                .sum::<usize>();
            let target = plan
                .table_deltas
                .iter()
                .map(|table| table.target.total())
                .sum::<usize>();
            assert_eq!(source, delta);
            assert_eq!(target, delta);
        }
    }

    #[test]
    fn preflight_derives_checked_case_history_and_bounded_scratch_before_io() {
        let mut plan = plan(100_000, 4, 50);
        plan.tables = 8;
        plan.requested_history_depth = 214;
        let preflight = plan.preflight().unwrap();
        assert_eq!(preflight.base_rows, 800_000);
        assert_eq!(preflight.base_load_commits, 200);
        assert_eq!(preflight.optimize_commits, 1);
        assert_eq!(preflight.divergence_commits_per_branch, 12);
        assert_eq!(preflight.expected_history_depth, 214);
        assert!(preflight.required_scratch_bytes > preflight.estimated_generated_bytes);
        assert!(preflight.estimated_max_entries <= MAX_RUNNER_ESTIMATED_ENTRIES);
    }

    #[test]
    fn preflight_refuses_emergent_history_and_unsafe_local_scale() {
        let mut wrong_history = plan(100_000, 4, 50);
        wrong_history.tables = 8;
        wrong_history.requested_history_depth = 1;
        assert!(
            wrong_history
                .preflight()
                .unwrap_err()
                .to_string()
                .contains("requires exactly 214 reachable commits")
        );

        let mut too_many_tables = plan(1, 1, 1);
        too_many_tables.tables = MAX_RUNNER_TABLES + 1;
        assert!(
            too_many_tables
                .preflight()
                .unwrap_err()
                .to_string()
                .contains("materializes at most")
        );
    }

    #[tokio::test]
    async fn fixture_initialization_proves_logical_rebuild_stability_and_history_depth() {
        // Two user tables require two base publications. Delta three yields one
        // update, delete, and insert publication on each named branch.
        let mut plan = plan(32, 1, 3);
        plan.compaction_recency = CompactionRecency::NotOptimized;
        plan.requested_history_depth = 6;
        assert_eq!(plan.preflight().unwrap().expected_history_depth, 6);

        let directory = tempfile::tempdir().unwrap();
        let summary = initialize_local_fixture(directory.path().to_str().unwrap(), &plan)
            .await
            .unwrap();
        assert_eq!(summary.base_load_commits, 2);
        assert_eq!(summary.optimized_user_tables, 0);
        assert_eq!(summary.source_history_depth, 6);
        assert_eq!(summary.target_history_depth, 6);
        assert_eq!(summary.logical_content_sha256.len(), 64);

        let rebuilt_directory = tempfile::tempdir().unwrap();
        let rebuilt = initialize_local_fixture(rebuilt_directory.path().to_str().unwrap(), &plan)
            .await
            .unwrap();
        assert_eq!(
            rebuilt.logical_content_sha256, summary.logical_content_sha256,
            "fresh ULIDs, timestamps, and physical Lance bytes must not change logical identity"
        );
    }

    #[tokio::test]
    async fn optimized_direct_plan_cannot_certify_declared_empty_index_state() {
        let mut plan = plan(4_097, 1, 1);
        plan.compaction_recency = CompactionRecency::Optimized;
        plan.requested_history_depth = 7;
        let preflight = plan.preflight().unwrap();
        assert_eq!(preflight.optimize_commits, 1);

        let directory = tempfile::tempdir().unwrap();
        let error = initialize_local_fixture(directory.path().to_str().unwrap(), &plan)
            .await
            .expect_err("optimized manifests cannot certify builder v2's indexes: [] state");
        let fixture_error = error
            .downcast_ref::<BranchMergeError>()
            .expect("fixture certification must return a classified scenario error");
        assert_eq!(fixture_error.kind(), BranchMergeErrorKind::Verification);
        assert!(
            error
                .to_string()
                .contains("raw Lance index-metadata section, but builder v2 declares indexes: []"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn every_declared_diverged_table_requires_an_update_on_both_sides() {
        for tables in [1, 2, 4, 17] {
            let minimum = minimum_update_bearing_delta(tables).unwrap();
            let below = split_delta(minimum.saturating_sub(1).max(1)).unwrap();
            if tables > 1 {
                assert!(
                    (0..tables).any(|table| share(below.updates, tables, table) == 0),
                    "delta below {minimum} must leave one of {tables} tables update-free"
                );
                let deltas = (0..tables)
                    .map(|table| {
                        let split = ChangeSplit {
                            updates: share(below.updates, tables, table),
                            deletes: share(below.deletes, tables, table),
                            inserts: share(below.inserts, tables, table),
                        };
                        TableDelta {
                            source: split,
                            target: split,
                        }
                    })
                    .collect::<Vec<_>>();
                let error = require_update_bearing_tables(&deltas, minimum - 1, tables)
                    .expect_err("an update-free declared table must be refused");
                assert!(error.to_string().contains(&format!(
                    "{tables} diverged edge tables require delta_rows_per_side >= {minimum}"
                )));
            }
            let exact = split_delta(minimum).unwrap();
            assert!(
                (0..tables).all(|table| share(exact.updates, tables, table) > 0),
                "minimum delta {minimum} must update all {tables} tables"
            );
        }
    }

    #[test]
    fn single_delta_cohorts_are_disjoint_and_non_diverged_tables_stay_keep() {
        let plan = plan(1_000, 4, 50);
        for table in 0..plan.diverged_tables {
            let ranges = plan.ranges(table).unwrap();
            assert!(ranges.source_updates_end <= ranges.source_deletes_end);
            assert!(ranges.source_deletes_end <= ranges.target_updates_end);
            assert!(ranges.target_updates_end <= ranges.target_deletes_end);
            assert!(ranges.target_deletes_end <= plan.rows_per_table);

            for row in 0..ranges.source_updates_end {
                assert!(matches!(
                    plan.base_cohort(table, row),
                    BaseCohort::SourceUpdate(_)
                ));
            }
            for row in ranges.source_updates_end..ranges.source_deletes_end {
                assert!(matches!(
                    plan.base_cohort(table, row),
                    BaseCohort::SourceDelete(_)
                ));
            }
            for row in ranges.source_deletes_end..ranges.target_updates_end {
                assert!(matches!(
                    plan.base_cohort(table, row),
                    BaseCohort::TargetUpdate(_)
                ));
            }
            for row in ranges.target_updates_end..ranges.target_deletes_end {
                assert!(matches!(
                    plan.base_cohort(table, row),
                    BaseCohort::TargetDelete(_)
                ));
            }
        }
        assert_eq!(plan.base_cohort(plan.diverged_tables, 0), BaseCohort::Keep);
    }

    #[test]
    fn expected_counts_cover_source_target_and_merged_states() {
        let plan = plan(1_000, 4, 50);
        for table in 0..plan.edge_tables() {
            let delta = delta_for(&plan, table);
            assert_eq!(
                expected_edge_rows(&plan, table, BranchState::Source).unwrap(),
                1_000 - delta.source.deletes + delta.source.inserts
            );
            assert_eq!(
                expected_edge_rows(&plan, table, BranchState::Target).unwrap(),
                1_000 - delta.target.deletes + delta.target.inserts
            );
            assert_eq!(
                expected_edge_rows(&plan, table, BranchState::Merged).unwrap(),
                1_000 - delta.source.deletes - delta.target.deletes
                    + delta.source.inserts
                    + delta.target.inserts
            );
        }
        assert_eq!(plan.expected_merged_rows().unwrap(), 7_998);
    }

    #[test]
    fn logical_node_and_edge_digests_are_stable_and_content_sensitive() {
        let node = logical_node_row_sha256(
            "BenchN000",
            "n000_r0000007",
            "n000_r0000007",
            "keep",
            7,
            "payload",
        );
        assert_eq!(
            node,
            logical_node_row_sha256(
                "BenchN000",
                "n000_r0000007",
                "n000_r0000007",
                "keep",
                7,
                "payload",
            )
        );
        assert_ne!(
            node,
            logical_node_row_sha256(
                "BenchN000",
                "n000_r0000007",
                "renamed",
                "keep",
                7,
                "payload",
            ),
            "a logical node property change must change the row digest"
        );

        let edge = logical_edge_row_sha256(
            "BenchE000",
            "e000_r0000007",
            "n000_r0000007",
            "n001_r0000007",
            "keep",
            7,
            "payload",
        );
        assert_eq!(
            edge,
            logical_edge_row_sha256(
                "BenchE000",
                "e000_r0000007",
                "n000_r0000007",
                "n001_r0000007",
                "keep",
                7,
                "payload",
            )
        );
        assert_ne!(
            edge,
            logical_edge_row_sha256(
                "BenchE000",
                "e000_r0000007",
                "n000_r0000007",
                "n000_r0000008",
                "keep",
                7,
                "payload",
            ),
            "a logical edge endpoint change must change the row digest"
        );
        assert_ne!(node, edge, "node and edge rows use separate digest domains");
    }

    #[test]
    fn key_parsers_accept_only_canonical_names_for_the_current_table() {
        assert_eq!(parse_node_name("n003_r0000042", 3), Some(42));
        assert_eq!(parse_node_name("n3_r42", 3), None);
        assert_eq!(parse_base_edge_id("e003_r0000042", 3), Some(42));
        assert_eq!(parse_base_edge_id("e003_r0000042", 2), None);
        assert_eq!(
            parse_insert_edge_id("src_e003_n0000042", Side::Source, 3),
            Some(42)
        );
        assert_eq!(
            parse_insert_edge_id("tgt_e003_n0000042", Side::Source, 3),
            None
        );
    }

    #[test]
    fn builder_v2_schema_is_balanced_and_ring_endpoints_are_uniform() {
        let schema = schema_source(8);
        assert_eq!(schema.matches("node BenchN").count(), 4);
        assert_eq!(schema.matches("edge BenchE").count(), 4);
        assert!(schema.contains("edge BenchE003: BenchN003 -> BenchN000"));

        let plan = plan(100, 4, 50);
        assert_eq!(
            edge_endpoints(&plan, 3, 42),
            (node_name(3, 42), node_name(0, 42))
        );
    }

    #[tokio::test]
    async fn real_node_edge_fixture_merges_and_verifies_two_table_walks() {
        use omnigraph::db::MergeOutcome;
        use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};

        use crate::runner::{
            MergePhaseEvidenceForm, MergeRouteObservation, phase_observations,
            validate_successful_merge_phase_topology,
        };

        let mut plan = plan(16, 2, 6);
        plan.compaction_recency = CompactionRecency::NotOptimized;
        plan.requested_history_depth = 11;
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().to_str().unwrap();
        let built = initialize_local_fixture(uri, &plan).await.unwrap();
        assert_eq!(built.base_load_commits, 4);
        assert_eq!(built.source_history_depth, 11);
        assert_eq!(built.target_history_depth, 11);

        let db = Omnigraph::open(uri).await.unwrap();
        let protected = capture_protected_branch_heads(&db).await.unwrap();
        let probes = MergeWriteProbes::default();
        let outcome = with_merge_write_probes(
            probes.clone(),
            db.branch_merge(SOURCE_BRANCH, TARGET_BRANCH),
        )
        .await
        .unwrap();
        assert_eq!(outcome, MergeOutcome::Merged);
        assert_eq!(probes.table_walk_interval_count(), 2);
        let route = MergeRouteObservation::from_probes(&probes);
        assert!(route.stage_merge_insert_calls >= 2);
        let phases = phase_observations(probes.merge_timing_snapshot());
        validate_successful_merge_phase_topology(
            &phases,
            &route,
            2,
            MergePhaseEvidenceForm::RawSnapshot,
        )
        .unwrap();
        assert_eq!(
            phases
                .iter()
                .find(|phase| phase.phase == "PhysicalPublish")
                .unwrap()
                .interval_count,
            1,
            "one merge operation with two changed tables publishes physically once"
        );

        let verified = verify_merged_graph(&db, &plan, &protected).await.unwrap();
        assert_eq!(verified.target.tables, 4);
        assert_eq!(verified.target.rows, 64);
        assert!(verified.source_exact_content);
        assert!(verified.main_exact_content);
        assert!(verified.protected_heads_unchanged);
    }
}
