use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use arrow_schema::SchemaRef;
use omnigraph_compiler::ir::{IRExpr, IROrdering, IRProjection, SubqueryPredicate};
use omnigraph_compiler::query::ast::{BinaryOp, CompOp};
use omnigraph_compiler::traversal::EdgeSelection;
use omnigraph_compiler::types::Direction;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::aggregate::AggregateSpec;
use crate::cost::{AccessPath, AntiJoinStrategy, ExpandMode, ExpandPolicy};
use crate::error::{PlanError, SET_TRAVERSAL_WORK_LIMIT};
use crate::logical::{
    ColumnRef, CsvSourceSpec, FragmentWriteSpec, KeyJoinKind, ScanSpec, ValidateSpec,
    csv_source_json, filters_json, fragment_write_json, metadata_count_json, ordering_text,
    scan_json, tiebreak_text, validate_json,
};
use crate::mirror::EdgeSelectionMirror;
use crate::operation::TableRef;
use crate::source::{FullTextCoverage, SideId};
use omnigraph_compiler::query::codes::{P002, P003};

/// The index of a node in a [`PhysicalPlan`].
pub type NodeId = usize;

/// The index a ranked scan asks to rank its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RankKind {
    /// `nearest($v.prop, q)`: the vector index appends `_distance`.
    Nearest,
    /// `bm25($v.prop, q)`: the full-text index appends `_score`.
    Bm25,
}

impl RankKind {
    /// Whether the retrieval's contract is approximate: `nearest` membership
    /// is, even when a run scores flat; `bm25` matching is exact.
    pub fn approximate(self) -> bool {
        self == Self::Nearest
    }

    /// The column Lance appends and the direction the query sorts it by.
    pub fn score(self) -> (&'static str, bool) {
        match self {
            Self::Nearest => ("_distance", false),
            Self::Bm25 => ("_score", true),
        }
    }
}

/// Where a ranked scan's rows go: the query's `order`, or one arm of an
/// `rrf()` fusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RankScope {
    Order,
    Primary,
    Secondary,
}

/// How the prefilter gates decide, as the session and the environment set it
/// when the plan was built: the `rrf_plan` setting and the admission
/// thresholds `OMNIGRAPH_RRF_GATE_RATIO` / `OMNIGRAPH_RRF_GATE_MAX_IDS`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GatePolicy {
    pub mode: PrefilterMode,
    /// The prefilter plan runs when |eligible| / corpus is at or below this.
    pub ratio: f64,
    /// The prefilter plan runs when |eligible| is at or below this.
    pub max_ids: u64,
}

/// Prefilter admission ratio: the gate's selective plan runs when
/// |eligible| / corpus is at or below this. It is the conservative crossover
/// of the historical `rrf-gate` corpora retained under `benchmarks/deferred/`.
pub const DEFAULT_GATE_RATIO: f64 = 0.10;

/// Absolute ceiling on the eligible-id in-list: the in-list probe cost
/// ceiling that the ratio cannot see.
pub const DEFAULT_GATE_MAX_IDS: u64 = 100_000;

impl Default for GatePolicy {
    fn default() -> Self {
        Self {
            mode: PrefilterMode::Auto,
            ratio: DEFAULT_GATE_RATIO,
            max_ids: DEFAULT_GATE_MAX_IDS,
        }
    }
}

/// The `rrf_plan` session setting as the gates read it: decide by size, or
/// force one plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrefilterMode {
    Auto,
    ForcePrefilter,
    ForcePostfilter,
}

/// The identity of one dataset the planner read: its path, the Lance branch
/// it lives on, and its staged manifest version when present, otherwise its
/// published version. A replay is refused unless the snapshot holds this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPin {
    pub dataset_path: String,
    pub native_branch: Option<String>,
    pub version: u64,
}

/// What the planner read while it built the plan: the parameter names it
/// read through `filter_pushable`, every setting it read by name and
/// spelling, every environment variable it read by name and resolved value,
/// every dataset it read by table key (`None` for a table the snapshot did
/// not hold), the gate policy, and the memory limit the run's pool takes.
/// `bind` refuses a binding that lacks one of the names.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Assumptions {
    pub params: BTreeSet<String>,
    pub settings: BTreeMap<String, String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub datasets: BTreeMap<String, Option<DatasetPin>>,
    pub gate_policy: GatePolicy,
    pub memory_limit: u64,
    /// One shared traversal-work allowance, present for statements using edge selections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traversal_work_limit: Option<u64>,
    /// Retained even when rewrites remove an expansion, for historical replay admission.
    #[serde(default, skip_serializing_if = "is_false")]
    pub has_wildcard_traversal: bool,
    /// The full-text coverage of every property a ranked scan's eligibility
    /// placement read, keyed `<table key>.<property>`. A pinned dataset
    /// version fixes it, so the dataset pins re-establish it on replay.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub full_text: BTreeMap<String, FullTextCoverage>,
    /// The fingerprint of every catalogue table the plan read, keyed by
    /// table key: the binary's rows and schema for that type. A replay is
    /// refused by a binary whose catalogue fingerprints differ.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub catalogue: BTreeMap<String, String>,
}

impl Assumptions {
    /// The key a property's full-text coverage is recorded under.
    pub fn full_text_key(type_key: &str, property: &str) -> String {
        format!("{type_key}.{property}")
    }
}

impl Assumptions {
    /// Validate the single captured traversal allowance before execution.
    /// Wildcard provenance survives rewrites, including an eliminated Expand.
    ///
    /// # Errors
    /// Returns an error for a duplicate settings entry, a limit outside
    /// `1..=i64::MAX`, or wildcard provenance without a captured limit.
    pub fn validated_traversal_work_limit(&self) -> Result<Option<NonZeroU64>, PlanError> {
        if self.settings.contains_key("traversal_work_limit") {
            return Err(PlanError::refused(
                P003,
                "traversal_work_limit must use the captured typed allowance, not a duplicate settings entry",
                None,
            ));
        }
        match self.traversal_work_limit {
            Some(limit) if limit == 0 || limit > i64::MAX as u64 => Err(PlanError::refused(
                P003,
                "traversal_work_limit must be in 1..=i64::MAX",
                Some("set `traversal_work_limit` to a value in 1..=9223372036854775807"),
            )),
            Some(limit) => Ok(NonZeroU64::new(limit)),
            None if self.has_wildcard_traversal => Err(PlanError::refused(
                P002,
                "wildcard traversal requires a finite traversal_work_limit",
                Some(SET_TRAVERSAL_WORK_LIMIT),
            )),
            None => Ok(None),
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// One required first hop from a ranked binding: a top-level `Expand` that
/// leaves it with `min_hops > 0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hop {
    pub edge_type: String,
    pub direction: Direction,
}

/// A pre-pass the plan declares: before the tree runs, the run computes the
/// ids of `ranked_type` that have every hop in `hops` and ANDs them into the
/// scans `feeds` names as `id IN (...)`, when the gate policy admits the set.
/// Empty `hops` admits nothing: the run records the shape fallback and the
/// scans run as planned. Every hop is a required first hop of the query, so
/// the eligible set holds every row that can survive the traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prefilter {
    pub ranked_type: String,
    pub hops: Vec<Hop>,
    pub feeds: Vec<NodeId>,
    /// What an empty eligible set decides: the empty answer for a
    /// standalone `nearest`, the unfiltered plan for a fusion.
    pub on_empty: EmptyEligible,
    /// Whether the recorded full-text coverage of every `bm25` scan in
    /// `feeds` is full, the guard under which prefiltering keeps BM25
    /// scores; true when it feeds no `bm25` scan.
    pub coverage_admits: bool,
}

/// What a pre-pass's empty eligible set decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmptyEligible {
    /// No row survives the traversal: the answer is empty and no scan runs.
    ProvenEmpty,
    /// The scans run unfiltered.
    Postfilter,
}

/// The adaptive policy of a `nearest` scan, declared before it runs: what
/// the scan may do after an attempt leaves it short of `fetch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NearestPolicy {
    /// A short scan under a probe cap rescans under the cap times this,
    /// and uncapped once that reaches the ranked partitions.
    pub probe_factor: usize,
    /// Rows the prefilter admitted at `_distance = +inf` (the partition
    /// search did not reach them) are rescanned flat and exact.
    pub flat_rescan_on_unreached: bool,
    /// A short capped scan without Lance's partition counters rescans
    /// uncapped (fail closed).
    pub uncapped_on_missing_counters: bool,
    /// A scan whose eligible set is no larger than `fetch` scores flat from
    /// its first attempt.
    pub flat_when_eligible_within_fetch: bool,
}

impl NearestPolicy {
    /// The policy the planner declares: probe caps grow fourfold (20 → 80 →
    /// 320 → none), and every fallback is on.
    pub const DEFAULT: Self = Self {
        probe_factor: 4,
        flat_rescan_on_unreached: true,
        uncapped_on_missing_counters: true,
        flat_when_eligible_within_fetch: true,
    };
}

impl Prefilter {
    /// Whether the plan's shape admits an eligible set at all.
    pub fn admits(&self) -> bool {
        !self.hops.is_empty() && !self.feeds.is_empty()
    }
}

/// Where a ranked scan applies its eligibility (its pushed filter, a gate's
/// eligible set and the members of its search predicates): before the index
/// scores rows, as Lance's prefilter, or after, on the scored rows. A BM25
/// score is independent of a filter applied before scoring only when the
/// property's full-text index covers every fragment; elsewhere eligibility
/// applies after scoring, so the filter never changes a score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Eligibility {
    BeforeScoring,
    AfterScoring,
}

/// One rerun of a `nearest` scan's overfetch ladder, taken in order after a
/// full scan left the answer short of the limit. Report rung `r >= 1` names
/// `overfetch[r - 1]`; rung 0 is the scan as planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum OverfetchRung {
    /// Ask the index for `k` = `factor` times the planned fetch.
    Wider { factor: usize, k: usize },
    /// Every live row of the type, scored flat, no probe cap.
    Exact,
}

/// Per-rung multiplier and ceiling of the overfetch ladder (`k`, `4k`,
/// `16k`, then exact).
pub const OVERFETCH_STEP: usize = 4;
pub const OVERFETCH_MAX_FACTOR: usize = 16;

impl OverfetchRung {
    /// The ladder above a planned `fetch`: wider by `OVERFETCH_STEP` up to
    /// `OVERFETCH_MAX_FACTOR`, then the exact pass.
    pub fn ladder(fetch: usize) -> Vec<Self> {
        let mut ladder = Vec::new();
        let mut factor = OVERFETCH_STEP;
        while factor <= OVERFETCH_MAX_FACTOR {
            ladder.push(Self::Wider {
                factor,
                k: fetch.saturating_mul(factor),
            });
            factor = factor.saturating_mul(OVERFETCH_STEP);
        }
        ladder.push(Self::Exact);
        ladder
    }
}

/// The ranking a Lance index computes while a scan runs: the access path of
/// the ranked binding's scan, not an operator over its rows. The query
/// argument is carried as the query wrote it; its value is in the bound
/// plan's value table.
#[derive(Debug, Clone)]
pub struct RankedAccess {
    pub kind: RankKind,
    pub property: String,
    pub query: IRExpr,
    pub score: IRExpr,
    /// Candidates the scan asks the index for; `None` is every match.
    pub fetch: Option<usize>,
    /// The IVF partitions a `nearest` scan may probe per index delta, the
    /// `ann_nprobes` setting when the plan was built; `None` is no cap, and
    /// on a `bm25` scan it is unread.
    pub nprobes: Option<usize>,
    pub scope: RankScope,
    /// The reruns the run may take above `fetch`; empty on a scan that never
    /// reruns (`bm25`, an arm of `rrf()`).
    pub overfetch: Vec<OverfetchRung>,
    /// The pre-pass that feeds this scan; `None` when no traversal leaves
    /// the ranked binding, and on every arm of `rrf()`, whose pre-pass the
    /// `RankFuse` declares.
    pub prefilter: Option<Prefilter>,
    /// Where the scan applies its eligibility: always before scoring on a
    /// `nearest` scan, whose candidate window is drawn from eligible rows;
    /// on a `bm25` scan before scoring only under recorded full coverage.
    pub eligibility: Eligibility,
    /// The adaptive policy of a `nearest` scan; `None` on a `bm25` scan.
    pub policy: Option<NearestPolicy>,
}

impl RankedAccess {
    /// The score ordering this ranking imposes on `binding`'s rows.
    pub fn ordering(&self, _binding: &str) -> IROrdering {
        let (_, descending) = self.kind.score();
        IROrdering {
            expr: self.score.clone(),
            descending,
        }
    }

    fn to_json(&self) -> Value {
        let mut value = json!({
            "kind": self.kind,
            "property": self.property,
            "query": self.query.to_string(),
            "typed_query": crate::typed::expr(&self.query),
            "typed_score": crate::typed::expr(&self.score),
            "fetch": self.fetch,
            "scope": self.scope,
        });
        match self.kind {
            RankKind::Nearest => {
                value["nprobes"] = json!(self.nprobes);
                value["policy"] = json!(self.policy);
            }
            RankKind::Bm25 => value["eligibility"] = json!(self.eligibility),
        }
        value
    }
}

/// One arm of a `RankFuse`: the subtree the arm runs and the ranked binding
/// whose score orders it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankArm {
    pub input: NodeId,
    pub binding: String,
    pub kind: RankKind,
}

/// A table scan, or the per-slice id lookup of a traversal's destination:
/// one Lance read per slice of the input's rows, `id IN (slice ids)`. The
/// other way to reach a destination is a [`PhysicalNode::HashJoin`] over a
/// table scan of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanInput {
    Table,
    Dependent { input: NodeId },
}

impl AccessPath {
    /// The path a run may take instead of `self`: `id_lookup` under a
    /// `hash_join`, nothing under an `id_lookup`.
    pub fn declared_fallback(self) -> Option<Self> {
        match self {
            Self::HashJoin => Some(Self::IdLookup),
            Self::IdLookup => None,
        }
    }
}

impl ScanInput {
    fn input(&self) -> Option<&NodeId> {
        match self {
            Self::Table => None,
            Self::Dependent { input, .. } => Some(input),
        }
    }

    fn input_mut(&mut self) -> Option<&mut NodeId> {
        match self {
            Self::Table => None,
            Self::Dependent { input, .. } => Some(input),
        }
    }
}

/// A row or byte estimate. An estimate is a plan property and by itself a
/// bound on nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Estimate {
    Known(u64),
    Unknown,
}

impl Estimate {
    /// The estimate capped at `cap`; an unknown estimate stays unknown.
    pub(crate) fn capped(self, cap: u64) -> Self {
        match self {
            Estimate::Known(rows) => Estimate::Known(rows.min(cap)),
            Estimate::Unknown => Estimate::Unknown,
        }
    }
}

/// One statistic a pass read, with where it came from; explain prints every
/// one so a reader can check the optimizer opened no data file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatisticSource {
    pub statistic: String,
    pub value: String,
    pub origin: String,
}

/// One key of the order a node's output leaves in, typed: the comparator a
/// sort establishes, the score order a ranking scan produces, or the logical
/// id order of a diff or merge plan.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderKey {
    /// A diff or merge plan's logical column, ascending.
    Column(String),
    /// An expression with its direction and null placement.
    Expr {
        expr: IRExpr,
        descending: bool,
        nulls_first: bool,
    },
    /// A binding's identity metadata key (`@id`, or `@type` of a selected
    /// edge): ascending, nulls first.
    Identity(ColumnRef),
    /// The fused reciprocal rank of an `rrf()` over its arms' bindings,
    /// descending.
    Fused { bindings: Vec<String> },
}

impl OrderKey {
    /// A written or score sort key: ascending keys place nulls first,
    /// descending keys nulls last (RFC 0047, "One total order").
    pub fn of(ordering: &IROrdering) -> Self {
        Self::Expr {
            expr: ordering.expr.clone(),
            descending: ordering.descending,
            nulls_first: !ordering.descending,
        }
    }
}

impl std::fmt::Display for OrderKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(column) => f.write_str(column),
            Self::Expr {
                expr, descending, ..
            } => write!(f, "{expr} {}", if *descending { "desc" } else { "asc" }),
            Self::Identity(column) => write!(f, "${column} asc"),
            Self::Fused { bindings } => {
                let targets: Vec<String> = bindings
                    .iter()
                    .map(|binding| format!("${binding}"))
                    .collect();
                write!(f, "rrf({}) desc", targets.join(", "))
            }
        }
    }
}

/// The properties a physical node declares: derived after selection,
/// recomputed when a later pass changes the node, never recomputed by an
/// executor. Each is a bound the executor adapts within, never above.
#[derive(Debug, Clone)]
pub struct Properties {
    pub schema: SchemaRef,
    pub ordering: Option<Vec<OrderKey>>,
    pub rows: Estimate,
    pub work_bytes: Estimate,
    pub retained_limit: Option<u64>,
    pub sources: Vec<StatisticSource>,
}

impl Properties {
    /// Result nodes declare their typed columns separately. Pipeline schemas
    /// remain conservative input schemas and are omitted for query plans.
    fn to_json(&self, query: bool) -> Value {
        let ordering: Option<Vec<String>> = self
            .ordering
            .as_ref()
            .map(|keys| keys.iter().map(ToString::to_string).collect());
        let mut value = json!({
            "ordering": ordering,
            "rows": self.rows,
            "work_bytes": self.work_bytes,
            "retained_limit": self.retained_limit,
            "sources": self.sources,
        });
        if !query {
            let fields: Vec<&str> = self
                .schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            value["schema"] = json!(fields);
        }
        value
    }
}

/// The prefix of the column a query's return projection carries for a
/// `HydrateColumns` above it: `^n` holds binding `n`'s row address. No GQ
/// alias or `binding.property` name starts with it.
pub const ROW_ADDRESS_PREFIX: &str = "^";

/// One binding whose return columns a `HydrateColumns` fetches: its pinned
/// table and the columns, each with the return position it fills.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HydratedBinding {
    pub binding: String,
    pub table: TableRef,
    pub columns: Vec<HydratedColumn>,
}

/// One deferred return item: `binding.property` under `output`, the return
/// column at `position`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HydratedColumn {
    pub position: usize,
    pub output: String,
    pub property: String,
}

/// A field of a [`PhysicalNode`] that holds the node's children: the ids it
/// holds now, and every count it can hold, which `@Operator.inputs` reads.
pub trait ChildSlots {
    const COUNTS: &'static [usize];
    fn ids(&self) -> Vec<NodeId>;
    fn ids_mut(&mut self) -> Vec<&mut NodeId>;
}

impl ChildSlots for NodeId {
    const COUNTS: &'static [usize] = &[1];

    fn ids(&self) -> Vec<NodeId> {
        vec![*self]
    }

    fn ids_mut(&mut self) -> Vec<&mut NodeId> {
        vec![self]
    }
}

impl ChildSlots for ScanInput {
    const COUNTS: &'static [usize] = &[0, 1];

    fn ids(&self) -> Vec<NodeId> {
        self.input().copied().into_iter().collect()
    }

    fn ids_mut(&mut self) -> Vec<&mut NodeId> {
        self.input_mut().into_iter().collect()
    }
}

impl ChildSlots for [RankArm; 2] {
    const COUNTS: &'static [usize] = &[2];

    fn ids(&self) -> Vec<NodeId> {
        self.iter().map(|arm| arm.input).collect()
    }

    fn ids_mut(&mut self) -> Vec<&mut NodeId> {
        self.iter_mut().map(|arm| &mut arm.input).collect()
    }
}

/// Every sum of one count from `left` and one from `right`, sorted and
/// distinct: the input counts of a node with both child fields.
fn sum_counts(left: &[usize], right: &[usize]) -> Vec<usize> {
    let mut sums: Vec<usize> = left
        .iter()
        .flat_map(|a| right.iter().map(move |b| a + b))
        .collect();
    sums.sort_unstable();
    sums.dedup();
    sums
}

/// Declares the operator catalog once: the payload-bearing [`PhysicalNode`],
/// its fieldless [`PhysicalNodeKind`] with `ALL`, each kind's explain name,
/// and the child visitation and admitted child counts, all from one declaration.
macro_rules! physical_nodes {
    (
        $(
            $(#[$meta:meta])*
            $variant:ident {
                $( $(#[$fmeta:meta])* $field:ident : $ty:ty ),* $(,)?
            } => $name:literal [ $( $child:ident : $cty:ty ),* ] ;
        )*
    ) => {
        /// The operator catalog. The change-feed and merge nodes (`Scan`,
        /// `SortMergeJoin`, `HydrateByAddress`, `RowCompare`, `ClassifyThreeWay`,
        /// `Page`) each have one push operator in the engine's `engine/push/`; the
        /// read nodes (`CrossJoin` and `ContainsJoin` among them) each have one arm
        /// in the engine's lowering
        /// (`crates/omnigraph/src/engine/lower.rs`), whose exhaustive match is the
        /// proof. A read node carries the IR pieces its arm takes, so the lowering
        /// never re-derives from `QueryIR`.
        #[derive(Debug, Clone)]
        pub enum PhysicalNode {
            $(
                $(#[$meta])*
                $variant { $( $(#[$fmeta])* $field : $ty ),* },
            )*
        }

        /// One fieldless kind per [`PhysicalNode`] variant, generated from the
        /// same declaration, so no variant is enumerated twice or not at all.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum PhysicalNodeKind {
            $( $variant, )*
        }

        impl PhysicalNodeKind {
            /// Every kind, in declaration order.
            pub const ALL: &'static [PhysicalNodeKind] = &[ $( PhysicalNodeKind::$variant, )* ];

            /// The name explain and the execution report print for the kind.
            pub fn explain_name(self) -> &'static str {
                match self { $( Self::$variant => $name, )* }
            }

            /// Every number of children a node of this kind can have, sorted:
            /// the sum over its child fields of the counts each admits.
            pub fn input_counts(self) -> Vec<usize> {
                match self {
                    $(
                        Self::$variant => {
                            #[allow(unused_mut)]
                            let mut counts: Vec<usize> = vec![0];
                            $( counts = sum_counts(&counts, <$cty as ChildSlots>::COUNTS); )*
                            counts
                        }
                    )*
                }
            }
        }

        impl PhysicalNode {
            pub fn kind(&self) -> PhysicalNodeKind {
                match self { $( Self::$variant { .. } => PhysicalNodeKind::$variant, )* }
            }

            pub fn name(&self) -> &'static str {
                self.kind().explain_name()
            }

            pub fn inputs(&self) -> Vec<NodeId> {
                match self {
                    $(
                        Self::$variant { $( $child, )* .. } => {
                            #[allow(unused_mut)]
                            let mut ids = Vec::new();
                            $(
                                let $child: &$cty = $child;
                                ids.extend(ChildSlots::ids($child));
                            )*
                            ids
                        }
                    )*
                }
            }

            fn inputs_mut(&mut self) -> Vec<&mut NodeId> {
                match self {
                    $(
                        Self::$variant { $( $child, )* .. } => {
                            #[allow(unused_mut)]
                            let mut ids = Vec::new();
                            $(
                                let $child: &mut $cty = $child;
                                ids.extend(ChildSlots::ids_mut($child));
                            )*
                            ids
                        }
                    )*
                }
            }
        }
    };
}

physical_nodes! {
    MetadataCount {
        spec: Box<ScanSpec>,
        return_exprs: Vec<IRProjection>,
    } => "MetadataCount" [];
    /// One Lance scan of a side: `ordered` sorts it by id under the bounded
    /// ordered-scan envelope, `keys_only` projects the id with `_rowid` and
    /// `_rowaddr` (else every column, Blobs as descriptors), `ranked` scores it.
    Scan {
        source: ScanInput,
        spec: Box<ScanSpec>,
        ordered: bool,
        keys_only: bool,
        ranked: Option<RankedAccess>,
    } => "Scan" [source: ScanInput];
    /// An id merge of two inputs. Streaming when both are ordered; with
    /// `build`, the right input sinks into the join (a collected side),
    /// is sorted once and the left input probes it.
    SortMergeJoin {
        left: NodeId,
        right: NodeId,
        kind: KeyJoinKind,
        on: String,
        build: bool,
        /// Pass 5: a matched pair with one `_rowaddr` on both sides is
        /// unchanged and never leaves the join.
        drop_equal_addresses: bool,
    } => "SortMergeJoin" [left: NodeId, right: NodeId];
    /// A traversal's destination (`build`, a table `Scan` of it) read once and
    /// hashed on `<binding>.<id>`, probed by the traversal (`probe`) in its
    /// order; `fallback` is the path a build memory refusal may take instead.
    HashJoin {
        probe: NodeId,
        build: NodeId,
        binding: String,
        fallback: Option<AccessPath>,
    } => "HashJoin" [probe: NodeId, build: NodeId];
    /// Fetch one side's complete rows by `_rowaddr`, one bounded chunk at a
    /// time, and re-attach them at their positions.
    HydrateByAddress {
        input: NodeId,
        side: SideId,
    } => "HydrateByAddress" [input: NodeId];
    /// Pass 6 on a query: the return columns only the output reads, fetched
    /// for the rows that reached it by each binding's row address (the
    /// `ROW_ADDRESS_PREFIX` column the projection below carries) from the
    /// binding's pinned table, in bounded chunks, and placed at their return
    /// positions between the input's other columns.
    HydrateColumns {
        input: NodeId,
        bindings: Vec<HydratedBinding>,
    } => "HydrateColumns" [input: NodeId];
    RowCompare {
        input: NodeId,
    } => "RowCompare" [input: NodeId];
    ClassifyThreeWay {
        input: NodeId,
    } => "ClassifyThreeWay" [input: NodeId];
    Limit {
        input: NodeId,
        rows: usize,
    } => "Page" [input: NodeId];
    Page {
        input: NodeId,
        rows: usize,
        bytes: u64,
        resume: Option<String>,
    } => "Page" [input: NodeId];
    /// Two `match` bindings with no edge between them: every pair, or only
    /// the pairs every one of `filters` holds true for, the in-memory
    /// conjuncts of the `Filter` that sat directly on the join.
    CrossJoin {
        left: NodeId,
        right: NodeId,
        filters: Vec<IRExpr>,
    } => "CrossJoin" [left: NodeId, right: NodeId];
    /// Two `match` bindings joined on `haystack contains needle`, both text:
    /// `left` is collected, its `needle` values filter `right` (the table
    /// `Scan` of the `haystack` binding), and `residual` runs on the pairs.
    ContainsJoin {
        left: NodeId,
        right: NodeId,
        haystack: (String, String),
        needle: (String, String),
        conjunct: IRExpr,
        residual: Vec<IRExpr>,
    } => "ContainsJoin" [left: NodeId, right: NodeId];
    /// The in-memory arm of a GQ filter: the conjuncts the placement pass
    /// left where the query wrote them, each evaluated on its own.
    Filter {
        input: NodeId,
        filters: Vec<IRExpr>,
    } => "Filter" [input: NodeId];
    /// A traversal: its declared mode and fallback policy, input row estimate,
    /// and pinned member datasets. A budgeted plan uses the input estimate only
    /// for diagnostics; costed plans also use it to choose their mode.
    Expand {
        input: NodeId,
        src: String,
        dst: String,
        edges: EdgeSelection,
        src_type: String,
        dst_type: String,
        min_hops: u32,
        max_hops: Option<u32>,
        edge_binding: Option<String>,
        mode: ExpandMode,
        frontier_estimate: Option<u64>,
        policy: ExpandPolicy,
        versions: BTreeMap<String, Option<u64>>,
    } => "Expand" [input: NodeId];
    /// A correlated block over `input`. Under `Correlated` the `inner` tree's
    /// leaf is an `OuterReference` to the input rows; under `KeySet` the inner
    /// tree reads no outer rows and ends on `outer_var`'s identity, the key
    /// the input rows are kept by.
    AntiJoin {
        input: NodeId,
        inner: NodeId,
        outer_var: String,
        predicate: SubqueryPredicate,
        aggregate: Option<AggregateSpec>,
        strategy: AntiJoinStrategy,
    } => "AntiJoin" [input: NodeId, inner: NodeId];
    /// The enclosing rows, the leaf of a correlated `AntiJoin` inner tree.
    OuterReference {
        outer_var: String,
    } => "OuterReference" [];
    /// `rrf(a, b)`: two arms, each its own copy of the pipeline with one
    /// ranked scan, every arm row emitted with `<primary binding>._rrf`; `k`
    /// as written, `limit` only the nearest arm's window, `prefilter` feeds `bm25`.
    RankFuse {
        arms: [RankArm; 2],
        k: Option<IRExpr>,
        limit: Option<usize>,
        prefilter: Prefilter,
        /// Downstream row keys after the arm score and fused node identity.
        row_tiebreak: Vec<ColumnRef>,
    } => "RankFuse" [arms: [RankArm; 2]];
    Projection {
        input: NodeId,
        return_exprs: Vec<IRProjection>,
        node_objects: Vec<crate::NodeObjectType>,
    } => "Projection" [input: NodeId];
    Aggregate {
        input: NodeId,
        return_exprs: Vec<IRProjection>,
        aggregates: Vec<Option<AggregateSpec>>,
        node_objects: Vec<crate::NodeObjectType>,
    } => "Aggregate" [input: NodeId];
    Sort {
        input: NodeId,
        order_by: Vec<IROrdering>,
        fetch: Option<usize>,
        /// The metadata columns following `order_by`, ascending nulls first,
        /// so the order is total; empty where they cannot change visible order.
        tiebreak: Vec<ColumnRef>,
    } => "Sort" [input: NodeId];
    /// A load's parallel parser: at most `max_partitions` byte ranges cut on
    /// quote-aware row boundaries, at most `max_workers` readers, batches in
    /// source order; an unprovable boundary falls back to one reader.
    CsvSource {
        spec: Box<CsvSourceSpec>,
        max_partitions: u32,
        max_workers: u32,
    } => "CsvSource" [];
    /// The one validation over the whole delta, a pipeline breaker: every
    /// check extracted in one pass per batch, each check's `access` saying
    /// how its committed image is read after the last batch.
    Validate {
        input: NodeId,
        table: String,
        checks: Vec<ValidateSpec>,
        max_workers: u32,
    } => "Validate" [input: NodeId];
    /// The fragment sink: the validated delta, every input checked against
    /// `max_input_bytes`, written by at most `max_workers` writers into one
    /// staged transaction the driver commits.
    FragmentWrite {
        input: NodeId,
        spec: Box<FragmentWriteSpec>,
        max_input_bytes: u64,
        max_workers: u32,
    } => "FragmentWrite" [input: NodeId];
}

/// How a node's operator relates its output to its streamed input in time:
/// the one fact [`PhysicalPlan::pipelines`] cuts on. The engine builds each
/// operator's output stream through the wrapper of the same name, so the
/// shape printed here is the emission executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PipelineShape {
    /// Emits as its streamed input arrives: a member of its input's pipeline.
    Streaming,
    /// Emits nothing until it has seen all of its streamed input: the cut
    /// between its input's pipeline and the one it sources.
    Breaker,
}

impl PhysicalNode {
    /// The node's [`PipelineShape`] over its streamed input (the one
    /// [`Self::collected_sides`] does not name). A source has no input to
    /// hold and a `FragmentWrite` is the pipeline's end, whose emission is
    /// its input's; neither is a cut. An `Expand` holds its paths only as a
    /// costed multi-hop traversal: one hop streams its pairs and a budgeted
    /// traversal streams its frontier.
    pub fn shape(&self) -> PipelineShape {
        match self {
            Self::Sort { .. }
            | Self::Aggregate { .. }
            | Self::AntiJoin { .. }
            | Self::RankFuse { .. }
            | Self::Validate { .. } => PipelineShape::Breaker,
            Self::Expand {
                max_hops, policy, ..
            } => {
                if *max_hops == Some(1) || matches!(policy, ExpandPolicy::Budgeted) {
                    PipelineShape::Streaming
                } else {
                    PipelineShape::Breaker
                }
            }
            Self::MetadataCount { .. }
            | Self::Scan { .. }
            | Self::OuterReference { .. }
            | Self::CsvSource { .. }
            | Self::HashJoin { .. }
            | Self::SortMergeJoin { .. }
            | Self::CrossJoin { .. }
            | Self::ContainsJoin { .. }
            | Self::RowCompare { .. }
            | Self::ClassifyThreeWay { .. }
            | Self::HydrateByAddress { .. }
            | Self::HydrateColumns { .. }
            | Self::Page { .. }
            | Self::Limit { .. }
            | Self::Filter { .. }
            | Self::Projection { .. }
            | Self::FragmentWrite { .. } => PipelineShape::Streaming,
        }
    }

    /// The inputs the node's operator drains whole before its streamed input
    /// runs: a join's build side, the collected left of a cross or contains
    /// join, a correlated block's inner tree, a fusion's secondary arm. Each
    /// is its own pipeline, drained into the node ([`PipelineSink::Node`]);
    /// the engine collects exactly these through its `CollectedSide`.
    pub fn collected_sides(&self) -> Vec<NodeId> {
        match self {
            Self::HashJoin { build, .. } => vec![*build],
            Self::SortMergeJoin {
                right, build: true, ..
            } => vec![*right],
            Self::CrossJoin { left, .. } | Self::ContainsJoin { left, .. } => vec![*left],
            Self::AntiJoin { inner, .. } => vec![*inner],
            Self::RankFuse { arms, .. } => vec![arms[1].input],
            Self::SortMergeJoin { build: false, .. }
            | Self::MetadataCount { .. }
            | Self::Scan { .. }
            | Self::OuterReference { .. }
            | Self::CsvSource { .. }
            | Self::RowCompare { .. }
            | Self::ClassifyThreeWay { .. }
            | Self::HydrateByAddress { .. }
            | Self::HydrateColumns { .. }
            | Self::Page { .. }
            | Self::Limit { .. }
            | Self::Filter { .. }
            | Self::Expand { .. }
            | Self::Projection { .. }
            | Self::Aggregate { .. }
            | Self::Sort { .. }
            | Self::Validate { .. }
            | Self::FragmentWrite { .. } => Vec::new(),
        }
    }
}

/// One `@Operator` row: a distinct explain name and the sorted, distinct
/// child counts every kind reporting that name admits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorRow {
    pub name: &'static str,
    pub inputs: Vec<usize>,
}

/// The `@Operator` rows: [`PhysicalNodeKind::ALL`] grouped by explain name
/// in declaration order, each group's input counts unioned.
pub fn operator_rows() -> Vec<OperatorRow> {
    let mut rows: Vec<OperatorRow> = Vec::new();
    for kind in PhysicalNodeKind::ALL {
        let name = kind.explain_name();
        let counts = kind.input_counts();
        match rows.iter_mut().find(|row| row.name == name) {
            Some(row) => {
                row.inputs.extend(counts);
                row.inputs.sort_unstable();
                row.inputs.dedup();
            }
            None => rows.push(OperatorRow {
                name,
                inputs: counts,
            }),
        }
    }
    rows
}

impl PhysicalNode {
    /// The most rows this node keeps of its input whatever any predicate
    /// holds: its count cut. Every variant is listed, so a new node kind
    /// cannot compile without a classification here; a field that gives an
    /// existing kind a cut still needs its arm changed by hand. Plan
    /// acceptance justifies every declared cut against the query.
    pub fn row_cut(&self) -> Option<usize> {
        match self {
            Self::Limit { rows, .. } | Self::Page { rows, .. } => Some(*rows),
            Self::Sort { fetch, .. } => *fetch,
            Self::Scan { ranked, .. } => ranked.as_ref().and_then(|ranked| ranked.fetch),
            // A fusion emits every arm row; its `limit` sizes only the
            // nearest arm's window, which that arm's scan declares as its
            // fetch.
            Self::RankFuse { .. }
            | Self::MetadataCount { .. }
            | Self::SortMergeJoin { .. }
            | Self::HashJoin { .. }
            | Self::HydrateByAddress { .. }
            | Self::HydrateColumns { .. }
            | Self::RowCompare { .. }
            | Self::ClassifyThreeWay { .. }
            | Self::CrossJoin { .. }
            | Self::ContainsJoin { .. }
            | Self::Filter { .. }
            | Self::Expand { .. }
            | Self::AntiJoin { .. }
            | Self::OuterReference { .. }
            | Self::Projection { .. }
            | Self::Aggregate { .. }
            | Self::CsvSource { .. }
            | Self::Validate { .. }
            | Self::FragmentWrite { .. } => None,
        }
    }

    /// Whether the node traverses edges: an `Expand`, or an `AntiJoin`,
    /// whose inner tree runs over the CSR.
    pub fn traverses(&self) -> bool {
        matches!(self, Self::Expand { .. } | Self::AntiJoin { .. })
    }

    /// The ranking of a `Scan`, `None` on every other node.
    pub fn ranked(&self) -> Option<&RankedAccess> {
        match self {
            Self::Scan { ranked, .. } => ranked.as_ref(),
            _ => None,
        }
    }
}

/// A `(binding, property)` as GQ spells it: `$p.text`.
pub(crate) fn column_text((binding, property): &(String, String)) -> String {
    format!("${binding}.{property}")
}

/// A `$a.x contains $b.y` conjunct over two bindings: the `(binding,
/// property)` whose text is searched and the one whose text is searched for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TextContains<'a> {
    pub haystack: (&'a str, &'a str),
    pub needle: (&'a str, &'a str),
}

impl<'a> TextContains<'a> {
    /// `conjunct` when it is a string `contains` between properties of two
    /// distinct bindings, `None` for any other shape.
    pub(crate) fn of(conjunct: &'a IRExpr) -> Option<Self> {
        let IRExpr::Binary {
            left,
            op: BinaryOp::Compare(CompOp::StringContains),
            right,
            ..
        } = conjunct
        else {
            return None;
        };
        let (
            IRExpr::PropAccess {
                variable: haystack,
                property: searched,
                ty: _,
            },
            IRExpr::PropAccess {
                variable: needle,
                property: sought,
                ty: _,
            },
        ) = (left.as_ref(), right.as_ref())
        else {
            return None;
        };
        (haystack != needle).then_some(Self {
            haystack: (haystack, searched),
            needle: (needle, sought),
        })
    }
}

/// Arena of physical nodes with their declared properties. Two plans are
/// equal when their serialized mirrors are, node for node and property for
/// property.
#[derive(Debug, Clone, Default)]
pub struct PhysicalPlan {
    slots: Vec<Option<PhysicalNode>>,
    properties: Vec<Option<Properties>>,
    root: NodeId,
    assumptions: Assumptions,
}

impl PartialEq for PhysicalPlan {
    fn eq(&self, other: &Self) -> bool {
        crate::mirror::PlanMirror::from(self) == crate::mirror::PlanMirror::from(other)
    }
}

impl PhysicalPlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_parts(
        slots: Vec<Option<PhysicalNode>>,
        properties: Vec<Option<Properties>>,
        root: NodeId,
        assumptions: Assumptions,
    ) -> Self {
        Self {
            slots,
            properties,
            root,
            assumptions,
        }
    }

    pub(crate) fn slots(&self) -> &[Option<PhysicalNode>] {
        &self.slots
    }

    /// What the planner read to build this plan.
    pub fn assumptions(&self) -> &Assumptions {
        &self.assumptions
    }

    pub fn set_assumptions(&mut self, assumptions: Assumptions) {
        self.assumptions = assumptions;
    }

    pub(crate) fn property_slots(&self) -> &[Option<Properties>] {
        &self.properties
    }

    pub fn add(&mut self, node: PhysicalNode) -> NodeId {
        self.slots.push(Some(node));
        self.properties.push(None);
        self.slots.len() - 1
    }

    pub fn set_root(&mut self, root: NodeId) {
        self.root = root;
    }

    pub fn root(&self) -> NodeId {
        self.root
    }

    pub fn node(&self, id: NodeId) -> Option<&PhysicalNode> {
        self.slots.get(id).and_then(Option::as_ref)
    }

    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut PhysicalNode> {
        self.slots.get_mut(id).and_then(Option::as_mut)
    }

    pub fn properties(&self, id: NodeId) -> Option<&Properties> {
        self.properties.get(id).and_then(Option::as_ref)
    }

    pub fn set_properties(&mut self, id: NodeId, properties: Properties) {
        if let Some(slot) = self.properties.get_mut(id) {
            *slot = Some(properties);
        }
    }

    pub fn splice_out(&mut self, from: NodeId, to: NodeId) {
        for slot in self.slots.iter_mut().flatten() {
            for input in slot.inputs_mut() {
                if *input == from {
                    *input = to;
                }
            }
        }
        if self.root == from {
            self.root = to;
        }
        if let Some(slot) = self.slots.get_mut(from) {
            *slot = None;
        }
        if let Some(slot) = self.properties.get_mut(from) {
            *slot = None;
        }
    }

    pub fn live(&self) -> impl Iterator<Item = (NodeId, &PhysicalNode)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(id, slot)| slot.as_ref().map(|node| (id, node)))
    }

    /// Post-order walk from the root: every input before its consumer.
    pub fn post_order(&self) -> Vec<NodeId> {
        let mut order = Vec::new();
        self.visit(self.root, &mut order);
        order
    }

    /// The subtree under `id` in post-order, `id` last.
    pub(crate) fn subtree(&self, id: NodeId) -> Vec<NodeId> {
        let mut order = Vec::new();
        self.visit(id, &mut order);
        order
    }

    /// A copy of the subtree under `id`, node for node, with fresh ids; the
    /// copy's root. Properties are not copied: they are derived afterwards.
    pub fn duplicate(&mut self, id: NodeId) -> Option<NodeId> {
        let mut node = self.node(id)?.clone();
        let inputs: Vec<NodeId> = node.inputs();
        let mut copies = Vec::with_capacity(inputs.len());
        for input in inputs {
            copies.push(self.duplicate(input)?);
        }
        for (slot, copy) in node.inputs_mut().into_iter().zip(copies) {
            *slot = copy;
        }
        Some(self.add(node))
    }

    fn visit(&self, id: NodeId, order: &mut Vec<NodeId>) {
        let Some(node) = self.node(id) else {
            return;
        };
        for input in node.inputs() {
            self.visit(input, order);
        }
        order.push(id);
    }

    pub fn to_json(&self) -> Value {
        self.node_json(self.root, self.is_query())
    }

    /// A GQ query plan: a scan bound to a `match` variable, or the
    /// `MetadataCount` that replaced one. Diff and merge plans hold neither.
    fn is_query(&self) -> bool {
        self.live().any(|(_, node)| match node {
            PhysicalNode::Scan { spec, .. } => spec.binding.is_some(),
            PhysicalNode::MetadataCount { .. } => true,
            _ => false,
        })
    }

    fn node_json(&self, id: NodeId, query: bool) -> Value {
        let Some(node) = self.node(id) else {
            return json!({ "node": "tombstone", "id": id });
        };
        let mut value = match node {
            PhysicalNode::MetadataCount { spec, return_exprs } => {
                metadata_count_json(spec, return_exprs)
            }
            PhysicalNode::Scan {
                source,
                spec,
                ordered,
                keys_only,
                ranked,
            } => {
                let mut value = scan_json("Scan", spec);
                if let Some(access) = &spec.access {
                    access.explain(&mut value);
                }
                if let ScanInput::Dependent { .. } = source {
                    value["id_restriction"] = json!("input");
                    value["access"] = json!(AccessPath::IdLookup);
                }
                value["ordered"] = json!(ordered);
                value["keys_only"] = json!(keys_only);
                if let Some(ranked) = ranked {
                    value["ranked"] = ranked.to_json();
                }
                value
            }
            PhysicalNode::SortMergeJoin {
                kind,
                on,
                build,
                drop_equal_addresses,
                ..
            } => json!({
                "node": "SortMergeJoin",
                "kind": kind,
                "on": on,
                "build": build,
                "drop_equal_addresses": drop_equal_addresses,
            }),
            PhysicalNode::HydrateByAddress { side, .. } => json!({
                "node": "HydrateByAddress",
                "side": side,
            }),
            PhysicalNode::HydrateColumns { bindings, .. } => json!({
                "node": "HydrateColumns",
                "bindings": bindings
                    .iter()
                    .map(|binding| json!({
                        "binding": binding.binding,
                        "table": binding.table.type_key,
                        "columns": binding
                            .columns
                            .iter()
                            .map(|column| column.property.as_str())
                            .collect::<Vec<&str>>(),
                    }))
                    .collect::<Vec<Value>>(),
            }),
            PhysicalNode::HashJoin {
                binding, fallback, ..
            } => json!({
                "node": "HashJoin",
                "binding": binding,
                "fallback": fallback,
            }),
            PhysicalNode::Limit { rows, .. } => json!({
                "node": "Page", "rows": rows, "bytes": 0, "resume": null,
            }),
            PhysicalNode::Page {
                rows,
                bytes,
                resume,
                ..
            } => json!({
                "node": "Page",
                "rows": rows,
                "bytes": bytes,
                "resume": resume,
            }),
            PhysicalNode::Filter { filters, .. } => json!({
                "node": "Filter",
                "filters": filters_json(filters),
                "typed_filters": crate::typed::exprs(filters),
            }),
            PhysicalNode::Expand {
                src,
                dst,
                edges,
                src_type,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                mode,
                frontier_estimate,
                policy,
                versions,
                ..
            } => json!({
                "node": "Expand",
                "src": src,
                "dst": dst,
                "edges": EdgeSelectionMirror::from(edges),
                "src_type": src_type,
                "dst_type": dst_type,
                "min_hops": min_hops,
                "max_hops": max_hops,
                "edge_binding": edge_binding,
                "mode": mode,
                "alternatives": policy.alternatives(*mode),
                "frontier_estimate": frontier_estimate,
                "versions": versions,
            }),
            PhysicalNode::AntiJoin {
                outer_var,
                predicate,
                aggregate,
                strategy,
                ..
            } => json!({
                "node": node.name(),
                "outer_var": outer_var,
                "strategy": strategy,
                "predicate": predicate.to_string(),
                "aggregate": crate::typed::block_aggregate(&predicate.left, *aggregate),
                "typed_left": crate::typed::block(&predicate.left),
                "typed_right": crate::typed::expr(&predicate.right),
            }),
            PhysicalNode::OuterReference { outer_var } => json!({
                "node": node.name(),
                "outer_var": outer_var,
            }),
            PhysicalNode::RankFuse {
                arms,
                k,
                limit,
                row_tiebreak,
                ..
            } => json!({
                "node": "RankFuse",
                "arms": arms
                    .iter()
                    .map(|arm| json!({ "binding": arm.binding, "kind": arm.kind }))
                    .collect::<Vec<Value>>(),
                "k": k.as_ref().map(ToString::to_string),
                "typed_k": k.as_ref().map(crate::typed::expr),
                "limit": limit,
                "row_tiebreak": tiebreak_text(row_tiebreak),
            }),
            PhysicalNode::Aggregate {
                return_exprs,
                aggregates,
                ..
            } => json!({
                "node": node.name(),
                "exprs": return_exprs.iter().map(|projection| projection.expr.to_string()).collect::<Vec<_>>(),
                "columns": crate::output::return_columns(return_exprs),
                "typed_exprs": crate::typed::returns(return_exprs),
                "aggregates": return_exprs.iter().zip(aggregates).map(|(projection, spec)| {
                    match (&projection.expr, spec) {
                        (IRExpr::Aggregate { func, distinct, signature, .. }, Some(spec)) => json!({
                            "column": crate::optimizer::result_column(projection),
                            "func": func.to_string(),
                            "distinct": distinct,
                            "input": signature.arg.spelling(),
                            "accumulator": spec.accumulator,
                            "overflow": spec.overflow,
                            "result": signature.result.spelling(),
                        }),
                        _ => Value::Null,
                    }
                }).collect::<Vec<_>>(),
            }),
            PhysicalNode::Projection { return_exprs, .. } => json!({
                "node": node.name(),
                "columns": crate::output::return_columns(return_exprs),
                "typed_exprs": crate::typed::returns(return_exprs),
                "exprs": return_exprs
                    .iter()
                    .map(|projection| projection.expr.to_string())
                    .collect::<Vec<String>>(),
            }),
            PhysicalNode::Sort {
                order_by,
                fetch,
                tiebreak,
                ..
            } => json!({
                "node": "Sort",
                "keys": order_by.iter().map(ordering_text).collect::<Vec<String>>(),
                "typed_keys": order_by.iter().map(|key| crate::typed::expr(&key.expr)).collect::<Vec<_>>(),
                "fetch": fetch,
                "tiebreak": tiebreak_text(tiebreak),
            }),
            PhysicalNode::CrossJoin { filters, .. } if !filters.is_empty() => json!({
                "node": "CrossJoin",
                "filters": filters_json(filters),
                "typed_filters": crate::typed::exprs(filters),
            }),
            PhysicalNode::ContainsJoin {
                haystack,
                needle,
                conjunct,
                residual,
                ..
            } => json!({
                "node": "ContainsJoin",
                "haystack": column_text(haystack),
                "needle": column_text(needle),
                "residual": filters_json(residual),
                "typed_residual": crate::typed::exprs(residual),
                "typed_conjunct": crate::typed::expr(conjunct),
            }),
            PhysicalNode::CsvSource {
                spec,
                max_partitions,
                max_workers,
            } => {
                let mut value = csv_source_json(spec);
                value["max_partitions"] = json!(max_partitions);
                value["max_workers"] = json!(max_workers);
                value["fallback"] = json!("single_scan");
                value
            }
            PhysicalNode::Validate {
                table,
                checks,
                max_workers,
                ..
            } => {
                let mut value = validate_json(table, checks);
                value["max_workers"] = json!(max_workers);
                value
            }
            PhysicalNode::FragmentWrite {
                spec,
                max_input_bytes,
                max_workers,
                ..
            } => {
                let mut value = fragment_write_json(spec);
                value["max_input_bytes"] = json!(max_input_bytes);
                value["max_workers"] = json!(max_workers);
                value
            }
            other => json!({ "node": other.name() }),
        };
        value["id"] = json!(id);
        if let Some(properties) = self.properties(id) {
            value["properties"] = properties.to_json(query);
        }
        let inputs: Vec<Value> = node
            .inputs()
            .into_iter()
            .map(|input| self.node_json(input, query))
            .collect();
        if !inputs.is_empty() {
            value["inputs"] = Value::Array(inputs);
        }
        value
    }

    /// The plan cut into pipelines: walking from the root, every side a node
    /// drains whole ([`PhysicalNode::collected_sides`]) becomes a child
    /// pipeline sunk into the node; over its streamed input a `Streaming`
    /// node joins the current pipeline and a `Breaker`
    /// ([`PhysicalNode::shape`]) ends it, the input chain becoming a child
    /// pipeline drained into the breaker and the breaker the source of the
    /// pipeline above; a scan is a source. Child pipelines come first, so a
    /// pipeline's `depends_on` names only earlier pipelines, and the last
    /// pipeline is the root one feeding the consumer. The push executor
    /// builds diff and merge plans from this decomposition; for a query or a
    /// load it describes the pull chain the engine lowers, cut where the
    /// engine's shape wrappers hold their input.
    pub fn pipelines(&self) -> Vec<PipelineSpec> {
        let mut pipelines = Vec::new();
        let mut operators = Vec::new();
        let mut depends_on = Vec::new();
        let source = self.pipeline_of(self.root, &mut operators, &mut pipelines, &mut depends_on);
        operators.reverse();
        let id = pipelines.len();
        pipelines.push(PipelineSpec {
            id,
            partitions: self.source_partitions(&source),
            source,
            operators,
            sink: PipelineSink::Consumer,
            depends_on,
        });
        pipelines
    }

    /// The output partitions of a pipeline's source.
    fn source_partitions(&self, source: &PipelineSource) -> u32 {
        match source {
            PipelineSource::Node(id) => match self.node(*id) {
                Some(PhysicalNode::CsvSource { max_partitions, .. }) => (*max_partitions).max(1),
                _ => 1,
            },
            PipelineSource::Streaming { .. } | PipelineSource::Tombstone(_) => 1,
        }
    }

    /// [`Self::pipelines`] serialized for explain: per pipeline its `id`,
    /// `source`, `operators` and `sink`, each operator as `{id, name}` whose
    /// `id` joins the `physical_plan` nodes, and `depends_on` naming the
    /// pipelines drained before this one starts.
    pub fn pipelines_json(&self) -> Value {
        Value::Array(
            self.pipelines()
                .iter()
                .map(|pipeline| self.pipeline_json(pipeline))
                .collect(),
        )
    }

    fn pipeline_json(&self, pipeline: &PipelineSpec) -> Value {
        let sink = match pipeline.sink {
            PipelineSink::Consumer => json!({ "name": "consumer" }),
            PipelineSink::Node(id) => json!({ "id": id, "name": self.sink_name(id) }),
            PipelineSink::Breaker(id) => json!({ "id": id, "name": self.breaker_name(id) }),
        };
        json!({
            "id": pipeline.id,
            "source": self.source_json(&pipeline.source),
            "partitions": pipeline.partitions,
            "operators": self.operators_json(&pipeline.operators),
            "sink": sink,
            "depends_on": pipeline.depends_on,
        })
    }

    fn operators_json(&self, operators: &[NodeId]) -> Value {
        Value::Array(
            operators
                .iter()
                .map(|id| json!({ "id": id, "name": self.operator_name(*id) }))
                .collect(),
        )
    }

    fn source_json(&self, source: &PipelineSource) -> Value {
        match source {
            PipelineSource::Tombstone(id) => json!({ "id": id, "node": "tombstone" }),
            PipelineSource::Node(id) => {
                let mut value = match self.node(*id) {
                    Some(PhysicalNode::MetadataCount { spec, return_exprs }) => {
                        metadata_count_json(spec, return_exprs)
                    }
                    Some(PhysicalNode::Scan {
                        source: ScanInput::Table,
                        spec,
                        ordered,
                        ..
                    }) => json!({
                        "node": "Scan",
                        "side": spec.side,
                        "ordered": ordered,
                    }),
                    Some(PhysicalNode::CsvSource { spec, .. }) => json!({
                        "node": "CsvSource",
                        "source": spec.source,
                    }),
                    Some(PhysicalNode::OuterReference { outer_var }) => json!({
                        "node": "OuterReference",
                        "outer_var": outer_var,
                    }),
                    Some(other) => json!({ "node": other.name() }),
                    None => json!({ "node": "tombstone" }),
                };
                value["id"] = json!(id);
                value
            }
            PipelineSource::Streaming { node, left, right } => {
                let name = match self.node(*node) {
                    Some(PhysicalNode::SortMergeJoin { .. }) => "SortMergeJoin(streaming)",
                    Some(other) => other.name(),
                    None => "tombstone",
                };
                json!({
                    "id": node,
                    "node": name,
                    "left": self.side_json(left),
                    "right": self.side_json(right),
                })
            }
        }
    }

    fn side_json(&self, side: &SideChain) -> Value {
        json!({
            "source": self.source_json(&side.source),
            "operators": self.operators_json(&side.operators),
        })
    }

    /// The name an operator occurrence of `id` prints in a pipeline: a join
    /// names its streaming role, every other node its own name.
    fn operator_name(&self, id: NodeId) -> String {
        match self.node(id) {
            Some(PhysicalNode::HashJoin { .. }) => "HashJoin(probe)".to_string(),
            Some(PhysicalNode::SortMergeJoin { .. }) => "SortMergeJoin(probe)".to_string(),
            Some(PhysicalNode::CrossJoin { .. }) => "CrossJoin(probe)".to_string(),
            Some(PhysicalNode::ContainsJoin { .. }) => "ContainsJoin(probe)".to_string(),
            Some(PhysicalNode::Scan {
                source: ScanInput::Dependent { .. },
                ..
            }) => "Scan(input_ids)".to_string(),
            Some(PhysicalNode::HydrateByAddress { side, .. }) => {
                format!("HydrateByAddress({})", side.name())
            }
            Some(node) => node.name().to_string(),
            None => "tombstone".to_string(),
        }
    }

    /// The name a sink occurrence of `id` prints: the side of the node the
    /// pipeline drains into.
    fn sink_name(&self, id: NodeId) -> String {
        match self.node(id) {
            Some(PhysicalNode::HashJoin { .. }) => "HashJoin(build)".to_string(),
            Some(PhysicalNode::SortMergeJoin { .. }) => "SortMergeJoin(build)".to_string(),
            Some(PhysicalNode::CrossJoin { .. }) => "CrossJoin(build)".to_string(),
            Some(PhysicalNode::ContainsJoin { .. }) => "ContainsJoin(build)".to_string(),
            Some(PhysicalNode::AntiJoin { .. }) => "AntiJoin(inner)".to_string(),
            Some(PhysicalNode::RankFuse { .. }) => "RankFuse(secondary)".to_string(),
            Some(node) => node.name().to_string(),
            None => "tombstone".to_string(),
        }
    }

    /// The name a breaker occurrence of `id` prints: the node itself, the
    /// whole of whose input the pipeline is.
    fn breaker_name(&self, id: NodeId) -> String {
        match self.node(id) {
            Some(node) => node.name().to_string(),
            None => "tombstone".to_string(),
        }
    }

    fn pipeline_of(
        &self,
        id: NodeId,
        operators: &mut Vec<NodeId>,
        pipelines: &mut Vec<PipelineSpec>,
        depends_on: &mut Vec<usize>,
    ) -> PipelineSource {
        let Some(node) = self.node(id) else {
            return PipelineSource::Tombstone(id);
        };
        if let PhysicalNode::SortMergeJoin {
            left,
            right,
            build: false,
            ..
        } = node
        {
            return PipelineSource::Streaming {
                node: id,
                left: self.side_chain(*left, pipelines, depends_on),
                right: self.side_chain(*right, pipelines, depends_on),
            };
        }
        let collected = node.collected_sides();
        for side in &collected {
            depends_on.push(self.build_pipeline(*side, id, pipelines));
        }
        let streamed: Vec<NodeId> = node
            .inputs()
            .into_iter()
            .filter(|input| !collected.contains(input))
            .collect();
        debug_assert!(
            streamed.len() <= 1,
            "`{}` streams {} inputs; a node streams at most one",
            node.name(),
            streamed.len()
        );
        let [streamed] = streamed[..] else {
            return PipelineSource::Node(id);
        };
        match node.shape() {
            PipelineShape::Breaker => {
                depends_on.push(self.breaker_pipeline(streamed, id, pipelines));
                PipelineSource::Node(id)
            }
            PipelineShape::Streaming => {
                operators.push(id);
                self.pipeline_of(streamed, operators, pipelines, depends_on)
            }
        }
    }

    /// The whole input of breaker `sink`: its own pipeline, drained into the
    /// breaker before the pipeline the breaker feeds starts. Returns the
    /// child pipeline's id.
    fn breaker_pipeline(
        &self,
        input: NodeId,
        sink: NodeId,
        pipelines: &mut Vec<PipelineSpec>,
    ) -> usize {
        let mut operators = Vec::new();
        let mut depends_on = Vec::new();
        let source = self.pipeline_of(input, &mut operators, pipelines, &mut depends_on);
        operators.reverse();
        let id = pipelines.len();
        pipelines.push(PipelineSpec {
            id,
            partitions: self.source_partitions(&source),
            source,
            operators,
            sink: PipelineSink::Breaker(sink),
            depends_on,
        });
        id
    }

    /// The build side of `sink`: its own pipeline, drained into `sink`
    /// before the pipeline holding `sink` as an operator starts. Returns the
    /// child pipeline's id.
    fn build_pipeline(
        &self,
        input: NodeId,
        sink: NodeId,
        pipelines: &mut Vec<PipelineSpec>,
    ) -> usize {
        let mut operators = Vec::new();
        let mut depends_on = Vec::new();
        let source = self.pipeline_of(input, &mut operators, pipelines, &mut depends_on);
        operators.reverse();
        let id = pipelines.len();
        pipelines.push(PipelineSpec {
            id,
            partitions: self.source_partitions(&source),
            source,
            operators,
            sink: PipelineSink::Node(sink),
            depends_on,
        });
        id
    }

    /// One input of a two-sided streaming node: its source and the operators
    /// between that source and the node, in run order; a build pipeline met
    /// inside the chain lands in `pipelines` and the caller's `depends_on`.
    fn side_chain(
        &self,
        id: NodeId,
        pipelines: &mut Vec<PipelineSpec>,
        depends_on: &mut Vec<usize>,
    ) -> SideChain {
        let mut operators = Vec::new();
        let source = self.pipeline_of(id, &mut operators, pipelines, depends_on);
        operators.reverse();
        SideChain {
            source: Box::new(source),
            operators,
        }
    }
}

/// One pipeline of a plan: `source` feeds `operators` in run order and the
/// last operator's output reaches `sink`. The push executor consumes the same
/// decomposition explain serializes, and the engine builds every operator's
/// stream through the wrapper named by [`PhysicalNode::shape`], so the
/// printed pipelines are the executed ones on every route.
#[derive(Debug, Clone, PartialEq)]
pub struct PipelineSpec {
    /// Position in [`PhysicalPlan::pipelines`], child pipelines first.
    pub id: usize,
    pub source: PipelineSource,
    /// The output partitions of `source`, which every operator of the
    /// pipeline keeps and its sink merges: a `CsvSource`'s `max_partitions`,
    /// 1 for every other source.
    pub partitions: u32,
    /// Operator node ids between `source` and `sink`, in run order.
    pub operators: Vec<NodeId>,
    pub sink: PipelineSink,
    /// Ids of the pipelines drained before this one starts: its build sides,
    /// in walk order.
    pub depends_on: Vec<usize>,
}

/// Where a pipeline's rows come from.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineSource {
    /// A single source node: a table scan, a metadata count, or an outer
    /// reference.
    Node(NodeId),
    /// A sort-merge join without a build side: each side is its own chain the
    /// node pulls, not a drained pipeline. A cross join and a contains join
    /// collect their left side, so that side is a build pipeline instead.
    Streaming {
        node: NodeId,
        left: SideChain,
        right: SideChain,
    },
    /// The walk reached a tombstoned slot.
    Tombstone(NodeId),
}

/// One input chain of a [`PipelineSource::Streaming`] node: the side's own
/// source and the operators between it and the node, in run order.
#[derive(Debug, Clone, PartialEq)]
pub struct SideChain {
    pub source: Box<PipelineSource>,
    pub operators: Vec<NodeId>,
}

/// What consumes a pipeline's output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineSink {
    /// The plan's consumer pulls the root pipeline.
    Consumer,
    /// The pipeline drains into this node's build side before the pipeline
    /// holding the node as an operator starts.
    Node(NodeId),
    /// The pipeline drains into this node, which emits nothing until it has
    /// seen all of its input ([`PipelineShape::Breaker`]); the node's
    /// output is the source of the pipeline that depends on this one.
    Breaker(NodeId),
}
