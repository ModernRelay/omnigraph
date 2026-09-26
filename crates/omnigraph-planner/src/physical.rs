use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::SchemaRef;
use omnigraph_compiler::ir::{IRExpr, IROrdering, IRProjection, SubqueryPredicate};
use omnigraph_compiler::types::Direction;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::cost::{AccessPath, ExpandMode, ExpandPolicy};
use crate::logical::{
    KeyJoinKind, ScanSpec, direction_word, filters_json, metadata_count_json, ordering_text,
    scan_json,
};
use crate::source::SideId;

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
/// of the `rrf-gate` bench (`benches/scenarios.rs`) across both corpora.
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
/// scans run as planned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prefilter {
    pub ranked_type: String,
    pub hops: Vec<Hop>,
    pub feeds: Vec<NodeId>,
}

impl Prefilter {
    /// Whether the plan's shape admits an eligible set at all.
    pub fn admits(&self) -> bool {
        !self.hops.is_empty() && !self.feeds.is_empty()
    }
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
}

impl RankedAccess {
    /// The score ordering this ranking imposes on `binding`'s rows.
    pub fn ordering(&self, binding: &str) -> IROrdering {
        let (property, descending) = self.kind.score();
        IROrdering {
            expr: IRExpr::PropAccess {
                variable: binding.to_string(),
                property: property.to_string(),
            },
            descending,
        }
    }

    fn to_json(&self) -> Value {
        let mut value = json!({
            "kind": self.kind,
            "property": self.property,
            "query": self.query.to_string(),
            "fetch": self.fetch,
            "scope": self.scope,
        });
        if self.kind == RankKind::Nearest {
            value["nprobes"] = json!(self.nprobes);
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
    pub origin: &'static str,
}

/// The properties a physical node declares: derived after selection,
/// recomputed when a later pass changes the node, never recomputed by an
/// executor. Each is a bound the executor adapts within, never above.
#[derive(Debug, Clone)]
pub struct Properties {
    pub schema: SchemaRef,
    pub ordering: Option<Vec<String>>,
    pub rows: Estimate,
    pub work_bytes: Estimate,
    pub retained_limit: Option<u64>,
    pub sources: Vec<StatisticSource>,
}

impl Properties {
    /// A query plan prints no `schema`: its run-time schemas are the
    /// engine's to derive, and the planner's are conservative input schemas.
    fn to_json(&self, query: bool) -> Value {
        let mut value = json!({
            "ordering": self.ordering,
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

/// The operator catalog. The change-feed and merge nodes (`Scan`,
/// `SortMergeJoin`, `HydrateByAddress`, `RowCompare`, `ClassifyThreeWay`,
/// `Page`) each have one push operator in the engine's `engine/push/`; the
/// read nodes each have one arm in the engine's lowering
/// (`crates/omnigraph/src/engine/lower.rs`), whose exhaustive match is the
/// proof. A read node carries the IR pieces its arm takes, so the lowering
/// never re-derives from `QueryIR`.
#[derive(Debug, Clone)]
pub enum PhysicalNode {
    MetadataCount {
        spec: Box<ScanSpec>,
        return_exprs: Vec<IRProjection>,
    },
    /// One Lance scan of a side: `ordered` sorts it by id under the bounded
    /// ordered-scan envelope, `keys_only` projects the id with `_rowid` and
    /// `_rowaddr` (else every column, Blobs as descriptors), `ranked` scores it.
    Scan {
        source: ScanInput,
        spec: Box<ScanSpec>,
        ordered: bool,
        keys_only: bool,
        ranked: Option<RankedAccess>,
    },
    /// An id merge of two inputs. Streaming when both are ordered; with
    /// `build`, the right input sinks into the join (the pipeline breaker),
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
    },
    /// A traversal's destination (`build`, a table `Scan` of it) read once and
    /// hashed on `<binding>.<id>`, probed by the traversal (`probe`) in its
    /// order; `fallback` is the path a build memory refusal may take instead.
    HashJoin {
        probe: NodeId,
        build: NodeId,
        binding: String,
        fallback: Option<AccessPath>,
    },
    /// Fetch one side's complete rows by `_rowaddr`, one bounded chunk at a
    /// time, and re-attach them at their positions.
    HydrateByAddress {
        input: NodeId,
        side: SideId,
    },
    RowCompare {
        input: NodeId,
    },
    ClassifyThreeWay {
        input: NodeId,
    },
    Limit {
        input: NodeId,
        rows: usize,
    },
    Page {
        input: NodeId,
        rows: usize,
        bytes: u64,
        resume: Option<String>,
    },
    /// Two `match` bindings with no edge between them: every pair.
    CrossJoin {
        left: NodeId,
        right: NodeId,
    },
    /// The in-memory arm of a GQ filter: the conjuncts the placement pass
    /// left where the query wrote them, each evaluated on its own.
    Filter {
        input: NodeId,
        filters: Vec<IRExpr>,
    },
    /// A traversal: the mode the cost model chose (or the session pinned), the
    /// estimate it was chosen for, the policy for taking the other mode, and
    /// the pinned dataset version of the edge table it reads.
    Expand {
        input: NodeId,
        src: String,
        dst: String,
        edge_type: String,
        direction: Direction,
        dst_type: String,
        min_hops: u32,
        max_hops: Option<u32>,
        edge_binding: Option<String>,
        mode: ExpandMode,
        frontier_estimate: Option<u64>,
        policy: ExpandPolicy,
        version: Option<u64>,
    },
    AntiJoin {
        input: NodeId,
        inner: NodeId,
        outer_var: String,
        predicate: SubqueryPredicate,
    },
    /// The enclosing rows, the leaf of an `AntiJoin` inner tree.
    OuterReference {
        outer_var: String,
    },
    /// `rrf(a, b)`: two arms, each its own copy of the pipeline with one
    /// ranked scan, fused by reciprocal rank; `k` as the query wrote it (`None`
    /// the default), `limit` the fused rows, `prefilter` feeds the `bm25` arms.
    RankFuse {
        arms: [RankArm; 2],
        k: Option<IRExpr>,
        limit: Option<usize>,
        prefilter: Prefilter,
    },
    Projection {
        input: NodeId,
        return_exprs: Vec<IRProjection>,
    },
    Aggregate {
        input: NodeId,
        return_exprs: Vec<IRProjection>,
    },
    Sort {
        input: NodeId,
        order_by: Vec<IROrdering>,
        fetch: Option<usize>,
        /// The bindings whose ids follow `order_by`, ascending nulls first,
        /// so the order is total; empty where ids cannot change the visible
        /// order.
        tiebreak: Vec<String>,
    },
}

impl PhysicalNode {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Scan { .. } => "Scan",
            Self::MetadataCount { .. } => "MetadataCount",
            Self::HashJoin { .. } => "HashJoin",
            Self::HydrateByAddress { .. } => "HydrateByAddress",
            Self::SortMergeJoin { .. } => "SortMergeJoin",
            Self::RowCompare { .. } => "RowCompare",
            Self::ClassifyThreeWay { .. } => "ClassifyThreeWay",
            Self::Page { .. } | Self::Limit { .. } => "Page",
            Self::CrossJoin { .. } => "CrossJoin",
            Self::Filter { .. } => "Filter",
            Self::Expand { .. } => "Expand",
            Self::AntiJoin { .. } => "AntiJoin",
            Self::OuterReference { .. } => "OuterReference",
            Self::RankFuse { .. } => "RankFuse",
            Self::Projection { .. } => "Projection",
            Self::Aggregate { .. } => "Aggregate",
            Self::Sort { .. } => "Sort",
        }
    }

    pub fn inputs(&self) -> Vec<NodeId> {
        match self {
            Self::HydrateByAddress { input, .. }
            | Self::RowCompare { input, .. }
            | Self::ClassifyThreeWay { input }
            | Self::Page { input, .. }
            | Self::Limit { input, .. }
            | Self::Filter { input, .. }
            | Self::Expand { input, .. }
            | Self::Projection { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. } => vec![*input],
            Self::SortMergeJoin { left, right, .. } | Self::CrossJoin { left, right } => {
                vec![*left, *right]
            }
            Self::HashJoin { probe, build, .. } => vec![*probe, *build],
            Self::AntiJoin { input, inner, .. } => vec![*input, *inner],
            Self::RankFuse { arms, .. } => arms.iter().map(|arm| arm.input).collect(),
            Self::Scan { source, .. } => source.input().copied().into_iter().collect(),
            Self::MetadataCount { .. } | Self::OuterReference { .. } => Vec::new(),
        }
    }

    fn inputs_mut(&mut self) -> Vec<&mut NodeId> {
        match self {
            Self::HydrateByAddress { input, .. }
            | Self::RowCompare { input, .. }
            | Self::ClassifyThreeWay { input }
            | Self::Page { input, .. }
            | Self::Limit { input, .. }
            | Self::Filter { input, .. }
            | Self::Expand { input, .. }
            | Self::Projection { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. } => vec![input],
            Self::SortMergeJoin { left, right, .. } | Self::CrossJoin { left, right } => {
                vec![left, right]
            }
            Self::HashJoin { probe, build, .. } => vec![probe, build],
            Self::AntiJoin { input, inner, .. } => vec![input, inner],
            Self::RankFuse { arms, .. } => arms.iter_mut().map(|arm| &mut arm.input).collect(),
            Self::Scan { source, .. } => source.input_mut().into_iter().collect(),
            Self::MetadataCount { .. } | Self::OuterReference { .. } => Vec::new(),
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
            }),
            PhysicalNode::Expand {
                src,
                dst,
                edge_type,
                direction,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                mode,
                frontier_estimate,
                policy,
                version,
                ..
            } => json!({
                "node": "Expand",
                "src": src,
                "dst": dst,
                "edge_type": edge_type,
                "direction": direction_word(direction),
                "dst_type": dst_type,
                "min_hops": min_hops,
                "max_hops": max_hops,
                "edge_binding": edge_binding,
                "mode": mode,
                "alternatives": policy.alternatives(*mode),
                "frontier_estimate": frontier_estimate,
                "version": version,
            }),
            PhysicalNode::AntiJoin {
                outer_var,
                predicate,
                ..
            } => json!({
                "node": node.name(),
                "outer_var": outer_var,
                "predicate": predicate.to_string(),
            }),
            PhysicalNode::OuterReference { outer_var } => json!({
                "node": node.name(),
                "outer_var": outer_var,
            }),
            PhysicalNode::RankFuse { arms, k, limit, .. } => json!({
                "node": "RankFuse",
                "arms": arms
                    .iter()
                    .map(|arm| json!({ "binding": arm.binding, "kind": arm.kind }))
                    .collect::<Vec<Value>>(),
                "k": k.as_ref().map(ToString::to_string),
                "limit": limit,
            }),
            PhysicalNode::Projection { return_exprs, .. }
            | PhysicalNode::Aggregate { return_exprs, .. } => json!({
                "node": node.name(),
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
                "fetch": fetch,
                "tiebreak": crate::logical::tiebreak_text(tiebreak),
            }),
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

    /// The pipelines the push operators build for a diff or merge plan:
    /// walking from the root, a streaming node joins the current pipeline; a
    /// join with a build side ends it and starts a child pipeline for the
    /// build input; a scan is the source. Child pipelines come first.
    pub fn pipelines_json(&self) -> Value {
        let mut pipelines = Vec::new();
        let mut operators = Vec::new();
        let source = self.pipeline_of(self.root, &mut operators, &mut pipelines);
        operators.reverse();
        pipelines.push(json!({
            "source": source,
            "operators": operators,
            "sink": "consumer",
        }));
        Value::Array(pipelines)
    }

    /// One input of a two-sided streaming node: its source and the operators
    /// between that source and the node, in run order.
    fn side_json(&self, id: NodeId, pipelines: &mut Vec<Value>) -> Value {
        let mut operators = Vec::new();
        let source = self.pipeline_of(id, &mut operators, pipelines);
        operators.reverse();
        json!({ "source": source, "operators": operators })
    }

    fn pipeline_of(
        &self,
        id: NodeId,
        operators: &mut Vec<String>,
        pipelines: &mut Vec<Value>,
    ) -> Value {
        let Some(node) = self.node(id) else {
            return json!("tombstone");
        };
        match node {
            PhysicalNode::MetadataCount { spec, return_exprs } => {
                metadata_count_json(spec, return_exprs)
            }
            PhysicalNode::HashJoin { probe, build, .. } => {
                let mut build_operators = Vec::new();
                let build_source = self.pipeline_of(*build, &mut build_operators, pipelines);
                build_operators.reverse();
                pipelines.push(json!({
                    "source": build_source,
                    "operators": build_operators,
                    "sink": "HashJoin(build)",
                }));
                operators.push("HashJoin(probe)".to_string());
                self.pipeline_of(*probe, operators, pipelines)
            }
            PhysicalNode::Scan {
                source: ScanInput::Dependent { input, .. },
                ..
            } => {
                operators.push("Scan(input ids)".to_string());
                self.pipeline_of(*input, operators, pipelines)
            }
            PhysicalNode::Scan {
                source: ScanInput::Table,
                spec,
                ordered,
                ..
            } => json!({
                "node": "Scan",
                "side": spec.side,
                "ordered": ordered,
            }),
            PhysicalNode::OuterReference { outer_var } => json!({
                "node": "OuterReference",
                "outer_var": outer_var,
            }),
            PhysicalNode::SortMergeJoin {
                left,
                right,
                build: true,
                ..
            } => {
                let mut build_operators = Vec::new();
                let build_source = self.pipeline_of(*right, &mut build_operators, pipelines);
                build_operators.reverse();
                pipelines.push(json!({
                    "source": build_source,
                    "operators": build_operators,
                    "sink": "SortMergeJoin(build)",
                }));
                operators.push("SortMergeJoin(probe)".to_string());
                self.pipeline_of(*left, operators, pipelines)
            }
            PhysicalNode::SortMergeJoin {
                left,
                right,
                build: false,
                ..
            } => json!({
                "node": "SortMergeJoin(streaming)",
                "left": self.side_json(*left, pipelines),
                "right": self.side_json(*right, pipelines),
            }),
            PhysicalNode::CrossJoin { left, right } => json!({
                "node": "CrossJoin",
                "left": self.side_json(*left, pipelines),
                "right": self.side_json(*right, pipelines),
            }),
            PhysicalNode::AntiJoin { input, inner, .. } => {
                let mut inner_operators = Vec::new();
                let inner_source = self.pipeline_of(*inner, &mut inner_operators, pipelines);
                inner_operators.reverse();
                pipelines.push(json!({
                    "source": inner_source,
                    "operators": inner_operators,
                    "sink": "AntiJoin(inner)",
                }));
                operators.push("AntiJoin".to_string());
                self.pipeline_of(*input, operators, pipelines)
            }
            PhysicalNode::HydrateByAddress { input, side } => {
                operators.push(format!("HydrateByAddress({})", side.name()));
                self.pipeline_of(*input, operators, pipelines)
            }
            PhysicalNode::RankFuse { arms, .. } => {
                let [primary, secondary] = arms;
                let mut secondary_operators = Vec::new();
                let secondary_source =
                    self.pipeline_of(secondary.input, &mut secondary_operators, pipelines);
                secondary_operators.reverse();
                pipelines.push(json!({
                    "source": secondary_source,
                    "operators": secondary_operators,
                    "sink": "RankFuse(secondary)",
                }));
                operators.push("RankFuse".to_string());
                self.pipeline_of(primary.input, operators, pipelines)
            }
            PhysicalNode::RowCompare { input }
            | PhysicalNode::ClassifyThreeWay { input }
            | PhysicalNode::Page { input, .. }
            | PhysicalNode::Limit { input, .. }
            | PhysicalNode::Filter { input, .. }
            | PhysicalNode::Expand { input, .. }
            | PhysicalNode::Projection { input, .. }
            | PhysicalNode::Aggregate { input, .. }
            | PhysicalNode::Sort { input, .. } => {
                operators.push(node.name().to_string());
                self.pipeline_of(*input, operators, pipelines)
            }
        }
    }
}
