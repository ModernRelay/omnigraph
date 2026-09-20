use std::fmt;
use std::hash::{Hash, Hasher};

use arrow_schema::SchemaRef;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::ir::{IRFilter, IROrdering, IRProjection};
use omnigraph_compiler::types::Direction;
use serde::Serialize;
use serde_json::{Value, json};

use crate::operation::TableRef;
use crate::source::SideId;

/// The index of a node in a [`LogicalPlan`], distinct from a physical node id.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct LogicalId(usize);

impl fmt::Display for LogicalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Bumped when a node's meaning changes; part of the structural hash.
pub const LOGICAL_PLAN_VERSION: u32 = 1;

/// The logical name of the id column; each scan binds it to its own
/// spelling through its [`SystemColumns`].
pub const LOGICAL_ID: &str = "id";

/// Join categories in a plan census, including keyless cross joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum JoinKind {
    FullOuter,
    LeftOuter,
    /// Two `match` bindings with no edge between them: every pair.
    Cross,
}

impl JoinKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::FullOuter => "FullOuter",
            Self::LeftOuter => "LeftOuter",
            Self::Cross => "Cross",
        }
    }
}

/// A keyed outer join; cross joins carry no key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum KeyJoinKind {
    FullOuter,
    LeftOuter,
}

impl From<KeyJoinKind> for JoinKind {
    fn from(kind: KeyJoinKind) -> Self {
        match kind {
            KeyJoinKind::FullOuter => Self::FullOuter,
            KeyJoinKind::LeftOuter => Self::LeftOuter,
        }
    }
}

/// 64-bit FNV-1a, the fixed algorithm behind `structural_hash`.
struct Fnv1a(u64);

impl Default for Fnv1a {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv1a {
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    /// A word and its length, so two adjacent words never hash as one.
    fn write_word(&mut self, word: &str) {
        self.write(&(word.len() as u64).to_le_bytes());
        self.write(word.as_bytes());
    }
}

/// A structured scan predicate. The engine lowers it to a DataFusion
/// expression inside its operators; the planner never builds one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Predicate {
    IdAfter {
        id: String,
    },
    VersionWindow {
        from: u64,
        to: u64,
    },
    And {
        left: Box<Predicate>,
        right: Box<Predicate>,
    },
    /// One GQ filter as the query wrote it: the bound values it reads, for the
    /// projection pass, its GQ text, for explain, and the `IRFilter` itself,
    /// which the engine's scan lowers.
    Gq {
        reads: Vec<ColumnRef>,
        text: String,
        #[serde(skip)]
        filter: GqFilter,
    },
}

/// The `IRFilter` behind a `Predicate::Gq`; equal and hashed by its `Debug`
/// form, since the IR derives neither.
#[derive(Debug, Clone)]
pub struct GqFilter(pub IRFilter);

impl PartialEq for GqFilter {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self.0) == format!("{:?}", other.0)
    }
}

impl Eq for GqFilter {}

impl Hash for GqFilter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!("{:?}", self.0).hash(state);
    }
}

impl Predicate {
    /// Every GQ filter in the conjunction, in the order the query wrote them.
    pub fn gq_filters(&self) -> Vec<IRFilter> {
        let mut out = Vec::new();
        self.collect_gq_filters(&mut out);
        out
    }

    fn collect_gq_filters(&self, out: &mut Vec<IRFilter>) {
        match self {
            Self::Gq { filter, .. } => out.push(filter.0.clone()),
            Self::And { left, right } => {
                left.collect_gq_filters(out);
                right.collect_gq_filters(out);
            }
            Self::IdAfter { .. } | Self::VersionWindow { .. } => {}
        }
    }

    /// The one GQ filter this predicate is, when it is no conjunction.
    pub fn single_gq(&self) -> Option<&IRFilter> {
        match self {
            Self::Gq { filter, .. } => Some(&filter.0),
            _ => None,
        }
    }

    pub fn and(self, other: Predicate) -> Predicate {
        Predicate::And {
            left: Box::new(self),
            right: Box::new(other),
        }
    }
}

/// One projected value a GQ query reads through a binding: `$v.prop` names a
/// property, a bare `$v` (`property: None`) the whole projected node object,
/// and `$v.@id` the identity alone (`count($v)`; `@id` is the object member
/// name of the identity in RFC 0040). Rendered `v.prop` / `v`; GQ
/// identifiers carry no `.`, so the first `.` splits exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ColumnRef {
    pub binding: String,
    pub property: Option<String>,
}

pub const IDENTITY_MEMBER: &str = "@id";

impl ColumnRef {
    pub fn entity(binding: &str) -> Self {
        Self {
            binding: binding.to_string(),
            property: None,
        }
    }

    pub fn property(binding: &str, property: &str) -> Self {
        Self {
            binding: binding.to_string(),
            property: Some(property.to_string()),
        }
    }

    pub fn parse(rendered: &str) -> Self {
        match rendered.split_once('.') {
            Some((binding, property)) => Self::property(binding, property),
            None => Self::entity(rendered),
        }
    }
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.property {
            Some(property) => write!(f, "{}.{}", self.binding, property),
            None => f.write_str(&self.binding),
        }
    }
}

/// Every logical node kind, declared in the order the census sorts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum LogicalKind {
    Aggregate,
    AntiJoin,
    Expand,
    Filter,
    Join,
    Limit,
    MergeClassify,
    MetadataCount,
    Nearest,
    OuterReference,
    Projection,
    RankFuse,
    RowDiff,
    Sort,
    TableScan,
    TextSearch,
}

impl LogicalKind {
    pub const ALL: [LogicalKind; 16] = [
        LogicalKind::Aggregate,
        LogicalKind::AntiJoin,
        LogicalKind::Expand,
        LogicalKind::Filter,
        LogicalKind::Join,
        LogicalKind::Limit,
        LogicalKind::MergeClassify,
        LogicalKind::MetadataCount,
        LogicalKind::Nearest,
        LogicalKind::OuterReference,
        LogicalKind::Projection,
        LogicalKind::RankFuse,
        LogicalKind::RowDiff,
        LogicalKind::Sort,
        LogicalKind::TableScan,
        LogicalKind::TextSearch,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Aggregate => "Aggregate",
            Self::AntiJoin => "AntiJoin",
            Self::Expand => "Expand",
            Self::Filter => "Filter",
            Self::Join => "Join",
            Self::Limit => "Limit",
            Self::MergeClassify => "MergeClassify",
            Self::MetadataCount => "MetadataCount",
            Self::Nearest => "Nearest",
            Self::OuterReference => "OuterReference",
            Self::Projection => "Projection",
            Self::RankFuse => "RankFuse",
            Self::RowDiff => "RowDiff",
            Self::Sort => "Sort",
            Self::TableScan => "TableScan",
            Self::TextSearch => "TextSearch",
        }
    }
}

/// Rows of one table at one pinned version, optionally scoped to a fragment
/// set, with a pushed structured filter and a pushed projection.
#[derive(Debug, Clone)]
pub struct ScanSpec {
    pub side: SideId,
    pub table: TableRef,
    /// The pinned dataset version, absent when no dataset belongs to this image.
    pub version: Option<u64>,
    pub columns: SystemColumns,
    pub fragments: Option<Vec<u64>>,
    pub projection: Option<Vec<String>>,
    pub filter: Option<Predicate>,
    /// The GQ `match` variable this scan binds; `None` on a diff or merge side.
    pub binding: Option<String>,
}

#[derive(Debug, Clone)]
pub enum LogicalNode {
    /// Read one pinned table, optionally restricted by input identities.
    /// Dependent reads preserve surviving input rows, order and multiplicity.
    TableScan {
        input: Option<LogicalId>,
        spec: Box<ScanSpec>,
    },
    MetadataCount {
        spec: Box<ScanSpec>,
        return_exprs: Vec<IRProjection>,
    },
    Filter {
        input: LogicalId,
        predicate: Predicate,
    },
    /// The query's return expressions and their typed column demand.
    Projection {
        input: LogicalId,
        reads: Vec<ColumnRef>,
        return_exprs: Vec<IRProjection>,
    },
    /// `keys` for the passes; `order_by` and `fetch` for the engine's sort.
    Sort {
        input: LogicalId,
        keys: Vec<String>,
        order_by: Vec<IROrdering>,
        fetch: Option<usize>,
    },
    /// Required input ordering for a diff or change-feed plan.
    Ordered {
        input: LogicalId,
        keys: Vec<String>,
    },
    Limit {
        input: LogicalId,
        rows: usize,
    },
    Page {
        input: LogicalId,
        rows: usize,
        bytes: u64,
        resume: Option<String>,
    },
    Join {
        left: LogicalId,
        right: LogicalId,
        kind: KeyJoinKind,
        on: String,
    },
    CrossJoin {
        left: LogicalId,
        right: LogicalId,
    },
    RowDiff {
        input: LogicalId,
        address_short_circuit: bool,
    },
    MergeClassify {
        base: LogicalId,
        source: LogicalId,
        target: LogicalId,
    },
    /// `$src edge $dst`: topology and bound edge rows only. Destination
    /// properties and filters belong to the following destination scan.
    Expand {
        input: LogicalId,
        src: String,
        dst: String,
        edge_type: String,
        direction: Direction,
        dst_type: String,
        min_hops: u32,
        max_hops: Option<u32>,
        edge_binding: Option<String>,
    },
    /// `not { … }`: input rows with no match in the inner tree, whose leaf
    /// is an `OuterReference` to those same rows.
    AntiJoin {
        input: LogicalId,
        inner: LogicalId,
        outer_var: String,
    },
    /// The leaf of a `not { … }` inner tree: the enclosing pipeline's rows,
    /// correlated on `outer_var`. A node of its own, not the outer node, so
    /// the inner tree stays a tree and no pass folds its filter outward.
    OuterReference {
        outer_var: String,
    },
    /// A leading `order { nearest($v.prop, q) }`: Lance ranks the binding's
    /// scan on the vector column and appends `_distance`.
    Nearest {
        input: LogicalId,
        binding: String,
        property: String,
        k: Option<u64>,
        reads: Vec<ColumnRef>,
    },
    /// A leading `order { bm25($v.prop, q) }`: the full-text ranking, `_score`.
    TextSearch {
        input: LogicalId,
        binding: String,
        property: String,
        reads: Vec<ColumnRef>,
    },
    /// A leading `order { rrf(a, b) }`: both arms run the input tree, ranked
    /// on their targets, and fuse.
    RankFuse {
        input: LogicalId,
        targets: Vec<String>,
        reads: Vec<ColumnRef>,
    },
    /// A `return` with an aggregate: the group keys and aggregate arguments
    /// it reads; `count($v)` reads the identity alone.
    Aggregate {
        input: LogicalId,
        reads: Vec<ColumnRef>,
        return_exprs: Vec<IRProjection>,
    },
}

impl LogicalNode {
    pub fn kind(&self) -> LogicalKind {
        match self {
            Self::TableScan { .. } => LogicalKind::TableScan,
            Self::MetadataCount { .. } => LogicalKind::MetadataCount,
            Self::Filter { .. } => LogicalKind::Filter,
            Self::Projection { .. } => LogicalKind::Projection,
            Self::Sort { .. } | Self::Ordered { .. } => LogicalKind::Sort,
            Self::Limit { .. } | Self::Page { .. } => LogicalKind::Limit,
            Self::Join { .. } | Self::CrossJoin { .. } => LogicalKind::Join,
            Self::RowDiff { .. } => LogicalKind::RowDiff,
            Self::MergeClassify { .. } => LogicalKind::MergeClassify,
            Self::Expand { .. } => LogicalKind::Expand,
            Self::AntiJoin { .. } => LogicalKind::AntiJoin,
            Self::OuterReference { .. } => LogicalKind::OuterReference,
            Self::Nearest { .. } => LogicalKind::Nearest,
            Self::TextSearch { .. } => LogicalKind::TextSearch,
            Self::RankFuse { .. } => LogicalKind::RankFuse,
            Self::Aggregate { .. } => LogicalKind::Aggregate,
        }
    }

    pub fn inputs(&self) -> Vec<LogicalId> {
        match self {
            Self::Filter { input, .. }
            | Self::Projection { input, .. }
            | Self::Sort { input, .. }
            | Self::Ordered { input, .. }
            | Self::Limit { input, .. }
            | Self::Page { input, .. }
            | Self::RowDiff { input, .. }
            | Self::Expand { input, .. }
            | Self::Nearest { input, .. }
            | Self::TextSearch { input, .. }
            | Self::RankFuse { input, .. }
            | Self::Aggregate { input, .. } => vec![*input],
            Self::Join { left, right, .. } | Self::CrossJoin { left, right } => vec![*left, *right],
            Self::AntiJoin { input, inner, .. } => vec![*input, *inner],
            Self::MergeClassify {
                base,
                source,
                target,
            } => vec![*base, *source, *target],
            Self::TableScan { input, .. } => input.iter().copied().collect(),
            Self::MetadataCount { .. } | Self::OuterReference { .. } => Vec::new(),
        }
    }

    fn inputs_mut(&mut self) -> Vec<&mut LogicalId> {
        match self {
            Self::Filter { input, .. }
            | Self::Projection { input, .. }
            | Self::Sort { input, .. }
            | Self::Ordered { input, .. }
            | Self::Limit { input, .. }
            | Self::Page { input, .. }
            | Self::RowDiff { input, .. }
            | Self::Expand { input, .. }
            | Self::Nearest { input, .. }
            | Self::TextSearch { input, .. }
            | Self::RankFuse { input, .. }
            | Self::Aggregate { input, .. } => vec![input],
            Self::Join { left, right, .. } | Self::CrossJoin { left, right } => vec![left, right],
            Self::AntiJoin { input, inner, .. } => vec![input, inner],
            Self::MergeClassify {
                base,
                source,
                target,
            } => vec![base, source, target],
            Self::TableScan { input, .. } => input.iter_mut().collect(),
            Self::MetadataCount { .. } | Self::OuterReference { .. } => Vec::new(),
        }
    }
}

/// The registry key: the sorted multiset of a plan's node kinds plus the kind
/// of every `Join`, computed once on the resolved plan before any rewrite.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Census {
    pub kinds: Vec<LogicalKind>,
    pub joins: Vec<JoinKind>,
}

impl fmt::Display for Census {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        let mut joins = self.joins.iter();
        for (index, kind) in self.kinds.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            match kind {
                LogicalKind::Join => match joins.next() {
                    Some(join) => write!(f, "Join({join:?})")?,
                    None => f.write_str(kind.name())?,
                },
                _ => f.write_str(kind.name())?,
            }
        }
        f.write_str("]")
    }
}

/// Arena of logical nodes referenced by index. A removed node leaves a
/// tombstone so indices stay valid.
#[derive(Debug, Clone, Default)]
pub struct LogicalPlan {
    slots: Vec<Option<LogicalNode>>,
    schemas: Vec<Option<SchemaRef>>,
    root: LogicalId,
}

impl LogicalPlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, node: LogicalNode, schema: SchemaRef) -> LogicalId {
        self.slots.push(Some(node));
        self.schemas.push(Some(schema));
        LogicalId(self.slots.len() - 1)
    }

    pub fn set_root(&mut self, root: LogicalId) {
        self.root = root;
    }

    pub fn root(&self) -> LogicalId {
        self.root
    }

    pub fn node(&self, id: LogicalId) -> Option<&LogicalNode> {
        self.slots.get(id.0).and_then(Option::as_ref)
    }

    pub fn node_mut(&mut self, id: LogicalId) -> Option<&mut LogicalNode> {
        self.slots.get_mut(id.0).and_then(Option::as_mut)
    }

    pub fn schema(&self, id: LogicalId) -> Option<&SchemaRef> {
        self.schemas.get(id.0).and_then(Option::as_ref)
    }

    /// Point every reference to `from` at `to`, then tombstone `from`.
    pub fn splice_out(&mut self, from: LogicalId, to: LogicalId) {
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
        if let Some(slot) = self.slots.get_mut(from.0) {
            *slot = None;
        }
        if let Some(schema) = self.schemas.get_mut(from.0) {
            *schema = None;
        }
    }

    pub fn live(&self) -> impl Iterator<Item = (LogicalId, &LogicalNode)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(id, slot)| slot.as_ref().map(|node| (LogicalId(id), node)))
    }

    /// The parent of `id`, when exactly one live node consumes it.
    pub fn parent_of(&self, id: LogicalId) -> Option<LogicalId> {
        let mut parents = self
            .live()
            .filter(|(_, node)| node.inputs().contains(&id))
            .map(|(parent, _)| parent);
        let first = parents.next()?;
        parents.next().is_none().then_some(first)
    }

    pub fn census(&self) -> Census {
        let mut kinds: Vec<LogicalKind> = self.live().map(|(_, node)| node.kind()).collect();
        kinds.sort();
        let mut joins: Vec<JoinKind> = self
            .live()
            .filter_map(|(_, node)| match node {
                LogicalNode::Join { kind, .. } => Some((*kind).into()),
                LogicalNode::CrossJoin { .. } => Some(JoinKind::Cross),
                _ => None,
            })
            .collect();
        joins.sort();
        Census { kinds, joins }
    }

    /// Hash of the tree's shape: the plan version, then per node its kind
    /// name, a `Join`'s kind and its input count. Explain serves it as
    /// `logical_hash`, so it is FNV-1a over those bytes: one value for one
    /// shape on every toolchain and pointer width.
    pub fn structural_hash(&self) -> u64 {
        let mut hasher = Fnv1a::default();
        hasher.write(&LOGICAL_PLAN_VERSION.to_le_bytes());
        self.hash_subtree(self.root, &mut hasher);
        hasher.0
    }

    fn hash_subtree(&self, id: LogicalId, hasher: &mut Fnv1a) {
        let Some(node) = self.node(id) else {
            hasher.write_word("tombstone");
            return;
        };
        hasher.write_word(node.kind().name());
        if let LogicalNode::Join { kind, .. } = node {
            hasher.write_word(JoinKind::from(*kind).name());
        } else if matches!(node, LogicalNode::CrossJoin { .. }) {
            hasher.write_word(JoinKind::Cross.name());
        }
        let inputs = node.inputs();
        hasher.write(&(inputs.len() as u64).to_le_bytes());
        for input in inputs {
            self.hash_subtree(input, hasher);
        }
    }

    pub fn to_json(&self) -> Value {
        self.node_json(self.root)
    }

    fn node_json(&self, id: LogicalId) -> Value {
        let Some(node) = self.node(id) else {
            return json!({ "node": "tombstone" });
        };
        let mut value = match node {
            LogicalNode::TableScan { input, spec } => {
                let mut value = scan_json("TableScan", spec);
                if input.is_some() {
                    value["id_restriction"] = json!("input");
                }
                value
            }
            LogicalNode::MetadataCount { spec, return_exprs } => {
                metadata_count_json(spec, return_exprs)
            }
            LogicalNode::Filter { predicate, .. } => json!({
                "node": "Filter",
                "predicate": predicate,
            }),
            LogicalNode::Projection { reads, .. } => json!({
                "node": "Projection",
                "columns": rendered(reads),
            }),
            LogicalNode::Sort { keys, fetch, .. } => json!({
                "node": "Sort",
                "keys": keys,
                "fetch": fetch,
            }),
            LogicalNode::Ordered { keys, .. } => json!({
                "node": "Sort", "keys": keys, "fetch": null,
            }),
            LogicalNode::Limit { rows, .. } => json!({
                "node": "Limit", "rows": rows, "bytes": 0, "resume": null,
            }),
            LogicalNode::Page {
                rows,
                bytes,
                resume,
                ..
            } => json!({
                "node": "Limit",
                "rows": rows,
                "bytes": bytes,
                "resume": resume,
            }),
            LogicalNode::Join { kind, on, .. } => json!({
                "node": "Join",
                "kind": kind,
                "on": on,
            }),
            LogicalNode::CrossJoin { .. } => json!({
                "node": "Join", "kind": "Cross", "on": LOGICAL_ID,
            }),
            LogicalNode::RowDiff {
                address_short_circuit,
                ..
            } => json!({
                "node": "RowDiff",
                "address_short_circuit": address_short_circuit,
            }),
            LogicalNode::MergeClassify { .. } => json!({ "node": "MergeClassify" }),
            LogicalNode::Expand {
                src,
                dst,
                edge_type,
                direction,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
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
            }),
            LogicalNode::AntiJoin { outer_var, .. } => json!({
                "node": "AntiJoin",
                "outer_var": outer_var,
            }),
            LogicalNode::OuterReference { outer_var } => json!({
                "node": "OuterReference",
                "outer_var": outer_var,
            }),
            LogicalNode::Nearest {
                binding,
                property,
                k,
                reads,
                ..
            } => json!({
                "node": "Nearest",
                "binding": binding,
                "property": property,
                "k": k,
                "reads": rendered(reads),
            }),
            LogicalNode::TextSearch {
                binding,
                property,
                reads,
                ..
            } => json!({
                "node": "TextSearch",
                "binding": binding,
                "property": property,
                "reads": rendered(reads),
            }),
            LogicalNode::RankFuse { targets, reads, .. } => json!({
                "node": "RankFuse",
                "targets": targets,
                "reads": rendered(reads),
            }),
            LogicalNode::Aggregate { reads, .. } => json!({
                "node": "Aggregate",
                "reads": rendered(reads),
            }),
        };
        let inputs: Vec<Value> = node
            .inputs()
            .into_iter()
            .map(|input| self.node_json(input))
            .collect();
        if !inputs.is_empty() {
            value["inputs"] = Value::Array(inputs);
        }
        value
    }
}

/// A query scan (one with a binding) prints what it reads, from which pinned
/// version and under what condition; a change-feed or merge scan prints its
/// side too, which the diff and merge documents are read by.
pub(crate) fn scan_json(name: &str, spec: &ScanSpec) -> Value {
    if let Some(binding) = &spec.binding {
        return json!({
            "node": name,
            "binding": binding,
            "table": spec.table.type_key,
            "version": spec.version,
            "projection": spec.projection,
            "filter": spec.filter,
        });
    }
    json!({
        "node": name,
        "side": spec.side,
        "table": spec.table.type_key,
        "version": spec.version,
        "id_column": spec.columns.id,
        "fragments": spec.fragments,
        "projection": spec.projection,
        "filter": spec.filter,
    })
}

pub(crate) fn filters_json(filters: &[IRFilter]) -> Vec<String> {
    filters.iter().map(ToString::to_string).collect()
}

/// One `order` key as GQ text with its direction: `$p.name desc`.
pub(crate) fn ordering_text(ordering: &IROrdering) -> String {
    let direction = if ordering.descending { "desc" } else { "asc" };
    format!("{} {direction}", ordering.expr)
}

/// The direction as the docs and the CLI spell it.
pub(crate) fn direction_word(direction: &Direction) -> &'static str {
    match direction {
        Direction::Out => "out",
        Direction::In => "in",
        Direction::Both => "both",
    }
}

fn rendered(reads: &[ColumnRef]) -> Vec<String> {
    reads.iter().map(ToString::to_string).collect()
}

pub(crate) fn metadata_count_json(spec: &ScanSpec, return_exprs: &[IRProjection]) -> Value {
    let mut value = scan_json("MetadataCount", spec);
    value["exprs"] = json!(
        return_exprs
            .iter()
            .map(|projection| json!({
                "expr": projection.expr.to_string(),
                "alias": projection.alias,
            }))
            .collect::<Vec<_>>()
    );
    value
}
