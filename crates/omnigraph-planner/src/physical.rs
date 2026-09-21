use arrow_schema::SchemaRef;
use omnigraph_compiler::ir::{IRFilter, IROrdering, IRProjection};
use omnigraph_compiler::types::Direction;
use serde::Serialize;
use serde_json::{Value, json};

use crate::cost::{AccessPath, ExpandCostInputs, ExpandMode};
use crate::logical::{
    KeyJoinKind, ScanSpec, direction_word, filters_json, metadata_count_json, ordering_text,
    scan_json,
};
use crate::source::SideId;

/// The index of a node in a [`PhysicalPlan`].
pub type NodeId = usize;

/// A table scan or an input-restricted scan with its required access path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanInput {
    Table,
    Dependent { input: NodeId, access: AccessPath },
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Estimate {
    Known(u64),
    Unknown,
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
    /// One Lance scan of a side. `ordered` sorts it by id under the bounded
    /// ordered-scan envelope; `keys_only` projects the id with `_rowid` and
    /// `_rowaddr`, otherwise every column rides with Blob descriptors.
    Scan {
        source: ScanInput,
        spec: Box<ScanSpec>,
        ordered: bool,
        keys_only: bool,
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
    /// The in-memory arm of a GQ filter: the ones the placement pass left
    /// where the query wrote them.
    Filter {
        input: NodeId,
        filters: Vec<IRFilter>,
    },
    /// A traversal with the mode the cost model chose (or the session pinned),
    /// the row-count estimate it was chosen for and the inputs the engine's
    /// mid-flight re-decision reads (`None` without edge statistics).
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
        cost: Option<ExpandCostInputs>,
    },
    AntiJoin {
        input: NodeId,
        inner: NodeId,
        outer_var: String,
    },
    /// The enclosing rows, the leaf of an `AntiJoin` inner tree.
    OuterReference {
        outer_var: String,
    },
    Nearest {
        input: NodeId,
        binding: String,
        property: String,
        k: Option<u64>,
    },
    TextSearch {
        input: NodeId,
        binding: String,
        property: String,
    },
    RankFuse {
        input: NodeId,
        targets: Vec<String>,
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
    },
}

impl PhysicalNode {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Scan { .. } => "Scan",
            Self::MetadataCount { .. } => "MetadataCount",
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
            Self::Nearest { .. } => "Nearest",
            Self::TextSearch { .. } => "TextSearch",
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
            | Self::Nearest { input, .. }
            | Self::TextSearch { input, .. }
            | Self::RankFuse { input, .. }
            | Self::Projection { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. } => vec![*input],
            Self::SortMergeJoin { left, right, .. } | Self::CrossJoin { left, right } => {
                vec![*left, *right]
            }
            Self::AntiJoin { input, inner, .. } => vec![*input, *inner],
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
            | Self::Nearest { input, .. }
            | Self::TextSearch { input, .. }
            | Self::RankFuse { input, .. }
            | Self::Projection { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. } => vec![input],
            Self::SortMergeJoin { left, right, .. } | Self::CrossJoin { left, right } => {
                vec![left, right]
            }
            Self::AntiJoin { input, inner, .. } => vec![input, inner],
            Self::Scan { source, .. } => source.input_mut().into_iter().collect(),
            Self::MetadataCount { .. } | Self::OuterReference { .. } => Vec::new(),
        }
    }
}

/// Arena of physical nodes with their declared properties.
#[derive(Debug, Clone, Default)]
pub struct PhysicalPlan {
    slots: Vec<Option<PhysicalNode>>,
    properties: Vec<Option<Properties>>,
    root: NodeId,
}

impl PhysicalPlan {
    pub fn new() -> Self {
        Self::default()
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
            return json!({ "node": "tombstone" });
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
            } => {
                let mut value = scan_json("Scan", spec);
                if let ScanInput::Dependent { access, .. } = source {
                    value["id_restriction"] = json!("input");
                    value["access"] = json!(access);
                }
                value["ordered"] = json!(ordered);
                value["keys_only"] = json!(keys_only);
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
                "frontier_estimate": frontier_estimate,
            }),
            PhysicalNode::AntiJoin { outer_var, .. }
            | PhysicalNode::OuterReference { outer_var } => json!({
                "node": node.name(),
                "outer_var": outer_var,
            }),
            PhysicalNode::Nearest {
                binding,
                property,
                k,
                ..
            } => json!({
                "node": "Nearest",
                "binding": binding,
                "property": property,
                "k": k,
            }),
            PhysicalNode::TextSearch {
                binding, property, ..
            } => json!({
                "node": "TextSearch",
                "binding": binding,
                "property": property,
            }),
            PhysicalNode::RankFuse { targets, .. } => json!({
                "node": "RankFuse",
                "targets": targets,
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
                order_by, fetch, ..
            } => json!({
                "node": "Sort",
                "keys": order_by.iter().map(ordering_text).collect::<Vec<String>>(),
                "fetch": fetch,
            }),
            other => json!({ "node": other.name() }),
        };
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
            PhysicalNode::Scan {
                source:
                    ScanInput::Dependent {
                        input,
                        access: AccessPath::HashJoin,
                    },
                spec,
                ..
            } => {
                pipelines.push(json!({
                    "source": { "node": "Scan", "side": spec.side },
                    "operators": [],
                    "sink": "Scan(hash join build)",
                }));
                operators.push("Scan(hash join probe)".to_string());
                self.pipeline_of(*input, operators, pipelines)
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
            PhysicalNode::RowCompare { input }
            | PhysicalNode::ClassifyThreeWay { input }
            | PhysicalNode::Page { input, .. }
            | PhysicalNode::Limit { input, .. }
            | PhysicalNode::Filter { input, .. }
            | PhysicalNode::Expand { input, .. }
            | PhysicalNode::Nearest { input, .. }
            | PhysicalNode::TextSearch { input, .. }
            | PhysicalNode::RankFuse { input, .. }
            | PhysicalNode::Projection { input, .. }
            | PhysicalNode::Aggregate { input, .. }
            | PhysicalNode::Sort { input, .. } => {
                operators.push(node.name().to_string());
                self.pipeline_of(*input, operators, pipelines)
            }
        }
    }
}
