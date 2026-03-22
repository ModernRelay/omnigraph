use std::collections::HashMap;

use arrow_array::RecordBatch;

#[derive(Debug, Default, Clone)]
pub(crate) struct MutationDelta {
    pub(crate) node_changes: HashMap<String, NodeDelta>,
    pub(crate) edge_changes: HashMap<String, EdgeDelta>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct NodeDelta {
    pub(crate) inserts: Option<RecordBatch>,
    pub(crate) upserts: Option<RecordBatch>,
    pub(crate) delete_ids: Vec<u64>,
    pub(crate) before_for_updates: Option<RecordBatch>,
    pub(crate) before_for_deletes: Option<RecordBatch>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct EdgeDelta {
    pub(crate) inserts: Option<RecordBatch>,
    pub(crate) delete_ids: Vec<u64>,
    pub(crate) before_for_deletes: Option<RecordBatch>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct DatasetMutationPlan {
    pub(crate) node_replacements: HashMap<String, Option<RecordBatch>>,
    pub(crate) edge_replacements: HashMap<String, Option<RecordBatch>>,
    pub(crate) delta: MutationDelta,
    pub(crate) next_node_id: u64,
    pub(crate) next_edge_id: u64,
    pub(crate) op_summary: String,
}

impl DatasetMutationPlan {
    pub(crate) fn new(op_summary: impl Into<String>, next_node_id: u64, next_edge_id: u64) -> Self {
        Self {
            node_replacements: HashMap::new(),
            edge_replacements: HashMap::new(),
            delta: MutationDelta::default(),
            next_node_id,
            next_edge_id,
            op_summary: op_summary.into(),
        }
    }
}
