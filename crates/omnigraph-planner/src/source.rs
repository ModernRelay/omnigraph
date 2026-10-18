use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use arrow_schema::SchemaRef;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::ir::IRExpr;
use omnigraph_compiler::settings::Traversal;
use omnigraph_compiler::types::Direction;
use serde::{Deserialize, Serialize};

use crate::error::PlanError;
use crate::logical::{RuntimeInput, ScanAccess, ScanSpec};
use crate::operation::TableRef;
use crate::physical::{DatasetPin, GatePolicy};

/// The source's answer to `PlanSource::index_split`, awaited by the
/// finalization stage both query entry points share.
pub type IndexSplitFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ScanAccess, PlanError>> + Send + 'a>>;

/// Which pinned image a scan reads. `Parent` is the before side (`from`),
/// `Child` the after side (`to`); a three-way merge adds `Base`. A GQ query
/// plan has one `Binding` side per `match` variable, numbered in pipeline
/// order; the variable's name rides on the scan (`ScanSpec::binding`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideId {
    Base,
    Parent,
    Child,
    Binding(u16),
}

impl SideId {
    /// The prefix a side's columns carry through a routed plan.
    pub fn name(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Parent => "parent",
            Self::Child => "child",
            Self::Binding(_) => "binding",
        }
    }
}

/// One node type as a GQ query plan sees it: the pinned table image, its
/// system-column spelling, its Arrow schema, its key columns, and the columns
/// the projected node object carries (`NodeType::node_object_fields`: the
/// identity and every declared property except `Blob` and `Vector`).
#[derive(Debug, Clone)]
pub struct NodeTypeSpec {
    pub table: TableRef,
    /// The pinned dataset version, absent when the snapshot predates this type.
    pub version: Option<u64>,
    pub columns: SystemColumns,
    pub schema: SchemaRef,
    pub key: Vec<String>,
    pub object_columns: Vec<String>,
    /// The projected node members, with logical names such as `@id`.
    pub object_fields: arrow_schema::Fields,
    /// The table's manifest-resident row count (`entity_count`); `None` when
    /// the table is absent from the pinned snapshot.
    pub row_count: Option<u64>,
}

/// The environment variable behind `ExpandStatistics::max_frontier_cap`,
/// recorded by name in the plan's assumptions.
pub const EXPAND_INDEXED_MAX_FRONTIER_ENV: &str = "OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER";

/// The environment variable behind `ExpandStatistics::max_hops_cap`,
/// recorded by name in the plan's assumptions.
pub const EXPAND_INDEXED_MAX_HOPS_ENV: &str = "OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS";

/// The manifest-resident counts an `Expand` over one edge type in one
/// direction is costed with. `src_node_count` is the keyed endpoint's node
/// count for the direction (`Out` → `from_type`, `In` → `to_type`, `Both` →
/// `from_type`), `dst_node_count` the opposite endpoint's; the caps are the
/// engine's resolved `OMNIGRAPH_EXPAND_INDEXED_MAX_*` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpandStatistics {
    pub edge_count: u64,
    pub src_node_count: u64,
    pub dst_node_count: u64,
    pub max_frontier_cap: u64,
    pub max_hops_cap: u32,
}

/// One fragment's manifest-resident row count and data-file bytes; `None`
/// when the manifest carries no such number for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FragmentStat {
    pub id: u64,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
}

/// One logical index at the table's pinned version, across all its segments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFact {
    pub name: String,
    pub column: String,
    #[serde(flatten)]
    pub kind: IndexKind,
    pub coverage: Option<FragmentCoverage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IndexKind {
    Btree { usable: bool },
    Inverted,
    Vector,
    Unknown,
}

/// Coverage counts current fragments, not rows; absence means unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentCoverage {
    pub covered: u64,
    pub total: u64,
}

impl IndexFact {
    /// Whether the index may narrow a scan: its coverage is unknown, or it
    /// covers at least one current fragment. An index that covers none, such
    /// as an untrained full-text segment that only declares an analyzer,
    /// cannot.
    pub fn may_narrow(&self) -> bool {
        self.coverage.is_none_or(|coverage| coverage.covered > 0)
    }

    pub fn fully_covers_btree(&self, column: &str) -> bool {
        self.column == column
            && matches!(self.kind, IndexKind::Btree { usable: true })
            && self
                .coverage
                .is_some_and(|coverage| coverage.total > 0 && coverage.covered == coverage.total)
    }
}

/// The proof the engine's candidate path captured for one interval: the
/// child version is the parent's immediate successor under one
/// row-set-preserving transaction, and these are the fragments it touched.
/// The planner reads it and never acquires one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdjacencyProof {
    pub child_fragments: Vec<u64>,
    pub parent_fragments: Vec<u64>,
    /// `(parent_version, child_version]`, the `_row_last_updated_at_version`
    /// window that drops rows a fragment rewrite carried along; `None` for a
    /// commit whose rows carry no linear stamp (a detached commit), where the
    /// child scan reads its fragments whole and the join drops unchanged rows.
    pub version_window: Option<(u64, u64)>,
}

/// The planner's whole view of the store. Implemented by the engine over its
/// pinned snapshot; implemented in memory by the planner's own tests.
pub trait PlanSource {
    /// Render a key value only when its identity agrees with supported persisted rows.
    fn canonical_key_id(
        &self,
        _type_key: &str,
        _value: &IRExpr,
    ) -> Result<Option<String>, PlanError> {
        Ok(None)
    }

    fn scan_runtime_input(&self, _scan: &ScanSpec) -> Option<RuntimeInput> {
        None
    }

    fn index_split<'a>(&'a self, _scan: &'a ScanSpec) -> IndexSplitFuture<'a> {
        Box::pin(async { Ok(ScanAccess::Sequential) })
    }

    fn index_facts(&self, _dataset_key: &str) -> Vec<IndexFact> {
        Vec::new()
    }

    /// Whether `property` holds at most one row per value in the table
    /// `type_key` names, beyond the `@key` columns `NodeTypeSpec::key` lists.
    fn is_unique_property(&self, _type_key: &str, _property: &str) -> bool {
        false
    }

    /// The element count of the list the parameter `name` is bound to; `None`
    /// for any other value and for a source that holds no parameters.
    fn list_parameter_len(&self, _name: &str) -> Option<usize> {
        None
    }

    /// The manifest-resident data-file bytes of the whole table `type_key`
    /// names; `None` when the source holds no such number.
    fn table_data_bytes(&self, _type_key: &str) -> Option<u64> {
        None
    }

    /// Compressed bytes for one field, including its nested fields, in the
    /// pinned dataset. Missing statistics remain unknown.
    fn column_data_bytes(&self, _type_key: &str, _column: &str) -> Option<u64> {
        None
    }

    /// The bytes of the memory pool one query runs under; a hash join's build
    /// side may take one part in `HASH_JOIN_POOL_DIVISOR` of it. The default
    /// admits no build side.
    fn query_memory_pool_bytes(&self) -> u64 {
        0
    }

    fn schema(&self, side: SideId) -> Result<SchemaRef, PlanError>;
    fn fragments(&self, side: SideId) -> Vec<FragmentStat>;
    fn adjacency_proof(&self) -> Option<&AdjacencyProof>;
    /// The node type a GQ `match` binding scans, by its bare type name. A
    /// source that serves no queries keeps the default.
    fn node_type(&self, type_name: &str) -> Result<NodeTypeSpec, PlanError> {
        Err(PlanError::Unresolved {
            detail: format!("this plan source serves no node type (asked for `{type_name}`)"),
        })
    }

    /// Whether the source's scanner can evaluate this GQ conjunct itself, so
    /// the placement pass may move it out of the in-memory arm. The engine
    /// answers from its lowering; a test source pushes everything.
    fn filter_pushable(&self, _filter: &IRExpr) -> bool {
        true
    }

    /// The counts the Expand cost model reads for one edge type in one
    /// direction; `None` when the source holds none, and the planner then
    /// records `Csr` with no estimate.
    fn expand_statistics(
        &self,
        _edge_type: &str,
        _direction: Direction,
    ) -> Option<ExpandStatistics> {
        None
    }

    /// The pinned dataset of `edge:<edge_type>`, recorded in the plan so a
    /// replay is refused unless the snapshot holds it; `None` for no such table.
    fn edge_dataset(&self, edge_type: &str) -> Option<DatasetPin>;

    /// The session's harness-only traversal pin: `Indexed` or `Csr` forces
    /// every Expand's mode, `Auto` leaves it to the cost model.
    fn traversal(&self) -> Traversal {
        Traversal::Auto
    }

    /// The finite query-wide traversal allowance; present only for statements
    /// using edge selections, including selections inside correlated blocks.
    fn traversal_work_limit(&self) -> Option<u64> {
        None
    }

    /// The session's `ann_nprobes` setting, the probe cap a `nearest` scan
    /// carries; `None` is no cap. The plan records the value it read.
    fn ann_nprobes(&self) -> Option<usize> {
        None
    }

    /// How the prefilter gates decide: the `rrf_plan` setting and the
    /// admission thresholds. The plan carries it in its assumptions.
    fn gate_policy(&self) -> GatePolicy {
        GatePolicy::default()
    }

    /// The directory each finished explain document is written into, one
    /// `<operation>_<logical_hash>.explain.json` per planned operation;
    /// `None` disables the write.
    fn explain_dump_dir(&self) -> Option<std::path::PathBuf> {
        None
    }

    /// How far the full-text index of `property` covers the pinned table
    /// `type_key` names. The default claims no coverage, the answer under
    /// which every plan stays correct.
    fn full_text_coverage(&self, _type_key: &str, _property: &str) -> FullTextCoverage {
        FullTextCoverage::Absent
    }

    /// The fingerprint of the catalogue table `type_key` names at this source:
    /// its rows and schema as the running binary derives them, so a plan records
    /// what it read and a replay checks it. `None` for a source with no catalogue.
    fn catalogue_fingerprint(&self, _type_key: &str) -> Option<String> {
        None
    }
}

/// How far a property's full-text index covers the pinned dataset's
/// fragments. Only full coverage makes a BM25 score independent of a filter
/// applied before scoring: Lance scores the unindexed fragments flat, from
/// statistics over the rows the filter admits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FullTextCoverage {
    /// Every fragment of the pinned version is in a full-text segment.
    Full,
    /// A full-text index exists; some fragment is not in it.
    Partial,
    /// The property has no full-text index.
    Absent,
}

impl FullTextCoverage {
    /// The coverage the index facts of one table establish for `column`:
    /// `Absent` with no index on it, `Full` only when it has a full-text
    /// index and every full-text index on it has known coverage of every
    /// current fragment, `Partial` otherwise. An index of unknown kind or
    /// unknown coverage on the column never counts as full.
    pub fn of_facts(facts: &[IndexFact], column: &str) -> Self {
        let on_column: Vec<&IndexFact> =
            facts.iter().filter(|fact| fact.column == column).collect();
        let full_text = on_column
            .iter()
            .any(|fact| fact.kind == IndexKind::Inverted);
        let unknown = on_column.iter().any(|fact| fact.kind == IndexKind::Unknown);
        if !full_text {
            return if unknown { Self::Partial } else { Self::Absent };
        }
        let full = !unknown
            && on_column
                .iter()
                .filter(|fact| fact.kind == IndexKind::Inverted)
                .all(|fact| {
                    fact.coverage
                        .is_some_and(|coverage| coverage.covered == coverage.total)
                });
        if full { Self::Full } else { Self::Partial }
    }
}

/// An in-memory [`PlanSource`] for planner tests and registry fixtures.
#[derive(Debug, Clone, Default)]
pub struct MemorySource {
    index_facts: HashMap<String, Vec<IndexFact>>,
    schemas: HashMap<SideId, SchemaRef>,
    fragments: HashMap<SideId, Vec<FragmentStat>>,
    proof: Option<AdjacencyProof>,
    node_types: HashMap<String, NodeTypeSpec>,
    expand_statistics: Vec<(String, Direction, ExpandStatistics)>,
    edge_datasets: HashMap<String, DatasetPin>,
    traversal: Option<Traversal>,
    traversal_work_limit: Option<u64>,
    ann_nprobes: Option<usize>,
    table_data_bytes: HashMap<String, u64>,
    column_data_bytes: HashMap<String, HashMap<String, u64>>,
    query_memory_pool_bytes: u64,
    full_text: HashMap<(String, String), FullTextCoverage>,
    /// When set, a key's canonical id renders as `id:<value>`, a parameter
    /// resolved from these values.
    canonical_keys: Option<omnigraph_compiler::ir::ParamMap>,
    explain_dump_dir: Option<std::path::PathBuf>,
    catalogue_fingerprints: HashMap<String, String>,
}

impl MemorySource {
    /// A catalogue node type: `rows` rows of the built-in type `name`
    /// (`@Operator` and its kin) with the given fingerprint, as the engine
    /// would serve it under the current identity spelling.
    pub fn with_catalogue_type(mut self, name: &str, rows: u64, fingerprint: &str) -> Self {
        let ty = omnigraph_compiler::catalogue_type(name)
            .unwrap_or_else(|| panic!("`{name}` is not a catalogue type"));
        let columns = omnigraph_compiler::SYSTEM_COLUMNS_V3;
        let node = ty.node_type(columns);
        let type_key = format!("node:{name}");
        self.catalogue_fingerprints
            .insert(type_key.clone(), fingerprint.to_string());
        self.node_types.insert(
            name.to_string(),
            NodeTypeSpec {
                table: TableRef::catalogue(type_key),
                version: None,
                columns,
                schema: node.arrow_schema.clone(),
                key: node.key.clone().unwrap_or_default(),
                object_columns: node
                    .node_object_fields()
                    .map(|field| field.name().clone())
                    .collect(),
                object_fields: node
                    .node_object_members()
                    .map(|(member, field)| {
                        arrow_schema::Field::new(
                            member,
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                    })
                    .collect(),
                row_count: Some(rows),
            },
        );
        self
    }

    pub fn with_full_text_coverage(
        mut self,
        type_key: &str,
        property: &str,
        coverage: FullTextCoverage,
    ) -> Self {
        self.full_text
            .insert((type_key.to_string(), property.to_string()), coverage);
        self
    }

    pub fn with_canonical_key_ids(mut self, params: omnigraph_compiler::ir::ParamMap) -> Self {
        self.canonical_keys = Some(params);
        self
    }

    pub fn with_explain_dump_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.explain_dump_dir = Some(dir);
        self
    }

    pub fn with_index_facts(mut self, dataset_key: &str, facts: Vec<IndexFact>) -> Self {
        self.index_facts.insert(dataset_key.to_string(), facts);
        self
    }

    pub fn with_query_memory_pool_bytes(mut self, bytes: u64) -> Self {
        self.query_memory_pool_bytes = bytes;
        self
    }

    pub fn with_table_data_bytes(mut self, type_key: &str, bytes: u64) -> Self {
        self.table_data_bytes.insert(type_key.to_string(), bytes);
        self
    }

    pub fn with_column_data_bytes(mut self, type_key: &str, column: &str, bytes: u64) -> Self {
        self.column_data_bytes
            .entry(type_key.to_string())
            .or_default()
            .insert(column.to_string(), bytes);
        self
    }

    pub fn with_schema(mut self, side: SideId, schema: SchemaRef) -> Self {
        self.schemas.insert(side, schema);
        self
    }

    pub fn with_expand_statistics(
        mut self,
        edge_type: &str,
        direction: Direction,
        statistics: ExpandStatistics,
    ) -> Self {
        self.expand_statistics
            .push((edge_type.to_string(), direction, statistics));
        self
    }

    pub fn with_edge_version(mut self, edge_type: &str, version: u64) -> Self {
        self.edge_datasets.insert(
            edge_type.to_string(),
            DatasetPin {
                dataset_path: format!("edge:{edge_type}"),
                native_branch: None,
                version,
            },
        );
        self
    }

    pub fn with_traversal(mut self, traversal: Traversal) -> Self {
        self.traversal = Some(traversal);
        self
    }

    pub fn with_traversal_work_limit(mut self, limit: u64) -> Self {
        self.traversal_work_limit = Some(limit);
        self
    }

    pub fn with_ann_nprobes(mut self, nprobes: Option<usize>) -> Self {
        self.ann_nprobes = nprobes;
        self
    }

    pub fn with_node_type(mut self, type_name: &str, spec: NodeTypeSpec) -> Self {
        self.node_types.insert(type_name.to_string(), spec);
        self
    }

    pub fn with_fragments(mut self, side: SideId, fragments: Vec<FragmentStat>) -> Self {
        self.fragments.insert(side, fragments);
        self
    }

    pub fn with_proof(mut self, proof: AdjacencyProof) -> Self {
        self.proof = Some(proof);
        self
    }
}

impl PlanSource for MemorySource {
    fn canonical_key_id(
        &self,
        _type_key: &str,
        value: &IRExpr,
    ) -> Result<Option<String>, PlanError> {
        let Some(params) = &self.canonical_keys else {
            return Ok(None);
        };
        let literal = match value {
            IRExpr::Literal(literal, _) => Some(literal),
            IRExpr::Param(name, _) => params.get(name),
            _ => None,
        };
        Ok(match literal {
            Some(omnigraph_compiler::query::ast::Literal::String(key)) => Some(format!("id:{key}")),
            _ => None,
        })
    }

    fn index_facts(&self, dataset_key: &str) -> Vec<IndexFact> {
        self.index_facts
            .get(dataset_key)
            .cloned()
            .unwrap_or_default()
    }

    fn schema(&self, side: SideId) -> Result<SchemaRef, PlanError> {
        self.schemas
            .get(&side)
            .cloned()
            .ok_or_else(|| PlanError::Unresolved {
                detail: format!("no schema for side {side:?}"),
            })
    }

    fn fragments(&self, side: SideId) -> Vec<FragmentStat> {
        self.fragments.get(&side).cloned().unwrap_or_default()
    }

    fn adjacency_proof(&self) -> Option<&AdjacencyProof> {
        self.proof.as_ref()
    }

    fn node_type(&self, type_name: &str) -> Result<NodeTypeSpec, PlanError> {
        self.node_types
            .get(type_name)
            .cloned()
            .ok_or_else(|| PlanError::Unresolved {
                detail: format!("no node type `{type_name}`"),
            })
    }

    fn expand_statistics(&self, edge_type: &str, direction: Direction) -> Option<ExpandStatistics> {
        self.expand_statistics
            .iter()
            .find(|(name, stored, _)| name == edge_type && *stored == direction)
            .map(|(_, _, statistics)| *statistics)
    }

    fn edge_dataset(&self, edge_type: &str) -> Option<DatasetPin> {
        self.edge_datasets.get(edge_type).cloned()
    }

    fn traversal(&self) -> Traversal {
        self.traversal.unwrap_or(Traversal::Auto)
    }

    fn traversal_work_limit(&self) -> Option<u64> {
        self.traversal_work_limit
    }

    fn ann_nprobes(&self) -> Option<usize> {
        self.ann_nprobes
    }

    fn table_data_bytes(&self, type_key: &str) -> Option<u64> {
        self.table_data_bytes.get(type_key).copied()
    }

    fn column_data_bytes(&self, type_key: &str, column: &str) -> Option<u64> {
        self.column_data_bytes.get(type_key)?.get(column).copied()
    }

    fn query_memory_pool_bytes(&self) -> u64 {
        self.query_memory_pool_bytes
    }

    fn full_text_coverage(&self, type_key: &str, property: &str) -> FullTextCoverage {
        self.full_text
            .get(&(type_key.to_string(), property.to_string()))
            .copied()
            .unwrap_or(FullTextCoverage::Absent)
    }

    fn explain_dump_dir(&self) -> Option<std::path::PathBuf> {
        self.explain_dump_dir.clone()
    }

    fn catalogue_fingerprint(&self, type_key: &str) -> Option<String> {
        self.catalogue_fingerprints.get(type_key).cloned()
    }
}
