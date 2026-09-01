//! Read-only logical validation for registered node-and-edge graph fixtures.
//!
//! Physical registration proves copied bytes. This module opens only that
//! disposable copy, captures one coherent `main` snapshot, checks its complete
//! table inventory, and derives rebuild-stable logical evidence. Generated
//! storage ids are deliberately excluded: keyed node properties remain in the
//! row image, while edge endpoints and declared properties identify edge
//! content (including duplicate parallel edges through multiset cardinality).

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::{self, Write};
use std::path::Path;

use arrow_schema::Schema as ArrowSchema;
use omnigraph::IndexCoverage;
use omnigraph::db::{Omnigraph, ReadTarget, SnapshotDataset};
use omnigraph_compiler::schema_shape_hash_from_ir;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::fixture_reference::{
    DigestReferenceV1, GraphTableCountV1, NormalizedFixtureReferenceV1, RealGraphIndexFreshnessV1,
    RealGraphIndexKindV1, RealGraphIndexV1, RealGraphPayloadV1,
};

pub const REAL_GRAPH_OBSERVATION_VERSION: u32 = 1;
pub const SCHEMA_SHAPE_ALGORITHM: &str = "omnigraph-schema-shape-v1";
pub const LOGICAL_PAYLOAD_ALGORITHM: &str = "omnigraph-logical-properties-v1";
pub const LOGICAL_GRAPH_ALGORITHM: &str = "omnigraph-logical-graph-multiset-v1";

const ROW_HASH_DOMAIN_A: &[u8] = b"omnigraph-logical-row-v1/a\0";
const ROW_HASH_DOMAIN_B: &[u8] = b"omnigraph-logical-row-v1/b\0";
const TABLE_HASH_DOMAIN: &[u8] = b"omnigraph-logical-table-multiset-v1\0";
const GRAPH_HASH_DOMAIN: &[u8] = b"omnigraph-logical-graph-multiset-v1\0";
const MAX_EXPORT_LINE_BYTES: usize = 128 * 1024 * 1024;
const STABLE_PROPERTY_ID_METADATA_KEY: &str = "omnigraph.stable_property_id";

pub type RealGraphResult<T> = Result<T, RealGraphError>;

#[derive(Debug)]
pub struct RealGraphError(String);

impl RealGraphError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for RealGraphError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for RealGraphError {}

impl From<io::Error> for RealGraphError {
    fn from(error: io::Error) -> Self {
        Self::new(error.to_string())
    }
}

/// Path-free evidence derived from one immutable copied graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealGraphObservationV1 {
    pub version: u32,
    pub graph_manifest_version: u64,
    pub main_commit_id: Option<String>,
    pub schema_shape: DigestReferenceV1,
    pub logical_content: DigestReferenceV1,
    pub node_tables: Vec<GraphTableCountV1>,
    pub edge_tables: Vec<GraphTableCountV1>,
    pub logical_payload_bytes: u64,
    pub indexes: Vec<RealGraphIndexV1>,
    pub history_depth: u64,
    pub relocation_self_contained: bool,
    pub branches: Vec<String>,
    /// These declaration fields do not yet have a substrate-owned exact
    /// witness. Keeping them explicit prevents this diagnostic validator from
    /// silently upgrading itself into claim-grade fixture certification.
    pub unverified_state_fields: Vec<String>,
}

/// Compare every currently implemented logical witness to one normalized
/// fixture declaration.
pub fn validate_real_graph_reference(
    reference: &NormalizedFixtureReferenceV1,
    observed: &RealGraphObservationV1,
) -> RealGraphResult<()> {
    let expected = &reference.definition;
    require_equal(
        "logical.data.schema_shape",
        &expected.logical.data.schema_shape,
        &observed.schema_shape,
    )?;
    require_equal(
        "logical.data.node_tables",
        &expected.logical.data.node_tables,
        &observed.node_tables,
    )?;
    require_equal(
        "logical.data.edge_tables",
        &expected.logical.data.edge_tables,
        &observed.edge_tables,
    )?;
    match &expected.logical.data.payload {
        RealGraphPayloadV1::Variable {
            algorithm,
            total_bytes,
        } if algorithm == LOGICAL_PAYLOAD_ALGORITHM
            && *total_bytes == observed.logical_payload_bytes => {}
        RealGraphPayloadV1::Variable {
            algorithm,
            total_bytes,
        } => {
            return Err(RealGraphError::new(format!(
                "logical.data.payload differs: expected algorithm={algorithm:?}, total_bytes={total_bytes}; observed algorithm={LOGICAL_PAYLOAD_ALGORITHM:?}, total_bytes={}",
                observed.logical_payload_bytes
            )));
        }
        RealGraphPayloadV1::Fixed { .. } => {
            return Err(RealGraphError::new(
                "logical.data.payload differs: this validator observes an exact variable-width logical property total",
            ));
        }
    }
    require_equal(
        "logical.state.indexes",
        &expected.logical.state.indexes,
        &observed.indexes,
    )?;
    require_equal(
        "logical.state.history_depth",
        &expected.logical.state.history_depth,
        &observed.history_depth,
    )?;
    require_equal(
        "expected.logical_content",
        &expected.expected.logical_content,
        &observed.logical_content,
    )?;
    if !observed.relocation_self_contained {
        return Err(RealGraphError::new(
            "copied graph retains external Lance base paths and is not relocation self-contained",
        ));
    }
    Ok(())
}

fn require_equal<T: PartialEq + std::fmt::Debug>(
    path: &str,
    expected: &T,
    observed: &T,
) -> RealGraphResult<()> {
    if expected == observed {
        Ok(())
    } else {
        Err(RealGraphError::new(format!(
            "{path} differs: expected {expected:?}, observed {observed:?}"
        )))
    }
}

/// Inspect a private, quiescent graph copy without mutating it.
pub async fn observe_real_graph(root: &Path) -> RealGraphResult<RealGraphObservationV1> {
    let root = root
        .to_str()
        .ok_or_else(|| RealGraphError::new("real graph root must be valid UTF-8"))?;
    let db = Omnigraph::open_read_only(root)
        .await
        .map_err(|error| RealGraphError::new(format!("open copied graph read-only: {error}")))?;
    let mut branches = db
        .branch_list()
        .await
        .map_err(|error| RealGraphError::new(format!("list copied graph branches: {error}")))?;
    branches.sort();
    if branches.iter().any(|branch| branch != "main") {
        return Err(RealGraphError::new(format!(
            "registered real graph must be a main-only base; observed branches {branches:?}"
        )));
    }

    let catalog = db.catalog();
    let accepted_ir = catalog.bound_schema_ir().ok_or_else(|| {
        RealGraphError::new("copied graph catalog is not bound to accepted schema identity")
    })?;
    for node in &accepted_ir.nodes {
        let catalog_node = catalog.node_types.get(&node.name).ok_or_else(|| {
            RealGraphError::new(format!(
                "accepted node {} is absent from catalog",
                node.name
            ))
        })?;
        if catalog_node.key.as_ref().is_none_or(Vec::is_empty) {
            return Err(RealGraphError::new(format!(
                "node {} has no deterministic @key; logical id normalization is unsupported",
                node.name
            )));
        }
    }
    let shape_hash = schema_shape_hash_from_ir(accepted_ir)
        .map_err(|error| RealGraphError::new(format!("hash accepted schema shape: {error}")))?;
    let schema_sha256 = shape_hash.strip_prefix("sha256:").ok_or_else(|| {
        RealGraphError::new("schema shape hash did not use the expected sha256 prefix")
    })?;

    let snapshot = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(|error| RealGraphError::new(format!("capture copied graph main: {error}")))?;
    let graph_manifest_version = snapshot.graph_manifest_version();
    let main_commit_id = snapshot.graph_head(None).map(str::to_owned);
    let expected_table_keys = accepted_ir
        .nodes
        .iter()
        .map(|node| format!("node:{}", node.name))
        .chain(
            accepted_ir
                .edges
                .iter()
                .map(|edge| format!("edge:{}", edge.name)),
        )
        .collect::<BTreeSet<_>>();
    let observed_table_keys = snapshot
        .datasets()
        .map(|entry| entry.type_key.clone())
        .collect::<BTreeSet<_>>();
    if observed_table_keys != expected_table_keys {
        return Err(RealGraphError::new(format!(
            "accepted schema and main snapshot table inventories differ: schema={expected_table_keys:?}, snapshot={observed_table_keys:?}"
        )));
    }

    let mut node_tables = Vec::new();
    let mut edge_tables = Vec::new();
    let mut indexes = Vec::new();
    let mut expected_logical_rows = BTreeMap::new();
    let mut relocation_self_contained =
        !db.manifest_has_external_base_paths(None)
            .await
            .map_err(|error| {
                RealGraphError::new(format!(
                    "inspect graph-manifest relocation metadata: {error}"
                ))
            })?;
    for table_key in &expected_table_keys {
        let entry = snapshot
            .dataset(table_key)
            .expect("table-key equality proved every accepted table is present");
        let dataset = snapshot.open_dataset(table_key).await.map_err(|error| {
            RealGraphError::new(format!("open pinned dataset {table_key}: {error}"))
        })?;
        let physical_rows = dataset.count_rows(None).await.map_err(|error| {
            RealGraphError::new(format!("count pinned dataset {table_key}: {error}"))
        })?;
        let physical_rows = u64::try_from(physical_rows)
            .map_err(|_| RealGraphError::new(format!("row count for {table_key} exceeds u64")))?;
        if physical_rows != entry.entity_count {
            return Err(RealGraphError::new(format!(
                "main snapshot row count for {table_key} differs: manifest={}, physical={physical_rows}",
                entry.entity_count
            )));
        }
        expected_logical_rows.insert(table_key.clone(), physical_rows);
        relocation_self_contained &= !dataset.has_external_base_paths();

        let (name, expected_schema, node_declared_indexes) =
            if let Some(name) = table_key.strip_prefix("node:") {
                let ty = catalog.node_types.get(name).ok_or_else(|| {
                    RealGraphError::new(format!("catalog is missing accepted node {name}"))
                })?;
                (name, ty.arrow_schema.as_ref(), Some(ty.indices.as_slice()))
            } else if let Some(name) = table_key.strip_prefix("edge:") {
                let ty = catalog.edge_types.get(name).ok_or_else(|| {
                    RealGraphError::new(format!("catalog is missing accepted edge {name}"))
                })?;
                (name, ty.arrow_schema.as_ref(), None)
            } else {
                return Err(RealGraphError::new(format!(
                    "main snapshot contains invalid graph table key {table_key}"
                )));
            };
        let physical_schema = ArrowSchema::from(dataset.schema());
        require_compatible_physical_schema(table_key, expected_schema, &physical_schema)?;

        // This inventories the indexes the current engine owns: every node id,
        // each declared node property (probing its actual physical kind), and
        // edge id/src/dst. Unknown raw Lance metadata remains explicitly
        // unverified below rather than being presented as a complete inventory.
        let mut index_columns = BTreeSet::from(["id".to_string()]);
        if let Some(declared_indexes) = node_declared_indexes {
            for fields in declared_indexes {
                let [column] = fields.as_slice() else {
                    return Err(RealGraphError::new(format!(
                        "{table_key} declares a composite index; real-graph validator v1 supports single-column indexes only"
                    )));
                };
                index_columns.insert(column.clone());
            }
        } else {
            index_columns.extend(["src".to_string(), "dst".to_string()]);
        }
        let table_has_stale_index = dataset.has_unindexed_fragments().await.map_err(|error| {
            RealGraphError::new(format!("inspect index freshness for {table_key}: {error}"))
        })?;
        for column in index_columns {
            observe_engine_index(
                &dataset,
                table_key,
                &column,
                table_has_stale_index,
                &mut indexes,
            )
            .await?;
        }
        let row_count = GraphTableCountV1 {
            name: name.to_string(),
            rows: physical_rows,
        };
        if table_key.starts_with("node:") {
            node_tables.push(row_count);
        } else {
            edge_tables.push(row_count);
        }
    }
    node_tables.sort();
    edge_tables.sort();
    indexes.sort();

    let history_depth = u64::try_from(
        db.list_commits(Some("main"))
            .await
            .map_err(|error| RealGraphError::new(format!("list main history: {error}")))?
            .len(),
    )
    .map_err(|_| RealGraphError::new("main history depth exceeds u64"))?;
    let mut sink = LogicalGraphSink::default();
    db.export_jsonl_unordered_to_writer("main", &[], &mut sink)
        .await
        .map_err(|error| RealGraphError::new(format!("stream copied graph content: {error}")))?;
    sink.verify_table_counts(&expected_logical_rows)?;
    let content = sink.finish(schema_sha256)?;
    let closing_snapshot = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(|error| RealGraphError::new(format!("recapture copied graph main: {error}")))?;
    if closing_snapshot.graph_manifest_version() != graph_manifest_version
        || closing_snapshot.graph_head(None) != main_commit_id.as_deref()
    {
        return Err(RealGraphError::new(
            "copied graph main changed while logical evidence was being observed",
        ));
    }
    drop(db);

    Ok(RealGraphObservationV1 {
        version: REAL_GRAPH_OBSERVATION_VERSION,
        graph_manifest_version,
        main_commit_id,
        schema_shape: DigestReferenceV1 {
            algorithm: SCHEMA_SHAPE_ALGORITHM.to_string(),
            sha256: schema_sha256.to_string(),
        },
        logical_content: DigestReferenceV1 {
            algorithm: LOGICAL_GRAPH_ALGORITHM.to_string(),
            sha256: content.logical_content_sha256,
        },
        node_tables,
        edge_tables,
        logical_payload_bytes: content.logical_payload_bytes,
        indexes,
        history_depth,
        relocation_self_contained,
        branches,
        unverified_state_fields: vec![
            "aging".to_string(),
            "deletion-history".to_string(),
            "compaction-recency".to_string(),
            "per-index-fts-ann-freshness".to_string(),
            "unknown-index-metadata".to_string(),
        ],
    })
}

fn require_compatible_physical_schema(
    table_key: &str,
    expected: &ArrowSchema,
    observed: &ArrowSchema,
) -> RealGraphResult<()> {
    if expected.fields().len() != observed.fields().len()
        || expected.metadata() != observed.metadata()
    {
        return Err(RealGraphError::new(format!(
            "physical schema for {table_key} differs from the accepted catalog"
        )));
    }
    for (expected, observed) in expected.fields().iter().zip(observed.fields()) {
        if expected.name() != observed.name()
            || expected.data_type() != observed.data_type()
            || expected.is_nullable() != observed.is_nullable()
        {
            return Err(RealGraphError::new(format!(
                "physical field {table_key}.{} differs from the accepted catalog",
                expected.name()
            )));
        }
        let mut observed_metadata = observed.metadata().clone();
        if !observed_metadata.contains_key(STABLE_PROPERTY_ID_METADATA_KEY) {
            if let Some(stable_id) = expected.metadata().get(STABLE_PROPERTY_ID_METADATA_KEY) {
                // Supported v6 graphs created before stable property markers
                // were added lack this one annotation. The engine accepts
                // those tables by exact pinned version; every other physical
                // field property and every present marker must still match.
                observed_metadata.insert(
                    STABLE_PROPERTY_ID_METADATA_KEY.to_string(),
                    stable_id.clone(),
                );
            }
        }
        if &observed_metadata != expected.metadata() {
            return Err(RealGraphError::new(format!(
                "physical metadata for {table_key}.{} differs from the accepted catalog",
                expected.name()
            )));
        }
    }
    Ok(())
}

async fn observe_engine_index(
    dataset: &SnapshotDataset,
    table_key: &str,
    column: &str,
    table_has_stale_index: bool,
    output: &mut Vec<RealGraphIndexV1>,
) -> RealGraphResult<()> {
    if dataset.has_btree_index(column).await.map_err(|error| {
        RealGraphError::new(format!("inspect BTREE {table_key}.{column}: {error}"))
    })? {
        let freshness = match dataset.index_coverage(column).await.map_err(|error| {
            RealGraphError::new(format!(
                "inspect BTREE coverage {table_key}.{column}: {error}"
            ))
        })? {
            IndexCoverage::Indexed => RealGraphIndexFreshnessV1::Optimized,
            IndexCoverage::Degraded { .. } => RealGraphIndexFreshnessV1::RowsStale,
        };
        output.push(RealGraphIndexV1 {
            table: table_key.to_string(),
            column: column.to_string(),
            kind: RealGraphIndexKindV1::Btree,
            freshness,
        });
    }
    for (present, kind) in [
        (
            dataset.has_fts_index(column).await.map_err(|error| {
                RealGraphError::new(format!("inspect FTS {table_key}.{column}: {error}"))
            })?,
            RealGraphIndexKindV1::Fts,
        ),
        (
            dataset.has_vector_index(column).await.map_err(|error| {
                RealGraphError::new(format!("inspect ANN {table_key}.{column}: {error}"))
            })?,
            RealGraphIndexKindV1::Ann,
        ),
    ] {
        if present {
            output.push(RealGraphIndexV1 {
                table: table_key.to_string(),
                column: column.to_string(),
                kind,
                freshness: if table_has_stale_index {
                    RealGraphIndexFreshnessV1::RowsStale
                } else {
                    RealGraphIndexFreshnessV1::Optimized
                },
            });
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct TableAccumulator {
    rows: u64,
    sum_a: [u8; 32],
    sum_b: [u8; 32],
}

#[derive(Debug, Default)]
struct LogicalGraphSink {
    pending: Vec<u8>,
    tables: BTreeMap<String, TableAccumulator>,
    logical_payload_bytes: u64,
}

struct LogicalContentSummary {
    logical_content_sha256: String,
    logical_payload_bytes: u64,
}

impl LogicalGraphSink {
    fn verify_table_counts(&self, expected: &BTreeMap<String, u64>) -> RealGraphResult<()> {
        let observed = expected
            .keys()
            .map(|table| {
                (
                    table.clone(),
                    self.tables.get(table).map_or(0, |value| value.rows),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let unexpected = self
            .tables
            .keys()
            .filter(|table| !expected.contains_key(*table))
            .cloned()
            .collect::<Vec<_>>();
        if &observed != expected || !unexpected.is_empty() {
            return Err(RealGraphError::new(format!(
                "canonical export table counts differ from the pinned graph: expected={expected:?}, observed={observed:?}, unexpected={unexpected:?}"
            )));
        }
        Ok(())
    }

    fn consume_pending_lines(&mut self, max_line_bytes: usize) -> io::Result<()> {
        while let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
            if newline > max_line_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "canonical export row exceeds the validator line budget",
                ));
            }
            let line = self.pending.drain(..=newline).collect::<Vec<_>>();
            self.consume_line(&line[..line.len() - 1])
                .map_err(io::Error::other)?;
        }
        if self.pending.len() > max_line_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical export row exceeds the validator line budget",
            ));
        }
        Ok(())
    }

    fn consume_line(&mut self, line: &[u8]) -> RealGraphResult<()> {
        if line.is_empty() {
            return Ok(());
        }
        let mut row: Value = serde_json::from_slice(line)
            .map_err(|error| RealGraphError::new(format!("parse canonical export row: {error}")))?;
        let object = row
            .as_object_mut()
            .ok_or_else(|| RealGraphError::new("canonical export row is not an object"))?;
        let node_type = object
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let edge_type = object
            .get("edge")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let (table_key, canonical_row, payload_bytes) = if let Some(node) = node_type {
            let mut data = object
                .remove("data")
                .and_then(|value| value.as_object().cloned())
                .ok_or_else(|| RealGraphError::new("node export row has no data object"))?;
            data.remove("id");
            let payload = canonical_json_bytes(&Value::Object(data.clone()))?;
            let mut canonical = Vec::new();
            frame(&mut canonical, b"node");
            frame(&mut canonical, node.as_bytes());
            frame(&mut canonical, &payload);
            (format!("node:{node}"), canonical, payload.len())
        } else if let Some(edge) = edge_type {
            let from = object
                .get("from")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| RealGraphError::new("edge export row has no string from"))?;
            let to = object
                .get("to")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| RealGraphError::new("edge export row has no string to"))?;
            let mut data = object
                .remove("data")
                .and_then(|value| value.as_object().cloned())
                .ok_or_else(|| RealGraphError::new("edge export row has no data object"))?;
            data.remove("id");
            let payload = canonical_json_bytes(&Value::Object(data.clone()))?;
            let mut canonical = Vec::new();
            frame(&mut canonical, b"edge");
            frame(&mut canonical, edge.as_bytes());
            frame(&mut canonical, from.as_bytes());
            frame(&mut canonical, to.as_bytes());
            frame(&mut canonical, &payload);
            (format!("edge:{edge}"), canonical, payload.len())
        } else {
            return Err(RealGraphError::new(
                "canonical export row is neither a node nor an edge",
            ));
        };
        self.logical_payload_bytes = self
            .logical_payload_bytes
            .checked_add(u64::try_from(payload_bytes).map_err(|_| {
                RealGraphError::new("one canonical logical payload length exceeds u64")
            })?)
            .ok_or_else(|| RealGraphError::new("logical payload byte total exceeds u64"))?;
        let table = self.tables.entry(table_key).or_default();
        table.rows = table
            .rows
            .checked_add(1)
            .ok_or_else(|| RealGraphError::new("logical table row count exceeds u64"))?;
        add_mod_256(
            &mut table.sum_a,
            &domain_hash(ROW_HASH_DOMAIN_A, &canonical_row),
        );
        add_mod_256(
            &mut table.sum_b,
            &domain_hash(ROW_HASH_DOMAIN_B, &canonical_row),
        );
        Ok(())
    }

    fn finish(self, schema_sha256: &str) -> RealGraphResult<LogicalContentSummary> {
        if !self.pending.is_empty() {
            return Err(RealGraphError::new(
                "canonical export ended with an unterminated JSONL row",
            ));
        }
        let schema = decode_sha256(schema_sha256)?;
        let mut graph = Sha256::new();
        graph.update(GRAPH_HASH_DOMAIN);
        frame_digest(&mut graph, b"schema-sha256", &schema);
        for (table_key, table) in self.tables {
            let mut table_digest = Sha256::new();
            table_digest.update(TABLE_HASH_DOMAIN);
            frame_digest(&mut table_digest, b"table", table_key.as_bytes());
            frame_digest(&mut table_digest, b"rows-u64-le", &table.rows.to_le_bytes());
            frame_digest(&mut table_digest, b"sum-a", &table.sum_a);
            frame_digest(&mut table_digest, b"sum-b", &table.sum_b);
            frame_digest(&mut graph, b"table-sha256", &table_digest.finalize());
        }
        Ok(LogicalContentSummary {
            logical_content_sha256: format!("{:x}", graph.finalize()),
            logical_payload_bytes: self.logical_payload_bytes,
        })
    }
}

impl Write for LogicalGraphSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.pending.extend_from_slice(bytes);
        self.consume_pending_lines(MAX_EXPORT_LINE_BYTES)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn canonical_json_bytes(value: &Value) -> RealGraphResult<Vec<u8>> {
    let mut bytes = Vec::new();
    write_canonical_json(value, &mut bytes)?;
    Ok(bytes)
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) -> RealGraphResult<()> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_writer(&mut *output, value).map_err(|error| {
                RealGraphError::new(format!("serialize canonical scalar: {error}"))
            })?;
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key).map_err(|error| {
                    RealGraphError::new(format!("serialize canonical object key: {error}"))
                })?;
                output.push(b':');
                write_canonical_json(
                    values
                        .get(key)
                        .expect("canonical object key came from this object"),
                    output,
                )?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn frame(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
}

fn frame_digest(digest: &mut Sha256, label: &[u8], value: &[u8]) {
    digest.update((label.len() as u64).to_le_bytes());
    digest.update(label);
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

fn domain_hash(domain: &[u8], value: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
    digest.finalize().into()
}

fn add_mod_256(sum: &mut [u8; 32], value: &[u8; 32]) {
    let mut carry = 0u16;
    for (left, right) in sum.iter_mut().zip(value) {
        let total = u16::from(*left) + u16::from(*right) + carry;
        *left = total as u8;
        carry = total >> 8;
    }
}

fn decode_sha256(value: &str) -> RealGraphResult<[u8; 32]> {
    if value.len() != 64 {
        return Err(RealGraphError::new("schema SHA-256 is not 64 hex digits"));
    }
    let mut output = [0u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        let offset = index * 2;
        let pair = &value.as_bytes()[offset..offset + 2];
        let high = decode_hex_digit(pair[0])?;
        let low = decode_hex_digit(pair[1])?;
        *byte = (high << 4) | low;
    }
    Ok(output)
}

fn decode_hex_digit(value: u8) -> RealGraphResult<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(RealGraphError::new(
            "schema SHA-256 contains a non-hex digit",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
    use omnigraph::loader::{LoadMode, load_jsonl};

    fn export_rows(rows: &[&str]) -> LogicalContentSummary {
        let mut sink = LogicalGraphSink::default();
        for row in rows {
            sink.write_all(row.as_bytes()).unwrap();
            sink.write_all(b"\n").unwrap();
        }
        sink.finish(&"a".repeat(64)).unwrap()
    }

    fn physical_schema(stable_property_id: Option<&str>, primary_key: bool) -> ArrowSchema {
        let mut id_metadata = std::collections::HashMap::new();
        if primary_key {
            id_metadata.insert(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string());
        }
        let mut name_metadata = std::collections::HashMap::new();
        if let Some(stable_property_id) = stable_property_id {
            name_metadata.insert(
                STABLE_PROPERTY_ID_METADATA_KEY.to_string(),
                stable_property_id.to_string(),
            );
        }
        ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false).with_metadata(id_metadata),
            Field::new("name", DataType::Utf8, true).with_metadata(name_metadata),
        ])
    }

    #[test]
    fn physical_schema_allows_only_absent_legacy_stable_property_marker() {
        let expected = physical_schema(Some("42"), true);
        let legacy = physical_schema(None, true);
        require_compatible_physical_schema("node:Person", &expected, &legacy).unwrap();

        let wrong_stable_id = physical_schema(Some("41"), true);
        assert!(
            require_compatible_physical_schema("node:Person", &expected, &wrong_stable_id)
                .unwrap_err()
                .to_string()
                .contains("physical metadata")
        );

        let missing_primary_key = physical_schema(None, false);
        assert!(
            require_compatible_physical_schema("node:Person", &expected, &missing_primary_key)
                .unwrap_err()
                .to_string()
                .contains("physical metadata")
        );
    }

    #[test]
    fn logical_multiset_omits_generated_ids_preserves_duplicates_and_ignores_order() {
        let edge_a =
            r#"{"edge":"Transfer","from":"a","to":"b","data":{"id":"generated-a","amount":1}}"#;
        let edge_b =
            r#"{"edge":"Transfer","from":"a","to":"b","data":{"id":"generated-b","amount":1}}"#;
        let edge_other =
            r#"{"edge":"Transfer","from":"b","to":"a","data":{"id":"generated-c","amount":2}}"#;
        let left = export_rows(&[edge_a, edge_other]);
        let rebuilt = export_rows(&[edge_other, edge_b]);
        assert_eq!(left.logical_content_sha256, rebuilt.logical_content_sha256);
        let duplicate = export_rows(&[edge_a, edge_b, edge_other]);
        assert_ne!(
            left.logical_content_sha256,
            duplicate.logical_content_sha256
        );
    }

    #[test]
    fn canonical_property_order_and_redundant_node_id_do_not_change_content() {
        let first = export_rows(&[
            r#"{"type":"Person","data":{"id":"physical-a","personId":"p1","active":true}}"#,
        ]);
        let second = export_rows(&[
            r#"{"type":"Person","data":{"active":true,"personId":"p1","id":"physical-b"}}"#,
        ]);
        assert_eq!(first.logical_content_sha256, second.logical_content_sha256);
        assert_eq!(first.logical_payload_bytes, second.logical_payload_bytes);
    }

    #[test]
    fn newline_terminated_rows_still_obey_the_line_budget() {
        let mut sink = LogicalGraphSink::default();
        sink.pending.extend_from_slice(b"12345\n");
        let error = sink.consume_pending_lines(4).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn partial_export_cannot_mint_a_logical_digest() {
        let mut sink = LogicalGraphSink::default();
        sink.write_all(
            br#"{"type":"Person","data":{"id":"p1","personId":"p1"}}
"#,
        )
        .unwrap();
        let expected = BTreeMap::from([("node:Person".to_string(), 2)]);
        assert!(sink.verify_table_counts(&expected).is_err());
    }

    #[tokio::test]
    async fn observes_a_complete_node_and_edge_graph_without_writing_it() {
        const SCHEMA: &str = r#"
            node Person {
                personId: String @key
                active: Bool
            }
            edge Knows: Person -> Person {
                since: I64
            }
        "#;
        const DATA: &str = r#"{"type":"Person","data":{"personId":"p1","active":true}}
{"type":"Person","data":{"personId":"p2","active":false}}
{"edge":"Knows","from":"p1","to":"p2","data":{"since":2026}}"#;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("graph");
        let uri = root.to_str().unwrap();
        let db = Omnigraph::init(uri, SCHEMA).await.unwrap();
        load_jsonl(&db, DATA, LoadMode::Overwrite).await.unwrap();
        drop(db);

        let before =
            crate::reset::digest_physical_tree(&root, crate::reset::TraversalLimits::default())
                .unwrap();
        let observed = observe_real_graph(&root).await.unwrap();
        let after =
            crate::reset::digest_physical_tree(&root, crate::reset::TraversalLimits::default())
                .unwrap();

        assert_eq!(before, after);
        assert_eq!(
            observed.node_tables,
            vec![GraphTableCountV1 {
                name: "Person".to_string(),
                rows: 2,
            }]
        );
        assert_eq!(
            observed.edge_tables,
            vec![GraphTableCountV1 {
                name: "Knows".to_string(),
                rows: 1,
            }]
        );
        assert!(observed.relocation_self_contained);
        assert!(observed.logical_payload_bytes > 0);
        assert_eq!(observed.logical_content.sha256.len(), 64);
        assert!(observed.history_depth > 0);
    }
}
