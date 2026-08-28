//! Logical declarations for externally built node-and-edge fixtures.
//!
//! This module only validates and normalizes a declaration. It does not open a
//! graph or certify that the declared digest is true. Dedicated location,
//! credential, commit, timestamp, and physical-tree fields are absent, and
//! builder parameters deliberately have no arbitrary string value.

use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::model::{
    Diagnostic, ValidationOutcome, declared_version, read_yaml_file, strict_yaml, typed_sha256,
    valid_kebab_id,
};

pub const FIXTURE_REFERENCE_FORMAT_VERSION: u32 = 1;

const MAX_ID_BYTES: usize = 128;
const MAX_PARAMETERS: usize = 128;
const MAX_INPUTS: usize = 256;
const MAX_GRAPH_TYPES: usize = 10_000;
const MAX_INDEXES: usize = 100_000;
const MAX_BUILDER_VERSION: u32 = 1_000_000;
const MAX_ROWS_PER_GRAPH_TYPE: u64 = 1_000_000_000_000;
const MAX_TOTAL_GRAPH_ROWS: u64 = 10_000_000_000_000;
const MAX_PAYLOAD_BYTES_PER_ROW: u64 = 64 * 1024 * 1024;
const MAX_TOTAL_LOGICAL_PAYLOAD_BYTES: u64 = 1 << 60;
const MAX_HISTORY_DEPTH: u64 = 10_000_000;

/// A strict V1 declaration of one externally built logical fixture.
///
/// `fixture_id`, the reference digest, and `expected` are linkage/evidence,
/// not future point identity. A future case must materialize `logical` into its
/// versioned run spec rather than hashing a path or this reference as an opaque
/// value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureReferenceV1 {
    pub version: u32,
    pub fixture_id: String,
    pub logical: RealGraphLogicalDeclarationV1,
    pub expected: ExpectedLogicalContentV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealGraphLogicalDeclarationV1 {
    pub builder: ImportedGraphBuilderV1,
    pub data: RealGraphDataV1,
    pub state: RealGraphStateV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportedGraphBuilderV1 {
    pub id: String,
    pub version: u32,
    pub recipe_sha256: String,
    pub parameters: Vec<BuilderParameterV1>,
    pub inputs: Vec<BuilderInputV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuilderParameterV1 {
    pub name: String,
    pub value: BuilderParameterValueV1,
}

/// V1 deliberately has no arbitrary string values, so paths, URIs, and
/// credentials cannot be stored here. Parameter names and numeric meanings are
/// trusted builder-contract input that the future builder adapter must check.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BuilderParameterValueV1 {
    Null(()),
    Bool(bool),
    U64(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuilderInputV1 {
    /// Path-free semantic role for one content-addressed builder input.
    pub role: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DigestReferenceV1 {
    /// Versioned canonical byte-projection identifier.
    pub algorithm: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealGraphDataV1 {
    pub provenance: RealGraphDataProvenance,
    pub schema_shape: DigestReferenceV1,
    pub node_tables: Vec<GraphTableCountV1>,
    pub edge_tables: Vec<GraphTableCountV1>,
    pub payload: RealGraphPayloadV1,
    pub column_shape: RealGraphColumnShape,
    pub topology_skew: RealGraphTopologySkew,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphDataProvenance {
    Synthetic,
    CorpusDerived,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphTableCountV1 {
    pub name: String,
    pub rows: u64,
}

/// Exact payload-size factor. Variable-width data records an exact total; its
/// bytes-per-row level is the rational `total_bytes / total graph rows`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RealGraphPayloadV1 {
    Fixed {
        /// Versioned rule defining which logical bytes each row includes.
        algorithm: String,
        bytes_per_row: u64,
    },
    Variable {
        /// Versioned rule defining which logical bytes the total includes.
        algorithm: String,
        total_bytes: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphColumnShape {
    Scalars,
    ScalarsVector,
    ScalarsBlob,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphTopologySkew {
    Uniform,
    PowerLaw,
    SourceDefined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealGraphStateV1 {
    pub aging: RealGraphAgingV1,
    pub indexes: Vec<RealGraphIndexV1>,
    pub deletion_history: RealGraphDeletionHistoryV1,
    pub compaction_recency: RealGraphCompactionRecencyV1,
    pub history_depth: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphAgingV1 {
    BulkLoaded,
    SmallCommits,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealGraphIndexV1 {
    pub table: String,
    pub column: String,
    pub kind: RealGraphIndexKindV1,
    pub freshness: RealGraphIndexFreshnessV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphIndexKindV1 {
    Btree,
    Fts,
    Ann,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphIndexFreshnessV1 {
    Optimized,
    RowsStale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphDeletionHistoryV1 {
    None,
    Heavy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphCompactionRecencyV1 {
    Optimized,
    NotOptimized,
}

/// Expected output of the future graph-level validator. This is a required
/// declaration, not proof: only recomputation against a copied graph can turn
/// it into validation evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedLogicalContentV1 {
    pub logical_content: DigestReferenceV1,
}

/// A normalized declaration and its complete, version-bearing audit digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedFixtureReferenceV1 {
    pub definition: FixtureReferenceV1,
    /// Audit/linkage only. A future point id hashes its typed run spec instead.
    pub reference_sha256: String,
}

pub fn load_fixture_reference(path: &Path) -> ValidationOutcome<NormalizedFixtureReferenceV1> {
    let source = match read_yaml_file(path, "fixture_reference") {
        Ok(source) => source,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    parse_fixture_reference(&source)
}

pub fn parse_fixture_reference(source: &str) -> ValidationOutcome<NormalizedFixtureReferenceV1> {
    let version = match declared_version(source, "fixture_reference") {
        Ok(version) => version,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    if version != FIXTURE_REFERENCE_FORMAT_VERSION {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "unsupported_fixture_reference_version",
            "version",
            format!(
                "expected fixture reference version {FIXTURE_REFERENCE_FORMAT_VERSION}, observed {version}"
            ),
        )]);
    }
    let reference = match strict_yaml(source, "fixture_reference") {
        Ok(reference) => reference,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    normalize_fixture_reference(reference)
}

pub fn normalize_fixture_reference(
    mut reference: FixtureReferenceV1,
) -> ValidationOutcome<NormalizedFixtureReferenceV1> {
    let mut diagnostics = Vec::new();
    if reference.version != FIXTURE_REFERENCE_FORMAT_VERSION {
        diagnostics.push(Diagnostic::error(
            "unsupported_fixture_reference_version",
            "version",
            format!(
                "expected fixture reference version {FIXTURE_REFERENCE_FORMAT_VERSION}, observed {}",
                reference.version
            ),
        ));
    }
    validate_kebab_id(
        &reference.fixture_id,
        "fixture_id",
        "invalid_fixture_reference_id",
        &mut diagnostics,
    );
    validate_builder(&mut reference.logical.builder, &mut diagnostics);
    validate_data(&mut reference.logical.data, &mut diagnostics);
    validate_state(
        &mut reference.logical.state,
        &reference.logical.data,
        &mut diagnostics,
    );
    validate_digest_reference(
        &reference.expected.logical_content,
        "expected.logical_content",
        &mut diagnostics,
    );
    if !diagnostics.is_empty() {
        return ValidationOutcome::failure(diagnostics);
    }
    let reference_sha256 = match typed_sha256(&reference) {
        Ok(digest) => digest,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    ValidationOutcome::success(NormalizedFixtureReferenceV1 {
        definition: reference,
        reference_sha256,
    })
}

fn validate_builder(builder: &mut ImportedGraphBuilderV1, diagnostics: &mut Vec<Diagnostic>) {
    validate_kebab_id(
        &builder.id,
        "logical.builder.id",
        "invalid_fixture_builder_id",
        diagnostics,
    );
    if !(1..=MAX_BUILDER_VERSION).contains(&builder.version) {
        diagnostics.push(Diagnostic::error(
            "invalid_fixture_builder_version",
            "logical.builder.version",
            format!("builder version must be in 1..={MAX_BUILDER_VERSION}"),
        ));
    }
    validate_sha256(
        &builder.recipe_sha256,
        "logical.builder.recipe_sha256",
        diagnostics,
    );
    if builder.parameters.len() > MAX_PARAMETERS {
        diagnostics.push(Diagnostic::error(
            "fixture_builder_parameter_budget_exceeded",
            "logical.builder.parameters",
            format!("at most {MAX_PARAMETERS} builder parameters are allowed"),
        ));
    }
    let mut parameter_names = BTreeSet::new();
    for (index, parameter) in builder.parameters.iter().enumerate() {
        validate_kebab_id(
            &parameter.name,
            &format!("logical.builder.parameters[{index}].name"),
            "invalid_fixture_builder_parameter",
            diagnostics,
        );
        if !parameter_names.insert(parameter.name.as_str()) {
            diagnostics.push(Diagnostic::error(
                "duplicate_fixture_builder_parameter",
                format!("logical.builder.parameters[{index}].name"),
                format!(
                    "builder parameter '{}' appears more than once",
                    parameter.name
                ),
            ));
        }
    }
    builder.parameters.sort();

    if builder.inputs.is_empty() || builder.inputs.len() > MAX_INPUTS {
        diagnostics.push(Diagnostic::error(
            "invalid_fixture_builder_inputs",
            "logical.builder.inputs",
            format!("an imported graph requires 1..={MAX_INPUTS} digest-pinned inputs"),
        ));
    }
    let mut input_roles = BTreeSet::new();
    for (index, input) in builder.inputs.iter().enumerate() {
        validate_kebab_id(
            &input.role,
            &format!("logical.builder.inputs[{index}].role"),
            "invalid_fixture_builder_input_role",
            diagnostics,
        );
        validate_sha256(
            &input.sha256,
            &format!("logical.builder.inputs[{index}].sha256"),
            diagnostics,
        );
        if !input_roles.insert(input.role.as_str()) {
            diagnostics.push(Diagnostic::error(
                "duplicate_fixture_builder_input",
                format!("logical.builder.inputs[{index}].role"),
                format!("builder input role '{}' appears more than once", input.role),
            ));
        }
    }
    builder.inputs.sort();
}

fn validate_data(data: &mut RealGraphDataV1, diagnostics: &mut Vec<Diagnostic>) {
    validate_digest_reference(&data.schema_shape, "logical.data.schema_shape", diagnostics);
    let node_rows = validate_table_inventory(&mut data.node_tables, "node", diagnostics);
    let edge_rows = validate_table_inventory(&mut data.edge_tables, "edge", diagnostics);
    if data
        .node_tables
        .len()
        .saturating_add(data.edge_tables.len())
        > MAX_GRAPH_TYPES
    {
        diagnostics.push(Diagnostic::error(
            "fixture_graph_type_budget_exceeded",
            "logical.data",
            format!("at most {MAX_GRAPH_TYPES} total node and edge types are allowed"),
        ));
    }
    let total_rows = node_rows
        .and_then(|node_rows| edge_rows.and_then(|edge_rows| node_rows.checked_add(edge_rows)));
    if total_rows.is_none_or(|total| total > MAX_TOTAL_GRAPH_ROWS) {
        diagnostics.push(Diagnostic::error(
            "fixture_total_row_budget_exceeded",
            "logical.data",
            format!("total node and edge rows must be <= {MAX_TOTAL_GRAPH_ROWS}"),
        ));
    }
    match &data.payload {
        RealGraphPayloadV1::Fixed {
            algorithm,
            bytes_per_row,
        } => {
            validate_algorithm_id(algorithm, "logical.data.payload.algorithm", diagnostics);
            if *bytes_per_row > MAX_PAYLOAD_BYTES_PER_ROW {
                diagnostics.push(Diagnostic::error(
                    "fixture_payload_budget_exceeded",
                    "logical.data.payload.bytes_per_row",
                    format!("payload bytes per row must be <= {MAX_PAYLOAD_BYTES_PER_ROW}"),
                ));
            }
            if total_rows
                .and_then(|rows| rows.checked_mul(*bytes_per_row))
                .is_none_or(|total| total > MAX_TOTAL_LOGICAL_PAYLOAD_BYTES)
            {
                diagnostics.push(Diagnostic::error(
                    "fixture_payload_budget_exceeded",
                    "logical.data.payload",
                    format!(
                        "total logical payload bytes must be <= {MAX_TOTAL_LOGICAL_PAYLOAD_BYTES}"
                    ),
                ));
            }
        }
        RealGraphPayloadV1::Variable {
            algorithm,
            total_bytes,
        } => {
            validate_algorithm_id(algorithm, "logical.data.payload.algorithm", diagnostics);
            let average_budget = total_rows
                .and_then(|rows| rows.checked_mul(MAX_PAYLOAD_BYTES_PER_ROW))
                .unwrap_or(u64::MAX);
            if *total_bytes > MAX_TOTAL_LOGICAL_PAYLOAD_BYTES || *total_bytes > average_budget {
                diagnostics.push(Diagnostic::error(
                    "fixture_payload_budget_exceeded",
                    "logical.data.payload.total_bytes",
                    format!(
                        "total logical payload bytes must be <= {MAX_TOTAL_LOGICAL_PAYLOAD_BYTES} and average <= {MAX_PAYLOAD_BYTES_PER_ROW} bytes per row"
                    ),
                ));
            }
        }
    }
}

fn validate_table_inventory(
    tables: &mut [GraphTableCountV1],
    kind: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<u64> {
    let base = format!("logical.data.{kind}_tables");
    if tables.is_empty() {
        diagnostics.push(Diagnostic::error(
            "empty_fixture_graph_inventory",
            &base,
            format!("a real graph fixture must declare at least one {kind} type"),
        ));
    }
    let mut names = BTreeSet::new();
    let mut total = Some(0u64);
    for (index, table) in tables.iter().enumerate() {
        if !valid_type_name(&table.name) {
            diagnostics.push(Diagnostic::error(
                "invalid_fixture_graph_type",
                format!("{base}[{index}].name"),
                "graph type names must match OmniGraph's uppercase ASCII type grammar",
            ));
        }
        if table.rows > MAX_ROWS_PER_GRAPH_TYPE {
            diagnostics.push(Diagnostic::error(
                "fixture_table_row_budget_exceeded",
                format!("{base}[{index}].rows"),
                format!("rows per graph type must be <= {MAX_ROWS_PER_GRAPH_TYPE}"),
            ));
        }
        if !names.insert(table.name.as_str()) {
            diagnostics.push(Diagnostic::error(
                "duplicate_fixture_graph_type",
                format!("{base}[{index}].name"),
                format!("{kind} type '{}' appears more than once", table.name),
            ));
        }
        total = total.and_then(|current| current.checked_add(table.rows));
    }
    if total == Some(0) {
        diagnostics.push(Diagnostic::error(
            "empty_fixture_graph_rows",
            &base,
            format!("declared {kind} types must contain at least one row in aggregate"),
        ));
    }
    if total.is_none() {
        diagnostics.push(Diagnostic::error(
            "fixture_row_count_overflow",
            &base,
            format!("total {kind} row count overflows u64"),
        ));
    }
    tables.sort();
    total
}

fn validate_state(
    state: &mut RealGraphStateV1,
    data: &RealGraphDataV1,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if !(1..=MAX_HISTORY_DEPTH).contains(&state.history_depth) {
        diagnostics.push(Diagnostic::error(
            "invalid_fixture_history_depth",
            "logical.state.history_depth",
            format!("history depth must be in 1..={MAX_HISTORY_DEPTH}"),
        ));
    }
    if state.indexes.len() > MAX_INDEXES {
        diagnostics.push(Diagnostic::error(
            "fixture_index_budget_exceeded",
            "logical.state.indexes",
            format!("at most {MAX_INDEXES} index declarations are allowed"),
        ));
    }
    let tables = data
        .node_tables
        .iter()
        .map(|table| format!("node:{}", table.name))
        .chain(
            data.edge_tables
                .iter()
                .map(|table| format!("edge:{}", table.name)),
        )
        .collect::<BTreeSet<_>>();
    let mut indexes = BTreeSet::new();
    for (index, entry) in state.indexes.iter().enumerate() {
        if !tables.contains(&entry.table) {
            diagnostics.push(Diagnostic::error(
                "unknown_fixture_index_table",
                format!("logical.state.indexes[{index}].table"),
                "index table must exactly name a declared `node:Type` or `edge:Type`",
            ));
        }
        if !valid_property_name(&entry.column) {
            diagnostics.push(Diagnostic::error(
                "invalid_fixture_index_column",
                format!("logical.state.indexes[{index}].column"),
                "index columns must match OmniGraph's lowercase ASCII property grammar",
            ));
        }
        if !indexes.insert((entry.table.as_str(), entry.column.as_str(), entry.kind)) {
            diagnostics.push(Diagnostic::error(
                "duplicate_fixture_index",
                format!("logical.state.indexes[{index}]"),
                "the same table, column, and kind appears more than once; freshness is one state",
            ));
        }
        if matches!(
            data.column_shape,
            RealGraphColumnShape::Scalars | RealGraphColumnShape::ScalarsBlob
        ) && entry.kind == RealGraphIndexKindV1::Ann
        {
            diagnostics.push(Diagnostic::error(
                "impossible_fixture_index_inventory",
                format!("logical.state.indexes[{index}].kind"),
                "ANN indexes require a column shape that includes vectors",
            ));
        }
    }
    state.indexes.sort();
}

fn validate_kebab_id(value: &str, path: &str, code: &str, diagnostics: &mut Vec<Diagnostic>) {
    if !valid_kebab_id(value) || value.len() > MAX_ID_BYTES {
        diagnostics.push(Diagnostic::error(
            code,
            path,
            "value must be 1..=128 characters of path-free kebab-case ASCII",
        ));
    }
}

fn validate_sha256(value: &str, path: &str, diagnostics: &mut Vec<Diagnostic>) {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        diagnostics.push(Diagnostic::error(
            "invalid_fixture_sha256",
            path,
            "SHA-256 must contain exactly 64 lowercase hexadecimal characters",
        ));
    }
}

fn validate_digest_reference(
    reference: &DigestReferenceV1,
    path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    validate_algorithm_id(
        &reference.algorithm,
        &format!("{path}.algorithm"),
        diagnostics,
    );
    validate_sha256(&reference.sha256, &format!("{path}.sha256"), diagnostics);
}

fn validate_algorithm_id(value: &str, path: &str, diagnostics: &mut Vec<Diagnostic>) {
    validate_kebab_id(value, path, "invalid_fixture_digest_algorithm", diagnostics);
    let version_is_canonical = value.rsplit_once("-v").is_some_and(|(name, version)| {
        !name.is_empty()
            && !version.is_empty()
            && !version.starts_with('0')
            && version.bytes().all(|byte| byte.is_ascii_digit())
            && version.parse::<u32>().is_ok()
    });
    if !version_is_canonical {
        diagnostics.push(Diagnostic::error(
            "unversioned_fixture_digest_algorithm",
            path,
            "digest algorithm ids must end in a canonical positive `-vN` version",
        ));
    }
}

fn valid_type_name(value: &str) -> bool {
    valid_ascii_identifier(value, |byte| byte.is_ascii_uppercase())
}

fn valid_property_name(value: &str) -> bool {
    valid_ascii_identifier(value, |byte| byte.is_ascii_lowercase() || byte == b'_')
}

fn valid_ascii_identifier(value: &str, valid_first: impl Fn(u8) -> bool) -> bool {
    if value.is_empty() || value.len() > MAX_ID_BYTES {
        return false;
    }
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    valid_first(first) && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_yaml() -> String {
        format!(
            r#"version: 1
fixture_id: finbench-sf10-main
logical:
  builder:
    id: finbench-import
    version: 1
    recipe_sha256: "{}"
    parameters:
      - {{ name: scale-factor, value: 10 }}
      - {{ name: optimize-every, value: 200 }}
    inputs:
      - role: source-snapshot
        sha256: "{}"
      - role: labels
        sha256: "{}"
  data:
    provenance: corpus-derived
    schema_shape:
      algorithm: future-schema-shape-v1
      sha256: "{}"
    node_tables:
      - {{ name: Person, rows: 582672 }}
      - {{ name: Account, rows: 1534180 }}
    edge_tables:
      - {{ name: PersonOwnAccount, rows: 1022914 }}
      - {{ name: AccountTransferAccount, rows: 6219881 }}
    payload:
      kind: variable
      algorithm: future-logical-payload-v1
      total_bytes: 4096
    column_shape: scalars
    topology_skew: source-defined
  state:
    aging: small-commits
    indexes:
      - table: node:Account
        column: accountId
        kind: btree
        freshness: optimized
      - table: node:Person
        column: name
        kind: fts
        freshness: rows-stale
    deletion_history: none
    compaction_recency: optimized
    history_depth: 3972
expected:
  logical_content:
    algorithm: future-logical-graph-v1
    sha256: "{}"
"#,
            "1".repeat(64),
            "2".repeat(64),
            "5".repeat(64),
            "3".repeat(64),
            "4".repeat(64),
        )
    }

    #[test]
    fn strict_reference_normalizes_every_order_free_inventory() {
        let first = parse_fixture_reference(&reference_yaml())
            .into_result()
            .unwrap();
        assert_eq!(first.definition.logical.data.node_tables[0].name, "Account");
        assert_eq!(
            first.definition.logical.builder.parameters[0].name,
            "optimize-every"
        );
        assert_eq!(first.reference_sha256.len(), 64);

        let reordered = reference_yaml()
            .replace(
                "      - { name: scale-factor, value: 10 }\n      - { name: optimize-every, value: 200 }",
                "      - { name: optimize-every, value: 200 }\n      - { name: scale-factor, value: 10 }",
            )
            .replace(
                "      - { name: Person, rows: 582672 }\n      - { name: Account, rows: 1534180 }",
                "      - { name: Account, rows: 1534180 }\n      - { name: Person, rows: 582672 }",
            )
            .replace(
                &format!(
                    "      - role: source-snapshot\n        sha256: \"{}\"\n      - role: labels\n        sha256: \"{}\"",
                    "2".repeat(64),
                    "5".repeat(64)
                ),
                &format!(
                    "      - role: labels\n        sha256: \"{}\"\n      - role: source-snapshot\n        sha256: \"{}\"",
                    "5".repeat(64),
                    "2".repeat(64)
                ),
            )
            .replace(
                "      - { name: PersonOwnAccount, rows: 1022914 }\n      - { name: AccountTransferAccount, rows: 6219881 }",
                "      - { name: AccountTransferAccount, rows: 6219881 }\n      - { name: PersonOwnAccount, rows: 1022914 }",
            )
            .replace(
                "      - table: node:Account\n        column: accountId\n        kind: btree\n        freshness: optimized\n      - table: node:Person\n        column: name\n        kind: fts\n        freshness: rows-stale",
                "      - table: node:Person\n        column: name\n        kind: fts\n        freshness: rows-stale\n      - table: node:Account\n        column: accountId\n        kind: btree\n        freshness: optimized",
            );
        assert_eq!(
            first,
            parse_fixture_reference(&reordered).into_result().unwrap()
        );
    }

    #[test]
    fn document_shape_does_not_accept_physical_facts_or_missing_expectation() {
        let physical = reference_yaml().replace(
            "fixture_id: finbench-sf10-main",
            "fixture_id: finbench-sf10-main\nsource_uri: s3://bucket/root",
        );
        assert!(!parse_fixture_reference(&physical).ok);

        let missing =
            reference_yaml().replace(&format!("    sha256: \"{}\"\n", "4".repeat(64)), "");
        assert!(!parse_fixture_reference(&missing).ok);

        let string_parameter = reference_yaml().replace("value: 10", "value: /tmp/graph");
        assert!(!parse_fixture_reference(&string_parameter).ok);

        let duplicate_key = reference_yaml().replace(
            "fixture_id: finbench-sf10-main",
            "fixture_id: finbench-sf10-main\nfixture_id: duplicate",
        );
        assert!(!parse_fixture_reference(&duplicate_key).ok);

        let cross_variant_field = reference_yaml().replace(
            "total_bytes: 4096",
            "total_bytes: 4096\n      bytes_per_row: 1",
        );
        assert!(!parse_fixture_reference(&cross_variant_field).ok);
    }

    #[test]
    fn duplicate_and_impossible_identity_entries_fail_closed() {
        let duplicate_parameter = reference_yaml().replace(
            "      - { name: optimize-every, value: 200 }",
            "      - { name: scale-factor, value: 99 }",
        );
        let duplicate_index = reference_yaml().replace(
            "    deletion_history: none",
            "      - table: node:Account\n        column: accountId\n        kind: btree\n        freshness: rows-stale\n    deletion_history: none",
        );
        let scalar_ann = reference_yaml().replace("kind: btree", "kind: ann");
        for (source, expected_code) in [
            (duplicate_parameter, "duplicate_fixture_builder_parameter"),
            (duplicate_index, "duplicate_fixture_index"),
            (scalar_ann, "impossible_fixture_index_inventory"),
        ] {
            assert!(
                parse_fixture_reference(&source)
                    .diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.code == expected_code),
                "missing {expected_code}"
            );
        }
    }

    #[test]
    fn grammar_versions_and_resource_bounds_are_checked() {
        let semantic = reference_yaml()
            .replace("name: Person", "name: person")
            .replace("column: accountId", "column: AccountId")
            .replace("rows: 582672", "rows: 1000000000001")
            .replace("total_bytes: 4096", "total_bytes: 1152921504606846977");
        let codes = parse_fixture_reference(&semantic)
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect::<BTreeSet<_>>();
        assert!(codes.contains("invalid_fixture_graph_type"));
        assert!(codes.contains("invalid_fixture_index_column"));
        assert!(codes.contains("fixture_table_row_budget_exceeded"));
        assert!(codes.contains("fixture_payload_budget_exceeded"));

        let oversized_fixed = reference_yaml()
            .replace("rows: 582672", "rows: 1000000000000")
            .replace("rows: 1534180", "rows: 1000000000000")
            .replace("rows: 1022914", "rows: 1000000000000")
            .replace("rows: 6219881", "rows: 1000000000000")
            .replace(
                "kind: variable\n      algorithm: future-logical-payload-v1\n      total_bytes: 4096",
                "kind: fixed\n      algorithm: future-logical-payload-v1\n      bytes_per_row: 67108864",
            );
        assert!(
            parse_fixture_reference(&oversized_fixed)
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "fixture_payload_budget_exceeded")
        );

        let oversized_variable_average = reference_yaml()
            .replace("rows: 582672", "rows: 1")
            .replace("rows: 1534180", "rows: 1")
            .replace("rows: 1022914", "rows: 1")
            .replace("rows: 6219881", "rows: 1")
            .replace("total_bytes: 4096", "total_bytes: 268435457");
        assert!(
            parse_fixture_reference(&oversized_variable_average)
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "fixture_payload_budget_exceeded")
        );

        for invalid in ["schema-shape", "schema-shape-v0", "schema-shape-v01"] {
            let unversioned_algorithm = reference_yaml().replace(
                "algorithm: future-schema-shape-v1",
                &format!("algorithm: {invalid}"),
            );
            assert!(
                parse_fixture_reference(&unversioned_algorithm)
                    .diagnostics
                    .iter()
                    .any(|diagnostic| {
                        diagnostic.code == "unversioned_fixture_digest_algorithm"
                    }),
                "accepted non-canonical algorithm id {invalid}"
            );
        }

        assert_eq!(
            parse_fixture_reference(&reference_yaml().replace("version: 1", "version: 2"))
                .diagnostics[0]
                .code,
            "unsupported_fixture_reference_version"
        );
    }
}
