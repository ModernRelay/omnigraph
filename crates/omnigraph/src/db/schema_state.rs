use std::collections::{BTreeMap, BTreeSet};

use omnigraph_compiler::schema::parser::parse_persisted_schema_contract;
use omnigraph_compiler::{
    SchemaIR, SchemaIdentityDomain, SchemaShape, compile_schema_shape, schema_ir_hash,
    schema_ir_pretty_json, schema_shape_hash, schema_shape_hash_from_ir, validate_schema_ir,
};
use serde::Deserialize;

use crate::db::Snapshot;
use crate::db::manifest::{
    SchemaContractHead, SchemaContractRow, TableIdentity, table_path_for_identity,
};
use crate::error::{OmniError, Result};

pub(crate) const SCHEMA_IDENTITY_VERSION: u32 = 2;

/// Refuse a newer schema contract before open builds anything from it. Only
/// the version envelope of the `schema_contract` row's IR text is inspected;
/// a partial or malformed row remains subject to the complete row validation.
pub(crate) fn refuse_unsupported_schema_versions(live_ir: &str) -> Result<()> {
    #[derive(Deserialize)]
    struct VersionEnvelope {
        ir_version: u32,
    }

    #[derive(Deserialize)]
    struct FeatureEnvelope {
        #[serde(default)]
        features: BTreeSet<String>,
    }

    let Ok(envelope) = serde_json::from_str::<VersionEnvelope>(live_ir) else {
        return Ok(());
    };
    if !omnigraph_compiler::is_supported_ir_version(envelope.ir_version) {
        return Err(schema_lock_conflict(format!(
            "unsupported ir_version {} in the schema_contract row (supported {}, {} and {}); open will not recover or migrate this schema",
            envelope.ir_version,
            omnigraph_compiler::SCHEMA_IR_VERSION,
            omnigraph_compiler::SCHEMA_IR_VERSION_EDGE_KEYS,
            omnigraph_compiler::SCHEMA_IR_VERSION_FEATURES,
        )));
    }
    let Ok(envelope) = serde_json::from_str::<FeatureEnvelope>(live_ir) else {
        return Ok(());
    };
    if let Some(unknown) = envelope
        .features
        .iter()
        .find(|name| !omnigraph_compiler::is_known_feature(name))
    {
        return Err(schema_lock_conflict(format!(
            "schema feature '{unknown}' in the schema_contract row is unknown to this build; upgrade omnigraph before opening this graph; open will not recover or migrate this schema"
        )));
    }
    Ok(())
}

/// The small fields of the `schema_contract` row of one `__manifest` version,
/// as the engine `Snapshot` exposes them: the identity every capture compares
/// and the key of `AcceptedCatalogMemo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaContractIdentity {
    pub(crate) schema_ir_hash: String,
    pub(crate) schema_identity_version: u32,
    pub(crate) schema_identity_domain: String,
}

impl From<&SchemaContractHead> for SchemaContractIdentity {
    fn from(head: &SchemaContractHead) -> Self {
        Self {
            schema_ir_hash: head.schema_ir_hash.clone(),
            schema_identity_version: head.schema_identity_version,
            schema_identity_domain: head.schema_identity_domain.clone(),
        }
    }
}

impl SchemaContractIdentity {
    /// The identity a contract writer records for `schema_ir`: the IR
    /// validated, hashed, and stamped with this build's identity version.
    pub(crate) fn from_ir(schema_ir: &SchemaIR) -> Result<Self> {
        validate_schema_ir(schema_ir).map_err(|error| schema_lock_conflict(error.to_string()))?;
        Ok(Self {
            schema_ir_hash: schema_ir_hash(schema_ir)
                .map_err(|error| schema_lock_conflict(error.to_string()))?,
            schema_identity_version: SCHEMA_IDENTITY_VERSION,
            schema_identity_domain: schema_ir.schema_identity_domain.as_str().to_string(),
        })
    }

    fn head(&self) -> SchemaContractHead {
        SchemaContractHead {
            schema_ir_hash: self.schema_ir_hash.clone(),
            schema_identity_version: self.schema_identity_version,
            schema_identity_domain: self.schema_identity_domain.clone(),
        }
    }
}

/// The identity of `snapshot`'s `schema_contract` row; a served version
/// always carries one (the stamp guard refuses older layouts at open).
pub(crate) fn snapshot_contract_identity(snapshot: &Snapshot) -> Result<SchemaContractIdentity> {
    snapshot.schema_contract().ok_or_else(|| {
        schema_lock_conflict(format!(
            "__manifest version {} carries no schema_contract row",
            snapshot.graph_manifest_version()
        ))
    })
}

/// The `schema_contract` row a publish writes for `schema_ir` and the `.pg`
/// text it was resolved from: the IR rendered as its exact stored text and
/// the identity of that IR.
pub(crate) fn render_schema_contract(
    schema_ir: &SchemaIR,
    source: &str,
) -> Result<SchemaContractRow> {
    let ir = schema_ir_pretty_json(schema_ir)
        .map_err(|err| OmniError::manifest_internal(err.to_string()))?;
    let identity = SchemaContractIdentity::from_ir(schema_ir)?;
    Ok(SchemaContractRow {
        source: source.to_string(),
        ir,
        head: identity.head(),
    })
}

/// Validate one `schema_contract` row: the IR parses as a valid
/// identity-bearing SchemaIR whose hash is the row's `schema_ir_hash`, the
/// identity envelope is this build's, and the source compiles to the IR's
/// semantic shape.
pub(crate) fn validate_schema_contract_row(
    row: &SchemaContractRow,
) -> Result<(SchemaIR, SchemaContractIdentity)> {
    let current_source_shape = compile_schema_source(&row.source)?;
    let ir = serde_json::from_str::<SchemaIR>(&row.ir).map_err(|err| {
        schema_lock_conflict(format!(
            "accepted compiled schema contract in the schema_contract row is invalid: {err}"
        ))
    })?;
    let identity = SchemaContractIdentity::from_ir(&ir)?;
    validate_identity_envelope(&row.head)?;
    if identity.schema_ir_hash != row.head.schema_ir_hash {
        return Err(schema_lock_conflict(format!(
            "schema_contract row schema_ir_hash {} is not the hash of its IR text ({}); accepted compiled schema does not match the recorded schema state",
            row.head.schema_ir_hash, identity.schema_ir_hash
        )));
    }
    if identity.schema_identity_domain != row.head.schema_identity_domain {
        return Err(schema_lock_conflict(
            "accepted compiled schema identity domain does not match the recorded schema state",
        ));
    }
    validate_current_source_matches(&ir, &current_source_shape)?;
    Ok((ir, identity))
}

/// Prove that one accepted identity-bearing SchemaIR and one live manifest
/// snapshot describe exactly the same physical table lifetimes.
///
/// Names are aliases, not identity. A same-name drop/re-add therefore fails
/// unless the manifest carries the IR's new stable type ID, incarnation, and
/// canonical identity-derived path. The reverse scan rejects orphan manifest
/// registrations that no longer exist in the accepted IR.
pub(crate) fn validate_schema_ir_against_snapshot(
    schema_ir: &SchemaIR,
    snapshot: &Snapshot,
) -> Result<()> {
    validate_schema_ir_against_entries(
        schema_ir,
        snapshot.datasets(),
        snapshot.graph_manifest_version(),
    )
}

pub(crate) fn validate_schema_ir_against_entries<'a>(
    schema_ir: &SchemaIR,
    entries: impl Iterator<Item = &'a crate::db::manifest::DatasetEntry>,
    manifest_version: u64,
) -> Result<()> {
    validate_schema_ir(schema_ir).map_err(|error| {
        schema_manifest_conflict(format!("accepted SchemaIR is invalid: {error}"))
    })?;

    let mut expected_by_alias = BTreeMap::<String, (TableIdentity, String)>::new();
    let mut expected_by_identity = BTreeMap::<TableIdentity, String>::new();
    for (kind, name, stable_type_id, table_incarnation_id) in schema_ir
        .nodes
        .iter()
        .map(|node| {
            (
                "node",
                node.name.as_str(),
                node.type_id.get(),
                node.table_incarnation_id.get(),
            )
        })
        .chain(schema_ir.edges.iter().map(|edge| {
            (
                "edge",
                edge.name.as_str(),
                edge.type_id.get(),
                edge.table_incarnation_id.get(),
            )
        }))
    {
        let table_key = format!("{kind}:{name}");
        let identity = TableIdentity::new(stable_type_id, table_incarnation_id)
            .map_err(|error| schema_manifest_conflict(error.to_string()))?;
        let table_path = table_path_for_identity(&table_key, identity)
            .map_err(|error| schema_manifest_conflict(error.to_string()))?;
        if let Some(previous) = expected_by_identity.insert(identity, table_key.clone()) {
            return Err(schema_manifest_conflict(format!(
                "SchemaIR aliases '{previous}' and '{table_key}' reuse table identity {identity}"
            )));
        }
        if expected_by_alias
            .insert(table_key.clone(), (identity, table_path))
            .is_some()
        {
            return Err(schema_manifest_conflict(format!(
                "SchemaIR contains duplicate live table alias '{table_key}'"
            )));
        }
    }

    let mut manifest_by_identity = BTreeMap::<TableIdentity, String>::new();
    for entry in entries {
        let Some((expected_identity, expected_path)) = expected_by_alias.get(&entry.type_key)
        else {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} contains live table '{}' that is absent from accepted SchemaIR",
                manifest_version, entry.type_key
            )));
        };
        if entry.identity != *expected_identity {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} table '{}' has identity {}, but accepted SchemaIR requires {}",
                manifest_version, entry.type_key, entry.identity, expected_identity
            )));
        }
        if entry.dataset_path != *expected_path {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} table '{}' has non-canonical path '{}', expected '{}' for identity {}",
                manifest_version,
                entry.type_key,
                entry.dataset_path,
                expected_path,
                expected_identity
            )));
        }
        if let Some(previous) = manifest_by_identity.insert(entry.identity, entry.type_key.clone())
        {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} aliases '{previous}' and '{}' to the same table identity {}",
                manifest_version, entry.type_key, entry.identity
            )));
        }
    }

    for (table_key, (identity, _)) in expected_by_alias {
        if !manifest_by_identity.contains_key(&identity) {
            return Err(schema_manifest_conflict(format!(
                "accepted SchemaIR table '{table_key}' with identity {identity} is missing from manifest v{}",
                manifest_version
            )));
        }
    }
    Ok(())
}

/// The identity envelope every served contract carries: this build's
/// identity version and a well-formed identity domain.
pub(crate) fn validate_identity_envelope(head: &SchemaContractHead) -> Result<()> {
    if head.schema_identity_version != SCHEMA_IDENTITY_VERSION {
        return Err(schema_lock_conflict(format!(
            "graph schema identity version {} is unsupported",
            head.schema_identity_version
        )));
    }
    SchemaIdentityDomain::parse(&head.schema_identity_domain).map_err(|error| {
        schema_lock_conflict(format!("graph schema identity domain is invalid: {error}"))
    })?;
    Ok(())
}

/// The `.pg` source must compile to the semantic shape of the accepted IR,
/// canonicalized at the IR's system-column vintage.
pub(crate) fn validate_current_source_matches(
    ir: &SchemaIR,
    current_source_shape: &SchemaShape,
) -> Result<()> {
    let current_source_shape = current_source_shape.canonicalized_for_system_columns(
        omnigraph_compiler::system_columns_for_features(&ir.features),
    );
    let current_hash = schema_shape_hash(&current_source_shape)
        .map_err(|err| schema_lock_conflict(err.to_string()))?;
    let accepted_hash =
        schema_shape_hash_from_ir(ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
    if current_hash != accepted_hash {
        return Err(schema_lock_conflict(
            "schema-contract source no longer matches the accepted compiled schema",
        ));
    }
    Ok(())
}

/// Compile a persisted `.pg` source with the compatibility parser, so a root
/// admitted before v0.10 with body-level @unique(Blob) remains openable; init
/// and the desired side of a schema apply use the strict parser.
pub(crate) fn compile_schema_source(source: &str) -> Result<SchemaShape> {
    let schema = parse_persisted_schema_contract(source).map_err(|err| {
        schema_lock_conflict(format!(
            "schema-contract source is not a valid accepted schema definition: {}",
            err
        ))
    })?;
    compile_schema_shape(&schema).map_err(|err| {
        schema_lock_conflict(format!(
            "schema-contract source could not be compiled into the accepted schema shape: {}",
            err
        ))
    })
}

pub(crate) fn schema_lock_conflict(detail: impl Into<String>) -> OmniError {
    OmniError::manifest_conflict(format!(
        "schema evolution is locked down in phase 1: {}; manual coordination is required",
        detail.into()
    ))
}

fn schema_manifest_conflict(detail: impl Into<String>) -> OmniError {
    OmniError::manifest_conflict(format!(
        "accepted schema/manifest identity mismatch: {}",
        detail.into()
    ))
}
