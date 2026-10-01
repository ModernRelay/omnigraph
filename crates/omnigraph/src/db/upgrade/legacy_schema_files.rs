//! The three root schema files a graph carried before the contract lived in
//! main's `__manifest` (`schema_contract` row, internal schema v13): read by
//! the storage-upgrade route over pre-row roots and by nothing served.
//! Conversion removes these files only after every branch carries its contract.

use std::sync::Arc;

use omnigraph_compiler::{SchemaIR, schema_ir_hash, schema_shape_hash_from_ir, validate_schema_ir};
use serde::Deserialize;

use crate::db::manifest::{SchemaContractHead, SchemaContractRow};
use crate::db::schema_state::{
    SchemaContractIdentity, compile_schema_source, schema_lock_conflict,
    validate_current_source_matches, validate_identity_envelope,
};
use crate::error::{OmniError, Result};
use crate::storage::{StorageAdapter, join_uri};

pub(crate) const SCHEMA_SOURCE_FILENAME: &str = "_schema.pg";
pub(crate) const SCHEMA_IR_FILENAME: &str = "_schema.ir.json";
pub(crate) const SCHEMA_STATE_FILENAME: &str = "__schema_state.json";

const SCHEMA_STATE_FORMAT_VERSION: u32 = 2;

const MISSING_SCHEMA_CONTRACT_MESSAGE: &str = "graph is missing the mandatory identity-bearing schema contract (_schema.ir.json and __schema_state.json); automatic bootstrap is not supported";
const INCOMPLETE_SCHEMA_CONTRACT_MESSAGE: &str = "graph schema contract is incomplete: _schema.ir.json and __schema_state.json must both be present";

pub(crate) fn schema_source_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_SOURCE_FILENAME)
}

pub(crate) fn schema_ir_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_IR_FILENAME)
}

pub(crate) fn schema_state_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_STATE_FILENAME)
}

/// The identity fields of `__schema_state.json`. Completed legacy applies
/// can retain a `publication` receipt; conversion ignores that retired field.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacySchemaState {
    format_version: u32,
    schema_shape_hash: String,
    schema_ir_hash: String,
    schema_identity_version: u32,
    schema_identity_domain: String,
}

/// A pre-row root's contract as its three files carry it, validated: the
/// texts the upgrade route writes into the `schema_contract` row and the
/// identity that row records.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LegacySchemaContract {
    pub(crate) source: String,
    pub(crate) ir_json: String,
    pub(crate) ir: SchemaIR,
    pub(crate) identity: SchemaContractIdentity,
}

impl LegacySchemaContract {
    pub(super) fn into_row(self) -> SchemaContractRow {
        SchemaContractRow {
            source: self.source,
            ir: self.ir_json,
            head: SchemaContractHead {
                schema_ir_hash: self.identity.schema_ir_hash,
                schema_identity_version: self.identity.schema_identity_version,
                schema_identity_domain: self.identity.schema_identity_domain,
            },
        }
    }
}

pub(super) async fn refuse_staging(root: &str, storage: &dyn StorageAdapter) -> Result<()> {
    for name in [
        SCHEMA_SOURCE_FILENAME,
        SCHEMA_IR_FILENAME,
        SCHEMA_STATE_FILENAME,
    ] {
        if storage
            .exists(&join_uri(root, &format!("{name}.staging")))
            .await?
        {
            return Err(schema_lock_conflict(
                "legacy schema staging requires the source-compatible executable before conversion",
            ));
        }
    }
    Ok(())
}

/// Main's converted row survives interruption after any subset of these deletes.
pub(super) async fn cleanup(
    root: &str,
    storage: &dyn StorageAdapter,
    contract: &SchemaContractRow,
) -> Result<()> {
    let (ir, _) = crate::db::schema_state::validate_schema_contract_row(contract)?;
    for name in [
        SCHEMA_SOURCE_FILENAME,
        SCHEMA_IR_FILENAME,
        SCHEMA_STATE_FILENAME,
    ] {
        let path = join_uri(root, name);
        if !storage.exists(&path).await? {
            continue;
        }
        let text = storage.read_text(&path).await?;
        let matches = match name {
            SCHEMA_SOURCE_FILENAME => text == contract.source,
            SCHEMA_IR_FILENAME => text == contract.ir,
            _ => {
                let state: LegacySchemaState = serde_json::from_str(&text)
                    .map_err(|error| invalid_contract_file("graph schema state", name, error))?;
                state.format_version == SCHEMA_STATE_FORMAT_VERSION
                    && state.schema_ir_hash == contract.head.schema_ir_hash
                    && state.schema_identity_version == contract.head.schema_identity_version
                    && state.schema_identity_domain == contract.head.schema_identity_domain
                    && state.schema_shape_hash
                        == schema_shape_hash_from_ir(&ir)
                            .map_err(|error| schema_lock_conflict(error.to_string()))?
            }
        };
        if !matches {
            return Err(schema_lock_conflict(format!(
                "legacy contract file {name} changed before cleanup"
            )));
        }
        storage.delete(&path).await?;
        crate::seams::fail(&super::UPGRADE_AFTER_SCHEMA_FILE_DELETE)?;
    }
    Ok(())
}

/// Load the accepted IR and its schema identity from the three legacy root
/// files (three `read_text`, two `exists`). A missing or incomplete contract
/// reports an unparseable source first, and a storage error before a parse
/// error.
pub(crate) async fn load_validated_schema_contract(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
) -> Result<LegacySchemaContract> {
    let storage = storage.as_ref();
    let source = storage.read_text(&schema_source_uri(root_uri)).await?;
    let ir_uri = schema_ir_uri(root_uri);
    let state_uri = schema_state_uri(root_uri);
    let ir_exists = storage.exists(&ir_uri).await?;
    let state_exists = storage.exists(&state_uri).await?;
    let (ir_json, state_json) = match (ir_exists, state_exists) {
        (true, true) => (
            storage.read_text(&ir_uri).await?,
            storage.read_text(&state_uri).await?,
        ),
        (false, false) => {
            compile_schema_source(&source)?;
            return Err(schema_lock_conflict(MISSING_SCHEMA_CONTRACT_MESSAGE));
        }
        _ => {
            compile_schema_source(&source)?;
            return Err(schema_lock_conflict(INCOMPLETE_SCHEMA_CONTRACT_MESSAGE));
        }
    };
    let current_source_shape = compile_schema_source(&source)?;
    let ir = serde_json::from_str::<SchemaIR>(&ir_json).map_err(|err| {
        invalid_contract_file("accepted compiled schema contract", SCHEMA_IR_FILENAME, err)
    })?;
    let state = serde_json::from_str::<LegacySchemaState>(&state_json)
        .map_err(|err| invalid_contract_file("graph schema state", SCHEMA_STATE_FILENAME, err))?;
    if state.format_version != SCHEMA_STATE_FORMAT_VERSION {
        return Err(schema_lock_conflict(format!(
            "graph schema state format {} is unsupported",
            state.format_version
        )));
    }
    let head = SchemaContractHead {
        schema_ir_hash: state.schema_ir_hash.clone(),
        schema_identity_version: state.schema_identity_version,
        schema_identity_domain: state.schema_identity_domain.clone(),
    };
    validate_identity_envelope(&head)?;
    validate_schema_ir(&ir).map_err(|error| {
        schema_lock_conflict(format!(
            "accepted compiled schema is not a valid identity-bearing IR: {error}"
        ))
    })?;
    let actual_hash = schema_ir_hash(&ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
    if actual_hash != state.schema_ir_hash {
        return Err(schema_lock_conflict(
            "accepted compiled schema does not match the recorded schema state",
        ));
    }
    let projected_shape_hash =
        schema_shape_hash_from_ir(&ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
    if projected_shape_hash != state.schema_shape_hash {
        return Err(schema_lock_conflict(
            "accepted compiled schema's semantic projection does not match the recorded schema shape",
        ));
    }
    if ir.schema_identity_domain.as_str() != state.schema_identity_domain {
        return Err(schema_lock_conflict(
            "accepted compiled schema identity domain does not match the recorded schema state",
        ));
    }
    validate_current_source_matches(&ir, &current_source_shape)?;
    Ok(LegacySchemaContract {
        source,
        ir_json,
        ir,
        identity: SchemaContractIdentity::from(&head),
    })
}

fn invalid_contract_file(subject: &str, file: &str, err: impl std::fmt::Display) -> OmniError {
    schema_lock_conflict(format!("{subject} in {file} is invalid: {err}"))
}
