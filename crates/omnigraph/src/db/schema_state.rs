use std::sync::Arc;

use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{SchemaIR, build_schema_ir, schema_ir_hash, schema_ir_pretty_json};
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};
use crate::storage::{StorageAdapter, join_uri};

pub(crate) const SCHEMA_SOURCE_FILENAME: &str = "_schema.pg";
pub(crate) const SCHEMA_IR_FILENAME: &str = "_schema.ir.json";
pub(crate) const SCHEMA_STATE_FILENAME: &str = "__schema_state.json";

const SCHEMA_STATE_FORMAT_VERSION: u32 = 1;
const SCHEMA_IDENTITY_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchemaState {
    pub(crate) format_version: u32,
    pub(crate) schema_ir_hash: String,
    pub(crate) schema_identity_version: u32,
}

impl SchemaState {
    pub(crate) fn new(schema_ir_hash: String) -> Self {
        Self {
            format_version: SCHEMA_STATE_FORMAT_VERSION,
            schema_ir_hash,
            schema_identity_version: SCHEMA_IDENTITY_VERSION,
        }
    }
}

pub(crate) async fn load_or_bootstrap_schema_contract(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
    public_branches: &[String],
    current_source_ir: &SchemaIR,
) -> Result<(SchemaIR, SchemaState)> {
    match read_schema_contract(root_uri, storage.as_ref()).await? {
        SchemaContractRead::Present { ir, state } => {
            validate_persisted_schema_contract(&ir, &state)?;
            validate_current_source_matches(&state, current_source_ir)?;
            Ok((ir, state))
        }
        SchemaContractRead::MissingAll => {
            let public_non_main = public_branches
                .iter()
                .filter(|branch| branch.as_str() != "main")
                .cloned()
                .collect::<Vec<_>>();
            if !public_non_main.is_empty() {
                return Err(schema_lock_conflict(format!(
                    "repo is missing persisted schema state and has public branches ({}); public branches block schema evolution entirely",
                    public_non_main.join(", ")
                )));
            }
            let state =
                write_schema_contract(root_uri, storage.as_ref(), current_source_ir).await?;
            Ok((current_source_ir.clone(), state))
        }
        SchemaContractRead::PartialMissing => Err(schema_lock_conflict(
            "repo schema state is incomplete (_schema.ir.json and __schema_state.json must either both exist or both be absent)",
        )),
    }
}

pub(crate) async fn validate_schema_contract(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
) -> Result<()> {
    let current_source_ir = read_current_source_ir(root_uri, storage.as_ref()).await?;
    let (persisted_ir, state) = match read_schema_contract(root_uri, storage.as_ref()).await? {
        SchemaContractRead::Present { ir, state } => (ir, state),
        SchemaContractRead::MissingAll | SchemaContractRead::PartialMissing => {
            return Err(schema_lock_conflict(
                "repo is missing persisted schema state; manual coordination is required before schema changes are allowed",
            ));
        }
    };

    validate_persisted_schema_contract(&persisted_ir, &state)?;
    validate_current_source_matches(&state, &current_source_ir)
}

pub(crate) async fn write_schema_contract(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    schema_ir: &SchemaIR,
) -> Result<SchemaState> {
    let ir_json = schema_ir_pretty_json(schema_ir)
        .map_err(|err| OmniError::manifest_internal(err.to_string()))?;
    let state = SchemaState::new(
        schema_ir_hash(schema_ir).map_err(|err| OmniError::manifest_internal(err.to_string()))?,
    );
    let state_json = serde_json::to_string_pretty(&state).map_err(|err| {
        OmniError::manifest_internal(format!("serialize schema state error: {}", err))
    })?;

    storage
        .write_text(&schema_ir_uri(root_uri), &ir_json)
        .await?;
    storage
        .write_text(&schema_state_uri(root_uri), &state_json)
        .await?;
    Ok(state)
}

pub(crate) async fn read_current_source_ir(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<SchemaIR> {
    let source = storage.read_text(&schema_source_uri(root_uri)).await?;
    compile_schema_source(&source)
}

pub(crate) async fn read_accepted_schema_ir(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
) -> Result<SchemaIR> {
    match read_schema_contract(root_uri, storage.as_ref()).await? {
        SchemaContractRead::Present { ir, state } => {
            validate_persisted_schema_contract(&ir, &state)?;
            Ok(ir)
        }
        SchemaContractRead::MissingAll | SchemaContractRead::PartialMissing => {
            Err(schema_lock_conflict(
                "repo is missing persisted schema state; manual coordination is required before schema changes are allowed",
            ))
        }
    }
}

pub(crate) fn schema_source_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_SOURCE_FILENAME)
}

pub(crate) fn schema_ir_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_IR_FILENAME)
}

pub(crate) fn schema_state_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_STATE_FILENAME)
}

enum SchemaContractRead {
    Present { ir: SchemaIR, state: SchemaState },
    MissingAll,
    PartialMissing,
}

async fn read_schema_contract(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<SchemaContractRead> {
    let ir_uri = schema_ir_uri(root_uri);
    let state_uri = schema_state_uri(root_uri);
    let ir_exists = storage.exists(&ir_uri).await?;
    let state_exists = storage.exists(&state_uri).await?;

    match (ir_exists, state_exists) {
        (false, false) => Ok(SchemaContractRead::MissingAll),
        (true, true) => {
            let ir_json = storage.read_text(&ir_uri).await?;
            let state_json = storage.read_text(&state_uri).await?;
            let ir = serde_json::from_str::<SchemaIR>(&ir_json).map_err(|err| {
                schema_lock_conflict(format!(
                    "accepted compiled schema contract in {} is invalid: {}",
                    SCHEMA_IR_FILENAME, err
                ))
            })?;
            let state = serde_json::from_str::<SchemaState>(&state_json).map_err(|err| {
                schema_lock_conflict(format!(
                    "repo schema state in {} is invalid: {}",
                    SCHEMA_STATE_FILENAME, err
                ))
            })?;
            Ok(SchemaContractRead::Present { ir, state })
        }
        _ => Ok(SchemaContractRead::PartialMissing),
    }
}

fn validate_persisted_schema_contract(ir: &SchemaIR, state: &SchemaState) -> Result<()> {
    if state.format_version != SCHEMA_STATE_FORMAT_VERSION {
        return Err(schema_lock_conflict(format!(
            "repo schema state format {} is unsupported",
            state.format_version
        )));
    }

    let actual_hash = schema_ir_hash(ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
    if actual_hash != state.schema_ir_hash {
        return Err(schema_lock_conflict(
            "accepted compiled schema does not match the recorded schema state",
        ));
    }

    Ok(())
}

fn validate_current_source_matches(
    state: &SchemaState,
    current_source_ir: &SchemaIR,
) -> Result<()> {
    let current_hash =
        schema_ir_hash(current_source_ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
    if current_hash != state.schema_ir_hash {
        return Err(schema_lock_conflict(
            "current _schema.pg no longer matches the accepted compiled schema",
        ));
    }
    Ok(())
}

fn compile_schema_source(source: &str) -> Result<SchemaIR> {
    let schema = parse_schema(source).map_err(|err| {
        schema_lock_conflict(format!(
            "current _schema.pg is not a valid accepted schema definition: {}",
            err
        ))
    })?;
    build_schema_ir(&schema).map_err(|err| {
        schema_lock_conflict(format!(
            "current _schema.pg could not be compiled into the accepted schema contract: {}",
            err
        ))
    })
}

fn schema_lock_conflict(detail: impl Into<String>) -> OmniError {
    OmniError::manifest_conflict(format!(
        "schema evolution is locked down in phase 1: {}; manual coordination is required",
        detail.into()
    ))
}
