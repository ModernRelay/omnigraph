//! The three schema objects at the root of a graph stamped 8 or 9, which
//! omnigraph 0.11.x wrote in place of a `schema_contract` row: `_schema.pg`,
//! `_schema.ir.json` and `__schema_state.json`. The upgrade reads the contract
//! of such a root from them and leaves them in place; nothing served reads them.

use omnigraph_compiler::{
    SYSTEM_COLUMNS_V3, SchemaIR, schema_ir_hash, schema_shape_hash_from_ir, validate_schema_ir,
};
use serde::Deserialize;

use crate::db::manifest::{SchemaContractHead, SchemaContractRow};
use crate::db::schema_state::{
    compile_schema_source, schema_lock_conflict, validate_current_source_matches,
    validate_identity_envelope,
};
use crate::error::{OmniError, Result, StorageFailureKind};
use crate::storage::{StorageAdapter, join_uri};

pub(super) const SCHEMA_SOURCE_FILENAME: &str = "_schema.pg";
pub(super) const SCHEMA_IR_FILENAME: &str = "_schema.ir.json";
pub(super) const SCHEMA_STATE_FILENAME: &str = "__schema_state.json";
/// The three root objects, in the order a 0.11.x schema apply staged them.
pub(super) const SCHEMA_FILENAMES: [&str; 3] = [
    SCHEMA_SOURCE_FILENAME,
    SCHEMA_IR_FILENAME,
    SCHEMA_STATE_FILENAME,
];
/// The stamp from which a 0.11.x root may spell its system columns
/// `__id`/`__src`/`__dst`.
const SYSTEM_COLUMNS_STAMP: u32 = 9;

const SCHEMA_STATE_FORMAT_VERSION: u32 = 2;

const MISSING_SCHEMA_CONTRACT_MESSAGE: &str = "graph is missing the mandatory identity-bearing schema contract (_schema.ir.json and __schema_state.json); automatic bootstrap is not supported";
const INCOMPLETE_SCHEMA_CONTRACT_MESSAGE: &str = "graph schema contract is incomplete: _schema.ir.json and __schema_state.json must both be present";

/// `__schema_state.json` as 0.11.0 writes it: these five fields and no other.
/// A field beside them is not read, as 0.11.0 itself does not read one.
#[derive(Debug, Deserialize)]
struct RootSchemaState {
    format_version: u32,
    schema_shape_hash: String,
    schema_ir_hash: String,
    schema_identity_version: u32,
    schema_identity_domain: String,
}

/// The contract of a stamp-8 or stamp-9 root as its three objects carry it,
/// validated: the texts the conversion writes into the `schema_contract` row
/// of every live ref, and the IR those texts parse to.
#[derive(Debug)]
pub(super) struct RootSchemaContract {
    pub(super) row: SchemaContractRow,
    pub(super) ir: SchemaIR,
}

/// The `.staging` object a 0.11.x schema apply left at the root, if any.
pub(super) async fn staging_object(
    root: &str,
    storage: &dyn StorageAdapter,
) -> Result<Option<String>> {
    for name in SCHEMA_FILENAMES {
        let staging = format!("{name}.staging");
        if storage.exists(&join_uri(root, &staging)).await? {
            return Ok(Some(staging));
        }
    }
    Ok(None)
}

/// Why a root stamped `stamp` cannot carry `ir`, as 0.11.x itself refused it:
/// only a stamp-8 root whose contract spells the system columns
/// `__id`/`__src`/`__dst`; the `id`/`src`/`dst` spellings convert at either stamp.
pub(super) fn stamp_covers_vintage(stamp: u32, ir: &SchemaIR) -> Result<()> {
    if stamp < SYSTEM_COLUMNS_STAMP && ir.system_columns() == SYSTEM_COLUMNS_V3 {
        let columns = SYSTEM_COLUMNS_V3;
        return Err(OmniError::manifest(format!(
            "the graph is stamped v{stamp} but its schema contract declares the \
             `{}`/`{}`/`{}` system columns, which need v{SYSTEM_COLUMNS_STAMP}; restore the \
             three root schema objects from the backup taken with the tables",
            columns.id, columns.src, columns.dst
        )));
    }
    Ok(())
}

/// Load the contract from the three root objects, each in one bounded read of
/// at most `max_object_bytes`. A missing or incomplete contract reports an
/// unparseable source first, and a storage error before a parse error.
pub(super) async fn load_validated_schema_contract(
    root: &str,
    storage: &dyn StorageAdapter,
    max_object_bytes: u64,
) -> Result<RootSchemaContract> {
    let read = |name: &'static str| async move {
        storage
            .read_text_if_exists_bounded(&join_uri(root, name), max_object_bytes)
            .await
            .map_err(|error| over_bound(name, max_object_bytes, error))
    };
    let Some(source) = read(SCHEMA_SOURCE_FILENAME).await? else {
        return Err(schema_lock_conflict(format!(
            "`{SCHEMA_SOURCE_FILENAME}` is absent at the graph root"
        )));
    };
    let (ir_json, state_json) = match (
        read(SCHEMA_IR_FILENAME).await?,
        read(SCHEMA_STATE_FILENAME).await?,
    ) {
        (Some(ir_json), Some(state_json)) => (ir_json, state_json),
        (None, None) => {
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
    let state = serde_json::from_str::<RootSchemaState>(&state_json)
        .map_err(|err| invalid_contract_file("graph schema state", SCHEMA_STATE_FILENAME, err))?;
    if state.format_version != SCHEMA_STATE_FORMAT_VERSION {
        return Err(schema_lock_conflict(format!(
            "graph schema state format {} is unsupported",
            state.format_version
        )));
    }
    let head = SchemaContractHead {
        schema_ir_hash: state.schema_ir_hash,
        schema_identity_version: state.schema_identity_version,
        schema_identity_domain: state.schema_identity_domain,
    };
    validate_identity_envelope(&head)?;
    validate_schema_ir(&ir).map_err(|error| {
        schema_lock_conflict(format!(
            "accepted compiled schema is not a valid identity-bearing IR: {error}"
        ))
    })?;
    let actual_hash = schema_ir_hash(&ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
    if actual_hash != head.schema_ir_hash {
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
    if ir.schema_identity_domain.as_str() != head.schema_identity_domain {
        return Err(schema_lock_conflict(
            "accepted compiled schema identity domain does not match the recorded schema state",
        ));
    }
    validate_current_source_matches(&ir, &current_source_shape)?;
    Ok(RootSchemaContract {
        row: SchemaContractRow {
            source,
            ir: ir_json,
            head,
        },
        ir,
    })
}

/// Whether loading the contract stopped on the object store, not on the
/// contract: any storage failure but an absent object (`NotFound`) or a body
/// that is not text (`Permanent`). A rerun can succeed without a repair.
pub(super) fn store_did_not_answer(error: &OmniError) -> bool {
    error.storage_failure().is_some_and(|failure| {
        !matches!(
            failure.kind,
            StorageFailureKind::NotFound | StorageFailureKind::Permanent
        )
    })
}

fn invalid_contract_file(subject: &str, file: &str, err: impl std::fmt::Display) -> OmniError {
    schema_lock_conflict(format!("{subject} in {file} is invalid: {err}"))
}

/// A read refused for the size of `name` is a fact about the contract, not
/// about the store: the object is larger than any the upgrade converts.
fn over_bound(name: &str, max_object_bytes: u64, error: OmniError) -> OmniError {
    match error {
        OmniError::ResourceLimitExceeded { actual, .. } => OmniError::manifest(format!(
            "`{name}` is {actual} bytes, above the {max_object_bytes} bytes one root schema \
             object may hold"
        )),
        other => other,
    }
}
