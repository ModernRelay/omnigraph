use std::collections::BTreeSet;
use std::sync::Arc;

use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{SchemaIR, build_schema_ir, schema_ir_hash, schema_ir_pretty_json};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::db::manifest::Snapshot;
use crate::error::{OmniError, Result};
use crate::storage::{StorageAdapter, join_uri};

pub(crate) const SCHEMA_SOURCE_FILENAME: &str = "_schema.pg";
pub(crate) const SCHEMA_IR_FILENAME: &str = "_schema.ir.json";
pub(crate) const SCHEMA_STATE_FILENAME: &str = "__schema_state.json";

// Staging filenames used by atomic schema apply. Schema apply writes to these
// first, then commits the manifest, then renames staging → final. Recovery on
// open reconciles any leftover staging files against the manifest.
pub(crate) const SCHEMA_SOURCE_STAGING_FILENAME: &str = "_schema.pg.staging";
pub(crate) const SCHEMA_IR_STAGING_FILENAME: &str = "_schema.ir.json.staging";
pub(crate) const SCHEMA_STATE_STAGING_FILENAME: &str = "__schema_state.json.staging";

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
    write_schema_contract_to(
        storage,
        &schema_ir_uri(root_uri),
        &schema_state_uri(root_uri),
        schema_ir,
    )
    .await
}

/// Variant of `write_schema_contract` that writes the IR + state JSON to the
/// staging filenames. Used by atomic schema apply: staging files are written
/// before the manifest commit, then renamed to the final names afterward.
pub(crate) async fn write_schema_contract_staging(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    schema_ir: &SchemaIR,
) -> Result<SchemaState> {
    write_schema_contract_to(
        storage,
        &schema_ir_staging_uri(root_uri),
        &schema_state_staging_uri(root_uri),
        schema_ir,
    )
    .await
}

async fn write_schema_contract_to(
    storage: &dyn StorageAdapter,
    ir_uri: &str,
    state_uri: &str,
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

    storage.write_text(ir_uri, &ir_json).await?;
    storage.write_text(state_uri, &state_json).await?;
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

pub(crate) fn schema_source_staging_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_SOURCE_STAGING_FILENAME)
}

pub(crate) fn schema_ir_staging_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_IR_STAGING_FILENAME)
}

pub(crate) fn schema_state_staging_uri(root_uri: &str) -> String {
    join_uri(root_uri, SCHEMA_STATE_STAGING_FILENAME)
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

/// Reconcile leftover schema staging files (`_schema.pg.staging`,
/// `_schema.ir.json.staging`, `__schema_state.json.staging`) against the
/// manifest snapshot.
///
/// Atomic schema apply writes these staging files before committing the
/// manifest, then renames them to their final names. A crash mid-apply can
/// leave staging files behind. This function determines whether the crash
/// happened before or after the manifest commit and either deletes the
/// staging files (pre-commit) or completes the rename (post-commit).
///
/// The discriminator is the manifest's set of registered table keys: it
/// matches the schema source whose state corresponds to the manifest's
/// current version. For migrations that change the table set
/// (add/remove/rename a node or edge type), this is unambiguous. For
/// property-only migrations where both schemas imply the same table set,
/// recovery cannot disambiguate from staging files alone and returns an
/// operator-actionable error rather than guessing.
pub(crate) async fn recover_schema_state_files(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
) -> Result<()> {
    let pg_staging = schema_source_staging_uri(root_uri);
    let ir_staging = schema_ir_staging_uri(root_uri);
    let state_staging = schema_state_staging_uri(root_uri);

    let pg_exists = storage.exists(&pg_staging).await?;
    let ir_exists = storage.exists(&ir_staging).await?;
    let state_exists = storage.exists(&state_staging).await?;

    if !pg_exists && !ir_exists && !state_exists {
        return Ok(());
    }

    if !pg_exists {
        // _schema.pg.staging is gone but at least one of the other staging
        // files is still present. This is a partial-rename: the post-commit
        // crash happened mid-rename (after _schema.pg was renamed in but
        // before _schema.ir.json or __schema_state.json was). The live
        // _schema.pg should already reflect the new schema; verify that
        // and complete the remaining renames.
        let live_source = storage.read_text(&schema_source_uri(root_uri)).await?;
        let live_ir = compile_schema_source(&live_source)?;
        let live_hash =
            schema_ir_hash(&live_ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
        if state_exists {
            let state_json = storage.read_text(&state_staging).await?;
            let staging_state = serde_json::from_str::<SchemaState>(&state_json)
                .map_err(|err| schema_lock_conflict(err.to_string()))?;
            if staging_state.schema_ir_hash != live_hash {
                return Err(schema_lock_conflict(format!(
                    "found schema staging files (ir/state) without _schema.pg.staging, and the live _schema.pg does not match the staging schema state hash; manual operator action required (manifest v{})",
                    snapshot.version()
                )));
            }
        }
        warn!(
            "completing partial schema-file rename (manifest v{})",
            snapshot.version()
        );
        complete_staging_rename(root_uri, storage.as_ref()).await?;
        return Ok(());
    }

    let staging_source = storage.read_text(&pg_staging).await?;
    let staging_ir = compile_schema_source(&staging_source)?;

    let live_source = storage.read_text(&schema_source_uri(root_uri)).await?;
    let live_ir = compile_schema_source(&live_source)?;

    let staging_hash =
        schema_ir_hash(&staging_ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
    let live_hash =
        schema_ir_hash(&live_ir).map_err(|err| schema_lock_conflict(err.to_string()))?;

    if staging_hash == live_hash {
        warn!(
            "removing leftover schema staging files matching the live schema (no-op apply that crashed)"
        );
        cleanup_staging_files(root_uri, storage.as_ref()).await?;
        return Ok(());
    }

    let live_keys = expected_table_keys(&live_ir);
    let staging_keys = expected_table_keys(&staging_ir);
    let actual_keys: BTreeSet<String> = snapshot
        .entries()
        .map(|entry| entry.table_key.clone())
        .collect();

    if live_keys == staging_keys {
        return Err(schema_lock_conflict(format!(
            "found schema staging files but cannot disambiguate pre- vs post-commit crash: live and staging schemas imply identical table sets (likely a property-only migration). Inspect _schema.pg.staging vs _schema.pg manually and either remove the staging files (to keep the live schema) or replace _schema.pg with the staging file (to apply the new schema). Manifest version: v{}",
            snapshot.version()
        )));
    }

    if actual_keys == live_keys {
        warn!(
            "schema apply crashed before manifest commit; removing staging files and keeping live schema (manifest v{})",
            snapshot.version()
        );
        cleanup_staging_files(root_uri, storage.as_ref()).await?;
        Ok(())
    } else if actual_keys == staging_keys {
        warn!(
            "schema apply crashed after manifest commit; completing schema-file rename (manifest v{})",
            snapshot.version()
        );
        complete_staging_rename(root_uri, storage.as_ref()).await?;
        Ok(())
    } else {
        Err(schema_lock_conflict(format!(
            "found schema staging files but the manifest's table set ({:?}) matches neither the live schema ({:?}) nor the staging schema ({:?}); manual operator action required",
            actual_keys, live_keys, staging_keys
        )))
    }
}

async fn cleanup_staging_files(root_uri: &str, storage: &dyn StorageAdapter) -> Result<()> {
    storage.delete(&schema_source_staging_uri(root_uri)).await?;
    storage.delete(&schema_ir_staging_uri(root_uri)).await?;
    storage.delete(&schema_state_staging_uri(root_uri)).await?;
    Ok(())
}

async fn complete_staging_rename(root_uri: &str, storage: &dyn StorageAdapter) -> Result<()> {
    // Each rename is independent and idempotent: if the source no longer
    // exists (already renamed) we skip it. This handles partial-rename
    // recovery (e.g. one file renamed before crash).
    rename_if_present(
        storage,
        &schema_source_staging_uri(root_uri),
        &schema_source_uri(root_uri),
    )
    .await?;
    rename_if_present(
        storage,
        &schema_ir_staging_uri(root_uri),
        &schema_ir_uri(root_uri),
    )
    .await?;
    rename_if_present(
        storage,
        &schema_state_staging_uri(root_uri),
        &schema_state_uri(root_uri),
    )
    .await?;
    Ok(())
}

async fn rename_if_present(
    storage: &dyn StorageAdapter,
    from_uri: &str,
    to_uri: &str,
) -> Result<()> {
    if storage.exists(from_uri).await? {
        storage.rename_text(from_uri, to_uri).await?;
    }
    Ok(())
}

fn expected_table_keys(ir: &SchemaIR) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for node in &ir.nodes {
        keys.insert(format!("node:{}", node.name));
    }
    for edge in &ir.edges {
        keys.insert(format!("edge:{}", edge.name));
    }
    keys
}
