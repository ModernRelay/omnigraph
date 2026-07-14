use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{
    SchemaIR, SchemaIdentityDomain, SchemaShape, compile_schema_shape, schema_ir_hash,
    schema_ir_pretty_json, schema_shape_hash, schema_shape_hash_from_ir, validate_schema_ir,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::db::manifest::{Snapshot, TableIdentity, table_path_for_identity};
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

const SCHEMA_STATE_FORMAT_VERSION: u32 = 2;
const SCHEMA_IDENTITY_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchemaState {
    pub(crate) format_version: u32,
    pub(crate) schema_shape_hash: String,
    pub(crate) schema_ir_hash: String,
    pub(crate) schema_identity_version: u32,
    pub(crate) schema_identity_domain: String,
}

impl SchemaState {
    fn from_ir(schema_ir: &SchemaIR) -> Result<Self> {
        validate_schema_ir(schema_ir).map_err(|error| schema_lock_conflict(error.to_string()))?;
        Ok(Self {
            format_version: SCHEMA_STATE_FORMAT_VERSION,
            schema_shape_hash: schema_shape_hash_from_ir(schema_ir)
                .map_err(|error| schema_lock_conflict(error.to_string()))?,
            schema_ir_hash: schema_ir_hash(schema_ir)
                .map_err(|error| schema_lock_conflict(error.to_string()))?,
            schema_identity_version: SCHEMA_IDENTITY_VERSION,
            schema_identity_domain: schema_ir.schema_identity_domain.as_str().to_string(),
        })
    }
}

pub(crate) async fn validate_schema_contract(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
) -> Result<SchemaState> {
    load_validated_schema_contract(root_uri, storage)
        .await
        .map(|(_, state)| state)
}

/// Load the accepted IR and its schema identity from one validated contract
/// read. Mutation/load preparation carries the catalog built from this exact IR
/// beside the identity in its `WriteTxn`; consulting the handle-global catalog
/// would let a long-lived handle combine a newly observed schema token with an
/// older in-memory plan.
pub(crate) async fn load_validated_schema_contract(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
) -> Result<(SchemaIR, SchemaState)> {
    let source = storage.read_text(&schema_source_uri(root_uri)).await?;
    load_validated_schema_contract_for_source(root_uri, storage, &source).await
}

/// Validate the complete durable schema contract against source bytes already
/// captured under the root schema gate. Open and refresh use this form so the
/// source that populates the handle is exactly the source whose semantic shape
/// was checked against the accepted identity-bearing IR.
pub(crate) async fn load_validated_schema_contract_for_source(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
    source: &str,
) -> Result<(SchemaIR, SchemaState)> {
    let current_source_shape = compile_schema_source(source)?;
    let (persisted_ir, state) = match read_schema_contract(root_uri, storage.as_ref()).await? {
        SchemaContractRead::Present { ir, state } => (ir, state),
        SchemaContractRead::MissingAll => {
            return Err(schema_lock_conflict(
                "graph is missing the mandatory identity-bearing schema contract (_schema.ir.json and __schema_state.json); automatic bootstrap is not supported",
            ));
        }
        SchemaContractRead::PartialMissing => {
            return Err(schema_lock_conflict(
                "graph schema contract is incomplete: _schema.ir.json and __schema_state.json must both be present",
            ));
        }
    };

    validate_persisted_schema_contract(&persisted_ir, &state)?;
    validate_current_source_matches(&state, &current_source_shape)?;
    Ok((persisted_ir, state))
}

/// Read only the durable schema-identity marker. Schema apply promotes this
/// file after `_schema.pg` and `_schema.ir.json`, then releases its sentinel.
/// A capture path that already performed one full contract validation can use a
/// trailing marker read to detect the publish-before-promotion window without
/// paying for a second full source+IR parse.
pub(crate) async fn read_schema_state_identity(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<SchemaState> {
    let text = storage.read_text(&schema_state_uri(root_uri)).await?;
    let state = serde_json::from_str::<SchemaState>(&text).map_err(|err| {
        schema_lock_conflict(format!(
            "graph schema state in {} is invalid: {}",
            SCHEMA_STATE_FILENAME, err
        ))
    })?;
    validate_schema_state_envelope(&state)?;
    Ok(state)
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
    let state = SchemaState::from_ir(schema_ir)?;
    let state_json = serde_json::to_string_pretty(&state).map_err(|err| {
        OmniError::manifest_internal(format!("serialize schema state error: {}", err))
    })?;

    storage.write_text(ir_uri, &ir_json).await?;
    storage.write_text(state_uri, &state_json).await?;
    Ok(state)
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
        SchemaContractRead::MissingAll => Err(schema_lock_conflict(
            "graph is missing the mandatory identity-bearing schema contract; automatic bootstrap is not supported",
        )),
        SchemaContractRead::PartialMissing => Err(schema_lock_conflict(
            "graph schema contract is incomplete: _schema.ir.json and __schema_state.json must both be present",
        )),
    }
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
    for entry in snapshot.entries() {
        let Some((expected_identity, expected_path)) = expected_by_alias.get(&entry.table_key)
        else {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} contains live table '{}' that is absent from accepted SchemaIR",
                snapshot.version(),
                entry.table_key
            )));
        };
        if entry.identity != *expected_identity {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} table '{}' has identity {}, but accepted SchemaIR requires {}",
                snapshot.version(),
                entry.table_key,
                entry.identity,
                expected_identity
            )));
        }
        if entry.table_path != *expected_path {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} table '{}' has non-canonical path '{}', expected '{}' for identity {}",
                snapshot.version(),
                entry.table_key,
                entry.table_path,
                expected_path,
                expected_identity
            )));
        }
        if let Some(previous) = manifest_by_identity.insert(entry.identity, entry.table_key.clone())
        {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} aliases '{previous}' and '{}' to the same table identity {}",
                snapshot.version(),
                entry.table_key,
                entry.identity
            )));
        }
    }

    for (table_key, (identity, _)) in expected_by_alias {
        if snapshot.entry(&table_key).is_none() {
            return Err(schema_manifest_conflict(format!(
                "accepted SchemaIR table '{table_key}' with identity {identity} is missing from manifest v{}",
                snapshot.version()
            )));
        }
    }
    Ok(())
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
                    "graph schema state in {} is invalid: {}",
                    SCHEMA_STATE_FILENAME, err
                ))
            })?;
            Ok(SchemaContractRead::Present { ir, state })
        }
        _ => Ok(SchemaContractRead::PartialMissing),
    }
}

async fn read_schema_ir_at(storage: &dyn StorageAdapter, uri: &str) -> Result<SchemaIR> {
    let text = storage.read_text(uri).await?;
    serde_json::from_str::<SchemaIR>(&text).map_err(|error| {
        schema_lock_conflict(format!(
            "accepted compiled schema contract at '{uri}' is invalid: {error}"
        ))
    })
}

async fn read_schema_state_at(storage: &dyn StorageAdapter, uri: &str) -> Result<SchemaState> {
    let text = storage.read_text(uri).await?;
    serde_json::from_str::<SchemaState>(&text).map_err(|error| {
        schema_lock_conflict(format!("graph schema state at '{uri}' is invalid: {error}"))
    })
}

fn validate_persisted_schema_contract(ir: &SchemaIR, state: &SchemaState) -> Result<()> {
    validate_schema_state_envelope(state)?;
    validate_schema_ir(ir).map_err(|error| {
        schema_lock_conflict(format!(
            "accepted compiled schema is not a valid identity-bearing IR: {error}"
        ))
    })?;

    let actual_hash = schema_ir_hash(ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
    if actual_hash != state.schema_ir_hash {
        return Err(schema_lock_conflict(
            "accepted compiled schema does not match the recorded schema state",
        ));
    }

    let projected_shape_hash =
        schema_shape_hash_from_ir(ir).map_err(|err| schema_lock_conflict(err.to_string()))?;
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

    Ok(())
}

fn validate_current_source_matches(
    state: &SchemaState,
    current_source_shape: &SchemaShape,
) -> Result<()> {
    let current_hash = schema_shape_hash(current_source_shape)
        .map_err(|err| schema_lock_conflict(err.to_string()))?;
    if current_hash != state.schema_shape_hash {
        return Err(schema_lock_conflict(
            "current _schema.pg no longer matches the accepted compiled schema",
        ));
    }
    Ok(())
}

fn validate_schema_state_envelope(state: &SchemaState) -> Result<()> {
    if state.format_version != SCHEMA_STATE_FORMAT_VERSION {
        return Err(schema_lock_conflict(format!(
            "graph schema state format {} is unsupported",
            state.format_version
        )));
    }
    if state.schema_identity_version != SCHEMA_IDENTITY_VERSION {
        return Err(schema_lock_conflict(format!(
            "graph schema identity version {} is unsupported",
            state.schema_identity_version
        )));
    }
    SchemaIdentityDomain::parse(&state.schema_identity_domain).map_err(|error| {
        schema_lock_conflict(format!("graph schema identity domain is invalid: {error}"))
    })?;
    Ok(())
}

fn compile_schema_source(source: &str) -> Result<SchemaShape> {
    let schema = parse_schema(source).map_err(|err| {
        schema_lock_conflict(format!(
            "current _schema.pg is not a valid accepted schema definition: {}",
            err
        ))
    })?;
    compile_schema_shape(&schema).map_err(|err| {
        schema_lock_conflict(format!(
            "current _schema.pg could not be compiled into the accepted schema shape: {}",
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

fn schema_manifest_conflict(detail: impl Into<String>) -> OmniError {
    OmniError::manifest_conflict(format!(
        "accepted schema/manifest identity mismatch: {}",
        detail.into()
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaStateRecovery {
    Noop,
    CleanedStaging,
    CompletedStagingRename { schema_apply_sidecar: bool },
}

impl SchemaStateRecovery {
    pub(crate) fn completed_schema_apply_sidecar_rename(self) -> bool {
        matches!(
            self,
            Self::CompletedStagingRename {
                schema_apply_sidecar: true,
            }
        )
    }
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
) -> Result<SchemaStateRecovery> {
    let pg_staging = schema_source_staging_uri(root_uri);
    let ir_staging = schema_ir_staging_uri(root_uri);
    let state_staging = schema_state_staging_uri(root_uri);

    let pg_exists = storage.exists(&pg_staging).await?;
    let ir_exists = storage.exists(&ir_staging).await?;
    let state_exists = storage.exists(&state_staging).await?;

    if !pg_exists && !ir_exists && !state_exists {
        return Ok(SchemaStateRecovery::Noop);
    }

    // Schema-apply atomicity: when a SchemaApply sidecar is present,
    // the writer reached Phase B (Lance HEADs advanced) but didn't
    // complete Phase C (manifest publish + staging→final renames). The
    // recovery sweep about to run will roll the table versions forward
    // to the new Lance HEADs; we MUST also rename the staging files
    // forward so the catalog matches. Without this, the disambiguation
    // logic below sees actual_keys == live_keys (manifest didn't move)
    // and deletes the staging files, leaving the graph with new-schema
    // data on disk but the old `_schema.pg` live — corruption.
    let schema_apply_sidecars = crate::db::manifest::list_sidecars(root_uri, storage.as_ref())
        .await?
        .into_iter()
        .filter(|sidecar| {
            matches!(
                sidecar.writer_kind,
                crate::db::manifest::SidecarKind::SchemaApply
            )
        })
        .collect::<Vec<_>>();
    if schema_apply_sidecars
        .iter()
        .any(|sidecar| sidecar.protocol_v7.is_some())
    {
        if schema_apply_sidecars
            .iter()
            .any(|sidecar| sidecar.protocol_v7.is_none())
        {
            return Err(schema_lock_conflict(
                "found both legacy and exact SchemaApply recovery intents; refusing to choose a schema-staging direction",
            ));
        }
        // Schema-v7 owns schema staging inside its exact state machine. An
        // Armed intent rolls back; an EffectsConfirmed intent promotes only
        // after its fixed manifest outcome is visible. The legacy eager
        // promotion below would turn a partial v7 migration into live schema.
        return Ok(SchemaStateRecovery::Noop);
    }
    if !schema_apply_sidecars.is_empty() {
        warn!(
            "recovery: SchemaApply sidecar present; completing schema-staging rename so the \
             manifest-drift sweep's roll-forward sees the new catalog (manifest v{})",
            snapshot.version()
        );
        complete_staging_rename(root_uri, storage.as_ref()).await?;
        return Ok(SchemaStateRecovery::CompletedStagingRename {
            schema_apply_sidecar: true,
        });
    }

    if !pg_exists {
        // _schema.pg.staging is gone but at least one of the other staging
        // files is still present. This is a partial-rename: the post-commit
        // crash happened mid-rename (after _schema.pg was renamed in but
        // before _schema.ir.json or __schema_state.json was). The live
        // _schema.pg should already reflect the new schema; verify that
        // and complete the remaining renames.
        let live_source = storage.read_text(&schema_source_uri(root_uri)).await?;
        let live_shape = compile_schema_source(&live_source)?;
        let selected_ir_uri = if ir_exists {
            ir_staging.clone()
        } else {
            schema_ir_uri(root_uri)
        };
        let selected_state_uri = if state_exists {
            state_staging.clone()
        } else {
            schema_state_uri(root_uri)
        };
        let selected_ir = read_schema_ir_at(storage.as_ref(), &selected_ir_uri).await?;
        let selected_state = read_schema_state_at(storage.as_ref(), &selected_state_uri).await?;
        validate_persisted_schema_contract(&selected_ir, &selected_state)?;
        validate_current_source_matches(&selected_state, &live_shape)?;
        warn!(
            "completing partial schema-file rename (manifest v{})",
            snapshot.version()
        );
        complete_staging_rename(root_uri, storage.as_ref()).await?;
        return Ok(SchemaStateRecovery::CompletedStagingRename {
            schema_apply_sidecar: false,
        });
    }

    let staging_source = storage.read_text(&pg_staging).await?;
    let staging_shape = compile_schema_source(&staging_source)?;

    let live_source = storage.read_text(&schema_source_uri(root_uri)).await?;
    let live_shape = compile_schema_source(&live_source)?;

    let staging_hash =
        schema_shape_hash(&staging_shape).map_err(|err| schema_lock_conflict(err.to_string()))?;
    let live_hash =
        schema_shape_hash(&live_shape).map_err(|err| schema_lock_conflict(err.to_string()))?;

    if staging_hash == live_hash {
        warn!(
            "removing leftover schema staging files matching the live schema (no-op apply that crashed)"
        );
        cleanup_staging_files(root_uri, storage.as_ref()).await?;
        return Ok(SchemaStateRecovery::CleanedStaging);
    }

    let live_keys = expected_table_keys(&live_shape);
    let staging_keys = expected_table_keys(&staging_shape);
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
        Ok(SchemaStateRecovery::CleanedStaging)
    } else if actual_keys == staging_keys {
        warn!(
            "schema apply crashed after manifest commit; completing schema-file rename (manifest v{})",
            snapshot.version()
        );
        complete_staging_rename(root_uri, storage.as_ref()).await?;
        Ok(SchemaStateRecovery::CompletedStagingRename {
            schema_apply_sidecar: false,
        })
    } else {
        Err(schema_lock_conflict(format!(
            "found schema staging files but the manifest's table set ({:?}) matches neither the live schema ({:?}) nor the staging schema ({:?}); manual operator action required",
            actual_keys, live_keys, staging_keys
        )))
    }
}

pub(crate) async fn cleanup_staging_files(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<()> {
    storage.delete(&schema_source_staging_uri(root_uri)).await?;
    storage.delete(&schema_ir_staging_uri(root_uri)).await?;
    storage.delete(&schema_state_staging_uri(root_uri)).await?;
    Ok(())
}

pub(crate) async fn complete_staging_rename(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<()> {
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

/// Verify that every durable staging artifact belongs to one exact
/// SchemaApply target. A partial promotion is accepted only when the live
/// identity already equals that same target; mismatched files are never
/// renamed or deleted on another intent's behalf.
pub(crate) async fn validate_exact_schema_staging_target(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    target_schema_ir_hash: &str,
) -> Result<()> {
    let pg_staging = schema_source_staging_uri(root_uri);
    let ir_staging = schema_ir_staging_uri(root_uri);
    let state_staging = schema_state_staging_uri(root_uri);
    let pg_exists = storage.exists(&pg_staging).await?;
    let ir_exists = storage.exists(&ir_staging).await?;
    let state_exists = storage.exists(&state_staging).await?;
    // Promotion renames source -> IR -> state independently. A crash between
    // any two renames therefore leaves a mixture of live and staging files.
    // Verify each artifact at whichever location currently owns it instead of
    // using the state marker as a proxy for the whole three-file contract.
    let source_uri = if pg_exists {
        pg_staging
    } else {
        schema_source_uri(root_uri)
    };
    let source = storage.read_text(&source_uri).await?;
    let source_shape = compile_schema_source(&source)?;

    let ir_uri = if ir_exists {
        ir_staging
    } else {
        schema_ir_uri(root_uri)
    };
    let ir = read_schema_ir_at(storage, &ir_uri).await?;
    let ir_hash = schema_ir_hash(&ir).map_err(|error| schema_lock_conflict(error.to_string()))?;
    if ir_hash != target_schema_ir_hash {
        return Err(schema_lock_conflict(format!(
            "compiled schema at '{}' does not belong to the exact SchemaApply intent",
            ir_uri
        )));
    }

    let state_uri = if state_exists {
        state_staging
    } else {
        schema_state_uri(root_uri)
    };
    let state = read_schema_state_at(storage, &state_uri).await?;
    validate_persisted_schema_contract(&ir, &state)?;
    validate_current_source_matches(&state, &source_shape)?;
    if state.schema_ir_hash != target_schema_ir_hash {
        return Err(schema_lock_conflict(format!(
            "schema identity at '{}' does not belong to the exact SchemaApply intent",
            state_uri
        )));
    }
    Ok(())
}

pub(crate) async fn promote_exact_schema_staging(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    target_schema_ir_hash: &str,
) -> Result<()> {
    validate_exact_schema_staging_target(root_uri, storage, target_schema_ir_hash).await?;
    complete_staging_rename(root_uri, storage).await?;
    let live_source = storage.read_text(&schema_source_uri(root_uri)).await?;
    let live_shape = compile_schema_source(&live_source)?;
    let live_ir = read_schema_ir_at(storage, &schema_ir_uri(root_uri)).await?;
    let live = read_schema_state_at(storage, &schema_state_uri(root_uri)).await?;
    validate_persisted_schema_contract(&live_ir, &live)?;
    validate_current_source_matches(&live, &live_shape)?;
    let live_ir_hash =
        schema_ir_hash(&live_ir).map_err(|error| schema_lock_conflict(error.to_string()))?;
    if live.schema_ir_hash != target_schema_ir_hash || live_ir_hash != target_schema_ir_hash {
        return Err(schema_lock_conflict(
            "exact SchemaApply promotion completed without the intended live identity",
        ));
    }
    Ok(())
}

pub(crate) async fn discard_exact_schema_staging(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    accepted_schema_ir_hash: &str,
    target_schema_ir_hash: &str,
) -> Result<()> {
    let live = read_schema_state_identity(root_uri, storage).await?;
    if live.schema_ir_hash != accepted_schema_ir_hash {
        return Err(schema_lock_conflict(format!(
            "cannot discard exact SchemaApply staging: live schema identity '{}' differs from captured authority '{}'",
            live.schema_ir_hash, accepted_schema_ir_hash
        )));
    }
    let any_staging = storage.exists(&schema_source_staging_uri(root_uri)).await?
        || storage.exists(&schema_ir_staging_uri(root_uri)).await?
        || storage.exists(&schema_state_staging_uri(root_uri)).await?;
    if any_staging {
        validate_present_exact_schema_staging_artifacts(root_uri, storage, target_schema_ir_hash)
            .await?;
        cleanup_staging_files(root_uri, storage).await?;
    }
    Ok(())
}

async fn validate_present_exact_schema_staging_artifacts(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    target_schema_ir_hash: &str,
) -> Result<()> {
    let pg_staging = schema_source_staging_uri(root_uri);
    let source_shape = if storage.exists(&pg_staging).await? {
        let source = storage.read_text(&pg_staging).await?;
        Some(compile_schema_source(&source)?)
    } else {
        None
    };

    let ir_staging = schema_ir_staging_uri(root_uri);
    let staged_ir = if storage.exists(&ir_staging).await? {
        let ir = read_schema_ir_at(storage, &ir_staging).await?;
        validate_schema_ir(&ir).map_err(|error| schema_lock_conflict(error.to_string()))?;
        let hash = schema_ir_hash(&ir).map_err(|error| schema_lock_conflict(error.to_string()))?;
        if hash != target_schema_ir_hash {
            return Err(schema_lock_conflict(
                "_schema.ir.json.staging does not belong to the exact SchemaApply intent",
            ));
        }
        Some(ir)
    } else {
        None
    };

    let state_staging = schema_state_staging_uri(root_uri);
    let staged_state = if storage.exists(&state_staging).await? {
        let state = read_schema_state_at(storage, &state_staging).await?;
        validate_schema_state_envelope(&state)?;
        if state.schema_ir_hash != target_schema_ir_hash {
            return Err(schema_lock_conflict(
                "__schema_state.json.staging does not belong to the exact SchemaApply intent",
            ));
        }
        Some(state)
    } else {
        None
    };

    if let (Some(ir), Some(state)) = (&staged_ir, &staged_state) {
        validate_persisted_schema_contract(ir, state)?;
    }
    if let (Some(shape), Some(state)) = (&source_shape, &staged_state) {
        validate_current_source_matches(state, shape)?;
    } else if let (Some(shape), Some(ir)) = (&source_shape, &staged_ir) {
        let source_hash =
            schema_shape_hash(shape).map_err(|error| schema_lock_conflict(error.to_string()))?;
        let ir_shape_hash = schema_shape_hash_from_ir(ir)
            .map_err(|error| schema_lock_conflict(error.to_string()))?;
        if source_hash != ir_shape_hash {
            return Err(schema_lock_conflict(
                "_schema.pg.staging does not project to the exact staged SchemaApply IR",
            ));
        }
    }

    // A crash immediately after writing `_schema.pg.staging` can leave source
    // without the identity-bearing IR/state files needed to prove its target
    // hash. The exact sidecar plus the held root schema gate owns this global
    // staging path, so rollback may delete a syntactically valid source-only
    // artifact; roll-forward still requires the complete selected tuple above.
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

fn expected_table_keys(shape: &SchemaShape) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for node in &shape.nodes {
        keys.insert(format!("node:{}", node.name));
    }
    for edge in &shape.edges {
        keys.insert(format!("edge:{}", edge.name));
    }
    keys
}
