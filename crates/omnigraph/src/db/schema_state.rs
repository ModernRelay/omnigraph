use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use omnigraph_compiler::schema::parser::parse_persisted_schema_contract;
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
// first, then commits the manifest, then installs the contract and retires the
// staging. A read-write open reconciles any leftover staging files against the
// manifest.
pub(crate) const SCHEMA_SOURCE_STAGING_FILENAME: &str = "_schema.pg.staging";
pub(crate) const SCHEMA_IR_STAGING_FILENAME: &str = "_schema.ir.json.staging";
pub(crate) const SCHEMA_STATE_STAGING_FILENAME: &str = "__schema_state.json.staging";

const SCHEMA_STATE_FORMAT_VERSION: u32 = 2;
const SCHEMA_IDENTITY_VERSION: u32 = 2;

/// Refuse a newer live or staged schema before open can perform recovery.
/// Only the version envelope is inspected here. Partial/malformed artifacts
/// remain subject to the existing recovery and complete contract validation;
/// this is not an alternate schema reader or an in-place format migration.
pub(crate) async fn refuse_unsupported_schema_versions(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<()> {
    #[derive(Deserialize)]
    struct VersionEnvelope {
        ir_version: u32,
    }

    #[derive(Deserialize)]
    struct FeatureEnvelope {
        #[serde(default)]
        features: BTreeSet<String>,
    }

    for filename in [SCHEMA_IR_FILENAME, SCHEMA_IR_STAGING_FILENAME] {
        let Some(text) = storage
            .read_text_if_exists(&join_uri(root_uri, filename))
            .await?
        else {
            continue;
        };
        let Ok(envelope) = serde_json::from_str::<VersionEnvelope>(&text) else {
            continue;
        };
        if !omnigraph_compiler::is_supported_ir_version(envelope.ir_version) {
            return Err(schema_lock_conflict(format!(
                "unsupported ir_version {} in {filename} (supported {}, {} and {}); open will not recover or migrate this schema",
                envelope.ir_version,
                omnigraph_compiler::SCHEMA_IR_VERSION,
                omnigraph_compiler::SCHEMA_IR_VERSION_EDGE_KEYS,
                omnigraph_compiler::SCHEMA_IR_VERSION_FEATURES,
            )));
        }
        let Ok(envelope) = serde_json::from_str::<FeatureEnvelope>(&text) else {
            continue;
        };
        if let Some(unknown) = envelope
            .features
            .iter()
            .find(|name| !omnigraph_compiler::is_known_feature(name))
        {
            return Err(schema_lock_conflict(format!(
                "schema feature '{unknown}' in {filename} is unknown to this build; upgrade omnigraph before opening this graph; open will not recover or migrate this schema"
            )));
        }
    }
    Ok(())
}

const MISSING_SCHEMA_CONTRACT_MESSAGE: &str = "graph is missing the mandatory identity-bearing schema contract (_schema.ir.json and __schema_state.json); automatic bootstrap is not supported";
const INCOMPLETE_SCHEMA_CONTRACT_MESSAGE: &str = "graph schema contract is incomplete: _schema.ir.json and __schema_state.json must both be present";
const INVALID_SCHEMA_IR_SUBJECT: &str = "accepted compiled schema contract";
const INVALID_SCHEMA_STATE_SUBJECT: &str = "graph schema state";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchemaState {
    pub(crate) format_version: u32,
    pub(crate) schema_shape_hash: String,
    pub(crate) schema_ir_hash: String,
    pub(crate) schema_identity_version: u32,
    pub(crate) schema_identity_domain: String,
    /// The graph commit that publishes this contract (RFC 0067). Schema apply
    /// and the system-column upgrade stage the contract with the commit they
    /// are about to publish; a read-write open promotes the staging when that
    /// commit is in main's lineage and discards it otherwise. Init writes no
    /// marker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) publication: Option<SchemaPublication>,
}

/// The manifest publication a staged schema contract belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchemaPublication {
    pub(crate) graph_commit_id: String,
    pub(crate) parent_commit_id: Option<String>,
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
            publication: None,
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
    let text = read_schema_contract_text(root_uri, storage.as_ref()).await?;
    validate_schema_contract_text(&text)
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
    let text = read_schema_contract_text_for_source(root_uri, storage.as_ref(), source.to_string())
        .await?;
    validate_schema_contract_text(&text)
}

/// The three durable schema-contract files as read, byte-exact. Memo key of
/// `ReadCaches::accepted_catalog` (see `AcceptedCatalogMemo`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaContractText {
    pub(crate) source: String,
    pub(crate) ir_json: String,
    pub(crate) state_json: String,
}

/// Read the schema contract's bytes: three `read_text` and two `exists` calls
/// (the per-query drift detection the lifecycle tests pin). A missing or
/// incomplete contract reports an unparseable source first; a storage error
/// from the existence probes is reported before a parse error, and so is a
/// failing IR or state read on the present-contract path.
pub(crate) async fn read_schema_contract_text(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<SchemaContractText> {
    let source = storage.read_text(&schema_source_uri(root_uri)).await?;
    read_schema_contract_text_for_source(root_uri, storage, source).await
}

/// [`read_schema_contract_text`] over a source already read under the root
/// schema gate.
pub(crate) async fn read_schema_contract_text_for_source(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    source: String,
) -> Result<SchemaContractText> {
    match read_schema_contract(root_uri, storage).await? {
        SchemaContractRead::Present {
            ir_json,
            state_json,
        } => Ok(SchemaContractText {
            source,
            ir_json,
            state_json,
        }),
        SchemaContractRead::MissingAll => {
            compile_schema_source(&source)?;
            Err(schema_lock_conflict(MISSING_SCHEMA_CONTRACT_MESSAGE))
        }
        SchemaContractRead::PartialMissing => {
            compile_schema_source(&source)?;
            Err(schema_lock_conflict(INCOMPLETE_SCHEMA_CONTRACT_MESSAGE))
        }
    }
}

/// Validate contract bytes: source compiled, IR and state parsed, IR checked
/// against the state, source shape checked against the state.
pub(crate) fn validate_schema_contract_text(
    text: &SchemaContractText,
) -> Result<(SchemaIR, SchemaState)> {
    let current_source_shape = compile_schema_source(&text.source)?;
    let (ir, state) = parse_schema_contract(&text.ir_json, &text.state_json)?;
    validate_persisted_schema_contract(&ir, &state)?;
    validate_current_source_matches(&state, &current_source_shape, &ir)?;
    Ok((ir, state))
}

fn parse_schema_contract(ir_json: &str, state_json: &str) -> Result<(SchemaIR, SchemaState)> {
    let ir = serde_json::from_str::<SchemaIR>(ir_json)
        .map_err(|err| invalid_contract_file(INVALID_SCHEMA_IR_SUBJECT, SCHEMA_IR_FILENAME, err))?;
    let state = serde_json::from_str::<SchemaState>(state_json).map_err(|err| {
        invalid_contract_file(INVALID_SCHEMA_STATE_SUBJECT, SCHEMA_STATE_FILENAME, err)
    })?;
    Ok((ir, state))
}

fn invalid_contract_file(subject: &str, file: &str, err: impl std::fmt::Display) -> OmniError {
    schema_lock_conflict(format!("{subject} in {file} is invalid: {err}"))
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
        invalid_contract_file(INVALID_SCHEMA_STATE_SUBJECT, SCHEMA_STATE_FILENAME, err)
    })?;
    validate_schema_state_envelope(&state)?;
    Ok(state)
}

/// The IR and state JSON of `schema_ir` as the contract writers put them on
/// the object store, with the state they encode.
pub(crate) fn render_schema_contract(
    schema_ir: &SchemaIR,
    publication: Option<SchemaPublication>,
) -> Result<(SchemaState, String, String)> {
    let ir_json = schema_ir_pretty_json(schema_ir)
        .map_err(|err| OmniError::manifest_internal(err.to_string()))?;
    let mut state = SchemaState::from_ir(schema_ir)?;
    state.publication = publication;
    let state_json = serde_json::to_string_pretty(&state).map_err(|err| {
        OmniError::manifest_internal(format!("serialize schema state error: {}", err))
    })?;
    Ok((state, ir_json, state_json))
}

/// Write the IR and state of an already-rendered contract to their final
/// filenames; init keeps the same text to seed its accepted-catalog memo.
pub(crate) async fn write_schema_contract(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    contract: &SchemaContractText,
) -> Result<()> {
    write_schema_contract_to(
        storage,
        &schema_ir_uri(root_uri),
        &schema_state_uri(root_uri),
        &contract.ir_json,
        &contract.state_json,
    )
    .await
}

async fn write_schema_contract_to(
    storage: &dyn StorageAdapter,
    ir_uri: &str,
    state_uri: &str,
    ir_json: &str,
    state_json: &str,
) -> Result<()> {
    storage.write_text(ir_uri, ir_json).await?;
    storage.write_text(state_uri, state_json).await
}

pub(crate) async fn read_accepted_schema_ir(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
) -> Result<SchemaIR> {
    match read_schema_contract(root_uri, storage.as_ref()).await? {
        SchemaContractRead::Present {
            ir_json,
            state_json,
        } => {
            let (ir, state) = parse_schema_contract(&ir_json, &state_json)?;
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
    for entry in snapshot.datasets() {
        let Some((expected_identity, expected_path)) = expected_by_alias.get(&entry.type_key)
        else {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} contains live table '{}' that is absent from accepted SchemaIR",
                snapshot.graph_manifest_version(),
                entry.type_key
            )));
        };
        if entry.identity != *expected_identity {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} table '{}' has identity {}, but accepted SchemaIR requires {}",
                snapshot.graph_manifest_version(),
                entry.type_key,
                entry.identity,
                expected_identity
            )));
        }
        if entry.dataset_path != *expected_path {
            return Err(schema_manifest_conflict(format!(
                "manifest v{} table '{}' has non-canonical path '{}', expected '{}' for identity {}",
                snapshot.graph_manifest_version(),
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
                snapshot.graph_manifest_version(),
                entry.type_key,
                entry.identity
            )));
        }
    }

    for (table_key, (identity, _)) in expected_by_alias {
        if snapshot.dataset(&table_key).is_none() {
            return Err(schema_manifest_conflict(format!(
                "accepted SchemaIR table '{table_key}' with identity {identity} is missing from manifest v{}",
                snapshot.graph_manifest_version()
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
    Present { ir_json: String, state_json: String },
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
            Ok(SchemaContractRead::Present {
                ir_json,
                state_json,
            })
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
    ir: &SchemaIR,
) -> Result<()> {
    let current_source_shape = current_source_shape.canonicalized_for_system_columns(
        omnigraph_compiler::system_columns_for_features(&ir.features),
    );
    let current_hash = schema_shape_hash(&current_source_shape)
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
    // This source is already bound to the persisted identity-bearing IR and
    // state hashes validated below. Use the compatibility parser so a v6 root
    // admitted before v0.10 with body-level @unique(Blob) remains openable for
    // inspection/export. Init and desired schema apply use the strict parser.
    let schema = parse_persisted_schema_contract(source).map_err(|err| {
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
    Discarded,
    Promoted,
}

/// What the schema staging files on the object store are, judged against
/// main's lineage (RFC 0067): the staged state carries the graph commit that
/// publishes the contract, and that commit's presence in lineage is the only
/// authority for promoting or discarding the staging.
pub(crate) enum StagedContract {
    /// No staging file exists.
    None,
    /// Staging files exist without the state file that closes the staging
    /// write; the writer crashed before its contract staging was complete.
    Incomplete,
    /// A complete staging without a publication marker. No current writer
    /// stages this way, so it is unowned and fails closed.
    Unmarked,
    /// A complete staging bound to one manifest publication.
    Marked {
        state: SchemaState,
        /// The marker's commit is in main's lineage: the publication happened.
        published: bool,
    },
}

/// `thorough` also probes the source and IR staging files when the state
/// file is absent, to tell an incomplete staging from none; a caller that
/// only acts on a complete staging passes `false` and pays one probe.
pub(crate) async fn inspect_staged_contract(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    thorough: bool,
) -> Result<StagedContract> {
    let state_staging = schema_state_staging_uri(root_uri);
    if !storage.exists(&state_staging).await? {
        if !thorough {
            return Ok(StagedContract::None);
        }
        let any_other = storage.exists(&schema_source_staging_uri(root_uri)).await?
            || storage.exists(&schema_ir_staging_uri(root_uri)).await?;
        return Ok(if any_other {
            StagedContract::Incomplete
        } else {
            StagedContract::None
        });
    }
    let state = read_schema_state_at(storage, &state_staging).await?;
    let Some(publication) = state.publication.clone() else {
        return Ok(StagedContract::Unmarked);
    };
    let (commits, _) =
        crate::db::manifest::ManifestCoordinator::read_graph_lineage_at(root_uri, None).await?;
    let published = match commits
        .iter()
        .find(|commit| commit.graph_commit_id == publication.graph_commit_id)
    {
        Some(commit) => {
            if commit.parent_commit_id != publication.parent_commit_id
                || commit.graph_branch.is_some()
            {
                return Err(schema_lock_conflict(format!(
                    "schema staging names graph commit '{}' with parent {:?}, but lineage records that commit with parent {:?} on branch {:?}",
                    publication.graph_commit_id,
                    publication.parent_commit_id,
                    commit.parent_commit_id,
                    commit.graph_branch
                )));
            }
            true
        }
        None => false,
    };
    Ok(StagedContract::Marked { state, published })
}

/// Prove that a read-only open can pair its manifest snapshot with the live
/// schema contract without writing. A staged contract names the graph commit
/// that publishes it (RFC 0067): once that commit is in lineage the manifest
/// already carries the new table set, and only a read-write open may install
/// the contract files, so serving the old catalog over it would be
/// incoherent. An unpublished staging is inert garbage the reader ignores.
pub(crate) async fn ensure_read_only_schema_coherent(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<()> {
    if let StagedContract::Marked {
        state,
        published: true,
    } = inspect_staged_contract(root_uri, storage, false).await?
    {
        let graph_commit_id = state
            .publication
            .map(|publication| publication.graph_commit_id)
            .unwrap_or_default();
        return Err(OmniError::recovery_required(
            graph_commit_id.clone(),
            format!(
                "read-only open found SchemaApply manifest outcome for graph commit '{}' but the schema contract promotion is pending; run a read-write open to finish it",
                graph_commit_id
            ),
        ));
    }
    Ok(())
}

/// What a read-write pass may do with schema staging it does not own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SchemaStagingPolicy {
    /// The open-time pass: promote a published staging, discard an unpublished
    /// one. Discarding relies on the one-mutation-process boundary: a live
    /// apply in another process loses its staging, and its own promotion then
    /// installs the contract from memory.
    PromoteOrDiscard,
    /// The write-entry pass and `refresh`: promote a published staging and
    /// leave anything else alone, since the apply that wrote it may still be
    /// live.
    PromoteOnly,
}

/// Promote or discard schema staging left by a crashed schema apply.
///
/// RFC 0067: the staged `__schema_state.json.staging` names the graph commit
/// that publishes the contract. That commit in main's lineage means the
/// manifest already carries the new registrations, so the contract files must
/// follow (promotion is idempotent per file); its absence means the manifest
/// never moved, so the staging is garbage.
pub(crate) async fn recover_schema_state_files(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
    snapshot: &Snapshot,
    policy: SchemaStagingPolicy,
) -> Result<SchemaStateRecovery> {
    let thorough = policy == SchemaStagingPolicy::PromoteOrDiscard;
    match inspect_staged_contract(root_uri, storage.as_ref(), thorough).await? {
        StagedContract::None => Ok(SchemaStateRecovery::Noop),
        StagedContract::Incomplete => {
            if policy == SchemaStagingPolicy::PromoteOnly {
                return Ok(SchemaStateRecovery::Noop);
            }
            warn!(
                "schema apply crashed while staging its contract; removing the incomplete staging (manifest v{})",
                snapshot.graph_manifest_version()
            );
            cleanup_staging_files(root_uri, storage.as_ref()).await?;
            Ok(SchemaStateRecovery::Discarded)
        }
        // Every writer marks its staging with the publishing commit, so an
        // unmarked one comes from a build that predates RFC 0067 or from
        // manual edits; neither can be judged here.
        StagedContract::Unmarked => Err(schema_lock_conflict(format!(
            "found complete schema staging files without a publication marker; inspect _schema.pg.staging against _schema.pg and remove the staging files to keep the live schema (manifest v{})",
            snapshot.graph_manifest_version()
        ))),
        StagedContract::Marked { state, published } => {
            if published {
                warn!(
                    "schema apply crashed after publishing graph commit {}; promoting its staged contract (manifest v{})",
                    state
                        .publication
                        .as_ref()
                        .map(|publication| publication.graph_commit_id.as_str())
                        .unwrap_or("?"),
                    snapshot.graph_manifest_version()
                );
                promote_exact_schema_staging(root_uri, storage.as_ref(), &state.schema_ir_hash)
                    .await?;
                return Ok(SchemaStateRecovery::Promoted);
            }
            if policy == SchemaStagingPolicy::PromoteOnly {
                return Ok(SchemaStateRecovery::Noop);
            }
            warn!(
                "schema apply crashed before publishing; removing its staged contract and keeping the live schema (manifest v{})",
                snapshot.graph_manifest_version()
            );
            cleanup_staging_files(root_uri, storage.as_ref()).await?;
            Ok(SchemaStateRecovery::Discarded)
        }
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
/// renamed or deleted on another apply's behalf.
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
    validate_current_source_matches(&state, &source_shape, &ir)?;
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
    validate_current_source_matches(&live, &live_shape, &live_ir)?;
    let live_ir_hash =
        schema_ir_hash(&live_ir).map_err(|error| schema_lock_conflict(error.to_string()))?;
    if live.schema_ir_hash != target_schema_ir_hash || live_ir_hash != target_schema_ir_hash {
        return Err(schema_lock_conflict(
            "exact SchemaApply promotion completed without the intended live identity",
        ));
    }
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
