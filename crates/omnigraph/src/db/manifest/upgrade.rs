use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{Schema, SchemaRef};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::refs::BranchIdentifier;
use lance::dataset::transaction::{Operation, Transaction, UpdateMap};
use lance::dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};
use crate::storage::{normalize_root_uri, storage_for_uri};

use super::layout::open_manifest_dataset_native_with_session;
use super::migrations::{INTERNAL_SCHEMA_VERSION_KEY, read_stamp};
use super::state::{read_manifest_state, read_manifest_state_with_registration_clocks};
use super::{
    OBJECT_TYPE_GRAPH_COMMIT, OBJECT_TYPE_GRAPH_HEAD, OBJECT_TYPE_TABLE,
    OBJECT_TYPE_TABLE_TOMBSTONE, OBJECT_TYPE_TABLE_VERSION,
};

pub(super) const UPGRADE_PENDING_KEY: &str = "omnigraph:storage_upgrade_pending";
const UPGRADE_RECEIPT_KEY: &str = "omnigraph:storage_upgrade_receipt";
const HANDLER: &str = "registration-clocks-v6-to-v7";
const DEFAULT_TARGET: u32 = 7;
const MAX_BRANCHES: usize = 1024;
const MAX_VERSIONS: usize = 100_000;
const MAX_APPENDED_UPGRADE_VERSIONS: u64 = 3;
const MAX_ROWS: usize = 1_000_000;
const MAX_METADATA_BYTES: usize = 64 * 1024 * 1024;
const MAX_INTENT_BYTES: usize = 1024 * 1024;

/// Explicit storage conversion options. Execution requires exclusive operator control.
#[derive(Debug, Clone, Copy, Default)]
pub struct UpgradeOptions {
    pub check: bool,
    pub to_format: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeMode {
    Check,
    Execute,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeOutcome {
    CheckPassed,
    AlreadyCurrent,
    Completed,
    CheckFailed,
    Interrupted,
    RecoveryRequired,
}

#[derive(Debug, Serialize)]
pub struct UpgradeFinding {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct UpgradeRecovery {
    pub failed_handler: String,
    pub executable_compatibility: String,
    pub action: String,
}

#[derive(Debug, Default, Serialize)]
pub struct UpgradeWork {
    pub metadata_rows: u64,
    pub retained_snapshots: u64,
    pub payload_bytes_copied: u64,
    pub payload_bytes_rewritten: u64,
    pub validation_bytes: Option<u64>,
    pub external_blob_exclusions: BTreeSet<String>,
    pub historical_blob_identity_limits: BTreeSet<String>,
}

#[derive(Debug, Serialize)]
pub struct UpgradeReport {
    pub mode: UpgradeMode,
    pub outcome: UpgradeOutcome,
    pub location: String,
    pub graph_identity: Option<String>,
    pub observed_format: Option<u32>,
    pub target_format: u32,
    pub target_defaulted: bool,
    pub route: Vec<String>,
    pub completed_handlers: Vec<String>,
    pub findings: Vec<UpgradeFinding>,
    pub last_durable_completed_boundary: Option<String>,
    pub recovery: Option<UpgradeRecovery>,
    pub work: UpgradeWork,
}

impl UpgradeReport {
    pub fn success(&self) -> bool {
        matches!(
            self.outcome,
            UpgradeOutcome::CheckPassed
                | UpgradeOutcome::AlreadyCurrent
                | UpgradeOutcome::Completed
        )
    }

    fn finding(&mut self, code: &str, message: impl Into<String>) {
        self.findings.push(UpgradeFinding {
            code: code.into(),
            message: message.into(),
        });
    }

    fn recover(&mut self, code: &str, message: impl Into<String>) {
        self.outcome = UpgradeOutcome::RecoveryRequired;
        self.finding(code, message);
        self.recovery = Some(UpgradeRecovery {
            failed_handler: HANDLER.into(),
            executable_compatibility: "this storage-upgrade-capable v7 executable; do not use the v6 executable".into(),
            action: "stop all writers and maintenance, retain the backup, then rerun the same upgrade command with --to-format 7 without --check".into(),
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SourceBranch {
    native: Option<String>,
    identity: BranchIdentifier,
    version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct UpgradeIntent {
    protocol: u32,
    attempt: String,
    source_format: u32,
    target_format: u32,
    graph_identity: String,
    branches: Vec<SourceBranch>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BranchReceipt {
    protocol: u32,
    attempt: String,
    source: SourceBranch,
}

/// Upgrade a standalone graph offline; neither this function nor `--check` starts recovery on open.
pub async fn upgrade_storage(uri: &str, options: UpgradeOptions) -> Result<UpgradeReport> {
    upgrade_storage_as(uri, options, None, None).await
}

/// Apply SchemaApply policy to every affected branch before any storage effects.
pub async fn upgrade_storage_as(
    uri: &str,
    options: UpgradeOptions,
    actor: Option<&str>,
    policy: Option<&dyn omnigraph_policy::PolicyChecker>,
) -> Result<UpgradeReport> {
    let root = normalize_root_uri(uri)?;
    let mut report = UpgradeReport {
        mode: if options.check {
            UpgradeMode::Check
        } else {
            UpgradeMode::Execute
        },
        outcome: UpgradeOutcome::CheckFailed,
        location: root.clone(),
        graph_identity: None,
        observed_format: None,
        target_format: options.to_format.unwrap_or(DEFAULT_TARGET),
        target_defaulted: options.to_format.is_none(),
        route: Vec::new(),
        completed_handlers: Vec::new(),
        findings: Vec::new(),
        last_durable_completed_boundary: None,
        recovery: None,
        work: UpgradeWork::default(),
    };
    let result = run(&root, options, actor, policy, &mut report).await;
    if let Err(error) = result {
        if report.last_durable_completed_boundary.is_some() || report.recovery.is_some() {
            report.recover("upgrade_interrupted", error.to_string());
        } else {
            report.finding("preflight_failed", error.to_string());
        }
    }
    Ok(report)
}

async fn open(root: &str, native: Option<&str>) -> Result<Dataset> {
    open_manifest_dataset_native_with_session(root, native, &crate::lance_access::control_session())
        .await
}

fn invalid(message: impl Into<String>) -> OmniError {
    OmniError::manifest(message)
}

fn intent_from(dataset: &Dataset) -> Result<Option<UpgradeIntent>> {
    let Some(json) = dataset.schema().metadata.get(UPGRADE_PENDING_KEY) else {
        return Ok(None);
    };
    if json.len() > MAX_INTENT_BYTES {
        return Err(invalid(
            "storage upgrade intent exceeds the metadata budget",
        ));
    }
    let intent: UpgradeIntent = serde_json::from_str(json)
        .map_err(|e| invalid(format!("unrecognized upgrade ownership: {e}")))?;
    let mut names = HashSet::new();
    if intent.protocol != 1
        || intent.source_format != 6
        || intent.target_format != 7
        || intent.attempt.parse::<ulid::Ulid>().is_err()
        || intent.graph_identity.is_empty()
        || intent.branches.is_empty()
        || intent.branches.len() > MAX_BRANCHES
        || intent
            .branches
            .last()
            .is_none_or(|branch| branch.native.is_some())
        || intent
            .branches
            .iter()
            .any(|branch| branch.version == 0 || !names.insert(branch.native.clone()))
    {
        return Err(invalid("unsupported or ambiguous storage upgrade intent"));
    }
    Ok(Some(intent))
}

async fn run(
    root: &str,
    options: UpgradeOptions,
    actor: Option<&str>,
    policy: Option<&dyn omnigraph_policy::PolicyChecker>,
    report: &mut UpgradeReport,
) -> Result<()> {
    let main = open(root, None).await?;
    report.observed_format = read_stamp(&main);
    let pending = match intent_from(&main) {
        Ok(value) => value,
        Err(error) => {
            report.recover("unknown_upgrade_ownership", error.to_string());
            return Ok(());
        }
    };
    if pending.is_some() {
        report.recover(
            "pending_upgrade",
            "an owned storage conversion requires explicit recovery",
        );
    }
    if report.target_format != DEFAULT_TARGET {
        report.finding("unsupported_target", "this binary declares target format 7 only; no complete route exists to the requested target");
        return Ok(());
    }
    let storage = storage_for_uri(root)?;
    let (_, schema_state) =
        crate::db::schema_state::load_validated_schema_contract(root, Arc::clone(&storage)).await?;
    report.graph_identity = Some(schema_state.schema_identity_domain.clone());
    let sidecars = super::list_sidecars(root, storage.as_ref()).await?;
    if !sidecars.is_empty() {
        report.outcome = UpgradeOutcome::RecoveryRequired;
        report.finding(
            "source_recovery_required",
            "pre-existing graph recovery must be resolved before storage conversion",
        );
        report.recovery = Some(UpgradeRecovery {
            failed_handler: HANDLER.into(), executable_compatibility: "the source-compatible executable for this graph's recovery format".into(),
            action: "before starting migration, stop writers, preserve the backup and resolve pending recovery with the source executable; then rerun upgrade --check".into(),
        });
        return Ok(());
    }
    if pending.is_none() && report.observed_format == Some(DEFAULT_TARGET) {
        super::migrations::guard_stamp(&main)?;
        read_manifest_state(&main).await?;
        let branches = crate::branch_control::list_branch_contents(&main).await?;
        if branches.len() >= MAX_BRANCHES {
            return Err(invalid("storage upgrade branch limit exceeded"));
        }
        for native in branches.keys() {
            if crate::db::is_internal_system_branch(native) {
                return Err(invalid(
                    "resolve internal branch recovery before storage upgrade",
                ));
            }
            let branch = main
                .checkout_branch(native)
                .await
                .map_err(OmniError::storage)?;
            super::migrations::guard_stamp(&branch)?;
            read_manifest_state(&branch).await?;
        }
        report.outcome = UpgradeOutcome::AlreadyCurrent;
        return Ok(());
    }
    if pending.is_none() && report.observed_format != Some(6) {
        report.finding("unsupported_source", "only validated v6 graphs have a conversion handler; preserve the source and use its executable for export/import");
        return Ok(());
    }
    report.route.push(HANDLER.into());
    let intent = match pending {
        Some(intent) => {
            if intent.graph_identity != schema_state.schema_identity_domain {
                return Err(invalid("upgrade schema identity changed"));
            }
            intent
        }
        None => inventory(&main, schema_state.schema_identity_domain.clone()).await?,
    };
    for branch in &intent.branches {
        if let Some(checker) = policy {
            let actor = actor.ok_or_else(|| {
                OmniError::Policy(
                    "storage upgrade requires an actor when policy is installed".into(),
                )
            })?;
            let name = branch
                .native
                .as_deref()
                .map(crate::branch_names::logical_branch_name)
                .unwrap_or("main");
            checker
                .check(
                    omnigraph_policy::PolicyAction::SchemaApply,
                    &omnigraph_policy::ResourceScope::TargetBranch(name.into()),
                    actor,
                )
                .map_err(|e| OmniError::Policy(e.to_string()))?;
        }
    }
    verify_inventory(root, &intent, report.recovery.is_some()).await?;
    preflight(root, &intent, &mut report.work).await?;
    if options.check {
        if report.recovery.is_none() {
            report.outcome = UpgradeOutcome::CheckPassed;
        }
        return Ok(());
    }
    let _root_exclusion = crate::db::reserve_export_root_exclusion(root)?;
    verify_inventory(root, &intent, report.recovery.is_some()).await?;
    if report.recovery.is_none() {
        let main = open(root, None).await?;
        let json = serde_json::to_string(&intent).map_err(|e| invalid(e.to_string()))?;
        if json.len() > MAX_INTENT_BYTES {
            return Err(invalid(
                "storage upgrade intent exceeds the metadata budget",
            ));
        }
        report.recover(
            "fence_publication_attempted",
            "inspect durable ownership before retrying an uncertain fence publication",
        );
        publish_fence(main, json).await?;
    }
    report.last_durable_completed_boundary = Some("source_fenced".into());
    crate::failpoints::maybe_fail(crate::failpoints::names::UPGRADE_AFTER_FENCE)?;
    for branch in &intent.branches {
        let current = open(root, branch.native.as_deref()).await?;
        if current
            .branch_identifier()
            .await
            .map_err(OmniError::storage)?
            != branch.identity
        {
            return Err(invalid("native branch lifetime changed before conversion"));
        }
        if branch_completed(&current, branch, &intent)? {
            continue;
        }
        verify_source_head(&current, branch, branch.native.is_none())?;
        let source = current
            .checkout_version(branch.version)
            .await
            .map_err(OmniError::storage)?;
        publish_conversion(current, source, branch, &intent).await?;
        report.last_durable_completed_boundary = Some(format!(
            "converted:{}",
            branch.native.as_deref().unwrap_or("main")
        ));
        crate::failpoints::maybe_fail(crate::failpoints::names::UPGRADE_AFTER_BRANCH)?;
    }
    for branch in &intent.branches {
        let current = open(root, branch.native.as_deref()).await?;
        if !branch_completed(&current, branch, &intent)? {
            return Err(invalid("activated graph has an incomplete branch"));
        }
        let source = current
            .checkout_version(branch.version)
            .await
            .map_err(OmniError::storage)?;
        equivalent(&source, &current).await?;
    }
    verify_inventory(root, &intent, true).await?;
    let main = open(root, None).await?;
    if intent_from(&main)?.as_ref() != Some(&intent) {
        return Err(invalid("activation ownership changed"));
    }
    crate::failpoints::maybe_fail(crate::failpoints::names::UPGRADE_BEFORE_ACTIVATION)?;
    publish_activation(main).await?;
    report.last_durable_completed_boundary = Some("activated".into());
    crate::failpoints::maybe_fail(crate::failpoints::names::UPGRADE_AFTER_ACTIVATION)?;
    report.outcome = UpgradeOutcome::Completed;
    report.completed_handlers.push(HANDLER.into());
    report.recovery = None;
    report.findings.clear();
    Ok(())
}

async fn inventory(main: &Dataset, graph_identity: String) -> Result<UpgradeIntent> {
    let branches = crate::branch_control::list_branch_contents(main).await?;
    if branches.len() >= MAX_BRANCHES {
        return Err(invalid("storage upgrade branch limit exceeded"));
    }
    let mut names: Vec<_> = branches.into_keys().collect();
    names.sort();
    let mut sources = Vec::with_capacity(names.len() + 1);
    for native in names {
        if crate::db::is_internal_system_branch(&native) {
            return Err(invalid(
                "resolve schema recovery and internal branches before upgrade",
            ));
        }
        let ds = main
            .checkout_branch(&native)
            .await
            .map_err(OmniError::storage)?;
        sources.push(SourceBranch {
            native: Some(native),
            identity: ds.branch_identifier().await.map_err(OmniError::storage)?,
            version: ds.version().version,
        });
    }
    sources.push(SourceBranch {
        native: None,
        identity: main.branch_identifier().await.map_err(OmniError::storage)?,
        version: main.version().version,
    });
    Ok(UpgradeIntent {
        protocol: 1,
        attempt: ulid::Ulid::new().to_string(),
        source_format: 6,
        target_format: 7,
        graph_identity,
        branches: sources,
    })
}

fn receipt(branch: &SourceBranch, intent: &UpgradeIntent) -> BranchReceipt {
    BranchReceipt {
        protocol: 1,
        attempt: intent.attempt.clone(),
        source: branch.clone(),
    }
}

fn branch_completed(
    dataset: &Dataset,
    source: &SourceBranch,
    intent: &UpgradeIntent,
) -> Result<bool> {
    let Some(raw) = dataset.schema().metadata.get(UPGRADE_RECEIPT_KEY) else {
        return Ok(false);
    };
    if raw.len() > MAX_INTENT_BYTES {
        return Err(invalid(
            "storage upgrade receipt exceeds the metadata budget",
        ));
    }
    let found: BranchReceipt =
        serde_json::from_str(raw).map_err(|e| invalid(format!("invalid upgrade receipt: {e}")))?;
    if found != receipt(source, intent) || read_stamp(dataset) != Some(7) {
        return Err(invalid("foreign upgrade receipt"));
    }
    let expected = source
        .version
        .checked_add(if source.native.is_none() { 2 } else { 1 })
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    let activated =
        source.native.is_none() && !dataset.schema().metadata.contains_key(UPGRADE_PENDING_KEY);
    let expected = expected
        .checked_add(u64::from(activated))
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    if dataset.version().version != expected {
        return Err(invalid(
            "upgraded branch moved while conversion was incomplete",
        ));
    }
    Ok(true)
}

fn verify_source_head(dataset: &Dataset, source: &SourceBranch, fenced: bool) -> Result<()> {
    let expected = source
        .version
        .checked_add(u64::from(fenced))
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    if dataset.version().version != expected {
        return Err(invalid("source branch changed; refusing foreign movement"));
    }
    if !fenced && read_stamp(dataset) != Some(6) {
        return Err(invalid("source branch is not v6"));
    }
    Ok(())
}

async fn verify_inventory(root: &str, intent: &UpgradeIntent, fenced: bool) -> Result<()> {
    let main = open(root, None).await?;
    let observed: BTreeSet<_> = crate::branch_control::list_branch_contents(&main)
        .await?
        .into_keys()
        .collect();
    let expected: BTreeSet<_> = intent
        .branches
        .iter()
        .filter_map(|branch| branch.native.clone())
        .collect();
    if observed != expected {
        return Err(invalid("native branch inventory changed during upgrade"));
    }
    if fenced && intent_from(&main)?.as_ref() != Some(intent) {
        return Err(invalid("main upgrade ownership changed"));
    }
    for source in &intent.branches {
        let dataset = open(root, source.native.as_deref()).await?;
        if dataset
            .branch_identifier()
            .await
            .map_err(OmniError::storage)?
            != source.identity
        {
            return Err(invalid("native branch lifetime changed during upgrade"));
        }
        if !branch_completed(&dataset, source, intent)? {
            verify_source_head(&dataset, source, fenced && source.native.is_none())?;
        }
    }
    Ok(())
}

fn dependency_root_identity(uri: &str) -> Result<String> {
    let uri = normalize_root_uri(uri)?;
    if crate::storage::storage_kind_for_uri(&uri)? == crate::storage::StorageKind::Local {
        std::fs::canonicalize(&uri)?
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| invalid("storage dependency path is not valid UTF-8"))
    } else {
        Ok(uri)
    }
}

fn confined(root: &str, dataset: &Dataset) -> Result<()> {
    let root = dependency_root_identity(root)?;
    for base in dataset.manifest().base_paths.values() {
        let path = dependency_root_identity(&base.path)?;
        if path != root && !path.starts_with(&format!("{root}/")) {
            return Err(invalid(
                "shared Lance dependencies outside the graph root require separately qualified retention and backup; upgrade refuses",
            ));
        }
    }
    Ok(())
}

async fn preflight(root: &str, intent: &UpgradeIntent, work: &mut UpgradeWork) -> Result<()> {
    let mut refs = HashSet::new();
    let mut logical_names = HashSet::from(["main"]);
    for native in intent
        .branches
        .iter()
        .filter_map(|branch| branch.native.as_deref())
    {
        let logical = crate::branch_names::logical_branch_name(native);
        crate::branch_names::ensure_logical_branch_name(logical)?;
        if !logical_names.insert(logical) {
            return Err(invalid(
                "ambiguous source branch identity: duplicate logical branch name",
            ));
        }
    }
    for branch in &intent.branches {
        let current = open(root, branch.native.as_deref()).await?;
        let source = current
            .checkout_version(branch.version)
            .await
            .map_err(OmniError::storage)?;
        if read_stamp(&source) != Some(6) {
            return Err(invalid("source history has an unsupported format"));
        }
        if source
            .schema()
            .unenforced_primary_key()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>()
            != ["object_id"]
            || !source.manifest().uses_stable_row_ids()
        {
            return Err(invalid("source manifest primary-key evidence is invalid"));
        }
        validate_metadata_budget(&source).await?;
        let (old, lineage) = super::state::read_manifest_state_and_lineage(&source).await?;
        validate_source_branch_identity(branch, &old, &lineage)?;
        let mut translated = read_manifest_state_with_registration_clocks(&source).await?;
        compare_states(old, &mut translated)?;
        let schema: Schema = source.schema().into();
        let expected = super::state::manifest_schema();
        if schema.fields().len() != expected.fields().len()
            || schema
                .fields()
                .iter()
                .zip(expected.fields())
                .any(|(actual, expected)| {
                    actual.name() != expected.name()
                        || actual.data_type() != expected.data_type()
                        || actual.is_nullable() != expected.is_nullable()
                })
        {
            return Err(invalid(
                "source manifest columns do not match the registered v6 format",
            ));
        }
        let schema = Arc::new(schema);
        let mut scan = source.scan();
        let mut columns: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        columns.push("_row_last_updated_at_version".into());
        scan.project(&columns).map_err(OmniError::storage)?;
        scan.batch_size(1024);
        let mut stream = scan.try_into_stream().await.map_err(OmniError::storage)?;
        let mut seen = HashSet::new();
        let mut count = 0usize;
        while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
            convert_batch(
                batch.clone(),
                Arc::clone(&schema),
                branch.version,
                &mut seen,
            )?;
            count = count
                .checked_add(batch.num_rows())
                .ok_or_else(|| invalid("metadata row count overflow"))?;
            if count > MAX_ROWS {
                return Err(invalid("storage upgrade metadata-row limit exceeded"));
            }
        }
        work.metadata_rows += u64::try_from(count).map_err(|e| invalid(e.to_string()))?;
        let versions = retained_version_refs(&source, MAX_VERSIONS).await?;
        for version in versions {
            let snapshot = source
                .checkout_version(version.version)
                .await
                .map_err(OmniError::storage)?;
            let unstamped_bootstrap = matches!(snapshot.version().version, 1 | 2)
                && !snapshot
                    .schema()
                    .metadata
                    .contains_key(INTERNAL_SCHEMA_VERSION_KEY);
            if read_stamp(&snapshot) != Some(6) && !unstamped_bootstrap {
                return Err(invalid(format!(
                    "retained history contains an unsupported format at {:?} version {}",
                    branch.native,
                    snapshot.version().version
                )));
            }
            confined(root, &snapshot)?;
            validate_metadata_budget(&snapshot).await?;
            let (state, lineage) = super::state::read_manifest_state_and_lineage(&snapshot).await?;
            if unstamped_bootstrap {
                let genesis = lineage.first();
                let valid_genesis = lineage.len() == 1
                    && genesis.is_some_and(|genesis| {
                        genesis.graph_manifest_version == 1
                            && genesis.graph_branch.is_none()
                            && genesis.parent_commit_id.is_none()
                            && genesis.merged_parent_commit_id.is_none()
                            && genesis.actor_id.is_none()
                            && state.graph_heads.len() == 1
                            && state.graph_heads.get("main") == Some(&genesis.graph_commit_id)
                    });
                let valid_entries = state.entries.iter().all(|entry| {
                    entry.entity_count == 0
                        && entry.published_dataset_version == 1
                        && entry.manifest_version == 1
                        && entry.native_dataset_branch.is_none()
                });
                let expected_rows = state.entries.len() * 2 + 2;
                if !valid_genesis
                    || !valid_entries
                    || snapshot
                        .count_rows(None)
                        .await
                        .map_err(OmniError::storage)?
                        != expected_rows
                {
                    return Err(invalid(
                        "unstamped history does not match the released v0.9 empty bootstrap contract",
                    ));
                }
            }
            work.retained_snapshots += 1;
            for entry in state.entries {
                let key = (
                    entry.dataset_path.clone(),
                    entry.native_dataset_branch.clone(),
                    entry.published_dataset_version,
                );
                if !refs.insert(key) {
                    continue;
                }
                if refs.len() > MAX_ROWS {
                    return Err(invalid("storage upgrade dependency limit exceeded"));
                }
                let uri = format!("{root}/{}", entry.dataset_path);
                let table = crate::instrumentation::open_dataset(
                    &uri,
                    crate::instrumentation::VersionResolution::Latest,
                    Some(&crate::lance_access::control_session()),
                    crate::instrumentation::manifest_wrapper(),
                )
                .await?;
                let table = if let Some(native) = &entry.native_dataset_branch {
                    table
                        .checkout_branch(native)
                        .await
                        .map_err(OmniError::storage)?
                } else {
                    table
                };
                let table = table
                    .checkout_version(entry.published_dataset_version)
                    .await
                    .map_err(OmniError::storage)?;
                confined(root, &table)?;
                if table
                    .schema()
                    .unenforced_primary_key()
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>()
                    != ["id"]
                    || table
                        .schema()
                        .unenforced_primary_key()
                        .iter()
                        .any(|field| field.nullable)
                    || !table.manifest().uses_stable_row_ids()
                {
                    return Err(invalid(
                        "source table lacks v6 id primary-key or stable-row identity evidence",
                    ));
                }
                table.validate().await.map_err(OmniError::storage)?;
                for field in table.schema().fields.iter().filter(|field| field.is_blob()) {
                    if !field
                        .metadata
                        .contains_key(crate::db::STABLE_PROPERTY_ID_METADATA_KEY)
                    {
                        work.historical_blob_identity_limits
                            .insert(format!("{}:{}", entry.dataset_path, field.name));
                    }
                }
                validate_blobs(&table, work).await?;
            }
        }
    }
    Ok(())
}

fn validate_source_branch_identity(
    branch: &SourceBranch,
    state: &super::state::ManifestState,
    lineage: &[super::state::GraphLineageRow],
) -> Result<()> {
    let Some(native) = branch.native.as_deref() else {
        return Ok(());
    };
    let (logical, suffix) = crate::branch_names::split_native_branch_name(native);
    if suffix.is_none() {
        return Ok(());
    }
    let fork_version = branch
        .identity
        .version_mapping
        .last()
        .map(|(version, _)| *version)
        .ok_or_else(|| invalid("missing source branch identity fork witness"))?;
    let head = state.graph_heads.get(logical);
    let witnessed = lineage.iter().any(|commit| {
        head == Some(&commit.graph_commit_id)
            && commit.graph_branch.as_deref() == Some(logical)
            && commit.graph_manifest_version > fork_version
            && commit.graph_manifest_version <= branch.version
    });
    if !witnessed {
        return Err(invalid(format!(
            "ambiguous source branch identity for '{native}': no post-fork logical-name witness; preserve the source and resolve branch naming with its executable before upgrade"
        )));
    }
    Ok(())
}

async fn retained_version_refs(
    dataset: &Dataset,
    limit: usize,
) -> Result<Vec<lance::dataset::VersionRef>> {
    let inventory_limit = u64::try_from(limit)
        .map_err(|error| invalid(error.to_string()))?
        .saturating_add(MAX_APPENDED_UPGRADE_VERSIONS);
    if dataset.count_versions().await.map_err(OmniError::storage)? > inventory_limit {
        return Err(invalid("storage upgrade retained-version limit exceeded"));
    }
    let mut versions = dataset.version_refs().await.map_err(OmniError::storage)?;
    if versions.len() as u64 > inventory_limit {
        return Err(invalid("storage upgrade retained-version limit exceeded"));
    }
    versions.retain(|version| version.version <= dataset.version().version);
    if versions.len() > limit {
        return Err(invalid("storage upgrade retained-version limit exceeded"));
    }
    Ok(versions)
}

fn compare_states(
    mut old: super::state::ManifestState,
    translated: &mut super::state::ManifestState,
) -> Result<()> {
    old.entries.sort_by_key(|entry| entry.identity);
    translated.entries.sort_by_key(|entry| entry.identity);
    if old.graph_heads != translated.graph_heads || old.entries.len() != translated.entries.len() {
        return Err(invalid(
            "registration-order damage: conversion would change the logical graph",
        ));
    }
    for (mut before, after) in old.entries.into_iter().zip(&translated.entries) {
        before.manifest_version = after.manifest_version;
        if !before.same_registration(after) {
            return Err(invalid(
                "registration-order damage: conversion would change logical rows; automatic repair is unsupported",
            ));
        }
    }
    Ok(())
}

async fn equivalent(source: &Dataset, target: &Dataset) -> Result<()> {
    let old = read_manifest_state(source).await?;
    let mut converted = read_manifest_state(target).await?;
    compare_states(old, &mut converted)
}

async fn publish_fence(dataset: Dataset, intent: String) -> Result<Dataset> {
    let operation = Operation::UpdateConfig {
        config_updates: None,
        table_metadata_updates: None,
        field_metadata_updates: HashMap::new(),
        schema_metadata_updates: Some(UpdateMap {
            update_entries: vec![
                (INTERNAL_SCHEMA_VERSION_KEY.to_string(), "7".to_string()).into(),
                (UPGRADE_PENDING_KEY.to_string(), intent).into(),
            ],
            replace: false,
        }),
    };
    let transaction = Transaction::new(dataset.version().version, operation, None);
    CommitBuilder::new(Arc::new(dataset))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(transaction)
        .await
        .map_err(OmniError::storage)
}

async fn publish_activation(dataset: Dataset) -> Result<Dataset> {
    let operation = Operation::UpdateConfig {
        config_updates: None,
        table_metadata_updates: None,
        field_metadata_updates: HashMap::new(),
        schema_metadata_updates: Some(UpdateMap {
            update_entries: vec![(UPGRADE_PENDING_KEY.to_string(), None::<String>).into()],
            replace: false,
        }),
    };
    let transaction = Transaction::new(dataset.version().version, operation, None);
    CommitBuilder::new(Arc::new(dataset))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(transaction)
        .await
        .map_err(OmniError::storage)
}

async fn publish_conversion(
    current: Dataset,
    source: Dataset,
    branch: &SourceBranch,
    intent: &UpgradeIntent,
) -> Result<()> {
    let mut metadata = source.schema().metadata.clone();
    metadata.insert(INTERNAL_SCHEMA_VERSION_KEY.into(), "7".into());
    metadata.remove(UPGRADE_PENDING_KEY);
    if branch.native.is_none() {
        metadata.insert(
            UPGRADE_PENDING_KEY.into(),
            serde_json::to_string(intent).map_err(|e| invalid(e.to_string()))?,
        );
    }
    metadata.insert(
        UPGRADE_RECEIPT_KEY.into(),
        serde_json::to_string(&receipt(branch, intent)).map_err(|e| invalid(e.to_string()))?,
    );
    let schema: Schema = source.schema().into();
    let schema = Arc::new(schema.with_metadata(metadata));
    let mut scan = source.scan();
    let mut columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    columns.push("_row_last_updated_at_version".into());
    scan.project(&columns).map_err(OmniError::storage)?;
    scan.batch_size(1024);
    let batches = scan.try_into_stream().await.map_err(OmniError::storage)?;
    let output_schema = Arc::clone(&schema);
    let mut seen = HashSet::new();
    let version = source.version().version;
    let converted = batches
        .map_err(datafusion::error::DataFusionError::from)
        .and_then(move |batch| {
            let result = convert_batch(batch, Arc::clone(&output_schema), version, &mut seen)
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)));
            futures::future::ready(result)
        });
    let stream: datafusion::physical_plan::SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(schema, converted));
    let params = WriteParams {
        mode: WriteMode::Overwrite,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        skip_auto_cleanup: true,
        max_rows_per_file: 64 * 1024,
        max_rows_per_group: 1024,
        ..Default::default()
    };
    let destination = Arc::new(current);
    let transaction = InsertBuilder::new(Arc::clone(&destination))
        .with_params(&params)
        .execute_uncommitted_stream(stream)
        .await
        .map_err(OmniError::storage)?;
    crate::failpoints::maybe_fail(crate::failpoints::names::UPGRADE_AFTER_STAGE)?;
    let target = CommitBuilder::new(destination)
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(transaction)
        .await
        .map_err(OmniError::storage)?;
    if !branch_completed(&target, branch, intent)? {
        return Err(invalid(
            "storage conversion publication did not carry its receipt",
        ));
    }
    equivalent(&source, &target).await
}

fn convert_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    source_version: u64,
    seen: &mut HashSet<String>,
) -> Result<RecordBatch> {
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .ok_or_else(|| invalid(format!("missing manifest column {name}")))
    };
    let ids = column("object_id")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("object_id must be Utf8"))?;
    let types = column("object_type")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("object_type must be Utf8"))?;
    let clocks = column("_row_last_updated_at_version")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("row update provenance must be UInt64"))?;
    let mut converted = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if ids.is_null(row) || types.is_null(row) {
            return Err(invalid("null manifest identity"));
        }
        if !matches!(
            types.value(row),
            OBJECT_TYPE_TABLE
                | OBJECT_TYPE_TABLE_VERSION
                | OBJECT_TYPE_TABLE_TOMBSTONE
                | OBJECT_TYPE_GRAPH_COMMIT
                | OBJECT_TYPE_GRAPH_HEAD
        ) {
            return Err(invalid(
                "source manifest contains an unsupported object type",
            ));
        }
        let id = if matches!(
            types.value(row),
            OBJECT_TYPE_TABLE_VERSION | OBJECT_TYPE_TABLE_TOMBSTONE
        ) {
            if clocks.is_null(row) || clocks.value(row) == 0 || clocks.value(row) > source_version {
                return Err(invalid("missing or invalid source registration provenance"));
            }
            let (prefix, _) = ids
                .value(row)
                .rsplit_once(':')
                .ok_or_else(|| invalid("invalid source registration key"))?;
            format!("{prefix}:{:020}", clocks.value(row))
        } else {
            ids.value(row).to_string()
        };
        if !seen.insert(id.clone()) {
            return Err(invalid("duplicate converted manifest identity"));
        }
        if seen.len() > MAX_ROWS {
            return Err(invalid("storage upgrade metadata-row limit exceeded"));
        }
        converted.push(id);
    }
    let mut output = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        output.push(if field.name() == "object_id" {
            Arc::new(StringArray::from(converted.clone())) as _
        } else {
            Arc::clone(column(field.name())?)
        });
    }
    RecordBatch::try_new(schema, output).map_err(|error| invalid(error.to_string()))
}

#[cfg(test)]
#[path = "upgrade/tests.rs"]
mod tests;

async fn validate_blobs(table: &Dataset, work: &mut UpgradeWork) -> Result<()> {
    let columns: Vec<_> = table
        .schema()
        .fields
        .iter()
        .filter(|field| field.is_blob())
        .map(|field| field.name.clone())
        .collect();
    if columns.is_empty() {
        return Ok(());
    }
    let table = Arc::new(table.clone());
    let mut scan = table.scan();
    scan.project::<&str>(&[]).map_err(OmniError::storage)?;
    scan.with_row_id();
    scan.batch_size(1024);
    let mut stream = scan.try_into_stream().await.map_err(OmniError::storage)?;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        let ids = batch
            .column_by_name("_rowid")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| invalid("missing stable row IDs while validating Blob dependencies"))?;
        if ids.null_count() != 0 {
            return Err(invalid("null stable row ID in Blob dependency scan"));
        }
        for column in &columns {
            for blob in table
                .take_blobs(ids.values(), column)
                .await
                .map_err(OmniError::storage)?
                .into_iter()
                .flatten()
            {
                if let Some(uri) = blob.uri() {
                    if work.external_blob_exclusions.len() >= MAX_ROWS {
                        return Err(invalid("external Blob dependency limit exceeded"));
                    }
                    work.external_blob_exclusions.insert(uri.to_string());
                } else {
                    let mut offset: u64 = 0;
                    while offset < blob.size() {
                        let end = offset.saturating_add(1024 * 1024).min(blob.size());
                        let bytes = blob
                            .read_range(offset..end)
                            .await
                            .map_err(OmniError::storage)?;
                        if bytes.len() as u64 != end - offset {
                            return Err(invalid("truncated managed Blob dependency"));
                        }
                        offset = end;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn validate_metadata_budget(dataset: &Dataset) -> Result<()> {
    if dataset.count_rows(None).await.map_err(OmniError::storage)? > MAX_ROWS {
        return Err(invalid("storage upgrade metadata-row limit exceeded"));
    }
    let mut scan = dataset.scan();
    scan.batch_size(1024);
    let mut stream = scan.try_into_stream().await.map_err(OmniError::storage)?;
    let mut bytes = 0usize;
    let mut rows = 0usize;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        bytes = bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or_else(|| invalid("metadata byte count overflow"))?;
        rows = rows
            .checked_add(batch.num_rows())
            .ok_or_else(|| invalid("metadata row count overflow"))?;
        if bytes > MAX_METADATA_BYTES || rows > MAX_ROWS {
            return Err(invalid(
                "storage upgrade metadata budget exceeded (1,000,000 rows or 64 MiB per retained manifest)",
            ));
        }
    }
    Ok(())
}
