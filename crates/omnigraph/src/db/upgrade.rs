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
use serde::Serialize;

use crate::error::{OmniError, Result};
use crate::storage::{normalize_root_uri, storage_for_uri};

use crate::db::manifest::layout::open_manifest_dataset_native_with_session;
use crate::db::manifest::migrations::{INTERNAL_SCHEMA_VERSION_KEY, is_served_stamp, read_stamp};
use crate::db::manifest::state::{
    flat_manifest_schema, read_manifest_state, read_manifest_state_with_registration_clocks,
};
use crate::db::manifest::{
    OBJECT_TYPE_GRAPH_COMMIT, OBJECT_TYPE_GRAPH_HEAD, OBJECT_TYPE_TABLE,
    OBJECT_TYPE_TABLE_TOMBSTONE, OBJECT_TYPE_TABLE_VERSION,
};
use crate::seams::{decide_seam, fail};

#[path = "upgrade/detached_only.rs"]
mod detached_only;
pub(crate) mod legacy_schema_files;
pub(crate) mod legacy_sidecars;

#[cfg(all(test, feature = "failpoints"))]
pub(super) use crate::db::manifest::migrations::BranchReceipt;
pub(super) use crate::db::manifest::migrations::{
    MAX_BRANCHES, MAX_INTENT_BYTES, SourceBranch, UPGRADE_PENDING_KEY, UPGRADE_RECEIPT_KEY,
    UpgradeIntent, branch_completed, fence_operation, historical_source, intent_from, invalid,
    receipt,
};
const HANDLER: &str = "registration-clocks-v6-to-v7";
const RETIREMENT_HANDLER: &str = "native-retirement-v7-to-v8";
const DETACHED_PINS_HANDLER: &str = "detached-pins-v8-v9-to-v10";
const DETACHED_ONLY_HANDLER: &str = "detached-only-v10-to-v11";
const SCHEMA_CONTRACT_HANDLER: &str = "schema-contract-v11-v12-to-v13";
const DEFAULT_TARGET: u32 = 13;
const RETIREMENT_KEY: &str = "omnigraph.retired_manifest_branch";
const MAX_VERSIONS: usize = 100_000;
const MAX_APPENDED_UPGRADE_VERSIONS: u64 = 3;
const MAX_ROWS: usize = 1_000_000;
const MAX_METADATA_BYTES: usize = 64 * 1024 * 1024;

/// Prepare a flat-v11 main manifest and return its legacy source, IR, and state text.
/// The DST harness persists these files before installing or enabling read weather.
#[cfg(feature = "dst")]
#[doc(hidden)]
pub async fn dst_prepare_legacy_upgrade_fixture(root: &str) -> Result<(String, String, String)> {
    let mut main = Dataset::open(&format!("{root}/__manifest"))
        .await
        .map_err(OmniError::storage)?;
    let row = crate::db::manifest::ManifestCoordinator::read_schema_contract_at(
        root,
        None,
        main.version().version,
    )
    .await?;
    let ir: omnigraph_compiler::SchemaIR =
        serde_json::from_str(&row.ir).map_err(|error| invalid(error.to_string()))?;
    let shape_hash = omnigraph_compiler::schema_shape_hash_from_ir(&ir)
        .map_err(|error| invalid(error.to_string()))?;
    let state = serde_json::json!({
        "format_version": 2,
        "schema_shape_hash": shape_hash,
        "schema_ir_hash": row.head.schema_ir_hash,
        "schema_identity_version": row.head.schema_identity_version,
        "schema_identity_domain": row.head.schema_identity_domain,
    })
    .to_string();
    crate::db::manifest::migrations::restamp_flat_for_test(&mut main, 11).await?;
    Ok((row.source, row.ir, state))
}

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
    pub deferred_checks: BTreeSet<String>,
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
            failed_handler: self.route.get(self.completed_handlers.len()).cloned()
                .unwrap_or_else(|| "storage-upgrade".into()),
            executable_compatibility: "this storage-upgrade-capable executable; preserve the exact pending intent and do not use the source executable".into(),
            action: format!("stop all writers and maintenance, retain the backup, then rerun the same upgrade command with --to-format {} without --check", self.target_format),
        });
    }
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

async fn run(
    root: &str,
    options: UpgradeOptions,
    actor: Option<&str>,
    policy: Option<&dyn omnigraph_policy::PolicyChecker>,
    report: &mut UpgradeReport,
) -> Result<()> {
    if !matches!(report.target_format, 7 | 8 | 10 | 11 | 13) {
        if let Ok(main) = open(root, None).await {
            report.observed_format = read_stamp(&main);
        }
        report.finding(
            "unsupported_target",
            "this binary has registered routes to formats 7, 8, 10, 11 and 13 only; v9 was the system-column vintage, which since v10 is converted on a served graph by `omnigraph schema upgrade-system-columns`",
        );
        return Ok(());
    }
    let _root_exclusion = if options.check {
        None
    } else {
        Some(crate::db::reserve_export_root_exclusion(root)?)
    };
    for _ in 0..5 {
        let main = open(root, None).await?;
        let stamp = read_stamp(&main);
        if report.observed_format.is_none() {
            report.observed_format = stamp;
        }
        let pending = match intent_from(&main) {
            Ok(pending) => pending,
            Err(error) => {
                report.recover("unknown_upgrade_ownership", error.to_string());
                return Ok(());
            }
        };
        if pending
            .as_ref()
            .is_some_and(|intent| intent.target_format > report.target_format)
        {
            report.recover(
                "incompatible_pending_target",
                "the pending upgrade cannot be resumed toward a lower target",
            );
            if let Some(recovery) = &mut report.recovery {
                recovery.action = format!(
                    "stop all writers and maintenance and rerun this executable with --to-format {}; do not alter the pending intent",
                    pending
                        .as_ref()
                        .map_or(DEFAULT_TARGET, |intent| intent.target_format)
                );
            }
            return Ok(());
        }
        let step_target = pending
            .as_ref()
            .map(|intent| intent.target_format)
            .unwrap_or_else(|| match stamp {
                Some(6) => 7,
                Some(7) => report.target_format.min(8),
                Some(8) | Some(9) => report.target_format.min(10),
                Some(10) => report.target_format.min(11),
                Some(stamp) if stamp >= 11 => report.target_format,
                _ => report.target_format.min(8),
            });
        let initial_step = pending
            .as_ref()
            .map(|intent| intent.source_format)
            .unwrap_or_else(|| stamp.unwrap_or(0));
        if initial_step == 6 && report.target_format >= 8 {
            report.route = vec![HANDLER.into(), RETIREMENT_HANDLER.into()];
        } else if initial_step == 7 && report.target_format >= 8 && report.route.is_empty() {
            report.route.push(RETIREMENT_HANDLER.into());
        }
        if matches!(initial_step, 6..=9)
            && report.target_format >= 10
            && !report
                .route
                .iter()
                .any(|entry| entry == DETACHED_PINS_HANDLER)
        {
            report.route.push(DETACHED_PINS_HANDLER.into());
        }
        if matches!(initial_step, 6..=10)
            && report.target_format >= 11
            && !report
                .route
                .iter()
                .any(|entry| entry == DETACHED_ONLY_HANDLER)
        {
            report.route.push(DETACHED_ONLY_HANDLER.into());
        }
        if matches!(initial_step, 6..=12)
            && report.target_format == 13
            && !report
                .route
                .iter()
                .any(|entry| entry == SCHEMA_CONTRACT_HANDLER)
        {
            report.route.push(SCHEMA_CONTRACT_HANDLER.into());
        }
        run_step(root, options, actor, policy, report, step_target).await?;
        if options.check {
            if step_target < report.target_format && report.success() {
                if step_target == 7 {
                    report.work.deferred_checks.insert("v7-to-v8 preflight must validate the converted v7 output before the second handler has effects".into());
                }
                if step_target < 10 && report.target_format >= 10 {
                    report.work.deferred_checks.insert("the v10 stamp step validates the converted v8 output's live branches before it has effects".into());
                }
                if step_target < 11 && report.target_format >= 11 {
                    report.work.deferred_checks.insert("the v11 step judges the converted v10 output's pins on every live branch before it has effects".into());
                }
                if report.target_format == 13 {
                    report.work.deferred_checks.insert("the v13 step validates legacy contract bytes and each branch's v11/v12 layout before it has effects".into());
                }
            }
            return Ok(());
        }
        if !report.success() || step_target == report.target_format {
            return Ok(());
        }
        report.work.deferred_checks.clear();
    }
    Err(invalid(
        "storage upgrade route exceeded its five registered steps",
    ))
}

decide_seam! {
    pub static UPGRADE_BEFORE_ACTIVATION = ("upgrade.before_activation", Unreachable, [Fail]);
}

decide_seam! {
    pub static UPGRADE_AFTER_BRANCH = ("upgrade.after_branch", Unreachable, [Fail]);
}

decide_seam! {
    pub static UPGRADE_AFTER_FENCE = ("upgrade.after_fence", Unreachable, [Fail]);
}

decide_seam! {
    pub static UPGRADE_AFTER_ACTIVATION = ("upgrade.after_activation", Unreachable, [Fail]);
}

decide_seam! {
    pub static UPGRADE_AFTER_SCHEMA_FILE_DELETE = ("upgrade.after_schema_file_delete", Unreachable, [Fail]);
}

fn refuse_legacy_sentinel(native: &str) -> Result<()> {
    if crate::branch_names::logical_branch_name(native).trim_start_matches('/')
        == "__schema_apply_lock__"
    {
        return Err(invalid(
            "legacy schema-apply sentinel requires the source-compatible executable before conversion",
        ));
    }
    Ok(())
}

async fn validated_manifest_contract(
    dataset: &Dataset,
) -> Result<crate::db::manifest::SchemaContractRow> {
    let row = crate::db::manifest::migrations::read_upgrade_schema_contract(dataset).await?;
    crate::db::schema_state::refuse_unsupported_schema_versions(&row.ir)?;
    let (ir, _) = crate::db::schema_state::validate_schema_contract_row(&row)?;
    let state = read_manifest_state(dataset).await?;
    crate::db::schema_state::validate_schema_ir_against_entries(
        &ir,
        state.entries.iter(),
        state.version,
    )?;
    Ok(row)
}

async fn validate_converted_contract(dataset: &Dataset, intent: &UpgradeIntent) -> Result<()> {
    let contract = validated_manifest_contract(dataset).await?;
    intent
        .schema_contract
        .as_ref()
        .ok_or_else(|| invalid("schema-contract upgrade has no exact contract ownership"))?
        .validate_row(&contract)
}

/// Whether a live branch at `branch_stamp` belongs to a current graph whose
/// main is at `main_stamp`: a served stamp when main is served, otherwise
/// main's exact intermediate target stamp.
fn branch_stamp_is_current(main_stamp: u32, branch_stamp: Option<u32>) -> bool {
    if is_served_stamp(main_stamp) {
        branch_stamp.is_some_and(is_served_stamp)
    } else {
        branch_stamp == Some(main_stamp)
    }
}

async fn run_step(
    root: &str,
    options: UpgradeOptions,
    actor: Option<&str>,
    policy: Option<&dyn omnigraph_policy::PolicyChecker>,
    report: &mut UpgradeReport,
    step_target: u32,
) -> Result<()> {
    report.outcome = UpgradeOutcome::CheckFailed;
    let main = open(root, None).await?;
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
    let storage = crate::storage::decorate(storage_for_uri(root)?);
    let sidecars =
        crate::db::upgrade::legacy_sidecars::pending_legacy_sidecars(root, storage.as_ref())
            .await?;
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
    let stamp = read_stamp(&main);
    let served_at_or_above_target = stamp.is_some_and(|stamp| {
        stamp >= step_target
            && step_target >= crate::db::manifest::migrations::MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION
            && stamp <= crate::db::manifest::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION
    });
    if pending.is_none()
        && let Some(expected) = stamp
        && (expected == step_target || served_at_or_above_target)
    {
        if expected >= crate::db::manifest::migrations::MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION {
            crate::db::manifest::migrations::guard_stamp(&main)?;
        }
        read_manifest_state(&main).await?;
        let current_contract = if expected == 13 {
            Some(validated_manifest_contract(&main).await?)
        } else {
            legacy_schema_files::refuse_staging(root, storage.as_ref()).await?;
            None
        };
        let legacy = if current_contract.is_none() {
            Some(
                legacy_schema_files::load_validated_schema_contract(root, Arc::clone(&storage))
                    .await?
                    .into_row(),
            )
        } else {
            None
        };
        let contract = current_contract
            .as_ref()
            .or(legacy.as_ref())
            .ok_or_else(|| invalid("current graph has no schema contract"))?;
        report.graph_identity = Some(contract.head.schema_identity_domain.clone());
        let branches = if expected >= 8 {
            crate::branch_control::list_live_manifest_branch_contents(&main).await?
        } else {
            legacy_branch_contents(&main).await?
        };
        if branches.len() >= MAX_BRANCHES {
            return Err(invalid("storage upgrade branch limit exceeded"));
        }
        let mut logical_names = HashSet::from(["main".to_string()]);
        for native in branches.keys() {
            if expected < 13 {
                refuse_legacy_sentinel(native)?;
            }
            if !logical_names.insert(crate::branch_names::logical_branch_name(native).to_string()) {
                return Err(invalid(
                    "current graph contains duplicate live logical branch names",
                ));
            }
            let branch = main
                .checkout_branch(native)
                .await
                .map_err(OmniError::storage)?;
            let branch_stamp = read_stamp(&branch);
            if !branch_stamp_is_current(expected, branch_stamp)
                || branch.schema().metadata.contains_key(UPGRADE_PENDING_KEY)
            {
                return Err(invalid(format!(
                    "current graph contains a branch with an incompatible format or pending upgrade: '{native}' is stamped {} while main is v{expected}",
                    branch_stamp.map_or("unreadably".to_string(), |stamp| format!("v{stamp}"))
                )));
            }
            read_manifest_state(&branch).await?;
            if let Some(contract) = &current_contract {
                if validated_manifest_contract(&branch).await? != *contract {
                    return Err(invalid("current branch schema contract differs from main"));
                }
            }
        }
        report.outcome = if report.completed_handlers.is_empty() {
            UpgradeOutcome::AlreadyCurrent
        } else {
            UpgradeOutcome::Completed
        };
        return Ok(());
    }
    if pending.is_none()
        && let Some(stamp) = stamp
        && stamp > crate::db::manifest::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION
    {
        report.finding(
            "newer_than_binary",
            format!(
                "this graph is stamped v{stamp}, newer than the v{} this executable serves; upgrade omnigraph before touching it",
                crate::db::manifest::migrations::INTERNAL_MANIFEST_SCHEMA_VERSION
            ),
        );
        return Ok(());
    }
    if pending.is_none()
        && let Some(stamp) = stamp
        && stamp > step_target
        && step_target < crate::db::manifest::migrations::MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION
    {
        report.finding(
            "target_below_stamp",
            format!(
                "this graph is stamped v{stamp}; the requested target v{step_target} is below the oldest format this executable serves (v{}), and no downgrade route exists",
                crate::db::manifest::migrations::MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION
            ),
        );
        return Ok(());
    }
    if pending.is_none()
        && !matches!(
            (read_stamp(&main), step_target),
            (Some(6), 7)
                | (Some(7), 8)
                | (Some(8), 10)
                | (Some(9), 10)
                | (Some(10), 11)
                | (Some(11), 13)
                | (Some(12), 13)
        )
    {
        report.finding("unsupported_source", "only validated v6, v7, v8, v9, v10, v11 and v12 graphs have conversion handlers; preserve the source and use its executable for export/import");
        return Ok(());
    }
    let resumed = pending.is_some();
    legacy_schema_files::refuse_staging(root, storage.as_ref()).await?;
    let contract = if let Some(intent) = pending.as_ref().filter(|intent| intent.protocol == 5) {
        let mut complete = true;
        for branch in &intent.branches {
            let current = open(root, branch.native.as_deref()).await?;
            if branch_completed(&current, branch, intent)? {
                validate_converted_contract(&current, intent).await?;
            } else {
                complete = false;
            }
        }
        if complete {
            verify_inventory(root, intent, true).await?;
            validated_manifest_contract(&main).await?
        } else {
            legacy_schema_files::load_validated_schema_contract(root, Arc::clone(&storage))
                .await?
                .into_row()
        }
    } else {
        legacy_schema_files::load_validated_schema_contract(root, Arc::clone(&storage))
            .await?
            .into_row()
    };
    report.graph_identity = Some(contract.head.schema_identity_domain.clone());
    let mut intent = match pending {
        Some(intent) => {
            if intent.graph_identity != contract.head.schema_identity_domain {
                return Err(invalid("upgrade schema identity changed"));
            }
            if let Some(expected) = &intent.schema_contract {
                expected.validate_row(&contract)?;
            }
            intent
        }
        None => inventory(&main, contract.head.schema_identity_domain.clone()).await?,
    };
    if intent.protocol == 5 && intent.schema_contract.is_none() {
        intent.schema_contract =
            Some(crate::db::manifest::migrations::UpgradeSchemaContract::from_row(&contract));
    }
    let handler = match intent.source_format {
        6 => HANDLER,
        7 => RETIREMENT_HANDLER,
        8 | 9 => DETACHED_PINS_HANDLER,
        10 => DETACHED_ONLY_HANDLER,
        _ => SCHEMA_CONTRACT_HANDLER,
    };
    if !report.route.iter().any(|entry| entry == handler) {
        report.route.push(handler.into());
    }
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
    if handler == DETACHED_ONLY_HANDLER && !resumed {
        let blocked = if options.check {
            detached_only::preflight(root, &contract).await?
        } else {
            let work = detached_only::execute(root, &contract).await?;
            tracing::info!(
                promoted = work.promoted,
                reaped = work.reaped,
                recorded = work.recorded,
                published_branches = work.published_branches,
                "detached-only conversion: pins promoted, copies reaped, last linear versions recorded"
            );
            work.blocked
        };
        if !blocked.is_empty() {
            for pin in blocked {
                report.finding("blocked_promotion", pin);
            }
            return Ok(());
        }
        if !options.check {
            let main = open(root, None).await?;
            intent = inventory(&main, contract.head.schema_identity_domain.clone()).await?;
        }
    }
    verify_inventory(root, &intent, report.recovery.is_some()).await?;
    preflight(root, &intent, &contract, &mut report.work).await?;
    if options.check {
        if report.recovery.is_none() {
            report.outcome = UpgradeOutcome::CheckPassed;
        }
        return Ok(());
    }
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
        publish_fence(main, json, intent.target_format).await?;
    }
    report.last_durable_completed_boundary = Some("source_fenced".into());
    fail(&UPGRADE_AFTER_FENCE)?;
    for branch in &intent.branches {
        let current = open(root, branch.native.as_deref()).await?;
        if crate::branch_control::dataset_branch_identifier(&current)
            .await
            .map_err(OmniError::storage)?
            != branch.identity
        {
            return Err(invalid("native branch lifetime changed before conversion"));
        }
        if branch_completed(&current, branch, &intent)? {
            continue;
        }
        verify_source_head(&current, branch, branch.native.is_none(), &intent)?;
        let source = current
            .checkout_version(branch.version)
            .await
            .map_err(OmniError::storage)?;
        publish_conversion(current, source, branch, &intent, &contract).await?;
        report.last_durable_completed_boundary = Some(format!(
            "converted:{}",
            branch.native.as_deref().unwrap_or("main")
        ));
        fail(&UPGRADE_AFTER_BRANCH)?;
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
        if intent.protocol == 5 {
            validate_converted_contract(&current, &intent).await?;
        }
    }
    verify_inventory(root, &intent, true).await?;
    let main = open(root, None).await?;
    if intent_from(&main)?.as_ref() != Some(&intent) {
        return Err(invalid("activation ownership changed"));
    }
    if intent.protocol == 5 {
        legacy_schema_files::cleanup(root, storage.as_ref(), &contract).await?;
        verify_inventory(root, &intent, true).await?;
    }
    fail(&UPGRADE_BEFORE_ACTIVATION)?;
    publish_activation(main).await?;
    report.last_durable_completed_boundary = Some("activated".into());
    fail(&UPGRADE_AFTER_ACTIVATION)?;
    report.outcome = UpgradeOutcome::Completed;
    report.completed_handlers.push(handler.into());
    report.recovery = None;
    report.findings.clear();
    Ok(())
}

async fn legacy_branch_contents(
    main: &Dataset,
) -> Result<HashMap<String, lance::dataset::refs::BranchContents>> {
    let branches = crate::branch_control::list_branch_contents(main).await?;
    if branches
        .values()
        .any(|contents| contents.metadata.contains_key(RETIREMENT_KEY))
    {
        return Err(invalid(
            "v6/v7 source contains reserved retirement metadata; ownership cannot be inferred",
        ));
    }
    Ok(branches)
}

/// The live branch census of a source. v6/v7 sources predate retirement
/// metadata and refuse any; v8 and later sources list live logical refs only,
/// so retired ancestors keep their stamp and stay out of the conversion.
async fn source_branch_contents(
    main: &Dataset,
    source_format: u32,
) -> Result<HashMap<String, lance::dataset::refs::BranchContents>> {
    if source_format >= 8 {
        crate::branch_control::list_live_manifest_branch_contents(main).await
    } else {
        legacy_branch_contents(main).await
    }
}

async fn inventory(main: &Dataset, graph_identity: String) -> Result<UpgradeIntent> {
    let source_format = read_stamp(main).ok_or_else(|| invalid("source stamp is missing"))?;
    let branches = source_branch_contents(main, source_format).await?;
    if branches.len() >= MAX_BRANCHES {
        return Err(invalid("storage upgrade branch limit exceeded"));
    }
    let mut names: Vec<_> = branches.into_keys().collect();
    names.sort();
    let mut sources = Vec::with_capacity(names.len() + 1);
    for native in names {
        refuse_legacy_sentinel(&native)?;
        let ds = main
            .checkout_branch(&native)
            .await
            .map_err(OmniError::storage)?;
        sources.push(SourceBranch {
            native: Some(native),
            identity: crate::branch_control::dataset_branch_identifier(&ds)
                .await
                .map_err(OmniError::storage)?,
            version: ds.version().version,
        });
    }
    sources.push(SourceBranch {
        native: None,
        identity: crate::branch_control::dataset_branch_identifier(main)
            .await
            .map_err(OmniError::storage)?,
        version: main.version().version,
    });
    Ok(UpgradeIntent {
        protocol: match source_format {
            6 => 1,
            7 => 2,
            8 | 9 => 3,
            10 => 4,
            _ => 5,
        },
        attempt: ulid::Ulid::new().to_string(),
        source_format,
        target_format: match source_format {
            6 => 7,
            7 => 8,
            8 | 9 => 10,
            10 => 11,
            _ => DEFAULT_TARGET,
        },
        graph_identity,
        branches: sources,
        schema_contract: None,
    })
}

fn verify_source_head(
    dataset: &Dataset,
    source: &SourceBranch,
    fenced: bool,
    intent: &UpgradeIntent,
) -> Result<()> {
    let expected = source
        .version
        .checked_add(u64::from(fenced))
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    if dataset.version().version != expected {
        return Err(invalid("source branch changed; refusing foreign movement"));
    }
    let stamp = read_stamp(dataset);
    let valid_stamp = if fenced {
        stamp == Some(intent.target_format)
    } else if intent.protocol == 5 {
        matches!(stamp, Some(11 | 12))
    } else {
        stamp == Some(intent.source_format)
    };
    if !valid_stamp {
        return Err(invalid("source branch has an incompatible format"));
    }
    Ok(())
}

async fn verify_inventory(root: &str, intent: &UpgradeIntent, fenced: bool) -> Result<()> {
    let main = open(root, None).await?;
    let observed: BTreeSet<_> = source_branch_contents(&main, intent.source_format)
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
        if crate::branch_control::dataset_branch_identifier(&dataset)
            .await
            .map_err(OmniError::storage)?
            != source.identity
        {
            return Err(invalid("native branch lifetime changed during upgrade"));
        }
        if !branch_completed(&dataset, source, intent)? {
            verify_source_head(&dataset, source, fenced && source.native.is_none(), intent)?;
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

async fn preflight(
    root: &str,
    intent: &UpgradeIntent,
    contract: &crate::db::manifest::SchemaContractRow,
    work: &mut UpgradeWork,
) -> Result<()> {
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
        if if intent.protocol == 5 {
            !matches!(read_stamp(&source), Some(11 | 12))
        } else {
            read_stamp(&source) != Some(intent.source_format)
        } {
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
        let (old, lineage) =
            crate::db::manifest::state::read_manifest_state_and_lineage(&source).await?;
        if intent.protocol == 5 {
            if old.schema_contract.is_some() {
                return Err(invalid(
                    "legacy source unexpectedly carries a schema contract row",
                ));
            }
            let (ir, _) = crate::db::schema_state::validate_schema_contract_row(contract)?;
            crate::db::schema_state::validate_schema_ir_against_entries(
                &ir,
                old.entries.iter(),
                old.version,
            )?;
        }
        if intent.source_format == 6 {
            validate_source_branch_identity(branch, &old, &lineage)?;
        } else if branch.native.is_some()
            && (branch.identity == BranchIdentifier::main()
                || branch.identity == BranchIdentifier::missing_identifier_sentinel())
        {
            return Err(invalid("v7 native branch lacks an exact lifetime identity"));
        }
        if intent.source_format == 6 {
            let mut translated = read_manifest_state_with_registration_clocks(&source).await?;
            compare_states(old, &mut translated)?;
        }
        let schema: Schema = source.schema().into();
        let expected = flat_manifest_schema();
        if intent.protocol == 5 {
            crate::db::manifest::migrations::validate_schema_contract_source(&source)?;
        } else if schema.fields().len() != expected.fields().len()
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
            if intent.source_format == 6 {
                convert_batch(
                    batch.clone(),
                    Arc::clone(&schema),
                    branch.version,
                    &mut seen,
                )?;
            }
            count = count
                .checked_add(batch.num_rows())
                .ok_or_else(|| invalid("metadata row count overflow"))?;
            if count > MAX_ROWS {
                return Err(invalid("storage upgrade metadata-row limit exceeded"));
            }
        }
        work.metadata_rows += u64::try_from(count).map_err(|e| invalid(e.to_string()))?;
        if intent.source_format >= 8 {
            continue;
        }
        let versions = retained_version_refs(&source, MAX_VERSIONS).await?;
        for version in versions {
            let snapshot = source
                .checkout_version(version.version)
                .await
                .map_err(OmniError::storage)?;
            let snapshot = historical_source(snapshot, intent.source_format).await?;
            let unstamped_bootstrap = matches!(snapshot.version().version, 1 | 2)
                && !snapshot
                    .schema()
                    .metadata
                    .contains_key(INTERNAL_SCHEMA_VERSION_KEY);
            if !matches!(read_stamp(&snapshot), Some(6))
                && !(intent.source_format == 7 && read_stamp(&snapshot) == Some(7))
                && !unstamped_bootstrap
            {
                return Err(invalid(format!(
                    "retained history contains an unsupported format at {:?} version {}",
                    branch.native,
                    snapshot.version().version
                )));
            }
            confined(root, &snapshot)?;
            validate_metadata_budget(&snapshot).await?;
            let (state, lineage) =
                crate::db::manifest::state::read_manifest_state_and_lineage(&snapshot).await?;
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
    state: &crate::db::manifest::state::ManifestState,
    lineage: &[crate::db::manifest::state::GraphLineageRow],
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
    mut old: crate::db::manifest::state::ManifestState,
    translated: &mut crate::db::manifest::state::ManifestState,
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

async fn publish_fence(dataset: Dataset, intent: String, target: u32) -> Result<Dataset> {
    let transaction = Transaction::new(
        dataset.version().version,
        fence_operation(intent, target),
        None,
    );
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

decide_seam! {
    pub static UPGRADE_AFTER_STAGE = ("upgrade.after_stage", Unreachable, [Fail]);
}

decide_seam! {
    /// Between two reaps of one promoted chain in the v11 step, oldest link
    /// deleted first; a retry after a failure here rediscovers the rest from
    /// the registered tip.
    pub static UPGRADE_DETACHED_ONLY_BETWEEN_REAPS = ("upgrade.detached_only_between_reaps", Unreachable, [Fail]);
}

async fn publish_conversion(
    current: Dataset,
    source: Dataset,
    branch: &SourceBranch,
    intent: &UpgradeIntent,
    contract: &crate::db::manifest::SchemaContractRow,
) -> Result<()> {
    let mut metadata = source.schema().metadata.clone();
    metadata.insert(
        INTERNAL_SCHEMA_VERSION_KEY.into(),
        intent.target_format.to_string(),
    );
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
    let destination = Arc::new(current);
    let transaction = if intent.source_format != 6 && intent.protocol != 5 {
        let operation = Operation::UpdateConfig {
            config_updates: None,
            table_metadata_updates: None,
            field_metadata_updates: HashMap::new(),
            schema_metadata_updates: Some(UpdateMap {
                update_entries: metadata
                    .into_iter()
                    .map(|(key, value)| (key, value).into())
                    .collect(),
                replace: true,
            }),
        };
        Transaction::new(destination.version().version, operation, None)
    } else {
        let stream: datafusion::physical_plan::SendableRecordBatchStream = if intent.protocol == 5 {
            let (schema, batches) =
                crate::db::manifest::migrations::schema_contract_conversion_batches(
                    &source, metadata, contract,
                )
                .await?;
            Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(batches.into_iter().map(Ok)),
            ))
        } else {
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
                    let result =
                        convert_batch(batch, Arc::clone(&output_schema), version, &mut seen)
                            .map_err(|error| {
                                datafusion::error::DataFusionError::External(Box::new(error))
                            });
                    futures::future::ready(result)
                });
            Box::pin(RecordBatchStreamAdapter::new(schema, converted))
        };
        let params = WriteParams {
            mode: WriteMode::Overwrite,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            skip_auto_cleanup: true,
            max_rows_per_file: 64 * 1024,
            max_rows_per_group: 1024,
            ..Default::default()
        };
        InsertBuilder::new(Arc::clone(&destination))
            .with_params(&params)
            .execute_uncommitted_stream(stream)
            .await
            .map_err(OmniError::storage)?
    };
    fail(&UPGRADE_AFTER_STAGE)?;
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
    equivalent(&source, &target).await?;
    if intent.protocol == 5 {
        validate_converted_contract(&target, intent).await?;
    }
    Ok(())
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
