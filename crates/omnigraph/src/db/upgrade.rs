//! `omnigraph upgrade`: the offline conversion of a standalone root from
//! storage format 13 to 14. The upgrade reads every ref of `__manifest` at a
//! pinned version, fences main with its intent, writes the graph commits of
//! the source as immutable objects under `__history/legacy/`, converts every
//! live ref to its head alone, validates what it wrote and activates main. A
//! run that stops after the fence is resumed by running the command again.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::refs::{BranchContents, Ref};
use lance::dataset::transaction::{Operation, Transaction, UpdateMap};
use lance::dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use lance::session::Session;
use lance_file::version::LanceFileVersion;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::db::legacy_sidecars::pending_legacy_sidecars;
use crate::db::manifest::TableIdentity;
use crate::db::manifest::history::{self, HistoryRecord, LegacyDirectory, LegacyLayout};
use crate::db::manifest::layout::open_manifest_dataset_native_with_session;
use crate::db::manifest::legacy::{
    self, CensusError, CensusInput, CensusRef, HeadScan, LegacyCensus, LegacyManifestSource,
    LegacyPlan, MAX_CENSUS_CELLS, MAX_CENSUS_SNAPSHOT_BYTES, SourceRole, Stamp13Source,
};
use crate::db::manifest::migrations::{
    INTERNAL_MANIFEST_SCHEMA_VERSION, INTERNAL_SCHEMA_VERSION_KEY, MAX_BRANCHES, MAX_INTENT_BYTES,
    SourceBranch, UPGRADE_PENDING_KEY, UPGRADE_PROTOCOL, UPGRADE_RECEIPT_KEY,
    UPGRADE_SOURCE_FORMAT, UpgradeIntent, UpgradeSchemaContract, branch_completed, fence_operation,
    guard_stamp, intent_from, read_stamp, receipt, recovery_guidance,
};
use crate::db::manifest::retention::retired_manifest_branches;
use crate::db::manifest::state::{TablePin, TableState, read_converted_state};
use crate::error::{OmniError, Result};
use crate::seams::{decide_seam, fail};
use crate::storage::{normalize_root_uri, storage_for_uri};

const HANDLER: &str = "history-lance-files-v13-to-v14";
/// The rows one head of `__manifest` may hold.
const MAX_ROWS: usize = 1_000_000;
/// The decoded bytes one head of `__manifest` may hold.
const MAX_METADATA_BYTES: usize = 64 * 1024 * 1024;
/// Object keys one finding names before it stops listing.
const NAMED_OBJECTS: usize = 20;
/// What an unfenced refusal of a retired ref tells the operator: its head is
/// immutable, so only removing the ref admits the graph.
const RETIRED_REMEDY: &str = "; `omnigraph cleanup --keep <N> --confirm <graph>` with the build \
     that wrote the graph removes a retired branch that no live branch, merge base or tag needs: \
     run it, then rerun the upgrade check; rebuild the graph only when the branch stays";

/// What one upgrade admits of its census sources before it scans any head.
#[derive(Debug, Clone, Copy)]
struct Bounds {
    census_cells: u64,
    census_snapshot_bytes: u64,
    head_rows: usize,
    head_bytes: usize,
    retired_refs: usize,
}

impl Bounds {
    const SERVED: Self = Self {
        census_cells: MAX_CENSUS_CELLS,
        census_snapshot_bytes: MAX_CENSUS_SNAPSHOT_BYTES,
        head_rows: MAX_ROWS,
        head_bytes: MAX_METADATA_BYTES,
        retired_refs: MAX_BRANCHES,
    };
}

/// Storage upgrade options. `to_format` defaults to the format this binary serves.
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

/// What one run read of the source and what it writes under
/// `__history/legacy/`. A run resumed after the legacy objects exist reads no
/// census, so its census counts are zero and its object counts are the
/// directory's.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct UpgradeWork {
    pub live_refs: u64,
    pub retired_refs: u64,
    pub orphan_writers: u64,
    pub legacy_commits: u64,
    pub bookkeeping_versions: u64,
    pub absent_parents: u64,
    pub data_files: u64,
    pub id_shards: u64,
    pub writer_shards: u64,
    pub schema_contents: u64,
    pub census_reads: u64,
    pub census_cells: u64,
    pub legacy_bytes: u64,
}

impl UpgradeWork {
    fn of_census(census: &LegacyCensus, plan: &LegacyPlan) -> Self {
        let counts = &census.counts;
        Self {
            live_refs: counts.live_refs,
            retired_refs: counts.retired_refs,
            orphan_writers: counts.orphan_writers,
            legacy_commits: counts.legacy_commits,
            bookkeeping_versions: counts.bookkeeping_versions,
            absent_parents: counts.absent_parents,
            data_files: u64::from(plan.data_files),
            id_shards: u64::from(plan.id_shards),
            writer_shards: u64::from(plan.writer_shards),
            schema_contents: counts.schema_contents,
            census_reads: counts.census_reads,
            census_cells: counts.census_cells,
            legacy_bytes: counts.legacy_bytes,
        }
    }

    fn of_directory(directory: &LegacyDirectory, intent: &UpgradeIntent) -> Self {
        Self {
            live_refs: intent.branches.len() as u64,
            legacy_commits: directory.commits,
            absent_parents: directory.absent_parents.len() as u64,
            data_files: directory.files.len() as u64,
            id_shards: directory.id_shards.len() as u64,
            writer_shards: directory.writer_shards.len() as u64,
            ..Self::default()
        }
    }
}

/// What the operator does about a fenced graph the run left unfinished.
#[derive(Debug, Clone, Copy)]
enum Remedy {
    /// This executable owns the attempt and resumes it.
    Rerun,
    /// The owner of the attempt is not established: nothing is rerun.
    PreserveAndCheck,
    /// The plan or the legacy objects are not the ones this executable makes.
    FencingExecutable,
}

impl Remedy {
    fn executable(self) -> &'static str {
        match self {
            Self::Rerun => "this omnigraph executable",
            Self::PreserveAndCheck => "the omnigraph executable that started the conversion",
            Self::FencingExecutable => "the omnigraph executable that fenced the graph",
        }
    }

    fn action(self, target_format: u32) -> String {
        const OFFLINE: &str = "keep the graph offline: stop all readers, writers and maintenance";
        match self {
            Self::Rerun => format!(
                "{OFFLINE}, retain the backup taken before the attempt, then rerun `omnigraph \
                 upgrade <graph> --to-format {target_format}` without `--check`"
            ),
            Self::PreserveAndCheck => format!(
                "{OFFLINE}, preserve the graph root and the backup taken before the attempt, run \
                 `omnigraph upgrade <graph> --check` for read-only diagnostics, and finish the \
                 conversion with the omnigraph executable that started it; do not rerun it \
                 without `--check` with this executable"
            ),
            Self::FencingExecutable => format!(
                "{OFFLINE}, preserve the graph root and the backup taken before the attempt, and \
                 finish the upgrade with the omnigraph executable that fenced the graph; do not \
                 rerun it with this executable. When a legacy object under `__history/legacy/` \
                 was modified, restore the whole root from that backup first"
            ),
        }
    }
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

    fn started(location: String, options: UpgradeOptions) -> Self {
        Self {
            mode: if options.check {
                UpgradeMode::Check
            } else {
                UpgradeMode::Execute
            },
            outcome: UpgradeOutcome::CheckFailed,
            location,
            graph_identity: None,
            observed_format: None,
            target_format: options
                .to_format
                .unwrap_or(INTERNAL_MANIFEST_SCHEMA_VERSION),
            target_defaulted: options.to_format.is_none(),
            route: Vec::new(),
            completed_handlers: Vec::new(),
            findings: Vec::new(),
            last_durable_completed_boundary: None,
            recovery: None,
            work: UpgradeWork::default(),
        }
    }

    /// The run stopped on `error`: a rerun resumes a fenced graph, and before
    /// the fence nothing was written. A recovery already reported is kept.
    fn failed(&mut self, error: &OmniError) {
        if self.last_durable_completed_boundary.is_some() {
            self.recover("upgrade_interrupted", error.to_string(), Remedy::Rerun);
        } else {
            self.finding("preflight_failed", error.to_string());
        }
    }

    fn finding(&mut self, code: &str, message: impl Into<String>) {
        self.findings.push(UpgradeFinding {
            code: code.into(),
            message: message.into(),
        });
    }

    fn recover(&mut self, code: &str, message: impl Into<String>, remedy: Remedy) {
        self.outcome = UpgradeOutcome::RecoveryRequired;
        self.finding(code, message);
        self.recovery = Some(UpgradeRecovery {
            failed_handler: HANDLER.into(),
            executable_compatibility: remedy.executable().into(),
            action: remedy.action(self.target_format),
        });
    }

    fn census_finding(&mut self, error: CensusError, fenced: bool) -> Result<()> {
        match error {
            CensusError::Finding(finding) if fenced => {
                self.recover(
                    finding.code.as_str(),
                    finding.message,
                    Remedy::FencingExecutable,
                );
                Ok(())
            }
            CensusError::Finding(finding) => {
                self.finding(finding.code.as_str(), finding.message);
                Ok(())
            }
            CensusError::Read(error) => Err(error),
        }
    }
}

/// Convert a standalone graph offline, or with `check` report whether it can be
/// converted; `check` writes nothing. Normal open never starts or resumes this.
pub async fn upgrade_storage(uri: &str, options: UpgradeOptions) -> Result<UpgradeReport> {
    upgrade_storage_as(uri, options, None, None).await
}

/// [`upgrade_storage`] with a caller identity: `SchemaApply` is checked for
/// every live branch before any storage effect.
pub async fn upgrade_storage_as(
    uri: &str,
    options: UpgradeOptions,
    actor: Option<&str>,
    policy: Option<&dyn omnigraph_policy::PolicyChecker>,
) -> Result<UpgradeReport> {
    upgrade_within(uri, options, actor, policy, Bounds::SERVED).await
}

async fn upgrade_within(
    uri: &str,
    options: UpgradeOptions,
    actor: Option<&str>,
    policy: Option<&dyn omnigraph_policy::PolicyChecker>,
    bounds: Bounds,
) -> Result<UpgradeReport> {
    let root = normalize_root_uri(uri)?;
    let mut report = UpgradeReport::started(root.clone(), options);
    if let Err(error) = run(&root, options, actor, policy, bounds, &mut report).await {
        report.failed(&error);
    }
    Ok(report)
}

fn invalid(message: impl Into<String>) -> OmniError {
    OmniError::manifest(message.into())
}

async fn open(root: &str, native: Option<&str>) -> Result<Dataset> {
    open_manifest_dataset_native_with_session(root, native, &crate::lance_access::control_session())
        .await
}

fn named(native: Option<&str>) -> String {
    native.map_or_else(|| "main".to_string(), |native| format!("ref '{native}'"))
}

decide_seam! {
    /// After main carries the fence and before any legacy object is written.
    pub static UPGRADE_AFTER_FENCE = ("upgrade.after_fence", Unreachable, [Fail]);
}

decide_seam! {
    /// After the legacy directory exists and before any ref is converted.
    pub static UPGRADE_AFTER_LEGACY = ("upgrade.after_legacy", Unreachable, [Fail]);
}

decide_seam! {
    /// After a ref's conversion rows are staged and before their commit.
    pub static UPGRADE_AFTER_STAGE = ("upgrade.after_stage", Unreachable, [Fail]);
}

decide_seam! {
    /// After a ref's conversion commit carries its receipt, main's included.
    pub static UPGRADE_AFTER_BRANCH = ("upgrade.after_branch", Unreachable, [Fail]);
}

decide_seam! {
    /// After every ref is converted and validated, before main drops the key.
    pub static UPGRADE_BEFORE_ACTIVATION = ("upgrade.before_activation", Unreachable, [Fail]);
}

decide_seam! {
    /// After main dropped the key: the root is served and the run only reports.
    pub static UPGRADE_AFTER_ACTIVATION = ("upgrade.after_activation", Unreachable, [Fail]);
}

async fn run(
    root: &str,
    options: UpgradeOptions,
    actor: Option<&str>,
    policy: Option<&dyn omnigraph_policy::PolicyChecker>,
    bounds: Bounds,
    report: &mut UpgradeReport,
) -> Result<()> {
    let session = crate::lance_access::control_session();
    let main = open(root, None).await?;
    report.observed_format = read_stamp(&main);
    let pending = match intent_from(&main) {
        Ok(pending) => pending,
        Err(_) => {
            report.recover(
                "unknown_upgrade_ownership",
                recovery_guidance(&main),
                Remedy::PreserveAndCheck,
            );
            return Ok(());
        }
    };
    if pending.is_some() && options.check {
        report.recover("pending_upgrade", recovery_guidance(&main), Remedy::Rerun);
        return Ok(());
    }
    let storage = crate::storage::decorate(storage_for_uri(root)?);
    let sidecars = pending_legacy_sidecars(root, storage.as_ref()).await?;
    if !sidecars.is_empty() {
        report.outcome = UpgradeOutcome::RecoveryRequired;
        report.finding(
            "source_recovery_required",
            format!(
                "{} recovery sidecar(s) under `__recovery/` ({}) must be resolved before the \
                 storage conversion",
                sidecars.len(),
                sidecars.join(", ")
            ),
        );
        report.recovery = Some(UpgradeRecovery {
            failed_handler: HANDLER.into(),
            executable_compatibility: "the omnigraph build that wrote the recovery sidecars".into(),
            action: "stop all writers, retain the backup, open the graph read-write with the \
                     build that wrote the sidecars so it finishes its recovery, then rerun \
                     `omnigraph upgrade <graph> --check`"
                .into(),
        });
        return Ok(());
    }
    if report.target_format != INTERNAL_MANIFEST_SCHEMA_VERSION {
        report.finding(
            "unsupported_target",
            format!(
                "this binary converts storage format v{UPGRADE_SOURCE_FORMAT} to \
                 v{INTERNAL_MANIFEST_SCHEMA_VERSION} and serves \
                 v{INTERNAL_MANIFEST_SCHEMA_VERSION} alone; `--to-format` accepts \
                 {INTERNAL_MANIFEST_SCHEMA_VERSION} only"
            ),
        );
        return Ok(());
    }
    if pending.is_none() {
        match (guard_stamp(&main), report.observed_format) {
            (Ok(_), _) => {
                report.outcome = UpgradeOutcome::AlreadyCurrent;
                return Ok(());
            }
            (Err(error), Some(stamp)) if stamp > INTERNAL_MANIFEST_SCHEMA_VERSION => {
                report.finding("newer_than_binary", error.to_string());
                return Ok(());
            }
            (Err(_), Some(UPGRADE_SOURCE_FORMAT)) => {}
            (Err(error), _) => {
                report.finding("unsupported_source", error.to_string());
                return Ok(());
            }
        }
    }
    report.route.push(HANDLER.into());
    let fenced = pending.is_some();
    if fenced {
        report.last_durable_completed_boundary = Some("source_fenced".into());
    }

    let source = Stamp13Source;
    let pinned_main = match &pending {
        Some(intent) => {
            let version = intent.branches.last().map_or(0, |main| main.version);
            main.checkout_version(version)
                .await
                .map_err(OmniError::storage)?
        }
        None => main.clone(),
    };
    let contract = match source.admits(&pinned_main, SourceRole::LiveHead) {
        Ok(_) => source.version_schema(&pinned_main, None).await?.contract,
        Err(error) if fenced => return Err(error),
        Err(error) => {
            report.finding("unsupported_source", error.to_string());
            return Ok(());
        }
    };
    let Some(contract) = contract else {
        let missing = format!(
            "version {} of main holds no schema contract row",
            pinned_main.version().version
        );
        if fenced {
            return Err(invalid(missing));
        }
        report.finding("unsupported_source", missing);
        return Ok(());
    };
    report.graph_identity = Some(contract.head.schema_identity_domain.clone());
    let (branches, attempt) = match &pending {
        Some(intent) => {
            if intent.graph_identity != contract.head.schema_identity_domain {
                return Err(invalid("upgrade schema identity changed"));
            }
            intent
                .schema_contract
                .as_ref()
                .ok_or_else(|| invalid("storage upgrade intent names no schema contract"))?
                .validate_row(&contract)?;
            (intent.branches.clone(), intent.attempt.clone())
        }
        None => (inventory(&main).await?, ulid::Ulid::new().to_string()),
    };
    authorize(&branches, actor, policy)?;
    verify_inventory(root, &branches, pending.as_ref()).await?;

    let directory = match &pending {
        Some(intent) => {
            let directory = history::legacy_directory(root, &session, None).await?;
            if let Some(directory) = &directory
                && let Err(error) = require_bound_directory(directory, intent)
            {
                report.recover(
                    "legacy_objects_differ",
                    error.to_string(),
                    Remedy::FencingExecutable,
                );
                return Ok(());
            }
            directory
        }
        None => None,
    };
    let census = match (&pending, directory) {
        (Some(intent), Some(directory)) => {
            report.work = UpgradeWork::of_directory(&directory, intent);
            None
        }
        _ => {
            if !fenced {
                let foreign =
                    history::foreign_history_objects(root, &session, NAMED_OBJECTS).await?;
                if !foreign.is_empty() {
                    report.finding(
                        "history_objects_present",
                        format!(
                            "`__history/` already holds objects no v{UPGRADE_SOURCE_FORMAT} \
                             build writes ({}); restore the whole root, `__history/` included, \
                             from the backup taken before the earlier attempt",
                            foreign.join(", ")
                        ),
                    );
                    return Ok(());
                }
                let findings = preflight_refs(root, &branches).await?;
                if !findings.is_empty() {
                    report.findings.extend(findings);
                    return Ok(());
                }
            }
            let input = match bounded_input(&main, &branches, &attempt, bounds, fenced).await? {
                Ok(input) => input,
                Err(over) if fenced => {
                    report.recover(
                        "unsupported_source",
                        over.join("; "),
                        Remedy::FencingExecutable,
                    );
                    return Ok(());
                }
                Err(over) => {
                    for message in over {
                        report.finding("unsupported_source", message);
                    }
                    return Ok(());
                }
            };
            let layout = pending
                .as_ref()
                .map_or(LegacyLayout::CURRENT, |intent| intent.legacy.layout);
            let census = match legacy::census_within(
                root,
                &input,
                &source,
                bounds.census_cells,
                bounds.census_snapshot_bytes,
            )
            .await
            {
                Ok(census) => census,
                Err(error) => return report.census_finding(error, fenced),
            };
            let plan = match legacy::plan(&census, &layout) {
                Ok(plan) => plan,
                Err(error) => return report.census_finding(error, fenced),
            };
            report.work = UpgradeWork::of_census(&census, &plan);
            Some((census, plan))
        }
    };

    let intent = match pending {
        Some(intent) => {
            if let Some((_, plan)) = &census
                && *plan != intent.legacy
            {
                report.recover(
                    "legacy_plan_changed",
                    format!(
                        "the census or planning build differs from the one that fenced: this \
                         run plans directory {} over {} commits, the intent binds directory {} \
                         over {} commits; finish the upgrade with the executable that fenced \
                         the graph",
                        plan.directory_sha256,
                        plan.commits,
                        intent.legacy.directory_sha256,
                        intent.legacy.commits
                    ),
                    Remedy::FencingExecutable,
                );
                return Ok(());
            }
            intent
        }
        None => {
            let (_, plan) = census
                .as_ref()
                .ok_or_else(|| invalid("an unfenced upgrade ran no census"))?;
            let intent = UpgradeIntent {
                protocol: UPGRADE_PROTOCOL,
                attempt,
                source_format: UPGRADE_SOURCE_FORMAT,
                target_format: INTERNAL_MANIFEST_SCHEMA_VERSION,
                graph_identity: contract.head.schema_identity_domain.clone(),
                branches,
                schema_contract: Some(UpgradeSchemaContract::from_row(&contract)),
                legacy: plan.clone(),
            };
            let json = intent_json(&intent)?;
            if options.check {
                report.outcome = UpgradeOutcome::CheckPassed;
                return Ok(());
            }
            verify_inventory(root, &intent.branches, None).await?;
            history::archive_schema(root, &session, &contract).await?;
            let pinned = intent.branches.last();
            if !fence_main(root, pinned, json, intent.target_format, report).await? {
                return Ok(());
            }
            intent
        }
    };
    fail(&UPGRADE_AFTER_FENCE)?;

    if let Some((census, _)) = &census {
        match legacy::materialize(root, &session, census, &intent.legacy).await {
            Ok(()) => {}
            Err(error) => return report.census_finding(error, true),
        }
    }
    if let Some(differs) = validate_legacy(root, &session, &intent).await? {
        report.recover("legacy_objects_differ", differs, Remedy::FencingExecutable);
        return Ok(());
    }
    report.last_durable_completed_boundary = Some("legacy_written".into());
    fail(&UPGRADE_AFTER_LEGACY)?;

    for branch in &intent.branches {
        let native = branch.native.as_deref();
        let current = open(root, native).await?;
        if crate::branch_control::dataset_branch_identifier(&current)
            .await
            .map_err(OmniError::storage)?
            != branch.identity
        {
            return Err(invalid(format!(
                "the native lifetime of {} changed before its conversion",
                named(native)
            )));
        }
        if branch_completed(&current, branch, &intent)? {
            continue;
        }
        verify_source_head(&current, branch, native.is_none())?;
        let head = converted_head(root, &session, branch, census.as_ref()).await?;
        publish_conversion(root, &session, current, branch, &intent, &head).await?;
        report.last_durable_completed_boundary =
            Some(format!("converted:{}", native.unwrap_or("main")));
        fail(&UPGRADE_AFTER_BRANCH)?;
    }

    if let Some(differs) = validate_legacy(root, &session, &intent).await? {
        report.recover("legacy_objects_differ", differs, Remedy::FencingExecutable);
        return Ok(());
    }
    for branch in &intent.branches {
        let native = branch.native.as_deref();
        let current = open(root, native).await?;
        if !branch_completed(&current, branch, &intent)? {
            return Err(invalid(format!(
                "{} is not converted; the graph cannot be activated",
                named(native)
            )));
        }
        let source = current
            .checkout_version(branch.version)
            .await
            .map_err(OmniError::storage)?;
        let head = converted_head(root, &session, branch, None).await?;
        equivalent_13_14(&source, &current, native, &head).await?;
        validate_converted_contract(&current, native, &head, &intent).await?;
    }
    verify_inventory(root, &intent.branches, Some(&intent)).await?;
    fail(&UPGRADE_BEFORE_ACTIVATION)?;
    publish_activation(open(root, None).await?).await?;
    report.last_durable_completed_boundary = Some("activated".into());
    fail(&UPGRADE_AFTER_ACTIVATION)?;
    report.outcome = UpgradeOutcome::Completed;
    report.completed_handlers.push(HANDLER.into());
    Ok(())
}

fn require_bound_directory(directory: &LegacyDirectory, intent: &UpgradeIntent) -> Result<()> {
    let found = format!("{:x}", Sha256::digest(directory.encode()?));
    if found != intent.legacy.directory_sha256 {
        return Err(invalid(format!(
            "the legacy directory under `__history/legacy/locator/` hashes to {found}, the \
             upgrade intent binds {}; restore the whole root from the backup taken before the \
             attempt",
            intent.legacy.directory_sha256
        )));
    }
    Ok(())
}

/// Read every legacy object again and require the set the intent binds.
/// `Ok(Some(_))` says how the objects differ; `Err` is a read that failed.
async fn validate_legacy(
    root: &str,
    session: &Arc<Session>,
    intent: &UpgradeIntent,
) -> Result<Option<String>> {
    match bound_legacy(root, session, intent).await {
        Ok(()) => Ok(None),
        Err(differs @ OmniError::Manifest(_)) => Ok(Some(differs.to_string())),
        Err(read) => Err(read),
    }
}

async fn bound_legacy(root: &str, session: &Arc<Session>, intent: &UpgradeIntent) -> Result<()> {
    let directory = history::verify_legacy(root, session)
        .await?
        .ok_or_else(|| invalid("the root holds no legacy directory after the legacy write"))?;
    require_bound_directory(&directory, intent)?;
    let counts = (
        directory.commits,
        directory.files.len(),
        directory.id_shards.len(),
        directory.writer_shards.len(),
    );
    let bound = (
        intent.legacy.commits,
        intent.legacy.data_files as usize,
        intent.legacy.id_shards as usize,
        intent.legacy.writer_shards as usize,
    );
    if counts != bound {
        return Err(invalid(format!(
            "the legacy directory lists (commits, data files, id shards, writer shards) \
             {counts:?}, the upgrade intent binds {bound:?}"
        )));
    }
    Ok(())
}

fn intent_json(intent: &UpgradeIntent) -> Result<String> {
    let json = serde_json::to_string(intent).map_err(|error| invalid(error.to_string()))?;
    if json.len() > MAX_INTENT_BYTES {
        return Err(invalid(format!(
            "the storage upgrade intent is {} bytes, above the metadata budget of \
             {MAX_INTENT_BYTES}",
            json.len()
        )));
    }
    let read: UpgradeIntent = serde_json::from_str(&json).map_err(|error| {
        invalid(format!(
            "the storage upgrade intent does not read back: {error}"
        ))
    })?;
    read.validate()?;
    if read != *intent {
        return Err(invalid(
            "the storage upgrade intent reads back as another intent",
        ));
    }
    Ok(json)
}

fn authorize(
    branches: &[SourceBranch],
    actor: Option<&str>,
    policy: Option<&dyn omnigraph_policy::PolicyChecker>,
) -> Result<()> {
    let Some(checker) = policy else {
        return Ok(());
    };
    let actor = actor.ok_or_else(|| {
        OmniError::Policy("storage upgrade requires an actor when policy is installed".into())
    })?;
    for branch in branches {
        let name = branch
            .native
            .as_deref()
            .map_or("main", crate::branch_names::logical_branch_name);
        checker
            .check(
                omnigraph_policy::PolicyAction::SchemaApply,
                &omnigraph_policy::ResourceScope::TargetBranch(name.into()),
                actor,
            )
            .map_err(|error| OmniError::Policy(error.to_string()))?;
    }
    Ok(())
}

/// The live refs of the source pinned at their heads, by native name, main last.
async fn inventory(main: &Dataset) -> Result<Vec<SourceBranch>> {
    let live = crate::branch_control::list_live_manifest_branch_contents(main).await?;
    if live.len() >= MAX_BRANCHES {
        return Err(invalid(format!(
            "the graph holds {} live branches beside main; one storage upgrade converts at most \
             {MAX_BRANCHES} live refs",
            live.len()
        )));
    }
    let mut live: Vec<_> = live.into_iter().collect();
    live.sort_by(|a, b| a.0.cmp(&b.0));
    let mut branches = Vec::with_capacity(live.len() + 1);
    for (native, contents) in live {
        let head = main
            .checkout_branch(&native)
            .await
            .map_err(OmniError::storage)?;
        branches.push(SourceBranch {
            identity: crate::branch_control::dataset_branch_identifier(&head)
                .await
                .map_err(OmniError::storage)?,
            version: head.version().version,
            parent_version: contents.parent_version,
            native: Some(native),
        });
    }
    branches.push(SourceBranch {
        native: None,
        identity: crate::branch_control::dataset_branch_identifier(main)
            .await
            .map_err(OmniError::storage)?,
        version: main.version().version,
        parent_version: 0,
    });
    Ok(branches)
}

/// The refs the census reads: the pinned live refs with the ref each forked
/// from, and every retired ref at its head.
async fn census_input(
    main: &Dataset,
    branches: &[SourceBranch],
    attempt: &str,
    retired_refs: HashMap<String, BranchContents>,
) -> Result<CensusInput> {
    let contents = crate::branch_control::list_live_manifest_branch_contents(main).await?;
    let mut live = Vec::with_capacity(branches.len());
    for branch in branches {
        let parent = match &branch.native {
            None => None,
            Some(native) => {
                let contents = contents
                    .get(native)
                    .filter(|contents| contents.parent_version == branch.parent_version)
                    .ok_or_else(|| {
                        invalid(format!(
                            "ref '{native}' no longer forks at version {} as the upgrade \
                             pinned it",
                            branch.parent_version
                        ))
                    })?;
                contents.parent_branch.clone()
            }
        };
        live.push(CensusRef {
            native: branch.native.clone(),
            version: branch.version,
            parent,
            parent_version: branch.parent_version,
        });
    }
    let mut retired = Vec::new();
    for (native, contents) in retired_refs {
        let head = main
            .checkout_version(Ref::Version(Some(native.clone()), None))
            .await
            .map_err(OmniError::storage)?;
        retired.push(CensusRef {
            native: Some(native),
            version: head.version().version,
            parent: contents.parent_branch,
            parent_version: contents.parent_version,
        });
    }
    Ok(CensusInput {
        attempt: attempt
            .parse()
            .map_err(|_| invalid("the storage upgrade attempt is not a ULID"))?,
        live,
        retired,
    })
}

fn verify_source_head(dataset: &Dataset, source: &SourceBranch, fenced: bool) -> Result<()> {
    let native = source.native.as_deref();
    let expected = source
        .version
        .checked_add(u64::from(fenced))
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    let found = dataset.version().version;
    if found != expected {
        return Err(invalid(format!(
            "{} is at version {found}, the upgrade pinned it at version {expected}: a writer \
             moved it during the offline upgrade; stop every omnigraph process and restore the \
             root from the backup taken before the attempt",
            named(native)
        )));
    }
    let stamp = match fenced {
        true => INTERNAL_MANIFEST_SCHEMA_VERSION,
        false => UPGRADE_SOURCE_FORMAT,
    };
    if read_stamp(dataset) != Some(stamp) {
        return Err(invalid(format!(
            "{} is stamped {:?} at version {found}, the upgrade expects v{stamp} there",
            named(native),
            read_stamp(dataset)
        )));
    }
    Ok(())
}

/// The live refs are the pinned ones, each at its pinned or converted head,
/// and a fenced main still carries `pending`.
async fn verify_inventory(
    root: &str,
    branches: &[SourceBranch],
    pending: Option<&UpgradeIntent>,
) -> Result<()> {
    let main = open(root, None).await?;
    let observed: BTreeSet<String> =
        crate::branch_control::list_live_manifest_branch_contents(&main)
            .await?
            .into_keys()
            .collect();
    let expected: BTreeSet<String> = branches
        .iter()
        .filter_map(|branch| branch.native.clone())
        .collect();
    if observed != expected {
        return Err(invalid(format!(
            "the live branches changed during the offline upgrade: pinned {expected:?}, found \
             {observed:?}"
        )));
    }
    if pending.is_some() && intent_from(&main)?.as_ref() != pending {
        return Err(invalid("main no longer carries this upgrade's intent"));
    }
    for source in branches {
        let native = source.native.as_deref();
        let dataset = open(root, native).await?;
        if crate::branch_control::dataset_branch_identifier(&dataset)
            .await
            .map_err(OmniError::storage)?
            != source.identity
        {
            return Err(invalid(format!(
                "the native lifetime of {} changed during the offline upgrade",
                named(native)
            )));
        }
        let converted = match pending {
            Some(intent) => branch_completed(&dataset, source, intent)?,
            None => false,
        };
        if !converted {
            verify_source_head(&dataset, source, pending.is_some() && native.is_none())?;
        }
    }
    Ok(())
}

/// What the engine checks of every live head before the census reads it: one
/// logical name per ref and the `object_id` key evidence.
async fn preflight_refs(root: &str, branches: &[SourceBranch]) -> Result<Vec<UpgradeFinding>> {
    let unsupported = |message: String| UpgradeFinding {
        code: "unsupported_source".into(),
        message,
    };
    let mut findings = Vec::new();
    let mut logical_names = HashSet::from(["main".to_string()]);
    for branch in branches {
        let native = branch.native.as_deref();
        if let Some(native) = native {
            let logical = crate::branch_names::logical_branch_name(native);
            crate::branch_names::ensure_logical_branch_name(logical)?;
            if !logical_names.insert(logical.to_string()) {
                findings.push(unsupported(format!(
                    "two live refs carry the logical branch name '{logical}'"
                )));
            }
        }
        let head = open(root, native)
            .await?
            .checkout_version(branch.version)
            .await
            .map_err(OmniError::storage)?;
        let key: Vec<&str> = head
            .schema()
            .unenforced_primary_key()
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        if key != ["object_id"] || !head.manifest().uses_stable_row_ids() {
            findings.push(unsupported(format!(
                "{} does not carry `object_id` as its unenforced primary key with stable row ids",
                named(native)
            )));
        }
    }
    Ok(findings)
}

/// The census input within `bounds`, or what exceeds them: the retired ref
/// count before any retired head is opened, then every live and retired head.
/// An unfenced refusal of a retired ref carries [`RETIRED_REMEDY`].
async fn bounded_input(
    main: &Dataset,
    branches: &[SourceBranch],
    attempt: &str,
    bounds: Bounds,
    fenced: bool,
) -> Result<std::result::Result<CensusInput, Vec<String>>> {
    let remedy = match fenced {
        true => "",
        false => RETIRED_REMEDY,
    };
    let retired_refs = retired_manifest_branches(main).await?;
    if retired_refs.len() > bounds.retired_refs {
        return Ok(Err(vec![format!(
            "the graph holds {} retired refs, one storage upgrade reads at most {}{remedy}",
            retired_refs.len(),
            bounds.retired_refs
        )]));
    }
    let input = census_input(main, branches, attempt, retired_refs).await?;
    let live = input.live.iter().map(|source| (source, "", ""));
    let retired = input
        .retired
        .iter()
        .map(|source| (source, "retired ", remedy));
    let mut over = Vec::new();
    for (source, role, remedy) in live.chain(retired) {
        let head = main
            .checkout_version(Ref::Version(source.native.clone(), Some(source.version)))
            .await
            .map_err(OmniError::storage)?;
        if let Some(held) = metadata_over_budget(&head, bounds).await? {
            over.push(format!(
                "{role}{} holds {held}, above the budget of {} rows or {} bytes per head{remedy}",
                named(source.native.as_deref()),
                bounds.head_rows,
                bounds.head_bytes
            ));
        }
    }
    Ok(match over.is_empty() {
        true => Ok(input),
        false => Err(over),
    })
}

/// What of `dataset` exceeds the per-head budget, `None` when it fits.
async fn metadata_over_budget(dataset: &Dataset, bounds: Bounds) -> Result<Option<String>> {
    let rows = dataset.count_rows(None).await.map_err(OmniError::storage)?;
    if rows > bounds.head_rows {
        return Ok(Some(format!("{rows} rows")));
    }
    let mut scan = dataset.scan();
    scan.batch_size(1024);
    let mut stream = scan.try_into_stream().await.map_err(OmniError::storage)?;
    let mut bytes = 0usize;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        bytes = bytes.saturating_add(batch.get_array_memory_size());
        if bytes > bounds.head_bytes {
            return Ok(Some(format!(
                "more than {} decoded bytes",
                bounds.head_bytes
            )));
        }
    }
    Ok(None)
}

fn canonical(record: &HistoryRecord) -> HistoryRecord {
    let mut record = record.clone();
    record
        .tables
        .sort_by_key(|table| table.registration.identity);
    record
}

/// The record `branch` is converted to, read through the legacy locator. A
/// census of this run must have derived the same record for the ref.
async fn converted_head(
    root: &str,
    session: &Arc<Session>,
    branch: &SourceBranch,
    census: Option<&(LegacyCensus, LegacyPlan)>,
) -> Result<HistoryRecord> {
    let native = branch.native.as_deref();
    let located = legacy::converted_head(root, session, native, branch.version).await?;
    if let Some((census, _)) = census {
        let derived = census
            .heads
            .iter()
            .find(|head| head.native.as_deref() == native)
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "the census derived no head for {}",
                    named(native)
                ))
            })?;
        if canonical(&derived.record) != canonical(&located) {
            return Err(OmniError::manifest_internal(format!(
                "the legacy locator serves graph commit '{}' as the head of {}, the census \
                 derived '{}'",
                located.commit.graph_commit_id,
                named(native),
                derived.record.commit.graph_commit_id
            )));
        }
    }
    Ok(located)
}

/// The pin a source head holds for `identity`: its greatest clock, unless a
/// tombstone at or above that clock dropped the table.
fn source_pin(scan: &HeadScan, identity: TableIdentity) -> Option<&TablePin> {
    let (&(_, clock), pin) = scan
        .pins
        .range((identity, 0)..=(identity, u64::MAX))
        .next_back()?;
    let dropped = scan
        .tombstones
        .range((identity, clock)..=(identity, u64::MAX))
        .next()
        .is_some();
    (!dropped).then_some(pin)
}

/// The converted version of a ref holds the visible state of its source
/// version: the same pinned tables, its own head commit alone under its
/// logical name, and none when the ref wrote no commit of its own.
async fn equivalent_13_14(
    source: &Dataset,
    target: &Dataset,
    native: Option<&str>,
    head: &HistoryRecord,
) -> Result<()> {
    let differs = |what: String| {
        invalid(format!(
            "the converted version {} of {} does not hold the state of its source version {}: \
             {what}",
            target.version().version,
            named(native),
            source.version().version
        ))
    };
    let scan = Stamp13Source.scan_head(source).await?;
    let (mut state, _) = read_converted_state(target).await?;
    state.entries.sort_by_key(|entry| entry.identity);
    let mut pinned: Vec<_> = head
        .tables
        .iter()
        .filter_map(|table| match &table.state {
            TableState::Pinned(pin) => Some((&table.registration, pin)),
            TableState::Registered | TableState::Dropped { .. } => None,
        })
        .collect();
    pinned.sort_by_key(|(registration, _)| registration.identity);
    let live_in_source = scan
        .pins
        .keys()
        .map(|(identity, _)| *identity)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|identity| source_pin(&scan, *identity).is_some())
        .count();
    if state.entries.len() != pinned.len() || live_in_source != pinned.len() {
        return Err(differs(format!(
            "the source pins {live_in_source} tables, its head record {} and the converted \
             version {}",
            pinned.len(),
            state.entries.len()
        )));
    }
    for ((registration, pin), entry) in pinned.into_iter().zip(&state.entries) {
        let same = entry.identity == registration.identity
            && entry.type_key == registration.table_key
            && entry.dataset_path == registration.table_path
            && entry.published_dataset_version == pin.table_version
            && entry.native_dataset_branch == pin.table_branch
            && entry.entity_count == pin.row_count
            && entry.version_metadata == pin.metadata
            && entry.manifest_version == pin.manifest_version
            && source_pin(&scan, registration.identity) == Some(pin);
        if !same {
            return Err(differs(format!(
                "table '{}' is pinned differently",
                registration.table_key
            )));
        }
    }
    let key = native.map_or("main", crate::branch_names::logical_branch_name);
    let id = &head.commit.graph_commit_id;
    let own = head.commit.native_branch.as_deref() == native;
    let expected = match own {
        true => HashMap::from([(key.to_string(), id.clone())]),
        false => HashMap::new(),
    };
    if state.graph_heads != expected {
        return Err(differs(format!(
            "it names the heads {:?}, the ref's head record gives {expected:?}",
            state.graph_heads
        )));
    }
    if own && scan.heads.get(key) != Some(id) {
        return Err(differs(format!(
            "the source names {:?} as the head of '{key}', the head record '{id}'",
            scan.heads.get(key)
        )));
    }
    Ok(())
}

/// The converted version carries the schema content its head commit names,
/// and main's is the contract the intent binds.
async fn validate_converted_contract(
    target: &Dataset,
    native: Option<&str>,
    head: &HistoryRecord,
    intent: &UpgradeIntent,
) -> Result<()> {
    let (_, contract) = read_converted_state(target).await?;
    let named_by_head = head.commit.schema_contract.as_ref() == Some(&contract.head)
        && head.commit.schema_content_hash.as_deref()
            == Some(history::schema_content_hash(&contract)?.as_str());
    if !named_by_head {
        return Err(invalid(format!(
            "the converted version of {} carries a schema contract other than the one its head \
             commit '{}' names",
            named(native),
            head.commit.graph_commit_id
        )));
    }
    if native.is_none() {
        intent
            .schema_contract
            .as_ref()
            .ok_or_else(|| invalid("storage upgrade intent names no schema contract"))?
            .validate_row(&contract)?;
    }
    Ok(())
}

/// Commit the fence on a main still at `pinned`; an error wrote nothing. `false`
/// when the commit failed: whether main carries the intent is then unknown, and
/// the report holds that recovery.
async fn fence_main(
    root: &str,
    pinned: Option<&SourceBranch>,
    intent: String,
    target: u32,
    report: &mut UpgradeReport,
) -> Result<bool> {
    let unfenced = open(root, None).await?;
    if let Some(pinned) = pinned {
        verify_source_head(&unfenced, pinned, false)?;
    }
    match publish_fence(unfenced, intent, target).await {
        Ok(_) => {
            report.last_durable_completed_boundary = Some("source_fenced".into());
            Ok(true)
        }
        Err(error) => {
            report.recover(
                "fence_publication_attempted",
                format!(
                    "the fence commit on main did not report its outcome ({error}); run \
                     `omnigraph upgrade <graph> --check` to see whether main carries the intent"
                ),
                Remedy::PreserveAndCheck,
            );
            Ok(false)
        }
    }
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

/// Overwrite the head of a live ref with its stamp-14 rows under its receipt
/// (and, on main, the pending key) in one commit that is never retried.
async fn publish_conversion(
    root: &str,
    session: &Arc<Session>,
    current: Dataset,
    branch: &SourceBranch,
    intent: &UpgradeIntent,
    head: &HistoryRecord,
) -> Result<()> {
    let native = branch.native.as_deref();
    let source = current
        .checkout_version(branch.version)
        .await
        .map_err(OmniError::storage)?;
    let mut metadata = source.schema().metadata.clone();
    metadata.insert(
        INTERNAL_SCHEMA_VERSION_KEY.into(),
        intent.target_format.to_string(),
    );
    metadata.remove(UPGRADE_PENDING_KEY);
    if native.is_none() {
        metadata.insert(
            UPGRADE_PENDING_KEY.into(),
            serde_json::to_string(intent).map_err(|error| invalid(error.to_string()))?,
        );
    }
    metadata.insert(
        UPGRADE_RECEIPT_KEY.into(),
        serde_json::to_string(&receipt(branch, intent))
            .map_err(|error| invalid(error.to_string()))?,
    );
    let (schema, batch) = legacy::conversion_batch(root, session, head, metadata).await?;
    let stream: datafusion::physical_plan::SendableRecordBatchStream = Box::pin(
        RecordBatchStreamAdapter::new(schema, futures::stream::iter([Ok(batch)])),
    );
    let params = WriteParams {
        mode: WriteMode::Overwrite,
        auto_cleanup: None,
        skip_auto_cleanup: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let destination = Arc::new(current);
    let transaction = InsertBuilder::new(Arc::clone(&destination))
        .with_params(&params)
        .execute_uncommitted_stream(stream)
        .await
        .map_err(OmniError::storage)?;
    fail(&UPGRADE_AFTER_STAGE)?;
    let target = CommitBuilder::new(destination)
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(transaction)
        .await
        .map_err(OmniError::storage)?;
    if !branch_completed(&target, branch, intent)? {
        return Err(invalid(format!(
            "the conversion commit of {} does not carry its receipt",
            named(native)
        )));
    }
    equivalent_13_14(&source, &target, native, head).await?;
    validate_converted_contract(&target, native, head, intent).await
}

#[cfg(test)]
mod tests;
