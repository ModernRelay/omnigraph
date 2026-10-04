//! `omnigraph upgrade`: report whether a graph's storage format is the one
//! this binary serves. This binary holds no conversion route, so the command
//! reads main's `__manifest` stamp and never writes.

use serde::Serialize;

use crate::db::manifest::layout::open_manifest_dataset_native_with_session;
use crate::db::manifest::migrations::{
    INTERNAL_MANIFEST_SCHEMA_VERSION, UPGRADE_PENDING_KEY, guard_stamp, read_stamp,
    recovery_guidance,
};
use crate::error::Result;
use crate::storage::normalize_root_uri;

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
}

/// Report a standalone graph's storage format against the one this binary serves.
pub async fn upgrade_storage(uri: &str, options: UpgradeOptions) -> Result<UpgradeReport> {
    upgrade_storage_as(uri, options, None, None).await
}

/// [`upgrade_storage`] with a caller identity. The report has no storage
/// effect, so neither the actor nor the policy is consulted.
pub async fn upgrade_storage_as(
    uri: &str,
    options: UpgradeOptions,
    _actor: Option<&str>,
    _policy: Option<&dyn omnigraph_policy::PolicyChecker>,
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
        target_format: options
            .to_format
            .unwrap_or(INTERNAL_MANIFEST_SCHEMA_VERSION),
        target_defaulted: options.to_format.is_none(),
        route: Vec::new(),
        completed_handlers: Vec::new(),
        findings: Vec::new(),
        last_durable_completed_boundary: None,
        recovery: None,
    };
    if let Err(error) = inspect(&root, &mut report).await {
        report.finding("preflight_failed", error.to_string());
    }
    Ok(report)
}

async fn inspect(root: &str, report: &mut UpgradeReport) -> Result<()> {
    let main = open_manifest_dataset_native_with_session(
        root,
        None,
        &crate::lance_access::control_session(),
    )
    .await?;
    report.observed_format = read_stamp(&main);
    if main.schema().metadata.contains_key(UPGRADE_PENDING_KEY) {
        report.outcome = UpgradeOutcome::RecoveryRequired;
        report.finding("pending_upgrade", recovery_guidance());
        report.recovery = Some(UpgradeRecovery {
            failed_handler: "storage-upgrade".into(),
            executable_compatibility: "the omnigraph executable that started the conversion"
                .into(),
            action: "stop all writers and maintenance, retain the backup, and finish the conversion with the executable that started it".into(),
        });
        return Ok(());
    }
    if report.target_format != INTERNAL_MANIFEST_SCHEMA_VERSION {
        report.finding(
            "unsupported_target",
            format!(
                "this binary has no storage conversion route; the only format it serves is v{INTERNAL_MANIFEST_SCHEMA_VERSION}"
            ),
        );
        return Ok(());
    }
    match guard_stamp(&main) {
        Ok(_) => report.outcome = UpgradeOutcome::AlreadyCurrent,
        Err(error) => report.finding("unsupported_source", error.to_string()),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
