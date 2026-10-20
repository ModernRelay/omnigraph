//! Direct cluster control. The cluster crate retains deployment and recovery authority.

mod args;
pub(crate) use args::{ClusterCommand, ClusterMembersCommand, ClusterRunArgs};

use crate::app::Completion;
use crate::helpers::resolve_actor;
use crate::output::*;
use crate::targets::{ResolvedTarget, TargetBinding};
use color_eyre::eyre::{Result, bail};
use omnigraph_cluster::validate_config_dir;
use std::io::Write;

#[derive(Clone, Copy)]
pub(crate) struct ClusterScope<'a> {
    pub(crate) operator: &'a crate::operator::InvocationConfig,
    pub(crate) target: Option<&'a ResolvedTarget>,
    pub(crate) actor: Option<&'a str>,
}

pub(crate) async fn run(
    command: ClusterCommand,
    scope: ClusterScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    if let ClusterCommand::Validate { config, json } = command {
        let output = validate_config_dir(config);
        return finish_cluster_validate(&output, json, stdout);
    }
    let target = scope
        .target
        .ok_or_else(|| color_eyre::eyre::eyre!("cluster operation requires a target"))?;
    if matches!(target.binding, TargetBinding::Server { .. }) {
        crate::cluster_remote::run(scope.operator, &command, target, stdout, stderr).await?;
        return Ok(Completion::success());
    }
    let TargetBinding::DirectRoot { uri: root, .. } = &target.binding else {
        bail!("this cluster operation requires a direct-root or server target");
    };
    let binding = crate::scope::capture_storage_binding(target)?;
    match command {
        ClusterCommand::Plan { config, json, .. } => {
            let output = omnigraph_cluster::plan_config_dir_with_binding_as(
                config
                    .as_deref()
                    .unwrap_or_else(|| std::path::Path::new(".")),
                &binding,
                resolve_actor(scope.operator, scope.actor, stderr)?,
            )
            .await;
            return finish_cluster_plan(&output, json, stdout);
        }
        ClusterCommand::Observe { config, json } => {
            let output = omnigraph_cluster::observe_config_dir_with_binding(config, &binding).await;
            return finish_cluster_state_sync(&output, json, stdout);
        }
        ClusterCommand::Apply {
            config,
            json,
            deployment_id,
            writers_stopped,
            ..
        } => {
            if writers_stopped && (config.is_some() || deployment_id.is_none()) {
                bail!(
                    "--writers-stopped requires source-free --deployment-id; it cannot select source apply"
                );
            }
            if writers_stopped && !target.is_explicit() {
                bail!("deployment recovery effects require an explicit --target");
            }
            let caller = omnigraph_cluster::DeploymentCaller::storage_owner(resolve_actor(
                scope.operator,
                scope.actor,
                stderr,
            )?);
            if let Some(id) = deployment_id.as_deref().filter(|_| config.is_none()) {
                let result = omnigraph_cluster::reconcile_deployment_with_binding(
                    &binding,
                    id,
                    writers_stopped,
                    &caller,
                )
                .await;
                let status =
                    omnigraph_cluster::deployment_status_with_binding(&binding, None, &caller)
                        .await;
                let custody = match &status {
                    Ok(status) => deployment_status_identity(status),
                    Err(_) => format!(
                        "Cluster-root: {}",
                        omnigraph::storage::redacted_storage_uri(root)
                    ),
                };
                let lock_report = match &status {
                    Ok(status) => report_deployment_lock(status, stderr),
                    Err(_) => Ok(()),
                };
                let outcome_report = finish_core_deployment(result, json, stdout);
                retain_delivery_identity(
                    finish_deployment_reports(outcome_report, lock_report),
                    format!(
                        "Deployment-ID: {id}\n{custody}; inspect the original identity, never replay unknown work"
                    ),
                )?;
            } else {
                apply_source(
                    config
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new(".")),
                    &binding,
                    deployment_id.as_deref(),
                    &caller,
                    json,
                    stdout,
                    stderr,
                )
                .await?;
            }
        }
        ClusterCommand::UpgradeLedger {
            writers_stopped,
            json,
        } => {
            if !target.is_explicit() {
                bail!("upgrade-ledger requires an explicit --target");
            }
            let caller = omnigraph_cluster::DeploymentCaller::storage_owner(resolve_actor(
                scope.operator,
                scope.actor,
                stderr,
            )?);
            let status = retain_delivery_identity(
                core_deployment_result(
                    omnigraph_cluster::upgrade_deployment_ledger_with_binding(
                        &binding,
                        writers_stopped,
                        &caller,
                    )
                    .await,
                    json,
                    stdout,
                ),
                format!(
                    "Cluster-root: {}; inspect this root's ledger status before further recovery actions",
                    omnigraph::storage::redacted_storage_uri(root)
                ),
            )?;
            let outcome_report = (|| -> Result<()> {
                write_json(&status, stdout)?;
                stdout.flush()?;
                Ok(())
            })();
            let lock_report = report_deployment_lock(&status, stderr);
            retain_delivery_identity(
                finish_deployment_reports(outcome_report, lock_report),
                format!(
                    "{}; inspect this root's ledger status before further recovery actions",
                    deployment_status_identity(&status)
                ),
            )?;
        }
        ClusterCommand::Status {
            json,
            deployment_id,
            ..
        } => {
            let caller = omnigraph_cluster::DeploymentCaller::storage_owner(None);
            let status = core_deployment_result(
                omnigraph_cluster::deployment_status_with_binding(
                    &binding,
                    deployment_id.as_deref(),
                    &caller,
                )
                .await,
                json,
                stdout,
            )?;
            write_json(&status, stdout)?;
        }
        ClusterCommand::ForceUnlock { lock_id, json } => {
            if !target.is_explicit() {
                bail!("force-unlock requires an explicit --target");
            }
            let custody = format!(
                "Cluster-root: {}\nRequested lock-ID: {lock_id}",
                omnigraph::storage::redacted_storage_uri(root)
            );
            retain_delivery_identity(
                core_deployment_result(
                    omnigraph_cluster::force_unlock_storage_binding(&binding, &lock_id).await,
                    json,
                    stdout,
                ),
                custody.clone(),
            )?;
            let report = (|| -> Result<()> {
                write_json(
                    &serde_json::json!({"unlocked": true, "lock_id": lock_id}),
                    stdout,
                )?;
                stdout.flush()?;
                Ok(())
            })();
            retain_delivery_identity(
                report,
                format!("{custody}; force-unlock completed; its receipt could not be delivered"),
            )?;
        }
        ClusterCommand::Validate { .. } => unreachable!("local validation dispatches first"),
        ClusterCommand::Create { .. }
        | ClusterCommand::List { .. }
        | ClusterCommand::Members { .. }
        | ClusterCommand::Pull { .. }
        | ClusterCommand::Push { .. }
        | ClusterCommand::Delete { .. }
        | ClusterCommand::UndoDelete { .. }
        | ClusterCommand::Token { .. }
        | ClusterCommand::Operation { .. }
        | ClusterCommand::History { .. }
        | ClusterCommand::Cancel { .. } => bail!("this operation requires a managed target"),
    }
    Ok(Completion::success())
}

pub(crate) fn core_deployment_result<T>(
    result: std::result::Result<T, omnigraph_cluster::Diagnostic>,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            let message = format!("{} {}: {}", error.code, error.path, error.message);
            let delivered = (|| -> Result<()> {
                if json {
                    write_json(
                        &serde_json::json!({"ok": false, "diagnostics": [&error]}),
                        stdout,
                    )?;
                } else {
                    write_cluster_diagnostics(std::slice::from_ref(&error), stdout)?;
                }
                stdout.flush()?;
                Ok(())
            })();
            delivered.map_err(|delivery| delivery.wrap_err(message.clone()))?;
            bail!("{message}")
        }
    }
}

fn finish_core_deployment(
    result: std::result::Result<omnigraph_cluster::DeploymentLookup, omnigraph_cluster::Diagnostic>,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let result = core_deployment_result(result, json, stdout)?;
    write_json(&result, stdout)?;
    stdout.flush()?;
    if !matches!(result, omnigraph_cluster::DeploymentLookup::Complete { ref result } if result.converged)
    {
        bail!(
            "deployment has not fully converged; inspect the original identity, never replay unknown work"
        );
    }
    Ok(())
}

fn report_deployment_lock(
    status: &omnigraph_cluster::DeploymentStatus,
    stderr: &mut dyn Write,
) -> Result<()> {
    if let Some(lock) = &status.lock_id {
        writeln!(
            stderr,
            "Admission lock retained: {lock} ({}); establish prior-work quiescence before exact-ID force-unlock",
            omnigraph::storage::redacted_storage_uri(&status.canonical_root)
        )?;
        stderr.flush()?;
    }
    Ok(())
}

fn deployment_status_identity(status: &omnigraph_cluster::DeploymentStatus) -> String {
    let mut identity = format!(
        "Cluster-root: {}",
        omnigraph::storage::redacted_storage_uri(&status.canonical_root)
    );
    if let Some(lock) = &status.lock_id {
        identity.push_str(&format!(
            "\nAdmission lock: {lock}; establish prior-work quiescence before exact-ID force-unlock"
        ));
    }
    identity
}

/// The core's identity callback cannot abort execution. Remember a delivery
/// failure, let that one call settle, and report its result without re-entering
/// the deployment owner. Recovery identity survives a failed output stream.
pub(crate) async fn apply_source(
    config: &std::path::Path,
    binding: &omnigraph::storage::CapturedStorageBinding,
    requested_id: Option<&str>,
    caller: &omnigraph_cluster::DeploymentCaller,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let mut custody = None;
    let mut custody_report = Ok(());
    let result = omnigraph_cluster::apply_deployment_with_binding(
        config, binding, requested_id, caller,
        |id, root, lock| {
            let root = omnigraph::storage::redacted_storage_uri(root);
            let message = format!("Deployment-ID: {id}\nCluster-root: {root}\nAdmission lock: {lock}; retain until prior work is quiescent, then exact-ID force-unlock");
            custody_report = (|| -> Result<()> {
                writeln!(stderr, "{message}")?;
                stderr.flush()?;
                Ok(())
            })();
            custody = Some(message);
        },
    ).await;
    let outcome_report = finish_core_deployment(result, json, stdout);
    let reports = finish_deployment_reports(outcome_report, custody_report);
    match custody {
        Some(custody) => retain_delivery_identity(reports, custody),
        None => match requested_id {
            Some(id) => retain_delivery_identity(
                reports,
                format!(
                    "Deployment-ID: {id}; inspect the original identity, never replay unknown work"
                ),
            ),
            None => reports,
        },
    }
}

fn finish_deployment_reports(outcome: Result<()>, custody: Result<()>) -> Result<()> {
    match (outcome, custody) {
        (Ok(()), custody) => custody,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(custody)) => {
            Err(custody.wrap_err(format!("deployment outcome also failed: {error:#}")))
        }
    }
}

fn retain_delivery_identity<T>(result: Result<T>, identity: String) -> Result<T> {
    result.map_err(|error| {
        if error.is::<std::io::Error>() {
            error.wrap_err(identity)
        } else {
            error
        }
    })
}

#[cfg(test)]
mod tests {
    use super::core_deployment_result;
    use omnigraph_cluster::{Diagnostic, DiagnosticSeverity};
    use std::io::{self, Write};

    struct FailedDelivery {
        on_flush: bool,
    }

    impl Write for FailedDelivery {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.on_flush {
                Ok(bytes.len())
            } else {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed output"))
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed output"))
        }
    }

    #[test]
    fn undelivered_core_diagnostic_retains_its_recovery_identity() {
        let diagnostic = Diagnostic {
            code: "deployment_outcome_unknown".to_owned(),
            severity: DiagnosticSeverity::Error,
            path: "cluster".to_owned(),
            message: "admission original-lock remains held pending settlement".to_owned(),
            detail: None,
        };
        let expected = "deployment_outcome_unknown cluster: admission original-lock remains held pending settlement";
        for json in [false, true] {
            let mut delivered = Vec::new();
            let ordinary =
                core_deployment_result::<()>(Err(diagnostic.clone()), json, &mut delivered)
                    .unwrap_err();
            assert_eq!(ordinary.to_string(), expected);
            for on_flush in [false, true] {
                let error = core_deployment_result::<()>(
                    Err(diagnostic.clone()),
                    json,
                    &mut FailedDelivery { on_flush },
                )
                .unwrap_err();
                assert_eq!(
                    error.downcast_ref::<io::Error>().unwrap().kind(),
                    io::ErrorKind::BrokenPipe
                );
                assert!(format!("{error:#}").contains(expected));
            }
        }
    }
}
