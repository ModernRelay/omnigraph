//! Direct cluster control. The cluster crate retains deployment and recovery authority.

mod args;
pub(crate) use args::{ClusterCommand, ClusterMembersCommand, ClusterRunArgs};

use crate::app::Completion;
use crate::helpers::resolve_cluster_actor;
use crate::output::*;
use color_eyre::eyre::{Result, bail};
use omnigraph_cluster::{
    force_unlock_config_dir, observe_config_dir, status_config_dir, validate_config_dir,
};
use std::io::Write;

#[derive(Clone, Copy)]
pub(crate) struct ClusterScope<'a> {
    pub(crate) operator: &'a crate::operator::InvocationConfig,
    pub(crate) root: Option<&'a str>,
    pub(crate) actor: Option<&'a str>,
}

pub(crate) async fn run(
    command: ClusterCommand,
    scope: ClusterScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    match command {
        ClusterCommand::Validate { config, json } => {
            let output = validate_config_dir(config);
            return finish_cluster_validate(&output, json, stdout);
        }
        ClusterCommand::Plan { config, json, .. } => {
            let output = omnigraph_cluster::plan_config_dir_as(
                config,
                resolve_cluster_actor(scope.operator, scope.actor, stderr)?,
            )
            .await;
            return finish_cluster_plan(&output, json, stdout);
        }
        ClusterCommand::Observe { config, json } => {
            let output = observe_config_dir(config).await;
            return finish_cluster_state_sync(&output, json, stdout);
        }
        ClusterCommand::Apply {
            config,
            json,
            deployment_id,
            writers_stopped,
            ..
        } => {
            let caller = omnigraph_cluster::DeploymentCaller::storage_owner(resolve_cluster_actor(
                scope.operator,
                scope.actor,
                stderr,
            )?);
            if let Some(root) = scope.root {
                let id = deployment_id.as_deref().ok_or_else(|| color_eyre::eyre::eyre!("root-addressed apply requires --deployment-id; it only reconciles the original deployment"))?;
                let result =
                    omnigraph_cluster::reconcile_deployment(root, id, writers_stopped, &caller)
                        .await;
                let status = omnigraph_cluster::deployment_status(root, None, &caller).await;
                let custody = match &status {
                    Ok(status) => deployment_status_identity(status),
                    Err(_) => format!(
                        "Cluster-root: {}",
                        omnigraph::storage::redacted_storage_uri(root)
                    ),
                };
                let lock_report = match &status {
                    Ok(status) => report_deployment_lock(status, stderr),
                    Err(_) => Ok(()), // Preserve the existing best-effort status lookup.
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
                    &config,
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
            let root = scope
                .root
                .ok_or_else(|| color_eyre::eyre::eyre!("upgrade-ledger requires --cluster ROOT"))?;
            let caller = omnigraph_cluster::DeploymentCaller::storage_owner(resolve_cluster_actor(
                scope.operator,
                scope.actor,
                stderr,
            )?);
            let status = retain_delivery_identity(
                core_deployment_result(
                    omnigraph_cluster::upgrade_deployment_ledger(root, writers_stopped, &caller)
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
            config,
            json,
            deployment_id,
            ..
        } => {
            if let Some(root) = scope.root {
                let caller = omnigraph_cluster::DeploymentCaller::storage_owner(None);
                let status = core_deployment_result(
                    omnigraph_cluster::deployment_status(root, deployment_id.as_deref(), &caller)
                        .await,
                    json,
                    stdout,
                )?;
                write_json(&status, stdout)?;
            } else {
                let output = status_config_dir(config).await;
                return finish_cluster_status(&output, json, stdout);
            }
        }
        ClusterCommand::ForceUnlock {
            lock_id,
            config,
            json,
        } => {
            if let Some(root) = scope.root {
                let custody = format!(
                    "Cluster-root: {}\nRequested lock-ID: {lock_id}",
                    omnigraph::storage::redacted_storage_uri(root)
                );
                retain_delivery_identity(
                    core_deployment_result(
                        omnigraph_cluster::force_unlock_storage_root(root, &lock_id).await,
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
                    format!(
                        "{custody}; force-unlock completed; its receipt could not be delivered"
                    ),
                )?;
            } else {
                let output = force_unlock_config_dir(config, lock_id.clone()).await;
                let mut custody = format!(
                    "Cluster-state: {}\nRequested lock-ID: {lock_id}; force-unlock result: ok={}, lock_removed={}",
                    omnigraph::storage::redacted_storage_uri(&output.state_observations.state_path),
                    output.ok,
                    output.lock_removed,
                );
                if let Some(lock) = &output.state_observations.lock_id {
                    custody.push_str(&format!("\nObserved admission lock: {lock}"));
                }
                let report = (|| -> Result<Completion> {
                    let completion = finish_cluster_force_unlock(&output, json, stdout)?;
                    stdout.flush()?;
                    Ok(completion)
                })();
                return retain_delivery_identity(report, custody);
            }
        }
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
        | ClusterCommand::Cancel { .. } => {
            unreachable!("managed cluster commands dispatch first")
        }
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
    requested_id: Option<&str>,
    caller: &omnigraph_cluster::DeploymentCaller,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let mut custody = None;
    let mut custody_report = Ok(());
    let result = omnigraph_cluster::apply_deployment(
        config, requested_id, caller,
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
