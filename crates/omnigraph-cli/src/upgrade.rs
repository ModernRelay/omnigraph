use super::*;

pub(crate) async fn run(
    profile: &Option<String>,
    store: &Option<String>,
    uri: Option<String>,
    check: bool,
    to_format: Option<u32>,
    json: bool,
    quiet: bool,
) -> Result<()> {
    let target = scope::resolve_scope(
        &operator::load_operator_config()?,
        planes::Capability::Direct,
        scope::ScopeFlags {
            profile: profile.as_deref(),
            store: store.as_deref(),
            server: None,
            cluster: None,
            graph: None,
            uri,
        },
    )?;
    if target.cluster.is_some() {
        bail!(
            "upgrade refuses cluster-managed graphs; a qualified cluster upgrade operation is required"
        );
    }
    let uri = resolve_local_uri(target.uri, "upgrade")?;
    let uri = omnigraph::storage::normalize_root_uri(&uri)?;
    let uri = if omnigraph::storage::storage_kind_for_uri(&uri)?
        == omnigraph::storage::StorageKind::Local
    {
        std::fs::canonicalize(&uri)?
            .to_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("upgrade path is not valid UTF-8"))?
            .to_owned()
    } else {
        uri
    };
    if let Some(root) = omnigraph_cluster::cluster_root_for_graph_uri(&uri)
        .await
        .map_err(|diagnostic| {
            color_eyre::eyre::eyre!("{}: {}", diagnostic.path, diagnostic.message)
        })?
    {
        bail!(
            "upgrade refuses graph `{uri}` inside cluster `{root}`; a qualified cluster upgrade operation is required"
        );
    }
    if !check {
        echo_write_target(quiet, "upgrade", &uri, false);
    }
    let report =
        omnigraph::db::upgrade_storage(&uri, omnigraph::db::UpgradeOptions { check, to_format })
            .await?;
    if json {
        print_json(&report)?;
    } else {
        print_human(&report)?;
    }
    if !report.success() {
        std::process::exit(1);
    }
    Ok(())
}

fn print_human(report: &omnigraph::db::UpgradeReport) -> Result<()> {
    let mode = serde_json::to_value(report.mode)?;
    let outcome = serde_json::to_value(report.outcome)?;
    println!(
        "upgrade {}: {} ({})",
        report.location,
        outcome.as_str().unwrap_or("unknown"),
        mode.as_str().unwrap_or("unknown")
    );
    println!(
        "graph identity: {}",
        report.graph_identity.as_deref().unwrap_or("unknown")
    );
    println!(
        "format: {} -> {}{}",
        report
            .observed_format
            .map_or_else(|| "unknown".into(), |v| v.to_string()),
        report.target_format,
        if report.target_defaulted {
            " (default target)"
        } else {
            ""
        }
    );
    println!("route: {}", report.route.join(" -> "));
    println!(
        "completed handlers: {}",
        report.completed_handlers.join(", ")
    );
    println!(
        "last durable completed boundary: {}",
        report
            .last_durable_completed_boundary
            .as_deref()
            .unwrap_or("unknown")
    );
    println!(
        "work: {} metadata rows, {} retained snapshots, {} payload bytes copied, {} payload bytes rewritten",
        report.work.metadata_rows,
        report.work.retained_snapshots,
        report.work.payload_bytes_copied,
        report.work.payload_bytes_rewritten
    );
    println!(
        "validation bytes: {}",
        report
            .work
            .validation_bytes
            .map_or_else(|| "unknown".into(), |v| v.to_string())
    );
    for check in &report.work.deferred_checks {
        println!("validation deferred until the preceding conversion completes: {check}");
    }
    for exclusion in &report.work.external_blob_exclusions {
        println!("external bytes excluded from preservation: {exclusion}");
    }
    for property in &report.work.historical_blob_identity_limits {
        println!(
            "historical Blob delivery retains the pre-0.10 property-lifetime restriction: {property}"
        );
    }
    for finding in &report.findings {
        println!("{}: {}", finding.code, finding.message);
    }
    if let Some(recovery) = &report.recovery {
        println!("failed handler: {}", recovery.failed_handler);
        println!("recovery executable: {}", recovery.executable_compatibility);
        println!("recovery action: {}", recovery.action);
    }
    if matches!(report.mode, omnigraph::db::UpgradeMode::Check) {
        println!(
            "Check is advisory. Stop all writers and maintenance and retain a verified backup before execution."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_upgrade_parses_explicit_route_and_check_options() {
        let cli = Cli::try_parse_from([
            "omnigraph",
            "upgrade",
            "graph.omni",
            "--check",
            "--to-format",
            "8",
            "--json",
        ])
        .unwrap();
        assert!(
            matches!(&cli.command, Command::Upgrade { uri: Some(uri), check: true, to_format: Some(8), json: true } if uri == "graph.omni")
        );
        assert_eq!(
            planes::command_capability(&cli.command),
            planes::Capability::Direct
        );
        assert!(planes::guard_addressing(&cli).is_ok());
        let cli = Cli::try_parse_from(["omnigraph", "--store", "graph.omni", "upgrade"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Upgrade {
                uri: None,
                check: false,
                to_format: None,
                json: false
            }
        ));
    }

    #[test]
    fn storage_upgrade_rejects_served_and_cluster_addressing() {
        for flag in ["--server", "--cluster", "--graph"] {
            let cli =
                Cli::try_parse_from(["omnigraph", flag, "prod", "upgrade", "graph.omni"]).unwrap();
            assert!(planes::guard_addressing(&cli).is_err(), "{flag}");
        }
        assert!(
            Cli::try_parse_from([
                "omnigraph",
                "upgrade",
                "graph.omni",
                "--to-format",
                "not-a-version"
            ])
            .is_err()
        );
    }
}
