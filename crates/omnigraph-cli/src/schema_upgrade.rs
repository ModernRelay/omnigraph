use super::*;

pub(crate) async fn run(
    profile: &Option<String>,
    store: &Option<String>,
    uri: Option<String>,
    check: bool,
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
            "schema upgrade-system-columns refuses cluster-managed graphs; a qualified cluster upgrade operation is required"
        );
    }
    let uri = resolve_local_uri(target.uri, "schema upgrade-system-columns")?;
    let uri = omnigraph::storage::normalize_root_uri(&uri)?;
    if let Some(root) = omnigraph_cluster::cluster_root_for_graph_uri(&uri)
        .await
        .map_err(|diagnostic| {
            color_eyre::eyre::eyre!("{}: {}", diagnostic.path, diagnostic.message)
        })?
    {
        bail!(
            "schema upgrade-system-columns refuses graph `{uri}` inside cluster `{root}`; a qualified cluster upgrade operation is required"
        );
    }
    if !check {
        echo_write_target(quiet, "schema upgrade-system-columns", &uri, false);
    }
    let db = if check {
        omnigraph::db::Omnigraph::open_read_only(&uri).await?
    } else {
        omnigraph::db::Omnigraph::open(&uri).await?
    };
    let report = db
        .upgrade_system_columns(omnigraph::db::SystemColumnUpgradeOptions { check })
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

fn print_human(report: &omnigraph::db::SystemColumnUpgradeReport) -> Result<()> {
    let mode = serde_json::to_value(report.mode)?;
    let outcome = serde_json::to_value(report.outcome)?;
    println!(
        "system-column upgrade {}: {} ({})",
        report.location,
        outcome.as_str().unwrap_or("unknown"),
        mode.as_str().unwrap_or("unknown")
    );
    println!(
        "storage format: v{} -> v{}",
        report.stamp_before, report.stamp_after
    );
    println!("tables: {}", report.tables.join(", "));
    if let Some(version) = report.graph_manifest_version {
        println!("graph manifest version: {version}");
    }
    for finding in &report.findings {
        println!("{}: {}", finding.code, finding.message);
    }
    if matches!(report.mode, omnigraph::db::UpgradeMode::Check) {
        println!(
            "Check is advisory. Stop every server serving the graph and retain a verified backup before execution."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_column_upgrade_parses_check_and_json() {
        let cli = Cli::try_parse_from([
            "omnigraph",
            "schema",
            "upgrade-system-columns",
            "graph.omni",
            "--check",
            "--json",
        ])
        .unwrap();
        assert!(matches!(
            &cli.command,
            Command::Schema {
                command: SchemaCommand::UpgradeSystemColumns { uri: Some(uri), check: true, json: true }
            } if uri == "graph.omni"
        ));
        assert_eq!(
            planes::command_capability(&cli.command),
            planes::Capability::Direct
        );
        assert!(planes::guard_addressing(&cli).is_ok());
    }

    #[test]
    fn system_column_upgrade_rejects_served_and_cluster_addressing() {
        for flag in ["--server", "--cluster", "--graph"] {
            let cli = Cli::try_parse_from([
                "omnigraph",
                flag,
                "x",
                "schema",
                "upgrade-system-columns",
                "graph.omni",
            ])
            .unwrap();
            assert!(
                planes::guard_addressing(&cli).is_err(),
                "{flag} must be refused"
            );
        }
    }
}
