use super::*;
use crate::app::Completion;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    input: &crate::operator::InvocationConfig,
    profile: &Option<String>,
    store: &Option<String>,
    uri: Option<String>,
    check: bool,
    json: bool,
    quiet: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let target = scope::resolve_scope(
        input.get(stderr)?,
        input.profile(),
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
        write_target(quiet, "schema upgrade-system-columns", &uri, false, stderr)?;
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
        write_json(&report, stdout)?;
    } else {
        write_human(&report, stdout)?;
    }
    if !report.success() {
        return Ok(Completion::exit(1));
    }
    Ok(Completion::success())
}

fn write_human(
    report: &omnigraph::db::SystemColumnUpgradeReport,
    writer: &mut dyn Write,
) -> Result<()> {
    let mode = serde_json::to_value(report.mode)?;
    let outcome = serde_json::to_value(report.outcome)?;
    writeln!(
        writer,
        "system-column upgrade {}: {} ({})",
        report.location,
        outcome.as_str().unwrap_or("unknown"),
        mode.as_str().unwrap_or("unknown")
    )?;
    writeln!(
        writer,
        "storage format: v{} -> v{}",
        report.stamp_before, report.stamp_after
    )?;
    writeln!(writer, "tables: {}", report.tables.join(", "))?;
    if let Some(version) = report.graph_manifest_version {
        writeln!(writer, "graph manifest version: {version}")?;
    }
    for finding in &report.findings {
        writeln!(writer, "{}: {}", finding.code, finding.message)?;
    }
    if matches!(report.mode, omnigraph::db::UpgradeMode::Check) {
        writeln!(
            writer,
            "Check is advisory. Stop every server serving the graph and retain a verified backup before execution."
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn system_column_upgrade_report_preserves_bytes_and_propagates_write_failure() {
        use omnigraph::db::{
            SystemColumnUpgradeFinding, SystemColumnUpgradeOutcome, SystemColumnUpgradeReport,
            UpgradeMode,
        };

        struct PrefixWriter(Vec<u8>);

        impl Write for PrefixWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                let remaining = 7 - self.0.len();
                if remaining == 0 {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "output closed"));
                }
                let count = remaining.min(bytes.len());
                self.0.extend_from_slice(&bytes[..count]);
                Ok(count)
            }

            fn flush(&mut self) -> io::Result<()> {
                panic!("report formatting leaves flush to application completion");
            }
        }

        let report = SystemColumnUpgradeReport {
            location: "graph".to_string(),
            mode: UpgradeMode::Check,
            outcome: SystemColumnUpgradeOutcome::Refused,
            stamp_before: 16,
            stamp_after: 16,
            tables: vec!["node:Doc".to_string(), "edge:Link".to_string()],
            findings: vec![SystemColumnUpgradeFinding {
                code: "system_columns_preflight",
                message: "stop serving first".to_string(),
            }],
            graph_manifest_version: None,
        };
        let completed = SystemColumnUpgradeReport {
            mode: UpgradeMode::Execute,
            outcome: SystemColumnUpgradeOutcome::Completed,
            findings: Vec::new(),
            graph_manifest_version: Some(9),
            ..report.clone()
        };
        for (report, expected) in [
            (
                report,
                "system-column upgrade graph: refused (check)\n\
                storage format: v16 -> v16\ntables: node:Doc, edge:Link\n\
                system_columns_preflight: stop serving first\n\
                Check is advisory. Stop every server serving the graph and retain a verified backup before execution.\n",
            ),
            (
                completed,
                "system-column upgrade graph: completed (execute)\n\
                storage format: v16 -> v16\ntables: node:Doc, edge:Link\ngraph manifest version: 9\n",
            ),
        ] {
            let mut bytes = Vec::new();
            write_human(&report, &mut bytes).unwrap();
            assert_eq!(bytes, expected.as_bytes());

            let mut writer = PrefixWriter(Vec::new());
            let error = write_human(&report, &mut writer).unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(writer.0, b"system-");
        }
    }

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
