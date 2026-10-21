use super::*;
use crate::app::Completion;

pub(crate) async fn run(
    target: &crate::targets::ResolvedTarget,
    check: bool,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    if !matches!(
        target.binding,
        crate::targets::TargetBinding::DirectGraph { .. }
    ) {
        bail!("schema upgrade-system-columns requires a standalone direct graph target");
    }
    let uri = scope::direct_graph_uri(target, None).await?;
    let uri = omnigraph::storage::normalize_root_uri(&uri)?;
    let binding = scope::capture_storage_binding(target)?.for_uri(&uri)?;
    if let Some(root) = omnigraph_cluster::cluster_root_for_graph_binding(&binding)
        .await
        .map_err(|diagnostic| {
            color_eyre::eyre::eyre!("{}: {}", diagnostic.path, diagnostic.message)
        })?
    {
        bail!(
            "schema upgrade-system-columns refuses graph `{uri}` inside cluster `{root}`; a qualified cluster upgrade operation is required"
        );
    }
    let uri = binding.uri();
    let db = if check {
        omnigraph::db::Omnigraph::open_read_only_with_storage(
            uri,
            omnigraph::storage::storage_for_binding(&binding)?,
        )
        .await?
    } else {
        omnigraph::db::Omnigraph::open_with_storage(
            uri,
            omnigraph::storage::storage_for_binding(&binding)?,
        )
        .await?
    };
    let report = db
        .upgrade_system_columns(omnigraph::db::SystemColumnUpgradeOptions { check })
        .await?;
    finish_report(&report, json, stdout)
}

fn finish_report(
    report: &omnigraph::db::SystemColumnUpgradeReport,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    if json {
        write_json(report, stdout)?;
    } else {
        write_human(report, stdout)?;
    }
    if !report.success() {
        stdout.flush()?;
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
                panic!("a failed report write must not reach flush");
            }
        }

        #[derive(Default)]
        struct ReportWriter {
            bytes: Vec<u8>,
            flushes: usize,
            fail_flush: bool,
        }

        impl Write for ReportWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                self.flushes += 1;
                if self.fail_flush {
                    Err(io::Error::new(io::ErrorKind::BrokenPipe, "output closed"))
                } else {
                    Ok(())
                }
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

            for json in [false, true] {
                let expected = if json {
                    let mut bytes = Vec::new();
                    write_json(&report, &mut bytes).unwrap();
                    bytes
                } else {
                    expected.as_bytes().to_vec()
                };
                for fail_flush in [false, true] {
                    let mut writer = ReportWriter {
                        fail_flush,
                        ..ReportWriter::default()
                    };
                    let result = finish_report(&report, json, &mut writer);
                    if fail_flush && !report.success() {
                        assert_eq!(
                            result
                                .unwrap_err()
                                .downcast_ref::<io::Error>()
                                .unwrap()
                                .kind(),
                            io::ErrorKind::BrokenPipe
                        );
                    } else {
                        assert_eq!(result.unwrap().code(), i32::from(!report.success()));
                    }
                    assert_eq!(writer.bytes, expected);
                    assert_eq!(writer.flushes, usize::from(!report.success()));
                }

                let mut writer = PrefixWriter(Vec::new());
                let error = finish_report(&report, json, &mut writer).unwrap_err();
                assert_eq!(
                    error.downcast_ref::<io::Error>().unwrap().kind(),
                    io::ErrorKind::BrokenPipe
                );
                assert_eq!(writer.0, &expected[..7]);
            }
        }
    }

    #[test]
    fn system_column_upgrade_parses_check_and_json() {
        let cli = Cli::try_parse_from([
            "omnigraph",
            "schema",
            "upgrade-system-columns",
            "--target",
            "./graph.omni",
            "--check",
            "--json",
        ])
        .unwrap();
        assert!(matches!(
            &cli.command,
            Command::Schema {
                command: SchemaCommand::UpgradeSystemColumns {
                    check: true,
                    json: true
                }
            }
        ));
        assert_eq!(cli.target.as_deref(), Some("./graph.omni"));
    }

    #[test]
    fn system_column_upgrade_rejects_retired_addressing() {
        for flag in ["--server", "--cluster", "--store", "--connection"] {
            assert!(
                Cli::try_parse_from(["omnigraph", flag, "x", "schema", "upgrade-system-columns",])
                    .is_err(),
                "{flag} must be refused"
            );
        }
        for args in [
            [
                "omnigraph",
                "--profile",
                "work",
                "schema",
                "upgrade-system-columns",
            ],
            [
                "omnigraph",
                "schema",
                "upgrade-system-columns",
                "--profile",
                "work",
            ],
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            assert_eq!(cli.profile.as_deref(), Some("work"));
            assert_eq!(
                crate::planes::guard_addressing(&cli)
                    .unwrap_err()
                    .to_string(),
                "schema upgrade-system-columns does not accept --profile"
            );
        }
        assert!(
            Cli::try_parse_from([
                "omnigraph",
                "schema",
                "upgrade-system-columns",
                "./graph.omni",
            ])
            .is_err(),
            "only init retains a positional address"
        );
    }
}
