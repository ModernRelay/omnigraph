use std::io::Write;

use color_eyre::eyre::{Result, bail};

use crate::app::Completion;
use crate::commands::maintenance::args::UpgradeArgs;
use crate::output::write_json;
use crate::scope;

pub(crate) async fn run(
    target: &crate::targets::ResolvedTarget,
    args: UpgradeArgs,
    stdout: &mut dyn Write,
    _stderr: &mut dyn Write,
) -> Result<Completion> {
    let UpgradeArgs {
        check,
        to_format,
        json,
    } = args;
    if !matches!(
        target.binding,
        crate::targets::TargetBinding::DirectGraph { .. }
    ) {
        bail!("upgrade requires a direct graph target; cluster graph upgrades are not qualified");
    }
    let uri = scope::direct_graph_uri(target, None).await?;
    let binding = scope::capture_storage_binding(target)?.for_uri(&uri)?;
    if let Some(root) = omnigraph_cluster::cluster_root_for_graph_binding(&binding)
        .await
        .map_err(|diagnostic| {
            color_eyre::eyre::eyre!("{}: {}", diagnostic.path, diagnostic.message)
        })?
    {
        bail!(
            "upgrade refuses graph `{uri}` inside cluster `{root}`; a qualified cluster upgrade operation is required"
        );
    }
    let report = omnigraph::db::upgrade_storage_as_with_storage(
        binding.uri(),
        omnigraph::db::UpgradeOptions { check, to_format },
        None,
        None,
        omnigraph::storage::storage_for_binding(&binding)?,
    )
    .await?;
    finish_report(&report, json, stdout)
}

fn finish_report(
    report: &omnigraph::db::UpgradeReport,
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

fn write_human(report: &omnigraph::db::UpgradeReport, writer: &mut dyn Write) -> Result<()> {
    let mode = serde_json::to_value(report.mode)?;
    let outcome = serde_json::to_value(report.outcome)?;
    writeln!(
        writer,
        "upgrade {}: {} ({})",
        report.location,
        outcome.as_str().unwrap_or("unknown"),
        mode.as_str().unwrap_or("unknown")
    )?;
    writeln!(
        writer,
        "graph identity: {}",
        report.graph_identity.as_deref().unwrap_or("unknown")
    )?;
    writeln!(
        writer,
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
    )?;
    writeln!(writer, "route: {}", joined(&report.route, " -> "))?;
    writeln!(
        writer,
        "completed handlers: {}",
        joined(&report.completed_handlers, ", ")
    )?;
    writeln!(
        writer,
        "last durable completed boundary: {}",
        report
            .last_durable_completed_boundary
            .as_deref()
            .unwrap_or("none")
    )?;
    let work = &report.work;
    writeln!(
        writer,
        "source: {} live refs, {} retired refs, {} orphan writers, {} legacy commits, \
         {} bookkeeping versions, {} absent parents",
        work.live_refs,
        work.retired_refs,
        work.orphan_writers,
        work.legacy_commits,
        work.bookkeeping_versions,
        work.absent_parents
    )?;
    writeln!(
        writer,
        "legacy objects: {} data files, {} id shards, {} writer shards, {} schema contents, \
         {} bytes",
        work.data_files,
        work.id_shards,
        work.writer_shards,
        work.schema_contents,
        work.legacy_bytes
    )?;
    writeln!(
        writer,
        "census: {} reads, {} cells",
        work.census_reads, work.census_cells
    )?;
    for finding in &report.findings {
        writeln!(writer, "{}: {}", finding.code, finding.message)?;
    }
    if let Some(recovery) = &report.recovery {
        writeln!(writer, "failed handler: {}", recovery.failed_handler)?;
        writeln!(
            writer,
            "recovery executable: {}",
            recovery.executable_compatibility
        )?;
        writeln!(writer, "recovery action: {}", recovery.action)?;
    }
    if matches!(report.mode, omnigraph::db::UpgradeMode::Check) {
        writeln!(
            writer,
            "Check is advisory. Keep the graph offline: stop all readers, writers and \
             maintenance, retain a verified backup of the whole root, then run `omnigraph \
             upgrade --target <graph>` without `--check`."
        )?;
    }
    Ok(())
}

fn joined(items: &[String], separator: &str) -> String {
    if items.is_empty() {
        "none".to_owned()
    } else {
        items.join(separator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command};
    use clap::Parser;

    #[test]
    fn upgrade_report_preserves_recovery_details_and_propagates_output_failure() {
        use omnigraph::db::{
            UpgradeFinding, UpgradeMode, UpgradeOutcome, UpgradeRecovery, UpgradeReport,
            UpgradeWork,
        };
        use std::io;

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

        let mut report = UpgradeReport {
            mode: UpgradeMode::Check,
            outcome: UpgradeOutcome::CheckFailed,
            location: "graph".into(),
            graph_identity: None,
            observed_format: None,
            target_format: 16,
            target_defaulted: true,
            route: Vec::new(),
            completed_handlers: Vec::new(),
            findings: vec![UpgradeFinding {
                code: "unsupported_source".into(),
                message: "inspect the source".into(),
            }],
            last_durable_completed_boundary: None,
            recovery: None,
            work: UpgradeWork::default(),
        };
        let mut bytes = Vec::new();
        write_human(&report, &mut bytes).unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            concat!(
                "upgrade graph: check_failed (check)\n",
                "graph identity: unknown\nformat: unknown -> 16 (default target)\n",
                "route: none\ncompleted handlers: none\nlast durable completed boundary: none\n",
                "source: 0 live refs, 0 retired refs, 0 orphan writers, 0 legacy commits, 0 bookkeeping versions, 0 absent parents\n",
                "legacy objects: 0 data files, 0 id shards, 0 writer shards, 0 schema contents, 0 bytes\n",
                "census: 0 reads, 0 cells\nunsupported_source: inspect the source\n",
                "Check is advisory. Keep the graph offline: stop all readers, writers and maintenance, retain a verified backup of the whole root, then run `omnigraph upgrade --target <graph>` without `--check`.\n",
            )
        );

        report.mode = UpgradeMode::Execute;
        report.outcome = UpgradeOutcome::RecoveryRequired;
        report.graph_identity = Some("original-identity".into());
        report.observed_format = Some(13);
        report.target_defaulted = false;
        report.route = vec!["convert".into(), "activate".into()];
        report.completed_handlers = vec!["convert".into()];
        report.findings.clear();
        report.last_durable_completed_boundary = Some("converted".into());
        report.recovery = Some(UpgradeRecovery {
            failed_handler: "activate".into(),
            executable_compatibility: "the original executable".into(),
            action: "keep offline and inspect".into(),
        });
        let mut bytes = Vec::new();
        write_human(&report, &mut bytes).unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            concat!(
                "upgrade graph: recovery_required (execute)\n",
                "graph identity: original-identity\nformat: 13 -> 16\n",
                "route: convert -> activate\ncompleted handlers: convert\nlast durable completed boundary: converted\n",
                "source: 0 live refs, 0 retired refs, 0 orphan writers, 0 legacy commits, 0 bookkeeping versions, 0 absent parents\n",
                "legacy objects: 0 data files, 0 id shards, 0 writer shards, 0 schema contents, 0 bytes\n",
                "census: 0 reads, 0 cells\nfailed handler: activate\n",
                "recovery executable: the original executable\nrecovery action: keep offline and inspect\n",
            )
        );
        let mut prefix = [0_u8; 7];
        let error = write_human(&report, &mut prefix.as_mut_slice()).unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::WriteZero
        );
        assert_eq!(&prefix, b"upgrade");

        for outcome in [UpgradeOutcome::RecoveryRequired, UpgradeOutcome::Completed] {
            report.outcome = outcome;
            if report.success() {
                report.recovery = None;
            }
            for json in [false, true] {
                let mut expected = Vec::new();
                if json {
                    write_json(&report, &mut expected).unwrap();
                } else {
                    write_human(&report, &mut expected).unwrap();
                }

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

                let mut prefix = [0_u8; 7];
                let error = finish_report(&report, json, &mut prefix.as_mut_slice()).unwrap_err();
                assert_eq!(
                    error.downcast_ref::<io::Error>().unwrap().kind(),
                    io::ErrorKind::WriteZero
                );
                assert_eq!(prefix.as_slice(), &expected[..7]);
            }
        }
    }

    #[test]
    fn storage_upgrade_parses_target_and_check_options() {
        let cli = Cli::try_parse_from([
            "omnigraph",
            "upgrade",
            "--target",
            "./graph.omni",
            "--check",
            "--to-format",
            "15",
            "--json",
        ])
        .unwrap();
        assert_eq!(cli.target.as_deref(), Some("./graph.omni"));
        assert!(matches!(
            cli.command,
            Command::Upgrade(UpgradeArgs {
                check: true,
                to_format: Some(15),
                json: true
            })
        ));
        for args in [
            vec!["upgrade", "graph.omni"],
            vec!["upgrade", "--store", "graph.omni"],
            vec!["upgrade", "--to-format", "not-a-version"],
        ] {
            assert!(Cli::try_parse_from(std::iter::once("omnigraph").chain(args)).is_err());
        }
    }
}
