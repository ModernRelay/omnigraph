//! Read presentation captured from an already loaded operator configuration.

use std::io::Write;

use color_eyre::eyre::Result;
use omnigraph_api_types::ReadOutput;

use crate::invocation::MachineErrors;
use crate::operator::OperatorConfig;
use crate::read_format::{ReadOutputFormat, ReadRenderOptions, render_read};

pub(crate) struct ReadPresentation {
    pub(crate) format: ReadOutputFormat,
    pub(crate) table_options: ReadRenderOptions,
}

impl ReadPresentation {
    /// `--json` > `--format` > alias format > operator default > table.
    pub(crate) fn capture(
        operator: &OperatorConfig,
        explicit: Option<ReadOutputFormat>,
        json: bool,
        alias_format: Option<ReadOutputFormat>,
    ) -> Self {
        let format = if json {
            ReadOutputFormat::Json
        } else {
            explicit
                .or(alias_format)
                .or(operator.output())
                .unwrap_or_default()
        };
        Self {
            format,
            table_options: ReadRenderOptions {
                max_column_width: operator.defaults.table_max_column_width.unwrap_or(80),
                cell_layout: operator.defaults.table_cell_layout.unwrap_or_default(),
            },
        }
    }

    pub(crate) fn machine_errors(&self) -> Option<MachineErrors> {
        match self.format {
            ReadOutputFormat::Json => Some(MachineErrors::Json),
            ReadOutputFormat::Jsonl => Some(MachineErrors::Jsonl),
            ReadOutputFormat::Toon => Some(MachineErrors::Toon),
            _ => None,
        }
    }

    pub(crate) fn write_read(&self, output: &ReadOutput, writer: &mut dyn Write) -> Result<()> {
        writeln!(
            writer,
            "{}",
            render_read(output, self.format, &self.table_options)?
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use omnigraph_api_types::{ChangeOutput, ReadTargetOutput};

    use super::*;
    use crate::output::{write_change_human, write_json};
    use crate::read_format::TableCellLayout;

    #[test]
    fn format_precedence_also_selects_machine_errors() {
        use ReadOutputFormat::{Arrow, Csv, Json, Jsonl, Kv, Table, Toon};

        for (default, alias, explicit, json, expected) in [
            (None, None, None, false, Table),
            (Some(Json), None, None, false, Json),
            (Some(Json), Some(Jsonl), None, false, Jsonl),
            (Some(Json), Some(Jsonl), Some(Csv), false, Csv),
            (Some(Jsonl), Some(Kv), Some(Table), false, Table),
            (Some(Json), None, Some(Arrow), false, Arrow),
            (Some(Json), None, Some(Kv), false, Kv),
            (Some(Json), None, Some(Toon), false, Toon),
            (Some(Toon), Some(Jsonl), Some(Csv), true, Json),
            (Some(Kv), Some(Jsonl), Some(Csv), true, Json),
        ] {
            let mut operator = OperatorConfig::default();
            operator.defaults.output = default;
            let presentation = ReadPresentation::capture(&operator, explicit, json, alias);
            assert_eq!(presentation.format, expected);
            assert_eq!(
                presentation.machine_errors(),
                match expected {
                    Json => Some(MachineErrors::Json),
                    Jsonl => Some(MachineErrors::Jsonl),
                    Toon => Some(MachineErrors::Toon),
                    _ => None,
                }
            );
        }
    }

    #[test]
    fn capture_retains_table_preferences_without_borrowing_config() {
        let mut operator = OperatorConfig::default();
        let defaults = ReadPresentation::capture(&operator, None, false, None);
        assert_eq!(defaults.table_options.max_column_width, 80);
        assert_eq!(
            defaults.table_options.cell_layout,
            TableCellLayout::Truncate
        );

        operator.defaults.output = Some(ReadOutputFormat::Jsonl);
        operator.defaults.table_max_column_width = Some(24);
        operator.defaults.table_cell_layout = Some(TableCellLayout::Wrap);
        let captured = ReadPresentation::capture(&operator, None, false, None);
        operator.defaults.output = Some(ReadOutputFormat::Csv);
        operator.defaults.table_max_column_width = Some(120);
        operator.defaults.table_cell_layout = Some(TableCellLayout::Truncate);
        assert_eq!(captured.format, ReadOutputFormat::Jsonl);
        assert_eq!(captured.table_options.max_column_width, 24);
        assert_eq!(captured.table_options.cell_layout, TableCellLayout::Wrap);
    }

    fn read_output() -> ReadOutput {
        ReadOutput {
            query_name: "people".to_owned(),
            target: ReadTargetOutput {
                branch: Some("main".to_owned()),
                snapshot: None,
            },
            row_count: 1,
            columns: vec!["name".to_owned()],
            rows: serde_json::value::RawValue::from_string(r#"[{"name":"Alice"}]"#.to_owned())
                .unwrap(),
            graph_commit_id: None,
            page: None,
            ranking: None,
        }
    }

    #[test]
    fn write_read_preserves_csv_bytes_and_trailing_newline() {
        let presentation = ReadPresentation::capture(
            &OperatorConfig::default(),
            Some(ReadOutputFormat::Csv),
            false,
            None,
        );
        let mut bytes = Vec::new();
        presentation.write_read(&read_output(), &mut bytes).unwrap();
        assert_eq!(bytes, b"name\nAlice\n");
    }

    struct FailsAfterPrefix {
        bytes: Vec<u8>,
    }

    impl Write for FailsAfterPrefix {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let remaining = 5 - self.bytes.len();
            if remaining == 0 {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed output"));
            }
            let count = remaining.min(bytes.len());
            self.bytes.extend_from_slice(&bytes[..count]);
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn assert_writer_failure(write: impl FnOnce(&mut dyn Write) -> Result<()>) {
        let mut writer = FailsAfterPrefix { bytes: Vec::new() };
        let error = write(&mut writer).unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(writer.bytes.len(), 5);
    }

    #[test]
    fn injected_writers_report_errors_after_partial_output() {
        let presentation = ReadPresentation::capture(&OperatorConfig::default(), None, false, None);
        assert_writer_failure(|writer| presentation.write_read(&read_output(), writer));
        assert_writer_failure(|writer| write_json(&serde_json::json!({ "ok": true }), writer));
        let change = ChangeOutput {
            branch: "main".to_owned(),
            query_name: "insert".to_owned(),
            affected_nodes: 1,
            affected_edges: 0,
            actor_id: None,
            commit: None,
            outcome: None,
            executed: None,
        };
        assert_writer_failure(|writer| write_change_human(&change, writer));
    }
}
