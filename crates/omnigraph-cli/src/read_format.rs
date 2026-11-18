use std::io::{BufWriter, Write};

use clap::ValueEnum;
use clap::builder::{PossibleValuesParser, TypedValueParser};
use color_eyre::eyre::Result;
use omnigraph_api_types::ErrorCode;
use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::result::QueryResult;
use omnigraph_server::api::ReadOutput;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::helpers::CodedRefusal;

/// Output rendering format for read-shaped commands (`read`/`query`/`alias`).
/// A CLI presentation concern — lives here, not in the server.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ReadOutputFormat {
    #[default]
    Table,
    Kv,
    Csv,
    Jsonl,
    Json,
    /// Same document as json, in TOON (specification 4.4).
    Toon,
    /// The result's columns and rows as an Arrow IPC stream on stdout. Direct
    /// targets only; refused when stdout is a terminal.
    Arrow,
}

/// `--format` for a read whose answer is always a server's JSON (an alias
/// invokes a stored query by name): every format but `arrow`.
pub(crate) fn rendered_format_parser() -> impl TypedValueParser<Value = ReadOutputFormat> {
    PossibleValuesParser::new(
        ReadOutputFormat::value_variants()
            .iter()
            .filter(|format| **format != ReadOutputFormat::Arrow)
            .filter_map(ValueEnum::to_possible_value),
    )
    .try_map(|name| ReadOutputFormat::from_str(&name, false))
}

/// The refusal of `--format arrow` for an answer that is rendered JSON and
/// has no result batches: a control statement's (`branch list`, `show`), or
/// one a server sent. `answer` names it.
pub(crate) fn arrow_has_no_batches(answer: &str) -> color_eyre::Report {
    CodedRefusal::report(
        ErrorCode::BadRequest,
        format!(
            "`--format arrow` writes a query's result batches, and {answer} has none; use `--format json` or `--format jsonl`"
        ),
    )
}

/// Write the engine's result batches to `writer` as one Arrow IPC stream:
/// the result's schema, its rows, and the end-of-stream marker. Nothing else
/// travels with them: the read's target, graph commit and ranking stay in
/// the JSON document. Every refusal precedes this call, so stdout never holds
/// part of a stream a refusal cut short; a failed write fails delivery, as
/// any format's does.
pub(crate) fn write_arrow_stream(result: &QueryResult, writer: &mut dyn Write) -> Result<()> {
    let mut writer = BufWriter::new(writer);
    result
        .write_arrow_ipc(&mut writer)
        .map_err(|error| match error {
            CompilerError::Io(error) => color_eyre::Report::from(error),
            error => error.into(),
        })?;
    writer.flush()?;
    Ok(())
}

/// How an over-wide table cell is laid out when rendering `--format table`.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TableCellLayout {
    #[default]
    Truncate,
    Wrap,
}

pub struct ReadRenderOptions {
    pub max_column_width: usize,
    pub cell_layout: TableCellLayout,
}

pub fn render_read(
    output: &ReadOutput,
    format: ReadOutputFormat,
    options: &ReadRenderOptions,
) -> Result<String> {
    match format {
        ReadOutputFormat::Json => Ok(serde_json::to_string_pretty(output)?),
        ReadOutputFormat::Toon => {
            let value = serde_json::to_value(output)?;
            Ok(crate::toon::encode(&value))
        }
        ReadOutputFormat::Jsonl => render_jsonl(output),
        ReadOutputFormat::Csv => render_csv(output),
        ReadOutputFormat::Kv => Ok(render_kv(output)? + &page_footer(output)),
        ReadOutputFormat::Table => Ok(render_table(output, options)? + &page_footer(output)),
        // Arrow output writes the engine's batches (`write_arrow_stream`);
        // a rendered answer reaches here only without any.
        ReadOutputFormat::Arrow => Err(arrow_has_no_batches("this answer")),
    }
}

fn render_jsonl(output: &ReadOutput) -> Result<String> {
    let mut lines = Vec::new();
    let mut metadata = serde_json::json!({
        "kind": "metadata",
        "query_name": output.query_name,
        "target": output.target,
        "row_count": output.row_count,
        "page": output.page,
    });
    if let Some(ranking) = &output.ranking {
        metadata["ranking"] = serde_json::to_value(ranking)?;
    }
    lines.push(serde_json::to_string(&metadata)?);
    for row in rows(output)? {
        lines.push(serde_json::to_string(&row)?);
    }
    Ok(lines.join("\n"))
}

fn render_csv(output: &ReadOutput) -> Result<String> {
    let rows = rows(output)?;
    let columns = columns(output, &rows);
    let mut lines = Vec::new();
    lines.push(
        columns
            .iter()
            .map(|column| csv_escape(column))
            .collect::<Vec<_>>()
            .join(","),
    );
    for row in rows {
        lines.push(
            columns
                .iter()
                .map(|column| csv_cell(row.get(column)))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    Ok(lines.join("\n"))
}

fn render_kv(output: &ReadOutput) -> Result<String> {
    let mut lines = vec![header_line(output)];
    let rows = rows(output)?;
    if rows.is_empty() {
        lines.push("(no rows)".to_string());
        return Ok(lines.join("\n"));
    }

    let columns = columns(output, &rows);
    for (idx, row) in rows.iter().enumerate() {
        if idx > 0 {
            lines.push(String::new());
        }
        lines.push(format!("row {}", idx + 1));
        for column in &columns {
            lines.push(format!(
                "{}: {}",
                column,
                stringify_value(row.get(column).unwrap_or(&Value::Null))
            ));
        }
    }
    Ok(lines.join("\n"))
}

fn render_table(output: &ReadOutput, options: &ReadRenderOptions) -> Result<String> {
    let mut lines = vec![header_line(output)];
    let rows = rows(output)?;
    let columns = columns(output, &rows);

    if columns.is_empty() {
        lines.push("(no rows)".to_string());
        return Ok(lines.join("\n"));
    }

    let widths = columns
        .iter()
        .map(|column| {
            let mut width = column.chars().count();
            for row in &rows {
                let rendered =
                    normalize_cell(&stringify_value(row.get(column).unwrap_or(&Value::Null)));
                let longest = rendered
                    .lines()
                    .map(|line| line.chars().count())
                    .max()
                    .unwrap_or(0);
                width = width.max(longest.min(options.max_column_width));
            }
            width.min(options.max_column_width.max(8))
        })
        .collect::<Vec<_>>();

    lines.push(render_table_line(&columns, &widths));
    lines.push(
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("-+-"),
    );

    for row in rows {
        let cell_lines = columns
            .iter()
            .zip(widths.iter())
            .map(|(column, width)| {
                split_cell(
                    &normalize_cell(&stringify_value(row.get(column).unwrap_or(&Value::Null))),
                    *width,
                    options.cell_layout,
                )
            })
            .collect::<Vec<_>>();
        let line_count = cell_lines.iter().map(Vec::len).max().unwrap_or(1);
        for line_idx in 0..line_count {
            let rendered = cell_lines
                .iter()
                .zip(widths.iter())
                .map(|(segments, width)| {
                    let segment = segments.get(line_idx).cloned().unwrap_or_default();
                    pad_to_width(&segment, *width)
                })
                .collect::<Vec<_>>();
            lines.push(rendered.join(" | "));
        }
    }

    Ok(lines.join("\n"))
}

fn render_table_line(columns: &[String], widths: &[usize]) -> String {
    columns
        .iter()
        .zip(widths.iter())
        .map(|(column, width)| pad_to_width(column, *width))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn header_line(output: &ReadOutput) -> String {
    let from = output
        .target
        .snapshot
        .as_deref()
        .map(|id| format!(" from snapshot {}", id))
        .or_else(|| {
            output
                .target
                .branch
                .as_deref()
                .map(|branch| format!(" from branch {}", branch))
        })
        .unwrap_or_default();
    let compared = output
        .page
        .as_ref()
        .and_then(|page| {
            Some((
                page.from_commit_id.as_deref()?,
                page.to_commit_id.as_deref()?,
            ))
        })
        .map(|(from, to)| format!(" comparing {from} to {to}"))
        .unwrap_or_default();
    format!(
        "{} rows{}{} via {}",
        output.row_count, from, compared, output.query_name
    )
}

/// The line a paged answer ends with when a further page exists: the clause
/// that continues it.
fn page_footer(output: &ReadOutput) -> String {
    output
        .page
        .as_ref()
        .and_then(|page| page.next_page_token.as_deref())
        .map(|token| format!("\nmore: continue with `after page \"{token}\"`"))
        .unwrap_or_default()
}

fn rows(output: &ReadOutput) -> Result<Vec<Map<String, Value>>> {
    let parsed: Value = serde_json::from_str(output.rows.get())?;
    Ok(parsed
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| match row {
            Value::Object(map) => map.clone(),
            other => {
                let mut map = Map::new();
                map.insert("value".to_string(), other.clone());
                map
            }
        })
        .collect())
}

fn columns(output: &ReadOutput, rows: &[Map<String, Value>]) -> Vec<String> {
    if !output.columns.is_empty() {
        return output.columns.clone();
    }

    let mut columns = rows
        .iter()
        .flat_map(|row| row.keys().cloned())
        .collect::<Vec<_>>();
    columns.sort();
    columns.dedup();
    columns
}

fn stringify_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "<invalid json>".to_string()),
    }
}

fn normalize_cell(value: &str) -> String {
    value.replace('\n', "\\n")
}

fn split_cell(value: &str, width: usize, layout: TableCellLayout) -> Vec<String> {
    if value.is_empty() {
        return vec![String::new()];
    }
    if value.chars().count() <= width {
        return vec![value.to_string()];
    }
    match layout {
        TableCellLayout::Truncate => vec![truncate(value, width)],
        TableCellLayout::Wrap => wrap(value, width),
    }
}

fn truncate(value: &str, width: usize) -> String {
    if width <= 1 {
        return value.chars().take(width).collect();
    }
    let keep = width.saturating_sub(1);
    let mut out = value.chars().take(keep).collect::<String>();
    out.push('…');
    out
}

fn wrap(value: &str, width: usize) -> Vec<String> {
    let chars = value.chars().collect::<Vec<_>>();
    chars
        .chunks(width.max(1))
        .map(|chunk| chunk.iter().collect::<String>())
        .collect()
}

fn pad_to_width(value: &str, width: usize) -> String {
    let value_width = value.chars().count();
    if value_width >= width {
        value.to_string()
    } else {
        format!("{}{}", value, " ".repeat(width - value_width))
    }
}

/// One CSV field. A null (a JSON row omits its key) is an empty unquoted
/// field, the spelling `load csv` reads as null, while an empty string is
/// `""`; so the string `null` stays a string.
fn csv_cell(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(value) => csv_escape(&stringify_value(value)),
    }
}

fn csv_escape(value: &str) -> String {
    if value.is_empty() || value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use omnigraph_server::api::{
        RankOrder, RankingOutput, ReadOutput, ReadTargetOutput, SearchFunction,
    };

    use super::*;

    fn sample_output() -> ReadOutput {
        ReadOutput {
            query_name: "get_person".to_string(),
            target: ReadTargetOutput {
                branch: Some("main".to_string()),
                snapshot: None,
            },
            row_count: 1,
            columns: vec!["name".to_string(), "age".to_string()],
            rows: serde_json::value::RawValue::from_string(
                r#"[{"name":"Alice","age":30}]"#.to_string(),
            )
            .unwrap(),
            graph_commit_id: None,
            page: None,
            ranking: None,
        }
    }

    #[test]
    fn csv_format_outputs_header_and_rows() {
        let rendered = render_read(
            &sample_output(),
            ReadOutputFormat::Csv,
            &ReadRenderOptions {
                max_column_width: 80,
                cell_layout: TableCellLayout::Truncate,
            },
        )
        .unwrap();

        assert!(rendered.lines().next().unwrap().contains("name,age"));
        assert!(rendered.contains("Alice,30"));
    }

    #[test]
    fn toon_format_encodes_the_same_document_as_json() {
        let output = sample_output();
        let document = serde_json::to_value(&output).unwrap();
        let rendered = render_read(
            &output,
            ReadOutputFormat::Toon,
            &ReadRenderOptions {
                max_column_width: 80,
                cell_layout: TableCellLayout::Truncate,
            },
        )
        .unwrap();

        assert_eq!(rendered, crate::toon::encode(&document));
        assert_eq!(
            rendered,
            "\
query_name: get_person
target:
  branch: main
  snapshot: null
row_count: 1
columns[2]: name,age
rows[1]{name,age}:
  Alice,30"
        );
    }

    #[test]
    fn toon_format_repeats_keys_when_a_null_cell_is_omitted() {
        let mut output = sample_output();
        output.row_count = 2;
        output.columns = vec!["name".to_string(), "score".to_string()];
        output.rows = serde_json::value::RawValue::from_string(
            r#"[{"name":"Ada, Lovelace","score":1.0},{"name":"Grace"}]"#.to_string(),
        )
        .unwrap();

        let rendered = render_read(
            &output,
            ReadOutputFormat::Toon,
            &ReadRenderOptions {
                max_column_width: 80,
                cell_layout: TableCellLayout::Truncate,
            },
        )
        .unwrap();

        assert_eq!(
            rendered,
            "\
query_name: get_person
target:
  branch: main
  snapshot: null
row_count: 2
columns[2]: name,score
rows[2]:
  - name: \"Ada, Lovelace\"
    score: 1
  - name: Grace"
        );
    }

    #[test]
    fn json_format_prints_the_envelope_pretty_and_the_rows_compact() {
        let rendered = render_read(
            &sample_output(),
            ReadOutputFormat::Json,
            &ReadRenderOptions {
                max_column_width: 80,
                cell_layout: TableCellLayout::Truncate,
            },
        )
        .unwrap();

        assert!(rendered.contains("\n  \"row_count\": 1,\n"), "{rendered}");
        assert!(
            rendered.contains(r#""rows": [{"name":"Alice","age":30}]"#),
            "{rendered}"
        );
    }

    #[test]
    fn jsonl_format_emits_metadata_first() {
        let rendered = render_read(
            &sample_output(),
            ReadOutputFormat::Jsonl,
            &ReadRenderOptions {
                max_column_width: 80,
                cell_layout: TableCellLayout::Truncate,
            },
        )
        .unwrap();

        let first = rendered.lines().next().unwrap();
        assert!(first.contains("\"kind\":\"metadata\""));
        assert!(!first.contains("ranking"));
        assert!(
            rendered
                .lines()
                .nth(1)
                .unwrap()
                .contains("\"name\":\"Alice\"")
        );

        // A ranked read's ranking rides in the metadata record, as in the
        // `--json` envelope.
        let mut ranked = sample_output();
        ranked.ranking = Some(RankingOutput {
            by: SearchFunction::Bm25,
            order: RankOrder::Descending,
            exhaustive: true,
            score_columns: vec!["score".to_string()],
        });
        let rendered = render_read(
            &ranked,
            ReadOutputFormat::Jsonl,
            &ReadRenderOptions {
                max_column_width: 80,
                cell_layout: TableCellLayout::Truncate,
            },
        )
        .unwrap();
        let metadata: Value = serde_json::from_str(rendered.lines().next().unwrap()).unwrap();
        assert_eq!(
            metadata["ranking"],
            serde_json::to_value(&ranked.ranking).unwrap()
        );
    }

    #[test]
    fn render_falls_back_to_discovered_columns_for_legacy_payloads() {
        let mut output = sample_output();
        output.columns.clear();

        let rendered = render_read(
            &output,
            ReadOutputFormat::Csv,
            &ReadRenderOptions {
                max_column_width: 80,
                cell_layout: TableCellLayout::Truncate,
            },
        )
        .unwrap();

        assert!(rendered.lines().next().unwrap().contains("age,name"));
    }

    /// A null cell (its key omitted from the JSON row) and the stored string
    /// "null" must not be written as the same CSV field, or a reader cannot
    /// tell them apart.
    #[test]
    fn csv_keeps_a_null_cell_distinct_from_the_string_null() {
        let mut output = sample_output();
        output.columns = vec!["slug".to_string(), "s".to_string()];
        output.row_count = 3;
        output.rows = serde_json::value::RawValue::from_string(
            r#"[{"slug":"empty","s":""},{"slug":"nul"},{"slug":"word","s":"null"}]"#.to_string(),
        )
        .unwrap();

        let rendered = render_read(
            &output,
            ReadOutputFormat::Csv,
            &ReadRenderOptions {
                max_column_width: 80,
                cell_layout: TableCellLayout::Truncate,
            },
        )
        .unwrap();
        let cells: Vec<&str> = rendered
            .lines()
            .skip(1)
            .map(|line| line.split_once(',').unwrap().1)
            .collect();

        assert_eq!(cells.len(), 3, "{rendered}");
        assert_ne!(cells[0], cells[1], "empty string and null: {rendered}");
        assert_ne!(
            cells[1], cells[2],
            "null and the string \"null\": {rendered}"
        );
        // The spelling `load csv` reads back: an empty field is null.
        assert_eq!(rendered, "slug,s\nempty,\"\"\nnul,\nword,null");
    }

    #[test]
    fn csv_quotes_carriage_returns() {
        assert_eq!(csv_escape("hello\rworld"), "\"hello\rworld\"");
    }
}
