use color_eyre::eyre::Result;
use omnigraph_server::ReadOutputFormat;
use omnigraph_server::api::ReadOutput;
use omnigraph_server::config::TableCellLayout;
use serde_json::{Map, Value};

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
        ReadOutputFormat::Jsonl => render_jsonl(output),
        ReadOutputFormat::Csv => render_csv(output),
        ReadOutputFormat::Kv => Ok(render_kv(output)),
        ReadOutputFormat::Table => Ok(render_table(output, options)),
    }
}

fn render_jsonl(output: &ReadOutput) -> Result<String> {
    let mut lines = Vec::new();
    lines.push(serde_json::to_string(&serde_json::json!({
        "kind": "metadata",
        "query_name": output.query_name,
        "target": output.target,
        "row_count": output.row_count,
    }))?);
    for row in rows(output) {
        lines.push(serde_json::to_string(&row)?);
    }
    Ok(lines.join("\n"))
}

fn render_csv(output: &ReadOutput) -> Result<String> {
    let rows = rows(output);
    let columns = columns(&rows);
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
                .map(|column| csv_escape(&stringify_value(row.get(column).unwrap_or(&Value::Null))))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    Ok(lines.join("\n"))
}

fn render_kv(output: &ReadOutput) -> String {
    let mut lines = vec![header_line(output)];
    let rows = rows(output);
    if rows.is_empty() {
        lines.push("(no rows)".to_string());
        return lines.join("\n");
    }

    for (idx, row) in rows.iter().enumerate() {
        if idx > 0 {
            lines.push(String::new());
        }
        lines.push(format!("row {}", idx + 1));
        for column in columns(&rows) {
            lines.push(format!(
                "{}: {}",
                column,
                stringify_value(row.get(&column).unwrap_or(&Value::Null))
            ));
        }
    }
    lines.join("\n")
}

fn render_table(output: &ReadOutput, options: &ReadRenderOptions) -> String {
    let mut lines = vec![header_line(output)];
    let rows = rows(output);
    let columns = columns(&rows);

    if columns.is_empty() {
        lines.push("(no rows)".to_string());
        return lines.join("\n");
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

    lines.join("\n")
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
    format!(
        "{} rows from {} via {}",
        output.row_count,
        output
            .target
            .snapshot
            .as_deref()
            .map(|id| format!("snapshot {}", id))
            .or_else(|| {
                output
                    .target
                    .branch
                    .as_deref()
                    .map(|branch| format!("branch {}", branch))
            })
            .unwrap_or_else(|| "target".to_string()),
        output.query_name
    )
}

fn rows(output: &ReadOutput) -> Vec<Map<String, Value>> {
    output
        .rows
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
        .collect()
}

fn columns(rows: &[Map<String, Value>]) -> Vec<String> {
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

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use omnigraph_server::api::{ReadOutput, ReadTargetOutput};

    use super::*;

    fn sample_output() -> ReadOutput {
        ReadOutput {
            query_name: "get_person".to_string(),
            target: ReadTargetOutput {
                branch: Some("main".to_string()),
                snapshot: None,
            },
            row_count: 1,
            rows: serde_json::json!([{ "name": "Alice", "age": 30 }]),
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

        assert!(rendered.lines().next().unwrap().contains("age,name"));
        assert!(rendered.contains("30,Alice"));
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
        assert!(
            rendered
                .lines()
                .nth(1)
                .unwrap()
                .contains("\"name\":\"Alice\"")
        );
    }
}
