//! Strict stdout protocol shared by the scenario parent and its ordinary
//! contract test. Child stdout is evidence, so malformed or ambiguous records
//! must fail the run instead of disappearing through best-effort parsing.

pub(crate) struct ParsedChildRecords {
    pub(crate) records: Vec<serde_json::Value>,
    pub(crate) protocol_error: Option<String>,
}

pub(crate) fn parse_child_records(
    child_stdout: &str,
    process_exit_status: i64,
) -> ParsedChildRecords {
    let mut records = Vec::new();
    let mut errors = Vec::new();
    let mut memory_cap_records = 0_usize;
    let mut scenario_metric_records = 0_usize;

    for (line_index, line) in child_stdout.lines().enumerate() {
        let record = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(record) => record,
            Err(error) => {
                errors.push(format!(
                    "child stdout line {} is not JSON: {error}",
                    line_index + 1
                ));
                continue;
            }
        };
        let has_memory_cap = record.get("memory_cap_status").is_some();
        let has_scenario_metrics = record.get("scenario_metrics").is_some();
        if let Some(value) = record.get("memory_cap_status")
            && !value.is_object()
        {
            errors.push(format!(
                "child stdout line {} has a non-object memory-cap value",
                line_index + 1
            ));
        }
        if let Some(value) = record.get("scenario_metrics")
            && !value.is_object()
        {
            errors.push(format!(
                "child stdout line {} has a non-object scenario-metrics value",
                line_index + 1
            ));
        }
        match (has_memory_cap, has_scenario_metrics) {
            (true, false) => memory_cap_records += 1,
            (false, true) => scenario_metric_records += 1,
            (true, true) => errors.push(format!(
                "child stdout line {} combines memory-cap and scenario records",
                line_index + 1
            )),
            (false, false) => errors.push(format!(
                "child stdout line {} has no recognized record kind",
                line_index + 1
            )),
        }
        records.push(record);
    }

    if memory_cap_records != 1 {
        errors.push(format!(
            "expected exactly one memory-cap record, observed {memory_cap_records}"
        ));
    }
    if process_exit_status == 0 && scenario_metric_records != 1 {
        errors.push(format!(
            "successful child must emit exactly one scenario-metrics record, observed {scenario_metric_records}"
        ));
    } else if scenario_metric_records > 1 {
        errors.push(format!(
            "expected at most one scenario-metrics record, observed {scenario_metric_records}"
        ));
    }

    ParsedChildRecords {
        records,
        protocol_error: (!errors.is_empty()).then(|| errors.join("; ")),
    }
}
