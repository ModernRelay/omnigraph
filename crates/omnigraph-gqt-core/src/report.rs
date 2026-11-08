//! The observable execution report of the same run GQT checks for rows and
//! plans. `ran` claims read operator attempts; ANN diagnostics are validated
//! without treating timings or physical work counts as fixed expectations.

use std::collections::{BTreeMap, BTreeSet};

use arrow_array::{Array, RecordBatch, StringArray};
use omnigraph_compiler::QueryResult;
use omnigraph_planner::NodeId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What one pass of an operator did, as the engine spells it: `true` or
/// `false` for polled or not, or the side of the node's declared switch it
/// took (`hash_join`, `id_lookup`, `csr`, `indexed_scan`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum Ran {
    Polled(bool),
    Took(String),
}

impl Ran {
    /// The side of a declared switch, `None` on a node without one.
    pub(crate) fn side(&self) -> Option<&str> {
        match self {
            Self::Took(side) => Some(side),
            Self::Polled(_) => None,
        }
    }
}

/// One pass of the tree over a node's operator: whether the operator polled
/// or which side it took.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct Attempt {
    pub(crate) ran: Ran,
}

/// One row of the engine's execution report, read from its serialized form.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct Row {
    pub(crate) id: NodeId,
    pub(crate) attempts: Vec<Attempt>,
}

#[derive(Debug, Deserialize)]
struct SearchDecision {
    id: NodeId,
    rung: usize,
    #[serde(flatten)]
    decision: SearchKind,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
enum SearchKind {
    Gate,
    Probes { attempts: Vec<ProbeAttempt> },
}

/// Decode the published probe shape even though diagnostics only use its
/// position. These values are observations, not additional GQT expectations.
#[derive(Debug, Deserialize, Serialize)]
struct ProbeAttempt {
    maximum_nprobes: Option<u64>,
    flat: bool,
    rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct AnnMetrics {
    id: NodeId,
    rung: usize,
    attempt: usize,
    counts: BTreeMap<String, u64>,
    cumulative_times_ns: BTreeMap<String, u64>,
}

/// An independent assertion of the public diagnostic vocabulary, not an
/// import from the engine's projector: changing that projector must not also
/// change what this test accepts without a contract update.
const ANN_COUNTS: &[&str] = &[
    "partitions_searched",
    "partitions_ranked",
    "prefilter_loads",
    "prefilter_input_rows",
    "prefilter_input_batches",
    "prefilter_row_ids",
];
const ANN_TIMES: &[&str] = &[
    "index_partition_load_time",
    "index_partition_prepare_time",
    "index_prefilter_wait_time",
    "index_cpu_queue_wait_time",
    "index_search_time",
    "index_query_prepare_time",
    "index_distance_topk_time",
    "index_result_materialize_time",
    "index_open_time",
    "prefilter_load_time",
    "prefilter_input_time",
    "prefilter_build_time",
];

#[derive(Debug, Default, Deserialize)]
pub(crate) struct Report {
    pub(crate) rows: Vec<Row>,
    #[serde(default)]
    search: Vec<SearchDecision>,
    #[serde(default)]
    ann_metrics: Vec<AnnMetrics>,
}

impl Report {
    pub(crate) fn has_ann(&self, id: u64) -> bool {
        self.ann_metrics
            .iter()
            .any(|metrics| metrics.id as u64 == id)
    }

    /// Validate the diagnostics' public shape and references, then their
    /// same-run profile rendering. Missing observations are legal; this does
    /// not demand a duration, a partition count, or a metric on every attempt.
    pub(crate) fn validate_ann(
        &self,
        explain: &Value,
        profile: &QueryResult,
    ) -> Result<(), String> {
        let mut nearest = BTreeSet::new();
        let mut pending: Vec<&Value> = explain.get("physical_plan").into_iter().collect();
        while let Some(node) = pending.pop() {
            if node.get("node").and_then(Value::as_str) == Some("Scan")
                && node
                    .get("ranked")
                    .and_then(|ranked| ranked.get("kind"))
                    .and_then(Value::as_str)
                    == Some("nearest")
                && let Some(id) = node.get("id").and_then(Value::as_u64)
            {
                nearest.insert(id);
            }
            if let Some(inputs) = node.get("inputs").and_then(Value::as_array) {
                pending.extend(inputs);
            }
        }

        let mut seen = BTreeSet::new();
        for metrics in &self.ann_metrics {
            let key = (metrics.id, metrics.rung, metrics.attempt);
            let fail = |message: &str| format!("ANN diagnostics {key:?}: {message}");
            if !seen.insert(key) {
                return Err(fail("duplicate node/rung/attempt"));
            }
            if !nearest.contains(&(metrics.id as u64)) {
                return Err(fail("does not name a nearest physical scan"));
            }
            if metrics.counts.is_empty() && metrics.cumulative_times_ns.is_empty() {
                return Err(fail(
                    "an unmeasured attempt must be absent, not an empty row",
                ));
            }
            for (values, allowed) in [
                (&metrics.counts, ANN_COUNTS),
                (&metrics.cumulative_times_ns, ANN_TIMES),
            ] {
                if let Some(name) = values.keys().find(|name| !allowed.contains(&name.as_str())) {
                    return Err(fail(&format!("unknown metric `{name}`")));
                }
            }
            let mut probes = self
                .search
                .iter()
                .filter_map(|decision| match &decision.decision {
                    SearchKind::Probes { attempts }
                        if decision.id == metrics.id && decision.rung == metrics.rung =>
                    {
                        Some(attempts)
                    }
                    _ => None,
                });
            let attempts = probes
                .next()
                .ok_or_else(|| fail("has no matching probe decision"))?;
            if probes.next().is_some() {
                return Err(fail("has more than one matching probe decision"));
            }
            if metrics.attempt >= attempts.len() {
                return Err(fail("attempt is outside the matching probe list"));
            }
        }

        let mut rendered = Vec::new();
        for batch in profile.batches() {
            let nodes = profile_strings(batch, "node")?;
            let trees = profile_strings(batch, "tree")?;
            let details = profile_strings(batch, "detail")?;
            let depth = batch
                .column_by_name("depth")
                .ok_or("ANN profile has no depth column")?;
            for row in 0..batch.num_rows() {
                if nodes.is_null(row) || nodes.value(row) != "ann_metrics" {
                    continue;
                }
                if trees.is_null(row) || trees.value(row) != "profile" || !depth.is_null(row) {
                    return Err("ANN profile row must have tree `profile` and null depth".into());
                }
                if details.is_null(row) {
                    return Err("ANN profile row has null detail".into());
                }
                rendered.push(
                    serde_json::from_str::<AnnMetrics>(details.value(row))
                        .map_err(|error| format!("ANN profile detail is malformed: {error}"))?,
                );
            }
        }
        if rendered != self.ann_metrics {
            return Err("ANN profile diagnostics differ from the same run's report".into());
        }
        Ok(())
    }
}

fn profile_strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, String> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| format!("ANN profile column `{name}` is missing or is not String"))
}

/// Read only the public fields needed by GQT. Other report and operator-row
/// fields remain additive; ANN rows use the bounded documented contract.
pub(crate) fn read_report(report: &impl Serialize) -> Result<Report, String> {
    serde_json::to_value(report)
        .and_then(serde_json::from_value)
        .map_err(|error| {
            format!("the execution report does not read as rows and diagnostics: {error}")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde_json::json;

    fn explain() -> Value {
        json!({"physical_plan": {"node": "Limit", "id": 9, "inputs": [
            {"node": "Scan", "id": 4, "ranked": {"kind": "nearest"}}
        ]}})
    }

    fn fixture() -> Value {
        json!({
            "rows": [{"id": 4, "attempts": [{"ran": true}]}],
            "search": [
                {"id":4,"rung":3,"decision":"gate","plan":"postfilter"},
                {"id":4,"rung":3,"decision":"probes","attempts":[
                    {"maximum_nprobes":1,"flat":false,"rows":0},
                    {"maximum_nprobes":null,"flat":true,"rows":3}
                ]}
            ],
            "ann_metrics": [{"id":4,"rung":3,"attempt":1,
                "counts":{"prefilter_loads":0},
                "cumulative_times_ns":{"index_search_time":0,"prefilter_load_time":u64::MAX}
            }]
        })
    }

    fn profile(rows: &[Value]) -> QueryResult {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tree", DataType::Utf8, false),
            Field::new("depth", DataType::Int64, true),
            Field::new("node", DataType::Utf8, false),
            Field::new("detail", DataType::Utf8, false),
        ]));
        let details: Vec<String> = rows.iter().map(Value::to_string).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["profile"; rows.len()])),
                Arc::new(Int64Array::from(vec![None; rows.len()])),
                Arc::new(StringArray::from(vec!["ann_metrics"; rows.len()])),
                Arc::new(StringArray::from(details)),
            ],
        )
        .unwrap();
        QueryResult::new(schema, vec![batch])
    }

    fn validate(value: &Value, explain: &Value) -> Result<(), String> {
        let rows = value["ann_metrics"].as_array().cloned().unwrap_or_default();
        read_report(value)?.validate_ann(explain, &profile(&rows))
    }

    #[test]
    fn ann_optional_sparse_and_zero_observations_are_valid() {
        let absent = json!({"rows": []});
        let report = read_report(&absent).unwrap();
        report.validate_ann(&json!({}), &profile(&[])).unwrap();
        assert!(!report.has_ann(4));

        let value = fixture();
        validate(&value, &explain()).unwrap();
        let report = read_report(&value).unwrap();
        assert!(report.has_ann(4));
        assert!(!report.has_ann(9));
        assert_eq!(
            report.ann_metrics[0].attempt, 1,
            "the unmeasured first attempt stays absent"
        );
        assert_eq!(
            report.ann_metrics[0].cumulative_times_ns["index_search_time"],
            0
        );
        assert_eq!(
            report.ann_metrics[0].cumulative_times_ns["prefilter_load_time"],
            u64::MAX
        );

        let mut two_arms = value.clone();
        let mut second_probe = value["search"][1].clone();
        second_probe["id"] = json!(7);
        two_arms["search"]
            .as_array_mut()
            .unwrap()
            .push(second_probe);
        let mut second_metrics = value["ann_metrics"][0].clone();
        second_metrics["id"] = json!(7);
        two_arms["ann_metrics"]
            .as_array_mut()
            .unwrap()
            .push(second_metrics);
        let mut plan = explain();
        plan["physical_plan"]["inputs"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "node":"Scan", "id":7, "ranked":{"kind":"nearest"}
            }));
        validate(&two_arms, &plan).unwrap();
        assert!(read_report(&two_arms).unwrap().has_ann(7));
    }

    #[test]
    fn ann_numeric_shape_is_not_coerced() {
        for invalid in [json!(-1), json!(0.5), json!("1"), Value::Null] {
            for field in ["id", "rung", "attempt"] {
                let mut value = fixture();
                value["ann_metrics"][0][field] = invalid.clone();
                assert!(read_report(&value).is_err(), "{field}: {invalid}");
            }
            for map in ["counts", "cumulative_times_ns"] {
                let mut value = fixture();
                let key = if map == "counts" {
                    "prefilter_loads"
                } else {
                    "index_search_time"
                };
                value["ann_metrics"][0][map][key] = invalid.clone();
                assert!(read_report(&value).is_err(), "{map}: {invalid}");
            }
        }
        let mut value = fixture();
        value["search"][1]["attempts"][0]["rows"] = json!(-1);
        assert!(
            read_report(&value).is_err(),
            "probe observations are typed too"
        );
    }

    #[test]
    fn ann_unknown_keys_and_empty_observations_are_refused() {
        for map in ["counts", "cumulative_times_ns"] {
            let mut value = fixture();
            value["ann_metrics"][0][map]["future_native_metric"] = json!(0);
            assert!(
                validate(&value, &explain())
                    .unwrap_err()
                    .contains("unknown metric")
            );
        }
        let mut value = fixture();
        value["ann_metrics"][0]["extra"] = json!(0);
        assert!(read_report(&value).is_err());
        value["ann_metrics"][0]
            .as_object_mut()
            .unwrap()
            .remove("extra");
        value["ann_metrics"][0]["counts"] = json!({});
        value["ann_metrics"][0]["cumulative_times_ns"] = json!({});
        assert!(
            validate(&value, &explain())
                .unwrap_err()
                .contains("empty row")
        );
    }

    #[test]
    fn ann_identity_must_name_one_real_probe_attempt() {
        let base = fixture();
        let mut duplicate = base.clone();
        duplicate["ann_metrics"]
            .as_array_mut()
            .unwrap()
            .push(base["ann_metrics"][0].clone());
        assert!(
            validate(&duplicate, &explain())
                .unwrap_err()
                .contains("duplicate node/rung/attempt")
        );

        for (field, invalid, message) in [
            ("id", 9, "nearest physical scan"),
            ("rung", 1, "no matching probe decision"),
            ("attempt", 2, "outside the matching probe list"),
        ] {
            let mut value = base.clone();
            value["ann_metrics"][0][field] = json!(invalid);
            assert!(validate(&value, &explain()).unwrap_err().contains(message));
        }
        let mut no_probes = base.clone();
        no_probes["search"].as_array_mut().unwrap().remove(1);
        assert!(
            validate(&no_probes, &explain())
                .unwrap_err()
                .contains("no matching probe decision")
        );
        let mut repeated_probes = base.clone();
        repeated_probes["search"]
            .as_array_mut()
            .unwrap()
            .push(base["search"][1].clone());
        assert!(
            validate(&repeated_probes, &explain())
                .unwrap_err()
                .contains("more than one matching probe decision")
        );

        let mut wrong_kind = explain();
        wrong_kind["physical_plan"]["inputs"][0]["ranked"]["kind"] = json!("bm25");
        assert!(
            validate(&base, &wrong_kind)
                .unwrap_err()
                .contains("nearest physical scan")
        );
        let logical_only = json!({"logical_plan":explain()["physical_plan"]});
        assert!(
            validate(&base, &logical_only)
                .unwrap_err()
                .contains("nearest physical scan")
        );
    }

    #[test]
    fn ann_profile_must_preserve_the_same_observations() {
        let value = fixture();
        let report = read_report(&value).unwrap();
        let row = value["ann_metrics"][0].clone();
        let mut changed = row.clone();
        changed["cumulative_times_ns"]["index_search_time"] = json!(1);
        for rows in [vec![], vec![changed], vec![row.clone(), row.clone()]] {
            assert!(
                report
                    .validate_ann(&explain(), &profile(&rows))
                    .unwrap_err()
                    .contains("differ from the same run")
            );
        }
        let mut malformed = row.clone();
        malformed["counts"]["prefilter_loads"] = json!("0");
        assert!(
            report
                .validate_ann(&explain(), &profile(&[malformed]))
                .unwrap_err()
                .contains("profile detail is malformed")
        );
        let absent = read_report(&json!({"rows":[]})).unwrap();
        assert!(
            absent
                .validate_ann(&explain(), &profile(&[row]))
                .unwrap_err()
                .contains("differ from the same run")
        );
    }
}
