//! The `--- expect plan` section after a `load csv` step: assertions over the
//! planner's document the load's receipt carries and what the load executed
//! (RFC 2026-10-15, bulk load as a query plan). Each line names one fact;
//! unmentioned facts are unchecked, as for a query plan.
//!
//! ```text
//! load route: plan
//! load scan: partitions 1 fallback single_scan
//! load validate value node:Person [email]
//! load validate unique node:Person key [id]: access delta
//! load validate unique node:Person unique [email]: access scan
//! load validate edge_ri edge:Knows from Person: access probe
//! load validate cardinality edge:Knows from Person
//! load validate retained_edges node:Person edge:Knows from: access probe
//! load write: overwrite fragments
//! load pipeline 0: CsvSource => Validate
//! load pipeline 1: Validate -> FragmentWrite => consumer after 0
//! load executed: partitions 1 rows 3
//! ```
//!
//! `route: plan` is the document's `engine` route, `pipeline` its `executor`
//! route; the validate lines are the one `Validate` node's checks in order;
//! a `pipeline` line is one entry of the document's `pipelines`, its source,
//! its operators, the sink it drains into, its partitions when above one and
//! the pipelines drained before it;
//! publication is the driver's epilogue, never a plan node, so no line names it.

use omnigraph::loader::LoadExecuted;
use serde::Serialize;
use serde_json::Value;

const FORMS: &str = "forms: `load route: <plan|pipeline>`, `load scan: partitions <n> fallback single_scan`, `load validate value <table> [a, b]`, `load validate unique <table> <key|unique> [a, b]: access <delta|probe|scan>`, `load validate edge_ri <edge> <from|to> <Node>: access <delta|probe|scan>`, `load validate cardinality <edge> <from|to> <Node>`, `load validate retained_edges <node> <edge> <from|to>: access <delta|probe|scan>`, `load write: <overwrite fragments|append strict_insert|merge upsert>`, `load pipeline <n>: <Source>[ -> <Operator>]* => <sink>[ partitions <n>][ after <n>[, <m>]]`, `load executed: partitions <n> rows <m>`, `load executed <Node>: rows <m>`";

const ACCESS: [&str; 3] = ["delta", "probe", "scan"];
const ENDPOINTS: [&str; 2] = ["from", "to"];
const ROUTES: [&str; 2] = ["plan", "pipeline"];
const WRITES: [&str; 3] = [
    "overwrite fragments",
    "append strict_insert",
    "merge upsert",
];

/// One fact of a load plan, in the spelling the section uses; the receipt's
/// document renders to the same spellings, so a fact holds when its spelling
/// is among the document's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LoadFact {
    Route(String),
    Scan {
        partitions: u64,
        fallback: String,
    },
    Value {
        table: String,
        columns: Vec<String>,
    },
    Unique {
        table: String,
        role: String,
        columns: Vec<String>,
        access: String,
    },
    EdgeRi {
        edge: String,
        endpoint: String,
        node: String,
        access: String,
    },
    Cardinality {
        edge: String,
        endpoint: String,
        node: String,
    },
    RetainedEdges {
        node: String,
        edge: String,
        endpoint: String,
        access: String,
    },
    Write(String),
    Pipeline(PipelineClaim),
    Executed {
        partitions: u32,
        rows: u64,
    },
    /// The rows one node's operator emitted, from `executed.operators`.
    OperatorRows {
        node: String,
        rows: u64,
    },
}

/// One entry of a planner document's `pipelines`, spelled
/// `<n>: <Source>[ -> <Operator>]* => <sink>[ partitions <n>][ after <n>[, <m>]]`:
/// the pipeline's position, its source node, its operators in run order, the
/// sink it drains into, the partitions its source runs (spelled only above
/// one) and the pipelines drained before it. A load section claims one as
/// `load pipeline …`, a query section as `pipeline …`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PipelineClaim {
    pub id: u64,
    pub source: String,
    pub operators: Vec<String>,
    pub sink: String,
    pub partitions: u64,
    pub after: Vec<u64>,
}

impl PipelineClaim {
    pub(crate) fn spell(&self) -> String {
        let mut line = format!("{}: {}", self.id, self.source);
        for operator in &self.operators {
            line.push_str(" -> ");
            line.push_str(operator);
        }
        line.push_str(" => ");
        line.push_str(&self.sink);
        if self.partitions > 1 {
            line.push_str(" partitions ");
            line.push_str(&self.partitions.to_string());
        }
        if !self.after.is_empty() {
            let after: Vec<String> = self.after.iter().map(u64::to_string).collect();
            line.push_str(" after ");
            line.push_str(&after.join(", "));
        }
        line
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        let (id, rest) = text.split_once(':')?;
        let id = id.trim().parse().ok()?;
        let (chain, sink) = rest.split_once(" => ")?;
        let (sink, after) = match sink.split_once(" after ") {
            Some((sink, after)) => (
                sink,
                after
                    .split(',')
                    .map(|n| n.trim().parse().ok())
                    .collect::<Option<Vec<u64>>>()?,
            ),
            None => (sink, Vec::new()),
        };
        let (sink, partitions) = match sink.split_once(" partitions ") {
            Some((sink, partitions)) => (sink, partitions.trim().parse().ok().filter(|n| *n >= 1)?),
            None => (sink, 1),
        };
        let mut names = chain.split(" -> ").map(operator_name);
        let source = names.next()??;
        let operators = names.collect::<Option<Vec<String>>>()?;
        Some(Self {
            id,
            source,
            operators,
            sink: operator_name(sink)?,
            partitions,
            after,
        })
    }

    /// Every entry of `document["pipelines"]`, in document order.
    pub(crate) fn of_document(document: &Value) -> Vec<Self> {
        let text = |value: &Value| value.as_str().unwrap_or_default().to_string();
        document["pipelines"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|pipeline| Self {
                id: pipeline["id"].as_u64().unwrap_or_default(),
                source: text(&pipeline["source"]["node"]),
                operators: pipeline["operators"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|operator| text(&operator["name"]))
                    .collect(),
                sink: text(&pipeline["sink"]["name"]),
                partitions: pipeline["partitions"].as_u64().unwrap_or(1),
                after: pipeline["depends_on"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_u64)
                    .collect(),
            })
            .collect()
    }
}

impl LoadFact {
    pub(crate) fn spell(&self) -> String {
        match self {
            LoadFact::Route(route) => format!("load route: {route}"),
            LoadFact::Scan {
                partitions,
                fallback,
            } => format!("load scan: partitions {partitions} fallback {fallback}"),
            LoadFact::Value { table, columns } => {
                format!("load validate value {table} {}", spell_columns(columns))
            }
            LoadFact::Unique {
                table,
                role,
                columns,
                access,
            } => format!(
                "load validate unique {table} {role} {}: access {access}",
                spell_columns(columns)
            ),
            LoadFact::EdgeRi {
                edge,
                endpoint,
                node,
                access,
            } => format!("load validate edge_ri {edge} {endpoint} {node}: access {access}"),
            LoadFact::Cardinality {
                edge,
                endpoint,
                node,
            } => format!("load validate cardinality {edge} {endpoint} {node}"),
            LoadFact::RetainedEdges {
                node,
                edge,
                endpoint,
                access,
            } => format!("load validate retained_edges {node} {edge} {endpoint}: access {access}"),
            LoadFact::Write(write) => format!("load write: {write}"),
            LoadFact::Pipeline(claim) => format!("load pipeline {}", claim.spell()),
            LoadFact::Executed { partitions, rows } => {
                format!("load executed: partitions {partitions} rows {rows}")
            }
            LoadFact::OperatorRows { node, rows } => format!("load executed {node}: rows {rows}"),
        }
    }
}

fn spell_columns(columns: &[String]) -> String {
    format!("[{}]", columns.join(", "))
}

#[derive(Debug)]
pub struct LoadPlanExpect {
    pub(crate) lines: Vec<LoadFact>,
}

pub(crate) fn parse_load_plan_body(body: &[(usize, &str)]) -> Result<Vec<LoadFact>, String> {
    let mut facts = Vec::new();
    for (index, raw) in body {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fact = parse_load_fact(line).ok_or_else(|| {
            format!(
                "line {}: `--- expect plan` has no load form for `{line}` ({FORMS})",
                index + 1
            )
        })?;
        facts.push(fact);
    }
    if facts.is_empty() {
        return Err(format!("`--- expect plan` names no fact ({FORMS})"));
    }
    Ok(facts)
}

fn parse_load_fact(line: &str) -> Option<LoadFact> {
    let rest = line.strip_prefix("load ")?.trim();
    if let Some(route) = rest.strip_prefix("route:") {
        let route = route.trim();
        return ROUTES
            .contains(&route)
            .then(|| LoadFact::Route(route.to_string()));
    }
    if let Some(scan) = rest.strip_prefix("scan:") {
        let words: Vec<&str> = scan.split_whitespace().collect();
        let ["partitions", partitions, "fallback", "single_scan"] = words.as_slice() else {
            return None;
        };
        return Some(LoadFact::Scan {
            partitions: partitions.parse().ok()?,
            fallback: "single_scan".to_string(),
        });
    }
    if let Some(write) = rest.strip_prefix("write:") {
        let write = write.split_whitespace().collect::<Vec<_>>().join(" ");
        return WRITES
            .contains(&write.as_str())
            .then_some(LoadFact::Write(write));
    }
    if let Some(executed) = rest.strip_prefix("executed:") {
        let words: Vec<&str> = executed.split_whitespace().collect();
        let ["partitions", partitions, "rows", rows] = words.as_slice() else {
            return None;
        };
        return Some(LoadFact::Executed {
            partitions: partitions.parse().ok()?,
            rows: rows.parse().ok()?,
        });
    }
    if let Some(executed) = rest.strip_prefix("executed ") {
        let (node, rows) = executed.split_once(':')?;
        let node = operator_name(node)?;
        let words: Vec<&str> = rows.split_whitespace().collect();
        let ["rows", rows] = words.as_slice() else {
            return None;
        };
        return Some(LoadFact::OperatorRows {
            node,
            rows: rows.parse().ok()?,
        });
    }
    if let Some(pipeline) = rest.strip_prefix("pipeline ") {
        return PipelineClaim::parse(pipeline).map(LoadFact::Pipeline);
    }
    let validate = rest.strip_prefix("validate ")?;
    parse_validate_fact(validate)
}

/// An operator spelling of the explain, `Validate`, `HashJoin(probe)`,
/// `consumer`: letters, digits, `_`, `(` and `)`, no spaces.
fn operator_name(text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '(' | ')')))
    .then(|| text.to_string())
}

fn parse_validate_fact(text: &str) -> Option<LoadFact> {
    if let Some(value) = text.strip_prefix("value ") {
        let (table, columns) = value.trim().split_once(' ')?;
        return Some(LoadFact::Value {
            table: table_key(table)?,
            columns: columns_list(columns)?,
        });
    }
    if let Some(unique) = text.strip_prefix("unique ") {
        let (identity, access) = unique.rsplit_once(": access ")?;
        let access = access_word(access)?;
        let words: Vec<&str> = identity.splitn(3, ' ').collect();
        let [table, role, columns] = words.as_slice() else {
            return None;
        };
        if !["key", "unique"].contains(role) {
            return None;
        }
        return Some(LoadFact::Unique {
            table: table_key(table)?,
            role: role.to_string(),
            columns: columns_list(columns)?,
            access,
        });
    }
    if let Some(edge_ri) = text.strip_prefix("edge_ri ") {
        let (identity, access) = edge_ri.rsplit_once(": access ")?;
        let words: Vec<&str> = identity.split_whitespace().collect();
        let [edge, endpoint, node] = words.as_slice() else {
            return None;
        };
        return Some(LoadFact::EdgeRi {
            edge: table_key(edge)?,
            endpoint: endpoint_word(endpoint)?,
            node: type_name(node)?,
            access: access_word(access)?,
        });
    }
    if let Some(cardinality) = text.strip_prefix("cardinality ") {
        let words: Vec<&str> = cardinality.split_whitespace().collect();
        let [edge, endpoint, node] = words.as_slice() else {
            return None;
        };
        return Some(LoadFact::Cardinality {
            edge: table_key(edge)?,
            endpoint: endpoint_word(endpoint)?,
            node: type_name(node)?,
        });
    }
    if let Some(retained) = text.strip_prefix("retained_edges ") {
        let (identity, access) = retained.rsplit_once(": access ")?;
        let words: Vec<&str> = identity.split_whitespace().collect();
        let [node, edge, endpoint] = words.as_slice() else {
            return None;
        };
        return Some(LoadFact::RetainedEdges {
            node: table_key(node)?,
            edge: table_key(edge)?,
            endpoint: endpoint_word(endpoint)?,
            access: access_word(access)?,
        });
    }
    None
}

/// A qualified table key, `node:<Type>` or `edge:<Type>`.
fn table_key(text: &str) -> Option<String> {
    let (kind, name) = text.split_once(':')?;
    (["node", "edge"].contains(&kind) && type_name(name).is_some()).then(|| text.to_string())
}

fn type_name(text: &str) -> Option<String> {
    let mut chars = text.chars();
    let first = chars.next()?;
    (first.is_ascii_alphabetic() && chars.all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .then(|| text.to_string())
}

fn access_word(text: &str) -> Option<String> {
    let text = text.trim();
    ACCESS.contains(&text).then(|| text.to_string())
}

fn endpoint_word(text: &str) -> Option<String> {
    ENDPOINTS.contains(&text).then(|| text.to_string())
}

fn columns_list(text: &str) -> Option<Vec<String>> {
    let inner = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    let columns: Vec<String> = inner
        .split(',')
        .map(|part| {
            let part = part.trim();
            (!part.is_empty()).then(|| part.to_string())
        })
        .collect::<Option<_>>()?;
    (!columns.is_empty()).then_some(columns)
}

/// Every fact of an executed load's planner document, in the section's
/// spellings: the route, then the physical tree from its root down.
pub(crate) fn plan_facts(document: &Value, executed: &LoadExecuted) -> Vec<LoadFact> {
    let mut facts = Vec::new();
    match document["route"].as_str() {
        Some("engine") => facts.push(LoadFact::Route("plan".to_string())),
        Some("executor") => facts.push(LoadFact::Route("pipeline".to_string())),
        _ => {}
    }
    let mut node = &document["physical_plan"];
    while node.is_object() {
        node_facts(node, &mut facts);
        node = &node["inputs"][0];
    }
    facts.extend(
        PipelineClaim::of_document(document)
            .into_iter()
            .map(LoadFact::Pipeline),
    );
    facts.push(LoadFact::Executed {
        partitions: executed.partitions,
        rows: executed.rows,
    });
    facts.extend(
        executed
            .operators
            .iter()
            .map(|operator| LoadFact::OperatorRows {
                node: operator.node.clone(),
                rows: operator.rows,
            }),
    );
    facts
}

fn node_facts(node: &Value, facts: &mut Vec<LoadFact>) {
    let text = |value: &Value| value.as_str().unwrap_or_default().to_string();
    match node["node"].as_str() {
        Some("CsvSource") => facts.push(LoadFact::Scan {
            partitions: node["max_partitions"].as_u64().unwrap_or_default(),
            fallback: text(&node["fallback"]),
        }),
        Some("Validate") => {
            let table = text(&node["table"]);
            for check in node["checks"].as_array().into_iter().flatten() {
                let columns = || -> Vec<String> {
                    check["columns"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(text)
                        .collect()
                };
                let access = text(&check["access"]);
                let fact = match check["check"].as_str() {
                    Some("value") => LoadFact::Value {
                        table: table.clone(),
                        columns: columns(),
                    },
                    Some("unique") => LoadFact::Unique {
                        table: table.clone(),
                        role: text(&check["role"]),
                        columns: columns(),
                        access,
                    },
                    Some("edge_ri") => LoadFact::EdgeRi {
                        edge: table.clone(),
                        endpoint: text(&check["endpoint"]),
                        node: text(&check["node_type"]),
                        access,
                    },
                    Some("cardinality") => LoadFact::Cardinality {
                        edge: table.clone(),
                        endpoint: text(&check["endpoint"]),
                        node: text(&check["node_type"]),
                    },
                    Some("retained_edges") => LoadFact::RetainedEdges {
                        node: table.clone(),
                        edge: text(&check["edge_table"]),
                        endpoint: text(&check["endpoint"]),
                        access,
                    },
                    _ => continue,
                };
                facts.push(fact);
            }
        }
        Some("FragmentWrite") => facts.push(LoadFact::Write(format!(
            "{} {}",
            text(&node["mode"]),
            text(&node["adapter"])
        ))),
        _ => {}
    }
}

/// The first expected fact the executed plan does not hold, with every fact
/// the plan does hold, or `None` when all hold.
pub(crate) fn load_plan_mismatch(
    expected: &[LoadFact],
    document: &Value,
    executed: &LoadExecuted,
) -> Option<String> {
    let actual: Vec<String> = plan_facts(document, executed)
        .iter()
        .map(LoadFact::spell)
        .collect();
    let missing = expected
        .iter()
        .map(LoadFact::spell)
        .find(|line| !actual.contains(line))?;
    let mut message = format!("load plan does not hold `{missing}`; the executed plan's facts:");
    for line in &actual {
        message.push_str("\n  ");
        message.push_str(line);
    }
    Some(message)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn every_documented_form_parses_and_spells_itself_back() {
        let lines = [
            "load route: plan",
            "load scan: partitions 1 fallback single_scan",
            "load validate value node:Person [email]",
            "load validate unique node:Person key [id]: access delta",
            "load validate unique node:Person unique [email, name]: access scan",
            "load validate edge_ri edge:Knows from Person: access probe",
            "load validate cardinality edge:Knows from Person",
            "load validate retained_edges node:Person edge:Knows to: access probe",
            "load write: overwrite fragments",
            "load write: append strict_insert",
            "load pipeline 0: CsvSource => Validate",
            "load pipeline 1: Validate -> FragmentWrite => consumer after 0",
            "load pipeline 2: Scan -> HashJoin(probe) -> Projection => consumer after 0, 1",
            "load pipeline 0: CsvSource => Validate partitions 2",
            "load pipeline 1: Validate -> FragmentWrite => consumer partitions 3 after 0",
            "load executed: partitions 1 rows 3",
            "load executed FragmentWrite: rows 3",
        ];
        for line in lines {
            let fact = parse_load_fact(line).unwrap_or_else(|| panic!("{line}"));
            assert_eq!(fact.spell(), line);
        }
    }

    #[test]
    fn a_line_outside_the_forms_is_refused() {
        for line in [
            "route: plan",
            "load route: fast",
            "load scan: partitions one fallback single_scan",
            "load scan: partitions 1 fallback none",
            "load validate unique Person key [id]: access delta",
            "load validate unique node:Person key [id]",
            "load validate unique node:Person primary [id]: access delta",
            "load validate unique node:Person key []: access delta",
            "load validate edge_ri edge:Knows via Person: access probe",
            "load validate edge_ri edge:Knows from Person: access index",
            "load validate retained_edges node:Person edge:Knows",
            "load validate retained_edges node:Person edge:Knows from",
            "load validate retained_edges node:Person edge:Knows from: access index",
            "load write: overwrite batches",
            "load publish: 1",
            "load executed: partitions 1",
            "load partitions: 1",
            "load pipeline 0: CsvSource",
            "load pipeline 0: CsvSource -> => Validate",
            "load pipeline 0: Csv Source => Validate",
            "load pipeline one: CsvSource => Validate",
            "load pipeline 1: Validate => consumer after one",
            "load pipeline 0: CsvSource => Validate partitions two",
            "load pipeline 0: CsvSource => Validate partitions 0",
            "load pipeline 1: Validate => consumer after 0 partitions 2",
            "load executed CsvSource rows 3",
            "load executed CsvSource: rows three",
            "load executed Csv Source: rows 3",
        ] {
            assert!(parse_load_fact(line).is_none(), "{line}");
        }
        assert!(parse_load_plan_body(&[(0, "# only a comment")]).is_err());
    }

    /// The planner document of an edge overwrite, as the explain JSON spells
    /// it, read back into every fact in tree order.
    #[test]
    fn the_planner_document_spells_every_fact() {
        let document = json!({
            "route": "engine",
            "physical_plan": {
                "node": "FragmentWrite", "table": "edge:Knows", "mode": "overwrite", "adapter": "fragments",
                "inputs": [{
                    "node": "Validate", "table": "edge:Knows",
                    "checks": [
                        {"check": "value", "columns": ["w"], "consults": null, "access": "delta"},
                        {"check": "unique", "role": "key", "columns": ["from", "to"], "access": "delta"},
                        {"check": "edge_ri", "endpoint": "from", "node_type": "Person", "access": "scan"},
                        {"check": "cardinality", "endpoint": "from", "node_type": "Person", "min": 0, "max": 2, "access": "probe"}
                    ],
                    "inputs": [{"node": "CsvSource", "max_partitions": 2, "fallback": "single_scan"}]
                }]
            },
            "pipelines": [
                {"id": 0, "source": {"id": 0, "node": "CsvSource"}, "partitions": 2, "operators": [], "sink": {"id": 1, "name": "Validate"}, "depends_on": []},
                {"id": 1, "source": {"id": 1, "node": "Validate"}, "partitions": 1, "operators": [{"id": 2, "name": "FragmentWrite"}], "sink": {"name": "consumer"}, "depends_on": [0]}
            ]
        });
        let executed = LoadExecuted {
            partitions: 1,
            rows: 7,
            source_bytes: 90,
            operators: vec![omnigraph::loader::LoadOperatorExecuted {
                id: 0,
                node: "CsvSource".to_string(),
                operator: "CsvSourceExec".to_string(),
                rows: 7,
                elapsed_compute_ns: 12,
                wall_ns: 34,
                partitions: 1,
                slices: Vec::new(),
            }],
        };
        let facts: Vec<String> = plan_facts(&document, &executed)
            .iter()
            .map(LoadFact::spell)
            .collect();
        assert_eq!(
            facts,
            [
                "load route: plan",
                "load write: overwrite fragments",
                "load validate value edge:Knows [w]",
                "load validate unique edge:Knows key [from, to]: access delta",
                "load validate edge_ri edge:Knows from Person: access scan",
                "load validate cardinality edge:Knows from Person",
                "load scan: partitions 2 fallback single_scan",
                "load pipeline 0: CsvSource => Validate partitions 2",
                "load pipeline 1: Validate -> FragmentWrite => consumer after 0",
                "load executed: partitions 1 rows 7",
                "load executed CsvSource: rows 7",
            ]
        );
        assert!(
            load_plan_mismatch(&[LoadFact::Route("pipeline".into())], &document, &executed)
                .is_some()
        );
    }
}
