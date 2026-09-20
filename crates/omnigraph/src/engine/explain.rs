//! The `explain` answer: the planner's document for a read query, and its
//! result form (one entry per node of the logical, physical and lowered DataFusion
//! trees, then one per pass and per other field).

use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan};
use omnigraph_planner::PhysicalPlan;

use super::plan_source::explain_query;
use super::*;

/// The planner's document for `ir` under `traversal`, the mode of the
/// settings the caller's query doors run the same source under.
pub(crate) async fn explain_document(
    ir: &QueryIR,
    params: &ParamMap,
    catalog: &Catalog,
    snapshot: &Snapshot,
    traversal: Traversal,
) -> Result<serde_json::Value> {
    let params = resolve_params(ir, params)?;
    Ok(explain_query(ir, &params, catalog, snapshot, traversal)
        .await?
        .explain
        .to_value())
}

/// The `tree` value of the lowered DataFusion plan's rows: one per operator
/// of `displayable(root).indent(true)`, `depth` from the line's indent.
const EXPLAIN_DATAFUSION: &str = "datafusion";

/// The `tree` value of the rows that are not plan nodes: one per pass, then
/// one per remaining top-level field of the document.
const EXPLAIN_PLAN_ROWS: &str = "plan";

/// One operator of the lowered plan: its depth and its own `Verbose` text.
struct OperatorLine {
    depth: i64,
    text: String,
}

struct OperatorText<'a>(&'a dyn ExecutionPlan);

impl std::fmt::Display for OperatorText<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt_as(DisplayFormatType::Verbose, f)
    }
}

fn operator_lines(node: &dyn ExecutionPlan, depth: i64, out: &mut Vec<OperatorLine>) {
    out.push(OperatorLine {
        depth,
        text: OperatorText(node).to_string(),
    });
    for child in node.children() {
        operator_lines(child.as_ref(), depth + 1, out);
    }
}

enum LoweredTree {
    Operators(Vec<OperatorLine>),
    /// Resolving the search mode attempted to use the embedding client.
    Unavailable(String),
}

/// The lowered DataFusion tree of the query's first pass in pre-order. An
/// explain cannot embed query text, so that search mode is `Unavailable`;
/// other failures propagate. Planning may already have read dataset metadata.
async fn lowered_tree(
    plan: &PhysicalPlan,
    ir: &QueryIR,
    params: &ResolvedParams,
    snapshot: &Snapshot,
    catalog: &Arc<Catalog>,
    settings: &SessionSettings,
) -> Result<LoweredTree> {
    let embedding = EmbeddingResolver::explain();
    let mode = match extract_search_mode(ir, params.shared(), catalog, &embedding, settings).await {
        Err(_) if embedding.was_requested() => {
            return Ok(LoweredTree::Unavailable(
                "the nearest() query is a string; embedding it needs the embedding client"
                    .to_string(),
            ));
        }
        mode => mode?,
    };
    let graph_index = Arc::new(GraphIndexHandle::none());
    let lowering = Lowering {
        plan,
        ir,
        params,
        snapshot,
        graph_index: &graph_index,
        catalog,
        settings,
        memory_limit: super::context::query_memory_limit(),
    };
    let run = match &mode.rrf {
        Some(rrf) => RunMode::Fused {
            rrf,
            primary: &rrf.primary,
            secondary: &rrf.secondary,
        },
        None => RunMode::Single(&mode),
    };
    let lowered = lowering.lower_query(&run)?;
    let mut operators = Vec::new();
    operator_lines(lowered.root.as_ref(), 0, &mut operators);
    Ok(LoweredTree::Operators(operators))
}

#[derive(Default)]
struct ExplainRows {
    tree: Vec<&'static str>,
    depth: Vec<Option<i64>>,
    node: Vec<String>,
    detail: Vec<String>,
}

impl ExplainRows {
    fn push(&mut self, tree: &'static str, depth: Option<i64>, node: &str, detail: String) {
        self.tree.push(tree);
        self.depth.push(depth);
        self.node.push(node.to_string());
        self.detail.push(detail);
    }

    fn push_field(&mut self, name: &str, value: &impl serde::Serialize) -> Result<()> {
        let value = serde_json::to_value(value).map_err(|error| {
            OmniError::manifest_internal(format!(
                "explain field '{name}' cannot serialize: {error}"
            ))
        })?;
        self.push(EXPLAIN_PLAN_ROWS, None, name, explain_detail(&value));
        Ok(())
    }

    /// One row per node of `node`'s subtree in pre-order, `depth` counting from
    /// the root, `detail` the node's own fields without its `inputs`.
    fn push_tree(&mut self, tree: &'static str, node: &serde_json::Value, depth: i64) {
        let Some(fields) = node.as_object() else {
            return;
        };
        let kind = fields
            .get("node")
            .and_then(|kind| kind.as_str())
            .unwrap_or("?");
        let own: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .filter(|(key, _)| *key != "node" && *key != "inputs")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        self.push(
            tree,
            Some(depth),
            kind,
            serde_json::Value::Object(own).to_string(),
        );
        for child in fields
            .get("inputs")
            .and_then(|inputs| inputs.as_array())
            .into_iter()
            .flatten()
        {
            self.push_tree(tree, child, depth + 1);
        }
    }

    /// One `datafusion` row per operator: `node` the operator name before the
    /// first `:` or `(`, `detail` the rest of its text, a line break inside
    /// it (a GQ string literal may hold one) written as `\n` or `\r`.
    fn push_datafusion(&mut self, operators: &[OperatorLine]) {
        for operator in operators {
            let text = operator.text.replace('\n', "\\n").replace('\r', "\\r");
            let text = text.trim();
            let (node, detail) = match text.find([':', '(']) {
                Some(at) if text.as_bytes()[at] == b':' => {
                    (&text[..at], text[at + 1..].trim_start())
                }
                Some(at) => (&text[..at], &text[at..]),
                None => (text, ""),
            };
            self.push(
                EXPLAIN_DATAFUSION,
                Some(operator.depth),
                node.trim_end(),
                detail.to_string(),
            );
        }
    }

    fn into_result(self) -> Result<QueryResult> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tree", DataType::Utf8, false),
            Field::new("depth", DataType::Int64, true),
            Field::new("node", DataType::Utf8, false),
            Field::new("detail", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(self.tree)),
                Arc::new(Int64Array::from(self.depth)),
                Arc::new(StringArray::from(self.node)),
                Arc::new(StringArray::from(self.detail)),
            ],
        )
        .map_err(OmniError::arrow_internal)?;
        Ok(QueryResult::new(schema, vec![batch]))
    }
}

/// A `detail` cell: a string bare, anything else as its JSON text.
fn explain_detail(value: &serde_json::Value) -> String {
    match value.as_str() {
        Some(text) => text.to_string(),
        None => value.to_string(),
    }
}

/// The rows an `explain` statement answers: the logical, the physical and
/// the lowered `datafusion` tree, one row per node in pre-order with its
/// `depth` (which rebuilds each tree), then `plan` rows: `datafusion` with
/// the reason when that tree is unavailable, one per fired pass, one per
/// other field of the document.
pub(crate) async fn explain_rows(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Arc<Catalog>,
    settings: &SessionSettings,
) -> Result<QueryResult> {
    let params = resolve_params(ir, params)?;
    let planned = explain_query(ir, &params, catalog, snapshot, settings.traversal()).await?;
    let lowered = lowered_tree(&planned.physical, ir, &params, snapshot, catalog, settings).await?;
    let omnigraph_planner::explain::Explain {
        explain_version,
        route,
        override_,
        reason,
        entry,
        operation,
        logical_plan,
        logical_hash,
        physical_plan,
        pipelines,
        statistics,
        passes,
    } = &planned.explain;
    let mut rows = ExplainRows::default();
    if let Some(root) = logical_plan {
        rows.push_tree("logical", root, 0);
    }
    if let Some(root) = physical_plan {
        rows.push_tree("physical", root, 0);
    }
    match &lowered {
        LoweredTree::Operators(operators) => rows.push_datafusion(operators),
        LoweredTree::Unavailable(reason) => rows.push(
            EXPLAIN_PLAN_ROWS,
            None,
            EXPLAIN_DATAFUSION,
            format!("unavailable: {reason}"),
        ),
    }
    for pass in passes {
        rows.push(EXPLAIN_PLAN_ROWS, None, "pass", (*pass).to_string());
    }
    rows.push_field("route", route)?;
    rows.push_field("override", override_)?;
    if let Some(reason) = reason {
        rows.push_field("reason", reason)?;
    }
    if let Some(entry) = entry {
        rows.push_field("entry", entry)?;
    }
    rows.push_field("logical_hash", logical_hash)?;
    rows.push_field("explain_version", explain_version)?;
    rows.push_field("operation", operation)?;
    if let Some(pipelines) = pipelines {
        rows.push_field("pipelines", pipelines)?;
    }
    if let Some(statistics) = statistics {
        rows.push_field("statistics", statistics)?;
    }
    rows.into_result()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exercises the capability directly; GQT cannot observe client initialization.
    #[tokio::test]
    async fn explain_resolver_refuses_client_initialization() {
        let resolver = EmbeddingResolver::explain();
        assert!(!resolver.was_requested());
        assert!(resolver.resolve().await.is_err());
        assert!(resolver.was_requested());
    }
}
