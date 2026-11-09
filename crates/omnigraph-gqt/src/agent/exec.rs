//! Running queries on a dataset's seed: the edition-2 ground truths with the
//! client glue between their steps, and the agent's edition-2 answers.

use std::collections::BTreeMap;

use omnigraph::Session;
use omnigraph::db::ReadTarget;
use omnigraph_compiler::query::ast::QueryDecl;
use omnigraph_compiler::{JsonParamMode, QueryResult, json_params_to_param_map};
use omnigraph_gqt_core::{canonical_json, parse_case};
use serde_json::{Value, json};

use super::task::{Binding, Combine, Dataset, Step, Task};

/// Rows by position: the columns a read returns, then one array per row in
/// column order, a null where the engine omits a null cell. Positional rows
/// let an answer that names a column differently still be compared on its
/// values; `columns` is reported beside them.
#[derive(Debug, Clone, PartialEq)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl Rows {
    fn from_result(result: &QueryResult) -> Result<Self, String> {
        let columns: Vec<String> = result
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let Value::Array(objects) = result
            .to_rust_json()
            .map_err(|e| format!("rows did not render as JSON: {e}"))?
        else {
            return Err("the engine returned a non-array row set".into());
        };
        let rows = objects
            .iter()
            .map(|object| {
                columns
                    .iter()
                    .map(|c| object.get(c).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        Ok(Rows { columns, rows })
    }

    /// The body of an `expect` section: `columns: [...]`, then one JSON
    /// array per row.
    pub fn parse(body: &str) -> Result<Self, String> {
        let mut lines = body.lines().filter(|l| !l.trim().is_empty());
        let first = lines
            .next()
            .ok_or("an expect section needs a `columns:` line")?;
        let columns = first
            .strip_prefix("columns:")
            .ok_or("an expect section starts with `columns:`")?;
        let columns: Vec<String> =
            serde_json::from_str(columns.trim()).map_err(|e| format!("columns: {e}"))?;
        let mut rows = Vec::new();
        for line in lines {
            let row: Vec<Value> =
                serde_json::from_str(line).map_err(|e| format!("row `{line}`: {e}"))?;
            if row.len() != columns.len() {
                return Err(format!(
                    "row `{line}` has {} cells for {} columns",
                    row.len(),
                    columns.len()
                ));
            }
            rows.push(row);
        }
        Ok(Rows { columns, rows })
    }

    pub fn render(&self) -> String {
        let mut out = format!(
            "columns: {}\n",
            serde_json::to_string(&self.columns).expect("strings serialize")
        );
        for row in &self.rows {
            out.push_str(&serde_json::to_string(row).expect("values serialize"));
            out.push('\n');
        }
        out
    }

    pub fn to_json(&self) -> Value {
        json!({"columns": self.columns, "rows": self.rows})
    }

    /// Equal values, row for row, in order or as a bag.
    pub fn same_values(&self, other: &Rows, ordered: bool) -> bool {
        let canon = |rows: &Rows| {
            let mut out: Vec<String> = rows
                .rows
                .iter()
                .map(|r| canonical_json(&Value::Array(r.clone())))
                .collect();
            if !ordered {
                out.sort();
            }
            out
        };
        self.columns.len() == other.columns.len() && canon(self) == canon(other)
    }

    /// The same columns and the same values, in order or as a bag: what a
    /// recorded expect must still match.
    pub fn matches(&self, other: &Rows, ordered: bool) -> bool {
        self.columns == other.columns && self.same_values(other, ordered)
    }

    /// The rows in a stable order for recording: as returned when the task
    /// orders them, sorted by their canonical form otherwise.
    pub fn recorded(mut self, ordered: bool) -> Self {
        if !ordered {
            self.rows
                .sort_by_cached_key(|r| canonical_json(&Value::Array(r.clone())));
        }
        self
    }

    fn column(&self, name: &str) -> Result<Vec<Value>, String> {
        let index = self
            .columns
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| format!("no column `{name}` among {:?}", self.columns))?;
        Ok(self.rows.iter().map(|r| r[index].clone()).collect())
    }
}

/// A seeded graph of one dataset.
pub struct Graph {
    session: Session,
    _dir: tempfile::TempDir,
}

impl Graph {
    pub async fn open(dataset: &Dataset) -> Result<Self, String> {
        let text = format!(
            "# issue: none\n# notes: an agent instrument dataset.\n\n--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    \
             storage: local-filesystem\n\n--- schema\n{}\n\n--- seed\n{}\n",
            dataset.schema.trim_end(),
            dataset.seed.trim_end()
        );
        let case = parse_case(&format!("agent_{}", dataset.name), &text)
            .map_err(|e| format!("dataset `{}`: {e}", dataset.name))?;
        let engine = crate::engine_from_env()?;
        let (session, _uri, dir) = crate::open_case_store(&case, engine).await?;
        session
            .ensure_indices()
            .await
            .map_err(|e| format!("dataset `{}`: ensure_indices failed: {e}", dataset.name))?;
        Ok(Graph { session, _dir: dir })
    }

    async fn read(&self, source: &str, decl: &QueryDecl, params: &Value) -> Result<Rows, String> {
        let map = json_params_to_param_map(Some(params), &decl.params, JsonParamMode::Standard)
            .map_err(|e| format!("params: {e}"))?;
        let result = self
            .session
            .query(ReadTarget::branch("main"), source, &decl.name, &map)
            .await
            .map_err(|e| format!("`{}` failed: {e}", decl.name))?;
        Rows::from_result(&result)
    }

    async fn write(&self, source: &str, decl: &QueryDecl, params: &Value) -> Result<(), String> {
        let map = json_params_to_param_map(Some(params), &decl.params, JsonParamMode::Standard)
            .map_err(|e| format!("params: {e}"))?;
        self.session
            .mutate("main", source, &decl.name, &map)
            .await
            .map(|_| ())
            .map_err(|e| format!("`{}` failed: {e}", decl.name))
    }

    /// Run the one declaration of `source`, reading or writing.
    async fn run(&self, source: &str, params: &Value) -> Result<Option<Rows>, String> {
        let decl = only_declaration(source)?;
        if decl.mutations.is_empty() {
            self.read(source, &decl, params).await.map(Some)
        } else {
            self.write(source, &decl, params).await.map(|()| None)
        }
    }
}

fn only_declaration(source: &str) -> Result<QueryDecl, String> {
    let file = omnigraph_compiler::query::parser::parse_query(source).map_err(|e| e.to_string())?;
    let decls = file.into_declarations().map_err(|e| e.to_string())?;
    let count = decls.len();
    match <[QueryDecl; 1]>::try_from(decls) {
        Ok([decl]) => Ok(decl),
        Err(_) => Err(format!("a step holds one declaration, not {count}")),
    }
}

/// The task's parameters restricted to the ones `source` declares, plus
/// `extra`; an undeclared task parameter is not an error, since an answer
/// may not need every value the task names.
fn params_for(source: &str, task: &Task, extra: &BTreeMap<String, Value>) -> Result<Value, String> {
    let decl = only_declaration(source)?;
    let mut out = serde_json::Map::new();
    for param in &decl.params {
        if let Some(value) = extra
            .get(&param.name)
            .or_else(|| task.params.get(&param.name))
        {
            out.insert(param.name.clone(), value.clone());
        }
    }
    Ok(Value::Object(out))
}

fn bound(binding: &Binding, results: &[Option<Rows>]) -> Result<Vec<Value>, String> {
    results[binding.step]
        .as_ref()
        .ok_or_else(|| {
            format!(
                "step {} is a write; `{}` binds from a read",
                binding.step + 1,
                binding.param
            )
        })?
        .column(&binding.column)
}

/// Run an edition-2 ground truth: its steps in order with their bindings,
/// then, for a write, the task's check. The graph must be fresh for a write.
pub async fn run_truth(graph: &Graph, task: &Task, steps: &[Step]) -> Result<Rows, String> {
    let mut results: Vec<Option<Rows>> = Vec::with_capacity(steps.len());
    for step in steps {
        let mut extra = BTreeMap::new();
        for binding in &step.bind {
            extra.insert(
                binding.param.clone(),
                Value::Array(bound(binding, &results)?),
            );
        }
        let result = match &step.each {
            None => {
                graph
                    .run(&step.query, &params_for(&step.query, task, &extra)?)
                    .await?
            }
            Some(each) => {
                let mut merged: Option<Rows> = None;
                for value in bound(each, &results)? {
                    extra.insert(each.param.clone(), value);
                    let rows = graph
                        .run(&step.query, &params_for(&step.query, task, &extra)?)
                        .await?;
                    merged = match (merged, rows) {
                        (None, rows) => rows,
                        (Some(mut acc), Some(rows)) => {
                            acc.rows.extend(rows.rows);
                            Some(acc)
                        }
                        (Some(acc), None) => Some(acc),
                    };
                }
                merged
            }
        };
        results.push(result);
    }
    answer_rows(graph, task, results).await
}

async fn answer_rows(
    graph: &Graph,
    task: &Task,
    results: Vec<Option<Rows>>,
) -> Result<Rows, String> {
    if let Some(check) = &task.check {
        let rows = graph
            .run(check, &params_for(check, task, &BTreeMap::new())?)
            .await?;
        return rows.ok_or_else(|| "the check is not a read".to_string());
    }
    let reads: Vec<Rows> = results.into_iter().flatten().collect();
    match task.combine {
        Combine::Last => reads
            .into_iter()
            .last()
            .ok_or_else(|| "no step reads".to_string()),
        Combine::Concat => {
            let mut iter = reads.into_iter();
            let mut acc = iter.next().ok_or("no step reads")?;
            for rows in iter {
                if rows.columns.len() != acc.columns.len() {
                    return Err(format!(
                        "concatenated reads disagree on columns: {:?} and {:?}",
                        acc.columns, rows.columns
                    ));
                }
                acc.rows.extend(rows.rows);
            }
            Ok(acc)
        }
    }
}

/// Run an agent's answer of one declaration with the task's parameters;
/// a write is followed by the task's check.
pub async fn run_answer(graph: &Graph, task: &Task, text: &str) -> Result<Rows, String> {
    let result = graph
        .run(text, &params_for(text, task, &BTreeMap::new())?)
        .await?;
    answer_rows(graph, task, vec![result]).await
}
