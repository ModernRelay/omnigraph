//! The GQ 3 agent instrument (RFC 2026-11-07-gq-3, stage M0): the data under
//! `crates/omnigraph-gqt/agent/` and the judge that accepts, types and runs
//! an agent's answer. `agent/README.md` states what each number means; the
//! `omnigraph-gqt-agent` binary is the runner's one door into this module.

pub mod card;
mod exec;
mod front;
mod normalize;
mod task;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

use omnigraph_compiler::build_catalog;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::schema::parser::parse_schema;
use serde_json::{Value, json};

pub use exec::{Graph, Rows, run_answer, run_truth};
pub use front::{Declaration, EDITION3_PENDING, Edition, Kind, Verdict, accept, available, render};
pub use task::{Answer, Dataset, Expect, Instrument, Seed, Task};

pub fn catalog(dataset: &Dataset) -> Result<Catalog, String> {
    let schema =
        parse_schema(&dataset.schema).map_err(|e| format!("dataset `{}`: {e}", dataset.name))?;
    build_catalog(&schema).map_err(|e| format!("dataset `{}`: {e}", dataset.name))
}

/// The rows a task's edition-2 ground truth answers: the recorded ones, or,
/// for a live expect, the ground truth run now.
pub async fn truth_rows(instrument: &Instrument, task: &Task) -> Result<Rows, String> {
    if let Some(Expect {
        pinned: Some(rows), ..
    }) = &task.expect
    {
        return Ok(rows.clone());
    }
    let graph = Graph::open(&instrument.datasets[&task.dataset]).await?;
    run_truth(&graph, task, &task.edition2).await
}

/// Judges answers against the instrument, keeping one catalog and one read
/// graph per dataset across answers.
pub struct Judge<'a> {
    instrument: &'a Instrument,
    catalogs: HashMap<String, Catalog>,
    graphs: HashMap<String, Graph>,
    expected: HashMap<String, Rows>,
}

impl<'a> Judge<'a> {
    pub fn new(instrument: &'a Instrument) -> Self {
        Judge {
            instrument,
            catalogs: HashMap::new(),
            graphs: HashMap::new(),
            expected: HashMap::new(),
        }
    }

    fn catalog(&mut self, dataset: &str) -> Result<&Catalog, String> {
        if !self.catalogs.contains_key(dataset) {
            let built = catalog(&self.instrument.datasets[dataset])?;
            self.catalogs.insert(dataset.to_string(), built);
        }
        Ok(&self.catalogs[dataset])
    }

    async fn expected(&mut self, task: &Task) -> Result<Rows, String> {
        if !self.expected.contains_key(&task.id) {
            let rows = truth_rows(self.instrument, task).await?;
            self.expected.insert(task.id.clone(), rows);
        }
        Ok(self.expected[&task.id].clone())
    }

    /// Accept or refuse `text`; when edition 2 accepts one declaration and
    /// the task has an edition-2 ground truth, run it on the seed and compare
    /// its rows. The result is one JSON object for the runner.
    pub async fn judge(&mut self, task_id: &str, edition: Edition, text: &str) -> Value {
        match self.judge_inner(task_id, edition, text).await {
            Ok(value) => value,
            Err(error) => json!({"error": error}),
        }
    }

    async fn judge_inner(
        &mut self,
        task_id: &str,
        edition: Edition,
        text: &str,
    ) -> Result<Value, String> {
        let instrument = self.instrument;
        let task = instrument
            .task(task_id)
            .ok_or_else(|| format!("no task `{task_id}`"))?;
        let verdict = accept(edition, self.catalog(&task.dataset)?, text);
        let mut out = verdict.to_json();
        let Verdict::Accepted(decls) = &verdict else {
            return Ok(out);
        };
        out["queries"] = json!(decls.len());
        let (correct, reason) = self
            .correctness(task, edition, decls, text, &mut out)
            .await?;
        out["correct"] = json!(correct);
        out["correct_reason"] = json!(reason);
        Ok(out)
    }

    async fn correctness(
        &mut self,
        task: &Task,
        edition: Edition,
        decls: &[Declaration],
        text: &str,
        out: &mut Value,
    ) -> Result<(Option<bool>, String), String> {
        if edition == Edition::Three {
            // Typed-IR equality with a ground truth; reached only once the
            // edition-3 front end accepts.
            let catalog = self.catalog(&task.dataset)?;
            for truth in &task.edition3 {
                if let Verdict::Accepted(truth) = accept(edition, catalog, &edition3_text(truth)) {
                    if same_ir(decls, &truth) {
                        return Ok((Some(true), "typed IR equals a ground truth".into()));
                    }
                }
            }
            return Ok((Some(false), "typed IR equals no ground truth".into()));
        }
        if task.edition2.is_empty() {
            return Ok((None, "edition 2 has no ground truth for this task".into()));
        }
        if decls.len() > 1 {
            return Ok((
                None,
                "several declarations: the glue between them is prose; judge by hand".into(),
            ));
        }
        let wants = if task.is_write() {
            Kind::Write
        } else {
            Kind::Read
        };
        if decls[0].kind != wants {
            return Ok((
                Some(false),
                "a read where the task writes, or the reverse".into(),
            ));
        }
        if let [step] = task.edition2.as_slice() {
            let catalog = self.catalog(&task.dataset)?;
            if let Verdict::Accepted(truth) = accept(edition, catalog, &step.query) {
                out["ir_equal"] = json!(same_ir(decls, &truth));
            }
        }
        let expected = self.expected(task).await?;
        let dataset = &self.instrument.datasets[&task.dataset];
        let actual = if task.is_write() {
            run_answer(&Graph::open(dataset).await?, task, text).await
        } else {
            if !self.graphs.contains_key(&task.dataset) {
                let graph = Graph::open(dataset).await?;
                self.graphs.insert(task.dataset.clone(), graph);
            }
            run_answer(&self.graphs[&task.dataset], task, text).await
        };
        let ordered = task.expect.as_ref().is_some_and(|e| e.ordered);
        Ok(match actual {
            Ok(rows) => {
                let equal = rows.same_values(&expected, ordered);
                out["rows"] = rows.to_json();
                let reason = if equal {
                    "rows equal the ground truth's on the seed"
                } else {
                    "rows differ from the ground truth's on the seed"
                };
                (Some(equal), reason.into())
            }
            Err(error) => {
                out["execution_error"] = json!(error);
                (
                    Some(false),
                    "accepted, but the engine refused it at run time".into(),
                )
            }
        })
    }
}

/// An edition-3 ground truth as an answer is judged: with its edition line.
pub fn edition3_text(truth: &str) -> String {
    format!("gq 3;\n{truth}\n")
}

fn same_ir(answer: &[Declaration], truth: &[Declaration]) -> bool {
    answer.len() == truth.len()
        && answer
            .iter()
            .zip(truth)
            .all(|(a, t)| a.kind == t.kind && a.ir == t.ir)
}
