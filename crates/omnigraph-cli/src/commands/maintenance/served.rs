//! `omnigraph optimize` against a server target. The command mints an id,
//! prints it before it sends anything, submits, and waits for the operation's
//! terminal record. After a lost response it looks the operation up by that
//! id; it never submits the same work again.

use std::io::Write;
use std::time::Duration;

use color_eyre::eyre::Result;
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::app::Completion;
use crate::client::GraphClient;
use crate::helpers::RemoteErrorCli;
use crate::output::{graph_type_subject, write_json};

const POLL: Duration = Duration::from_millis(500);

/// The exits of a served optimize.
const SUCCEEDED: i32 = 0;
const NO_EFFECTS: i32 = 1;
const IN_DOUBT: i32 = 2;
const DIFFERENT_REQUEST: i32 = 4;
const ANOTHER_ACTIVE: i32 = 75;

pub(crate) struct ServedOptimize {
    pub(crate) tables: Vec<String>,
    pub(crate) max_gate_hold: Option<Duration>,
    pub(crate) no_wait: bool,
    pub(crate) operation: Option<String>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) json: bool,
}

/// How a submission that got no record ended.
#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    /// The id names another request.
    DifferentRequest,
    /// Another maintenance operation holds the cluster's slot.
    AnotherActive,
    /// Refused before acceptance: nothing ran.
    BeforeAcceptance,
    /// No definite answer: the operation may have been accepted.
    Unanswered,
}

fn classify(error: &color_eyre::Report) -> Refusal {
    let Some(remote) = error.downcast_ref::<RemoteErrorCli>() else {
        return Refusal::Unanswered;
    };
    match remote.status.as_u16() {
        409 if remote
            .output
            .error
            .starts_with("[maintenance_request_mismatch]") =>
        {
            Refusal::DifferentRequest
        }
        429 => Refusal::AnotherActive,
        status if status >= 500 => Refusal::Unanswered,
        _ => Refusal::BeforeAcceptance,
    }
}

fn request_body(request: &ServedOptimize, maintenance_id: &str) -> Value {
    let mut body = json!({ "maintenance_id": maintenance_id });
    if !request.tables.is_empty() {
        body["tables"] = json!(request.tables);
    }
    if let Some(hold) = request.max_gate_hold {
        body["max_gate_hold_ms"] = json!(hold.as_millis() as u64);
    }
    body
}

fn state(record: &Value) -> Option<&str> {
    record["operation"]["state"].as_str()
}

fn is_terminal(state: &str) -> bool {
    matches!(state, "succeeded" | "failed" | "cancelled" | "interrupted")
}

pub(crate) async fn run(
    client: &GraphClient,
    request: ServedOptimize,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let (maintenance_id, mut answer) = match &request.operation {
        Some(id) => (id.clone(), client.maintenance_operation(id).await),
        None => {
            let id = omnigraph_cluster::new_maintenance_id();
            writeln!(stderr, "maintenance id: {id}")?;
            stderr.flush()?;
            match client.start_optimize(request_body(&request, &id)).await {
                Ok(record) => (id, Ok(record)),
                Err(error) => match classify(&error) {
                    Refusal::DifferentRequest => {
                        writeln!(stderr, "error: {error}")?;
                        return Ok(Completion::exit(DIFFERENT_REQUEST));
                    }
                    Refusal::AnotherActive => {
                        writeln!(stderr, "error: {error}")?;
                        return Ok(Completion::exit(ANOTHER_ACTIVE));
                    }
                    Refusal::BeforeAcceptance => {
                        writeln!(stderr, "error: {error}")?;
                        return Ok(Completion::exit(NO_EFFECTS));
                    }
                    Refusal::Unanswered => {
                        writeln!(
                            stderr,
                            "no definite answer for {id} ({error}); looking it up, never resubmitting"
                        )?;
                        let answer = client.maintenance_operation(&id).await;
                        (id, answer)
                    }
                },
            }
        }
    };

    let deadline = request.timeout.map(|timeout| Instant::now() + timeout);
    loop {
        if let Ok(record) = &answer {
            if record["lookup"] == "unknown" {
                writeln!(
                    stderr,
                    "the server holds no record of maintenance operation {maintenance_id}; that proves neither that it ran nor that it did not, and submitting the same work again would be a new operation"
                )?;
                if request.json {
                    write_json(record, stdout)?;
                }
                return Ok(Completion::exit(IN_DOUBT));
            }
            match state(record) {
                Some(state) if request.no_wait || is_terminal(state) => {
                    write_record(&maintenance_id, record, request.json, stdout)?;
                    return Ok(Completion::exit(match state {
                        _ if !is_terminal(state) => SUCCEEDED,
                        "succeeded" => SUCCEEDED,
                        _ => NO_EFFECTS,
                    }));
                }
                _ => {}
            }
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            match &answer {
                Ok(record) => write_record(&maintenance_id, record, request.json, stdout)?,
                Err(error) => writeln!(stderr, "error: {error}")?,
            }
            writeln!(
                stderr,
                "stopped waiting; the operation continues on the server: observe it with `omnigraph optimize --operation {maintenance_id}`"
            )?;
            return Ok(Completion::exit(IN_DOUBT));
        }
        tokio::time::sleep(POLL).await;
        answer = client.maintenance_operation(&maintenance_id).await;
    }
}

fn write_record(
    maintenance_id: &str,
    record: &Value,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        return write_json(record, stdout);
    }
    let operation = &record["operation"];
    let state = operation["state"].as_str().unwrap_or("unknown");
    writeln!(stdout, "optimize {state} — maintenance {maintenance_id}")?;
    if let Some(tables) = operation["tables"].as_array() {
        for table in tables {
            let subject = graph_type_subject(table["type_key"].as_str().unwrap_or(""));
            let outcome = table["outcome"].as_str().unwrap_or("");
            match outcome {
                "optimized" => writeln!(
                    stdout,
                    "  {subject:<40} frags {} → {} ✓",
                    table["fragments_removed"], table["fragments_added"]
                )?,
                "unchanged" => writeln!(stdout, "  {subject:<40} no-op")?,
                _ => {
                    let detail = table["detail"].as_str().or(table["reason"].as_str());
                    match detail {
                        Some(detail) => writeln!(stdout, "  {subject:<40} {outcome} ({detail})")?,
                        None => writeln!(stdout, "  {subject:<40} {outcome}")?,
                    }
                }
            }
        }
    }
    if let Some(commit) = operation["published_commit"].as_str() {
        writeln!(stdout, "graph commit: {commit}")?;
    }
    if let Some(key) = operation["resume_at"].as_str() {
        writeln!(stdout, "resume at: {}", graph_type_subject(key))?;
    }
    if let Some(reason) = operation["reason"].as_str() {
        writeln!(stdout, "reason: {reason}")?;
    }
    if record["in_progress"] == true {
        writeln!(stdout, "in progress on the server")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote(status: u16, code: &str, message: &str) -> color_eyre::Report {
        let output: omnigraph_api_types::ErrorOutput = serde_json::from_value(json!({
            "error": message,
            "code": code,
        }))
        .unwrap();
        color_eyre::Report::new(RemoteErrorCli {
            output,
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            retry_after: None,
            missing_credential: None,
        })
    }

    #[test]
    fn refusals_map_to_their_exits() {
        assert_eq!(
            classify(&remote(
                409,
                "conflict",
                "[maintenance_request_mismatch] maintenance id x names a different request"
            )),
            Refusal::DifferentRequest
        );
        assert_eq!(
            classify(&remote(409, "conflict", "deployment d is outstanding")),
            Refusal::BeforeAcceptance
        );
        assert_eq!(
            classify(&remote(429, "too_many_requests", "active")),
            Refusal::AnotherActive
        );
        assert_eq!(
            classify(&remote(500, "outcome_unknown", "no definite answer")),
            Refusal::Unanswered
        );
        assert_eq!(
            classify(&color_eyre::eyre::eyre!("connection reset")),
            Refusal::Unanswered
        );
    }

    #[test]
    fn the_request_carries_only_what_was_asked() {
        let request = ServedOptimize {
            tables: vec!["node:Person".to_string()],
            max_gate_hold: Some(Duration::from_secs(30)),
            no_wait: false,
            operation: None,
            timeout: None,
            json: false,
        };
        assert_eq!(
            request_body(&request, "01JC0000000000000000000001"),
            json!({
                "maintenance_id": "01JC0000000000000000000001",
                "tables": ["node:Person"],
                "max_gate_hold_ms": 30_000,
            })
        );
    }
}
