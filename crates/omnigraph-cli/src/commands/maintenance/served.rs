//! `omnigraph optimize` against a server target, a recorded operation. The
//! command mints an id,
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
/// Consecutive lookups without a definite answer before the wait gives up.
const UNANSWERED_LOOKUPS: u32 = 5;

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
    /// Another operation holds the scope's active slot.
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
            .starts_with("[operation_request_mismatch]") =>
        {
            Refusal::DifferentRequest
        }
        429 => Refusal::AnotherActive,
        // A server error that says it took no effect (routes off, admission
        // closed) refused the request before acceptance; any other server
        // error leaves the submission in doubt.
        status
            if status >= 500
                && !remote.output.disposition.as_ref().is_some_and(|disposition| {
                    disposition.effects == omnigraph_api_types::DispositionEffects::None
                }) =>
        {
            Refusal::Unanswered
        }
        _ => Refusal::BeforeAcceptance,
    }
}

/// How a lookup during the wait ended. The operation is accepted by then
/// (or its submission is in doubt), so a refusal says the record cannot be
/// observed now, never that nothing started.
#[derive(Debug, PartialEq, Eq)]
enum LookupAnswer {
    /// A definite refusal: the id is accepted but cannot be observed by this
    /// caller now (a grant removed, the graph gone, the record foreign).
    Refused,
    /// No definite answer, or the server is busy: ask again.
    Unanswered,
}

fn classify_lookup(error: &color_eyre::Report) -> LookupAnswer {
    match error.downcast_ref::<RemoteErrorCli>() {
        Some(remote) if remote.status.as_u16() < 500 && remote.status.as_u16() != 429 => {
            LookupAnswer::Refused
        }
        _ => LookupAnswer::Unanswered,
    }
}

fn request_body(request: &ServedOptimize, operation_id: &str) -> Value {
    let mut body = json!({ "operation_id": operation_id });
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
    matches!(
        state,
        "succeeded" | "failed" | "cancelled" | "interrupted" | "unsettled"
    )
}

pub(crate) async fn run(
    client: &GraphClient,
    request: ServedOptimize,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let (operation_id, mut answer) = match &request.operation {
        Some(id) => (id.clone(), client.operation(id).await),
        None => {
            let id = omnigraph_cluster::new_operation_id();
            writeln!(stderr, "operation id: {id}")?;
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
                        let answer = client.operation(&id).await;
                        (id, answer)
                    }
                },
            }
        }
    };

    let deadline = request.timeout.map(|timeout| Instant::now() + timeout);
    let mut unanswered = 0_u32;
    loop {
        match &answer {
            Ok(_) => unanswered = 0,
            // The operation is accepted or in doubt by now, so a refused
            // lookup is doubt about its outcome, never "nothing started"; a
            // lost answer is retried a bounded number of times.
            Err(error) => match classify_lookup(error) {
                LookupAnswer::Refused => {
                    writeln!(
                        stderr,
                        "operation {operation_id} cannot be observed now ({error}); it may still run, observe it with `omnigraph optimize --operation {operation_id}`"
                    )?;
                    return Ok(Completion::exit(IN_DOUBT));
                }
                LookupAnswer::Unanswered => {
                    unanswered += 1;
                    if unanswered >= UNANSWERED_LOOKUPS {
                        writeln!(
                            stderr,
                            "no answer from the server after {UNANSWERED_LOOKUPS} lookups of {operation_id} ({error}); the operation may still run, observe it with `omnigraph optimize --operation {operation_id}`"
                        )?;
                        return Ok(Completion::exit(IN_DOUBT));
                    }
                }
            },
        }
        if let Ok(record) = &answer {
            if record["lookup"] == "unknown" {
                writeln!(
                    stderr,
                    "the server holds no record of operation {operation_id}; that proves neither that it ran nor that it did not, and submitting the same work again would be a new operation"
                )?;
                if request.json {
                    write_json(record, stdout)?;
                }
                return Ok(Completion::exit(IN_DOUBT));
            }
            match state(record) {
                Some(state) if request.no_wait || is_terminal(state) => {
                    write_record(&operation_id, record, request.json, stdout)?;
                    return Ok(Completion::exit(match state {
                        _ if !is_terminal(state) => SUCCEEDED,
                        "succeeded" => SUCCEEDED,
                        // The authority that could settle the publication is gone.
                        "unsettled" => IN_DOUBT,
                        _ => NO_EFFECTS,
                    }));
                }
                _ => {}
            }
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            match &answer {
                Ok(record) => write_record(&operation_id, record, request.json, stdout)?,
                Err(error) => writeln!(stderr, "error: {error}")?,
            }
            writeln!(
                stderr,
                "stopped waiting; the operation continues on the server: observe it with `omnigraph optimize --operation {operation_id}`"
            )?;
            return Ok(Completion::exit(IN_DOUBT));
        }
        tokio::time::sleep(POLL).await;
        answer = client.operation(&operation_id).await;
    }
}

fn write_record(
    operation_id: &str,
    record: &Value,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        return write_json(record, stdout);
    }
    let operation = &record["operation"];
    let state = operation["state"].as_str().unwrap_or("unknown");
    writeln!(stdout, "optimize {state} — operation {operation_id}")?;
    let outcome = &operation["outcome"];
    if let Some(tables) = outcome["tables"].as_array() {
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
    if let Some(key) = outcome["resume_at"].as_str() {
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
    fn a_server_error_that_took_no_effect_is_a_refusal_before_acceptance() {
        let refused: omnigraph_api_types::ErrorOutput = serde_json::from_value(json!({
            "error": "the recorded-operation routes are not enabled on this server",
            "code": "service_unavailable",
            "disposition": {"effects": "none", "action": "retry"},
        }))
        .unwrap();
        let refused = color_eyre::Report::new(RemoteErrorCli {
            output: refused,
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            retry_after: None,
            missing_credential: None,
        });
        assert_eq!(classify(&refused), Refusal::BeforeAcceptance);
        let unknown: omnigraph_api_types::ErrorOutput = serde_json::from_value(json!({
            "error": "the operation record write has no definite answer",
            "code": "outcome_unknown",
            "disposition": {"effects": "unknown", "action": "reconcile"},
        }))
        .unwrap();
        let unknown = color_eyre::Report::new(RemoteErrorCli {
            output: unknown,
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            retry_after: None,
            missing_credential: None,
        });
        assert_eq!(classify(&unknown), Refusal::Unanswered);
    }

    #[test]
    fn refusals_map_to_their_exits() {
        assert_eq!(
            classify(&remote(
                409,
                "conflict",
                "[operation_request_mismatch] operation id x names a different request"
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
    fn a_refused_lookup_is_doubt_not_a_refusal_before_acceptance() {
        assert_eq!(
            classify_lookup(&remote(403, "forbidden", "maintain was revoked")),
            LookupAnswer::Refused
        );
        assert_eq!(
            classify_lookup(&remote(404, "not_found", "graph gone")),
            LookupAnswer::Refused
        );
        assert_eq!(
            classify_lookup(&remote(429, "too_many_requests", "busy")),
            LookupAnswer::Unanswered
        );
        assert_eq!(
            classify_lookup(&remote(503, "service_unavailable", "draining")),
            LookupAnswer::Unanswered
        );
        assert_eq!(
            classify_lookup(&color_eyre::eyre::eyre!("connection reset")),
            LookupAnswer::Unanswered
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
                "operation_id": "01JC0000000000000000000001",
                "tables": ["node:Person"],
                "max_gate_hold_ms": 30_000,
            })
        );
    }
}
