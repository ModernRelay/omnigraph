//! Conservative evidence for one CLI data-write invocation. This is caller
//! bookkeeping, never engine settlement or durable operation authority.

use std::sync::{Arc, Mutex};

use color_eyre::Report;
use omnigraph_api_types::{ErrorCode, ErrorOutput};
use reqwest::StatusCode;
use serde::Serialize;

use crate::cli::{BranchCommand, Cli, Command, SchemaCommand};
use crate::graph_http::ApiContractError;
use crate::helpers::{PreconditionFailedCli, RemoteErrorCli};

#[derive(Default, Debug, Clone)]
pub(crate) struct Evidence {
    dispatched: usize,
    writable_open: bool,
    last_request_effectful: bool,
    http_status: Option<u16>,
    retry_after: Option<String>,
}

tokio::task_local! {
    static COMMAND: Arc<Mutex<Evidence>>;
}

pub(crate) fn applies(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Command::Load { .. }
            | Command::Ingest { .. }
            | Command::Mutate { .. }
            | Command::Branch {
                command: BranchCommand::Create { .. }
                    | BranchCommand::Delete { .. }
                    | BranchCommand::Merge { .. }
            }
            | Command::Schema {
                command: SchemaCommand::Apply { .. }
            }
    )
}

pub(crate) async fn observe<F: std::future::Future>(future: F) -> (F::Output, Evidence) {
    let evidence = Arc::new(Mutex::new(Evidence::default()));
    let output = COMMAND.scope(evidence.clone(), future).await;
    let evidence = evidence
        .lock()
        .expect("CLI command evidence poisoned")
        .clone();
    (output, evidence)
}

fn update(change: impl FnOnce(&mut Evidence)) {
    let _ = COMMAND.try_with(|state| {
        change(&mut state.lock().expect("CLI command evidence poisoned"));
    });
}

/// GET/HEAD discovery and scope inspection cannot change a graph. All other
/// requests in a data-write command count conservatively, even if a future
/// compound command uses a read-by-POST as a preparatory step.
pub(crate) fn dispatched(method: &reqwest::Method) {
    update(|state| {
        state.last_request_effectful =
            method != reqwest::Method::GET && method != reqwest::Method::HEAD;
        if state.last_request_effectful {
            state.dispatched += 1;
        }
        state.http_status = None;
        state.retry_after = None;
    });
}

pub(crate) fn response(
    status: StatusCode,
    headers: &reqwest::header::HeaderMap,
    bearer: Option<&str>,
) {
    update(|state| {
        state.http_status = Some(status.as_u16());
        state.retry_after = retry_after(headers).map(|value| scrub_backoff(value, bearer));
    });
}

pub(crate) fn scrub_backoff(value: String, bearer: Option<&str>) -> String {
    match bearer.filter(|token| !token.is_empty()) {
        Some(token) => value.replace(token, "[redacted]"),
        None => value,
    }
}

pub(crate) fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let mut values = headers.get_all(reqwest::header::RETRY_AFTER).iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then(|| value.to_owned())
}

/// A remote conditional refusal closes only its request. Earlier dispatched
/// work in the command prevents promotion to the whole-command exit 4.
pub(crate) fn single_request() -> bool {
    COMMAND
        .try_with(|state| {
            let state = state.lock().expect("CLI command evidence poisoned");
            state.dispatched == 1 && state.last_request_effectful && !state.writable_open
        })
        .unwrap_or(true)
}

/// Opening writable storage may complete earlier schema work before the
/// requested write even begins; a later error must not claim no effects.
pub(crate) fn writable_open() {
    update(|state| state.writable_open = true);
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Execution {
    NotStarted,
    Unknown,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Effects {
    None,
    Unknown,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    Retry,
    Refresh,
    Recover,
    Reconcile,
}

#[derive(Debug, Serialize)]
pub(crate) struct CommandOutcome {
    execution: Execution,
    effects: Effects,
    action: Action,
}

#[derive(Debug, Serialize)]
pub(crate) struct Failure {
    #[serde(flatten)]
    pub(crate) output: ErrorOutput,
    pub(crate) command_outcome: CommandOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retry_after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) request_dispatched: Option<bool>,
    #[serde(skip)]
    pub(crate) exit: i32,
}

impl Failure {
    pub(crate) fn classify(error: Report, evidence: Evidence) -> Self {
        let remote = error.downcast_ref::<RemoteErrorCli>();
        let precondition = error.downcast_ref::<PreconditionFailedCli>();
        let single =
            evidence.dispatched == 1 && evidence.last_request_effectful && !evidence.writable_open;
        let conditional =
            precondition.is_some_and(|value| value.http_status == Some(412)) && single;
        let retry = remote.is_some_and(|remote| {
            remote.status == StatusCode::TOO_MANY_REQUESTS
                && matches!(remote.output.code, Some(ErrorCode::TooManyRequests))
                && !remote.output.error.trim().is_empty()
                && is_plain_refusal(&remote.output)
                && single
        });
        let contract = error.downcast_ref::<ApiContractError>();
        let request_dispatched = contract.map(|value| value.request_dispatched);
        let http_status = remote
            .map(|value| value.status.as_u16())
            .or_else(|| contract.and_then(|value| value.http_status))
            .or_else(|| precondition.and_then(|value| value.http_status))
            .or(evidence.http_status);
        let retry_after = if let Some(remote) = remote {
            remote.retry_after.clone()
        } else if let Some(precondition) = precondition {
            precondition.retry_after.clone()
        } else {
            evidence.retry_after
        };
        let output = if let Some(remote) = remote {
            remote.output.clone()
        } else if let Some(precondition) = precondition {
            precondition.output.clone()
        } else if let Some(contract) = contract {
            let mut output = ErrorOutput::message(contract.error.clone());
            output.code = Some(contract.code);
            output
        } else {
            match error.downcast::<omnigraph::error::OmniError>() {
                Ok(engine) => omnigraph_server::engine_error_output(engine),
                Err(error) => crate::error_output_of(&error)
                    .unwrap_or_else(|| ErrorOutput::message(error.to_string())),
            }
        };
        let not_started =
            retry || conditional || (evidence.dispatched == 0 && !evidence.writable_open);
        let action = if retry {
            Action::Retry
        } else if output.recovery_required.is_some()
            || output.full_text_index_rebuild_required.is_some()
        {
            Action::Recover
        } else if not_started
            || output.precondition_failure.is_some()
            || output.read_set_conflict.is_some()
            || output.published_dataset_version_conflict.is_some()
            || output.key_conflict.is_some()
            || output.resource_limit.is_some()
            || !output.merge_conflicts.is_empty()
            || output.diagnostic.is_some()
        {
            Action::Refresh
        } else {
            Action::Reconcile
        };
        Self {
            output,
            command_outcome: CommandOutcome {
                execution: if not_started {
                    Execution::NotStarted
                } else {
                    Execution::Unknown
                },
                effects: if not_started {
                    Effects::None
                } else {
                    Effects::Unknown
                },
                action,
            },
            http_status,
            retry_after,
            request_dispatched,
            exit: if retry {
                75
            } else if conditional {
                crate::EXIT_PRECONDITION_FAILED
            } else {
                1
            },
        }
    }
}

pub(crate) fn is_plain_refusal(output: &ErrorOutput) -> bool {
    output.merge_conflicts.is_empty()
        && output.published_dataset_version_conflict.is_none()
        && output.read_set_conflict.is_none()
        && output.key_conflict.is_none()
        && output.resource_limit.is_none()
        && output.blob_range.is_none()
        && output.external_blob_source.is_none()
        && output.recovery_required.is_none()
        && output.precondition_failure.is_none()
        && output.change_feed_gap.is_none()
        && output.change_diff_refusal.is_none()
        && output.full_text_index_rebuild_required.is_none()
        && output.diagnostic.is_none()
}
