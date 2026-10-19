//! Conservative evidence for one CLI invocation, and the outcome and exit its
//! failure reports. This is caller bookkeeping, never engine settlement or
//! durable operation authority.

use std::sync::{Arc, Mutex};

use color_eyre::Report;
use omnigraph_api_types::{
    ErrorCode, ErrorOutput, OutcomeAction as Action, OutcomeEffects as Effects,
    OutcomeExecution as Execution, OutcomeOutput,
};
use reqwest::StatusCode;
use serde::Serialize;

use crate::cli::{
    BlobCommand, BranchCommand, Cli, Command, CommitCommand, GraphsCommand, QueriesCommand,
    SchemaCommand,
};
use crate::graph_http::ApiContractError;
use crate::helpers::{MissingCredential, PreconditionFailedCli, RemoteErrorCli};

/// What a command can change, which decides how its failure reads.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandKind {
    /// A data write: its own requests and opens decide whether it may have
    /// had effects.
    #[default]
    Write,
    /// A read, which changes nothing: its failure never leaves effects.
    Read,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct Evidence {
    kind: CommandKind,
    dispatched: usize,
    writable_open: bool,
    compound: bool,
    last_request_effectful: bool,
    http_status: Option<u16>,
    retry_after: Option<String>,
}

tokio::task_local! {
    static COMMAND: Arc<Mutex<Evidence>>;
}

/// The kind of a command whose failure reports an outcome. Commands with
/// their own failure reports (`lint`, `queries validate`, cluster and managed
/// commands) and maintenance report none yet. The match is exhaustive, so a
/// new command declares how its failure reads.
pub(crate) fn kind(cli: &Cli) -> Option<CommandKind> {
    use CommandKind::{Read, Write};
    match &cli.command {
        // An alias invokes a stored query as a read; the server refuses a
        // mutation behind one before it runs.
        Command::Query(_)
        | Command::Alias(_)
        | Command::Snapshot(_)
        | Command::Export(_)
        | Command::Diff(_)
        | Command::Changes { .. } => Some(Read),
        Command::Load(_) | Command::Mutate(_) => Some(Write),
        Command::Branch { command, .. } => Some(match command {
            BranchCommand::Create { .. }
            | BranchCommand::Delete { .. }
            | BranchCommand::Merge { .. } => Write,
            BranchCommand::List { .. }
            | BranchCommand::Diff { .. }
            | BranchCommand::MergeBase { .. } => Read,
        }),
        Command::Commit { command, .. } => Some(match command {
            CommitCommand::Publish { .. } | CommitCommand::Drop { .. } => Write,
            CommitCommand::List { .. }
            | CommitCommand::Show { .. }
            | CommitCommand::Changes { .. } => Read,
        }),
        Command::Schema { command, .. } => match command {
            SchemaCommand::Apply { .. } => Some(Write),
            SchemaCommand::Plan { .. } | SchemaCommand::Show { .. } => Some(Read),
            SchemaCommand::UpgradeSystemColumns { .. } => None,
        },
        Command::Blob { command, .. } => Some(match command {
            BlobCommand::Put { .. } | BlobCommand::Clear { .. } => Write,
            BlobCommand::Get { .. } | BlobCommand::Stat { .. } => Read,
        }),
        Command::Queries { command, .. } => match command {
            QueriesCommand::List { .. } => Some(Read),
            QueriesCommand::Validate { .. } => None,
        },
        Command::Graphs { command, .. } => match command {
            GraphsCommand::List { .. } => Some(Read),
        },
        Command::Organization { .. }
        | Command::Init(_)
        | Command::Upgrade(_)
        | Command::Optimize(_)
        | Command::RebuildFullTextIndexes(_)
        | Command::Repair(_)
        | Command::Cleanup(_)
        | Command::Lint(_)
        | Command::Cluster { .. }
        | Command::Policy { .. }
        | Command::Embed(_)
        | Command::Login { .. }
        | Command::Logout { .. }
        | Command::Use { .. }
        | Command::Profile { .. }
        | Command::Skill { .. }
        | Command::Version { .. } => None,
    }
}

/// Commands whose requests are observed without a classified outcome: a
/// served cluster command reports its own failures, but a refused `--as`
/// assertion still exits as the condition it is.
pub(crate) fn observed_unclassified(cli: &Cli) -> bool {
    matches!(&cli.command, Command::Cluster { .. })
}

/// Whether `error` is a refused `--as` assertion and nothing the command sent
/// before it could have had effects. The server refuses the assertion before
/// any effect whatever the method, so the refused request ran nothing.
pub(crate) fn actor_assertion_refused(error: &Report, evidence: &Evidence) -> bool {
    error
        .downcast_ref::<RemoteErrorCli>()
        .is_some_and(|remote| {
            remote.status == StatusCode::PRECONDITION_FAILED
                && remote.output.code == Some(ErrorCode::ActorMismatch)
        })
        && !evidence.writable_open
        && (evidence.dispatched == 0
            || (evidence.dispatched == 1 && evidence.last_request_effectful))
}

pub(crate) async fn observe<F: std::future::Future>(
    kind: CommandKind,
    future: F,
) -> (F::Output, Evidence) {
    let evidence = Arc::new(Mutex::new(Evidence {
        kind,
        ..Evidence::default()
    }));
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

/// An embedded write session opened. Only the engine write's own error
/// ([`EngineWriteError`]) can then say that nothing ran; any other later
/// error, such as one rendering a committed receipt, must not claim it.
pub(crate) fn writable_open() {
    update(|state| state.writable_open = true);
}

/// The command runs an engine write that may publish more than once, as a
/// load that forks its branch does, so its refusal may follow an earlier
/// publication.
pub(crate) fn compound() {
    update(|state| state.compound = true);
}

/// An embedded engine write's own error. The engine refuses with a typed,
/// settled error before it publishes and marks a publication in doubt, so
/// this error carries what the write did, unlike any error after it.
#[derive(Debug)]
pub(crate) struct EngineWriteError(pub(crate) omnigraph::error::OmniError);

impl std::fmt::Display for EngineWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for EngineWriteError {}

/// Mark an embedded engine write's result as that write's own.
pub(crate) trait EngineWrite<T> {
    fn engine_write(self) -> color_eyre::Result<T>;
}

impl<T> EngineWrite<T> for std::result::Result<T, omnigraph::error::OmniError> {
    fn engine_write(self) -> color_eyre::Result<T> {
        self.map_err(|error| EngineWriteError(error).into())
    }
}

/// The failure the CLI prints for a command: the server's error body, its
/// `outcome` reconciled with what this command's own requests show, and the
/// transport facts beside it.
#[derive(Debug, Serialize)]
pub(crate) struct Failure {
    #[serde(flatten)]
    pub(crate) output: ErrorOutput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retry_after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) request_dispatched: Option<bool>,
    /// The CLI's account of a refused request that carried no credential.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) missing_credential: Option<MissingCredential>,
    #[serde(skip)]
    pub(crate) exit: i32,
}

impl Failure {
    /// The whole command's outcome, which `classify` always sets.
    pub(crate) fn outcome(&self) -> OutcomeOutput {
        self.output
            .outcome
            .expect("a classified failure carries its outcome")
    }

    pub(crate) fn classify(error: Report, evidence: Evidence) -> Self {
        let remote = error.downcast_ref::<RemoteErrorCli>();
        let precondition = error.downcast_ref::<PreconditionFailedCli>();
        let read = evidence.kind == CommandKind::Read;
        let engine_write = error.downcast_ref::<EngineWriteError>().is_some();
        // An embedded write's own refusal, before any request, says what the
        // engine did, projected with what the request path would know.
        let embedded = evidence.dispatched == 0;
        let embedded_precondition =
            precondition.is_some_and(|value| value.http_status.is_none()) && embedded;
        // A refusal answers the whole command when its request was the
        // command's only effectful one, or when the command is a read.
        let single = read
            || (evidence.dispatched == 1
                && evidence.last_request_effectful
                && !evidence.writable_open);
        // A refused `--as` assertion is a condition the caller set, like a
        // graph-commit precondition: nothing ran.
        let actor_mismatch = actor_assertion_refused(&error, &evidence);
        let conditional = (precondition.is_some_and(|value| value.http_status == Some(412))
            && single)
            || embedded_precondition
            || actor_mismatch;
        let admission = remote
            .filter(|remote| {
                single && !remote.output.error.trim().is_empty() && is_plain_refusal(&remote.output)
            })
            .and_then(|remote| admission_refusal(remote.status, remote.output.code.as_ref()));
        let retry = matches!(admission, Some(Action::Retry));
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
        let missing_credential = remote.and_then(|remote| remote.missing_credential.clone());
        // A server states the outcome of the one request it answered; that is
        // the whole command's outcome only when that request was the
        // command's sole effectful one.
        let served = remote
            .filter(|_| single)
            .and_then(|remote| remote.output.outcome)
            .or_else(|| {
                precondition
                    .filter(|_| single)
                    .and_then(|value| value.output.outcome)
            });
        let output = if let Some(remote) = remote {
            remote.output.clone()
        } else if let Some(precondition) = precondition {
            precondition.output.clone()
        } else if let Some(contract) = contract {
            let mut output = ErrorOutput::message(contract.error.clone());
            output.code = Some(contract.code);
            output
        } else {
            match error.downcast::<EngineWriteError>() {
                Ok(EngineWriteError(engine)) => embedded_write_output(engine, evidence.compound),
                Err(error) => match error.downcast::<omnigraph::error::OmniError>() {
                    // A read changes nothing, so its error is never an unknown
                    // outcome.
                    Ok(engine) if read => omnigraph_server::engine_read_error_output(engine),
                    Ok(engine) => omnigraph_server::engine_error_output(engine),
                    // Keep the cause chain the stderr report used to print.
                    Err(error) => crate::error_output_of(&error)
                        .unwrap_or_else(|| ErrorOutput::message(format!("{error:#}"))),
                },
            }
        };
        let served = served.or_else(|| output.outcome.filter(|_| engine_write && embedded));
        let not_started = read
            || admission.is_some()
            || conditional
            || (evidence.dispatched == 0 && !evidence.writable_open);
        let action = if retry {
            Action::Retry
        } else if output.recovery_required.is_some()
            || output.full_text_index_rebuild_required.is_some()
            || output.full_text_index_required.is_some()
        {
            Action::Recover
        } else if not_started
            || output.precondition_failure.is_some()
            || output.blob_precondition_failure.is_some()
            || output.read_set_conflict.is_some()
            || output.published_dataset_version_conflict.is_some()
            || output.key_conflict.is_some()
            || output.resource_limit.is_some()
            || output.external_blob_source.is_some()
            || !output.merge_conflicts.is_empty()
            || output.diagnostic.is_some()
        {
            Action::Refresh
        } else {
            Action::Reconcile
        };
        let outcome = served.unwrap_or(OutcomeOutput {
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
        });
        let exit = exit_for(&outcome, conditional);
        let mut output = output;
        output.outcome = Some(outcome);
        Self {
            output,
            http_status,
            retry_after,
            request_dispatched,
            missing_credential,
            exit,
        }
    }
}

/// An embedded write's own error, rendered as the request path renders it.
/// A policy refusal precedes every publication of the write, a load's fork
/// included, as the server's authorization precedes its owned write. Any
/// other error of a compound write follows the fork that published.
fn embedded_write_output(engine: omnigraph::error::OmniError, compound: bool) -> ErrorOutput {
    if matches!(engine, omnigraph::error::OmniError::Policy(_)) {
        omnigraph_server::engine_error_output_after(engine, false, false)
    } else if compound {
        omnigraph_server::engine_error_output_after(engine, true, true)
    } else {
        omnigraph_server::engine_error_output(engine)
    }
}

/// The exit a failure's outcome selects: busy and safe to retry (75), a
/// condition the caller set (4), effects unknown (2), or not started (1).
fn exit_for(outcome: &OutcomeOutput, conditional: bool) -> i32 {
    if outcome.execution == Execution::NotStarted && outcome.action == Action::Retry {
        crate::EXIT_RETRY
    } else if conditional {
        crate::EXIT_PRECONDITION_FAILED
    } else if outcome.execution == Execution::Unknown {
        crate::EXIT_OUTCOME_UNKNOWN
    } else {
        1
    }
}

/// The typed refusals the server sends before it admits a write, each code
/// under its own status. A busy actor may retry the whole command; failed
/// authentication, an unsupported contract, a refused `--as` assertion or a
/// policy denial needs a changed credential, client or grant first.
fn admission_refusal(status: StatusCode, code: Option<&ErrorCode>) -> Option<Action> {
    match (status, code?) {
        (StatusCode::TOO_MANY_REQUESTS, ErrorCode::TooManyRequests) => Some(Action::Retry),
        (StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized)
        | (StatusCode::BAD_REQUEST, ErrorCode::ApiContractMismatch)
        | (StatusCode::PRECONDITION_FAILED, ErrorCode::ActorMismatch)
        | (StatusCode::FORBIDDEN, ErrorCode::Forbidden) => Some(Action::Refresh),
        _ => None,
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
        && output.blob_precondition_failure.is_none()
        && output.change_feed_gap.is_none()
        && output.change_diff_refusal.is_none()
        && output.full_text_index_rebuild_required.is_none()
        && output.full_text_index_required.is_none()
        && output.diagnostic.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph::error::OmniError;

    fn embedded_write(compound: bool) -> Evidence {
        Evidence {
            kind: CommandKind::Write,
            writable_open: true,
            compound,
            ..Evidence::default()
        }
    }

    fn outcome(failure: &Failure) -> serde_json::Value {
        serde_json::to_value(failure.output.outcome.as_ref().unwrap()).unwrap()
    }

    /// An embedded load's policy refusal ran nothing, its fork included: the
    /// engine checks policy before any publication. Any other refusal of a
    /// compound load follows the fork that published.
    #[test]
    fn an_embedded_compound_write_reports_what_the_engine_did() {
        for compound in [false, true] {
            let refused = Failure::classify(
                EngineWriteError(OmniError::Policy("change denied".into())).into(),
                embedded_write(compound),
            );
            assert_eq!(refused.exit, 1, "compound {compound}");
            assert_eq!(refused.output.code, Some(ErrorCode::Forbidden));
            assert_eq!(
                outcome(&refused),
                serde_json::json!({"execution":"not_started","effects":"none","action":"refresh"}),
                "compound {compound}"
            );
        }
        let alone = Failure::classify(
            EngineWriteError(OmniError::manifest("not valid graph NDJSON")).into(),
            embedded_write(false),
        );
        assert_eq!(outcome(&alone)["execution"], "not_started");
        assert_eq!(alone.exit, 1);
        let after_fork = Failure::classify(
            EngineWriteError(OmniError::manifest("not valid graph NDJSON")).into(),
            embedded_write(true),
        );
        assert_eq!(outcome(&after_fork)["execution"], "unknown");
        assert_eq!(after_fork.exit, crate::EXIT_OUTCOME_UNKNOWN);
    }

    /// A read changes nothing, so an engine error is never an unknown outcome.
    #[test]
    fn an_embedded_read_error_keeps_its_own_code() {
        let failure = Failure::classify(
            OmniError::manifest_internal("historical published dataset version 1 was reclaimed")
                .into(),
            Evidence {
                kind: CommandKind::Read,
                ..Evidence::default()
            },
        );
        assert_ne!(failure.output.code, Some(ErrorCode::OutcomeUnknown));
        assert_eq!(outcome(&failure)["execution"], "not_started");
        assert_eq!(failure.exit, 1);
    }
}
