//! Whole-command guidance for data writes. Substrate retryability is not
//! permission to repeat a command, especially one that can create a branch.

use color_eyre::eyre::Report;
use omnigraph::error::{ManifestConflictDetails, OmniError};
use omnigraph_api_types::{ErrorCode, ErrorOutput};
use serde::Serialize;

use crate::helpers::{PreconditionFailedCli, RemoteErrorCli};

pub(crate) const EXIT_RETRY_SAFE: i32 = 75;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteContext {
    Single,
    /// Standalone append/merge load, without a preceding branch creation.
    RepreparableLoad,
    Compound,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Execution {
    NotStarted,
    /// An engine call or server response returned; not an external-I/O fence.
    Returned,
    Unknown,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Effects {
    /// No requested logical effect. Reclaimable staging and recovery of an
    /// older operation are outside this statement.
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

impl CommandOutcome {
    fn unknown(execution: Execution) -> Self {
        Self {
            execution,
            effects: Effects::Unknown,
            action: Action::Reconcile,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Details {
    Structured(Box<ErrorOutput>),
    Message { error: String },
}

#[derive(Debug, Serialize)]
pub(crate) struct DataCommandFailure {
    #[serde(flatten)]
    details: Details,
    command_outcome: CommandOutcome,
}

impl DataCommandFailure {
    /// Writable open may finish an older operation. Preserve its typed
    /// evidence, but never classify the entire command as effect-free.
    pub(crate) fn opening(error: OmniError) -> Report {
        Self::engine(error, WriteContext::Compound)
    }

    pub(crate) fn exit_code(&self) -> i32 {
        if matches!(self.command_outcome.action, Action::Retry) {
            EXIT_RETRY_SAFE
        } else {
            1
        }
    }

    /// Atomic contexts apply only to the actual engine write. Open/recovery
    /// and commands with earlier effects require the conservative Compound context.
    pub(crate) fn engine(error: OmniError, context: WriteContext) -> Report {
        let mut outcome = CommandOutcome::unknown(Execution::Returned);
        if matches!(error, OmniError::RecoveryRequired { .. }) {
            outcome.action = Action::Recover;
        } else if context != WriteContext::Compound
            && (matches!(error, OmniError::RetryableCommitConflict(_))
                || matches!(
                    &error,
                    OmniError::Manifest(error)
                        if matches!(error.details, Some(ManifestConflictDetails::ReadSetChanged { .. }))
                ))
        {
            outcome.effects = Effects::None;
            outcome.action = if context == WriteContext::RepreparableLoad {
                Action::Retry
            } else {
                Action::Refresh
            };
        }
        Self {
            details: Details::Structured(Box::new(
                omnigraph_server::ApiError::from_omni(error).into_output(),
            )),
            command_outcome: outcome,
        }
        .into()
    }

    /// A request has been dispatched. Loss of headers, body or a decodable
    /// result cannot prove that execution stopped or that no write occurred.
    pub(crate) fn remote(error: Report, context: WriteContext) -> Report {
        // Preserve the existing caller-precondition contract and exit code 4.
        if error.is::<PreconditionFailedCli>() || error.is::<Self>() {
            return error;
        }
        let (details, outcome) = if let Some(remote) = error.downcast_ref::<RemoteErrorCli>() {
            let output = &remote.output;
            let mut outcome = CommandOutcome::unknown(Execution::Returned);
            if output.recovery_required.is_some() {
                outcome.action = Action::Recover;
            } else if context != WriteContext::Compound
                && remote.status == reqwest::StatusCode::TOO_MANY_REQUESTS
                && output.code == Some(ErrorCode::TooManyRequests)
                && remote.plain_admission_refusal
                && no_effect_details(output, false)
            {
                outcome = CommandOutcome {
                    execution: Execution::NotStarted,
                    effects: Effects::None,
                    action: Action::Retry,
                };
            }
            (Details::Structured(Box::new(output.clone())), outcome)
        } else {
            (
                Details::Message {
                    error: error.to_string(),
                },
                CommandOutcome::unknown(Execution::Unknown),
            )
        };
        // Keep the original typed cause reachable through eyre's context
        // chain (for example, request timeouts and server policy refusals).
        error.wrap_err(Self {
            details,
            command_outcome: outcome,
        })
    }
}

// An inconsistent response must not gain retry permission merely by also
// carrying a 429 code. New/unknown server error codes fail decoding closed.
pub(crate) fn only_precondition_detail(output: &ErrorOutput) -> bool {
    output.precondition_failure.is_some()
        && matches!(output.code, None | Some(ErrorCode::Conflict))
        && no_effect_details(output, true)
}

fn no_effect_details(output: &ErrorOutput, allow_precondition: bool) -> bool {
    output.merge_conflicts.is_empty()
        && output.published_dataset_version_conflict.is_none()
        && output.read_set_conflict.is_none()
        && output.key_conflict.is_none()
        && output.resource_limit.is_none()
        && output.blob_range.is_none()
        && output.external_blob_source.is_none()
        && output.recovery_required.is_none()
        && (allow_precondition || output.precondition_failure.is_none())
        && output.change_feed_gap.is_none()
        && output.change_diff_refusal.is_none()
        && output.full_text_index_rebuild_required.is_none()
}

impl std::fmt::Display for DataCommandFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match &self.details {
            Details::Structured(output) => &output.error,
            Details::Message { error } => error,
        };
        let guidance = match self.command_outcome.action {
            Action::Retry => {
                "No requested logical effect; a bounded retry with unchanged preconditions is permitted."
            }
            Action::Refresh => {
                "Re-read current state and reconsider the request; do not discard caller preconditions."
            }
            Action::Recover => {
                "Resolve the named recovery operation and reconcile this command before resubmitting."
            }
            Action::Reconcile => {
                "Command effects are unconfirmed; reconcile the original request before resubmitting."
            }
        };
        write!(f, "{message}\n{guidance}")
    }
}

impl std::error::Error for DataCommandFailure {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_command_scope_controls_retry_issue_466() {
        for (context, effects, action, exit) in [
            (WriteContext::RepreparableLoad, "none", "retry", 75),
            (WriteContext::Single, "none", "refresh", 1),
            (WriteContext::Compound, "unknown", "reconcile", 1),
        ] {
            for error in [
                OmniError::RetryableCommitConflict("proved contention".into()),
                OmniError::manifest_read_set_changed("head", Some("a".into()), Some("b".into())),
            ] {
                let failure = DataCommandFailure::engine(error, context);
                let failure = failure.downcast_ref::<DataCommandFailure>().unwrap();
                let json = serde_json::to_value(failure).unwrap();
                assert_eq!(json["command_outcome"]["effects"], effects);
                assert_eq!(json["command_outcome"]["action"], action);
                assert_eq!(json["command_outcome"]["execution"], "returned");
                assert_eq!(failure.exit_code(), exit);
            }
            for (error, action) in [
                (
                    OmniError::recovery_required("original", "possibly published"),
                    "recover",
                ),
                (
                    OmniError::manifest_conflict("generic conflict"),
                    "reconcile",
                ),
            ] {
                let failure = DataCommandFailure::engine(error, context);
                let failure = failure.downcast_ref::<DataCommandFailure>().unwrap();
                let json = serde_json::to_value(failure).unwrap();
                assert_eq!(json["command_outcome"]["effects"], "unknown");
                assert_eq!(json["command_outcome"]["action"], action);
                assert_eq!(failure.exit_code(), 1);
            }
        }
    }

    #[test]
    fn writable_open_never_grants_retry_permission_issue_466() {
        let failure = DataCommandFailure::opening(OmniError::manifest_read_set_changed(
            "recovery",
            Some("a".into()),
            Some("b".into()),
        ));
        let failure = failure.downcast_ref::<DataCommandFailure>().unwrap();
        let json = serde_json::to_value(failure).unwrap();
        assert_eq!(failure.exit_code(), 1);
        assert_eq!(json["read_set_conflict"]["member"], "recovery");
        assert_eq!(json["command_outcome"]["effects"], "unknown");
        assert_eq!(json["command_outcome"]["action"], "reconcile");
    }
}
