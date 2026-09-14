use super::WorkerReport;
use crate::runner_config::{ErrorMatch, Execution, Storage};
use crate::{Case, Item, MutateExpect, Step};

/// The step a marker may name: an ordinary mutate carrying its healthy
/// expectation, or a restart whose reopen is expected to refuse.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MarkedStep {
    Mutate,
    Restart,
}

fn marked_step(case: &Case, ordinal: usize) -> Option<MarkedStep> {
    case.items.iter().find_map(|item| match item {
        Item::Step(Step::Mutate(step))
            if step.ordinal == ordinal
                && matches!(
                    step.expect,
                    MutateExpect::Ok | MutateExpect::Affected { .. }
                ) =>
        {
            Some(MarkedStep::Mutate)
        }
        Item::Step(Step::Restart { ordinal: at }) if *at == ordinal => Some(MarkedStep::Restart),
        _ => None,
    })
}

pub(super) fn validate(case: &Case) -> Result<(), String> {
    let Some(marker) = &case.known_failure else {
        return Ok(());
    };
    let step = marked_step(case, marker.step);
    let matcher_fits = matches!(
        (&marker.matcher, step),
        (
            ErrorMatch::RecoveryRequired { .. },
            Some(MarkedStep::Mutate)
        ) | (ErrorMatch::Internal { .. }, Some(MarkedStep::Restart))
    );
    if case.has_loops()
        || !case.runner.environments.iter().all(|environment| {
            matches!(
                environment.execution,
                Execution::Dst {
                    storage: Storage::InMemoryObjectStore,
                    ..
                }
            )
        })
        || case.seams.is_empty()
        || case.seams.keys().any(|ordinal| *ordinal >= marker.step)
        || !matcher_fits
    {
        return Err("invalid_case: known_failure requires an engine-DST/in-memory case without loops, seams only at earlier steps, and either a RecoveryRequired matcher on a mutate with a healthy expectation or an Internal matcher on a restart".into());
    }
    Ok(())
}

pub(super) fn classify(case: &Case, report: &WorkerReport) -> Result<bool, String> {
    let Some(marker) = &case.known_failure else {
        return report.result.clone().map(|()| false);
    };
    let Err(failure) = &report.result else {
        return Err(format!(
            "unexpected_pass: known_failure step {} passed; remove the stale marker after verifying the fix",
            marker.step
        ));
    };
    if report.code != "assertion_failed" || report.phase != "execution" {
        return Err(failure.clone());
    }
    let assertions = report
        .evidence
        .iter()
        .filter(|event| event["kind"] == "assertion")
        .collect::<Vec<_>>();
    if assertions.len() != marker.step
        || assertions.iter().enumerate().any(|(index, event)| {
            event["operation"]["ordinal"].as_u64() != u64::try_from(index + 1).ok()
                || !event["operation"]["loop_binding"].is_null()
                || event["value"]["status"]
                    != if index + 1 == marker.step {
                        "failed"
                    } else {
                        "passed"
                    }
        })
    {
        return Err(failure.clone());
    }
    let errors = report
        .evidence
        .iter()
        .filter(|event| {
            event["kind"] == "typed_error"
                && event["operation"]["ordinal"].as_u64() == u64::try_from(marker.step).ok()
        })
        .collect::<Vec<_>>();
    let [error] = errors.as_slice() else {
        return Err(failure.clone());
    };
    let matches = match &marker.matcher {
        ErrorMatch::RecoveryRequired { reason } => {
            error["value"]["error"] == "RecoveryRequired"
                && error["value"]["reason"].as_str() == Some(reason.as_str())
        }
        ErrorMatch::Internal { reason_prefix } => {
            error["value"]["error"] == "Manifest"
                && error["value"]["kind"] == "Internal"
                && error["value"]["reason"]
                    .as_str()
                    .is_some_and(|reason| reason.starts_with(reason_prefix.as_str()))
        }
    };
    if !matches {
        return Err(failure.clone());
    }
    let Some(message) = error["value"]["message"].as_str() else {
        return Err(failure.clone());
    };
    let (assertion_message, label) = match marked_step(case, marker.step) {
        Some(MarkedStep::Mutate) => (format!("mutation failed: {message}"), "mutate"),
        Some(MarkedStep::Restart) => (format!("reopen failed: {message}"), "restart"),
        None => return Err(failure.clone()),
    };
    let Some(last) = assertions.last() else {
        return Err(failure.clone());
    };
    if last["value"]["message"] != assertion_message
        || *failure != format!("step {} ({label}): {assertion_message}", marker.step)
    {
        return Err(failure.clone());
    }
    let delivered = report
        .evidence
        .iter()
        .filter(|event| event["kind"] == "seam_delivered")
        .map(|event| {
            (
                event["operation"]["ordinal"].as_u64(),
                event["value"]["at"].as_str().map(str::to_string),
                event["value"]["occurrence"].as_u64(),
            )
        })
        .collect::<Vec<_>>();
    let required = case
        .seams
        .iter()
        .map(|(ordinal, seam)| {
            (
                u64::try_from(*ordinal).ok(),
                Some(seam.at.clone()),
                u64::try_from(seam.occurrence).ok(),
            )
        })
        .collect::<Vec<_>>();
    if delivered != required {
        return Err(failure.clone());
    }
    Ok(true)
}

pub(super) fn verify_status(
    case: &Case,
    report: &WorkerReport,
    known_failure: bool,
) -> Result<(), String> {
    if classify(case, report).unwrap_or(false) != known_failure {
        return Err("report_failed: inconsistent known_failure status".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
