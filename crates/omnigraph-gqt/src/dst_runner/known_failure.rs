use super::WorkerReport;
use crate::runner_config::{ErrorMatch, Execution, Storage};
use crate::{Case, Item, MutateExpect, Step};

pub(super) fn validate(case: &Case) -> Result<(), String> {
    let Some(marker) = &case.known_failure else {
        return Ok(());
    };
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
        || case.faults.is_empty()
        || case.faults.keys().any(|ordinal| *ordinal >= marker.step)
        || !case.items.iter().any(|item| {
            matches!(item, Item::Step(Step::Mutate(step))
                if step.ordinal == marker.step
                    && matches!(step.expect, MutateExpect::Ok | MutateExpect::Affected { .. }))
        })
    {
        return Err("invalid_case: known_failure requires an engine-DST/in-memory case without loops, a healthy mutate expectation at its step, and faults only at earlier steps".into());
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
    };
    if !matches {
        return Err(failure.clone());
    }
    let Some(message) = error["value"]["message"].as_str() else {
        return Err(failure.clone());
    };
    let assertion_message = format!("mutation failed: {message}");
    let Some(last) = assertions.last() else {
        return Err(failure.clone());
    };
    if last["value"]["message"] != assertion_message
        || *failure != format!("step {} (mutate): {assertion_message}", marker.step)
    {
        return Err(failure.clone());
    }
    let delivered = report
        .evidence
        .iter()
        .filter(|event| event["kind"] == "fault_delivered")
        .map(|event| {
            (
                event["operation"]["ordinal"].as_u64(),
                event["value"]["hook"].as_str(),
            )
        })
        .collect::<Vec<_>>();
    let required = case
        .faults
        .iter()
        .map(|(ordinal, fault)| (u64::try_from(*ordinal).ok(), Some(fault.at.as_str())))
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
