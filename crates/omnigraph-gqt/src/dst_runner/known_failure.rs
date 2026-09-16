use omnigraph_dst::store_places::StoreAction;

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
    let deliveries = report
        .evidence
        .iter()
        .filter(|event| event["kind"] == "seam_delivered")
        .collect::<Vec<_>>();
    let delivered = deliveries
        .iter()
        .map(|event| {
            (
                event["operation"]["ordinal"].as_u64(),
                event["value"]["at"].as_str().map(str::to_string),
                event["value"]["occurrence"].as_u64(),
                event["value"]["effect"].as_str().map(str::to_string),
                event["value"]["subject"].as_str().map(str::to_string),
            )
        })
        .collect::<Vec<_>>();
    let seams = case
        .seams
        .iter()
        .flat_map(|(ordinal, seams)| seams.iter().map(move |seam| (*ordinal, seam)))
        .collect::<Vec<_>>();
    let required = seams
        .iter()
        .map(|(ordinal, seam)| {
            let admitted = super::resolve_seam(seam).ok();
            (
                u64::try_from(*ordinal).ok(),
                Some(seam.at.clone()),
                u64::try_from(seam.occurrence).ok(),
                admitted
                    .as_ref()
                    .map(|admitted| admitted.effect_name().to_string()),
                match &admitted {
                    Some(super::Admitted::CodeStore(entry, _)) => {
                        entry.store_subject().map(str::to_string)
                    }
                    _ => seam.subject.clone(),
                },
            )
        })
        .collect::<Vec<_>>();
    if delivered != required {
        return Err(failure.clone());
    }
    for ((_, seam), event) in seams.iter().zip(&deliveries) {
        let admitted = super::resolve_seam(seam).map_err(|_| failure.clone())?;
        if !hit_is_valid(&admitted, &event["value"]["hit"]) {
            return Err(failure.clone());
        }
    }
    Ok(true)
}

/// A store delivery carries one complete hit: a method of its row, a
/// requested name the subject selects, and for `misdirect` the stored name
/// the transform produces. An engine effect carries none.
fn hit_is_valid(admitted: &super::Admitted, hit: &serde_json::Value) -> bool {
    let Some(row) = admitted.store_row() else {
        return hit.is_null();
    };
    let (Some(method), Some(requested)) = (hit["method"].as_str(), hit["requested"].as_str())
    else {
        return false;
    };
    if !row.methods.contains(&method) {
        return false;
    }
    let (action, subject) = match admitted {
        super::Admitted::Store(_, action, subject) => (*action, Some(subject.clone())),
        super::Admitted::CodeStore(entry, omnigraph::seams::StoreEffect::Misdirect) => (
            StoreAction::Misdirect,
            entry
                .store_subject()
                .and_then(|declared| omnigraph_dst::store_places::Subject::parse(declared).ok()),
        ),
        super::Admitted::Code(..) => return false,
    };
    if subject.is_some_and(|subject| !subject.matches(requested)) {
        return false;
    }
    match action {
        StoreAction::Misdirect => {
            hit["stored"].as_str()
                == Some(omnigraph_dst::store_places::misdirect_uri(requested).as_str())
        }
        StoreAction::Lose | StoreAction::Error | StoreAction::Corrupt | StoreAction::Delay => {
            hit["stored"].is_null()
        }
    }
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
