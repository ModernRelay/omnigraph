use super::*;
use omnigraph::error::OmniError;
use serde_json::json;

const CASE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/cases/dst_mutation_failure_keeps_writing.gqt"
));

fn fixture() -> (Case, WorkerReport) {
    let case = crate::parse_case("dst_mutation_failure_keeps_writing", CASE).unwrap();
    let marker = case.known_failure.as_ref().unwrap();
    let ErrorMatch::RecoveryRequired { reason } = &marker.matcher;
    let error = OmniError::RecoveryRequired {
        operation_id: "operation-17".into(),
        reason: reason.clone(),
    };
    let message = format!("mutation failed: {error}");
    let mut evidence = Vec::new();
    for ordinal in 1..=marker.step {
        let operation = json!({"ordinal": ordinal, "loop_binding": null});
        if let Some(fault) = case.faults.get(&ordinal) {
            evidence.push(json!({"kind": "fault_delivered", "operation": operation, "value": {"hook": fault.at}}));
        }
        if ordinal == marker.step {
            evidence.push(json!({"kind": "typed_error", "operation": operation, "value": {"error": "RecoveryRequired", "reason": reason, "operation_id": "operation-17", "message": error.to_string()}}));
        }
        let value = if ordinal == marker.step {
            json!({"status": "failed", "code": "assertion_failed", "message": message})
        } else {
            json!({"status": "passed"})
        };
        evidence.push(json!({"kind": "assertion", "operation": operation, "value": value}));
    }
    let report = WorkerReport {
        code: "assertion_failed".into(),
        phase: "execution".into(),
        input_digest: "frozen-input".into(),
        result: Err(format!("step {} (mutate): {message}", marker.step)),
        observations: vec![],
        evidence,
    };
    (case, report)
}

#[test]
fn accepts_only_the_exact_typed_recovery_failure_and_keeps_raw_evidence() {
    let (case, report) = fixture();
    let raw = serde_json::to_vec(&report).unwrap();
    assert_eq!(classify(&case, &report), Ok(true));
    assert_eq!(serde_json::to_vec(&report).unwrap(), raw);
    verify_status(&case, &report, true).unwrap();
    assert!(verify_status(&case, &report, false).is_err());
}

#[test]
fn refuses_different_reasons_steps_faults_and_incomplete_assertions() {
    for mutation in 0..10 {
        let (case, mut report) = fixture();
        match mutation {
            0 => report
                .evidence
                .retain(|event| event["kind"] != "typed_error"),
            1 => report
                .evidence
                .retain(|event| event["kind"] != "fault_delivered"),
            2 => {
                let error = report
                    .evidence
                    .iter_mut()
                    .find(|e| e["kind"] == "typed_error")
                    .unwrap();
                error["value"]["reason"] = "another recovery problem".into();
            }
            3 => {
                let error = report
                    .evidence
                    .iter_mut()
                    .find(|e| e["kind"] == "typed_error")
                    .unwrap();
                error["operation"]["ordinal"] = 3.into();
            }
            4 => {
                let first = report
                    .evidence
                    .iter_mut()
                    .find(|e| e["kind"] == "assertion")
                    .unwrap();
                first["value"]["status"] = "failed".into();
            }
            5 => {
                let fault = report
                    .evidence
                    .iter_mut()
                    .find(|e| e["kind"] == "fault_delivered")
                    .unwrap();
                fault["value"]["hook"] = "another.hook".into();
            }
            6 => report.result = Err("step 4 (mutate): affected counts mismatch".into()),
            7 => {
                let fault = report
                    .evidence
                    .iter()
                    .find(|e| e["kind"] == "fault_delivered")
                    .unwrap()
                    .clone();
                report.evidence.push(fault);
            }
            8 => {
                let error = report
                    .evidence
                    .iter_mut()
                    .find(|e| e["kind"] == "typed_error")
                    .unwrap();
                error["value"]["error"] = "Manifest".into();
            }
            9 => {
                let error = report
                    .evidence
                    .iter_mut()
                    .find(|e| e["kind"] == "typed_error")
                    .unwrap();
                error["kind"] = "query_result".into();
            }
            _ => unreachable!(),
        }
        assert!(
            classify(&case, &report).is_err(),
            "accepted mutation {mutation}"
        );
        assert!(verify_status(&case, &report, true).is_err());
    }
}

#[test]
fn never_waives_harness_errors_or_an_unexpected_pass() {
    for code in [
        "worker_failed",
        "timeout",
        "fault_cleanup_failed",
        "report_failed",
        "fault_unobserved",
    ] {
        let (case, mut report) = fixture();
        report.code = code.into();
        report.result = Err(format!("{code}: original cause"));
        assert_eq!(
            classify(&case, &report),
            Err(format!("{code}: original cause"))
        );
    }
    let (case, mut report) = fixture();
    report.phase = "setup".into();
    assert!(classify(&case, &report).is_err());
    report.result = Ok(());
    report.code = "passed".into();
    assert!(
        classify(&case, &report)
            .unwrap_err()
            .starts_with("unexpected_pass:")
    );
    assert!(verify_status(&case, &report, true).is_err());
}

#[test]
fn marker_is_closed_and_cannot_replace_healthy_expectations() {
    for text in [
        CASE.replace("step: 4", "step: 0"),
        CASE.replace("step: 4", "step: 99"),
        CASE.replace("step: 4", "step: 3"),
        CASE.replace("step: 4", "step: 4\nunknown: value"),
        CASE.replace("step: 4", "step: 4\nstep: 4"),
        CASE.replace("--- known_failure", "--- known_failure ignored"),
        CASE.replace("--- known_failure", "--- fixme"),
        CASE.replace("error: RecoveryRequired", "error: Unknown"),
        CASE.replace("  error: RecoveryRequired\n", ""),
        CASE.replace("match:\n", "match: {}\n"),
        CASE.replace("  reason:", "  other_reason:"),
        CASE.replace("match:\n", "match:\n  reason: \"\"\n"),
        CASE.replace("error: RecoveryRequired", "error: Manifest"),
        CASE.replace("error: RecoveryRequired", "error: recovery_required"),
        CASE.replace("error: RecoveryRequired", "error: RecoveryRequired\n  error: RecoveryRequired"),
        CASE.replace("error: RecoveryRequired", "error: RecoveryRequired\n  unknown: value"),
        CASE.replace("--- known_failure", "--- known_failure\nnotes: \"\""),
        CASE.replace("--- expect affected: nodes=1 edges=0", "--- expect error: recovery required"),
        CASE.replace("--- fault\nat: mutation.post_sidecar_pre_fork\noccurrence: 1\naction: return_error\nscope: next_step\n", ""),
        CASE.replace("target: omnigraph-engine-dst", "target: omnigraph-engine"),
    ] {
        assert!(crate::parse_case("dst_mutation_failure_keeps_writing", &text).is_err(), "accepted {text}");
    }
}

#[tokio::test]
async fn typed_error_evidence_comes_from_the_engine_variant() {
    let error = OmniError::RecoveryRequired {
        operation_id: "operation-17".into(),
        reason: "pending typed recovery".into(),
    };
    let report = super::super::capture("input".into(), async {
        super::super::begin_operation(json!({"ordinal": 4}));
        super::super::observe_fault(&error);
        Err("assertion failed".into())
    })
    .await
    .unwrap();
    let typed = report
        .evidence
        .iter()
        .find(|event| event["kind"] == "typed_error")
        .unwrap();
    assert_eq!(typed["value"]["error"], "RecoveryRequired");
    assert_eq!(typed["value"]["operation_id"], "operation-17");
    assert_eq!(typed["value"]["reason"], "pending typed recovery");
    assert_eq!(typed["value"]["message"], error.to_string());
}

#[tokio::test]
async fn scenario_panic_cannot_become_a_known_failure() {
    let (case, _) = fixture();
    let report = super::super::capture("input".into(), async {
        super::super::begin_operation(json!({"ordinal": 4}));
        panic!("recovery required for operation 17: pending typed recovery");
    })
    .await
    .unwrap();
    assert_eq!(report.code, "worker_failed");
    assert!(classify(&case, &report).is_err());
}
