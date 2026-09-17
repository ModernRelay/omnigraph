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
    let ErrorMatch::RecoveryRequired { reason } = &marker.matcher else {
        unreachable!("the fixture case names a RecoveryRequired marker")
    };
    let error = OmniError::RecoveryRequired {
        operation_id: "operation-17".into(),
        reason: reason.clone(),
    };
    let message = format!("mutation failed: {error}");
    let mut evidence = Vec::new();
    for ordinal in 1..=marker.step {
        let operation = json!({"ordinal": ordinal, "loop_binding": null});
        for seam in case.seams.get(&ordinal).into_iter().flatten() {
            evidence.push(json!({"kind": "seam_delivered", "operation": operation, "value": {"at": seam.at, "occurrence": seam.occurrence, "effect": "fail"}}));
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
fn checks_each_effect_when_multiple_seams_share_a_step() {
    let (mut case, mut report) = fixture();
    let seams = case.seams.values_mut().next().unwrap();
    let mut second = seams[0].clone();
    second.at = "mutation.sidecar_confirm_put".into();
    second.action = crate::runner_config::SeamAction::Skip;
    seams.push(second.clone());
    let first = report
        .evidence
        .iter()
        .position(|event| event["kind"] == "seam_delivered")
        .unwrap();
    let mut delivery = report.evidence[first].clone();
    delivery["value"]["at"] = second.at.into();
    delivery["value"]["effect"] = "skip".into();
    report.evidence.insert(first + 1, delivery);
    validate(&case).unwrap();
    assert_eq!(classify(&case, &report), Ok(true));
    verify_status(&case, &report, true).unwrap();

    for index in [first, first + 1] {
        let expected = report.evidence[index]["value"]["effect"].clone();
        for wrong in [json!("fail"), json!("skip"), serde_json::Value::Null] {
            if wrong == expected {
                continue;
            }
            report.evidence[index]["value"]["effect"] = wrong;
            assert!(classify(&case, &report).is_err());
            assert!(verify_status(&case, &report, true).is_err());
        }
        report.evidence[index]["value"]["effect"] = expected;
    }
    report.evidence.swap(first, first + 1);
    assert!(classify(&case, &report).is_err());
}

#[test]
fn refuses_different_reasons_steps_faults_and_incomplete_assertions() {
    for mutation in 0..12 {
        let (case, mut report) = fixture();
        match mutation {
            0 => report
                .evidence
                .retain(|event| event["kind"] != "typed_error"),
            1 => report
                .evidence
                .retain(|event| event["kind"] != "seam_delivered"),
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
                    .find(|e| e["kind"] == "seam_delivered")
                    .unwrap();
                fault["value"]["at"] = "another.seam".into();
            }
            6 => report.result = Err("step 4 (mutate): affected counts mismatch".into()),
            7 => {
                let fault = report
                    .evidence
                    .iter()
                    .find(|e| e["kind"] == "seam_delivered")
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
            10 => {
                let fault = report
                    .evidence
                    .iter_mut()
                    .find(|e| e["kind"] == "seam_delivered")
                    .unwrap();
                fault["value"]["effect"] = "skip".into();
            }
            11 => {
                let fault = report
                    .evidence
                    .iter_mut()
                    .find(|e| e["kind"] == "seam_delivered")
                    .unwrap();
                fault["value"].as_object_mut().unwrap().remove("effect");
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

const STORE_CASE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/cases/issue_601_foreign_named_sidecar_blocks_branch.gqt"
));

const STORE_MARKER: &str = "--- known_failure\nstep: 3\nmatch:\n  error: RecoveryRequired\n  reason: \"pending Mutation recovery operation blocks writes on branch 'main'\"\n\n--- schema\n";

/// The issue 601 case with a step-3 `known_failure` marker re-inserted (the
/// classifier needs one), as a decision-seam store effect or rewritten onto the
/// store place `storage.put` under `__recovery/*`, plus a one-delivery report.
fn store_fixture(store_place: bool) -> (Case, WorkerReport) {
    let text = STORE_CASE.replace("--- schema\n", STORE_MARKER);
    let text = if store_place {
        text.replace(
            "at: recovery.sidecar_write\n",
            "at: storage.put\nsubject: \"__recovery/*\"\n",
        )
    } else {
        text
    };
    let case = crate::parse_case("issue_601_foreign_named_sidecar_blocks_branch", &text).unwrap();
    let marker = case.known_failure.as_ref().unwrap();
    let ErrorMatch::RecoveryRequired { reason } = &marker.matcher else {
        unreachable!("the fixture case names a RecoveryRequired marker")
    };
    let error = OmniError::RecoveryRequired {
        operation_id: "op-1".into(),
        reason: reason.clone(),
    };
    let message = format!("mutation failed: {error}");
    let mut evidence = Vec::new();
    for ordinal in 1..=marker.step {
        let operation = json!({"ordinal": ordinal, "loop_binding": null});
        for seam in case.seams.get(&ordinal).into_iter().flatten() {
            let mut value = json!({"at": seam.at, "occurrence": seam.occurrence, "effect": "misdirect", "hit": {"method": "write_text", "requested": "__recovery/op-1.json", "stored": "__recovery/dstm-op-1.json"}});
            value["subject"] = json!(seam.subject.as_deref().unwrap_or("__recovery/*"));
            evidence
                .push(json!({"kind": "seam_delivered", "operation": operation, "value": value}));
        }
        if ordinal == marker.step {
            evidence.push(json!({"kind": "typed_error", "operation": operation, "value": {"error": "RecoveryRequired", "reason": reason, "operation_id": "op-1", "message": error.to_string()}}));
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
fn store_deliveries_need_one_complete_hit() {
    type Mutation = fn(&mut serde_json::Value);
    let mutations: [(&str, Mutation); 7] = [
        ("missing hit", |v| {
            v.as_object_mut().unwrap().remove("hit");
        }),
        ("hit without method", |v| {
            v["hit"].as_object_mut().unwrap().remove("method");
        }),
        ("method outside the row", |v| {
            v["hit"]["method"] = "delete".into();
        }),
        ("requested outside the subject", |v| {
            v["hit"]["requested"] = "data/x.lance".into();
            v["hit"]["stored"] = "data/dstm-x.lance".into();
        }),
        ("stored not the transform", |v| {
            v["hit"]["stored"] = "__recovery/op-1.json".into();
        }),
        ("stored in another directory", |v| {
            v["hit"]["stored"] = "other/dstm-op-1.json".into();
        }),
        ("subject dropped from the record", |v| {
            v.as_object_mut().unwrap().remove("subject");
        }),
    ];
    for store_place in [false, true] {
        let (case, report) = store_fixture(store_place);
        assert_eq!(
            classify(&case, &report),
            Ok(true),
            "store_place={store_place}"
        );
        verify_status(&case, &report, true).unwrap();
        let index = report
            .evidence
            .iter()
            .position(|event| event["kind"] == "seam_delivered")
            .unwrap();
        for (label, mutate) in mutations {
            let mut report = store_fixture(store_place).1;
            mutate(&mut report.evidence[index]["value"]);
            assert!(
                classify(&case, &report).is_err(),
                "{label}, store_place={store_place}"
            );
        }
    }
    let (case, mut report) = fixture();
    let index = report
        .evidence
        .iter()
        .position(|event| event["kind"] == "seam_delivered")
        .unwrap();
    report.evidence[index]["value"]["hit"] = json!({"method": "write_text", "requested": "__recovery/op-1.json", "stored": "__recovery/dstm-op-1.json"});
    assert!(
        classify(&case, &report).is_err(),
        "an engine effect carries no hit"
    );
}

#[test]
fn never_waives_harness_errors_or_an_unexpected_pass() {
    for code in [
        "worker_failed",
        "timeout",
        "fault_cleanup_failed",
        "report_failed",
        "seam_unobserved",
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
        CASE.replace("--- seam\nat: mutation.post_sidecar_pre_fork\noccurrence: 1\naction: fail\nscope: next_step\n", ""),
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
