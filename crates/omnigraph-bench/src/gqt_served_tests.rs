//! Served admission and archive evidence must not inherit local measurements.
use super::*;
use crate::case::{
    Attribution, EnginePreparation, NetworkPosition, PageCacheCondition, ProcessLifecycle,
    ResetMode,
};
use crate::gqt_record::{GqtRunRecordV1, GqtSutIdentityV1, ServedSutIdentityV1, ServedSutKind};
use crate::gqt_served::{
    ServerArtifactV1, ServerBuildAttestationV1, ServerDatasetAttestationV1,
    ServerDeploymentReceiptV1,
};

fn receipt_for(plan: &PlannedGqt, url: &str) -> ServerDeploymentReceiptV1 {
    ServerDeploymentReceiptV1 {
        format_version: 1,
        endpoint_sha256: sha256_bytes(
            crate::gqt_served::canonical_endpoint(url)
                .unwrap()
                .as_bytes(),
        ),
        graph: "bench".into(),
        server: ServerBuildAttestationV1 {
            package_version: "0.13.0".into(),
            source_commit: "a".repeat(40),
            source_tree_dirty: false,
            profile: "release".into(),
            cargo_opt_level: "2".into(),
            debug_assertions: false,
            artifact: ServerArtifactV1::Image {
                sha256: "b".repeat(64),
            },
            target_triple: None,
            rustc_version: None,
            engine: None,
        },
        backend: plan.definition.environment.backend.clone(),
        dataset: ServerDatasetAttestationV1 {
            recipe_sha256: plan.recipe_sha256.clone(),
            logical_content_sha256: "c".repeat(64),
            algorithm: crate::dataset_identity::DATASET_LOGICAL_ALGORITHM.into(),
        },
        machine: None,
    }
}

#[test]
fn served_catalog_preserves_embedded_defaults_and_contains_only_reads() {
    let old = plan("tiny-read");
    let implicit = serde_json::to_value(&old.definition.environment).unwrap();
    let mut explicit = implicit.clone();
    explicit["target"] = "omnigraph-engine".into();
    explicit["network_position"] = "same-host".into();
    let explicit: GqtEnvironment = serde_json::from_value(explicit).unwrap();
    assert_eq!(serde_json::to_value(explicit).unwrap(), implicit);
    for (group, count) in [("query-shapes-served", 13), ("traversal-served", 3)] {
        let suite = recorded_catalog().resolve(Some(group), None).unwrap();
        assert_eq!(suite.runs.len(), count);
        for run in suite.runs {
            let p = run.case.gqt().unwrap();
            assert_eq!(p.definition.environment.target, Target::Server);
            assert_eq!(p.definition.protocol.reset, ResetMode::None);
            assert_eq!(p.definition.protocol.attribution, Attribution::Off);
            assert_eq!(
                p.cache_condition.process,
                ProcessLifecycle::LongRunningServer
            );
            assert_eq!(p.cache_condition.engine, EnginePreparation::WarmedByProgram);
            assert_eq!(
                p.cache_condition.page_cache,
                PageCacheCondition::Uncontrolled
            );
            assert_eq!(p.cache_condition.iterations, 1);
            assert_eq!(p.definition.workload.measured_step.ordinal, 2);
            let q = p.queries.parse().unwrap();
            assert!(q.fixture.is_none());
            assert_eq!(q.steps().len(), 3);
            assert!(q.steps().iter().all(|s| s.kind == StepKind::Query));
            assert!(
                p.dataset_build_plan()
                    .unwrap_err()
                    .contains("the dataset cache does not apply")
            );
        }
    }
}

fn replan(mut p: PlannedGqt) -> Result<PlannedGqt, String> {
    p.case_digest = crate::model::typed_sha256(&p.definition).unwrap();
    p.planned_sha256 = p.planned_hash().unwrap();
    p.revalidate()?;
    Ok(p)
}

#[test]
fn served_identity_axes_and_read_only_suffix_are_enforced() {
    let p = plan("e2e-query-count-served");
    let mut remote = p.clone();
    remote.definition.environment.network_position = NetworkPosition::Remote;
    let remote = replan(remote).unwrap();
    assert_ne!(p.planned_sha256, remote.planned_sha256);
    let mut invalid = p.clone();
    invalid.definition.protocol.reset = ResetMode::LocalClonefile;
    assert!(replan(invalid).is_err());
    let mut invalid = p.clone();
    invalid.definition.protocol.attribution = Attribution::PerPhase;
    assert!(replan(invalid).is_err());
    let mut invalid = plan("tiny-read");
    invalid.definition.environment.network_position = NetworkPosition::Remote;
    assert!(replan(invalid).is_err());
    let mut invalid = plan("tiny-read");
    invalid.definition.protocol.reset = ResetMode::None;
    assert!(replan(invalid).is_err());
    for (suffix, refusal) in [
        (
            "\n--- restart\n",
            "omnigraph-server has no door for step 4 (restart)",
        ),
        (
            "\n--- mutate\nquery write() { delete Person where name = \"added\" }\n--- expect affected: nodes=0 edges=0\n",
            "step 4 (mutate) is not read-only",
        ),
    ] {
        let text = format!("{}{suffix}", p.queries.text);
        let parsed = omnigraph_gqt_core::parse_case("served_refusal", &text).unwrap();
        let error = admit_target_queries(
            &parsed,
            &p.definition.workload.measured_step,
            Target::Server,
        )
        .unwrap_err();
        assert!(error.contains(refusal), "{error}");
    }
    let measured = p
        .queries
        .text
        .match_indices("--- query\n")
        .nth(1)
        .unwrap()
        .0;
    let mut text = p.queries.text.clone();
    text.insert_str(measured + "--- query\n".len(), "set merge_lineage = off;\n");
    let parsed = omnigraph_gqt_core::parse_case("served_settings_prefix", &text).unwrap();
    let error = admit_target_queries(
        &parsed,
        &p.definition.workload.measured_step,
        Target::Server,
    )
    .unwrap_err();
    assert_eq!(
        error,
        "step 2 (query with a settings prefix) is refused for served acquisition until the served door is proven by a conformance case"
    );
}

#[test]
fn served_environment_requires_explicit_network_position() {
    let backend = serde_json::to_value(crate::case::Backend::LocalFs {
        filesystem: crate::case::LocalFilesystem::Apfs,
        storage_class: crate::case::LocalStorageClass::NvmeSsd,
    })
    .unwrap();
    let served = serde_json::json!({"backend": backend.clone(), "target": "omnigraph-server"});
    let error = serde_json::from_value::<GqtEnvironment>(served.clone())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(
            "server targets must declare network_position (same-host, same-region or remote)"
        ),
        "{error}"
    );
    let mut explicit = served;
    explicit["network_position"] = "same-host".into();
    let environment: GqtEnvironment = serde_json::from_value(explicit.clone()).unwrap();
    assert_eq!(environment.network_position, NetworkPosition::SameHost);
    assert_eq!(serde_json::to_value(&environment).unwrap(), explicit);
    let embedded = serde_json::json!({ "backend": backend });
    let environment: GqtEnvironment = serde_json::from_value(embedded.clone()).unwrap();
    assert_eq!(environment.target, Target::Engine);
    assert_eq!(environment.network_position, NetworkPosition::SameHost);
    assert_eq!(serde_json::to_value(&environment).unwrap(), embedded);
}

#[test]
fn receipt_graph_ids_match_the_server_namespace() {
    let mut receipt = receipt_for(&plan("e2e-query-count-served"), "http://localhost:8080");
    for graph in ["0", "-", "tenant-001", &"a".repeat(64)] {
        receipt.graph = graph.into();
        receipt.validate().unwrap();
    }
    for graph in [
        "",
        "under_score",
        "policies",
        "healthz",
        "openapi",
        "graphs",
        &"a".repeat(65),
    ] {
        receipt.graph = graph.into();
        assert!(receipt.validate().is_err(), "accepted {graph}");
    }
}

#[test]
fn deployment_receipts_refuse_unbound_or_non_release_evidence() {
    let p = plan("e2e-query-count-served");
    let receipt = receipt_for(&p, "http://127.0.0.1:8000/");
    receipt.validate().unwrap();
    let mutations: &[fn(&mut ServerDeploymentReceiptV1)] = &[
        |r| r.server.debug_assertions = true,
        |r| r.server.source_tree_dirty = true,
        |r| r.server.profile = "dev".into(),
        |r| r.server.cargo_opt_level = "0".into(),
        |r| r.server.source_commit = "unknown".into(),
        |r| r.dataset.algorithm = "unknown".into(),
        |r| r.graph = "../graph".into(),
        |r| r.endpoint_sha256 = "unknown".into(),
        |r| r.server.package_version = "x".repeat(9000),
    ];
    for mutate in mutations {
        let mut invalid = receipt.clone();
        mutate(&mut invalid);
        assert!(invalid.validate().is_err());
    }
    for url in [
        "http://user:secret@localhost",
        "http://localhost?token=secret",
        "http://localhost/#secret",
        "file:///tmp/server",
    ] {
        assert!(crate::gqt_served::canonical_endpoint(url).is_err());
    }
    let input = crate::gqt_served::ServedInput {
        target: omnigraph_gqt_core::ServerTarget {
            url: "http://127.0.0.1:8000".into(),
            graph: "bench".into(),
            token: Some("private-token".into()),
        },
        receipt,
    };
    let bound = input.receipt.bind(&p).unwrap();
    input.validate(&bound).unwrap();
    assert!(!format!("{input:?}").contains("private-token"));
    for token in [String::new(), "x".repeat(4097), "secret\nvalue".into()] {
        let mut changed = input.clone();
        changed.target.token = Some(token);
        assert_eq!(
            changed.validate(&bound).unwrap_err(),
            "invalid bounded bearer token"
        );
    }
    let mutations: &[fn(&mut crate::gqt_served::ServedInput)] = &[
        |i| i.receipt.endpoint_sha256 = "d".repeat(64),
        |i| i.target.graph = "another-graph".into(),
        |i| i.receipt.dataset.recipe_sha256 = "d".repeat(64),
        |i| i.target.url = "http://127.0.0.1:8001".into(),
    ];
    for mutate in mutations {
        let mut changed = input.clone();
        mutate(&mut changed);
        assert!(changed.validate(&bound).is_err());
    }
}

#[test]
fn receipt_refuses_future_format_and_bad_digests() {
    let receipt = receipt_for(&plan("e2e-query-count-served"), "http://127.0.0.1:8000");
    receipt.validate().unwrap();
    let mutations: &[fn(&mut ServerDeploymentReceiptV1)] = &[
        |r| r.format_version = 2,
        |r| r.server.source_commit = "A".repeat(40),
        |r| {
            r.server.artifact = ServerArtifactV1::Executable {
                sha256: "B".repeat(64),
            }
        },
        |r| r.dataset.recipe_sha256 = "c".repeat(63),
        |r| r.dataset.logical_content_sha256 = "g".repeat(64),
    ];
    for mutate in mutations {
        let mut invalid = receipt.clone();
        mutate(&mut invalid);
        assert!(invalid.validate().is_err(), "{invalid:?}");
    }
}

#[test]
fn canonical_endpoint_folds_equivalent_spellings() {
    use crate::gqt_served::canonical_endpoint;
    for (spelling, canonical) in [
        ("HTTP://LOCALHOST:80/", "http://localhost"),
        ("https://Example.COM:443/", "https://example.com"),
        ("http://127.0.0.1:8080/", "http://127.0.0.1:8080"),
        ("http://[0:0:0:0:0:0:0:1]:9000", "http://[::1]:9000"),
        ("http://example.com/a/../b/", "http://example.com/b"),
    ] {
        assert_eq!(canonical_endpoint(spelling).unwrap(), canonical);
        assert_eq!(canonical_endpoint(canonical).unwrap(), canonical);
    }
    assert_eq!(
        canonical_endpoint("http://h:8080/graphs/bench").unwrap_err(),
        "the server URL is the server base, not a graph path"
    );
    assert_eq!(
        canonical_endpoint("http://127.0.0.1:80800").unwrap_err(),
        "invalid server URL: invalid port number"
    );
}

#[test]
fn token_environment_cannot_be_captured_as_runtime_evidence() {
    use crate::gqt_served::validate_token_environment_name;
    let long = "A".repeat(129);
    for (name, refusal) in [
        ("LANCE_MEM_POOL_SIZE", "runtime namespaces"),
        ("OMNIGRAPH_BENCH_BUILD_OPT_LEVEL", "runtime namespaces"),
        ("", "is empty"),
        ("bad=name", "only ASCII letters, digits and underscores"),
        (long.as_str(), "exceeds 128 bytes"),
    ] {
        let error = validate_token_environment_name(name).unwrap_err();
        assert!(error.contains(refusal), "{name}: {error}");
    }
    validate_token_environment_name("BENCH_SERVER_TOKEN").unwrap();
}

#[test]
fn served_redaction_covers_form_encoded_token() {
    let token = "secret value/with+plus\"quote";
    let escaped = serde_json::to_string(token).unwrap();
    let form: String = url::form_urlencoded::byte_serialize(token.as_bytes()).collect();
    let percent = form.replace('+', "%20");
    for spelling in [
        &escaped[1..escaped.len() - 1],
        form.as_str(),
        percent.as_str(),
        token,
    ] {
        let redacted =
            crate::gqt_served::redact_error(format!("server said: {spelling}."), Some(token));
        assert_eq!(redacted, "server said: <redacted>.", "{spelling}");
    }
}

async fn served_record() -> GqtRunRecordV1 {
    let mut r = authority_fixture().await;
    let GqtSutIdentityV1::Embedded(client_build) = r.sut else {
        panic!("expected embedded fixture")
    };
    let fixture = r.fixture.take().unwrap();
    let mut receipt = receipt_for(&plan("tiny-read"), "http://127.0.0.1:8000");
    receipt.dataset.logical_content_sha256 = fixture.handoff.summary.logical_content_sha256;
    let receipt_sha = receipt.digest().unwrap();
    r.sut = GqtSutIdentityV1::Served(Box::new(ServedSutIdentityV1 {
        kind: ServedSutKind::DeclaredDeployment,
        receipt,
        client_build: *client_build,
        client_machine: r.machine.take().unwrap(),
    }));
    r.backend = None;
    r.dataset_cache_hit = None;
    r.run.run_spec.environment.target = Target::Server;
    r.run.run_spec.protocol.reset = ResetMode::None;
    r.run.run_spec.protocol.attribution = Attribution::Off;
    r.run.run_spec.cache_condition.process = ProcessLifecycle::LongRunningServer;
    r.run.run_spec.cache_condition.engine = EnginePreparation::Uncontrolled;
    r.run.run_spec.cache_condition.page_cache = PageCacheCondition::Uncontrolled;
    r.measurements.layer_presence = crate::gqt_record::layer_presence(Target::Server);
    for s in &mut r.measurements.raw_samples {
        s.input_physical_digest_sha256 = None;
        s.logical_store_calls = None;
        s.control_store_calls = None;
        s.client_peak_rss_bytes = s.peak_rss_bytes.take();
        s.server_receipt_sha256 = Some(receipt_sha.clone());
    }
    rehash_record(&mut r);
    crate::gqt_record::validate(&r).unwrap();
    r
}

#[tokio::test]
async fn served_records_preserve_absence_and_reject_mixed_evidence() {
    let r = served_record().await;
    assert!(!r.claim_eligible());
    let bytes = crate::gqt_record::canonical_bytes(&r).unwrap();
    assert_eq!(
        crate::gqt_record::parse(&bytes).unwrap(),
        crate::gqt_record::AnyRunRecordV1::Gqt(Box::new(r.clone()))
    );
    let value = serde_json::to_value(&r).unwrap();
    for field in ["machine", "backend", "fixture", "dataset_cache_hit"] {
        assert!(value.get(field).is_none());
    }
    let mutations: &[fn(&mut GqtRunRecordV1)] = &[
        |r| r.dataset_cache_hit = Some(false),
        |r| r.measurements.raw_samples[0].peak_rss_bytes = Some(1024),
        |r| r.measurements.raw_samples[0].client_peak_rss_bytes = None,
        |r| r.measurements.raw_samples[0].input_physical_digest_sha256 = Some("a".repeat(64)),
        |r| r.measurements.raw_samples[0].server_receipt_sha256 = Some("a".repeat(64)),
        |r| {
            r.measurements.raw_samples[0].steps.last_mut().unwrap().kind =
                crate::gqt_runner::GqtOperationKind::Mutate
        },
        |r| {
            r.measurements.layer_presence.logical.counts =
                crate::record::MeasurementPresenceV1::Observed
        },
        |r| {
            if let GqtSutIdentityV1::Served(sut) = &mut r.sut {
                sut.receipt.dataset.logical_content_sha256 = "d".repeat(64);
            }
        },
    ];
    for mutate in mutations {
        let mut invalid = r.clone();
        mutate(&mut invalid);
        assert!(crate::gqt_record::validate(&invalid).is_err());
    }
    let embedded = authority_fixture().await;
    let value = serde_json::to_value(&embedded).unwrap();
    assert!(value["sut"].get("kind").is_none());
    assert!(
        value["measurements"]["raw_samples"][0]
            .get("server_receipt_sha256")
            .is_none()
    );
    let bytes = crate::gqt_record::canonical_bytes(&embedded).unwrap();
    assert_eq!(
        crate::gqt_record::canonical_bytes(&crate::gqt_record::parse(&bytes).unwrap()).unwrap(),
        bytes
    );
}

#[tokio::test]
async fn embedded_record_keys_match_base_shape() {
    let record = serde_json::to_value(authority_fixture().await).unwrap();
    for field in ["machine", "backend", "fixture", "dataset_cache_hit"] {
        assert!(record.get(field).is_some(), "{field}");
    }
    assert!(record["sut"].get("kind").is_none());
    let environment = &record["run"]["run_spec"]["environment"];
    assert!(environment.get("target").is_none());
    assert!(environment.get("network_position").is_none());
    let keys: std::collections::BTreeSet<_> = record["measurements"]["raw_samples"][0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        std::collections::BTreeSet::from([
            "repetition",
            "input_physical_digest_sha256",
            "elapsed_us",
            "peak_rss_bytes",
            "outcome",
            "logical_store_calls",
            "control_store_calls",
            "steps",
            "verification",
            "merge",
        ])
    );
}

#[tokio::test]
async fn embedded_rejects_served_only_evidence() {
    let r = authority_fixture().await;
    crate::gqt_record::validate(&r).unwrap();
    let mutations: &[fn(&mut GqtRunRecordV1)] = &[
        |r| r.machine = None,
        |r| r.dataset_cache_hit = None,
        |r| r.measurements.raw_samples[0].server_receipt_sha256 = Some("a".repeat(64)),
        |r| r.measurements.raw_samples[0].client_peak_rss_bytes = Some(1024),
        |r| r.measurements.raw_samples[0].control_store_calls = None,
    ];
    for mutate in mutations {
        let mut invalid = r.clone();
        mutate(&mut invalid);
        assert!(crate::gqt_record::validate(&invalid).is_err());
    }
}

#[tokio::test]
async fn served_rejects_embedded_only_evidence() {
    let r = served_record().await;
    let embedded = authority_fixture().await;
    let mut invalid = r.clone();
    invalid.machine = embedded.machine.clone();
    assert!(crate::gqt_record::validate(&invalid).is_err());
    let mut invalid = r.clone();
    invalid.backend = embedded.backend.clone();
    assert!(crate::gqt_record::validate(&invalid).is_err());
    let mut invalid = r.clone();
    invalid.fixture = embedded.fixture.clone();
    assert!(crate::gqt_record::validate(&invalid).is_err());
    let mut invalid = r.clone();
    invalid.measurements.raw_samples[0].logical_store_calls = embedded.measurements.raw_samples[0]
        .logical_store_calls
        .clone();
    assert!(crate::gqt_record::validate(&invalid).is_err());
}

#[tokio::test]
async fn served_sut_dispatch_is_by_field_set() {
    let served = served_record().await.sut;
    let embedded = authority_fixture().await.sut;
    for sut in [&served, &embedded] {
        let value = serde_json::to_value(sut).unwrap();
        assert_eq!(
            &serde_json::from_value::<GqtSutIdentityV1>(value).unwrap(),
            sut
        );
    }
    let mut value = serde_json::to_value(&served).unwrap();
    value.as_object_mut().unwrap().remove("kind");
    assert!(serde_json::from_value::<GqtSutIdentityV1>(value).is_err());
    let mut value = serde_json::to_value(&embedded).unwrap();
    value["kind"] = "declared-deployment".into();
    assert!(serde_json::from_value::<GqtSutIdentityV1>(value).is_err());
}

#[tokio::test]
async fn mixed_archive_projects_declared_server_and_missing_counters() {
    let directory = tempfile::tempdir().unwrap();
    let archive = directory.path().join("archive");
    let projection = directory.path().join("projection");
    crate::archive::preflight_archive_publication(&archive).unwrap();
    let legacy = crate::record::tests::valid_record_fixture();
    let embedded = authority_fixture().await;
    let mut served = served_record().await;
    served.invocation.invocation_id.replace_range(25..26, "D");
    let mut other_client = served.clone();
    other_client
        .invocation
        .invocation_id
        .replace_range(25..26, "E");
    if let GqtSutIdentityV1::Served(sut) = &mut other_client.sut {
        sut.client_build.source_commit = "e".repeat(40);
    }
    crate::archive::publish_record(&archive, &legacy).unwrap();
    crate::archive::publish_record(&archive, &embedded).unwrap();
    crate::archive::publish_record(&archive, &served).unwrap();
    crate::archive::publish_record(&archive, &other_client).unwrap();
    let built = crate::projection::rebuild_projection(&archive, &projection)
        .await
        .unwrap();
    assert_eq!(built.record_count, 4);
    assert_eq!(built.point_count, 3);
    let runs =
        crate::projection::list_runs_for_point_page(&projection, &served.run.point_id, 10, None)
            .await
            .unwrap();
    assert_eq!(runs.rows.len(), 2);
    assert!(!runs.rows[0]["sut_fingerprint"].is_null());
    assert_eq!(
        runs.rows[0]["sut_fingerprint"],
        runs.rows[1]["sut_fingerprint"]
    );
    assert_ne!(
        runs.rows[0]["client_build_json"],
        runs.rows[1]["client_build_json"]
    );
    let GqtSutIdentityV1::Served(sut) = &served.sut else {
        panic!("served fixture")
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(runs.rows[0]["sut_json"].as_str().unwrap())
            .unwrap(),
        serde_json::to_value(&sut.receipt.server).unwrap()
    );
    let absent = [
        "machine_fingerprint",
        "machine_cpu_model",
        "worker_executable_sha256",
        "fixture_manifest_sha256",
        "fixture_physical_sha256",
        "lance_data_plane_logical_calls_min",
        "lance_data_plane_logical_calls_p50",
        "lance_data_plane_logical_calls_max",
        "control_plane_logical_calls_min",
        "control_plane_logical_calls_p50",
        "control_plane_logical_calls_max",
    ];
    for row in &runs.rows {
        assert_eq!(row["sut_evidence"], "declared-deployment");
        assert_eq!(row["backend_evidence"], "declared-deployment");
        assert_eq!(row["source_commit"], "a".repeat(40));
        assert_eq!(row["claim_eligible"], false);
        assert!(row["client_build_json"].is_string());
        assert!(row["client_machine_json"].is_string());
        for field in absent {
            assert!(row[field].is_null(), "{field} must be absent");
        }
    }
    let embedded_runs =
        crate::projection::list_runs_for_point_page(&projection, &embedded.run.point_id, 10, None)
            .await
            .unwrap();
    let [row] = embedded_runs.rows.as_slice() else {
        panic!("one embedded run")
    };
    assert_eq!(row["sut_evidence"], "worker-observed");
    assert_eq!(row["backend_evidence"], "worker-observed");
    assert!(row["client_build_json"].is_null());
    assert!(row["client_machine_json"].is_null());
    for field in absent {
        assert!(!row[field].is_null(), "{field} must be projected");
    }
}

mod measured_http {

    use crate::case::{Backend, LocalFilesystem, LocalStorageClass, NetworkPosition, ResetMode};
    use crate::gqt_case::{
        BoundGqt, GqtEnvironment, MeasuredStep, PlannedGqt, Target, explicit_pair_in_environment,
        sha256_bytes,
    };
    use crate::gqt_protocol::{
        ParentFrameV2, PreparationProofV2, RepetitionInputV2, WORKER_PROTOCOL_VERSION,
        WorkerRequestV2, read_frame, write_frame,
    };
    use crate::gqt_runner::{
        GqtRepObservation, GqtStepObservation, GqtVerification, execute_served_rep_signaled,
        validate_failed_sample, validate_sample,
    };
    use crate::gqt_served::{
        ServedInput, ServerArtifactV1, ServerBuildAttestationV1, ServerDatasetAttestationV1,
        ServerDeploymentReceiptV1, canonical_endpoint,
    };
    use crate::runner::{MeasurementSignals, RunnerResult};
    use omnigraph_gqt_core::ServerTarget;
    use serde_json::{Value, json};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;

    const TOKEN: &str = "served-token-must-not-be-retained";
    const CONTRACT_HEADER: &str = "omnigraph-http-api";
    const CONTRACT: &str = "0.13";
    type Events = Arc<Mutex<Vec<String>>>;

    #[derive(Clone, Copy)]
    enum SuffixReply {
        Rows,
        ErrorWithToken,
        UndecodableWithToken,
    }

    #[derive(Debug)]
    struct ObservedRequest {
        method: String,
        path: String,
        body: Value,
        authorized: bool,
        correct_contract: bool,
    }

    /// One listener survives all repetitions; each response closes its connection.
    /// Drop always stops and joins the thread, including an early test panic.
    struct ReadServer {
        url: String,
        events: Events,
        requests: Arc<Mutex<Vec<ObservedRequest>>>,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl ReadServer {
        fn start(token: &str, suffix: SuffixReply) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let events = Arc::new(Mutex::new(Vec::new()));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let worker_events = Arc::clone(&events);
            let worker_requests = Arc::clone(&requests);
            let worker_stop = Arc::clone(&stop);
            let token = token.to_owned();
            let thread = std::thread::spawn(move || {
                while !worker_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            stream.set_nonblocking(false).unwrap();
                            stream
                                .set_read_timeout(Some(Duration::from_secs(5)))
                                .unwrap();
                            stream
                                .set_write_timeout(Some(Duration::from_secs(5)))
                                .unwrap();
                            let request = read_request(&mut stream, &token);
                            let name = request.body["name"].as_str().unwrap().to_owned();
                            worker_events.lock().unwrap().push(format!("http:{name}"));
                            worker_requests.lock().unwrap().push(request);
                            let (status, body) = if name == "verify" {
                                match suffix {
                                    SuffixReply::Rows => ("200 OK", read_output(&name)),
                                    SuffixReply::ErrorWithToken => (
                                        "400 Bad Request",
                                        json!({"error": format!("suffix rejected: {token}")}),
                                    ),
                                    SuffixReply::UndecodableWithToken => (
                                        "200 OK",
                                        json!({"unexpected": format!("reflected credential: {token}")}),
                                    ),
                                }
                            } else {
                                ("200 OK", read_output(&name))
                            };
                            let bytes = serde_json::to_vec(&body).unwrap();
                            write!(
                            stream,
                            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{CONTRACT_HEADER}: {CONTRACT}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            bytes.len()
                        ).unwrap();
                            stream.write_all(&bytes).unwrap();
                            stream.flush().unwrap();
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(error) => panic!("test listener failed: {error}"),
                    }
                }
            });
            Self {
                url,
                events,
                requests,
                stop,
                thread: Some(thread),
            }
        }

        fn signals(&self) -> Signals {
            Signals {
                events: Arc::clone(&self.events),
                elapsed: None,
            }
        }

        fn assert_read_only_requests(&self, repetitions: usize) {
            let requests = self.requests.lock().unwrap();
            assert_eq!(requests.len(), repetitions * 3);
            for request in requests.iter() {
                assert_eq!(request.method, "POST");
                assert_eq!(request.path, "/graphs/smoke/query");
                assert!(
                    request.authorized,
                    "the request omitted its bearer credential"
                );
                assert!(
                    request.correct_contract,
                    "the request omitted its API contract"
                );
                assert_eq!(request.body["branch"], "main");
                assert!(
                    request.body["query"]
                        .as_str()
                        .unwrap()
                        .contains("match { $i: Item }")
                );
            }
        }
    }

    impl Drop for ReadServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            if let Some(thread) = self.thread.take() {
                let result = thread.join();
                if !std::thread::panicking() {
                    result.expect("test HTTP server panicked");
                }
            }
        }
    }

    fn read_request(stream: &mut TcpStream, token: &str) -> ObservedRequest {
        let mut reader = BufReader::new(stream);
        let mut first = String::new();
        reader.read_line(&mut first).unwrap();
        let mut request_line = first.split_whitespace();
        let method = request_line.next().unwrap().to_owned();
        let path = request_line.next().unwrap().to_owned();
        assert_eq!(request_line.next(), Some("HTTP/1.1"));
        let mut content_length = None;
        let mut authorized = false;
        let mut correct_contract = false;
        let mut header_bytes = first.len();
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            header_bytes += line.len();
            assert!(
                header_bytes <= 16 * 1024,
                "test request headers exceed their bound"
            );
            if line == "\r\n" {
                break;
            }
            let (name, value) = line.split_once(':').unwrap();
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = Some(value.parse::<usize>().unwrap());
            } else if name.eq_ignore_ascii_case("authorization") {
                authorized = value == format!("Bearer {token}");
            } else if name.eq_ignore_ascii_case(CONTRACT_HEADER) {
                correct_contract = value == CONTRACT;
            }
        }
        let content_length = content_length.expect("JSON request must have a content length");
        assert!(content_length <= 16 * 1024);
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
        ObservedRequest {
            method,
            path,
            body: serde_json::from_slice(&body).unwrap(),
            authorized,
            correct_contract,
        }
    }

    /// Actual ReadOutput JSON shape; API types is deliberately not a new test dependency.
    fn read_output(name: &str) -> Value {
        json!({
            "query_name": name,
            "target": { "branch": "main", "snapshot": null },
            "row_count": 1,
            "columns": ["key", "val"],
            "rows": [{ "key": "a", "val": 1 }],
            "graph_commit_id": "fixture-commit"
        })
    }

    struct Signals {
        events: Events,
        elapsed: Option<u64>,
    }
    impl MeasurementSignals for Signals {
        fn ready(&mut self) -> RunnerResult<()> {
            self.events.lock().unwrap().push("ready".into());
            Ok(())
        }
        fn settled(&mut self, elapsed: u64) -> RunnerResult<()> {
            self.events.lock().unwrap().push("settled".into());
            self.elapsed = Some(elapsed);
            Ok(())
        }
    }

    fn fixture() -> (tempfile::TempDir, PlannedGqt) {
        let directory = tempfile::tempdir().unwrap();
        let dataset = directory.path().join("dataset.gqt");
        std::fs::write(
            &dataset,
            r#"# issue: none
# notes: Frozen recipe; this dataset must never execute in the repetition client.
--- runner
timeout_ms: 5000
environments:
  - target: omnigraph-engine
    storage: local-filesystem
--- schema
node Item { key: String @key val: I64 }
--- seed
{"type":"Item","data":{"key":"a","val":1}}
"#,
        )
        .unwrap();
        let queries = directory.path().join("queries.gqt");
        let mut source = String::from(
            "# issue: none\n# notes: Served timing callback fixture.\n--- runner\ntimeout_ms: 5000\nenvironments:\n  - target: omnigraph-server\n    storage: local-filesystem\n",
        );
        for name in ["warmup", "selected", "verify"] {
            source.push_str(&format!(
                r#"
--- query branch: main
query {name}() {{
    match {{ $i: Item }}
    return {{ $i.key as key, $i.val as val }}
}}
--- expect unordered
{{"key":"a","val":1}}
--- expect shape
key: String
val: I64
"#
            ));
        }
        std::fs::write(&queries, &source).unwrap();
        let parsed = omnigraph_gqt_core::parse_case("queries", &source).unwrap();
        let selected = parsed
            .steps()
            .into_iter()
            .find(|step| step.ordinal == 2)
            .unwrap();
        let plan = explicit_pair_in_environment(
            &dataset,
            &queries,
            MeasuredStep {
                ordinal: 2,
                text: selected.source,
            },
            GqtEnvironment {
                target: Target::Server,
                network_position: NetworkPosition::SameHost,
                backend: Backend::LocalFs {
                    filesystem: LocalFilesystem::Apfs,
                    storage_class: LocalStorageClass::NvmeSsd,
                },
            },
            ResetMode::None,
            Some(5),
        )
        .unwrap();
        (directory, plan)
    }

    fn served_input(plan: &PlannedGqt, url: &str, token: &str) -> (BoundGqt, ServedInput) {
        let receipt = ServerDeploymentReceiptV1 {
            format_version: 1,
            endpoint_sha256: sha256_bytes(canonical_endpoint(url).unwrap().as_bytes()),
            graph: "smoke".into(),
            server: ServerBuildAttestationV1 {
                package_version: "0.13.0".into(),
                source_commit: "a".repeat(40),
                source_tree_dirty: false,
                profile: "release".into(),
                cargo_opt_level: "2".into(),
                debug_assertions: false,
                artifact: ServerArtifactV1::Executable {
                    sha256: "b".repeat(64),
                },
                target_triple: None,
                rustc_version: None,
                engine: None,
            },
            backend: plan.definition.environment.backend.clone(),
            dataset: ServerDatasetAttestationV1 {
                recipe_sha256: plan.recipe_sha256.clone(),
                logical_content_sha256: "c".repeat(64),
                algorithm: crate::dataset_identity::DATASET_LOGICAL_ALGORITHM.into(),
            },
            machine: None,
        };
        let bound = receipt.bind(plan).unwrap();
        let input = ServedInput {
            target: ServerTarget {
                url: url.into(),
                graph: "smoke".into(),
                token: Some(token.into()),
            },
            receipt,
        };
        input.validate(&bound).unwrap();
        (bound, input)
    }

    fn proof(input: &ServedInput) -> PreparationProofV2 {
        RepetitionInputV2::Served {
            input: Box::new(input.clone()),
        }
        .proof()
        .unwrap()
    }

    fn assert_client_only_sample(sample: &GqtRepObservation, input: &ServedInput) {
        assert!(sample.input_physical_digest_sha256.is_none());
        assert!(sample.logical_store_calls.is_none());
        assert!(sample.control_store_calls.is_none());
        assert!(sample.peak_rss_bytes.is_none());
        assert!(sample.client_peak_rss_bytes.is_none());
        assert!(sample.merge.is_none());
        assert_eq!(
            sample.server_receipt_sha256.as_ref(),
            Some(&input.receipt.digest().unwrap())
        );
    }

    #[tokio::test]
    async fn served_repetitions_only_query_and_preserve_ready_settled_order() {
        let server = ReadServer::start(TOKEN, SuffixReply::Rows);
        let (_directory, plan) = fixture();
        let (bound, input) = served_input(&plan, &server.url, TOKEN);
        for repetition in 1..=2 {
            let mut signals = server.signals();
            let sample = execute_served_rep_signaled(repetition, &bound, &input, &mut signals)
                .await
                .unwrap();
            assert_eq!(signals.elapsed, Some(sample.elapsed_us));
            assert_client_only_sample(&sample, &input);
            assert_eq!(
                sample
                    .steps
                    .iter()
                    .map(|step| (step.ordinal, step.occurrence))
                    .collect::<Vec<_>>(),
                vec![(1, 1), (2, 1), (3, 1)]
            );
            assert_eq!(sample.steps[1].elapsed_us, sample.elapsed_us);
            assert_eq!(sample.verification.assertions_passed, 3);
            assert_eq!(sample.verification.following_assertions, 1);
            assert!(sample.verification.selected_assertion_passed);
            assert!(!serde_json::to_string(&sample).unwrap().contains(TOKEN));
        }
        server.assert_read_only_requests(2);
        assert_eq!(
            *server.events.lock().unwrap(),
            vec![
                "http:warmup",
                "ready",
                "http:selected",
                "settled",
                "http:verify",
                "http:warmup",
                "ready",
                "http:selected",
                "settled",
                "http:verify",
            ]
        );
    }

    #[tokio::test]
    async fn served_suffix_failure_retains_closed_clock_without_local_evidence_or_token() {
        let server = ReadServer::start(TOKEN, SuffixReply::ErrorWithToken);
        let (_directory, plan) = fixture();
        let (bound, input) = served_input(&plan, &server.url, TOKEN);
        let mut signals = server.signals();
        let error = execute_served_rep_signaled(7, &bound, &input, &mut signals)
            .await
            .unwrap_err();
        assert_eq!(error.code, "gqt_verification_failed");
        assert!(error.message.contains("<redacted>"));
        assert!(!serde_json::to_string(&error).unwrap().contains(TOKEN));
        let sample = error
            .context
            .gqt_settled_sample
            .as_ref()
            .expect("closed clock must retain a rejected sample");
        assert_client_only_sample(sample, &input);
        assert_eq!(sample.outcome, "verification-failed");
        assert_eq!(signals.elapsed, Some(sample.elapsed_us));
        assert!(sample.verification.selected_assertion_passed);
        assert_eq!(sample.verification.following_assertions, 0);
        validate_failed_sample(sample, &bound, 7, &proof(&input), sample.elapsed_us).unwrap();
        assert_eq!(
            *server.events.lock().unwrap(),
            vec![
                "http:warmup",
                "ready",
                "http:selected",
                "settled",
                "http:verify"
            ]
        );
        server.assert_read_only_requests(1);
    }

    #[tokio::test]
    async fn served_decode_failure_redacts_json_escaped_token() {
        let token = "served-secret-quote\"-slash\\-end";
        let server = ReadServer::start(token, SuffixReply::UndecodableWithToken);
        let (_directory, plan) = fixture();
        let (bound, input) = served_input(&plan, &server.url, token);
        let mut signals = server.signals();
        let error = execute_served_rep_signaled(1, &bound, &input, &mut signals)
            .await
            .unwrap_err();
        assert_eq!(error.code, "gqt_verification_failed");
        assert!(error.context.gqt_settled_sample.is_some());
        assert!(!error.message.contains(token));
        let escaped = serde_json::to_string(token).unwrap();
        assert!(
            !error.message.contains(&escaped[1..escaped.len() - 1]),
            "JSON-escaped credential leaked through decode diagnostics"
        );
        assert!(
            !error.message.contains("served-secret-quote"),
            "credential bytes leaked through transformed diagnostics"
        );
        server.assert_read_only_requests(1);
    }

    fn settled_sample(bound: &BoundGqt, input: &ServedInput) -> GqtRepObservation {
        let steps = bound
            .plan
            .queries
            .parse()
            .unwrap()
            .steps()
            .into_iter()
            .map(|step| GqtStepObservation {
                ordinal: step.ordinal,
                occurrence: 1,
                kind: step.kind.into(),
                elapsed_us: 7,
            })
            .collect();
        GqtRepObservation {
            repetition: 1,
            input_physical_digest_sha256: None,
            elapsed_us: 7,
            peak_rss_bytes: None,
            outcome: "expectations-passed".into(),
            logical_store_calls: None,
            control_store_calls: None,
            steps,
            verification: GqtVerification {
                selected_assertion_passed: true,
                assertions_passed: 3,
                following_assertions: 1,
            },
            merge: None,
            server_receipt_sha256: Some(input.receipt.digest().unwrap()),
            client_peak_rss_bytes: None,
        }
    }

    #[test]
    fn served_sample_rejects_forged_proof_and_local_measurement_fields() {
        let (_directory, plan) = fixture();
        let (bound, input) = served_input(&plan, "http://127.0.0.1:9", TOKEN);
        let sample = settled_sample(&bound, &input);
        let expected = proof(&input);
        let accepts = |sample: &GqtRepObservation, expected: &PreparationProofV2, parent: bool| {
            validate_sample(sample, &bound, 1, expected, sample.elapsed_us, parent).is_ok()
        };
        assert!(accepts(&sample, &expected, false));
        assert!(!accepts(
            &sample,
            &PreparationProofV2::Served {
                server_receipt_sha256: "d".repeat(64)
            },
            false
        ));
        let embedded = RepetitionInputV2::Embedded {
            repetition_root: PathBuf::from("/unused"),
            physical_digest: crate::reset::PhysicalDigest {
                files: 1,
                bytes: 1,
                digest_sha256: "e".repeat(64),
            },
            metadata_digest: crate::reset::MetadataDigest {
                entries: 1,
                files: 1,
                directories: 0,
                bytes: 1,
                shape_sha256: "f".repeat(64),
                state_sha256: "a".repeat(64),
            },
            fixture_manifest_sha256: "b".repeat(64),
        };
        assert!(embedded.validate(&bound).is_err());
        assert!(!accepts(&sample, &embedded.proof().unwrap(), false));
        let mut changed = sample.clone();
        changed.input_physical_digest_sha256 = Some("e".repeat(64));
        assert!(!accepts(&changed, &expected, false));
        let mut changed = sample.clone();
        changed.logical_store_calls = Some(crate::runner::LogicalStoreCallObservation {
            manifest: Default::default(),
            table: Default::default(),
            physical_attempts_observed: false,
        });
        assert!(!accepts(&changed, &expected, false));
        let mut changed = sample.clone();
        changed.control_store_calls = Some(crate::runner::ControlCallObservation {
            read_text: 0,
            read_text_if_exists: 0,
            read_text_versioned: 0,
            exists: 0,
            list_dir: 0,
            mutation_calls: 0,
            write_text: 0,
            delete: 0,
        });
        assert!(!accepts(&changed, &expected, false));
        let mut changed = sample.clone();
        changed.peak_rss_bytes = Some(1024);
        assert!(!accepts(&changed, &expected, false));
        assert!(!accepts(&sample, &expected, true));
        let mut parent_sample = sample.clone();
        parent_sample.client_peak_rss_bytes = Some(1024);
        assert!(!accepts(&parent_sample, &expected, false));
        assert!(accepts(&parent_sample, &expected, true));
    }

    #[tokio::test]
    async fn served_invalid_secret_or_binding_fails_before_callbacks_or_http() {
        let server = ReadServer::start(TOKEN, SuffixReply::Rows);
        let (_directory, plan) = fixture();
        let (bound, mut input) = served_input(&plan, &server.url, TOKEN);
        input.target.token = Some("secret\nvalue".into());
        let mut signals = server.signals();
        let error = execute_served_rep_signaled(1, &bound, &input, &mut signals)
            .await
            .unwrap_err();
        assert_eq!(error.code, "gqt_prepare_failed");
        assert!(error.context.gqt_settled_sample.is_none());
        assert!(signals.elapsed.is_none());
        assert!(server.events.lock().unwrap().is_empty());
        assert!(server.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn served_input_on_engine_point_fails_before_http() {
        let server = ReadServer::start(TOKEN, SuffixReply::Rows);
        let (directory, plan) = fixture();
        let queries = directory.path().join("engine_queries.gqt");
        std::fs::write(
            &queries,
            plan.queries
                .text
                .replace("target: omnigraph-server", "target: omnigraph-engine"),
        )
        .unwrap();
        let engine = explicit_pair_in_environment(
            &directory.path().join("dataset.gqt"),
            &queries,
            plan.definition.workload.measured_step.clone(),
            GqtEnvironment::embedded(plan.definition.environment.backend.clone()),
            ResetMode::LocalClonefile,
            Some(5),
        )
        .unwrap();
        let (_, input) = served_input(&plan, &server.url, TOKEN);
        let bound = input.receipt.bind(&engine).unwrap();
        input.validate(&bound).unwrap();
        let mut signals = server.signals();
        let error = execute_served_rep_signaled(1, &bound, &input, &mut signals)
            .await
            .unwrap_err();
        assert!(
            error
                .message
                .contains("served input requires a server point"),
            "{}",
            error.message
        );
        assert!(signals.elapsed.is_none());
        assert!(server.events.lock().unwrap().is_empty());
        assert!(server.requests.lock().unwrap().is_empty());
    }

    #[test]
    fn served_private_request_round_trips_while_ready_proof_is_secret_free() {
        let (_directory, plan) = fixture();
        let (bound, input) = served_input(&plan, "http://127.0.0.1:9", TOKEN);
        let execution = RepetitionInputV2::Served {
            input: Box::new(input.clone()),
        };
        execution.validate(&bound).unwrap();
        let request = ParentFrameV2::Request {
            protocol_version: WORKER_PROTOCOL_VERSION,
            request: Box::new(WorkerRequestV2 {
                repetition: 1,
                expected_point_id: bound.point_id.clone(),
                expected_case_digest: bound.plan.case_digest.clone(),
                case: bound,
                worker_scratch_root: PathBuf::from("/scratch/worker-scratch-00000001"),
                execution,
            }),
        };
        assert!(!format!("{request:?}").contains(TOKEN));
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        let decoded = read_frame::<_, ParentFrameV2>(&mut std::io::Cursor::new(&bytes))
            .unwrap()
            .unwrap();
        assert_eq!(decoded, request);
        let public_proof = serde_json::to_string(&proof(&input)).unwrap();
        assert!(!public_proof.contains(TOKEN));
        assert!(!public_proof.contains(&input.target.url));
        assert!(!public_proof.contains("token"));
        let mut invalid_shape = serde_json::to_value(&request).unwrap();
        invalid_shape["request"]["execution"]["physical_digest"] = json!({"files": 0});
        assert!(serde_json::from_value::<ParentFrameV2>(invalid_shape).is_err());
    }
}

#[test]
fn served_admission_rejects_loop_writes_and_wrong_environment() {
    let p = plan("e2e-query-count-served");
    let suffix = "\n--- loop $i 0 2\n--- mutate\nquery write() { delete Person where name = \"added\" }\n--- expect affected: nodes=0 edges=0\n--- endloop\n";
    let text = format!("{}{suffix}", p.queries.text);
    let parsed = omnigraph_gqt_core::parse_case("served_loop_write", &text).unwrap();
    let error = admit_target_queries(
        &parsed,
        &p.definition.workload.measured_step,
        Target::Server,
    )
    .unwrap_err();
    assert!(error.contains("read-only"));

    for (from, to) in [
        ("target: omnigraph-server", "target: omnigraph-engine"),
        (
            "storage: local-filesystem",
            "storage: in-memory-object-store",
        ),
    ] {
        let text = p.queries.text.replace(from, to);
        let parsed = omnigraph_gqt_core::parse_case("served_wrong_environment", &text).unwrap();
        let error = admit_target_queries(
            &parsed,
            &p.definition.workload.measured_step,
            Target::Server,
        )
        .unwrap_err();
        assert!(error.contains("matching admitted local-filesystem environment"));
    }
}

#[test]
fn served_zero_warmup_has_uncontrolled_server_cache() {
    let mut p = plan("e2e-query-count-served");
    let first = p.queries.text.find("\n--- query\n").unwrap();
    let second = p.queries.text[first + 1..].find("\n--- query\n").unwrap() + first + 1;
    p.queries.text.replace_range(first..second, "");
    p.queries.sha256 = sha256_bytes(p.queries.text.as_bytes());
    p.definition.workload.measured_step.ordinal = 1;
    p.cache_condition = admit_target_queries(
        &p.queries.parse().unwrap(),
        &p.definition.workload.measured_step,
        Target::Server,
    )
    .unwrap();
    let p = replan(p).unwrap();
    assert_eq!(
        p.cache_condition,
        crate::case::CacheCondition {
            process: ProcessLifecycle::LongRunningServer,
            engine: EnginePreparation::Uncontrolled,
            page_cache: PageCacheCondition::Uncontrolled,
            program: crate::case::WarmupProgram::None,
            iterations: 0,
        }
    );
    let bound = p
        .bind(
            &"a".repeat(64),
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM,
        )
        .unwrap();
    for condition in [
        crate::case::CacheCondition {
            process: ProcessLifecycle::FreshPerRepetition,
            ..p.cache_condition.clone()
        },
        crate::case::CacheCondition {
            engine: EnginePreparation::PreparationOnly,
            ..p.cache_condition.clone()
        },
        crate::case::CacheCondition {
            page_cache: PageCacheCondition::ProgramConditioned,
            ..p.cache_condition.clone()
        },
        crate::case::CacheCondition {
            iterations: 1,
            ..p.cache_condition.clone()
        },
    ] {
        let mut identity = bound.identity.clone();
        identity.cache_condition = condition;
        assert!(validate_point_spec(&identity).is_err());
    }
}

/// Grows the receipt's engine feature flags to the largest receipt its own 8 KiB bound admits.
fn grow_receipt_to_bound(
    receipt: &mut ServerDeploymentReceiptV1,
    mut engine: crate::record::EngineConfigurationV1,
) {
    engine.feature_flags.clear();
    receipt.server.engine = Some(engine);
    let mut last_valid = receipt.clone();
    for index in 0..16 {
        receipt
            .server
            .engine
            .as_mut()
            .unwrap()
            .feature_flags
            .push(format!("{index:02}-{}", "x".repeat(1000)));
        if receipt.validate().is_err() {
            break;
        }
        last_valid = receipt.clone();
    }
    *receipt = last_valid;
    receipt.validate().unwrap();
}

#[tokio::test]
async fn served_evidence_budget_refuses_oversized_combined_sut() {
    let record = served_record().await;
    let GqtSutIdentityV1::Served(mut sut) = record.sut else {
        panic!("served fixture")
    };
    grow_receipt_to_bound(&mut sut.receipt, sut.client_build.engine.clone());
    assert!(
        sut.validate()
            .unwrap_err()
            .to_string()
            .contains("served SUT evidence exceeds")
    );
}

#[cfg(unix)]
mod process_supervision {
    use super::{plan, receipt_for};
    use crate::gqt_protocol::{
        ChildFrameV2, PreparationProofV2, RepetitionInputV2, WORKER_PROTOCOL_VERSION,
        digest_worker_executable, write_frame,
    };
    use crate::gqt_runner::{GqtRepObservation, GqtStepObservation, GqtVerification};
    use crate::gqt_served::ServedInput;
    use crate::gqt_supervisor::{SupervisedRepetition, SupervisionInput, supervise_repetition};
    use crate::supervisor::tests::{machine_identity, worker_build, worker_script};
    use omnigraph_gqt_core::ServerTarget;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    const EXCHANGE: &str = r#"
[ "$1" = __gqt-worker-v2 ] || exit 89
IFS= read -r request || exit 90
printf '%s\n' "$$" >> "$0.pids"
IFS= read -r frame < "$0.ready" || exit 92
printf '%s\n' "$frame"
IFS= read -r begin || exit 91
printf '%s\n' "$begin" >> "$0.begun"
if [ -f "$0.hold" ]; then
    while [ ! -f "$0.release" ]; do /bin/sleep 0.01; done
fi
IFS= read -r frame < "$0.settled" || exit 93
printf '%s\n' "$frame"
IFS= read -r frame < "$0.complete" || exit 94
printf '%s\n' "$frame"
exit 0
"#;

    fn sidecar(worker: &Path, suffix: &str) -> PathBuf {
        let mut path = worker.as_os_str().to_owned();
        path.push(format!(".{suffix}"));
        PathBuf::from(path)
    }

    fn input(worker: &Path, root: &Path) -> SupervisionInput {
        let planned = plan("e2e-query-count-served");
        let receipt = receipt_for(&planned, "http://127.0.0.1:9");
        let bound = receipt.bind(&planned).unwrap();
        let execution = RepetitionInputV2::Served {
            input: Box::new(ServedInput {
                target: ServerTarget {
                    url: "http://127.0.0.1:9".into(),
                    graph: receipt.graph.clone(),
                    token: Some("private-process-test-token".into()),
                },
                receipt,
            }),
        };
        execution.validate(&bound).unwrap();
        let scratch = root.join("worker-scratch-00000001");
        std::fs::create_dir_all(&scratch).unwrap();
        SupervisionInput {
            worker_executable: worker.to_owned(),
            expected_worker_executable_sha256: digest_worker_executable(worker).unwrap(),
            expected_machine: None,
            execution,
            repetition: 1,
            case: bound,
            worker_scratch_root: scratch,
            deadline: Some(Duration::from_secs(10)),
            auxiliary_deadline_override: Some(Duration::from_secs(10)),
        }
    }

    fn sample(input: &SupervisionInput) -> GqtRepObservation {
        let parsed = input.case.plan.queries.parse().unwrap();
        let descriptors = parsed.steps();
        assert_eq!(descriptors.len(), 3);
        assert_eq!(input.case.plan.definition.workload.measured_step.ordinal, 2);
        let PreparationProofV2::Served {
            server_receipt_sha256,
        } = input.execution.proof().unwrap()
        else {
            panic!("served fixture required")
        };
        GqtRepObservation {
            repetition: input.repetition,
            input_physical_digest_sha256: None,
            elapsed_us: 0,
            peak_rss_bytes: None,
            outcome: "expectations-passed".into(),
            logical_store_calls: None,
            control_store_calls: None,
            steps: descriptors
                .into_iter()
                .map(|step| GqtStepObservation {
                    ordinal: step.ordinal,
                    occurrence: 1,
                    kind: step.kind.into(),
                    elapsed_us: 0,
                })
                .collect(),
            verification: GqtVerification {
                selected_assertion_passed: true,
                assertions_passed: 3,
                following_assertions: 1,
            },
            merge: None,
            server_receipt_sha256: Some(server_receipt_sha256),
            client_peak_rss_bytes: None,
        }
    }

    fn write_json_frame(path: &Path, frame: &ChildFrameV2) {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, frame).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    fn prepare_frames(input: &SupervisionInput, proof: PreparationProofV2) {
        let digest = crate::model::sha256_bytes(&std::fs::read(&input.worker_executable).unwrap());
        assert_eq!(input.expected_worker_executable_sha256, digest);
        let mut build = worker_build();
        build.executable_sha256 = digest.clone();
        crate::runner::validate_worker_build_attestation(&build, &digest).unwrap();
        write_json_frame(
            &sidecar(&input.worker_executable, "ready"),
            &ChildFrameV2::Ready {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: input.repetition,
                point_id: input.case.point_id.clone(),
                case_digest: input.case.plan.case_digest.clone(),
                worker_build: Box::new(build),
                machine: Box::new(machine_identity()),
                proof,
            },
        );
        write_json_frame(
            &sidecar(&input.worker_executable, "settled"),
            &ChildFrameV2::Settled {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: input.repetition,
                elapsed_us: 0,
            },
        );
        write_json_frame(
            &sidecar(&input.worker_executable, "complete"),
            &ChildFrameV2::Complete {
                protocol_version: WORKER_PROTOCOL_VERSION,
                point_id: input.case.point_id.clone(),
                case_digest: input.case.plan.case_digest.clone(),
                sample: Box::new(sample(input)),
            },
        );
    }

    fn assert_client_rss(observed: SupervisedRepetition, expected: GqtRepObservation) {
        let mut sample = observed.sample;
        assert!(sample.client_peak_rss_bytes.is_some_and(|bytes| bytes > 0));
        assert!(sample.peak_rss_bytes.is_none());
        assert!(sample.logical_store_calls.is_none());
        assert!(sample.control_store_calls.is_none());
        assert!(sample.input_physical_digest_sha256.is_none());
        sample.client_peak_rss_bytes = None;
        assert_eq!(sample, expected);
        assert_eq!(observed.machine, machine_identity());
    }

    fn pids(worker: &Path) -> Vec<u32> {
        std::fs::read_to_string(sidecar(worker, "pids"))
            .unwrap_or_default()
            .split_inclusive('\n')
            .filter(|line| line.ends_with('\n'))
            .map(|line| line.trim_end().parse().unwrap())
            .collect()
    }

    #[test]
    fn served_supervisor_adds_only_client_rss_after_reaping() {
        let (_guard, directory, worker) = worker_script(EXCHANGE);
        let input = input(&worker, directory.path());
        let expected = sample(&input);
        let digest = input.expected_worker_executable_sha256.clone();
        prepare_frames(&input, input.execution.proof().unwrap());
        let ready_path = sidecar(&worker, "ready");
        let mut ready: ChildFrameV2 =
            serde_json::from_slice(&std::fs::read(&ready_path).unwrap()).unwrap();
        let ChildFrameV2::Ready { worker_build, .. } = &mut ready else {
            panic!("Ready frame required")
        };
        worker_build.source_tree_dirty = Some(true);
        write_json_frame(&ready_path, &ready);
        let observed = supervise_repetition(input).unwrap();
        assert_eq!(observed.worker_build.executable_sha256, digest);
        assert_eq!(observed.worker_build.source_tree_dirty, Some(true));
        assert_client_rss(observed, expected);
        assert_eq!(pids(&worker).len(), 1);
        assert!(sidecar(&worker, "begun").is_file());
    }

    #[test]
    fn served_supervisor_starts_distinct_client_processes() {
        let (_guard, directory, worker) = worker_script(EXCHANGE);
        let first = input(&worker, &directory.path().join("first"));
        let mut second = input(&worker, &directory.path().join("second"));
        second.expected_machine = Some(machine_identity());
        let expected = sample(&first);
        prepare_frames(&first, first.execution.proof().unwrap());
        std::fs::write(
            sidecar(&worker, "hold"),
            b"hold until both workers are alive",
        )
        .unwrap();
        let first_thread = std::thread::spawn(move || supervise_repetition(first));
        let second_thread = std::thread::spawn(move || supervise_repetition(second));
        let started = Instant::now();
        while pids(&worker).len() < 2 && started.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(5));
        }
        let logged = pids(&worker);
        std::fs::write(sidecar(&worker, "release"), b"release").unwrap();
        let first = first_thread.join().unwrap();
        let second = second_thread.join().unwrap();
        assert_eq!(logged.len(), 2, "both child processes must start");
        for result in [first, second] {
            assert_client_rss(result.unwrap(), expected.clone());
        }
        assert_eq!(
            std::fs::read_to_string(sidecar(&worker, "begun"))
                .unwrap()
                .lines()
                .count(),
            2
        );
    }

    #[test]
    fn served_supervisor_rejects_wrong_receipt_proof_before_begin() {
        let (_guard, directory, worker) = worker_script(EXCHANGE);
        let input = input(&worker, directory.path());
        prepare_frames(
            &input,
            PreparationProofV2::Served {
                server_receipt_sha256: "d".repeat(64),
            },
        );
        let error = supervise_repetition(input).unwrap_err();
        assert_eq!(error.code, "worker_protocol_error");
        let child = error
            .context
            .child_process
            .as_ref()
            .expect("spawned worker must have containment evidence");
        assert_eq!(child.stage, "prepare-protocol");
        assert!(child.direct_child_reaped);
        assert!(child.process_group_gone);
        assert!(child.stdio_closed_cleanly);
        assert_eq!(pids(&worker).len(), 1);
        assert!(
            !sidecar(&worker, "begun").exists(),
            "invalid proof must never release the selected operation"
        );
        assert!(error.context.gqt_settled_elapsed_us.is_none());
        assert!(error.context.gqt_settled_sample.is_none());
    }

    #[test]
    fn served_supervisor_validates_combined_evidence_size_before_begin() {
        let (_guard, directory, worker) = worker_script(EXCHANGE);
        let mut input = input(&worker, directory.path());
        let client_build = crate::record::sut_identity_from_build(
            &crate::runner::build_evidence(Some(&worker_build())).unwrap(),
        )
        .unwrap();
        let RepetitionInputV2::Served { input: served } = &mut input.execution else {
            panic!("served input")
        };
        super::grow_receipt_to_bound(&mut served.receipt, client_build.engine.clone());
        input.execution.validate(&input.case).unwrap();
        prepare_frames(&input, input.execution.proof().unwrap());
        let error = supervise_repetition(input).unwrap_err();
        assert_eq!(error.code, "worker_protocol_error");
        assert!(error.message.contains("served SUT evidence exceeds"));
        let child = error.context.child_process.as_ref().unwrap();
        assert_eq!(child.stage, "prepare-protocol");
        assert!(child.direct_child_reaped && child.process_group_gone);
        assert!(!sidecar(&worker, "begun").exists());
        assert!(error.context.gqt_settled_elapsed_us.is_none());
    }
}
