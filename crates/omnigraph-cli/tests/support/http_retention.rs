//! Current-only fixed-state retention control, separate from cross-binary timing.

use super::super::http_soak::{export_slot_refusal, read_body};
use super::*;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionConfig {
    fixture_session: PathBuf,
    output: PathBuf,
    seconds: u64,
    repetitions: usize,
}

#[derive(Default, Serialize)]
struct Census {
    offered: usize,
    completed: usize,
    refused: usize,
    abandoned: usize,
    failed: usize,
    errors: Vec<String>,
    successful_modes: BTreeMap<String, usize>,
    // Start/end since repetition origin in ms, full-body latency in us.
    // Reader timing ends before parsing; stream timings include consumer pauses.
    samples: Vec<(u64, u64, u64)>,
    body_windows_ms: Vec<(u64, u64)>,
    completed_body_bytes: usize,
}

impl Census {
    fn fail(&mut self, error: String) {
        self.failed += 1;
        if self.errors.len() < 8 {
            self.errors.push(error);
        }
    }
}

fn baseline_records(
    bytes: &[u8],
    expected: &BTreeSet<String>,
    expected_head: &Value,
) -> Result<(), String> {
    let lines: Vec<_> = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    let Some(last) = lines.last() else {
        return Err("empty baseline".into());
    };
    let terminal: Value = serde_json::from_slice(last).map_err(|error| error.to_string())?;
    let cursor = &terminal["baseline"];
    if cursor["resume_cursor"].as_str().is_none_or(str::is_empty)
        || &cursor["snapshot_commit_id"] != expected_head
    {
        return Err("baseline lacks a terminal cursor for the frozen graph head".into());
    }
    // Any earlier cursor is an unexpected record, so exactly one is accepted.
    let records = lines[..lines.len() - 1].join(&b'\n');
    verify_export(&records, expected)
}

fn run_reader(base: String, origin: Instant, deadline: Instant) -> Census {
    let control = client();
    let mut census = Census::default();
    while Instant::now() < deadline {
        census.offered += 1;
        let begin = Instant::now();
        let body = receive(
            &control,
            &base,
            "query",
            json!({"branch":"main","query":QUERY}),
            deadline + Duration::from_secs(20),
        );
        census.samples.push((
            begin.duration_since(origin).as_millis() as u64,
            origin.elapsed().as_millis() as u64,
            begin.elapsed().as_micros() as u64,
        ));
        match body.and_then(|body| verify_read(&body)) {
            Ok(()) => census.completed += 1,
            Err(error) => {
                census.fail(error);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    census
}

fn run_streams(
    base: String,
    origin: Instant,
    deadline: Instant,
    expected: Arc<BTreeSet<String>>,
    head: Value,
) -> Census {
    let control = client();
    let mut census = Census::default();
    let mut mode_index = 0;
    let modes = [
        ("export", "fast"),
        ("changes/baseline", "fast"),
        ("export", "pause"),
        ("changes/baseline", "pause"),
        ("export", "abandon"),
        ("changes/baseline", "abandon"),
    ];
    while Instant::now() < deadline {
        let (route, mode) = modes[mode_index % modes.len()];
        census.offered += 1;
        let begin = Instant::now();
        let outcome = (|| -> Result<(), String> {
            let response = post(&control, &base, route, json!({"branch":"main"}))
                .map_err(|error| error.to_string())?;
            let status = response.status();
            if !status.is_success() {
                let mut bytes = Vec::new();
                response
                    .take(4097)
                    .read_to_end(&mut bytes)
                    .map_err(|error| error.to_string())?;
                let body: Value =
                    serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
                if export_slot_refusal(status, &body) {
                    census.refused += 1;
                    return Ok(());
                }
                return Err(format!(
                    "HTTP {status}: {}",
                    String::from_utf8_lossy(&bytes)
                ));
            }
            let consumed = read_body(response, mode, origin)?;
            if let Some(window) = consumed.window {
                census.body_windows_ms.push(window);
            }
            if let Some(body) = consumed.body {
                if route == "changes/baseline" {
                    baseline_records(&body, &expected, &head)?;
                } else {
                    verify_export(&body, &expected)?;
                }
                census.completed += 1;
                census.completed_body_bytes += body.len();
            } else {
                census.abandoned += 1;
            }
            *census
                .successful_modes
                .entry(format!("{route}:{mode}"))
                .or_default() += 1;
            mode_index += 1;
            Ok(())
        })();
        census.samples.push((
            begin.duration_since(origin).as_millis() as u64,
            origin.elapsed().as_millis() as u64,
            begin.elapsed().as_micros() as u64,
        ));
        if let Err(error) = outcome {
            census.fail(error);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    census
}

fn final_snapshot(
    control: &Client,
    base: &str,
    expected: &BTreeSet<String>,
    deadline: Instant,
    refusals: &mut usize,
) -> Result<(), String> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("final export slot did not become available in 20s".into());
        }
        let response = control
            .post(format!("{base}/graphs/parity/export"))
            .bearer_auth("parity-tok")
            .json(&json!({"branch":"main"}))
            .timeout(remaining.min(Duration::from_secs(20)))
            .send()
            .map_err(|error| error.to_string())?;
        let status = response.status();
        let mut bytes = Vec::new();
        response
            .take(BODY_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        if bytes.len() as u64 > BODY_LIMIT {
            return Err("final export exceeds bound".into());
        }
        if status.is_success() {
            return verify_export(&bytes, expected);
        }
        let body = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        if !export_slot_refusal(status, &body) {
            return Err(format!("unexpected final export HTTP {status}: {body}"));
        }
        *refusals += 1;
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn run_control(
    config: &RetentionConfig,
    sut: &Executable,
    cluster: &Path,
    expected: Arc<BTreeSet<String>>,
    head: &Value,
    repetition: usize,
) -> Value {
    let thermal_before = thermal_observation();
    let server = spawn_server_with_cluster_binary(cluster, &sut.binary);
    let pid = server.id();
    let origin = Instant::now();
    let phase = Arc::new(AtomicU8::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler_phase = Arc::clone(&phase);
    let sampler_stop = Arc::clone(&stop);
    let sampler = std::thread::spawn(move || {
        let mut rows = Vec::new();
        while !sampler_stop.load(Ordering::Relaxed) {
            if let Some(kib) = sample_rss_kib(pid) {
                rows.push((
                    origin.elapsed().as_millis() as u64,
                    sampler_phase.load(Ordering::Relaxed),
                    kib,
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        rows
    });
    let control = client();
    let mut readers = Vec::new();
    let mut streams = Census::default();
    let mut recovery = Vec::new();
    let mut final_slot_refusals = 0;
    let mut final_export_elapsed_ms = 0;
    let mut post_idle_rss = None;
    let mut load_window = None;
    let outcome = (|| -> Result<(), String> {
        let warmup_deadline = origin + Duration::from_secs(60);
        for _ in 0..25 {
            verify_read(&receive(
                &control,
                &server.base_url,
                "query",
                json!({"branch":"main","query":QUERY}),
                warmup_deadline,
            )?)?;
        }
        for _ in 0..2 {
            verify_export(
                &receive(
                    &control,
                    &server.base_url,
                    "export",
                    json!({"branch":"main"}),
                    warmup_deadline,
                )?,
                &expected,
            )?;
        }
        phase.store(2, Ordering::Relaxed);
        let begin = Instant::now();
        let deadline = begin + Duration::from_secs(config.seconds);
        let reader_handles: Vec<_> = (0..2)
            .map(|_| {
                let base = server.base_url.clone();
                std::thread::spawn(move || run_reader(base, origin, deadline))
            })
            .collect();
        let base = server.base_url.clone();
        let stream_expected = Arc::clone(&expected);
        let stream_head = head.clone();
        let stream_handle = std::thread::spawn(move || {
            run_streams(base, origin, deadline, stream_expected, stream_head)
        });
        readers = reader_handles
            .into_iter()
            .map(|worker| worker.join().expect("reader panicked"))
            .collect();
        streams = stream_handle.join().expect("stream consumer panicked");
        load_window = Some((
            begin.duration_since(origin).as_millis() as u64,
            origin.elapsed().as_millis() as u64,
        ));
        phase.store(3, Ordering::Relaxed);
        let final_begin = Instant::now();
        let result = final_snapshot(
            &control,
            &server.base_url,
            &expected,
            final_begin + Duration::from_secs(20),
            &mut final_slot_refusals,
        );
        final_export_elapsed_ms = final_begin.elapsed().as_millis() as u64;
        result?;
        phase.store(4, Ordering::Relaxed);
        let recovery_deadline = Instant::now() + Duration::from_secs(20);
        for _ in 0..25 {
            let begin = Instant::now();
            let body = receive(
                &control,
                &server.base_url,
                "query",
                json!({"branch":"main","query":QUERY}),
                recovery_deadline,
            );
            recovery.push((
                origin.elapsed().as_millis() as u64,
                begin.elapsed().as_micros() as u64,
            ));
            verify_read(&body?)?;
        }
        phase.store(5, Ordering::Relaxed);
        std::thread::sleep(Duration::from_secs(10));
        post_idle_rss = sample_rss_kib(pid);
        if readers
            .iter()
            .any(|row| row.failed != 0 || row.completed == 0)
            || streams.failed != 0
        {
            return Err("a worker failed or made no progress".into());
        }
        for route in ["export", "changes/baseline"] {
            for mode in ["fast", "pause", "abandon"] {
                if streams
                    .successful_modes
                    .get(&format!("{route}:{mode}"))
                    .copied()
                    .unwrap_or(0)
                    == 0
                {
                    return Err(format!("consumer mode was not exercised: {route}:{mode}"));
                }
            }
        }
        Ok(())
    })();
    stop.store(true, Ordering::Relaxed);
    let rss = sampler.join().unwrap();
    let thermal_after = thermal_observation();
    fs::write(
        config
            .output
            .join(format!("retention-{repetition}.server.log")),
        server.stop(),
    )
    .unwrap();
    json!({
        "format":"omnigraph-http-fixed-retention-v1", "claim_eligible":false,
        "status":if outcome.is_ok(){"complete"}else{"failed"}, "error":outcome.err(),
        "repetition":repetition,"pid":pid,"readers":readers,"streams":streams,
        "load_window_ms":load_window,"rss":rss,"recovery_reads":recovery,"post_idle_rss_kib":post_idle_rss,
        "final_export_slot_refusals":final_slot_refusals,"final_export_elapsed_ms":final_export_elapsed_ms,
        "rss_phase_ids":{"1":"warmup","2":"mixed-read-stream","3":"final-snapshot","4":"recovery-reads","5":"post-idle"},
        "thermal_observation":{"before":thermal_before,"after":thermal_after},
    })
}

#[test]
#[ignore = "instrument: current-only immutable-fixture retention control; requires a completed HTTP comparison"]
fn fixed_state_http_retention() {
    let config_path = std::env::var_os("OMNIGRAPH_HTTP_RETENTION_CONFIG")
        .expect("set OMNIGRAPH_HTTP_RETENTION_CONFIG");
    let input = fs::read(config_path).unwrap();
    let config: RetentionConfig = serde_json::from_slice(&input).unwrap();
    assert!((10..=600).contains(&config.seconds));
    assert!((1..=4).contains(&config.repetitions));
    assert!(config.output.is_absolute() && config.fixture_session.is_absolute());
    let source = &config.fixture_session;
    let session: Value =
        serde_json::from_slice(&fs::read(source.join("session.json")).unwrap()).unwrap();
    let source_config: Config = serde_json::from_value(session["config"].clone()).unwrap();
    assert_eq!(&source_config.output, source, "fixture cannot be relocated");
    let complete: Value =
        serde_json::from_slice(&fs::read(source.join("complete.json")).unwrap()).unwrap();
    assert_eq!(complete["status"], "complete");
    let attestation = source_config.current.attest();
    assert_eq!(
        attestation, session["sut"]["current"],
        "SUT differs from completed comparison"
    );
    if config.output.exists() {
        assert!(
            fs::read_dir(&config.output).unwrap().next().is_none(),
            "output must be empty"
        );
    }
    fs::create_dir_all(&config.output).unwrap();
    let fixture_bytes = fs::read(source.join("fixture-fragmented.json")).unwrap();
    let descriptor: Value = serde_json::from_slice(&fixture_bytes).unwrap();
    let identity: Physical = serde_json::from_value(descriptor["physical"].clone()).unwrap();
    let cluster = source.join("parity-cluster");
    let active = cluster.join("graphs/parity.omni");
    let template = source.join("template-fragmented");
    assert_eq!(descriptor["active_path"], json!(active));
    assert_eq!(descriptor["template"], json!(template));
    let expected: BTreeSet<String> =
        serde_json::from_slice(&fs::read(source.join("logical-records.json")).unwrap()).unwrap();
    let logical: Value =
        serde_json::from_slice(&fs::read(source.join("logical-fixture.json")).unwrap()).unwrap();
    let logical_digest = hex_digest(&serde_json::to_vec(&expected).unwrap());
    assert_eq!(logical["logical_export_sha256"], logical_digest);
    assert_eq!(logical["records"], expected.len());
    assert_eq!(physical(&template), identity, "frozen template changed");
    save(
        &config.output.join("session.json"),
        &json!({
            "format":"omnigraph-http-fixed-retention-v1", "claim_eligible":false,
            "config":serde_json::from_slice::<Value>(&input).unwrap(), "sut":attestation,
            "source_session_sha256":file_digest(&source.join("session.json")),
            "fixture_descriptor_sha256":hex_digest(&fixture_bytes),
            "logical_export_sha256":logical_digest,"fixture_physical_sha256":identity.sha256,
            "layout":descriptor["layout"], "runtime_environment":session["runtime_environment"],
            "protocol":{"full_duration":config.seconds>=300,"full_replication":config.repetitions>=2,
                "readers":2,"reader_think_ms":25,"consumer_think_ms":100,"pause_ms":1000,
                "consumer":"one stream; alternates export/baseline across fast/pause/abandon",
                "writes":0,"warmup":{"queries":25,"exports":2,"deadline_seconds":60},
                "request_deadline_seconds":20,"drain_bound_seconds":20,
                "final_export_deadline_seconds":20,"recovery_deadline_seconds":20,"idle_seconds":10,
                "reader_timing":"dispatch through complete bytes; validation excluded",
                "stream_timing":"diagnostic only; includes intentional consumer delays and validation",
                "retries":"only exact typed stream_export_slots refusal; never mutations"},
            "machine":{"hardware":hardware_observation(),"uname":command_description("uname", &["-a"]),"isolation":"not qualified"},
        }),
    );
    let expected = Arc::new(expected);
    for repetition in 0..config.repetitions {
        restore(&template, &active, &identity);
        let mut result = run_control(
            &config,
            &source_config.current,
            &cluster,
            Arc::clone(&expected),
            &descriptor["layout"]["graph_head"],
            repetition,
        );
        let after = physical(&active);
        result["physical_verified"] = json!(after == identity);
        result["fixture_physical_sha256"] = json!(identity.sha256);
        result["post_fixture_physical_sha256"] = json!(after.sha256);
        result["logical_export_sha256"] = json!(logical_digest);
        if after != identity {
            result["status"] = json!("failed");
            result["physical_error"] = json!("read-only retention workload changed frozen bytes");
        }
        let path = config.output.join(format!("retention-{repetition}.json"));
        save(&path, &result);
        assert_eq!(
            result["status"],
            "complete",
            "retention failed: {}",
            path.display()
        );
        println!("HTTP_RETENTION_SAMPLE {}", path.display());
    }
    save(
        &config.output.join("complete.json"),
        &json!({
            "status":"complete","repetitions":config.repetitions,"claim_eligible":false,
            "final_sut_attestation":source_config.current.attest(),
        }),
    );
}
