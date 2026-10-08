//! Controlled local HTTP comparisons. This diagnostic shares the parity
//! fixture and process owner; it does not publish RFC 0039 archive records.

#[path = "http_retention.rs"]
mod retention;

use super::http_perf_layout::observe_layout;
use super::http_soak::{client, post, sample_rss_kib};
use super::support::{
    HERMETIC_OPERATOR_HOME, HTTP_DIAGNOSTIC_LANCE_POOL_BYTES, cli_at, copy_dir, fixture,
    output_success, parity_configs_with_cli, spawn_server_with_cluster_binary,
};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use std::time::{Duration, Instant};

const WIDE_ROWS: usize = 4096;
const SMALL_ROWS: usize = 1000;
const BODY_LIMIT: u64 = 32 * 1024 * 1024;
const QUERY: &str =
    "query find() { match { $p: Person { name: \"Alice\" } } return { $p.name, $p.age } }";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    output: PathBuf,
    baseline: Executable,
    current: Executable,
    fixture_cli: Executable,
    abba_blocks: usize,
    query_samples: usize,
    export_samples: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Executable {
    binary: PathBuf,
    receipt: PathBuf,
}

impl Executable {
    fn attest(&self) -> Value {
        assert!(self.binary.is_absolute() && self.receipt.is_absolute());
        let receipt_bytes = fs::read(&self.receipt).unwrap();
        assert!(receipt_bytes.len() <= 1024 * 1024);
        let receipt: Value = serde_json::from_slice(&receipt_bytes).unwrap();
        assert_eq!(
            receipt["profile"], "release",
            "release build receipt required"
        );
        let digest = file_digest(&self.binary);
        assert_eq!(
            receipt["binary_sha256"], digest,
            "binary differs from build receipt"
        );
        json!({"path":self.binary, "binary_sha256":digest,
            "build_receipt_sha256":hex_digest(&receipt_bytes), "build_receipt":receipt})
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct FileIdentity {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Physical {
    sha256: String,
    files: Vec<FileIdentity>,
}

fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn file_digest(path: &Path) -> String {
    let mut file = File::open(path).unwrap();
    let mut hasher = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    format!("{:x}", hasher.finalize())
}

fn physical(root: &Path) -> Physical {
    fn visit(root: &Path, directory: &Path, files: &mut Vec<FileIdentity>) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                visit(root, &entry.path(), files);
            } else {
                assert!(
                    kind.is_file(),
                    "fixture must contain only regular files/directories"
                );
                files.push(FileIdentity {
                    path: entry
                        .path()
                        .strip_prefix(root)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                    bytes: entry.metadata().unwrap().len(),
                    sha256: file_digest(&entry.path()),
                });
            }
        }
    }
    let mut files = Vec::new();
    visit(root, root, &mut files);
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let sha256 = hex_digest(&serde_json::to_vec(&files).unwrap());
    Physical { sha256, files }
}

fn save(path: &Path, value: &impl Serialize) {
    let bytes = serde_json::to_vec_pretty(value).unwrap();
    fs::write(path, bytes).unwrap();
}

fn canonical_records(bytes: &[u8]) -> Result<BTreeSet<String>, String> {
    let mut records = BTreeSet::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut value: Value = serde_json::from_slice(line).map_err(|error| error.to_string())?;
        value.sort_all_objects();
        let canonical = serde_json::to_string(&value).unwrap();
        if !records.insert(canonical) {
            return Err("duplicate logical export record".into());
        }
    }
    Ok(records)
}

fn local_export(cli: &Path, graph: &Path) -> Vec<u8> {
    output_success(cli_at(cli).args(["export", "--store"]).arg(graph)).stdout
}

fn load(cli: &Path, graph: &Path, input: &Path) {
    output_success(
        cli_at(cli)
            .args(["load", "--mode", "merge", "--data"])
            .arg(input)
            .arg(graph),
    );
}

fn restore(template: &Path, active: &Path, expected: &Physical) {
    // Both paths belong to this newly-created diagnostic workspace. The active
    // graph always has this exact URI; native references are never relocated.
    fs::remove_dir_all(active).unwrap();
    copy_dir(template, active);
    assert_eq!(
        &physical(active),
        expected,
        "fixture reset differs from frozen bytes"
    );
}

struct State {
    name: &'static str,
    template: PathBuf,
    identity: Physical,
    layout: Value,
}

fn freeze(name: &'static str, graph: &Path, output: &Path, layout: Value) -> State {
    let template = output.join(format!("template-{name}"));
    let identity = physical(graph);
    copy_dir(graph, &template);
    assert_eq!(physical(&template), identity);
    save(
        &output.join(format!("fixture-{name}.json")),
        &json!({
            "state":name, "physical":identity, "layout":layout,
            "template":template, "active_path":graph,
        }),
    );
    State {
        name,
        template,
        identity,
        layout,
    }
}

fn prepare(config: &Config) -> (PathBuf, PathBuf, Vec<State>, BTreeSet<String>) {
    let cluster = parity_configs_with_cli(
        &config.output,
        &config.output.join("unused-local-twin.omni"),
        &fixture("test.pg"),
        &config.fixture_cli.binary,
    );
    let graph = cluster.join("graphs/parity.omni");
    let wide = config.output.join("wide.jsonl");
    let mut input = File::create(&wide).unwrap();
    for row in 0..WIDE_ROWS {
        serde_json::to_writer(
            &mut input,
            &json!({"type":"Person", "data":{
                "name":format!("bench-wide-{row:04}-{}", "x".repeat(2048)), "age":12
            }}),
        )
        .unwrap();
        input.write_all(b"\n").unwrap();
    }
    input.flush().unwrap();
    load(&config.fixture_cli.binary, &graph, &wide);
    let base = freeze("base", &graph, &config.output, observe_layout(&graph));
    let small = config.output.join("small.jsonl");
    let mut input = File::create(&small).unwrap();
    for row in 0..SMALL_ROWS {
        serde_json::to_writer(
            &mut input,
            &json!({"type":"Person", "data":{
                "name":format!("bench-small-{row:04}"), "age":row
            }}),
        )
        .unwrap();
        input.write_all(b"\n").unwrap();
    }
    input.flush().unwrap();
    load(&config.fixture_cli.binary, &graph, &small);
    let expected = canonical_records(&local_export(&config.fixture_cli.binary, &graph)).unwrap();
    assert_eq!(expected.len(), WIDE_ROWS + SMALL_ROWS + 11);
    let bulk = freeze("bulk", &graph, &config.output, observe_layout(&graph));

    restore(&base.template, &graph, &base.identity);
    let server = spawn_server_with_cluster_binary(&cluster, &config.current.binary);
    let control = client();
    let builder_origin = Instant::now();
    let mut builder_rss =
        vec![json!({"committed":0,"elapsed_ms":0,"rss_kib":sample_rss_kib(server.id())})];
    let mut last_version = 0;
    for row in 0..SMALL_ROWS {
        let body: Value = post(&control, &server.base_url, "mutate", json!({
            "branch":"main",
            "query":"query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
            "params":{"name":format!("bench-small-{row:04}"), "age":row}
        })).unwrap().error_for_status().unwrap().json().unwrap();
        let version = body["commit"]["graph_manifest_version"].as_u64().unwrap();
        assert!(version > last_version);
        assert_eq!(body["affected_nodes"], 1);
        assert!(body["commit"]["graph_commit_id"].is_string());
        last_version = version;
        if (row + 1) % 100 == 0 {
            builder_rss.push(json!({"committed":row+1,"elapsed_ms":builder_origin.elapsed().as_millis(),"rss_kib":sample_rss_kib(server.id())}));
        }
    }
    std::thread::sleep(Duration::from_secs(5));
    save(
        &config.output.join("fixture-builder-memory.json"),
        &json!({
            "pid":server.id(),"samples":builder_rss,"post_idle_seconds":5,
            "post_idle_elapsed_ms":builder_origin.elapsed().as_millis(),
            "post_idle_rss_kib":sample_rss_kib(server.id()),
            "measurement":"descriptive fixture preparation; outside A/B timing",
        }),
    );
    fs::write(
        config.output.join("fixture-builder-server.log"),
        server.stop(),
    )
    .unwrap();
    assert_eq!(
        canonical_records(&local_export(&config.fixture_cli.binary, &graph)).unwrap(),
        expected
    );
    let fragmented = freeze("fragmented", &graph, &config.output, observe_layout(&graph));

    let optimized = output_success(
        cli_at(&config.fixture_cli.binary)
            .args(["optimize", "--json"])
            .arg(&graph),
    );
    fs::write(
        config.output.join("fixture-optimize.json"),
        &optimized.stdout,
    )
    .unwrap();
    assert_eq!(
        canonical_records(&local_export(&config.fixture_cli.binary, &graph)).unwrap(),
        expected
    );
    let optimized = freeze(
        "maintenance-optimized",
        &graph,
        &config.output,
        observe_layout(&graph),
    );
    (cluster, graph, vec![bulk, fragmented, optimized], expected)
}

#[derive(Default, Serialize)]
struct Samples {
    query_us: Vec<u64>,
    validated_query_count: usize,
    export_us: Vec<u64>,
    validated_export_count: usize,
    export_bytes: Vec<usize>,
    // Time since process measurement owner started, phase id, sampled KiB.
    rss: Vec<(u64, u8, u64)>,
    post_idle_rss_kib: Option<u64>,
}

fn receive(
    client: &Client,
    base: &str,
    route: &str,
    body: Value,
    deadline: Instant,
) -> Result<Vec<u8>, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err("repetition deadline elapsed".into());
    }
    let response = client
        .post(format!("{base}/graphs/parity/{route}"))
        .bearer_auth("parity-tok")
        .json(&body)
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
        return Err("response exceeds diagnostic bound".into());
    }
    if !status.is_success() {
        return Err(format!(
            "HTTP {status}: {}",
            String::from_utf8_lossy(&bytes[..bytes.len().min(4096)])
        ));
    }
    Ok(bytes)
}

fn verify_read(bytes: &[u8]) -> Result<(), String> {
    let body: Value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    if body["rows"] != json!([{"p.name":"Alice", "p.age":30}]) || body["row_count"] != 1 {
        return Err("query result differs from authored expectation".into());
    }
    Ok(())
}

fn verify_export(bytes: &[u8], expected: &BTreeSet<String>) -> Result<(), String> {
    if &canonical_records(bytes)? != expected {
        return Err("export content differs from frozen logical fixture".into());
    }
    Ok(())
}

fn run_repetition(
    config: &Config,
    state: &State,
    expected: &BTreeSet<String>,
    cluster: &Path,
    arm: &str,
    order: usize,
    record_path: &Path,
) -> Result<(), String> {
    let executable = if arm == "baseline" {
        &config.baseline
    } else {
        &config.current
    };
    let thermal_before = thermal_observation();
    let server = spawn_server_with_cluster_binary(cluster, &executable.binary);
    let control = client();
    let pid = server.id();
    let origin = Instant::now();
    let deadline = origin + Duration::from_secs(120);
    let phase = Arc::new(AtomicU8::new(1));
    let stopped = Arc::new(AtomicBool::new(false));
    let sampler_phase = Arc::clone(&phase);
    let sampler_stopped = Arc::clone(&stopped);
    let sampler = std::thread::spawn(move || {
        let mut rows = Vec::new();
        while !sampler_stopped.load(Ordering::Relaxed) {
            if let Some(kib) = sample_rss_kib(pid) {
                rows.push((
                    origin.elapsed().as_millis() as u64,
                    sampler_phase.load(Ordering::Relaxed),
                    kib,
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        rows
    });
    let mut samples = Samples::default();
    let outcome = (|| -> Result<(), String> {
        let query = json!({"branch":"main", "query":QUERY});
        for _ in 0..25 {
            verify_read(&receive(
                &control,
                &server.base_url,
                "query",
                query.clone(),
                deadline,
            )?)?;
        }
        for _ in 0..2 {
            verify_export(
                &receive(
                    &control,
                    &server.base_url,
                    "export",
                    json!({"branch":"main"}),
                    deadline,
                )?,
                expected,
            )?;
        }
        phase.store(2, Ordering::Relaxed);
        for _ in 0..config.query_samples {
            let begin = Instant::now();
            let body = receive(&control, &server.base_url, "query", query.clone(), deadline);
            let elapsed = begin.elapsed().as_micros() as u64;
            samples.query_us.push(elapsed);
            verify_read(&body?)?;
            samples.validated_query_count += 1;
        }
        phase.store(3, Ordering::Relaxed);
        for _ in 0..config.export_samples {
            let begin = Instant::now();
            let body = receive(
                &control,
                &server.base_url,
                "export",
                json!({"branch":"main"}),
                deadline,
            );
            let elapsed = begin.elapsed().as_micros() as u64;
            samples.export_us.push(elapsed);
            let body = body?;
            samples.export_bytes.push(body.len());
            verify_export(&body, expected)?;
            samples.validated_export_count += 1;
        }
        phase.store(4, Ordering::Relaxed);
        std::thread::sleep(Duration::from_secs(5));
        samples.post_idle_rss_kib = sample_rss_kib(pid);
        Ok(())
    })();
    stopped.store(true, Ordering::Relaxed);
    samples.rss = sampler.join().unwrap();
    let thermal_after = thermal_observation();
    fs::write(record_path.with_extension("server.log"), server.stop()).unwrap();
    save(
        record_path,
        &json!({
            "format":"omnigraph-http-controlled-diagnostic-v1", "claim_eligible":false,
            "arm":arm, "state":state.name, "order":order, "block":order / 12, "position_in_block":order % 4, "pid":pid,
        "logical_export_sha256":hex_digest(&serde_json::to_vec(expected).unwrap()),
            "binary_sha256":file_digest(&executable.binary),
            "fixture_physical_sha256":state.identity.sha256,
            "layout":state.layout, "status":if outcome.is_ok(){"complete"}else{"failed"},
            "error":outcome.as_ref().err(), "raw_samples":samples,
            "thermal_observation":{"before":thermal_before,"after":thermal_after},
            "rss_phase_ids":{"1":"warmup","2":"serial-query","3":"serial-export","4":"post-idle"},
        }),
    );
    outcome
}

fn thermal_observation() -> Value {
    if cfg!(target_os = "macos") {
        command_description("/usr/bin/pmset", &["-g", "therm"])
    } else {
        json!({"unavailable":"thermal sampler is implemented only for macOS"})
    }
}

fn hardware_observation() -> Value {
    if cfg!(target_os = "macos") {
        json!({
            "cpu_model":command_description("/usr/sbin/sysctl", &["-n", "machdep.cpu.brand_string"]),
            "physical_ram_bytes":command_description("/usr/sbin/sysctl", &["-n", "hw.memsize"]),
        })
    } else {
        json!({
            "cpuinfo":fs::read_to_string("/proc/cpuinfo").ok(),
            "meminfo":fs::read_to_string("/proc/meminfo").ok(),
        })
    }
}

fn command_description(program: &str, args: &[&str]) -> Value {
    match std::process::Command::new(program).args(args).output() {
        Ok(output) => {
            json!({"success":output.status.success(), "stdout":String::from_utf8_lossy(&output.stdout), "stderr":String::from_utf8_lossy(&output.stderr)})
        }
        Err(error) => json!({"unavailable":error.to_string()}),
    }
}

#[test]
#[ignore = "instrument: controlled release-server comparison; requires attested binaries and explicit config"]
fn controlled_http_comparison() {
    let config_path =
        std::env::var_os("OMNIGRAPH_HTTP_BENCH_CONFIG").expect("set OMNIGRAPH_HTTP_BENCH_CONFIG");
    let input = fs::read(config_path).unwrap();
    let config: Config = serde_json::from_slice(&input).unwrap();
    assert!((1..=6).contains(&config.abba_blocks));
    assert!((1..=2000).contains(&config.query_samples));
    assert!((1..=32).contains(&config.export_samples));
    assert!(config.output.is_absolute());
    if config.output.exists() {
        assert!(
            fs::read_dir(&config.output).unwrap().next().is_none(),
            "output directory must be empty"
        );
    }
    fs::create_dir_all(&config.output).unwrap();
    let baseline = config.baseline.attest();
    let current = config.current.attest();
    let fixture_cli = config.fixture_cli.attest();
    save(
        &config.output.join("session.json"),
        &json!({
            "format":"omnigraph-http-controlled-diagnostic-v1", "claim_eligible":false,
            "config":serde_json::from_slice::<Value>(&input).unwrap(),
            "sut":{"baseline":baseline,"current":current}, "fixture_cli":fixture_cli,
            "runtime_environment":{"inheritance":"cleared for fixture CLI and both server arms",
                "common":{"LANCE_MEM_POOL_SIZE":HTTP_DIAGNOSTIC_LANCE_POOL_BYTES,
                    "OMNIGRAPH_HOME":HERMETIC_OPERATOR_HOME,"LANG":"C","LC_ALL":"C"},
                "server_only":{"OMNIGRAPH_SERVER_BEARER_TOKENS_JSON":"disposable parity fixture token"}},
            "protocol":{"surface":"HTTP/1.1 loopback", "arrival":"unscheduled serial requests",
                "process":"fresh per repetition", "warmup":{"queries":25,"exports":2},
                "reset":"full byte-verified plain copy to one stable active graph path",
                "os_page_cache":"uncontrolled; copy/hash and warmup read fixture bytes",
                "timing":"request dispatch through complete response bytes; client verification excluded",
                "post_idle_seconds":5,"request_deadline_seconds":20,"repetition_deadline_seconds":120,
                "schedule":"3-state rotated ABBA blocks; requested block count captured in config",
                "storage_request_counts":"not captured", "native_settlement":"not qualified",
            "margin_fraction":0.20,
            "comparison_rule":"every paired-block ratio > 1 + maximum symmetric within-block same-arm gap + margin"},
            "machine":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
                "hardware":hardware_observation(), "isolation":"not qualified",
                "available_parallelism":std::thread::available_parallelism().map(|n|n.get()).ok(),
                "uname":command_description("uname", &["-a"]),
                "volume":command_description("df", &["-P",config.output.to_str().unwrap()]),
                "filesystem":if cfg!(target_os="macos") { command_description("diskutil", &["info","-plist",config.output.to_str().unwrap()]) } else { command_description("stat", &["--file-system","--format=%T",config.output.to_str().unwrap()]) }},
        }),
    );
    let (cluster, graph, states, expected) = prepare(&config);
    save(&config.output.join("logical-records.json"), &expected);
    save(
        &config.output.join("logical-fixture.json"),
        &json!({
            "schema_sha256":file_digest(&fixture("test.pg")),
            "records":expected.len(), "logical_export_sha256":hex_digest(&serde_json::to_vec(&expected).unwrap()),
            "builder":"parity-shared-seed-wide4096-small1000-v1",
            "maintenance_note":"public optimize changes compaction and may change index state; not a pure-fragment intervention",
        }),
    );
    let mut order = 0;
    for block in 0..config.abba_blocks {
        for offset in 0..states.len() {
            let state = &states[(offset + block) % states.len()];
            for arm in ["baseline", "current", "current", "baseline"] {
                restore(&state.template, &graph, &state.identity);
                let record = config
                    .output
                    .join(format!("sample-{order:03}-{}-{arm}.json", state.name));
                let outcome =
                    run_repetition(&config, state, &expected, &cluster, arm, order, &record);
                let after = physical(&graph);
                let matched = after == state.identity;
                let mut saved: Value = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
                saved["physical_verified"] = json!(matched);
                saved["post_fixture_physical_sha256"] = json!(after.sha256);
                if !matched {
                    saved["status"] = json!("failed");
                    saved["physical_error"] =
                        json!("read-only measurement changed frozen physical fixture");
                }
                save(&record, &saved);
                assert_eq!(
                    after,
                    state.identity,
                    "read-only measurement changed physical fixture; record {}",
                    record.display()
                );
                outcome.unwrap_or_else(|error| {
                    panic!("measurement failed; {}: {error}", record.display())
                });
                println!("HTTP_BENCH_SAMPLE {}", record.display());
                order += 1;
            }
        }
    }
    assert_eq!(order, config.abba_blocks * states.len() * 4);
    save(
        &config.output.join("complete.json"),
        &json!({"repetitions":order,"status":"complete","claim_eligible":false,
            "final_sut_attestation":{"baseline":config.baseline.attest(), "current":config.current.attest(), "fixture_cli":config.fixture_cli.attest()}}),
    );
}
