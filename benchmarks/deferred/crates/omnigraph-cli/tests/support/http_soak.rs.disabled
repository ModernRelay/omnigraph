//! Diagnostic HTTP workload using the parity owner's real CLI/server fixture.
//! Closed-loop clients and sampled RSS are observations, not capacity bounds.

use super::{assert_write_parity, cli, output_success, parity};
use reqwest::blocking::{Client, Response};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::sync::{
    Arc, Barrier, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

const ROWS: usize = 4096;
const MAX_BODY: usize = 32 * 1024 * 1024;

#[derive(Default, Serialize)]
struct Census {
    client: String,
    offered: usize,
    completed: usize,
    refused: usize,
    failed: usize,
    unknown: usize,
    abandoned: usize,
    bytes: usize,
    successful_modes: BTreeMap<String, usize>,
    refusal_reasons: BTreeMap<String, usize>,
    observed_writes: BTreeSet<usize>,
    latency_ms: Vec<u64>,
    completed_at_ms: Vec<u64>,
    slow_windows_ms: Vec<(u64, u64)>,
    acknowledged_writes: BTreeSet<usize>,
    errors: Vec<String>,
}

impl Census {
    fn error(&mut self, message: impl ToString, uncertain: bool) {
        if uncertain {
            self.unknown += 1;
        } else {
            self.failed += 1;
        }
        if self.errors.len() < 5 {
            self.errors.push(message.to_string());
        }
    }

    fn report(&self) -> Value {
        let mut latencies = self.latency_ms.clone();
        latencies.sort_unstable();
        let percentile = |p: usize| {
            latencies
                .get((latencies.len() * p).div_ceil(100).saturating_sub(1))
                .copied()
        };
        json!({"client": self.client, "offered": self.offered, "completed": self.completed,
            "refused": self.refused, "failed": self.failed, "unknown": self.unknown,
            "abandoned": self.abandoned, "completed_body_bytes": self.bytes, "successful_consumer_modes": self.successful_modes, "refusal_reasons":self.refusal_reasons,
            "attempt_including_validation_ms": {"p50": percentile(50), "p95": percentile(95), "p99": percentile(99)},
            "errors": self.errors})
    }
}

pub(super) fn client() -> Client {
    Client::builder()
        .http1_only()
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::HeaderName::from_static(omnigraph_api_types::HTTP_API_CONTRACT_HEADER),
            reqwest::header::HeaderValue::from_static(omnigraph_api_types::HTTP_API_CONTRACT),
        )]))
        .timeout(Duration::from_secs(20))
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

pub(super) fn post(
    client: &Client,
    base: &str,
    route: &str,
    body: Value,
) -> reqwest::Result<Response> {
    client
        .post(format!("{base}/graphs/parity/{route}"))
        .bearer_auth("parity-tok")
        .json(&body)
        .send()
}

pub(super) fn export_slot_refusal(status: reqwest::StatusCode, body: &Value) -> bool {
    status == reqwest::StatusCode::PAYLOAD_TOO_LARGE
        && body["code"] == json!(omnigraph_api_types::ErrorCode::BadRequest)
        && body["resource_limit"]["resource"] == "stream_export_slots"
        && body["resource_limit"]["limit"] == 1
        && body["resource_limit"]["actual"] == 2
}

pub(super) struct Consumed {
    pub(super) body: Option<Vec<u8>>,
    pub(super) window: Option<(u64, u64)>,
}

pub(super) fn read_body(
    mut response: Response,
    mode: &str,
    started: Instant,
) -> Result<Consumed, String> {
    let mut body = Vec::new();
    let mut first_byte = None;
    let mut buffer = [0; 16 * 1024];
    loop {
        let count = response
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        let at = started.elapsed().as_millis() as u64;
        if count == 0 {
            return Ok(Consumed {
                body: Some(body),
                window: first_byte.map(|first| (first, at)),
            });
        }
        if body.is_empty() {
            first_byte = Some(at);
            if mode == "abandon" {
                return Ok(Consumed {
                    body: None,
                    window: Some((at, at)),
                });
            }
            if mode == "pause" {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        if body.len() + count > MAX_BODY {
            return Err("response exceeded instrument's body bound".into());
        }
        body.extend_from_slice(&buffer[..count]);
        if mode == "throttle" {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

/// Full snapshots contain every immutable fixture record once plus a subset
/// of unique authored writes. Final verification reconciles that subset exactly
/// with acknowledged receipts, independently of server counters.
fn verify_snapshot(
    body: &[u8],
    fixture: &BTreeSet<String>,
    baseline: bool,
) -> Result<BTreeSet<usize>, String> {
    let mut remaining = fixture.clone();
    let mut writes = BTreeSet::new();
    let mut terminal = false;
    for line in body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let row: Value = serde_json::from_slice(line).map_err(|error| error.to_string())?;
        if terminal {
            return Err("record after baseline cursor".into());
        }
        if let Some(cursor) = row.get("baseline") {
            if !baseline
                || cursor["resume_cursor"].as_str().is_none()
                || cursor["snapshot_commit_id"].as_str().is_none()
            {
                return Err("invalid baseline terminal cursor".into());
            }
            terminal = true;
        } else if let Some(name) = row["data"]["name"]
            .as_str()
            .and_then(|name| name.strip_prefix("soak-write-"))
        {
            let sequence: usize = name.parse().map_err(|_| "invalid write key")?;
            if row["type"] != "Person"
                || row["data"]["age"] != json!(sequence)
                || !writes.insert(sequence)
            {
                return Err("corrupt or duplicated authored write".into());
            }
        } else if !remaining.remove(&serde_json::to_string(&row).unwrap()) {
            return Err("unexpected or duplicated fixture record".into());
        }
    }
    if !remaining.is_empty() || terminal != baseline {
        return Err("truncated snapshot or missing terminal cursor".into());
    }
    Ok(writes)
}

pub(super) fn sample_rss_kib(pid: u32) -> Option<u64> {
    #[cfg(unix)]
    {
        let output = std::process::Command::new("/bin/ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        if output.status.success() {
            return std::str::from_utf8(&output.stdout)
                .ok()?
                .trim()
                .parse()
                .ok();
        }
    }
    let _ = pid;
    None
}

#[test]
#[ignore = "instrument: real HTTP mixed-load soak; prints diagnostic latency/RSS, no performance thresholds"]
fn mixed_http_soak() {
    let seconds = std::env::var("OMNIGRAPH_HTTP_SOAK_SECONDS")
        .map(|value| value.parse::<u64>().expect("integer soak seconds"))
        .unwrap_or(60);
    assert!(
        (10..=600).contains(&seconds),
        "soak seconds must be 10..=600"
    );
    let p = parity();
    let data = p._temp.path().join("soak.jsonl");
    let mut file = std::io::BufWriter::new(std::fs::File::create(&data).unwrap());
    for row in 0..ROWS {
        serde_json::to_writer(&mut file, &json!({"type":"Person", "data":{"name":format!("soak-fixture-{row:04}-{}", "x".repeat(2048)), "age":12}})).unwrap();
        file.write_all(b"\n").unwrap();
    }
    file.flush().unwrap();
    let (local, remote) = p.run(&[
        "load",
        "--mode",
        "merge",
        "--data",
        data.to_str().unwrap(),
        "--json",
    ]);
    assert_write_parity("soak fixture", &local, &remote);
    let export = output_success(cli().args(["export", "--store", p.local.to_str().unwrap()]));
    let fixture = Arc::new(
        std::str::from_utf8(&export.stdout)
            .unwrap()
            .lines()
            .map(|line| {
                serde_json::to_string(&serde_json::from_str::<Value>(line).unwrap()).unwrap()
            })
            .collect::<BTreeSet<_>>(),
    );
    assert!(export.stdout.len() > 8 * 1024 * 1024);
    let query = json!({"branch":"main", "query":"query find() { match { $p: Person { name: \"Alice\" } } return { $p.name, $p.age } }"});
    let mut isolated_ms = Vec::new();
    let control = client();
    for _ in 0..10 {
        let started = Instant::now();
        let response: Value = post(&control, &p.server.base_url, "query", query.clone())
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(response["rows"], json!([{"p.name":"Alice", "p.age":30}]));
        isolated_ms.push(started.elapsed().as_millis() as u64);
    }
    let barrier = Arc::new(Barrier::new(6));
    let started = Instant::now();
    let stop_sampling = Arc::new(AtomicBool::new(false));
    let sampler_stop = Arc::clone(&stop_sampling);
    let pid = p.server.id();
    let sampler = std::thread::spawn(move || {
        let mut samples = Vec::new();
        while !sampler_stop.load(Ordering::Relaxed) {
            if let Some(rss) = sample_rss_kib(pid) {
                samples.push((started.elapsed().as_millis() as u64, rss));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        samples
    });
    let acknowledged = Arc::new(Mutex::new(BTreeSet::new()));
    let offered_writes = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for role in ["read-0", "read-1", "write", "export", "changes/baseline"] {
        let fixture = Arc::clone(&fixture);
        let acknowledged = Arc::clone(&acknowledged);
        let offered_writes = Arc::clone(&offered_writes);
        let barrier = Arc::clone(&barrier);
        let base = p.server.base_url.clone();
        let query = query.clone();
        workers.push(std::thread::spawn(move || {
            let client = client();
            let mut census = Census {
                client: role.to_string(),
                ..Default::default()
            };
            let mut last_version = 0;
            barrier.wait();
            while started.elapsed() < Duration::from_secs(seconds) {
                let attempt = census.offered;
                census.offered += 1;
                let begin = Instant::now();
                let write = role == "write";
                let stream = role == "export" || role == "changes/baseline";
                let mode = if stream {
                    ["throttle", "pause", "abandon"][(census.completed + census.abandoned) % 3]
                } else {
                    "normal"
                };
                let before = acknowledged.lock().unwrap().clone();
                let route = if write {
                    "mutate"
                } else if stream {
                    role
                } else {
                    "query"
                };
                let body = if write {
                    json!({
                        "branch": "main",
                        "query": "query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }",
                        "params": {"name": format!("soak-write-{attempt}"), "age": attempt}
                    })
                } else if stream {
                    json!({"branch":"main"})
                } else {
                    query.clone()
                };
                if write {
                    offered_writes.store(attempt + 1, Ordering::Release);
                }
                match post(&client, &base, route, body) {
                    Err(error) => census.error(error, write),
                    Ok(response) if !response.status().is_success() => {
                        let status = response.status();
                        let body: Value = response.json().unwrap_or(Value::Null);
                        let limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS
                            && body["code"] == json!(omnigraph_api_types::ErrorCode::TooManyRequests);
                        let stream_slot = stream && export_slot_refusal(status, &body);
                        if limited || stream_slot {
                            census.refused += 1;
                            let reason = format!(
                                "HTTP {status}: {}",
                                body["resource_limit"]["resource"].as_str().unwrap_or("too_many_requests")
                            );
                            *census.refusal_reasons.entry(reason).or_default() += 1;
                        } else {
                            census.error(format!("HTTP {status}: {body}"), write);
                        }
                    }
                    Ok(response) => match read_body(response, mode, started) {
                        Err(error) => census.error(error, write),
                        Ok(consumed) => {
                            if stream && let Some(window) = consumed.window {
                                census.slow_windows_ms.push(window);
                            }
                            if let Some(body) = consumed.body {
                                census.bytes += body.len();
                                let valid = if stream {
                                    verify_snapshot(&body, &fixture, role == "changes/baseline")
                                        .and_then(|writes| {
                                            if !before.is_subset(&writes) {
                                                return Err("snapshot omitted an already acknowledged write".into());
                                            }
                                            if writes.iter().any(|id| *id >= offered_writes.load(Ordering::Acquire)) {
                                                return Err("snapshot fabricated an unoffered write".into());
                                            }
                                            census.observed_writes.extend(writes);
                                            Ok(())
                                        })
                                } else {
                                    serde_json::from_slice::<Value>(&body)
                                        .map_err(|error| error.to_string())
                                        .and_then(|body| {
                                            if write {
                                                let commit = &body["commit"];
                                                let version = commit["graph_manifest_version"].as_u64().unwrap_or(0);
                                                if body["affected_nodes"] != 1
                                                    || commit["graph_commit_id"].as_str().is_none()
                                                    || version <= last_version
                                                {
                                                    return Err("invalid or non-monotonic mutation receipt".into());
                                                }
                                                last_version = version;
                                                census.acknowledged_writes.insert(attempt);
                                                acknowledged.lock().unwrap().insert(attempt);
                                            } else if body["rows"] != json!([{"p.name":"Alice", "p.age":30}]) {
                                                return Err("incorrect peer read".into());
                                            }
                                            Ok(())
                                        })
                                };
                                match valid {
                                    Ok(()) => {
                                        census.completed += 1;
                                        census.completed_at_ms.push(started.elapsed().as_millis() as u64);
                                        if stream {
                                            *census.successful_modes.entry(mode.to_string()).or_default() += 1;
                                        }
                                    }
                                    Err(error) => census.error(error, write),
                                }
                            } else {
                                census.abandoned += 1;
                                *census.successful_modes.entry(mode.to_string()).or_default() += 1;
                            }
                        }
                    }
                }
                census.latency_ms.push(begin.elapsed().as_millis() as u64);
                std::thread::sleep(Duration::from_millis(if write {
                    250
                } else if stream {
                    100
                } else {
                    25
                }));
            }
            census
        }));
    }
    barrier.wait();
    let census: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().expect("soak worker panicked"))
        .collect();
    stop_sampling.store(true, Ordering::Relaxed);
    let rss = sampler.join().unwrap();
    // An abandoned consumer can return before its producer releases the cut.
    // Only this exact read-only admission refusal is retried; uncertainty and
    // all other responses fail, and mutation attempts are never replayed.
    let final_export_started = Instant::now();
    let final_export_deadline = final_export_started + Duration::from_secs(20);
    let mut final_export_slot_refusals = 0;
    let final_response = loop {
        let remaining = final_export_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "export cut still held after 20s and {final_export_slot_refusals} refusals"
        );
        let response = control
            .post(format!("{}/graphs/parity/export", p.server.base_url))
            .bearer_auth("parity-tok")
            .json(&json!({"branch":"main"}))
            .timeout(remaining)
            .send()
            .unwrap();
        if response.status().is_success() {
            break response;
        }
        let status = response.status();
        let body: Value = response.json().unwrap();
        assert!(
            export_slot_refusal(status, &body),
            "unexpected final export response: {status}: {body}"
        );
        final_export_slot_refusals += 1;
        std::thread::sleep(
            Duration::from_millis(50)
                .min(final_export_deadline.saturating_duration_since(Instant::now())),
        );
    };
    let final_export_wait_ms = final_export_started.elapsed().as_millis();
    let final_body = read_body(final_response, "normal", started)
        .unwrap()
        .body
        .unwrap();
    let final_writes = verify_snapshot(&final_body, &fixture, false).unwrap();
    let writer = census.iter().find(|row| row.client == "write").unwrap();
    let windows: Vec<_> = census
        .iter()
        .flat_map(|row| row.slow_windows_ms.iter())
        .collect();
    let peer_progress: Vec<_> = census
        .iter()
        .filter(|row| row.client == "write" || row.client.starts_with("read"))
        .map(|row| {
            let completed_during_streams = row
                .completed_at_ms
                .iter()
                .filter(|at| {
                    windows
                        .iter()
                        .any(|(begin, end)| begin <= *at && *at <= end)
                })
                .count();
            (row.client.clone(), completed_during_streams)
        })
        .collect();
    let mut recovery_ms = Vec::new();
    for _ in 0..5 {
        let begin = Instant::now();
        let body: Value = post(&control, &p.server.base_url, "query", query.clone())
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(body["rows"], json!([{"p.name":"Alice", "p.age":30}]));
        recovery_ms.push(begin.elapsed().as_millis() as u64);
    }
    let quarter_medians: Vec<_> = (0..4)
        .map(|quarter| {
            let mut values: Vec<_> = rss
                .iter()
                .filter(|(at, _)| (at * 4 / (seconds * 1000)).min(3) == quarter)
                .map(|(_, value)| *value)
                .collect();
            values.sort_unstable();
            values.get(values.len() / 2).copied()
        })
        .collect();
    println!(
        "HTTP_SOAK {}",
        json!({
            "diagnostic_only":true,
            "backend":"local-filesystem",
            "protocol":"HTTP/1.1",
            "schedule":"five closed-loop clients; read pause 25ms, write pause 250ms, stream pause 100ms; 16KiB stream reads with 2ms throttle / 1s initial pause / abandon after first bytes",
            "duration_seconds":seconds,
            "elapsed_ms":started.elapsed().as_millis(),
            "server_pid":pid,
            "fixture_rows":ROWS,
            "fixture_bytes":export.stdout.len(),
            "clients":census.iter().map(Census::report).collect::<Vec<_>>(),
            "isolated_read_ms":isolated_ms,
            "recovery_read_ms":recovery_ms,
            "peer_completions_during_streams":peer_progress,
            "server_rss_kib":{"samples":rss.len(), "quarter_medians":quarter_medians, "after_recovery":sample_rss_kib(pid), "first":rss.first(), "last":rss.last(), "sampled_max":rss.iter().map(|(_, rss)|rss).max()},
            "final_export_slot_refusals":final_export_slot_refusals,
            "final_export_wait_ms":final_export_wait_ms,
            "final_authored_writes":final_writes.len()})
    );
    for row in &census {
        assert_eq!(
            row.offered,
            row.completed + row.refused + row.failed + row.unknown + row.abandoned
        );
        assert_eq!(
            row.failed + row.unknown,
            0,
            "{}: {:?}\n{}",
            row.client,
            row.errors,
            p.server.stderr()
        );
        assert!(row.completed > 0, "{} made no progress", row.client);
        assert!(
            row.observed_writes.is_subset(&writer.acknowledged_writes),
            "snapshot observed a write never acknowledged"
        );
        if row.client == "export" || row.client == "changes/baseline" {
            for mode in ["throttle", "pause", "abandon"] {
                assert!(
                    row.successful_modes.get(mode).copied().unwrap_or(0) > 0,
                    "{} did not exercise {mode}; increase the diagnostic duration",
                    row.client
                );
            }
        }
    }
    assert_eq!(
        final_writes, writer.acknowledged_writes,
        "durable writes differ from received receipts"
    );
    assert!(
        peer_progress.iter().all(|(_, count)| *count > 0),
        "light reads/writes made no progress while streams were active"
    );
}
