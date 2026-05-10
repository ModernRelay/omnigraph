//! Integration tests for `omnigraph update` (MR-612).
//!
//! Spins up a minimal in-process HTTP server (raw `std::net::TcpListener` so
//! we don't drag axum into the CLI's dev-deps) that serves both:
//! - GitHub Releases JSON metadata (`/repos/.../releases/latest` etc.)
//! - Release asset downloads (`.tar.gz` and `.sha256`)
//!
//! Tests stub the binaries with shell scripts so we can inspect what got
//! written to the install dir without needing real Rust binaries in the
//! release archive.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use assert_cmd::Command as AssertCommand;
use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

mod support;

fn cli_in(install_dir: &Path) -> AssertCommand {
    let bin = install_dir.join("omnigraph");
    let mut cmd = AssertCommand::new(bin);
    // Use a unique cache dir per test so the version_check side never races us.
    cmd.env("OMNIGRAPH_NO_UPDATE_CHECK", "1");
    cmd
}

/// Stage `omnigraph` and `omnigraph-server` shell stubs into `dir`. Returns the
/// path to the built `omnigraph` shim that calls into `assert_cmd`'s real CLI.
/// We can't put the cargo-built binary into the install dir directly because
/// the update will rename-over it during the test, which would break other
/// tests that share the same target dir.
fn stage_install_dir(dir: &Path, omnigraph_payload: &[u8], server_payload: &[u8]) {
    let og = dir.join("omnigraph");
    fs::write(&og, omnigraph_payload).unwrap();
    let server = dir.join("omnigraph-server");
    fs::write(&server, server_payload).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut p = fs::metadata(&og).unwrap().permissions();
        p.set_mode(0o755);
        fs::set_permissions(&og, p).unwrap();
        let mut p = fs::metadata(&server).unwrap().permissions();
        p.set_mode(0o755);
        fs::set_permissions(&server, p).unwrap();
    }
}

/// Build a `.tar.gz` containing `omnigraph` and (optionally) `omnigraph-server`
/// binaries with the supplied byte payloads. Returns `(archive_bytes, sha256_hex)`.
fn build_release_archive(
    omnigraph: &[u8],
    omnigraph_server: Option<&[u8]>,
) -> (Vec<u8>, String) {
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    {
        let mut tar = tar::Builder::new(&mut gz);
        let mut header = tar::Header::new_gnu();
        header.set_size(omnigraph.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        tar.append_data(&mut header, "omnigraph", omnigraph).unwrap();
        if let Some(server) = omnigraph_server {
            let mut h = tar::Header::new_gnu();
            h.set_size(server.len() as u64);
            h.set_mode(0o755);
            h.set_cksum();
            tar.append_data(&mut h, "omnigraph-server", server).unwrap();
        }
        tar.finish().unwrap();
    }
    let bytes = gz.finish().unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = format!("{:x}", hasher.finalize());
    (bytes, digest)
}

/// In-memory routing table: path -> (status, content-type, body).
type Routes = Arc<Mutex<HashMap<String, (u16, &'static str, Vec<u8>)>>>;

struct Fixture {
    addr: String,
    _shutdown: TcpListener, // dropped at end of test, ignored
    routes: Routes,
    _join: Option<thread::JoinHandle<()>>,
}

impl Fixture {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(false).unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
        let routes_clone = routes.clone();
        let listener_clone = listener.try_clone().unwrap();
        let join = thread::spawn(move || {
            // Each test spins up a short-lived server. Accept loop runs until
            // the listener is dropped (Fixture::stop sets nonblocking + drops).
            for stream in listener_clone.incoming() {
                let stream = match stream {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let routes = routes_clone.clone();
                thread::spawn(move || handle_request(stream, routes));
            }
        });
        Self {
            addr,
            _shutdown: listener,
            routes,
            _join: Some(join),
        }
    }

    fn route(&self, path: &str, status: u16, content_type: &'static str, body: Vec<u8>) {
        self.routes
            .lock()
            .unwrap()
            .insert(path.to_string(), (status, content_type, body));
    }

    fn url(&self) -> &str {
        &self.addr
    }
}

fn handle_request(mut stream: TcpStream, routes: Routes) {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_string();
    // Drain headers so the client sees a clean TCP stream.
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        if line == "\r\n" || line.is_empty() {
            break;
        }
    }
    let response = match routes.lock().unwrap().get(&path).cloned() {
        Some((status, ct, body)) => {
            let header = format!(
                "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                status,
                ct,
                body.len()
            );
            let mut out = header.into_bytes();
            out.extend_from_slice(&body);
            out
        }
        None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            .to_vec(),
    };
    let _ = stream.write_all(&response);
    let _ = stream.flush();
}

/// Build a stable releases-API metadata JSON body.
fn release_json(tag: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "tag_name": tag,
        "name": format!("Release {tag}"),
        "draft": false,
        "prerelease": false,
    }))
    .unwrap()
}

/// Build the full asset path the binary expects under the configured download
/// base, e.g. `/ModernRelay/omnigraph/releases/download/v0.5.0/omnigraph-linux-x86_64.tar.gz`.
fn asset_path(tag: &str, name: &str) -> String {
    format!("/ModernRelay/omnigraph/releases/download/{tag}/{name}")
}

fn current_platform_asset() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("omnigraph-linux-x86_64.tar.gz"),
        ("macos", "aarch64") => Some("omnigraph-macos-arm64.tar.gz"),
        _ => None,
    }
}

// ---------- helpers used by individual tests ----------

/// Run the cargo-built `omnigraph` from the install dir, using the test
/// fixture as both the API and download base.
fn run_update(install_dir: &Path, fixture: &Fixture, args: &[&str]) -> std::process::Output {
    // `assert_cmd::Command` panics on non-zero exit by default; we want raw
    // output to inspect failure modes, so go through the standard `Output`.
    let bin = install_dir.join("omnigraph");
    let mut cmd = std::process::Command::new(bin);
    cmd.arg("update")
        .args(args)
        .env("OMNIGRAPH_NO_UPDATE_CHECK", "1")
        .env("OMNIGRAPH_UPDATE_API_BASE", fixture.url())
        .env("OMNIGRAPH_UPDATE_DOWNLOAD_BASE", fixture.url());
    cmd.output().unwrap()
}

/// Copy the cargo-built `omnigraph` binary into the install dir so we have a
/// real binary to invoke. We can't link against the lib (there isn't one), so
/// each test stages its own copy.
fn install_real_binary(install_dir: &Path) -> PathBuf {
    let cargo_bin = assert_cmd::cargo::cargo_bin("omnigraph");
    let target = install_dir.join("omnigraph");
    fs::copy(&cargo_bin, &target).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut p = fs::metadata(&target).unwrap().permissions();
        p.set_mode(0o755);
        fs::set_permissions(&target, p).unwrap();
    }
    target
}

fn install_fake_server(install_dir: &Path, payload: &[u8]) {
    let target = install_dir.join("omnigraph-server");
    fs::write(&target, payload).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut p = fs::metadata(&target).unwrap().permissions();
        p.set_mode(0o755);
        fs::set_permissions(&target, p).unwrap();
    }
}

// ---------- tests ----------

#[test]
fn update_check_when_up_to_date_reports_current() {
    let asset = match current_platform_asset() {
        Some(a) => a,
        None => return, // unsupported platform: nothing to test
    };
    let dir = TempDir::new().unwrap();
    install_real_binary(dir.path());
    let fixture = Fixture::start();
    let current = env!("CARGO_PKG_VERSION");
    fixture.route(
        "/repos/ModernRelay/omnigraph/releases/latest",
        200,
        "application/json",
        release_json(&format!("v{current}")),
    );
    let _ = asset; // route only needed when an update is required

    let out = run_update(dir.path(), &fixture, &["--check"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected success; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("up to date"),
        "expected 'up to date' message, got: {stdout}"
    );
}

#[test]
fn update_check_when_newer_version_available_reports_upgrade() {
    if current_platform_asset().is_none() {
        return;
    }
    let dir = TempDir::new().unwrap();
    install_real_binary(dir.path());
    let fixture = Fixture::start();
    fixture.route(
        "/repos/ModernRelay/omnigraph/releases/latest",
        200,
        "application/json",
        release_json("v999.0.0"),
    );

    let out = run_update(dir.path(), &fixture, &["--check"]);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("v999.0.0") || stdout.contains("999.0.0"),
        "expected new version in output; got: {stdout}"
    );
    assert!(
        stdout.contains("new version") || stdout.contains("newer"),
        "expected upgrade-available message; got: {stdout}"
    );
}

#[test]
fn update_full_flow_replaces_both_binaries_and_verifies_checksum() {
    let asset = match current_platform_asset() {
        Some(a) => a,
        None => return,
    };
    let dir = TempDir::new().unwrap();
    install_real_binary(dir.path());
    install_fake_server(dir.path(), b"#!/bin/sh\necho old-server\n");

    let new_omnigraph = b"NEW-OMNIGRAPH-PAYLOAD";
    let new_server = b"NEW-SERVER-PAYLOAD";
    let (archive, digest) = build_release_archive(new_omnigraph, Some(new_server));

    let fixture = Fixture::start();
    fixture.route(
        "/repos/ModernRelay/omnigraph/releases/latest",
        200,
        "application/json",
        release_json("v999.0.0"),
    );
    fixture.route(
        &asset_path("v999.0.0", asset),
        200,
        "application/octet-stream",
        archive,
    );
    let stem = asset.trim_end_matches(".tar.gz");
    fixture.route(
        &asset_path("v999.0.0", &format!("{stem}.sha256")),
        200,
        "text/plain",
        format!("{digest}  {asset}\n").into_bytes(),
    );

    let out = run_update(dir.path(), &fixture, &["--yes"]);
    assert!(
        out.status.success(),
        "update failed; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Verify both binaries got replaced byte-for-byte.
    let updated_og = fs::read(dir.path().join("omnigraph")).unwrap();
    assert_eq!(updated_og, new_omnigraph);
    let updated_server = fs::read(dir.path().join("omnigraph-server")).unwrap();
    assert_eq!(updated_server, new_server);
}

#[test]
fn update_fails_loudly_on_checksum_mismatch() {
    let asset = match current_platform_asset() {
        Some(a) => a,
        None => return,
    };
    let dir = TempDir::new().unwrap();
    install_real_binary(dir.path());
    install_fake_server(dir.path(), b"#!/bin/sh\necho old-server\n");

    let new_omnigraph = b"NEW-OMNIGRAPH-PAYLOAD";
    let (archive, _digest) = build_release_archive(new_omnigraph, None);

    let fixture = Fixture::start();
    fixture.route(
        "/repos/ModernRelay/omnigraph/releases/latest",
        200,
        "application/json",
        release_json("v999.0.0"),
    );
    fixture.route(
        &asset_path("v999.0.0", asset),
        200,
        "application/octet-stream",
        archive,
    );
    let stem = asset.trim_end_matches(".tar.gz");
    let bad_digest = "0".repeat(64);
    fixture.route(
        &asset_path("v999.0.0", &format!("{stem}.sha256")),
        200,
        "text/plain",
        format!("{bad_digest}  {asset}\n").into_bytes(),
    );

    let out = run_update(dir.path(), &fixture, &["--yes"]);
    assert!(!out.status.success(), "checksum mismatch should fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("sha256 mismatch") || stderr.contains("checksum"),
        "expected checksum error; got: {stderr}"
    );
    // The original binary must not be replaced when the checksum fails.
    let preserved = fs::read(dir.path().join("omnigraph")).unwrap();
    assert!(
        preserved.starts_with(b"\x7fELF") || preserved.starts_with(b"#!"),
        "original binary should still be in place"
    );
}

#[test]
fn update_does_not_replace_omnigraph_server_when_not_present() {
    let asset = match current_platform_asset() {
        Some(a) => a,
        None => return,
    };
    let dir = TempDir::new().unwrap();
    install_real_binary(dir.path());
    // Note: no `omnigraph-server` staged.

    let new_omnigraph = b"NEW-OMNIGRAPH-PAYLOAD";
    let new_server = b"NEW-SERVER-PAYLOAD";
    let (archive, digest) = build_release_archive(new_omnigraph, Some(new_server));

    let fixture = Fixture::start();
    fixture.route(
        "/repos/ModernRelay/omnigraph/releases/latest",
        200,
        "application/json",
        release_json("v999.0.0"),
    );
    fixture.route(
        &asset_path("v999.0.0", asset),
        200,
        "application/octet-stream",
        archive,
    );
    let stem = asset.trim_end_matches(".tar.gz");
    fixture.route(
        &asset_path("v999.0.0", &format!("{stem}.sha256")),
        200,
        "text/plain",
        format!("{digest}  {asset}\n").into_bytes(),
    );

    let out = run_update(dir.path(), &fixture, &["--yes"]);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(!dir.path().join("omnigraph-server").exists());
}

#[test]
fn update_check_handles_missing_release_metadata() {
    if current_platform_asset().is_none() {
        return;
    }
    let dir = TempDir::new().unwrap();
    install_real_binary(dir.path());
    let fixture = Fixture::start(); // no routes registered

    let out = run_update(dir.path(), &fixture, &["--check"]);
    assert!(!out.status.success(), "should fail when API returns 404");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("404") || stderr.contains("release lookup failed"),
        "expected fetch-error message; got: {stderr}"
    );
}
