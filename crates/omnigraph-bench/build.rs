use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const PROBE_DEADLINE: Duration = Duration::from_secs(3);
const PROBE_REAP_DEADLINE: Duration = Duration::from_secs(1);
const MAX_GIT_POINTER_BYTES: u64 = 4 * 1024;
const MAX_PACKED_REFS_BYTES: u64 = 16 * 1024 * 1024;
const MAX_GIT_STATUS_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RUSTC_VERSION_BYTES: u64 = 64 * 1024;
const MAX_WORKSPACE_MANIFEST_BYTES: u64 = 1024 * 1024;
const RAW_HASH_CHUNK_PATHS: usize = 128;
const RELEASE_PROFILE_OVERRIDE_PREFIX: &str = "CARGO_PROFILE_RELEASE_";
const RELEASE_PROFILE_OVERRIDE_NAMES: &[&str] = &[
    "CARGO_PROFILE_RELEASE_CODEGEN_UNITS",
    "CARGO_PROFILE_RELEASE_DEBUG",
    "CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS",
    "CARGO_PROFILE_RELEASE_INCREMENTAL",
    "CARGO_PROFILE_RELEASE_LTO",
    "CARGO_PROFILE_RELEASE_OPT_LEVEL",
    "CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS",
    "CARGO_PROFILE_RELEASE_PANIC",
    "CARGO_PROFILE_RELEASE_RPATH",
    "CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO",
    "CARGO_PROFILE_RELEASE_STRIP",
];
const UNKNOWN: &str = "unknown";

#[cfg(unix)]
const GIT_EXECUTABLE: &str = "/usr/bin/git";
#[cfg(not(unix))]
const GIT_EXECUTABLE: &str = "git";

#[derive(Debug)]
struct ReleaseProfile {
    lto: String,
    codegen_units: u32,
    strip: bool,
}

fn main() {
    let profile = std::env::var("PROFILE").expect("Cargo sets PROFILE for build scripts");
    let opt_level = std::env::var("OPT_LEVEL").expect("Cargo sets OPT_LEVEL for build scripts");
    let target = std::env::var("TARGET").expect("Cargo sets TARGET for build scripts");
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_BUILD_PROFILE={profile}");
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_BUILD_OPT_LEVEL={opt_level}");

    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
    let repository = manifest_dir
        .as_deref()
        .map(Path::new)
        .and_then(|path| path.parent())
        .and_then(Path::parent);
    if let Some(repository) = repository {
        // These are the binary's workspace-owned source/config inputs. Cargo
        // reruns the provenance probe when they change; Git control files
        // below cover commit/index movement that does not touch source bytes.
        for input in [
            repository.join("crates"),
            repository.join("benchmarks"),
            repository.join("Cargo.toml"),
            repository.join("Cargo.lock"),
            repository.join("rust-toolchain.toml"),
            repository.join(".cargo"),
            repository.join(".gitattributes"),
            repository.join(".gitignore"),
        ] {
            println!("cargo:rerun-if-changed={}", input.display());
        }
        emit_git_rerun_paths(repository);
    }
    let source_git_commit = repository
        .and_then(probe_git_commit)
        .unwrap_or_else(|| UNKNOWN.to_string());
    let source_worktree_dirty = repository
        .and_then(probe_git_dirty)
        .map(|dirty| dirty.to_string())
        .unwrap_or_else(|| UNKNOWN.to_string());
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_SOURCE_GIT_COMMIT={source_git_commit}");
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_SOURCE_WORKTREE_DIRTY={source_worktree_dirty}");
    let declared_engine_features = repository
        .and_then(read_declared_engine_features)
        .map(|features| features.join(","))
        .unwrap_or_else(|| UNKNOWN.to_string());
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_DECLARED_ENGINE_FEATURES={declared_engine_features}");

    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let rustc_version = run_command_output_bounded(Path::new(&rustc), &["--version"])
        .unwrap_or_else(|| UNKNOWN.to_string());
    let release_profile =
        repository.and_then(|root| read_release_profile(&root.join("Cargo.toml")));
    let declared_release_lto = release_profile
        .as_ref()
        .map_or(UNKNOWN, |profile| profile.lto.as_str());
    let declared_release_codegen_units = release_profile
        .as_ref()
        .map(|profile| profile.codegen_units.to_string())
        .unwrap_or_else(|| UNKNOWN.to_string());
    let declared_release_strip = release_profile
        .as_ref()
        .map(|profile| profile.strip.to_string())
        .unwrap_or_else(|| UNKNOWN.to_string());
    let encoded_rustflags_present =
        std::env::var_os("CARGO_ENCODED_RUSTFLAGS").is_some_and(|value| !value.is_empty());
    let release_overrides = if release_profile_overrides_supported() {
        "supported"
    } else {
        "unsupported"
    };

    println!("cargo:rustc-env=OMNIGRAPH_BENCH_TARGET_TRIPLE={target}");
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_RUSTC_VERSION={rustc_version}");
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_DECLARED_RELEASE_LTO={declared_release_lto}");
    println!(
        "cargo:rustc-env=OMNIGRAPH_BENCH_DECLARED_RELEASE_CODEGEN_UNITS={declared_release_codegen_units}"
    );
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_DECLARED_RELEASE_STRIP={declared_release_strip}");
    println!(
        "cargo:rustc-env=OMNIGRAPH_BENCH_CARGO_ENCODED_RUSTFLAGS_PRESENT={encoded_rustflags_present}"
    );
    println!(
        "cargo:rustc-env=OMNIGRAPH_BENCH_RELEASE_PROFILE_ENVIRONMENT_OVERRIDES={release_overrides}"
    );
    println!("cargo:rustc-env=OMNIGRAPH_BENCH_EFFECTIVE_CODEGEN_OPTIONS_PROVED=false");

    // Build-script output is cached independently from the final target
    // compilation. Keep every inherited input to these facts explicit so a
    // later build cannot reuse stale provenance.
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    for name in RELEASE_PROFILE_OVERRIDE_NAMES {
        println!("cargo:rerun-if-env-changed={name}");
    }
    // Cargo supports package-specific and future profile keys too. Existing
    // unknown release overrides fail closed below; watching each one present
    // in this invocation ensures changing its value reruns the probe.
    for (name, _) in std::env::vars_os() {
        if let Some(name) = name.to_str()
            && name.starts_with(RELEASE_PROFILE_OVERRIDE_PREFIX)
            && !RELEASE_PROFILE_OVERRIDE_NAMES.contains(&name)
        {
            println!("cargo:rerun-if-env-changed={name}");
        }
    }
}

fn probe_git_commit(repository: &Path) -> Option<String> {
    let git_dir = discover_git_dir(repository)?;
    let common_dir = discover_common_git_dir(&git_dir).unwrap_or_else(|| git_dir.clone());
    let head = read_bounded_file(&git_dir.join("HEAD"), MAX_GIT_POINTER_BYTES)?;
    let head = std::str::from_utf8(&head).ok()?.trim();
    if let Some(commit) = normalize_commit(head) {
        return Some(commit);
    }
    let reference = head.strip_prefix("ref: ")?;
    safe_git_reference(reference)?;
    for base in [&git_dir, &common_dir] {
        let Some(value) = read_bounded_file(&base.join(reference), MAX_GIT_POINTER_BYTES) else {
            continue;
        };
        if let Some(commit) = std::str::from_utf8(&value)
            .ok()
            .and_then(|value| normalize_commit(value.trim()))
        {
            return Some(commit);
        }
    }
    let packed = read_bounded_file(&common_dir.join("packed-refs"), MAX_PACKED_REFS_BYTES)?;
    std::str::from_utf8(&packed).ok()?.lines().find_map(|line| {
        let (commit, name) = line.split_once(' ')?;
        (name == reference)
            .then(|| normalize_commit(commit))
            .flatten()
    })
}

fn probe_git_dirty(repository: &Path) -> Option<bool> {
    let index = run_git_output_bounded(
        repository,
        &["ls-files", "-v", "-z", "--"],
        MAX_GIT_STATUS_BYTES,
    )?;
    if !canonical_index_inventory(&index) {
        return Some(true);
    }
    if !raw_source_matches_index(repository)? {
        return Some(true);
    }
    let tracked = run_git_status_bounded(
        repository,
        &[
            "diff-index",
            "--quiet",
            "--no-ext-diff",
            "--no-textconv",
            "--ignore-submodules=none",
            "HEAD",
            "--",
        ],
    )?;
    let tracked_dirty = match tracked.code() {
        Some(0) => false,
        Some(1) => true,
        _ => return None,
    };
    if tracked_dirty {
        return Some(true);
    }
    let untracked_source = run_git_output_bounded(
        repository,
        &[
            "ls-files",
            "--others",
            "-z",
            "--",
            "crates",
            "benchmarks",
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            ".cargo",
            ".gitattributes",
            ".gitignore",
        ],
        MAX_GIT_STATUS_BYTES,
    )?;
    if !untracked_source.is_empty() {
        return Some(true);
    }
    let untracked = run_git_output_bounded(
        repository,
        &["ls-files", "--others", "--exclude-standard", "-z"],
        MAX_GIT_STATUS_BYTES,
    )?;
    Some(!untracked.is_empty())
}

fn canonical_index_inventory(encoded: &[u8]) -> bool {
    (encoded.is_empty() || encoded.last() == Some(&0))
        && encoded
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
            .all(|entry| entry.len() >= 3 && entry[0] == b'H' && entry[1] == b' ')
}

#[derive(Debug)]
struct SourceIndexEntry {
    object_id: String,
    path: PathBuf,
    executable: bool,
}

fn raw_source_matches_index(repository: &Path) -> Option<bool> {
    let encoded = run_git_output_bounded(
        repository,
        &[
            "ls-files",
            "--stage",
            "-z",
            "--",
            "crates",
            "benchmarks",
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            ".cargo",
            ".gitattributes",
            ".gitignore",
        ],
        MAX_GIT_STATUS_BYTES,
    )?;
    let entries = parse_source_index_entries(&encoded)?;
    if entries.is_empty() {
        return None;
    }
    for entry in &entries {
        let metadata = match std::fs::symlink_metadata(repository.join(&entry.path)) {
            Ok(metadata) => metadata,
            Err(_) => return Some(false),
        };
        if !metadata.is_file() {
            return Some(false);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if (metadata.permissions().mode() & 0o111 != 0) != entry.executable {
                return Some(false);
            }
        }
        #[cfg(not(unix))]
        return None;
    }
    for entries in entries.chunks(RAW_HASH_CHUNK_PATHS) {
        let mut command = sanitized_git_command(repository)?;
        command.args(["hash-object", "--no-filters", "--"]);
        for entry in entries {
            command.arg(&entry.path);
        }
        let (status, output) = run_child_output_bounded(&mut command, MAX_GIT_STATUS_BYTES)?;
        if !status.success() {
            return None;
        }
        let hashes = std::str::from_utf8(&output)
            .ok()?
            .lines()
            .collect::<Vec<_>>();
        if hashes.len() != entries.len() {
            return None;
        }
        if entries
            .iter()
            .zip(hashes)
            .any(|(entry, observed)| observed != entry.object_id)
        {
            return Some(false);
        }
    }
    Some(true)
}

fn parse_source_index_entries(encoded: &[u8]) -> Option<Vec<SourceIndexEntry>> {
    if !encoded.is_empty() && encoded.last() != Some(&0) {
        return None;
    }
    encoded
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let tab = entry.iter().position(|byte| *byte == b'\t')?;
            let metadata = std::str::from_utf8(&entry[..tab]).ok()?;
            let mut fields = metadata.split(' ');
            let mode = fields.next()?;
            let object_id = fields.next()?;
            let stage = fields.next()?;
            if fields.next().is_some()
                || !matches!(mode, "100644" | "100755")
                || stage != "0"
                || normalize_commit(object_id).is_none()
            {
                return None;
            }
            let path = path_from_git_bytes(&entry[tab + 1..])?;
            if path.is_absolute()
                || path
                    .components()
                    .any(|component| !matches!(component, std::path::Component::Normal(_)))
            {
                return None;
            }
            Some(SourceIndexEntry {
                object_id: object_id.to_string(),
                path,
                executable: mode == "100755",
            })
        })
        .collect()
}

#[cfg(unix)]
fn path_from_git_bytes(path: &[u8]) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    Some(PathBuf::from(std::ffi::OsString::from_vec(path.to_vec())))
}

#[cfg(not(unix))]
fn path_from_git_bytes(path: &[u8]) -> Option<PathBuf> {
    std::str::from_utf8(path).ok().map(PathBuf::from)
}

/// Run one non-interactive, status-only Git metadata probe. Standard streams
/// are null, so timeout handling never blocks on a pipe-reader join.
fn run_git_status_bounded(repository: &Path, arguments: &[&str]) -> Option<ExitStatus> {
    let mut command = sanitized_git_command(repository)?;
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_probe_process_group(&mut command);
    let mut child = command.spawn().ok()?;
    let process_group = match i32::try_from(child.id()) {
        Ok(process_group) => process_group,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    };
    let deadline = Instant::now() + PROBE_DEADLINE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if wait_for_probe_process_group_gone(process_group, PROBE_REAP_DEADLINE) {
                    return Some(status);
                }
                let _ = kill_probe_process_group(process_group);
                let _ = wait_for_probe_process_group_gone(process_group, PROBE_REAP_DEADLINE);
                return None;
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(None) | Err(_) => {
                if !kill_probe_process_group(process_group) {
                    let _ = child.kill();
                }
                let reap_deadline = Instant::now() + PROBE_REAP_DEADLINE;
                while Instant::now() < reap_deadline {
                    match child.try_wait() {
                        Ok(Some(_)) => break,
                        Ok(None) => thread::sleep(Duration::from_millis(10)),
                        Err(_) => break,
                    }
                }
                let _ = wait_for_probe_process_group_gone(process_group, PROBE_REAP_DEADLINE);
                return None;
            }
        }
    }
}

fn run_git_output_bounded(repository: &Path, arguments: &[&str], limit: u64) -> Option<Vec<u8>> {
    let mut command = sanitized_git_command(repository)?;
    command.args(arguments);
    let (status, output) = run_child_output_bounded(&mut command, limit)?;
    status.success().then_some(output)
}

fn run_command_output_bounded(executable: &Path, arguments: &[&str]) -> Option<String> {
    let mut command = Command::new(executable);
    command.args(arguments).env("LC_ALL", "C");
    let (status, bytes) = run_child_output_bounded(&mut command, MAX_RUSTC_VERSION_BYTES)?;
    if !status.success() {
        return None;
    }
    let value = String::from_utf8(bytes).ok()?;
    let value = value.trim();
    (!value.is_empty() && !value.chars().any(char::is_control)).then(|| value.to_string())
}

/// Capture bounded stdout from a non-interactive child without trusting the
/// child or its descendants to close the pipe. In particular, a compiler
/// wrapper can exit after spawning a descendant that retains stdout. The
/// process group is killed before we wait for pipe completion, and the reader
/// is observed through a bounded channel wait rather than an unbounded join.
fn run_child_output_bounded(command: &mut Command, limit: u64) -> Option<(ExitStatus, Vec<u8>)> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    configure_probe_process_group(command);
    let mut child = command.spawn().ok()?;
    let process_group = match i32::try_from(child.id()) {
        Ok(process_group) => process_group,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    };
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = kill_probe_process_group(process_group);
            let _ = child.kill();
            reap_child_bounded(&mut child);
            return None;
        }
    };
    let (sender, receiver) = mpsc::sync_channel(1);
    let _reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stdout
            .take(limit.saturating_add(1))
            .read_to_end(&mut bytes)
            .ok()
            .map(|_| bytes);
        let _ = sender.send(result);
    });
    let deadline = Instant::now() + PROBE_DEADLINE;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(None) | Err(_) => {
                if !kill_probe_process_group(process_group) {
                    let _ = child.kill();
                }
                reap_child_bounded(&mut child);
                let _ = wait_for_probe_process_group_gone(process_group, PROBE_REAP_DEADLINE);
                return None;
            }
        }
    };

    if !wait_for_probe_process_group_gone(process_group, PROBE_REAP_DEADLINE) {
        let _ = kill_probe_process_group(process_group);
        let _ = wait_for_probe_process_group_gone(process_group, PROBE_REAP_DEADLINE);
        // A probe whose root exited while descendants remained is not trusted,
        // even if killing those descendants eventually closed the pipe.
        return None;
    }
    let bytes = receiver.recv_timeout(PROBE_REAP_DEADLINE).ok()??;
    (u64::try_from(bytes.len()).ok()? <= limit).then_some((status, bytes))
}

fn reap_child_bounded(child: &mut std::process::Child) {
    let deadline = Instant::now() + PROBE_REAP_DEADLINE;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => thread::sleep(Duration::from_millis(10)),
            Err(_) => return,
        }
    }
}

/// Git's repository-selection and configuration environment takes precedence
/// over `-C`. Remove all inherited `GIT_*` variables so dirty-state evidence
/// cannot be redirected to another worktree, index, namespace, or config.
fn sanitized_git_command(repository: &Path) -> Option<Command> {
    let repository = repository.canonicalize().ok()?;
    let git_dir = discover_git_dir(&repository)?.canonicalize().ok()?;
    let mut command = Command::new(GIT_EXECUTABLE);
    #[cfg(not(unix))]
    let path = std::env::var_os("PATH");
    command.env_clear();
    #[cfg(not(unix))]
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .arg("--no-optional-locks")
        .arg("--git-dir")
        .arg(git_dir)
        .arg("--work-tree")
        .arg(&repository)
        .args([
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.untrackedCache=false",
            "-c",
            "core.ignoreCase=false",
            "-c",
            "core.fileMode=true",
            "-c",
            "core.symlinks=true",
            "-c",
            "core.precomposeUnicode=false",
            "-c",
            "core.trustctime=true",
            "-c",
            "core.checkStat=default",
            "-c",
            "core.ignoreStat=false",
        ])
        .current_dir(&repository);
    Some(command)
}

fn read_declared_engine_features(repository: &Path) -> Option<Vec<String>> {
    let manifest = read_bounded_file(
        &repository.join("crates/omnigraph/Cargo.toml"),
        MAX_WORKSPACE_MANIFEST_BYTES,
    )?;
    let source = std::str::from_utf8(&manifest).ok()?;
    let document = toml::from_str::<toml::Value>(source).ok()?;
    let table = document.get("features")?.as_table()?;
    let mut features = table.keys().cloned().collect::<Vec<_>>();
    if features.iter().any(|feature| {
        feature.is_empty()
            || feature.len() > 128
            || feature.contains(',')
            || feature.chars().any(char::is_control)
    }) {
        return None;
    }
    let suppressed_optional_dependencies = table
        .values()
        .filter_map(toml::Value::as_array)
        .flatten()
        .filter_map(toml::Value::as_str)
        .filter_map(|feature| feature.strip_prefix("dep:"))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    if optional_dependency_names(&document)
        .difference(&suppressed_optional_dependencies)
        .next()
        .is_some()
    {
        return None;
    }
    features.sort_unstable();
    Some(features)
}

fn optional_dependency_names(document: &toml::Value) -> BTreeSet<String> {
    fn extend_from_table(value: Option<&toml::Value>, names: &mut BTreeSet<String>) {
        let Some(table) = value.and_then(toml::Value::as_table) else {
            return;
        };
        names.extend(
            table
                .iter()
                .filter(|(_name, specification)| {
                    specification
                        .as_table()
                        .and_then(|fields| fields.get("optional"))
                        .and_then(toml::Value::as_bool)
                        .is_some_and(|optional| optional)
                })
                .map(|(name, _specification)| name.clone()),
        );
    }

    let mut names = BTreeSet::new();
    for table in ["dependencies", "build-dependencies"] {
        extend_from_table(document.get(table), &mut names);
    }
    if let Some(targets) = document.get("target").and_then(toml::Value::as_table) {
        for target in targets.values() {
            for table in ["dependencies", "build-dependencies"] {
                extend_from_table(target.get(table), &mut names);
            }
        }
    }
    names
}

fn read_release_profile(path: &Path) -> Option<ReleaseProfile> {
    let bytes = read_bounded_file(path, MAX_WORKSPACE_MANIFEST_BYTES)?;
    let source = std::str::from_utf8(&bytes).ok()?;
    let document = toml::from_str::<toml::Value>(source).ok()?;
    let release = document.get("profile")?.get("release")?;
    let lto = release.get("lto")?.as_str()?.to_string();
    let codegen_units = u32::try_from(release.get("codegen-units")?.as_integer()?).ok()?;
    let strip = release.get("strip")?.as_bool()?;
    Some(ReleaseProfile {
        lto,
        codegen_units,
        strip,
    })
}

fn release_profile_overrides_supported() -> bool {
    !std::env::vars_os().any(|(name, _)| {
        name.to_string_lossy()
            .starts_with(RELEASE_PROFILE_OVERRIDE_PREFIX)
    })
}

#[cfg(unix)]
fn configure_probe_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_probe_process_group(_command: &mut Command) {}

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn send_signal(process: i32, signal: i32) -> i32;
}

#[cfg(unix)]
fn kill_probe_process_group(process_group: i32) -> bool {
    // SAFETY: `send_signal` has the POSIX `kill(2)` signature. The negative
    // pid selects the fresh process group created immediately before spawn.
    let result = unsafe { send_signal(-process_group, 9) };
    result == 0 || no_such_process(std::io::Error::last_os_error())
}

#[cfg(not(unix))]
fn kill_probe_process_group(_process_group: i32) -> bool {
    false
}

#[cfg(unix)]
fn probe_process_group_is_gone(process_group: i32) -> bool {
    // SAFETY: signal zero performs existence/permission checking only; the
    // negative pid addresses the dedicated process group.
    let result = unsafe { send_signal(-process_group, 0) };
    result != 0 && no_such_process(std::io::Error::last_os_error())
}

#[cfg(unix)]
fn no_such_process(error: std::io::Error) -> bool {
    // ESRCH is 3 on the Unix hosts supported by runner-v1 (macOS and Linux).
    error.raw_os_error() == Some(3)
}

#[cfg(not(unix))]
fn probe_process_group_is_gone(_process_group: i32) -> bool {
    true
}

fn wait_for_probe_process_group_gone(process_group: i32, timeout: Duration) -> bool {
    let started = Instant::now();
    loop {
        if probe_process_group_is_gone(process_group) {
            return true;
        }
        if started.elapsed() >= timeout {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn discover_git_dir(repository: &Path) -> Option<PathBuf> {
    let dot_git = repository.join(".git");
    if dot_git.is_dir() {
        return Some(dot_git);
    }
    let pointer = read_bounded_file(&dot_git, MAX_GIT_POINTER_BYTES)?;
    let pointer = std::str::from_utf8(&pointer).ok()?.trim();
    let path = Path::new(pointer.strip_prefix("gitdir: ")?);
    Some(if path.is_absolute() {
        path.to_path_buf()
    } else {
        repository.join(path)
    })
}

fn discover_common_git_dir(git_dir: &Path) -> Option<PathBuf> {
    let pointer = read_bounded_file(&git_dir.join("commondir"), MAX_GIT_POINTER_BYTES)?;
    let path = Path::new(std::str::from_utf8(&pointer).ok()?.trim());
    Some(if path.is_absolute() {
        path.to_path_buf()
    } else {
        git_dir.join(path)
    })
}

fn read_bounded_file(path: &Path, limit: u64) -> Option<Vec<u8>> {
    let metadata = std::fs::metadata(path).ok()?;
    if !metadata.is_file() || metadata.len() > limit {
        return None;
    }
    let mut file = std::fs::File::open(path).ok()?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).ok()?);
    file.by_ref().take(limit + 1).read_to_end(&mut bytes).ok()?;
    (u64::try_from(bytes.len()).ok()? <= limit).then_some(bytes)
}

fn normalize_commit(value: &str) -> Option<String> {
    ((value.len() == 40 || value.len() == 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| value.to_ascii_lowercase())
}

fn safe_git_reference(reference: &str) -> Option<()> {
    (!reference.is_empty()
        && !reference.starts_with('/')
        && !reference
            .split('/')
            .any(|part| part.is_empty() || part == ".."))
    .then_some(())
}

fn emit_git_rerun_paths(repository: &Path) {
    let Some(git_dir) = discover_git_dir(repository) else {
        return;
    };
    let common_dir = discover_common_git_dir(&git_dir).unwrap_or_else(|| git_dir.clone());
    for path in [
        git_dir.join("HEAD"),
        git_dir.join("index"),
        common_dir.join("packed-refs"),
    ] {
        emit_existing_rerun_path(&path);
    }
    if let Some(reference) = read_bounded_file(&git_dir.join("HEAD"), MAX_GIT_POINTER_BYTES)
        .and_then(|head| String::from_utf8(head).ok())
        .and_then(|head| head.trim().strip_prefix("ref: ").map(str::to_string))
        .filter(|reference| safe_git_reference(reference).is_some())
    {
        let private_reference = git_dir.join(&reference);
        emit_reference_rerun_path(&private_reference, &git_dir);
        if common_dir != git_dir {
            emit_reference_rerun_path(&common_dir.join(reference), &common_dir);
        }
    }
}

fn emit_existing_rerun_path(path: &Path) {
    if path.is_file() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn emit_reference_rerun_path(reference: &Path, git_dir: &Path) {
    if reference.is_file() {
        println!("cargo:rerun-if-changed={}", reference.display());
        return;
    }
    // A packed ref has no loose file yet. Watch its nearest existing refs
    // directory so creating the loose ref reruns the probe, but never broaden
    // that fallback to the whole `.git` directory.
    let mut ancestor = reference.parent();
    while let Some(path) = ancestor {
        if path == git_dir {
            return;
        }
        if path.is_dir() {
            println!("cargo:rerun-if-changed={}", path.display());
            return;
        }
        ancestor = path.parent();
    }
}
