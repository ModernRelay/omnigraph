use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const GIT_PROBE_DEADLINE: Duration = Duration::from_secs(3);
const GIT_REAP_DEADLINE: Duration = Duration::from_secs(1);
const MAX_GIT_POINTER_BYTES: u64 = 4 * 1024;
const MAX_PACKED_REFS_BYTES: u64 = 16 * 1024 * 1024;
const UNKNOWN: &str = "unknown";

fn main() {
    let profile = std::env::var("PROFILE").expect("Cargo sets PROFILE for build scripts");
    let opt_level = std::env::var("OPT_LEVEL").expect("Cargo sets OPT_LEVEL for build scripts");
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
    let untracked = run_git_status_bounded(
        repository,
        &[
            "ls-files",
            "--others",
            "--exclude-standard",
            "--error-unmatch",
            "*",
        ],
    )?;
    let untracked_dirty = match untracked.code() {
        Some(0) => true,
        Some(1) => false,
        _ => return None,
    };
    Some(untracked_dirty)
}

/// Run one non-interactive, status-only Git metadata probe. Standard streams
/// are null, so timeout handling never blocks on a pipe-reader join.
fn run_git_status_bounded(repository: &Path, arguments: &[&str]) -> Option<ExitStatus> {
    let mut command = Command::new("git");
    command
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_git_process_group(&mut command);
    let mut child = command.spawn().ok()?;
    let process_group = i32::try_from(child.id()).ok()?;
    let deadline = Instant::now() + GIT_PROBE_DEADLINE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if wait_for_git_process_group_gone(process_group, GIT_REAP_DEADLINE) {
                    return Some(status);
                }
                let _ = kill_git_process_group(process_group);
                let _ = wait_for_git_process_group_gone(process_group, GIT_REAP_DEADLINE);
                return None;
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(None) | Err(_) => {
                if !kill_git_process_group(process_group) {
                    let _ = child.kill();
                }
                let reap_deadline = Instant::now() + GIT_REAP_DEADLINE;
                while Instant::now() < reap_deadline {
                    match child.try_wait() {
                        Ok(Some(_)) => break,
                        Ok(None) => thread::sleep(Duration::from_millis(10)),
                        Err(_) => break,
                    }
                }
                let _ = wait_for_git_process_group_gone(process_group, GIT_REAP_DEADLINE);
                return None;
            }
        }
    }
}

#[cfg(unix)]
fn configure_git_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_git_process_group(_command: &mut Command) {}

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn send_signal(process: i32, signal: i32) -> i32;
}

#[cfg(unix)]
fn kill_git_process_group(process_group: i32) -> bool {
    // SAFETY: `send_signal` has the POSIX `kill(2)` signature. The negative
    // pid selects the fresh process group created immediately before spawn.
    let result = unsafe { send_signal(-process_group, 9) };
    result == 0 || no_such_process(std::io::Error::last_os_error())
}

#[cfg(not(unix))]
fn kill_git_process_group(_process_group: i32) -> bool {
    false
}

#[cfg(unix)]
fn git_process_group_is_gone(process_group: i32) -> bool {
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
fn git_process_group_is_gone(_process_group: i32) -> bool {
    true
}

fn wait_for_git_process_group_gone(process_group: i32, timeout: Duration) -> bool {
    let started = Instant::now();
    loop {
        if git_process_group_is_gone(process_group) {
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
        common_dir.join("index"),
    ] {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    if let Some(reference) = read_bounded_file(&git_dir.join("HEAD"), MAX_GIT_POINTER_BYTES)
        .and_then(|head| String::from_utf8(head).ok())
        .and_then(|head| head.trim().strip_prefix("ref: ").map(str::to_string))
        .filter(|reference| safe_git_reference(reference).is_some())
    {
        println!(
            "cargo:rerun-if-changed={}",
            common_dir.join(reference).display()
        );
    }
}
