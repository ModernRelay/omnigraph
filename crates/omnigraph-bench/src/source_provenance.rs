//! Runtime revalidation for local durable benchmark provenance.
//!
//! Build-time facts identify the source used to compile the worker. Before an
//! archive is touched and again at every publication boundary, the local CLI
//! proves that the same checkout still names that commit and remains clean.
//! The separately attested worker must report the same clean source. This is
//! deliberately local-runner policy; controlled infrastructure can later
//! replace it with a digest-bound build receipt.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const GIT_DEADLINE: Duration = Duration::from_secs(3);
const OUTPUT_REAP_DEADLINE: Duration = Duration::from_secs(1);
const MAX_HEAD_BYTES: usize = 128;
const MAX_GIT_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_GIT_POINTER_BYTES: u64 = 4 * 1024;
const RAW_HASH_CHUNK_PATHS: usize = 128;

// Runner-v1 supports macOS and Linux. Using the system Git by absolute path
// prevents a benchmark invocation's PATH from substituting a different
// executable for the source-attestation probe.
#[cfg(unix)]
const GIT_EXECUTABLE: &str = "/usr/bin/git";
#[cfg(not(unix))]
const GIT_EXECUTABLE: &str = "git";

/// Revalidate the checkout from which this crate was compiled.
pub fn verify_compiled_source_checkout(expected_commit: &str) -> Result<(), String> {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| "compiled source checkout location is unavailable".to_string())?;
    verify_source_checkout(repository, expected_commit)
}

fn verify_source_checkout(repository: &Path, expected_commit: &str) -> Result<(), String> {
    if !valid_commit(expected_commit) {
        return Err("compiled source commit is unavailable or non-canonical".to_string());
    }
    let repository = repository
        .canonicalize()
        .map_err(|_| "compiled source checkout location is unavailable".to_string())?;
    let git_dir = discover_git_dir(&repository)
        .ok_or_else(|| "compiled source Git directory is unavailable".to_string())?;
    let (head_status, head_bytes) = run_git_output_bounded(
        &repository,
        &git_dir,
        &["rev-parse", "--verify", "HEAD"],
        MAX_HEAD_BYTES,
    )
    .ok_or_else(|| "runtime source HEAD probe failed or exceeded its bound".to_string())?;
    let head = std::str::from_utf8(&head_bytes)
        .ok()
        .map(str::trim)
        .filter(|head| valid_commit(head))
        .ok_or_else(|| "runtime source HEAD was not a canonical commit".to_string())?;
    if !head_status.success() || head != expected_commit {
        return Err("runtime source HEAD does not match the compiled source commit".to_string());
    }

    let (index_status, index_bytes) = run_git_output_bounded(
        &repository,
        &git_dir,
        &["ls-files", "-v", "-z", "--"],
        MAX_GIT_INDEX_BYTES,
    )
    .ok_or_else(|| "runtime source index probe failed or exceeded its bound".to_string())?;
    if !index_status.success() {
        return Err("runtime source index probe failed".to_string());
    }
    validate_canonical_index_inventory(&index_bytes)?;
    verify_raw_source_bytes(&repository, &git_dir)?;

    let tracked = run_git_status(
        &repository,
        &git_dir,
        &[
            "diff-index",
            "--quiet",
            "--no-ext-diff",
            "--no-textconv",
            "--ignore-submodules=none",
            "HEAD",
            "--",
        ],
    )
    .ok_or_else(|| "runtime tracked-source probe failed or exceeded its bound".to_string())?;
    match tracked.code() {
        Some(0) => {}
        Some(1) => return Err("runtime source checkout has tracked changes".to_string()),
        _ => return Err("runtime tracked-source probe failed".to_string()),
    }

    let (source_status, first_source_byte) = run_git_first_output_byte(
        &repository,
        &git_dir,
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
    )
    .ok_or_else(|| {
        "runtime untracked-source-input probe failed or exceeded its bound".to_string()
    })?;
    if first_source_byte.is_some() {
        return Err("runtime source checkout has untracked source inputs".to_string());
    }
    if !source_status.success() {
        return Err("runtime untracked-source-input probe failed".to_string());
    }

    let (untracked_status, first_untracked_byte) = run_git_first_output_byte(
        &repository,
        &git_dir,
        &["ls-files", "--others", "--exclude-standard", "-z"],
    )
    .ok_or_else(|| "runtime untracked-source probe failed or exceeded its bound".to_string())?;
    if first_untracked_byte.is_some() {
        return Err("runtime source checkout has untracked files".to_string());
    }
    if !untracked_status.success() {
        return Err("runtime untracked-source probe failed".to_string());
    }
    Ok(())
}

fn validate_canonical_index_inventory(encoded: &[u8]) -> Result<(), String> {
    if !encoded.is_empty() && encoded.last() != Some(&0) {
        return Err("runtime source index inventory was not NUL-terminated".to_string());
    }
    for entry in encoded
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        if entry.len() < 3 || entry[0] != b'H' || entry[1] != b' ' {
            return Err(
                "runtime source index uses assume-unchanged, skip-worktree, or non-canonical tracked entries"
                    .to_string(),
            );
        }
    }
    Ok(())
}

#[derive(Debug)]
struct SourceIndexEntry {
    object_id: String,
    path: PathBuf,
    executable: bool,
}

fn verify_raw_source_bytes(repository: &Path, git_dir: &Path) -> Result<(), String> {
    let (status, encoded) = run_git_output_bounded(
        repository,
        git_dir,
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
        MAX_GIT_INDEX_BYTES,
    )
    .ok_or_else(|| "runtime raw-source inventory probe failed or exceeded its bound".to_string())?;
    if !status.success() {
        return Err("runtime raw-source inventory probe failed".to_string());
    }
    let entries = parse_source_index_entries(&encoded)?;
    if entries.is_empty() {
        return Err("runtime raw-source inventory was empty".to_string());
    }
    for entry in &entries {
        let metadata = std::fs::symlink_metadata(repository.join(&entry.path)).map_err(|_| {
            format!(
                "runtime source input is missing or unreadable: {}",
                entry.path.display()
            )
        })?;
        if !metadata.is_file() {
            return Err(format!(
                "runtime source input is not a regular file: {}",
                entry.path.display()
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if (metadata.permissions().mode() & 0o111 != 0) != entry.executable {
                return Err(format!(
                    "runtime source input executable mode differs from Git: {}",
                    entry.path.display()
                ));
            }
        }
    }
    for entries in entries.chunks(RAW_HASH_CHUNK_PATHS) {
        let mut command = sanitized_git(repository, git_dir, &["hash-object", "--no-filters"]);
        command.arg("--");
        for entry in entries {
            command.arg(&entry.path);
        }
        let (status, output) = run_command_output_bounded(command, MAX_GIT_INDEX_BYTES)
            .ok_or_else(|| {
                "runtime raw-source hash probe failed or exceeded its bound".to_string()
            })?;
        if !status.success() {
            return Err("runtime raw-source hash probe failed".to_string());
        }
        let hashes = std::str::from_utf8(&output)
            .map_err(|_| "runtime raw-source hash output was not UTF-8".to_string())?
            .lines()
            .collect::<Vec<_>>();
        if hashes.len() != entries.len() {
            return Err("runtime raw-source hash output count was invalid".to_string());
        }
        for (entry, observed) in entries.iter().zip(hashes) {
            if observed != entry.object_id {
                return Err(format!(
                    "runtime source input raw bytes differ from Git: {}",
                    entry.path.display()
                ));
            }
        }
    }
    Ok(())
}

fn parse_source_index_entries(encoded: &[u8]) -> Result<Vec<SourceIndexEntry>, String> {
    if !encoded.is_empty() && encoded.last() != Some(&0) {
        return Err("runtime raw-source inventory was not NUL-terminated".to_string());
    }
    encoded
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let tab = entry
                .iter()
                .position(|byte| *byte == b'\t')
                .ok_or_else(|| "runtime raw-source inventory entry was malformed".to_string())?;
            let metadata = std::str::from_utf8(&entry[..tab])
                .map_err(|_| "runtime raw-source inventory metadata was not UTF-8".to_string())?;
            let path = &entry[tab + 1..];
            let mut fields = metadata.split(' ');
            let mode = fields.next().unwrap_or_default();
            let object_id = fields.next().unwrap_or_default();
            let stage = fields.next().unwrap_or_default();
            if fields.next().is_some()
                || !matches!(mode, "100644" | "100755")
                || stage != "0"
                || !valid_commit(object_id)
            {
                return Err(
                    "runtime raw-source inventory contained an unsupported tracked entry"
                        .to_string(),
                );
            }
            let path = path_from_git_bytes(path)
                .ok_or_else(|| "runtime raw-source path was not representable".to_string())?;
            if path.is_absolute()
                || path
                    .components()
                    .any(|component| !matches!(component, std::path::Component::Normal(_)))
            {
                return Err("runtime raw-source path was not a safe relative path".to_string());
            }
            Ok(SourceIndexEntry {
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

fn valid_commit(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sanitized_git(repository: &Path, git_dir: &Path, arguments: &[&str]) -> Command {
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
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .arg("--no-optional-locks")
        .arg("--git-dir")
        .arg(git_dir)
        .arg("--work-tree")
        .arg(repository)
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
        .args(arguments)
        .current_dir(repository)
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    configure_process_group(&mut command);
    command
}

fn run_git_status(repository: &Path, git_dir: &Path, arguments: &[&str]) -> Option<ExitStatus> {
    let mut command = sanitized_git(repository, git_dir, arguments);
    command.stdout(Stdio::null());
    let mut child = command.spawn().ok()?;
    wait_bounded(&mut child)
}

fn run_git_output_bounded(
    repository: &Path,
    git_dir: &Path,
    arguments: &[&str],
    limit: usize,
) -> Option<(ExitStatus, Vec<u8>)> {
    let command = sanitized_git(repository, git_dir, arguments);
    run_command_output_bounded(command, limit)
}

fn run_command_output_bounded(mut command: Command, limit: usize) -> Option<(ExitStatus, Vec<u8>)> {
    command.stdout(Stdio::piped());
    let mut child = command.spawn().ok()?;
    let stdout = child.stdout.take()?;
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut output = Vec::new();
        let result = stdout
            .take(u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1))
            .read_to_end(&mut output)
            .map(|_| output);
        let _ = sender.send(result);
    });
    let status = wait_bounded(&mut child)?;
    let output = receiver.recv_timeout(OUTPUT_REAP_DEADLINE).ok()?.ok()?;
    (output.len() <= limit).then_some((status, output))
}

fn run_git_first_output_byte(
    repository: &Path,
    git_dir: &Path,
    arguments: &[&str],
) -> Option<(ExitStatus, Option<u8>)> {
    let mut command = sanitized_git(repository, git_dir, arguments);
    command.stdout(Stdio::piped());
    let mut child = command.spawn().ok()?;
    let mut stdout = child.stdout.take()?;
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut byte = [0_u8; 1];
        let result = stdout
            .read(&mut byte)
            .map(|read| (read == 1).then_some(byte[0]));
        let _ = sender.send(result);
    });
    let status = wait_bounded(&mut child)?;
    let byte = receiver.recv_timeout(OUTPUT_REAP_DEADLINE).ok()?.ok()?;
    Some((status, byte))
}

fn discover_git_dir(repository: &Path) -> Option<PathBuf> {
    let dot_git = repository.join(".git");
    let metadata = std::fs::symlink_metadata(&dot_git).ok()?;
    if metadata.file_type().is_symlink() {
        return None;
    }
    if metadata.is_dir() {
        return dot_git.canonicalize().ok();
    }
    if !metadata.is_file() || metadata.len() > MAX_GIT_POINTER_BYTES {
        return None;
    }
    let pointer = read_bounded_regular_file(&dot_git, MAX_GIT_POINTER_BYTES)?;
    let pointer = std::str::from_utf8(&pointer).ok()?.trim();
    let path = Path::new(pointer.strip_prefix("gitdir: ")?);
    let git_dir = if path.is_absolute() {
        path.to_path_buf()
    } else {
        repository.join(path)
    };
    git_dir.canonicalize().ok().filter(|path| path.is_dir())
}

fn read_bounded_regular_file(path: &Path, limit: u64) -> Option<Vec<u8>> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() > limit {
        return None;
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).ok()?);
    file.by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;
    (u64::try_from(bytes.len()).ok()? <= limit).then_some(bytes)
}

fn wait_bounded(child: &mut Child) -> Option<ExitStatus> {
    let deadline = Instant::now() + GIT_DEADLINE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                contain_process_group(child);
                return Some(status);
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(None) | Err(_) => {
                terminate_process_group(child);
                return None;
            }
        }
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    // SAFETY: setpgid is async-signal-safe and touches no Rust-managed memory.
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn contain_process_group(child: &mut Child) {
    let Ok(group) = i32::try_from(child.id()) else {
        return;
    };
    // A direct child can exit while a descendant retains its stdout pipe.
    // Kill only when this private group still has members.
    if unsafe { libc::kill(-group, 0) } == 0 {
        unsafe {
            libc::kill(-group, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn contain_process_group(_child: &mut Child) {}

fn terminate_process_group(child: &mut Child) {
    #[cfg(unix)]
    if let Ok(group) = i32::try_from(child.id()) {
        unsafe {
            libc::kill(-group, libc::SIGKILL);
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(repository: &Path, arguments: &[&str]) {
        assert!(
            Command::new(GIT_EXECUTABLE)
                .args(arguments)
                .current_dir(repository)
                .status()
                .unwrap()
                .success()
        );
    }

    fn committed_repository() -> (tempfile::TempDir, String) {
        let repository = tempfile::tempdir().unwrap();
        git(repository.path(), &["init", "-q"]);
        std::fs::create_dir(repository.path().join("crates")).unwrap();
        std::fs::write(repository.path().join("crates/tracked"), "one").unwrap();
        git(repository.path(), &["add", "crates/tracked"]);
        git(
            repository.path(),
            &[
                "-c",
                "user.name=Bench",
                "-c",
                "user.email=bench@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
        );
        let canonical = repository.path().canonicalize().unwrap();
        let git_dir = discover_git_dir(&canonical).unwrap();
        let (_, head) = run_git_output_bounded(
            &canonical,
            &git_dir,
            &["rev-parse", "--verify", "HEAD"],
            MAX_HEAD_BYTES,
        )
        .expect("head probe");
        let head = std::str::from_utf8(&head).unwrap().trim().to_string();
        (repository, head)
    }

    #[test]
    fn runtime_checkout_probe_rejects_untracked_files() {
        let (repository, head) = committed_repository();
        verify_source_checkout(repository.path(), &head).unwrap();

        std::fs::write(repository.path().join("untracked"), "two").unwrap();
        assert!(
            verify_source_checkout(repository.path(), &head)
                .unwrap_err()
                .contains("untracked")
        );
    }

    #[test]
    fn runtime_checkout_probe_rejects_ignored_source_inputs() {
        let (repository, head) = committed_repository();
        std::fs::write(
            repository.path().join(".git/info/exclude"),
            "crates/generated.rs\n",
        )
        .unwrap();
        std::fs::write(repository.path().join("crates/generated.rs"), "generated").unwrap();

        let error = verify_source_checkout(repository.path(), &head).unwrap_err();
        assert!(error.contains("untracked source inputs"), "{error}");
    }

    #[test]
    fn runtime_checkout_probe_rejects_assume_unchanged_entries() {
        let (repository, head) = committed_repository();
        git(
            repository.path(),
            &["update-index", "--assume-unchanged", "crates/tracked"],
        );
        std::fs::write(repository.path().join("crates/tracked"), "changed").unwrap();

        let error = verify_source_checkout(repository.path(), &head).unwrap_err();
        assert!(error.contains("assume-unchanged"), "{error}");
    }

    #[test]
    fn runtime_checkout_probe_overrides_hostile_stat_cache_configuration() {
        let (repository, head) = committed_repository();
        let tracked = repository.path().join("crates/tracked");
        let original = std::fs::metadata(&tracked).unwrap();
        git(repository.path(), &["config", "core.trustctime", "false"]);
        git(repository.path(), &["config", "core.checkStat", "minimal"]);
        std::fs::write(&tracked, "two").unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&tracked)
            .unwrap();
        file.set_times(
            std::fs::FileTimes::new()
                .set_accessed(original.accessed().unwrap())
                .set_modified(original.modified().unwrap()),
        )
        .unwrap();

        let error = verify_source_checkout(repository.path(), &head).unwrap_err();
        assert!(error.contains("raw bytes differ"), "{error}");
    }

    #[test]
    fn runtime_checkout_probe_compares_raw_bytes_without_clean_filters() {
        let (repository, _head) = committed_repository();
        std::fs::write(
            repository.path().join(".gitattributes"),
            "crates/tracked filter=constant\n",
        )
        .unwrap();
        git(
            repository.path(),
            &["config", "filter.constant.clean", "sed 's/.*/one/'"],
        );
        git(
            repository.path(),
            &["add", ".gitattributes", "crates/tracked"],
        );
        git(
            repository.path(),
            &[
                "-c",
                "user.name=Bench",
                "-c",
                "user.email=bench@example.invalid",
                "commit",
                "-qm",
                "attributes",
            ],
        );
        let canonical = repository.path().canonicalize().unwrap();
        let git_dir = discover_git_dir(&canonical).unwrap();
        let (_, head) = run_git_output_bounded(
            &canonical,
            &git_dir,
            &["rev-parse", "--verify", "HEAD"],
            MAX_HEAD_BYTES,
        )
        .unwrap();
        let head = std::str::from_utf8(&head).unwrap().trim();
        std::fs::write(repository.path().join("crates/tracked"), "two").unwrap();

        let error = verify_source_checkout(repository.path(), head).unwrap_err();
        assert!(error.contains("raw bytes differ"), "{error}");
    }

    #[test]
    fn runtime_checkout_probe_ignores_configured_worktree_redirection() {
        let (repository, head) = committed_repository();
        let alternate = tempfile::tempdir().unwrap();
        assert!(
            Command::new(GIT_EXECUTABLE)
                .args(["config", "core.worktree"])
                .arg(alternate.path())
                .current_dir(repository.path())
                .status()
                .unwrap()
                .success()
        );
        std::fs::write(repository.path().join("crates/tracked"), "changed").unwrap();

        let error = verify_source_checkout(repository.path(), &head).unwrap_err();
        assert!(error.contains("raw bytes differ"), "{error}");
    }

    #[test]
    fn runtime_checkout_probe_ignores_git_replacement_objects() {
        let (repository, original_head) = committed_repository();
        std::fs::write(repository.path().join("crates/tracked"), "replacement").unwrap();
        git(repository.path(), &["add", "crates/tracked"]);
        git(
            repository.path(),
            &[
                "-c",
                "user.name=Bench",
                "-c",
                "user.email=bench@example.invalid",
                "commit",
                "-qm",
                "replacement",
            ],
        );
        let canonical = repository.path().canonicalize().unwrap();
        let git_dir = discover_git_dir(&canonical).unwrap();
        let (_, replacement_head) = run_git_output_bounded(
            &canonical,
            &git_dir,
            &["rev-parse", "--verify", "HEAD"],
            MAX_HEAD_BYTES,
        )
        .unwrap();
        let replacement_head = std::str::from_utf8(&replacement_head).unwrap().trim();
        git(repository.path(), &["reset", "--soft", &original_head]);
        git(
            repository.path(),
            &["replace", &original_head, replacement_head],
        );

        let error = verify_source_checkout(repository.path(), &original_head).unwrap_err();
        assert!(error.contains("tracked changes"), "{error}");
    }
}
