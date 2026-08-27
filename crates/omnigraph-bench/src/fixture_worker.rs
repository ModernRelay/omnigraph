//! Private, bounded process boundary for deterministic fixture construction.
//!
//! Fixture construction performs substantial engine I/O before repetitions
//! begin. Public runs execute it in a dedicated process group so a blocked
//! engine or filesystem operation can be killed and reaped without stranding
//! an in-process task that still owns the disposable store.

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
#[cfg(unix)]
use std::process::{Command, ExitStatus, Stdio};
use std::time::Duration;
#[cfg(unix)]
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::branch_merge::{BranchMergePlan, FixtureBuildSummary, initialize_local_fixture};
use crate::case::CaseV1;
use crate::reset::{MetadataDigest, PhysicalDigest, TraversalLimits, freeze_clonefile_template};
#[cfg(unix)]
use crate::runner::{
    ChildProcessEvidence, configure_fixture_child_environment,
    validate_fixture_child_runtime_overrides,
};
use crate::runner::{RunnerError, RunnerResult};
use crate::{ValidatedCase, validate_case};

const FIXTURE_PROTOCOL_VERSION: u32 = 2;
const MAX_FIXTURE_PROTOCOL_BYTES: u64 = 1024 * 1024;
const PROCESS_POLL: Duration = Duration::from_millis(10);
const REAP_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureRequestV1 {
    protocol_version: u32,
    case: CaseV1,
    expected_point_id: String,
    expected_case_digest: String,
    active_root: PathBuf,
    template_root: PathBuf,
    fixture_scratch_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixtureBuildHandoff {
    pub summary: FixtureBuildSummary,
    pub physical: PhysicalDigest,
    pub template_metadata: MetadataDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "kebab-case", deny_unknown_fields)]
enum FixtureResultV1 {
    Complete {
        protocol_version: u32,
        point_id: String,
        case_digest: String,
        handoff: FixtureBuildHandoff,
    },
    Failed {
        protocol_version: u32,
        code: String,
        message: String,
    },
}

/// Run the private fixture-builder endpoint using bounded request/result files.
///
/// Standard streams are deliberately not part of this protocol; the parent
/// redirects both to null, so an untrusted diagnostic burst cannot deadlock or
/// grow parent memory. The result file is create-new and size bounded.
pub async fn run_fixture_worker_files_v1(request_path: &Path, result_path: &Path) -> ExitCode {
    let result = run_fixture_worker(request_path).await;
    let success = matches!(result, FixtureResultV1::Complete { .. });
    if let Err(error) = write_new_json(result_path, &result) {
        eprintln!("fixture worker could not write its bounded result: {error}");
        return ExitCode::from(2);
    }
    if success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

async fn run_fixture_worker(request_path: &Path) -> FixtureResultV1 {
    let request = match read_bounded_json::<FixtureRequestV1>(request_path) {
        Ok(request) if request.protocol_version == FIXTURE_PROTOCOL_VERSION => request,
        Ok(request) => {
            return failure(
                "fixture_protocol_error",
                format!(
                    "fixture request protocol version {} is unsupported; expected {FIXTURE_PROTOCOL_VERSION}",
                    request.protocol_version
                ),
            );
        }
        Err(error) => return failure("fixture_protocol_error", error),
    };
    if let Err(error) = validate_fixture_paths(&request) {
        return failure("fixture_protocol_error", error);
    }
    if let Err(error) = validate_fixture_child_runtime_overrides(&request.fixture_scratch_root) {
        return failure(error.code, error.message);
    }
    execute_fixture_request(request).await
}

async fn execute_fixture_request(request: FixtureRequestV1) -> FixtureResultV1 {
    let case = match validate_case(request.case).into_result() {
        Ok(case) => case,
        Err(diagnostics) => {
            return failure(
                "fixture_case_invalid",
                diagnostics
                    .into_iter()
                    .map(|diagnostic| format!("{}: {}", diagnostic.code, diagnostic.message))
                    .collect::<Vec<_>>()
                    .join("; "),
            );
        }
    };
    if case.point_id != request.expected_point_id
        || case.case_digest != request.expected_case_digest
    {
        return failure(
            "fixture_identity_mismatch",
            "fixture request identities do not match the revalidated case",
        );
    }
    let plan = match BranchMergePlan::try_from(&case) {
        Ok(plan) => plan,
        Err(error) => return failure("unsupported_runner_axis", error.to_string()),
    };
    let Some(active_uri) = request.active_root.to_str() else {
        return failure("non_utf8_path", "active fixture path is not valid UTF-8");
    };
    let summary = match initialize_local_fixture(active_uri, &plan).await {
        Ok(summary) => summary,
        Err(error) => return failure("fixture_build_failed", error.to_string()),
    };
    let frozen = match freeze_clonefile_template(
        &request.active_root,
        &request.template_root,
        TraversalLimits::default(),
    ) {
        Ok(frozen) => frozen,
        Err(error) => return failure("fixture_freeze_failed", error.to_string()),
    };
    let physical = frozen.physical_digest().clone();
    let template_metadata = frozen.metadata_digest().clone();
    if let Err(error) = remove_active_tree(&request.active_root) {
        return failure("fixture_active_remove_failed", error);
    }
    FixtureResultV1::Complete {
        protocol_version: FIXTURE_PROTOCOL_VERSION,
        point_id: case.point_id,
        case_digest: case.case_digest,
        handoff: FixtureBuildHandoff {
            summary,
            physical,
            template_metadata,
        },
    }
}

fn validate_fixture_paths(request: &FixtureRequestV1) -> Result<(), String> {
    if !request.active_root.is_absolute()
        || !request.template_root.is_absolute()
        || !request.fixture_scratch_root.is_absolute()
    {
        return Err("fixture protocol paths must be absolute".to_string());
    }
    if request
        .fixture_scratch_root
        .file_name()
        .and_then(|name| name.to_str())
        != Some("fixture-scratch-v1")
    {
        return Err("fixture scratch root has an invalid protocol name".to_string());
    }
    let active_metadata = std::fs::symlink_metadata(&request.active_root)
        .map_err(|error| format!("could not inspect fixture active root: {error}"))?;
    let scratch_metadata = std::fs::symlink_metadata(&request.fixture_scratch_root)
        .map_err(|error| format!("could not inspect fixture scratch root: {error}"))?;
    if active_metadata.file_type().is_symlink() || !active_metadata.is_dir() {
        return Err("fixture active root must be a real directory".to_string());
    }
    if scratch_metadata.file_type().is_symlink() || !scratch_metadata.is_dir() {
        return Err("fixture scratch root must be a real directory".to_string());
    }
    match std::fs::symlink_metadata(&request.template_root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => return Err("fixture template root must not exist before construction".to_string()),
        Err(error) => {
            return Err(format!(
                "could not inspect fixture template destination: {error}"
            ));
        }
    }
    let active = std::fs::canonicalize(&request.active_root)
        .map_err(|error| format!("could not resolve fixture active root: {error}"))?;
    let scratch = std::fs::canonicalize(&request.fixture_scratch_root)
        .map_err(|error| format!("could not resolve fixture scratch root: {error}"))?;
    if active.parent() != scratch.parent()
        || request.template_root.parent() != request.fixture_scratch_root.parent()
    {
        return Err(
            "fixture active, template, and scratch roots must be siblings on the verified backend"
                .to_string(),
        );
    }
    let mut entries = std::fs::read_dir(&scratch)
        .map_err(|error| format!("could not inspect fixture scratch contents: {error}"))?;
    if entries
        .next()
        .transpose()
        .map_err(|error| format!("could not inspect fixture scratch entry: {error}"))?
        .is_some()
    {
        return Err("fixture scratch root must be empty before construction".to_string());
    }
    Ok(())
}

fn failure(code: impl Into<String>, message: impl Into<String>) -> FixtureResultV1 {
    FixtureResultV1::Failed {
        protocol_version: FIXTURE_PROTOCOL_VERSION,
        code: code.into(),
        message: message.into(),
    }
}

/// Build, byte-digest, clone-freeze, and retire one active fixture in a
/// contained child process, then return only its checked handoff facts.
#[cfg(unix)]
pub(crate) fn supervise_fixture_build(
    executable: &Path,
    case: &ValidatedCase,
    active_root: &Path,
    template_root: &Path,
    workspace_root: &Path,
    watchdog: Duration,
) -> RunnerResult<FixtureBuildHandoff> {
    supervise_fixture_build_with_hook(
        executable,
        case,
        active_root,
        template_root,
        workspace_root,
        watchdog,
        |_| {},
    )
}

#[cfg(unix)]
fn supervise_fixture_build_with_hook<F>(
    executable: &Path,
    case: &ValidatedCase,
    active_root: &Path,
    template_root: &Path,
    workspace_root: &Path,
    watchdog: Duration,
    after_spawn: F,
) -> RunnerResult<FixtureBuildHandoff>
where
    F: FnOnce(i32),
{
    let request_path = workspace_root.join("fixture-request-v1.json");
    let result_path = workspace_root.join("fixture-result-v1.json");
    let fixture_scratch_root = workspace_root.join("fixture-scratch-v1");
    std::fs::create_dir(&fixture_scratch_root).map_err(|error| {
        RunnerError::new(
            "fixture_scratch_directory_error",
            format!(
                "could not create harness-owned fixture scratch directory {}: {error}",
                fixture_scratch_root.display()
            ),
        )
    })?;
    let request = FixtureRequestV1 {
        protocol_version: FIXTURE_PROTOCOL_VERSION,
        case: case.definition.clone(),
        expected_point_id: case.point_id.clone(),
        expected_case_digest: case.case_digest.clone(),
        active_root: active_root.to_path_buf(),
        template_root: template_root.to_path_buf(),
        fixture_scratch_root: fixture_scratch_root.clone(),
    };
    write_new_json(&request_path, &request)
        .map_err(|error| RunnerError::new("fixture_protocol_error", error))?;

    let mut command = Command::new(executable);
    configure_fixture_child_environment(&mut command, &fixture_scratch_root);
    command
        .arg("__fixture-worker-v1")
        .arg(&request_path)
        .arg(&result_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_child_process_group(&mut command);
    let started = Instant::now();
    let mut child = command.spawn().map_err(|error| {
        RunnerError::new(
            "fixture_worker_spawn_failed",
            format!(
                "could not spawn fixture worker {}: {error}",
                executable.display()
            ),
        )
    })?;
    let process_group = match i32::try_from(child.id()) {
        Ok(process_group) => process_group,
        Err(_) => {
            let _ = child.kill();
            let status = child.wait().ok();
            return Err(RunnerError::new(
                "fixture_worker_pid_overflow",
                "fixture worker identifier does not fit the process-group API",
            )
            .with_child_process(fixture_evidence(
                "fixture-spawn",
                watchdog,
                started.elapsed(),
                "direct-child-kill",
                status,
                false,
            )));
        }
    };

    let mut child = FixtureProcess::new(child, process_group);
    after_spawn(process_group);

    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < watchdog => std::thread::sleep(PROCESS_POLL),
            Ok(None) => {
                let _ = kill_process_group(process_group);
                let status = child.wait_for_exit(REAP_DEADLINE);
                let group_gone =
                    wait_for_process_group_gone(process_group, REAP_DEADLINE).unwrap_or(false);
                return Err(RunnerError::new(
                    "fixture_build_watchdog_exceeded",
                    format!(
                        "fixture construction did not finish within {} seconds; its process group was killed",
                        watchdog.as_secs()
                    ),
                )
                .with_child_process(fixture_evidence(
                    "fixture-build-timeout",
                    watchdog,
                    started.elapsed(),
                    "sigkill",
                    status,
                    group_gone,
                )));
            }
            Err(error) => {
                let _ = kill_process_group(process_group);
                let status = child.wait_for_exit(REAP_DEADLINE);
                let group_gone =
                    wait_for_process_group_gone(process_group, REAP_DEADLINE).unwrap_or(false);
                return Err(RunnerError::new(
                    "fixture_worker_reap_failed",
                    format!("could not wait for fixture worker: {error}"),
                )
                .with_child_process(fixture_evidence(
                    "fixture-build-wait",
                    watchdog,
                    started.elapsed(),
                    "sigkill",
                    status,
                    group_gone,
                )));
            }
        }
    };

    let group_gone = process_group_is_gone(process_group).unwrap_or(false);
    if !group_gone {
        let _ = kill_process_group(process_group);
        let group_gone = wait_for_process_group_gone(process_group, REAP_DEADLINE).unwrap_or(false);
        return Err(RunnerError::new(
            "fixture_worker_descendant_leaked",
            "fixture worker exited while a descendant remained in its process group",
        )
        .with_child_process(fixture_evidence(
            "fixture-build-exit",
            watchdog,
            started.elapsed(),
            "sigkill-descendants",
            Some(status),
            group_gone,
        )));
    }

    let result = read_bounded_json::<FixtureResultV1>(&result_path).map_err(|error| {
        RunnerError::new("fixture_protocol_error", error).with_child_process(fixture_evidence(
            "fixture-build-result",
            watchdog,
            started.elapsed(),
            "worker-exited",
            Some(status),
            true,
        ))
    })?;
    let evidence = || {
        fixture_evidence(
            "fixture-build-result",
            watchdog,
            started.elapsed(),
            "worker-exited",
            Some(status),
            true,
        )
    };
    match result {
        FixtureResultV1::Complete {
            protocol_version,
            point_id,
            case_digest,
            handoff,
        } if status.success()
            && protocol_version == FIXTURE_PROTOCOL_VERSION
            && point_id == case.point_id
            && case_digest == case.case_digest =>
        {
            validate_handoff_summary(case, &handoff.summary)
                .map_err(|error| error.with_child_process(evidence()))?;
            remove_owned_tree(&fixture_scratch_root, "fixture scratch").map_err(|message| {
                RunnerError::new("fixture_scratch_cleanup_failed", message)
                    .with_child_process(evidence())
            })?;
            Ok(handoff)
        }
        FixtureResultV1::Failed {
            protocol_version,
            code,
            message,
        } if !status.success() && protocol_version == FIXTURE_PROTOCOL_VERSION => {
            Err(RunnerError::new(code, message).with_child_process(evidence()))
        }
        result => Err(RunnerError::new(
            "fixture_protocol_error",
            format!("fixture worker status/result mismatch: status={status}, result={result:?}"),
        )
        .with_child_process(evidence())),
    }
}

#[cfg(not(unix))]
pub(crate) fn supervise_fixture_build(
    _executable: &Path,
    _case: &ValidatedCase,
    _active_root: &Path,
    _template_root: &Path,
    _workspace_root: &Path,
    _watchdog: Duration,
) -> RunnerResult<FixtureBuildHandoff> {
    Err(RunnerError::new(
        "unsupported_fixture_worker_platform",
        "contained fixture construction requires a Unix process group",
    ))
}

fn validate_handoff_summary(
    case: &ValidatedCase,
    observed: &FixtureBuildSummary,
) -> RunnerResult<()> {
    let plan = BranchMergePlan::try_from(case)
        .map_err(|error| RunnerError::new("fixture_protocol_error", error.to_string()))?;
    let preflight = plan
        .preflight()
        .map_err(|error| RunnerError::new("fixture_protocol_error", error.to_string()))?;
    let expected_base_load_commits =
        usize::try_from(preflight.base_load_commits).map_err(|_| {
            RunnerError::new(
                "fixture_protocol_error",
                "parent-derived base-load publication count does not fit usize",
            )
        })?;
    let expected_history_depth = preflight.expected_history_depth;
    let expected_optimized_user_tables = usize::from(preflight.optimize_commits > 0)
        .checked_mul(plan.tables)
        .ok_or_else(|| {
            RunnerError::new(
                "fixture_protocol_error",
                "parent-derived optimized table count overflowed usize",
            )
        })?;
    let valid_logical_digest = observed.logical_content_sha256.len() == 64
        && observed
            .logical_content_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
    if observed.base_load_commits != expected_base_load_commits
        || observed.optimized_user_tables != expected_optimized_user_tables
        || observed.source_history_depth != expected_history_depth
        || observed.target_history_depth != expected_history_depth
        || !valid_logical_digest
    {
        return Err(RunnerError::new(
            "fixture_protocol_error",
            format!(
                "fixture worker summary disagrees with the parent-derived recipe or carries an invalid logical digest: observed={observed:?}"
            ),
        ));
    }
    Ok(())
}

fn remove_active_tree(active_root: &Path) -> Result<(), String> {
    remove_owned_tree(active_root, "active fixture")
}

fn remove_owned_tree(root: &Path, label: &str) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(root).map_err(|error| {
        format!(
            "could not inspect {label} {} before removal: {error}",
            root.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "{label} path is not a real directory: {}",
            root.display()
        ));
    }
    std::fs::remove_dir_all(root)
        .map_err(|error| format!("could not remove {label} {}: {error}", root.display()))
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let encoded = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    if encoded.len() as u64 > MAX_FIXTURE_PROTOCOL_BYTES {
        return Err(format!(
            "fixture protocol payload has {} bytes; limit is {MAX_FIXTURE_PROTOCOL_BYTES}",
            encoded.len()
        ));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("could not create {}: {error}", path.display()))?;
    file.write_all(&encoded)
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("could not durably write {}: {error}", path.display()))
}

fn read_bounded_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        // A protocol path is untrusted process-boundary input. O_NONBLOCK
        // prevents a FIFO or device from pinning the child or parent before
        // its watchdog can act, and O_NOFOLLOW prevents a symlink swap from
        // redirecting the read after the path was selected.
        options.custom_flags(nix::libc::O_NONBLOCK | nix::libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("could not open {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not stat {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!(
            "fixture protocol path is not a regular file: {}",
            path.display()
        ));
    }
    let length = metadata.len();
    if length > MAX_FIXTURE_PROTOCOL_BYTES {
        return Err(format!(
            "fixture protocol file {} has {length} bytes; limit is {MAX_FIXTURE_PROTOCOL_BYTES}",
            path.display()
        ));
    }
    let mut encoded = Vec::with_capacity(usize::try_from(length).unwrap_or(0));
    file.take(MAX_FIXTURE_PROTOCOL_BYTES + 1)
        .read_to_end(&mut encoded)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    if encoded.len() as u64 > MAX_FIXTURE_PROTOCOL_BYTES {
        return Err(format!(
            "fixture protocol file {} grew beyond {MAX_FIXTURE_PROTOCOL_BYTES} bytes",
            path.display()
        ));
    }
    serde_json::from_slice(&encoded)
        .map_err(|error| format!("could not decode {}: {error}", path.display()))
}

#[cfg(unix)]
fn configure_child_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

#[cfg(unix)]
fn kill_process_group(process_group: i32) -> Result<(), String> {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    match kill(Pid::from_raw(-process_group), Signal::SIGKILL) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(unix)]
fn process_group_is_gone(process_group: i32) -> Result<bool, String> {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    match kill(Pid::from_raw(-process_group), None) {
        Ok(()) => Ok(false),
        Err(Errno::ESRCH) => Ok(true),
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(unix)]
fn wait_for_process_group_gone(process_group: i32, timeout: Duration) -> Result<bool, String> {
    let started = Instant::now();
    loop {
        if process_group_is_gone(process_group)? {
            return Ok(true);
        }
        if started.elapsed() >= timeout {
            return Ok(false);
        }
        std::thread::sleep(PROCESS_POLL);
    }
}

#[cfg(unix)]
struct FixtureProcess {
    child: std::process::Child,
    process_group: i32,
    reaped: Option<ExitStatus>,
}

#[cfg(unix)]
impl FixtureProcess {
    fn new(child: std::process::Child, process_group: i32) -> Self {
        Self {
            child,
            process_group,
            reaped: None,
        }
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        if self.reaped.is_some() {
            return Ok(self.reaped);
        }
        let status = self.child.try_wait()?;
        if status.is_some() {
            self.reaped = status;
        }
        Ok(status)
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let started = Instant::now();
        loop {
            match self.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) if started.elapsed() < timeout => std::thread::sleep(PROCESS_POLL),
                Ok(None) | Err(_) => return None,
            }
        }
    }
}

#[cfg(unix)]
impl Drop for FixtureProcess {
    fn drop(&mut self) {
        let group_gone = process_group_is_gone(self.process_group).unwrap_or(false);
        if self.reaped.is_none() || !group_gone {
            let _ = kill_process_group(self.process_group);
            let _ = self.wait_for_exit(REAP_DEADLINE);
            let _ = wait_for_process_group_gone(self.process_group, REAP_DEADLINE);
        }
    }
}

#[cfg(unix)]
fn fixture_evidence(
    stage: &str,
    watchdog: Duration,
    elapsed: Duration,
    termination: &str,
    status: Option<ExitStatus>,
    process_group_gone: bool,
) -> ChildProcessEvidence {
    use std::os::unix::process::ExitStatusExt;

    ChildProcessEvidence {
        stage: stage.to_string(),
        measurement_watchdog_us: duration_us(watchdog),
        supervisor_elapsed_us: duration_us(elapsed),
        termination: termination.to_string(),
        exit_code: status.as_ref().and_then(ExitStatus::code),
        signal: status.as_ref().and_then(ExitStatusExt::signal),
        direct_child_reaped: status.is_some(),
        process_group_gone,
        stdio_closed_cleanly: true,
        ..ChildProcessEvidence::default()
    }
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use crate::parse_case;

    use super::*;

    fn test_case() -> ValidatedCase {
        parse_case(include_str!(
            "../../../benchmarks/cases/branch-merge-d50-warm.case-v1.yaml"
        ))
        .into_result()
        .unwrap()
    }

    #[cfg(target_os = "macos")]
    fn tiny_case() -> ValidatedCase {
        let mut definition = test_case().definition;
        definition.id = "fixture-worker-tiny".to_string();
        definition.fixture.data.tables = 2;
        definition.fixture.data.rows_per_table = 12;
        definition.fixture.state.history_depth = 6;
        definition.workload.delta_rows_per_side = 3;
        definition.workload.diverged_tables = 1;
        validate_case(definition).into_result().unwrap()
    }

    fn script(body: &str) -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fixture-worker-stub");
        std::fs::write(&path, format!("#!/bin/sh\n{body}")).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).unwrap();
        (directory, path)
    }

    fn shell_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }

    fn run_fifo_operation_with_deadline<T, F>(fifo: &Path, operation: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(PathBuf) -> T + Send + 'static,
    {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let fifo_for_thread = fifo.to_path_buf();
        let handle = std::thread::spawn(move || {
            let result = operation(fifo_for_thread);
            let _ = sender.send(result);
        });
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(result) => {
                handle.join().expect("FIFO protocol reader panicked");
                result
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                use std::os::unix::fs::OpenOptionsExt;

                // A read/write nonblocking endpoint releases a buggy blocking
                // read-open without introducing another unbounded test wait.
                let mut options = std::fs::OpenOptions::new();
                options
                    .read(true)
                    .write(true)
                    .custom_flags(nix::libc::O_NONBLOCK);
                let unblocker = options.open(fifo).expect("could not unblock test FIFO");
                drop(unblocker);
                if receiver.recv_timeout(Duration::from_secs(1)).is_ok() {
                    handle
                        .join()
                        .expect("blocked FIFO protocol reader panicked after release");
                } else {
                    std::mem::forget(handle);
                }
                panic!("fixture protocol reader blocked while opening a FIFO");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                handle.join().expect("FIFO protocol reader panicked");
                panic!("fixture protocol reader exited without reporting a result");
            }
        }
    }

    #[test]
    fn fixture_protocol_reader_rejects_fifo_without_blocking() {
        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("request.fifo");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::from_bits_truncate(0o600)).unwrap();

        let error = run_fifo_operation_with_deadline(&fifo, |path| {
            read_bounded_json::<serde_json::Value>(&path)
        })
        .unwrap_err();

        assert!(error.contains("not a regular file"), "{error}");
    }

    #[test]
    fn fixture_protocol_reader_rejects_symlink() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("request.json");
        let link = directory.path().join("request-link.json");
        std::fs::write(&target, br#"{"ok":true}"#).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let error = read_bounded_json::<serde_json::Value>(&link).unwrap_err();

        assert!(error.contains("could not open"), "{error}");
    }

    #[test]
    fn fixture_protocol_reader_rejects_character_device() {
        use std::os::unix::fs::FileTypeExt;

        let null = Path::new("/dev/null");
        assert!(
            std::fs::symlink_metadata(null)
                .unwrap()
                .file_type()
                .is_char_device()
        );

        let error = read_bounded_json::<serde_json::Value>(null).unwrap_err();

        assert!(error.contains("not a regular file"), "{error}");
    }

    #[test]
    fn hanging_fixture_worker_is_killed_and_reaped() {
        let (_script_directory, executable) = script("sleep 300\n");
        let workspace = tempfile::tempdir().unwrap();
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        std::fs::create_dir(&active).unwrap();
        let started = Instant::now();

        let error = supervise_fixture_build(
            &executable,
            &test_case(),
            &active,
            &template,
            workspace.path(),
            Duration::from_millis(200),
        )
        .unwrap_err();

        assert_eq!(error.code, "fixture_build_watchdog_exceeded");
        let evidence = error.context.child_process.unwrap();
        assert!(evidence.direct_child_reaped, "{evidence:?}");
        assert!(evidence.process_group_gone, "{evidence:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn panicking_fixture_supervisor_kills_and_reaps_its_child_group() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicI32, Ordering};

        use nix::errno::Errno;
        use nix::sys::wait::{WaitPidFlag, waitpid};
        use nix::unistd::Pid;

        let (_script_directory, executable) = script("sleep 300\n");
        let workspace = tempfile::tempdir().unwrap();
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        std::fs::create_dir(&active).unwrap();
        let observed_process_group = Arc::new(AtomicI32::new(0));
        let hook_process_group = observed_process_group.clone();

        let panic = std::panic::catch_unwind(|| {
            let _ = supervise_fixture_build_with_hook(
                &executable,
                &test_case(),
                &active,
                &template,
                workspace.path(),
                Duration::from_secs(2),
                |process_group| {
                    hook_process_group.store(process_group, Ordering::Release);
                    panic!("injected fixture supervisor panic after spawn");
                },
            );
        });

        assert!(panic.is_err());
        let process_group = observed_process_group.load(Ordering::Acquire);
        assert!(process_group > 0);
        assert!(
            process_group_is_gone(process_group).unwrap(),
            "fixture process group {process_group} survived supervisor unwind"
        );
        assert_eq!(
            waitpid(Pid::from_raw(process_group), Some(WaitPidFlag::WNOHANG)),
            Err(Errno::ECHILD),
            "fixture direct child was not reaped during supervisor unwind"
        );
    }

    #[test]
    fn malformed_fixture_handoff_is_rejected_after_containment() {
        let (_script_directory, executable) = script("printf 'not-json' > \"$3\"\n");
        let workspace = tempfile::tempdir().unwrap();
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        std::fs::create_dir(&active).unwrap();

        let error = supervise_fixture_build(
            &executable,
            &test_case(),
            &active,
            &template,
            workspace.path(),
            Duration::from_secs(2),
        )
        .unwrap_err();

        assert_eq!(error.code, "fixture_protocol_error");
        let evidence = error.context.child_process.unwrap();
        assert!(evidence.direct_child_reaped, "{evidence:?}");
        assert!(evidence.process_group_gone, "{evidence:?}");
    }

    #[test]
    fn forged_fixture_summary_is_rejected_against_parent_recipe() {
        let case = test_case();
        let result = FixtureResultV1::Complete {
            protocol_version: FIXTURE_PROTOCOL_VERSION,
            point_id: case.point_id.clone(),
            case_digest: case.case_digest.clone(),
            handoff: FixtureBuildHandoff {
                summary: FixtureBuildSummary {
                    base_load_commits: 0,
                    optimized_user_tables: 0,
                    source_history_depth: 0,
                    target_history_depth: 0,
                    logical_content_sha256: "0".repeat(64),
                },
                physical: PhysicalDigest {
                    files: 0,
                    bytes: 0,
                    digest_sha256: "0".repeat(64),
                },
                template_metadata: MetadataDigest {
                    entries: 0,
                    files: 0,
                    directories: 0,
                    bytes: 0,
                    shape_sha256: "0".repeat(64),
                    state_sha256: "0".repeat(64),
                },
            },
        };
        let encoded = serde_json::to_string(&result).unwrap();
        let body = format!("printf '%s' {} > \"$3\"\n", shell_quote(&encoded));
        let (_script_directory, executable) = script(&body);
        let workspace = tempfile::tempdir().unwrap();
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        std::fs::create_dir(&active).unwrap();

        let error = supervise_fixture_build(
            &executable,
            &case,
            &active,
            &template,
            workspace.path(),
            Duration::from_secs(2),
        )
        .unwrap_err();

        assert_eq!(error.code, "fixture_protocol_error");
        assert!(error.message.contains("parent-derived recipe"), "{error:?}");
        assert!(error.context.child_process.is_some(), "{error:?}");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn fixture_request_revalidates_builds_and_returns_checked_identity() {
        let workspace = tempfile::tempdir().unwrap();
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        let fixture_scratch_root = workspace.path().join("fixture-scratch-v1");
        std::fs::create_dir(&active).unwrap();
        std::fs::create_dir(&fixture_scratch_root).unwrap();
        let case = tiny_case();
        let result_path = workspace.path().join("result.json");
        let request = FixtureRequestV1 {
            protocol_version: FIXTURE_PROTOCOL_VERSION,
            case: case.definition.clone(),
            expected_point_id: case.point_id.clone(),
            expected_case_digest: case.case_digest.clone(),
            active_root: active.clone(),
            template_root: template.clone(),
            fixture_scratch_root,
        };
        validate_fixture_paths(&request).unwrap();
        let result = execute_fixture_request(request).await;
        assert!(matches!(result, FixtureResultV1::Complete { .. }));
        write_new_json(&result_path, &result).unwrap();
        match read_bounded_json::<FixtureResultV1>(&result_path).unwrap() {
            FixtureResultV1::Complete {
                point_id,
                case_digest,
                handoff,
                ..
            } => {
                assert_eq!(point_id, case.point_id);
                assert_eq!(case_digest, case.case_digest);
                assert_eq!(handoff.summary.source_history_depth, 6);
                assert_eq!(handoff.summary.target_history_depth, 6);
                assert!(!active.exists());
                assert!(template.is_dir());
            }
            result => panic!("unexpected fixture result: {result:?}"),
        }
    }
}
