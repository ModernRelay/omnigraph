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

use crate::dataset_identity::DatasetLogicalV1;
use crate::gqt_case::{DatasetBuildPlan, DatasetRecipe};
use crate::legacy::case::ResetMode;
use crate::reset::{
    MetadataDigest, PhysicalDigest, TraversalLimits, freeze_clonefile_template,
    freeze_plain_copy_template,
};
#[cfg(unix)]
use crate::runner::{
    ChildProcessEvidence, configure_fixture_child_environment,
    validate_fixture_child_runtime_overrides,
};
use crate::runner::{RunnerError, RunnerResult};

const FIXTURE_PROTOCOL_VERSION: u32 = 1;
const MAX_FIXTURE_PROTOCOL_BYTES: u64 = 1024 * 1024;
const PROCESS_POLL: Duration = Duration::from_millis(10);
const REAP_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureRequestV1 {
    protocol_version: u32,
    case: DatasetBuildPlan,
    registered_binding: Option<String>,
    expected_recipe_sha256: String,
    expected_plan_sha256: String,
    active_root: PathBuf,
    template_root: PathBuf,
    fixture_scratch_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureBuildHandoff {
    pub summary: DatasetLogicalV1,
    pub registered_source_identity: Option<String>,
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
        handoff: Box<FixtureBuildHandoff>,
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
pub async fn run_dataset_worker_files_v1(request_path: &Path, result_path: &Path) -> ExitCode {
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
    let case = request.case;
    if let Err(e) = case.revalidate() {
        return failure("fixture_case_invalid", e);
    }
    let plan_digest = match case.request_digest() {
        Ok(d) => d,
        Err(e) => return failure("fixture_case_invalid", e),
    };
    if case.recipe_sha256 != request.expected_recipe_sha256
        || plan_digest != request.expected_plan_sha256
    {
        return failure(
            "fixture_identity_mismatch",
            "frozen dataset recipe differs from request",
        );
    }
    let Some(active_uri) = request.active_root.to_str() else {
        return failure("non_utf8_path", "active root is not UTF-8");
    };
    let parsed = match &case.dataset {
        DatasetRecipe::Gqt { source } => source.parse(),
        DatasetRecipe::Registered { preparation, .. } => preparation.parse(),
    };
    let timeout = match parsed {
        Ok(c) => Duration::from_millis(c.runner.timeout_ms),
        Err(e) => return failure("fixture_case_invalid", e),
    };
    let (summary, registered_source_identity) = match tokio::time::timeout(
        timeout,
        build_dataset(
            &case,
            active_uri,
            &request.fixture_scratch_root,
            request.registered_binding.as_deref(),
        ),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return failure("fixture_build_failed", e),
        Err(_) => {
            return failure(
                "fixture_build_timeout",
                "dataset whole-file timeout exceeded",
            );
        }
    };
    let limits = TraversalLimits::default();
    let (physical, template_metadata) = match case.reset {
        ResetMode::LocalClonefile => {
            let frozen = match freeze_clonefile_template(
                &request.active_root,
                &request.template_root,
                limits,
            ) {
                Ok(frozen) => frozen,
                Err(error) => return failure("fixture_freeze_failed", error.to_string()),
            };
            (
                frozen.physical_digest().clone(),
                frozen.metadata_digest().clone(),
            )
        }
        ResetMode::PlainCopy => {
            let frozen = match freeze_plain_copy_template(
                &request.active_root,
                &request.template_root,
                limits,
            ) {
                Ok(frozen) => frozen,
                Err(error) => return failure("fixture_freeze_failed", error.to_string()),
            };
            (
                frozen.physical_digest().clone(),
                frozen.metadata_digest().clone(),
            )
        }
        ResetMode::None | ResetMode::S3Versioning => {
            return failure(
                "unsupported_runner_axis",
                "local fixture worker cannot freeze an S3 reset template",
            );
        }
    };
    if let Err(error) = remove_active_tree(&request.active_root) {
        return failure("fixture_active_remove_failed", error);
    }
    FixtureResultV1::Complete {
        protocol_version: FIXTURE_PROTOCOL_VERSION,
        point_id: case.recipe_sha256,
        case_digest: plan_digest,
        handoff: Box::new(FixtureBuildHandoff {
            summary,
            registered_source_identity,
            physical,
            template_metadata,
        }),
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
    case: &DatasetBuildPlan,
    registered_binding: Option<&str>,
    active_root: &Path,
    template_root: &Path,
    workspace_root: &Path,
    watchdog: Duration,
) -> RunnerResult<FixtureBuildHandoff> {
    supervise_fixture_build_with_hook(
        executable,
        case,
        registered_binding,
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
    case: &DatasetBuildPlan,
    registered_binding: Option<&str>,
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
        case: case.clone(),
        registered_binding: registered_binding.map(str::to_owned),
        expected_recipe_sha256: case.recipe_sha256.clone(),
        expected_plan_sha256: case
            .request_digest()
            .map_err(|e| RunnerError::new("fixture_case_invalid", e))?,
        active_root: active_root.to_path_buf(),
        template_root: template_root.to_path_buf(),
        fixture_scratch_root: fixture_scratch_root.clone(),
    };
    write_new_json(&request_path, &request)
        .map_err(|error| RunnerError::new("fixture_protocol_error", error))?;

    let mut command = Command::new(executable);
    configure_fixture_child_environment(&mut command, &fixture_scratch_root);
    command
        .arg("__dataset-worker-v1")
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
            && point_id == case.recipe_sha256
            && case_digest == request.expected_plan_sha256 =>
        {
            validate_handoff_summary(case, &handoff.summary)
                .map_err(|error| error.with_child_process(evidence()))?;
            remove_owned_tree(&fixture_scratch_root, "fixture scratch").map_err(|message| {
                RunnerError::new("fixture_scratch_cleanup_failed", message)
                    .with_child_process(evidence())
            })?;
            Ok(*handoff)
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
    _case: &DatasetBuildPlan,
    _registered_binding: Option<&str>,
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
    _case: &DatasetBuildPlan,
    observed: &DatasetLogicalV1,
) -> RunnerResult<()> {
    if !crate::gqt_case::digest(&observed.logical_content_sha256) || observed.branches.is_empty() {
        return Err(RunnerError::new(
            "fixture_protocol_error",
            "invalid dataset logical handoff",
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

pub(crate) async fn build_dataset(
    plan: &DatasetBuildPlan,
    uri: &str,
    scratch: &Path,
    binding: Option<&str>,
) -> Result<(DatasetLogicalV1, Option<String>), String> {
    use omnigraph::db::Omnigraph;
    use omnigraph_compiler::settings::Engine;
    use omnigraph_gqt_core::{PlainHost, case_session, execute_steps, seed_case};
    let (session, case, registered_identity) = match &plan.dataset {
        DatasetRecipe::Gqt { source } => {
            let case = source.parse()?;
            let fixture = case.fixture.as_ref().ok_or("dataset requires schema")?;
            let db = Omnigraph::init(uri, &fixture.schema)
                .await
                .map_err(|e| e.to_string())?;
            let session = case_session(db, &case, Engine::V2)?;
            seed_case(&session, &fixture.seed, plan.needs_indices).await?;
            (session, case, None)
        }
        DatasetRecipe::Registered {
            reference,
            preparation,
        } => {
            let binding = binding.ok_or("registered dataset requires --fixture ID=BUNDLE")?;
            let staged =
                crate::registered_fixture::stage_registered_fixture_binding(binding, Some(scratch))
                    .into_result()
                    .map_err(|e| format!("{e:?}"))?;
            if staged.receipt().fixture_id != reference.definition.fixture_id {
                return Err("registered binding fixture ID mismatch".into());
            }
            let observed = crate::real_graph::observe_real_graph(staged.root())
                .await
                .map_err(|e| e.to_string())?;
            crate::real_graph::validate_real_graph_reference(reference, &observed)
                .map_err(|e| e.to_string())?;
            let exact = crate::dataset_identity::registered_identity(staged.root()).await?;
            staged.verify_unchanged().map_err(|e| e.message)?;
            std::fs::remove_dir(uri).map_err(|e| e.to_string())?;
            let physical =
                crate::reset::digest_physical_tree(staged.root(), TraversalLimits::default())
                    .map_err(|e| e.to_string())?;
            crate::reset::copy_verified(
                staged.root(),
                Path::new(uri),
                &physical,
                TraversalLimits::default(),
            )
            .map_err(|e| e.to_string())?;
            let case = preparation.parse()?;
            let session = case_session(
                Omnigraph::open(uri).await.map_err(|e| e.to_string())?,
                &case,
                Engine::V2,
            )?;
            if plan.needs_indices {
                session.ensure_indices().await.map_err(|e| e.to_string())?;
            }
            (session, case, Some(exact))
        }
    };
    let session = execute_steps(
        &case,
        Path::new("frozen-dataset.gqt"),
        false,
        session,
        uri,
        None,
        &PlainHost,
    )
    .await?;
    drop(session);
    let mut logical = crate::dataset_identity::observe(Path::new(uri)).await?;
    if let Some(exact) = &registered_identity {
        logical.logical_content_sha256 = crate::model::typed_sha256(&(
            &logical.algorithm,
            &logical.logical_content_sha256,
            exact,
        ))
        .map_err(|e| e.to_string())?;
        logical.algorithm = crate::dataset_identity::REGISTERED_LOGICAL_ALGORITHM.into();
    }
    crate::dataset_identity::validate(&logical, registered_identity.as_deref())?;
    Ok((logical, registered_identity))
}
