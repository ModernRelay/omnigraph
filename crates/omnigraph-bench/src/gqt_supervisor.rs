//! Synchronous containment supervisor for one measured repetition worker.
//!
//! The caller runs this module from `spawn_blocking` and transfers ownership of
//! the disposable workspace into that blocking task. A canceled async caller
//! therefore cannot drop the store while a child mutation is still live.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::gqt_case::BoundGqt as ValidatedCase;
use crate::gqt_runner::GqtRepObservation as RepObservation;

use crate::gqt_protocol::{
    ChildFrameV1, MAX_WORKER_FRAME_BYTES, ParentFrameV1, WORKER_PROTOCOL_VERSION, WorkerBuildV1,
    WorkerRequestV1, WorkerStageV1, write_frame,
};
use crate::machine::MachineIdentityV1;
use crate::reset::{MetadataDigest, PhysicalDigest};
use crate::runner::{
    ChildProcessEvidence, RunnerError, RunnerResult, configure_benchmark_worker_environment,
    validate_worker_build_attestation,
};

const AUXILIARY_DEADLINE_FLOOR: Duration = Duration::from_secs(300);
const UNDECLARED_MEASUREMENT_WATCHDOG: Duration = Duration::from_secs(3_600);
const PROTOCOL_WRITE_DEADLINE: Duration = Duration::from_secs(30);
const REAP_DEADLINE: Duration = Duration::from_secs(10);
const PROCESS_GROUP_POLL: Duration = Duration::from_millis(10);
const PIPE_POLL: Duration = Duration::from_millis(10);
const PIPE_DRAIN_DEADLINE: Duration = Duration::from_secs(2);
const PIPE_STOP_DEADLINE: Duration = Duration::from_secs(1);
const MAX_CHILD_FRAMES: usize = 8;
const STDERR_TAIL_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct SupervisionInput {
    pub worker_executable: PathBuf,
    pub expected_worker_executable_sha256: String,
    /// The first worker establishes this identity. Later workers must report
    /// it exactly before the supervisor sends Begin.
    pub expected_machine: Option<MachineIdentityV1>,
    pub fixture_manifest_sha256: String,
    pub repetition: u32,
    pub case: ValidatedCase,
    pub repetition_root: PathBuf,
    pub worker_scratch_root: PathBuf,
    pub physical_digest: PhysicalDigest,
    pub metadata_digest: MetadataDigest,
    pub deadline: Option<Duration>,
    #[cfg(test)]
    pub auxiliary_deadline_override: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SupervisedRepetition {
    pub sample: RepObservation,
    pub worker_build: WorkerBuildV1,
    pub machine: MachineIdentityV1,
}

/// Supervise one fresh worker process through exactly one selected operation.
#[cfg(unix)]
pub(crate) fn supervise_repetition(input: SupervisionInput) -> RunnerResult<SupervisedRepetition> {
    if !lower_sha256(&input.fixture_manifest_sha256) {
        return Err(RunnerError::new(
            "fixture_stamp_invalid",
            "repetition input does not carry a canonical pre-measurement fixture stamp digest",
        )
        .with_repetition(input.repetition));
    }
    let request = ParentFrameV1::Request {
        protocol_version: WORKER_PROTOCOL_VERSION,
        request: Box::new(WorkerRequestV1 {
            repetition: input.repetition,
            case: input.case.clone(),
            expected_point_id: input.case.point_id.clone(),
            expected_case_digest: input.case.plan.case_digest.clone(),
            repetition_root: input.repetition_root.clone(),
            worker_scratch_root: input.worker_scratch_root.clone(),
            expected_physical_digest: input.physical_digest.clone(),
            expected_metadata_digest: input.metadata_digest.clone(),
        }),
    };
    crate::gqt_protocol::write_frame(&mut std::io::sink(), &request)
        .map_err(|e| RunnerError::new("worker_protocol_error", e.to_string()))?;
    let measurement_watchdog = measurement_watchdog(&input);
    let mut command = Command::new(&input.worker_executable);
    configure_benchmark_worker_environment(&mut command, &input.worker_scratch_root);
    command
        .arg("__gqt-worker-v1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_child_process_group(&mut command);
    let mut child = command.spawn().map_err(|error| {
        RunnerError::new(
            "worker_spawn_failed",
            format!(
                "could not spawn repetition worker {}: {error}",
                input.worker_executable.display()
            ),
        )
        .with_repetition(input.repetition)
    })?;
    let process_group = i32::try_from(child.id()).map_err(|_| {
        let _ = child.kill();
        let _ = child.wait();
        RunnerError::new(
            "worker_pid_overflow",
            "worker process identifier does not fit the process-group API",
        )
        .with_repetition(input.repetition)
    })?;
    let (_, empty_frames) = mpsc::channel();
    let mut worker = WorkerProcess {
        child,
        stdin_commands: None,
        stdin_stop: None,
        stdin_done: None,
        stdin_thread: None,
        frames: empty_frames,
        stdout_stop: None,
        stdout_done: None,
        stdout_thread: None,
        stderr_stop: None,
        stderr_result: None,
        stderr_thread: None,
        process_group,
        repetition: input.repetition,
        declared_deadline: input.deadline,
        measurement_watchdog,
        started: Instant::now(),
        reaped: None,
        settled_elapsed_us: None,
    };
    let Some(stdin) = worker.child.stdin.take() else {
        return worker.kill_error(
            "pipe-setup",
            "worker_pipe_failed",
            "worker stdin was not piped",
        );
    };
    let stdin_writer = match spawn_stdin_writer(stdin) {
        Ok(writer) => writer,
        Err(error) => {
            return worker.kill_error(
                "writer-setup",
                "worker_writer_spawn_failed",
                format!("could not start worker stdin writer: {error}"),
            );
        }
    };
    worker.stdin_commands = Some(stdin_writer.commands);
    worker.stdin_stop = Some(stdin_writer.stop);
    worker.stdin_done = Some(stdin_writer.done);
    worker.stdin_thread = Some(stdin_writer.thread);
    let Some(stdout) = worker.child.stdout.take() else {
        return worker.kill_error(
            "pipe-setup",
            "worker_pipe_failed",
            "worker stdout was not piped",
        );
    };
    let Some(stderr) = worker.child.stderr.take() else {
        return worker.kill_error(
            "pipe-setup",
            "worker_pipe_failed",
            "worker stderr was not piped",
        );
    };
    let frame_reader = match spawn_frame_reader(stdout) {
        Ok(reader) => reader,
        Err(error) => {
            return worker.kill_error(
                "reader-setup",
                "worker_reader_spawn_failed",
                format!("could not start worker stdout reader: {error}"),
            );
        }
    };
    worker.frames = frame_reader.frames;
    worker.stdout_stop = Some(frame_reader.stop);
    worker.stdout_done = Some(frame_reader.done);
    worker.stdout_thread = Some(frame_reader.thread);
    let (stderr_stop, stderr_result, stderr_thread) = match spawn_stderr_reader(stderr) {
        Ok(reader) => reader,
        Err(error) => {
            return worker.kill_error(
                "reader-setup",
                "worker_reader_spawn_failed",
                format!("could not start worker stderr reader: {error}"),
            );
        }
    };
    worker.stderr_stop = Some(stderr_stop);
    worker.stderr_result = Some(stderr_result);
    worker.stderr_thread = Some(stderr_thread);

    if let Err(error) = worker.write(&request, PROTOCOL_WRITE_DEADLINE) {
        return worker.kill_error("request-write", "worker_protocol_error", error);
    }

    let auxiliary_deadline = auxiliary_deadline(&input);
    let ready = match worker.receive(auxiliary_deadline) {
        Ok(frame) => frame,
        Err(ReceiveFailure::Timeout) => {
            return worker.kill_error(
                "prepare-timeout",
                "worker_prepare_timeout",
                format!(
                    "repetition {} did not finish open/cache preparation within {} seconds",
                    input.repetition,
                    auxiliary_deadline.as_secs()
                ),
            );
        }
        Err(ReceiveFailure::Protocol(message)) => {
            return worker.kill_error("prepare-protocol", "worker_protocol_error", message);
        }
    };
    let (worker_build, machine) = match ready {
        ChildFrameV1::Ready {
            protocol_version,
            repetition,
            point_id,
            case_digest,
            worker_build,
            machine,
            physical_digest,
            metadata_digest,
        } if protocol_version == WORKER_PROTOCOL_VERSION
            && repetition == input.repetition
            && point_id == input.case.point_id
            && case_digest == input.case.plan.case_digest
            && physical_digest == input.physical_digest
            && metadata_digest == input.metadata_digest =>
        {
            if let Err(error) = validate_worker_build_attestation(
                &worker_build,
                &input.expected_worker_executable_sha256,
            ) {
                return worker.kill_error(
                    "prepare-protocol",
                    "worker_protocol_error",
                    format!("worker build attestation was invalid: {}", error.message),
                );
            }
            if let Err(error) = machine.validate() {
                return worker.kill_error(
                    "prepare-protocol",
                    "worker_protocol_error",
                    format!("worker machine identity was invalid: {error}"),
                );
            }
            if input
                .expected_machine
                .as_ref()
                .is_some_and(|expected| expected != machine.as_ref())
            {
                return worker.kill_error(
                    "prepare-protocol",
                    "worker_machine_identity_changed",
                    "repetition worker machine identity differs from the first worker in this run",
                );
            }
            (*worker_build, *machine)
        }
        ChildFrameV1::Failed {
            stage,
            code,
            message,
            settled_sample,
            ..
        } => {
            if settled_sample.is_some()
                || !matches!(
                    stage,
                    WorkerStageV1::Bootstrap
                        | WorkerStageV1::Prepare
                        | WorkerStageV1::Finalize
                        | WorkerStageV1::Protocol
                )
            {
                return worker.kill_error(
                    "prepare-protocol",
                    "worker_protocol_error",
                    format!("worker sent an out-of-order {stage:?} failure before Ready"),
                );
            }
            return worker.structured_failure(stage, code, message, settled_sample);
        }
        frame => {
            return worker.kill_error(
                "prepare-protocol",
                "worker_protocol_error",
                format!("worker sent an invalid ready frame: {frame:?}"),
            );
        }
    };

    let begin = ParentFrameV1::Begin {
        protocol_version: WORKER_PROTOCOL_VERSION,
        repetition: input.repetition,
    };
    let measured_started = Instant::now();
    if let Err(error) = worker.write(
        &begin,
        remaining(measurement_watchdog, measured_started.elapsed()),
    ) {
        return worker.kill_error("begin-write", "worker_protocol_error", error);
    }
    let settled = match worker.receive(remaining(measurement_watchdog, measured_started.elapsed()))
    {
        Ok(frame) => frame,
        Err(ReceiveFailure::Timeout) => {
            let (code, message) =
                measurement_timeout_failure(&input, "did not settle before the supervisor ceiling");
            return worker.kill_error("measure-timeout", code, message);
        }
        Err(ReceiveFailure::Protocol(message)) => {
            return worker.kill_error("measure-protocol", "worker_protocol_error", message);
        }
    };
    let supervisor_settled_elapsed = measured_started.elapsed();
    if supervisor_settled_elapsed > measurement_watchdog {
        let (code, message) = measurement_timeout_failure(
            &input,
            "was not observed settled before the supervisor ceiling",
        );
        return worker.kill_error("measure-timeout", code, message);
    }
    let settled_elapsed_us = match settled {
        ChildFrameV1::Settled {
            protocol_version,
            repetition,
            elapsed_us,
        } if protocol_version == WORKER_PROTOCOL_VERSION && repetition == input.repetition => {
            elapsed_us
        }
        ChildFrameV1::Failed {
            stage,
            code,
            message,
            settled_sample,
            ..
        } => {
            if settled_sample.is_some()
                || !matches!(
                    stage,
                    WorkerStageV1::Prepare
                        | WorkerStageV1::Measure
                        | WorkerStageV1::Finalize
                        | WorkerStageV1::Protocol
                )
            {
                return worker.kill_error(
                    "measure-protocol",
                    "worker_protocol_error",
                    format!("worker sent an out-of-order {stage:?} failure before Settled"),
                );
            }
            return worker.structured_failure(stage, code, message, settled_sample);
        }
        frame => {
            return worker.kill_error(
                "measure-protocol",
                "worker_protocol_error",
                format!("worker sent an invalid settled frame: {frame:?}"),
            );
        }
    };
    worker.settled_elapsed_us = Some(settled_elapsed_us);
    let supervisor_settled_elapsed_us = duration_us(supervisor_settled_elapsed);
    if settled_elapsed_us > supervisor_settled_elapsed_us {
        return worker.kill_error(
            "measure-protocol",
            "worker_protocol_error",
            format!(
                "worker reported elapsed_us={settled_elapsed_us}, but the parent observed only {supervisor_settled_elapsed_us}us from before Begin through Settled"
            ),
        );
    }

    let complete = match worker.receive(auxiliary_deadline) {
        Ok(frame) => frame,
        Err(ReceiveFailure::Timeout) => {
            return worker.kill_error(
                "verify-timeout",
                "worker_verification_timeout",
                format!(
                    "repetition {} settled but did not finish exact verification within {} seconds",
                    input.repetition,
                    auxiliary_deadline.as_secs()
                ),
            );
        }
        Err(ReceiveFailure::Protocol(message)) => {
            return worker.kill_error("verify-protocol", "worker_protocol_error", message);
        }
    };
    let mut sample = match complete {
        ChildFrameV1::Complete {
            protocol_version,
            point_id,
            case_digest,
            sample,
        } if protocol_version == WORKER_PROTOCOL_VERSION
            && point_id == input.case.point_id
            && case_digest == input.case.plan.case_digest =>
        {
            *sample
        }
        ChildFrameV1::Failed {
            stage,
            code,
            message,
            settled_sample,
            ..
        } => {
            if !matches!(
                stage,
                WorkerStageV1::Measure
                    | WorkerStageV1::Verify
                    | WorkerStageV1::Finalize
                    | WorkerStageV1::Protocol
            ) {
                return worker.kill_error(
                    "verify-protocol",
                    "worker_protocol_error",
                    format!("worker sent an out-of-order {stage:?} failure after Settled"),
                );
            }
            if let Some(sample) = settled_sample.as_deref()
                && let Err(message) = crate::gqt_runner::validate_failed_sample(
                    sample,
                    &input.case,
                    input.repetition,
                    &input.physical_digest,
                    settled_elapsed_us,
                )
            {
                return worker.kill_error(
                    "verify-protocol",
                    "worker_protocol_error",
                    format!(
                        "worker failure carried invalid rejected-operation evidence: {message}"
                    ),
                );
            }
            return worker.structured_failure(stage, code, message, settled_sample);
        }
        frame => {
            return worker.kill_error(
                "verify-protocol",
                "worker_protocol_error",
                format!("worker sent an invalid completion frame: {frame:?}"),
            );
        }
    };
    if let Err(message) = validate_sample_admission(&sample, &input, settled_elapsed_us) {
        return worker.kill_error("finalize-protocol", "worker_protocol_error", message);
    }

    let child_exit = match worker.wait_for_exit(auxiliary_deadline) {
        Ok(child_exit) => child_exit,
        Err(message) => {
            return worker.kill_error("exit-timeout", "worker_exit_timeout", message);
        }
    };
    let group_gone = match process_group_is_gone(process_group) {
        Ok(gone) => gone,
        Err(error) => {
            return worker.kill_error(
                "group-proof",
                "worker_group_probe_failed",
                error.to_string(),
            );
        }
    };
    if !child_exit.status.success() || !group_gone {
        return worker.kill_error(
            "finalize-exit",
            "worker_exit_failed",
            format!(
                "worker reported completion but exited with {}; process_group_gone={group_gone}",
                child_exit.status
            ),
        );
    }
    let capture = worker.finish_capture();
    let trailing_output = worker
        .frames
        .try_iter()
        .map(|frame| match frame {
            Ok(frame) => format!("unexpected trailing frame {frame:?}"),
            Err(error) => error,
        })
        .collect::<Vec<_>>();
    if !capture.threads_stopped
        || !capture.stdout_clean_eof
        || !capture.stderr.clean_eof
        || !trailing_output.is_empty()
    {
        let detail = if trailing_output.is_empty() {
            "worker stdio did not close cleanly".to_string()
        } else {
            trailing_output.join("; ")
        };
        return Err(RunnerError::new(
            "worker_protocol_error",
            format!("worker completion was followed by invalid output: {detail}"),
        )
        .with_gqt_settled_elapsed(settled_elapsed_us)
        .with_repetition(input.repetition)
        .with_child_process(process_evidence(
            "finalize-protocol".to_string(),
            input.deadline,
            measurement_watchdog,
            worker.started.elapsed(),
            "worker-completed".to_string(),
            Some(&child_exit),
            group_gone,
            capture,
        )));
    }

    sample.peak_rss_bytes = Some(child_exit.peak_rss_bytes);

    if let Some(deadline) = input.deadline {
        let deadline_us = duration_us(deadline);
        if settled_elapsed_us > deadline_us {
            return Err(RunnerError::new(
                "operation_deadline_exceeded",
                format!(
                    "repetition {} settled in {}us, beyond the declared {}us deadline",
                    input.repetition, settled_elapsed_us, deadline_us
                ),
            )
            .with_repetition(input.repetition)
            .with_gqt_settled_sample(sample));
        }
    }
    Ok(SupervisedRepetition {
        sample,
        worker_build,
        machine,
    })
}

fn lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(not(unix))]
pub(crate) fn supervise_repetition(input: SupervisionInput) -> RunnerResult<SupervisedRepetition> {
    Err(RunnerError::new(
        "unsupported_worker_platform",
        format!(
            "process-isolated repetition supervision is unavailable on this platform for repetition {}",
            input.repetition
        ),
    ))
}

#[cfg(unix)]
struct WorkerProcess {
    child: Child,
    stdin_commands: Option<SyncSender<StdinWrite>>,
    stdin_stop: Option<Arc<AtomicBool>>,
    stdin_done: Option<Receiver<()>>,
    stdin_thread: Option<JoinHandle<()>>,
    frames: Receiver<Result<ChildFrameV1, String>>,
    stdout_stop: Option<Arc<AtomicBool>>,
    stdout_done: Option<Receiver<Result<(), String>>>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_stop: Option<Arc<AtomicBool>>,
    stderr_result: Option<Receiver<StderrCapture>>,
    stderr_thread: Option<JoinHandle<()>>,
    process_group: i32,
    repetition: u32,
    declared_deadline: Option<Duration>,
    measurement_watchdog: Duration,
    started: Instant,
    reaped: Option<ReapedChild>,
    settled_elapsed_us: Option<u64>,
}

#[cfg(unix)]
#[derive(Debug, Clone)]
struct ReapedChild {
    status: ExitStatus,
    peak_rss_bytes: u64,
}

#[cfg(unix)]
impl WorkerProcess {
    fn write(&mut self, frame: &ParentFrameV1, timeout: Duration) -> Result<(), String> {
        let mut encoded = Vec::new();
        write_frame(&mut encoded, frame).map_err(|error| error.to_string())?;
        let commands = self
            .stdin_commands
            .as_ref()
            .ok_or_else(|| "worker stdin is already closed".to_string())?;
        let (completed, completion) = mpsc::sync_channel(1);
        match commands.try_send(StdinWrite { encoded, completed }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err("worker stdin already has a pending protocol write".to_string());
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err("worker stdin writer stopped unexpectedly".to_string());
            }
        }
        match completion.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => Err(format!(
                "worker stdin write did not complete within {}ms",
                timeout.as_millis()
            )),
            Err(RecvTimeoutError::Disconnected) => {
                Err("worker stdin writer stopped before acknowledging the frame".to_string())
            }
        }
    }

    fn receive(&self, timeout: Duration) -> Result<ChildFrameV1, ReceiveFailure> {
        match self.frames.recv_timeout(timeout) {
            Ok(Ok(frame)) if frame.protocol_version() == WORKER_PROTOCOL_VERSION => Ok(frame),
            Ok(Ok(frame)) => Err(ReceiveFailure::Protocol(format!(
                "worker sent protocol version {}, expected {}",
                frame.protocol_version(),
                WORKER_PROTOCOL_VERSION
            ))),
            Ok(Err(message)) => Err(ReceiveFailure::Protocol(message)),
            Err(RecvTimeoutError::Timeout) => Err(ReceiveFailure::Timeout),
            Err(RecvTimeoutError::Disconnected) => Err(ReceiveFailure::Protocol(
                "worker stdout closed before the expected frame".to_string(),
            )),
        }
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<ReapedChild, String> {
        if let Some(reaped) = &self.reaped {
            return Ok(reaped.clone());
        }
        let started = Instant::now();
        loop {
            match wait4_nonblocking(self.process_group) {
                Ok(Some(reaped)) => {
                    self.reaped = Some(reaped.clone());
                    return Ok(reaped);
                }
                Ok(None) if started.elapsed() < timeout => {
                    std::thread::sleep(PROCESS_GROUP_POLL);
                }
                Ok(None) => {
                    return Err(format!(
                        "worker did not exit within {} seconds after its terminal frame",
                        timeout.as_secs()
                    ));
                }
                Err(error) => return Err(format!("could not wait for worker: {error}")),
            }
        }
    }

    fn structured_failure<T>(
        mut self,
        stage: WorkerStageV1,
        code: String,
        message: String,
        settled_sample: Option<Box<RepObservation>>,
    ) -> RunnerResult<T> {
        let status = self.wait_for_exit(REAP_DEADLINE).ok();
        let group_gone = process_group_is_gone(self.process_group).unwrap_or(false);
        if status.is_none() || !group_gone {
            return self.kill_error(
                "structured-failure-reap",
                "worker_reap_failed",
                format!(
                    "worker sent {stage:?} failure `{code}` but did not exit cleanly enough to release its store"
                ),
            );
        }
        let capture = self.finish_capture();
        let mut error = RunnerError::new(code, message)
            .with_repetition(self.repetition)
            .with_child_process(process_evidence(
                format!("{stage:?}"),
                self.declared_deadline,
                self.measurement_watchdog,
                self.started.elapsed(),
                "worker-failed".to_string(),
                status.as_ref(),
                group_gone,
                capture,
            ));
        error.context.gqt_settled_elapsed_us = self.settled_elapsed_us;
        if let Some(sample) = settled_sample {
            error = error.with_gqt_settled_sample(*sample);
        }
        Err(error)
    }

    fn kill_error<T>(
        mut self,
        stage: &str,
        code: &str,
        message: impl Into<String>,
    ) -> RunnerResult<T> {
        let message = message.into();
        let kill_result = kill_process_group(self.process_group);
        let status = self.wait_for_exit(REAP_DEADLINE).ok();
        let group_gone =
            wait_for_process_group_gone(self.process_group, REAP_DEADLINE).unwrap_or(false);
        let capture = if group_gone {
            self.finish_capture()
        } else {
            CaptureOutcome::default()
        };
        let termination = match kill_result {
            Ok(()) => "sigkill".to_string(),
            Err(error) => format!("sigkill-failed: {error}"),
        };
        let mut error = RunnerError::new(code, message)
            .with_repetition(self.repetition)
            .with_child_process(process_evidence(
                stage.to_string(),
                self.declared_deadline,
                self.measurement_watchdog,
                self.started.elapsed(),
                termination,
                status.as_ref(),
                group_gone,
                capture,
            ));
        error.context.gqt_settled_elapsed_us = self.settled_elapsed_us;
        Err(error)
    }

    fn finish_capture(&mut self) -> CaptureOutcome {
        self.stdin_commands.take();
        let stdin_stopped = finish_pipe_thread(
            "stdin writer",
            self.stdin_done.take(),
            self.stdin_stop.take(),
            self.stdin_thread.take(),
        )
        .is_ok();
        let stdout_result = finish_pipe_thread(
            "stdout reader",
            self.stdout_done.take(),
            self.stdout_stop.take(),
            self.stdout_thread.take(),
        );
        let stderr_result = finish_pipe_thread(
            "stderr reader",
            self.stderr_result.take(),
            self.stderr_stop.take(),
            self.stderr_thread.take(),
        );
        let stdout_clean_eof = matches!(&stdout_result, Ok(Ok(())));
        let stderr_stopped = stderr_result.is_ok();
        CaptureOutcome {
            stderr: stderr_result.unwrap_or_else(|error| StderrCapture {
                tail: format!("[stderr capture did not stop cleanly: {error}]").into_bytes(),
                truncated: false,
                clean_eof: false,
            }),
            stdout_clean_eof,
            threads_stopped: stdin_stopped && stdout_result.is_ok() && stderr_stopped,
        }
    }
}

#[cfg(unix)]
impl Drop for WorkerProcess {
    fn drop(&mut self) {
        let child_reaped = self.reaped.is_some();
        let group_gone = process_group_is_gone(self.process_group).unwrap_or(false);
        if !child_reaped || !group_gone {
            let _ = kill_process_group(self.process_group);
            let _ = self.wait_for_exit(REAP_DEADLINE);
            let _ = wait_for_process_group_gone(self.process_group, REAP_DEADLINE);
        }
        let _ = self.finish_capture();
    }
}

#[cfg(unix)]
struct StdinWrite {
    encoded: Vec<u8>,
    completed: SyncSender<Result<(), String>>,
}

#[cfg(unix)]
struct StdinWriter {
    commands: SyncSender<StdinWrite>,
    stop: Arc<AtomicBool>,
    done: Receiver<()>,
    thread: JoinHandle<()>,
}

#[cfg(unix)]
fn spawn_stdin_writer(mut stdin: std::process::ChildStdin) -> std::io::Result<StdinWriter> {
    set_nonblocking(&stdin)?;
    let (send, receive) = mpsc::sync_channel::<StdinWrite>(1);
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let (done_send, done) = mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("omnigraph-bench-worker-stdin".to_string())
        .spawn(move || {
            loop {
                match receive.recv_timeout(PIPE_POLL) {
                    Ok(write) => {
                        let result =
                            write_nonblocking(&mut stdin, &write.encoded, thread_stop.as_ref());
                        let failed = result.is_err();
                        let _ = write.completed.send(result);
                        if failed {
                            break;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) if !thread_stop.load(Ordering::Acquire) => {}
                    Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
                }
            }
            let _ = done_send.send(());
        })?;
    Ok(StdinWriter {
        commands: send,
        stop,
        done,
        thread,
    })
}

#[cfg(unix)]
enum ReceiveFailure {
    Timeout,
    Protocol(String),
}

#[cfg(unix)]
#[derive(Debug, Default)]
struct StderrCapture {
    tail: Vec<u8>,
    truncated: bool,
    clean_eof: bool,
}

#[cfg(unix)]
#[derive(Debug, Default)]
struct CaptureOutcome {
    stderr: StderrCapture,
    stdout_clean_eof: bool,
    threads_stopped: bool,
}

#[cfg(unix)]
struct FrameReader {
    frames: Receiver<Result<ChildFrameV1, String>>,
    stop: Arc<AtomicBool>,
    done: Receiver<Result<(), String>>,
    thread: JoinHandle<()>,
}

#[cfg(unix)]
fn spawn_frame_reader(mut stdout: std::process::ChildStdout) -> std::io::Result<FrameReader> {
    set_nonblocking(&stdout)?;
    let (send, receive) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let (done_send, done) = mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("omnigraph-bench-worker-stdout".to_string())
        .spawn(move || {
            let result = read_frame_pipe(&mut stdout, thread_stop.as_ref(), &send);
            if let Err(error) = &result {
                let _ = send.send(Err(error.clone()));
            }
            let _ = done_send.send(result);
        })?;
    Ok(FrameReader {
        frames: receive,
        stop,
        done,
        thread,
    })
}

#[cfg(unix)]
fn spawn_stderr_reader(
    mut stderr: std::process::ChildStderr,
) -> std::io::Result<(Arc<AtomicBool>, Receiver<StderrCapture>, JoinHandle<()>)> {
    set_nonblocking(&stderr)?;
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let (result_send, result) = mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("omnigraph-bench-worker-stderr".to_string())
        .spawn(move || {
            let capture = read_stderr_pipe(&mut stderr, thread_stop.as_ref());
            let _ = result_send.send(capture);
        })?;
    Ok((stop, result, thread))
}

#[cfg(unix)]
fn set_nonblocking<T: std::os::fd::AsFd>(pipe: &T) -> std::io::Result<()> {
    use nix::fcntl::{FcntlArg, OFlag, fcntl};

    let flags = fcntl(pipe, FcntlArg::F_GETFL).map_err(std::io::Error::from)?;
    let flags = OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK;
    fcntl(pipe, FcntlArg::F_SETFL(flags))
        .map(|_| ())
        .map_err(std::io::Error::from)
}

#[cfg(unix)]
fn write_nonblocking(
    writer: &mut impl Write,
    encoded: &[u8],
    stop: &AtomicBool,
) -> Result<(), String> {
    let mut offset = 0;
    while offset < encoded.len() {
        if stop.load(Ordering::Acquire) {
            return Err("worker stdin writer was stopped".to_string());
        }
        match writer.write(&encoded[offset..]) {
            Ok(0) => return Err("worker stdin closed during a protocol frame".to_string()),
            Ok(written) => offset += written,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(PIPE_POLL);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    writer.flush().map_err(|error| error.to_string())
}

#[cfg(unix)]
fn read_frame_pipe(
    reader: &mut impl Read,
    stop: &AtomicBool,
    send: &mpsc::Sender<Result<ChildFrameV1, String>>,
) -> Result<(), String> {
    let mut pending = Vec::new();
    let mut frames = 0usize;
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) if pending.is_empty() => return Ok(()),
            Ok(0) => {
                return Err(format!(
                    "worker stdout ended with {} unterminated frame bytes",
                    pending.len()
                ));
            }
            Ok(read) => {
                pending.extend_from_slice(&buffer[..read]);
                while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                    let mut framed = pending.drain(..=newline).collect::<Vec<_>>();
                    framed.pop();
                    if framed.is_empty() {
                        return Err("worker stdout contained an empty frame".to_string());
                    }
                    if framed.len() > MAX_WORKER_FRAME_BYTES {
                        return Err(format!(
                            "worker stdout frame has {} bytes; the limit is {MAX_WORKER_FRAME_BYTES}",
                            framed.len()
                        ));
                    }
                    frames += 1;
                    if frames > MAX_CHILD_FRAMES {
                        return Err(format!(
                            "worker emitted more than {MAX_CHILD_FRAMES} protocol frames"
                        ));
                    }
                    let frame = serde_json::from_slice::<ChildFrameV1>(&framed)
                        .map_err(|error| format!("could not decode worker frame: {error}"))?;
                    if send.send(Ok(frame)).is_err() {
                        return Ok(());
                    }
                }
                if pending.len() > MAX_WORKER_FRAME_BYTES {
                    return Err(format!(
                        "worker stdout unterminated frame exceeds {MAX_WORKER_FRAME_BYTES} bytes"
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if stop.load(Ordering::Acquire) {
                    return Err(
                        "worker stdout did not reach clean EOF before capture shutdown".to_string(),
                    );
                }
                std::thread::sleep(PIPE_POLL);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(format!("worker stdout read failed: {error}")),
        }
    }
}

#[cfg(unix)]
fn read_stderr_pipe(reader: &mut impl Read, stop: &AtomicBool) -> StderrCapture {
    let mut retained = VecDeque::with_capacity(STDERR_TAIL_BYTES);
    let mut truncated = false;
    let clean_eof = loop {
        let mut buffer = [0_u8; 8192];
        match reader.read(&mut buffer) {
            Ok(0) => break true,
            Ok(read) => retain_stderr(&mut retained, &buffer[..read], &mut truncated),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if stop.load(Ordering::Acquire) {
                    break false;
                }
                std::thread::sleep(PIPE_POLL);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => {
                retain_stderr(
                    &mut retained,
                    format!("\n[stderr read failed: {error}]").as_bytes(),
                    &mut truncated,
                );
                break false;
            }
        }
    };
    StderrCapture {
        tail: retained.into_iter().collect(),
        truncated,
        clean_eof,
    }
}

#[cfg(unix)]
fn retain_stderr(retained: &mut VecDeque<u8>, bytes: &[u8], truncated: &mut bool) {
    for byte in bytes {
        if retained.len() == STDERR_TAIL_BYTES {
            retained.pop_front();
            *truncated = true;
        }
        retained.push_back(*byte);
    }
}

#[cfg(unix)]
fn finish_pipe_thread<T>(
    name: &str,
    result: Option<Receiver<T>>,
    stop: Option<Arc<AtomicBool>>,
    thread: Option<JoinHandle<()>>,
) -> Result<T, String> {
    let result = result.ok_or_else(|| format!("{name} result channel was not installed"))?;
    let stop = stop.ok_or_else(|| format!("{name} stop flag was not installed"))?;
    let thread = thread.ok_or_else(|| format!("{name} thread was not installed"))?;
    let value = match result.recv_timeout(PIPE_DRAIN_DEADLINE) {
        Ok(value) => value,
        Err(RecvTimeoutError::Timeout) => {
            stop.store(true, Ordering::Release);
            result.recv_timeout(PIPE_STOP_DEADLINE).map_err(|error| {
                format!("{name} did not stop after bounded drain and cancellation: {error}")
            })?
        }
        Err(RecvTimeoutError::Disconnected) => {
            return Err(format!("{name} stopped without a completion result"));
        }
    };
    thread
        .join()
        .map_err(|_| format!("{name} panicked after reporting completion"))?;
    Ok(value)
}

#[cfg(unix)]
fn process_evidence(
    stage: String,
    declared_deadline: Option<Duration>,
    measurement_watchdog: Duration,
    elapsed: Duration,
    termination: String,
    child_exit: Option<&ReapedChild>,
    process_group_gone: bool,
    capture: CaptureOutcome,
) -> ChildProcessEvidence {
    use std::os::unix::process::ExitStatusExt;

    ChildProcessEvidence {
        stage,
        declared_deadline_us: declared_deadline.map(duration_us),
        measurement_watchdog_us: duration_us(measurement_watchdog),
        supervisor_elapsed_us: duration_us(elapsed),
        termination,
        exit_code: child_exit.and_then(|exit| exit.status.code()),
        signal: child_exit.and_then(|exit| exit.status.signal()),
        peak_rss_bytes: child_exit.map(|exit| exit.peak_rss_bytes),
        direct_child_reaped: child_exit.is_some(),
        process_group_gone,
        stdio_closed_cleanly: capture.threads_stopped
            && capture.stdout_clean_eof
            && capture.stderr.clean_eof,
        stderr_tail: String::from_utf8_lossy(&capture.stderr.tail).into_owned(),
        stderr_truncated: capture.stderr.truncated,
        quarantined_workspace: None,
    }
}

#[cfg(unix)]
fn wait4_nonblocking(pid: i32) -> Result<Option<ReapedChild>, String> {
    use std::os::unix::process::ExitStatusExt;

    let mut status = 0;
    let mut usage = std::mem::MaybeUninit::<nix::libc::rusage>::zeroed();
    loop {
        let reaped =
            unsafe { nix::libc::wait4(pid, &mut status, nix::libc::WNOHANG, usage.as_mut_ptr()) };
        if reaped == 0 {
            return Ok(None);
        }
        if reaped == pid {
            let usage = unsafe { usage.assume_init() };
            return Ok(Some(ReapedChild {
                status: ExitStatus::from_raw(status),
                peak_rss_bytes: normalized_peak_rss_bytes(&usage),
            }));
        }
        if reaped == -1 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(nix::libc::EINTR) {
                continue;
            }
            return Err(error.to_string());
        }
        return Err(format!(
            "wait4 reaped unexpected process {reaped}; expected {pid}"
        ));
    }
}

#[cfg(unix)]
fn normalized_peak_rss_bytes(usage: &nix::libc::rusage) -> u64 {
    let peak = u64::try_from(usage.ru_maxrss).unwrap_or(0);
    #[cfg(target_os = "macos")]
    return peak;
    #[cfg(not(target_os = "macos"))]
    return peak.saturating_mul(1024);
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
        std::thread::sleep(PROCESS_GROUP_POLL);
    }
}

#[cfg(unix)]
fn validate_sample_admission(
    sample: &RepObservation,
    input: &SupervisionInput,
    settled_elapsed_us: u64,
) -> Result<(), String> {
    crate::gqt_runner::validate_sample(
        sample,
        &input.case,
        input.repetition,
        &input.physical_digest,
        settled_elapsed_us,
        false,
    )
    .map_err(|error| error.to_string())
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn auxiliary_deadline(input: &SupervisionInput) -> Duration {
    #[cfg(test)]
    if let Some(override_deadline) = input.auxiliary_deadline_override {
        return override_deadline;
    }
    measurement_watchdog(input).max(AUXILIARY_DEADLINE_FLOOR)
}

fn measurement_watchdog(input: &SupervisionInput) -> Duration {
    input.deadline.unwrap_or(UNDECLARED_MEASUREMENT_WATCHDOG)
}

fn measurement_timeout_failure(input: &SupervisionInput, action: &str) -> (&'static str, String) {
    match input.deadline {
        Some(deadline) => (
            "operation_deadline_exceeded",
            format!(
                "repetition {} {action} within the declared {} second deadline; the worker process group was killed and reaped",
                input.repetition,
                deadline.as_secs()
            ),
        ),
        None => (
            "worker_measurement_watchdog_exceeded",
            format!(
                "repetition {} {action} within the independent {} second safety watchdog for an operation with no declared deadline; the worker process group was killed and reaped",
                input.repetition,
                UNDECLARED_MEASUREMENT_WATCHDOG.as_secs()
            ),
        ),
    }
}

fn remaining(total: Duration, elapsed: Duration) -> Duration {
    total.saturating_sub(elapsed)
}

/// Bound the complete request before admitting a dataset build, using fixed-width
/// digest placeholders and maximum-width counters for the not-yet-built tree.
pub(crate) fn preflight_plan(plan: &crate::gqt_case::PlannedGqt, cache: &Path) -> RunnerResult<()> {
    let case = plan
        .bind(
            &"0".repeat(64),
            crate::dataset_identity::REGISTERED_LOGICAL_ALGORITHM,
        )
        .map_err(|e| RunnerError::new("worker_protocol_error", e))?;
    let entry = cache.join("0".repeat(64));
    let request = ParentFrameV1::Request {
        protocol_version: WORKER_PROTOCOL_VERSION,
        request: Box::new(WorkerRequestV1 {
            repetition: 10_000,
            expected_point_id: case.point_id.clone(),
            expected_case_digest: case.plan.case_digest.clone(),
            case,
            repetition_root: entry.join("active"),
            worker_scratch_root: entry.join("worker-scratch-00010000"),
            expected_physical_digest: PhysicalDigest {
                files: u64::MAX,
                bytes: u64::MAX,
                digest_sha256: "0".repeat(64),
            },
            expected_metadata_digest: MetadataDigest {
                entries: u64::MAX,
                files: u64::MAX,
                directories: u64::MAX,
                bytes: u64::MAX,
                shape_sha256: "0".repeat(64),
                state_sha256: "0".repeat(64),
            },
        }),
    };
    crate::gqt_protocol::write_frame(&mut std::io::sink(), &request)
        .map_err(|e| RunnerError::new("worker_protocol_error", e.to_string()))
}
