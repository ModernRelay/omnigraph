//! Synchronous containment supervisor for one measured repetition worker.
//!
//! The caller runs this module from `spawn_blocking` and transfers ownership of
//! the disposable workspace into that blocking task. A canceled async caller
//! therefore cannot drop the store while a child mutation is still live.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::ValidatedCase;
use crate::branch_merge::{BranchMergePlan, TARGET_BRANCH};
use crate::reset::{MetadataDigest, PhysicalDigest};
use crate::runner::{ChildProcessEvidence, RepObservation, RunnerError, RunnerResult};
use crate::worker_protocol::{
    ChildFrameV1, MAX_WORKER_FRAME_BYTES, ParentFrameV1, WORKER_PROTOCOL_VERSION, WorkerBuildV1,
    WorkerRequestV1, WorkerStageV1, write_frame,
};

const AUXILIARY_DEADLINE_FLOOR: Duration = Duration::from_secs(300);
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
    pub worker_build: WorkerBuildV1,
    pub repetition: u32,
    pub case: ValidatedCase,
    pub repetition_root: PathBuf,
    pub physical_digest: PhysicalDigest,
    pub metadata_digest: MetadataDigest,
    pub deadline: Duration,
    #[cfg(test)]
    pub auxiliary_deadline_override: Option<Duration>,
}

/// Supervise one fresh worker process through exactly one mutation.
#[cfg(unix)]
pub(crate) fn supervise_repetition(input: SupervisionInput) -> RunnerResult<RepObservation> {
    let mut command = Command::new(&input.worker_executable);
    command
        .arg("__worker-v1")
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
        deadline: input.deadline,
        started: Instant::now(),
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

    let request = ParentFrameV1::Request {
        protocol_version: WORKER_PROTOCOL_VERSION,
        request: Box::new(WorkerRequestV1 {
            repetition: input.repetition,
            case: input.case.definition.clone(),
            expected_point_id: input.case.point_id.clone(),
            expected_case_digest: input.case.case_digest.clone(),
            repetition_root: input.repetition_root.clone(),
            expected_physical_digest: input.physical_digest.clone(),
            expected_metadata_digest: input.metadata_digest.clone(),
        }),
    };
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
                    "repetition {} did not finish open/warmth preparation within {} seconds",
                    input.repetition,
                    auxiliary_deadline.as_secs()
                ),
            );
        }
        Err(ReceiveFailure::Protocol(message)) => {
            return worker.kill_error("prepare-protocol", "worker_protocol_error", message);
        }
    };
    match ready {
        ChildFrameV1::Ready {
            protocol_version,
            repetition,
            point_id,
            case_digest,
            worker_build,
            physical_digest,
            metadata_digest,
        } if protocol_version == WORKER_PROTOCOL_VERSION
            && repetition == input.repetition
            && point_id == input.case.point_id
            && case_digest == input.case.case_digest
            && worker_build == input.worker_build
            && physical_digest == input.physical_digest
            && metadata_digest == input.metadata_digest => {}
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
    }

    let begin = ParentFrameV1::Begin {
        protocol_version: WORKER_PROTOCOL_VERSION,
        repetition: input.repetition,
    };
    let measured_started = Instant::now();
    if let Err(error) = worker.write(
        &begin,
        remaining(input.deadline, measured_started.elapsed()),
    ) {
        return worker.kill_error("begin-write", "worker_protocol_error", error);
    }
    let settled = match worker.receive(remaining(input.deadline, measured_started.elapsed())) {
        Ok(frame) => frame,
        Err(ReceiveFailure::Timeout) => {
            return worker.kill_error(
                "measure-timeout",
                "merge_deadline_exceeded",
                format!(
                    "repetition {} did not settle within the declared {} second deadline; the worker process group was killed and reaped",
                    input.repetition,
                    input.deadline.as_secs()
                ),
            );
        }
        Err(ReceiveFailure::Protocol(message)) => {
            return worker.kill_error("measure-protocol", "worker_protocol_error", message);
        }
    };
    let supervisor_settled_elapsed = measured_started.elapsed();
    if supervisor_settled_elapsed > input.deadline {
        return worker.kill_error(
            "measure-timeout",
            "merge_deadline_exceeded",
            format!(
                "repetition {} was not observed settled within the declared {} second supervisor deadline",
                input.repetition,
                input.deadline.as_secs()
            ),
        );
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
    let sample = match complete {
        ChildFrameV1::Complete {
            protocol_version,
            point_id,
            case_digest,
            sample,
        } if protocol_version == WORKER_PROTOCOL_VERSION
            && point_id == input.case.point_id
            && case_digest == input.case.case_digest =>
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
                && let Err(message) = validate_sample_admission(sample, &input, settled_elapsed_us)
            {
                return worker.kill_error(
                    "verify-protocol",
                    "worker_protocol_error",
                    format!("worker failure carried an invalid settled sample: {message}"),
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

    let status = match worker.wait_for_exit(auxiliary_deadline) {
        Ok(status) => status,
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
    if !status.success() || !group_gone {
        return worker.kill_error(
            "finalize-exit",
            "worker_exit_failed",
            format!(
                "worker reported completion but exited with {status}; process_group_gone={group_gone}"
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
        .with_repetition(input.repetition)
        .with_child_process(process_evidence(
            "finalize-protocol".to_string(),
            input.deadline,
            worker.started.elapsed(),
            "worker-completed".to_string(),
            Some(status),
            group_gone,
            capture,
        )));
    }

    let deadline_us = duration_us(input.deadline);
    if settled_elapsed_us > deadline_us {
        return Err(RunnerError::new(
            "merge_deadline_exceeded",
            format!(
                "repetition {} settled in {}us, beyond the declared {}us deadline",
                input.repetition, settled_elapsed_us, deadline_us
            ),
        )
        .with_repetition(input.repetition)
        .with_settled_sample(sample));
    }
    Ok(sample)
}

#[cfg(not(unix))]
pub(crate) fn supervise_repetition(input: SupervisionInput) -> RunnerResult<RepObservation> {
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
    deadline: Duration,
    started: Instant,
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

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<ExitStatus, String> {
        let started = Instant::now();
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Ok(status),
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

    fn structured_failure(
        mut self,
        stage: WorkerStageV1,
        code: String,
        message: String,
        settled_sample: Option<Box<RepObservation>>,
    ) -> RunnerResult<RepObservation> {
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
                self.deadline,
                self.started.elapsed(),
                "worker-failed".to_string(),
                status,
                group_gone,
                capture,
            ));
        if let Some(sample) = settled_sample {
            error = error.with_settled_sample(*sample);
        }
        Err(error)
    }

    fn kill_error(
        mut self,
        stage: &str,
        code: &str,
        message: impl Into<String>,
    ) -> RunnerResult<RepObservation> {
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
        Err(RunnerError::new(code, message)
            .with_repetition(self.repetition)
            .with_child_process(process_evidence(
                stage.to_string(),
                self.deadline,
                self.started.elapsed(),
                termination,
                status,
                group_gone,
                capture,
            )))
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
        let child_reaped = matches!(self.child.try_wait(), Ok(Some(_)));
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
    deadline: Duration,
    elapsed: Duration,
    termination: String,
    status: Option<ExitStatus>,
    process_group_gone: bool,
    capture: CaptureOutcome,
) -> ChildProcessEvidence {
    use std::os::unix::process::ExitStatusExt;

    ChildProcessEvidence {
        stage,
        declared_deadline_us: duration_us(deadline),
        supervisor_elapsed_us: duration_us(elapsed),
        termination,
        exit_code: status.as_ref().and_then(ExitStatus::code),
        signal: status.as_ref().and_then(ExitStatusExt::signal),
        direct_child_reaped: status.is_some(),
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
fn configure_child_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    // The standard-library hook is async-signal-safe; a custom `pre_exec`
    // closure in a multithreaded parent would not be.
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
    let plan = BranchMergePlan::try_from(&input.case)
        .map_err(|error| format!("parent could not derive the worker plan: {error}"))?;
    let expected_intervals = u64::try_from(plan.diverged_tables)
        .map_err(|_| "diverged table count does not fit u64".to_string())?;
    let expected_rows = plan
        .expected_merged_rows()
        .map_err(|error| format!("parent could not derive expected merged rows: {error}"))?;
    let table_walk_phases = sample
        .phases
        .iter()
        .filter(|phase| phase.phase == "TableWalk")
        .collect::<Vec<_>>();

    if sample.repetition != input.repetition
        || sample.elapsed_us != settled_elapsed_us
        || sample.input_physical_digest_sha256 != input.physical_digest.digest_sha256
        || sample.outcome != "merged"
        || sample.route.table_walk_intervals != expected_intervals
        || table_walk_phases.len() != 1
        || table_walk_phases[0].interval_count != expected_intervals
        || sample.verification.branch != TARGET_BRANCH
        || sample.verification.tables != plan.tables
        || sample.verification.rows != expected_rows
        || !sample.verification.exact_content
        || !sample.verification.source_exact_content
        || !sample.verification.main_exact_content
        || !sample.verification.protected_heads_unchanged
        || sample.logical_store_calls.physical_attempts_observed
    {
        return Err(format!(
            "worker sample failed parent admission: repetition={} elapsed_us={} digest={} outcome={} table_walk={} table_walk_phases={} verification_branch={} verification_tables={} verification_rows={} exact_target={} exact_source={} exact_main={} protected_heads={} physical_attempts_observed={}",
            sample.repetition,
            sample.elapsed_us,
            sample.input_physical_digest_sha256,
            sample.outcome,
            sample.route.table_walk_intervals,
            table_walk_phases.len(),
            sample.verification.branch,
            sample.verification.tables,
            sample.verification.rows,
            sample.verification.exact_content,
            sample.verification.source_exact_content,
            sample.verification.main_exact_content,
            sample.verification.protected_heads_unchanged,
            sample.logical_store_calls.physical_attempts_observed,
        ));
    }
    Ok(())
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn auxiliary_deadline(input: &SupervisionInput) -> Duration {
    #[cfg(test)]
    if let Some(override_deadline) = input.auxiliary_deadline_override {
        return override_deadline;
    }
    input.deadline.max(AUXILIARY_DEADLINE_FLOOR)
}

fn remaining(total: Duration, elapsed: Duration) -> Duration {
    total.saturating_sub(elapsed)
}

#[cfg(all(test, unix))]
mod tests {
    use crate::counting::LogicalCallCounts;
    use crate::runner::{
        ControlCallObservation, LogicalStoreCallObservation, MergeRouteObservation,
        PhaseObservation, VerificationObservation,
    };
    use crate::worker_protocol::ChildFrameV1;
    use crate::{ValidatedCase, parse_case};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard};

    use super::*;

    const TEST_REPETITION: u32 = 7;
    const TEST_ELAPSED_US: u64 = 1_000;
    const QUICK_DEADLINE: Duration = Duration::from_millis(250);
    const GENEROUS_DEADLINE: Duration = Duration::from_secs(2);
    const GENEROUS_AUXILIARY_DEADLINE: Duration = Duration::from_secs(2);
    static WORKER_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn test_case() -> ValidatedCase {
        parse_case(include_str!(
            "../../../benchmarks/cases/branch-merge-d50-warm.case-v1.yaml"
        ))
        .into_result()
        .unwrap()
    }

    fn physical_digest() -> PhysicalDigest {
        PhysicalDigest {
            files: 3,
            bytes: 42,
            digest_sha256: "a".repeat(64),
        }
    }

    fn metadata_digest() -> MetadataDigest {
        MetadataDigest {
            entries: 5,
            files: 3,
            directories: 2,
            bytes: 42,
            shape_sha256: "b".repeat(64),
            state_sha256: "c".repeat(64),
        }
    }

    fn worker_build() -> WorkerBuildV1 {
        WorkerBuildV1 {
            cargo_profile: "release".to_string(),
            opt_level: "3".to_string(),
            debug_assertions: false,
            executable_sha256: "d".repeat(64),
        }
    }

    fn supervision_input(worker_executable: PathBuf) -> SupervisionInput {
        SupervisionInput {
            worker_executable,
            worker_build: worker_build(),
            repetition: TEST_REPETITION,
            case: test_case(),
            repetition_root: PathBuf::from("/unused/supervisor-test-fixture"),
            physical_digest: physical_digest(),
            metadata_digest: metadata_digest(),
            deadline: GENEROUS_DEADLINE,
            auxiliary_deadline_override: Some(GENEROUS_AUXILIARY_DEADLINE),
        }
    }

    fn ready_frame(input: &SupervisionInput) -> ChildFrameV1 {
        ChildFrameV1::Ready {
            protocol_version: WORKER_PROTOCOL_VERSION,
            repetition: input.repetition,
            point_id: input.case.point_id.clone(),
            case_digest: input.case.case_digest.clone(),
            worker_build: input.worker_build.clone(),
            physical_digest: input.physical_digest.clone(),
            metadata_digest: input.metadata_digest.clone(),
        }
    }

    fn settled_frame(input: &SupervisionInput) -> ChildFrameV1 {
        ChildFrameV1::Settled {
            protocol_version: WORKER_PROTOCOL_VERSION,
            repetition: input.repetition,
            elapsed_us: TEST_ELAPSED_US,
        }
    }

    fn valid_sample(input: &SupervisionInput) -> RepObservation {
        let plan = BranchMergePlan::try_from(&input.case).unwrap();
        let intervals = u64::try_from(plan.diverged_tables).unwrap();
        RepObservation {
            repetition: input.repetition,
            input_physical_digest_sha256: input.physical_digest.digest_sha256.clone(),
            elapsed_us: TEST_ELAPSED_US,
            outcome: "merged".to_string(),
            phases: vec![PhaseObservation {
                phase: "TableWalk".to_string(),
                total_us: 700,
                max_us: 250,
                interval_count: intervals,
            }],
            route: MergeRouteObservation {
                table_walk_intervals: intervals,
                stage_merge_insert_calls: 0,
                stage_merge_insert_rows: 0,
                stage_known_present_update_calls: 0,
                stage_known_present_update_rows: 0,
                stage_fenced_insert_calls: 0,
                stage_fenced_insert_rows: 0,
                strict_insert_preflight_calls: 0,
            },
            logical_store_calls: LogicalStoreCallObservation {
                manifest: LogicalCallCounts::default(),
                table: LogicalCallCounts::default(),
                physical_attempts_observed: false,
            },
            control_store_calls: ControlCallObservation {
                read_text: 0,
                read_text_if_exists: 0,
                read_text_versioned: 0,
                exists: 0,
                list_dir: 0,
                mutation_calls: 0,
                write_text: 0,
                delete: 0,
            },
            verification: VerificationObservation {
                branch: TARGET_BRANCH.to_string(),
                tables: plan.tables,
                rows: plan.expected_merged_rows().unwrap(),
                exact_content: true,
                source_exact_content: true,
                main_exact_content: true,
                protected_heads_unchanged: true,
            },
        }
    }

    fn complete_frame(input: &SupervisionInput, sample: RepObservation) -> ChildFrameV1 {
        ChildFrameV1::Complete {
            protocol_version: WORKER_PROTOCOL_VERSION,
            point_id: input.case.point_id.clone(),
            case_digest: input.case.case_digest.clone(),
            sample: Box::new(sample),
        }
    }

    fn shell_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }

    fn emit(frame: &ChildFrameV1) -> String {
        let encoded = serde_json::to_string(frame).unwrap();
        format!("printf '%s\\n' {}\n", shell_quote(&encoded))
    }

    fn worker_script(body: &str) -> (MutexGuard<'static, ()>, tempfile::TempDir, PathBuf) {
        let guard = WORKER_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("worker-stub");
        std::fs::write(&path, format!("#!/bin/sh\n{body}")).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).unwrap();
        (guard, directory, path)
    }

    fn normal_exchange(input: &SupervisionInput, complete: ChildFrameV1) -> String {
        format!(
            "IFS= read -r request || exit 90\n{}IFS= read -r begin || exit 91\n{}{}",
            emit(&ready_frame(input)),
            emit(&settled_frame(input)),
            emit(&complete),
        )
    }

    fn assert_contained_failure(
        result: RunnerResult<RepObservation>,
        expected_code: &str,
        expected_stage: &str,
    ) -> RunnerError {
        let error = result.unwrap_err();
        assert_eq!(error.code, expected_code, "{error:?}");
        let evidence = error
            .context
            .child_process
            .as_ref()
            .expect("spawned worker failures must carry containment evidence");
        assert_eq!(evidence.stage, expected_stage, "{evidence:?}");
        assert!(evidence.direct_child_reaped, "{evidence:?}");
        assert!(evidence.process_group_gone, "{evidence:?}");
        error
    }

    #[test]
    fn valid_worker_completes_ready_begin_settled_complete_protocol() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        let expected = valid_sample(&input);
        let body = normal_exchange(&input, complete_frame(&input, expected.clone()));
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;

        let observed = supervise_repetition(input).unwrap();

        assert_eq!(observed, expected);
    }

    #[test]
    fn worker_that_never_reads_stdin_is_bounded_by_prepare_deadline() {
        let (_guard, _directory, worker) = worker_script("sleep 300\n");
        let mut input = supervision_input(worker);
        input.auxiliary_deadline_override = Some(QUICK_DEADLINE);
        let started = Instant::now();

        let error = assert_contained_failure(
            supervise_repetition(input),
            "worker_prepare_timeout",
            "prepare-timeout",
        );

        assert!(started.elapsed() < Duration::from_secs(5), "{error:?}");
        assert!(
            error
                .context
                .child_process
                .as_ref()
                .unwrap()
                .stdio_closed_cleanly
        );
    }

    #[test]
    fn worker_measurement_hang_is_killed_at_the_declared_deadline() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        input.deadline = QUICK_DEADLINE;
        let body = format!(
            "IFS= read -r request || exit 90\n{}IFS= read -r begin || exit 91\nsleep 300\n",
            emit(&ready_frame(&input)),
        );
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;

        let error = assert_contained_failure(
            supervise_repetition(input),
            "merge_deadline_exceeded",
            "measure-timeout",
        );

        assert!(
            error
                .context
                .child_process
                .as_ref()
                .unwrap()
                .supervisor_elapsed_us
                < duration_us(Duration::from_secs(5)),
            "{error:?}"
        );
    }

    #[test]
    fn worker_verification_hang_after_settled_is_bounded() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        input.auxiliary_deadline_override = Some(GENEROUS_AUXILIARY_DEADLINE);
        let body = format!(
            "IFS= read -r request || exit 90\n{}IFS= read -r begin || exit 91\n{}sleep 300\n",
            emit(&ready_frame(&input)),
            emit(&settled_frame(&input)),
        );
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;
        let started = Instant::now();

        assert_contained_failure(
            supervise_repetition(input),
            "worker_verification_timeout",
            "verify-timeout",
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn worker_that_reports_complete_but_does_not_exit_is_killed() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        input.auxiliary_deadline_override = Some(GENEROUS_AUXILIARY_DEADLINE);
        let sample = valid_sample(&input);
        let mut body = normal_exchange(&input, complete_frame(&input, sample));
        body.push_str("sleep 300\n");
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;
        let started = Instant::now();

        assert_contained_failure(
            supervise_repetition(input),
            "worker_exit_timeout",
            "exit-timeout",
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn valid_trailing_frame_after_complete_is_rejected() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        let sample = valid_sample(&input);
        let mut body = normal_exchange(&input, complete_frame(&input, sample));
        body.push_str(&emit(&ready_frame(&input)));
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;

        let error = assert_contained_failure(
            supervise_repetition(input),
            "worker_protocol_error",
            "finalize-protocol",
        );
        assert!(
            error
                .context
                .child_process
                .as_ref()
                .unwrap()
                .stdio_closed_cleanly
        );
    }

    #[test]
    fn malformed_trailing_output_after_complete_is_rejected() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        let sample = valid_sample(&input);
        let mut body = normal_exchange(&input, complete_frame(&input, sample));
        body.push_str("printf '%s\\n' '{not-json'\n");
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;

        let error = assert_contained_failure(
            supervise_repetition(input),
            "worker_protocol_error",
            "finalize-protocol",
        );
        assert!(!error.message.is_empty());
    }

    #[test]
    fn forged_ready_identity_is_rejected() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        let mut forged = ready_frame(&input);
        if let ChildFrameV1::Ready { point_id, .. } = &mut forged {
            *point_id = "0".repeat(64);
        }
        let body = format!(
            "IFS= read -r request || exit 90\n{}IFS= read -r begin || exit 91\n",
            emit(&forged),
        );
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;

        assert_contained_failure(
            supervise_repetition(input),
            "worker_protocol_error",
            "prepare-protocol",
        );
    }

    #[test]
    fn out_of_order_failed_frame_cannot_smuggle_settled_evidence_before_ready() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        let forged = ChildFrameV1::Failed {
            protocol_version: WORKER_PROTOCOL_VERSION,
            stage: WorkerStageV1::Verify,
            code: "forged-verification-failure".to_string(),
            message: "settled evidence arrived before Ready".to_string(),
            settled_sample: Some(Box::new(valid_sample(&input))),
        };
        let body = format!("IFS= read -r request || exit 90\n{}", emit(&forged),);
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;

        assert_contained_failure(
            supervise_repetition(input),
            "worker_protocol_error",
            "prepare-protocol",
        );
    }

    #[test]
    fn forged_complete_sample_is_rejected_by_parent_admission() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        let mut sample = valid_sample(&input);
        sample.verification.rows += 1;
        let body = normal_exchange(&input, complete_frame(&input, sample));
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;

        assert_contained_failure(
            supervise_repetition(input),
            "worker_protocol_error",
            "finalize-protocol",
        );
    }

    #[test]
    fn descendant_holding_inherited_pipes_prevents_false_clean_completion() {
        let placeholder = PathBuf::from("/placeholder");
        let mut input = supervision_input(placeholder);
        input.auxiliary_deadline_override = Some(GENEROUS_AUXILIARY_DEADLINE);
        let sample = valid_sample(&input);
        let mut body = normal_exchange(&input, complete_frame(&input, sample));
        body.push_str("sleep 300 &\nexit 0\n");
        let (_guard, _directory, worker) = worker_script(&body);
        input.worker_executable = worker;

        assert_contained_failure(
            supervise_repetition(input),
            "worker_exit_failed",
            "finalize-exit",
        );
    }

    #[test]
    fn sigkill_reaps_a_dedicated_process_group_before_returning() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("sleep 300");
        configure_child_process_group(&mut command);
        let mut child = command.spawn().unwrap();
        let process_group = i32::try_from(child.id()).unwrap();

        kill_process_group(process_group).unwrap();
        let started = Instant::now();
        while child.try_wait().unwrap().is_none() {
            assert!(started.elapsed() < REAP_DEADLINE);
            std::thread::sleep(PROCESS_GROUP_POLL);
        }
        assert!(wait_for_process_group_gone(process_group, REAP_DEADLINE).unwrap());
    }

    #[test]
    fn stderr_capture_retains_a_bounded_tail_without_blocking_the_writer() {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("yes x | head -c 131072 >&2")
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        let (_stop, captured, stderr) = spawn_stderr_reader(child.stderr.take().unwrap()).unwrap();
        assert!(child.wait().unwrap().success());
        let captured = captured.recv_timeout(PIPE_DRAIN_DEADLINE).unwrap();
        stderr.join().unwrap();
        assert_eq!(captured.tail.len(), STDERR_TAIL_BYTES);
        assert!(captured.truncated);
        assert!(captured.clean_eof);
    }
}
