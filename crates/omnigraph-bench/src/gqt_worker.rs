//! One-process execution endpoint for the private repetition protocol.

use std::io::{BufReader, BufWriter};
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver};

use crate::gqt_case::BoundGqt as ValidatedCase;
use crate::gqt_protocol::{
    ChildFrameV1, ParentFrameV1, WORKER_PROTOCOL_VERSION, WorkerRequestV1, WorkerStageV1,
    digest_worker_executable, read_frame, validate_protocol_version, write_frame,
};
use crate::gqt_runner::execute_gqt_rep_signaled;
use crate::runner::{MeasurementSignals, RunnerError, RunnerResult};

/// Run exactly one repetition over the private stdin/stdout worker protocol.
///
/// This is public only so the package binary can host the hidden worker
/// command. It is not a stable embedding API.
#[doc(hidden)]
pub async fn run_worker_stdio_v1() -> ExitCode {
    let input = std::io::stdin();
    let output = std::io::stdout();
    let mut input = BufReader::new(input);
    let mut output = BufWriter::new(output);

    let request = match read_request(&mut input) {
        Ok(request) => request,
        Err(error) => {
            let _ = send_failure(&mut output, WorkerStageV1::Bootstrap, &error, None);
            return ExitCode::FAILURE;
        }
    };
    let parent_frames = match spawn_parent_watch(input) {
        Ok(parent_frames) => parent_frames,
        Err(error) => {
            let error = RunnerError::new(
                "worker_watchdog_failed",
                format!("could not start parent-liveness watcher: {error}"),
            );
            let _ = send_failure(&mut output, WorkerStageV1::Bootstrap, &error, None);
            return ExitCode::FAILURE;
        }
    };
    let executable_digest = match std::env::current_exe()
        .map_err(|error| error.to_string())
        .and_then(|path| digest_worker_executable(&path).map_err(|error| error.to_string()))
    {
        Ok(digest) => digest,
        Err(message) => {
            let error = RunnerError::new(
                "worker_attestation_failed",
                format!("could not attest the running worker executable: {message}"),
            );
            let _ = send_failure(&mut output, WorkerStageV1::Bootstrap, &error, None);
            return ExitCode::FAILURE;
        }
    };
    let worker_build = match crate::runner::worker_build_attestation(executable_digest) {
        Ok(build) => build,
        Err(error) => {
            let _ = send_failure(&mut output, WorkerStageV1::Bootstrap, &error, None);
            return ExitCode::FAILURE;
        }
    };
    let mut signals = ProtocolSignals {
        parent_frames,
        output,
        request: request.clone(),
        worker_build,
    };

    let result = execute_request(&request, &mut signals).await;
    match result {
        Ok(sample) => {
            let frame = ChildFrameV1::Complete {
                protocol_version: WORKER_PROTOCOL_VERSION,
                point_id: request.expected_point_id.clone(),
                case_digest: request.expected_case_digest.clone(),
                sample: Box::new(sample),
            };
            match write_frame(&mut signals.output, &frame) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("could not send repetition completion: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        Err(error) => {
            let settled = error.context.gqt_settled_sample.clone();
            let (stage, emitted_error) = match stage_for_error(&error) {
                Some(stage) => (stage, error),
                None => (
                    WorkerStageV1::Protocol,
                    RunnerError::new(
                        "worker_stage_unclassified",
                        format!(
                            "worker error code `{}` has no declared execution-stage mapping",
                            error.code
                        ),
                    ),
                ),
            };
            if let Err(protocol_error) =
                send_failure(&mut signals.output, stage, &emitted_error, settled)
            {
                eprintln!("could not send structured worker failure: {protocol_error}");
            }
            ExitCode::FAILURE
        }
    }
}

fn read_request(input: &mut BufReader<std::io::Stdin>) -> RunnerResult<WorkerRequestV1> {
    let frame = read_frame::<_, ParentFrameV1>(input).map_err(|error| {
        RunnerError::new(
            "worker_protocol_error",
            format!("could not read worker request: {error}"),
        )
    })?;
    let Some(ParentFrameV1::Request {
        protocol_version,
        request,
    }) = frame
    else {
        return Err(RunnerError::new(
            "worker_protocol_error",
            "worker expected exactly one request frame before preparation",
        ));
    };
    validate_protocol_version(protocol_version)
        .map_err(|error| RunnerError::new("worker_protocol_error", error.to_string()))?;
    Ok(*request)
}

async fn execute_request(
    request: &WorkerRequestV1,
    signals: &mut ProtocolSignals,
) -> RunnerResult<crate::gqt_runner::GqtRepObservation> {
    crate::runner::enforce_release_build()?;
    crate::runner::validate_benchmark_child_runtime_overrides(&request.worker_scratch_root)?;
    let validated = validate_worker_case(request)?;
    execute_gqt_rep_signaled(
        request.repetition,
        &request.repetition_root,
        &request.expected_physical_digest,
        &request.expected_metadata_digest,
        &validated,
        signals,
    )
    .await
}

fn validate_worker_case(request: &WorkerRequestV1) -> RunnerResult<ValidatedCase> {
    if !request.repetition_root.is_absolute() {
        return Err(RunnerError::new(
            "worker_identity_mismatch",
            format!(
                "repetition root must be absolute: {}",
                request.repetition_root.display()
            ),
        ));
    }
    if !request.worker_scratch_root.is_absolute() {
        return Err(RunnerError::new(
            "worker_protocol_error",
            "worker scratch root must be absolute",
        ));
    }
    let expected_name = format!("worker-scratch-{:08}", request.repetition);
    if request
        .worker_scratch_root
        .file_name()
        .and_then(|name| name.to_str())
        != Some(expected_name.as_str())
    {
        return Err(RunnerError::new(
            "worker_protocol_error",
            "worker scratch root does not match the repetition identity",
        ));
    }
    let scratch_metadata =
        std::fs::symlink_metadata(&request.worker_scratch_root).map_err(|error| {
            RunnerError::new(
                "worker_protocol_error",
                format!("could not inspect worker scratch root: {error}"),
            )
        })?;
    if scratch_metadata.file_type().is_symlink() || !scratch_metadata.is_dir() {
        return Err(RunnerError::new(
            "worker_protocol_error",
            "worker scratch root must be a real directory",
        ));
    }
    let repetition_root = std::fs::canonicalize(&request.repetition_root).map_err(|error| {
        RunnerError::new(
            "worker_protocol_error",
            format!("could not resolve worker repetition root: {error}"),
        )
    })?;
    let worker_scratch_root =
        std::fs::canonicalize(&request.worker_scratch_root).map_err(|error| {
            RunnerError::new(
                "worker_protocol_error",
                format!("could not resolve worker scratch root: {error}"),
            )
        })?;
    if repetition_root.parent() != worker_scratch_root.parent() {
        return Err(RunnerError::new(
            "worker_protocol_error",
            "worker scratch root must be a sibling of the repetition store on the same verified scratch backend",
        ));
    }
    let mut entries = std::fs::read_dir(&worker_scratch_root).map_err(|error| {
        RunnerError::new(
            "worker_protocol_error",
            format!("could not inspect worker scratch contents: {error}"),
        )
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|error| {
            RunnerError::new(
                "worker_protocol_error",
                format!("could not inspect worker scratch entry: {error}"),
            )
        })?
        .is_some()
    {
        return Err(RunnerError::new(
            "worker_protocol_error",
            "worker scratch root must be empty before measurement preparation",
        ));
    }
    request
        .case
        .revalidate()
        .map_err(|e| RunnerError::new("worker_case_invalid", e))?;
    let validated = request.case.clone();
    if validated.point_id != request.expected_point_id
        || validated.plan.case_digest != request.expected_case_digest
    {
        return Err(RunnerError::new(
            "worker_identity_mismatch",
            format!(
                "worker derived point_id={} case_digest={}, expected point_id={} case_digest={}",
                validated.point_id,
                validated.plan.case_digest,
                request.expected_point_id,
                request.expected_case_digest
            ),
        ));
    }
    Ok(validated)
}

struct ProtocolSignals {
    parent_frames: Receiver<Result<ParentFrameV1, String>>,
    output: BufWriter<std::io::Stdout>,
    request: WorkerRequestV1,
    worker_build: crate::gqt_protocol::WorkerBuildV1,
}

impl MeasurementSignals for ProtocolSignals {
    fn ready(&mut self) -> RunnerResult<()> {
        let machine = crate::machine::capture_machine_identity().map_err(|error| {
            RunnerError::new(
                "machine_identity_capture_failed",
                format!("could not capture repetition-worker machine identity: {error}"),
            )
        })?;
        write_frame(
            &mut self.output,
            &ChildFrameV1::Ready {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: self.request.repetition,
                point_id: self.request.expected_point_id.clone(),
                case_digest: self.request.expected_case_digest.clone(),
                worker_build: Box::new(self.worker_build.clone()),
                machine: Box::new(machine),
                physical_digest: self.request.expected_physical_digest.clone(),
                metadata_digest: self.request.expected_metadata_digest.clone(),
            },
        )
        .map_err(|error| RunnerError::new("worker_protocol_error", error.to_string()))?;

        let begin = self
            .parent_frames
            .recv()
            .map_err(|_| {
                RunnerError::new(
                    "worker_parent_disconnected",
                    "parent-liveness watcher stopped before measurement began",
                )
            })?
            .map_err(|error| RunnerError::new("worker_protocol_error", error))?;
        match begin {
            ParentFrameV1::Begin {
                protocol_version,
                repetition,
            } if protocol_version == WORKER_PROTOCOL_VERSION
                && repetition == self.request.repetition => {}
            frame => {
                return Err(RunnerError::new(
                    "worker_protocol_error",
                    format!(
                        "worker expected begin-v{} for repetition {}, got {frame:?}",
                        WORKER_PROTOCOL_VERSION, self.request.repetition
                    ),
                ));
            }
        }
        Ok(())
    }

    fn settled(&mut self, elapsed_us: u64) -> RunnerResult<()> {
        write_frame(
            &mut self.output,
            &ChildFrameV1::Settled {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: self.request.repetition,
                elapsed_us,
            },
        )
        .map_err(|error| RunnerError::new("worker_protocol_error", error.to_string()))
    }
}

/// Parent EOF terminates the worker during preparation, measurement, or verification.
fn spawn_parent_watch(
    mut input: BufReader<std::io::Stdin>,
) -> std::io::Result<Receiver<Result<ParentFrameV1, String>>> {
    let (send, receive) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("omnigraph-bench-parent-watch".to_string())
        .spawn(move || {
            match read_frame::<_, ParentFrameV1>(&mut input) {
                Ok(Some(frame)) => {
                    if send.send(Ok(frame)).is_err() {
                        return;
                    }
                }
                Ok(None) => std::process::exit(125),
                Err(error) => {
                    let _ = send.send(Err(error.to_string()));
                    return;
                }
            }

            match read_frame::<_, ParentFrameV1>(&mut input) {
                Ok(None) => std::process::exit(125),
                Ok(Some(_)) | Err(_) => std::process::exit(126),
            }
        })?;
    Ok(receive)
}

fn stage_for_error(error: &RunnerError) -> Option<WorkerStageV1> {
    if error.code.starts_with("gqt_") {
        return Some(if error.code == "gqt_verification_failed" {
            WorkerStageV1::Verify
        } else if error.code == "gqt_measure_failed" {
            WorkerStageV1::Measure
        } else {
            WorkerStageV1::Prepare
        });
    }
    Some(match error.code.as_str() {
        "release_build_required"
        | "worker_attestation_failed"
        | "worker_build_attestation_invalid"
        | "worker_case_invalid"
        | "worker_identity_mismatch"
        | "invalid_lance_mem_pool_size"
        | "unsupported_runtime_override"
        | "build_attestation_environment_mismatch"
        | "unsupported_runner_axis" => WorkerStageV1::Bootstrap,
        "pre_measurement_write_detected"
        | "pre_measurement_shape_mismatch"
        | "cache_preparation_failed"
        | "protected_head_capture_failed"
        | "unsupported_cache_condition"
        | "machine_identity_capture_failed"
        | "engine_open_failed"
        | "storage_open_failed"
        | "non_utf8_path" => WorkerStageV1::Prepare,
        "merge_failed" | "merge_deadline_exceeded" | "duration_overflow" | "counter_regression" => {
            WorkerStageV1::Measure
        }
        "verification_failed"
        | "vacuous_merge"
        | "missing_table_walk_phase"
        | "interval_overflow" => WorkerStageV1::Verify,
        "worker_protocol_error" | "worker_parent_disconnected" => WorkerStageV1::Protocol,
        _ => return None,
    })
}

fn send_failure(
    output: &mut BufWriter<std::io::Stdout>,
    stage: WorkerStageV1,
    error: &RunnerError,
    settled_sample: Option<Box<crate::gqt_runner::GqtRepObservation>>,
) -> Result<(), crate::gqt_protocol::WorkerProtocolError> {
    write_frame(
        output,
        &ChildFrameV1::Failed {
            protocol_version: WORKER_PROTOCOL_VERSION,
            stage,
            code: error.code.clone(),
            message: error.message.clone(),
            settled_sample,
        },
    )
}
