//! One-process execution endpoint for the private repetition protocol.

use std::io::{BufReader, BufWriter};
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use crate::branch_merge::BranchMergePlan;
use crate::runner::{MeasurementSignals, RunnerError, RunnerResult, execute_rep_signaled};
use crate::worker_protocol::{
    ChildFrameV1, ParentFrameV1, WORKER_PROTOCOL_VERSION, WorkerRequestV1, WorkerStageV1,
    digest_worker_executable, read_frame, validate_protocol_version, write_frame,
};
use crate::{ValidatedCase, validate_case};

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
    let worker_build = match std::env::current_exe()
        .map_err(|error| error.to_string())
        .and_then(|path| digest_worker_executable(&path).map_err(|error| error.to_string()))
    {
        Ok(digest) => crate::runner::worker_build_attestation(digest),
        Err(message) => {
            let error = RunnerError::new(
                "worker_attestation_failed",
                format!("could not attest the running worker executable: {message}"),
            );
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
            let stage = stage_for_error(&error);
            let settled = error.context.settled_sample.clone();
            if let Err(protocol_error) = send_failure(&mut signals.output, stage, &error, settled) {
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
) -> RunnerResult<crate::runner::RepObservation> {
    crate::runner::enforce_release_build()?;
    let validated = validate_worker_case(request)?;
    let plan = BranchMergePlan::try_from(&validated)
        .map_err(|error| RunnerError::new("unsupported_runner_axis", error.to_string()))?;
    let deadline = validated
        .definition
        .protocol
        .deadline_seconds
        .map(Duration::from_secs);
    execute_rep_signaled(
        request.repetition,
        &request.repetition_root,
        &request.expected_physical_digest,
        &request.expected_metadata_digest,
        &plan,
        &validated.definition.environment.cache_condition,
        deadline,
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
    let validated = validate_case(request.case.clone())
        .into_result()
        .map_err(|diagnostics| {
            RunnerError::new(
                "worker_case_invalid",
                diagnostics
                    .into_iter()
                    .map(|diagnostic| {
                        format!(
                            "{} at {}: {}",
                            diagnostic.code, diagnostic.path, diagnostic.message
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })?;
    if validated.point_id != request.expected_point_id
        || validated.case_digest != request.expected_case_digest
    {
        return Err(RunnerError::new(
            "worker_identity_mismatch",
            format!(
                "worker derived point_id={} case_digest={}, expected point_id={} case_digest={}",
                validated.point_id,
                validated.case_digest,
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
    worker_build: crate::worker_protocol::WorkerBuildV1,
}

impl MeasurementSignals for ProtocolSignals {
    fn ready(&mut self) -> RunnerResult<()> {
        write_frame(
            &mut self.output,
            &ChildFrameV1::Ready {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: self.request.repetition,
                point_id: self.request.expected_point_id.clone(),
                case_digest: self.request.expected_case_digest.clone(),
                worker_build: self.worker_build.clone(),
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

/// Own stdin from immediately after the immutable request is decoded.
///
/// The thread waits for the one `Begin` frame while fixture preparation runs,
/// so parent EOF terminates the process even if open or cache preparation hangs. After it
/// forwards that frame it accepts no more protocol input and keeps watching
/// the pipe until the worker process exits normally.
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
                // Parent EOF before normal process exit means the supervisor
                // can no longer contain or observe an in-flight mutation.
                Ok(None) => std::process::exit(125),
                // No frame is valid after Begin. Fail immediately even if the
                // measured task is currently blocked.
                Ok(Some(_)) | Err(_) => std::process::exit(126),
            }
        })?;
    Ok(receive)
}

fn stage_for_error(error: &RunnerError) -> WorkerStageV1 {
    match error.code.as_str() {
        "release_build_required"
        | "worker_attestation_failed"
        | "worker_case_invalid"
        | "worker_identity_mismatch"
        | "unsupported_runner_axis" => WorkerStageV1::Bootstrap,
        "pre_measurement_write_detected"
        | "pre_measurement_shape_mismatch"
        | "cache_preparation_failed"
        | "protected_head_capture_failed"
        | "unsupported_cache_condition"
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
        _ => WorkerStageV1::Finalize,
    }
}

fn send_failure(
    output: &mut BufWriter<std::io::Stdout>,
    stage: WorkerStageV1,
    error: &RunnerError,
    settled_sample: Option<Box<crate::runner::RepObservation>>,
) -> Result<(), crate::worker_protocol::WorkerProtocolError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_ready_protected_head_failure_is_classified_as_prepare() {
        let error = RunnerError::new("protected_head_capture_failed", "capture failed");
        assert_eq!(stage_for_error(&error), WorkerStageV1::Prepare);
    }

    #[test]
    fn reachable_errors_are_classified_at_their_owning_stage() {
        for (code, expected) in [
            ("unsupported_runner_axis", WorkerStageV1::Bootstrap),
            ("non_utf8_path", WorkerStageV1::Prepare),
            ("counter_regression", WorkerStageV1::Measure),
            ("interval_overflow", WorkerStageV1::Verify),
        ] {
            let error = RunnerError::new(code, "test failure");
            assert_eq!(stage_for_error(&error), expected, "{code}");
        }
    }

    #[test]
    fn worker_refuses_identity_mismatch_before_opening_a_store() {
        let case: crate::CaseV1 = serde_yaml::from_str(
            r#"
version: 1
id: worker-identity-test
scenario: branch-merge-v1
fixture:
  builder: { kind: synthetic-branch-merge, version: 2, seed: 0 }
  data: { provenance: synthetic, tables: 2, rows_per_table: 12, payload_bytes: 8, column_shape: scalars, topology_skew: uniform }
  state: { aging: bulk-loaded, indexes: [], deletion_history: none, compaction_recency: not-optimized, history_depth: 6 }
workload: { delta_rows_per_side: 6, diverged_tables: 1, arrival: unscheduled-single-shot, clients: 1, read_write_mix: write-heavy, contention: distinct-key }
environment:
  backend: { kind: local-fs, filesystem: apfs, storage_class: nvme-ssd }
  network_position: same-host
  execution: embedded
  cache_condition: { process: fresh-per-repetition, engine: warmed-by-program, page_cache: program-conditioned, program: branch-merge-read-set-v1, iterations: 1 }
protocol: { deadline_seconds: 60, attribution: per-phase, schedule: manual, reset: local-clonefile, timer: monotonic }
"#,
        )
        .unwrap();
        let request = WorkerRequestV1 {
            repetition: 0,
            case,
            expected_point_id: "0".repeat(64),
            expected_case_digest: "1".repeat(64),
            repetition_root: std::path::PathBuf::from("/tmp/does-not-open"),
            expected_physical_digest: crate::reset::PhysicalDigest {
                files: 0,
                bytes: 0,
                digest_sha256: "2".repeat(64),
            },
            expected_metadata_digest: crate::reset::MetadataDigest {
                entries: 0,
                files: 0,
                directories: 0,
                bytes: 0,
                shape_sha256: "3".repeat(64),
                state_sha256: "4".repeat(64),
            },
        };

        let error = validate_worker_case(&request).unwrap_err();
        assert_eq!(error.code, "worker_identity_mismatch");
    }
}
