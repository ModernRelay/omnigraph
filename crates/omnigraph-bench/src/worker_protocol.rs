//! Private, versioned protocol between the benchmark supervisor and one
//! repetition worker process.
//!
//! Standard output is reserved exclusively for these frames. Human diagnostics
//! belong on standard error so a supervisor can reject malformed or unexpected
//! output rather than guessing where one message ends.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::case::CaseV1;
use crate::reset::{MetadataDigest, PhysicalDigest};
use crate::runner::RepObservation;

/// The only worker protocol understood by this build.
pub const WORKER_PROTOCOL_VERSION: u32 = 1;

/// Maximum compact JSON payload bytes in one frame, excluding its newline.
///
/// Case documents are independently bounded well below this value. Keeping a
/// framing bound here also prevents corrupt or unexpected worker output from
/// growing the supervisor's memory without limit.
pub const MAX_WORKER_FRAME_BYTES: usize = 1024 * 1024;
const MAX_WORKER_EXECUTABLE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const EXECUTABLE_DIGEST_BUFFER_BYTES: usize = 1024 * 1024;

/// Attested build facts reported by an honest worker from its own process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBuildV1 {
    pub cargo_profile: String,
    pub opt_level: String,
    pub debug_assertions: bool,
    pub executable_sha256: String,
}

/// Complete, immutable input for one worker process.
///
/// The worker revalidates `case` and compares the derived identities with the
/// expected values. It must not reload a case file that could change between
/// parent planning and repetition execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRequestV1 {
    pub repetition: u32,
    pub case: CaseV1,
    pub expected_point_id: String,
    pub expected_case_digest: String,
    pub repetition_root: PathBuf,
    pub expected_physical_digest: PhysicalDigest,
    pub expected_metadata_digest: MetadataDigest,
}

/// Frames sent by the supervising parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ParentFrameV1 {
    /// Supplies the complete repetition input. This is always the first frame.
    Request {
        protocol_version: u32,
        request: Box<WorkerRequestV1>,
    },
    /// Releases a prepared worker into the measured mutation.
    Begin {
        protocol_version: u32,
        repetition: u32,
    },
}

impl ParentFrameV1 {
    pub fn protocol_version(&self) -> u32 {
        match self {
            Self::Request {
                protocol_version, ..
            }
            | Self::Begin {
                protocol_version, ..
            } => *protocol_version,
        }
    }
}

/// Stable worker stage attached to a structured failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerStageV1 {
    Bootstrap,
    Prepare,
    Measure,
    Verify,
    Finalize,
    Protocol,
}

/// Frames sent by one repetition worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ChildFrameV1 {
    /// Open, cache preparation, identity, and pre-measurement checks completed.
    Ready {
        protocol_version: u32,
        repetition: u32,
        point_id: String,
        case_digest: String,
        worker_build: WorkerBuildV1,
        physical_digest: PhysicalDigest,
        metadata_digest: MetadataDigest,
    },
    /// The mutating future returned and its elapsed clock is closed.
    ///
    /// Exact content verification happens after this frame and therefore does
    /// not extend the declared measured-operation deadline.
    Settled {
        protocol_version: u32,
        repetition: u32,
        elapsed_us: u64,
    },
    /// Verification completed and the worker produced an admissible sample.
    Complete {
        protocol_version: u32,
        point_id: String,
        case_digest: String,
        sample: Box<RepObservation>,
    },
    /// The worker failed at a named stage.
    ///
    /// `settled_sample` is present only when the mutation returned and the
    /// worker subsequently completed enough verification to construct the
    /// full sample. Partial or killed mutations never become sample rows.
    Failed {
        protocol_version: u32,
        stage: WorkerStageV1,
        code: String,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        settled_sample: Option<Box<RepObservation>>,
    },
}

/// SHA-256 one bounded regular worker executable.
pub fn digest_worker_executable(path: &Path) -> io::Result<String> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "worker executable is not a non-empty regular file: {}",
                path.display()
            ),
        ));
    }
    if metadata.len() > MAX_WORKER_EXECUTABLE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "worker executable has {} bytes; the limit is {MAX_WORKER_EXECUTABLE_BYTES}",
                metadata.len()
            ),
        ));
    }
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; EXECUTABLE_DIGEST_BUFFER_BYTES];
    let mut observed = 0_u64;
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker executable read overflow",
                )
            })?)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker executable size overflow",
                )
            })?;
        if observed > MAX_WORKER_EXECUTABLE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "worker executable grew beyond its digest bound while reading",
            ));
        }
        digest.update(&buffer[..read]);
    }
    if observed != metadata.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "worker executable changed length while reading: metadata={} observed={observed}",
                metadata.len()
            ),
        ));
    }
    Ok(format!("{:x}", digest.finalize()))
}

impl ChildFrameV1 {
    pub fn protocol_version(&self) -> u32 {
        match self {
            Self::Ready {
                protocol_version, ..
            }
            | Self::Settled {
                protocol_version, ..
            }
            | Self::Complete {
                protocol_version, ..
            }
            | Self::Failed {
                protocol_version, ..
            } => *protocol_version,
        }
    }
}

#[derive(Debug)]
pub enum WorkerProtocolError {
    Io(io::Error),
    Encode(serde_json::Error),
    Decode(serde_json::Error),
    EmptyFrame,
    UnterminatedFrame { bytes: usize },
    FrameTooLarge { observed: usize, limit: usize },
    UnsupportedVersion { expected: u32, observed: u32 },
}

impl Display for WorkerProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "worker protocol I/O failed: {error}"),
            Self::Encode(error) => write!(formatter, "could not encode worker frame: {error}"),
            Self::Decode(error) => write!(formatter, "could not decode worker frame: {error}"),
            Self::EmptyFrame => write!(formatter, "worker protocol contained an empty frame"),
            Self::UnterminatedFrame { bytes } => write!(
                formatter,
                "worker protocol reached EOF after {bytes} unterminated frame bytes"
            ),
            Self::FrameTooLarge { observed, limit } => write!(
                formatter,
                "worker protocol frame has at least {observed} bytes; the limit is {limit}"
            ),
            Self::UnsupportedVersion { expected, observed } => write!(
                formatter,
                "unsupported worker protocol version {observed}; this build supports {expected}"
            ),
        }
    }
}

impl Error for WorkerProtocolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Encode(error) | Self::Decode(error) => Some(error),
            Self::EmptyFrame
            | Self::UnterminatedFrame { .. }
            | Self::FrameTooLarge { .. }
            | Self::UnsupportedVersion { .. } => None,
        }
    }
}

/// Encode and flush one compact NDJSON frame.
pub fn write_frame<W, T>(writer: &mut W, frame: &T) -> Result<(), WorkerProtocolError>
where
    W: Write,
    T: Serialize,
{
    let encoded = serde_json::to_vec(frame).map_err(WorkerProtocolError::Encode)?;
    if encoded.len() > MAX_WORKER_FRAME_BYTES {
        return Err(WorkerProtocolError::FrameTooLarge {
            observed: encoded.len(),
            limit: MAX_WORKER_FRAME_BYTES,
        });
    }
    writer
        .write_all(&encoded)
        .and_then(|()| writer.write_all(b"\n"))
        .and_then(|()| writer.flush())
        .map_err(WorkerProtocolError::Io)
}

/// Decode one bounded NDJSON frame.
///
/// Clean EOF between frames returns `Ok(None)`. EOF after any bytes, an empty
/// line, malformed JSON, and an oversized line are distinct protocol errors.
/// The caller owns frame ordering and must reject a valid frame in an invalid
/// state.
pub fn read_frame<R, T>(reader: &mut R) -> Result<Option<T>, WorkerProtocolError>
where
    R: BufRead,
    T: DeserializeOwned,
{
    let mut encoded = Vec::new();
    loop {
        let available = reader.fill_buf().map_err(WorkerProtocolError::Io)?;
        if available.is_empty() {
            return if encoded.is_empty() {
                Ok(None)
            } else {
                Err(WorkerProtocolError::UnterminatedFrame {
                    bytes: encoded.len(),
                })
            };
        }

        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            let observed = encoded.len().saturating_add(newline);
            if observed > MAX_WORKER_FRAME_BYTES {
                return Err(WorkerProtocolError::FrameTooLarge {
                    observed,
                    limit: MAX_WORKER_FRAME_BYTES,
                });
            }
            encoded.extend_from_slice(&available[..newline]);
            reader.consume(newline + 1);
            if encoded.is_empty() {
                return Err(WorkerProtocolError::EmptyFrame);
            }
            return serde_json::from_slice(&encoded)
                .map(Some)
                .map_err(WorkerProtocolError::Decode);
        }

        let chunk_len = available.len();
        let observed = encoded.len().saturating_add(chunk_len);
        if observed > MAX_WORKER_FRAME_BYTES {
            return Err(WorkerProtocolError::FrameTooLarge {
                observed,
                limit: MAX_WORKER_FRAME_BYTES,
            });
        }
        encoded.extend_from_slice(available);
        reader.consume(chunk_len);
    }
}

/// Reject a frame from any worker protocol version other than V1.
pub fn validate_protocol_version(observed: u32) -> Result<(), WorkerProtocolError> {
    if observed == WORKER_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(WorkerProtocolError::UnsupportedVersion {
            expected: WORKER_PROTOCOL_VERSION,
            observed,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor, Write};

    use crate::counting::LogicalCallCounts;
    use crate::runner::{
        ControlCallObservation, LogicalStoreCallObservation, MergeRouteObservation,
        PhaseObservation, VerificationObservation,
    };
    use crate::validate_case;

    use super::*;

    fn case() -> CaseV1 {
        serde_yaml::from_str(
            r#"
version: 1
id: worker-protocol-test
scenario: branch-merge-v1
fixture:
  builder: { kind: synthetic-branch-merge, version: 1, seed: 0 }
  data:
    provenance: synthetic
    tables: 1
    rows_per_table: 12
    payload_bytes: 8
    column_shape: scalars
    topology_skew: uniform
  state:
    aging: bulk-loaded
    indexes: []
    deletion_history: none
    compaction_recency: not-optimized
    history_depth: 5
workload:
  delta_rows_per_side: 6
  diverged_tables: 1
  arrival: unscheduled-single-shot
  clients: 1
  read_write_mix: write-heavy
  contention: distinct-key
environment:
  backend: { kind: local-fs, filesystem: apfs, storage_class: nvme-ssd }
  network_position: same-host
  execution: embedded
  cache_condition: { process: fresh-per-repetition, engine: warmed-by-program, page_cache: program-conditioned, program: branch-merge-read-set-v1, iterations: 1 }
protocol:
  deadline_seconds: 60
  attribution: per-phase
  schedule: manual
  reset: local-clonefile
  timer: monotonic
"#,
        )
        .unwrap()
    }

    fn physical_digest() -> PhysicalDigest {
        PhysicalDigest {
            files: 11,
            bytes: 12_345,
            digest_sha256: "a".repeat(64),
        }
    }

    fn metadata_digest() -> MetadataDigest {
        MetadataDigest {
            entries: 13,
            files: 11,
            directories: 2,
            bytes: 12_345,
            shape_sha256: "b".repeat(64),
            state_sha256: "c".repeat(64),
        }
    }

    fn sample() -> RepObservation {
        RepObservation {
            repetition: 3,
            input_physical_digest_sha256: "a".repeat(64),
            elapsed_us: 42,
            outcome: "merged".to_string(),
            phases: vec![PhaseObservation {
                phase: "TableWalk".to_string(),
                total_us: 20,
                max_us: 20,
                interval_count: 1,
            }],
            route: MergeRouteObservation {
                table_walk_intervals: 1,
                stage_merge_insert_calls: 1,
                stage_merge_insert_rows: 2,
                stage_known_present_update_calls: 1,
                stage_known_present_update_rows: 2,
                stage_fenced_insert_calls: 0,
                stage_fenced_insert_rows: 0,
                strict_insert_preflight_calls: 0,
            },
            logical_store_calls: LogicalStoreCallObservation {
                manifest: LogicalCallCounts {
                    get: 1,
                    ..Default::default()
                },
                table: LogicalCallCounts {
                    put: 2,
                    ..Default::default()
                },
                physical_attempts_observed: false,
            },
            control_store_calls: ControlCallObservation {
                read_text: 1,
                read_text_if_exists: 2,
                read_text_versioned: 3,
                exists: 4,
                list_dir: 5,
                mutation_calls: 6,
                write_text: 7,
                delete: 8,
            },
            verification: VerificationObservation {
                branch: "bench-target".to_string(),
                tables: 1,
                rows: 12,
                exact_content: true,
                source_exact_content: true,
                main_exact_content: true,
                protected_heads_unchanged: true,
            },
        }
    }

    fn round_trip<T>(frame: &T) -> T
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, frame).unwrap();
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
        let mut reader = BufReader::new(Cursor::new(bytes));
        let decoded = read_frame(&mut reader).unwrap().unwrap();
        assert_eq!(read_frame::<_, T>(&mut reader).unwrap(), None);
        decoded
    }

    #[derive(Default)]
    struct FlushWitness {
        bytes: Vec<u8>,
        flushes: usize,
    }

    impl Write for FlushWitness {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn parent_frames_round_trip_with_complete_immutable_request() {
        let case = case();
        let validated = validate_case(case.clone()).into_result().unwrap();
        let request = ParentFrameV1::Request {
            protocol_version: WORKER_PROTOCOL_VERSION,
            request: Box::new(WorkerRequestV1 {
                repetition: 3,
                case,
                expected_point_id: validated.point_id,
                expected_case_digest: validated.case_digest,
                repetition_root: PathBuf::from("/tmp/worker-repetition"),
                expected_physical_digest: physical_digest(),
                expected_metadata_digest: metadata_digest(),
            }),
        };
        assert_eq!(round_trip(&request), request);

        let begin = ParentFrameV1::Begin {
            protocol_version: WORKER_PROTOCOL_VERSION,
            repetition: 3,
        };
        assert_eq!(round_trip(&begin), begin);
        assert_eq!(request.protocol_version(), WORKER_PROTOCOL_VERSION);
        assert_eq!(begin.protocol_version(), WORKER_PROTOCOL_VERSION);
    }

    #[test]
    fn every_child_frame_round_trips() {
        let frames = [
            ChildFrameV1::Ready {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: 3,
                point_id: "d".repeat(64),
                case_digest: "e".repeat(64),
                worker_build: WorkerBuildV1 {
                    cargo_profile: "release".to_string(),
                    opt_level: "2".to_string(),
                    debug_assertions: false,
                    executable_sha256: "f".repeat(64),
                },
                physical_digest: physical_digest(),
                metadata_digest: metadata_digest(),
            },
            ChildFrameV1::Settled {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: 3,
                elapsed_us: 42,
            },
            ChildFrameV1::Complete {
                protocol_version: WORKER_PROTOCOL_VERSION,
                point_id: "d".repeat(64),
                case_digest: "e".repeat(64),
                sample: Box::new(sample()),
            },
            ChildFrameV1::Failed {
                protocol_version: WORKER_PROTOCOL_VERSION,
                stage: WorkerStageV1::Verify,
                code: "verification_failed".to_string(),
                message: "target contents differed".to_string(),
                settled_sample: Some(Box::new(sample())),
            },
        ];

        for frame in frames {
            assert_eq!(round_trip(&frame), frame);
            assert_eq!(frame.protocol_version(), WORKER_PROTOCOL_VERSION);
        }
    }

    #[test]
    fn multiple_frames_do_not_consume_each_other() {
        let first = ParentFrameV1::Begin {
            protocol_version: WORKER_PROTOCOL_VERSION,
            repetition: 1,
        };
        let second = ParentFrameV1::Begin {
            protocol_version: WORKER_PROTOCOL_VERSION,
            repetition: 2,
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &first).unwrap();
        write_frame(&mut bytes, &second).unwrap();

        let mut reader = BufReader::with_capacity(7, Cursor::new(bytes));
        assert_eq!(read_frame(&mut reader).unwrap(), Some(first));
        assert_eq!(read_frame(&mut reader).unwrap(), Some(second));
        assert_eq!(read_frame::<_, ParentFrameV1>(&mut reader).unwrap(), None);
    }

    #[test]
    fn every_written_frame_is_newline_delimited_and_flushed() {
        let frame = ParentFrameV1::Begin {
            protocol_version: WORKER_PROTOCOL_VERSION,
            repetition: 1,
        };
        let mut witness = FlushWitness::default();
        write_frame(&mut witness, &frame).unwrap();

        assert_eq!(witness.flushes, 1);
        assert_eq!(witness.bytes.last(), Some(&b'\n'));
        assert!(!witness.bytes[..witness.bytes.len() - 1].contains(&b'\n'));
    }

    #[test]
    fn writer_rejects_oversized_frames_before_writing() {
        let frame = ChildFrameV1::Failed {
            protocol_version: WORKER_PROTOCOL_VERSION,
            stage: WorkerStageV1::Protocol,
            code: "oversized".to_string(),
            message: "x".repeat(MAX_WORKER_FRAME_BYTES),
            settled_sample: None,
        };
        let mut bytes = Vec::new();
        let error = write_frame(&mut bytes, &frame).unwrap_err();
        assert!(matches!(error, WorkerProtocolError::FrameTooLarge { .. }));
        assert!(bytes.is_empty());
    }

    #[test]
    fn reader_bounds_an_unterminated_or_delimited_oversized_frame() {
        for suffix in [b"".as_slice(), b"\n".as_slice()] {
            let mut bytes = vec![b'x'; MAX_WORKER_FRAME_BYTES + 1];
            bytes.extend_from_slice(suffix);
            let mut reader = BufReader::with_capacity(1024, Cursor::new(bytes));
            let error = read_frame::<_, ParentFrameV1>(&mut reader).unwrap_err();
            assert!(matches!(error, WorkerProtocolError::FrameTooLarge { .. }));
        }
    }

    #[test]
    fn clean_eof_empty_and_unterminated_frames_are_distinct() {
        let mut clean = BufReader::new(Cursor::new(Vec::<u8>::new()));
        assert_eq!(read_frame::<_, ParentFrameV1>(&mut clean).unwrap(), None);

        let mut empty = BufReader::new(Cursor::new(b"\n".to_vec()));
        assert!(matches!(
            read_frame::<_, ParentFrameV1>(&mut empty).unwrap_err(),
            WorkerProtocolError::EmptyFrame
        ));

        let mut unterminated = BufReader::new(Cursor::new(b"{}".to_vec()));
        assert!(matches!(
            read_frame::<_, ParentFrameV1>(&mut unterminated).unwrap_err(),
            WorkerProtocolError::UnterminatedFrame { bytes: 2 }
        ));
    }

    #[test]
    fn malformed_or_extended_frames_fail_closed() {
        let mut malformed = BufReader::new(Cursor::new(b"not-json\n".to_vec()));
        assert!(matches!(
            read_frame::<_, ParentFrameV1>(&mut malformed).unwrap_err(),
            WorkerProtocolError::Decode(_)
        ));

        let source = format!(
            r#"{{"frame":"begin","protocol_version":{},"repetition":1,"extra":true}}"#,
            WORKER_PROTOCOL_VERSION
        );
        let mut source = source.into_bytes();
        source.push(b'\n');
        let mut extended = BufReader::new(Cursor::new(source));
        assert!(matches!(
            read_frame::<_, ParentFrameV1>(&mut extended).unwrap_err(),
            WorkerProtocolError::Decode(_)
        ));
    }

    #[test]
    fn unsupported_versions_are_typed_errors() {
        validate_protocol_version(WORKER_PROTOCOL_VERSION).unwrap();
        assert!(matches!(
            validate_protocol_version(WORKER_PROTOCOL_VERSION + 1).unwrap_err(),
            WorkerProtocolError::UnsupportedVersion {
                expected: WORKER_PROTOCOL_VERSION,
                observed: 2
            }
        ));
    }
}
