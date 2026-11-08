//! Private, versioned protocol between the benchmark supervisor and one
//! repetition worker process.
//!
//! Standard output is reserved exclusively for these frames. Human diagnostics
//! belong on standard error so a supervisor can reject malformed or unexpected
//! output rather than guessing where one message ends.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Seek, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::gqt_case::BoundGqt as CaseV1;
pub use crate::gqt_evidence::{PreparationProofV2, RepetitionInputV2};
use crate::gqt_runner::GqtRepObservation as RepObservation;
use crate::machine::MachineIdentityV1;
use crate::runner::EffectiveEnvironmentValue;
/// Attested build facts reported by an honest worker from its own process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBuildV1 {
    pub source_commit: String,
    /// `None` is an explicit failed build-time probe and is never admissible
    /// for a measured worker. Keeping the absence typed lets the parent fail
    /// closed instead of accepting a fabricated clean/dirty value.
    pub source_tree_dirty: Option<bool>,
    pub cargo_profile: String,
    /// Cargo's profile-level observation, not proof against direct target
    /// rustc arguments.
    pub cargo_opt_level: String,
    pub debug_assertions: bool,
    /// Effective Lance memory-pool setting inherited by the measured worker.
    /// This is retained until the complete typed engine-environment registry
    /// below replaces the one-setting representation.
    pub effective_lance_mem_pool_size: Box<EffectiveEnvironmentValue>,
    pub target_triple: String,
    pub rustc_version: String,
    pub declared_release_lto: String,
    pub declared_release_codegen_units: Option<u32>,
    pub declared_release_strip: Option<bool>,
    pub cargo_encoded_rustflags_present: Option<bool>,
    pub release_profile_environment_overrides_supported: Option<bool>,
    /// False until a controlled build wrapper supplies the final target rustc
    /// command line in a digest-bound external receipt.
    pub effective_codegen_options_proved: bool,
    /// Canonical Cargo feature names reported by the linked
    /// `omnigraph-engine` artifact itself, not inferred from this CLI crate.
    pub engine_feature_flags: Vec<String>,
    /// Canonical non-Cargo engine techniques enabled for this execution.
    /// Runner-v1 has no such controls and therefore admits only an empty set.
    pub enabled_techniques: Vec<String>,
    pub executable_sha256: String,
}

/// The only worker protocol understood by this build.
pub const WORKER_PROTOCOL_VERSION: u32 = 2;

/// Maximum compact JSON payload bytes in one frame, excluding its newline.
///
/// Case documents are independently bounded well below this value. Keeping a
/// framing bound here also prevents corrupt or unexpected worker output from
/// growing the supervisor's memory without limit.
pub const MAX_WORKER_FRAME_BYTES: usize = 1024 * 1024;
const MAX_WORKER_EXECUTABLE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const EXECUTABLE_DIGEST_BUFFER_BYTES: usize = 1024 * 1024;

/// Complete, immutable input for one worker process.
///
/// The worker revalidates `case` and compares the derived identities with the
/// expected values. It must not reload a case file that could change between
/// parent planning and repetition execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRequestV2 {
    pub repetition: u32,
    pub case: CaseV1,
    pub expected_point_id: String,
    pub expected_case_digest: String,
    pub execution: RepetitionInputV2,
    /// Harness-owned empty sibling directory on the verified scratch backend.
    /// Both generic process-temporary spill and OmniGraph merge staging must
    /// resolve through this exact protocol field.
    pub worker_scratch_root: PathBuf,
}

/// Frames sent by the supervising parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ParentFrameV2 {
    /// Supplies the complete repetition input. This is always the first frame.
    Request {
        protocol_version: u32,
        request: Box<WorkerRequestV2>,
    },
    /// Releases a prepared worker into the selected operation.
    Begin {
        protocol_version: u32,
        repetition: u32,
    },
}

impl ParentFrameV2 {
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
pub enum ChildFrameV2 {
    /// Open, cache preparation, identity, and pre-measurement checks completed.
    Ready {
        protocol_version: u32,
        repetition: u32,
        point_id: String,
        case_digest: String,
        worker_build: Box<WorkerBuildV1>,
        /// Process-effective identity captured by this child immediately
        /// before it declared itself ready for measurement.
        machine: Box<MachineIdentityV1>,
        proof: PreparationProofV2,
    },
    /// The operation returned; subsequent assertions cannot extend its clock.
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
    /// Failure evidence can retain a closed operation clock without admitting a successful sample.
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
    open_and_digest_worker_executable(path).map(|(_file, _bytes, digest)| digest)
}

/// Open and SHA-256 one bounded regular worker executable through the same
/// file descriptor. Callers that must survive a later path replacement can
/// retain the returned descriptor and stage those exact bytes elsewhere.
pub(crate) fn open_and_digest_worker_executable(path: &Path) -> io::Result<(File, u64, String)> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(nix::libc::O_NONBLOCK | nix::libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    let metadata = file.metadata()?;
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
    file.rewind()?;
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
    file.rewind()?;
    Ok((file, observed, format!("{:x}", digest.finalize())))
}

impl ChildFrameV2 {
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

/// Reject a frame from any worker protocol version other than this build's.
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
