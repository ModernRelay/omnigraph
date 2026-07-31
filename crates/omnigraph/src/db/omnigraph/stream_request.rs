#![allow(dead_code)] // Hidden F4 request vocabulary; production transport activation is intentionally deferred.

//! Private, transport-neutral request mechanics for RFC-026 F4.
//!
//! This module deliberately owns no HTTP, CLI, or public DTO shape. It provides
//! only the bounded pieces shared by a future transport adapter: incremental
//! NDJSON framing, per-line result classification, request admission, and
//! caller-order result buffering.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::db::manifest::stream_token::{StreamRowOrigin, StreamTerminalCorrection};
use crate::error::{OmniError, Result};

pub(super) const STREAM_NDJSON_MAX_LINE_BYTES: usize = 32 * 1024 * 1024;
pub(super) const STREAM_NDJSON_MAX_CHUNK_BYTES: usize = 32 * 1024 * 1024;
pub(super) const STREAM_REQUEST_MAX_RESULT_STATUSES: usize = 8_192;

const STREAM_REQUEST_RESERVATION_BYTES: u64 = 128 * 1024 * 1024;
const STREAM_REQUEST_ROOT_MAX_REQUESTS: usize = 2;
const STREAM_REQUEST_ROOT_MAX_BYTES: u64 =
    STREAM_REQUEST_RESERVATION_BYTES * STREAM_REQUEST_ROOT_MAX_REQUESTS as u64;
const STREAM_REQUEST_ACTOR_MAX_REQUESTS: usize = 1;
const STREAM_REQUEST_ACTOR_MAX_BYTES: u64 =
    STREAM_REQUEST_RESERVATION_BYTES * STREAM_REQUEST_ACTOR_MAX_REQUESTS as u64;

/// One completed raw NDJSON line.
///
/// Invalid UTF-8 and invalid JSON remain ordinary `Line` values; the caller's
/// parser reports those as the per-line `invalid` disposition. An oversized
/// line is counted without retaining its tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NdjsonFrame {
    Line {
        ordinal: u64,
        bytes: Vec<u8>,
    },
    InputTooLarge {
        ordinal: u64,
        limit: usize,
        actual: usize,
    },
}

impl NdjsonFrame {
    pub(super) fn ordinal(&self) -> u64 {
        match self {
            Self::Line { ordinal, .. } | Self::InputTooLarge { ordinal, .. } => *ordinal,
        }
    }
}

/// Incremental NDJSON framer with a hard retained-line ceiling.
///
/// Crossing the ceiling drops the retained prefix immediately and counts bytes
/// until the next newline. This is what lets the caller continue with later
/// lines without ever materializing an oversized one. One trailing `\r` before
/// `\n` is treated as the CRLF delimiter and does not count against the line
/// limit.
#[derive(Debug)]
pub(super) struct NdjsonFramer {
    max_line_bytes: usize,
    max_chunk_bytes: usize,
    next_ordinal: u64,
    line: Vec<u8>,
    current_line_bytes: usize,
    last_byte_was_cr: bool,
    discarding_oversized_line: bool,
}

impl Default for NdjsonFramer {
    fn default() -> Self {
        Self::new()
    }
}

impl NdjsonFramer {
    pub(super) fn new() -> Self {
        Self::with_limit(STREAM_NDJSON_MAX_LINE_BYTES)
    }

    fn with_limit(max_line_bytes: usize) -> Self {
        Self::with_limits(max_line_bytes, STREAM_NDJSON_MAX_CHUNK_BYTES)
    }

    fn with_limits(max_line_bytes: usize, max_chunk_bytes: usize) -> Self {
        assert!(max_line_bytes > 0, "NDJSON line limit must be non-zero");
        assert!(max_chunk_bytes > 0, "NDJSON chunk limit must be non-zero");
        Self {
            max_line_bytes,
            max_chunk_bytes,
            next_ordinal: 0,
            line: Vec::new(),
            current_line_bytes: 0,
            last_byte_was_cr: false,
            discarding_oversized_line: false,
        }
    }

    /// Consume one body chunk lazily, yielding at most one completed line at a
    /// time.
    ///
    /// The iterator owns the caller's chunk and borrows the framer, so the
    /// request driver can process and release each line before framing the next
    /// one. Dropping the iterator intentionally discards the unread chunk tail,
    /// which is exactly what request cancellation requires.
    pub(super) fn push_chunk(&mut self, chunk: Vec<u8>) -> Result<NdjsonChunkFrames<'_>> {
        let actual = chunk.len();
        if actual > self.max_chunk_bytes {
            return Err(OmniError::resource_limit(
                "stream_transport_chunk_bytes",
                u64::try_from(self.max_chunk_bytes).unwrap_or(u64::MAX),
                u64::try_from(actual).unwrap_or(u64::MAX),
            ));
        }
        Ok(NdjsonChunkFrames {
            framer: self,
            bytes: chunk.into_iter(),
        })
    }

    fn push_byte(&mut self, byte: u8) -> Option<NdjsonFrame> {
        if byte == b'\n' {
            return Some(self.finish_delimited_line());
        }

        self.current_line_bytes = self.current_line_bytes.saturating_add(1);
        self.last_byte_was_cr = byte == b'\r';
        if self.discarding_oversized_line {
            return None;
        }

        self.line.push(byte);
        if self.line.len() > self.max_line_bytes {
            // A line of exactly `limit` bytes followed by CRLF is legal.
            // Retain the possible delimiter byte until the next byte tells
            // us whether it is really a delimiter.
            let possible_crlf = self.line.len() == self.max_line_bytes + 1 && byte == b'\r';
            if !possible_crlf {
                self.discarding_oversized_line = true;
                self.line = Vec::new();
            }
        }
        None
    }

    /// Complete the final unterminated line, if any.
    ///
    /// Consuming `self` makes pushing bytes after EOF impossible.
    pub(super) fn finish(mut self) -> Vec<NdjsonFrame> {
        if self.current_line_bytes == 0 {
            return Vec::new();
        }

        let ordinal = self.take_ordinal();
        if self.discarding_oversized_line || self.current_line_bytes > self.max_line_bytes {
            return vec![NdjsonFrame::InputTooLarge {
                ordinal,
                limit: self.max_line_bytes,
                actual: self.current_line_bytes,
            }];
        }

        vec![NdjsonFrame::Line {
            ordinal,
            bytes: self.line,
        }]
    }

    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> usize {
        self.line.len()
    }

    #[cfg(test)]
    pub(super) fn is_discarding_oversized_line(&self) -> bool {
        self.discarding_oversized_line
    }

    fn finish_delimited_line(&mut self) -> NdjsonFrame {
        let ordinal = self.take_ordinal();
        let delimiter_bytes = usize::from(self.last_byte_was_cr);
        let actual = self.current_line_bytes.saturating_sub(delimiter_bytes);

        let frame = if self.discarding_oversized_line || actual > self.max_line_bytes {
            NdjsonFrame::InputTooLarge {
                ordinal,
                limit: self.max_line_bytes,
                actual,
            }
        } else {
            if self.last_byte_was_cr {
                let removed = self.line.pop();
                debug_assert_eq!(removed, Some(b'\r'));
            }
            NdjsonFrame::Line {
                ordinal,
                bytes: std::mem::take(&mut self.line),
            }
        };

        self.line.clear();
        self.current_line_bytes = 0;
        self.last_byte_was_cr = false;
        self.discarding_oversized_line = false;
        frame
    }

    fn take_ordinal(&mut self) -> u64 {
        let ordinal = self.next_ordinal;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .expect("a bounded stream request cannot exhaust u64 ordinals");
        ordinal
    }
}

/// Lazy completed-frame view over one transport chunk.
///
/// This intentionally has no eager collection method in the request path: an
/// arbitrary chunk containing many tiny lines must not become an equally large
/// queue of retained `NdjsonFrame` values.
#[derive(Debug)]
pub(super) struct NdjsonChunkFrames<'a> {
    framer: &'a mut NdjsonFramer,
    bytes: std::vec::IntoIter<u8>,
}

impl Iterator for NdjsonChunkFrames<'_> {
    type Item = NdjsonFrame;

    fn next(&mut self) -> Option<Self::Item> {
        for byte in self.bytes.by_ref() {
            if let Some(frame) = self.framer.push_byte(byte) {
                return Some(frame);
            }
        }
        None
    }
}

/// Stable internal names for the future tagged per-line response union.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamLineStatus {
    Durable,
    AckUnknown,
    AlreadyDurable,
    Withdrawn,
    DeadLettered,
    Invalid,
    StreamInputTooLarge,
    StreamBindingChanged,
    StreamLifecycleChanged,
    StreamAuthorityChanged,
    StreamSequenceConflict,
    StreamIdempotencyConflict,
    StreamResumeRequired,
    StreamFoldRequired,
    StreamBackpressure,
    RecoveryRequired,
    StreamRetryRequired,
}

impl StreamLineStatus {
    fn is_adapter_terminal(self) -> bool {
        matches!(self, Self::Invalid | Self::StreamInputTooLarge)
    }

    fn stops_physical_admission(self) -> bool {
        matches!(
            self,
            Self::AckUnknown
                | Self::StreamBindingChanged
                | Self::StreamLifecycleChanged
                | Self::StreamAuthorityChanged
                | Self::StreamResumeRequired
                | Self::StreamFoldRequired
                | Self::StreamBackpressure
                | Self::RecoveryRequired
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamOccurrence {
    pub(super) stream_incarnation_id: String,
    pub(super) logical_id: String,
    pub(super) write_id: String,
}

/// Fresh current physical binding returned only after an authoritative engine
/// capture. It is deliberately separate from a token's immutable origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamLineBinding {
    pub(super) enrollment_id: String,
    pub(super) shard_id: String,
    pub(super) writer_epoch: u64,
}

/// Variant-safe authoritative evidence carried beside one status.
///
/// Keeping terminal correction evidence out of a nullable field bag prevents
/// a PRESENT acknowledgement from accidentally advertising withdrawal
/// provenance, while `None` remains the only shape available to adapter-local
/// parse/normalization failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StreamLineEvidence {
    None,
    Binding(StreamLineBinding),
    Current {
        binding: StreamLineBinding,
        origin: StreamRowOrigin,
    },
    Withdrawn {
        binding: StreamLineBinding,
        origin: StreamRowOrigin,
        terminal_correction: StreamTerminalCorrection,
    },
}

impl StreamLineEvidence {
    fn binding(&self) -> Option<&StreamLineBinding> {
        match self {
            Self::None => None,
            Self::Binding(binding)
            | Self::Current { binding, .. }
            | Self::Withdrawn { binding, .. } => Some(binding),
        }
    }

    fn origin(&self) -> Option<&StreamRowOrigin> {
        match self {
            Self::Current { origin, .. } | Self::Withdrawn { origin, .. } => Some(origin),
            Self::None | Self::Binding(_) => None,
        }
    }

    fn terminal_correction(&self) -> Option<&StreamTerminalCorrection> {
        match self {
            Self::Withdrawn {
                terminal_correction,
                ..
            } => Some(terminal_correction),
            Self::None | Self::Binding(_) | Self::Current { .. } => None,
        }
    }
}

/// Variant-specific evidence for one line.
///
/// Keeping confirmed and unconfirmed tokens in different variants makes it
/// impossible for response mapping to accidentally turn `AckUnknown` into a
/// durable acknowledgement through a nullable field mix-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StreamLineDetail {
    Durable {
        occurrence: StreamOccurrence,
        confirmed_stream_token: String,
        admission_attempt_id: String,
    },
    AckUnknown {
        occurrence: StreamOccurrence,
        unconfirmed_candidate_token: String,
        admission_attempt_id: String,
        message: String,
    },
    AlreadyDurable {
        occurrence: StreamOccurrence,
        confirmed_stream_token: String,
    },
    Withdrawn {
        occurrence: StreamOccurrence,
        confirmed_stream_token: String,
        message: String,
    },
    #[allow(dead_code)] // Reserved fail-closed vocabulary for the inactive F5 disposition.
    DeadLettered {
        occurrence: StreamOccurrence,
        current_token: String,
        operation_id: String,
        message: String,
    },
    Invalid {
        message: String,
    },
    StreamInputTooLarge {
        limit: u64,
        actual: u64,
    },
    StreamBindingChanged {
        current_stream_incarnation_id: Option<String>,
        logical_id: Option<String>,
        message: String,
    },
    StreamLifecycleChanged {
        current_stream_incarnation_id: Option<String>,
        message: String,
    },
    StreamAuthorityChanged {
        current_stream_incarnation_id: Option<String>,
        message: String,
    },
    StreamSequenceConflict {
        current_stream_incarnation_id: Option<String>,
        logical_id: String,
        current_token: Option<String>,
        message: String,
    },
    StreamIdempotencyConflict {
        current_stream_incarnation_id: Option<String>,
        logical_id: String,
        current_token: String,
        message: String,
    },
    StreamResumeRequired {
        current_stream_incarnation_id: Option<String>,
        message: String,
    },
    StreamFoldRequired {
        message: String,
    },
    StreamBackpressure {
        message: String,
    },
    RecoveryRequired {
        operation_id: String,
        message: String,
    },
    StreamRetryRequired {
        message: String,
    },
}

impl StreamLineDetail {
    pub(super) fn status(&self) -> StreamLineStatus {
        match self {
            Self::Durable { .. } => StreamLineStatus::Durable,
            Self::AckUnknown { .. } => StreamLineStatus::AckUnknown,
            Self::AlreadyDurable { .. } => StreamLineStatus::AlreadyDurable,
            Self::Withdrawn { .. } => StreamLineStatus::Withdrawn,
            Self::DeadLettered { .. } => StreamLineStatus::DeadLettered,
            Self::Invalid { .. } => StreamLineStatus::Invalid,
            Self::StreamInputTooLarge { .. } => StreamLineStatus::StreamInputTooLarge,
            Self::StreamBindingChanged { .. } => StreamLineStatus::StreamBindingChanged,
            Self::StreamLifecycleChanged { .. } => StreamLineStatus::StreamLifecycleChanged,
            Self::StreamAuthorityChanged { .. } => StreamLineStatus::StreamAuthorityChanged,
            Self::StreamSequenceConflict { .. } => StreamLineStatus::StreamSequenceConflict,
            Self::StreamIdempotencyConflict { .. } => StreamLineStatus::StreamIdempotencyConflict,
            Self::StreamResumeRequired { .. } => StreamLineStatus::StreamResumeRequired,
            Self::StreamFoldRequired { .. } => StreamLineStatus::StreamFoldRequired,
            Self::StreamBackpressure { .. } => StreamLineStatus::StreamBackpressure,
            Self::RecoveryRequired { .. } => StreamLineStatus::RecoveryRequired,
            Self::StreamRetryRequired { .. } => StreamLineStatus::StreamRetryRequired,
        }
    }

    pub(super) fn confirmed_stream_token(&self) -> Option<&str> {
        match self {
            Self::Durable {
                confirmed_stream_token,
                ..
            }
            | Self::AlreadyDurable {
                confirmed_stream_token,
                ..
            }
            | Self::Withdrawn {
                confirmed_stream_token,
                ..
            } => Some(confirmed_stream_token),
            _ => None,
        }
    }

    pub(super) fn unconfirmed_candidate_token(&self) -> Option<&str> {
        match self {
            Self::AckUnknown {
                unconfirmed_candidate_token,
                ..
            } => Some(unconfirmed_candidate_token),
            _ => None,
        }
    }

    pub(super) fn current_token(&self) -> Option<&str> {
        match self {
            Self::DeadLettered { current_token, .. }
            | Self::StreamIdempotencyConflict { current_token, .. } => Some(current_token),
            Self::StreamSequenceConflict { current_token, .. } => current_token.as_deref(),
            _ => None,
        }
    }

    pub(super) fn current_stream_incarnation_id(&self) -> Option<&str> {
        match self {
            Self::Durable { occurrence, .. }
            | Self::AckUnknown { occurrence, .. }
            | Self::AlreadyDurable { occurrence, .. }
            | Self::Withdrawn { occurrence, .. }
            | Self::DeadLettered { occurrence, .. } => {
                Some(occurrence.stream_incarnation_id.as_str())
            }
            Self::StreamBindingChanged {
                current_stream_incarnation_id,
                ..
            }
            | Self::StreamLifecycleChanged {
                current_stream_incarnation_id,
                ..
            }
            | Self::StreamAuthorityChanged {
                current_stream_incarnation_id,
                ..
            }
            | Self::StreamSequenceConflict {
                current_stream_incarnation_id,
                ..
            }
            | Self::StreamIdempotencyConflict {
                current_stream_incarnation_id,
                ..
            }
            | Self::StreamResumeRequired {
                current_stream_incarnation_id,
                ..
            } => current_stream_incarnation_id.as_deref(),
            _ => None,
        }
    }

    pub(super) fn logical_id(&self) -> Option<&str> {
        match self {
            Self::Durable { occurrence, .. }
            | Self::AckUnknown { occurrence, .. }
            | Self::AlreadyDurable { occurrence, .. }
            | Self::Withdrawn { occurrence, .. }
            | Self::DeadLettered { occurrence, .. } => Some(occurrence.logical_id.as_str()),
            Self::StreamBindingChanged { logical_id, .. } => logical_id.as_deref(),
            Self::StreamSequenceConflict { logical_id, .. }
            | Self::StreamIdempotencyConflict { logical_id, .. } => Some(logical_id),
            _ => None,
        }
    }

    pub(super) fn write_id(&self) -> Option<&str> {
        match self {
            Self::Durable { occurrence, .. }
            | Self::AckUnknown { occurrence, .. }
            | Self::AlreadyDurable { occurrence, .. }
            | Self::Withdrawn { occurrence, .. }
            | Self::DeadLettered { occurrence, .. } => Some(occurrence.write_id.as_str()),
            _ => None,
        }
    }

    pub(super) fn operation_id(&self) -> Option<&str> {
        match self {
            Self::DeadLettered { operation_id, .. }
            | Self::RecoveryRequired { operation_id, .. } => Some(operation_id),
            _ => None,
        }
    }

    pub(super) fn admission_attempt_id(&self) -> Option<&str> {
        match self {
            Self::Durable {
                admission_attempt_id,
                ..
            }
            | Self::AckUnknown {
                admission_attempt_id,
                ..
            } => Some(admission_attempt_id),
            _ => None,
        }
    }

    pub(super) fn message(&self) -> Option<&str> {
        match self {
            Self::AckUnknown { message, .. }
            | Self::Withdrawn { message, .. }
            | Self::DeadLettered { message, .. }
            | Self::Invalid { message }
            | Self::StreamBindingChanged { message, .. }
            | Self::StreamLifecycleChanged { message, .. }
            | Self::StreamAuthorityChanged { message, .. }
            | Self::StreamSequenceConflict { message, .. }
            | Self::StreamIdempotencyConflict { message, .. }
            | Self::StreamResumeRequired { message, .. }
            | Self::StreamFoldRequired { message }
            | Self::StreamBackpressure { message }
            | Self::RecoveryRequired { message, .. }
            | Self::StreamRetryRequired { message } => Some(message),
            Self::Durable { .. }
            | Self::AlreadyDurable { .. }
            | Self::StreamInputTooLarge { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamBlockingRef {
    pub(super) ordinal: u64,
    pub(super) status: StreamLineStatus,
    pub(super) admission_attempt_id: Option<String>,
    pub(super) current_stream_incarnation_id: Option<String>,
    pub(super) operation_id: Option<String>,
    pub(super) binding: Option<StreamLineBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamLineOutcome {
    pub(super) ordinal: u64,
    pub(super) detail: StreamLineDetail,
    pub(super) evidence: StreamLineEvidence,
    pub(super) blocking: Option<StreamBlockingRef>,
}

impl StreamLineOutcome {
    pub(super) fn new(ordinal: u64, detail: StreamLineDetail) -> Self {
        Self {
            ordinal,
            detail,
            evidence: StreamLineEvidence::None,
            blocking: None,
        }
    }

    pub(super) fn with_binding(mut self, binding: StreamLineBinding) -> Self {
        self.evidence = StreamLineEvidence::Binding(binding);
        self
    }

    pub(super) fn with_current_evidence(
        mut self,
        binding: StreamLineBinding,
        origin: StreamRowOrigin,
    ) -> Self {
        self.evidence = StreamLineEvidence::Current { binding, origin };
        self
    }

    pub(super) fn with_withdrawn_evidence(
        mut self,
        binding: StreamLineBinding,
        origin: StreamRowOrigin,
        terminal_correction: StreamTerminalCorrection,
    ) -> Self {
        self.evidence = StreamLineEvidence::Withdrawn {
            binding,
            origin,
            terminal_correction,
        };
        self
    }

    pub(super) fn binding(&self) -> Option<&StreamLineBinding> {
        self.evidence.binding()
    }

    pub(super) fn origin(&self) -> Option<&StreamRowOrigin> {
        self.evidence.origin()
    }

    pub(super) fn terminal_correction(&self) -> Option<&StreamTerminalCorrection> {
        self.evidence.terminal_correction()
    }

    pub(super) fn status(&self) -> StreamLineStatus {
        self.detail.status()
    }

    pub(super) fn confirmed_stream_token(&self) -> Option<&str> {
        self.detail.confirmed_stream_token()
    }

    pub(super) fn unconfirmed_candidate_token(&self) -> Option<&str> {
        self.detail.unconfirmed_candidate_token()
    }

    pub(super) fn current_token(&self) -> Option<&str> {
        self.detail.current_token()
    }

    pub(super) fn current_stream_incarnation_id(&self) -> Option<&str> {
        self.detail.current_stream_incarnation_id()
    }

    pub(super) fn logical_id(&self) -> Option<&str> {
        self.detail.logical_id()
    }

    pub(super) fn write_id(&self) -> Option<&str> {
        self.detail.write_id()
    }

    pub(super) fn operation_id(&self) -> Option<&str> {
        self.detail.operation_id()
    }

    pub(super) fn admission_attempt_id(&self) -> Option<&str> {
        self.detail.admission_attempt_id()
    }

    pub(super) fn message(&self) -> Option<&str> {
        self.detail.message()
    }

    pub(super) fn as_blocker(&self) -> Option<StreamRequestBlocker> {
        StreamRequestBlocker::from_outcome(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamRequestBlocker {
    reference: StreamBlockingRef,
    tail_detail: StreamLineDetail,
}

impl StreamRequestBlocker {
    fn from_outcome(outcome: &StreamLineOutcome) -> Option<Self> {
        if !outcome.status().stops_physical_admission() {
            return None;
        }

        let message = outcome
            .message()
            .unwrap_or("stream request admission stopped");
        let reference = StreamBlockingRef {
            ordinal: outcome.ordinal,
            status: outcome.status(),
            admission_attempt_id: outcome.admission_attempt_id().map(ToOwned::to_owned),
            current_stream_incarnation_id: outcome
                .current_stream_incarnation_id()
                .map(ToOwned::to_owned),
            operation_id: outcome.operation_id().map(ToOwned::to_owned),
            binding: outcome.binding().cloned(),
        };
        let tail_detail = match &outcome.detail {
            StreamLineDetail::AckUnknown { .. } => StreamLineDetail::StreamRetryRequired {
                message: message.to_string(),
            },
            StreamLineDetail::StreamBindingChanged { .. } => {
                StreamLineDetail::StreamBindingChanged {
                    current_stream_incarnation_id: None,
                    logical_id: None,
                    message: message.to_string(),
                }
            }
            StreamLineDetail::StreamLifecycleChanged { .. } => {
                StreamLineDetail::StreamLifecycleChanged {
                    current_stream_incarnation_id: None,
                    message: message.to_string(),
                }
            }
            StreamLineDetail::StreamAuthorityChanged { .. } => {
                StreamLineDetail::StreamAuthorityChanged {
                    current_stream_incarnation_id: None,
                    message: message.to_string(),
                }
            }
            StreamLineDetail::StreamResumeRequired { .. } => {
                StreamLineDetail::StreamResumeRequired {
                    current_stream_incarnation_id: None,
                    message: message.to_string(),
                }
            }
            StreamLineDetail::StreamFoldRequired { .. } => StreamLineDetail::StreamFoldRequired {
                message: message.to_string(),
            },
            StreamLineDetail::StreamBackpressure { .. } => StreamLineDetail::StreamBackpressure {
                message: message.to_string(),
            },
            StreamLineDetail::RecoveryRequired { operation_id, .. } => {
                StreamLineDetail::RecoveryRequired {
                    operation_id: operation_id.clone(),
                    message: message.to_string(),
                }
            }
            _ => unreachable!("stopping status and detail must stay structurally aligned"),
        };
        Some(Self {
            reference,
            tail_detail,
        })
    }
}

/// Apply RFC-026's stop-tail precedence to one effect-free local result.
///
/// Adapter-local invalidity and intrinsic single-row oversize always win.
/// Every other uninvoked line inherits the request blocker; an ambiguous
/// blocker becomes `stream_retry_required`. Tail results contain no confirmed,
/// candidate, or current token copied from another occurrence.
pub(super) fn apply_tail_precedence(
    local: StreamLineOutcome,
    blocker: Option<&StreamRequestBlocker>,
) -> StreamLineOutcome {
    let Some(blocker) = blocker else {
        return local;
    };
    if local.status().is_adapter_terminal() {
        return local;
    }

    StreamLineOutcome {
        ordinal: local.ordinal,
        detail: blocker.tail_detail.clone(),
        evidence: StreamLineEvidence::None,
        blocking: Some(blocker.reference.clone()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamRequestLimits {
    root_requests: usize,
    root_bytes: u64,
    actor_requests: usize,
    actor_bytes: u64,
    request_bytes: u64,
}

const DEFAULT_STREAM_REQUEST_LIMITS: StreamRequestLimits = StreamRequestLimits {
    root_requests: STREAM_REQUEST_ROOT_MAX_REQUESTS,
    root_bytes: STREAM_REQUEST_ROOT_MAX_BYTES,
    actor_requests: STREAM_REQUEST_ACTOR_MAX_REQUESTS,
    actor_bytes: STREAM_REQUEST_ACTOR_MAX_BYTES,
    request_bytes: STREAM_REQUEST_RESERVATION_BYTES,
};

#[derive(Debug, Default)]
struct ActorRequestUsage {
    requests: usize,
    bytes: u64,
}

#[derive(Debug, Default)]
struct StreamRequestUsage {
    root_requests: usize,
    root_bytes: u64,
    actors: HashMap<String, ActorRequestUsage>,
}

/// Root-shared transport admission independent of MemWAL worker accounting.
///
/// One mutex owns root and actor count/byte checks as one compare-and-update,
/// preventing either dimension from leaking when another dimension refuses.
#[derive(Debug)]
pub(super) struct StreamRequestRegistry {
    limits: StreamRequestLimits,
    usage: Mutex<StreamRequestUsage>,
}

static STREAM_REQUEST_REGISTRIES: OnceLock<Mutex<HashMap<String, Weak<StreamRequestRegistry>>>> =
    OnceLock::new();

impl StreamRequestRegistry {
    pub(super) fn for_root(root_identity: &str) -> Arc<Self> {
        Self::for_root_with_limits(root_identity, DEFAULT_STREAM_REQUEST_LIMITS)
    }

    fn for_root_with_limits(root_identity: &str, limits: StreamRequestLimits) -> Arc<Self> {
        assert!(limits.root_requests > 0);
        assert!(limits.root_bytes > 0);
        assert!(limits.actor_requests > 0);
        assert!(limits.actor_bytes > 0);
        assert!(limits.request_bytes > 0);

        let registries = STREAM_REQUEST_REGISTRIES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut roots = registries
            .lock()
            .expect("stream request root registry poisoned");
        if let Some(existing) = roots.get(root_identity).and_then(Weak::upgrade) {
            assert_eq!(
                existing.limits, limits,
                "one stream request root cannot use different limits"
            );
            return existing;
        }
        roots.retain(|_, registry| registry.strong_count() > 0);
        let created = Arc::new(Self {
            limits,
            usage: Mutex::new(StreamRequestUsage::default()),
        });
        roots.insert(root_identity.to_string(), Arc::downgrade(&created));
        created
    }

    pub(super) fn try_acquire(self: &Arc<Self>, actor_id: &str) -> Result<StreamRequestPermit> {
        let mut usage = self.usage.lock().expect("stream request usage poisoned");
        let actor = usage.actors.get(actor_id);
        let actor_requests = actor
            .map(|actor| actor.requests)
            .unwrap_or_default()
            .saturating_add(1);
        if actor_requests > self.limits.actor_requests {
            return Err(OmniError::resource_limit(
                "stream_transport_actor_requests",
                self.limits.actor_requests as u64,
                actor_requests as u64,
            ));
        }
        let actor_bytes = actor
            .map(|actor| actor.bytes)
            .unwrap_or_default()
            .saturating_add(self.limits.request_bytes);
        if actor_bytes > self.limits.actor_bytes {
            return Err(OmniError::resource_limit(
                "stream_transport_actor_bytes",
                self.limits.actor_bytes,
                actor_bytes,
            ));
        }

        let root_requests = usage.root_requests.saturating_add(1);
        if root_requests > self.limits.root_requests {
            return Err(OmniError::resource_limit(
                "stream_transport_root_requests",
                self.limits.root_requests as u64,
                root_requests as u64,
            ));
        }
        let root_bytes = usage.root_bytes.saturating_add(self.limits.request_bytes);
        if root_bytes > self.limits.root_bytes {
            return Err(OmniError::resource_limit(
                "stream_transport_root_bytes",
                self.limits.root_bytes,
                root_bytes,
            ));
        }

        usage.root_requests = root_requests;
        usage.root_bytes = root_bytes;
        let actor = usage.actors.entry(actor_id.to_string()).or_default();
        actor.requests = actor_requests;
        actor.bytes = actor_bytes;
        Ok(StreamRequestPermit {
            registry: Arc::clone(self),
            actor_id: actor_id.to_string(),
            bytes: self.limits.request_bytes,
        })
    }

    #[cfg(test)]
    fn usage_for_test(&self, actor_id: &str) -> (usize, u64, usize, u64) {
        let usage = self.usage.lock().expect("stream request usage poisoned");
        let actor = usage.actors.get(actor_id);
        (
            usage.root_requests,
            usage.root_bytes,
            actor.map(|actor| actor.requests).unwrap_or_default(),
            actor.map(|actor| actor.bytes).unwrap_or_default(),
        )
    }
}

#[derive(Debug)]
#[must_use = "dropping the permit releases stream request transport ownership"]
pub(super) struct StreamRequestPermit {
    registry: Arc<StreamRequestRegistry>,
    actor_id: String,
    bytes: u64,
}

impl Drop for StreamRequestPermit {
    fn drop(&mut self) {
        let mut usage = self
            .registry
            .usage
            .lock()
            .expect("stream request usage poisoned");
        usage.root_requests = usage
            .root_requests
            .checked_sub(1)
            .expect("stream request root slot underflow");
        usage.root_bytes = usage
            .root_bytes
            .checked_sub(self.bytes)
            .expect("stream request root byte underflow");

        let remove_actor = {
            let actor = usage
                .actors
                .get_mut(&self.actor_id)
                .expect("stream request actor usage missing");
            actor.requests = actor
                .requests
                .checked_sub(1)
                .expect("stream request actor slot underflow");
            actor.bytes = actor
                .bytes
                .checked_sub(self.bytes)
                .expect("stream request actor byte underflow");
            actor.requests == 0 && actor.bytes == 0
        };
        if remove_actor {
            usage.actors.remove(&self.actor_id);
        }
    }
}

/// Deterministic bounded reorder storage for per-line outcomes.
#[derive(Debug)]
pub(super) struct OrderedResultBuffer {
    next_ordinal: Option<u64>,
    pending: BTreeMap<u64, StreamLineOutcome>,
    capacity: usize,
}

impl OrderedResultBuffer {
    pub(super) fn new(first_ordinal: u64) -> Self {
        Self::with_capacity(first_ordinal, STREAM_REQUEST_MAX_RESULT_STATUSES)
    }

    fn with_capacity(first_ordinal: u64, capacity: usize) -> Self {
        assert!(capacity > 0);
        assert!(capacity <= STREAM_REQUEST_MAX_RESULT_STATUSES);
        Self {
            next_ordinal: Some(first_ordinal),
            pending: BTreeMap::new(),
            capacity,
        }
    }

    pub(super) fn insert(&mut self, outcome: StreamLineOutcome) -> Result<()> {
        let Some(next_ordinal) = self.next_ordinal else {
            return Err(OmniError::manifest_internal(
                "stream result arrived after ordinal space was exhausted",
            ));
        };
        if outcome.ordinal < next_ordinal {
            return Err(OmniError::manifest_internal(format!(
                "stream result ordinal {} was already emitted (next is {})",
                outcome.ordinal, next_ordinal
            )));
        }
        if self.pending.contains_key(&outcome.ordinal) {
            return Err(OmniError::manifest_internal(format!(
                "duplicate stream result ordinal {}",
                outcome.ordinal
            )));
        }
        let actual = self.pending.len().saturating_add(1);
        if actual > self.capacity {
            return Err(OmniError::resource_limit(
                "stream_result_reorder_statuses",
                self.capacity as u64,
                actual as u64,
            ));
        }
        self.pending.insert(outcome.ordinal, outcome);
        Ok(())
    }

    pub(super) fn pop_ready(&mut self) -> Option<StreamLineOutcome> {
        let next = self.next_ordinal?;
        let outcome = self.pending.remove(&next)?;
        self.next_ordinal = next.checked_add(1);
        Some(outcome)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    #[cfg(test)]
    pub(super) fn is_full(&self) -> bool {
        self.pending.len() == self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn occurrence(logical_id: &str) -> StreamOccurrence {
        StreamOccurrence {
            stream_incarnation_id: "stream-1".to_string(),
            logical_id: logical_id.to_string(),
            write_id: format!("write-{logical_id}"),
        }
    }

    fn invalid(ordinal: u64, message: &str) -> StreamLineOutcome {
        StreamLineOutcome::new(
            ordinal,
            StreamLineDetail::Invalid {
                message: message.to_string(),
            },
        )
    }

    #[test]
    fn ndjson_framer_handles_split_crlf_multiple_lines_and_eof() {
        let mut framer = NdjsonFramer::with_limit(32);
        assert!(
            framer
                .push_chunk(br#"{"a":1}"#.to_vec())
                .expect("bounded chunk")
                .next()
                .is_none()
        );
        assert_eq!(
            framer
                .push_chunk(b"\r\n\n{\"b\":2}\n{\"c\":3}".to_vec())
                .expect("bounded chunk")
                .collect::<Vec<_>>(),
            vec![
                NdjsonFrame::Line {
                    ordinal: 0,
                    bytes: br#"{"a":1}"#.to_vec(),
                },
                NdjsonFrame::Line {
                    ordinal: 1,
                    bytes: Vec::new(),
                },
                NdjsonFrame::Line {
                    ordinal: 2,
                    bytes: br#"{"b":2}"#.to_vec(),
                },
            ]
        );
        assert_eq!(
            framer.finish(),
            vec![NdjsonFrame::Line {
                ordinal: 3,
                bytes: br#"{"c":3}"#.to_vec(),
            }]
        );
    }

    #[test]
    fn ndjson_framer_discards_oversized_tail_and_resumes_at_newline() {
        let mut framer = NdjsonFramer::with_limit(4);
        assert!(
            framer
                .push_chunk(b"12345".to_vec())
                .expect("bounded chunk")
                .next()
                .is_none()
        );
        assert!(framer.is_discarding_oversized_line());
        assert_eq!(framer.retained_bytes(), 0);
        assert_eq!(
            framer
                .push_chunk(b"678\r\nok\n".to_vec())
                .expect("bounded chunk")
                .collect::<Vec<_>>(),
            vec![
                NdjsonFrame::InputTooLarge {
                    ordinal: 0,
                    limit: 4,
                    actual: 8,
                },
                NdjsonFrame::Line {
                    ordinal: 1,
                    bytes: b"ok".to_vec(),
                },
            ]
        );

        let mut exact_crlf = NdjsonFramer::with_limit(4);
        assert_eq!(
            exact_crlf
                .push_chunk(b"1234\r\n".to_vec())
                .expect("bounded chunk")
                .collect::<Vec<_>>(),
            vec![NdjsonFrame::Line {
                ordinal: 0,
                bytes: b"1234".to_vec(),
            }]
        );

        let mut oversized_eof = NdjsonFramer::with_limit(4);
        assert!(
            oversized_eof
                .push_chunk(b"12345".to_vec())
                .expect("bounded chunk")
                .next()
                .is_none()
        );
        assert_eq!(
            oversized_eof.finish(),
            vec![NdjsonFrame::InputTooLarge {
                ordinal: 0,
                limit: 4,
                actual: 5,
            }]
        );
    }

    #[test]
    fn ndjson_chunk_frames_are_lazy_and_drop_the_unread_tail() {
        let mut framer = NdjsonFramer::with_limit(32);
        let mut frames = framer
            .push_chunk(b"first\nsecond\nthird\n".to_vec())
            .expect("bounded chunk");
        assert_eq!(
            frames.next(),
            Some(NdjsonFrame::Line {
                ordinal: 0,
                bytes: b"first".to_vec(),
            })
        );
        drop(frames);

        assert_eq!(
            framer
                .push_chunk(b"after-cancel\n".to_vec())
                .expect("bounded chunk")
                .collect::<Vec<_>>(),
            vec![NdjsonFrame::Line {
                ordinal: 1,
                bytes: b"after-cancel".to_vec(),
            }]
        );
    }

    #[test]
    fn ndjson_framer_rejects_an_oversized_transport_chunk_before_consuming_it() {
        let mut framer = NdjsonFramer::with_limits(32, 8);
        let error = match framer.push_chunk(b"a\nb\nc\nd\nx".to_vec()) {
            Ok(_) => panic!("an oversized transport chunk must be refused"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 8,
                actual: 9,
            } if resource == "stream_transport_chunk_bytes"
        ));
        assert_eq!(
            framer
                .push_chunk(b"okay\n".to_vec())
                .expect("the refused chunk must not advance framing")
                .collect::<Vec<_>>(),
            vec![NdjsonFrame::Line {
                ordinal: 0,
                bytes: b"okay".to_vec(),
            }]
        );
    }

    #[test]
    fn stop_tail_preserves_local_terminal_results_and_sanitizes_tokens() {
        let binding = StreamLineBinding {
            enrollment_id: "enrollment-a".to_string(),
            shard_id: "shard-a".to_string(),
            writer_epoch: 7,
        };
        let blocker_outcome = StreamLineOutcome::new(
            4,
            StreamLineDetail::AckUnknown {
                occurrence: occurrence("a"),
                unconfirmed_candidate_token: "candidate-a".to_string(),
                admission_attempt_id: "attempt-a".to_string(),
                message: "watcher result unknown".to_string(),
            },
        )
        .with_binding(binding.clone());
        assert_eq!(blocker_outcome.confirmed_stream_token(), None);
        assert_eq!(
            blocker_outcome.unconfirmed_candidate_token(),
            Some("candidate-a")
        );
        let blocker = blocker_outcome.as_blocker().expect("ack is a blocker");

        let otherwise_admissible = StreamLineOutcome::new(
            5,
            StreamLineDetail::AlreadyDurable {
                occurrence: occurrence("b"),
                confirmed_stream_token: "confirmed-b".to_string(),
            },
        );
        let inherited = apply_tail_precedence(otherwise_admissible, Some(&blocker));
        assert_eq!(inherited.status(), StreamLineStatus::StreamRetryRequired);
        assert_eq!(inherited.confirmed_stream_token(), None);
        assert_eq!(inherited.unconfirmed_candidate_token(), None);
        assert_eq!(inherited.current_token(), None);
        assert_eq!(inherited.admission_attempt_id(), None);
        assert_eq!(
            inherited.blocking,
            Some(StreamBlockingRef {
                ordinal: 4,
                status: StreamLineStatus::AckUnknown,
                admission_attempt_id: Some("attempt-a".to_string()),
                current_stream_incarnation_id: Some("stream-1".to_string()),
                operation_id: None,
                binding: Some(binding),
            })
        );
        assert_eq!(inherited.binding(), None);

        let invalid = apply_tail_precedence(invalid(6, "bad json"), Some(&blocker));
        assert_eq!(invalid.status(), StreamLineStatus::Invalid);
        assert_eq!(invalid.message(), Some("bad json"));
        assert_eq!(invalid.blocking, None);

        let too_large = apply_tail_precedence(
            StreamLineOutcome::new(
                7,
                StreamLineDetail::StreamInputTooLarge {
                    limit: 32,
                    actual: 33,
                },
            ),
            Some(&blocker),
        );
        assert_eq!(too_large.status(), StreamLineStatus::StreamInputTooLarge);
        assert_eq!(too_large.blocking, None);
    }

    #[test]
    fn stop_tail_repeats_effect_free_blocker_and_conflicts_keep_current_token() {
        let fold = StreamLineOutcome::new(
            2,
            StreamLineDetail::StreamFoldRequired {
                message: "generation full".to_string(),
            },
        );
        let fold_blocker = fold.as_blocker().expect("fold is a blocker");
        let conflict = StreamLineOutcome::new(
            3,
            StreamLineDetail::StreamIdempotencyConflict {
                current_stream_incarnation_id: Some("stream-1".to_string()),
                logical_id: "x".to_string(),
                current_token: "current-x".to_string(),
                message: "payload changed".to_string(),
            },
        );
        assert_eq!(conflict.current_token(), Some("current-x"));
        let inherited = apply_tail_precedence(conflict, Some(&fold_blocker));
        assert_eq!(inherited.status(), StreamLineStatus::StreamFoldRequired);
        assert_eq!(inherited.current_token(), None);
        assert_eq!(
            inherited.blocking.as_ref().map(|blocked| blocked.ordinal),
            Some(2)
        );
    }

    #[test]
    fn request_registry_enforces_actor_and_root_slots_and_releases_raii() {
        let actor_limited = StreamRequestRegistry::for_root_with_limits(
            &format!("memory://stream-request-actor/{}", ulid::Ulid::new()),
            StreamRequestLimits {
                root_requests: 4,
                root_bytes: 4_000,
                actor_requests: 1,
                actor_bytes: 4_000,
                request_bytes: 100,
            },
        );
        let alice = actor_limited.try_acquire("alice").expect("alice first");
        let error = actor_limited
            .try_acquire("alice")
            .expect_err("alice second must hit actor slot cap");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "stream_transport_actor_requests"
        ));
        assert_eq!(actor_limited.usage_for_test("alice"), (1, 100, 1, 100));
        drop(alice);
        assert_eq!(actor_limited.usage_for_test("alice"), (0, 0, 0, 0));
        let _alice_again = actor_limited
            .try_acquire("alice")
            .expect("RAII release restores actor capacity");

        let root_limited = StreamRequestRegistry::for_root_with_limits(
            &format!("memory://stream-request-root/{}", ulid::Ulid::new()),
            StreamRequestLimits {
                root_requests: 1,
                root_bytes: 4_000,
                actor_requests: 2,
                actor_bytes: 4_000,
                request_bytes: 100,
            },
        );
        let _alice = root_limited.try_acquire("alice").expect("root first");
        let error = root_limited
            .try_acquire("bob")
            .expect_err("root second must hit root slot cap");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "stream_transport_root_requests"
        ));
    }

    #[test]
    fn request_registry_enforces_actor_and_root_byte_budgets_atomically() {
        let actor_bytes = StreamRequestRegistry::for_root_with_limits(
            &format!("memory://stream-request-actor-bytes/{}", ulid::Ulid::new()),
            StreamRequestLimits {
                root_requests: 4,
                root_bytes: 4_000,
                actor_requests: 4,
                actor_bytes: 150,
                request_bytes: 100,
            },
        );
        let _alice = actor_bytes.try_acquire("alice").expect("actor byte first");
        let error = actor_bytes
            .try_acquire("alice")
            .expect_err("actor byte second");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "stream_transport_actor_bytes"
        ));
        assert_eq!(actor_bytes.usage_for_test("alice"), (1, 100, 1, 100));

        let root_bytes = StreamRequestRegistry::for_root_with_limits(
            &format!("memory://stream-request-root-bytes/{}", ulid::Ulid::new()),
            StreamRequestLimits {
                root_requests: 4,
                root_bytes: 150,
                actor_requests: 4,
                actor_bytes: 4_000,
                request_bytes: 100,
            },
        );
        let _alice = root_bytes.try_acquire("alice").expect("root byte first");
        let error = root_bytes.try_acquire("bob").expect_err("root byte second");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "stream_transport_root_bytes"
        ));
        assert_eq!(root_bytes.usage_for_test("alice"), (1, 100, 1, 100));
        assert_eq!(root_bytes.usage_for_test("bob"), (1, 100, 0, 0));
    }

    #[test]
    fn ordered_result_buffer_emits_in_order_and_never_exceeds_bound() {
        let mut buffer = OrderedResultBuffer::with_capacity(10, 3);
        buffer.insert(invalid(12, "twelve")).expect("ordinal 12");
        buffer.insert(invalid(10, "ten")).expect("ordinal 10");
        assert_eq!(buffer.pop_ready().map(|result| result.ordinal), Some(10));
        assert_eq!(buffer.pop_ready(), None);
        buffer.insert(invalid(11, "eleven")).expect("ordinal 11");
        assert_eq!(buffer.pop_ready().map(|result| result.ordinal), Some(11));
        assert_eq!(buffer.pop_ready().map(|result| result.ordinal), Some(12));
        assert_eq!(buffer.len(), 0);

        buffer.insert(invalid(14, "fourteen")).expect("ordinal 14");
        buffer.insert(invalid(15, "fifteen")).expect("ordinal 15");
        buffer.insert(invalid(16, "sixteen")).expect("ordinal 16");
        assert!(buffer.is_full());
        let error = buffer
            .insert(invalid(17, "seventeen"))
            .expect_err("fourth retained status must be refused");
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 3,
                actual: 4,
            } if resource == "stream_result_reorder_statuses"
        ));
        assert_eq!(buffer.len(), 3);
    }
}
