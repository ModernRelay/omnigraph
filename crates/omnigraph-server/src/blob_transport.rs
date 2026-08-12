//! HTTP representation semantics and bounded body transport for graph-level
//! Blob reads.
//!
//! The engine owns descriptor interpretation and snapshot pinning. This module
//! owns only the HTTP boundary: validators, one-range selection, redirects,
//! response headers, and a pull-driven two-chunk read window. No producer task
//! is detached from the response. Each read permit is stored inside the
//! returned [`Bytes`], so Hyper retaining a chunk counts against the same
//! two-chunk / 8 MiB bound as an in-flight or prefetched read.

use std::io;
use std::ops::Range;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::http::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_RANGE,
    LOCATION, RANGE,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, stream};
use headers::{ETag as TypedEtag, HeaderMapExt, IfMatch, IfNoneMatch, IfRange};
use omnigraph::error::OmniError;
use omnigraph::{BLOB_READ_RANGE_MAX_BYTES, BlobContent, BlobRead, BlobReader, ExternalBlobRef};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ApiError;

const SNAPSHOT_ID_HEADER: HeaderName = HeaderName::from_static("omnigraph-snapshot-id");

/// A response may own at most two managed chunks across reads, prefetch, and
/// Hyper. Each chunk is independently capped by the engine at 4 MiB.
pub(crate) const BLOB_BODY_MAX_RETAINED_CHUNKS: usize = 2;
pub(crate) const BLOB_BODY_MAX_RETAINED_BYTES: u64 =
    BLOB_BODY_MAX_RETAINED_CHUNKS as u64 * BLOB_READ_RANGE_MAX_BYTES;

trait RangeReader: Send + Sync + 'static {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'static, Result<Bytes, OmniError>>;
}

impl RangeReader for BlobReader {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'static, Result<Bytes, OmniError>> {
        let reader = self.clone();
        Box::pin(async move { BlobReader::read_range(&reader, range).await })
    }
}

struct ManagedSource {
    length: u64,
    etag: String,
    reader: Arc<dyn RangeReader>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryMethod {
    Get,
    Head,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestedRange {
    Ignore,
    Satisfiable { start: u64, end: u64 },
    Unsatisfiable { start: u64, end: u64 },
}

/// Construct a GET response for a fully resolved Blob cell.
pub(crate) fn serve_blob_get(
    read: BlobRead,
    request_headers: &HeaderMap,
) -> Result<Response, ApiError> {
    serve_blob(read, request_headers, DeliveryMethod::Get)
}

/// Construct a HEAD response for a fully resolved Blob cell.
///
/// This is intentionally a separate entry point. The managed HEAD path builds
/// an empty body directly and never constructs or polls a range-read stream.
pub(crate) fn serve_blob_head(
    read: BlobRead,
    request_headers: &HeaderMap,
) -> Result<Response, ApiError> {
    serve_blob(read, request_headers, DeliveryMethod::Head)
}

fn serve_blob(
    read: BlobRead,
    request_headers: &HeaderMap,
    method: DeliveryMethod,
) -> Result<Response, ApiError> {
    let snapshot_id = read.resolved_target.snapshot_id.as_str().to_owned();
    match read.content {
        BlobContent::Managed {
            length,
            etag,
            reader,
        } => serve_managed(
            ManagedSource {
                length,
                etag: etag.into_string(),
                reader: Arc::new(reader),
            },
            &snapshot_id,
            request_headers,
            method,
        ),
        BlobContent::External(reference) => serve_external(&reference, &snapshot_id),
    }
}

fn serve_managed(
    source: ManagedSource,
    snapshot_id: &str,
    request_headers: &HeaderMap,
    method: DeliveryMethod,
) -> Result<Response, ApiError> {
    let etag = header_value(&source.etag, "managed Blob ETag")?;
    let typed_etag = source.etag.parse::<TypedEtag>().map_err(|error| {
        ApiError::internal(format!("cannot parse managed Blob ETag for HTTP: {error}"))
    })?;
    let snapshot_id = header_value(snapshot_id, "resolved snapshot id")?;

    // RFC 9110 evaluates If-Match before If-None-Match and Range. Strong
    // comparison prevents a resumed download from mixing representations.
    if !if_match_passes(request_headers, &typed_etag) {
        let mut response = ApiError::blob_precondition_failed(
            "If-Match did not match the selected managed Blob representation",
        )
        .into_response();
        insert_managed_identity_headers(response.headers_mut(), &etag, &snapshot_id);
        return Ok(response);
    }

    // Weak comparison applies to If-None-Match on GET and HEAD, while the
    // stored validator remains strong on output.
    if if_none_match_matches(request_headers, &typed_etag) {
        let mut response = empty_response(StatusCode::NOT_MODIFIED);
        insert_managed_identity_headers(response.headers_mut(), &etag, &snapshot_id);
        // RFC 9110 permits Content-Length on 304 only when it equals the
        // selected 200 representation length. Set it explicitly so Axum does
        // not synthesize the invalid value `0` for a non-empty Blob.
        insert_header(
            response.headers_mut(),
            CONTENT_LENGTH,
            &source.length.to_string(),
            "managed Blob length",
        )?;
        return Ok(response);
    }

    // HEAD describes the complete selected representation. Range and If-Range
    // are deliberately ignored, and no body stream (therefore no read future)
    // is constructed.
    if method == DeliveryMethod::Head {
        let mut response = empty_response(StatusCode::OK);
        insert_managed_identity_headers(response.headers_mut(), &etag, &snapshot_id);
        insert_header(
            response.headers_mut(),
            CONTENT_LENGTH,
            &source.length.to_string(),
            "managed Blob length",
        )?;
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        return Ok(response);
    }

    let requested = if range_is_allowed_by_if_range(request_headers, &typed_etag) {
        requested_range(request_headers, source.length)
    } else {
        RequestedRange::Ignore
    };

    match requested {
        RequestedRange::Unsatisfiable { start, end } => {
            let mut response =
                ApiError::range_not_satisfiable(start, end, source.length).into_response();
            insert_managed_identity_headers(response.headers_mut(), &etag, &snapshot_id);
            insert_header(
                response.headers_mut(),
                CONTENT_RANGE,
                &format!("bytes */{}", source.length),
                "unsatisfied Blob content range",
            )?;
            Ok(response)
        }
        RequestedRange::Satisfiable { start, end } => {
            let served_length = end - start;
            let body = managed_body(source.reader, start..end);
            let mut response = Response::new(body);
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            insert_managed_identity_headers(response.headers_mut(), &etag, &snapshot_id);
            insert_header(
                response.headers_mut(),
                CONTENT_LENGTH,
                &served_length.to_string(),
                "managed Blob range length",
            )?;
            insert_header(
                response.headers_mut(),
                CONTENT_RANGE,
                &format!("bytes {}-{}/{}", start, end - 1, source.length),
                "managed Blob content range",
            )?;
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            Ok(response)
        }
        RequestedRange::Ignore => {
            let body = if source.length == 0 {
                Body::empty()
            } else {
                managed_body(source.reader, 0..source.length)
            };
            let mut response = Response::new(body);
            *response.status_mut() = StatusCode::OK;
            insert_managed_identity_headers(response.headers_mut(), &etag, &snapshot_id);
            insert_header(
                response.headers_mut(),
                CONTENT_LENGTH,
                &source.length.to_string(),
                "managed Blob length",
            )?;
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            Ok(response)
        }
    }
}

fn serve_external(reference: &ExternalBlobRef, snapshot_id: &str) -> Result<Response, ApiError> {
    // A redirect can faithfully represent only a whole-object descriptor.
    // Sending the bare URI for a ranged descriptor would silently return
    // bytes outside the logical Blob cell, while proxying or translating the
    // range is deliberately outside V1. Fail before headers instead.
    if reference.offset != 0 || reference.length.is_some() {
        return Err(ApiError::internal(
            "ranged external Blob descriptors cannot be redirected safely",
        ));
    }
    let mut response = empty_response(StatusCode::FOUND);
    insert_header(
        response.headers_mut(),
        LOCATION,
        &reference.uri,
        "external Blob redirect URI",
    )?;
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    insert_header(
        response.headers_mut(),
        SNAPSHOT_ID_HEADER,
        snapshot_id,
        "resolved snapshot id",
    )?;
    Ok(response)
}

fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

fn insert_managed_identity_headers(
    headers: &mut HeaderMap,
    etag: &HeaderValue,
    snapshot_id: &HeaderValue,
) {
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(ETAG, etag.clone());
    headers.insert(SNAPSHOT_ID_HEADER, snapshot_id.clone());
}

fn header_value(value: &str, description: &str) -> Result<HeaderValue, ApiError> {
    HeaderValue::from_str(value).map_err(|error| {
        ApiError::internal(format!(
            "cannot encode {description} as an HTTP header: {error}"
        ))
    })
}

fn insert_header(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: &str,
    description: &str,
) -> Result<(), ApiError> {
    headers.insert(name, header_value(value, description)?);
    Ok(())
}

/// Weak If-None-Match comparison over the complete typed entity-tag list.
/// Invalid fields are ignored as a failed precondition match.
fn if_none_match_matches(headers: &HeaderMap, current: &TypedEtag) -> bool {
    headers
        .typed_get::<IfNoneMatch>()
        .is_some_and(|condition| !condition.precondition_passes(current))
}

/// Strong If-Match comparison. An absent field passes; malformed input fails
/// closed as an unmet precondition instead of serving possibly stale bytes.
fn if_match_passes(headers: &HeaderMap, current: &TypedEtag) -> bool {
    match headers.typed_try_get::<IfMatch>() {
        Ok(None) => true,
        Ok(Some(condition)) => condition.precondition_passes(current),
        Err(_) => false,
    }
}

fn trim_ows(mut value: &[u8]) -> &[u8] {
    while value
        .first()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        value = &value[1..];
    }
    while value
        .last()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        value = &value[..value.len() - 1];
    }
    value
}

/// If-Range admits a Range only when the field is absent or contains exactly
/// the current strong validator. HTTP dates, weak tags, malformed fields, and
/// repeated fields all turn the request into a full response.
fn range_is_allowed_by_if_range(headers: &HeaderMap, current: &TypedEtag) -> bool {
    let value_count = headers.get_all(IF_RANGE).iter().count();
    if value_count == 0 {
        return true;
    }
    if value_count != 1 {
        return false;
    }
    headers
        .typed_try_get::<IfRange>()
        .ok()
        .flatten()
        .is_some_and(|condition| !condition.is_modified(Some(current), None))
}

fn requested_range(headers: &HeaderMap, length: u64) -> RequestedRange {
    let mut values = headers.get_all(RANGE).iter();
    let Some(value) = values.next() else {
        return RequestedRange::Ignore;
    };
    if values.next().is_some() {
        return RequestedRange::Ignore;
    }
    parse_range_header(value.as_bytes(), length)
}

fn parse_range_header(raw: &[u8], length: u64) -> RequestedRange {
    let raw = trim_ows(raw);
    let Some(equals) = raw.iter().position(|byte| *byte == b'=') else {
        return RequestedRange::Ignore;
    };
    if !raw[..equals].eq_ignore_ascii_case(b"bytes") {
        return RequestedRange::Ignore;
    }
    let spec = &raw[equals + 1..];
    if spec.is_empty() || spec.contains(&b',') {
        return RequestedRange::Ignore;
    }
    let Some(dash) = spec.iter().position(|byte| *byte == b'-') else {
        return RequestedRange::Ignore;
    };
    if spec[dash + 1..].contains(&b'-') {
        return RequestedRange::Ignore;
    }

    let first = &spec[..dash];
    let last = &spec[dash + 1..];
    if first.is_empty() {
        let Some(suffix_length) = parse_decimal(last) else {
            return RequestedRange::Ignore;
        };
        if suffix_length == 0 || length == 0 {
            return RequestedRange::Unsatisfiable {
                start: length,
                end: length,
            };
        }
        return RequestedRange::Satisfiable {
            start: length.saturating_sub(suffix_length),
            end: length,
        };
    }

    let Some(start) = parse_decimal(first) else {
        return RequestedRange::Ignore;
    };
    if last.is_empty() {
        return if start >= length {
            RequestedRange::Unsatisfiable { start, end: length }
        } else {
            RequestedRange::Satisfiable { start, end: length }
        };
    }

    let Some(inclusive_end) = parse_decimal(last) else {
        return RequestedRange::Ignore;
    };
    if start > inclusive_end {
        return RequestedRange::Ignore;
    }
    let requested_end = inclusive_end.saturating_add(1);
    if start >= length {
        return RequestedRange::Unsatisfiable {
            start,
            end: requested_end,
        };
    }
    let end = if inclusive_end >= length - 1 {
        length
    } else {
        inclusive_end + 1
    };
    RequestedRange::Satisfiable { start, end }
}

fn parse_decimal(raw: &[u8]) -> Option<u64> {
    if raw.is_empty() || !raw.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut value = 0_u64;
    for digit in raw {
        value = value
            .checked_mul(10)?
            .checked_add(u64::from(digit - b'0'))?;
    }
    Some(value)
}

fn managed_body(reader: Arc<dyn RangeReader>, range: Range<u64>) -> Body {
    let permits = Arc::new(Semaphore::new(BLOB_BODY_MAX_RETAINED_CHUNKS));
    Body::from_stream(managed_stream(reader, range, permits))
}

/// Pull-driven ordered prefetch. `buffered(2)` may poll two reads, but each
/// must acquire a permit before asking the engine for bytes. The permit then
/// moves into the returned `Bytes` owner and remains charged through every
/// clone Hyper retains.
fn managed_stream(
    reader: Arc<dyn RangeReader>,
    range: Range<u64>,
    permits: Arc<Semaphore>,
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
    debug_assert_eq!(
        BLOB_BODY_MAX_RETAINED_BYTES,
        BLOB_BODY_MAX_RETAINED_CHUNKS as u64 * BLOB_READ_RANGE_MAX_BYTES
    );
    let final_offset = range.end;
    stream::unfold(range.start, move |start| {
        let reader = Arc::clone(&reader);
        let permits = Arc::clone(&permits);
        async move {
            if start >= final_offset {
                return None;
            }
            let end = final_offset.min(start.saturating_add(BLOB_READ_RANGE_MAX_BYTES));
            Some((read_chunk(reader, permits, start..end), end))
        }
    })
    .buffered(BLOB_BODY_MAX_RETAINED_CHUNKS)
}

async fn read_chunk(
    reader: Arc<dyn RangeReader>,
    permits: Arc<Semaphore>,
    range: Range<u64>,
) -> Result<Bytes, io::Error> {
    let permit = permits
        .acquire_owned()
        .await
        .map_err(|_| io::Error::other("managed Blob response byte budget closed unexpectedly"))?;
    let start = range.start;
    let end = range.end;
    let expected = end - start;
    let bytes = reader.read_range(range).await.map_err(|_error| {
        // Engine/storage errors can contain physical object paths or
        // credentials. Neither logs nor the HTTP body may expose them.
        tracing::error!(
            range_start = start,
            range_end = end,
            "managed Blob payload read failed"
        );
        io::Error::other("managed Blob payload read failed")
    })?;
    let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual != expected {
        return Err(io::Error::other(format!(
            "managed Blob range returned {actual} bytes; expected {expected}"
        )));
    }
    Ok(Bytes::from_owner(LeasedChunk {
        bytes,
        _permit: permit,
    }))
}

struct LeasedChunk {
    bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for LeasedChunk {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use axum::body::to_bytes;
    use futures::StreamExt;
    use omnigraph_api_types::ErrorOutput;
    use tokio::time::timeout;

    use super::*;

    #[derive(Clone, Default)]
    struct FakeReader {
        calls: Arc<AtomicUsize>,
        ranges: Arc<Mutex<Vec<Range<u64>>>>,
    }

    impl RangeReader for FakeReader {
        fn read_range(&self, range: Range<u64>) -> BoxFuture<'static, Result<Bytes, OmniError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.ranges.lock().unwrap().push(range.clone());
            Box::pin(async move {
                let len = usize::try_from(range.end - range.start).unwrap();
                let marker = u8::try_from(range.start / BLOB_READ_RANGE_MAX_BYTES).unwrap();
                Ok(Bytes::from(vec![marker; len]))
            })
        }
    }

    fn source(length: u64, reader: Arc<dyn RangeReader>) -> ManagedSource {
        ManagedSource {
            length,
            etag: "\"managed-tag\"".to_string(),
            reader,
        }
    }

    fn headers(entries: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in entries {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    fn managed_etag() -> TypedEtag {
        "\"managed-tag\"".parse().unwrap()
    }

    #[test]
    fn weak_if_none_match_parses_lists_without_splitting_opaque_commas() {
        assert!(if_none_match_matches(
            &headers(&[("if-none-match", "W/\"other\", \"managed-tag\"")]),
            &managed_etag()
        ));
        assert!(if_none_match_matches(
            &headers(&[("if-none-match", "W/\"managed-tag\"")]),
            &managed_etag()
        ));
        assert!(if_none_match_matches(
            &headers(&[("if-none-match", "*")]),
            &managed_etag()
        ));
        assert!(!if_none_match_matches(
            &headers(&[("if-none-match", "\"opaque,comma\", \"other\"")]),
            &managed_etag()
        ));
        assert!(!if_none_match_matches(
            &headers(&[("if-none-match", "not-an-entity-tag")]),
            &managed_etag()
        ));
    }

    #[test]
    fn if_range_requires_one_exact_strong_validator() {
        assert!(range_is_allowed_by_if_range(
            &HeaderMap::new(),
            &managed_etag()
        ));
        assert!(range_is_allowed_by_if_range(
            &headers(&[("if-range", "\"managed-tag\"")]),
            &managed_etag()
        ));
        for value in [
            "W/\"managed-tag\"",
            "\"other\"",
            "Tue, 11 Aug 2026 12:00:00 GMT",
        ] {
            assert!(!range_is_allowed_by_if_range(
                &headers(&[("if-range", value)]),
                &managed_etag()
            ));
        }
        assert!(!range_is_allowed_by_if_range(
            &headers(&[
                ("if-range", "\"managed-tag\""),
                ("if-range", "\"managed-tag\"")
            ]),
            &managed_etag()
        ));
    }

    #[test]
    fn one_range_parser_distinguishes_ignore_satisfiable_and_unsatisfiable() {
        let cases = [
            (
                b"bytes=0-4".as_slice(),
                10,
                RequestedRange::Satisfiable { start: 0, end: 5 },
            ),
            (
                b"BYTES=5-".as_slice(),
                10,
                RequestedRange::Satisfiable { start: 5, end: 10 },
            ),
            (
                b"bytes=-3".as_slice(),
                10,
                RequestedRange::Satisfiable { start: 7, end: 10 },
            ),
            (
                b"bytes=-30".as_slice(),
                10,
                RequestedRange::Satisfiable { start: 0, end: 10 },
            ),
            (
                b"bytes=7-99".as_slice(),
                10,
                RequestedRange::Satisfiable { start: 7, end: 10 },
            ),
            (
                b"bytes=10-12".as_slice(),
                10,
                RequestedRange::Unsatisfiable { start: 10, end: 13 },
            ),
            (
                b"bytes=10-".as_slice(),
                10,
                RequestedRange::Unsatisfiable { start: 10, end: 10 },
            ),
            (
                b"bytes=-0".as_slice(),
                10,
                RequestedRange::Unsatisfiable { start: 10, end: 10 },
            ),
            (
                b"bytes=0-0".as_slice(),
                0,
                RequestedRange::Unsatisfiable { start: 0, end: 1 },
            ),
            (
                b"bytes=-1".as_slice(),
                0,
                RequestedRange::Unsatisfiable { start: 0, end: 0 },
            ),
            (b"items=0-1".as_slice(), 10, RequestedRange::Ignore),
            (b"bytes=0-1,3-4".as_slice(), 10, RequestedRange::Ignore),
            (b"bytes=4-3".as_slice(), 10, RequestedRange::Ignore),
            (b"bytes =0-1".as_slice(), 10, RequestedRange::Ignore),
            (b"bytes= 0-1".as_slice(), 10, RequestedRange::Ignore),
            (
                b"bytes=18446744073709551616-".as_slice(),
                10,
                RequestedRange::Ignore,
            ),
        ];
        for (raw, length, expected) in cases {
            assert_eq!(parse_range_header(raw, length), expected, "{raw:?}");
        }
    }

    #[tokio::test]
    async fn head_never_constructs_or_polls_a_payload_read() {
        for length in [0, BLOB_READ_RANGE_MAX_BYTES + 1] {
            let reader = FakeReader::default();
            let calls = Arc::clone(&reader.calls);
            let response = serve_managed(
                source(length, Arc::new(reader)),
                "snapshot-1",
                &headers(&[("range", "bytes=0-0"), ("if-range", "\"managed-tag\"")]),
                DeliveryMethod::Head,
            )
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(CONTENT_LENGTH).unwrap(),
                &length.to_string()
            );
            assert!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn conditional_precedence_is_strong_if_match_then_weak_if_none_match_then_range() {
        for (if_match, expected) in [
            ("\"managed-tag\"", StatusCode::OK),
            ("\"other\", \"managed-tag\"", StatusCode::OK),
            ("*", StatusCode::OK),
            ("W/\"managed-tag\"", StatusCode::PRECONDITION_FAILED),
            ("\"other\"", StatusCode::PRECONDITION_FAILED),
            ("not-an-entity-tag", StatusCode::PRECONDITION_FAILED),
        ] {
            let reader = FakeReader::default();
            let calls = Arc::clone(&reader.calls);
            let response = serve_managed(
                source(4, Arc::new(reader)),
                "snapshot-1",
                &headers(&[("if-match", if_match)]),
                DeliveryMethod::Get,
            )
            .unwrap();
            assert_eq!(response.status(), expected, "If-Match: {if_match}");
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }

        let response = serve_managed(
            source(4, Arc::new(FakeReader::default())),
            "snapshot-1",
            &headers(&[
                ("if-match", "\"other\""),
                ("if-none-match", "W/\"managed-tag\""),
                ("range", "bytes=99-100"),
            ]),
            DeliveryMethod::Get,
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

        let reader = FakeReader::default();
        let calls = Arc::clone(&reader.calls);
        let response = serve_managed(
            source(4, Arc::new(reader)),
            "snapshot-1",
            &headers(&[
                ("if-match", "\"managed-tag\""),
                ("if-none-match", "W/\"managed-tag\""),
                ("range", "bytes=99-100"),
            ]),
            DeliveryMethod::Get,
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(CONTENT_LENGTH).unwrap(), "4");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn valid_unsatisfiable_range_is_structured_416_without_a_read() {
        let reader = FakeReader::default();
        let calls = Arc::clone(&reader.calls);
        let response = serve_managed(
            source(5, Arc::new(reader)),
            "snapshot-1",
            &headers(&[("range", "bytes=7-9")]),
            DeliveryMethod::Get,
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers().get(CONTENT_RANGE).unwrap(), "bytes */5");
        let output: ErrorOutput =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let detail = output.blob_range.unwrap();
        assert_eq!((detail.start, detail.end, detail.length), (7, 10, 5));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn if_range_mismatch_and_multiple_ranges_fall_back_to_full_200() {
        for request_headers in [
            headers(&[("range", "bytes=2-3"), ("if-range", "\"other\"")]),
            headers(&[("range", "bytes=0-1,3-4")]),
            headers(&[("range", "items=0-1")]),
        ] {
            let reader = FakeReader::default();
            let response = serve_managed(
                source(5, Arc::new(reader)),
                "snapshot-1",
                &request_headers,
                DeliveryMethod::Get,
            )
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers().get(CONTENT_LENGTH).unwrap(), "5");
            assert!(response.headers().get(CONTENT_RANGE).is_none());
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .len(),
                5
            );
        }
    }

    #[tokio::test]
    async fn one_range_returns_206_and_exact_half_open_read() {
        let reader = FakeReader::default();
        let ranges = Arc::clone(&reader.ranges);
        let response = serve_managed(
            source(10, Arc::new(reader)),
            "snapshot-1",
            &headers(&[("range", "bytes=2-5")]),
            DeliveryMethod::Get,
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            "bytes 2-5/10"
        );
        assert_eq!(response.headers().get(CONTENT_LENGTH).unwrap(), "4");
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .len(),
            4
        );
        assert_eq!(*ranges.lock().unwrap(), vec![2..6]);
    }

    #[tokio::test]
    async fn permits_follow_bytes_and_bound_paused_client_retention() {
        let reader = Arc::new(FakeReader::default());
        let ranges = Arc::clone(&reader.ranges);
        let permits = Arc::new(Semaphore::new(BLOB_BODY_MAX_RETAINED_CHUNKS));
        let total = 3 * BLOB_READ_RANGE_MAX_BYTES;
        let mut stream = Box::pin(managed_stream(reader, 0..total, Arc::clone(&permits)));

        let first = stream.next().await.unwrap().unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(first.len() as u64, BLOB_READ_RANGE_MAX_BYTES);
        assert_eq!(second.len() as u64, BLOB_READ_RANGE_MAX_BYTES);
        assert_eq!(
            (first.len() + second.len()) as u64,
            BLOB_BODY_MAX_RETAINED_BYTES
        );
        assert_eq!(permits.available_permits(), 0);
        assert_eq!(ranges.lock().unwrap().len(), 2);

        assert!(
            timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err(),
            "a third read must wait while Hyper-equivalent owners retain two chunks"
        );
        assert_eq!(ranges.lock().unwrap().len(), 2);
        drop(first);
        let third = stream.next().await.unwrap().unwrap();
        assert_eq!(ranges.lock().unwrap().len(), 3);
        assert_eq!(third.len() as u64, BLOB_READ_RANGE_MAX_BYTES);
        assert_eq!(
            (second.len() + third.len()) as u64,
            BLOB_BODY_MAX_RETAINED_BYTES
        );
        assert_eq!(permits.available_permits(), 0);

        drop(second);
        drop(third);
        drop(stream);
        assert_eq!(permits.available_permits(), BLOB_BODY_MAX_RETAINED_CHUNKS);
    }

    #[derive(Clone, Default)]
    struct PendingReader {
        active: Arc<AtomicUsize>,
    }

    impl RangeReader for PendingReader {
        fn read_range(&self, _range: Range<u64>) -> BoxFuture<'static, Result<Bytes, OmniError>> {
            self.active.fetch_add(1, Ordering::SeqCst);
            Box::pin(PendingRead {
                active: Arc::clone(&self.active),
            })
        }
    }

    struct PendingRead {
        active: Arc<AtomicUsize>,
    }

    impl Future for PendingRead {
        type Output = Result<Bytes, OmniError>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingRead {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn disconnect_drops_every_in_flight_read_and_releases_permits() {
        let reader = PendingReader::default();
        let active = Arc::clone(&reader.active);
        let permits = Arc::new(Semaphore::new(BLOB_BODY_MAX_RETAINED_CHUNKS));
        let mut stream = Box::pin(managed_stream(
            Arc::new(reader),
            0..(2 * BLOB_READ_RANGE_MAX_BYTES),
            Arc::clone(&permits),
        ));

        assert!(
            timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );
        assert!(active.load(Ordering::SeqCst) > 0);
        assert!(active.load(Ordering::SeqCst) <= BLOB_BODY_MAX_RETAINED_CHUNKS);
        drop(stream);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(permits.available_permits(), BLOB_BODY_MAX_RETAINED_CHUNKS);
    }

    #[derive(Clone)]
    struct FailingReader;

    impl RangeReader for FailingReader {
        fn read_range(&self, _range: Range<u64>) -> BoxFuture<'static, Result<Bytes, OmniError>> {
            Box::pin(async {
                Err(OmniError::Lance(
                    "s3://private-bucket/physical/object".to_string(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn body_error_does_not_expose_engine_or_physical_storage_detail() {
        let error = read_chunk(
            Arc::new(FailingReader),
            Arc::new(Semaphore::new(BLOB_BODY_MAX_RETAINED_CHUNKS)),
            0..1,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "managed Blob payload read failed");
        assert!(!error.to_string().contains("private-bucket"));
    }

    #[tokio::test]
    async fn external_redirect_is_exact_no_store_and_has_no_managed_headers() {
        let reference = ExternalBlobRef {
            uri: "s3://bucket/path/object?versionId=exact".to_string(),
            offset: 0,
            length: None,
        };
        let response = serve_external(&reference, "snapshot-1").unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers().get(LOCATION).unwrap(),
            "s3://bucket/path/object?versionId=exact"
        );
        assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(
            response.headers().get(SNAPSHOT_ID_HEADER).unwrap(),
            "snapshot-1"
        );
        assert!(response.headers().get(ETAG).is_none());
        assert!(response.headers().get(CONTENT_LENGTH).is_none());
        assert!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );

        for reference in [
            ExternalBlobRef {
                uri: "s3://bucket/path/object".to_string(),
                offset: 1,
                length: None,
            },
            ExternalBlobRef {
                uri: "s3://bucket/path/object".to_string(),
                offset: 0,
                length: Some(1),
            },
        ] {
            let error = serve_external(&reference, "snapshot-1")
                .expect_err("a redirect must not widen a ranged external Blob");
            assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}
