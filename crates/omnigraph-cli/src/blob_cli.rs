//! CLI-only shaping for graph-level Blob reads.
//!
//! The engine and server own descriptor interpretation, snapshot fencing, and
//! bounded payload reads. This module keeps the command line's selector,
//! range, and error vocabulary identical across the embedded and served arms.

use std::fs::File;
use std::io::{self, Write};
use std::ops::Range;
use std::path::PathBuf;

use color_eyre::Report;
use color_eyre::eyre::{Result, eyre};
use omnigraph::db::{ReadTarget, SnapshotId};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph::{BlobCell, EntityKind, ExternalBlobRef};
use omnigraph_api_types::{BlobEntityKind, BlobReadQuery};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_LENGTH, ETAG, HeaderMap, HeaderName};

const SNAPSHOT_ID_HEADER: HeaderName = HeaderName::from_static("omnigraph-snapshot-id");

/// A `--out` destination that is not touched until payload transfer starts.
///
/// `blob get` may leave a successfully delivered prefix after a mid-stream
/// failure, but a selector, range, or descriptor failure that transfers no
/// bytes must not truncate an existing destination. A successful empty Blob
/// reaches `flush` and therefore still creates/truncates the requested file.
pub(crate) struct DeferredOutputFile {
    path: PathBuf,
    file: Option<File>,
}

impl DeferredOutputFile {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path, file: None }
    }

    fn file(&mut self) -> io::Result<&mut File> {
        if self.file.is_none() {
            self.file = Some(File::create(&self.path)?);
        }
        Ok(self.file.as_mut().expect("file was just initialized"))
    }
}

impl Write for DeferredOutputFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.file()?.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file()?.flush()
    }
}

/// One optional CLI byte range, validated before graph or HTTP resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlobRangeRequest {
    start: u64,
    length: Option<u64>,
}

impl BlobRangeRequest {
    pub(crate) fn new(offset: Option<u64>, length: Option<u64>) -> Result<Option<Self>> {
        if offset.is_none() && length.is_none() {
            return Ok(None);
        }
        if length == Some(0) {
            return Err(eyre!("Blob range --length must be greater than zero"));
        }
        let start = offset.unwrap_or(0);
        if let Some(length) = length {
            start.checked_add(length).ok_or_else(|| {
                eyre!("Blob range overflows u64: offset {start}, length {length}")
            })?;
        }
        Ok(Some(Self { start, length }))
    }

    /// Convert to the one HTTP Range shape emitted by the CLI.
    pub(crate) fn header_value(self) -> String {
        match self.length {
            Some(length) => format!("bytes={}-{}", self.start, self.start + length - 1),
            None => format!("bytes={}-", self.start),
        }
    }

    /// Resolve against a managed representation length for embedded reads.
    pub(crate) fn resolve(self, total: u64) -> Result<Range<u64>> {
        if self.start >= total {
            return Err(eyre!("Blob range is not satisfiable"));
        }
        let end = match self.length {
            Some(length) => total.min(self.start + length),
            None => total,
        };
        Ok(self.start..end)
    }

    pub(crate) fn start(self) -> u64 {
        self.start
    }

    pub(crate) fn requested_length(self) -> Option<u64> {
        self.length
    }
}

pub(crate) fn blob_query(
    entity: BlobEntityKind,
    type_name: String,
    id: String,
    property: String,
    branch: Option<String>,
    snapshot: Option<String>,
) -> BlobReadQuery {
    BlobReadQuery {
        entity,
        r#type: type_name,
        id,
        property,
        branch,
        snapshot,
    }
}

pub(crate) fn blob_read_target(query: &BlobReadQuery) -> ReadTarget {
    match query.snapshot.as_deref() {
        Some(snapshot) => ReadTarget::snapshot(SnapshotId::new(snapshot)),
        None => ReadTarget::branch(query.branch.as_deref().unwrap_or("main")),
    }
}

pub(crate) fn blob_cell(query: &BlobReadQuery) -> BlobCell {
    BlobCell {
        entity: match query.entity {
            BlobEntityKind::Node => EntityKind::Node,
            BlobEntityKind::Edge => EntityKind::Edge,
        },
        type_name: query.r#type.clone(),
        id: query.id.clone(),
        property: query.property.clone(),
    }
}

pub(crate) fn blob_url(base_url: &str, query: &BlobReadQuery) -> Result<String> {
    let entity = match query.entity {
        BlobEntityKind::Node => "node",
        BlobEntityKind::Edge => "edge",
    };
    let mut params = vec![
        ("entity", entity),
        ("type", query.r#type.as_str()),
        ("id", query.id.as_str()),
        ("property", query.property.as_str()),
    ];
    if let Some(branch) = query.branch.as_deref() {
        params.push(("branch", branch));
    }
    if let Some(snapshot) = query.snapshot.as_deref() {
        params.push(("snapshot", snapshot));
    }
    crate::helpers::remote_url(base_url, &["blob"], &params)
}

pub(crate) fn whole_external_uri(reference: &ExternalBlobRef) -> Result<&str> {
    if reference.offset != 0 || reference.length.is_some() {
        return Err(eyre!("Blob delivery failed"));
    }
    Ok(&reference.uri)
}

pub(crate) fn map_embedded_blob_error(error: OmniError) -> Report {
    match error {
        OmniError::Manifest(manifest) => match manifest.kind {
            ManifestErrorKind::NotFound => eyre!("Blob cell not found"),
            ManifestErrorKind::BadRequest => eyre!("invalid Blob selector"),
            ManifestErrorKind::Conflict => eyre!("Blob read target changed; retry the command"),
            ManifestErrorKind::Internal => eyre!("Blob delivery failed"),
        },
        OmniError::BlobRangeNotSatisfiable { .. } => {
            eyre!("Blob range is not satisfiable")
        }
        OmniError::ResourceLimitExceeded { .. } => eyre!("Blob read range exceeds the limit"),
        OmniError::BlobIntegrity { .. }
        | OmniError::Storage(_)
        | OmniError::DataFusion(_)
        | OmniError::Io(_) => eyre!("Blob delivery failed"),
        OmniError::Policy(_) => eyre!("Blob read is forbidden"),
        _ => eyre!("Blob read failed"),
    }
}

pub(crate) fn remote_blob_error(status: StatusCode) -> Report {
    match status {
        StatusCode::BAD_REQUEST => eyre!("invalid Blob selector"),
        StatusCode::UNAUTHORIZED => eyre!("Blob read requires authentication"),
        StatusCode::FORBIDDEN => eyre!("Blob read is forbidden"),
        StatusCode::NOT_FOUND => eyre!("Blob cell not found"),
        StatusCode::CONFLICT => eyre!("Blob read target changed; retry the command"),
        StatusCode::RANGE_NOT_SATISFIABLE => eyre!("Blob range is not satisfiable"),
        StatusCode::INTERNAL_SERVER_ERROR => eyre!("Blob delivery failed"),
        other => eyre!("Blob server request failed with status {other}"),
    }
}

#[derive(Debug)]
pub(crate) struct ManagedResponseHeaders {
    pub(crate) length: u64,
    pub(crate) etag: String,
    pub(crate) snapshot_id: String,
}

pub(crate) fn managed_response_headers(headers: &HeaderMap) -> Result<ManagedResponseHeaders> {
    let length = required_header(headers, &CONTENT_LENGTH, "Content-Length")?
        .parse::<u64>()
        .map_err(|_| eyre!("Blob server returned an invalid Content-Length"))?;
    let etag = required_header(headers, &ETAG, "ETag")?.to_string();
    if etag.starts_with("W/") || etag.len() < 2 || !etag.starts_with('"') || !etag.ends_with('"') {
        return Err(eyre!("Blob server returned an invalid strong ETag"));
    }
    let snapshot_id = required_header(headers, &SNAPSHOT_ID_HEADER, "Omnigraph-Snapshot-Id")?;
    if snapshot_id.is_empty() {
        return Err(eyre!("Blob server returned an empty resolved snapshot id"));
    }
    Ok(ManagedResponseHeaders {
        length,
        etag,
        snapshot_id: snapshot_id.to_string(),
    })
}

pub(crate) fn external_response_headers(headers: &HeaderMap) -> Result<(String, String)> {
    let uri = required_header(headers, &reqwest::header::LOCATION, "Location")?;
    let snapshot_id = required_header(headers, &SNAPSHOT_ID_HEADER, "Omnigraph-Snapshot-Id")?;
    if uri.is_empty() || snapshot_id.is_empty() {
        return Err(eyre!("Blob server returned incomplete external metadata"));
    }
    Ok((uri.to_string(), snapshot_id.to_string()))
}

fn required_header<'a>(headers: &'a HeaderMap, name: &HeaderName, label: &str) -> Result<&'a str> {
    headers
        .get(name)
        .ok_or_else(|| eyre!("Blob server response omitted {label}"))?
        .to_str()
        .map_err(|_| eyre!("Blob server returned a non-text {label}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_flags_reject_zero_and_overflow_before_resolution() {
        assert!(BlobRangeRequest::new(None, Some(0)).is_err());
        assert!(BlobRangeRequest::new(Some(u64::MAX), Some(1)).is_err());
    }

    #[test]
    fn range_flags_resolve_open_and_clamped_shapes() {
        let open = BlobRangeRequest::new(Some(2), None).unwrap().unwrap();
        assert_eq!(open.header_value(), "bytes=2-");
        assert_eq!(open.resolve(6).unwrap(), 2..6);

        let clamped = BlobRangeRequest::new(Some(2), Some(99)).unwrap().unwrap();
        assert_eq!(clamped.header_value(), "bytes=2-100");
        assert_eq!(clamped.resolve(6).unwrap(), 2..6);
    }

    #[test]
    fn external_delivery_reports_only_whole_object_descriptors() {
        let whole = ExternalBlobRef {
            uri: "s3://example/blob.bin".to_string(),
            offset: 0,
            length: None,
        };
        assert_eq!(whole_external_uri(&whole).unwrap(), whole.uri);

        for ranged in [
            ExternalBlobRef {
                uri: whole.uri.clone(),
                offset: 1,
                length: None,
            },
            ExternalBlobRef {
                uri: whole.uri.clone(),
                offset: 0,
                length: Some(8),
            },
        ] {
            assert_eq!(
                whole_external_uri(&ranged).unwrap_err().to_string(),
                "Blob delivery failed",
                "a ranged descriptor must fail rather than widen to its whole object"
            );
        }
    }

    #[test]
    fn remote_conflict_matches_embedded_target_change_diagnostic() {
        assert_eq!(
            remote_blob_error(StatusCode::CONFLICT).to_string(),
            "Blob read target changed; retry the command"
        );
    }
}
