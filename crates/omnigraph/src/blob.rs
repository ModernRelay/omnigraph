//! Engine-owned interpretation of Lance Blob-v2 descriptors.
//!
//! Lance remains responsible for resolving physical files and reading bytes.
//! This module owns the logical boundary that every OmniGraph carrier needs:
//! parent validity is the sole null witness, a valid zero-length descriptor is
//! a managed value, and malformed persisted descriptors fail before a caller
//! can silently reinterpret them.

use std::fmt;
use std::fmt::Write as _;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{Array, StringArray, StructArray, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::DataType;
use bytes::Bytes;
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::BlobFile;
use lance::datatypes::BlobHandling;
use lance_core::datatypes::BlobKind;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::changes::EntityKind;
use crate::db::{Omnigraph, ReadTarget, ResolvedTarget};
use crate::error::{OmniError, Result};

/// Inclusive raw-byte ceiling for one configured or input external Blob URI.
///
/// This is checked before trimming, URL parsing, percent decoding, or file
/// canonicalization so parser scratch remains bounded independently of the
/// operation-wide retained-metadata budget.
pub const EXTERNAL_BLOB_URI_MAX_BYTES: u64 = 64 * 1024;
const EXTERNAL_BLOB_URI_BYTES_RESOURCE: &str = "external Blob URI bytes";

/// Maximum managed Blob bytes returned by one [`BlobReader::read_range`] call.
///
/// Larger values remain readable through multiple bounded calls. This is a
/// per-call returned-buffer ceiling, not a Blob-size limit.
pub const BLOB_READ_RANGE_MAX_BYTES: u64 = 4 * 1024 * 1024;
const BLOB_READ_RANGE_BYTES_RESOURCE: &str = "Blob read range bytes";

/// One logical Blob cell addressed at graph level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobCell {
    pub entity: EntityKind,
    pub type_name: String,
    pub id: String,
    pub property: String,
}

/// Strong, quoted entity tag for one managed Blob value in one exact table
/// version and immutable-manifest transaction-file identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobEtag(String);

impl BlobEtag {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for BlobEtag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Descriptor-only external Blob reference. Reading the cell never probes or
/// dereferences this URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalBlobRef {
    pub uri: String,
    pub offset: u64,
    pub length: Option<u64>,
}

/// Snapshot-pinned, bounded reader for one managed Blob value.
///
/// The Lance dataset and Blob handle remain private so callers cannot recover
/// a writable dataset or substrate-specific API from this graph-level facade.
/// Pinning prevents retargeting; it is not a durable lease, so branch-tree or
/// version reclamation may make an uncached later range fail loudly.
#[derive(Clone)]
pub struct BlobReader {
    _dataset: Arc<Dataset>,
    file: Arc<BlobFile>,
    length: u64,
}

impl fmt::Debug for BlobReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlobReader")
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl BlobReader {
    pub fn len(&self) -> u64 {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Read one Blob-local half-open byte range without changing reader state.
    pub async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end || range.end > self.length {
            return Err(OmniError::BlobRangeNotSatisfiable {
                start: range.start,
                end: range.end,
                length: self.length,
            });
        }

        let requested = range.end - range.start;
        if requested > BLOB_READ_RANGE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                BLOB_READ_RANGE_BYTES_RESOURCE,
                BLOB_READ_RANGE_MAX_BYTES,
                requested,
            ));
        }
        if requested == 0 {
            return Ok(Bytes::new());
        }

        self.file
            .read_range(range)
            .await
            .map_err(OmniError::storage)
    }
}

/// Logical content of one non-null Blob cell.
#[derive(Debug, Clone)]
pub enum BlobContent {
    Managed {
        length: u64,
        etag: BlobEtag,
        reader: BlobReader,
    },
    External(ExternalBlobRef),
}

/// One Blob cell resolved against an immutable graph read target.
#[derive(Debug, Clone)]
pub struct BlobRead {
    pub cell: BlobCell,
    pub resolved_target: ResolvedTarget,
    pub content: BlobContent,
}

/// Bound unavoidable URI parser/builder scratch before any caller copies or
/// interprets an external reference.
pub(crate) fn validate_external_blob_uri_raw_limit(raw: &str) -> Result<()> {
    let raw_bytes = u64::try_from(raw.len()).unwrap_or(u64::MAX);
    if raw_bytes > EXTERNAL_BLOB_URI_MAX_BYTES {
        return Err(OmniError::resource_limit(
            EXTERNAL_BLOB_URI_BYTES_RESOURCE,
            EXTERNAL_BLOB_URI_MAX_BYTES,
            raw_bytes,
        ));
    }
    Ok(())
}

/// Reject URI spellings that Lance's Blob builder would otherwise turn into
/// a storage-layer error before operation-wide policy admission runs.
pub(crate) fn validate_external_blob_uri_builder_input(raw: &str) -> Result<()> {
    validate_external_blob_uri_raw_limit(raw)?;
    if raw.is_empty() {
        return Err(policy_error("external Blob URI must be non-empty"));
    }
    Ok(())
}

/// Where an external Blob base is safe to dereference.
///
/// `ServerSafe` is deliberately narrower than "the URI parses": it currently
/// admits only S3 because that is the only remote object-store provider built
/// into OmniGraph. `EmbeddedOnly` additionally admits an exact local
/// `file://` directory. Server boot filters a graph policy to `ServerSafe`
/// before installing it on the engine handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalBlobExecutionScope {
    ServerSafe,
    EmbeddedOnly,
}

/// One normalized URI-component base allowed for new external Blob ingress.
///
/// Fields stay private so callers cannot accidentally implement containment
/// with string-prefix comparison. Use [`Self::new`] and the accessors instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalBlobBase {
    /// Normalized operator spelling used for the no-I/O containment gate.
    uri: String,
    scope: ExternalBlobExecutionScope,
    /// Canonical filesystem directory used for the final containment proof.
    /// Derived again during policy validation rather than persisted so server
    /// projection can discard embedded-only bases without touching local paths.
    #[serde(skip)]
    canonical_uri: Option<String>,
}

impl ExternalBlobBase {
    /// Parse, normalize, and validate an external Blob base.
    pub fn new(uri: impl AsRef<str>, scope: ExternalBlobExecutionScope) -> Result<Self> {
        let lexical = NormalizedExternalUri::parse(uri.as_ref(), UriRole::Base)?;
        if scope == ExternalBlobExecutionScope::ServerSafe && lexical.scheme == "file" {
            return Err(policy_error(
                "a server-safe external Blob base may not use file://",
            ));
        }
        let canonical = if lexical.scheme == "file" {
            Some(lexical.clone().canonical_file_base()?)
        } else {
            None
        };
        Ok(Self {
            uri: lexical.base_uri(),
            scope,
            canonical_uri: canonical.map(|uri| uri.base_uri()),
        })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn scope(&self) -> ExternalBlobExecutionScope {
        self.scope
    }

    fn normalized(&self) -> Result<NormalizedExternalUri> {
        NormalizedExternalUri::parse(
            self.canonical_uri.as_deref().unwrap_or(&self.uri),
            UriRole::Base,
        )
    }

    fn lexical_normalized(&self) -> Result<NormalizedExternalUri> {
        NormalizedExternalUri::parse(&self.uri, UriRole::Base)
    }
}

/// Graph-level trust policy for new external Blob references.
///
/// This policy is independent of Cedar authorization: Cedar decides who may
/// write the graph, while this policy limits which resources any authorized
/// writer may cause the engine to probe or read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalBlobPolicy {
    #[default]
    Deny,
    Allow {
        bases: Vec<ExternalBlobBase>,
    },
}

impl ExternalBlobPolicy {
    /// Construct and validate an allow policy. An empty allow-list is rejected
    /// so configuration cannot carry two spellings for `Deny`.
    pub fn allow(bases: Vec<ExternalBlobBase>) -> Result<Self> {
        Self::Allow { bases }.validated()
    }

    /// Return one canonical policy, rejecting deserialized or manually-built
    /// bases that bypassed [`ExternalBlobBase::new`].
    pub fn validated(&self) -> Result<Self> {
        let Self::Allow { bases } = self else {
            return Ok(Self::Deny);
        };
        if bases.is_empty() {
            return Err(policy_error(
                "an allow policy must contain at least one external Blob base",
            ));
        }

        let mut normalized = bases
            .iter()
            .map(|base| ExternalBlobBase::new(&base.uri, base.scope))
            .collect::<Result<Vec<_>>>()?;
        normalized.sort_by(|left, right| {
            left.uri
                .cmp(&right.uri)
                .then_with(|| scope_order(left.scope).cmp(&scope_order(right.scope)))
        });

        for (index, left) in normalized.iter().enumerate() {
            let left_parts = left.normalized()?;
            for right in normalized.iter().skip(index + 1) {
                let right_parts = right.normalized()?;
                if left_parts.overlaps_base(&right_parts) {
                    return Err(policy_error(format!(
                        "external Blob bases '{}' and '{}' overlap; configure one unambiguous base",
                        left.uri, right.uri
                    )));
                }
            }
        }
        Ok(Self::Allow { bases: normalized })
    }

    /// Restrict a validated graph policy for a server execution context.
    /// Embedded-only entries are omitted, never promoted.
    pub fn server_safe_only(&self) -> Result<Self> {
        match self {
            Self::Deny => Ok(Self::Deny),
            Self::Allow { bases } => {
                let bases = bases
                    .iter()
                    .filter(|base| base.scope == ExternalBlobExecutionScope::ServerSafe)
                    .cloned()
                    .collect::<Vec<_>>();
                if bases.is_empty() {
                    Ok(Self::Deny)
                } else {
                    Self::Allow { bases }.validated()
                }
            }
        }
    }

    pub fn bases(&self) -> &[ExternalBlobBase] {
        match self {
            Self::Deny => &[],
            Self::Allow { bases } => bases,
        }
    }

    pub(crate) fn authorize(&self, uri: &str) -> Result<NormalizedExternalBlobUri> {
        let candidate = NormalizedExternalUri::parse(uri, UriRole::Input)?;
        let requested_uri = candidate.uri();
        let Self::Allow { bases } = self else {
            return Err(OmniError::external_blob_policy(
                requested_uri,
                "new external Blob URI ingress is denied for this graph",
            ));
        };
        let lexical_match = bases.iter().try_fold(false, |matched, base| {
            Ok::<_, OmniError>(
                matched
                    || base.lexical_normalized()?.contains(&candidate)
                    || base.normalized()?.contains(&candidate),
            )
        })?;
        if !lexical_match {
            return Err(OmniError::external_blob_policy(
                requested_uri,
                "URI is outside every configured external Blob base",
            ));
        }
        let candidate = if candidate.scheme == "file" {
            candidate.canonical_file_input()?
        } else {
            candidate
        };
        let normalized_uri = candidate.uri();
        for base in bases {
            let base = base.normalized()?;
            if base.contains(&candidate) {
                return Ok(NormalizedExternalBlobUri(normalized_uri));
            }
        }
        Err(OmniError::external_blob_policy(
            requested_uri,
            "canonical target escapes its configured external Blob base",
        ))
    }
}

fn policy_error(reason: impl Into<String>) -> OmniError {
    OmniError::external_blob_policy("<redacted>", reason)
}

fn scope_order(scope: ExternalBlobExecutionScope) -> u8 {
    match scope {
        ExternalBlobExecutionScope::ServerSafe => 0,
        ExternalBlobExecutionScope::EmbeddedOnly => 1,
    }
}

/// Canonical URI admitted by a graph's external Blob policy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct NormalizedExternalBlobUri(String);

impl NormalizedExternalBlobUri {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy)]
enum UriRole {
    Base,
    Input,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedExternalUri {
    scheme: String,
    authority: String,
    path: Vec<Vec<u8>>,
    trailing_slash: bool,
}

impl NormalizedExternalUri {
    fn parse(raw: &str, role: UriRole) -> Result<Self> {
        validate_external_blob_uri_raw_limit(raw)?;
        if raw.trim() != raw || raw.is_empty() {
            return Err(policy_error(
                "external Blob URI must be non-empty and contain no surrounding whitespace",
            ));
        }
        if raw.contains('\\') {
            return Err(policy_error(
                "external Blob URI paths may not contain backslashes",
            ));
        }
        reject_raw_dot_path_components(raw)?;
        let parsed = url::Url::parse(raw).map_err(|error| {
            policy_error(format!(
                "external Blob URI is not a valid absolute URI: {error}"
            ))
        })?;
        if parsed.cannot_be_a_base() {
            return Err(policy_error(
                "external Blob URI must use a hierarchical storage scheme",
            ));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(policy_error(
                "external Blob URI may not contain user-info credentials",
            ));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(policy_error(
                "external Blob URI may not contain a query or fragment",
            ));
        }
        if parsed.port().is_some() {
            return Err(policy_error(
                "external Blob URI may not contain an explicit port",
            ));
        }

        let scheme = parsed.scheme().to_ascii_lowercase();
        let authority = match scheme.as_str() {
            "s3" => parsed
                .host_str()
                .filter(|host| !host.is_empty())
                .map(str::to_ascii_lowercase)
                .ok_or_else(|| policy_error("an s3 external Blob URI must contain a bucket"))?,
            "file" => {
                if parsed.host_str().is_some_and(|host| !host.is_empty()) {
                    return Err(policy_error(
                        "file external Blob URIs must be local and may not contain a host",
                    ));
                }
                String::new()
            }
            _ => {
                return Err(policy_error(format!(
                    "external Blob URI scheme '{scheme}' is not supported by this build"
                )));
            }
        };

        let encoded_path = parsed.path();
        if !encoded_path.starts_with('/') {
            return Err(policy_error(
                "external Blob URI must contain an absolute path",
            ));
        }
        let trailing_slash = encoded_path.ends_with('/');
        let body = encoded_path.strip_prefix('/').unwrap_or(encoded_path);
        let raw_segments = if body.is_empty() {
            Vec::new()
        } else {
            body.split('/').collect::<Vec<_>>()
        };
        let ambiguous_empty_component = raw_segments.iter().enumerate().any(|(index, segment)| {
            segment.is_empty()
                && !(matches!(role, UriRole::Base) && index + 1 == raw_segments.len())
        });
        if ambiguous_empty_component {
            return Err(policy_error(
                "external Blob URI contains an ambiguous empty path component",
            ));
        }
        let mut path = raw_segments
            .iter()
            .map(|segment| decode_path_component(segment))
            .collect::<Result<Vec<_>>>()?;
        if matches!(role, UriRole::Base) && path.last().is_some_and(Vec::is_empty) {
            path.pop();
        }
        if matches!(role, UriRole::Base) && path.iter().any(Vec::is_empty) {
            return Err(policy_error(
                "external Blob base contains an ambiguous empty path component",
            ));
        }

        Ok(Self {
            scheme,
            authority,
            path,
            trailing_slash,
        })
    }

    fn canonical_file_base(self) -> Result<Self> {
        let uri = self.uri();
        let path = file_path_from_normalized_uri(&uri)?;
        let canonical = std::fs::canonicalize(&path).map_err(|error| {
            policy_error(format!(
                "embedded file external Blob base must be an existing directory: {error}"
            ))
        })?;
        if !canonical.is_dir() {
            return Err(policy_error(
                "embedded file external Blob base must name a directory",
            ));
        }
        let canonical_uri = url::Url::from_directory_path(&canonical).map_err(|_| {
            policy_error("could not represent canonical external Blob base as file URI")
        })?;
        Self::parse(canonical_uri.as_str(), UriRole::Base)
    }

    fn canonical_file_input(self) -> Result<Self> {
        let uri = self.uri();
        let path = file_path_from_normalized_uri(&uri)?;
        let canonical = std::fs::canonicalize(&path).map_err(|error| {
            OmniError::external_blob_source(
                &uri,
                format!("could not resolve external Blob file target: {error}"),
            )
        })?;
        let metadata = std::fs::metadata(&canonical).map_err(|error| {
            OmniError::external_blob_source(
                &uri,
                format!("could not inspect external Blob file target: {error}"),
            )
        })?;
        if !metadata.is_file() {
            return Err(OmniError::external_blob_source(
                &uri,
                "external Blob file target must name a regular file",
            ));
        }
        let canonical_uri = url::Url::from_file_path(&canonical).map_err(|_| {
            OmniError::external_blob_source(
                &uri,
                "could not represent canonical external Blob target as file URI",
            )
        })?;
        Self::parse(canonical_uri.as_str(), UriRole::Input)
    }

    fn uri(&self) -> String {
        self.render(false)
    }

    fn base_uri(&self) -> String {
        self.render(true)
    }

    fn render(&self, as_base: bool) -> String {
        let mut uri = if self.scheme == "file" {
            "file://".to_string()
        } else {
            format!("{}://{}", self.scheme, self.authority)
        };
        uri.push('/');
        uri.push_str(
            &self
                .path
                .iter()
                .map(|segment| encode_path_component(segment))
                .collect::<Vec<_>>()
                .join("/"),
        );
        if (as_base || self.trailing_slash) && !uri.ends_with('/') {
            uri.push('/');
        }
        uri
    }

    fn contains(&self, candidate: &Self) -> bool {
        self.scheme == candidate.scheme
            && self.authority == candidate.authority
            && candidate.path.len() > self.path.len()
            && candidate.path.starts_with(&self.path)
    }

    fn overlaps_base(&self, other: &Self) -> bool {
        self.scheme == other.scheme
            && self.authority == other.authority
            && (self.path.starts_with(&other.path) || other.path.starts_with(&self.path))
    }
}

fn reject_raw_dot_path_components(raw: &str) -> Result<()> {
    let Some(scheme_end) = raw.find("://") else {
        return Ok(());
    };
    let after_authority = &raw[scheme_end + 3..];
    let Some(path_start) = after_authority.find('/') else {
        return Ok(());
    };
    let raw_path = &after_authority[path_start..];
    let path_end = raw_path.find(['?', '#']).unwrap_or(raw_path.len());
    for component in raw_path[..path_end].split('/') {
        // Run the same exact component decoder before `url::Url` gets a chance
        // to normalize `.` / `..` (including percent-encoded spellings) away.
        // Other malformed escapes are also safer to reject at the raw boundary.
        decode_path_component(component)?;
    }
    Ok(())
}

fn file_path_from_normalized_uri(uri: &str) -> Result<std::path::PathBuf> {
    url::Url::parse(uri)
        .ok()
        .and_then(|parsed| parsed.to_file_path().ok())
        .ok_or_else(|| policy_error("external Blob file URI is not an absolute local path"))
}

fn decode_path_component(component: &str) -> Result<Vec<u8>> {
    let source = component.as_bytes();
    let mut decoded = Vec::with_capacity(source.len());
    let mut index = 0;
    while index < source.len() {
        if source[index] == b'%' {
            if index + 2 >= source.len() {
                return Err(policy_error(
                    "external Blob URI contains an incomplete percent escape",
                ));
            }
            let high = hex_value(source[index + 1]).ok_or_else(|| {
                policy_error("external Blob URI contains an invalid percent escape")
            })?;
            let low = hex_value(source[index + 2]).ok_or_else(|| {
                policy_error("external Blob URI contains an invalid percent escape")
            })?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(source[index]);
            index += 1;
        }
    }
    if decoded == b"." || decoded == b".." {
        return Err(policy_error(
            "external Blob URI contains a dot path component",
        ));
    }
    if decoded
        .iter()
        .any(|byte| matches!(*byte, 0 | b'%' | b'/' | b'\\'))
    {
        return Err(policy_error(
            "external Blob URI contains an encoded separator, percent sign, or NUL",
        ));
    }
    Ok(decoded)
}

fn encode_path_component(component: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(component.len());
    for byte in component {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Logical state decoded from one persisted Blob-v2 descriptor.
///
/// Physical managed placement (inline, packed, or dedicated) is deliberately
/// not exposed. It is Lance-owned derived state and is irrelevant to callers
/// deciding whether a logical cell is null, managed, or external.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlobDescriptor {
    Null,
    Managed {
        length: u64,
    },
    External {
        uri: String,
        offset: u64,
        length: Option<u64>,
    },
}

/// A schema-validated view over one Arrow batch of Blob-v2 descriptions.
///
/// Construction validates the exact five-child v2 shape once. Per-row
/// classification then validates child validity and range arithmetic without
/// repeating schema lookup/downcasts for every row.
#[derive(Debug)]
pub(crate) struct BlobDescriptorDecoder<'a> {
    descriptions: &'a StructArray,
    kinds: &'a UInt8Array,
    positions: &'a UInt64Array,
    sizes: &'a UInt64Array,
    blob_ids: &'a UInt32Array,
    blob_uris: &'a StringArray,
}

impl<'a> BlobDescriptorDecoder<'a> {
    pub(crate) fn try_new(descriptions: &'a StructArray) -> Result<Self> {
        const EXPECTED: [(&str, DataType); 5] = [
            ("kind", DataType::UInt8),
            ("position", DataType::UInt64),
            ("size", DataType::UInt64),
            ("blob_id", DataType::UInt32),
            ("blob_uri", DataType::Utf8),
        ];

        let fields = descriptions.fields();
        let shape_matches = fields.len() == EXPECTED.len()
            && fields
                .iter()
                .zip(EXPECTED.iter())
                .all(|(actual, (name, data_type))| {
                    actual.name() == *name
                        && actual.data_type() == data_type
                        && !actual.is_nullable()
                });
        if !shape_matches {
            return Err(malformed_descriptor(format!(
                "expected exact children kind:UInt8, position:UInt64, size:UInt64, \
                 blob_id:UInt32, blob_uri:Utf8 (all non-nullable), got {:?}",
                fields
            )));
        }

        let kinds = descriptions
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| malformed_descriptor("kind child is not UInt8"))?;
        let positions = descriptions
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| malformed_descriptor("position child is not UInt64"))?;
        let sizes = descriptions
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| malformed_descriptor("size child is not UInt64"))?;
        let blob_ids = descriptions
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| malformed_descriptor("blob_id child is not UInt32"))?;
        let blob_uris = descriptions
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| malformed_descriptor("blob_uri child is not Utf8"))?;

        Ok(Self {
            descriptions,
            kinds,
            positions,
            sizes,
            blob_ids,
            blob_uris,
        })
    }

    pub(crate) fn classify(&self, row: usize) -> Result<BlobDescriptor> {
        if row >= self.descriptions.len() {
            return Err(malformed_descriptor(format!(
                "row {row} is outside descriptor batch of length {}",
                self.descriptions.len()
            )));
        }

        // This check must precede all child inspection. Lance encodes sentinel
        // child values for null parents, and those values carry no logical
        // meaning. Conversely, a non-null parent with a null child is corrupt;
        // child nullness must never become an alternate null representation.
        if self.descriptions.is_null(row) {
            return Ok(BlobDescriptor::Null);
        }

        for (name, child) in [
            ("kind", self.kinds as &dyn Array),
            ("position", self.positions as &dyn Array),
            ("size", self.sizes as &dyn Array),
            ("blob_id", self.blob_ids as &dyn Array),
            ("blob_uri", self.blob_uris as &dyn Array),
        ] {
            if child.is_null(row) {
                return Err(malformed_descriptor(format!(
                    "non-null row {row} has null child '{name}'"
                )));
            }
        }

        let position = self.positions.value(row);
        let size = self.sizes.value(row);
        let _end = position.checked_add(size).ok_or_else(|| {
            malformed_descriptor(format!(
                "row {row} range overflows u64: position={position}, size={size}"
            ))
        })?;

        let blob_id = self.blob_ids.value(row);
        let blob_uri = self.blob_uris.value(row);

        match self.kinds.value(row) {
            // Inline, packed, and dedicated are one logical Managed state, but
            // each physical discriminator owns exact sentinel fields. Fields
            // ignored by Lance must still be canonical here; otherwise corrupt
            // persisted state could be normalized into plausible bytes.
            0 => {
                if blob_id != 0 {
                    return Err(malformed_descriptor(format!(
                        "inline row {row} uses nonzero blob_id {blob_id}"
                    )));
                }
                if !blob_uri.is_empty() {
                    return Err(malformed_descriptor(format!(
                        "managed row {row} has non-empty blob_uri"
                    )));
                }
                Ok(BlobDescriptor::Managed { length: size })
            }
            1 => {
                if blob_id == 0 {
                    return Err(malformed_descriptor(format!(
                        "packed row {row} uses reserved blob_id 0"
                    )));
                }
                if !blob_uri.is_empty() {
                    return Err(malformed_descriptor(format!(
                        "managed row {row} has non-empty blob_uri"
                    )));
                }
                Ok(BlobDescriptor::Managed { length: size })
            }
            2 => {
                if blob_id == 0 {
                    return Err(malformed_descriptor(format!(
                        "dedicated row {row} uses reserved blob_id 0"
                    )));
                }
                if position != 0 {
                    return Err(malformed_descriptor(format!(
                        "dedicated row {row} uses nonzero position {position}"
                    )));
                }
                if !blob_uri.is_empty() {
                    return Err(malformed_descriptor(format!(
                        "managed row {row} has non-empty blob_uri"
                    )));
                }
                Ok(BlobDescriptor::Managed { length: size })
            }
            3 => {
                if blob_id != 0 {
                    return Err(malformed_descriptor(format!(
                        "external row {row} uses unsupported base-relative blob_id {blob_id}"
                    )));
                }
                validate_external_blob_uri_raw_limit(blob_uri).map_err(|error| {
                    malformed_descriptor(format!(
                        "external row {row} blob_uri exceeds the persisted URI contract: {error}"
                    ))
                })?;
                url::Url::parse(blob_uri).map_err(|error| {
                    malformed_descriptor(format!(
                        "external row {row} blob_uri is not an absolute URI: {error}"
                    ))
                })?;
                Ok(BlobDescriptor::External {
                    uri: blob_uri.to_owned(),
                    offset: position,
                    // Lance persists zero when an external length is unknown.
                    length: (size != 0).then_some(size),
                })
            }
            kind => Err(malformed_descriptor(format!(
                "row {row} has unknown Blob-v2 kind {kind}"
            ))),
        }
    }

    /// Canonical payload identity of one descriptor row, qualified by the
    /// owning immutable data file. Equal identities reference identical stored
    /// bytes, so a comparator can skip payload I/O on equality; inequality
    /// proves nothing (compaction relocates identical bytes to a new file), so
    /// callers must byte-compare payloads before reporting a change.
    ///
    /// **A managed descriptor's fields are file-relative, not global.** Lance
    /// resolves managed (inline/packed/dedicated) bytes from the row's owning
    /// data file, then `position`/`size`/`blob_id` within it. So the identity
    /// must be qualified by *which* data file. It uses the file's stable path
    /// (`data_file_path`) — a per-file v4 UUID Lance mints once and never
    /// rewrites; compaction, `Overwrite`, and per-branch writes all produce a
    /// NEW UUID. Unlike the numeric fragment id, this is globally unique: it
    /// does not restart at 0 on `Overwrite` and is not branch-local, so equal
    /// managed identities really do imply equal bytes across overwrites and
    /// branches. External references resolve by URI independently of physical
    /// placement, so their identity is source-independent. Null uses the
    /// classified state, never sentinel child values, so the two physical null
    /// encodings share one identity.
    pub(crate) fn physical_identity(&self, row: usize, data_file_path: &str) -> Result<String> {
        match self.classify(row)? {
            BlobDescriptor::Null => Ok("null".to_string()),
            BlobDescriptor::External {
                uri,
                offset,
                length,
            } => Ok(format!(
                "ext:{offset}:{}:{uri}",
                length.map(|len| len.to_string()).unwrap_or_default(),
            )),
            BlobDescriptor::Managed { .. } => Ok(format!(
                "mgd:{data_file_path}:{}:{}:{}:{}",
                self.kinds.value(row),
                self.positions.value(row),
                self.sizes.value(row),
                self.blob_ids.value(row),
            )),
        }
    }
}

struct ResolvedBlobCell {
    table_key: String,
    stable_table_id: u64,
    table_incarnation_id: u64,
    stable_property_id: u64,
}

impl Omnigraph {
    /// Resolve and read one Blob cell against a branch or immutable snapshot.
    ///
    /// External references are returned descriptor-only, without policy
    /// evaluation, object-store setup, metadata probes, or payload reads.
    pub async fn read_blob_at(
        &self,
        target: impl Into<ReadTarget>,
        cell: BlobCell,
    ) -> Result<BlobRead> {
        let requested_target = target.into();
        let (resolved_target, catalog) = self.capture_read_view(requested_target.clone()).await?;
        let resolved_cell = resolve_blob_cell(&catalog, &cell)?;

        // Resolving an old commit reopens its persisted `(manifest branch,
        // version)`. A deleted and recreated named branch can reuse both, so
        // prove that the reopened graph snapshot still contains the requested
        // commit before trusting any of its table entries. Main snapshots are
        // checked too; this keeps the rule structural even though main cannot
        // undergo branch-name ABA.
        if matches!(requested_target, ReadTarget::Snapshot(_))
            && resolved_target
                .snapshot
                .graph_head(resolved_target.branch.as_deref())
                != resolved_target.graph_commit_id.as_deref()
        {
            return Err(OmniError::manifest(format!(
                "Blob property '{}.{}' has no persisted native-branch incarnation witness at the selected target",
                cell.type_name, cell.property
            )));
        }

        let entry = resolved_target
            .snapshot
            .dataset(&resolved_cell.table_key)
            .ok_or_else(|| {
                OmniError::manifest(format!(
                    "{} type '{}' is unavailable at the selected target",
                    entity_label(cell.entity),
                    cell.type_name
                ))
            })?;
        if entry.identity.stable_table_id != resolved_cell.stable_table_id
            || entry.identity.table_incarnation_id != resolved_cell.table_incarnation_id
        {
            return Err(OmniError::blob_integrity(format!(
                "selected dataset for {} type '{}' has identity {}, expected {:016x}:{:016x}",
                entity_label(cell.entity),
                cell.type_name,
                entry.identity,
                resolved_cell.stable_table_id,
                resolved_cell.table_incarnation_id
            )));
        }
        let expected_table_version = entry.published_dataset_version;
        let stable_table_id = entry.identity.stable_table_id;
        let table_incarnation_id = entry.identity.table_incarnation_id;

        crate::failpoints::maybe_fail(crate::failpoints::names::BLOB_READ_POST_CAPTURE)?;

        let dataset = Arc::new(if entry.native_dataset_branch.is_some() {
            // Local filesystems provide no manifest e-tag, so the ordinary
            // live-read cache key cannot distinguish delete/recreate ABA at the
            // same named branch and numeric table version. Blob reads need a
            // coherent physical-incarnation witness; bypass the held handle for
            // named-native-branch tables and prove the graph ref again below.
            entry.open(self.uri(), None).await?
        } else {
            resolved_target
                .snapshot
                .open_lance_dataset(&resolved_cell.table_key)
                .await?
        });
        let actual_table_version = dataset.version().version;
        if actual_table_version != expected_table_version {
            return Err(OmniError::blob_integrity(format!(
                "selected dataset for {} type '{}' opened at Lance version {}, expected published dataset version {}",
                entity_label(cell.entity),
                cell.type_name,
                actual_table_version,
                expected_table_version
            )));
        }
        if entry.native_dataset_branch.is_some() {
            // A named Lance branch can be deleted and recreated at the same
            // path and numeric version. Neither V6 graph entries nor Lance's
            // local manifest e-tag persist a branch-incarnation witness strong
            // enough to distinguish that ABA. Re-proving the effective current
            // graph head *after* the table open therefore covers both explicit
            // branch-owned snapshots and a live-branch capture racing
            // delete/recreate. A concurrent ordinary branch advance may also
            // make this read fail loudly; it never retargets.
            // This proof must bypass the handle's warm read coordinator: a
            // delete/recreate can reuse the same native path and version while
            // that coordinator still holds the old graph head. Control-plane
            // opens use the zero-cache Lance session, so this observes the
            // selected graph ref that exists now. Its effective head is exact
            // once materialized and inherited on a fresh fork.
            let live_branch = resolved_target.branch.as_deref().ok_or_else(|| {
                OmniError::blob_integrity(format!(
                    "selected named-branch dataset for {} type '{}' has no graph branch identity",
                    entity_label(cell.entity),
                    cell.type_name
                ))
            })?;
            let expected_graph_head = resolved_target.graph_commit_id.as_deref().ok_or_else(|| {
                OmniError::manifest(format!(
                    "Blob property '{}.{}' has no persisted native-branch incarnation witness at the selected target",
                    cell.type_name, cell.property
                ))
            })?;
            let live_coordinator = self.open_coordinator_for_branch(Some(live_branch)).await?;
            if live_coordinator.effective_graph_head().await?.as_deref()
                != Some(expected_graph_head)
            {
                return Err(OmniError::manifest(format!(
                    "Blob property '{}.{}' has no persisted native-branch incarnation witness at the selected target",
                    cell.type_name, cell.property
                )));
            }
        }
        if let Some(expected_e_tag) = entry.version_metadata.e_tag()
            && dataset.manifest_location().e_tag.as_deref() != Some(expected_e_tag)
        {
            return Err(OmniError::blob_integrity(format!(
                "selected dataset for {} type '{}' opened a different Lance manifest incarnation at published dataset version {}",
                entity_label(cell.entity),
                cell.type_name,
                expected_table_version
            )));
        }

        let field = dataset.schema().field(&cell.property).ok_or_else(|| {
            OmniError::manifest(format!(
                "Blob property '{}.{}' is unavailable at the selected target",
                cell.type_name, cell.property
            ))
        })?;
        if !field.is_blob() {
            return Err(OmniError::blob_integrity(format!(
                "catalog Blob property '{}.{}' is not physically encoded as Blob-v2",
                cell.type_name, cell.property
            )));
        }
        match field
            .metadata
            .get(crate::db::STABLE_PROPERTY_ID_METADATA_KEY)
        {
            Some(persisted_id) => {
                let persisted_id = persisted_id.parse::<u64>().map_err(|error| {
                    OmniError::blob_integrity(format!(
                        "Blob property '{}.{}' has invalid persisted stable-property identity: {error}",
                        cell.type_name, cell.property
                    ))
                })?;
                if persisted_id != resolved_cell.stable_property_id {
                    return Err(OmniError::manifest(format!(
                        "Blob property '{}.{}' belongs to a different property lifetime at the selected target",
                        cell.type_name, cell.property
                    )));
                }
            }
            None if matches!(requested_target, ReadTarget::Snapshot(_)) => {
                // Pre-0.10 v6 tables did not persist stable property identity.
                // A snapshot at the exact current physical table version is
                // still covered by current-catalog validation.  Older physical
                // versions cannot prove that an identically-spelled field did
                // not cross a drop/re-add boundary, so fail closed instead of
                // inferring identity from Lance field ids or positions.
                let live_branch = resolved_target.branch.as_deref().unwrap_or("main");
                let (live_target, live_catalog) = self
                    .capture_read_view(ReadTarget::branch(live_branch))
                    .await?;
                let live_cell = resolve_blob_cell(&live_catalog, &cell)?;
                let live_entry = live_target
                    .snapshot
                    .dataset(&live_cell.table_key)
                    .ok_or_else(|| {
                        OmniError::manifest(format!(
                            "{} type '{}' is unavailable at the current branch head",
                            entity_label(cell.entity),
                            cell.type_name
                        ))
                    })?;
                if live_cell.stable_table_id != resolved_cell.stable_table_id
                    || live_cell.table_incarnation_id != resolved_cell.table_incarnation_id
                    || live_cell.stable_property_id != resolved_cell.stable_property_id
                    || live_entry.published_dataset_version != entry.published_dataset_version
                    || live_entry.native_dataset_branch != entry.native_dataset_branch
                    || live_entry.version_metadata != entry.version_metadata
                {
                    return Err(OmniError::manifest(format!(
                        "Blob property '{}.{}' has no persisted property-lifetime witness at the selected target",
                        cell.type_name, cell.property
                    )));
                }
            }
            None => {}
        }

        let mut scanner = dataset.scan();
        scanner
            .project(&[cell.property.as_str()])
            .map_err(OmniError::storage)?;
        scanner.filter_expr(col(catalog.system_columns.id).eq(lit(cell.id.clone())));
        scanner.blob_handling(BlobHandling::BlobsDescriptions);
        scanner.with_row_id();
        scanner.limit(Some(2), None).map_err(OmniError::storage)?;
        let stream = scanner
            .try_into_stream()
            .await
            .map_err(OmniError::storage)?;
        let batches = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(OmniError::storage)?;

        let mut selected = None;
        for batch in &batches {
            let descriptions = batch
                .column_by_name(&cell.property)
                .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                .ok_or_else(|| {
                    OmniError::blob_integrity(format!(
                        "Blob property '{}.{}' did not scan as a Blob-v2 descriptor",
                        cell.type_name, cell.property
                    ))
                })?;
            let row_ids = batch
                .column_by_name("_rowid")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    OmniError::blob_integrity(format!(
                        "Blob lookup for '{}.{}' omitted its stable row id",
                        cell.type_name, cell.property
                    ))
                })?;
            if descriptions.len() != row_ids.len() {
                return Err(OmniError::blob_integrity(format!(
                    "Blob descriptor and stable-row-id cardinalities differ for '{}.{}'",
                    cell.type_name, cell.property
                )));
            }
            let decoder = BlobDescriptorDecoder::try_new(descriptions)?;
            for row in 0..batch.num_rows() {
                if selected.is_some() {
                    return Err(OmniError::blob_integrity(format!(
                        "entity id '{}' appears more than once in {} type '{}'",
                        cell.id,
                        entity_label(cell.entity),
                        cell.type_name
                    )));
                }
                if row_ids.is_null(row) {
                    return Err(OmniError::blob_integrity(format!(
                        "Blob lookup for '{}.{}' returned a null stable row id",
                        cell.type_name, cell.property
                    )));
                }
                selected = Some((row_ids.value(row), decoder.classify(row)?));
            }
        }

        let (stable_row_id, descriptor) = selected.ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "no {} '{}' with id '{}' found",
                entity_label(cell.entity),
                cell.type_name,
                cell.id
            ))
        })?;

        let content = match descriptor {
            BlobDescriptor::Null => {
                return Err(OmniError::manifest_not_found(format!(
                    "Blob property '{}.{}' is null for id '{}'",
                    cell.type_name, cell.property, cell.id
                )));
            }
            BlobDescriptor::External {
                uri,
                offset,
                length,
            } => BlobContent::External(ExternalBlobRef {
                uri,
                offset,
                length,
            }),
            BlobDescriptor::Managed { length } => {
                // Lance's branch identifier is resolved from the *current*
                // named ref and can change underneath an older pinned Dataset
                // after branch delete/recreate. The transaction-file identity
                // is stored in this exact immutable manifest and carries the
                // UUID of the transaction that produced this table version.
                let transaction_file = immutable_transaction_witness(
                    dataset.manifest().transaction_file.as_deref(),
                    &cell,
                )?;
                let mut files = dataset
                    .take_blobs(&[stable_row_id], &cell.property)
                    .await
                    .map_err(OmniError::storage)?;
                if files.len() != 1 {
                    return Err(OmniError::blob_integrity(format!(
                        "managed Blob lookup returned {} slots instead of one",
                        files.len()
                    )));
                }
                let file = files.pop().flatten().ok_or_else(|| {
                    OmniError::blob_integrity(
                        "a non-null managed Blob descriptor resolved to a null payload slot",
                    )
                })?;
                if file.kind() == BlobKind::External || file.uri().is_some() {
                    return Err(OmniError::blob_integrity(
                        "a managed Blob descriptor resolved to an external payload handle",
                    ));
                }
                if file.size() != length {
                    return Err(OmniError::blob_integrity(format!(
                        "managed Blob descriptor length {} disagrees with payload length {}",
                        length,
                        file.size()
                    )));
                }
                BlobContent::Managed {
                    length,
                    etag: blob_etag(
                        stable_table_id,
                        table_incarnation_id,
                        resolved_cell.stable_property_id,
                        actual_table_version,
                        stable_row_id,
                        transaction_file,
                    ),
                    reader: BlobReader {
                        _dataset: Arc::clone(&dataset),
                        file: Arc::new(file),
                        length,
                    },
                }
            }
        };

        Ok(BlobRead {
            cell,
            resolved_target,
            content,
        })
    }
}

fn resolve_blob_cell(
    catalog: &omnigraph_compiler::catalog::Catalog,
    cell: &BlobCell,
) -> Result<ResolvedBlobCell> {
    let (table_prefix, blob_properties, stable_table_id, table_incarnation_id, property_id) =
        match cell.entity {
            EntityKind::Node => {
                let node = catalog.node_types.get(&cell.type_name).ok_or_else(|| {
                    OmniError::manifest(format!("unknown node type '{}'", cell.type_name))
                })?;
                (
                    "node",
                    &node.blob_properties,
                    catalog.node_type_id(&cell.type_name),
                    catalog.node_table_incarnation_id(&cell.type_name),
                    catalog.node_property_id(&cell.type_name, &cell.property),
                )
            }
            EntityKind::Edge => {
                let edge = catalog.edge_types.get(&cell.type_name).ok_or_else(|| {
                    OmniError::manifest(format!("unknown edge type '{}'", cell.type_name))
                })?;
                (
                    "edge",
                    &edge.blob_properties,
                    catalog.edge_type_id(&cell.type_name),
                    catalog.edge_table_incarnation_id(&cell.type_name),
                    catalog.edge_property_id(&cell.type_name, &cell.property),
                )
            }
        };

    if !blob_properties.contains(&cell.property) {
        return Err(OmniError::manifest(format!(
            "property '{}.{}' is not a Blob",
            cell.type_name, cell.property
        )));
    }
    let stable_table_id = stable_table_id.ok_or_else(|| {
        OmniError::blob_integrity(format!(
            "{} type '{}' lacks a stable type id",
            entity_label(cell.entity),
            cell.type_name
        ))
    })?;
    let table_incarnation_id = table_incarnation_id.ok_or_else(|| {
        OmniError::blob_integrity(format!(
            "{} type '{}' lacks a table incarnation id",
            entity_label(cell.entity),
            cell.type_name
        ))
    })?;
    let property_id = property_id.ok_or_else(|| {
        OmniError::blob_integrity(format!(
            "Blob property '{}.{}' lacks a stable property id",
            cell.type_name, cell.property
        ))
    })?;

    Ok(ResolvedBlobCell {
        table_key: format!("{table_prefix}:{}", cell.type_name),
        stable_table_id: stable_table_id.get(),
        table_incarnation_id: table_incarnation_id.get(),
        stable_property_id: property_id.get(),
    })
}

fn entity_label(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::Node => "node",
        EntityKind::Edge => "edge",
    }
}

fn immutable_transaction_witness<'a>(
    transaction_file: Option<&'a str>,
    cell: &BlobCell,
) -> Result<&'a str> {
    transaction_file
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            OmniError::blob_integrity(format!(
                "managed Blob '{}.{}' has no immutable Lance transaction witness",
                cell.type_name, cell.property
            ))
        })
}

fn blob_etag(
    stable_table_id: u64,
    table_incarnation_id: u64,
    stable_property_id: u64,
    table_version: u64,
    stable_row_id: u64,
    transaction_file: &str,
) -> BlobEtag {
    let mut digest = Sha256::new();
    digest.update(b"omnigraph/blob-etag/v1\0");
    for value in [
        stable_table_id,
        table_incarnation_id,
        stable_property_id,
        table_version,
        stable_row_id,
    ] {
        digest.update(value.to_be_bytes());
    }
    digest.update(
        u64::try_from(transaction_file.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    digest.update(transaction_file.as_bytes());
    let digest = digest.finalize();
    let mut value = String::with_capacity(34);
    value.push('"');
    for byte in digest.iter().take(16) {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value.push('"');
    BlobEtag(value)
}

fn malformed_descriptor(message: impl Into<String>) -> OmniError {
    OmniError::blob_integrity(format!("malformed Blob-v2 descriptor: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, StructArray};
    use arrow_schema::{Field, Fields};

    use super::*;

    fn assert_blob_integrity(error: OmniError, expected: &str) {
        match error {
            OmniError::BlobIntegrity { reason } => assert!(
                reason.contains(expected),
                "BlobIntegrity reason '{reason}' must contain '{expected}'"
            ),
            other => panic!("expected BlobIntegrity, got {other:?}"),
        }
    }

    #[test]
    fn blob_reader_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BlobReader>();
    }

    #[test]
    fn blob_etag_v1_uses_big_endian_identity_components() {
        let etag = blob_etag(1, 2, 3, 4, 5, "6-00000000000000000000000000000007.txn");
        assert_eq!(etag.as_str(), "\"f0e89bc86388accc9a7877df658a1f1c\"");
        assert_eq!(etag.to_string(), etag.clone().into_string());
    }

    #[test]
    fn managed_blob_requires_an_immutable_transaction_witness() {
        let cell = BlobCell {
            entity: EntityKind::Node,
            type_name: "Document".to_string(),
            id: "doc-1".to_string(),
            property: "content".to_string(),
        };
        for missing in [None, Some("")] {
            assert_blob_integrity(
                immutable_transaction_witness(missing, &cell).unwrap_err(),
                "no immutable Lance transaction witness",
            );
        }
        assert_eq!(
            immutable_transaction_witness(Some("opaque-version-witness"), &cell).unwrap(),
            "opaque-version-witness"
        );
    }

    fn assert_external_uri_byte_limit(role: UriRole) {
        let prefix = "s3://bucket/base/";
        let limit = usize::try_from(EXTERNAL_BLOB_URI_MAX_BYTES).unwrap();
        let mut exact = String::with_capacity(limit + 1);
        exact.push_str(prefix);
        exact.push_str(&"a".repeat(limit - prefix.len()));
        assert_eq!(exact.len(), limit);
        NormalizedExternalUri::parse(&exact, role).unwrap();

        exact.push('a');
        let error = NormalizedExternalUri::parse(&exact, role).unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: EXTERNAL_BLOB_URI_MAX_BYTES,
                actual,
            } if resource == EXTERNAL_BLOB_URI_BYTES_RESOURCE
                && actual == EXTERNAL_BLOB_URI_MAX_BYTES + 1
        ));
    }

    #[test]
    fn external_blob_base_uri_byte_limit_is_inclusive() {
        assert_external_uri_byte_limit(UriRole::Base);
    }

    #[test]
    fn external_blob_input_uri_byte_limit_is_inclusive() {
        assert_external_uri_byte_limit(UriRole::Input);
    }

    #[test]
    fn external_blob_policy_is_default_deny_and_uses_component_containment() {
        assert_eq!(ExternalBlobPolicy::default(), ExternalBlobPolicy::Deny);
        assert!(
            ExternalBlobPolicy::Deny
                .authorize("s3://bucket/allowed/object")
                .unwrap_err()
                .to_string()
                .contains("denied")
        );

        let policy = ExternalBlobPolicy::allow(vec![
            ExternalBlobBase::new(
                "s3://BUCKET/allowed",
                ExternalBlobExecutionScope::ServerSafe,
            )
            .unwrap(),
        ])
        .unwrap();
        assert_eq!(policy.bases()[0].uri(), "s3://bucket/allowed/");
        assert!(policy.authorize("s3://bucket/allowed").is_err());
        assert_eq!(
            policy
                .authorize("s3://bucket/allowed/%66ile")
                .unwrap()
                .as_str(),
            "s3://bucket/allowed/file"
        );
        assert!(policy.authorize("s3://bucket/allowed-child/file").is_err());
        assert!(policy.authorize("s3://other/allowed/file").is_err());
        assert!(policy.authorize("s3://bucket/allowed//file").is_err());
        assert!(policy.authorize("s3://bucket/allowed/%252Fescape").is_err());
    }

    #[test]
    fn external_blob_policy_rejects_credentials_ambiguity_and_overlapping_bases() {
        for uri in [
            " ",
            " s3://bucket/base",
            "s3://bucket/base ",
            "s3://user@bucket/base",
            "s3://bucket/base?token=secret",
            "s3://bucket/base#fragment",
            "s3://bucket/base/%2Fescape",
            "s3://bucket/base/%00escape",
            "s3://bucket/base//child",
            "s3://bucket/base/../escape",
            "s3://bucket/base/%2e%2e/escape",
            "s3://bucket/base/.%2E/escape",
        ] {
            assert!(
                ExternalBlobBase::new(uri, ExternalBlobExecutionScope::ServerSafe).is_err(),
                "{uri}"
            );
        }

        let parent =
            ExternalBlobBase::new("s3://bucket/base", ExternalBlobExecutionScope::ServerSafe)
                .unwrap();
        let child = ExternalBlobBase::new(
            "s3://bucket/base/child",
            ExternalBlobExecutionScope::EmbeddedOnly,
        )
        .unwrap();
        assert!(ExternalBlobPolicy::allow(vec![parent, child]).is_err());
    }

    #[test]
    fn external_blob_policy_keeps_file_embedded_only() {
        let directory = tempfile::tempdir().unwrap();
        let directory_uri = url::Url::from_directory_path(directory.path()).unwrap();
        let existing_error = ExternalBlobBase::new(
            directory_uri.as_str(),
            ExternalBlobExecutionScope::ServerSafe,
        )
        .unwrap_err();
        let unavailable_uri =
            url::Url::from_directory_path(directory.path().join("missing-server-safe-base"))
                .unwrap();
        let unavailable_error = ExternalBlobBase::new(
            unavailable_uri.as_str(),
            ExternalBlobExecutionScope::ServerSafe,
        )
        .unwrap_err();
        for error in [&existing_error, &unavailable_error] {
            assert!(matches!(
                error,
                OmniError::ExternalBlobPolicy { uri, reason }
                    if uri == "<redacted>"
                        && reason == "a server-safe external Blob base may not use file://"
            ));
        }
        assert_eq!(existing_error.to_string(), unavailable_error.to_string());
        let local = ExternalBlobBase::new(
            directory_uri.as_str(),
            ExternalBlobExecutionScope::EmbeddedOnly,
        )
        .unwrap();
        let policy = ExternalBlobPolicy::allow(vec![local]).unwrap();
        let object = directory.path().join("object");
        std::fs::write(&object, b"payload").unwrap();
        let object_uri = url::Url::from_file_path(object).unwrap();
        assert!(policy.authorize(object_uri.as_str()).is_ok());

        let child_directory = directory.path().join("child-directory");
        std::fs::create_dir(&child_directory).unwrap();
        let child_directory_uri = url::Url::from_directory_path(child_directory).unwrap();
        assert!(policy.authorize(child_directory_uri.as_str()).is_err());
        assert_eq!(policy.server_safe_only().unwrap(), ExternalBlobPolicy::Deny);

        // Server projection must not touch a persisted embedded-only path on
        // the server host. It is filtered before retained bases are validated.
        let unavailable_embedded = ExternalBlobPolicy::Allow {
            bases: vec![ExternalBlobBase {
                uri: "file:///definitely/not/present/omnigraph-blob-base/".to_string(),
                scope: ExternalBlobExecutionScope::EmbeddedOnly,
                canonical_uri: None,
            }],
        };
        assert_eq!(
            unavailable_embedded.server_safe_only().unwrap(),
            ExternalBlobPolicy::Deny
        );

        let missing = directory.path().join("missing");
        let missing_uri = url::Url::from_directory_path(missing).unwrap();
        assert!(
            ExternalBlobBase::new(
                missing_uri.as_str(),
                ExternalBlobExecutionScope::EmbeddedOnly,
            )
            .is_err()
        );
        let plain_file = directory.path().join("not-a-directory");
        std::fs::write(&plain_file, b"payload").unwrap();
        let plain_file_uri = url::Url::from_file_path(plain_file).unwrap();
        assert!(
            ExternalBlobBase::new(
                plain_file_uri.as_str(),
                ExternalBlobExecutionScope::EmbeddedOnly,
            )
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn external_blob_file_policy_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let allowed = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_object = outside.path().join("secret");
        std::fs::write(&outside_object, b"secret").unwrap();
        let link = allowed.path().join("escape");
        symlink(&outside_object, &link).unwrap();

        let base_uri = url::Url::from_directory_path(allowed.path()).unwrap();
        let policy = ExternalBlobPolicy::allow(vec![
            ExternalBlobBase::new(base_uri.as_str(), ExternalBlobExecutionScope::EmbeddedOnly)
                .unwrap(),
        ])
        .unwrap();
        let link_uri = url::Url::from_file_path(link).unwrap();
        assert!(policy.authorize(link_uri.as_str()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn external_blob_file_policy_rejects_special_files() {
        use std::os::unix::net::UnixListener;

        let allowed = tempfile::tempdir().unwrap();
        let socket = allowed.path().join("socket");
        let _listener = UnixListener::bind(&socket).unwrap();
        let base_uri = url::Url::from_directory_path(allowed.path()).unwrap();
        let policy = ExternalBlobPolicy::allow(vec![
            ExternalBlobBase::new(base_uri.as_str(), ExternalBlobExecutionScope::EmbeddedOnly)
                .unwrap(),
        ])
        .unwrap();
        let socket_uri = url::Url::from_file_path(socket).unwrap();

        assert!(policy.authorize(socket_uri.as_str()).is_err());
    }

    fn fields() -> Fields {
        Fields::from(vec![
            Field::new("kind", DataType::UInt8, false),
            Field::new("position", DataType::UInt64, false),
            Field::new("size", DataType::UInt64, false),
            Field::new("blob_id", DataType::UInt32, false),
            Field::new("blob_uri", DataType::Utf8, false),
        ])
    }

    fn descriptor(
        kind: Option<u8>,
        position: Option<u64>,
        size: Option<u64>,
        blob_id: Option<u32>,
        blob_uri: Option<&str>,
    ) -> StructArray {
        StructArray::new(
            fields(),
            vec![
                Arc::new(UInt8Array::from(vec![kind])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![position])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![size])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![blob_id])) as ArrayRef,
                Arc::new(StringArray::from(vec![blob_uri])) as ArrayRef,
            ],
            None,
        )
    }

    #[test]
    fn parent_validity_is_the_only_null_authority() {
        let null = StructArray::new_null(fields(), 1);
        let decoder = BlobDescriptorDecoder::try_new(&null).unwrap();
        assert_eq!(decoder.classify(0).unwrap(), BlobDescriptor::Null);

        // A child-level null representation cannot become a second logical
        // null encoding. Safe Arrow construction requires such a child to be
        // declared nullable, and the decoder rejects that non-v2 shape before
        // any row can be classified.
        let nullable_fields = Fields::from(vec![
            Field::new("kind", DataType::UInt8, true),
            Field::new("position", DataType::UInt64, false),
            Field::new("size", DataType::UInt64, false),
            Field::new("blob_id", DataType::UInt32, false),
            Field::new("blob_uri", DataType::Utf8, false),
        ]);
        let child_null = StructArray::new(
            nullable_fields,
            vec![
                Arc::new(UInt8Array::from(vec![None])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                Arc::new(StringArray::from(vec![""])) as ArrayRef,
            ],
            None,
        );
        assert_blob_integrity(
            BlobDescriptorDecoder::try_new(&child_null).unwrap_err(),
            "expected exact children",
        );
    }

    #[test]
    fn non_null_empty_is_managed_and_all_v2_kinds_are_classified() {
        for kind in 0..=2 {
            let blob_id = if kind == 0 { 0 } else { 1 };
            let descriptions = descriptor(Some(kind), Some(0), Some(0), Some(blob_id), Some(""));
            let decoder = BlobDescriptorDecoder::try_new(&descriptions).unwrap();
            assert_eq!(
                decoder.classify(0).unwrap(),
                BlobDescriptor::Managed { length: 0 },
                "kind {kind}"
            );
        }

        let external = descriptor(
            Some(3),
            Some(4),
            Some(8),
            Some(0),
            Some("s3://bucket/object"),
        );
        let decoder = BlobDescriptorDecoder::try_new(&external).unwrap();
        assert_eq!(
            decoder.classify(0).unwrap(),
            BlobDescriptor::External {
                uri: "s3://bucket/object".to_owned(),
                offset: 4,
                length: Some(8),
            }
        );

        let unknown_length =
            descriptor(Some(3), Some(0), Some(0), Some(0), Some("file:///tmp/blob"));
        let decoder = BlobDescriptorDecoder::try_new(&unknown_length).unwrap();
        assert_eq!(
            decoder.classify(0).unwrap(),
            BlobDescriptor::External {
                uri: "file:///tmp/blob".to_owned(),
                offset: 0,
                length: None,
            }
        );
    }

    #[test]
    fn exact_v2_shape_unknown_kind_bounds_and_arithmetic_fail_closed() {
        for index in 0..5 {
            let mut wrong_fields = fields().iter().cloned().collect::<Vec<_>>();
            let field = wrong_fields[index].as_ref();
            wrong_fields[index] = Arc::new(Field::new(
                format!("wrong_{index}"),
                field.data_type().clone(),
                false,
            ));
            let values: Vec<ArrayRef> = vec![
                Arc::new(UInt8Array::from(vec![0])),
                Arc::new(UInt64Array::from(vec![0])),
                Arc::new(UInt64Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(StringArray::from(vec![""])),
            ];
            let wrong = StructArray::new(Fields::from(wrong_fields), values, None);
            assert!(
                BlobDescriptorDecoder::try_new(&wrong).is_err(),
                "child {index}"
            );
        }

        let wrong_type_fields = Fields::from(vec![
            Field::new("kind", DataType::UInt8, false),
            Field::new("position", DataType::UInt64, false),
            Field::new("size", DataType::UInt32, false),
            Field::new("blob_id", DataType::UInt32, false),
            Field::new("blob_uri", DataType::Utf8, false),
        ]);
        let wrong_type = StructArray::new(
            wrong_type_fields,
            vec![
                Arc::new(UInt8Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                Arc::new(StringArray::from(vec![""])) as ArrayRef,
            ],
            None,
        );
        assert_blob_integrity(
            BlobDescriptorDecoder::try_new(&wrong_type).unwrap_err(),
            "expected exact children",
        );

        let missing_child = StructArray::new(
            Fields::from(fields().iter().take(4).cloned().collect::<Vec<_>>()),
            vec![
                Arc::new(UInt8Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
            ],
            None,
        );
        assert_blob_integrity(
            BlobDescriptorDecoder::try_new(&missing_child).unwrap_err(),
            "expected exact children",
        );

        let unknown = descriptor(Some(4), Some(0), Some(0), Some(0), Some(""));
        let decoder = BlobDescriptorDecoder::try_new(&unknown).unwrap();
        assert_blob_integrity(decoder.classify(0).unwrap_err(), "unknown");

        let overflow = descriptor(Some(0), Some(u64::MAX), Some(1), Some(0), Some(""));
        let decoder = BlobDescriptorDecoder::try_new(&overflow).unwrap();
        assert_blob_integrity(decoder.classify(0).unwrap_err(), "overflows");
        assert_blob_integrity(decoder.classify(1).unwrap_err(), "outside");
    }

    #[test]
    fn managed_and_external_uri_invariants_fail_closed() {
        let inline_with_sidecar_id = descriptor(Some(0), Some(0), Some(1), Some(9), Some(""));
        let decoder = BlobDescriptorDecoder::try_new(&inline_with_sidecar_id).unwrap();
        assert_blob_integrity(
            decoder.classify(0).unwrap_err(),
            "inline row 0 uses nonzero blob_id",
        );

        for kind in [1, 2] {
            let reserved_id = descriptor(Some(kind), Some(0), Some(1), Some(0), Some(""));
            let decoder = BlobDescriptorDecoder::try_new(&reserved_id).unwrap();
            let error = decoder.classify(0).unwrap_err().to_string();
            assert!(
                error.contains("reserved blob_id 0"),
                "managed kind {kind}: {error}"
            );
        }

        let dedicated_with_position = descriptor(Some(2), Some(7), Some(1), Some(9), Some(""));
        let decoder = BlobDescriptorDecoder::try_new(&dedicated_with_position).unwrap();
        assert_blob_integrity(
            decoder.classify(0).unwrap_err(),
            "dedicated row 0 uses nonzero position",
        );
        let managed_uri = descriptor(
            Some(0),
            Some(0),
            Some(1),
            Some(0),
            Some("s3://bucket/not-managed"),
        );
        let decoder = BlobDescriptorDecoder::try_new(&managed_uri).unwrap();
        assert!(
            decoder
                .classify(0)
                .unwrap_err()
                .to_string()
                .contains("non-empty blob_uri")
        );

        let base_relative = descriptor(Some(3), Some(0), Some(0), Some(7), Some("object.bin"));
        let decoder = BlobDescriptorDecoder::try_new(&base_relative).unwrap();
        assert!(
            decoder
                .classify(0)
                .unwrap_err()
                .to_string()
                .contains("base-relative")
        );

        let relative = descriptor(
            Some(3),
            Some(0),
            Some(0),
            Some(0),
            Some("relative/object.bin"),
        );
        let decoder = BlobDescriptorDecoder::try_new(&relative).unwrap();
        assert!(
            decoder
                .classify(0)
                .unwrap_err()
                .to_string()
                .contains("absolute URI")
        );

        let prefix = "s3://bucket/";
        let oversized = format!(
            "{prefix}{}",
            "x".repeat(EXTERNAL_BLOB_URI_MAX_BYTES as usize + 1 - prefix.len())
        );
        let descriptor = descriptor(Some(3), Some(0), Some(0), Some(0), Some(&oversized));
        let decoder = BlobDescriptorDecoder::try_new(&descriptor).unwrap();
        let error = decoder.classify(0).unwrap_err();
        assert!(error.to_string().contains("malformed Blob-v2 descriptor"));
        assert_blob_integrity(error, "persisted URI contract");
    }
}
