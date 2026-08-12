//! Engine-owned interpretation of Lance Blob-v2 descriptors.
//!
//! Lance remains responsible for resolving physical files and reading bytes.
//! This module owns the logical boundary that every OmniGraph carrier needs:
//! parent validity is the sole null witness, a valid zero-length descriptor is
//! a managed value, and malformed persisted descriptors fail before a caller
//! can silently reinterpret them.

use arrow_array::{Array, StringArray, StructArray, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::DataType;
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};

/// Inclusive raw-byte ceiling for one configured or input external Blob URI.
///
/// This is checked before trimming, URL parsing, percent decoding, or file
/// canonicalization so parser scratch remains bounded independently of the
/// operation-wide retained-metadata budget.
pub const EXTERNAL_BLOB_URI_MAX_BYTES: u64 = 64 * 1024;
const EXTERNAL_BLOB_URI_BYTES_RESOURCE: &str = "external Blob URI bytes";

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
}

fn malformed_descriptor(message: impl Into<String>) -> OmniError {
    OmniError::Lance(format!("malformed Blob-v2 descriptor: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, StructArray};
    use arrow_schema::{Field, Fields};

    use super::*;

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
        assert!(BlobDescriptorDecoder::try_new(&child_null).is_err());
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
        assert!(BlobDescriptorDecoder::try_new(&wrong_type).is_err());

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
        assert!(BlobDescriptorDecoder::try_new(&missing_child).is_err());

        let unknown = descriptor(Some(4), Some(0), Some(0), Some(0), Some(""));
        let decoder = BlobDescriptorDecoder::try_new(&unknown).unwrap();
        assert!(
            decoder
                .classify(0)
                .unwrap_err()
                .to_string()
                .contains("unknown")
        );

        let overflow = descriptor(Some(0), Some(u64::MAX), Some(1), Some(0), Some(""));
        let decoder = BlobDescriptorDecoder::try_new(&overflow).unwrap();
        assert!(
            decoder
                .classify(0)
                .unwrap_err()
                .to_string()
                .contains("overflows")
        );
        assert!(
            decoder
                .classify(1)
                .unwrap_err()
                .to_string()
                .contains("outside")
        );
    }

    #[test]
    fn managed_and_external_uri_invariants_fail_closed() {
        let inline_with_sidecar_id = descriptor(Some(0), Some(0), Some(1), Some(9), Some(""));
        let decoder = BlobDescriptorDecoder::try_new(&inline_with_sidecar_id).unwrap();
        assert!(
            decoder
                .classify(0)
                .unwrap_err()
                .to_string()
                .contains("inline row 0 uses nonzero blob_id")
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
        assert!(
            decoder
                .classify(0)
                .unwrap_err()
                .to_string()
                .contains("dedicated row 0 uses nonzero position")
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
        assert!(matches!(error, OmniError::Lance(_)));
        assert!(error.to_string().contains("malformed Blob-v2 descriptor"));
        assert!(error.to_string().contains("persisted URI contract"));
    }
}
