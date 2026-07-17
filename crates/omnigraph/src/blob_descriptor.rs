//! Shared blob-v2 descriptor decoding.
//!
//! One decoder for the persisted blob descriptor struct
//! (`kind, position, size, blob_id, blob_uri`; legacy v1 is `position, size`)
//! so the read facade and the merge/rewrite path cannot drift on null
//! semantics. The field shape is a Lance format contract pinned by
//! `tests/lance_surface_guards.rs`.

use arrow_array::{Array, StringArray, StructArray, UInt8Array, UInt32Array, UInt64Array};
use lance::datatypes::BlobKind;

use crate::error::{OmniError, Result};

/// One row's decoded blob descriptor.
///
/// `Null` mirrors Lance's skip condition exactly: a null struct/kind, a
/// legacy-v1 row with null position or size, or the v2 null sentinel
/// (`Inline` with zero position, zero size, and an empty uri).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlobDescriptor {
    Null,
    Inline { position: u64, size: u64 },
    Packed { size: u64 },
    Dedicated { size: u64 },
    External { uri: String, size: u64, base_id: u32 },
}

fn u64_field<'a>(
    descriptions: &'a StructArray,
    name: &str,
) -> Result<&'a UInt64Array> {
    descriptions
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            OmniError::Lance(format!(
                "unrecognized blob description schema {:?}: missing UInt64 {} field",
                descriptions.fields(),
                name
            ))
        })
}

pub(crate) fn decode_blob_descriptor(
    descriptions: &StructArray,
    row: usize,
) -> Result<BlobDescriptor> {
    if descriptions.is_null(row) {
        return Ok(BlobDescriptor::Null);
    }

    let position_col = u64_field(descriptions, "position")?;
    let size_col = u64_field(descriptions, "size")?;

    let Some(kind_col) = descriptions.column_by_name("kind") else {
        // Legacy v1 layout (position, size): per-field nullness decides.
        if position_col.is_null(row) || size_col.is_null(row) {
            return Ok(BlobDescriptor::Null);
        }
        return Ok(BlobDescriptor::Inline {
            position: position_col.value(row),
            size: size_col.value(row),
        });
    };

    let kind_raw = if let Some(kind) = kind_col.as_any().downcast_ref::<UInt8Array>() {
        if kind.is_null(row) {
            return Ok(BlobDescriptor::Null);
        }
        kind.value(row)
    } else if let Some(kind) = kind_col.as_any().downcast_ref::<UInt32Array>() {
        if kind.is_null(row) {
            return Ok(BlobDescriptor::Null);
        }
        kind.value(row) as u8
    } else {
        return Err(OmniError::Lance(format!(
            "unrecognized blob description schema {:?}: kind field must be UInt8 or UInt32",
            descriptions.fields()
        )));
    };
    let kind = BlobKind::try_from(kind_raw).map_err(|e| OmniError::Lance(e.to_string()))?;

    let position = (!position_col.is_null(row))
        .then(|| position_col.value(row))
        .unwrap_or(0);
    let size = (!size_col.is_null(row))
        .then(|| size_col.value(row))
        .unwrap_or(0);
    let blob_uri = descriptions
        .column_by_name("blob_uri")
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .and_then(|arr| (!arr.is_null(row)).then(|| arr.value(row).to_string()))
        .unwrap_or_default();

    if matches!(kind, BlobKind::Inline) && position == 0 && size == 0 && blob_uri.is_empty() {
        return Ok(BlobDescriptor::Null);
    }

    Ok(match kind {
        BlobKind::Inline => BlobDescriptor::Inline { position, size },
        BlobKind::Packed => BlobDescriptor::Packed { size },
        BlobKind::Dedicated => BlobDescriptor::Dedicated { size },
        BlobKind::External => {
            let base_id = descriptions
                .column_by_name("blob_id")
                .and_then(|col| col.as_any().downcast_ref::<UInt32Array>())
                .and_then(|arr| (!arr.is_null(row)).then(|| arr.value(row)))
                .unwrap_or(0);
            BlobDescriptor::External {
                uri: blob_uri,
                size,
                base_id,
            }
        }
    })
}
