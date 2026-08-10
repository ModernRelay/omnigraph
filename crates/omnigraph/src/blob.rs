//! Engine-owned interpretation of Lance Blob-v2 descriptors.
//!
//! Lance remains responsible for resolving physical files and reading bytes.
//! This module owns the logical boundary that every OmniGraph carrier needs:
//! parent validity is the sole null witness, a valid zero-length descriptor is
//! a managed value, and malformed persisted descriptors fail before a caller
//! can silently reinterpret them.

use arrow_array::{Array, StringArray, StructArray, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::DataType;

use crate::error::{OmniError, Result};

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

impl BlobDescriptor {
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
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
            // Inline, packed, and dedicated are one logical Managed state.
            // Their physical discriminator still owns validation: Lance
            // reserves blob id zero, so accepting it for a sidecar-backed
            // descriptor would turn corrupt persisted state into a plausible
            // logical value.
            0 => {
                if !blob_uri.is_empty() {
                    return Err(malformed_descriptor(format!(
                        "managed row {row} has non-empty blob_uri"
                    )));
                }
                Ok(BlobDescriptor::Managed { length: size })
            }
            1 | 2 => {
                if blob_id == 0 {
                    return Err(malformed_descriptor(format!(
                        "managed row {row} kind {} uses reserved blob_id 0",
                        self.kinds.value(row)
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
        for kind in [1, 2] {
            let reserved_id = descriptor(Some(kind), Some(0), Some(1), Some(0), Some(""));
            let decoder = BlobDescriptorDecoder::try_new(&reserved_id).unwrap();
            let error = decoder.classify(0).unwrap_err().to_string();
            assert!(
                error.contains("reserved blob_id 0"),
                "managed kind {kind}: {error}"
            );
        }

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
    }
}
