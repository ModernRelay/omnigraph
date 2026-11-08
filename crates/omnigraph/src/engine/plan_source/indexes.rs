use std::collections::{BTreeMap, BTreeSet};

use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance::index::scalar::IndexDetails;
use lance_core::datatypes::Schema;
use lance_table::format::IndexMetadata;
use lance_table::system_index::is_system_index;
use omnigraph_planner::{FragmentCoverage, IndexFact, IndexKind};

use crate::error::{OmniError, Result};

pub(super) async fn gather(dataset: &Dataset) -> Result<Vec<IndexFact>> {
    let indices = dataset.load_indices().await.map_err(OmniError::storage)?;
    let current: BTreeSet<_> = dataset
        .fragments()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    let physical_rows_known = dataset
        .fragments()
        .iter()
        .all(|fragment| fragment.physical_rows.is_some());
    Ok(catalog(
        dataset.schema(),
        &indices,
        &current,
        physical_rows_known,
    ))
}

fn catalog(
    schema: &Schema,
    indices: &[IndexMetadata],
    current: &BTreeSet<u64>,
    physical_rows_known: bool,
) -> Vec<IndexFact> {
    let mut groups: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for index in indices.iter().filter(|index| !is_system_index(index)) {
        let Some(field) = index.keyed_field().and_then(|id| schema.field_by_id(id)) else {
            continue;
        };
        let column = schema
            .field_ancestry_by_id(field.id)
            .map(|ancestors| {
                lance_core::datatypes::format_field_path(
                    &ancestors
                        .iter()
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>(),
                )
            })
            .unwrap_or_else(|| field.name.clone());
        groups
            .entry((index.name.clone(), column))
            .or_default()
            .push(index);
    }
    groups
        .into_iter()
        .map(|((name, column), segments)| {
            fact(name, column, &segments, current, physical_rows_known)
        })
        .collect()
}

fn kind(index: &IndexMetadata) -> IndexKind {
    let Some(details) = &index.index_details else {
        return IndexKind::Unknown;
    };
    let details = IndexDetails(details.clone());
    if details
        .get_plugin()
        .is_ok_and(|plugin| plugin.name() == "BTree")
    {
        IndexKind::Btree { usable: false }
    } else if details.supports_fts() {
        IndexKind::Inverted
    } else if details.is_vector() && vector_index_ranks_exactly_by_l2(index) {
        IndexKind::Vector
    } else {
        IndexKind::Unknown
    }
}

/// Whether a vector index segment scores every row of each partition it
/// searches exactly by squared L2, the metric `nearest` promises: IVF with
/// unquantized vectors (IVF_FLAT) and the L2 metric, the only vector index
/// the engine builds. A segment with quantized vectors (PQ, SQ, RQ), an HNSW
/// graph or another metric approximates or ranks differently, and so does
/// one whose details are empty: Lance infers missing details when it loads
/// indices, and an empty value left after that is unknown, not the proto
/// defaults (L2, no compression) it would decode to. Details carrying a
/// field this Lance does not know are not exact either: the decoder drops
/// the field, which may be a newer sub-index (an IVF_VAMANA_FLAT index adds
/// `vamana_index_config`, lance#10318), so the re-encoded details must be as
/// long as the stored ones.
pub(crate) fn vector_index_ranks_exactly_by_l2(index: &IndexMetadata) -> bool {
    use lance_index::pb::{
        VectorIndexDetails, VectorMetricType, vector_index_details::Compression,
    };
    let Some(details) = &index.index_details else {
        return false;
    };
    if details.value.is_empty() || !IndexDetails(details.clone()).is_vector() {
        return false;
    }
    let Ok(decoded) = details.to_msg::<VectorIndexDetails>() else {
        return false;
    };
    if prost::Message::encoded_len(&decoded) != details.value.len() {
        return false;
    }
    let details = decoded;
    VectorMetricType::try_from(details.metric_type) == Ok(VectorMetricType::L2)
        && details.hnsw_index_config.is_none()
        && matches!(details.compression, None | Some(Compression::Flat(_)))
}

/// Whether every vector index segment keyed on `column` of `dataset` ranks
/// exactly by squared L2 ([`vector_index_ranks_exactly_by_l2`]); true when no
/// vector index covers the column, since Lance then scores every row flat.
pub(crate) async fn vector_indexes_rank_exactly_by_l2(
    dataset: &Dataset,
    column: &str,
) -> Result<bool> {
    let Some(field) = dataset.schema().field(column) else {
        return Ok(true);
    };
    let indices = dataset.load_indices().await.map_err(OmniError::storage)?;
    Ok(indices
        .iter()
        .filter(|index| !is_system_index(index))
        .filter(|index| index.fields.first() == Some(&field.id))
        .filter(|index| {
            index
                .index_details
                .as_ref()
                .is_none_or(|details| IndexDetails(details.clone()).is_vector())
        })
        .all(vector_index_ranks_exactly_by_l2))
}

fn fact(
    name: String,
    column: String,
    segments: &[&IndexMetadata],
    current: &BTreeSet<u64>,
    physical_rows_known: bool,
) -> IndexFact {
    let covered: BTreeSet<_> = segments
        .iter()
        .filter_map(|segment| segment.fragment_bitmap.as_ref())
        .flat_map(|bitmap| bitmap.iter().map(u64::from))
        .filter(|fragment| current.contains(fragment))
        .collect();
    let mut index_kind = segments
        .first()
        .map_or(IndexKind::Unknown, |segment| kind(segment));
    if segments.iter().any(|segment| kind(segment) != index_kind) {
        index_kind = IndexKind::Unknown;
    }
    if let IndexKind::Btree { usable } = &mut index_kind {
        *usable = physical_rows_known && !covered.is_empty();
    }
    IndexFact {
        name,
        column,
        kind: index_kind,
        coverage: segments
            .iter()
            .all(|segment| segment.fragment_bitmap.is_some())
            .then_some(FragmentCoverage {
                covered: covered.len() as u64,
                total: current.len() as u64,
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_table::format::pb;

    fn segment(
        name: &str,
        column: i32,
        fragments: Option<&[u32]>,
        family: Option<&str>,
    ) -> IndexMetadata {
        let mut proto = pb::IndexMetadata {
            uuid: Some(pb::Uuid { uuid: vec![0; 16] }),
            name: name.into(),
            fields: vec![column],
            ..Default::default()
        };
        if let Some(family) = family {
            let details = proto.index_details.get_or_insert_default();
            details.type_url = format!("type.googleapis.com/lance.index.pb.{family}");
        }
        let mut metadata = IndexMetadata::try_from(proto).unwrap();
        metadata.fragment_bitmap = fragments.map(|ids| ids.iter().copied().collect());
        metadata
    }

    /// Synthetic metadata exercises unknown coverage and segment overlap that GQT cannot create.
    #[test]
    fn segment_union_keeps_unknown_coverage_and_usability_separate() {
        let current = BTreeSet::from([1, 2, 3]);
        let a = segment("key", 0, Some(&[1, 2, 99]), Some("BTreeIndexDetails"));
        let mut b = segment("key", 0, Some(&[2, 3]), Some("BTreeIndexDetails"));
        let get = |segments: &[&IndexMetadata], current: &BTreeSet<u64>, rows| {
            fact("key".into(), "__src".into(), segments, current, rows)
        };
        let union = get(&[&a, &b], &current, true);
        assert!(union.fully_covers_btree("__src"));
        assert_eq!(
            union.coverage,
            Some(FragmentCoverage {
                covered: 3,
                total: 3
            })
        );
        assert_eq!(
            get(&[&a], &current, true).coverage,
            Some(FragmentCoverage {
                covered: 2,
                total: 3
            })
        );
        b.fragment_bitmap = None;
        let unknown = get(&[&a, &b], &current, true);
        assert_eq!(unknown.coverage, None);
        assert_eq!(unknown.kind, IndexKind::Btree { usable: true });
        assert!(!unknown.fully_covers_btree("__src"));
        assert_eq!(
            get(&[&a], &current, false).kind,
            IndexKind::Btree { usable: false }
        );
        assert_eq!(
            get(&[&a], &BTreeSet::new(), true).kind,
            IndexKind::Btree { usable: false }
        );
        assert_eq!(
            get(&[&a], &BTreeSet::from([100]), true).kind,
            IndexKind::Btree { usable: false }
        );
        assert_eq!(
            get(&[&b], &current, true).kind,
            IndexKind::Btree { usable: false }
        );
    }

    #[test]
    fn unknown_details_never_claim_a_btree() {
        for family in [
            None,
            Some("FutureBTreeIndexDetails"),
            Some("BitmapIndexDetails"),
        ] {
            let index = segment("key", 0, Some(&[1]), family);
            assert_eq!(kind(&index), IndexKind::Unknown, "{family:?}");
        }
        for (family, expected) in [
            ("InvertedIndexDetails", IndexKind::Inverted),
            // Empty vector details are unknown: Lance infers missing details
            // when it loads indices, so empty ones are details it could not.
            ("VectorIndexDetails", IndexKind::Unknown),
        ] {
            assert_eq!(kind(&segment("key", 0, Some(&[1]), Some(family))), expected);
        }
    }

    /// Issue #1297: only an IVF_FLAT, L2 vector index is a vector index fact;
    /// another metric, quantized vectors or an HNSW graph rank otherwise.
    #[test]
    fn only_flat_l2_vector_details_claim_a_vector_index_issue_1297() {
        use lance_index::pb::{
            HnswParameters, VectorIndexDetails, VectorMetricType,
            vector_index_details::{
                Compression, FlatCompression, ProductQuantization, ScalarQuantization,
            },
        };
        let flat = || Some(Compression::Flat(FlatCompression {}));
        let vector = |details: VectorIndexDetails| {
            let mut proto = pb::IndexMetadata {
                uuid: Some(pb::Uuid { uuid: vec![0; 16] }),
                name: "key".into(),
                fields: vec![0],
                ..Default::default()
            };
            let any = proto.index_details.get_or_insert_default();
            any.type_url = "type.googleapis.com/lance.index.pb.VectorIndexDetails".into();
            any.value = prost::Message::encode_to_vec(&details);
            IndexMetadata::try_from(proto).unwrap()
        };
        let l2 = VectorMetricType::L2 as i32;
        let cases = [
            (l2, flat(), None, true),
            (l2, None, None, true),
            (VectorMetricType::Cosine as i32, flat(), None, false),
            (VectorMetricType::Dot as i32, flat(), None, false),
            (
                l2,
                Some(Compression::Pq(ProductQuantization {
                    num_bits: 8,
                    num_sub_vectors: 2,
                })),
                None,
                false,
            ),
            (
                l2,
                Some(Compression::Sq(ScalarQuantization { num_bits: 8 })),
                None,
                false,
            ),
            (l2, flat(), Some(HnswParameters::default()), false),
        ];
        for (metric_type, compression, hnsw_index_config, exact) in cases {
            let index = vector(VectorIndexDetails {
                metric_type,
                compression,
                hnsw_index_config,
                target_partition_size: 1,
                ..Default::default()
            });
            assert_eq!(
                vector_index_ranks_exactly_by_l2(&index),
                exact,
                "{metric_type} {compression:?}"
            );
            let expected = if exact {
                IndexKind::Vector
            } else {
                IndexKind::Unknown
            };
            assert_eq!(kind(&index), expected, "{metric_type} {compression:?}");
        }

        // An IVF_FLAT, L2 index whose details carry a field this Lance does
        // not know (field 10, a newer sub-index's configuration) is not exact.
        let mut index = vector(VectorIndexDetails {
            metric_type: l2,
            compression: flat(),
            target_partition_size: 1,
            ..Default::default()
        });
        let mut details = (**index.index_details.as_ref().unwrap()).clone();
        details.value.extend([(10 << 3) | 2, 0]);
        index.index_details = Some(std::sync::Arc::new(details));
        assert!(!vector_index_ranks_exactly_by_l2(&index));
        assert_eq!(kind(&index), IndexKind::Unknown);
    }

    #[test]
    fn catalog_keeps_nested_paths_and_keyed_covering_prefixes() {
        use arrow_schema::{DataType, Field};
        let schema = Schema::try_from(&arrow_schema::Schema::new(vec![
            Field::new("age", DataType::Int64, true),
            Field::new(
                "profile",
                DataType::Struct(vec![Field::new("age", DataType::Int64, true)].into()),
                true,
            ),
        ]))
        .unwrap();
        let mut nested = segment("nested", 2, Some(&[1]), Some("BTreeIndexDetails"));
        nested.fields.push(0);
        nested.covering_fields.push(0);
        let mut system = segment(
            lance_table::system_index::frag_reuse::FRAG_REUSE_INDEX_NAME,
            0,
            Some(&[1]),
            Some("BTreeIndexDetails"),
        );
        let top = segment("top", 0, Some(&[1]), Some("BTreeIndexDetails"));
        let first = catalog(
            &schema,
            &[nested.clone(), top.clone(), system.clone()],
            &BTreeSet::from([1]),
            true,
        );
        assert_eq!(
            first
                .iter()
                .map(|fact| fact.column.as_str())
                .collect::<Vec<_>>(),
            ["profile.age", "age"]
        );
        system.name = lance_table::system_index::mem_wal::MEM_WAL_INDEX_NAME.into();
        assert_eq!(
            catalog(&schema, &[top, system, nested], &BTreeSet::from([1]), true),
            first
        );
    }
}
