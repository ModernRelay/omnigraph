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
    } else if details.is_vector() {
        IndexKind::Vector
    } else {
        IndexKind::Unknown
    }
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
            ("VectorIndexDetails", IndexKind::Vector),
        ] {
            assert_eq!(kind(&segment("key", 0, Some(&[1]), Some(family))), expected);
        }
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
