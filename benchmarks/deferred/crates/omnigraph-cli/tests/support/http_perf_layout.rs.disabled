//! Read-only physical evidence for the controlled HTTP instrument.
//!
//! The caller stops the fixture's server before this runs, outside measurement.
//! These fresh observer handles say nothing about the server's cache occupancy.

use std::path::{Component, Path};

use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::index::DatasetIndexExt;
use omnigraph::db::{Omnigraph, ReadTarget};
use serde_json::{Value, json};

/// Inspect accepted main pins, never a user table's linear HEAD. This intentionally
/// supports only the instrument's local, current-format, main-only fixtures.
pub(super) fn observe_layout(graph: &Path) -> Value {
    let graph = graph.canonicalize().expect("canonical benchmark graph");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("layout observer runtime")
        .block_on(async {
            let uri = graph.to_str().expect("UTF-8 benchmark graph");
            let db = Omnigraph::open_read_only(uri)
                .await
                .expect("read-only benchmark graph open");
            let snapshot = db
                .snapshot_of(ReadTarget::branch("main"))
                .await
                .expect("accepted benchmark snapshot");
            let mut entries: Vec<_> = snapshot.datasets().cloned().collect();
            assert!(entries.len() <= 16, "unexpected benchmark table count");
            entries.sort_by(|left, right| left.type_key.cmp(&right.type_key));
            let mut tables = Vec::with_capacity(entries.len());
            for entry in &entries {
                assert!(
                    entry.native_dataset_branch.is_none(),
                    "layout observer only supports main native pins"
                );
                let metadata = &entry.version_metadata;
                let last_linear = metadata
                    .last_linear_version()
                    .expect("current-format fixture records its linear boundary");
                // Match open_pinned_dataset's detached-only rule. The legacy
                // v10 twin fallback is deliberately outside this fixture owner.
                let native_version = match metadata.staged_version() {
                    Some(staged) if entry.published_dataset_version > last_linear => staged,
                    None => {
                        assert!(entry.published_dataset_version <= last_linear);
                        entry.published_dataset_version
                    }
                    Some(_) => panic!("legacy twin pin in current benchmark fixture"),
                };
                let relative = Path::new(&entry.dataset_path);
                assert!(
                    relative
                        .components()
                        .all(|part| matches!(part, Component::Normal(_))),
                    "expected a relative benchmark table path"
                );
                let path = graph.join(relative).canonicalize().expect("table path");
                assert!(path.starts_with(&graph), "table escaped fixture root");
                let dataset = DatasetBuilder::from_uri(path.to_str().expect("UTF-8 table path"))
                    .with_version(native_version)
                    .load()
                    .await
                    .expect("open exact accepted native table pin");
                assert!(metadata.witnesses(&dataset), "table pin witness mismatch");
                let layout = dataset_layout(&dataset).await;
                assert_eq!(layout["rows"].as_u64(), Some(entry.entity_count));
                tables.push(json!({
                    "type_key": entry.type_key,
                    "published_dataset_version": entry.published_dataset_version,
                    "staged_version": metadata.staged_version(),
                    "last_linear_version": last_linear,
                    "layout": layout,
                }));
            }
            let manifest = DatasetBuilder::from_uri(
                graph
                    .join("__manifest")
                    .to_str()
                    .expect("UTF-8 manifest path"),
            )
            .with_version(snapshot.graph_manifest_version())
            .load()
            .await
            .expect("open captured graph manifest version");
            let manifest_layout = dataset_layout(&manifest).await;

            // A separate read-only open avoids mistaking handle-local cached
            // authority for proof that the stopped fixture did not move.
            let after_db = Omnigraph::open_read_only(uri)
                .await
                .expect("reopen observed graph read-only");
            let after = after_db
                .snapshot_of(ReadTarget::branch("main"))
                .await
                .expect("recheck accepted benchmark snapshot");
            assert_eq!(
                snapshot.graph_manifest_version(),
                after.graph_manifest_version()
            );
            assert_eq!(snapshot.graph_head(None), after.graph_head(None));
            assert_eq!(
                db.schema_contract_digest(),
                after_db.schema_contract_digest()
            );
            assert_eq!(entries.len(), after.datasets().count());
            for entry in &entries {
                assert!(
                    after
                        .dataset(&entry.type_key)
                        .is_some_and(|candidate| entry.same_registration(candidate)),
                    "table authority moved during physical observation"
                );
            }
            json!({
                "observer": "accepted-main-layout-v1",
                "graph_manifest_version": snapshot.graph_manifest_version(),
                "graph_head": snapshot.graph_head(None),
                "manifest": manifest_layout,
                "tables": tables,
            })
        })
}

async fn dataset_layout(dataset: &Dataset) -> Value {
    assert!(
        dataset.manifest().base_paths.is_empty(),
        "benchmark fixtures must not depend on relocated external files"
    );
    let fragments = dataset.get_fragments();
    assert!(
        fragments.len() <= 20_000,
        "unexpected benchmark fragment count"
    );
    let indices = dataset
        .load_indices()
        .await
        .expect("read native index inventory");
    assert!(indices.len() <= 64, "unexpected benchmark index count");
    let mut indices: Vec<_> = indices.iter().collect();
    indices.sort_by(|left, right| (&left.name, left.uuid).cmp(&(&right.name, right.uuid)));
    let indices: Vec<_> = indices
        .into_iter()
        .map(|index| {
            assert!(index.name.len() <= 256 && index.fields.len() <= 64);
            let present_coverage = index.fragment_bitmap.as_ref().map(|bitmap| {
                fragments.iter().filter(|fragment| {
                    u32::try_from(fragment.metadata().id)
                        .is_ok_and(|id| bitmap.contains(id))
                }).count()
            });
            json!({
                "name": index.name,
                "uuid": index.uuid.to_string(),
                "fields": index.fields,
                "index_version": index.index_version,
                "dataset_version": index.dataset_version,
                "declared_fragment_count": index.fragment_bitmap.as_ref().map(|bitmap| bitmap.len()),
                "present_fragment_count": present_coverage,
                "file_bytes_if_recorded": index.total_size_bytes(),
            })
        })
        .collect();
    json!({
        "native_version": dataset.version().version,
        "rows": dataset.count_rows(None).await.expect("count live fixture rows"),
        "fragments": fragments.len(),
        "physical_rows_if_recorded": fragments.iter().map(|fragment| fragment.metadata().physical_rows).sum::<Option<usize>>(),
        "manifest_bytes_if_recorded": dataset.manifest_location().size,
        "has_raw_index_section": dataset.manifest().index_section.is_some(),
        // Lance filters metadata for index versions this reader cannot load.
        // The raw-section bit above preserves that distinction.
        "visible_index_metadata": indices,
    })
}
