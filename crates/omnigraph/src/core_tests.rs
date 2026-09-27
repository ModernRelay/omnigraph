//! Tests of `omnigraph-core` code that need engine items (`Omnigraph`,
//! `TableStore`, `seams::FailScenario`), so they live on the engine side.

mod fts_compat_tests {
    use std::sync::Arc;

    use lance::Dataset;
    use lance_table::format::IndexMetadata;

    use crate::error::OmniError;
    use crate::table_store::fts_compat::{
        CERTIFICATE_FILE, MAX_CERTIFICATE_BYTES, certificate_path, verify_index,
    };

    #[tokio::test]
    async fn certificate_file_is_bounded_and_follows_shallow_clone_ownership() {
        use arrow_array::{RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use lance::{
            dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE},
            index::DatasetIndexExt,
            io::ObjectStoreRegistry,
            session::Session,
        };
        use object_store::ObjectStoreExt;

        use crate::{storage_layer::IndexBuildSpec, table_store::TableStore};

        async fn assert_native_siblings(dataset: &Dataset, index: &IndexMetadata) {
            let parent = certificate_path(dataset, index).unwrap().parent().unwrap();
            let store = dataset.object_store(index.base_id).await.unwrap();
            for file in index.files.as_ref().unwrap() {
                let meta = store
                    .inner
                    .head(&parent.clone().join(file.path.as_str()))
                    .await
                    .unwrap();
                assert_eq!(meta.size, file.size_bytes, "native sibling {}", file.path);
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("source.lance");
        let uri = uri.to_str().unwrap();
        // Local-file clients (and their counters) are pooled by registry, not
        // dataset path. Isolate I/O accounting while retaining normal caches.
        let session = Arc::new(Session::new(
            DEFAULT_INDEX_CACHE_SIZE,
            DEFAULT_METADATA_CACHE_SIZE,
            Arc::new(ObjectStoreRegistry::default()),
        ));
        let store = TableStore::new(uri, session);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["organism"]))],
        )
        .unwrap();
        TableStore::write_dataset(uri, batch).await.unwrap();
        // The low-level bootstrap writer intentionally returns a zero-cache
        // control session. Read through the engine's normal data-table opener
        // and graph-scoped data session before testing warm query behavior.
        let dataset = store.open_dataset_head(uri, None).await.unwrap();
        let staged = store
            .stage_create_indices(
                &dataset,
                &[IndexBuildSpec::FullText {
                    column: "body".into(),
                }],
            )
            .await
            .unwrap();
        let (mut dataset, _) = store
            .commit_staged_exact(Arc::new(dataset), staged)
            .await
            .unwrap();
        let index = dataset.load_indices().await.unwrap()[0].clone();
        // Lance wrote the native files independently of certificate_path. A
        // wrong mirror must not pass just because our writer and reader agree.
        assert_native_siblings(&dataset, &index).await;
        let object_store = dataset.object_store(None).await.unwrap();
        let path = certificate_path(&dataset, &index).unwrap();
        let pooled_reader = TableStore::new(
            uri,
            crate::lance_access::LanceAccessContext::new().data_session(),
        )
        .open_dataset_head(uri, None)
        .await
        .unwrap();
        let pooled_store = pooled_reader.object_store(None).await.unwrap();
        assert!(!Arc::ptr_eq(&object_store, &pooled_store));
        object_store.io_stats_incremental();
        verify_index(&dataset, &index).await.unwrap();
        assert!(object_store.io_stats_incremental().read_iops > 0);
        verify_index(&dataset, &index).await.unwrap();
        // Another graph's pooled client must not contaminate these counters.
        // Keep this deterministic instead of relying on parallel test timing.
        pooled_store.read_one_all(&path).await.unwrap();
        let warm = object_store.io_stats_incremental();
        assert_eq!((warm.read_iops, warm.write_iops), (0, 0));

        // Native local read_iops excludes file metadata/open calls. Lance's
        // test scheme uses CloudObjectReader over the same files, exposing
        // both the HEAD and payload GET through its existing object-store tracker.
        let file_uri = url::Url::from_file_path(uri).unwrap().to_string();
        // forbidden-api-allow: test-only native reader seam to count certificate HEAD and GET separately.
        let cloud_reader = Dataset::open(&file_uri.replacen("file:", "file-object-store:", 1))
            .await
            .unwrap();
        let tracked_store = cloud_reader.object_store(None).await.unwrap();
        assert!(!tracked_store.has_direct_local_paths());
        tracked_store.io_stats_incremental();
        verify_index(&cloud_reader, &index).await.unwrap();
        let cold = tracked_store.io_stats_incremental();
        let methods: Vec<_> = cold.requests.iter().map(|request| request.method).collect();
        assert_eq!(
            methods,
            vec!["get_opts", "get_ranges"],
            "HEAD then payload GET"
        );
        verify_index(&cloud_reader, &index).await.unwrap();
        assert!(tracked_store.io_stats_incremental().requests.is_empty());

        // A changed certificate inventory or artifact cannot borrow the warm
        // proof even when its immutable UUID is unchanged.
        for changed_certificate in [false, true] {
            let mut changed = index.clone();
            changed
                .files
                .as_mut()
                .unwrap()
                .iter_mut()
                .find(|file| (file.path == CERTIFICATE_FILE) == changed_certificate)
                .unwrap()
                .size_bytes += 1;
            assert!(matches!(
                verify_index(&dataset, &changed).await,
                Err(OmniError::FullTextIndexRebuildRequired { .. })
            ));
        }

        let original = object_store.read_one_all(&path).await.unwrap();

        let clone_uri = directory.path().join("source.lance/tree/certificate-clone");
        let version = dataset.version().version;
        crate::storage_layer::lance_clone::create_branch(
            &mut dataset,
            "certificate-clone",
            version,
        )
        .await
        .unwrap();
        let mut cloned =
            // forbidden-api-allow: test-only same-session clone proves certificate ownership cannot alias by UUID.
            lance::dataset::builder::DatasetBuilder::from_uri(clone_uri.to_str().unwrap())
                .with_session(dataset.session())
                .load()
                .await
                .unwrap();
        let clone_index = cloned.load_indices().await.unwrap()[0].clone();
        assert_eq!(clone_index.uuid, index.uuid);
        assert!(Arc::ptr_eq(&cloned.session(), &dataset.session()));
        let base_id = clone_index
            .base_id
            .expect("shallow clone must reference its source base");
        assert!(
            !clone_uri
                .join("_indices")
                .join(index.uuid.to_string())
                .join(CERTIFICATE_FILE)
                .exists()
        );
        // A different dataset URI sharing the same session and index UUID
        // must acquire its own proof. Failures must not be cached either.
        object_store.inner.delete(&path).await.unwrap();
        assert!(matches!(
            verify_index(&cloned, &clone_index).await,
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
        object_store.put(&path, &original).await.unwrap();
        verify_index(&cloned, &clone_index).await.unwrap();
        assert_native_siblings(&cloned, &clone_index).await;
        // Both public BasePath shapes must resolve the same immutable file.
        let base = Arc::make_mut(&mut cloned.manifest)
            .base_paths
            .get_mut(&base_id)
            .unwrap();
        assert!(base.is_dataset_root);
        base.path.push_str("/_indices");
        base.is_dataset_root = false;
        verify_index(&cloned, &clone_index).await.unwrap();
        assert_native_siblings(&cloned, &clone_index).await;

        for (case, payload) in [
            ("truncated", original[..original.len() - 1].to_vec()),
            (
                "oversized despite small inventory",
                vec![b' '; MAX_CERTIFICATE_BYTES as usize + 1],
            ),
            (
                "invalid JSON at exact recorded size",
                vec![b'x'; original.len()],
            ),
            (
                "invalid UTF-8 at exact recorded size",
                vec![0xff; original.len()],
            ),
        ] {
            object_store.put(&path, &payload).await.unwrap();
            // These deliberate out-of-band rewrites violate immutable-file
            // ownership. Reopening models their first observation by a reader.
            // forbidden-api-allow: test-only cold reader must revalidate deliberately corrupted certificate bytes.
            let cold = Dataset::open(uri).await.unwrap();
            assert!(
                matches!(
                    verify_index(&cold, &index).await,
                    Err(OmniError::FullTextIndexRebuildRequired { .. })
                ),
                "accepted {case}"
            );
            object_store.put(&path, &original).await.unwrap();
            verify_index(&cold, &index).await.unwrap();
        }
        object_store.put(&path, &original).await.unwrap();
        verify_index(&dataset, &index).await.unwrap();

        let mut missing_reference = index.clone();
        missing_reference
            .files
            .as_mut()
            .unwrap()
            .retain(|file| file.path != CERTIFICATE_FILE);
        assert!(matches!(
            verify_index(&dataset, &missing_reference).await,
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
        object_store.inner.delete(&path).await.unwrap();
        // forbidden-api-allow: test-only cold reader must observe deliberate certificate deletion.
        let cold = Dataset::open(uri).await.unwrap();
        assert!(matches!(
            verify_index(&cold, &index).await,
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
        // forbidden-api-allow: test-only cold clone must observe deletion at its source-owned certificate path.
        let cold_clone = Dataset::open(clone_uri.to_str().unwrap()).await.unwrap();
        assert!(matches!(
            verify_index(&cold_clone, &clone_index).await,
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
    }
}

mod branch_control_tests {
    #[cfg(feature = "failpoints")]
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::Dataset;
    #[cfg(feature = "failpoints")]
    use lance::dataset::refs::branch_contents_path;
    use lance::dataset::{WriteMode, WriteParams};

    use crate::branch_control::*;
    #[cfg(feature = "failpoints")]
    use crate::error::OmniError;

    async fn test_dataset(dir: &tempfile::TempDir) -> Dataset {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let path = dir.path().to_str().unwrap().replace('\\', "/");
        let path_prefix = if path.starts_with('/') { "" } else { "/" };
        let uri = format!("file-object-store://{path_prefix}{path}");
        // forbidden-api-allow: test-only raw Lance fixture for native branch-control truth cells
        Dataset::write(
            reader,
            &uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                auto_cleanup: None,
                skip_auto_cleanup: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    #[serial_test::serial]
    async fn retire_classifies_durable_metadata_after_lost_ack_and_retries() {
        let _scenario = crate::seams::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("external".to_string(), "preserved".to_string());
        dataset
            .branches()
            .replace_metadata("feature", metadata)
            .await
            .unwrap();
        let original = dataset.branches().get("feature").await.unwrap();
        {
            let _lost_ack = BRANCH_DELETE_POST_NATIVE.fire_always();
            retire_branch_recoverably(&dataset, "feature", &original.identifier)
                .await
                .unwrap();
        }
        let retired = archived_manifest_branch(&dataset, "feature")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retired.identifier, original.identifier);
        assert_eq!(
            retired.metadata.get("external").map(String::as_str),
            Some("preserved")
        );
        assert!(!manifest_branch_is_live("feature", &retired).unwrap());
        assert!(
            !list_branch_contents(&dataset)
                .await
                .unwrap()
                .contains_key("feature")
        );
        assert!(
            !list_live_manifest_branch_contents(&dataset)
                .await
                .unwrap()
                .contains_key("feature")
        );
        assert!(matches!(
            get_live_manifest_branch_contents(&dataset, "feature").await,
            Err(OmniError::BranchNotFound { .. })
        ));
        let historical = dataset
            .checkout_version(lance::dataset::refs::Ref::Version(
                Some("feature".to_string()),
                Some(version),
            ))
            .await
            .unwrap();
        assert_eq!(historical.version().version, version);
        retire_branch_recoverably(&dataset, "feature", &original.identifier)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(
                archived_manifest_branch(&dataset, "feature")
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_value(retired).unwrap(),
        );
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    #[serial_test::serial]
    async fn retirement_archive_retry_preserves_legacy_identity_and_ref_until_valid() {
        let _scenario = crate::seams::FailScenario::setup();
        for legacy in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut dataset = test_dataset(&dir).await;
            let version = dataset.version().version;
            dataset
                .create_branch("feature", version, None)
                .await
                .unwrap();
            let root = dataset.branch_location().find_main().unwrap().path;
            let store = dataset.object_store(None).await.unwrap();
            if legacy {
                let mut value =
                    serde_json::to_value(dataset.branches().get("feature").await.unwrap()).unwrap();
                value.as_object_mut().unwrap().remove("identifier");
                store
                    .put(
                        &branch_contents_path(&root, "feature"),
                        &serde_json::to_vec(&value).unwrap(),
                    )
                    .await
                    .unwrap();
            }
            let expected = dataset.branches().get("feature").await.unwrap().identifier;
            {
                let _interrupted = BRANCH_DELETE_POST_ARCHIVE.fire_always();
                retire_branch_recoverably(&dataset, "feature", &expected)
                    .await
                    .unwrap_err();
            }
            let pending = dataset.branches().get("feature").await.unwrap();
            assert!(!manifest_branch_is_live("feature", &pending).unwrap());
            assert_eq!(pending.identifier, expected);
            let archived = archived_manifest_branch(&dataset, "feature")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(archived.identifier, expected);
            let archive_path = retirement_archive_path_for_test(&dataset, "feature").unwrap();
            store.put(&archive_path, b"{").await.unwrap();
            retire_branch_recoverably(&dataset, "feature", &expected)
                .await
                .unwrap_err();
            assert_eq!(
                dataset.branches().get("feature").await.unwrap().identifier,
                expected
            );
            assert!(list_archived_manifest_branches(&dataset).await.is_err());
            store
                .put(&archive_path, &serde_json::to_vec(&archived).unwrap())
                .await
                .unwrap();
            archive_retired_manifest_branches(&dataset).await.unwrap();
            assert!(matches!(
                dataset.branches().get("feature").await,
                Err(lance::Error::RefNotFound { .. })
            ));
            retire_branch_recoverably(&dataset, "feature", &expected)
                .await
                .unwrap();
            assert!(
                reclaim_ref_absent_tree(&mut dataset, "feature")
                    .await
                    .is_err()
            );
            assert_eq!(
                list_archived_manifest_branches(&dataset).await.unwrap()["feature"].identifier,
                expected
            );
        }
    }

    #[tokio::test]
    async fn retirement_archive_preserves_nested_native_path_ownership() {
        #[cfg(feature = "failpoints")]
        let _scenario = crate::seams::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let native = "team/data/feature";
        let version = dataset.version().version;
        dataset.create_branch(native, version, None).await.unwrap();
        let identifier = dataset.branches().get(native).await.unwrap().identifier;
        retire_branch_recoverably(&dataset, native, &identifier)
            .await
            .unwrap();
        assert!(
            dir.path()
                .join("tree/team/data/feature")
                .join(RETIRED_BRANCH_ARCHIVE)
                .exists()
        );
        let archives = list_archived_manifest_branches(&dataset).await.unwrap();
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[native].identifier, identifier);
        assert!(matches!(
            dataset.branches().get(native).await,
            Err(lance::Error::RefNotFound { .. })
        ));
        let error = create_branch_recoverably(&mut dataset, "team/data/feature/child", version)
            .await
            .err()
            .expect("retired path must refuse a descendant");
        assert!(error.to_string().contains("retired branch"), "{error}");
    }
}

mod instrumentation_tests {
    use std::collections::BTreeSet;

    use crate::instrumentation::declared_engine_cargo_features;

    #[test]
    fn benchmark_feature_attestation_covers_every_engine_feature() {
        let manifest = toml::from_str::<toml::Value>(include_str!("../Cargo.toml"))
            .expect("engine Cargo.toml parses as TOML");
        let declared = manifest["features"]
            .as_table()
            .expect("engine Cargo.toml has a features table")
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let registry = declared_engine_cargo_features()
            .iter()
            .map(|feature| (*feature).to_string())
            .collect::<BTreeSet<_>>();

        assert_eq!(declared, registry, "update the benchmark feature registry");

        let suppressed_optional_dependencies = manifest["features"]
            .as_table()
            .unwrap()
            .values()
            .filter_map(toml::Value::as_array)
            .flatten()
            .filter_map(toml::Value::as_str)
            .filter_map(|feature| feature.strip_prefix("dep:"))
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        let mut optional_dependencies = BTreeSet::new();
        let mut inspect_dependencies = |value: Option<&toml::Value>| {
            let Some(table) = value.and_then(toml::Value::as_table) else {
                return;
            };
            optional_dependencies.extend(
                table
                    .iter()
                    .filter(|(_name, specification)| {
                        specification
                            .as_table()
                            .and_then(|fields| fields.get("optional"))
                            .and_then(toml::Value::as_bool)
                            .is_some_and(|optional| optional)
                    })
                    .map(|(name, _specification)| name.clone()),
            );
        };
        for table in ["dependencies", "build-dependencies"] {
            inspect_dependencies(manifest.get(table));
        }
        if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
            for target in targets.values() {
                for table in ["dependencies", "build-dependencies"] {
                    inspect_dependencies(target.get(table));
                }
            }
        }
        assert!(
            optional_dependencies
                .difference(&suppressed_optional_dependencies)
                .next()
                .is_none(),
            "optional dependencies must use dep:name so no implicit feature escapes attestation"
        );
    }
}
