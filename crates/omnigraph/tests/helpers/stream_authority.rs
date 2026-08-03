use std::sync::Arc;
#[cfg(feature = "failpoints")]
use std::time::Duration;

#[cfg(feature = "failpoints")]
use omnigraph::db::StreamOperationalStatus;
use omnigraph::db::{
    Omnigraph, StreamAuthorityRetirementPlan, StreamAuthorityRetirementResult,
    StreamDeadLetterPage, StreamDeadLetterPayloadPage,
};
use omnigraph::error::Result;
use omnigraph_control_authority::{
    AuthorityOperationClass, OfflineAuthorityRequest, RuntimeBindingRequest,
    ServedExportBindingRequest, ServedExportTerminalRequest, StateLockAcquire, acquire_state_lock,
    mint_runtime_guard, mint_served_export_guard, validate_offline_guard, validate_runtime_binding,
    validate_served_export_binding,
};
use omnigraph_storage::storage_handle_for_uri;
use sha2::{Digest, Sha256};

const GRAPH_ID: &str = "knowledge";
const STREAM_DECLARATION_REVISION: &str = "memwal-stream-test-declaration-v1";
const STREAM_DECLARATION_DIGEST: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

pub fn graph_uri(cluster_uri: &str) -> String {
    format!(
        "{}/graphs/{GRAPH_ID}.omni",
        cluster_uri.trim_end_matches('/')
    )
}

async fn write_cluster_state(cluster_uri: &str) -> String {
    let value = serde_json::json!({
        "version": 1,
        "state_revision": 1,
        "applied_revision": {
            "config_digest": "memwal-stream-test-config",
            "resources": {}
        }
    });
    let text = serde_json::to_string_pretty(&value).unwrap();
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    storage
        .adapter()
        .write_text(&format!("{cluster_uri}/__cluster/state.json"), &text)
        .await
        .unwrap();
    format!("sha256:{:x}", Sha256::digest(text.as_bytes()))
}

pub async fn enable_stream_profile(db: &Arc<Omnigraph>, cluster_uri: &str) {
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let expected_profile_revision = db.stream_status().await.unwrap().profile_revision;
    let operation_id = if expected_profile_revision == 1 {
        "memwal-stream-test-enable".to_string()
    } else {
        format!("memwal-stream-test-enable-{expected_profile_revision}")
    };
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh MemWAL test apply lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision,
            operation_id: &operation_id,
            operation: AuthorityOperationClass::StreamProfileEnable,
            actor: "operator:memwal-test",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_apply_authority(guard).await.unwrap();
    let result = db.set_streaming_profile_checked(authority).await.unwrap();
    assert!(result.streaming_enabled);
}

pub async fn disable_stream_profile(db: &Arc<Omnigraph>, cluster_uri: &str) {
    try_disable_stream_profile(db, cluster_uri).await.unwrap();
}

pub async fn try_disable_stream_profile(db: &Arc<Omnigraph>, cluster_uri: &str) -> Result<()> {
    disable_stream_profile_with_operation_id(db, cluster_uri, "memwal-stream-test-disable").await
}

#[cfg(feature = "failpoints")]
pub async fn disabling_operational_status(
    db: &Arc<Omnigraph>,
    cluster_uri: &str,
) -> Result<StreamOperationalStatus> {
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let status = db.stream_status().await?;
    assert_eq!(status.profile_mode, "DISABLING");
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh status apply lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision: status.profile_revision,
            operation_id: "memwal-stream-test-disabling-status",
            operation: AuthorityOperationClass::StreamProfileDisable,
            actor: "operator:memwal-test",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_apply_authority(guard).await?;
    db.failpoint_stream_operational_status_for_apply_for_test(&authority, Duration::from_secs(10))
        .await
}

/// Disable with a caller-owned receipt identity. Cost fixtures use this to
/// grow immutable profile history through repeated zero-lane transitions.
pub async fn disable_stream_profile_with_operation_id(
    db: &Arc<Omnigraph>,
    cluster_uri: &str,
    operation_id: &str,
) -> Result<()> {
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let status = db.stream_status().await.unwrap();
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh MemWAL test apply lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision: status.profile_revision,
            operation_id,
            operation: AuthorityOperationClass::StreamProfileDisable,
            actor: "operator:memwal-test",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_apply_authority(guard).await.unwrap();
    let result = db.set_streaming_profile_checked(authority).await?;
    assert!(!result.streaming_enabled);
    assert_eq!(db.stream_status().await.unwrap().profile_mode, "DISABLED");
    Ok(())
}

pub async fn assert_offline_stream_profile_authority_available(
    db: &Arc<Omnigraph>,
    cluster_uri: &str,
) {
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let status = db.stream_status().await.unwrap();
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh MemWAL test apply lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision: status.profile_revision,
            operation_id: "memwal-stream-test-shutdown-handoff",
            operation: AuthorityOperationClass::StreamProfileDisable,
            actor: "operator:memwal-test",
            confirm_stream_offline: true,
        },
    )
    .await
    .expect("clean served-runtime shutdown must release the exclusive local registration");
    drop(guard);
}

pub async fn rebind_stream_table_offline(
    db: &Omnigraph,
    cluster_uri: &str,
    table_key: &str,
    rebind_id: &str,
    expected_lifecycle_revision: u64,
) -> Result<String> {
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let status = db.stream_status().await?;
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh MemWAL test apply lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision: status.profile_revision,
            operation_id: rebind_id,
            operation: AuthorityOperationClass::StreamMaintenance,
            actor: "operator:memwal-rebind",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_maintenance_authority(guard).await?;
    Box::pin(db.failpoint_stream_rebind_checked_for_test(
        authority,
        table_key,
        rebind_id,
        expected_lifecycle_revision,
    ))
    .await
}

pub async fn list_stream_dead_letters(
    db: &Omnigraph,
    cluster_uri: &str,
    cursor: Option<&str>,
) -> Result<StreamDeadLetterPage> {
    list_stream_dead_letters_after_authority(db, cluster_uri, cursor, || {}).await
}

/// Run a callback after stopped/offline authority is fully checked but before
/// the selected-token scan starts. Cost instruments use this boundary to keep
/// cluster-lock/state setup out of the measured list operation.
pub async fn list_stream_dead_letters_after_authority<F>(
    db: &Omnigraph,
    cluster_uri: &str,
    cursor: Option<&str>,
    ready: F,
) -> Result<StreamDeadLetterPage>
where
    F: FnOnce(),
{
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let status = db.stream_status().await?;
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh dead-letter list lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision: status.profile_revision,
            operation_id: "memwal-stream-test-dead-letter-list",
            operation: AuthorityOperationClass::StreamDeadLetterControl,
            actor: "operator:memwal-dead-letter",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_dead_letter_authority(guard).await?;
    ready();
    db.list_stream_dead_letters(authority, cursor).await
}

pub async fn export_stream_dead_letter_payloads(
    db: &Omnigraph,
    cluster_uri: &str,
    cursor: Option<&str>,
) -> Result<StreamDeadLetterPayloadPage> {
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let status = db.stream_status().await?;
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh dead-letter export lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision: status.profile_revision,
            operation_id: "memwal-stream-test-dead-letter-export",
            operation: AuthorityOperationClass::StreamDeadLetterControl,
            actor: "operator:memwal-dead-letter",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_dead_letter_authority(guard).await?;
    db.export_stream_dead_letter_payloads(authority, cursor)
        .await
}

pub async fn plan_stream_authority_retirement(
    db: &Omnigraph,
    cluster_uri: &str,
) -> StreamAuthorityRetirementPlan {
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let status = db.stream_status().await.unwrap();
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "stream-retire-for-rebuild")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh retirement plan lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision: status.profile_revision,
            operation_id: "78787878-7878-4878-8878-787878787878",
            operation: AuthorityOperationClass::StreamAuthorityRetirement,
            actor: "operator:memwal-retirement",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_retirement_authority(guard).await.unwrap();
    db.plan_stream_authority_retirement(authority)
        .await
        .unwrap()
}

pub async fn confirm_stream_authority_retirement(
    db: &Omnigraph,
    cluster_uri: &str,
    retirement_id: &str,
    plan_digest: &str,
) -> Result<StreamAuthorityRetirementResult> {
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let status = db.stream_status().await.unwrap();
    let state_cas = write_cluster_state(cluster_uri).await;
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    let lock_uri = format!("{cluster_uri}/__cluster/lock.json");
    let lock = match acquire_state_lock(&storage, &lock_uri, "stream-retire-for-rebuild")
        .await
        .unwrap()
    {
        StateLockAcquire::Acquired(lock) => lock,
        StateLockAcquire::Held => panic!("fresh retirement confirm lock is already held"),
    };
    let guard = validate_offline_guard(
        &lock,
        OfflineAuthorityRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            expected_profile_revision: status.profile_revision,
            operation_id: retirement_id,
            operation: AuthorityOperationClass::StreamAuthorityRetirement,
            actor: "operator:memwal-retirement",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_retirement_authority(guard).await?;
    db.confirm_stream_authority_retirement(authority, retirement_id, plan_digest)
        .await
}

pub async fn bind_checked_stream_runtime(db: Arc<Omnigraph>, cluster_uri: &str) -> Arc<Omnigraph> {
    let status = db.stream_status().await.unwrap();
    assert_eq!(status.profile_mode, "ENABLED");
    let value = serde_json::json!({
        "version": 1,
        "state_revision": 1,
        "applied_revision": {
            "config_digest": "memwal-stream-test-config",
            "resources": {
                "graph.knowledge": {
                    "digest": "memwal-stream-test-graph"
                },
                "streaming.knowledge": {
                    "digest": STREAM_DECLARATION_DIGEST,
                    "declaration_revision": STREAM_DECLARATION_REVISION,
                    "streaming_enabled": true,
                    "profile_mode": "ENABLED",
                    "profile_revision": status.profile_revision
                }
            }
        }
    });
    let text = serde_json::to_string_pretty(&value).unwrap();
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    storage
        .adapter()
        .write_text(&format!("{cluster_uri}/__cluster/state.json"), &text)
        .await
        .unwrap();
    let state_cas = format!("sha256:{:x}", Sha256::digest(text.as_bytes()));
    let binding = validate_runtime_binding(
        &storage,
        cluster_uri,
        RuntimeBindingRequest {
            graph_id: GRAPH_ID,
            graph_store_uri: db.uri(),
            expected_state_cas: &state_cas,
            state_revision: 1,
            declaration_revision: STREAM_DECLARATION_REVISION,
            declaration_digest: STREAM_DECLARATION_DIGEST,
            profile_mode: "ENABLED",
            profile_revision: status.profile_revision,
        },
    )
    .await
    .unwrap();
    let guard = mint_runtime_guard(binding, "memwal-stream-test-runtime", "omnigraph:test")
        .await
        .unwrap();
    let db = match Arc::try_unwrap(db) {
        Ok(db) => db,
        Err(_) => panic!("runtime fixture must own the sole engine handle"),
    };
    Arc::new(db.with_checked_cluster_stream_runtime(guard).await.unwrap())
}

pub async fn bind_checked_served_export(db: Arc<Omnigraph>, cluster_uri: &str) -> Arc<Omnigraph> {
    bind_checked_served_export_kind(db, cluster_uri, true).await
}

pub async fn bind_checked_unmanaged_terminal_export(
    db: Arc<Omnigraph>,
    cluster_uri: &str,
) -> Arc<Omnigraph> {
    bind_checked_served_export_kind(db, cluster_uri, false).await
}

async fn bind_checked_served_export_kind(
    db: Arc<Omnigraph>,
    cluster_uri: &str,
    managed: bool,
) -> Arc<Omnigraph> {
    let status = db.stream_status().await.unwrap();
    assert!(
        matches!(status.profile_mode, "DISABLED" | "RETIRED"),
        "served-export fixture requires a terminal profile, got {}",
        status.profile_mode
    );
    if !managed {
        assert!(
            status.profile_mode == "RETIRED"
                || (status.profile_mode == "DISABLED" && !status.tables.is_empty()),
            "an unmanaged export fixture requires RETIRED or enrolled DISABLED"
        );
    }
    let mut resources = serde_json::Map::new();
    resources.insert(
        "graph.knowledge".to_string(),
        serde_json::json!({ "digest": "memwal-stream-test-graph" }),
    );
    if managed {
        resources.insert(
            "streaming.knowledge".to_string(),
            serde_json::json!({
                "digest": STREAM_DECLARATION_DIGEST,
                "declaration_revision": STREAM_DECLARATION_REVISION,
                "streaming_enabled": false,
                "profile_mode": status.profile_mode,
                "profile_revision": status.profile_revision
            }),
        );
    }
    let value = serde_json::json!({
        "version": 1,
        "state_revision": 1,
        "applied_revision": {
            "config_digest": "memwal-stream-test-config",
            "resources": resources
        }
    });
    let text = serde_json::to_string_pretty(&value).unwrap();
    let cluster_uri = cluster_uri.trim_end_matches('/');
    let storage = storage_handle_for_uri(cluster_uri).unwrap();
    storage
        .adapter()
        .write_text(&format!("{cluster_uri}/__cluster/state.json"), &text)
        .await
        .unwrap();
    let state_cas = format!("sha256:{:x}", Sha256::digest(text.as_bytes()));
    let binding = if managed {
        validate_served_export_binding(
            &storage,
            cluster_uri,
            ServedExportBindingRequest {
                graph_id: GRAPH_ID,
                graph_store_uri: db.uri(),
                expected_state_cas: &state_cas,
                state_revision: 1,
                terminal: ServedExportTerminalRequest::Managed {
                    declaration_revision: STREAM_DECLARATION_REVISION,
                    declaration_digest: STREAM_DECLARATION_DIGEST,
                    profile_mode: &status.profile_mode,
                    profile_revision: status.profile_revision,
                },
            },
        )
        .await
        .unwrap()
    } else {
        validate_served_export_binding(
            &storage,
            cluster_uri,
            ServedExportBindingRequest {
                graph_id: GRAPH_ID,
                graph_store_uri: db.uri(),
                expected_state_cas: &state_cas,
                state_revision: 1,
                terminal: ServedExportTerminalRequest::UnmanagedTerminal {
                    graph_digest: "memwal-stream-test-graph",
                },
            },
        )
        .await
        .unwrap()
    };
    let guard = mint_served_export_guard(
        binding,
        "memwal-stream-test-served-export",
        "omnigraph:test-export",
    )
    .await
    .unwrap();
    let db = match Arc::try_unwrap(db) {
        Ok(db) => db,
        Err(_) => panic!("served-export fixture must own the sole engine handle"),
    };
    Arc::new(db.with_checked_cluster_served_export(guard).await.unwrap())
}
