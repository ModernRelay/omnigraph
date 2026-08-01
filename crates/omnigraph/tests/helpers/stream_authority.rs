use std::sync::Arc;

use omnigraph::db::{Omnigraph, StreamAuthorityRetirementPlan, StreamAuthorityRetirementResult};
use omnigraph::error::Result;
use omnigraph_control_authority::{
    AuthorityOperationClass, OfflineAuthorityRequest, RuntimeBindingRequest, StateLockAcquire,
    acquire_state_lock, mint_runtime_guard, validate_offline_guard, validate_runtime_binding,
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

pub async fn enable_stream_profile(db: &Omnigraph, cluster_uri: &str) {
    let cluster_uri = cluster_uri.trim_end_matches('/');
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
            expected_profile_revision: 1,
            operation_id: "memwal-stream-test-enable",
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

pub async fn disable_stream_profile(db: &Omnigraph, cluster_uri: &str) {
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
            operation_id: "memwal-stream-test-disable",
            operation: AuthorityOperationClass::StreamProfileDisable,
            actor: "operator:memwal-test",
            confirm_stream_offline: true,
        },
    )
    .await
    .unwrap();
    let authority = db.check_cluster_apply_authority(guard).await.unwrap();
    let result = db.set_streaming_profile_checked(authority).await.unwrap();
    assert!(!result.streaming_enabled);
    assert_eq!(db.stream_status().await.unwrap().profile_mode, "DISABLED");
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
