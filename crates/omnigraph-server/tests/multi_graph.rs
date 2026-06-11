//! Cluster-mode boot and the concurrent branch-ops matrix.
//! Moved verbatim from tests/server.rs in the modularization.

use std::fs;

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use omnigraph_server::api::ErrorOutput;
use omnigraph_server::{AppState, build_app};
use serde_json::Value;
use tower::ServiceExt;


mod support;
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_branch_ops_morphological_matrix() {
    // Cell a: Merge × Merge, distinct targets.
    // Pre-fix on b09a097/22d76db: branch_merge_impl's swap-restore race
    // landed feature_a's content in target_b instead of target_a (and
    // vice versa — symmetric swap). Identity asserts catch both
    // asymmetric and symmetric variants.
    {
        let cell = "a:merge×merge:distinct-targets";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "feature-a-cella").await;
        h.insert_person("feature-a-cella", "EveA-cella", 22).await;
        h.create_branch("main", "feature-b-cella").await;
        h.insert_person("feature-b-cella", "FrankB-cella", 33).await;
        h.create_branch("main", "target-a-cella").await;
        h.create_branch("main", "target-b-cella").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("feature-a-cella".to_string(), "target-a-cella".to_string()),
                matrix::op_merge("feature-b-cella".to_string(), "target-b-cella".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] merge a", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] merge b", cell);
        h.assert_persons("target-a-cella", cell, &["EveA-cella"], &["FrankB-cella"])
            .await;
        h.assert_persons("target-b-cella", cell, &["FrankB-cella"], &["EveA-cella"])
            .await;
        h.assert_post_op_sentinel(cell, "sentinel-cella").await;
    }

    // Cell b: Merge × Merge, same target / distinct sources.
    // Both want to land in main. merge_exclusive serializes; both should
    // succeed and main should contain BOTH sources' contributions.
    {
        let cell = "b:merge×merge:same-target-distinct-sources";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "src-x-cellb").await;
        h.insert_person("src-x-cellb", "Xavier-cellb", 41).await;
        h.create_branch("main", "src-y-cellb").await;
        h.insert_person("src-y-cellb", "Yvonne-cellb", 42).await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("src-x-cellb".to_string(), "main".to_string()),
                matrix::op_merge("src-y-cellb".to_string(), "main".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] merge x", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] merge y", cell);
        h.assert_persons("main", cell, &["Xavier-cellb", "Yvonne-cellb"], &[])
            .await;
        h.assert_post_op_sentinel(cell, "sentinel-cellb").await;
    }

    // Cell c: Merge × Merge, same source / distinct targets (fanout).
    // One source merged into two targets simultaneously. merge_exclusive
    // serializes; both targets should reflect the source's content.
    {
        let cell = "c:merge×merge:same-source-distinct-targets";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "src-shared-cellc").await;
        h.insert_person("src-shared-cellc", "Sharon-cellc", 50)
            .await;
        h.create_branch("main", "tgt-1-cellc").await;
        h.create_branch("main", "tgt-2-cellc").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("src-shared-cellc".to_string(), "tgt-1-cellc".to_string()),
                matrix::op_merge("src-shared-cellc".to_string(), "tgt-2-cellc".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] merge into tgt-1", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] merge into tgt-2", cell);
        h.assert_persons("tgt-1-cellc", cell, &["Sharon-cellc"], &[])
            .await;
        h.assert_persons("tgt-2-cellc", cell, &["Sharon-cellc"], &[])
            .await;
        h.assert_post_op_sentinel(cell, "sentinel-cellc").await;
    }

    // Cell d: Merge × Change, both touching main. C2 permits both
    // succeed, or exactly one clean 409 if the merge detects target
    // movement after planning but before acquiring the queue.
    {
        let cell = "d:merge×change:into-target";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "feature-celld").await;
        h.insert_person("feature-celld", "EveD-celld", 22).await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("feature-celld".to_string(), "main".to_string()),
                matrix::op_change_insert("main".to_string(), "FrankD-celld".to_string(), 33),
            )
            .await;
        assert_eq!(sb.status, StatusCode::OK, "[{}] change", cell);
        assert!(
            sa.status == StatusCode::OK || sa.status == StatusCode::CONFLICT,
            "[{}] merge must be 200 or clean 409, got {}",
            cell,
            sa.status
        );
        if sa.status == StatusCode::OK {
            h.assert_persons("main", cell, &["EveD-celld", "FrankD-celld"], &[])
                .await;
        } else {
            let error: ErrorOutput = serde_json::from_slice(&sa.body).unwrap();
            let conflict = error
                .manifest_conflict
                .expect("merge 409 must include manifest_conflict");
            assert_eq!(
                conflict.table_key, "node:Person",
                "[{}] conflict table",
                cell
            );
            h.assert_persons("main", cell, &["FrankD-celld"], &["EveD-celld"])
                .await;
        }
        h.assert_post_op_sentinel(cell, "sentinel-celld").await;
    }

    // Cell e: Merge × BranchCreateFrom-target. Concurrent fork off the
    // merge target while the merge runs. Both should succeed; the new
    // branch should have a coherent view (either pre- or post-merge,
    // both valid). After both, target = main has the merged content.
    {
        let cell = "e:merge×branch_create_from:target";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "src-celle").await;
        h.insert_person("src-celle", "Eve-celle", 22).await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("src-celle".to_string(), "main".to_string()),
                matrix::op_branch_create("main".to_string(), "fork-celle".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] merge", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] branch_create_from", cell);
        // Main definitely has Eve.
        h.assert_persons("main", cell, &["Eve-celle"], &[]).await;
        // fork-celle was forked off main at SOME version; main's current
        // count is 5 (4 seeded + Eve). fork-celle has either 4 (pre-merge
        // snapshot) or 5 (post-merge snapshot); both are valid timings.
        let fork_count = h.person_count("fork-celle").await;
        assert!(
            fork_count == 4 || fork_count == 5,
            "[{}] fork-celle row count must be pre- or post-merge view (4 or 5), got {}",
            cell,
            fork_count
        );
        h.assert_post_op_sentinel(cell, "sentinel-celle").await;
    }

    // Cell f: BranchCreateFrom × BranchCreateFrom, distinct parents.
    // Pre-fix on f925ad1: swap-restore race in branch_create_from_impl
    // forked the new branch off the wrong parent. Identity asserts pin
    // that fork-from-A inherits A's content, fork-from-B inherits B's.
    {
        let cell = "f:branch_create_from×branch_create_from:distinct-parents";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "alpha-cellf").await;
        h.insert_person("alpha-cellf", "Eve-cellf", 22).await;
        h.create_branch("main", "beta-cellf").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_create("alpha-cellf".to_string(), "gamma-cellf".to_string()),
                matrix::op_branch_create("beta-cellf".to_string(), "delta-cellf".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] gamma create", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] delta create", cell);
        // gamma forks off alpha → must contain Eve.
        h.assert_persons("gamma-cellf", cell, &["Eve-cellf"], &[])
            .await;
        // delta forks off beta → must NOT contain Eve.
        h.assert_persons("delta-cellf", cell, &[], &["Eve-cellf"])
            .await;
        h.assert_post_op_sentinel(cell, "sentinel-cellf").await;
    }

    // Cell g: BranchCreateFrom × BranchDelete, unrelated branches.
    // Disjoint branches; both should complete cleanly without
    // interference.
    {
        let cell = "g:branch_create_from×branch_delete:unrelated";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "doomed-cellg").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_create("main".to_string(), "newborn-cellg".to_string()),
                matrix::op_branch_delete("doomed-cellg".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] create newborn", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] delete doomed", cell);
        // newborn-cellg exists with main's content.
        h.assert_persons("newborn-cellg", cell, &["Alice"], &[])
            .await;
        h.assert_post_op_sentinel(cell, "sentinel-cellg").await;
    }

    // Cell h: BranchDelete × BranchDelete, distinct branches. Both call
    // refresh() internally; verify no deadlock and both deletes land.
    {
        let cell = "h:branch_delete×branch_delete:distinct";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "doomed1-cellh").await;
        h.create_branch("main", "doomed2-cellh").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_delete("doomed1-cellh".to_string()),
                matrix::op_branch_delete("doomed2-cellh".to_string()),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] delete 1", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] delete 2", cell);
        // Verify both gone via /branches list (snapshot would still work
        // for a deleted branch via parent fallback in some paths, so we
        // use the explicit list).
        let r = h
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/branches")
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let body = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        let list_body: Value = serde_json::from_slice(&body).unwrap();
        let branches: Vec<&str> = list_body["branches"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(
            !branches.contains(&"doomed1-cellh"),
            "[{}] doomed1 still in branch list: {:?}",
            cell,
            branches
        );
        assert!(
            !branches.contains(&"doomed2-cellh"),
            "[{}] doomed2 still in branch list: {:?}",
            cell,
            branches
        );
        h.assert_post_op_sentinel(cell, "sentinel-cellh").await;
    }

    // Cell i: BranchDelete × Change, on a different branch. Delete one
    // branch while a /change runs on main. Both should succeed.
    {
        let cell = "i:branch_delete×change:distinct-branch";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "doomed-celli").await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_delete("doomed-celli".to_string()),
                matrix::op_change_insert("main".to_string(), "Pat-celli".to_string(), 44),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] delete", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] change", cell);
        h.assert_persons("main", cell, &["Pat-celli"], &[]).await;
        h.assert_post_op_sentinel(cell, "sentinel-celli").await;
    }

    // Cell j: BranchCreateFrom × Change, both on main. The fork timing
    // determines whether the new branch sees the change (pre or post).
    // Both valid. Main must contain the inserted row.
    {
        let cell = "j:branch_create_from×change:on-source";
        let h = matrix::Harness::new().await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_branch_create("main".to_string(), "twin-cellj".to_string()),
                matrix::op_change_insert("main".to_string(), "Quincy-cellj".to_string(), 55),
            )
            .await;
        assert_eq!(sa.status, StatusCode::OK, "[{}] branch_create", cell);
        assert_eq!(sb.status, StatusCode::OK, "[{}] change", cell);
        h.assert_persons("main", cell, &["Quincy-cellj"], &[]).await;
        // twin-cellj has either pre-change view (no Quincy) or
        // post-change view (with Quincy); either is valid.
        let twin_has_quincy = h.person_exists("twin-cellj", "Quincy-cellj").await;
        let _ = twin_has_quincy; // either valid timing — just ensure no panic
        h.assert_post_op_sentinel(cell, "sentinel-cellj").await;
    }

    // Cell k: reopen consistency. Run a representative concurrent pair,
    // drop the engine, reopen on a separate handle, verify state matches.
    {
        let cell = "k:reopen-after-pair";
        let h = matrix::Harness::new().await;
        h.create_branch("main", "src-cellk").await;
        h.insert_person("src-cellk", "Rita-cellk", 36).await;

        let (sa, sb) = h
            .run_pair(
                matrix::op_merge("src-cellk".to_string(), "main".to_string()),
                matrix::op_change_insert("main".to_string(), "Steve-cellk".to_string(), 37),
            )
            .await;
        assert_eq!(sb.status, StatusCode::OK, "[{}] change", cell);
        assert!(
            sa.status == StatusCode::OK || sa.status == StatusCode::CONFLICT,
            "[{}] merge must be 200 or clean 409, got {}",
            cell,
            sa.status
        );
        if sa.status == StatusCode::OK {
            h.assert_persons("main", cell, &["Rita-cellk", "Steve-cellk"], &[])
                .await;
        } else {
            let error: ErrorOutput = serde_json::from_slice(&sa.body).unwrap();
            let conflict = error
                .manifest_conflict
                .expect("merge 409 must include manifest_conflict");
            assert_eq!(
                conflict.table_key, "node:Person",
                "[{}] conflict table",
                cell
            );
            h.assert_persons("main", cell, &["Steve-cellk"], &["Rita-cellk"])
                .await;
        }

        // Reopen via a fresh AppState on the same graph.
        let graph_uri = format!("{}/server.omni", h._temp.path().display());
        let reopened = AppState::open(graph_uri.clone()).await.unwrap();
        let app2 = build_app(reopened);
        // Sanity: the same identity check via the new app must see
        // Rita and Steve.
        let r = app2
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/snapshot?branch=main")
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "[{}] reopen snapshot", cell);
        let body = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let person_rows = v["tables"]
            .as_array()
            .and_then(|tables| {
                tables
                    .iter()
                    .find(|t| t["table_key"].as_str() == Some("node:Person"))
            })
            .and_then(|t| t["row_count"].as_u64())
            .expect("reopen snapshot must include node:Person row_count");
        let expected_rows = if sa.status == StatusCode::OK { 6 } else { 5 };
        assert_eq!(
            person_rows, expected_rows,
            "[{}] reopened main should include seed (4) + committed concurrent writes",
            cell,
        );
    }
}

#[tokio::test]
async fn cluster_boot_serves_applied_state() {
    let temp = converged_cluster_dir("").await;
    let settings = cluster_settings(temp.path()).await.unwrap();
    let omnigraph_server::ServerConfigMode::Multi {
        graphs,
        config_path,
        server_policy_file,
    } = settings.mode
    else {
        panic!("cluster boot must select multi-graph routing");
    };
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0].graph_id, "knowledge");
    assert!(server_policy_file.is_none());

    let state =
        omnigraph_server::open_multi_graph_state(graphs, Vec::new(), None, config_path)
            .await
            .unwrap();
    let app = build_app(state);

    // The management surface keeps its closed-by-default contract: without a
    // cluster-scoped policy bundle there is no server-level Cedar engine, so
    // GET /graphs refuses even in cluster mode.
    let (status, body) = json_response(
        &app,
        Request::builder().uri("/graphs").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body) = json_response(
        &app,
        Request::builder()
            .uri("/graphs/knowledge/queries")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["queries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|q| q["name"] == "find_person"),
        "{body}"
    );

    let (status, body) = json_response(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/graphs/knowledge/queries/find_person")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"params":{"name":"nobody"}}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn cluster_boot_wires_policy_bindings_into_cedar_slots() {
    let temp = tempfile::tempdir().unwrap();
    drop(temp);
    let policy_block = r#"policies:
  graph_rules:
    file: ./graph.policy.yaml
    applies_to: [knowledge]
  cluster_rules:
    file: ./cluster.policy.yaml
    applies_to: [cluster]
"#;
    let temp = {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("people.pg"),
            "\nnode Person {\n  name: String @key\n}\n",
        )
        .unwrap();
        fs::write(
            temp.path().join("people.gq"),
            "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name }\n}\n",
        )
        .unwrap();
        fs::write(
            temp.path().join("graph.policy.yaml"),
            permit_all_policy_yaml(&["default"]),
        )
        .unwrap();
        fs::write(
            temp.path().join("cluster.policy.yaml"),
            permit_all_policy_yaml(&["default"]).replace("protected_branches: [main]\n", "protected_branches: [main]\nkind: server\n"),
        )
        .unwrap();
        fs::write(
            temp.path().join("cluster.yaml"),
            format!(
                r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
{policy_block}"#
            ),
        )
        .unwrap();
        let import = omnigraph_cluster::import_config_dir(temp.path()).await;
        assert!(import.ok, "{:?}", import.diagnostics);
        let apply = omnigraph_cluster::apply_config_dir(temp.path()).await;
        assert!(apply.ok && apply.converged, "{:?}", apply.diagnostics);
        temp
    };

    let settings = cluster_settings(temp.path()).await.unwrap();
    let omnigraph_server::ServerConfigMode::Multi {
        graphs,
        server_policy_file,
        ..
    } = settings.mode
    else {
        panic!("cluster boot must select multi-graph routing");
    };
    let graph_policy = graphs[0].policy_file.as_ref().expect("graph-bound bundle");
    assert!(
        graph_policy
            .to_string_lossy()
            .contains("__cluster/resources/policy/graph_rules/"),
        "{graph_policy:?}"
    );
    let server_policy = server_policy_file.expect("cluster-bound bundle");
    assert!(
        server_policy
            .to_string_lossy()
            .contains("__cluster/resources/policy/cluster_rules/"),
        "{server_policy:?}"
    );
}

#[tokio::test]
async fn cluster_boot_refusals() {
    // Mutual exclusion with --config / URI.
    let temp = converged_cluster_dir("").await;
    let dir = temp.path().to_path_buf();
    let err = omnigraph_server::load_server_settings(
        Some(&dir.join("omnigraph.yaml")),
        Some(&dir),
        None,
        None,
        None,
        true,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("exclusive boot source"), "{err}");
    let err = omnigraph_server::load_server_settings(
        None,
        Some(&dir),
        Some("file:///tmp/x.omni".to_string()),
        None,
        None,
        true,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("exclusive boot source"), "{err}");

    // Tampered catalog blob refuses boot with the remedy.
    let blob_dir = dir.join("__cluster/resources/query/knowledge/find_person");
    let blob = fs::read_dir(&blob_dir).unwrap().next().unwrap().unwrap().path();
    fs::write(&blob, "tampered").unwrap();
    let err = cluster_settings(&dir).await.unwrap_err();
    assert!(
        err.to_string().contains("catalog_payload_digest_mismatch"),
        "{err}"
    );
    assert!(err.to_string().contains("cluster refresh"), "{err}");

    // Missing state refuses with the import/apply remedy.
    let empty = tempfile::tempdir().unwrap();
    let err = cluster_settings(empty.path()).await.unwrap_err();
    assert!(err.to_string().contains("cluster_state_missing"), "{err}");
}
