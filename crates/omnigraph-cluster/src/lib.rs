use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;

use omnigraph::db::{Omnigraph, ReadTarget, SchemaApplyOptions};
use omnigraph_compiler::SchemaMigrationPlan;
use omnigraph_compiler::build_catalog;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::query::typecheck::typecheck_query_decl;
use omnigraph_compiler::schema::parser::parse_schema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

pub mod failpoints;

mod config;
mod types;
mod diff;
mod serve;
mod sweep;
mod store;
use store::{ClusterStore, StateLockGuard, StateSnapshot};
pub use types::*;
use types::*;
pub use serve::{ServingGraph, ServingPolicy, ServingQuery, ServingSnapshot, cluster_graph_ids, cluster_root_for_graph_uri, read_serving_snapshot, read_serving_snapshot_from_storage, resolve_graph_storage_uri};
use config::{QueriesDecl, observe_declared_graphs, validate_cluster_header, future_field_diagnostics, initial_import_state, observe_live_graph, preview_schema_migration, state_resource_digests, graph_address, policy_address, query_address, schema_address, load_desired, normalize_policy_target, parse_cluster_config, resolve_config_path, resolve_query_decls, validate_id, validate_query_source};
use diff::{FailedGraphOrigin, ResourceKind, append_policy_binding_changes, approved_resources, classify_changes, compute_approvals, compute_blast_radius, demote_dependents_of_failed_graphs, diff_resources, resource_kind};
use sweep::{mark_approvals_consumed, record_approval_consumed, sweep_recovery_sidecars, tombstone_graph_subtree, warn_pending_recovery_sidecars};

pub const CLUSTER_CONFIG_FILE: &str = "cluster.yaml";
pub const CLUSTER_GRAPHS_DIR: &str = "graphs";
pub const CLUSTER_STATE_DIR: &str = "__cluster";
pub const CLUSTER_STATE_FILE: &str = "__cluster/state.json";
pub const CLUSTER_LOCK_FILE: &str = "__cluster/lock.json";
pub const CLUSTER_RESOURCES_DIR: &str = "__cluster/resources";
pub const CLUSTER_RECOVERIES_DIR: &str = "__cluster/recoveries";
pub const CLUSTER_APPROVALS_DIR: &str = "__cluster/approvals";

/// The store for a load outcome: the declared `storage:` root when present,
/// the config directory itself otherwise. A bad root is a loud error.
fn store_for(
    config_dir: &Path,
    storage_root: Option<&str>,
) -> Result<ClusterStore, Diagnostic> {
    match storage_root {
        Some(root) => ClusterStore::for_storage_root(root),
        None => Ok(ClusterStore::for_config_dir(config_dir)),
    }
}

pub fn validate_config_dir(config_dir: impl AsRef<Path>) -> ValidateOutput {
    let outcome = load_desired(config_dir.as_ref());
    let (resource_digests, resources, dependencies) = match outcome.desired {
        Some(desired) => (
            desired.resource_digests,
            desired.resources,
            desired.dependencies,
        ),
        None => (BTreeMap::new(), Vec::new(), Vec::new()),
    };
    let ok = !has_errors(&outcome.diagnostics);

    ValidateOutput {
        ok,
        config_dir: display_path(&outcome.config_dir),
        config_file: display_path(&outcome.config_file),
        resource_digests,
        resources,
        dependencies,
        diagnostics: outcome.diagnostics,
    }
}

pub async fn plan_config_dir(config_dir: impl AsRef<Path>) -> PlanOutput {
    let outcome = load_desired(config_dir.as_ref());
    let mut diagnostics = outcome.diagnostics;
    let storage_root = outcome
        .desired
        .as_ref()
        .and_then(|desired| desired.storage_root.clone());
    let backend = match store_for(&outcome.config_dir, storage_root.as_deref()) {
        Ok(backend) => backend,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            ClusterStore::for_config_dir(&outcome.config_dir)
        }
    };
    let mut observations = backend.observations();

    let Some(desired) = outcome.desired else {
        return PlanOutput {
            ok: false,
            config_dir: display_path(&outcome.config_dir),
            desired_revision: DesiredRevision {
                config_digest: None,
            },
            resource_digests: BTreeMap::new(),
            dependencies: Vec::new(),
            state_observations: observations,
            changes: Vec::new(),
            blast_radius: Vec::new(),
            approvals_required: Vec::new(),
            diagnostics,
        };
    };

    if has_errors(&diagnostics) {
        return PlanOutput {
            ok: false,
            config_dir: display_path(&desired.config_dir),
            desired_revision: DesiredRevision {
                config_digest: Some(desired.config_digest),
            },
            resource_digests: desired.resource_digests,
            dependencies: desired.dependencies,
            state_observations: observations,
            changes: Vec::new(),
            blast_radius: Vec::new(),
            approvals_required: Vec::new(),
            diagnostics,
        };
    }

    let _lock_guard = if desired.state_lock {
        match backend.acquire_lock("plan", &mut observations).await {
            Ok(guard) => Some(guard),
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                None
            }
        }
    } else {
        diagnostics.push(Diagnostic::warning(
            "state_lock_disabled",
            "state.lock",
            "state.lock is false; plan read state without acquiring the cluster state lock",
        ));
        None
    };

    // Plan is read-only: pending sidecars are reported, never acted on
    // (RFC-004 open question 3 keeps read-only commands warn-only).
    warn_pending_recovery_sidecars(&desired.config_dir, &mut diagnostics);

    let mut prior_resources = BTreeMap::new();
    let mut prior_state: Option<ClusterState> = None;
    if !has_errors(&diagnostics) {
        match backend.read_state(&mut observations).await {
            Ok(snapshot) => {
                if let Some(state) = snapshot.state {
                    prior_resources = state_resource_digests(&state);
                    prior_state = Some(state);
                }
            }
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
    }

    let mut changes = if has_errors(&diagnostics) {
        Vec::new()
    } else {
        diff_resources(&prior_resources, &desired.resource_digests)
    };
    if !has_errors(&diagnostics) {
        append_policy_binding_changes(&mut changes, prior_state.as_ref(), &desired);
    }
    // Plan previews dispositions without sweeping; a pending recovery is
    // surfaced as the cluster_recovery_pending warning above instead.
    let artifacts = backend.list_approval_artifacts(&mut diagnostics).await;
    let approved = approved_resources(
        &artifacts,
        &changes,
        &desired.config_digest,
        &mut diagnostics,
    );
    classify_changes(&mut changes, &desired.dependencies, &BTreeSet::new(), &approved);

    // Embed real migration steps for schema updates so plan is a data-aware
    // preview; failures degrade to the digest diff with a warning.
    for change in &mut changes {
        if change.operation != PlanOperation::Update {
            continue;
        }
        let ResourceKind::Schema(graph_id) = resource_kind(&change.resource) else {
            continue;
        };
        let graph_uri = backend.graph_root(&graph_id);
        let source_path = desired
            .resources
            .iter()
            .find(|resource| resource.address == change.resource)
            .and_then(|resource| resource.path.clone());
        let preview = match source_path {
            Some(path) => preview_schema_migration(&graph_uri, &path).await,
            None => Err("no schema source recorded".to_string()),
        };
        match preview {
            Ok(migration) => change.migration = Some(migration),
            Err(err) => diagnostics.push(Diagnostic::warning(
                "schema_preview_unavailable",
                change.resource.clone(),
                format!("could not preview the schema migration: {err}"),
            )),
        }
    }
    let blast_radius = compute_blast_radius(&changes, &desired.dependencies);
    let approvals_required = compute_approvals(&changes, &approved);
    let ok = !has_errors(&diagnostics);

    PlanOutput {
        ok,
        config_dir: display_path(&desired.config_dir),
        desired_revision: DesiredRevision {
            config_digest: Some(desired.config_digest),
        },
        resource_digests: desired.resource_digests,
        dependencies: desired.dependencies,
        state_observations: observations,
        changes,
        blast_radius,
        approvals_required,
        diagnostics,
    }
}

/// Config-only `cluster apply` (Stage 3A): execute the query/policy subset of
/// the plan against the local cluster catalog. The plan is recomputed under
/// the state lock, so freshness is structural; the state CAS inside
/// `write_state` is the second fence. Graph/schema changes are never executed
/// here — they are deferred to the graph-lifecycle phase and reported loudly.
///
/// Payloads are content-addressed and written BEFORE the state CAS because
/// state is the publish point: a failure after payload writes leaves inert
/// digest-named blobs and no success acknowledgement; re-running apply is the
/// repair.
/// Options for `cluster apply`. `actor` attributes graph-moving operations
/// (recorded in sidecars and audit entries, threaded to the engine's
/// `apply_schema_as` so Cedar enforcement fires wherever a policy checker is
/// installed).
#[derive(Debug, Clone, Default)]
pub struct ApplyOptions {
    pub actor: Option<String>,
}

pub async fn apply_config_dir(config_dir: impl AsRef<Path>) -> ApplyOutput {
    apply_config_dir_with_options(config_dir, ApplyOptions::default()).await
}

pub async fn apply_config_dir_with_options(
    config_dir: impl AsRef<Path>,
    options: ApplyOptions,
) -> ApplyOutput {
    let outcome = load_desired(config_dir.as_ref());
    let mut diagnostics = outcome.diagnostics;
    let storage_root = outcome
        .desired
        .as_ref()
        .and_then(|desired| desired.storage_root.clone());
    let backend = match store_for(&outcome.config_dir, storage_root.as_deref()) {
        Ok(backend) => backend,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            ClusterStore::for_config_dir(&outcome.config_dir)
        }
    };
    let mut observations = backend.observations();

    let actor_for_output = options.actor.clone();
    let early_return = |config_dir: String,
                        config_digest: Option<String>,
                        observations: StateObservations,
                        changes: Vec<PlanChange>,
                        resource_statuses: BTreeMap<String, ResourceStatusRecord>,
                        diagnostics: Vec<Diagnostic>| {
        ApplyOutput {
            ok: !has_errors(&diagnostics),
            config_dir,
            actor: actor_for_output.clone(),
            desired_revision: DesiredRevision {
                config_digest,
            },
            state_observations: observations,
            changes,
            applied_count: 0,
            deferred_count: 0,
            converged: false,
            state_written: false,
            resource_statuses,
            diagnostics,
        }
    };

    let Some(desired) = outcome.desired else {
        return early_return(
            display_path(&outcome.config_dir),
            None,
            observations,
            Vec::new(),
            BTreeMap::new(),
            diagnostics,
        );
    };

    if has_errors(&diagnostics) {
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            Vec::new(),
            BTreeMap::new(),
            diagnostics,
        );
    }

    // Named guard: the lock must be held until the state outcome is recorded.
    let _lock_guard = if desired.state_lock {
        match backend.acquire_lock("apply", &mut observations).await {
            Ok(guard) => Some(guard),
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                None
            }
        }
    } else {
        diagnostics.push(Diagnostic::warning(
            "state_lock_disabled",
            "state.lock",
            "state.lock is false; apply wrote state without acquiring the cluster state lock",
        ));
        None
    };

    if has_errors(&diagnostics) {
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            Vec::new(),
            BTreeMap::new(),
            diagnostics,
        );
    }

    let snapshot = match backend.read_state(&mut observations).await {
        Ok(snapshot) => snapshot,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return early_return(
                display_path(&desired.config_dir),
                Some(desired.config_digest),
                observations,
                Vec::new(),
                BTreeMap::new(),
                diagnostics,
            );
        }
    };
    let expected_cas = snapshot.state_cas;
    let Some(mut state) = snapshot.state else {
        diagnostics.push(Diagnostic::error(
            "state_missing",
            CLUSTER_STATE_FILE,
            "apply requires an existing state.json; run `cluster import` to bootstrap state",
        ));
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            Vec::new(),
            BTreeMap::new(),
            diagnostics,
        );
    };

    // Snapshot the as-read state BEFORE the sweep so sweep mutations count as
    // changes for the final dirty check and get persisted by the state CAS.
    let before_value =
        serde_json::to_value(&state).expect("cluster state must serialize deterministically");
    let sweep = sweep_recovery_sidecars(&backend, &mut state, &mut diagnostics).await;

    let prior_resources = state_resource_digests(&state);
    let mut changes = diff_resources(&prior_resources, &desired.resource_digests);
    append_policy_binding_changes(&mut changes, Some(&state), &desired);
    let approval_artifacts = backend.list_approval_artifacts(&mut diagnostics).await;
    let approved = approved_resources(
        &approval_artifacts,
        &changes,
        &desired.config_digest,
        &mut diagnostics,
    );
    classify_changes(
        &mut changes,
        &desired.dependencies,
        &sweep.pending_graphs,
        &approved,
    );

    // Defensive invariant: nothing the approval gate covers may be executable
    // WITHOUT a matching approval. Gated changes with a valid artifact are the
    // sanctioned exception (stage 4C).
    let approvals = compute_approvals(&changes, &approved);
    let approval_violation = changes.iter().any(|change| {
        change.disposition == Some(ApplyDisposition::Applied)
            && approvals
                .iter()
                .any(|approval| approval.resource == change.resource && !approval.satisfied)
    });
    if approval_violation {
        diagnostics.push(Diagnostic::error(
            "apply_approval_invariant_violation",
            "changes",
            "an executable change requires approval; refusing to apply",
        ));
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            changes,
            state.resource_statuses,
            diagnostics,
        );
    }

    // Graph creates execute first (RFC-004 §D5), sequentially, sidecar-fenced:
    // sidecar written before the init, rewritten with the post-init manifest
    // version, deleted only after the final state CAS lands. A failure stops
    // further graph-moving work and demotes that graph's dependents.
    let source_paths: BTreeMap<&str, &str> = desired
        .resources
        .iter()
        .filter_map(|resource| {
            resource
                .path
                .as_deref()
                .map(|path| (resource.address.as_str(), path))
        })
        .collect();
    let graph_creates_to_run: Vec<String> = changes
        .iter()
        .filter(|change| {
            change.disposition == Some(ApplyDisposition::Applied)
                && change.operation == PlanOperation::Create
                && matches!(resource_kind(&change.resource), ResourceKind::Graph(_))
        })
        .filter_map(|change| change.resource.strip_prefix("graph.").map(str::to_string))
        .collect();
    let mut completed_op_sidecars: Vec<String> = Vec::new();
    let mut failed_graphs: BTreeMap<String, FailedGraphOrigin> = BTreeMap::new();
    let mut graph_moving_aborted = false;
    for graph_id in &graph_creates_to_run {
        if graph_moving_aborted {
            // A prior create failed: stop graph-moving work (loud partials).
            diagnostics.push(Diagnostic::warning(
                "graph_create_skipped",
                graph_address(graph_id),
                "skipped after an earlier graph create failed in this run",
            ));
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
            continue;
        }
        let Some(desired_graph) = desired.graphs.iter().find(|graph| &graph.id == graph_id)
        else {
            continue;
        };
        let graph_uri = backend.graph_root(graph_id);
        let mut sidecar = RecoverySidecar {
            schema_version: 1,
            operation_id: Ulid::new().to_string(),
            started_at: now_rfc3339(),
            actor: options.actor.clone(),
            kind: RecoverySidecarKind::GraphCreate,
            graph_id: graph_id.clone(),
            graph_uri: graph_uri.clone(),
            observed_manifest_version: None,
            expected_manifest_version: None,
            desired_schema_digest: desired_graph.schema_digest.clone(),
            state_cas_base: expected_cas.clone(),
            approval_id: None,
        };
        let sidecar_path = match backend.write_recovery_sidecar(&sidecar).await {
            Ok(path) => path,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
                graph_moving_aborted = true;
                continue;
            }
        };
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.before_graph_create") {
            // Simulated crash before the init: the sidecar stays for the
            // sweep (row 1: root absent -> intent removed next run).
            diagnostics.push(diagnostic);
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
            graph_moving_aborted = true;
            continue;
        }
        // Re-read + re-verify the schema source under the lock — the same
        // TOCTOU posture as write_resource_payload.
        let schema_source = source_paths
            .get(schema_address(graph_id).as_str())
            .ok_or_else(|| {
                Diagnostic::error(
                    "graph_create_failed",
                    graph_address(graph_id),
                    "no schema source recorded for graph",
                )
            })
            .and_then(|path| {
                fs::read_to_string(Path::new(path)).map_err(|err| {
                    Diagnostic::error(
                        "graph_create_failed",
                        graph_address(graph_id),
                        format!("could not read schema source '{path}': {err}"),
                    )
                })
            })
            .and_then(|source| {
                if sha256_hex(source.as_bytes()) == desired_graph.schema_digest {
                    Ok(source)
                } else {
                    Err(Diagnostic::error(
                        "resource_content_changed",
                        schema_address(graph_id),
                        "schema source changed while apply was running; re-run `cluster apply`",
                    ))
                }
            });
        let schema_source = match schema_source {
            Ok(source) => source,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                backend.delete_object(&sidecar_path).await; // nothing moved
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
                graph_moving_aborted = true;
                continue;
            }
        };
        match Omnigraph::init(&graph_uri, &schema_source).await {
            Ok(_) => {}
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "graph_create_failed",
                    graph_address(graph_id),
                    format!("could not initialize graph at '{graph_uri}': {err}"),
                ));
                // The sidecar stays: the sweep classifies whether the failed
                // init left a partial root (row 5) or nothing (row 1).
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
                graph_moving_aborted = true;
                continue;
            }
        }
        // Record the post-init pin in the sidecar (best effort — a failure
        // here leaves expected = null and the sweep classifies by digest).
        if let Ok(db) = Omnigraph::open_read_only(&graph_uri).await {
            if let Ok(snapshot) = db.snapshot_of(ReadTarget::branch("main")).await {
                sidecar.expected_manifest_version = Some(snapshot.version());
                if let Err(diagnostic) = backend.write_recovery_sidecar(&sidecar).await {
                    diagnostics.push(diagnostic);
                }
            }
        }
        // Crash point: the graph exists, the cluster state does not record it
        // yet. A failure here must acknowledge nothing; the next run's sweep
        // rolls the ledger forward (row 4).
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.after_graph_create") {
            diagnostics.push(diagnostic);
            return early_return(
                display_path(&desired.config_dir),
                Some(desired.config_digest),
                observations,
                changes,
                state.resource_statuses,
                diagnostics,
            );
        }
        completed_op_sidecars.push(sidecar_path);
    }

    // Schema applies execute next (RFC-004 §D5): the first cluster operation
    // that moves an EXISTING graph manifest, sidecar-fenced the same way.
    let schema_updates_to_run: Vec<String> = changes
        .iter()
        .filter(|change| {
            change.disposition == Some(ApplyDisposition::Applied)
                && change.operation == PlanOperation::Update
                && matches!(resource_kind(&change.resource), ResourceKind::Schema(_))
        })
        .filter_map(|change| change.resource.strip_prefix("schema.").map(str::to_string))
        .collect();
    for graph_id in &schema_updates_to_run {
        if graph_moving_aborted {
            diagnostics.push(Diagnostic::warning(
                "schema_apply_skipped",
                schema_address(graph_id),
                "skipped after an earlier graph-moving operation failed in this run",
            ));
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
            continue;
        }
        let Some(desired_graph) = desired.graphs.iter().find(|graph| &graph.id == graph_id)
        else {
            continue;
        };
        let graph_uri = backend.graph_root(graph_id);
        // Read-write open: the engine's own recovery sweep runs here, which
        // is exactly what we want before moving its manifest.
        let db = match Omnigraph::open(&graph_uri).await {
            Ok(db) => db,
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "schema_apply_failed",
                    schema_address(graph_id),
                    format!("could not open graph at '{graph_uri}': {err}"),
                ));
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
                graph_moving_aborted = true;
                continue;
            }
        };
        let observed_manifest_version = match db.snapshot_of(ReadTarget::branch("main")).await {
            Ok(snapshot) => Some(snapshot.version()),
            Err(_) => None,
        };
        let mut sidecar = RecoverySidecar {
            schema_version: 1,
            operation_id: Ulid::new().to_string(),
            started_at: now_rfc3339(),
            actor: options.actor.clone(),
            kind: RecoverySidecarKind::SchemaApply,
            graph_id: graph_id.clone(),
            graph_uri: graph_uri.clone(),
            observed_manifest_version,
            expected_manifest_version: None,
            desired_schema_digest: desired_graph.schema_digest.clone(),
            state_cas_base: expected_cas.clone(),
            approval_id: None,
        };
        let sidecar_path = match backend.write_recovery_sidecar(&sidecar).await {
            Ok(path) => path,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
                graph_moving_aborted = true;
                continue;
            }
        };
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.before_schema_apply") {
            // Simulated crash before the engine call: the sidecar stays; the
            // sweep retires it next run (ledger still consistent with live).
            diagnostics.push(diagnostic);
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
            graph_moving_aborted = true;
            continue;
        }
        // Re-read + digest-verify the desired schema source under the lock.
        let schema_source = source_paths
            .get(schema_address(graph_id).as_str())
            .ok_or_else(|| {
                Diagnostic::error(
                    "schema_apply_failed",
                    schema_address(graph_id),
                    "no schema source recorded for graph",
                )
            })
            .and_then(|path| {
                fs::read_to_string(Path::new(path)).map_err(|err| {
                    Diagnostic::error(
                        "schema_apply_failed",
                        schema_address(graph_id),
                        format!("could not read schema source '{path}': {err}"),
                    )
                })
            })
            .and_then(|source| {
                if sha256_hex(source.as_bytes()) == desired_graph.schema_digest {
                    Ok(source)
                } else {
                    Err(Diagnostic::error(
                        "resource_content_changed",
                        schema_address(graph_id),
                        "schema source changed while apply was running; re-run `cluster apply`",
                    ))
                }
            });
        let schema_source = match schema_source {
            Ok(source) => source,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                backend.delete_object(&sidecar_path).await; // nothing moved
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
                graph_moving_aborted = true;
                continue;
            }
        };
        // Soft drops only: allow_data_loss stays false until the approval
        // artifacts of stage 4C exist (RFC-004 §D4).
        match db
            .apply_schema_as(
                &schema_source,
                SchemaApplyOptions::default(),
                options.actor.as_deref(),
            )
            .await
        {
            Ok(result) => {
                sidecar.expected_manifest_version = Some(result.manifest_version);
                if let Err(diagnostic) = backend.write_recovery_sidecar(&sidecar).await {
                    diagnostics.push(diagnostic);
                }
            }
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "schema_apply_failed",
                    schema_address(graph_id),
                    format!("schema apply failed on '{graph_uri}': {err}"),
                ));
                // Sidecar stays; the sweep retires it (live digest unchanged
                // == ledger consistent) or flags real movement.
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
                graph_moving_aborted = true;
                continue;
            }
        }
        // Crash point: the manifest moved, the ledger does not record it yet.
        // A failure here acknowledges nothing; the sweep rolls forward.
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.after_schema_apply") {
            diagnostics.push(diagnostic);
            return early_return(
                display_path(&desired.config_dir),
                Some(desired.config_digest),
                observations,
                changes,
                state.resource_statuses,
                diagnostics,
            );
        }
        completed_op_sidecars.push(sidecar_path);
    }

    if !failed_graphs.is_empty() {
        demote_dependents_of_failed_graphs(&mut changes, &failed_graphs, &desired.dependencies);
    }

    for change in &changes {
        match change.disposition {
            Some(ApplyDisposition::Deferred) => diagnostics.push(Diagnostic::warning(
                "apply_unsupported_change",
                change.resource.clone(),
                "graph/schema changes are not applied in this stage; they are deferred to the graph-lifecycle phase",
            )),
            Some(ApplyDisposition::Blocked) => diagnostics.push(Diagnostic::warning(
                "apply_dependency_blocked",
                change.resource.clone(),
                format!(
                    "blocked by an unapplied or missing dependency ({})",
                    change.reason.as_deref().unwrap_or("dependency")
                ),
            )),
            _ => {}
        }
    }

    // Payload phase: content-addressed writes before the state CAS. Any
    // failure aborts before state moves; blobs already written are inert.
    // Gate on payload-phase errors only — sweep errors (e.g. a kept row-5
    // sidecar) must not abort the run, or their statuses would never persist.
    let errors_before_payloads = count_errors(&diagnostics);
    for change in &changes {
        if change.disposition != Some(ApplyDisposition::Applied)
            || change.operation == PlanOperation::Delete
        {
            continue;
        }
        let kind = resource_kind(&change.resource);
        let digest = change
            .after_digest
            .as_deref()
            .expect("create/update always carries an after digest");
        if ClusterStore::payload_relative(&kind, digest).is_none() {
            continue;
        }
        let Some(source) = source_paths.get(change.resource.as_str()) else {
            diagnostics.push(Diagnostic::error(
                "resource_payload_write_error",
                change.resource.clone(),
                "no source file recorded for resource",
            ));
            continue;
        };
        if let Err(diagnostic) =
            write_resource_payload(&backend, &kind, Path::new(source), digest, &change.resource)
                .await
        {
            diagnostics.push(diagnostic);
        }
    }
    if count_errors(&diagnostics) > errors_before_payloads {
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            changes,
            state.resource_statuses,
            diagnostics,
        );
    }

    // Crash point: payloads are on disk, state has not moved. A failure here
    // must leave state.json byte-identical and acknowledge nothing; re-running
    // apply repairs via the skip-if-exists blob reuse.
    if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.after_payload_phase") {
        diagnostics.push(diagnostic);
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            changes,
            state.resource_statuses,
            diagnostics,
        );
    }

    // Approved graph deletes execute LAST (RFC-004 §D5): catalog writes for
    // surviving resources land first, then the irreversible work.
    let graph_deletes_to_run: Vec<String> = changes
        .iter()
        .filter(|change| {
            change.disposition == Some(ApplyDisposition::Applied)
                && change.operation == PlanOperation::Delete
                && matches!(resource_kind(&change.resource), ResourceKind::Graph(_))
        })
        .filter_map(|change| change.resource.strip_prefix("graph.").map(str::to_string))
        .collect();
    let mut executed_deletes: Vec<(String, Option<String>)> = Vec::new(); // (graph_id, approval_id)
    let mut consumed_approval_ids: Vec<String> = Vec::new();
    for graph_id in &graph_deletes_to_run {
        if graph_moving_aborted {
            diagnostics.push(Diagnostic::warning(
                "graph_delete_skipped",
                graph_address(graph_id),
                "skipped after an earlier graph-moving operation failed in this run",
            ));
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphDelete);
            continue;
        }
        let graph_addr = graph_address(graph_id);
        // Re-locate the consumable approval (classification verified one exists).
        let approval_id = approval_artifacts
            .iter()
            .map(|(_, artifact)| artifact)
            .find(|artifact| {
                artifact.consumed_at.is_none()
                    && artifact.resource == graph_addr
                    && artifact.bound_config_digest == desired.config_digest
            })
            .map(|artifact| artifact.approval_id.clone());
        let graph_uri = backend.graph_root(graph_id);
        let observed_manifest_version = match Omnigraph::open_read_only(&graph_uri).await {
            Ok(db) => match db.snapshot_of(ReadTarget::branch("main")).await {
                Ok(snapshot) => Some(snapshot.version()),
                Err(_) => None,
            },
            Err(_) => None, // partial/unopenable roots still get deleted
        };
        let sidecar = RecoverySidecar {
            schema_version: 1,
            operation_id: Ulid::new().to_string(),
            started_at: now_rfc3339(),
            actor: options.actor.clone(),
            kind: RecoverySidecarKind::GraphDelete,
            graph_id: graph_id.clone(),
            graph_uri: graph_uri.clone(),
            observed_manifest_version,
            expected_manifest_version: None, // no post-op manifest exists
            desired_schema_digest: String::new(),
            state_cas_base: expected_cas.clone(),
            approval_id: approval_id.clone(),
        };
        let sidecar_path = match backend.write_recovery_sidecar(&sidecar).await {
            Ok(path) => path,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphDelete);
                graph_moving_aborted = true;
                continue;
            }
        };
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.before_graph_delete") {
            // Simulated crash before removal: row 8 retires the intent and
            // the still-valid approval lets a later run retry.
            diagnostics.push(diagnostic);
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphDelete);
            graph_moving_aborted = true;
            continue;
        }
        // Prefix delete through the storage layer: remove_dir_all locally,
        // list+delete on object stores (idempotent; already-gone is fine).
        match backend.delete_graph_root(&graph_uri).await {
            Ok(()) => {}
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "graph_delete_failed",
                    graph_addr.clone(),
                    format!("could not remove graph root '{graph_uri}': {err}"),
                ));
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphDelete);
                graph_moving_aborted = true;
                continue;
            }
        }
        // Crash point: the root is gone, the ledger does not record it yet.
        // The sweep rolls forward (row 7b) and consumes the approval.
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.after_graph_delete") {
            diagnostics.push(diagnostic);
            return early_return(
                display_path(&desired.config_dir),
                Some(desired.config_digest),
                observations,
                changes,
                state.resource_statuses,
                diagnostics,
            );
        }
        executed_deletes.push((graph_id.clone(), approval_id.clone()));
        if let Some(approval_id) = approval_id {
            consumed_approval_ids.push(approval_id);
        }
        completed_op_sidecars.push(sidecar_path);
    }
    if !failed_graphs.is_empty() {
        demote_dependents_of_failed_graphs(&mut changes, &failed_graphs, &desired.dependencies);
    }

    // State mutation. Apply owns query/policy statuses only; graph/schema
    // statuses belong to refresh/import observation and must not be clobbered
    // (the sweep above is the one exception: it owns recovery statuses).
    let mut new_state = state.clone();
    for change in &changes {
        match change.disposition {
            Some(ApplyDisposition::Applied) => match change.operation {
                PlanOperation::Create | PlanOperation::Update => {
                    new_state.applied_revision.resources.insert(
                        change.resource.clone(),
                        StateResource {
                            digest: change
                                .after_digest
                                .clone()
                                .expect("create/update always carries an after digest"),
                            // Policies record their applied bindings so the
                            // ledger is serving-sufficient (RFC-005 §D3).
                            applies_to: desired
                                .policy_bindings
                                .get(&change.resource)
                                .cloned(),
                        },
                    );
                    set_resource_status_applied(&mut new_state, &change.resource);
                }
                PlanOperation::Delete => {
                    new_state.applied_revision.resources.remove(&change.resource);
                    new_state.resource_statuses.remove(&change.resource);
                }
            },
            Some(ApplyDisposition::Blocked) => {
                // The sweep owns recovery statuses (Drifted/Error with their
                // conditions); a generic Blocked must not clobber them.
                if change.reason.as_deref() != Some("cluster_recovery_pending") {
                    set_resource_status(
                        &mut new_state,
                        &change.resource,
                        ResourceLifecycleStatus::Blocked,
                        change.reason.as_deref().unwrap_or("dependency_not_applied"),
                        "waiting on an unapplied or missing dependency",
                    );
                }
            }
            _ => {}
        }
    }
    for (graph_id, approval_id) in &executed_deletes {
        tombstone_graph_subtree(
            &mut new_state,
            graph_id,
            approval_id.as_deref(),
            options.actor.as_deref(),
        );
        if let Some(approval_id) = approval_id {
            record_approval_consumed(&mut new_state, approval_id, "apply");
        }
    }
    recompute_state_graph_digests(&mut new_state, &desired);

    let mut residual = diff_resources(
        &state_resource_digests(&new_state),
        &desired.resource_digests,
    );
    append_policy_binding_changes(&mut residual, Some(&new_state), &desired);
    let converged = residual.is_empty();
    if converged {
        new_state.applied_revision.config_digest = Some(desired.config_digest.clone());
    }

    let after_value =
        serde_json::to_value(&new_state).expect("cluster state must serialize deterministically");
    let mut state_written = false;
    let mut state_write_failed = false;
    if after_value != before_value {
        new_state.state_revision = new_state.state_revision.saturating_add(1);
        // The failpoint error routes through state_write_failed so the
        // persisted-statuses revert contract below is exercised; a cfg_callback
        // on this point can mutate state.json to simulate a concurrent writer,
        // making write_state's CAS check fail organically.
        let write_result = match failpoints::maybe_fail("cluster_apply.before_state_write") {
            Ok(()) => {
                backend
                    .write_state(&new_state, expected_cas.as_deref(), &mut observations)
                    .await
            }
            Err(diagnostic) => Err(diagnostic),
        };
        match write_result {
            Ok(()) => state_written = true,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                state_write_failed = true;
            }
        }
    }
    // Completed (rows 2/4) sweep sidecars are deleted only once their outcome
    // is durably recorded; on a failed write they stay and re-sweep next run.
    if !state_write_failed {
        for sidecar_uri in sweep
            .completed_sidecars
            .iter()
            .chain(completed_op_sidecars.iter())
        {
            backend.delete_object(sidecar_uri).await;
        }
        let mut all_consumed = sweep.consumed_approvals.clone();
        all_consumed.extend(consumed_approval_ids.iter().cloned());
        mark_approvals_consumed(&backend, &all_consumed).await;
    }
    // On a failed state write, report the statuses that are actually on disk
    // (the pre-apply snapshot), not the in-memory mutations that were never
    // persisted — automation reading `resource_statuses` independently of `ok`
    // must not see phantom status updates.
    let resource_statuses = if state_write_failed {
        state.resource_statuses
    } else {
        new_state.resource_statuses
    };

    let applied_count = changes
        .iter()
        .filter(|change| change.disposition == Some(ApplyDisposition::Applied))
        .count();
    let deferred_count = changes
        .iter()
        .filter(|change| {
            matches!(
                change.disposition,
                Some(ApplyDisposition::Deferred) | Some(ApplyDisposition::Blocked)
            )
        })
        .count();

    ApplyOutput {
        ok: !has_errors(&diagnostics),
        config_dir: display_path(&desired.config_dir),
        actor: options.actor.clone(),
        desired_revision: DesiredRevision {
            config_digest: Some(desired.config_digest),
        },
        state_observations: observations,
        changes,
        applied_count,
        deferred_count,
        converged,
        state_written,
        resource_statuses,
        diagnostics,
    }
}

/// Record a digest-bound human approval for a gated (irreversible) change —
/// today: graph deletes. The artifact binds to the exact desired config
/// digest and the change's before/after digests, so config or state drift
/// invalidates it automatically (a stale approval can never authorize a
/// different change).
pub async fn approve_config_dir(
    config_dir: impl AsRef<Path>,
    resource: &str,
    approved_by: &str,
) -> ApproveOutput {
    let outcome = load_desired(config_dir.as_ref());
    let mut diagnostics = outcome.diagnostics;
    let storage_root = outcome
        .desired
        .as_ref()
        .and_then(|desired| desired.storage_root.clone());
    let backend = match store_for(&outcome.config_dir, storage_root.as_deref()) {
        Ok(backend) => backend,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            ClusterStore::for_config_dir(&outcome.config_dir)
        }
    };
    let mut observations = backend.observations();

    let fail = |config_dir: String, diagnostics: Vec<Diagnostic>| ApproveOutput {
        ok: false,
        config_dir,
        approval_id: None,
        resource: None,
        operation: None,
        approved_by: None,
        diagnostics,
    };

    let Some(desired) = outcome.desired else {
        return fail(display_path(&outcome.config_dir), diagnostics);
    };
    if has_errors(&diagnostics) {
        return fail(display_path(&desired.config_dir), diagnostics);
    }

    let _lock_guard = if desired.state_lock {
        match backend.acquire_lock("approve", &mut observations).await {
            Ok(guard) => Some(guard),
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                return fail(display_path(&desired.config_dir), diagnostics);
            }
        }
    } else {
        diagnostics.push(Diagnostic::warning(
            "state_lock_disabled",
            "state.lock",
            "state.lock is false; approve ran without acquiring the cluster state lock",
        ));
        None
    };

    let state = match backend.read_state(&mut observations).await {
        Ok(snapshot) => match snapshot.state {
            Some(state) => state,
            None => {
                diagnostics.push(Diagnostic::error(
                    "state_missing",
                    CLUSTER_STATE_FILE,
                    "approve requires an existing state.json; run `cluster import` first",
                ));
                return fail(display_path(&desired.config_dir), diagnostics);
            }
        },
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return fail(display_path(&desired.config_dir), diagnostics);
        }
    };

    let prior_resources = state_resource_digests(&state);
    let changes = diff_resources(&prior_resources, &desired.resource_digests);
    let gates = compute_approvals(&changes, &BTreeSet::new());
    let Some(change) = changes.iter().find(|change| {
        change.resource == resource && gates.iter().any(|gate| gate.resource == resource)
    }) else {
        diagnostics.push(Diagnostic::error(
            "approval_not_required",
            resource,
            "no pending change for this resource requires approval (check `cluster plan`)",
        ));
        return fail(display_path(&desired.config_dir), diagnostics);
    };

    let artifact = ApprovalArtifact {
        schema_version: 1,
        approval_id: Ulid::new().to_string(),
        resource: change.resource.clone(),
        operation: match change.operation {
            PlanOperation::Create => "create",
            PlanOperation::Update => "update",
            PlanOperation::Delete => "delete",
        }
        .to_string(),
        reason: gates
            .iter()
            .find(|gate| gate.resource == resource)
            .map(|gate| gate.reason.clone())
            .unwrap_or_default(),
        bound_config_digest: desired.config_digest.clone(),
        bound_before_digest: change.before_digest.clone(),
        bound_after_digest: change.after_digest.clone(),
        approved_by: approved_by.to_string(),
        created_at: now_rfc3339(),
        consumed_at: None,
        consumed_by_operation: None,
    };
    if let Err(diagnostic) = backend.write_approval_artifact(&artifact).await {
        diagnostics.push(diagnostic);
        return fail(display_path(&desired.config_dir), diagnostics);
    }

    ApproveOutput {
        ok: !has_errors(&diagnostics),
        config_dir: display_path(&desired.config_dir),
        approval_id: Some(artifact.approval_id),
        resource: Some(artifact.resource),
        operation: Some(change.operation.clone()),
        approved_by: Some(artifact.approved_by),
        diagnostics,
    }
}


pub async fn status_config_dir(config_dir: impl AsRef<Path>) -> StatusOutput {
    let parsed = parse_cluster_config(config_dir.as_ref());
    let mut diagnostics = parsed.diagnostics;
    let storage_root = parsed.raw.as_ref().and_then(|raw| {
        raw.storage
            .as_deref()
            .map(str::trim)
            .filter(|root| !root.is_empty())
            .map(|root| root.trim_end_matches('/').to_string())
    });
    let backend = match store_for(&parsed.config_dir, storage_root.as_deref()) {
        Ok(backend) => backend,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            ClusterStore::for_config_dir(&parsed.config_dir)
        }
    };
    let mut observations = backend.observations();
    backend.observe_lock(&mut observations, &mut diagnostics).await;
    warn_pending_recovery_sidecars(&parsed.config_dir, &mut diagnostics);

    let mut resource_digests = BTreeMap::new();
    let mut resource_statuses = BTreeMap::new();
    let mut state_observation_records = BTreeMap::new();

    if let Some(raw) = parsed.raw.as_ref() {
        let _settings = validate_cluster_header(raw, &mut diagnostics);
        if !has_errors(&diagnostics) {
            match backend.read_state(&mut observations).await {
                Ok(snapshot) => {
                    if let Some(state) = snapshot.state {
                        // Read-only point-in-time catalog check: report the
                        // findings as diagnostics; persisting Drifted statuses
                        // is refresh's job. Status never writes state.
                        for (address, finding) in
                            verify_catalog_payloads(&backend, &state).await
                        {
                            diagnostics.push(payload_finding_diagnostic(&address, &finding));
                        }
                        resource_digests = state_resource_digests(&state);
                        resource_statuses = state.resource_statuses;
                        state_observation_records = state.observations;
                    } else {
                        diagnostics.push(Diagnostic::warning(
                            "state_missing",
                            CLUSTER_STATE_FILE,
                            "state.json is missing; no applied cluster revision has been recorded",
                        ));
                    }
                }
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
    }

    StatusOutput {
        ok: !has_errors(&diagnostics),
        config_dir: display_path(&parsed.config_dir),
        state_observations: observations,
        resource_digests,
        resource_statuses,
        observations: state_observation_records,
        diagnostics,
    }
}

pub async fn force_unlock_config_dir(
    config_dir: impl AsRef<Path>,
    lock_id: impl AsRef<str>,
) -> ForceUnlockOutput {
    let parsed = parse_cluster_config(config_dir.as_ref());
    let mut diagnostics = parsed.diagnostics;
    let storage_root = parsed.raw.as_ref().and_then(|raw| {
        raw.storage
            .as_deref()
            .map(str::trim)
            .filter(|root| !root.is_empty())
            .map(|root| root.trim_end_matches('/').to_string())
    });
    let backend = match store_for(&parsed.config_dir, storage_root.as_deref()) {
        Ok(backend) => backend,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            ClusterStore::for_config_dir(&parsed.config_dir)
        }
    };
    let mut observations = backend.observations();
    let mut lock_removed = false;

    if let Some(raw) = parsed.raw.as_ref() {
        let _settings = validate_cluster_header(raw, &mut diagnostics);
        if !has_errors(&diagnostics) {
            match backend.force_unlock(lock_id.as_ref(), &mut observations).await {
                Ok(()) => lock_removed = true,
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
    }

    ForceUnlockOutput {
        ok: !has_errors(&diagnostics),
        config_dir: display_path(&parsed.config_dir),
        state_observations: observations,
        lock_removed,
        diagnostics,
    }
}

pub async fn refresh_config_dir(config_dir: impl AsRef<Path>) -> StateSyncOutput {
    sync_config_dir(config_dir.as_ref(), StateSyncOperation::Refresh).await
}

pub async fn import_config_dir(config_dir: impl AsRef<Path>) -> StateSyncOutput {
    sync_config_dir(config_dir.as_ref(), StateSyncOperation::Import).await
}

async fn sync_config_dir(config_dir: &Path, operation: StateSyncOperation) -> StateSyncOutput {
    let outcome = load_desired(config_dir);
    let mut diagnostics = outcome.diagnostics;
    let storage_root = outcome
        .desired
        .as_ref()
        .and_then(|desired| desired.storage_root.clone());
    let backend = match store_for(&outcome.config_dir, storage_root.as_deref()) {
        Ok(backend) => backend,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            ClusterStore::for_config_dir(&outcome.config_dir)
        }
    };
    let mut observations = backend.observations();

    let Some(desired) = outcome.desired else {
        return StateSyncOutput {
            ok: false,
            operation,
            config_dir: display_path(&outcome.config_dir),
            state_observations: observations,
            resource_digests: BTreeMap::new(),
            resource_statuses: BTreeMap::new(),
            observations: BTreeMap::new(),
            diagnostics,
        };
    };

    if has_errors(&diagnostics) {
        return StateSyncOutput {
            ok: false,
            operation,
            config_dir: display_path(&desired.config_dir),
            state_observations: observations,
            resource_digests: desired.resource_digests,
            resource_statuses: BTreeMap::new(),
            observations: BTreeMap::new(),
            diagnostics,
        };
    }

    let operation_label = state_sync_operation_label(operation);
    let _lock_guard = if desired.state_lock {
        match backend.acquire_lock(operation_label, &mut observations).await {
            Ok(guard) => Some(guard),
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                None
            }
        }
    } else {
        diagnostics.push(Diagnostic::warning(
            "state_lock_disabled",
            "state.lock",
            format!(
                "state.lock is false; {operation_label} wrote state without acquiring the cluster state lock"
            ),
        ));
        None
    };

    if has_errors(&diagnostics) {
        return StateSyncOutput {
            ok: false,
            operation,
            config_dir: display_path(&desired.config_dir),
            state_observations: observations,
            resource_digests: desired.resource_digests,
            resource_statuses: BTreeMap::new(),
            observations: BTreeMap::new(),
            diagnostics,
        };
    }

    let snapshot = match backend.read_state(&mut observations).await {
        Ok(snapshot) => snapshot,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return StateSyncOutput {
                ok: false,
                operation,
                config_dir: display_path(&desired.config_dir),
                state_observations: observations,
                resource_digests: desired.resource_digests,
                resource_statuses: BTreeMap::new(),
                observations: BTreeMap::new(),
                diagnostics,
            };
        }
    };

    let expected_cas = snapshot.state_cas;
    let mut state = match (operation, snapshot.state) {
        (StateSyncOperation::Refresh, Some(state)) => state,
        (StateSyncOperation::Refresh, None) => {
            diagnostics.push(Diagnostic::error(
                "state_missing",
                CLUSTER_STATE_FILE,
                "refresh requires an existing state.json; run `cluster import` to bootstrap state",
            ));
            return StateSyncOutput {
                ok: false,
                operation,
                config_dir: display_path(&desired.config_dir),
                state_observations: observations,
                resource_digests: BTreeMap::new(),
                resource_statuses: BTreeMap::new(),
                observations: BTreeMap::new(),
                diagnostics,
            };
        }
        (StateSyncOperation::Import, Some(state)) => {
            diagnostics.push(Diagnostic::error(
                "state_already_exists",
                CLUSTER_STATE_FILE,
                "import creates initial state only when state.json is missing; use `cluster refresh` for an existing state ledger",
            ));
            return StateSyncOutput {
                ok: false,
                operation,
                config_dir: display_path(&desired.config_dir),
                state_observations: observations,
                resource_digests: state_resource_digests(&state),
                resource_statuses: state.resource_statuses,
                observations: state.observations,
                diagnostics,
            };
        }
        (StateSyncOperation::Import, None) => initial_import_state(&desired),
    };

    // Recovery sweep first (RFC-004 §D3): classify any interrupted graph
    // operation before observation/verification so a rolled-forward outcome
    // is what those passes see.
    let sweep = sweep_recovery_sidecars(&backend, &mut state, &mut diagnostics).await;

    // Catalog payload verification must run BEFORE graph observation: removing
    // a drifted query digest first means the live-graph composite recompute
    // below already excludes it, so the persisted graph.<id> composite stays
    // consistent and the next plan shows exactly the create + derived update.
    for (address, finding) in verify_catalog_payloads(&backend, &state).await {
        diagnostics.push(payload_finding_diagnostic(&address, &finding));
        match finding {
            PayloadFinding::Missing => {
                state.applied_revision.resources.remove(&address);
                set_resource_status(
                    &mut state,
                    &address,
                    ResourceLifecycleStatus::Drifted,
                    "payload_missing",
                    "catalog payload blob is missing; re-run `cluster apply` to republish",
                );
            }
            PayloadFinding::Mismatch { .. } => {
                state.applied_revision.resources.remove(&address);
                set_resource_status(
                    &mut state,
                    &address,
                    ResourceLifecycleStatus::Drifted,
                    "payload_mismatch",
                    "catalog payload blob does not match the recorded digest; re-run `cluster apply` to republish",
                );
            }
            // Transient IO must not trigger a spurious republish: keep the
            // digest, surface the error, let a later clean refresh converge.
            PayloadFinding::ReadError(error) => {
                set_resource_status(
                    &mut state,
                    &address,
                    ResourceLifecycleStatus::Error,
                    "payload_read_error",
                    &error,
                );
            }
        }
    }

    let graph_error_count = observe_declared_graphs(&desired, &backend, &mut state).await;
    if graph_error_count > 0 {
        diagnostics.push(Diagnostic::error(
            "graph_observation_error",
            CLUSTER_GRAPHS_DIR,
            format!("{graph_error_count} graph observation(s) failed"),
        ));
    }

    if operation == StateSyncOperation::Import && has_errors(&diagnostics) {
        return StateSyncOutput {
            ok: false,
            operation,
            config_dir: display_path(&desired.config_dir),
            state_observations: observations,
            resource_digests: state_resource_digests(&state),
            resource_statuses: state.resource_statuses,
            observations: state.observations,
            diagnostics,
        };
    }

    if operation == StateSyncOperation::Import {
        state.state_revision = 1;
    } else {
        state.state_revision = state.state_revision.saturating_add(1);
    }

    match backend.write_state(&state, expected_cas.as_deref(), &mut observations).await {
        Ok(()) => {
            // Completed sweep sidecars are deleted only after their outcome
            // is durably recorded; on failure they stay and re-sweep.
            for sidecar_uri in &sweep.completed_sidecars {
                backend.delete_object(sidecar_uri).await;
            }
            mark_approvals_consumed(&backend, &sweep.consumed_approvals).await;
        }
        Err(diagnostic) => diagnostics.push(diagnostic),
    }

    let resource_digests = state_resource_digests(&state);
    let ok = !has_errors(&diagnostics);

    StateSyncOutput {
        ok,
        operation,
        config_dir: display_path(&desired.config_dir),
        state_observations: observations,
        resource_digests,
        resource_statuses: state.resource_statuses,
        observations: state.observations,
        diagnostics,
    }
}




#[derive(Debug, PartialEq, Eq)]
enum PayloadFinding {
    Missing,
    Mismatch { actual_digest: String },
    ReadError(String),
}

/// Verify every catalog-backed resource digest in state against its
/// content-addressed blob under `__cluster/resources/`. Graph, schema, and
/// unknown addresses have no payloads and are skipped. Read-only; findings
/// are deterministic (BTreeMap order). Payloads are small (queries, policy
/// bundles), so a full digest re-hash is cheap.
async fn verify_catalog_payloads(
    backend: &ClusterStore,
    state: &ClusterState,
) -> Vec<(String, PayloadFinding)> {
    let mut findings = Vec::new();
    for (address, resource) in &state.applied_revision.resources {
        let kind = resource_kind(address);
        if ClusterStore::payload_relative(&kind, &resource.digest).is_none() {
            continue;
        }
        match backend.read_payload(&kind, &resource.digest).await {
            Ok(Some(text)) => {
                let actual_digest = sha256_hex(text.as_bytes());
                if actual_digest != resource.digest {
                    findings.push((address.clone(), PayloadFinding::Mismatch { actual_digest }));
                }
            }
            Ok(None) => findings.push((address.clone(), PayloadFinding::Missing)),
            Err(err) => {
                findings.push((address.clone(), PayloadFinding::ReadError(err)));
            }
        }
    }
    findings
}

fn payload_finding_diagnostic(address: &str, finding: &PayloadFinding) -> Diagnostic {
    match finding {
        PayloadFinding::Missing => Diagnostic::warning(
            "catalog_payload_missing",
            address,
            "catalog payload blob is missing; re-run `cluster apply` to republish",
        ),
        PayloadFinding::Mismatch { actual_digest } => Diagnostic::warning(
            "catalog_payload_mismatch",
            address,
            format!(
                "catalog payload blob does not match the recorded digest (actual sha256:{actual_digest}); re-run `cluster apply` to republish"
            ),
        ),
        // An unverifiable blob must not report healthy.
        PayloadFinding::ReadError(error) => {
            Diagnostic::error("catalog_payload_read_error", address, error.clone())
        }
    }
}

/// Write one content-addressed payload blob. Idempotent: an existing
/// digest-named file is trusted as-is. The digest re-check is the apply-side
/// TOCTOU detector — the source file changing between `load_desired` and the
/// payload write must fail loudly, never publish mismatched content.
async fn write_resource_payload(
    backend: &ClusterStore,
    kind: &ResourceKind,
    source: &Path,
    expected_digest: &str,
    resource: &str,
) -> Result<(), Diagnostic> {
    if backend.payload_exists(kind, expected_digest).await {
        // Content-addressed: an existing digest-named object is identical.
        return Ok(());
    }
    let bytes = fs::read(source).map_err(|err| {
        Diagnostic::error(
            "resource_payload_write_error",
            resource,
            format!("could not read resource source '{}': {err}", source.display()),
        )
    })?;
    if sha256_hex(&bytes) != expected_digest {
        // The apply-side TOCTOU detector: the source changing between
        // load_desired and this write must fail loudly, never publish
        // mismatched content.
        return Err(Diagnostic::error(
            "resource_content_changed",
            resource,
            format!(
                "resource source '{}' changed while apply was running; re-run `cluster apply`",
                source.display()
            ),
        ));
    }
    let content = String::from_utf8(bytes).map_err(|err| {
        Diagnostic::error(
            "resource_payload_write_error",
            resource,
            format!("resource source is not valid UTF-8: {err}"),
        )
    })?;
    backend
        .write_payload(kind, expected_digest, &content)
        .await
        .map_err(|err| {
            Diagnostic::error(
                "resource_payload_write_error",
                resource,
                format!("could not write payload: {err}"),
            )
        })
}

/// Recompute the composite `graph.<id>` digests for state-resident graphs from
/// state's own schema/query components. Without this, an applied query change
/// would leave the prior composite digest in state and `graph.<id>` would show
/// a phantom update in every later plan — apply could never converge.
fn recompute_state_graph_digests(state: &mut ClusterState, desired: &DesiredCluster) {
    for graph in &desired.graphs {
        let graph_address = graph_address(&graph.id);
        if !state.applied_revision.resources.contains_key(&graph_address) {
            continue;
        }
        let schema_digest = state
            .applied_revision
            .resources
            .get(&schema_address(&graph.id))
            .map(|resource| resource.digest.clone());
        let query_digests = state_query_digests_for_graph(state, &graph.id);
        let digest = graph_digest(&graph.id, schema_digest.as_ref(), Some(&query_digests));
        state
            .applied_revision
            .resources
            .insert(graph_address, StateResource { digest, applies_to: None });
    }
}

fn duplicate_key_diagnostics(text: &str) -> Vec<Diagnostic> {
    #[derive(Debug)]
    struct Frame {
        indent: isize,
        path: String,
        keys: BTreeSet<String>,
    }

    let mut diagnostics = Vec::new();
    let mut stack = vec![Frame {
        indent: -1,
        path: String::new(),
        keys: BTreeSet::new(),
    }];

    for (line_idx, line) in text.lines().enumerate() {
        let line_without_comment = strip_comment(line);
        if line_without_comment.trim().is_empty() {
            continue;
        }
        let indent = line_without_comment
            .chars()
            .take_while(|ch| *ch == ' ')
            .count() as isize;
        let trimmed = line_without_comment.trim_start();
        if trimmed.starts_with('-') {
            continue;
        }
        let Some((raw_key, raw_value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = raw_key.trim();
        if key.is_empty() || key.starts_with('{') || key.starts_with('[') {
            continue;
        }

        while stack.last().is_some_and(|frame| indent <= frame.indent) {
            stack.pop();
        }
        let parent = stack.last_mut().expect("root frame is always present");
        let full_path = if parent.path.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", parent.path, key)
        };
        if !parent.keys.insert(key.to_string()) {
            diagnostics.push(Diagnostic::error(
                "duplicate_yaml_key",
                full_path.clone(),
                format!("duplicate YAML key `{key}` on line {}", line_idx + 1),
            ));
        }
        if raw_value.trim().is_empty() {
            stack.push(Frame {
                indent,
                path: full_path,
                keys: BTreeSet::new(),
            });
        }
    }

    diagnostics
}


fn strip_comment(line: &str) -> String {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escaped = false;

    for (idx, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_double_quote => escaped = true,
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '#' if !in_single_quote && !in_double_quote => return line[..idx].to_string(),
            _ => {}
        }
    }

    line.to_string()
}


fn state_query_digests_for_graph(state: &ClusterState, graph_id: &str) -> BTreeMap<String, String> {
    let prefix = format!("query.{graph_id}.");
    state
        .applied_revision
        .resources
        .iter()
        .filter_map(|(address, resource)| {
            address
                .strip_prefix(&prefix)
                .map(|name| (name.to_string(), resource.digest.clone()))
        })
        .collect()
}

fn set_resource_status_applied(state: &mut ClusterState, address: &str) {
    state.resource_statuses.insert(
        address.to_string(),
        ResourceStatusRecord {
            status: ResourceLifecycleStatus::Applied,
            conditions: Vec::new(),
            message: None,
        },
    );
}

fn set_resource_status(
    state: &mut ClusterState,
    address: &str,
    status: ResourceLifecycleStatus,
    condition: &str,
    message: &str,
) {
    state.resource_statuses.insert(
        address.to_string(),
        ResourceStatusRecord {
            status,
            conditions: vec![condition.to_string()],
            message: Some(message.to_string()),
        },
    );
}

fn graph_digest(
    graph_id: &str,
    schema_digest: Option<&String>,
    query_digests: Option<&BTreeMap<String, String>>,
) -> String {
    let mut input = format!(
        "graph\0{graph_id}\0schema\0{}\0",
        schema_digest.map_or("", String::as_str)
    );
    if let Some(query_digests) = query_digests {
        for (name, digest) in query_digests {
            input.push_str("query\0");
            input.push_str(name);
            input.push('\0');
            input.push_str(digest);
            input.push('\0');
        }
    }
    sha256_hex(input.as_bytes())
}

fn desired_config_digest(
    raw: &RawClusterConfig,
    resource_digests: &BTreeMap<String, String>,
) -> String {
    let mut input = String::from("cluster-config\0");
    // Hash parsed semantics, not raw YAML bytes, so comments and formatting do
    // not create a new desired revision and the digest cannot drift from parse.
    let config_semantics =
        serde_json::to_string(raw).expect("raw cluster config must serialize deterministically");
    input.push_str(&config_semantics);
    input.push('\0');
    for (address, digest) in resource_digests {
        input.push_str(address);
        input.push('\0');
        input.push_str(digest);
        input.push('\0');
    }
    sha256_hex(input.as_bytes())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn lock_age_seconds(created_at: &str) -> Option<u64> {
    let created_at = OffsetDateTime::parse(created_at, &Rfc3339).ok()?;
    Some(
        (OffsetDateTime::now_utc() - created_at)
            .whole_seconds()
            .max(0) as u64,
    )
}

fn state_sync_operation_label(operation: StateSyncOperation) -> &'static str {
    match operation {
        StateSyncOperation::Refresh => "refresh",
        StateSyncOperation::Import => "import",
    }
}

fn has_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
}

fn count_errors(diagnostics: &[Diagnostic]) -> usize {
    diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
        .count()
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}


#[cfg(test)]
#[path = "tests.rs"]
mod tests;
