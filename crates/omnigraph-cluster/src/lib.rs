// The cluster state-machine tests compose several large async futures; rustc
// needs the wider layout-query budget while building the lib-test harness
// (with or without failpoints). Production builds keep the default.
#![cfg_attr(test, recursion_limit = "256")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self};
use std::path::{Path, PathBuf};

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_compiler::SchemaMigrationPlan;
use omnigraph_compiler::build_catalog;
use omnigraph_compiler::query::ast::QueryFile;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::query::typecheck::typecheck_query_decl;
use omnigraph_compiler::schema::parser::parse_schema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

pub mod seams;

mod admission;
mod authorization;
mod config;
mod deployment;
mod diff;
mod graph_read;
mod serve;
mod state_lock;
mod store;
mod types;
pub use admission::{
    ClusterAdmission, ClusterAdmissionPurpose, acquire_cluster_admission, acquire_graph_admission,
};
pub use authorization::{
    AuthorizedEffect, AuthorizedPlanOutput, IdentityAuthorization, PlanAuthorization,
    PlanReadAuthorization, PolicyAuthorizationCheck, authorize_apply_plan, authorize_plan_read,
};
use config::{
    QueriesDecl, graph_address, load_desired, observe_declared_graphs, parse_cluster_config,
    preview_schema_migration, schema_address, state_resource_digests, validate_cluster_header,
};
pub use deployment::*;
use diff::{
    ResourceKind, append_embedding_profile_changes, append_policy_binding_changes,
    compute_blast_radius, diff_resources, resource_kind,
};
pub use graph_read::GraphReadAuthority;
#[cfg(any(test, feature = "test-util"))]
pub use serve::read_serving_snapshot_with_display_root;
pub use serve::{
    AdmittedServingSnapshot, RootBoundServingSnapshot, ServingBlockedGraph, ServingGraph,
    ServingPolicy, ServingQuery, ServingSnapshot, acquire_serving_admission,
    admit_serving_snapshot, cluster_graph_ids, cluster_root_for_graph_uri,
    read_root_bound_serving_snapshot, read_root_bound_serving_snapshot_from_storage,
    read_serving_snapshot, read_serving_snapshot_from_storage, resolve_graph_storage_uri,
};
use store::ClusterStore;
pub use types::*;

pub const CLUSTER_CONFIG_FILE: &str = "cluster.yaml";
pub const CLUSTER_GRAPHS_DIR: &str = "graphs";
pub const CLUSTER_STATE_DIR: &str = "__cluster";
pub const CLUSTER_STATE_FILE: &str = "__cluster/state.json";
pub const CLUSTER_LOCK_FILE: &str = "__cluster/lock.json";
pub const CLUSTER_RESOURCES_DIR: &str = "__cluster/resources";
pub const CLUSTER_RECOVERIES_DIR: &str = "__cluster/recoveries";

/// The store for a load outcome: the declared `storage:` root when present,
/// the config directory itself otherwise. A bad root is a loud error.
fn store_for(config_dir: &Path, storage_root: Option<&str>) -> Result<ClusterStore, Diagnostic> {
    match storage_root {
        Some(root) => ClusterStore::for_storage_root(root),
        None => Ok(ClusterStore::for_config_dir(config_dir)),
    }
}

pub fn validate_config_dir(config_dir: impl AsRef<Path>) -> ValidateOutput {
    let mut outcome = load_desired(config_dir.as_ref());
    // This release intentionally tightens the historical permissive URI
    // behavior. Keep the notice on the validate surface (rather than every
    // plan/apply invocation): a valid graph with no bases must make its
    // effective default-Deny posture visible before an operator upgrades a
    // workload that previously supplied external references.
    if !has_errors(&outcome.diagnostics) {
        let denied_graphs = outcome
            .desired
            .as_ref()
            .map(|desired| {
                desired
                    .graphs
                    .iter()
                    .filter(|graph| {
                        matches!(
                            &graph.external_blob_policy,
                            omnigraph::ExternalBlobPolicy::Deny
                        )
                    })
                    .map(|graph| graph.id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for graph_id in denied_graphs {
            outcome.diagnostics.push(Diagnostic::warning(
                "external_blob_ingress_default_deny",
                format!("graphs.{graph_id}.external_blobs"),
                "effective policy is deny: new external Blob URI references will be rejected; existing stored references remain readable",
            ));
        }
    }
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
    plan_config_dir_with_options(config_dir, PlanOptions::default()).await
}

/// `plan`, optionally without the cluster lock (RFC 0048). An observed plan
/// reads the ledger once, reports any lock it finds instead of refusing, and
/// labels its output `authority: observed`; it is never authority for an
/// effect.
pub async fn plan_config_dir_with_options(
    config_dir: impl AsRef<Path>,
    options: PlanOptions,
) -> PlanOutput {
    // Keep the shared implementation off the forwarding caller's stack.
    Box::pin(plan_config_dir_impl(
        config_dir.as_ref(),
        options,
        None,
        &mut None,
    ))
    .await
}

/// Plan using the current applied policy for an already authenticated actor.
/// Existing storage-holder entry points retain their explicit trust boundary.
pub async fn plan_config_dir_authorized(
    config_dir: impl AsRef<Path>,
    options: PlanOptions,
    identity: &IdentityAuthorization,
) -> AuthorizedPlanOutput {
    let mut authorization = None;
    let plan = Box::pin(plan_config_dir_impl(
        config_dir.as_ref(),
        options,
        Some(identity),
        &mut authorization,
    ))
    .await;
    AuthorizedPlanOutput {
        plan,
        authorization,
    }
}

async fn plan_config_dir_impl(
    config_dir: &Path,
    options: PlanOptions,
    identity: Option<&IdentityAuthorization>,
    authorization: &mut Option<PlanAuthorization>,
) -> PlanOutput {
    let mut authority = if options.observe {
        LedgerAuthority::Observed
    } else {
        LedgerAuthority::Locked
    };
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
        return PlanOutput {
            ok: false,
            authority,
            config_dir: display_path(&outcome.config_dir),
            desired_revision: DesiredRevision {
                config_digest: None,
            },
            resource_digests: BTreeMap::new(),
            dependencies: Vec::new(),
            state_observations: observations,
            changes: Vec::new(),
            blast_radius: Vec::new(),
            diagnostics,
        };
    };

    if has_errors(&diagnostics) {
        return PlanOutput {
            ok: false,
            authority,
            config_dir: display_path(&desired.config_dir),
            desired_revision: DesiredRevision {
                config_digest: Some(desired.config_digest),
            },
            resource_digests: desired.resource_digests,
            dependencies: desired.dependencies,
            state_observations: observations,
            changes: Vec::new(),
            blast_radius: Vec::new(),
            diagnostics,
        };
    }

    if !options.observe && !desired.state_lock {
        authority = LedgerAuthority::Unlocked;
    }
    let _lock_guard = if options.observe {
        backend
            .observe_lock(&mut observations, &mut diagnostics)
            .await;
        None
    } else if desired.state_lock {
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
    warn_pending_recovery_sidecars(&backend, &mut diagnostics).await;

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
        append_embedding_profile_changes(&mut changes, prior_state.as_ref(), &desired);
    }
    // The same v2 scope rules govern previews and execution. A refused scope
    // is wholly pre-effect; no approval artifact can authorize a removed path.
    let scope_error = prior_state.as_ref().and_then(|state| {
        if state.version != 2 {
            return Some(Diagnostic::error(
                "ledger_upgrade_required",
                CLUSTER_STATE_FILE,
                "convert the stopped cluster ledger to v2 before planning deployments",
            ));
        }
        match capture_deployment(config_dir, &BTreeMap::new()) {
            Ok(bundle) => preview_deployment_scope(state, &bundle).err(),
            Err(error) => Some(error),
        }
    });
    for change in &mut changes {
        if let Some(error) = &scope_error {
            change.disposition = Some(ApplyDisposition::Blocked);
            change.reason = Some(error.code.clone());
        } else {
            change.disposition = Some(
                if matches!(resource_kind(&change.resource), ResourceKind::Graph(_))
                    && change.operation == PlanOperation::Update
                {
                    ApplyDisposition::Derived
                } else {
                    ApplyDisposition::Applied
                },
            );
            change.reason = None;
        }
    }
    if let Some(error) = scope_error {
        diagnostics.push(error);
    }

    if !has_errors(&diagnostics) {
        if let Some(identity) = identity {
            match authorization::authorize_candidate(
                &backend,
                &desired,
                prior_state.as_ref(),
                &observations,
                &changes,
                identity,
                false,
            )
            .await
            {
                Ok((evidence, _)) => *authorization = Some(evidence),
                Err(diagnostic) => {
                    diagnostics.push(diagnostic);
                    changes.clear();
                }
            }
        }
    }

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
    let ok = !has_errors(&diagnostics);

    PlanOutput {
        ok,
        authority,
        config_dir: display_path(&desired.config_dir),
        desired_revision: DesiredRevision {
            config_digest: Some(desired.config_digest),
        },
        resource_digests: desired.resource_digests,
        dependencies: desired.dependencies,
        state_observations: observations,
        changes,
        blast_radius,
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
    backend
        .observe_lock(&mut observations, &mut diagnostics)
        .await;
    warn_pending_recovery_sidecars(&backend, &mut diagnostics).await;

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
                        // findings as diagnostics. Status never rewrites the
                        // achieved revision or adopts observed graph state.
                        for (address, finding) in verify_catalog_payloads(&backend, &state).await {
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
            match backend
                .force_unlock(lock_id.as_ref(), &mut observations)
                .await
            {
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

/// Inspect the current catalog and graph observations without changing authority.
/// The ledger bytes, revision and ownership lock remain untouched.
pub async fn observe_config_dir(config_dir: impl AsRef<Path>) -> StateSyncOutput {
    let outcome = load_desired(config_dir.as_ref());
    let mut diagnostics = outcome.diagnostics;
    let backend = match store_for(
        &outcome.config_dir,
        outcome
            .desired
            .as_ref()
            .and_then(|desired| desired.storage_root.as_deref()),
    ) {
        Ok(backend) => backend,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            ClusterStore::for_config_dir(&outcome.config_dir)
        }
    };
    let mut observations = backend.observations();
    backend
        .observe_lock(&mut observations, &mut diagnostics)
        .await;
    warn_pending_recovery_sidecars(&backend, &mut diagnostics).await;
    let mut state = if has_errors(&diagnostics) {
        None
    } else {
        match backend.read_state(&mut observations).await {
            Ok(snapshot) => match snapshot.state {
                Some(state) if state.version == 2 => Some(state),
                Some(_) => {
                    diagnostics.push(Diagnostic::error(
                        "ledger_upgrade_required",
                        CLUSTER_STATE_FILE,
                        "convert the stopped cluster ledger to v2 before observing its graphs",
                    ));
                    None
                }
                None => {
                    diagnostics.push(Diagnostic::error(
                        "state_missing",
                        CLUSTER_STATE_FILE,
                        "no applied cluster state; run `cluster apply` first",
                    ));
                    None
                }
            },
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                None
            }
        }
    };
    if let Some(state) = state.as_mut() {
        if validate_state_graph_resource_digests(state, &mut diagnostics) {
            for (address, finding) in verify_catalog_payloads(&backend, state).await {
                diagnostics.push(payload_finding_diagnostic(&address, &finding));
                let (status, code, message) = match finding {
                    PayloadFinding::Missing => (
                        ResourceLifecycleStatus::Drifted,
                        "payload_missing",
                        "catalog payload blob is missing".to_owned(),
                    ),
                    PayloadFinding::Mismatch { .. } => (
                        ResourceLifecycleStatus::Drifted,
                        "payload_mismatch",
                        "catalog payload does not match its recorded digest".to_owned(),
                    ),
                    PayloadFinding::ReadError(error) => {
                        (ResourceLifecycleStatus::Error, "payload_read_error", error)
                    }
                };
                set_resource_status(state, &address, status, code, &message);
            }
            if let Some(desired) = outcome.desired {
                if observe_declared_graphs(&desired, &backend, state).await > 0 {
                    diagnostics.push(Diagnostic::error(
                        "graph_observation_error",
                        CLUSTER_GRAPHS_DIR,
                        "one or more graph observations failed",
                    ));
                }
            }
        }
    }
    StateSyncOutput {
        ok: !has_errors(&diagnostics),
        operation: StateSyncOperation::Observe,
        authority: LedgerAuthority::Observed,
        config_dir: display_path(&outcome.config_dir),
        state_observations: observations,
        resource_digests: state
            .as_ref()
            .map(state_resource_digests)
            .unwrap_or_default(),
        resource_statuses: state
            .as_ref()
            .map(|state| state.resource_statuses.clone())
            .unwrap_or_default(),
        observations: state.map(|state| state.observations).unwrap_or_default(),
        diagnostics,
    }
}

async fn warn_pending_recovery_sidecars(backend: &ClusterStore, diagnostics: &mut Vec<Diagnostic>) {
    for location in backend.list_recovery_sidecar_locations(diagnostics).await {
        diagnostics.push(Diagnostic::warning("legacy_recovery_pending", location, "legacy recovery evidence must be resolved before explicit ledger conversion; this build never sweeps or adopts it"));
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
        let trimmed = if trimmed == "-" || trimmed.starts_with("- ") {
            while stack.last().is_some_and(|frame| indent <= frame.indent) {
                stack.pop();
            }
            let parent = stack.last().expect("root frame is always present");
            let item_path = if parent.path.is_empty() {
                "[]".to_string()
            } else {
                format!("{}[]", parent.path)
            };
            stack.push(Frame {
                indent,
                path: item_path,
                keys: BTreeSet::new(),
            });
            let item = trimmed.strip_prefix('-').unwrap().trim_start();
            if item.is_empty() {
                continue;
            }
            item
        } else {
            while stack.last().is_some_and(|frame| indent <= frame.indent) {
                stack.pop();
            }
            trimmed
        };
        let Some((raw_key, raw_value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = raw_key.trim();
        if key.is_empty() || key.starts_with('{') || key.starts_with('[') {
            continue;
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

fn state_graph_embedding_provider(state: &ClusterState, graph_id: &str) -> Option<String> {
    state
        .applied_revision
        .resources
        .get(&graph_address(graph_id))
        .and_then(|resource| resource.embedding_provider.clone())
}

fn state_graph_external_blob_policy(
    state: &ClusterState,
    graph_id: &str,
) -> omnigraph::ExternalBlobPolicy {
    state
        .applied_revision
        .resources
        .get(&graph_address(graph_id))
        .and_then(|resource| resource.external_blob_policy.clone())
        .unwrap_or_default()
}

fn expected_state_graph_resource_digest(
    state: &ClusterState,
    graph_id: &str,
    graph_resource: &StateResource,
) -> String {
    let schema_digest = state
        .applied_revision
        .resources
        .get(&schema_address(graph_id))
        .map(|resource| resource.digest.clone());
    let query_digests = state_query_digests_for_graph(state, graph_id);
    let embedding_provider_digest =
        state_embedding_provider_digest(state, graph_resource.embedding_provider.as_deref());
    // Historical state predates this field. Absence therefore has exactly the
    // old/default Deny meaning, rather than becoming an unbound wildcard.
    let external_blob_policy = graph_resource
        .external_blob_policy
        .as_ref()
        .cloned()
        .unwrap_or_default();
    graph_digest_with_external_blob_policy(
        graph_id,
        schema_digest.as_ref(),
        Some(&query_digests),
        graph_resource.embedding_provider.as_deref(),
        embedding_provider_digest.as_ref(),
        &external_blob_policy,
    )
}

fn validate_state_graph_resource_digests(
    state: &ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
) -> bool {
    let before = count_errors(diagnostics);
    for (address, resource) in &state.applied_revision.resources {
        let ResourceKind::Graph(graph_id) = resource_kind(address) else {
            continue;
        };
        if expected_state_graph_resource_digest(state, &graph_id, resource) != resource.digest {
            diagnostics.push(Diagnostic::error(
                "external_blob_policy_digest_mismatch",
                address.clone(),
                "the applied graph resource metadata is not bound by its composite digest; restore the cluster state ledger from a trusted copy before retrying",
            ));
        }
    }
    count_errors(diagnostics) == before
}

fn persisted_external_blob_policy(
    policy: &omnigraph::ExternalBlobPolicy,
) -> Option<omnigraph::ExternalBlobPolicy> {
    (!matches!(policy, omnigraph::ExternalBlobPolicy::Deny)).then(|| policy.clone())
}

fn state_embedding_provider_digest(
    state: &ClusterState,
    embedding_provider: Option<&str>,
) -> Option<String> {
    embedding_provider
        .and_then(|address| state.applied_revision.resources.get(address))
        .map(|resource| resource.digest.clone())
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

fn graph_digest_with_external_blob_policy(
    graph_id: &str,
    schema_digest: Option<&String>,
    query_digests: Option<&BTreeMap<String, String>>,
    embedding_provider: Option<&str>,
    embedding_provider_digest: Option<&String>,
    external_blob_policy: &omnigraph::ExternalBlobPolicy,
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
    if let Some(provider) = embedding_provider {
        input.push_str("embedding_provider\0");
        input.push_str(provider);
        input.push('\0');
        input.push_str(embedding_provider_digest.map_or("", String::as_str));
        input.push('\0');
    }
    // `Deny` is the historical/default graph meaning, so omitting its marker
    // keeps pre-policy graph digests stable. Any allow-list is normalized
    // before reaching this function and therefore has one deterministic hash.
    if !matches!(external_blob_policy, omnigraph::ExternalBlobPolicy::Deny) {
        input.push_str("external_blob_policy\0");
        input.push_str(
            &serde_json::to_string(external_blob_policy)
                .expect("external Blob policy must serialize deterministically"),
        );
        input.push('\0');
    }
    sha256_hex(input.as_bytes())
}

#[cfg(test)]
fn graph_digest(
    graph_id: &str,
    schema_digest: Option<&String>,
    query_digests: Option<&BTreeMap<String, String>>,
    embedding_provider: Option<&str>,
    embedding_provider_digest: Option<&String>,
) -> String {
    graph_digest_with_external_blob_policy(
        graph_id,
        schema_digest,
        query_digests,
        embedding_provider,
        embedding_provider_digest,
        &omnigraph::ExternalBlobPolicy::Deny,
    )
}

fn embedding_provider_digest(profile: &EmbeddingProviderConfig) -> String {
    let mut input = String::from("embedding-provider\0");
    let config_semantics =
        serde_json::to_string(profile).expect("embedding provider config must serialize");
    input.push_str(&config_semantics);
    sha256_hex(input.as_bytes())
}

fn desired_config_digest(
    raw: &RawClusterConfig,
    resource_digests: &BTreeMap<String, String>,
) -> String {
    // Hash parsed semantics, not raw YAML bytes, so comments and formatting do
    // not create a new desired revision and the digest cannot drift from parse.
    let config_semantics =
        serde_json::to_string(raw).expect("raw cluster config must serialize deterministically");
    desired_config_digest_from_semantics(&config_semantics, resource_digests)
}

fn desired_config_digest_from_semantics(
    config_semantics: &str,
    resource_digests: &BTreeMap<String, String>,
) -> String {
    let mut input = String::from("cluster-config\0");
    input.push_str(config_semantics);
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
