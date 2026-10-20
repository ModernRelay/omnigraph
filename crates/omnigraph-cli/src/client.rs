//! `GraphClient` — the one place the embedded-vs-remote split lives
//! (RFC-009 Phase 3). A CLI command body calls a verb method; the
//! enum routes to the engine (local URI) or HTTP (remote URI). The
//! 15 per-command `if graph.is_remote { … } else { … }` forks collapse
//! into two arms here.
//!
//! Phase 3a put the factory + the uniform read verbs in place. Phase 3b
//! adds the data-plane writes (`load`/`mutate`/`branch_*`/
//! `apply_schema`) and `query`. The wrinkle 3a deferred: writes open the
//! local engine WITH policy (`open_local_db_with_policy`) and carry a
//! resolved actor, while reads/`query` open WITHOUT policy. So the
//! `Embedded` variant grows an optional policy context (`graph`/`actor`)
//! and a second factory (`resolve_with_policy`) fills it; `resolve()`
//! leaves it empty. The open path picks itself from whether `graph` is
//! set, preserving today's two behaviors exactly. Export + graphs-list
//! land in 3c. Behavior is unchanged per verb — the Phase-1 parity matrix
//! is the referee and stays textually unchanged.
//!
//! Enum, not a trait (RFC sketch said "trait"): only two variants ever,
//! and inherent async methods sidestep `async_trait` boxing plus the
//! `apply_schema` catalog-validator closure that is not object-safe.
//! Same one-body-two-impls collapse, less ceremony.

use std::io::Write;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use omnigraph::db::{Omnigraph, ReadTarget, SnapshotId};
use omnigraph::settings::{SessionSettings, SettingId, SettingValue, Source};
use omnigraph::{BLOB_READ_RANGE_MAX_BYTES, BlobContent, Session};
use omnigraph_api_types::{
    BlobReadQuery, BlobStatOutput, BlobWriteOutput, BlobWriteQuery, BranchCreateOutput,
    BranchCreateRequest, BranchDeleteOutput, BranchListOutput, BranchMergeOutcome,
    BranchMergeOutput, BranchMergeRequest, BranchOutcomeOutput, ChangeBaselineOutput,
    ChangeBaselineRecord, ChangeBaselineRequest, ChangeFeedOutput, ChangeOpOutput, ChangeOutput,
    ChangeRequest, CommitChangesOutput, CommitListOutput, CommitOutput, DiffOutput,
    EntityKindOutput, ExportRequest, GraphBatchLoadOutput, GraphDiscoveryResponse,
    GraphListResponse, InvokeStoredQueryRequest, MergeBaseOutput, QueryRequest, ReadOutput,
    SchemaApplyOutput, SchemaOutput, SettingsRequest, SnapshotOutput, branch_list_read_output,
    branch_read_output, change_baseline_output, change_feed_output, change_scope,
    commit_changes_output, commit_output, diff_output, merge_base_output, read_output,
    schema_apply_output, show_read_output, snapshot_payload,
};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::query::ast::{BranchCreateFrom, BranchRead, BranchWrite, CommitWrite};
use omnigraph_compiler::query::parser::parse_query;
use reqwest::header::{CONTENT_RANGE, RANGE};
use reqwest::{Method, StatusCode};
use serde_json::Value;

use crate::blob_cli::{
    BlobRangeRequest, blob_cell, blob_read_target, blob_url, blob_write_cell, blob_write_url,
    external_response_headers, managed_response_headers, map_embedded_blob_error,
    remote_blob_error, whole_external_uri,
};
use crate::cli::CliLoadMode;
use crate::command_outcome::{EngineWrite as _, EngineWriteError};
use crate::graph_http::{ApiContractError, GraphHttpClient};
use crate::helpers::{
    PreconditionFailedCli, apply_bearer_token, apply_server_flag, branch_statement_change_request,
    branch_statement_query_request, is_remote_uri, missing_credential, precondition_failed_cli,
    query_params_from_json, remote_json, remote_json_bounded, remote_no_content,
    remote_response_json_bounded, remote_url, resolve_actor_with_config, resolve_cli_graph,
    resolve_server_flag_with_config, select_named_query, served_client_with_config,
};
use crate::operator::{InvocationConfig, OperatorConfig};
use crate::output::{LoadOutput, load_output_from_graph_batch, load_output_from_receipt};

const MANAGED_LOAD_REQUEST_LIMIT: usize = 32 * 1024 * 1024;
// Managed load transport has its own longer bounded receipt wait.
// Managed queries and mutations share a thirty-second total request deadline.
const MANAGED_LOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// A success response must describe the merge that was submitted. A receipt
/// cannot be recovered by reading a mutable HEAD after receiving bad evidence.
fn validate_merge_receipt(
    outcome: BranchMergeOutcome,
    commit: Option<&CommitOutput>,
    target: &str,
    actor: Option<&str>,
) -> Result<()> {
    match (outcome, commit) {
        (BranchMergeOutcome::AlreadyUpToDate, None) => Ok(()),
        (BranchMergeOutcome::FastForward | BranchMergeOutcome::Merged, Some(commit))
            if !commit.graph_commit_id.is_empty()
                && commit.graph_manifest_version > 0
                && commit.graph_branch.as_deref().unwrap_or("main") == target
                && commit
                    .parent_commit_id
                    .as_deref()
                    .is_some_and(|id| !id.is_empty())
                && commit
                    .merged_parent_commit_id
                    .as_deref()
                    .is_some_and(|id| !id.is_empty())
                && commit.actor_id.as_deref() == actor =>
        {
            Ok(())
        }
        _ => bail!(
            "invalid merge response: outcome and commit disagree; effects are unknown; reconcile before retrying"
        ),
    }
}

fn validate_merge_output(
    output: &BranchMergeOutput,
    source: &str,
    target: &str,
    delete_branch: bool,
    as_commit: bool,
    dry_run: bool,
) -> Result<()> {
    if output.source != source || output.target != target {
        bail!(
            "invalid merge response: source or target differs from the request; effects are unknown; reconcile before retrying"
        );
    }
    if output.dry_run != dry_run {
        bail!(
            "invalid merge response: the dry-run flag contradicts the request; effects are unknown; reconcile before retrying"
        );
    }
    if dry_run {
        // A dry run writes nothing: no commit, no deletion.
        if output.commit.is_some()
            || output.branch_deleted.is_some()
            || output.branch_delete_error_details.is_some()
        {
            bail!(
                "invalid merge response: a dry run reported a write; effects are unknown; reconcile before retrying"
            );
        }
        return Ok(());
    }
    validate_merge_receipt(
        output.outcome,
        output.commit.as_ref(),
        target,
        output.actor_id.as_deref(),
    )?;
    if output
        .commit
        .as_ref()
        .is_some_and(|commit| commit.published == as_commit)
    {
        bail!(
            "invalid merge response: the commit's publication state contradicts the request; effects are unknown; reconcile before retrying"
        );
    }
    match (
        delete_branch,
        output.branch_deleted,
        &output.branch_delete_error_details,
    ) {
        (false, None, None) | (true, Some(true), None) => Ok(()),
        (true, Some(false), Some(error)) if !error.error.is_empty() => Ok(()),
        _ => bail!(
            "invalid merge response: optional deletion result is incomplete or inconsistent; effects are unknown; reconcile before retrying"
        ),
    }
}

/// The engine owns parsed-table limits. This bound covers only the exact
/// UTF-8 NDJSON body sent to the existing server route, before any request.
fn read_managed_load_data(path: &str) -> Result<String> {
    use std::io::Read;

    let file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.take(MANAGED_LOAD_REQUEST_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MANAGED_LOAD_REQUEST_LIMIT {
        bail!("managed load request exceeds 32 MiB; split the input into bounded batches");
    }
    String::from_utf8(bytes).map_err(|_| eyre!("managed load input must be valid UTF-8"))
}

fn load_request(
    request: reqwest::RequestBuilder,
    data: String,
    managed: bool,
) -> reqwest::RequestBuilder {
    let request = if managed {
        request.timeout(MANAGED_LOAD_TIMEOUT)
    } else {
        request
    };
    request
        .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
        .body(data)
}

fn blob_transport_error(error: color_eyre::Report) -> color_eyre::Report {
    if error.downcast_ref::<reqwest::Error>().is_some() {
        eyre!("Blob server request failed")
    } else {
        error
    }
}

/// Why a served `load` refuses a `--set`: neither load route's request
/// type carries a `settings` field, so a value could only be dropped.
const SETTINGS_AT_SERVED_LOAD: &str = "load takes --set only on an embedded store; \
                                       the served load routes carry no settings field";

/// The `--set name=value` flags of one invocation, each checked against the
/// settings definition (the Session settings RFC). Scope is the transport's: the embedded
/// session accepts every setting, a remote request refuses a `process` one.
pub(crate) fn parse_set_flags(flags: &[String]) -> Result<Vec<(SettingId, SettingValue)>> {
    let mut settings = Vec::with_capacity(flags.len());
    for flag in flags {
        let Some((name, value)) = flag.split_once('=') else {
            bail!("--set takes NAME=VALUE, got '{flag}'");
        };
        settings.push(SettingId::parse_assignment(name, value)?);
    }
    Ok(settings)
}

pub(crate) enum GraphClient {
    /// Local engine at `uri`. Reads (`resolve()`) leave `actor` empty;
    /// writes (`resolve_with_policy()`) attribute the resolved actor.
    /// Direct-store access carries no Cedar policy (RFC-011: policy lives
    /// in the cluster/server, not in per-operator addressing).
    Embedded { uri: String, actor: Option<String> },
    /// Remote HTTP server. The actor is resolved server-side from the
    /// token; the client never sets identity.
    Remote {
        http: GraphHttpClient,
        base_url: String,
        token: Option<String>,
        response_limit: Option<usize>,
        catalogue: bool,
    },
    /// The engine catalogue scope `og_catalog`: the binary's own registries
    /// read as `@` types, with no store opened. Serves read queries and
    /// `schema show`; every other verb is refused.
    Catalogue,
}

/// The refusal every verb outside the catalogue scope's reads answers.
pub(crate) fn catalogue_scope_refusal(verb: &str) -> color_eyre::Report {
    eyre!(
        "`{verb}` is not available under `{}`: the engine catalogue scope serves read queries, `explain` and `schema show` only",
        omnigraph::catalogue::CATALOGUE_SCOPE
    )
}

/// Whether an addressed store is the exact catalogue scope token; `./og_catalog`,
/// an absolute path and a `file://` URI stay ordinary store addresses.
pub(crate) fn is_catalogue_scope(uri: Option<&str>) -> bool {
    uri == Some(omnigraph::catalogue::CATALOGUE_SCOPE)
}

/// RFC-011 Decision 7: a server scope that selects no graph (no `--graph`, no
/// `default_graph`) must not silently fall through to the bare server URL when
/// the server is multi-graph. Best-effort probe `GET /graphs`: a populated list
/// forces `--graph` (listing the candidates); a single-graph/flat server (405),
/// a policy-gated `/graphs` proceeds — the bare URL
/// is then correct, or the real request surfaces the failure. Only fires on the
/// no-graph path. Contract/discovery failures always stop without fallback.
async fn require_graph_for_multi_graph_server(
    probe: &GraphClient,
    scope: &crate::scope::ResolvedScope,
) -> Result<()> {
    let (Some(server), None) = (scope.server.as_deref(), scope.graph.as_deref()) else {
        return Ok(());
    };
    let resp = match probe.list_graphs().await {
        Ok(resp) => resp,
        Err(error) if error.downcast_ref::<ApiContractError>().is_some() => return Err(error),
        Err(_) => return Ok(()),
    };
    if !resp.graphs.is_empty() {
        let ids: Vec<&str> = resp.graphs.iter().map(|g| g.graph_id.as_str()).collect();
        bail!(
            "server scope '{server}' has {} {}: [{}]; pass --graph <id> to select one \
             (or set `default_graph` in your operator config)",
            ids.len(),
            if ids.len() == 1 { "graph" } else { "graphs" },
            ids.join(", ")
        );
    }
    Ok(())
}

/// A remote graph must be addressed with `--server` (RFC-011): a positional or
/// `--uri` server URL (`http(s)://` or `unix://`) no longer auto-dispatches to a
/// server. A remote URL produced by a server scope (`via_server`) is fine.
fn reject_positional_remote(via_server: bool, uri: &str) -> Result<()> {
    if !via_server && is_remote_uri(uri) {
        bail!(
            "a remote graph must be addressed with `--server <url>` — a positional \
             (or `--uri`) http(s):// or unix:// URL no longer dispatches to a server"
        );
    }
    Ok(())
}

impl GraphClient {
    /// A validated managed credential does not resolve operator profiles or tokens.
    pub(crate) fn managed(endpoint: &str, graph: &str, token: String) -> Result<Self> {
        Self::managed_url(
            endpoint,
            remote_url(endpoint, &["graphs", graph], &[])?,
            token,
            is_catalogue_scope(Some(graph)),
        )
    }

    pub(crate) fn managed_registry(endpoint: &str, token: String) -> Result<Self> {
        Self::managed_url(endpoint, endpoint.to_owned(), token, false)
    }

    fn managed_url(
        endpoint: &str,
        base_url: String,
        token: String,
        catalogue: bool,
    ) -> Result<Self> {
        Ok(Self::Remote {
            http: GraphHttpClient::managed(endpoint)?,
            base_url,
            token: Some(token),
            response_limit: Some(8 * 1024 * 1024),
            catalogue,
        })
    }

    /// The single owner of registry (`GET /graphs`) addressing: the bare base
    /// URL of `server` (a config name or literal URL) — never `/graphs/<id>`
    /// — with the keyed bearer-token chain. Uses the caller's captured config;
    /// credential is captured for both discovery and data. Used by the RFC-011 D7 multi-graph probe
    /// and the `graphs list` registry factory.
    fn registry_client(
        input: &InvocationConfig,
        operator: &OperatorConfig,
        server: &str,
    ) -> Result<Self> {
        let base = resolve_server_flag_with_config(operator, Some(server), None)?
            .expect("server name is present");
        let (http, token) = served_client_with_config(input, operator, &base, &base)?;
        Ok(GraphClient::Remote {
            http,
            base_url: base,
            token,
            response_limit: None,
            catalogue: false,
        })
    }

    /// Served-REGISTRY factory (RFC-011): resolve a server scope (`--server`
    /// / `--profile` / `defaults.server`) to the bare server base URL for
    /// `graphs list`. Synchronous by design: the RFC-011 D7 multi-graph probe
    /// (`require_graph_for_multi_graph_server`) is async, so it structurally
    /// cannot run on this path — `graphs list` IS the enumeration the probe
    /// performs. There is no graph selection and no `/graphs/<id>` append; a
    /// scope's `default_graph` is deliberately ignored (rejecting a config
    /// default would make `graphs list` unusable in any profile that sets
    /// one, and the registry is server-scoped either way). An explicit
    /// `--graph` never reaches here — the addressing guard rejects it.
    pub(crate) fn resolve_registry(
        input: &InvocationConfig,
        server: Option<&str>,
        profile: Option<&str>,
        stderr: &mut dyn Write,
    ) -> Result<Self> {
        let operator = input.get(stderr)?;
        let scope = crate::scope::resolve_scope(
            operator,
            input.profile(),
            crate::planes::Capability::Served,
            crate::scope::ScopeFlags {
                target: input.target(),
                profile,
                store: None,
                server,
                cluster: None,
                graph: None,
                uri: None,
                reads_root: false,
            },
        )?;
        let Some(server) = scope.server.as_deref() else {
            bail!(
                "`graphs list` needs a server scope — pass --server <name|url> or \
                 --profile <name>, or set `defaults.server` in ~/.omnigraph/config.yaml"
            );
        };
        // Server resolution refuses any scope that is not a server address.
        Self::registry_client(input, operator, server)
    }

    /// Resolve the addressing (positional URI / `--target` / `--server`)
    /// and credential once, then pick the variant by URI scheme — the
    /// single branch point that replaces every per-command `is_remote`
    /// fork. Mirrors the read verbs' current preamble (`resolve_uri`
    /// path, not the policy-bearing `resolve_cli_graph`). Used by reads
    /// and `query` (which opens without policy, like the reads).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn resolve(
        input: &InvocationConfig,
        capability: crate::planes::Capability,
        server: Option<&str>,
        graph: Option<&str>,
        uri: Option<String>,
        profile: Option<&str>,
        store: Option<&str>,
        stderr: &mut dyn Write,
    ) -> Result<Self> {
        let operator = input.get(stderr)?;
        Self::resolve_with_config(
            input, operator, capability, server, graph, uri, profile, store,
        )
        .await
    }

    /// Reuse the caller's validated configuration for routing and presentation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn resolve_with_config(
        input: &InvocationConfig,
        operator: &OperatorConfig,
        capability: crate::planes::Capability,
        server: Option<&str>,
        graph: Option<&str>,
        uri: Option<String>,
        profile: Option<&str>,
        store: Option<&str>,
    ) -> Result<Self> {
        // RFC-011: a scope (profile / --store / operator defaults) may stand in
        // for omitted addressing. The explicit branch passes server/graph/uri
        // straight through, so existing invocations are unchanged. The caller
        // threads its verb's declared capability (planes::command_capability)
        // so scope resolution and the addressing guard share one
        // classification; every current caller is a data-plane (`Any`) verb —
        // registry-scoped `graphs list` uses `resolve_registry` instead.
        let scope = crate::scope::resolve_scope(
            operator,
            input.profile(),
            capability,
            crate::scope::ScopeFlags {
                target: input.target(),
                profile,
                store,
                server,
                cluster: None,
                graph,
                uri,
                reads_root: false,
            },
        )?;
        // The no-graph probe and eventual request use one acquired credential.
        // Keep discovery at the read path's existing validation boundary.
        let registry =
            if let (Some(server), None) = (scope.server.as_deref(), scope.graph.as_deref()) {
                let registry = Self::registry_client(input, operator, server)?;
                require_graph_for_multi_graph_server(&registry, &scope).await?;
                Some(registry)
            } else {
                None
            };
        let (server, graph, uri) = (scope.server.as_deref(), scope.graph.as_deref(), scope.uri);
        if server.is_none() && is_catalogue_scope(uri.as_deref()) {
            return Ok(GraphClient::Catalogue);
        }
        let via_server = server.is_some();
        let server_root = resolve_server_flag_with_config(operator, server, None)?;
        let uri = apply_server_flag(operator, server_root.as_deref(), graph, uri)?;
        let uri = crate::helpers::resolve_uri(uri)?;
        reject_positional_remote(via_server, &uri)?;
        if is_remote_uri(&uri) {
            if let Some(registry) = registry {
                return Ok(registry);
            }
            let (http, token) = served_client_with_config(
                input,
                operator,
                server_root.as_deref().expect("remote server scope"),
                &uri,
            )?;
            Ok(GraphClient::Remote {
                http,
                base_url: uri,
                token,
                response_limit: None,
                catalogue: is_catalogue_scope(graph),
            })
        } else {
            Ok(GraphClient::Embedded { uri, actor: None })
        }
    }

    /// Write-path factory: the same addressing/credential resolution as
    /// `resolve()`, but through the stricter `resolve_cli_graph` (which
    /// carries `policy_file`/`graph_id`/`selected`), and with the actor
    /// resolved up front. The embedded arm then opens WITH policy. The
    /// resolution order matches the write arms exactly: server flag →
    /// graph → the served credential.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn resolve_with_policy(
        input: &InvocationConfig,
        capability: crate::planes::Capability,
        server: Option<&str>,
        graph: Option<&str>,
        uri: Option<String>,
        cli_as: Option<&str>,
        profile: Option<&str>,
        store: Option<&str>,
        stderr: &mut dyn Write,
    ) -> Result<Self> {
        // RFC-011 scope translation (see `resolve`); explicit addressing passes
        // through unchanged, and the caller threads its verb's declared
        // capability.
        let operator = input.get(stderr)?;
        let scope = crate::scope::resolve_scope(
            operator,
            input.profile(),
            capability,
            crate::scope::ScopeFlags {
                target: input.target(),
                profile,
                store,
                server,
                cluster: None,
                graph,
                uri,
                reads_root: false,
            },
        )?;
        Self::resolve_with_policy_scope(input, operator, scope, cli_as).await
    }

    async fn resolve_with_policy_scope(
        input: &InvocationConfig,
        operator: &OperatorConfig,
        scope: crate::scope::ResolvedScope,
        cli_as: Option<&str>,
    ) -> Result<Self> {
        let (server, graph) = (scope.server.as_deref(), scope.graph.as_deref());
        if server.is_none() && is_catalogue_scope(scope.uri.as_deref()) {
            return Err(catalogue_scope_refusal("a write"));
        }
        let via_server = server.is_some();
        let server_root = resolve_server_flag_with_config(operator, server, None)?;
        let uri = apply_server_flag(operator, server_root.as_deref(), graph, scope.uri.clone())?;
        let resolved = resolve_cli_graph(uri)?;
        reject_positional_remote(via_server, &resolved.uri)?;
        if resolved.is_remote {
            // A served write's actor is the one its bearer token resolves
            // to. `--as` cannot set it; the server refuses the request when
            // the token resolves to another actor. `operator.actor` names the
            // direct actor and is not asserted against servers.
            // Complete local addressing/identity validation before discovery.
            // With no selected graph, this is already the registry address.
            let (http, token) = served_client_with_config(
                input,
                operator,
                server_root.as_deref().expect("remote server scope"),
                &resolved.uri,
            )?;
            let http = http.with_expected_actor(cli_as);
            let client = GraphClient::Remote {
                http,
                base_url: resolved.uri,
                token,
                response_limit: None,
                catalogue: is_catalogue_scope(graph),
            };
            require_graph_for_multi_graph_server(&client, &scope).await?;
            Ok(client)
        } else {
            let actor = resolve_actor_with_config(operator, cli_as);
            Ok(GraphClient::Embedded {
                uri: resolved.uri,
                actor,
            })
        }
    }

    /// The graph URI (local path / remote base URL) this client addresses.
    pub(crate) fn uri(&self) -> &str {
        match self {
            GraphClient::Embedded { uri, .. } => uri,
            GraphClient::Remote { base_url, .. } => base_url,
            GraphClient::Catalogue => omnigraph::catalogue::CATALOGUE_SCOPE,
        }
    }

    pub(crate) fn is_catalogue(&self) -> bool {
        matches!(
            self,
            Self::Catalogue
                | Self::Remote {
                    catalogue: true,
                    ..
                }
        )
    }

    /// The catalogue scope under the environment's defaults and the `--set`
    /// values, the one session the embedded CLI is.
    fn catalogue_scope(
        settings: &[(SettingId, SettingValue)],
    ) -> Result<omnigraph::catalogue::CatalogueScope> {
        let (defaults, _) = omnigraph::settings::from_env()?;
        let mut scope = omnigraph::catalogue::CatalogueScope::new(defaults)?;
        for (id, value) in settings {
            scope.set(*id, value)?;
        }
        Ok(scope)
    }

    pub(crate) fn is_remote(&self) -> bool {
        matches!(self, GraphClient::Remote { .. })
    }

    /// Writable verbs alone acquire durable cluster admission. Direct-store
    /// access carries no Cedar policy (RFC-011), the actor rides the `_as` APIs.
    async fn open_write(uri: &str, stderr: &mut dyn Write) -> Result<Session> {
        Self::open_write_session(uri, &[], stderr).await
    }

    /// Read dispatch uses no writable opener or retained writer admission.
    /// Settings are still checked before storage access, as for write dispatch.
    async fn open_read_session(
        uri: &str,
        settings: &[(SettingId, SettingValue)],
    ) -> Result<Session> {
        Self::open_read_session_with_external_blob_policy(uri, settings, None).await
    }

    /// An embedded read session, with `external_blob_policy` installed on
    /// its fresh handle when given: `explain load csv "file://…"` probes its
    /// source under the same base a direct load installs.
    async fn open_read_session_with_external_blob_policy(
        uri: &str,
        settings: &[(SettingId, SettingValue)],
        external_blob_policy: Option<omnigraph::ExternalBlobPolicy>,
    ) -> Result<Session> {
        let (defaults, sources) = omnigraph::settings::from_env()?;
        let db = crate::admission::open_read_only(uri, None).await?;
        let db = match external_blob_policy {
            Some(policy) => db.with_external_blob_policy(policy)?,
            None => db,
        };
        let mut session = Arc::new(db).session(defaults, sources);
        for (id, value) in settings {
            session.set(*id, value, Source::Request)?;
        }
        Ok(session)
    }

    /// The embedded CLI is the process (the Session settings RFC): one session over the
    /// environment's defaults and the `--set` values, every setting accepted;
    /// the source's own `set` lines apply per call, on top.
    async fn open_write_session(
        uri: &str,
        settings: &[(SettingId, SettingValue)],
        stderr: &mut dyn Write,
    ) -> Result<Session> {
        Self::open_write_session_with_external_blob_policy(uri, settings, None, stderr).await
    }

    /// An embedded write session, with `external_blob_policy` installed on
    /// its fresh handle when given: the one way a CLI run admits a source
    /// outside the graph (a `load csv "file://…"` statement run directly).
    async fn open_write_session_with_external_blob_policy(
        uri: &str,
        settings: &[(SettingId, SettingValue)],
        external_blob_policy: Option<omnigraph::ExternalBlobPolicy>,
        stderr: &mut dyn Write,
    ) -> Result<Session> {
        let (defaults, sources) = omnigraph::settings::from_env()?;
        crate::admission::ensure_graph(uri, stderr).await?;
        crate::command_outcome::writable_open();
        let db = Omnigraph::open(uri).await?;
        let db = match external_blob_policy {
            Some(policy) => db.with_external_blob_policy(policy)?,
            None => db,
        };
        let mut session = Arc::new(db).session(defaults, sources);
        for (id, value) in settings {
            session.set(*id, value, Source::Request)?;
        }
        Ok(session)
    }

    /// The request's `settings` field for the `--set` values. A `process`
    /// setting is refused here, before anything is sent: the typed field
    /// cannot carry it.
    fn remote_settings(settings: &[(SettingId, SettingValue)]) -> Result<Option<SettingsRequest>> {
        if settings.is_empty() {
            return Ok(None);
        }
        let mut given = SessionSettings::default();
        let mut request = SettingsRequest::default();
        for (id, value) in settings {
            id.refuse_from_request()?;
            given.set(*id, value)?;
            match id {
                SettingId::Engine => request.engine = Some(given.engine()),
                SettingId::MergeLineage => request.merge_lineage = Some(given.merge_lineage()),
                SettingId::AnnNprobes => {
                    request.ann_nprobes = Some(given.get(SettingId::AnnNprobes).parse()?)
                }
                SettingId::TraversalWorkLimit => {
                    request.traversal_work_limit =
                        Some(given.get(SettingId::TraversalWorkLimit).parse()?)
                }
                SettingId::WriteMaxBytes => {
                    request.write_max_bytes = Some(given.get(SettingId::WriteMaxBytes).parse()?)
                }
                SettingId::LoadSourceMaxBytes => {
                    request.load_source_max_bytes =
                        Some(given.get(SettingId::LoadSourceMaxBytes).parse()?)
                }
                SettingId::QueryMemoryBytes => {
                    request.query_memory_bytes =
                        Some(given.get(SettingId::QueryMemoryBytes).parse()?)
                }
                SettingId::HistoryReleaseBytes => {
                    request.history_release_bytes =
                        Some(given.get(SettingId::HistoryReleaseBytes).parse()?)
                }
                SettingId::RrfPlan
                | SettingId::StageWriteConcurrency
                | SettingId::ExplainDumpDir => {
                    bail!(
                        "setting `{}` has request scope but no request field; add it to \
                         `SettingsRequest`",
                        id.name()
                    )
                }
            }
        }
        Ok(Some(request))
    }

    /// The `set=<name>=<value>` query parameters of a remote change surface,
    /// under the same scope rule as `remote_settings`.
    fn set_query_values(settings: &[(SettingId, SettingValue)]) -> Result<Vec<String>> {
        settings
            .iter()
            .map(|(id, value)| {
                id.refuse_from_request()?;
                Ok(format!("{}={value}", id.name()))
            })
            .collect()
    }

    /// Apply the source's `set` and `reset` lines to `session`, for the
    /// statements the engine does not parse itself (`branch merge`, `show`).
    fn apply_prefix(session: &mut Session, source: &str) -> Result<()> {
        let prefixed = session.with_prefix(&parse_query(source)?.settings)?;
        *session = prefixed;
        Ok(())
    }

    pub(crate) async fn branch_list(&self) -> Result<BranchListOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["branches"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let session = Self::open_read_session(uri, &[]).await?;
                let mut branches = session.branch_list().await?;
                branches.sort();
                Ok(BranchListOutput { branches })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("branch list")),
        }
    }

    pub(crate) async fn snapshot(&self, branch: &str) -> Result<SnapshotOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["snapshot"], &[("branch", branch)])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                let snapshot = db.snapshot_of(ReadTarget::branch(branch)).await?;
                let internal_schema_version = db.internal_schema_version_at(&snapshot).await?;
                snapshot_payload(branch, &snapshot, internal_schema_version)
                    .map_err(|error| eyre!(error))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("snapshot")),
        }
    }

    pub(crate) async fn schema_source(&self) -> Result<SchemaOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["schema"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                Ok(SchemaOutput {
                    schema_source: db.schema_source().to_string(),
                    system_columns: Some(db.catalog().system_columns.into()),
                    catalogue_source: Some(omnigraph_compiler::catalogue_declarations()),
                })
            }
            GraphClient::Catalogue => {
                let scope = Self::catalogue_scope(&[])?;
                Ok(SchemaOutput {
                    schema_source: String::new(),
                    system_columns: Some(scope.catalog().system_columns.into()),
                    catalogue_source: Some(scope.schema_source()),
                })
            }
        }
    }

    pub(crate) async fn list_commits(&self, branch: Option<&str>) -> Result<CommitListOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let url = match branch {
                    Some(branch) => remote_url(base_url, &["commits"], &[("branch", branch)])?,
                    None => remote_url(base_url, &["commits"], &[])?,
                };
                remote_json_bounded(
                    http,
                    Method::GET,
                    url,
                    None,
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                let commits = db
                    .list_commits(branch)
                    .await?
                    .iter()
                    .map(commit_output)
                    .collect::<Vec<_>>();
                Ok(CommitListOutput {
                    commits,
                    head_commit_id: None,
                    next_page_token: None,
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("list commits")),
        }
    }

    /// `commit list --limit`/`--page-token`: one page of the branch's history
    /// (`None` = main), `GET /commits?limit=&page_token=` or the engine's
    /// paged listing.
    pub(crate) async fn list_commits_page(
        &self,
        branch: Option<&str>,
        page_token: Option<&str>,
        limit: Option<usize>,
    ) -> Result<CommitListOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let limit_value = limit.map(|limit| limit.to_string());
                let mut query = Vec::new();
                if let Some(branch) = branch {
                    query.push(("branch", branch));
                }
                if let Some(page_token) = page_token {
                    query.push(("page_token", page_token));
                }
                if let Some(limit) = limit_value.as_deref() {
                    query.push(("limit", limit));
                }
                remote_json_bounded(
                    http,
                    Method::GET,
                    remote_url(base_url, &["commits"], &query)?,
                    None,
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                let page = db.list_commits_page(branch, page_token, limit).await?;
                Ok(CommitListOutput {
                    commits: page.commits.iter().map(commit_output).collect(),
                    head_commit_id: Some(page.head_commit_id),
                    next_page_token: page.next_page_token,
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("list commits")),
        }
    }

    /// `commit list --unpublished`: the proposals whose recorded target is
    /// `branch` (`None` = main), `GET /commits?unpublished=true` or the
    /// engine's listing.
    pub(crate) async fn list_unpublished_commits(
        &self,
        branch: Option<&str>,
    ) -> Result<CommitListOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let url = match branch {
                    Some(branch) => remote_url(
                        base_url,
                        &["commits"],
                        &[("branch", branch), ("unpublished", "true")],
                    )?,
                    None => remote_url(base_url, &["commits"], &[("unpublished", "true")])?,
                };
                remote_json_bounded(
                    http,
                    Method::GET,
                    url,
                    None,
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                let commits = db
                    .list_unpublished_commits(branch)
                    .await?
                    .iter()
                    .map(commit_output)
                    .collect::<Vec<_>>();
                Ok(CommitListOutput {
                    commits,
                    head_commit_id: None,
                    next_page_token: None,
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("list unpublished commits")),
        }
    }

    /// `commit list --unattached`: the live unattached snapshots whose
    /// recorded target is `branch` (`None` = main),
    /// `GET /commits?unattached=true` or the engine's listing.
    pub(crate) async fn list_unattached_commits(
        &self,
        branch: Option<&str>,
    ) -> Result<CommitListOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let url = match branch {
                    Some(branch) => remote_url(
                        base_url,
                        &["commits"],
                        &[("branch", branch), ("unattached", "true")],
                    )?,
                    None => remote_url(base_url, &["commits"], &[("unattached", "true")])?,
                };
                remote_json_bounded(
                    http,
                    Method::GET,
                    url,
                    None,
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                let commits = db
                    .list_unattached_commits(branch)
                    .await?
                    .iter()
                    .map(commit_output)
                    .collect::<Vec<_>>();
                Ok(CommitListOutput {
                    commits,
                    head_commit_id: None,
                    next_page_token: None,
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("list unattached commits")),
        }
    }

    pub(crate) async fn get_commit(&self, commit_id: &str) -> Result<CommitOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                remote_json_bounded(
                    http,
                    Method::GET,
                    remote_url(base_url, &["commits", commit_id], &[])?,
                    None,
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let session = Self::open_read_session(uri, &[]).await?;
                Ok(commit_output(&session.get_commit(commit_id).await?))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("get commit")),
        }
    }

    /// Fetch one bounded page of a commit's entity diff. Auto-pagination is
    /// deliberately owned by the command output loop, which emits each page
    /// before fetching the next one instead of rebuilding an unbounded result.
    pub(crate) async fn commit_changes_page(
        &self,
        commit_id: &str,
        page_token: Option<&str>,
        limit: Option<usize>,
        filter: &ChangeFilterArgs<'_>,
        settings: &[(SettingId, SettingValue)],
    ) -> Result<CommitChangesOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                let limit_value = limit.map(|limit| limit.to_string());
                let set_values = Self::set_query_values(settings)?;
                let mut query = Vec::new();
                if let Some(page_token) = page_token {
                    query.push(("page_token", page_token));
                }
                if let Some(limit) = limit_value.as_deref() {
                    query.push(("limit", limit));
                }
                let filter_pairs = filter.query_pairs();
                query.extend(
                    filter_pairs
                        .iter()
                        .map(|(name, value)| (*name, value.as_str())),
                );
                query.extend(set_values.iter().map(|value| ("set", value.as_str())));
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["commits", commit_id, "changes"], &query)?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let session = Self::open_read_session(uri, settings).await?;
                let scope = change_scope(filter.kinds, filter.types, filter.ops);
                let page = session
                    .commit_changes_page(commit_id, &scope, page_token, limit, None)
                    .await?;
                Ok(commit_changes_output(&page))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("commit changes page")),
        }
    }

    /// Fetch one bounded page of a captured feed poll. The caller continues
    /// with `next_page_token`; this method never aggregates pages in memory.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn poll_changes_page(
        &self,
        branch: Option<&str>,
        cursor: Option<&str>,
        start: Option<&str>,
        page_token: Option<&str>,
        limit: Option<usize>,
        filter: &ChangeFilterArgs<'_>,
        settings: &[(SettingId, SettingValue)],
    ) -> Result<ChangeFeedOutput> {
        // A page token continues one poll and supersedes the start position.
        let (cursor, start) = if page_token.is_some() {
            (None, None)
        } else {
            (cursor, start)
        };
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                let limit_value = limit.map(|limit| limit.to_string());
                let set_values = Self::set_query_values(settings)?;
                let mut query = Vec::new();
                if let Some(branch) = branch {
                    query.push(("branch", branch));
                }
                if let Some(cursor) = cursor {
                    query.push(("cursor", cursor));
                }
                if let Some(start) = start {
                    query.push(("start", start));
                }
                if let Some(page_token) = page_token {
                    query.push(("page_token", page_token));
                }
                if let Some(limit) = limit_value.as_deref() {
                    query.push(("limit", limit));
                }
                let filter_pairs = filter.query_pairs();
                query.extend(
                    filter_pairs
                        .iter()
                        .map(|(name, value)| (*name, value.as_str())),
                );
                query.extend(set_values.iter().map(|value| ("set", value.as_str())));
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["changes"], &query)?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let session = Self::open_read_session(uri, settings).await?;
                let position = if let Some(token) = page_token {
                    omnigraph::changes::ChangeFeedPosition::PageToken(token.to_string())
                } else if let Some(cursor) = cursor {
                    omnigraph::changes::ChangeFeedPosition::Cursor(cursor.to_string())
                } else {
                    omnigraph::changes::ChangeFeedPosition::Start(parse_change_feed_start(
                        start.unwrap_or("now"),
                    )?)
                };
                let page = session
                    .poll_change_feed(omnigraph::changes::ChangeFeedRequest {
                        branch: branch.map(str::to_string),
                        position,
                        scope: change_scope(filter.kinds, filter.types, filter.ops),
                        max_changes: limit,
                        max_bytes: None,
                        max_commits: None,
                    })
                    .await?;
                Ok(change_feed_output(&page))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("poll changes page")),
        }
    }

    /// Capture a change baseline: stream the snapshot records into `writer`
    /// and return the terminal handshake. The terminal record itself is NOT
    /// written to `writer` — a stream that ends without one is an error, so a
    /// usable cursor never outlives a broken snapshot.
    pub(crate) async fn change_baseline<W: Write>(
        &self,
        branch: Option<&str>,
        filter: &ChangeFilterArgs<'_>,
        writer: &mut W,
    ) -> Result<ChangeBaselineOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                let request = apply_bearer_token(
                    http.request(
                        Method::POST,
                        remote_url(base_url, &["changes", "baseline"], &[])?,
                    )?,
                    token.as_deref(),
                )
                .json(&ChangeBaselineRequest {
                    branch: branch.map(str::to_string),
                    kind: filter.kinds.to_vec(),
                    r#type: filter.types.to_vec(),
                    op: filter.ops.to_vec(),
                });
                let mut response = http.send(request).await?;
                let status = response.status();
                if !status.is_success() {
                    // Share structured status/backoff handling with JSON responses.
                    return remote_response_json_bounded(response, token.as_deref(), None).await;
                }
                // Hold back the most recent complete line while streaming: at
                // EOF it must be the terminal handshake record. Everything
                // before it is snapshot data.
                let mut pending: Vec<u8> = Vec::new();
                let mut held: Option<Vec<u8>> = None;
                while let Some(chunk) = response.chunk().await? {
                    pending.extend_from_slice(&chunk);
                    while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                        let mut line: Vec<u8> = pending.drain(..=newline).collect();
                        line.pop();
                        if let Some(previous) = held.replace(line) {
                            writer.write_all(&previous)?;
                            writer.write_all(b"\n")?;
                        }
                    }
                }
                writer.flush()?;
                if !pending.is_empty() {
                    bail!("baseline stream ended mid-record — no usable cursor");
                }
                let terminal =
                    held.ok_or_else(|| eyre!("baseline stream carried no terminal record"))?;
                let record: ChangeBaselineRecord =
                    serde_json::from_slice(&terminal).map_err(|_| {
                        eyre!("baseline stream ended without a terminal record — no usable cursor")
                    })?;
                Ok(record.baseline)
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                let scope = change_scope(filter.kinds, filter.types, filter.ops);
                let baseline = db
                    .capture_change_baseline(branch.unwrap_or("main"), &scope, writer)
                    .await?;
                writer.flush()?;
                Ok(change_baseline_output(&baseline))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("change baseline")),
        }
    }

    /// `load` — bulk-load `data` (a file path) onto `branch`, forking from
    /// `from` if missing. Returns the CLI `LoadOutput`; each arm keeps its
    /// own mapping (remote uses the logical graph-batch result, embedded reads
    /// the engine `LoadResult` directly).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load(
        &self,
        branch: &str,
        from: Option<&str>,
        data: &str,
        mode: CliLoadMode,
        settings: &[(SettingId, SettingValue)],
        stderr: &mut dyn Write,
    ) -> Result<LoadOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                if !settings.is_empty() {
                    bail!("{}", SETTINGS_AT_SERVED_LOAD);
                }
                let data = if response_limit.is_some() {
                    read_managed_load_data(data)?
                } else {
                    std::fs::read_to_string(data)?
                };
                let mut query = vec![("branch", branch), ("mode", mode.as_str())];
                if let Some(from) = from {
                    query.push(("from", from));
                }
                let request = load_request(
                    apply_bearer_token(
                        http.request(
                            Method::POST,
                            remote_url(base_url, &["load", "ndjson"], &query)?,
                        )?,
                        token.as_deref(),
                    ),
                    data,
                    response_limit.is_some(),
                );
                // One attempt only. A lost response may follow a committed
                // load or a created branch; neither can be replayed blindly.
                let response = http.send(request).await?;
                let output: GraphBatchLoadOutput =
                    remote_response_json_bounded(response, token.as_deref(), *response_limit)
                        .await?;
                Ok(load_output_from_graph_batch(
                    base_url,
                    mode.as_str(),
                    &output,
                ))
            }
            GraphClient::Embedded { uri, actor } => {
                refuse_noncanonical_branches([Some(branch), from])?;
                let data = std::fs::read_to_string(data)?;
                let session = Self::open_write_session(uri, settings, stderr).await?;
                // A missing branch is forked first, which publishes on its
                // own; the engine marks a later refusal as following it.
                let receipt = session
                    .load_graph_batch_as_with_receipt(
                        branch,
                        from,
                        &data,
                        mode.into(),
                        actor.as_deref(),
                    )
                    .await
                    .engine_write()?;
                Ok(load_output_from_receipt(
                    uri,
                    branch,
                    mode.as_str(),
                    &receipt,
                    &session.catalog(),
                ))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("load")),
        }
    }

    /// Run a mutation against `branch`.
    ///
    /// `expected_head` is the `--if-commit` compare-and-swap precondition:
    /// the write runs only if the branch head commit still equals it. A
    /// mismatch preserves typed precondition details on both transports.
    /// The command boundary reserves exit 4 for verified remote refusals
    /// without earlier whole-command effects.
    ///
    /// A `--set` value travels in the `settings` field of `POST /mutate`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn mutate(
        &self,
        branch: Option<&str>,
        query_source: &str,
        query_name: Option<&str>,
        params_json: Option<&Value>,
        expected_head: Option<&str>,
        settings: &[(SettingId, SettingValue)],
        stderr: &mut dyn Write,
    ) -> Result<ChangeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let route: &[&str] = if expected_head.is_some() {
                    &["mutate", "if-graph-commit"]
                } else {
                    &["mutate"]
                };
                let url = remote_url(base_url, route, &[])?;
                let body = serde_json::to_value(ChangeRequest {
                    query: query_source.to_string(),
                    name: query_name.map(ToOwned::to_owned),
                    params: params_json.cloned(),
                    branch: branch.map(str::to_string),
                    settings: Self::remote_settings(settings)?,
                })?;
                remote_json_bounded(
                    http,
                    Method::POST,
                    url,
                    Some(body),
                    token.as_deref(),
                    expected_head,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let (selected_name, query_params) =
                    select_named_query(parse_query(query_source)?, query_name)?;
                let params = query_params_from_json(&query_params, params_json)?;
                refuse_noncanonical_branches([branch])?;
                let session = Self::open_write_session(uri, settings, stderr).await?;
                let actor = actor.as_deref();
                let receipt = session
                    .mutate_as_with_optional_branch_receipt(
                        branch,
                        query_source,
                        &selected_name,
                        &params,
                        actor,
                        expected_head,
                    )
                    .await
                    .map_err(|err| {
                        let message = err.to_string();
                        match err {
                            omnigraph::error::OmniError::PreconditionFailed {
                                branch: _,
                                expected,
                                actual,
                            } => precondition_failed_cli(message, expected, actual).into(),
                            other => color_eyre::eyre::Report::from(EngineWriteError(other)),
                        }
                    })?;
                Ok(ChangeOutput {
                    branch: receipt.branch,
                    query_name: selected_name,
                    affected_nodes: receipt.result.affected_nodes,
                    affected_edges: receipt.result.affected_edges,
                    actor_id: actor.map(String::from),
                    commit: receipt.commit.as_ref().map(commit_output),
                    outcome: None,
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("mutate")),
        }
    }

    /// A control write statement (`branch create`, `branch delete`, `branch
    /// merge`) from `-e`/`--query`: `POST /mutate` with the source alone, or
    /// the engine call the matching `branch` verb makes, answered as the
    /// server answers it (`branch` received the effect, both counts `0`,
    /// `commit` the merge's own publication). The `--set`
    /// values and the source's `set` lines reach the merge, the one control
    /// write that consults a setting.
    pub(crate) async fn branch_write_statement(
        &self,
        query_source: &str,
        write: BranchWrite,
        settings: &[(SettingId, SettingValue)],
        stderr: &mut dyn Write,
    ) -> Result<ChangeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let mut request = branch_statement_change_request(query_source);
                request.settings = Self::remote_settings(settings)?;
                let output: ChangeOutput = remote_json_bounded(
                    http,
                    Method::POST,
                    remote_url(base_url, &["mutate"], &[])?,
                    Some(serde_json::to_value(request)?),
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await?;
                if let BranchWrite::Merge {
                    source,
                    into,
                    as_commit,
                } = &write
                {
                    let target = into.as_deref().unwrap_or("main");
                    let Some(BranchOutcomeOutput::Merged {
                        source: actual_source,
                        target: actual_target,
                        merge,
                    }) = &output.outcome
                    else {
                        bail!(
                            "invalid merge response: missing merge outcome; effects are unknown; reconcile before retrying"
                        );
                    };
                    if actual_source != source || actual_target != target || output.branch != target
                    {
                        bail!(
                            "invalid merge response: source or target differs from the request; effects are unknown; reconcile before retrying"
                        );
                    }
                    validate_merge_receipt(
                        *merge,
                        output.commit.as_ref(),
                        target,
                        output.actor_id.as_deref(),
                    )?;
                    if output
                        .commit
                        .as_ref()
                        .is_some_and(|commit| commit.published == *as_commit)
                    {
                        bail!(
                            "invalid merge response: the commit's publication state contradicts the request; effects are unknown; reconcile before retrying"
                        );
                    }
                }
                Ok(output)
            }
            GraphClient::Embedded { uri, actor } => {
                let query_name = write.statement_name().to_string();
                let (branch, commit, outcome) = match write {
                    BranchWrite::Create { name, from } => {
                        let start = match from {
                            Some(BranchCreateFrom::Commit(commit)) => {
                                ReadTarget::snapshot(SnapshotId::new(commit))
                            }
                            Some(BranchCreateFrom::Branch(branch)) => ReadTarget::branch(branch),
                            None => ReadTarget::branch("main"),
                        };
                        let created = self.branch_create(start, &name, stderr).await?;
                        (
                            name.clone(),
                            None,
                            BranchOutcomeOutput::Created {
                                from: created.from,
                                name,
                                from_commit_id: created.from_commit_id,
                            },
                        )
                    }
                    BranchWrite::Delete { name } => {
                        self.branch_delete(&name, stderr).await?;
                        (name.clone(), None, BranchOutcomeOutput::Deleted { name })
                    }
                    BranchWrite::Merge {
                        source,
                        into,
                        as_commit,
                    } => {
                        let target = into.unwrap_or_else(|| "main".to_string());
                        let mut session = Self::open_write_session(uri, settings, stderr).await?;
                        Self::apply_prefix(&mut session, query_source)?;
                        let result = session
                            .branch_merge_as_with_options(
                                &source,
                                &target,
                                actor.as_deref(),
                                as_commit,
                            )
                            .await
                            .engine_write()?;
                        let commit = result.commit.as_ref().map(commit_output);
                        (
                            target.clone(),
                            commit,
                            BranchOutcomeOutput::Merged {
                                source,
                                target,
                                merge: result.outcome.into(),
                            },
                        )
                    }
                };
                Ok(ChangeOutput {
                    branch,
                    query_name,
                    affected_nodes: 0,
                    affected_edges: 0,
                    actor_id: actor.clone(),
                    commit,
                    outcome: Some(outcome),
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("branch write statement")),
        }
    }

    /// A commit statement (`commit publish`, `commit drop`) from `-e`/`--query`:
    /// `POST /mutate` with the source alone, or the engine call, answered as
    /// the server answers it (`branch` is the proposal's recorded target, both
    /// counts `0`, `commit` the published commit or null after a drop).
    pub(crate) async fn commit_write_statement(
        &self,
        query_source: &str,
        write: CommitWrite,
        settings: &[(SettingId, SettingValue)],
        stderr: &mut dyn Write,
    ) -> Result<ChangeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let mut request = branch_statement_change_request(query_source);
                request.settings = Self::remote_settings(settings)?;
                let output: ChangeOutput = remote_json_bounded(
                    http,
                    Method::POST,
                    remote_url(base_url, &["mutate"], &[])?,
                    Some(serde_json::to_value(request)?),
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await?;
                let answered = match (&write, &output.outcome, &output.commit) {
                    (
                        CommitWrite::Publish { graph_commit_id },
                        Some(BranchOutcomeOutput::Published),
                        Some(commit),
                    ) => commit.published && commit.graph_commit_id == *graph_commit_id,
                    (
                        CommitWrite::Drop { graph_commit_id },
                        Some(BranchOutcomeOutput::Dropped {
                            graph_commit_id: dropped,
                        }),
                        None,
                    ) => dropped == graph_commit_id,
                    _ => false,
                };
                if !answered {
                    bail!(
                        "invalid `{}` response: the outcome does not describe the requested commit; effects are unknown; reconcile before retrying",
                        write.statement_name()
                    );
                }
                Ok(output)
            }
            GraphClient::Embedded { uri, actor } => {
                let query_name = write.statement_name().to_string();
                let mut session = Self::open_write_session(uri, settings, stderr).await?;
                Self::apply_prefix(&mut session, query_source)?;
                let target = session
                    .get_commit(write.graph_commit_id())
                    .await
                    .engine_write()?
                    .graph_branch
                    .unwrap_or_else(|| "main".to_string());
                let (commit, outcome) = match write {
                    CommitWrite::Publish { graph_commit_id } => {
                        let published = session
                            .publish_commit_as(&graph_commit_id, actor.as_deref())
                            .await
                            .engine_write()?;
                        (
                            Some(commit_output(&published)),
                            BranchOutcomeOutput::Published,
                        )
                    }
                    CommitWrite::Drop { graph_commit_id } => {
                        session
                            .drop_commit_as(&graph_commit_id, actor.as_deref())
                            .await
                            .engine_write()?;
                        (None, BranchOutcomeOutput::Dropped { graph_commit_id })
                    }
                };
                Ok(ChangeOutput {
                    branch: target,
                    query_name,
                    affected_nodes: 0,
                    affected_edges: 0,
                    actor_id: actor.clone(),
                    commit,
                    outcome: Some(outcome),
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("commit write statement")),
        }
    }

    /// A `load csv` statement from `-e`/`--query`: `POST /mutate` with the
    /// source and the request `branch`, or the engine call on the branch
    /// (`main` when unspelled), answered as the server answers it: the
    /// branch that received the load, `load csv` as the name, the loaded
    /// node and edge counts, and the load's own publication.
    pub(crate) async fn load_statement(
        &self,
        branch: Option<&str>,
        query_source: &str,
        stmt: omnigraph_compiler::query::ast::LoadStmt,
        external_blob_policy: Option<omnigraph::ExternalBlobPolicy>,
        settings: &[(SettingId, SettingValue)],
        stderr: &mut dyn Write,
    ) -> Result<ChangeOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let mut request = branch_statement_change_request(query_source);
                request.branch = branch.map(str::to_string);
                request.settings = Self::remote_settings(settings)?;
                remote_json_bounded(
                    http,
                    Method::POST,
                    remote_url(base_url, &["mutate"], &[])?,
                    Some(serde_json::to_value(request)?),
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let query_name = stmt.statement_name().to_string();
                let request = omnigraph::loader::CsvLoadRequest::from_statement(&stmt);
                let mut session = Self::open_write_session_with_external_blob_policy(
                    uri,
                    settings,
                    external_blob_policy,
                    stderr,
                )
                .await?;
                Self::apply_prefix(&mut session, query_source)?;
                let receipt = session
                    .load_csv_as_with_receipt(branch.unwrap_or("main"), &request, actor.as_deref())
                    .await
                    .engine_write()?;
                Ok(ChangeOutput {
                    branch: receipt.result.branch,
                    query_name,
                    affected_nodes: receipt.result.nodes_loaded.values().sum(),
                    affected_edges: receipt.result.edges_loaded.values().sum(),
                    actor_id: actor.clone(),
                    commit: Some(commit_output(&receipt.commit)),
                    outcome: None,
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("load statement")),
        }
    }

    /// `commit publish <id>`: `POST /commits/{id}/publish`, or the engine
    /// call; the answer is the published commit, checked to be the one asked.
    pub(crate) async fn commit_publish(
        &self,
        commit_id: &str,
        stderr: &mut dyn Write,
    ) -> Result<CommitOutput> {
        let published: CommitOutput = match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["commits", commit_id, "publish"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await?
            }
            GraphClient::Embedded { uri, actor } => {
                let session = Self::open_write_session(uri, &[], stderr).await?;
                commit_output(
                    &session
                        .publish_commit_as(commit_id, actor.as_deref())
                        .await
                        .engine_write()?,
                )
            }
            GraphClient::Catalogue => return Err(catalogue_scope_refusal("commit publish")),
        };
        if !published.published || published.graph_commit_id != commit_id {
            bail!(
                "invalid `commit publish` response: the answer does not describe the requested commit as published; effects are unknown; reconcile before retrying"
            );
        }
        Ok(published)
    }

    /// `commit drop <id>`: `DELETE /commits/{id}` (204), or the engine call.
    pub(crate) async fn commit_drop(&self, commit_id: &str, stderr: &mut dyn Write) -> Result<()> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_no_content(
                    http,
                    Method::DELETE,
                    remote_url(base_url, &["commits", commit_id], &[])?,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let session = Self::open_write_session(uri, &[], stderr).await?;
                session
                    .drop_commit_as(commit_id, actor.as_deref())
                    .await
                    .engine_write()
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("commit drop")),
        }
    }

    /// The `branch list` statement from `-e`/`--query`: `POST /query` with
    /// the source and the `--set` values, or the engine's ref list in byte
    /// order, both as the one `ReadOutput` shape (`branch_list_read_output`).
    /// Nothing in `branch list` consults a setting: the remote arm still runs
    /// the scope refusal on the `--set` values, so a `process` one is never
    /// sent; the embedded arm applies none.
    pub(crate) async fn branch_list_statement(
        &self,
        query_source: &str,
        settings: &[(SettingId, SettingValue)],
    ) -> Result<ReadOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let mut request = branch_statement_query_request(query_source);
                request.settings = Self::remote_settings(settings)?;
                remote_json_bounded(
                    http,
                    Method::POST,
                    remote_url(base_url, &["query"], &[])?,
                    Some(serde_json::to_value(request)?),
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { .. } => Ok(branch_list_read_output(
                &self.branch_list().await?.branches,
            )?),
            GraphClient::Catalogue => Err(catalogue_scope_refusal("branch list statement")),
        }
    }

    /// A branch control read from `-e`/`--query` (`branch diff`,
    /// `branch merge-base`, `branch merge … dry run`): `POST /query` with the
    /// source and the `--set` values, or the embedded engine's answer in the
    /// one `ReadOutput` shape the server renders.
    pub(crate) async fn branch_read_statement(
        &self,
        query_source: &str,
        read: BranchRead,
        settings: &[(SettingId, SettingValue)],
    ) -> Result<ReadOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let mut request = branch_statement_query_request(query_source);
                request.settings = Self::remote_settings(settings)?;
                remote_json_bounded(
                    http,
                    Method::POST,
                    remote_url(base_url, &["query"], &[])?,
                    Some(serde_json::to_value(request)?),
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let session = Self::open_read_session(uri, settings).await?;
                Ok(branch_read_output(&session.branch_read(&read).await?)?)
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("branch read statement")),
        }
    }

    /// The `show` statement from `-e`/`--query`: `POST /query` with the
    /// source and the `--set` values, or the embedded session's rows after
    /// the source's prefix, both as the one `ReadOutput` shape
    /// (`show_read_output`).
    pub(crate) async fn show_statement(
        &self,
        query_source: &str,
        id: Option<SettingId>,
        settings: &[(SettingId, SettingValue)],
    ) -> Result<ReadOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let mut request = branch_statement_query_request(query_source);
                request.settings = Self::remote_settings(settings)?;
                remote_json_bounded(
                    http,
                    Method::POST,
                    remote_url(base_url, &["query"], &[])?,
                    Some(serde_json::to_value(request)?),
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let mut session = Self::open_read_session(uri, settings).await?;
                Self::apply_prefix(&mut session, query_source)?;
                Ok(show_read_output(&session.show(id))?)
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("show statement")),
        }
    }

    /// Run a read query with an optional explicit target. An omitted target
    /// lets the selected scope choose its default; only a stored graph has
    /// a default branch. Reads resolve through `resolve()` without policy.
    pub(crate) async fn query(
        &self,
        target: Option<ReadTarget>,
        query_source: &str,
        query_name: Option<&str>,
        params_json: Option<&Value>,
        settings: &[(SettingId, SettingValue)],
    ) -> Result<ReadOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let (branch, snapshot) = match &target {
                    Some(ReadTarget::Branch(branch)) => (Some(branch.clone()), None),
                    Some(ReadTarget::Snapshot(snapshot)) => {
                        (None, Some(snapshot.as_str().to_string()))
                    }
                    None => (None, None),
                };
                remote_json_bounded(
                    http,
                    Method::POST,
                    remote_url(base_url, &["query"], &[])?,
                    Some(serde_json::to_value(QueryRequest {
                        query: query_source.to_string(),
                        name: query_name.map(ToOwned::to_owned),
                        params: params_json.cloned(),
                        branch,
                        snapshot,
                        settings: Self::remote_settings(settings)?,
                    })?),
                    token.as_deref(),
                    None,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let (selected_name, query_params) =
                    select_named_query(parse_query(query_source)?, query_name)?;
                let params = query_params_from_json(&query_params, params_json)?;
                let session = Self::open_read_session(uri, settings).await?;
                let target = target.unwrap_or_else(|| ReadTarget::branch("main"));
                let (result, graph_commit_id) = session
                    .query_with_head(target.clone(), query_source, &selected_name, &params)
                    .await?;
                Ok(read_output(
                    selected_name,
                    &target,
                    result,
                    graph_commit_id,
                )?)
            }
            GraphClient::Catalogue => {
                if target.is_some() {
                    bail!(
                        "`{}` has no branches or snapshots; drop --branch and --snapshot",
                        omnigraph::catalogue::CATALOGUE_SCOPE
                    );
                }
                let (selected_name, query_params) =
                    select_named_query(parse_query(query_source)?, query_name)?;
                let params = query_params_from_json(&query_params, params_json)?;
                let scope = Self::catalogue_scope(settings)?;
                let result = scope.query(query_source, &selected_name, &params).await?;
                let mut output =
                    read_output(selected_name, &ReadTarget::branch("main"), result, None)?;
                output.target = omnigraph_api_types::ReadTargetOutput {
                    branch: None,
                    snapshot: None,
                };
                Ok(output)
            }
        }
    }

    /// `explain load csv …` against `target`: `POST /query` when served; the
    /// embedded arm opens read-only with `external_blob_policy` installed so
    /// the source's metadata probe is authorized as a direct load's would be.
    pub(crate) async fn explain_load(
        &self,
        target: ReadTarget,
        query_source: &str,
        stmt: omnigraph_compiler::query::ast::LoadStmt,
        external_blob_policy: Option<omnigraph::ExternalBlobPolicy>,
        settings: &[(SettingId, SettingValue)],
    ) -> Result<ReadOutput> {
        match self {
            GraphClient::Remote { .. } => {
                self.query(Some(target), query_source, None, None, settings)
                    .await
            }
            GraphClient::Embedded { uri, .. } => {
                let mut session = Self::open_read_session_with_external_blob_policy(
                    uri,
                    settings,
                    external_blob_policy,
                )
                .await?;
                Self::apply_prefix(&mut session, query_source)?;
                let (result, graph_commit_id) = session.explain_load(target.clone(), &stmt).await?;
                Ok(read_output(
                    stmt.statement_name().to_string(),
                    &target,
                    result,
                    graph_commit_id,
                )?)
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("explain load")),
        }
    }

    /// `invoke_named` — run a stored query **by catalog name** (RFC-011 D3).
    /// Served-only: the catalog is server-owned, so a `--store` (embedded)
    /// scope has nothing to resolve the name against. `expect_mutation` carries
    /// the verb's asserted kind; the server rejects a mismatch (400) before
    /// running, so the response is exactly the expected envelope — the caller
    /// deserializes it as the concrete `T` (`ReadOutput` for `query`,
    /// `ChangeOutput` for `mutate`), sidestepping the untagged wire enum.
    pub(crate) async fn invoke_named<T: serde::de::DeserializeOwned>(
        &self,
        name: &str,
        expect_mutation: bool,
        params_json: Option<&Value>,
        branch: Option<String>,
        snapshot: Option<String>,
        expected_head: Option<&str>,
    ) -> Result<T> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let body = InvokeStoredQueryRequest {
                    params: params_json.cloned(),
                    branch,
                    snapshot,
                    expect_mutation: Some(expect_mutation),
                };
                remote_json_bounded(
                    http,
                    Method::POST,
                    if expected_head.is_some() {
                        remote_url(base_url, &["queries", name, "if-graph-commit"], &[])?
                    } else {
                        remote_url(base_url, &["queries", name], &[])?
                    },
                    Some(serde_json::to_value(body)?),
                    token.as_deref(),
                    expected_head,
                    *response_limit,
                )
                .await
            }
            GraphClient::Embedded { .. } => bail!(
                "by-name invocation needs a server (the stored-query catalog is \
                 server-owned); use -e '<gq>' or --query <file> for an ad-hoc query \
                 against --store, or address a server with --server / --profile"
            ),
            GraphClient::Catalogue => Err(catalogue_scope_refusal("invoke named")),
        }
    }

    /// Create `name` at `start`: a branch's head, or a published graph commit.
    pub(crate) async fn branch_create(
        &self,
        start: ReadTarget,
        name: &str,
        stderr: &mut dyn Write,
    ) -> Result<BranchCreateOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["branches"], &[])?,
                    Some(serde_json::to_value(match start {
                        ReadTarget::Branch(branch) => BranchCreateRequest {
                            from: Some(branch),
                            from_commit: None,
                            name: name.to_string(),
                        },
                        ReadTarget::Snapshot(commit) => BranchCreateRequest {
                            from: None,
                            from_commit: Some(commit.as_str().to_string()),
                            name: name.to_string(),
                        },
                    })?),
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_write(uri, stderr).await?;
                let actor = actor.as_deref();
                let created = db
                    .branch_create_from_as(start, name, actor)
                    .await
                    .engine_write()?;
                Ok(BranchCreateOutput {
                    uri: uri.clone(),
                    from: created.from,
                    name: name.to_string(),
                    actor_id: actor.map(String::from),
                    from_commit_id: created.from_commit_id,
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("branch create from")),
        }
    }

    pub(crate) async fn branch_delete(
        &self,
        name: &str,
        stderr: &mut dyn Write,
    ) -> Result<BranchDeleteOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::DELETE,
                    remote_url(base_url, &["branches", name], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_write(uri, stderr).await?;
                let actor = actor.as_deref();
                db.branch_delete_as(name, actor).await.engine_write()?;
                Ok(BranchDeleteOutput {
                    uri: uri.clone(),
                    name: name.to_string(),
                    actor_id: actor.map(String::from),
                })
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("branch delete")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn branch_merge(
        &self,
        source: &str,
        into: &str,
        delete_branch: bool,
        as_commit: bool,
        dry_run: bool,
        settings: &[(SettingId, SettingValue)],
        stderr: &mut dyn Write,
    ) -> Result<BranchMergeOutput> {
        // Dispatch, receipt validation and optional deletion all use the names
        // as given, so refuse one the engine and the server would refuse before
        // any storage or HTTP work: none can then fail after publication.
        refuse_noncanonical_branches([Some(source), Some(into)])?;
        let output = match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::POST,
                    remote_url(base_url, &["branches", "merge"], &[])?,
                    Some(serde_json::to_value(BranchMergeRequest {
                        source: source.to_string(),
                        target: Some(into.to_string()),
                        delete_branch,
                        as_commit,
                        dry_run,
                        settings: Self::remote_settings(settings)?,
                    })?),
                    token.as_deref(),
                )
                .await?
            }
            GraphClient::Embedded { uri, actor } if dry_run => {
                // A read of both branches, composed as the server's dry run.
                let session = Self::open_read_session(uri, settings).await?;
                let result = session.branch_merge_dry_run(source, into).await?;
                BranchMergeOutput {
                    source: source.to_string(),
                    target: into.to_string(),
                    outcome: result.outcome.into(),
                    commit: None,
                    actor_id: actor.clone(),
                    branch_deleted: None,
                    branch_delete_error_details: None,
                    dry_run: true,
                }
            }
            GraphClient::Embedded { uri, actor } => {
                let session = Self::open_write_session(uri, settings, stderr).await?;
                let actor = actor.as_deref();
                let result = session
                    .branch_merge_as_with_options(source, into, actor, as_commit)
                    .await
                    .engine_write()?;
                // Composed exactly like the server handler: the merge is
                // durable, so a deletion refusal/failure is reported in the
                // payload, never as an error (parity_matrix pins the two
                // composition sites against drift).
                let (branch_deleted, branch_delete_error_details) = if delete_branch {
                    match session.branch_delete_as(source, actor).await {
                        Ok(()) => (Some(true), None),
                        Err(err) => (
                            Some(false),
                            Some(omnigraph_server::engine_error_output(err)),
                        ),
                    }
                } else {
                    (None, None)
                };
                BranchMergeOutput {
                    source: source.to_string(),
                    target: into.to_string(),
                    outcome: result.outcome.into(),
                    commit: result.commit.as_ref().map(commit_output),
                    actor_id: actor.map(String::from),
                    branch_deleted,
                    branch_delete_error_details,
                    dry_run: false,
                }
            }
            GraphClient::Catalogue => return Err(catalogue_scope_refusal("branch merge")),
        };
        validate_merge_output(&output, source, into, delete_branch, as_commit, dry_run)?;
        Ok(output)
    }

    /// Fetch one bounded page of a graph diff. Auto-pagination is owned by the
    /// command output loop, as for `commit_changes_page`.
    pub(crate) async fn diff_page(
        &self,
        sides: DiffSidesArg<'_>,
        page_token: Option<&str>,
        limit: Option<usize>,
        filter: &ChangeFilterArgs<'_>,
    ) -> Result<DiffOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                let limit_value = limit.map(|limit| limit.to_string());
                let (segments, sides): (&[&str], [(&str, &str); 2]) = match sides {
                    DiffSidesArg::Between { from, to } => (&["diff"], [("from", from), ("to", to)]),
                    DiffSidesArg::Branch { source, target } => (
                        &["branches", "diff"],
                        [("source", source), ("target", target)],
                    ),
                };
                let mut query: Vec<(&str, &str)> = sides.to_vec();
                if let Some(page_token) = page_token {
                    query.push(("page_token", page_token));
                }
                if let Some(limit) = limit_value.as_deref() {
                    query.push(("limit", limit));
                }
                let filter_pairs = filter.query_pairs();
                query.extend(
                    filter_pairs
                        .iter()
                        .map(|(name, value)| (*name, value.as_str())),
                );
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, segments, &query)?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let session = Self::open_read_session(uri, &[]).await?;
                let sides = match sides {
                    DiffSidesArg::Between { from, to } => {
                        omnigraph::changes::GraphDiffSides::Between {
                            from: from.to_string(),
                            to: to.to_string(),
                        }
                    }
                    DiffSidesArg::Branch { source, target } => {
                        omnigraph::changes::GraphDiffSides::Branch {
                            source: source.to_string(),
                            target: target.to_string(),
                        }
                    }
                };
                let page = session
                    .diff_page(&omnigraph::changes::GraphDiffRequest {
                        sides,
                        scope: change_scope(filter.kinds, filter.types, filter.ops),
                        page_token: page_token.map(str::to_string),
                        max_changes: limit,
                        max_bytes: None,
                    })
                    .await?;
                Ok(diff_output(&page))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("diff page")),
        }
    }

    /// The merge base of `source` and `target`, with the heads it was
    /// computed from.
    pub(crate) async fn merge_base(&self, source: &str, target: &str) -> Result<MergeBaseOutput> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(
                        base_url,
                        &["branches", "merge-base"],
                        &[("source", source), ("target", target)],
                    )?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { uri, .. } => {
                let session = Self::open_read_session(uri, &[]).await?;
                let base = session.merge_base(source, target).await?;
                Ok(merge_base_output(source, target, &base))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("merge base")),
        }
    }

    /// Apply a standalone graph's schema with the caller's catalog validator.
    /// Served schemas are deployed through the cluster configuration.
    pub(crate) async fn apply_schema<F>(
        &self,
        schema_source: &str,
        validate: F,
        stderr: &mut dyn Write,
    ) -> Result<SchemaApplyOutput>
    where
        F: FnOnce(&Catalog) -> omnigraph::error::Result<()>,
    {
        match self {
            GraphClient::Remote { .. } => bail!(
                "schema apply requires a standalone storage URI; deploy a served schema with \
                 `cluster apply --server <SERVER> --config <CONFIG>`"
            ),
            GraphClient::Embedded { uri, actor } => {
                let db = Self::open_write(uri, stderr).await?;
                let result = db
                    .apply_schema_as_with_catalog_check(schema_source, actor.as_deref(), validate)
                    .await
                    .engine_write()?;
                Ok(schema_apply_output(uri, result))
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("apply schema")),
        }
    }

    /// `export` — stream the branch as JSONL into `writer`. The streaming
    /// shape (a `W: Write`, not a returned DTO) is why this lands in 3c
    /// rather than 3b. Opens WITHOUT policy (like reads), so it is reached
    /// via `resolve()`; the Embedded arm opens bare. The Remote arm streams
    /// the chunked response body straight through (no buffering the whole
    /// export in memory).
    pub(crate) async fn export<W: Write>(
        &self,
        branch: &str,
        type_names: &[String],
        writer: &mut W,
    ) -> Result<()> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                let request = apply_bearer_token(
                    http.request(Method::POST, remote_url(base_url, &["export"], &[])?)?,
                    token.as_deref(),
                )
                .json(&ExportRequest {
                    branch: Some(branch.to_string()),
                    type_names: type_names.to_vec(),
                });
                let mut response = http.send(request).await?;
                let status = response.status();
                if !status.is_success() {
                    // Share structured status/backoff handling with JSON responses.
                    return remote_response_json_bounded(response, token.as_deref(), None).await;
                }
                while let Some(chunk) = response.chunk().await? {
                    writer.write_all(&chunk)?;
                }
                writer.flush()?;
                Ok(())
            }
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                db.export_jsonl_to_writer(branch, type_names, writer)
                    .await?;
                writer.flush()?;
                Ok(())
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("export")),
        }
    }

    /// Stream one managed Blob without buffering the whole value. External
    /// descriptors are reported but never followed or dereferenced.
    pub(crate) async fn blob_get<W: Write + ?Sized>(
        &self,
        query: &BlobReadQuery,
        range: Option<BlobRangeRequest>,
        writer: &mut W,
    ) -> Result<()> {
        match self {
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                let read = db
                    .read_blob_at(blob_read_target(query), blob_cell(query))
                    .await
                    .map_err(map_embedded_blob_error)?;
                match read.content {
                    BlobContent::Managed { length, reader, .. } => {
                        let selected = match range {
                            Some(range) => range.resolve(length)?,
                            None => 0..length,
                        };
                        let mut cursor = selected.start;
                        while cursor < selected.end {
                            let end = selected
                                .end
                                .min(cursor.saturating_add(BLOB_READ_RANGE_MAX_BYTES));
                            let bytes = reader
                                .read_range(cursor..end)
                                .await
                                .map_err(map_embedded_blob_error)?;
                            let expected = usize::try_from(end - cursor)
                                .map_err(|_| color_eyre::eyre::eyre!("Blob delivery failed"))?;
                            if bytes.len() != expected {
                                bail!("Blob delivery failed: managed range length mismatch");
                            }
                            writer.write_all(&bytes)?;
                            cursor = end;
                        }
                        writer.flush()?;
                        Ok(())
                    }
                    BlobContent::External(reference) => {
                        let uri = whole_external_uri(&reference)?;
                        bail!(
                            "external Blob is not downloaded; URI: {uri}; use `blob stat --json` \
                             to inspect the descriptor"
                        )
                    }
                }
            }
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                let http = http.blob_delivery()?;
                let mut request = apply_bearer_token(
                    http.request(Method::GET, blob_url(base_url, query)?)?,
                    token.as_deref(),
                );
                if let Some(range) = range {
                    request = request.header(RANGE, range.header_value());
                }
                let mut response = http.send(request).await.map_err(blob_transport_error)?;
                let status = response.status();
                if status == StatusCode::FOUND {
                    let (uri, _snapshot_id) = external_response_headers(response.headers())?;
                    bail!(
                        "external Blob is not downloaded; URI: {uri}; use `blob stat --json` \
                         to inspect the descriptor"
                    );
                }
                let expected_status = if range.is_some() {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                };
                if status != expected_status {
                    return Err(remote_blob_error(status, missing_credential(&response)));
                }
                let headers = managed_response_headers(response.headers())?;
                if let Some(range) = range {
                    validate_content_range(response.headers(), range, headers.length)?;
                }
                let mut written = 0_u64;
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| color_eyre::eyre::eyre!("Blob delivery stream failed"))?
                {
                    writer.write_all(&chunk)?;
                    written = written
                        .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
                        .ok_or_else(|| color_eyre::eyre::eyre!("Blob delivery failed"))?;
                }
                if written != headers.length {
                    bail!(
                        "Blob delivery stream ended after {written} bytes; expected {}",
                        headers.length
                    );
                }
                writer.flush()?;
                Ok(())
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("blob get")),
        }
    }

    /// Inspect one Blob descriptor. Neither arm reads managed payload bytes;
    /// external references are classified without probing their target.
    pub(crate) async fn blob_stat(&self, query: &BlobReadQuery) -> Result<BlobStatOutput> {
        match self {
            GraphClient::Embedded { uri, .. } => {
                let db = Self::open_read_session(uri, &[]).await?;
                let read = db
                    .read_blob_at(blob_read_target(query), blob_cell(query))
                    .await
                    .map_err(map_embedded_blob_error)?;
                let resolved_snapshot = read.resolved_target.snapshot_id.to_string();
                match read.content {
                    BlobContent::Managed { length, etag, .. } => Ok(BlobStatOutput::managed(
                        query,
                        resolved_snapshot,
                        length,
                        etag.into_string(),
                    )),
                    BlobContent::External(reference) => Ok(BlobStatOutput::external(
                        query,
                        resolved_snapshot,
                        whole_external_uri(&reference)?.to_string(),
                    )),
                }
            }
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                let http = http.blob_delivery()?;
                let request = apply_bearer_token(
                    http.request(Method::HEAD, blob_url(base_url, query)?)?,
                    token.as_deref(),
                );
                let response = http.send(request).await.map_err(blob_transport_error)?;
                match response.status() {
                    StatusCode::OK => {
                        let headers = managed_response_headers(response.headers())?;
                        Ok(BlobStatOutput::managed(
                            query,
                            headers.snapshot_id,
                            headers.length,
                            headers.etag,
                        ))
                    }
                    StatusCode::FOUND => {
                        let (uri, snapshot_id) = external_response_headers(response.headers())?;
                        Ok(BlobStatOutput::external(query, snapshot_id, uri))
                    }
                    status => Err(remote_blob_error(status, missing_credential(&response))),
                }
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("blob stat")),
        }
    }

    /// `blob put` (with `bytes`) or `blob clear` (without): one Blob cell
    /// write, sent once. The embedded arm calls the engine with the parsed
    /// precondition; the served arm sends `if_match` as the `If-Match` header
    /// the server parses the same way. Both answer the same receipt.
    pub(crate) async fn blob_write(
        &self,
        query: &BlobWriteQuery,
        bytes: Option<bytes::Bytes>,
        if_match: Option<&str>,
        precondition: Option<omnigraph::BlobPrecondition>,
        stderr: &mut dyn Write,
    ) -> Result<BlobWriteOutput> {
        let branch = query.branch.as_deref().unwrap_or("main");
        match self {
            GraphClient::Embedded { uri, actor } => {
                let session = Self::open_write_session(uri, &[], stderr).await?;
                let cell = blob_write_cell(query);
                let outcome = match bytes {
                    Some(bytes) => {
                        session
                            .put_blob_at_as(branch, cell, bytes, precondition, actor.as_deref())
                            .await
                    }
                    None => {
                        session
                            .clear_blob_at_as(branch, cell, precondition, actor.as_deref())
                            .await
                    }
                }
                .map_err(|error| match error {
                    // The body a server answers, so `--json` is transport-uniform.
                    error @ omnigraph::error::OmniError::BlobWritePreconditionFailed { .. } => {
                        PreconditionFailedCli {
                            output: omnigraph_server::engine_error_output(error),
                            http_status: None,
                            retry_after: None,
                        }
                        .into()
                    }
                    other => color_eyre::eyre::Report::from(EngineWriteError(other)),
                })?;
                Ok(omnigraph_api_types::blob_write_output(
                    query.into(),
                    branch,
                    &outcome,
                    actor.clone(),
                ))
            }
            GraphClient::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                let url = blob_write_url(base_url, query)?;
                let request = match bytes {
                    Some(bytes) => http
                        .request(Method::PUT, url)?
                        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                        .body(bytes),
                    None => http.request(Method::DELETE, url)?,
                };
                let request = apply_bearer_token(request, token.as_deref());
                let request = match if_match {
                    Some(tags) => request.header(reqwest::header::IF_MATCH, tags),
                    None => request,
                };
                let response = http.send(request).await?;
                remote_response_json_bounded(response, token.as_deref(), *response_limit)
                    .await
                    .map_err(verified_blob_precondition)
            }
            GraphClient::Catalogue => Err(catalogue_scope_refusal("blob write")),
        }
    }

    /// `graphs list` — enumerate the graphs a multi-graph server serves
    /// (`GET /graphs`). Reached only through registry-addressed clients
    /// (`resolve_registry` / the D7 probe's `registry_client`), which always
    /// build the Remote variant — the Embedded arm is unreachable by
    /// construction and kept as a defensive internal-invariant bail.
    pub(crate) async fn list_graphs(&self) -> Result<GraphListResponse> {
        match self {
            GraphClient::Remote {
                http,
                base_url,
                token,
                ..
            } => {
                remote_json(
                    http,
                    Method::GET,
                    remote_url(base_url, &["graphs"], &[])?,
                    None,
                    token.as_deref(),
                )
                .await
            }
            GraphClient::Embedded { .. } => bail!(
                "internal error: `graphs list` reached an embedded client — registry \
                 addressing always resolves a server"
            ),
            GraphClient::Catalogue => Err(catalogue_scope_refusal("list graphs")),
        }
    }

    /// Minimal existence inventory. No fallback to the metadata-bearing catalog.
    pub(crate) async fn discover_graphs(&self) -> Result<GraphDiscoveryResponse> {
        match self {
            Self::Remote {
                http,
                base_url,
                token,
                response_limit,
                ..
            } => {
                remote_json_bounded(
                    http,
                    Method::GET,
                    remote_url(base_url, &["graphs", "discovery"], &[])?,
                    None,
                    token.as_deref(),
                    None,
                    response_limit.or(Some(8 * 1024 * 1024)),
                )
                .await
            }
            Self::Embedded { .. } => bail!("graph discovery requires a server"),
            GraphClient::Catalogue => Err(catalogue_scope_refusal("discover graphs")),
        }
    }
}

fn validate_content_range(
    headers: &reqwest::header::HeaderMap,
    requested: BlobRangeRequest,
    served_length: u64,
) -> Result<()> {
    let raw = headers
        .get(CONTENT_RANGE)
        .ok_or_else(|| color_eyre::eyre::eyre!("Blob server response omitted Content-Range"))?
        .to_str()
        .map_err(|_| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let Some(spec) = raw.strip_prefix("bytes ") else {
        bail!("Blob server returned an invalid Content-Range");
    };
    let Some((bounds, total)) = spec.split_once('/') else {
        bail!("Blob server returned an invalid Content-Range");
    };
    let Some((start, end)) = bounds.split_once('-') else {
        bail!("Blob server returned an invalid Content-Range");
    };
    let start = start
        .parse::<u64>()
        .map_err(|_| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let end = end
        .parse::<u64>()
        .map_err(|_| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let total = total
        .parse::<u64>()
        .map_err(|_| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let actual = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| color_eyre::eyre::eyre!("Blob server returned an invalid Content-Range"))?;
    let available = total.checked_sub(requested.start()).ok_or_else(|| {
        color_eyre::eyre::eyre!("Blob server returned an inconsistent Content-Range")
    })?;
    let expected = requested
        .requested_length()
        .map_or(available, |length| length.min(available));
    if start != requested.start() || actual != served_length || actual != expected || end >= total {
        bail!("Blob server returned an inconsistent Content-Range");
    }
    Ok(())
}

/// A served Blob write's 412 whose only detail is `blob_precondition_failure`
/// is a verified request precondition, treated as a graph-commit one is: the
/// whole command exits 4 when it was the command's only effectful request.
fn verified_blob_precondition(error: color_eyre::eyre::Report) -> color_eyre::eyre::Report {
    match error.downcast::<crate::helpers::RemoteErrorCli>() {
        Ok(remote)
            if remote.status == StatusCode::PRECONDITION_FAILED
                && remote.output.blob_precondition_failure.is_some()
                && remote.output.code
                    == Some(omnigraph_api_types::ErrorCode::PreconditionFailed)
                && crate::command_outcome::single_request()
                && {
                    let mut rest = remote.output.clone();
                    rest.blob_precondition_failure = None;
                    crate::command_outcome::is_plain_refusal(&rest)
                } =>
        {
            PreconditionFailedCli {
                output: remote.output,
                http_status: Some(StatusCode::PRECONDITION_FAILED.as_u16()),
                retry_after: remote.retry_after,
            }
            .into()
        }
        Ok(remote) => remote.into(),
        Err(error) => error,
    }
}

/// The two sides of a graph diff, as the command spelled them.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DiffSidesArg<'a> {
    /// Two read targets: branches or commit ids.
    Between { from: &'a str, to: &'a str },
    /// A source branch against its merge base with a target branch.
    Branch { source: &'a str, target: &'a str },
}

/// Shared spelling of the change-surface filters across the three verbs; one
/// translation (`change_scope`) is used by the embedded arm and the server.
pub(crate) struct ChangeFilterArgs<'a> {
    pub kinds: &'a [EntityKindOutput],
    pub types: &'a [String],
    pub ops: &'a [ChangeOpOutput],
}

impl ChangeFilterArgs<'_> {
    fn query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        for kind in self.kinds {
            pairs.push(("kind", kind.as_str().to_string()));
        }
        for type_name in self.types {
            pairs.push(("type", type_name.clone()));
        }
        for op in self.ops {
            pairs.push(("op", op.as_str().to_string()));
        }
        pairs
    }
}

/// The embedded arm's twin of the served start-mode parser.
fn parse_change_feed_start(start: &str) -> Result<omnigraph::changes::ChangeFeedStart> {
    match start {
        "now" => Ok(omnigraph::changes::ChangeFeedStart::Now),
        "beginning" => Ok(omnigraph::changes::ChangeFeedStart::Beginning),
        other => other
            .strip_prefix("after:")
            .filter(|commit_id| !commit_id.is_empty())
            .map(|commit_id| {
                omnigraph::changes::ChangeFeedStart::AfterCommit(commit_id.to_string())
            })
            .ok_or_else(|| eyre!("start must be now | beginning | after:<commit_id>")),
    }
}

/// Refuse a branch name the engine refuses, with the engine's own error,
/// before any storage open or request: the refusal then proves that nothing
/// ran.
fn refuse_noncanonical_branches<'a>(
    names: impl IntoIterator<Item = Option<&'a str>>,
) -> Result<()> {
    for name in names.into_iter().flatten() {
        omnigraph_policy::ensure_canonical_branch_name(name)
            .map_err(|error| omnigraph::error::OmniError::manifest(error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{RemoteErrorCli, remote_json_with_graph_commit_precondition};
    use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
    use serde_json::json;

    fn contract_reply(status: u16, body: Value) -> IntentReply {
        let mut reply = IntentReply::json(status, body);
        reply.headers.push((
            omnigraph_api_types::HTTP_API_CONTRACT_HEADER.into(),
            omnigraph_api_types::HTTP_API_CONTRACT.into(),
        ));
        reply
    }

    #[tokio::test]
    async fn graph_http_discovery_refuses_before_data_dispatch() {
        use omnigraph_api_types::{
            HTTP_API_CONTRACT as CONTRACT, HTTP_API_CONTRACT_HEADER as HEADER,
        };

        let target = IntentApiFixture::new(vec![]);
        let cases: [(u16, Vec<(String, String)>); 9] = [
            (200, vec![]),
            (200, vec![(HEADER.into(), "0.11".into())]),
            (200, vec![(HEADER.into(), "0.12".into())]),
            (200, vec![(HEADER.into(), "0.13".into())]),
            (
                200,
                vec![(HEADER.into(), format!("{CONTRACT}, {CONTRACT}"))],
            ),
            (
                200,
                vec![
                    (HEADER.into(), CONTRACT.into()),
                    (HEADER.into(), CONTRACT.into()),
                ],
            ),
            (401, vec![(HEADER.into(), CONTRACT.into())]),
            (503, vec![(HEADER.into(), CONTRACT.into())]),
            (
                302,
                vec![
                    (HEADER.into(), CONTRACT.into()),
                    ("Location".into(), target.origin.clone()),
                ],
            ),
        ];
        for managed in [false, true] {
            let server = IntentApiFixture::new(
                cases
                    .iter()
                    .map(|(status, headers)| IntentReply {
                        status: *status,
                        headers: headers.clone(),
                        body: b"secret untrusted body".to_vec(),
                    })
                    .collect(),
            );
            let mut endpoint = url::Url::parse(&server.origin).unwrap();
            endpoint.set_username("secret-user").unwrap();
            endpoint.set_password(Some("secret-password")).unwrap();
            let http = if managed {
                GraphHttpClient::managed(endpoint.as_str())
            } else {
                GraphHttpClient::new(endpoint.as_str())
            }
            .unwrap();
            for (i, (status, _)) in cases.iter().enumerate() {
                let error = remote_json::<Value>(
                    &http,
                    Method::POST,
                    format!("{}/graphs/knowledge/mutate", server.origin),
                    Some(json!({"query":"mutation m() {}"})),
                    Some("secret-bearer"),
                )
                .await
                .unwrap_err();
                let contract = error.downcast_ref::<ApiContractError>().unwrap();
                assert!(!contract.request_dispatched);
                assert_eq!(contract.http_status, Some(*status));
                assert!(!format!("{error:?}").contains("secret"));
                let requests = server.requests();
                assert_eq!(requests.len(), i + 1);
                assert_eq!(requests[i].method, "HEAD");
                assert_eq!(requests[i].path, "/healthz");
                assert!(!requests[i].headers.contains_key("authorization"));
            }
            server.assert_complete();
        }
        assert!(
            target.requests().is_empty(),
            "discovery never follows redirects"
        );
        let server = IntentApiFixture::new(vec![IntentReply::json(200, json!(null))]);
        let scope = crate::scope::ResolvedScope {
            server: Some(server.origin.clone()),
            ..Default::default()
        };
        let input = InvocationConfig::at(None, None);
        let probe =
            GraphClient::registry_client(&input, &OperatorConfig::default(), &server.origin)
                .unwrap();
        let error = require_graph_for_multi_graph_server(&probe, &scope)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<ApiContractError>().is_some());
        server.assert_complete();
    }

    #[tokio::test]
    async fn graph_http_discovery_keeps_proxy_prefix_and_probes_every_request() {
        let server = IntentApiFixture::new(vec![
            contract_reply(200, json!(null)),
            contract_reply(200, json!({"one": 1})),
            contract_reply(204, json!(null)),
            contract_reply(200, json!({"two": 2})),
        ]);
        let endpoint = format!("{}/proxy/graphs/front/", server.origin);
        let client = GraphClient::managed(&endpoint, "knowledge", "data-bearer".into()).unwrap();
        for expected in [json!({"one":1}), json!({"two":2})] {
            let value: Value = client
                .invoke_named("read", false, None, None, None, None)
                .await
                .unwrap();
            assert_eq!(value, expected);
        }
        let requests = server.requests();
        assert_eq!(requests.len(), 4);
        for pair in requests.chunks_exact(2) {
            assert_eq!(pair[0].method, "HEAD");
            assert_eq!(pair[0].path, "/proxy/graphs/front/healthz");
            assert!(!pair[0].headers.contains_key("authorization"));
            assert_eq!(
                pair[1].path,
                "/proxy/graphs/front/graphs/knowledge/queries/read"
            );
            assert_eq!(pair[1].headers["authorization"], "Bearer data-bearer");
            assert_eq!(
                pair[1].headers[omnigraph_api_types::HTTP_API_CONTRACT_HEADER],
                omnigraph_api_types::HTTP_API_CONTRACT
            );
        }
        server.assert_complete();
    }

    #[tokio::test]
    #[ignore = "nightly: waits out the real 5 s discovery bound"]
    async fn graph_http_discovery_has_its_own_five_second_bound() {
        let server = IntentApiFixture::with_response_delay(
            vec![contract_reply(200, json!(null))],
            std::time::Duration::from_millis(5_250),
        );
        let http = GraphHttpClient::new(&server.origin).unwrap();
        let started = std::time::Instant::now();
        let error = remote_json::<Value>(
            &http,
            Method::GET,
            format!("{}/graphs", server.origin),
            None,
            Some("secret"),
        )
        .await
        .unwrap_err();
        assert!(started.elapsed() >= std::time::Duration::from_secs(5));
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        let contract = error.downcast_ref::<ApiContractError>().unwrap();
        assert!(!contract.request_dispatched);
        assert_eq!(contract.http_status, None);
        assert!(contract.error.contains("timed out"), "{error:?}");
        server.assert_complete();
    }

    #[tokio::test]
    async fn graph_http_discovery_preserves_connection_failure_without_credentials() {
        let server = IntentApiFixture::new(vec![]);
        let mut endpoint = url::Url::parse(&server.origin).unwrap();
        endpoint.set_username("secret-user").unwrap();
        endpoint.set_password(Some("secret-password")).unwrap();
        endpoint.set_query(Some("api_key=secret-query"));
        drop(server);

        let http = GraphHttpClient::new(endpoint.as_str()).unwrap();
        let error = remote_json::<Value>(
            &http,
            Method::POST,
            remote_url(endpoint.as_str(), &["graphs", "knowledge", "change"], &[]).unwrap(),
            Some(json!({})),
            Some("secret-bearer"),
        )
        .await
        .unwrap_err();
        let contract = error.downcast_ref::<ApiContractError>().unwrap();
        assert!(!contract.request_dispatched);
        assert_eq!(contract.http_status, None);
        assert!(contract.error.contains("connection failed"), "{error:?}");
        assert!(
            contract
                .error
                .contains(omnigraph_api_types::HTTP_API_CONTRACT_HEADER)
        );
        assert!(
            contract
                .error
                .contains(omnigraph_api_types::HTTP_API_CONTRACT)
        );
        assert!(!format!("{error:?}").contains("secret"));
        assert!(!serde_json::to_string(contract).unwrap().contains("secret"));
    }

    /// A `unix://` root's whole path names the socket: the server receives
    /// only the HTTP target after the root, with the same discovery, bearer
    /// and contract rules as over TCP. A URL outside the root is never sent.
    #[cfg(unix)]
    #[tokio::test]
    async fn graph_http_over_a_unix_socket_sends_root_relative_targets_issue_979() {
        let server = IntentApiFixture::graph(vec![contract_reply(200, json!({"ok": true}))]);
        let gateway = crate::unix_gateway_fixture::UnixGateway::new(&server.origin);
        let root = gateway.url();
        let http = GraphHttpClient::new(&root).unwrap();
        let base = resolve_server_flag_with_config(
            &OperatorConfig::default(),
            Some(&root),
            Some("knowledge"),
        )
        .unwrap()
        .unwrap();
        let value: Value = remote_json(
            &http,
            Method::GET,
            remote_url(&base, &["branches", "etl/run 1"], &[("branch", "a b")]).unwrap(),
            None,
            Some("data-bearer"),
        )
        .await
        .unwrap();
        assert_eq!(value, json!({"ok": true}));
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, "HEAD");
        assert_eq!(requests[0].path, "/healthz");
        assert!(!requests[0].headers.contains_key("authorization"));
        assert_eq!(
            requests[1].path,
            "/graphs/knowledge/branches/etl%2Frun%201?branch=a+b"
        );
        assert_eq!(requests[1].headers["host"], "localhost");
        assert_eq!(requests[1].headers["authorization"], "Bearer data-bearer");
        assert_eq!(
            requests[1].headers[omnigraph_api_types::HTTP_API_CONTRACT_HEADER],
            omnigraph_api_types::HTTP_API_CONTRACT
        );
        assert_eq!(gateway.connections(), 2, "both requests used the socket");
        server.assert_complete();

        // `--graph` refuses anything but one plain segment before any URL is
        // composed; the transport still refuses a composed URL that climbs
        // out of the root.
        let refusal =
            resolve_server_flag_with_config(&OperatorConfig::default(), Some(&root), Some("../.."))
                .unwrap_err();
        assert!(
            refusal
                .to_string()
                .contains("a graph name is one path segment"),
            "{refusal}"
        );
        for outside in [
            format!("{root}2/graphs"),
            remote_url(&format!("{root}/graphs/../.."), &["snapshot"], &[]).unwrap(),
        ] {
            let error = http.request(Method::GET, outside.clone()).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("outside the Unix-socket server root"),
                "{outside}: {error}"
            );
        }
        assert_eq!(server.requests().len(), 2);

        // Removing the socket fails discovery before the data request.
        drop(gateway);
        let error = remote_json::<Value>(
            &http,
            Method::GET,
            remote_url(&base, &["snapshot"], &[]).unwrap(),
            None,
            Some("data-bearer"),
        )
        .await
        .unwrap_err();
        let contract = error.downcast_ref::<ApiContractError>().unwrap();
        assert!(!contract.request_dispatched);
        assert!(
            contract.error.contains("connection failed (Unix socket)"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn graph_http_checks_json_ndjson_and_streams_before_exposing_any_body() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "{}\n").unwrap();
        let blob = BlobReadQuery {
            entity: omnigraph_api_types::BlobEntityKind::Node,
            r#type: "Document".into(),
            id: "document-one".into(),
            property: "data".into(),
            branch: None,
            snapshot: None,
        };
        let forms = [
            "json",
            "ndjson",
            "baseline",
            "export",
            "blob-get",
            "blob-head",
        ];
        let mut replies = Vec::new();
        for _ in forms {
            for dispatched in [false, true] {
                if dispatched {
                    replies.push(contract_reply(200, json!(null)));
                }
                replies.push(IntentReply {
                    status: 200,
                    headers: vec![],
                    body: b"untrusted body must not escape\n".to_vec(),
                });
            }
        }
        let server = IntentApiFixture::new(replies);
        let client =
            GraphClient::managed(&server.origin, "knowledge", "data-bearer".into()).unwrap();
        let mut served = 0;
        for form in forms {
            for dispatched in [false, true] {
                let mut output = Vec::new();
                let result = match form {
                    "json" => client
                        .invoke_named::<Value>("write", true, None, None, None, None)
                        .await
                        .map(|_| ()),
                    "ndjson" => client
                        .load(
                            "main",
                            None,
                            file.path().to_str().unwrap(),
                            CliLoadMode::Append,
                            &[],
                            &mut std::io::sink(),
                        )
                        .await
                        .map(|_| ()),
                    "baseline" => client
                        .change_baseline(
                            None,
                            &ChangeFilterArgs {
                                kinds: &[],
                                types: &[],
                                ops: &[],
                            },
                            &mut output,
                        )
                        .await
                        .map(|_| ()),
                    "export" => client.export("main", &[], &mut output).await,
                    "blob-get" => client.blob_get(&blob, None, &mut output).await,
                    "blob-head" => client.blob_stat(&blob).await.map(|_| ()),
                    _ => unreachable!(),
                };
                let error = result.unwrap_err();
                let contract = error
                    .downcast_ref::<ApiContractError>()
                    .unwrap_or_else(|| panic!("{form}: {error:?}"));
                assert_eq!(contract.request_dispatched, dispatched, "{form}");
                assert_eq!(contract.http_status, Some(200));
                assert!(
                    output.is_empty(),
                    "{form} must validate before writing any bytes"
                );
                served += if dispatched { 2 } else { 1 };
                assert_eq!(server.requests().len(), served, "{form}");
            }
        }
        server.assert_complete();
    }

    #[tokio::test]
    async fn graph_http_preserves_data_errors_without_following_redirects() {
        // These statuses exercise response forwarding and redirect refusal.
        // They are not reqwest's retryable HTTP/2 or HTTP/3 transport faults.
        let target = IntentApiFixture::new(vec![]);
        for status in [302, 429, 503] {
            let mut reply = contract_reply(status, json!({"error":"stop"}));
            reply
                .headers
                .push(("Location".into(), target.origin.clone()));
            let server = IntentApiFixture::graph(vec![reply]);
            let http = GraphHttpClient::new(&server.origin).unwrap();
            let error = remote_json::<Value>(
                &http,
                Method::POST,
                format!("{}/graphs/knowledge/mutate", server.origin),
                Some(json!({})),
                Some("data-bearer"),
            )
            .await
            .unwrap_err();
            assert!(error.downcast_ref::<RemoteErrorCli>().is_some());
            assert_eq!(
                server.requests().len(),
                2,
                "one discovery and one data request"
            );
            server.assert_complete();
        }
        assert!(target.requests().is_empty());
    }

    #[tokio::test]
    async fn command_outcome_keeps_earlier_effects_and_scopes_independent_invocations() {
        #[derive(Clone, Copy)]
        enum Refusal {
            Busy,
            Condition,
            Denied,
        }
        async fn attempt(
            earlier_request: bool,
            read_refusal: bool,
            kind: Refusal,
        ) -> crate::command_outcome::Failure {
            let conditional = matches!(kind, Refusal::Condition);
            let mut refusal = match kind {
                Refusal::Busy => contract_reply(
                    429,
                    json!({"error":"actor is busy", "code":"too_many_requests"}),
                ),
                Refusal::Condition => contract_reply(
                    412,
                    json!({"error":"head changed", "precondition_failure":{"expected":"head-a","actual":"head-b"}}),
                ),
                Refusal::Denied => contract_reply(
                    403,
                    json!({"error":"change denied by current policy", "code":"forbidden"}),
                ),
            };
            refusal
                .headers
                .push(("Retry-After".into(), "Wed, 21 Oct 2026 07:28:00 GMT".into()));
            let replies = if earlier_request {
                vec![contract_reply(200, json!({"created":true})), refusal]
            } else {
                vec![refusal]
            };
            let server = IntentApiFixture::graph(replies);
            let http = GraphHttpClient::new(&server.origin).unwrap();
            let (error, evidence) = crate::command_outcome::observe(
                crate::command_outcome::CommandKind::Write,
                async {
                    if earlier_request {
                        remote_json::<Value>(
                            &http,
                            Method::POST,
                            format!("{}/graphs/knowledge/branches", server.origin),
                            Some(json!({"name":"review"})),
                            None,
                        )
                        .await
                        .unwrap();
                    }
                    remote_json_with_graph_commit_precondition::<Value>(
                        &http,
                        if read_refusal {
                            Method::GET
                        } else {
                            Method::POST
                        },
                        format!("{}/graphs/knowledge/mutate", server.origin),
                        Some(json!({"query":"mutation m() {}"})),
                        None,
                        conditional.then_some("head-a"),
                    )
                    .await
                    .unwrap_err()
                },
            )
            .await;
            assert_eq!(
                server.workflow_requests().len(),
                if earlier_request { 2 } else { 1 }
            );
            server.assert_complete();
            crate::command_outcome::Failure::classify(error, evidence)
        }
        // Poll concurrently: invocation evidence must never leak to a peer.
        let (compound, single) = tokio::join!(
            attempt(true, false, Refusal::Busy),
            attempt(false, false, Refusal::Busy)
        );
        assert_eq!(compound.exit, 2);
        assert_eq!(single.exit, 75);
        assert_eq!(
            serde_json::to_value(compound).unwrap()["disposition"],
            json!({
                "effects":"unknown", "action":"reconcile"
            })
        );
        let single = serde_json::to_value(single).unwrap();
        assert_eq!(single["retry_after"], "Wed, 21 Oct 2026 07:28:00 GMT");
        assert_eq!(
            single["disposition"],
            json!({
                "effects":"none", "action":"retry"
            })
        );
        // A later read's typed refusal cannot erase an earlier successful
        // write; likewise a later conditional refusal or policy denial closes
        // only its request.
        for (read_refusal, kind) in [
            (true, Refusal::Busy),
            (false, Refusal::Condition),
            (true, Refusal::Condition),
            (false, Refusal::Denied),
        ] {
            let compound = attempt(true, read_refusal, kind).await;
            assert_eq!(compound.exit, 2);
            assert_eq!(
                serde_json::to_value(compound).unwrap()["disposition"]["effects"],
                "unknown"
            );
        }
        let conditional = attempt(false, false, Refusal::Condition).await;
        assert_eq!(conditional.exit, 4);
        assert_eq!(
            serde_json::to_value(conditional).unwrap()["disposition"],
            json!({"effects":"none","action":"refresh"})
        );
        let denied = attempt(false, false, Refusal::Denied).await;
        assert_eq!(denied.exit, 1);
        assert_eq!(
            serde_json::to_value(denied).unwrap()["disposition"],
            json!({"effects":"none","action":"refresh"})
        );
    }

    #[tokio::test]
    #[ignore = "nightly: waits out the real 30 s managed deadline for five mutation forms"]
    async fn managed_mutations_use_thirty_second_deadline_without_retrying() {
        // Exercise every mutation request owner through actual HTTP. Receipts
        // can arrive after ten seconds, but the total thirty-second deadline
        // still bounds uncertain writes without replaying them.
        futures::future::join_all(
            [
                "ad-hoc",
                "conditional",
                "branch",
                "stored",
                "stored-conditional",
            ]
            .into_iter()
            .flat_map(|form| {
                [
                    (form, std::time::Duration::from_millis(10_250), false),
                    (form, std::time::Duration::from_millis(30_250), true),
                ]
            })
            .map(|(form, delay, expect_timeout)| {
                let commit = json!({
                    "graph_commit_id": "head-after", "graph_branch": "main",
                    "graph_manifest_version": 7, "parent_commit_id": "head-before",
                    "merged_parent_commit_id": "head-source",
                    "actor_id": "principal:alice", "created_at": 12345,
                    "published": true, "kind": "published", "root_commit_id": null
                });
                let mut reply = json!({
                    "branch": "main", "query_name": "m", "affected_nodes": 1,
                    "affected_edges": 0, "actor_id": "principal:alice", "commit": commit
                });
                if form == "branch" {
                    reply["query_name"] = json!("branch merge");
                    reply["affected_nodes"] = json!(0);
                    reply["outcome"] = json!({
                        "kind": "merged", "source": "review", "target": "main",
                        "merge": "fast_forward"
                    });
                }
                let server = IntentApiFixture::graph_with_response_delay(
                    vec![IntentReply::json(200, reply)],
                    delay,
                );
                let client =
                    GraphClient::managed(&server.origin, "knowledge", "data-credential".into())
                        .unwrap();
                // Finish synchronous client setup before join_all polls requests.
                async move {
                    let started = std::time::Instant::now();
                    let (result, path) = match form {
                        "branch" => (
                            client
                                .branch_write_statement(
                                    "branch merge review into main",
                                    BranchWrite::Merge {
                                        source: "review".into(),
                                        into: Some("main".into()),
                                        as_commit: false,
                                    },
                                    &[],
                                    &mut std::io::sink(),
                                )
                                .await,
                            "/graphs/knowledge/mutate",
                        ),
                        "stored" | "stored-conditional" => (
                            client
                                .invoke_named::<ChangeOutput>(
                                    "m",
                                    true,
                                    None,
                                    Some("main".into()),
                                    None,
                                    (form == "stored-conditional").then_some("head-before"),
                                )
                                .await,
                            if form == "stored-conditional" {
                                "/graphs/knowledge/queries/m/if-graph-commit"
                            } else {
                                "/graphs/knowledge/queries/m"
                            },
                        ),
                        _ => (
                            client
                                .mutate(
                                    Some("main"),
                                    "mutation m() {}",
                                    Some("m"),
                                    None,
                                    (form == "conditional").then_some("head-before"),
                                    &[],
                                    &mut std::io::sink(),
                                )
                                .await,
                            if form == "conditional" {
                                "/graphs/knowledge/mutate/if-graph-commit"
                            } else {
                                "/graphs/knowledge/mutate"
                            },
                        ),
                    };
                    if !expect_timeout {
                        let result = result.unwrap_or_else(|error| panic!("{form}: {error}"));
                        assert!(started.elapsed() >= std::time::Duration::from_secs(10));
                        assert_eq!(serde_json::to_value(result.commit).unwrap(), commit);
                        assert_eq!(result.actor_id.as_deref(), Some("principal:alice"));
                    } else {
                        let error =
                            result.expect_err("mutation must stop at its thirty-second deadline");
                        assert!(
                            error
                                .downcast_ref::<reqwest::Error>()
                                .is_some_and(reqwest::Error::is_timeout),
                            "{form}: {error}"
                        );
                        assert!(started.elapsed() >= std::time::Duration::from_secs(30));
                    }
                    let requests = server.workflow_requests();
                    assert_eq!(requests.len(), 1, "{form} must not retry");
                    assert_eq!(requests[0].path, path);
                    assert_eq!(
                        requests[0].headers["authorization"],
                        "Bearer data-credential"
                    );
                    if form.ends_with("conditional") {
                        assert_eq!(
                            requests[0].headers["omnigraph-if-graph-commit"],
                            "head-before"
                        );
                    }
                    if form.starts_with("stored") {
                        assert_eq!(requests[0].body["expect_mutation"], true);
                    }
                    server.assert_complete();
                }
            }),
        )
        .await;
    }

    #[tokio::test]
    #[ignore = "nightly: waits out the real 30 s managed deadline for four read operations"]
    async fn managed_reads_use_thirty_second_deadline_without_retrying() {
        futures::future::join_all(
            ["ad-hoc", "stored", "commit-list", "commit-show"]
                .into_iter()
                .flat_map(|operation| {
                    [
                        (operation, std::time::Duration::from_millis(10_250), false),
                        (operation, std::time::Duration::from_millis(30_250), true),
                    ]
                })
                .map(|(operation, delay, expect_timeout)| {
                    let commit = json!({
                        "graph_commit_id": "commit-a", "graph_branch": "main",
                        "graph_manifest_version": 7, "parent_commit_id": "prior",
                        "merged_parent_commit_id": null, "actor_id": "principal:alice",
                        "created_at": 12345, "published": true, "kind": "published",
                        "root_commit_id": null
                    });
                    let reply = match operation {
                        "commit-list" => json!({"commits": [commit]}),
                        "commit-show" => commit,
                        _ => json!({
                            "query_name": "q", "target": {"branch":"main", "snapshot":null},
                            "row_count": 1, "columns": ["value"], "rows": [{"value":42}],
                            "graph_commit_id": "head"
                        }),
                    };
                    let server = IntentApiFixture::graph_with_response_delay(
                        vec![IntentReply::json(200, reply.clone())],
                        delay,
                    );
                    let client =
                        GraphClient::managed(&server.origin, "knowledge", "data-credential".into())
                            .unwrap();
                    // Finish synchronous client setup before join_all polls requests.
                    async move {
                        let started = std::time::Instant::now();
                        let (result, path) = match operation {
                            "stored" => (
                                client
                                    .invoke_named::<ReadOutput>(
                                        "q",
                                        false,
                                        None,
                                        Some("main".into()),
                                        None,
                                        None,
                                    )
                                    .await
                                    .map(|output| serde_json::to_value(output).unwrap()),
                                "/graphs/knowledge/queries/q",
                            ),
                            "commit-list" => (
                                client
                                    .list_commits(Some("main"))
                                    .await
                                    .map(|output| serde_json::to_value(output).unwrap()),
                                "/graphs/knowledge/commits?branch=main",
                            ),
                            "commit-show" => (
                                client
                                    .get_commit("commit-a")
                                    .await
                                    .map(|output| serde_json::to_value(output).unwrap()),
                                "/graphs/knowledge/commits/commit-a",
                            ),
                            _ => (
                                client
                                    .query(
                                        Some(ReadTarget::branch("main")),
                                        "query q() {}",
                                        Some("q"),
                                        None,
                                        &[],
                                    )
                                    .await
                                    .map(|output| serde_json::to_value(output).unwrap()),
                                "/graphs/knowledge/query",
                            ),
                        };
                        if expect_timeout {
                            let error =
                                result.expect_err("read must stop at its thirty-second deadline");
                            assert!(
                                error
                                    .downcast_ref::<reqwest::Error>()
                                    .is_some_and(reqwest::Error::is_timeout),
                                "{operation}: {error}"
                            );
                            assert!(started.elapsed() >= std::time::Duration::from_secs(30));
                        } else {
                            let output =
                                result.unwrap_or_else(|error| panic!("{operation}: {error}"));
                            assert!(started.elapsed() >= std::time::Duration::from_secs(10));
                            assert_eq!(output, reply);
                        }
                        let requests = server.workflow_requests();
                        assert_eq!(requests.len(), 1, "read must not retry");
                        assert_eq!(requests[0].path, path);
                        assert_eq!(
                            requests[0].headers["authorization"],
                            "Bearer data-credential"
                        );
                        server.assert_complete();
                    }
                }),
        )
        .await;
    }

    #[tokio::test]
    async fn process_setting_is_refused_before_any_request_is_sent() {
        let server = IntentApiFixture::new(vec![]);
        let client =
            GraphClient::managed(&server.origin, "knowledge", "data-credential".into()).unwrap();
        let settings = parse_set_flags(&["stage_write_concurrency=4".to_string()]).unwrap();
        const REFUSAL: &str = "setting `stage_write_concurrency` is a process setting; it is \
                               read from the server's environment, not from a request";
        let refused = [
            (
                "query",
                client
                    .query(
                        Some(ReadTarget::branch("main")),
                        "query q() {}",
                        Some("q"),
                        None,
                        &settings,
                    )
                    .await
                    .map(|_| ()),
            ),
            (
                "mutate",
                client
                    .mutate(
                        Some("main"),
                        "mutation m() {}",
                        Some("m"),
                        None,
                        None,
                        &settings,
                        &mut std::io::sink(),
                    )
                    .await
                    .map(|_| ()),
            ),
            (
                "branch list",
                client
                    .branch_list_statement("branch list", &settings)
                    .await
                    .map(|_| ()),
            ),
            (
                "branch merge",
                client
                    .branch_merge(
                        "review",
                        "main",
                        false,
                        false,
                        false,
                        &settings,
                        &mut std::io::sink(),
                    )
                    .await
                    .map(|_| ()),
            ),
            (
                "changes poll",
                client
                    .poll_changes_page(
                        Some("main"),
                        None,
                        None,
                        None,
                        None,
                        &ChangeFilterArgs {
                            kinds: &[],
                            types: &[],
                            ops: &[],
                        },
                        &settings,
                    )
                    .await
                    .map(|_| ()),
            ),
        ];
        for (owner, result) in refused {
            let error = result.expect_err(owner);
            assert_eq!(error.to_string(), REFUSAL, "{owner}");
        }
        assert!(
            server.requests().is_empty(),
            "the scope refusal precedes the round trip"
        );
        server.assert_complete();
    }

    #[tokio::test]
    async fn request_setting_travels_in_the_settings_field_of_query_mutate_and_merge() {
        let read = json!({
            "query_name": "q", "target": {"branch":"main", "snapshot":null},
            "row_count": 0, "columns": [], "rows": [], "graph_commit_id": "head"
        });
        let change = json!({
            "branch": "main", "query_name": "m", "affected_nodes": 1,
            "affected_edges": 0, "actor_id": "principal:alice", "commit": {
                "graph_commit_id": "head-after", "graph_branch": "main",
                "graph_manifest_version": 7, "parent_commit_id": "head-before",
                "merged_parent_commit_id": null,
                "actor_id": "principal:alice", "created_at": 12345
            }
        });
        let merged = json!({
            "source": "review", "target": "main", "outcome": "merged",
            "actor_id": "principal:alice", "commit": {
                "graph_commit_id": "merge", "graph_branch": null,
                "graph_manifest_version": 8, "parent_commit_id": "target",
                "merged_parent_commit_id": "source",
                "actor_id": "principal:alice", "created_at": 12345
            }
        });
        let server = IntentApiFixture::graph(vec![
            IntentReply::json(200, read),
            IntentReply::json(200, change.clone()),
            IntentReply::json(200, change.clone()),
            IntentReply::json(200, merged),
            IntentReply::json(200, change),
        ]);
        let client =
            GraphClient::managed(&server.origin, "knowledge", "data-credential".into()).unwrap();
        let settings = parse_set_flags(&[
            "merge_lineage=off".to_string(),
            "ann_nprobes=1".to_string(),
            "write_max_bytes=4093".to_string(),
        ])
        .unwrap();
        client
            .query(
                Some(ReadTarget::branch("main")),
                "query q() {}",
                Some("q"),
                None,
                &settings,
            )
            .await
            .unwrap();
        client
            .mutate(
                Some("main"),
                "mutation m() {}",
                Some("m"),
                None,
                None,
                &settings,
                &mut std::io::sink(),
            )
            .await
            .unwrap();
        client
            .mutate(
                Some("main"),
                "mutation m() {}",
                Some("m"),
                None,
                Some("head-before"),
                &settings,
                &mut std::io::sink(),
            )
            .await
            .unwrap();
        client
            .branch_merge(
                "review",
                "main",
                false,
                false,
                false,
                &settings,
                &mut std::io::sink(),
            )
            .await
            .unwrap();

        client
            .mutate(
                Some("main"),
                "mutation m() {}",
                Some("m"),
                None,
                None,
                &[],
                &mut std::io::sink(),
            )
            .await
            .unwrap();
        let requests = server.workflow_requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[4].path, "/graphs/knowledge/mutate");
        assert_eq!(requests[4].body["query"], "mutation m() {}");
        assert_eq!(requests[4].body["name"], "m");
        assert!(requests[4].body.get("query_source").is_none());
        assert!(requests[4].body.get("query_name").is_none());
        assert!(requests[4].body.get("settings").is_none());
        let field = json!({"merge_lineage": "off", "ann_nprobes": 1, "write_max_bytes": 4093});
        assert_eq!(requests[0].path, "/graphs/knowledge/query");
        assert_eq!(requests[0].body["settings"], field);
        assert_eq!(
            requests[1].path, "/graphs/knowledge/mutate",
            "mutations use the canonical route"
        );
        assert_eq!(requests[1].body["settings"], field);
        assert!(
            !requests[1]
                .headers
                .contains_key("omnigraph-if-graph-commit")
        );
        assert_eq!(requests[2].path, "/graphs/knowledge/mutate/if-graph-commit");
        assert_eq!(
            requests[2].headers["omnigraph-if-graph-commit"],
            "head-before"
        );
        assert_eq!(requests[2].body["settings"], field);
        assert_eq!(requests[3].path, "/graphs/knowledge/branches/merge");
        assert_eq!(requests[3].body["settings"], field);
        server.assert_complete();
    }

    #[test]
    fn managed_load_request_has_its_own_deadline_and_exact_input_bound() {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(32 * 1024 * 1024).unwrap();
        assert_eq!(
            read_managed_load_data(file.path().to_str().unwrap())
                .unwrap()
                .len(),
            32 * 1024 * 1024
        );
        file.as_file().set_len(32 * 1024 * 1024 + 1).unwrap();
        assert!(
            read_managed_load_data(file.path().to_str().unwrap())
                .unwrap_err()
                .to_string()
                .contains("32 MiB")
        );
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();
        for (managed, expected) in [
            (true, Some(std::time::Duration::from_secs(300))),
            (false, None),
        ] {
            let request = load_request(http.post("https://data.example"), "{}\n".into(), managed)
                .build()
                .unwrap();
            assert_eq!(request.timeout().copied(), expected);
            assert_eq!(
                request.headers()[reqwest::header::CONTENT_TYPE],
                "application/x-ndjson"
            );
            assert_eq!(request.body().unwrap().as_bytes(), Some(b"{}\n".as_slice()));
        }
    }

    fn content_range_headers(value: &'static str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(CONTENT_RANGE, value.parse().unwrap());
        headers
    }

    #[test]
    fn resolve_registry_is_sync_and_yields_the_bare_base_url() {
        // Structural proof the RFC-011 D7 multi-graph probe cannot fire on the
        // graphs-list path: resolve_registry is synchronous (called here with
        // no tokio runtime), while the probe is async and performs GET
        // /graphs. Also pins the URL-corruption fix: the bare base URL with
        // the trailing slash trimmed and no `/graphs/<id>` segment. A literal
        // `://` --server value bypasses the operator server registry, so a
        // developer's real config cannot change the outcome.
        let client = GraphClient::resolve_registry(
            &InvocationConfig::at(None, None),
            Some("http://server.invalid:9/"),
            None,
            &mut std::io::sink(),
        )
        .unwrap();
        assert_eq!(client.uri(), "http://server.invalid:9");
        assert!(client.is_remote());
    }

    #[test]
    fn content_range_must_cover_the_exact_requested_or_eof_clamped_length() {
        let exact = BlobRangeRequest::new(Some(0), Some(6)).unwrap().unwrap();
        validate_content_range(&content_range_headers("bytes 0-5/100"), exact, 6).unwrap();
        assert!(
            validate_content_range(&content_range_headers("bytes 0-2/100"), exact, 3).is_err(),
            "a self-consistent but truncated 206 must not be accepted as the requested range"
        );

        let clamped = BlobRangeRequest::new(Some(98), Some(6)).unwrap().unwrap();
        validate_content_range(&content_range_headers("bytes 98-99/100"), clamped, 2).unwrap();

        let open = BlobRangeRequest::new(Some(97), None).unwrap().unwrap();
        validate_content_range(&content_range_headers("bytes 97-99/100"), open, 3).unwrap();
    }
}
