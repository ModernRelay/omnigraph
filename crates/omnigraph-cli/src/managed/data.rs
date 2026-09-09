//! Cached data credentials have a separate keychain namespace and transport.
use super::auth::{self, Store};
use super::{Api, Context, Failure, Method, Output, Result, canonical_origin, json};
use crate::cli::{Cli, Command, GraphsCommand};
use crate::client::GraphClient;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const MAX_CREDENTIAL: usize = 64 * 1024;
const MAX_TOKEN: usize = 8192;
const ACTIONS: [&str; 8] = [
    "read",
    "export",
    "change",
    "branch_create",
    "branch_delete",
    "branch_merge",
    "invoke_query",
    "graph_list",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Grant {
    graph_id: String,
    actions: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    version: u8,
    api: String,
    cluster_id: String,
    endpoint: String,
    token: String,
    expires_at: String,
    kid: String,
    actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cluster_incarnation: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    grants: Vec<Grant>,
}

/// Parsed only to reject mismatched issuance/cache metadata, never to select
/// configuration or grant authority. The server still verifies the signature.
fn parse_identity_claims(
    token: &str,
) -> Option<omnigraph_server::data_tokens::IdentityTokenClaims> {
    if token.len() > MAX_TOKEN {
        return None;
    }
    let mut parts = token.split('.');
    let _header = parts.next()?;
    let claims = parts.next()?;
    let _signature = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let claims: omnigraph_server::data_tokens::IdentityTokenClaims =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims).ok()?).ok()?;
    (claims.version == 2).then_some(claims)
}

fn key(context: &Context) -> String {
    format!("{}/clusters/{}", context.api, context.cluster)
}

fn invalid() -> Failure {
    Failure::refused(
        "data_credential_invalid",
        "the cached data credential is invalid; mint a new cluster token",
    )
}

fn graph_id(graph: &str) -> Result<()> {
    // The server's graph selector is a single path segment, never path syntax.
    if graph.is_empty()
        || graph.len() > 64
        || !graph.as_bytes()[0].is_ascii_alphabetic()
        || !graph
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        || matches!(graph, "policies" | "healthz" | "openapi" | "graphs")
    {
        return Err(Failure::refused(
            "graph_invalid",
            "--graph must select one valid graph id",
        ));
    }
    Ok(())
}

fn validate_grants(grants: &[Grant]) -> Result<()> {
    let mut graphs = BTreeSet::new();
    if grants.is_empty() || grants.len() > 64 {
        return Err(invalid());
    }
    for grant in grants {
        graph_id(&grant.graph_id).map_err(|_| invalid())?;
        let mut seen = BTreeSet::new();
        if !graphs.insert(&grant.graph_id)
            || grant.actions.is_empty()
            || grant.actions.len() > ACTIONS.len()
            || grant
                .actions
                .iter()
                .any(|a| !ACTIONS.contains(&a.as_str()) || !seen.insert(a))
        {
            return Err(invalid());
        }
    }
    Ok(())
}

impl Credential {
    fn validate(&self, context: &Context) -> Result<()> {
        let now = OffsetDateTime::now_utc();
        let expires = OffsetDateTime::parse(&self.expires_at, &Rfc3339).map_err(|_| invalid())?;
        if !matches!(self.version, 1 | 2)
            || self.api != context.api
            || self.cluster_id != context.cluster
            || !canonical_origin(&self.endpoint).is_ok_and(|o| o == self.endpoint)
            || self.token.is_empty()
            || self.token.len() > MAX_TOKEN
            || self.token.split('.').count() != 3
            || self.token.split('.').any(str::is_empty)
            || !self
                .token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            || self.kid.len() != 64
            || !self
                .kid
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !self.actor.starts_with("principal:")
            || self.actor.len() == "principal:".len()
            || self.actor.len() > 1024
            || self
                .actor
                .chars()
                .any(|c| c.is_control() || c.is_whitespace())
            || expires > now + time::Duration::seconds(86430)
        {
            return Err(invalid());
        }
        if expires <= now {
            return Err(Failure::refused(
                "data_credential_expired",
                "the data credential has expired; mint a new cluster token",
            ));
        }
        if self.version == 1 {
            if self.cluster_incarnation.is_some() || parse_identity_claims(&self.token).is_some() {
                return Err(invalid());
            }
            validate_grants(&self.grants)
        } else {
            let claims = parse_identity_claims(&self.token).ok_or_else(invalid)?;
            let header = self.token.split('.').next().ok_or_else(invalid)?;
            let header: omnigraph_server::data_tokens::DataTokenHeader =
                serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).map_err(|_| invalid())?)
                    .map_err(|_| invalid())?;
            if !self.grants.is_empty()
                || claims.iss != self.api
                || claims.cluster_id != self.cluster_id
                || self.cluster_incarnation.as_deref() != Some(claims.cluster_incarnation.as_str())
                || claims.aud != format!("urn:omnigraph:data:{}", self.cluster_id)
                || self.actor != format!("principal:{}", claims.sub)
                || i64::try_from(claims.exp).ok() != Some(expires.unix_timestamp())
                || header.kid != self.kid
                || header.typ != "JWT"
                || header.alg != "ES256"
                || !claims
                    .exp
                    .checked_sub(claims.iat)
                    .is_some_and(|ttl| (60..=86400).contains(&ttl))
                || claims.iat
                    > u64::try_from(now.unix_timestamp())
                        .unwrap_or_default()
                        .saturating_add(30)
            {
                return Err(invalid());
            }
            Ok(())
        }
    }

    fn metadata(&self) -> Value {
        let mut metadata = json!({"cluster_id":self.cluster_id,"endpoint":self.endpoint,"expires_at":self.expires_at,"kid":self.kid,"actor":self.actor});
        if self.version == 1 {
            metadata["grants"] = json!(self.grants);
        } else {
            metadata["version"] = json!(2);
        }
        metadata
    }
}

pub(crate) fn parse_ttl(input: &str) -> std::result::Result<u64, String> {
    let (number, multiplier) = match input.as_bytes().last() {
        Some(b's') => (&input[..input.len() - 1], 1),
        Some(b'm') => (&input[..input.len() - 1], 60),
        Some(b'h') => (&input[..input.len() - 1], 3600),
        Some(b'd') => (&input[..input.len() - 1], 86400),
        _ => (input, 1),
    };
    number
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .filter(|n| (60..=86400).contains(n))
        .ok_or_else(|| {
            "TTL must be 60–86400 seconds, optionally suffixed s, m, h, or d".to_string()
        })
}

fn scope(cli: &Cli) -> Result<()> {
    if cli.server.is_some()
        || cli.profile.is_some()
        || cli.store.is_some()
        || cli.cluster.is_some()
        || cli.as_actor.is_some()
    {
        return Err(Failure::refused(
            "managed_scope_conflict",
            "this managed operation uses its selected context; ordinary target selectors and an explicit --as are not applicable",
        ));
    }
    Ok(())
}

fn requested_grant(graph: Option<&str>, actions: Option<&str>) -> Result<Grant> {
    let graph =
        graph.ok_or_else(|| Failure::refused("graph_required", "managed data requires --graph"))?;
    graph_id(graph)?;
    let actions: Vec<String> = actions
        .unwrap_or("")
        .split(',')
        .map(str::to_string)
        .collect();
    let grant = Grant {
        graph_id: graph.into(),
        actions,
    };
    validate_grants(std::slice::from_ref(&grant)).map_err(|_| Failure::refused("data_actions_invalid", "--actions must be a nonempty, duplicate-free comma-separated list of supported data actions"))?;
    Ok(grant)
}

async fn mint(
    store: &impl Store,
    context: &Context,
    api: &Api,
    grant: Grant,
    ttl: u64,
) -> Result<Value> {
    mint_profile(store, context, api, Some(grant), ttl).await
}

async fn mint_profile(
    store: &impl Store,
    context: &Context,
    api: &Api,
    grant: Option<Grant>,
    ttl: u64,
) -> Result<Value> {
    // Fail early if the platform cannot access its credential store.
    let _ = store.get(&key(context))?;
    let body = api
        .request(
            Method::POST,
            &format!("/v1/clusters/{}/tokens", context.cluster),
            Some(&match &grant {
                Some(grant) => json!({"grants":[grant],"ttl_seconds":ttl}),
                None => json!({"version":2,"ttl_seconds":ttl}),
            }),
            None,
        )
        .await?;
    super::cluster_matches(&body, &context.cluster)?;
    let data = &body["data"];
    if grant.is_some()
        && (data.get("version").is_some_and(|version| *version != 1)
            || data["token"]
                .as_str()
                .and_then(parse_identity_claims)
                .is_some())
    {
        // A response cannot upgrade an explicit restricted request, even if
        // it also echoes the requested grants beside an identity credential.
        return Err(Failure::protocol());
    }
    if grant.is_none()
        && (data["version"] != 2
            || ["grants", "roles", "actions", "groups", "policy"]
                .iter()
                .any(|field| data.get(field).is_some()))
    {
        return Err(Failure::protocol());
    }
    let string = |field| {
        data.get(field)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(Failure::protocol)
    };
    let credential = Credential {
        version: if grant.is_some() { 1 } else { 2 },
        api: context.api.clone(),
        cluster_id: context.cluster.clone(),
        endpoint: string("endpoint")?,
        token: string("token")?,
        expires_at: string("expires_at")?,
        kid: string("kid")?,
        actor: string("actor")?,
        cluster_incarnation: if grant.is_none() {
            Some(
                body["meta"]["incarnation"]
                    .as_str()
                    .ok_or_else(Failure::protocol)?
                    .to_owned(),
            )
        } else {
            None
        },
        grants: if grant.is_some() {
            serde_json::from_value(data["grants"].clone()).map_err(|_| Failure::protocol())?
        } else {
            Vec::new()
        },
    };
    credential.validate(context)?;
    if grant.as_ref().is_some_and(|grant| {
        credential.grants.len() != 1
            || credential.grants[0].graph_id != grant.graph_id
            || credential.grants[0].actions.iter().collect::<BTreeSet<_>>()
                != grant.actions.iter().collect::<BTreeSet<_>>()
    }) || OffsetDateTime::parse(&credential.expires_at, &Rfc3339).map_err(|_| invalid())?
        > OffsetDateTime::now_utc() + time::Duration::seconds(ttl as i64 + 30)
    {
        return Err(Failure::protocol());
    }
    let saved = serde_json::to_string(&credential).map_err(|_| invalid())?;
    if saved.len() > MAX_CREDENTIAL {
        return Err(invalid());
    }
    store.put(&key(context), &saved)?;
    // Construct output from a strict metadata allowlist; never echo provider/API extras.
    let mut metadata = credential.metadata();
    auth::scrub_value(&mut metadata, &credential.token);
    Ok(json!({"data":metadata,"meta":{"cluster_id":context.cluster}}))
}

fn clear(store: &impl Store, context: &Context) -> Result<Value> {
    store.remove(&key(context))?;
    Ok(
        json!({"data":{"cluster_id":context.cluster,"local_credential_removed":true,"revocation_performed":false},"meta":{"cluster_id":context.cluster}}),
    )
}

pub(super) async fn token(
    cli: &Cli,
    context: &Context,
    actions: Option<&str>,
    ttl: Option<u64>,
    clear: bool,
) -> Result<Value> {
    scope(cli)?;
    if clear {
        if cli.graph.is_some() || actions.is_some() || ttl.is_some() {
            return Err(Failure::refused(
                "token_clear_conflict",
                "--clear forgets the whole cached cluster credential and cannot select a graph, actions, or TTL",
            ));
        }
        return self::clear(&auth::DATA_STORE, context);
    }
    let grant = if actions.is_some() {
        Some(requested_grant(cli.graph.as_deref(), actions)?)
    } else {
        if cli.graph.is_some() {
            return Err(Failure::refused(
                "token_profile_conflict",
                "identity credentials do not select a graph; use --graph on the graph operation, or pair it with --actions for the legacy restricted profile",
            ));
        }
        None
    };
    let api = Api::new(
        context.api.clone(),
        Some(auth::credential(&auth::CONTROL_STORE, &context.api)?),
    )?;
    match grant {
        Some(grant) => mint(&auth::DATA_STORE, context, &api, grant, ttl.unwrap_or(3600)).await,
        None => mint_profile(&auth::DATA_STORE, context, &api, None, ttl.unwrap_or(3600)).await,
    }
}

fn load_credential(store: &impl Store, context: &Context) -> Result<Credential> {
    let raw = store.get(&key(context))?.ok_or_else(|| {
        Failure::refused(
            "data_credential_required",
            "no data credential is cached for this cluster; run cluster token",
        )
    })?;
    if raw.len() > MAX_CREDENTIAL {
        return Err(invalid());
    }
    let credential: Credential = serde_json::from_str(&raw).map_err(|_| invalid())?;
    if credential.version == 2
        && serde_json::from_str::<Value>(&raw)
            .map_err(|_| invalid())?
            .get("grants")
            .is_some()
    {
        return Err(invalid());
    }
    credential.validate(context)?;
    Ok(credential)
}

fn load(
    store: &impl Store,
    context: &Context,
    graph: &str,
    required: &[&str],
) -> Result<GraphClient> {
    let credential = load_credential(store, context)?;
    if credential.version == 1
        && !credential.grants.iter().any(|grant| {
            grant.graph_id == graph
                && required
                    .iter()
                    .all(|action| grant.actions.iter().any(|a| a == action))
        })
    {
        return Err(Failure::refused(
            "data_scope_missing",
            "the cached credential does not grant this graph and action; mint a matching cluster token",
        ));
    }
    GraphClient::managed(&credential.endpoint, graph, credential.token).map_err(|_| {
        Failure::new(
            "transport_failed",
            "could not initialize the managed data client",
            1,
        )
    })
}

fn skips_context(cli: &Cli) -> bool {
    cli.direct
        || !matches!(
            cli.command,
            Command::Query { .. }
                | Command::Mutate { .. }
                | Command::Graphs {
                    command: GraphsCommand::List { .. }
                }
        )
        || cli.server.is_some()
        || cli.profile.is_some()
        || cli.store.is_some()
        || cli.cluster.is_some()
}

/// This check never resolves a competing target or consults credentials. Even
/// an unknown profile or matching-looking server URL is ambiguous beside a
/// managed binding. Call it only after selecting a valid exact-directory
/// context, so ordinary commands retain their own operator-config behavior.
fn has_ambient_target() -> Result<bool> {
    if std::env::var_os(crate::scope::PROFILE_ENV).is_some_and(|value| !value.is_empty()) {
        return Ok(true);
    }
    let operator = crate::operator::load_operator_config().map_err(|_| {
        Failure::refused(
            "operator_config_invalid",
            "cannot read valid operator configuration to exclude a competing data target",
        )
    })?;
    Ok(operator.default_server().is_some() || operator.default_store().is_some())
}

fn resolve(
    cli: &Cli,
    config: &std::path::Path,
    store: &impl Store,
    ambient_target: impl FnOnce() -> Result<bool>,
) -> Result<Option<GraphClient>> {
    if skips_context(cli) {
        return Ok(None);
    }
    let Some(context) = super::read_context(config)? else {
        return Ok(None);
    };
    if ambient_target()? {
        return Err(Failure::refused(
            "managed_target_ambiguous",
            "folder context competes with OMNIGRAPH_PROFILE or an operator default target; select the intended ordinary target explicitly, use --direct for ordinary ambient resolution, or clear the competing ambient target to use this managed folder",
        ));
    }
    if matches!(cli.command, Command::Graphs { .. }) {
        scope(cli)?;
        if cli.graph.is_some() {
            return Err(Failure::refused(
                "graph_scope_conflict",
                "graphs list enumerates a cluster; omit --graph",
            ));
        }
        let credential = load_credential(store, &context)?;
        if credential.version != 2 {
            return Err(Failure::refused(
                "data_profile_unsupported",
                "managed graph discovery requires an identity credential; legacy restrictions are not widened",
            ));
        }
        return GraphClient::managed_registry(&credential.endpoint, credential.token)
            .map(Some)
            .map_err(|_| {
                Failure::new(
                    "transport_failed",
                    "could not initialize the managed discovery client",
                    1,
                )
            });
    }
    let (action, named) = match &cli.command {
        Command::Query {
            query,
            query_string,
            ..
        } => ("read", query.is_none() && query_string.is_none()),
        Command::Mutate {
            query,
            query_string,
            ..
        } => ("change", query.is_none() && query_string.is_none()),
        _ => unreachable!("only implicit query/mutate consult data context"),
    };
    scope(cli)?;
    let graph = cli
        .graph
        .as_deref()
        .ok_or_else(|| Failure::refused("graph_required", "managed data requires --graph"))?;
    graph_id(graph)?;
    let required = if named {
        vec![action, "invoke_query"]
    } else {
        vec![action]
    };
    load(store, &context, graph, &required).map(Some)
}

pub(crate) fn client(cli: &Cli) -> std::result::Result<Option<GraphClient>, Output> {
    if skips_context(cli) {
        return Ok(None);
    }
    let json = match &cli.command {
        Command::Query { json, format, .. } => {
            *json || matches!(format, Some(crate::read_format::ReadOutputFormat::Json))
        }
        Command::Mutate { json, .. } => *json,
        Command::Graphs {
            command: GraphsCommand::List { json, .. },
        } => *json,
        _ => false,
    };
    let result = std::env::current_dir()
        .map_err(|_| Failure::refused("context_invalid", "cannot resolve the current directory"))
        .and_then(|cwd| resolve(cli, &cwd, &auth::DATA_STORE, has_ambient_target));
    result.map_err(|e| Output::from_result(Err(e), json, 2))
}

#[cfg(test)]
mod tests;
