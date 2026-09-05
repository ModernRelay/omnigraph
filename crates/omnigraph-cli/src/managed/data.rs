//! RFC 0053: cached data authority has its own keychain namespace and transport.
use super::auth::{self, Store};
use super::{Api, Context, Failure, Method, Output, Result, canonical_origin, json};
use crate::cli::{Cli, Command};
use crate::client::GraphClient;
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
    grants: Vec<Grant>,
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
        if self.version != 1
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
        validate_grants(&self.grants)
    }

    fn metadata(&self) -> Value {
        json!({"cluster_id":self.cluster_id,"endpoint":self.endpoint,"expires_at":self.expires_at,"kid":self.kid,"actor":self.actor,"grants":self.grants})
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
            "managed data uses folder context and its cached credential; --server, --profile, --store, --cluster, and --as require explicit --direct",
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
    // Fail early if the platform cannot access its credential store.
    let _ = store.get(&key(context))?;
    let body = api
        .request(
            Method::POST,
            &format!("/v1/clusters/{}/tokens", context.cluster),
            Some(&json!({"grants":[grant],"ttl_seconds":ttl})),
            None,
        )
        .await?;
    super::cluster_matches(&body, &context.cluster)?;
    let data = &body["data"];
    let string = |field| {
        data.get(field)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(Failure::protocol)
    };
    let credential = Credential {
        version: 1,
        api: context.api.clone(),
        cluster_id: context.cluster.clone(),
        endpoint: string("endpoint")?,
        token: string("token")?,
        expires_at: string("expires_at")?,
        kid: string("kid")?,
        actor: string("actor")?,
        grants: serde_json::from_value(data["grants"].clone()).map_err(|_| Failure::protocol())?,
    };
    credential.validate(context)?;
    if credential.grants.len() != 1
        || credential.grants[0].graph_id != grant.graph_id
        || credential.grants[0].actions.iter().collect::<BTreeSet<_>>()
            != grant.actions.iter().collect::<BTreeSet<_>>()
        || OffsetDateTime::parse(&credential.expires_at, &Rfc3339).map_err(|_| invalid())?
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
    let grant = requested_grant(cli.graph.as_deref(), actions)?;
    let api = Api::new(
        context.api.clone(),
        Some(auth::credential(&auth::CONTROL_STORE, &context.api)?),
    )?;
    mint(&auth::DATA_STORE, context, &api, grant, ttl.unwrap_or(3600)).await
}

fn load(
    store: &impl Store,
    context: &Context,
    graph: &str,
    required: &[&str],
) -> Result<GraphClient> {
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
    credential.validate(context)?;
    if !credential.grants.iter().any(|grant| {
        grant.graph_id == graph
            && required
                .iter()
                .all(|action| grant.actions.iter().any(|a| a == action))
    }) {
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
        || matches!(cli.command, Command::Cluster { .. })
        || (crate::planes::command_plane(&cli.command) == crate::planes::Plane::Session
            && !matches!(cli.command, Command::Alias { .. }))
}

fn resolve(cli: &Cli, config: &std::path::Path, store: &impl Store) -> Result<Option<GraphClient>> {
    if skips_context(cli) {
        return Ok(None);
    }
    let Some(context) = super::read_context(config)? else {
        return Ok(None);
    };
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
        _ => {
            return Err(Failure::refused(
                "managed_command_unsupported",
                "managed data currently supports query and mutate; use --direct only when legacy addressing is intended",
            ));
        }
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
        _ => false,
    };
    let result = std::env::current_dir()
        .map_err(|_| Failure::refused("context_invalid", "cannot resolve the current directory"))
        .and_then(|cwd| resolve(cli, &cwd, &auth::DATA_STORE));
    result.map_err(|e| Output::from_result(Err(e), json, 2))
}

#[cfg(test)]
mod tests;
