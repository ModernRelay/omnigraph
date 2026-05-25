//! Identity types for the multi-graph server (MR-668) + forward-compatible
//! shapes for Cloud mode (RFC 0003) and OAuth provider (RFC 0004).
//!
//! Per decision 13 in the implementation plan: ship the type shapes that
//! Cloud mode will consume, without committing to any trait shape
//! (`TokenVerifier` stays draft in RFC 0001). Every Cluster-mode call site
//! constructs these types with their Cluster-mode-specific values:
//!
//! - `tenant_id: None` (Cloud will set `Some(...)` from the OAuth `org_id` claim)
//! - `scopes: vec![Scope::Full]` (Cloud will populate from the OAuth `scope` claim)
//! - `source: AuthSource::Static` (Cloud / OIDC will set `AuthSource::Oidc`)
//!
//! The enums use `#[non_exhaustive]` so RFC 0001 step 1 / RFC 0004 can
//! add variants without breaking exhaustive matches in callers.

use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;

use color_eyre::eyre::{Result, bail};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::graph_id::GraphId;

/// Maximum length of a `TenantId` value.
pub const TENANT_ID_MAX_LEN: usize = 64;

/// Cloud-mode tenant identifier. Validated with the same regex as
/// `GraphId` so the two interchange syntactically.
///
/// `None` in Cluster mode; Cloud mode (RFC 0003) sets `Some(...)` from
/// the OAuth `org_id` claim. Constructed only via `try_from` so callers
/// cannot bypass validation.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize)]
#[serde(transparent)]
pub struct TenantId(String);

impl TenantId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for TenantId {
    type Error = color_eyre::eyre::Error;

    fn try_from(value: String) -> Result<Self> {
        validate_tenant_id(value.as_str())?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for TenantId {
    type Error = color_eyre::eyre::Error;

    fn try_from(value: &str) -> Result<Self> {
        validate_tenant_id(value)?;
        Ok(Self(value.to_string()))
    }
}

impl<'de> Deserialize<'de> for TenantId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_from(s).map_err(serde::de::Error::custom)
    }
}

fn validate_tenant_id(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("tenant_id must not be empty");
    }
    if value.len() > TENANT_ID_MAX_LEN {
        bail!(
            "tenant_id '{}' is {} chars; max {}",
            value,
            value.len(),
            TENANT_ID_MAX_LEN
        );
    }
    if !tenant_id_regex().is_match(value) {
        bail!(
            "tenant_id '{}' must match ^[a-zA-Z0-9-]{{1,64}}$",
            value
        );
    }
    Ok(())
}

fn tenant_id_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-zA-Z0-9-]{1,64}$").expect("regex literal"))
}

/// Registry HashMap key. Cluster mode populates `tenant_id: None`;
/// Cloud mode (RFC 0003) populates `tenant_id: Some(...)`.
///
/// The `Option<TenantId>` field is the **single forward-compatibility seam**
/// between Cluster and Cloud modes. Every handler reaches the engine via
/// `state.registry.get(&key)` — the key shape stays stable, so handlers
/// don't get re-touched when Cloud mode lands.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct GraphKey {
    pub tenant_id: Option<TenantId>,
    pub graph_id: GraphId,
}

impl GraphKey {
    /// Cluster-mode constructor (`tenant_id: None`).
    pub fn cluster(graph_id: GraphId) -> Self {
        Self {
            tenant_id: None,
            graph_id,
        }
    }

    /// Cloud-mode constructor — reserved for RFC 0003; included here so
    /// the seam is visible even though no Cluster-mode code path calls it.
    pub fn cloud(tenant_id: TenantId, graph_id: GraphId) -> Self {
        Self {
            tenant_id: Some(tenant_id),
            graph_id,
        }
    }
}

impl fmt::Display for GraphKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.tenant_id {
            Some(t) => write!(f, "{}/{}", t, self.graph_id),
            None => write!(f, "{}", self.graph_id),
        }
    }
}

/// Authorization scope. Cluster mode: every authenticated actor gets
/// `Scope::Full`. Cloud mode (RFC 0004) adds OAuth-style scopes via the
/// dashboard-configured `graph:read`, `graph:write`, `graph:admin`,
/// `graph:*` set; those become additional variants here.
///
/// `#[non_exhaustive]` so RFC 0004 can extend without breaking matches.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum Scope {
    /// Full access. The Cluster-mode default — every authenticated actor
    /// has unrestricted access subject to Cedar policy.
    Full,
}

/// How the actor was authenticated. Cluster mode: every actor authenticates
/// via the existing SHA-256 hash compare against a static token set, so
/// `AuthSource::Static`. RFC 0001 step 1 adds `AuthSource::Oidc` when the
/// `OidcJwtVerifier` ships.
///
/// `#[non_exhaustive]` so RFC 0001 can extend without breaking matches.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum AuthSource {
    /// Authenticated via the static bearer-token hash table.
    Static,
}

/// Server-resolved actor identity. Replaces the previous
/// `AuthenticatedActor(Arc<str>)` from `lib.rs`.
///
/// The fields are populated by `authenticate_bearer_token` after a successful
/// constant-time hash match. **Clients cannot set any of these fields directly**
/// — this is the MR-731 invariant. See `authorize_request` in `lib.rs` for the
/// chokepoint that overwrites any client-supplied actor identity.
///
/// Cluster mode constructs this with `tenant_id: None`, `scopes: vec![Scope::Full]`,
/// `source: AuthSource::Static` via the convenience constructor below.
#[derive(Debug, Clone)]
pub struct ResolvedActor {
    pub actor_id: Arc<str>,
    pub tenant_id: Option<TenantId>,
    pub scopes: Vec<Scope>,
    pub source: AuthSource,
}

impl ResolvedActor {
    /// Cluster-mode constructor — Static auth, no tenant, Full scope.
    /// Used by `authenticate_bearer_token` after a successful hash match.
    pub fn cluster_static(actor_id: Arc<str>) -> Self {
        Self {
            actor_id,
            tenant_id: None,
            scopes: vec![Scope::Full],
            source: AuthSource::Static,
        }
    }

    /// View the actor identifier as `&str`. Stable across the Cluster/Cloud
    /// boundary — Cedar always sees this value as the principal.
    pub fn actor_id_str(&self) -> &str {
        &self.actor_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_accepts_simple_values() {
        for ok in ["alpha", "tenant-001", "X", "01HZWA0KT0H0V0V0V0V0V0V0V0"] {
            TenantId::try_from(ok).unwrap_or_else(|_| panic!("expected accept: {ok}"));
        }
    }

    #[test]
    fn tenant_id_rejects_empty_and_over_max() {
        assert!(TenantId::try_from("").is_err());
        let too_long = "a".repeat(65);
        assert!(TenantId::try_from(too_long.as_str()).is_err());
    }

    #[test]
    fn tenant_id_rejects_path_traversal() {
        assert!(TenantId::try_from("../etc").is_err());
        assert!(TenantId::try_from("alpha/beta").is_err());
    }

    #[test]
    fn tenant_id_deserialize_runs_validation() {
        let bad: Result<TenantId, _> = serde_json::from_str("\"../evil\"");
        assert!(bad.is_err());
    }

    #[test]
    fn graph_key_cluster_constructor_sets_no_tenant() {
        let id = GraphId::try_from("alpha").unwrap();
        let key = GraphKey::cluster(id.clone());
        assert!(key.tenant_id.is_none());
        assert_eq!(key.graph_id, id);
    }

    #[test]
    fn graph_key_cloud_constructor_sets_tenant() {
        let tenant = TenantId::try_from("acme").unwrap();
        let id = GraphId::try_from("alpha").unwrap();
        let key = GraphKey::cloud(tenant.clone(), id.clone());
        assert_eq!(key.tenant_id.as_ref(), Some(&tenant));
        assert_eq!(key.graph_id, id);
    }

    #[test]
    fn graph_key_displays_with_or_without_tenant() {
        let id = GraphId::try_from("alpha").unwrap();
        let cluster_key = GraphKey::cluster(id.clone());
        assert_eq!(format!("{cluster_key}"), "alpha");

        let tenant = TenantId::try_from("acme").unwrap();
        let cloud_key = GraphKey::cloud(tenant, id);
        assert_eq!(format!("{cloud_key}"), "acme/alpha");
    }

    #[test]
    fn graph_key_is_hashable_for_map_use() {
        use std::collections::HashMap;
        let a = GraphKey::cluster(GraphId::try_from("alpha").unwrap());
        let b = GraphKey::cluster(GraphId::try_from("alpha").unwrap());
        let mut m: HashMap<GraphKey, u32> = HashMap::new();
        m.insert(a, 1);
        assert_eq!(m.get(&b), Some(&1));
    }

    #[test]
    fn graph_key_distinguishes_tenants() {
        let id = GraphId::try_from("alpha").unwrap();
        let t1 = TenantId::try_from("acme").unwrap();
        let t2 = TenantId::try_from("globex").unwrap();
        let k1 = GraphKey::cloud(t1, id.clone());
        let k2 = GraphKey::cloud(t2, id);
        assert_ne!(k1, k2);
    }

    #[test]
    fn resolved_actor_cluster_defaults() {
        let actor = ResolvedActor::cluster_static(Arc::<str>::from("act-alice"));
        assert_eq!(actor.actor_id_str(), "act-alice");
        assert!(actor.tenant_id.is_none());
        assert_eq!(actor.scopes, vec![Scope::Full]);
        assert_eq!(actor.source, AuthSource::Static);
    }

    #[test]
    fn scope_and_auth_source_are_non_exhaustive() {
        // Regression: keep the `#[non_exhaustive]` annotation. If someone
        // removes it, this test still passes (matches are still legal); it's
        // the cross-crate compile that catches it. Document the contract here.
        let _scope = Scope::Full;
        let _src = AuthSource::Static;
    }
}
