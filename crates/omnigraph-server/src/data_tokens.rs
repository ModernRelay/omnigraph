//! RFC 0053: bounded offline data credentials. Public trust is fixed at boot;
//! signature verification never performs network I/O or creates policy actors.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use color_eyre::eyre::{Result, bail, eyre};
use omnigraph_policy::PolicyAction;
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use p256::pkcs8::{DecodePublicKey, EncodePublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::graph_id::GraphId;
use crate::identity::{AuthSource, ResolvedActor, Scope};

pub const MAX_TOKEN_BYTES: usize = 8_192;
pub const MAX_TRUST_BYTES: usize = 65_536;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataGrant {
    pub graph_id: GraphId,
    pub actions: Vec<PolicyAction>,
}

/// Only authenticated, non-development principal classes enter this profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Human,
    Automation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataAssurance {
    VerifiedHuman,
    VerifiedWorkload,
}

/// Parsed claims are authenticated only after `DataTokenTrust::verify_at`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataTokenClaims {
    pub version: u8,
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub account_id: String,
    pub cluster_id: String,
    pub cluster_incarnation: String,
    pub principal_kind: PrincipalKind,
    pub assurance: DataAssurance,
    pub iat: u64,
    pub exp: u64,
    pub jti: String,
    pub grants: Vec<DataGrant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataTokenHeader {
    pub typ: String,
    pub alg: String,
    pub kid: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustDocument {
    version: u8,
    canonical_root: String,
    account_id: String,
    cluster_id: String,
    cluster_incarnation: String,
    issuer: String,
    audience: String,
    keys: Vec<TrustKey>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustKey {
    kid: String,
    public_key_pem: String,
}

/// Validated immutable public trust. Private key material never enters it.
#[derive(Debug, Clone)]
pub struct DataTokenTrust {
    account_id: String,
    cluster_id: String,
    cluster_incarnation: String,
    issuer: String,
    audience: String,
    keys: Vec<(String, VerifyingKey)>,
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
}

fn valid_kid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn valid_origin(value: &str) -> bool {
    if value.is_empty() || value.len() > 2_048 {
        return false;
    }
    let Ok(url) = Url::parse(value) else {
        return false;
    };
    url.username().is_empty()
        && url.password().is_none()
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none()
        && url.host().is_some()
        && url.origin().ascii_serialization() == value
        && (url.scheme() == "https"
            || (url.scheme() == "http"
                && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))))
}

impl DataTokenTrust {
    /// Read a bounded regular public file (projected-volume symlinks are allowed).
    /// `expected_root` comes from the same Core serving snapshot being booted.
    pub fn read(path: &Path, expected_root: &str) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NONBLOCK);
        }
        let file = options.open(path)?;
        if !file.metadata()?.is_file() {
            bail!("data-token trust must be a regular file");
        }
        let mut bytes = Vec::new();
        file.take((MAX_TRUST_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        Self::from_json(&bytes, expected_root)
    }

    /// Validate all trust entries and their binding before any graph engine opens.
    pub fn from_json(bytes: &[u8], expected_root: &str) -> Result<Self> {
        if bytes.len() > MAX_TRUST_BYTES {
            bail!("data-token trust exceeds 64 KiB");
        }
        let document: TrustDocument = serde_json::from_slice(bytes)?;
        if document.version != 1
            || document.canonical_root.is_empty()
            || document.canonical_root.len() > 4_096
            || document.canonical_root != expected_root
            || !valid_id(&document.account_id)
            || !valid_id(&document.cluster_id)
            || !valid_id(&document.cluster_incarnation)
            || !valid_origin(&document.issuer)
            || document.audience != format!("urn:omnigraph:data:{}", document.cluster_id)
            || !(1..=4).contains(&document.keys.len())
        {
            bail!("invalid data-token trust or serving-root binding");
        }
        let mut seen = HashSet::new();
        let mut keys = Vec::with_capacity(document.keys.len());
        for entry in document.keys {
            let key = VerifyingKey::from_public_key_pem(&entry.public_key_pem)
                .map_err(|_| eyre!("data-token trust key must be ECDSA P-256 SPKI PEM"))?;
            let der = key.to_public_key_der()?;
            let fingerprint = format!("{:x}", Sha256::digest(der.as_bytes()));
            if !valid_kid(&entry.kid) || entry.kid != fingerprint || !seen.insert(entry.kid.clone())
            {
                bail!("data-token trust has an invalid or duplicate key fingerprint");
            }
            keys.push((entry.kid, key));
        }
        Ok(Self {
            account_id: document.account_id,
            cluster_id: document.cluster_id,
            cluster_incarnation: document.cluster_incarnation,
            issuer: document.issuer,
            audience: document.audience,
            keys,
        })
    }

    /// Verify using an explicit admission time. Failure is deliberately opaque:
    /// callers must not log a credential or expose its unverified claims.
    #[must_use]
    pub fn verify_at(&self, token: &str, now: u64) -> Option<ResolvedActor> {
        if token.len() > MAX_TOKEN_BYTES {
            return None;
        }
        let mut parts = token.split('.');
        let header_part = parts.next()?;
        let claims_part = parts.next()?;
        let signature_part = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        let header_bytes = URL_SAFE_NO_PAD.decode(header_part).ok()?;
        let header: DataTokenHeader = serde_json::from_slice(&header_bytes).ok()?;
        if header.typ != "JWT" || header.alg != "ES256" || !valid_kid(&header.kid) {
            return None;
        }
        let (_, key) = self.keys.iter().find(|(kid, _)| *kid == header.kid)?;
        let signature_bytes = URL_SAFE_NO_PAD.decode(signature_part).ok()?;
        let signature = Signature::from_slice(&signature_bytes).ok()?;
        key.verify(
            format!("{header_part}.{claims_part}").as_bytes(),
            &signature,
        )
        .ok()?;
        let claims_bytes = URL_SAFE_NO_PAD.decode(claims_part).ok()?;
        let claims: DataTokenClaims = serde_json::from_slice(&claims_bytes).ok()?;
        if !self.valid_claims(&claims, now) {
            return None;
        }
        Some(ResolvedActor {
            actor_id: Arc::from(format!("principal:{}", claims.sub)),
            tenant_id: None,
            scopes: vec![Scope::DataToken],
            source: AuthSource::SignedData,
            data_token: Some(Arc::new(claims)),
            selected_graph: None,
        })
    }

    fn valid_claims(&self, claims: &DataTokenClaims, now: u64) -> bool {
        let Some(ttl) = claims.exp.checked_sub(claims.iat) else {
            return false;
        };
        if claims.version != 1
            || claims.iss != self.issuer
            || claims.aud != self.audience
            || claims.account_id != self.account_id
            || claims.cluster_id != self.cluster_id
            || claims.cluster_incarnation != self.cluster_incarnation
            || !valid_id(&claims.sub)
            || !valid_id(&claims.jti)
            || !(60..=86_400).contains(&ttl)
            || claims.exp <= now
            || claims.iat > now.saturating_add(30)
            || !(1..=64).contains(&claims.grants.len())
            || !matches!(
                (claims.principal_kind, claims.assurance),
                (PrincipalKind::Human, DataAssurance::VerifiedHuman)
                    | (PrincipalKind::Automation, DataAssurance::VerifiedWorkload)
            )
        {
            return false;
        }
        let mut graphs = HashSet::new();
        claims.grants.iter().all(|grant| {
            let mut actions = HashSet::new();
            graphs.insert(grant.graph_id.as_str())
                && (1..=8).contains(&grant.actions.len())
                && grant.actions.iter().all(|action| {
                    matches!(
                        action,
                        PolicyAction::Read
                            | PolicyAction::Export
                            | PolicyAction::Change
                            | PolicyAction::BranchCreate
                            | PolicyAction::BranchDelete
                            | PolicyAction::BranchMerge
                            | PolicyAction::InvokeQuery
                            | PolicyAction::GraphList
                    ) && actions.insert(action)
                })
        })
    }
}

#[cfg(test)]
mod tests;
