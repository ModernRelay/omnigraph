//! Offline OAuth resource identity. Public snapshots carry identity admission,
//! never graph permissions. The initial serving binding cannot be refreshed.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use color_eyre::eyre::{Result, bail, eyre};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::identity::AuthenticatedActor;

pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024;
pub const MAX_TOKEN_BYTES: usize = 16 * 1024;
pub const MAX_LIFETIME_SECONDS: i64 = 300;
pub const CLOCK_SKEW_SECONDS: i64 = 30;
pub const REFRESH_SECONDS: u64 = 5;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Binding {
    issuer: String,
    audience: String,
    organization_id: String,
    account_id: String,
    cluster_id: String,
    cluster_incarnation: String,
    canonical_root: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u8,
    revision: u64,
    generated_at: i64,
    expires_at: i64,
    issuer: String,
    audience: String,
    organization_id: String,
    account_id: String,
    cluster_id: String,
    cluster_incarnation: String,
    canonical_root: String,
    keys: Vec<PublicKey>,
    principals: Vec<Admission>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicKey {
    kid: String,
    kty: String,
    alg: String,
    #[serde(rename = "use")]
    usage: String,
    n: String,
    e: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    subject: String,
    principal_id: String,
}

struct Snapshot {
    binding: Binding,
    revision: u64,
    generated_at: i64,
    expires_at: i64,
    digest: [u8; 32],
    keys: BTreeMap<String, DecodingKey>,
    principals: BTreeMap<String, String>,
}

/// An identity established by signature, exact resource and local admission.
/// Its fields cannot be supplied as request authority by a caller.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedIdentity {
    pub principal_id: String,
}

/// Public trust validated against the same canonical root as the serving
/// snapshot. Updating the projected file never changes that boot binding.
pub struct OidcIdentityTrust {
    path: PathBuf,
    current: ArcSwap<Snapshot>,
}

impl std::fmt::Debug for OidcIdentityTrust {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OidcIdentityTrust")
    }
}

fn bounded(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

fn identifier(value: &str) -> bool {
    bounded(value, 128)
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

fn https_uri(value: &str) -> bool {
    bounded(value, 2048)
        && url::Url::parse(value).is_ok_and(|u| {
            u.scheme() == "https"
                && u.host_str().is_some()
                && u.username().is_empty()
                && u.password().is_none()
                && u.query().is_none()
                && u.fragment().is_none()
                && (u.as_str() == value
                    || (u.path() == "/" && u.origin().ascii_serialization() == value))
        })
}

fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        bail!("OIDC identity trust must be a regular file");
    }
    let mut bytes = Vec::new();
    file.take((MAX_SNAPSHOT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn timestamp() -> Option<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
}

impl Snapshot {
    fn parse(bytes: &[u8], expected_root: &str, now: i64) -> Result<Self> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            bail!("OIDC identity snapshot exceeds 256 KiB");
        }
        let doc: Document = serde_json::from_slice(bytes)?;
        if doc.version != 1
            || doc.revision == 0
            || doc.generated_at <= 0
            || doc.generated_at > now.saturating_add(CLOCK_SKEW_SECONDS)
            || doc.expires_at <= now
            || doc.expires_at <= doc.generated_at
            || doc.expires_at.saturating_sub(doc.generated_at) > MAX_LIFETIME_SECONDS
            || doc.canonical_root != expected_root
            || !bounded(expected_root, 4096)
            || !https_uri(&doc.issuer)
            || !https_uri(&doc.audience)
            || !bounded(&doc.organization_id, 512)
            || !identifier(&doc.account_id)
            || !identifier(&doc.cluster_id)
            || !identifier(&doc.cluster_incarnation)
            || !(1..=4).contains(&doc.keys.len())
            || doc.principals.len() > 1000
        {
            bail!("invalid OIDC identity snapshot or serving-root binding");
        }
        let mut keys = BTreeMap::new();
        for key in doc.keys {
            let modulus = URL_SAFE_NO_PAD.decode(&key.n)?;
            let exponent = URL_SAFE_NO_PAD.decode(&key.e)?;
            if !bounded(&key.kid, 128)
                || key.kty != "RSA"
                || key.alg != "RS256"
                || key.usage != "sig"
                || !(256..=512).contains(&modulus.len())
                || modulus.first().is_none_or(|b| b & 0x80 == 0)
                || !(exponent == [1, 0, 1] || exponent == [3])
                || keys.contains_key(&key.kid)
            {
                bail!("invalid or duplicate OIDC RSA signing key");
            }
            keys.insert(key.kid, DecodingKey::from_rsa_components(&key.n, &key.e)?);
        }
        let mut principals = BTreeMap::new();
        let mut ids = BTreeSet::new();
        for principal in doc.principals {
            if !bounded(&principal.subject, 512)
                || !identifier(&principal.principal_id)
                || !ids.insert(principal.principal_id.clone())
                || principals
                    .insert(principal.subject, principal.principal_id)
                    .is_some()
            {
                bail!("invalid or conflicting OIDC principal admission");
            }
        }
        Ok(Self {
            binding: Binding {
                issuer: doc.issuer,
                audience: doc.audience,
                organization_id: doc.organization_id,
                account_id: doc.account_id,
                cluster_id: doc.cluster_id,
                cluster_incarnation: doc.cluster_incarnation,
                canonical_root: doc.canonical_root,
            },
            revision: doc.revision,
            generated_at: doc.generated_at,
            expires_at: doc.expires_at,
            digest: Sha256::digest(bytes).into(),
            keys,
            principals,
        })
    }
}

impl OidcIdentityTrust {
    pub fn read(path: &Path, expected_root: &str) -> Result<Arc<Self>> {
        Self::read_at(
            path,
            expected_root,
            timestamp().ok_or_else(|| eyre!("invalid clock"))?,
        )
    }

    fn read_at(path: &Path, expected_root: &str, now: i64) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            path: path.to_owned(),
            current: ArcSwap::from_pointee(Snapshot::parse(
                &read_bytes(path)?,
                expected_root,
                now,
            )?),
        }))
    }

    /// Public metadata only; resource registration is independent of routing.
    pub fn resource_metadata(&self) -> Value {
        let snapshot = self.current.load();
        serde_json::json!({"resource":snapshot.binding.audience,
            "authorization_servers":[snapshot.binding.issuer],
            "bearer_methods_supported":["header"]})
    }

    /// Local refresh never extends stale authority or changes the initial binding.
    pub fn refresh_at(&self, now: i64) -> Result<()> {
        let before = self.current.load_full();
        let next = Snapshot::parse(
            &read_bytes(&self.path)?,
            &before.binding.canonical_root,
            now,
        )?;
        if next.binding != before.binding
            || next.revision < before.revision
            || next.generated_at < before.generated_at
            || (next.revision == before.revision && next.digest != before.digest)
        {
            bail!("OIDC identity refresh changed its binding or replayed a revision");
        }
        if next.revision > before.revision {
            // Single background publisher. Compare-and-swap also protects embedders
            // calling refresh concurrently from installing an older result last.
            let previous = self.current.compare_and_swap(&before, Arc::new(next));
            if !Arc::ptr_eq(&previous, &before) {
                bail!("OIDC identity refresh raced with a newer snapshot");
            }
        }
        Ok(())
    }

    pub(crate) fn start_refresh(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(REFRESH_SECONDS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(trust) = weak.upgrade() else {
                    break;
                };
                let Some(now) = timestamp() else {
                    continue;
                };
                let result = tokio::task::spawn_blocking(move || trust.refresh_at(now)).await;
                if !matches!(result, Ok(Ok(()))) {
                    tracing::warn!(
                        "OIDC identity refresh refused; original expiry remains enforced"
                    );
                }
            }
        });
    }

    pub(crate) fn verify_at(&self, token: &str, now: i64) -> Option<AuthenticatedActor> {
        if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
            return None;
        }
        let snapshot = self.current.load();
        if now < 0
            || snapshot.expires_at <= now
            || snapshot.generated_at > now.saturating_add(CLOCK_SKEW_SECONDS)
        {
            return None;
        }
        let header = decode_header(token).ok()?;
        if header.alg != Algorithm::RS256
            || header.crit.is_some()
            || header.enc.is_some()
            || header.zip.is_some()
            || header
                .typ
                .as_deref()
                .is_some_and(|t| !matches!(t, "JWT" | "at+jwt"))
        {
            return None;
        }
        let key = snapshot.keys.get(header.kid.as_deref()?)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.leeway = 0;
        // Explicit clock makes expiry tests deterministic and shares one instant
        // with the snapshot check; signature/issuer/audience still use the library.
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.set_required_spec_claims(&["iss", "sub", "aud", "iat", "exp"]);
        validation.set_issuer(&[&snapshot.binding.issuer]);
        validation.set_audience(&[&snapshot.binding.audience]);
        let claims = decode::<Value>(token, key, &validation).ok()?.claims;
        if claims.get("iss")?.as_str()? != snapshot.binding.issuer
            || claims.get("aud")?.as_str()? != snapshot.binding.audience
            || claims.get("org_id")?.as_str()? != snapshot.binding.organization_id
            || !bounded(claims.get("client_id")?.as_str()?, 512)
            || claims.get("act").is_some_and(|v| !v.is_null())
            || claims.get("impersonator").is_some_and(|v| !v.is_null())
            || claims
                .get("sub_profile")
                .is_some_and(|v| v.as_str() != Some("user"))
        {
            return None;
        }
        let iat = claims.get("iat")?.as_i64()?;
        let exp = claims.get("exp")?.as_i64()?;
        if iat < 0
            || iat > now.saturating_add(CLOCK_SKEW_SECONDS)
            || exp <= now
            || exp <= iat
            || exp.checked_sub(iat)? > MAX_LIFETIME_SECONDS
            || claims
                .get("nbf")
                .is_some_and(|v| v.as_i64().is_none_or(|n| n > now))
        {
            return None;
        }
        let subject = claims.get("sub")?.as_str()?;
        let principal_id = snapshot.principals.get(subject)?.clone();
        Some(AuthenticatedActor::oidc_identity(VerifiedIdentity {
            principal_id,
        }))
    }
}

#[cfg(test)]
#[path = "oidc_identity_tests.rs"]
mod tests;
