//! Shared OIDC resource-identity fixture. Its fixed key is test-only.

use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use omnigraph_server::oidc_identity::OidcIdentityTrust;
use rsa::{RsaPrivateKey, pkcs8::DecodePrivateKey as _, traits::PublicKeyParts as _};
use serde_json::json;

const KEY: &str = include_str!("../fixtures/oidc-test-key.pem");
pub const RESOURCE: &str = "https://data.example/clusters/A/incarnations/one";
/// The protected-resource metadata URL the challenge advertises for [`RESOURCE`].
pub const RESOURCE_METADATA: &str =
    "https://data.example/.well-known/oauth-protected-resource/clusters/A/incarnations/one";
const ISSUER: &str = "https://identity.example";

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn token(subject: &str, audience: &str) -> String {
    let now = now();
    let claims = json!({"iss":ISSUER,"aud":audience,"org_id":"org_example",
        "client_id":"oauth-client","sub":subject,"iat":now,"exp":now+300,
        "roles":["admin"],"permissions":["everything"]});
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("key-1".into());
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(KEY.as_bytes()).unwrap(),
    )
    .unwrap()
}

pub fn trust(root: &std::path::Path) -> Arc<OidcIdentityTrust> {
    let now = now();
    let key = RsaPrivateKey::from_pkcs8_pem(KEY).unwrap();
    let canonical_root = format!("file://{}", root.display());
    let document = json!({"version":1,"revision":1,"generated_at":now,"expires_at":now+300,
        "issuer":ISSUER,"audience":RESOURCE,"organization_id":"org_example","account_id":"account_1",
        "cluster_id":"A","cluster_incarnation":"one","canonical_root":canonical_root,
        "keys":[{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig",
            "n":URL_SAFE_NO_PAD.encode(key.n().to_bytes_be()),"e":URL_SAFE_NO_PAD.encode(key.e().to_bytes_be())}],
        "principals":[{"subject":"alice","principal_id":"stable_alice"},{"subject":"bob","principal_id":"stable_bob"},
            {"subject":"read_only","principal_id":"stable_reader"},{"subject":"change_only","principal_id":"stable_changer"}]});
    let path = root.join("oidc-public.json");
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    OidcIdentityTrust::read(&path, &canonical_root).unwrap()
}
