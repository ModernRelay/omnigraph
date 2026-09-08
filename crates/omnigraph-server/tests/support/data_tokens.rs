//! Shared signed-credential transport fixture. Its fixed key is test-only.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use omnigraph_server::data_tokens::DataTokenTrust;
use p256::ecdsa::{Signature, SigningKey, signature::Signer};
use serde_json::{Value, json};

pub struct DataTokens {
    pub trust: DataTokenTrust,
    pub actor: String,
    pub document: Value,
    fixture: Value,
}

impl DataTokens {
    pub fn new() -> Self {
        let fixture: Value =
            serde_json::from_str(include_str!("../fixtures/data-token-v1.json")).unwrap();
        let document = json!({
            "version":1,"canonical_root":"file:///fixture",
            "account_id":fixture["claims"]["account_id"],"cluster_id":fixture["claims"]["cluster_id"],
            "cluster_incarnation":fixture["claims"]["cluster_incarnation"],
            "issuer":fixture["claims"]["iss"],"audience":fixture["claims"]["aud"],
            "keys":[{"kid":fixture["public_key_sha256"],"public_key_pem":fixture["public_key_pem"]}]
        });
        Self {
            trust: DataTokenTrust::from_json(
                &serde_json::to_vec(&document).unwrap(),
                "file:///fixture",
            )
            .unwrap(),
            actor: format!("principal:{}", fixture["claims"]["sub"].as_str().unwrap()),
            document,
            fixture,
        }
    }

    pub fn token(&self, grants: Value) -> String {
        let mut claims = self.fixture["claims"].clone();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        claims["iat"] = json!(now);
        claims["exp"] = json!(now + 3600);
        claims["grants"] = grants;
        let content = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(self.fixture["header"].to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        let key = SigningKey::from_slice(&[7; 32]).unwrap();
        let signature: Signature = key.sign(content.as_bytes());
        format!("{content}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
    }
}
