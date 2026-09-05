use super::*;
use p256::ecdsa::{SigningKey, signature::Signer};
use serde_json::{Value, json};

fn golden() -> Value {
    serde_json::from_str(include_str!("../../tests/fixtures/data-token-v1.json")).unwrap()
}

fn document() -> Value {
    let fixture = golden();
    json!({
        "version": 1, "canonical_root": "file:///fixture",
        "account_id": fixture["claims"]["account_id"],
        "cluster_id": fixture["claims"]["cluster_id"],
        "cluster_incarnation": fixture["claims"]["cluster_incarnation"],
        "issuer": fixture["claims"]["iss"], "audience": fixture["claims"]["aud"],
        "keys": [{"kid": fixture["public_key_sha256"], "public_key_pem": fixture["public_key_pem"]}]
    })
}

fn trust() -> DataTokenTrust {
    DataTokenTrust::from_json(&serde_json::to_vec(&document()).unwrap(), "file:///fixture").unwrap()
}

fn sign_raw(header: &str, claims: &str) -> String {
    let key = SigningKey::from_slice(&[7; 32]).unwrap();
    let content = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header),
        URL_SAFE_NO_PAD.encode(claims)
    );
    let signature: Signature = key.sign(content.as_bytes());
    format!("{content}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

fn sign(claims: &Value) -> String {
    sign_raw(&golden()["header"].to_string(), &claims.to_string())
}

#[test]
fn issuer_golden_signature_and_per_graph_ceiling() {
    let fixture = golden();
    let trust = trust();
    let now = fixture["verification_time"].as_u64().unwrap();
    let mut actor = trust
        .verify_at(fixture["token"].as_str().unwrap(), now)
        .unwrap();
    assert_eq!(
        actor.actor_id_str(),
        format!("principal:{}", fixture["claims"]["sub"].as_str().unwrap())
    );
    assert_eq!(actor.source, AuthSource::SignedData);
    assert_eq!(
        serde_json::to_value(actor.data_claims().unwrap()).unwrap(),
        fixture["claims"]
    );
    assert!(actor.select_graph(&GraphId::try_from("graph-a").unwrap()));
    assert!(actor.permits_action(PolicyAction::Change));
    assert!(actor.select_graph(&GraphId::try_from("reports").unwrap()));
    assert!(actor.permits_action(PolicyAction::Read));
    assert!(!actor.permits_action(PolicyAction::Change));
    assert!(!actor.select_graph(&GraphId::try_from("other").unwrap()));
    // Tokens are reusable capabilities, not an invented one-use token ledger.
    assert!(
        trust
            .verify_at(fixture["token"].as_str().unwrap(), now)
            .is_some()
    );
}

#[test]
fn signed_profile_rejects_invalid_authority_and_unsupported_claims() {
    let fixture = golden();
    let now = fixture["verification_time"].as_u64().unwrap();
    let trust = trust();
    for (field, value) in [
        ("version", json!(2)),
        ("iss", json!("https://other.example")),
        ("aud", json!([fixture["claims"]["aud"]])),
        ("account_id", json!("other")),
        ("cluster_id", json!("other")),
        ("cluster_incarnation", json!("other")),
        ("sub", json!("email@example.com")),
        ("sub", json!("a".repeat(129))),
        ("jti", json!("")),
        ("principal_kind", json!("development")),
        ("assurance", json!("verified_workload")),
        ("iat", json!(now + 31)),
        ("iat", json!(-1)),
        ("exp", json!(now)),
        ("nbf", json!(now + 1)),
        ("actor", json!("admin")),
        ("grants", json!([])),
        (
            "grants",
            json!([{"graph_id":"graph-a","actions":["admin"]}]),
        ),
        (
            "grants",
            json!([{"graph_id":"graph-a","actions":["schema_apply"]}]),
        ),
        (
            "grants",
            json!([{"graph_id":"graph-a","actions":["read","read"]}]),
        ),
        ("grants", json!([{"graph_id":"graph-a","actions":["*"]}])),
        ("grants", json!([{"graph_id":"graph_a","actions":["read"]}])),
        (
            "grants",
            json!([{"graph_id":"policies","actions":["read"]}]),
        ),
    ] {
        let mut claims = fixture["claims"].clone();
        claims[field] = value;
        assert!(
            trust.verify_at(&sign(&claims), now).is_none(),
            "accepted {field}: {claims}"
        );
    }
    let mut claims = fixture["claims"].clone();
    let duplicate = claims["grants"][0].clone();
    claims["grants"].as_array_mut().unwrap().push(duplicate);
    assert!(trust.verify_at(&sign(&claims), now).is_none());
    claims["grants"] = Value::Array(
        (0..65)
            .map(|i| json!({"graph_id":format!("g{i}"),"actions":["read"]}))
            .collect(),
    );
    assert!(trust.verify_at(&sign(&claims), now).is_none());
    let duplicate = fixture["claims"]
        .to_string()
        .replacen('{', "{\"version\":1,", 1);
    assert!(
        trust
            .verify_at(&sign_raw(&fixture["header"].to_string(), &duplicate), now)
            .is_none()
    );
}

#[test]
fn exact_time_limits_and_truthful_automation() {
    let fixture = golden();
    let now = fixture["verification_time"].as_u64().unwrap();
    let trust = trust();
    for (ttl, accepted) in [(59, false), (60, true), (86_400, true), (86_401, false)] {
        let mut claims = fixture["claims"].clone();
        claims["iat"] = json!(now);
        claims["exp"] = json!(now + ttl);
        assert_eq!(trust.verify_at(&sign(&claims), now).is_some(), accepted);
    }
    let mut claims = fixture["claims"].clone();
    claims["iat"] = json!(now + 30);
    claims["exp"] = json!(now + 90);
    claims["principal_kind"] = json!("automation");
    claims["assurance"] = json!("verified_workload");
    assert!(trust.verify_at(&sign(&claims), now).is_some());
    assert!(trust.verify_at(&sign(&claims), now + 90).is_none());
    claims["exp"] = json!(now + 86_430);
    assert!(
        trust.verify_at(&sign(&claims), now).is_some(),
        "maximum signed lifetime permits 30 seconds of issuance-clock skew"
    );
    assert!(
        trust.verify_at(&sign(&claims), now + 86_430).is_none(),
        "expiry has zero leeway even with a future issuance time"
    );
    claims["exp"] = json!(now + 86_431);
    assert!(trust.verify_at(&sign(&claims), now).is_none());
    claims["iat"] = json!(u64::MAX);
    assert!(trust.verify_at(&sign(&claims), now).is_none());
}

#[test]
fn jose_header_signature_and_size_are_strict() {
    let fixture = golden();
    let now = fixture["verification_time"].as_u64().unwrap();
    let trust = trust();
    for (field, value) in [
        ("alg", json!("none")),
        ("alg", json!("HS256")),
        ("typ", json!("other")),
        ("kid", json!("0".repeat(64))),
        ("jku", json!("https://attacker.example")),
        ("crit", json!([])),
    ] {
        let mut header = fixture["header"].clone();
        header[field] = value;
        assert!(
            trust
                .verify_at(
                    &sign_raw(&header.to_string(), &fixture["claims"].to_string()),
                    now
                )
                .is_none()
        );
    }
    let token = fixture["token"].as_str().unwrap();
    let mut parts: Vec<_> = token.split('.').map(str::to_string).collect();
    parts[2] = URL_SAFE_NO_PAD.encode([0; 64]);
    assert!(trust.verify_at(&parts.join("."), now).is_none());
    assert!(trust.verify_at(&format!("{token}.extra"), now).is_none());
    assert!(
        trust
            .verify_at(&"x".repeat(MAX_TOKEN_BYTES + 1), now)
            .is_none()
    );
    let duplicate = fixture["header"]
        .to_string()
        .replacen('{', "{\"alg\":\"ES256\",", 1);
    assert!(
        trust
            .verify_at(&sign_raw(&duplicate, &fixture["claims"].to_string()), now)
            .is_none()
    );
}

#[test]
fn trust_bounds_fingerprints_origins_and_root_are_checked() {
    let document = document();
    for (field, value) in [
        ("version", json!(2)),
        ("canonical_root", json!("file:///other")),
        ("canonical_root", json!("x".repeat(4097))),
        ("issuer", json!("https://api.example.test/")),
        ("issuer", json!("http://public.example")),
        ("issuer", json!("https://a".to_string() + &"x".repeat(2048))),
        ("issuer", json!("https://api.example.test/path")),
        ("keys", json!([])),
        ("account_id", json!("a".repeat(129))),
        ("unknown", json!(true)),
    ] {
        let mut bad = document.clone();
        bad[field] = value;
        assert!(
            DataTokenTrust::from_json(&serde_json::to_vec(&bad).unwrap(), "file:///fixture")
                .is_err(),
            "accepted {field}"
        );
    }
    let mut bad = document.clone();
    bad["keys"][0]["kid"] = json!("0".repeat(64));
    assert!(
        DataTokenTrust::from_json(&serde_json::to_vec(&bad).unwrap(), "file:///fixture").is_err()
    );
    bad["keys"] = json!([document["keys"][0], document["keys"][0]]);
    assert!(
        DataTokenTrust::from_json(&serde_json::to_vec(&bad).unwrap(), "file:///fixture").is_err()
    );
    assert!(
        DataTokenTrust::from_json(&vec![b' '; MAX_TRUST_BYTES + 1], "file:///fixture").is_err()
    );
    for origin in [
        "http://localhost:8080",
        "http://127.0.0.1:80",
        "http://[::1]:8080",
    ] {
        let mut local = document.clone();
        local["issuer"] = json!(origin);
        // The explicit default port is not a canonical origin.
        assert_eq!(
            DataTokenTrust::from_json(&serde_json::to_vec(&local).unwrap(), "file:///fixture")
                .is_ok(),
            !origin.ends_with(":80")
        );
    }
}

#[test]
fn rotation_accepts_each_installed_key_without_online_discovery() {
    let mut document = document();
    let key = SigningKey::from_slice(&[8; 32]).unwrap();
    let der = key.verifying_key().to_public_key_der().unwrap();
    let kid = format!("{:x}", Sha256::digest(der.as_bytes()));
    let pem = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    document["keys"]
        .as_array_mut()
        .unwrap()
        .push(json!({"kid":kid,"public_key_pem":pem}));
    let trust =
        DataTokenTrust::from_json(&serde_json::to_vec(&document).unwrap(), "file:///fixture")
            .unwrap();
    let fixture = golden();
    let now = fixture["verification_time"].as_u64().unwrap();
    assert!(
        trust
            .verify_at(fixture["token"].as_str().unwrap(), now)
            .is_some()
    );
    let mut header = fixture["header"].clone();
    header["kid"] = json!(kid);
    let content = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(fixture["claims"].to_string())
    );
    let signature: Signature = key.sign(content.as_bytes());
    assert!(
        trust
            .verify_at(
                &format!("{content}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes())),
                now
            )
            .is_some()
    );
}

#[test]
fn action_ceiling_has_no_implicit_permissions() {
    let fixture = golden();
    let trust = trust();
    let now = fixture["verification_time"].as_u64().unwrap();
    let actions = [
        PolicyAction::Read,
        PolicyAction::Change,
        PolicyAction::Export,
        PolicyAction::BranchCreate,
        PolicyAction::BranchDelete,
        PolicyAction::BranchMerge,
        PolicyAction::InvokeQuery,
        PolicyAction::GraphList,
    ];
    for allowed in actions {
        let mut claims = fixture["claims"].clone();
        claims["grants"] = json!([{"graph_id":"graph-a","actions":[allowed]}]);
        let mut actor = trust.verify_at(&sign(&claims), now).unwrap();
        assert!(actor.select_graph(&GraphId::try_from("graph-a").unwrap()));
        for checked in actions {
            assert_eq!(
                actor.permits_action(checked),
                allowed == checked,
                "{allowed} vs {checked}"
            );
        }
        assert!(!actor.permits_action(PolicyAction::SchemaApply));
        assert!(!actor.permits_action(PolicyAction::Admin));
    }
}

#[test]
fn trust_file_reads_are_bounded_and_allow_projected_regular_files() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("trust.json");
    std::fs::write(&path, serde_json::to_vec(&document()).unwrap()).unwrap();
    assert!(DataTokenTrust::read(&path, "file:///fixture").is_ok());
    assert!(DataTokenTrust::read(temp.path(), "file:///fixture").is_err());
    #[cfg(unix)]
    {
        let link = temp.path().join("projected.json");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(DataTokenTrust::read(&link, "file:///fixture").is_ok());
        let fifo = temp.path().join("fifo");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());
        assert!(DataTokenTrust::read(&fifo, "file:///fixture").is_err());
    }
    std::fs::write(&path, vec![b' '; MAX_TRUST_BYTES + 1]).unwrap();
    assert!(DataTokenTrust::read(&path, "file:///fixture").is_err());
    let mut bad = document();
    bad["keys"][0]["public_key_pem"] = json!("not a P-256 public key");
    assert!(
        DataTokenTrust::from_json(&serde_json::to_vec(&bad).unwrap(), "file:///fixture").is_err()
    );
    // Valid SPKI for a different EC curve is still outside this fixed profile.
    bad["keys"][0]["public_key_pem"] = json!(concat!(
        "-----BEGIN PUBLIC KEY-----\n",
        "MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAENZvc2bpMEtW1sf4GV905mNLjJzTsoLAN\n",
        "QAt0OJTcapKwgYfcT7og3GSfKZf5H/WCIJCtfA1hNAHNck2uKqFeOCPcY/Ci6QV8\n",
        "79TGgOXMmHNQC0W1kcZRixyx2ffP5h4/\n",
        "-----END PUBLIC KEY-----\n"
    ));
    let der = base64::engine::general_purpose::STANDARD
        .decode(concat!(
            "MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAENZvc2bpMEtW1sf4GV905mNLjJzTsoLAN",
            "QAt0OJTcapKwgYfcT7og3GSfKZf5H/WCIJCtfA1hNAHNck2uKqFeOCPcY/Ci6QV8",
            "79TGgOXMmHNQC0W1kcZRixyx2ffP5h4/"
        ))
        .unwrap();
    bad["keys"][0]["kid"] = json!(format!("{:x}", Sha256::digest(der)));
    assert!(
        DataTokenTrust::from_json(&serde_json::to_vec(&bad).unwrap(), "file:///fixture").is_err()
    );
    let duplicate = document().to_string().replacen('{', "{\"version\":1,", 1);
    assert!(DataTokenTrust::from_json(duplicate.as_bytes(), "file:///fixture").is_err());
}
