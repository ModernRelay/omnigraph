use super::*;
use jsonwebtoken::{EncodingKey, Header, encode};
use rsa::{RsaPrivateKey, pkcs8::DecodePrivateKey as _, traits::PublicKeyParts as _};
use serde_json::json;

const NOW: i64 = 1_800_000_000;
const ROOT: &str = "s3://test/cluster-A";
const KEY: &str = include_str!("../tests/fixtures/oidc-test-key.pem");

fn document() -> Value {
    let key = RsaPrivateKey::from_pkcs8_pem(KEY).unwrap();
    json!({"version":1,"revision":1,"generated_at":NOW,"expires_at":NOW+300,
        "issuer":"https://identity.example", "audience":"https://data.example/clusters/A/incarnations/one",
        "organization_id":"org_example","account_id":"account_1","cluster_id":"A",
        "cluster_incarnation":"one","canonical_root":ROOT,
        "keys":[{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig",
            "n":URL_SAFE_NO_PAD.encode(key.n().to_bytes_be()),
            "e":URL_SAFE_NO_PAD.encode(key.e().to_bytes_be())}],
        "principals":[{"subject":"user_1","principal_id":"stable_1"}]})
}

fn claims() -> Value {
    json!({"iss":"https://identity.example",
        "aud":"https://data.example/clusters/A/incarnations/one",
        "org_id":"org_example","sub":"user_1","client_id":"oauth-client",
        "iat":NOW,"exp":NOW+300})
}

fn sign(claims: &Value) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("key-1".into());
    header.typ = None;
    encode(
        &header,
        claims,
        &EncodingKey::from_rsa_pem(KEY.as_bytes()).unwrap(),
    )
    .unwrap()
}

fn setup(doc: &Value) -> (tempfile::TempDir, Arc<OidcIdentityTrust>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trust.json");
    std::fs::write(&path, serde_json::to_vec(doc).unwrap()).unwrap();
    let trust = OidcIdentityTrust::read_at(&path, ROOT, NOW).unwrap();
    (dir, trust)
}

#[test]
fn identity_is_local_stable_admission_without_token_permissions() {
    let (_dir, trust) = setup(&document());
    let mut c = claims();
    c["roles"] = json!(["admin"]);
    c["permissions"] = json!(["everything"]);
    let actor = trust.verify_at(&sign(&c), NOW).unwrap();
    assert_eq!(actor.actor_id_str(), "principal:stable_1");
    assert!(actor.is_identity());
    assert!(actor.data_claims().is_none());
    assert!(
        actor.identity_claims().is_none(),
        "OIDC is a distinct authenticated profile"
    );
    assert_eq!(trust.resource_metadata()["resource"], c["aud"]);
}

#[test]
fn exact_recipient_identity_and_profile_are_required() {
    let (_dir, trust) = setup(&document());
    for (field, value) in [
        ("iss", json!("https://foreign.example")),
        ("aud", json!("environment-client")),
        (
            "aud",
            json!(["https://data.example/clusters/A/incarnations/one"]),
        ),
        (
            "aud",
            json!("https://data.example/clusters/A/incarnations/two"),
        ),
        ("org_id", json!("org_other")),
        ("sub", json!("user_unadmitted")),
        ("sub", json!("machine_client")),
        ("client_id", json!("")),
        ("client_id", json!(null)),
        ("act", json!({"sub":"agent"})),
        ("impersonator", json!({"email":"example@example.com"})),
        ("sub_profile", json!("agent")),
        ("sub_profile", json!(42)),
    ] {
        let mut c = claims();
        c[field] = value;
        assert!(
            trust.verify_at(&sign(&c), NOW).is_none(),
            "accepted {field}"
        );
    }
    for field in ["iss", "aud", "sub", "iat", "exp", "client_id", "org_id"] {
        let mut c = claims();
        c.as_object_mut().unwrap().remove(field);
        assert!(trust.verify_at(&sign(&c), NOW).is_none(), "missing {field}");
    }
}

#[test]
fn original_token_and_snapshot_deadlines_never_slide() {
    let (_dir, trust) = setup(&document());
    let token = sign(&claims());
    assert!(trust.verify_at(&token, NOW + 299).is_some());
    assert!(trust.verify_at(&token, NOW + 300).is_none());
    assert!(trust.verify_at(&token, NOW - 31).is_none());
    for (field, value) in [
        ("exp", json!(NOW + 301)),
        ("exp", json!(NOW)),
        ("iat", json!(NOW + 31)),
        ("iat", json!(-1)),
        ("iat", json!("bad")),
        ("nbf", json!(NOW + 1)),
        ("nbf", json!("bad")),
    ] {
        let mut c = claims();
        c[field] = value;
        assert!(
            trust.verify_at(&sign(&c), NOW).is_none(),
            "accepted {field}"
        );
    }
    // A freshly signed token cannot outlive expired public admission.
    let mut c = claims();
    c["iat"] = json!(NOW + 200);
    c["exp"] = json!(NOW + 500);
    assert!(trust.verify_at(&sign(&c), NOW + 300).is_none());
}

#[test]
fn rotation_and_signature_headers_never_bypass_the_published_keys() {
    let (dir, trust) = setup(&document());
    let old = sign(&claims());
    let key = EncodingKey::from_rsa_pem(KEY.as_bytes()).unwrap();
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("key-2".into());
    let rotated = encode(&header, &claims(), &key).unwrap();
    assert!(trust.verify_at(&rotated, NOW).is_none());
    let mut doc = document();
    doc["revision"] = json!(2);
    doc["keys"][0]["kid"] = json!("key-2");
    std::fs::write(
        dir.path().join("trust.json"),
        serde_json::to_vec(&doc).unwrap(),
    )
    .unwrap();
    trust.refresh_at(NOW).unwrap();
    assert!(trust.verify_at(&rotated, NOW).is_some());
    assert!(trust.verify_at(&old, NOW).is_none());

    header.crit = Some(vec!["unsupported".into()]);
    assert!(
        trust
            .verify_at(&encode(&header, &claims(), &key).unwrap(), NOW)
            .is_none()
    );
    header.crit = None;
    header.typ = Some("application/id-token".into());
    assert!(
        trust
            .verify_at(&encode(&header, &claims(), &key).unwrap(), NOW)
            .is_none()
    );
    // Keep the right header/key ID but alter signed claims without resigning.
    let mut parts: Vec<_> = rotated.split('.').map(String::from).collect();
    let mut altered = claims();
    altered["sub"] = json!("other");
    parts[1] = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&altered).unwrap());
    assert!(trust.verify_at(&parts.join("."), NOW).is_none());
}

#[test]
fn malformed_replayed_or_rebound_updates_do_not_replace_valid_trust() {
    let (dir, trust) = setup(&document());
    let token = sign(&claims());
    for field in [
        "issuer",
        "audience",
        "organization_id",
        "account_id",
        "cluster_id",
        "cluster_incarnation",
        "canonical_root",
    ] {
        let mut doc = document();
        doc["revision"] = json!(2);
        doc[field] = json!(match field {
            "issuer" | "audience" => "https://other.example",
            _ => "other",
        });
        std::fs::write(
            dir.path().join("trust.json"),
            serde_json::to_vec(&doc).unwrap(),
        )
        .unwrap();
        assert!(trust.refresh_at(NOW).is_err(), "accepted changed {field}");
        assert!(trust.verify_at(&token, NOW).is_some());
    }
    let mut doc = document();
    doc["principals"] = json!([]);
    std::fs::write(
        dir.path().join("trust.json"),
        serde_json::to_vec(&doc).unwrap(),
    )
    .unwrap();
    assert!(
        trust.refresh_at(NOW).is_err(),
        "same revision changed bytes"
    );
    doc["revision"] = json!(2);
    std::fs::write(
        dir.path().join("trust.json"),
        serde_json::to_vec(&doc).unwrap(),
    )
    .unwrap();
    trust.refresh_at(NOW).unwrap();
    assert!(
        trust.verify_at(&token, NOW).is_none(),
        "removed admission survived refresh"
    );
    std::fs::write(
        dir.path().join("trust.json"),
        serde_json::to_vec(&document()).unwrap(),
    )
    .unwrap();
    assert!(
        trust.refresh_at(NOW).is_err(),
        "older revision restored access"
    );
    assert!(trust.verify_at(&token, NOW).is_none());
}

#[test]
fn missing_or_corrupt_projection_expires_without_network_fallback() {
    let (dir, trust) = setup(&document());
    std::fs::write(dir.path().join("trust.json"), b"invalid").unwrap();
    assert!(trust.refresh_at(NOW + 1).is_err());
    let token = sign(&claims());
    assert!(trust.verify_at(&token, NOW + 1).is_some());
    assert!(trust.verify_at(&token, NOW + 300).is_none());
    std::fs::remove_file(dir.path().join("trust.json")).unwrap();
    assert!(trust.refresh_at(NOW + 2).is_err());
    assert!(OidcIdentityTrust::read_at(&dir.path().join("trust.json"), ROOT, NOW).is_err());
}

#[test]
fn public_snapshot_bounds_and_ambiguous_admission_refuse_before_boot() {
    for (field, value) in [
        ("version", json!(2)),
        ("revision", json!(0)),
        ("expires_at", json!(NOW + 301)),
        ("expires_at", json!(NOW)),
        ("generated_at", json!(NOW + 31)),
        ("canonical_root", json!("s3://other/root")),
        ("keys", json!([])),
        (
            "principals",
            json!([
            {"subject":"user_1","principal_id":"stable_1"},
            {"subject":"user_1","principal_id":"other"}]),
        ),
    ] {
        let mut d = document();
        d[field] = value;
        assert!(
            Snapshot::parse(&serde_json::to_vec(&d).unwrap(), ROOT, NOW).is_err(),
            "{field}"
        );
    }
    let mut d = document();
    d["principals"][0]["permissions"] = json!(["read"]);
    assert!(Snapshot::parse(&serde_json::to_vec(&d).unwrap(), ROOT, NOW).is_err());
    for field in ["kty", "alg", "use", "n", "e"] {
        let mut d = document();
        d["keys"][0][field] = json!("invalid");
        assert!(
            Snapshot::parse(&serde_json::to_vec(&d).unwrap(), ROOT, NOW).is_err(),
            "{field}"
        );
    }
    let mut d = document();
    d["keys"][0]["kid"] = json!("");
    assert!(Snapshot::parse(&serde_json::to_vec(&d).unwrap(), ROOT, NOW).is_err());
    assert!(Snapshot::parse(&vec![b' '; MAX_SNAPSHOT_BYTES + 1], ROOT, NOW).is_err());
    let (_dir, trust) = setup(&document());
    assert!(
        trust
            .verify_at(&"x".repeat(MAX_TOKEN_BYTES + 1), NOW)
            .is_none()
    );
    let mut h = Header::new(Algorithm::HS256);
    h.kid = Some("key-1".into());
    let bad = encode(&h, &claims(), &EncodingKey::from_secret(b"not an RSA key")).unwrap();
    assert!(trust.verify_at(&bad, NOW).is_none());
}
