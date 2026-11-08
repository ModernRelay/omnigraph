use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::*;

const SCHEMA: &str = include_str!("../../../../docs/user/cli/operator-config.schema.json");

fn schema() -> Value {
    serde_json::from_str(SCHEMA).unwrap()
}

#[test]
fn schema_id_grammars_and_scope_references_match_execution_issue_1115() {
    let schema = schema();
    let defs = &schema["$defs"];
    for (name, pattern) in [
        ("organizationId", r"^[A-Za-z0-9_-]{1,128}$(?![\s\S])"),
        ("managedId", r"^[A-Za-z0-9_-]{1,256}$(?![\s\S])"),
    ] {
        assert_eq!(defs[name], json!({"type": "string", "pattern": pattern}));
    }
    assert_eq!(
        defs["graphId"],
        json!({
            "type": "string",
            "pattern": r"^[A-Za-z0-9-]{1,64}$(?![\s\S])",
            "not": {"enum": ["policies", "healthz", "openapi", "graphs"]}
        })
    );
    assert_eq!(
        defs["servedGraphId"]["oneOf"],
        json!([{"$ref": "#/$defs/graphId"}, {"const": "og_catalog"}])
    );
    for (record, field, definition) in [
        ("profile", "organization_id", "organizationId"),
        ("managedTarget", "cluster_id", "managedId"),
        ("managedTarget", "cluster_incarnation", "managedId"),
        ("managedTarget", "default_graph", "servedGraphId"),
        ("serverTarget", "default_graph", "servedGraphId"),
        ("directRootTarget", "default_graph", "graphId"),
        ("alias", "graph", "servedGraphId"),
    ] {
        assert_eq!(
            defs[record]["properties"][field]["$ref"],
            format!("#/$defs/{definition}"),
            "{record}.{field}"
        );
    }
}

fn record<T: DeserializeOwned + Serialize>(name: &str, value: Value) {
    let schema = schema();
    let declaration = if name == "document" {
        &schema
    } else {
        &schema["$defs"][name]
    };
    assert_eq!(declaration["additionalProperties"], false, "{name}");
    let parsed: T = serde_json::from_value(value.clone()).unwrap();
    let serialized = serde_json::to_value(parsed).unwrap();
    let fields: Vec<_> = serialized.as_object().unwrap().keys().collect();
    let declared: Vec<_> = declaration["properties"]
        .as_object()
        .unwrap()
        .keys()
        .collect();
    assert_eq!(fields, declared, "public field drift in {name}");
    for field in fields {
        let mut omitted = value.clone();
        omitted.as_object_mut().unwrap().remove(field);
        let required = declaration["required"]
            .as_array()
            .is_some_and(|required| required.contains(&Value::String(field.clone())));
        assert_eq!(
            serde_json::from_value::<T>(omitted).is_err(),
            required,
            "required-field drift in {name}.{field}"
        );
    }
    let mut extra = value;
    extra
        .as_object_mut()
        .unwrap()
        .insert("undeclared".into(), json!(true));
    assert!(
        serde_json::from_value::<T>(extra).is_err(),
        "unknown field accepted by {name}"
    );
}

#[test]
fn checked_in_schema_fields_and_required_lists_match_serde_records() {
    record::<OperatorConfig>(
        "document",
        json!({"version":1,"profiles":{},"targets":{},"aliases":{},"defaults":{}}),
    );
    record::<Profile>(
        "profile",
        json!({"api":"https://example.com","organization_id":"id","auth":{"provider":"omnigraph"},"transport":{}}),
    );
    record::<Target>(
        "managedTarget",
        json!({"kind":"managed","profile":"service","cluster_id":"id","cluster_incarnation":"generation","endpoint":"https://example.com","transport":{},"default_graph":"graph"}),
    );
    record::<Target>(
        "serverTarget",
        json!({"kind":"server","endpoint":"https://example.com","auth":{"provider":"anonymous"},"transport":{},"default_graph":"graph"}),
    );
    record::<Target>(
        "directGraphTarget",
        json!({"kind":"direct","scope":"graph","uri":"file:///graph"}),
    );
    record::<Target>(
        "directRootTarget",
        json!({"kind":"direct","scope":"root","uri":"file:///root","default_graph":"graph"}),
    );
    record::<AuthProfile>("omnigraphAuth", json!({"provider":"omnigraph"}));
    record::<AuthProfile>("anonymousAuth", json!({"provider":"anonymous"}));
    record::<AuthProfile>(
        "tokenAuth",
        json!({"provider":"token","source":{"store":{}}}),
    );
    record::<AuthProfile>(
        "execAuth",
        json!({"provider":"exec","command":["/helper"],"timeout":"30s"}),
    );
    record::<AuthProfile>(
        "oauthClientCredentialsAuth",
        json!({"provider":"oauth_client_credentials","token_endpoint":"https://auth.example/token","client_id":"client","client_secret":{"env":"CLIENT_SECRET"},"scope":"read"}),
    );
    record::<TokenSource>("storeSource", json!({"store":{}}));
    record::<TokenSource>("envSource", json!({"env":"TOKEN"}));
    record::<TokenSource>("fileSource", json!({"file":"token"}));
    record::<Transport>("transport", json!({"tls":{},"proxy":{"mode":"direct"}}));
    record::<Tls>("tls", json!({"ca":{"mode":"append","file":"ca.pem"}}));
    record::<Ca>("ca", json!({"mode":"append","file":"ca.pem"}));
    record::<Proxy>("directProxy", json!({"mode":"direct"}));
    record::<Proxy>("environmentProxy", json!({"mode":"environment"}));
    record::<Proxy>("urlProxy", json!({"mode":"url","url":"http://proxy"}));
    record::<Defaults>(
        "defaults",
        json!({"profile":"service","target":"target","output":"table"}),
    );
    record::<ReadAlias>(
        "alias",
        json!({"target":"target","query":"query","graph":"graph","params":{}}),
    );
}

#[test]
fn schema_discriminators_defaults_and_owner_variants_match_contract() {
    let schema = schema();
    let defs = &schema["$defs"];
    assert!(
        schema["$comment"]
            .as_str()
            .unwrap()
            .contains(&MAX_CONFIG_BYTES.to_string())
    );
    assert_eq!(
        defs["profile"]["properties"]["auth"],
        json!({"$ref": "#/$defs/profileAuth"})
    );
    assert!(defs["profile"]["properties"].get("default_auth").is_none());
    assert!(schema["properties"].get("connections").is_none());
    assert_eq!(
        defs["serverTarget"]["properties"]["auth"],
        json!({"$ref": "#/$defs/serverAuth"})
    );
    assert!(
        defs["serverTarget"]["properties"]
            .get("default_auth")
            .is_none()
    );
    for (name, expected) in [
        (
            "target",
            vec![
                "managedTarget",
                "serverTarget",
                "directGraphTarget",
                "directRootTarget",
            ],
        ),
        (
            "profileAuth",
            vec![
                "omnigraphAuth",
                "tokenAuth",
                "execAuth",
                "oauthClientCredentialsAuth",
            ],
        ),
        ("serverAuth", vec!["anonymousAuth", "tokenAuth", "execAuth"]),
        ("clientSecretSource", vec!["envSource", "fileSource"]),
        (
            "tokenSource",
            vec!["storeSource", "envSource", "fileSource"],
        ),
        ("proxy", vec!["directProxy", "urlProxy", "environmentProxy"]),
    ] {
        let expected: Vec<_> = expected
            .into_iter()
            .map(|name| json!({"$ref":format!("#/$defs/{name}")}))
            .collect();
        assert_eq!(defs[name]["oneOf"], json!(expected), "{name}");
    }
    for (name, field, value) in [
        ("managedTarget", "kind", "managed"),
        ("serverTarget", "kind", "server"),
        ("directGraphTarget", "kind", "direct"),
        ("directGraphTarget", "scope", "graph"),
        ("directRootTarget", "kind", "direct"),
        ("directRootTarget", "scope", "root"),
        ("omnigraphAuth", "provider", "omnigraph"),
        ("anonymousAuth", "provider", "anonymous"),
        ("tokenAuth", "provider", "token"),
        ("execAuth", "provider", "exec"),
        (
            "oauthClientCredentialsAuth",
            "provider",
            "oauth_client_credentials",
        ),
        ("directProxy", "mode", "direct"),
        ("urlProxy", "mode", "url"),
        ("environmentProxy", "mode", "environment"),
    ] {
        assert_eq!(defs[name]["properties"][field]["const"], value);
    }
    let outputs = [
        OutputFormat::Table,
        OutputFormat::Json,
        OutputFormat::Jsonl,
        OutputFormat::Csv,
        OutputFormat::Kv,
        OutputFormat::Toon,
    ];
    assert_eq!(
        defs["defaults"]["properties"]["output"]["enum"],
        serde_json::to_value(outputs).unwrap()
    );
    assert_eq!(
        defs["ca"]["properties"]["mode"]["enum"],
        serde_json::to_value([CaMode::Append, CaMode::Replace]).unwrap()
    );
    assert_eq!(
        defs["defaults"]["properties"]["output"]["default"],
        serde_json::to_value(OutputFormat::default()).unwrap()
    );
    assert_eq!(
        defs["execAuth"]["properties"]["timeout"]["default"],
        default_timeout()
    );
    assert_eq!(
        defs["transport"]["properties"]["proxy"]["default"],
        serde_json::to_value(Proxy::default()).unwrap()
    );
    assert_eq!(schema["properties"]["version"]["const"], 1);
    assert_eq!(
        defs["serverTarget"]["allOf"][0]["then"]["properties"]["auth"],
        json!({"$ref":"#/$defs/anonymousAuth"})
    );
}

#[test]
fn every_schema_reference_resolves_locally() {
    fn visit(value: &Value, root: &Value) {
        match value {
            Value::Object(fields) => {
                if let Some(reference) = fields.get("$ref") {
                    let reference = reference.as_str().unwrap();
                    assert!(reference.starts_with("#/"));
                    assert!(root.pointer(&reference[1..]).is_some(), "{reference}");
                }
                for value in fields.values() {
                    visit(value, root);
                }
            }
            Value::Array(values) => {
                for value in values {
                    visit(value, root);
                }
            }
            _ => {}
        }
    }
    let schema = schema();
    visit(&schema, &schema);
}

#[test]
fn schema_forbids_custom_destination_ca_with_explicit_https_proxy() {
    assert_eq!(
        schema()["$defs"]["transport"]["not"],
        json!({
            "required": ["tls", "proxy"],
            "properties": {
                "tls": {"required": ["ca"]},
                "proxy": {
                    "required": ["mode", "url"],
                    "properties": {
                        "mode": {"const": "url"},
                        "url": {"pattern": "^[Hh][Tt][Tt][Pp][Ss]://"}
                    }
                }
            }
        })
    );
}
