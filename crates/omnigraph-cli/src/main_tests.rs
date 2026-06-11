//! In-source test suite for the CLI binary (moved verbatim from
//! main.rs; `use super::*` resolves through the #[path] declaration).

    use std::fs;

    use super::{
        DEFAULT_BEARER_TOKEN_ENV, apply_bearer_token, bearer_token_from_env_file,
        legacy_change_request_body, load_cli_config, load_env_file_into_process,
        normalize_bearer_token, parse_env_assignment, resolve_cli_graph, resolve_policy_context,
        resolve_remote_bearer_token,
    };
    use omnigraph_server::load_config;
    use reqwest::header::AUTHORIZATION;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn legacy_change_request_body_uses_legacy_field_names() {
        // `execute_change_remote` hits `POST /change`, which old
        // `omnigraph-server` builds deserialize as `ChangeRequest` with
        // **required** `query_source` and optional `query_name` keys.
        // Newer servers accept both spellings via serde alias, but a
        // newer CLI must still emit the legacy keys on the wire so it
        // can talk to an old server during a rolling upgrade.
        let body = legacy_change_request_body(
            "query insert_person($n: String) { insert Person { name: $n } }",
            Some("insert_person"),
            "main",
            Some(&json!({ "n": "Alice" })),
        );
        assert_eq!(
            body["query_source"].as_str(),
            Some("query insert_person($n: String) { insert Person { name: $n } }"),
        );
        assert_eq!(body["query_name"].as_str(), Some("insert_person"));
        assert_eq!(body["branch"].as_str(), Some("main"));
        assert_eq!(body["params"]["n"].as_str(), Some("Alice"));
        // Crucially, the **new** field names must NOT appear -- old
        // servers would silently treat them as unknown fields and then
        // fail on missing required `query_source`.
        assert!(
            body.get("query").is_none(),
            "legacy /change body must not carry the renamed `query` key; got {body}"
        );
        assert!(
            body.get("name").is_none(),
            "legacy /change body must not carry the renamed `name` key; got {body}"
        );
    }

    #[test]
    fn legacy_change_request_body_omits_optional_fields_when_unset() {
        let body = legacy_change_request_body(
            "query find() { match { $p: Person } return { $p.name } }",
            None,
            "main",
            None,
        );
        assert_eq!(body["branch"].as_str(), Some("main"));
        assert!(body.get("query_name").is_none());
        assert!(body.get("params").is_none());
    }

    #[test]
    fn apply_bearer_token_adds_header_when_configured() {
        let client = reqwest::Client::new();
        let request = apply_bearer_token(client.get("http://example.com"), Some("demo-token"))
            .build()
            .unwrap();
        assert_eq!(
            request
                .headers()
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer demo-token")
        );
    }

    #[test]
    fn apply_bearer_token_leaves_request_unchanged_when_not_configured() {
        let client = reqwest::Client::new();
        let request = apply_bearer_token(client.get("http://example.com"), None)
            .build()
            .unwrap();
        assert!(request.headers().get(AUTHORIZATION).is_none());
    }

    #[test]
    fn normalize_bearer_token_trims_and_filters_blank_values() {
        assert_eq!(normalize_bearer_token(None), None);
        assert_eq!(normalize_bearer_token(Some("   ".to_string())), None);
        assert_eq!(
            normalize_bearer_token(Some(" demo-token ".to_string())).as_deref(),
            Some("demo-token")
        );
    }

    #[test]
    fn parse_env_assignment_supports_plain_and_exported_values() {
        assert_eq!(
            parse_env_assignment("DEMO_TOKEN=demo-token"),
            Some(("DEMO_TOKEN".to_string(), "demo-token".to_string()))
        );
        assert_eq!(
            parse_env_assignment("export DEMO_TOKEN=\"quoted-token\""),
            Some(("DEMO_TOKEN".to_string(), "quoted-token".to_string()))
        );
        assert_eq!(parse_env_assignment("# comment"), None);
        assert_eq!(parse_env_assignment("   "), None);
    }

    #[test]
    fn bearer_token_from_env_file_reads_named_value() {
        let temp = tempdir().unwrap();
        let env_file = temp.path().join(".env.omni");
        fs::write(
            &env_file,
            "FIRST=ignore\nexport DEMO_TOKEN=\" demo-token \"\n",
        )
        .unwrap();

        assert_eq!(
            bearer_token_from_env_file(&env_file, "DEMO_TOKEN")
                .unwrap()
                .as_deref(),
            Some("demo-token")
        );
        assert_eq!(
            bearer_token_from_env_file(&env_file, "MISSING").unwrap(),
            None
        );
    }

    #[test]
    fn load_env_file_into_process_sets_missing_values_without_overriding_existing_ones() {
        let temp = tempdir().unwrap();
        let env_file = temp.path().join(".env.omni");
        fs::write(
            &env_file,
            "AUTOLOAD_ONLY=from-file\nAUTOLOAD_PRESET=from-file\n",
        )
        .unwrap();

        let missing_key = "AUTOLOAD_ONLY";
        let preset_key = "AUTOLOAD_PRESET";
        let previous_missing = std::env::var_os(missing_key);
        let previous_preset = std::env::var_os(preset_key);

        unsafe {
            std::env::remove_var(missing_key);
            std::env::set_var(preset_key, "from-env");
        }

        load_env_file_into_process(&env_file).unwrap();

        assert_eq!(std::env::var(missing_key).unwrap(), "from-file");
        assert_eq!(std::env::var(preset_key).unwrap(), "from-env");

        unsafe {
            if let Some(value) = previous_missing {
                std::env::set_var(missing_key, value);
            } else {
                std::env::remove_var(missing_key);
            }

            if let Some(value) = previous_preset {
                std::env::set_var(preset_key, value);
            } else {
                std::env::remove_var(preset_key);
            }
        }
    }

    #[test]
    fn resolve_remote_bearer_token_uses_scoped_env_file_with_global_fallback() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            r#"
graphs:
  demo:
    uri: https://example.com
    bearer_token_env: DEMO_TOKEN
auth:
  env_file: .env.omni
cli:
  graph: demo
"#,
        )
        .unwrap();
        fs::write(
            temp.path().join(".env.omni"),
            "DEMO_TOKEN=scoped-token\nOMNIGRAPH_BEARER_TOKEN=global-token\n",
        )
        .unwrap();

        let previous = std::env::var_os(DEFAULT_BEARER_TOKEN_ENV);
        let previous_home = std::env::var_os("OMNIGRAPH_HOME");
        unsafe {
            std::env::remove_var(DEFAULT_BEARER_TOKEN_ENV);
            // Hermetic: the keyed hop (RFC-007 PR 2) must not pick up a real
            // ~/.omnigraph on the developer's machine — and with no operator
            // servers defined, the legacy chain below must behave
            // byte-identically to pre-PR-2 (tested-as-untouched).
            std::env::set_var("OMNIGRAPH_HOME", temp.path().join("no-operator-config"));
        }

        let config_path = temp.path().join("omnigraph.yaml");
        let config = load_config(Some(&config_path)).unwrap();

        assert_eq!(
            resolve_remote_bearer_token(&config, None, Some("demo"))
                .unwrap()
                .as_deref(),
            Some("scoped-token")
        );
        assert_eq!(
            resolve_remote_bearer_token(&config, Some("https://override.example.com"), None)
                .unwrap()
                .as_deref(),
            Some("global-token")
        );

        unsafe {
            if let Some(value) = previous {
                std::env::set_var(DEFAULT_BEARER_TOKEN_ENV, value);
            } else {
                std::env::remove_var(DEFAULT_BEARER_TOKEN_ENV);
            }
            if let Some(value) = previous_home {
                std::env::set_var("OMNIGRAPH_HOME", value);
            } else {
                std::env::remove_var("OMNIGRAPH_HOME");
            }
        }
    }

    #[test]
    fn load_cli_config_autoloads_env_file_into_process() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            r#"
auth:
  env_file: .env.omni
graphs:
  demo:
    uri: s3://bucket/prefix
"#,
        )
        .unwrap();
        fs::write(
            temp.path().join(".env.omni"),
            "AUTOLOAD_FROM_CONFIG=loaded\n",
        )
        .unwrap();

        let key = "AUTOLOAD_FROM_CONFIG";
        let previous = std::env::var_os(key);
        unsafe {
            std::env::remove_var(key);
        }

        let config_path = temp.path().join("omnigraph.yaml");
        let config = load_cli_config(Some(&config_path)).unwrap();

        assert_eq!(
            config.resolve_target_uri(None, Some("demo"), None).unwrap(),
            "s3://bucket/prefix"
        );
        assert_eq!(std::env::var(key).unwrap(), "loaded");

        unsafe {
            if let Some(value) = previous {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn graph_identity_resolve_policy_context_named_cli_graph_uses_graph_key_not_project_name_or_uri()
     {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  local:
    uri: /tmp/local-policy-graph.omni
    policy:
      file: ./policy.yaml
cli:
  graph: local
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let context = resolve_policy_context(&config).unwrap();
        assert_eq!(context.graph_id, "local");
    }

    #[test]
    fn graph_identity_resolve_policy_context_server_graph_uses_graph_key_when_cli_graph_absent() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  local:
    uri: /tmp/local-policy-graph.omni
    policy:
      file: ./server-policy.yaml
server:
  graph: local
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let context = resolve_policy_context(&config).unwrap();
        assert_eq!(context.graph_id, "local");
        assert!(context.policy_file.ends_with("server-policy.yaml"));
    }

    #[test]
    fn graph_identity_resolve_policy_context_anonymous_uses_top_level_default_identity() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  local:
    uri: /tmp/local-policy-graph.omni
policy:
  file: ./top-policy.yaml
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let context = resolve_policy_context(&config).unwrap();
        assert_eq!(context.graph_id, "default");
        assert!(context.policy_file.ends_with("top-policy.yaml"));
    }

    #[test]
    fn graph_identity_resolve_cli_graph_named_target_uses_graph_key_not_project_name_or_uri() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  prod:
    uri: s3://bucket/prod-graph/
    policy:
      file: ./prod-policy.yaml
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let graph = resolve_cli_graph(&config, None, Some("prod")).unwrap();
        assert_eq!(graph.selected(), Some("prod"));
        assert_eq!(graph.graph_id, "prod");
        assert_eq!(graph.uri, "s3://bucket/prod-graph/");
    }

    #[test]
    fn graph_identity_resolve_cli_graph_positional_uri_uses_anonymous_normalized_uri() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  local:
    uri: /tmp/configured-graph.omni
    policy:
      file: ./policy.yaml
cli:
  graph: local
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let local_graph_path = temp.path().join("explicit-graph.omni");
        let local_graph = resolve_cli_graph(
            &config,
            Some(format!("file://{}", local_graph_path.display())),
            None,
        )
        .unwrap();
        assert_eq!(local_graph.selected(), None);
        assert_eq!(
            local_graph.graph_id,
            local_graph_path.to_string_lossy().as_ref()
        );
        assert_eq!(local_graph.policy_file, None);

        let s3_graph = resolve_cli_graph(
            &config,
            Some("s3://bucket/anonymous-graph/".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(s3_graph.selected(), None);
        assert_eq!(s3_graph.graph_id, "s3://bucket/anonymous-graph");
        assert_eq!(s3_graph.policy_file, None);
    }
