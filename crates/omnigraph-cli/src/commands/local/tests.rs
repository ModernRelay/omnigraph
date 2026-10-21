use std::io::{self, Cursor, Read, Write};

use super::*;

fn target() -> crate::targets::ResolvedTarget {
    crate::targets::ResolvedTarget::new(
        crate::targets::TargetBinding::Server {
            endpoint: "https://prod.example".into(),
            auth: crate::targets::ServerAuth::Token {
                owner: "prod".into(),
            },
            default_graph: None,
        },
        crate::targets::TargetProvenance::Explicit {
            name: "prod".into(),
        },
    )
}
fn configure(home: &std::path::Path) {
    std::fs::write(home.join("config.yaml"), "target_config_version: 1\ntargets:\n  prod:\n    kind: server\n    endpoint: https://prod.example\n    credential: prod\n").unwrap();
}

struct ClosedOutput;

impl Write for ClosedOutput {
    fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "output closed"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn login_reads_one_line_and_explicit_token_does_not_read_stdin() {
    let home = tempfile::tempdir().unwrap();
    configure(home.path());
    let operator = InvocationConfig::at(Some(home.path().to_path_buf()), None);
    let mut stdin = Cursor::new(b"  first-token  \r\nsecond-token\n");
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    run_login(
        &operator,
        &target(),
        None,
        true,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    )
    .unwrap();
    assert_eq!(stdin.position(), b"  first-token  \r\n".len() as u64);
    let path = operator.credentials_path().unwrap();
    assert_eq!(
        crate::operator::read_credential_at(&path, "prod")
            .unwrap()
            .as_deref(),
        Some("first-token")
    );
    let report: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(report["target"], "prod");
    assert_eq!(report["credential_stored"], true);
    assert!(!String::from_utf8_lossy(&stdout).contains("first-token"));
    assert!(!String::from_utf8_lossy(&stderr).contains("first-token"));

    struct Unreadable;
    impl Read for Unreadable {
        fn read(&mut self, _bytes: &mut [u8]) -> io::Result<usize> {
            panic!("--token must not read stdin");
        }
    }
    run_login(
        &operator,
        &target(),
        Some(" explicit-token ".into()),
        false,
        &mut Unreadable,
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap();
    assert_eq!(
        crate::operator::read_credential_at(&path, "prod")
            .unwrap()
            .as_deref(),
        Some("explicit-token")
    );
}

#[test]
fn invalid_login_input_precedes_config_and_keeps_credentials_unchanged() {
    let home = tempfile::tempdir().unwrap();
    configure(home.path());
    std::fs::write(home.path().join("config.yaml"), "servers: [\n").unwrap();
    let operator = InvocationConfig::at(Some(home.path().to_path_buf()), None);
    let path = operator.credentials_path().unwrap();
    crate::operator::rewrite_credentials_at(&path, "prod", Some("original")).unwrap();
    let original = std::fs::read(&path).unwrap();
    for bytes in [b" \nignored".as_slice(), b"\xff\n".as_slice()] {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let error = run_login(
            &operator,
            &target(),
            None,
            false,
            &mut Cursor::new(bytes),
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
        assert!(!error.to_string().contains("operator config"), "{error}");
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}

#[test]
fn login_and_logout_output_failures_preserve_the_completed_credential_change() {
    let home = tempfile::tempdir().unwrap();
    configure(home.path());
    let operator = InvocationConfig::at(Some(home.path().to_path_buf()), None);
    let path = operator.credentials_path().unwrap();
    crate::operator::rewrite_credentials_at(&path, "other", Some("untouched")).unwrap();
    for json in [false, true] {
        let error = run_login(
            &operator,
            &target(),
            Some("stored".into()),
            json,
            &mut io::empty(),
            &mut ClosedOutput,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            crate::operator::read_credential_at(&path, "prod")
                .unwrap()
                .as_deref(),
            Some("stored")
        );
        let error = run_logout(&operator, &target(), json, &mut ClosedOutput).unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            crate::operator::read_credential_at(&path, "prod").unwrap(),
            None
        );
        assert_eq!(
            crate::operator::read_credential_at(&path, "other")
                .unwrap()
                .as_deref(),
            Some("untouched")
        );
    }
}

#[test]
fn skill_export_delivery_failure_keeps_the_written_files_and_record() {
    let home = tempfile::tempdir().unwrap();
    configure(home.path());
    for json in [false, true] {
        let dir = home.path().join(if json { "json" } else { "human" });
        let error = run_skill(
            SkillCommand::Export {
                dir: dir.clone(),
                force: false,
                json,
            },
            &mut ClosedOutput,
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        for file in omnigraph_skill::FILES {
            assert_eq!(std::fs::read(dir.join(file.path)).unwrap(), file.contents);
        }
        let record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join(crate::skill::RECORD)).unwrap())
                .unwrap();
        assert_eq!(
            record["files"].as_object().unwrap().len(),
            omnigraph_skill::FILES.len()
        );
        let mut retry = Vec::new();
        assert!(
            run_skill(
                SkillCommand::Export {
                    dir,
                    force: false,
                    json
                },
                &mut retry
            )
            .unwrap_err()
            .to_string()
            .contains("already holds")
        );
        assert!(retry.is_empty());
    }
}

#[tokio::test]
async fn embed_report_failure_keeps_the_completed_output_file() {
    let home = tempfile::tempdir().unwrap();
    configure(home.path());
    let input = home.path().join("input.jsonl");
    let spec = home.path().join("spec.json");
    std::fs::write(
        &input,
        "{\"type\":\"Doc\",\"data\":{\"text\":\"hello\",\"vector\":[1,2]}}\n",
    )
    .unwrap();
    std::fs::write(
        &spec,
        r#"{"dimension":2,"types":{"Doc":{"target":"vector","fields":["text"]}}}"#,
    )
    .unwrap();
    for json in [false, true] {
        let output = home
            .path()
            .join(if json { "json.jsonl" } else { "human.jsonl" });
        let args = EmbedArgs {
            seed: None,
            input: Some(input.clone()),
            output: Some(output.clone()),
            spec: Some(spec.clone()),
            clean: true,
            reembed_all: false,
            types: Vec::new(),
            selectors: Vec::new(),
            json,
        };
        let error = run_embed(args, &mut ClosedOutput).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::BrokenPipe
        );
        let record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(
            record,
            serde_json::json!({"type":"Doc","data":{"text":"hello"}})
        );
        assert!(!output.with_extension("jsonl.tmp").exists());
        assert!(std::fs::read_to_string(&input).unwrap().contains("vector"));
    }
}
