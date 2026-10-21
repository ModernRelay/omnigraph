use std::io::{self, Write};

use super::*;

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
fn skill_export_delivery_failure_keeps_the_written_files_and_record() {
    let home = tempfile::tempdir().unwrap();
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
