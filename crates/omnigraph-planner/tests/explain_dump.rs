//! Process-isolated diagnostic failures that GQT cannot inject.

#![cfg(unix)]

use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};

use omnigraph_compiler::ir::QueryIR;
use omnigraph_planner::{Bounds, MemorySource, Operation, RouteOverride, route};

#[path = "support/bounds.rs"]
mod fixture_bounds;

const CHILD_DIR: &str = "OMNIGRAPH_TEST_EXPLAIN_DUMP_FAILURE_DIR";

#[test]
fn a_dump_failure_survives_broken_stderr() {
    let dir = tempfile::tempdir().unwrap();
    let invalid_dir = dir.path().join("ordinary-file");
    std::fs::write(&invalid_dir, "not a directory").unwrap();
    for broken in [false, true] {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "dump_failure_child", "--nocapture"])
            .env(CHILD_DIR, &invalid_dir);
        if broken {
            let (reader, writer) = UnixStream::pair().unwrap();
            drop(reader);
            child.stderr(Stdio::from(OwnedFd::from(writer)));
        }
        let output = child.output().unwrap();
        assert!(output.status.success(), "broken={broken}: {output:?}");
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("explain returned")
        );
        assert_eq!(
            std::fs::read_to_string(&invalid_dir).unwrap(),
            "not a directory"
        );
    }
}

#[test]
fn dump_failure_child() {
    let Some(dir) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    let query = Operation::Query(Box::new(QueryIR {
        name: "unresolved".into(),
        params: vec![],
        pipeline: vec![],
        return_exprs: vec![],
        distinct: false,
        order_by: vec![],
        limit: None,
    }));
    let source = MemorySource::default();
    let expected = route(
        &query,
        &source,
        RouteOverride::Registry,
        &fixture_bounds::BOUNDS,
    );
    let source = source.with_explain_dump_dir(dir.into());
    let actual = route(
        &query,
        &source,
        RouteOverride::Registry,
        &fixture_bounds::BOUNDS,
    );
    assert_eq!(
        actual.explain().unwrap().to_value(),
        expected.explain().unwrap().to_value()
    );
    println!("explain returned");
}
