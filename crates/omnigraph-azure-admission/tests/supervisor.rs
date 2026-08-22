#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use omnigraph_azure_admission::{
    AcquireOutcome, AdmissionClient, LeaseId, LeaseState, ReleaseOutcome,
};
use uuid::Uuid;

fn configured_root(label: &str) -> Option<String> {
    let container = std::env::var("OMNIGRAPH_AZURE_TEST_CONTAINER").ok()?;
    Some(format!(
        "az://{container}/admission-supervisor/{label}-{}",
        Uuid::new_v4().simple()
    ))
}

fn marker_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "omnigraph-azure-admission-{label}-{}",
        Uuid::new_v4().simple()
    ))
}

fn spawn_wrapper(root: &str, mode: &str, child: &[&str], marker: Option<&Path>) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_omnigraph-azure-admission"));
    command
        .arg("run")
        .arg("--mode")
        .arg(mode)
        .arg("--root")
        .arg(root)
        .arg("--retry-seconds")
        .arg("1")
        .arg("--grace-seconds")
        .arg("5")
        .arg("--")
        .args(child)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(marker) = marker {
        command.env("OMNIGRAPH_ADMISSION_TEST_MARKER", marker);
    }
    command.spawn().expect("admission wrapper must spawn")
}

async fn wait_for_exit(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "child did not exit before timeout"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_file(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        assert!(Instant::now() < deadline, "marker was not created in time");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_locked(client: &AdmissionClient) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if matches!(
            client.inspect().await.unwrap(),
            LeaseState::Present {
                status: Some(ref status),
                ..
            } if status == "locked"
        ) {
            return;
        }
        assert!(Instant::now() < deadline, "lease did not become locked");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn cleanup(root: &str) {
    omnigraph_storage::storage_for_uri(root)
        .unwrap()
        .delete_prefix(root)
        .await
        .unwrap();
}

#[tokio::test]
async fn held_admission_never_launches_child_then_runs_once_after_release() {
    let Some(root) = configured_root("held") else {
        eprintln!("skipping Azure admission supervisor test: backend is not configured");
        return;
    };
    let client = AdmissionClient::from_env(&root).unwrap();
    let owner = match client.try_acquire(LeaseId::new()).await.unwrap() {
        AcquireOutcome::Acquired(owner) => owner,
        outcome => panic!("first owner did not acquire: {outcome:?}"),
    };
    let marker = marker_path("held");
    let mut wrapper = spawn_wrapper(
        &root,
        "job",
        &[
            "/bin/sh",
            "-c",
            "printf launched > \"$OMNIGRAPH_ADMISSION_TEST_MARKER\"",
        ],
        Some(&marker),
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!marker.exists(), "a held contender launched its child");
    assert_eq!(
        client.release(&owner).await.unwrap(),
        ReleaseOutcome::Released
    );
    assert!(
        wait_for_exit(&mut wrapper, Duration::from_secs(10))
            .await
            .success()
    );
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "launched");

    let _ = std::fs::remove_file(marker);
    cleanup(&root).await;
}

#[tokio::test]
async fn held_job_terminated_before_admission_fails_without_launching_child() {
    let Some(root) = configured_root("held-job-signal") else {
        eprintln!("skipping Azure admission supervisor test: backend is not configured");
        return;
    };
    let client = AdmissionClient::from_env(&root).unwrap();
    let owner = match client.try_acquire(LeaseId::new()).await.unwrap() {
        AcquireOutcome::Acquired(owner) => owner,
        outcome => panic!("first owner did not acquire: {outcome:?}"),
    };
    let marker = marker_path("held-job-signal");
    let mut wrapper = spawn_wrapper(
        &root,
        "job",
        &[
            "/bin/sh",
            "-c",
            "printf launched > \"$OMNIGRAPH_ADMISSION_TEST_MARKER\"",
        ],
        Some(&marker),
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!marker.exists(), "a held contender launched its child");
    kill(
        Pid::from_raw(i32::try_from(wrapper.id()).unwrap()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert!(
        !wait_for_exit(&mut wrapper, Duration::from_secs(10))
            .await
            .success(),
        "a job that never acquired admission must not report success"
    );
    assert!(!marker.exists(), "the terminated job launched its child");
    assert!(client.renew(&owner).await.unwrap());
    assert_eq!(
        client.release(&owner).await.unwrap(),
        ReleaseOutcome::Released
    );

    let _ = std::fs::remove_file(marker);
    cleanup(&root).await;
}

#[tokio::test]
async fn graceful_server_signal_drains_process_group_before_release() {
    let Some(root) = configured_root("drain") else {
        eprintln!("skipping Azure admission supervisor test: backend is not configured");
        return;
    };
    let client = AdmissionClient::from_env(&root).unwrap();
    let marker = marker_path("drain");
    let script = r#"
        trap 'kill "$worker" 2>/dev/null || true; wait "$worker" 2>/dev/null || true; exit 0' TERM
        sleep 300 & worker=$!
        printf ready > "$OMNIGRAPH_ADMISSION_TEST_MARKER"
        wait "$worker"
    "#;
    let mut wrapper = spawn_wrapper(&root, "server", &["/bin/sh", "-c", script], Some(&marker));
    wait_for_file(&marker, Duration::from_secs(10)).await;
    wait_for_locked(&client).await;

    kill(
        Pid::from_raw(i32::try_from(wrapper.id()).unwrap()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert!(
        wait_for_exit(&mut wrapper, Duration::from_secs(10))
            .await
            .success()
    );
    assert!(matches!(
        client.inspect().await.unwrap(),
        LeaseState::Present {
            status: Some(ref status),
            ..
        } if status == "unlocked"
    ));

    let _ = std::fs::remove_file(marker);
    cleanup(&root).await;
}

#[tokio::test]
async fn unexpected_server_exit_strands_lease_until_explicit_break() {
    let Some(root) = configured_root("crash") else {
        eprintln!("skipping Azure admission supervisor test: backend is not configured");
        return;
    };
    let client = AdmissionClient::from_env(&root).unwrap();
    let mut wrapper = spawn_wrapper(&root, "server", &["/bin/sh", "-c", "exit 7"], None);
    assert!(
        !wait_for_exit(&mut wrapper, Duration::from_secs(10))
            .await
            .success()
    );
    wait_for_locked(&client).await;

    client.break_after_operator_proof().await.unwrap();
    cleanup(&root).await;
}
