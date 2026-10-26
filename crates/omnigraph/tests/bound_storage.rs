//! A graph handle bound to one storage destination sends every request there,
//! whatever the ambient provider environment names.
//!
//! The parent test starts two recording forwarders in front of the configured
//! S3-compatible endpoint: `bound`, which the captured binding names, and
//! `decoy`, which the ambient endpoint variables name. It then runs the
//! workload in a child of this binary whose environment carries the decoy,
//! unsigned requests and disabled conditional puts from its first
//! instruction, so no test mutates its own process environment and Lance's
//! environment snapshots see them too. Both forwarders relay to the same
//! backend, so a request that strays to the decoy still completes; the parent
//! names it instead of failing on a symptom. An ambient proxy variable is not
//! among those settings: a capture refuses it, which the core
//! `captured_provider` test owns.
//!
//! The workload covers control objects and Lance data, writes and reads, the
//! live re-read a constrained edge insert takes, a racing writer's publish
//! retry, a branch create, a three-way merge whose base is not the target
//! head, a branch delete, historical and commit-addressed reads, the change
//! feed, export and maintenance.
//!
//! Runs only against a configured plain-HTTP endpoint the forwarders can
//! front (RustFS in CI) and skips, naming why, for AWS itself or an HTTPS
//! endpoint. It needs its own process because the child re-executes this
//! binary.

mod helpers;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};

use omnigraph::changes::ChangeFeedScope;
use omnigraph::db::{
    CleanupPolicyOptions, InitOptions, MergeOutcome, Omnigraph, ReadTarget, Snapshot, SnapshotId,
};
use omnigraph::loader::LoadMode;
use omnigraph::storage::{CapturedStorageBinding, StorageDestination, storage_for_binding};

use helpers::*;

const CHILD: &str = "OMNIGRAPH_BOUND_STORAGE_CHILD";
const CHILD_URI: &str = "OMNIGRAPH_BOUND_STORAGE_URI";
const CHILD_ENDPOINT: &str = "OMNIGRAPH_BOUND_STORAGE_ENDPOINT";
const CHILD_REGION: &str = "OMNIGRAPH_BOUND_STORAGE_REGION";

/// Every variable the S3 clients read an endpoint from: object_store's
/// builder reads `AWS_ENDPOINT*`, and Lance's provider also reads the
/// unprefixed spellings, per store.
const AMBIENT_ENDPOINT_VARIABLES: [&str; 5] = [
    "AWS_ENDPOINT",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_S3",
    "ENDPOINT",
    "ENDPOINT_URL",
];

/// Ambient transport settings that would weaken a bound request: an unsigned
/// request or a disabled conditional put fails against the backend.
const AMBIENT_TRANSPORT_VARIABLES: [(&str, &str); 2] = [
    ("AWS_SKIP_SIGNATURE", "true"),
    ("AWS_CONDITIONAL_PUT", "disabled"),
];

const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
edge Knows: Person -> Person
edge WorksAt: Person -> Company @card(0..1)
"#;

const DATA: &str = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}
{"type": "Company", "data": {"name": "Acme"}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
"#;

const QUERIES: &str = r#"
query get_person($name: String) {
    match { $p: Person { name: $name } }
    return { $p.name, $p.age }
}
query friends_of($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $f
    }
    return { $f.name }
}
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
query employ($person: String, $company: String) {
    insert WorksAt { from: $person, to: $company }
}
"#;

#[test]
fn bound_handle_never_reaches_the_ambient_endpoint() {
    let Some(uri) = s3_test_graph_uri("omnigraph-bound-storage") else {
        eprintln!("skipping s3 bound-storage decoy test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let upstream = match plain_http_upstream() {
        Ok(upstream) => upstream,
        Err(reason) => {
            eprintln!("skipping s3 bound-storage decoy test: {reason}");
            return;
        }
    };
    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".into());

    let bound = Forwarder::start(upstream);
    let decoy = Forwarder::start(upstream);
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "bound_handle_workload_child",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env(CHILD_URI, &uri)
        .env(CHILD_ENDPOINT, bound.endpoint())
        .env(CHILD_REGION, &region);
    for name in AMBIENT_ENDPOINT_VARIABLES {
        child.env(name, decoy.endpoint());
    }
    for (name, value) in AMBIENT_TRANSPORT_VARIABLES {
        child.env(name, value);
    }
    let output = child.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    // A request sent to the decoy under an ambient setting can fail the
    // workload itself; name what reached the decoy in that case too.
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "bound-storage workload failed or did not run\nstdout:\n{stdout}\nstderr:\n{}\n\
         requests that reached the ambient endpoint:\n  {}",
        String::from_utf8_lossy(&output.stderr),
        decoy.requests().join("\n  ")
    );

    let strayed = decoy.requests();
    assert!(
        strayed.is_empty(),
        "{} request(s) of a bound handle reached the ambient endpoint:\n  {}",
        strayed.len(),
        strayed.join("\n  ")
    );
    let served = bound.requests();
    for (traffic, marker) in [
        ("Lance `__manifest`", "/__manifest/"),
        ("Lance table", "/nodes/"),
        ("control-object", "/__init_claim.json"),
        ("graph-index artifact", "/__graph_index/"),
    ] {
        assert!(
            served.iter().any(|request| request.contains(marker)),
            "the bound endpoint carried no {traffic} request ({marker}) among {} requests",
            served.len()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "subprocess helper; exercised by bound_handle_never_reaches_the_ambient_endpoint"]
async fn bound_handle_workload_child() {
    // An ignored-tests run reaches this without the parent: run the parent then.
    if std::env::var_os(CHILD).is_none() {
        tokio::task::spawn_blocking(bound_handle_never_reaches_the_ambient_endpoint)
            .await
            .unwrap();
        return;
    }
    let uri = std::env::var(CHILD_URI).unwrap();
    let binding = CapturedStorageBinding::capture(
        &uri,
        &StorageDestination::S3 {
            region: std::env::var(CHILD_REGION).unwrap(),
            endpoint: Some(std::env::var(CHILD_ENDPOINT).unwrap()),
            virtual_hosted_style: false,
        },
    )
    .unwrap();
    let storage = || storage_for_binding(&binding).unwrap();

    let db = session(
        Omnigraph::init_with_storage(&uri, SCHEMA, storage(), InitOptions::default())
            .await
            .unwrap(),
    );
    db.load("main", DATA, LoadMode::Overwrite).await.unwrap();
    // The `@card` check counts committed edges on a fresh re-read of the
    // live branch, not on the write's pinned base.
    db.mutate(
        "main",
        QUERIES,
        "employ",
        &params(&[("$person", "Alice"), ("$company", "Acme")]),
    )
    .await
    .unwrap();
    let history_version = graph_version(&db.snapshot_of("main").await.unwrap());

    // A second handle publishes over the first one's view: the first
    // handle's next publish loses the version race and reloads.
    let racer = session(Omnigraph::open_with_storage(&uri, storage()).await.unwrap());
    racer
        .mutate(
            "main",
            QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Racer")], &[("$age", 41)]),
        )
        .await
        .unwrap();
    db.mutate(
        "main",
        QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Stale")], &[("$age", 42)]),
    )
    .await
    .unwrap();

    // Both sides write after the branch point, so the merge reads its base
    // from the commit the two lineages share rather than the target head.
    db.branch_create("feature").await.unwrap();
    db.mutate(
        "feature",
        QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Feature")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    db.mutate(
        "main",
        QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Main")], &[("$age", 33)]),
    )
    .await
    .unwrap();
    let merged = db.branch_merge("feature", "main").await.unwrap();
    assert_eq!(merged.outcome, MergeOutcome::Merged);
    db.branch_delete("feature").await.unwrap();

    // The CSR route of a historical read builds its graph index through the
    // handle's storage: the persisted artifact is probed before the scan build.
    let historical = with_traversal(&db, Traversal::Csr)
        .run_query_at(
            history_version,
            QUERIES,
            "friends_of",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    assert_eq!(historical.num_rows(), 1);
    let commits = db.list_commits(None).await.unwrap();
    let first_write = commits
        .iter()
        .find(|commit| commit.parent_commit_id.is_some())
        .expect("main holds a commit after genesis");
    let at_commit = db
        .query(
            ReadTarget::Snapshot(SnapshotId::new(first_write.graph_commit_id.clone())),
            QUERIES,
            "get_person",
            &params(&[("$name", "Bob")]),
        )
        .await
        .unwrap();
    assert_eq!(at_commit.num_rows(), 1);
    let changes = db
        .commit_changes_page(
            &merged
                .commit
                .expect("a merge publishes a commit")
                .graph_commit_id,
            &ChangeFeedScope::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(!changes.block.changes.is_empty());
    assert!(
        db.export_jsonl("main", &[])
            .await
            .unwrap()
            .contains("Feature")
    );

    db.optimize_as(None).await.unwrap();
    db.cleanup_as(
        CleanupPolicyOptions {
            keep_versions: Some(2),
            older_than: None,
        },
        None,
    )
    .await
    .unwrap();
    let reopened = session(Omnigraph::open_with_storage(&uri, storage()).await.unwrap());
    let rows = query_main(
        &reopened,
        QUERIES,
        "get_person",
        &params(&[("$name", "Feature")]),
    )
    .await
    .unwrap();
    assert_eq!(rows.num_rows(), 1);
}

/// The address of the configured S3-compatible endpoint, when it is one the
/// forwarders can relay: they parse plain HTTP, so AWS itself (no endpoint
/// override) and an HTTPS endpoint are reasons to skip, not failures.
fn plain_http_upstream() -> Result<SocketAddr, String> {
    let Some((name, configured)) = ["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_ENDPOINT"]
        .iter()
        .find_map(|name| Some((*name, std::env::var(name).ok()?)))
    else {
        return Err(
            "no S3 endpoint override is set (AWS itself); it needs a plain-HTTP \
                    endpoint in AWS_ENDPOINT_URL_S3"
                .into(),
        );
    };
    let Some(authority) = configured.strip_prefix("http://") else {
        return Err(format!(
            "{name} is not a plain-HTTP endpoint; the decoy forwarders relay only http://"
        ));
    };
    let upstream = authority
        .trim_end_matches('/')
        .to_socket_addrs()
        .unwrap_or_else(|error| panic!("{name} does not resolve: {error}"))
        .next()
        .unwrap_or_else(|| panic!("{name} resolves to no address"));
    Ok(upstream)
}

fn graph_version(snapshot: &Snapshot) -> u64 {
    snapshot.graph_manifest_version()
}

/// A TCP relay to one upstream that records the request line of every HTTP
/// request a client sends through it, across keep-alive reuse.
struct Forwarder {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Forwarder {
    fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                let log = Arc::clone(&log);
                std::thread::spawn(move || relay(client, upstream, &log));
            }
        });
        Self { endpoint, requests }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

/// Responses stream back untouched; requests are parsed only far enough to
/// find each head and the body length that follows it.
fn relay(client: TcpStream, upstream: SocketAddr, log: &Mutex<Vec<String>>) {
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    let (Ok(mut to_client), Ok(mut from_server)) = (client.try_clone(), server.try_clone()) else {
        return;
    };
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut from_server, &mut to_client);
        let _ = to_client.shutdown(std::net::Shutdown::Write);
    });
    let mut to_server = server;
    let mut from_client = BufReader::new(client);
    while let Some(()) = relay_request(&mut from_client, &mut to_server, log) {}
    let _ = to_server.shutdown(std::net::Shutdown::Write);
}

fn relay_request(
    from_client: &mut BufReader<TcpStream>,
    to_server: &mut TcpStream,
    log: &Mutex<Vec<String>>,
) -> Option<()> {
    let mut head = Vec::new();
    let mut line = String::new();
    let mut request_line = None;
    let mut content_length = 0_u64;
    let mut chunked = false;
    loop {
        line.clear();
        if from_client.read_line(&mut line).ok()? == 0 {
            return None;
        }
        head.extend_from_slice(line.as_bytes());
        if request_line.is_none() {
            request_line = Some(line.trim_end().to_string());
            continue;
        }
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().ok()?;
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                chunked = value.eq_ignore_ascii_case("chunked");
            }
        }
    }
    let request_line = request_line?;
    let request = request_line
        .rsplit_once(' ')
        .map_or(request_line.as_str(), |(request, _version)| request);
    log.lock().unwrap().push(request.to_string());
    to_server.write_all(&head).ok()?;
    if chunked {
        loop {
            line.clear();
            from_client.read_line(&mut line).ok()?;
            to_server.write_all(line.as_bytes()).ok()?;
            let size = u64::from_str_radix(line.trim().split(';').next()?, 16).ok()?;
            if size == 0 {
                // Trailers, then the blank line that ends the message.
                loop {
                    line.clear();
                    from_client.read_line(&mut line).ok()?;
                    to_server.write_all(line.as_bytes()).ok()?;
                    if line == "\r\n" || line.is_empty() {
                        return Some(());
                    }
                }
            }
            copy_exact(from_client, to_server, size + 2)?;
        }
    }
    copy_exact(from_client, to_server, content_length)
}

fn copy_exact(from: &mut BufReader<TcpStream>, to: &mut TcpStream, len: u64) -> Option<()> {
    let copied = std::io::copy(&mut from.by_ref().take(len), to).ok()?;
    (copied == len).then_some(())
}
