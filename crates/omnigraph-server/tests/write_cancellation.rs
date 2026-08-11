#![cfg(feature = "failpoints")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph_server::api::ChangeRequest;
use omnigraph_server::{AppState, build_app};
use serde_json::json;
use serial_test::serial;
use tokio::net::TcpListener;
use tower_http::timeout::TimeoutLayer;

const SCHEMA: &str = "node Person { name: String @key age: I32? }\n";
const MUTATION: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
"#;

#[derive(Clone, Copy)]
enum Cancellation {
    Http1Disconnect,
    Http2Reset,
    TimeoutMiddleware,
}

struct EffectGate {
    reached: AtomicBool,
    released: Mutex<bool>,
    release: Condvar,
}

impl EffectGate {
    fn new() -> Self {
        Self {
            reached: AtomicBool::new(false),
            released: Mutex::new(false),
            release: Condvar::new(),
        }
    }

    fn park_after_effect(&self) {
        self.reached.store(true, Ordering::Release);
        let mut released = self.released.lock().expect("effect gate poisoned");
        while !*released {
            released = self.release.wait(released).expect("effect gate poisoned");
        }
    }

    async fn wait_until_reached(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !self.reached.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("write never reached the armed-sidecar seam");
    }

    fn release(&self) {
        *self.released.lock().expect("effect gate poisoned") = true;
        self.release.notify_all();
    }
}

async fn assert_cancellation_independent_write(kind: Cancellation) {
    let temp = tempfile::tempdir().unwrap();
    let graph = temp.path().join("server.omni");
    let uri = graph.to_string_lossy().to_string();
    Omnigraph::init(&uri, SCHEMA).await.unwrap();

    let state = AppState::open(uri.clone()).await.unwrap();
    let app = match kind {
        Cancellation::TimeoutMiddleware => build_app(state).layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(1),
        )),
        Cancellation::Http1Disconnect | Cancellation::Http2Reset => build_app(state),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });

    let gate = Arc::new(EffectGate::new());
    let callback_gate = Arc::clone(&gate);
    let _failpoint =
        ScopedFailPoint::with_callback(names::MUTATION_POST_FINALIZE_PRE_PUBLISHER, move || {
            callback_gate.park_after_effect()
        });
    let client = match kind {
        Cancellation::Http1Disconnect => reqwest::Client::builder().http1_only().build().unwrap(),
        Cancellation::Http2Reset => reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap(),
        Cancellation::TimeoutMiddleware => reqwest::Client::new(),
    };
    let body = ChangeRequest {
        query: MUTATION.to_string(),
        name: Some("insert_person".to_string()),
        params: Some(json!({"name": "Terminal", "age": 42})),
        branch: Some("main".to_string()),
    };
    let request = tokio::spawn(async move {
        client
            .post(format!("http://{address}/graphs/default/mutate"))
            .json(&body)
            .send()
            .await
    });

    gate.wait_until_reached().await;
    match kind {
        Cancellation::Http1Disconnect | Cancellation::Http2Reset => {
            request.abort();
            assert!(request.await.unwrap_err().is_cancelled());
        }
        Cancellation::TimeoutMiddleware => {
            let response = tokio::time::timeout(Duration::from_secs(3), request)
                .await
                .expect("timeout middleware did not answer")
                .unwrap()
                .unwrap();
            assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        }
    }
    gate.release();

    // Terminal publication, not a reopen or replay, consumes the sidecar.
    let recovery_dir = graph.join("__recovery");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let remaining = std::fs::read_dir(&recovery_dir)
                .map(|entries| entries.count())
                .unwrap_or(0);
            if remaining == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned write did not reach terminal publication");

    let observer = Omnigraph::open_read_only(&uri).await.unwrap();
    let snapshot = observer
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(snapshot.entry("node:Person").unwrap().row_count, 1);

    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(write_cancellation)]
async fn http1_disconnect_does_not_cancel_armed_write() {
    assert_cancellation_independent_write(Cancellation::Http1Disconnect).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(write_cancellation)]
async fn http2_reset_does_not_cancel_armed_write() {
    assert_cancellation_independent_write(Cancellation::Http2Reset).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(write_cancellation)]
async fn timeout_middleware_does_not_cancel_armed_write() {
    assert_cancellation_independent_write(Cancellation::TimeoutMiddleware).await;
}
