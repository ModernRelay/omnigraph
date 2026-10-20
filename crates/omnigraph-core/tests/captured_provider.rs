//! Process environment is a native provider input, so each case starts alone.

use lance_io::object_store::{ObjectStore, ObjectStoreParams, StorageOptions};
use object_store::{ObjectStoreExt, PutMode, PutOptions};
use omnigraph_core::{lance_access::LanceAccessContext, storage::storage_for_binding};
use omnigraph_storage::{CapturedStorageBinding, StorageDestination};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    process::Command,
    time::{Duration, Instant},
};

#[test]
fn captured_provider_native_environment() {
    const CASE: &str = "OMNIGRAPH_CAPTURED_PROVIDER_CASE";
    if let Ok(case) = std::env::var(CASE) {
        run_case(&case);
        return;
    }
    for case in [
        "s3_capture",
        "s3_drift",
        "azure_capture",
        "azure_drift",
        "s3_proxy_capture",
        "s3_proxy_drift",
        "azure_cached_proxy",
        "s3_native_credentials",
        "generic_proxy",
    ] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "captured_provider_native_environment",
                "--nocapture",
                "--test-threads=1",
            ])
            .env_clear()
            .env(CASE, case)
            .env("AWS_ACCESS_KEY_ID", "test-key")
            .env("AWS_SECRET_ACCESS_KEY", "test-secret")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{case}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn run_case(case: &str) {
    // The child runs one synchronous test. Environment changes finish before
    // starting the runtime or HTTP worker; no other thread reads environment.
    let set = |key, value| unsafe { std::env::set_var(key, value) };
    let unset = |key| unsafe { std::env::remove_var(key) };
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let azure = case.starts_with("azure_");
    let uri = if azure {
        "az://captured/cluster/object"
    } else {
        "s3://captured/cluster/object"
    };
    let destination = if azure {
        StorageDestination::Azure {
            account_name: "devstoreaccount1".into(),
            endpoint: Some(endpoint),
            emulator: true,
        }
    } else {
        StorageDestination::S3 {
            region: "us-west-2".into(),
            endpoint: Some(endpoint),
            virtual_hosted_style: false,
        }
    };
    let hostile = || {
        if azure {
            set("AZURE_SKIP_SIGNATURE", "true");
            set("AZURE_ALLOW_INVALID_CERTIFICATES", "true");
            set("AZURE_STORAGE_ACCOUNT_NAME", "wrongaccount");
            set("AZURE_STORAGE_ENDPOINT", "https://wrong.invalid");
            return;
        }
        set("AWS_SKIP_SIGNATURE", "true");
        set("SKIP_SIGNATURE", "true");
        set("AWS_UNSIGNED_PAYLOAD", "true");
        set("AWS_CONDITIONAL_PUT", "disabled");
        set("ALLOW_INVALID_CERTIFICATES", "true");
        set("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:1");
        set("AWS_REGION", "wrong-region");
        set("AWS_BUCKET", "wrong-bucket");
        set("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true");
    };
    if matches!(case, "s3_capture" | "azure_capture") {
        hostile();
    }
    if case == "s3_proxy_capture" {
        set("AWS_PROXY_URL", "http://sensitive.invalid:9");
    }
    if case == "generic_proxy" {
        for key in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            set(key, "http://sensitive.invalid:9");
            let error = CapturedStorageBinding::capture(uri, &destination)
                .unwrap_err()
                .to_string();
            assert!(error.contains(&key.to_ascii_lowercase()), "{error}");
            assert!(!error.contains("sensitive.invalid"), "{error}");
            unset(key);
        }
        assert!(listener.accept().is_err());
        return;
    }
    if case == "azure_cached_proxy" {
        set("AZURE_PROXY_URL", "http://sensitive.invalid:9");
        let mut native = StorageOptions::new(HashMap::new());
        native.with_env_azure();
        unset("AZURE_PROXY_URL");
    }
    let captured = CapturedStorageBinding::capture(uri, &destination);
    if matches!(case, "s3_proxy_capture" | "generic_proxy") {
        let error = captured.unwrap_err().to_string();
        assert!(error.contains("proxy"), "{error}");
        assert!(!error.contains("sensitive.invalid"), "{error}");
        assert!(listener.accept().is_err());
        return;
    }
    let binding = captured.unwrap();
    let before_drift = storage_for_binding(&binding).unwrap();
    let context = LanceAccessContext::with_storage(before_drift.as_ref());
    if matches!(case, "s3_drift" | "azure_drift") {
        hostile();
    }
    if case == "s3_proxy_drift" {
        set("PROXY_URL", "http://sensitive.invalid:9");
    }
    if case == "s3_proxy_drift" {
        let error = binding.adapter(None).unwrap_err().to_string();
        assert!(error.contains("proxy_url"), "{error}");
        assert!(!error.contains("sensitive.invalid"), "{error}");
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if matches!(case, "s3_proxy_drift" | "azure_cached_proxy") {
        // The registry was prepared before drift; a later native open must
        // still refuse an unbound setting, including Azure's cached values.
        let error = runtime
            .block_on(ObjectStore::from_uri_and_params(
                context.data_session().store_registry(),
                binding.uri(),
                &ObjectStoreParams::default(),
            ))
            .unwrap_err()
            .to_string();
        assert!(error.contains("proxy_url"), "{error}");
        assert!(!error.contains("sensitive.invalid"), "{error}");
        assert!(listener.accept().is_err());
        return;
    }
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut requests = Vec::new();
        while requests.len() < 4 {
            let (mut socket, _) = match listener.accept() {
                Ok(pair) => pair,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "missing selected-endpoint request: {requests:?}"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(e) => panic!("accept: {e}"),
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0; 4096];
            let header_end = loop {
                let n = socket.read(&mut buffer).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break end + 4;
                }
                assert!(bytes.len() < 16384);
            };
            let request = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
            let lower = request.to_ascii_lowercase();
            let length = lower
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .map(|s| s.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            while bytes.len() < header_end + length {
                let n = socket.read(&mut buffer).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let body = if request.starts_with("GET ") {
                "selected"
            } else {
                ""
            };
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"test\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            socket.flush().unwrap();
            requests.push(lower);
        }
        requests
    });
    runtime.block_on(async {
        let control = storage_for_binding(&binding).unwrap();
        assert_eq!(control.read_text(uri).await.unwrap(), "selected");
        assert!(control.write_text_if_absent(uri, "new").await.unwrap());
        let context = LanceAccessContext::with_storage(control.as_ref());
        let mut params = ObjectStoreParams::default();
        if case == "s3_native_credentials" {
            params.aws_credentials = Some(Arc::new(RotatingCredentials::default()));
        }
        let (store, path) =
            ObjectStore::from_uri_and_params(context.data_session().store_registry(), uri, &params)
                .await
                .unwrap();
        assert_eq!(
            store
                .inner
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"selected"
        );
        store
            .inner
            .put_opts(
                &path,
                b"new".as_slice().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    });
    let requests = server.join().unwrap();
    if case == "s3_native_credentials" {
        assert!(
            requests[2].contains("credential=native-1/"),
            "{}",
            requests[2]
        );
        assert!(
            requests[3].contains("credential=native-2/"),
            "{}",
            requests[3]
        );
    }
    for request in requests {
        let prefix = if azure {
            "/devstoreaccount1/captured/cluster/object"
        } else {
            "/captured/cluster/object"
        };
        assert!(
            request.starts_with(&format!("get {prefix} "))
                || request.starts_with(&format!("put {prefix} ")),
            "{request}"
        );
        let auth = if azure {
            "authorization: sharedkey devstoreaccount1:"
        } else {
            "authorization: aws4-hmac-sha256"
        };
        assert!(request.contains(auth), "unsigned request: {request}");
        assert!(
            !request.contains("unsigned-payload"),
            "unsigned payload: {request}"
        );
        if request.starts_with("put ") {
            assert!(
                request.contains("if-none-match: *"),
                "unconditional create: {request}"
            );
        }
    }
}

#[derive(Debug, Default)]
struct RotatingCredentials(AtomicUsize);

#[async_trait::async_trait]
impl object_store::CredentialProvider for RotatingCredentials {
    type Credential = object_store::aws::AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        Ok(Arc::new(Self::Credential {
            key_id: format!("native-{}", self.0.fetch_add(1, Ordering::SeqCst) + 1),
            secret_key: "test-secret".into(),
            token: None,
        }))
    }
}
