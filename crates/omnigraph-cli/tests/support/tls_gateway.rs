//! TLS termination only: one accepted connection opens one upstream connection.
//! No HTTP parsing, discovery, retry, or replay is performed here, so the
//! existing HTTP fixtures remain the owners of request and fault assertions.
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use tokio_rustls::{TlsAcceptor, rustls};

struct Identity {
    _directory: tempfile::TempDir,
    ca: std::path::PathBuf,
    pem: String,
    config: std::sync::Arc<rustls::ServerConfig>,
}

fn identity() -> &'static Identity {
    static IDENTITY: OnceLock<Identity> = OnceLock::new();
    IDENTITY.get_or_init(|| {
        let certified =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
                .unwrap();
        let pem = certified.cert.pem();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der());
        let provider = rustls::crypto::ring::default_provider();
        let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![certified.cert.der().clone()], key.into())
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let ca = directory.path().join("fixture-ca.pem");
        std::fs::write(&ca, &pem).unwrap();
        Identity {
            _directory: directory,
            ca,
            pem,
            config: std::sync::Arc::new(config),
        }
    })
}

pub fn ca_path() -> &'static Path {
    &identity().ca
}

pub fn certificate() -> reqwest::Certificate {
    reqwest::Certificate::from_pem(identity().pem.as_bytes()).unwrap()
}

fn upstreams() -> &'static Mutex<BTreeMap<String, String>> {
    static UPSTREAMS: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();
    UPSTREAMS.get_or_init(Mutex::default)
}

/// Fault fixtures may speak raw HTTP to the underlying test server. This is
/// deliberately unavailable to production code and never used by CLI children.
pub fn http_upstream(origin: &str) -> String {
    upstreams()
        .lock()
        .unwrap()
        .get(origin)
        .cloned()
        .unwrap_or_else(|| origin.into())
}

pub struct TlsGateway {
    pub origin: String,
    _anonymous: tempfile::TempDir,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TlsGateway {
    pub fn new(upstream: &str) -> Self {
        let upstream = url::Url::parse(upstream).unwrap();
        assert_eq!(upstream.scheme(), "http");
        assert_eq!(upstream.host_str(), Some("127.0.0.1"));
        let address = format!("127.0.0.1:{}", upstream.port().unwrap());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("https://{}", listener.local_addr().unwrap());
        let anonymous = tempfile::tempdir().unwrap();
        std::fs::write(anonymous.path().join("config.yaml"), format!(
            "version: 1\ntargets:\n  test-server:\n    kind: server\n    endpoint: {origin}\n    default_auth: public\n    auth:\n      public:\n        provider: anonymous\n    transport:\n      tls:\n        ca: {{mode: replace, file: '{}'}}\n", ca_path().display()
        )).unwrap();
        anonymous_homes()
            .lock()
            .unwrap()
            .insert(origin.clone(), anonymous.path().to_owned());
        upstreams().lock().unwrap().insert(
            origin.clone(),
            upstream.to_string().trim_end_matches('/').into(),
        );
        let acceptor = TlsAcceptor::from(identity().config.clone());
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        _ = &mut stopped => break,
                        Some(_) = connections.join_next(), if !connections.is_empty() => {},
                        incoming = listener.accept(), if connections.len() < 64 => {
                            let (socket, _) = incoming.unwrap();
                            let acceptor = acceptor.clone();
                            let address = address.clone();
                            connections.spawn(async move {
                                let _ = tokio::time::timeout(std::time::Duration::from_secs(90), async move {
                                    let mut downstream = acceptor.accept(socket).await?;
                                    let mut upstream = tokio::net::TcpStream::connect(address).await?;
                                    tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await?;
                                    Ok::<_, std::io::Error>(())
                                }).await;
                            });
                        }
                    }
                }
                connections.abort_all();
                while connections.join_next().await.is_some() {}
            });
        });
        Self {
            origin,
            _anonymous: anonymous,
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for TlsGateway {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.thread.take().unwrap().join().unwrap();
        upstreams().lock().unwrap().remove(&self.origin);
        anonymous_homes().lock().unwrap().remove(&self.origin);
    }
}

fn anonymous_homes() -> &'static Mutex<BTreeMap<String, std::path::PathBuf>> {
    static HOMES: OnceLock<Mutex<BTreeMap<String, std::path::PathBuf>>> = OnceLock::new();
    HOMES.get_or_init(Mutex::default)
}

pub fn anonymous_home(origin: &str) -> std::path::PathBuf {
    anonymous_homes()
        .lock()
        .unwrap()
        .get(origin)
        .unwrap_or_else(|| panic!("not a shared TLS fixture: {origin}"))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_home_pins_its_endpoint_and_ca_and_is_removed_on_drop() {
        let upstream = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let gateway = TlsGateway::new(&format!("http://{}", upstream.local_addr().unwrap()));
        let origin = gateway.origin.clone();
        let home = anonymous_home(&origin);
        let config: serde_json::Value =
            yaml_serde::from_str(&std::fs::read_to_string(home.join("config.yaml")).unwrap())
                .unwrap();
        let target = &config["targets"]["test-server"];
        assert_eq!(config["version"], 1);
        assert_eq!(target["endpoint"], origin);
        assert_eq!(target["default_auth"], "public");
        assert_eq!(target["auth"]["public"]["provider"], "anonymous");
        assert_eq!(target["transport"]["tls"]["ca"]["mode"], "replace");
        assert_eq!(
            target["transport"]["tls"]["ca"]["file"],
            ca_path().to_str().unwrap()
        );
        reqwest::Client::builder()
            .tls_built_in_root_certs(false)
            .add_root_certificate(certificate())
            .build()
            .unwrap();
        drop(gateway);
        assert!(!home.exists());
        assert!(!anonymous_homes().lock().unwrap().contains_key(&origin));
        assert!(!upstreams().lock().unwrap().contains_key(&origin));
    }
}
