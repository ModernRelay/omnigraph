//! Provider selection carried by the graph's own Lance registry.

use async_trait::async_trait;
use lance::io::{ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor};
use lance_io::object_store::{ObjectStore, ObjectStoreProvider, StorageOptions};
use object_store::{aws::AmazonS3ConfigKey, azure::AzureConfigKey, path::Path};
use omnigraph_storage::{CapturedStorageBinding, StorageIoScope, StorageKind};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use url::Url;

pub(super) const MARKER: &str = "omnigraph-captured-provider";

pub(super) fn registry(
    binding: CapturedStorageBinding,
    scope: Option<StorageIoScope>,
) -> Arc<ObjectStoreRegistry> {
    let registry = match scope {
        Some(scope) => super::scoped::registry(scope),
        None => {
            let registry = ObjectStoreRegistry::default();
            #[cfg(feature = "dst")]
            super::object_store_seam::hook(&registry);
            Arc::new(registry)
        }
    };
    let schemes: &[&str] = match binding.kind() {
        StorageKind::Local => &["file", "file-object-store"],
        StorageKind::S3 => &["s3"],
        StorageKind::Azure => &["az", "abfss"],
    };
    for scheme in schemes {
        if let Some(inner) = registry.get_provider(scheme) {
            let provider = Arc::new(BoundProvider {
                inner,
                binding: binding.clone(),
            });
            registry.insert(scheme, provider.clone());
            // A local registry capability, never global or a mutable lookup table.
            // Parameter construction uses it to avoid consulting ambient Azure config.
            registry.insert(MARKER, provider);
        }
    }
    registry
}

#[derive(Debug)]
struct BoundProvider {
    inner: Arc<dyn ObjectStoreProvider>,
    binding: CapturedStorageBinding,
}

impl BoundProvider {
    fn options(&self, supplied: Option<&HashMap<String, String>>) -> HashMap<String, String> {
        let mut options = supplied.cloned().unwrap_or_default();
        // Remove aliases before inserting the canonical captured options, so
        // HashMap iteration can never choose an ambient alias over the binding.
        options.retain(|key, _| match self.binding.kind() {
            StorageKind::S3 => AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).is_err(),
            StorageKind::Azure => AzureConfigKey::from_str(&key.to_ascii_lowercase()).is_err(),
            StorageKind::Local => true,
        });
        options.extend(self.binding.lance_options());
        options
    }
}

#[async_trait]
impl ObjectStoreProvider for BoundProvider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance_core::Result<ObjectStore> {
        if params
            .storage_options_accessor
            .as_ref()
            .is_some_and(|accessor| accessor.has_provider())
        {
            return Err(lance_core::Error::invalid_input(
                "a captured target cannot accept a dynamic storage-option provider; native credential refresh remains available",
            ));
        }
        // Resolve the native provider's own environment merge before handing
        // it a complete map. Azure retains its first environment snapshot;
        // validating only today's process environment would miss that cache.
        let mut options = StorageOptions::new(self.options(params.storage_options()));
        match self.binding.kind() {
            StorageKind::S3 => options.with_env_s3(),
            StorageKind::Azure => options.with_env_azure(),
            StorageKind::Local => {}
        }
        self.binding
            .validate_native_options(&options.0)
            .map_err(|error| lance_core::Error::invalid_input(error.to_string()))?;
        let mut params = params.clone();
        params.storage_options_accessor = Some(Arc::new(
            StorageOptionsAccessor::with_static_options(options.0),
        ));
        self.inner.new_store(base_path, &params).await
    }
    fn extract_path(&self, url: &Url) -> lance_core::Result<Path> {
        self.inner.extract_path(url)
    }
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        supplied: Option<&HashMap<String, String>>,
    ) -> lance_core::Result<String> {
        self.inner
            .calculate_object_store_prefix(url, Some(&self.options(supplied)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_storage::StorageDestination;

    #[test]
    fn registry_captures_account_identity_and_overrides_conflicting_aliases() {
        let binding = CapturedStorageBinding::capture(
            "az://graphs/g",
            &StorageDestination::Azure {
                account_name: "firstaccount".into(),
                endpoint: None,
                emulator: false,
            },
        )
        .unwrap();
        let second = CapturedStorageBinding::capture(
            "az://graphs/g",
            &StorageDestination::Azure {
                account_name: "secondaccount".into(),
                endpoint: None,
                emulator: false,
            },
        )
        .unwrap();
        let first_registry = registry(binding.clone(), None);
        let second_registry = registry(second, None);
        assert!(!Arc::ptr_eq(&first_registry, &second_registry));
        let uri = Url::parse("az://graphs/g/table.lance").unwrap();
        let hostile = HashMap::from([
            ("account_name".into(), "wrongaccount".into()),
            ("endpoint".into(), "https://wrong.example".into()),
        ]);
        let first_prefix = first_registry
            .get_provider("az")
            .unwrap()
            .calculate_object_store_prefix(&uri, Some(&hostile))
            .unwrap();
        let second_prefix = second_registry
            .get_provider("az")
            .unwrap()
            .calculate_object_store_prefix(&uri, None)
            .unwrap();
        assert!(first_prefix.contains("firstaccount"));
        assert!(!first_prefix.contains("wrongaccount"));
        assert!(second_prefix.contains("secondaccount"));
        assert_ne!(first_prefix, second_prefix);
        let provider = BoundProvider {
            inner: ObjectStoreRegistry::default().get_provider("az").unwrap(),
            binding,
        };
        let resolved = provider.options(Some(&hostile));
        assert!(!resolved.contains_key("account_name"));
        assert_eq!(
            resolved.get("azure_storage_endpoint").map(String::as_str),
            Some("https://firstaccount.blob.core.windows.net/")
        );
    }
    #[tokio::test]
    async fn captured_azure_destination_is_shared_by_control_and_lance() {
        use object_store::ObjectStoreExt;
        use omnigraph_storage::StorageAdapter;
        use std::io::{Read, Write};
        use std::time::{Duration, Instant};

        // One small real HTTP endpoint distinguishes account paths. No process
        // environment mutation, Azure service, or network credential is needed.
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut paths = Vec::new();
            while paths.len() < 4 {
                let (mut socket, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "captured provider did not reach selected endpoint"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("accept failed: {error}"),
                };
                // macOS accepted sockets inherit the nonblocking listener mode.
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut bytes = [0_u8; 16384];
                let mut len = 0;
                loop {
                    let read = socket.read(&mut bytes[len..]).unwrap();
                    assert!(read > 0);
                    len += read;
                    if bytes[..len].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    assert!(len < bytes.len());
                }
                let request = std::str::from_utf8(&bytes[..len]).unwrap();
                assert!(request.starts_with("GET "));
                let path = request.split_whitespace().nth(1).unwrap();
                let body = if path.starts_with("/firstaccount/") {
                    "first"
                } else if path.starts_with("/secondaccount/") {
                    "second"
                } else {
                    panic!("wrong provider authority: {path}")
                };
                paths.push(path.to_owned());
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"test\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
                socket.flush().unwrap();
            }
            paths
        });
        let uri = "az://graphs/cluster/check";
        for (account, expected) in [("firstaccount", "first"), ("secondaccount", "second")] {
            let binding = CapturedStorageBinding::capture(
                uri,
                &StorageDestination::Azure {
                    account_name: account.into(),
                    endpoint: Some(endpoint.clone()),
                    emulator: true,
                },
            )
            .unwrap();
            let control = binding.adapter(None).unwrap();
            assert_eq!(control.read_text(uri).await.unwrap(), expected);
            let registry = registry(binding, None);
            let (store, path) =
                ObjectStore::from_uri_and_params(registry, uri, &ObjectStoreParams::default())
                    .await
                    .unwrap();
            let bytes = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
            assert_eq!(bytes.as_ref(), expected.as_bytes());
        }
        let paths = server.await.unwrap();
        assert_eq!(
            paths,
            [
                "/firstaccount/graphs/cluster/check",
                "/firstaccount/graphs/cluster/check",
                "/secondaccount/graphs/cluster/check",
                "/secondaccount/graphs/cluster/check"
            ]
        );
    }
}
