//! One immutable provider selection shared by control objects and Lance.

use super::*;
use object_store::aws::AmazonS3ConfigKey;

/// Non-secret destination settings. Credentials retain the native provider chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageDestination {
    Local,
    S3 {
        region: String,
        endpoint: Option<String>,
        virtual_hosted_style: bool,
    },
    Azure {
        account_name: String,
        endpoint: Option<String>,
        emulator: bool,
    },
}

/// Captured once at an invocation boundary. Rebinding a related URI does not
/// reread provider configuration or change its account/service authority.
#[derive(Clone)]
pub struct CapturedStorageBinding {
    requested_uri: String,
    uri: String,
    destination: StorageDestination,
    options: HashMap<String, String>,
    azure: Option<CanonicalAzureRoot>,
}

impl fmt::Debug for CapturedStorageBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapturedStorageBinding")
            .field("uri", &redacted_storage_uri(&self.uri))
            .field("destination", &self.destination)
            .finish_non_exhaustive()
    }
}

impl CapturedStorageBinding {
    pub fn capture(uri: &str, destination: &StorageDestination) -> Result<Self> {
        let requested_uri = normalize_root_uri(uri)?;
        let uri = write_queue_root_identity(&requested_uri)?;
        let mut options = HashMap::new();
        let mut azure = None;
        match (storage_kind_for_uri(&uri)?, destination) {
            (StorageKind::Local, StorageDestination::Local) => {}
            (
                StorageKind::S3,
                StorageDestination::S3 {
                    region,
                    endpoint,
                    virtual_hosted_style,
                },
            ) => {
                if region.is_empty()
                    || !region
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                {
                    return Err(binding_error(
                        "a captured S3 destination requires an explicit valid region",
                    ));
                }
                if parse_s3_uri(&uri)?.bucket.ends_with("--x-s3") {
                    return Err(binding_error(
                        "S3 Express requires a distinct provider binding and is not a supported direct target",
                    ));
                }
                for (key, value) in env::vars() {
                    if key.starts_with("AWS_")
                        && let Ok(key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
                    {
                        options.insert(key.as_ref().to_owned(), value);
                    }
                }
                let endpoint = endpoint.clone().unwrap_or_else(|| {
                    format!(
                        "https://s3.{region}.{}",
                        if region.starts_with("cn-") {
                            "amazonaws.com.cn"
                        } else {
                            "amazonaws.com"
                        }
                    )
                });
                validate_endpoint(&endpoint)?;
                for key in [AmazonS3ConfigKey::Region, AmazonS3ConfigKey::DefaultRegion] {
                    options.insert(key.as_ref().to_owned(), region.clone());
                }
                // The service-specific option outranks the generic endpoint in Lance.
                for key in [AmazonS3ConfigKey::Endpoint, AmazonS3ConfigKey::S3Endpoint] {
                    options.insert(key.as_ref().to_owned(), endpoint.clone());
                }
                options.insert(
                    AmazonS3ConfigKey::VirtualHostedStyleRequest.as_ref().into(),
                    virtual_hosted_style.to_string(),
                );
                options.insert(AmazonS3ConfigKey::S3Express.as_ref().into(), "false".into());
                options.remove(AmazonS3ConfigKey::Bucket.as_ref());
                if endpoint.starts_with("http://") {
                    options.insert("allow_http".into(), "true".into());
                }
            }
            (
                StorageKind::Azure,
                StorageDestination::Azure {
                    account_name,
                    endpoint,
                    emulator,
                },
            ) => {
                let mut config = AzureStorageConfig::new(account_name);
                if *emulator {
                    config = match endpoint {
                        Some(endpoint) => config.with_emulator(endpoint),
                        None => config.with_default_emulator(),
                    };
                } else if let Some(endpoint) = endpoint {
                    config = config.with_endpoint(endpoint);
                }
                let mut root = CanonicalAzureRoot::from_config(&uri, config)?;
                // This explicit binding already captured all options. Future clients
                // must use that snapshot even if another invocation changes its environment.
                root.environment_snapshot = None;
                options = root.lance_storage_options()?;
                azure = Some(root);
            }
            _ => {
                return Err(binding_error(
                    "storage destination kind does not match its URI",
                ));
            }
        }
        options.insert("use_opendal".into(), "false".into());
        Ok(Self {
            requested_uri,
            uri,
            destination: destination.clone(),
            options,
            azure,
        })
    }

    /// Original normalized spelling, retained for lexical cluster ownership probes.
    pub fn requested_uri(&self) -> &str {
        &self.requested_uri
    }

    /// Frozen execution URI; local symlinks resolve at capture time.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Authority key for process-local admission caching; never contains credentials.
    pub fn identity(&self) -> String {
        sha256_hex(
            format!(
                "omnigraph-storage-binding-v1\n{}\n{:?}",
                self.uri, self.destination
            )
            .as_bytes(),
        )
    }
    pub fn kind(&self) -> StorageKind {
        match self.destination {
            StorageDestination::Local => StorageKind::Local,
            StorageDestination::S3 { .. } => StorageKind::S3,
            StorageDestination::Azure { .. } => StorageKind::Azure,
        }
    }
    pub fn azure_root(&self) -> Option<&CanonicalAzureRoot> {
        self.azure.as_ref()
    }

    /// Derive a graph or its admission root within the same provider authority.
    /// Prefix ancestry is intentionally unrestricted: admission probes the
    /// containing cluster root before opening a graph.
    pub fn for_uri(&self, uri: &str) -> Result<Self> {
        if uri == self.uri || uri == self.requested_uri {
            return Ok(self.clone());
        }
        let requested_uri = normalize_root_uri(uri)?;
        let uri = write_queue_root_identity(&requested_uri)?;
        if storage_kind_for_uri(&uri)? != self.kind() {
            return Err(binding_error(
                "a captured storage binding cannot change provider",
            ));
        }
        match self.kind() {
            StorageKind::Local => {}
            StorageKind::S3 if parse_s3_uri(&uri)?.bucket != parse_s3_uri(&self.uri)?.bucket => {
                return Err(binding_error(
                    "a captured storage binding cannot change S3 bucket",
                ));
            }
            StorageKind::Azure
                if parse_azure_uri(&uri)?.container != parse_azure_uri(&self.uri)?.container =>
            {
                return Err(binding_error(
                    "a captured storage binding cannot change Azure container",
                ));
            }
            _ => {}
        }
        let mut binding = self.clone();
        if let Some(root) = &mut binding.azure {
            let location = parse_azure_uri(&uri)?;
            root.canonical_uri = location.canonical_uri;
            root.prefix = location.key;
            let digest_input = format!(
                "omnigraph-azure-root-v1\nservice={}\naccount={}\ncontainer={}\nroot={}",
                root.service_url, root.account_name, root.container, root.canonical_uri
            );
            root.root_digest_hex = sha256_hex(digest_input.as_bytes());
            root.admission_blob_path = format!(
                "{AZURE_ADMISSION_PREFIX}/{}/writer.lock",
                root.root_digest_hex
            );
            root.admission_blob_uri =
                format!("az://{}/{}", root.container, root.admission_blob_path);
        }
        binding.requested_uri = requested_uri;
        binding.uri = uri;
        Ok(binding)
    }

    /// Sensitive native options; pass straight into Lance's redacted accessor.
    #[doc(hidden)]
    pub fn lance_options(&self) -> HashMap<String, String> {
        self.options.clone()
    }

    pub fn adapter(&self, scope: Option<StorageIoScope>) -> Result<ObjectStorageAdapter> {
        match &self.destination {
            StorageDestination::Local => Ok(match scope {
                Some(scope) => ObjectStorageAdapter::local_scoped(scope),
                None => ObjectStorageAdapter::local(),
            }),
            StorageDestination::Azure { .. } => ObjectStorageAdapter::azure_from_root_with_scope(
                self.azure
                    .as_ref()
                    .expect("Azure capture includes its root"),
                scope,
            ),
            StorageDestination::S3 { .. } => {
                let location = parse_s3_uri(&self.uri)?;
                let mut builder = AmazonS3Builder::new().with_bucket_name(&location.bucket);
                for (key, value) in &self.options {
                    if let Ok(key) = AmazonS3ConfigKey::from_str(key) {
                        builder = builder.with_config(key, value);
                    }
                }
                if scope.is_some() {
                    builder = builder.with_retry(single_attempt_retry_config());
                }
                let store = builder.build().map_err(|_| binding_error("failed to initialize the captured S3 destination; verify its native credentials and transport options"))?;
                let adapter = ObjectStorageAdapter {
                    store: Arc::new(store),
                    scope: None,
                    codec: UriCodec::S3 {
                        bucket: location.bucket,
                    },
                    supports_conditional_update: true,
                    #[cfg(test)]
                    omit_read_etag: false,
                    #[cfg(test)]
                    omit_write_etag: false,
                };
                Ok(match scope {
                    Some(scope) => adapter.with_scope(scope),
                    None => adapter,
                })
            }
        }
    }
}

pub fn storage_handle_for_binding(
    binding: &CapturedStorageBinding,
    scope: Option<StorageIoScope>,
) -> Result<StorageHandle> {
    Ok(StorageHandle {
        adapter: Arc::new(binding.adapter(scope)?),
        kind: binding.kind(),
    })
}

fn binding_error(message: impl Into<String>) -> StorageError {
    StorageError::backend(StorageFailureKind::Configuration, message)
}
fn validate_endpoint(endpoint: &str) -> Result<()> {
    let url = Url::parse(endpoint).map_err(|_| binding_error("invalid storage endpoint"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(binding_error(
            "storage endpoint must be an HTTP(S) URL without credentials, query or fragment",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_binding_keeps_provider_authority_across_root_and_graph_uris() {
        let binding = CapturedStorageBinding::capture(
            "s3://bucket/cluster/graphs/g",
            &StorageDestination::S3 {
                region: "us-west-2".into(),
                endpoint: Some("http://127.0.0.1:9000".into()),
                virtual_hosted_style: false,
            },
        )
        .unwrap();
        let root = binding.for_uri("s3://bucket/cluster").unwrap();
        assert_eq!(root.lance_options(), binding.lance_options());
        assert_ne!(root.identity(), binding.identity());
        assert_eq!(
            root.for_uri(binding.uri()).unwrap().identity(),
            binding.identity()
        );
        assert!(binding.for_uri("s3://other/cluster").is_err());
        assert!(binding.for_uri("az://bucket/cluster").is_err());
        let options = binding.lance_options();
        assert_eq!(
            options.get("aws_region").map(String::as_str),
            Some("us-west-2")
        );
        assert_eq!(
            options.get("aws_endpoint_url_s3").map(String::as_str),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(
            options.get("aws_endpoint").map(String::as_str),
            Some("http://127.0.0.1:9000")
        );
    }

    #[test]
    fn azure_binding_uses_one_account_for_admission_native_and_lance() {
        let binding = CapturedStorageBinding::capture(
            "az://graphs/cluster/graphs/g",
            &StorageDestination::Azure {
                account_name: "devstoreaccount1".into(),
                endpoint: Some("http://127.0.0.1:12345".into()),
                emulator: true,
            },
        )
        .unwrap();
        let root = binding.for_uri("az://graphs/cluster").unwrap();
        let azure = root.azure_root().unwrap();
        assert_eq!(azure.account_name(), "devstoreaccount1");
        assert_eq!(azure.canonical_uri(), "az://graphs/cluster");
        assert!(
            azure
                .admission_blob_url()
                .unwrap()
                .as_str()
                .starts_with("http://127.0.0.1:12345/devstoreaccount1/graphs/")
        );
        assert_ne!(
            azure.admission_blob_uri(),
            binding.azure_root().unwrap().admission_blob_uri()
        );
        assert_eq!(root.lance_options(), binding.lance_options());
        assert_eq!(
            root.lance_options()
                .get("azure_storage_endpoint")
                .map(String::as_str),
            Some("http://127.0.0.1:12345/devstoreaccount1")
        );
        assert!(root.adapter(None).is_ok());
        assert!(root.for_uri("az://other/cluster").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn local_binding_freezes_nearest_existing_ancestor_before_creation() {
        let parent = tempfile::tempdir().unwrap();
        let real = parent.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let alias = parent.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let bound = CapturedStorageBinding::capture(
            alias.join("new").to_str().unwrap(),
            &StorageDestination::Local,
        )
        .unwrap();
        assert_eq!(
            bound.uri(),
            write_queue_root_identity(real.join("new").to_str().unwrap()).unwrap()
        );
        assert_eq!(
            bound.requested_uri(),
            normalize_root_uri(alias.join("new").to_str().unwrap()).unwrap()
        );
        std::fs::remove_file(&alias).unwrap();
        std::fs::create_dir(&alias).unwrap();
        assert_eq!(
            bound.for_uri(bound.requested_uri()).unwrap().uri(),
            bound.uri()
        );
        assert_eq!(
            bound.uri(),
            write_queue_root_identity(real.join("new").to_str().unwrap()).unwrap()
        );
    }
}
