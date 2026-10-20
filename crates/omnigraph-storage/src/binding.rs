//! One immutable provider selection shared by control objects and Lance.

use super::*;
use object_store::{ClientConfigKey, RetryConfig, aws::AmazonS3ConfigKey};
use std::time::Duration;

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
                let location = parse_s3_uri(&uri)?;
                if location.bucket.ends_with("--x-s3") {
                    return Err(binding_error(
                        "S3 Express requires a distinct provider binding and is not a supported direct target",
                    ));
                }
                // Only authentication is inherited. Destination and transport
                // settings come from the binding or fixed supported defaults.
                for (key, value) in environment_values() {
                    if let Some((key, true)) = provider_option(StorageKind::S3, &key) {
                        if options.get(&key).is_some_and(|prior| prior != &value) {
                            return Err(binding_error(format!(
                                "conflicting authentication aliases for provider option '{key}'"
                            )));
                        }
                        options.insert(key, value);
                    }
                }
                let endpoint = endpoint.clone().unwrap_or_else(|| {
                    let domain = if region.starts_with("cn-") {
                        "amazonaws.com.cn"
                    } else {
                        "amazonaws.com"
                    };
                    // Native S3 expects an explicit virtual-host endpoint to
                    // include the bucket; path-style endpoints omit it.
                    if *virtual_hosted_style {
                        format!("https://{}.s3.{region}.{domain}", location.bucket)
                    } else {
                        format!("https://s3.{region}.{domain}")
                    }
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
                options.insert(AmazonS3ConfigKey::Bucket.as_ref().into(), location.bucket);
                for key in [
                    AmazonS3ConfigKey::S3Express,
                    AmazonS3ConfigKey::SkipSignature,
                    AmazonS3ConfigKey::UnsignedPayload,
                    AmazonS3ConfigKey::ImdsV1Fallback,
                    AmazonS3ConfigKey::DisableTagging,
                    AmazonS3ConfigKey::DisableBulkDelete,
                    AmazonS3ConfigKey::RequestPayer,
                ] {
                    options.insert(key.as_ref().into(), "false".into());
                }
                options.insert(
                    AmazonS3ConfigKey::ConditionalPut.as_ref().into(),
                    "etag".into(),
                );
                fixed_transport_options(&mut options, endpoint.starts_with("http://"));
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
                root.azure_options.retain(|key, _| {
                    provider_option(StorageKind::Azure, key).is_some_and(|(_, auth)| auth)
                        || matches!(
                            AzureConfigKey::from_str(key),
                            Ok(AzureConfigKey::AccountName
                                | AzureConfigKey::Endpoint
                                | AzureConfigKey::UseEmulator
                                | AzureConfigKey::UseFabricEndpoint)
                        )
                });
                root.azure_options.insert(
                    AzureConfigKey::ContainerName.as_ref().into(),
                    root.container.clone(),
                );
                for key in [
                    AzureConfigKey::SkipSignature,
                    AzureConfigKey::DisableTagging,
                ] {
                    root.azure_options
                        .insert(key.as_ref().into(), "false".into());
                }
                options = root.lance_storage_options()?;
                fixed_transport_options(&mut options, *emulator);
                root.azure_options.extend(options.clone());
                azure = Some(root);
            }
            _ => {
                return Err(binding_error(
                    "storage destination kind does not match its URI",
                ));
            }
        }
        if !matches!(destination, StorageDestination::Local) {
            let retries = env::var("OBJECT_STORE_CLIENT_MAX_RETRIES").ok();
            let timeout = env::var("OBJECT_STORE_CLIENT_RETRY_TIMEOUT").ok();
            let retry = captured_retry_config(retries.as_deref(), timeout.as_deref())?;
            options.insert("client_max_retries".into(), retry.max_retries.to_string());
            options.insert(
                "client_retry_timeout".into(),
                retry.retry_timeout.as_secs().to_string(),
            );
            if let Some(root) = &mut azure {
                for key in ["client_max_retries", "client_retry_timeout"] {
                    root.azure_options.insert(key.into(), options[key].clone());
                }
            }
        }
        options.insert("use_opendal".into(), "false".into());
        let binding = Self {
            requested_uri,
            uri,
            destination: destination.clone(),
            options,
            azure,
        };
        binding.validate_environment()?;
        Ok(binding)
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

    /// Check the native provider's merged options, including its cached or
    /// live environment refill. Authentication remains native; every other
    /// recognized option must equal the captured setting or fixed safe default.
    #[doc(hidden)]
    pub fn validate_native_options(&self, options: &HashMap<String, String>) -> Result<()> {
        if self.kind() == StorageKind::Local {
            return Ok(());
        }
        self.validate_environment()?;
        for key in ["client_max_retries", "client_retry_timeout"] {
            if options.get(key) != self.options.get(key) {
                return Err(unsupported_option(key));
            }
        }
        for (key, value) in options {
            if let Some((canonical, auth)) = provider_option(self.kind(), key)
                && !auth
                && self.options.get(&canonical) != Some(value)
            {
                return Err(unsupported_option(&canonical));
            }
        }
        Ok(())
    }

    fn validate_environment(&self) -> Result<()> {
        if self.kind() == StorageKind::Local {
            return Ok(());
        }
        for (key, value) in environment_values() {
            let lower = key.to_ascii_lowercase();
            // reqwest also reads these independently of provider options.
            if matches!(lower.as_str(), "http_proxy" | "https_proxy" | "all_proxy")
                && !value.is_empty()
            {
                return Err(unsupported_option(&lower));
            }
            // Lance applies these *after* explicit options. Refuse any value
            // that could change the captured transport; never print its value.
            if matches!(
                key.as_str(),
                "AWS_ALLOW_HTTP" | "AZURE_STORAGE_ALLOW_HTTP" | "AZURE_STORAGE_USE_HTTP"
            ) && self.options.get("allow_http").map(String::as_str) != Some(value.as_str())
            {
                return Err(unsupported_option(&lower));
            }
            let retry_key = match key.as_str() {
                "OBJECT_STORE_CLIENT_MAX_RETRIES" => Some("client_max_retries"),
                "OBJECT_STORE_CLIENT_RETRY_TIMEOUT" => Some("client_retry_timeout"),
                _ => None,
            };
            if let Some(retry_key) = retry_key
                && self.options.get(retry_key) != Some(&value)
            {
                return Err(unsupported_option(&lower));
            }
            if let Some((canonical, auth)) = provider_option(self.kind(), &key)
                && !auth
                && !self.options.contains_key(&canonical)
            {
                return Err(unsupported_option(&canonical));
            }
        }
        Ok(())
    }

    pub fn adapter(&self, scope: Option<StorageIoScope>) -> Result<ObjectStorageAdapter> {
        self.validate_environment()?;
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
                builder = builder.with_retry(if scope.is_some() {
                    single_attempt_retry_config()
                } else {
                    captured_retry_config(
                        self.options.get("client_max_retries").map(String::as_str),
                        self.options.get("client_retry_timeout").map(String::as_str),
                    )?
                });
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

// The native providers accept prefixed and unprefixed aliases. Classify with
// their parser instead of maintaining a second spelling table. Only these
// authentication classes may consult the native credential chain; all other
// recognized options need an explicit value in the captured map. Newly added
// native keys therefore refuse until classified here.
fn provider_option(kind: StorageKind, key: &str) -> Option<(String, bool)> {
    let key = key.to_ascii_lowercase();
    match kind {
        StorageKind::S3 => {
            use AmazonS3ConfigKey::*;
            let key = AmazonS3ConfigKey::from_str(&key).ok()?;
            let auth = matches!(
                key,
                AccessKeyId
                    | SecretAccessKey
                    | Token
                    | MetadataEndpoint
                    | ContainerCredentialsRelativeUri
                    | ContainerCredentialsFullUri
                    | ContainerAuthorizationTokenFile
                    | WebIdentityTokenFile
                    | RoleArn
                    | RoleSessionName
                    | StsEndpoint
            );
            Some((key.as_ref().into(), auth))
        }
        StorageKind::Azure => {
            use AzureConfigKey::*;
            let key = AzureConfigKey::from_str(&key).ok()?;
            let auth = matches!(
                key,
                AccessKey
                    | ClientId
                    | ClientSecret
                    | AuthorityId
                    | AuthorityHost
                    | SasKey
                    | Token
                    | MsiEndpoint
                    | ObjectId
                    | MsiResourceId
                    | FederatedTokenFile
                    | UseAzureCli
                    | CredentialType
            );
            Some((key.as_ref().into(), auth))
        }
        StorageKind::Local => None,
    }
}

pub(super) fn captured_retry_config(
    max_retries: Option<&str>,
    retry_timeout: Option<&str>,
) -> Result<RetryConfig> {
    let max_retries = max_retries
        .unwrap_or("3")
        .parse::<usize>()
        .ok()
        .filter(|n| *n <= 3)
        .ok_or_else(|| unsupported_option("client_max_retries"))?;
    let retry_timeout = retry_timeout
        .unwrap_or("180")
        .parse::<u64>()
        .ok()
        .filter(|n| (1..=180).contains(n))
        .ok_or_else(|| unsupported_option("client_retry_timeout"))?;
    Ok(RetryConfig {
        max_retries,
        retry_timeout: Duration::from_secs(retry_timeout),
        ..Default::default()
    })
}

fn fixed_transport_options(options: &mut HashMap<String, String>, allow_http: bool) {
    options.insert(
        ClientConfigKey::AllowHttp.as_ref().into(),
        allow_http.to_string(),
    );
    for key in [
        ClientConfigKey::AllowInvalidCertificates,
        ClientConfigKey::NoSystemCertificates,
    ] {
        options.insert(key.as_ref().into(), "false".into());
    }
}

fn environment_values() -> impl Iterator<Item = (String, String)> {
    env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
}

fn unsupported_option(key: &str) -> StorageError {
    binding_error(format!(
        "captured target does not support provider option '{key}'; remove the ambient override"
    ))
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
        for (key, value) in [
            ("aws_skip_signature", "false"),
            ("aws_unsigned_payload", "false"),
            ("aws_conditional_put", "etag"),
            ("allow_invalid_certificates", "false"),
        ] {
            assert_eq!(options.get(key).map(String::as_str), Some(value), "{key}");
        }
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

        // Native S3 treats an explicit virtual-host endpoint as already
        // bucket-qualified. Capturing the default must preserve that contract.
        for (region, virtual_hosted_style, endpoint, expected_endpoint) in [
            (
                "us-west-2",
                false,
                None,
                "https://s3.us-west-2.amazonaws.com",
            ),
            (
                "us-west-2",
                true,
                None,
                "https://bucket.s3.us-west-2.amazonaws.com",
            ),
            (
                "cn-north-1",
                false,
                None,
                "https://s3.cn-north-1.amazonaws.com.cn",
            ),
            (
                "cn-north-1",
                true,
                None,
                "https://bucket.s3.cn-north-1.amazonaws.com.cn",
            ),
            (
                "us-west-2",
                false,
                Some("https://objects.example.test"),
                "https://objects.example.test",
            ),
            (
                "us-west-2",
                true,
                Some("https://bucket.objects.example.test"),
                "https://bucket.objects.example.test",
            ),
        ] {
            let binding = CapturedStorageBinding::capture(
                "s3://bucket/cluster/graphs/g",
                &StorageDestination::S3 {
                    region: region.into(),
                    endpoint: endpoint.map(str::to_owned),
                    virtual_hosted_style,
                },
            )
            .unwrap();
            let options = binding.lance_options();
            for key in ["aws_endpoint", "aws_endpoint_url_s3"] {
                assert_eq!(
                    options.get(key).map(String::as_str),
                    Some(expected_endpoint),
                    "{region}, virtual_hosted_style={virtual_hosted_style}, {key}"
                );
            }
            assert_eq!(
                options.get("aws_virtual_hosted_style_request"),
                Some(&virtual_hosted_style.to_string())
            );
        }
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
