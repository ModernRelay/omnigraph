//! The one Google Cloud Storage configuration a process's `gs://` roots use.
//!
//! A graph on a `gs://` root has two clients: the control-object adapter's
//! `object_store` client and Lance's dataset client. Each would otherwise
//! read the environment on its own, and they read different names. Lance's
//! provider (`StorageOptions::with_env_gcs`) takes every variable whose
//! lowercased name `GoogleConfigKey` recognizes, unprefixed aliases such as
//! `BEARER_TOKEN`, `BASE_URL` or `TIMEOUT` included, plus
//! `GOOGLE_STORAGE_TOKEN`, and it lets `AWS_ALLOW_HTTP` and the Azure HTTP
//! allowances set its client's `allow_http`. `GoogleCloudStorageBuilder::
//! from_env` reads only `GOOGLE_*` names and a bare `SERVICE_ACCOUNT`. Left
//! alone, one stray variable sends table data to another endpoint or signs
//! it with another credential while control objects still reach Google.
//!
//! So the configuration is captured once per process, under one rule:
//! a `GOOGLE_*` name Lance recognizes is accepted, any other name it would
//! read as a Google option is refused, and the HTTP allowances Lance applies
//! are mirrored. Both clients are then built from the captured options and
//! nothing else ([`gcs_bucket_builder`], [`gcs_lance_storage_options`]); a
//! later change to any captured variable is refused until the process
//! restarts, as for Azure.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};

use crate::{Result, StorageError, StorageFailure, StorageFailureKind};

/// The static bearer token Lance's provider reads outside `GoogleConfigKey`
/// and prefers over every other credential.
///
/// `GOOGLE_BEARER_TOKEN` is captured as this option too. Lance applies a
/// `google_bearer_token` option and then, for an external-account
/// application-default credentials file, replaces it with its own
/// workload-identity provider, while the control client keeps the token: the
/// two clients would sign with different credentials. Lance's storage token
/// is applied before that provider and wins over it, so one option makes
/// both clients use the token.
const STORAGE_TOKEN: &str = "google_storage_token";

/// The variables Lance's `StorageOptions::new` copies into `allow_http`, in
/// the order it applies them (a later one wins), for every provider.
const LANCE_HTTP_ALLOWANCES: [&str; 3] = [
    "AZURE_STORAGE_ALLOW_HTTP",
    "AZURE_STORAGE_USE_HTTP",
    "AWS_ALLOW_HTTP",
];

const ALLOW_HTTP: &str = "allow_http";

/// What decides a `gs://` client: the variables read, for drift detection,
/// and the options both clients receive, keyed by Lance's spelling
/// (`GoogleConfigKey::as_ref`, or [`STORAGE_TOKEN`]).
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GcsEnvironment {
    source: BTreeMap<String, String>,
    options: BTreeMap<String, String>,
}

impl fmt::Debug for GcsEnvironment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcsEnvironment")
            .field("keys", &self.source.keys().collect::<Vec<_>>())
            .field("values", &"<redacted>")
            .finish()
    }
}

impl GcsEnvironment {
    fn verify_matches(&self, current: &Self) -> Result<()> {
        if current == self {
            return Ok(());
        }
        let mut changed = self
            .source
            .keys()
            .chain(current.source.keys())
            .filter(|key| self.source.get(*key) != current.source.get(*key))
            .cloned()
            .collect::<Vec<_>>();
        changed.sort();
        changed.dedup();
        Err(configuration_error(format!(
            "Google Cloud Storage environment changed after process capture (changed keys: {}); restart the process",
            changed.join(", ")
        )))
    }
}

fn configuration_error(message: impl Into<String>) -> StorageError {
    StorageError::backend(StorageFailureKind::Configuration, message)
}

/// The process's captured configuration, refusing an environment that has
/// drifted from it. The first call, a failed one included, fixes it.
pub(crate) fn gcs_environment() -> Result<GcsEnvironment> {
    static CAPTURED: OnceLock<std::result::Result<GcsEnvironment, StorageFailure>> =
        OnceLock::new();
    let current = capture_gcs_environment()
        .map_err(|error| StorageFailure::new(StorageFailureKind::Configuration, error.to_string()));
    let captured = CAPTURED.get_or_init(|| current.clone());
    match captured {
        Ok(captured) => {
            captured.verify_matches(&current.map_err(StorageError::Backend)?)?;
            Ok(captured.clone())
        }
        Err(failure) => Err(StorageError::Backend(failure.clone())),
    }
}

fn capture_gcs_environment() -> Result<GcsEnvironment> {
    capture_gcs_environment_values(
        env::vars_os()
            .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?))),
    )
}

pub(crate) fn capture_gcs_environment_values(
    environment: impl IntoIterator<Item = (String, String)>,
) -> Result<GcsEnvironment> {
    let environment = environment.into_iter().collect::<BTreeMap<_, _>>();
    let mut source = BTreeMap::new();
    let mut options = BTreeMap::<String, String>::new();
    let mut unsupported = Vec::new();
    let mut conflicting = Vec::new();
    for (key, value) in &environment {
        let lowercase = key.to_ascii_lowercase();
        // A bearer token is the storage token under another name: one
        // credential option, so both clients resolve it alike (below).
        let option = match GoogleConfigKey::from_str(&lowercase) {
            _ if lowercase == STORAGE_TOKEN => STORAGE_TOKEN.to_string(),
            Ok(GoogleConfigKey::BearerToken) => STORAGE_TOKEN.to_string(),
            Ok(config_key) => config_key.as_ref().to_string(),
            Err(_) => continue,
        };
        if !key.starts_with("GOOGLE_") {
            // Lance would read it; the documented spelling is GOOGLE_*.
            unsupported.push(key.clone());
            continue;
        }
        source.insert(key.clone(), value.clone());
        // Two spellings of one option: Lance keeps whichever its unordered
        // environment walk meets first, so differing values have no answer.
        match options.get(&option) {
            Some(previous) if previous != value => conflicting.push(option),
            Some(_) => {}
            None => {
                options.insert(option, value.clone());
            }
        }
    }
    if !unsupported.is_empty() {
        return Err(configuration_error(format!(
            "unsupported Google Cloud Storage environment names detected: {}; use the documented GOOGLE_* names",
            unsupported.join(", ")
        )));
    }
    if !conflicting.is_empty() {
        conflicting.sort();
        conflicting.dedup();
        return Err(configuration_error(format!(
            "conflicting Google Cloud Storage environment values for: {}; set each option once",
            conflicting.join(", ")
        )));
    }
    // Lance sets these after its options and before the GOOGLE_* names, so
    // the last one present decides `allow_http` for its client.
    for key in LANCE_HTTP_ALLOWANCES {
        if let Some(value) = environment.get(key) {
            source.insert(key.to_string(), value.clone());
            options.insert(ALLOW_HTTP.to_string(), value.clone());
        }
    }
    Ok(GcsEnvironment { source, options })
}

/// The control-object client builder for one bucket, from the captured
/// options alone. `object_store` resolves them as Lance's provider does:
/// the storage token first (a bearer token is captured as it), then a
/// service account, the application-default credentials file, then the instance
/// metadata server.
pub(crate) fn gcs_bucket_builder(
    bucket: &str,
    environment: &GcsEnvironment,
) -> GoogleCloudStorageBuilder {
    let mut builder = GoogleCloudStorageBuilder::new();
    for (key, value) in &environment.options {
        if key == STORAGE_TOKEN {
            continue;
        }
        let key = GoogleConfigKey::from_str(key).expect("captured options are GoogleConfigKey");
        builder = builder.with_config(key, value);
    }
    if let Some(token) = environment.options.get(STORAGE_TOKEN) {
        builder = builder.with_bearer_token(token);
    }
    // The root names the bucket, as Lance's URL does for its client.
    builder.with_bucket_name(bucket)
}

/// The storage options Lance's `gs://` provider receives: exactly the
/// options the control-object client is built from.
pub fn gcs_lance_storage_options() -> Result<HashMap<String, String>> {
    Ok(gcs_environment()?.options.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture(pairs: &[(&str, &str)]) -> Result<GcsEnvironment> {
        capture_gcs_environment_values(
            pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string())),
        )
    }

    #[test]
    fn google_names_are_captured_in_lances_spelling() {
        let environment = capture(&[
            ("GOOGLE_SERVICE_ACCOUNT", "/run/key.json"),
            ("GOOGLE_STORAGE_TOKEN", "token"),
            ("GOOGLE_HTTP1_ONLY", "false"),
            ("PATH", "/usr/bin"),
        ])
        .unwrap();
        assert_eq!(
            environment.options,
            BTreeMap::from([
                (
                    "google_service_account".to_string(),
                    "/run/key.json".to_string()
                ),
                (STORAGE_TOKEN.to_string(), "token".to_string()),
                ("http1_only".to_string(), "false".to_string()),
            ])
        );
        assert!(!environment.source.contains_key("PATH"));
    }

    #[test]
    fn names_only_lance_reads_are_refused_without_their_values() {
        for name in [
            "BEARER_TOKEN",
            "BASE_URL",
            "SERVICE_ACCOUNT",
            "SERVICE_ACCOUNT_KEY",
            "APPLICATION_CREDENTIALS",
            "TIMEOUT",
            "PROXY_URL",
            "HTTP1_ONLY",
            "google_service_account",
            "google_storage_token",
        ] {
            let error = capture(&[(name, "unrelated-ci-secret")]).unwrap_err();
            let message = error.to_string();
            assert_eq!(error.failure_kind(), StorageFailureKind::Configuration);
            assert!(message.contains(name), "{name}: {message}");
            assert!(!message.contains("unrelated-ci-secret"), "{message}");
        }
    }

    /// A bearer token beside an external-account credentials file: Lance
    /// would replace the token with its workload-identity provider unless
    /// the token reaches it as the storage token, which it applies first.
    #[test]
    fn a_bearer_token_is_the_one_storage_token() {
        let environment = capture(&[
            ("GOOGLE_BEARER_TOKEN", "token"),
            (
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/run/external-account.json",
            ),
        ])
        .unwrap();
        assert_eq!(environment.options[STORAGE_TOKEN], "token");
        assert!(!environment.options.contains_key("google_bearer_token"));
        assert!(
            capture(&[
                ("GOOGLE_BEARER_TOKEN", "token"),
                ("GOOGLE_STORAGE_TOKEN", "token"),
            ])
            .is_ok()
        );
        let error = capture(&[
            ("GOOGLE_BEARER_TOKEN", "one-token"),
            ("GOOGLE_STORAGE_TOKEN", "other-token"),
        ])
        .unwrap_err()
        .to_string();
        assert!(error.contains(STORAGE_TOKEN), "{error}");
        assert!(!error.contains("-token"), "{error}");
    }

    #[test]
    fn two_spellings_of_one_option_must_agree() {
        assert!(
            capture(&[
                ("GOOGLE_SERVICE_ACCOUNT", "/a.json"),
                ("GOOGLE_SERVICE_ACCOUNT_PATH", "/a.json"),
            ])
            .is_ok()
        );
        let error = capture(&[
            ("GOOGLE_SERVICE_ACCOUNT", "/a.json"),
            ("GOOGLE_SERVICE_ACCOUNT_PATH", "/b.json"),
        ])
        .unwrap_err()
        .to_string();
        assert!(error.contains("google_service_account"), "{error}");
        assert!(!error.contains("/b.json"), "{error}");
    }

    #[test]
    fn lances_http_allowances_are_mirrored_with_its_precedence() {
        let environment = capture(&[
            ("GOOGLE_ALLOW_HTTP", "false"),
            ("AZURE_STORAGE_ALLOW_HTTP", "false"),
            ("AWS_ALLOW_HTTP", "true"),
        ])
        .unwrap();
        assert_eq!(environment.options[ALLOW_HTTP], "true");
        assert!(environment.source.contains_key("AWS_ALLOW_HTTP"));
    }

    #[test]
    fn drift_names_the_changed_key_only() {
        let captured = capture(&[("GOOGLE_BASE_URL", "https://one.invalid")]).unwrap();
        let current = capture(&[("GOOGLE_BASE_URL", "https://two.invalid")]).unwrap();
        let error = captured.verify_matches(&current).unwrap_err().to_string();
        assert!(error.contains("GOOGLE_BASE_URL"), "{error}");
        assert!(!error.contains("invalid"), "{error}");
        captured.verify_matches(&captured.clone()).unwrap();
    }

    /// Both clients see one configuration: every captured option reaches
    /// the control-object builder, under the key Lance reads it by.
    #[test]
    fn the_control_builder_takes_every_captured_option() {
        let environment = capture(&[
            ("GOOGLE_BASE_URL", "http://127.0.0.1:9"),
            ("GOOGLE_SERVICE_ACCOUNT", "/run/key.json"),
            ("AWS_ALLOW_HTTP", "true"),
        ])
        .unwrap();
        let builder = gcs_bucket_builder("bucket", &environment);
        assert_eq!(
            builder
                .get_config_value(&GoogleConfigKey::BaseUrl)
                .as_deref(),
            Some("http://127.0.0.1:9")
        );
        assert_eq!(
            builder
                .get_config_value(&GoogleConfigKey::ServiceAccount)
                .as_deref(),
            Some("/run/key.json")
        );
        assert_eq!(
            builder
                .get_config_value(&GoogleConfigKey::Client(
                    object_store::ClientConfigKey::AllowHttp
                ))
                .as_deref(),
            Some("true")
        );
        assert_eq!(
            builder
                .get_config_value(&GoogleConfigKey::Bucket)
                .as_deref(),
            Some("bucket")
        );
    }
}
