//! Bearer token sources.
//!
//! The server holds its static bearer tokens as a trust snapshot
//! ([`crate::trust`]): one [`StaticTokens`] set, hashed when it is read and
//! installed whole. The source is selected once, at startup: Secrets Manager
//! whenever `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` is set, otherwise the
//! token file (`OMNIGRAPH_SERVER_BEARER_TOKENS_FILE`), otherwise the process
//! environment. Only the file can be polled
//! (`OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS`); a single
//! `OMNIGRAPH_SERVER_BEARER_TOKEN` beside it is the `default` actor, fixed for
//! the process, and the file's entries refresh around it.
//!
//! A [`TokenSource`] loads `(actor_id, token)` pairs once. The server reads
//! Secrets Manager through it; plaintext it returns is hashed at once and
//! never kept.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use color_eyre::eyre::{Result, bail, eyre};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use zeroize::{Zeroize as _, Zeroizing};

use crate::trust::{Digest, Freshness, LoadFailure, TrustHolder, TrustSet, TrustSource};
use crate::{BearerTokenHash, hash_bearer_token, server_bearer_tokens_from_env};

/// Environment variable that, when set, selects AWS Secrets Manager as the
/// token source. Its value is the secret ID or ARN. Only honored when the
/// binary is compiled with `--features aws`.
pub const AWS_SECRET_ENV: &str = "OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET";
/// One token, for the actor `default`.
pub(crate) const TOKEN_ENV: &str = "OMNIGRAPH_SERVER_BEARER_TOKEN";
/// An actor-to-token JSON object.
pub(crate) const TOKENS_JSON_ENV: &str = "OMNIGRAPH_SERVER_BEARER_TOKENS_JSON";
/// A path to a file holding that object.
pub(crate) const TOKENS_FILE_ENV: &str = "OMNIGRAPH_SERVER_BEARER_TOKENS_FILE";
/// Seconds between reads of the token file; unset or 0 reads it only at
/// startup.
pub(crate) const REFRESH_SECONDS_ENV: &str = "OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS";

/// The interval range a token file may be polled at.
pub(crate) const MIN_FILE_REFRESH: Duration = Duration::from_secs(1);
pub(crate) const MAX_FILE_REFRESH: Duration = Duration::from_secs(3600);

/// A source of bearer tokens, returned as `(actor_id, token)` pairs in
/// plaintext. The caller is expected to hash tokens before storing them.
#[async_trait]
pub trait TokenSource: Send + Sync {
    /// Fetch the current set of actor → token pairs, once, at startup.
    async fn load(&self) -> Result<Vec<(String, String)>>;

    /// Human-readable name for logs and error messages.
    fn name(&self) -> &'static str;
}

/// Reads bearer tokens from environment variables and / or files, matching
/// the long-standing server configuration:
///
/// - `OMNIGRAPH_SERVER_BEARER_TOKEN` — a single token assigned to the
///   implicit actor `default`.
/// - `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` — a JSON object of
///   `{"actor_id": "token", …}`.
/// - `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` — a path to a JSON file of the
///   same shape.
///
/// One read, through the same file reader and validator the server's own
/// holder uses; the server polls the file through [`FileSource`] instead.
#[derive(Debug, Default, Clone)]
pub struct EnvOrFileTokenSource;

#[async_trait]
impl TokenSource for EnvOrFileTokenSource {
    async fn load(&self) -> Result<Vec<(String, String)>> {
        server_bearer_tokens_from_env()
    }

    fn name(&self) -> &'static str {
        "env-or-file"
    }
}

/// Actor → token pairs whose tokens are scrubbed when dropped, refused or not.
pub(crate) struct PlaintextTokens(pub(crate) Vec<(String, String)>);

impl Drop for PlaintextTokens {
    fn drop(&mut self) {
        for (_, token) in &mut self.0 {
            token.zeroize();
        }
    }
}

/// Trim `value` in place, so no untrimmed copy of a secret is left to drop.
pub(crate) fn trim_in_place(value: &mut String) {
    let end = value.trim_end().len();
    value.truncate(end);
    let start = value.len() - value.trim_start().len();
    value.drain(..start);
}

/// A non-blank environment variable, trimmed in place.
fn env_value(name: &str) -> Option<Zeroizing<String>> {
    let mut value = Zeroizing::new(std::env::var(name).ok()?);
    trim_in_place(&mut value);
    (!value.is_empty()).then_some(value)
}

/// The static bearer tokens: actor ids and token hashes, no plaintext.
pub(crate) struct StaticTokens {
    entries: Box<[(BearerTokenHash, Arc<str>)]>,
}

impl StaticTokens {
    /// Hash `pairs` as given, scrubbing their plaintext. Embedded
    /// constructors pass their own; the Secrets Manager loader validates
    /// first.
    pub(crate) fn from_pairs(pairs: Vec<(String, String)>) -> Self {
        let pairs = PlaintextTokens(pairs);
        let entries = pairs
            .0
            .iter()
            .map(|(actor, token)| (hash_bearer_token(token), Arc::<str>::from(actor.as_str())))
            .collect::<Vec<_>>();
        Self {
            entries: entries.into(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// What every decode of the static source adds to the bytes it reads: the
/// `OMNIGRAPH_SERVER_BEARER_TOKEN` entry, hashed at startup.
#[derive(Default)]
pub(crate) struct StaticRules {
    fixed: Vec<(BearerTokenHash, Arc<str>)>,
}

impl StaticRules {
    pub(crate) fn from_env() -> Self {
        Self {
            fixed: env_value(TOKEN_ENV)
                .map(|token| (hash_bearer_token(&token), Arc::<str>::from("default")))
                .into_iter()
                .collect(),
        }
    }
}

impl TrustSet for StaticTokens {
    type Admitted = Arc<str>;
    type Rules = StaticRules;
    const LABEL: &'static str = "static principal set";

    /// An actor-to-token JSON object, checked by the validator every static
    /// source shares, then joined to the fixed `default` entry.
    fn decode(raw: &[u8], rules: &StaticRules) -> Result<Self> {
        let mut pairs = PlaintextTokens(crate::settings::parse_bearer_tokens_slice(raw)?);
        crate::settings::check_bearer_tokens(&mut pairs.0)?;
        let mut entries = rules.fixed.clone();
        for (actor, token) in &pairs.0 {
            let hash = hash_bearer_token(token);
            for (fixed_hash, fixed_actor) in &rules.fixed {
                if fixed_actor.as_ref() == actor {
                    bail!("duplicate bearer token actor '{actor}'");
                }
                if fixed_hash == &hash {
                    bail!("duplicate bearer token value configured");
                }
            }
            entries.push((hash, Arc::from(actor.as_str())));
        }
        Ok(Self {
            entries: entries.into(),
        })
    }

    /// SHA-256 over the `(actor, token hash)` pairs in actor order, each
    /// actor length-prefixed.
    fn digest(&self) -> Digest {
        let mut sorted = self
            .entries
            .iter()
            .map(|(hash, actor)| (actor.as_ref(), hash))
            .collect::<Vec<_>>();
        sorted.sort();
        let mut hasher = Sha256::new();
        for (actor, hash) in sorted {
            hasher.update((actor.len() as u64).to_be_bytes());
            hasher.update(actor.as_bytes());
            hasher.update(hash);
        }
        hasher.finalize().into()
    }

    fn actors(&self) -> BTreeSet<&str> {
        self.entries
            .iter()
            .map(|(_, actor)| actor.as_ref())
            .collect()
    }

    /// Hash the token and compare it with every stored digest in constant
    /// time. Every entry is visited, so total work, and therefore response
    /// timing, does not depend on which slot matches.
    fn admit(&self, token: &str, _now: SystemTime) -> Option<Arc<str>> {
        let provided = hash_bearer_token(token);
        let mut matched: Option<Arc<str>> = None;
        for (hash, actor) in self.entries.iter() {
            if bool::from(hash.ct_eq(&provided)) && matched.is_none() {
                matched = Some(Arc::clone(actor));
            }
        }
        matched
    }
}

/// The token file, opened again at every read so a symlink swap is seen.
pub(crate) struct FileSource {
    path: String,
    freshness: Freshness,
    /// Whether the read-by-others warning was logged for the current mode.
    readable_warned: AtomicBool,
}

impl FileSource {
    /// A file is read at startup, or polled every 1 to 3600 seconds.
    pub(crate) fn new(path: String, freshness: Freshness) -> Result<Self> {
        if let Freshness::Polled { interval } = freshness {
            if !(MIN_FILE_REFRESH..=MAX_FILE_REFRESH).contains(&interval) {
                bail!(
                    "{REFRESH_SECONDS_ENV} must be from {} to {} seconds for the token file \
                     ({TOKENS_FILE_ENV}), or 0 to read it only at startup; got {:?}",
                    MIN_FILE_REFRESH.as_secs(),
                    MAX_FILE_REFRESH.as_secs(),
                    interval
                );
            }
        }
        Ok(Self {
            path,
            freshness,
            readable_warned: AtomicBool::new(false),
        })
    }
}

impl TrustSource for FileSource {
    fn name(&self) -> &'static str {
        "file"
    }

    fn describe(&self) -> String {
        format!("bearer tokens file at {}", self.path)
    }

    fn freshness(&self) -> Freshness {
        self.freshness
    }

    fn load(&self) -> std::result::Result<Zeroizing<Vec<u8>>, LoadFailure> {
        let read = crate::settings::read_bearer_tokens_bytes(&self.path)?;
        match read.readable_by_others {
            // Once per mode, not once per tick.
            Some(mode) if !self.readable_warned.swap(true, Ordering::Relaxed) => {
                crate::settings::warn_bearer_tokens_file_readable(mode);
            }
            Some(_) => {}
            None => self.readable_warned.store(false, Ordering::Relaxed),
        }
        Ok(read.bytes)
    }
}

/// `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`: process environment, which cannot
/// change in a running process. An unset variable holds no entries.
pub(crate) struct EnvSource;

impl EnvSource {
    pub(crate) fn new(freshness: Freshness) -> Result<Self> {
        if freshness != Freshness::BootOnly {
            bail!(
                "{REFRESH_SECONDS_ENV} is set, but the selected static token source is the \
                 process environment ({TOKEN_ENV} or {TOKENS_JSON_ENV}), which cannot change \
                 in a running process. Unset {REFRESH_SECONDS_ENV}, or set {TOKENS_FILE_ENV} \
                 to refresh the tokens from a file"
            );
        }
        Ok(Self)
    }
}

impl TrustSource for EnvSource {
    fn name(&self) -> &'static str {
        "environment"
    }

    fn describe(&self) -> String {
        TOKENS_JSON_ENV.to_string()
    }

    fn freshness(&self) -> Freshness {
        Freshness::BootOnly
    }

    fn load(&self) -> std::result::Result<Zeroizing<Vec<u8>>, LoadFailure> {
        Ok(match env_value(TOKENS_JSON_ENV) {
            Some(json) => Zeroizing::new(json.as_bytes().to_vec()),
            None => Zeroizing::new(b"{}".to_vec()),
        })
    }
}

/// The static tokens this process holds.
pub(crate) struct StaticTokenTrust {
    pub(crate) holder: Arc<TrustHolder<StaticTokens>>,
    /// Whether any static source variable is set, whatever it holds. The
    /// authentication mode is classified from this, never from the set.
    pub(crate) configured: bool,
}

/// Whether any static source variable is set.
pub(crate) fn static_source_configured() -> bool {
    [TOKEN_ENV, TOKENS_JSON_ENV, TOKENS_FILE_ENV, AWS_SECRET_ENV]
        .into_iter()
        .any(|name| env_value(name).is_some())
}

/// `OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS`: unset or 0 is boot-only.
fn refresh_from_env() -> Result<Freshness> {
    let Some(value) = env_value(REFRESH_SECONDS_ENV) else {
        return Ok(Freshness::BootOnly);
    };
    let seconds = value.parse::<u64>().map_err(|_| {
        eyre!(
            "{REFRESH_SECONDS_ENV} must be a whole number of seconds, got '{}'",
            value.as_str()
        )
    })?;
    Ok(match seconds {
        0 => Freshness::BootOnly,
        seconds => Freshness::Polled {
            interval: Duration::from_secs(seconds),
        },
    })
}

/// Select the static source, refuse a refresh interval it cannot honor, and
/// read it once. A refusal here refuses startup.
pub(crate) async fn static_token_trust() -> Result<StaticTokenTrust> {
    let freshness = refresh_from_env()?;
    let configured = static_source_configured();
    let holder = if env_value(AWS_SECRET_ENV).is_some() {
        if freshness != Freshness::BootOnly {
            bail!(
                "{REFRESH_SECONDS_ENV} is set, but the selected static token source is AWS \
                 Secrets Manager ({AWS_SECRET_ENV}), which this build reads only at startup. \
                 Unset {REFRESH_SECONDS_ENV}, or serve the tokens from {TOKENS_FILE_ENV} with \
                 {AWS_SECRET_ENV} unset"
            );
        }
        let source = resolve_token_source().await?;
        let set = StaticTokens::from_pairs(source.load().await?);
        TrustHolder::with_boot_set(source.name(), set, StaticRules::default())
    } else {
        let rules = StaticRules::from_env();
        let source: Box<dyn TrustSource> = match env_value(TOKENS_FILE_ENV) {
            Some(path) => Box::new(FileSource::new(path.to_string(), freshness)?),
            None => Box::new(EnvSource::new(freshness)?),
        };
        TrustHolder::open(source, rules)?
    };
    Ok(StaticTokenTrust { holder, configured })
}

/// Pick the token source based on configuration.
///
/// Preference order:
/// 1. If `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` is set AND the binary was
///    built with `--features aws`, returns an AWS Secrets Manager source.
/// 2. If that env var is set but the binary was built without the feature,
///    errors with a clear rebuild instruction rather than silently falling
///    back to the env/file source (which would hide the misconfiguration).
/// 3. Otherwise, returns `EnvOrFileTokenSource`.
pub async fn resolve_token_source() -> Result<Box<dyn TokenSource>> {
    if let Ok(secret_id) = std::env::var(AWS_SECRET_ENV) {
        let secret_id = secret_id.trim().to_string();
        if !secret_id.is_empty() {
            #[cfg(feature = "aws")]
            {
                let source = aws::SecretsManagerTokenSource::new(secret_id).await?;
                return Ok(Box::new(source));
            }
            #[cfg(not(feature = "aws"))]
            {
                bail!(
                    "{} is set but this binary was not built with --features aws. \
                     Rebuild: cargo build --release --features aws",
                    AWS_SECRET_ENV
                );
            }
        }
    }
    Ok(Box::new(EnvOrFileTokenSource))
}

/// Parse a JSON secret payload (from AWS Secrets Manager or any equivalent
/// source) into actor → token pairs.
///
/// Payload shape: `{"actor_id_1": "token_1", "actor_id_2": "token_2", ...}`.
/// Extracted as a free function so it can be unit-tested without the AWS SDK.
#[cfg(any(test, feature = "aws"))]
pub(crate) fn parse_json_secret_payload(payload: &str) -> Result<Vec<(String, String)>> {
    use std::collections::HashMap;

    let map: HashMap<String, String> = serde_json::from_str(payload).map_err(|err| {
        color_eyre::eyre::eyre!(
            "bearer-token secret payload is not a JSON object of actor→token: {}",
            err
        )
    })?;

    let mut pairs: Vec<(String, String)> = Vec::with_capacity(map.len());
    for (actor, token) in map {
        let actor = actor.trim().to_string();
        let token = token.trim().to_string();
        if actor.is_empty() {
            bail!("bearer-token secret contains a blank actor id");
        }
        if token.is_empty() {
            bail!(
                "bearer-token secret has a blank token for actor '{}'",
                actor
            );
        }
        pairs.push((actor, token));
    }
    // The same checks as the environment and file sources: reserved actor
    // names and duplicate token values are refused.
    crate::settings::validate_bearer_tokens(pairs)
}

#[cfg(feature = "aws")]
pub mod aws {
    //! AWS Secrets Manager bearer-token backend.
    //!
    //! Fetches a JSON payload from a named secret on startup. Credentials are
    //! resolved via the AWS default chain — env vars, shared config, IMDSv2
    //! instance role, or ECS task role — so no explicit credential plumbing
    //! is needed when running under an IAM role.
    //!
    //! The secret is read once; a refresh interval refuses startup.
    use super::TokenSource;
    use async_trait::async_trait;
    use color_eyre::eyre::{Result, WrapErr, eyre};

    /// Loads bearer tokens from a named AWS Secrets Manager secret.
    pub struct SecretsManagerTokenSource {
        client: aws_sdk_secretsmanager::Client,
        secret_id: String,
    }

    impl SecretsManagerTokenSource {
        /// Construct a new source. Resolves AWS credentials + region via the
        /// default chain — no explicit configuration needed on EC2/ECS/EKS.
        pub async fn new(secret_id: impl Into<String>) -> Result<Self> {
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = aws_sdk_secretsmanager::Client::new(&config);
            Ok(Self {
                client,
                secret_id: secret_id.into(),
            })
        }
    }

    #[async_trait]
    impl TokenSource for SecretsManagerTokenSource {
        async fn load(&self) -> Result<Vec<(String, String)>> {
            let output = self
                .client
                .get_secret_value()
                .secret_id(&self.secret_id)
                .send()
                .await
                .wrap_err_with(|| {
                    format!("fetch AWS Secrets Manager secret '{}'", self.secret_id)
                })?;

            let payload = output.secret_string().ok_or_else(|| {
                eyre!(
                    "secret '{}' has no SecretString — binary secrets are not supported",
                    self.secret_id
                )
            })?;

            super::parse_json_secret_payload(payload)
        }

        fn name(&self) -> &'static str {
            "aws-secrets-manager"
        }
    }
}

#[cfg(feature = "aws")]
pub use aws::SecretsManagerTokenSource;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trust::{Refresh, Warned};
    use serial_test::serial;
    use std::env;
    use std::fs;
    use std::path::Path;

    fn clear_env() {
        unsafe {
            env::remove_var("OMNIGRAPH_SERVER_BEARER_TOKEN");
            env::remove_var("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON");
            env::remove_var("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE");
            env::remove_var(REFRESH_SECONDS_ENV);
        }
    }

    #[tokio::test]
    #[serial]
    async fn env_or_file_source_returns_empty_when_nothing_configured() {
        clear_env();
        let source = EnvOrFileTokenSource;
        let tokens = source.load().await.unwrap();
        assert!(tokens.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn env_or_file_source_reads_single_token_as_default_actor() {
        clear_env();
        unsafe {
            env::set_var("OMNIGRAPH_SERVER_BEARER_TOKEN", "some-token");
        }
        let source = EnvOrFileTokenSource;
        let tokens = source.load().await.unwrap();
        unsafe {
            env::remove_var("OMNIGRAPH_SERVER_BEARER_TOKEN");
        }
        assert_eq!(
            tokens,
            vec![("default".to_string(), "some-token".to_string())]
        );
    }

    const POLLED: Freshness = Freshness::Polled {
        interval: Duration::from_secs(1),
    };

    fn write_private(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    fn file_holder(path: &Path) -> Arc<TrustHolder<StaticTokens>> {
        let source = FileSource::new(path.to_string_lossy().into_owned(), POLLED).unwrap();
        TrustHolder::open(Box::new(source), StaticRules::default()).unwrap()
    }

    fn admitted(holder: &TrustHolder<StaticTokens>, token: &str) -> Option<String> {
        holder
            .load()
            .set
            .admit(token, SystemTime::now())
            .map(|actor| actor.to_string())
    }

    fn generation(holder: &TrustHolder<StaticTokens>) -> u64 {
        holder.load().generation
    }

    /// Every refusal at a poll keeps the installed set, and records what it
    /// warned about: refused content by its digest, a failure that yields no
    /// content by its class.
    #[test]
    fn token_file_refusals_keep_the_installed_set_issue_434() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("tokens.json");
        write_private(&path, br#"{"alice":"tok-a"}"#);
        let holder = file_holder(&path);
        assert_eq!(generation(&holder), 1);
        assert_eq!(admitted(&holder, "tok-a").as_deref(), Some("alice"));

        let keeps = |label: &str, expected: fn(Option<Warned>) -> bool| {
            assert_eq!(
                holder.refresh(),
                Refresh::Refused { logged: true },
                "{label}"
            );
            assert!(expected(holder.warned()), "{label}: {:?}", holder.warned());
            assert_eq!(generation(&holder), 1, "{label}");
            assert_eq!(
                admitted(&holder, "tok-a").as_deref(),
                Some("alice"),
                "{label}"
            );
        };
        let refused: [(&str, &[u8]); 6] = [
            ("invalid json", b"{\"alice\": "),
            ("empty bytes", b""),
            ("not an object", b"[\"alice\"]"),
            ("duplicate token", br#"{"alice":"same","bob":"same"}"#),
            ("blank token", br#"{"alice":"tok-a","bob":"  "}"#),
            ("reserved actor", br#"{"principal:bob":"tok-b"}"#),
        ];
        for (label, bytes) in refused {
            write_private(&path, bytes);
            keeps(label, |warned| matches!(warned, Some(Warned::Content(_))));
        }

        let oversized = format!(
            r#"{{"alice":"tok-a","bob":"{}"}}"#,
            "x".repeat(crate::trust::MAX_SOURCE_BYTES)
        );
        write_private(&path, oversized.as_bytes());
        keeps("oversized", |warned| {
            warned == Some(Warned::NoContent("too_large"))
        });

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            write_private(&path, br#"{"bob":"tok-b"}"#);
            fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
            keeps("writable by others", |warned| {
                warned == Some(Warned::NoContent("writable_by_others"))
            });
        }

        fs::remove_file(&path).unwrap();
        keeps("missing", |warned| {
            warned == Some(Warned::NoContent("missing"))
        });

        fs::create_dir(&path).unwrap();
        keeps("directory", |warned| {
            warned == Some(Warned::NoContent("not_regular_file"))
        });
        fs::remove_dir(&path).unwrap();

        // A FIFO with no writer opens at once and is refused, never waited
        // on; its class was just warned about, so it is not logged again.
        #[cfg(unix)]
        {
            let fifo = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
            assert_eq!(holder.refresh(), Refresh::Refused { logged: false });
            assert_eq!(holder.warned(), Some(Warned::NoContent("not_regular_file")));
            assert_eq!(admitted(&holder, "tok-a").as_deref(), Some("alice"));
        }
    }

    /// Bytes equal to the installed snapshot's install nothing; a rewrite that
    /// moves no token records its bytes and bumps no generation.
    #[test]
    fn unchanged_and_cosmetic_rewrites_install_nothing_issue_434() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("tokens.json");
        write_private(&path, br#"{"alice":"tok-a","bob":"tok-b"}"#);
        let holder = file_holder(&path);
        let digest = holder.load().digest;
        assert_eq!(holder.refresh(), Refresh::Unchanged);

        write_private(
            &path,
            b"{\n  \"bob\" : \"tok-b\",\n  \"alice\": \" tok-a \"\n}\n",
        );
        assert_eq!(holder.refresh(), Refresh::Rewritten);
        assert_eq!(generation(&holder), 1);
        assert_eq!(holder.load().digest, digest);
        // The new bytes are now the installed ones.
        assert_eq!(holder.refresh(), Refresh::Unchanged);
        assert_eq!(admitted(&holder, "tok-a").as_deref(), Some("alice"));
    }

    /// A token rotated to a value of the same length, copied with the old
    /// mtime, leaves size and mtime alike; the bytes still differ.
    #[test]
    fn same_size_and_mtime_rotation_is_installed_issue_434() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("tokens.json");
        write_private(&path, br#"{"alice":"tok-a"}"#);
        let holder = file_holder(&path);
        let before = fs::metadata(&path).unwrap();

        write_private(&path, br#"{"alice":"tok-z"}"#);
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(before.modified().unwrap())
            .unwrap();
        let after = fs::metadata(&path).unwrap();
        assert_eq!(after.len(), before.len());
        assert_eq!(after.modified().unwrap(), before.modified().unwrap());

        assert_eq!(holder.refresh(), Refresh::Installed(2));
        assert_eq!(admitted(&holder, "tok-z").as_deref(), Some("alice"));
        assert_eq!(admitted(&holder, "tok-a"), None);
    }

    /// A Kubernetes projected volume replaces its content by swapping the
    /// `..data` symlink; the path the server reads is a symlink through it.
    #[cfg(unix)]
    #[test]
    fn data_symlink_swap_is_installed_issue_434() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let mount = temp.path();
        fs::create_dir(mount.join("..2026_11_17_a")).unwrap();
        write_private(
            &mount.join("..2026_11_17_a/tokens.json"),
            br#"{"alice":"tok-a"}"#,
        );
        symlink("..2026_11_17_a", mount.join("..data")).unwrap();
        symlink("..data/tokens.json", mount.join("tokens.json")).unwrap();
        let holder = file_holder(&mount.join("tokens.json"));
        assert_eq!(admitted(&holder, "tok-a").as_deref(), Some("alice"));

        fs::create_dir(mount.join("..2026_11_17_b")).unwrap();
        write_private(
            &mount.join("..2026_11_17_b/tokens.json"),
            br#"{"bob":"tok-b"}"#,
        );
        symlink("..2026_11_17_b", mount.join("..data_tmp")).unwrap();
        fs::rename(mount.join("..data_tmp"), mount.join("..data")).unwrap();

        assert_eq!(holder.refresh(), Refresh::Installed(2));
        assert_eq!(admitted(&holder, "tok-b").as_deref(), Some("bob"));
        assert_eq!(admitted(&holder, "tok-a"), None);
    }

    /// A valid object with no entries is a valid state: installed, as a change
    /// that removed every actor.
    #[test]
    fn valid_empty_object_is_installed_issue_434() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("tokens.json");
        write_private(&path, br#"{"alice":"tok-a"}"#);
        let holder = file_holder(&path);

        write_private(&path, b"{}");
        assert_eq!(holder.refresh(), Refresh::Installed(2));
        assert!(holder.load().set.is_empty());
        assert_eq!(admitted(&holder, "tok-a"), None);
        assert_eq!(holder.warned(), None);

        write_private(&path, br#"{"bob":"tok-b"}"#);
        assert_eq!(holder.refresh(), Refresh::Installed(3));
        assert_eq!(admitted(&holder, "tok-b").as_deref(), Some("bob"));
    }

    /// A failure with no content warns once per class; a successful read,
    /// even of the installed bytes, ends it. Refused content warns once per
    /// distinct content, and is validated again at every poll.
    #[test]
    fn refusals_warn_once_until_a_successful_read_issue_434() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("tokens.json");
        let good = br#"{"alice":"tok-a"}"#;
        write_private(&path, good);
        let holder = file_holder(&path);

        fs::remove_file(&path).unwrap();
        assert_eq!(holder.refresh(), Refresh::Refused { logged: true });
        assert_eq!(holder.refresh(), Refresh::Refused { logged: false });
        assert_eq!(holder.warned(), Some(Warned::NoContent("missing")));

        // Another class is another warning.
        fs::create_dir(&path).unwrap();
        assert_eq!(holder.refresh(), Refresh::Refused { logged: true });
        assert_eq!(holder.refresh(), Refresh::Refused { logged: false });
        fs::remove_dir(&path).unwrap();

        // The installed bytes come back: a successful read clears the warning.
        write_private(&path, good);
        assert_eq!(holder.refresh(), Refresh::Unchanged);
        assert_eq!(holder.warned(), None);
        fs::remove_file(&path).unwrap();
        assert_eq!(holder.refresh(), Refresh::Refused { logged: true });

        write_private(&path, b"{");
        assert_eq!(holder.refresh(), Refresh::Refused { logged: true });
        assert_eq!(holder.refresh(), Refresh::Refused { logged: false });
        write_private(&path, b"[");
        assert_eq!(holder.refresh(), Refresh::Refused { logged: true });
        assert_eq!(generation(&holder), 1);
        assert_eq!(admitted(&holder, "tok-a").as_deref(), Some("alice"));
    }

    /// A source declares its freshness when it is constructed: process
    /// environment cannot change, and a file is polled every 1 to 3600 s.
    #[test]
    fn refresh_intervals_are_a_property_of_the_source_issue_434() {
        assert!(EnvSource::new(Freshness::BootOnly).is_ok());
        let error = EnvSource::new(POLLED).err().unwrap();
        assert!(error.to_string().contains("process environment"), "{error}");

        let path = "/run/omnigraph/tokens.json".to_string();
        for seconds in [1, 5, 3600] {
            let interval = Duration::from_secs(seconds);
            assert!(FileSource::new(path.clone(), Freshness::Polled { interval }).is_ok());
        }
        assert!(FileSource::new(path.clone(), Freshness::BootOnly).is_ok());
        for interval in [
            Duration::ZERO,
            Duration::from_millis(999),
            Duration::from_secs(3601),
        ] {
            let error = FileSource::new(path.clone(), Freshness::Polled { interval })
                .err()
                .unwrap();
            assert!(
                error.to_string().contains("from 1 to 3600 seconds"),
                "{error}"
            );
        }
    }

    /// An interval refuses startup unless the selected source is the file;
    /// the refusal names the source the server selected. With `_TOKEN` beside
    /// `_FILE`, the `default` actor is fixed and the file refreshes around it.
    #[tokio::test]
    #[serial]
    async fn refresh_interval_refuses_a_source_that_cannot_refresh_issue_434() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("tokens.json");
        write_private(&path, br#"{"alice":"tok-a"}"#);
        let set = |vars: &[(&str, &str)]| {
            clear_env();
            unsafe {
                env::remove_var(AWS_SECRET_ENV);
                for (name, value) in vars {
                    env::set_var(name, value);
                }
            }
        };
        let refused = |message: &'static str| async move {
            let error = static_token_trust().await.err().unwrap().to_string();
            assert!(error.contains(message), "{error}");
        };

        set(&[(REFRESH_SECONDS_ENV, "5"), (AWS_SECRET_ENV, "tokens")]);
        refused("selected static token source is AWS Secrets Manager").await;
        set(&[
            (REFRESH_SECONDS_ENV, "5"),
            (AWS_SECRET_ENV, "tokens"),
            (TOKENS_FILE_ENV, path.to_str().unwrap()),
        ]);
        refused("selected static token source is AWS Secrets Manager").await;
        set(&[(REFRESH_SECONDS_ENV, "5"), (TOKEN_ENV, "tok-d")]);
        refused("selected static token source is the process environment").await;
        set(&[(REFRESH_SECONDS_ENV, "5"), (TOKENS_JSON_ENV, "{}")]);
        refused("selected static token source is the process environment").await;
        set(&[
            (REFRESH_SECONDS_ENV, "soon"),
            (TOKENS_FILE_ENV, path.to_str().unwrap()),
        ]);
        refused("whole number of seconds").await;
        set(&[
            (REFRESH_SECONDS_ENV, "3601"),
            (TOKENS_FILE_ENV, path.to_str().unwrap()),
        ]);
        refused("from 1 to 3600 seconds").await;

        set(&[(REFRESH_SECONDS_ENV, "0"), (TOKEN_ENV, "tok-d")]);
        let trust = static_token_trust().await.unwrap();
        assert_eq!(trust.holder.freshness(), Freshness::BootOnly);
        assert_eq!(trust.holder.source_name(), "environment");

        set(&[
            (REFRESH_SECONDS_ENV, "5"),
            (TOKEN_ENV, " tok-d "),
            (TOKENS_FILE_ENV, path.to_str().unwrap()),
        ]);
        let trust = static_token_trust().await.unwrap();
        clear_env();
        let holder = trust.holder;
        assert!(trust.configured);
        assert_eq!(holder.source_name(), "file");
        assert_eq!(
            holder.freshness(),
            Freshness::Polled {
                interval: Duration::from_secs(5)
            }
        );
        assert_eq!(admitted(&holder, "tok-d").as_deref(), Some("default"));
        write_private(&path, br#"{"bob":"tok-b"}"#);
        assert_eq!(holder.refresh(), Refresh::Installed(2));
        assert_eq!(admitted(&holder, "tok-d").as_deref(), Some("default"));
        assert_eq!(admitted(&holder, "tok-b").as_deref(), Some("bob"));
        assert_eq!(admitted(&holder, "tok-a"), None);
        // The fixed entry is validated with the file at every read.
        let refused: [&[u8]; 2] = [br#"{"default":"tok-x"}"#, br#"{"carol":"tok-d"}"#];
        for refused in refused {
            write_private(&path, refused);
            assert_eq!(holder.refresh(), Refresh::Refused { logged: true });
            assert_eq!(admitted(&holder, "tok-b").as_deref(), Some("bob"));
        }
    }

    #[test]
    fn parse_json_secret_payload_reads_actor_token_map() {
        let pairs = parse_json_secret_payload(r#"{"alice": "tok-a", "bob": "tok-b"}"#).unwrap();
        assert_eq!(
            pairs,
            vec![
                ("alice".to_string(), "tok-a".to_string()),
                ("bob".to_string(), "tok-b".to_string()),
            ]
        );
    }

    #[test]
    fn parse_json_secret_payload_trims_whitespace() {
        let pairs = parse_json_secret_payload(r#"{"  alice  ": "  tok-a  "}"#).unwrap();
        assert_eq!(pairs, vec![("alice".to_string(), "tok-a".to_string())]);
    }

    #[test]
    fn parse_json_secret_payload_rejects_blank_actor() {
        let err = parse_json_secret_payload(r#"{"   ": "tok"}"#).unwrap_err();
        assert!(err.to_string().contains("blank actor"));
    }

    #[test]
    fn parse_json_secret_payload_rejects_blank_token() {
        let err = parse_json_secret_payload(r#"{"alice": "  "}"#).unwrap_err();
        assert!(err.to_string().contains("blank token"));
    }

    #[test]
    fn parse_json_secret_payload_rejects_non_object() {
        let err = parse_json_secret_payload("[1, 2, 3]").unwrap_err();
        assert!(err.to_string().contains("not a JSON object"));
    }

    /// The secret source applies the same checks as the environment and file
    /// sources: no actor in the signed-identity namespace, no shared token.
    #[test]
    fn parse_json_secret_payload_refuses_reserved_actors_and_duplicate_tokens() {
        let err = parse_json_secret_payload(r#"{"principal:alice": "tok-a"}"#).unwrap_err();
        assert!(err.to_string().contains("reserved prefix"), "{err}");
        let err = parse_json_secret_payload(r#"{"alice": "same", "bob": "same"}"#).unwrap_err();
        assert!(err.to_string().contains("duplicate bearer token"), "{err}");
    }

    #[tokio::test]
    #[serial]
    async fn resolve_token_source_falls_back_to_env_or_file_when_aws_var_unset() {
        clear_env();
        unsafe {
            env::remove_var(AWS_SECRET_ENV);
        }
        let source = resolve_token_source().await.unwrap();
        assert_eq!(source.name(), "env-or-file");
    }

    #[cfg(not(feature = "aws"))]
    #[tokio::test]
    #[serial]
    async fn resolve_token_source_errors_when_aws_var_set_without_feature() {
        clear_env();
        unsafe {
            env::set_var(AWS_SECRET_ENV, "some-secret-id");
        }
        let result = resolve_token_source().await;
        unsafe {
            env::remove_var(AWS_SECRET_ENV);
        }
        let err = match result {
            Ok(_) => panic!("expected resolve_token_source to error without aws feature"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("--features aws"));
    }
}
