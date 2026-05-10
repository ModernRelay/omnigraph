//! `omnigraph update` — self-update from GitHub Releases.
//!
//! See MR-612 and `scripts/install.sh` for the reference distribution flow.
//! Replaces both `omnigraph` and `omnigraph-server` in-place when they live in
//! the same directory as the running binary. Out of scope: Windows, Linux arm64,
//! and automatic (non-user-invoked) updates.

use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, ValueEnum};
use color_eyre::eyre::{Context, Result, bail, eyre};
use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const REPO_SLUG: &str = "ModernRelay/omnigraph";

const DEFAULT_API_BASE: &str = "https://api.github.com";
const DEFAULT_DOWNLOAD_BASE: &str = "https://github.com";
const USER_AGENT: &str = concat!("omnigraph-cli/", env!("CARGO_PKG_VERSION"));

const HOMEBREW_PREFIXES: &[&str] = &[
    "/opt/homebrew/",
    "/usr/local/Cellar/",
    "/usr/local/opt/",
    "/home/linuxbrew/.linuxbrew/",
];

/// Release channel to update from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Channel {
    /// Latest tagged stable release.
    Stable,
    /// Rolling `edge` release republished on every push to `main`.
    Edge,
}

impl Channel {
    fn as_str(self) -> &'static str {
        match self {
            Channel::Stable => "stable",
            Channel::Edge => "edge",
        }
    }
}

#[derive(Debug, Args)]
pub struct UpdateArgs {
    /// Release channel to update from.
    #[arg(long, value_enum, default_value_t = Channel::Stable)]
    pub channel: Channel,
    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
    /// Only check whether a newer version is available; do not install.
    #[arg(long)]
    pub check: bool,
}

/// Latest-release metadata we pull from the GitHub Releases API.
#[derive(Debug, Deserialize)]
struct ReleaseInfo {
    tag_name: String,
}

pub async fn run(args: UpdateArgs) -> Result<()> {
    let current_version = env!("CARGO_PKG_VERSION");
    let current_exe = std::env::current_exe().context("resolve path of running omnigraph binary")?;
    let install_dir = current_exe
        .parent()
        .ok_or_else(|| eyre!("running binary has no parent directory: {}", current_exe.display()))?
        .to_path_buf();

    if let Some(prefix) = detect_homebrew_install(&current_exe) {
        println!(
            "omnigraph was installed via Homebrew (prefix: {}). Run `brew upgrade ModernRelay/tap/omnigraph` instead.",
            prefix.display()
        );
        return Ok(());
    }

    let asset_name = platform_asset_name()?;
    let client = build_http_client()?;

    let release = fetch_release_metadata(&client, args.channel).await?;
    let latest_tag = release.tag_name.trim().to_string();
    if latest_tag.is_empty() {
        bail!("release metadata missing tag_name");
    }
    let latest_version = latest_tag.trim_start_matches('v');

    let needs_update = match args.channel {
        Channel::Stable => version_is_newer(current_version, latest_version),
        // The `edge` tag moves with every push to `main`; treat any user-invoked
        // `update --channel edge` as a refresh and always reinstall.
        Channel::Edge => true,
    };

    if !needs_update {
        println!("omnigraph is up to date (v{})", current_version);
        return Ok(());
    }

    println!(
        "A new version of omnigraph is available: v{} -> {} (channel: {})",
        current_version,
        latest_tag,
        args.channel.as_str(),
    );

    if args.check {
        return Ok(());
    }

    if !args.yes && io::stdin().is_terminal() {
        eprint!(
            "Update from v{} to {}? [y/N] ",
            current_version, latest_tag
        );
        io::stderr().flush().ok();
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .context("read confirmation prompt")?;
        let answer = input.trim().to_ascii_lowercase();
        if answer != "y" && answer != "yes" {
            eprintln!("aborted.");
            return Ok(());
        }
    }

    // Verify we can write into the install directory before downloading
    // anything — gives a clear error early rather than after the download.
    ensure_dir_writable(&install_dir)?;

    let workdir = tempfile::tempdir_in(&install_dir)
        .with_context(|| format!("create temp dir in {}", install_dir.display()))?;

    let asset_stem = asset_name.trim_end_matches(".tar.gz");
    let archive_path = workdir.path().join(asset_name);
    let checksum_path = workdir.path().join(format!("{asset_stem}.sha256"));

    let archive_url = release_asset_url(&latest_tag, asset_name);
    let checksum_url = release_asset_url(&latest_tag, &format!("{asset_stem}.sha256"));

    println!("downloading {asset_name}…");
    download_file(&client, &archive_url, &archive_path)
        .await
        .with_context(|| format!("download {archive_url}"))?;
    download_file(&client, &checksum_url, &checksum_path)
        .await
        .with_context(|| format!("download {checksum_url}"))?;

    verify_checksum(&archive_path, &checksum_path)?;
    extract_archive(&archive_path, workdir.path())?;

    let updated = replace_binaries(&install_dir, workdir.path())?;
    if updated.is_empty() {
        bail!("release archive did not contain any of the expected binaries");
    }

    println!();
    println!("Updated:");
    for path in &updated {
        println!("  {}", path.display());
    }
    println!();
    println!(
        "Version: v{} -> {} (install dir: {})",
        current_version,
        latest_tag,
        install_dir.display()
    );

    Ok(())
}

/// Return `Some(prefix)` if `path` resolves under a known Homebrew prefix.
pub fn detect_homebrew_install(path: &Path) -> Option<PathBuf> {
    let canonical = fs::canonicalize(path).ok().unwrap_or_else(|| path.to_path_buf());
    let canonical_str = canonical.to_string_lossy();
    for prefix in HOMEBREW_PREFIXES {
        if canonical_str.starts_with(prefix) {
            return Some(PathBuf::from(prefix));
        }
    }
    if let Some(prefix) = brew_prefix() {
        let prefix_path = PathBuf::from(&prefix);
        if canonical.starts_with(&prefix_path) {
            return Some(prefix_path);
        }
    }
    None
}

fn brew_prefix() -> Option<String> {
    let output = std::process::Command::new("brew")
        .arg("--prefix")
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let prefix = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!prefix.is_empty()).then_some(prefix)
}

pub fn platform_asset_name() -> Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok("omnigraph-linux-x86_64.tar.gz"),
        ("macos", "aarch64") => Ok("omnigraph-macos-arm64.tar.gz"),
        (os, arch) => bail!(
            "no prebuilt omnigraph release for {os}/{arch}; rebuild from source via scripts/install-source.sh"
        ),
    }
}

fn build_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()
        .context("build reqwest client")
}

fn api_base() -> String {
    std::env::var("OMNIGRAPH_UPDATE_API_BASE").unwrap_or_else(|_| DEFAULT_API_BASE.to_string())
}

fn download_base() -> String {
    std::env::var("OMNIGRAPH_UPDATE_DOWNLOAD_BASE")
        .unwrap_or_else(|_| DEFAULT_DOWNLOAD_BASE.to_string())
}

fn release_asset_url(tag: &str, asset_name: &str) -> String {
    format!(
        "{}/{}/releases/download/{}/{}",
        download_base().trim_end_matches('/'),
        REPO_SLUG,
        tag,
        asset_name,
    )
}

async fn fetch_release_metadata(client: &reqwest::Client, channel: Channel) -> Result<ReleaseInfo> {
    let url = match channel {
        Channel::Stable => format!(
            "{}/repos/{}/releases/latest",
            api_base().trim_end_matches('/'),
            REPO_SLUG,
        ),
        Channel::Edge => format!(
            "{}/repos/{}/releases/tags/edge",
            api_base().trim_end_matches('/'),
            REPO_SLUG,
        ),
    };
    fetch_release_info(client, &url).await
}

/// Hit the latest stable release endpoint without invoking the full update flow.
/// Used by the startup version-check.
pub async fn fetch_latest_stable_tag(client: &reqwest::Client) -> Result<String> {
    let url = format!(
        "{}/repos/{}/releases/latest",
        api_base().trim_end_matches('/'),
        REPO_SLUG,
    );
    Ok(fetch_release_info(client, &url).await?.tag_name)
}

async fn fetch_release_info(client: &reqwest::Client, url: &str) -> Result<ReleaseInfo> {
    let response = client
        .get(url)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let text = response.text().await.context("read response body")?;
    if !status.is_success() {
        bail!("release lookup failed ({status}): {text}");
    }
    serde_json::from_str::<ReleaseInfo>(&text)
        .with_context(|| format!("parse release metadata from {url}: {text}"))
}

/// Returns true when `latest` is strictly newer than `current` under semver.
/// On parse failure on either side, returns `current != latest` so users still
/// see a notice on non-semver tags.
pub fn version_is_newer(current: &str, latest: &str) -> bool {
    match (parse_semver(current), parse_semver(latest)) {
        (Some(c), Some(l)) => l > c,
        _ => current.trim_start_matches('v') != latest.trim_start_matches('v'),
    }
}

fn parse_semver(s: &str) -> Option<(u64, u64, u64)> {
    let s = s.trim().trim_start_matches('v');
    // Drop pre-release / build metadata for a coarse comparison.
    let core = s.split(['-', '+']).next()?;
    let mut parts = core.split('.');
    let major: u64 = parts.next()?.parse().ok()?;
    let minor: u64 = parts.next().unwrap_or("0").parse().ok()?;
    let patch: u64 = parts.next().unwrap_or("0").parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

async fn download_file(client: &reqwest::Client, url: &str, dst: &Path) -> Result<()> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("download failed ({status}) for {url}");
    }
    let bytes = response.bytes().await.context("read response body")?;
    let mut file = fs::File::create(dst)
        .with_context(|| format!("create {}", dst.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("write {}", dst.display()))?;
    Ok(())
}

fn verify_checksum(archive: &Path, checksum_file: &Path) -> Result<()> {
    let expected = parse_checksum_file(checksum_file)?;
    let mut hasher = Sha256::new();
    let mut file = fs::File::open(archive)
        .with_context(|| format!("open {}", archive.display()))?;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).context("read archive for checksum")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let actual = format!("{:x}", hasher.finalize());
    if !actual.eq_ignore_ascii_case(&expected) {
        bail!(
            "sha256 mismatch for {}: expected {}, got {}",
            archive.display(),
            expected,
            actual
        );
    }
    Ok(())
}

fn parse_checksum_file(path: &Path) -> Result<String> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    let first = raw
        .lines()
        .next()
        .ok_or_else(|| eyre!("checksum file is empty: {}", path.display()))?;
    let digest = first
        .split_whitespace()
        .next()
        .ok_or_else(|| eyre!("checksum file did not contain a SHA256 digest: {}", path.display()))?;
    Ok(digest.to_string())
}

fn extract_archive(archive: &Path, dst_dir: &Path) -> Result<()> {
    let file = fs::File::open(archive)
        .with_context(|| format!("open {}", archive.display()))?;
    let decoder = GzDecoder::new(file);
    let mut tar = tar::Archive::new(decoder);
    tar.unpack(dst_dir)
        .with_context(|| format!("unpack {} into {}", archive.display(), dst_dir.display()))?;
    Ok(())
}

/// For each known binary that exists in `staging_dir`, replace the version in
/// `install_dir` via a same-filesystem rename. Returns the paths that were
/// updated, in order.
fn replace_binaries(install_dir: &Path, staging_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut updated = Vec::new();
    for name in ["omnigraph", "omnigraph-server"] {
        let staged = staging_dir.join(name);
        if !staged.exists() {
            continue;
        }
        // Only replace `omnigraph-server` if it's already alongside `omnigraph`.
        let target = install_dir.join(name);
        if name == "omnigraph-server" && !target.exists() {
            continue;
        }
        replace_binary(&target, &staged)?;
        updated.push(target);
    }
    Ok(updated)
}

fn replace_binary(target: &Path, src: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(src)
            .with_context(|| format!("stat {}", src.display()))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(src, perms)
            .with_context(|| format!("chmod {}", src.display()))?;
    }
    fs::rename(src, target).with_context(|| {
        format!(
            "rename {} -> {} (same-filesystem rename required)",
            src.display(),
            target.display()
        )
    })?;
    Ok(())
}

fn ensure_dir_writable(dir: &Path) -> Result<()> {
    let probe = dir.join(format!(".omnigraph-update-probe-{}", std::process::id()));
    match fs::File::create(&probe) {
        Ok(_) => {
            let _ = fs::remove_file(&probe);
            Ok(())
        }
        Err(err) => bail!(
            "cannot write to install directory {}: {err}. Re-run via the install script with appropriate permissions, or rerun as root.",
            dir.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_compare_semver() {
        assert!(version_is_newer("0.4.1", "0.5.0"));
        assert!(version_is_newer("0.4.1", "v0.4.2"));
        assert!(!version_is_newer("0.5.0", "0.4.9"));
        assert!(!version_is_newer("0.4.1", "0.4.1"));
        assert!(!version_is_newer("0.4.1", "v0.4.1"));
    }

    #[test]
    fn version_compare_non_semver_falls_back_to_inequality() {
        assert!(version_is_newer("0.4.1", "edge"));
        assert!(!version_is_newer("edge", "edge"));
    }

    #[test]
    fn homebrew_detection_recognizes_canonical_prefixes() {
        assert!(detect_homebrew_install(Path::new("/opt/homebrew/bin/omnigraph")).is_some());
        assert!(
            detect_homebrew_install(Path::new(
                "/usr/local/Cellar/omnigraph/0.4.1/bin/omnigraph"
            ))
            .is_some()
        );
        assert!(detect_homebrew_install(Path::new("/home/ubuntu/.local/bin/omnigraph")).is_none());
    }

    #[test]
    fn platform_asset_name_known_platforms() {
        let name = platform_asset_name();
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("linux", "x86_64") => {
                assert_eq!(name.unwrap(), "omnigraph-linux-x86_64.tar.gz");
            }
            ("macos", "aarch64") => {
                assert_eq!(name.unwrap(), "omnigraph-macos-arm64.tar.gz");
            }
            _ => {
                assert!(name.is_err());
            }
        }
    }
}
