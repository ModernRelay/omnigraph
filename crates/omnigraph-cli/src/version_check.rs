//! Background notification when a newer stable omnigraph release is available.
//!
//! Design (best practice — same pattern as `npm/update-notifier` and `gh`):
//!
//! 1. The fast path is a single JSON file read at
//!    `$XDG_CACHE_HOME/omnigraph/update-check.json` (default
//!    `~/.cache/omnigraph/update-check.json`). If the cache says a newer
//!    stable release is out, emit one stderr line and continue. No network
//!    on this path.
//! 2. When the cache is stale or missing, refresh it in a **detached child
//!    process** (`omnigraph __refresh-update-cache`). The parent never blocks
//!    on the network; the child writes the cache on its next run.
//! 3. Suppression: `OMNIGRAPH_NO_UPDATE_CHECK=1`, `CI` set, stdout not a TTY,
//!    and the `version` / `update` / `__refresh-update-cache` subcommands.
//! 4. The notice is written to stderr only — stdout is reserved for piped /
//!    scripted output.

use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use color_eyre::eyre::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::update::{REPO_SLUG, fetch_latest_stable_tag, version_is_newer};

const CACHE_FILE_NAME: &str = "update-check.json";
const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Hidden subcommand name used for the detached background refresh.
pub const REFRESH_SUBCOMMAND: &str = "__refresh-update-cache";

#[derive(Debug, Serialize, Deserialize)]
struct CacheEntry {
    /// Last time we successfully fetched the latest release.
    checked_at_unix: u64,
    /// Latest tag observed at `checked_at_unix`, with any `v` prefix stripped.
    latest_version: String,
    /// Repo slug the cache was populated for. Lets us invalidate the cache if
    /// the binary is ever pointed at a different repo (e.g. a fork).
    #[serde(default)]
    repo_slug: String,
}

/// Read the cache (if any) and emit a stderr notice when a newer stable
/// version is recorded. Spawn a detached refresh subprocess if the cache is
/// stale. Never blocks on the network and never returns an error.
pub fn maybe_notify(current_version: &str, subcommand_skips_check: bool) {
    if subcommand_skips_check || is_suppressed() {
        return;
    }

    let cache_path = match cache_file_path() {
        Some(p) => p,
        None => return,
    };

    let now = unix_now();
    let cached = read_cache(&cache_path);

    if let Some(entry) = &cached
        && entry.repo_slug == REPO_SLUG
        && version_is_newer(current_version, &entry.latest_version)
    {
        let _ = writeln!(
            io::stderr(),
            "A new version of omnigraph is available ({} -> {}). Run `omnigraph update` to upgrade.",
            current_version,
            entry.latest_version,
        );
    }

    let cache_is_fresh = cached
        .as_ref()
        .map(|e| now.saturating_sub(e.checked_at_unix) < CACHE_TTL.as_secs())
        .unwrap_or(false);
    if !cache_is_fresh {
        spawn_detached_refresh();
    }
}

/// Body of the hidden `__refresh-update-cache` subcommand. Performs a single
/// network call to the GitHub Releases API and writes the cache file.
///
/// On a transient failure (network down, rate-limited, …) we still touch the
/// cache file with the previously-known `latest_version` (or the running
/// binary's own version as a fallback) so the 24h cooldown is honored and the
/// parent process doesn't spawn a fresh refresh on every invocation.
pub async fn refresh_cache_subcommand() -> Result<()> {
    let cache_path = cache_file_path().ok_or_else(|| {
        color_eyre::eyre::eyre!("could not resolve cache directory (HOME unset?)")
    })?;
    let previous = read_cache(&cache_path);
    let client = reqwest::Client::builder()
        .user_agent(concat!("omnigraph-cli/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(10))
        .build()
        .context("build reqwest client")?;
    match fetch_latest_stable_tag(&client).await {
        Ok(tag) => {
            let version = tag.trim().trim_start_matches('v').to_string();
            if version.is_empty() {
                touch_cooldown(&cache_path, previous.as_ref());
                bail!("release metadata missing tag_name");
            }
            let entry = CacheEntry {
                checked_at_unix: unix_now(),
                latest_version: version,
                repo_slug: REPO_SLUG.to_string(),
            };
            write_cache(&cache_path, &entry)?;
            Ok(())
        }
        Err(err) => {
            // Persist a fresh `checked_at_unix` even on failure so we honor
            // the 24h cooldown across transient outages. We keep whatever
            // `latest_version` we already knew (or fall back to the running
            // binary's own version, which never produces a false "newer
            // available" notice).
            touch_cooldown(&cache_path, previous.as_ref());
            Err(err)
        }
    }
}

/// Write a fresh `checked_at_unix` timestamp to the cache while keeping the
/// previously-known `latest_version` (or, when there is none, recording the
/// current binary's version so `version_is_newer` doesn't fire spuriously).
fn touch_cooldown(cache_path: &Path, previous: Option<&CacheEntry>) {
    let latest_version = previous
        .map(|p| p.latest_version.clone())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
    let entry = CacheEntry {
        checked_at_unix: unix_now(),
        latest_version,
        repo_slug: REPO_SLUG.to_string(),
    };
    let _ = write_cache(cache_path, &entry);
}

fn spawn_detached_refresh() {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(_) => return,
    };
    let mut cmd = Command::new(exe);
    cmd.arg(REFRESH_SUBCOMMAND)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Detach the child from the parent's process group so it survives parent
    // exit and never inherits a controlling terminal.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                // SAFETY: setsid sets a new session+process group; safe pre-exec.
                if libc_setsid() < 0 {
                    // best-effort; don't fail spawn
                }
                Ok(())
            });
        }
    }
    let _ = cmd.spawn();
}

#[cfg(unix)]
fn libc_setsid() -> i64 {
    // We avoid pulling the `libc` crate as a direct dep just for setsid;
    // declare the prototype inline.
    unsafe extern "C" {
        fn setsid() -> i32;
    }
    unsafe { setsid() as i64 }
}

fn read_cache(path: &Path) -> Option<CacheEntry> {
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_cache(path: &Path, entry: &CacheEntry) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create cache dir {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec(entry).context("serialize cache entry")?;
    fs::write(&tmp, &bytes).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn cache_file_path() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("OMNIGRAPH_UPDATE_CACHE_DIR") {
        return Some(PathBuf::from(dir).join(CACHE_FILE_NAME));
    }
    let base = if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
        if !xdg.is_empty() {
            PathBuf::from(xdg)
        } else {
            home_dir()?.join(".cache")
        }
    } else {
        home_dir()?.join(".cache")
    };
    Some(base.join("omnigraph").join(CACHE_FILE_NAME))
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_suppressed() -> bool {
    if env_truthy("OMNIGRAPH_NO_UPDATE_CHECK") {
        return true;
    }
    if env_truthy("CI") {
        return true;
    }
    // Suppress when stdout is not a TTY — covers pipes, scripts, and CI runners
    // that don't set `CI`.
    if !io::stdout().is_terminal() {
        return true;
    }
    false
}

fn env_truthy(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.trim();
            !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn cache_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("update-check.json");
        let entry = CacheEntry {
            checked_at_unix: 1_700_000_000,
            latest_version: "0.5.0".to_string(),
            repo_slug: REPO_SLUG.to_string(),
        };
        write_cache(&path, &entry).unwrap();
        let loaded = read_cache(&path).unwrap();
        assert_eq!(loaded.checked_at_unix, 1_700_000_000);
        assert_eq!(loaded.latest_version, "0.5.0");
        assert_eq!(loaded.repo_slug, REPO_SLUG);
    }

    #[test]
    fn read_cache_missing_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.json");
        assert!(read_cache(&path).is_none());
    }

    #[test]
    fn env_truthy_handles_common_falsy_values() {
        unsafe {
            std::env::remove_var("__OMNIGRAPH_TEST_TRUTHY");
        }
        assert!(!env_truthy("__OMNIGRAPH_TEST_TRUTHY"));
        for value in ["", "0", "false", "FALSE"] {
            unsafe {
                std::env::set_var("__OMNIGRAPH_TEST_TRUTHY", value);
            }
            assert!(
                !env_truthy("__OMNIGRAPH_TEST_TRUTHY"),
                "expected falsy for {value:?}",
            );
        }
        for value in ["1", "true", "yes", "anything"] {
            unsafe {
                std::env::set_var("__OMNIGRAPH_TEST_TRUTHY", value);
            }
            assert!(
                env_truthy("__OMNIGRAPH_TEST_TRUTHY"),
                "expected truthy for {value:?}",
            );
        }
        unsafe {
            std::env::remove_var("__OMNIGRAPH_TEST_TRUTHY");
        }
    }
}
