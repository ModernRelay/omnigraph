//! Change-feed paging and durable baseline installation.
use super::prepare::{ChangeFilter, Pagination};
use super::{ChangesCommand, HistoryScope};
use crate::client::{self, ChangeFilterArgs, GraphClient};
use crate::output::{
    ChangeFeedHumanStream, ChangeFeedJsonStream, write_change_baseline_human, write_json,
};
use color_eyre::eyre::{Result, bail};
use std::fs;
use std::io::{self, Write};
use std::path::Path;

/// Exclusive advisory lock on `<out>.lock`, serializing cooperating baseline
/// captures of the same `--out` from before staging through cursor delivery.
/// Two concurrent captures otherwise both replace `--out` and both print their
/// own cursor, and the one that printed after being replaced pairs its cursor
/// with the other's snapshot — a consumer restoring that pairing skips the
/// changes between the two snapshot positions. The lock file is a stable
/// sibling (never removed), so lockers cannot race a delete; blocking
/// `LOCK_EX` makes the second capture wait rather than fail. Unix-only, like
/// the rest of the baseline install barrier.
#[cfg(unix)]
struct BaselineOutLock {
    _file: fs::File,
}

#[cfg(unix)]
impl BaselineOutLock {
    fn acquire(out: &std::path::Path) -> Result<Self> {
        use std::os::unix::io::AsRawFd;
        let lock_path = {
            let mut os = out.as_os_str().to_os_string();
            os.push(".lock");
            std::path::PathBuf::from(os)
        };
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;
        // Blocking exclusive lock; released on drop (close).
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(Self { _file: file })
    }
}

/// Whether the file NOW at `path` is the very file handle this capture
/// installed (same device + inode). A concurrent or foreign writer that
/// replaced `--out` after our atomic persist yields a different inode, so the
/// caller can refuse to print a resume cursor that no longer pairs with the
/// installed snapshot. Content-free: the baseline file deliberately carries
/// only snapshot records (the handshake goes to stdout), so identity — not a
/// terminal marker — is the correct witness.
#[cfg(unix)]
fn installed_file_is_current(installed: &fs::File, path: &std::path::Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let ours = installed.metadata()?;
    let now = fs::metadata(path)?;
    Ok(ours.dev() == now.dev() && ours.ino() == now.ino())
}

pub(crate) async fn run_changes(
    command: ChangesCommand,
    scope: HistoryScope<'_>,
    stdout: &mut dyn Write,
) -> Result<()> {
    match command {
        ChangesCommand::Poll {
            uri,
            branch,
            cursor,
            start,
            limit,
            kinds,
            types,
            ops,
            settings,
            json,
        } => {
            let settings = client::parse_set_flags(&settings)?;
            let client = GraphClient::resolve(
                scope.capability,
                scope.server,
                scope.graph,
                uri,
                scope.profile,
                scope.store,
            )
            .await?;
            let filter = ChangeFilter::from_args(kinds, types, ops);
            let filter = filter.as_args();
            let pagination = Pagination::from_args(None, limit);
            if json {
                let mut output = ChangeFeedJsonStream::new(io::BufWriter::new(&mut *stdout));
                let mut next_page_token: Option<String> = None;
                loop {
                    let page = client
                        .poll_changes_page(
                            branch.as_deref(),
                            cursor.as_deref(),
                            start.as_deref(),
                            next_page_token.as_deref(),
                            pagination.limit(),
                            &filter,
                            &settings,
                        )
                        .await?;
                    next_page_token = page.next_page_token.clone();
                    output.write_page(&page)?;
                    if next_page_token.is_none() {
                        let _ = output.finish(page.cursor.as_deref(), page.caught_up)?;
                        break;
                    }
                }
            } else {
                let mut output = ChangeFeedHumanStream::new(&mut *stdout);
                let mut next_page_token: Option<String> = None;
                loop {
                    let page = client
                        .poll_changes_page(
                            branch.as_deref(),
                            cursor.as_deref(),
                            start.as_deref(),
                            next_page_token.as_deref(),
                            pagination.limit(),
                            &filter,
                            &settings,
                        )
                        .await?;
                    next_page_token = page.next_page_token.clone();
                    output.write_page(&page)?;
                    if next_page_token.is_none() {
                        let _ = output.finish(page.cursor.as_deref(), page.caught_up)?;
                        break;
                    }
                }
            }
        }
        ChangesCommand::Baseline {
            uri,
            branch,
            kinds,
            types,
            ops,
            out,
            json,
        } => {
            let client = GraphClient::resolve(
                scope.capability,
                scope.server,
                scope.graph,
                uri,
                scope.profile,
                scope.store,
            )
            .await?;
            let filter = ChangeFilter::from_args(kinds, types, ops);
            let filter = filter.as_args();
            capture_baseline(&client, branch.as_deref(), &filter, &out, json, stdout).await?;
        }
    }
    Ok(())
}

/// The resolved-client boundary owns installation through delivery of its cursor.
pub(crate) async fn capture_baseline(
    client: &GraphClient,
    branch: Option<&str>,
    filter: &ChangeFilterArgs<'_>,
    out: &Path,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    // The install contract below (file fsync + atomic replace +
    // parent-directory fsync) is POSIX-shaped. Elsewhere (e.g.
    // Windows, where std's rename is not write-through and a
    // directory is not an fsync-able durability primitive) the
    // namespace replacement cannot be proven durable before the
    // resume cursor prints, so fail closed BEFORE any capture work
    // rather than print a cursor for a snapshot the filesystem may
    // lose. A durable/write-through replace is the sanctioned
    // future lift for those platforms.
    if cfg!(not(unix)) {
        bail!(
            "changes baseline is not supported on this platform: the \
         snapshot-install durability barrier (directory fsync after \
         an atomic replace) requires a POSIX filesystem"
        );
    }
    // Serialize cooperating captures of the same --out from before
    // staging through cursor delivery; held until this arm returns.
    #[cfg(unix)]
    let _out_lock = BaselineOutLock::acquire(out)?;
    // Stream into a UNIQUE temp file in --out's directory and
    // atomically replace --out only after the handshake completes
    // AND the bytes are durable. NamedTempFile is created O_EXCL and
    // removes itself on drop, so two concurrent captures cannot
    // truncate each other's staging file, a failed capture never
    // destroys the previous snapshot, and — with the fsync barrier
    // below — a crash never exposes a resume cursor for a snapshot
    // that is not durably on disk.
    let out_dir = match out.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    let mut temp = tempfile::NamedTempFile::new_in(&out_dir)?;
    let baseline = {
        let mut writer = io::BufWriter::new(temp.as_file_mut());
        match client.change_baseline(branch, filter, &mut writer).await {
            Ok(baseline) => {
                writer.flush()?;
                baseline
            }
            // `temp` auto-removes on drop; --out is left untouched.
            Err(error) => return Err(error),
        }
    };
    // Durability barrier BEFORE the resume cursor is printed: fsync
    // the file, atomically persist it over --out, then fsync the
    // parent directory so the rename itself survives a crash. Every
    // step propagates its failure with `?`, so a durability error
    // returns before any cursor is emitted — a printed cursor always
    // implies the snapshot is durably on disk.
    temp.as_file().sync_all()?;
    let installed = temp.persist(out).map_err(|error| error.error)?;
    crate::sync_dir(&out_dir)?;
    // Concurrent-capture guard (belt to the lock's braces): if a
    // NON-cooperating writer replaced --out after our atomic
    // persist, the path no longer names the file we installed
    // (different inode), and printing our cursor would pair it
    // with someone else's snapshot — a consumer restoring that
    // pairing skips the changes between the two snapshot
    // positions. Refuse the print instead. Cooperating captures
    // are already serialized by the .lock file, so they can never
    // hit this between our persist and print.
    #[cfg(unix)]
    if !installed_file_is_current(&installed, out)? {
        bail!(
            "the baseline at {} was replaced by another writer after \
         this capture installed its snapshot; no resume cursor \
         printed — re-run the capture",
            out.display()
        );
    }
    let _ = installed;
    if json {
        write_json(&baseline, stdout)?;
    } else {
        write_change_baseline_human(&baseline, out, stdout)?;
    }
    // Keep the output lock until the cursor has reached the supplied sink.
    stdout.flush()?;
    Ok(())
}
