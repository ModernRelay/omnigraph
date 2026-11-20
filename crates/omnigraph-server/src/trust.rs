//! Trust snapshots: a credential set held as one immutable, generation-stamped
//! value that a request reads once, at admission, and keeps for its lifetime.
//!
//! A [`TrustHolder`] owns one surface's set. Its source is read and validated
//! by [`TrustHolder::refresh`], the one place the set changes, which runs
//! under the holder's installer lock: read, compare the raw bytes with the
//! installed snapshot's, validate, hash, compare the canonical digest, and
//! store. A refusal at any step leaves the installed snapshot in force and
//! is logged once per distinct refused content, or once per error class for
//! a failure that yields no content, until the next successful read.
//! Triggers (the interval timer of a polled source) call `refresh` and
//! nothing else.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use arc_swap::{ArcSwap, Guard};
use color_eyre::eyre::{Report, Result, eyre};
use sha2::{Digest as _, Sha256};
use tracing::{info, warn};
use zeroize::Zeroizing;

/// A SHA-256 digest.
pub(crate) type Digest = [u8; 32];

/// The most bytes a source may yield. One byte more is read to tell a file at
/// the bound from one past it.
pub(crate) const MAX_SOURCE_BYTES: usize = 256 * 1024;

/// How a source's content reaches the installed snapshot after boot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Freshness {
    /// Read once, at startup.
    BootOnly,
    /// Read again every `interval`.
    Polled { interval: Duration },
}

/// Why a source yielded no complete content. `class` is fixed text naming the
/// failure; it keys the once-per-class warning, so it carries no path or
/// detail.
#[derive(Debug)]
pub(crate) struct LoadFailure {
    pub(crate) class: &'static str,
    pub(crate) error: Report,
}

impl LoadFailure {
    pub(crate) fn new(class: &'static str, error: Report) -> Self {
        Self { class, error }
    }
}

/// A configured credential source: where the raw bytes come from. Its
/// [`Freshness`] is fixed when it is constructed, and a constructor refuses a
/// policy the source cannot honor.
pub(crate) trait TrustSource: Send + Sync {
    /// The source's name in logs.
    fn name(&self) -> &'static str;
    /// The source as a refusal names it, e.g. with its path.
    fn describe(&self) -> String;
    fn freshness(&self) -> Freshness;
    /// The source's current bytes, read afresh.
    fn load(&self) -> std::result::Result<Zeroizing<Vec<u8>>, LoadFailure>;
}

/// One surface's validated credential set. It holds no plaintext secret.
pub(crate) trait TrustSet: Send + Sync + Sized + 'static {
    /// What a token this set admits resolves to.
    type Admitted;
    /// The inputs every decode combines with the raw bytes, fixed for the
    /// holder's lifetime.
    type Rules: Send + Sync;
    /// Names the set in log lines, e.g. "static principal set".
    const LABEL: &'static str;

    /// Parse and validate raw bytes, hashing every secret they carry.
    fn decode(raw: &[u8], rules: &Self::Rules) -> Result<Self>;
    /// The canonical digest: equal for two sets that admit the same tokens
    /// as the same actors, whatever bytes they were parsed from.
    fn digest(&self) -> Digest;
    /// The actor ids the set admits, for the change line.
    fn actors(&self) -> BTreeSet<&str>;
    /// The request side. `now` is the request's own clock; the holder keeps
    /// none.
    fn admit(&self, token: &str, now: SystemTime) -> Option<Self::Admitted>;
}

/// One installed snapshot.
pub(crate) struct Installed<S> {
    pub(crate) set: S,
    /// 1 at boot, one more per installed change.
    pub(crate) generation: u64,
    /// The canonical digest of `set`.
    pub(crate) digest: Digest,
    /// The digest of the bytes `set` was parsed from; `None` for a set its
    /// loader validated before the holder existed.
    pub(crate) raw_digest: Option<Digest>,
    pub(crate) applied_at: SystemTime,
}

/// The refusal already logged, so a refusal that repeats is not logged again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Warned {
    /// Content that was read whole and refused, by the digest of its bytes.
    Content(Digest),
    /// A failure that yielded no content, by its class.
    NoContent(&'static str),
}

/// What one [`TrustHolder::refresh`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refresh {
    /// The bytes equal the installed snapshot's, or the holder has no source.
    Unchanged,
    /// Different bytes, the same set: the raw digest is recorded and the
    /// generation stays.
    Rewritten,
    /// A changed set, installed under this generation.
    Installed(u64),
    /// The source failed or its content was refused; the installed snapshot
    /// stays. `logged` says whether this call wrote the warning.
    Refused { logged: bool },
}

/// One surface's installed trust snapshot and the source it refreshes from.
pub(crate) struct TrustHolder<S: TrustSet> {
    /// What requests read.
    installed: ArcSwap<Installed<S>>,
    /// One installer at a time; guards the refusal already logged.
    install: Mutex<Option<Warned>>,
    /// `None` for a set its loader validated before the holder existed,
    /// which is never read again.
    source: Option<Box<dyn TrustSource>>,
    source_name: &'static str,
    policy: Freshness,
    rules: S::Rules,
}

fn sha256(bytes: &[u8]) -> Digest {
    Sha256::digest(bytes).into()
}

/// The first eight hexadecimal digits of a digest, for log lines.
pub(crate) fn digest_prefix(digest: &Digest) -> String {
    digest[..4]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl<S: TrustSet> TrustHolder<S> {
    /// Read `source` once and install generation 1. A refusal here is a
    /// startup error, with the source's own message.
    pub(crate) fn open(source: Box<dyn TrustSource>, rules: S::Rules) -> Result<Arc<Self>> {
        let raw = source.load().map_err(|failure| failure.error)?;
        let set =
            S::decode(&raw, &rules).map_err(|error| eyre!("{}: {error:#}", source.describe()))?;
        let raw_digest = Some(sha256(&raw));
        Ok(Arc::new(Self {
            installed: ArcSwap::from_pointee(Installed {
                digest: set.digest(),
                set,
                generation: 1,
                raw_digest,
                applied_at: SystemTime::now(),
            }),
            install: Mutex::new(None),
            source_name: source.name(),
            policy: source.freshness(),
            source: Some(source),
            rules,
        }))
    }

    /// A boot-only holder over a set its loader read and validated at
    /// startup, outside any [`TrustSource`].
    pub(crate) fn with_boot_set(source_name: &'static str, set: S, rules: S::Rules) -> Arc<Self> {
        Arc::new(Self {
            installed: ArcSwap::from_pointee(Installed {
                digest: set.digest(),
                set,
                generation: 1,
                raw_digest: None,
                applied_at: SystemTime::now(),
            }),
            install: Mutex::new(None),
            source: None,
            source_name,
            policy: Freshness::BootOnly,
            rules,
        })
    }

    /// The installed snapshot. A request loads it once, at admission.
    pub(crate) fn load(&self) -> Guard<Arc<Installed<S>>> {
        self.installed.load()
    }

    pub(crate) fn source_name(&self) -> &'static str {
        self.source_name
    }

    pub(crate) fn freshness(&self) -> Freshness {
        self.policy
    }

    #[cfg(test)]
    pub(crate) fn warned(&self) -> Option<Warned> {
        *self.install.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Read the source and install what it holds, if that differs from the
    /// installed snapshot. The one place the set changes.
    pub(crate) fn refresh(&self) -> Refresh {
        let mut warned = self.install.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(source) = self.source.as_ref() else {
            return Refresh::Unchanged;
        };
        let raw = match source.load() {
            Ok(raw) => raw,
            Err(failure) => {
                return self.refuse(
                    &mut warned,
                    Warned::NoContent(failure.class),
                    &failure.error,
                );
            }
        };
        let raw_digest = sha256(&raw);
        let installed = self.installed.load_full();
        // Bytes equal to what is installed cannot install anything else. A
        // refused digest is no skip: validation may depend on the clock.
        if installed.raw_digest == Some(raw_digest) {
            *warned = None;
            return Refresh::Unchanged;
        }
        let set = match S::decode(&raw, &self.rules) {
            Ok(set) => set,
            Err(error) => return self.refuse(&mut warned, Warned::Content(raw_digest), &error),
        };
        drop(raw);
        *warned = None;
        let digest = set.digest();
        if digest == installed.digest {
            // A rewrite that moves no token: no generation, no log line.
            self.installed.store(Arc::new(Installed {
                set,
                generation: installed.generation,
                digest,
                raw_digest: Some(raw_digest),
                applied_at: installed.applied_at,
            }));
            return Refresh::Rewritten;
        }
        let generation = installed.generation + 1;
        let (before, after) = (installed.set.actors(), set.actors());
        let added = after.difference(&before).copied().collect::<Vec<_>>();
        let removed = before.difference(&after).copied().collect::<Vec<_>>();
        info!(
            source = self.source_name,
            generation,
            digest = %digest_prefix(&digest),
            actors = after.len(),
            added = %added.join(","),
            removed = %removed.join(","),
            "{} changed",
            S::LABEL
        );
        if after.is_empty() {
            warn!(
                source = self.source_name,
                "{} is empty: every token is refused until the source names one",
                S::LABEL
            );
        }
        self.installed.store(Arc::new(Installed {
            set,
            generation,
            digest,
            raw_digest: Some(raw_digest),
            applied_at: SystemTime::now(),
        }));
        Refresh::Installed(generation)
    }

    fn refuse(&self, warned: &mut Option<Warned>, refusal: Warned, error: &Report) -> Refresh {
        if *warned == Some(refusal) {
            return Refresh::Refused { logged: false };
        }
        let class = match refusal {
            Warned::Content(_) => "invalid",
            Warned::NoContent(class) => class,
        };
        warn!(
            source = self.source_name,
            class,
            error = %format!("{error:#}"),
            "{} refresh refused; the installed set stays in force, so a token removed \
             from the source is still admitted until the source is valid again",
            S::LABEL
        );
        *warned = Some(refusal);
        Refresh::Refused { logged: true }
    }

    /// Poll the source on its interval, on the blocking pool, while the holder
    /// lives. Missed ticks are skipped, never queued. A boot-only holder
    /// starts nothing.
    pub(crate) fn start_refresh(self: &Arc<Self>) {
        let Freshness::Polled { interval } = self.policy else {
            return;
        };
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                let Some(holder) = weak.upgrade() else {
                    break;
                };
                let source_name = holder.source_name;
                // A panic is contained to its tick; the next tick reads again.
                // The panic payload is not logged, so no source text can reach
                // the log through it.
                if tokio::task::spawn_blocking(move || holder.refresh())
                    .await
                    .is_err()
                {
                    warn!(
                        source = source_name,
                        class = "panicked",
                        "{} refresh tick panicked; the installed set stays in force and the \
                         next tick reads the source again",
                        S::LABEL
                    );
                }
            }
        });
    }
}
