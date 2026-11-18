//! The writer epoch a process holds per branch, and the check every
//! publication makes against the epoch its load read.
//!
//! A branch's epoch is the `writer_epoch` row of its own `__manifest`
//! ([`crate::state::WriterEpochRow`]). A process holds an epoch for a branch
//! only after a claim it published landed there: no reload raises what it
//! holds, a refusal clears it, its own delete of the branch drops it, and
//! otherwise it ends with the process. The registry lives here and not beside
//! the engine's write queues because the check runs in the publisher, a crate
//! below the engine.
//!
//! The entry also records the `__manifest` version of the process's last
//! publication on the branch. No other process's publication can land on a
//! held branch, so a handle whose view is at or above that version reads it
//! without a probe; `docs/dev/writes.md` lists what the fence does not cover.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::error::Result;
use crate::state::WriterEpochRow;

/// One branch of one root in the registry: the root identity the engine's
/// write queues are shared by, and the branch's logical name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct HolderKey {
    root: String,
    branch: String,
}

impl HolderKey {
    pub(crate) fn resolve(root_uri: &str, branch: Option<&str>) -> Result<Self> {
        Ok(Self {
            root: crate::storage::write_queue_root_identity(root_uri)?,
            branch: branch.unwrap_or("main").to_string(),
        })
    }
}

/// What a process holds for one branch: the epoch of the last claim it landed
/// there, the native ref that claim was published on, and the `__manifest`
/// version of its last publication there, `None` once an attempt was withdrawn.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeldEpoch {
    epoch: u64,
    native: Option<String>,
    landed: Option<u64>,
}

/// The writer epochs one process holds. Every publisher of the process shares
/// [`Self::of_process`]; a test builds its own to stand for a second process.
#[derive(Debug, Default)]
pub(crate) struct WriterEpochHolders {
    held: Mutex<HashMap<HolderKey, HeldEpoch>>,
}

impl WriterEpochHolders {
    pub(crate) fn of_process() -> Arc<Self> {
        static PROCESS: OnceLock<Arc<WriterEpochHolders>> = OnceLock::new();
        Arc::clone(PROCESS.get_or_init(Arc::default))
    }

    /// Whether this process holds no branch at all, so a publish skips the
    /// lookup and its key.
    pub(crate) fn holds_nothing(&self) -> bool {
        self.lock().is_empty()
    }

    /// The epoch held for `key` on the chain whose native ref is `native`. An
    /// entry a claim on another native ref set is dropped: a branch recreated
    /// under its name starts with nothing held.
    pub(crate) fn held(&self, key: &HolderKey, native: Option<&str>) -> Option<u64> {
        let mut held = self.lock();
        let entry = held.get(key)?;
        if entry.native.as_deref() == native {
            return Some(entry.epoch);
        }
        held.remove(key);
        None
    }

    /// Record the claim of `epoch` that landed on `native` as `version`.
    pub(crate) fn record_claim(
        &self,
        key: &HolderKey,
        native: Option<&str>,
        epoch: u64,
        version: u64,
    ) {
        self.lock().insert(
            key.clone(),
            HeldEpoch {
                epoch,
                native: native.map(str::to_string),
                landed: Some(version),
            },
        );
    }

    /// Raise the version held on `native` to a publication this process
    /// landed there; a process holding nothing on `native` records nothing.
    pub(crate) fn record_publication(&self, key: &HolderKey, native: Option<&str>, version: u64) {
        let mut held = self.lock();
        let entry = held
            .get_mut(key)
            .filter(|entry| entry.native.as_deref() == native);
        if let Some(entry) = entry {
            entry.landed = Some(entry.landed.map_or(version, |landed| landed.max(version)));
        }
    }

    /// Withdraw the version held for `key`: a publish that returned an error
    /// or was dropped, a claim about to put and this process's delete of the
    /// branch about to run may each move the head with no record of it.
    pub(crate) fn withdraw_version(&self, key: &HolderKey) {
        if let Some(entry) = self.lock().get_mut(key) {
            entry.landed = None;
        }
    }

    /// Drop the entry for `key` that a claim on `native` set: this process
    /// deleted that incarnation of the branch.
    pub(crate) fn release(&self, key: &HolderKey, native: &str) {
        let mut held = self.lock();
        if held
            .get(key)
            .is_some_and(|entry| entry.native.as_deref() == Some(native))
        {
            held.remove(key);
        }
    }

    /// The version of this process's last publication on `native` while no
    /// attempt since was withdrawn. The entry of another native ref is left
    /// alone.
    pub(crate) fn landed_version(&self, key: &HolderKey, native: Option<&str>) -> Option<u64> {
        let held = self.lock();
        let entry = held.get(key)?;
        (entry.native.as_deref() == native)
            .then_some(entry.landed)
            .flatten()
    }

    /// Drop the entry for `key` while it still holds `epoch`, as the
    /// `WriterFenced` refusal that read `epoch` does; a claim that landed
    /// since keeps its entry.
    pub(crate) fn clear(&self, key: &HolderKey, epoch: u64) {
        let mut held = self.lock();
        if held.get(key).is_some_and(|entry| entry.epoch == epoch) {
            held.remove(key);
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<HolderKey, HeldEpoch>> {
        self.held
            .lock()
            .expect("writer epoch holders lock poisoned")
    }
}

/// What a publication asserts about the writer epoch its load read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochAssertion {
    /// An ordinary publication: the rows stand at the epoch the process
    /// holds, 0 when it holds none.
    Publish,
    /// A claim whose first load read `read`: the rows still stand there.
    Claim { read: u64 },
}

/// What a publication that passed the check does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochVerdict {
    /// Write: an ordinary publication its rows, a claim the row at `read + 1`.
    Proceed,
    /// A claim by the process that holds the rows' epoch: nothing to write.
    AlreadyHeld,
}

/// A refused check, the fields of `ManifestConflictDetails::WriterFenced`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Fenced {
    pub(crate) held: Option<u64>,
    pub(crate) read: Option<u64>,
    pub(crate) current: u64,
}

/// The check of one attempt: `held` is what the process holds on the chain
/// the attempt loaded, `current` the epoch of the rows it loaded.
pub(crate) fn check_writer_epoch(
    held: Option<u64>,
    current: u64,
    assertion: EpochAssertion,
) -> std::result::Result<EpochVerdict, Fenced> {
    match assertion {
        EpochAssertion::Publish if held.unwrap_or(0) == current => Ok(EpochVerdict::Proceed),
        EpochAssertion::Publish => Err(Fenced {
            held,
            read: None,
            current,
        }),
        EpochAssertion::Claim { .. } if held == Some(current) => Ok(EpochVerdict::AlreadyHeld),
        EpochAssertion::Claim { read } if read == current => Ok(EpochVerdict::Proceed),
        EpochAssertion::Claim { read } => Err(Fenced {
            held,
            read: Some(read),
            current,
        }),
    }
}

/// How a claim whose put returned no answer settles, by the `writer_epoch`
/// row of the version it attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnansweredClaim {
    /// The version holds the row with this claim's attempt id.
    Landed,
    /// Another publication holds the version: no request of this claim lands.
    Lost,
    /// The store acknowledged the version and it holds something else.
    InDoubt,
}

pub(crate) fn settle_unanswered_claim(
    attempted: Option<&WriterEpochRow>,
    attempt_id: &str,
    after_ack: bool,
) -> UnansweredClaim {
    match attempted {
        Some(row) if row.attempt_id == attempt_id => UnansweredClaim::Landed,
        _ if after_ack => UnansweredClaim::InDoubt,
        _ => UnansweredClaim::Lost,
    }
}
