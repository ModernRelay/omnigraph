//! The one owner of a write that prepares again.
//!
//! A mutation, a load and a Blob write prepare outside the writer gates and
//! revalidate under them. A publication that moved the branch in between
//! sends the attempt back with `ReadSetChanged` before any effect, unless
//! every publication since the capture changed no row (optimize, an index
//! build): revalidation then carries the attempt onto the current head
//! ([`Omnigraph::revalidate_write_txn_carrying`], reached by every one of
//! these writers through `StagedMutation::commit_all`), and the attempt goes
//! on. Lance's conflict rules still refuse to carry an update of a row in a
//! fragment a rewrite replaced; that attempt is sent back too.
//!
//! A write that was sent back prepares again with the branch's writer gates
//! held, from its next capture through its publication
//! ([`WriteAttempt`]). Every publisher on the branch in this process, optimize
//! included, takes the same branch gate, so nothing can move the branch under
//! that attempt: a write finishes in at most two attempts, whatever sent it
//! back and however often maintenance runs. The cost, one preparation under
//! the gates, is paid only after a send-back. A publication by another
//! process is outside that bound (one mutation-capable writer process is the
//! supported topology); the publisher's CAS refuses it, and the write prepares
//! again within [`MAX_PRE_EFFECT_REPREPARES`].
//!
//! A send-back whose publications changed no row is never the caller's: the
//! write's logical reads still hold, so even a strict update or delete
//! prepares again instead of returning `ReadSetChanged`. Taking all of this
//! from [`PreEffectReprepares`] keeps the next writer of this kind from
//! forgetting any of it.

use super::omnigraph::Omnigraph;
use super::write_queue::BranchWriteGates;
use crate::error::{OmniError, Result};

/// How many times a write prepares again before its `ReadSetChanged` is the
/// caller's.
const MAX_PRE_EFFECT_REPREPARES: usize = 32;

/// What one attempt of a write carries to its revalidation: the writer gates
/// it prepared under, if any, and whether a row-preserving carry was refused.
#[derive(Default)]
pub(crate) struct WriteAttempt {
    gates: Option<BranchWriteGates>,
    carry_refused: bool,
}

impl WriteAttempt {
    /// The schema permit and branch gate this attempt prepared under, for
    /// `commit_all` to complete with the table gates; `None` on a first
    /// attempt, which takes them at its revalidation.
    pub(crate) fn take_gates(&mut self) -> Option<BranchWriteGates> {
        self.gates.take()
    }

    /// Revalidation proved that every publication since the capture changed
    /// no row, and Lance's conflict rules refused to carry a staged table
    /// over them. The write's logical reads still hold, so it prepares again
    /// whatever its kind.
    pub(crate) fn note_carry_refused(&mut self) {
        self.carry_refused = true;
    }
}

/// One write that may prepare again: its attempt count and the state of its
/// current attempt.
pub(crate) struct PreEffectReprepares<'a> {
    db: &'a Omnigraph,
    branch: Option<&'a str>,
    what: &'static str,
    attempt: usize,
    current: WriteAttempt,
}

impl<'a> PreEffectReprepares<'a> {
    /// A write of `what` on `branch` starts. Main is one gate whether the
    /// caller names it or not.
    pub(crate) fn begin(db: &'a Omnigraph, branch: Option<&'a str>, what: &'static str) -> Self {
        Self {
            db,
            branch: branch.filter(|name| *name != "main"),
            what,
            attempt: 0,
            current: WriteAttempt::default(),
        }
    }

    /// Whether the attempt about to run is the write's first.
    pub(crate) fn is_first(&self) -> bool {
        self.attempt == 0
    }

    /// The attempt about to run, for its writer to pass to `commit_all`.
    pub(crate) fn attempt(&mut self) -> &mut WriteAttempt {
        &mut self.current
    }

    /// Whether `error` from an attempt sends the write back to prepare
    /// again: a `ReadSetChanged`, within the bound, that the attempt marked
    /// `retryable` or that a refused row-preserving carry raised.
    pub(crate) fn may_prepare_again(&self, error: &OmniError, retryable: bool) -> bool {
        (retryable || self.current.carry_refused)
            && error.is_read_set_changed()
            && self.attempt < MAX_PRE_EFFECT_REPREPARES
    }

    /// Go round again: take the branch's writer gates for the next attempt,
    /// then refresh the coordinator it prepares from under them.
    pub(crate) async fn prepare_again(&mut self) -> Result<()> {
        self.attempt += 1;
        tracing::debug!(
            attempt = self.attempt,
            branch = self.branch.unwrap_or("main"),
            write = self.what,
            "prepared write authority changed before effects; preparing again under the writer gates"
        );
        crate::instrumentation::record_mutation_reprepare();
        // The last attempt's gates, if it never reached `commit_all`, go
        // first: the gates are not reentrant.
        self.current = WriteAttempt::default();
        let gates = self
            .db
            .write_queue()
            .acquire_branch_write_gates(self.branch)
            .await;
        self.current.gates = Some(gates);
        self.db.refresh_coordinator_only().await
    }
}
