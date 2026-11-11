//! The one owner of a write that prepares again.
//!
//! A mutation, a load and a Blob write prepare outside the writer gates and
//! revalidate under them. A publication that changed no row (optimize, an
//! index build) does not send an attempt back: under the gates, revalidation
//! carries it over that publication onto the current head
//! ([`Omnigraph::revalidate_write_txn_carrying`], used by every one of these
//! writers through `StagedMutation::commit_all`). Any other publication that
//! moved the branch in between, or one Lance's conflict rules refuse to carry
//! a staged table over, fails the attempt with `ReadSetChanged` before any
//! effect, and the write prepares again from a refreshed coordinator, a
//! bounded number of times. Taking that loop from [`PreEffectReprepares`]
//! keeps the next writer of this kind from forgetting its bound.

use super::omnigraph::Omnigraph;
use crate::error::{OmniError, Result};

/// How many times a write prepares again before its `ReadSetChanged` is the
/// caller's.
const MAX_PRE_EFFECT_REPREPARES: usize = 32;

/// One write that may prepare again, and its attempt count.
pub(crate) struct PreEffectReprepares<'a> {
    db: &'a Omnigraph,
    branch: Option<&'a str>,
    what: &'static str,
    attempt: usize,
}

impl<'a> PreEffectReprepares<'a> {
    /// A write of `what` on `branch` starts.
    pub(crate) fn begin(db: &'a Omnigraph, branch: Option<&'a str>, what: &'static str) -> Self {
        Self {
            db,
            branch,
            what,
            attempt: 0,
        }
    }

    /// Whether the attempt about to run is the write's first.
    pub(crate) fn is_first(&self) -> bool {
        self.attempt == 0
    }

    /// Whether `error` from an attempt sends the write back to prepare
    /// again: a `ReadSetChanged` the attempt marked `retryable`, within the
    /// bound.
    pub(crate) fn may_prepare_again(&self, error: &OmniError, retryable: bool) -> bool {
        retryable && error.is_read_set_changed() && self.attempt < MAX_PRE_EFFECT_REPREPARES
    }

    /// Go round again: refresh the coordinator the next attempt prepares
    /// from.
    pub(crate) async fn prepare_again(&mut self) -> Result<()> {
        self.attempt += 1;
        tracing::debug!(
            attempt = self.attempt,
            branch = self.branch.unwrap_or("main"),
            write = self.what,
            "prepared write authority changed before effects; repreparing"
        );
        crate::instrumentation::record_mutation_reprepare();
        self.db.refresh_coordinator_only().await
    }
}
