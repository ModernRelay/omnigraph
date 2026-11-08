//! Every Lance commit the engine makes starts from [`commit_builder`], so the
//! one policy a simulated run needs has one home: Lance bounds a commit with
//! [`DEFAULT_COMMIT_TIMEOUT`], a tokio timer that stays pending for as long
//! as the commit runs. On a paused clock that timer is the far alarm the
//! runtime jumps to the moment it idles with a commit held open (a GQT block
//! parking a session inside its commit), so under simulation the bound is
//! off and the harness's wall-clock budget is the hang guard instead.
//! Production keeps Lance's default. The engine and the catalog write data
//! through `execute_uncommitted` and commit the transaction here; the
//! dataset creations through `Dataset::write` keep Lance's own commit,
//! which no block can hold open since a block runs on an opened graph.

use std::time::Duration;

use lance::dataset::{CommitBuilder, DEFAULT_COMMIT_TIMEOUT, WriteDestination};

use crate::dst_clock;

/// The bound one commit runs under: Lance's default, or none while the
/// simulation clock is installed on this thread.
pub fn commit_timeout() -> Option<Duration> {
    if dst_clock::simulated() {
        None
    } else {
        Some(DEFAULT_COMMIT_TIMEOUT)
    }
}

/// A commit builder for `dest` under the engine's policy; callers add their
/// own options after it.
pub fn commit_builder<'a>(dest: impl Into<WriteDestination<'a>>) -> CommitBuilder<'a> {
    CommitBuilder::new(dest).with_timeout(commit_timeout())
}
