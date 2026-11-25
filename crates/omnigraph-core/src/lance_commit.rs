//! Every Lance commit the engine makes starts from [`commit_builder`], so the
//! policy every commit needs has one home.
//!
//! **Session.** A commit runs on the session of the graph that makes it: its
//! object-store registry is the one that knows the graph's captured storage
//! binding, its storage IO scope and, in a test or DST build, the simulated
//! buckets. Lance falls back to `Session::default()`, a fresh registry with
//! Lance's own providers, when a commit to a URI is handed no session, so a
//! [`CommitDestination`] to a URI carries one, and one to an opened dataset
//! uses that dataset's.
//!
//! **Timeout.** Lance bounds a commit with [`DEFAULT_COMMIT_TIMEOUT`], a
//! tokio timer that stays pending for as long as the commit runs. On a paused
//! clock that timer is the far alarm the runtime jumps to the moment it idles
//! with a commit held open (a GQT block parking a session inside its commit),
//! so under simulation the bound is off and the harness's wall-clock budget is
//! the hang guard instead. Production keeps Lance's default.
//!
//! The engine and the catalog write data through `execute_uncommitted` and
//! commit the transaction here; the dataset creations through `Dataset::write`
//! keep Lance's own commit, on the session their `WriteParams` name, and no
//! block can hold one open since a block runs on an opened graph.

use std::sync::Arc;
use std::time::Duration;

use lance::Dataset;
use lance::dataset::{CommitBuilder, DEFAULT_COMMIT_TIMEOUT, WriteDestination};
use lance::session::Session;

use crate::dst_clock;

/// Where a commit lands, with the session its stores come from.
#[derive(Clone)]
pub enum CommitDestination<'a> {
    /// A version of an opened dataset, on the dataset's own session.
    Dataset(Arc<Dataset>),
    /// The dataset at `uri`, which may not exist yet, on `session`.
    Uri { uri: &'a str, session: Arc<Session> },
}

impl From<Arc<Dataset>> for CommitDestination<'_> {
    fn from(dataset: Arc<Dataset>) -> Self {
        Self::Dataset(dataset)
    }
}

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
pub fn commit_builder<'a>(dest: impl Into<CommitDestination<'a>>) -> CommitBuilder<'a> {
    let (dest, session) = match dest.into() {
        CommitDestination::Dataset(dataset) => {
            let session = dataset.session();
            (WriteDestination::Dataset(dataset), session)
        }
        CommitDestination::Uri { uri, session } => (WriteDestination::Uri(uri), session),
    };
    CommitBuilder::new(dest)
        .with_session(session)
        .with_timeout(commit_timeout())
}
