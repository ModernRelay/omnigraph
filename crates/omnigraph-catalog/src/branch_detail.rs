//! What a detailed branch listing reports about each live graph branch: its
//! head, the branch and commit its current lifetime started from, and when
//! that lifetime was created. Every fact is read from main's branch rows, or
//! from the branch refs below the branch-row stamp, and each branch's own
//! `__manifest`; nothing is stored for the listing.

use std::sync::Arc;

use futures::StreamExt;
use lance::Dataset;
use lance::dataset::refs::BranchContents;

use crate::branch_control::list_live_manifest_branch_contents;
use crate::branch_rows::BranchRow;
use crate::error::{OmniError, Result};
use crate::history::ExtentCache;
use crate::instrumentation::VersionResolution;
use crate::layout::{branch_open_error, has_branch_rows, open_manifest_dataset_with_session};
use crate::state::{GraphLineageRow, exact_head, read_manifest_rows_projected};

/// One branch lifetime as its record states it: a `branch` row of main at or
/// above the branch-row stamp, a ref's `BranchContents` below it.
pub(crate) struct BranchLifetime {
    /// The native name of the source branch; `None` for main.
    pub(crate) parent: Option<String>,
    pub(crate) parent_version: u64,
    pub(crate) created_at: u64,
}

impl BranchLifetime {
    fn of_contents(contents: BranchContents) -> Self {
        Self {
            parent: contents.parent_branch,
            parent_version: contents.parent_version,
            created_at: contents.create_at,
        }
    }

    fn of_row(row: &BranchRow) -> Self {
        Self {
            parent: (!row.parent_is_main()).then(|| row.parent.clone()),
            parent_version: row.parent_version,
            created_at: row.created_at,
        }
    }
}

/// One live named graph branch: its logical name and the native name, with
/// the record, of its current lifetime.
pub(crate) struct LiveBranch {
    pub(crate) name: String,
    pub(crate) native: String,
    pub(crate) lifetime: BranchLifetime,
}

/// The live named graph branches of a `__manifest`, sorted by name: each live
/// lifetime mapped to its one logical name, `main` excluded. A name with
/// more than one live incarnation fails loudly. The one enumeration both
/// branch listings use, so names and details agree on what is live. `main`
/// is the main `__manifest`: at or above the branch-row stamp its rows are
/// the set, read with no request.
pub(crate) async fn live_named_branches(main: &Dataset) -> Result<Vec<LiveBranch>> {
    let mut named: Vec<_> = if has_branch_rows(main) {
        crate::BranchRows::decode(main.manifest())?
            .live()
            .map(|row| LiveBranch {
                name: row.logical().to_string(),
                native: row.native.clone(),
                lifetime: BranchLifetime::of_row(row),
            })
            .collect()
    } else {
        list_live_manifest_branch_contents(main)
            .await?
            .into_iter()
            .filter(|(native, _)| native != "main")
            .map(|(native, contents)| LiveBranch {
                name: crate::branch_names::logical_branch_name(&native).to_string(),
                native,
                lifetime: BranchLifetime::of_contents(contents),
            })
            .collect()
    };
    named.sort_by(|left, right| left.name.cmp(&right.name));
    if let Some(pair) = named.windows(2).find(|pair| pair[0].name == pair[1].name) {
        return Err(OmniError::manifest_conflict(format!(
            "branch '{}' has more than one live native incarnation; run cleanup before using it",
            pair[0].name
        )));
    }
    Ok(named)
}

/// How many branches a listing reads at once.
const BRANCH_DETAIL_READ_CONCURRENCY: usize = 8;

/// One listing of the live graph branches.
#[derive(Debug, Clone)]
pub struct GraphBranchListing {
    /// Every branch with a live row, `main` first and the rest by name.
    pub branches: Vec<String>,
    /// One entry per branch the caller admitted, in the order of `branches`.
    pub details: Vec<GraphBranchDetail>,
}

/// One live graph branch as a detailed listing reports it.
#[derive(Debug, Clone)]
pub struct GraphBranchDetail {
    pub name: String,
    /// The head record of the branch's latest `__manifest` version: its newest
    /// commit, or the commit it was created at while it has written none.
    /// `None` while the branch has no tree to read: not ready (RFC "Branch
    /// existence in the catalog" §Readers), or its retirement pending.
    pub head: Option<GraphLineageRow>,
    /// Whether the branch's current lifetime wrote `head`; `None` with it.
    pub head_is_own: Option<bool>,
    /// The logical name of the branch the current lifetime was created from,
    /// as that branch was named then; it may since have been deleted or
    /// recreated. `None` on main.
    pub from: Option<String>,
    /// The head commit of the `__manifest` version the lifetime was created
    /// from: the commit it started at. `None` on main and with `head`.
    pub from_commit_id: Option<String>,
    /// The lifetime's creation time in Unix seconds; `None` on main.
    pub created_at_seconds: Option<u64>,
    /// The tree carries a tombstone: a delete crashed before its retired row
    /// or finished after the rows were read, and the next delete finishes it.
    pub retirement_pending: bool,
}

/// Every live graph branch, with details of those `can_read` admits. One open
/// of main's `__manifest` and, below the branch-row stamp, one listing of its
/// refs; then, for each admitted branch, a checkout of its latest version and
/// one scan of its rows, and when the branch has written a commit, the
/// checkout and scan of the version it was created from. A branch gone
/// before its own reads keeps its name and has no details: it was deleted
/// after the listing.
pub async fn list_graph_branches_detailed(
    root_uri: &str,
    control_session: &Arc<lance::session::Session>,
    can_read: &mut (dyn FnMut(&str) -> bool + Send + '_),
) -> Result<GraphBranchListing> {
    let root = root_uri.trim_end_matches('/');
    let main = open_manifest_dataset_with_session(root, None, control_session).await?;
    let named = live_named_branches(&main).await?;
    let branches = std::iter::once("main".to_string())
        .chain(named.iter().map(|branch| branch.name.clone()))
        .collect();
    let mut details = Vec::with_capacity(named.len() + 1);
    if can_read("main") {
        let head = read_manifest_rows_projected(&main).await?.head;
        details.push(GraphBranchDetail {
            name: "main".to_string(),
            head_is_own: Some(exact_head(&head, None).is_some()),
            head: Some(head),
            from: None,
            from_commit_id: None,
            created_at_seconds: None,
            retirement_pending: false,
        });
    }
    let admitted: Vec<_> = named
        .into_iter()
        .filter(|branch| can_read(&branch.name))
        .collect();
    let legacy = ExtentCache::default();
    let mut reads = futures::stream::iter(admitted)
        .map(|branch| {
            let (main, legacy) = (&main, &legacy);
            async move { branch_detail(root, main, legacy, branch).await }
        })
        .buffered(BRANCH_DETAIL_READ_CONCURRENCY);
    while let Some(detail) = reads.next().await {
        details.extend(detail?);
    }
    Ok(GraphBranchListing { branches, details })
}

async fn branch_detail(
    root: &str,
    main: &Dataset,
    legacy: &ExtentCache,
    branch: LiveBranch,
) -> Result<Option<GraphBranchDetail>> {
    let LiveBranch {
        name: logical,
        native,
        lifetime,
    } = branch;
    match read_branch(root, main, legacy, &logical, &native, &lifetime).await {
        Ok(TreeRead::NotReady) if lifetime_is_gone(main, &native).await? => Ok(None),
        Ok(tree) => Ok(Some(detail_of(logical, &lifetime, tree))),
        Err(error) => match lifetime_is_gone(main, &native).await {
            Ok(true) => Ok(None),
            _ => Err(error),
        },
    }
}

/// What a read of a live row's tree found.
enum TreeRead {
    Ready(Box<ReadyTree>),
    NotReady,
    RetirementPending,
}

/// A tree with a head: the head, whether the lifetime wrote it, and the
/// commit the lifetime started at.
struct ReadyTree {
    head: GraphLineageRow,
    head_is_own: bool,
    started_at: String,
}

fn detail_of(name: String, lifetime: &BranchLifetime, tree: TreeRead) -> GraphBranchDetail {
    let from = Some(
        lifetime
            .parent
            .as_deref()
            .map(crate::branch_names::logical_branch_name)
            .unwrap_or("main")
            .to_string(),
    );
    let created_at_seconds = Some(lifetime.created_at);
    let (head, head_is_own, from_commit_id, retirement_pending) = match tree {
        TreeRead::Ready(ready) => {
            let ReadyTree {
                head,
                head_is_own,
                started_at,
            } = *ready;
            (Some(head), Some(head_is_own), Some(started_at), false)
        }
        TreeRead::NotReady => (None, None, None, false),
        TreeRead::RetirementPending => (None, None, None, true),
    };
    GraphBranchDetail {
        name,
        head,
        head_is_own,
        from,
        from_commit_id,
        created_at_seconds,
        retirement_pending,
    }
}

/// Whether the lifetime `native` the listing saw live is gone since, which a
/// failed read of it then reports as a branch deleted after the listing:
/// main's latest rows at or above the branch-row stamp, the ref below it.
async fn lifetime_is_gone(main: &Dataset, native: &str) -> Result<bool> {
    if has_branch_rows(main) {
        let latest = main.latest_version_id().await.map_err(OmniError::storage)?;
        let latest = main
            .checkout_version(latest)
            .await
            .map_err(OmniError::storage)?;
        return Ok(crate::BranchRows::decode(latest.manifest())?
            .branch(native)
            .is_none());
    }
    match crate::branch_control::get_branch_identifier(main, native).await {
        Err(lance::Error::RefNotFound { .. }) => Ok(true),
        Err(error) => Err(OmniError::storage(error)),
        Ok(_) => Ok(false),
    }
}

/// The latest head of the lifetime `native`, whether the lifetime wrote it,
/// and the commit the lifetime started at: the head of the version it was
/// created from, which is still its head while it has written none.
async fn read_branch(
    root: &str,
    main: &Dataset,
    legacy: &ExtentCache,
    logical: &str,
    native: &str,
    lifetime: &BranchLifetime,
) -> Result<TreeRead> {
    let branch = match main.checkout_branch(native).await {
        Ok(branch) => branch,
        Err(error) => {
            return match branch_open_error(main, VersionResolution::Latest, error, logical) {
                OmniError::BranchNotReady { .. } => Ok(TreeRead::NotReady),
                error => Err(error),
            };
        }
    };
    if crate::BranchRows::decode(branch.manifest())?
        .tombstone()
        .is_some()
    {
        return Ok(TreeRead::RetirementPending);
    }
    let head = read_manifest_rows_projected(&branch).await?.head;
    if exact_head(&head, Some(native)).is_none() {
        let started_at = head.graph_commit_id.clone();
        return Ok(TreeRead::Ready(Box::new(ReadyTree {
            head,
            head_is_own: false,
            started_at,
        })));
    }
    let created = branch
        .checkout_version(lifetime.parent_version)
        .await
        .map_err(OmniError::storage)?;
    let started_at =
        match crate::legacy::at_version(root, &created.session(), legacy, &created).await? {
            None => read_manifest_rows_projected(&created).await?.head,
            Some(at) => at.served(Some(native), lifetime.parent_version)?.commit,
        };
    Ok(TreeRead::Ready(Box::new(ReadyTree {
        head,
        head_is_own: true,
        started_at: started_at.graph_commit_id,
    })))
}
