//! What a detailed branch listing reports about each live graph branch: its
//! head, the branch and commit its current lifetime started from, and when
//! that lifetime was created. Every fact is read from the branch refs and each
//! branch's own `__manifest`; nothing is stored for the listing.

use std::sync::Arc;

use futures::StreamExt;
use lance::Dataset;
use lance::dataset::refs::BranchContents;

use crate::branch_control::list_live_manifest_branch_contents;
use crate::error::{OmniError, Result};
use crate::history::ExtentCache;
use crate::layout::open_manifest_dataset_with_session;
use crate::state::{GraphLineageRow, exact_head, read_manifest_rows_projected};

/// One live named graph branch: its logical name and the native ref, with
/// that ref's contents, of its current lifetime.
pub(crate) struct LiveBranch {
    pub(crate) name: String,
    pub(crate) native: String,
    pub(crate) contents: BranchContents,
}

/// The live named graph branches of a `__manifest`, sorted by name: each live
/// native ref mapped to its one logical name, `main` excluded. A name with
/// more than one live incarnation fails loudly. The one enumeration both
/// branch listings use, so names and details agree on what is live.
pub(crate) async fn live_named_branches(dataset: &Dataset) -> Result<Vec<LiveBranch>> {
    let mut named: Vec<_> = list_live_manifest_branch_contents(dataset)
        .await?
        .into_iter()
        .filter(|(native, _)| native != "main")
        .map(|(native, contents)| LiveBranch {
            name: crate::branch_names::logical_branch_name(&native).to_string(),
            native,
            contents,
        })
        .collect();
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
    /// Every live branch, `main` first and the rest by name.
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
    pub head: GraphLineageRow,
    /// Whether the branch's current lifetime wrote `head`.
    pub head_is_own: bool,
    /// The logical name of the branch the current lifetime was created from,
    /// as that branch was named then; it may since have been deleted or
    /// recreated. `None` on main.
    pub from: Option<String>,
    /// The head commit of the `__manifest` version the lifetime was created
    /// from: the commit it started at. `None` on main.
    pub from_commit_id: Option<String>,
    /// `BranchContents::create_at` of the lifetime's native ref, in Unix
    /// seconds; `None` on main.
    pub created_at_seconds: Option<u64>,
}

/// Every live graph branch, with details of those `can_read` admits. One open
/// of main's `__manifest` and one listing of its refs; then, for each admitted
/// branch, a checkout of its latest version and one scan of its rows, and when
/// the branch has written a commit, the checkout and scan of the version it was
/// created from. A branch whose ref is gone before its own reads keeps its name
/// and has no details: it was deleted after the listing.
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
            head_is_own: exact_head(&head, None).is_some(),
            head,
            from: None,
            from_commit_id: None,
            created_at_seconds: None,
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
        contents,
    } = branch;
    match read_branch(root, main, legacy, &native, &contents).await {
        Ok((head, head_is_own, from_commit_id)) => {
            let from = contents
                .parent_branch
                .as_deref()
                .map(crate::branch_names::logical_branch_name)
                .unwrap_or("main")
                .to_string();
            Ok(Some(GraphBranchDetail {
                name: logical,
                from: Some(from),
                from_commit_id: Some(from_commit_id),
                head,
                head_is_own,
                created_at_seconds: Some(contents.create_at),
            }))
        }
        // The listing saw this lifetime live, and a native name is never
        // reused. A read that fails once its ref is gone reports a branch
        // deleted since; any other failure is the read's own.
        Err(error) => match crate::branch_control::get_branch_identifier(main, &native).await {
            Err(lance::Error::RefNotFound { .. }) => Ok(None),
            _ => Err(error),
        },
    }
}

/// The latest head of the lifetime `native`, whether the lifetime wrote it,
/// and the commit the lifetime started at: the head of the version it was
/// created from, which is still its head while it has written none.
async fn read_branch(
    root: &str,
    main: &Dataset,
    legacy: &ExtentCache,
    native: &str,
    contents: &BranchContents,
) -> Result<(GraphLineageRow, bool, String)> {
    let branch = main
        .checkout_branch(native)
        .await
        .map_err(OmniError::storage)?;
    let head = read_manifest_rows_projected(&branch).await?.head;
    if exact_head(&head, Some(native)).is_none() {
        let started_at = head.graph_commit_id.clone();
        return Ok((head, false, started_at));
    }
    let created = branch
        .checkout_version(contents.parent_version)
        .await
        .map_err(OmniError::storage)?;
    let started_at =
        match crate::legacy::at_version(root, &created.session(), legacy, &created).await? {
            None => read_manifest_rows_projected(&created).await?.head,
            Some(at) => at.served(Some(native), contents.parent_version)?.commit,
        };
    Ok((head, true, started_at.graph_commit_id))
}
