//! Crash classification for Lance's public native branch controls.
//!
//! Graph retirement archives exact native authority before unlinking its active
//! ref. Native trees remain available until dependency-aware cleanup. Controls remain serialized by
//! OmniGraph's single-writer-process schema, branch, and table gates.

use std::collections::HashMap;
use std::future::Future;

use futures::StreamExt;
use lance::Dataset;
use lance::dataset::refs::{
    BranchContents, BranchIdentifier, branch_contents_path, check_valid_branch,
};
use object_store::{ObjectStoreExt, PutMode, PutOptions};

use crate::error::{CompletionEvidence, OmniError, Result};
use crate::manifest_request::{
    FailedRequest, MANIFEST_REQUEST_REISSUES, classify_failed_request, reissue_pause,
};
use crate::seams::{decide_seam, fail};

/// Result of a recoverable native create attempt.
pub enum BranchCreateOutcome {
    /// The requested branch is durably authoritative and its dataset opens.
    Created,
    /// The target ref existed before this invocation. Callers classify its
    /// ownership at their own logical layer; this helper never deletes it.
    /// A ref that appears after target absence was captured is instead a typed
    /// authority-change error and cannot enter an orphan-reclaim path.
    RefAlreadyExists,
}

const BRANCH_REF_READ_MAX_ATTEMPTS: usize = 3;

/// Retry a read-only branch ref read across Lance's two ref races: a ref
/// deleted between list and read (`NotFound`) and a ref rewritten between
/// `head` and `get_range` (a torn prefix: `Arrow`, or `CorruptFile` mid-char).
async fn read_branch_refs<T, F, Fut>(read: F, raced: fn(&lance::Error) -> bool) -> lance::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = lance::Result<T>>,
{
    for attempt in 1..BRANCH_REF_READ_MAX_ATTEMPTS {
        match read().await {
            Err(error) if raced(&error) => {
                tracing::debug!(attempt, %error, "branch ref read raced a concurrent ref change; rereading");
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
    read().await
}

fn is_torn_ref_read(error: &lance::Error) -> bool {
    matches!(
        error,
        lance::Error::Arrow { .. } | lance::Error::CorruptFile { .. }
    )
}

fn is_raced_ref_read(error: &lance::Error) -> bool {
    matches!(error, lance::Error::NotFound { .. }) || is_torn_ref_read(error)
}

/// List Lance's authoritative named-branch refs through the bounded read-only
/// retry boundary. Relisting is safe: it has no durable effect and does not
/// replay the enclosing graph operation; churn beyond the bound keeps the error.
pub async fn list_branch_contents(dataset: &Dataset) -> Result<HashMap<String, BranchContents>> {
    read_branch_refs(|| dataset.list_branches(), is_raced_ref_read)
        .await
        .map_err(OmniError::storage)
}

/// Read one physical ref through the same bounded retry boundary; the caller
/// maps `RefNotFound` to its own public miss.
pub async fn get_branch_contents(dataset: &Dataset, branch: &str) -> lance::Result<BranchContents> {
    read_branch_refs(
        || async move { dataset.branches().get(branch).await },
        is_raced_ref_read,
    )
    .await
}

/// Read one physical ref's exact lifetime identity; same boundary and
/// `RefNotFound` contract as `get_branch_contents`.
pub async fn get_branch_identifier(
    dataset: &Dataset,
    native: &str,
) -> lance::Result<BranchIdentifier> {
    read_branch_refs(
        || async move { dataset.branches().get_identifier(Some(native)).await },
        is_raced_ref_read,
    )
    .await
}

/// The identity of the ref a dataset handle is checked out on, through the
/// same boundary; every engine read of `Dataset::branch_identifier` goes here.
pub async fn dataset_branch_identifier(dataset: &Dataset) -> lance::Result<BranchIdentifier> {
    read_branch_refs(|| dataset.branch_identifier(), is_raced_ref_read).await
}

/// Resolve a logical branch to its single live native ref by reading only that
/// branch's own incarnations: one listing of `_refs/branches/`, then Lance's own
/// listing read per candidate; a sibling's rewrite or a reclaimed candidate cannot fail it.
pub async fn resolve_live_native_branch(
    dataset: &Dataset,
    logical: &str,
) -> Result<Option<String>> {
    let live = live_manifest_branches_matching(dataset, |candidate| candidate == logical).await?;
    crate::branch_names::resolve_native_branch(live.iter().map(String::as_str), logical)
}

async fn live_manifest_branches_matching(
    dataset: &Dataset,
    matches: impl Fn(&str) -> bool,
) -> Result<Vec<String>> {
    let root = dataset
        .branch_location()
        .find_main()
        .map_err(OmniError::storage)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let mut live = Vec::new();
    for native in list_branch_ref_names(&store, &root.path).await? {
        if !matches(crate::branch_names::logical_branch_name(&native)) {
            continue;
        }
        let path = lance::dataset::refs::branch_contents_path(&root.path, &native);
        let (path, store, name) = (&path, &store, native.as_str());
        let read = || async move { BranchContents::from_path(path, store, name).await };
        match read_branch_refs(read, is_torn_ref_read).await {
            Ok(contents) => {
                if manifest_branch_is_live(&native, &contents)? {
                    live.push(native);
                }
            }
            Err(lance::Error::NotFound { .. }) => {}
            Err(error) => return Err(OmniError::storage(error)),
        }
    }
    Ok(live)
}

/// Validate the source and the target's equal, ancestor and descendant names.
pub async fn ensure_manifest_branch_create_namespace(
    dataset: &Dataset,
    target: &str,
) -> Result<()> {
    let target_prefix = format!("{target}/");
    let source = dataset
        .manifest()
        .branch
        .as_deref()
        .map(crate::branch_names::logical_branch_name);
    let overlaps_target = |candidate: &str| {
        candidate == target
            || (candidate != "main"
                && (candidate.starts_with(&target_prefix)
                    || target.starts_with(&format!("{candidate}/"))))
    };
    let mut live = live_manifest_branches_matching(dataset, |candidate| {
        Some(candidate) == source || overlaps_target(candidate)
    })
    .await?;
    live.sort();
    let mut names = std::collections::BTreeSet::new();
    for native in &live {
        let logical = crate::branch_names::logical_branch_name(native);
        if !names.insert(logical) {
            return Err(OmniError::manifest_conflict(format!(
                "branch '{logical}' has more than one live native incarnation; run cleanup \
                 before using it"
            )));
        }
    }
    if names.contains(target) {
        return Err(OmniError::manifest_conflict(format!(
            "branch '{target}' already exists"
        )));
    }
    if let Some(conflicting) = names.into_iter().find(|name| overlaps_target(name)) {
        return Err(OmniError::manifest_conflict(format!(
            "cannot create branch '{target}' while live branch '{conflicting}' shares its \
             physical Lance path; live graph branch names may not be ancestors or descendants"
        )));
    }
    Ok(())
}

async fn branch_ref_names(dataset: &Dataset) -> Result<Vec<String>> {
    let root = dataset
        .branch_location()
        .find_main()
        .map_err(OmniError::storage)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    list_branch_ref_names(&store, &root.path).await
}

async fn branch_tree_exists(dataset: &Dataset, branch: &str) -> Result<bool> {
    let target = dataset
        .branch_location()
        .find_branch(Some(branch))
        .map_err(OmniError::storage)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    Ok(store
        .read_dir_all(&target.path, None)
        .next()
        .await
        .transpose()
        .map_err(OmniError::storage)?
        .is_some())
}

/// Every native ref name under `_refs/branches/`, decoded the way Lance's own
/// listing decodes them; no ref is read.
async fn list_branch_ref_names(
    store: &lance::io::ObjectStore,
    root: &object_store::path::Path,
) -> Result<Vec<String>> {
    let directory = lance::dataset::refs::base_branches_contents_path(root);
    let files = store
        .read_dir(directory)
        .await
        .map_err(OmniError::storage)?;
    files
        .iter()
        .filter_map(|file| file.strip_suffix(".json"))
        .map(|encoded| {
            object_store::path::Path::from_url_path(encoded)
                .map(|path| path.to_string())
                .map_err(|error| {
                    OmniError::storage(lance::Error::InvalidRef {
                        message: format!("branch ref name '{encoded}' does not decode: {error}"),
                    })
                })
        })
        .collect()
}

/// Enumerate physical native lifetimes, including retired graph manifests.
pub async fn list_all_branch_contents(
    dataset: &Dataset,
) -> Result<HashMap<String, BranchContents>> {
    list_branch_contents(dataset).await
}

const RETIRED_MANIFEST_BRANCH_KEY: &str = "omnigraph.retired_manifest_branch";
pub const RETIRED_BRANCH_ARCHIVE: &str = "_omnigraph_retired_branch.json";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RetiredManifestBranch {
    version: u32,
    native_branch: String,
    identifier: BranchIdentifier,
}

pub fn manifest_branch_is_live(branch: &str, contents: &BranchContents) -> Result<bool> {
    let Some(value) = contents.metadata.get(RETIRED_MANIFEST_BRANCH_KEY) else {
        return Ok(true);
    };
    let record: RetiredManifestBranch = serde_json::from_str(value).map_err(|error| {
        OmniError::manifest_conflict(format!(
            "native manifest branch '{branch}' has invalid retirement metadata: {error}"
        ))
    })?;
    if record.version != 1
        || branch == "main"
        || record.native_branch != branch
        || record.identifier != contents.identifier
        || record.identifier == BranchIdentifier::main()
        || record.identifier == BranchIdentifier::missing_identifier_sentinel()
    {
        return Err(OmniError::manifest_conflict(format!(
            "native manifest branch '{branch}' has unsupported or mismatched retirement metadata"
        )));
    }
    Ok(false)
}

/// Validate all physical refs before selecting live graph incarnations.
pub async fn list_live_manifest_branch_contents(
    dataset: &Dataset,
) -> Result<HashMap<String, BranchContents>> {
    let mut live = HashMap::new();
    for (branch, contents) in list_branch_contents(dataset).await? {
        if manifest_branch_is_live(&branch, &contents)? {
            live.insert(branch, contents);
        }
    }
    Ok(live)
}

/// Retired refs left by older writers or an interrupted archive publication.
pub async fn list_retired_manifest_branch_contents(
    dataset: &Dataset,
) -> Result<HashMap<String, BranchContents>> {
    let mut retired = HashMap::new();
    for (branch, contents) in list_branch_contents(dataset).await? {
        if !manifest_branch_is_live(&branch, &contents)? {
            retired.insert(branch, contents);
        }
    }
    Ok(retired)
}

fn retirement_archive_path(dataset: &Dataset, branch: &str) -> Result<object_store::path::Path> {
    Ok(dataset
        .branch_location()
        .find_branch(Some(branch))
        .map_err(OmniError::storage)?
        .path
        .join(RETIRED_BRANCH_ARCHIVE))
}

/// `retirement_archive_path` for the engine's branch-control tests, which
/// corrupt the archive in place; compiled only under `test` or `test-util`.
#[cfg(any(test, feature = "test-util"))]
pub fn retirement_archive_path_for_test(
    dataset: &Dataset,
    branch: &str,
) -> Result<object_store::path::Path> {
    retirement_archive_path(dataset, branch)
}

/// Read immutable retirement history without restoring live branch authority.
pub async fn archived_manifest_branch(
    dataset: &Dataset,
    branch: &str,
) -> Result<Option<BranchContents>> {
    check_valid_branch(branch).map_err(OmniError::storage)?;
    let path = retirement_archive_path(dataset, branch)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let bytes = match store.inner.get(&path).await {
        Ok(object) => object
            .bytes()
            .await
            .map_err(|error| OmniError::storage(error.into()))?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(OmniError::storage(error.into())),
    };
    let contents: BranchContents = serde_json::from_slice(&bytes).map_err(|error| {
        OmniError::manifest_conflict(format!(
            "invalid retirement archive for '{branch}': {error}"
        ))
    })?;
    if manifest_branch_is_live(branch, &contents)? {
        return Err(OmniError::manifest_conflict(format!(
            "retirement archive for '{branch}' lacks exact retirement metadata"
        )));
    }
    Ok(Some(contents))
}

/// Enumerate immutable histories only during maintenance and historical lookup.
pub async fn list_archived_manifest_branches(
    dataset: &Dataset,
) -> Result<HashMap<String, BranchContents>> {
    let root = dataset
        .branch_location()
        .find_main()
        .map_err(OmniError::storage)?
        .path
        .join("tree");
    let prefix = format!("{root}/");
    let suffix = format!("/{RETIRED_BRANCH_ARCHIVE}");
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let mut files = store.read_dir_all(&root, None);
    let mut archives = HashMap::new();
    while let Some(file) = files.next().await {
        let file = file.map_err(OmniError::storage)?;
        if file.location.filename() != Some(RETIRED_BRANCH_ARCHIVE) {
            continue;
        }
        let branch = file
            .location
            .as_ref()
            .strip_prefix(&prefix)
            .and_then(|relative| relative.strip_suffix(&suffix))
            .ok_or_else(|| {
                OmniError::manifest_conflict("retirement archive outside native tree")
            })?;
        let contents = archived_manifest_branch(dataset, branch)
            .await?
            .ok_or_else(|| {
                OmniError::manifest_conflict(format!(
                    "retirement archive for '{branch}' disappeared during inventory"
                ))
            })?;
        archives.insert(branch.to_string(), contents);
    }
    Ok(archives)
}

fn same_branch_contents(left: &BranchContents, right: &BranchContents) -> bool {
    left.identifier == right.identifier
        && left.parent_branch == right.parent_branch
        && left.parent_version == right.parent_version
        && left.create_at == right.create_at
        && left.manifest_size == right.manifest_size
        && left.metadata == right.metadata
}

/// Archive before unlinking: a crash leaves either retired authority or its
/// immutable history, and never makes a retired native path reusable.
async fn archive_retired_ref(
    dataset: &Dataset,
    branch: &str,
    contents: &BranchContents,
) -> Result<()> {
    if manifest_branch_is_live(branch, contents)? {
        return Err(OmniError::manifest_conflict(format!(
            "cannot archive live branch '{branch}'"
        )));
    }
    let path = retirement_archive_path(dataset, branch)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let bytes = serde_json::to_vec(contents).map_err(|error| {
        OmniError::manifest_internal(format!("failed to encode retirement archive: {error}"))
    })?;
    if let Err(error) = store
        .inner
        .put_opts(
            &path,
            bytes.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        let Some(existing) = archived_manifest_branch(dataset, branch).await? else {
            return Err(OmniError::storage(error.into()));
        };
        if !same_branch_contents(contents, &existing) {
            return Err(OmniError::manifest_conflict(format!(
                "retirement archive for '{branch}' changed"
            )));
        }
    }
    fail(&BRANCH_DELETE_POST_ARCHIVE)?;
    match get_branch_contents(dataset, branch).await {
        Ok(observed) if same_branch_contents(&observed, contents) => {}
        Ok(_) => {
            return Err(OmniError::manifest_conflict(format!(
                "branch '{branch}' changed before ref unlink"
            )));
        }
        Err(lance::Error::RefNotFound { .. }) => return Ok(()),
        Err(error) => return Err(OmniError::storage(error)),
    }
    let root = dataset
        .branch_location()
        .find_main()
        .map_err(OmniError::storage)?
        .path;
    match store
        .inner
        .delete(&branch_contents_path(&root, branch))
        .await
    {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => match get_branch_contents(dataset, branch).await {
            Err(lance::Error::RefNotFound { .. }) => Ok(()),
            Ok(_) => Err(OmniError::storage(error.into())),
            Err(read_error) => Err(OmniError::storage(read_error)),
        },
    }
}

/// Finish retirement records written by an older writer or before a crash.
pub async fn archive_retired_manifest_branches(dataset: &Dataset) -> Result<()> {
    let mut retired = list_retired_manifest_branch_contents(dataset)
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    retired.sort_by(|left, right| left.0.cmp(&right.0));
    for (branch, contents) in retired {
        archive_retired_ref(dataset, &branch, &contents).await?;
    }
    Ok(())
}

/// Read exact native authority and refuse retired or malformed graph refs.
pub async fn get_live_manifest_branch_contents(
    dataset: &Dataset,
    branch: &str,
) -> Result<BranchContents> {
    let not_found = || OmniError::BranchNotFound {
        branch: crate::branch_names::logical_branch_name(branch).to_string(),
    };
    let contents = match get_branch_contents(dataset, branch).await {
        Ok(contents) => contents,
        Err(lance::Error::RefNotFound { .. }) => {
            return Err(not_found());
        }
        Err(error) => return Err(OmniError::storage(error)),
    };
    if !manifest_branch_is_live(branch, &contents)? {
        return Err(not_found());
    }
    Ok(contents)
}

decide_seam! {
    /// After native retirement succeeds, before OmniGraph acknowledges it.
    /// A lost acknowledgement is classified through the exact retirement marker.
    pub static BRANCH_DELETE_POST_NATIVE = ("branch_delete.post_native", BranchDelete, [Fail]);
}

decide_seam! {
    /// After immutable retirement history is durable, before unlinking its ref.
    pub static BRANCH_DELETE_POST_ARCHIVE = ("branch_delete.post_archive", BranchDelete, [Fail]);
}

/// Revoke logical authority, archive its exact history, then unlink only the ref.
/// Callers hold schema, branch, and table gates in one writer process.
/// The metadata replacement is one-object publication, without a native CAS.
pub async fn retire_branch_recoverably(
    dataset: &Dataset,
    branch: &str,
    expected_identifier: &BranchIdentifier,
) -> Result<()> {
    if branch == "main" {
        return Err(OmniError::manifest_conflict("cannot retire branch 'main'"));
    }
    let mut contents = match get_branch_contents(dataset, branch).await {
        Ok(contents) => contents,
        Err(lance::Error::RefNotFound { .. }) => {
            let archive = archived_manifest_branch(dataset, branch)
                .await
                .map_err(OmniError::before_effect)?;
            return match archive {
                Some(archive) if archive.identifier == *expected_identifier => Ok(()),
                _ => Err(OmniError::manifest_conflict(format!(
                    "branch '{branch}' has no matching retirement archive"
                ))),
            };
        }
        Err(error) => return Err(OmniError::storage(error).before_effect()),
    };
    if contents.identifier != *expected_identifier {
        return Err(OmniError::manifest_conflict(format!(
            "branch '{branch}' changed before retirement"
        )));
    }
    if !manifest_branch_is_live(branch, &contents).map_err(OmniError::before_effect)? {
        return archive_retired_ref(dataset, branch, &contents)
            .await
            .map_err(|error| error.with_completion_evidence(CompletionEvidence::Uncertain));
    }
    if expected_identifier == &BranchIdentifier::main()
        || expected_identifier == &BranchIdentifier::missing_identifier_sentinel()
    {
        return Err(OmniError::manifest_conflict(format!(
            "branch '{branch}' has no exact retirement identity"
        )));
    }
    let tags = dataset
        .tags()
        .list()
        .await
        .map_err(|error| OmniError::storage(error).before_effect())?;
    if let Some((tag, _)) = tags.iter().find(|(name, tag)| {
        tag.branch.as_deref() == Some(branch)
            && !crate::branch_names::is_merge_input_tag(name)
            && !crate::branch_names::is_unpublished_tag(name)
            && !crate::branch_names::is_unattached_tag(name)
    }) {
        return Err(OmniError::storage(lance::Error::RefConflict {
            message: format!(
                "cannot retire graph branch '{branch}' while native manifest tag '{tag}' pins it; remove the tag first"
            ),
        }).before_effect());
    }
    let record = RetiredManifestBranch {
        version: 1,
        native_branch: branch.to_string(),
        identifier: expected_identifier.clone(),
    };
    let value = serde_json::to_string(&record).map_err(|error| {
        OmniError::manifest_internal(format!("failed to encode branch retirement: {error}"))
            .before_effect()
    })?;
    contents
        .metadata
        .insert(RETIRED_MANIFEST_BRANCH_KEY.to_string(), value);
    // From submission onward, a refusal-shaped cause cannot establish that
    // retirement and its required archive/unlink completion made no effects.
    let completion = async {
        let result = match dataset
            .branches()
            .replace_metadata(branch, contents.metadata.clone())
            .await
        {
            Ok(()) => fail(&BRANCH_DELETE_POST_NATIVE),
            Err(error) => Err(OmniError::storage(error)),
        };
        if let Err(error) = result {
            let observed = get_branch_contents(dataset, branch)
                .await
                .map_err(OmniError::storage)?;
            if observed.identifier != *expected_identifier {
                return Err(OmniError::manifest_conflict(format!(
                    "branch '{branch}' changed during retirement"
                )));
            }
            if manifest_branch_is_live(branch, &observed)? {
                return Err(error);
            }
            contents = observed;
        }
        archive_retired_ref(dataset, branch, &contents).await
    }
    .await;
    completion.map_err(|error| error.with_completion_evidence(CompletionEvidence::Uncertain))
}

/// Pinned Lance treats an already-absent target tree as success. OmniGraph
/// still normalizes `RefNotFound` / `NotFound` around the whole call because
/// Lance's branch-contents existence check and delete are separate operations,
/// so a concurrent cleanup can win between them. The live path-descendant
/// precheck remains ours because Lance deliberately retains an ancestor tree
/// while a child branch shares that physical path.
pub async fn force_delete_branch_idempotent(dataset: &mut Dataset, branch: &str) -> Result<()> {
    let branches = list_branch_contents(dataset).await?;
    if let Some(child) = path_descendant(&branches, branch) {
        return Err(OmniError::manifest_conflict(format!(
            "cannot reclaim branch '{branch}' while live branch '{child}' shares its physical \
             Lance path; delete the child branch first"
        )));
    }
    force_delete_branch_tree_unchecked(dataset, branch).await
}

async fn force_delete_branch_tree_unchecked(dataset: &mut Dataset, branch: &str) -> Result<()> {
    match dataset.force_delete_branch(branch).await {
        Ok(()) | Err(lance::Error::RefNotFound { .. }) | Err(lance::Error::NotFound { .. }) => {
            Ok(())
        }
        Err(error) => Err(OmniError::storage(error)),
    }
}

async fn branch_contents(dataset: &Dataset, branch: &str) -> Result<Option<BranchContents>> {
    match get_branch_contents(dataset, branch).await {
        Ok(contents) => Ok(Some(contents)),
        Err(lance::Error::RefNotFound { .. }) => Ok(None),
        Err(error) => Err(OmniError::storage(error)),
    }
}

pub fn path_descendant<'a>(
    branches: &'a HashMap<String, BranchContents>,
    branch: &str,
) -> Option<&'a str> {
    let prefix = format!("{branch}/");
    branches
        .keys()
        .map(String::as_str)
        .filter(|candidate| candidate.starts_with(&prefix))
        .min()
}

fn path_collision<'a>(branches: &'a [String], branch: &str) -> Option<&'a str> {
    let branch_prefix = format!("{branch}/");
    branches
        .iter()
        .map(String::as_str)
        .filter(|candidate| {
            *candidate != branch
                && *candidate != "main"
                && (candidate.starts_with(&branch_prefix)
                    || branch.starts_with(&format!("{candidate}/")))
        })
        .min()
}

fn encode_identifier(identifier: &BranchIdentifier) -> Result<String> {
    serde_json::to_string(identifier).map_err(|error| {
        OmniError::manifest_internal(format!("failed to encode Lance branch identifier: {error}"))
    })
}

async fn authority_appeared_after_absence(dataset: &Dataset, branch: &str) -> Result<OmniError> {
    match branch_contents(dataset, branch).await? {
        Some(contents) => Ok(OmniError::manifest_read_set_changed(
            format!("branch_identifier:{branch}"),
            None,
            Some(encode_identifier(&contents.identifier)?),
        )),
        None => Ok(OmniError::manifest_conflict(format!(
            "branch authority for '{branch}' changed during absent-only reclaim; refusing \
             destructive cleanup"
        ))),
    }
}

/// Reclaim only when the authoritative ref is freshly absent. Returns `false`
/// if a ref exists and therefore nothing was touched. A live path-child is a
/// conflict because Lance deliberately keeps the ancestor directory in that
/// state; the graph namespace prevents new instances of that overlap.
pub async fn reclaim_ref_absent_tree(dataset: &mut Dataset, branch: &str) -> Result<bool> {
    let branches = list_branch_contents(dataset).await?;
    if branches.contains_key(branch) {
        return Ok(false);
    }
    match dataset.branches().get(branch).await {
        Ok(_) => return Ok(false),
        Err(lance::Error::RefNotFound { .. }) => {}
        Err(error) => return Err(OmniError::storage(error)),
    }
    if let Some(child) = path_descendant(&branches, branch) {
        return Err(OmniError::manifest_conflict(format!(
            "cannot reclaim absent branch '{branch}' while live branch '{child}' shares its \
             physical Lance path; delete the child branch first"
        )));
    }
    let target = dataset
        .branch_location()
        .find_branch(Some(branch))
        .map_err(OmniError::storage)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let mut files = store.read_dir_all(&target.path, None);
    while let Some(file) = files.next().await {
        if file.map_err(OmniError::storage)?.location.filename() == Some(RETIRED_BRANCH_ARCHIVE) {
            return Err(OmniError::manifest_conflict(format!(
                "cannot reuse branch '{branch}' while retired native history remains"
            )));
        }
    }
    // This is the final authority read before the destructive call. The
    // supported single-writer-process gate boundary prevents a local create
    // from interleaving after it; an exact ref observed here is never passed to
    // Lance's broad force-delete primitive.
    force_delete_branch_tree_unchecked(dataset, branch).await?;
    Ok(true)
}

/// A completed create receives an identifier equal to the parent's complete
/// identifier mapping plus one newly-generated `(parent_version, uuid)` entry.
/// The UUID itself cannot be pre-minted through Lance's public API, so this is
/// the strongest completion proof available inside the supported
/// single-writer-process boundary.
fn matches_create_expectation(
    contents: &BranchContents,
    parent_branch: Option<&str>,
    parent_version: u64,
    parent_identifier: &BranchIdentifier,
) -> bool {
    let expected_parent = parent_branch.filter(|branch| *branch != "main");
    let mapping = &contents.identifier.version_mapping;
    let parent_mapping = &parent_identifier.version_mapping;
    contents.parent_branch.as_deref() == expected_parent
        && contents.parent_version == parent_version
        && mapping.len() == parent_mapping.len() + 1
        && mapping.starts_with(parent_mapping)
        && mapping
            .last()
            .is_some_and(|(version, uuid)| *version == parent_version && !uuid.is_empty())
}

decide_seam! {
    /// After Lance returns success from its two-phase native create, before
    /// OmniGraph acknowledges it. Recovery must classify the matching
    /// BranchContents as a completed create (lost acknowledgement).
    pub static BRANCH_CREATE_POST_NATIVE = ("branch_create.post_native", BranchCreate, [Fail]);
}

decide_seam! {
    /// After the namespace inventory (refs listed, path collision checked,
    /// a ref-less tree reclaimed), before the native create. No CAS covers
    /// this window: the caller's exclusive schema permit is what keeps a
    /// sibling create from passing its own inventory meanwhile.
    pub static BRANCH_CREATE_POST_INVENTORY_PRE_NATIVE = ("branch_create.post_inventory_pre_native", BranchCreate, [Fail]);
}

/// Archived legacy ancestors still own their native path, even without refs.
/// Flat generated names need no archive probe; slash names cost one per ancestor.
async fn refuse_archived_path_ancestor(dataset: &Dataset, branch: &str) -> Result<()> {
    for (end, _) in branch.match_indices('/') {
        let ancestor = &branch[..end];
        let path = retirement_archive_path(dataset, ancestor)?;
        let store = dataset
            .object_store(None)
            .await
            .map_err(OmniError::storage)?;
        match store.inner.head(&path).await {
            Ok(_) => {
                return Err(OmniError::manifest_conflict(format!(
                    "cannot create branch '{branch}' while retired branch '{ancestor}' shares its physical Lance path"
                )));
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(OmniError::storage(error.into())),
        }
    }
    Ok(())
}

/// Create a branch in one native attempt, classified from fresh authority.
///
/// The caller must already hold OmniGraph's schema → source/target branch →
/// table gate envelope. A ref-absent tree without retirement history is derived
/// garbage and is reclaimed before the attempt. Lance commits the clone, then
/// writes the ref put-if-absent, and its ref error says nothing about the
/// object, so a failed attempt reads the ref back:
///
/// - matching contents are this attempt's, a lost acknowledgement;
/// - mismatching contents are another's, never deleted;
/// - an absent ref that may still land, because the clone committed and the
///   failure was not a definite refusal, is read again with the pauses of a
///   conditional re-issue, then stays in doubt: the clone it would name is
///   kept, and the error is an in-doubt publication;
/// - an absent ref that cannot land leaves a ref-less clone, which is
///   reclaimed, and the error is `Unpublished`. Only then may a caller retry,
///   under a fresh native name (`Catalog::create_branch_from` does).
pub async fn create_branch_recoverably(
    source: &mut Dataset,
    branch: &str,
    source_version: u64,
    binding: Option<&crate::storage::CapturedStorageBinding>,
) -> Result<BranchCreateOutcome> {
    // Lance validates inside phase 2 today. Validate before phase 1 so an
    // invalid name cannot leave a clone-only zombie. This validation and the
    // inventory below precede even orphan-tree reclamation, our first possible
    // durable effect; native create errors do not carry this proof.
    check_valid_branch(branch)
        .map_err(|error| OmniError::manifest(error.to_string()).before_effect())?;
    if source.version().version != source_version {
        return Err(OmniError::manifest_conflict(format!(
            "branch source moved before native create: expected version {}, current {}",
            source_version,
            source.version().version
        )));
    }

    let parent_branch = source.manifest().branch.clone();
    let parent_identifier = dataset_branch_identifier(source)
        .await
        .map_err(|error| OmniError::storage(error).before_effect())?;

    let initial_branches = branch_ref_names(source)
        .await
        .map_err(OmniError::before_effect)?;
    if initial_branches.iter().any(|name| name == branch)
        && branch_contents(source, branch)
            .await
            .map_err(OmniError::before_effect)?
            .is_some()
    {
        return Ok(BranchCreateOutcome::RefAlreadyExists);
    }
    if let Some(conflicting) = path_collision(&initial_branches, branch) {
        return Err(OmniError::manifest_conflict(format!(
            "cannot create branch '{branch}' while live branch '{conflicting}' shares its \
             physical Lance path; live graph branch names may not be ancestors or descendants"
        )));
    }
    refuse_archived_path_ancestor(source, branch)
        .await
        .map_err(OmniError::before_effect)?;
    if branch_contents(source, branch)
        .await
        .map_err(OmniError::before_effect)?
        .is_some()
    {
        return Err(authority_appeared_after_absence(source, branch)
            .await
            .map_err(OmniError::before_effect)?);
    }
    let tree_exists = branch_tree_exists(source, branch)
        .await
        .map_err(OmniError::before_effect)?;
    // Reclamation can delete durable files. Nothing from this boundary or a
    // later native attempt is certified as a pre-effect failure.
    if tree_exists && !reclaim_ref_absent_tree(source, branch).await? {
        return Err(authority_appeared_after_absence(source, branch).await?);
    }
    fail(&BRANCH_CREATE_POST_INVENTORY_PRE_NATIVE)?;

    let (native_error, refused) =
        match crate::lance_clone::create_branch(source, branch, source_version, binding).await {
            Ok(_) => match fail(&BRANCH_CREATE_POST_NATIVE) {
                Ok(()) => return Ok(BranchCreateOutcome::Created),
                Err(error) => (error, false),
            },
            Err(error) => {
                let refused = classify_failed_request(&error) == FailedRequest::Refused;
                (OmniError::storage(error), refused)
            }
        };

    let read_back = |classifier_error: OmniError| {
        OmniError::manifest_internal(format!(
            "native create of branch '{branch}' returned an ambiguous error ({native_error}); \
             reading BranchContents to classify it also failed ({classifier_error})"
        ))
    };
    let mut observed = branch_contents(source, branch).await.map_err(read_back)?;
    // Lance writes the ref only after its clone commit returned, and that
    // commit's manifest is durable when it returns: a clone with no version
    // proves the ref put was never sent. Otherwise the put may still be in
    // flight (S3 answers 409 over it), so read again before calling it lost.
    let may_land = observed.is_none()
        && !refused
        && branch_tree_has_version(source, branch)
            .await
            .map_err(read_back)?;
    if may_land {
        for reread in 1..=MANIFEST_REQUEST_REISSUES {
            tokio::time::sleep(reissue_pause(reread)).await;
            observed = branch_contents(source, branch).await.map_err(read_back)?;
            if observed.is_some() {
                break;
            }
        }
        if observed.is_none() {
            return Err(OmniError::manifest_publish_in_doubt(format!(
                "native create of branch '{branch}' committed its clone and may have sent its \
                 ref, which is still absent ({native_error}); the ref may land later, so the \
                 clone it would name is kept"
            )));
        }
    }
    if let Some(contents) = observed {
        if !matches_create_expectation(
            &contents,
            parent_branch.as_deref(),
            source_version,
            &parent_identifier,
        ) {
            return Err(OmniError::manifest_read_set_changed(
                format!("branch_identifier:{branch}"),
                None,
                Some(encode_identifier(&contents.identifier)?),
            ));
        }
        source.checkout_branch(branch).await.map_err(|error| {
            OmniError::manifest_internal(format!(
                "branch '{}' has matching authoritative metadata after an ambiguous create, \
                 but its branch dataset cannot be opened: {}; original native error: {}",
                branch, error, native_error
            ))
        })?;
        return Ok(BranchCreateOutcome::Created);
    }

    // No copy of the ref can land: the clone it would name is garbage.
    match reclaim_ref_absent_tree(source, branch).await {
        Ok(true) => Err(native_error.unpublished()),
        Ok(false) => Err(authority_appeared_after_absence(source, branch).await?),
        Err(cleanup_error) => Err(OmniError::manifest_internal(format!(
            "native create of branch '{}' failed before its ref could be sent \
             ({native_error}); clone-only cleanup also failed ({cleanup_error})",
            branch
        ))
        .unpublished()),
    }
}

/// Whether the native tree of `branch` holds a version manifest: Lance's
/// clone commit, the first phase of a native create, landed.
async fn branch_tree_has_version(dataset: &Dataset, branch: &str) -> Result<bool> {
    let target = dataset
        .branch_location()
        .find_branch(Some(branch))
        .map_err(OmniError::storage)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let mut versions = store.read_dir_all(&target.path.join("_versions"), None);
    while let Some(file) = versions.next().await {
        let file = file.map_err(OmniError::storage)?;
        if file.location.extension() == Some("manifest") {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use lance::dataset::{WriteMode, WriteParams};
    use lance::io::WrappingObjectStore;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as ObjectStoreResult,
    };

    fn is_branch_ref_directory(path: &Path) -> bool {
        path.filename() == Some("branches")
            && path
                .parent()
                .is_some_and(|parent| parent.filename() == Some("_refs"))
    }

    fn is_target_branch_ref(path: &Path, target_file: &str) -> bool {
        path.filename() == Some(target_file)
            && path
                .parent()
                .is_some_and(|parent| is_branch_ref_directory(&parent))
    }

    /// What one injected read of the target ref observes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum InjectedRefRead {
        /// The ref is deleted first, so the real read answers `NotFound`: the
        /// delete race.
        DeleteThenRead,
        /// A synthetic `NotFound`; the ref itself stays intact.
        NotFound,
        /// `head` reports two bytes less than the object holds, so the
        /// following `get_range` returns a prefix: the rewrite race. The
        /// injection counts pin Lance 11's `head` per read; they move at the bump.
        StaleHeadSize,
        /// `head` reports a size that cuts the object inside its first
        /// multi-byte character, so the prefix is not UTF-8 at all.
        StaleHeadSizeMidChar,
    }

    impl InjectedRefRead {
        fn injects_on_head_only(self) -> bool {
            matches!(self, Self::StaleHeadSize | Self::StaleHeadSizeMidChar)
        }
    }

    #[derive(Debug, Clone)]
    struct VanishingBranchRefFault {
        remaining_failures: Arc<AtomicUsize>,
        injected_failures: Arc<AtomicUsize>,
        branch_lists: Arc<AtomicUsize>,
        read: InjectedRefRead,
        target_file: Arc<str>,
    }

    impl VanishingBranchRefFault {
        fn delete_once(target_file: &str) -> Self {
            Self::new(target_file, 1, InjectedRefRead::DeleteThenRead)
        }

        fn always_not_found(target_file: &str) -> Self {
            Self::new(target_file, usize::MAX, InjectedRefRead::NotFound)
        }

        fn stale_head_size_once(target_file: &str) -> Self {
            Self::new(target_file, 1, InjectedRefRead::StaleHeadSize)
        }

        /// `Branches::get` probes the ref with `exists` (one `head`) before
        /// `from_path`'s own `head`; the second stale answer is the torn read.
        fn stale_head_size_twice(target_file: &str) -> Self {
            Self::new(target_file, 2, InjectedRefRead::StaleHeadSize)
        }

        fn stale_head_size_mid_char_once(target_file: &str) -> Self {
            Self::new(target_file, 1, InjectedRefRead::StaleHeadSizeMidChar)
        }

        fn new(target_file: &str, failures: usize, read: InjectedRefRead) -> Self {
            Self {
                remaining_failures: Arc::new(AtomicUsize::new(failures)),
                injected_failures: Arc::new(AtomicUsize::new(0)),
                branch_lists: Arc::new(AtomicUsize::new(0)),
                read,
                target_file: Arc::from(target_file),
            }
        }

        fn injected_failures(&self) -> usize {
            self.injected_failures.load(Ordering::SeqCst)
        }

        fn branch_lists(&self) -> usize {
            self.branch_lists.load(Ordering::SeqCst)
        }
    }

    impl WrappingObjectStore for VanishingBranchRefFault {
        fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
            Arc::new(VanishingBranchRefStore {
                target,
                fault: self.clone(),
            })
        }

        fn wrap_paginated(
            &self,
            _store_prefix: &str,
            _original: Arc<dyn object_store::list::PaginatedListStore>,
        ) -> Option<Arc<dyn object_store::list::PaginatedListStore>> {
            // Listings must pass through the fault store and its accounting.
            None
        }
    }

    #[derive(Debug)]
    struct VanishingBranchRefStore {
        target: Arc<dyn ObjectStore>,
        fault: VanishingBranchRefFault,
    }

    impl fmt::Display for VanishingBranchRefStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "VanishingBranchRefStore({})", self.target)
        }
    }

    #[async_trait]
    impl ObjectStore for VanishingBranchRefStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.target.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.target.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            let targets_ref = is_target_branch_ref(location, &self.fault.target_file)
                && (options.head || !self.fault.read.injects_on_head_only());
            let injected = targets_ref
                && self
                    .fault
                    .remaining_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok();
            if injected {
                self.fault.injected_failures.fetch_add(1, Ordering::SeqCst);
                match self.fault.read {
                    InjectedRefRead::DeleteThenRead => {
                        self.target.delete(location).await?;
                    }
                    InjectedRefRead::NotFound => {
                        return Err(object_store::Error::NotFound {
                            path: location.to_string(),
                            source: Box::new(std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                "injected branch-ref deletion race",
                            )),
                        });
                    }
                    InjectedRefRead::StaleHeadSize => {
                        let mut result = self.target.get_opts(location, options).await?;
                        result.meta.size -= 2;
                        return Ok(result);
                    }
                    InjectedRefRead::StaleHeadSizeMidChar => {
                        let mut result = self.target.get_opts(location, options).await?;
                        let bytes = self.target.get(location).await?.bytes().await?;
                        let first_multibyte = bytes
                            .iter()
                            .position(|byte| *byte >= 0x80)
                            .expect("the target ref must carry a multi-byte character");
                        result.meta.size = (first_multibyte + 1) as u64;
                        return Ok(result);
                    }
                }
            }
            self.target.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.target.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.target.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.target.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            if prefix.is_some_and(is_branch_ref_directory) {
                self.fault.branch_lists.fetch_add(1, Ordering::SeqCst);
            }
            self.target.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.target.copy_opts(from, to, options).await
        }
    }

    /// What [`BranchCreatePutFault`] does to a native create's puts.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CreatePut {
        /// The ref put applies, and its answer is lost.
        RefAnswerLost,
        /// The ref put is lost before it reaches the store.
        RefLost,
        /// The ref put is answered as lost and held; it lands when the test
        /// lands it.
        RefHeld,
        /// The ref put is answered as lost and lands 100 ms later.
        RefLandsSoon,
        /// The store refuses the ref put before it applies.
        RefRefused,
        /// Every manifest put of the clone is lost before it reaches the store.
        CloneLost,
    }

    type HeldPut = (Arc<dyn ObjectStore>, Path, PutPayload, PutOptions);

    #[derive(Debug, Clone)]
    struct BranchCreatePutFault {
        put: CreatePut,
        branch: Arc<str>,
        held: Arc<std::sync::Mutex<Option<HeldPut>>>,
        ref_puts: Arc<AtomicUsize>,
    }

    impl BranchCreatePutFault {
        fn new(put: CreatePut, branch: &str) -> Self {
            Self {
                put,
                branch: Arc::from(branch),
                held: Arc::default(),
                ref_puts: Arc::default(),
            }
        }

        fn ref_puts(&self) -> usize {
            self.ref_puts.load(Ordering::SeqCst)
        }

        /// Apply the held ref put now.
        async fn land(&self) {
            let (store, location, payload, options) =
                self.held.lock().unwrap().take().expect("a held ref put");
            store.put_opts(&location, payload, options).await.unwrap();
        }
    }

    /// A remote store's failure whose request may or may not have applied.
    fn lost_put() -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: Box::new(std::io::Error::other("connection reset")),
        }
    }

    impl WrappingObjectStore for BranchCreatePutFault {
        fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
            Arc::new(BranchCreatePutStore {
                target,
                fault: self.clone(),
            })
        }

        fn wrap_paginated(
            &self,
            _store_prefix: &str,
            _original: Arc<dyn object_store::list::PaginatedListStore>,
        ) -> Option<Arc<dyn object_store::list::PaginatedListStore>> {
            // Listings pass through the fault store, which forwards them.
            None
        }
    }

    #[derive(Debug)]
    struct BranchCreatePutStore {
        target: Arc<dyn ObjectStore>,
        fault: BranchCreatePutFault,
    }

    impl fmt::Display for BranchCreatePutStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "BranchCreatePutStore({})", self.target)
        }
    }

    #[async_trait]
    impl ObjectStore for BranchCreatePutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            let branch = self.fault.branch.as_ref();
            let is_ref = is_target_branch_ref(location, &format!("{branch}.json"));
            let parts: Vec<_> = location
                .parts()
                .map(|part| part.as_ref().to_string())
                .collect();
            let is_clone_manifest = parts
                .windows(3)
                .any(|window| window == ["tree", branch, "_versions"])
                && location.extension() == Some("manifest");
            if is_ref {
                self.fault.ref_puts.fetch_add(1, Ordering::SeqCst);
            }
            match self.fault.put {
                CreatePut::RefAnswerLost if is_ref => {
                    self.target.put_opts(location, payload, options).await?;
                    Err(lost_put())
                }
                CreatePut::RefLost if is_ref => Err(lost_put()),
                CreatePut::RefHeld if is_ref => {
                    *self.fault.held.lock().unwrap() =
                        Some((Arc::clone(&self.target), location.clone(), payload, options));
                    Err(lost_put())
                }
                CreatePut::RefLandsSoon if is_ref => {
                    let target = Arc::clone(&self.target);
                    let location = location.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        target.put_opts(&location, payload, options).await.unwrap();
                    });
                    Err(lost_put())
                }
                CreatePut::RefRefused if is_ref => Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: Box::new(std::io::Error::other("no such bucket")),
                }),
                CreatePut::CloneLost if is_clone_manifest => Err(lost_put()),
                _ => self.target.put_opts(location, payload, options).await,
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.target.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.target.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.target.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.target.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.target.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.target.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.target.copy_opts(from, to, options).await
        }
    }

    async fn test_dataset(dir: &tempfile::TempDir) -> Dataset {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let path = dir.path().to_str().unwrap().replace('\\', "/");
        let path_prefix = if path.starts_with('/') { "" } else { "/" };
        let uri = format!("file-object-store://{path_prefix}{path}");
        // forbidden-api-allow: test-only raw Lance fixture for native branch-control truth cells
        Dataset::write(
            reader,
            &uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                auto_cleanup: None,
                skip_auto_cleanup: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn branch_enumeration_relists_after_ref_vanishes_between_list_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("vanish", version, None)
            .await
            .unwrap();
        dataset
            .create_branch("survivor", version, None)
            .await
            .unwrap();

        let fault = Arc::new(VanishingBranchRefFault::delete_once("vanish.json"));
        let wrapped = dataset
            .with_object_store_wrappers(vec![Arc::clone(&fault) as Arc<dyn WrappingObjectStore>]);

        let branches = list_branch_contents(&wrapped).await.unwrap();
        assert!(!branches.contains_key("vanish"));
        assert!(branches.contains_key("survivor"));
        assert_eq!(fault.injected_failures(), 1);
        assert_eq!(fault.branch_lists(), 2);
    }

    #[tokio::test]
    async fn branch_enumeration_stops_after_the_fixed_retry_bound() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();

        let fault = Arc::new(VanishingBranchRefFault::always_not_found("feature.json"));
        let wrapped = dataset
            .with_object_store_wrappers(vec![Arc::clone(&fault) as Arc<dyn WrappingObjectStore>]);

        let error = list_branch_contents(&wrapped)
            .await
            .expect_err("persistent branch churn must escape after the fixed bound");
        assert!(matches!(
            error,
            OmniError::Storage(ref failure)
                if failure.kind == crate::error::StorageFailureKind::NotFound
        ));
        assert_eq!(fault.branch_lists(), BRANCH_REF_READ_MAX_ATTEMPTS);
        assert!(
            fault.injected_failures() >= BRANCH_REF_READ_MAX_ATTEMPTS,
            "every branch-list attempt must observe the injected missing ref"
        );
    }

    #[tokio::test]
    async fn branch_enumeration_relists_after_a_ref_rewrite_between_head_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("retiring", version, None)
            .await
            .unwrap();
        dataset
            .create_branch("survivor", version, None)
            .await
            .unwrap();

        let fault = Arc::new(VanishingBranchRefFault::stale_head_size_once(
            "retiring.json",
        ));
        let wrapped = dataset
            .with_object_store_wrappers(vec![Arc::clone(&fault) as Arc<dyn WrappingObjectStore>]);

        let branches = list_branch_contents(&wrapped).await.unwrap();
        assert!(branches.contains_key("retiring"));
        assert!(branches.contains_key("survivor"));
        assert_eq!(fault.injected_failures(), 1);
        assert_eq!(fault.branch_lists(), 2);
    }

    #[tokio::test]
    async fn branch_get_rereads_after_a_ref_rewrite_between_head_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("retiring", version, None)
            .await
            .unwrap();

        let fault = Arc::new(VanishingBranchRefFault::stale_head_size_twice(
            "retiring.json",
        ));
        let wrapped = dataset
            .with_object_store_wrappers(vec![Arc::clone(&fault) as Arc<dyn WrappingObjectStore>]);

        let contents = get_live_manifest_branch_contents(&wrapped, "retiring")
            .await
            .unwrap();
        assert_eq!(contents.parent_version, version);
        assert_eq!(fault.injected_failures(), 2);
    }

    #[tokio::test]
    async fn branch_enumeration_relists_after_a_prefix_cut_inside_a_multibyte_character() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("retiring", version, None)
            .await
            .unwrap();
        dataset
            .branches()
            .replace_metadata(
                "retiring",
                HashMap::from([("note".to_string(), "équipe".to_string())]),
            )
            .await
            .unwrap();

        let fault = Arc::new(VanishingBranchRefFault::stale_head_size_mid_char_once(
            "retiring.json",
        ));
        let wrapped = dataset
            .with_object_store_wrappers(vec![Arc::clone(&fault) as Arc<dyn WrappingObjectStore>]);

        let branches = list_branch_contents(&wrapped).await.unwrap();
        assert_eq!(branches["retiring"].metadata["note"], "équipe");
        assert_eq!(fault.injected_failures(), 1);
        assert_eq!(fault.branch_lists(), 2);
    }

    #[tokio::test]
    async fn branch_identifier_rereads_after_a_ref_rewrite_between_head_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("retiring", version, None)
            .await
            .unwrap();
        let expected = dataset.branches().get("retiring").await.unwrap().identifier;

        let fault = Arc::new(VanishingBranchRefFault::stale_head_size_twice(
            "retiring.json",
        ));
        let wrapped = dataset
            .with_object_store_wrappers(vec![Arc::clone(&fault) as Arc<dyn WrappingObjectStore>]);

        let identifier = get_branch_identifier(&wrapped, "retiring").await.unwrap();
        assert_eq!(identifier, expected);
        assert_eq!(fault.injected_failures(), 2);
    }

    /// The nightly's failure shape: resolving one branch read every ref, so
    /// another actor's retirement rewrite tore the lookup.
    #[tokio::test]
    async fn resolving_a_branch_reads_only_its_own_incarnations() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        let native = |logical: &str| {
            crate::branch_names::native_branch_name(
                logical,
                &crate::branch_names::mint_incarnation(),
            )
        };
        let (live, retired, sibling, nested, other) = (
            native("dev"),
            native("dev"),
            native("dev-2"),
            native("team/dev"),
            native("feature"),
        );
        for name in [&live, &retired, &sibling, &nested, &other] {
            dataset.create_branch(name, version, None).await.unwrap();
        }
        let retired_identifier = dataset.branches().get(&retired).await.unwrap().identifier;
        retire_branch_recoverably(&dataset, &retired, &retired_identifier)
            .await
            .unwrap();

        let fault = Arc::new(VanishingBranchRefFault::new(
            &format!("{other}.json"),
            usize::MAX,
            InjectedRefRead::StaleHeadSize,
        ));
        let wrapped = dataset
            .with_object_store_wrappers(vec![Arc::clone(&fault) as Arc<dyn WrappingObjectStore>]);

        list_live_manifest_branch_contents(&wrapped)
            .await
            .expect_err("reading every ref meets the other branch's torn ref");
        assert!(fault.injected_failures() > 0);

        let listings_before = fault.branch_lists();
        let resolved = resolve_live_native_branch(&wrapped, "dev").await.unwrap();
        assert_eq!(resolved.as_deref(), Some(live.as_str()));
        let resolved = resolve_live_native_branch(&wrapped, "dev-2").await.unwrap();
        assert_eq!(resolved.as_deref(), Some(sibling.as_str()));
        let resolved = resolve_live_native_branch(&wrapped, "team/dev")
            .await
            .unwrap();
        assert_eq!(resolved.as_deref(), Some(nested.as_str()));
        assert_eq!(
            resolve_live_native_branch(&wrapped, "absent")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            fault.branch_lists() - listings_before,
            4,
            "each resolve lists the ref directory exactly once"
        );
        assert_eq!(
            fault.injected_failures(),
            BRANCH_REF_READ_MAX_ATTEMPTS,
            "the four resolves never read the other branch's ref; only the listing above did"
        );

        let every_live = list_live_manifest_branch_contents(&dataset).await.unwrap();
        for logical in ["dev", "dev-2", "team/dev", "feature", "absent"] {
            assert_eq!(
                resolve_live_native_branch(&dataset, logical).await.unwrap(),
                crate::branch_names::resolve_native_branch(
                    every_live.keys().map(String::as_str),
                    logical
                )
                .unwrap(),
                "resolving '{logical}' by its own refs must agree with the full listing"
            );
        }

        let vanish = Arc::new(VanishingBranchRefFault::delete_once(&format!(
            "{live}.json"
        )));
        let wrapped = dataset
            .with_object_store_wrappers(vec![Arc::clone(&vanish) as Arc<dyn WrappingObjectStore>]);
        assert_eq!(
            resolve_live_native_branch(&wrapped, "dev").await.unwrap(),
            None,
            "a candidate reclaimed between the listing and its read is skipped, not an error"
        );
        assert_eq!(vanish.injected_failures(), 1);
    }

    /// Relevant damaged refs fail authority reads; unrelated refs do not.
    #[tokio::test]
    async fn live_branch_lookup_preserves_authority_while_ignoring_unrelated_refs() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        let logical = "guarded";
        let generated = crate::branch_names::native_branch_name(
            logical,
            &crate::branch_names::mint_incarnation(),
        );

        dataset
            .create_branch("unrelated", version, None)
            .await
            .unwrap();
        let root = dataset.branch_location().find_main().unwrap().path;
        let store = dataset.object_store(None).await.unwrap();
        let unrelated = lance::dataset::refs::branch_contents_path(&root, "unrelated");
        store.put(&unrelated, b"{").await.unwrap();
        assert_eq!(
            resolve_live_native_branch(&dataset, logical).await.unwrap(),
            None,
            "an unrelated malformed ref must not turn an absent branch into a lookup error"
        );

        for native in [logical.to_string(), generated] {
            dataset.create_branch(&native, version, None).await.unwrap();
            assert_eq!(
                resolve_live_native_branch(&dataset, logical).await.unwrap(),
                Some(native.clone()),
                "a live ref must resolve: {native}"
            );

            let transient = Arc::new(VanishingBranchRefFault::stale_head_size_once(&format!(
                "{native}.json"
            )));
            let wrapped = dataset.with_object_store_wrappers(vec![
                Arc::clone(&transient) as Arc<dyn WrappingObjectStore>
            ]);
            assert_eq!(
                resolve_live_native_branch(&wrapped, logical).await.unwrap(),
                Some(native.clone()),
                "a torn ref read must retry without reporting the branch absent"
            );
            assert_eq!(transient.injected_failures(), 1);

            let identity = dataset.branches().get(&native).await.unwrap().identifier;
            retire_branch_recoverably(&dataset, &native, &identity)
                .await
                .unwrap();
            assert_eq!(
                resolve_live_native_branch(&dataset, logical).await.unwrap(),
                None,
                "a validated retired ref must no longer resolve: {native}"
            );

            let retired = archived_manifest_branch(&dataset, &native)
                .await
                .unwrap()
                .unwrap();
            store
                .put(
                    &lance::dataset::refs::branch_contents_path(&root, &native),
                    &serde_json::to_vec(&retired).unwrap(),
                )
                .await
                .unwrap();
            let mismatched = RetiredManifestBranch {
                version: 1,
                native_branch: "different-native-ref".to_string(),
                identifier: retired.identifier.clone(),
            };
            for malformed in ["{".to_string(), serde_json::to_string(&mismatched).unwrap()] {
                let mut metadata = retired.metadata.clone();
                metadata.insert(RETIRED_MANIFEST_BRANCH_KEY.to_string(), malformed);
                dataset
                    .branches()
                    .replace_metadata(&native, metadata)
                    .await
                    .unwrap();
                let error = resolve_live_native_branch(&dataset, logical)
                    .await
                    .expect_err("malformed or mismatched retirement must fail closed");
                assert!(error.to_string().contains("retirement metadata"), "{error}");
                dataset
                    .branches()
                    .replace_metadata(&native, retired.metadata.clone())
                    .await
                    .unwrap();
            }

            let persistent = Arc::new(VanishingBranchRefFault::new(
                &format!("{native}.json"),
                usize::MAX,
                InjectedRefRead::StaleHeadSize,
            ));
            let wrapped = dataset.with_object_store_wrappers(vec![
                Arc::clone(&persistent) as Arc<dyn WrappingObjectStore>
            ]);
            resolve_live_native_branch(&wrapped, logical)
                .await
                .expect_err("an unreadable relevant ref must never be treated as absent");
            assert!(persistent.injected_failures() >= BRANCH_REF_READ_MAX_ATTEMPTS);
            assert_eq!(persistent.branch_lists(), 1);
            assert_eq!(
                resolve_live_native_branch(&dataset, logical).await.unwrap(),
                None,
                "the read-only failures must leave the validated retirement intact"
            );
        }
    }

    #[test]
    fn create_match_requires_exact_parent_incarnation_prefix() {
        let parent = BranchIdentifier {
            version_mapping: vec![(2, "parent-a".to_string())],
        };
        let matching = BranchContents {
            parent_branch: Some("source".to_string()),
            identifier: BranchIdentifier {
                version_mapping: vec![(2, "parent-a".to_string()), (7, "child".to_string())],
            },
            parent_version: 7,
            create_at: 0,
            manifest_size: 0,
            metadata: Default::default(),
        };
        assert!(matches_create_expectation(
            &matching,
            Some("source"),
            7,
            &parent
        ));

        let foreign_parent = BranchIdentifier {
            version_mapping: vec![(2, "parent-b".to_string())],
        };
        assert!(!matches_create_expectation(
            &matching,
            Some("source"),
            7,
            &foreign_parent
        ));
    }

    #[tokio::test]
    async fn retire_rejects_recreated_identifier_without_removing_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let original = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();
        dataset.delete_branch("feature").await.unwrap();
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let recreated = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();
        assert_ne!(original, recreated);

        let error = retire_branch_recoverably(&dataset, "feature", &original)
            .await
            .expect_err("a recreated target must be fenced by its identifier");
        assert!(error.to_string().contains("changed"), "{error}");
        assert_eq!(
            dataset
                .list_branches()
                .await
                .unwrap()
                .get("feature")
                .unwrap()
                .identifier,
            recreated,
            "classifier must never delete the recreated authority"
        );
    }

    #[tokio::test]
    async fn lower_create_chokepoint_rejects_prefix_collision_before_clone() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();

        let error = match create_branch_recoverably(&mut dataset, "feature/child", version, None)
            .await
        {
            Ok(_) => panic!("the lower branch-control surface must enforce prefix disjointness"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("feature"));
        assert!(error.to_string().contains("physical Lance path"));
        assert!(
            !dataset
                .list_branches()
                .await
                .unwrap()
                .contains_key("feature/child"),
            "admission refusal must precede authoritative ref creation"
        );
        assert!(
            !dir.path()
                .join("tree")
                .join("feature")
                .join("child")
                .exists(),
            "admission refusal must precede Lance's shallow-clone phase"
        );
    }

    /// What one native create's faulted put leaves.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CreateEnds {
        /// The ref is this create's.
        Created,
        /// The ref may still land: the create is in doubt and its clone is
        /// kept, so a late landing leaves a whole branch.
        InDoubt,
        /// No copy of the ref can land: the clone is reclaimed and the
        /// failure is unpublished, which a caller may retry under a fresh
        /// native name.
        Unpublished,
    }

    /// A native create reads its ref back after a failed put. An applied put
    /// whose answer was lost is this create's, as is one that lands while
    /// the ref is read again. A put that may still land keeps the create in
    /// doubt and its clone in place; landing it later leaves a branch that
    /// opens. A refused ref put, or a clone that never committed, cannot
    /// land: its clone is reclaimed and the failure is unpublished. No path
    /// sends a second ref put.
    #[tokio::test]
    async fn native_create_reads_its_ref_back_before_deciding_issue_1067() {
        use CreateEnds::*;
        for (put, expected) in [
            (CreatePut::RefAnswerLost, Created),
            (CreatePut::RefLandsSoon, Created),
            (CreatePut::RefLost, InDoubt),
            (CreatePut::RefHeld, InDoubt),
            (CreatePut::RefRefused, Unpublished),
            (CreatePut::CloneLost, Unpublished),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let dataset = test_dataset(&dir).await;
            let version = dataset.version().version;
            let fault = Arc::new(BranchCreatePutFault::new(put, "feature"));
            let mut wrapped = dataset.with_object_store_wrappers(vec![
                Arc::clone(&fault) as Arc<dyn WrappingObjectStore>
            ]);
            let result = create_branch_recoverably(&mut wrapped, "feature", version, None).await;
            let clone = dir.path().join("tree").join("feature");
            match expected {
                Created => {
                    assert!(
                        matches!(result, Ok(BranchCreateOutcome::Created)),
                        "{put:?}: {:?}",
                        result.err()
                    );
                    assert!(dataset.branches().get("feature").await.is_ok(), "{put:?}");
                }
                InDoubt => {
                    let error = result.err().expect("a create in doubt is an error");
                    assert!(error.is_manifest_publish_in_doubt(), "{put:?}: {error}");
                    assert!(clone.exists(), "{put:?}: the clone a late ref names stays");
                    assert!(dataset.branches().get("feature").await.is_err(), "{put:?}");
                    if put == CreatePut::RefHeld {
                        fault.land().await;
                        dataset
                            .checkout_branch("feature")
                            .await
                            .expect("a ref that lands late names a whole clone");
                    }
                }
                Unpublished => {
                    let error = result.err().expect("a failed create is an error");
                    assert_eq!(
                        error.completion_evidence(),
                        Some(CompletionEvidence::Unpublished),
                        "{put:?}: {error}"
                    );
                    assert!(!clone.exists(), "{put:?}: the clone is reclaimed");
                    assert!(dataset.branches().get("feature").await.is_err(), "{put:?}");
                }
            }
            let expected_ref_puts = usize::from(put != CreatePut::CloneLost);
            assert_eq!(fault.ref_puts(), expected_ref_puts, "{put:?}");
        }
    }

    #[tokio::test]
    async fn lower_create_preserves_preexisting_same_name_ref() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let before = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();

        let outcome = create_branch_recoverably(&mut dataset, "feature", version, None)
            .await
            .unwrap();
        assert!(matches!(outcome, BranchCreateOutcome::RefAlreadyExists));
        let after = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();
        assert_eq!(
            before, after,
            "classification must not replace the live ref"
        );
    }

    #[tokio::test]
    async fn absent_only_reclaim_preserves_authority_that_appeared_after_prior_absence() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        assert!(
            !dataset
                .list_branches()
                .await
                .unwrap()
                .contains_key("feature"),
            "precondition: an earlier classifier observed target absence"
        );
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let before = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();

        assert!(
            !reclaim_ref_absent_tree(&mut dataset, "feature")
                .await
                .unwrap(),
            "the final authority check must refuse destructive cleanup"
        );
        assert_eq!(
            dataset
                .list_branches()
                .await
                .unwrap()
                .get("feature")
                .unwrap()
                .identifier,
            before
        );
    }

    #[tokio::test]
    async fn force_reclaim_reports_lexically_first_live_physical_path_child() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature/zeta", version, None)
            .await
            .unwrap();
        dataset
            .create_branch("feature/alpha", version, None)
            .await
            .unwrap();
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();

        let error = force_delete_branch_idempotent(&mut dataset, "feature")
            .await
            .expect_err("ancestor reclaim must not report success while a path-child is live");
        assert!(error.to_string().contains("feature/alpha"));
        assert!(
            dataset
                .list_branches()
                .await
                .unwrap()
                .contains_key("feature"),
            "preflight refusal must preserve ancestor authority"
        );
    }
}
