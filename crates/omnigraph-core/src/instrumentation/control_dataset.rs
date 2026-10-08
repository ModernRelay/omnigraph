use std::sync::Arc;

use lance::Dataset;
use lance::dataset::refs::Refs;
use lance::io::WrappingObjectStore;
use lance_core::deepsize::DeepSizeOf;
use lance_table::io::commit::ManifestLocation;

use crate::error::{OmniError, Result};

fn retained_bytes(dataset: &Dataset) -> usize {
    let manifest = dataset.manifest();
    let location = dataset.manifest_location();
    std::mem::size_of::<Dataset>()
        .saturating_add(manifest.deep_size_of())
        .saturating_add(dataset.uri().len().saturating_mul(4))
        .saturating_add(location.path.as_ref().len().saturating_mul(4))
        .saturating_add(location.e_tag.as_ref().map_or(0, String::capacity))
        .saturating_add(manifest.fragments.len().saturating_mul(128))
        .saturating_add(1024)
}

async fn rebind(
    dataset: &Dataset,
    wrapper: Option<Arc<dyn WrappingObjectStore>>,
) -> Result<Dataset> {
    let mut params = dataset.store_params().cloned().ok_or_else(|| {
        OmniError::manifest_internal("control dataset has no object-store parameters".to_string())
    })?;
    params.object_store_wrapper = wrapper.clone();
    let (store, _) = lance::io::ObjectStore::from_uri_and_params(
        dataset.session().store_registry(),
        dataset.uri(),
        &params,
    )
    .await
    .map_err(OmniError::storage)?;
    let handler =
        crate::lance_clone::configured_commit_handler(dataset.uri(), &Some(params.clone()), None)
            .await
            .map_err(OmniError::storage)?;
    let mut rebound = dataset.with_object_store(Arc::clone(&store), Some(params));
    rebound.refs = Refs::new(Arc::clone(&store), handler, dataset.branch_location());
    if let Some(wrapper) = wrapper {
        super::record_probed_store(&wrapper, store);
    }
    Ok(rebound)
}

/// Retain bounded acknowledged control metadata without retaining a query's IO scope.
/// Shared session/provider resources remain owned by their existing owners.
pub async fn retain_control_dataset(dataset: &Dataset, budget: usize) -> Option<Dataset> {
    let manifest = dataset.manifest();
    if manifest.branch.is_some()
        || !manifest.base_paths.is_empty()
        || manifest
            .schema
            .fields_pre_order()
            .any(|field| field.dictionary.is_some())
        || retained_bytes(dataset) > budget
    {
        return None;
    }
    rebind(dataset, None).await.ok()
}

/// Qualify a retained main control image against fresh storage metadata.
/// Missing or changed ETags return a cache miss without consulting session metadata caches.
pub async fn current_control_dataset(
    dataset: &Dataset,
    wrapper: Option<Arc<dyn WrappingObjectStore>>,
) -> Result<Option<Dataset>> {
    if dataset.manifest().branch.is_some()
        || !dataset.manifest().base_paths.is_empty()
        || dataset
            .manifest_location()
            .e_tag
            .as_ref()
            .is_none_or(String::is_empty)
    {
        return Ok(None);
    }
    let rebound = rebind(dataset, wrapper).await?;
    let store = rebound
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let params = rebound.store_params().cloned();
    let handler = crate::lance_clone::configured_commit_handler(rebound.uri(), &params, None)
        .await
        .map_err(OmniError::storage)?;
    let latest = match handler
        .resolve_latest_location(&rebound.branch_location().path, &store)
        .await
    {
        Ok(location) => location,
        Err(lance::Error::DatasetNotFound { .. }) => return Ok(None),
        Err(error) => return Err(OmniError::storage(error)),
    };
    let held = rebound.manifest_location();
    Ok(same_location(held, &latest).then_some(rebound))
}

fn same_location(held: &ManifestLocation, latest: &ManifestLocation) -> bool {
    held.path == latest.path
        && held.version == latest.version
        && held.naming_scheme == latest.naming_scheme
        && latest
            .e_tag
            .as_ref()
            .is_some_and(|tag| !tag.is_empty() && Some(tag) == held.e_tag.as_ref())
        && !matches!((held.size, latest.size), (Some(a), Some(b)) if a != b)
}

#[cfg(test)]
mod tests {
    use lance_table::io::commit::ManifestNamingScheme;

    use super::*;

    #[test]
    fn control_dataset_requires_matching_nonempty_generation_tokens() {
        let held = ManifestLocation {
            version: 2,
            path: "root/__manifest/_versions/2.manifest".into(),
            size: Some(1024),
            naming_scheme: ManifestNamingScheme::V1,
            e_tag: Some("original".to_string()),
        };
        assert!(same_location(&held, &held));
        for (old, new) in [
            (Some("original"), None),
            (None, None),
            (None, Some("new")),
            (Some(""), Some("")),
            (Some("original"), Some("recreated")),
        ] {
            let mut before = held.clone();
            before.e_tag = old.map(str::to_string);
            let mut after = held.clone();
            after.e_tag = new.map(str::to_string);
            assert!(!same_location(&before, &after), "old={old:?}, new={new:?}");
        }
        let mut changed = held.clone();
        changed.size = Some(2048);
        assert!(!same_location(&held, &changed));
        changed = held.clone();
        changed.path = "root/__history/_versions/2.manifest".into();
        assert!(!same_location(&held, &changed));
        changed = held.clone();
        changed.version = 3;
        assert!(!same_location(&held, &changed));
    }
}
