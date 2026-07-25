//! Canonical fail-closed inventory for the currently listed Lance MemWAL
//! namespace used by RFC-026 evidence tests.
//!
//! This is intentionally one shared persisted-layout truth. It classifies only
//! the pinned Lance RC.1 path grammar and validates shard-manifest authority;
//! unknown or malformed paths remain visible as `Unknown` so callers fail
//! closed. Ordinary LIST still cannot expose incomplete multipart uploads,
//! superseded provider versions, delete markers, local staging files, or
//! provider-billed bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::TryStreamExt;
use lance::dataset::mem_wal::ShardManifestStore;
use lance_index::mem_wal::{ShardId, ShardManifest};
use object_store::path::Path as ObjectPath;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemWalObjectKind {
    Wal,
    ShardManifestVersion,
    ShardManifestHint,
    GenerationData,
    GenerationManifest,
    GenerationTransaction,
    GenerationDeletion,
    GenerationPkSidecar,
    GenerationBloom,
    GenerationUserIndex,
    Unknown,
}

#[derive(Debug, Clone, Default)]
pub struct CurrentMemWalInventory {
    pub objects: BTreeMap<String, (MemWalObjectKind, u64)>,
    pub generation_roots: BTreeSet<String>,
    pub referenced_generation_roots: BTreeSet<String>,
}

impl CurrentMemWalInventory {
    pub fn object_count(&self, kind: MemWalObjectKind) -> usize {
        self.objects
            .values()
            .filter(|(candidate, _)| *candidate == kind)
            .count()
    }

    pub fn bytes(&self, kind: MemWalObjectKind) -> u64 {
        self.objects
            .values()
            .filter(|(candidate, _)| *candidate == kind)
            .map(|(_, bytes)| *bytes)
            .sum()
    }

    pub fn immutable_object_bytes(&self) -> u64 {
        self.objects
            .values()
            .filter(|(kind, _)| *kind != MemWalObjectKind::ShardManifestHint)
            .map(|(_, bytes)| *bytes)
            .sum()
    }

    pub fn unknown_paths(&self) -> Vec<&str> {
        self.objects
            .iter()
            .filter_map(|(path, (kind, _))| {
                (*kind == MemWalObjectKind::Unknown).then_some(path.as_str())
            })
            .collect()
    }

    pub fn generation_subtree_objects(&self) -> BTreeMap<String, (MemWalObjectKind, u64)> {
        self.objects
            .iter()
            .filter(|(_, (kind, _))| {
                matches!(
                    kind,
                    MemWalObjectKind::GenerationData
                        | MemWalObjectKind::GenerationManifest
                        | MemWalObjectKind::GenerationTransaction
                        | MemWalObjectKind::GenerationDeletion
                        | MemWalObjectKind::GenerationPkSidecar
                        | MemWalObjectKind::GenerationBloom
                        | MemWalObjectKind::GenerationUserIndex
                )
            })
            .map(|(path, value)| (path.clone(), *value))
            .collect()
    }

    pub fn assert_path_class_size_retained_by(&self, later: &Self) {
        for (path, (kind, bytes)) in &self.objects {
            if *kind == MemWalObjectKind::ShardManifestHint {
                continue;
            }
            assert_eq!(
                later.objects.get(path),
                Some(&(*kind, *bytes)),
                "retain-all evidence lost or changed the class/size of listed object {path}"
            );
        }
    }
}

pub fn is_generation_root(component: &str) -> bool {
    let Some((hash, generation)) = component.split_once("_gen_") else {
        return false;
    };
    let Ok(generation_number) = generation.parse::<u64>() else {
        return false;
    };
    hash.len() == 8
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && generation_number > 0
        && generation_number.to_string() == generation
}

pub fn parse_positive_bit_reversed_filename(filename: &str, extension: &str) -> Option<u64> {
    let Some(stem) = filename.strip_suffix(&format!(".{extension}")) else {
        return None;
    };
    if stem.len() != 64 || !stem.bytes().all(|byte| matches!(byte, b'0' | b'1')) {
        return None;
    }
    let reversed = u64::from_str_radix(stem, 2).ok()?;
    let value = reversed.reverse_bits();
    (value > 0).then_some(value)
}

fn parse_canonical_shard_id(value: &str) -> Option<ShardId> {
    let shard_id = ShardId::parse_str(value).ok()?;
    (shard_id.as_hyphenated().to_string() == value).then_some(shard_id)
}

fn join_object_path(base: &str, suffix: &str) -> String {
    if base.is_empty() {
        suffix.to_string()
    } else {
        format!("{base}/{suffix}")
    }
}

pub fn classify_mem_wal_object(
    components: &[&str],
    mem_wal_index: usize,
) -> (MemWalObjectKind, Option<String>) {
    let tail = &components[mem_wal_index + 1..];
    if tail.len() < 3 || parse_canonical_shard_id(tail[0]).is_none() {
        return (MemWalObjectKind::Unknown, None);
    }

    match tail[1] {
        "wal"
            if tail.len() == 3
                && parse_positive_bit_reversed_filename(tail[2], "arrow").is_some() =>
        {
            (MemWalObjectKind::Wal, None)
        }
        "manifest" if tail.len() == 3 && tail[2] == "version_hint.json" => {
            (MemWalObjectKind::ShardManifestHint, None)
        }
        "manifest"
            if tail.len() == 3
                && parse_positive_bit_reversed_filename(tail[2], "binpb").is_some() =>
        {
            (MemWalObjectKind::ShardManifestVersion, None)
        }
        generation if is_generation_root(generation) => {
            let root = components[..mem_wal_index + 3].join("/");
            let kind = match tail.get(2).copied() {
                Some("bloom_filter.bin") if tail.len() == 3 => MemWalObjectKind::GenerationBloom,
                Some("data") if tail.len() >= 4 => MemWalObjectKind::GenerationData,
                Some("_versions") if tail.len() >= 4 => MemWalObjectKind::GenerationManifest,
                Some("_transactions") if tail.len() >= 4 => MemWalObjectKind::GenerationTransaction,
                Some("_deletions") if tail.len() >= 4 => MemWalObjectKind::GenerationDeletion,
                Some("_pk_index") if tail.len() >= 4 => MemWalObjectKind::GenerationPkSidecar,
                Some("_indices") if tail.len() >= 4 => MemWalObjectKind::GenerationUserIndex,
                _ => MemWalObjectKind::Unknown,
            };
            (kind, Some(root))
        }
        _ => (MemWalObjectKind::Unknown, None),
    }
}

pub fn validated_referenced_generation_roots(
    table_base: &str,
    directory_shard_id: ShardId,
    manifest: &ShardManifest,
    listed_generation_roots: &BTreeSet<String>,
    listed_manifest_versions: &BTreeSet<u64>,
) -> BTreeSet<String> {
    assert_eq!(
        manifest.shard_id, directory_shard_id,
        "Gate R0 must fail closed when decoded shard authority disagrees with its directory"
    );
    assert!(
        manifest.version > 0,
        "Gate R0 must fail closed on a zero shard-manifest version"
    );
    let latest_listed_version = listed_manifest_versions
        .last()
        .copied()
        .expect("Gate R0 must fail closed when a shard has no listed manifest version");
    assert_eq!(
        latest_listed_version, manifest.version,
        "Gate R0 must fail closed when the latest shard-manifest filename disagrees with its body"
    );
    let expected_manifest_version_count = usize::try_from(manifest.version)
        .expect("Gate R0 must fail closed when shard-manifest authority exceeds usize");
    assert_eq!(
        listed_manifest_versions.len(),
        expected_manifest_version_count,
        "Gate R0 must fail closed on a gapped or noncanonical shard-manifest version chain"
    );
    assert!(
        manifest.current_generation > 0,
        "Gate R0 must fail closed on zero next-generation allocation authority"
    );

    let shard_prefix = join_object_path(
        table_base,
        &format!("_mem_wal/{}", directory_shard_id.as_hyphenated()),
    );
    let mut generations = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut roots = BTreeSet::new();
    for flushed in &manifest.flushed_generations {
        assert!(
            flushed.generation > 0 && generations.insert(flushed.generation),
            "Gate R0 must fail closed on a zero or duplicate flushed generation"
        );
        assert!(
            paths.insert(flushed.path.clone()),
            "Gate R0 must fail closed on a duplicate flushed-generation path"
        );
        assert!(
            is_generation_root(&flushed.path),
            "Gate R0 must fail closed on a noncanonical flushed-generation path"
        );
        let (_, path_generation) = flushed
            .path
            .split_once("_gen_")
            .expect("validated generation root must contain _gen_");
        assert_eq!(
            path_generation.parse::<u64>().unwrap(),
            flushed.generation,
            "Gate R0 must fail closed when generation authority disagrees with its path"
        );
        assert!(
            manifest.current_generation > flushed.generation,
            "Gate R0 must fail closed when next-generation allocation authority has not advanced past a flushed generation"
        );
        let root = join_object_path(&shard_prefix, &flushed.path);
        assert!(
            listed_generation_roots.contains(&root),
            "Gate R0 must fail closed when shard authority references a missing listed generation"
        );
        assert!(
            roots.insert(root),
            "Gate R0 must fail closed on duplicate generation authority"
        );
    }
    roots
}

/// Strict current-object inventory for the part of retained storage that stock
/// Lance exposes through ordinary LIST.
pub async fn current_mem_wal_inventory(uri: &str) -> CurrentMemWalInventory {
    let (store, root_path) = lance_io::object_store::ObjectStore::from_uri(uri)
        .await
        .expect("MemWAL fixture URI must resolve");
    let listed = store
        .inner
        .list(Some(&root_path))
        .try_collect::<Vec<_>>()
        .await
        .expect("MemWAL inventory must fail closed on a listing error");

    let mut inventory = CurrentMemWalInventory::default();
    let mut shards = BTreeSet::new();
    let mut manifest_versions = BTreeMap::<(String, ShardId), BTreeSet<u64>>::new();
    for metadata in listed {
        let path = metadata.location.as_ref().to_string();
        let components = path.split('/').collect::<Vec<_>>();
        let Some(mem_wal_index) = components
            .iter()
            .position(|component| *component == "_mem_wal")
        else {
            continue;
        };
        let (kind, generation_root) = classify_mem_wal_object(&components, mem_wal_index);
        if let Some(root) = generation_root {
            inventory.generation_roots.insert(root);
        }
        let bytes = u64::try_from(metadata.size).expect("object size must fit u64");
        assert!(
            inventory
                .objects
                .insert(path.clone(), (kind, bytes))
                .is_none(),
            "object listing returned duplicate path {path}"
        );

        let tail = &components[mem_wal_index + 1..];
        if let Some(shard) = tail
            .first()
            .and_then(|value| parse_canonical_shard_id(value))
        {
            let table_base = components[..mem_wal_index].join("/");
            shards.insert((table_base.clone(), shard));
            if kind == MemWalObjectKind::ShardManifestVersion {
                let version = parse_positive_bit_reversed_filename(tail[2], "binpb")
                    .expect("classified shard-manifest version must decode");
                assert!(
                    manifest_versions
                        .entry((table_base, shard))
                        .or_default()
                        .insert(version),
                    "object listing returned duplicate decoded shard-manifest version"
                );
            }
        }
    }

    for (table_base, shard_id) in shards {
        let table_path =
            ObjectPath::parse(&table_base).expect("listed table base must remain a valid path");
        let manifest_store = ShardManifestStore::new(Arc::clone(&store), &table_path, shard_id, 2);
        let manifest = manifest_store
            .read_latest()
            .await
            .expect("MemWAL inventory must fail closed on an unreadable shard manifest")
            .expect("Gate R0 must fail closed when a listed shard has no latest manifest");
        let listed_manifest_versions = manifest_versions
            .get(&(table_base.clone(), shard_id))
            .expect("Gate R0 must fail closed when a shard has no listed manifest version");
        inventory
            .referenced_generation_roots
            .extend(validated_referenced_generation_roots(
                &table_base,
                shard_id,
                &manifest,
                &inventory.generation_roots,
                listed_manifest_versions,
            ));
    }

    inventory
}
