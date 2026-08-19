//! The `fixture` subcommands: build + validate + freeze a base store, or
//! stamp an existing pre-stamp fixture after validating a copy.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Args;
use omnigraph::IndexCoverage;
use omnigraph::db::{Omnigraph, ReadTarget};

use crate::fixture::{
    self, BaseProfile, BenchResult, FixtureManifest, IndexLevel, ValidationStamp,
};
use crate::{refuse_debug_build, source_commit, unix_now};

#[derive(Debug, Args)]
pub struct FixtureBuildArgs {
    /// Directory that holds frozen fixtures (created if absent). The fixture
    /// itself lands in `<fixtures-root>/<derived-name>/`.
    #[arg(long)]
    fixtures_root: PathBuf,
    /// T — node-table count.
    #[arg(long, default_value_t = 8)]
    tables: usize,
    /// N — base rows per table.
    #[arg(long, default_value_t = 100_000)]
    rows: usize,
    /// Filler bytes in the scalar payload column per row.
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    /// Deltas (rows per side) to pre-tag cohorts for, comma-separated.
    /// Default: the m3 sweep 1,50,5000.
    #[arg(long, value_delimiter = ',')]
    deltas: Vec<usize>,
    /// Tables any delta touches on both sides. Default: min(4, T) — the m3
    /// shape.
    #[arg(long)]
    diverged_tables: Option<usize>,
    /// High end of the index axes (F2 = index existence, F3 = index
    /// freshness): build the engine's declared key indexes (BTREE on the
    /// physical row-key column `id`, plus the FTS the `name @key` String
    /// implies) via `ensure_indices`, then run `optimize` so coverage is
    /// fresh, then verify coverage before freezing.
    #[arg(long)]
    index: bool,
}

pub async fn fixture_build(args: FixtureBuildArgs) -> BenchResult<()> {
    refuse_debug_build("freezing a fixture")?;
    let deltas = if args.deltas.is_empty() {
        vec![1, 50, 5000]
    } else {
        args.deltas.clone()
    };
    let diverged = args.diverged_tables.unwrap_or(args.tables.min(4));
    let profile = BaseProfile::new(args.tables, args.rows, args.payload_bytes, diverged, deltas)?;
    let name = fixture::derived_fixture_name(&profile, args.index);
    let fixture_dir = args.fixtures_root.join(&name);
    std::fs::create_dir_all(&args.fixtures_root)
        .map_err(|e| format!("creating {}: {e}", args.fixtures_root.display()))?;
    // `create_dir` (not exists + create_dir_all): claiming the directory IS
    // the existence check, so a concurrent builder cannot slip between them.
    match std::fs::create_dir(&fixture_dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(format!(
                "fixture directory {} already exists — delete it first to rebuild",
                fixture_dir.display()
            )
            .into());
        }
        Err(e) => return Err(format!("creating {}: {e}", fixture_dir.display()).into()),
    }
    // A fixture that fails to build or validate never freezes: on any error
    // the partial directory is removed so no unstamped store lingers.
    let result = fixture_build_inner(&args, &profile, &name, &fixture_dir).await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&fixture_dir);
    }
    result
}

async fn fixture_build_inner(
    args: &FixtureBuildArgs,
    profile: &BaseProfile,
    name: &str,
    fixture_dir: &Path,
) -> BenchResult<()> {
    let store = fixture::store_dir(fixture_dir);
    std::fs::create_dir_all(&store)?;
    let root = store
        .to_str()
        .ok_or("fixture store path is not UTF-8")?
        .to_string();

    let started = Instant::now();
    println!(
        "[fixture {name}] building: T={} N={} payload={}B diverged={} deltas={:?} index={}",
        profile.tables,
        profile.rows,
        profile.payload_bytes,
        profile.diverged_tables,
        profile.deltas,
        args.index
    );
    let schema_source = fixture::schema_source(profile.tables);
    let db = Omnigraph::init(&root, &schema_source).await?;
    let base_load_commits = fixture::load_base(&db, profile).await?;
    println!(
        "[fixture {name}] base loaded: {base_load_commits} commits, {:.1}s",
        started.elapsed().as_secs_f64()
    );

    let state = if args.index {
        let pending = db.ensure_indices().await?;
        if !pending.is_empty() {
            return Err(format!(
                "ensure_indices left {} declared index(es) pending — an indexed fixture \
                 must freeze with every declared index materialized: {pending:?}",
                pending.len()
            )
            .into());
        }
        println!(
            "[fixture {name}] ensure_indices done, {:.1}s",
            started.elapsed().as_secs_f64()
        );
        let stats = db.optimize().await?;
        println!(
            "[fixture {name}] optimize done ({} tables), {:.1}s",
            stats.len(),
            started.elapsed().as_secs_f64()
        );
        let fts_on_name = verify_index_state(&db, profile.tables).await?;
        println!(
            "[fixture {name}] verified: id BTREE Indexed on all {} tables, no unindexed \
             fragments, fts(name)={fts_on_name}",
            profile.tables
        );
        fixture::FixtureState {
            fragmentation: "bulk load in payload-bounded chunks, then optimize compaction \
                            (F1: compacted at freeze)"
                .to_string(),
            index_existence: format!(
                "BTREE on 'id' (physical row-key for name @key) on all {} node tables via \
                 ensure_indices{}; declared via the engine's key-column index path, not a \
                 .pg annotation",
                profile.tables,
                if fts_on_name {
                    ", plus the engine-implied FTS on 'name' (@key String)"
                } else {
                    ""
                }
            ),
            index_freshness: "fresh (F3): optimize run after index build; verified at freeze — \
                              id BTREE coverage Indexed on every table, no unindexed fragments"
                .to_string(),
            deletion_history: "none before freeze (F4 stub)".to_string(),
            compaction_recency: "optimize (compaction + reindex) run immediately before freeze \
                                 (F5: recent)"
                .to_string(),
            index_level: IndexLevel::BtreeFresh,
            base_load_commits,
        }
    } else {
        fixture::FixtureState {
            fragmentation: "fresh bulk load in payload-bounded chunks, no aging (F1 stub)"
                .to_string(),
            index_existence: "none declared (F2 low end)".to_string(),
            index_freshness: "n/a — no indexes (F3 stub)".to_string(),
            deletion_history: "none before freeze (F4 stub)".to_string(),
            compaction_recency: "optimize never run (F5 stub)".to_string(),
            index_level: IndexLevel::None,
            base_load_commits,
        }
    };

    // Validation pass (RFC 0039: "a fixture is validated once, before
    // anything is ever measured against it"): row counts per table against
    // the spec, then a content digest of the built store into the stamp.
    // The index-coverage check above is part of the same pass for --index
    // builds. This builder fetches nothing, so there are no artifact digests
    // to pin — recorded as such rather than skipped silently.
    let row_counts = validate_row_counts(&db, profile).await?;
    println!(
        "[fixture {name}] validated: {} tables x {} rows",
        profile.tables, profile.rows
    );
    drop(db);
    let (store_files, store_bytes, content_digest_sha256) = fixture::digest_store_dir(&store)?;
    let mut checks = vec!["row counts per table equal the spec's N".to_string()];
    if args.index {
        checks.push(
            "declared indexes present and covering (id BTREE Indexed, no unindexed fragments)"
                .to_string(),
        );
    }
    checks.push("no fetched artifacts (generated data; nothing to digest-pin)".to_string());
    checks.push("content digest of the built store recorded".to_string());
    let validation = ValidationStamp {
        validated_unix_seconds: unix_now(),
        checks,
        row_counts,
        store_files,
        store_bytes,
        content_digest_sha256,
    };

    let build_seconds = started.elapsed().as_secs_f64();
    let manifest = FixtureManifest {
        manifest_version: fixture::MANIFEST_VERSION,
        fixture_name: name.to_string(),
        builder_version: fixture::BUILDER_VERSION,
        built_unix_seconds: unix_now(),
        engine_commit: source_commit(),
        build_seconds,
        profile: profile.clone(),
        column_shape: "scalars-only (String key, String cohort, I32, String payload)".to_string(),
        state,
        validation: Some(validation),
    };
    fixture::write_manifest_atomic(fixture_dir, &manifest)?;
    println!(
        "[fixture {name}] validated + frozen at {} in {:.1}s",
        fixture_dir.display(),
        build_seconds
    );
    Ok(())
}

/// Row counts per table must equal the spec's N. Returns the observed map
/// for the validation stamp.
async fn validate_row_counts(
    db: &Omnigraph,
    profile: &BaseProfile,
) -> BenchResult<BTreeMap<String, usize>> {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await?;
    let mut counts = BTreeMap::new();
    for k in 0..profile.tables {
        let table_key = fixture::table_key(k);
        let actual = snapshot.open(&table_key).await?.count_rows(None).await?;
        if actual != profile.rows {
            return Err(format!(
                "validation failed: {table_key} holds {actual} rows, spec says {} — \
                 this fixture never freezes",
                profile.rows
            )
            .into());
        }
        counts.insert(table_key, actual);
    }
    Ok(counts)
}

/// F2/F3 verification (index existence and freshness): the id BTREE must
/// exist with full fragment coverage on every node table (the condition the
/// indexed merge arms need). Returns whether the engine-implied FTS on `name`
/// is present everywhere.
async fn verify_index_state(db: &Omnigraph, tables: usize) -> BenchResult<bool> {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await?;
    let mut fts_on_name = true;
    for k in 0..tables {
        let table_key = fixture::table_key(k);
        let table = snapshot.open(&table_key).await?;
        if !table.has_btree_index("id").await? {
            return Err(format!("{table_key}: no BTREE on 'id' after ensure_indices").into());
        }
        match table.index_coverage("id").await? {
            IndexCoverage::Indexed => {}
            IndexCoverage::Degraded { reason } => {
                return Err(format!(
                    "{table_key}: id BTREE coverage degraded after optimize: {reason}"
                )
                .into());
            }
        }
        if table.has_unindexed_fragments().await? {
            return Err(format!("{table_key}: unindexed fragments remain after optimize").into());
        }
        fts_on_name &= table.has_fts_index("name").await?;
    }
    Ok(fts_on_name)
}

/// Stamp an existing stamp-less fixture: validate a copy of the frozen
/// store, digest the untouched original, write the stamp into its manifest
/// (atomically: temp file + rename, never an in-place overwrite). An
/// already-stamped fixture is refused — a stamp is minted once, and silently
/// re-stamping would move `validated_unix_seconds` under a frozen store.
pub async fn fixture_validate(dir: PathBuf) -> BenchResult<()> {
    refuse_debug_build("stamping a fixture")?;
    let mut manifest = fixture::load_manifest(&dir)?;
    if let Some(stamp) = &manifest.validation {
        return Err(format!(
            "fixture {} is already stamped (validated_unix_seconds {}) — to force \
             re-validation, delete the manifest's `validation` block (or rebuild the \
             fixture) and rerun `fixture validate`",
            manifest.fixture_name, stamp.validated_unix_seconds
        )
        .into());
    }
    let store = fixture::store_dir(&dir);
    // Open a copy: the engine's open path may write (recovery sweep), and the
    // frozen bytes must stay exactly what the digest stamps.
    let scratch = tempfile::tempdir()?;
    let (files, bytes) = fixture::copy_dir_recursive(&store, scratch.path())?;
    println!(
        "[fixture {}] validating a copy: {} files, {:.1} MiB",
        manifest.fixture_name,
        files,
        bytes as f64 / (1024.0 * 1024.0)
    );
    let root = scratch
        .path()
        .to_str()
        .ok_or("scratch path is not UTF-8")?
        .to_string();
    let db = Omnigraph::open(&root).await?;
    let row_counts = validate_row_counts(&db, &manifest.profile).await?;
    let mut checks = vec!["row counts per table equal the spec's N".to_string()];
    if manifest.state.index_level == IndexLevel::BtreeFresh {
        verify_index_state(&db, manifest.profile.tables).await?;
        checks.push(
            "declared indexes present and covering (id BTREE Indexed, no unindexed fragments)"
                .to_string(),
        );
    }
    drop(db);
    checks.push("no fetched artifacts (generated data; nothing to digest-pin)".to_string());
    checks.push("content digest of the frozen store recorded".to_string());
    let (store_files, store_bytes, content_digest_sha256) = fixture::digest_store_dir(&store)?;
    manifest.validation = Some(ValidationStamp {
        validated_unix_seconds: unix_now(),
        checks,
        row_counts,
        store_files,
        store_bytes,
        content_digest_sha256,
    });
    manifest.manifest_version = fixture::MANIFEST_VERSION;
    fixture::write_manifest_atomic(&dir, &manifest)?;
    println!(
        "[fixture {}] validated + stamped (manifest v{})",
        manifest.fixture_name,
        fixture::MANIFEST_VERSION
    );
    Ok(())
}
