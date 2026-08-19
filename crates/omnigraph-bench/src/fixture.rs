//! Fixture builder for the branch-control micro-benchmark (RFC 0039 micro
//! profile, Data + State classes).
//!
//! Builds a fresh multi-table store at chosen content axes (T tables, N rows
//! per table, all scalar columns), then diverges a source/target branch pair
//! with a planned mixed delta (updates + deletes + inserts on both sides,
//! disjoint row sets) so `branch_merge` takes the general three-way
//! RewriteMerged route — never the fast-forward or proven-pure-insert
//! shortcuts, and never a genuine row conflict.
//!
//! The planned mutation sets are baked into the base data as a `cohort`
//! column, so one GQ mutation per (side, table, kind) diverges thousands of
//! rows in one commit instead of one commit per row. Cohort values are
//! delta-namespaced (`d50_src_upd`, `d5000_tgt_del`, ..., `keep`): a base
//! store carries disjoint pre-tagged row sets for EVERY delta in its
//! [`BaseProfile::deltas`] list, which is what lets one frozen fixture serve
//! a whole delta sweep.
//!
//! Frozen fixtures (`fixture build` subcommand): the built store is kept in
//! `<fixtures-root>/<derived-name>/store/` beside a `fixture-manifest.json`
//! recording the full Data+State tuple, engine commit, and build metadata.
//! `run --fixture <dir>` copies the store to a per-run tempdir and runs
//! against the copy, so the frozen bytes are never mutated.

use std::error::Error;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use sha2::{Digest, Sha256};

/// The engine's keyed-write cap per table per transaction
/// (`KEYED_WRITE_MAX_ROWS` / `KEYED_WRITE_MAX_BYTES` in the engine).
const KEYED_WRITE_MAX_ROWS: usize = 8192;
const KEYED_WRITE_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Serialized-JSONL bytes one base row carries beyond its payload column
/// (type, name, cohort, val, JSON syntax) — a deliberate overestimate so the
/// derived chunk size errs small.
const ROW_OVERHEAD_BYTES: usize = 160;

/// Rows per table per `load` call, derived from the payload size: the chunk's
/// byte total stays at half the engine's 32 MiB keyed-write cap, and the row
/// count at half the 8,192-row cap, whichever binds first. Loads are issued
/// per table (one table per `load` call), so the chunk bound is also the
/// bound on any single in-memory chunk string — a 32 KiB payload at T=140
/// can breach neither the cap nor memory.
pub fn load_chunk_rows(payload_bytes: usize) -> usize {
    let row_bytes = payload_bytes + ROW_OVERHEAD_BYTES;
    (KEYED_WRITE_MAX_BYTES / 2 / row_bytes).clamp(1, KEYED_WRITE_MAX_ROWS / 2)
}

/// Bumped when the builder's baked base content changes shape (cohort scheme,
/// row naming, chunking) so a stale frozen fixture is detectable.
/// v2: per-table load chunking with payload-derived chunk rows.
pub const BUILDER_VERSION: u32 = 2;

pub const MANIFEST_FILE: &str = "fixture-manifest.json";
pub const STORE_SUBDIR: &str = "store";

pub type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// The Data(+divergence-shape) axes one base store is built from. A store
/// built from a profile can serve any single-delta run whose delta is in
/// `deltas` and whose diverged-table count equals `diverged_tables` (both are
/// baked into the cohort tags).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BaseProfile {
    /// T — node-table count (v0 approximates "T node/edge types" as T node
    /// types; edges are deferred).
    pub tables: usize,
    /// N — rows per table in the base load.
    pub rows: usize,
    /// Bytes of filler in the scalar `payload` string column per row.
    pub payload_bytes: usize,
    /// How many tables any delta touches, on BOTH sides (overlapping-table
    /// divergence). Baked into which tables carry tagged cohorts.
    pub diverged_tables: usize,
    /// Every delta (rows per side) this base is pre-tagged for, ascending.
    pub deltas: Vec<usize>,
}

impl BaseProfile {
    pub fn new(
        tables: usize,
        rows: usize,
        payload_bytes: usize,
        diverged_tables: usize,
        mut deltas: Vec<usize>,
    ) -> BenchResult<Self> {
        if tables == 0 || rows == 0 {
            return Err("tables and rows must both be >= 1".into());
        }
        if diverged_tables == 0 || diverged_tables > tables {
            return Err(format!(
                "diverged_tables must be between 1 and tables (T = {tables}), got {diverged_tables}"
            )
            .into());
        }
        deltas.sort_unstable();
        deltas.dedup();
        if deltas.is_empty() || deltas[0] == 0 {
            return Err("deltas must be a non-empty list of values >= 1".into());
        }
        let profile = Self {
            tables,
            rows,
            payload_bytes,
            diverged_tables,
            deltas,
        };
        // Every delta's four tagged sets live side by side in the base rows;
        // the busiest table must still fit them all.
        for k in 0..profile.diverged_tables {
            let tagged = profile.tagged_rows(k);
            if tagged > profile.rows {
                return Err(format!(
                    "table {k}: the delta list needs {tagged} pre-tagged rows but N = {} — raise rows or trim deltas",
                    profile.rows
                )
                .into());
            }
        }
        Ok(profile)
    }

    /// Per-side operation counts for `delta` on diverged table `k`.
    fn table_plan(&self, delta: usize, k: usize) -> TablePlan {
        let side_split = split_delta(delta);
        let per_table = |split: KindSplit| KindSplit {
            updates: share(split.updates, self.diverged_tables, k),
            deletes: share(split.deletes, self.diverged_tables, k),
            inserts: share(split.inserts, self.diverged_tables, k),
        };
        TablePlan {
            src: per_table(side_split),
            tgt: per_table(side_split),
        }
    }

    /// Cohort segments of diverged table `k`: `(end_row_exclusive, cohort)`
    /// in layout order. Rows past the last end are cohort `keep`.
    fn cohort_segments(&self, k: usize) -> Vec<(usize, String)> {
        let mut segments = Vec::with_capacity(self.deltas.len() * 4);
        let mut end = 0usize;
        for &delta in &self.deltas {
            let tp = self.table_plan(delta, k);
            for (count, side_kind) in [
                (tp.src.updates, "src_upd"),
                (tp.src.deletes, "src_del"),
                (tp.tgt.updates, "tgt_upd"),
                (tp.tgt.deletes, "tgt_del"),
            ] {
                if count > 0 {
                    end += count;
                    segments.push((end, format!("d{delta}_{side_kind}")));
                }
            }
        }
        segments
    }

    /// Total pre-tagged (non-`keep`) rows of diverged table `k`.
    fn tagged_rows(&self, k: usize) -> usize {
        self.cohort_segments(k).last().map_or(0, |(end, _)| *end)
    }
}

/// Per-side operation counts for one delta of d rows.
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct KindSplit {
    pub updates: usize,
    pub deletes: usize,
    pub inserts: usize,
}

impl std::fmt::Display for KindSplit {
    /// Compact progress-line form: `1667u/1667d/1666i`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}u/{}d/{}i", self.updates, self.deletes, self.inserts)
    }
}

/// Split d operations into updates/deletes/inserts, updates first so every
/// d >= 1 keeps at least one update per side — an update-bearing divergence
/// can never be classified onto the proven-pure-insert shortcut.
pub fn split_delta(d: usize) -> KindSplit {
    assert!(d >= 1, "delta must be >= 1");
    let updates = d.div_ceil(3).max(1);
    let rest = d - updates;
    let deletes = rest.div_ceil(2);
    let inserts = rest - deletes;
    KindSplit {
        updates,
        deletes,
        inserts,
    }
}

/// `idx`'s share when `total` items are dealt round-robin over `parts` bins.
fn share(total: usize, parts: usize, idx: usize) -> usize {
    total / parts + usize::from(idx < total % parts)
}

/// One diverged table's planned per-side operation counts.
#[derive(Debug, Clone, Copy)]
pub struct TablePlan {
    pub src: KindSplit,
    pub tgt: KindSplit,
}

/// The divergence plan for ONE run: a base profile plus the chosen delta.
#[derive(Debug, Clone)]
pub struct FixturePlan {
    pub profile: BaseProfile,
    /// d — delta rows per side, one of `profile.deltas`.
    pub delta: usize,
    /// One entry per diverged table (tables `0..diverged_tables`).
    pub table_plans: Vec<TablePlan>,
    /// Per-side totals (the d split).
    pub side_split: KindSplit,
}

impl FixturePlan {
    pub fn new(profile: BaseProfile, delta: usize) -> BenchResult<Self> {
        if !profile.deltas.contains(&delta) {
            return Err(format!(
                "delta {delta} is not pre-tagged in this base (supported deltas: {:?})",
                profile.deltas
            )
            .into());
        }
        let table_plans: Vec<TablePlan> = (0..profile.diverged_tables)
            .map(|k| profile.table_plan(delta, k))
            .collect();
        let side_split = split_delta(delta);
        Ok(Self {
            profile,
            delta,
            table_plans,
            side_split,
        })
    }

    /// Expected row count of diverged table `k` on the target branch after a
    /// clean three-way merge (deletes from both sides applied, inserts from
    /// both sides landed).
    pub fn expected_rows_after_merge(&self, k: usize) -> usize {
        let tp = &self.table_plans[k];
        self.profile.rows - tp.src.deletes - tp.tgt.deletes + tp.src.inserts + tp.tgt.inserts
    }
}

pub fn type_name(k: usize) -> String {
    format!("BenchT{k:03}")
}

pub fn table_key(k: usize) -> String {
    format!("node:{}", type_name(k))
}

/// Generate the `.pg` schema: T node types, all scalar columns.
pub fn schema_source(tables: usize) -> String {
    let mut out = String::new();
    for k in 0..tables {
        writeln!(
            out,
            "node {} {{\n    name: String @key\n    cohort: String?\n    val: I32?\n    payload: String?\n}}\n",
            type_name(k)
        )
        .expect("writing to String cannot fail");
    }
    out
}

/// Generate the `.gq` mutation-query source for the diverged tables: one
/// cohort-filtered bulk update and one cohort-filtered bulk delete per table.
pub fn mutation_queries(diverged_tables: usize) -> String {
    let mut out = String::new();
    for k in 0..diverged_tables {
        let ty = type_name(k);
        writeln!(
            out,
            "query upd_{k}($v: I32, $c: String) {{\n    update {ty} set {{ val: $v }} where cohort = $c\n}}\n"
        )
        .expect("writing to String cannot fail");
        writeln!(
            out,
            "query del_{k}($c: String) {{\n    delete {ty} where cohort = $c\n}}\n"
        )
        .expect("writing to String cannot fail");
    }
    out
}

/// Which cohort base row `j` carries, given the table's precomputed cohort
/// segments. Rows are partitioned front-to-back into the planned mutation
/// sets of every supported delta; all sets are disjoint by construction, so
/// any single-delta merge is conflict-free.
fn cohort_at(segments: &[(usize, String)], j: usize) -> &str {
    for (end, cohort) in segments {
        if j < *end {
            return cohort;
        }
    }
    "keep"
}

fn jsonl_node_row(ty: &str, name: &str, cohort: &str, val: usize, payload: &str) -> String {
    serde_json::json!({
        "type": ty,
        "data": { "name": name, "cohort": cohort, "val": val as i64, "payload": payload }
    })
    .to_string()
}

/// Bulk-load the base content onto `main`: N rows into each of T tables, one
/// table per `load` call, in chunks of [`load_chunk_rows`] rows per commit
/// (bounded under the engine's keyed-write cap at any payload size). Returns
/// the number of load commits performed.
pub async fn load_base(db: &Omnigraph, profile: &BaseProfile) -> BenchResult<usize> {
    let payload = "x".repeat(profile.payload_bytes);
    let chunk_rows = load_chunk_rows(profile.payload_bytes);
    let segments_per_table: Vec<Vec<(usize, String)>> = (0..profile.diverged_tables)
        .map(|k| profile.cohort_segments(k))
        .collect();
    let mut commits = 0usize;
    for k in 0..profile.tables {
        let ty = type_name(k);
        let segments = segments_per_table.get(k);
        let mut start = 0usize;
        while start < profile.rows {
            let end = (start + chunk_rows).min(profile.rows);
            let mut chunk = String::new();
            for j in start..end {
                let cohort = segments.map_or("keep", |segments| cohort_at(segments, j));
                let name = format!("t{k:03}_r{j:07}");
                chunk.push_str(&jsonl_node_row(&ty, &name, cohort, j, &payload));
                chunk.push('\n');
            }
            db.load("main", &chunk, LoadMode::Append).await?;
            commits += 1;
            start = end;
        }
    }
    Ok(commits)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Source,
    Target,
}

impl Side {
    fn tag(self) -> &'static str {
        match self {
            Side::Source => "src",
            Side::Target => "tgt",
        }
    }
}

fn string_param(map: &mut ParamMap, key: &str, value: &str) {
    map.insert(key.to_string(), Literal::String(value.to_string()));
}

/// Apply one side's planned delta on `branch` (already forked from main):
/// per diverged table one bulk cohort update and one bulk cohort delete, then
/// the side's new rows in one (chunked) load. `rep` uniquifies insert keys
/// across repetitions.
pub async fn diverge(
    db: &Omnigraph,
    branch: &str,
    side: Side,
    plan: &FixturePlan,
    queries: &str,
    rep: usize,
) -> BenchResult<()> {
    let tag = side.tag();
    let d = plan.delta;
    for (k, tp) in plan.table_plans.iter().enumerate() {
        let split = match side {
            Side::Source => tp.src,
            Side::Target => tp.tgt,
        };
        if split.updates > 0 {
            let mut params = ParamMap::new();
            params.insert("v".to_string(), Literal::Integer(1_000_000 + rep as i64));
            string_param(&mut params, "c", &format!("d{d}_{tag}_upd"));
            db.mutate(branch, queries, &format!("upd_{k}"), &params)
                .await?;
        }
        if split.deletes > 0 {
            let mut params = ParamMap::new();
            string_param(&mut params, "c", &format!("d{d}_{tag}_del"));
            db.mutate(branch, queries, &format!("del_{k}"), &params)
                .await?;
        }
    }
    // Inserts: one table per load call, chunk rows derived from the payload
    // size (same keyed-write-cap bound as the base load).
    let payload = "y".repeat(plan.profile.payload_bytes);
    let chunk_rows = load_chunk_rows(plan.profile.payload_bytes);
    for (k, tp) in plan.table_plans.iter().enumerate() {
        let inserts = match side {
            Side::Source => tp.src.inserts,
            Side::Target => tp.tgt.inserts,
        };
        let ty = type_name(k);
        let mut start = 0usize;
        while start < inserts {
            let end = (start + chunk_rows).min(inserts);
            let mut chunk = String::new();
            for j in start..end {
                let name = format!("{tag}{rep:03}_t{k:03}_n{j:07}");
                chunk.push_str(&jsonl_node_row(&ty, &name, "new", j, &payload));
                chunk.push('\n');
            }
            db.load(branch, &chunk, LoadMode::Append).await?;
            start = end;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Frozen fixtures
// ---------------------------------------------------------------------------

/// `fixture-manifest.json`: full provenance of one frozen base store. A run
/// against the fixture embeds this whole manifest in its record's state
/// group, so the record stays reproducible from itself alone.
///
/// Manifest v2 added the [`ValidationStamp`] (RFC 0039: "a fixture is
/// validated once, before anything is ever measured against it"; a fixture
/// that fails validation never freezes, and `run --fixture` refuses a
/// manifest without the stamp); v3 added the machine-readable
/// `state.index_level`. Manifests below v3 are refused by [`load_manifest`]
/// (fixtures are disposable — rebuild); `fixture validate` stamps a v3
/// manifest whose stamp was deleted to force re-validation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FixtureManifest {
    pub manifest_version: u32,
    /// Derived name, also the fixture's directory name.
    pub fixture_name: String,
    /// [`BUILDER_VERSION`] at build time.
    pub builder_version: u32,
    pub built_unix_seconds: u64,
    /// `git rev-parse HEAD` of the engine tree the builder was built from.
    pub engine_commit: String,
    pub build_seconds: f64,
    pub profile: BaseProfile,
    pub column_shape: String,
    pub state: FixtureState,
    /// The validation stamp; absent only on v1 manifests, which `run` refuses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<ValidationStamp>,
}

/// Result of the fixture validation pass, stamped into the manifest at build
/// (or by `fixture validate` for pre-stamp fixtures). `run --fixture`
/// re-digests the frozen store and refuses on mismatch, so the stamp is
/// load-bearing, not decorative.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ValidationStamp {
    pub validated_unix_seconds: u64,
    /// The checks that ran, in order (row counts, index coverage, digest, ...).
    pub checks: Vec<String>,
    /// Observed row count per table key (each must equal the spec's N).
    pub row_counts: std::collections::BTreeMap<String, usize>,
    pub store_files: u64,
    pub store_bytes: u64,
    /// SHA-256 over the store directory (sorted relative paths + contents).
    pub content_digest_sha256: String,
}

/// Machine-readable F2/F3 level of a fixture (F2 = index existence, F3 =
/// index freshness). The prose fields of [`FixtureState`] carry the detail;
/// this enum is what code branches on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IndexLevel {
    /// No declared indexes (F2 low end).
    #[default]
    None,
    /// Declared key indexes built and freshly optimized (F2/F3 high end).
    BtreeFresh,
}

/// F1–F5 state axes of the frozen store, in the run-record vocabulary.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FixtureState {
    pub fragmentation: String,
    pub index_existence: String,
    pub index_freshness: String,
    pub deletion_history: String,
    pub compaction_recency: String,
    /// Machine-readable F2/F3 level; the prose fields above are detail only,
    /// never gated on. Defaults (pre-v3 manifests) to `None`; those manifests
    /// are refused by the manifest-version floor in `load_manifest`, so the
    /// default never reaches a run.
    #[serde(default)]
    pub index_level: IndexLevel,
    pub base_load_commits: usize,
}

/// Derived directory name for a profile, e.g. `fx-t8-n100k-scalars-btree-fresh`.
/// A non-default diverged-table count (anything but the historical
/// `min(4, T)` m3 shape) is part of the tuple and gets a `-div<D>` suffix so
/// e.g. an all-diverged base cannot collide with the diverged-4 base of the
/// same T/N.
pub fn derived_fixture_name(profile: &BaseProfile, index: bool) -> String {
    let rows = if profile.rows.is_multiple_of(1000) {
        format!("{}k", profile.rows / 1000)
    } else {
        profile.rows.to_string()
    };
    let div = if profile.diverged_tables == profile.tables.min(4) {
        String::new()
    } else {
        format!("-div{}", profile.diverged_tables)
    };
    format!(
        "fx-t{}-n{}-scalars-{}{div}",
        profile.tables,
        rows,
        if index { "btree-fresh" } else { "noindex" }
    )
}

pub fn store_dir(fixture_dir: &Path) -> PathBuf {
    fixture_dir.join(STORE_SUBDIR)
}

pub fn manifest_path(fixture_dir: &Path) -> PathBuf {
    fixture_dir.join(MANIFEST_FILE)
}

pub fn load_manifest(fixture_dir: &Path) -> BenchResult<FixtureManifest> {
    let path = manifest_path(fixture_dir);
    let text =
        std::fs::read_to_string(&path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    let manifest: FixtureManifest =
        serde_json::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))?;
    if manifest.builder_version != BUILDER_VERSION {
        return Err(format!(
            "fixture {} was built by builder v{}, this binary is v{BUILDER_VERSION} — rebuild the fixture",
            manifest.fixture_name, manifest.builder_version
        )
        .into());
    }
    // Pre-v3 manifests predate `state.index_level`; builder v2 coexisted with
    // manifest v2, so the builder-version check above does not catch them.
    if manifest.manifest_version < MANIFEST_VERSION {
        return Err(format!(
            "fixture {} has manifest v{}, this binary reads only v{MANIFEST_VERSION} — \
             fixtures are disposable; rebuild it",
            manifest.fixture_name, manifest.manifest_version
        )
        .into());
    }
    if manifest.manifest_version > MANIFEST_VERSION {
        return Err(format!(
            "fixture {} has manifest v{}, this binary reads up to v{MANIFEST_VERSION}",
            manifest.fixture_name, manifest.manifest_version
        )
        .into());
    }
    if !store_dir(fixture_dir).is_dir() {
        return Err(format!(
            "fixture {} has no {STORE_SUBDIR}/ directory",
            fixture_dir.display()
        )
        .into());
    }
    Ok(manifest)
}

/// Current manifest version (2 = validation stamp present; 3 = machine-
/// readable `state.index_level`), and the only version `load_manifest`
/// accepts.
pub const MANIFEST_VERSION: u32 = 3;

/// The state tag a fixture name / point name carries for its F2/F3 level.
pub fn state_tag(level: IndexLevel) -> &'static str {
    match level {
        IndexLevel::BtreeFresh => "btree-fresh",
        IndexLevel::None => "noindex",
    }
}

/// Write the manifest atomically: a temp file in the fixture directory, then
/// a rename over the final path. A crash mid-write can leave a stray temp
/// file, never a torn `fixture-manifest.json`.
pub fn write_manifest_atomic(fixture_dir: &Path, manifest: &FixtureManifest) -> BenchResult<()> {
    let path = manifest_path(fixture_dir);
    let tmp = fixture_dir.join(format!("{MANIFEST_FILE}.tmp"));
    std::fs::write(&tmp, serde_json::to_string_pretty(manifest)?)
        .map_err(|e| format!("writing {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .map_err(|e| format!("renaming {} over {}: {e}", tmp.display(), path.display()))?;
    Ok(())
}

/// Load a fixture manifest and enforce the run-side contract: the manifest
/// must carry a validation stamp, and the frozen store must re-digest to the
/// stamped digest (RFC 0039: run records may reference only validated
/// fixtures; a mutated frozen store cannot be measured against).
pub fn load_validated(dir: &Path) -> BenchResult<FixtureManifest> {
    let manifest = load_manifest(dir)?;
    let Some(stamp) = manifest.validation.as_ref() else {
        return Err(format!(
            "fixture {} has no validation stamp (RFC 0039: run records may reference \
             only validated fixtures) — rebuild it, or stamp it with \
             `fixture validate {}`",
            manifest.fixture_name,
            dir.display()
        )
        .into());
    };
    let (_, _, digest) = digest_store_dir(&store_dir(dir))?;
    if digest != stamp.content_digest_sha256 {
        return Err(format!(
            "fixture {}: frozen store digest mismatch (manifest {}, on-store {digest}) — \
             the store was modified after validation; rebuild or re-validate",
            manifest.fixture_name, stamp.content_digest_sha256
        )
        .into());
    }
    Ok(manifest)
}

/// Digest a per-run copy of the frozen store against the manifest's stamp.
/// Runs once per copy (each point copies afresh): the entry check in `run`
/// covers the moment the fixture is loaded, this one covers the bytes each
/// point actually opens.
pub fn verify_copy_digest(copy_root: &Path, manifest: &FixtureManifest) -> BenchResult<()> {
    let stamp = manifest
        .validation
        .as_ref()
        .ok_or("fixture manifest lost its validation stamp between load and copy")?;
    let (_, _, digest) = digest_store_dir(copy_root)?;
    if digest != stamp.content_digest_sha256 {
        return Err(format!(
            "fixture {}: per-run store copy digests {digest}, manifest stamps {} — \
             the frozen store changed after this run started (or the copy is unfaithful); \
             refusing to measure against it",
            manifest.fixture_name, stamp.content_digest_sha256
        )
        .into());
    }
    Ok(())
}

/// SHA-256 content digest of a store directory: every regular file in sorted
/// relative-path order, hashing `relpath \0 len \0 bytes`. Returns
/// (files, bytes, hex digest). Identical trees digest identically on any
/// machine — the hashed relative path is component-joined with '/' so the
/// platform separator never enters the digest (a unix tree keeps its
/// historical digest byte for byte). Non-UTF-8 path components hash their
/// lossy replacement, so two names differing only in invalid bytes could
/// collide — accepted for generated fixture trees, which are ASCII. A mutated
/// frozen store cannot pass `run --fixture`.
pub fn digest_store_dir(store: &Path) -> BenchResult<(u64, u64, String)> {
    fn walk(dir: &Path, files: &mut Vec<PathBuf>) -> BenchResult<()> {
        for entry in
            std::fs::read_dir(dir).map_err(|e| format!("reading {}: {e}", dir.display()))?
        {
            let entry = entry?;
            // DirEntry::file_type never follows symlinks (same refusal as
            // copy_dir_recursive): a symlink must fail the digest, not be
            // silently hashed as whatever it points at.
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_dir() {
                walk(&path, files)?;
            } else if file_type.is_file() {
                files.push(path);
            } else {
                return Err(format!(
                    "unsupported entry (symlink?) in fixture store: {}",
                    path.display()
                )
                .into());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(store, &mut files)?;
    files.sort();
    let mut hasher = Sha256::new();
    let mut total_bytes = 0u64;
    for path in &files {
        let rel = path
            .strip_prefix(store)
            .expect("walked path is under the store root");
        // '/'-joined components: on unix this reproduces the path string
        // exactly (frozen digests stay valid); on Windows it replaces the
        // '\\' separator without touching separator bytes inside a name.
        let rel_portable = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        let bytes = std::fs::read(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
        hasher.update(rel_portable.as_bytes());
        hasher.update([0]);
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update([0]);
        hasher.update(&bytes);
        total_bytes += bytes.len() as u64;
    }
    let digest = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Ok((files.len() as u64, total_bytes, digest))
}

/// Recursive copy of `src` into `dst` (created). Returns (files, bytes).
pub fn copy_dir_recursive(src: &Path, dst: &Path) -> BenchResult<(u64, u64)> {
    std::fs::create_dir_all(dst)?;
    let mut files = 0u64;
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(src).map_err(|e| format!("reading {}: {e}", src.display()))? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            let (f, b) = copy_dir_recursive(&from, &to)?;
            files += f;
            bytes += b;
        } else if file_type.is_file() {
            bytes += std::fs::copy(&from, &to)
                .map_err(|e| format!("copying {}: {e}", from.display()))?;
            files += 1;
        } else {
            return Err(format!(
                "unsupported entry (symlink?) in fixture store: {}",
                from.display()
            )
            .into());
        }
    }
    Ok((files, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_delta_cohorts_are_disjoint_and_fit() {
        let profile = BaseProfile::new(8, 4000, 64, 4, vec![1, 50, 5000]).unwrap();
        // Busiest table's tagged rows must fit N and lay out without overlap.
        for k in 0..profile.diverged_tables {
            let segments = profile.cohort_segments(k);
            let mut prev = 0usize;
            for (end, cohort) in &segments {
                assert!(*end > prev, "segment {cohort} is empty or out of order");
                prev = *end;
            }
            assert!(prev <= profile.rows);
        }
        // A per-delta plan's tagged counts match the segments carrying its tag.
        let plan = FixturePlan::new(profile.clone(), 50).unwrap();
        let tp = plan.table_plans[0];
        let segments = profile.cohort_segments(0);
        let seg_len = |cohort: &str| {
            let mut prev = 0usize;
            for (end, c) in &segments {
                if c == cohort {
                    return end - prev;
                }
                prev = *end;
            }
            0
        };
        assert_eq!(seg_len("d50_src_upd"), tp.src.updates);
        assert_eq!(seg_len("d50_src_del"), tp.src.deletes);
        assert_eq!(seg_len("d50_tgt_upd"), tp.tgt.updates);
        assert_eq!(seg_len("d50_tgt_del"), tp.tgt.deletes);
    }

    #[test]
    fn unsupported_delta_is_refused() {
        let profile = BaseProfile::new(2, 1000, 8, 2, vec![1, 50]).unwrap();
        assert!(FixturePlan::new(profile, 7).is_err());
    }

    #[test]
    fn out_of_bounds_diverged_tables_is_a_hard_error() {
        for (tables, diverged) in [(8, 0), (8, 9), (1, 2)] {
            let err = BaseProfile::new(tables, 1000, 8, diverged, vec![1])
                .expect_err("out-of-bounds diverged_tables must be refused, never clamped");
            let msg = err.to_string();
            assert!(
                msg.contains("diverged_tables must be between 1 and tables"),
                "error must name the bound, got: {msg}"
            );
        }
    }

    #[test]
    fn chunk_rows_stay_under_the_keyed_write_cap() {
        // (payload_bytes, expectation on the derived chunk)
        for payload in [0usize, 64, 4096, 32 * 1024, 10 * 1024 * 1024] {
            let rows = load_chunk_rows(payload);
            assert!(
                rows >= 1,
                "payload {payload}: chunk must hold at least one row"
            );
            assert!(
                rows <= KEYED_WRITE_MAX_ROWS / 2,
                "payload {payload}: {rows} rows exceeds half the row cap"
            );
            assert!(
                rows * (payload + ROW_OVERHEAD_BYTES) <= KEYED_WRITE_MAX_BYTES,
                "payload {payload}: {rows} rows breach the 32 MiB keyed-write cap"
            );
        }
        // The historical default shape keeps its historical chunk size.
        assert_eq!(load_chunk_rows(64), 4096);
    }

    fn test_manifest(name: &str, stamp: Option<ValidationStamp>) -> FixtureManifest {
        FixtureManifest {
            manifest_version: MANIFEST_VERSION,
            fixture_name: name.to_string(),
            builder_version: BUILDER_VERSION,
            built_unix_seconds: 0,
            engine_commit: "test".to_string(),
            build_seconds: 0.0,
            profile: BaseProfile::new(1, 10, 8, 1, vec![1]).unwrap(),
            column_shape: "test".to_string(),
            state: FixtureState {
                fragmentation: "test".to_string(),
                index_existence: "none declared (F2 low end)".to_string(),
                index_freshness: "n/a".to_string(),
                deletion_history: "none".to_string(),
                compaction_recency: "never".to_string(),
                index_level: IndexLevel::None,
                base_load_commits: 1,
            },
            validation: stamp,
        }
    }

    /// Builder v2 coexisted with manifest v2, so the version floor (not the
    /// builder-version check) must refuse pre-v3 manifests.
    #[test]
    fn pre_v3_manifests_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let fixture_dir = dir.path();
        std::fs::create_dir_all(store_dir(fixture_dir)).unwrap();
        let mut manifest = test_manifest("fx-old", None);
        manifest.manifest_version = 2;
        write_manifest_atomic(fixture_dir, &manifest).unwrap();
        let err = load_manifest(fixture_dir).expect_err("a v2 manifest must be refused");
        assert!(err.to_string().contains("manifest v2"), "got: {err}");
        assert!(err.to_string().contains("rebuild"), "got: {err}");
    }

    #[test]
    fn run_side_contract_refuses_unstamped_and_mutated_fixtures() {
        let dir = tempfile::tempdir().unwrap();
        let fixture_dir = dir.path();
        let store = store_dir(fixture_dir);
        std::fs::create_dir_all(&store).unwrap();
        std::fs::write(store.join("data.bin"), b"frozen bytes").unwrap();

        // Unstamped: refused with the stamp instruction.
        write_manifest_atomic(fixture_dir, &test_manifest("fx-test", None)).unwrap();
        let err = load_validated(fixture_dir).expect_err("unstamped fixture must be refused");
        assert!(err.to_string().contains("no validation stamp"));

        // Stamped and untouched: accepted.
        let (files, bytes, digest) = digest_store_dir(&store).unwrap();
        let stamp = ValidationStamp {
            validated_unix_seconds: 0,
            checks: vec!["test".to_string()],
            row_counts: std::collections::BTreeMap::new(),
            store_files: files,
            store_bytes: bytes,
            content_digest_sha256: digest,
        };
        write_manifest_atomic(fixture_dir, &test_manifest("fx-test", Some(stamp))).unwrap();
        load_validated(fixture_dir).expect("stamped, unmodified fixture must load");

        // Mutated after stamping: refused with the digest mismatch.
        std::fs::write(store.join("data.bin"), b"tampered bytes").unwrap();
        let err = load_validated(fixture_dir).expect_err("mutated store must be refused");
        assert!(err.to_string().contains("digest mismatch"));
    }

    #[test]
    fn derived_names_match_convention() {
        let p = BaseProfile::new(8, 100_000, 64, 4, vec![1, 50, 5000]).unwrap();
        assert_eq!(
            derived_fixture_name(&p, true),
            "fx-t8-n100k-scalars-btree-fresh"
        );
        let p = BaseProfile::new(8, 4000, 64, 4, vec![1, 50, 5000]).unwrap();
        assert_eq!(derived_fixture_name(&p, false), "fx-t8-n4k-scalars-noindex");
        // Non-default diverged count is part of the name (all-diverged bases
        // must not collide with the diverged-4 base of the same T/N).
        let p = BaseProfile::new(8, 4000, 64, 8, vec![24]).unwrap();
        assert_eq!(
            derived_fixture_name(&p, false),
            "fx-t8-n4k-scalars-noindex-div8"
        );
        let p = BaseProfile::new(140, 4000, 64, 140, vec![420]).unwrap();
        assert_eq!(
            derived_fixture_name(&p, false),
            "fx-t140-n4k-scalars-noindex-div140"
        );
    }

    #[test]
    fn store_digest_is_deterministic_and_content_sensitive() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("data");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(dir.path().join("a.txt"), b"alpha").unwrap();
        std::fs::write(sub.join("b.bin"), b"beta").unwrap();
        let (files, bytes, first) = digest_store_dir(dir.path()).unwrap();
        assert_eq!(files, 2);
        assert_eq!(bytes, 9);
        let (_, _, second) = digest_store_dir(dir.path()).unwrap();
        assert_eq!(first, second, "same tree must digest identically");
        std::fs::write(sub.join("b.bin"), b"BETA").unwrap();
        let (_, _, third) = digest_store_dir(dir.path()).unwrap();
        assert_ne!(first, third, "a mutated store must change the digest");
    }

    /// Pins the digest formula (`relpath \0 len \0 bytes`, '/'-separated
    /// relative paths): frozen fixtures stamped before the separator
    /// normalization must keep digesting to the same value.
    #[test]
    fn digest_formula_is_stable_for_slash_separated_paths() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("data");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(dir.path().join("a.txt"), b"alpha").unwrap();
        std::fs::write(sub.join("b.bin"), b"beta").unwrap();
        let mut hasher = Sha256::new();
        for (rel, contents) in [("a.txt", b"alpha" as &[u8]), ("data/b.bin", b"beta")] {
            hasher.update(rel.as_bytes());
            hasher.update([0]);
            hasher.update((contents.len() as u64).to_le_bytes());
            hasher.update([0]);
            hasher.update(contents);
        }
        let expected = hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let (_, _, actual) = digest_store_dir(dir.path()).unwrap();
        assert_eq!(actual, expected, "digest formula drifted");
    }

    #[cfg(unix)]
    #[test]
    fn digest_refuses_symlinks_without_following_them() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"alpha").unwrap();
        std::os::unix::fs::symlink(dir.path().join("a.txt"), dir.path().join("link")).unwrap();
        let err = digest_store_dir(dir.path()).expect_err("a symlink must fail the digest");
        assert!(err.to_string().contains("symlink"));
    }
}
