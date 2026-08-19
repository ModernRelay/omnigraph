//! The `run` subcommand: acquire a base store (inline build or a validated
//! frozen fixture copy), run one merge scenario per point under one declared
//! warmth regime, and write one schema-validated run record per point.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, ValueEnum};
use lance::io::WrappingObjectStore;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::instrumentation::{
    CountingStorageAdapter, MergeWriteProbes, QueryIoProbes, StorageReadCounts,
    with_merge_write_probes, with_query_io_probes,
};
use omnigraph::storage::storage_for_uri;

use crate::counting::CallCounter;
use crate::fixture::{self, BaseProfile, BenchResult, FixtureManifest, FixturePlan, Side};
use crate::record::{
    ARRIVAL_UNSCHEDULED_SINGLE_SHOT, ATTRIBUTION_PER_PHASE_ON, ControlCalls, DataAxes,
    EnvironmentAxes, FLOOR_VERSION, FloorPoint, NoiseFloor, POINT_NAME_FORMAT,
    PROVENANCE_SYNTHETIC, PhaseStats, ProtocolAxes, RECORD_VERSION, RowCheck, RunRecord,
    RunResults, RunSpec, StateAxes, StorageCalls, SutBlock, WallClockStats, WarmthDeclaration,
    WorkloadAxes, WritePathCounters, default_margin, derive_profile, pair_delta_pct,
    percentile_f64, percentile_u64, tail_support,
};
use crate::{refuse_debug_build, schema, source_commit, unix_now};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Scenario {
    /// Three-way diverged mixed merge, delta sweep at fixed N.
    M3,
    /// Composite headline: every table diverged, small delta, one merge.
    M5,
}

impl Scenario {
    pub fn id(self) -> &'static str {
        match self {
            Scenario::M3 => "m3",
            Scenario::M5 => "m5",
        }
    }
}

/// The declared warmth regime (RFC 0039 Environment class; rule 3 requires
/// exactly one per cell, declared in the record).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Warmth {
    /// Fresh process per measured repetition (spawned by this binary).
    Cold,
    /// Discarded warm-up repetition(s), then measurement.
    Warm,
    /// Warm-up, then the engine handle is dropped and reopened (engine +
    /// Lance session caches invalidated), then measurement.
    PostInvalidation,
}

impl Warmth {
    pub fn id(self) -> &'static str {
        match self {
            Warmth::Cold => "cold",
            Warmth::Warm => "warm",
            Warmth::PostInvalidation => "post-invalidation",
        }
    }
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Scenario to run.
    #[arg(long, value_enum)]
    scenario: Scenario,
    /// T — node-table count (default 12; large center: 140).
    /// Incompatible with --fixture (the fixture pins the Data axes).
    #[arg(long)]
    tables: Option<usize>,
    /// N — base rows per table (default 10000; center: 100000).
    /// Incompatible with --fixture.
    #[arg(long)]
    rows: Option<usize>,
    /// d values to sweep, comma-separated (delta rows per side).
    /// Defaults: m3 = 1,50,5000; m5 = 50.
    #[arg(long, value_delimiter = ',')]
    delta: Vec<usize>,
    /// Tables the delta touches on both sides. Defaults: m3 = min(4, T);
    /// m5 = T (m5 diverges every table; any other value is refused).
    /// Incompatible with --fixture.
    #[arg(long)]
    diverged_tables: Option<usize>,
    /// Measured merges per point (warm-ups are extra; see --warmup-reps).
    #[arg(long, default_value_t = 5)]
    reps: usize,
    /// Warmth regime, declared per cell (RFC 0039 rule 3).
    #[arg(long, value_enum, default_value_t = Warmth::Warm)]
    warmth: Warmth,
    /// Warm-up repetitions run and discarded before measurement (warm and
    /// post-invalidation regimes; cold has none).
    #[arg(long, default_value_t = 1)]
    warmup_reps: usize,
    /// Run every point twice at equal spec and SUT (the A/A pair) and write
    /// a noise-floor.json beside the records for `diff --floor` (rule 7).
    #[arg(long)]
    aa: bool,
    /// Filler bytes in the scalar payload column per row (default 64).
    /// Incompatible with --fixture.
    #[arg(long)]
    payload_bytes: Option<usize>,
    /// Directory for run-record JSON files (created if absent).
    #[arg(long)]
    out: PathBuf,
    /// Store root. Absent: a fresh local tempdir per run
    /// (backend "local-fs-tempdir"). `s3://bucket/prefix`: an S3-compatible
    /// backend (MinIO/RustFS via AWS_* env; see README) — a unique sub-prefix
    /// is appended per run.
    #[arg(long)]
    root_uri: Option<String>,
    /// Frozen fixture directory (from `fixture build`). Must carry a
    /// validation stamp; the frozen store is re-digested and refused on
    /// mismatch (again per per-point copy). The store is copied to a fresh
    /// per-point tempdir and the run opens the copy; Data+State axes come
    /// from the fixture's manifest.
    #[arg(long)]
    fixture: Option<PathBuf>,
    /// Free-form label stored in the record ("baseline-main", "after-O3").
    #[arg(long)]
    label: Option<String>,
    /// Internal: run exactly one cold measured repetition with this index
    /// (spawned by the cold parent; not for direct use).
    #[arg(long, hide = true)]
    internal_cold_rep: Option<usize>,
}

/// Engine configuration as data (RFC 0039 SUT block): every `OMNIGRAPH_*`
/// environment variable set at run time. An empty map means no flag was set,
/// recorded as such — never inferred from a prose label. Values of
/// secret-looking variables are stored redacted ([`is_secret_config_name`]):
/// records and reports are shareable artifacts, and the server's bearer-token
/// variables (`OMNIGRAPH_SERVER_BEARER_TOKEN*`) live in this namespace.
fn engine_configuration() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(key, _)| key.starts_with("OMNIGRAPH_"))
        .map(|(key, value)| {
            let value = if is_secret_config_name(&key) {
                "<redacted>".to_string()
            } else {
                value
            };
            (key, value)
        })
        .collect()
}

/// Whether a configuration variable's NAME marks its value as a secret: the
/// name persists, the value never does. Case-insensitive substring match —
/// KEY also covers plausible future credential-style flags.
fn is_secret_config_name(name: &str) -> bool {
    let upper = name.to_uppercase();
    ["TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "KEY"]
        .iter()
        .any(|marker| upper.contains(marker))
}

pub async fn run(args: RunArgs, session_id: String) -> BenchResult<()> {
    refuse_debug_build("recording benchmark numbers")?;
    if args.reps == 0 {
        return Err("--reps must be >= 1".into());
    }
    if args.warmth != Warmth::Cold && args.warmup_reps == 0 {
        return Err(
            "the warm and post-invalidation regimes need --warmup-reps >= 1 \
                    (0 warm-ups would mix rep 1's cold caches into the cell — the regime \
                    mixing RFC 0039 rule 3 forbids); use --warmth cold for cold numbers"
                .into(),
        );
    }
    if args.internal_cold_rep.is_some() && (args.warmth != Warmth::Cold || args.reps != 1) {
        return Err("--internal-cold-rep is the cold parent's internal seam \
                    (requires --warmth cold --reps 1)"
            .into());
    }
    if args.scenario == Scenario::M5 && args.fixture.is_none() {
        let tables = args.tables.unwrap_or(12);
        if let Some(diverged) = args.diverged_tables {
            if diverged != tables {
                return Err(format!(
                    "m5 diverges every table: --diverged-tables {diverged} contradicts \
                     --tables {tables}; drop --diverged-tables or make them equal"
                )
                .into());
            }
        }
    }
    let fixture = match &args.fixture {
        Some(dir) => {
            if args.root_uri.is_some() {
                return Err("--fixture and --root-uri are mutually exclusive (frozen \
                            fixtures are local-copy only in this version)"
                    .into());
            }
            if args.tables.is_some()
                || args.rows.is_some()
                || args.payload_bytes.is_some()
                || args.diverged_tables.is_some()
            {
                return Err("--fixture pins the Data axes; drop --tables/--rows/\
                            --payload-bytes/--diverged-tables"
                    .into());
            }
            let manifest = fixture::load_validated(dir)?;
            if args.scenario == Scenario::M5
                && manifest.profile.diverged_tables != manifest.profile.tables
            {
                return Err(format!(
                    "m5 diverges every table, but fixture {} was built with diverged_tables = {} of {}",
                    manifest.fixture_name,
                    manifest.profile.diverged_tables,
                    manifest.profile.tables
                )
                .into());
            }
            Some((dir.clone(), manifest))
        }
        None => None,
    };
    let deltas = if args.delta.is_empty() {
        match args.scenario {
            Scenario::M3 => vec![1, 50, 5000],
            Scenario::M5 => vec![50],
        }
    } else {
        args.delta.clone()
    };
    std::fs::create_dir_all(&args.out)?;
    let floor_path = args.out.join("noise-floor.json");
    // Checked before any measurement: a floor is per session, and overwriting
    // an existing one would silently retarget every diff that reads it.
    if args.aa && floor_path.exists() {
        return Err(format!(
            "{} already exists — refusing to overwrite a noise floor; point --out at \
             a fresh directory or delete the file first",
            floor_path.display()
        )
        .into());
    }

    let mut floor_points: BTreeMap<String, FloorPoint> = BTreeMap::new();
    let mut floor_commit = String::new();
    for delta in deltas {
        let plan = match &fixture {
            Some((_, manifest)) => FixturePlan::new(manifest.profile.clone(), delta)?,
            None => {
                let tables = args.tables.unwrap_or(12);
                let rows = args.rows.unwrap_or(10_000);
                let payload_bytes = args.payload_bytes.unwrap_or(64);
                let diverged_default = match args.scenario {
                    Scenario::M3 => tables.min(4),
                    Scenario::M5 => tables,
                };
                let diverged = args.diverged_tables.unwrap_or(diverged_default);
                let profile = BaseProfile::new(tables, rows, payload_bytes, diverged, vec![delta])?;
                FixturePlan::new(profile, delta)?
            }
        };
        if args.aa {
            let rec_a = run_point(&args, &plan, fixture.as_ref(), &session_id).await?;
            write_record(&args.out, &rec_a, "_aa1")?;
            let rec_b = run_point(&args, &plan, fixture.as_ref(), &session_id).await?;
            write_record(&args.out, &rec_b, "_aa2")?;
            floor_commit = rec_a.sut.source_commit.clone();
            floor_points.insert(rec_a.point_name.clone(), floor_point(&rec_a, &rec_b));
        } else {
            let record = run_point(&args, &plan, fixture.as_ref(), &session_id).await?;
            write_record(&args.out, &record, "")?;
        }
    }
    if args.aa {
        let floor = NoiseFloor {
            floor_version: FLOOR_VERSION,
            created_unix_seconds: unix_now(),
            session_id,
            source_commit: floor_commit,
            default_margin: default_margin(),
            points: floor_points,
        };
        // Temp + rename in the same directory (the fixture-manifest pattern):
        // a crash mid-write can leave a stray temp file, never a torn floor.
        // The pre-run existence check refused an existing floor; the rename
        // still makes any race a replace, not interleaved bytes.
        let tmp = args.out.join("noise-floor.json.tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(&floor)?)
            .map_err(|e| format!("writing {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &floor_path).map_err(|e| {
            format!(
                "renaming {} over {}: {e}",
                tmp.display(),
                floor_path.display()
            )
        })?;
        println!("noise floor written: {}", floor_path.display());
    }
    Ok(())
}

/// One point: dispatches the cold parent (fresh process per rep) or the
/// in-process warm / post-invalidation / cold-child run. Stamps the
/// caller-minted invocation id (a ULID — identity never rests on clock
/// resolution) and the invocation timestamp (ordering only) at entry — one
/// record = one invocation, and (spec, SUT, invocation id) is the record's
/// unique key (RFC 0039).
async fn run_point(
    args: &RunArgs,
    plan: &FixturePlan,
    fixture: Option<&(PathBuf, FixtureManifest)>,
    session_id: &str,
) -> BenchResult<RunRecord> {
    let invocation_id = ulid::Ulid::new().to_string();
    let invoked = unix_now();
    if args.warmth == Warmth::Cold && args.internal_cold_rep.is_none() {
        run_cold(args, plan, fixture, invocation_id, session_id, invoked).await
    } else {
        run_one(args, plan, fixture, invocation_id, session_id, invoked).await
    }
}

/// Validate against the shipped schema, then write
/// `<point_name><suffix>_<invocation_id>.json` — the full point name keeps
/// distinct points from colliding, the invocation id keeps re-runs of one
/// point from colliding. `create_new` makes any residual collision a hard
/// error instead of a silent overwrite, and no code path rewrites an existing
/// record (RFC 0039: append-only until first cited). The harness refuses to
/// emit an invalid record (validation on WRITE).
fn write_record(out: &Path, record: &RunRecord, suffix: &str) -> BenchResult<PathBuf> {
    let value = serde_json::to_value(record)?;
    schema::require_valid_v3(&value, "refusing to write an invalid run record")?;
    let path = out.join(format!(
        "{}{suffix}_{}.json",
        record.point_name, record.invocation_id
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| format!("creating {} (refusing to overwrite): {e}", path.display()))?;
    use std::io::Write as _;
    file.write_all(serde_json::to_string_pretty(&value)?.as_bytes())?;
    println!("run record written: {}", path.display());
    Ok(path)
}

/// The A/A pair's floor entry for one point (rule 7): wall-clock p50 delta as
/// a percentage of the pair mean, plus per-phase floors where a phase has a
/// nonzero pair mean.
fn floor_point(a: &RunRecord, b: &RunRecord) -> FloorPoint {
    let (pa, pb) = (a.results.wall_clock_ms.p50, b.results.wall_clock_ms.p50);
    let mut phases = BTreeMap::new();
    for phase_a in &a.results.phases {
        let Some(phase_b) = b.results.phases.iter().find(|p| p.phase == phase_a.phase) else {
            continue;
        };
        let (fa, fb) = (phase_a.total_us_p50 as f64, phase_b.total_us_p50 as f64);
        if fa + fb > 0.0 {
            phases.insert(phase_a.phase.clone(), pair_delta_pct(fa, fb));
        }
    }
    FloorPoint {
        wall_p50_a_ms: pa,
        wall_p50_b_ms: pb,
        abs_delta_ms: (pa - pb).abs(),
        pct: pair_delta_pct(pa, pb),
        phases,
    }
}

/// The store root for one run, plus the tempdir guard that must outlive it.
fn run_root(
    args: &RunArgs,
    delta: usize,
) -> BenchResult<(String, String, Option<tempfile::TempDir>)> {
    match &args.root_uri {
        Some(uri) if uri.starts_with("s3://") => {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let root = format!(
                "{}/bench-{}-d{}-{}",
                uri.trim_end_matches('/'),
                args.scenario.id(),
                delta,
                nanos
            );
            Ok((root, "s3-compatible".to_string(), None))
        }
        Some(other) => Err(format!(
            "--root-uri must be an s3:// URI (got '{other}'); omit it for the local tempdir backend"
        )
        .into()),
        None => {
            let dir = tempfile::tempdir()?;
            let root = dir
                .path()
                .to_str()
                .ok_or("tempdir path is not UTF-8")?
                .to_string();
            Ok((root, "local-fs-tempdir".to_string(), Some(dir)))
        }
    }
}

/// Open the store with the engine's public counting decorator on the
/// control-plane `StorageAdapter`, returning the handle plus the counter.
async fn open_counting(root: &str) -> BenchResult<(Omnigraph, Arc<StorageReadCounts>)> {
    let inner = storage_for_uri(root)?;
    let (adapter, counts) = CountingStorageAdapter::new(inner);
    Ok((Omnigraph::open_with_storage(root, adapter).await?, counts))
}

/// One rep's control-plane call tallies (cumulative; subtract for deltas).
#[derive(Debug, Clone, Copy, Default)]
struct ControlSnapshot {
    read: u64,
    exists: u64,
    list: u64,
    mutation: u64,
}

fn control_snapshot(counts: &StorageReadCounts) -> ControlSnapshot {
    ControlSnapshot {
        read: counts.read_text() + counts.read_text_if_exists() + counts.read_text_versioned(),
        exists: counts.exists(),
        list: counts.list_dir(),
        mutation: counts.mutation_calls(),
    }
}

fn control_delta(before: ControlSnapshot, after: ControlSnapshot) -> ControlCalls {
    ControlCalls {
        read: after.read - before.read,
        exists: after.exists - before.exists,
        list: after.list - before.list,
        mutation: after.mutation - before.mutation,
    }
}

/// The state tag a point name carries for this run's F2/F3 level.
fn run_state_tag(fixture: Option<&(PathBuf, FixtureManifest)>) -> &'static str {
    match fixture {
        Some((_, manifest)) => fixture::state_tag(manifest.state.index_level),
        None => fixture::state_tag(fixture::IndexLevel::None),
    }
}

fn derive_point_name(
    args: &RunArgs,
    plan: &FixturePlan,
    fixture: Option<&(PathBuf, FixtureManifest)>,
    backend_is_s3: bool,
) -> String {
    let p = &plan.profile;
    let non_default_diverged = (p.diverged_tables != p.tables.min(4)).then_some(p.diverged_tables);
    crate::record::point_name(
        args.scenario.id(),
        p.tables,
        p.rows,
        p.payload_bytes,
        run_state_tag(fixture),
        non_default_diverged,
        plan.delta,
        args.warmth.id(),
        backend_is_s3,
    )
}

async fn run_one(
    args: &RunArgs,
    plan: &FixturePlan,
    fixture: Option<&(PathBuf, FixtureManifest)>,
    invocation_id: String,
    session_id: &str,
    invoked: u64,
) -> BenchResult<RunRecord> {
    let warmup = match args.warmth {
        Warmth::Cold => 0,
        _ => args.warmup_reps,
    };
    let (root, backend, _tempdir_guard) = run_root(args, plan.delta)?;
    let manifest_counter = CallCounter::default();
    let table_counter = CallCounter::default();
    let probes = QueryIoProbes {
        manifest_wrapper: Some(Arc::new(manifest_counter.clone()) as Arc<dyn WrappingObjectStore>),
        table_wrapper: Some(Arc::new(table_counter.clone()) as Arc<dyn WrappingObjectStore>),
        ..Default::default()
    };
    // The whole run body executes under the probe scope so every dataset the
    // engine opens (and every cached handle reused later) carries the
    // counting wrapper — the completeness condition for per-run counts.
    with_query_io_probes(
        probes,
        run_one_body(
            args,
            plan,
            fixture,
            root,
            backend,
            warmup,
            invocation_id,
            session_id,
            invoked,
            manifest_counter,
            table_counter,
        ),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_one_body(
    args: &RunArgs,
    plan: &FixturePlan,
    fixture: Option<&(PathBuf, FixtureManifest)>,
    root: String,
    backend: String,
    warmup: usize,
    invocation_id: String,
    session_id: &str,
    invoked: u64,
    manifest_counter: CallCounter,
    table_counter: CallCounter,
) -> BenchResult<RunRecord> {
    let p = &plan.profile;
    println!(
        "[{} d={}] base: T={} N={} diverged={} split/side={}",
        args.scenario.id(),
        plan.delta,
        p.tables,
        p.rows,
        p.diverged_tables,
        plan.side_split,
    );
    println!(
        "[{} d={}] env: backend={} warmth={}{}",
        args.scenario.id(),
        plan.delta,
        backend,
        args.warmth.id(),
        fixture.map_or(String::new(), |(_, m)| format!(
            " fixture={}",
            m.fixture_name
        )),
    );

    let build_started = Instant::now();
    let queries = fixture::mutation_queries(p.diverged_tables);
    let (mut db, mut control_counts, base_load_commits) = match fixture {
        Some((dir, manifest)) => {
            let (files, bytes) =
                fixture::copy_dir_recursive(&fixture::store_dir(dir), Path::new(&root))?;
            // Re-verify per copy: the entry check in `run` covered the load
            // moment; this covers the bytes this point actually opens.
            fixture::verify_copy_digest(Path::new(&root), manifest)?;
            println!(
                "[{} d={}] fixture copied + digest-verified: {} files, {:.1} MiB, {:.1}s",
                args.scenario.id(),
                plan.delta,
                files,
                bytes as f64 / (1024.0 * 1024.0),
                build_started.elapsed().as_secs_f64()
            );
            let (db, counts) = open_counting(&root).await?;
            (db, counts, manifest.state.base_load_commits)
        }
        None => {
            let schema_source = fixture::schema_source(p.tables);
            let db = Omnigraph::init(&root, &schema_source).await?;
            let commits = fixture::load_base(&db, p).await?;
            println!(
                "[{} d={}] base loaded: {} commits, {:.1}s",
                args.scenario.id(),
                plan.delta,
                commits,
                build_started.elapsed().as_secs_f64()
            );
            drop(db);
            let (db, counts) = open_counting(&root).await?;
            (db, counts, commits)
        }
    };

    let mut wall_ms: Vec<f64> = Vec::with_capacity(args.reps);
    let mut phase_names: Vec<String> = Vec::new();
    let mut phase_totals: Vec<Vec<u64>> = Vec::new();
    let mut phase_max_single: Vec<u64> = Vec::new();
    let mut write_path = WritePathCounters::default();
    let mut storage_manifest: Vec<crate::counting::CallCounts> = Vec::with_capacity(args.reps);
    let mut storage_table: Vec<crate::counting::CallCounts> = Vec::with_capacity(args.reps);
    let mut storage_control: Vec<ControlCalls> = Vec::with_capacity(args.reps);
    let mut fixture_seconds = build_started.elapsed().as_secs_f64();
    let mut last_target = String::new();

    let rep_base = args.internal_cold_rep.unwrap_or(0);
    let total_reps = warmup + args.reps;
    for i in 0..total_reps {
        let measured = i >= warmup;
        if args.warmth == Warmth::PostInvalidation && i == warmup {
            // The regime's invalidation step: drop the engine handle and
            // reopen (engine + Lance session caches fresh; OS page cache
            // stays warm — the engine exposes no finer invalidation door).
            drop(db);
            let (fresh_db, fresh_counts) = open_counting(&root).await?;
            db = fresh_db;
            control_counts = fresh_counts;
            println!(
                "[{} d={}] caches invalidated: engine handle reopened after {warmup} warm-up rep(s)",
                args.scenario.id(),
                plan.delta
            );
        }
        let rep = rep_base + i;
        let src = format!("bench_src_d{}_{rep}", plan.delta);
        let tgt = format!("bench_tgt_d{}_{rep}", plan.delta);
        let diverge_started = Instant::now();
        db.branch_create_from(ReadTarget::branch("main"), &src)
            .await?;
        db.branch_create_from(ReadTarget::branch("main"), &tgt)
            .await?;
        fixture::diverge(&db, &src, Side::Source, plan, &queries, rep).await?;
        fixture::diverge(&db, &tgt, Side::Target, plan, &queries, rep).await?;
        fixture_seconds += diverge_started.elapsed().as_secs_f64();

        // Reset the storage-call tallies so each measured window covers the
        // merge only (divergence/prepare calls are deliberately excluded).
        let _ = manifest_counter.take();
        let _ = table_counter.take();
        let control_before = control_snapshot(&control_counts);

        let probes = MergeWriteProbes::default();
        let merge_started = Instant::now();
        let outcome = with_merge_write_probes(probes.clone(), db.branch_merge(&src, &tgt)).await?;
        let elapsed = merge_started.elapsed();

        // Non-vacuous checks (every rep, warm-ups included): the measured
        // merge must be the general three-way route with real work behind it.
        if !matches!(outcome, MergeOutcome::Merged) {
            return Err(format!(
                "rep {rep}: merge outcome was {outcome:?}, expected Merged (three-way); \
                 the fixture short-circuited and this run is vacuous"
            )
            .into());
        }
        let readings = probes.merge_timing_snapshot();
        if readings.iter().map(|r| r.total_us).sum::<u64>() == 0 {
            return Err(format!(
                "rep {rep}: every merge phase reads 0 µs — the timing probes saw no work; \
                 refusing to record a vacuous run"
            )
            .into());
        }
        println!(
            "[{} d={}] rep {rep}{}: merge {:?} in {:.1} ms",
            args.scenario.id(),
            plan.delta,
            if measured {
                ""
            } else {
                " (warm-up, discarded)"
            },
            outcome,
            duration_ms(elapsed)
        );
        last_target = tgt;
        if !measured {
            continue;
        }
        if phase_names.is_empty() {
            phase_names = readings.iter().map(|r| r.phase.to_string()).collect();
            phase_totals = vec![Vec::with_capacity(args.reps); readings.len()];
            phase_max_single = vec![0; readings.len()];
        }
        for (i, reading) in readings.iter().enumerate() {
            phase_totals[i].push(reading.total_us);
            phase_max_single[i] = phase_max_single[i].max(reading.max_us);
        }
        write_path
            .stage_merge_insert_calls
            .push(probes.stage_merge_insert_calls());
        write_path
            .stage_merge_insert_rows
            .push(probes.stage_merge_insert_rows());
        write_path
            .stage_known_present_update_calls
            .push(probes.stage_known_present_update_calls());
        write_path
            .stage_known_present_update_rows
            .push(probes.stage_known_present_update_rows());
        write_path
            .stage_fenced_insert_calls
            .push(probes.stage_fenced_insert_calls());
        write_path
            .stage_fenced_insert_rows
            .push(probes.stage_fenced_insert_rows());
        write_path
            .strict_insert_preflight_calls
            .push(probes.strict_insert_preflight_calls());
        storage_manifest.push(manifest_counter.take());
        storage_table.push(table_counter.take());
        storage_control.push(control_delta(
            control_before,
            control_snapshot(&control_counts),
        ));
        wall_ms.push(duration_ms(elapsed));
    }

    // Non-vacuous row check on the first diverged table of the last target.
    let table_key = fixture::table_key(0);
    let expected = plan.expected_rows_after_merge(0);
    let actual = db
        .snapshot_of(ReadTarget::branch(&last_target))
        .await?
        .open(&table_key)
        .await?
        .count_rows(None)
        .await?;
    if actual != expected {
        return Err(format!(
            "post-merge row count on {table_key} is {actual}, expected {expected}; \
             the merge did not apply the planned delta — refusing to record"
        )
        .into());
    }

    let phases: Vec<PhaseStats> = phase_names
        .iter()
        .zip(&phase_totals)
        .zip(&phase_max_single)
        .map(|((name, totals), max_single)| PhaseStats {
            phase: name.clone(),
            total_us_p50: percentile_u64(totals, 50),
            total_us_max: *totals.iter().max().expect("reps >= 1"),
            max_single_us: *max_single,
            per_rep_total_us: totals.clone(),
        })
        .collect();
    let results = RunResults {
        wall_clock_ms: wall_stats(&wall_ms),
        phases,
        merge_outcome: "Merged".to_string(),
        verified_rows_table0: RowCheck {
            table_key,
            expected,
            actual,
        },
        fixture_build_seconds: fixture_seconds,
        write_path,
        storage_calls: Some(StorageCalls {
            scope: "object-store calls of the measured merges only (tallies reset before each \
                    merge; divergence and prepare excluded), split by store class; complete for \
                    every dataset opened under this run's probe scope (the whole run body). \
                    control_plane counts the engine StorageAdapter (non-Lance control objects); \
                    delete counted per delete-stream invocation, not per object."
                .to_string(),
            layer: "logical-operations".to_string(),
            physical_attempts: None,
            physical_attempts_note: "physical request attempts (RFC-031's second layer: \
                                     retries, multipart fan-out) are not observable at the \
                                     WrappingObjectStore seam — retries happen inside the \
                                     object_store backend client below this wrapper. Recorded \
                                     as explicitly absent, never assumed equal to the logical \
                                     counts."
                .to_string(),
            concurrency_witness: None,
            concurrency_witness_note: "the concurrency witness is defined at the physical \
                                       request layer, which is not observable at this seam; \
                                       recorded as absent — no measured span is assumed \
                                       serial, so elapsed-vs-cumulative reconciliation is \
                                       unavailable here, never assumed."
                .to_string(),
            cumulative_request_time_logical_us: None,
            cumulative_request_time_physical_us: None,
            cumulative_request_time_note: "this seam counts requests but does not time them; \
                                           per-layer request durations are recorded as absent \
                                           at both layers."
                .to_string(),
            latency_calibration: None,
            latency_calibration_note: "no backend latency calibration measured yet — the \
                                       attempts x per-request-latency cross-check waits on \
                                       it; the next measurement to wire."
                .to_string(),
            manifest_store: storage_manifest,
            table_store: storage_table,
            control_plane: storage_control,
        }),
    };
    print_summary(args.scenario.id(), plan.delta, &results);

    let state = match fixture {
        Some((_, manifest)) => StateAxes {
            fragmentation: manifest.state.fragmentation.clone(),
            index_existence: manifest.state.index_existence.clone(),
            index_freshness: manifest.state.index_freshness.clone(),
            deletion_history: manifest.state.deletion_history.clone(),
            compaction_recency: manifest.state.compaction_recency.clone(),
            builder_version: Some(manifest.builder_version),
            generation: Some(manifest.profile.clone()),
            base_load_commits,
            fixture_name: Some(manifest.fixture_name.clone()),
            fixture_manifest: Some(serde_json::to_value(manifest)?),
        },
        None => StateAxes {
            fragmentation: "fresh bulk load, no aging (stub for F1)".to_string(),
            index_existence: "none declared (F2 low end)".to_string(),
            index_freshness: "n/a — no indexes (stub for F3)".to_string(),
            deletion_history: "none before the measured merges (stub for F4)".to_string(),
            compaction_recency: "optimize never run (stub for F5)".to_string(),
            builder_version: Some(fixture::BUILDER_VERSION),
            generation: Some(p.clone()),
            base_load_commits,
            fixture_name: None,
            fixture_manifest: None,
        },
    };

    let warmth = WarmthDeclaration {
        regime: args.warmth.id().to_string(),
        warmup_reps_discarded: warmup,
        detail: match args.warmth {
            Warmth::Warm => format!(
                "{warmup} warm-up repetition(s) ran the full divergence + merge and were \
                 discarded; measurement started with engine, Lance-session, and OS caches warm"
            ),
            Warmth::PostInvalidation => format!(
                "{warmup} warm-up repetition(s), then the engine handle was dropped and \
                 reopened (engine + Lance session caches invalidated) before measurement; \
                 the OS page cache stays warm — the engine exposes no finer \
                 cache-invalidation door at this commit"
            ),
            Warmth::Cold => "fresh process per measured repetition; the store is copied or \
                             built fresh in that process (note: the copy itself leaves the OS \
                             page cache warm for the store files)"
                .to_string(),
        },
    };
    let rep_independence = match args.warmth {
        Warmth::Cold => "single measured repetition in its own fresh process on a fresh store \
                         copy (fully independent; the cold parent aggregates per-process \
                         records into one cell)"
            .to_string(),
        _ => "each rep merges a fresh branch pair off an unchanged main, but branches and \
              __manifest journal history accumulate across reps within the run"
            .to_string(),
    };
    let backend_is_s3 = backend == "s3-compatible";

    let run_spec = RunSpec {
        data: DataAxes {
            provenance: PROVENANCE_SYNTHETIC.to_string(),
            tables: p.tables,
            rows_per_table: p.rows,
            column_shape: "scalars-only (String key, String cohort, I32, String payload)"
                .to_string(),
            payload_bytes: p.payload_bytes,
        },
        state,
        workload: WorkloadAxes {
            scenario: args.scenario.id().to_string(),
            merge_kind: "diverged mixed three-way (updates+deletes+inserts, both sides, \
                         disjoint rows)"
                .to_string(),
            arrival: ARRIVAL_UNSCHEDULED_SINGLE_SHOT.to_string(),
            delta_rows_per_side: plan.delta,
            delta_split_per_side: plan.side_split,
            diverged_tables: p.diverged_tables,
        },
        environment: EnvironmentAxes {
            backend,
            root_uri_scheme: if args.root_uri.is_some() {
                "s3"
            } else {
                "file"
            }
            .to_string(),
            s3_endpoint: if backend_is_s3 {
                std::env::var("AWS_ENDPOINT_URL_S3").ok()
            } else {
                None
            },
            warmth,
        },
        protocol: ProtocolAxes {
            instrument: "in-process wall-clock + MergeTimingPhase snapshot \
                         (MergeWriteProbes::merge_timing_snapshot) + per-class \
                         storage-call counts (QueryIoProbes wrappers + \
                         CountingStorageAdapter)"
                .to_string(),
            attribution: ATTRIBUTION_PER_PHASE_ON.to_string(),
            repetitions: args.reps,
            timer: "std::time::Instant around branch_merge only (fixture build excluded)"
                .to_string(),
            rep_independence,
        },
    };
    // RFC 0039: the profile is decidable from the spec's levels alone — so it
    // is derived from them, never asserted independently.
    let profile = derive_profile(&run_spec)
        .expect("this harness always runs single-shot + synthetic + per-phase on: the micro region")
        .to_string();

    Ok(RunRecord {
        record_version: RECORD_VERSION,
        point_name: derive_point_name(args, plan, fixture, backend_is_s3),
        point_name_format: POINT_NAME_FORMAT,
        profile,
        instrument_access: crate::record::INSTRUMENT_ACCESS_INTERIM.to_string(),
        label: args.label.clone(),
        invocation_id,
        session_id: session_id.to_string(),
        invocation_unix_seconds: invoked,
        run_spec,
        sut: SutBlock {
            source_commit: source_commit(),
            build_profile: "release".to_string(),
            build_opt_level: crate::build_opt_level(),
            engine_configuration: engine_configuration(),
        },
        machine: crate::machine::capture(),
        results,
    })
}

fn wall_stats(wall_ms: &[f64]) -> WallClockStats {
    WallClockStats {
        p50: percentile_f64(wall_ms, 50),
        p95: percentile_f64(wall_ms, 95),
        min: wall_ms.iter().copied().fold(f64::INFINITY, f64::min),
        max: wall_ms.iter().copied().fold(0.0, f64::max),
        mean: wall_ms.iter().sum::<f64>() / wall_ms.len() as f64,
        tail_support: tail_support(wall_ms.len()).to_string(),
        reps: wall_ms.to_vec(),
    }
}

/// The cold regime's parent: one fresh process per measured repetition (the
/// RFC's cold definition), aggregated into one cell/record. Each child copies
/// or rebuilds its own store, runs exactly one measured merge with no
/// warm-ups, and writes a schema-valid single-rep record the parent folds in.
async fn run_cold(
    args: &RunArgs,
    plan: &FixturePlan,
    fixture: Option<&(PathBuf, FixtureManifest)>,
    invocation_id: String,
    session_id: &str,
    invoked: u64,
) -> BenchResult<RunRecord> {
    if fixture.is_none() {
        println!(
            "[{} d={}] note: cold without --fixture rebuilds the base store once per \
             repetition process — freeze a fixture for faster cold runs",
            args.scenario.id(),
            plan.delta
        );
    }
    let scratch = tempfile::tempdir()?;
    let exe = std::env::current_exe()?;
    let mut children: Vec<RunRecord> = Vec::with_capacity(args.reps);
    for rep in 0..args.reps {
        let out_dir = scratch.path().join(format!("rep{rep}"));
        std::fs::create_dir_all(&out_dir)?;
        let mut cmd = std::process::Command::new(&exe);
        cmd.arg("run")
            .arg("--scenario")
            .arg(args.scenario.id())
            .arg("--delta")
            .arg(plan.delta.to_string())
            .arg("--reps")
            .arg("1")
            .arg("--warmth")
            .arg("cold")
            .arg("--internal-cold-rep")
            .arg(rep.to_string())
            .arg("--out")
            .arg(&out_dir);
        if let Some((dir, _)) = fixture {
            cmd.arg("--fixture").arg(dir);
        } else {
            if let Some(t) = args.tables {
                cmd.arg("--tables").arg(t.to_string());
            }
            if let Some(r) = args.rows {
                cmd.arg("--rows").arg(r.to_string());
            }
            if let Some(pb) = args.payload_bytes {
                cmd.arg("--payload-bytes").arg(pb.to_string());
            }
            if let Some(d) = args.diverged_tables {
                cmd.arg("--diverged-tables").arg(d.to_string());
            }
        }
        if let Some(uri) = &args.root_uri {
            cmd.arg("--root-uri").arg(uri);
        }
        if let Some(label) = &args.label {
            cmd.arg("--label").arg(label);
        }
        println!(
            "[{} d={}] cold rep {rep}: spawning fresh process",
            args.scenario.id(),
            plan.delta
        );
        let status = cmd.status()?;
        if !status.success() {
            return Err(format!("cold rep {rep}: child process failed with {status}").into());
        }
        // The child names its record `<point>_<its own invocation id>.json`,
        // so the parent discovers it: the child's out dir holds exactly one
        // record.
        let record_path = sole_record_in(&out_dir)?;
        let text = std::fs::read_to_string(&record_path)
            .map_err(|e| format!("reading cold child record {}: {e}", record_path.display()))?;
        let value: serde_json::Value = serde_json::from_str(&text)?;
        schema::require_valid_v3(&value, "cold child produced an invalid record")?;
        children.push(serde_json::from_value(value)?);
    }
    merge_cold_records(args, children, invocation_id, session_id, invoked)
}

/// The single `.json` record a cold child's private out dir holds.
fn sole_record_in(dir: &Path) -> BenchResult<PathBuf> {
    let mut records: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| format!("reading {}: {e}", dir.display()))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
        .collect();
    match records.len() {
        1 => Ok(records.remove(0)),
        n => Err(format!(
            "cold child out dir {} holds {n} .json records, expected exactly 1",
            dir.display()
        )
        .into()),
    }
}

/// Fold the cold children's single-rep records into one cell. Wall-clock,
/// phase, write-path, and storage-call vectors concatenate in rep order;
/// aggregates are recomputed over the combined sample.
fn merge_cold_records(
    args: &RunArgs,
    children: Vec<RunRecord>,
    invocation_id: String,
    session_id: &str,
    invoked: u64,
) -> BenchResult<RunRecord> {
    let mut iter = children.into_iter();
    let mut merged = iter.next().ok_or("cold run produced no child records")?;
    for child in iter {
        if child.point_name != merged.point_name {
            return Err("cold children disagree on the point name — mixed binaries?".into());
        }
        if child.results.phases.len() != merged.results.phases.len() {
            return Err("cold children disagree on the phase list — mixed binaries?".into());
        }
        merged
            .results
            .wall_clock_ms
            .reps
            .extend(child.results.wall_clock_ms.reps);
        for (into, from) in merged.results.phases.iter_mut().zip(child.results.phases) {
            if into.phase != from.phase {
                return Err("cold children disagree on phase order — mixed binaries?".into());
            }
            into.per_rep_total_us.extend(from.per_rep_total_us);
            into.max_single_us = into.max_single_us.max(from.max_single_us);
        }
        let (wp_into, wp_from) = (&mut merged.results.write_path, child.results.write_path);
        wp_into
            .stage_merge_insert_calls
            .extend(wp_from.stage_merge_insert_calls);
        wp_into
            .stage_merge_insert_rows
            .extend(wp_from.stage_merge_insert_rows);
        wp_into
            .stage_known_present_update_calls
            .extend(wp_from.stage_known_present_update_calls);
        wp_into
            .stage_known_present_update_rows
            .extend(wp_from.stage_known_present_update_rows);
        wp_into
            .stage_fenced_insert_calls
            .extend(wp_from.stage_fenced_insert_calls);
        wp_into
            .stage_fenced_insert_rows
            .extend(wp_from.stage_fenced_insert_rows);
        wp_into
            .strict_insert_preflight_calls
            .extend(wp_from.strict_insert_preflight_calls);
        if let (Some(sc_into), Some(sc_from)) = (
            merged.results.storage_calls.as_mut(),
            child.results.storage_calls,
        ) {
            sc_into.manifest_store.extend(sc_from.manifest_store);
            sc_into.table_store.extend(sc_from.table_store);
            sc_into.control_plane.extend(sc_from.control_plane);
        }
        merged.results.verified_rows_table0 = child.results.verified_rows_table0;
        merged.results.fixture_build_seconds += child.results.fixture_build_seconds;
        if child.sut.source_commit != merged.sut.source_commit {
            return Err("cold children report different SUT commits".into());
        }
    }
    let reps = std::mem::take(&mut merged.results.wall_clock_ms.reps);
    merged.results.wall_clock_ms = wall_stats(&reps);
    for phase in &mut merged.results.phases {
        phase.total_us_p50 = percentile_u64(&phase.per_rep_total_us, 50);
        phase.total_us_max = *phase
            .per_rep_total_us
            .iter()
            .max()
            .expect("at least one rep");
    }
    if let Some(sc) = merged.results.storage_calls.as_mut() {
        sc.scope = format!(
            "{} (aggregated across {} fresh cold processes)",
            sc.scope, args.reps
        );
    }
    merged.run_spec.protocol.repetitions = args.reps;
    // The cell's invocation is the parent command; children were its rows.
    // The parent-minted id carries identity, the timestamp only ordering, and
    // the parent's session is the batch every child ran inside.
    merged.invocation_id = invocation_id;
    merged.session_id = session_id.to_string();
    merged.invocation_unix_seconds = invoked;
    print_summary(
        merged.scenario(),
        merged.run_spec.workload.delta_rows_per_side,
        &merged.results,
    );
    Ok(merged)
}

fn duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn print_summary(scenario: &str, delta: usize, results: &RunResults) {
    let top = results
        .phases
        .iter()
        .max_by_key(|p| p.total_us_p50)
        .expect("a recorded run holds at least one phase (the vacuous-run guard refused zero)");
    println!(
        "[{scenario} d={delta}] wall-clock p50 {:.1} ms, p95 {:.1} ms; top phase {} ({} µs p50)",
        results.wall_clock_ms.p50, results.wall_clock_ms.p95, top.phase, top.total_us_p50
    );
    if let Some(calls) = &results.storage_calls {
        let sum = |v: &[crate::counting::CallCounts]| {
            v.iter()
                .fold(crate::counting::CallCounts::default(), |mut acc, c| {
                    acc.get += c.get;
                    acc.head += c.head;
                    acc.put += c.put;
                    acc.list += c.list;
                    acc.copy += c.copy;
                    acc.delete += c.delete;
                    acc
                })
        };
        let (m, t) = (sum(&calls.manifest_store), sum(&calls.table_store));
        println!(
            "[{scenario} d={delta}] storage calls (sum over reps): table get {} / put {} / \
             list {}, __manifest get {} / put {} / list {}",
            t.get + t.head,
            t.put,
            t.list,
            m.get + m.head,
            m.put,
            m.list
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::testkit::sample_record;

    fn cold_args(reps: usize) -> RunArgs {
        RunArgs {
            scenario: Scenario::M3,
            tables: None,
            rows: None,
            delta: vec![],
            diverged_tables: None,
            reps,
            warmth: Warmth::Cold,
            warmup_reps: 0,
            aa: false,
            payload_bytes: None,
            out: PathBuf::from("unused"),
            root_uri: None,
            fixture: None,
            label: None,
            internal_cold_rep: None,
        }
    }

    #[test]
    fn secret_config_names_are_detected_case_insensitively() {
        // (name, is secret): the NAME persists either way, only the value is
        // replaced.
        for (name, secret) in [
            ("OMNIGRAPH_SERVER_BEARER_TOKEN", true),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", true),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET", true),
            ("omnigraph_api_key", true),
            ("OMNIGRAPH_DB_PASSWORD", true),
            ("OMNIGRAPH_CREDENTIAL_PATH", true),
            ("OMNIGRAPH_MERGE_LINEAGE", false),
            ("OMNIGRAPH_QUERY_TIMEOUT_MS", false),
        ] {
            assert_eq!(is_secret_config_name(name), secret, "name {name}");
        }
    }

    #[test]
    fn cold_fold_concatenates_reps_and_recomputes_aggregates() {
        let point = "m3-t2-n100-noindex-d1-cold";
        let children = vec![
            sample_record(point, &[10.0]),
            sample_record(point, &[30.0]),
            sample_record(point, &[20.0]),
        ];
        let merged = merge_cold_records(
            &cold_args(3),
            children,
            "parent-invocation".to_string(),
            "parent-session",
            7,
        )
        .unwrap();
        assert_eq!(merged.results.wall_clock_ms.reps, vec![10.0, 30.0, 20.0]);
        assert_eq!(merged.results.wall_clock_ms.p50, 20.0);
        assert_eq!(merged.results.wall_clock_ms.max, 30.0);
        assert_eq!(merged.results.wall_clock_ms.tail_support, "directional");
        assert_eq!(merged.run_spec.protocol.repetitions, 3);
        assert_eq!(merged.invocation_id, "parent-invocation");
        assert_eq!(merged.session_id, "parent-session");
        assert_eq!(merged.invocation_unix_seconds, 7);
        // Phase vectors concatenated in rep order.
        assert_eq!(merged.results.phases[0].per_rep_total_us.len(), 3);
        // Storage-call vectors concatenated too.
        let sc = merged.results.storage_calls.as_ref().unwrap();
        assert_eq!(sc.manifest_store.len(), 3);
        assert!(sc.scope.contains("3 fresh cold processes"));
        // Write-path vectors concatenated.
        assert_eq!(merged.results.write_path.stage_merge_insert_calls.len(), 3);
    }

    #[test]
    fn cold_fold_refuses_mixed_binaries() {
        let point = "m3-t2-n100-noindex-d1-cold";
        let a = sample_record(point, &[10.0]);
        let mut b = sample_record(point, &[20.0]);
        b.results.phases[0].phase = "SomethingElse".to_string();
        let err = merge_cold_records(&cold_args(2), vec![a, b], "id".to_string(), "s", 0)
            .expect_err("disagreeing phase lists must be refused");
        assert!(err.to_string().contains("phase order"));

        let a = sample_record(point, &[10.0]);
        let mut b = sample_record(point, &[20.0]);
        b.sut.source_commit = "different".to_string();
        let err = merge_cold_records(&cold_args(2), vec![a, b], "id".to_string(), "s", 0)
            .expect_err("disagreeing SUT commits must be refused");
        assert!(err.to_string().contains("different SUT commits"));

        let a = sample_record(point, &[10.0]);
        let b = sample_record("m3-t2-n100-noindex-d1-warm", &[20.0]);
        let err = merge_cold_records(&cold_args(2), vec![a, b], "id".to_string(), "s", 0)
            .expect_err("disagreeing point names must be refused");
        assert!(err.to_string().contains("point name"));
    }
}
