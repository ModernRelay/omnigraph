//! Run-record schema v3 (one JSON file per run) and small stat helpers.
//!
//! v3 aligns the record with RFC 0039 (the end-to-end benchmark):
//!
//! - the five-class factorization is named `run_spec` (fixture + workload +
//!   conditions) and doubles as the natural key; its serialized form is the
//!   [`RunRecord::point_name`];
//! - the **SUT block** ([`SutBlock`]) sits deliberately OUTSIDE the run spec
//!   (RFC 0039: "the spec describes the experiment, the SUT is the subject"):
//!   source commit, build profile, and the engine configuration captured as
//!   data (every `OMNIGRAPH_*` environment variable), never as prose labels;
//! - the auto-captured [`MachineSpec`] is record-level identity beside the
//!   SUT (rule 4), outside the run spec; the Environment class carries the
//!   declared [`WarmthDeclaration`] (one regime per cell, rule 3);
//! - the record declares its [`RunRecord::profile`] ("micro") and names the
//!   in-process phase readout as an implementation interim, per the RFC's
//!   profile definition;
//! - `results.storage_calls` records per-repetition object-store call counts
//!   per class (get / put / list / ...) beside the timings.
//!
//! The normative field list is the JSON Schema shipped with this crate
//! (`schema/run-record-v3.schema.json`, see `schema.rs`): records validate on
//! write (the harness refuses to emit an invalid record) and on read (`diff`
//! refuses to consume one). v1/v2 records remain readable through
//! [`v2::RunRecord`] + [`upgrade_v2`]; `diff` upgrades them on load.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::counting::CallCounts;
use crate::fixture::{BaseProfile, KindSplit};

/// v3: run_spec + SUT block + machine spec + warmth declaration + profile +
/// per-run storage-call counts (breaking rename of v2's `five_tuple`; v2 stays
/// readable through [`v2`]).
pub const RECORD_VERSION: u32 = 3;

/// Version of the point-name serialization format (RFC 0039: a name is
/// decodable only with its format, so the format version is a persisted field
/// of every record). Format 1 is [`point_name`]'s layout.
pub const POINT_NAME_FORMAT: u32 = 1;

/// The profile every record of this harness declares (RFC 0039: canonical
/// region of the run-spec space, named so it can be invoked without reciting
/// levels). This binary implements the micro profile only.
pub const PROFILE_MICRO: &str = "micro";

/// How the micro profile's per-phase attribution is served while the engine's
/// phase-timing exposure is unshipped (RFC 0039: "the harness's in-process
/// access is an implementation interim, not a different instrument").
pub const INSTRUMENT_ACCESS_INTERIM: &str = "in-process (implementation interim per RFC 0039: the public-surface phase-timing \
     exposure is unshipped; wall-clock and phases are read via \
     MergeWriteProbes::merge_timing_snapshot on the merge future)";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub record_version: u32,
    /// The run spec flattened into one string — the natural key a series over
    /// time shares (e.g. `m3-t8-n100k-btree-fresh-d50-warm`).
    pub point_name: String,
    /// The point-name format version ([`POINT_NAME_FORMAT`]): a name is
    /// decodable only with its format (RFC 0039).
    pub point_name_format: u32,
    /// "micro" (the realistic profile is not implemented by this binary).
    pub profile: String,
    /// How the instrument reaches its measurements (the in-process interim).
    pub instrument_access: String,
    /// Optional operator label ("baseline-main", "after-O3", ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Caller-minted unique id generated when the invocation starts (a
    /// ULID here). One record holds one invocation, its repetitions as rows
    /// inside it; (spec, SUT, invocation id) identifies one record uniquely
    /// (RFC 0039) — identity never rests on clock resolution.
    pub invocation_id: String,
    /// The session id (RFC 0039: one harness invocation batch on one machine)
    /// — a ULID minted once per CLI run and persisted in every record the
    /// batch produces. "unknown" only on upgraded v1/v2 records.
    #[serde(default = "unknown_string")]
    pub session_id: String,
    /// Seconds since UNIX epoch when this invocation started (UTC).
    /// Persisted for ordering only; identity is the invocation id.
    pub invocation_unix_seconds: u64,
    pub run_spec: RunSpec,
    pub sut: SutBlock,
    /// Auto-captured machine specification — record-level identity beside the
    /// SUT (RFC 0039 rule 4), deliberately outside the run spec: two runs of
    /// one spec on two machines are the same experiment on different
    /// hardware, and the identity warning (not the pairing key) carries that.
    pub machine: MachineSpec,
    pub results: RunResults,
}

impl RunRecord {
    /// The scenario id ("m3" | "m5"), owned by the workload axes (single
    /// owner; upgraded v1/v2 records fold their top-level copy in here).
    pub fn scenario(&self) -> &str {
        &self.run_spec.workload.scenario
    }
}

/// The five-class run spec: fixture (Data + State) + workload + conditions
/// (Environment + Protocol). Equal specs are directly comparable and form one
/// series over time; the SUT is deliberately not in here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSpec {
    pub data: DataAxes,
    pub state: StateAxes,
    pub workload: WorkloadAxes,
    pub environment: EnvironmentAxes,
    pub protocol: ProtocolAxes,
}

/// The system under test: what the run measures, outside the spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SutBlock {
    /// The commit the binary was built from, embedded at build time
    /// ("-dirty" appended when the build tree had uncommitted changes;
    /// "unverified:<sha>" when the build embedded nothing and the value came
    /// from run-time git; "unknown" when neither source was available).
    pub source_commit: String,
    /// "release" on v3-written records — an approximation derived from the
    /// debug-assertions guard (`cfg!(debug_assertions)` off), not from cargo's
    /// profile name; `build_opt_level` carries the hard datum. Upgraded v1/v2
    /// records carry whatever their protocol block recorded.
    pub build_profile: String,
    /// The `OPT_LEVEL` cargo compiled this binary with, embedded at build
    /// time ("unknown" on records that predate the field).
    #[serde(default = "unknown_string")]
    pub build_opt_level: String,
    /// Engine configuration as data: every `OMNIGRAPH_*` environment variable
    /// set at run time (feature flags and enabled techniques, e.g.
    /// `OMNIGRAPH_MERGE_LINEAGE`). Empty map = no flag set, recorded as such.
    pub engine_configuration: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataAxes {
    /// Data provenance factor ("synthetic" | "corpus-derived") — one of the
    /// three levels the profile is decided from (RFC 0039: a profile is
    /// decidable from a spec's levels alone). This builder generates, so
    /// every record here reads "synthetic"; the serde default covers v1/v2
    /// records, which were synthetic by construction.
    #[serde(default = "synthetic")]
    pub provenance: String,
    /// T — node-table count.
    pub tables: usize,
    /// N — base rows per table.
    pub rows_per_table: usize,
    pub column_shape: String,
    pub payload_bytes: usize,
}

fn synthetic() -> String {
    "synthetic".to_string()
}

/// The micro profile's three deciding levels (RFC 0039 profile definition).
pub const ARRIVAL_UNSCHEDULED_SINGLE_SHOT: &str = "unscheduled single-shot";
pub const PROVENANCE_SYNTHETIC: &str = "synthetic";
pub const ATTRIBUTION_PER_PHASE_ON: &str = "per-phase on";

/// Decide the profile from the spec's levels (RFC 0039: decidable from a
/// spec's levels alone, never asserted independently). `None` = the spec
/// falls in neither profile's region (a valid run, named by its full spec).
pub fn derive_profile(spec: &RunSpec) -> Option<&'static str> {
    if spec.workload.arrival == ARRIVAL_UNSCHEDULED_SINGLE_SHOT
        && spec.data.provenance == PROVENANCE_SYNTHETIC
        && spec.protocol.attribution == ATTRIBUTION_PER_PHASE_ON
    {
        return Some(PROFILE_MICRO);
    }
    None
}

/// F1–F5 fixture-state axes. Fresh-only levels name their stub explicitly so
/// a later real sweep is a value change, not a schema change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateAxes {
    pub fragmentation: String,
    pub index_existence: String,
    pub index_freshness: String,
    pub deletion_history: String,
    pub compaction_recency: String,
    /// The dataset builder's version (RFC 0039: the builder's identity is
    /// persisted with every run) — recorded unconditionally on v3 writes,
    /// fixture and inline alike; absent only on upgraded v1/v2 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub builder_version: Option<u32>,
    /// The builder's generation parameters (the base profile) — with
    /// `builder_version` these fully determine the generated bytes, so an
    /// inline record stays rebuildable without a fixture directory. Absent
    /// only on upgraded v1/v2 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<BaseProfile>,
    /// Commits on main after the base load (load chunks + init).
    pub base_load_commits: usize,
    /// Frozen-fixture provenance: the fixture's directory name, when the run
    /// used `--fixture` instead of building its base inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixture_name: Option<String>,
    /// The fixture's full `fixture-manifest.json` (validation stamp included),
    /// embedded verbatim so the record stays self-describing without the
    /// fixture directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixture_manifest: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadAxes {
    pub scenario: String,
    pub merge_kind: String,
    /// Arrival factor ("unscheduled single-shot" here; scheduled arrivals
    /// belong to the realistic profile) — profile-deciding level. The serde
    /// default covers v1/v2 records, which were single-shot by construction.
    #[serde(default = "unscheduled_single_shot")]
    pub arrival: String,
    /// d — delta rows per side.
    pub delta_rows_per_side: usize,
    pub delta_split_per_side: KindSplit,
    pub diverged_tables: usize,
}

fn unscheduled_single_shot() -> String {
    ARRIVAL_UNSCHEDULED_SINGLE_SHOT.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentAxes {
    /// Backend id — load-bearing (repo rule: never generic "disk").
    /// "local-fs-tempdir" for the default `file://`-semantics run;
    /// "s3-compatible" when `--root-uri s3://...` points at MinIO/RustFS/S3.
    pub backend: String,
    pub root_uri_scheme: String,
    /// `AWS_ENDPOINT_URL_S3` at run time, when the backend is S3-compatible —
    /// part of the backend identity (rule 4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_endpoint: Option<String>,
    /// The declared warmth regime of this cell (rule 3: exactly one).
    pub warmth: WarmthDeclaration,
}

/// Auto-captured machine identity. Unknown fields say "unknown"/absent rather
/// than guessing; an unknown identity still blocks a silent comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineSpec {
    pub os: String,
    pub arch: String,
    pub os_version: String,
    pub cpu_model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_cores: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_cores: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    /// "ssd" | "rotational" | "unknown".
    pub storage_class: String,
}

/// One warmth regime per cell (RFC 0039 rule 3: mixing regimes within a cell
/// invalidates it).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmthDeclaration {
    /// "cold" | "warm" | "post-invalidation" ("uncontrolled-v2" only on
    /// records upgraded from v1/v2, which mixed rep 1's cold caches into the
    /// warm cell).
    pub regime: String,
    /// Warm-up repetitions run and discarded before measurement (0 for cold).
    pub warmup_reps_discarded: usize,
    /// Exactly what the regime did in this harness (which caches are fresh).
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolAxes {
    pub instrument: String,
    /// Attribution factor ("per-phase on" | "off") — profile-deciding level.
    /// The serde default covers v1/v2 records, which always recorded phases.
    #[serde(default = "per_phase_on")]
    pub attribution: String,
    /// Measured repetitions (warm-ups excluded; see the warmth declaration).
    pub repetitions: usize,
    pub timer: String,
    /// Repetition-independence note (regime-dependent: cold repetitions are
    /// fully independent processes; warm ones accumulate branches + journal
    /// history within the run).
    pub rep_independence: String,
}

fn per_phase_on() -> String {
    ATTRIBUTION_PER_PHASE_ON.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResults {
    pub wall_clock_ms: WallClockStats,
    /// Per merge phase, aggregated over repetitions (see field docs).
    pub phases: Vec<PhaseStats>,
    /// Engine outcome of every measured merge (must be "Merged").
    pub merge_outcome: String,
    /// Non-vacuous proof: expected vs observed row count on the first
    /// diverged table of the target branch after the last measured merge.
    pub verified_rows_table0: RowCheck,
    /// Fixture build time (base load + per-rep divergence), for ops planning
    /// only — never a benchmark number.
    pub fixture_build_seconds: f64,
    /// Which constructive write primitives each measured merge staged
    /// (`MergeWriteProbes` counters, one entry per rep). Route evidence: a
    /// merge that stages known-present rows through the update primitive
    /// shows nonzero `stage_known_present_update_*`; one that stages
    /// everything through `stage_merge_insert_*` shows zeros there.
    #[serde(default)]
    pub write_path: WritePathCounters,
    /// Per-repetition object-store call counts per class, measured-merge only
    /// (RFC 0039: every run records counts beside timings). Absent only on
    /// records upgraded from v1/v2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_calls: Option<StorageCalls>,
}

/// Per-repetition storage-call counts of the measured merges, split by store
/// class the way the engine's cost harness splits them.
///
/// RFC 0039 counts at RFC-031's two layers: logical operations AND physical
/// request attempts. The per-class vectors here are the layer named in
/// `layer`; an unobservable layer is explicitly `null` with its reason in the
/// note field — the rationale ships in the record itself (`run.rs` builds
/// the note strings), so the record is the canonical home, not this comment.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageCalls {
    /// Exactly what the counts cover (probe scope, merge-only windows).
    pub scope: String,
    /// The counting layer of the per-class vectors below
    /// ("logical-operations" at the `WrappingObjectStore` seam).
    pub layer: String,
    /// RFC-031's physical-request-attempt layer, when observable; `null`
    /// means not observable at this seam, reason in `physical_attempts_note`.
    pub physical_attempts: Option<PhysicalAttempts>,
    /// Why `physical_attempts` is absent, when it is (the canonical rationale
    /// string, written per record).
    pub physical_attempts_note: String,
    /// The concurrency witness (RFC 0039: the highest number of storage
    /// requests simultaneously in flight per measured span) — a physical-layer
    /// measurement at per-repetition span grain (one entry per rep), when
    /// captured; `null` means not captured at this seam, reason in
    /// `concurrency_witness_note`. Without it, elapsed-vs-cumulative
    /// reconciliation is unavailable, never assumed serial.
    #[serde(default)]
    pub concurrency_witness: Option<Vec<u64>>,
    /// Why `concurrency_witness` is absent, when it is.
    #[serde(default)]
    pub concurrency_witness_note: String,
    /// Observed cumulative request time at the logical layer (µs, sum of
    /// request durations, one entry per rep), where captured; `null` = this
    /// seam counts requests but does not time them (see the note).
    #[serde(default)]
    pub cumulative_request_time_logical_us: Option<Vec<u64>>,
    /// Observed cumulative request time at the physical layer, where
    /// captured; `null` = the physical layer is not observable at this seam.
    #[serde(default)]
    pub cumulative_request_time_physical_us: Option<Vec<u64>>,
    /// Per-layer presence statement for the request-timing columns.
    #[serde(default)]
    pub cumulative_request_time_note: String,
    /// The latency calibration (the backend's measured per-request latency,
    /// the second operand of the attempts x latency prediction), where
    /// captured; `null` = not yet measured by this harness (see the note).
    #[serde(default)]
    pub latency_calibration: Option<serde_json::Value>,
    /// Why `latency_calibration` is absent, when it is.
    #[serde(default)]
    pub latency_calibration_note: String,
    /// Calls on the `__manifest` registry's object store, one entry per rep.
    pub manifest_store: Vec<CallCounts>,
    /// Calls on the data-table object stores, one entry per rep.
    pub table_store: Vec<CallCounts>,
    /// Non-Lance control-plane calls (the engine `StorageAdapter`), one entry
    /// per rep.
    pub control_plane: Vec<ControlCalls>,
}

/// The physical-request-attempt layer, per store class, one entry per rep.
/// Unpopulated at the current seam; the slot exists so a future seam that
/// observes HTTP-level attempts lands as a value change, not a schema change.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PhysicalAttempts {
    pub manifest_store: Vec<CallCounts>,
    pub table_store: Vec<CallCounts>,
}

/// One rep's control-plane (`StorageAdapter`) call deltas, from the engine's
/// public `CountingStorageAdapter`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ControlCalls {
    /// `read_text` + `read_text_if_exists` + `read_text_versioned`.
    pub read: u64,
    pub exists: u64,
    pub list: u64,
    /// All mutating calls (`write_text*`, `rename`, `delete*`, CAS writes).
    pub mutation: u64,
}

/// Per-rep write-path counters snapshotted from the merge's
/// `MergeWriteProbes` after each measured merge. All vectors are in
/// execution order and empty on records that predate them (v1).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WritePathCounters {
    pub stage_merge_insert_calls: Vec<u64>,
    pub stage_merge_insert_rows: Vec<u64>,
    pub stage_known_present_update_calls: Vec<u64>,
    pub stage_known_present_update_rows: Vec<u64>,
    pub stage_fenced_insert_calls: Vec<u64>,
    pub stage_fenced_insert_rows: Vec<u64>,
    pub strict_insert_preflight_calls: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WallClockStats {
    pub p50: f64,
    /// Nearest-rank p95; with < 20 reps this equals the max — read it as
    /// "worst observed", not a distribution tail (RFC-031's sample rule).
    pub p95: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    /// Whether the sample count supports the reported tail (rule 3: p95
    /// needs 20+ samples, p99 would need 100+): "supported" | "directional".
    /// Derived from the rep count via [`tail_support`]; `diff` prints the
    /// directional label on unsupported tails.
    #[serde(default = "unknown_string")]
    pub tail_support: String,
    /// Raw per-repetition values, in execution order.
    pub reps: Vec<f64>,
}

/// The tail-support marker for a cell of `reps` measured repetitions.
pub fn tail_support(reps: usize) -> &'static str {
    if reps >= 20 {
        "supported"
    } else {
        "directional"
    }
}

fn unknown_string() -> String {
    "unknown".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseStats {
    pub phase: String,
    /// Nearest-rank p50 of the per-rep phase totals (µs).
    pub total_us_p50: u64,
    /// Largest per-rep phase total (µs).
    pub total_us_max: u64,
    /// Largest single recorded interval across all reps (µs) — for
    /// per-table phases this is the slowest single table.
    pub max_single_us: u64,
    /// Raw per-repetition totals (µs), in execution order.
    pub per_rep_total_us: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowCheck {
    pub table_key: String,
    pub expected: usize,
    pub actual: usize,
}

// ---------------------------------------------------------------------------
// The A/A noise floor (rule 7)
// ---------------------------------------------------------------------------

/// The session's measured noise floor: the delta between two runs with equal
/// spec and equal SUT (`run --aa`). `diff --floor` consumes it; a delta below
/// the floor reads "no detected effect", never a small effect. A floor
/// licenses comparisons at its own cell (same spec, same SUT, same session,
/// RFC 0039); `diff` marks any other application as extrapolation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoiseFloor {
    pub floor_version: u32,
    pub created_unix_seconds: u64,
    /// The session the A/A pair ran in.
    #[serde(default = "unknown_string")]
    pub session_id: String,
    /// SUT commit both floor runs shared (a floor is per session + SUT).
    pub source_commit: String,
    /// The persisted claim margin (rule 7: a protocol-level default, never an
    /// after-the-fact choice): an effect must exceed floor x margin to be
    /// claimed. `diff --margin` overrides per invocation; the floor note then
    /// prints the effective margin with this default beside it.
    #[serde(default = "default_margin")]
    pub default_margin: f64,
    /// Per point: the A/A pair's wall-clock p50s and their relative delta.
    pub points: BTreeMap<String, FloorPoint>,
}

/// v2: session id + persisted default margin.
pub const FLOOR_VERSION: u32 = 2;

/// The protocol-level default claim margin (rule 7): an effect clears the
/// floor when it exceeds floor x this factor.
pub fn default_margin() -> f64 {
    2.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FloorPoint {
    pub wall_p50_a_ms: f64,
    pub wall_p50_b_ms: f64,
    pub abs_delta_ms: f64,
    /// |A − B| as a percentage of the pair mean — the floor a claimed effect
    /// on this point must clearly exceed.
    pub pct: f64,
    /// Per-phase floors (same formula over phase total_us p50), for phases
    /// with a nonzero pair mean.
    #[serde(default)]
    pub phases: BTreeMap<String, f64>,
}

/// |a − b| as a percentage of the pair mean (0 when both are 0).
pub fn pair_delta_pct(a: f64, b: f64) -> f64 {
    let mean = (a + b) / 2.0;
    if mean == 0.0 {
        0.0
    } else {
        ((a - b).abs() / mean) * 100.0
    }
}

/// First nine characters of a commit string, cut on a char boundary (a byte
/// slice would panic on multibyte input — commits are operator-influenced
/// strings on upgraded records, not guaranteed hex).
pub fn short_commit(commit: &str) -> &str {
    commit
        .char_indices()
        .nth(9)
        .map_or(commit, |(i, _)| &commit[..i])
}

// ---------------------------------------------------------------------------
// Point names
// ---------------------------------------------------------------------------

/// Derive the point name: the run spec flattened into one string (RFC 0039's
/// natural key, e.g. `m3-t8-n100k-btree-fresh-d50-warm`). Payload bytes join
/// only off the default so historical names stay stable; the S3 backend joins
/// as a suffix (the local backend is the default and stays out of the name —
/// the record's environment block always carries the full identity).
#[allow(clippy::too_many_arguments)]
pub fn point_name(
    scenario: &str,
    tables: usize,
    rows: usize,
    payload_bytes: usize,
    state_tag: &str,
    non_default_diverged: Option<usize>,
    delta: usize,
    warmth: &str,
    s3_backend: bool,
) -> String {
    let rows_fmt = if rows.is_multiple_of(1000) {
        format!("{}k", rows / 1000)
    } else {
        rows.to_string()
    };
    let payload = if payload_bytes == 64 {
        String::new()
    } else {
        format!("-p{payload_bytes}")
    };
    let div = non_default_diverged.map_or(String::new(), |d| format!("-div{d}"));
    let backend = if s3_backend { "-s3" } else { "" };
    format!("{scenario}-t{tables}-n{rows_fmt}{payload}-{state_tag}{div}-d{delta}-{warmth}{backend}")
}

// ---------------------------------------------------------------------------
// v1/v2 legacy records (read path only)
// ---------------------------------------------------------------------------

/// The v1/v2 record shapes, kept verbatim so `diff` can still read every
/// record written before the v3 rename. Never written.
pub mod v2 {
    use serde::{Deserialize, Serialize};

    use super::{DataAxes, RowCheck, StateAxes, WallClockStats, WorkloadAxes, WritePathCounters};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RunRecord {
        pub record_version: u32,
        pub scenario: String,
        #[serde(default)]
        pub label: Option<String>,
        pub created_unix_seconds: u64,
        pub source_commit: String,
        pub five_tuple: FiveTuple,
        pub results: RunResults,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct FiveTuple {
        pub data: DataAxes,
        pub state: StateAxes,
        pub workload: WorkloadAxes,
        pub environment: EnvironmentAxes,
        pub protocol: ProtocolAxes,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct EnvironmentAxes {
        pub backend: String,
        pub root_uri_scheme: String,
        pub host_os: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ProtocolAxes {
        pub instrument: String,
        pub repetitions: usize,
        pub build_profile: String,
        pub timer: String,
        pub rep_independence: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RunResults {
        pub wall_clock_ms: WallClockStats,
        pub phases: Vec<super::PhaseStats>,
        pub merge_outcome: String,
        pub verified_rows_table0: RowCheck,
        pub fixture_build_seconds: f64,
        #[serde(default)]
        pub write_path: WritePathCounters,
    }
}

/// Upgrade a v1/v2 record to the v3 shape for diffing. The upgrade is honest
/// about what v2 did not control or capture: warmth reads
/// `"uncontrolled-v2"` (v0 mixed rep 1's cold caches into the cell), the
/// machine spec is unknown beyond os-arch, the engine configuration is empty
/// (v2's gap: flags lived in prose labels), and storage calls are absent.
pub fn upgrade_v2(old: v2::RunRecord) -> RunRecord {
    let (os, arch) = old
        .five_tuple
        .environment
        .host_os
        .split_once('-')
        .map(|(os, arch)| (os.to_string(), arch.to_string()))
        .unwrap_or_else(|| (old.five_tuple.environment.host_os.clone(), "unknown".into()));
    let state_tag = if old.five_tuple.state.index_existence.starts_with("BTREE") {
        "btree-fresh"
    } else {
        "noindex"
    };
    let default_div = old.five_tuple.data.tables.min(4);
    let non_default_diverged = (old.five_tuple.workload.diverged_tables != default_div)
        .then_some(old.five_tuple.workload.diverged_tables);
    let name = point_name(
        &old.scenario,
        old.five_tuple.data.tables,
        old.five_tuple.data.rows_per_table,
        old.five_tuple.data.payload_bytes,
        state_tag,
        non_default_diverged,
        old.five_tuple.workload.delta_rows_per_side,
        "uncontrolled-v2",
        old.five_tuple.environment.backend == "s3-compatible",
    );
    let mut wall_clock_ms = old.results.wall_clock_ms;
    wall_clock_ms.tail_support = tail_support(wall_clock_ms.reps.len()).to_string();
    RunRecord {
        record_version: old.record_version,
        point_name: name,
        point_name_format: POINT_NAME_FORMAT,
        profile: PROFILE_MICRO.to_string(),
        instrument_access: INSTRUMENT_ACCESS_INTERIM.to_string(),
        // v2 records predate the caller-minted id. The upgrade derives a
        // DETERMINISTIC id from the record's own content so re-reading the
        // same file never mints a new identity.
        invocation_id: format!(
            "v2-upgrade:{}:{}:d{}:{}",
            short_commit(&old.source_commit),
            old.scenario,
            old.five_tuple.workload.delta_rows_per_side,
            old.created_unix_seconds
        ),
        session_id: "unknown (v2 record: sessions were not minted)".to_string(),
        label: old.label,
        // v2 stamped record-assembly time; the closest thing to an
        // invocation timestamp the old record carries.
        invocation_unix_seconds: old.created_unix_seconds,
        run_spec: RunSpec {
            data: old.five_tuple.data,
            state: old.five_tuple.state,
            workload: old.five_tuple.workload,
            environment: EnvironmentAxes {
                backend: old.five_tuple.environment.backend,
                root_uri_scheme: old.five_tuple.environment.root_uri_scheme,
                s3_endpoint: None,
                warmth: WarmthDeclaration {
                    regime: "uncontrolled-v2".to_string(),
                    warmup_reps_discarded: 0,
                    detail: "v0/v2 record: rep 1 ran with partly cold caches and was mixed \
                             into the cell (the regime mixing RFC 0039 rule 3 forbids); \
                             per-rep arrays keep the drift visible"
                        .to_string(),
                },
            },
            protocol: ProtocolAxes {
                instrument: old.five_tuple.protocol.instrument,
                // v1/v2 always recorded the per-phase attribution.
                attribution: ATTRIBUTION_PER_PHASE_ON.to_string(),
                repetitions: old.five_tuple.protocol.repetitions,
                timer: old.five_tuple.protocol.timer,
                rep_independence: old.five_tuple.protocol.rep_independence,
            },
        },
        sut: SutBlock {
            source_commit: old.source_commit,
            build_profile: old.five_tuple.protocol.build_profile,
            build_opt_level: "unknown (v2 record: opt-level was not captured)".to_string(),
            engine_configuration: BTreeMap::new(),
        },
        machine: MachineSpec {
            os,
            arch,
            os_version: "unknown".to_string(),
            cpu_model: "unknown (v2 record: machine spec was not captured)".to_string(),
            physical_cores: None,
            logical_cores: None,
            memory_bytes: None,
            storage_class: "unknown".to_string(),
        },
        results: RunResults {
            wall_clock_ms,
            phases: old.results.phases,
            merge_outcome: old.results.merge_outcome,
            verified_rows_table0: old.results.verified_rows_table0,
            fixture_build_seconds: old.results.fixture_build_seconds,
            write_path: old.results.write_path,
            storage_calls: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Stat helpers
// ---------------------------------------------------------------------------

/// Nearest-rank percentile over an unsorted sample (q in 0..=100).
pub fn percentile_f64(values: &[f64], q: usize) -> f64 {
    assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("timings are finite"));
    let rank = (q * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

pub fn percentile_u64(values: &[u64], q: usize) -> u64 {
    assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = (q * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Test-only builders shared across the crate's unit tests.
#[cfg(test)]
pub mod testkit {
    use super::*;

    /// A complete, schema-valid v3 run record with the given wall-clock reps.
    pub fn sample_record(point_name: &str, wall_reps: &[f64]) -> RunRecord {
        let phases = vec![PhaseStats {
            phase: "OuterPrepare".to_string(),
            total_us_p50: percentile_u64(&vec![100; wall_reps.len()], 50),
            total_us_max: 100,
            max_single_us: 100,
            per_rep_total_us: vec![100; wall_reps.len()],
        }];
        let call_counts = vec![CallCounts::default(); wall_reps.len()];
        RunRecord {
            record_version: RECORD_VERSION,
            point_name: point_name.to_string(),
            point_name_format: POINT_NAME_FORMAT,
            profile: PROFILE_MICRO.to_string(),
            instrument_access: INSTRUMENT_ACCESS_INTERIM.to_string(),
            label: Some("test".to_string()),
            invocation_id: format!("test-{point_name}-{}", wall_reps.len()),
            session_id: "test-session".to_string(),
            invocation_unix_seconds: 1,
            run_spec: RunSpec {
                data: DataAxes {
                    provenance: PROVENANCE_SYNTHETIC.to_string(),
                    tables: 2,
                    rows_per_table: 100,
                    column_shape: "scalars-only".to_string(),
                    payload_bytes: 64,
                },
                state: StateAxes {
                    fragmentation: "fresh".to_string(),
                    index_existence: "none declared (F2 low end)".to_string(),
                    index_freshness: "n/a".to_string(),
                    deletion_history: "none".to_string(),
                    compaction_recency: "never".to_string(),
                    builder_version: Some(crate::fixture::BUILDER_VERSION),
                    generation: Some(
                        crate::fixture::BaseProfile::new(2, 100, 64, 2, vec![1]).unwrap(),
                    ),
                    base_load_commits: 2,
                    fixture_name: None,
                    fixture_manifest: None,
                },
                workload: WorkloadAxes {
                    scenario: "m3".to_string(),
                    merge_kind: "diverged mixed three-way".to_string(),
                    arrival: ARRIVAL_UNSCHEDULED_SINGLE_SHOT.to_string(),
                    delta_rows_per_side: 1,
                    delta_split_per_side: KindSplit {
                        updates: 1,
                        deletes: 0,
                        inserts: 0,
                    },
                    diverged_tables: 2,
                },
                environment: EnvironmentAxes {
                    backend: "local-fs-tempdir".to_string(),
                    root_uri_scheme: "file".to_string(),
                    s3_endpoint: None,
                    warmth: WarmthDeclaration {
                        regime: "warm".to_string(),
                        warmup_reps_discarded: 1,
                        detail: "test".to_string(),
                    },
                },
                protocol: ProtocolAxes {
                    instrument: "test".to_string(),
                    attribution: ATTRIBUTION_PER_PHASE_ON.to_string(),
                    repetitions: wall_reps.len(),
                    timer: "test".to_string(),
                    rep_independence: "test".to_string(),
                },
            },
            sut: SutBlock {
                source_commit: "deadbeefdeadbeef".to_string(),
                build_profile: "release".to_string(),
                build_opt_level: "3".to_string(),
                engine_configuration: BTreeMap::new(),
            },
            machine: MachineSpec {
                os: "testos".to_string(),
                arch: "testarch".to_string(),
                os_version: "1".to_string(),
                cpu_model: "test cpu".to_string(),
                physical_cores: Some(4),
                logical_cores: Some(8),
                memory_bytes: Some(1024),
                storage_class: "ssd".to_string(),
            },
            results: RunResults {
                wall_clock_ms: WallClockStats {
                    p50: percentile_f64(wall_reps, 50),
                    p95: percentile_f64(wall_reps, 95),
                    min: wall_reps.iter().copied().fold(f64::INFINITY, f64::min),
                    max: wall_reps.iter().copied().fold(0.0, f64::max),
                    mean: wall_reps.iter().sum::<f64>() / wall_reps.len() as f64,
                    tail_support: tail_support(wall_reps.len()).to_string(),
                    reps: wall_reps.to_vec(),
                },
                phases,
                merge_outcome: "Merged".to_string(),
                verified_rows_table0: RowCheck {
                    table_key: "node:BenchT000".to_string(),
                    expected: 100,
                    actual: 100,
                },
                fixture_build_seconds: 0.1,
                write_path: WritePathCounters {
                    stage_merge_insert_calls: vec![1; wall_reps.len()],
                    stage_merge_insert_rows: vec![1; wall_reps.len()],
                    stage_known_present_update_calls: vec![0; wall_reps.len()],
                    stage_known_present_update_rows: vec![0; wall_reps.len()],
                    stage_fenced_insert_calls: vec![0; wall_reps.len()],
                    stage_fenced_insert_rows: vec![0; wall_reps.len()],
                    strict_insert_preflight_calls: vec![0; wall_reps.len()],
                },
                storage_calls: Some(StorageCalls {
                    scope: "test".to_string(),
                    layer: "logical-operations".to_string(),
                    physical_attempts: None,
                    physical_attempts_note: "test".to_string(),
                    concurrency_witness: None,
                    concurrency_witness_note: "test".to_string(),
                    cumulative_request_time_logical_us: None,
                    cumulative_request_time_physical_us: None,
                    cumulative_request_time_note: "test".to_string(),
                    latency_calibration: None,
                    latency_calibration_note: "test".to_string(),
                    manifest_store: call_counts.clone(),
                    table_store: call_counts,
                    control_plane: vec![ControlCalls::default(); wall_reps.len()],
                }),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles() {
        let v = [5.0, 1.0, 3.0, 2.0, 4.0];
        assert_eq!(percentile_f64(&v, 50), 3.0);
        assert_eq!(percentile_f64(&v, 95), 5.0);
        assert_eq!(percentile_u64(&[7], 50), 7);
    }

    #[test]
    fn point_names_flatten_the_spec() {
        assert_eq!(
            point_name("m3", 8, 100_000, 64, "btree-fresh", None, 50, "warm", false),
            "m3-t8-n100k-btree-fresh-d50-warm"
        );
        assert_eq!(
            point_name(
                "m5",
                140,
                4000,
                64,
                "noindex",
                Some(140),
                420,
                "cold",
                false
            ),
            "m5-t140-n4k-noindex-div140-d420-cold"
        );
        assert_eq!(
            point_name(
                "m3",
                8,
                4000,
                128,
                "noindex",
                None,
                1,
                "post-invalidation",
                true
            ),
            "m3-t8-n4k-p128-noindex-d1-post-invalidation-s3"
        );
    }

    #[test]
    fn short_commit_cuts_on_char_boundaries() {
        // (input, expected): multibyte input must truncate, never panic.
        for (input, expected) in [
            ("cafebabecafebabe", "cafebabec"),
            ("short", "short"),
            ("", ""),
            ("héllo-wörld-commit", "héllo-wör"),
            ("ééééééééééé", "ééééééééé"),
        ] {
            assert_eq!(short_commit(input), expected, "input {input}");
        }
    }

    #[test]
    fn pair_delta_pct_is_symmetric_and_zero_safe() {
        assert_eq!(pair_delta_pct(0.0, 0.0), 0.0);
        assert!((pair_delta_pct(100.0, 110.0) - pair_delta_pct(110.0, 100.0)).abs() < 1e-12);
        assert!((pair_delta_pct(100.0, 110.0) - 9.523_809_523_809_524).abs() < 1e-9);
    }

    #[test]
    fn tail_support_thresholds() {
        // (reps, expected marker): p95 needs >= 20 samples (rule 3).
        for (reps, expected) in [
            (1, "directional"),
            (5, "directional"),
            (19, "directional"),
            (20, "supported"),
            (100, "supported"),
        ] {
            assert_eq!(tail_support(reps), expected, "reps = {reps}");
        }
    }

    fn v2_record(scenario: &str, backend: &str, indexed: bool, reps: usize) -> v2::RunRecord {
        v2::RunRecord {
            record_version: 2,
            scenario: scenario.to_string(),
            label: None,
            created_unix_seconds: 42,
            source_commit: "cafebabecafebabe".to_string(),
            five_tuple: v2::FiveTuple {
                data: DataAxes {
                    provenance: PROVENANCE_SYNTHETIC.to_string(),
                    tables: 8,
                    rows_per_table: 100_000,
                    column_shape: "scalars-only".to_string(),
                    payload_bytes: 64,
                },
                state: StateAxes {
                    fragmentation: "fresh".to_string(),
                    index_existence: if indexed {
                        "BTREE on 'id'".to_string()
                    } else {
                        "none declared".to_string()
                    },
                    index_freshness: "n/a".to_string(),
                    deletion_history: "none".to_string(),
                    compaction_recency: "never".to_string(),
                    builder_version: None,
                    generation: None,
                    base_load_commits: 25,
                    fixture_name: None,
                    fixture_manifest: None,
                },
                workload: WorkloadAxes {
                    scenario: scenario.to_string(),
                    merge_kind: "diverged mixed three-way".to_string(),
                    arrival: ARRIVAL_UNSCHEDULED_SINGLE_SHOT.to_string(),
                    delta_rows_per_side: 50,
                    delta_split_per_side: KindSplit {
                        updates: 17,
                        deletes: 17,
                        inserts: 16,
                    },
                    diverged_tables: 4,
                },
                environment: v2::EnvironmentAxes {
                    backend: backend.to_string(),
                    root_uri_scheme: "file".to_string(),
                    host_os: "macos-aarch64".to_string(),
                },
                protocol: v2::ProtocolAxes {
                    instrument: "test".to_string(),
                    repetitions: reps,
                    build_profile: "release".to_string(),
                    timer: "test".to_string(),
                    rep_independence: "test".to_string(),
                },
            },
            results: v2::RunResults {
                wall_clock_ms: WallClockStats {
                    p50: 10.0,
                    p95: 12.0,
                    min: 9.0,
                    max: 12.0,
                    mean: 10.3,
                    tail_support: "unknown".to_string(),
                    reps: vec![10.0; reps],
                },
                phases: vec![],
                merge_outcome: "Merged".to_string(),
                verified_rows_table0: RowCheck {
                    table_key: "node:BenchT000".to_string(),
                    expected: 1,
                    actual: 1,
                },
                fixture_build_seconds: 1.0,
                write_path: WritePathCounters::default(),
            },
        }
    }

    #[test]
    fn upgrade_v2_is_honest_about_what_v2_did_not_capture() {
        // (scenario, backend, indexed, reps, expected point name, expected tail marker)
        for (scenario, backend, indexed, reps, want_name, want_tail) in [
            (
                "m3",
                "local-fs-tempdir",
                false,
                5,
                "m3-t8-n100k-noindex-d50-uncontrolled-v2",
                "directional",
            ),
            (
                "m5",
                "s3-compatible",
                true,
                25,
                "m5-t8-n100k-btree-fresh-d50-uncontrolled-v2-s3",
                "supported",
            ),
        ] {
            let up = upgrade_v2(v2_record(scenario, backend, indexed, reps));
            assert_eq!(up.point_name, want_name);
            assert_eq!(up.point_name_format, POINT_NAME_FORMAT);
            assert_eq!(up.scenario(), scenario);
            // v2 records sit in the micro region: derivable from the levels.
            assert_eq!(derive_profile(&up.run_spec), Some(PROFILE_MICRO));
            assert!(up.session_id.contains("v2 record"));
            assert_eq!(up.run_spec.environment.warmth.regime, "uncontrolled-v2");
            assert_eq!(up.results.wall_clock_ms.tail_support, want_tail);
            assert!(up.machine.cpu_model.contains("not captured"));
            assert_eq!(up.machine.os, "macos");
            assert_eq!(up.machine.arch, "aarch64");
            assert!(up.sut.build_opt_level.contains("v2 record"));
            assert!(up.sut.engine_configuration.is_empty());
            assert!(up.results.storage_calls.is_none());
            assert!(up.run_spec.state.builder_version.is_none());
            assert!(up.run_spec.state.generation.is_none());
            // Re-reading the same file never mints a new identity.
            assert_eq!(
                up.invocation_id,
                upgrade_v2(v2_record(scenario, backend, indexed, reps)).invocation_id
            );
        }
    }
}
