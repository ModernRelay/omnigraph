use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

use crate::model::{
    Diagnostic, ValidationOutcome, declared_version, read_yaml_file, strict_yaml, typed_sha256,
    valid_kebab_id,
};
use crate::{CASE_FORMAT_VERSION, POINT_IDENTITY_VERSION};

// Planning rejects configurations that a bounded runner could not reasonably
// materialize. These ceilings sit well above the RFC's large-scale points but
// prevent a syntactically valid file from asking a future runner to iterate or
// allocate near integer limits before its invocation budget can intervene.
const MAX_TABLES: u64 = 10_000;
const MAX_ROWS_PER_TABLE: u64 = 1_000_000_000;
const MAX_TOTAL_ROWS: u64 = 10_000_000_000;
const MAX_PAYLOAD_BYTES_PER_ROW: u64 = 64 * 1024 * 1024;
const MAX_LOGICAL_PAYLOAD_BYTES: u64 = 1 << 50;
const MAX_HISTORY_DEPTH: u64 = 1_000_000;
const MAX_WARMUP_ITERATIONS: u32 = 1_000;
const MAX_DEADLINE_SECONDS: u64 = 3_600;

/// A complete V1 branch-merge experiment. `id` is a human selector only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseV1 {
    pub version: u32,
    pub id: String,
    pub scenario: Scenario,
    pub fixture: Fixture,
    pub workload: Workload,
    pub environment: Environment,
    pub protocol: Protocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Scenario {
    #[serde(rename = "branch-merge-v1")]
    BranchMergeV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub builder: FixtureBuilder,
    pub data: Data,
    pub state: State,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureBuilder {
    pub kind: FixtureBuilderKind,
    pub version: u32,
    pub seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FixtureBuilderKind {
    #[serde(rename = "synthetic-branch-merge")]
    SyntheticBranchMerge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Data {
    pub provenance: DataProvenance,
    pub tables: u64,
    pub rows_per_table: u64,
    pub payload_bytes: u64,
    pub column_shape: ColumnShape,
    pub topology_skew: TopologySkew,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataProvenance {
    Synthetic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ColumnShape {
    Scalars,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TopologySkew {
    Uniform,
    PowerLaw,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub aging: Aging,
    /// Structured index inventory. Empty means no indexes. Each entry names
    /// the exact table/column, index mechanism, and current coverage state, so
    /// adding indexed cases never requires replacing a lossy global tag.
    pub indexes: Vec<IndexSpec>,
    pub deletion_history: DeletionHistory,
    pub compaction_recency: CompactionRecency,
    pub history_depth: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Aging {
    BulkLoaded,
    SmallCommits,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexSpec {
    pub table: String,
    pub column: String,
    pub kind: IndexKind,
    pub freshness: IndexFreshness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IndexKind {
    Btree,
    Fts,
    Ann,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IndexFreshness {
    Optimized,
    RowsStale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeletionHistory {
    None,
    Heavy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompactionRecency {
    Optimized,
    NotOptimized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    pub delta_rows_per_side: u64,
    pub diverged_tables: u64,
    pub arrival: Arrival,
    pub clients: u32,
    pub read_write_mix: ReadWriteMix,
    pub contention: Contention,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Arrival {
    UnscheduledSingleShot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReadWriteMix {
    ReadHeavy,
    Balanced,
    WriteHeavy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Contention {
    NotApplicable,
    SameKey,
    DistinctKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    pub backend: Backend,
    pub network_position: NetworkPosition,
    pub execution: Execution,
    pub warmth: Warmth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Backend {
    LocalFs {
        filesystem: LocalFilesystem,
        storage_class: LocalStorageClass,
    },
    S3 {
        implementation: S3Implementation,
        implementation_version: String,
        region: String,
        storage_class: String,
        versioning: S3Versioning,
        image_digest: Option<String>,
    },
}

// Serde's internally tagged enum decoder accepts extra fields on a unit
// variant. Decode through strict variant DTOs so `{ kind: local-fs, ... }`
// cannot smuggle S3 or future identity fields into a local point.
impl<'de> Deserialize<'de> for Backend {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match BackendWire::deserialize(deserializer)? {
            BackendWire::Local(LocalBackendWire {
                kind: LocalBackendKind::LocalFs,
                filesystem,
                storage_class,
            }) => Ok(Self::LocalFs {
                filesystem,
                storage_class,
            }),
            BackendWire::S3(S3BackendWire {
                kind: S3BackendKind::S3,
                implementation,
                implementation_version,
                region,
                storage_class,
                versioning,
                image_digest,
            }) => Ok(Self::S3 {
                implementation,
                implementation_version,
                region,
                storage_class,
                versioning,
                image_digest,
            }),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BackendWire {
    Local(LocalBackendWire),
    S3(S3BackendWire),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalBackendWire {
    kind: LocalBackendKind,
    filesystem: LocalFilesystem,
    storage_class: LocalStorageClass,
}

#[derive(Deserialize)]
enum LocalBackendKind {
    #[serde(rename = "local-fs")]
    LocalFs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LocalFilesystem {
    Apfs,
    Ext4,
    Xfs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LocalStorageClass {
    NvmeSsd,
    SataSsd,
    NetworkBlock,
    RamDisk,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct S3BackendWire {
    kind: S3BackendKind,
    implementation: S3Implementation,
    implementation_version: String,
    region: String,
    storage_class: String,
    versioning: S3Versioning,
    #[serde(default)]
    image_digest: Option<String>,
}

#[derive(Deserialize)]
enum S3BackendKind {
    #[serde(rename = "s3")]
    S3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum S3Implementation {
    AwsS3,
    Minio,
    Rustfs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum S3Versioning {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkPosition {
    SameHost,
    SameRegion,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Execution {
    Embedded,
    Server,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Warmth {
    pub regime: WarmthRegime,
    pub program: WarmthProgram,
    pub iterations: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WarmthRegime {
    Cold,
    Warm,
    PostInvalidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WarmthProgram {
    None,
    BranchMergeReadSetV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Protocol {
    /// Measured-operation deadline. An explicit YAML `null` means no
    /// measurement deadline; the runner's independent supervisor watchdog
    /// remains bounded either way.
    #[serde(deserialize_with = "deserialize_required_optional_deadline")]
    pub deadline_seconds: Option<u64>,
    pub attribution: Attribution,
    pub schedule: Schedule,
    pub reset: ResetMode,
    pub timer: Timer,
}

fn deserialize_required_optional_deadline<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<u64>::deserialize(deserializer)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Attribution {
    PerPhase,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Schedule {
    Manual,
    Earned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResetMode {
    PlainCopy,
    LocalClonefile,
    S3Versioning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Timer {
    Monotonic,
}

/// Versioned, canonical point identity. It deliberately has no case id,
/// source path, suite, or repetition quantity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointIdentityV1 {
    pub identity_version: u32,
    pub scenario: Scenario,
    pub fixture: Fixture,
    pub workload: Workload,
    pub environment: Environment,
    pub protocol: Protocol,
}

impl From<&CaseV1> for PointIdentityV1 {
    fn from(case: &CaseV1) -> Self {
        Self {
            identity_version: POINT_IDENTITY_VERSION,
            scenario: case.scenario,
            fixture: case.fixture.clone(),
            workload: case.workload.clone(),
            environment: case.environment.clone(),
            protocol: case.protocol.clone(),
        }
    }
}

/// A strict, semantically valid case plus its two distinct digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ValidatedCase {
    pub definition: CaseV1,
    pub identity: PointIdentityV1,
    /// Full SHA-256 of [`PointIdentityV1`], the experiment's natural key.
    pub point_id: String,
    /// Full SHA-256 of the complete typed case, including format version/id.
    pub case_digest: String,
    /// Human-readable convenience label. Never use this as identity.
    pub point_name: String,
}

/// Parse and semantically validate one case-v1 YAML document.
pub fn parse_case(source: &str) -> ValidationOutcome<ValidatedCase> {
    let version = match declared_version(source, "case") {
        Ok(version) => version,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    if version != CASE_FORMAT_VERSION {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "unsupported_case_version",
            "version",
            format!(
                "unsupported case version {version}; this build supports version {CASE_FORMAT_VERSION}"
            ),
        )]);
    }
    let definition: CaseV1 = match strict_yaml(source, "case") {
        Ok(case) => case,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    validate_case(definition)
}

/// Read, parse, and semantically validate one case-v1 YAML file.
pub fn load_case(path: &Path) -> ValidationOutcome<ValidatedCase> {
    let source = match read_yaml_file(path, "case") {
        Ok(source) => source,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    parse_case(&source)
}

/// Check cross-field invariants and derive stable identities.
pub fn validate_case(mut definition: CaseV1) -> ValidationOutcome<ValidatedCase> {
    normalize_identity_strings(&mut definition);
    let mut diagnostics = Vec::new();
    if definition.version != CASE_FORMAT_VERSION {
        diagnostics.push(Diagnostic::error(
            "unsupported_case_version",
            "version",
            format!(
                "unsupported case version {}; this build supports version {CASE_FORMAT_VERSION}",
                definition.version
            ),
        ));
    }
    if !valid_kebab_id(&definition.id) || definition.id.len() > 128 {
        diagnostics.push(Diagnostic::error(
            "invalid_case_id",
            "id",
            "case id must be 1..=128 characters of kebab-case ASCII ([a-z0-9]+(?:-[a-z0-9]+)*)",
        ));
    }
    if definition.fixture.builder.version != 1 {
        diagnostics.push(Diagnostic::error(
            "unsupported_builder_version",
            "fixture.builder.version",
            format!(
                "synthetic-branch-merge builder version {} is unsupported; this build supports version 1",
                definition.fixture.builder.version
            ),
        ));
    }
    validate_fixture_scale(&definition.fixture, &mut diagnostics);
    if definition.fixture.data.topology_skew != TopologySkew::Uniform {
        diagnostics.push(Diagnostic::error(
            "unsupported_branch_merge_topology",
            "fixture.data.topology_skew",
            "synthetic branch-merge-v1 currently supports topology_skew: uniform",
        ));
    }
    validate_indexes(
        definition.fixture.data.column_shape,
        &definition.fixture.state.indexes,
        &mut diagnostics,
    );
    validate_fixture_state(&definition.fixture, &mut diagnostics);
    validate_workload(&definition, &mut diagnostics);
    validate_warmth(&definition.environment.warmth, &mut diagnostics);
    validate_backend_protocol(&definition, &mut diagnostics);

    if !diagnostics.is_empty() {
        return ValidationOutcome::failure(diagnostics);
    }

    // Inventory order is presentation, not experiment identity. Normalize it
    // before producing either typed digest so two authors listing the same
    // indexes in a different order still name one point.
    definition.fixture.state.indexes.sort();
    let identity = PointIdentityV1::from(&definition);
    let point_id = match typed_sha256(&identity) {
        Ok(digest) => digest,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    let case_digest = match typed_sha256(&definition) {
        Ok(digest) => digest,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    let short = &point_id[..12];
    let point_name = format!(
        "branch-merge-t{}-n{}-d{}-{}-{short}",
        definition.fixture.data.tables,
        definition.fixture.data.rows_per_table,
        definition.workload.delta_rows_per_side,
        definition.environment.warmth.regime.as_str(),
    );
    ValidationOutcome::success(ValidatedCase {
        definition,
        identity,
        point_id,
        case_digest,
        point_name,
    })
}

fn normalize_identity_strings(case: &mut CaseV1) {
    for index in &mut case.fixture.state.indexes {
        index.table = index.table.trim().to_owned();
        index.column = index.column.trim().to_owned();
    }
    if let Backend::S3 {
        implementation_version,
        region,
        storage_class,
        ..
    } = &mut case.environment.backend
    {
        *implementation_version = implementation_version.trim().to_owned();
        *region = region.trim().to_owned();
        *storage_class = storage_class.trim().to_owned();
    }
}

impl WarmthRegime {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
            Self::PostInvalidation => "post-invalidation",
        }
    }
}

fn validate_workload(case: &CaseV1, diagnostics: &mut Vec<Diagnostic>) {
    let data = &case.fixture.data;
    let workload = &case.workload;
    if workload.delta_rows_per_side == 0 {
        diagnostics.push(Diagnostic::error(
            "invalid_delta",
            "workload.delta_rows_per_side",
            "delta_rows_per_side must be >= 1",
        ));
    }
    if workload.diverged_tables == 0 || workload.diverged_tables > data.tables {
        diagnostics.push(Diagnostic::error(
            "invalid_diverged_tables",
            "workload.diverged_tables",
            format!(
                "diverged_tables must be between 1 and fixture.data.tables ({})",
                data.tables
            ),
        ));
    }
    if workload.clients != 1 {
        diagnostics.push(Diagnostic::error(
            "unsupported_branch_merge_clients",
            "workload.clients",
            "branch-merge-v1 is an unscheduled single-shot workload and requires clients: 1",
        ));
    }
    if workload.read_write_mix != ReadWriteMix::WriteHeavy {
        diagnostics.push(Diagnostic::error(
            "unsupported_branch_merge_read_write_mix",
            "workload.read_write_mix",
            "branch-merge-v1 measures one client-visible write and requires read_write_mix: write-heavy",
        ));
    }
    if workload.contention != Contention::DistinctKey {
        diagnostics.push(Diagnostic::error(
            "unsupported_branch_merge_contention",
            "workload.contention",
            "branch-merge-v1 uses disjoint source/target cohorts and requires contention: distinct-key",
        ));
    }

    // The builder pre-tags separate source/target update and delete cohorts in
    // each diverged table. Prove the busiest table can hold those rows without
    // overflow rather than letting fixture construction fail much later.
    if workload.delta_rows_per_side > 0
        && workload.diverged_tables > 0
        && workload.diverged_tables <= data.tables
    {
        let updates = workload.delta_rows_per_side.div_ceil(3);
        let rest = workload.delta_rows_per_side - updates;
        let deletes = rest.div_ceil(2);
        let per_table = updates
            .div_ceil(workload.diverged_tables)
            .checked_add(deletes.div_ceil(workload.diverged_tables))
            .and_then(|rows| rows.checked_mul(2));
        match per_table {
            Some(required) if required <= data.rows_per_table => {}
            Some(required) => diagnostics.push(Diagnostic::error(
                "fixture_cohort_capacity_exceeded",
                "workload.delta_rows_per_side",
                format!(
                    "the branch-merge cohorts need {required} base rows in the busiest table, but rows_per_table is {}",
                    data.rows_per_table
                ),
            )),
            None => diagnostics.push(Diagnostic::error(
                "fixture_cohort_capacity_overflow",
                "workload.delta_rows_per_side",
                "branch-merge cohort sizing overflowed u64",
            )),
        }
    }
}

fn validate_fixture_scale(fixture: &Fixture, diagnostics: &mut Vec<Diagnostic>) {
    let data = &fixture.data;
    if !(1..=MAX_TABLES).contains(&data.tables) {
        diagnostics.push(Diagnostic::error(
            "invalid_table_count",
            "fixture.data.tables",
            format!("table count must be in 1..={MAX_TABLES}"),
        ));
    }
    if !(1..=MAX_ROWS_PER_TABLE).contains(&data.rows_per_table) {
        diagnostics.push(Diagnostic::error(
            "invalid_row_count",
            "fixture.data.rows_per_table",
            format!("rows_per_table must be in 1..={MAX_ROWS_PER_TABLE}"),
        ));
    }
    if data.payload_bytes > MAX_PAYLOAD_BYTES_PER_ROW {
        diagnostics.push(Diagnostic::error(
            "invalid_payload_size",
            "fixture.data.payload_bytes",
            format!("payload_bytes must be <= {MAX_PAYLOAD_BYTES_PER_ROW}"),
        ));
    }
    if !(1..=MAX_HISTORY_DEPTH).contains(&fixture.state.history_depth) {
        diagnostics.push(Diagnostic::error(
            "invalid_history_depth",
            "fixture.state.history_depth",
            format!("history_depth must be in 1..={MAX_HISTORY_DEPTH}"),
        ));
    }

    match data.tables.checked_mul(data.rows_per_table) {
        Some(total_rows) if total_rows > MAX_TOTAL_ROWS => diagnostics.push(Diagnostic::error(
            "fixture_row_budget_exceeded",
            "fixture.data",
            format!("tables * rows_per_table must be <= {MAX_TOTAL_ROWS}, got {total_rows}"),
        )),
        Some(total_rows) => match total_rows.checked_mul(data.payload_bytes) {
            Some(total_bytes) if total_bytes > MAX_LOGICAL_PAYLOAD_BYTES => {
                diagnostics.push(Diagnostic::error(
                    "fixture_payload_budget_exceeded",
                    "fixture.data",
                    format!(
                        "logical payload must be <= {MAX_LOGICAL_PAYLOAD_BYTES} bytes, got {total_bytes}"
                    ),
                ));
            }
            Some(_) => {}
            None => diagnostics.push(Diagnostic::error(
                "fixture_payload_size_overflow",
                "fixture.data",
                "tables * rows_per_table * payload_bytes overflowed u64",
            )),
        },
        None => diagnostics.push(Diagnostic::error(
            "fixture_row_count_overflow",
            "fixture.data",
            "tables * rows_per_table overflowed u64",
        )),
    }
}

fn validate_fixture_state(fixture: &Fixture, diagnostics: &mut Vec<Diagnostic>) {
    if fixture.builder.kind == FixtureBuilderKind::SyntheticBranchMerge
        && fixture.builder.version == 1
        && fixture.state.compaction_recency == CompactionRecency::Optimized
    {
        diagnostics.push(Diagnostic::error(
            "impossible_optimized_index_state",
            "fixture.state.compaction_recency",
            "synthetic-branch-merge builder v1 cannot truthfully declare optimized state because OmniGraph optimization materializes indexes outside this builder's exact inventory contract; use not-optimized",
        ));
    }
}

fn validate_indexes(
    column_shape: ColumnShape,
    indexes: &[IndexSpec],
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut seen = std::collections::BTreeSet::new();
    for (index, spec) in indexes.iter().enumerate() {
        if spec.table.trim().is_empty() {
            diagnostics.push(Diagnostic::error(
                "empty_index_table",
                format!("fixture.state.indexes[{index}].table"),
                "index table must not be empty",
            ));
        }
        if spec.column.trim().is_empty() {
            diagnostics.push(Diagnostic::error(
                "empty_index_column",
                format!("fixture.state.indexes[{index}].column"),
                "index column must not be empty",
            ));
        }
        if !seen.insert((&spec.table, &spec.column, spec.kind)) {
            diagnostics.push(Diagnostic::error(
                "duplicate_index",
                format!("fixture.state.indexes[{index}]"),
                format!(
                    "duplicate {:?} index on {}.{}; freshness is one state of one index",
                    spec.kind, spec.table, spec.column
                ),
            ));
        }
        if column_shape == ColumnShape::Scalars && spec.kind == IndexKind::Ann {
            diagnostics.push(Diagnostic::error(
                "impossible_index_inventory",
                format!("fixture.state.indexes[{index}].kind"),
                "ANN indexes require a vector column shape; this case declares column_shape: scalars",
            ));
        }
    }
}

fn validate_warmth(warmth: &Warmth, diagnostics: &mut Vec<Diagnostic>) {
    if warmth.iterations > MAX_WARMUP_ITERATIONS {
        diagnostics.push(Diagnostic::error(
            "warmup_iteration_budget_exceeded",
            "environment.warmth.iterations",
            format!("warm-up iterations must be <= {MAX_WARMUP_ITERATIONS}"),
        ));
    }
    match warmth.regime {
        WarmthRegime::Cold => {
            if warmth.program != WarmthProgram::None || warmth.iterations != 0 {
                diagnostics.push(Diagnostic::error(
                    "invalid_cold_warmth",
                    "environment.warmth",
                    "cold warmth requires program: none and iterations: 0",
                ));
            }
        }
        WarmthRegime::Warm | WarmthRegime::PostInvalidation => {
            if warmth.program != WarmthProgram::BranchMergeReadSetV1 || warmth.iterations == 0 {
                diagnostics.push(Diagnostic::error(
                    "invalid_warmth_program",
                    "environment.warmth",
                    "warm and post-invalidation regimes require a named program and iterations >= 1",
                ));
            }
        }
    }
}

fn valid_sha256_image_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_backend_protocol(case: &CaseV1, diagnostics: &mut Vec<Diagnostic>) {
    let environment = &case.environment;
    let protocol = &case.protocol;
    if let Some(deadline_seconds) = protocol.deadline_seconds {
        if !(1..=MAX_DEADLINE_SECONDS).contains(&deadline_seconds) {
            diagnostics.push(Diagnostic::error(
                "invalid_deadline",
                "protocol.deadline_seconds",
                format!("deadline_seconds must be null or in 1..={MAX_DEADLINE_SECONDS}"),
            ));
        }
    }
    if protocol.schedule != Schedule::Manual {
        diagnostics.push(Diagnostic::error(
            "unsupported_automatic_schedule",
            "protocol.schedule",
            "branch-merge-v1 is unscheduled and must use the manual schedule",
        ));
    }
    if protocol.attribution == Attribution::PerPhase && environment.execution != Execution::Embedded
    {
        diagnostics.push(Diagnostic::error(
            "unavailable_phase_attribution",
            "protocol.attribution",
            "per-phase attribution is currently available only for embedded execution",
        ));
    }
    match &environment.backend {
        Backend::LocalFs { filesystem, .. } => {
            if environment.network_position != NetworkPosition::SameHost {
                diagnostics.push(Diagnostic::error(
                    "invalid_local_network_position",
                    "environment.network_position",
                    "local-fs requires network_position: same-host",
                ));
            }
            if !matches!(
                protocol.reset,
                ResetMode::PlainCopy | ResetMode::LocalClonefile
            ) {
                diagnostics.push(Diagnostic::error(
                    "invalid_local_reset",
                    "protocol.reset",
                    "local-fs requires reset: plain-copy or local-clonefile",
                ));
            }
            if protocol.reset == ResetMode::LocalClonefile && *filesystem != LocalFilesystem::Apfs {
                diagnostics.push(Diagnostic::error(
                    "invalid_clonefile_filesystem",
                    "protocol.reset",
                    "local-clonefile is the APFS clonefile reset; other filesystems must use plain-copy",
                ));
            }
        }
        Backend::S3 {
            implementation,
            implementation_version,
            region,
            storage_class,
            versioning,
            image_digest,
        } => {
            for (path, value) in [
                (
                    "environment.backend.implementation_version",
                    implementation_version,
                ),
                ("environment.backend.region", region),
                ("environment.backend.storage_class", storage_class),
            ] {
                if value.trim().is_empty() {
                    diagnostics.push(Diagnostic::error(
                        "incomplete_s3_identity",
                        path,
                        "S3 backend identity fields must not be empty",
                    ));
                }
            }
            if *implementation == S3Implementation::AwsS3
                && environment.network_position == NetworkPosition::SameHost
            {
                diagnostics.push(Diagnostic::error(
                    "invalid_aws_s3_network_position",
                    "environment.network_position",
                    "AWS S3 requires network_position: same-region or remote; same-host is reserved for local compatible stores",
                ));
            }
            if protocol.reset != ResetMode::S3Versioning {
                diagnostics.push(Diagnostic::error(
                    "invalid_s3_reset",
                    "protocol.reset",
                    "S3 requires reset: s3-versioning",
                ));
            }
            if *versioning != S3Versioning::Enabled {
                diagnostics.push(Diagnostic::error(
                    "s3_versioning_required",
                    "environment.backend.versioning",
                    "s3-versioning reset requires bucket versioning to be enabled",
                ));
            }
            match implementation {
                S3Implementation::AwsS3 => {
                    if image_digest.is_some() {
                        diagnostics.push(Diagnostic::error(
                            "forbidden_aws_s3_image_digest",
                            "environment.backend.image_digest",
                            "AWS S3 is a managed service identity and must not declare image_digest",
                        ));
                    }
                }
                S3Implementation::Minio | S3Implementation::Rustfs => match image_digest {
                    None => diagnostics.push(Diagnostic::error(
                        "missing_s3_image_digest",
                        "environment.backend.image_digest",
                        "MinIO and RustFS backend identities require an image_digest",
                    )),
                    Some(digest) if !valid_sha256_image_digest(digest) => {
                        diagnostics.push(Diagnostic::error(
                            "invalid_s3_image_digest",
                            "environment.backend.image_digest",
                            "image_digest must be exactly lowercase sha256:<64hex>",
                        ));
                    }
                    Some(_) => {}
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
version: 1
id: m3-n100k-d50-warm
scenario: branch-merge-v1
fixture:
  builder:
    kind: synthetic-branch-merge
    version: 1
    seed: 0
  data:
    provenance: synthetic
    tables: 8
    rows_per_table: 100000
    payload_bytes: 64
    column_shape: scalars
    topology_skew: uniform
  state:
    aging: bulk-loaded
    indexes: []
    deletion_history: none
    compaction_recency: not-optimized
    history_depth: 1
workload:
  delta_rows_per_side: 50
  diverged_tables: 4
  arrival: unscheduled-single-shot
  clients: 1
  read_write_mix: write-heavy
  contention: distinct-key
environment:
  backend:
    kind: local-fs
    filesystem: apfs
    storage_class: nvme-ssd
  network_position: same-host
  execution: embedded
  warmth:
    regime: warm
    program: branch-merge-read-set-v1
    iterations: 1
protocol:
  deadline_seconds: 60
  attribution: per-phase
  schedule: manual
  reset: plain-copy
  timer: monotonic
"#;

    fn valid() -> ValidatedCase {
        parse_case(VALID).into_result().unwrap()
    }

    fn s3_case(
        implementation: &str,
        implementation_version: &str,
        versioning: &str,
        image_digest: Option<&str>,
    ) -> String {
        let image = image_digest
            .map(|digest| format!("\n    image_digest: {digest}"))
            .unwrap_or_default();
        VALID
            .replace(
                "  backend:\n    kind: local-fs\n    filesystem: apfs\n    storage_class: nvme-ssd",
                &format!(
                    "  backend:\n    kind: s3\n    implementation: {implementation}\n    implementation_version: {implementation_version}\n    region: us-east-1\n    storage_class: standard\n    versioning: {versioning}{image}"
                ),
            )
            .replace("network_position: same-host", "network_position: same-region")
            .replace("reset: plain-copy", "reset: s3-versioning")
    }

    #[test]
    fn parses_strict_v1_and_derives_full_distinct_digests() {
        let case = valid();
        assert_eq!(case.point_id.len(), 64);
        assert_eq!(case.case_digest.len(), 64);
        assert_ne!(case.point_id, case.case_digest);
        assert!(case.point_name.ends_with(&case.point_id[..12]));
    }

    #[test]
    fn case_id_and_yaml_format_do_not_change_point_identity() {
        let first = valid();
        let reordered = VALID
            .replace("id: m3-n100k-d50-warm", "id: another-id")
            .replace(
                "rows_per_table: 100000\n    payload_bytes: 64",
                "payload_bytes: 64\n    rows_per_table: 100000",
            )
            .replace(
                "version: 1\nid:",
                "# formatting is not identity\nversion: 1\nid:",
            );
        let second = parse_case(&reordered).into_result().unwrap();
        assert_eq!(first.point_id, second.point_id);
        assert_ne!(first.case_digest, second.case_digest);
    }

    #[test]
    fn every_typed_identity_change_moves_the_point_id() {
        let first = valid();
        let changed = VALID.replace("payload_bytes: 64", "payload_bytes: 65");
        let second = parse_case(&changed).into_result().unwrap();
        assert_ne!(first.point_id, second.point_id);
    }

    #[test]
    fn complete_data_and_workload_factors_are_point_identity() {
        let base = valid().definition;
        let base_id = typed_sha256(&PointIdentityV1::from(&base)).unwrap();

        let mut topology = base.clone();
        topology.fixture.data.topology_skew = TopologySkew::PowerLaw;
        assert_ne!(
            base_id,
            typed_sha256(&PointIdentityV1::from(&topology)).unwrap()
        );

        let mut mix = base.clone();
        mix.workload.read_write_mix = ReadWriteMix::Balanced;
        assert_ne!(base_id, typed_sha256(&PointIdentityV1::from(&mix)).unwrap());

        let mut contention = base;
        contention.workload.contention = Contention::SameKey;
        assert_ne!(
            base_id,
            typed_sha256(&PointIdentityV1::from(&contention)).unwrap()
        );
    }

    #[test]
    fn branch_merge_requires_supported_complete_factor_levels() {
        for (from, to, code) in [
            (
                "topology_skew: uniform",
                "topology_skew: power-law",
                "unsupported_branch_merge_topology",
            ),
            (
                "read_write_mix: write-heavy",
                "read_write_mix: balanced",
                "unsupported_branch_merge_read_write_mix",
            ),
            (
                "contention: distinct-key",
                "contention: same-key",
                "unsupported_branch_merge_contention",
            ),
        ] {
            assert!(
                parse_case(&VALID.replace(from, to))
                    .diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.code == code),
                "missing {code}"
            );
        }
    }

    #[test]
    fn unknown_fields_and_versions_fail_closed() {
        let unknown = VALID.replace("version: 1", "version: 1\nunknown: true");
        assert_eq!(
            parse_case(&unknown).diagnostics[0].code,
            "invalid_case_yaml"
        );
        let future = VALID.replace("version: 1", "version: 2");
        assert_eq!(
            parse_case(&future).diagnostics[0].code,
            "unsupported_case_version"
        );
        let future_builder =
            VALID.replace("    version: 1\n    seed: 0", "    version: 2\n    seed: 0");
        assert_eq!(
            parse_case(&future_builder).diagnostics[0].code,
            "unsupported_builder_version"
        );
        let duplicate = VALID.replace("version: 1", "version: 1\nversion: 1");
        assert_eq!(
            parse_case(&duplicate).diagnostics[0].code,
            "invalid_case_yaml"
        );
        let backend_unknown =
            VALID.replace("kind: local-fs", "kind: local-fs\n    endpoint: surprise");
        assert_eq!(
            parse_case(&backend_unknown).diagnostics[0].code,
            "invalid_case_yaml"
        );
    }

    #[test]
    fn index_inventory_is_normalized_before_identity_and_duplicate_checks() {
        let first_yaml = VALID.replace(
            "indexes: []",
            "indexes:\n      - { table: n0, column: id, kind: btree, freshness: optimized }\n      - { table: n1, column: name, kind: fts, freshness: rows-stale }",
        );
        let second_yaml = first_yaml.replace(
            "      - { table: n0, column: id, kind: btree, freshness: optimized }\n      - { table: n1, column: name, kind: fts, freshness: rows-stale }",
            "      - { table: n1, column: name, kind: fts, freshness: rows-stale }\n      - { table: n0, column: id, kind: btree, freshness: optimized }",
        );
        let first = parse_case(&first_yaml).into_result().unwrap();
        let second = parse_case(&second_yaml).into_result().unwrap();
        assert_eq!(first.point_id, second.point_id);
        assert_eq!(first.case_digest, second.case_digest);

        let padded_yaml = first_yaml
            .replace("table: n0", "table: ' n0 '")
            .replace("column: id", "column: ' id '");
        let padded = parse_case(&padded_yaml).into_result().unwrap();
        assert_eq!(first.point_id, padded.point_id);
        assert_eq!(first.case_digest, padded.case_digest);

        let duplicate = first_yaml.replace(
            "      - { table: n1, column: name, kind: fts, freshness: rows-stale }",
            "      - { table: n0, column: id, kind: btree, freshness: rows-stale }",
        );
        assert!(
            parse_case(&duplicate)
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "duplicate_index")
        );

        let padded_duplicate = first_yaml.replace(
            "      - { table: n1, column: name, kind: fts, freshness: rows-stale }",
            "      - { table: ' n0 ', column: ' id ', kind: btree, freshness: rows-stale }",
        );
        assert!(
            parse_case(&padded_duplicate)
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "duplicate_index")
        );

        let impossible = VALID.replace(
            "indexes: []",
            "indexes:\n      - { table: n0, column: embedding, kind: ann, freshness: optimized }",
        );
        assert!(
            parse_case(&impossible)
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "impossible_index_inventory")
        );
    }

    #[test]
    fn builder_v1_rejects_optimized_fixture_state_for_any_index_inventory() {
        let optimized = VALID.replace(
            "compaction_recency: not-optimized",
            "compaction_recency: optimized",
        );
        let partial_inventory = optimized.replace(
            "indexes: []",
            "indexes:\n      - { table: n0, column: id, kind: btree, freshness: optimized }",
        );
        for yaml in [optimized, partial_inventory] {
            let outcome = parse_case(&yaml);
            assert!(!outcome.ok);
            assert!(outcome.value.is_none());
            assert!(outcome.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "impossible_optimized_index_state"
                    && diagnostic.path == "fixture.state.compaction_recency"
            }));
        }
    }

    #[test]
    fn bounds_and_structured_index_semantics_are_checked_together() {
        let invalid = VALID
            .replace("rows_per_table: 100000", "rows_per_table: 1")
            .replace("diverged_tables: 4", "diverged_tables: 9")
            .replace(
                "indexes: []",
                "indexes:\n      - { table: '', column: id, kind: btree, freshness: optimized }",
            );
        let codes: Vec<_> = parse_case(&invalid)
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect();
        assert!(codes.contains(&"invalid_diverged_tables".to_string()));
        assert!(codes.contains(&"empty_index_table".to_string()));
    }

    #[test]
    fn fixture_scale_is_bounded_before_a_runner_sees_the_plan() {
        let oversized = VALID
            .replace("tables: 8", "tables: 10001")
            .replace("rows_per_table: 100000", "rows_per_table: 1000000000")
            .replace("payload_bytes: 64", "payload_bytes: 67108864")
            .replace("history_depth: 1", "history_depth: 1000001");
        let codes: Vec<_> = parse_case(&oversized)
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect();
        assert!(codes.contains(&"invalid_table_count".to_string()));
        assert!(codes.contains(&"fixture_row_budget_exceeded".to_string()));
        assert!(codes.contains(&"invalid_history_depth".to_string()));

        let oversized_payload = VALID
            .replace("rows_per_table: 100000", "rows_per_table: 1000000000")
            .replace("payload_bytes: 64", "payload_bytes: 67108864");
        assert!(
            parse_case(&oversized_payload)
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "fixture_payload_budget_exceeded")
        );
    }

    #[test]
    fn warmth_is_a_typed_program_with_cross_validation() {
        let cold = VALID
            .replace("regime: warm", "regime: cold")
            .replace("program: branch-merge-read-set-v1", "program: none")
            .replace("iterations: 1", "iterations: 0");
        assert!(parse_case(&cold).ok);

        let invalid = VALID.replace("regime: warm", "regime: cold");
        assert_eq!(
            parse_case(&invalid).diagnostics[0].code,
            "invalid_cold_warmth"
        );
        let unsupported = VALID.replace(
            "program: branch-merge-read-set-v1",
            "program: invented-warmup",
        );
        assert_eq!(
            parse_case(&unsupported).diagnostics[0].code,
            "invalid_case_yaml"
        );

        let unbounded = VALID.replace("iterations: 1", "iterations: 1001");
        assert!(
            parse_case(&unbounded)
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "warmup_iteration_budget_exceeded")
        );
    }

    #[test]
    fn backend_reset_and_identity_semantics_are_checked() {
        let invalid = s3_case("aws-s3", "''", "disabled", None)
            .replace("reset: s3-versioning", "reset: plain-copy");
        let codes: Vec<_> = parse_case(&invalid)
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect();
        assert!(codes.contains(&"incomplete_s3_identity".to_string()));
        assert!(codes.contains(&"invalid_s3_reset".to_string()));
        assert!(codes.contains(&"s3_versioning_required".to_string()));

        let invalid_clonefile = VALID
            .replace("filesystem: apfs", "filesystem: ext4")
            .replace("reset: plain-copy", "reset: local-clonefile");
        assert!(
            parse_case(&invalid_clonefile)
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_clonefile_filesystem")
        );
    }

    #[test]
    fn deadline_is_explicit_bounded_or_none_and_remains_identity() {
        let bounded = valid();
        let none = parse_case(&VALID.replace("deadline_seconds: 60", "deadline_seconds: null"))
            .into_result()
            .unwrap();
        assert_eq!(none.definition.protocol.deadline_seconds, None);
        assert_eq!(
            serde_json::to_value(&none.identity).unwrap()["protocol"]["deadline_seconds"],
            serde_json::Value::Null
        );
        assert_ne!(bounded.point_id, none.point_id);

        let thirty = parse_case(&VALID.replace("deadline_seconds: 60", "deadline_seconds: 30"))
            .into_result()
            .unwrap();
        assert_ne!(bounded.point_id, thirty.point_id);

        for seconds in [0, MAX_DEADLINE_SECONDS + 1] {
            assert!(
                parse_case(&VALID.replace(
                    "deadline_seconds: 60",
                    &format!("deadline_seconds: {seconds}")
                ))
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_deadline")
            );
        }

        let missing = VALID.replace("  deadline_seconds: 60\n", "");
        assert_eq!(
            parse_case(&missing).diagnostics[0].code,
            "invalid_case_yaml"
        );
    }

    #[test]
    fn aws_and_image_pinned_compatible_s3_identities_are_typed() {
        let aws = parse_case(&s3_case("aws-s3", "managed", "enabled", None))
            .into_result()
            .unwrap();
        let digest = format!("sha256:{}", "a".repeat(64));
        let minio = parse_case(&s3_case("minio", "release-1", "enabled", Some(&digest)))
            .into_result()
            .unwrap();
        assert_ne!(aws.point_id, minio.point_id);

        let same_host_minio = s3_case("minio", "release-1", "enabled", Some(&digest)).replace(
            "network_position: same-region",
            "network_position: same-host",
        );
        assert!(parse_case(&same_host_minio).ok);
    }

    #[test]
    fn s3_identity_strings_are_normalized_before_validation_and_hashing() {
        let digest = format!("sha256:{}", "a".repeat(64));
        let canonical = parse_case(&s3_case("minio", "release-1", "enabled", Some(&digest)))
            .into_result()
            .unwrap();
        let padded_yaml = s3_case("minio", "' release-1 '", "enabled", Some(&digest))
            .replace("region: us-east-1", "region: ' us-east-1 '")
            .replace("storage_class: standard", "storage_class: ' standard '");
        let padded = parse_case(&padded_yaml).into_result().unwrap();

        assert_eq!(canonical.point_id, padded.point_id);
        assert_eq!(canonical.case_digest, padded.case_digest);
        let Backend::S3 {
            implementation_version,
            region,
            storage_class,
            ..
        } = padded.definition.environment.backend
        else {
            panic!("expected S3 backend");
        };
        assert_eq!(implementation_version, "release-1");
        assert_eq!(region, "us-east-1");
        assert_eq!(storage_class, "standard");
    }

    #[test]
    fn compatible_s3_image_digest_rules_fail_closed() {
        let missing = parse_case(&s3_case("minio", "release-1", "enabled", None));
        assert!(
            missing
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "missing_s3_image_digest")
        );

        let malformed = format!("sha256:{}", "A".repeat(64));
        let malformed = parse_case(&s3_case("rustfs", "release-1", "enabled", Some(&malformed)));
        assert!(
            malformed
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_s3_image_digest")
        );

        let forbidden = format!("sha256:{}", "b".repeat(64));
        let forbidden = parse_case(&s3_case("aws-s3", "managed", "enabled", Some(&forbidden)));
        assert!(
            forbidden
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "forbidden_aws_s3_image_digest")
        );
    }
}
