//! Column codecs of the row types. `__manifest` stores a table and a commit as
//! the fields of a packed record; `__history` packs its commit fields together
//! and stores the tables of its `__manifest` version in a separate list of
//! structs. Both expose the same logical columns after unpacking, so one codec
//! per row type serves both datasets. [`ReplacedColumns`] is `__manifest`'s alone.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, StringArray, UInt64Array};
use serde_json::Value;

use crate::error::{OmniError, Result};
use crate::metadata::TableVersionMetadata;
use crate::record::{COMMIT_FIELDS, REPLACED_FIELDS, TABLE_FIELDS};
use crate::state::{GraphLineageRow, TablePin, TableRow, TableState};
use crate::{TableIdentity, TableRegistration, table_path_for_identity};

fn typed<'a, T: Array + 'static>(
    lookup: &dyn Fn(&str) -> Option<&'a ArrayRef>,
    name: &str,
) -> Result<&'a T> {
    lookup(name)
        .ok_or_else(|| OmniError::manifest_internal(format!("catalog rows miss column '{name}'")))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("catalog column '{name}' has an unexpected type"))
        })
}

fn string(column: &StringArray, row: usize) -> Option<String> {
    column.is_valid(row).then(|| column.value(row).to_string())
}

fn number(column: &UInt64Array, row: usize) -> Option<u64> {
    column.is_valid(row).then(|| column.value(row))
}

fn required<T>(value: Option<T>, name: &str, row: usize) -> Result<T> {
    value.ok_or_else(|| {
        OmniError::manifest_internal(format!("catalog column '{name}' is null at row {row}"))
    })
}

/// The table columns of a batch or of a struct, borrowed for decoding.
pub(crate) struct TableColumns<'a> {
    locations: &'a StringArray,
    metadata: &'a StringArray,
    table_keys: &'a StringArray,
    stable_table_ids: &'a UInt64Array,
    table_incarnation_ids: &'a UInt64Array,
    table_versions: &'a UInt64Array,
    table_branches: &'a StringArray,
    row_counts: &'a UInt64Array,
    manifest_versions: &'a UInt64Array,
    dropped_at: &'a UInt64Array,
}

impl<'a> TableColumns<'a> {
    pub(crate) fn new(lookup: &dyn Fn(&str) -> Option<&'a ArrayRef>) -> Result<Self> {
        Ok(Self {
            locations: typed(lookup, "location")?,
            metadata: typed(lookup, "metadata")?,
            table_keys: typed(lookup, "table_key")?,
            stable_table_ids: typed(lookup, "stable_table_id")?,
            table_incarnation_ids: typed(lookup, "table_incarnation_id")?,
            table_versions: typed(lookup, "table_version")?,
            table_branches: typed(lookup, "table_branch")?,
            row_counts: typed(lookup, "row_count")?,
            manifest_versions: typed(lookup, "manifest_version")?,
            dropped_at: typed(lookup, "dropped_at")?,
        })
    }

    /// Whether `row` holds no table column, which a commit row requires.
    pub(crate) fn is_null(&self, row: usize) -> bool {
        let strings = [
            self.locations,
            self.metadata,
            self.table_keys,
            self.table_branches,
        ];
        let numbers = [
            self.stable_table_ids,
            self.table_incarnation_ids,
            self.table_versions,
            self.row_counts,
            self.manifest_versions,
            self.dropped_at,
        ];
        strings.iter().all(|column| column.is_null(row))
            && numbers.iter().all(|column| column.is_null(row))
    }

    /// The table at `row`. `written_by` is the dataset version the row was read at, which no
    /// clock of the row may exceed; a `__history` row carries clocks of another dataset and
    /// passes `None`.
    pub(crate) fn decode(&self, row: usize, written_by: Option<u64>) -> Result<TableRow> {
        let raw = self.raw(row)?;
        let table_key = required(raw.table_key.clone(), "table_key", row)?;
        let table_path = required(string(self.locations, row), "location", row)?;
        raw.into_row(table_key, Some(table_path), None, written_by)
    }

    /// The table columns of `row` as stored, before the row is completed.
    pub(crate) fn raw(&self, row: usize) -> Result<RawTable> {
        let identity = TableIdentity::new(
            required(number(self.stable_table_ids, row), "stable_table_id", row)?,
            required(
                number(self.table_incarnation_ids, row),
                "table_incarnation_id",
                row,
            )?,
        )
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "table row {row} has an invalid table identity: {error}"
            ))
        })?;
        Ok(RawTable {
            identity,
            table_key: string(self.table_keys, row),
            metadata: string(self.metadata, row),
            table_version: number(self.table_versions, row),
            table_branch: string(self.table_branches, row),
            row_count: number(self.row_counts, row),
            manifest_version: number(self.manifest_versions, row),
            dropped_at: number(self.dropped_at, row),
        })
    }
}

/// The table columns of one row as stored. A `table` row and a `__history`
/// table carry every field; a `replaced_table` row carries what differs from
/// the identity's `table` row in the same version ([`TableColumnsBuilder::push_replaced`]).
pub(crate) struct RawTable {
    pub(crate) identity: TableIdentity,
    table_key: Option<String>,
    metadata: Option<String>,
    table_version: Option<u64>,
    table_branch: Option<String>,
    row_count: Option<u64>,
    manifest_version: Option<u64>,
    dropped_at: Option<u64>,
}

impl RawTable {
    /// The row `current`, the identity's `table` row of the same version, lets a
    /// `replaced_table` row omit: its key when unchanged, and the metadata the
    /// stored delta applies to.
    pub(crate) fn into_replaced_row(
        self,
        current: Option<&TableRow>,
        written_by: Option<u64>,
    ) -> Result<TableRow> {
        let table_key = match (&self.table_key, current) {
            (Some(key), _) => key.clone(),
            (None, Some(current)) => current.registration.table_key.clone(),
            (None, None) => {
                return Err(OmniError::manifest_internal(format!(
                    "replaced_table row for identity {} names no table key and the version \
                     holds no table row for the identity",
                    self.identity
                )));
            }
        };
        let base = current.and_then(|current| match &current.state {
            TableState::Pinned(pin) => Some(&pin.metadata),
            _ => None,
        });
        self.into_row(table_key, None, base, written_by)
    }

    fn into_row(
        self,
        table_key: String,
        table_path: Option<String>,
        metadata_base: Option<&TableVersionMetadata>,
        written_by: Option<u64>,
    ) -> Result<TableRow> {
        let identity = self.identity;
        let canonical_path = table_path_for_identity(&table_key, identity)?;
        if let Some(table_path) = &table_path
            && *table_path != canonical_path
        {
            return Err(OmniError::manifest_internal(format!(
                "manifest table row for identity {identity} has path '{table_path}', \
                 expected '{canonical_path}'"
            )));
        }
        let state = match (
            self.dropped_at,
            self.table_version,
            self.row_count,
            self.metadata,
            self.manifest_version,
            self.table_branch,
        ) {
            (None, None, None, None, None, None) => TableState::Registered,
            (
                None,
                Some(table_version),
                Some(row_count),
                Some(metadata),
                Some(manifest_version),
                table_branch,
            ) => TableState::Pinned(TablePin {
                table_version,
                table_branch,
                row_count,
                metadata: match metadata_base {
                    Some(base) => apply_metadata_delta(base, &metadata)?,
                    None => TableVersionMetadata::from_json_str(&metadata)?,
                },
                manifest_version,
            }),
            (Some(dropped_at), Some(sealed_version), None, None, None, None) => {
                TableState::Dropped {
                    dropped_at,
                    sealed_version,
                }
            }
            _ => {
                return Err(OmniError::manifest_internal(format!(
                    "table row for {table_key} holds neither a whole pin, nor a drop, nor a \
                     bare registration"
                )));
            }
        };
        let clock = match &state {
            TableState::Registered => None,
            TableState::Pinned(pin) => Some(pin.manifest_version),
            TableState::Dropped { dropped_at, .. } => Some(*dropped_at),
        };
        if let (Some(clock), Some(version)) = (clock, written_by)
            && clock > version
        {
            return Err(OmniError::manifest_internal(format!(
                "manifest row for {table_key} carries manifest version {clock} above the scanned \
                 dataset version {version}"
            )));
        }
        Ok(TableRow {
            registration: TableRegistration {
                identity,
                table_key,
                table_path: canonical_path,
            },
            state,
        })
    }
}

fn metadata_object(metadata: &TableVersionMetadata) -> Result<serde_json::Map<String, Value>> {
    match serde_json::from_str(&metadata.to_json_string()?) {
        Ok(Value::Object(object)) => Ok(object),
        _ => Err(OmniError::manifest_internal(
            "table version metadata is not a JSON object".to_string(),
        )),
    }
}

/// The JSON object a `replaced_table` row stores for a pin's metadata: the
/// members of `before` that `current` lacks or holds differently, and `null`
/// for every member only `current` holds.
pub(crate) fn metadata_delta(
    before: &TableVersionMetadata,
    current: &TableVersionMetadata,
) -> Result<String> {
    let before = metadata_object(before)?;
    let current = metadata_object(current)?;
    Ok(delta_object(&before, &current).to_string())
}

/// The bytes the release measure charges for `manifest_size`, whatever the
/// stored row holds: Lance sizes its manifest by a wall-clock varint, so the
/// measure follows neither the member's digits nor its elision from a delta.
pub(crate) const MANIFEST_SIZE_MEASURE_BYTES: usize =
    r#","manifest_size":18446744073709551615"#.len();

const MANIFEST_SIZE: &str = "manifest_size";

/// The length of a pin's metadata as [`TableColumnsBuilder::push_replaced`]
/// stores it, whole when `current` is absent and as the delta against
/// `current` otherwise, with `manifest_size` at [`MANIFEST_SIZE_MEASURE_BYTES`].
pub(crate) fn metadata_measure(
    before: &TableVersionMetadata,
    current: Option<&TableVersionMetadata>,
) -> Result<usize> {
    let mut before = metadata_object(before)?;
    before.remove(MANIFEST_SIZE);
    let stored = match current {
        Some(current) => {
            let mut current = metadata_object(current)?;
            current.remove(MANIFEST_SIZE);
            delta_object(&before, &current).to_string()
        }
        None => Value::Object(before).to_string(),
    };
    Ok(stored.len() + MANIFEST_SIZE_MEASURE_BYTES)
}

fn delta_object(
    before: &serde_json::Map<String, Value>,
    current: &serde_json::Map<String, Value>,
) -> Value {
    let mut delta = serde_json::Map::new();
    for (key, value) in before {
        if current.get(key) != Some(value) {
            delta.insert(key.clone(), value.clone());
        }
    }
    for key in current.keys() {
        if !before.contains_key(key) {
            delta.insert(key.clone(), Value::Null);
        }
    }
    if let (Some(Value::String(before_path)), Some(Value::String(current_path))) =
        (delta.get(MANIFEST_PATH), current.get(MANIFEST_PATH))
        && let (Some((before_dir, before_file)), Some((current_dir, _))) =
            (before_path.rsplit_once('/'), current_path.rsplit_once('/'))
        && before_dir == current_dir
    {
        let file = Value::String(before_file.to_string());
        delta.remove(MANIFEST_PATH);
        delta.insert(MANIFEST_FILE.to_string(), file);
    }
    Value::Object(delta)
}

const MANIFEST_PATH: &str = "manifest_path";
/// The delta member replacing `manifest_path` when only its file name
/// differs from the current pin's: the name under the current path's directory.
const MANIFEST_FILE: &str = "manifest_file";

/// `base` with the members of `delta` applied: a `null` member is removed.
pub(crate) fn apply_metadata_delta(
    base: &TableVersionMetadata,
    delta: &str,
) -> Result<TableVersionMetadata> {
    let Ok(Value::Object(delta)) = serde_json::from_str(delta) else {
        return Err(OmniError::manifest_internal(format!(
            "replaced_table metadata delta is not a JSON object: {delta}"
        )));
    };
    let mut merged = metadata_object(base)?;
    for (key, value) in delta {
        if key == MANIFEST_FILE {
            let (Value::String(file), Some(Value::String(base_path))) =
                (&value, merged.get(MANIFEST_PATH))
            else {
                return Err(OmniError::manifest_internal(format!(
                    "replaced_table metadata delta names a manifest file over no path: {value}"
                )));
            };
            let directory = base_path
                .rsplit_once('/')
                .map_or("", |(directory, _)| directory);
            let path = Value::String(format!("{directory}/{file}"));
            merged.insert(MANIFEST_PATH.to_string(), path);
        } else if value.is_null() {
            merged.remove(&key);
        } else {
            merged.insert(key, value);
        }
    }
    TableVersionMetadata::from_json_str(&Value::Object(merged).to_string())
}

/// The table columns of rows being written, one entry per row.
#[derive(Default)]
pub(crate) struct TableColumnsBuilder {
    locations: Vec<Option<String>>,
    metadata: Vec<Option<String>>,
    table_keys: Vec<Option<String>>,
    stable_table_ids: Vec<Option<u64>>,
    table_incarnation_ids: Vec<Option<u64>>,
    table_versions: Vec<Option<u64>>,
    table_branches: Vec<Option<String>>,
    row_counts: Vec<Option<u64>>,
    manifest_versions: Vec<Option<u64>>,
    dropped_at: Vec<Option<u64>>,
}

impl TableColumnsBuilder {
    pub(crate) fn push(&mut self, row: &TableRow) -> Result<()> {
        row.registration.identity.validate()?;
        self.locations
            .push(Some(row.registration.table_path.clone()));
        self.table_keys
            .push(Some(row.registration.table_key.clone()));
        self.stable_table_ids
            .push(Some(row.registration.identity.stable_table_id));
        self.table_incarnation_ids
            .push(Some(row.registration.identity.table_incarnation_id));
        let (pin, dropped_at, sealed_version) = match &row.state {
            TableState::Registered => (None, None, None),
            TableState::Pinned(pin) => (Some(pin), None, None),
            TableState::Dropped {
                dropped_at,
                sealed_version,
            } => (None, Some(*dropped_at), Some(*sealed_version)),
        };
        self.metadata
            .push(pin.map(|pin| pin.metadata.to_json_string()).transpose()?);
        self.table_versions
            .push(pin.map(|pin| pin.table_version).or(sealed_version));
        self.table_branches
            .push(pin.and_then(|pin| pin.table_branch.clone()));
        self.row_counts.push(pin.map(|pin| pin.row_count));
        self.manifest_versions
            .push(pin.map(|pin| pin.manifest_version));
        self.dropped_at.push(dropped_at);
        Ok(())
    }

    /// A `replaced_table` row: `location` is derived from the key and identity
    /// on read, `table_key` is stored only when `current` (the identity's
    /// `table` row of the same version) has another, and a pin's metadata is
    /// stored as its delta against `current`'s pin ([`metadata_delta`]).
    pub(crate) fn push_replaced(
        &mut self,
        row: &TableRow,
        current: Option<&TableRow>,
    ) -> Result<()> {
        self.push(row)?;
        let last = self.locations.len() - 1;
        self.locations[last] = None;
        let Some(current) = current else {
            return Ok(());
        };
        if current.registration.table_key == row.registration.table_key {
            self.table_keys[last] = None;
        }
        if let (TableState::Pinned(before), TableState::Pinned(base)) = (&row.state, &current.state)
        {
            self.metadata[last] = Some(metadata_delta(&before.metadata, &base.metadata)?);
        }
        Ok(())
    }

    pub(crate) fn push_contract(&mut self, head: &crate::SchemaContractHead) -> Result<()> {
        self.push_null();
        if let Some(metadata) = self.metadata.last_mut() {
            *metadata = Some(
                serde_json::to_string(head)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
            );
        }
        Ok(())
    }

    pub(crate) fn push_null(&mut self) {
        self.locations.push(None);
        self.metadata.push(None);
        self.table_keys.push(None);
        self.stable_table_ids.push(None);
        self.table_incarnation_ids.push(None);
        self.table_versions.push(None);
        self.table_branches.push(None);
        self.row_counts.push(None);
        self.manifest_versions.push(None);
        self.dropped_at.push(None);
    }

    /// The columns in the order of [`TABLE_FIELDS`].
    pub(crate) fn finish(self) -> [ArrayRef; TABLE_FIELDS.len()] {
        [
            Arc::new(StringArray::from(self.locations)),
            Arc::new(StringArray::from(self.metadata)),
            Arc::new(StringArray::from(self.table_keys)),
            Arc::new(UInt64Array::from(self.stable_table_ids)),
            Arc::new(UInt64Array::from(self.table_incarnation_ids)),
            Arc::new(UInt64Array::from(self.table_versions)),
            Arc::new(StringArray::from(self.table_branches)),
            Arc::new(UInt64Array::from(self.row_counts)),
            Arc::new(UInt64Array::from(self.manifest_versions)),
            Arc::new(UInt64Array::from(self.dropped_at)),
        ]
    }
}

/// The commit columns of a batch, borrowed for decoding.
pub(crate) struct CommitColumns<'a> {
    graph_branches: &'a StringArray,
    native_branches: &'a StringArray,
    graph_manifest_versions: &'a UInt64Array,
    generations: &'a UInt64Array,
    parent_commit_ids: &'a StringArray,
    merged_parent_commit_ids: &'a StringArray,
    actor_ids: &'a StringArray,
    created_at: &'a Int64Array,
    schema_ir_hash: &'a StringArray,
    schema_identity_version: &'a UInt64Array,
    schema_identity_domain: &'a StringArray,
    schema_content_hash: &'a StringArray,
}

impl<'a> CommitColumns<'a> {
    pub(crate) fn new(lookup: &dyn Fn(&str) -> Option<&'a ArrayRef>) -> Result<Self> {
        Ok(Self {
            graph_branches: typed(lookup, "graph_branch")?,
            native_branches: typed(lookup, "native_branch")?,
            graph_manifest_versions: typed(lookup, "graph_manifest_version")?,
            generations: typed(lookup, "generation")?,
            parent_commit_ids: typed(lookup, "parent_commit_id")?,
            merged_parent_commit_ids: typed(lookup, "merged_parent_commit_id")?,
            actor_ids: typed(lookup, "actor_id")?,
            created_at: typed(lookup, "created_at")?,
            schema_ir_hash: typed(lookup, "schema_ir_hash")?,
            schema_identity_version: typed(lookup, "schema_identity_version")?,
            schema_identity_domain: typed(lookup, "schema_identity_domain")?,
            schema_content_hash: typed(lookup, "schema_content_hash")?,
        })
    }

    /// Whether `row` holds no commit column, which a `table` row requires.
    pub(crate) fn is_null(&self, row: usize) -> bool {
        let strings = [
            self.graph_branches,
            self.native_branches,
            self.parent_commit_ids,
            self.merged_parent_commit_ids,
            self.actor_ids,
            self.schema_ir_hash,
            self.schema_identity_domain,
            self.schema_content_hash,
        ];
        strings.iter().all(|column| column.is_null(row))
            && self.graph_manifest_versions.is_null(row)
            && self.generations.is_null(row)
            && self.created_at.is_null(row)
            && self.schema_identity_version.is_null(row)
    }

    /// The commit at `row`, and whether the row inherits its schema contract
    /// and content hash from the next newer commit ([`CommitColumnsBuilder::push_settled`]).
    pub(crate) fn decode(
        &self,
        row: usize,
        graph_commit_id: &str,
    ) -> Result<(GraphLineageRow, bool)> {
        if graph_commit_id.is_empty() {
            return Err(OmniError::manifest_internal(format!(
                "graph commit at row {row} has an empty id"
            )));
        }
        let mut inherits = false;
        let schema_contract = match (
            string(self.schema_ir_hash, row),
            number(self.schema_identity_version, row),
            string(self.schema_identity_domain, row),
        ) {
            (Some(schema_ir_hash), Some(version), Some(schema_identity_domain)) => {
                Some(crate::SchemaContractHead {
                    schema_ir_hash,
                    schema_identity_version: u32::try_from(version).map_err(|_| {
                        OmniError::manifest_internal(
                            "schema identity version exceeds u32".to_string(),
                        )
                    })?,
                    schema_identity_domain,
                })
            }
            (None, None, None) => None,
            (None, Some(INHERITED_SCHEMA_IDENTITY_VERSION), None) => {
                inherits = true;
                None
            }
            _ => {
                return Err(OmniError::manifest_internal(
                    "commit carries a partial schema contract identity".to_string(),
                ));
            }
        };
        let schema_content_hash = string(self.schema_content_hash, row);
        if schema_contract.is_some() != schema_content_hash.is_some()
            || (inherits && schema_content_hash.is_some())
        {
            return Err(OmniError::manifest_internal(format!(
                "graph commit '{graph_commit_id}' carries a schema identity without the name of \
                 its archived schema content, or the name without the identity"
            )));
        }
        let commit = GraphLineageRow {
            graph_commit_id: graph_commit_id.to_string(),
            schema_contract,
            schema_content_hash,
            graph_branch: string(self.graph_branches, row),
            native_branch: string(self.native_branches, row),
            graph_manifest_version: required(
                number(self.graph_manifest_versions, row),
                "graph_manifest_version",
                row,
            )?,
            generation: required(number(self.generations, row), "generation", row)?,
            parent_commit_id: string(self.parent_commit_ids, row),
            merged_parent_commit_id: string(self.merged_parent_commit_ids, row),
            actor_id: string(self.actor_ids, row),
            created_at: required(
                self.created_at
                    .is_valid(row)
                    .then(|| self.created_at.value(row)),
                "created_at",
                row,
            )?,
        };
        Ok((commit, inherits))
    }
}

/// The `schema_identity_version` a `settled_commit` row stores, with the
/// three schema strings null, when its contract and content hash are those of
/// the next newer commit; a contract's own version is 1 or more.
pub(crate) const INHERITED_SCHEMA_IDENTITY_VERSION: u64 = 0;

/// The commit columns of rows being written, one entry per row.
#[derive(Default)]
pub(crate) struct CommitColumnsBuilder {
    graph_branches: Vec<Option<String>>,
    native_branches: Vec<Option<String>>,
    graph_manifest_versions: Vec<Option<u64>>,
    generations: Vec<Option<u64>>,
    parent_commit_ids: Vec<Option<String>>,
    merged_parent_commit_ids: Vec<Option<String>>,
    actor_ids: Vec<Option<String>>,
    created_at: Vec<Option<i64>>,
    schema_ir_hash: Vec<Option<String>>,
    schema_identity_version: Vec<Option<u64>>,
    schema_identity_domain: Vec<Option<String>>,
    schema_content_hash: Vec<Option<String>>,
}

impl CommitColumnsBuilder {
    pub(crate) fn push(&mut self, commit: &GraphLineageRow) {
        self.graph_branches.push(commit.graph_branch.clone());
        self.native_branches.push(commit.native_branch.clone());
        self.graph_manifest_versions
            .push(Some(commit.graph_manifest_version));
        self.generations.push(Some(commit.generation));
        self.parent_commit_ids.push(commit.parent_commit_id.clone());
        self.merged_parent_commit_ids
            .push(commit.merged_parent_commit_id.clone());
        self.actor_ids.push(commit.actor_id.clone());
        self.created_at.push(Some(commit.created_at));
        self.schema_ir_hash.push(
            commit
                .schema_contract
                .as_ref()
                .map(|head| head.schema_ir_hash.clone()),
        );
        self.schema_identity_version.push(
            commit
                .schema_contract
                .as_ref()
                .map(|head| u64::from(head.schema_identity_version)),
        );
        self.schema_identity_domain.push(
            commit
                .schema_contract
                .as_ref()
                .map(|head| head.schema_identity_domain.clone()),
        );
        self.schema_content_hash
            .push(commit.schema_content_hash.clone());
    }

    /// A `settled_commit` row: `parent_commit_id` is left null when it names
    /// `previous`, the buffered commit before it, and the schema contract
    /// and content hash are left null with `schema_identity_version` =
    /// [`INHERITED_SCHEMA_IDENTITY_VERSION`] when they equal `child`'s, the
    /// next newer commit (the head after the newest buffered one).
    pub(crate) fn push_settled(
        &mut self,
        commit: &GraphLineageRow,
        previous: Option<&GraphLineageRow>,
        child: &GraphLineageRow,
    ) {
        self.push(commit);
        let last = self.graph_branches.len() - 1;
        if previous.is_some_and(|previous| {
            commit.parent_commit_id.as_deref() == Some(previous.graph_commit_id.as_str())
        }) {
            self.parent_commit_ids[last] = None;
        }
        if commit.schema_contract == child.schema_contract
            && commit.schema_content_hash == child.schema_content_hash
        {
            self.schema_ir_hash[last] = None;
            self.schema_identity_version[last] = Some(INHERITED_SCHEMA_IDENTITY_VERSION);
            self.schema_identity_domain[last] = None;
            self.schema_content_hash[last] = None;
        }
    }

    pub(crate) fn push_null(&mut self) {
        self.graph_branches.push(None);
        self.native_branches.push(None);
        self.graph_manifest_versions.push(None);
        self.generations.push(None);
        self.parent_commit_ids.push(None);
        self.merged_parent_commit_ids.push(None);
        self.actor_ids.push(None);
        self.created_at.push(None);
        self.schema_ir_hash.push(None);
        self.schema_identity_version.push(None);
        self.schema_identity_domain.push(None);
        self.schema_content_hash.push(None);
    }

    /// The columns in the order of [`COMMIT_FIELDS`].
    pub(crate) fn finish(self) -> [ArrayRef; COMMIT_FIELDS.len()] {
        [
            Arc::new(StringArray::from(self.graph_branches)),
            Arc::new(StringArray::from(self.native_branches)),
            Arc::new(UInt64Array::from(self.graph_manifest_versions)),
            Arc::new(UInt64Array::from(self.generations)),
            Arc::new(StringArray::from(self.parent_commit_ids)),
            Arc::new(StringArray::from(self.merged_parent_commit_ids)),
            Arc::new(StringArray::from(self.actor_ids)),
            Arc::new(Int64Array::from(self.created_at)),
            Arc::new(StringArray::from(self.schema_ir_hash)),
            Arc::new(UInt64Array::from(self.schema_identity_version)),
            Arc::new(StringArray::from(self.schema_identity_domain)),
            Arc::new(StringArray::from(self.schema_content_hash)),
        ]
    }
}

/// When a `replaced_table` row was replaced, and whether the publish that
/// replaced it registered the identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplacedClock {
    pub(crate) replaced_at: u64,
    pub(crate) registered: bool,
}

/// The columns only a `replaced_table` row fills, borrowed for decoding.
pub(crate) struct ReplacedColumns<'a> {
    replaced_at: &'a UInt64Array,
    registered_at: &'a UInt64Array,
}

impl<'a> ReplacedColumns<'a> {
    pub(crate) fn new(lookup: &dyn Fn(&str) -> Option<&'a ArrayRef>) -> Result<Self> {
        Ok(Self {
            replaced_at: typed(lookup, "replaced_at")?,
            registered_at: typed(lookup, "registered_at")?,
        })
    }

    /// Whether `row` holds neither column, which every other row type requires.
    pub(crate) fn is_null(&self, row: usize) -> bool {
        self.replaced_at.is_null(row) && self.registered_at.is_null(row)
    }

    pub(crate) fn decode(&self, row: usize) -> Result<ReplacedClock> {
        let replaced_at = required(number(self.replaced_at, row), "replaced_at", row)?;
        let registered_at = number(self.registered_at, row);
        if registered_at.is_some_and(|registered_at| registered_at != replaced_at) {
            return Err(OmniError::manifest_internal(format!(
                "replaced_table row {row} is replaced at {replaced_at} and registered at \
                 {registered_at:?}; the publish that registers an identity is the one that \
                 replaces its absence"
            )));
        }
        Ok(ReplacedClock {
            replaced_at,
            registered: registered_at.is_some(),
        })
    }
}

/// The columns of [`REPLACED_FIELDS`] of rows being written, one entry per row.
#[derive(Default)]
pub(crate) struct ReplacedColumnsBuilder {
    replaced_at: Vec<Option<u64>>,
    registered_at: Vec<Option<u64>>,
}

impl ReplacedColumnsBuilder {
    pub(crate) fn push(&mut self, clock: ReplacedClock) {
        self.replaced_at.push(Some(clock.replaced_at));
        self.registered_at
            .push(clock.registered.then_some(clock.replaced_at));
    }

    pub(crate) fn push_null(&mut self) {
        self.replaced_at.push(None);
        self.registered_at.push(None);
    }

    /// The columns in the order of [`REPLACED_FIELDS`].
    pub(crate) fn finish(self) -> [ArrayRef; REPLACED_FIELDS.len()] {
        [
            Arc::new(UInt64Array::from(self.replaced_at)),
            Arc::new(UInt64Array::from(self.registered_at)),
        ]
    }
}
