//! The one-pass judgment of a load plan's `Validate` node: every check's
//! facts are extracted from each delta batch once while it is hot, then the
//! checks are judged in list order (the refusal precedence) after the last
//! batch, each consulting the committed image under its physical access. The
//! per-batch builders and the finalizers are the evaluator's kernels, so this
//! route and the mutation evaluator cannot judge the same rows differently.

use std::collections::{HashMap, HashSet};

use arrow_array::{Array, RecordBatch, StringArray};
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::{Catalog, EdgeType};
use omnigraph_compiler::ir::{Endpoint, ValidateCheck};
use omnigraph_compiler::types::ScalarType;
use omnigraph_planner::{ValidateAccess, ValidateSpec};

use super::kernels::{
    CardPlan, UniqueImageBuilder, card_affected, card_observe, card_violations, orphan_violation,
    orphans_retained, unique_coalesced_dups, unique_committed_collisions, unique_probe_keys,
};
use super::key_set::{IntKeySet, canonical_i64, key_bytes};
use super::{
    AccessPath, CommittedState, TableChange, Violation, integer_capacity_error,
    overwrite_removed_ids, string_col, value_violation,
};
use crate::db::Snapshot;
use crate::error::{OmniError, Result};
use crate::loader::read_reuse::LoadReadReuse;
use crate::loader::{LoadMode, validate_enum_constraints, validate_value_constraints};

pub(crate) enum LoadPhase {
    RemovedInput,
    Extracted,
}

pub(super) type ByteAdmission<'a> = dyn FnMut(usize) -> Result<Box<dyn Send>> + Send + 'a;

type PhaseAdmission<'a> = dyn FnMut(LoadPhase) -> Result<Box<dyn Send>> + Send + 'a;
type ReadReuseAdmission<'a> = dyn FnMut() -> Option<LoadReadReuse> + Send + 'a;

pub(crate) struct LoadAdmission<'a> {
    pub(crate) phase: Option<&'a mut PhaseAdmission<'a>>,
    pub(crate) bytes: Option<&'a mut ByteAdmission<'a>>,
    pub(crate) reuse: Option<&'a mut ReadReuseAdmission<'a>>,
    pub(crate) removed: &'a mut (dyn FnMut(&str) -> Result<()> + Send),
    pub(crate) retained: &'a mut (dyn FnMut(&RecordBatch) -> Result<()> + Send),
}

impl LoadAdmission<'_> {
    fn reserve_bytes(&mut self, bytes: usize) -> Result<Box<dyn Send>> {
        match self.bytes.as_mut() {
            Some(admit) => admit(bytes),
            None => Ok(Box::new(())),
        }
    }
}

/// Judge `batches`, the whole delta of `table`, against `checks` in order.
/// `Ok(())` admits the delta; the first violation in check order, then in
/// the check's own order, is the error, with the evaluator's text.
/// Admission charges removed ids and the first retained edge before copying them.
pub(crate) async fn judge_load(
    table: &str,
    checks: &[ValidateSpec],
    catalog: &Catalog,
    base: &Snapshot,
    mode: LoadMode,
    batches: &[RecordBatch],
    admission: &mut LoadAdmission<'_>,
) -> Result<()> {
    let system_columns = catalog.system_columns;
    let overwrite = mode == LoadMode::Overwrite;
    let removed = if overwrite {
        let _phase = admission
            .phase
            .as_mut()
            .map(|admit| admit(LoadPhase::RemovedInput))
            .transpose()?;
        let change = TableChange {
            added: batches.to_vec(),
            ..TableChange::default()
        };
        overwrite_removed_ids(base, table, &change, system_columns, |id| {
            (admission.removed)(id)
        })
        .await?
    } else {
        Vec::new()
    };
    let _phase = admission
        .phase
        .as_mut()
        .map(|admit| admit(LoadPhase::Extracted))
        .transpose()?;
    let mut extracted =
        Extracted::sweep(table, checks, catalog, batches, system_columns, overwrite)?;
    let mut reuse = admission.reuse.as_mut().and_then(|admit| admit());
    let mut cached_base = reuse.as_ref().map(|reuse| {
        let mut snapshot = base.clone();
        snapshot.set_read_caches(reuse.snapshot_caches());
        snapshot
    });
    let integer_endpoints = checks.iter().any(|spec| {
        matches!(&spec.check, ValidateCheck::EdgeRi { node_type, .. } if int_keyed(catalog, node_type))
    });
    let mut first = FirstViolation::default();
    let mut edge_ri_judged = false;
    let mut retained_judged: HashSet<&str> = HashSet::new();
    for (index, spec) in checks.iter().enumerate() {
        if matches!(spec.check, ValidateCheck::RetainedEdges { .. })
            || (integer_endpoints && matches!(spec.check, ValidateCheck::EdgeRi { .. }))
        {
            drop(cached_base.take());
            drop(reuse.take());
        }
        let committed = CommittedState {
            committed: Some(cached_base.as_ref().unwrap_or(base)),
            overwritten: overwrite.then(|| table.to_string()).into_iter().collect(),
            live: None,
        };
        match &spec.check {
            ValidateCheck::Value { .. } => first.refuse(extracted.value.take())?,
            ValidateCheck::Unique { columns, .. } => {
                let (builder, within_batch) = extracted
                    .unique
                    .remove(&index)
                    .ok_or_else(|| missing_facts(spec))?;
                first.refuse(within_batch)?;
                let image = builder.finish();
                unique_coalesced_dups(table, columns, &image.entries, &mut first.sink())?;
                first.refuse_pending()?;
                if spec.consults.is_some() {
                    let probe = unique_probe_keys(&image.entries);
                    let holders = committed
                        .unique_holders(table, columns, &probe, system_columns, access_path(spec)?)
                        .await?;
                    unique_committed_collisions(
                        table,
                        columns,
                        &image.entries,
                        &holders,
                        &extracted.ids,
                        &HashSet::new(),
                        &mut first.sink(),
                    )?;
                    first.refuse_pending()?;
                }
            }
            ValidateCheck::EdgeRi { .. } => {
                if edge_ri_judged || batches.iter().all(|batch| batch.num_rows() == 0) {
                    continue;
                }
                edge_ri_judged = true;
                let edge_type = edge_type_of(catalog, table)?;
                let access_of = |wanted: Endpoint| -> Result<AccessPath> {
                    let spec = checks
                        .iter()
                        .find(|spec| {
                            matches!(&spec.check, ValidateCheck::EdgeRi { endpoint, .. } if *endpoint == wanted)
                        })
                        .unwrap_or(spec);
                    access_path(spec)
                };
                let from_keys = EndpointKeys::load(
                    &committed,
                    catalog,
                    &edge_type.from_type,
                    batches,
                    system_columns.src,
                    system_columns,
                    access_of(Endpoint::From)?,
                    admission,
                )
                .await?;
                let to_keys = EndpointKeys::load(
                    &committed,
                    catalog,
                    &edge_type.to_type,
                    batches,
                    system_columns.dst,
                    system_columns,
                    access_of(Endpoint::To)?,
                    admission,
                )
                .await?;
                first_orphan(
                    table,
                    &edge_type.from_type,
                    &edge_type.to_type,
                    batches,
                    &from_keys,
                    &to_keys,
                    system_columns,
                    admission,
                    &mut first.sink(),
                )?;
                first.refuse_pending()?;
            }
            ValidateCheck::Cardinality { .. } => {
                let edge_type = edge_type_of(catalog, table)?;
                if edge_type.cardinality.is_default() {
                    continue;
                }
                let delta_by_id = std::mem::take(&mut extracted.card);
                let plan = CardPlan {
                    changed_ids: delta_by_id.keys().cloned().collect(),
                    delta_by_id,
                    removed_ids: Vec::new(),
                };
                let moved_from = committed
                    .committed_edges(table, system_columns.id, &plan.changed_ids, system_columns)
                    .await?;
                let affected = card_affected(&plan, &[], &moved_from, &HashSet::new());
                if affected.is_empty() {
                    continue;
                }
                let affected_ids: Vec<String> = affected.iter().cloned().collect();
                let committed_for_affected = committed
                    .committed_edges(table, system_columns.src, &affected_ids, system_columns)
                    .await?;
                card_violations(
                    table,
                    edge_type,
                    &plan,
                    &affected,
                    &committed_for_affected,
                    &mut first.sink(),
                )?;
                first.refuse_pending()?;
            }
            ValidateCheck::RetainedEdges { edge_table, .. } => {
                if !retained_judged.insert(edge_table) {
                    continue;
                }
                let edge_type = edge_type_of(catalog, edge_table)?;
                let deleted_at = |wanted: Endpoint| -> &[String] {
                    let listed = checks.iter().any(|spec| {
                        matches!(&spec.check, ValidateCheck::RetainedEdges { edge_table: table, endpoint }
                            if table == edge_table && *endpoint == wanted)
                    });
                    if listed { removed.as_slice() } else { &[] }
                };
                let deleted_from = deleted_at(Endpoint::From);
                let deleted_to = deleted_at(Endpoint::To);
                let retained = committed
                    .first_edge_referencing(
                        edge_table,
                        deleted_from,
                        deleted_to,
                        system_columns,
                        admission.retained,
                    )
                    .await?;
                orphans_retained(
                    edge_table,
                    &edge_type.from_type,
                    &edge_type.to_type,
                    &retained,
                    &HashSet::new(),
                    &HashSet::new(),
                    deleted_from,
                    deleted_to,
                    system_columns,
                    &mut first.sink(),
                )?;
                first.refuse_pending()?;
            }
        }
    }
    Ok(())
}

/// What one sweep over the delta extracts for every check at once.
#[derive(Default)]
struct Extracted {
    /// The first value violation in batch order.
    value: Option<Violation>,
    /// Every non-null id of the delta.
    ids: HashSet<String>,
    /// Per uniqueness check (by list index): its image under construction and
    /// the first key repeated anywhere in the input.
    unique: HashMap<usize, (UniqueImageBuilder, Option<Violation>)>,
    /// Last-wins src per edge id, when a cardinality check reads it.
    card: HashMap<String, String>,
}

impl Extracted {
    fn sweep(
        table: &str,
        checks: &[ValidateSpec],
        catalog: &Catalog,
        batches: &[RecordBatch],
        system_columns: SystemColumns,
        overwrite: bool,
    ) -> Result<Self> {
        let mut extracted = Self::default();
        let needs_value = checks
            .iter()
            .any(|spec| matches!(spec.check, ValidateCheck::Value { .. }));
        let needs_ids = checks.iter().any(|spec| {
            matches!(spec.check, ValidateCheck::Unique { .. }) && spec.consults.is_some()
        });
        let needs_card = checks
            .iter()
            .any(|spec| matches!(spec.check, ValidateCheck::Cardinality { .. }));
        let unique_checks: Vec<(usize, &[String])> = checks
            .iter()
            .enumerate()
            .filter_map(|(index, spec)| match &spec.check {
                ValidateCheck::Unique { columns, .. } => Some((index, columns.as_slice())),
                _ => None,
            })
            .collect();
        for &(index, _) in &unique_checks {
            extracted.unique.insert(
                index,
                (
                    if overwrite {
                        UniqueImageBuilder::for_replacement()
                    } else {
                        UniqueImageBuilder::for_input()
                    },
                    None,
                ),
            );
        }
        let type_name = table.split_once(':').map(|(_, name)| name).unwrap_or(table);
        for batch in batches {
            if needs_value && extracted.value.is_none() {
                extracted.value = value_check(table, type_name, catalog, batch)
                    .map(|err| value_violation(table, err));
            }
            if needs_ids {
                let ids = string_col(batch, system_columns.id)?;
                for row in 0..ids.len() {
                    if !ids.is_null(row) {
                        extracted.ids.insert(ids.value(row).to_string());
                    }
                }
            }
            for &(index, columns) in &unique_checks {
                let (builder, within_batch) = extracted
                    .unique
                    .get_mut(&index)
                    .expect("every uniqueness check has a builder");
                builder.observe(table, columns, batch, system_columns, &mut |violation| {
                    within_batch.get_or_insert(violation);
                    Ok(())
                })?;
            }
            if needs_card {
                card_observe(batch, system_columns, &mut extracted.card)?;
            }
        }
        Ok(extracted)
    }
}

/// The evaluator's row-local value judgment of one batch: `@range`/`@check`
/// then enum membership on a node, enum membership on an edge.
fn value_check(
    table: &str,
    type_name: &str,
    catalog: &Catalog,
    batch: &RecordBatch,
) -> Option<OmniError> {
    if table.starts_with("node:") {
        let node_type = catalog.node_types.get(type_name)?;
        validate_value_constraints(batch, node_type)
            .err()
            .or_else(|| validate_enum_constraints(batch, &node_type.properties, type_name).err())
    } else {
        let edge_type = catalog.edge_types.get(type_name)?;
        validate_enum_constraints(batch, &edge_type.properties, type_name).err()
    }
}

/// The first violation a finalizer reports, kept until the check is settled.
#[derive(Default)]
struct FirstViolation {
    pending: Option<Violation>,
}

impl FirstViolation {
    fn sink(&mut self) -> impl FnMut(Violation) -> Result<()> + '_ {
        move |violation| {
            self.pending.get_or_insert(violation);
            Ok(())
        }
    }

    fn refuse_pending(&mut self) -> Result<()> {
        let pending = self.pending.take();
        self.refuse(pending)
    }

    fn refuse(&mut self, violation: Option<Violation>) -> Result<()> {
        match violation {
            Some(violation) => Err(violation.into_omni_error()),
            None => Ok(()),
        }
    }
}

fn access_path(spec: &ValidateSpec) -> Result<AccessPath> {
    match spec.access {
        Some(ValidateAccess::Probe) => Ok(AccessPath::Probe),
        Some(ValidateAccess::Scan) => Ok(AccessPath::Scan),
        other => Err(OmniError::manifest_internal(format!(
            "validate check {} consults a committed image under access {other:?}",
            spec.check.name()
        ))),
    }
}

fn missing_facts(spec: &ValidateSpec) -> OmniError {
    OmniError::manifest_internal(format!(
        "validate check {} has no extracted facts",
        spec.check.name()
    ))
}

/// The committed ids of one endpoint table, as the probe reads them: an
/// integer key set when the node type's `@key` is one `I32`/`I64`/`U32`
/// property (its ids are canonical decimals), the id strings otherwise.
enum EndpointKeys {
    Int {
        keys: IntKeySet,
        _memory: Box<dyn Send>,
    },
    Text(HashSet<String>),
}

impl EndpointKeys {
    async fn load(
        committed: &CommittedState<'_>,
        catalog: &Catalog,
        node_type: &str,
        batches: &[RecordBatch],
        column: &str,
        system_columns: SystemColumns,
        access: AccessPath,
        admission: &mut LoadAdmission<'_>,
    ) -> Result<Self> {
        let table_key = format!("node:{node_type}");
        let _wanted_memory = match access {
            AccessPath::Probe => admission.reserve_bytes(endpoint_value_bytes(batches, column)?)?,
            AccessPath::Scan => Box::new(()),
        };
        if int_keyed(catalog, node_type) {
            let wanted = match access {
                AccessPath::Probe => endpoint_values(batches, column)?,
                AccessPath::Scan => Vec::new(),
            };
            let (keys, memory) = committed
                .existing_int_keys(&table_key, &wanted, system_columns, access, &mut |bytes| {
                    admission.reserve_bytes(bytes)
                })
                .await?;
            return Ok(Self::Int {
                keys,
                _memory: memory,
            });
        }
        let values = endpoint_values(batches, column)?;
        let ids = committed
            .existing_ids(&table_key, &values, system_columns, access)
            .await?;
        Ok(Self::Text(ids))
    }

    /// The first row of `column` naming no committed id (a null or a
    /// non-canonical spelling misses). Integer keys are parsed for the whole
    /// column into `parsed` first, so the probes run as independent loads.
    fn first_miss(&self, column: &StringArray, parsed: &mut Vec<i64>) -> Option<usize> {
        match self {
            Self::Int { keys, .. } => {
                parsed.clear();
                let mut unparsed = None;
                for row in 0..column.len() {
                    let key = (!column.is_null(row))
                        .then(|| canonical_i64(column.value(row)))
                        .flatten();
                    match key {
                        Some(key) => parsed.push(key),
                        None => {
                            unparsed = Some(row);
                            break;
                        }
                    }
                }
                parsed
                    .iter()
                    .position(|&key| !keys.contains(key))
                    .or(unparsed)
            }
            Self::Text(ids) => (0..column.len())
                .find(|&row| column.is_null(row) || !ids.contains(column.value(row))),
        }
    }
}

pub(crate) fn int_keyed(catalog: &Catalog, node_type: &str) -> bool {
    catalog
        .node_types
        .get(node_type)
        .and_then(|node| match node.key.as_deref() {
            Some([key]) => node.properties.get(key),
            _ => None,
        })
        .is_some_and(|prop| {
            !prop.list
                && matches!(
                    prop.scalar,
                    ScalarType::I32 | ScalarType::I64 | ScalarType::U32
                )
        })
}

/// What `endpoint_values` allocates: one `String` per cell plus its text.
fn endpoint_value_bytes(batches: &[RecordBatch], column: &str) -> Result<usize> {
    let mut bytes = 0usize;
    for batch in batches {
        let endpoints = string_col(batch, column)?;
        let text = endpoints
            .iter()
            .flatten()
            .fold(0usize, |sum, value| sum.saturating_add(value.len()));
        bytes = bytes
            .saturating_add(text)
            .saturating_add(endpoints.len().saturating_mul(size_of::<String>()));
    }
    Ok(bytes)
}

fn endpoint_values(batches: &[RecordBatch], column: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let endpoints = string_col(batch, column)?;
        values.extend((0..endpoints.len()).map(|row| endpoints.value(row).to_string()));
    }
    Ok(values)
}

/// The first delta edge, in row order and src before dst, whose endpoint is
/// absent from its node table: the one violation the endpoint check reports.
fn first_orphan<F>(
    edge_table: &str,
    from_type: &str,
    to_type: &str,
    batches: &[RecordBatch],
    from_keys: &EndpointKeys,
    to_keys: &EndpointKeys,
    system_columns: SystemColumns,
    admission: &mut LoadAdmission<'_>,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let parsed_len = if matches!(from_keys, EndpointKeys::Int { .. })
        || matches!(to_keys, EndpointKeys::Int { .. })
    {
        batches.iter().map(RecordBatch::num_rows).max().unwrap_or(0)
    } else {
        0
    };
    let _parsed_memory = admission
        .reserve_bytes(key_bytes(parsed_len).ok_or_else(|| integer_capacity_error(parsed_len))?)?;
    let mut parsed = Vec::with_capacity(parsed_len);
    for batch in batches {
        let ids = string_col(batch, system_columns.id)?;
        let srcs = string_col(batch, system_columns.src)?;
        let dsts = string_col(batch, system_columns.dst)?;
        let src_miss = from_keys.first_miss(srcs, &mut parsed);
        let dst_miss = to_keys.first_miss(dsts, &mut parsed);
        let (row, column, values, node_type) = match (src_miss, dst_miss) {
            (None, None) => continue,
            (Some(src), Some(dst)) if dst < src => (dst, system_columns.dst, dsts, to_type),
            (Some(src), _) => (src, system_columns.src, srcs, from_type),
            (None, Some(dst)) => (dst, system_columns.dst, dsts, to_type),
        };
        return sink(orphan_violation(
            edge_table,
            ids.value(row),
            column,
            values.value(row),
            node_type,
        ));
    }
    Ok(())
}

fn edge_type_of<'c>(catalog: &'c Catalog, edge_table: &str) -> Result<&'c EdgeType> {
    edge_table
        .strip_prefix("edge:")
        .and_then(|name| catalog.edge_types.get(name))
        .ok_or_else(|| OmniError::manifest(format!("load csv: unknown edge table '{edge_table}'")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::ir::lower_load;
    use omnigraph_compiler::query::ast::{LoadStmt, LoadStmtMode, LoadTarget};
    use omnigraph_compiler::settings::SessionSettings;
    use omnigraph_planner::ConsultedImage;

    use super::*;
    use crate::Session;
    use crate::db::Omnigraph;

    const GRAPH_SCHEMA: &str = "node Person {\n  name: String @key\n  email: String @unique\n}\nedge Knows: Person -> Person @card(0..2)\n";

    const COMMITTED: &str = r#"{"type": "Person", "data": {"name": "Alice", "email": "alice@x"}}
{"type": "Person", "data": {"name": "Bob", "email": "bob@x"}}
{"type": "Person", "data": {"name": "Carol", "email": "carol@x"}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
{"edge": "Knows", "from": "Alice", "to": "Carol"}
"#;

    async fn committed_graph(dir: &tempfile::TempDir) -> Session {
        let db = Session::from_defaults(
            Arc::new(
                Omnigraph::init(dir.path().to_str().unwrap(), GRAPH_SCHEMA)
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        db.load_jsonl(COMMITTED, LoadMode::Overwrite).await.unwrap();
        db
    }

    /// The physical checks of a load statement, every consulting check under
    /// `access`.
    fn checks(
        catalog: &Catalog,
        target: LoadTarget,
        mode: LoadStmtMode,
        access: ValidateAccess,
    ) -> Vec<ValidateSpec> {
        let stmt = LoadStmt {
            source: "file:///input.csv".to_string(),
            target,
            mode,
            delimiter: b',',
            header: true,
            columns: None,
        };
        lower_load(catalog, &stmt)
            .checks
            .into_iter()
            .map(|check| ValidateSpec {
                access: Some(if check.consults.is_some() {
                    access
                } else {
                    ValidateAccess::Delta
                }),
                consults: check.consults.map(|table| ConsultedImage {
                    table,
                    version: None,
                }),
                check: check.check,
            })
            .collect()
    }

    fn load_mode(mode: LoadStmtMode) -> LoadMode {
        match mode {
            LoadStmtMode::Overwrite => LoadMode::Overwrite,
            LoadStmtMode::Append => LoadMode::Append,
            LoadStmtMode::Merge => LoadMode::Merge,
        }
    }

    fn person_batch(sc: SystemColumns, rows: &[(&str, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(sc.id, DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]));
        let names: Vec<&str> = rows.iter().map(|(name, _)| *name).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names.clone())) as _,
                Arc::new(StringArray::from(names)) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, email)| *email).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    fn knows_batch(sc: SystemColumns, rows: &[(&str, &str, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(sc.id, DataType::Utf8, false),
            Field::new(sc.src, DataType::Utf8, false),
            Field::new(sc.dst, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, src, _)| *src).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, dst)| *dst).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    async fn judge(
        db: &Session,
        target: LoadTarget,
        mode: LoadStmtMode,
        access: ValidateAccess,
        batches: &[RecordBatch],
    ) -> std::result::Result<(), String> {
        let catalog = db.catalog();
        let snapshot = db.snapshot().await;
        let table = target.table_key();
        let checks = checks(&catalog, target, mode, access);
        judge_load(
            &table,
            &checks,
            &catalog,
            &snapshot,
            load_mode(mode),
            batches,
            &mut LoadAdmission {
                phase: None,
                bytes: None,
                reuse: None,
                removed: &mut |_| Ok(()),
                retained: &mut |_| Ok(()),
            },
        )
        .await
        .map_err(|error| error.to_string())
    }

    /// Check order beats row order: a uniqueness repeat in the last batch is
    /// reported before an orphan endpoint in the first, under both accesses;
    /// a `.gqt` case sees one refusal text and cannot vary the access.
    #[tokio::test]
    async fn edge_append_refuses_in_check_order_under_probe_and_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = committed_graph(&dir).await;
        let sc = db.catalog().system_columns;
        for access in [ValidateAccess::Probe, ValidateAccess::Scan] {
            let orphan_first = [
                knows_batch(sc, &[("e1", "Bob", "Nobody"), ("e2", "Bob", "Carol")]),
                knows_batch(sc, &[("e3", "Ghost", "Alice")]),
            ];
            let refused = judge(
                &db,
                LoadTarget::Edge("Knows".into()),
                LoadStmtMode::Append,
                access,
                &orphan_first,
            )
            .await
            .unwrap_err();
            assert_eq!(
                refused,
                format!("{} 'Nobody' not found in Person", sc.dst),
                "{access:?}"
            );

            let over_card = [
                knows_batch(sc, &[("e1", "Alice", "Carol")]),
                knows_batch(sc, &[("e2", "Alice", "Bob")]),
            ];
            let refused = judge(
                &db,
                LoadTarget::Edge("Knows".into()),
                LoadStmtMode::Append,
                access,
                &over_card,
            )
            .await
            .unwrap_err();
            assert_eq!(
                refused, "@card violation on edge Knows: source 'Alice' has 4 edges (max 2)",
                "{access:?}: two committed and two new edges under one src"
            );

            let admitted = [knows_batch(
                sc,
                &[("e1", "Bob", "Carol"), ("e2", "Carol", "Bob")],
            )];
            judge(
                &db,
                LoadTarget::Edge("Knows".into()),
                LoadStmtMode::Append,
                access,
                &admitted,
            )
            .await
            .unwrap();
        }
    }

    /// A node append with a committed `@unique` holder in one batch and a
    /// `@key` repeat across batches: the key check precedes the unique one.
    #[tokio::test]
    async fn node_append_reports_the_key_repeat_before_the_committed_unique_holder() {
        let dir = tempfile::tempdir().unwrap();
        let db = committed_graph(&dir).await;
        let sc = db.catalog().system_columns;
        let batches = [
            person_batch(sc, &[("Zed", "alice@x")]),
            person_batch(sc, &[("Yan", "yan@x"), ("Yan", "yan2@x")]),
        ];
        let refused = judge(
            &db,
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Append,
            ValidateAccess::Probe,
            &batches,
        )
        .await
        .unwrap_err();
        assert_eq!(
            refused,
            "@unique violation on Person.name: value 'Yan' held by 'Yan' and 'Yan'"
        );
        let only_committed = [person_batch(sc, &[("Zed", "alice@x")])];
        let refused = judge(
            &db,
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Append,
            ValidateAccess::Scan,
            &only_committed,
        )
        .await
        .unwrap_err();
        assert_eq!(
            refused,
            "@unique violation on Person.email: value 'alice@x' held by 'Alice' and 'Zed'"
        );
    }

    /// A node overwrite dropping a committed node reports the committed edge
    /// that would survive without it, admits the image keeping every
    /// referenced node, and charges each removed id through the callback once.
    #[tokio::test]
    async fn node_overwrite_reports_a_retained_edge_to_a_removed_node() {
        let dir = tempfile::tempdir().unwrap();
        let db = committed_graph(&dir).await;
        let catalog = db.catalog();
        let sc = catalog.system_columns;
        let snapshot = db.snapshot().await;
        let checks = checks(
            &catalog,
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Overwrite,
            ValidateAccess::Scan,
        );
        assert_eq!(
            checks
                .iter()
                .map(|spec| spec.check.name())
                .collect::<Vec<_>>(),
            [
                "value",
                "unique",
                "unique",
                "retained_edges",
                "retained_edges"
            ]
        );
        let mut admitted = Vec::new();
        let refused = judge_load(
            "node:Person",
            &checks,
            &catalog,
            &snapshot,
            LoadMode::Overwrite,
            &[person_batch(sc, &[("Alice", "alice@x"), ("Bob", "bob@x")])],
            &mut LoadAdmission {
                phase: None,
                bytes: None,
                reuse: None,
                removed: &mut |id| {
                    admitted.push(id.to_string());
                    Ok(())
                },
                retained: &mut |_| Ok(()),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            refused.to_string(),
            format!("{} 'Carol' not found in Person", sc.dst)
        );
        assert_eq!(admitted, ["Carol"]);

        judge_load(
            "node:Person",
            &checks,
            &catalog,
            &snapshot,
            LoadMode::Overwrite,
            &[person_batch(
                sc,
                &[("Alice", "alice@x"), ("Bob", "bob@x"), ("Carol", "carol@x")],
            )],
            &mut LoadAdmission {
                phase: None,
                bytes: None,
                reuse: None,
                removed: &mut |_| Ok(()),
                retained: &mut |_| Ok(()),
            },
        )
        .await
        .unwrap();
    }
}
