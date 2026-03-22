use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, UInt64Array};
use lance::dataset::WriteMode;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tracing::{debug, info};

use super::mutation::{DatasetMutationPlan, EdgeDelta, MutationDelta, NodeDelta};
use super::{
    Database, DatabaseWriteGuard, DeletePredicate, EmbedOptions, EmbedResult, LoadMode,
    MutationPlan, MutationSource,
};
use crate::catalog::schema_ir::SchemaIR;
use crate::error::{NanoError, Result};
use crate::json_output::array_value_to_json;
use crate::query::ast::{CompOp, Literal, MatchValue, Mutation, QueryDecl};
use crate::result::MutationResult;
use crate::store::graph::DatasetAccumulator;
use crate::store::indexing::{rebuild_node_scalar_indexes, rebuild_node_vector_indexes};
use crate::store::lance_io::{
    read_lance_batches, run_lance_delete_by_ids, run_lance_merge_insert_with_key,
    write_lance_batch, write_lance_batch_with_mode,
};
use crate::store::loader::{
    EmbedValueRequest, build_next_storage_for_load, build_next_storage_for_load_reader,
    build_next_storage_for_load_reader_internal, build_next_storage_for_load_reader_with_options,
    collect_embed_specs, json_values_to_array, resolve_embedding_requests,
};
use crate::store::manifest::{DatasetEntry, GraphManifest, hash_string};
use crate::store::metadata::DatabaseMetadata;
use crate::store::runtime::DatabaseRuntime;
use crate::store::txlog::{CdcLogEntry, commit_manifest_and_logs};
use crate::types::ScalarType;

use super::cdc::{
    build_delete_cdc_events_from_batch, build_insert_cdc_events_from_batch, build_update_cdc_event,
    record_batch_row_to_json_map,
};

#[derive(Debug, Clone)]
struct SelectedEmbedProp {
    target_prop: String,
    source_prop: String,
    dim: usize,
    indexed: bool,
}

impl Database {
    /// Load JSONL data using compatibility defaults:
    /// - any `@key` in schema => `LoadMode::Merge`
    /// - no `@key` in schema => `LoadMode::Overwrite`
    pub async fn load(&self, data_source: &str) -> Result<()> {
        let mode = if self
            .schema_ir
            .node_types()
            .any(|node| node.properties.iter().any(|prop| prop.key))
        {
            LoadMode::Merge
        } else {
            LoadMode::Overwrite
        };
        self.load_with_mode(data_source, mode).await
    }

    /// Load JSONL data using explicit semantics.
    pub async fn load_with_mode(&self, data_source: &str, mode: LoadMode) -> Result<()> {
        let mut writer = self.lock_writer().await;
        self.load_with_mode_locked(data_source, mode, &mut writer)
            .await
    }

    /// Load JSONL data from a file using compatibility defaults.
    pub async fn load_file(&self, data_path: &Path) -> Result<()> {
        let mode = if self
            .schema_ir
            .node_types()
            .any(|node| node.properties.iter().any(|prop| prop.key))
        {
            LoadMode::Merge
        } else {
            LoadMode::Overwrite
        };
        self.load_file_with_mode(data_path, mode).await
    }

    /// Load JSONL data from a file using explicit semantics.
    pub async fn load_file_with_mode(&self, data_path: &Path, mode: LoadMode) -> Result<()> {
        let mut writer = self.lock_writer().await;
        self.load_file_with_mode_locked(data_path, mode, &mut writer)
            .await
    }

    async fn load_with_mode_locked(
        &self,
        data_source: &str,
        mode: LoadMode,
        writer: &mut DatabaseWriteGuard<'_>,
    ) -> Result<()> {
        info!("starting database load");
        self.apply_mutation_plan_locked(MutationPlan::for_load(data_source, mode), writer)
            .await?;
        let metadata = DatabaseMetadata::open(self.path())?;
        let node_types = metadata
            .manifest()
            .datasets
            .iter()
            .filter(|entry| entry.kind == "node")
            .count();
        let edge_types = metadata
            .manifest()
            .datasets
            .iter()
            .filter(|entry| entry.kind == "edge")
            .count();
        info!(
            mode = ?mode,
            node_types,
            edge_types,
            "database load complete"
        );

        Ok(())
    }

    async fn load_file_with_mode_locked(
        &self,
        data_path: &Path,
        mode: LoadMode,
        writer: &mut DatabaseWriteGuard<'_>,
    ) -> Result<()> {
        info!(data_path = %data_path.display(), "starting database file load");
        self.apply_mutation_plan_locked(MutationPlan::for_load_file(data_path, mode), writer)
            .await?;
        let metadata = DatabaseMetadata::open(self.path())?;
        let node_types = metadata
            .manifest()
            .datasets
            .iter()
            .filter(|entry| entry.kind == "node")
            .count();
        let edge_types = metadata
            .manifest()
            .datasets
            .iter()
            .filter(|entry| entry.kind == "edge")
            .count();
        info!(
            mode = ?mode,
            node_types,
            edge_types,
            "database file load complete"
        );

        Ok(())
    }

    /// Apply one append-only mutation payload through the unified mutation path.
    pub async fn apply_append_mutation(&self, data_source: &str, op_summary: &str) -> Result<()> {
        let mut writer = self.lock_writer().await;
        self.apply_append_mutation_locked(data_source, op_summary, &mut writer)
            .await
    }

    /// Apply one keyed-merge mutation payload through the unified mutation path.
    pub async fn apply_merge_mutation(&self, data_source: &str, op_summary: &str) -> Result<()> {
        let mut writer = self.lock_writer().await;
        self.apply_merge_mutation_locked(data_source, op_summary, &mut writer)
            .await
    }

    pub async fn embed(&self, options: EmbedOptions) -> Result<EmbedResult> {
        if options.property.is_some() && options.type_name.is_none() {
            return Err(NanoError::Execution(
                "--property requires --type for `nanograph embed`".to_string(),
            ));
        }

        let embed_specs = collect_embed_specs(&self.schema_ir)?;
        let selected_types = select_embed_types(
            &self.schema_ir,
            &embed_specs,
            options.type_name.as_deref(),
            options.property.as_deref(),
        )?;

        let reindexable_types: HashSet<String> = selected_types
            .iter()
            .filter(|(_, props)| props.iter().any(|prop| prop.indexed))
            .map(|(node_def, _)| node_def.name.clone())
            .collect();

        let metadata = DatabaseMetadata::open(self.path())?;
        let mut mutation_plan = DatasetMutationPlan::new(
            "embed",
            metadata.manifest().next_node_id,
            metadata.manifest().next_edge_id,
        );
        let mut rows_selected = 0usize;
        let mut embeddings_generated = 0usize;
        let mut remaining_limit = options.limit.unwrap_or(usize::MAX);
        let mut touched_types = HashSet::new();

        for (node_def, props) in &selected_types {
            if remaining_limit == 0 {
                break;
            }

            let Some(batch) = read_sparse_node_batch(&metadata, &node_def.name).await? else {
                continue;
            };

            let mut rows = record_batch_to_json_rows(&batch);
            let mut requests = Vec::new();
            let mut assignments = Vec::new();

            for (row_idx, row) in rows.iter().enumerate() {
                if remaining_limit == 0 {
                    break;
                }

                let mut row_assignments = Vec::new();
                for prop in props {
                    let current_value = row.get(&prop.target_prop).unwrap_or(&JsonValue::Null);
                    let should_generate = if options.only_null {
                        current_value.is_null()
                    } else {
                        true
                    };
                    if !should_generate {
                        continue;
                    }

                    let source_text = row
                        .get(&prop.source_prop)
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| {
                            NanoError::Execution(format!(
                                "cannot embed {}.{}: source property {} must be a String",
                                node_def.name, prop.target_prop, prop.source_prop
                            ))
                        })?;

                    row_assignments.push((
                        prop.target_prop.clone(),
                        source_text.to_string(),
                        prop.dim,
                    ));
                }

                if row_assignments.is_empty() {
                    continue;
                }

                rows_selected += 1;
                remaining_limit = remaining_limit.saturating_sub(1);
                for (target_prop, source_text, dim) in row_assignments {
                    requests.push(EmbedValueRequest { source_text, dim });
                    assignments.push((row_idx, target_prop));
                    embeddings_generated += 1;
                }
            }

            if requests.is_empty() {
                continue;
            }

            touched_types.insert(node_def.name.clone());
            if options.dry_run {
                continue;
            }

            let mut touched_row_indices: Vec<usize> =
                assignments.iter().map(|(row_idx, _)| *row_idx).collect();
            touched_row_indices.sort_unstable();
            touched_row_indices.dedup();
            let before_rows: Vec<JsonMap<String, JsonValue>> = touched_row_indices
                .iter()
                .map(|row_idx| rows[*row_idx].clone())
                .collect();

            let vectors = resolve_embedding_requests(self.path(), &requests).await?;
            for ((row_idx, target_prop), vector) in assignments.into_iter().zip(vectors.into_iter())
            {
                rows[row_idx].insert(
                    target_prop,
                    serde_json::to_value(vector).map_err(|e| {
                        NanoError::Storage(format!("serialize embedding vector failed: {}", e))
                    })?,
                );
            }

            let after_rows: Vec<JsonMap<String, JsonValue>> = touched_row_indices
                .iter()
                .map(|row_idx| rows[*row_idx].clone())
                .collect();
            let rebuilt = json_rows_to_record_batch(batch.schema().as_ref(), &rows)?;
            mutation_plan
                .node_replacements
                .insert(node_def.name.clone(), Some(rebuilt));
            mutation_plan.delta.node_changes.insert(
                node_def.name.clone(),
                NodeDelta {
                    upserts: Some(json_rows_to_record_batch(
                        batch.schema().as_ref(),
                        &after_rows,
                    )?),
                    before_for_updates: Some(json_rows_to_record_batch(
                        batch.schema().as_ref(),
                        &before_rows,
                    )?),
                    ..Default::default()
                },
            );
        }

        let properties_selected = selected_types.iter().map(|(_, props)| props.len()).sum();
        let reindexed_types = if options.dry_run {
            if options.reindex {
                reindexable_types.len()
            } else {
                reindexable_types
                    .iter()
                    .filter(|type_name| touched_types.contains(*type_name))
                    .count()
            }
        } else if !mutation_plan.delta.node_changes.is_empty() {
            let mut writer = self.lock_writer().await;
            self.apply_mutation_plan_locked(
                MutationPlan::prepared_datasets(mutation_plan),
                &mut writer,
            )
            .await?;
            reindexable_types
                .iter()
                .filter(|type_name| touched_types.contains(*type_name))
                .count()
        } else if options.reindex {
            self.rebuild_vector_indexes_for_types(&reindexable_types)
                .await?
        } else {
            0
        };

        Ok(EmbedResult {
            node_types_considered: selected_types.len(),
            properties_selected,
            rows_selected,
            embeddings_generated,
            reindexed_types,
            dry_run: options.dry_run,
        })
    }

    pub(crate) async fn apply_append_mutation_locked(
        &self,
        data_source: &str,
        op_summary: &str,
        writer: &mut DatabaseWriteGuard<'_>,
    ) -> Result<()> {
        self.apply_mutation_plan_locked(
            MutationPlan::append_mutation(data_source, op_summary),
            writer,
        )
        .await
    }

    pub(crate) async fn apply_merge_mutation_locked(
        &self,
        data_source: &str,
        op_summary: &str,
        writer: &mut DatabaseWriteGuard<'_>,
    ) -> Result<()> {
        self.apply_mutation_plan_locked(
            MutationPlan::merge_mutation(data_source, op_summary),
            writer,
        )
        .await
    }

    pub(super) async fn apply_mutation_plan_locked(
        &self,
        plan: MutationPlan,
        _writer: &mut DatabaseWriteGuard<'_>,
    ) -> Result<()> {
        let MutationPlan { source, op_summary } = plan;
        match source {
            MutationSource::LoadString { mode, data_source } => {
                if mode != LoadMode::Overwrite {
                    persist_sparse_load_string_at_path(&self.path, &data_source, mode, &op_summary)
                        .await?;
                    self.refresh_runtime_storage_from_manifest()?;
                    return Ok(());
                }
                let metadata = DatabaseMetadata::open(self.path())?;
                let previous_storage = Arc::new(restore_full_existing_storage(&metadata).await?);
                let load_result = build_next_storage_for_load(
                    &self.path,
                    previous_storage.as_ref(),
                    &self.schema_ir,
                    &data_source,
                    mode,
                )
                .await?;
                persist_dataset_mutation_plan_at_path(
                    &self.path,
                    &self.schema_ir,
                    &load_result.plan,
                )
                .await?;
                self.refresh_runtime_storage_from_manifest()?;
                Ok(())
            }
            MutationSource::LoadFile { mode, data_path } => {
                if mode != LoadMode::Overwrite {
                    persist_sparse_load_file_at_path(&self.path, &data_path, mode, &op_summary)
                        .await?;
                    self.refresh_runtime_storage_from_manifest()?;
                    return Ok(());
                }
                let metadata = DatabaseMetadata::open(self.path())?;
                let previous_storage = Arc::new(restore_full_existing_storage(&metadata).await?);
                let file = std::fs::File::open(&data_path)?;
                let reader = BufReader::new(file);
                let load_result = build_next_storage_for_load_reader(
                    &self.path,
                    previous_storage.as_ref(),
                    &self.schema_ir,
                    reader,
                    mode,
                )
                .await?;
                persist_dataset_mutation_plan_at_path(
                    &self.path,
                    &self.schema_ir,
                    &load_result.plan,
                )
                .await?;
                self.refresh_runtime_storage_from_manifest()?;
                Ok(())
            }
            MutationSource::PreparedDatasets(dataset_plan) => {
                persist_dataset_mutation_plan_at_path(&self.path, &self.schema_ir, &dataset_plan)
                    .await?;
                self.refresh_runtime_storage_from_manifest()?;
                Ok(())
            }
        }
    }

    fn refresh_runtime_storage_from_manifest(&self) -> Result<()> {
        let refreshed = DatabaseMetadata::open(self.path())?;
        self.replace_runtime(Arc::new(DatabaseRuntime::from_metadata(&refreshed)));
        Ok(())
    }

    async fn rebuild_vector_indexes_for_types(
        &self,
        type_names: &HashSet<String>,
    ) -> Result<usize> {
        if type_names.is_empty() {
            return Ok(0);
        }

        let runtime = self.current_runtime();
        let mut rebuilt = 0usize;
        for node_def in self.schema_ir.node_types() {
            if !type_names.contains(&node_def.name) {
                continue;
            }
            let Some(dataset_path) = runtime.node_dataset_path(&node_def.name) else {
                continue;
            };
            rebuild_node_vector_indexes(dataset_path, node_def).await?;
            rebuilt += 1;
        }
        Ok(rebuilt)
    }
}

pub async fn load_database_file_sparse(
    db_path: &Path,
    data_path: &Path,
    mode: LoadMode,
) -> Result<()> {
    if matches!(mode, LoadMode::Overwrite) {
        return Err(NanoError::Storage(
            "sparse file load only supports append and merge".to_string(),
        ));
    }

    let metadata = DatabaseMetadata::open(db_path)?;
    let existing_storage =
        build_sparse_existing_storage_for_load(&metadata, data_path, mode).await?;
    let file = std::fs::File::open(data_path)?;
    let reader = BufReader::new(file);
    let load_result = build_next_storage_for_load_reader_with_options(
        db_path,
        &existing_storage,
        metadata.schema_ir(),
        reader,
        mode,
        false,
    )
    .await?;
    persist_dataset_mutation_plan_at_path(db_path, metadata.schema_ir(), &load_result.plan).await
}

async fn persist_sparse_load_string_at_path(
    db_path: &Path,
    data_source: &str,
    mode: LoadMode,
    op_summary: &str,
) -> Result<()> {
    if matches!(mode, LoadMode::Overwrite) {
        return Err(NanoError::Storage(
            "sparse string load only supports append and merge".to_string(),
        ));
    }

    let metadata = DatabaseMetadata::open(db_path)?;
    let existing_storage = build_sparse_existing_storage_for_load_reader(
        &metadata,
        Cursor::new(data_source.as_bytes()),
        mode,
    )
    .await?;
    let load_result = build_next_storage_for_load_reader_with_options(
        db_path,
        &existing_storage,
        metadata.schema_ir(),
        Cursor::new(data_source.as_bytes()),
        mode,
        false,
    )
    .await?;
    let mut plan = load_result.plan;
    plan.op_summary = op_summary.to_string();
    persist_dataset_mutation_plan_at_path(db_path, metadata.schema_ir(), &plan).await
}

async fn persist_sparse_load_file_at_path(
    db_path: &Path,
    data_path: &Path,
    mode: LoadMode,
    op_summary: &str,
) -> Result<()> {
    if matches!(mode, LoadMode::Overwrite) {
        return Err(NanoError::Storage(
            "sparse file load only supports append and merge".to_string(),
        ));
    }

    let metadata = DatabaseMetadata::open(db_path)?;
    let existing_storage =
        build_sparse_existing_storage_for_load(&metadata, data_path, mode).await?;
    let file = std::fs::File::open(data_path)?;
    let reader = BufReader::new(file);
    let load_result = build_next_storage_for_load_reader_with_options(
        db_path,
        &existing_storage,
        metadata.schema_ir(),
        reader,
        mode,
        false,
    )
    .await?;
    let mut plan = load_result.plan;
    plan.op_summary = op_summary.to_string();
    persist_dataset_mutation_plan_at_path(db_path, metadata.schema_ir(), &plan).await
}

pub async fn run_mutation_query_sparse(
    db_path: &Path,
    query: &QueryDecl,
    params: &crate::ParamMap,
) -> Result<MutationResult> {
    let runtime_params = super::params_with_runtime_now(params)?;
    let mutation = query
        .mutation
        .as_ref()
        .ok_or_else(|| NanoError::Execution("expected mutation query".to_string()))?;
    match mutation {
        Mutation::Insert(insert) => {
            let metadata = DatabaseMetadata::open(db_path)?;
            if metadata
                .catalog()
                .node_types
                .contains_key(&insert.type_name)
            {
                let mut data = serde_json::Map::new();
                for assignment in &insert.assignments {
                    let lit = resolve_mutation_match_value(&assignment.value, &runtime_params)?;
                    data.insert(assignment.property.clone(), literal_to_json_value(&lit)?);
                }
                let payload = serde_json::json!({
                    "type": insert.type_name,
                    "data": data
                })
                .to_string();
                let (existing_storage, next_storage) = build_sparse_storage_transition_from_string(
                    &metadata,
                    &payload,
                    LoadMode::Append,
                )
                .await?;
                let Some(inserted_batch) = extract_appended_node_batch(
                    &existing_storage,
                    &next_storage,
                    &insert.type_name,
                )?
                else {
                    return Ok(MutationResult::default());
                };
                let affected_nodes = inserted_batch.num_rows();
                let mut mutation_plan = DatasetMutationPlan::new(
                    "mutation:insert_node",
                    next_storage.next_node_id(),
                    next_storage.next_edge_id(),
                );
                mutation_plan.node_replacements.insert(
                    insert.type_name.clone(),
                    next_storage.get_all_nodes(&insert.type_name)?,
                );
                mutation_plan.delta.node_changes.insert(
                    insert.type_name.clone(),
                    NodeDelta {
                        inserts: Some(inserted_batch),
                        ..Default::default()
                    },
                );
                persist_dataset_mutation_plan_at_path(
                    db_path,
                    metadata.schema_ir(),
                    &mutation_plan,
                )
                .await?;
                return Ok(MutationResult {
                    affected_nodes,
                    affected_edges: 0,
                });
            }
            if metadata
                .catalog()
                .edge_types
                .contains_key(&insert.type_name)
            {
                let mut from_name = None;
                let mut to_name = None;
                let mut data = serde_json::Map::new();
                for assignment in &insert.assignments {
                    let lit = resolve_mutation_match_value(&assignment.value, &runtime_params)?;
                    match assignment.property.as_str() {
                        "from" => {
                            from_name = Some(literal_to_endpoint_name(&lit, "from")?);
                        }
                        "to" => {
                            to_name = Some(literal_to_endpoint_name(&lit, "to")?);
                        }
                        _ => {
                            data.insert(assignment.property.clone(), literal_to_json_value(&lit)?);
                        }
                    }
                }

                let from = from_name.ok_or_else(|| {
                    NanoError::Execution(format!(
                        "edge insert for `{}` requires endpoint property `from`",
                        insert.type_name
                    ))
                })?;
                let to = to_name.ok_or_else(|| {
                    NanoError::Execution(format!(
                        "edge insert for `{}` requires endpoint property `to`",
                        insert.type_name
                    ))
                })?;
                let payload = serde_json::json!({
                    "edge": insert.type_name,
                    "from": from,
                    "to": to,
                    "data": data
                })
                .to_string();
                let (existing_storage, next_storage) = build_sparse_storage_transition_from_string(
                    &metadata,
                    &payload,
                    LoadMode::Append,
                )
                .await?;
                let Some(inserted_batch) = extract_appended_edge_batch(
                    &existing_storage,
                    &next_storage,
                    &insert.type_name,
                )?
                else {
                    return Ok(MutationResult::default());
                };
                let affected_edges = inserted_batch.num_rows();
                let mut mutation_plan = DatasetMutationPlan::new(
                    "mutation:insert_edge",
                    next_storage.next_node_id(),
                    next_storage.next_edge_id(),
                );
                mutation_plan.edge_replacements.insert(
                    insert.type_name.clone(),
                    next_storage.edge_batch_for_save(&insert.type_name)?,
                );
                mutation_plan.delta.edge_changes.insert(
                    insert.type_name.clone(),
                    EdgeDelta {
                        inserts: Some(inserted_batch),
                        ..Default::default()
                    },
                );
                persist_dataset_mutation_plan_at_path(
                    db_path,
                    metadata.schema_ir(),
                    &mutation_plan,
                )
                .await?;
                return Ok(MutationResult {
                    affected_nodes: 0,
                    affected_edges,
                });
            }
            Err(NanoError::Execution(format!(
                "unknown mutation target type `{}`",
                insert.type_name
            )))
        }
        Mutation::Update(update) => {
            let metadata = DatabaseMetadata::open(db_path)?;
            if metadata
                .catalog()
                .edge_types
                .contains_key(&update.type_name)
            {
                return Err(NanoError::Execution(
                    "sparse mutation path does not support edge updates".to_string(),
                ));
            }
            if !metadata
                .catalog()
                .node_types
                .contains_key(&update.type_name)
            {
                return Err(NanoError::Execution(format!(
                    "unknown mutation target type `{}`",
                    update.type_name
                )));
            }

            let key_prop = metadata
                .schema_ir()
                .node_types()
                .find(|node| node.name == update.type_name)
                .and_then(|node| node.properties.iter().find(|prop| prop.key))
                .map(|prop| prop.name.clone())
                .ok_or_else(|| {
                    NanoError::Storage(format!(
                        "update mutation requires @key on node type `{}` for identity-safe updates",
                        update.type_name
                    ))
                })?;
            if update
                .assignments
                .iter()
                .any(|assignment| assignment.property == key_prop)
            {
                return Err(NanoError::Storage(format!(
                    "update mutation cannot assign @key property `{}`",
                    key_prop
                )));
            }

            let Some(target_batch) = read_sparse_node_batch(&metadata, &update.type_name).await?
            else {
                return Ok(MutationResult::default());
            };
            let delete_pred = DeletePredicate {
                property: update.predicate.property.clone(),
                op: comp_op_to_delete_op(update.predicate.op)?,
                value: literal_to_predicate_string(&resolve_mutation_match_value(
                    &update.predicate.value,
                    &runtime_params,
                )?)?,
            };
            let match_mask = crate::store::database::build_delete_mask_for_mutation(
                &target_batch,
                &delete_pred,
            )?;
            let matched_rows: Vec<usize> = (0..target_batch.num_rows())
                .filter(|&row| !match_mask.is_null(row) && match_mask.value(row))
                .collect();
            if matched_rows.is_empty() {
                return Ok(MutationResult::default());
            }

            let mut assignment_values = HashMap::new();
            for assignment in &update.assignments {
                let lit = resolve_mutation_match_value(&assignment.value, &runtime_params)?;
                assignment_values.insert(assignment.property.clone(), literal_to_json_value(&lit)?);
            }

            let schema = target_batch.schema();
            let prop_columns: Vec<(usize, String)> = schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(idx, field)| (idx != 0).then_some((idx, field.name().clone())))
                .collect();

            let before_rows_all = record_batch_to_json_rows(&target_batch);
            let mut before_rows = Vec::new();
            let mut after_rows = Vec::new();
            let mut payload_lines = Vec::new();
            for row in matched_rows.iter().copied() {
                let before = before_rows_all[row].clone();
                let mut after = before.clone();
                for (prop, value) in &assignment_values {
                    after.insert(prop.clone(), value.clone());
                }
                if before == after {
                    continue;
                }
                let mut data_obj = serde_json::Map::new();
                for (_, prop_name) in &prop_columns {
                    data_obj.insert(
                        prop_name.clone(),
                        after.get(prop_name).cloned().unwrap_or(JsonValue::Null),
                    );
                }
                payload_lines.push(
                    serde_json::json!({
                        "type": update.type_name,
                        "data": data_obj
                    })
                    .to_string(),
                );
                before_rows.push(before);
                after_rows.push(after);
            }
            if after_rows.is_empty() {
                return Ok(MutationResult::default());
            }

            let before_batch = json_rows_to_record_batch(schema.as_ref(), &before_rows)?;
            let after_batch = json_rows_to_record_batch(schema.as_ref(), &after_rows)?;
            let (_existing_storage, next_storage) = build_sparse_storage_transition_from_string(
                &metadata,
                &payload_lines.join("\n"),
                LoadMode::Merge,
            )
            .await?;
            let affected_nodes = after_batch.num_rows();
            let mut mutation_plan = DatasetMutationPlan::new(
                "mutation:update_node",
                next_storage.next_node_id(),
                next_storage.next_edge_id(),
            );
            mutation_plan.node_replacements.insert(
                update.type_name.clone(),
                next_storage.get_all_nodes(&update.type_name)?,
            );
            mutation_plan.delta.node_changes.insert(
                update.type_name.clone(),
                NodeDelta {
                    upserts: Some(after_batch),
                    before_for_updates: Some(before_batch),
                    ..Default::default()
                },
            );
            persist_dataset_mutation_plan_at_path(db_path, metadata.schema_ir(), &mutation_plan)
                .await?;
            Ok(MutationResult {
                affected_nodes,
                affected_edges: 0,
            })
        }
        Mutation::Delete(delete) => {
            let metadata = DatabaseMetadata::open(db_path)?;
            if metadata
                .catalog()
                .node_types
                .contains_key(&delete.type_name)
            {
                if node_type_has_incident_edges(&metadata, &delete.type_name) {
                    return Err(NanoError::Execution(format!(
                        "sparse mutation path only supports node deletes for `{}` when the type has no incident edge types",
                        delete.type_name
                    )));
                }
                let Some(target_batch) =
                    read_sparse_node_batch(&metadata, &delete.type_name).await?
                else {
                    return Ok(MutationResult::default());
                };
                let delete_pred = DeletePredicate {
                    property: delete.predicate.property.clone(),
                    op: comp_op_to_delete_op(delete.predicate.op)?,
                    value: literal_to_predicate_string(&resolve_mutation_match_value(
                        &delete.predicate.value,
                        &runtime_params,
                    )?)?,
                };
                let delete_mask = crate::store::database::build_delete_mask_for_mutation(
                    &target_batch,
                    &delete_pred,
                )?;
                let deleted_rows: Vec<usize> = (0..target_batch.num_rows())
                    .filter(|&row| !delete_mask.is_null(row) && delete_mask.value(row))
                    .collect();
                if deleted_rows.is_empty() {
                    return Ok(MutationResult::default());
                }

                let filtered_target =
                    filter_record_batch_by_delete_mask(&target_batch, &delete_mask, "node")?;
                let deleted_batch = select_record_batch_rows(&target_batch, &deleted_rows)?;
                let delete_ids = collect_ids_from_batch(&deleted_batch)?;
                let mut mutation_plan = DatasetMutationPlan::new(
                    "mutation:delete_nodes",
                    metadata.manifest().next_node_id,
                    metadata.manifest().next_edge_id,
                );
                mutation_plan.node_replacements.insert(
                    delete.type_name.clone(),
                    (filtered_target.num_rows() > 0).then_some(filtered_target),
                );
                mutation_plan.delta.node_changes.insert(
                    delete.type_name.clone(),
                    NodeDelta {
                        delete_ids: delete_ids.clone(),
                        before_for_deletes: Some(deleted_batch),
                        ..Default::default()
                    },
                );
                persist_dataset_mutation_plan_at_path(
                    db_path,
                    metadata.schema_ir(),
                    &mutation_plan,
                )
                .await?;
                return Ok(MutationResult {
                    affected_nodes: delete_ids.len(),
                    affected_edges: 0,
                });
            }
            if metadata
                .catalog()
                .edge_types
                .contains_key(&delete.type_name)
            {
                let Some(target_batch) =
                    read_sparse_edge_batch(&metadata, &delete.type_name).await?
                else {
                    return Ok(MutationResult::default());
                };
                let delete_pred = map_sparse_edge_delete_predicate(
                    &metadata,
                    &delete.type_name,
                    &delete.predicate.property,
                    delete.predicate.op,
                    &delete.predicate.value,
                    &runtime_params,
                )
                .await?;
                let delete_mask = crate::store::database::build_delete_mask_for_mutation(
                    &target_batch,
                    &delete_pred,
                )?;
                let deleted_rows: Vec<usize> = (0..target_batch.num_rows())
                    .filter(|&row| !delete_mask.is_null(row) && delete_mask.value(row))
                    .collect();
                if deleted_rows.is_empty() {
                    return Ok(MutationResult::default());
                }

                let filtered_target =
                    filter_record_batch_by_delete_mask(&target_batch, &delete_mask, "edge")?;
                let deleted_batch = select_record_batch_rows(&target_batch, &deleted_rows)?;
                let delete_ids = collect_ids_from_batch(&deleted_batch)?;
                let mut mutation_plan = DatasetMutationPlan::new(
                    "mutation:delete_edges",
                    metadata.manifest().next_node_id,
                    metadata.manifest().next_edge_id,
                );
                mutation_plan.edge_replacements.insert(
                    delete.type_name.clone(),
                    (filtered_target.num_rows() > 0).then_some(filtered_target),
                );
                mutation_plan.delta.edge_changes.insert(
                    delete.type_name.clone(),
                    EdgeDelta {
                        delete_ids: delete_ids.clone(),
                        before_for_deletes: Some(deleted_batch),
                        ..Default::default()
                    },
                );
                persist_dataset_mutation_plan_at_path(
                    db_path,
                    metadata.schema_ir(),
                    &mutation_plan,
                )
                .await?;
                return Ok(MutationResult {
                    affected_nodes: 0,
                    affected_edges: delete_ids.len(),
                });
            }
            Err(NanoError::Execution(format!(
                "unknown mutation target type `{}`",
                delete.type_name
            )))
        }
    }
}

async fn build_sparse_storage_transition_from_string(
    metadata: &DatabaseMetadata,
    data_source: &str,
    mode: LoadMode,
) -> Result<(DatasetAccumulator, DatasetAccumulator)> {
    let existing_storage = build_sparse_existing_storage_for_load_reader(
        metadata,
        Cursor::new(data_source.as_bytes()),
        mode,
    )
    .await?;
    let load_result = build_next_storage_for_load_reader_internal(
        metadata.path(),
        &existing_storage,
        metadata.schema_ir(),
        Cursor::new(data_source.as_bytes()),
        mode,
        false,
    )
    .await?;
    Ok((existing_storage, load_result.next_storage))
}

fn extract_appended_node_batch(
    existing_storage: &DatasetAccumulator,
    next_storage: &DatasetAccumulator,
    type_name: &str,
) -> Result<Option<RecordBatch>> {
    extract_appended_batch(
        existing_storage.get_all_nodes(type_name)?,
        next_storage.get_all_nodes(type_name)?,
        "node",
        type_name,
    )
}

fn extract_appended_edge_batch(
    existing_storage: &DatasetAccumulator,
    next_storage: &DatasetAccumulator,
    type_name: &str,
) -> Result<Option<RecordBatch>> {
    extract_appended_batch(
        existing_storage.edge_batch_for_save(type_name)?,
        next_storage.edge_batch_for_save(type_name)?,
        "edge",
        type_name,
    )
}

fn extract_appended_batch(
    previous: Option<RecordBatch>,
    next: Option<RecordBatch>,
    entity_kind: &str,
    type_name: &str,
) -> Result<Option<RecordBatch>> {
    let previous_rows = previous.as_ref().map(|batch| batch.num_rows()).unwrap_or(0);
    let Some(next_batch) = next else {
        return Ok(None);
    };
    if next_batch.num_rows() < previous_rows {
        return Err(NanoError::Storage(format!(
            "{} {} shrank during append transition",
            entity_kind, type_name
        )));
    }
    let appended_rows = next_batch.num_rows().saturating_sub(previous_rows);
    if appended_rows == 0 {
        return Ok(None);
    }
    Ok(Some(next_batch.slice(previous_rows, appended_rows)))
}

pub(crate) fn select_record_batch_rows(batch: &RecordBatch, rows: &[usize]) -> Result<RecordBatch> {
    let all_rows = record_batch_to_json_rows(batch);
    let selected_rows: Vec<JsonMap<String, JsonValue>> =
        rows.iter().map(|row| all_rows[*row].clone()).collect();
    json_rows_to_record_batch(batch.schema().as_ref(), &selected_rows)
}

pub(crate) fn collect_ids_from_batch(batch: &RecordBatch) -> Result<Vec<u64>> {
    let id_arr = batch
        .column_by_name("id")
        .ok_or_else(|| NanoError::Storage("batch missing id column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("batch id column is not UInt64".to_string()))?;
    Ok((0..batch.num_rows()).map(|row| id_arr.value(row)).collect())
}

pub(crate) async fn persist_dataset_mutation_plan_at_path(
    db_path: &Path,
    schema_ir: &SchemaIR,
    plan: &DatasetMutationPlan,
) -> Result<()> {
    let previous_manifest = GraphManifest::read(db_path)?;
    let mut dataset_entries = Vec::new();
    let mut previous_entries_by_key: HashMap<String, DatasetEntry> = HashMap::new();
    for entry in &previous_manifest.datasets {
        previous_entries_by_key.insert(
            dataset_entity_key(&entry.kind, &entry.type_name),
            entry.clone(),
        );
    }

    for node_def in schema_ir.node_types() {
        let entity_key = dataset_entity_key("node", &node_def.name);
        let previous_entry = previous_entries_by_key.get(&entity_key).cloned();
        let replacement = plan.node_replacements.get(&node_def.name);
        let node_delta = plan.delta.node_changes.get(&node_def.name);

        if replacement.is_none() {
            if let Some(prev) = previous_entry {
                dataset_entries.push(prev);
            }
            continue;
        }

        let Some(batch) = replacement.cloned().flatten() else {
            continue;
        };
        let node_delta = node_delta.cloned().unwrap_or_default();

        let row_count = batch.num_rows() as u64;
        let dataset_rel_path = previous_entry
            .as_ref()
            .map(|entry| entry.dataset_path.clone())
            .unwrap_or_else(|| format!("nodes/{}", SchemaIR::dir_name(node_def.type_id)));
        let dataset_path = db_path.join(&dataset_rel_path);
        let duplicate_field_names = schema_has_duplicate_field_names(batch.schema().as_ref());
        let key_prop = node_def
            .properties
            .iter()
            .find(|prop| prop.key)
            .map(|prop| prop.name.as_str());
        let can_merge_upsert = previous_entry.is_some()
            && key_prop.is_some()
            && !duplicate_field_names
            && node_delta.delete_ids.is_empty()
            && node_delta.inserts.is_none()
            && node_delta
                .upserts
                .as_ref()
                .map(|rows| rows.num_rows() > 0)
                .unwrap_or(false);
        let can_append = previous_entry.is_some()
            && !duplicate_field_names
            && node_delta.delete_ids.is_empty()
            && node_delta.upserts.is_none()
            && node_delta
                .inserts
                .as_ref()
                .map(|rows| rows.num_rows() > 0)
                .unwrap_or(false);
        let can_native_delete = previous_entry.is_some()
            && node_delta.inserts.is_none()
            && node_delta.upserts.is_none()
            && !node_delta.delete_ids.is_empty();
        let dataset_version = if can_merge_upsert {
            let source_batch = node_delta.upserts.clone().ok_or_else(|| {
                NanoError::Storage(format!("missing node upsert batch for {}", node_def.name))
            })?;
            let key_prop = key_prop.unwrap_or_default();
            let pinned_version = previous_entry
                .as_ref()
                .map(|entry| entry.dataset_version)
                .ok_or_else(|| {
                    NanoError::Storage(format!(
                        "missing previous dataset version for {}",
                        node_def.name
                    ))
                })?;
            debug!(
                node_type = %node_def.name,
                rows = source_batch.num_rows(),
                key_prop = key_prop,
                "merging node delta into existing Lance dataset"
            );
            run_lance_merge_insert_with_key(&dataset_path, pinned_version, source_batch, key_prop)
                .await?
        } else if can_append {
            let delta_batch = node_delta.inserts.clone().ok_or_else(|| {
                NanoError::Storage(format!("missing node insert batch for {}", node_def.name))
            })?;
            debug!(
                node_type = %node_def.name,
                rows = delta_batch.num_rows(),
                "appending node delta to existing Lance dataset"
            );
            write_lance_batch_with_mode(&dataset_path, delta_batch, WriteMode::Append).await?
        } else if can_native_delete {
            let pinned_version = previous_entry
                .as_ref()
                .map(|entry| entry.dataset_version)
                .ok_or_else(|| {
                    NanoError::Storage(format!(
                        "missing previous dataset version for {}",
                        node_def.name
                    ))
                })?;
            debug!(
                node_type = %node_def.name,
                rows = node_delta.delete_ids.len(),
                "deleting node delta from existing Lance dataset"
            );
            run_lance_delete_by_ids(&dataset_path, pinned_version, &node_delta.delete_ids).await?
        } else {
            debug!(
                node_type = %node_def.name,
                rows = row_count,
                "writing full node dataset from dataset mutation plan"
            );
            write_lance_batch(&dataset_path, batch).await?
        };
        rebuild_node_scalar_indexes(&dataset_path, node_def).await?;
        rebuild_node_vector_indexes(&dataset_path, node_def).await?;
        dataset_entries.push(DatasetEntry {
            type_id: node_def.type_id,
            type_name: node_def.name.clone(),
            kind: "node".to_string(),
            dataset_path: dataset_rel_path,
            dataset_version,
            row_count,
        });
    }

    for edge_def in schema_ir.edge_types() {
        let entity_key = dataset_entity_key("edge", &edge_def.name);
        let previous_entry = previous_entries_by_key.get(&entity_key).cloned();
        let replacement = plan.edge_replacements.get(&edge_def.name);
        let edge_delta = plan.delta.edge_changes.get(&edge_def.name);

        if replacement.is_none() {
            if let Some(prev) = previous_entry {
                dataset_entries.push(prev);
            }
            continue;
        }

        let Some(batch) = replacement.cloned().flatten() else {
            continue;
        };
        let edge_delta = edge_delta.cloned().unwrap_or_default();

        let row_count = batch.num_rows() as u64;
        let dataset_rel_path = previous_entry
            .as_ref()
            .map(|entry| entry.dataset_path.clone())
            .unwrap_or_else(|| format!("edges/{}", SchemaIR::dir_name(edge_def.type_id)));
        let dataset_path = db_path.join(&dataset_rel_path);
        let duplicate_field_names = schema_has_duplicate_field_names(batch.schema().as_ref());
        let can_append = previous_entry.is_some()
            && !duplicate_field_names
            && edge_delta.delete_ids.is_empty()
            && edge_delta
                .inserts
                .as_ref()
                .map(|rows| rows.num_rows() > 0)
                .unwrap_or(false);
        let can_native_delete = previous_entry.is_some()
            && edge_delta.inserts.is_none()
            && !edge_delta.delete_ids.is_empty();
        let dataset_version = if can_append {
            let delta_batch = edge_delta.inserts.clone().ok_or_else(|| {
                NanoError::Storage(format!("missing edge insert batch for {}", edge_def.name))
            })?;
            debug!(
                edge_type = %edge_def.name,
                rows = delta_batch.num_rows(),
                "appending edge delta to existing Lance dataset"
            );
            write_lance_batch_with_mode(&dataset_path, delta_batch, WriteMode::Append).await?
        } else if can_native_delete {
            let pinned_version = previous_entry
                .as_ref()
                .map(|entry| entry.dataset_version)
                .ok_or_else(|| {
                    NanoError::Storage(format!(
                        "missing previous dataset version for {}",
                        edge_def.name
                    ))
                })?;
            debug!(
                edge_type = %edge_def.name,
                rows = edge_delta.delete_ids.len(),
                "deleting edge delta from existing Lance dataset"
            );
            run_lance_delete_by_ids(&dataset_path, pinned_version, &edge_delta.delete_ids).await?
        } else {
            debug!(
                edge_type = %edge_def.name,
                rows = row_count,
                "writing full edge dataset from dataset mutation plan"
            );
            write_lance_batch(&dataset_path, batch).await?
        };
        dataset_entries.push(DatasetEntry {
            type_id: edge_def.type_id,
            type_name: edge_def.name.clone(),
            kind: "edge".to_string(),
            dataset_path: dataset_rel_path,
            dataset_version,
            row_count,
        });
    }

    let ir_json = serde_json::to_string_pretty(schema_ir)
        .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
    let ir_hash = hash_string(&ir_json);

    let mut manifest = GraphManifest::new(ir_hash);
    manifest.db_version = previous_manifest.db_version.saturating_add(1);
    manifest.last_tx_id = format!("manifest-{}", manifest.db_version);
    manifest.committed_at = super::now_unix_seconds_string();
    manifest.next_node_id = plan.next_node_id;
    manifest.next_edge_id = plan.next_edge_id;
    let (next_type_id, next_prop_id) = super::next_schema_identity_counters(schema_ir);
    manifest.next_type_id = next_type_id;
    manifest.next_prop_id = next_prop_id;
    manifest.schema_identity_version = previous_manifest.schema_identity_version.max(1);
    manifest.datasets = dataset_entries;

    let committed_cdc_events = finalize_cdc_entries_for_manifest(
        &build_pending_cdc_entries_from_delta(&plan.delta)?,
        &manifest,
    );
    commit_manifest_and_logs(db_path, &manifest, &committed_cdc_events, &plan.op_summary)?;

    super::maintenance::cleanup_stale_dirs(db_path, &manifest)?;
    Ok(())
}

#[derive(Default)]
struct IncomingLoadTypes {
    node_types: HashSet<String>,
    edge_types: HashSet<String>,
}

async fn build_sparse_existing_storage_for_load(
    metadata: &DatabaseMetadata,
    data_path: &Path,
    mode: LoadMode,
) -> Result<DatasetAccumulator> {
    let file = std::fs::File::open(data_path)?;
    build_sparse_existing_storage_for_load_reader(metadata, BufReader::new(file), mode).await
}

async fn build_sparse_existing_storage_for_load_reader<R: BufRead>(
    metadata: &DatabaseMetadata,
    reader: R,
    mode: LoadMode,
) -> Result<DatasetAccumulator> {
    let incoming_types = collect_incoming_load_types(metadata, reader)?;
    let (required_node_types, required_edge_types) =
        sparse_load_restore_scope(metadata.schema_ir(), &incoming_types, mode)?;
    restore_sparse_existing_storage(metadata, &required_node_types, &required_edge_types).await
}

async fn restore_full_existing_storage(metadata: &DatabaseMetadata) -> Result<DatasetAccumulator> {
    let node_types = metadata
        .catalog()
        .node_types
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let edge_types = metadata
        .catalog()
        .edge_types
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    restore_sparse_existing_storage(metadata, &node_types, &edge_types).await
}

fn collect_incoming_load_types<R: BufRead>(
    metadata: &DatabaseMetadata,
    reader: R,
) -> Result<IncomingLoadTypes> {
    let mut incoming = IncomingLoadTypes::default();
    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }

        let obj: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            NanoError::Storage(format!("JSON parse error on line {}: {}", line_no + 1, e))
        })?;
        if let Some(type_name) = obj.get("type").and_then(|value| value.as_str()) {
            incoming.node_types.insert(type_name.to_string());
        } else if let Some(edge_type) = obj.get("edge").and_then(|value| value.as_str()) {
            let edge_name = metadata
                .catalog()
                .edge_types
                .get(edge_type)
                .map(|_| edge_type.to_string())
                .or_else(|| metadata.catalog().edge_name_index.get(edge_type).cloned())
                .ok_or_else(|| NanoError::Storage(format!("unknown edge type: {}", edge_type)))?;
            incoming.edge_types.insert(edge_name);
        }
    }
    Ok(incoming)
}

fn sparse_load_restore_scope(
    schema_ir: &SchemaIR,
    incoming_types: &IncomingLoadTypes,
    mode: LoadMode,
) -> Result<(HashSet<String>, HashSet<String>)> {
    let mut node_types = incoming_types.node_types.clone();
    let mut edge_types = incoming_types.edge_types.clone();

    for edge_name in &incoming_types.edge_types {
        let edge_def = schema_ir
            .edge_types()
            .find(|edge| edge.name == *edge_name)
            .ok_or_else(|| NanoError::Storage(format!("unknown edge type: {}", edge_name)))?;
        node_types.insert(edge_def.src_type_name.clone());
        node_types.insert(edge_def.dst_type_name.clone());
    }

    if mode == LoadMode::Merge {
        let unkeyed_incoming_types: HashSet<String> = incoming_types
            .node_types
            .iter()
            .filter_map(|type_name| {
                schema_ir
                    .node_types()
                    .find(|node| node.name == *type_name)
                    .filter(|node| !node.properties.iter().any(|prop| prop.key))
                    .map(|node| node.name.clone())
            })
            .collect();
        if !unkeyed_incoming_types.is_empty() {
            for edge_def in schema_ir.edge_types() {
                if unkeyed_incoming_types.contains(&edge_def.src_type_name)
                    || unkeyed_incoming_types.contains(&edge_def.dst_type_name)
                {
                    edge_types.insert(edge_def.name.clone());
                    node_types.insert(edge_def.src_type_name.clone());
                    node_types.insert(edge_def.dst_type_name.clone());
                }
            }
        }
    }

    Ok((node_types, edge_types))
}

async fn restore_sparse_existing_storage(
    metadata: &DatabaseMetadata,
    node_types: &HashSet<String>,
    edge_types: &HashSet<String>,
) -> Result<DatasetAccumulator> {
    let mut storage = DatasetAccumulator::new(metadata.catalog().clone());
    storage.set_next_node_id(metadata.manifest().next_node_id);
    storage.set_next_edge_id(metadata.manifest().next_edge_id);

    let mut node_names: Vec<String> = node_types.iter().cloned().collect();
    node_names.sort();
    for type_name in node_names {
        let Some(locator) = metadata.node_dataset_locator(&type_name) else {
            continue;
        };
        let dataset_path = locator.dataset_path.clone();
        let dataset_version = locator.dataset_version;
        let batches = read_lance_batches(&dataset_path, dataset_version).await?;
        for batch in batches {
            storage.load_node_batch(&type_name, batch)?;
        }
        storage.set_node_dataset_path(&type_name, dataset_path);
        storage.set_node_dataset_version(&type_name, dataset_version);
    }

    let mut edge_names: Vec<String> = edge_types.iter().cloned().collect();
    edge_names.sort();
    for type_name in edge_names {
        let Some(locator) = metadata.edge_dataset_locator(&type_name) else {
            continue;
        };
        let batches = read_lance_batches(&locator.dataset_path, locator.dataset_version).await?;
        for batch in batches {
            storage.load_edge_batch(&type_name, batch)?;
        }
    }

    Ok(storage)
}

pub(crate) async fn read_sparse_node_batch(
    metadata: &DatabaseMetadata,
    type_name: &str,
) -> Result<Option<RecordBatch>> {
    let Some(locator) = metadata.node_dataset_locator(type_name) else {
        return Ok(None);
    };
    let batches = read_lance_batches(&locator.dataset_path, locator.dataset_version).await?;
    if batches.is_empty() {
        return Ok(None);
    }
    if batches.len() == 1 {
        return Ok(Some(batches[0].clone()));
    }
    let schema = batches[0].schema();
    let batch = arrow_select::concat::concat_batches(&schema, &batches)
        .map_err(|e| NanoError::Storage(format!("concat error: {}", e)))?;
    Ok(Some(batch))
}

pub(crate) async fn read_sparse_edge_batch(
    metadata: &DatabaseMetadata,
    type_name: &str,
) -> Result<Option<RecordBatch>> {
    let Some(locator) = metadata.edge_dataset_locator(type_name) else {
        return Ok(None);
    };
    let batches = read_lance_batches(&locator.dataset_path, locator.dataset_version).await?;
    if batches.is_empty() {
        return Ok(None);
    }
    if batches.len() == 1 {
        return Ok(Some(batches[0].clone()));
    }
    let schema = batches[0].schema();
    let batch = arrow_select::concat::concat_batches(&schema, &batches)
        .map_err(|e| NanoError::Storage(format!("concat error: {}", e)))?;
    Ok(Some(batch))
}

fn node_type_has_incident_edges(metadata: &DatabaseMetadata, type_name: &str) -> bool {
    metadata
        .catalog()
        .edge_types
        .values()
        .any(|edge| edge.from_type == type_name || edge.to_type == type_name)
}

pub(crate) fn filter_record_batch_by_delete_mask(
    batch: &RecordBatch,
    delete_mask: &BooleanArray,
    entity_kind: &str,
) -> Result<RecordBatch> {
    let mut keep_builder = BooleanBuilder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let delete = !delete_mask.is_null(row) && delete_mask.value(row);
        keep_builder.append_value(!delete);
    }
    let keep_mask = keep_builder.finish();
    arrow_select::filter::filter_record_batch(batch, &keep_mask)
        .map_err(|e| NanoError::Storage(format!("{} delete filter error: {}", entity_kind, e)))
}

async fn map_sparse_edge_delete_predicate(
    metadata: &DatabaseMetadata,
    edge_type_name: &str,
    property: &str,
    op: CompOp,
    value: &MatchValue,
    params: &crate::ParamMap,
) -> Result<DeletePredicate> {
    let delete_op = comp_op_to_delete_op(op)?;
    match property {
        "from" => {
            let edge_type = metadata
                .catalog()
                .edge_types
                .get(edge_type_name)
                .ok_or_else(|| {
                    NanoError::Execution(format!("unknown edge type `{}`", edge_type_name))
                })?;
            let endpoint = resolve_mutation_match_value(value, params)?;
            let endpoint_name = literal_to_endpoint_name(&endpoint, "from")?;
            let src_id =
                resolve_sparse_node_id_by_name(metadata, &edge_type.from_type, &endpoint_name)
                    .await?;
            Ok(DeletePredicate {
                property: "src".to_string(),
                op: delete_op,
                value: src_id.to_string(),
            })
        }
        "to" => {
            let edge_type = metadata
                .catalog()
                .edge_types
                .get(edge_type_name)
                .ok_or_else(|| {
                    NanoError::Execution(format!("unknown edge type `{}`", edge_type_name))
                })?;
            let endpoint = resolve_mutation_match_value(value, params)?;
            let endpoint_name = literal_to_endpoint_name(&endpoint, "to")?;
            let dst_id =
                resolve_sparse_node_id_by_name(metadata, &edge_type.to_type, &endpoint_name)
                    .await?;
            Ok(DeletePredicate {
                property: "dst".to_string(),
                op: delete_op,
                value: dst_id.to_string(),
            })
        }
        _ => Ok(DeletePredicate {
            property: property.to_string(),
            op: delete_op,
            value: literal_to_predicate_string(&resolve_mutation_match_value(value, params)?)?,
        }),
    }
}

pub(crate) async fn resolve_sparse_node_id_by_name(
    metadata: &DatabaseMetadata,
    node_type: &str,
    node_name: &str,
) -> Result<u64> {
    let batch = read_sparse_node_batch(metadata, node_type)
        .await?
        .ok_or_else(|| {
            NanoError::Execution(format!(
                "edge endpoint lookup failed: node type `{}` has no rows",
                node_type
            ))
        })?;

    let id_col = batch
        .column_by_name("id")
        .ok_or_else(|| NanoError::Execution("node batch missing id column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Execution("node id column is not UInt64".to_string()))?;
    let name_col = batch
        .column_by_name("name")
        .ok_or_else(|| {
            NanoError::Execution(format!(
                "edge endpoint lookup requires node type `{}` to have `name` property",
                node_type
            ))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution(format!(
                "edge endpoint lookup requires `{}`.name to be String",
                node_type
            ))
        })?;

    for row in 0..batch.num_rows() {
        if !name_col.is_null(row) && name_col.value(row) == node_name {
            return Ok(id_col.value(row));
        }
    }

    Err(NanoError::Execution(format!(
        "edge endpoint node not found: {}:{}",
        node_type, node_name
    )))
}

fn resolve_mutation_match_value(value: &MatchValue, params: &crate::ParamMap) -> Result<Literal> {
    match value {
        MatchValue::Literal(literal) => Ok(literal.clone()),
        MatchValue::Variable(name) => params.get(name).cloned().ok_or_else(|| {
            NanoError::Execution(format!("missing required mutation parameter `${}`", name))
        }),
        MatchValue::Now => params
            .get(crate::query::ast::NOW_PARAM_NAME)
            .cloned()
            .ok_or_else(|| {
                NanoError::Execution("missing runtime timestamp for mutation now()".to_string())
            }),
    }
}

fn comp_op_to_delete_op(op: CompOp) -> Result<super::DeleteOp> {
    match op {
        CompOp::Eq => Ok(super::DeleteOp::Eq),
        CompOp::Ne => Ok(super::DeleteOp::Ne),
        CompOp::Gt => Ok(super::DeleteOp::Gt),
        CompOp::Lt => Ok(super::DeleteOp::Lt),
        CompOp::Ge => Ok(super::DeleteOp::Ge),
        CompOp::Le => Ok(super::DeleteOp::Le),
        CompOp::Contains => Err(NanoError::Execution(
            "membership predicates are not supported for delete operations".to_string(),
        )),
    }
}

fn literal_to_json_value(lit: &Literal) -> Result<serde_json::Value> {
    match lit {
        Literal::String(s) => Ok(serde_json::Value::String(s.clone())),
        Literal::Integer(i) => Ok(serde_json::Value::Number((*i).into())),
        Literal::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or_else(|| NanoError::Execution(format!("non-finite float literal {}", f))),
        Literal::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Literal::Date(s) | Literal::DateTime(s) => Ok(serde_json::Value::String(s.clone())),
        Literal::List(items) => items
            .iter()
            .map(literal_to_json_value)
            .collect::<Result<Vec<_>>>()
            .map(serde_json::Value::Array),
    }
}

fn literal_to_predicate_string(lit: &Literal) -> Result<String> {
    match lit {
        Literal::String(s) => Ok(s.clone()),
        Literal::Integer(i) => Ok(i.to_string()),
        Literal::Float(f) => Ok(f.to_string()),
        Literal::Bool(b) => Ok(b.to_string()),
        Literal::Date(s) | Literal::DateTime(s) => Ok(s.clone()),
        Literal::List(_) => Err(NanoError::Execution(
            "list literals are not supported in mutation predicates".to_string(),
        )),
    }
}

fn literal_to_endpoint_name(lit: &Literal, endpoint: &str) -> Result<String> {
    match lit {
        Literal::String(s) => Ok(s.clone()),
        _ => Err(NanoError::Execution(format!(
            "edge endpoint `{}` must be a String literal or String parameter",
            endpoint
        ))),
    }
}

fn select_embed_types<'a>(
    schema_ir: &'a SchemaIR,
    embed_specs: &'a HashMap<String, Vec<crate::store::loader::EmbedSpec>>,
    type_name: Option<&str>,
    property_name: Option<&str>,
) -> Result<
    Vec<(
        &'a crate::catalog::schema_ir::NodeTypeDef,
        Vec<SelectedEmbedProp>,
    )>,
> {
    let mut selected = Vec::new();

    for node_def in schema_ir.node_types() {
        if let Some(expected) = type_name
            && node_def.name != expected
        {
            continue;
        }
        let Some(specs) = embed_specs.get(&node_def.name) else {
            continue;
        };
        let props: Vec<SelectedEmbedProp> = specs
            .iter()
            .filter(|spec| {
                property_name
                    .map(|name| spec.target_prop == name)
                    .unwrap_or(true)
            })
            .map(|spec| SelectedEmbedProp {
                target_prop: spec.target_prop.clone(),
                source_prop: spec.source_prop.clone(),
                dim: spec.dim,
                indexed: node_def
                    .properties
                    .iter()
                    .find(|prop| prop.name == spec.target_prop)
                    .map(|prop| {
                        prop.index
                            && matches!(
                                ScalarType::from_str_name(&prop.scalar_type),
                                Some(ScalarType::Vector(_))
                            )
                    })
                    .unwrap_or(false),
            })
            .collect();
        if !props.is_empty() {
            selected.push((node_def, props));
        }
    }

    if selected.is_empty() {
        return match (type_name, property_name) {
            (Some(type_name), Some(property_name)) => Err(NanoError::Execution(format!(
                "type {} has no @embed property {}",
                type_name, property_name
            ))),
            (Some(type_name), None) => Err(NanoError::Execution(format!(
                "type {} has no @embed properties",
                type_name
            ))),
            (None, Some(property_name)) => Err(NanoError::Execution(format!(
                "no @embed properties named {} found",
                property_name
            ))),
            (None, None) => Err(NanoError::Execution(
                "schema has no @embed properties".to_string(),
            )),
        };
    }

    Ok(selected)
}

fn record_batch_to_json_rows(batch: &RecordBatch) -> Vec<JsonMap<String, JsonValue>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    let schema = batch.schema();
    for row_idx in 0..batch.num_rows() {
        let mut row = JsonMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            row.insert(
                field.name().clone(),
                array_value_to_json(batch.column(col_idx), row_idx),
            );
        }
        rows.push(row);
    }
    rows
}

fn json_rows_to_record_batch(
    schema: &arrow_schema::Schema,
    rows: &[JsonMap<String, JsonValue>],
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let values: Vec<JsonValue> = rows
            .iter()
            .map(|row| row.get(field.name()).cloned().unwrap_or(JsonValue::Null))
            .collect();
        columns.push(json_values_to_array(
            &values,
            field.data_type(),
            field.is_nullable(),
        )?);
    }

    RecordBatch::try_new(std::sync::Arc::new(schema.clone()), columns)
        .map_err(|e| NanoError::Storage(format!("rebuild embed batch failed: {}", e)))
}

fn dataset_entity_key(kind: &str, type_name: &str) -> String {
    format!("{}:{}", kind, type_name)
}

fn build_pending_cdc_entries_from_delta(delta: &MutationDelta) -> Result<Vec<CdcLogEntry>> {
    let mut events = Vec::new();

    let mut node_names: Vec<&String> = delta.node_changes.keys().collect();
    node_names.sort();
    let mut edge_names: Vec<&String> = delta.edge_changes.keys().collect();
    edge_names.sort();

    for type_name in &node_names {
        let Some(change) = delta.node_changes.get(*type_name) else {
            continue;
        };
        if let Some(before_batch) = &change.before_for_deletes {
            events.extend(build_delete_cdc_events_from_batch(
                before_batch,
                "node",
                type_name,
            )?);
        }
    }

    for type_name in &edge_names {
        let Some(change) = delta.edge_changes.get(*type_name) else {
            continue;
        };
        if let Some(before_batch) = &change.before_for_deletes {
            events.extend(build_delete_cdc_events_from_batch(
                before_batch,
                "edge",
                type_name,
            )?);
        }
    }

    for type_name in &node_names {
        let Some(change) = delta.node_changes.get(*type_name) else {
            continue;
        };
        if let (Some(before_batch), Some(after_batch)) =
            (&change.before_for_updates, &change.upserts)
        {
            if before_batch.num_rows() != after_batch.num_rows() {
                return Err(NanoError::Storage(format!(
                    "node update delta row count mismatch for {} (before {}, after {})",
                    type_name,
                    before_batch.num_rows(),
                    after_batch.num_rows()
                )));
            }
            for row in 0..after_batch.num_rows() {
                let before = record_batch_row_to_json_map(before_batch, row)?;
                let after = record_batch_row_to_json_map(after_batch, row)?;
                if let Some(event) = build_update_cdc_event("node", type_name, &before, &after) {
                    events.push(event);
                }
            }
        }
    }

    for type_name in &node_names {
        let Some(change) = delta.node_changes.get(*type_name) else {
            continue;
        };
        if let Some(insert_batch) = &change.inserts {
            events.extend(build_insert_cdc_events_from_batch(
                insert_batch,
                "node",
                type_name,
            )?);
        }
    }

    for type_name in &edge_names {
        let Some(change) = delta.edge_changes.get(*type_name) else {
            continue;
        };
        if let Some(insert_batch) = &change.inserts {
            events.extend(build_insert_cdc_events_from_batch(
                insert_batch,
                "edge",
                type_name,
            )?);
        }
    }

    Ok(events)
}

fn finalize_cdc_entries_for_manifest(
    cdc_events: &[CdcLogEntry],
    manifest: &GraphManifest,
) -> Vec<CdcLogEntry> {
    cdc_events
        .iter()
        .enumerate()
        .map(|(seq, entry)| CdcLogEntry {
            tx_id: manifest.last_tx_id.clone(),
            db_version: manifest.db_version,
            seq_in_tx: seq.min(u32::MAX as usize) as u32,
            op: entry.op.clone(),
            entity_kind: entry.entity_kind.clone(),
            type_name: entry.type_name.clone(),
            entity_key: entry.entity_key.clone(),
            payload: entry.payload.clone(),
            committed_at: manifest.committed_at.clone(),
        })
        .collect()
}

fn schema_has_duplicate_field_names(schema: &arrow_schema::Schema) -> bool {
    let mut seen = HashSet::with_capacity(schema.fields().len());
    schema
        .fields()
        .iter()
        .any(|field| !seen.insert(field.name().clone()))
}
