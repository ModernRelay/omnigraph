use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{ArrayRef, RecordBatch, UInt64Array, new_null_array};
use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{
    EdgeTypeDef, NodeTypeDef, PropDef, SchemaIR, TypeDef, build_catalog_from_ir,
};
use crate::error::{NanoError, Result};
use crate::schema::ast::{PropDecl, SchemaDecl, SchemaFile, annotation_value, has_annotation};
use crate::schema::parser::parse_schema;
use crate::store::graph::DatasetAccumulator;
use crate::store::indexing::{rebuild_node_scalar_indexes, rebuild_node_vector_indexes};
use crate::store::lance_io::{read_lance_batches, write_lance_batch};
use crate::store::manifest::{DatasetEntry, GraphManifest, hash_string};
use crate::store::metadata::{DatabaseMetadata, DatasetLocator};
use crate::store::txlog::commit_manifest_and_logs;
use crate::types::ScalarType;

const SCHEMA_PG_FILENAME: &str = "schema.pg";
const SCHEMA_IR_FILENAME: &str = "schema.ir.json";

struct MigrationDataView {
    metadata: DatabaseMetadata,
    node_batches: HashMap<String, Option<RecordBatch>>,
    edge_batches: HashMap<String, Option<RecordBatch>>,
}

impl MigrationDataView {
    fn open(db_path: &Path) -> Result<Self> {
        Ok(Self {
            metadata: DatabaseMetadata::open(db_path)?,
            node_batches: HashMap::new(),
            edge_batches: HashMap::new(),
        })
    }

    fn metadata(&self) -> &DatabaseMetadata {
        &self.metadata
    }

    async fn node_batch(&mut self, type_name: &str) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.node_batches.get(type_name) {
            return Ok(batch.clone());
        }
        let batch = read_dataset_batch(self.metadata.node_dataset_locator(type_name)).await?;
        self.node_batches
            .insert(type_name.to_string(), batch.clone());
        Ok(batch)
    }

    async fn edge_batch(&mut self, type_name: &str) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.edge_batches.get(type_name) {
            return Ok(batch.clone());
        }
        let batch = read_dataset_batch(self.metadata.edge_dataset_locator(type_name)).await?;
        self.edge_batches
            .insert(type_name.to_string(), batch.clone());
        Ok(batch)
    }
}

#[derive(Clone, Copy)]
enum PropertyStorageKind {
    Node,
    Edge,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MigrationSafety {
    Safe,
    Confirm,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum MigrationStep {
    AddNodeType {
        name: String,
        type_id: u32,
    },
    AddEdgeType {
        name: String,
        type_id: u32,
        src_type_id: u32,
        dst_type_id: u32,
    },
    DropNodeType {
        name: String,
        type_id: u32,
    },
    DropEdgeType {
        name: String,
        type_id: u32,
    },
    RenameType {
        type_kind: String,
        type_id: u32,
        old_name: String,
        new_name: String,
    },
    AddProperty {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        data_type: String,
        nullable: bool,
    },
    DropProperty {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
    },
    RenameProperty {
        type_name: String,
        type_id: u32,
        old_name: String,
        new_name: String,
        prop_id: u32,
    },
    AlterPropertyType {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        old_type: String,
        new_type: String,
    },
    AlterPropertyNullability {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        nullable: bool,
    },
    AlterPropertyKey {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        keyed: bool,
    },
    AlterPropertyUnique {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        unique: bool,
    },
    AlterPropertyIndex {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        indexed: bool,
    },
    AlterPropertyEnumValues {
        type_name: String,
        type_id: u32,
        prop_name: String,
        prop_id: u32,
        old_values: Vec<String>,
        new_values: Vec<String>,
    },
    AlterMetadata {
        target_kind: String,
        type_name: String,
        type_id: u32,
        prop_name: Option<String>,
        annotation: String,
        old_value: Option<String>,
        new_value: Option<String>,
    },
    RebindEdgeEndpoints {
        edge_name: String,
        edge_type_id: u32,
        old_src_type_id: u32,
        old_dst_type_id: u32,
        new_src_type_id: u32,
        new_dst_type_id: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedStep {
    pub step: MigrationStep,
    pub safety: MigrationSafety,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub db_path: String,
    pub old_schema_hash: String,
    pub new_schema_hash: String,
    pub steps: Vec<PlannedStep>,
    pub warnings: Vec<String>,
    pub blocked: Vec<String>,
}

impl MigrationPlan {
    pub fn has_blocked(&self) -> bool {
        self.steps
            .iter()
            .any(|s| s.safety == MigrationSafety::Blocked)
            || !self.blocked.is_empty()
    }

    pub fn has_confirm(&self) -> bool {
        self.steps
            .iter()
            .any(|s| s.safety == MigrationSafety::Confirm)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum SchemaCompatibility {
    Additive,
    CompatibleWithConfirmation,
    Breaking,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiffStep {
    pub step: MigrationStep,
    pub classification: SchemaCompatibility,
    pub reason: String,
    #[serde(default)]
    pub remediation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiffReport {
    pub old_schema_hash: String,
    pub new_schema_hash: String,
    pub compatibility: SchemaCompatibility,
    pub has_breaking: bool,
    pub steps: Vec<SchemaDiffStep>,
    pub warnings: Vec<String>,
    pub blocked: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationStatus {
    Applied,
    NeedsConfirmation,
    Blocked,
}

#[derive(Debug, Clone)]
pub struct MigrationExecution {
    pub status: MigrationStatus,
    pub plan: MigrationPlan,
}

struct PlannedMigration {
    plan: MigrationPlan,
    schema_source: String,
    new_ir: SchemaIR,
    manifest: GraphManifest,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
enum JournalState {
    Prepared,
    Applying,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigrationJournal {
    version: u32,
    state: JournalState,
    db_path: String,
    backup_path: String,
    staging_path: String,
    old_schema_hash: String,
    new_schema_hash: String,
    created_at_unix: u64,
}

pub fn reconcile_migration_sidecars(db_path: &Path) -> Result<()> {
    let paths = migration_base_paths(db_path)?;

    if !paths.journal_path.exists() {
        if paths.backup_path.exists() {
            return Err(NanoError::Manifest(format!(
                "found orphaned migration backup at {} without a journal; inspect before proceeding",
                paths.backup_path.display()
            )));
        }
        return Ok(());
    }

    let raw = std::fs::read_to_string(&paths.journal_path)?;
    let journal: MigrationJournal = serde_json::from_str(&raw)
        .map_err(|e| NanoError::Manifest(format!("parse migration journal: {}", e)))?;

    let backup_path = PathBuf::from(&journal.backup_path);
    let staging_path = PathBuf::from(&journal.staging_path);

    match journal.state {
        JournalState::Committed => {
            if !db_path.exists() {
                return Err(NanoError::Manifest(format!(
                    "found committed migration journal at {} but database path {} is missing; manual recovery required",
                    paths.journal_path.display(),
                    db_path.display()
                )));
            }
            if backup_path.exists() {
                std::fs::remove_dir_all(&backup_path)?;
            }
            if staging_path.exists() {
                std::fs::remove_dir_all(&staging_path)?;
            }
            if paths.lock_path.exists() {
                let _ = std::fs::remove_file(&paths.lock_path);
            }
            std::fs::remove_file(&paths.journal_path)?;
            Ok(())
        }
        JournalState::Prepared | JournalState::Aborted => {
            if !db_path.exists() {
                return Err(NanoError::Manifest(format!(
                    "found incomplete migration journal at {} (state {}) but database path {} is missing; manual recovery required",
                    paths.journal_path.display(),
                    journal_state_name(journal.state),
                    db_path.display()
                )));
            }
            if backup_path.exists() {
                return Err(NanoError::Manifest(format!(
                    "found incomplete migration journal at {} (state {}) with backup path {} still present; manual recovery required",
                    paths.journal_path.display(),
                    journal_state_name(journal.state),
                    backup_path.display()
                )));
            }
            if staging_path.exists() {
                std::fs::remove_dir_all(&staging_path)?;
            }
            if paths.lock_path.exists() {
                let _ = std::fs::remove_file(&paths.lock_path);
            }
            std::fs::remove_file(&paths.journal_path)?;
            Ok(())
        }
        // TODO: implement deterministic roll-forward / rollback for APPLYING state.
        state => Err(NanoError::Manifest(format!(
            "incomplete migration journal at {} (state {}); manual recovery required",
            paths.journal_path.display(),
            journal_state_name(state)
        ))),
    }
}

fn journal_state_name(state: JournalState) -> &'static str {
    match state {
        JournalState::Prepared => "PREPARED",
        JournalState::Applying => "APPLYING",
        JournalState::Committed => "COMMITTED",
        JournalState::Aborted => "ABORTED",
    }
}

pub async fn execute_schema_migration(
    db_path: &Path,
    desired_schema_path: Option<&Path>,
    dry_run: bool,
    auto_approve: bool,
) -> Result<MigrationExecution> {
    reconcile_migration_sidecars(db_path)?;
    let planned = plan_schema_migration(db_path, desired_schema_path).await?;

    if planned.plan.has_blocked() {
        return Ok(MigrationExecution {
            status: MigrationStatus::Blocked,
            plan: planned.plan,
        });
    }

    if planned.plan.has_confirm() && !auto_approve {
        return Ok(MigrationExecution {
            status: MigrationStatus::NeedsConfirmation,
            plan: planned.plan,
        });
    }

    if dry_run {
        return Ok(MigrationExecution {
            status: MigrationStatus::Applied,
            plan: planned.plan,
        });
    }

    apply_planned_migration(db_path, &planned).await?;
    Ok(MigrationExecution {
        status: MigrationStatus::Applied,
        plan: planned.plan,
    })
}

async fn plan_schema_migration(
    db_path: &Path,
    desired_schema_path: Option<&Path>,
) -> Result<PlannedMigration> {
    let schema_path = desired_schema_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| db_path.join(SCHEMA_PG_FILENAME));
    let schema_source = std::fs::read_to_string(&schema_path).map_err(|e| {
        NanoError::Io(std::io::Error::new(
            e.kind(),
            format!("failed to read {}: {}", schema_path.display(), e),
        ))
    })?;
    let desired_schema = parse_schema(&schema_source)?;

    let old_ir_json = std::fs::read_to_string(db_path.join(SCHEMA_IR_FILENAME))?;
    let old_ir: SchemaIR = serde_json::from_str(&old_ir_json)
        .map_err(|e| NanoError::Manifest(format!("parse IR error: {}", e)))?;
    let mut manifest = GraphManifest::read(db_path)?;
    bootstrap_identity_counters(&old_ir, &mut manifest);

    let mut warnings = Vec::new();
    let mut blocked = Vec::new();
    let (new_ir, next_type_id, next_prop_id) = build_desired_schema_ir(
        &old_ir,
        &manifest,
        &desired_schema,
        &mut warnings,
        &mut blocked,
    )?;
    manifest.next_type_id = next_type_id;
    manifest.next_prop_id = next_prop_id;

    // Open metadata and manifest-pinned datasets so planner can validate cast/nullability
    // and endpoint rebind against existing data without restoring a full DatasetAccumulator snapshot.
    let mut data_view = MigrationDataView::open(db_path)?;
    let mut steps = Vec::new();
    diff_schema(&old_ir, &new_ir, &mut data_view, &mut steps, &mut blocked).await?;

    let new_ir_json = serde_json::to_string_pretty(&new_ir)
        .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
    let plan = MigrationPlan {
        db_path: db_path.display().to_string(),
        old_schema_hash: hash_string(&old_ir_json),
        new_schema_hash: hash_string(&new_ir_json),
        steps,
        warnings,
        blocked,
    };

    Ok(PlannedMigration {
        plan,
        schema_source,
        new_ir,
        manifest,
    })
}

fn build_desired_schema_ir(
    old_ir: &SchemaIR,
    manifest: &GraphManifest,
    desired_schema: &SchemaFile,
    warnings: &mut Vec<String>,
    blocked: &mut Vec<String>,
) -> Result<(SchemaIR, u32, u32)> {
    build_desired_schema_ir_with_ids(
        old_ir,
        manifest.next_type_id,
        manifest.next_prop_id,
        desired_schema,
        warnings,
        blocked,
    )
}

fn build_desired_schema_ir_with_ids(
    old_ir: &SchemaIR,
    mut next_type_id: u32,
    mut next_prop_id: u32,
    desired_schema: &SchemaFile,
    warnings: &mut Vec<String>,
    blocked: &mut Vec<String>,
) -> Result<(SchemaIR, u32, u32)> {
    let mut old_nodes_by_name = HashMap::new();
    let mut old_edges_by_name = HashMap::new();
    for ty in &old_ir.types {
        match ty {
            TypeDef::Node(n) => {
                old_nodes_by_name.insert(n.name.clone(), n);
            }
            TypeDef::Edge(e) => {
                old_edges_by_name.insert(e.name.clone(), e);
            }
        }
    }

    let mut used_old_node_type_ids = HashSet::new();
    let mut used_old_edge_type_ids = HashSet::new();

    let mut node_defs = Vec::new();
    let mut edge_defs = Vec::new();
    let mut node_name_to_type_id = HashMap::new();

    for decl in &desired_schema.declarations {
        let SchemaDecl::Node(node) = decl else {
            continue;
        };

        let rename_from = annotation_value(&node.annotations, "rename_from");
        let old_node = match rename_from {
            Some(from) => {
                let found = old_nodes_by_name.get(from).copied();
                if found.is_none() {
                    blocked.push(format!(
                        "node `{}` has @rename_from(\"{}\") but source type does not exist",
                        node.name, from
                    ));
                }
                found
            }
            None => old_nodes_by_name.get(&node.name).copied(),
        };

        let type_id = if let Some(old) = old_node {
            if used_old_node_type_ids.insert(old.type_id) {
                old.type_id
            } else {
                blocked.push(format!(
                    "node `{}` maps to reused source type_id {}",
                    node.name, old.type_id
                ));
                alloc_next_id(&mut next_type_id)?
            }
        } else {
            alloc_next_id(&mut next_type_id)?
        };
        node_name_to_type_id.insert(node.name.clone(), type_id);

        let mut old_props_by_name = HashMap::new();
        if let Some(old) = old_node {
            for p in &old.properties {
                old_props_by_name.insert(p.name.clone(), p);
            }
        }
        let mut used_old_prop_ids = HashSet::new();
        let mut props = Vec::new();
        for prop in &node.properties {
            let old_prop = resolve_old_prop(prop, &old_props_by_name, &node.name, blocked);
            let prop_id = if let Some(old) = old_prop {
                if used_old_prop_ids.insert(old.prop_id) {
                    old.prop_id
                } else {
                    blocked.push(format!(
                        "node `{}` property `{}` maps to reused source prop_id {}",
                        node.name, prop.name, old.prop_id
                    ));
                    alloc_next_id(&mut next_prop_id)?
                }
            } else {
                alloc_next_id(&mut next_prop_id)?
            };
            props.push(PropDef {
                name: prop.name.clone(),
                prop_id,
                scalar_type: prop.prop_type.scalar.to_string(),
                list: prop.prop_type.list,
                enum_values: prop.prop_type.enum_values.clone().unwrap_or_default(),
                nullable: prop.prop_type.nullable,
                key: has_annotation(&prop.annotations, "key"),
                unique: has_annotation(&prop.annotations, "unique"),
                index: has_annotation(&prop.annotations, "key")
                    || has_annotation(&prop.annotations, "index"),
                embed_source: annotation_value(&prop.annotations, "embed").map(str::to_string),
                description: annotation_value(&prop.annotations, "description").map(str::to_string),
            });
        }

        if let Some(from) = rename_from
            && from == node.name
        {
            warnings.push(format!(
                "node `{}` uses redundant @rename_from(\"{}\")",
                node.name, from
            ));
        }

        node_defs.push(NodeTypeDef {
            name: node.name.clone(),
            type_id,
            description: annotation_value(&node.annotations, "description").map(str::to_string),
            instruction: annotation_value(&node.annotations, "instruction").map(str::to_string),
            properties: props,
        });
    }

    for decl in &desired_schema.declarations {
        let SchemaDecl::Edge(edge) = decl else {
            continue;
        };

        let rename_from = annotation_value(&edge.annotations, "rename_from");
        let old_edge = match rename_from {
            Some(from) => {
                let found = old_edges_by_name.get(from).copied();
                if found.is_none() {
                    blocked.push(format!(
                        "edge `{}` has @rename_from(\"{}\") but source type does not exist",
                        edge.name, from
                    ));
                }
                found
            }
            None => old_edges_by_name.get(&edge.name).copied(),
        };

        let src_type_id = *node_name_to_type_id.get(&edge.from_type).ok_or_else(|| {
            NanoError::Catalog(format!(
                "edge `{}` references unknown source node type `{}`",
                edge.name, edge.from_type
            ))
        })?;
        let dst_type_id = *node_name_to_type_id.get(&edge.to_type).ok_or_else(|| {
            NanoError::Catalog(format!(
                "edge `{}` references unknown target node type `{}`",
                edge.name, edge.to_type
            ))
        })?;

        let type_id = if let Some(old) = old_edge {
            if used_old_edge_type_ids.insert(old.type_id) {
                old.type_id
            } else {
                blocked.push(format!(
                    "edge `{}` maps to reused source type_id {}",
                    edge.name, old.type_id
                ));
                alloc_next_id(&mut next_type_id)?
            }
        } else {
            alloc_next_id(&mut next_type_id)?
        };

        let mut old_props_by_name = HashMap::new();
        if let Some(old) = old_edge {
            for p in &old.properties {
                old_props_by_name.insert(p.name.clone(), p);
            }
        }
        let mut used_old_prop_ids = HashSet::new();
        let mut props = Vec::new();
        for prop in &edge.properties {
            let old_prop = resolve_old_prop(prop, &old_props_by_name, &edge.name, blocked);
            let prop_id = if let Some(old) = old_prop {
                if used_old_prop_ids.insert(old.prop_id) {
                    old.prop_id
                } else {
                    blocked.push(format!(
                        "edge `{}` property `{}` maps to reused source prop_id {}",
                        edge.name, prop.name, old.prop_id
                    ));
                    alloc_next_id(&mut next_prop_id)?
                }
            } else {
                alloc_next_id(&mut next_prop_id)?
            };
            props.push(PropDef {
                name: prop.name.clone(),
                prop_id,
                scalar_type: prop.prop_type.scalar.to_string(),
                list: prop.prop_type.list,
                enum_values: prop.prop_type.enum_values.clone().unwrap_or_default(),
                nullable: prop.prop_type.nullable,
                key: false,
                unique: false,
                index: false,
                embed_source: None,
                description: annotation_value(&prop.annotations, "description").map(str::to_string),
            });
        }

        if let Some(from) = rename_from
            && from == edge.name
        {
            warnings.push(format!(
                "edge `{}` uses redundant @rename_from(\"{}\")",
                edge.name, from
            ));
        }

        edge_defs.push(EdgeTypeDef {
            name: edge.name.clone(),
            type_id,
            src_type_id,
            dst_type_id,
            src_type_name: edge.from_type.clone(),
            dst_type_name: edge.to_type.clone(),
            description: annotation_value(&edge.annotations, "description").map(str::to_string),
            instruction: annotation_value(&edge.annotations, "instruction").map(str::to_string),
            properties: props,
        });
    }

    let mut types = Vec::new();
    for n in node_defs {
        types.push(TypeDef::Node(n));
    }
    for e in edge_defs {
        types.push(TypeDef::Edge(e));
    }
    let ir = SchemaIR {
        ir_version: old_ir.ir_version.max(2),
        types,
    };
    // Validate resulting IR can build a catalog.
    let _ = build_catalog_from_ir(&ir)?;
    Ok((ir, next_type_id, next_prop_id))
}

fn resolve_old_prop<'a>(
    prop: &'a PropDecl,
    old_props_by_name: &'a HashMap<String, &'a PropDef>,
    type_name: &str,
    blocked: &mut Vec<String>,
) -> Option<&'a PropDef> {
    match annotation_value(&prop.annotations, "rename_from") {
        Some(from) => {
            let old = old_props_by_name.get(from).copied();
            if old.is_none() {
                blocked.push(format!(
                    "type `{}` property `{}` has @rename_from(\"{}\") but source property does not exist",
                    type_name, prop.name, from
                ));
            }
            old
        }
        None => old_props_by_name.get(&prop.name).copied(),
    }
}

fn alloc_next_id(next: &mut u32) -> Result<u32> {
    let id = (*next).max(1);
    *next = id
        .checked_add(1)
        .ok_or_else(|| NanoError::Manifest("identity counter overflow".to_string()))?;
    Ok(id)
}

pub fn analyze_schema_diff(
    old_schema: &SchemaFile,
    new_schema: &SchemaFile,
) -> Result<SchemaDiffReport> {
    let old_ir = crate::catalog::schema_ir::build_schema_ir(old_schema)?;
    let (next_type_id, next_prop_id) = next_schema_identity_counters(&old_ir);
    let mut warnings = Vec::new();
    let mut blocked = Vec::new();
    let (new_ir, _, _) = build_desired_schema_ir_with_ids(
        &old_ir,
        next_type_id,
        next_prop_id,
        new_schema,
        &mut warnings,
        &mut blocked,
    )?;

    let old_ir_json = serde_json::to_string_pretty(&old_ir)
        .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
    let new_ir_json = serde_json::to_string_pretty(&new_ir)
        .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;

    let mut steps = Vec::new();
    analyze_schema_changes(&old_ir, &new_ir, &mut steps, &mut blocked);

    let compatibility = overall_schema_compatibility(&steps, &blocked);
    Ok(SchemaDiffReport {
        old_schema_hash: hash_string(&old_ir_json),
        new_schema_hash: hash_string(&new_ir_json),
        compatibility,
        has_breaking: compatibility >= SchemaCompatibility::Breaking,
        steps,
        warnings,
        blocked,
    })
}

fn next_schema_identity_counters(ir: &SchemaIR) -> (u32, u32) {
    let next_type_id = ir
        .types
        .iter()
        .map(|typedef| match typedef {
            TypeDef::Node(node) => node.type_id,
            TypeDef::Edge(edge) => edge.type_id,
        })
        .max()
        .unwrap_or(0)
        .saturating_add(1)
        .max(1);
    let next_prop_id = ir
        .types
        .iter()
        .flat_map(|typedef| match typedef {
            TypeDef::Node(node) => node.properties.iter(),
            TypeDef::Edge(edge) => edge.properties.iter(),
        })
        .map(|prop| prop.prop_id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
        .max(1);
    (next_type_id, next_prop_id)
}

fn analyze_schema_changes(
    old_ir: &SchemaIR,
    new_ir: &SchemaIR,
    steps: &mut Vec<SchemaDiffStep>,
    blocked: &mut Vec<String>,
) {
    let old_nodes = old_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Node(node) => Some((node.type_id, node)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let new_nodes = new_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Node(node) => Some((node.type_id, node)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let old_edges = old_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Edge(edge) => Some((edge.type_id, edge)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let new_edges = new_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Edge(edge) => Some((edge.type_id, edge)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    let unmatched_old_nodes = old_nodes
        .values()
        .copied()
        .filter(|node| !new_nodes.contains_key(&node.type_id))
        .collect::<Vec<_>>();
    let unmatched_new_nodes = new_nodes
        .values()
        .copied()
        .filter(|node| !old_nodes.contains_key(&node.type_id))
        .collect::<Vec<_>>();
    let unmatched_old_edges = old_edges
        .values()
        .copied()
        .filter(|edge| !new_edges.contains_key(&edge.type_id))
        .collect::<Vec<_>>();
    let unmatched_new_edges = new_edges
        .values()
        .copied()
        .filter(|edge| !old_edges.contains_key(&edge.type_id))
        .collect::<Vec<_>>();

    for new_node in new_nodes.values() {
        match old_nodes.get(&new_node.type_id) {
            None => {
                let remediation = possible_node_rename_source(new_node, &unmatched_old_nodes)
                    .map(|old_name| format!("@rename_from(\"{}\")", old_name));
                let classification = if remediation.is_some() {
                    SchemaCompatibility::CompatibleWithConfirmation
                } else {
                    SchemaCompatibility::Additive
                };
                let reason = if let Some(old_name) = remediation
                    .as_ref()
                    .and_then(|value| value.strip_prefix("@rename_from(\""))
                    .and_then(|value| value.strip_suffix("\")"))
                {
                    format!(
                        "possible node rename from `{}` without @rename_from",
                        old_name
                    )
                } else {
                    "new node type".to_string()
                };
                steps.push(SchemaDiffStep {
                    step: MigrationStep::AddNodeType {
                        name: new_node.name.clone(),
                        type_id: new_node.type_id,
                    },
                    classification,
                    reason,
                    remediation,
                });
            }
            Some(old_node) => {
                if old_node.name != new_node.name {
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::RenameType {
                            type_kind: "node".to_string(),
                            type_id: new_node.type_id,
                            old_name: old_node.name.clone(),
                            new_name: new_node.name.clone(),
                        },
                        classification: SchemaCompatibility::Additive,
                        reason: "node rename preserved with @rename_from".to_string(),
                        remediation: None,
                    });
                }
                analyze_metadata_change(
                    MetadataTarget {
                        target_kind: "node",
                        type_name: &new_node.name,
                        type_id: new_node.type_id,
                        prop: None,
                    },
                    "description",
                    old_node.description.as_deref(),
                    new_node.description.as_deref(),
                    steps,
                );
                analyze_metadata_change(
                    MetadataTarget {
                        target_kind: "node",
                        type_name: &new_node.name,
                        type_id: new_node.type_id,
                        prop: None,
                    },
                    "instruction",
                    old_node.instruction.as_deref(),
                    new_node.instruction.as_deref(),
                    steps,
                );
                analyze_property_changes(
                    &old_node.name,
                    &new_node.name,
                    old_node.type_id,
                    &old_node.properties,
                    &new_node.properties,
                    steps,
                    blocked,
                );
            }
        }
    }

    for old_node in old_nodes.values() {
        if new_nodes.contains_key(&old_node.type_id) {
            continue;
        }
        let remediation =
            possible_node_rename_target(old_node, &unmatched_new_nodes).map(|new_name| {
                format!(
                    "consider @rename_from(\"{}\") on `{}`",
                    old_node.name, new_name
                )
            });
        let classification = SchemaCompatibility::CompatibleWithConfirmation;
        let reason = if remediation.is_some() {
            format!(
                "possible node rename from `{}` without @rename_from",
                old_node.name
            )
        } else {
            "node type removed from desired schema".to_string()
        };
        steps.push(SchemaDiffStep {
            step: MigrationStep::DropNodeType {
                name: old_node.name.clone(),
                type_id: old_node.type_id,
            },
            classification,
            reason,
            remediation,
        });
    }

    for new_edge in new_edges.values() {
        match old_edges.get(&new_edge.type_id) {
            None => {
                let remediation = possible_edge_rename_source(new_edge, &unmatched_old_edges)
                    .map(|old_name| format!("@rename_from(\"{}\")", old_name));
                let classification = if remediation.is_some() {
                    SchemaCompatibility::CompatibleWithConfirmation
                } else {
                    SchemaCompatibility::Additive
                };
                let reason = if let Some(old_name) = remediation
                    .as_ref()
                    .and_then(|value| value.strip_prefix("@rename_from(\""))
                    .and_then(|value| value.strip_suffix("\")"))
                {
                    format!(
                        "possible edge rename from `{}` without @rename_from",
                        old_name
                    )
                } else {
                    "new edge type".to_string()
                };
                steps.push(SchemaDiffStep {
                    step: MigrationStep::AddEdgeType {
                        name: new_edge.name.clone(),
                        type_id: new_edge.type_id,
                        src_type_id: new_edge.src_type_id,
                        dst_type_id: new_edge.dst_type_id,
                    },
                    classification,
                    reason,
                    remediation,
                });
            }
            Some(old_edge) => {
                if old_edge.name != new_edge.name {
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::RenameType {
                            type_kind: "edge".to_string(),
                            type_id: new_edge.type_id,
                            old_name: old_edge.name.clone(),
                            new_name: new_edge.name.clone(),
                        },
                        classification: SchemaCompatibility::Additive,
                        reason: "edge rename preserved with @rename_from".to_string(),
                        remediation: None,
                    });
                }
                analyze_metadata_change(
                    MetadataTarget {
                        target_kind: "edge",
                        type_name: &new_edge.name,
                        type_id: new_edge.type_id,
                        prop: None,
                    },
                    "description",
                    old_edge.description.as_deref(),
                    new_edge.description.as_deref(),
                    steps,
                );
                analyze_metadata_change(
                    MetadataTarget {
                        target_kind: "edge",
                        type_name: &new_edge.name,
                        type_id: new_edge.type_id,
                        prop: None,
                    },
                    "instruction",
                    old_edge.instruction.as_deref(),
                    new_edge.instruction.as_deref(),
                    steps,
                );
                if old_edge.src_type_id != new_edge.src_type_id
                    || old_edge.dst_type_id != new_edge.dst_type_id
                {
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::RebindEdgeEndpoints {
                            edge_name: new_edge.name.clone(),
                            edge_type_id: new_edge.type_id,
                            old_src_type_id: old_edge.src_type_id,
                            old_dst_type_id: old_edge.dst_type_id,
                            new_src_type_id: new_edge.src_type_id,
                            new_dst_type_id: new_edge.dst_type_id,
                        },
                        classification: SchemaCompatibility::Breaking,
                        reason: "edge endpoints changed".to_string(),
                        remediation: None,
                    });
                }
                analyze_property_changes(
                    &old_edge.name,
                    &new_edge.name,
                    old_edge.type_id,
                    &old_edge.properties,
                    &new_edge.properties,
                    steps,
                    blocked,
                );
            }
        }
    }

    for old_edge in old_edges.values() {
        if new_edges.contains_key(&old_edge.type_id) {
            continue;
        }
        let remediation =
            possible_edge_rename_target(old_edge, &unmatched_new_edges).map(|new_name| {
                format!(
                    "consider @rename_from(\"{}\") on `{}`",
                    old_edge.name, new_name
                )
            });
        let reason = if remediation.is_some() {
            format!(
                "possible edge rename from `{}` without @rename_from",
                old_edge.name
            )
        } else {
            "edge type removed from desired schema".to_string()
        };
        steps.push(SchemaDiffStep {
            step: MigrationStep::DropEdgeType {
                name: old_edge.name.clone(),
                type_id: old_edge.type_id,
            },
            classification: SchemaCompatibility::CompatibleWithConfirmation,
            reason,
            remediation,
        });
    }
}

fn analyze_property_changes(
    source_type_name: &str,
    display_type_name: &str,
    type_id: u32,
    old_props: &[PropDef],
    new_props: &[PropDef],
    steps: &mut Vec<SchemaDiffStep>,
    blocked: &mut Vec<String>,
) {
    let old_by_id = old_props
        .iter()
        .map(|prop| (prop.prop_id, prop))
        .collect::<HashMap<_, _>>();
    let new_by_id = new_props
        .iter()
        .map(|prop| (prop.prop_id, prop))
        .collect::<HashMap<_, _>>();

    let unmatched_old = old_props
        .iter()
        .filter(|prop| !new_by_id.contains_key(&prop.prop_id))
        .collect::<Vec<_>>();
    let unmatched_new = new_props
        .iter()
        .filter(|prop| !old_by_id.contains_key(&prop.prop_id))
        .collect::<Vec<_>>();

    for new_prop in new_props {
        match old_by_id.get(&new_prop.prop_id) {
            None => {
                let remediation = possible_property_rename_source(new_prop, &unmatched_old)
                    .map(|old_name| format!("@rename_from(\"{}\")", old_name));
                let (classification, reason, remediation) = if let Some(remediation) = remediation {
                    (
                        SchemaCompatibility::CompatibleWithConfirmation,
                        format!(
                            "possible property rename without @rename_from on `{}`.`{}`",
                            display_type_name, new_prop.name
                        ),
                        Some(remediation),
                    )
                } else if new_prop.nullable {
                    (
                        SchemaCompatibility::Additive,
                        "new nullable property".to_string(),
                        None,
                    )
                } else {
                    let reason = format!(
                        "type `{}` adds non-nullable property `{}` without a backfill",
                        display_type_name, new_prop.name
                    );
                    blocked.push(reason.clone());
                    (
                        SchemaCompatibility::Blocked,
                        reason,
                        Some(
                            "make the property nullable or backfill it during migration"
                                .to_string(),
                        ),
                    )
                };
                steps.push(SchemaDiffStep {
                    step: MigrationStep::AddProperty {
                        type_name: display_type_name.to_string(),
                        type_id,
                        prop_name: new_prop.name.clone(),
                        prop_id: new_prop.prop_id,
                        data_type: prop_display_type(new_prop),
                        nullable: new_prop.nullable,
                    },
                    classification,
                    reason,
                    remediation,
                });
            }
            Some(old_prop) => {
                if old_prop.name != new_prop.name {
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::RenameProperty {
                            type_name: display_type_name.to_string(),
                            type_id,
                            old_name: old_prop.name.clone(),
                            new_name: new_prop.name.clone(),
                            prop_id: new_prop.prop_id,
                        },
                        classification: SchemaCompatibility::Additive,
                        reason: "property rename preserved with @rename_from".to_string(),
                        remediation: None,
                    });
                }
                analyze_metadata_change(
                    MetadataTarget {
                        target_kind: "property",
                        type_name: display_type_name,
                        type_id,
                        prop: Some((&new_prop.name, new_prop.prop_id)),
                    },
                    "description",
                    old_prop.description.as_deref(),
                    new_prop.description.as_deref(),
                    steps,
                );
                if old_prop.enum_values != new_prop.enum_values {
                    let (classification, reason, remediation) = classify_enum_value_change(
                        display_type_name,
                        &new_prop.name,
                        old_prop,
                        new_prop,
                    );
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::AlterPropertyEnumValues {
                            type_name: display_type_name.to_string(),
                            type_id,
                            prop_name: new_prop.name.clone(),
                            prop_id: new_prop.prop_id,
                            old_values: old_prop.enum_values.clone(),
                            new_values: new_prop.enum_values.clone(),
                        },
                        classification,
                        reason,
                        remediation,
                    });
                }
                if old_prop.list != new_prop.list || old_prop.scalar_type != new_prop.scalar_type {
                    let (classification, reason, remediation) = classify_schema_only_type_change(
                        display_type_name,
                        &new_prop.name,
                        old_prop,
                        new_prop,
                    );
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::AlterPropertyType {
                            type_name: display_type_name.to_string(),
                            type_id,
                            prop_name: new_prop.name.clone(),
                            prop_id: new_prop.prop_id,
                            old_type: prop_display_type(old_prop),
                            new_type: prop_display_type(new_prop),
                        },
                        classification,
                        reason,
                        remediation,
                    });
                }
                if old_prop.nullable != new_prop.nullable {
                    let (classification, reason) = classify_schema_only_nullability_change(
                        display_type_name,
                        &new_prop.name,
                        old_prop,
                        new_prop,
                    );
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::AlterPropertyNullability {
                            type_name: display_type_name.to_string(),
                            type_id,
                            prop_name: new_prop.name.clone(),
                            prop_id: new_prop.prop_id,
                            nullable: new_prop.nullable,
                        },
                        classification,
                        reason,
                        remediation: None,
                    });
                }
                if old_prop.key != new_prop.key {
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::AlterPropertyKey {
                            type_name: display_type_name.to_string(),
                            type_id,
                            prop_name: new_prop.name.clone(),
                            prop_id: new_prop.prop_id,
                            keyed: new_prop.key,
                        },
                        classification: SchemaCompatibility::Breaking,
                        reason: format!(
                            "changing @key on `{}`.`{}` changes identity semantics",
                            display_type_name, new_prop.name
                        ),
                        remediation: None,
                    });
                }
                if old_prop.unique != new_prop.unique {
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::AlterPropertyUnique {
                            type_name: display_type_name.to_string(),
                            type_id,
                            prop_name: new_prop.name.clone(),
                            prop_id: new_prop.prop_id,
                            unique: new_prop.unique,
                        },
                        classification: SchemaCompatibility::CompatibleWithConfirmation,
                        reason: if new_prop.unique {
                            "unique constraint added".to_string()
                        } else {
                            "unique constraint removed".to_string()
                        },
                        remediation: None,
                    });
                }
                if old_prop.index != new_prop.index {
                    steps.push(SchemaDiffStep {
                        step: MigrationStep::AlterPropertyIndex {
                            type_name: display_type_name.to_string(),
                            type_id,
                            prop_name: new_prop.name.clone(),
                            prop_id: new_prop.prop_id,
                            indexed: new_prop.index,
                        },
                        classification: if new_prop.index {
                            SchemaCompatibility::Additive
                        } else {
                            SchemaCompatibility::CompatibleWithConfirmation
                        },
                        reason: if new_prop.index {
                            "index added".to_string()
                        } else {
                            "index removed".to_string()
                        },
                        remediation: None,
                    });
                }
            }
        }
    }

    for old_prop in old_props {
        if new_by_id.contains_key(&old_prop.prop_id) {
            continue;
        }
        let remediation =
            possible_property_rename_target(old_prop, &unmatched_new).map(|new_name| {
                format!(
                    "consider @rename_from(\"{}\") on `{}`.`{}`",
                    old_prop.name, source_type_name, new_name
                )
            });
        let reason = if remediation.is_some() {
            format!(
                "possible property rename without @rename_from on `{}`.`{}`",
                display_type_name, old_prop.name
            )
        } else {
            "property removed from desired schema".to_string()
        };
        steps.push(SchemaDiffStep {
            step: MigrationStep::DropProperty {
                type_name: display_type_name.to_string(),
                type_id,
                prop_name: old_prop.name.clone(),
                prop_id: old_prop.prop_id,
            },
            classification: SchemaCompatibility::CompatibleWithConfirmation,
            reason,
            remediation,
        });
    }
}

struct MetadataTarget<'a> {
    target_kind: &'a str,
    type_name: &'a str,
    type_id: u32,
    prop: Option<(&'a str, u32)>,
}

fn analyze_metadata_change(
    target: MetadataTarget<'_>,
    annotation: &str,
    old_value: Option<&str>,
    new_value: Option<&str>,
    steps: &mut Vec<SchemaDiffStep>,
) {
    if old_value == new_value {
        return;
    }
    let (prop_name, _prop_id) = target
        .prop
        .map(|(name, prop_id)| (Some(name.to_string()), Some(prop_id)))
        .unwrap_or((None, None));
    steps.push(SchemaDiffStep {
        step: MigrationStep::AlterMetadata {
            target_kind: target.target_kind.to_string(),
            type_name: target.type_name.to_string(),
            type_id: target.type_id,
            prop_name,
            annotation: annotation.to_string(),
            old_value: old_value.map(str::to_string),
            new_value: new_value.map(str::to_string),
        },
        classification: SchemaCompatibility::Additive,
        reason: format!("{} metadata updated", annotation),
        remediation: None,
    });
}

fn classify_enum_value_change(
    type_name: &str,
    prop_name: &str,
    old_prop: &PropDef,
    new_prop: &PropDef,
) -> (SchemaCompatibility, String, Option<String>) {
    let old_values = old_prop.enum_values.iter().cloned().collect::<HashSet<_>>();
    let new_values = new_prop.enum_values.iter().cloned().collect::<HashSet<_>>();

    if old_values.is_subset(&new_values) && !new_values.is_subset(&old_values) {
        return (
            SchemaCompatibility::Additive,
            format!("enum domain expanded for `{}`.`{}`", type_name, prop_name),
            None,
        );
    }
    if new_values.is_subset(&old_values) && !old_values.is_subset(&new_values) {
        return (
            SchemaCompatibility::Breaking,
            format!("enum values removed from `{}`.`{}`", type_name, prop_name),
            None,
        );
    }
    (
        SchemaCompatibility::Breaking,
        format!(
            "enum values changed incompatibly for `{}`.`{}`",
            type_name, prop_name
        ),
        None,
    )
}

fn classify_schema_only_type_change(
    type_name: &str,
    prop_name: &str,
    old_prop: &PropDef,
    new_prop: &PropDef,
) -> (SchemaCompatibility, String, Option<String>) {
    if old_prop.list != new_prop.list {
        return (
            SchemaCompatibility::Breaking,
            format!(
                "list/scalar shape changed for `{}`.`{}`",
                type_name, prop_name
            ),
            None,
        );
    }

    let old_scalar = ScalarType::from_str_name(&old_prop.scalar_type);
    let new_scalar = ScalarType::from_str_name(&new_prop.scalar_type);
    if let (Some(old_scalar), Some(new_scalar)) = (old_scalar, new_scalar)
        && is_lossless_scalar_change(old_scalar, new_scalar)
    {
        return (
            SchemaCompatibility::CompatibleWithConfirmation,
            format!(
                "lossless scalar widening for `{}`.`{}` requires a rewrite",
                type_name, prop_name
            ),
            None,
        );
    }

    (
        SchemaCompatibility::Breaking,
        format!(
            "non-lossless type change for `{}`.`{}`",
            type_name, prop_name
        ),
        None,
    )
}

fn classify_schema_only_nullability_change(
    type_name: &str,
    prop_name: &str,
    old_prop: &PropDef,
    new_prop: &PropDef,
) -> (SchemaCompatibility, String) {
    if old_prop.nullable && !new_prop.nullable {
        (
            SchemaCompatibility::Breaking,
            format!("nullability tightened for `{}`.`{}`", type_name, prop_name),
        )
    } else {
        (
            SchemaCompatibility::Additive,
            format!("property `{}`.`{}` became nullable", type_name, prop_name),
        )
    }
}

fn overall_schema_compatibility(
    steps: &[SchemaDiffStep],
    blocked: &[String],
) -> SchemaCompatibility {
    let mut compatibility = if blocked.is_empty() {
        SchemaCompatibility::Additive
    } else {
        SchemaCompatibility::Blocked
    };
    for step in steps {
        compatibility = compatibility.max(step.classification);
    }
    compatibility
}

fn possible_node_rename_source<'a>(
    new_node: &NodeTypeDef,
    unmatched_old_nodes: &[&'a NodeTypeDef],
) -> Option<&'a str> {
    unmatched_old_nodes
        .iter()
        .copied()
        .find(|old_node| same_node_signature(old_node, new_node))
        .map(|node| node.name.as_str())
}

fn possible_node_rename_target<'a>(
    old_node: &NodeTypeDef,
    unmatched_new_nodes: &[&'a NodeTypeDef],
) -> Option<&'a str> {
    unmatched_new_nodes
        .iter()
        .copied()
        .find(|new_node| same_node_signature(old_node, new_node))
        .map(|node| node.name.as_str())
}

fn possible_edge_rename_source<'a>(
    new_edge: &EdgeTypeDef,
    unmatched_old_edges: &[&'a EdgeTypeDef],
) -> Option<&'a str> {
    unmatched_old_edges
        .iter()
        .copied()
        .find(|old_edge| same_edge_signature(old_edge, new_edge))
        .map(|edge| edge.name.as_str())
}

fn possible_edge_rename_target<'a>(
    old_edge: &EdgeTypeDef,
    unmatched_new_edges: &[&'a EdgeTypeDef],
) -> Option<&'a str> {
    unmatched_new_edges
        .iter()
        .copied()
        .find(|new_edge| same_edge_signature(old_edge, new_edge))
        .map(|edge| edge.name.as_str())
}

fn possible_property_rename_source<'a>(
    new_prop: &PropDef,
    unmatched_old_props: &[&'a PropDef],
) -> Option<&'a str> {
    unmatched_old_props
        .iter()
        .copied()
        .find(|old_prop| same_property_signature(old_prop, new_prop))
        .map(|prop| prop.name.as_str())
}

fn possible_property_rename_target<'a>(
    old_prop: &PropDef,
    unmatched_new_props: &[&'a PropDef],
) -> Option<&'a str> {
    unmatched_new_props
        .iter()
        .copied()
        .find(|new_prop| same_property_signature(old_prop, new_prop))
        .map(|prop| prop.name.as_str())
}

fn same_node_signature(old_node: &NodeTypeDef, new_node: &NodeTypeDef) -> bool {
    same_property_collections(&old_node.properties, &new_node.properties)
}

fn same_edge_signature(old_edge: &EdgeTypeDef, new_edge: &EdgeTypeDef) -> bool {
    old_edge.src_type_name == new_edge.src_type_name
        && old_edge.dst_type_name == new_edge.dst_type_name
        && same_property_collections(&old_edge.properties, &new_edge.properties)
}

fn same_property_collections(old_props: &[PropDef], new_props: &[PropDef]) -> bool {
    if old_props.len() != new_props.len() {
        return false;
    }
    old_props.iter().all(|old_prop| {
        new_props
            .iter()
            .find(|new_prop| new_prop.name == old_prop.name)
            .is_some_and(|new_prop| same_property_signature(old_prop, new_prop))
    })
}

fn same_property_signature(old_prop: &PropDef, new_prop: &PropDef) -> bool {
    old_prop.scalar_type == new_prop.scalar_type
        && old_prop.list == new_prop.list
        && old_prop.enum_values == new_prop.enum_values
        && old_prop.nullable == new_prop.nullable
        && old_prop.key == new_prop.key
        && old_prop.unique == new_prop.unique
        && old_prop.index == new_prop.index
        && old_prop.embed_source == new_prop.embed_source
}

fn is_lossless_scalar_change(old_scalar: ScalarType, new_scalar: ScalarType) -> bool {
    matches!(
        (old_scalar, new_scalar),
        (ScalarType::I32, ScalarType::I64)
            | (ScalarType::U32, ScalarType::U64)
            | (ScalarType::U32, ScalarType::I64)
            | (ScalarType::F32, ScalarType::F64)
    )
}

fn prop_display_type(prop: &PropDef) -> String {
    let base = if prop.enum_values.is_empty() {
        prop.scalar_type.clone()
    } else {
        format!("enum({})", prop.enum_values.join(", "))
    };
    let wrapped = if prop.list {
        format!("[{}]", base)
    } else {
        base
    };
    if prop.nullable {
        format!("{}?", wrapped)
    } else {
        wrapped
    }
}

async fn diff_schema(
    old_ir: &SchemaIR,
    new_ir: &SchemaIR,
    data_view: &mut MigrationDataView,
    steps: &mut Vec<PlannedStep>,
    blocked: &mut Vec<String>,
) -> Result<()> {
    let old_nodes = old_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Node(n) => Some((n.type_id, n)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let new_nodes = new_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Node(n) => Some((n.type_id, n)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    let old_edges = old_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Edge(e) => Some((e.type_id, e)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let new_edges = new_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Edge(e) => Some((e.type_id, e)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    for new_node in new_nodes.values() {
        match old_nodes.get(&new_node.type_id) {
            None => {
                steps.push(PlannedStep {
                    step: MigrationStep::AddNodeType {
                        name: new_node.name.clone(),
                        type_id: new_node.type_id,
                    },
                    safety: MigrationSafety::Safe,
                    reason: "new node type".to_string(),
                });
            }
            Some(old_node) => {
                if old_node.name != new_node.name {
                    steps.push(PlannedStep {
                        step: MigrationStep::RenameType {
                            type_kind: "node".to_string(),
                            type_id: new_node.type_id,
                            old_name: old_node.name.clone(),
                            new_name: new_node.name.clone(),
                        },
                        safety: MigrationSafety::Safe,
                        reason: "id-preserving node rename".to_string(),
                    });
                }
                diff_properties(
                    PropertyDiffTarget {
                        source_type_name: &old_node.name,
                        display_type_name: &new_node.name,
                        type_id: old_node.type_id,
                        storage_kind: PropertyStorageKind::Node,
                    },
                    &old_node.properties,
                    &new_node.properties,
                    data_view,
                    steps,
                    blocked,
                )
                .await?;
            }
        }
    }

    for old_node in old_nodes.values() {
        if !new_nodes.contains_key(&old_node.type_id) {
            steps.push(PlannedStep {
                step: MigrationStep::DropNodeType {
                    name: old_node.name.clone(),
                    type_id: old_node.type_id,
                },
                safety: MigrationSafety::Confirm,
                reason: "node type removed from desired schema".to_string(),
            });
        }
    }

    for new_edge in new_edges.values() {
        match old_edges.get(&new_edge.type_id) {
            None => {
                steps.push(PlannedStep {
                    step: MigrationStep::AddEdgeType {
                        name: new_edge.name.clone(),
                        type_id: new_edge.type_id,
                        src_type_id: new_edge.src_type_id,
                        dst_type_id: new_edge.dst_type_id,
                    },
                    safety: MigrationSafety::Safe,
                    reason: "new edge type".to_string(),
                });
            }
            Some(old_edge) => {
                if old_edge.name != new_edge.name {
                    steps.push(PlannedStep {
                        step: MigrationStep::RenameType {
                            type_kind: "edge".to_string(),
                            type_id: new_edge.type_id,
                            old_name: old_edge.name.clone(),
                            new_name: new_edge.name.clone(),
                        },
                        safety: MigrationSafety::Safe,
                        reason: "id-preserving edge rename".to_string(),
                    });
                }

                if old_edge.src_type_id != new_edge.src_type_id
                    || old_edge.dst_type_id != new_edge.dst_type_id
                {
                    let (safety, reason) =
                        classify_endpoint_rebind(data_view, old_ir, old_edge, new_edge).await;
                    if safety == MigrationSafety::Blocked {
                        blocked.push(reason.clone());
                    }
                    steps.push(PlannedStep {
                        step: MigrationStep::RebindEdgeEndpoints {
                            edge_name: new_edge.name.clone(),
                            edge_type_id: new_edge.type_id,
                            old_src_type_id: old_edge.src_type_id,
                            old_dst_type_id: old_edge.dst_type_id,
                            new_src_type_id: new_edge.src_type_id,
                            new_dst_type_id: new_edge.dst_type_id,
                        },
                        safety,
                        reason,
                    });
                }

                diff_properties(
                    PropertyDiffTarget {
                        source_type_name: &old_edge.name,
                        display_type_name: &new_edge.name,
                        type_id: old_edge.type_id,
                        storage_kind: PropertyStorageKind::Edge,
                    },
                    &old_edge.properties,
                    &new_edge.properties,
                    data_view,
                    steps,
                    blocked,
                )
                .await?;
            }
        }
    }

    for old_edge in old_edges.values() {
        if !new_edges.contains_key(&old_edge.type_id) {
            steps.push(PlannedStep {
                step: MigrationStep::DropEdgeType {
                    name: old_edge.name.clone(),
                    type_id: old_edge.type_id,
                },
                safety: MigrationSafety::Confirm,
                reason: "edge type removed from desired schema".to_string(),
            });
        }
    }

    Ok(())
}

struct PropertyDiffTarget<'a> {
    source_type_name: &'a str,
    display_type_name: &'a str,
    type_id: u32,
    storage_kind: PropertyStorageKind,
}

async fn diff_properties(
    target: PropertyDiffTarget<'_>,
    old_props: &[PropDef],
    new_props: &[PropDef],
    data_view: &mut MigrationDataView,
    steps: &mut Vec<PlannedStep>,
    blocked: &mut Vec<String>,
) -> Result<()> {
    let old_by_id = old_props
        .iter()
        .map(|p| (p.prop_id, p))
        .collect::<HashMap<_, _>>();
    let new_by_id = new_props
        .iter()
        .map(|p| (p.prop_id, p))
        .collect::<HashMap<_, _>>();

    for new_p in new_props {
        match old_by_id.get(&new_p.prop_id) {
            None => {
                let safety = if new_p.nullable {
                    MigrationSafety::Safe
                } else {
                    blocked.push(format!(
                        "type `{}` adds non-nullable property `{}` without a default/backfill",
                        target.display_type_name, new_p.name
                    ));
                    MigrationSafety::Blocked
                };
                steps.push(PlannedStep {
                    step: MigrationStep::AddProperty {
                        type_name: target.display_type_name.to_string(),
                        type_id: target.type_id,
                        prop_name: new_p.name.clone(),
                        prop_id: new_p.prop_id,
                        data_type: new_p.scalar_type.clone(),
                        nullable: new_p.nullable,
                    },
                    safety,
                    reason: "new property".to_string(),
                });
            }
            Some(old_p) => {
                if old_p.name != new_p.name {
                    steps.push(PlannedStep {
                        step: MigrationStep::RenameProperty {
                            type_name: target.display_type_name.to_string(),
                            type_id: target.type_id,
                            old_name: old_p.name.clone(),
                            new_name: new_p.name.clone(),
                            prop_id: new_p.prop_id,
                        },
                        safety: MigrationSafety::Safe,
                        reason: "id-preserving property rename".to_string(),
                    });
                }
                if old_p.scalar_type != new_p.scalar_type {
                    let (safety, reason) = classify_type_change(
                        data_view,
                        target.storage_kind,
                        target.source_type_name,
                        target.display_type_name,
                        &old_p.name,
                        &new_p.scalar_type,
                    )
                    .await?;
                    if safety == MigrationSafety::Blocked {
                        blocked.push(reason.clone());
                    }
                    steps.push(PlannedStep {
                        step: MigrationStep::AlterPropertyType {
                            type_name: target.display_type_name.to_string(),
                            type_id: target.type_id,
                            prop_name: new_p.name.clone(),
                            prop_id: new_p.prop_id,
                            old_type: old_p.scalar_type.clone(),
                            new_type: new_p.scalar_type.clone(),
                        },
                        safety,
                        reason,
                    });
                }
                if old_p.nullable != new_p.nullable {
                    let (safety, reason) = classify_nullability_change(
                        data_view,
                        target.storage_kind,
                        target.source_type_name,
                        target.display_type_name,
                        &old_p.name,
                        old_p.nullable,
                        new_p.nullable,
                    )
                    .await?;
                    if safety == MigrationSafety::Blocked {
                        blocked.push(reason.clone());
                    }
                    steps.push(PlannedStep {
                        step: MigrationStep::AlterPropertyNullability {
                            type_name: target.display_type_name.to_string(),
                            type_id: target.type_id,
                            prop_name: new_p.name.clone(),
                            prop_id: new_p.prop_id,
                            nullable: new_p.nullable,
                        },
                        safety,
                        reason,
                    });
                }
            }
        }
    }

    for old_p in old_props {
        if !new_by_id.contains_key(&old_p.prop_id) {
            steps.push(PlannedStep {
                step: MigrationStep::DropProperty {
                    type_name: target.display_type_name.to_string(),
                    type_id: target.type_id,
                    prop_name: old_p.name.clone(),
                    prop_id: old_p.prop_id,
                },
                safety: MigrationSafety::Confirm,
                reason: "property removed from desired schema".to_string(),
            });
        }
    }

    Ok(())
}

async fn classify_type_change(
    data_view: &mut MigrationDataView,
    storage_kind: PropertyStorageKind,
    source_type_name: &str,
    display_type_name: &str,
    old_prop_name: &str,
    target_scalar: &str,
) -> Result<(MigrationSafety, String)> {
    let target = ScalarType::from_str_name(target_scalar)
        .ok_or_else(|| NanoError::Catalog(format!("unknown scalar type: {}", target_scalar)))?
        .to_arrow();

    match get_property_column(data_view, storage_kind, source_type_name, old_prop_name).await? {
        None => Ok((
            MigrationSafety::Confirm,
            "type change requires data cast".to_string(),
        )),
        Some(col) => {
            if arrow_cast::cast(col.as_ref(), &target).is_ok() {
                Ok((
                    MigrationSafety::Confirm,
                    "type change requires data cast".to_string(),
                ))
            } else {
                Ok((
                    MigrationSafety::Blocked,
                    format!(
                        "cannot cast existing data for `{}`.`{}` to {}",
                        display_type_name, old_prop_name, target_scalar
                    ),
                ))
            }
        }
    }
}

async fn classify_nullability_change(
    data_view: &mut MigrationDataView,
    storage_kind: PropertyStorageKind,
    source_type_name: &str,
    display_type_name: &str,
    old_prop_name: &str,
    old_nullable: bool,
    new_nullable: bool,
) -> Result<(MigrationSafety, String)> {
    if old_nullable && !new_nullable {
        if let Some(col) =
            get_property_column(data_view, storage_kind, source_type_name, old_prop_name).await?
            && col.null_count() > 0
        {
            return Ok((
                MigrationSafety::Blocked,
                format!(
                    "cannot make `{}`.`{}` non-nullable: {} null value(s) exist",
                    display_type_name,
                    old_prop_name,
                    col.null_count()
                ),
            ));
        }
        return Ok((
            MigrationSafety::Confirm,
            "nullable -> non-nullable requires data check".to_string(),
        ));
    }

    Ok((
        MigrationSafety::Safe,
        if new_nullable {
            "non-nullable -> nullable".to_string()
        } else {
            "nullability changed".to_string()
        },
    ))
}

async fn classify_endpoint_rebind(
    data_view: &mut MigrationDataView,
    old_ir: &SchemaIR,
    old_edge: &EdgeTypeDef,
    new_edge: &EdgeTypeDef,
) -> (MigrationSafety, String) {
    let edge_batch = match data_view.edge_batch(&old_edge.name).await {
        Ok(batch) => batch,
        Err(err) => {
            return (
                MigrationSafety::Blocked,
                format!("failed to read edge batch for `{}`: {}", old_edge.name, err),
            );
        }
    };
    let Some(edge_batch) = edge_batch else {
        return (
            MigrationSafety::Safe,
            "edge endpoint rebind has no existing rows".to_string(),
        );
    };
    let Some(src_ids) = edge_batch
        .column_by_name("src")
        .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
    else {
        return (
            MigrationSafety::Blocked,
            format!(
                "edge batch for `{}` is missing UInt64 src column",
                old_edge.name
            ),
        );
    };
    let Some(dst_ids) = edge_batch
        .column_by_name("dst")
        .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
    else {
        return (
            MigrationSafety::Blocked,
            format!(
                "edge batch for `{}` is missing UInt64 dst column",
                old_edge.name
            ),
        );
    };

    let node_sets = match build_node_id_sets_by_type_id(old_ir, data_view).await {
        Ok(sets) => sets,
        Err(err) => {
            return (
                MigrationSafety::Blocked,
                format!("failed to read node ids for endpoint validation: {}", err),
            );
        }
    };
    let new_src = node_sets.get(&new_edge.src_type_id);
    let new_dst = node_sets.get(&new_edge.dst_type_id);
    if new_src.is_none() || new_dst.is_none() {
        return (
            MigrationSafety::Blocked,
            format!(
                "edge `{}` endpoint rebind targets unknown node type id(s)",
                old_edge.name
            ),
        );
    }
    let new_src = new_src.expect("checked above");
    let new_dst = new_dst.expect("checked above");

    let mut invalid = 0usize;
    for i in 0..edge_batch.num_rows() {
        if !new_src.contains(&src_ids.value(i)) || !new_dst.contains(&dst_ids.value(i)) {
            invalid += 1;
        }
    }

    if invalid > 0 {
        (
            MigrationSafety::Blocked,
            format!(
                "edge `{}` endpoint rebind would invalidate {} existing edge row(s)",
                old_edge.name, invalid
            ),
        )
    } else {
        (
            MigrationSafety::Confirm,
            "edge endpoint rebind requires data rewrite validation".to_string(),
        )
    }
}

async fn build_node_id_sets_by_type_id(
    old_ir: &SchemaIR,
    data_view: &mut MigrationDataView,
) -> Result<HashMap<u32, HashSet<u64>>> {
    let mut out = HashMap::new();
    for ty in &old_ir.types {
        let TypeDef::Node(n) = ty else {
            continue;
        };
        let mut set = HashSet::new();
        if let Some(batch) = data_view.node_batch(&n.name).await?
            && let Some(col) = batch.column_by_name("id")
            && let Some(ids) = col.as_any().downcast_ref::<UInt64Array>()
        {
            for i in 0..ids.len() {
                set.insert(ids.value(i));
            }
        }
        out.insert(n.type_id, set);
    }
    Ok(out)
}

async fn get_property_column(
    data_view: &mut MigrationDataView,
    storage_kind: PropertyStorageKind,
    type_name: &str,
    prop_name: &str,
) -> Result<Option<ArrayRef>> {
    let batch = match storage_kind {
        PropertyStorageKind::Node => data_view.node_batch(type_name).await?,
        PropertyStorageKind::Edge => data_view.edge_batch(type_name).await?,
    };
    Ok(batch.and_then(|batch| batch.column_by_name(prop_name).cloned()))
}

async fn apply_planned_migration(db_path: &Path, planned: &PlannedMigration) -> Result<()> {
    let lock = acquire_migration_lock(db_path)?;
    let names = migration_sidecar_paths(db_path)?;

    if names.journal_path.exists() {
        drop(lock);
        return Err(NanoError::Manifest(format!(
            "migration journal already exists at {}; resolve it before retrying",
            names.journal_path.display()
        )));
    }
    if names.backup_path.exists() {
        drop(lock);
        return Err(NanoError::Manifest(format!(
            "refusing to overwrite existing migration backup at {}",
            names.backup_path.display()
        )));
    }

    let mut journal = MigrationJournal {
        version: 1,
        state: JournalState::Prepared,
        db_path: db_path.display().to_string(),
        backup_path: names.backup_path.display().to_string(),
        staging_path: names.staging_path.display().to_string(),
        old_schema_hash: planned.plan.old_schema_hash.clone(),
        new_schema_hash: planned.plan.new_schema_hash.clone(),
        created_at_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };
    write_journal(&names.journal_path, &journal)?;

    let new_catalog = build_catalog_from_ir(&planned.new_ir)?;
    let mut data_view = MigrationDataView::open(db_path)?;
    let new_storage =
        transform_storage_for_new_schema(&mut data_view, &planned.new_ir, &new_catalog).await?;

    if names.staging_path.exists() {
        std::fs::remove_dir_all(&names.staging_path)?;
    }
    write_staged_db(
        &names.staging_path,
        &planned.schema_source,
        &planned.new_ir,
        &new_storage,
        &planned.manifest,
    )
    .await?;

    journal.state = JournalState::Applying;
    write_journal(&names.journal_path, &journal)?;

    std::fs::rename(db_path, &names.backup_path)?;
    if let Err(e) = std::fs::rename(&names.staging_path, db_path) {
        let _ = std::fs::rename(&names.backup_path, db_path);
        journal.state = JournalState::Aborted;
        let _ = write_journal(&names.journal_path, &journal);
        drop(lock);
        return Err(NanoError::Io(e));
    }

    journal.state = JournalState::Committed;
    write_journal(&names.journal_path, &journal)?;
    std::fs::remove_dir_all(&names.backup_path)?;
    std::fs::remove_file(&names.journal_path)?;

    drop(lock);
    Ok(())
}

struct SidecarPaths {
    journal_path: PathBuf,
    backup_path: PathBuf,
    staging_path: PathBuf,
}

struct BaseSidecarPaths {
    journal_path: PathBuf,
    backup_path: PathBuf,
    lock_path: PathBuf,
}

fn migration_base_paths(db_path: &Path) -> Result<BaseSidecarPaths> {
    let parent = db_path
        .parent()
        .ok_or_else(|| NanoError::Manifest(format!("invalid db path: {}", db_path.display())))?;
    let name = db_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| NanoError::Manifest("db path must be valid unicode".to_string()))?;

    Ok(BaseSidecarPaths {
        journal_path: parent.join(format!("{}.migration.journal.json", name)),
        backup_path: parent.join(format!("{}.migration.backup", name)),
        lock_path: parent.join(format!("{}.migration.lock", name)),
    })
}

fn migration_sidecar_paths(db_path: &Path) -> Result<SidecarPaths> {
    let base = migration_base_paths(db_path)?;
    let parent = db_path
        .parent()
        .ok_or_else(|| NanoError::Manifest(format!("invalid db path: {}", db_path.display())))?;
    let name = db_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| NanoError::Manifest("db path must be valid unicode".to_string()))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    Ok(SidecarPaths {
        journal_path: base.journal_path,
        backup_path: base.backup_path,
        staging_path: parent.join(format!("{}.migration.staging.{}", name, nonce)),
    })
}

struct LockGuard {
    path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn acquire_migration_lock(db_path: &Path) -> Result<LockGuard> {
    let path = migration_base_paths(db_path)?.lock_path;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|e| {
            NanoError::Manifest(format!(
                "migration lock already held or cannot be created at {}: {}",
                path.display(),
                e
            ))
        })?;
    Ok(LockGuard { path })
}

fn write_journal(path: &Path, journal: &MigrationJournal) -> Result<()> {
    let json = serde_json::to_string_pretty(journal)
        .map_err(|e| NanoError::Manifest(format!("serialize migration journal: {}", e)))?;
    std::fs::write(path, json)?;
    Ok(())
}

async fn transform_storage_for_new_schema(
    data_view: &mut MigrationDataView,
    new_ir: &SchemaIR,
    new_catalog: &Catalog,
) -> Result<DatasetAccumulator> {
    let mut out = DatasetAccumulator::new(new_catalog.clone());
    let old_ir = data_view.metadata().schema_ir().clone();

    let old_nodes_by_id = old_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Node(n) => Some((n.type_id, n)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let old_edges_by_id = old_ir
        .types
        .iter()
        .filter_map(|t| match t {
            TypeDef::Edge(e) => Some((e.type_id, e)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    for ty in &new_ir.types {
        let TypeDef::Node(new_node) = ty else {
            continue;
        };
        let Some(old_node) = old_nodes_by_id.get(&new_node.type_id) else {
            continue;
        };
        let Some(old_batch) = data_view.node_batch(&old_node.name).await? else {
            continue;
        };
        let old_props_by_id = old_node
            .properties
            .iter()
            .map(|p| (p.prop_id, p))
            .collect::<HashMap<_, _>>();

        let mut cols: Vec<ArrayRef> = Vec::new();
        cols.push(
            old_batch
                .column_by_name("id")
                .ok_or_else(|| NanoError::Storage("node batch missing id".to_string()))?
                .clone(),
        );
        for new_prop in &new_node.properties {
            let arr = if let Some(old_prop) = old_props_by_id.get(&new_prop.prop_id) {
                let old_col = old_batch.column_by_name(&old_prop.name).ok_or_else(|| {
                    NanoError::Storage(format!(
                        "node batch missing property `{}` for type `{}`",
                        old_prop.name, old_node.name
                    ))
                })?;
                cast_array_if_needed(old_col.clone(), new_prop)?
            } else {
                if !new_prop.nullable {
                    return Err(NanoError::Manifest(format!(
                        "cannot apply migration: new non-nullable node property `{}` on `{}` has no value source",
                        new_prop.name, new_node.name
                    )));
                }
                let dt = propdef_to_arrow(new_prop)?;
                new_null_array(&dt, old_batch.num_rows())
            };
            if !new_prop.nullable && arr.null_count() > 0 {
                return Err(NanoError::Manifest(format!(
                    "cannot apply migration: `{}`.`{}` contains nulls but target is non-nullable",
                    new_node.name, new_prop.name
                )));
            }
            cols.push(arr);
        }

        let schema = new_catalog
            .node_types
            .get(&new_node.name)
            .ok_or_else(|| {
                NanoError::Storage(format!("new catalog missing node type `{}`", new_node.name))
            })?
            .arrow_schema
            .clone();
        let batch = RecordBatch::try_new(schema, cols)
            .map_err(|e| NanoError::Storage(format!("migrated node batch error: {}", e)))?;
        out.load_node_batch(&new_node.name, batch)?;
    }

    for ty in &new_ir.types {
        let TypeDef::Edge(new_edge) = ty else {
            continue;
        };
        let Some(old_edge) = old_edges_by_id.get(&new_edge.type_id) else {
            continue;
        };
        let Some(old_batch) = data_view.edge_batch(&old_edge.name).await? else {
            continue;
        };
        let old_props_by_id = old_edge
            .properties
            .iter()
            .map(|p| (p.prop_id, p))
            .collect::<HashMap<_, _>>();

        let mut cols: Vec<ArrayRef> = Vec::new();
        let id_col = old_batch
            .column_by_name("id")
            .ok_or_else(|| NanoError::Storage("edge batch missing id".to_string()))?
            .clone();
        let src_col = old_batch
            .column_by_name("src")
            .ok_or_else(|| NanoError::Storage("edge batch missing src".to_string()))?
            .clone();
        let dst_col = old_batch
            .column_by_name("dst")
            .ok_or_else(|| NanoError::Storage("edge batch missing dst".to_string()))?
            .clone();

        validate_edge_endpoints_during_apply(
            &out,
            new_ir,
            new_edge.src_type_id,
            new_edge.dst_type_id,
            src_col.as_ref(),
            dst_col.as_ref(),
            &new_edge.name,
        )?;

        cols.push(id_col);
        cols.push(src_col);
        cols.push(dst_col);

        for new_prop in &new_edge.properties {
            let arr = if let Some(old_prop) = old_props_by_id.get(&new_prop.prop_id) {
                let old_col = old_batch.column_by_name(&old_prop.name).ok_or_else(|| {
                    NanoError::Storage(format!(
                        "edge batch missing property `{}` for type `{}`",
                        old_prop.name, old_edge.name
                    ))
                })?;
                cast_array_if_needed(old_col.clone(), new_prop)?
            } else {
                if !new_prop.nullable {
                    return Err(NanoError::Manifest(format!(
                        "cannot apply migration: new non-nullable edge property `{}` on `{}` has no value source",
                        new_prop.name, new_edge.name
                    )));
                }
                let dt = propdef_to_arrow(new_prop)?;
                new_null_array(&dt, old_batch.num_rows())
            };
            if !new_prop.nullable && arr.null_count() > 0 {
                return Err(NanoError::Manifest(format!(
                    "cannot apply migration: `{}`.`{}` contains nulls but target is non-nullable",
                    new_edge.name, new_prop.name
                )));
            }
            cols.push(arr);
        }

        let schema = out
            .edge_segments
            .get(&new_edge.name)
            .ok_or_else(|| {
                NanoError::Storage(format!(
                    "new storage missing edge segment `{}`",
                    new_edge.name
                ))
            })?
            .schema
            .clone();

        let batch = RecordBatch::try_new(schema, cols)
            .map_err(|e| NanoError::Storage(format!("migrated edge batch error: {}", e)))?;
        out.load_edge_batch(&new_edge.name, batch)?;
    }

    out.build_indices()?;
    Ok(out)
}

fn validate_edge_endpoints_during_apply(
    storage: &DatasetAccumulator,
    ir: &SchemaIR,
    src_type_id: u32,
    dst_type_id: u32,
    src_col: &dyn arrow_array::Array,
    dst_col: &dyn arrow_array::Array,
    edge_name: &str,
) -> Result<()> {
    let src_name = ir.type_name(src_type_id).ok_or_else(|| {
        NanoError::Manifest(format!(
            "edge `{}` source type id {} not found in target schema",
            edge_name, src_type_id
        ))
    })?;
    let dst_name = ir.type_name(dst_type_id).ok_or_else(|| {
        NanoError::Manifest(format!(
            "edge `{}` destination type id {} not found in target schema",
            edge_name, dst_type_id
        ))
    })?;
    let src_ids = src_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("edge src column must be UInt64".to_string()))?;
    let dst_ids = dst_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| NanoError::Storage("edge dst column must be UInt64".to_string()))?;

    let src_set = collect_node_ids(storage, src_name)?;
    let dst_set = collect_node_ids(storage, dst_name)?;

    for i in 0..src_ids.len() {
        let s = src_ids.value(i);
        let d = dst_ids.value(i);
        if !src_set.contains(&s) || !dst_set.contains(&d) {
            return Err(NanoError::Manifest(format!(
                "edge `{}` endpoint rebind invalidates row {} (src={}, dst={})",
                edge_name, i, s, d
            )));
        }
    }
    Ok(())
}

fn collect_node_ids(storage: &DatasetAccumulator, type_name: &str) -> Result<HashSet<u64>> {
    let mut set = HashSet::new();
    if let Some(batch) = storage.get_all_nodes(type_name)? {
        let ids = batch
            .column_by_name("id")
            .ok_or_else(|| NanoError::Storage("missing id column".to_string()))?
            .as_primitive::<UInt64Type>();
        for i in 0..ids.len() {
            set.insert(ids.value(i));
        }
    }
    Ok(set)
}

async fn read_dataset_batch(locator: Option<DatasetLocator>) -> Result<Option<RecordBatch>> {
    let Some(locator) = locator else {
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
        .map_err(|e| NanoError::Storage(format!("concat migration batch error: {}", e)))?;
    Ok(Some(batch))
}

fn propdef_to_prop_type(prop: &PropDef) -> Result<crate::types::PropType> {
    let scalar = ScalarType::from_str_name(&prop.scalar_type)
        .ok_or_else(|| NanoError::Catalog(format!("unknown scalar type: {}", prop.scalar_type)))?;
    Ok(crate::types::PropType {
        scalar,
        nullable: prop.nullable,
        list: prop.list,
        enum_values: if prop.enum_values.is_empty() {
            None
        } else {
            Some(prop.enum_values.clone())
        },
    })
}

fn propdef_to_arrow(prop: &PropDef) -> Result<arrow_schema::DataType> {
    Ok(propdef_to_prop_type(prop)?.to_arrow())
}

fn cast_array_if_needed(col: ArrayRef, target_prop: &PropDef) -> Result<ArrayRef> {
    let target = propdef_to_arrow(target_prop)?;
    if col.data_type() == &target {
        Ok(col)
    } else {
        arrow_cast::cast(col.as_ref(), &target)
            .map_err(|e| NanoError::Execution(format!("cast error: {}", e)))
    }
}

async fn write_staged_db(
    path: &Path,
    schema_source: &str,
    schema_ir: &SchemaIR,
    storage: &DatasetAccumulator,
    manifest_seed: &GraphManifest,
) -> Result<()> {
    std::fs::create_dir_all(path)?;
    std::fs::create_dir_all(path.join("nodes"))?;
    std::fs::create_dir_all(path.join("edges"))?;
    std::fs::write(path.join(SCHEMA_PG_FILENAME), schema_source)?;

    let ir_json = serde_json::to_string_pretty(schema_ir)
        .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
    std::fs::write(path.join(SCHEMA_IR_FILENAME), &ir_json)?;

    let mut dataset_entries = Vec::new();
    for node_def in schema_ir.node_types() {
        if let Some(batch) = storage.get_all_nodes(&node_def.name)? {
            let row_count = batch.num_rows() as u64;
            let dataset_rel_path = format!("nodes/{}", SchemaIR::dir_name(node_def.type_id));
            let dataset_path = path.join(&dataset_rel_path);
            let dataset_version = write_lance_batch(&dataset_path, batch).await?;
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
    }
    for edge_def in schema_ir.edge_types() {
        if let Some(batch) = storage.edge_batch_for_save(&edge_def.name)? {
            let row_count = batch.num_rows() as u64;
            let dataset_rel_path = format!("edges/{}", SchemaIR::dir_name(edge_def.type_id));
            let dataset_path = path.join(&dataset_rel_path);
            let dataset_version = write_lance_batch(&dataset_path, batch).await?;
            dataset_entries.push(DatasetEntry {
                type_id: edge_def.type_id,
                type_name: edge_def.name.clone(),
                kind: "edge".to_string(),
                dataset_path: dataset_rel_path,
                dataset_version,
                row_count,
            });
        }
    }

    let mut manifest = GraphManifest::new(hash_string(&ir_json));
    manifest.db_version = manifest_seed.db_version.saturating_add(1);
    manifest.last_tx_id = format!("migration-{}", manifest.db_version);
    manifest.committed_at = now_unix_seconds_string();
    manifest.next_node_id = storage.next_node_id();
    manifest.next_edge_id = storage.next_edge_id();
    manifest.next_type_id = manifest_seed.next_type_id;
    manifest.next_prop_id = manifest_seed.next_prop_id;
    manifest.schema_identity_version = manifest_seed
        .schema_identity_version
        .saturating_add(1)
        .max(1);
    manifest.datasets = dataset_entries;

    commit_manifest_and_logs(path, &manifest, &[], "schema_migration")?;
    Ok(())
}

fn now_unix_seconds_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn bootstrap_identity_counters(ir: &SchemaIR, manifest: &mut GraphManifest) {
    if manifest.next_type_id > 0 && manifest.next_prop_id > 0 {
        return;
    }
    let mut max_type_id = 0u32;
    let mut max_prop_id = 0u32;
    for ty in &ir.types {
        match ty {
            TypeDef::Node(n) => {
                max_type_id = max_type_id.max(n.type_id);
                for p in &n.properties {
                    max_prop_id = max_prop_id.max(p.prop_id);
                }
            }
            TypeDef::Edge(e) => {
                max_type_id = max_type_id.max(e.type_id);
                for p in &e.properties {
                    max_prop_id = max_prop_id.max(p.prop_id);
                }
            }
        }
    }
    if manifest.next_type_id == 0 {
        manifest.next_type_id = max_type_id.saturating_add(1).max(1);
    }
    if manifest.next_prop_id == 0 {
        manifest.next_prop_id = max_prop_id.saturating_add(1).max(1);
    }
    manifest.schema_identity_version = manifest.schema_identity_version.max(1);
}
