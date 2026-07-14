use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::schema::ast::{Annotation, Constraint};
use crate::types::PropType;

use super::schema_ir::{
    ConstraintIR, EdgeIR, InterfaceIR, NodeIR, PropertyIR, SchemaIR, StablePropertyId,
    StableTypeId, TableIncarnationId, constraint_from_ir, validate_schema_ir,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaTypeKind {
    Interface,
    Node,
    Edge,
}

/// How a drop step interacts with data.
///
/// - **`Soft`** — catalog tombstone only. The type / property is hidden
///   from queries but the underlying Lance column / dataset is retained
///   on disk. Reversible via `omnigraph schema unhide` (forthcoming).
///   Tier: `safe`.
/// - **`Hard`** — actual data removal. The Lance column is rewritten
///   without the property, or the Lance dataset is dropped. Irreversible
///   short of branch / snapshot restore. Tier: `destructive`; requires
///   `--allow-data-loss` to apply.
///
/// The planner emits `Soft` by default; `--allow-data-loss` on the apply
/// CLI promotes drops to `Hard`. This is the dimension orthogonal to
/// `SafetyTier` from the schema-lint chassis (`crate::lint`): tier
/// describes the rule's class; mode describes the operator's intent for
/// data treatment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropMode {
    Soft,
    Hard,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaMigrationPlan {
    pub supported: bool,
    pub steps: Vec<SchemaMigrationStep>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaMigrationStep {
    AddType {
        type_kind: SchemaTypeKind,
        name: String,
    },
    RenameType {
        type_kind: SchemaTypeKind,
        from: String,
        to: String,
    },
    AddProperty {
        type_kind: SchemaTypeKind,
        type_name: String,
        property_name: String,
        property_type: PropType,
    },
    RenameProperty {
        type_kind: SchemaTypeKind,
        type_name: String,
        from: String,
        to: String,
    },
    AddConstraint {
        type_kind: SchemaTypeKind,
        type_name: String,
        constraint: Constraint,
    },
    /// Widen an enum property's value set. Emitted only for a PURE widening —
    /// and `property_name` is the DESIRED (post-rename) name: when a property
    /// is renamed and widened in one migration, the plan emits
    /// `RenameProperty` first and this step names the new property, so a
    /// sequential step consumer resolves it after the rename.
    ///
    /// same scalar/list shape, same nullability, and the desired value set is
    /// a superset of the accepted one (order-insensitive; enum semantics are
    /// set membership, not position). Metadata-only at apply time: every
    /// committed row is valid under a superset, so no table data is touched
    /// and the unified validator accepts the new variants on all three write
    /// surfaces the moment the accepted catalog updates. Narrowing, renames,
    /// and enum<->free-String conversions still plan as `UnsupportedChange`
    /// (OG-MF-106).
    ExtendEnum {
        type_kind: SchemaTypeKind,
        type_name: String,
        property_name: String,
        /// The variants the desired schema adds (accepted-set order preserved
        /// for the untouched prefix; display/debug aid).
        added_values: Vec<String>,
    },
    UpdateTypeMetadata {
        type_kind: SchemaTypeKind,
        name: String,
        annotations: Vec<Annotation>,
    },
    UpdatePropertyMetadata {
        type_kind: SchemaTypeKind,
        type_name: String,
        property_name: String,
        annotations: Vec<Annotation>,
    },
    /// Remove a node or edge type. Soft mode tombstones in the catalog
    /// and retains data on disk; Hard mode drops the Lance dataset and
    /// requires `--allow-data-loss`.
    ///
    /// Dormant in this commit — emitted by the planner in a later
    /// commit (see `docs/schema-lint-v1-plan.md`).
    DropType {
        type_kind: SchemaTypeKind,
        name: String,
        mode: DropMode,
    },
    /// Remove a property from an existing type. Soft mode tombstones
    /// the property in the catalog and retains the Lance column; Hard
    /// mode rewrites the column out and requires `--allow-data-loss`.
    ///
    /// Dormant in this commit.
    DropProperty {
        type_kind: SchemaTypeKind,
        type_name: String,
        property_name: String,
        mode: DropMode,
    },
    UnsupportedChange {
        entity: String,
        reason: String,
        /// Stable schema-lint code (`OG-XXX-NNN`) for this rejection,
        /// or `None` if the path predates the chassis catalog. See
        /// [`crate::lint::codes`] for the registry. Renderers should
        /// prefix the message with `[code]` when present so operators
        /// can suppress, look up docs, or filter on stable identifiers
        /// rather than free-text prose.
        ///
        /// Stored as `String` (not `&'static str`) so the enum stays
        /// serde-friendly. Emitters pass the catalog constant's
        /// `.code` (a `&'static str`) and we own a clone here.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<String>,
    },
}

impl SchemaMigrationStep {
    /// Returns the formatted error message for an `UnsupportedChange`
    /// step, prefixed with `[code] ` when a schema-lint code is attached.
    /// Returns `None` for every other variant.
    pub fn unsupported_error_message(&self) -> Option<String> {
        match self {
            Self::UnsupportedChange { reason, code, .. } => Some(match code {
                Some(c) => format!("[{}] {}", c, reason),
                None => reason.clone(),
            }),
            _ => None,
        }
    }

    /// If this step carries a schema-lint code, return the full
    /// catalog entry — including family, safety tier, and default
    /// severity. Used by renderers that want to display richer
    /// context than just the code string (e.g. `omnigraph schema
    /// plan` annotating each line with its tier).
    ///
    /// Returns `None` for steps that carry no code (the 12 of 17
    /// `UnsupportedChange` paths still untagged in v0, plus every
    /// non-`UnsupportedChange` variant).
    pub fn diagnostic(&self) -> Option<&'static crate::lint::DiagnosticCode> {
        match self {
            Self::UnsupportedChange { code: Some(c), .. } => crate::lint::lookup(c),
            _ => None,
        }
    }
}

pub fn plan_schema_migration(
    accepted: &SchemaIR,
    desired: &SchemaIR,
) -> Result<SchemaMigrationPlan> {
    validate_schema_ir(accepted)?;
    validate_schema_ir(desired)?;
    if accepted.schema_identity_domain != desired.schema_identity_domain {
        return Err(crate::error::SchemaIdentityError::Resolution(
            "migration planning requires accepted and desired IR in the same identity domain"
                .to_string(),
        )
        .into());
    }
    validate_evolution_identity(accepted, desired)?;
    let mut steps = Vec::new();
    plan_interfaces(&accepted.interfaces, &desired.interfaces, &mut steps);
    plan_nodes(&accepted.nodes, &desired.nodes, &mut steps);
    plan_edges(&accepted.edges, &desired.edges, &mut steps);

    Ok(SchemaMigrationPlan {
        supported: !steps
            .iter()
            .any(|step| matches!(step, SchemaMigrationStep::UnsupportedChange { .. })),
        steps,
    })
}

fn validate_evolution_identity(accepted: &SchemaIR, desired: &SchemaIR) -> Result<()> {
    use crate::error::SchemaIdentityError;

    if desired.next_identity_id < accepted.next_identity_id {
        return Err(SchemaIdentityError::Resolution(format!(
            "desired next_identity_id {} regresses accepted high-water mark {}",
            desired.next_identity_id, accepted.next_identity_id
        ))
        .into());
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Kind {
        Interface,
        Node,
        Edge,
    }
    let accepted_types = accepted
        .interfaces
        .iter()
        .map(|entry| (entry.type_id, (Kind::Interface, None)))
        .chain(accepted.nodes.iter().map(|entry| {
            (
                entry.type_id,
                (Kind::Node, Some(entry.table_incarnation_id)),
            )
        }))
        .chain(accepted.edges.iter().map(|entry| {
            (
                entry.type_id,
                (Kind::Edge, Some(entry.table_incarnation_id)),
            )
        }))
        .collect::<HashMap<StableTypeId, (Kind, Option<TableIncarnationId>)>>();
    let desired_types = desired
        .interfaces
        .iter()
        .map(|entry| (entry.type_id, (Kind::Interface, None)))
        .chain(desired.nodes.iter().map(|entry| {
            (
                entry.type_id,
                (Kind::Node, Some(entry.table_incarnation_id)),
            )
        }))
        .chain(desired.edges.iter().map(|entry| {
            (
                entry.type_id,
                (Kind::Edge, Some(entry.table_incarnation_id)),
            )
        }));
    for (type_id, (kind, incarnation)) in desired_types {
        if let Some((accepted_kind, accepted_incarnation)) = accepted_types.get(&type_id) {
            if *accepted_kind != kind || *accepted_incarnation != incarnation {
                return Err(SchemaIdentityError::Resolution(format!(
                    "stable type id {type_id} changes declaration kind or table incarnation"
                ))
                .into());
            }
        } else if type_id.get() < accepted.next_identity_id {
            return Err(SchemaIdentityError::Resolution(format!(
                "new stable type id {type_id} reuses retired allocator space"
            ))
            .into());
        }
        if let Some(incarnation) = incarnation
            && !accepted_types
                .get(&type_id)
                .is_some_and(|(_, previous)| *previous == Some(incarnation))
            && incarnation.get() < accepted.next_identity_id
        {
            return Err(SchemaIdentityError::Resolution(format!(
                "new table incarnation id {incarnation} reuses retired allocator space"
            ))
            .into());
        }
    }

    let accepted_properties = accepted
        .interfaces
        .iter()
        .map(|entry| (entry.type_id, entry.properties.as_slice()))
        .chain(
            accepted
                .nodes
                .iter()
                .map(|entry| (entry.type_id, entry.properties.as_slice())),
        )
        .chain(
            accepted
                .edges
                .iter()
                .map(|entry| (entry.type_id, entry.properties.as_slice())),
        )
        .flat_map(|(owner, properties)| {
            properties
                .iter()
                .map(move |property| (property.property_id, owner))
        })
        .collect::<HashMap<StablePropertyId, StableTypeId>>();
    let desired_properties = desired
        .interfaces
        .iter()
        .map(|entry| (entry.type_id, entry.properties.as_slice()))
        .chain(
            desired
                .nodes
                .iter()
                .map(|entry| (entry.type_id, entry.properties.as_slice())),
        )
        .chain(
            desired
                .edges
                .iter()
                .map(|entry| (entry.type_id, entry.properties.as_slice())),
        );
    for (owner, properties) in desired_properties {
        for property in properties {
            match accepted_properties.get(&property.property_id) {
                Some(accepted_owner) if *accepted_owner != owner => {
                    return Err(SchemaIdentityError::Resolution(format!(
                        "stable property id {} moves across owners",
                        property.property_id
                    ))
                    .into());
                }
                None if property.property_id.get() < accepted.next_identity_id => {
                    return Err(SchemaIdentityError::Resolution(format!(
                        "new stable property id {} reuses retired allocator space",
                        property.property_id
                    ))
                    .into());
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn plan_interfaces(
    accepted: &[InterfaceIR],
    desired: &[InterfaceIR],
    steps: &mut Vec<SchemaMigrationStep>,
) {
    let accepted_by_id = accepted
        .iter()
        .map(|interface| (interface.type_id, interface))
        .collect::<HashMap<_, _>>();
    let mut consumed = HashSet::new();

    for interface in desired {
        if let Some(existing) = accepted_by_id.get(&interface.type_id) {
            consumed.insert(existing.type_id);
            if existing.name != interface.name {
                steps.push(SchemaMigrationStep::UnsupportedChange {
                    entity: format!("interface:{}", interface.name),
                    reason: "renaming interfaces is not supported in schema migration v1"
                        .to_string(),
                    code: None,
                });
            }
            plan_properties(
                SchemaTypeKind::Interface,
                &interface.name,
                &existing.properties,
                &interface.properties,
                steps,
            );
            continue;
        }

        steps.push(SchemaMigrationStep::AddType {
            type_kind: SchemaTypeKind::Interface,
            name: interface.name.clone(),
        });
    }

    for leftover in accepted
        .iter()
        .filter(|interface| !consumed.contains(&interface.type_id))
    {
        steps.push(SchemaMigrationStep::UnsupportedChange {
            entity: format!("interface:{}", leftover.name),
            reason: format!(
                "removing interface '{}' is not supported in schema migration v1",
                leftover.name
            ),
            code: None,
        });
    }
}

fn plan_nodes(accepted: &[NodeIR], desired: &[NodeIR], steps: &mut Vec<SchemaMigrationStep>) {
    let accepted_by_id = accepted
        .iter()
        .map(|node| (node.type_id, node))
        .collect::<HashMap<_, _>>();
    let mut consumed = HashSet::new();

    for node in desired {
        let Some(existing) = accepted_by_id.get(&node.type_id).copied() else {
            steps.push(SchemaMigrationStep::AddType {
                type_kind: SchemaTypeKind::Node,
                name: node.name.clone(),
            });
            continue;
        };

        consumed.insert(existing.type_id);
        if existing.name != node.name {
            steps.push(SchemaMigrationStep::RenameType {
                type_kind: SchemaTypeKind::Node,
                from: existing.name.clone(),
                to: node.name.clone(),
            });
        }

        let accepted_implements = existing
            .implements
            .iter()
            .map(|reference| reference.type_id)
            .collect::<BTreeSet<_>>();
        let desired_implements = node
            .implements
            .iter()
            .map(|reference| reference.type_id)
            .collect::<BTreeSet<_>>();
        if accepted_implements != desired_implements {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("node:{}", node.name),
                reason: format!(
                    "changing implemented interfaces on node '{}' is not supported in schema migration v1",
                    node.name
                ),
                code: None,
            });
        }

        plan_type_metadata(
            SchemaTypeKind::Node,
            &node.name,
            &existing.annotations,
            &node.annotations,
            steps,
        );
        plan_properties(
            SchemaTypeKind::Node,
            &node.name,
            &existing.properties,
            &node.properties,
            steps,
        );
        plan_constraints(
            SchemaTypeKind::Node,
            &node.name,
            &existing.constraints,
            &node.constraints,
            steps,
        );
    }

    for leftover in accepted
        .iter()
        .filter(|node| !consumed.contains(&node.type_id))
    {
        // Node type removed from the desired schema: emit
        // DropType { Node, Soft } per docs/dev/schema-lint-v1-plan.md
        // commit #4. Soft = remove the table's entry from the current
        // __manifest version; data files retained; previous manifest
        // versions still reference the table, so Lance time travel
        // restores it until cleanup_old_versions ages out the older
        // __manifest entries. Hard mode (immediate dataset deletion)
        // lands in commit #5 gated by --allow-data-loss.
        steps.push(SchemaMigrationStep::DropType {
            type_kind: SchemaTypeKind::Node,
            name: leftover.name.clone(),
            mode: DropMode::Soft,
        });
    }
}

fn plan_edges(accepted: &[EdgeIR], desired: &[EdgeIR], steps: &mut Vec<SchemaMigrationStep>) {
    let accepted_by_id = accepted
        .iter()
        .map(|edge| (edge.type_id, edge))
        .collect::<HashMap<_, _>>();
    let mut consumed = HashSet::new();

    for edge in desired {
        let Some(existing) = accepted_by_id.get(&edge.type_id).copied() else {
            steps.push(SchemaMigrationStep::AddType {
                type_kind: SchemaTypeKind::Edge,
                name: edge.name.clone(),
            });
            continue;
        };

        consumed.insert(existing.type_id);
        if existing.name != edge.name {
            steps.push(SchemaMigrationStep::RenameType {
                type_kind: SchemaTypeKind::Edge,
                from: existing.name.clone(),
                to: edge.name.clone(),
            });
        }

        if existing.from_type.type_id != edge.from_type.type_id
            || existing.to_type.type_id != edge.to_type.type_id
        {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("edge:{}", edge.name),
                reason: format!(
                    "changing edge endpoints on '{}' is not supported in schema migration v1",
                    edge.name
                ),
                code: None,
            });
        }
        if existing.cardinality != edge.cardinality {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("edge:{}", edge.name),
                reason: format!(
                    "changing cardinality on edge '{}' is not supported in schema migration v1",
                    edge.name
                ),
                code: None,
            });
        }

        plan_type_metadata(
            SchemaTypeKind::Edge,
            &edge.name,
            &existing.annotations,
            &edge.annotations,
            steps,
        );
        plan_properties(
            SchemaTypeKind::Edge,
            &edge.name,
            &existing.properties,
            &edge.properties,
            steps,
        );
        plan_constraints(
            SchemaTypeKind::Edge,
            &edge.name,
            &existing.constraints,
            &edge.constraints,
            steps,
        );
    }

    for leftover in accepted
        .iter()
        .filter(|edge| !consumed.contains(&edge.type_id))
    {
        // Edge type removed from the desired schema: emit
        // DropType { Edge, Soft } per docs/dev/schema-lint-v1-plan.md
        // commit #4. Same Soft mechanics as node-type drops — manifest
        // entry tombstoned, data files retained, reversible via Lance
        // time travel until cleanup.
        steps.push(SchemaMigrationStep::DropType {
            type_kind: SchemaTypeKind::Edge,
            name: leftover.name.clone(),
            mode: DropMode::Soft,
        });
    }
}

fn plan_properties(
    type_kind: SchemaTypeKind,
    type_name: &str,
    accepted: &[PropertyIR],
    desired: &[PropertyIR],
    steps: &mut Vec<SchemaMigrationStep>,
) {
    let accepted_by_id = accepted
        .iter()
        .map(|property| (property.property_id, property))
        .collect::<HashMap<_, _>>();
    let mut consumed = HashSet::new();

    for property in desired {
        let Some(existing) = accepted_by_id.get(&property.property_id).copied() else {
            if property.prop_type.nullable {
                steps.push(SchemaMigrationStep::AddProperty {
                    type_kind,
                    type_name: type_name.to_string(),
                    property_name: property.name.clone(),
                    property_type: property.prop_type.clone(),
                });
            } else {
                steps.push(SchemaMigrationStep::UnsupportedChange {
                    entity: format!(
                        "{}:{}.{}",
                        schema_type_kind_key(type_kind),
                        type_name,
                        property.name
                    ),
                    reason: format!(
                        "adding required property '{}.{}' requires a backfill and is not supported in schema migration v1",
                        type_name, property.name
                    ),
                    code: Some(crate::lint::codes::OG_MF_103.code.to_string()),
                });
            }
            continue;
        };

        consumed.insert(existing.property_id);
        if existing.name != property.name {
            steps.push(SchemaMigrationStep::RenameProperty {
                type_kind,
                type_name: type_name.to_string(),
                from: existing.name.clone(),
                to: property.name.clone(),
            });
        }

        if let Some(added_values) = enum_widening(&existing.prop_type, &property.prop_type) {
            steps.push(SchemaMigrationStep::ExtendEnum {
                type_kind,
                type_name: type_name.to_string(),
                property_name: property.name.clone(),
                added_values,
            });
        } else if existing.prop_type != property.prop_type {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!(
                    "{}:{}.{}",
                    schema_type_kind_key(type_kind),
                    type_name,
                    property.name
                ),
                reason: format!(
                    "changing property type for '{}.{}' is not supported in schema migration v1",
                    type_name, property.name
                ),
                code: Some(crate::lint::codes::OG_MF_106.code.to_string()),
            });
        }

        plan_property_metadata(
            type_kind,
            type_name,
            &property.name,
            &existing.annotations,
            &property.annotations,
            steps,
        );
    }

    for leftover in accepted
        .iter()
        .filter(|property| !consumed.contains(&property.property_id))
    {
        // Property removed from the desired schema: emit
        // DropProperty { Soft } per docs/schema-lint-v1-plan.md
        // commit #3. The Soft mode reuses the existing
        // stage_overwrite rewrite path — batch_for_schema_apply_rewrite
        // iterates target_schema.fields(), so the dropped column is
        // naturally projected away. The prior Lance version retains
        // the column until cleanup_old_versions runs, matching the
        // OG-DS-104 destructive-tier expectation that data remains
        // recoverable via time travel until cleanup. Hard mode (with
        // immediate compact_files + cleanup_old_versions) lands in
        // commit #5, gated by --allow-data-loss.
        steps.push(SchemaMigrationStep::DropProperty {
            type_kind,
            type_name: type_name.to_string(),
            property_name: leftover.name.clone(),
            mode: DropMode::Soft,
        });
    }
}

fn plan_constraints(
    type_kind: SchemaTypeKind,
    type_name: &str,
    accepted: &[ConstraintIR],
    desired: &[ConstraintIR],
    steps: &mut Vec<SchemaMigrationStep>,
) {
    let desired_map = desired
        .iter()
        .cloned()
        .map(|constraint| (constraint_ir_key(&constraint), constraint))
        .collect::<BTreeMap<_, _>>();
    let accepted_map = accepted
        .iter()
        .cloned()
        .map(|constraint| (constraint_ir_key(&constraint), constraint))
        .collect::<BTreeMap<_, _>>();

    let removed = accepted_map
        .keys()
        .filter(|key| !desired_map.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    if !removed.is_empty() {
        steps.push(SchemaMigrationStep::UnsupportedChange {
            entity: format!("{}:{}", schema_type_kind_key(type_kind), type_name),
            reason: format!(
                "removing constraints from '{}' is not supported in schema migration v1",
                type_name
            ),
            code: None,
        });
    }

    for (key, constraint) in desired_map {
        if accepted_map.contains_key(&key) {
            continue;
        }
        match &constraint {
            ConstraintIR::Index { .. } => steps.push(SchemaMigrationStep::AddConstraint {
                type_kind,
                type_name: type_name.to_string(),
                constraint: constraint_from_ir(&constraint),
            }),
            _ => steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("{}:{}", schema_type_kind_key(type_kind), type_name),
                reason: format!(
                    "adding constraint '{}' to '{}' is not supported in schema migration v1",
                    key, type_name
                ),
                code: None,
            }),
        }
    }
}

fn plan_type_metadata(
    type_kind: SchemaTypeKind,
    name: &str,
    accepted: &[Annotation],
    desired: &[Annotation],
    steps: &mut Vec<SchemaMigrationStep>,
) {
    match annotation_change_kind(accepted, desired) {
        AnnotationChangeKind::None => {}
        AnnotationChangeKind::MetadataOnly(metadata) => {
            steps.push(SchemaMigrationStep::UpdateTypeMetadata {
                type_kind,
                name: name.to_string(),
                annotations: metadata,
            });
        }
        AnnotationChangeKind::Unsupported(reason) => {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("{}:{}", schema_type_kind_key(type_kind), name),
                reason,
                code: None,
            });
        }
    }
}

fn plan_property_metadata(
    type_kind: SchemaTypeKind,
    type_name: &str,
    property_name: &str,
    accepted: &[Annotation],
    desired: &[Annotation],
    steps: &mut Vec<SchemaMigrationStep>,
) {
    match annotation_change_kind(accepted, desired) {
        AnnotationChangeKind::None => {}
        AnnotationChangeKind::MetadataOnly(metadata) => {
            steps.push(SchemaMigrationStep::UpdatePropertyMetadata {
                type_kind,
                type_name: type_name.to_string(),
                property_name: property_name.to_string(),
                annotations: metadata,
            });
        }
        AnnotationChangeKind::Unsupported(reason) => {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!(
                    "{}:{}.{}",
                    schema_type_kind_key(type_kind),
                    type_name,
                    property_name
                ),
                reason,
                code: None,
            });
        }
    }
}

/// `Some(added)` when `desired` is a PURE enum widening of `accepted`: both
/// are enums of the same scalar/list shape and nullability, every accepted
/// value is retained, and at least one value is new. Enum values arrive
/// sorted + deduped from the schema IR (`normalize` in schema_ir.rs), so a
/// bare reorder is already type equality (no step), and a returned `added`
/// is never empty. Everything else (narrowing, renamed values, enum<->plain
/// conversions, shape changes) returns `None` and falls through to
/// OG-MF-106.
fn enum_widening(accepted: &PropType, desired: &PropType) -> Option<Vec<String>> {
    let accepted_values = accepted.enum_values.as_ref()?;
    let desired_values = desired.enum_values.as_ref()?;
    if accepted.scalar != desired.scalar
        || accepted.nullable != desired.nullable
        || accepted.list != desired.list
    {
        return None;
    }
    if accepted_values == desired_values {
        // Identical type — not a change at all; let the equality check pass.
        return None;
    }
    let desired_set: std::collections::HashSet<&str> =
        desired_values.iter().map(String::as_str).collect();
    if !accepted_values
        .iter()
        .all(|v| desired_set.contains(v.as_str()))
    {
        return None; // narrowing or rename
    }
    let accepted_set: std::collections::HashSet<&str> =
        accepted_values.iter().map(String::as_str).collect();
    Some(
        desired_values
            .iter()
            .filter(|v| !accepted_set.contains(v.as_str()))
            .cloned()
            .collect(),
    )
}

enum AnnotationChangeKind {
    None,
    MetadataOnly(Vec<Annotation>),
    Unsupported(String),
}

fn annotation_change_kind(accepted: &[Annotation], desired: &[Annotation]) -> AnnotationChangeKind {
    let accepted_non_metadata = strip_metadata_annotations(accepted);
    let desired_non_metadata = strip_metadata_annotations(desired);
    if accepted_non_metadata != desired_non_metadata {
        return AnnotationChangeKind::Unsupported(
            "changing annotations beyond @description/@instruction is not supported in schema migration v1"
                .to_string(),
        );
    }

    let accepted_metadata = metadata_annotations(accepted);
    let desired_metadata = metadata_annotations(desired);
    if accepted_metadata == desired_metadata {
        AnnotationChangeKind::None
    } else {
        AnnotationChangeKind::MetadataOnly(desired_metadata)
    }
}

fn strip_metadata_annotations(annotations: &[Annotation]) -> Vec<Annotation> {
    annotations
        .iter()
        .filter(|annotation| {
            !matches!(
                annotation.name.as_str(),
                "description" | "instruction" | "rename_from" | "key" | "unique" | "index"
            )
        })
        .cloned()
        .collect()
}

fn metadata_annotations(annotations: &[Annotation]) -> Vec<Annotation> {
    annotations
        .iter()
        .filter(|annotation| matches!(annotation.name.as_str(), "description" | "instruction"))
        .cloned()
        .collect()
}

fn constraint_ir_key(constraint: &ConstraintIR) -> String {
    use super::schema_ir::{FieldRefIR, SystemFieldRole};

    let field = |field: &FieldRefIR| match field {
        FieldRefIR::Property(reference) => format!(
            "property:{}:{}",
            reference.owner_type_id.get(),
            reference.property_id.get()
        ),
        FieldRefIR::System(reference) => format!(
            "system:{}:{}:{}",
            reference.stable_table_id.get(),
            reference.table_incarnation_id.get(),
            match reference.role {
                SystemFieldRole::Id => "id",
                SystemFieldRole::Src => "src",
                SystemFieldRole::Dst => "dst",
            }
        ),
    };
    let fields = |fields: &[FieldRefIR]| fields.iter().map(&field).collect::<Vec<_>>().join(",");
    match constraint {
        ConstraintIR::Key { fields: values } => format!("key:{}", fields(values)),
        ConstraintIR::Unique { fields: values } => format!("unique:{}", fields(values)),
        ConstraintIR::Index { fields: values } => format!("index:{}", fields(values)),
        ConstraintIR::Range {
            field: value,
            min,
            max,
        } => {
            format!("range:{}:{min:?}:{max:?}", field(value))
        }
        ConstraintIR::Check {
            field: value,
            pattern,
        } => format!("check:{}:{pattern}", field(value)),
    }
}

fn schema_type_kind_key(kind: SchemaTypeKind) -> &'static str {
    match kind {
        SchemaTypeKind::Interface => "interface",
        SchemaTypeKind::Node => "node",
        SchemaTypeKind::Edge => "edge",
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::schema_ir::{
        SchemaIdentityDomain, initialize_schema_ir, resolve_schema_ir,
    };
    use crate::catalog::schema_shape::compile_schema_shape;
    use crate::schema::parser::parse_schema;

    use super::SchemaMigrationStep::{
        AddConstraint, AddProperty, RenameProperty, RenameType, UnsupportedChange,
        UpdateTypeMetadata,
    };
    use super::*;

    fn ir(source: &str) -> crate::catalog::schema_ir::SchemaIR {
        let shape = compile_schema_shape(&parse_schema(source).unwrap()).unwrap();
        initialize_schema_ir(
            SchemaIdentityDomain::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            &shape,
        )
        .unwrap()
        .schema_ir
    }

    fn evolve(
        accepted: &crate::catalog::schema_ir::SchemaIR,
        source: &str,
    ) -> crate::catalog::schema_ir::SchemaIR {
        let shape = compile_schema_shape(&parse_schema(source).unwrap()).unwrap();
        resolve_schema_ir(accepted, &shape).unwrap().schema_ir
    }

    const ENUM_ACCEPTED: &str = r#"
node Ticket {
    slug: String @key
    status: enum(todo, doing, done)
}
"#;

    #[test]
    fn plan_supports_pure_enum_widening() {
        let accepted = ir(ENUM_ACCEPTED);
        let desired = evolve(
            &accepted,
            r#"
node Ticket {
    slug: String @key
    status: enum(todo, doing, done, blocked, canceled)
}
"#,
        );
        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(
            plan.supported,
            "widening must be a supported plan: {plan:?}"
        );
        assert!(plan.steps.contains(&SchemaMigrationStep::ExtendEnum {
            type_kind: SchemaTypeKind::Node,
            type_name: "Ticket".to_string(),
            property_name: "status".to_string(),
            added_values: vec!["blocked".to_string(), "canceled".to_string()],
        }));
    }

    #[test]
    fn plan_treats_pure_reorder_as_no_change() {
        // Enum values are sorted + deduped by the schema IR, so a reorder is
        // type-identical — no step at all, not even a widening.
        let accepted = ir(ENUM_ACCEPTED);
        let desired = evolve(
            &accepted,
            r#"
node Ticket {
    slug: String @key
    status: enum(done, todo, doing)
}
"#,
        );
        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(plan.supported);
        assert!(
            plan.steps.is_empty(),
            "reorder must be a no-op plan: {:?}",
            plan.steps
        );
    }

    #[test]
    fn plan_orders_rename_before_widening_and_names_the_new_property() {
        // A property renamed AND widened in one migration emits both steps:
        // RenameProperty first, then ExtendEnum carrying the post-rename name
        // (the sequential-consumer contract pinned on the variant's doc).
        let accepted = ir(ENUM_ACCEPTED);
        let desired = evolve(
            &accepted,
            r#"
node Ticket {
    slug: String @key
    state: enum(todo, doing, done, blocked) @rename_from("status")
}
"#,
        );
        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(plan.supported, "rename+widen must be supported: {plan:?}");
        let rename_pos = plan.steps.iter().position(
            |s| matches!(s, RenameProperty { from, to, .. } if from == "status" && to == "state"),
        );
        let widen_pos = plan.steps.iter().position(|s| {
            matches!(
                s,
                SchemaMigrationStep::ExtendEnum { property_name, added_values, .. }
                    if property_name == "state" && added_values == &vec!["blocked".to_string()]
            )
        });
        let (Some(rename_pos), Some(widen_pos)) = (rename_pos, widen_pos) else {
            panic!(
                "expected RenameProperty + ExtendEnum, got: {:?}",
                plan.steps
            );
        };
        assert!(
            rename_pos < widen_pos,
            "rename must precede the widening for sequential consumers"
        );
    }

    #[test]
    fn plan_rejects_enum_narrowing_and_rename() {
        let accepted = ir(ENUM_ACCEPTED);
        for desired_src in [
            // narrowing
            "node Ticket {\n    slug: String @key\n    status: enum(todo, done)\n}\n",
            // rename of a variant (doing -> in_progress) = remove + add
            "node Ticket {\n    slug: String @key\n    status: enum(todo, in_progress, done)\n}\n",
        ] {
            let desired = evolve(&accepted, desired_src);
            let plan = plan_schema_migration(&accepted, &desired).unwrap();
            assert!(!plan.supported, "must be unsupported: {desired_src}");
            assert!(
                plan.steps.iter().any(|s| matches!(
                    s,
                    UnsupportedChange { code: Some(c), .. } if c == crate::lint::codes::OG_MF_106.code
                )),
                "expected OG-MF-106: {plan:?}"
            );
        }
    }

    #[test]
    fn plan_rejects_widening_combined_with_nullability_change() {
        let accepted = ir(ENUM_ACCEPTED);
        let desired = evolve(
            &accepted,
            r#"
node Ticket {
    slug: String @key
    status: enum(todo, doing, done, blocked)?
}
"#,
        );
        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(!plan.supported, "widen+nullable-flip must stay unsupported");
    }

    #[test]
    fn plan_rejects_enum_to_free_string_and_back() {
        let accepted = ir(ENUM_ACCEPTED);
        let free = evolve(
            &accepted,
            "node Ticket {\n    slug: String @key\n    status: String\n}\n",
        );
        let plan = plan_schema_migration(&accepted, &free).unwrap();
        assert!(!plan.supported, "enum->String must stay unsupported");
        let plan_back = plan_schema_migration(&free, &accepted).unwrap();
        assert!(!plan_back.supported, "String->enum must stay unsupported");
    }

    #[test]
    fn plan_supports_additive_nullable_property_and_index() {
        let accepted = ir(r#"
node Person {
    name: String @key
    age: I32?
}
"#);
        let desired = evolve(
            &accepted,
            r#"
node Person {
    name: String @key
    age: I32? @index
    nickname: String?
}
"#,
        );

        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(plan.supported);
        assert!(plan.steps.contains(&AddProperty {
            type_kind: SchemaTypeKind::Node,
            type_name: "Person".to_string(),
            property_name: "nickname".to_string(),
            property_type: PropType::scalar(crate::types::ScalarType::String, true),
        }));
        assert!(plan.steps.contains(&AddConstraint {
            type_kind: SchemaTypeKind::Node,
            type_name: "Person".to_string(),
            constraint: Constraint::Index(vec!["age".to_string()]),
        }));
    }

    #[test]
    fn plan_supports_explicit_type_and_property_rename() {
        let accepted = ir(r#"
node User {
    name: String @key
}
"#);
        let desired = evolve(
            &accepted,
            r#"
node Account @rename_from("User") {
    full_name: String @key @rename_from("name")
}
"#,
        );

        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(plan.supported);
        assert!(plan.steps.contains(&RenameType {
            type_kind: SchemaTypeKind::Node,
            from: "User".to_string(),
            to: "Account".to_string(),
        }));
        assert!(plan.steps.contains(&RenameProperty {
            type_kind: SchemaTypeKind::Node,
            type_name: "Account".to_string(),
            from: "name".to_string(),
            to: "full_name".to_string(),
        }));
    }

    #[test]
    fn plan_emits_soft_drop_for_removed_nullable_property() {
        // Removing a property from the desired schema emits
        // DropProperty { Soft } (schema-lint v1 chassis commit #3,
        // MR-694). The plan is `supported = true` — the apply path
        // handles soft drop via the existing stage_overwrite rewrite
        // projection. Verified at the integration level by
        // `apply_schema_drops_a_nullable_property_softly_preserves_prior_version`
        // in `crates/omnigraph/tests/schema_apply.rs`.
        let accepted = ir(r#"
node Person {
    name: String @key
    age: I32?
}
"#);
        let desired = evolve(
            &accepted,
            r#"
node Person {
    name: String @key
}
"#,
        );

        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(
            plan.supported,
            "drop-property plan must be supported: {plan:?}"
        );
        assert!(
            plan.steps.iter().any(|step| matches!(
                step,
                SchemaMigrationStep::DropProperty {
                    type_kind: SchemaTypeKind::Node,
                    type_name,
                    property_name,
                    mode: DropMode::Soft,
                    ..
                } if type_name == "Person" && property_name == "age"
            )),
            "expected DropProperty {{ Soft }} step in plan: {plan:?}",
        );
        // Negative: no UnsupportedChange anywhere in the plan.
        assert!(
            !plan
                .steps
                .iter()
                .any(|step| matches!(step, UnsupportedChange { .. })),
            "soft drop must not emit UnsupportedChange: {plan:?}",
        );
    }

    #[test]
    fn plan_emits_soft_drop_for_removed_node_and_edge_types() {
        // Removing a node type + the edge type that references it
        // emits two DropType { Soft } steps (chassis v1 commit #4,
        // MR-694). The plan is `supported = true` — apply tombstones
        // both manifest entries. Time-travel reversibility is verified
        // at the integration level by
        // `apply_schema_drops_node_and_referencing_edge_softly`
        // in `crates/omnigraph/tests/schema_apply.rs`.
        let accepted = ir(r#"
node Person {
    name: String @key
}

node Company {
    name: String @key
}

edge WorksAt: Person -> Company
"#);
        let desired = evolve(
            &accepted,
            r#"
node Person {
    name: String @key
}
"#,
        );

        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(plan.supported, "drop-type plan must be supported: {plan:?}");
        assert!(
            plan.steps.iter().any(|step| matches!(
                step,
                SchemaMigrationStep::DropType {
                    type_kind: SchemaTypeKind::Node,
                    name,
                    mode: DropMode::Soft,
                } if name == "Company"
            )),
            "expected DropType {{ Node, Company, Soft }} in plan: {plan:?}",
        );
        assert!(
            plan.steps.iter().any(|step| matches!(
                step,
                SchemaMigrationStep::DropType {
                    type_kind: SchemaTypeKind::Edge,
                    name,
                    mode: DropMode::Soft,
                } if name == "WorksAt"
            )),
            "expected DropType {{ Edge, WorksAt, Soft }} in plan: {plan:?}",
        );
        // Negative: no UnsupportedChange anywhere in the plan.
        assert!(
            !plan
                .steps
                .iter()
                .any(|step| matches!(step, UnsupportedChange { .. })),
            "soft type drop must not emit UnsupportedChange: {plan:?}",
        );
    }

    #[test]
    fn plan_rejects_required_property_addition() {
        let accepted = ir(r#"
node Person {
    name: String @key
}
"#);
        let desired = evolve(
            &accepted,
            r#"
node Person {
    name: String @key
    age: I32
}
"#,
        );

        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(!plan.supported);
        assert!(plan.steps.iter().any(|step| matches!(
            step,
            UnsupportedChange { entity, code, .. }
                if entity.contains("Person.age")
                    && code.as_deref() == Some(crate::lint::codes::OG_MF_103.code)
        )));
    }

    #[test]
    fn plan_supports_metadata_only_annotation_changes() {
        let accepted = ir(r#"
node Person @description("old") {
    name: String @key
}
"#);
        let desired = evolve(
            &accepted,
            r#"
node Person @description("new") {
    name: String @key
}
"#,
        );

        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(plan.supported);
        assert!(plan.steps.contains(&UpdateTypeMetadata {
            type_kind: SchemaTypeKind::Node,
            name: "Person".to_string(),
            annotations: vec![Annotation {
                name: "description".to_string(),
                value: Some("new".to_string()),
                kwargs: Default::default(),
            }],
        }));
    }

    #[test]
    fn drop_steps_round_trip_through_serde() {
        // The DropType / DropProperty variants are dormant in this
        // commit — the planner doesn't emit them yet — but their
        // serde shape needs to be stable from day one. A future
        // SchemaIR JSON containing one of these must deserialize
        // back to the same value. This test pins the wire format
        // so a v0 schema-ir consumer never sees a surprise variant
        // shape after v1 ships.
        let steps = vec![
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Node,
                name: "Person".to_string(),
                mode: DropMode::Soft,
            },
            SchemaMigrationStep::DropType {
                type_kind: SchemaTypeKind::Edge,
                name: "Knows".to_string(),
                mode: DropMode::Hard,
            },
            SchemaMigrationStep::DropProperty {
                type_kind: SchemaTypeKind::Node,
                type_name: "Person".to_string(),
                property_name: "age".to_string(),
                mode: DropMode::Soft,
            },
            SchemaMigrationStep::DropProperty {
                type_kind: SchemaTypeKind::Interface,
                type_name: "Named".to_string(),
                property_name: "alias".to_string(),
                mode: DropMode::Hard,
            },
        ];

        for step in steps {
            let json = serde_json::to_string(&step).expect("serialize");
            let round_trip: SchemaMigrationStep = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(step, round_trip, "round-trip mismatch on {json}");
        }
    }

    #[test]
    fn drop_mode_serde_uses_snake_case() {
        // External tools may write SchemaIR JSON by hand. Pin the
        // wire form so we don't silently break them later.
        assert_eq!(serde_json::to_string(&DropMode::Soft).unwrap(), "\"soft\"");
        assert_eq!(serde_json::to_string(&DropMode::Hard).unwrap(), "\"hard\"");
        let soft: DropMode = serde_json::from_str("\"soft\"").unwrap();
        assert_eq!(soft, DropMode::Soft);
    }
}
