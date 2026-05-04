use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::schema::ast::{Annotation, Constraint};
use crate::types::PropType;

use super::schema_ir::{EdgeIR, InterfaceIR, NodeIR, PropertyIR, SchemaIR};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaTypeKind {
    Interface,
    Node,
    Edge,
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
    UpdateSchemaConfig {
        embedding_model: String,
    },
    ReembedProperty {
        type_name: String,
        property_name: String,
        embedding_model: String,
        dimensions: u32,
    },
    UnsupportedChange {
        entity: String,
        reason: String,
    },
}

pub fn plan_schema_migration(
    accepted: &SchemaIR,
    desired: &SchemaIR,
) -> Result<SchemaMigrationPlan> {
    let mut steps = Vec::new();
    let changed_embedding_model = accepted.config.embedding_model != desired.config.embedding_model;
    if changed_embedding_model {
        steps.push(SchemaMigrationStep::UpdateSchemaConfig {
            embedding_model: desired.config.embedding_model.clone(),
        });
    }
    let interface_renames = plan_interfaces(&accepted.interfaces, &desired.interfaces, &mut steps);
    let node_renames = plan_nodes(
        &accepted.nodes,
        &desired.nodes,
        &interface_renames,
        changed_embedding_model.then_some(desired.config.embedding_model.as_str()),
        &mut steps,
    );
    plan_edges(&accepted.edges, &desired.edges, &node_renames, &mut steps);

    Ok(SchemaMigrationPlan {
        supported: !steps
            .iter()
            .any(|step| matches!(step, SchemaMigrationStep::UnsupportedChange { .. })),
        steps,
    })
}

fn plan_interfaces(
    accepted: &[InterfaceIR],
    desired: &[InterfaceIR],
    steps: &mut Vec<SchemaMigrationStep>,
) -> HashMap<String, String> {
    let accepted_by_name = accepted
        .iter()
        .map(|interface| (interface.name.as_str(), interface))
        .collect::<HashMap<_, _>>();
    let mut consumed = HashSet::new();

    for interface in desired {
        if let Some(existing) = accepted_by_name.get(interface.name.as_str()) {
            consumed.insert(existing.name.clone());
            let _property_renames = plan_properties(
                SchemaTypeKind::Interface,
                &interface.name,
                &existing.properties,
                &interface.properties,
                None,
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
        .filter(|interface| !consumed.contains(&interface.name))
    {
        steps.push(SchemaMigrationStep::UnsupportedChange {
            entity: format!("interface:{}", leftover.name),
            reason: format!(
                "removing interface '{}' is not supported in schema migration v1",
                leftover.name
            ),
        });
    }

    HashMap::new()
}

fn plan_nodes(
    accepted: &[NodeIR],
    desired: &[NodeIR],
    interface_renames: &HashMap<String, String>,
    changed_embedding_model: Option<&str>,
    steps: &mut Vec<SchemaMigrationStep>,
) -> HashMap<String, String> {
    let accepted_by_name = accepted
        .iter()
        .map(|node| (node.name.as_str(), node))
        .collect::<HashMap<_, _>>();
    let mut consumed = HashSet::new();
    let mut renames = HashMap::new();

    for node in desired {
        let rename_from = rename_from_value(&node.annotations);
        let matched = accepted_by_name
            .get(node.name.as_str())
            .copied()
            .or_else(|| {
                rename_from.and_then(|from| {
                    accepted_by_name
                        .get(from)
                        .copied()
                        .filter(|candidate| candidate.name != node.name)
                })
            });

        let Some(existing) = matched else {
            if let Some(from) = rename_from {
                steps.push(SchemaMigrationStep::UnsupportedChange {
                    entity: format!("node:{}", node.name),
                    reason: format!(
                        "node '{}' declares @rename_from(\"{}\") but no accepted node with that name exists",
                        node.name, from
                    ),
                });
            } else {
                steps.push(SchemaMigrationStep::AddType {
                    type_kind: SchemaTypeKind::Node,
                    name: node.name.clone(),
                });
            }
            continue;
        };

        consumed.insert(existing.name.clone());
        if existing.name != node.name {
            renames.insert(existing.name.clone(), node.name.clone());
            steps.push(SchemaMigrationStep::RenameType {
                type_kind: SchemaTypeKind::Node,
                from: existing.name.clone(),
                to: node.name.clone(),
            });
        }

        if normalize_strings(&existing.implements, interface_renames)
            != normalize_strings(&node.implements, &HashMap::new())
        {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("node:{}", node.name),
                reason: format!(
                    "changing implemented interfaces on node '{}' is not supported in schema migration v1",
                    node.name
                ),
            });
        }

        plan_type_metadata(
            SchemaTypeKind::Node,
            &node.name,
            &existing.annotations,
            &node.annotations,
            steps,
        );
        let property_renames = plan_properties(
            SchemaTypeKind::Node,
            &node.name,
            &existing.properties,
            &node.properties,
            changed_embedding_model,
            steps,
        );
        plan_constraints(
            SchemaTypeKind::Node,
            &node.name,
            &existing.constraints,
            &node.constraints,
            &property_renames,
            steps,
        );
    }

    for leftover in accepted
        .iter()
        .filter(|node| !consumed.contains(&node.name))
    {
        steps.push(SchemaMigrationStep::UnsupportedChange {
            entity: format!("node:{}", leftover.name),
            reason: format!(
                "removing node type '{}' is not supported in schema migration v1",
                leftover.name
            ),
        });
    }

    renames
}

fn plan_edges(
    accepted: &[EdgeIR],
    desired: &[EdgeIR],
    node_renames: &HashMap<String, String>,
    steps: &mut Vec<SchemaMigrationStep>,
) {
    let accepted_by_name = accepted
        .iter()
        .map(|edge| (edge.name.as_str(), edge))
        .collect::<HashMap<_, _>>();
    let mut consumed = HashSet::new();

    for edge in desired {
        let rename_from = rename_from_value(&edge.annotations);
        let matched = accepted_by_name
            .get(edge.name.as_str())
            .copied()
            .or_else(|| {
                rename_from.and_then(|from| {
                    accepted_by_name
                        .get(from)
                        .copied()
                        .filter(|candidate| candidate.name != edge.name)
                })
            });

        let Some(existing) = matched else {
            if let Some(from) = rename_from {
                steps.push(SchemaMigrationStep::UnsupportedChange {
                    entity: format!("edge:{}", edge.name),
                    reason: format!(
                        "edge '{}' declares @rename_from(\"{}\") but no accepted edge with that name exists",
                        edge.name, from
                    ),
                });
            } else {
                steps.push(SchemaMigrationStep::AddType {
                    type_kind: SchemaTypeKind::Edge,
                    name: edge.name.clone(),
                });
            }
            continue;
        };

        consumed.insert(existing.name.clone());
        if existing.name != edge.name {
            steps.push(SchemaMigrationStep::RenameType {
                type_kind: SchemaTypeKind::Edge,
                from: existing.name.clone(),
                to: edge.name.clone(),
            });
        }

        let normalized_from = normalize_type_ref(&existing.from_type, node_renames);
        let normalized_to = normalize_type_ref(&existing.to_type, node_renames);
        if normalized_from != edge.from_type || normalized_to != edge.to_type {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("edge:{}", edge.name),
                reason: format!(
                    "changing edge endpoints on '{}' is not supported in schema migration v1",
                    edge.name
                ),
            });
        }
        if existing.cardinality != edge.cardinality {
            steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("edge:{}", edge.name),
                reason: format!(
                    "changing cardinality on edge '{}' is not supported in schema migration v1",
                    edge.name
                ),
            });
        }

        plan_type_metadata(
            SchemaTypeKind::Edge,
            &edge.name,
            &existing.annotations,
            &edge.annotations,
            steps,
        );
        let property_renames = plan_properties(
            SchemaTypeKind::Edge,
            &edge.name,
            &existing.properties,
            &edge.properties,
            None,
            steps,
        );
        plan_constraints(
            SchemaTypeKind::Edge,
            &edge.name,
            &existing.constraints,
            &edge.constraints,
            &property_renames,
            steps,
        );
    }

    for leftover in accepted
        .iter()
        .filter(|edge| !consumed.contains(&edge.name))
    {
        steps.push(SchemaMigrationStep::UnsupportedChange {
            entity: format!("edge:{}", leftover.name),
            reason: format!(
                "removing edge type '{}' is not supported in schema migration v1",
                leftover.name
            ),
        });
    }
}

fn plan_properties(
    type_kind: SchemaTypeKind,
    type_name: &str,
    accepted: &[PropertyIR],
    desired: &[PropertyIR],
    changed_embedding_model: Option<&str>,
    steps: &mut Vec<SchemaMigrationStep>,
) -> HashMap<String, String> {
    let accepted_by_name = accepted
        .iter()
        .map(|property| (property.name.as_str(), property))
        .collect::<HashMap<_, _>>();
    let mut consumed = HashSet::new();
    let mut renames = HashMap::new();

    for property in desired {
        let rename_from = rename_from_value(&property.annotations);
        let matched = accepted_by_name
            .get(property.name.as_str())
            .copied()
            .or_else(|| {
                rename_from.and_then(|from| {
                    accepted_by_name
                        .get(from)
                        .copied()
                        .filter(|candidate| candidate.name != property.name)
                })
            });

        let Some(existing) = matched else {
            if let Some(from) = rename_from {
                steps.push(SchemaMigrationStep::UnsupportedChange {
                    entity: format!(
                        "{}:{}.{}",
                        schema_type_kind_key(type_kind),
                        type_name,
                        property.name
                    ),
                    reason: format!(
                        "property '{}.{}' declares @rename_from(\"{}\") but no accepted property with that name exists",
                        type_name, property.name, from
                    ),
                });
            } else if property.prop_type.nullable {
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
                });
            }
            continue;
        };

        consumed.insert(existing.name.clone());
        if existing.name != property.name {
            renames.insert(existing.name.clone(), property.name.clone());
            steps.push(SchemaMigrationStep::RenameProperty {
                type_kind,
                type_name: type_name.to_string(),
                from: existing.name.clone(),
                to: property.name.clone(),
            });
        }

        if existing.prop_type != property.prop_type {
            if type_kind == SchemaTypeKind::Node
                && is_embed_property(existing)
                && is_embed_property(property)
                && let Some(model) = changed_embedding_model
                && let Some(dimensions) = vector_dimensions(&property.prop_type)
            {
                steps.push(SchemaMigrationStep::ReembedProperty {
                    type_name: type_name.to_string(),
                    property_name: property.name.clone(),
                    embedding_model: model.to_string(),
                    dimensions,
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
                        "changing property type for '{}.{}' is not supported in schema migration v1",
                        type_name, property.name
                    ),
                });
            }
        } else if type_kind == SchemaTypeKind::Node
            && is_embed_property(existing)
            && is_embed_property(property)
            && let Some(model) = changed_embedding_model
            && let Some(dimensions) = vector_dimensions(&property.prop_type)
        {
            steps.push(SchemaMigrationStep::ReembedProperty {
                type_name: type_name.to_string(),
                property_name: property.name.clone(),
                embedding_model: model.to_string(),
                dimensions,
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
        .filter(|property| !consumed.contains(&property.name))
    {
        steps.push(SchemaMigrationStep::UnsupportedChange {
            entity: format!(
                "{}:{}.{}",
                schema_type_kind_key(type_kind),
                type_name,
                leftover.name
            ),
            reason: format!(
                "removing property '{}.{}' is not supported in schema migration v1",
                type_name, leftover.name
            ),
        });
    }

    renames
}

fn plan_constraints(
    type_kind: SchemaTypeKind,
    type_name: &str,
    accepted: &[Constraint],
    desired: &[Constraint],
    property_renames: &HashMap<String, String>,
    steps: &mut Vec<SchemaMigrationStep>,
) {
    let accepted = accepted
        .iter()
        .cloned()
        .map(|constraint| rename_constraint_properties(constraint, property_renames))
        .collect::<Vec<_>>();
    let desired_map = desired
        .iter()
        .cloned()
        .map(|constraint| (constraint_key(&constraint), constraint))
        .collect::<BTreeMap<_, _>>();
    let accepted_map = accepted
        .into_iter()
        .map(|constraint| (constraint_key(&constraint), constraint))
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
        });
    }

    for (key, constraint) in desired_map {
        if accepted_map.contains_key(&key) {
            continue;
        }
        match constraint {
            Constraint::Index(_) => steps.push(SchemaMigrationStep::AddConstraint {
                type_kind,
                type_name: type_name.to_string(),
                constraint,
            }),
            _ => steps.push(SchemaMigrationStep::UnsupportedChange {
                entity: format!("{}:{}", schema_type_kind_key(type_kind), type_name),
                reason: format!(
                    "adding constraint '{}' to '{}' is not supported in schema migration v1",
                    key, type_name
                ),
            }),
        }
    }
}

fn is_embed_property(property: &PropertyIR) -> bool {
    property
        .annotations
        .iter()
        .any(|annotation| annotation.name == "embed")
}

fn vector_dimensions(prop_type: &PropType) -> Option<u32> {
    if prop_type.list {
        return None;
    }
    match prop_type.scalar {
        crate::types::ScalarType::Vector(dim) => Some(dim),
        _ => None,
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
            });
        }
    }
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

fn normalize_strings(values: &[String], renames: &HashMap<String, String>) -> BTreeSet<String> {
    values
        .iter()
        .map(|value| normalize_type_ref(value, renames))
        .collect()
}

fn normalize_type_ref(value: &str, renames: &HashMap<String, String>) -> String {
    renames
        .get(value)
        .cloned()
        .unwrap_or_else(|| value.to_string())
}

fn rename_constraint_properties(
    constraint: Constraint,
    property_renames: &HashMap<String, String>,
) -> Constraint {
    match constraint {
        Constraint::Key(columns) => {
            Constraint::Key(rename_constraint_columns(columns, property_renames))
        }
        Constraint::Unique(columns) => {
            Constraint::Unique(rename_constraint_columns(columns, property_renames))
        }
        Constraint::Index(columns) => {
            Constraint::Index(rename_constraint_columns(columns, property_renames))
        }
        Constraint::Range { property, min, max } => Constraint::Range {
            property: normalize_property_ref(&property, property_renames),
            min,
            max,
        },
        Constraint::Check { property, pattern } => Constraint::Check {
            property: normalize_property_ref(&property, property_renames),
            pattern,
        },
    }
}

fn rename_constraint_columns(
    columns: Vec<String>,
    property_renames: &HashMap<String, String>,
) -> Vec<String> {
    let mut columns = columns
        .into_iter()
        .map(|column| normalize_property_ref(&column, property_renames))
        .collect::<Vec<_>>();
    columns.sort();
    columns
}

fn normalize_property_ref(value: &str, renames: &HashMap<String, String>) -> String {
    renames
        .get(value)
        .cloned()
        .unwrap_or_else(|| value.to_string())
}

fn constraint_key(constraint: &Constraint) -> String {
    match constraint {
        Constraint::Key(columns) => format!("key:{}", columns.join(",")),
        Constraint::Unique(columns) => format!("unique:{}", columns.join(",")),
        Constraint::Index(columns) => format!("index:{}", columns.join(",")),
        Constraint::Range { property, min, max } => {
            format!("range:{}:{:?}:{:?}", property, min, max)
        }
        Constraint::Check { property, pattern } => format!("check:{}:{}", property, pattern),
    }
}

fn rename_from_value(annotations: &[Annotation]) -> Option<&str> {
    annotations
        .iter()
        .find(|annotation| annotation.name == "rename_from")
        .and_then(|annotation| annotation.value.as_deref())
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
    use crate::catalog::schema_ir::build_schema_ir;
    use crate::schema::parser::parse_schema;

    use super::SchemaMigrationStep::{
        AddConstraint, AddProperty, RenameProperty, RenameType, UnsupportedChange,
        UpdateTypeMetadata,
    };
    use super::*;

    #[test]
    fn plan_supports_additive_nullable_property_and_index() {
        let accepted = build_schema_ir(
            &parse_schema(
                r#"
node Person {
    name: String @key
    age: I32?
}
"#,
            )
            .unwrap(),
        )
        .unwrap();
        let desired = build_schema_ir(
            &parse_schema(
                r#"
node Person {
    name: String @key
    age: I32? @index
    nickname: String?
}
"#,
            )
            .unwrap(),
        )
        .unwrap();

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
        let accepted = build_schema_ir(
            &parse_schema(
                r#"
node User {
    name: String @key
}
"#,
            )
            .unwrap(),
        )
        .unwrap();
        let desired = build_schema_ir(
            &parse_schema(
                r#"
node Account @rename_from("User") {
    full_name: String @key @rename_from("name")
}
"#,
            )
            .unwrap(),
        )
        .unwrap();

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
    fn plan_rejects_required_property_addition() {
        let accepted = build_schema_ir(
            &parse_schema(
                r#"
node Person {
    name: String @key
}
"#,
            )
            .unwrap(),
        )
        .unwrap();
        let desired = build_schema_ir(
            &parse_schema(
                r#"
node Person {
    name: String @key
    age: I32
}
"#,
            )
            .unwrap(),
        )
        .unwrap();

        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(!plan.supported);
        assert!(plan.steps.iter().any(|step| matches!(
            step,
            UnsupportedChange { entity, reason }
                if entity.contains("Person.age")
                    && reason.contains("adding required property")
        )));
    }

    #[test]
    fn plan_supports_metadata_only_annotation_changes() {
        let accepted = build_schema_ir(
            &parse_schema(
                r#"
node Person @description("old") {
    name: String @key
}
"#,
            )
            .unwrap(),
        )
        .unwrap();
        let desired = build_schema_ir(
            &parse_schema(
                r#"
node Person @description("new") {
    name: String @key
}
"#,
            )
            .unwrap(),
        )
        .unwrap();

        let plan = plan_schema_migration(&accepted, &desired).unwrap();
        assert!(plan.supported);
        assert!(plan.steps.contains(&UpdateTypeMetadata {
            type_kind: SchemaTypeKind::Node,
            name: "Person".to_string(),
            annotations: vec![Annotation {
                name: "description".to_string(),
                value: Some("new".to_string()),
            }],
        }));
    }
}
