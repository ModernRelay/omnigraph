use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};

use crate::catalog::schema_ir::SchemaIR;
use crate::error::{NanoError, Result};

use super::super::graph::DatasetAccumulator;

#[derive(Debug, Default)]
pub(crate) struct NodeConstraintAnnotations {
    pub(crate) key_props: HashMap<String, String>,
    pub(crate) unique_props: HashMap<String, Vec<String>>,
}

pub(crate) fn load_node_constraint_annotations(
    schema_ir: &SchemaIR,
) -> Result<NodeConstraintAnnotations> {
    let mut constraints = NodeConstraintAnnotations::default();

    for node in schema_ir.node_types() {
        let mut node_key_prop: Option<String> = None;
        let mut node_unique_props: Vec<String> = Vec::new();

        for prop in &node.properties {
            if prop.key && node_key_prop.replace(prop.name.clone()).is_some() {
                return Err(NanoError::Storage(format!(
                    "node type {} has multiple @key properties; only one is currently supported",
                    node.name
                )));
            }
            if prop.unique {
                node_unique_props.push(prop.name.clone());
            }
        }

        if let Some(prop_name) = node_key_prop {
            if !node_unique_props.contains(&prop_name) {
                node_unique_props.push(prop_name.clone());
            }
            constraints.key_props.insert(node.name.clone(), prop_name);
        }
        if !node_unique_props.is_empty() {
            node_unique_props.sort();
            node_unique_props.dedup();
            constraints
                .unique_props
                .insert(node.name.clone(), node_unique_props);
        }
    }

    Ok(constraints)
}

pub(crate) fn enforce_node_unique_constraints(
    storage: &DatasetAccumulator,
    unique_props: &HashMap<String, Vec<String>>,
) -> Result<()> {
    for (type_name, properties) in unique_props {
        let Some(batch) = storage.get_all_nodes(type_name)? else {
            continue;
        };

        for property in properties {
            let prop_idx =
                node_property_index(batch.schema().as_ref(), property).ok_or_else(|| {
                    NanoError::Storage(format!(
                        "node type {} missing @unique property {}",
                        type_name, property
                    ))
                })?;
            let arr = batch.column(prop_idx);
            let mut seen: HashMap<String, usize> = HashMap::new();
            for row in 0..batch.num_rows() {
                let Some(value) = unique_value_string(arr, row, type_name, property)? else {
                    continue;
                };
                if let Some(prev_row) = seen.insert(value.clone(), row) {
                    return Err(NanoError::UniqueConstraint {
                        type_name: type_name.clone(),
                        property: property.clone(),
                        value,
                        first_row: prev_row,
                        second_row: row,
                    });
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn collect_incoming_node_types(data_source: &str) -> Result<HashSet<String>> {
    let mut node_types = HashSet::new();
    for line in data_source.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        let obj: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| NanoError::Storage(format!("JSON parse error: {}", e)))?;
        if let Some(type_name) = obj.get("type").and_then(|v| v.as_str()) {
            node_types.insert(type_name.to_string());
        }
    }
    Ok(node_types)
}

pub(crate) fn build_name_seed_for_keyed_load(
    storage: &DatasetAccumulator,
    key_props: &HashMap<String, String>,
) -> Result<HashMap<(String, String), u64>> {
    let mut seed = HashMap::new();

    for (type_name, key_prop) in key_props {
        let Some(batch) = storage.get_all_nodes(type_name)? else {
            continue;
        };

        let key_idx = node_property_index(batch.schema().as_ref(), key_prop).ok_or_else(|| {
            NanoError::Storage(format!(
                "node type {} missing @key property {}",
                type_name, key_prop
            ))
        })?;
        let key_arr = batch.column(key_idx).clone();
        let id_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                NanoError::Storage(format!("node type {} has non-UInt64 id column", type_name))
            })?;

        for row in 0..batch.num_rows() {
            let key = key_value_string(&key_arr, row, key_prop)?;
            seed.insert((type_name.clone(), key), id_arr.value(row));
        }
    }

    Ok(seed)
}

pub(crate) fn build_name_seed_for_append(
    storage: &DatasetAccumulator,
    key_props: &HashMap<String, String>,
) -> Result<HashMap<(String, String), u64>> {
    build_name_seed_for_keyed_load(storage, key_props)
}

pub(crate) fn node_property_index(schema: &Schema, prop_name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(idx, field)| (field.name() == prop_name).then_some(idx))
}

pub(crate) fn node_property_field<'a>(schema: &'a Schema, prop_name: &str) -> Option<&'a Field> {
    node_property_index(schema, prop_name).map(|idx| schema.field(idx))
}

pub(crate) fn key_value_string(array: &ArrayRef, row: usize, prop_name: &str) -> Result<String> {
    let value = scalar_value_string(array, row, "key", None, prop_name)?;
    if let Some(value) = value {
        return Ok(value);
    }
    Err(NanoError::Storage(format!(
        "@key property {} cannot be null",
        prop_name
    )))
}

fn unique_value_string(
    array: &ArrayRef,
    row: usize,
    type_name: &str,
    prop_name: &str,
) -> Result<Option<String>> {
    scalar_value_string(array, row, "unique", Some(type_name), prop_name)
}

fn scalar_value_string(
    array: &ArrayRef,
    row: usize,
    annotation: &str,
    type_name: Option<&str>,
    prop_name: &str,
) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let value = match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string()),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string()),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string()),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(row).to_string()),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| a.value(row).to_string()),
        _ => None,
    };

    let value = value.ok_or_else(|| {
        let target = match type_name {
            Some(name) => format!("{}.{}", name, prop_name),
            None => prop_name.to_string(),
        };
        NanoError::Storage(format!(
            "unsupported @{} data type {:?} for {}",
            annotation,
            array.data_type(),
            target
        ))
    })?;

    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use arrow_array::StringArray;

    use crate::catalog::schema_ir::{build_catalog_from_ir, build_schema_ir};
    use crate::schema::parser::parse_schema;

    use super::super::jsonl::load_jsonl_data;
    use super::*;

    fn build_schema_ir_and_storage(schema_src: &str) -> (SchemaIR, DatasetAccumulator) {
        let schema = parse_schema(schema_src).unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let catalog = build_catalog_from_ir(&ir).unwrap();
        (ir, DatasetAccumulator::new(catalog))
    }

    #[test]
    fn load_node_constraint_annotations_collects_key_and_unique() {
        let schema = r#"node Person {
    name: String @key
    email: String @unique
    alias: String? @unique
}"#;
        let (ir, _) = build_schema_ir_and_storage(schema);
        let annotations = load_node_constraint_annotations(&ir).unwrap();

        assert_eq!(annotations.key_props.get("Person").unwrap(), "name");
        assert_eq!(
            annotations.unique_props.get("Person").unwrap(),
            &vec!["alias".to_string(), "email".to_string(), "name".to_string()]
        );
    }

    #[test]
    fn collect_incoming_node_types_ignores_comments_and_blanks() {
        let data = r#"
// comment
{"type":"Person","data":{"name":"Alice"}}

{"edge":"Knows","from":"Alice","to":"Bob"}
{"type":"Company","data":{"name":"Acme"}}
"#;
        let types = collect_incoming_node_types(data).unwrap();
        assert_eq!(
            types,
            HashSet::from(["Person".to_string(), "Company".to_string()])
        );
    }

    #[test]
    fn enforce_node_unique_constraints_detects_duplicate_non_null() {
        let schema = r#"node Person {
    name: String
    email: String? @unique
}"#;
        let (_, mut storage) = build_schema_ir_and_storage(schema);
        let key_props = HashMap::new();
        load_jsonl_data(
            &mut storage,
            r#"{"type":"Person","data":{"name":"Alice","email":"dupe@example.com"}}
{"type":"Person","data":{"name":"Bob","email":"dupe@example.com"}}"#,
            &key_props,
        )
        .unwrap();

        let unique_props = HashMap::from([("Person".to_string(), vec!["email".to_string()])]);
        let err = enforce_node_unique_constraints(&storage, &unique_props).unwrap_err();
        match err {
            NanoError::UniqueConstraint {
                type_name,
                property,
                value,
                ..
            } => {
                assert_eq!(type_name, "Person");
                assert_eq!(property, "email");
                assert_eq!(value, "dupe@example.com");
            }
            other => panic!("expected UniqueConstraint, got {other}"),
        }
    }

    #[test]
    fn enforce_node_unique_constraints_allows_multiple_nulls() {
        let schema = r#"node Person {
    name: String
    nick: String? @unique
}"#;
        let (_, mut storage) = build_schema_ir_and_storage(schema);
        let key_props = HashMap::new();
        load_jsonl_data(
            &mut storage,
            r#"{"type":"Person","data":{"name":"Alice","nick":null}}
{"type":"Person","data":{"name":"Bob","nick":null}}"#,
            &key_props,
        )
        .unwrap();

        let unique_props = HashMap::from([("Person".to_string(), vec!["nick".to_string()])]);
        enforce_node_unique_constraints(&storage, &unique_props).unwrap();
    }

    #[test]
    fn enforce_node_unique_constraints_uses_user_property_named_id() {
        let schema = r#"node Person {
    id: String @unique
    name: String
}"#;
        let (_, mut storage) = build_schema_ir_and_storage(schema);
        let key_props = HashMap::new();
        load_jsonl_data(
            &mut storage,
            r#"{"type":"Person","data":{"id":"user-1","name":"Alice"}}
{"type":"Person","data":{"id":"user-1","name":"Bob"}}"#,
            &key_props,
        )
        .unwrap();

        let unique_props = HashMap::from([("Person".to_string(), vec!["id".to_string()])]);
        let err = enforce_node_unique_constraints(&storage, &unique_props).unwrap_err();
        match err {
            NanoError::UniqueConstraint {
                type_name,
                property,
                value,
                ..
            } => {
                assert_eq!(type_name, "Person");
                assert_eq!(property, "id");
                assert_eq!(value, "user-1");
            }
            other => panic!("expected UniqueConstraint, got {other}"),
        }
    }

    #[test]
    fn build_name_seed_for_keyed_load_uses_declared_key_property() {
        let schema = r#"node Person {
    uid: String @key
    name: String
}
node Company {
    name: String
}"#;
        let (_, mut storage) = build_schema_ir_and_storage(schema);
        let key_props = HashMap::from([("Person".to_string(), "uid".to_string())]);
        load_jsonl_data(
            &mut storage,
            r#"{"type":"Person","data":{"uid":"u1","name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}"#,
            &key_props,
        )
        .unwrap();

        let seed = build_name_seed_for_keyed_load(&storage, &key_props).unwrap();

        assert!(seed.contains_key(&("Person".to_string(), "u1".to_string())));
        assert!(!seed.contains_key(&("Company".to_string(), "Acme".to_string())));
    }

    #[test]
    fn build_name_seed_for_keyed_load_uses_user_property_named_id() {
        let schema = r#"node Person {
    id: String @key
    name: String
}"#;
        let (_, mut storage) = build_schema_ir_and_storage(schema);
        let key_props = HashMap::from([("Person".to_string(), "id".to_string())]);
        load_jsonl_data(
            &mut storage,
            r#"{"type":"Person","data":{"id":"user-1","name":"Alice"}}"#,
            &key_props,
        )
        .unwrap();

        let seed = build_name_seed_for_keyed_load(&storage, &key_props).unwrap();
        assert!(seed.contains_key(&("Person".to_string(), "user-1".to_string())));
    }

    #[test]
    fn build_name_seed_for_append_keeps_all_existing_keyed_nodes() {
        let schema = r#"node Person {
    uid: String @key
    name: String
}
node Company {
    name: String
}"#;
        let (_, mut storage) = build_schema_ir_and_storage(schema);
        let key_props = HashMap::from([("Person".to_string(), "uid".to_string())]);
        load_jsonl_data(
            &mut storage,
            r#"{"type":"Person","data":{"uid":"u1","name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}"#,
            &key_props,
        )
        .unwrap();

        let seed = build_name_seed_for_append(&storage, &key_props).unwrap();
        assert!(seed.contains_key(&("Person".to_string(), "u1".to_string())));
        assert!(!seed.contains_key(&("Company".to_string(), "Acme".to_string())));
    }

    #[test]
    fn key_value_string_rejects_null() {
        let arr: ArrayRef = std::sync::Arc::new(StringArray::from(vec![Some("x"), None]));
        assert_eq!(key_value_string(&arr, 0, "name").unwrap(), "x");
        let err = key_value_string(&arr, 1, "name").unwrap_err();
        assert!(err.to_string().contains("cannot be null"));
    }
}
