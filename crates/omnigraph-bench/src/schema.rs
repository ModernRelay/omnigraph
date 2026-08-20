//! The versioned record-schema artifact and its validator.
//!
//! `schema/run-record-v3.schema.json` is the single normative definition of
//! the v3 run record (RFC 0039: "the field list is normative" — here made
//! executable). The harness validates every record against it on WRITE
//! (refusing to emit an invalid record) and `diff` validates on READ
//! (refusing to consume one), so the structs in `record.rs` are a derivation
//! of the schema, never a second truth.
//!
//! The validator interprets the subset of JSON Schema the artifact uses —
//! `type`, `properties`, `required`, `items`, `enum`, `minItems`,
//! `additionalProperties: false` — rather than pulling a full JSON Schema
//! engine into the workspace for an internal instrument. Anything the schema
//! file says that this subset cannot express would be silently unenforced, so
//! the subset is a deliberate boundary: extend the validator before extending
//! the schema vocabulary.

use serde_json::Value;

use crate::fixture::BenchResult;

/// The v3 schema artifact, embedded so the binary and the file cannot drift.
pub const RUN_RECORD_V3_SCHEMA: &str = include_str!("../schema/run-record-v3.schema.json");

/// Validate a v3 run-record instance against the shipped schema. Returns
/// every violation, not just the first.
pub fn validate_run_record_v3(instance: &Value) -> Result<(), Vec<String>> {
    let schema: Value =
        serde_json::from_str(RUN_RECORD_V3_SCHEMA).expect("embedded schema is valid JSON");
    let mut errors = Vec::new();
    validate(&schema, instance, "$", &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Convenience wrapper turning violations into one refusal error.
pub fn require_valid_v3(instance: &Value, context: &str) -> BenchResult<()> {
    validate_run_record_v3(instance).map_err(|errors| {
        format!(
            "{context}: record violates schema/run-record-v3.schema.json:\n  {}",
            errors.join("\n  ")
        )
        .into()
    })
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) => {
            if n.is_u64() || n.is_i64() {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn matches_type(instance: &Value, wanted: &str) -> bool {
    match wanted {
        // Every integer is a number.
        "number" => matches!(instance, Value::Number(_)),
        other => type_name(instance) == other,
    }
}

fn validate(schema: &Value, instance: &Value, path: &str, errors: &mut Vec<String>) {
    let Some(schema_obj) = schema.as_object() else {
        return;
    };

    if let Some(wanted) = schema_obj.get("type") {
        let ok = match wanted {
            Value::String(t) => matches_type(instance, t),
            Value::Array(ts) => ts
                .iter()
                .filter_map(|t| t.as_str())
                .any(|t| matches_type(instance, t)),
            _ => true,
        };
        if !ok {
            errors.push(format!(
                "{path}: expected type {wanted}, found {}",
                type_name(instance)
            ));
            return; // Structural checks below assume the right type.
        }
    }

    if let Some(allowed) = schema_obj.get("enum").and_then(Value::as_array) {
        if !allowed.contains(instance) {
            errors.push(format!(
                "{path}: value {instance} is not one of {allowed:?}"
            ));
        }
    }

    if let Some(required) = schema_obj.get("required").and_then(Value::as_array) {
        if let Some(map) = instance.as_object() {
            for key in required.iter().filter_map(Value::as_str) {
                if !map.contains_key(key) {
                    errors.push(format!("{path}: missing required field '{key}'"));
                }
            }
        }
    }

    if let Some(props) = schema_obj.get("properties").and_then(Value::as_object) {
        if let Some(map) = instance.as_object() {
            for (key, subschema) in props {
                if let Some(value) = map.get(key) {
                    validate(subschema, value, &format!("{path}.{key}"), errors);
                }
            }
        }
    }

    // Closed objects (struct-derived): an instance key the schema does not
    // name is drift — a struct field added without its schema counterpart.
    if schema_obj.get("additionalProperties") == Some(&Value::Bool(false)) {
        if let Some(map) = instance.as_object() {
            let props = schema_obj.get("properties").and_then(Value::as_object);
            for key in map.keys() {
                if props.is_none_or(|p| !p.contains_key(key)) {
                    errors.push(format!(
                        "{path}: unknown field '{key}' (the object is closed: \
                         additionalProperties false)"
                    ));
                }
            }
        }
    }

    if let Some(items) = schema_obj.get("items") {
        if let Some(array) = instance.as_array() {
            for (i, value) in array.iter().enumerate() {
                validate(items, value, &format!("{path}[{i}]"), errors);
            }
        }
    }

    if let Some(min) = schema_obj.get("minItems").and_then(Value::as_u64) {
        if let Some(array) = instance.as_array() {
            if (array.len() as u64) < min {
                errors.push(format!(
                    "{path}: array has {} item(s), schema requires at least {min}",
                    array.len()
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn schema_artifact_parses() {
        let schema: Value = serde_json::from_str(RUN_RECORD_V3_SCHEMA).unwrap();
        assert_eq!(schema["properties"]["record_version"]["enum"][0], json!(3));
    }

    #[test]
    fn missing_required_field_is_reported_with_its_path() {
        let errors = validate_run_record_v3(&json!({ "record_version": 3 })).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("missing required field 'run_spec'"))
        );
        assert!(errors.iter().any(|e| e.contains("'point_name'")));
    }

    #[test]
    fn wrong_type_and_bad_enum_are_reported() {
        let errors =
            validate_run_record_v3(&json!({ "record_version": 2, "point_name": 7 })).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("$.record_version")));
        assert!(
            errors
                .iter()
                .any(|e| e.contains("$.point_name") && e.contains("expected type"))
        );
    }

    /// Drift guard, direction schema -> validator: every keyword the shipped
    /// schema uses must be one the mini-validator interprets. A keyword the
    /// validator ignores is a constraint silently unenforced — the deliberate
    /// boundary the module doc states, here made a failing test.
    #[test]
    fn every_schema_keyword_is_interpreted_by_the_validator() {
        /// Keywords `validate` interprets.
        const INTERPRETED: [&str; 7] = [
            "type",
            "properties",
            "required",
            "items",
            "enum",
            "minItems",
            "additionalProperties",
        ];
        /// Pure annotations: no validation semantics to enforce.
        const ANNOTATIONS: [&str; 4] = ["$schema", "$id", "title", "description"];
        fn walk_schema(schema: &Value, path: &str, violations: &mut Vec<String>) {
            let Some(obj) = schema.as_object() else {
                return;
            };
            for (key, value) in obj {
                match key.as_str() {
                    // The values under `properties` are named subschemas.
                    "properties" => {
                        if let Some(props) = value.as_object() {
                            for (name, sub) in props {
                                walk_schema(sub, &format!("{path}.{name}"), violations);
                            }
                        }
                    }
                    // The value of `items` is itself a schema.
                    "items" => walk_schema(value, &format!("{path}[]"), violations),
                    k if INTERPRETED.contains(&k) || ANNOTATIONS.contains(&k) => {}
                    other => violations.push(format!(
                        "{path}: keyword '{other}' is not interpreted by the mini-validator; \
                         extend `validate` in schema.rs before using it"
                    )),
                }
            }
        }
        let schema: Value = serde_json::from_str(RUN_RECORD_V3_SCHEMA).unwrap();
        let mut violations = Vec::new();
        walk_schema(&schema, "$", &mut violations);
        assert!(violations.is_empty(), "{}", violations.join("\n"));
    }

    /// Drift guard, direction structs -> schema: a full in-memory `RunRecord`
    /// must validate against the shipped schema and survive a serialize +
    /// deserialize round trip unchanged. Catches a struct field added without
    /// its schema counterpart (the closed objects' `additionalProperties:
    /// false` refuses the unknown key) and a schema field removed from the
    /// structs (through the required lists).
    #[test]
    fn golden_record_validates_and_round_trips() {
        let record =
            crate::record::testkit::sample_record("m3-t2-n100-noindex-d1-warm", &[10.0, 11.0]);
        let value = serde_json::to_value(&record).unwrap();
        require_valid_v3(&value, "golden record").unwrap();
        let reread: crate::record::RunRecord = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(&reread).unwrap(),
            value,
            "a validated record must round-trip byte-identically"
        );
    }

    /// The closed objects refuse unknown keys at every depth; the
    /// deliberately open objects (embedded manifest, engine configuration)
    /// still accept anything.
    #[test]
    fn closed_objects_refuse_unknown_keys_and_open_objects_accept_them() {
        let record = crate::record::testkit::sample_record("m3-t2-n100-noindex-d1-warm", &[10.0]);
        let mut value = serde_json::to_value(&record).unwrap();
        value["stray_top_level"] = json!(1);
        value["run_spec"]["data"]["stray_nested"] = json!("x");
        let errors = validate_run_record_v3(&value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("'stray_top_level'")));
        assert!(
            errors
                .iter()
                .any(|e| e.contains("$.run_spec.data") && e.contains("'stray_nested'"))
        );

        let mut value = serde_json::to_value(&record).unwrap();
        value["sut"]["engine_configuration"]["OMNIGRAPH_ANYTHING"] = json!("on");
        require_valid_v3(&value, "open engine configuration").unwrap();
    }
}
