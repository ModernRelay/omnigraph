//! The lock that makes `param_json_schema` correct *by construction*: for a
//! fixed corpus, the generated JSON Schema must accept **at least** every value
//! the engine coercer (`coerce_param_typed`, Standard mode — the mode the
//! stored-query invoke path uses) accepts. A schema narrower than the coercer
//! would make a strict client reject inputs the engine would have taken; a
//! wider one is fine (it reaches the coercer and surfaces as `isError`,
//! SEP-1303). Drift in either direction-of-acceptance turns this red.
//!
//! Keyed by **engine type-name string** (`"I32"`, `"U64"`, `"Vector(3)"`,
//! `"[I32]"`), not `ParamKind`, because the coercer keys on the type-name and
//! `ParamKind` is a lossy bucket (`I32`/`U32` → `Int`). The descriptor is built
//! through the real projection (`param_descriptor`).

use omnigraph_api_types::{param_descriptor, param_json_schema};
use omnigraph_compiler::query::ast::Param;
use omnigraph_compiler::{JsonParamMode, coerce_param_typed};
use serde_json::{Value, json};

/// A faithful validator for the closed schema vocabulary `param_json_schema`
/// emits: `type` (string/integer/number/boolean/array/null), `anyOf`, `items`,
/// `minItems`/`maxItems`, and `pattern` (enforced via `regex`). It interprets
/// the *emitted* schema JSON — not a hardcoded copy — so it tracks generator
/// changes. A new construct the generator emits would need a new arm here.
fn schema_accepts(schema: &Value, value: &Value) -> bool {
    if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array) {
        return any_of.iter().any(|s| schema_accepts(s, value));
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("string") => {
            let Some(s) = value.as_str() else { return false };
            match schema.get("pattern").and_then(Value::as_str) {
                Some(pat) => regex::Regex::new(pat).unwrap().is_match(s),
                None => true,
            }
        }
        Some("integer") => value.as_i64().is_some() || value.as_u64().is_some(),
        Some("number") => value.is_number(),
        Some("boolean") => value.is_boolean(),
        Some("null") => value.is_null(),
        Some("array") => {
            let Some(arr) = value.as_array() else { return false };
            if let Some(min) = schema.get("minItems").and_then(Value::as_u64) {
                if (arr.len() as u64) < min {
                    return false;
                }
            }
            if let Some(max) = schema.get("maxItems").and_then(Value::as_u64) {
                if (arr.len() as u64) > max {
                    return false;
                }
            }
            match schema.get("items") {
                Some(items) => arr.iter().all(|el| schema_accepts(items, el)),
                None => true,
            }
        }
        other => panic!("schema_accepts: unhandled schema {other:?} in {schema}"),
    }
}

fn descriptor(type_name: &str, nullable: bool) -> omnigraph_api_types::ParamDescriptor {
    param_descriptor(&Param {
        name: "p".to_string(),
        type_name: type_name.to_string(),
        nullable,
    })
}

/// Every engine scalar/composite type-name a stored-query param can declare.
const TYPE_NAMES: &[&str] = &[
    "String", "Bool", "I32", "I64", "U32", "U64", "F32", "F64", "Date", "DateTime", "Blob",
    "Vector(3)", "[I32]", "[String]",
];

/// A broad value bag thrown at every type-name; the coercer decides which it
/// accepts, and the schema must accept at least those.
fn corpus() -> Vec<Value> {
    vec![
        json!("hello"),
        json!("AAEC"),                     // base64-looking → still just a string (Blob)
        json!("2024-01-02"),               // date string
        json!("2024-01-02T03:04:05Z"),     // datetime string
        json!("og://blob/abc"),            // blob URI
        json!(5),
        json!(-5),
        json!(0),
        json!(5_000_000_000i64),           // fits i64/u64, exceeds i32
        json!(99_999_999_999u64),
        json!("5"),
        json!("-5"),
        json!("9999999999999999999999"),   // exceeds i64 even as string
        json!(1.5),
        json!(true),
        json!([1.0, 2.0, 3.0]),
        json!([1, 2, 3]),
        json!([1, 2]),
        json!(["a", "b"]),
        json!({ "k": 1 }),
    ]
}

#[test]
fn schema_is_a_superset_of_the_coercer() {
    for &type_name in TYPE_NAMES {
        let schema = param_json_schema(&descriptor(type_name, false));
        for value in corpus() {
            // The coercer is the authority; null is handled by the parent
            // (`json_params_to_param_map`), not `coerce_param_typed`, so skip it
            // here — the null rule is pinned separately below.
            if value.is_null() {
                continue;
            }
            if coerce_param_typed("p", &value, type_name, JsonParamMode::Standard).is_ok() {
                assert!(
                    schema_accepts(&schema, &value),
                    "type {type_name}: coercer accepts {value} but schema {schema} rejects it"
                );
            }
        }
    }
}

#[test]
fn nullable_rule_matches_the_parent_coercer() {
    // The parent coercer accepts explicit `null` iff the param is nullable.
    // `param_json_schema` must mirror that at the schema level.
    for &type_name in TYPE_NAMES {
        let nullable = param_json_schema(&descriptor(type_name, true));
        let non_nullable = param_json_schema(&descriptor(type_name, false));
        assert!(
            schema_accepts(&nullable, &Value::Null),
            "type {type_name}: nullable schema must accept null ({nullable})"
        );
        assert!(
            !schema_accepts(&non_nullable, &Value::Null),
            "type {type_name}: non-nullable schema must reject null ({non_nullable})"
        );
    }
}

#[test]
fn vector_dim_bounds_are_present_or_omitted() {
    let with_dim = param_json_schema(&descriptor("Vector(4)", false));
    assert_eq!(with_dim["minItems"], json!(4));
    assert_eq!(with_dim["maxItems"], json!(4));
    // A four-element array validates; three or five do not.
    assert!(schema_accepts(&with_dim, &json!([1.0, 2.0, 3.0, 4.0])));
    assert!(!schema_accepts(&with_dim, &json!([1.0, 2.0, 3.0])));
}
