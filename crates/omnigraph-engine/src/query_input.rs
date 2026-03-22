use std::error::Error;
use std::fmt;

use serde_json::Value;

use crate::error::NanoError;
use crate::ir::ParamMap;
use crate::json_output::{JS_MAX_SAFE_INTEGER_U64, is_js_safe_integer_i64};
use crate::query::ast::{Literal, Param, QueryDecl};
use crate::query::parser::parse_query;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonParamMode {
    Standard,
    JavaScript,
}

#[derive(Debug)]
pub enum RunInputError {
    Core(NanoError),
    Message(String),
}

impl RunInputError {
    fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

impl fmt::Display for RunInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(err) => err.fmt(f),
            Self::Message(message) => f.write_str(message),
        }
    }
}

impl Error for RunInputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Core(err) => Some(err),
            Self::Message(_) => None,
        }
    }
}

impl From<NanoError> for RunInputError {
    fn from(value: NanoError) -> Self {
        Self::Core(value)
    }
}

pub type RunInputResult<T> = std::result::Result<T, RunInputError>;

pub trait ToParam {
    fn to_param(self) -> crate::error::Result<Literal>;
}

impl ToParam for Literal {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(self)
    }
}

impl ToParam for &Literal {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(self.clone())
    }
}

impl ToParam for String {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::String(self))
    }
}

impl ToParam for &String {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::String(self.clone()))
    }
}

impl ToParam for &str {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::String(self.to_string()))
    }
}

impl ToParam for bool {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::Bool(self))
    }
}

impl ToParam for i8 {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::Integer(i64::from(self)))
    }
}

impl ToParam for i16 {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::Integer(i64::from(self)))
    }
}

impl ToParam for i32 {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::Integer(i64::from(self)))
    }
}

impl ToParam for i64 {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::Integer(self))
    }
}

impl ToParam for isize {
    fn to_param(self) -> crate::error::Result<Literal> {
        let value = i64::try_from(self).map_err(|_| {
            NanoError::Execution(format!(
                "param value {} exceeds current engine range for numeric literals (max {})",
                self,
                i64::MAX
            ))
        })?;
        Ok(Literal::Integer(value))
    }
}

impl ToParam for u8 {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::Integer(i64::from(self)))
    }
}

impl ToParam for u16 {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::Integer(i64::from(self)))
    }
}

impl ToParam for u32 {
    fn to_param(self) -> crate::error::Result<Literal> {
        Ok(Literal::Integer(i64::from(self)))
    }
}

impl ToParam for u64 {
    fn to_param(self) -> crate::error::Result<Literal> {
        let value = i64::try_from(self).map_err(|_| {
            NanoError::Execution(format!(
                "param value {} exceeds current engine range for numeric literals (max {})",
                self,
                i64::MAX
            ))
        })?;
        Ok(Literal::Integer(value))
    }
}

impl ToParam for usize {
    fn to_param(self) -> crate::error::Result<Literal> {
        let value = i64::try_from(self).map_err(|_| {
            NanoError::Execution(format!(
                "param value {} exceeds current engine range for numeric literals (max {})",
                self,
                i64::MAX
            ))
        })?;
        Ok(Literal::Integer(value))
    }
}

impl ToParam for f32 {
    fn to_param(self) -> crate::error::Result<Literal> {
        if !self.is_finite() {
            return Err(NanoError::Execution(format!(
                "invalid float parameter {}",
                self
            )));
        }
        Ok(Literal::Float(f64::from(self)))
    }
}

impl ToParam for f64 {
    fn to_param(self) -> crate::error::Result<Literal> {
        if !self.is_finite() {
            return Err(NanoError::Execution(format!(
                "invalid float parameter {}",
                self
            )));
        }
        Ok(Literal::Float(self))
    }
}

impl<T> ToParam for Vec<T>
where
    T: ToParam,
{
    fn to_param(self) -> crate::error::Result<Literal> {
        let mut out = Vec::with_capacity(self.len());
        for value in self {
            out.push(value.to_param()?);
        }
        Ok(Literal::List(out))
    }
}

impl<T> ToParam for &[T]
where
    T: Clone + ToParam,
{
    fn to_param(self) -> crate::error::Result<Literal> {
        let mut out = Vec::with_capacity(self.len());
        for value in self {
            out.push(value.clone().to_param()?);
        }
        Ok(Literal::List(out))
    }
}

impl<T, const N: usize> ToParam for [T; N]
where
    T: ToParam,
{
    fn to_param(self) -> crate::error::Result<Literal> {
        let mut out = Vec::with_capacity(N);
        for value in self {
            out.push(value.to_param()?);
        }
        Ok(Literal::List(out))
    }
}

#[macro_export]
macro_rules! params {
    () => {
        ::std::result::Result::Ok($crate::ParamMap::new())
    };
    ($($key:expr => $value:expr),+ $(,)?) => {{
        (|| -> $crate::error::Result<$crate::ParamMap> {
            let mut map = $crate::ParamMap::new();
            $(
                map.insert(::std::convert::Into::<String>::into($key), $crate::ToParam::to_param($value)?);
            )+
            Ok(map)
        })()
    }};
}

pub fn find_named_query(query_source: &str, query_name: &str) -> RunInputResult<QueryDecl> {
    let queries = parse_query(query_source)?;
    queries
        .queries
        .into_iter()
        .find(|query| query.name == query_name)
        .ok_or_else(|| RunInputError::message(format!("query '{}' not found", query_name)))
}

pub fn json_params_to_param_map(
    params: Option<&Value>,
    query_params: &[Param],
    mode: JsonParamMode,
) -> RunInputResult<ParamMap> {
    let mut map = ParamMap::new();
    let object = match params {
        Some(Value::Object(object)) => object,
        Some(Value::Null) | None => return Ok(map),
        Some(other) => {
            let message = match mode {
                JsonParamMode::Standard => "params must be a JSON object".to_string(),
                JsonParamMode::JavaScript => {
                    format!("params must be an object, got {}", json_type_name(other))
                }
            };
            return Err(RunInputError::message(message));
        }
    };

    for (key, value) in object {
        let decl = query_params.iter().find(|param| param.name == *key);
        let literal = if let Some(decl) = decl {
            json_value_to_literal_typed(key, value, &decl.type_name, mode)?
        } else {
            json_value_to_literal_inferred(key, value, mode)?
        };
        map.insert(key.clone(), literal);
    }

    Ok(map)
}

fn json_value_to_literal_typed(
    key: &str,
    value: &Value,
    type_name: &str,
    mode: JsonParamMode,
) -> RunInputResult<Literal> {
    match type_name {
        "String" => match value {
            Value::String(value) => Ok(Literal::String(value.clone())),
            other => Err(RunInputError::message(format!(
                "param '{}': expected string, got {}",
                key,
                json_type_name(other)
            ))),
        },
        "I32" => match mode {
            JsonParamMode::Standard => {
                let value = parse_i64_param(key, value, mode)?;
                let value = i32::try_from(value).map_err(|_| {
                    RunInputError::message(format!("param '{}': value {} exceeds I32", key, value))
                })?;
                Ok(Literal::Integer(i64::from(value)))
            }
            JsonParamMode::JavaScript => {
                let value = parse_i64_param(key, value, mode)?;
                let value = i32::try_from(value).map_err(|_| {
                    RunInputError::message(format!(
                        "param '{}': value {} exceeds I32 range",
                        key, value
                    ))
                })?;
                Ok(Literal::Integer(i64::from(value)))
            }
        },
        "I64" => Ok(Literal::Integer(parse_i64_param(key, value, mode)?)),
        "U32" => {
            let value = parse_u64_param(key, value, mode)?;
            let value = match mode {
                JsonParamMode::Standard => u32::try_from(value).map_err(|_| {
                    RunInputError::message(format!("param '{}': value {} exceeds U32", key, value))
                })?,
                JsonParamMode::JavaScript => u32::try_from(value).map_err(|_| {
                    RunInputError::message(format!(
                        "param '{}': value {} exceeds U32 range",
                        key, value
                    ))
                })?,
            };
            Ok(Literal::Integer(i64::from(value)))
        }
        "U64" => {
            let value = parse_u64_param(key, value, mode)?;
            let value = match mode {
                JsonParamMode::Standard => i64::try_from(value).map_err(|_| {
                    RunInputError::message(format!(
                        "param '{}': value {} exceeds current engine range for U64 (max {})",
                        key,
                        value,
                        i64::MAX
                    ))
                })?,
                JsonParamMode::JavaScript => i64::try_from(value).map_err(|_| {
                    RunInputError::message(format!(
                        "param '{}': value {} exceeds current engine range for U64 parameters (max {})",
                        key,
                        value,
                        i64::MAX
                    ))
                })?,
            };
            Ok(Literal::Integer(value))
        }
        "F32" | "F64" => {
            let value = value.as_f64().ok_or_else(|| match mode {
                JsonParamMode::Standard => {
                    RunInputError::message(format!("param '{}': expected float", key))
                }
                JsonParamMode::JavaScript => RunInputError::message(format!(
                    "param '{}': expected float, got {}",
                    key,
                    json_type_name(value)
                )),
            })?;
            Ok(Literal::Float(value))
        }
        "Bool" => {
            let value = value.as_bool().ok_or_else(|| match mode {
                JsonParamMode::Standard => {
                    RunInputError::message(format!("param '{}': expected boolean", key))
                }
                JsonParamMode::JavaScript => RunInputError::message(format!(
                    "param '{}': expected boolean, got {}",
                    key,
                    json_type_name(value)
                )),
            })?;
            Ok(Literal::Bool(value))
        }
        "Date" => match value {
            Value::String(value) => Ok(Literal::Date(value.clone())),
            other => Err(match mode {
                JsonParamMode::Standard => {
                    RunInputError::message(format!("param '{}': expected date string", key))
                }
                JsonParamMode::JavaScript => RunInputError::message(format!(
                    "param '{}': expected date string, got {}",
                    key,
                    json_type_name(other)
                )),
            }),
        },
        "DateTime" => match value {
            Value::String(value) => Ok(Literal::DateTime(value.clone())),
            other => Err(match mode {
                JsonParamMode::Standard => {
                    RunInputError::message(format!("param '{}': expected datetime string", key))
                }
                JsonParamMode::JavaScript => RunInputError::message(format!(
                    "param '{}': expected datetime string, got {}",
                    key,
                    json_type_name(other)
                )),
            }),
        },
        other if other.starts_with("Vector(") => {
            let expected_dim = parse_vector_dim(other).ok_or_else(|| match mode {
                JsonParamMode::Standard => RunInputError::message(format!(
                    "param '{}': invalid vector type '{}'",
                    key, other
                )),
                JsonParamMode::JavaScript => RunInputError::message(format!(
                    "param '{}': invalid vector type '{}' (expected Vector(N))",
                    key, other
                )),
            })?;
            let items = value.as_array().ok_or_else(|| match mode {
                JsonParamMode::Standard => {
                    RunInputError::message(format!("param '{}': expected array for {}", key, other))
                }
                JsonParamMode::JavaScript => RunInputError::message(format!(
                    "param '{}': expected array for {}, got {}",
                    key,
                    other,
                    json_type_name(value)
                )),
            })?;
            if items.len() != expected_dim {
                return Err(RunInputError::message(format!(
                    "param '{}': expected {} values for {}, got {}",
                    key,
                    expected_dim,
                    other,
                    items.len()
                )));
            }
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let value = item.as_f64().ok_or_else(|| match mode {
                    JsonParamMode::Standard => RunInputError::message(format!(
                        "param '{}': vector element is not numeric",
                        key
                    )),
                    JsonParamMode::JavaScript => RunInputError::message(format!(
                        "param '{}': vector element '{}' is not numeric",
                        key, item
                    )),
                })?;
                out.push(Literal::Float(value));
            }
            Ok(Literal::List(out))
        }
        _ => match value {
            Value::String(value) => Ok(Literal::String(value.clone())),
            other => Err(RunInputError::message(format!(
                "param '{}': expected string for type '{}', got {}",
                key,
                type_name,
                json_type_name(other)
            ))),
        },
    }
}

fn json_value_to_literal_inferred(
    key: &str,
    value: &Value,
    mode: JsonParamMode,
) -> RunInputResult<Literal> {
    match value {
        Value::String(value) => Ok(Literal::String(value.clone())),
        Value::Bool(value) => Ok(Literal::Bool(*value)),
        Value::Number(number) => match mode {
            JsonParamMode::Standard => {
                if let Some(value) = number.as_i64() {
                    Ok(Literal::Integer(value))
                } else if let Some(value) = number.as_f64() {
                    Ok(Literal::Float(value))
                } else {
                    Err(RunInputError::message(format!(
                        "param '{}': unsupported numeric value",
                        key
                    )))
                }
            }
            JsonParamMode::JavaScript => {
                if let Some(value) = number.as_i64() {
                    if !is_js_safe_integer_i64(value) {
                        return Err(RunInputError::message(format!(
                            "param '{}': integer {} exceeds JS safe integer range; use a decimal string and a typed query parameter for exact values",
                            key, value
                        )));
                    }
                    Ok(Literal::Integer(value))
                } else if let Some(value) = number.as_u64() {
                    if value > JS_MAX_SAFE_INTEGER_U64 {
                        return Err(RunInputError::message(format!(
                            "param '{}': integer {} exceeds JS safe integer range; use a decimal string and a typed query parameter for exact values",
                            key, value
                        )));
                    }
                    let value = i64::try_from(value).map_err(|_| {
                        RunInputError::message(format!(
                            "param '{}': integer {} exceeds supported range (max {})",
                            key,
                            value,
                            i64::MAX
                        ))
                    })?;
                    Ok(Literal::Integer(value))
                } else if let Some(value) = number.as_f64() {
                    Ok(Literal::Float(value))
                } else {
                    Err(RunInputError::message(format!(
                        "param '{}': unsupported number value",
                        key
                    )))
                }
            }
        },
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(json_value_to_literal_inferred(key, value, mode)?);
            }
            Ok(Literal::List(out))
        }
        Value::Null => Err(match mode {
            JsonParamMode::Standard => {
                RunInputError::message(format!("param '{}': null is not supported", key))
            }
            JsonParamMode::JavaScript => RunInputError::message(format!(
                "param '{}': null values are not supported as query parameters",
                key
            )),
        }),
        Value::Object(_) => Err(match mode {
            JsonParamMode::Standard => {
                RunInputError::message(format!("param '{}': object is not supported", key))
            }
            JsonParamMode::JavaScript => RunInputError::message(format!(
                "param '{}': object values are not supported as query parameters",
                key
            )),
        }),
    }
}

fn parse_i64_param(key: &str, value: &Value, mode: JsonParamMode) -> RunInputResult<i64> {
    match mode {
        JsonParamMode::Standard => match value {
            Value::Number(number) => number.as_i64().ok_or_else(|| {
                RunInputError::message(format!("param '{}': expected integer number", key))
            }),
            Value::String(value) => value.parse::<i64>().map_err(|_| {
                RunInputError::message(format!(
                    "param '{}': expected integer string, got '{}'",
                    key, value
                ))
            }),
            _ => Err(RunInputError::message(format!(
                "param '{}': expected integer",
                key
            ))),
        },
        JsonParamMode::JavaScript => match value {
            Value::Number(number) => {
                let parsed = if let Some(parsed) = number.as_i64() {
                    parsed
                } else if let Some(parsed) = number.as_f64() {
                    if !parsed.is_finite() || parsed.fract() != 0.0 {
                        return Err(RunInputError::message(format!(
                            "param '{}': expected integer, got number",
                            key
                        )));
                    }
                    if parsed < i64::MIN as f64 || parsed > i64::MAX as f64 {
                        return Err(RunInputError::message(format!(
                            "param '{}': integer {} is outside i64 range",
                            key, parsed
                        )));
                    }
                    parsed as i64
                } else {
                    return Err(RunInputError::message(format!(
                        "param '{}': expected integer, got number",
                        key
                    )));
                };
                if !is_js_safe_integer_i64(parsed) {
                    return Err(RunInputError::message(format!(
                        "param '{}': integer {} exceeds JS safe integer range; pass a decimal string for exact values",
                        key, parsed
                    )));
                }
                Ok(parsed)
            }
            Value::String(value) => value.parse::<i64>().map_err(|_| {
                RunInputError::message(format!(
                    "param '{}': expected integer string, got '{}'",
                    key, value
                ))
            }),
            other => Err(RunInputError::message(format!(
                "param '{}': expected integer, got {}",
                key,
                json_type_name(other)
            ))),
        },
    }
}

fn parse_u64_param(key: &str, value: &Value, mode: JsonParamMode) -> RunInputResult<u64> {
    match mode {
        JsonParamMode::Standard => match value {
            Value::Number(number) => number.as_u64().ok_or_else(|| {
                RunInputError::message(format!("param '{}': expected unsigned integer number", key))
            }),
            Value::String(value) => value.parse::<u64>().map_err(|_| {
                RunInputError::message(format!(
                    "param '{}': expected unsigned integer string, got '{}'",
                    key, value
                ))
            }),
            _ => Err(RunInputError::message(format!(
                "param '{}': expected unsigned integer",
                key
            ))),
        },
        JsonParamMode::JavaScript => match value {
            Value::Number(number) => {
                let parsed = if let Some(parsed) = number.as_u64() {
                    parsed
                } else if let Some(parsed) = number.as_f64() {
                    if !parsed.is_finite() || parsed.fract() != 0.0 || parsed < 0.0 {
                        return Err(RunInputError::message(format!(
                            "param '{}': expected unsigned integer, got number",
                            key
                        )));
                    }
                    if parsed > u64::MAX as f64 {
                        return Err(RunInputError::message(format!(
                            "param '{}': integer {} is outside u64 range",
                            key, parsed
                        )));
                    }
                    parsed as u64
                } else {
                    return Err(RunInputError::message(format!(
                        "param '{}': expected unsigned integer, got number",
                        key
                    )));
                };
                if parsed > JS_MAX_SAFE_INTEGER_U64 {
                    return Err(RunInputError::message(format!(
                        "param '{}': integer {} exceeds JS safe integer range; pass a decimal string for exact values",
                        key, parsed
                    )));
                }
                Ok(parsed)
            }
            Value::String(value) => value.parse::<u64>().map_err(|_| {
                RunInputError::message(format!(
                    "param '{}': expected unsigned integer string, got '{}'",
                    key, value
                ))
            }),
            other => Err(RunInputError::message(format!(
                "param '{}': expected unsigned integer, got {}",
                key,
                json_type_name(other)
            ))),
        },
    }
}

fn parse_vector_dim(type_name: &str) -> Option<usize> {
    let dim = type_name
        .strip_prefix("Vector(")?
        .strip_suffix(')')?
        .parse::<usize>()
        .ok()?;
    if dim == 0 { None } else { Some(dim) }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{JsonParamMode, ToParam, find_named_query, json_params_to_param_map};
    use crate::query::ast::Literal;

    #[test]
    fn js_mode_rejects_unsafe_integer_numbers() {
        let query = find_named_query(
            "query find($id: U64) { match { $u: User } return { $u } }",
            "find",
        )
        .expect("query should parse");

        let error = json_params_to_param_map(
            Some(&json!({ "id": 9_007_199_254_740_992u64 })),
            &query.params,
            JsonParamMode::JavaScript,
        )
        .expect_err("unsafe integer should fail");

        assert_eq!(
            error.to_string(),
            "param 'id': integer 9007199254740992 exceeds JS safe integer range; pass a decimal string for exact values"
        );
    }

    #[test]
    fn standard_mode_preserves_ffi_param_object_error() {
        let error = json_params_to_param_map(Some(&json!(["nope"])), &[], JsonParamMode::Standard)
            .expect_err("non-object params should fail");

        assert_eq!(error.to_string(), "params must be a JSON object");
    }

    #[test]
    fn to_param_supports_lists_and_explicit_date_literals() {
        let vector = vec![1_i32, 2_i32, 3_i32].to_param().expect("vector param");
        match vector {
            Literal::List(values) => {
                assert!(matches!(values.first(), Some(Literal::Integer(1))));
                assert!(matches!(values.get(1), Some(Literal::Integer(2))));
                assert!(matches!(values.get(2), Some(Literal::Integer(3))));
            }
            other => panic!("expected list param, got {:?}", other),
        }

        let date = Literal::Date("2026-03-06".to_string())
            .to_param()
            .expect("date param");
        assert!(matches!(date, Literal::Date(ref value) if value == "2026-03-06"));
    }

    #[test]
    fn to_param_rejects_unsigned_values_outside_engine_range() {
        let error = u64::MAX.to_param().expect_err("oversized u64 should fail");

        assert_eq!(
            error.to_string(),
            format!(
                "execution error: param value {} exceeds current engine range for numeric literals (max {})",
                u64::MAX,
                i64::MAX
            )
        );
    }

    #[test]
    fn params_macro_builds_param_map() {
        let params = params! {
            "name" => "Alice",
            "age" => 41_i32,
            "scores" => [1_u8, 2_u8, 3_u8],
            "published_at" => Literal::DateTime("2026-03-06T12:00:00Z".to_string()),
        }
        .expect("params");

        assert!(matches!(
            params.get("name"),
            Some(Literal::String(value)) if value == "Alice"
        ));
        assert!(matches!(params.get("age"), Some(Literal::Integer(41))));
        match params.get("scores") {
            Some(Literal::List(values)) => {
                assert!(matches!(values.first(), Some(Literal::Integer(1))));
                assert!(matches!(values.get(1), Some(Literal::Integer(2))));
                assert!(matches!(values.get(2), Some(Literal::Integer(3))));
            }
            other => panic!("expected list param, got {:?}", other),
        }
        assert!(matches!(
            params.get("published_at"),
            Some(Literal::DateTime(value)) if value == "2026-03-06T12:00:00Z"
        ));
    }
}
