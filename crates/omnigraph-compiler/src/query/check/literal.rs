//! Literal typing and type compatibility: the type a literal takes, the
//! check of a literal against a property's type, and which value types an
//! assignment or a comparison accepts.

use crate::error::{CompilerError, Result};
use crate::query::codes::*;
use crate::query::value::Literal;
use crate::types::{PropType, ScalarType, check_date_literal, check_datetime_literal};

/// The type of a literal payload, computed once per occurrence (list
/// elements included) and retained on the typed node.
pub(crate) fn literal_type(lit: &Literal) -> Result<PropType> {
    match lit {
        // Null is compatible with any nullable type; default to String for inference.
        Literal::Null => Ok(PropType::scalar(ScalarType::String, true)),
        Literal::String(_) => Ok(PropType::scalar(ScalarType::String, false)),
        Literal::Integer(_) => Ok(PropType::scalar(ScalarType::I64, false)),
        Literal::Float(value) if value.is_infinite() => Err(CompilerError::typed(
            T3,
            "float literal is outside the F64 range".to_string(),
        )),
        Literal::Float(_) => Ok(PropType::scalar(ScalarType::F64, false)),
        Literal::Bool(_) => Ok(PropType::scalar(ScalarType::Bool, false)),
        Literal::Date(value) => {
            check_date_literal(value)
                .map_err(|reason| CompilerError::typed(T3, reason.to_string()))?;
            Ok(PropType::scalar(ScalarType::Date, false))
        }
        Literal::DateTime(value) => {
            check_datetime_literal(value)
                .map_err(|reason| CompilerError::typed(T3, reason.to_string()))?;
            Ok(PropType::scalar(ScalarType::DateTime, false))
        }
        Literal::List(items) => {
            let mut scalar = None;
            for item in items {
                let item_type = literal_type(item)?;
                if matches!(item, Literal::Null) {
                    continue;
                }
                if item_type.list {
                    return Err(CompilerError::typed(
                        T52,
                        "nested list literals are not supported".to_string(),
                    ));
                }
                scalar = Some(match scalar {
                    None => item_type.scalar,
                    Some(previous) if previous == item_type.scalar => previous,
                    Some(ScalarType::I64 | ScalarType::F64)
                        if matches!(item_type.scalar, ScalarType::I64 | ScalarType::F64) =>
                    {
                        ScalarType::F64
                    }
                    Some(_) => {
                        return Err(CompilerError::typed(
                            T53,
                            "list literal elements must share a compatible scalar type".to_string(),
                        ));
                    }
                });
            }
            Ok(PropType::list_of(
                scalar.unwrap_or(ScalarType::String),
                false,
            ))
        }
    }
}

/// A literal against the property it is assigned to or matched with;
/// `lit_type` is the type the resolver computed for `lit`, so the payload
/// is inspected here (null, vector width, enum values) but never typed again.
pub(crate) fn check_literal_type(
    lit: &Literal,
    lit_type: &PropType,
    expected: &PropType,
    prop_name: &str,
) -> Result<()> {
    // Null is compatible with any nullable property type.
    if matches!(lit, Literal::Null) {
        return if expected.nullable {
            Ok(())
        } else {
            Err(CompilerError::typed(
                T3,
                format!("property `{}` is non-nullable but got null", prop_name),
            ))
        };
    }

    if !expected.list
        && let ScalarType::Vector(expected_dim) = expected.scalar
        && let Some(actual_dim) = numeric_vector_literal_dim(lit)
    {
        if actual_dim == expected_dim {
            return Ok(());
        }
        return Err(CompilerError::typed(
            T3,
            format!(
                "property `{}` has type Vector({}) but got vector literal with {} elements",
                prop_name, expected_dim, actual_dim
            ),
        ));
    }

    if !types_compatible(lit_type, expected) {
        return Err(CompilerError::typed(
            T3,
            format!(
                "property `{}` has type {} but got {}",
                prop_name,
                expected.display_name(),
                lit_type.display_name()
            ),
        ));
    }
    if expected.is_enum() {
        let allowed = expected.enum_values.as_ref().cloned().unwrap_or_default();
        match lit {
            Literal::String(v) if !allowed.contains(v) => {
                return Err(CompilerError::typed(
                    T3,
                    format!(
                        "property `{}` expects one of [{}], got '{}'",
                        prop_name,
                        allowed.join(", "),
                        v
                    ),
                ));
            }
            Literal::List(items) if expected.list => {
                for item in items {
                    match item {
                        Literal::String(v) if allowed.contains(v) => {}
                        Literal::String(v) => {
                            return Err(CompilerError::typed(
                                T3,
                                format!(
                                    "property `{}` expects one of [{}], got '{}'",
                                    prop_name,
                                    allowed.join(", "),
                                    v
                                ),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// `types_compatible`, plus a String parameter assigned to a Blob property
/// (the blob's URI).
pub(crate) fn assignment_compatible(actual: &PropType, expected: &PropType) -> bool {
    types_compatible(actual, expected)
        || (matches!(expected.scalar, ScalarType::Blob)
            && matches!(actual.scalar, ScalarType::String)
            && !actual.list)
}

pub(crate) fn types_compatible(a: &PropType, b: &PropType) -> bool {
    if a.list != b.list {
        return false;
    }
    if a.scalar == b.scalar {
        return true;
    }
    // Numeric types are mutually compatible for comparison
    if a.scalar.is_numeric() && b.scalar.is_numeric() {
        return true;
    }
    false
}

fn numeric_vector_literal_dim(lit: &Literal) -> Option<u32> {
    let items = match lit {
        Literal::List(items) => items,
        _ => return None,
    };
    if items.is_empty() {
        return None;
    }
    if items
        .iter()
        .all(|v| matches!(v, Literal::Integer(_) | Literal::Float(_)))
    {
        Some(items.len() as u32)
    } else {
        None
    }
}
