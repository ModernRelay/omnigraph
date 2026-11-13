//! Literal typing and type compatibility: the type a literal takes, the
//! check of a literal against a property's type, and which value types an
//! assignment or a comparison accepts.

use crate::error::{CompilerError, Result};
use crate::query::codes::*;
use crate::query::value::Literal;
use crate::types::{
    PropType, ScalarType, check_date_literal, check_datetime_literal, date_literal_days,
    datetime_literal_millis,
};

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

/// A literal outside the domain of the property it is compared with: the
/// refusal's text, its printed fix, and the literal that replaces the
/// written one when a nearest allowed value exists.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiteralMismatch {
    pub message: String,
    pub fix: String,
    pub replacement: Option<String>,
}

/// A literal compared with a property, or listed in an `in` against one,
/// checked against the property's scalar type `expected`: an enum value
/// outside the declared set, an integer outside the type's range, a float
/// outside `F32`'s range, and a `Date` or `DateTime` that does not parse.
/// Edition 2 checked these inside property braces; without the check a
/// misspelled enum value returns no rows. A literal of an incompatible type
/// is left to the comparison's own refusal, and every other pairing is
/// admitted as before.
pub(crate) fn compared_literal(
    lit: &Literal,
    expected: &PropType,
    prop: &str,
) -> Option<LiteralMismatch> {
    if expected.list {
        return None;
    }
    let ty = expected.display_name();
    match (lit, expected.scalar) {
        (Literal::String(value), ScalarType::String) => {
            let allowed = expected.enum_values.as_deref()?;
            if allowed.contains(value) {
                return None;
            }
            let nearest = Literal::String(nearest_value(value, allowed)?.clone()).to_string();
            Some(LiteralMismatch {
                message: format!(
                    "property `{prop}` expects one of [{}], got '{value}'",
                    allowed.join(", ")
                ),
                fix: nearest.clone(),
                replacement: Some(nearest),
            })
        }
        (Literal::Integer(n), scalar) => {
            let (low, high) = integer_range(scalar)?;
            if (low..=high).contains(n) {
                return None;
            }
            let bound = Literal::Integer(if *n < low { low } else { high }).to_string();
            Some(LiteralMismatch {
                message: format!(
                    "property `{prop}` has type {ty} but got {n}, outside its range {low} to {high}"
                ),
                fix: bound.clone(),
                replacement: Some(bound),
            })
        }
        // A float against an integer property compares through a cast to
        // `F64`, which keeps the fraction (`$p.age > 2.5`), so it is not
        // refused; only an `F32` property bounds a float literal.
        (Literal::Float(f), ScalarType::F32) => {
            let max = f64::from(f32::MAX);
            if f.abs() <= max {
                return None;
            }
            let bound = Literal::Float(max.copysign(*f)).to_string();
            Some(LiteralMismatch {
                message: format!(
                    "property `{prop}` has type {ty} but got {lit}, outside its range -{max:e} to {max:e}"
                ),
                fix: bound.clone(),
                replacement: Some(bound),
            })
        }
        (Literal::Date(value), ScalarType::Date) => {
            let reason = date_literal_days(value).err()?;
            let nearest = nearest_date(value).map(|d| Literal::Date(d).to_string());
            Some(LiteralMismatch {
                message: format!(
                    "property `{prop}` has type {ty} but got the malformed date \"{value}\" ({reason}); a Date is written date(\"YYYY-MM-DD\")"
                ),
                fix: nearest
                    .clone()
                    .unwrap_or_else(|| "date(\"YYYY-MM-DD\")".to_string()),
                replacement: nearest,
            })
        }
        (Literal::DateTime(value), ScalarType::DateTime) => {
            let reason = datetime_literal_millis(value).err()?;
            Some(LiteralMismatch {
                message: format!(
                    "property `{prop}` has type {ty} but got the malformed datetime \"{value}\" ({reason}); a DateTime is written datetime(\"YYYY-MM-DDTHH:MM:SSZ\")"
                ),
                fix: "datetime(\"YYYY-MM-DDTHH:MM:SSZ\")".to_string(),
                replacement: None,
            })
        }
        _ => None,
    }
}

/// A literal a string function tests an enum property with (`contains`,
/// `starts_with`): refused when no declared value passes the test, since
/// the call is then false on every row.
pub(crate) fn tested_enum_literal(
    func: &str,
    value: &str,
    expected: &PropType,
    prop: &str,
) -> Option<LiteralMismatch> {
    let allowed = expected.enum_values.as_deref().filter(|_| !expected.list)?;
    let passes = |v: &String| match func {
        "contains" => v.contains(value),
        "starts_with" => v.starts_with(value),
        _ => true,
    };
    if allowed.iter().any(passes) {
        return None;
    }
    let nearest = Literal::String(nearest_value(value, allowed)?.clone()).to_string();
    Some(LiteralMismatch {
        message: format!(
            "property `{prop}` expects one of [{}], and none passes `{func}` with '{value}'",
            allowed.join(", ")
        ),
        fix: nearest.clone(),
        replacement: Some(nearest),
    })
}

/// The inclusive range of an integer type.
fn integer_range(scalar: ScalarType) -> Option<(i128, i128)> {
    Some(match scalar {
        ScalarType::I32 => (i32::MIN.into(), i32::MAX.into()),
        ScalarType::I64 => (i64::MIN.into(), i64::MAX.into()),
        ScalarType::U32 => (0, u32::MAX.into()),
        ScalarType::U64 => (0, u64::MAX.into()),
        _ => return None,
    })
}

/// The allowed value nearest to `written` by edit distance, the first in
/// declared order on a tie.
fn nearest_value<'a>(written: &str, allowed: &'a [String]) -> Option<&'a String> {
    allowed
        .iter()
        .enumerate()
        .min_by_key(|(i, v)| (edit_distance(written, v), *i))
        .map(|(_, v)| v)
}

/// Levenshtein distance over characters.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let above = row[j + 1];
            row[j + 1] = (above + 1)
                .min(row[j] + 1)
                .min(diagonal + usize::from(ca != *cb));
            diagonal = above;
        }
    }
    row[b.len()]
}

/// A `YYYY-MM-DD` spelling whose month or day is out of range, moved to the
/// nearest calendar day: the month into 1 to 12, the day into the month.
fn nearest_date(value: &str) -> Option<String> {
    let mut parts = value.splitn(3, '-');
    let (year, month, day) = (parts.next()?, parts.next()?, parts.next()?);
    let digits = |s: &str, n: std::ops::RangeInclusive<usize>| {
        n.contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit())
    };
    if !digits(year, 4..=4) || !digits(month, 1..=2) || !digits(day, 1..=2) {
        return None;
    }
    let year: i32 = year.parse().ok()?;
    let month = month.parse::<u32>().ok()?.clamp(1, 12);
    let last = (28..=31)
        .rev()
        .find(|d| date_literal_days(&format!("{year:04}-{month:02}-{d:02}")).is_ok())?;
    let day = day.parse::<u32>().ok()?.clamp(1, last);
    let nearest = format!("{year:04}-{month:02}-{day:02}");
    date_literal_days(&nearest).ok().map(|_| nearest)
}
