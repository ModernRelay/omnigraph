//! The scalar registry rows of the break (the GQ 3 RFC, section 2) over
//! Arrow arrays: the string, numeric, list and `cast` rows.
//!
//! Each row runs on the Arrow or DataFusion kernel already in the
//! dependency tree whose semantics match GQ's, or on a GQ kernel where none
//! matches. GQ's rules come first and are checked before the kernel: a row
//! is null where any argument is null, and a row that can fail refuses the
//! row with a typed execution diagnostic (`E1` an integer overflow, `E3` a
//! float result that is not finite, `E5` an argument outside the row's
//! domain, `E6` a value `cast` does not convert), never `NaN`, an infinity
//! or a null. The reference interpreter states every row independently
//! (`omnigraph-reference`, `registry.rs`); a kernel that disagrees with it
//! is a failing test.
//!
//! Where DataFusion 54's kernel differs from the RFC, the row wraps it:
//! - `trim`, `ltrim`, `rtrim`: DataFusion strips U+0020 alone; GQ strips
//!   Unicode whitespace, so these run on a GQ kernel.
//! - `left`, `right`: DataFusion reads a negative count as "all but the
//!   last (first) `n`"; GQ refuses it (`E5`), checked before the kernel.
//! - `substring`: DataFusion refuses a negative length with an untyped
//!   error and overflows on the smallest start; GQ checks the length first
//!   (`E5`) and clamps the start, which the window does not change.
//! - `round`: DataFusion reads a null `digits` as 0 and takes `Int32`
//!   digits and floats only; GQ's null is null, digits outside `I32` are
//!   `E5`, an integer rounds on a GQ kernel (DataFusion has none) and a
//!   result that is not finite is `E3`.
//! - `sqrt`, `ln`, `log10`, `power`, `exp`: DataFusion returns `NaN` or an
//!   infinity; GQ checks the domain first (`E5`) and refuses a result that
//!   is not finite (`E3`).
//! - `abs`: DataFusion's checked kernel refuses the smallest integer with an
//!   untyped error; GQ checks it first (`E1`).
//! - `power`: DataFusion refuses zero to a negative power with an untyped
//!   error; GQ's result is not finite (`E3`), checked before the kernel.
//! - `cast`: Arrow's cast with `safe: false` refuses an overflow and an
//!   unparsable string, but reads strings more widely than GQ (Arrow's Bool
//!   reads `yes`, `on`, `1` and their prefixes; its floats read `inf` and
//!   `NaN`) and truncates a `DateTime` to its `Date` toward zero; GQ checks
//!   a string against its literal forms first, refuses a value that becomes
//!   infinite, and floors an instant to its day on a GQ kernel. A string
//!   read as a `Date` or `DateTime` goes through the one literal parse the
//!   checker uses.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, PrimitiveArray, StringArray, builder::StringBuilder,
    types::ArrowPrimitiveType,
};
use arrow_schema::{DataType, Field};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use omnigraph_compiler::ir::expr::ScalarFunc;
use omnigraph_compiler::query::codes::{E1, E3, E5, E6};
use omnigraph_compiler::query::diagnostic::{QueryCode, QueryDiagnostic};
use omnigraph_compiler::types::{ExprType, ScalarType};

use crate::error::{OmniError, Result};

fn refused(code: QueryCode, message: impl Into<String>) -> OmniError {
    OmniError::Compiler(omnigraph_compiler::error::CompilerError::query(
        QueryDiagnostic::execution(code, message.into()),
    ))
}

/// A DataFusion scalar function over whole arrays, its result in `want`:
/// the function's own return type is asked for (`left` over `Utf8` returns
/// `Utf8View`), then cast.
fn udf(f: Arc<ScalarUDF>, args: &[ArrayRef], want: DataType) -> Result<ArrayRef> {
    let rows = args.first().map_or(0, |a| a.len());
    let arg_fields: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(i, a)| Arc::new(Field::new(format!("a{i}"), a.data_type().clone(), true)))
        .collect();
    let scalars: Vec<Option<&datafusion::scalar::ScalarValue>> = vec![None; args.len()];
    let return_field = f
        .return_field_from_args(datafusion::logical_expr::ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalars,
        })
        .map_err(OmniError::datafusion_internal)?;
    let out = f
        .invoke_with_args(ScalarFunctionArgs {
            args: args
                .iter()
                .map(|a| ColumnarValue::Array(Arc::clone(a)))
                .collect(),
            arg_fields,
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
        .map_err(OmniError::datafusion_internal)?
        .into_array(rows)
        .map_err(OmniError::datafusion_internal)?;
    if out.data_type() == &want {
        Ok(out)
    } else {
        arrow_cast::cast(&out, &want).map_err(OmniError::arrow_internal)
    }
}

/// The rows where every argument is valid.
fn all_valid(args: &[ArrayRef]) -> Vec<bool> {
    let rows = args.first().map_or(0, |a| a.len());
    (0..rows)
        .map(|r| args.iter().all(|a| a.is_valid(r)))
        .collect()
}

/// `values` with every row where some argument is null set null: GQ's rows
/// are null where an argument is, whatever the kernel made of it.
fn null_where_any(values: ArrayRef, args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.iter().all(|a| a.null_count() == 0) {
        return Ok(values);
    }
    let keep: BooleanArray = all_valid(args).into_iter().map(Some).collect();
    let nulls = arrow_array::new_null_array(values.data_type(), values.len());
    arrow_select::zip::zip(&keep, &values, &nulls).map_err(OmniError::arrow_internal)
}

fn downcast<'a, T: 'static>(a: &'a ArrayRef, what: &str) -> Result<&'a T> {
    a.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| OmniError::manifest_internal(format!("{what} has an unexpected type")))
}

fn strings<'a>(a: &'a ArrayRef, row: &str) -> Result<&'a StringArray> {
    downcast::<StringArray>(a, &format!("`{row}`'s String"))
}

fn int64s<'a>(a: &'a ArrayRef, row: &str) -> Result<&'a Int64Array> {
    downcast::<Int64Array>(a, &format!("`{row}`'s integer"))
}

fn float64s<'a>(a: &'a ArrayRef, row: &str) -> Result<&'a Float64Array> {
    downcast::<Float64Array>(a, &format!("`{row}`'s number"))
}

/// A String row on a GQ kernel, value by value.
fn map_strings(values: &StringArray, f: impl Fn(&str) -> &str) -> ArrayRef {
    let mut out = StringBuilder::with_capacity(values.len(), 0);
    for v in values.iter() {
        match v {
            Some(s) => out.append_value(f(s)),
            None => out.append_null(),
        }
    }
    Arc::new(out.finish())
}

/// Refuse a non-finite valid slot of a float row's result.
fn require_finite(row: &str, values: &ArrayRef) -> Result<()> {
    let finite = match values.data_type() {
        DataType::Float64 => float64s(values, row)?.iter().flatten().all(f64::is_finite),
        DataType::Float32 => downcast::<Float32Array>(values, row)?
            .iter()
            .flatten()
            .all(f32::is_finite),
        _ => true,
    };
    if finite {
        Ok(())
    } else {
        Err(refused(
            E3,
            format!("`{row}` produced a float that is not finite"),
        ))
    }
}

/// Refuse with `code` where `values` meets `bad` on a row whose every
/// argument is valid (`valid`, from [`all_valid`]): a row with a null
/// argument is null, never refused.
fn check_each<T: ArrowPrimitiveType>(
    values: &PrimitiveArray<T>,
    valid: &[bool],
    bad: impl Fn(T::Native) -> bool,
    code: QueryCode,
    message: &str,
) -> Result<()> {
    if values
        .iter()
        .zip(valid)
        .any(|(v, ok)| *ok && v.is_some_and(&bad))
    {
        Err(refused(code, message))
    } else {
        Ok(())
    }
}

/// `values` with the rows outside `valid` set to `0`, so a kernel never
/// refuses a value the row's null result discards.
fn zero_outside(values: &Int64Array, valid: &[bool]) -> ArrayRef {
    let kept: Int64Array = values
        .iter()
        .zip(valid)
        .map(|(v, ok)| if *ok { v } else { Some(0) })
        .collect();
    Arc::new(kept)
}

/// The `I64` argument `values` narrowed to `I32` on the rows of `valid`,
/// null elsewhere; a value outside `I32` there is `E5`.
fn i64_to_i32(values: &ArrayRef, valid: &[bool], row: &str, what: &str) -> Result<ArrayRef> {
    let values = int64s(values, row)?;
    let narrowed: Int32Array = values
        .iter()
        .zip(valid)
        .map(|(v, ok)| match ok {
            true => v.map(i32::try_from).transpose(),
            false => Ok(None),
        })
        .collect::<std::result::Result<_, _>>()
        .map_err(|_| refused(E5, format!("`{row}`'s {what} is outside I32")))?;
    Ok(Arc::new(narrowed))
}

fn scalar_of(ty: &ExprType) -> Option<ScalarType> {
    match ty {
        ExprType::Value { scalar, .. } => Some(*scalar),
        _ => None,
    }
}

/// Whether `func` is one of this module's rows.
pub(super) fn is_row(func: ScalarFunc) -> bool {
    !matches!(
        func,
        ScalarFunc::If
            | ScalarFunc::Coalesce
            | ScalarFunc::Concat
            | ScalarFunc::List
            | ScalarFunc::MatchTerms { .. }
    )
}

/// A break registry row over its evaluated arguments, `ty` the call's type.
pub(super) fn call(
    func: ScalarFunc,
    args: &[ArrayRef],
    arg_types: &[&ExprType],
    ty: &ExprType,
) -> Result<ArrayRef> {
    use datafusion::functions::{math, string, unicode};
    let name = func.name();
    let returns = ty
        .to_arrow()
        .ok_or_else(|| OmniError::manifest_internal(format!("`{name}` has no Arrow type")))?;
    let arg = |i: usize| -> Result<&ArrayRef> {
        args.get(i).ok_or_else(|| {
            OmniError::manifest_internal(format!("`{name}` is missing argument {i}"))
        })
    };
    // The rows whose every argument is valid: a domain check reads only
    // these, since every other row is null.
    let valid = all_valid(args);
    let out: ArrayRef = match func {
        ScalarFunc::Lower => udf(string::lower(), args, DataType::Utf8)?,
        ScalarFunc::Upper => udf(string::upper(), args, DataType::Utf8)?,
        // DataFusion strips U+0020 only; GQ strips Unicode whitespace.
        ScalarFunc::Trim => map_strings(strings(arg(0)?, name)?, |s| {
            s.trim_matches(char::is_whitespace)
        }),
        ScalarFunc::LTrim => map_strings(strings(arg(0)?, name)?, |s| {
            s.trim_start_matches(char::is_whitespace)
        }),
        ScalarFunc::RTrim => map_strings(strings(arg(0)?, name)?, |s| {
            s.trim_end_matches(char::is_whitespace)
        }),
        // Unicode scalar values, as DataFusion counts them; `Int32` for a
        // `Utf8` input, widened to the row's `I64`.
        ScalarFunc::Length => {
            let counted = udf(unicode::character_length(), args, DataType::Int32)?;
            arrow_cast::cast(&counted, &DataType::Int64).map_err(OmniError::arrow_internal)?
        }
        ScalarFunc::Left | ScalarFunc::Right => {
            let counts = int64s(arg(1)?, name)?;
            check_each(
                counts,
                &valid,
                |n| n < 0,
                E5,
                &format!("`{name}`'s count is negative"),
            )?;
            let f = if func == ScalarFunc::Left {
                unicode::left()
            } else {
                unicode::right()
            };
            udf(
                f,
                &[Arc::clone(arg(0)?), zero_outside(counts, &valid)],
                DataType::Utf8,
            )?
        }
        ScalarFunc::Substring => {
            let lengths = int64s(arg(2)?, name)?;
            check_each(
                lengths,
                &valid,
                |n| n < 0,
                E5,
                "`substring`'s length is negative",
            )?;
            // The smallest start overflows DataFusion's 1-based offset; any
            // start at or before `1 - length` gives the same empty window.
            let starts: Int64Array = int64s(arg(1)?, name)?.unary(|s| s.max(i64::MIN + 1));
            let starts: ArrayRef = Arc::new(starts);
            udf(
                unicode::substr(),
                &[Arc::clone(arg(0)?), starts, zero_outside(lengths, &valid)],
                DataType::Utf8,
            )?
        }
        ScalarFunc::Abs => {
            let x = arg(0)?;
            let smallest = match x.data_type() {
                DataType::Int32 => downcast::<Int32Array>(x, name)?
                    .iter()
                    .flatten()
                    .any(|v| v == i32::MIN),
                DataType::Int64 => int64s(x, name)?.iter().flatten().any(|v| v == i64::MIN),
                _ => false,
            };
            if smallest {
                return Err(refused(E1, "`abs` overflowed its integer type"));
            }
            udf(math::abs(), args, returns.clone())?
        }
        ScalarFunc::Floor | ScalarFunc::Ceil => match arg(0)?.data_type() {
            // An integer is its own floor and ceiling.
            DataType::Float32 | DataType::Float64 => {
                let f = if func == ScalarFunc::Floor {
                    math::floor()
                } else {
                    math::ceil()
                };
                udf(f, args, returns.clone())?
            }
            _ => Arc::clone(arg(0)?),
        },
        ScalarFunc::Round => round(arg(0)?, arg(1)?, &valid, &returns)?,
        ScalarFunc::Sqrt => {
            let x = float64s(arg(0)?, name)?;
            check_each(x, &valid, |v| v < 0.0, E5, "`sqrt` of a negative number")?;
            udf(math::sqrt(), args, DataType::Float64)?
        }
        ScalarFunc::Exp => udf(math::exp(), args, DataType::Float64)?,
        ScalarFunc::Ln | ScalarFunc::Log10 => {
            let x = float64s(arg(0)?, name)?;
            check_each(
                x,
                &valid,
                |v| v <= 0.0,
                E5,
                &format!("`{name}` of a number that is not positive"),
            )?;
            let f = if func == ScalarFunc::Ln {
                math::ln()
            } else {
                math::log10()
            };
            udf(f, args, DataType::Float64)?
        }
        ScalarFunc::Power => {
            // DataFusion refuses zero to a negative power with an untyped
            // error (PostgreSQL's rule); GQ's result is not finite (`E3`).
            let (bases, exponents) = (float64s(arg(0)?, name)?, float64s(arg(1)?, name)?);
            if bases
                .iter()
                .zip(exponents.iter())
                .zip(valid.iter())
                .any(|((b, e), ok)| *ok && b == Some(0.0) && e.is_some_and(|e| e < 0.0))
            {
                return Err(refused(E3, "`power` produced a float that is not finite"));
            }
            let r = udf(math::power(), args, DataType::Float64)?;
            if float64s(&r, name)?
                .iter()
                .zip(valid.iter().copied())
                .any(|(v, ok)| ok && v.is_some_and(f64::is_nan))
            {
                return Err(refused(
                    E5,
                    "`power` of a negative base to a fractional exponent",
                ));
            }
            r
        }
        // A list's elements: Arrow's length kernel reads the offsets.
        ScalarFunc::Size => {
            let lengths = arrow_string::length::length(arg(0)?.as_ref())
                .map_err(OmniError::arrow_internal)?;
            arrow_cast::cast(&lengths, &DataType::Int64).map_err(OmniError::arrow_internal)?
        }
        ScalarFunc::Cast => {
            let from = arg_types.first().and_then(|t| scalar_of(t));
            let to = scalar_of(ty);
            match (from, to) {
                (Some(from), Some(to)) => cast(arg(0)?, from, to, &returns)?,
                _ => {
                    return Err(OmniError::manifest_internal("`cast` without scalar types"));
                }
            }
        }
        ScalarFunc::If
        | ScalarFunc::Coalesce
        | ScalarFunc::Concat
        | ScalarFunc::List
        | ScalarFunc::MatchTerms { .. } => {
            return Err(OmniError::manifest_internal(format!(
                "`{name}` is not a break registry row"
            )));
        }
    };
    let out = null_where_any(out, args)?;
    if func.row_can_fail(
        &arg_types.iter().map(|t| (*t).clone()).collect::<Vec<_>>(),
        ty,
    ) && !matches!(func, ScalarFunc::Cast)
    {
        require_finite(name, &out)?;
    }
    Ok(out)
}

/// `round(x, digits)`: DataFusion's float kernel, `(x · 10^d).round() /
/// 10^d` in the float's own width, half away from zero; an integer on a GQ
/// kernel, exact.
fn round(x: &ArrayRef, digits: &ArrayRef, valid: &[bool], returns: &DataType) -> Result<ArrayRef> {
    let digits_valid = int64s(digits, "round")?;
    match x.data_type() {
        DataType::Float32 | DataType::Float64 => {
            let d = i64_to_i32(digits, valid, "round", "digits")?;
            // A null `digits` is null, not DataFusion's 0: the row is
            // nulled after the kernel.
            udf(
                datafusion::functions::math::round(),
                &[Arc::clone(x), d],
                returns.clone(),
            )
        }
        DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64 => {
            let wide = arrow_cast::cast(x, &DataType::Decimal128(38, 0))
                .map_err(OmniError::arrow_internal)?;
            let wide = downcast::<arrow_array::Decimal128Array>(&wide, "round")?;
            let mut out: Vec<Option<i128>> = Vec::with_capacity(wide.len());
            for (v, d) in wide.iter().zip(digits_valid.iter()) {
                out.push(match (v, d) {
                    (Some(v), Some(d)) => Some(
                        round_integer(v, d)
                            .ok_or_else(|| refused(E1, "`round` overflowed its integer type"))?,
                    ),
                    _ => None,
                });
            }
            let wide: ArrayRef = Arc::new(
                arrow_array::Decimal128Array::from(out)
                    .with_precision_and_scale(38, 0)
                    .map_err(OmniError::arrow_internal)?,
            );
            let options = arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            };
            arrow_cast::cast_with_options(&wide, returns, &options)
                .map_err(|_| refused(E1, "`round` overflowed its integer type"))
        }
        other => Err(OmniError::manifest_internal(format!(
            "`round` over {other}"
        ))),
    }
}

/// An integer rounded half away from zero at `digits` decimal digits;
/// `None` where the result leaves `i128`.
fn round_integer(v: i128, digits: i64) -> Option<i128> {
    if digits >= 0 {
        return Some(v);
    }
    let Some(p) = 10i128.checked_pow(u32::try_from(-digits).unwrap_or(u32::MAX).min(40)) else {
        return Some(0);
    };
    let (q, r) = (v / p, v % p);
    let away = if r.abs() * 2 >= p { v.signum() } else { 0 };
    (q + away).checked_mul(p)
}

/// Whether `s` is an integer GQ's `cast` reads: an optional sign and
/// ASCII digits.
fn integer_text(s: &str) -> bool {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Whether `s` is a decimal number GQ's `cast` reads: an optional sign,
/// digits with an optional fraction (or a fraction alone) and an optional
/// exponent; never `inf` or `NaN`.
fn float_text(s: &str) -> bool {
    let body = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], Some(&body[at + 1..])),
        None => (body, None),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((w, f)) => (w, Some(f)),
        None => (mantissa, None),
    };
    let all_digits = |t: &str| t.bytes().all(|b| b.is_ascii_digit());
    let mantissa_ok = all_digits(whole)
        && fraction.is_none_or(all_digits)
        && !(whole.is_empty() && fraction.is_none_or(str::is_empty));
    let exponent_ok = exponent.is_none_or(|e| {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        !e.is_empty() && all_digits(e)
    });
    mantissa_ok && exponent_ok
}

fn not_converted(from: ScalarType, to: ScalarType) -> OmniError {
    refused(
        E6,
        format!("`cast` met a {from:?} value that does not convert to {to:?}"),
    )
}

/// `cast(x, to)` from a `from` value.
fn cast(x: &ArrayRef, from: ScalarType, to: ScalarType, returns: &DataType) -> Result<ArrayRef> {
    use ScalarType as S;
    let strict = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    let arrow = |x: &ArrayRef| {
        arrow_cast::cast_with_options(x, returns, &strict).map_err(|_| not_converted(from, to))
    };
    if from == to {
        return Ok(Arc::clone(x));
    }
    let numeric = |s: S| s.is_numeric();
    let integer = |s: S| matches!(s, S::I32 | S::I64 | S::U32 | S::U64);
    match (from, to) {
        (S::String, S::Bool) => {
            let text = strings(x, "cast")?;
            let parsed: BooleanArray = text
                .iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(s) if s.eq_ignore_ascii_case("true") => Ok(Some(true)),
                    Some(s) if s.eq_ignore_ascii_case("false") => Ok(Some(false)),
                    Some(_) => Err(not_converted(from, to)),
                })
                .collect::<Result<_>>()?;
            Ok(Arc::new(parsed))
        }
        (S::String, t) if numeric(t) => {
            let text = strings(x, "cast")?;
            let reads = if integer(t) { integer_text } else { float_text };
            if text.iter().flatten().any(|s| !reads(s)) {
                return Err(not_converted(from, to));
            }
            let out = arrow(x)?;
            require_converted_finite(&out, from, to)?;
            Ok(out)
        }
        // The one literal parse the checker uses.
        (S::String, S::Date) => {
            let text = strings(x, "cast")?;
            let days: Date32Array = text
                .iter()
                .map(|v| {
                    v.map(|s| {
                        omnigraph_compiler::date_literal_days(s)
                            .map_err(|_| not_converted(from, to))
                    })
                    .transpose()
                })
                .collect::<Result<_>>()?;
            Ok(Arc::new(days))
        }
        (S::String, S::DateTime) => {
            let text = strings(x, "cast")?;
            let millis: Date64Array = text
                .iter()
                .map(|v| {
                    v.map(|s| {
                        omnigraph_compiler::check_datetime_literal(s)
                            .and_then(|()| omnigraph_compiler::datetime_literal_millis(s))
                            .map_err(|_| not_converted(from, to))
                    })
                    .transpose()
                })
                .collect::<Result<_>>()?;
            Ok(Arc::new(millis))
        }
        // An instant's day, floored: Arrow truncates toward zero.
        (S::DateTime, S::Date) => {
            let millis = downcast::<Date64Array>(x, "cast")?;
            let days: Date32Array = millis
                .iter()
                .map(|v| {
                    v.map(|ms| {
                        i32::try_from(ms.div_euclid(86_400_000))
                            .map_err(|_| not_converted(from, to))
                    })
                    .transpose()
                })
                .collect::<Result<_>>()?;
            Ok(Arc::new(days))
        }
        (S::Date, S::DateTime) | (_, S::String) | (S::Bool, _) | (_, S::Bool) => arrow(x),
        (a, b) if numeric(a) && numeric(b) => {
            let out = arrow(x)?;
            require_converted_finite(&out, from, to)?;
            Ok(out)
        }
        _ => Err(OmniError::manifest_internal(format!(
            "`cast` from {from:?} to {to:?}"
        ))),
    }
}

/// A float `cast` made infinite (a string past `F64`, an `F64` past
/// `F32`) does not convert.
fn require_converted_finite(out: &ArrayRef, from: ScalarType, to: ScalarType) -> Result<()> {
    let finite = match out.data_type() {
        DataType::Float64 => float64s(out, "cast")?.iter().flatten().all(f64::is_finite),
        DataType::Float32 => downcast::<Float32Array>(out, "cast")?
            .iter()
            .flatten()
            .all(f32::is_finite),
        _ => true,
    };
    if finite {
        Ok(())
    } else {
        Err(not_converted(from, to))
    }
}
