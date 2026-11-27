//! The scalar registry rows of the break (the GQ 3 RFC, section 2): the
//! string, numeric, list and `cast` rows, evaluated as the RFC states them.
//! The engine computes each with an Arrow or DataFusion kernel, or a GQ
//! kernel where none matches; this module is the independent statement
//! those kernels are compared with.
//!
//! Every row is null where any argument is null. A row that can fail keeps
//! GQ's error rules: `E1` an integer that overflows its type, `E3` a float
//! result that is not finite, `E5` an argument outside the row's domain
//! (the root of a negative, the logarithm of a non-positive, a negative
//! count or length, `round`'s digits outside `I32`), `E6` a value `cast`
//! does not convert.

use omnigraph_compiler::ir::expr::ScalarFunc;
use omnigraph_compiler::types::{ExprType, ScalarType};
use omnigraph_compiler::{date_literal_days, datetime_literal_millis};

use crate::{R, Refusal, Value, canonical_datetime};

fn scalar(t: &ExprType) -> Option<ScalarType> {
    match t {
        ExprType::Value { scalar, .. } => Some(*scalar),
        _ => None,
    }
}

fn refused<T>(code: &'static str, what: impl Into<String>) -> R<T> {
    Err(Refusal::Runtime(code, what.into()))
}

fn text(v: &Value) -> R<&str> {
    match v {
        Value::Str(s) => Ok(s),
        other => Err(Refusal::Input(format!("a String row read {other:?}"))),
    }
}

fn int(v: &Value) -> R<i128> {
    match v {
        Value::Int(i) => Ok(*i),
        other => Err(Refusal::Input(format!(
            "an integer argument read {other:?}"
        ))),
    }
}

fn float(v: &Value) -> R<f64> {
    match v {
        Value::Float(f) => Ok(*f),
        Value::Int(i) => Ok(*i as f64),
        other => Err(Refusal::Input(format!("a number argument read {other:?}"))),
    }
}

/// The inclusive range of an integer type.
fn range(s: ScalarType) -> Option<(i128, i128)> {
    Some(match s {
        ScalarType::I32 => (i128::from(i32::MIN), i128::from(i32::MAX)),
        ScalarType::I64 => (i128::from(i64::MIN), i128::from(i64::MAX)),
        ScalarType::U32 => (0, i128::from(u32::MAX)),
        ScalarType::U64 => (0, i128::from(u64::MAX)),
        _ => return None,
    })
}

fn in_type(i: i128, s: ScalarType) -> bool {
    range(s).is_some_and(|(lo, hi)| lo <= i && i <= hi)
}

fn finite(code: &'static str, row: &str, f: f64) -> R<Value> {
    if f.is_finite() {
        Ok(Value::Float(f))
    } else {
        refused(code, format!("`{row}` produced a float that is not finite"))
    }
}

/// One row over its evaluated arguments, `args` their stored types and
/// `ty` the call's.
pub(crate) fn row(
    func: ScalarFunc,
    vals: Vec<Value>,
    args: &[&ExprType],
    ty: &ExprType,
) -> R<Value> {
    if vals.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    // A number is a value of its argument's declared type: an integer
    // literal or parameter typed as a float (`cast(0, F64)`, a `F64`
    // parameter bound to `3`) is that float, so `abs` and `floor` return a
    // float and `cast` to String writes `0.0`.
    let vals: Vec<Value> = vals
        .into_iter()
        .zip(
            args.iter()
                .map(|t| scalar(t))
                .chain(std::iter::repeat(None)),
        )
        .map(|(v, t)| match (v, t) {
            (Value::Int(i), Some(ScalarType::F32 | ScalarType::F64)) => Value::Float(i as f64),
            (v, _) => v,
        })
        .collect();
    let out = scalar(ty);
    let single = out == Some(ScalarType::F32);
    Ok(match func {
        ScalarFunc::Lower => Value::Str(text(&vals[0])?.to_lowercase()),
        ScalarFunc::Upper => Value::Str(text(&vals[0])?.to_uppercase()),
        // Unicode whitespace, the `White_Space` property.
        ScalarFunc::Trim => Value::Str(text(&vals[0])?.trim_matches(char::is_whitespace).into()),
        ScalarFunc::LTrim => Value::Str(
            text(&vals[0])?
                .trim_start_matches(char::is_whitespace)
                .into(),
        ),
        ScalarFunc::RTrim => {
            Value::Str(text(&vals[0])?.trim_end_matches(char::is_whitespace).into())
        }
        ScalarFunc::Length => Value::Int(text(&vals[0])?.chars().count() as i128),
        ScalarFunc::Left | ScalarFunc::Right => {
            let chars: Vec<char> = text(&vals[0])?.chars().collect();
            let n = int(&vals[1])?;
            if n < 0 {
                return refused("E5", format!("`{}`'s count is negative", func.name()));
            }
            let n = (n as usize).min(chars.len());
            let kept = if func == ScalarFunc::Left {
                &chars[..n]
            } else {
                &chars[chars.len() - n..]
            };
            Value::Str(kept.iter().collect())
        }
        // The characters at 1-based positions `start .. start + len`,
        // clipped to the String.
        ScalarFunc::Substring => {
            let chars: Vec<char> = text(&vals[0])?.chars().collect();
            let (start, len) = (int(&vals[1])?, int(&vals[2])?);
            if len < 0 {
                return refused("E5", "`substring`'s length is negative");
            }
            let count = chars.len() as i128;
            let from = (start - 1).clamp(0, count);
            let to = (start - 1 + len).clamp(0, count);
            Value::Str(chars[from as usize..to.max(from) as usize].iter().collect())
        }
        ScalarFunc::Abs => match &vals[0] {
            Value::Int(i) => {
                let a = i.abs();
                if !out.is_some_and(|s| in_type(a, s)) {
                    return refused("E1", "`abs` overflowed its integer type");
                }
                Value::Int(a)
            }
            other => Value::Float(float(other)?.abs()),
        },
        ScalarFunc::Floor | ScalarFunc::Ceil => match &vals[0] {
            Value::Int(i) => Value::Int(*i),
            other => {
                let f = float(other)?;
                Value::Float(if func == ScalarFunc::Floor {
                    f.floor()
                } else {
                    f.ceil()
                })
            }
        },
        // Half away from zero at `digits` decimal digits: an integer
        // exactly, a float as `(x · 10^d).round() / 10^d` in its own width.
        ScalarFunc::Round => {
            let digits = int(&vals[1])?;
            match &vals[0] {
                Value::Int(i) => {
                    if digits >= 0 {
                        Value::Int(*i)
                    } else {
                        let rounded = match 10i128.checked_pow((-digits).min(40) as u32) {
                            Some(p) => {
                                let (q, r) = (i / p, i % p);
                                let away = if r.abs() * 2 >= p { i.signum() } else { 0 };
                                (q + away).checked_mul(p)
                            }
                            None => Some(0),
                        };
                        match rounded {
                            Some(v) if out.is_some_and(|s| in_type(v, s)) => Value::Int(v),
                            _ => return refused("E1", "`round` overflowed its integer type"),
                        }
                    }
                }
                other => {
                    let Ok(d) = i32::try_from(digits) else {
                        return refused("E5", "`round`'s digits are outside I32");
                    };
                    let x = float(other)?;
                    let factor = 10_f64.powi(d);
                    let r = if single {
                        let factor = factor as f32;
                        f64::from(((x as f32) * factor).round() / factor)
                    } else {
                        (x * factor).round() / factor
                    };
                    finite("E3", "round", r)?
                }
            }
        }
        ScalarFunc::Sqrt => {
            let x = float(&vals[0])?;
            if x < 0.0 {
                return refused("E5", "`sqrt` of a negative number");
            }
            finite("E3", "sqrt", x.sqrt())?
        }
        ScalarFunc::Exp => finite("E3", "exp", float(&vals[0])?.exp())?,
        ScalarFunc::Ln | ScalarFunc::Log10 => {
            let x = float(&vals[0])?;
            if x <= 0.0 {
                return refused(
                    "E5",
                    format!("`{}` of a number that is not positive", func.name()),
                );
            }
            let r = if func == ScalarFunc::Ln {
                x.ln()
            } else {
                x.log10()
            };
            finite("E3", func.name(), r)?
        }
        ScalarFunc::Power => {
            let r = float(&vals[0])?.powf(float(&vals[1])?);
            if r.is_nan() {
                return refused("E5", "`power` of a negative base to a fractional exponent");
            }
            finite("E3", "power", r)?
        }
        ScalarFunc::Size => match &vals[0] {
            Value::List(items) => Value::Int(items.len() as i128),
            other => return Err(Refusal::Input(format!("`size` read {other:?}"))),
        },
        ScalarFunc::Cast => {
            let from = args.first().and_then(|t| scalar(t));
            match (from, out) {
                (Some(from), Some(to)) => cast(vals.into_iter().next().unwrap(), from, to)?,
                _ => return Err(Refusal::Input("`cast` without scalar types".into())),
            }
        }
        ScalarFunc::If
        | ScalarFunc::Coalesce
        | ScalarFunc::Concat
        | ScalarFunc::List
        | ScalarFunc::MatchTerms { .. } => {
            return Err(Refusal::Input(format!(
                "`{}` is not a break registry row",
                func.name()
            )));
        }
    })
}

fn not_converted<T>(what: impl std::fmt::Display, to: ScalarType) -> R<T> {
    refused("E6", format!("`cast` does not convert {what} to {to:?}"))
}

/// The civil date `days` after 1970-01-01, `YYYY-MM-DD` (H. Hinnant's
/// algorithm).
fn civil(days: i64) -> String {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// The shortest decimal that reads back to `f`, laid out as Arrow's
/// display writes a float (the `ryu` layout): positional between
/// `10^-5` and `10^16` (`10^13` for `F32`), else scientific, with `.0` on a
/// whole number.
fn shortest(f: f64, single: bool) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f < 0.0 { "-inf" } else { "inf" }.into();
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    // `{:e}` gives the shortest digits: `d.ddde±x`.
    let sci = if single {
        format!("{:e}", f as f32)
    } else {
        format!("{f:e}")
    };
    let (sign, sci) = match sci.strip_prefix('-') {
        Some(rest) => ("-", rest.to_string()),
        None => ("", sci),
    };
    let (mantissa, exponent) = sci.split_once('e').expect("`{:e}` writes an exponent");
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let length = digits.len() as i64;
    let kk = exponent.parse::<i64>().expect("an integer exponent") + 1;
    let k = kk - length;
    let limit = if single { 13 } else { 16 };
    let body = if 0 <= k && kk <= limit {
        format!("{digits}{}.0", "0".repeat(k as usize))
    } else if 0 < kk && kk <= limit {
        format!("{}.{}", &digits[..kk as usize], &digits[kk as usize..])
    } else if -5 < kk && kk <= 0 {
        format!("0.{}{digits}", "0".repeat((-kk) as usize))
    } else if length == 1 {
        format!("{digits}e{}", kk - 1)
    } else {
        format!("{}.{}e{}", &digits[..1], &digits[1..], kk - 1)
    };
    format!("{sign}{body}")
}

/// Whether `s` is an integer GQ's `cast` reads: an optional sign and
/// ASCII digits.
fn integer_text(s: &str) -> bool {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Whether `s` is a decimal number GQ's `cast` reads: an optional sign,
/// digits with an optional fraction (or a fraction alone) and an optional
/// exponent.
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

/// `cast(v, to)` from a `from` value: GQ's conversion rules.
fn cast(v: Value, from: ScalarType, to: ScalarType) -> R<Value> {
    use ScalarType::*;
    let integer = |t: ScalarType| range(t).is_some();
    Ok(match (v, from, to) {
        (v, a, b) if a == b => v,
        // To String: an integer in decimal, a float in its shortest form,
        // a Bool as `true` or `false`, a Date and a DateTime as their
        // literals read back.
        (Value::Int(i), _, String) => Value::Str(i.to_string()),
        (Value::Float(f), F32, String) => Value::Str(shortest(f, true)),
        (Value::Float(f), _, String) => Value::Str(shortest(f, false)),
        (Value::Bool(b), _, String) => Value::Str(b.to_string()),
        (Value::Str(s), Date, String) => Value::Str(civil(i64::from(
            date_literal_days(&s).map_err(Refusal::Input)?,
        ))),
        (Value::Str(s), _, String) => Value::Str(s),
        // From String: the text of a literal of the target type.
        (Value::Str(s), String, t) if integer(t) => {
            if !integer_text(&s) {
                return not_converted(format!("{s:?}"), t);
            }
            match s.parse::<i128>() {
                Ok(i) if in_type(i, t) => Value::Int(i),
                _ => return not_converted(format!("{s:?}"), t),
            }
        }
        (Value::Str(s), String, F32) => match (float_text(&s), s.parse::<f32>()) {
            (true, Ok(f)) if f.is_finite() => Value::Float(f64::from(f)),
            _ => return not_converted(format!("{s:?}"), F32),
        },
        (Value::Str(s), String, F64) => match (float_text(&s), s.parse::<f64>()) {
            (true, Ok(f)) if f.is_finite() => Value::Float(f),
            _ => return not_converted(format!("{s:?}"), F64),
        },
        (Value::Str(s), String, Bool) => {
            if s.eq_ignore_ascii_case("true") {
                Value::Bool(true)
            } else if s.eq_ignore_ascii_case("false") {
                Value::Bool(false)
            } else {
                return not_converted(format!("{s:?}"), Bool);
            }
        }
        (Value::Str(s), String, Date) => match date_literal_days(&s) {
            Ok(days) => Value::Str(civil(i64::from(days))),
            Err(_) => return not_converted(format!("{s:?}"), Date),
        },
        (Value::Str(s), String, DateTime) => {
            match (
                omnigraph_compiler::check_datetime_literal(&s),
                datetime_literal_millis(&s),
            ) {
                (Ok(()), Ok(_)) => match canonical_datetime(&s) {
                    Some(c) => Value::Str(c),
                    None => return not_converted(format!("{s:?}"), DateTime),
                },
                _ => return not_converted(format!("{s:?}"), DateTime),
            }
        }
        // Between Date and DateTime: midnight, and the day an instant falls
        // on.
        (Value::Str(s), Date, DateTime) => {
            let days = date_literal_days(&s).map_err(Refusal::Input)?;
            Value::Str(format!("{}T00:00:00", civil(i64::from(days))))
        }
        (Value::Str(s), DateTime, Date) => {
            let millis = datetime_literal_millis(&s).map_err(Refusal::Input)?;
            Value::Str(civil(millis.div_euclid(86_400_000)))
        }
        // Bool and integers: 1 and 0, and non-zero.
        (Value::Bool(b), Bool, _) => Value::Int(i128::from(b)),
        (Value::Int(i), _, Bool) => Value::Bool(i != 0),
        // Between numbers: an integer exactly where it fits, a float
        // truncated toward zero where it fits, rounded to nearest into a
        // float.
        (Value::Int(i), _, t) if integer(t) => {
            if in_type(i, t) {
                Value::Int(i)
            } else {
                return not_converted(i, t);
            }
        }
        (Value::Int(i), _, F32) => Value::Float(f64::from(i as f32)),
        (Value::Int(i), _, F64) => Value::Float(i as f64),
        (Value::Float(f), _, t) if integer(t) => {
            let truncated = f.trunc();
            if !f.is_finite() || truncated.abs() >= 2f64.powi(64) || !in_type(truncated as i128, t)
            {
                return not_converted(f, t);
            }
            Value::Int(truncated as i128)
        }
        (Value::Float(f), _, F32) => {
            let narrowed = f as f32;
            if f.is_finite() && !narrowed.is_finite() {
                return not_converted(f, F32);
            }
            Value::Float(f64::from(narrowed))
        }
        (Value::Float(f), _, F64) => Value::Float(f),
        (v, _, t) => {
            return Err(Refusal::Input(format!(
                "`cast` of {v:?} from {from:?} to {t:?}"
            )));
        }
    })
}
