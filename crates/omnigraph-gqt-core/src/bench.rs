//! The `--- bench` section: how long a measuring host runs the file's
//! `--- concurrent measured` block and at which values of one swept name. A
//! host with a scripted clock runs the block once and reads only the sweep,
//! whose first value it binds.
use std::collections::BTreeMap;

use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer};

use crate::diagnostic::{CaseError, CaseErrorCode};
use crate::yaml::yaml;

pub const MAX_WARMUP_SECONDS: u32 = 3600;
pub const MAX_DURATION_SECONDS: u32 = 3600;
pub const MAX_REPETITIONS: u32 = 10_000;
pub const MAX_SWEEP_VALUES: usize = 16;
pub const MAX_SWEEP_VALUE: u64 = 4096;

const FIX: &str = "write `duration_seconds:` (1 to 3600), optionally `warmup_seconds:`, `repetitions:` and `sweep: { <NAME>: [<n>, ...] }`";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchSection {
    pub warmup_seconds: u32,
    pub duration_seconds: u32,
    pub repetitions: Option<u32>,
    pub sweep: Option<Sweep>,
}

/// One swept name and its values in rising order: `${<name>}` anywhere in
/// the file outside the section reads as one of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sweep {
    pub name: String,
    pub values: Vec<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    #[serde(default)]
    warmup_seconds: u32,
    duration_seconds: u32,
    #[serde(default)]
    repetitions: Option<u32>,
    #[serde(default)]
    sweep: Option<BTreeMap<String, Vec<u64>>>,
    #[serde(default, deserialize_with = "key_is_written")]
    rate: bool,
}

/// True for a key the section writes, whatever its value: `rate: ~` and a
/// bare `rate:` name a rate as `rate: 100` does.
fn key_is_written<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    IgnoredAny::deserialize(deserializer)?;
    Ok(true)
}

fn refusal(message: impl Into<String>) -> CaseError {
    CaseError::new(CaseErrorCode::BodyShape, message).fix(FIX)
}

fn is_swept_name(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= 16
        && chars.next().is_some_and(|c| c.is_ascii_uppercase())
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

pub(crate) fn parse(body: &str) -> Result<BenchSection, CaseError> {
    let wire: Wire = yaml(body, "bench section")
        .map_err(|e| CaseError::from(e).code(CaseErrorCode::BodyShape).fix(FIX))?;
    if wire.rate {
        return Err(CaseError::new(
            CaseErrorCode::BodyShape,
            "`rate:` belongs to an open-loop driver, which no host runs yet; a measured block runs closed-loop, each worker issuing its next statement when the last one is acknowledged",
        )
        .fix("drop the `rate:` line"));
    }
    if wire.warmup_seconds > MAX_WARMUP_SECONDS
        || !(1..=MAX_DURATION_SECONDS).contains(&wire.duration_seconds)
    {
        return Err(refusal(format!(
            "`warmup_seconds` is 0 to {MAX_WARMUP_SECONDS} and `duration_seconds` 1 to {MAX_DURATION_SECONDS}, got {} and {}",
            wire.warmup_seconds, wire.duration_seconds
        )));
    }
    if wire
        .repetitions
        .is_some_and(|n| !(1..=MAX_REPETITIONS).contains(&n))
    {
        return Err(refusal(format!("`repetitions` is 1 to {MAX_REPETITIONS}")));
    }
    let sweep = match wire.sweep {
        None => None,
        Some(names) => {
            let mut names = names.into_iter();
            let (Some((name, values)), None) = (names.next(), names.next()) else {
                return Err(refusal("`sweep:` binds exactly one name"));
            };
            if !is_swept_name(&name) {
                return Err(refusal(format!(
                    "a swept name is upper case, `[A-Z][A-Z0-9_]*` of at most 16 characters, so it never meets a loop variable or a bound commit name; got `{name}`"
                )));
            }
            if values.is_empty()
                || values.len() > MAX_SWEEP_VALUES
                || values[0] == 0
                || values.windows(2).any(|pair| pair[0] >= pair[1])
                || values.last().is_some_and(|last| *last > MAX_SWEEP_VALUE)
            {
                return Err(refusal(format!(
                    "`sweep: {{ {name}: [...] }}` lists 1 to {MAX_SWEEP_VALUES} rising values between 1 and {MAX_SWEEP_VALUE}"
                )));
            }
            Some(Sweep { name, values })
        }
    };
    Ok(BenchSection {
        warmup_seconds: wire.warmup_seconds,
        duration_seconds: wire.duration_seconds,
        repetitions: wire.repetitions,
        sweep,
    })
}

/// The text with every `${<name>}` outside the section replaced by the bound
/// value, `swept` or else the sweep's first, and that value; `None` when the
/// text sweeps nothing or its section is one the parser will refuse.
pub(crate) fn bind(text: &str, swept: Option<u64>) -> Result<Option<(String, u64)>, CaseError> {
    let lines: Vec<&str> = text.lines().collect();
    let (_, sections) = crate::split_sections(&lines);
    let section = sections.iter().find(|section| section.name == "bench");
    let sweep = section
        .and_then(|section| parse(&crate::yaml_body(&section.body)).ok())
        .and_then(|bench| bench.sweep);
    let (Some(section), Some(sweep)) = (section, sweep) else {
        return match swept {
            None => Ok(None),
            Some(value) => Err(CaseError::new(
                CaseErrorCode::BodyShape,
                format!("the case sweeps no name, so it cannot be bound at {value}"),
            )
            .fix("bind no value, or give the case a `sweep:` in its `--- bench` section")),
        };
    };
    let value = match swept {
        None => sweep.values[0],
        Some(value) if sweep.values.contains(&value) => value,
        Some(value) => {
            return Err(CaseError::new(
                CaseErrorCode::BodyShape,
                format!(
                    "{} = {value} is not a value of the sweep {:?}",
                    sweep.name, sweep.values
                ),
            )
            .fix("bind one of the values the `sweep:` lists"));
        }
    };
    let token = format!("${{{}}}", sweep.name);
    let rendered = value.to_string();
    let last = section
        .body
        .last()
        .map_or(section.header_line, |(index, _)| *index);
    let mut bound = String::with_capacity(text.len());
    for (index, line) in lines.iter().enumerate() {
        if (section.header_line..=last).contains(&index) {
            bound.push_str(line);
        } else {
            bound.push_str(&line.replace(&token, &rendered));
        }
        bound.push('\n');
    }
    Ok(Some((bound, value)))
}
