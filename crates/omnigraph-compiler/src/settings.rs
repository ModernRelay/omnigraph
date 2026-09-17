//! Session settings: the one definition of every setting a GQ file can
//! `set`, `reset` or `show`, the typed value struct a session carries, and
//! the reader of the process environment the two process doors call once.
//!
//! [`DEFINITIONS`] is the list; [`SettingId`] has one variant and
//! [`SessionSettings`] one field per row, and the unit tests hold the three
//! equal in both directions. [`SessionSettings`] carries one further field,
//! `traversal`, which no row names: it is a harness knob reached only through
//! [`SessionSettings::with_traversal`]. Scope is not checked here: whether a `process`
//! setting may be set depends on the door, so the door calls
//! [`SettingId::refuse_from_request`].

use std::fmt;

use serde::{Deserialize, Serialize};

/// The legal values of a setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingKind {
    /// One of the listed identifiers, in GQ spelling.
    Enum(&'static [&'static str]),
    /// An integer from `min` up to `max` inclusive; `None` is unbounded.
    Integer { min: i64, max: Option<i64> },
}

/// Who may set a setting: any caller (`request`), or only the process that
/// hosts the engine (`process`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingScope {
    Request,
    Process,
}

impl SettingScope {
    pub fn as_str(self) -> &'static str {
        match self {
            SettingScope::Request => "request",
            SettingScope::Process => "process",
        }
    }
}

/// One row of the settings definition.
#[derive(Debug)]
pub struct SettingSpec {
    pub name: &'static str,
    pub kind: SettingKind,
    /// The default in GQ spelling, the `default` column of `show`.
    pub default: &'static str,
    pub scope: SettingScope,
    /// The environment variable the process doors read the default from.
    pub env: &'static str,
    /// What the setting chooses, the sentence the user docs table and the
    /// OpenAPI schema print.
    pub doc: &'static str,
}

pub const DEFINITIONS: &[SettingSpec] = &[
    SettingSpec {
        name: "rrf_plan",
        kind: SettingKind::Enum(&["auto", "force_prefilter", "force_postfilter"]),
        default: "auto",
        scope: SettingScope::Process,
        env: "OMNIGRAPH_RRF_PLAN",
        doc: "the reciprocal rank fusion plan on a traversal-constrained `nearest`, for diagnosis",
    },
    SettingSpec {
        name: "merge_lineage",
        kind: SettingKind::Enum(&["off", "on", "verify"]),
        default: "on",
        scope: SettingScope::Request,
        env: "OMNIGRAPH_MERGE_LINEAGE",
        doc: "how a merge finds the entities it classifies: the full-scan walk, the lineage path, or both compared",
    },
    SettingSpec {
        name: "ann_nprobes",
        kind: SettingKind::Integer { min: 0, max: None },
        default: "20",
        scope: SettingScope::Process,
        env: "OMNIGRAPH_ANN_NPROBES",
        doc: "the partition cap per index delta of a `nearest` scan; `0` is no cap",
    },
    SettingSpec {
        name: "stage_write_concurrency",
        kind: SettingKind::Integer {
            min: 1,
            max: Some(64),
        },
        default: "8",
        scope: SettingScope::Process,
        env: "OMNIGRAPH_LOAD_CONCURRENCY",
        doc: "the width of the staged-write fan-out for `load` and `mutate`",
    },
];

/// One variant per [`DEFINITIONS`] row, in the table's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SettingId {
    RrfPlan,
    MergeLineage,
    AnnNprobes,
    StageWriteConcurrency,
}

impl SettingId {
    /// Every setting, in definition order.
    pub const ALL: [SettingId; DEFINITIONS.len()] = [
        SettingId::RrfPlan,
        SettingId::MergeLineage,
        SettingId::AnnNprobes,
        SettingId::StageWriteConcurrency,
    ];

    /// The setting a name denotes.
    ///
    /// # Errors
    ///
    /// [`SessionSettingsError::UnknownSetting`] for a name outside the
    /// definition.
    pub fn parse(name: &str) -> Result<SettingId, SessionSettingsError> {
        SettingId::ALL
            .into_iter()
            .find(|id| id.name() == name)
            .ok_or_else(|| SessionSettingsError::UnknownSetting {
                name: name.to_string(),
            })
    }

    pub fn spec(self) -> &'static SettingSpec {
        &DEFINITIONS[self as usize]
    }

    pub fn name(self) -> &'static str {
        self.spec().name
    }

    pub fn scope(self) -> SettingScope {
        self.spec().scope
    }

    /// The `process` scope rule: a remote caller may set only a `request`
    /// setting.
    ///
    /// # Errors
    ///
    /// [`SessionSettingsError::ProcessScope`] for a `process` setting.
    pub fn refuse_from_request(self) -> Result<(), SessionSettingsError> {
        match self.scope() {
            SettingScope::Request => Ok(()),
            SettingScope::Process => Err(SessionSettingsError::ProcessScope { setting: self }),
        }
    }

    /// The pair a `name=value` spelling denotes, the name resolved and the
    /// value validated against the row before either is returned: the one
    /// door a `--set` flag or a `set=` query parameter goes through.
    ///
    /// # Errors
    ///
    /// An unknown name, a value of the wrong kind, an unknown enum value or an
    /// integer outside its range.
    pub fn parse_assignment(
        name: &str,
        value: &str,
    ) -> Result<(SettingId, SettingValue), SessionSettingsError> {
        let id = SettingId::parse(name)?;
        let value = SettingValue::from_spelling(value);
        SessionSettings::default().set(id, &value)?;
        Ok((id, value))
    }
}

/// The value after `=` in a `set` line: a bare integer, a bare identifier,
/// or a string literal. The definition's kind decides which is legal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingValue {
    Integer(i64),
    Ident(String),
    Str(String),
}

impl SettingValue {
    /// The value a `--set name=value` flag or an environment variable spells,
    /// the text trimmed first: a digit run under an optional `-` or `+` sign
    /// is an integer, a quoted text is a string, anything else an identifier.
    /// A digit run too large for `i64` stays a string, which the integer rows
    /// refuse as out of range so the message names the spelling.
    pub fn from_spelling(text: &str) -> SettingValue {
        let text = text.trim();
        if let Some(inner) = text
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
        {
            return SettingValue::Str(inner.to_string());
        }
        if is_digit_run(text) {
            if let Ok(value) = text.parse::<i64>() {
                return SettingValue::Integer(value);
            }
            return SettingValue::Str(text.to_string());
        }
        SettingValue::Ident(text.to_string())
    }

    fn kind_word(&self) -> &'static str {
        match self {
            SettingValue::Integer(_) => "an integer",
            SettingValue::Ident(_) => "an identifier",
            SettingValue::Str(_) => "a string",
        }
    }
}

impl fmt::Display for SettingValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SettingValue::Integer(value) => write!(f, "{value}"),
            SettingValue::Ident(text) | SettingValue::Str(text) => f.write_str(text),
        }
    }
}

/// Which multi-hop traversal path an Expand runs. No definition row names it:
/// see [`SessionSettings::with_traversal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Traversal {
    Auto,
    Indexed,
    Csr,
}

/// `rrf_plan`: the reciprocal rank fusion plan on a traversal-constrained
/// `nearest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RrfPlan {
    Auto,
    ForcePrefilter,
    ForcePostfilter,
}

/// `merge_lineage`: how a merge finds its candidate rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeLineage {
    Off,
    On,
    Verify,
}

macro_rules! enum_spelling {
    ($ty:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        impl $ty {
            pub fn as_str(self) -> &'static str {
                match self {
                    $($ty::$variant => $text,)+
                }
            }

            pub fn from_spelling(text: &str) -> Option<Self> {
                match text {
                    $($text => Some($ty::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

enum_spelling!(Traversal { Auto => "auto", Indexed => "indexed", Csr => "csr" });
enum_spelling!(RrfPlan { Auto => "auto", ForcePrefilter => "force_prefilter", ForcePostfilter => "force_postfilter" });
enum_spelling!(MergeLineage { Off => "off", On => "on", Verify => "verify" });

/// Where a setting's current value came from, the `source` column of `show`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Default,
    Env,
    Request,
    File,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Default => "default",
            Source::Env => "env",
            Source::Request => "request",
            Source::File => "file",
        }
    }
}

/// One [`Source`] per definition row, indexed by `SettingId as usize`.
pub type Sources = [Source; DEFINITIONS.len()];

/// The refusal of a stored `.gq` source whose file opens with a settings
/// prefix: a stored query runs under the process defaults.
pub const STORED_QUERY_CARRIES_NO_SETTINGS: &str =
    "a stored query carries no settings; it runs under the process defaults";

/// Every setting's value, validated against [`DEFINITIONS`] by every
/// constructor and mutator, so no field ever holds a value outside its row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSettings {
    traversal: Traversal,
    rrf_plan: RrfPlan,
    merge_lineage: MergeLineage,
    ann_nprobes: Option<usize>,
    stage_write_concurrency: usize,
}

impl Default for SessionSettings {
    /// The definition's defaults; `merge_lineage` is `verify` in a debug
    /// build, `on` otherwise.
    fn default() -> Self {
        let mut settings = SessionSettings {
            traversal: Traversal::Auto,
            rrf_plan: RrfPlan::Auto,
            merge_lineage: MergeLineage::On,
            ann_nprobes: None,
            stage_write_concurrency: 1,
        };
        for id in SettingId::ALL {
            settings
                .set(id, &SettingValue::from_spelling(id.spec().default))
                .expect("invariant: every DEFINITIONS default parses under its own row");
        }
        if cfg!(debug_assertions) {
            settings.merge_lineage = MergeLineage::Verify;
        }
        settings
    }
}

impl SessionSettings {
    /// The defaults, then each `(name, value)` pair in GQ spelling.
    ///
    /// # Errors
    ///
    /// The first pair the definition refuses.
    pub fn try_from_values(values: &[(&str, &str)]) -> Result<Self, SessionSettingsError> {
        values
            .iter()
            .try_fold(SessionSettings::default(), |settings, (name, value)| {
                settings.with(name, value)
            })
    }

    /// Checked builder: `SessionSettings::default().with("merge_lineage", "off")?`.
    ///
    /// # Errors
    ///
    /// An unknown name, a value of the wrong kind, an unknown enum value or an
    /// integer outside its range.
    pub fn with(mut self, name: &str, value: &str) -> Result<Self, SessionSettingsError> {
        let (id, value) = SettingId::parse_assignment(name, value)?;
        self.set(id, &value)?;
        Ok(self)
    }

    /// Assign one setting.
    ///
    /// # Errors
    ///
    /// A value of the wrong kind, an unknown enum value or an integer outside
    /// its range; `self` is unchanged then.
    pub fn set(&mut self, id: SettingId, value: &SettingValue) -> Result<(), SessionSettingsError> {
        match id {
            SettingId::RrfPlan => self.rrf_plan = parse_enum(id, value, RrfPlan::from_spelling)?,
            SettingId::MergeLineage => {
                self.merge_lineage = parse_enum(id, value, MergeLineage::from_spelling)?
            }
            SettingId::AnnNprobes => {
                let probes = parse_integer(id, value)?;
                self.ann_nprobes = usize::try_from(probes).ok().filter(|probes| *probes > 0);
            }
            SettingId::StageWriteConcurrency => {
                let width = parse_integer(id, value)?;
                self.stage_write_concurrency =
                    usize::try_from(width).expect("invariant: the row's range is 1..=64");
            }
        }
        Ok(())
    }

    /// Take one setting's typed value from `from`, no spelling in between:
    /// the field the row names, and no other field.
    pub fn copy_field(&mut self, from: &SessionSettings, id: SettingId) {
        match id {
            SettingId::RrfPlan => self.rrf_plan = from.rrf_plan,
            SettingId::MergeLineage => self.merge_lineage = from.merge_lineage,
            SettingId::AnnNprobes => self.ann_nprobes = from.ann_nprobes,
            SettingId::StageWriteConcurrency => {
                self.stage_write_concurrency = from.stage_write_concurrency;
            }
        }
    }

    /// The current value in GQ spelling, the `value` column of `show`.
    pub fn get(&self, id: SettingId) -> String {
        match id {
            SettingId::RrfPlan => self.rrf_plan.as_str().to_string(),
            SettingId::MergeLineage => self.merge_lineage.as_str().to_string(),
            SettingId::AnnNprobes => self.ann_nprobes.unwrap_or(0).to_string(),
            SettingId::StageWriteConcurrency => self.stage_write_concurrency.to_string(),
        }
    }

    /// The `show` row of one setting under the source the session recorded.
    pub fn row(&self, id: SettingId, source: Source) -> SettingRow {
        let spec = id.spec();
        SettingRow {
            name: spec.name,
            value: self.get(id),
            default: spec.default,
            source,
            scope: spec.scope,
        }
    }

    /// A harness knob, not a definition row: no name, no `set`/`show`, no
    /// environment variable. Tests, DST and the GQT `# traversal:` header pin
    /// choose the expand path with it.
    pub fn with_traversal(mut self, traversal: Traversal) -> Self {
        self.traversal = traversal;
        self
    }

    pub fn traversal(&self) -> Traversal {
        self.traversal
    }

    pub fn rrf_plan(&self) -> RrfPlan {
        self.rrf_plan
    }

    pub fn merge_lineage(&self) -> MergeLineage {
        self.merge_lineage
    }

    /// The probe cap; `None` is no cap (spelled `0`).
    pub fn ann_nprobes(&self) -> Option<usize> {
        self.ann_nprobes
    }

    pub fn stage_write_concurrency(&self) -> usize {
        self.stage_write_concurrency
    }
}

fn parse_enum<T>(
    id: SettingId,
    value: &SettingValue,
    from_spelling: fn(&str) -> Option<T>,
) -> Result<T, SessionSettingsError> {
    let spelled = match value {
        SettingValue::Ident(text) | SettingValue::Str(text) => from_spelling(text),
        SettingValue::Integer(_) => None,
    };
    spelled.ok_or_else(|| SessionSettingsError::UnknownValue {
        setting: id,
        value: value.to_string(),
    })
}

/// A digit run under an optional `-` or `+` sign, the shape
/// [`SettingValue::from_spelling`] reads as an integer attempt.
fn is_digit_run(text: &str) -> bool {
    let digits = text.strip_prefix(['-', '+']).unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn parse_integer(id: SettingId, value: &SettingValue) -> Result<i64, SessionSettingsError> {
    let SettingKind::Integer { min, max } = id.spec().kind else {
        unreachable!("parse_integer is called for the integer rows only")
    };
    let SettingValue::Integer(number) = value else {
        return Err(match value {
            SettingValue::Str(text) if is_digit_run(text) && text.parse::<i64>().is_err() => {
                SessionSettingsError::OutOfRange {
                    setting: id,
                    got: text.clone(),
                }
            }
            _ => SessionSettingsError::WrongKind {
                setting: id,
                got: value.kind_word(),
                spelling: value.to_string(),
            },
        });
    };
    if *number < min || max.is_some_and(|max| *number > max) {
        return Err(SessionSettingsError::OutOfRange {
            setting: id,
            got: number.to_string(),
        });
    }
    Ok(*number)
}

/// One row of `show`: `name`, `value`, `default`, `source`, `scope`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingRow {
    pub name: &'static str,
    pub value: String,
    pub default: &'static str,
    pub source: Source,
    pub scope: SettingScope,
}

impl SettingRow {
    /// The five column names, in row order.
    pub const COLUMNS: [&'static str; 5] = ["name", "value", "default", "source", "scope"];
}

/// The process defaults: every row's environment variable read once, an
/// unset or empty variable leaving the definition's default.
///
/// # Errors
///
/// The first variable holding a value its row refuses, or one that is not
/// valid Unicode, named in the message.
pub fn from_env() -> Result<(SessionSettings, Sources), SessionSettingsError> {
    for id in SettingId::ALL {
        let variable = id.spec().env;
        if let Err(std::env::VarError::NotUnicode(_)) = std::env::var(variable) {
            return Err(SessionSettingsError::Environment {
                variable,
                message: "value is not valid Unicode".to_string(),
            });
        }
    }
    from_env_with(|variable| std::env::var(variable).ok())
}

/// [`from_env`] over a caller-supplied lookup, so a test or an embedded host
/// seeds the process defaults without touching the process environment.
///
/// # Errors
///
/// As [`from_env`].
pub fn from_env_with(
    mut lookup: impl FnMut(&'static str) -> Option<String>,
) -> Result<(SessionSettings, Sources), SessionSettingsError> {
    let mut settings = SessionSettings::default();
    let mut sources = [Source::Default; DEFINITIONS.len()];
    for id in SettingId::ALL {
        let spec = id.spec();
        let Some(raw) = lookup(spec.env) else {
            continue;
        };
        let text = raw.trim();
        if text.is_empty() {
            continue;
        }
        settings
            .set(id, &SettingValue::from_spelling(text))
            .map_err(|error| SessionSettingsError::Environment {
                variable: spec.env,
                message: error.to_string(),
            })?;
        sources[id as usize] = Source::Env;
    }
    Ok((settings, sources))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionSettingsError {
    #[error("unknown setting `{name}`; expected one of {}", setting_names())]
    UnknownSetting { name: String },
    #[error("unknown value `{value}` for setting `{}`; expected one of {}", setting.name(), enum_values(*setting))]
    UnknownValue { setting: SettingId, value: String },
    #[error("setting `{}` takes {}, got {got} `{spelling}`", setting.name(), integer_range(*setting))]
    WrongKind {
        setting: SettingId,
        got: &'static str,
        spelling: String,
    },
    #[error("setting `{}` takes {}, got {got}", setting.name(), integer_range(*setting))]
    OutOfRange { setting: SettingId, got: String },
    #[error(
        "setting `{}` is a process setting; it is read from the server's environment, not from a request",
        setting.name()
    )]
    ProcessScope { setting: SettingId },
    #[error("{variable}: {message}")]
    Environment {
        variable: &'static str,
        message: String,
    },
}

fn setting_names() -> String {
    DEFINITIONS
        .iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>()
        .join(", ")
}

fn enum_values(id: SettingId) -> String {
    match id.spec().kind {
        SettingKind::Enum(values) => values.join(", "),
        SettingKind::Integer { .. } => integer_range(id),
    }
}

fn integer_range(id: SettingId) -> String {
    match id.spec().kind {
        SettingKind::Integer {
            min,
            max: Some(max),
        } => format!("an integer in {min}..={max}"),
        SettingKind::Integer { min, max: None } => format!("an integer of at least {min}"),
        SettingKind::Enum(values) => format!("one of {}", values.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definitions_ids_and_fields_agree_in_both_directions() {
        assert_eq!(SettingId::ALL.len(), DEFINITIONS.len());
        for (index, id) in SettingId::ALL.into_iter().enumerate() {
            assert_eq!(id as usize, index, "{id:?} sits at its definition row");
            assert_eq!(SettingId::parse(id.name()).unwrap(), id);
            let spelled = SettingValue::from_spelling(id.spec().default);
            SessionSettings::default()
                .set(id, &spelled)
                .unwrap_or_else(|error| panic!("default of {} parses: {error}", id.name()));
        }
        let mut names = DEFINITIONS.iter().map(|spec| spec.name).collect::<Vec<_>>();
        names.dedup();
        assert_eq!(names.len(), DEFINITIONS.len(), "names are unique");
        let defaults = SessionSettings::default();
        for id in SettingId::ALL {
            let expected = if id == SettingId::MergeLineage && cfg!(debug_assertions) {
                "verify"
            } else {
                id.spec().default
            };
            assert_eq!(defaults.get(id), expected, "{}", id.name());
        }
    }

    #[test]
    fn messages_follow_the_definition() {
        let err = SettingId::parse("merge_linage").unwrap_err();
        assert_eq!(
            err.to_string(),
            "unknown setting `merge_linage`; expected one of rrf_plan, merge_lineage, ann_nprobes, stage_write_concurrency"
        );
        let err = SessionSettings::default()
            .with("merge_lineage", "v3")
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "unknown value `v3` for setting `merge_lineage`; expected one of off, on, verify"
        );
        let err = SessionSettings::default()
            .with("ann_nprobes", "\"many\"")
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "setting `ann_nprobes` takes an integer of at least 0, got a string `many`"
        );
        let err = SessionSettings::default()
            .with("stage_write_concurrency", "0")
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "setting `stage_write_concurrency` takes an integer in 1..=64, got 0"
        );
        let err = SessionSettings::default()
            .with("stage_write_concurrency", "65")
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "setting `stage_write_concurrency` takes an integer in 1..=64, got 65"
        );
        let err = SettingId::StageWriteConcurrency
            .refuse_from_request()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "setting `stage_write_concurrency` is a process setting; it is read from the server's environment, not from a request"
        );
        assert!(SettingId::MergeLineage.refuse_from_request().is_ok());
    }

    #[test]
    fn values_are_typed_and_the_same_under_every_spelling() {
        let settings = SessionSettings::try_from_values(&[
            ("rrf_plan", "\"force_prefilter\""),
            ("merge_lineage", "off"),
            ("ann_nprobes", "0"),
            ("stage_write_concurrency", "64"),
        ])
        .unwrap();
        assert_eq!(settings.rrf_plan(), RrfPlan::ForcePrefilter);
        assert_eq!(settings.merge_lineage(), MergeLineage::Off);
        assert_eq!(settings.ann_nprobes(), None);
        assert_eq!(settings.get(SettingId::AnnNprobes), "0");
        assert_eq!(settings.stage_write_concurrency(), 64);
        let mut again = settings.clone();
        again
            .set(SettingId::AnnNprobes, &SettingValue::Integer(7))
            .unwrap();
        assert_eq!(again.ann_nprobes(), Some(7));
        assert!(
            again
                .set(SettingId::MergeLineage, &SettingValue::Integer(1))
                .is_err()
        );
        assert_eq!(
            again.merge_lineage(),
            MergeLineage::Off,
            "a refused set changes nothing"
        );
        let huge = SessionSettings::default()
            .with("ann_nprobes", "99999999999999999999")
            .unwrap_err();
        assert_eq!(
            huge.to_string(),
            "setting `ann_nprobes` takes an integer of at least 0, got 99999999999999999999"
        );
        let negative = SessionSettings::default()
            .with("ann_nprobes", " -1 ")
            .unwrap_err();
        assert_eq!(
            negative.to_string(),
            "setting `ann_nprobes` takes an integer of at least 0, got -1"
        );
        assert_eq!(
            SettingValue::from_spelling(" +7 "),
            SettingValue::Integer(7)
        );
        assert_eq!(
            SettingId::parse_assignment("merge_lineage", " verify ").unwrap(),
            (
                SettingId::MergeLineage,
                SettingValue::Ident("verify".to_string())
            )
        );
        assert!(SettingId::parse_assignment("merge_lineage", "sometimes").is_err());
        assert!(SettingId::parse_assignment("merge_linage", "on").is_err());
    }

    #[test]
    fn from_env_seeds_defaults_and_refuses_invalid_values() {
        let (settings, sources) = from_env_with(|variable| match variable {
            "OMNIGRAPH_ANN_NPROBES" => Some("5".to_string()),
            "OMNIGRAPH_MERGE_LINEAGE" => Some(" ".to_string()),
            "OMNIGRAPH_RRF_PLAN" => Some("force_prefilter".to_string()),
            _ => None,
        })
        .unwrap();
        assert_eq!(settings.ann_nprobes(), Some(5));
        assert_eq!(settings.rrf_plan(), RrfPlan::ForcePrefilter);
        assert_eq!(sources[SettingId::AnnNprobes as usize], Source::Env);
        assert_eq!(sources[SettingId::RrfPlan as usize], Source::Env);
        assert_eq!(sources[SettingId::MergeLineage as usize], Source::Default);
        assert_eq!(
            sources[SettingId::StageWriteConcurrency as usize],
            Source::Default
        );

        for (variable, value, expected) in [
            (
                "OMNIGRAPH_LOAD_CONCURRENCY",
                "0",
                "OMNIGRAPH_LOAD_CONCURRENCY: setting `stage_write_concurrency` takes an integer in 1..=64, got 0",
            ),
            (
                "OMNIGRAPH_LOAD_CONCURRENCY",
                "128",
                "OMNIGRAPH_LOAD_CONCURRENCY: setting `stage_write_concurrency` takes an integer in 1..=64, got 128",
            ),
            (
                "OMNIGRAPH_RRF_PLAN",
                "prefilter",
                "OMNIGRAPH_RRF_PLAN: unknown value `prefilter` for setting `rrf_plan`; expected one of auto, force_prefilter, force_postfilter",
            ),
            (
                "OMNIGRAPH_MERGE_LINEAGE",
                "OFF",
                "OMNIGRAPH_MERGE_LINEAGE: unknown value `OFF` for setting `merge_lineage`; expected one of off, on, verify",
            ),
            (
                "OMNIGRAPH_ANN_NPROBES",
                "many",
                "OMNIGRAPH_ANN_NPROBES: setting `ann_nprobes` takes an integer of at least 0, got an identifier `many`",
            ),
        ] {
            let err = from_env_with(|candidate| (candidate == variable).then(|| value.to_string()))
                .unwrap_err();
            assert_eq!(err.to_string(), expected);
        }
        let (unbounded, _) = from_env_with(|variable| {
            (variable == "OMNIGRAPH_ANN_NPROBES").then(|| "100000".to_string())
        })
        .unwrap();
        assert_eq!(unbounded.ann_nprobes(), Some(100_000));
    }

    #[cfg(unix)]
    #[test]
    fn from_env_refuses_a_value_that_is_not_unicode() {
        use std::os::unix::ffi::OsStrExt as _;
        let variable = SettingId::MergeLineage.spec().env;
        // SAFETY: no other thread reads the environment; every other environment test here goes through `from_env_with`.
        unsafe { std::env::set_var(variable, std::ffi::OsStr::from_bytes(b"on\xff")) };
        let err = from_env().unwrap_err();
        unsafe { std::env::remove_var(variable) };
        assert_eq!(
            err.to_string(),
            "OMNIGRAPH_MERGE_LINEAGE: value is not valid Unicode"
        );
    }

    #[test]
    fn show_rows_carry_the_definition_columns() {
        let row = SessionSettings::default().row(SettingId::StageWriteConcurrency, Source::File);
        assert_eq!(
            row,
            SettingRow {
                name: "stage_write_concurrency",
                value: "8".to_string(),
                default: "8",
                source: Source::File,
                scope: SettingScope::Process,
            }
        );
        assert_eq!(
            SettingRow::COLUMNS,
            ["name", "value", "default", "source", "scope"]
        );
    }
}
