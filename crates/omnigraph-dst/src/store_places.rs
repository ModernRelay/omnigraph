//! Store places: the storage decoration's adapter calls as seams a logic
//! test names (`storage.put` and its siblings), the store actions each
//! honors, and the per-step targeting state the GQT runner installs on the
//! decoration (RFC 0066 §Design, Store places).

use std::panic::Location;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// One family of adapter calls, named as a seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StorePlace {
    Put,
    Delete,
    Rename,
    Cas,
    Get,
    List,
}

impl StorePlace {
    /// The row name a case's `at:` spells for this place.
    pub const fn as_str(self) -> &'static str {
        match self {
            StorePlace::Put => "storage.put",
            StorePlace::Delete => "storage.delete",
            StorePlace::Rename => "storage.rename",
            StorePlace::Cas => "storage.cas",
            StorePlace::Get => "storage.get",
            StorePlace::List => "storage.list",
        }
    }
}

/// What the decoration does to one targeted call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StoreAction {
    Misdirect,
    Lose,
    Error,
    Corrupt,
    Delay,
}

impl StoreAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            StoreAction::Misdirect => "misdirect",
            StoreAction::Lose => "lose",
            StoreAction::Error => "error",
            StoreAction::Corrupt => "corrupt",
            StoreAction::Delay => "delay",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        [
            StoreAction::Misdirect,
            StoreAction::Lose,
            StoreAction::Error,
            StoreAction::Corrupt,
            StoreAction::Delay,
        ]
        .into_iter()
        .find(|action| action.as_str() == name)
    }

    /// The store effect a decision seam declares for this action, or `None`
    /// for an action no seam can fire.
    pub const fn store_effect(self) -> Option<omnigraph::seams::StoreEffect> {
        match self {
            StoreAction::Misdirect => Some(omnigraph::seams::StoreEffect::Misdirect),
            StoreAction::Lose | StoreAction::Error | StoreAction::Corrupt | StoreAction::Delay => {
                None
            }
        }
    }
}

/// The adapter methods the put place covers, one variant per hook, so a hook
/// and the row it is admitted against name the method once.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PutMethod {
    WriteText,
    WriteBytes,
    WriteTextIfAbsent,
}

impl PutMethod {
    /// The adapter method name, the value a delivery record's `method` carries.
    pub const fn as_str(self) -> &'static str {
        match self {
            PutMethod::WriteText => "write_text",
            PutMethod::WriteBytes => "write_bytes",
            PutMethod::WriteTextIfAbsent => "write_text_if_absent",
        }
    }
}

/// Every put method, in the order the put row lists them.
pub const PUT_METHODS: &[PutMethod] = &[
    PutMethod::WriteText,
    PutMethod::WriteBytes,
    PutMethod::WriteTextIfAbsent,
];

/// One row of the table a case is admitted against: the place, the adapter
/// methods it covers, the actions the hooks honor under the random plan, and
/// the actions a targeted rule may name.
#[derive(Debug)]
pub struct StorePlaceEntry {
    pub place: StorePlace,
    pub name: &'static str,
    pub methods: &'static [&'static str],
    pub honors: &'static [StoreAction],
    pub admitted: &'static [StoreAction],
    pub file: &'static str,
    pub line: u32,
}

impl StorePlaceEntry {
    /// `file:line` of the row, the value a delivery record's `declared_at` carries.
    pub fn declared_at(&self) -> String {
        file_line(self.file, self.line)
    }
}

/// The one spelling of a source position in a record a case reads.
pub fn file_line(file: &str, line: u32) -> String {
    format!("{file}:{line}")
}

macro_rules! row {
    ($place:ident, $name:literal, [$($method:expr),+], honors [$($honor:ident),+], admitted [$($admit:ident),*]) => {
        StorePlaceEntry {
            place: StorePlace::$place,
            name: $name,
            methods: &[$($method),+],
            honors: &[$(StoreAction::$honor),+],
            admitted: &[$(StoreAction::$admit),*],
            file: file!(),
            line: line!(),
        }
    };
}

/// Every store place, in the order the RFC table lists them.
pub static STORE_PLACES: &[StorePlaceEntry] = &[
    row!(Put, "storage.put", [PutMethod::WriteText.as_str(), PutMethod::WriteBytes.as_str(), PutMethod::WriteTextIfAbsent.as_str()], honors [Misdirect, Lose, Error, Corrupt, Delay], admitted [Misdirect]),
    row!(Delete, "storage.delete", ["delete", "delete_prefix"], honors [Lose, Error, Delay], admitted []),
    row!(Rename, "storage.rename", ["rename_text"], honors [Lose, Error, Delay], admitted []),
    row!(Cas, "storage.cas", ["write_text_if_match"], honors [Error, Delay, Corrupt], admitted []),
    row!(Get, "storage.get", ["read_text", "read_text_if_exists", "read_text_if_exists_bounded", "read_bytes_if_exists_bounded", "read_text_versioned"], honors [Error, Corrupt, Delay], admitted []),
    row!(List, "storage.list", ["list_dir", "list_dir_bounded"], honors [Error, Delay], admitted []),
];

/// The row a case's `at:` names, or `None` for any other string.
pub fn store_place(name: &str) -> Option<&'static StorePlaceEntry> {
    STORE_PLACES.iter().find(|entry| entry.name == name)
}

/// The row of a place; the table carries every place exactly once.
pub fn entry_of(place: StorePlace) -> &'static StorePlaceEntry {
    STORE_PLACES
        .iter()
        .find(|entry| entry.place == place)
        .expect("every store place has a row")
}

/// The call a store effect acts on: the place whose next call takes it, and
/// the action the decoration applies there.
pub const fn effect_target(effect: omnigraph::seams::StoreEffect) -> (StorePlace, StoreAction) {
    match effect {
        omnigraph::seams::StoreEffect::Misdirect => (StorePlace::Put, StoreAction::Misdirect),
    }
}

/// The misdirection transform: same directory, `dstm-` prefixed on the file
/// name (extension preserved), so the write lands at a wrong key inside the
/// same keyspace and listings still see it.
pub fn misdirect_uri(uri: &str) -> String {
    match uri.rsplit_once('/') {
        Some((dir, file)) => format!("{dir}/dstm-{file}"),
        None => format!("dstm-{uri}"),
    }
}

/// The largest `subject` a case may carry, the bound the known-failure
/// reason shares.
pub const SUBJECT_MAX_BYTES: usize = 2048;

/// A case's `subject`, parsed: a `globset` glob over the object's
/// root-relative name with the dialect pinned the same on every host (`*`
/// within one segment, `**` across, case-sensitive, backslash escapes).
#[derive(Clone, Debug)]
pub struct Subject {
    matcher: globset::GlobSet,
}

impl Subject {
    /// # Errors
    /// Refuses an empty or over-long subject and a glob `globset` cannot
    /// build.
    pub fn parse(text: &str) -> Result<Self, String> {
        if text.is_empty() || text.len() > SUBJECT_MAX_BYTES {
            return Err(format!(
                "invalid_case: subject must be 1 to {SUBJECT_MAX_BYTES} bytes"
            ));
        }
        let glob = globset::GlobBuilder::new(text)
            .literal_separator(true)
            .case_insensitive(false)
            .backslash_escape(true)
            .build()
            .map_err(|error| format!("invalid_case: subject is not a glob: {error}"))?;
        let mut set = globset::GlobSetBuilder::new();
        set.add(glob);
        let matcher = set
            .build()
            .map_err(|error| format!("invalid_case: subject is not a glob: {error}"))?;
        Ok(Self { matcher })
    }

    pub fn matches(&self, name: &str) -> bool {
        self.matcher.is_match(name)
    }
}

/// One rule a case installs on the decoration for one step: the `occurrence`th
/// call of `place` whose root-relative name matches `subject` takes `action`.
#[derive(Debug)]
pub struct TargetedRule {
    pub place: StorePlace,
    pub subject: Subject,
    pub occurrence: u64,
    pub action: StoreAction,
    seen: AtomicU64,
}

impl TargetedRule {
    pub fn new(place: StorePlace, subject: Subject, occurrence: u64, action: StoreAction) -> Self {
        Self {
            place,
            subject,
            occurrence,
            action,
            seen: AtomicU64::new(0),
        }
    }
}

/// What the decoration did to one targeted call: the proof a delivery record
/// carries. `requested` and `stored` are root-relative, the domain a
/// `subject` is matched in.
#[derive(Clone, Debug)]
pub struct StoreHit {
    pub place: StorePlace,
    pub action: StoreAction,
    pub method: &'static str,
    pub requested: String,
    pub stored: Option<String>,
    pub fired_at: String,
    /// Whether the hit consumed a seam's one-shot rather than a rule.
    pub one_shot: bool,
    /// Whether the store accepted the call the action was applied to; a
    /// targeted put the store refused, or a conditional put that found the
    /// object present, is no delivery.
    pub landed: bool,
}

/// What `Targets::clear` hands back after a step.
#[derive(Debug, Default)]
pub struct Cleared {
    pub one_shot_unconsumed: bool,
    pub rule_matched_calls: u64,
    pub hits: Vec<StoreHit>,
}

/// The targeting state of one decoration: one rule and one one-shot at a
/// time, installed before a step and cleared after it.
#[derive(Debug)]
pub struct Targets {
    root: Option<String>,
    rule: Mutex<Option<TargetedRule>>,
    one_shot: Mutex<Option<(StorePlace, StoreAction, Option<Subject>)>>,
    hits: Mutex<Vec<StoreHit>>,
}

/// The decision `Targets::on_call` hands the hook that asked.
#[derive(Debug)]
pub struct Targeted {
    pub action: StoreAction,
    pub one_shot: bool,
    fired_at: String,
}

impl Targets {
    pub fn new(root: Option<String>) -> Self {
        Self {
            root: root.map(|root| root.trim_end_matches('/').to_string()),
            rule: Mutex::new(None),
            one_shot: Mutex::new(None),
            hits: Mutex::new(Vec::new()),
        }
    }

    pub fn install_rule(&self, rule: TargetedRule) {
        *self.rule.lock().unwrap() = Some(rule);
    }

    /// Arm the one store call the declaring site precedes: the next call of
    /// `place` whose name matches `subject` takes `action`, and a call that
    /// does not match passes through with the one-shot still armed.
    pub fn arm_one_shot(&self, place: StorePlace, action: StoreAction, subject: Option<Subject>) {
        *self.one_shot.lock().unwrap() = Some((place, action, subject));
    }

    pub fn clear(&self) -> Cleared {
        let one_shot_unconsumed = self.one_shot.lock().unwrap().take().is_some();
        let rule_matched_calls = self
            .rule
            .lock()
            .unwrap()
            .take()
            .map_or(0, |rule| rule.seen.load(Ordering::SeqCst));
        let hits = std::mem::take(&mut *self.hits.lock().unwrap());
        Cleared {
            one_shot_unconsumed,
            rule_matched_calls,
            hits,
        }
    }

    fn relative(&self, uri: &str) -> Option<String> {
        let root = self.root.as_deref()?;
        uri.strip_prefix(root)
            .and_then(|rest| rest.strip_prefix('/'))
            .map(str::to_string)
    }

    fn subject_matches(&self, subject: Option<&Subject>, uri: &str) -> bool {
        match subject {
            None => true,
            Some(subject) => self
                .relative(uri)
                .is_some_and(|name| subject.matches(&name)),
        }
    }

    /// One-shot first, on its own place and subject; then the rule counts a
    /// matching call and fires on its occurrence.
    #[track_caller]
    pub fn on_call(&self, place: StorePlace, uri: &str) -> Option<Targeted> {
        let fired_at = {
            let at = Location::caller();
            file_line(at.file(), at.line())
        };
        {
            let mut one_shot = self.one_shot.lock().unwrap();
            let consumed = match one_shot.as_ref() {
                Some((armed, action, subject))
                    if *armed == place && self.subject_matches(subject.as_ref(), uri) =>
                {
                    Some(*action)
                }
                _ => None,
            };
            if let Some(action) = consumed {
                *one_shot = None;
                return Some(Targeted {
                    action,
                    one_shot: true,
                    fired_at,
                });
            }
        }
        let guard = self.rule.lock().unwrap();
        let rule = guard.as_ref()?;
        if rule.place != place {
            return None;
        }
        let name = self.relative(uri)?;
        if !rule.subject.matches(&name) {
            return None;
        }
        let seen = rule.seen.fetch_add(1, Ordering::SeqCst) + 1;
        (seen == rule.occurrence).then_some(Targeted {
            action: rule.action,
            one_shot: false,
            fired_at,
        })
    }

    /// Record what became of a targeted call once the store answered.
    pub fn record(
        &self,
        targeted: Targeted,
        place: StorePlace,
        method: &'static str,
        requested: &str,
        stored: Option<&str>,
        landed: bool,
    ) {
        let relative = |uri: &str| self.relative(uri).unwrap_or_else(|| uri.to_string());
        self.hits.lock().unwrap().push(StoreHit {
            place,
            action: targeted.action,
            method,
            requested: relative(requested),
            stored: stored.map(relative),
            fired_at: targeted.fired_at,
            one_shot: targeted.one_shot,
            landed,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_star_stays_within_one_segment() {
        let subject = Subject::parse("__recovery/*").unwrap();
        assert!(subject.matches("__recovery/01J.json"));
        assert!(!subject.matches("__recovery/x/01J.json"));
        assert!(
            Subject::parse("__recovery/**")
                .unwrap()
                .matches("__recovery/x/01J.json")
        );
    }

    #[test]
    fn rule_counts_only_matching_calls_of_its_place() {
        let targets = Targets::new(Some("shared-memory://t/case".into()));
        targets.install_rule(TargetedRule::new(
            StorePlace::Put,
            Subject::parse("__recovery/*").unwrap(),
            2,
            StoreAction::Misdirect,
        ));
        assert!(
            targets
                .on_call(
                    StorePlace::Delete,
                    "shared-memory://t/case/__recovery/a.json"
                )
                .is_none()
        );
        assert!(
            targets
                .on_call(StorePlace::Put, "shared-memory://t/case/data/a.lance")
                .is_none()
        );
        assert!(
            targets
                .on_call(StorePlace::Put, "shared-memory://t/case/__recovery/a.json")
                .is_none()
        );
        let hit = targets
            .on_call(StorePlace::Put, "shared-memory://t/case/__recovery/a.json")
            .unwrap();
        assert_eq!(hit.action, StoreAction::Misdirect);
        assert!(!hit.one_shot);
        assert_eq!(targets.clear().rule_matched_calls, 2);
    }

    #[test]
    fn one_shot_is_consumed_by_the_next_put_only() {
        let targets = Targets::new(None);
        targets.arm_one_shot(StorePlace::Put, StoreAction::Misdirect, None);
        assert!(targets.on_call(StorePlace::Delete, "x").is_none());
        assert!(targets.on_call(StorePlace::Put, "x").unwrap().one_shot);
        assert!(targets.on_call(StorePlace::Put, "x").is_none());
        targets.arm_one_shot(StorePlace::Put, StoreAction::Misdirect, None);
        assert!(targets.clear().one_shot_unconsumed);
    }

    #[test]
    fn one_shot_with_a_subject_passes_a_put_of_another_object() {
        let targets = Targets::new(Some("shared-memory://t/case".into()));
        targets.arm_one_shot(
            StorePlace::Put,
            StoreAction::Misdirect,
            Some(Subject::parse("__recovery/*").unwrap()),
        );
        assert!(
            targets
                .on_call(StorePlace::Put, "shared-memory://t/case/data/a.lance")
                .is_none()
        );
        assert!(
            targets
                .on_call(StorePlace::Put, "shared-memory://t/case/__recovery/a.json")
                .unwrap()
                .one_shot
        );
    }

    #[test]
    fn every_put_method_is_a_method_of_the_put_row() {
        let row = entry_of(StorePlace::Put);
        assert_eq!(PUT_METHODS.len(), row.methods.len());
        for method in PUT_METHODS {
            assert!(
                row.methods.contains(&method.as_str()),
                "{}",
                method.as_str()
            );
        }
    }

    #[test]
    fn every_admitted_action_is_honored() {
        for entry in STORE_PLACES {
            for action in entry.admitted {
                assert!(entry.honors.contains(action), "{}", entry.name);
            }
        }
        assert!(store_place("storage.put").is_some());
        assert!(store_place("recovery.sidecar_write").is_none());
    }

    #[test]
    fn subject_escapes_the_same_on_every_host() {
        let subject = Subject::parse(r"__recovery/\*.json").unwrap();
        assert!(subject.matches("__recovery/*.json"));
        assert!(!subject.matches("__recovery/a.json"));
        assert!(
            !Subject::parse("__recovery/A.json")
                .unwrap()
                .matches("__recovery/a.json")
        );
    }

    #[test]
    fn nested_braces_are_refused_without_a_panic() {
        let text = "{".repeat(300) + "x" + &"}".repeat(300);
        let error = Subject::parse(&text).unwrap_err();
        assert!(error.starts_with("invalid_case:"), "{error}");
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn store_place_names_are_not_engine_seams() {
        for entry in STORE_PLACES {
            assert!(
                omnigraph::seams::catalog::decide(entry.name).is_none(),
                "{} is both a store place and an engine seam",
                entry.name
            );
        }
    }
}
