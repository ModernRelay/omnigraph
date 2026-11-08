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
    pub const fn store_effect(self) -> Option<super::StoreEffect> {
        match self {
            StoreAction::Misdirect => Some(super::StoreEffect::Misdirect),
            StoreAction::Lose | StoreAction::Error | StoreAction::Corrupt | StoreAction::Delay => {
                None
            }
        }
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
