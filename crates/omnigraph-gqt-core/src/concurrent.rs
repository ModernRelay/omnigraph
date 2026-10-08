use std::{fmt, str::FromStr};

pub const MAX_SESSIONS: usize = 4;
const RESERVED_LABELS: [&str; 4] = ["setup", "runner", "step", "order"];

/// The store verbs an `order:` entry may name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verb {
    Get,
    Head,
    Put,
    List,
    Delete,
    Copy,
}

impl Verb {
    const ALL: [Verb; 6] = [
        Verb::Get,
        Verb::Head,
        Verb::Put,
        Verb::List,
        Verb::Delete,
        Verb::Copy,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Verb::Get => "get",
            Verb::Head => "head",
            Verb::Put => "put",
            Verb::List => "list",
            Verb::Delete => "delete",
            Verb::Copy => "copy",
        }
    }
}

impl FromStr for Verb {
    type Err = String;

    fn from_str(word: &str) -> Result<Self, Self::Err> {
        Verb::ALL
            .into_iter()
            .find(|verb| verb.name() == word)
            .ok_or_else(|| {
                let names: Vec<&str> = Verb::ALL.iter().map(|verb| verb.name()).collect();
                format!("`order:` verb `{word}` is not one of {}", names.join(", "))
            })
    }
}

impl fmt::Display for Verb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What a session's statement is: the executor and the comparator differ.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionKind {
    Query,
    Mutation,
}

impl SessionKind {
    /// The step-kind word the labels and evidence rows carry.
    pub fn name(self) -> &'static str {
        match self {
            SessionKind::Query => "query",
            SessionKind::Mutation => "mutate",
        }
    }
}

/// One session of a block: a labeled statement on a branch, and the outcome
/// the `--- expect` section after the block names for it.
#[derive(Debug)]
pub struct SessionOp {
    pub label: String,
    pub branch: String,
    pub source: String,
    pub name: String,
    pub kind: SessionKind,
    pub expect: SessionExpect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionExpect {
    Ok,
    Error { needle: String },
}

/// A `--- concurrent` step: its sessions and the script.
#[derive(Debug)]
pub struct ConcurrentStep {
    pub ordinal: usize,
    pub sessions: Vec<SessionOp>,
    pub order: Vec<Entry>,
}

/// One `order:` entry: which session, and what event of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub session: usize,
    pub event: Event,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The session's statement begins: the runner holds the session before
    /// it, so a session can be made to start inside another's hold without
    /// naming a request the engine makes under a lock.
    Start,
    /// The session's completion, written as the bare label.
    Done,
    /// The session's next request of `verb` on a key containing `suffix`.
    /// `park` holds the session at the request's arrival until its next
    /// entry is due, instead of running the request in turn.
    Call {
        park: bool,
        verb: Verb,
        suffix: String,
    },
}

impl Event {
    /// Whether `self` is the un-parked form of the parked request `parked`.
    pub fn releases(&self, parked: &Event) -> bool {
        match (self, parked) {
            (
                Event::Call {
                    park: false,
                    verb,
                    suffix,
                },
                Event::Call {
                    park: true,
                    verb: parked_verb,
                    suffix: parked_suffix,
                },
            ) => verb == parked_verb && suffix == parked_suffix,
            (Event::Done, Event::Call { park: true, .. }) => true,
            _ => false,
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Start => f.write_str("start"),
            Event::Done => f.write_str("done"),
            Event::Call { park, verb, suffix } => {
                if *park {
                    write!(f, "park {verb} {suffix}")
                } else {
                    write!(f, "{verb} {suffix}")
                }
            }
        }
    }
}

/// A session line of the block body before its statement is parsed: the
/// label, the branch (`main` unless `on <branch>`), the statement text and
/// the line the statement starts on.
#[derive(Debug, PartialEq, Eq)]
pub struct SessionLine {
    pub label: String,
    pub branch: String,
    pub text: String,
    pub line: usize,
}

/// The block body split into session lines and the script. A session starts
/// at column 0 as `<label>[ on <branch>]: <statement>`; every other line
/// continues the open session's statement (a GQ body closes with `}` at
/// column 0, so continuation is not a matter of indentation); `order:` at
/// column 0 is the script and closes the sessions.
pub fn parse_block(body: &[(usize, &str)]) -> Result<(Vec<SessionLine>, Vec<Entry>), String> {
    let mut sessions: Vec<SessionLine> = Vec::new();
    let mut order: Option<(usize, &str)> = None;
    for (idx, raw) in body {
        let line = idx + 1;
        if raw.trim().is_empty() {
            continue;
        }
        if let Some(rest) = raw.strip_prefix("order:") {
            if order.is_some() {
                return Err(format!("line {line}: a block has one `order:` line"));
            }
            order = Some((line, rest));
            continue;
        }
        match session_start(raw) {
            None if order.is_some() => {
                return Err(format!(
                    "line {line}: `order:` closes the sessions; a session statement cannot continue after it"
                ));
            }
            None => {
                if looks_like_order(raw) {
                    return Err(format!(
                        "line {line}: the script line is `order:` at column 0, got `{raw}`"
                    ));
                }
                let Some(current) = sessions.last_mut() else {
                    return Err(format!(
                        "line {line}: a block opens with `<label>[ on <branch>]: <statement>` at column 0, got `{raw}`"
                    ));
                };
                current.text.push('\n');
                current.text.push_str(raw);
            }
            Some(_) if order.is_some() => {
                return Err(format!(
                    "line {line}: `order:` is the last line of a block; sessions come before it"
                ));
            }
            Some((label, branch, text)) => {
                if RESERVED_LABELS.contains(&label) {
                    return Err(format!(
                        "line {line}: `{label}` is reserved (the measure report's own rows and the script keyword); name the session otherwise"
                    ));
                }
                if sessions.iter().any(|s| s.label == label) {
                    return Err(format!("line {line}: session `{label}` is declared twice"));
                }
                if sessions.len() == MAX_SESSIONS {
                    return Err(format!(
                        "line {line}: a block declares at most {MAX_SESSIONS} sessions"
                    ));
                }
                sessions.push(SessionLine {
                    label: label.to_string(),
                    branch: branch.to_string(),
                    text: text.trim_start().to_string(),
                    line,
                });
            }
        }
    }
    if sessions.len() < 2 {
        return Err("a concurrent block declares at least two sessions".into());
    }
    let Some((order_line, order_text)) = order else {
        return Err(
            "a concurrent block needs an `order:` line; sessions with no named interleaving belong to the DST fleet"
                .into(),
        );
    };
    let labels: Vec<&str> = sessions.iter().map(|s| s.label.as_str()).collect();
    let entries =
        parse_order(order_text, &labels).map_err(|e| format!("line {order_line}: {e}"))?;
    Ok((sessions, entries))
}

fn is_label(word: &str) -> bool {
    let mut chars = word.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// A misspelled script line (`order :`, `Order:`), so the refusal names the
/// script instead of a session that does not parse.
fn looks_like_order(raw: &str) -> bool {
    raw.split_once(':')
        .is_some_and(|(head, _)| head.trim().eq_ignore_ascii_case("order"))
}

/// `(label, branch, statement)` when `raw` opens a session: column 0, a
/// label (optionally `on <branch>`), a colon. Any other line is a
/// continuation.
fn session_start(raw: &str) -> Option<(&str, &str, &str)> {
    if raw.starts_with([' ', '\t']) {
        return None;
    }
    let (head, text) = raw.split_once(':')?;
    let words: Vec<&str> = head.split_whitespace().collect();
    let (label, branch) = match words.as_slice() {
        [label] => (*label, "main"),
        [label, "on", branch] => (*label, *branch),
        _ => return None,
    };
    is_label(label).then_some((label, branch, text))
}

/// The `order:` line: comma-separated entries, each `<label> start` (the
/// session's statement begins), `<label>` (the session's completion),
/// `<label> <verb> <key-suffix>` (its next such request, run in turn) or
/// `<label> park <verb> <key-suffix>` (held at arrival until the label's next
/// entry is due). Every label must be declared; a `start` is its label's
/// first entry; the entry after a `park` for its label is that request
/// without `park` or the completion; a completion is its label's last.
pub fn parse_order(text: &str, labels: &[&str]) -> Result<Vec<Entry>, String> {
    let mut entries = Vec::new();
    for item in text.split(',') {
        let words: Vec<&str> = item.split_whitespace().collect();
        let (label, event) = match words.as_slice() {
            [] => return Err("an empty `order:` entry".into()),
            [label] => (*label, Event::Done),
            [label, "start"] => (*label, Event::Start),
            [label, verb, suffix] => (
                *label,
                Event::Call {
                    park: false,
                    verb: verb.parse()?,
                    suffix: (*suffix).to_string(),
                },
            ),
            [label, "park", verb, suffix] => (
                *label,
                Event::Call {
                    park: true,
                    verb: verb.parse()?,
                    suffix: (*suffix).to_string(),
                },
            ),
            _ => {
                return Err(format!(
                    "an `order:` entry is `<label> start`, `<label>`, `<label> <verb> <key-suffix>` or `<label> park <verb> <key-suffix>`, got `{}`",
                    item.trim()
                ));
            }
        };
        let Some(session) = labels.iter().position(|l| *l == label) else {
            return Err(format!(
                "`order:` names `{label}`, which is not a session of the block"
            ));
        };
        entries.push(Entry { session, event });
    }
    if entries.is_empty() {
        return Err("an `order:` line names at least one entry".into());
    }
    for (index, entry) in entries.iter().enumerate() {
        let label = labels[entry.session];
        let later = entries[index + 1..]
            .iter()
            .find(|e| e.session == entry.session);
        if entry.event == Event::Start
            && entries[..index].iter().any(|e| e.session == entry.session)
        {
            return Err(format!(
                "`{label} start` must be the first entry of `{label}`"
            ));
        }
        if matches!(entry.event, Event::Call { park: true, .. }) {
            match later {
                None => {
                    return Err(format!(
                        "`{}` parks `{label}` with no later entry of it to release the park",
                        entry.event
                    ));
                }
                Some(next) if !next.event.releases(&entry.event) => {
                    return Err(format!(
                        "`{}` parks `{label}`; the next entry of `{label}` must be that request without `park`, or its completion `{label}`, got `{}`",
                        entry.event, next.event
                    ));
                }
                Some(_) => {}
            }
        }
        if entry.event == Event::Done && later.is_some() {
            return Err(format!("`{label}` completes before a later entry of it"));
        }
    }
    Ok(entries)
}

/// The `--- expect` body after a block: one `<label>: ok` or
/// `<label>: error: <needle>` line per session, every session named once.
pub fn parse_expect_body(
    body: &[(usize, &str)],
    labels: &[&str],
) -> Result<Vec<SessionExpect>, String> {
    let mut expects: Vec<Option<SessionExpect>> = vec![None; labels.len()];
    for (idx, raw) in body {
        let line = idx + 1;
        if raw.trim().is_empty() {
            continue;
        }
        let Some((label, rest)) = raw.split_once(':') else {
            return Err(format!(
                "line {line}: a block expect line is `<label>: ok` or `<label>: error: <needle>`, got `{raw}`"
            ));
        };
        let label = label.trim();
        let Some(session) = labels.iter().position(|l| *l == label) else {
            return Err(format!(
                "line {line}: `{label}` is not a session of the block"
            ));
        };
        if expects[session].is_some() {
            return Err(format!(
                "line {line}: session `{label}` has two expect lines"
            ));
        }
        let rest = rest.trim();
        let expect = if rest == "ok" {
            SessionExpect::Ok
        } else if let Some(needle) = rest.strip_prefix("error:") {
            let needle = needle.trim();
            if needle.is_empty() {
                return Err(format!(
                    "line {line}: `error:` needs a substring; a bare any-error expectation is refused"
                ));
            }
            SessionExpect::Error {
                needle: needle.to_string(),
            }
        } else {
            return Err(format!(
                "line {line}: a session expects `ok` or `error: <needle>`, got `{rest}`"
            ));
        };
        expects[session] = Some(expect);
    }
    expects
        .into_iter()
        .zip(labels)
        .map(|(expect, label)| {
            expect.ok_or_else(|| format!("session `{label}` has no expect line"))
        })
        .collect()
}
