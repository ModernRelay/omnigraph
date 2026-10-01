//! `--- concurrent`: two to four labeled GQ statements run at the same time
//! on the case's one handle, under DST, in the order the block's `order:`
//! line names.
//!
//! Every session is a future joined on the worker's one seeded, paused
//! runtime, so the interleaving is a property of the seed and the script,
//! never of a thread arbiter. The script is enforced at the object-store
//! seam: the gate decorator sees every request the engine makes, attributes
//! it to the session whose future is being polled (a task-local), and holds
//! a request that matches an entry of the script until the cursor reaches
//! that entry. A `park` entry holds a session at the request's arrival
//! instead, inside whatever lock the engine holds at that point, until the
//! session's next entry is due. A request no entry names runs at once.
//!
//! While a session waits on the script the block drives the paused clock
//! itself (RFC 0045 §Concurrent block): one tick per scheduler turn up to
//! [`STUCK_VIRTUAL_BUDGET`] past the last cursor move or request, so the
//! other sessions' modeled request costs elapse and no engine timer fires;
//! past that budget the clock stands still and the wall clock decides
//! starvation (the block's budget, half the case's `timeout_ms`, at most
//! [`MAX_STARVE_BUDGET`]).

use std::fmt;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::ObjectStoreExt as _;
use object_store::path::Path as OsPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use omnigraph::object_store_seam::{DecorateObjectStore, OBJECT_STORE};
use omnigraph::seams::{Behavior, Global, Installed};
use tokio::sync::{Notify, watch};

use crate::measure::SESSION;

/// The most sessions one block declares.
pub(crate) const MAX_SESSIONS: usize = 4;

/// The longest a block waits, in wall time, with neither a cursor move nor a
/// request before it is starved; the budget is half the case's `timeout_ms`
/// below this, so the parent's kill never precedes the block's own verdict.
pub(crate) const MAX_STARVE_BUDGET: Duration = Duration::from_secs(10);

/// Virtual time the block drives the clock past the last cursor move or
/// request while a session waits on the script: enough for the running
/// sessions' modeled request costs, short of any engine timer.
const STUCK_VIRTUAL_BUDGET: Duration = Duration::from_secs(10);

/// The clock step while the block drives the clock: the measure tick.
const CLOCK_STEP: Duration = crate::measure::TICK;

/// The wall pause per driver turn once the clock stands still, so a stuck
/// block does not spin a core while it waits out its budget.
const SPIN_SLEEP: Duration = Duration::from_millis(1);

/// The cursor value that aborts every waiter.
const ABORTED: usize = usize::MAX;

/// Session labels the measure report keys its own rows by, and the script
/// keyword; a session named like one would merge into that row or be read
/// as the script.
const RESERVED_LABELS: [&str; 4] = ["setup", "runner", "step", "order"];

/// The starvation budget for a case: half its `timeout_ms`, at most
/// [`MAX_STARVE_BUDGET`].
pub(crate) fn starve_budget(timeout_ms: u64) -> Duration {
    Duration::from_millis(timeout_ms / 2).min(MAX_STARVE_BUDGET)
}

/// The store verbs an `order:` entry may name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verb {
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

    fn name(self) -> &'static str {
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
pub(crate) enum SessionKind {
    Query,
    Mutation,
}

impl SessionKind {
    /// The step-kind word the labels and evidence rows carry.
    pub(crate) fn name(self) -> &'static str {
        match self {
            SessionKind::Query => "query",
            SessionKind::Mutation => "mutate",
        }
    }
}

/// One session of a block: a labeled statement on a branch, and the outcome
/// the `--- expect` section after the block names for it.
#[derive(Debug)]
pub(crate) struct SessionOp {
    pub label: String,
    pub branch: String,
    pub source: String,
    pub name: String,
    pub kind: SessionKind,
    pub expect: SessionExpect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionExpect {
    Ok,
    Error { needle: String },
}

/// A `--- concurrent` step: its sessions and the script.
#[derive(Debug)]
pub(crate) struct ConcurrentStep {
    pub ordinal: usize,
    pub sessions: Vec<SessionOp>,
    pub order: Vec<Entry>,
}

/// One `order:` entry: which session, and what event of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    pub session: usize,
    pub event: Event,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event {
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
    fn releases(&self, parked: &Event) -> bool {
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
pub(crate) struct SessionLine {
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
pub(crate) fn parse_block(
    body: &[(usize, &str)],
) -> Result<(Vec<SessionLine>, Vec<Entry>), String> {
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
pub(crate) fn parse_order(text: &str, labels: &[&str]) -> Result<Vec<Entry>, String> {
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
pub(crate) fn parse_expect_body(
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

/// One block's run: the script, the cursor every gated request waits on, and
/// what happened, for the evidence row.
pub(crate) struct Run {
    entries: Vec<Entry>,
    labels: Vec<String>,
    /// The index of the entry due next; `ABORTED` wakes every waiter.
    cursor: watch::Sender<usize>,
    /// Per session, the index of its next entry of the script.
    next: Mutex<Vec<usize>>,
    /// Grants, parks and completions in order, with the wall time since the
    /// block began: measurement-only.
    log: Mutex<Vec<serde_json::Value>>,
    /// Requests made while no session was being polled (a Lance pool thread
    /// or a task the engine spawned): they run at once and no entry can name
    /// them.
    unattributed: AtomicU64,
    failure: Mutex<Option<String>>,
    /// The cursor at the first failure: the entry the script stopped at.
    stuck_at: Mutex<Option<usize>>,
    /// Shared with every session's task-local: set once the block is
    /// aborted, so the measure ledger files the drain under `after_abort`.
    draining: Arc<AtomicBool>,
    began: Instant,
    /// Sessions waiting on the cursor right now; the clock driver runs
    /// while there is one.
    waiters: AtomicUsize,
    wake_driver: Notify,
    /// When the cursor last moved or a request last arrived: virtual, then
    /// wall.
    moved: Mutex<(tokio::time::Instant, Instant)>,
    starve_budget: Duration,
}

/// A registered waiter on the cursor; the count drops with it, so a wait
/// the engine cancels (a dropped future) never leaves the driver running.
struct Waiting<'a>(&'a Run);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

static ACTIVE: Mutex<Option<Arc<Run>>> = Mutex::new(None);

/// Whether a block is running: the gate's fast path, so a case with no block
/// pays one atomic load per request and never the mutex.
static LIVE: AtomicBool = AtomicBool::new(false);

fn active() -> Option<Arc<Run>> {
    if !LIVE.load(Ordering::Acquire) {
        return None;
    }
    ACTIVE.lock().unwrap().clone()
}

/// What a block reports once every session returned.
pub(crate) struct Outcome {
    /// The entry the cursor stopped at, `None` when the script ran through.
    pub stuck_at: Option<usize>,
    pub failure: Option<String>,
    pub unattributed: u64,
    pub log: Vec<serde_json::Value>,
    pub wall_ms: u64,
}

impl Run {
    /// Start the block: the run becomes the process's active block. One
    /// block at a time by construction (steps are sequential, a block in a
    /// loop is refused at parse); a second one is a harness defect.
    pub(crate) fn begin(step: &ConcurrentStep, starve_budget: Duration) -> Arc<Run> {
        let labels: Vec<String> = step.sessions.iter().map(|s| s.label.clone()).collect();
        let next = (0..labels.len())
            .map(|session| next_entry(&step.order, session, 0))
            .collect();
        let run = Arc::new(Run {
            entries: step.order.clone(),
            labels,
            cursor: watch::Sender::new(0),
            next: Mutex::new(next),
            log: Mutex::new(Vec::new()),
            unattributed: AtomicU64::new(0),
            failure: Mutex::new(None),
            stuck_at: Mutex::new(None),
            draining: Arc::new(AtomicBool::new(false)),
            began: Instant::now(),
            waiters: AtomicUsize::new(0),
            wake_driver: Notify::new(),
            moved: Mutex::new((tokio::time::Instant::now(), Instant::now())),
            starve_budget,
        });
        let previous = ACTIVE.lock().unwrap().replace(Arc::clone(&run));
        assert!(
            previous.is_none(),
            "a concurrent block began while another block was still active"
        );
        LIVE.store(true, Ordering::Release);
        run
    }

    /// The flag every session's task-local shares with this run.
    pub(crate) fn draining_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.draining)
    }

    /// Drive the clock while a session waits on the script (module doc).
    /// Polled beside the sessions for the block's whole life; never returns.
    pub(crate) async fn drive_clock(&self) {
        loop {
            if self.waiters.load(Ordering::SeqCst) == 0 || self.aborted() {
                self.wake_driver.notified().await;
                continue;
            }
            let (moved_virtual, moved_wall) = *self.moved.lock().unwrap();
            if tokio::time::Instant::now().saturating_duration_since(moved_virtual)
                < STUCK_VIRTUAL_BUDGET
            {
                tokio::time::advance(CLOCK_STEP).await;
            } else if moved_wall.elapsed() >= self.starve_budget {
                let at = *self.cursor.borrow();
                let position = match at.checked_sub(1) {
                    Some(previous) if previous < self.entries.len() => {
                        format!("after entry {at} `{}`", self.entries[previous].event)
                    }
                    _ => format!(
                        "before entry 1 `{}`",
                        self.entries
                            .first()
                            .map(|e| e.event.to_string())
                            .unwrap_or_default()
                    ),
                };
                self.fail(format!(
                    "starved: neither an entry nor a request arrived for {:.1} s of wall time {position}",
                    self.starve_budget.as_secs_f64()
                ));
            } else {
                std::thread::sleep(SPIN_SLEEP);
            }
            tokio::task::yield_now().await;
        }
    }

    /// The block is over: no longer the active block; the outcome for the
    /// evidence row.
    pub(crate) fn end(self: &Arc<Run>) -> Outcome {
        LIVE.store(false, Ordering::Release);
        *ACTIVE.lock().unwrap() = None;
        Outcome {
            stuck_at: *self.stuck_at.lock().unwrap(),
            failure: self.failure.lock().unwrap().clone(),
            unattributed: self.unattributed.load(Ordering::SeqCst),
            log: std::mem::take(&mut *self.log.lock().unwrap()),
            wall_ms: self.began.elapsed().as_millis() as u64,
        }
    }

    /// The session is about to run its statement: its `start` entry, if the
    /// script names one, is taken in turn.
    pub(crate) async fn start_session(&self, session: usize) -> Result<(), String> {
        let Some(j) = self.next_of(session) else {
            return Ok(());
        };
        if self.entries[j].event != Event::Start {
            return Ok(());
        }
        self.wait_for(j).await;
        if self.aborted() {
            return Err(self.failure_text());
        }
        self.note(session, j, "started");
        self.advance(j, session);
        Ok(())
    }

    /// The session's future returned: its `done` entry, if the script names
    /// one, is taken in turn; a call entry it never made fails the block. A
    /// session returning after the abort is the drain, not a new failure.
    pub(crate) async fn finish_session(&self, session: usize) -> Result<(), String> {
        if self.aborted() {
            return Err(self.failure_text());
        }
        let Some(j) = self.next_of(session) else {
            return Ok(());
        };
        match &self.entries[j].event {
            Event::Done => {
                self.wait_for(j).await;
                if self.aborted() {
                    return Err(self.failure_text());
                }
                self.note(session, j, "done");
                self.advance(j, session);
                Ok(())
            }
            call => {
                let message = format!(
                    "session `{}` finished without its entry {} `{call}`",
                    self.labels[session],
                    j + 1
                );
                self.fail(message.clone());
                Err(message)
            }
        }
    }

    fn next_of(&self, session: usize) -> Option<usize> {
        let next = self.next.lock().unwrap()[session];
        (next < self.entries.len()).then_some(next)
    }

    fn aborted(&self) -> bool {
        *self.cursor.borrow() == ABORTED
    }

    fn failure_text(&self) -> String {
        self.failure
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "the block was aborted".into())
    }

    /// The first failure wins and records where the cursor stood; every
    /// waiter wakes and every later gate is a pass-through, so the sessions
    /// drain and the block reports.
    fn fail(&self, message: String) {
        let mut failure = self.failure.lock().unwrap();
        if failure.is_none() {
            *failure = Some(message);
            let cursor = *self.cursor.borrow();
            *self.stuck_at.lock().unwrap() = (cursor < self.entries.len()).then_some(cursor);
        }
        drop(failure);
        self.draining.store(true, Ordering::SeqCst);
        self.cursor.send_replace(ABORTED);
        self.wake_driver.notify_one();
    }

    async fn wait_for(&self, entry: usize) {
        if *self.cursor.borrow() >= entry {
            return;
        }
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting(self);
        self.wake_driver.notify_one();
        let mut rx = self.cursor.subscribe();
        let _ = rx.wait_for(|cursor| *cursor >= entry).await;
    }

    /// Progress: the budgets count from here.
    fn touch(&self) {
        *self.moved.lock().unwrap() = (tokio::time::Instant::now(), Instant::now());
    }

    /// Entry `at` happened: the cursor moves past it and the session's next
    /// entry is looked up.
    fn advance(&self, at: usize, session: usize) {
        let moved = self.cursor.send_if_modified(|cursor| {
            if *cursor == at {
                *cursor = at + 1;
                true
            } else {
                false
            }
        });
        if moved {
            self.touch();
        }
        let mut next = self.next.lock().unwrap();
        next[session] = next_entry(&self.entries, session, at + 1);
    }

    fn note(&self, session: usize, entry: usize, what: &'static str) {
        self.log.lock().unwrap().push(serde_json::json!({
            "session": self.labels[session],
            "entry": entry + 1,
            "event": self.entries[entry].event.to_string(),
            "what": what,
            "wall_ms": self.began.elapsed().as_millis() as u64,
        }));
    }
}

/// The index of `session`'s first entry at or after `from`, or the script's
/// length when it has none.
fn next_entry(entries: &[Entry], session: usize, from: usize) -> usize {
    entries[from.min(entries.len())..]
        .iter()
        .position(|e| e.session == session)
        .map_or(entries.len(), |offset| from + offset)
}

/// What a gated request holds while it runs: the entry it fulfils, or nothing;
/// completion moves the cursor, and so does a drop (a store future the engine
/// cancelled), noted as `dropped`, so the script never stalls on it.
struct Permit(Option<Held>);

struct Held {
    run: Arc<Run>,
    session: usize,
    entry: usize,
}

impl Permit {
    fn complete(mut self) {
        self.settle("completed");
    }

    fn settle(&mut self, what: &'static str) {
        if let Some(held) = self.0.take() {
            held.run.note(held.session, held.entry, what);
            held.run.advance(held.entry, held.session);
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.settle("dropped");
    }
}

/// A request outside the gate (the control realm's, through the engine's
/// `StorageAdapter`, which a measured run wraps) is progress for a live
/// block's budgets like a gated one.
pub(crate) fn touch() {
    if let Some(run) = active() {
        run.touch();
    }
}

/// Hold the request until its turn, if the script names it. Every request
/// of a live block, named or not, is progress for the budgets.
async fn gate(verb: Verb, key: &str) -> Permit {
    let Some(run) = active() else {
        return Permit(None);
    };
    run.touch();
    let Ok(session) = SESSION.try_with(|s| s.index) else {
        run.unattributed.fetch_add(1, Ordering::SeqCst);
        return Permit(None);
    };
    let Some(j) = run.next_of(session) else {
        return Permit(None);
    };
    let Event::Call {
        park,
        verb: named,
        suffix,
    } = &run.entries[j].event
    else {
        return Permit(None);
    };
    if *named != verb || !key.contains(suffix.as_str()) {
        return Permit(None);
    }
    let park = *park;
    run.wait_for(j).await;
    if run.aborted() {
        return Permit(None);
    }
    if !park {
        run.note(session, j, "granted");
        return Permit(Some(Held {
            run: Arc::clone(&run),
            session,
            entry: j,
        }));
    }
    run.note(session, j, "parked");
    run.advance(j, session);
    let Some(release) = run.next_of(session) else {
        return Permit(None);
    };
    run.wait_for(release).await;
    if run.aborted() {
        return Permit(None);
    }
    match &run.entries[release].event {
        Event::Call { park: false, .. } => {
            run.note(session, release, "granted");
            Permit(Some(Held {
                run: Arc::clone(&run),
                session,
                entry: release,
            }))
        }
        Event::Done => {
            run.note(session, j, "released");
            Permit(None)
        }
        other => {
            run.fail(format!(
                "session `{}` was parked at `{}` and released into entry {} `{other}`, a shape the parser refuses; harness defect",
                run.labels[session],
                run.entries[j].event,
                release + 1
            ));
            Permit(None)
        }
    }
}

static INSTALLED: OnceLock<Installed<dyn DecorateObjectStore, Global<dyn DecorateObjectStore>>> =
    OnceLock::new();

/// Install the gate on the object-store seam for the rest of the process,
/// outside `inner` (the measure decorator when the run measures), so a
/// request's arrival is gated before its cost is charged. Idempotent.
pub(crate) fn install(inner: Option<Arc<dyn DecorateObjectStore>>) {
    INSTALLED.get_or_init(|| OBJECT_STORE.install(Arc::new(GateDecorator { inner })));
}

struct GateDecorator {
    inner: Option<Arc<dyn DecorateObjectStore>>,
}

impl Behavior for GateDecorator {}

impl DecorateObjectStore for GateDecorator {
    fn wrap(&self, base: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        let inner = match &self.inner {
            Some(decorator) => decorator.wrap(base),
            None => base,
        };
        Arc::new(GateStore { inner })
    }
}

#[derive(Debug)]
struct GateStore {
    inner: Arc<dyn ObjectStore>,
}

impl fmt::Display for GateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GqtGated({})", self.inner)
    }
}

/// A multipart write's permit completes when the upload completes or aborts,
/// not when its handle is created: the entry names the whole write.
struct GatedUpload {
    inner: Box<dyn MultipartUpload>,
    permit: Option<Permit>,
}

impl fmt::Debug for GatedUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GatedUpload({:?})", self.inner)
    }
}

#[async_trait]
impl MultipartUpload for GatedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let result = self.inner.complete().await;
        if let Some(permit) = self.permit.take() {
            permit.complete();
        }
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let result = self.inner.abort().await;
        if let Some(permit) = self.permit.take() {
            permit.complete();
        }
        result
    }
}

/// A listing's permit completes on the stream's first item or end, when the
/// listing has run, not when the lazy stream is created.
struct CompleteOnFirst<S> {
    inner: S,
    permit: Option<Permit>,
}

impl<S: Stream + Unpin> Stream for CompleteOnFirst<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = Pin::new(&mut self.inner).poll_next(cx);
        if polled.is_ready()
            && let Some(permit) = self.permit.take()
        {
            permit.complete();
        }
        polled
    }
}

#[async_trait]
impl ObjectStore for GateStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let permit = gate(Verb::Put, location.as_ref()).await;
        let result = self.inner.put_opts(location, payload, opts).await;
        permit.complete();
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let permit = gate(Verb::Put, location.as_ref()).await;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(GatedUpload {
            inner,
            permit: Some(permit),
        }))
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let verb = if options.head { Verb::Head } else { Verb::Get };
        let permit = gate(verb, location.as_ref()).await;
        let result = self.inner.get_opts(location, options).await;
        permit.complete();
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<OsPath>>,
    ) -> BoxStream<'static, object_store::Result<OsPath>> {
        let inner = Arc::clone(&self.inner);
        locations
            .then(move |path| {
                let inner = Arc::clone(&inner);
                async move {
                    let path = path?;
                    let permit = gate(Verb::Delete, path.as_ref()).await;
                    let result = inner.delete(&path).await;
                    permit.complete();
                    result?;
                    Ok(path)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let path = prefix
                .as_ref()
                .map(|p| p.as_ref())
                .unwrap_or_default()
                .to_string();
            let permit = gate(Verb::List, &path).await;
            CompleteOnFirst {
                inner: inner.list(prefix.as_ref()),
                permit: Some(permit),
            }
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<ListResult> {
        let path = prefix.map(|p| p.as_ref()).unwrap_or_default();
        let permit = gate(Verb::List, path).await;
        let result = self.inner.list_with_delimiter(prefix).await;
        permit.complete();
        result
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let permit = gate(Verb::Copy, to.as_ref()).await;
        let result = self.inner.copy_opts(from, to, options).await;
        permit.complete();
        result
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        Event, SessionExpect, Verb, parse_block, parse_expect_body, parse_order, starve_budget,
    };

    fn body(text: &str) -> Vec<(usize, &str)> {
        text.lines().enumerate().collect()
    }

    #[test]
    fn a_block_splits_sessions_and_the_script() {
        let (sessions, order) = parse_block(&body(
            "w1: query add() {\n    insert Person { name: \"bob\" }\n}\nr1 on feature: query all() { match { $p: Person } return { $p.name } }\norder: w1 park put _versions/, r1, w1 put _versions/\n",
        ))
        .unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].label, "w1");
        assert_eq!(sessions[0].branch, "main");
        assert!(sessions[0].text.contains("insert Person"));
        assert_eq!(sessions[1].branch, "feature");
        assert_eq!(sessions[1].line, 4);
        assert_eq!(order.len(), 3);
        assert_eq!(
            order[0].event,
            Event::Call {
                park: true,
                verb: Verb::Put,
                suffix: "_versions/".into()
            }
        );
        assert_eq!(order[1].event, Event::Done);
        assert_eq!(order[1].session, 1);
    }

    #[test]
    fn a_block_refuses_the_shapes_it_cannot_run() {
        let refused = |text: &str| parse_block(&body(text)).unwrap_err();
        assert!(refused("w1: query a() {}\norder: w1\n").contains("at least two sessions"));
        assert!(refused("w1: query a() {}\nw2: query b() {}\n").contains("needs an `order:` line"));
        assert!(
            refused("w1: query a() {}\nw1: query b() {}\norder: w1\n").contains("declared twice")
        );
        assert!(refused("W1: query a() {}\nw2: query b() {}\norder: w2\n").contains("opens with"));
        assert!(
            refused("step: query a() {}\nw2: query b() {}\norder: w2\n").contains("is reserved")
        );
        assert!(
            refused("w1: query a() {}\nw2: query b() {}\nOrder: w1\n")
                .contains("the script line is `order:`")
        );
        assert!(
            refused("w1: query a() {}\nw2: query b() {}\norder: w3\n").contains("not a session")
        );
        assert!(
            refused("w1: query a() {}\nw2: query b() {}\norder: w1 park put x\n")
                .contains("no later entry")
        );
        assert!(
            refused("w1: query a() {}\nw2: query b() {}\norder: w1 park put x, w2, w1 get y\n")
                .contains("must be that request without `park`")
        );
        assert!(
            refused("w1: query a() {}\nw2: query b() {}\norder: w1, w1 put x\n")
                .contains("completes before a later entry")
        );
        assert!(
            refused("w1: query a() {}\nw2: query b() {}\norder: w1 fetch x\n").contains("verb")
        );
        assert!(
            refused("w1: query a() {}\nw2: query b() {}\norder: w1\n    stray\n")
                .contains("cannot continue after it")
        );
        assert!(refused("    indented\nw1: query a() {}\norder: w1\n").contains("opens with"));
        assert!(parse_order("", &["a"]).is_err());
        assert_eq!(parse_order("a", &["a", "b"]).unwrap()[0].event, Event::Done);
        assert_eq!(
            parse_order("b start, a", &["a", "b"]).unwrap()[0].event,
            Event::Start
        );
        assert!(
            parse_order("b put x, b start", &["a", "b"])
                .unwrap_err()
                .contains("must be the first entry")
        );
        assert!(parse_order("a park put x, b, a put x", &["a", "b"]).is_ok());
        assert!(parse_order("a park put x, b, a", &["a", "b"]).is_ok());
    }

    #[test]
    fn the_expect_body_names_every_session_once() {
        let labels = ["w1", "r1"];
        let expects = parse_expect_body(&body("w1: ok\nr1: error: conflict\n"), &labels).unwrap();
        assert_eq!(expects[0], SessionExpect::Ok);
        assert_eq!(
            expects[1],
            SessionExpect::Error {
                needle: "conflict".into()
            }
        );
        assert!(
            parse_expect_body(&body("w1: ok\n"), &labels)
                .unwrap_err()
                .contains("no expect line")
        );
        assert!(
            parse_expect_body(&body("w1: ok\nw1: ok\nr1: ok\n"), &labels)
                .unwrap_err()
                .contains("two expect lines")
        );
        assert!(
            parse_expect_body(&body("w1: ok\nr1: error:\n"), &labels)
                .unwrap_err()
                .contains("needs a substring")
        );
        assert!(
            parse_expect_body(&body("w1: ok\nr1: rows\n"), &labels)
                .unwrap_err()
                .contains("`ok` or `error:")
        );
    }

    #[test]
    fn the_starvation_budget_is_half_the_case_budget_capped() {
        assert_eq!(starve_budget(10_000), Duration::from_secs(5));
        assert_eq!(starve_budget(600_000), Duration::from_secs(10));
        assert_eq!(starve_budget(1), Duration::from_millis(0));
    }
}
