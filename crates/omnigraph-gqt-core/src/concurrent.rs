use std::sync::Arc;
use std::{fmt, str::FromStr};

use omnigraph::loader::LoadMode;
use omnigraph_compiler::query::file::{BranchStmt, BranchWrite, FileBody};
use omnigraph_compiler::query::parser::parse_query;

use crate::diagnostic::{CaseError, CaseErrorCode};
use crate::generate::{self, Generated};

/// How many labels a block declares at most; a label's worker count is
/// bounded apart, by the host at admission (`SCRIPTED_HOST_WORKERS`).
pub const MAX_LABELS: usize = 4;
/// How many times a host runs a session's statement again after the engine
/// answered `ReadSetChanged`, each counted as an authority conflict; the
/// engine's own pre-effect budget, once more at the statement level.
pub const MAX_HOST_RETRIES: u32 = 32;
/// How many session orders the plain host runs a free-running block in at
/// most: every permutation when the block has at most three sessions, this
/// many seeded shuffles above that, the declaration order always the first.
pub const PLAIN_PASSES: usize = 6;
const RESERVED_LABELS: [&str; 4] = ["setup", "runner", "step", "order"];

/// The most sessions whose every order the plain host runs; above it the
/// passes are [`PLAIN_PASSES`] seeded shuffles and no factorial is computed
/// (an admitted block of 24 sessions has a 24! that overflows `usize`).
const EXACT_SESSIONS: usize = 3;
const _: () = assert!(
    factorial(EXACT_SESSIONS) == PLAIN_PASSES,
    "the exact bound is the session count whose factorial is PLAIN_PASSES"
);

const fn factorial(n: usize) -> usize {
    let mut product = 1;
    let mut factor = 2;
    while factor <= n {
        product *= factor;
        factor += 1;
    }
    product
}

/// How many passes the plain host runs a block of `sessions` sessions in.
pub fn passes(sessions: usize) -> usize {
    if sessions <= EXACT_SESSIONS {
        factorial(sessions)
    } else {
        PLAIN_PASSES
    }
}

/// Which run of the case the plain host is on, `Pass(0)` the first; the
/// index of a session order, never a session count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pass(pub usize);

/// The session order of pass `pass` over `sessions` sessions: pass 0 is the
/// declaration order; while the orders fit in [`PLAIN_PASSES`] every order
/// appears once, in lexicographic rank; above that each later pass is a
/// seeded shuffle. The case's pass count is its largest block's
/// ([`crate::Case::plain_passes`]), so a smaller block cycles its own orders.
pub fn permutation(sessions: usize, pass: Pass) -> Vec<usize> {
    let index = pass.0 % passes(sessions);
    let mut order: Vec<usize> = (0..sessions).collect();
    if index == 0 {
        return order;
    }
    if sessions <= EXACT_SESSIONS {
        let mut rank = index;
        let mut rest = order;
        order = Vec::with_capacity(sessions);
        for place in (1..=sessions).rev() {
            let block = (1..place).product::<usize>();
            order.push(rest.remove(rank / block));
            rank %= block;
        }
        return order;
    }
    let mut state = index as u64;
    let mut next = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    for i in (1..sessions).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        order.swap(i, j);
    }
    order
}

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

/// What a session's statement is, with what the executor needs for it: the
/// executor and the comparator differ.
#[derive(Clone, Debug)]
pub enum SessionKind {
    Query,
    Mutation,
    /// A `load generate:` recipe the host deals rows from, one per run,
    /// shared by the label's workers.
    Load(Arc<Dealt>),
    /// A `branch merge <source> into <target>` control write.
    Merge(MergeOp),
}

impl SessionKind {
    /// The step-kind word the labels and evidence rows carry.
    pub fn name(&self) -> &'static str {
        match self {
            SessionKind::Query => "query",
            SessionKind::Mutation => "mutate",
            SessionKind::Load(_) => "load",
            SessionKind::Merge(_) => "branch merge",
        }
    }
}

/// The recipe a `load generate:` session deals rows from: one table with a
/// `kind: key` column, so every dealt row is readable by its key after the
/// block. Worker `w` on its run `s` loads row `w + workers * s`.
#[derive(Debug)]
pub struct Dealt {
    pub generated: Generated,
    pub mode: LoadMode,
}

/// The operands of a `branch merge` session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeOp {
    pub source: String,
    pub into: Option<String>,
    pub as_commit: bool,
}

/// A session's place under its label: its index and the label's worker
/// count, which only make sense together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Worker {
    pub index: usize,
    pub count: usize,
}

/// One session of a block: a labeled statement on a branch, and the outcome
/// the `--- expect` section after the block names for it. A session line
/// with a worker count `x<n>` is `n` sessions, `<label>.0` to
/// `<label>.<n-1>`, all under the declared label `group`.
#[derive(Debug)]
pub struct SessionOp {
    /// The session's own name: the declared label, or `<label>.<worker>`
    /// under a worker count.
    pub label: String,
    /// The label the session line declared, which the `--- expect` names.
    pub group: String,
    pub worker: Worker,
    pub branch: String,
    pub source: String,
    pub name: String,
    pub kind: SessionKind,
    pub expect: SessionExpect,
}

impl SessionOp {
    /// The row of a dealt recipe this worker loads on its run `seq`: worker
    /// `w` of `n` takes row `w + n·s`, so the workers' keys are disjoint and
    /// the readback after the block asks for the same row.
    pub fn dealt_index(&self, seq: u64) -> u64 {
        self.worker.index as u64 + self.worker.count as u64 * seq
    }
}

/// The session name of `worker` under `label`: the label itself for a
/// single worker, `<label>.<index>` under a count.
pub fn worker_label(label: &str, worker: Worker) -> String {
    if worker.count == 1 {
        label.to_string()
    } else {
        format!("{label}.{}", worker.index)
    }
}

/// The sessions of a block from its parsed lines: every worker of every
/// label as one [`SessionOp`], its `${worker}` resolved, its statement
/// parsed and checked for the kinds a block admits (one query or mutation
/// declaration with no parameters and no settings, a `branch merge`, or a
/// `load generate:` recipe whose rows the workers are dealt).
/// `needs_indices` is set when a statement needs the case's indexes.
pub fn build_sessions(
    lines: Vec<SessionLine>,
    needs_indices: &mut bool,
) -> Result<Vec<SessionOp>, CaseError> {
    let mut sessions = Vec::with_capacity(lines.len());
    for session in lines {
        let at_line = session.line;
        let label = session.label.clone();
        let step_kind =
            |message: String| CaseError::new(CaseErrorCode::StepKind, message).at_line(at_line);
        let unbound = |e: String| {
            CaseError::new(CaseErrorCode::SubstitutionPlacement, e)
                .fix("write the value, or `${worker}` for the worker's index")
                .at_line(at_line)
        };
        let (first, recipe_body) = session
            .text
            .split_once('\n')
            .unwrap_or((session.text.as_str(), ""));
        if let Some(arguments) = first.trim().strip_prefix("load ")
            && arguments.trim_start().starts_with("generate:")
        {
            if arguments.contains("branch:") {
                return Err(step_kind(format!(
                    "session `{label}`: a session's branch is its `on <branch>`; drop `branch:` from the recipe header"
                )));
            }
            if recipe_body.contains("${") {
                return Err(unbound(format!(
                    "session `{label}`: `${{` inside a dealt recipe; the recipe is one text shared by the label's workers, so `${{worker}}` is bound on the session line and nowhere in the recipe"
                )));
            }
            let shape = |e: String| {
                CaseError::new(CaseErrorCode::BodyShape, format!("session `{label}`: {e}"))
                    .at_line(at_line)
            };
            let (seed, mode, _) =
                generate::parse_arguments(arguments.trim(), true).map_err(shape)?;
            let generated = Generated::parse(seed, &crate::dedent(recipe_body))
                .map_err(|e| e.at_line(at_line))?;
            generate::require_batches(
                &generated,
                "give the table `rows:` and `commits:` of at least 1",
            )?;
            let (rows, _) = generated.dealt_shape().map_err(shape)?;
            if rows < session.workers as u64 {
                return Err(shape(format!(
                    "the recipe deals {rows} rows to {} workers; one pass deals row w to worker w, so `rows:` is at least the worker count",
                    session.workers
                )));
            }
            let dealt = Arc::new(Dealt { generated, mode });
            for index in 0..session.workers {
                let worker = Worker {
                    index,
                    count: session.workers,
                };
                sessions.push(SessionOp {
                    label: worker_label(&label, worker),
                    group: label.clone(),
                    worker,
                    branch: resolve_worker(&session.branch, index).map_err(unbound)?,
                    source: session.text.clone(),
                    name: "load".into(),
                    kind: SessionKind::Load(Arc::clone(&dealt)),
                    expect: SessionExpect::Ok,
                });
            }
            continue;
        }
        for index in 0..session.workers {
            let worker = Worker {
                index,
                count: session.workers,
            };
            let branch = resolve_worker(&session.branch, index).map_err(unbound)?;
            let text = resolve_worker(&session.text, index).map_err(unbound)?;
            let file = parse_query(&text).map_err(|e| {
                CaseError::new(
                    CaseErrorCode::QueryParse,
                    format!("session `{label}` does not parse: {e}"),
                )
                .fix("rewrite the session's statement so it parses; the message names where the grammar stopped")
                .at_line(at_line)
            })?;
            if !file.settings.is_empty() {
                return Err(step_kind(format!(
                    "session `{label}` carries `set` or `reset` lines; a block session is one statement"
                )));
            }
            let (kind, name) = match file.body {
                FileBody::Queries(decls) => {
                    let [decl] = decls.as_slice() else {
                        return Err(step_kind(format!(
                            "session `{label}` holds exactly one declaration, got {}",
                            decls.len()
                        )));
                    };
                    if !decl.params.is_empty() {
                        return Err(step_kind(format!(
                            "session `{label}` declares parameters; a block session takes none"
                        )));
                    }
                    crate::inspect_decl(decl, needs_indices)?;
                    let kind = if !decl.is_mutation() {
                        SessionKind::Query
                    } else {
                        SessionKind::Mutation
                    };
                    (kind, decl.name.clone())
                }
                FileBody::Branch(BranchStmt::Write(BranchWrite::Merge {
                    source,
                    into,
                    as_commit,
                })) => (
                    SessionKind::Merge(MergeOp {
                        source,
                        into,
                        as_commit,
                    }),
                    "branch merge".to_string(),
                ),
                _ => {
                    return Err(step_kind(format!(
                        "session `{label}` must be a query or mutation declaration, a `branch merge` or a `load generate:` recipe; show, explain and the other branch statements are not admitted in a block"
                    )));
                }
            };
            sessions.push(SessionOp {
                label: worker_label(&label, worker),
                group: label.clone(),
                worker,
                branch,
                source: text,
                name,
                kind,
                expect: SessionExpect::Ok,
            });
        }
    }
    Ok(sessions)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionExpect {
    Ok,
    Error { needle: String },
}

/// A `--- concurrent` step: its sessions and the script. An empty script is
/// a free-running block: the sessions run with no named interleaving, once
/// each under a scripted clock (in every order on the plain host, by seed
/// under DST) and repeatedly for a protocol's duration under a measuring
/// host.
#[derive(Debug)]
pub struct ConcurrentStep {
    pub ordinal: usize,
    pub sessions: Vec<SessionOp>,
    pub order: Vec<Entry>,
}

impl ConcurrentStep {
    /// Whether the block has no `order:` line, so no host holds a session.
    pub fn is_free_running(&self) -> bool {
        self.order.is_empty()
    }

    /// The most workers one label of the block declares.
    pub fn max_workers(&self) -> usize {
        self.sessions
            .iter()
            .map(|op| op.worker.count)
            .max()
            .unwrap_or(0)
    }

    /// The labels of the block's sessions in the order pass `pass` runs
    /// them on the plain host.
    pub fn order_labels(&self, pass: Pass) -> Vec<&str> {
        permutation(self.sessions.len(), pass)
            .into_iter()
            .map(|i| self.sessions[i].label.as_str())
            .collect()
    }
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
/// label, its worker count (`x<n>`, 1 when unspelled), the branch (`main`
/// unless `on <branch>`, `${worker}` still unresolved), the statement text
/// and the line the statement starts on.
#[derive(Debug, PartialEq, Eq)]
pub struct SessionLine {
    pub label: String,
    pub workers: usize,
    pub branch: String,
    pub text: String,
    pub line: usize,
}

/// The block body split into session lines and the script. A session starts
/// at column 0 as `<label>[ x<n>][ on <branch>]: <statement>`; every other
/// line continues the open session's statement (a GQ body closes with `}` at
/// column 0, so continuation is not a matter of indentation); `order:` at
/// column 0 is the script and closes the sessions. The header says which
/// block it is: `--- concurrent scripted` (`scripted`) ends with its
/// `order:` line, `--- concurrent` takes none and its script is empty; the
/// other shape is refused, never read as the other block. A worker count
/// above one belongs to a free-running block, since a script names sessions
/// one by one.
pub fn parse_block(
    body: &[(usize, &str)],
    scripted: bool,
) -> Result<(Vec<SessionLine>, Vec<Entry>), String> {
    let mut sessions: Vec<SessionLine> = Vec::new();
    let mut order: Option<(usize, &str)> = None;
    for (idx, raw) in body {
        let line = idx + 1;
        if raw.trim().is_empty() {
            continue;
        }
        if let Some(rest) = raw.strip_prefix("order:") {
            if !scripted {
                return Err(format!(
                    "line {line}: `--- concurrent` is a free-running block and takes no `order:` line; a block with a script is `--- concurrent scripted`"
                ));
            }
            if order.is_some() {
                return Err(format!("line {line}: a block has one `order:` line"));
            }
            order = Some((line, rest));
            continue;
        }
        match session_start(raw).map_err(|e| format!("line {line}: {e}"))? {
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
                        "line {line}: a block opens with `<label>[ x<n>][ on <branch>]: <statement>` at column 0, got `{raw}`"
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
            Some((label, workers, branch, text)) => {
                if RESERVED_LABELS.contains(&label) {
                    return Err(format!(
                        "line {line}: `{label}` is reserved (the measure report's own rows and the script keyword); name the session otherwise"
                    ));
                }
                if sessions.iter().any(|s| s.label == label) {
                    return Err(format!("line {line}: session `{label}` is declared twice"));
                }
                if sessions.len() == MAX_LABELS {
                    return Err(format!(
                        "line {line}: a block declares at most {MAX_LABELS} labels; workers under one label are `x<n>`"
                    ));
                }
                sessions.push(SessionLine {
                    label: label.to_string(),
                    workers,
                    branch: branch.to_string(),
                    text: text.trim_start().to_string(),
                    line,
                });
            }
        }
    }
    if sessions.iter().map(|s| s.workers).sum::<usize>() < 2 {
        return Err(
            "a concurrent block declares at least two sessions: two labels, or one label with `x2` or more"
                .into(),
        );
    }
    let Some((order_line, order_text)) = order else {
        if scripted {
            return Err(
                "`--- concurrent scripted` ends with its `order:` line; a block with no named interleaving is `--- concurrent`"
                    .into(),
            );
        }
        return Ok((sessions, Vec::new()));
    };
    if let Some(counted) = sessions.iter().find(|s| s.workers > 1) {
        return Err(format!(
            "line {}: `{} x{}` declares workers, which an `order:` line cannot name one by one; a worker count belongs to a free-running `--- concurrent` block",
            counted.line, counted.label, counted.workers
        ));
    }
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

/// `(label, workers, branch, statement)` when `raw` opens a session (column
/// 0, a label, optionally `x<n>` and `on <branch>`, a colon); any other line
/// continues a statement, and `x` with no positive integer is an error.
fn session_start(raw: &str) -> Result<Option<(&str, usize, &str, &str)>, String> {
    if raw.starts_with([' ', '\t']) {
        return Ok(None);
    }
    let Some((head, text)) = raw.split_once(':') else {
        return Ok(None);
    };
    let words: Vec<&str> = head.split_whitespace().collect();
    let (label, count, branch) = match words.as_slice() {
        [label] => (*label, None, "main"),
        [label, "on", branch] => (*label, None, *branch),
        [label, count] => (*label, Some(*count), "main"),
        [label, count, "on", branch] => (*label, Some(*count), *branch),
        _ => return Ok(None),
    };
    if !is_label(label) {
        return Ok(None);
    }
    let workers = match count {
        None => 1,
        Some(count) => {
            let Some(digits) = count.strip_prefix('x') else {
                return Ok(None);
            };
            match digits.parse::<usize>() {
                Ok(n) if n >= 1 && digits.bytes().all(|b| b.is_ascii_digit()) => n,
                _ => {
                    return Err(format!(
                        "`{count}` is not a worker count; a count is `x<n>` with `n` a positive integer"
                    ));
                }
            }
        }
    };
    Ok(Some((label, workers, branch, text)))
}

/// A session line's text (its `on <branch>` operand or its statement) with
/// `${worker}` replaced by the worker's index; any other substitution is
/// refused, since nothing else is bound inside a block.
pub fn resolve_worker(text: &str, worker: usize) -> Result<String, String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find("${") {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 2..];
        let Some(close) = after.find('}') else {
            return Err("unterminated `${` on a session line".into());
        };
        match &after[..close] {
            "worker" => out.push_str(&worker.to_string()),
            other => {
                return Err(format!(
                    "`${{{other}}}` on a session line: only `${{worker}}` is bound inside a block"
                ));
            }
        }
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
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
