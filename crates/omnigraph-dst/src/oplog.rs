//! The lane B op-log grammar: one module owning both EMIT and PARSE.
//!
//! The grammar spans three writers (the dst_child binary's workload
//! lines, `KillState::barrier_and_park`'s barrier line, the recover-mode
//! summary) and several parsers (the replay judge, the parent
//! instruments' diagnostics). Every prefix and line constructor lives
//! here so the writers and their parsers cannot drift apart silently.
//!
//! Durability rationale (cited by every writer): each line is emitted as
//! ONE `write(2)` of a complete newline-terminated line, so a polling
//! reader can never observe a torn prefix of a finished line; a completed
//! `write(2)` survives SIGKILL (the page cache outlives the process); the
//! `sync_data` guards machine crash, out of scope for lane B.
//!
//! Line kinds:
//!   `invoke {i} {target} {kind} {args...}`   op announced (log-ahead)
//!   `ok {i}` / `err {i} {message}`           op outcome
//!   `armed c={c} weather={bool}`             rig armed (forensics only)
//!   `fixtures-loaded`                        workload about to start
//!   `barrier {c} {op} {uri} in_flight={n}`   completion-cut reached
//!   `N {n}`                                  probe total (durable completions)
//!   `recover-done N {n}`                     recover-mode total
//!   `done`                                   workload completed

use std::io::Write as _;

pub const INVOKE: &str = "invoke ";
pub const OK: &str = "ok ";
pub const ERR: &str = "err ";
pub const BARRIER: &str = "barrier ";
pub const ARMED: &str = "armed ";
pub const FIXTURES_LOADED: &str = "fixtures-loaded";
pub const N_LINE: &str = "N ";
pub const RECOVER_DONE_N: &str = "recover-done N ";
pub const DONE: &str = "done";

/// Workload person-name prefix for a seed (names are minted, never reused).
pub fn lb_prefix(seed: u64) -> String {
    format!("lb-{seed}-")
}

pub fn lb_name(seed: u64, minted: usize) -> String {
    format!("lb-{seed}-{minted}")
}

/// Branch-name prefix; the judge filters engine branches on the constant.
pub const LB_BRANCH_PREFIX: &str = "lb-br-";

pub fn lb_branch(seed: u64, minted: usize) -> String {
    format!("{LB_BRANCH_PREFIX}{seed}-{minted}")
}

// ---------------------------------------------- line constructors --

/// `args` is the kind's argument list pre-joined with single spaces
/// (`"name age"`, `"from to"`, `"branch"`). The parser checks exact
/// arity per kind, so a constructor caller passing the wrong argument
/// count is caught at the first parse.
pub fn invoke_line(i: usize, target: &str, kind: &str, args: &str) -> String {
    format!("{INVOKE}{i} {target} {kind} {args}")
}

pub fn ok_line(i: usize) -> String {
    format!("{OK}{i}")
}

/// `message` is flattened to one line (the grammar is line-oriented).
pub fn err_line(i: usize, message: &str) -> String {
    format!("{ERR}{i} {}", message.replace('\n', " "))
}

pub fn barrier_line(c: usize, op: &str, uri: &str, in_flight: usize) -> String {
    format!("{BARRIER}{c} {op} {uri} in_flight={in_flight}")
}

pub fn armed_line(die_at: Option<usize>, weather: bool) -> String {
    format!("{ARMED}c={die_at:?} weather={weather}")
}

pub fn n_line(n: usize) -> String {
    format!("{N_LINE}{n}")
}

pub fn recover_done_line(n: usize) -> String {
    format!("{RECOVER_DONE_N}{n}")
}

/// Durably append one grammar line: one `write_all` of the whole
/// newline-terminated line (never per-fragment writes), then
/// `sync_data`. See the module doc for why one syscall matters.
///
/// # Panics
/// When the write or sync fails — the op log IS the evidence; an
/// unwritable log invalidates the run.
pub fn emit(f: &mut std::fs::File, line: &str) {
    f.write_all(format!("{line}\n").as_bytes())
        .expect("oplog write");
    f.sync_data().expect("oplog sync");
}

// -------------------------------------------------------- parse --

/// One parsed op log. `invokes` preserves order; `outcomes` maps op index
/// to true (ok) / false (err); an invoke absent from `outcomes` is the
/// single in-flight indeterminate op.
pub struct OplogSummary {
    pub invokes: Vec<(usize, String)>,
    pub outcomes: std::collections::BTreeMap<usize, bool>,
    pub fixtures_loaded: bool,
    pub completed: bool,
    /// The barrier line's recorded completion ordinal, when one was cut.
    pub barrier_c: Option<usize>,
    /// The barrier line's in-flight gauge reading (must be 1: the parked
    /// call itself).
    pub barrier_in_flight: Option<usize>,
    pub probe_n: Option<usize>,
    pub recover_n: Option<usize>,
}

impl OplogSummary {
    pub fn invoked(&self) -> usize {
        self.invokes.len()
    }
    pub fn acked(&self) -> usize {
        self.outcomes.values().filter(|v| **v).count()
    }
    pub fn errs(&self) -> usize {
        self.outcomes.values().filter(|v| !**v).count()
    }
}

/// Exact arity per op kind (the constructors emit exactly these shapes;
/// trailing garbage on an interior line is corruption).
fn invoke_arity_ok(parts: &[&str]) -> bool {
    if parts.len() < 4 {
        return false;
    }
    match parts[3] {
        "insert" | "set_age" | "edge" => parts.len() == 6,
        "remove" | "branch_create" | "branch_delete" => parts.len() == 5,
        _ => false,
    }
}

/// Parse an op log. TORN-TAIL RULE: a SIGKILL can land mid-`write(2)`, so
/// the final line, when the log does not end in a newline, is dropped
/// unparsed (a torn `invoke` prefix must not be indexed, and a torn
/// `ok 1[2]` must not close the wrong op). Interior lines are fsync-
/// ordered history: ANY malformed, duplicate, or unknown interior line is
/// log corruption and panics — the only tolerated non-grammar shape is
/// the `armed ` forensics line.
///
/// # Panics
/// On any interior-line corruption (see above), on an outcome whose index
/// was never invoked (a log-ahead violation), and on more than one
/// unclosed invoke.
pub fn parse(log: &str, label: &str) -> OplogSummary {
    let mut s = OplogSummary {
        invokes: Vec::new(),
        outcomes: Default::default(),
        fixtures_loaded: false,
        completed: false,
        barrier_c: None,
        barrier_in_flight: None,
        probe_n: None,
        recover_n: None,
    };
    let mut invoke_indices = std::collections::BTreeSet::new();
    let mut lines: Vec<&str> = log.lines().collect();
    if !log.ends_with('\n') {
        // The unterminated tail is the in-flight write at the cut; its op
        // is already represented by its (fsync'd, terminated) invoke line
        // or is itself a torn invoke that never fully happened.
        lines.pop();
    }
    for line in lines {
        if let Some(rest) = line.strip_prefix(INVOKE) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            let idx: usize = rest
                .split_whitespace()
                .next()
                .and_then(|t| t.parse().ok())
                .unwrap_or_else(|| panic!("{label}: malformed interior invoke line: {line:?}"));
            assert!(
                invoke_arity_ok(&parts),
                "{label}: malformed interior invoke line (arity): {line:?}"
            );
            assert!(
                invoke_indices.insert(idx),
                "{label}: duplicate invoke index {idx}: {line:?}"
            );
            s.invokes.push((idx, line.to_string()));
        } else if let Some(rest) = line.strip_prefix(OK) {
            let idx: usize = rest
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("{label}: malformed interior ok line: {line:?}"));
            assert!(
                s.outcomes.insert(idx, true).is_none(),
                "{label}: duplicate outcome for op {idx}: {line:?}"
            );
        } else if let Some(rest) = line.strip_prefix(ERR) {
            let idx: usize = rest
                .split_whitespace()
                .next()
                .and_then(|t| t.parse().ok())
                .unwrap_or_else(|| panic!("{label}: malformed interior err line: {line:?}"));
            assert!(
                s.outcomes.insert(idx, false).is_none(),
                "{label}: duplicate outcome for op {idx}: {line:?}"
            );
        } else if let Some(rest) = line.strip_prefix(BARRIER) {
            // A terminated barrier line was written whole in one syscall
            // (torn ones fall to the tail rule), so unparseable fields or
            // a second barrier line are corruption.
            assert!(
                s.barrier_c.is_none(),
                "{label}: second barrier line: {line:?}"
            );
            s.barrier_c = Some(
                rest.split_whitespace()
                    .next()
                    .and_then(|t| t.parse().ok())
                    .unwrap_or_else(|| panic!("{label}: malformed barrier ordinal: {line:?}")),
            );
            s.barrier_in_flight = Some(
                rest.split("in_flight=")
                    .nth(1)
                    .and_then(|t| t.trim().parse().ok())
                    .unwrap_or_else(|| panic!("{label}: malformed barrier in_flight: {line:?}")),
            );
        } else if let Some(rest) = line.strip_prefix(RECOVER_DONE_N) {
            s.recover_n = Some(
                rest.trim()
                    .parse()
                    .unwrap_or_else(|_| panic!("{label}: malformed recover-done line: {line:?}")),
            );
        } else if let Some(rest) = line.strip_prefix(N_LINE) {
            s.probe_n = Some(
                rest.trim()
                    .parse()
                    .unwrap_or_else(|_| panic!("{label}: malformed N line: {line:?}")),
            );
        } else if line == FIXTURES_LOADED {
            s.fixtures_loaded = true;
        } else if line == DONE {
            s.completed = true;
        } else if line.strip_prefix(ARMED).is_some() {
            // Forensics only; never judged.
        } else {
            // A line matching no prefix is prefix corruption (`ok 12`
            // rotted to `k 12` would otherwise silently reclassify op 12
            // as the in-flight indeterminate op).
            panic!("{label}: unknown interior line (prefix corruption?): {line:?}");
        }
    }
    for idx in s.outcomes.keys() {
        assert!(
            invoke_indices.contains(idx),
            "{label}: outcome for op {idx} that was never invoked (log-ahead violation)"
        );
    }
    let unclosed = s
        .invokes
        .iter()
        .filter(|(i, _)| !s.outcomes.contains_key(i))
        .count();
    assert!(
        unclosed <= 1,
        "{label}: log-ahead discipline broken: {unclosed} unclosed invokes"
    );
    s
}
