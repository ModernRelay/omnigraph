//! The suite's verdict/detector taxonomy AS CODE, mechanically enforced.
//!
//! A **verdict** is the outcome of one judgment: observation + independent
//! expectation + mechanical comparison. The observation arrives through an
//! [`ObservationSource`] (its doc has the referent naming that separates
//! channels from Time/HarnessOutput); the expectation + comparison are the
//! [`Oracle`], classed differential / prediction / obligation / meta
//! (documentation taxonomy only — ruling on [`Oracle::class_name`]). One
//! (observation source, oracle) pairing is a [`Detector`] — the unit that
//! fires; one oracle riding two sources is two detectors, and the two can
//! disagree (reconcile's query render vs its export tie-break).
//!
//! Enforcement (the one-vocabulary rule upgraded from discipline to checked
//! invariant, the engine's failpoint-names-guard pattern):
//! 1. census golden file generated FROM these enums and diffed against the
//!    committed copy (`detector_census.txt`, crate root),
//! 2. recorded violations carry their detector by construction
//!    ([`Violation`]), rendered into fleet failure rows as `detector=`,
//! 3. a banned-word lint over this crate's sources for the renamed-away
//!    vocabulary (the lint's own rules are the list),
//! 4. each variant's doc comment IS the census definition (the golden file
//!    embeds them, so definition drift trips guard 1),
//! 5. every guard proven red-capable once, deliberately (the red-proof rule
//!    applies to lints too).

use std::fmt;

/// One of the five observation channels — the store surfaces a universe can
/// be observed through. Channels observe THE STORE and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// The write path's asserted effects — what each op's return value
    /// claims happened (defect polarities: claimed-but-invisible /
    /// visible-but-unclaimed).
    Claim,
    /// The engine's live read surface — scans and real traversals through
    /// the full query path.
    Query,
    /// The stored rows themselves via the export feature, no query
    /// machinery; contract: physical = logical ∪ ghosts.
    Physical,
    /// Time-travel reads of the store's version history (snapshot reads,
    /// commit lists, diffs). Distinct from Jepsen's "history" = the recorded
    /// op set (the one standing vocabulary collision, unruled).
    History,
    /// Per-session handle views — what differently-aged open handles each
    /// see of one store.
    Session,
}

/// Where an observation comes from, named by REFERENT: the five channels
/// observe THE STORE; Time and HarnessOutput observe different objects,
/// hence are not channels (cross-channel disagreement semantics do not
/// exist for them). Named referent gap, deliberately deferred: ENGINE
/// PROCESS STATE — session-channel staleness detectors
/// cover it indirectly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationSource {
    /// The store, through one of the five channels.
    Store(Channel),
    /// Real time — feeds only the liveness bound.
    Time,
    /// The harness's own emissions diffed run-vs-run — feeds only strict
    /// replay.
    HarnessOutput,
}

/// Defines `Oracle` once: variant, class section, canonical observation
/// source(s), harness anchor, and the one-clause census definition — the
/// doc comment and the census text are THE SAME STRING, so rustdoc, the
/// golden file, and the glossary sentence cannot drift apart (guard 4
/// rides guard 1).
macro_rules! oracles {
    ( $( $name:ident, $class:literal, $sources:expr, $anchor:literal, $doc:literal; )+ ) => {
        /// The expectation + comparison half of a verdict, at named-mechanism
        /// grain. Oracles born after the agreed v11 census extend this enum
        /// via a deliberate census bump, never silently; `census_counts_hold`
        /// owns the current numbers.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Oracle {
            $( #[doc = $doc] $name, )+
        }

        impl Oracle {
            /// Every variant, census order (class sections in sequence).
            pub const ALL: &'static [Oracle] = &[ $( Oracle::$name, )+ ];

            /// The documentation-taxonomy class (differential / prediction /
            /// obligation / meta) — a §-comment concept, deliberately not a
            /// type for now (minimalism ruling, 2026-08-13).
            pub fn class_name(self) -> &'static str {
                match self { $( Oracle::$name => $class, )+ }
            }

            /// The canonical observation source(s) this oracle rides — each
            /// pairing is one detector.
            pub fn sources(self) -> &'static [ObservationSource] {
                match self { $( Oracle::$name => $sources, )+ }
            }

            /// Where the oracle lives in the harness (file::function or
            /// test-name grain).
            pub fn anchor(self) -> &'static str {
                match self { $( Oracle::$name => $anchor, )+ }
            }

            /// The one-clause census definition — byte-identical to the
            /// variant's rustdoc.
            pub fn doc(self) -> &'static str {
                match self { $( Oracle::$name => $doc, )+ }
            }
        }
    };
}

use Channel::*;
use ObservationSource::{HarnessOutput, Store, Time};

oracles! {
    // ---- differential (expectation = model state) -----------------------
    WorldDifferential, "differential",
        &[Store(Query)],
        "harness.rs::assert_world_matches (continuous, every 3rd op + final audit)",
        "the whole observable world — branch list plus every branch's persons and edges, read via real traversal — equals the model state";
    MembershipQuery, "differential",
        &[Store(Query)],
        "harness.rs::run_universe mid-run get_person check",
        "one live point query (get_person) through the full read path agrees with the model on a row's presence";
    ReadOnlyAudit, "differential",
        &[Store(Query)],
        "harness.rs::assert_matches_model at the final audit's read-only reopen",
        "the read-only open path renders main equal to the model (main only; branch RO reads are a candidate widening)";
    PhysicalExport, "differential",
        &[Store(Physical)],
        "harness.rs::assert_physical_matches (final audit, every branch)",
        "the stored-row dump via export_jsonl equals the model's physical expectation — persons exactly, Knows = logical ∪ ghosts";
    HistoryDifferential, "differential",
        &[Store(History)],
        "harness.rs::assert_history_matches (final audit)",
        "every recorded commit re-read via ReadTarget::Snapshot equals the model's memory of that moment, plus a conservative Person diff over adjacent commits";
    TraversalModeDifferential, "differential",
        &[Store(Query)],
        "harness.rs::assert_traversal_modes_agree (every check + final audit)",
        "forced-indexed, forced-CSR, and the model agree pairwise, and the bound-edge arm equals logical ∪ ghosts exactly (#474's delta as a checked contract)";
    SessionDifferential, "differential",
        &[Store(Session)],
        "harness.rs::check_sessions (three sessions per universe)",
        "actor, fresh, and bystander sessions stay coherent — actor strict-equal, bystander view ∈ history ∪ current and monotone — with the model as arbiter";

    // ---- prediction (expectation = the model's per-op prediction) -------
    OpArbitration, "prediction",
        &[Store(Query), Store(Physical)],
        "harness.rs::reconcile_after_failure (hypothesis arbitration + ghost tie-break)",
        "after a failed op the world renders as exactly one model hypothesis — Applied / ForkOnly / NotApplied — with the export tie-break resolving ghost-only effects";
    MergePrediction, "prediction",
        &[Store(Claim)],
        "harness.rs::predict_merge + the accept/conflict asserts around branch_merge",
        "the engine's merge accept/conflict decision matches the model's three-way prediction, asserted in both directions";
    LegalRejection, "prediction",
        &[Store(Claim), Store(Query)],
        "harness.rs::is_legal_rejection",
        "every op failure matches the written-down definite-error catalog — an unlisted rejection is a red, not a shrug";
    AckLossArbitration, "prediction",
        &[Store(Claim), Store(Query)],
        "harness.rs::reconcile_after_failure via FailingStorage::lose_ack routing",
        "an ack-lost write's outcome is arbitrated Applied-or-NotApplied against the durable world — never double-applied, never silently retried";

    // ---- obligation (expectation = a standing contract) -----------------
    CrashContract, "obligation",
        &[Store(Query)],
        "harness.rs::reconcile_after_failure (legal-state + monotonicity asserts)",
        "the two-sided crash contract: atomicity (no partial application) and recovery monotonicity (no demoted commit, no deleted durable fork)";
    BirthContract, "obligation",
        &[Store(Claim)],
        "harness.rs::run_birth_universe / run_open_crash_universe",
        "a store that dies during init is honestly-unopenable or indistinguishable from never-inited, and a crashed open is effect-free";
    RecoveryObligation, "obligation",
        &[Store(Physical)],
        "harness.rs::reconcile_after_failure residue check (recovery_residue)",
        "a successful read-write reopen leaves __recovery/ empty — recovery may not silently do nothing (added after the recovery-no-op honesty audit)";
    ResidueObligation, "obligation",
        &[Store(Physical)],
        "harness.rs final audit residue check + tests::dst_residue_channel_sees_planted_file",
        "no universe ends owing recovery work: __recovery/ empty at quiesce, backed by the planted-file channel canary";
    LiveWriteAvailability, "obligation",
        &[Store(Session)],
        "harness.rs::run_universe_caught keep-serving watch (Scenario::keep_serving_ops)",
        "a live handle must not wedge permanently on one pending effect-free Armed recovery operation: with reconcile's reopen deferred, consecutive same-operation RecoveryRequired refusals stay under the keep-serving budget (issue #554)";
    MaintenanceObligations, "obligation",
        &[Store(Query)],
        "harness.rs::maintenance_obligations",
        "after every maintenance death the rerun converges (idempotence), post-Cleanup state stays readable, and indexes agree immediately";
    DetectedOrHarmless, "obligation",
        &[Store(Claim)],
        "harness.rs damage-attribution window (corruption_detections)",
        "an injected storage lie is either surfaced as an error attributed by damage-ledger overlap or provably harmless — never a silent wrong answer";
    CommitIdUniqueness, "obligation",
        &[Store(History)],
        "harness.rs final audit list_commits walk",
        "no two commits in one branch's history share a graph_commit_id (the OCC invariant; cross-branch reuse is legal shared-fork lineage)";
    LivenessBound, "obligation",
        &[Time],
        "harness.rs final audit ensure_indices timeout (real clock)",
        "convergence completes within the real-clock bound — the deadlock detector every state oracle is blind to";

    // ---- meta (expectation = the harness's own guarantees) --------------
    StrictReplay, "meta",
        &[HarnessOutput],
        "tests/scenarios.rs replay pins (run twice, compare UniverseReport)",
        "the same seed reproduces an equal UniverseReport, row order included (adapter-realm and crash universes; the lance-realm carve-out is documented on the report)";
    SensitivityProof, "meta",
        &[HarnessOutput],
        "tests/scenarios.rs dst_*_sensitivity_* + the seeded honesty procedures",
        "a deliberately planted defect turns the guarded oracle red — a green is trusted only after a manufactured red";
}

/// One (observation source, oracle) pairing — the unit that actually fires
/// in a hunt (lineage: Chandra–Toueg failure detectors). Channels and
/// oracles compose many-to-many; an oracle on two sources is two detectors
/// that can disagree. Takes an [`ObservationSource`], not a [`Channel`]:
/// the two meta-oracles observe non-store referents, and the type forces
/// that distinction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Detector {
    pub source: ObservationSource,
    pub oracle: Oracle,
}

/// The outcome of one judgment. Binary by ruling (2026-08-13): "absorbed"
/// is a workload outcome, a reconcile tie resolves to Green-with-breadth,
/// a legal rejection is Green by the definite-error catalog — none are
/// verdict variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Green,
    /// Comparison failed; both sides rendered report-ready.
    Red {
        observed: String,
        expected: String,
    },
}

/// A recorded violation = one red verdict + its provenance: the row in a
/// fleet failure record, carrying the detector tag by construction.
/// `at_op` is the transcript position (replay anchor); final-audit reds
/// carry the op count (the audit sits after the last op).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub detector: Detector,
    pub verdict: Verdict, // always Red in a recorded row
    pub at_op: usize,     // transcript position, replay anchor
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for ObservationSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Store(c) => write!(f, "Store({c})"),
            Time => write!(f, "Time"),
            HarnessOutput => write!(f, "HarnessOutput"),
        }
    }
}

impl fmt::Display for Detector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{:?}", self.source, self.oracle)
    }
}

impl Violation {
    /// The one-line report row: `VIOLATION detector=<source>/<oracle>
    /// at_op=<n>: observed=... expected=...` — the string fleet failure
    /// records carry (and `panic_message` renders).
    pub fn render(&self) -> String {
        match &self.verdict {
            Verdict::Green => format!(
                "VIOLATION detector={} at_op={}: (green? malformed row)",
                self.detector, self.at_op
            ),
            Verdict::Red { observed, expected } => format!(
                "VIOLATION detector={} at_op={}: observed={observed} expected={expected}",
                self.detector, self.at_op
            ),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

/// Record a red verdict: panic with a [`Violation`] payload so the fleet's
/// catch path records a detector-tagged row instead of a bare string
/// (guard 2 — a recorded violation without a detector does not exist).
pub fn violation(
    detector: Detector,
    at_op: usize,
    observed: impl Into<String>,
    expected: impl Into<String>,
) -> ! {
    std::panic::panic_any(Violation {
        detector,
        verdict: Verdict::Red {
            observed: observed.into(),
            expected: expected.into(),
        },
        at_op,
    })
}

/// Run one oracle funnel under a detector tag: a panic escaping the future
/// is wrapped into a [`Violation`] for this detector (already-tagged
/// violations pass through untouched, so inner tags win — e.g. the
/// traversal-mode funnel keeps its own tag when maintenance obligations
/// reuse it). `observed` carries the original panic message verbatim —
/// message-matching tests and repro greps keep working; `expected` carries
/// the oracle's census contract.
pub async fn tagged<T, F>(detector: Detector, at_op: usize, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    use futures::FutureExt;
    match std::panic::AssertUnwindSafe(fut).catch_unwind().await {
        Ok(v) => v,
        Err(payload) => {
            if payload.downcast_ref::<Violation>().is_some() {
                std::panic::resume_unwind(payload);
            }
            let observed = crate::harness::panic_message(payload.as_ref());
            violation(detector, at_op, observed, detector.oracle.doc());
        }
    }
}

// ---------------------------------------------------------------- census --

/// Render the census golden file FROM the enums (guard 1). The committed
/// copy (`detector_census.txt`, crate root) is diffed against this by
/// `census_golden_matches_code`; docs CITE the file instead of
/// duplicating the table.
pub fn render_census() -> String {
    let classes: &[(&str, &str)] = &[
        ("differential", "expectation = model state"),
        ("prediction", "expectation = the model's per-op prediction"),
        ("obligation", "expectation = a standing contract"),
        ("meta", "expectation = the harness's own guarantees"),
    ];
    let mut out = String::new();
    out.push_str(
        "# Detector census — the verdict/detector taxonomy, GENERATED from\n\
         # crates/omnigraph-dst/src/detectors.rs (guard 1).\n\
         # Do not edit by hand: regenerate via the DST_REGEN_CENSUS flow in\n\
         # detectors.rs tests; `census_golden_matches_code` diffs this file\n\
         # against the code on every suite run.\n\
         #\n\
         # verdict = observation + independent expectation + mechanical comparison\n\
         # detector = one (observation source, oracle) pair — the unit that fires;\n\
         # an oracle listed with two sources is two detectors.\n\
         #\n",
    );
    let mut counts = Vec::new();
    for (class, _) in classes {
        let n = Oracle::ALL
            .iter()
            .filter(|o| o.class_name() == *class)
            .count();
        counts.push(format!("{class} {n}"));
    }
    out.push_str(&format!(
        "# {} oracles: {} (named-mechanism grain;\n\
         # new oracles enter by deliberate census bump).\n",
        Oracle::ALL.len(),
        counts.join(" / ")
    ));
    for (class, subtitle) in classes {
        out.push_str(&format!("\n[{class}]  {subtitle}\n"));
        for o in Oracle::ALL.iter().filter(|o| o.class_name() == *class) {
            let sources = o
                .sources()
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!(
                "{:?}\n  sources: {sources}\n  anchor:  {}\n  doc:     {}\n",
                o,
                o.anchor(),
                o.doc()
            ));
        }
    }
    out
}

/// Every `.rs` source in this crate (src/ + tests/, RECURSIVE so
/// src/bin/ is seen), sorted. The one walker every source-scanning lint
/// uses, so all lints see one file set. Symlinks are NOT followed
/// (`file_type()`, not `is_dir()`), so a linked directory can neither
/// loop the walk nor pull foreign files into the lints; dev/CI test-only.
pub fn crate_rs_sources() -> Vec<std::path::PathBuf> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    let mut stack = vec![root.join("src"), root.join("tests")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read crate source dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if entry.file_type().expect("entry file type").is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn crate_golden_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("detector_census.txt")
    }

    /// GUARD 1 (+ guard 4, since the golden embeds the doc sentences):
    /// regenerated census == committed census.
    /// Regenerate: run with DST_REGEN_CENSUS=1.
    #[test]
    fn census_golden_matches_code() {
        let rendered = render_census();
        if std::env::var("DST_REGEN_CENSUS").is_ok() {
            std::fs::write(crate_golden_path(), &rendered).expect("write in-crate census");
            return;
        }
        let committed = std::fs::read_to_string(crate_golden_path())
            .expect("in-crate detector_census.txt missing — regenerate with DST_REGEN_CENSUS=1");
        assert_eq!(
            committed, rendered,
            "census drift: detector_census.txt no longer matches the enums — \
             regenerate with DST_REGEN_CENSUS=1 and review the diff"
        );
    }

    /// The documented shape: 22 oracles, differential 7 / prediction 4 /
    /// obligation 9 / meta 2 (the documented counts, self-checked).
    #[test]
    fn census_counts_hold() {
        let count = |class: &str| {
            Oracle::ALL
                .iter()
                .filter(|o| o.class_name() == class)
                .count()
        };
        assert_eq!(
            Oracle::ALL.len(),
            22,
            "oracle count drifted from the documented 22"
        );
        assert_eq!(count("differential"), 7);
        assert_eq!(count("prediction"), 4);
        assert_eq!(count("obligation"), 9);
        assert_eq!(count("meta"), 2);
        // Detector count: one per (source, oracle) pairing.
        let detectors: usize = Oracle::ALL.iter().map(|o| o.sources().len()).sum();
        assert_eq!(
            detectors, 25,
            "detector count drifted (three oracles ride two sources each)"
        );
    }

    /// GUARD 2's render contract: the row a fleet failure record carries.
    #[test]
    fn violation_renders_detector_field() {
        let v = Violation {
            detector: Detector {
                source: Store(Claim),
                oracle: Oracle::MergePrediction,
            },
            verdict: Verdict::Red {
                observed: "engine ACCEPTED".into(),
                expected: "model predicts conflict".into(),
            },
            at_op: 7,
        };
        assert_eq!(
            v.render(),
            "VIOLATION detector=Store(Claim)/MergePrediction at_op=7: \
             observed=engine ACCEPTED expected=model predicts conflict"
        );
    }

    /// GUARD 3 — the banned-word lint: this crate's sources may not use the
    /// renamed-away suite vocabulary (extending a rename means extending this lint
    /// in the same change). Mechanism names keep their words per the
    /// boundary (`KillState`, `die_at_write`, the engine's sidecar state,
    /// instrument proper names carrying legacy words as identifiers), and
    /// "recovery" + the s-word is the engine's own mechanism phrase for the
    /// reopen pass — both allowed by construction below.
    #[test]
    fn banned_words_lint() {
        // Built from parts so this file's own source never trips the scan.
        let s_word = ["sw", "eep"].concat(); // prose form banned outside identifiers
        let banned_substrings: Vec<String> = vec![
            ["tox", "ic"].concat(),
            ["kill ", "sw", "eep"].concat(),
            ["kill ", "point"].concat(),
            ["unarm", "able"].concat(),
            ["ack ", "channel"].concat(),
            ["raw ", "channel"].concat(),
        ];
        let banned_words: Vec<String> = vec![["un", "reached"].concat()];
        let window_words = ["window", "windows"];
        let window_state_words = [["arm", "ed"].concat(), ["fir", "ed"].concat()];

        let files = crate::detectors::crate_rs_sources();
        assert!(files.len() >= 10, "lint saw too few files — scan broken?");

        let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
        let word_hit = |line_lc: &str, w: &str| {
            let bytes = line_lc.as_bytes();
            let mut from = 0;
            while let Some(i) = line_lc[from..].find(w) {
                let start = from + i;
                let end = start + w.len();
                let pre = start > 0 && is_word(bytes[start - 1]);
                let post = end < bytes.len() && is_word(bytes[end]);
                if !pre && !post {
                    return Some(start);
                }
                from = end;
            }
            None
        };

        let mut hits: Vec<String> = Vec::new();
        for file in &files {
            let text = std::fs::read_to_string(file).expect("read source file");
            let name = file.file_name().unwrap().to_string_lossy().to_string();
            // NOTE: this file scans ITSELF too — its banned literals are
            // built from concatenated parts precisely so the scan stays
            // clean while the rule stays total.
            for (n, line) in text.lines().enumerate() {
                let lc = line.to_lowercase();
                for b in &banned_substrings {
                    if lc.contains(b.as_str()) {
                        hits.push(format!("{name}:{}: banned '{b}': {line}", n + 1));
                    }
                }
                for w in &banned_words {
                    if word_hit(&lc, w).is_some() {
                        hits.push(format!("{name}:{}: banned word '{w}': {line}", n + 1));
                    }
                }
                // Prose s-word: allowed inside identifiers (adjacent word
                // chars) and in the engine's "recovery <s-word>" phrase.
                if let Some(start) = word_hit(&lc, &s_word) {
                    let allowed = lc[..start].trim_end().ends_with("recovery");
                    if !allowed {
                        hits.push(format!("{name}:{}: prose '{s_word}': {line}", n + 1));
                    }
                }
                // Crash-window state words in their window sense: the old
                // state vocabulary collocated with "window(s)".
                let has_window = window_words.iter().any(|w| word_hit(&lc, w).is_some());
                if has_window {
                    for w in &window_state_words {
                        if word_hit(&lc, w).is_some() {
                            hits.push(format!(
                                "{name}:{}: window-state word '{w}' (use scheduled/hit/never \
                                 reached/unschedulable): {line}",
                                n + 1
                            ));
                        }
                    }
                }
            }
        }
        assert!(
            hits.is_empty(),
            "banned-vocabulary hits (renamed-away suite vocabulary):\n{}",
            hits.join("\n")
        );
    }
}
