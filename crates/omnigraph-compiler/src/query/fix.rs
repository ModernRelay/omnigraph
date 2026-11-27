//! Apply the fixes the compiler prints (the GQ 3 RFC, section 6, "The
//! respelling map" and "Canonical forms"): `omnigraph lint --fix`, and every
//! test or instrument that respells a source, share this one loop.
//!
//! The loop has no rules of its own. Each pass compiles the source
//! ([`check_source_all`]): while it is refused, the pass applies the edits
//! of every refusal's printed suggestion; once it compiles, the pass applies
//! the edit of every canonical form lint names whose fix keeps the typed IR
//! (`L203`, `L206` to `L216`). The two rows-equal rewrites, `L204` and
//! `L205`, are the author's choice and never applied. A pass applies a
//! maximal set of suggestions whose edits do not overlap, whole suggestions
//! only, first in source order winning; a suggestion left out reappears in
//! the next pass, which compiles the text the earlier one wrote.
//!
//! A refusal with no suggestion, or whose suggestion has no edits, stops the
//! loop: nothing is written and the report lists those refusals. A
//! suggestion printed for review ([`Applicability::NeedsReview`]) stops it
//! too unless [`FixOptions::review`] is set, since several of them change
//! what a query means (a dropped `offset`, a guessed parameter type, a
//! renamed declaration); with it set they are applied and the report counts
//! them apart. A pass that writes a text an earlier pass already wrote, or a
//! loop still refused or still naming a canonical form after
//! [`MAX_FIX_PASSES`] passes, does not converge, and nothing is written.

use std::collections::{BTreeMap, HashSet};

use serde::Serialize;

use crate::catalog::Catalog;
use crate::query::canonical::source_canonical_forms;
use crate::query::check::check_source_all;
use crate::query::diagnostic::{Applicability, QueryDiagnostic, TextEdit};

/// The passes after which a source still changing does not converge.
pub const MAX_FIX_PASSES: usize = 16;

/// Which printed fixes the loop applies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FixOptions {
    /// Also apply the suggestions printed for review; without it, a refusal
    /// whose suggestion needs review stops the loop as one with no fix.
    pub review: bool,
}

/// How the loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixStatus {
    /// The source compiles and names no canonical form: nothing to apply.
    Unchanged,
    /// The fixes applied, and the fixed source compiles with no canonical
    /// form left.
    Fixed,
    /// A refusal printed no applicable fix; nothing is written.
    Unfixed,
    /// The fixes loop or do not settle within [`MAX_FIX_PASSES`] passes;
    /// nothing is written.
    DoesNotConverge,
}

/// What the loop did to one source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FixReport {
    pub status: FixStatus,
    /// The passes that applied edits.
    pub passes: usize,
    /// Each code whose fix was applied, with how many times, over every
    /// pass; a refusal's code or a canonical form's (`L…`).
    pub applied: BTreeMap<String, usize>,
    /// The part of `applied` printed for review: read these changes before
    /// relying on the fixed source.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub applied_for_review: BTreeMap<String, usize>,
    /// The refusals standing when the loop stopped: for `unfixed`, those
    /// with no applicable fix; for `does_not_converge`, every refusal of the
    /// last pass. Their positions count into the text that pass compiled,
    /// the original source when `stopped_at_pass` is 0.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unfixed: Vec<QueryDiagnostic>,
    /// The pass that stopped the loop, counting from 0, for a status that
    /// writes nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped_at_pass: Option<usize>,
}

impl FixReport {
    /// Whether the loop wrote nothing because a refusal could not be fixed
    /// or the fixes did not settle.
    pub fn failed(&self) -> bool {
        matches!(self.status, FixStatus::Unfixed | FixStatus::DoesNotConverge)
    }
}

/// The fixed source, present only when [`FixStatus::Fixed`], and the
/// report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixOutcome {
    pub fixed: Option<String>,
    pub report: FixReport,
}

/// One printed fix: the code that printed it, whether it is for review, and
/// its edits of the text the pass compiled.
struct Candidate {
    code: String,
    review: bool,
    edits: Vec<TextEdit>,
}

/// Apply the fixes the compiler prints for `source` against `catalog`, pass
/// by pass, until it compiles and names no canonical form (see the module
/// documentation).
pub fn fix_source(catalog: &Catalog, source: &str, options: FixOptions) -> FixOutcome {
    fix_loop(
        source,
        options,
        |text| check_source_all(catalog, text),
        canonical_candidates,
    )
}

/// The loop of [`fix_source`] over a compile (`refusals`) and the canonical
/// forms of a compiling text (`canonical`).
fn fix_loop(
    source: &str,
    options: FixOptions,
    refusals_of: impl Fn(&str) -> Vec<QueryDiagnostic>,
    canonical: impl Fn(&str) -> Vec<Candidate>,
) -> FixOutcome {
    let mut text = source.to_string();
    let mut seen: HashSet<String> = HashSet::from([text.clone()]);
    let mut report = FixReport {
        status: FixStatus::Unchanged,
        passes: 0,
        applied: BTreeMap::new(),
        applied_for_review: BTreeMap::new(),
        unfixed: Vec::new(),
        stopped_at_pass: None,
    };
    let stop = |mut report: FixReport, status, unfixed, pass| {
        report.status = status;
        report.unfixed = unfixed;
        report.stopped_at_pass = Some(pass);
        FixOutcome {
            fixed: None,
            report,
        }
    };
    for pass in 0..=MAX_FIX_PASSES {
        let refusals = refusals_of(&text);
        let candidates = if refusals.is_empty() {
            canonical(&text)
        } else {
            let unfixed: Vec<QueryDiagnostic> = refusals
                .iter()
                .filter(|d| !applicable(d, options))
                .cloned()
                .collect();
            if !unfixed.is_empty() {
                return stop(report, FixStatus::Unfixed, unfixed, pass);
            }
            refusals
                .iter()
                .filter_map(|d| {
                    let s = d.suggestion.as_deref()?;
                    Some(Candidate {
                        code: d.code.as_str().to_string(),
                        review: s.applicability == Applicability::NeedsReview,
                        edits: s.edits.clone(),
                    })
                })
                .collect()
        };
        if candidates.is_empty() {
            report.status = if report.passes == 0 {
                FixStatus::Unchanged
            } else {
                FixStatus::Fixed
            };
            let fixed = (report.status == FixStatus::Fixed).then_some(text);
            return FixOutcome { fixed, report };
        }
        if pass == MAX_FIX_PASSES {
            return stop(report, FixStatus::DoesNotConverge, refusals, pass);
        }
        let (next, applied) = apply_candidates(&text, candidates);
        if applied.is_empty() || !seen.insert(next.clone()) {
            return stop(report, FixStatus::DoesNotConverge, refusals, pass);
        }
        for (code, review) in applied {
            *report.applied.entry(code.clone()).or_default() += 1;
            if review {
                *report.applied_for_review.entry(code).or_default() += 1;
            }
        }
        report.passes += 1;
        text = next;
    }
    unreachable!("the last pass returns")
}

/// Whether the loop may apply `diagnostic`'s printed fix under `options`.
fn applicable(diagnostic: &QueryDiagnostic, options: FixOptions) -> bool {
    diagnostic.suggestion.as_deref().is_some_and(|s| {
        !s.edits.is_empty() && (options.review || s.applicability != Applicability::NeedsReview)
    })
}

/// Every canonical form of a compiling source whose fix keeps the typed IR,
/// as a candidate.
fn canonical_candidates(text: &str) -> Vec<Candidate> {
    source_canonical_forms(text)
        .into_iter()
        .filter(|(_, form)| form.same_ir)
        .filter_map(|(_, form)| {
            Some(Candidate {
                code: form.code.to_string(),
                review: false,
                edits: vec![form.edit?],
            })
        })
        .collect()
}

/// Two edits of one text that cannot both apply: their ranges overlap, or
/// both insert at one point (their order would be a guess). Identical edits
/// are one edit.
fn conflict(a: &TextEdit, b: &TextEdit) -> bool {
    a != b
        && ((a.start < b.end && b.start < a.end)
            || (a.start == a.end && b.start == b.end && a.start == b.start))
}

/// Apply a maximal set of whole candidates whose edits do not conflict,
/// first in source order winning, and return the new text and the applied
/// candidates' codes. A candidate with an edit outside `text` is skipped.
fn apply_candidates(text: &str, mut candidates: Vec<Candidate>) -> (String, Vec<(String, bool)>) {
    candidates.sort_by_key(|c| {
        c.edits
            .iter()
            .map(|e| (e.start, e.end))
            .min()
            .unwrap_or((usize::MAX, 0))
    });
    let mut kept: Vec<TextEdit> = Vec::new();
    let mut applied = Vec::new();
    for candidate in candidates {
        let valid = candidate.edits.iter().all(|e| {
            e.start <= e.end
                && e.end <= text.len()
                && text.is_char_boundary(e.start)
                && text.is_char_boundary(e.end)
        });
        let fresh: Vec<&TextEdit> = candidate
            .edits
            .iter()
            .filter(|e| !kept.contains(e))
            .collect();
        let clashes = fresh.iter().enumerate().any(|(i, e)| {
            kept.iter().any(|k| conflict(e, k)) || fresh[..i].iter().any(|f| conflict(e, f))
        });
        if !valid || clashes {
            continue;
        }
        for e in fresh {
            if !kept.contains(e) {
                kept.push(e.clone());
            }
        }
        applied.push((candidate.code, candidate.review));
    }
    kept.sort_by_key(|e| (e.start, e.end));
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    for e in &kept {
        out.push_str(&text[cursor..e.start]);
        out.push_str(&e.replacement);
        cursor = e.end;
    }
    out.push_str(&text[cursor..]);
    (out, applied)
}

#[cfg(test)]
#[path = "fix_tests.rs"]
mod tests;
