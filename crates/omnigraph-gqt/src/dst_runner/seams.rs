//! Seam admission, arming and delivery: what a case's `seam:` directive
//! selects in the engine catalog or the store-place table, how it is armed
//! around one step, and what proves it was delivered (RFC 0066).

use omnigraph_dst::store_places;

use super::location;
#[cfg(tokio_unstable)]
use super::{observe, record};
use crate::runner_config::{SeamAction, SeamDirective};

#[cfg(tokio_unstable)]
tokio::task_local! {
    pub(super) static DECORATION: std::sync::Arc<omnigraph_dst::harness::FailingStorage>;
}

/// The guard a decision seam's installer returns.
#[cfg(tokio_unstable)]
pub(super) type DecideGuard = omnigraph::seams::Installed<
    dyn omnigraph::seams::Decide,
    omnigraph::seams::Global<dyn omnigraph::seams::Decide>,
>;

/// The effect a case's action fires on a seam declaring `effects`, or `None`
/// when the seam admits no such action: `fail` takes `Fail`, else
/// `Contention`; `contention` takes only `Contention`; `skip` takes `Skip`.
/// `hold` and the store actions are never engine effects. One rule for
/// arming and for judging a report's delivery evidence.
pub(crate) fn admitted_effect(
    action: SeamAction,
    effects: &[omnigraph::seams::Effect],
) -> Option<omnigraph::seams::Effect> {
    use omnigraph::seams::Effect;
    let candidates: &[Effect] = match action {
        SeamAction::Fail => &[Effect::Fail, Effect::Contention],
        SeamAction::Contention => &[Effect::Contention],
        SeamAction::Skip => &[Effect::Skip],
        SeamAction::Hold | SeamAction::Store(_) => &[],
    };
    candidates
        .iter()
        .copied()
        .find(|candidate| effects.contains(candidate))
}

/// What a directive's `at` and `action` select, once admitted.
pub(crate) enum Admitted {
    /// An engine effect on a decision seam: the site produces the outcome.
    Code(
        &'static omnigraph::seams::DecideSeam,
        omnigraph::seams::Effect,
    ),
    /// A store effect on a decision seam (see `omnigraph_seams::StoreEffect`).
    CodeStore(
        &'static omnigraph::seams::DecideSeam,
        omnigraph::seams::StoreEffect,
    ),
    /// A store action on a store place, selected by `subject`.
    Store(
        &'static omnigraph_dst::store_places::StorePlaceEntry,
        omnigraph_dst::store_places::StoreAction,
        omnigraph_dst::store_places::Subject,
    ),
}

/// Resolve `at` by exact name, the engine catalog first and `STORE_PLACES`
/// second, and check the action against what the entry or row declares.
/// The step check is `admit_seam`'s; the known-failure classifier resolves
/// without a step.
pub(crate) fn resolve_seam(seam: &SeamDirective) -> Result<Admitted, String> {
    let code = omnigraph::seams::catalog::decide(&seam.at);
    let row = store_places::store_place(&seam.at);
    match (code, row) {
        (Some(entry), Some(row)) => Err(format!(
            "unsupported_environment: seam {} is both an engine seam (declared at {}) and a store place (row {}); the two registries must be disjoint",
            seam.at,
            location(entry.site()),
            row.declared_at()
        )),
        (None, None) => Err(format!(
            "unsupported_environment: unknown seam: {}",
            seam.at
        )),
        (None, Some(row)) => {
            let Some(subject) = &seam.subject else {
                return Err(format!(
                    "unsupported_environment: store place {} (row {}) requires subject, a glob over the object's root-relative name",
                    seam.at,
                    row.declared_at()
                ));
            };
            let subject = store_places::Subject::parse(subject)?;
            let Some(action) = seam.action.store_action() else {
                let tail = if row.admitted.is_empty() {
                    "; this row admits no store action yet".to_string()
                } else {
                    format!(
                        "; a store place takes a store action ({})",
                        store_actions_list(row.admitted)
                    )
                };
                return Err(format!(
                    "unsupported_environment: store place {} (row {}) does not admit engine action {}{tail}",
                    seam.at,
                    row.declared_at(),
                    seam.action.as_str()
                ));
            };
            if !row.honors.contains(&action) {
                return Err(format!(
                    "unsupported_environment: the storage decoration does not implement action {} on store place {} (row {}); implemented: {}",
                    action.as_str(),
                    seam.at,
                    row.declared_at(),
                    store_actions_list(row.honors)
                ));
            }
            if !row.admitted.contains(&action) {
                return Err(format!(
                    "unsupported_environment: store place {} (row {}) lists action {} but it is not admitted; admitted: {}",
                    seam.at,
                    row.declared_at(),
                    action.as_str(),
                    store_actions_list(row.admitted)
                ));
            }
            Ok(Admitted::Store(row, action, subject))
        }
        (Some(entry), None) => {
            if let Some(subject) = &seam.subject {
                return Err(match entry.store_subject() {
                    Some(declared) => format!(
                        "unsupported_environment: seam {} (declared at {}) declares its own subject {declared:?}; a case does not restate it",
                        seam.at,
                        location(entry.site())
                    ),
                    None => format!(
                        "unsupported_environment: seam {} (declared at {}) declares no subject; subject {subject:?} is refused",
                        seam.at,
                        location(entry.site())
                    ),
                });
            }
            match seam.action.store_action() {
                Some(action) => match action.store_effect() {
                    Some(effect) if entry.store_effects().contains(&effect) => {
                        Ok(Admitted::CodeStore(entry, effect))
                    }
                    _ => Err(format!(
                        "unsupported_environment: seam {} (declared at {}) declares store effects {} and does not admit action {}",
                        seam.at,
                        location(entry.site()),
                        omnigraph::seams::store_effects_list(entry.store_effects()),
                        seam.action.as_str()
                    )),
                },
                None => {
                    let Some(effect) = admitted_effect(seam.action, entry.effects()) else {
                        return Err(format!(
                            "unsupported_environment: seam {} (declared at {}) declares effects {} and does not admit action {}",
                            seam.at,
                            location(entry.site()),
                            omnigraph::seams::effects_list(entry.effects()),
                            seam.action.as_str()
                        ));
                    };
                    Ok(Admitted::Code(entry, effect))
                }
            }
        }
    }
}

/// The spelling of a store action set in a message: `[misdirect, lose]`.
fn store_actions_list(actions: &[omnigraph_dst::store_places::StoreAction]) -> String {
    let names: Vec<&str> = actions.iter().map(|a| a.as_str()).collect();
    format!("[{}]", names.join(", "))
}

/// Whether a step of this kind crosses a seam of operation `op`.
fn crosses(op: omnigraph::seams::Op, step: Option<&crate::Step>) -> bool {
    use omnigraph::seams::Op;
    matches!(
        (op, step),
        (Op::Mutation | Op::AnyWrite, Some(crate::Step::Mutate(_)))
            | (
                Op::BranchMerge | Op::AnyWrite,
                Some(crate::Step::Control(crate::ControlStep {
                    write: crate::ControlWrite::Merge { .. },
                    ..
                })),
            )
            | (
                Op::BranchCreate | Op::AnyWrite,
                Some(crate::Step::Control(crate::ControlStep {
                    write: crate::ControlWrite::Create { .. },
                    ..
                })),
            )
            | (
                Op::BranchDelete | Op::AnyWrite,
                Some(crate::Step::Control(crate::ControlStep {
                    write: crate::ControlWrite::Delete { .. },
                    ..
                })),
            )
    )
}

/// Admission of one seam directive against the registries and the step it
/// precedes: the name must resolve, the action must be among what the entry
/// or row declares, and the step must be of a kind that crosses the seam's
/// operation (any mutate or branch step for a store place).
pub(crate) fn admit_seam(
    seam: &SeamDirective,
    step: Option<&crate::Step>,
) -> Result<Admitted, String> {
    let admitted = resolve_seam(seam)?;
    match &admitted {
        Admitted::Code(entry, _) | Admitted::CodeStore(entry, _) => {
            if !crosses(entry.op(), step) {
                return Err(format!(
                    "unsupported_environment: seam {} (operation {}) is not crossed by the step it precedes",
                    seam.at,
                    entry.op().as_str()
                ));
            }
        }
        Admitted::Store(..) => {
            if !crosses(omnigraph::seams::Op::AnyWrite, step) {
                return Err(format!(
                    "unsupported_environment: store place {} is admitted before a mutate or branch step only",
                    seam.at
                ));
            }
        }
    }
    Ok(admitted)
}

/// At most one directive per step acts on the store, since admission holds no
/// map from a decision seam to the store call it precedes.
pub(crate) fn refuse_two_store_actors(
    seams: &[SeamDirective],
    admitted: &[Admitted],
) -> Result<(), String> {
    let actors = seams
        .iter()
        .zip(admitted)
        .filter(|(_, admitted)| !matches!(admitted, Admitted::Code(..)))
        .map(|(seam, _)| seam.at.as_str())
        .collect::<Vec<_>>();
    if let [first, second, ..] = actors.as_slice() {
        return Err(format!(
            "unsupported_environment: one store action per step; {first} and {second} both act on the store before this step"
        ));
    }
    Ok(())
}

/// One armed seam: the guard that keeps the decider installed for the step,
/// and what proves the delivery once the step is done.
#[cfg(tokio_unstable)]
pub(crate) struct ArmedSeam {
    at: String,
    occurrence: usize,
    subject: Option<String>,
    guard: Option<DecideGuard>,
    kind: ArmedKind,
}

#[cfg(tokio_unstable)]
enum ArmedKind {
    Code {
        entry: &'static omnigraph::seams::DecideSeam,
        counted: std::sync::Arc<omnigraph::seams::Counted>,
    },
    CodeStore {
        entry: &'static omnigraph::seams::DecideSeam,
        counted: std::sync::Arc<StoreCounted>,
    },
    Store {
        row: &'static omnigraph_dst::store_places::StorePlaceEntry,
        action: omnigraph_dst::store_places::StoreAction,
        decoration: std::sync::Arc<omnigraph_dst::harness::FailingStorage>,
    },
}

/// A rule outlives its step only through `finish_seams`, which drains it; the
/// guard covers every other exit.
#[cfg(tokio_unstable)]
impl Drop for ArmedSeam {
    fn drop(&mut self) {
        if let ArmedKind::Store { decoration, .. } = &self.kind {
            decoration.targets().clear();
        }
    }
}

/// The decider of a store effect: counts crossings, and on the declared one
/// arms the decoration's one-shot on the site's declared subject and answers
/// `Decision::Store`, which the site passes through.
#[cfg(tokio_unstable)]
struct StoreCounted {
    target: u64,
    effect: omnigraph::seams::StoreEffect,
    subject: Option<omnigraph_dst::store_places::Subject>,
    decoration: std::sync::Arc<omnigraph_dst::harness::FailingStorage>,
    crossings: std::sync::atomic::AtomicU64,
    fired: std::sync::atomic::AtomicBool,
}

#[cfg(tokio_unstable)]
impl StoreCounted {
    fn crossings(&self) -> u64 {
        self.crossings.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn fired(&self) -> bool {
        self.fired.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(tokio_unstable)]
impl omnigraph::seams::Behavior for StoreCounted {}

#[cfg(tokio_unstable)]
impl omnigraph::seams::Decide for StoreCounted {
    fn decide(&self, _name: &'static str) -> omnigraph::seams::Decision {
        use std::sync::atomic::Ordering;
        let seen = self.crossings.fetch_add(1, Ordering::SeqCst) + 1;
        if seen == self.target {
            let (place, action) = store_places::effect_target(self.effect);
            self.decoration
                .targets()
                .arm_one_shot(place, action, self.subject.clone());
            self.fired.store(true, Ordering::SeqCst);
            omnigraph::seams::Decision::Store(self.effect)
        } else {
            omnigraph::seams::Decision::Pass
        }
    }
}

/// Arm every seam declared before one step, in declaration order; the parser
/// already refused the same seam twice before one step. At most one
/// directive per step acts on the store, since admission holds no map from
/// a decision seam to the store call it precedes.
#[cfg(tokio_unstable)]
pub(crate) fn arm_seams(
    seams: &[SeamDirective],
    step: &crate::Step,
) -> Result<Vec<ArmedSeam>, String> {
    use omnigraph_dst::store_places::TargetedRule;
    let admitted = seams
        .iter()
        .map(|seam| admit_seam(seam, Some(step)))
        .collect::<Result<Vec<_>, _>>()?;
    refuse_two_store_actors(seams, &admitted)?;
    let decoration = DECORATION.try_with(|decoration| decoration.clone()).ok();
    seams
        .iter()
        .zip(admitted)
        .map(|(seam, admitted)| {
            let (guard, kind) = match admitted {
                Admitted::Code(entry, effect) => {
                    let (guard, counted) =
                        entry.count_and_fire_at_with(seam.occurrence as u64, effect);
                    (Some(guard), ArmedKind::Code { entry, counted })
                }
                Admitted::CodeStore(entry, effect) => {
                    let Some(decoration) = decoration.clone() else {
                        return Err(store_needs_decoration(&seam.at));
                    };
                    let subject = match entry.store_subject() {
                        Some(declared) => {
                            Some(store_places::Subject::parse(declared).map_err(|_| {
                                format!(
                                    "unsupported_environment: seam {} declares an unparsable subject {declared:?}",
                                    seam.at
                                )
                            })?)
                        }
                        None => None,
                    };
                    let counted = std::sync::Arc::new(StoreCounted {
                        target: seam.occurrence as u64,
                        effect,
                        subject,
                        decoration,
                        crossings: std::sync::atomic::AtomicU64::new(0),
                        fired: std::sync::atomic::AtomicBool::new(false),
                    });
                    let behavior: std::sync::Arc<dyn omnigraph::seams::Decide> = counted.clone();
                    (
                        Some(entry.install(behavior)),
                        ArmedKind::CodeStore { entry, counted },
                    )
                }
                Admitted::Store(row, action, subject) => {
                    let Some(decoration) = decoration.clone() else {
                        return Err(store_needs_decoration(&seam.at));
                    };
                    decoration.targets().install_rule(TargetedRule::new(
                        row.place,
                        subject,
                        seam.occurrence as u64,
                        action,
                    ));
                    (
                        None,
                        ArmedKind::Store {
                            row,
                            action,
                            decoration,
                        },
                    )
                }
            };
            Ok(ArmedSeam {
                at: seam.at.clone(),
                occurrence: seam.occurrence,
                subject: seam.subject.clone(),
                guard,
                kind,
            })
        })
        .collect()
}

#[cfg(tokio_unstable)]
fn store_needs_decoration(at: &str) -> String {
    format!(
        "unsupported_environment: seam {at} acts on the store, which needs the DST target's storage decoration"
    )
}

#[cfg(not(tokio_unstable))]
pub(crate) struct ArmedSeam;

#[cfg(not(tokio_unstable))]
pub(crate) fn arm_seams(
    seams: &[SeamDirective],
    _step: &crate::Step,
) -> Result<Vec<ArmedSeam>, String> {
    if seams.is_empty() {
        Ok(Vec::new())
    } else {
        Err("unsupported_environment: DST runner is unavailable".into())
    }
}

/// Uninstall every decider first and drain the decoration's targeting, then
/// check delivery seam by seam: each site fired exactly on its declared
/// crossing, and each store action has the hit the decoration recorded. The
/// records are the proof a case's report carries, one per seam in
/// declaration order.
#[cfg(tokio_unstable)]
pub(crate) fn finish_seams(mut armed: Vec<ArmedSeam>) -> Result<(), String> {
    for seam in &mut armed {
        drop(seam.guard.take());
    }
    let cleared = DECORATION
        .try_with(|decoration| decoration.targets().clear())
        .ok();
    if cleared.is_none()
        && let Some(seam) = armed
            .iter()
            .find(|seam| !matches!(seam.kind, ArmedKind::Code { .. }))
    {
        return Err(store_needs_decoration(&seam.at));
    }
    let cleared = cleared.unwrap_or_default();
    for seam in &armed {
        let (crossings, effect, declared_at, fired_at, hit, subject) = match &seam.kind {
            ArmedKind::Code { entry, counted } => {
                let crossings = counted.crossings();
                if !counted.fired() {
                    return Err(not_crossed(seam, crossings));
                }
                (
                    crossings,
                    counted.effect().as_str(),
                    location(entry.site()),
                    entry.last_fired().map(location),
                    None,
                    seam.subject.clone(),
                )
            }
            ArmedKind::CodeStore { entry, counted } => {
                let crossings = counted.crossings();
                if !counted.fired() {
                    return Err(not_crossed(seam, crossings));
                }
                let one_shots = cleared
                    .hits
                    .iter()
                    .filter(|hit| hit.one_shot)
                    .collect::<Vec<_>>();
                let hit = match one_shots.as_slice() {
                    [hit] => *hit,
                    [] if cleared.one_shot_unconsumed => {
                        return Err(format!(
                            "seam_unobserved: seam {} fired {} on crossing {} but no put followed the seam before the step ended",
                            seam.at,
                            counted.effect.as_str(),
                            seam.occurrence
                        ));
                    }
                    [] => {
                        return Err(format!(
                            "seam_unobserved: seam {} fired {} on crossing {} but the storage decoration recorded no put",
                            seam.at,
                            counted.effect.as_str(),
                            seam.occurrence
                        ));
                    }
                    hits => {
                        return Err(format!(
                            "seam_unobserved: seam {} fired {} on crossing {} but the storage decoration recorded {} targeted puts",
                            seam.at,
                            counted.effect.as_str(),
                            seam.occurrence,
                            hits.len()
                        ));
                    }
                };
                let (place, action) = store_places::effect_target(counted.effect);
                if hit.place != place || hit.action != action {
                    return Err(format!(
                        "seam_unobserved: seam {} expected {} on {} but the decoration recorded {} on {}",
                        seam.at,
                        action.as_str(),
                        place.as_str(),
                        hit.action.as_str(),
                        hit.place.as_str()
                    ));
                }
                let declared = entry.store_subject();
                if let Some(declared) = declared
                    && !store_places::Subject::parse(declared)
                        .is_ok_and(|subject| subject.matches(&hit.requested))
                {
                    return Err(format!(
                        "seam_unobserved: seam {} declares subject {declared:?} but the decoration recorded a put of {}",
                        seam.at, hit.requested
                    ));
                }
                (
                    crossings,
                    counted.effect.as_str(),
                    location(entry.site()),
                    entry.last_fired().map(location),
                    Some(hit),
                    declared.map(str::to_string),
                )
            }
            ArmedKind::Store { row, action, .. } => {
                let matched = cleared.rule_matched_calls;
                let rule_hits = cleared
                    .hits
                    .iter()
                    .filter(|hit| !hit.one_shot)
                    .collect::<Vec<_>>();
                let hit = match rule_hits.as_slice() {
                    [hit] => *hit,
                    [] => {
                        return Err(format!(
                            "seam_unobserved: store place {} (subject {}) saw {matched} matching call(s) by the selected operation, below occurrence {}",
                            seam.at,
                            seam.subject.as_deref().unwrap_or(""),
                            seam.occurrence
                        ));
                    }
                    hits => {
                        return Err(format!(
                            "seam_unobserved: store place {} (subject {}) fired on occurrence {} but the decoration recorded {} targeted calls",
                            seam.at,
                            seam.subject.as_deref().unwrap_or(""),
                            seam.occurrence,
                            hits.len()
                        ));
                    }
                };
                if hit.place != row.place || hit.action != *action {
                    return Err(format!(
                        "seam_unobserved: seam {} expected {} on {} but the decoration recorded {} on {}",
                        seam.at,
                        action.as_str(),
                        row.place.as_str(),
                        hit.action.as_str(),
                        hit.place.as_str()
                    ));
                }
                (
                    matched,
                    action.as_str(),
                    row.declared_at(),
                    Some(hit.fired_at.clone()),
                    Some(hit),
                    seam.subject.clone(),
                )
            }
        };
        if let Some(hit) = hit
            && !hit.landed
        {
            if hit.method == "write_text_if_absent" {
                return Err(format!(
                    "seam_unobserved: seam {} targeted write_text_if_absent of {} but the object was already present, so nothing was planted",
                    seam.at, hit.requested
                ));
            }
            return Err(format!(
                "seam_unobserved: seam {} targeted {} of {} but the store did not accept the call",
                seam.at, hit.method, hit.requested
            ));
        }
        let mut value = serde_json::json!({"at": seam.at, "occurrence": seam.occurrence, "crossings": crossings, "effect": effect, "declared_at": declared_at, "fired_at": fired_at});
        if let Some(subject) = &subject {
            value["subject"] = serde_json::json!(subject);
        }
        if let Some(hit) = hit {
            value["hit"] = serde_json::json!({"method": hit.method, "requested": hit.requested, "stored": hit.stored});
        }
        record("seam_delivered", value);
        observe(|| {
            format!(
                "seam delivered: {} on crossing {} with effect {effect}",
                seam.at, seam.occurrence
            )
        });
    }
    Ok(())
}

#[cfg(tokio_unstable)]
fn not_crossed(seam: &ArmedSeam, crossings: u64) -> String {
    format!(
        "seam_unobserved: seam {} was not crossed on occurrence {} by the selected operation; crossings observed: {crossings}",
        seam.at, seam.occurrence
    )
}

#[cfg(not(tokio_unstable))]
pub(crate) fn finish_seams(_armed: Vec<ArmedSeam>) -> Result<(), String> {
    Ok(())
}
