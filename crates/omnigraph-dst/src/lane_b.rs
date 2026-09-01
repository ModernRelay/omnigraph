//! Lane B replay judge — the reference-model judgment shared by every
//! real-kill instrument (the `dst_child` binary's parents in
//! `tests/lane_b.rs`, and any future baseline runner).
//!
//! Rebuilds the expected lb- world by replaying the op log (Jepsen
//! history semantics: `ok` must have effect, `err` must not, the single
//! unclosed `invoke` is indeterminate, legal both ways), then requires
//! the reopened store to match a candidate world EXACTLY on persons AND
//! visible edges. Any mismatch is a finding: lost acked op, lost update,
//! phantom effect of a rejected op, ghost edge, resurrection after
//! remove. Edges are judged as VISIBLE pairs (both endpoints alive):
//! `all_knows` is a traversal, so a Knows row whose endpoint was removed
//! is invisible by construction. The fixture rows (loaded before arming,
//! untouched by the workload) are additionally asserted intact on every
//! judged branch — a cut that damages a row OUTSIDE the lb- namespace
//! must not judge green. The judge reads only branch HEADS, so
//! retention-horizon/time-travel readability classes are structurally
//! out of its scope.
//!
//! WEATHER MODE (`weather = true`): an injected clean error can land on
//! a POST-COMMIT write, so an `err` op may legally have applied
//! (visible-but-unclaimed, the reason the crash contract is two-sided).
//! Every `err` op is therefore indeterminate, resolved PER KEY: the
//! actual final state of each person/edge must be reachable by some
//! on/off choice of that key's optional ops. For this op alphabet the
//! per-key factoring is EQUIVALENT to full candidate enumeration
//! (names are minted uniquely and every data op touches one key, so
//! optional choices are independent across keys). The residual
//! looseness is the err-indeterminacy itself: a lost acked write is
//! masked on any key whose later optional ops can reproduce the
//! observed state (e.g. `ok insert; err remove` observed as absent).
//! Weather children are main-only: a fork snapshot couples every key's
//! indeterminacy, and resolving that needs the WorldModel, not a
//! re-derivation.
//!
//! Census note: lane B's `FINDING:` panics sit OUTSIDE the detector
//! census (`detectors.rs`), like the concurrent judges — the lane is a
//! labeled preview; its verdicts enter the census when it qualifies.

use std::collections::{BTreeMap, BTreeSet};

use omnigraph::db::Omnigraph;

use crate::fixtures::{
    fixture_knows, fixture_persons, knows_pairs_on, person_rows_on, physical_view_on,
};
use crate::oplog::{self, LB_BRANCH_PREFIX};

type Persons = BTreeMap<String, i64>;
type Edges = BTreeSet<(String, String)>;

#[derive(Clone, Default)]
struct World {
    persons: Persons,
    edges: Edges,
}

impl World {
    fn visible(&self) -> (Persons, Edges) {
        let edges = self
            .edges
            .iter()
            .filter(|(a, b)| self.persons.contains_key(a) && self.persons.contains_key(b))
            .cloned()
            .collect();
        (self.persons.clone(), edges)
    }
}

type Worlds = BTreeMap<String, World>;

/// One person-state transition for a data op (`kind`, its age token when
/// the kind carries one, the current state). The ONE encoding of the
/// engine's per-person semantics — `apply` (whole-world replay) and the
/// weather judge's per-key resolver both call it, so a new op kind
/// cannot land in one and not the other.
fn person_next(kind: &str, age_token: Option<&str>, current: Option<i64>) -> Option<i64> {
    let age = || -> i64 {
        age_token
            .expect("age token present for insert/set_age")
            .parse()
            .expect("age token in invoke line must be i64")
    };
    match kind {
        "insert" => Some(age()),
        // Engine semantics: update-where, a miss updates nothing.
        "set_age" => current.map(|_| age()),
        "remove" => None,
        // Edge ops do not change person state.
        _ => current,
    }
}

/// Log grammar: `invoke {i} {target} {kind} {args...}`. branch_create is
/// a fork-clone of main (the WorldModel's own BranchCreate semantics).
/// Deliberately NOT the in-suite `Model`: that type is harness-private,
/// carries `ver`/ghost machinery this alphabet lacks, and cascades edge
/// deletion on person removal, while this judge keeps edge rows and
/// judges VISIBLE pairs instead.
fn apply(worlds: &mut Worlds, parts: &[&str], label: &str) {
    let target = parts[2];
    match parts[3] {
        "branch_create" => {
            let main = worlds.get("main").expect("main world").clone();
            worlds.insert(parts[4].to_string(), main);
        }
        "branch_delete" => {
            worlds.remove(parts[4]);
        }
        "insert" | "set_age" | "remove" => {
            let w = worlds
                .get_mut(target)
                .unwrap_or_else(|| panic!("{label}: op targets unknown branch {target}"));
            let current = w.persons.get(parts[4]).copied();
            match person_next(parts[3], parts.get(5).copied(), current) {
                Some(age) => {
                    w.persons.insert(parts[4].to_string(), age);
                }
                None => {
                    w.persons.remove(parts[4]);
                }
            }
        }
        "edge" => {
            let w = worlds
                .get_mut(target)
                .unwrap_or_else(|| panic!("{label}: op targets unknown branch {target}"));
            w.edges.insert((parts[4].to_string(), parts[5].to_string()));
        }
        other => panic!("{label}: unknown op kind in log: {other}"),
    }
}

/// Duplicate-row tripwire + prefix projection: the raw row list is
/// SORTED, so two rows for one name are adjacent — assert none before
/// collapsing into a map (a `BTreeMap` collect would silently mask a
/// duplicated person row, the born-on-both / double-apply signature).
fn persons_map(rows: &[(String, i64, i64)], prefix: &str, label: &str, channel: &str) -> Persons {
    for pair in rows.windows(2) {
        assert!(
            pair[0].0 != pair[1].0,
            "{label}: FINDING: duplicate {channel} person row for {:?} \
             (rows {pair:?}); a map projection would have masked it",
            pair[0].0
        );
    }
    rows.iter()
        .filter(|(n, _, _)| n.starts_with(prefix))
        .map(|(n, age, _)| (n.clone(), *age))
        .collect()
}

/// Fixture intactness: the rows `TEST_DATA` loaded sit outside the lb-
/// prefix and receive zero workload ops, so their expected state is
/// unconditional on every judged branch (forks clone them from main).
fn assert_fixtures_intact(
    rows: &[(String, i64, i64)],
    knows: &[(String, String)],
    branch: &str,
    label: &str,
) {
    let actual: Persons = rows.iter().map(|(n, age, _)| (n.clone(), *age)).collect();
    for (name, age) in fixture_persons() {
        assert!(
            actual.get(&name) == Some(&age),
            "{label}: FINDING: fixture person {name} on branch {branch} is {:?}, \
             expected age {age} (fixture rows receive no workload ops)",
            actual.get(&name)
        );
    }
    for (from, to) in fixture_knows() {
        assert!(
            knows.contains(&(from.clone(), to.clone())),
            "{label}: FINDING: fixture edge {from}->{to} missing on branch {branch}"
        );
    }
}

/// # Panics
/// Panics with a `FINDING:` message on any judgment failure (world
/// mismatch, channel disagreement, duplicate row, damaged fixture row),
/// and on op-log corruption via [`oplog::parse`].
pub async fn lane_b_replay_judge(
    db: &Omnigraph,
    log: &str,
    prefix: &str,
    label: &str,
    weather: bool,
) -> &'static str {
    let s = oplog::parse(log, label);
    // A judged log is always a workload log; a log without the
    // fixtures-loaded line is the wrong file (a probe or recovery log)
    // or a torn setup — judging it would compare against nothing.
    assert!(
        s.fixtures_loaded,
        "{label}: log carries no fixtures-loaded line — not a workload log"
    );
    let unclosed: Vec<&(usize, String)> = s
        .invokes
        .iter()
        .filter(|(i, _)| !s.outcomes.contains_key(i))
        .collect();

    if weather {
        return weather_judge(db, &s, prefix, label).await;
    }

    let mut base: Worlds = Default::default();
    base.insert("main".to_string(), World::default());
    for (i, line) in &s.invokes {
        if s.outcomes.get(i) == Some(&true) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            apply(&mut base, &parts, label);
        }
    }
    let mut candidates: Vec<(&'static str, Worlds)> = vec![("without-op", base.clone())];
    if let Some((_, line)) = unclosed.first() {
        // The with-op candidate applies the unclosed op as-written, even
        // when it is statically rejectable (the poison insert): a store
        // that applied it exactly would match, one that truncated the
        // value mismatches anyway. Tightening to reject known-poison
        // shapes is deliberately left out of this alphabet's judge.
        let mut with = base.clone();
        let parts: Vec<&str> = line.split_whitespace().collect();
        apply(&mut with, &parts, label);
        candidates.push(("with-op", with));
    }

    // Engine-side lb- branch set (branch resurrection / loss detector).
    let engine_branches: BTreeSet<String> = db
        .branch_list()
        .await
        .expect("branch_list")
        .into_iter()
        .filter(|b| b.starts_with(LB_BRANCH_PREFIX))
        .collect();

    let mut mentioned: BTreeSet<String> = engine_branches.clone();
    mentioned.insert("main".to_string());
    for (_, worlds) in &candidates {
        mentioned.extend(worlds.keys().cloned());
    }
    let mut actual: BTreeMap<String, (Persons, Edges)> = Default::default();
    for b in &mentioned {
        if b != "main" && !engine_branches.contains(b) {
            continue;
        }
        let rows = person_rows_on(db, b).await;
        let knows = knows_pairs_on(db, b).await;
        assert_fixtures_intact(&rows, &knows, b, label);
        let persons = persons_map(&rows, prefix, label, "query");
        let edges: Edges = knows
            .into_iter()
            .filter(|(a, bb)| a.starts_with(prefix) && bb.starts_with(prefix))
            .collect();
        channel_agreement(db, b, prefix, &persons, label).await;
        actual.insert(b.clone(), (persons, edges));
    }

    'candidate: for (verdict, worlds) in &candidates {
        let expected_branches: BTreeSet<String> =
            worlds.keys().filter(|b| *b != "main").cloned().collect();
        if expected_branches != engine_branches {
            continue;
        }
        for (b, w) in worlds {
            let Some((p_act, e_act)) = actual.get(b) else {
                continue 'candidate;
            };
            let (p, e) = w.visible();
            if p != *p_act || e != *e_act {
                continue 'candidate;
            }
        }
        return verdict;
    }
    let (verdict0, worlds0) = &candidates[0];
    let expected0: Vec<String> = worlds0.keys().cloned().collect();
    panic!(
        "{label}: FINDING: post-recovery world matches no legal candidate.\n\
         candidate {verdict0} branches: {expected0:?}\n\
         engine lb- branches: {engine_branches:?}\n\
         candidate main: {:?}\n\
         actual: {actual:?}\n\
         candidates tried: {}",
        worlds0.get("main").map(|w| w.visible()),
        candidates
            .iter()
            .map(|(v, _)| *v)
            .collect::<Vec<_>>()
            .join(", ")
    );
}

/// SECOND CHANNEL (query alone can lie: ghost rows are DEFINED as
/// query-vs-physical disagreement): the physical channel must agree with
/// the query channel on lb- persons, per branch, before any candidate
/// matching. Persons only: edge cascade semantics on remove are not
/// pinned here, so raw physical edges are not compared.
async fn channel_agreement(
    db: &Omnigraph,
    branch: &str,
    prefix: &str,
    persons_query: &Persons,
    label: &str,
) {
    let physical_rows = physical_view_on(db, branch).await.0;
    let persons_physical = persons_map(&physical_rows, prefix, label, "physical");
    assert!(
        persons_physical == *persons_query,
        "{label}: FINDING: channel disagreement on lb- persons of branch {branch} \
         (ghost or lost row).\nquery:    {persons_query:?}\nphysical: {persons_physical:?}"
    );
}

async fn weather_judge(
    db: &Omnigraph,
    s: &oplog::OplogSummary,
    prefix: &str,
    label: &str,
) -> &'static str {
    let engine_branches: Vec<String> = db
        .branch_list()
        .await
        .expect("branch_list")
        .into_iter()
        .filter(|b| b.starts_with(LB_BRANCH_PREFIX))
        .collect();
    assert!(
        engine_branches.is_empty(),
        "{label}: weather child is main-only but engine has lb- branches: {engine_branches:?}"
    );
    let rows = person_rows_on(db, "main").await;
    let knows = knows_pairs_on(db, "main").await;
    assert_fixtures_intact(&rows, &knows, "main", label);
    let persons_actual = persons_map(&rows, prefix, label, "query");
    let edges_actual: Edges = knows
        .into_iter()
        .filter(|(a, b)| a.starts_with(prefix) && b.starts_with(prefix))
        .collect();
    channel_agreement(db, "main", prefix, &persons_actual, label).await;

    let op_mandatory = |i: &usize| s.outcomes.get(i).copied();
    let mut names: BTreeSet<String> = persons_actual.keys().cloned().collect();
    let mut pairs: BTreeSet<(String, String)> = edges_actual.clone();
    for (_, line) in &s.invokes {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts[3] {
            "insert" | "set_age" | "remove" => {
                names.insert(parts[4].to_string());
            }
            "edge" => {
                pairs.insert((parts[4].to_string(), parts[5].to_string()));
            }
            other => panic!("{label}: weather child produced branch op: {other}"),
        }
    }
    for name in &names {
        let mut states: BTreeSet<Option<i64>> = [None].into_iter().collect();
        for (i, line) in &s.invokes {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.get(4) != Some(&name.as_str()) {
                continue;
            }
            let next = |st: &Option<i64>| person_next(parts[3], parts.get(5).copied(), *st);
            states = match op_mandatory(i) {
                Some(true) => states.iter().map(next).collect(),
                // err or unclosed: the op may or may not have applied.
                _ => states
                    .iter()
                    .map(next)
                    .chain(states.iter().cloned())
                    .collect(),
            };
        }
        let actual = persons_actual.get(name).copied();
        assert!(
            states.contains(&actual),
            "{label}: FINDING: person {name} is {actual:?} but the op history \
             can only reach {states:?}"
        );
    }
    for (from, to) in &pairs {
        // Presence accumulates: no op removes an edge, so once an `ok`
        // makes it mandatory, a later `err` on the same pair must not
        // reintroduce the absent possibility (last-op-wins would mask a
        // lost acked edge).
        let mut possible = BTreeSet::from([false]);
        for (i, line) in &s.invokes {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts[3] == "edge" && parts[4] == from && parts[5] == to {
                match op_mandatory(i) {
                    Some(true) => {
                        possible = BTreeSet::from([true]);
                    }
                    _ => {
                        possible.insert(true);
                    }
                }
            }
        }
        let endpoints_alive =
            persons_actual.contains_key(from.as_str()) && persons_actual.contains_key(to.as_str());
        let visible = edges_actual.contains(&(from.clone(), to.clone()));
        let legal = if visible {
            endpoints_alive && possible.contains(&true)
        } else {
            !endpoints_alive || possible.contains(&false)
        };
        assert!(
            legal,
            "{label}: FINDING: edge {from}->{to} visible={visible} is unreachable \
             (written-possibilities {possible:?}, endpoints_alive={endpoints_alive})"
        );
    }
    "weather-resolved"
}
