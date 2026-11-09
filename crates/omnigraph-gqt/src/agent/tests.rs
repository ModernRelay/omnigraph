//! The instrument's data stays valid: every edition-2 ground truth is
//! accepted and returns its recorded rows on its seed, every edition-2 seed is
//! refused with its code, every card matches its pin and the edition-2 card
//! regenerates from the skill, every edition-3 ground truth is accepted by
//! the full front end and every edition-3 seed refused with its code.

use std::path::Path;

use super::*;

fn instrument() -> Instrument {
    Instrument::load(&Instrument::default_root()).expect("the agent instrument loads")
}

fn repo_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn the_instrument_has_the_shape_stage_m0_names() {
    let instrument = instrument();
    let set = |name: &'static str| instrument.tasks.iter().filter(move |t| t.set == name);
    assert_eq!(
        set("tasks").count(),
        45,
        "the competence instrument holds 45 tasks"
    );
    assert_eq!(set("corpus").count(), 30, "the agent corpus holds 30 tasks");
    let several = set("corpus").filter(|t| t.edition2.len() > 1).count();
    assert!(
        several * 3 >= 30,
        "at least a third of the corpus needs several edition-2 queries; {several} do"
    );
    let datasets: std::collections::BTreeSet<&str> =
        set("corpus").map(|t| t.dataset.as_str()).collect();
    assert!(
        (2..=3).contains(&datasets.len()),
        "the corpus runs against two or three datasets, not {datasets:?}"
    );
    for task in &instrument.tasks {
        let signature = task.signature().unwrap();
        if let [step] = task.edition2.as_slice() {
            assert_eq!(
                task::entry_params(&step.query).unwrap(),
                signature,
                "{}: both editions' ground truths take the parameters the prompt states",
                task.id
            );
        }
    }
    for seed in instrument.seeds.iter().filter(|s| s.edition == 2) {
        let task = instrument.task(&seed.task).expect("seeds name tasks");
        assert!(
            !task.edition2.is_empty(),
            "seed {} repairs task {}, which has no edition-2 ground truth to judge the repair by",
            seed.id,
            task.id
        );
        assert!(
            seed.code.is_some(),
            "edition-2 seed {} names its code",
            seed.id
        );
    }
}

#[tokio::test]
async fn edition2_ground_truths_are_accepted_and_return_their_recorded_rows() {
    let instrument = instrument();
    let mut checked = 0;
    for task in instrument.tasks.iter().filter(|t| !t.edition2.is_empty()) {
        let dataset = &instrument.datasets[&task.dataset];
        let catalog = catalog(dataset).unwrap();
        for (index, step) in task.edition2.iter().enumerate() {
            let verdict = accept(Edition::Two, &catalog, &step.query);
            assert!(
                matches!(verdict, Verdict::Accepted(_)),
                "{}: edition-2 step {} is refused: {}",
                task.id,
                index + 1,
                verdict.to_json()
            );
        }
        if let Some(check) = &task.check {
            let verdict = accept(Edition::Two, &catalog, check);
            assert!(
                matches!(verdict, Verdict::Accepted(_)),
                "{}: the check is refused: {}",
                task.id,
                verdict.to_json()
            );
        }
        let graph = Graph::open(dataset).await.unwrap();
        let rows = run_truth(&graph, task, &task.edition2)
            .await
            .unwrap_or_else(|e| panic!("{}: the edition-2 ground truth fails: {e}", task.id));
        let expect = task
            .expect
            .as_ref()
            .expect("an edition-2 truth records its rows");
        if let Some(pinned) = &expect.pinned {
            assert!(
                rows.matches(pinned, expect.ordered),
                "{}: the edition-2 ground truth no longer returns its recorded rows; if the change \
                 is intended, rerun `omnigraph-gqt-agent record --write` and review the rows\n\
                 recorded: {pinned:?}\nnow: {rows:?}",
                task.id
            );
        }
        checked += 1;
    }
    assert!(checked >= 50, "only {checked} edition-2 ground truths ran");
}

#[test]
fn edition2_seeds_are_refused_with_their_code() {
    let instrument = instrument();
    for seed in instrument.seeds.iter().filter(|s| s.edition == 2) {
        let task = instrument.task(&seed.task).unwrap();
        let catalog = catalog(&instrument.datasets[&task.dataset]).unwrap();
        let verdict = accept(Edition::Two, &catalog, &seed.text);
        assert!(
            matches!(verdict, Verdict::Refused(_)) && verdict.code() == seed.code.as_deref(),
            "seed {} ({}) should be refused with {:?}: {}",
            seed.id,
            seed.class,
            seed.code,
            verdict.to_json()
        );
    }
}

#[test]
fn cards_match_their_pins_and_the_edition2_card_regenerates() {
    let root = Instrument::default_root();
    let manifest = card::Manifest::read(&root).unwrap();
    assert!(
        !manifest.model.is_empty(),
        "manifest.json pins the agent model"
    );
    let mismatches = manifest.verify(&root);
    assert!(
        mismatches.is_empty(),
        "a card changed without its pin; review the card, then run \
         `omnigraph-gqt-agent cards --write`:\n{}",
        mismatches.join("\n")
    );
    let generated = card::edition2(&card::skill_root(&repo_root())).unwrap();
    let on_disk = std::fs::read_to_string(card::card_path(&root, 2)).unwrap();
    assert!(
        generated == on_disk,
        "skills/omnigraph changed, so the edition-2 card no longer matches it; run \
         `omnigraph-gqt-agent cards --write`, which regenerates the card and its pin"
    );
}

/// The judge's verdicts on answers whose correctness is known: the ground
/// truth itself, the same query under other names, a wrong answer, a write,
/// and an answer of several declarations.
#[tokio::test]
async fn the_judge_runs_answers_and_compares_rows_and_typed_ir() {
    let instrument = instrument();
    let mut judge = Judge::new(&instrument);
    let truth = &instrument.task("t14").unwrap().edition2[0].query;
    let verdict = judge.judge("t14", Edition::Two, truth).await;
    assert_eq!(verdict["correct"], true, "{verdict}");
    assert_eq!(verdict["ir_equal"], true, "{verdict}");

    let renamed = "query who_works_there($slug: String) {\n  match { $company: Company { slug: $slug }  \
                   $person worksAt $company }\n  return { $person.name }\n}";
    let verdict = judge.judge("t14", Edition::Two, renamed).await;
    assert_eq!(verdict["correct"], true, "{verdict}");
    assert_eq!(
        verdict["ir_equal"], true,
        "other names, the same typed IR: {verdict}"
    );

    let aliased = "query staff($slug: String) {\n  match { $c: Company { slug: $slug }  $p worksAt $c }\n  \
                   return { $p.name as employee }\n}";
    let verdict = judge.judge("t14", Edition::Two, aliased).await;
    assert_eq!(
        verdict["correct"], true,
        "rows compare by position: {verdict}"
    );
    assert_eq!(
        verdict["ir_equal"], false,
        "an alias is a column the truth has not: {verdict}"
    );

    let wrong = "query staff($slug: String) {\n  match { $c: Company { slug: $slug }  $p worksAt $c  \
                 $p knows $q }\n  return { $q.name }\n}";
    let verdict = judge.judge("t14", Edition::Two, wrong).await;
    assert_eq!(verdict["accepted"], true, "{verdict}");
    assert_eq!(verdict["correct"], false, "{verdict}");
    assert_eq!(verdict["ir_equal"], false, "{verdict}");

    let write = &instrument.task("t34").unwrap().edition2[0].query;
    let verdict = judge.judge("t34", Edition::Two, write).await;
    assert_eq!(verdict["correct"], true, "{verdict}");
    let read_instead = "query close_company($slug: String) {\n  match { $c: Company { slug: $slug } }\n  \
                        return { $c.name }\n}";
    let verdict = judge.judge("t34", Edition::Two, read_instead).await;
    assert_eq!(
        verdict["correct"], false,
        "a read answers no write: {verdict}"
    );

    let two = format!("{truth}\n\n{renamed}");
    let verdict = judge.judge("t14", Edition::Two, &two).await;
    assert_eq!(verdict["queries"], 2, "{verdict}");
    assert!(
        verdict["correct"].is_null(),
        "two declarations are judged by hand: {verdict}"
    );

    // Edition 3 does not execute: typed-IR equality after normalisation.
    let renamed3 = "gq 3;\nquery who($slug: String) {\n  match { $company: Company  $company.slug = $slug  \
                    $person worksAt $company }\n  return { $person.name }\n}\n";
    let verdict = judge.judge("t14", Edition::Three, renamed3).await;
    assert_eq!(
        verdict["correct"], true,
        "other names, a filter for the braces: {verdict}"
    );
    let wrong3 = "gq 3;\nquery staff($slug: String) {\n  match { $c: Company { slug: $slug }  $p worksAt $c  \
                  $p knows $q }\n  return { $q.name }\n}\n";
    let verdict = judge.judge("t14", Edition::Three, wrong3).await;
    assert_eq!(verdict["correct"], false, "{verdict}");
    let verdict = judge.judge("t14", Edition::Two, renamed3).await;
    assert_eq!(
        verdict["accepted"], false,
        "edition 3 text is no edition-2 answer: {verdict}"
    );

    let refused = "query staff { match { $c: Company } return { $c.name } }";
    let verdict = judge.judge("t14", Edition::Two, refused).await;
    assert_eq!(verdict["accepted"], false, "{verdict}");
    assert_eq!(verdict["diagnostic"]["code"], "Q002", "{verdict}");
}

/// Every edition-3 ground truth, alternatives included, is accepted by the
/// full front end, and every edition-3 seed is refused with its code.
#[test]
fn edition3_ground_truths_are_accepted_and_seeds_refused() {
    let instrument = instrument();
    let mut failures = Vec::new();
    for task in &instrument.tasks {
        let catalog = catalog(&instrument.datasets[&task.dataset]).unwrap();
        for (index, truth) in task.edition3.iter().enumerate() {
            let verdict = accept(Edition::Three, &catalog, &edition3_text(truth));
            if !matches!(verdict, Verdict::Accepted(_)) {
                failures.push(format!(
                    "{} edition-3 truth {}: {}",
                    task.id,
                    index + 1,
                    verdict.to_json()
                ));
            }
        }
    }
    for seed in instrument.seeds.iter().filter(|s| s.edition == 3) {
        let task = instrument.task(&seed.task).unwrap();
        let catalog = catalog(&instrument.datasets[&task.dataset]).unwrap();
        let verdict = accept(Edition::Three, &catalog, &seed.text);
        let refused = matches!(verdict, Verdict::Refused(_));
        let coded = seed.code.is_some() && verdict.code() == seed.code.as_deref();
        if !(refused && coded) {
            failures.push(format!("seed {}: {}", seed.id, verdict.to_json()));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
