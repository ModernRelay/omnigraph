//! B5: proptest-state-machine campaign with automatic SHRINKING + regression
//! persistence, over the CLEAN op subset.
//!
//! proptest-state-machine fails and minimizes on ANY reference↔SUT divergence,
//! so it runs over the ops that have no known open bug — insert-person,
//! insert-doc, update-doc, read (no delete/optimize/edge/concurrency, which
//! trigger RC-1 / RC-X / FTS-OOB / dup-`@key`). Over that subset reference and
//! engine must agree exactly; a regression that breaks a clean op auto-minimizes
//! to the shortest failing op sequence and persists its seed under
//! `proptest-regressions/`. The generative walk + the named regressions cover
//! the buggy ops; this layer adds the shrinking machinery.
//!
//! The async engine is bridged the canonical sync way (`proptest_equivalence.rs`
//! pattern): the SUT owns a current-thread `Runtime` and every engine call is a
//! `rt.block_on(...)` — the whole campaign is a plain `#[test]`, no ambient
//! runtime, so `block_on` is legal.

use std::collections::BTreeMap;

use proptest::prelude::*;
use proptest::strategy::Union;
use proptest::test_runner::Config;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use tokio::runtime::Runtime;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;

use crate::backend;
use crate::op::{doc, person};

/// The reference model (also the state-machine `State`): the keys that must
/// exist and each Doc's expected body. `BTreeMap`/`u32` keep `Debug` output
/// stable, which keeps shrink reports deterministic.
#[derive(Clone, Debug, Default)]
pub struct RefModel {
    persons: BTreeMap<u32, ()>,
    docs: BTreeMap<u32, String>,
    next: u32,
}

/// The clean transitions. Ids are explicit so the reference is a pure function.
#[derive(Clone, Debug)]
pub enum Tr {
    InsertPerson(u32),
    InsertDoc(u32),
    /// (existing doc id, tag) → body `u{id}-{tag}`.
    UpdateDoc(u32, u32),
    Read,
}

impl ReferenceStateMachine for RefModel {
    type State = RefModel;
    type Transition = Tr;

    fn init_state() -> BoxedStrategy<RefModel> {
        Just(RefModel::default()).boxed()
    }

    fn transitions(state: &RefModel) -> BoxedStrategy<Tr> {
        let next = state.next;
        let mut options: Vec<BoxedStrategy<Tr>> = vec![
            Just(Tr::InsertPerson(next)).boxed(),
            Just(Tr::InsertDoc(next)).boxed(),
            Just(Tr::Read).boxed(),
        ];
        if !state.docs.is_empty() {
            let ids: Vec<u32> = state.docs.keys().copied().collect();
            options.push(
                (proptest::sample::select(ids), any::<u16>())
                    .prop_map(|(id, tag)| Tr::UpdateDoc(id, tag as u32))
                    .boxed(),
            );
        }
        Union::new(options).boxed()
    }

    fn apply(mut state: RefModel, transition: &Tr) -> RefModel {
        match transition {
            Tr::InsertPerson(id) => {
                state.persons.insert(*id, ());
                state.next = state.next.max(id + 1);
            }
            Tr::InsertDoc(id) => {
                state.docs.insert(*id, "needle filler".to_string());
                state.next = state.next.max(id + 1);
            }
            Tr::UpdateDoc(id, tag) => {
                // Mirror engine no-op semantics: only an existing doc changes.
                if state.docs.contains_key(id) {
                    state.docs.insert(*id, format!("u{id}-{tag}"));
                }
            }
            Tr::Read => {}
        }
        state
    }
}

/// The system under test: a real graph plus the runtime that drives it.
pub struct Sut {
    rt: Runtime,
    db: Omnigraph,
    _dir: tempfile::TempDir,
}

pub struct DstMachine;

impl Sut {
    fn person_count(&self) -> usize {
        self.rt.block_on(async {
            self.db
                .query(
                    ReadTarget::branch("main"),
                    "query q() { match { $x: Person } return { $x.slug } }",
                    "q",
                    &ParamMap::new(),
                )
                .await
                .unwrap()
                .num_rows()
        })
    }

    /// id → body for every live Doc.
    fn doc_bodies(&self) -> BTreeMap<u32, String> {
        self.rt.block_on(async {
            let res = self
                .db
                .query(
                    ReadTarget::branch("main"),
                    "query c() { match { $d: Doc } return { $d.slug, $d.body } }",
                    "c",
                    &ParamMap::new(),
                )
                .await
                .unwrap();
            let json = res.to_rust_json();
            let mut out = BTreeMap::new();
            for row in json.as_array().unwrap() {
                let slug = row["d.slug"].as_str().unwrap();
                let body = row["d.body"].as_str().unwrap().to_string();
                let id: u32 = slug.strip_prefix('g').unwrap().parse().unwrap();
                out.insert(id, body);
            }
            out
        })
    }
}

impl StateMachineTest for DstMachine {
    type SystemUnderTest = Sut;
    type Reference = RefModel;

    fn init_test(_ref_state: &RefModel) -> Sut {
        let rt = Runtime::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap().to_string();
        let db = rt.block_on(backend::open_clean(&uri));
        Sut {
            rt,
            db,
            _dir: dir,
        }
    }

    fn apply(sut: Sut, _ref_state: &RefModel, transition: Tr) -> Sut {
        sut.rt.block_on(async {
            match transition {
                Tr::InsertPerson(id) => {
                    load_jsonl(&sut.db, &person(&format!("g{id}")), LoadMode::Merge)
                        .await
                        .unwrap();
                }
                Tr::InsertDoc(id) => {
                    load_jsonl(&sut.db, &doc(&format!("g{id}"), "whatsapp"), LoadMode::Merge)
                        .await
                        .unwrap();
                }
                Tr::UpdateDoc(id, tag) => {
                    let q = format!(
                        "query u() {{ update Doc set {{ body: \"u{id}-{tag}\" }} where slug = \"g{id}\" }}"
                    );
                    sut.db.mutate("main", &q, "u", &ParamMap::new()).await.unwrap();
                }
                Tr::Read => {
                    sut.db
                        .query(
                            ReadTarget::branch("main"),
                            "query w() { match { $d: Doc { source: \"whatsapp\" } } return { $d.slug } }",
                            "w",
                            &ParamMap::new(),
                        )
                        .await
                        .unwrap();
                }
            }
        });
        sut
    }

    fn check_invariants(sut: &Sut, ref_state: &RefModel) {
        // count==model and content==model against the reference, plus structural
        // row-id uniqueness — all must hold exactly over the clean op subset.
        assert_eq!(
            sut.person_count(),
            ref_state.persons.len(),
            "Person count diverged from reference"
        );
        assert_eq!(
            sut.doc_bodies(),
            ref_state.docs,
            "Doc id→body diverged from reference (lost/dup/stale write)"
        );
        sut.rt
            .block_on(crate::invariants::no_duplicate_live_row_ids(&sut.db))
            .expect("duplicate live stable row-id");
    }
}

proptest_state_machine::prop_state_machine! {
    #![proptest_config(Config { cases: 24, .. Config::default() })]

    /// Over the clean op subset, the engine must track the reference for any
    /// generated sequence of 1..30 ops; a divergence auto-shrinks + persists.
    #[test]
    fn clean_ops_track_reference(sequential 1..30 => DstMachine);
}
