//! WRITE CENSUS (coverage-map instrument): record every
//! write-class storage call that reaches a wrapper — both realms — and
//! classify each key against the taxonomy of KNOWN artifact kinds. The
//! census answers two harness-honesty questions no oracle asks: (1) how
//! many writes does the interposition actually see per universe, and
//! (2) does the engine write any artifact kind the coverage map has
//! never heard of (an UNKNOWN row = a new work item, not an engine bug).
//! Global slot, ARMED EXTERNALLY by the census instrument (unlike the
//! lance fault slots, run_universe cannot clear it at start without
//! wiping the armed instrument); cleared only by arm()/harvest(), so a
//! census test that panics mid-universe leaks its rows until the next
//! arm() — bounded by #[serial] and by the panic already failing the
//! suite.
//!
//! Reconciliation boundary: the bottom listings see the store at
//! universe END, so a bypass write that was deleted again before then
//! leaves no key to reconcile — the census catches persistent bypass
//! writers, not transient ones.

use std::sync::Mutex;

/// One recorded write-class call: (realm, op, uri, counting).
/// `counting = false` marks harness-window writes (init/fixtures, oracle
/// checks, reconcile/recovery) — the same gate the kill counter uses.
#[derive(Clone, Debug)]
pub struct CensusRow {
    pub realm: &'static str,
    pub op: String,
    pub uri: String,
    pub counting: bool,
}

static CENSUS: Mutex<Option<Vec<CensusRow>>> = Mutex::new(None);

/// Bottom-count listings gathered at universe end while the stores are
/// alive: (adapter-realm keys, lance-realm keys). Set by `run_universe`
/// when the census is active; harvested by the reconciling test.
static FINAL_KEYS: Mutex<Option<(Vec<String>, Vec<String>)>> = Mutex::new(None);

/// Arm the census for the next universe (clears any predecessor's rows).
pub fn arm() {
    *CENSUS.lock().unwrap() = Some(Vec::new());
    *FINAL_KEYS.lock().unwrap() = None;
}

/// Whether the census is currently recording (run_universe reads this to
/// decide whether to gather the end-of-universe bottom listings).
pub fn recording() -> bool {
    CENSUS.lock().unwrap().is_some()
}

/// Harvest and deactivate; None if never activated.
pub fn harvest() -> Option<Vec<CensusRow>> {
    CENSUS.lock().unwrap().take()
}

/// Stash the end-of-universe bottom listings (called from run_universe).
pub fn set_final_keys(adapter: Vec<String>, lance: Vec<String>) {
    *FINAL_KEYS.lock().unwrap() = Some((adapter, lance));
}

/// Harvest the bottom listings; None if run_universe never gathered them.
pub fn harvest_final_keys() -> Option<(Vec<String>, Vec<String>)> {
    FINAL_KEYS.lock().unwrap().take()
}

/// Normalize a recorded adapter URI to raw-key space: strip the scheme
/// (`shared-memory://x/y` -> `x/y`), then run the remainder through
/// `object_store::path::Path` — the SAME normalization the store's own
/// listing keys went through, so the reconciliation compares like with
/// like (a plain string strip can differ from the stored key on empty
/// or dot segments). Engine-minted URIs always carry a scheme; the
/// scheme-less arm exists for hand-built test keys.
pub fn normalize_adapter_uri(uri: &str) -> String {
    let stripped = match uri.find("://") {
        Some(idx) => &uri[idx + 3..],
        None => uri,
    };
    object_store::path::Path::from(stripped.trim_start_matches('/')).to_string()
}

/// Record one write-class call. No-op when the census is not active, so
/// the hooks cost one mutex probe in ordinary runs.
pub fn record(realm: &'static str, op: &str, uri: &str, counting: bool) {
    if let Some(rows) = CENSUS.lock().unwrap().as_mut() {
        rows.push(CensusRow {
            realm,
            op: op.to_string(),
            uri: uri.to_string(),
            counting,
        });
    }
}

/// The complete inventory of write primitives the two wrappers can
/// intercept, as "realm:op" labels: the adapter realm's six
/// `StorageAdapter` write methods and the Lance proxy's write set.
/// Audited against both trait definitions 2026-08-24 (StorageAdapter in
/// omnigraph-storage/src/lib.rs; object_store 0.13.2, where every
/// defaulted write method routes through these four proxy arms).
/// The census asserts observed ⊆ inventory — a NOVEL op is an arm the
/// audit never saw, a staleness red. The reverse direction (inventory
/// arms a run never fired) is REPORT-ONLY: no single workload exercises
/// every primitive (the CAS fires only in ack-loss instruments), so
/// never-fired is a per-run coverage fact, not a failure.
pub const WRITE_PRIMITIVES: &[&str] = &[
    "adapter:write_text",
    "adapter:write_text_if_absent",
    "adapter:write_text_if_match",
    "adapter:rename_text",
    "adapter:delete",
    "adapter:delete_prefix",
    "lance:put",
    "lance:put_multipart",
    "lance:delete",
    "lance:copy",
];

/// TYPED EXCLUSIONS from completion-cut coverage claims (#527 P1-3/P1-4):
/// primitives the interposition sees but whose INTERNAL stages the cut
/// coordinates cannot split — each row is (primitive, why the exclusion
/// holds). Any coverage claim ("cuts enumerate every durable completion")
/// carries this carve-out; the census gate and report print it so the
/// claim can never silently swallow these arms. The multipart row is
/// additionally TRIPWIRED (the census gate reds on first multipart use).
/// Removing a row requires the mechanism that splits its stages.
/// TODO(#527): part-level multipart cuts; the per-stage sequencer below
/// the adapter (v2 tracking issues renumber this marker).
pub const CUT_COVERAGE_EXCLUSIONS: &[(&str, &str)] = &[
    (
        "lance:put_multipart",
        "part-level writes bypass the completion hooks; tripwired — first use reds the census gate",
    ),
    (
        "adapter:rename_text",
        "the adapter's internal copy+delete stages count as ONE completion; the copied-but-undeleted intermediate is not an enumerable cut",
    ),
    (
        "adapter:delete_prefix",
        "per-key deletes inside one call count as ONE completion; partial-prefix states are not enumerable cuts",
    ),
];

/// Diff a run's observed "realm:op" labels against `WRITE_PRIMITIVES`:
/// returns (never_fired, novel). Callers assert `novel` empty and print
/// `never_fired` as the dormant-arms report.
pub fn inventory_diff(
    observed: &std::collections::BTreeSet<String>,
) -> (Vec<&'static str>, Vec<String>) {
    let never_fired = WRITE_PRIMITIVES
        .iter()
        .copied()
        .filter(|p| !observed.contains(*p))
        .collect();
    let novel = observed
        .iter()
        .filter(|o| !WRITE_PRIMITIVES.contains(&o.as_str()))
        .cloned()
        .collect();
    (never_fired, novel)
}

/// The census reconciliation checks shared by the per-PR gate and the
/// report instrument (one implementation — the two tests must not drift):
/// (1) counting rows == the kill counter's W. WEATHERLESS-ONLY identity:
/// the census records at write_fault entry (before the error/lose rolls)
/// while W counts landed writes, so the equality holds iff no weather is
/// armed — an internal-consistency tripwire on hook placement; the
/// INDEPENDENT evidence is (3)/(4) plus the bypass red-proof.
/// (2) the Lance bottom listing is non-empty (a universe always writes
/// Lance keys; an empty listing means the from-below channel is broken
/// and the reconciliation would be vacuously green).
/// (3)/(4) every bottom-listed key in either realm was recorded — a key
/// the store holds that the census never saw is a writer bypassing the
/// interposition.
///
/// # Panics
/// On any failed check, labeled with `label`.
pub fn assert_reconciles(
    label: &str,
    rows: &[CensusRow],
    writes_observed: usize,
    adapter_keys: &[String],
    lance_keys: &[String],
) {
    let counting_rows = rows.iter().filter(|c| c.counting).count();
    assert_eq!(
        counting_rows, writes_observed,
        "{label}: census counting rows must equal the kill counter's W"
    );
    assert!(
        !lance_keys.is_empty(),
        "{label}: the Lance bottom listing came back EMPTY — a universe always \
         writes Lance keys, so an empty listing means the from-below channel is \
         broken and the reconciliation would be vacuously green"
    );
    let recorded_adapter: std::collections::BTreeSet<String> = rows
        .iter()
        .filter(|c| c.realm == "adapter")
        .map(|c| normalize_adapter_uri(&c.uri))
        .collect();
    let recorded_lance: std::collections::BTreeSet<&str> = rows
        .iter()
        .filter(|c| c.realm == "lance")
        .map(|c| c.uri.as_str())
        .collect();
    let orphan_adapter: Vec<&String> = adapter_keys
        .iter()
        .filter(|k| !recorded_adapter.contains(k.as_str()))
        .collect();
    let orphan_lance: Vec<&String> = lance_keys
        .iter()
        .filter(|k| !recorded_lance.contains(k.as_str()))
        .collect();
    assert!(
        orphan_adapter.is_empty() && orphan_lance.is_empty(),
        "{label}: store holds keys the census never saw written — a write path \
         bypasses the interposition: adapter {orphan_adapter:?} lance {orphan_lance:?}"
    );
}

/// The coverage-map taxonomy: every KNOWN artifact kind the engine
/// writes, as key patterns. A key matching no row classifies UNKNOWN —
/// the census's red, meaning the map is out of date, and the new kind
/// needs a triage decision (corruption verbs? kill spine? recovery
/// obligation?) before being added here.
pub fn classify(uri: &str) -> &'static str {
    // Order matters: most specific first.
    if uri.contains("__recovery/") {
        return "recovery-sidecar";
    }
    // TRIAGE OPEN: the init-claim file —
    // written write_text_if_absent during store birth, deleted when init
    // completes. Known to the map, NOT yet triaged: does any instrument
    // own a death between its write and its delete (stranded claim in the
    // root)? Should persisted-tier corruption verbs target it? #495
    // neighborhood.
    if uri.contains("__init_claim") {
        return "init-claim";
    }
    // TRIAGE OPEN: create-if-absent
    // capability probes — three throwaway files written+deleted at birth.
    // NOT yet triaged: a death between write and delete strands them as
    // orphan artifacts; does anything ever clean them, and can a foreign
    // stranded probe at reopen block recovery the way a foreign-named
    // sidecar does (unhealable-yet-blocking)?
    if uri.contains("__create_if_absent_probe") {
        return "capability-probe";
    }
    // INTENDED COLLAPSE: __manifest is itself a Lance dataset, so its
    // sub-artifacts (its _transactions/, _versions/, data files) all
    // classify as the coarse manifest-realm class by this early match —
    // the Lance-shape rows below describe the TABLE datasets only.
    if uri.contains("__manifest") {
        return "manifest";
    }
    if uri.contains("_transactions/") || uri.ends_with(".txn") {
        return "lance-transaction";
    }
    if uri.contains("_versions/") || uri.ends_with(".manifest") {
        return "lance-version-manifest";
    }
    if uri.contains("_indices/") {
        return "lance-index";
    }
    if uri.contains("_deletions/") {
        return "lance-deletion";
    }
    if (uri.contains("/data/") || uri.starts_with("data/")) && uri.ends_with(".lance") {
        // Segment-anchored: a bare `contains("data/")` also matches any
        // segment ENDING in "data" (`metadata/x.lance`), which would
        // silently swallow the UNKNOWN red.
        return "lance-data-file";
    }
    if uri.contains("_refs/") || uri.contains("_branches/") {
        return "branch-ref";
    }
    // Exact shapes only: a broad substring here would silently swallow
    // the UNKNOWN red for a future artifact whose name merely contains
    // the word (under-matching is safe, over-matching defeats the map).
    if uri.contains("_schema.pg") || uri.contains("_schema.ir") || uri.contains("__schema_state") {
        return "schema-artifact";
    }
    "UNKNOWN"
}
