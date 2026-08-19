//! Diff and show modes: compare two run records (or two directories of them)
//! point by point, or render one record (or directory) as tables — the
//! before/after and status views the benchmark's records are read through.
//!
//! RFC 0039 conformance: records are matched by their full point name (the
//! run spec flattened — cells under different specs never pair silently); v3
//! records validate against the shipped schema on READ; v1/v2 records are
//! upgraded on load and say so. Rule 4: differing backend, machine, or
//! build-profile identities print a loud warning before any number. Rule 7:
//! with `--floor noise-floor.json` (from `run --aa`), a delta below the
//! session's A/A floor is labeled "no detected effect", never a small effect;
//! a delta above the floor but inside the claim margin is named as the
//! in-between band; a floor applied outside its own cell (different spec,
//! SUT, or session) is marked as extrapolation. Output renders as plain text
//! or GitHub-flavored markdown (`--format md`); the JSON record stays the
//! single source of truth, the tables are views.

use std::collections::BTreeMap;
use std::path::Path;

use crate::fixture::BenchResult;
use crate::record::{FloorPoint, NoiseFloor, PhaseStats, RunRecord, short_commit, upgrade_v2, v2};
use crate::schema;

/// Output format for the diff/show tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    /// Aligned plain-text tables.
    Text,
    /// GitHub-flavored markdown tables (paste-ready for PRs and docs).
    Md,
}

/// Per-column alignment for [`Out::table`]: numbers right, free text left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Align {
    Left,
    Right,
}

/// Table/heading renderer for the two output formats.
struct Out {
    md: bool,
}

impl Out {
    fn new(format: Format) -> Self {
        Self {
            md: format == Format::Md,
        }
    }

    fn heading(&self, text: &str) {
        if self.md {
            println!("### {text}");
            println!();
        } else {
            println!("== {text} ==");
        }
    }

    fn warn(&self, text: &str) {
        if self.md {
            println!("> **WARNING** {text}");
            println!();
        } else {
            println!("WARNING {text}");
        }
    }

    fn note(&self, text: &str) {
        if self.md {
            println!("> note: {text}");
            println!();
        } else {
            println!("note: {text}");
        }
    }

    /// Render one table with the given per-column alignment (`aligns` and
    /// `headers` are the same length).
    fn table(&self, headers: &[&str], aligns: &[Align], rows: &[Vec<String>]) {
        assert_eq!(headers.len(), aligns.len(), "one alignment per column");
        if self.md {
            println!("| {} |", headers.join(" | "));
            let seps: Vec<&str> = aligns
                .iter()
                .map(|a| match a {
                    Align::Left => ":--",
                    Align::Right => "--:",
                })
                .collect();
            println!("| {} |", seps.join(" | "));
            for row in rows {
                println!("| {} |", row.join(" | "));
            }
            println!();
        } else {
            let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
            for row in rows {
                for (i, cell) in row.iter().enumerate() {
                    widths[i] = widths[i].max(cell.len());
                }
            }
            let render = |cells: Vec<String>| {
                let mut line = String::new();
                for (i, cell) in cells.into_iter().enumerate() {
                    if i > 0 {
                        line.push_str("  ");
                    }
                    match aligns[i] {
                        Align::Left => line.push_str(&format!("{cell:<width$}", width = widths[i])),
                        Align::Right => {
                            line.push_str(&format!("{cell:>width$}", width = widths[i]))
                        }
                    }
                }
                // Left-aligned last columns pad to width; the terminal never
                // needs trailing spaces.
                line.trim_end().to_string()
            };
            println!(
                "{}",
                render(headers.iter().map(|h| h.to_string()).collect())
            );
            for row in rows {
                println!("{}", render(row.clone()));
            }
        }
    }

    fn blank(&self) {
        if !self.md {
            println!();
        }
    }
}

pub(crate) fn load_record(path: &Path) -> BenchResult<RunRecord> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    let value: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))?;
    match value.get("record_version").and_then(|v| v.as_u64()) {
        Some(3) => {
            schema::require_valid_v3(&value, &format!("refusing to read {}", path.display()))?;
            Ok(serde_json::from_value(value)
                .map_err(|e| format!("decoding {}: {e}", path.display()))?)
        }
        Some(1) | Some(2) => {
            let old: v2::RunRecord = serde_json::from_value(value)
                .map_err(|e| format!("decoding v1/v2 record {}: {e}", path.display()))?;
            Ok(upgrade_v2(old))
        }
        other => Err(format!(
            "{}: unsupported record_version {other:?} (this binary reads v1..=v3)",
            path.display()
        )
        .into()),
    }
}

pub(crate) fn load_floor(path: &Path) -> BenchResult<NoiseFloor> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    let floor: NoiseFloor =
        serde_json::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))?;
    Ok(floor)
}

fn records_in_dir(dir: &Path) -> BenchResult<BTreeMap<String, RunRecord>> {
    let mut out = BTreeMap::new();
    for path in record_paths(dir)? {
        let record = load_record(&path)?;
        let name = record.point_name.clone();
        if out.insert(name.clone(), record).is_some() {
            return Err(format!(
                "{}: two records share point {name} — ambiguous diff (an A/A pair \
                 belongs to `run --aa` + `--floor`, not to a diff directory)",
                dir.display()
            )
            .into());
        }
    }
    if out.is_empty() {
        return Err(format!("{}: no .json run records found", dir.display()).into());
    }
    Ok(out)
}

/// The `.json` run-record files of a directory, sorted (`noise-floor.json`
/// excluded).
pub(crate) fn record_paths(dir: &Path) -> BenchResult<Vec<std::path::PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|e| format!("reading {}: {e}", dir.display()))? {
        let path = entry.map_err(|e| e.to_string())?.path();
        let is_floor = path.file_name().is_some_and(|n| n == "noise-floor.json");
        if path.extension().is_some_and(|ext| ext == "json") && !is_floor {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

/// Human-readable byte count for identity rows ("64.0 GiB"); "unknown" when
/// the capture recorded nothing — never a Debug-formatted `Option`.
pub(crate) fn fmt_bytes(bytes: Option<u64>) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    match bytes {
        None => "unknown".to_string(),
        Some(b) if b as f64 >= GIB => format!("{:.1} GiB", b as f64 / GIB),
        Some(b) if b as f64 >= MIB => format!("{:.1} MiB", b as f64 / MIB),
        Some(b) => format!("{b} B"),
    }
}

/// Engine configuration as space-separated `KEY=value` pairs; "(none)" for
/// the empty map (no flag set, recorded as such).
fn fmt_engine_config(config: &BTreeMap<String, String>) -> String {
    if config.is_empty() {
        return "(none)".to_string();
    }
    config
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn fmt_delta_pct(a: f64, b: f64) -> String {
    if a == 0.0 {
        return "n/a".to_string();
    }
    format!("{:+.1}%", (b - a) / a * 100.0)
}

fn delta_pct_abs(a: f64, b: f64) -> Option<f64> {
    if a == 0.0 {
        None
    } else {
        Some(((b - a) / a * 100.0).abs())
    }
}

/// Rule-7 label for a delta against its floor and claim margin: below the
/// floor reads "no detected effect"; between floor and floor x margin is the
/// explicit in-between band (not claimable, not "no effect"); above clears.
pub(crate) fn floor_label(a: f64, b: f64, floor_pct: f64, margin: f64) -> String {
    match delta_pct_abs(a, b) {
        Some(pct) if pct <= floor_pct => {
            format!("[below floor {floor_pct:.2}%: no detected effect]")
        }
        Some(pct) if pct <= floor_pct * margin => {
            format!("[within margin x{margin}: not claimable]")
        }
        _ => String::new(),
    }
}

/// Rule-4 (and rule-2/3) identity checks: identities that differ never
/// compare silently. The table still prints — the warning makes the reader
/// carry the caveat, and an unknown identity warns too.
fn print_identity_warnings(a: &RunRecord, b: &RunRecord, out: &Out) {
    let (ea, eb) = (&a.run_spec.environment, &b.run_spec.environment);
    if ea.backend != eb.backend || ea.s3_endpoint != eb.s3_endpoint {
        out.warn(&format!(
            "(RFC 0039 rule 4): backend identities differ — A {} endpoint {} vs \
             B {} endpoint {}; these numbers never compare silently.",
            ea.backend,
            ea.s3_endpoint.as_deref().unwrap_or("unset"),
            eb.backend,
            eb.s3_endpoint.as_deref().unwrap_or("unset")
        ));
    }
    let (ma, mb) = (&a.machine, &b.machine);
    if ma.cpu_model != mb.cpu_model
        || ma.memory_bytes != mb.memory_bytes
        || ma.os != mb.os
        || ma.arch != mb.arch
    {
        out.warn(&format!(
            "(RFC 0039 rule 4): machine specifications differ — A [{} {} {} mem {}] \
             vs B [{} {} {} mem {}]; these numbers never compare silently.",
            ma.os,
            ma.arch,
            ma.cpu_model,
            fmt_bytes(ma.memory_bytes),
            mb.os,
            mb.arch,
            mb.cpu_model,
            fmt_bytes(mb.memory_bytes)
        ));
    }
    if a.sut.build_profile != b.sut.build_profile {
        out.warn(&format!(
            "(RFC 0039 rule 2): build profiles differ ({} vs {}) — wall-clock across \
             build profiles never compares.",
            a.sut.build_profile, b.sut.build_profile
        ));
    }
    if a.run_spec.environment.warmth.regime != b.run_spec.environment.warmth.regime {
        out.warn(&format!(
            "(RFC 0039 rule 3): warmth regimes differ ({} vs {}) — cells under \
             different regimes do not compare.",
            a.run_spec.environment.warmth.regime, b.run_spec.environment.warmth.regime
        ));
    }
    if a.sut.engine_configuration != b.sut.engine_configuration {
        out.note(&format!(
            "SUT engine configurations differ (a systems comparison, as data): \
             A {} vs B {}",
            fmt_engine_config(&a.sut.engine_configuration),
            fmt_engine_config(&b.sut.engine_configuration)
        ));
    }
}

/// Warn on ANY run-spec field mismatch (the machine spec is record-level
/// identity, checked separately): records pair by point name, and the name is
/// derived, so a spec field differing under an equal name is drift the reader
/// must see.
fn print_spec_mismatches(a: &RunRecord, b: &RunRecord, out: &Out) {
    fn walk(a: &serde_json::Value, b: &serde_json::Value, path: &str, diffs: &mut Vec<String>) {
        match (a, b) {
            (serde_json::Value::Object(ma), serde_json::Value::Object(mb)) => {
                for key in ma.keys().chain(mb.keys().filter(|k| !ma.contains_key(*k))) {
                    let sub_path = format!("{path}.{key}");
                    match (ma.get(key), mb.get(key)) {
                        (Some(va), Some(vb)) => walk(va, vb, &sub_path, diffs),
                        _ => diffs.push(format!("{sub_path} (present on one side only)")),
                    }
                }
            }
            _ if a != b => diffs.push(path.to_string()),
            _ => {}
        }
    }
    let (va, vb) = (
        serde_json::to_value(&a.run_spec).expect("run spec serializes"),
        serde_json::to_value(&b.run_spec).expect("run spec serializes"),
    );
    let mut diffs = Vec::new();
    walk(&va, &vb, "run_spec", &mut diffs);
    if !diffs.is_empty() {
        out.warn(&format!(
            "run-spec fields differ between A and B: {} — equal point names promise \
             equal specs; treat the deltas below with that caveat.",
            diffs.join(", ")
        ));
    }
}

/// Cell check for the floor (RFC 0039: a floor licenses comparisons at its
/// own cell — same spec, same SUT, same session; anything else is stated
/// extrapolation).
fn print_floor_cell_warnings(floor: &NoiseFloor, a: &RunRecord, b: &RunRecord, out: &Out) {
    for (r, side) in [(a, "A"), (b, "B")] {
        if floor.source_commit != r.sut.source_commit {
            out.warn(&format!(
                "(RFC 0039 rule 7): the floor was measured on SUT {} but record {side} is \
                 SUT {} — applying it here is extrapolation outside the floor's cell.",
                floor.source_commit, r.sut.source_commit
            ));
        }
        if floor.session_id != r.session_id {
            out.warn(&format!(
                "(RFC 0039 rule 7): the floor comes from session {} but record {side} is \
                 from session {} — a floor is per session; this application is \
                 extrapolation.",
                floor.session_id, r.session_id
            ));
        }
    }
}

/// The directional marker (rule 3) for the persisted tail-support fields of
/// a compared pair: one unsupported side already makes the p95 row
/// directional — it never prints as a clean percentile. The row carries the
/// short marker; [`print_pair`] prints the explanation once as a note.
fn tail_marker(a: &RunRecord, b: &RunRecord) -> &'static str {
    if a.results.wall_clock_ms.tail_support == "supported"
        && b.results.wall_clock_ms.tail_support == "supported"
    {
        ""
    } else {
        " [directional]"
    }
}

pub(crate) fn storage_sums(r: &RunRecord) -> Option<crate::counting::CallCounts> {
    let calls = r.results.storage_calls.as_ref()?;
    let mut sum = crate::counting::CallCounts::default();
    for c in calls.manifest_store.iter().chain(&calls.table_store) {
        sum.get += c.get;
        sum.put += c.put;
        sum.put_part += c.put_part;
        sum.head += c.head;
        sum.list += c.list;
        sum.delete += c.delete;
        sum.copy += c.copy;
        sum.rename += c.rename;
        sum.put_multipart += c.put_multipart;
        sum.multipart_complete += c.multipart_complete;
        sum.multipart_abort += c.multipart_abort;
    }
    Some(sum)
}

fn print_write_path_and_storage(records: &[(&RunRecord, &str)], out: &Out) {
    let mut wp_rows = Vec::new();
    for (r, side) in records {
        let wp = &r.results.write_path;
        if !wp.stage_merge_insert_calls.is_empty() {
            wp_rows.push(vec![
                (*side).to_string(),
                format!(
                    "{}/{}",
                    wp.stage_merge_insert_calls.iter().sum::<u64>(),
                    wp.stage_merge_insert_rows.iter().sum::<u64>()
                ),
                format!(
                    "{}/{}",
                    wp.stage_known_present_update_calls.iter().sum::<u64>(),
                    wp.stage_known_present_update_rows.iter().sum::<u64>()
                ),
                format!(
                    "{}/{}",
                    wp.stage_fenced_insert_calls.iter().sum::<u64>(),
                    wp.stage_fenced_insert_rows.iter().sum::<u64>()
                ),
                wp.strict_insert_preflight_calls
                    .iter()
                    .sum::<u64>()
                    .to_string(),
            ]);
        }
    }
    if !wp_rows.is_empty() {
        out.table(
            &[
                "write-path (sum calls/rows)",
                "merge_insert",
                "known_present_update",
                "fenced_insert",
                "strict_preflight",
            ],
            &[
                Align::Left,
                Align::Right,
                Align::Right,
                Align::Right,
                Align::Right,
            ],
            &wp_rows,
        );
    }
    let mut sc_rows = Vec::new();
    for (r, side) in records {
        if let Some(sum) = storage_sums(r) {
            let calls = r.results.storage_calls.as_ref().expect("summed above");
            sc_rows.push(vec![
                format!(
                    "{side} ({}{})",
                    calls.layer,
                    if calls.physical_attempts.is_none() {
                        "; physical absent"
                    } else {
                        ""
                    }
                ),
                sum.get.to_string(),
                sum.put.to_string(),
                sum.put_part.to_string(),
                sum.head.to_string(),
                sum.list.to_string(),
                sum.delete.to_string(),
                sum.copy.to_string(),
                sum.rename.to_string(),
                sum.put_multipart.to_string(),
            ]);
        } else {
            out.note(&format!(
                "storage-calls {side}: not recorded (v1/v2 record)"
            ));
        }
    }
    if !sc_rows.is_empty() {
        let mut aligns = vec![Align::Right; 10];
        aligns[0] = Align::Left;
        out.table(
            &[
                "storage-calls (merge windows, sum)",
                "get",
                "put",
                "put_part",
                "head",
                "list",
                "delete",
                "copy",
                "rename",
                "put_multipart",
            ],
            &aligns,
            &sc_rows,
        );
    }
}

/// The phase portion of the diff table: paired phases (both zero skipped),
/// then phases present on only one side — a missing side is stated in the
/// same style in both directions, never silently dropped.
fn phase_rows(
    a: &[PhaseStats],
    b: &[PhaseStats],
    label: impl Fn(&str, f64, f64) -> String,
) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for pa in a {
        let Some(pb) = b.iter().find(|p| p.phase == pa.phase) else {
            rows.push(vec![
                format!("phase {} total_us p50", pa.phase),
                "?".to_string(),
                "(missing from B)".to_string(),
                String::new(),
                String::new(),
                String::new(),
            ]);
            continue;
        };
        let (va, vb) = (pa.total_us_p50 as f64, pb.total_us_p50 as f64);
        if va == 0.0 && vb == 0.0 {
            continue;
        }
        rows.push(vec![
            format!("phase {} total_us p50", pa.phase),
            format!("{va:.0}"),
            format!("{vb:.0}"),
            format!("{:+.0}", vb - va),
            fmt_delta_pct(va, vb),
            label(&pa.phase, va, vb),
        ]);
    }
    for pb in b {
        if a.iter().all(|p| p.phase != pb.phase) {
            rows.push(vec![
                format!("phase {} total_us p50", pb.phase),
                "(missing from A)".to_string(),
                "?".to_string(),
                String::new(),
                String::new(),
                String::new(),
            ]);
        }
    }
    rows
}

fn print_pair(
    a: &RunRecord,
    b: &RunRecord,
    floor: Option<&NoiseFloor>,
    margin_override: Option<f64>,
    out: &Out,
) {
    out.heading(&format!(
        "point A {} -> B {} (warmth {}) : A = {} @ {} -> B = {} @ {}",
        a.point_name,
        b.point_name,
        a.run_spec.environment.warmth.regime,
        a.label.as_deref().unwrap_or("unlabeled"),
        short_commit(&a.sut.source_commit),
        b.label.as_deref().unwrap_or("unlabeled"),
        short_commit(&b.sut.source_commit),
    ));
    print_identity_warnings(a, b, out);
    print_spec_mismatches(a, b, out);
    let margin = margin_override
        .or(floor.map(|f| f.default_margin))
        .unwrap_or_else(crate::record::default_margin);
    let floor_point: Option<&FloorPoint> = floor.and_then(|f| {
        print_floor_cell_warnings(f, a, b, out);
        let hit = f
            .points
            .get(&a.point_name)
            .or_else(|| f.points.get(&b.point_name));
        if hit.is_none() {
            out.note(&format!(
                "the noise floor has no entry for point {} — wall/phase deltas print \
                 unlabeled; rerun `run --aa` on this point for a floor.",
                a.point_name
            ));
        }
        hit
    });
    if let Some(fp) = floor_point {
        // The effective margin, with the floor file's persisted default
        // beside it when a `--margin` override displaced it.
        let margin_note = match (margin_override, floor) {
            (Some(m), Some(f)) => format!(
                "claim margin x{m} (floor file default x{})",
                f.default_margin
            ),
            _ => format!("claim margin x{margin}"),
        };
        out.note(&format!(
            "A/A noise floor for this point: {:.2}% ({:.2} ms on p50 {:.1}/{:.1} ms); \
             {margin_note}",
            fp.pct, fp.abs_delta_ms, fp.wall_p50_a_ms, fp.wall_p50_b_ms
        ));
    }
    let marker = tail_marker(a, b);
    if !marker.is_empty() {
        out.note("directional p95 (rule 3): < 20 reps on at least one side — read as worst observed, not a tail.");
    }
    let mut rows = Vec::new();
    for (name, va, vb, marker) in [
        (
            "wall_clock_ms p50",
            a.results.wall_clock_ms.p50,
            b.results.wall_clock_ms.p50,
            "",
        ),
        (
            "wall_clock_ms p95",
            a.results.wall_clock_ms.p95,
            b.results.wall_clock_ms.p95,
            marker,
        ),
    ] {
        let label = floor_point.map_or(String::new(), |fp| floor_label(va, vb, fp.pct, margin));
        rows.push(vec![
            format!("{name}{marker}"),
            format!("{va:.2}"),
            format!("{vb:.2}"),
            format!("{:+.2}", vb - va),
            fmt_delta_pct(va, vb),
            label,
        ]);
    }
    rows.extend(phase_rows(
        &a.results.phases,
        &b.results.phases,
        |phase, va, vb| {
            floor_point
                .and_then(|fp| fp.phases.get(phase))
                .map_or(String::new(), |phase_floor| {
                    floor_label(va, vb, *phase_floor, margin)
                })
        },
    ));
    out.table(
        &["metric", "A", "B", "delta", "delta%", "floor"],
        &[
            Align::Left,
            Align::Right,
            Align::Right,
            Align::Right,
            Align::Right,
            Align::Left,
        ],
        &rows,
    );
    print_write_path_and_storage(&[(a, "A"), (b, "B")], out);
    out.blank();
}

/// Render one record's identity and results as tables (the `show` view).
fn print_record(r: &RunRecord, out: &Out) {
    out.heading(&format!(
        "record {} : {} @ {}",
        r.point_name,
        r.label.as_deref().unwrap_or("unlabeled"),
        short_commit(&r.sut.source_commit),
    ));
    let identity = vec![
        vec!["point_name".to_string(), r.point_name.clone()],
        vec!["profile".to_string(), r.profile.clone()],
        vec!["invocation_id".to_string(), r.invocation_id.clone()],
        vec!["session_id".to_string(), r.session_id.clone()],
        vec!["source_commit".to_string(), r.sut.source_commit.clone()],
        vec![
            "build".to_string(),
            format!(
                "{} (opt-level {})",
                r.sut.build_profile, r.sut.build_opt_level
            ),
        ],
        vec![
            "backend".to_string(),
            r.run_spec.environment.backend.clone(),
        ],
        vec![
            "machine".to_string(),
            format!(
                "{} {} / {} / mem {}",
                r.machine.os,
                r.machine.arch,
                r.machine.cpu_model,
                fmt_bytes(r.machine.memory_bytes)
            ),
        ],
        vec![
            "warmth".to_string(),
            r.run_spec.environment.warmth.regime.clone(),
        ],
        vec![
            "repetitions".to_string(),
            format!(
                "{} ({})",
                r.run_spec.protocol.repetitions, r.results.wall_clock_ms.tail_support
            ),
        ],
    ];
    out.table(
        &["identity", "value"],
        &[Align::Left, Align::Left],
        &identity,
    );
    let w = &r.results.wall_clock_ms;
    out.table(
        &["wall_clock_ms", "p50", "p95", "min", "max", "mean"],
        &[
            Align::Left,
            Align::Right,
            Align::Right,
            Align::Right,
            Align::Right,
            Align::Right,
        ],
        &[vec![
            format!("{} reps", w.reps.len()),
            format!("{:.2}", w.p50),
            format!("{:.2}", w.p95),
            format!("{:.2}", w.min),
            format!("{:.2}", w.max),
            format!("{:.2}", w.mean),
        ]],
    );
    // Grouped phase view: plain-language group rows (total ms + share of
    // the wall-clock median) with the raw phases indented beneath each —
    // grouping adds a layer, the raw names and µs stay. The remainder the 14
    // phases do not cover prints as its own explicit row, never normalized
    // away (see groups.rs for the verified mapping and the remainder's
    // mechanism).
    if !r.results.phases.is_empty() {
        let grouped = crate::groups::group_phases(&r.results.phases, w.p50);
        let mut rows = Vec::new();
        for group in &grouped.groups {
            rows.push(vec![
                group.name.to_string(),
                format!("{:.1} ms", group.total_us as f64 / 1000.0),
                String::new(),
                String::new(),
                format!("{:.1}%", group.share_pct),
            ]);
            for m in &group.members {
                rows.push(vec![
                    format!("  {}", m.name),
                    format!("{} us", m.total_us_p50),
                    format!("{} us", m.total_us_max),
                    format!("{} us", m.max_single_us),
                    String::new(),
                ]);
            }
        }
        if grouped.shows_unattributed() {
            rows.push(vec![
                crate::groups::UNATTRIBUTED_NAME.to_string(),
                format!("{:.1} ms", grouped.unattributed_us as f64 / 1000.0),
                String::new(),
                String::new(),
                format!("{:.1}%", grouped.unattributed_share_pct),
            ]);
        }
        out.table(
            &[
                "merge phases (grouped)",
                "total p50",
                "total max",
                "max single",
                "share",
            ],
            &[
                Align::Left,
                Align::Right,
                Align::Right,
                Align::Right,
                Align::Right,
            ],
            &rows,
        );
        if grouped.shows_unattributed() {
            out.note(&format!(
                "{}: {}",
                crate::groups::UNATTRIBUTED_NAME,
                crate::groups::UNATTRIBUTED_ANNOTATION
            ));
        }
        if grouped.over_attributed {
            out.note(crate::groups::OVER_ATTRIBUTION_NOTE);
        }
    }
    print_write_path_and_storage(&[(r, "this run")], out);
    out.blank();
}

/// Entry point for `show`: render one record file, or every record of a
/// directory, as tables.
pub fn run_show(path: &Path, format: Format) -> BenchResult<()> {
    let out = Out::new(format);
    if path.is_dir() {
        let paths = record_paths(path)?;
        if paths.is_empty() {
            return Err(format!("{}: no .json run records found", path.display()).into());
        }
        for p in paths {
            print_record(&load_record(&p)?, &out);
        }
    } else {
        print_record(&load_record(path)?, &out);
    }
    Ok(())
}

/// Entry point: `a` and `b` are either two record files or two directories
/// whose records are matched by their full point name.
pub fn run_diff(
    a: &Path,
    b: &Path,
    floor: Option<&Path>,
    margin: Option<f64>,
    format: Format,
) -> BenchResult<()> {
    if let Some(m) = margin {
        if m < 1.0 {
            return Err("--margin must be >= 1.0 (1.0 = the floor itself)".into());
        }
    }
    let out = Out::new(format);
    let floor = floor.map(load_floor).transpose()?;
    match (a.is_dir(), b.is_dir()) {
        (false, false) => {
            let (ra, rb) = (load_record(a)?, load_record(b)?);
            if ra.point_name != rb.point_name {
                out.note(&format!(
                    "comparing different points ({} vs {}) — deltas below cross points",
                    ra.point_name, rb.point_name
                ));
            }
            print_pair(&ra, &rb, floor.as_ref(), margin, &out);
            Ok(())
        }
        (true, true) => {
            let (mapa, mapb) = (records_in_dir(a)?, records_in_dir(b)?);
            let mut matched = 0usize;
            for (key, ra) in &mapa {
                if let Some(rb) = mapb.get(key) {
                    print_pair(ra, rb, floor.as_ref(), margin, &out);
                    matched += 1;
                } else {
                    out.note(&format!("point {key} present only in {}", a.display()));
                }
            }
            for key in mapb.keys().filter(|k| !mapa.contains_key(*k)) {
                out.note(&format!("point {key} present only in {}", b.display()));
            }
            if matched == 0 {
                return Err("no point name exists in both directories".into());
            }
            Ok(())
        }
        _ => Err("pass two record files or two directories, not a mix".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_labels_name_all_three_bands() {
        // (a, b, floor_pct, margin, expected fragment; "" = clears the floor)
        let cases: [(f64, f64, f64, f64, &str); 6] = [
            (100.0, 100.5, 2.0, 2.0, "no detected effect"),
            (100.0, 102.0, 2.0, 2.0, "no detected effect"),
            (100.0, 103.0, 2.0, 2.0, "within margin"),
            (100.0, 104.0, 2.0, 2.0, "within margin"),
            (100.0, 110.0, 2.0, 2.0, ""),
            (0.0, 50.0, 2.0, 2.0, ""), // zero baseline: n/a, never labeled
        ];
        for (a, b, floor_pct, margin, expected) in cases {
            let label = floor_label(a, b, floor_pct, margin);
            if expected.is_empty() {
                assert!(
                    label.is_empty(),
                    "({a}, {b}): expected no label, got {label}"
                );
            } else {
                assert!(
                    label.contains(expected),
                    "({a}, {b}): expected '{expected}' in '{label}'"
                );
            }
        }
    }

    #[test]
    fn margin_widens_the_unclaimable_band() {
        // At margin x3 a 5% delta over a 2% floor is still inside the band.
        assert!(floor_label(100.0, 105.0, 2.0, 3.0).contains("within margin"));
        assert!(floor_label(100.0, 105.0, 2.0, 2.0).is_empty());
    }

    #[test]
    fn fmt_bytes_uses_human_units_never_debug_forms() {
        // (input, expected)
        for (input, expected) in [
            (None, "unknown"),
            (Some(68_719_476_736), "64.0 GiB"),
            (Some(17_179_869_184), "16.0 GiB"),
            (Some(536_870_912), "512.0 MiB"),
            (Some(1_610_612_736), "1.5 GiB"),
            (Some(1024), "1024 B"),
            (Some(0), "0 B"),
        ] {
            assert_eq!(fmt_bytes(input), expected, "input {input:?}");
        }
    }

    fn phase(name: &str, p50: u64) -> PhaseStats {
        PhaseStats {
            phase: name.to_string(),
            total_us_p50: p50,
            total_us_max: p50,
            max_single_us: p50,
            per_rep_total_us: vec![p50],
        }
    }

    #[test]
    fn phase_rows_state_a_only_and_b_only_phases_in_both_directions() {
        let a = [phase("Shared", 100), phase("OnlyA", 5)];
        let b = [phase("Shared", 120), phase("OnlyB", 7)];
        let rows = phase_rows(&a, &b, |_, _, _| String::new());
        let row_for = |name: &str| {
            rows.iter()
                .find(|r| r[0].contains(name))
                .unwrap_or_else(|| panic!("no row for {name}"))
        };
        assert_eq!(row_for("Shared")[1], "100");
        assert_eq!(row_for("Shared")[2], "120");
        assert_eq!(row_for("OnlyA")[2], "(missing from B)");
        assert_eq!(row_for("OnlyB")[1], "(missing from A)");
        // Both-zero pairs are skipped, one-sided phases never are.
        let a = [phase("Zero", 0)];
        let b = [phase("Zero", 0), phase("OnlyB", 0)];
        let rows = phase_rows(&a, &b, |_, _, _| String::new());
        assert_eq!(rows.len(), 1);
        assert!(rows[0][0].contains("OnlyB"));
    }
}
