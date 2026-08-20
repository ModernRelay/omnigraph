//! The `report` subcommand: render run records as one self-contained HTML
//! page — the shareable view of the same records `show` prints as tables.
//!
//! Self-containment is a hard constraint: no external requests of any kind
//! (no CDN scripts, no web fonts, no remote images). All CSS is inline, the
//! charts are inline SVG generated here, and there is no JavaScript —
//! collapsed sections use `<details>`, so the page reads fully with JS
//! disabled. Identity is stated with the same honesty as the terminal view:
//! the full SUT commit (dirty/stale markers included), session, machine,
//! warmth, directional tail labels, and rule-7 floor labels on any delta.
//! Every interpolated free-form string passes through [`esc`]. The JSON
//! record stays the single source of truth; this page is a view.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap::Args;

use crate::counting::CallCounts;
use crate::diff::{floor_label, fmt_bytes, fmt_delta_pct, load_floor, load_record, record_paths};
use crate::fixture::BenchResult;
use crate::record::{ControlCalls, NoiseFloor, RunRecord, short_commit};

#[derive(Debug, Args)]
pub struct ReportArgs {
    /// Run-record file, or directory of run records.
    path: PathBuf,
    /// noise-floor.json from `run --aa`; labels the A/A pair deltas per
    /// RFC 0039 rule 7 (below the floor reads "no detected effect").
    #[arg(long)]
    floor: Option<PathBuf>,
    /// Output HTML file (default: report.html beside the records).
    #[arg(long)]
    out: Option<PathBuf>,
}

pub fn run_report(args: ReportArgs) -> BenchResult<()> {
    let (dir, paths) = if args.path.is_dir() {
        (args.path.clone(), record_paths(&args.path)?)
    } else {
        let parent = args
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf();
        (parent, vec![args.path.clone()])
    };
    if paths.is_empty() {
        return Err(format!("{}: no .json run records found", args.path.display()).into());
    }
    let mut records = Vec::with_capacity(paths.len());
    for path in &paths {
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string());
        records.push((file_name, load_record(path)?));
    }
    let floor = args.floor.as_deref().map(load_floor).transpose()?;
    let html = render_report(&records, floor.as_ref(), crate::unix_now());
    let out = args.out.unwrap_or_else(|| dir.join("report.html"));
    std::fs::write(&out, html).map_err(|e| format!("writing {}: {e}", out.display()))?;
    println!("report written: {}", out.display());
    Ok(())
}

/// HTML-escape every free-form string before interpolation (labels, notes,
/// commits, phase names — anything not generated here is untrusted markup).
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// `1234567` -> `1,234,567` (tabular figures need readable magnitudes).
fn fmt_thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// UTC timestamp for the page header, from seconds since the UNIX epoch
/// (civil-from-days, so the crate needs no calendar dependency).
fn fmt_utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let secs_of_day = secs % 86_400;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02} UTC",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

fn sum_counts(counts: &[CallCounts]) -> CallCounts {
    counts.iter().fold(CallCounts::default(), |mut acc, c| {
        acc.get += c.get;
        acc.put += c.put;
        acc.put_part += c.put_part;
        acc.head += c.head;
        acc.list += c.list;
        acc.delete += c.delete;
        acc.copy += c.copy;
        acc.rename += c.rename;
        acc.put_multipart += c.put_multipart;
        acc.multipart_complete += c.multipart_complete;
        acc.multipart_abort += c.multipart_abort;
        acc
    })
}

/// The 11 RFC-031 operation classes of one summed tally, in vocabulary order.
fn class_values(c: &CallCounts) -> [(&'static str, u64); 11] {
    [
        ("get", c.get),
        ("put", c.put),
        ("put_part", c.put_part),
        ("head", c.head),
        ("list", c.list),
        ("delete", c.delete),
        ("copy", c.copy),
        ("rename", c.rename),
        ("put_multipart", c.put_multipart),
        ("multipart_complete", c.multipart_complete),
        ("multipart_abort", c.multipart_abort),
    ]
}

fn total_calls(c: &CallCounts) -> u64 {
    class_values(c).iter().map(|(_, v)| *v).sum()
}

/// Horizontal SVG bar chart: one row per (label, value, annotation). Colors
/// come from the page's CSS variables so the chart follows the theme.
fn svg_bars(rows: &[(String, u64, String)]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    const LABEL_W: u64 = 200;
    const BAR_MAX_W: u64 = 380;
    const ROW_H: u64 = 22;
    let peak = rows.iter().map(|(_, v, _)| *v).max().unwrap_or(0).max(1);
    let height = rows.len() as u64 * ROW_H + 6;
    // Inline SVG in HTML needs no xmlns, and the namespace URI would trip
    // the self-containment guard on external URLs.
    let mut svg = format!(
        "<svg viewBox=\"0 0 720 {height}\" width=\"720\" height=\"{height}\" role=\"img\">\n"
    );
    for (i, (label, value, annotation)) in rows.iter().enumerate() {
        let y = i as u64 * ROW_H + 4;
        let w = (value * BAR_MAX_W) / peak;
        // A nonzero value always draws a visible sliver.
        let w = if *value > 0 { w.max(2) } else { 0 };
        svg.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" text-anchor=\"end\" class=\"bl\">{}</text>\
             <rect x=\"{}\" y=\"{y}\" width=\"{w}\" height=\"14\" rx=\"2\" class=\"bar\"/>\
             <text x=\"{}\" y=\"{}\" class=\"bv\">{}{}</text>\n",
            LABEL_W - 8,
            y + 11,
            esc(label),
            LABEL_W,
            LABEL_W + w + 6,
            y + 11,
            fmt_thousands(*value),
            esc(annotation),
        ));
    }
    svg.push_str("</svg>\n");
    svg
}

/// One grouped-phase bar: name + value/share, a CSS track whose fill width IS
/// the share of wall-clock (bars and shares can never disagree), and the
/// one-line annotation. The unattributed remainder renders in the muted fill.
fn phase_group_bar(
    out: &mut String,
    name: &str,
    total_us: u64,
    share_pct: f64,
    annotation: &str,
    unattributed: bool,
) {
    let fill = if unattributed { "fill un" } else { "fill" };
    out.push_str(&format!(
        "<div class=\"grp\">\n<div class=\"grp-head\"><span>{}</span>\
         <span class=\"grp-val\">{:.1} ms · {:.1}%</span></div>\n\
         <div class=\"track\"><div class=\"{fill}\" style=\"width:{:.1}%\"></div></div>\n\
         <p class=\"muted\">{}</p>\n</div>\n",
        esc(name),
        total_us as f64 / 1000.0,
        share_pct,
        share_pct.min(100.0),
        esc(annotation),
    ));
}

fn identity_rows(file_name: &str, r: &RunRecord) -> Vec<(String, String)> {
    let m = &r.machine;
    let w = &r.run_spec.environment.warmth;
    let engine_config = if r.sut.engine_configuration.is_empty() {
        "(none)".to_string()
    } else {
        r.sut
            .engine_configuration
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    vec![
        ("record file".into(), file_name.to_string()),
        ("point_name".into(), r.point_name.clone()),
        ("profile".into(), r.profile.clone()),
        (
            "label".into(),
            r.label.clone().unwrap_or("unlabeled".into()),
        ),
        ("invocation_id".into(), r.invocation_id.clone()),
        ("session_id".into(), r.session_id.clone()),
        ("source_commit".into(), r.sut.source_commit.clone()),
        (
            "build".into(),
            format!(
                "{} (opt-level {})",
                r.sut.build_profile, r.sut.build_opt_level
            ),
        ),
        ("engine configuration".into(), engine_config),
        ("backend".into(), r.run_spec.environment.backend.clone()),
        (
            "s3_endpoint".into(),
            r.run_spec
                .environment
                .s3_endpoint
                .clone()
                .unwrap_or("unset".into()),
        ),
        (
            "warmth".into(),
            format!(
                "{} ({} warm-up(s) discarded)",
                w.regime, w.warmup_reps_discarded
            ),
        ),
        ("warmth detail".into(), w.detail.clone()),
        (
            "machine".into(),
            format!(
                "{} {} ({}) / {} / mem {} / storage {}",
                m.os,
                m.arch,
                m.os_version,
                m.cpu_model,
                fmt_bytes(m.memory_bytes),
                m.storage_class
            ),
        ),
        (
            "repetitions".into(),
            format!(
                "{} ({})",
                r.run_spec.protocol.repetitions, r.results.wall_clock_ms.tail_support
            ),
        ),
        (
            "rep independence".into(),
            r.run_spec.protocol.rep_independence.clone(),
        ),
        ("instrument".into(), r.run_spec.protocol.instrument.clone()),
        ("timer".into(), r.run_spec.protocol.timer.clone()),
        (
            "storage-call scope".into(),
            r.results
                .storage_calls
                .as_ref()
                .map_or("not recorded (v1/v2 record)".into(), |sc| sc.scope.clone()),
        ),
    ]
}

/// The `-dirty` / `-stale-build` / `unverified:` markers stay visible: the
/// commit prints whole, plus a badge when any marker is present.
fn commit_html(commit: &str) -> String {
    let mut badges = String::new();
    for marker in ["-dirty", "-stale-build"] {
        if commit.contains(marker) {
            badges.push_str(&format!(
                " <span class=\"badge\">{}</span>",
                esc(marker.trim_start_matches('-'))
            ));
        }
    }
    if commit.starts_with("unverified:") {
        badges.push_str(" <span class=\"badge\">unverified</span>");
    }
    format!("<code>{}</code>{badges}", esc(commit))
}

fn per_record_section(out: &mut String, file_name: &str, r: &RunRecord) {
    let w = &r.results.wall_clock_ms;
    let directional = if w.tail_support == "supported" {
        ""
    } else {
        " <span class=\"badge\">directional</span>"
    };
    out.push_str(&format!(
        "<section id=\"r-{}\">\n<h2>{}</h2>\n<p class=\"muted\">{} @ {} · warmth {} · \
         {} reps{directional}</p>\n",
        esc(&r.invocation_id),
        esc(&r.point_name),
        esc(r.label.as_deref().unwrap_or("unlabeled")),
        commit_html(&r.sut.source_commit),
        esc(&r.run_spec.environment.warmth.regime),
        r.run_spec.protocol.repetitions,
    ));

    // Headline first: the answer as metric cards before any table.
    let grouped = crate::groups::group_phases(&r.results.phases, w.p50);
    let storage_total = r.results.storage_calls.as_ref().map(|sc| {
        total_calls(&sum_counts(&sc.table_store)) + total_calls(&sum_counts(&sc.manifest_store))
    });
    let (cost_name, cost_share) = grouped.biggest_cost();
    out.push_str("<div class=\"cards\">\n");
    out.push_str(&format!(
        "<div class=\"card\"><div class=\"v\">{:.1} ms</div>\
         <div class=\"l\">median merge (p50)</div></div>\n",
        w.p50
    ));
    out.push_str(&format!(
        "<div class=\"card\"><div class=\"v\">{:.1} ms{directional}</div>\
         <div class=\"l\">p95</div></div>\n",
        w.p95
    ));
    out.push_str(&format!(
        "<div class=\"card\"><div class=\"v\">{}</div>\
         <div class=\"l\">storage requests</div></div>\n",
        storage_total.map_or("n/a".to_string(), fmt_thousands)
    ));
    out.push_str(&format!(
        "<div class=\"card\"><div class=\"v\">{} {:.0}%</div>\
         <div class=\"l\">biggest cost</div></div>\n",
        esc(cost_name),
        cost_share
    ));
    out.push_str("</div>\n");

    // Wall-clock stats.
    out.push_str(
        "<table><thead><tr><th>wall_clock_ms</th><th class=\"num\">p50</th>\
         <th class=\"num\">p95</th><th class=\"num\">min</th><th class=\"num\">max</th>\
         <th class=\"num\">mean</th></tr></thead><tbody>\n",
    );
    out.push_str(&format!(
        "<tr><td>{} reps{directional}</td><td class=\"num\">{:.2}</td>\
         <td class=\"num\">{:.2}</td><td class=\"num\">{:.2}</td>\
         <td class=\"num\">{:.2}</td><td class=\"num\">{:.2}</td></tr>\n</tbody></table>\n",
        w.reps.len(),
        w.p50,
        w.p95,
        w.min,
        w.max,
        w.mean,
    ));

    // Grouped phase attribution (sorted descending; shares against the
    // wall-clock median, so these bars agree with the "biggest cost" card and
    // the explicit remainder). Each group unfolds to its raw phases — exact
    // names, total/max µs — so nothing is hidden, just layered.
    if !r.results.phases.is_empty() {
        out.push_str("<h3>where the merge time goes (share of wall-clock p50)</h3>\n");
        for group in &grouped.groups {
            phase_group_bar(
                out,
                group.name,
                group.total_us,
                group.share_pct,
                group.annotation,
                false,
            );
            if !group.members.is_empty() {
                out.push_str(
                    "<details><summary>raw phases</summary>\n<table><thead><tr><th>phase</th>\
                     <th class=\"num\">total_us p50</th><th class=\"num\">total_us max</th>\
                     <th class=\"num\">max_single_us</th></tr></thead><tbody>\n",
                );
                for m in &group.members {
                    out.push_str(&format!(
                        "<tr><td>{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td>\
                         <td class=\"num\">{}</td></tr>\n",
                        esc(&m.name),
                        fmt_thousands(m.total_us_p50),
                        fmt_thousands(m.total_us_max),
                        fmt_thousands(m.max_single_us),
                    ));
                }
                out.push_str("</tbody></table>\n</details>\n");
            }
        }
        if grouped.shows_unattributed() {
            phase_group_bar(
                out,
                crate::groups::UNATTRIBUTED_NAME,
                grouped.unattributed_us,
                grouped.unattributed_share_pct,
                crate::groups::UNATTRIBUTED_ANNOTATION,
                true,
            );
        }
        if grouped.over_attributed {
            out.push_str(&format!(
                "<p class=\"muted\">{}</p>\n",
                esc(crate::groups::OVER_ATTRIBUTION_NOTE)
            ));
        }
    }

    // Storage calls, split by store class the way the record splits them.
    if let Some(sc) = &r.results.storage_calls {
        let table = sum_counts(&sc.table_store);
        let manifest = sum_counts(&sc.manifest_store);
        out.push_str(&format!(
            "<h3>storage calls ({}, merge windows, sum over reps)</h3>\n",
            esc(&sc.layer)
        ));
        out.push_str(&format!(
            "<p><strong>{} requests total</strong> — {} on the table stores, {} on the \
             __manifest store.</p>\n<p class=\"muted\">On S3 every request is one network \
             round trip, so this count predicts cloud latency and request cost.</p>\n",
            fmt_thousands(total_calls(&table) + total_calls(&manifest)),
            fmt_thousands(total_calls(&table)),
            fmt_thousands(total_calls(&manifest)),
        ));
        out.push_str(
            "<table><thead><tr><th>class</th><th class=\"num\">table stores</th>\
             <th class=\"num\">__manifest store</th></tr></thead><tbody>\n",
        );
        for ((class, t), (_, m)) in class_values(&table).iter().zip(class_values(&manifest)) {
            out.push_str(&format!(
                "<tr><td>{class}</td><td class=\"num\">{}</td><td class=\"num\">{}</td></tr>\n",
                fmt_thousands(*t),
                fmt_thousands(m)
            ));
        }
        out.push_str("</tbody></table>\n");
        let bars: Vec<(String, u64, String)> = class_values(&table)
            .iter()
            .zip(class_values(&manifest))
            .filter(|((_, t), (_, m))| *t + m > 0)
            .map(|((class, t), (_, m))| ((*class).to_string(), *t + m, String::new()))
            .collect();
        out.push_str(&svg_bars(&bars));
        let control = sc
            .control_plane
            .iter()
            .fold(ControlCalls::default(), |mut acc, c| {
                acc.read += c.read;
                acc.exists += c.exists;
                acc.list += c.list;
                acc.mutation += c.mutation;
                acc
            });
        out.push_str(&format!(
            "<p class=\"muted\">control plane (engine StorageAdapter): read {} · exists {} · \
             list {} · mutation {}</p>\n",
            fmt_thousands(control.read),
            fmt_thousands(control.exists),
            fmt_thousands(control.list),
            fmt_thousands(control.mutation)
        ));
    } else {
        out.push_str("<p class=\"muted\">storage calls: not recorded (v1/v2 record)</p>\n");
    }

    // Write-path counters.
    let wp = &r.results.write_path;
    if !wp.stage_merge_insert_calls.is_empty() {
        out.push_str(
            "<h3>write path (sum calls/rows)</h3>\n<table><thead><tr>\
             <th class=\"num\">merge_insert</th><th class=\"num\">known_present_update</th>\
             <th class=\"num\">fenced_insert</th><th class=\"num\">strict_preflight</th>\
             </tr></thead><tbody>\n",
        );
        out.push_str(&format!(
            "<tr><td class=\"num\">{}/{}</td><td class=\"num\">{}/{}</td>\
             <td class=\"num\">{}/{}</td><td class=\"num\">{}</td></tr>\n</tbody></table>\n",
            wp.stage_merge_insert_calls.iter().sum::<u64>(),
            wp.stage_merge_insert_rows.iter().sum::<u64>(),
            wp.stage_known_present_update_calls.iter().sum::<u64>(),
            wp.stage_known_present_update_rows.iter().sum::<u64>(),
            wp.stage_fenced_insert_calls.iter().sum::<u64>(),
            wp.stage_fenced_insert_rows.iter().sum::<u64>(),
            wp.strict_insert_preflight_calls.iter().sum::<u64>(),
        ));
    }

    // Full identity, collapsed.
    out.push_str("<details><summary>full identity</summary>\n<table><tbody>\n");
    for (key, value) in identity_rows(file_name, r) {
        out.push_str(&format!(
            "<tr><td>{}</td><td>{}</td></tr>\n",
            esc(&key),
            esc(&value)
        ));
    }
    out.push_str("</tbody></table>\n</details>\n</section>\n");
}

/// The rule-7 delta section for the A/A pairs a records directory holds (two
/// records sharing one point name). Below-floor deltas read "no detected
/// effect", never a small effect. Pairing is by point name, so a before/after
/// pair can land here — differing identities (SUT, machine, backend) are then
/// badged loudly (rule 4), never presented as a silent A/A comparison;
/// anything else about pairing belongs to `diff`.
fn pair_sections(out: &mut String, records: &[(String, RunRecord)], floor: &NoiseFloor) {
    let mut by_point: BTreeMap<&str, Vec<&RunRecord>> = BTreeMap::new();
    for (_, r) in records {
        by_point.entry(&r.point_name).or_default().push(r);
    }
    // Named, not silent: a point with 3+ records cannot pair.
    let unpaired: Vec<(&str, usize)> = by_point
        .iter()
        .filter(|(_, rs)| rs.len() > 2)
        .map(|(point, rs)| (*point, rs.len()))
        .collect();
    let pairs: Vec<(&str, &RunRecord, &RunRecord)> = by_point
        .iter()
        .filter(|(_, rs)| rs.len() == 2)
        .map(|(point, rs)| {
            let (a, b) = (rs[0], rs[1]);
            if a.invocation_unix_seconds <= b.invocation_unix_seconds {
                (*point, a, b)
            } else {
                (*point, b, a)
            }
        })
        .collect();
    if pairs.is_empty() && unpaired.is_empty() {
        return;
    }
    out.push_str(
        "<section>\n<h2>A/A pair deltas (rule 7)</h2>\n<p class=\"muted\">Two records per \
         point at equal spec and SUT; deltas are labeled against the session's noise floor \
         and claim margin.</p>\n",
    );
    for (point, count) in unpaired {
        out.push_str(&format!(
            "<p class=\"muted\">{}: {count} records share this point — pairing needs \
             exactly two, not rendered</p>\n",
            esc(point)
        ));
    }
    for (point, a, b) in pairs {
        // Identity checks mirror diff.rs's rule-4 warnings: a same-point pair
        // with a differing identity is a before/after comparison, badged as
        // such rather than presented as A/A.
        if a.sut.source_commit != b.sut.source_commit {
            out.push_str("<p><span class=\"badge\">not an A/A pair: SUT differs</span></p>\n");
        }
        let (ma, mb) = (&a.machine, &b.machine);
        if ma.cpu_model != mb.cpu_model
            || ma.memory_bytes != mb.memory_bytes
            || ma.os != mb.os
            || ma.arch != mb.arch
        {
            out.push_str(
                "<p><span class=\"badge\">not an A/A pair: machine specifications differ \
                 (rule 4)</span></p>\n",
            );
        }
        let (ea, eb) = (&a.run_spec.environment, &b.run_spec.environment);
        if ea.backend != eb.backend || ea.s3_endpoint != eb.s3_endpoint {
            out.push_str(
                "<p><span class=\"badge\">not an A/A pair: backend identities differ \
                 (rule 4)</span></p>\n",
            );
        }
        // The floor licenses only its own cell — checked against BOTH sides.
        for (side, r) in [("A", a), ("B", b)] {
            if floor.source_commit != r.sut.source_commit || floor.session_id != r.session_id {
                out.push_str(&format!(
                    "<p class=\"badge\">extrapolation: the floor's SUT commit or session \
                     differs from record {side}'s — a floor licenses only its own cell</p>\n",
                ));
            }
        }
        let Some(fp) = floor.points.get(point) else {
            out.push_str(&format!(
                "<p class=\"muted\">{}: no floor entry — deltas not shown; rerun \
                 `run --aa` on this point</p>\n",
                esc(point)
            ));
            continue;
        };
        let margin = floor.default_margin;
        out.push_str(&format!(
            "<h3>{}</h3>\n<p class=\"muted\">floor {:.2}% · claim margin x{margin}</p>\n",
            esc(point),
            fp.pct
        ));
        // The commits stay visible in the header even when equal.
        out.push_str(&format!(
            "<table><thead><tr><th>metric</th><th class=\"num\">A @ {}</th>\
             <th class=\"num\">B @ {}</th><th class=\"num\">delta%</th><th>floor</th>\
             </tr></thead><tbody>\n",
            esc(short_commit(&a.sut.source_commit)),
            esc(short_commit(&b.sut.source_commit)),
        ));
        let (pa, pb) = (a.results.wall_clock_ms.p50, b.results.wall_clock_ms.p50);
        out.push_str(&format!(
            "<tr><td>wall_clock_ms p50</td><td class=\"num\">{pa:.2}</td>\
             <td class=\"num\">{pb:.2}</td><td class=\"num\">{}</td><td>{}</td></tr>\n",
            fmt_delta_pct(pa, pb),
            esc(&floor_label(pa, pb, fp.pct, margin)),
        ));
        for phase_a in &a.results.phases {
            let Some(phase_b) = b.results.phases.iter().find(|p| p.phase == phase_a.phase) else {
                continue;
            };
            let (va, vb) = (phase_a.total_us_p50 as f64, phase_b.total_us_p50 as f64);
            if va == 0.0 && vb == 0.0 {
                continue;
            }
            let label = fp
                .phases
                .get(&phase_a.phase)
                .map_or(String::new(), |phase_floor| {
                    floor_label(va, vb, *phase_floor, margin)
                });
            out.push_str(&format!(
                "<tr><td>phase {} total_us p50</td><td class=\"num\">{va:.0}</td>\
                 <td class=\"num\">{vb:.0}</td><td class=\"num\">{}</td><td>{}</td></tr>\n",
                esc(&phase_a.phase),
                fmt_delta_pct(va, vb),
                esc(&label),
            ));
        }
        out.push_str("</tbody></table>\n");
    }
    out.push_str("</section>\n");
}

const STYLE: &str = "\
:root { --bg: #ffffff; --fg: #1a1f27; --muted: #667085; --accent: #2563eb; \
--card: #f6f8fa; --border: #e2e6ea; }
@media (prefers-color-scheme: dark) {
  :root { --bg: #0f1216; --fg: #e6e8eb; --muted: #98a1ab; --accent: #6b9aef; \
--card: #161b21; --border: #2a313a; }
}
* { box-sizing: border-box; }
body { margin: 0 auto; max-width: 60rem; padding: 2.5rem 1.5rem 4rem; \
background: var(--bg); color: var(--fg); line-height: 1.55; \
font-family: ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif; }
h1 { font-size: 1.6rem; margin: 0 0 0.25rem; }
h1 .product { color: var(--accent); }
h2 { font-size: 1.15rem; margin: 2.5rem 0 0.5rem; border-top: 1px solid var(--border); \
padding-top: 1.5rem; }
h3 { font-size: 0.95rem; margin: 1.25rem 0 0.4rem; }
.muted { color: var(--muted); font-size: 0.85rem; margin: 0.15rem 0 0.75rem; }
code { font-size: 0.85em; background: var(--card); padding: 0.1em 0.35em; \
border-radius: 4px; }
.badge { display: inline-block; font-size: 0.7rem; font-weight: 600; \
color: var(--bg); background: var(--accent); border-radius: 4px; \
padding: 0.05rem 0.4rem; vertical-align: middle; }
table { border-collapse: collapse; font-size: 0.85rem; \
font-variant-numeric: tabular-nums; margin: 0.5rem 0 1rem; }
th, td { text-align: left; padding: 0.3rem 0.9rem 0.3rem 0; \
border-bottom: 1px solid var(--border); vertical-align: top; }
th { color: var(--muted); font-weight: 600; }
th.num, td.num { text-align: right; }
details { margin: 1rem 0; }
summary { cursor: pointer; color: var(--muted); font-size: 0.85rem; }
.cards { display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 0.75rem 0 1rem; }
.card { border: 1px solid var(--border); background: var(--card); border-radius: 8px; \
padding: 0.55rem 0.9rem; min-width: 8.5rem; }
.card .v { font-size: 1.2rem; font-weight: 600; font-variant-numeric: tabular-nums; }
.card .l { color: var(--muted); font-size: 0.72rem; }
.grp { margin: 0.65rem 0 0; }
.grp-head { display: flex; justify-content: space-between; gap: 1rem; \
font-size: 0.9rem; }
.grp-val { color: var(--muted); font-variant-numeric: tabular-nums; \
white-space: nowrap; }
.track { height: 8px; background: var(--card); border: 1px solid var(--border); \
border-radius: 4px; overflow: hidden; margin: 0.25rem 0; }
.fill { height: 100%; background: var(--accent); }
.fill.un { background: var(--muted); }
.grp .muted { margin: 0.1rem 0 0.2rem; }
.grp + details { margin: 0 0 0.6rem; }
svg { max-width: 100%; height: auto; }
svg .bl { font-size: 12px; fill: var(--fg); }
svg .bv { font-size: 12px; fill: var(--muted); }
svg .bar { fill: var(--accent); }
footer { margin-top: 3rem; border-top: 1px solid var(--border); \
padding-top: 1rem; color: var(--muted); font-size: 0.8rem; }
";

/// Render the whole page. Pure (records + floor + timestamp in, HTML out) so
/// the tests exercise it without touching the filesystem.
fn render_report(
    records: &[(String, RunRecord)],
    floor: Option<&NoiseFloor>,
    generated_unix_seconds: u64,
) -> String {
    let mut sessions: Vec<&str> = records.iter().map(|(_, r)| r.session_id.as_str()).collect();
    sessions.sort_unstable();
    sessions.dedup();
    let mut commits: Vec<&str> = records
        .iter()
        .map(|(_, r)| r.sut.source_commit.as_str())
        .collect();
    commits.sort_unstable();
    commits.dedup();

    let mut out = String::with_capacity(64 * 1024);
    out.push_str(&format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>omnigraph-bench report</title>\n<style>\n{STYLE}</style>\n</head>\n<body>\n\
         <header>\n<h1><span class=\"product\">omnigraph-bench</span> run report</h1>\n\
         <p class=\"muted\">generated {} · {} record(s) · session(s): {} · SUT: {}</p>\n\
         </header>\n",
        fmt_utc(generated_unix_seconds),
        records.len(),
        esc(&sessions.join(", ")),
        commits
            .iter()
            .map(|c| commit_html(c))
            .collect::<Vec<_>>()
            .join(", "),
    ));

    // Overview: one row per record.
    out.push_str(
        "<section>\n<h2>overview</h2>\n<table><thead><tr><th>point</th><th>label</th>\
         <th class=\"num\">reps</th><th class=\"num\">p50 ms</th><th class=\"num\">p95 ms</th>\
         <th class=\"num\">storage calls</th></tr></thead><tbody>\n",
    );
    for (_, r) in records {
        let w = &r.results.wall_clock_ms;
        let directional = if w.tail_support == "supported" {
            ""
        } else {
            " <span class=\"badge\">directional</span>"
        };
        let calls = r
            .results
            .storage_calls
            .as_ref()
            .map(|sc| {
                fmt_thousands(
                    total_calls(&sum_counts(&sc.table_store))
                        + total_calls(&sum_counts(&sc.manifest_store)),
                )
            })
            .unwrap_or("n/a".into());
        out.push_str(&format!(
            "<tr><td><a href=\"#r-{}\">{}</a></td><td>{}</td><td class=\"num\">{}</td>\
             <td class=\"num\">{:.2}</td><td class=\"num\">{:.2}{directional}</td>\
             <td class=\"num\">{calls}</td></tr>\n",
            esc(&r.invocation_id),
            esc(&r.point_name),
            esc(r.label.as_deref().unwrap_or("unlabeled")),
            r.run_spec.protocol.repetitions,
            w.p50,
            w.p95,
        ));
    }
    out.push_str("</tbody></table>\n</section>\n");

    if let Some(floor) = floor {
        pair_sections(&mut out, records, floor);
    }

    for (file_name, r) in records {
        per_record_section(&mut out, file_name, r);
    }

    // Footer: versions + record filenames.
    let mut builder_versions: Vec<String> = records
        .iter()
        .filter_map(|(_, r)| r.run_spec.state.builder_version)
        .map(|v| v.to_string())
        .collect();
    builder_versions.sort_unstable();
    builder_versions.dedup();
    out.push_str(&format!(
        "<footer>\nrecord schema v{} · builder v{} · records: {}\n</footer>\n</body>\n</html>\n",
        crate::record::RECORD_VERSION,
        if builder_versions.is_empty() {
            "unknown".to_string()
        } else {
            builder_versions.join(", ")
        },
        esc(&records
            .iter()
            .map(|(f, _)| f.as_str())
            .collect::<Vec<_>>()
            .join(", ")),
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::testkit::sample_record;

    #[test]
    fn utc_formatting_is_civil() {
        // (unix seconds, expected)
        for (secs, expected) in [
            (0, "1970-01-01 00:00:00 UTC"),
            (951_782_400, "2000-02-29 00:00:00 UTC"),
            (1_755_600_000, "2025-08-19 10:40:00 UTC"),
        ] {
            assert_eq!(fmt_utc(secs), expected);
        }
    }

    #[test]
    fn thousands_grouping() {
        for (n, expected) in [
            (0, "0"),
            (999, "999"),
            (1_000, "1,000"),
            (1_234_567, "1,234,567"),
        ] {
            assert_eq!(fmt_thousands(n), expected);
        }
    }

    #[test]
    fn report_names_points_phases_and_identity() {
        let records = vec![
            (
                "a.json".to_string(),
                sample_record("m3-t2-n100-noindex-d1-warm", &[10.0, 11.0]),
            ),
            (
                "b.json".to_string(),
                sample_record("m3-t2-n100-noindex-d50-warm", &[20.0]),
            ),
        ];
        let html = render_report(&records, None, 0);
        assert!(html.contains("omnigraph-bench"));
        assert!(html.contains("m3-t2-n100-noindex-d1-warm"));
        assert!(html.contains("m3-t2-n100-noindex-d50-warm"));
        assert!(html.contains("OuterPrepare"));
        assert!(html.contains("a.json") && html.contains("b.json"));
        // Directional tails and the SUT commit stay visible.
        assert!(html.contains("directional"));
        assert!(html.contains("deadbeefdeadbeef"));
        // Headline cards precede the tables.
        assert!(html.contains("median merge (p50)"));
        assert!(html.contains("biggest cost"));
        // The S3 cost annotation rides with the storage split.
        assert!(html.contains("one network round trip"));
        // Self-containment: no external fetches of any kind.
        for needle in ["http://", "https://", "<script", "@import", "url("] {
            assert!(
                !html.contains(needle),
                "self-contained page must not contain '{needle}'"
            );
        }
    }

    #[test]
    fn phases_render_grouped_with_explicit_unattributed_remainder() {
        // sample_record: wall p50 10 ms, phases 100 µs — the remainder is
        // material and must render as its own bar, never normalized away.
        let record = sample_record("m3-t2-n100-noindex-d1-warm", &[10.0]);
        let html = render_report(&[("a.json".to_string(), record)], None, 0);
        for group in [
            "setup and refresh",
            "discover changes",
            "table walk",
            "validate changes",
            "write merged data",
            "publish new state",
            "crash-safety bookkeeping",
        ] {
            assert!(html.contains(group), "missing group '{group}'");
        }
        assert!(html.contains(crate::groups::UNATTRIBUTED_NAME));
        assert!(html.contains("three-way row walk"));
        // The raw phase stays visible inside the fold.
        assert!(html.contains("OuterPrepare"));

        // Sub-bucket exclusion, visible in the rendered totals: PhysicalPublish
        // 5 ms wrapping KeyedStage 3 ms + KeyedCommit 1 ms on a 5 ms wall —
        // the write group reads 5.0 ms (parent only) and fully explains the
        // wall, so no unattributed bar renders.
        let mut record = sample_record("m3-t2-n100-noindex-d1-warm", &[5.0]);
        record.results.phases = ["PhysicalPublish", "KeyedStage", "KeyedCommit"]
            .iter()
            .zip([5_000_u64, 3_000, 1_000])
            .map(|(name, us)| crate::record::PhaseStats {
                phase: (*name).to_string(),
                total_us_p50: us,
                total_us_max: us,
                max_single_us: us,
                per_rep_total_us: vec![us],
            })
            .collect();
        let html = render_report(&[("a.json".to_string(), record)], None, 0);
        assert!(html.contains("5.0 ms · 100.0%"), "parent-only write total");
        assert!(!html.contains("9.0 ms"), "sub-buckets must not sum on top");
        assert!(!html.contains(crate::groups::UNATTRIBUTED_NAME));
        // The sub-buckets stay visible as raw phases in the fold.
        assert!(html.contains("KeyedStage") && html.contains("KeyedCommit"));
    }

    #[test]
    fn free_form_strings_are_escaped() {
        let mut record = sample_record("m3-t2-n100-noindex-d1-warm", &[10.0]);
        record.label = Some("<script>alert(1)</script>".to_string());
        record.results.phases[0].phase = "Phase<&>\"quote".to_string();
        let html = render_report(&[("x.json".to_string(), record)], None, 0);
        assert!(!html.contains("<script>alert"));
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(html.contains("Phase&lt;&amp;&gt;&quot;quote"));
    }

    #[test]
    fn aa_pairs_get_rule_7_labels() {
        let a = sample_record("m3-t2-n100-noindex-d1-warm", &[100.0]);
        let b = sample_record("m3-t2-n100-noindex-d1-warm", &[100.5]);
        let floor = NoiseFloor {
            floor_version: crate::record::FLOOR_VERSION,
            created_unix_seconds: 0,
            session_id: a.session_id.clone(),
            source_commit: a.sut.source_commit.clone(),
            default_margin: 2.0,
            points: [(
                a.point_name.clone(),
                crate::record::FloorPoint {
                    wall_p50_a_ms: 100.0,
                    wall_p50_b_ms: 100.5,
                    abs_delta_ms: 0.5,
                    pct: 2.0,
                    phases: BTreeMap::new(),
                },
            )]
            .into(),
        };
        let html = render_report(
            &[("a.json".to_string(), a), ("b.json".to_string(), b)],
            Some(&floor),
            0,
        );
        assert!(html.contains("A/A pair deltas"));
        assert!(html.contains("no detected effect"));
        // Commits stay visible in the pair table header even when equal.
        assert!(html.contains("A @ deadbeefd") && html.contains("B @ deadbeefd"));
        assert!(!html.contains("not an A/A pair"));
    }

    fn empty_floor(session_id: &str, source_commit: &str) -> NoiseFloor {
        NoiseFloor {
            floor_version: crate::record::FLOOR_VERSION,
            created_unix_seconds: 0,
            session_id: session_id.to_string(),
            source_commit: source_commit.to_string(),
            default_margin: 2.0,
            points: BTreeMap::new(),
        }
    }

    #[test]
    fn same_point_pairs_with_differing_identity_are_badged_not_aa() {
        let point = "m3-t2-n100-noindex-d1-warm";
        let a = sample_record(point, &[100.0]);
        let mut b = sample_record(point, &[100.5]);
        b.sut.source_commit = "0123456789abcdef".to_string();
        b.machine.cpu_model = "other cpu".to_string();
        let floor = empty_floor(&a.session_id, &a.sut.source_commit);
        let html = render_report(
            &[("a.json".to_string(), a), ("b.json".to_string(), b)],
            Some(&floor),
            0,
        );
        assert!(html.contains("not an A/A pair: SUT differs"));
        assert!(html.contains("machine specifications differ"));
        // The floor cell check covers BOTH sides: B's SUT differs from the
        // floor's, A's does not.
        assert!(html.contains("differs from record B"));
        assert!(!html.contains("differs from record A"));
    }

    #[test]
    fn points_with_more_than_two_records_are_named_not_silently_dropped() {
        let point = "m3-t2-n100-noindex-d1-warm";
        let records: Vec<(String, RunRecord)> = (0..3)
            .map(|i| {
                (
                    format!("r{i}.json"),
                    sample_record(point, &[100.0 + i as f64]),
                )
            })
            .collect();
        let floor = empty_floor("test-session", "deadbeefdeadbeef");
        let html = render_report(&records, Some(&floor), 0);
        assert!(html.contains("3 records share this point"));
    }

    #[test]
    fn over_attributed_records_carry_the_footnote() {
        // Wall p50 0.05 ms vs a 100 µs phase median: shares exceed 100% and
        // the footnote renders instead of a silent cap.
        let record = sample_record("m3-t2-n100-noindex-d1-warm", &[0.05]);
        let html = render_report(&[("a.json".to_string(), record)], None, 0);
        assert!(html.contains(crate::groups::OVER_ATTRIBUTION_NOTE));
        // A fully-attributed record renders no footnote.
        let record = sample_record("m3-t2-n100-noindex-d1-warm", &[10.0]);
        let html = render_report(&[("a.json".to_string(), record)], None, 0);
        assert!(!html.contains(crate::groups::OVER_ATTRIBUTION_NOTE));
    }
}
