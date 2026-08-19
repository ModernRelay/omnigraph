//! The `list` subcommand: enumerate the runnable benchmark points (known
//! scenarios x frozen fixtures x their pre-tagged deltas x the three warmth
//! regimes). The point name is the run spec flattened (RFC 0039), so this
//! list is also the naming reference.

use std::path::PathBuf;

use clap::Args;

use crate::fixture::{self, BenchResult};
use crate::record;

#[derive(Debug, Args)]
pub struct ListArgs {
    /// Directory holding frozen fixtures; omit to list only the points
    /// runnable without a fixture (inline base build).
    #[arg(long)]
    fixtures_root: Option<PathBuf>,
}

pub fn list_points(args: ListArgs) -> BenchResult<()> {
    const WARMTHS: [&str; 3] = ["warm", "cold", "post-invalidation"];
    let mut any = false;
    if let Some(root) = &args.fixtures_root {
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(root)
            .map_err(|e| format!("reading {}: {e}", root.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.is_dir())
            .collect();
        dirs.sort();
        for dir in dirs {
            let manifest = match fixture::load_manifest(&dir) {
                Ok(m) => m,
                Err(e) => {
                    println!("(skipping {}: {e})", dir.display());
                    continue;
                }
            };
            let stamped = manifest.validation.is_some();
            let tag = fixture::state_tag(manifest.state.index_level);
            let p = &manifest.profile;
            let non_default_diverged =
                (p.diverged_tables != p.tables.min(4)).then_some(p.diverged_tables);
            let mut scenarios = vec!["m3"];
            if p.diverged_tables == p.tables {
                scenarios.push("m5");
            }
            for scenario in scenarios {
                for &delta in &p.deltas {
                    for warmth in WARMTHS {
                        let name = record::point_name(
                            scenario,
                            p.tables,
                            p.rows,
                            p.payload_bytes,
                            tag,
                            non_default_diverged,
                            delta,
                            warmth,
                            false,
                        );
                        println!(
                            "{name}  [fixture {}{}]",
                            manifest.fixture_name,
                            if stamped {
                                ""
                            } else {
                                ", UNSTAMPED — run refuses it; stamp with `fixture validate`"
                            }
                        );
                        any = true;
                    }
                }
            }
        }
    }
    if !any {
        println!(
            "(no frozen-fixture points{})",
            if args.fixtures_root.is_some() {
                ""
            } else {
                " — pass --fixtures-root to enumerate them"
            }
        );
    }
    println!();
    println!("inline points (no fixture; base built per run):");
    for warmth in WARMTHS {
        println!(
            "  {}",
            record::point_name("m3", 12, 10_000, 64, "noindex", None, 50, warmth, false)
        );
    }
    println!("  (m3 sweeps d=1,50,5000 by default; m5 runs d=50 with every table diverged)");
    println!();
    println!(
        "run a point: omnigraph-bench run --scenario <m3|m5> [--fixture <dir>] \
         --delta <d> --warmth <regime> --out <dir>"
    );
    Ok(())
}
