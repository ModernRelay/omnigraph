mod data;
mod ops;

use anyhow::Result;
use arrow_array::RecordBatch;

use crate::data::{DataParams, FanoutShape, build, factorized_edge_count};
use crate::ops::{
    OpResult, aggregate_on_list_sql_factorized, aggregate_sql_factorized, aggregate_sql_flat,
    explain_factorized, filter_sql, join_on_list_sql_factorized, join_sql_factorized,
    join_sql_flat, probe_unnest_flatten, project_sql_factorized, project_sql_flat, run_sql,
    sort_sql_factorized, sort_sql_flat,
};

/// One row in the final per-op recommendation matrix.
#[derive(Debug, Clone)]
struct OpRow {
    op_name: &'static str,
    n_src: usize,
    fanout: String,
    factorized: OpResult,
    flat: Option<OpResult>,
}

fn print_table(rows: &[OpRow]) {
    println!("{:-^140}", " factorized-batches results ");
    println!(
        "{:<22} {:>6} {:>14} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {}",
        "op", "n_src", "fanout", "f_ok", "f_rows", "f_time_ms", "x_ok", "x_rows", "x_time_ms",
        "speedup", "recommendation"
    );
    println!("{:-<140}", "");
    for r in rows {
        let f_ok = if r.factorized.accepts { "Y" } else { "N" };
        let f_time = format!("{:.2}", r.factorized.time_ms);
        let (x_ok, x_rows, x_time, speedup) = match &r.flat {
            Some(flat) => {
                let ok = if flat.accepts { "Y" } else { "N" };
                let speedup = if flat.accepts && r.factorized.accepts && flat.time_ms > 0.0 {
                    format!("{:.2}x", flat.time_ms / r.factorized.time_ms.max(1e-3))
                } else {
                    "-".to_string()
                };
                (
                    ok.to_string(),
                    flat.out_rows.to_string(),
                    format!("{:.2}", flat.time_ms),
                    speedup,
                )
            }
            None => ("-".into(), "-".into(), "-".into(), "-".into()),
        };
        let rec = recommendation(r);
        println!(
            "{:<22} {:>6} {:>14} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {}",
            r.op_name, r.n_src, r.fanout, f_ok, r.factorized.out_rows, f_time,
            x_ok, x_rows, x_time, speedup, rec
        );
        if let Some(err) = &r.factorized.error {
            println!("    factorized error: {err}");
        }
        if let Some(flat) = &r.flat {
            if let Some(err) = &flat.error {
                println!("    flat error:       {err}");
            }
        }
    }
}

/// Map (accepts, error class) -> {KEEP_FACTORIZED, FLATTEN_BEFORE, MULTIPLICITY_AWARE_FUTURE}.
fn recommendation(row: &OpRow) -> &'static str {
    if !row.factorized.accepts {
        return "FLATTEN_BEFORE";
    }
    match (&row.flat, row.factorized.out_rows) {
        (Some(flat), f_rows) if flat.accepts => {
            // If factorized emits a superset of rows-of-interest with no
            // multiplicity loss, KEEP. If it changes semantics, demand
            // multiplicity awareness.
            if row.op_name == "aggregate_on_list" || row.op_name == "join_on_list" {
                // Semantically different from a flat baseline.
                "MULTIPLICITY_AWARE_FUTURE"
            } else if f_rows <= flat.out_rows {
                "KEEP_FACTORIZED"
            } else {
                "FLATTEN_BEFORE"
            }
        }
        _ => "KEEP_FACTORIZED",
    }
}

async fn run_one_op(
    op_name: &'static str,
    factorized: RecordBatch,
    flat_for_op: Option<RecordBatch>,
    factorized_sql: &str,
    flat_sql: Option<&str>,
    params: &DataParams,
    fanout_label: String,
) -> OpRow {
    let f = run_sql(op_name, "factorized", factorized, "t", factorized_sql).await;
    let x = match (flat_for_op, flat_sql) {
        (Some(b), Some(sql)) => Some(run_sql(op_name, "flat", b, "t", sql).await),
        _ => None,
    };
    OpRow {
        op_name,
        n_src: params.n_src,
        fanout: fanout_label,
        factorized: f,
        flat: x,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    // Cells from the ticket: 10K source rows × {1, 10, 100, 1000} neighbors,
    // plus a skewed cell.
    let cells: Vec<DataParams> = vec![
        DataParams {
            n_src: 10_000,
            fanout: FanoutShape::Uniform { target: 1 },
            seed: 7,
        },
        DataParams {
            n_src: 10_000,
            fanout: FanoutShape::Uniform { target: 10 },
            seed: 7,
        },
        DataParams {
            n_src: 10_000,
            fanout: FanoutShape::Uniform { target: 100 },
            seed: 7,
        },
        DataParams {
            n_src: 10_000,
            fanout: FanoutShape::Uniform { target: 1000 },
            seed: 7,
        },
        DataParams {
            n_src: 10_000,
            fanout: FanoutShape::Skewed {
                target: 10,
                heavy_fraction: 0.02,
            },
            seed: 7,
        },
    ];

    let mut rows: Vec<OpRow> = Vec::new();
    for params in &cells {
        let (factorized, flat) = build(params);
        let edges = factorized_edge_count(&factorized);
        let label = match params.fanout {
            FanoutShape::Uniform { target } => format!("u={target}"),
            FanoutShape::Skewed { target, heavy_fraction } => format!("s={target}/{heavy_fraction}"),
        };
        println!(
            "\n[cell] n_src={} fanout={} edges={}\n",
            params.n_src, label, edges
        );

        rows.push(
            run_one_op(
                "filter",
                factorized.clone(),
                Some(flat.clone()),
                filter_sql(),
                Some(filter_sql()),
                params,
                label.clone(),
            )
            .await,
        );
        rows.push(
            run_one_op(
                "project",
                factorized.clone(),
                Some(flat.clone()),
                project_sql_factorized(),
                Some(project_sql_flat()),
                params,
                label.clone(),
            )
            .await,
        );
        rows.push(
            run_one_op(
                "sort",
                factorized.clone(),
                Some(flat.clone()),
                sort_sql_factorized(),
                Some(sort_sql_flat()),
                params,
                label.clone(),
            )
            .await,
        );
        rows.push(
            run_one_op(
                "aggregate_scalar",
                factorized.clone(),
                Some(flat.clone()),
                aggregate_sql_factorized(),
                Some(aggregate_sql_flat()),
                params,
                label.clone(),
            )
            .await,
        );
        rows.push(
            run_one_op(
                "aggregate_on_list",
                factorized.clone(),
                None,
                aggregate_on_list_sql_factorized(),
                None,
                params,
                label.clone(),
            )
            .await,
        );
        rows.push(
            run_one_op(
                "join_scalar",
                factorized.clone(),
                Some(flat.clone()),
                join_sql_factorized(),
                Some(join_sql_flat()),
                params,
                label.clone(),
            )
            .await,
        );
        rows.push(
            run_one_op(
                "join_on_list",
                factorized.clone(),
                None,
                join_on_list_sql_factorized(),
                None,
                params,
                label.clone(),
            )
            .await,
        );

        // Calibrate the cost of an explicit `Flatten` (UNNEST) on the
        // factorized batch alone. This is the "flatten cost" column the
        // writeup needs.
        let unnest = probe_unnest_flatten(factorized.clone(), "t").await;
        rows.push(OpRow {
            op_name: "unnest_flatten",
            n_src: params.n_src,
            fanout: label.clone(),
            factorized: unnest,
            flat: None,
        });
    }

    print_table(&rows);

    // Capture one EXPLAIN per representative op to anchor the writeup.
    let probe_params = DataParams {
        n_src: 1000,
        fanout: FanoutShape::Uniform { target: 10 },
        seed: 1,
    };
    let (factorized, _) = build(&probe_params);
    println!("\n[explain] aggregate_scalar (factorized input):");
    println!(
        "{}",
        explain_factorized(factorized.clone(), "t", aggregate_sql_factorized())
            .await
            .unwrap_or_else(|e| format!("<explain failed: {e:#}>"))
    );
    println!("\n[explain] join_scalar (factorized input):");
    println!(
        "{}",
        explain_factorized(factorized.clone(), "t", join_sql_factorized())
            .await
            .unwrap_or_else(|e| format!("<explain failed: {e:#}>"))
    );
    println!("\n[explain] aggregate_on_list (factorized input):");
    println!(
        "{}",
        explain_factorized(factorized.clone(), "t", aggregate_on_list_sql_factorized())
            .await
            .unwrap_or_else(|e| format!("<explain failed: {e:#}>"))
    );
    println!("\n[explain] sort (factorized input):");
    println!(
        "{}",
        explain_factorized(factorized, "t", sort_sql_factorized())
            .await
            .unwrap_or_else(|e| format!("<explain failed: {e:#}>"))
    );

    Ok(())
}
