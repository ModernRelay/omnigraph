//! The agent instrument's language door (`crates/omnigraph-gqt/agent/README.md`).
//! The Python runner calls the agent; everything about the language, from
//! acceptance to rows on the seed, happens here.

#![recursion_limit = "512"]

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use omnigraph_gqt::agent::{self, Edition, Instrument, Judge, card};
use serde_json::{Value, json};

const USAGE: &str = "usage: omnigraph-gqt-agent [--root <agent dir>] <command>
commands:
  tasks                   every task, one JSON object per line
  seeds --edition <2|3>   the seeded answers of an edition
  capabilities            the editions this build can judge
  judge --edition <2|3>   judge {\"id\", \"task\", \"text\"} lines from stdin
  cards --check|--write   the edition-2 card regenerated from skills/omnigraph, and both card digests
  record --check|--write  the edition-2 ground truths' rows on their seeds";

struct Args {
    root: PathBuf,
    command: String,
    edition: Option<Edition>,
    write: Option<bool>,
}

fn parse() -> Result<Args, String> {
    let mut root = Instrument::default_root();
    let mut command = None;
    let mut edition = None;
    let mut write = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => root = PathBuf::from(args.next().ok_or("--root needs a directory")?),
            "--edition" => {
                edition = Some(Edition::parse(
                    &args.next().ok_or("--edition needs 2 or 3")?,
                )?)
            }
            "--check" => write = Some(false),
            "--write" => write = Some(true),
            "-h" | "--help" => return Err(USAGE.into()),
            other if command.is_none() && !other.starts_with('-') => {
                command = Some(other.to_string())
            }
            other => return Err(format!("unexpected argument `{other}`\n{USAGE}")),
        }
    }
    Ok(Args {
        root,
        command: command.ok_or(USAGE)?,
        edition,
        write,
    })
}

fn print(value: &Value) {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{value}").expect("stdout");
    out.flush().expect("stdout");
}

fn repo_root() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

async fn run(args: Args) -> Result<(), String> {
    match args.command.as_str() {
        "capabilities" => {
            let pending: serde_json::Map<String, Value> = [Edition::Two, Edition::Three]
                .into_iter()
                .filter(|e| !agent::available(*e))
                .map(|e| (e.number().to_string(), json!(agent::EDITION3_PENDING)))
                .collect();
            let editions: Vec<u8> = [Edition::Two, Edition::Three]
                .into_iter()
                .filter(|e| agent::available(*e))
                .map(Edition::number)
                .collect();
            print(&json!({"editions": editions, "pending": pending}));
            Ok(())
        }
        "tasks" => {
            let instrument = Instrument::load(&args.root)?;
            for task in &instrument.tasks {
                print(&json!({
                    "id": task.id,
                    "set": task.set,
                    "dataset": task.dataset,
                    "kind": task.kind,
                    "answer": task.answer.as_str(),
                    "composition": task.composition,
                    "intent": task.intent,
                    "write": task.is_write(),
                    "schema": instrument.datasets[&task.dataset].schema,
                    "edition2_queries": task.edition2.len(),
                    "edition3_truths": task.edition3.len(),
                }));
            }
            Ok(())
        }
        "seeds" => {
            let edition = args.edition.ok_or("seeds needs --edition")?;
            let instrument = Instrument::load(&args.root)?;
            for seed in instrument
                .seeds
                .iter()
                .filter(|s| s.edition == edition.number())
            {
                print(&json!({
                    "id": seed.id,
                    "task": seed.task,
                    "class": seed.class,
                    "code": seed.code,
                    "text": seed.text,
                }));
            }
            Ok(())
        }
        "judge" => {
            let edition = args.edition.ok_or("judge needs --edition")?;
            let instrument = Instrument::load(&args.root)?;
            let mut judge = Judge::new(&instrument);
            for line in std::io::stdin().lock().lines() {
                let line = line.map_err(|e| e.to_string())?;
                if line.trim().is_empty() {
                    continue;
                }
                let job: Value = serde_json::from_str(&line).map_err(|e| format!("job: {e}"))?;
                let (Some(task), Some(text)) = (job["task"].as_str(), job["text"].as_str()) else {
                    return Err(format!("a job needs `task` and `text`: {line}"));
                };
                let mut verdict = judge.judge(task, edition, text).await;
                verdict["id"] = job["id"].clone();
                print(&verdict);
            }
            Ok(())
        }
        "cards" => {
            let write = args.write.ok_or("cards needs --check or --write")?;
            let generated = card::edition2(&card::skill_root(&repo_root()))?;
            let path = card::card_path(&args.root, 2);
            if write {
                std::fs::write(&path, &generated).map_err(|e| e.to_string())?;
                let mut manifest = card::Manifest::read(&args.root)?;
                for (edition, digest) in &mut manifest.cards {
                    let bytes = std::fs::read(card::card_path(&args.root, *edition))
                        .map_err(|e| e.to_string())?;
                    *digest = card::sha256(&bytes);
                }
                std::fs::write(args.root.join(card::MANIFEST), manifest.render())
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
            let on_disk = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
            let mut problems = card::Manifest::read(&args.root)?.verify(&args.root);
            if on_disk != generated {
                problems.push(format!(
                    "{} differs from the card generated from skills/omnigraph; run `cards --write`",
                    path.display()
                ));
            }
            if problems.is_empty() {
                Ok(())
            } else {
                Err(problems.join("\n"))
            }
        }
        "record" => {
            let write = args.write.ok_or("record needs --check or --write")?;
            let instrument = Instrument::load(&args.root)?;
            let mut stale = Vec::new();
            for task in instrument.tasks.iter().filter(|t| !t.edition2.is_empty()) {
                let Some(expect) = &task.expect else { continue };
                let Some(pinned) = &expect.pinned else {
                    continue;
                };
                let graph = agent::Graph::open(&instrument.datasets[&task.dataset]).await?;
                let rows = agent::run_truth(&graph, task, &task.edition2)
                    .await
                    .map_err(|e| format!("{}: {e}", task.id))?;
                if rows.matches(pinned, expect.ordered) {
                    continue;
                }
                stale.push(task.id.clone());
                if write {
                    rewrite_expect(&task.path, &rows.recorded(expect.ordered).render())?;
                }
            }
            if stale.is_empty() || write {
                for id in &stale {
                    eprintln!("recorded {id}");
                }
                Ok(())
            } else {
                Err(format!("recorded rows differ for: {}", stale.join(", ")))
            }
        }
        other => Err(format!("unknown command `{other}`\n{USAGE}")),
    }
}

/// Replace the body of the `--- expect` section, keeping its header.
fn rewrite_expect(path: &std::path::Path, body: &str) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut out = String::new();
    let mut skipping = false;
    for line in text.split_inclusive('\n') {
        if line.starts_with("--- ") {
            if skipping {
                out.push('\n');
            }
            skipping = line.starts_with("--- expect");
            out.push_str(line);
            if skipping {
                out.push_str(body);
            }
            continue;
        }
        if !skipping {
            out.push_str(line);
        }
    }
    std::fs::write(path, out).map_err(|e| e.to_string())
}

fn main() -> ExitCode {
    let args = match parse() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Unoptimized engine futures nest deeper than a default 2 MiB stack.
        .thread_stack_size(16 << 20)
        .build()
        .expect("tokio runtime");
    match runtime.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}
