//! The instrument's data files: tasks, datasets and seeded answers.
//!
//! A task file (`*.task`) and a seed file (`*.seeds`) are sections in the
//! shape of a `.gqt` case: a line `--- <header> [words…]` opens a section and
//! the lines up to the next header are its body. Lines before the first
//! header that start with `#` are comments. The format is read here only;
//! the Python runner asks `omnigraph-gqt-agent tasks` for what it needs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// One section: its header words after `---` and its body.
#[derive(Debug, Clone)]
pub struct Section {
    pub words: Vec<String>,
    pub body: String,
    /// The 1-based line of the header, for messages.
    pub line: usize,
}

/// Split a sectioned file. A line that is neither a comment nor blank before
/// the first header is refused, so a stray edit cannot be dropped silently.
pub fn sections(text: &str) -> Result<Vec<Section>, String> {
    let mut out: Vec<Section> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if let Some(rest) = line.strip_prefix("--- ") {
            out.push(Section {
                words: rest.split_whitespace().map(str::to_string).collect(),
                body: String::new(),
                line: index + 1,
            });
            continue;
        }
        match out.last_mut() {
            Some(section) => {
                section.body.push_str(line);
                section.body.push('\n');
            }
            None if line.trim().is_empty() || line.starts_with('#') => {}
            None => {
                return Err(format!(
                    "line {}: text before the first `--- ` header",
                    index + 1
                ));
            }
        }
    }
    for section in &mut out {
        let trimmed = section.body.trim_end_matches(['\n', ' ']).len();
        section.body.truncate(trimmed);
        if section.words.is_empty() {
            return Err(format!("line {}: a header names no section", section.line));
        }
    }
    Ok(out)
}

/// `key: value` lines of a header section.
fn fields(section: &Section) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for line in section.body.lines().filter(|l| !l.trim().is_empty()) {
        let (key, value) = line.split_once(':').ok_or_else(|| {
            format!(
                "section at line {}: `{line}` is not `key: value`",
                section.line
            )
        })?;
        if out
            .insert(key.trim().to_string(), value.trim().to_string())
            .is_some()
        {
            return Err(format!("section at line {}: `{key}` repeats", section.line));
        }
    }
    Ok(out)
}

/// How the prompt asks for the answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// Exactly one query.
    One,
    /// One query if the language can express the task, else the fewest.
    Fewest,
}

impl Answer {
    pub fn as_str(self) -> &'static str {
        match self {
            Answer::One => "one",
            Answer::Fewest => "fewest",
        }
    }
}

/// A parameter of a later edition-2 step taken from an earlier step's rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub param: String,
    /// 0-based index of the earlier step.
    pub step: usize,
    pub column: String,
}

/// One query of an edition-2 ground truth.
#[derive(Debug, Clone)]
pub struct Step {
    pub query: String,
    /// List parameters: every value of the column, in row order.
    pub bind: Vec<Binding>,
    /// A scalar parameter the step runs once per value of the column.
    pub each: Option<Binding>,
}

/// How an edition-2 ground truth of several reads forms the answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Combine {
    /// The last read's rows (or the check's rows after a write).
    Last,
    /// Every read step's rows, in step order; the columns must agree.
    Concat,
}

/// The rows a ground truth answers on its dataset's seed.
#[derive(Debug, Clone)]
pub struct Expect {
    pub ordered: bool,
    /// `None` when the rows are owned by something else (the engine
    /// catalogue) and are compared live against the ground truth's.
    pub pinned: Option<super::exec::Rows>,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub path: PathBuf,
    /// `tasks` or `corpus`.
    pub set: String,
    pub dataset: String,
    pub kind: String,
    pub answer: Answer,
    pub combine: Combine,
    /// Why the task composes: present for the M0 composition tasks and for
    /// every task whose edition-2 ground truth needs several queries.
    pub composition: Option<String>,
    pub intent: String,
    pub params: serde_json::Map<String, Value>,
    /// Empty when edition 2 cannot express the task.
    pub edition2: Vec<Step>,
    /// The first is canonical; the rest are equally canonical spellings.
    pub edition3: Vec<String>,
    /// The read run after a write, whose rows judge the write.
    pub check: Option<String>,
    pub expect: Option<Expect>,
    pub note: Option<String>,
}

impl Task {
    pub fn is_write(&self) -> bool {
        self.check.is_some()
    }
}

fn binding(word: &str, line: usize) -> Result<Binding, String> {
    // `<param>=<step>:<column>`, steps numbered from 1 as written.
    let (param, source) = word
        .split_once('=')
        .ok_or_else(|| format!("line {line}: `{word}` is not `<param>=<step>:<column>`"))?;
    let (step, column) = source
        .split_once(':')
        .ok_or_else(|| format!("line {line}: `{source}` is not `<step>:<column>`"))?;
    let step: usize = step
        .parse()
        .map_err(|_| format!("line {line}: step `{step}` is not a number"))?;
    if step == 0 {
        return Err(format!("line {line}: steps are numbered from 1"));
    }
    Ok(Binding {
        param: param.to_string(),
        step: step - 1,
        column: column.to_string(),
    })
}

fn step(section: &Section) -> Result<Step, String> {
    let mut bind = Vec::new();
    let mut each = None;
    let mut words = section.words[1..].iter();
    while let Some(word) = words.next() {
        let target = words
            .next()
            .ok_or_else(|| format!("line {}: `{word}` needs a binding", section.line))?;
        match word.as_str() {
            "bind" => bind.push(binding(target, section.line)?),
            "each" if each.is_none() => each = Some(binding(target, section.line)?),
            other => {
                return Err(format!(
                    "line {}: `{other}` is not `bind` or a single `each`",
                    section.line
                ));
            }
        }
    }
    Ok(Step {
        query: section.body.clone(),
        bind,
        each,
    })
}

fn expect(section: &Section) -> Result<Expect, String> {
    let mut ordered = None;
    let mut live = false;
    for word in &section.words[1..] {
        match word.as_str() {
            "ordered" if ordered.is_none() => ordered = Some(true),
            "unordered" if ordered.is_none() => ordered = Some(false),
            "live" => live = true,
            other => {
                return Err(format!(
                    "line {}: unknown expect word `{other}`",
                    section.line
                ));
            }
        }
    }
    let ordered =
        ordered.ok_or_else(|| format!("line {}: say `ordered` or `unordered`", section.line))?;
    if live {
        if !section.body.is_empty() {
            return Err(format!(
                "line {}: a live expect records no rows",
                section.line
            ));
        }
        return Ok(Expect {
            ordered,
            pinned: None,
        });
    }
    Ok(Expect {
        ordered,
        pinned: Some(
            super::exec::Rows::parse(&section.body)
                .map_err(|e| format!("line {}: {e}", section.line))?,
        ),
    })
}

pub fn parse_task(path: &Path, set: &str, text: &str) -> Result<Task, String> {
    let at = |e: String| format!("{}: {e}", path.display());
    let sections = sections(text).map_err(at)?;
    let mut header = None;
    let mut intent = None;
    let mut params = None;
    let mut edition2 = Vec::new();
    let mut edition3 = Vec::new();
    let mut check = None;
    let mut expect_section = None;
    let mut note = None;
    for section in &sections {
        let once = |present: bool| {
            if present {
                Err(at(format!(
                    "line {}: `{}` repeats",
                    section.line, section.words[0]
                )))
            } else {
                Ok(())
            }
        };
        match section.words[0].as_str() {
            "task" => {
                once(header.is_some())?;
                header = Some(fields(section).map_err(at)?);
            }
            "intent" => {
                once(intent.is_some())?;
                intent = Some(section.body.clone());
            }
            "params" => {
                once(params.is_some())?;
                let value: Value = serde_json::from_str(&section.body)
                    .map_err(|e| at(format!("line {}: params: {e}", section.line)))?;
                let Value::Object(map) = value else {
                    return Err(at(format!(
                        "line {}: params must be an object",
                        section.line
                    )));
                };
                params = Some(map);
            }
            "edition2" => edition2.push(step(section).map_err(at)?),
            "edition3" if section.words.len() == 1 => edition3.push(section.body.clone()),
            "check" => {
                once(check.is_some())?;
                check = Some(section.body.clone());
            }
            "expect" => {
                once(expect_section.is_some())?;
                expect_section = Some(expect(section).map_err(at)?);
            }
            "note" => {
                once(note.is_some())?;
                note = Some(section.body.clone());
            }
            other => {
                return Err(at(format!(
                    "line {}: unknown section `{other}`",
                    section.line
                )));
            }
        }
    }
    let mut header = header.ok_or_else(|| at("no `--- task` section".into()))?;
    let mut take = |key: &str| {
        header
            .remove(key)
            .ok_or_else(|| at(format!("the task section has no `{key}`")))
    };
    let id = take("id")?;
    let dataset = take("dataset")?;
    let kind = take("kind")?;
    let answer = match header.remove("answer").as_deref() {
        None | Some("one") => Answer::One,
        Some("fewest") => Answer::Fewest,
        Some(other) => return Err(at(format!("answer `{other}` is not `one` or `fewest`"))),
    };
    let combine = match header.remove("combine").as_deref() {
        None | Some("last") => Combine::Last,
        Some("concat") => Combine::Concat,
        Some(other) => return Err(at(format!("combine `{other}` is not `last` or `concat`"))),
    };
    let composition = header.remove("composition");
    if let Some(extra) = header.keys().next() {
        return Err(at(format!("unknown task field `{extra}`")));
    }
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    if stem != id {
        return Err(at(format!(
            "the file is named `{stem}` but its id is `{id}`"
        )));
    }
    if edition3.is_empty() {
        return Err(at("a task needs an edition-3 ground truth".into()));
    }
    for (index, step) in edition2.iter().enumerate() {
        for binding in step.bind.iter().chain(&step.each) {
            if binding.step >= index {
                return Err(at(format!(
                    "edition-2 step {} binds `{}` from step {}, which does not run before it",
                    index + 1,
                    binding.param,
                    binding.step + 1
                )));
            }
        }
    }
    if edition2.len() > 1 && composition.is_none() {
        return Err(at(
            "an edition-2 ground truth of several queries needs a `composition` line".into(),
        ));
    }
    if !edition2.is_empty() && expect_section.is_none() {
        return Err(at(
            "an edition-2 ground truth needs an `expect` section".into()
        ));
    }
    if edition2.is_empty() && expect_section.is_some() {
        return Err(at(
            "rows are recorded only from an edition-2 ground truth; drop the `expect`".into(),
        ));
    }
    Ok(Task {
        id,
        path: path.to_path_buf(),
        set: set.to_string(),
        dataset,
        kind,
        answer,
        combine,
        composition,
        intent: intent.ok_or_else(|| at("no `--- intent` section".into()))?,
        params: params.unwrap_or_default(),
        edition2,
        edition3,
        check,
        expect: expect_section,
        note,
    })
}

/// A seeded answer: a ground truth with one injected error of one refusal
/// class, repaired from the diagnostic alone.
#[derive(Debug, Clone)]
pub struct Seed {
    pub id: String,
    pub edition: u8,
    pub task: String,
    pub class: String,
    /// The code the front door must refuse it with; `None` where the RFC
    /// names the refusal but no code (the front end allocates it).
    pub code: Option<String>,
    pub text: String,
}

pub fn parse_seeds(path: &Path, edition: u8, text: &str) -> Result<Vec<Seed>, String> {
    let at = |e: String| format!("{}: {e}", path.display());
    let sections = sections(text).map_err(at)?;
    let mut out = Vec::new();
    let mut pending: Option<BTreeMap<String, String>> = None;
    for section in &sections {
        match section.words[0].as_str() {
            "seed" => {
                if pending.is_some() {
                    return Err(at(format!(
                        "line {}: a seed without its text",
                        section.line
                    )));
                }
                pending = Some(fields(section).map_err(at)?);
            }
            "text" => {
                let mut fields = pending
                    .take()
                    .ok_or_else(|| at(format!("line {}: text without a seed", section.line)))?;
                let mut take = |key: &str| {
                    fields.remove(key).ok_or_else(|| {
                        at(format!("line {}: the seed has no `{key}`", section.line))
                    })
                };
                let seed = Seed {
                    id: take("id")?,
                    edition,
                    task: take("task")?,
                    class: take("class")?,
                    code: match take("code")?.as_str() {
                        "-" => None,
                        code => Some(code.to_string()),
                    },
                    text: section.body.clone(),
                };
                if let Some(extra) = fields.keys().next() {
                    return Err(at(format!("unknown seed field `{extra}`")));
                }
                out.push(seed);
            }
            other => {
                return Err(at(format!(
                    "line {}: unknown section `{other}`",
                    section.line
                )));
            }
        }
    }
    if pending.is_some() {
        return Err(at("the last seed has no text".into()));
    }
    Ok(out)
}

/// A dataset: a schema and a seed in GQT's seed format.
#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub schema: String,
    pub seed: String,
}

/// The instrument directory and everything in it.
#[derive(Debug)]
pub struct Instrument {
    pub root: PathBuf,
    pub tasks: Vec<Task>,
    pub datasets: BTreeMap<String, Dataset>,
    pub seeds: Vec<Seed>,
}

impl Instrument {
    pub fn default_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("agent")
    }

    pub fn load(root: &Path) -> Result<Self, String> {
        let read = |path: &Path| {
            std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
        };
        let listed = |dir: &Path| -> Result<Vec<PathBuf>, String> {
            let mut out = std::fs::read_dir(dir)
                .map_err(|e| format!("{}: {e}", dir.display()))?
                .map(|entry| entry.map(|e| e.path()).map_err(|e| e.to_string()))
                .collect::<Result<Vec<_>, _>>()?;
            out.sort();
            Ok(out)
        };
        let mut datasets = BTreeMap::new();
        for dir in listed(&root.join("datasets"))? {
            if !dir.is_dir() {
                continue;
            }
            let name = dir
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or("dataset directory name is not UTF-8")?
                .to_string();
            datasets.insert(
                name.clone(),
                Dataset {
                    name,
                    schema: read(&dir.join("schema.pg"))?,
                    seed: read(&dir.join("seed.jsonl"))?,
                },
            );
        }
        let mut tasks = Vec::new();
        for set in ["tasks", "corpus"] {
            for path in listed(&root.join(set))? {
                if path.extension().and_then(|e| e.to_str()) != Some("task") {
                    return Err(format!(
                        "{}: only `.task` files belong here",
                        path.display()
                    ));
                }
                let task = parse_task(&path, set, &read(&path)?)?;
                if !datasets.contains_key(&task.dataset) {
                    return Err(format!(
                        "{}: no dataset `{}` under datasets/",
                        path.display(),
                        task.dataset
                    ));
                }
                tasks.push(task);
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        for task in &tasks {
            if !seen.insert(task.id.as_str()) {
                return Err(format!("task id `{}` repeats", task.id));
            }
        }
        let mut seeds = Vec::new();
        for edition in [2u8, 3] {
            let path = root.join("seeds").join(format!("edition{edition}.seeds"));
            seeds.extend(parse_seeds(&path, edition, &read(&path)?)?);
        }
        let mut seed_ids = std::collections::BTreeSet::new();
        for seed in &seeds {
            if !seed_ids.insert(seed.id.as_str()) {
                return Err(format!("seed id `{}` repeats", seed.id));
            }
            if !seen.contains(seed.task.as_str()) {
                return Err(format!("seed `{}` names no task `{}`", seed.id, seed.task));
            }
        }
        Ok(Instrument {
            root: root.to_path_buf(),
            tasks,
            datasets,
            seeds,
        })
    }

    pub fn task(&self, id: &str) -> Option<&Task> {
        self.tasks.iter().find(|t| t.id == id)
    }
}
