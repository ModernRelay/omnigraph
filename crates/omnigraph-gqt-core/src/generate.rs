use std::collections::BTreeMap;

use omnigraph::Session;
use omnigraph::loader::LoadMode;
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

const MAX_ROWS: u64 = 10_000_000;
const MAX_COMMITS: u64 = 100_000;
const MAX_BATCH_ROWS: u64 = 4096;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_GENERATED_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_ZIPF_POPULATION: u64 = 1_000_000;

#[derive(Debug)]
pub enum Seed {
    Inline(String),
    Generated(Generated),
}

#[derive(Debug)]
pub struct Generated {
    seed: u64,
    tables: Vec<Table>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Recipe {
    tables: Vec<Table>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    kind: RowKind,
    name: String,
    rows: u64,
    commits: u64,
    #[serde(default)]
    batch_rows: Option<u64>,
    #[serde(default)]
    start: u64,
    #[serde(default)]
    wrap: Option<u64>,
    #[serde(default)]
    id: Option<Column>,
    #[serde(default)]
    from: Option<Column>,
    #[serde(default)]
    to: Option<Column>,
    columns: BTreeMap<String, Column>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RowKind {
    Node,
    Edge,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum Column {
    Literal {
        value: Value,
    },
    Repeat {
        text: String,
        count: usize,
    },
    Ordinal {
        start: i64,
        step: i64,
    },
    Key {
        prefix: String,
        width: usize,
        #[serde(default)]
        modulo: Option<u64>,
    },
    Modulo {
        modulus: u64,
    },
    Ranges {
        ranges: Vec<Range>,
        fallback: Value,
    },
    Vector {
        dimensions: usize,
    },
    Endpoint {
        prefix: String,
        width: usize,
        population: u64,
        distribution: Distribution,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Range {
    end: u64,
    value: Value,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum Distribution {
    Ordinal,
    Uniform,
    Zipf { exponent: f64 },
}

impl Seed {
    pub(crate) fn parse(header: &str, body: String) -> Result<Self, String> {
        if header == "seed" {
            return Ok(Self::Inline(
                body.lines()
                    .filter(|line| !line.trim().is_empty())
                    .collect::<Vec<_>>()
                    .join("\n"),
            ));
        }
        let args = header.strip_prefix("seed ").ok_or("invalid seed header")?;
        let seed = parse_arguments(args, false)?.0;
        Generated::parse(seed, &body).map(Self::Generated)
    }

    pub async fn load(&self, session: &Session) -> Result<(), String> {
        match self {
            Self::Inline(text) if text.trim().is_empty() => Ok(()),
            Self::Inline(text) => session
                .load_jsonl(text, LoadMode::Overwrite)
                .await
                .map(|_| ())
                .map_err(|error| format!("seed load failed: {error}")),
            Self::Generated(generated) => generated.load(session, "main", LoadMode::Append).await,
        }
    }
}

pub(crate) fn parse_arguments(
    arguments: &str,
    is_load: bool,
) -> Result<(u64, LoadMode, String), String> {
    let tokens = arguments.split_whitespace().collect::<Vec<_>>();
    if !tokens.len().is_multiple_of(2) {
        return Err("generator header requires key: value pairs".into());
    }
    let mut values = BTreeMap::new();
    for pair in tokens.chunks_exact(2) {
        if !matches!(pair[0], "generate:" | "seed:" | "mode:" | "branch:")
            || (!is_load && matches!(pair[0], "mode:" | "branch:"))
            || values.insert(pair[0], pair[1]).is_some()
        {
            return Err(format!(
                "unknown or repeated generator argument {:?}",
                pair[0]
            ));
        }
    }
    if values.get("generate:") != Some(&"v1") {
        return Err("generated rows require `generate: v1`".into());
    }
    let seed = values
        .get("seed:")
        .ok_or("generated rows require an explicit `seed: <u64>`")?
        .parse::<u64>()
        .map_err(|_| "generator seed must be an unsigned 64-bit integer")?;
    let mode = match values.get("mode:").copied() {
        Some("append") => LoadMode::Append,
        Some("merge") => LoadMode::Merge,
        None if !is_load => LoadMode::Append,
        _ => return Err("a generated load requires `mode: append` or `mode: merge`".into()),
    };
    let branch = values.get("branch:").copied().unwrap_or("main");
    if branch.is_empty() {
        return Err("generated load branch must not be empty".into());
    }
    Ok((seed, mode, branch.to_string()))
}

impl Generated {
    pub fn call_count(&self) -> u64 {
        self.tables.iter().map(|table| table.commits).sum()
    }

    pub(crate) fn single_batch(&self) -> Result<Option<String>, String> {
        if self.call_count() != 1 {
            return Ok(None);
        }
        let table = self
            .tables
            .iter()
            .find(|table| table.commits == 1)
            .ok_or("single-call generator has no nonempty table")?;
        self.batch(table, &table.distributions()?, 0, table.rows)
            .map(Some)
    }

    pub(crate) fn parse(seed: u64, body: &str) -> Result<Self, String> {
        let recipe: Recipe = crate::runner_config::yaml(body, "generated recipe")?;
        if recipe.tables.len() > 256 {
            return Err("generated rows admit at most 256 table recipes".into());
        }
        let mut rows = 0u64;
        let mut commits = 0u64;
        let mut bytes = 0u64;
        let mut zipf_entries = 0u64;
        for table in &recipe.tables {
            let row_bytes = table.validate()?;
            for (_, column) in table.fields() {
                if let Column::Endpoint {
                    population,
                    distribution: Distribution::Zipf { .. },
                    ..
                } = column
                {
                    zipf_entries = zipf_entries
                        .checked_add(*population)
                        .ok_or("zipf table size overflow")?;
                }
            }
            rows = rows
                .checked_add(table.rows)
                .ok_or("generated row count overflow")?;
            commits = commits
                .checked_add(table.commits)
                .ok_or("generated commit count overflow")?;
            bytes = bytes
                .checked_add(
                    table
                        .rows
                        .checked_mul(row_bytes)
                        .ok_or("generated byte count overflow")?,
                )
                .ok_or("generated byte count overflow")?;
        }
        if zipf_entries > 8_000_000 {
            return Err("generated zipf tables exceed 8000000 entries".into());
        }
        if rows > MAX_ROWS || commits > MAX_COMMITS || bytes > MAX_GENERATED_BYTES {
            return Err(format!(
                "generated rows exceed limits: rows {rows}/{MAX_ROWS}, commits {commits}/{MAX_COMMITS}, bytes {bytes}/{MAX_GENERATED_BYTES}"
            ));
        }
        Ok(Self {
            seed,
            tables: recipe.tables,
        })
    }

    pub async fn load(
        &self,
        session: &Session,
        branch: &str,
        mode: LoadMode,
    ) -> Result<(), String> {
        self.load_observed(session, branch, mode, |_| {}).await
    }

    pub(crate) async fn load_observed(
        &self,
        session: &Session,
        branch: &str,
        mode: LoadMode,
        observe: impl Fn(&omnigraph::error::OmniError),
    ) -> Result<(), String> {
        for table in &self.tables {
            let distributions = table.distributions()?;
            for std::ops::Range { start, end } in table.batches() {
                let text = self.batch(table, &distributions, start, end)?;
                session
                    .load(branch, &text, mode)
                    .await
                    .inspect_err(&observe)
                    .map_err(|error| {
                        format!(
                            "generated load {} rows {start}..{end} on {branch}: {error}",
                            table.name
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn batch(
        &self,
        table: &Table,
        distributions: &BTreeMap<String, Vec<f64>>,
        start: u64,
        end: u64,
    ) -> Result<String, String> {
        let mut output = Vec::new();
        for index in start..end {
            let ordinal = table.ordinal(index)?;
            let value = |column: &Column, name: &str| {
                column.value(
                    self.seed,
                    &table.name,
                    name,
                    ordinal,
                    distributions.get(name).map(Vec::as_slice),
                )
            };
            let mut object = Map::new();
            object.insert(
                match table.kind {
                    RowKind::Node => "type",
                    RowKind::Edge => "edge",
                }
                .into(),
                Value::String(table.name.clone()),
            );
            for (key, column) in [("id", &table.id), ("from", &table.from), ("to", &table.to)] {
                if let Some(column) = column {
                    object.insert(key.into(), value(column, &format!("row.{key}"))?);
                }
            }
            let data = table
                .columns
                .iter()
                .map(|(name, column)| {
                    value(column, &format!("data.{name}")).map(|value| (name.clone(), value))
                })
                .collect::<Result<Map<_, _>, _>>()?;
            object.insert("data".into(), Value::Object(data));
            serde_json::to_writer(&mut output, &object).map_err(|error| error.to_string())?;
            output.push(b'\n');
            if output.len() > MAX_BATCH_BYTES {
                return Err("generated batch exceeds the 16 MiB limit".into());
            }
        }
        String::from_utf8(output).map_err(|error| error.to_string())
    }
}

impl Table {
    fn batches(&self) -> impl Iterator<Item = std::ops::Range<u64>> + '_ {
        (0..self.commits).scan(0u64, |start, commit| {
            let rows = self.batch_rows.unwrap_or_else(|| {
                self.rows / self.commits + u64::from(commit < self.rows % self.commits)
            });
            let end = start.saturating_add(rows).min(self.rows);
            let range = *start..end;
            *start = end;
            Some(range)
        })
    }

    fn batch_rows(&self) -> Result<u64, String> {
        if self
            .batch_rows
            .is_some_and(|rows| rows == 0 || rows > MAX_BATCH_ROWS)
        {
            return Err("generated batch_rows must be in 1..=4096".into());
        }
        if self.rows == 0 && self.commits == 0 {
            return Ok(1);
        }
        if self.commits == 0 || self.commits > self.rows {
            return Err("generated commits must be in 1..=rows, or both counts zero".into());
        }
        let batch = self.batch_rows.unwrap_or(self.rows.div_ceil(self.commits));
        if batch == 0
            || batch > MAX_BATCH_ROWS
            || self
                .batch_rows
                .is_some_and(|_| self.rows.div_ceil(batch) != self.commits)
        {
            return Err("generated batch_rows must be in 1..=4096 and produce exactly commits nonempty batches".into());
        }
        Ok(batch)
    }

    fn ordinal(&self, index: u64) -> Result<u64, String> {
        let ordinal = self
            .start
            .checked_add(index)
            .ok_or("generated ordinal overflow")?;
        Ok(self.wrap.map_or(ordinal, |wrap| ordinal % wrap))
    }

    fn fields(&self) -> impl Iterator<Item = (String, &Column)> {
        [
            ("id", self.id.as_ref()),
            ("from", self.from.as_ref()),
            ("to", self.to.as_ref()),
        ]
        .into_iter()
        .filter_map(|(name, column)| column.map(|column| (format!("row.{name}"), column)))
        .chain(
            self.columns
                .iter()
                .map(|(name, column)| (format!("data.{name}"), column)),
        )
    }

    fn validate(&self) -> Result<u64, String> {
        if self.name.is_empty() || self.name.len() > 256 || self.columns.len() > 256 {
            return Err(
                "generated table names require 1..=256 bytes and at most 256 columns".into(),
            );
        }
        match self.kind {
            RowKind::Node if self.from.is_some() || self.to.is_some() => {
                return Err("a generated node cannot carry edge endpoints".into());
            }
            RowKind::Edge if self.from.is_none() || self.to.is_none() => {
                return Err("a generated edge requires from and to".into());
            }
            RowKind::Node | RowKind::Edge => {}
        }
        for (name, column) in [("id", &self.id), ("from", &self.from), ("to", &self.to)] {
            if column.as_ref().is_some_and(|column| !column.is_string()) {
                return Err(format!("generated row {name} must produce strings"));
            }
        }
        if self.wrap == Some(0) {
            return Err("generated ordinal wrap must be positive".into());
        }
        self.start
            .checked_add(self.rows.saturating_sub(1))
            .ok_or("generated ordinal overflow")?;
        let max = self
            .wrap
            .map_or_else(|| self.start + self.rows.saturating_sub(1), |wrap| wrap - 1);
        let mut bytes =
            64u64 + u64::try_from(self.name.len()).map_err(|_| "table name too large")? * 6;
        for (name, column) in self.fields() {
            let bound = column.validate(max)?;
            let key_bytes = u64::try_from(name.len())
                .map_err(|_| "column name too large")?
                .checked_mul(6)
                .ok_or("column name too large")?;
            bytes = bytes
                .checked_add(bound)
                .and_then(|bytes| bytes.checked_add(key_bytes + 8))
                .ok_or("generated row size overflow")?;
        }
        let batch_bytes = bytes
            .checked_mul(self.batch_rows()?)
            .ok_or("generated batch size overflow")?;
        if batch_bytes > u64::try_from(MAX_BATCH_BYTES).map_err(|_| "invalid batch limit")? {
            return Err("generated batch byte bound exceeds 16 MiB; reduce batch_rows".into());
        }
        Ok(bytes)
    }

    fn distributions(&self) -> Result<BTreeMap<String, Vec<f64>>, String> {
        self.fields()
            .filter_map(|(name, column)| {
                let Column::Endpoint {
                    population,
                    distribution: Distribution::Zipf { exponent },
                    ..
                } = column
                else {
                    return None;
                };
                Some(zipf_cdf(*population, *exponent).map(|cdf| (name.to_string(), cdf)))
            })
            .collect()
    }
}

impl Column {
    fn is_string(&self) -> bool {
        match self {
            Self::Literal { value } => value.is_string(),
            Self::Ranges { ranges, fallback } => {
                fallback.is_string() && ranges.iter().all(|range| range.value.is_string())
            }
            Self::Repeat { .. } | Self::Key { .. } | Self::Endpoint { .. } => true,
            Self::Ordinal { .. } | Self::Modulo { .. } | Self::Vector { .. } => false,
        }
    }

    fn validate(&self, max_ordinal: u64) -> Result<u64, String> {
        let encoded = |value: &Value| {
            serde_json::to_vec(value)
                .map_err(|error| error.to_string())
                .and_then(|value| {
                    u64::try_from(value.len()).map_err(|_| "literal too large".into())
                })
        };
        let key_bound = |prefix: &str, width: usize| {
            if width > 128 {
                return Err("generated key width exceeds 128".into());
            }
            u64::try_from(prefix.len())
                .ok()
                .and_then(|n| n.checked_mul(6))
                .and_then(|n| n.checked_add(u64::try_from(width.max(20)).ok()? + 2))
                .ok_or_else(|| "generated key size overflow".to_string())
        };
        match self {
            Self::Literal { value } => encoded(value),
            Self::Repeat { text, count } => u64::try_from(text.len())
                .ok()
                .and_then(|n| n.checked_mul(u64::try_from(*count).ok()?))
                .and_then(|n| n.checked_mul(6))
                .and_then(|n| n.checked_add(2))
                .ok_or_else(|| "generated repeated string size overflow".into()),
            Self::Ordinal { start, step } => {
                affine(*start, *step, max_ordinal)?;
                Ok(21)
            }
            Self::Key {
                prefix,
                width,
                modulo,
            } => {
                if *modulo == Some(0) {
                    return Err("generated key modulo must be positive".into());
                }
                key_bound(prefix, *width)
            }
            Self::Modulo { modulus } => {
                if *modulus == 0 {
                    return Err("generated modulus must be positive".into());
                }
                Ok(20)
            }
            Self::Ranges { ranges, fallback } => {
                if ranges.len() > 256 || ranges.windows(2).any(|r| r[0].end >= r[1].end) {
                    return Err(
                        "generated ranges require at most 256 strictly increasing ends".into(),
                    );
                }
                ranges.iter().try_fold(encoded(fallback)?, |max, range| {
                    encoded(&range.value).map(|bytes| max.max(bytes))
                })
            }
            Self::Vector { dimensions } => {
                if !(1..=4096).contains(dimensions) {
                    return Err("generated vector dimensions must be in 1..=4096".into());
                }
                Ok(u64::try_from(*dimensions).map_err(|_| "vector dimension overflow")? * 32 + 2)
            }
            Self::Endpoint {
                prefix,
                width,
                population,
                distribution,
            } => {
                if *population == 0 {
                    return Err("generated endpoint population must be positive".into());
                }
                if let Distribution::Zipf { exponent } = distribution {
                    if *population > MAX_ZIPF_POPULATION
                        || !exponent.is_finite()
                        || *exponent <= 0.0
                        || *exponent > 16.0
                    {
                        return Err(
                            "zipf requires population 1..=1000000 and finite exponent in (0, 16]"
                                .into(),
                        );
                    }
                }
                key_bound(prefix, *width)
            }
        }
    }

    fn value(
        &self,
        seed: u64,
        table: &str,
        column: &str,
        ordinal: u64,
        cdf: Option<&[f64]>,
    ) -> Result<Value, String> {
        let word = |lane, attempt| random_word(seed, table, column, ordinal, lane, attempt);
        match self {
            Self::Literal { value } => Ok(value.clone()),
            Self::Repeat { text, count } => Ok(Value::String(text.repeat(*count))),
            Self::Ordinal { start, step } => affine(*start, *step, ordinal).map(Value::from),
            Self::Key {
                prefix,
                width,
                modulo,
            } => {
                let ordinal = modulo.map_or(ordinal, |modulo| ordinal % modulo);
                Ok(Value::String(format!("{prefix}{ordinal:0width$}")))
            }
            Self::Modulo { modulus } => Ok(Value::from(ordinal % modulus)),
            Self::Ranges { ranges, fallback } => Ok(ranges
                .iter()
                .find(|range| ordinal < range.end)
                .map_or(fallback, |range| &range.value)
                .clone()),
            Self::Vector { dimensions } => (0..*dimensions)
                .map(|dimension| {
                    let lane = u64::try_from(dimension).map_err(|_| "vector lane overflow")?;
                    let bits =
                        u32::try_from(word(lane, 0) >> 40).map_err(|_| "vector bits overflow")?;
                    Ok(Value::from(f64::from(bits) / 16_777_216.0))
                })
                .collect::<Result<Vec<_>, String>>()
                .map(Value::Array),
            Self::Endpoint {
                prefix,
                width,
                population,
                distribution,
            } => {
                let selected = match distribution {
                    Distribution::Ordinal => ordinal % population,
                    Distribution::Uniform => {
                        let threshold = population.wrapping_neg() % population;
                        let mut selected = None;
                        for attempt in 0..128 {
                            let random = word(0, attempt);
                            if random >= threshold {
                                selected = Some(random % population);
                                break;
                            }
                        }
                        selected.ok_or("uniform rejection sampling exceeded 128 draws")?
                    }
                    Distribution::Zipf { exponent: _ } => {
                        let cdf = cdf.ok_or("zipf distribution was not prepared")?;
                        let bits = word(0, 0) >> 11;
                        let u = (bits as f64) / 9_007_199_254_740_992.0;
                        u64::try_from(cdf.partition_point(|&end| end <= u).min(cdf.len() - 1))
                            .map_err(|_| "zipf index overflow")?
                    }
                };
                Ok(Value::String(format!("{prefix}{selected:0width$}")))
            }
        }
    }
}

fn affine(start: i64, step: i64, ordinal: u64) -> Result<i64, String> {
    i128::from(step)
        .checked_mul(i128::from(ordinal))
        .and_then(|v| v.checked_add(i128::from(start)))
        .and_then(|v| i64::try_from(v).ok())
        .ok_or_else(|| "generated ordinal exceeds signed 64-bit range".into())
}

fn random_word(seed: u64, table: &str, column: &str, ordinal: u64, lane: u64, attempt: u64) -> u64 {
    let mut hash = Sha256::new();
    hash.update(b"omnigraph-gqt-generate-v1\0");
    for value in [seed, ordinal, lane, attempt] {
        hash.update(value.to_le_bytes());
    }
    for value in [table, column] {
        hash.update(
            u64::try_from(value.len())
                .expect("input lengths fit the generator's u64 bounds")
                .to_le_bytes(),
        );
        hash.update(value.as_bytes());
    }
    let digest = hash.finalize();
    u64::from_le_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 output contains eight bytes"),
    )
}

fn zipf_cdf(population: u64, exponent: f64) -> Result<Vec<f64>, String> {
    let mut cdf =
        Vec::with_capacity(usize::try_from(population).map_err(|_| "zipf population overflow")?);
    let mut total = 0.0;
    for rank in 1..=population {
        total += libm::pow(rank as f64, -exponent);
        cdf.push(total);
    }
    for bound in &mut cdf {
        *bound /= total;
    }
    Ok(cdf)
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECIPE: &str = r#"tables:
  - kind: node
    name: Person
    rows: 5
    commits: 2
    columns:
      name: {kind: key, prefix: person-, width: 3}
      ordinal: {kind: ordinal, start: -1, step: -1}
      bucket: {kind: modulo, modulus: 2}
      embedding: {kind: vector, dimensions: 3}
      uniform: {kind: endpoint, prefix: p, width: 2, population: 7, distribution: {kind: uniform}}
      skew: {kind: endpoint, prefix: p, width: 2, population: 7, distribution: {kind: zipf, exponent: 1.25}}
"#;

    #[test]
    fn random_stream_is_pinned_and_values_do_not_depend_on_batching() {
        assert_eq!(
            random_word(42, "Person", "data.embedding", 7, 2, 0),
            5_200_264_969_650_745_713
        );
        let generated = Generated::parse(42, RECIPE).unwrap();
        let table = &generated.tables[0];
        let distributions = table.distributions().unwrap();
        let full = generated.batch(table, &distributions, 0, 5).unwrap();
        let chunks = generated.batch(table, &distributions, 0, 2).unwrap()
            + &generated.batch(table, &distributions, 2, 5).unwrap();
        assert_eq!(full, chunks);
        let another = Generated::parse(43, RECIPE).unwrap();
        assert_ne!(full, another.batch(table, &distributions, 0, 5).unwrap());
        let rows: Vec<Value> = full
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows[0]["data"]["name"], "person-000");
        assert_eq!(rows[4]["data"]["ordinal"], -5);
        assert_eq!(rows[3]["data"]["bucket"], 1);
        assert_eq!(
            rows[0]["data"]["embedding"],
            serde_json::json!([0.05705291032791138, 0.5826990008354187, 0.6762937307357788])
        );
        assert_eq!(
            rows[4]["data"]["embedding"],
            serde_json::json!([0.27964383363723755, 0.6454817652702332, 0.5683015584945679])
        );
        assert_eq!(
            rows.iter()
                .map(|row| row["data"]["uniform"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["p04", "p00", "p05", "p03", "p02"]
        );
        assert_eq!(
            rows.iter()
                .map(|row| row["data"]["skew"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["p00", "p02", "p01", "p00", "p05"]
        );
        for row in rows {
            let data = &row["data"];
            for value in data["embedding"].as_array().unwrap() {
                assert!((0.0..1.0).contains(&value.as_f64().unwrap()));
            }
            for column in ["uniform", "skew"] {
                let ordinal = data[column].as_str().unwrap()[1..].parse::<u64>().unwrap();
                assert!(ordinal < 7);
            }
        }
    }

    #[test]
    fn generation_refuses_invalid_or_unbounded_recipes_before_loading() {
        for (from, to) in [
            ("commits: 2", "commits: 0"),
            ("commits: 2", "commits: 6"),
            ("rows: 5", "rows: 10000001"),
            ("modulus: 2", "modulus: 0"),
            ("dimensions: 3", "dimensions: 0"),
            ("dimensions: 3", "dimensions: 4097"),
            ("population: 7", "population: 0"),
            ("exponent: 1.25", "exponent: .nan"),
            ("exponent: 1.25", "exponent: -1"),
            ("exponent: 1.25", "exponent: 17"),
            ("step: -1", "step: 9223372036854775807"),
            ("kind: node", "kind: edge"),
            ("width: 3", "width: 129"),
            ("name: Person", "name: Person\n    wrap: 0"),
            ("name: Person", "name: &table Person"),
            ("name: Person", "name: Person\n    unsupported: true"),
            ("commits: 2", "commits: 2\n    batch_rows: 1"),
        ] {
            assert!(
                Generated::parse(0, &RECIPE.replace(from, to)).is_err(),
                "{from} -> {to}"
            );
        }
        assert!(
            Generated::parse(
                0,
                &RECIPE.replace(
                    "kind: vector, dimensions: 3",
                    "kind: repeat, text: x, count: 16777216"
                )
            )
            .is_err()
        );
        assert!(Generated::parse(0, "tables: []").is_ok());
        assert!(
            Generated::parse(
                0,
                &RECIPE
                    .replace("rows: 5", "rows: 0")
                    .replace("commits: 2", "commits: 0")
            )
            .is_ok()
        );
        assert!(Generated::parse(0, &RECIPE.replace("commits: 2", "commits: 4")).is_ok());
        let empty = RECIPE
            .replace("rows: 5", "rows: 0")
            .replace("commits: 2", "commits: 0");
        for batch in [0, 4097, u64::MAX] {
            let invalid = empty.replace(
                "commits: 0",
                &format!("commits: 0\n    batch_rows: {batch}"),
            );
            assert!(
                Generated::parse(0, &invalid)
                    .unwrap_err()
                    .contains("batch_rows")
            );
        }
    }

    #[test]
    fn partitions_preserve_exact_commit_boundaries() {
        for (rows, commits, batch, expected) in [
            (5, 4, None, vec![0..2, 2..3, 3..4, 4..5]),
            (4097, 2, Some(4096), vec![0..4096, 4096..4097]),
            (0, 0, Some(4096), vec![]),
        ] {
            let mut recipe: Recipe =
                crate::runner_config::yaml(RECIPE, "generated recipe").unwrap();
            let table = &mut recipe.tables[0];
            table.rows = rows;
            table.commits = commits;
            table.batch_rows = batch;
            table.validate().unwrap();
            assert_eq!(table.batches().collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn keys_ranges_and_wrapping_address_existing_rows() {
        let body = r#"tables:
  - kind: edge
    name: knows
    rows: 3
    commits: 1
    start: 4
    wrap: 5
    id: {kind: key, prefix: e, width: 2}
    from: {kind: endpoint, prefix: n, width: 2, population: 5, distribution: {kind: ordinal}}
    to: {kind: key, prefix: n, width: 2, modulo: 5}
    columns:
      cohort:
        kind: ranges
        ranges: [{end: 1, value: first}, {end: 3, value: middle}]
        fallback: last
      payload: {kind: repeat, text: x, count: 4}
"#;
        let generated = Generated::parse(0, body).unwrap();
        let rows = generated
            .single_batch()
            .unwrap()
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            rows.iter()
                .map(|row| row["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["e04", "e00", "e01"]
        );
        assert_eq!(
            rows.iter()
                .map(|row| row["data"]["cohort"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["last", "first", "middle"]
        );
        assert_eq!(rows[0]["data"]["payload"], "xxxx");
        assert_eq!(rows[0]["from"], rows[0]["to"]);
        assert!(
            Generated::parse(
                0,
                &body.replace("    id: {kind: key, prefix: e, width: 2}\n", "")
            )
            .is_ok()
        );
        for field in ["id", "from", "to"] {
            let original = body
                .lines()
                .find(|line| line.trim_start().starts_with(&format!("{field}:")))
                .unwrap();
            for generator in [
                "{kind: literal, value: null}",
                "{kind: ordinal, start: 0, step: 1}",
                "{kind: modulo, modulus: 2}",
                "{kind: vector, dimensions: 3}",
                "{kind: ranges, ranges: [{end: 1, value: 1}], fallback: text}",
            ] {
                assert!(
                    Generated::parse(
                        0,
                        &body.replace(original, &format!("    {field}: {generator}"))
                    )
                    .is_err(),
                    "{field}: {generator}"
                );
            }
        }
    }
}
