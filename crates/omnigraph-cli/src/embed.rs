use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use clap::Args;
use color_eyre::eyre::{Result, bail, eyre};
use omnigraph::embedding::EmbeddingClient;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Debug, Args, Clone)]
pub(crate) struct EmbedArgs {
    /// Seed manifest path
    #[arg(long, conflicts_with_all = ["input", "output", "spec"])]
    pub seed: Option<PathBuf>,
    /// Raw seed JSONL input path
    #[arg(long, requires_all = ["output", "spec"], conflicts_with = "seed")]
    pub input: Option<PathBuf>,
    /// Embedded JSONL output path
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Embedding spec JSON path
    #[arg(long, requires_all = ["input", "output"], conflicts_with = "seed")]
    pub spec: Option<PathBuf>,
    /// Remove embedding fields instead of generating embeddings
    #[arg(long, conflicts_with = "reembed_all")]
    pub clean: bool,
    /// Regenerate embeddings for all matching records
    #[arg(long, conflicts_with = "clean")]
    pub reembed_all: bool,
    /// Restrict processing to these type names
    #[arg(long = "type")]
    pub types: Vec<String>,
    /// Reembed or clean matching records only. Syntax: Type:field=value or field=value
    #[arg(long = "select")]
    pub selectors: Vec<String>,
    /// Print JSON summary
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EmbedOutput {
    pub input: String,
    pub output: String,
    pub records: usize,
    pub selected_records: usize,
    pub embedded_records: usize,
    pub cleaned_records: usize,
    pub mode: &'static str,
    pub dimension: usize,
    pub model: String,
}

#[derive(Debug, Clone)]
pub(crate) struct EmbedJob {
    input: PathBuf,
    output: PathBuf,
    spec: EmbedSpec,
    mode: EmbedMode,
    type_filter: HashSet<String>,
    selectors: Vec<RowSelector>,
}

#[derive(Debug, Clone, Copy)]
enum EmbedMode {
    FillMissing,
    ReembedAll,
    Clean,
}

impl EmbedMode {
    fn as_str(self, selectors_present: bool) -> &'static str {
        match self {
            Self::FillMissing if selectors_present => "reembed_selected",
            Self::FillMissing => "fill_missing",
            Self::ReembedAll => "reembed_all",
            Self::Clean => "clean",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "RawEmbedSpec")]
struct EmbedSpec {
    dimension: usize,
    types: BTreeMap<String, EmbedTypeSpec>,
}

#[derive(Debug, Clone)]
struct EmbedTypeSpec {
    target: String,
    /// The property the target's `@embed("…")` names. Its value is the text
    /// embedded.
    source: String,
}

/// The spec as written: `fields` is a list, and it must name one property.
#[derive(Deserialize)]
struct RawEmbedSpec {
    dimension: usize,
    types: BTreeMap<String, RawEmbedTypeSpec>,
}

#[derive(Deserialize)]
struct RawEmbedTypeSpec {
    target: String,
    fields: Vec<String>,
}

impl TryFrom<RawEmbedSpec> for EmbedSpec {
    type Error = String;

    fn try_from(raw: RawEmbedSpec) -> Result<Self, Self::Error> {
        let types = raw
            .types
            .into_iter()
            .map(|(type_name, spec)| match spec.fields.as_slice() {
                [source] if !source.trim().is_empty() => Ok((
                    type_name,
                    EmbedTypeSpec {
                        target: spec.target,
                        source: source.clone(),
                    },
                )),
                fields => Err(format!(
                    "embedding spec for type '{type_name}' must name exactly one field, the \
                     source property its `@embed(\"…\")` declares, found {fields:?}"
                )),
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            dimension: raw.dimension,
            types,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SeedManifest {
    #[serde(default)]
    sources: Option<SeedSources>,
    #[serde(default)]
    artifacts: Option<SeedArtifacts>,
    #[serde(default)]
    embeddings: Option<EmbedSpec>,
    #[serde(default)]
    seed: Option<LegacySeed>,
}

#[derive(Debug, Clone, Deserialize)]
struct SeedSources {
    raw_seed: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct SeedArtifacts {
    embedded_seed: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct LegacySeed {
    data: PathBuf,
}

#[derive(Debug, Clone)]
struct RowSelector {
    type_name: Option<String>,
    field: String,
    expected: String,
}

#[derive(Debug)]
enum EmbedRow {
    Entity {
        type_name: String,
        data: Map<String, Value>,
        root: Map<String, Value>,
    },
    Passthrough(Map<String, Value>),
}

pub(crate) fn resolve_embed_job(args: &EmbedArgs) -> Result<EmbedJob> {
    let mode = if args.clean {
        EmbedMode::Clean
    } else if args.reembed_all {
        EmbedMode::ReembedAll
    } else {
        EmbedMode::FillMissing
    };
    let selectors = args
        .selectors
        .iter()
        .map(|selector| RowSelector::parse(selector))
        .collect::<Result<Vec<_>>>()?;
    let type_filter = args.types.iter().cloned().collect::<HashSet<_>>();

    let (input, output, spec) = if let Some(seed_path) = &args.seed {
        let manifest = load_seed_manifest(seed_path)?;
        (
            manifest.raw_seed,
            args.output.clone().unwrap_or(manifest.embedded_seed),
            manifest.spec,
        )
    } else {
        let input = args
            .input
            .clone()
            .ok_or_else(|| eyre!("--input is required when --seed is not provided"))?;
        let output = args
            .output
            .clone()
            .ok_or_else(|| eyre!("--output is required when --seed is not provided"))?;
        let spec_path = args
            .spec
            .clone()
            .ok_or_else(|| eyre!("--spec is required when --seed is not provided"))?;
        let spec = load_embed_spec(&spec_path)?;
        (input, output, spec)
    };

    Ok(EmbedJob {
        input,
        output,
        spec,
        mode,
        type_filter,
        selectors,
    })
}

pub(crate) async fn execute_embed(args: &EmbedArgs) -> Result<EmbedOutput> {
    let job = resolve_embed_job(args)?;
    run_embed_job(&job).await
}

pub(crate) async fn run_embed_job(job: &EmbedJob) -> Result<EmbedOutput> {
    if !job.input.exists() {
        bail!("seed input does not exist: {}", job.input.display());
    }

    if let Some(parent) = job.output.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_output = temp_output_path(&job.output);
    let mut reader = BufReader::new(File::open(&job.input)?);
    let mut writer = BufWriter::new(File::create(&temp_output)?);
    let client = match job.mode {
        EmbedMode::Clean => None,
        _ => Some(EmbeddingClient::from_env()?),
    };

    let mut line = String::new();
    let mut records = 0usize;
    let mut selected_records = 0usize;
    let mut embedded_records = 0usize;
    let mut cleaned_records = 0usize;

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        let raw = line.trim();
        if raw.is_empty() {
            continue;
        }
        records += 1;
        let mut row = parse_row(raw, records)?;
        let selected = row_matches_selection(&row, &job.type_filter, &job.selectors);
        if selected {
            selected_records += 1;
        }

        if let Some(type_spec) = row
            .type_name()
            .and_then(|type_name| job.spec.types.get(type_name))
        {
            match job.mode {
                EmbedMode::Clean => {
                    if selected
                        && row
                            .data_mut()
                            .is_some_and(|data| data.remove(&type_spec.target).is_some())
                    {
                        cleaned_records += 1;
                    }
                }
                EmbedMode::ReembedAll | EmbedMode::FillMissing => {
                    let reembed =
                        matches!(job.mode, EmbedMode::ReembedAll) || !job.selectors.is_empty();
                    if selected
                        && (reembed
                            || embedding_missing(
                                row.data().and_then(|data| data.get(&type_spec.target)),
                            ))
                    {
                        match embed_row(
                            &mut row,
                            type_spec,
                            job.spec.dimension,
                            client.as_ref().unwrap(),
                            records,
                        )
                        .await?
                        {
                            VectorChange::Embedded => embedded_records += 1,
                            VectorChange::Removed => cleaned_records += 1,
                            VectorChange::Unchanged => {}
                        }
                    }
                }
            }
        }

        writer.write_all(serde_json::to_string(&row.into_value())?.as_bytes())?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    fs::rename(&temp_output, &job.output)?;

    Ok(EmbedOutput {
        input: job.input.display().to_string(),
        output: job.output.display().to_string(),
        records,
        selected_records,
        embedded_records,
        cleaned_records,
        mode: job.mode.as_str(!job.selectors.is_empty()),
        dimension: job.spec.dimension,
        // The embedding model is resolved solely from the provider config; the
        // spec carries no model field, so there is no second source of truth to
        // silently disagree with the API. Report what was actually used (empty
        // for `--clean`, which builds no client).
        model: client
            .as_ref()
            .map(|c| c.config().model.clone())
            .unwrap_or_default(),
    })
}

fn temp_output_path(output: &Path) -> PathBuf {
    let mut temp = output.as_os_str().to_os_string();
    temp.push(".tmp");
    PathBuf::from(temp)
}

fn load_embed_spec(path: &Path) -> Result<EmbedSpec> {
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

struct ResolvedSeedManifest {
    raw_seed: PathBuf,
    embedded_seed: PathBuf,
    spec: EmbedSpec,
}

fn load_seed_manifest(path: &Path) -> Result<ResolvedSeedManifest> {
    let base_dir = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or(std::env::current_dir()?);
    let manifest: SeedManifest = serde_yaml::from_str(&fs::read_to_string(path)?)?;
    let raw_seed = manifest
        .sources
        .as_ref()
        .map(|sources| sources.raw_seed.clone())
        .or_else(|| manifest.seed.as_ref().map(|seed| seed.data.clone()))
        .ok_or_else(|| eyre!("seed manifest is missing sources.raw_seed"))?;
    let embedded_seed = manifest
        .artifacts
        .as_ref()
        .map(|artifacts| artifacts.embedded_seed.clone())
        .unwrap_or_else(|| PathBuf::from("./build/seed.embedded.jsonl"));
    let spec = manifest
        .embeddings
        .ok_or_else(|| eyre!("seed manifest is missing embeddings"))?;

    Ok(ResolvedSeedManifest {
        raw_seed: base_dir.join(raw_seed),
        embedded_seed: base_dir.join(embedded_seed),
        spec,
    })
}

impl RowSelector {
    fn parse(value: &str) -> Result<Self> {
        let (lhs, expected) = value
            .split_once('=')
            .ok_or_else(|| eyre!("selector must be field=value or Type:field=value"))?;
        let (type_name, field) = if let Some((type_name, field)) = lhs.split_once(':') {
            (
                Some(type_name.trim().to_string()).filter(|value| !value.is_empty()),
                field.trim().to_string(),
            )
        } else {
            (None, lhs.trim().to_string())
        };

        if field.is_empty() {
            bail!("selector field cannot be empty");
        }

        Ok(Self {
            type_name,
            field,
            expected: expected.trim().to_string(),
        })
    }

    fn matches(&self, type_name: &str, data: &Map<String, Value>) -> bool {
        if self
            .type_name
            .as_deref()
            .is_some_and(|expected| expected != type_name)
        {
            return false;
        }

        data.get(&self.field)
            .map(render_value)
            .is_some_and(|value| value == self.expected)
    }
}

fn parse_row(raw: &str, line_number: usize) -> Result<EmbedRow> {
    let mut root = serde_json::from_str::<Map<String, Value>>(raw)
        .map_err(|err| eyre!("line {} is not valid JSON: {}", line_number, err))?;
    let Some(type_name) = root.get("type").and_then(Value::as_str).map(str::to_string) else {
        return Ok(EmbedRow::Passthrough(root));
    };
    let data = root
        .remove("data")
        .and_then(|value| value.as_object().cloned())
        .ok_or_else(|| eyre!("line {} is missing object field 'data'", line_number))?;

    Ok(EmbedRow::Entity {
        type_name,
        data,
        root,
    })
}

impl EmbedRow {
    fn into_value(self) -> Value {
        match self {
            Self::Entity {
                type_name,
                data,
                mut root,
            } => {
                root.insert("type".to_string(), Value::String(type_name));
                root.insert("data".to_string(), Value::Object(data));
                Value::Object(root)
            }
            Self::Passthrough(root) => Value::Object(root),
        }
    }

    fn type_name(&self) -> Option<&str> {
        match self {
            Self::Entity { type_name, .. } => Some(type_name.as_str()),
            Self::Passthrough(_) => None,
        }
    }

    fn data(&self) -> Option<&Map<String, Value>> {
        match self {
            Self::Entity { data, .. } => Some(data),
            Self::Passthrough(_) => None,
        }
    }

    fn data_mut(&mut self) -> Option<&mut Map<String, Value>> {
        match self {
            Self::Entity { data, .. } => Some(data),
            Self::Passthrough(_) => None,
        }
    }
}

fn row_matches_selection(
    row: &EmbedRow,
    type_filter: &HashSet<String>,
    selectors: &[RowSelector],
) -> bool {
    let Some(type_name) = row.type_name() else {
        return false;
    };
    let Some(data) = row.data() else {
        return false;
    };

    let matches_type = type_filter.is_empty() || type_filter.contains(type_name);
    if !matches_type {
        return false;
    }
    if selectors.is_empty() {
        return true;
    }
    selectors
        .iter()
        .any(|selector| selector.matches(type_name, data))
}

fn embedding_missing(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::Array(values)) => values.is_empty(),
        _ => false,
    }
}

fn render_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.trim().to_string(),
        Value::Bool(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Number(value) => value.to_string(),
        Value::Array(values) => values
            .iter()
            .map(render_value)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(", "),
        other => other.to_string(),
    }
}

/// The text an `@embed("source")` vector embeds: the source value exactly as
/// stored, with no type name, field label or trimming. A text
/// `nearest($v, $q)` embeds `$q` the same way, so a record whose source equals
/// the query text embeds the same input. `None` when the source is absent,
/// null or blank.
fn embedding_source_text<'a>(
    type_name: &str,
    data: &'a Map<String, Value>,
    source: &str,
    line_number: usize,
) -> Result<Option<&'a str>> {
    match data.get(source) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.trim().is_empty() => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => bail!(
            "line {}: {}.{} is not a string; an `@embed` source property is a String",
            line_number,
            type_name,
            source
        ),
    }
}

/// What embedding a record did to its vector.
#[derive(Debug, PartialEq, Eq)]
enum VectorChange {
    Embedded,
    /// The record has no source text, so its vector was removed.
    Removed,
    /// The record has no source text and had no vector.
    Unchanged,
}

/// Set the record's vector to the embedding of its source text. A record
/// without source text keeps no vector: one derived from anything else would
/// not answer a `nearest()` query for that text.
async fn embed_row(
    row: &mut EmbedRow,
    spec: &EmbedTypeSpec,
    dimension: usize,
    client: &EmbeddingClient,
    line_number: usize,
) -> Result<VectorChange> {
    let EmbedRow::Entity {
        type_name, data, ..
    } = row
    else {
        bail!("cannot embed non-entity seed records");
    };
    let Some(text) = embedding_source_text(type_name, data, &spec.source, line_number)? else {
        if embedding_missing(data.get(&spec.target)) {
            return Ok(VectorChange::Unchanged);
        }
        data.remove(&spec.target);
        return Ok(VectorChange::Removed);
    };
    let embedding = client.embed_document_text(text, dimension).await?;
    data.insert(spec.target.clone(), json!(embedding));
    Ok(VectorChange::Embedded)
}

#[cfg(test)]
mod tests {
    use super::{
        EmbedRow, EmbedSpec, EmbedTypeSpec, RowSelector, VectorChange, embed_row, parse_row,
        render_value,
    };
    use omnigraph::embedding::{EmbeddingClient, EmbeddingConfig};
    use serde_json::{Value, json};

    fn mock_client() -> EmbeddingClient {
        let config = EmbeddingConfig::from_parts(Some("mock"), None, None, String::new()).unwrap();
        EmbeddingClient::new(config).unwrap()
    }

    fn title_spec() -> EmbedTypeSpec {
        EmbedTypeSpec {
            target: "embedding".to_string(),
            source: "title".to_string(),
        }
    }

    fn issue(data: Value) -> EmbedRow {
        parse_row(&json!({ "type": "Issue", "data": data }).to_string(), 1).unwrap()
    }

    fn vector(row: EmbedRow) -> Option<Value> {
        row.into_value()["data"].get("embedding").cloned()
    }

    #[test]
    fn selector_parses_type_and_field_forms() {
        let typed = RowSelector::parse("Decision:slug=dec-1").unwrap();
        assert_eq!(typed.type_name.as_deref(), Some("Decision"));
        assert_eq!(typed.field, "slug");
        assert_eq!(typed.expected, "dec-1");

        let plain = RowSelector::parse("slug=dec-2").unwrap();
        assert_eq!(plain.type_name, None);
        assert_eq!(plain.field, "slug");
        assert_eq!(plain.expected, "dec-2");
    }

    #[test]
    fn render_value_handles_lists_and_scalars() {
        assert_eq!(render_value(&json!(["a", "b"])), "a, b");
        assert_eq!(render_value(&json!(true)), "true");
        assert_eq!(render_value(&json!(3)), "3");
    }

    /// A stored vector and a text `nearest()` query embed the same text, the
    /// source value as stored, so under the mock provider, which ignores the
    /// retrieval role, the two vectors are equal. A template such as
    /// `type: Issue\ntitle: …` on one side makes them unrelated.
    #[tokio::test]
    async fn document_vector_is_the_query_vector_of_its_source_text() {
        let client = mock_client();
        for title in ["Fix login timeout", "  Fix login timeout\n"] {
            let mut row = issue(json!({ "slug": "i-1", "title": title }));
            let change = embed_row(&mut row, &title_spec(), 8, &client, 1)
                .await
                .unwrap();
            assert_eq!(change, VectorChange::Embedded);
            let query = client.embed_query_text(title, 8).await.unwrap();
            assert_eq!(vector(row), Some(json!(query)), "title {title:?}");
        }
    }

    #[tokio::test]
    async fn a_record_without_source_text_keeps_no_vector() {
        let client = mock_client();
        for data in [
            json!({ "slug": "i-1" }),
            json!({ "slug": "i-1", "title": null }),
            json!({ "slug": "i-1", "title": " " }),
        ] {
            let mut row = issue(data.clone());
            let change = embed_row(&mut row, &title_spec(), 8, &client, 1)
                .await
                .unwrap();
            assert_eq!(change, VectorChange::Unchanged, "{data}");
            assert_eq!(vector(row), None, "{data}");

            let mut stale = data.clone();
            stale["embedding"] = json!([0.1, 0.2]);
            let mut row = issue(stale);
            let change = embed_row(&mut row, &title_spec(), 8, &client, 1)
                .await
                .unwrap();
            assert_eq!(change, VectorChange::Removed, "{data}");
            assert_eq!(vector(row), None, "{data}");
        }
    }

    #[tokio::test]
    async fn a_non_string_source_is_refused() {
        let mut row = issue(json!({ "slug": "i-1", "title": 7 }));
        let err = embed_row(&mut row, &title_spec(), 8, &mock_client(), 3)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("line 3: Issue.title is not a string"), "{err}");
    }

    #[test]
    fn spec_names_exactly_one_source_field() {
        let spec = |fields: Value| {
            serde_json::from_value::<EmbedSpec>(json!({
                "dimension": 4,
                "types": { "Issue": { "target": "embedding", "fields": fields } }
            }))
        };
        let resolved = spec(json!(["title"])).unwrap();
        assert_eq!(resolved.types["Issue"].target, "embedding");
        assert_eq!(resolved.types["Issue"].source, "title");
        for fields in [json!([]), json!(["slug", "title"]), json!([" "])] {
            let err = spec(fields.clone()).unwrap_err().to_string();
            assert!(
                err.contains("embedding spec for type 'Issue' must name exactly one field"),
                "{fields}: {err}"
            );
        }
    }
}
