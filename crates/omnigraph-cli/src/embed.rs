use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use clap::Args;
use color_eyre::eyre::{Result, bail, eyre};
use omnigraph::embedding::{EmbeddingClient, EmbeddingConfig};
use omnigraph_compiler::build_catalog;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::types::ScalarType;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Debug, Args, Clone)]
pub(crate) struct EmbedArgs {
    /// Seed manifest path; it names the schema, the raw input and the output
    #[arg(long, conflicts_with_all = ["input", "output", "schema"])]
    pub seed: Option<PathBuf>,
    /// Raw seed JSONL input path
    #[arg(long, requires_all = ["output", "schema"], conflicts_with = "seed")]
    pub input: Option<PathBuf>,
    /// Embedded JSONL output path
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Schema (.pg) whose `@embed` declarations name each vector, its source
    /// property, its dimension and its model
    #[arg(long, requires_all = ["input", "output"], conflicts_with = "seed")]
    pub schema: Option<PathBuf>,
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
    pub schema: String,
    pub records: usize,
    pub selected_records: usize,
    pub embedded_records: usize,
    pub cleaned_records: usize,
    pub mode: &'static str,
    /// The `@embed` vectors this run processed, as the schema declares them.
    pub vectors: Vec<EmbedTarget>,
    pub model: String,
}

#[derive(Debug, Clone)]
pub(crate) struct EmbedJob {
    input: PathBuf,
    output: PathBuf,
    schema: PathBuf,
    plan: EmbedPlan,
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

/// One vector the schema declares as
/// `property: Vector(dimension) @embed("source", model="…")` on a node type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct EmbedTarget {
    #[serde(rename = "type")]
    pub type_name: String,
    pub property: String,
    /// The String property whose value is the text embedded.
    pub source: String,
    pub dimension: usize,
    /// The model `@embed` records, if any.
    #[serde(skip)]
    pub model: Option<String>,
    /// `Vector(N)` without `?`: a load refuses a record that lacks it.
    #[serde(skip)]
    pub required: bool,
}

/// The vectors a run fills, keyed by node type and read from the schema's
/// `@embed` declarations, so no target, source, dimension or model is
/// restated where it could disagree with the schema.
#[derive(Debug, Clone)]
struct EmbedPlan {
    types: BTreeMap<String, Vec<EmbedTarget>>,
}

impl EmbedPlan {
    fn from_schema_file(path: &Path) -> Result<Self> {
        let source = fs::read_to_string(path)
            .map_err(|err| eyre!("cannot read schema {}: {err}", path.display()))?;
        Self::from_schema_source(&source).map_err(|err| eyre!("schema {}: {err}", path.display()))
    }

    fn from_schema_source(source: &str) -> Result<Self> {
        let schema = parse_schema(source).map_err(|err| eyre!(err.to_string()))?;
        let catalog = build_catalog(&schema).map_err(|err| eyre!(err.to_string()))?;
        Self::from_catalog(&catalog)
    }

    fn from_catalog(catalog: &Catalog) -> Result<Self> {
        let mut types = BTreeMap::<String, Vec<EmbedTarget>>::new();
        for node_type in catalog.node_types.values() {
            for (property, embed) in &node_type.embed_sources {
                let (dimension, required) = node_type
                    .properties
                    .get(property)
                    .and_then(|prop| match prop.scalar {
                        ScalarType::Vector(dimension) if !prop.list => {
                            Some((dimension as usize, !prop.nullable))
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        eyre!(
                            "@embed property {}.{property} is not a Vector(N)",
                            node_type.name
                        )
                    })?;
                types
                    .entry(node_type.name.clone())
                    .or_default()
                    .push(EmbedTarget {
                        type_name: node_type.name.clone(),
                        property: property.clone(),
                        source: embed.source.clone(),
                        dimension,
                        model: embed.model.clone(),
                        required,
                    });
            }
        }
        if types.is_empty() {
            bail!("the schema declares no `@embed` vector property, so there is nothing to embed");
        }
        for targets in types.values_mut() {
            targets.sort_by(|a, b| a.property.cmp(&b.property));
        }
        Ok(Self { types })
    }

    /// Keep the vectors of the named types. Every name must declare one: a
    /// `--type` the run could do nothing for is a mistake, not a filter.
    fn restrict_to(mut self, type_filter: &HashSet<String>) -> Result<Self> {
        if type_filter.is_empty() {
            return Ok(self);
        }
        if let Some(type_name) = type_filter
            .iter()
            .filter(|type_name| !self.types.contains_key(*type_name))
            .min()
        {
            bail!(
                "--type {type_name}: the schema declares no `@embed` vector on a node type of that name"
            );
        }
        self.types
            .retain(|type_name, _| type_filter.contains(type_name));
        Ok(self)
    }

    /// Refuse to embed for a property whose recorded `@embed` model the
    /// provider does not resolve to, the rule a text `nearest()` applies to
    /// the same property.
    fn check_model(&self, config: &EmbeddingConfig) -> Result<()> {
        for target in self.types.values().flatten() {
            if let Some(recorded) = &target.model
                && !config.embeds_with_model(recorded)
            {
                bail!(
                    "{}.{} records `@embed(model=\"{recorded}\")`, but the embedding provider \
                     resolves to model '{}'; vectors from another model would not answer a text \
                     nearest() on it. Set OMNIGRAPH_EMBED_MODEL='{recorded}' (and the matching \
                     provider), or restrict the run with --type.",
                    target.type_name,
                    target.property,
                    config.model
                );
            }
        }
        Ok(())
    }

    fn targets(&self) -> Vec<EmbedTarget> {
        self.types.values().flatten().cloned().collect()
    }
}

/// A seed manifest: the files one `omnigraph embed --seed` run reads and
/// writes. What each vector holds comes from the schema it names.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedManifest {
    #[serde(default)]
    schema: Option<PathBuf>,
    #[serde(default)]
    sources: Option<SeedSources>,
    #[serde(default)]
    artifacts: Option<SeedArtifacts>,
    #[serde(default)]
    seed: Option<LegacySeed>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedSources {
    raw_seed: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedArtifacts {
    embedded_seed: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

    let (input, output, schema) = if let Some(seed_path) = &args.seed {
        let manifest = load_seed_manifest(seed_path)?;
        (
            manifest.raw_seed,
            args.output.clone().unwrap_or(manifest.embedded_seed),
            manifest.schema,
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
        let schema = args
            .schema
            .clone()
            .ok_or_else(|| eyre!("--schema is required when --seed is not provided"))?;
        (input, output, schema)
    };
    let plan = EmbedPlan::from_schema_file(&schema)?.restrict_to(&type_filter)?;

    Ok(EmbedJob {
        input,
        output,
        schema,
        plan,
        mode,
        type_filter,
        selectors,
    })
}

pub(crate) async fn execute_embed(args: &EmbedArgs) -> Result<EmbedOutput> {
    let job = resolve_embed_job(args)?;
    let client = match job.mode {
        EmbedMode::Clean => None,
        EmbedMode::FillMissing | EmbedMode::ReembedAll => Some(EmbeddingClient::from_env()?),
    };
    run_embed_job(&job, client.as_ref()).await
}

/// What a run does to each selected record's vectors.
#[derive(Clone, Copy)]
enum VectorAction<'a> {
    Clean,
    /// `reembed` replaces a vector the record already has.
    Embed {
        client: &'a EmbeddingClient,
        reembed: bool,
    },
}

/// Run `job`, embedding with `client`, which `--clean` alone may omit. A
/// refusal on a recorded model comes before the output is created.
async fn run_embed_job(job: &EmbedJob, client: Option<&EmbeddingClient>) -> Result<EmbedOutput> {
    let action = match (job.mode, client) {
        (EmbedMode::Clean, _) => VectorAction::Clean,
        (mode, Some(client)) => {
            job.plan.check_model(client.config())?;
            VectorAction::Embed {
                client,
                reembed: matches!(mode, EmbedMode::ReembedAll) || !job.selectors.is_empty(),
            }
        }
        (_, None) => bail!("generating embeddings needs an embedding client"),
    };
    if !job.input.exists() {
        bail!("seed input does not exist: {}", job.input.display());
    }

    if let Some(parent) = job.output.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_output = temp_output_path(&job.output);
    let mut reader = BufReader::new(File::open(&job.input)?);
    let mut writer = BufWriter::new(File::create(&temp_output)?);

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

        let targets: &[EmbedTarget] = match row.type_name() {
            Some(type_name) if selected => job.plan.types.get(type_name).map_or(&[], Vec::as_slice),
            _ => &[],
        };
        let mut embedded = false;
        let mut cleaned = false;
        for target in targets {
            match action {
                VectorAction::Clean => {
                    if row
                        .data_mut()
                        .is_some_and(|data| data.remove(&target.property).is_some())
                    {
                        cleaned = true;
                    }
                }
                VectorAction::Embed { client, reembed } => {
                    if reembed
                        || embedding_missing(row.data().and_then(|data| data.get(&target.property)))
                    {
                        match embed_row(&mut row, target, client, records).await? {
                            VectorChange::Embedded => embedded = true,
                            VectorChange::Removed => cleaned = true,
                            VectorChange::Unchanged => {}
                        }
                    }
                }
            }
        }
        embedded_records += usize::from(embedded);
        cleaned_records += usize::from(cleaned);

        writer.write_all(serde_json::to_string(&row.into_value())?.as_bytes())?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    fs::rename(&temp_output, &job.output)?;

    Ok(EmbedOutput {
        input: job.input.display().to_string(),
        output: job.output.display().to_string(),
        schema: job.schema.display().to_string(),
        records,
        selected_records,
        embedded_records,
        cleaned_records,
        mode: job.mode.as_str(!job.selectors.is_empty()),
        vectors: job.plan.targets(),
        // The model the provider resolved, which `check_model` held to every
        // recorded `@embed` model (empty for `--clean`, which builds no
        // client).
        model: match action {
            VectorAction::Embed { client, .. } => client.config().model.clone(),
            VectorAction::Clean => String::new(),
        },
    })
}

fn temp_output_path(output: &Path) -> PathBuf {
    let mut temp = output.as_os_str().to_os_string();
    temp.push(".tmp");
    PathBuf::from(temp)
}

struct ResolvedSeedManifest {
    raw_seed: PathBuf,
    embedded_seed: PathBuf,
    schema: PathBuf,
}

fn load_seed_manifest(path: &Path) -> Result<ResolvedSeedManifest> {
    let base_dir = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or(std::env::current_dir()?);
    let manifest: SeedManifest = serde_yaml::from_str(&fs::read_to_string(path)?)
        .map_err(|err| eyre!("seed manifest {}: {err}", path.display()))?;
    let schema = manifest.schema.ok_or_else(|| {
        eyre!(
            "seed manifest {} is missing schema, the .pg file whose `@embed` declarations \
             the run fills",
            path.display()
        )
    })?;
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

    Ok(ResolvedSeedManifest {
        raw_seed: base_dir.join(raw_seed),
        embedded_seed: base_dir.join(embedded_seed),
        schema: base_dir.join(schema),
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
    /// The record has no source text, so its target value was removed.
    Removed,
    /// The record has no source text and had no target value.
    Unchanged,
}

/// Set the record's vector to the embedding of its source text. A record
/// without source text keeps no target value: a vector derived from anything
/// else would not answer a `nearest()` query for that text, and a null or an
/// empty placeholder is no vector either. When the vector is required, such a
/// record is refused here, by line, rather than written for a load to refuse.
async fn embed_row(
    row: &mut EmbedRow,
    target: &EmbedTarget,
    client: &EmbeddingClient,
    line_number: usize,
) -> Result<VectorChange> {
    let EmbedRow::Entity {
        type_name, data, ..
    } = row
    else {
        bail!("cannot embed non-entity seed records");
    };
    let Some(text) = embedding_source_text(type_name, data, &target.source, line_number)? else {
        if target.required {
            bail!(
                "line {line_number}: {type_name}.{} has no text to embed, but {type_name}.{} is \
                 a required Vector({}), which a load refuses to leave empty",
                target.source,
                target.property,
                target.dimension
            );
        }
        return Ok(match data.remove(&target.property) {
            Some(_) => VectorChange::Removed,
            None => VectorChange::Unchanged,
        });
    };
    let embedding = client.embed_document_text(text, target.dimension).await?;
    data.insert(target.property.clone(), json!(embedding));
    Ok(VectorChange::Embedded)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;

    use super::{
        EmbedArgs, EmbedPlan, EmbedRow, EmbedTarget, RowSelector, VectorChange, embed_row,
        load_seed_manifest, parse_row, render_value, resolve_embed_job, run_embed_job,
    };
    use omnigraph::embedding::{EmbeddingClient, EmbeddingConfig};
    use serde_json::{Value, json};

    /// Two vectors on one type, with different sources and dimensions, one
    /// required and one recording its model, and a type with none.
    const ISSUE_SCHEMA: &str = r#"
node Issue {
    slug: String @key
    title: String?
    body: String?
    embedding: Vector(8) @embed("title")
    body_embedding: Vector(4)? @embed("body", model="mock-body")
}

node Tag {
    slug: String @key
}
"#;

    /// A mock client resolving `model`, or `OMNIGRAPH_EMBED_MODEL` when `None`.
    fn mock_client(model: Option<&str>) -> EmbeddingClient {
        let config = EmbeddingConfig::from_parts(
            Some("mock"),
            None,
            model.map(str::to_string),
            String::new(),
        )
        .unwrap();
        EmbeddingClient::new(config).unwrap()
    }

    fn target(
        property: &str,
        source: &str,
        dimension: usize,
        model: Option<&str>,
        required: bool,
    ) -> EmbedTarget {
        EmbedTarget {
            type_name: "Issue".to_string(),
            property: property.to_string(),
            source: source.to_string(),
            dimension,
            model: model.map(str::to_string),
            required,
        }
    }

    /// `embedding: Vector(8)? @embed("title")`.
    fn title_target() -> EmbedTarget {
        target("embedding", "title", 8, None, false)
    }

    fn issue_plan() -> EmbedPlan {
        EmbedPlan::from_schema_source(ISSUE_SCHEMA).unwrap()
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

    /// The schema is the one statement of what each vector embeds: every
    /// `@embed` declaration becomes a target with its own source, dimension
    /// and recorded model, and a type without one is not a target.
    #[test]
    fn each_embed_declaration_names_its_target_source_and_dimension() {
        assert_eq!(
            issue_plan().targets(),
            vec![
                target("body_embedding", "body", 4, Some("mock-body"), false),
                target("embedding", "title", 8, None, true),
            ]
        );

        let err = EmbedPlan::from_schema_source("node Tag {\n    slug: String @key\n}\n")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("the schema declares no `@embed` vector property"),
            "{err}"
        );
    }

    #[test]
    fn a_type_filter_names_types_with_embed_vectors() {
        let only = |names: &[&str]| {
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<HashSet<_>>()
        };
        let plan = issue_plan().restrict_to(&only(&["Issue"])).unwrap();
        assert_eq!(plan.targets(), issue_plan().targets());
        for name in ["Tag", "Missing"] {
            let err = issue_plan()
                .restrict_to(&only(&["Issue", name]))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!(
                    "--type {name}: the schema declares no `@embed` vector"
                )),
                "{err}"
            );
        }
    }

    /// The rule a text `nearest()` applies to a property's recorded model.
    #[test]
    fn a_recorded_model_the_provider_does_not_resolve_is_refused() {
        issue_plan()
            .check_model(mock_client(Some("mock-body")).config())
            .unwrap();
        let err = issue_plan()
            .check_model(mock_client(Some("text-embedding-3-large")).config())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "Issue.body_embedding records `@embed(model=\"mock-body\")`, but the embedding \
                 provider resolves to model 'text-embedding-3-large'"
            ),
            "{err}"
        );
    }

    /// A run fills each vector from the source and dimension its `@embed`
    /// declares, so each stored vector is the query vector of its source text;
    /// a provider resolving another model than a recorded one is refused
    /// before the output exists.
    #[tokio::test]
    async fn a_run_embeds_each_declared_source_at_its_declared_dimension() {
        let dir = tempfile::tempdir().unwrap();
        let schema = dir.path().join("schema.pg");
        fs::write(&schema, ISSUE_SCHEMA).unwrap();
        let tag = json!({ "type": "Tag", "data": { "slug": "t-1" } });
        let edge = json!({ "edge": "Tagged", "from": "i-1", "to": "t-1" });
        let rows = [
            json!({ "type": "Issue", "data": {
                "slug": "i-1",
                "title": "Fix login timeout",
                "body": "Sessions expire after a minute",
            } }),
            json!({ "type": "Issue", "data": { "slug": "i-2", "title": "Paint the bikeshed" } }),
            tag.clone(),
            edge.clone(),
        ];
        let input = dir.path().join("raw.jsonl");
        fs::write(
            &input,
            rows.iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let output = dir.path().join("build/embedded.jsonl");
        let job = resolve_embed_job(&EmbedArgs {
            seed: None,
            input: Some(input),
            output: Some(output.clone()),
            schema: Some(schema),
            clean: false,
            reembed_all: false,
            types: Vec::new(),
            selectors: Vec::new(),
            json: false,
        })
        .unwrap();

        let refused = run_embed_job(&job, Some(&mock_client(Some("other"))))
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains("resolves to model 'other'"), "{refused}");
        assert!(!output.exists(), "a refused run writes no output");

        let client = mock_client(Some("mock-body"));
        let summary = run_embed_job(&job, Some(&client)).await.unwrap();
        assert_eq!(
            (
                summary.records,
                summary.selected_records,
                summary.embedded_records,
                summary.cleaned_records,
            ),
            (4, 3, 2, 0)
        );
        assert_eq!(summary.vectors, issue_plan().targets());
        assert_eq!(summary.model, "mock-body");

        let written = fs::read_to_string(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        for (row, property, text, dimension) in [
            (0, "embedding", "Fix login timeout", 8),
            (0, "body_embedding", "Sessions expire after a minute", 4),
            (1, "embedding", "Paint the bikeshed", 8),
        ] {
            let query = client.embed_query_text(text, dimension).await.unwrap();
            assert_eq!(written[row]["data"][property], json!(query), "{property}");
        }
        assert_eq!(written[1]["data"].get("body_embedding"), None);
        assert_eq!(written[2], tag);
        assert_eq!(written[3], edge);
    }

    /// A seed manifest names its schema; a key the command does not read,
    /// such as the retired `embeddings` spec, is refused, not ignored.
    #[test]
    fn a_seed_manifest_names_its_schema_and_refuses_unknown_keys() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = dir.path().join("seed.yaml");
        let sources = "sources:\n  raw_seed: ./data/seed.jsonl\n";

        fs::write(&manifest, format!("schema: ./schema.pg\n{sources}")).unwrap();
        let resolved = load_seed_manifest(&manifest).unwrap();
        assert_eq!(resolved.schema, dir.path().join("./schema.pg"));
        assert_eq!(resolved.raw_seed, dir.path().join("./data/seed.jsonl"));
        assert_eq!(
            resolved.embedded_seed,
            dir.path().join("./build/seed.embedded.jsonl")
        );

        for (yaml, expected) in [
            (sources.to_string(), "is missing schema".to_string()),
            (
                format!(
                    "schema: ./schema.pg\n{sources}embeddings:\n  dimension: 4\n  types:\n    \
                     Decision:\n      target: embedding\n      fields: [intent]\n"
                ),
                "unknown field `embeddings`".to_string(),
            ),
            (
                format!("graph:\n  slug: g\nschema: ./schema.pg\n{sources}"),
                "unknown field `graph`".to_string(),
            ),
            (
                format!("schema: ./schema.pg\n{sources}  model: m\n"),
                "unknown field `model`".to_string(),
            ),
        ] {
            fs::write(&manifest, &yaml).unwrap();
            let err = load_seed_manifest(&manifest)
                .err()
                .map(|err| err.to_string())
                .unwrap_or_default();
            assert!(err.contains(&expected), "{yaml}: {err}");
        }
    }

    /// A stored vector and a text `nearest()` query embed the same text, the
    /// source value as stored, so under the mock provider, which ignores the
    /// retrieval role, the two vectors are equal. A template such as
    /// `type: Issue\ntitle: …` on one side makes them unrelated.
    #[tokio::test]
    async fn document_vector_is_the_query_vector_of_its_source_text() {
        let client = mock_client(None);
        for title in ["Fix login timeout", "  Fix login timeout\n"] {
            let mut row = issue(json!({ "slug": "i-1", "title": title }));
            let change = embed_row(&mut row, &title_target(), &client, 1)
                .await
                .unwrap();
            assert_eq!(change, VectorChange::Embedded);
            let query = client.embed_query_text(title, 8).await.unwrap();
            assert_eq!(vector(row), Some(json!(query)), "title {title:?}");
        }
    }

    #[tokio::test]
    async fn a_record_without_source_text_keeps_no_vector() {
        let client = mock_client(None);
        for data in [
            json!({ "slug": "i-1" }),
            json!({ "slug": "i-1", "title": null }),
            json!({ "slug": "i-1", "title": " " }),
        ] {
            let mut row = issue(data.clone());
            let change = embed_row(&mut row, &title_target(), &client, 1)
                .await
                .unwrap();
            assert_eq!(change, VectorChange::Unchanged, "{data}");
            assert_eq!(vector(row), None, "{data}");

            // A stale vector, a null, or an empty placeholder (which a load
            // refuses for a `Vector(N)`) all leave with the record's source.
            for target in [json!([0.1, 0.2]), json!([]), json!(null)] {
                let mut stale = data.clone();
                stale["embedding"] = target.clone();
                let mut row = issue(stale);
                let change = embed_row(&mut row, &title_target(), &client, 1)
                    .await
                    .unwrap();
                assert_eq!(change, VectorChange::Removed, "{data} with {target}");
                assert_eq!(vector(row), None, "{data} with {target}");
            }
        }
    }

    #[tokio::test]
    async fn a_required_vector_without_source_text_is_refused() {
        let required = target("embedding", "title", 8, None, true);
        for data in [
            json!({ "slug": "i-1" }),
            json!({ "slug": "i-1", "title": null, "embedding": [0.1, 0.2] }),
            json!({ "slug": "i-1", "title": " " }),
        ] {
            let mut row = issue(data.clone());
            let err = embed_row(&mut row, &required, &mock_client(None), 3)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(
                    "line 3: Issue.title has no text to embed, but Issue.embedding is a \
                     required Vector(8)"
                ),
                "{data}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn a_non_string_source_is_refused() {
        let mut row = issue(json!({ "slug": "i-1", "title": 7 }));
        let err = embed_row(&mut row, &title_target(), &mock_client(None), 3)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("line 3: Issue.title is not a string"), "{err}");
    }
}
