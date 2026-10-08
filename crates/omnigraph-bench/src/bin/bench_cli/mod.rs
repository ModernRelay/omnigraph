//! Discovery, config generation and help share the executable's parser.
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, CommandFactory, Subcommand, ValueEnum};
use omnigraph_bench::catalog::{
    self, Catalog, ConfigV2, Defaults, FixtureInput, ProtocolOverrides, Scenario,
};
use omnigraph_bench::dataset_cache::{self, CacheInspection, CacheState, SourceAvailability};
use omnigraph_bench::discovery;
use omnigraph_bench::gqt_case::{self, GqtFixture, MeasuredStep};
use omnigraph_bench::gqt_runner::GqtOperationKind;
use omnigraph_bench::{Diagnostic, RUNNER_OUTPUT_VERSION};
use serde::Serialize;
use serde_json::{Value, json};

use super::{Cli, print_diagnostics, print_json_success};

#[derive(Debug, Args)]
pub struct CatalogArgs {
    /// Read this config; otherwise discover benchmarks/benchmarks.yaml from cwd ancestors.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Emit versioned machine-readable output.
    #[arg(long)]
    pub json: bool,
}
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum InventoryKind {
    Fixtures,
    Workloads,
    Scenarios,
}
#[derive(Debug, Args)]
pub struct ListArgs {
    #[arg(value_enum)]
    pub kind: InventoryKind,
    #[command(flatten)]
    pub catalog: CatalogArgs,
}
#[derive(Debug, Args)]
pub struct ShowArgs {
    /// Scenario ID from the selected config.
    #[arg(required_unless_present = "workload", conflicts_with = "workload")]
    pub name: Option<String>,
    /// Inspect parser operation ordinals/text in this cwd-relative GQT file.
    #[arg(long, conflicts_with = "config")]
    pub workload: Option<PathBuf>,
    #[command(flatten)]
    pub catalog: CatalogArgs,
}
#[derive(Debug, Subcommand)]
pub enum CacheCommand {
    /// Inspect the exact dataset variant needed by a scenario; never builds or restores it.
    Status {
        name: String,
        #[command(flatten)]
        catalog: CatalogArgs,
        /// Same cwd-relative cache root used by run.
        #[arg(long, default_value = "target/gqt-datasets")]
        dataset_cache: PathBuf,
        /// Registered source mapping, ID=BUNDLE. Never copied by inspection.
        #[arg(long = "fixture", value_name = "ID=BUNDLE")]
        fixtures: Vec<String>,
        /// Audit all published bytes as well as the descriptor and logical evidence.
        #[arg(long)]
        verify: bool,
    },
    /// List a bounded page of existing cache variants, including historical entries.
    List {
        #[arg(long, default_value = "target/gqt-datasets")]
        dataset_cache: PathBuf,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        /// Continue after the previous page's cursor.
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        verify: bool,
        #[arg(long)]
        json: bool,
    },
}
#[derive(Debug, Args)]
pub struct InitArgs {
    /// Fixture GQT path relative to the current directory.
    #[arg(long)]
    pub fixture: PathBuf,
    /// Workload GQT path relative to the current directory.
    #[arg(long)]
    pub workload: PathBuf,
    /// Parser operation ordinal; use show --workload to discover these.
    #[arg(long)]
    pub step: usize,
    /// New YAML file; existing files and symlinks are never replaced.
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value = "custom")]
    pub name: String,
    #[arg(long)]
    pub json: bool,
}
#[derive(Debug, Args)]
pub struct HelpArgs {
    /// config, cache, or a command path such as "suite run".
    pub topic: Vec<String>,
    #[arg(long)]
    pub json: bool,
}

pub fn legacy_input(path: &Path) -> Result<bool, Vec<Diagnostic>> {
    if path.is_absolute() || path.components().count() > 1 || path.extension().is_some() {
        return Ok(true);
    }
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(vec![Diagnostic::error(
            "case_path_unreadable",
            path.display().to_string(),
            e.to_string(),
        )]),
    }
}

pub fn load(args: &CatalogArgs) -> Result<Catalog, Vec<Diagnostic>> {
    Catalog::load(&catalog::find_config(args.config.as_deref())?)
}
pub fn failure(diagnostics: Vec<Diagnostic>, json_output: bool) -> ExitCode {
    if json_output {
        print_json_success(&json!({"cli_output_version":1,"ok":false,"diagnostics":diagnostics}));
        ExitCode::FAILURE
    } else {
        print_diagnostics(&diagnostics)
    }
}
fn success<T: Serialize>(value: &T, json_output: bool) -> ExitCode {
    if json_output {
        let value = match serde_json::to_value(value) {
            Ok(value) => value,
            Err(e) => return failure(problem("output_serialization_failed", e.to_string()), true),
        };
        print_json_success(
            &json!({"cli_output_version":1,"ok":true,"value":value,"diagnostics":[]}),
        )
    } else {
        match serde_yaml::to_string(value) {
            Ok(text) => {
                print!("{text}");
                ExitCode::SUCCESS
            }
            Err(e) => failure(
                vec![Diagnostic::error("serialization_error", "$", e.to_string())],
                false,
            ),
        }
    }
}
fn problem(code: &str, message: impl Into<String>) -> Vec<Diagnostic> {
    vec![Diagnostic::error(code, "$", message)]
}

pub fn list(args: ListArgs) -> ExitCode {
    let catalog = match load(&args.catalog) {
        Ok(c) => c,
        Err(e) => return failure(e, args.catalog.json),
    };
    match args.kind {
        InventoryKind::Scenarios => {
            let inventory = discovery::scenarios(&catalog);
            if args.catalog.json {
                return success(&inventory, true);
            }
            println!("SCENARIO  SOURCE  GROUPS");
            for entry in inventory.entries {
                println!(
                    "{}  {:?}  {}",
                    entry.id,
                    entry.source,
                    entry.groups.join(", ")
                );
                for diagnostic in entry.diagnostics {
                    eprintln!("{}: {}", diagnostic.path, diagnostic.message);
                }
            }
            ExitCode::SUCCESS
        }
        kind => match discovery::sources(&catalog, matches!(kind, InventoryKind::Fixtures)) {
            Ok(inventory) => {
                if args.catalog.json {
                    return success(&inventory, true);
                }
                println!("PATH  SOURCE  KIND  SCENARIOS");
                for entry in inventory.entries {
                    println!(
                        "{}  {:?}  {}  {}",
                        entry.path.display(),
                        entry.source,
                        entry.kind,
                        entry.scenarios.join(", ")
                    );
                    for diagnostic in entry.diagnostics {
                        eprintln!("{}: {}", diagnostic.path, diagnostic.message);
                    }
                }
                ExitCode::SUCCESS
            }
            Err(e) => failure(e, args.catalog.json),
        },
    }
}

pub fn show(args: ShowArgs) -> ExitCode {
    if let Some(path) = args.workload {
        let source = match gqt_case::read_source(&path) {
            Ok(s) => s,
            Err(e) => return failure(problem("invalid_workload", e), args.catalog.json),
        };
        let case = match source.parse() {
            Ok(c) => c,
            Err(e) => return failure(problem("invalid_workload", e), args.catalog.json),
        };
        let descriptors = match gqt_case::workload_steps(&case) {
            Ok(steps) => steps,
            Err(e) => return failure(problem("invalid_workload", e), args.catalog.json),
        };
        let operations: Vec<_> = descriptors.into_iter().map(|s| {
            json!({"ordinal":s.ordinal,"kind":GqtOperationKind::from(s.kind),"text":s.source,"in_loop":s.in_loop})
        }).collect();
        return success(
            &json!({"workload":path.to_string_lossy(),"sha256":source.sha256,"operations":operations}),
            args.catalog.json,
        );
    }
    let catalog = match load(&args.catalog) {
        Ok(c) => c,
        Err(e) => return failure(e, args.catalog.json),
    };
    let name = args
        .name
        .as_deref()
        .expect("clap requires scenario or workload");
    let plan = match catalog.plan(name) {
        Ok(p) => p,
        Err(e) => return failure(e, args.catalog.json),
    };
    let scenario = catalog.scenario(name).expect("resolved scenario exists");
    let resolve = |path: &Path| {
        let resolved = catalog.root.join(path).canonicalize()?;
        if resolved.to_str().is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "resolved source path must be UTF-8",
            ));
        }
        Ok(resolved)
    };
    let resolved_fixture = match catalog::fixture_paths(&plan.definition.fixture)
        .iter()
        .map(|p| resolve(p))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(paths) => paths,
        Err(e) => return failure(problem("source_changed", e.to_string()), args.catalog.json),
    };
    let resolved_workload = match resolve(&scenario.workload) {
        Ok(path) => path,
        Err(e) => return failure(problem("source_changed", e.to_string()), args.catalog.json),
    };
    success(
        &json!({
            "id":name,"config":catalog.path,"config_version":catalog::CONFIG_VERSION,"fixture":plan.definition.fixture,
            "resolved_fixture":resolved_fixture,
            "resolved_workload":resolved_workload,
            "workload":scenario.workload,"measured_step":plan.definition.workload.measured_step,
            "environment":plan.definition.environment,"protocol":plan.definition.protocol,
            "repetitions":scenario.expand(&catalog.definition.defaults).1,
            "recipe_sha256":plan.recipe_sha256,"planned_sha256":plan.planned_sha256,
            "needs_indices":plan.needs_indices,"cache_condition":plan.cache_condition,
        }),
        args.catalog.json,
    )
}

pub fn cache(command: CacheCommand) -> ExitCode {
    match command {
        CacheCommand::List {
            dataset_cache,
            limit,
            after,
            verify,
            json,
        } => match dataset_cache::list(&dataset_cache, limit, after.as_deref(), verify) {
            Ok(page) => {
                let ok = page.entries.iter().all(inspection_succeeded);
                let diagnostics = page
                    .entries
                    .iter()
                    .filter_map(|entry| {
                        entry.diagnostic.as_ref().map(|e| {
                            Diagnostic::error(&e.code, entry.path.display().to_string(), &e.message)
                        })
                    })
                    .collect();
                cache_output(&page, ok, diagnostics, json)
            }
            Err(e) => failure(problem(&e.code, e.message), json),
        },
        CacheCommand::Status {
            name,
            catalog: args,
            dataset_cache,
            fixtures,
            verify,
        } => {
            let catalog = match load(&args) {
                Ok(c) => c,
                Err(e) => return failure(e, args.json),
            };
            let scenario = match catalog.scenario(&name) {
                Ok(s) => s,
                Err(e) => return failure(e, args.json),
            };
            let plan = match catalog.plan(&name) {
                Ok(plan) => plan,
                Err(diagnostics) => {
                    let source = discovery::scenario(&catalog, scenario).source;
                    let inspection = CacheInspection {
                        inspection_version: 1,
                        source: Some(if source == discovery::SourceState::Missing {
                            SourceAvailability::Missing
                        } else {
                            SourceAvailability::Invalid
                        }),
                        cache: CacheState::Unknown,
                        key: None,
                        path: dataset_cache,
                        identity: None,
                        diagnostic: None,
                    };
                    return cache_output(
                        &inspection,
                        inspection_succeeded(&inspection),
                        diagnostics,
                        args.json,
                    );
                }
            };
            let binding = match fixture_binding(&scenario.fixture.definition(), &fixtures) {
                Ok(binding) => binding,
                Err(e) => return failure(e, args.json),
            };
            let inspection =
                dataset_cache::inspect(&plan.dataset_build_plan(), &dataset_cache, binding, verify);
            let diagnostics = inspection
                .diagnostic
                .as_ref()
                .map(|e| problem(&e.code, &e.message))
                .unwrap_or_default();
            cache_output(
                &inspection,
                inspection_succeeded(&inspection),
                diagnostics,
                args.json,
            )
        }
    }
}
fn inspection_succeeded(inspection: &CacheInspection) -> bool {
    match inspection.source {
        Some(SourceAvailability::Missing | SourceAvailability::Unbound) => true,
        Some(SourceAvailability::Invalid) => false,
        Some(SourceAvailability::Available) | None => match inspection.cache {
            CacheState::Missing | CacheState::Present | CacheState::Cached | CacheState::Busy => {
                true
            }
            CacheState::Invalid | CacheState::Incomplete | CacheState::Unknown => false,
        },
    }
}
fn cache_output<T: Serialize>(
    inspection: &T,
    ok: bool,
    diagnostics: Vec<Diagnostic>,
    json_output: bool,
) -> ExitCode {
    let output = if json_output {
        let inspection = match serde_json::to_value(inspection) {
            Ok(value) => value,
            Err(e) => return failure(problem("output_serialization_failed", e.to_string()), true),
        };
        print_json_success(
            &json!({"cli_output_version":1,"ok":ok,"value":inspection,"diagnostics":diagnostics}),
        )
    } else {
        success(inspection, false)
    };
    if ok { output } else { ExitCode::FAILURE }
}
fn fixture_binding<'a>(
    fixture: &GqtFixture,
    bindings: &'a [String],
) -> Result<Option<&'a str>, Vec<Diagnostic>> {
    let expected = match fixture {
        GqtFixture::Registered { .. } => true,
        GqtFixture::Dataset { .. } => false,
    };
    if !expected && !bindings.is_empty() {
        return Err(problem(
            "unexpected_fixture_binding",
            "GQT fixtures do not use --fixture bindings",
        ));
    }
    if bindings.len() > 1 {
        return Err(problem(
            "invalid_fixture_binding",
            "one scenario accepts at most one registered --fixture ID=BUNDLE",
        ));
    }
    Ok(bindings.first().map(String::as_str))
}

pub fn init(args: InitArgs) -> ExitCode {
    match generate(&args) {
        Ok(config) => success(&json!({"config":config,"scenario":args.name}), args.json),
        Err(e) => failure(e, args.json),
    }
}
fn generate(args: &InitArgs) -> Result<PathBuf, Vec<Diagnostic>> {
    let output = if args.output.is_absolute() {
        args.output.clone()
    } else {
        std::env::current_dir()
            .map_err(|e| problem("init_path_error", e.to_string()))?
            .join(&args.output)
    };
    if output.to_str().is_none() {
        return Err(problem("invalid_output_path", "output path must be UTF-8"));
    }
    let parent = output
        .parent()
        .ok_or_else(|| problem("init_path_error", "output has no parent"))?
        .canonicalize()
        .map_err(|e| problem("init_path_error", e.to_string()))?;
    let relative = |path: &Path| -> Result<PathBuf, Vec<Diagnostic>> {
        path.canonicalize()
            .map_err(|e| problem("source_path_error", e.to_string()))?
            .strip_prefix(&parent)
            .map(Path::to_path_buf)
            .map_err(|_| {
                problem(
                    "source_outside_root",
                    "place the output YAML in a directory containing both source files",
                )
            })
    };
    let fixture = relative(&args.fixture)?;
    let workload = relative(&args.workload)?;
    let source =
        gqt_case::read_source(&args.workload).map_err(|e| problem("invalid_workload", e))?;
    let case = source.parse().map_err(|e| problem("invalid_workload", e))?;
    let operation = gqt_case::workload_steps(&case)
        .map_err(|e| problem("invalid_workload", e))?
        .into_iter()
        .find(|s| s.ordinal == args.step)
        .ok_or_else(|| {
            problem(
                "unknown_operation",
                "step ordinal does not exist; use show --workload",
            )
        })?;
    let config = ConfigV2 {
        version: catalog::CONFIG_VERSION,
        defaults: Defaults::default(),
        scenarios: vec![Scenario {
            id: args.name.clone(),
            fixture: FixtureInput::Path(fixture),
            workload,
            measured_step: MeasuredStep {
                ordinal: args.step,
                text: operation.source,
            },
            repetitions: None,
            deadline_seconds: None,
            environment: None,
            protocol: ProtocolOverrides::default(),
        }],
        groups: BTreeMap::new(),
        run: vec![args.name.clone()],
    };
    let yaml = serde_yaml::to_string(&config)
        .map_err(|e| problem("config_serialization_error", e.to_string()))?;
    Catalog::from_source(&output, &yaml)?.resolve(None, None)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)
        .map_err(|e| problem("config_create_failed", format!("{}: {e}", output.display())))?;
    file.write_all(yaml.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|e| problem("config_write_failed", e.to_string()))?;
    Ok(output)
}

const CONFIG_HELP: &str = "Config version 2\n\nfixtures/ contains starting-state GQT; workloads/ contains operations and checks.\nbenchmarks.yaml defines scenarios once, optional groups of scenario IDs, and an explicit run list.\n\nrun NAME selects one scenario or group. run --config FILE uses only FILE's run list.\nWithout --config, explicit paths, filenames with a suffix, and existing local entries use the legacy case loader.\nUse ./NAME to force a case path; use --config to select a catalog name even if a local entry has that name.\nPaths inside YAML are relative to that YAML's directory and must stay inside it.\n--config never falls back if the file is missing or invalid.\n\nEach scenario needs id, fixture, workload and measured_step (ordinal plus exact operation text).\nA fixture is a GQT path, or {kind: registered, reference: ..., preparation: ...}.\nDefaults: repetitions=5, deadline_seconds=60, local APFS/clonefile on macOS or XFS/plain-copy elsewhere.\nEnvironment and protocol are expanded before identity: per-phase attribution, manual schedule, monotonic timer.\nScenario fields override defaults; run --repetitions overrides only sample quantity.\ndeadline_seconds: null disables the measurement deadline, not the supervisor watchdog.\nProtocol overrides are attribution, schedule, reset and timer; deadline_seconds is a scenario/default field.\n\nCopy benchmarks/custom.example.yaml, or generate a checked config:\n  omnigraph-bench show --workload benchmarks/workloads/tiny_read.gqt\n  omnigraph-bench init --fixture benchmarks/fixtures/tiny_graph.gqt --workload benchmarks/workloads/tiny_read.gqt --step 1 --output custom.yaml\n  omnigraph-bench run --config custom.yaml\n";
const CACHE_HELP: &str = "Cache inspection is read-only and never builds, restores, cleans or quarantines data.\n\n  omnigraph-bench cache status tiny-read --json\n  omnigraph-bench cache status tiny-read --verify --json\n  omnigraph-bench cache list --limit 100 --json\n\nThe default cache is target/gqt-datasets relative to cwd. Use the same --dataset-cache path for run and status.\nKeys bind recipe, engine/builder, backend/reset, cache location, required indexes and registered-source identity.\nHistorical variants may exist while the current scenario has a cache miss.\nSource: available, missing, invalid, unbound. Cache: missing, present, cached, busy, invalid, incomplete, unknown.\npresent checks published evidence; --verify audits full bytes before reporting cached.\nbusy returns immediately for a held lease. Missing or unbound sources give unknown cache status.\nA status is a snapshot, not a reservation. Cache missing is a successful observation; inability to inspect fails.\nrun builds missing fixtures unless --no-build is supplied. dataset build prepares fixtures explicitly.\nLegacy dataset validate acquires a lease and stages worker files; use cache status for read-only inspection.\n";

pub fn help(args: HelpArgs) -> ExitCode {
    let mut command = Cli::command();
    if args.json {
        if !args.topic.is_empty() {
            return failure(
                problem(
                    "invalid_help_topic",
                    "help --json describes all commands; omit the topic",
                ),
                true,
            );
        }
        command.build();
        return success(
            &json!({"config_version":catalog::CONFIG_VERSION,"cli_output_version":1,"runner_output_version":RUNNER_OUTPUT_VERSION,
                "commands":command_schema(&command),
                "required_combinations": {"show":"exactly one of NAME or --workload; --workload conflicts with --config", "run":"NAME or config run list; --dataset/--queries overrides require a legacy YAML case path", "init":"--fixture, --workload, --step and --output"},"config_help":CONFIG_HELP,"cache_help":CACHE_HELP,
                "output":{"inspection":"cli_output_version=1, ok, value, diagnostics","execution":"runner_output_version=2 with archive/partial-failure evidence","errors":"severity, code, path, message; JSON stdout only"},
                "effects":{"help/list/show/cache":"read-only; no database execution","init":"creates one new YAML file","run/dataset":"builds, restores or executes; run may publish to an explicitly selected archive"},
                "examples":["omnigraph-bench list fixtures --json","omnigraph-bench list workloads --json","omnigraph-bench list scenarios --json","omnigraph-bench show tiny-read --json","omnigraph-bench run tiny-read","omnigraph-bench run --config benchmarks/custom.example.yaml","omnigraph-bench cache status tiny-read --json"]
            }),
            true,
        );
    }
    match args.topic.as_slice() {
        [topic] if topic == "config" => {
            print!(
                "{CONFIG_HELP}\nExample:\n{}",
                include_str!("../../../../../benchmarks/custom.example.yaml")
            );
            ExitCode::SUCCESS
        }
        [topic] if topic == "cache" => {
            print!("{CACHE_HELP}");
            ExitCode::SUCCESS
        }
        topics => {
            for topic in topics {
                let Some(child) = command.find_subcommand(topic) else {
                    return failure(
                        problem(
                            "unknown_help_topic",
                            format!("unknown help topic '{topic}'"),
                        ),
                        false,
                    );
                };
                command = child.clone();
            }
            match command.print_long_help() {
                Ok(()) => {
                    println!();
                    ExitCode::SUCCESS
                }
                Err(e) => failure(problem("help_output_error", e.to_string()), false),
            }
        }
    }
}
fn command_schema(command: &clap::Command) -> Value {
    let mut printable = command.clone();
    let usage = printable.render_usage().to_string();
    json!({"name":command.get_name(),"usage":usage,
        "groups":command.get_groups().map(|g| json!({"id":g.get_id().as_str(),"required":g.is_required_set(),"args":g.get_args().map(|a|a.as_str()).collect::<Vec<_>>()})).collect::<Vec<_>>(),"about":command.get_about().map(ToString::to_string),
        "arguments":command.get_arguments().filter(|a| !a.is_hide_set()).map(|a| json!({
            "id":a.get_id().as_str(),"long":a.get_long(),"short":a.get_short(),"required":a.is_required_set(),
            "help":a.get_help().map(ToString::to_string),"action":format!("{:?}",a.get_action()),
            "defaults":a.get_default_values().iter().map(|v| v.to_string_lossy()).collect::<Vec<_>>(),
            "values":a.get_value_parser().possible_values().map(|values|values.map(|v| v.get_name().to_owned()).collect::<Vec<_>>()),
            "conflicts":command.get_arg_conflicts_with(a).iter().map(|v|v.get_id().as_str()).collect::<Vec<_>>()
        })).collect::<Vec<_>>(),
        "subcommands":command.get_subcommands().filter(|c| !c.is_hide_set()).map(command_schema).collect::<Vec<_>>()})
}
