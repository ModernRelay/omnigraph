use crate::catalog::{Catalog, ConfigV2, FixtureInput};
use crate::discovery::{self, SourceState};
use std::path::Path;

fn setup() -> (tempfile::TempDir, Catalog) {
    let directory = tempfile::tempdir().unwrap();
    for name in ["fixtures", "workloads"] {
        std::fs::create_dir(directory.path().join(name)).unwrap();
    }
    std::fs::write(
        directory.path().join("fixtures/tiny_graph.gqt"),
        include_str!("../../../benchmarks/fixtures/tiny_graph.gqt"),
    )
    .unwrap();
    std::fs::write(
        directory.path().join("workloads/tiny_read.gqt"),
        include_str!("../../../benchmarks/workloads/tiny_read.gqt"),
    )
    .unwrap();
    let source = include_str!("../../../benchmarks/custom.example.yaml");
    let path = directory.path().join("benchmarks.yaml");
    std::fs::write(&path, source).unwrap();
    let catalog = Catalog::load(&path).unwrap();
    (directory, catalog)
}
fn changed(catalog: &Catalog, config: &ConfigV2) -> Result<Catalog, Vec<crate::Diagnostic>> {
    Catalog::from_source(&catalog.path, &serde_yaml::to_string(config).unwrap())
}

#[test]
fn strict_config_rejects_unknown_versions_fields_and_duplicate_yaml_keys() {
    let (_directory, catalog) = setup();
    let source = std::fs::read_to_string(&catalog.path).unwrap();
    for changed in [
        source.replacen("version: 2", "version: 3", 1),
        format!("{source}\nunknown: true\n"),
        format!("{source}\nversion: 2\n"),
        format!("{source}\ngroups:\n  small: [custom-tiny-read]\n  small: [custom-tiny-read]\n"),
    ] {
        assert!(Catalog::from_source(&catalog.path, &changed).is_err());
    }
}

#[test]
fn selection_is_explicit_unique_and_does_not_load_unselected_sources() {
    let (_directory, catalog) = setup();
    let mut config = catalog.definition.clone();
    let mut absent = config.scenarios[0].clone();
    absent.id = "absent".into();
    absent.fixture = FixtureInput::Path("fixtures/not-here.gqt".into());
    config.scenarios.push(absent);
    config.run.clear();
    let explicit = changed(&catalog, &config).unwrap();
    assert!(explicit.resolve(None, None).is_err());
    assert_eq!(
        explicit
            .resolve(Some("custom-tiny-read"), None)
            .unwrap()
            .runs
            .len(),
        1
    );
    assert!(explicit.resolve(Some("absent"), None).is_err());
    assert!(explicit.resolve(Some("typo"), None).is_err());
    config
        .groups
        .insert("small".into(), vec!["custom-tiny-read".into()]);
    config.run = vec!["small".into(), "custom-tiny-read".into()];
    assert!(changed(&catalog, &config).is_err());
    config.run.clear();
    config
        .groups
        .insert("custom-tiny-read".into(), vec!["absent".into()]);
    assert!(changed(&catalog, &config).is_err());
    config.groups.remove("custom-tiny-read");
    config
        .groups
        .insert("unknown".into(), vec!["missing".into()]);
    assert!(changed(&catalog, &config).is_err());
    config.groups.clear();
    config.scenarios[1].id = "custom-tiny-read".into();
    assert!(changed(&catalog, &config).is_err());
}

#[test]
fn defaults_and_explicit_null_resolve_before_identity() {
    let (_directory, catalog) = setup();
    let mut config = catalog.definition.clone();
    config.defaults.deadline_seconds = Some(Some(120));
    config.defaults.repetitions = Some(7);
    for (deadline, expected) in [
        (None, Some(120)),
        (Some(None), None),
        (Some(Some(30)), Some(30)),
    ] {
        config.scenarios[0].deadline_seconds = deadline;
        config.scenarios[0].repetitions = Some(2);
        let changed = changed(&catalog, &config).unwrap();
        let suite = changed.resolve(None, None).unwrap();
        let plan = suite.runs[0].case.gqt().unwrap();
        assert_eq!(plan.definition.protocol.deadline_seconds, expected);
        assert_eq!(suite.runs[0].repetitions, 2);
        let overridden = changed.resolve(None, Some(3)).unwrap();
        assert_eq!(overridden.runs[0].repetitions, 3);
        assert_eq!(
            overridden.runs[0].case.planned_identity(),
            plan.planned_sha256
        );
    }
}

#[test]
fn repetition_budget_precedes_source_loading() {
    let (_directory, catalog) = setup();
    for repetitions in [0, 10001] {
        assert!(catalog.resolve(None, Some(repetitions)).is_err());
    }
    let mut config = catalog.definition.clone();
    config.scenarios.clear();
    config.run.clear();
    for index in 0..11 {
        let mut scenario = catalog.definition.scenarios[0].clone();
        scenario.id = format!("scenario-{index}");
        scenario.fixture = FixtureInput::Path("fixtures/missing.gqt".into());
        scenario.repetitions = Some(10000);
        config.run.push(scenario.id.clone());
        config.scenarios.push(scenario);
    }
    let changed = changed(&catalog, &config).unwrap();
    assert_eq!(
        changed.resolve(None, None).unwrap_err()[0].code,
        "repetition_budget_exceeded"
    );
}

#[test]
fn moved_catalogs_keep_identity_and_aliases_cannot_duplicate_execution() {
    let (directory, catalog) = setup();
    let before = catalog.plan("custom-tiny-read").unwrap();
    let mut config = catalog.definition.clone();
    let mut alias = config.scenarios[0].clone();
    alias.id = "alias".into();
    config.scenarios.push(alias);
    config.run.push("alias".into());
    let alias = changed(&catalog, &config).unwrap();
    assert_eq!(
        alias.resolve(None, None).unwrap_err()[0].code,
        "duplicate_planned_identity"
    );
    assert!(alias.resolve(Some("alias"), None).is_ok());
    let moved_parent = tempfile::tempdir().unwrap();
    let moved = moved_parent.path().join("catalog");
    std::fs::rename(directory.path(), &moved).unwrap();
    let after = Catalog::load(&moved.join("benchmarks.yaml"))
        .unwrap()
        .plan("custom-tiny-read")
        .unwrap();
    assert_eq!(before.planned_sha256, after.planned_sha256);
    let logical = "a".repeat(64);
    let algorithm = crate::dataset_identity::DATASET_LOGICAL_ALGORITHM;
    assert_eq!(
        before.bind(&logical, algorithm).unwrap().point_id,
        after.bind(&logical, algorithm).unwrap().point_id
    );
}

#[test]
fn inventory_retains_missing_invalid_and_unselected_sources() {
    let (directory, catalog) = setup();
    std::fs::copy(
        directory.path().join("fixtures/tiny_graph.gqt"),
        directory.path().join("fixtures/unselected.gqt"),
    )
    .unwrap();
    std::fs::remove_file(directory.path().join("fixtures/tiny_graph.gqt")).unwrap();
    std::fs::write(directory.path().join("workloads/tiny_read.gqt"), "not GQT").unwrap();
    let fixtures = discovery::sources(&catalog, true).unwrap();
    assert_eq!(fixtures.entries.len(), 2);
    assert_eq!(fixtures.entries[0].source, SourceState::Missing);
    assert_eq!(fixtures.entries[1].source, SourceState::Available);
    assert!(fixtures.entries[1].scenarios.is_empty());
    let scenarios = discovery::scenarios(&catalog);
    assert_eq!(scenarios.entries[0].source, SourceState::Invalid);
    assert!(scenarios.entries[0].diagnostics.len() >= 2);
}

#[test]
#[cfg(unix)]
fn config_and_source_paths_cannot_escape_or_block_on_special_files() {
    use std::os::unix::fs::symlink;
    let (directory, catalog) = setup();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(
        outside.path().join("config.yaml"),
        std::fs::read(&catalog.path).unwrap(),
    )
    .unwrap();
    symlink(
        outside.path().join("config.yaml"),
        directory.path().join("alias.yaml"),
    )
    .unwrap();
    assert!(Catalog::load(&directory.path().join("alias.yaml")).is_err());
    let source = directory.path().join("fixtures/tiny_graph.gqt");
    std::fs::rename(&source, outside.path().join("fixture.gqt")).unwrap();
    symlink(outside.path().join("fixture.gqt"), &source).unwrap();
    assert!(catalog.plan("custom-tiny-read").is_err());
    let mut config = catalog.definition.clone();
    for path in [Path::new("../escape.gqt"), Path::new("/escape.gqt")] {
        config.scenarios[0].fixture = FixtureInput::Path(path.into());
        assert!(changed(&catalog, &config).is_err());
    }
    let fifo = directory.path().join("fifo.yaml");
    nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRUSR).unwrap();
    assert!(Catalog::load(&fifo).is_err());
}

#[test]
#[cfg(unix)]
fn references_and_gqt_fifos_are_refused_by_the_shared_bounded_reader() {
    let directory = tempfile::tempdir().unwrap();
    let fifo = directory.path().join("source");
    nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRUSR).unwrap();
    assert!(crate::model::read_yaml_file(&fifo, "reference").is_err());
    assert!(crate::gqt_case::read_source(&fifo).is_err());
}

#[test]
fn workload_descriptor_inspection_refuses_excessive_expansion_before_enumeration() {
    let template = include_str!("../../../benchmarks/workloads/tiny_read.gqt");
    let header = template.split("--- query branch: main").next().unwrap();
    let source = format!(
        "{header}{}",
        "--- restart\n".repeat(crate::gqt_case::MAX_EXPANDED_STEPS + 1)
    );
    let case = omnigraph_gqt_core::parse_case("many_steps", &source).unwrap();
    assert!(crate::gqt_case::workload_steps(&case).is_err());
}
