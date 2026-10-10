use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunnerConfig {
    pub timeout_ms: u64,
    pub environments: Vec<Environment>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct Environment {
    pub execution: Execution,
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(self).map_err(|_| std::fmt::Error)?)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "target", deny_unknown_fields)]
pub enum Execution {
    #[serde(rename = "omnigraph-engine")]
    Engine { storage: Storage },
    #[serde(rename = "omnigraph-engine-dst")]
    Dst { storage: Storage, seeds: Vec<u64> },
    #[serde(rename = "omnigraph-server")]
    Server { storage: Storage },
    #[serde(rename = "omnigraph-server-dst")]
    ServerDst { storage: Storage, seeds: Vec<u64> },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum Storage {
    LocalFilesystem,
    InMemoryObjectStore,
    S3Compatible,
    #[serde(rename = "azure-blob-storage")]
    AzureBlob,
}

impl Environment {
    pub fn admit_store(&self, needs_dst: bool, store: Option<&str>) -> Result<(), String> {
        let Some(uri) = store else {
            return self.admit(needs_dst);
        };
        let Execution::Engine { storage } = self.execution else {
            return Err("unsupported_environment: --store requires direct engine execution without DST or server targets".into());
        };
        if needs_dst {
            return Err(
                "unsupported_environment: --store cannot execute seams or concurrent blocks".into(),
            );
        }
        if !["file://", "s3://", "az://"]
            .iter()
            .any(|prefix| uri.starts_with(prefix))
        {
            return Err("invalid_case: --store requires a file://, s3:// or az:// URI".into());
        }
        let normalized = omnigraph::storage::normalize_root_uri(uri)
            .map_err(|e| format!("invalid_case: --store: {e}"))?;
        let actual = match omnigraph::storage::storage_kind_for_uri(&normalized)
            .map_err(|e| format!("invalid_case: --store: {e}"))?
        {
            omnigraph::storage::StorageKind::Local => Storage::LocalFilesystem,
            omnigraph::storage::StorageKind::S3 => Storage::S3Compatible,
            omnigraph::storage::StorageKind::Azure => Storage::AzureBlob,
        };
        if storage != actual {
            return Err(format!(
                "unsupported_environment: --store backend does not match declared environment {self}"
            ));
        }
        Ok(())
    }

    pub fn matches(&self, target: Option<&str>, storage: Option<&str>) -> bool {
        let (actual_target, actual_storage) = match self.execution {
            Execution::Engine { storage } => ("omnigraph-engine", storage),
            Execution::Dst { storage, .. } => ("omnigraph-engine-dst", storage),
            Execution::Server { storage } => ("omnigraph-server", storage),
            Execution::ServerDst { storage, .. } => ("omnigraph-server-dst", storage),
        };
        let actual_storage = match actual_storage {
            Storage::LocalFilesystem => "local-filesystem",
            Storage::InMemoryObjectStore => "in-memory-object-store",
            Storage::S3Compatible => "s3-compatible",
            Storage::AzureBlob => "azure-blob-storage",
        };
        target.is_none_or(|value| value == actual_target)
            && storage.is_none_or(|value| value == actual_storage)
    }

    pub fn seeds(&self) -> Vec<Option<u64>> {
        match &self.execution {
            Execution::Dst { seeds, .. } | Execution::ServerDst { seeds, .. } => {
                seeds.iter().copied().map(Some).collect()
            }
            _ => vec![None],
        }
    }

    /// Admission under `--server`: only an `omnigraph-server` environment
    /// runs against the named server, with any declared storage (the server
    /// reports no backend to check it against, so the declaration is
    /// recorded, not verified); `omnigraph-server-dst` stays unimplemented.
    pub fn admit_served(&self) -> Result<(), String> {
        match self.execution {
            Execution::Server { .. } => Ok(()),
            Execution::Engine { .. } | Execution::Dst { .. } => Err(format!(
                "unsupported_environment: --server runs only omnigraph-server environments, not {self}"
            )),
            Execution::ServerDst { .. } => Err(format!(
                "unsupported_environment: {self} requests an unavailable combination; --server implements omnigraph-server only"
            )),
        }
    }

    /// Whether the environment is served by an `omnigraph-server` process,
    /// which only a `--server` invocation can reach.
    pub fn is_served(&self) -> bool {
        matches!(
            self.execution,
            Execution::Server { .. } | Execution::ServerDst { .. }
        )
    }

    /// `needs_dst` is `Case::needs_dst`: what only the DST runner can host.
    pub fn admit(&self, needs_dst: bool) -> Result<(), String> {
        match self.execution {
            Execution::Engine {
                storage: Storage::LocalFilesystem,
            } if !needs_dst => Ok(()),
            Execution::Dst {
                storage: Storage::InMemoryObjectStore,
                ..
            } => {
                if cfg!(tokio_unstable) {
                    Ok(())
                } else {
                    Err("unsupported_environment: DST runner is unavailable in this build; the workspace .cargo/config.toml sets --cfg tokio_unstable, an env RUSTFLAGS without it overrides that".into())
                }
            }
            _ => Err(format!(
                "unsupported_environment: {} requests an unavailable combination; implemented combinations are omnigraph-engine/local-filesystem without seams or concurrent blocks and omnigraph-engine-dst/in-memory-object-store",
                self
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SeamDirective {
    pub at: String,
    pub occurrence: usize,
    pub action: SeamAction,
    pub scope: SeamScope,
    /// A glob over the object's root-relative name; required on a store
    /// place, refused on an entry that declares no subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

/// What the site does when the installed decision fires; must match an
/// effect the seam declares in the engine's catalog, or, for a store action,
/// a store effect the entry declares or an action the store place admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeamAction {
    Fail,
    Contention,
    Skip,
    Hold,
    /// A store action, spelled and parsed by `StoreAction` itself, so the
    /// store vocabulary has one home.
    Store(omnigraph::seams::store::StoreAction),
}

impl SeamAction {
    /// The spelling a case file uses.
    pub fn as_str(self) -> &'static str {
        match self {
            SeamAction::Fail => "fail",
            SeamAction::Contention => "contention",
            SeamAction::Skip => "skip",
            SeamAction::Hold => "hold",
            SeamAction::Store(action) => action.as_str(),
        }
    }

    /// The store action this spelling names, or `None` for an engine action.
    pub fn store_action(self) -> Option<omnigraph::seams::store::StoreAction> {
        match self {
            SeamAction::Store(action) => Some(action),
            SeamAction::Fail | SeamAction::Contention | SeamAction::Skip | SeamAction::Hold => None,
        }
    }
}

impl Serialize for SeamAction {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SeamAction {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let spelling = String::deserialize(deserializer)?;
        match spelling.as_str() {
            "fail" => Ok(SeamAction::Fail),
            "contention" => Ok(SeamAction::Contention),
            "skip" => Ok(SeamAction::Skip),
            "hold" => Ok(SeamAction::Hold),
            other => omnigraph::seams::store::StoreAction::parse(other)
                .map(SeamAction::Store)
                .ok_or_else(|| {
                    <D::Error as serde::de::Error>::custom(format!(
                        "unknown action `{other}`; engine actions are fail, contention, skip, hold; store actions are misdirect, lose, error, corrupt, delay"
                    ))
                }),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SeamScope {
    NextStep,
}

pub(crate) use crate::yaml::yaml;

pub fn parse_runner(body: &str) -> Result<RunnerConfig, String> {
    let config: RunnerConfig = yaml(body, "runner configuration")?;
    if !(1..=600_000).contains(&config.timeout_ms) {
        return Err("invalid_case: timeout_ms must be between 1 and 600000".into());
    }
    if !(1..=16).contains(&config.environments.len()) {
        return Err("invalid_case: environments must contain between 1 and 16 entries".into());
    }
    let mut environments = BTreeSet::new();
    for env in &config.environments {
        if !environments.insert(env) {
            return Err(format!(
                "invalid_case: duplicate environment parameters: {env}"
            ));
        }
        if let Execution::Dst { seeds, .. } | Execution::ServerDst { seeds, .. } = &env.execution {
            if !(1..=64).contains(&seeds.len())
                || seeds.iter().collect::<BTreeSet<_>>().len() != seeds.len()
            {
                return Err("invalid_case: DST seeds require 1 to 64 distinct u64 values".into());
            }
        }
    }
    Ok(config)
}

pub fn parse_seam(body: &str) -> Result<SeamDirective, String> {
    let seam: SeamDirective = yaml(body, "seam directive")?;
    if !(1..=1_000_000).contains(&seam.occurrence) {
        return Err("invalid_case: seam occurrence must be between 1 and 1000000".into());
    }
    if seam.action == SeamAction::Hold {
        return Err(
            "invalid_case: `action: hold` is refused a named interleaving is a `--- concurrent` block, whose `park` entry holds a session at a store request; `hold` on a seam is not supported"
                .into(),
        );
    }
    if let Some(subject) = &seam.subject {
        omnigraph::seams::store::Subject::parse(subject)?;
    }
    Ok(seam)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_uses_syntax_tokens_and_refuses_duplicate_keys_recursively() {
        for text in [
            "name: it's Alice & Bob",
            "name: <<",
            "\"<<\": value",
            "name: '&literal *literal !literal <<'",
            "name: \"escaped \\\" &literal\"",
            "name: 'it''s &literal'",
            "name: |-\n  &literal\n\n  *literal !literal <<\n",
            "name: >-\n  &literal\n  *literal\n",
            "a: {key: 1}\nb: {key: 2}",
            "\"true\": 1\n\"1\": 2\n\"null\": 3",
            "a: [true, 1, null, {b: ~}]",
        ] {
            assert!(
                yaml::<serde_yaml::Value>(text, "test body").is_ok(),
                "{text}"
            );
        }
        for text in [
            "a: 1\na: 2",
            "columns: {n: 1, n: 2}",
            "rows: [{a: {b: 1, b: 2}}]",
            "\"true\": 1\ntrue: 2",
            "\"1\": a\n1: b",
            "true: 1",
            "1: a",
            "~: a",
            "a: {1: b}",
            "a: [{true: b}]",
            "a: &unused 1",
            "a: &unused [1]",
            "a: &unused {b: 1}",
            "name: it's Alice\na: &x 1\nb: *x",
            "a: !custom value",
            "a: !!str value",
            "a: !<tag:example.org,2026:v> value",
            "%TAG !e! tag:example.org,2026:\n---\na: value",
            "a: {<<: {b: 1}}",
        ] {
            assert!(
                yaml::<serde_yaml::Value>(text, "test body").is_err(),
                "{text}"
            );
        }
        assert_eq!(
            yaml::<serde_yaml::Value>("true: 1", "test body").unwrap_err(),
            "invalid_case: YAML mapping keys must be strings; `true` reads as a boolean, quote it"
        );
        assert!(
            yaml::<serde_yaml::Value>("a: 1\na: 2", "generated recipe")
                .unwrap_err()
                .starts_with("invalid_case: invalid generated recipe: ")
        );
    }
    #[test]
    fn external_store_admission_matches_declared_backend() {
        for (storage, uri) in [
            (Storage::LocalFilesystem, "file:///tmp/graph"),
            (Storage::S3Compatible, "s3://bucket/graph"),
            (Storage::AzureBlob, "az://container/graph"),
        ] {
            let env = Environment {
                execution: Execution::Engine { storage },
            };
            assert!(env.admit_store(false, Some(uri)).is_ok());
            assert!(env.admit_store(true, Some(uri)).is_err());
            assert!(env.admit_store(false, Some("memory://graph")).is_err());
            for other in [
                Storage::LocalFilesystem,
                Storage::S3Compatible,
                Storage::AzureBlob,
                Storage::InMemoryObjectStore,
            ] {
                if storage != other {
                    let other = Environment {
                        execution: Execution::Engine { storage: other },
                    };
                    assert!(other.admit_store(false, Some(uri)).is_err());
                }
            }
            if storage != Storage::LocalFilesystem {
                assert!(env.admit_store(false, None).is_err());
            }
        }
        for execution in [
            Execution::Dst {
                storage: Storage::InMemoryObjectStore,
                seeds: vec![0],
            },
            Execution::ServerDst {
                storage: Storage::InMemoryObjectStore,
                seeds: vec![0],
            },
            Execution::Server {
                storage: Storage::LocalFilesystem,
            },
        ] {
            assert!(
                Environment { execution }
                    .admit_store(false, Some("file:///tmp/graph"))
                    .unwrap_err()
                    .contains("--store requires direct engine")
            );
        }
    }

    const ENGINE: &str = "timeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n";

    #[test]
    fn explicit_config_and_admission() {
        let config = parse_runner(ENGINE).unwrap();
        assert!(config.environments[0].admit(false).is_ok());
        assert!(config.environments[0].admit(true).is_err());
        let server = parse_runner(&ENGINE.replace("omnigraph-engine", "omnigraph-server")).unwrap();
        assert!(server.environments[0].admit(false).is_err());
        assert!(server.environments[0].admit_served().is_ok());
        assert!(server.environments[0].is_served());
        assert!(!config.environments[0].is_served());
        assert!(
            config.environments[0]
                .admit_served()
                .unwrap_err()
                .contains("--server runs only omnigraph-server environments")
        );
        let server_dst = parse_runner(
            &(ENGINE.replace("omnigraph-engine", "omnigraph-server-dst") + "    seeds: [0]\n"),
        )
        .unwrap();
        assert!(server_dst.environments[0].is_served());
        assert!(server_dst.environments[0].admit_served().is_err());
        let dst = ENGINE
            .replace("omnigraph-engine", "omnigraph-engine-dst")
            .replace("local-filesystem", "in-memory-object-store")
            + "    seeds: [0, 42]\n";
        assert_eq!(
            parse_runner(&dst).unwrap().environments[0].seeds(),
            vec![Some(0), Some(42)]
        );
    }

    #[test]
    fn refuses_missing_unknown_duplicate_and_inapplicable_fields() {
        for body in [
            ENGINE.replace("timeout_ms: 10000\n", ""),
            ENGINE.replace("    storage: local-filesystem\n", ""),
            ENGINE.replace("  - target: omnigraph-engine\n", "  - "),
            format!("version: 1\n{ENGINE}"),
            format!("{ENGINE}    id: engine\n"),
            ENGINE.replace("10000", "0"),
            ENGINE.replace("10000", "600001"),
            ENGINE.replace("target:", "runtime:"),
            format!("{ENGINE}    seeds: [0]\n"),
            format!("{ENGINE}    target: omnigraph-engine\n"),
            ENGINE.replace(
                "target: omnigraph-engine",
                "target: &alias omnigraph-engine",
            ),
            ENGINE.replace("target: omnigraph-engine", "target: *alias"),
            ENGINE.replace("target: omnigraph-engine", "target: !str omnigraph-engine"),
            ENGINE.replace("environments:", "environments: []\nignored:"),
        ] {
            assert!(parse_runner(&body).is_err(), "accepted {body}");
        }
    }

    #[test]
    fn seed_and_environment_bounds() {
        let dst = ENGINE.replace("omnigraph-engine", "omnigraph-engine-dst");
        for seeds in [
            "[]".into(),
            "[0, 0]".into(),
            "[-1]".into(),
            format!("{:?}", (0..65).collect::<Vec<_>>()),
        ] {
            assert!(parse_runner(&format!("{dst}    seeds: {seeds}\n")).is_err());
        }
        let entry = ENGINE.split_once("environments:\n").unwrap().1;
        assert!(parse_runner(&format!("{ENGINE}{entry}")).is_err());
        let first = format!("{dst}    seeds: [0]\n");
        let other = first
            .split_once("environments:\n")
            .unwrap()
            .1
            .replace("[0]", "[42]");
        assert_eq!(
            parse_runner(&format!("{first}{other}"))
                .unwrap()
                .environments
                .len(),
            2
        );
    }

    #[test]
    fn seam_fields_are_required_and_closed() {
        let seam = "at: branch_merge.post_authority_capture\noccurrence: 1\naction: fail\nscope: next_step";
        assert!(parse_seam(seam).is_ok());
        assert!(parse_seam(&seam.replace("action: fail", "action: skip")).is_ok());
        let contention = parse_seam(&seam.replace("action: fail", "action: contention")).unwrap();
        assert_eq!(contention.action, SeamAction::Contention);
        assert_eq!(contention.action.as_str(), "contention");
        for text in [
            seam.replace("scope: next_step", ""),
            seam.replace("next_step", "workload"),
            seam.replace("occurrence: 1", "occurrence: 0"),
            seam.replace("action: fail", "action: return_error"),
            seam.replace("action: fail", "action: panic"),
            seam.replace("action: fail", "action: hold"),
        ] {
            assert!(parse_seam(&text).is_err());
        }
    }

    #[test]
    fn quoted_subjects_are_scalars_not_yaml_syntax() {
        let store = "at: storage.put\noccurrence: 1\naction: misdirect\nscope: next_step\n";
        for subject in [
            "subject: \"__recovery/{*.json,*.bin}\"\n",
            "subject: \"__recovery/[!x]*\"\n",
            "subject: '__recovery/*'\n",
        ] {
            let body = format!("{store}{subject}");
            assert!(parse_seam(&body).is_ok(), "refused {body}");
        }
        assert!(
            parse_seam(&format!("{store}subject: *.json\n"))
                .unwrap_err()
                .starts_with("invalid_case:")
        );
        for body in [
            format!("{store}subject: \"__recovery/*\"\n")
                .replace("at: storage.put", "at: &anchor storage.put"),
            format!("{store}subject: \"__recovery/*\"\n")
                .replace("scope: next_step", "scope: !tag next_step"),
        ] {
            assert_eq!(
                parse_seam(&body).unwrap_err(),
                "invalid_case: YAML aliases, anchors, tags and merge keys are refused",
                "accepted {body}"
            );
        }
    }

    #[test]
    fn nested_brace_subject_is_refused_not_a_panic() {
        let store = "at: storage.put\noccurrence: 1\naction: misdirect\nscope: next_step\n";
        let subject = "{".repeat(300) + "x" + &"}".repeat(300);
        let error = parse_seam(&format!("{store}subject: \"{subject}\"\n")).unwrap_err();
        assert!(error.starts_with("invalid_case:"), "{error}");
    }
}
