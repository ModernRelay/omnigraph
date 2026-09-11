use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunnerConfig {
    pub(crate) timeout_ms: u64,
    pub(crate) environments: Vec<Environment>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub(crate) struct Environment {
    pub(crate) execution: Execution,
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(self).map_err(|_| std::fmt::Error)?)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "target", deny_unknown_fields)]
pub(crate) enum Execution {
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
pub(crate) enum Storage {
    LocalFilesystem,
    InMemoryObjectStore,
    S3Compatible,
    #[serde(rename = "azure-blob-storage")]
    AzureBlob,
}

impl Environment {
    pub(crate) fn matches(&self, target: Option<&str>, storage: Option<&str>) -> bool {
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

    pub(crate) fn seeds(&self) -> Vec<Option<u64>> {
        match &self.execution {
            Execution::Dst { seeds, .. } | Execution::ServerDst { seeds, .. } => {
                seeds.iter().copied().map(Some).collect()
            }
            _ => vec![None],
        }
    }

    pub(crate) fn admit(&self, has_faults: bool) -> Result<(), String> {
        match self.execution {
            Execution::Engine {
                storage: Storage::LocalFilesystem,
            } if !has_faults => Ok(()),
            Execution::Dst {
                storage: Storage::InMemoryObjectStore,
                ..
            } => {
                if cfg!(tokio_unstable) {
                    Ok(())
                } else {
                    Err("unsupported_environment: DST runner is unavailable in this build; build from crates/omnigraph-gqt".into())
                }
            }
            _ => Err(format!(
                "unsupported_environment: {} requests an unavailable combination; implemented combinations are omnigraph-engine/local-filesystem without faults and omnigraph-engine-dst/in-memory-object-store",
                self
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fault {
    pub(crate) at: String,
    pub(crate) occurrence: usize,
    pub(crate) action: FaultAction,
    pub(crate) scope: FaultScope,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct KnownFailure {
    pub(crate) step: usize,
    #[serde(rename = "match")]
    pub(crate) matcher: ErrorMatch,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "error", deny_unknown_fields)]
pub(crate) enum ErrorMatch {
    RecoveryRequired { reason: String },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FaultAction {
    ReturnError,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FaultScope {
    NextStep,
}

fn yaml<T: serde::de::DeserializeOwned>(body: &str) -> Result<T, String> {
    if body
        .split(|c: char| c.is_whitespace() || "[]{},:".contains(c))
        .any(|word| word.starts_with(['&', '*', '!']) || word == "<<")
    {
        return Err("invalid_case: YAML aliases, anchors, tags and merge keys are refused".into());
    }
    serde_yaml::from_str(body)
        .map_err(|error| format!("invalid_case: invalid runner configuration: {error}"))
}

pub(crate) fn parse_runner(body: &str) -> Result<RunnerConfig, String> {
    let config: RunnerConfig = yaml(body)?;
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

pub(crate) fn parse_fault(body: &str) -> Result<Fault, String> {
    let fault: Fault = yaml(body)?;
    if !(1..=1_000_000).contains(&fault.occurrence) {
        return Err("invalid_case: fault occurrence must be between 1 and 1000000".into());
    }
    Ok(fault)
}

pub(crate) fn parse_known_failure(body: &str) -> Result<KnownFailure, String> {
    let known_failure: KnownFailure = yaml(body)?;
    let ErrorMatch::RecoveryRequired { reason } = &known_failure.matcher;
    if known_failure.step == 0 || reason.trim().is_empty() || reason.len() > 2048 {
        return Err("invalid_case: known_failure requires a positive step and a nonempty RecoveryRequired reason (at most 2048 bytes)".into());
    }
    Ok(known_failure)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENGINE: &str = "timeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n";

    #[test]
    fn explicit_config_and_admission() {
        let config = parse_runner(ENGINE).unwrap();
        assert!(config.environments[0].admit(false).is_ok());
        assert!(config.environments[0].admit(true).is_err());
        let server = ENGINE.replace("omnigraph-engine", "omnigraph-server");
        assert!(
            parse_runner(&server).unwrap().environments[0]
                .admit(false)
                .is_err()
        );
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
    fn fault_fields_are_required_and_closed() {
        let fault = "at: branch_merge.post_authority_capture\noccurrence: 1\naction: return_error\nscope: next_step";
        assert!(parse_fault(fault).is_ok());
        for text in [
            fault.replace("scope: next_step", ""),
            fault.replace("next_step", "workload"),
            fault.replace("occurrence: 1", "occurrence: 0"),
            fault.replace("return_error", "panic"),
        ] {
            assert!(parse_fault(&text).is_err());
        }
    }
}
