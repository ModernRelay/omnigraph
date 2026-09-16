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

    pub(crate) fn admit(&self, has_seams: bool) -> Result<(), String> {
        match self.execution {
            Execution::Engine {
                storage: Storage::LocalFilesystem,
            } if !has_seams => Ok(()),
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
                "unsupported_environment: {} requests an unavailable combination; implemented combinations are omnigraph-engine/local-filesystem without seams and omnigraph-engine-dst/in-memory-object-store",
                self
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeamDirective {
    pub(crate) at: String,
    pub(crate) occurrence: usize,
    pub(crate) action: SeamAction,
    pub(crate) scope: SeamScope,
    /// A glob over the object's root-relative name; required on a store
    /// place, refused on an entry that declares no subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct KnownFailure {
    pub(crate) step: usize,
    #[serde(rename = "match")]
    pub(crate) matcher: ErrorMatch,
}

/// The typed failure a known-failure marker names: a `RecoveryRequired`
/// returned by a mutate step with its exact reason, or an `Internal`
/// manifest refusal from a restart whose message opens with `reason_prefix`
/// (the rest of that message carries a per-run operation id).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "error", deny_unknown_fields)]
pub(crate) enum ErrorMatch {
    RecoveryRequired { reason: String },
    Internal { reason_prefix: String },
}

/// What the site does when the installed decision fires; must match an
/// effect the seam declares in the engine's catalog, or, for a store action,
/// a store effect the entry declares or an action the store place admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SeamAction {
    Fail,
    Contention,
    Skip,
    Hold,
    /// A store action, spelled and parsed by `StoreAction` itself, so the
    /// store vocabulary has one home.
    Store(omnigraph_dst::store_places::StoreAction),
}

impl SeamAction {
    /// The spelling a case file uses.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SeamAction::Fail => "fail",
            SeamAction::Contention => "contention",
            SeamAction::Skip => "skip",
            SeamAction::Hold => "hold",
            SeamAction::Store(action) => action.as_str(),
        }
    }

    /// The store action this spelling names, or `None` for an engine action.
    pub(crate) fn store_action(self) -> Option<omnigraph_dst::store_places::StoreAction> {
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
            other => omnigraph_dst::store_places::StoreAction::parse(other)
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
pub(crate) enum SeamScope {
    NextStep,
}

/// `body` with every quoted scalar replaced by one space, so the token scan
/// below reads YAML syntax and never a quoted glob's own punctuation.
fn outside_quotes(body: &str) -> String {
    let chars: Vec<char> = body.chars().collect();
    let mut out = String::with_capacity(body.len());
    let mut at = 0;
    while at < chars.len() {
        let quote = chars[at];
        if quote != '"' && quote != '\'' {
            out.push(quote);
            at += 1;
            continue;
        }
        out.push(' ');
        at += 1;
        while at < chars.len() {
            if quote == '"' && chars[at] == '\\' {
                at += 2;
                continue;
            }
            if chars[at] == quote {
                if quote == '\'' && chars.get(at + 1) == Some(&'\'') {
                    at += 2;
                    continue;
                }
                at += 1;
                break;
            }
            at += 1;
        }
    }
    out
}

fn yaml<T: serde::de::DeserializeOwned>(body: &str) -> Result<T, String> {
    if outside_quotes(body)
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

pub(crate) fn parse_seam(body: &str) -> Result<SeamDirective, String> {
    let seam: SeamDirective = yaml(body)?;
    if !(1..=1_000_000).contains(&seam.occurrence) {
        return Err("invalid_case: seam occurrence must be between 1 and 1000000".into());
    }
    if seam.action == SeamAction::Hold {
        return Err(
            "invalid_case: `action: hold` is refused until a case can express two concurrent steps"
                .into(),
        );
    }
    if let Some(subject) = &seam.subject {
        omnigraph_dst::store_places::Subject::parse(subject)?;
    }
    Ok(seam)
}

pub(crate) fn parse_known_failure(body: &str) -> Result<KnownFailure, String> {
    let known_failure: KnownFailure = yaml(body)?;
    let text = match &known_failure.matcher {
        ErrorMatch::RecoveryRequired { reason } => reason,
        ErrorMatch::Internal { reason_prefix } => reason_prefix,
    };
    if known_failure.step == 0
        || text.trim().is_empty()
        || text.len() > omnigraph_dst::store_places::SUBJECT_MAX_BYTES
    {
        return Err("invalid_case: known_failure requires a positive step and a nonempty RecoveryRequired reason or Internal reason_prefix (at most 2048 bytes)".into());
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
        for body in [
            format!("{store}subject: *.json\n"),
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

    #[test]
    fn known_failure_accepts_both_matchers() {
        assert!(
            parse_known_failure(
                "step: 4\nmatch:\n  error: Internal\n  reason_prefix: \"OCC recovery sidecar '\""
            )
            .is_ok()
        );
        assert!(
            parse_known_failure("step: 4\nmatch:\n  error: Internal\n  reason_prefix: \"\"")
                .is_err()
        );
        assert!(parse_known_failure("step: 4\nmatch:\n  error: Unknown\n  reason: \"x\"").is_err());
    }
}
