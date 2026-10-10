//! Declared deployment evidence for a pre-provisioned, read-only server graph.
//! The receipt binds supplied facts; it does not remotely verify the deployment.

use crate::case::Backend;
use crate::dataset_identity::DATASET_LOGICAL_ALGORITHM;
use crate::gqt_case::{BoundGqt, PlannedGqt};
use crate::machine::{MachineIdentityV1, validate_machine_identity};
use crate::model::{digest, read_text_file, sha256_bytes, typed_sha256};
use crate::record::{
    EngineConfigurationV1, valid_lower_hex, validate_engine_configuration, validate_text,
};
use crate::runner::{is_lance_runtime_override, is_omnigraph_runtime_override};
use omnigraph_gqt_core::ServerTarget;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::Path;

pub const MAX_RECEIPT_BYTES: usize = 8 * 1024;
pub const RECEIPT_FORMAT_VERSION: u32 = 1;
const MAX_TOKEN_BYTES: usize = 4096;
const MAX_TOKEN_VARIABLE_NAME_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ServerArtifactV1 {
    Executable { sha256: String },
    Image { sha256: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerBuildAttestationV1 {
    pub package_version: String,
    pub source_commit: String,
    pub source_tree_dirty: bool,
    pub profile: String,
    pub cargo_opt_level: String,
    pub debug_assertions: bool,
    pub artifact: ServerArtifactV1,
    pub target_triple: Option<String>,
    pub rustc_version: Option<String>,
    pub engine: Option<EngineConfigurationV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerDatasetAttestationV1 {
    pub recipe_sha256: String,
    pub logical_content_sha256: String,
    pub algorithm: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerDeploymentReceiptV1 {
    pub format_version: u32,
    pub endpoint_sha256: String,
    pub graph: String,
    pub server: ServerBuildAttestationV1,
    pub backend: Backend,
    pub dataset: ServerDatasetAttestationV1,
    pub machine: Option<MachineIdentityV1>,
}

impl ServerDeploymentReceiptV1 {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text =
            read_text_file(path, MAX_RECEIPT_BYTES, "server receipt").map_err(|e| e.message)?;
        let receipt: Self = serde_json::from_str(&text)
            .map_err(|e| format!("invalid server receipt JSON: {e}"))?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), String> {
        let server = &self.server;
        let artifact = match &server.artifact {
            ServerArtifactV1::Executable { sha256 } | ServerArtifactV1::Image { sha256 } => sha256,
        };
        if self.format_version != RECEIPT_FORMAT_VERSION {
            return Err(format!(
                "server receipt format_version {} is unsupported; this build reads format {RECEIPT_FORMAT_VERSION}",
                self.format_version
            ));
        }
        if !digest(&self.endpoint_sha256)
            || !digest(artifact)
            || !digest(&self.dataset.recipe_sha256)
            || !digest(&self.dataset.logical_content_sha256)
            || !valid_lower_hex(&server.source_commit, &[40, 64])
            || server.source_tree_dirty
            || server.profile != "release"
            || server.cargo_opt_level != "2"
            || server.debug_assertions
            || !matches!(self.backend, Backend::LocalFs { .. })
            || self.dataset.algorithm != DATASET_LOGICAL_ALGORITHM
        {
            return Err("server receipt requires a clean release build and bound local-filesystem dataset evidence".into());
        }
        for (value, path) in [
            (Some(&self.graph), "graph"),
            (Some(&server.package_version), "server.package_version"),
            (Some(&self.dataset.algorithm), "dataset.algorithm"),
            (server.target_triple.as_ref(), "server.target_triple"),
            (server.rustc_version.as_ref(), "server.rustc_version"),
        ] {
            if let Some(value) = value {
                validate_text(value, path).map_err(|e| e.to_string())?;
            }
        }
        if self.graph.len() > 64
            || !self
                .graph
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || matches!(
                self.graph.as_str(),
                "policies" | "healthz" | "openapi" | "graphs"
            )
        {
            return Err("server receipt graph must be a valid server graph ID".into());
        }
        if let Some(engine) = &server.engine {
            validate_engine_configuration(engine).map_err(|e| format!("server.engine: {e}"))?;
        }
        if let Some(machine) = &self.machine {
            validate_machine_identity(machine).map_err(|e| e.to_string())?;
        }
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > MAX_RECEIPT_BYTES {
            return Err("server receipt re-serializes to more than 8 KiB; the receipt together with the observed client build and machine identity must fit 8 KiB".into());
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String, String> {
        self.validate()?;
        typed_sha256(self).map_err(|e| e.to_string())
    }

    pub fn bind(&self, plan: &PlannedGqt) -> Result<BoundGqt, String> {
        self.validate()?;
        if self.backend != plan.definition.environment.backend
            || self.dataset.recipe_sha256 != plan.recipe_sha256
        {
            return Err(
                "server receipt disagrees with the selected backend or dataset recipe".into(),
            );
        }
        plan.bind(
            &self.dataset.logical_content_sha256,
            &self.dataset.algorithm,
        )
    }
}

pub(crate) fn redact_error(mut message: String, token: Option<&str>) -> String {
    if let Some(token) = token {
        if !token.is_empty() {
            let json = serde_json::to_string(token).expect("string encoding is infallible");
            let escaped = &json[1..json.len() - 1];
            let encoded: String = url::form_urlencoded::byte_serialize(token.as_bytes()).collect();
            let percent = encoded.replace('+', "%20");
            for spelling in [escaped, encoded.as_str(), percent.as_str(), token] {
                message = message.replace(spelling, "<redacted>");
            }
        }
    }
    message
}

/// Credentials exist only in invocation memory and the bounded private stdin frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServedInput {
    pub target: ServerTarget,
    pub receipt: ServerDeploymentReceiptV1,
}

/// A refused served CLI input, addressed by the flag that supplied it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedInputError {
    pub path: &'static str,
    pub message: String,
}

impl ServedInput {
    /// Loads the receipt and token named by the served CLI flags and binds them to the URL and graph.
    pub fn from_cli(
        server: &str,
        graph: &str,
        receipt_path: &Path,
        token_variable: Option<&str>,
    ) -> Result<Self, ServedInputError> {
        let at = |path: &'static str| move |message: String| ServedInputError { path, message };
        let url = canonical_endpoint(server).map_err(at("server"))?;
        let receipt = ServerDeploymentReceiptV1::load(receipt_path).map_err(at("server-receipt"))?;
        let token = token_variable
            .map(read_token)
            .transpose()
            .map_err(at("server-token-env"))?;
        let input = Self {
            target: ServerTarget {
                url,
                graph: graph.into(),
                token,
            },
            receipt,
        };
        input.validate_graph().map_err(at("graph"))?;
        input.validate_endpoint().map_err(at("server"))?;
        Ok(input)
    }

    pub fn validate(&self, bound: &BoundGqt) -> Result<(), String> {
        self.receipt.validate()?;
        self.validate_endpoint()?;
        self.validate_graph()?;
        if self.receipt.bind(&bound.plan)? != *bound {
            return Err("server target, receipt and frozen point disagree".into());
        }
        if let Some(token) = &self.target.token {
            validate_token(token)?;
        }
        Ok(())
    }

    fn validate_endpoint(&self) -> Result<(), String> {
        let endpoint = canonical_endpoint(&self.target.url)?;
        if self.receipt.endpoint_sha256 != sha256_bytes(endpoint.as_bytes()) {
            return Err("deployment receipt does not bind the requested server URL".into());
        }
        Ok(())
    }

    fn validate_graph(&self) -> Result<(), String> {
        if self.receipt.graph != self.target.graph {
            return Err("deployment receipt does not bind the requested graph".into());
        }
        Ok(())
    }
}

fn read_token(name: &str) -> Result<String, String> {
    validate_token_environment_name(name)?;
    let value = std::env::var(name)
        .map_err(|_| format!("token environment variable {name} is absent or not UTF-8"))?;
    validate_token(&value)?;
    Ok(value)
}

pub(crate) fn validate_token_environment_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("token variable name is empty".into());
    }
    if name.len() > MAX_TOKEN_VARIABLE_NAME_BYTES {
        return Err(format!(
            "token variable name exceeds {MAX_TOKEN_VARIABLE_NAME_BYTES} bytes"
        ));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(
            "token variable name may contain only ASCII letters, digits and underscores".into(),
        );
    }
    if is_omnigraph_runtime_override(OsStr::new(name)) || is_lance_runtime_override(OsStr::new(name))
    {
        return Err("token variable must use a separate name outside the LANCE_ and OMNIGRAPH_ runtime namespaces".into());
    }
    Ok(())
}

fn validate_token(token: &str) -> Result<(), String> {
    if token.is_empty() || token.len() > MAX_TOKEN_BYTES || token.chars().any(char::is_control) {
        return Err("invalid bounded bearer token".into());
    }
    Ok(())
}

pub fn canonical_endpoint(value: &str) -> Result<String, String> {
    if value.len() > 2048 {
        return Err("server URL exceeds its bound".into());
    }
    let mut url = url::Url::parse(value).map_err(|e| format!("invalid server URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "server URL requires HTTP(S), a host and no credentials, query or fragment".into(),
        );
    }
    if url
        .path_segments()
        .is_some_and(|mut segments| segments.any(|segment| segment == "graphs"))
    {
        return Err("the server URL is the server base, not a graph path".into());
    }
    let path = url.path().trim_end_matches('/').to_owned();
    url.set_path(&path);
    Ok(url.as_str().trim_end_matches('/').to_owned())
}
