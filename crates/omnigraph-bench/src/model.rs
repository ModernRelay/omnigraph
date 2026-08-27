use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_YAML_BYTES: usize = 1024 * 1024;

/// Severity of one machine-readable validation diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
}

/// One stable, path-addressed parser or semantic-validation diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Diagnostic {
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub path: String,
    pub message: String,
}

impl Diagnostic {
    pub fn error(
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            severity: DiagnosticSeverity::Error,
            code: code.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

/// Result of strict loading and semantic validation.
///
/// Invalid input never produces a usable value. Keeping diagnostics as data
/// lets the CLI render the same facts as human text or JSON without parsing an
/// error string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ValidationOutcome<T> {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<T>,
    pub diagnostics: Vec<Diagnostic>,
}

impl<T> ValidationOutcome<T> {
    pub fn success(value: T) -> Self {
        Self {
            ok: true,
            value: Some(value),
            diagnostics: Vec::new(),
        }
    }

    pub fn failure(diagnostics: Vec<Diagnostic>) -> Self {
        debug_assert!(!diagnostics.is_empty());
        Self {
            ok: false,
            value: None,
            diagnostics,
        }
    }

    pub fn into_result(self) -> Result<T, Vec<Diagnostic>> {
        match self.value {
            Some(value) if self.ok => Ok(value),
            _ => Err(self.diagnostics),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionHeader {
    version: u32,
}

/// Read only the common version header before dispatching to a strict format.
///
/// `serde_yaml` reports a duplicate `version` field here. Unknown fields are
/// intentionally rejected only by the selected full-format DTO.
pub(crate) fn declared_version(source: &str, noun: &str) -> Result<u32, Diagnostic> {
    if source.len() > MAX_YAML_BYTES {
        return Err(Diagnostic::error(
            format!("{noun}_yaml_too_large"),
            "$",
            format!("{noun} YAML must be <= {MAX_YAML_BYTES} bytes"),
        ));
    }
    // A value projection avoids accepting a future document through a V1 DTO,
    // while the one-field wrapper keeps duplicate/missing version failures
    // typed. Deserialize into Value first because VersionHeader is strict.
    let value: serde_yaml::Value = serde_yaml::from_str(source).map_err(|error| {
        Diagnostic::error(
            format!("invalid_{noun}_yaml"),
            "$",
            format!("could not parse {noun} YAML: {error}"),
        )
    })?;
    let Some(version_value) = value.get("version") else {
        return Err(Diagnostic::error(
            format!("invalid_{noun}_yaml"),
            "version",
            "required field `version` is missing",
        ));
    };
    let header_value = serde_yaml::Value::Mapping(serde_yaml::Mapping::from_iter([(
        serde_yaml::Value::String("version".to_string()),
        version_value.clone(),
    )]));
    let header: VersionHeader = serde_yaml::from_value(header_value).map_err(|error| {
        Diagnostic::error(
            format!("invalid_{noun}_yaml"),
            "version",
            format!("invalid `{noun}.version`: {error}"),
        )
    })?;
    Ok(header.version)
}

pub(crate) fn strict_yaml<T>(source: &str, noun: &str) -> Result<T, Diagnostic>
where
    T: for<'de> Deserialize<'de>,
{
    serde_yaml::from_str(source).map_err(|error| {
        Diagnostic::error(
            format!("invalid_{noun}_yaml"),
            "$",
            format!("could not parse {noun} YAML: {error}"),
        )
    })
}

pub(crate) fn read_yaml_file(path: &Path, noun: &str) -> Result<String, Diagnostic> {
    let file = File::open(path).map_err(|error| {
        Diagnostic::error(
            format!("{noun}_read_error"),
            path.display().to_string(),
            format!("could not open {noun} file: {error}"),
        )
    })?;
    let mut source = String::new();
    file.take(MAX_YAML_BYTES as u64 + 1)
        .read_to_string(&mut source)
        .map_err(|error| {
            Diagnostic::error(
                format!("{noun}_read_error"),
                path.display().to_string(),
                format!("could not read {noun} file as UTF-8: {error}"),
            )
        })?;
    if source.len() > MAX_YAML_BYTES {
        return Err(Diagnostic::error(
            format!("{noun}_yaml_too_large"),
            path.display().to_string(),
            format!("{noun} YAML must be <= {MAX_YAML_BYTES} bytes"),
        ));
    }
    Ok(source)
}

/// SHA-256 of a versioned typed value serialized in its declared struct order.
///
/// Identity DTOs contain no unordered maps. This deliberately hashes canonical
/// JSON produced from the typed value, not YAML bytes, so comments and key
/// ordering cannot fracture a measurement series.
pub(crate) fn typed_sha256<T: Serialize>(value: &T) -> Result<String, Diagnostic> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        Diagnostic::error(
            "identity_serialization_failed",
            "$",
            format!("could not serialize typed benchmark identity: {error}"),
        )
    })?;
    let mut digest = Sha256::new();
    digest.update(bytes);
    Ok(format!("{:x}", digest.finalize()))
}

pub(crate) fn valid_kebab_id(value: &str) -> bool {
    let mut saw_segment_char = false;
    let mut previous_was_hyphen = false;
    for character in value.chars() {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            saw_segment_char = true;
            previous_was_hyphen = false;
        } else if character == '-' && saw_segment_char && !previous_was_hyphen {
            previous_was_hyphen = true;
        } else {
            return false;
        }
    }
    saw_segment_char && !previous_was_hyphen
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kebab_id_validation_is_strict_and_path_free() {
        for valid in ["m3", "merge-warm", "case-1", "v1-2"] {
            assert!(valid_kebab_id(valid), "{valid}");
        }
        for invalid in [
            "",
            "Upper",
            "-leading",
            "trailing-",
            "two--segments",
            "has space",
            "case_1",
            "v1.2",
            "../case",
        ] {
            assert!(!valid_kebab_id(invalid), "{invalid}");
        }
    }

    #[test]
    fn file_loading_is_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("oversized.yaml");
        std::fs::write(&path, vec![b'a'; MAX_YAML_BYTES + 1]).unwrap();
        assert_eq!(
            read_yaml_file(&path, "case").unwrap_err().code,
            "case_yaml_too_large"
        );
        assert_eq!(
            declared_version(&"a".repeat(MAX_YAML_BYTES + 1), "suite")
                .unwrap_err()
                .code,
            "suite_yaml_too_large"
        );
    }
}
