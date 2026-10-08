//! Strict scenario dispatch. Historical definitions remain readable; new runs use GQT.
pub use crate::legacy::case::{
    Attribution, Backend, CacheCondition, EnginePreparation, LocalFilesystem, LocalStorageClass,
    PageCacheCondition, ProcessLifecycle, Protocol, ResetMode, S3Implementation, S3Versioning,
    Schedule, Timer, WarmupProgram,
};
use crate::model::{Diagnostic, ValidationOutcome};
use serde::{Deserialize, Serialize};
use std::path::Path;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CaseV1 {
    Gqt(crate::gqt_case::GqtCaseV1),
    BranchMerge(crate::legacy::case::CaseV1),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum ValidatedCase {
    Gqt(crate::gqt_case::PlannedGqt),
    Legacy(crate::legacy::case::ValidatedCase),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PointIdentityV1 {
    Gqt(crate::gqt_case::GqtPointIdentityV1),
    BranchMerge(crate::legacy::case::PointIdentityV1),
}
impl ValidatedCase {
    pub fn id(&self) -> &str {
        match self {
            Self::Gqt(c) => &c.definition.id,
            Self::Legacy(c) => &c.definition.id,
        }
    }
    pub fn case_digest(&self) -> &str {
        match self {
            Self::Gqt(c) => &c.case_digest,
            Self::Legacy(c) => &c.case_digest,
        }
    }
    pub fn planned_identity(&self) -> &str {
        match self {
            Self::Gqt(c) => &c.planned_sha256,
            Self::Legacy(c) => &c.point_id,
        }
    }
    pub fn point_id(&self) -> Option<&str> {
        match self {
            Self::Gqt(_) => None,
            Self::Legacy(c) => Some(&c.point_id),
        }
    }
    pub fn gqt(&self) -> Result<&crate::gqt_case::PlannedGqt, String> {
        match self {
            Self::Gqt(c) => Ok(c),
            Self::Legacy(_) => Err(
                "branch-merge-v1 execution is retired; migrate the authored case to gqt-v1".into(),
            ),
        }
    }
}
pub fn parse_case(source: &str) -> ValidationOutcome<CaseV1> {
    match crate::model::declared_version(source, "case") {
        Ok(crate::CASE_FORMAT_VERSION) => {}
        Ok(version) => {
            return ValidationOutcome::failure(vec![Diagnostic::error(
                "unsupported_case_version",
                "version",
                format!(
                    "unsupported case version {version}; expected {}",
                    crate::CASE_FORMAT_VERSION
                ),
            )]);
        }
        Err(error) => return ValidationOutcome::failure(vec![error]),
    }
    match crate::model::strict_yaml(source, "case") {
        Ok(c) => validate_case(c),
        Err(e) => ValidationOutcome::failure(vec![e]),
    }
}
pub fn validate_case(case: CaseV1) -> ValidationOutcome<CaseV1> {
    let result = match &case {
        CaseV1::Gqt(c) => crate::gqt_case::validate_definition(c),
        CaseV1::BranchMerge(c) => crate::legacy::case::validate_case(c.clone())
            .into_result()
            .map(|_| ())
            .map_err(|e| format!("{e:?}")),
    };
    match result {
        Ok(()) => ValidationOutcome::success(case),
        Err(e) => refusal(e),
    }
}
pub fn load_case(path: &Path) -> ValidationOutcome<ValidatedCase> {
    let source = match crate::model::read_yaml_file(path, "case") {
        Ok(s) => s,
        Err(e) => return ValidationOutcome::failure(vec![e]),
    };
    let case = match parse_case(&source).into_result() {
        Ok(c) => c,
        Err(e) => return ValidationOutcome::failure(e),
    };
    match case {
        CaseV1::Gqt(c) => match crate::gqt_case::load(path, c) {
            Ok(p) => ValidationOutcome::success(ValidatedCase::Gqt(p)),
            Err(e) => refusal(e),
        },
        CaseV1::BranchMerge(c) => match crate::legacy::case::validate_case(c).into_result() {
            Ok(c) => ValidationOutcome::success(ValidatedCase::Legacy(c)),
            Err(e) => ValidationOutcome::failure(e),
        },
    }
}
fn refusal<T>(message: String) -> ValidationOutcome<T> {
    ValidationOutcome::failure(vec![Diagnostic::error("invalid_case", "$", message)])
}
