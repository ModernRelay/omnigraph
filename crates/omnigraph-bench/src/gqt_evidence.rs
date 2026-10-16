//! Per-repetition preparation input and the proof a worker reports before
//! measurement; the protocol frames carry them, the runner and record check them.
use crate::gqt_case::{BoundGqt, Target};
use crate::gqt_served::ServedInput;
use crate::model::digest;
use crate::reset::{MetadataDigest, PhysicalDigest};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "target", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RepetitionInputV2 {
    Embedded {
        repetition_root: PathBuf,
        physical_digest: PhysicalDigest,
        metadata_digest: MetadataDigest,
        fixture_manifest_sha256: String,
    },
    Served {
        input: Box<ServedInput>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "target", rename_all = "kebab-case", deny_unknown_fields)]
pub enum PreparationProofV2 {
    Embedded {
        physical_digest: PhysicalDigest,
        metadata_digest: MetadataDigest,
    },
    Served {
        server_receipt_sha256: String,
    },
}

impl RepetitionInputV2 {
    pub fn validate(&self, case: &BoundGqt) -> Result<(), String> {
        case.revalidate()?;
        match (self, case.identity.environment.target) {
            (
                Self::Embedded {
                    repetition_root,
                    fixture_manifest_sha256,
                    ..
                },
                Target::Engine,
            ) if repetition_root.is_absolute() && digest(fixture_manifest_sha256) => Ok(()),
            (Self::Served { input }, Target::Server) => input.validate(case),
            _ => Err("worker execution input disagrees with the admitted target".into()),
        }
    }

    pub fn proof(&self) -> Result<PreparationProofV2, String> {
        match self {
            Self::Embedded {
                physical_digest,
                metadata_digest,
                ..
            } => Ok(PreparationProofV2::Embedded {
                physical_digest: physical_digest.clone(),
                metadata_digest: metadata_digest.clone(),
            }),
            Self::Served { input } => Ok(PreparationProofV2::Served {
                server_receipt_sha256: input.receipt.digest()?,
            }),
        }
    }
}
