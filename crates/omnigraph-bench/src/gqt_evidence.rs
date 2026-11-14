//! Per-repetition preparation input and the proof a worker reports before
//! measurement; the protocol frames carry them, the runner and record check them.
use crate::gqt_case::{BoundGqt, Target};
use crate::gqt_served::ServedInput;
use crate::model::digest;
use crate::reset::{MetadataDigest, PhysicalDigest};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// How an embedded repetition's engine reaches its local root.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "route", rename_all = "kebab-case", deny_unknown_fields)]
pub enum LocalRoute {
    /// The restored working copy, with Lance's direct local file path.
    #[default]
    Direct,
    /// The restored working copy, every read and write through the object
    /// store: the route a served local root takes.
    ObjectStore,
    /// An empty root read through the frozen template at `base_root`, on the
    /// object-store route; nothing is restored.
    ReadThrough { base_root: PathBuf },
}

impl LocalRoute {
    fn is_direct(&self) -> bool {
        *self == Self::Direct
    }
}

/// A [`LocalRoute`] before its template is known: the run option.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LocalRouteKind {
    #[default]
    Direct,
    ObjectStore,
    ReadThrough,
}

impl LocalRouteKind {
    pub const ALL: [Self; 3] = [Self::Direct, Self::ObjectStore, Self::ReadThrough];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::ObjectStore => "object-store",
            Self::ReadThrough => "read-through",
        }
    }

    pub fn over(self, template: &std::path::Path) -> LocalRoute {
        match self {
            Self::Direct => LocalRoute::Direct,
            Self::ObjectStore => LocalRoute::ObjectStore,
            Self::ReadThrough => LocalRoute::ReadThrough {
                base_root: template.into(),
            },
        }
    }
}

impl std::str::FromStr for LocalRouteKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.name() == value)
            .ok_or_else(|| {
                format!(
                    "unknown local route `{value}`; expected direct, object-store or read-through"
                )
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "target", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RepetitionInputV2 {
    Embedded {
        repetition_root: PathBuf,
        physical_digest: PhysicalDigest,
        metadata_digest: MetadataDigest,
        fixture_manifest_sha256: String,
        #[serde(default, skip_serializing_if = "LocalRoute::is_direct")]
        route: LocalRoute,
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
                    route,
                    ..
                },
                Target::Engine,
            ) if repetition_root.is_absolute()
                && digest(fixture_manifest_sha256)
                && !matches!(route, LocalRoute::ReadThrough { base_root } if !base_root.is_absolute()) =>
            {
                Ok(())
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn embedded(route: LocalRoute) -> RepetitionInputV2 {
        RepetitionInputV2::Embedded {
            repetition_root: PathBuf::from("/entry/active"),
            physical_digest: PhysicalDigest {
                files: 1,
                bytes: 1,
                digest_sha256: "e".repeat(64),
            },
            metadata_digest: MetadataDigest {
                entries: 1,
                files: 1,
                directories: 0,
                bytes: 1,
                shape_sha256: "f".repeat(64),
                state_sha256: "a".repeat(64),
            },
            fixture_manifest_sha256: "b".repeat(64),
            route,
        }
    }

    #[test]
    fn the_frame_omits_the_route_on_direct_and_names_it_otherwise() {
        let direct = serde_json::to_value(embedded(LocalRoute::Direct)).unwrap();
        assert!(direct.get("route").is_none(), "{direct}");
        let read_through = serde_json::to_value(embedded(LocalRoute::ReadThrough {
            base_root: PathBuf::from("/entry/root"),
        }))
        .unwrap();
        assert_eq!(read_through["route"]["route"], "read-through");
        assert_eq!(read_through["route"]["base_root"], "/entry/root");
        for frame in [direct, read_through] {
            let back: RepetitionInputV2 = serde_json::from_value(frame.clone()).unwrap();
            assert_eq!(serde_json::to_value(&back).unwrap(), frame);
        }
    }
}
