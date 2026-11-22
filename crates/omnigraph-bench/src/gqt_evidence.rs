//! Per-repetition preparation input and the proof a worker reports before
//! measurement; the protocol frames carry them, the runner and record check them.
use crate::case::ResetMode;
use crate::gqt_case::{BoundGqt, SERVED_BLOCK_SESSIONS, Target};
use crate::gqt_served::ServedInput;
use crate::model::digest;
use crate::reset::{MetadataDigest, PhysicalDigest};
use crate::store::CloneRoot;
use omnigraph_gqt_core::{SeededTable, ServerTarget};
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
    Clone {
        input: Box<CloneInput>,
    },
    OwnedServer {
        input: Box<OwnedServerInput>,
    },
}

/// The graph id of the one graph a server the bench starts serves.
pub const OWNED_SERVER_GRAPH: &str = "bench";

/// What a `reset: rebuild` repetition addresses: the server the parent
/// started for it over an empty cluster, with the token of the actor that
/// loads the fixture and runs the workload's own steps; one token per session
/// of the measured block, each another actor's; the digest of the server
/// binary; and the node tables (`tables`) and edge tables of the fixture's
/// base with their rows, which the worker counts on the server once the
/// fixture is loaded.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedServerInput {
    pub target: ServerTarget,
    pub session_tokens: Vec<String>,
    pub server_sha256: String,
    pub tables: Vec<SeededTable>,
    pub edge_tables: Vec<SeededTable>,
}

/// `Debug` names the server and counts the tokens, never a token.
impl std::fmt::Debug for OwnedServerInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedServerInput")
            .field("target", &self.target)
            .field("session_tokens", &self.session_tokens.len())
            .field("server_sha256", &self.server_sha256)
            .field("tables", &self.tables)
            .field("edge_tables", &self.edge_tables)
            .finish()
    }
}

impl OwnedServerInput {
    /// Every token of the input, the target's first.
    pub(crate) fn tokens(&self) -> impl Iterator<Item = &str> {
        self.target
            .token
            .as_deref()
            .into_iter()
            .chain(self.session_tokens.iter().map(String::as_str))
    }

    fn validate(&self, case: &BoundGqt) -> Result<(), String> {
        let endpoint = crate::gqt_served::canonical_endpoint(&self.target.url)?;
        if endpoint != self.target.url || !endpoint.starts_with("http://127.0.0.1:") {
            return Err("a server the bench starts is addressed at http://127.0.0.1:<port>".into());
        }
        if self.target.graph != OWNED_SERVER_GRAPH || !digest(&self.server_sha256) {
            return Err("the owned server input names another graph or no server binary".into());
        }
        let block = case.plan.queries.parse()?;
        let sessions = block
            .measured_block()
            .map_or(0, |block| block.sessions.len());
        if self.session_tokens.len() != sessions || sessions > SERVED_BLOCK_SESSIONS {
            return Err(format!(
                "the measured block declares {sessions} sessions and the input carries {} tokens",
                self.session_tokens.len()
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for token in self.tokens() {
            crate::gqt_served::validate_token(token)?;
            if !seen.insert(token) {
                return Err("two actors of an owned server share a token".into());
            }
        }
        if seen.len() != sessions + 1 {
            return Err("the owned server input carries no token for the workload's steps".into());
        }
        if self.tables.len() + self.edge_tables.len() > 4096
            || self
                .tables
                .iter()
                .chain(&self.edge_tables)
                .any(|seeded| seeded.branch.is_empty() || seeded.table.is_empty())
        {
            return Err("the owned server input names an unbounded or unnamed table".into());
        }
        Ok(())
    }
}

/// What a `--store` repetition's child processes address: the clone the
/// repetition owns, the frozen base it reads through, the product binary
/// whose digest the record names, and the environment the children run with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloneInput {
    pub store_uri: String,
    pub base_root_uri: String,
    pub base_manifest_sha256: String,
    pub clone: CloneRoot,
    pub product: PathBuf,
    pub product_sha256: String,
    pub environment: Vec<(String, String)>,
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
    Clone {
        base_manifest_sha256: String,
    },
    OwnedServer {
        server_sha256: String,
    },
}

impl RepetitionInputV2 {
    pub fn validate(&self, case: &BoundGqt) -> Result<(), String> {
        case.revalidate()?;
        match (self, case.identity.environment.target) {
            (Self::Clone { input }, Target::Engine)
                if case.plan.definition.protocol.reset == ResetMode::Clone
                    && digest(&input.base_manifest_sha256)
                    && digest(&input.product_sha256)
                    && input.product.is_absolute()
                    && input.clone.uri.starts_with(&input.store_uri)
                    && input.base_root_uri.starts_with(&input.store_uri)
                    && input.environment.iter().all(|(name, _)| {
                        !name.is_empty() && !name.contains('=') && !name.contains('\0')
                    }) =>
            {
                Ok(())
            }
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
            (Self::Served { input }, Target::Server)
                if case.plan.definition.protocol.reset != ResetMode::Rebuild =>
            {
                input.validate(case)
            }
            (Self::OwnedServer { input }, Target::Server)
                if case.plan.definition.protocol.reset == ResetMode::Rebuild =>
            {
                input.validate(case)
            }
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
            Self::Clone { input } => Ok(PreparationProofV2::Clone {
                base_manifest_sha256: input.base_manifest_sha256.clone(),
            }),
            Self::OwnedServer { input } => Ok(PreparationProofV2::OwnedServer {
                server_sha256: input.server_sha256.clone(),
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
