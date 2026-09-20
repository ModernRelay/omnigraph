use std::fmt;
use std::str::FromStr;

use serde::Serialize;
use thiserror::Error;

/// The operator's route override, read once per operation at the engine
/// handle and passed to the gate. It selects between the executor and the
/// registry's route; it cannot route a shape the registry does not name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteOverride {
    #[default]
    Registry,
    ForceExecutor,
    ForcePlanner,
}

impl RouteOverride {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Registry => "registry",
            Self::ForceExecutor => "force_executor",
            Self::ForcePlanner => "force_planner",
        }
    }
}

impl fmt::Display for RouteOverride {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "unrecognized OMNIGRAPH_PLANNER_ROUTE value `{0}`; expected registry, force_executor or force_planner"
)]
pub struct UnknownRouteOverride(pub String);

impl FromStr for RouteOverride {
    type Err = UnknownRouteOverride;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "registry" => Ok(Self::Registry),
            "force_executor" => Ok(Self::ForceExecutor),
            "force_planner" => Ok(Self::ForcePlanner),
            other => Err(UnknownRouteOverride(other.to_string())),
        }
    }
}
