//! A physical plan with the values a run needs beside it: the query's
//! parameters and, per ranked scan, its query vector. Execution takes a
//! [`BoundPlan`] and nothing else about the query; serialized through the
//! planner-owned mirrors in `mirror.rs`, it is the replay boundary.

use std::collections::BTreeMap;
use std::sync::Arc;

use omnigraph_compiler::ir::ParamMap;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::PlanError;
use crate::mirror::BoundPlanMirror;
use crate::physical::{NodeId, PhysicalPlan};

/// Saved execution plans use a separate version from rendered explain output.
pub const BOUND_PLAN_VERSION: u32 = 6;

/// The envelope omits top-level `plan` and `values` so unversioned readers reject it.
#[derive(Serialize, Deserialize)]
struct BoundPlanEnvelope {
    bound_plan_version: u32,
    body: BoundPlanMirror,
}

/// The values of one run: every parameter as the engine resolved it (`now()`
/// among them) and the query vector of every `nearest` scan, keyed by the
/// scan's node id.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValueTable {
    pub params: Arc<ParamMap>,
    pub vectors: BTreeMap<NodeId, Vec<f32>>,
}

/// A [`PhysicalPlan`] and its [`ValueTable`].
#[derive(Debug, Clone, PartialEq)]
pub struct BoundPlan {
    pub plan: PhysicalPlan,
    pub values: ValueTable,
}

impl Serialize for BoundPlan {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        BoundPlanEnvelope {
            bound_plan_version: BOUND_PLAN_VERSION,
            body: BoundPlanMirror::from(self),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BoundPlan {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let envelope = BoundPlanEnvelope::deserialize(deserializer).map_err(|error| {
            D::Error::custom(format!(
                "cannot read saved bound plan; regenerate it with this version: {error}"
            ))
        })?;
        if envelope.bound_plan_version != BOUND_PLAN_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported bound plan version {}; expected {}; regenerate the saved plan",
                envelope.bound_plan_version, BOUND_PLAN_VERSION
            )));
        }
        Self::try_from(envelope.body).map_err(D::Error::custom)
    }
}

impl TryFrom<BoundPlanMirror> for BoundPlan {
    type Error = PlanError;

    fn try_from(mirror: BoundPlanMirror) -> Result<Self, PlanError> {
        Ok(Self {
            plan: PhysicalPlan::try_from(mirror.plan)?,
            values: ValueTable::from(mirror.values),
        })
    }
}
