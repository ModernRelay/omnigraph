use omnigraph_compiler::ir::IRExpr;
use omnigraph_compiler::query::ast::{CompOp, Literal};
use omnigraph_compiler::{ExprType, ScalarType};

use crate::error::PlanError;
use crate::logical::{RuntimeInput, ScanAccess, ScanSpec};
use crate::optimizer::{Optimized, gq_conjunct};
use crate::physical::{PhysicalNode, RankKind, ScanInput};
use crate::source::{IndexKind, PlanSource};

pub const PASS_SCAN_ACCESS: &str = "scan_access";
pub const PASS_KEY_TO_ID: &str = "key_to_id";

pub async fn finalize_scan_access(
    optimized: &mut Optimized,
    source: &(dyn PlanSource + Sync),
) -> Result<(), PlanError> {
    let scans: Vec<_> = optimized
        .physical
        .live()
        .filter_map(|(id, node)| matches!(node, PhysicalNode::Scan { .. }).then_some(id))
        .collect();
    let mut rewrote_key = false;
    for id in &scans {
        let Some(PhysicalNode::Scan {
            source: input,
            spec,
            ranked,
            ..
        }) = optimized.physical.node_mut(*id)
        else {
            continue;
        };
        let facts = source.index_facts(&spec.table.type_key);
        let access = if matches!(input, ScanInput::Dependent { .. }) {
            ScanAccess::IdLookup {
                index: facts
                    .iter()
                    .find(|fact| {
                        fact.column == spec.columns.id
                            && matches!(fact.kind, IndexKind::Btree { usable: true })
                    })
                    .map(|fact| fact.name.clone()),
            }
        } else if spec.runtime_filter.is_some() {
            ScanAccess::Runtime {
                input: RuntimeInput::JoinFilter,
            }
        } else if let Some(ranked) = ranked {
            ScanAccess::Runtime {
                input: match ranked.kind {
                    RankKind::Nearest => RuntimeInput::Nearest,
                    RankKind::Bm25 => RuntimeInput::FullText,
                },
            }
        } else if let Some(input) = source.scan_runtime_input(spec) {
            ScanAccess::Runtime { input }
        } else if spec.filter.is_none() || facts.is_empty() {
            ScanAccess::Sequential
        } else {
            if facts.iter().any(|fact| {
                fact.column == spec.columns.id
                    && matches!(fact.kind, IndexKind::Btree { usable: true })
            }) {
                rewrote_key |= narrow_key(spec, source)?;
            }
            source.index_split(spec).await?
        };
        spec.access = Some(access);
    }
    if rewrote_key {
        optimized.fired.push(PASS_KEY_TO_ID);
    }
    if !scans.is_empty() {
        optimized.fired.push(PASS_SCAN_ACCESS);
    }
    Ok(())
}

fn narrow_key(spec: &mut ScanSpec, source: &dyn PlanSource) -> Result<bool, PlanError> {
    let Some(type_name) = spec.table.type_key.strip_prefix("node:") else {
        return Ok(false);
    };
    let node = source.node_type(type_name)?;
    let [key] = node.key.as_slice() else {
        return Ok(false);
    };
    let Some(binding) = spec.binding.as_ref() else {
        return Ok(false);
    };
    let Some(original) = spec.filter.as_ref() else {
        return Ok(false);
    };
    for conjunct in original
        .gq_filters()
        .into_iter()
        .flat_map(IRExpr::into_conjuncts)
    {
        let Some((left, CompOp::Eq, right)) = conjunct.comparison_parts() else {
            continue;
        };
        for (property, value) in [(left, right), (right, left)] {
            let IRExpr::PropAccess {
                variable,
                property,
                ty,
            } = property
            else {
                continue;
            };
            if variable != binding
                || property != key
                || !string_type(ty)
                || !string_type(value.ty())
                || !matches!(value, IRExpr::Literal(..) | IRExpr::Param(..))
            {
                continue;
            }
            let Some(id) = source.canonical_key_id(&spec.table.type_key, value)? else {
                continue;
            };
            let ty = ExprType::Value {
                scalar: ScalarType::String,
                list: false,
                nullable: false,
            };
            let derived = IRExpr::comparison(
                IRExpr::PropAccess {
                    variable: binding.clone(),
                    property: spec.columns.id.into(),
                    ty: ty.clone(),
                },
                CompOp::Eq,
                IRExpr::Literal(Literal::String(id), ty),
            );
            spec.filter = Some(original.clone().and(gq_conjunct(&derived)));
            return Ok(true);
        }
    }
    Ok(false)
}

fn string_type(ty: &ExprType) -> bool {
    matches!(
        ty,
        ExprType::Value {
            scalar: ScalarType::String,
            list: false,
            ..
        }
    )
}
