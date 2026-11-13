//! A triple resolved against the catalog: its edge selection, the types of
//! its ends and its hop bounds (`T5`, `T15`, `T22`, `T23`).

use std::collections::{HashMap, HashSet};

use crate::catalog::{Catalog, EdgeType};
use crate::error::{CompilerError, Result};
use crate::query::codes::*;
use crate::query::diagnostic::QueryDiagnostic;
use crate::query::syntax::{EdgeSelector, Traversal};
use crate::traversal::{EdgeMember, EdgeSelection};
use crate::types::Direction;

/// A variable in the query's single namespace, tagged by what it binds.
///
/// Node and edge bindings share one symbol table (GQ has one variable
/// namespace) but live in *separate type namespaces* — a node type and an edge
/// type may share a name. A type name therefore only means something once the
/// kind is known, so the kind is the discriminant: every consumer must match,
/// and a new kind is a compile error at each site.
#[derive(Debug, Clone)]
pub enum BoundVariable {
    Node { type_name: String },
    Edge,
}

impl BoundVariable {
    /// Node type name of a traversal endpoint, or T23 if `self` is an edge
    /// binding. The type name is only reachable through this check, so no
    /// caller can compare endpoint types without having ruled the edge case
    /// out. `var` names the variable for the error message only.
    fn require_traversal_endpoint(&self, var: &str) -> Result<&str> {
        match self {
            Self::Node { type_name } => Ok(type_name),
            Self::Edge => Err(CompilerError::typed(
                T23,
                format!(
                    "edge binding `${var}` cannot be used as a traversal endpoint; traversal endpoints must be node bindings"
                ),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedTraversal {
    pub edges: EdgeSelection,
    pub src_type: String,
    pub dst_type: String,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
    /// Variable bound to the matched edge (`$p $w:knows $f`), if any;
    /// lowering uses it to carry edge properties through the expand.
    pub edge_binding: Option<String>,
}

pub(crate) fn resolve_traversal(
    catalog: &Catalog,
    traversal: &Traversal,
    bindings: &HashMap<String, BoundVariable>,
    declarations: &HashMap<String, BoundVariable>,
) -> Result<ResolvedTraversal> {
    if traversal.min_hops == 0 {
        return Err(CompilerError::typed(
            T15,
            "traversal min hop bound must be >= 1".to_string(),
        ));
    }
    if let Some(max_hops) = traversal.max_hops {
        if max_hops < traversal.min_hops {
            return Err(CompilerError::typed(
                T15,
                format!(
                    "invalid traversal bounds {{{},{}}}; max must be >= min",
                    traversal.min_hops, max_hops
                ),
            ));
        }
    } else {
        return Err(CompilerError::typed(
            T15,
            "unbounded traversal is disabled; use bounded traversal {min,max}".to_string(),
        ));
    }
    if let Some(binding) = &traversal.edge_binding
        && (traversal.min_hops != 1 || traversal.max_hops != Some(1))
    {
        return Err(CompilerError::typed(
            T23,
            format!(
                "edge binding `${binding}` cannot be combined with traversal bounds; a multi-hop traversal matches a path of edges, not one edge"
            ),
        ));
    }
    let src = bindings
        .get(&traversal.src)
        .or_else(|| declarations.get(&traversal.src))
        .filter(|_| traversal.src != "_")
        .map(|binding| binding.require_traversal_endpoint(&traversal.src))
        .transpose()?;
    let dst = bindings
        .get(&traversal.dst)
        .or_else(|| declarations.get(&traversal.dst))
        .filter(|_| traversal.dst != "_")
        .map(|binding| binding.require_traversal_endpoint(&traversal.dst))
        .transpose()?;
    let (edges, src_type, dst_type) = match &traversal.selector {
        EdgeSelector::Named(name) => {
            let edge = lookup_traversal_edge(catalog, name)?;
            let (member, src_type, dst_type) = resolve_member(edge, traversal, src, dst)?;
            (EdgeSelection::Named(member), src_type, dst_type)
        }
        EdgeSelector::Alternation(names) => {
            let Some((first, rest)) = names.split_first() else {
                return Err(CompilerError::typed(
                    T4,
                    "edge alternation must name at least one edge type".to_string(),
                ));
            };
            let edge = lookup_traversal_edge(catalog, first)?;
            if src.is_none() && dst.is_none() {
                for name in rest {
                    let other = lookup_traversal_edge(catalog, name)?;
                    if edge.from_type != other.from_type || edge.to_type != other.to_type {
                        return Err(CompilerError::typed(T5, "an edge alternation with mixed endpoint orientations requires a declared endpoint type".to_string(),
                        ));
                    }
                }
            }
            let (member, src_type, dst_type) = resolve_member(edge, traversal, src, dst)?;
            let mut seen = HashSet::from([member.edge_type.clone()]);
            let mut members = vec![member];
            for name in rest {
                let edge = lookup_traversal_edge(catalog, name)?;
                if seen.insert(edge.name.clone()) {
                    let (member, _, _) =
                        resolve_member(edge, traversal, Some(&src_type), Some(&dst_type))?;
                    members.push(member);
                }
            }
            members.sort_by(|left, right| left.edge_type.cmp(&right.edge_type));
            (EdgeSelection::Alternation(members), src_type, dst_type)
        }
        EdgeSelector::Wildcard => {
            let (Some(src), Some(dst)) = (src, dst) else {
                return Err(CompilerError::typed(
                    T5,
                    "wildcard traversal requires known source and destination node types"
                        .to_string(),
                ));
            };
            if traversal.undirected && src != dst {
                return Err(CompilerError::typed(
                    T22,
                    "undirected wildcard traversal requires the same node type at both endpoints"
                        .to_string(),
                ));
            }
            let mut members = Vec::new();
            for edge in catalog.edge_types.values() {
                if (edge.from_type == src && edge.to_type == dst)
                    || (edge.to_type == src && edge.from_type == dst)
                {
                    let (member, _, _) = resolve_member(edge, traversal, Some(src), Some(dst))?;
                    members.push(member);
                }
            }
            members.sort_by(|left, right| left.edge_type.cmp(&right.edge_type));
            (
                EdgeSelection::Wildcard(members),
                src.to_string(),
                dst.to_string(),
            )
        }
    };
    if traversal.src == traversal.dst && traversal.src != "_" && src_type != dst_type {
        return Err(CompilerError::typed(
            T5,
            format!(
                "traversal endpoint `${}` cannot have both type `{src_type}` and type `{dst_type}`",
                traversal.src
            ),
        ));
    }
    // A hop ends on the destination type, and the next hop must start on the
    // source type, so a path across distinct endpoint types never reaches a
    // second hop; a bound that allows one is refused, not silently capped.
    if let Some(max_hops) = traversal.max_hops
        && max_hops > 1
        && src_type != dst_type
    {
        let (EdgeSelector::Named(name), Some(member)) = (&traversal.selector, edges.named()) else {
            return Err(CompilerError::typed(
                T5,
                "recursive edge selection requires the same node type at both endpoints"
                    .to_string(),
            ));
        };
        let edge = lookup_traversal_edge(catalog, &member.edge_type)?;
        // Dropping the bound keeps the rows of a range that includes hop one;
        // a range starting past it matched nothing, so that rewrite would not.
        let fix = if traversal.min_hops == 1 {
            format!(
                "follow one hop without a bound: `${} {name} ${}`",
                traversal.src, traversal.dst
            )
        } else {
            format!(
                "no `{}` path reaches hop {}, so this pattern matches nothing; to go further, start another traversal at the `{dst_type}` endpoint",
                edge.name, traversal.min_hops
            )
        };
        return Err(CompilerError::query(
            QueryDiagnostic::typecheck(
                T5,
                format!(
                    "multi-hop traversal `{name}{{{},{max_hops}}}` requires the same node type at both endpoints, but `{}: {} -> {}` connects different types, so no path continues past one hop",
                    traversal.min_hops, edge.name, edge.from_type, edge.to_type
                ),
            )
            .with_fix(fix),
        ));
    }
    Ok(ResolvedTraversal {
        edges,
        src_type,
        dst_type,
        min_hops: traversal.min_hops,
        max_hops: traversal.max_hops,
        edge_binding: traversal
            .edge_binding
            .as_deref()
            .filter(|name| *name != "_")
            .map(str::to_string),
    })
}

fn lookup_traversal_edge<'a>(catalog: &'a Catalog, name: &str) -> Result<&'a EdgeType> {
    catalog.lookup_edge_by_name(name).ok_or_else(|| {
        CompilerError::typed(
            T4,
            format!(
                "unknown edge type `{name}`{}",
                super::nearest::listing(name, super::nearest::edges(catalog))
            ),
        )
    })
}

fn resolve_member(
    edge: &EdgeType,
    traversal: &Traversal,
    src: Option<&str>,
    dst: Option<&str>,
) -> Result<(EdgeMember, String, String)> {
    if traversal.undirected && edge.from_type != edge.to_type {
        return Err(CompilerError::typed(
            T22,
            format!(
                "undirected traversal `<{}>` requires a same-endpoint-type edge, but `{}: {} -> {}` is asymmetric; use the directional form",
                edge.name, edge.name, edge.from_type, edge.to_type
            ),
        ));
    }
    let direction = if let Some(src) = src {
        if src == edge.from_type {
            Direction::Out
        } else if src == edge.to_type {
            Direction::In
        } else {
            return Err(endpoint_type_error(&traversal.src, src, edge));
        }
    } else if let Some(dst) = dst {
        if dst == edge.to_type {
            Direction::Out
        } else if dst == edge.from_type {
            Direction::In
        } else {
            return Err(endpoint_type_error(&traversal.dst, dst, edge));
        }
    } else {
        Direction::Out
    };
    let (src_type, dst_type) = match direction {
        Direction::Out | Direction::Both => (&edge.from_type, &edge.to_type),
        Direction::In => (&edge.to_type, &edge.from_type),
    };
    if let Some(dst) = dst
        && dst != dst_type
    {
        return Err(CompilerError::typed(
            T5,
            format!(
                "endpoint `${}` resolves to type `{dst}` but edge `{}` expects `{dst_type}`",
                traversal.dst, edge.name
            ),
        ));
    }
    Ok((
        EdgeMember {
            edge_type: edge.name.clone(),
            direction: if traversal.undirected {
                Direction::Both
            } else {
                direction
            },
        },
        src_type.clone(),
        dst_type.clone(),
    ))
}

fn endpoint_type_error(var: &str, type_name: &str, edge: &EdgeType) -> CompilerError {
    CompilerError::typed(
        T5,
        format!(
            "endpoint `${var}` resolves to type `{type_name}`, which is not an endpoint of edge `{}: {} -> {}`",
            edge.name, edge.from_type, edge.to_type
        ),
    )
}
