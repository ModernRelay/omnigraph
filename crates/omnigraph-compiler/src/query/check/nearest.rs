//! The names a refusal of an unknown name lists (docs/rfcs/2026-11-07-gq-3.md,
//! section 5): up to five of the right kind, nearest the written name first,
//! each with its type, so that an agent picks the name it meant from the
//! diagnostic alone.

use crate::catalog::Catalog;
use crate::types::PropType;

/// How many names a refusal lists.
const LISTED: usize = 5;

/// Up to five `(name, type)` candidates nearest `written` by edit distance
/// (letter case ignored); on a tie a name one of them begins with comes
/// first, then name order.
pub(super) fn nearest(written: &str, candidates: Vec<(String, String)>) -> Vec<(String, String)> {
    let written = written.to_lowercase();
    let mut ranked: Vec<((usize, bool), (String, String))> = candidates
        .into_iter()
        .map(|c| {
            let name = c.0.to_lowercase();
            let prefix = name.starts_with(&written) || written.starts_with(&name);
            ((edit_distance(&written, &name), !prefix), c)
        })
        .collect();
    ranked.sort_by(|(a, x), (b, y)| a.cmp(b).then_with(|| x.0.cmp(&y.0)));
    ranked.dedup_by(|(_, x), (_, y)| x.0 == y.0);
    ranked.into_iter().take(LISTED).map(|(_, c)| c).collect()
}

/// `; nearest: a (T), b (U)`, the suffix a refusal's message takes, or
/// nothing when there is no candidate.
pub(crate) fn listing(written: &str, candidates: Vec<(String, String)>) -> String {
    let near = nearest(written, candidates);
    if near.is_empty() {
        return String::new();
    }
    format!(
        "; nearest: {}",
        near.iter()
            .map(|(name, ty)| format!("{name} ({ty})"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// The node types and edge types a binding `$x: T` may name.
pub(super) fn binding_types(catalog: &Catalog) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = catalog
        .node_types
        .keys()
        .map(|n| (n.clone(), "node".to_string()))
        .collect();
    out.extend(catalog.edge_types.values().map(|e| {
        (
            e.name.clone(),
            format!("edge {} -> {}", e.from_type, e.to_type),
        )
    }));
    out
}

/// The edges a triple may name, spelled as a triple writes them (`knows`,
/// `worksAt`), with their endpoint types.
pub(super) fn edges(catalog: &Catalog) -> Vec<(String, String)> {
    catalog
        .edge_types
        .values()
        .map(|e| {
            (
                triple_spelling(&e.name),
                format!("{} -> {}", e.from_type, e.to_type),
            )
        })
        .collect()
}

/// An edge name as a triple writes it: its first letter lowered.
fn triple_spelling(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// The scalar types a parameter may declare.
pub(crate) fn scalar_types() -> Vec<(String, String)> {
    [
        "String", "Bool", "I32", "I64", "U32", "U64", "F32", "F64", "Date", "DateTime", "Blob",
    ]
    .into_iter()
    .map(|t| (t.to_string(), "scalar".to_string()))
    .collect()
}

/// The properties of a type, with their types.
pub(super) fn properties<'a>(
    props: impl IntoIterator<Item = (&'a String, &'a PropType)>,
) -> Vec<(String, String)> {
    props
        .into_iter()
        .map(|(name, ty)| (name.clone(), ty.display_name()))
        .collect()
}

/// Levenshtein distance over characters.
pub(super) fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut prev = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cur = row[j + 1];
            row[j + 1] = if ca == *cb {
                prev
            } else {
                1 + prev.min(cur).min(row[j])
            };
            prev = cur;
        }
    }
    row[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_five_nearest_first_with_their_types() {
        let candidates = [
            "name", "handle", "age", "city", "nickname", "title", "names",
        ]
        .into_iter()
        .map(|n| (n.to_string(), "String".to_string()))
        .collect();
        let near = nearest("nmae", candidates);
        assert_eq!(near.len(), 5);
        assert_eq!(near[0].0, "name");
        assert_eq!(
            listing("Knws", vec![("knows".into(), "Person -> Person".into())]),
            "; nearest: knows (Person -> Person)"
        );
        assert_eq!(listing("x", Vec::new()), "");
        assert_eq!(triple_spelling("WorksAt"), "worksAt");
    }
}
