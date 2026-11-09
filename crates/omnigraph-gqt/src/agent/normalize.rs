//! Typed-IR normalisation for comparing an answer with a ground truth.
//!
//! Two declarations that differ only in the names they choose, for a
//! variable (`$p` against `$person`), an alias (`as n` against `as
//! headcount`) or the declaration itself, lower to typed IR that differs only
//! in those names. The normaliser renames every such name, wherever the IR
//! quotes it, to `v0`, `v1`, … in order of first appearance in the IR, so the
//! comparison sees structure and the names the task fixes (parameters, types,
//! properties, edges) and nothing else. It works on the IR's `Debug` text, so
//! it serves any edition whose typed IR quotes names the same way.
//!
//! The rename is by spelling: a string literal or a property that is spelled
//! like a chosen name is renamed with it. Both sides of a comparison receive
//! the same treatment, so this can only make two different declarations
//! compare unequal, never two equal ones.

use std::collections::{BTreeSet, HashMap};

/// The names an author chooses in `text`: every `$variable`, every alias
/// after `as`, and every declared query name. Comments and string literals
/// are skipped.
pub fn source_names(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes = text.as_bytes();
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut words: Vec<&str> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'"' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            i += 1;
            words.push("");
            continue;
        }
        if b == b'/' && bytes.get(i + 1) == Some(&b'/') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        if b == b'$' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && ident(bytes[end]) {
                end += 1;
            }
            if end > start {
                out.insert(text[start..end].to_string());
            }
            i = end.max(i + 1);
            words.push("");
            continue;
        }
        if ident(b) {
            let start = i;
            while i < bytes.len() && ident(bytes[i]) {
                i += 1;
            }
            words.push(&text[start..i]);
            continue;
        }
        if !b.is_ascii_whitespace() {
            words.push("");
        }
        i += 1;
    }
    for pair in words.windows(2) {
        if (pair[0] == "as" || pair[0] == "query") && !pair[1].is_empty() {
            out.insert(pair[1].to_string());
        }
    }
    out
}

/// Rename, in `ir`, every quoted string that is one of `names` and not a
/// parameter, in order of first appearance.
pub fn rename(ir: &str, names: &BTreeSet<String>, params: &[String]) -> String {
    let mut map: HashMap<&str, String> = HashMap::new();
    let mut out = String::with_capacity(ir.len());
    let mut rest = ir;
    while let Some(start) = rest.find('"') {
        out.push_str(&rest[..=start]);
        let after = &rest[start + 1..];
        let mut end = 0;
        let bytes = after.as_bytes();
        while end < bytes.len() && bytes[end] != b'"' {
            if bytes[end] == b'\\' {
                end += 1;
            }
            end += 1;
        }
        if end >= bytes.len() {
            out.push_str(after);
            return out;
        }
        let word = &after[..end];
        // A derived column name, `p.name`, carries its variable's name, and
        // a column the compiler adds for itself marks it with a leading
        // sigil (`^p.name`).
        let sigil = word.len()
            - word
                .trim_start_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .len();
        let (prefix, name) = word.split_at(sigil);
        let head = name.split_once('.').map_or(name, |(head, _)| head);
        let tail = &name[head.len()..];
        let chosen = names.contains(head) && !params.iter().any(|p| p == head);
        if chosen {
            let next = map.len();
            out.push_str(prefix);
            out.push_str(map.entry(head).or_insert_with(|| format!("v{next}")));
            out.push_str(tail);
        } else {
            out.push_str(word);
        }
        out.push('"');
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chosen_names_are_variables_aliases_and_declarations() {
        let names = source_names(
            "query people($h: String) { match { $p: Person { handle: $h } } \
             return { $p.name as who, \"as x\" } } // as y\n",
        );
        let names: Vec<_> = names.into_iter().collect();
        assert_eq!(names, ["h", "p", "people", "who"]);
    }

    #[test]
    fn renaming_skips_parameters_and_follows_first_appearance() {
        let names: BTreeSet<String> = ["p", "q", "h"].iter().map(|s| s.to_string()).collect();
        let ir =
            r#"Bind { var: "q" } Filter { "p" = Param("h") } Return { "q", column: "q.name" }"#;
        assert_eq!(
            rename(ir, &names, &["h".to_string()]),
            r#"Bind { var: "v0" } Filter { "v1" = Param("h") } Return { "v0", column: "v0.name" }"#
        );
    }
}
