//! The graph id rules.
//!
//! [`graph_id_violation`] is the server's rule, the one predicate the server's
//! `GraphId` applies: 1–64 ASCII letters, digits or hyphens
//! (`^[a-zA-Z0-9-]{1,64}$`), and not a reserved name.
//!
//! [`declared_graph_id_violation`] is the rule for a graph `cluster.yaml`
//! declares: the server's rule and a letter first, so an id that validates is
//! one the server serves and one deployment addresses and recorded state
//! accept.
//!
//! The engine reserves every filename starting with `_` at the graph root
//! (`_schema.pg`, `__manifest/`, `__recovery/`, ...), so an id contains no
//! underscore at all. Path traversal (`..`, `/`) and dot-containing names
//! (`openapi.json`) are unrepresentable. The reserved names are the route
//! prefixes and top-level endpoint names the character rule alone would admit.

/// Maximum length of a graph id.
pub const GRAPH_ID_MAX_LEN: usize = 64;

const RESERVED_GRAPH_IDS: [&str; 4] = ["policies", "healthz", "openapi", "graphs"];

/// Why `value` is not a graph id the server serves, or `None` when it is one.
pub fn graph_id_violation(value: &str) -> Option<String> {
    if value.is_empty() {
        return Some("graph_id must not be empty".to_string());
    }
    if value.len() > GRAPH_ID_MAX_LEN {
        return Some(format!(
            "graph_id '{value}' is {} chars; max {GRAPH_ID_MAX_LEN}",
            value.len()
        ));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Some(format!(
            "graph_id '{value}' must match ^[a-zA-Z0-9-]{{1,64}}$ — \
             no underscores (engine reserves them), no path separators, no unicode"
        ));
    }
    if RESERVED_GRAPH_IDS.contains(&value) {
        return Some(format!(
            "graph_id '{value}' is reserved (would collide with engine-managed names or \
             future cluster routes)"
        ));
    }
    None
}

/// Why `value` cannot be declared as a graph in `cluster.yaml`, or `None` when
/// it can: the server's rule ([`graph_id_violation`]) and a letter first.
///
/// Deployment addresses and recorded state check graph ids with the wider
/// resource-name grammar (`config::validate_id`: a letter or `_` first, then
/// letters, digits, `_` or `-`). That stays wider on purpose: a graph an older
/// binary applied under an id this rule refuses must still be removable by
/// taking it out of `cluster.yaml`. Every id this rule admits is inside that
/// grammar.
pub(crate) fn declared_graph_id_violation(value: &str) -> Option<String> {
    graph_id_violation(value).or_else(|| {
        (!value.starts_with(|ch: char| ch.is_ascii_alphabetic()))
            .then(|| format!("graph_id '{value}' must start with an ASCII letter"))
    })
}
