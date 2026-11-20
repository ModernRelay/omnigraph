//! The graph id rule: the one predicate both `cluster.yaml` validation and the
//! server's `GraphId` apply, so a configuration that applies is one the server
//! can serve.
//!
//! A graph id is 1–64 ASCII letters, digits or hyphens (`^[a-zA-Z0-9-]{1,64}$`),
//! and not a reserved name.
//!
//! The engine reserves every filename starting with `_` at the graph root
//! (`_schema.pg`, `__manifest/`, `__recovery/`, ...), so an id contains no
//! underscore at all. Path traversal (`..`, `/`) and dot-containing names
//! (`openapi.json`) are unrepresentable. The reserved names are the route
//! prefixes and top-level endpoint names the character rule alone would admit.

/// Maximum length of a graph id.
pub const GRAPH_ID_MAX_LEN: usize = 64;

const RESERVED_GRAPH_IDS: [&str; 4] = ["policies", "healthz", "openapi", "graphs"];

/// Why `value` is not a graph id, or `None` when it is one.
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
