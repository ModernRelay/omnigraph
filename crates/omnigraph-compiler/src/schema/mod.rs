pub mod ast;
pub mod parser;

/// Names owned by Lance's virtual row-address and row-version columns.
///
/// Keep this compiler-owned list exact rather than depending on Lance here:
/// `omnigraph-compiler` deliberately has no storage-substrate dependency. The
/// engine's Lance surface guards pin the five surveyed upstream constants;
/// every Lance bump still requires a source audit for newly added names.
/// These five cannot exist inside a stored schema (Lance itself would have
/// collided), so this predicate is safe to enforce at every IR validation,
/// including stored-catalog load.
pub(crate) fn is_reserved_storage_system_column(name: &str) -> bool {
    matches!(
        name,
        "_rowid"
            | "_rowaddr"
            | "_rowoffset"
            | "_row_created_at_version"
            | "_row_last_updated_at_version"
    )
}

/// `_distance` and `_score` are Lance's search output names: the engine ranks
/// search-ordered results by those columns, so a same-named user property
/// could silently shadow the ranking data. Unlike the storage system columns
/// above, these names CAN exist in schemas created before the reservation, so
/// they are enforced only where a NEW declaration enters (`.pg` parse), never
/// against a stored catalog — an existing graph keeps opening.
pub(crate) fn is_reserved_search_output_column(name: &str) -> bool {
    matches!(name, "_distance" | "_score")
}
