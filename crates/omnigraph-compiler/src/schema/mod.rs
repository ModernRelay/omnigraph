pub mod ast;
pub mod parser;

/// Names owned by Lance's virtual row-address and row-version columns.
///
/// Keep this compiler-owned list exact rather than depending on Lance here:
/// `omnigraph-compiler` deliberately has no storage-substrate dependency. The
/// engine's Lance surface guards pin the five surveyed upstream constants;
/// every Lance bump still requires a source audit for newly added names.
/// `_distance` and `_score` are additionally reserved as Lance's search
/// output names: the engine ranks search-ordered results by those columns,
/// so a same-named user property could silently shadow the ranking data.
pub(crate) fn is_reserved_storage_system_column(name: &str) -> bool {
    matches!(
        name,
        "_rowid"
            | "_rowaddr"
            | "_rowoffset"
            | "_row_created_at_version"
            | "_row_last_updated_at_version"
            | "_distance"
            | "_score"
    )
}
