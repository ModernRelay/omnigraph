pub mod ast;
pub mod parser;

/// Names owned by Lance's virtual row-address and row-version columns.
///
/// Keep this compiler-owned list exact rather than depending on Lance here:
/// `omnigraph-compiler` deliberately has no storage-substrate dependency. The
/// engine's Lance surface guards pin these five surveyed upstream constants;
/// every Lance bump still requires a source audit for newly added names.
/// The reserved system namespace: every property name starting with `_`
/// (RFC 0040). Covers Lance's virtual columns and OmniGraph's own implicit
/// stored columns, present and future; enforced at schema admission and
/// again for current-version IRs in `validate_schema_ir`.
pub(crate) fn is_reserved_system_column_name(name: &str) -> bool {
    name.starts_with('_')
}

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
