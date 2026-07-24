pub mod ast;
pub mod parser;

/// Names owned by Lance's virtual columns or by OmniGraph's physical protocol.
///
/// Keep this compiler-owned list exact rather than depending on Lance here:
/// `omnigraph-compiler` deliberately has no storage-substrate dependency. The
/// The engine's Lance surface guards pin the five surveyed upstream constants;
/// every Lance bump still requires a source audit for newly added names.  The
/// final name is OmniGraph's nullable RFC-026 trusted-attribution envelope. Its
/// trailing `$` is deliberately outside the `.pg` identifier grammar; this
/// check still protects hand-authored accepted SchemaIR.
pub(crate) fn is_reserved_storage_system_column(name: &str) -> bool {
    name.eq_ignore_ascii_case("__omnigraph_stream_v1$")
        || matches!(
            name,
            "_rowid"
            | "_rowaddr"
            | "_rowoffset"
            | "_row_created_at_version"
            | "_row_last_updated_at_version"
        )
}
