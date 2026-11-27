//! The front door every caller parses through.

use crate::error::{CompilerError, Result};

use super::depth::check_query_nesting;
use super::diagnostic::QueryDiagnostic;
use super::file::QueryFile;
use super::syntax::parse;

pub fn parse_query(input: &str) -> Result<QueryFile> {
    parse_query_diagnostic(input).map_err(CompilerError::query)
}

/// `input` after the grammar's `WHITESPACE` and `COMMENT`: an unterminated
/// block comment consumes the rest, as it does in the parser.
pub(crate) fn skip_trivia(mut input: &str) -> &str {
    loop {
        let trimmed = input.trim_start_matches([' ', '\t', '\r', '\n']);
        if let Some(rest) = trimmed.strip_prefix("//") {
            input = rest.split_once('\n').map_or("", |(_, tail)| tail);
        } else if let Some(rest) = trimmed.strip_prefix("/*") {
            input = rest.split_once("*/").map_or("", |(_, tail)| tail);
        } else {
            return trimmed;
        }
    }
}

/// Whether `input` opens with a settings statement: `set` or `reset` as the
/// first token after leading whitespace and comments, closed by a word
/// boundary as the grammar's keywords are. The gate a caller that needs only
/// the prefix takes before `parse_query`, so a source without one is never
/// parsed for it.
pub fn has_settings_prefix(input: &str) -> bool {
    let rest = skip_trivia(input);
    ["set", "reset"].iter().any(|keyword| {
        rest.strip_prefix(keyword).is_some_and(|after| {
            !after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
        })
    })
}

/// Parse `input` into its file body, refusing with a positioned diagnostic:
/// the nesting bound first, then the grammar. Each declaration carries its
/// parse tree ([`super::file::QueryDecl::syntax`]).
pub fn parse_query_diagnostic(input: &str) -> std::result::Result<QueryFile, QueryDiagnostic> {
    check_query_nesting(input)?;
    parse::parse_source(input).map(parse::into_query_file)
}
