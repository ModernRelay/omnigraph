//! The front door every caller parses through: the edition line selects
//! the grammar a source is read with ([`super::syntax::edition`]).

use crate::error::{CompilerError, Result};

use super::ast::QueryFile;
use super::diagnostic::{Applicability, Position, QueryDiagnostic, Suggestion, TextEdit};
use super::syntax::edition::skip_trivia;
use super::syntax::{edition_window, parse};

pub fn parse_query(input: &str) -> Result<QueryFile> {
    parse_query_diagnostic(input).map_err(CompilerError::query)
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

/// Parse `input` into its file body, refusing with a positioned diagnostic.
///
/// The one front door of every caller. The edition line selects the grammar
/// (docs/rfcs/2026-11-07-gq-3.md section 6): a source without it, or with
/// `gq 2;` (blanked so positions keep), is edition 2, unchanged; `gq 3;`
/// selects the edition-3 front end, whose declarations carry their syntax
/// tree ([`super::ast::QueryDecl::syntax`]); any other edition, and a malformed line,
/// is `Q006` with `gq 3;` as the fix.
pub fn parse_query_diagnostic(input: &str) -> std::result::Result<QueryFile, QueryDiagnostic> {
    use super::syntax::edition::{EditionLine, edition_line, edition_near_miss};
    match edition_line(input) {
        // A malformed edition line (`gq 3` without `;`, `GQ 3;`, a settings
        // line before it) is refused with `gq 3;` as the fix; no edition-2
        // text opens with `gq`, so edition 2 is unchanged.
        EditionLine::Absent => match edition_near_miss(input) {
            Some(refusal) => Err(refusal.diagnostic),
            None => edition_window::parse(input),
        },
        EditionLine::Present { edition: 3, .. } => {
            parse::parse_source(input).map(parse::into_query_file)
        }
        EditionLine::Present {
            edition: 2,
            start,
            end,
        } => {
            let mut blanked = input.to_string();
            blanked.replace_range(start..end, &" ".repeat(end - start));
            edition_window::parse(&blanked)
        }
        EditionLine::Present {
            edition,
            start,
            end,
        } => Err(QueryDiagnostic::parse(
            super::codes::Q006,
            format!("`gq {edition};` names no edition this compiler reads; it reads 2 and 3"),
            Some(Position::at(input, start)),
        )
        .with_fix("gq 3;")
        .with_suggestion(Suggestion {
            applicability: Applicability::NeedsReview,
            edits: vec![TextEdit {
                start,
                end,
                replacement: "gq 3;".to_string(),
            }],
        })),
    }
}
