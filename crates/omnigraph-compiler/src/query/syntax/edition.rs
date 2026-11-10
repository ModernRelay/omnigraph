//! The edition line (docs/rfcs/2026-11-07-gq-3.md, section 6): `gq 3;`
//! before the settings lines selects edition 3; no line, or `gq 2;`, is
//! edition 2; a malformed line is refused with `gq 3;` as the fix.

use super::parse::{MACHINE, Refusal, edit, refusal};
use crate::query::codes::Q022;

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

/// The edition a source's first line selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditionLine {
    /// No `gq N;` line.
    Absent,
    /// `gq N;` spanning `start..end` in the source.
    Present {
        edition: u64,
        start: usize,
        end: usize,
    },
}

/// Read the edition line: `gq`, word-bounded, an integer and `;`, after
/// leading whitespace and comments. Anything else is `Absent`.
pub fn edition_line(input: &str) -> EditionLine {
    let rest = skip_trivia(input);
    let start = input.len() - rest.len();
    let Some(after) = rest.strip_prefix("gq") else {
        return EditionLine::Absent;
    };
    if after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
        return EditionLine::Absent;
    }
    let after = skip_trivia(after);
    let digits = after.len() - after.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    if digits == 0 {
        return EditionLine::Absent;
    }
    let Ok(edition) = after[..digits].parse::<u64>() else {
        return EditionLine::Absent;
    };
    let after = skip_trivia(&after[digits..]);
    let Some(tail) = after.strip_prefix(';') else {
        return EditionLine::Absent;
    };
    EditionLine::Present {
        edition,
        start,
        end: input.len() - tail.len(),
    }
}

/// A malformed edition line (section 5): `gq 3` without `;`, `GQ 3;`, or a
/// settings line before it. Refused with `gq 3;` as the fix. `None` when the
/// source has no edition line at all, or a well-formed one.
pub fn edition_near_miss(input: &str) -> Option<Refusal> {
    if !matches!(edition_line(input), EditionLine::Absent) {
        return None;
    }
    // The candidate line, after any settings lines.
    let mut cursor = input.len() - skip_trivia(input).len();
    let first = cursor;
    let mut settings_end = None;
    loop {
        let rest = &input[cursor..];
        let word_end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        let word = &rest[..word_end];
        if word == "set" || word == "reset" {
            let semi = rest.find(';')?;
            cursor += semi + 1;
            settings_end = Some(cursor);
            cursor = input.len() - skip_trivia(&input[cursor..]).len();
            continue;
        }
        break;
    }
    let rest = &input[cursor..];
    if !rest.get(..2).is_some_and(|w| w.eq_ignore_ascii_case("gq")) {
        return None;
    }
    let after = &rest[2..];
    if after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    let ws = after.len() - after.trim_start().len();
    let digits = after[ws..].len()
        - after[ws..]
            .trim_start_matches(|c: char| c.is_ascii_digit())
            .len();
    if digits == 0 {
        return None;
    }
    let mut end = cursor + 2 + ws + digits;
    let tail = &input[end..];
    let tail_ws = tail.len() - tail.trim_start_matches([' ', '\t']).len();
    let has_semi = tail[tail_ws..].starts_with(';');
    if has_semi {
        end += tail_ws + 1;
    }
    let line = &input[cursor..end];
    let number = &line[2 + ws..2 + ws + digits];
    let fixed = format!("gq {number};");
    let (message, edits, span) = match settings_end {
        Some(_) => (
            "the edition line comes first, before every settings line".to_string(),
            vec![edit(first, first, format!("{fixed}\n")), {
                // Delete the misplaced line and the whitespace before it.
                let before = input[..cursor].trim_end_matches([' ', '\t', '\r', '\n']);
                edit(before.len(), end, "")
            }],
            (first, end),
        ),
        None => (
            if has_semi {
                format!(
                    "`{line}` is not the edition line; it is lowercase `gq`, the edition and `;`"
                )
            } else {
                format!(
                    "`{line}` is missing its `;`; the edition line is `gq`, the edition and `;`"
                )
            },
            vec![edit(cursor, end, fixed)],
            (cursor, end),
        ),
    };
    Some(refusal(input, Q022, message, span, edits, MACHINE))
}
