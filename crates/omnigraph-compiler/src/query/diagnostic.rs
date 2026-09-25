//! The one shape every query compile diagnostic takes (RFC 0047, "Diagnostics
//! contract"): a stable code, where the failure is (a source position, or the
//! stage and expression when it is post-parse), what was expected or
//! violated, and one concrete fix. The reader is an agent that treats an
//! error as the documentation it acts on, so the fix names the construct to
//! use, not the rule that was broken; a diagnostic with no fix names the
//! decision in its message instead.
//!
//! `Display` renders the legacy one-line form (`parse error: …`,
//! `type error: T33: …`) and nothing else, so every existing assertion on the
//! rendered text keeps holding; the other fields travel as data.

use serde::Serialize;

/// The catalog entry behind a [`QueryCode`]: the stable code string and its
/// one-line meaning.
#[derive(Debug, PartialEq, Eq)]
pub struct QueryCodeSpec {
    pub code: &'static str,
    pub short: &'static str,
}

/// A stable diagnostic code, a pointer-sized handle to its catalog entry so
/// a diagnostic stays small enough to travel in a `Result`. Parse codes are
/// `Q…`, typecheck codes are `T…`; the catalog is `super::codes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryCode(pub &'static QueryCodeSpec);

impl QueryCode {
    /// The code string, e.g. `T33`.
    pub fn as_str(self) -> &'static str {
        self.0.code
    }

    /// The code's one-line meaning.
    pub fn short(self) -> &'static str {
        self.0.short
    }
}

impl Serialize for QueryCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl std::fmt::Display for QueryCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which compiler phase refused the source; decides the rendered prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryDiagnosticKind {
    Parse,
    Type,
}

/// A source position: 1-based line and column (in characters) plus the byte
/// offset the parser reported (saturated; a query source is never that long).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Position {
    pub line: u32,
    pub column: u32,
    pub byte: u32,
}

impl Position {
    /// The line and column of byte offset `byte` in `source`; an offset past
    /// the end positions at the end.
    pub fn at(source: &str, byte: usize) -> Self {
        let byte = byte.min(source.len());
        let prefix = source.get(..byte).unwrap_or(source);
        let line = prefix.matches('\n').count() + 1;
        let column = prefix
            .rsplit('\n')
            .next()
            .map_or(0, |last| last.chars().count())
            + 1;
        Self {
            line: u32::try_from(line).unwrap_or(u32::MAX),
            column: u32::try_from(column).unwrap_or(u32::MAX),
            byte: u32::try_from(byte).unwrap_or(u32::MAX),
        }
    }
}

/// Where a post-parse failure sits: the stage that refused and, when the
/// site can render it, the expression it refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Stage {
    pub name: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
}

/// One query compile diagnostic. See the module documentation for the
/// contract each field carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryDiagnostic {
    pub kind: QueryDiagnosticKind,
    pub code: QueryCode,
    /// What was expected or violated, one line, without a position.
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<Position>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<Stage>,
    /// One concrete fix, naming the construct to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
}

impl QueryDiagnostic {
    /// A parse diagnostic at `position` (the caller resolves the byte offset
    /// against the source it parsed).
    pub fn parse(code: QueryCode, message: impl Into<String>, position: Option<Position>) -> Self {
        Self {
            kind: QueryDiagnosticKind::Parse,
            code,
            message: message.into(),
            position,
            stage: None,
            fix: None,
        }
    }

    /// A typecheck diagnostic: post-parse, so it carries the stage and no
    /// position.
    pub fn typecheck(code: QueryCode, message: impl Into<String>) -> Self {
        Self {
            kind: QueryDiagnosticKind::Type,
            code,
            message: message.into(),
            position: None,
            stage: Some(Stage {
                name: "typecheck",
                expression: None,
            }),
            fix: None,
        }
    }

    pub fn with_fix(mut self, fix: impl Into<String>) -> Self {
        self.fix = Some(fix.into());
        self
    }

    pub fn with_expression(mut self, expression: impl Into<String>) -> Self {
        let expression = expression.into();
        match &mut self.stage {
            Some(stage) => stage.expression = Some(expression),
            None => {
                self.stage = Some(Stage {
                    name: "typecheck",
                    expression: Some(expression),
                })
            }
        }
        self
    }
}

impl std::fmt::Display for QueryDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            QueryDiagnosticKind::Parse => write!(f, "parse error: {}", self.message),
            QueryDiagnosticKind::Type => {
                write!(f, "type error: {}: {}", self.code, self.message)
            }
        }
    }
}

impl std::error::Error for QueryDiagnostic {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::codes::{Q001, T1};

    #[test]
    fn position_counts_lines_and_character_columns() {
        let source = "set a = 1;\nquery é() {";
        let at = Position::at(source, source.find("()").unwrap());
        assert_eq!((at.line, at.column), (2, 8));
        let end = Position::at(source, usize::MAX);
        assert_eq!(end.byte as usize, source.len());
    }

    #[test]
    fn display_keeps_the_legacy_one_line_forms() {
        let parse = QueryDiagnostic::parse(Q001, "expected `(`", Some(Position::at("x", 0)));
        assert_eq!(parse.to_string(), "parse error: expected `(`");
        let typed = QueryDiagnostic::typecheck(T1, "unknown node type `X`").with_fix("declare X");
        assert_eq!(typed.to_string(), "type error: T1: unknown node type `X`");
    }

    #[test]
    fn a_diagnostic_fits_a_result_without_boxing() {
        // Clippy's `result_large_err` threshold; the parser returns this type
        // from every helper.
        assert!(std::mem::size_of::<QueryDiagnostic>() <= 128);
    }

    #[test]
    fn serializes_the_code_as_its_string_and_omits_absent_fields() {
        let value = serde_json::to_value(QueryDiagnostic::typecheck(T1, "m")).unwrap();
        assert_eq!(value["code"], "T1");
        assert_eq!(value["stage"]["name"], "typecheck");
        assert!(value.get("position").is_none());
        assert!(value.get("fix").is_none());
    }
}
