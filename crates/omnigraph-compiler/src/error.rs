use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSpan {
    pub start: usize,
    pub end: usize,
}

impl SourceSpan {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseDiagnostic {
    pub message: String,
    pub span: Option<SourceSpan>,
}

impl ParseDiagnostic {
    pub fn new(message: String, span: Option<SourceSpan>) -> Self {
        Self { message, span }
    }
}

impl std::fmt::Display for ParseDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ParseDiagnostic {}

pub fn render_span(span: SourceSpan) -> SourceSpan {
    SourceSpan {
        start: span.start,
        end: span.end.max(span.start.saturating_add(1)),
    }
}

pub fn decode_string_literal(raw: &str) -> Result<String> {
    let inner = raw
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
        .unwrap_or(raw);

    let mut decoded = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }

        let escaped = chars
            .next()
            .ok_or_else(|| NanoError::Parse("unterminated escape sequence".to_string()))?;
        match escaped {
            '"' => decoded.push('"'),
            '\\' => decoded.push('\\'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            't' => decoded.push('\t'),
            other => {
                return Err(NanoError::Parse(format!(
                    "unsupported escape sequence: \\{}",
                    other
                )));
            }
        }
    }

    Ok(decoded)
}

#[derive(Debug, Error)]
pub enum NanoError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error(
        "@unique constraint violation on {type_name}.{property}: duplicate value '{value}' at rows {first_row} and {second_row}"
    )]
    UniqueConstraint {
        type_name: String,
        property: String,
        value: String,
        first_row: usize,
        second_row: usize,
    },

    #[error("plan error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("lance error: {0}")]
    Lance(String),

    #[error("manifest error: {0}")]
    Manifest(String),
}

pub type Result<T> = std::result::Result<T, NanoError>;

#[cfg(test)]
mod tests {
    use super::{SourceSpan, decode_string_literal, render_span};

    #[test]
    fn source_span_preserves_zero_width() {
        let span = SourceSpan::new(7, 7);
        assert_eq!(span.start, 7);
        assert_eq!(span.end, 7);
    }

    #[test]
    fn render_span_widens_zero_width_for_diagnostics() {
        let rendered = render_span(SourceSpan::new(7, 7));
        assert_eq!(rendered.start, 7);
        assert_eq!(rendered.end, 8);
    }

    #[test]
    fn decode_string_literal_supports_common_escapes() {
        let decoded = decode_string_literal("\"a\\n\\r\\t\\\\\\\"b\"").unwrap();
        assert_eq!(decoded, "a\n\r\t\\\"b");
    }
}
