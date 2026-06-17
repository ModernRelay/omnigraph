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
            .ok_or_else(|| CompilerError::Parse("unterminated escape sequence".to_string()))?;
        match escaped {
            '"' => decoded.push('"'),
            '\\' => decoded.push('\\'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            't' => decoded.push('\t'),
            other => {
                return Err(CompilerError::Parse(format!(
                    "unsupported escape sequence: \\{}",
                    other
                )));
            }
        }
    }

    Ok(decoded)
}

#[derive(Debug, Error)]
pub enum CompilerError {
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

#[deprecated(note = "use CompilerError")]
pub type NanoError = CompilerError;

pub type Result<T> = std::result::Result<T, CompilerError>;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{CompilerError, SourceSpan, decode_string_literal, render_span};

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

    #[test]
    fn compiler_error_parse_display_is_stable() {
        let err = CompilerError::Parse("bad token".to_string());
        assert_eq!(err.to_string(), "parse error: bad token");
    }

    #[allow(deprecated)]
    #[test]
    fn legacy_nano_error_alias_constructs_variants() {
        let err = super::NanoError::Parse("bad token".to_string());
        assert_eq!(err.to_string(), "parse error: bad token");
    }

    #[test]
    fn legacy_name_is_confined_to_alias_and_compatibility_test() {
        let legacy_name = ["Nano", "Error"].concat();
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("compiler crate should live under crates/");
        let allowed_file = workspace_root.join("crates/omnigraph-compiler/src/error.rs");
        let mut offenders = Vec::new();

        visit_rs_files(&workspace_root.join("crates"), &mut |path| {
            let text = std::fs::read_to_string(path).expect("source file should be readable");
            let count = text.matches(&legacy_name).count();
            if path == allowed_file {
                if count != 2 {
                    offenders.push(format!(
                        "{} contains {count} legacy-name occurrences; expected exactly 2",
                        display_path(workspace_root, path)
                    ));
                }
            } else if count > 0 {
                offenders.push(format!(
                    "{} contains {count} legacy-name occurrence(s)",
                    display_path(workspace_root, path)
                ));
            }
        });

        assert!(
            offenders.is_empty(),
            "legacy compiler error name should stay compatibility-only:\n{}",
            offenders.join("\n")
        );
    }

    fn visit_rs_files(dir: &Path, visit: &mut impl FnMut(&Path)) {
        for entry in std::fs::read_dir(dir).expect("source directory should be readable") {
            let entry = entry.expect("source entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                visit_rs_files(&path, visit);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                visit(&path);
            }
        }
    }

    fn display_path(root: &Path, path: &Path) -> String {
        path.strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    }
}
