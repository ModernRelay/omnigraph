//! The parser's refusals as values: a stable code per rule, the source line
//! the rule fired on, what is wrong and what to write instead.

use std::fmt;

use serde_json::{Value, json};

/// One rule the case format refuses on. `name()` is the stable spelling a
/// log, a golden and a README anchor use; `rule()` states the rule itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaseErrorCode {
    LineEndings,
    HeaderShape,
    UnknownHeaderKey,
    DuplicateHeader,
    MissingIssue,
    MissingRedOn,
    FileName,
    MissingRunner,
    UnknownSection,
    ExpectModeMissing,
    UnknownExpectMode,
    MissingExpect,
    CorpusTimeout,
    Other,
}

impl CaseErrorCode {
    pub const ALL: [CaseErrorCode; 14] = [
        CaseErrorCode::LineEndings,
        CaseErrorCode::HeaderShape,
        CaseErrorCode::UnknownHeaderKey,
        CaseErrorCode::DuplicateHeader,
        CaseErrorCode::MissingIssue,
        CaseErrorCode::MissingRedOn,
        CaseErrorCode::FileName,
        CaseErrorCode::MissingRunner,
        CaseErrorCode::UnknownSection,
        CaseErrorCode::ExpectModeMissing,
        CaseErrorCode::UnknownExpectMode,
        CaseErrorCode::MissingExpect,
        CaseErrorCode::CorpusTimeout,
        CaseErrorCode::Other,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            CaseErrorCode::LineEndings => "line_endings",
            CaseErrorCode::HeaderShape => "header_shape",
            CaseErrorCode::UnknownHeaderKey => "unknown_header_key",
            CaseErrorCode::DuplicateHeader => "duplicate_header",
            CaseErrorCode::MissingIssue => "missing_issue",
            CaseErrorCode::MissingRedOn => "missing_red_on",
            CaseErrorCode::FileName => "file_name",
            CaseErrorCode::MissingRunner => "missing_runner",
            CaseErrorCode::UnknownSection => "unknown_section",
            CaseErrorCode::ExpectModeMissing => "expect_mode_missing",
            CaseErrorCode::UnknownExpectMode => "unknown_expect_mode",
            CaseErrorCode::MissingExpect => "missing_expect",
            CaseErrorCode::CorpusTimeout => "corpus_timeout",
            CaseErrorCode::Other => "invalid_case",
        }
    }

    pub fn rule(&self) -> &'static str {
        match self {
            CaseErrorCode::LineEndings => "a case file is UTF-8 with `\\n` line endings",
            CaseErrorCode::HeaderShape => {
                "every line before the first section is a header `# <key>: <value>`"
            }
            CaseErrorCode::UnknownHeaderKey => {
                "a header key is one of issue, red_on, notes, traversal, csv_slice_bytes, overwrite_chunk_rows, writer"
            }
            CaseErrorCode::DuplicateHeader => "a header key appears at most once",
            CaseErrorCode::MissingIssue => {
                "every case carries `# issue: <N>` or `# issue: none` as a header"
            }
            CaseErrorCode::MissingRedOn => {
                "a case whose `# issue:` names a number carries `# red_on:` naming where the red showed"
            }
            CaseErrorCode::FileName => {
                "an issue case is `issue_<N>_<short_name>.gqt` matching its `# issue:`; a feature case is `<short_name>.gqt` over [a-z0-9_]"
            }
            CaseErrorCode::MissingRunner => {
                "the first section is `--- runner` with explicit configuration"
            }
            CaseErrorCode::UnknownSection => "a section header names a known section kind",
            CaseErrorCode::ExpectModeMissing => {
                "every `--- expect` names its mode; the modes depend on the step kind"
            }
            CaseErrorCode::UnknownExpectMode => {
                "an expect mode is ok, unordered, ordered, error: <needle>, affected: nodes=<N> edges=<M> or outcome: <word>"
            }
            CaseErrorCode::MissingExpect => "every step is followed by its `--- expect` section",
            CaseErrorCode::CorpusTimeout => {
                "a case under the required corpus (`crates/omnigraph-gqt/cases/`) declares `timeout_ms` of at most 10000"
            }
            CaseErrorCode::Other => "a rule the parser states in its message",
        }
    }
}

/// One refusal: the code, the line it fired on with that line's text, what
/// is wrong, and what to write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseError {
    pub code: CaseErrorCode,
    pub line: Option<usize>,
    pub source: Option<String>,
    pub message: String,
    pub fix: Option<String>,
}

impl CaseError {
    pub fn new(code: CaseErrorCode, message: impl Into<String>) -> Self {
        CaseError {
            code,
            line: None,
            source: None,
            message: message.into(),
            fix: None,
        }
    }

    pub fn at_line(mut self, line: usize) -> Self {
        self.line = Some(line);
        self
    }

    pub fn fix(mut self, fix: impl Into<String>) -> Self {
        self.fix = Some(fix.into());
        self
    }

    /// A line the error did not name yet, and the text of the line it has.
    pub fn located(mut self, fallback_line: usize, lines: &[&str]) -> Self {
        let line = *self.line.get_or_insert(fallback_line);
        if self.source.is_none() {
            self.source = line
                .checked_sub(1)
                .and_then(|idx| lines.get(idx))
                .map(|text| text.to_string());
        }
        self
    }

    pub fn to_json(&self) -> Value {
        json!({
            "code": self.code.name(),
            "line": self.line,
            "source": self.source,
            "message": self.message,
            "fix": self.fix,
        })
    }
}

/// `line N:` and `invalid_case:` prefixes, in either order, read off a
/// message written before errors carried a line of their own.
fn split_prefixes(message: &str) -> (Option<usize>, &str) {
    let mut rest = message.trim();
    let mut line = None;
    loop {
        if let Some(after) = rest.strip_prefix("invalid_case:") {
            rest = after.trim_start();
            continue;
        }
        if let Some(after) = rest.strip_prefix("line ") {
            let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
            if let Some(after_digits) = after[digits.len()..].strip_prefix(':')
                && !digits.is_empty()
                && line.is_none()
            {
                line = digits.parse().ok();
                rest = after_digits.trim_start();
                continue;
            }
        }
        break;
    }
    (line, rest)
}

impl From<String> for CaseError {
    fn from(message: String) -> Self {
        let (line, rest) = split_prefixes(&message);
        CaseError {
            code: CaseErrorCode::Other,
            line,
            source: None,
            message: rest.to_string(),
            fix: None,
        }
    }
}

impl From<&str> for CaseError {
    fn from(message: &str) -> Self {
        CaseError::from(message.to_string())
    }
}

impl fmt::Display for CaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid_case: ")?;
        if let Some(line) = self.line {
            write!(f, "line {line}: ")?;
        }
        write!(f, "{}", self.message)?;
        if let Some(fix) = &self.fix {
            write!(f, "; {fix}")?;
        }
        Ok(())
    }
}

impl std::error::Error for CaseError {}

/// Every refusal one parse found, never empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseErrors(Vec<CaseError>);

impl CaseErrors {
    pub fn new(errors: Vec<CaseError>) -> Self {
        debug_assert!(!errors.is_empty(), "a refusal names at least one error");
        CaseErrors(errors)
    }

    pub fn one(error: CaseError) -> Self {
        CaseErrors(vec![error])
    }

    pub fn iter(&self) -> impl Iterator<Item = &CaseError> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn first(&self) -> &CaseError {
        &self.0[0]
    }

    pub fn into_vec(self) -> Vec<CaseError> {
        self.0
    }
}

impl fmt::Display for CaseErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, error) in self.0.iter().enumerate() {
            if idx > 0 {
                writeln!(f)?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for CaseErrors {}

impl From<CaseErrors> for String {
    fn from(errors: CaseErrors) -> Self {
        errors.to_string()
    }
}

impl From<CaseError> for CaseErrors {
    fn from(error: CaseError) -> Self {
        CaseErrors::one(error)
    }
}

/// The candidate within two edits of `word`, when one exists: the spelling
/// a near miss meant.
pub fn nearest<'a>(word: &str, candidates: &[&'a str]) -> Option<&'a str> {
    candidates
        .iter()
        .map(|candidate| (edit_distance(word, candidate), *candidate))
        .filter(|(distance, _)| *distance <= 2)
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, candidate)| candidate)
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let substitution = previous[j] + usize::from(ca != cb);
            current[j + 1] = substitution.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b.len()]
}

/// `did you mean X?` when a near miss exists, else the whole list.
pub fn suggestion(word: &str, what: &str, candidates: &[&str]) -> String {
    match nearest(word, candidates) {
        Some(candidate) => format!("did you mean `{candidate}`?"),
        None => format!("{what}: {}", candidates.join(" | ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_in_either_order_become_the_line() {
        let a = CaseError::from("line 12: invalid_case: unknown section `--- expects`");
        let b = CaseError::from("invalid_case: line 12: unknown section `--- expects`");
        assert_eq!(a.line, Some(12));
        assert_eq!(a, b);
        assert_eq!(a.message, "unknown section `--- expects`");
        assert_eq!(
            a.to_string(),
            "invalid_case: line 12: unknown section `--- expects`"
        );
    }

    #[test]
    fn a_message_without_prefixes_keeps_no_line() {
        let error = CaseError::from("the final step is missing its `--- expect`");
        assert_eq!(error.line, None);
        assert_eq!(
            error.to_string(),
            "invalid_case: the final step is missing its `--- expect`"
        );
    }

    #[test]
    fn located_fills_the_line_and_its_text_once() {
        let lines = ["# issue: none", "--- runner", "timeout_ms: 10000"];
        let error = CaseError::from("needs a value").located(2, &lines);
        assert_eq!(error.line, Some(2));
        assert_eq!(error.source.as_deref(), Some("--- runner"));
        let kept = CaseError::from("line 3: x").located(1, &lines);
        assert_eq!(kept.line, Some(3));
        assert_eq!(kept.source.as_deref(), Some("timeout_ms: 10000"));
    }

    #[test]
    fn nearest_is_within_two_edits_or_nothing() {
        let kinds = ["expect", "query", "mutate", "runner"];
        assert_eq!(nearest("expects", &kinds), Some("expect"));
        assert_eq!(nearest("mutte", &kinds), Some("mutate"));
        assert_eq!(nearest("params", &kinds), None);
    }

    #[test]
    fn every_code_has_a_distinct_name_and_a_rule() {
        let mut names: Vec<&str> = CaseErrorCode::ALL.iter().map(CaseErrorCode::name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), CaseErrorCode::ALL.len());
        assert!(
            CaseErrorCode::ALL
                .iter()
                .all(|code| !code.rule().is_empty())
        );
    }
}
