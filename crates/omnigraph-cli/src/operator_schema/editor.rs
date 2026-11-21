//! A configuration edit has two representations: the authored YAML and the
//! intended change to its unresolved values. Both must agree before saving.

use super::{OperatorConfig, parse_source, resolve};
use color_eyre::eyre::Result;
use serde_json::Value;
use std::path::Path;
use yaml_rt::{JsonPointer, NodeKind, Span, TokenKind, YamlDoc, YamlFragment, YamlValue};

pub(crate) struct Editor {
    document: YamlDoc,
    expected: yaml_serde::Value,
    crlf: bool,
}

fn not_editable() -> color_eyre::Report {
    crate::targets::refusal(
        "config_not_editable",
        "could not isolate the requested edit in the operator YAML; edit the file manually",
    )
}

impl Editor {
    pub(crate) fn open(text: &str, base: &Path) -> Result<(OperatorConfig, Self)> {
        let expected = parse_source(text)?;
        let config = resolve(expected.clone(), base)?;
        let document = YamlDoc::parse(text).map_err(|_| not_editable())?;
        let crlf =
            text.contains("\r\n") && text.split("\r\n").all(|line| !line.contains(['\r', '\n']));
        Ok((
            config,
            Self {
                document,
                expected,
                crlf,
            },
        ))
    }

    pub(crate) fn finish(self, base: &Path) -> Result<String> {
        // Give ordinary validation errors (e.g. removing a referenced target)
        // even when the lossless emitter cannot represent a malformed result.
        resolve(self.expected.clone(), base)?;
        let mut text = self.document.to_string();
        // Block fragments emitted with LF must use the existing document's
        // line ending. Files with mixed endings keep their authored bytes.
        if self.crlf {
            text = text.replace("\r\n", "\n").replace('\n', "\r\n");
        }
        let actual = parse_source(&text).map_err(|_| not_editable())?;
        if actual != self.expected {
            return Err(not_editable());
        }
        Ok(text)
    }

    pub(crate) fn insert(&mut self, section: &str, name: &str, value: Value) -> Result<()> {
        if self.expected.get(section).is_none() {
            return self.put(&[section], &serde_json::json!({ name: value }));
        }
        self.put(&[section, name], &value)
    }

    pub(crate) fn put(&mut self, path: &[&str], value: &Value) -> Result<()> {
        if let Some(node) = self.document.get_path(path).map_err(|_| not_editable())? {
            // Replacement is used only for string references and endpoints.
            // A block scalar owns its header comment and trailing line break;
            // leave that presentation to the operator if it cannot be retained.
            if self.document.node(node).ok_or_else(not_editable)?.kind() != NodeKind::Scalar {
                return Err(not_editable());
            }
            let string = value.as_str().ok_or_else(not_editable)?;
            // Even a valid local name can be a YAML keyword. Probe the flow
            // spelling as it has stricter delimiters than a block scalar.
            let plain = yaml_serde::from_str::<Vec<yaml_serde::Value>>(&format!("[{string}]"))
                .is_ok_and(
                    |items| matches!(items.as_slice(), [one] if one.as_str() == Some(string)),
                );
            if plain {
                self.document
                    .set_scalar(path, string)
                    .map_err(|_| not_editable())?;
            } else {
                string
                    .to_owned()
                    .write_yaml(&mut self.document, Some(node))
                    .map_err(|_| not_editable())?;
            }
        } else {
            let fragment = YamlFragment::parse_owned(yaml_serde::to_string(value)?)
                .map_err(|_| not_editable())?;
            let before = self.document.as_source().to_owned();
            self.document
                .add_at(0, &pointer(path)?, &fragment)
                .map_err(|_| not_editable())?;
            // For example, appending after an unterminated |+ scalar can
            // cause an emitter to change its chomping indicator elsewhere.
            if !single_insertion(before.as_bytes(), self.document.as_source().as_bytes()) {
                return Err(not_editable());
            }
        }
        let mut slot = &mut self.expected;
        for key in path {
            slot = &mut slot[*key];
        }
        *slot = yaml_serde::to_value(value)?;
        Ok(())
    }

    pub(crate) fn remove(&mut self, section: &str, name: &str) -> Result<()> {
        let entries = self.expected[section]
            .as_mapping_mut()
            .ok_or_else(not_editable)?;
        entries.remove(name).ok_or_else(not_editable)?;
        let map = self
            .document
            .get_path(&[section])
            .map_err(|_| not_editable())?
            .ok_or_else(not_editable)?;
        let entry = self
            .document
            .get_mapping_entry(map, name)
            .map_err(|_| not_editable())?
            .ok_or_else(not_editable)?;
        if self.document.node(map).ok_or_else(not_editable)?.kind() == NodeKind::FlowMapping {
            let range = self.document.node(entry).ok_or_else(not_editable)?.span();
            self.document = remove_flow_entry(&self.document, range)?;
        } else if entries.is_empty() {
            self.document
                .replace_node_text(map, "{}")
                .map_err(|_| not_editable())?;
        } else {
            // Use the entry's own lines. remove_at additionally consumes blank
            // lines up to the next entry, even when they separate two sections.
            self.document
                .remove_node(entry)
                .map_err(|_| not_editable())?;
        }
        Ok(())
    }

    pub(crate) fn rename(&mut self, section: &str, from: &str, to: &str) -> Result<()> {
        let values = self.expected[section]
            .as_mapping_mut()
            .ok_or_else(not_editable)?;
        let original = values.remove(from).ok_or_else(not_editable)?;
        values.insert(to.into(), original);
        self.document
            .rename_key_at(0, &pointer(&[section, from])?, to)
            .map_err(|_| not_editable())
    }

    pub(crate) fn set_reference(&mut self, path: &[&str], value: &str) -> Result<()> {
        self.put(path, &Value::String(value.to_owned()))
    }
}

fn single_insertion(before: &[u8], after: &[u8]) -> bool {
    let Some(added) = after.len().checked_sub(before.len()) else {
        return false;
    };
    let common = before.iter().zip(after).take_while(|(a, b)| a == b).count();
    before[common..] == after[common + added..]
}

// A flow separator may have a neighbouring key's comments or line breaks on
// either side. Remove only the member's span and one comma token.
fn remove_flow_entry(document: &YamlDoc, entry: Span) -> Result<YamlDoc> {
    let tokens = document.tokens().map_err(|_| not_editable())?;
    let significant = tokens.iter().filter(|token| {
        !matches!(
            token.kind,
            TokenKind::Whitespace | TokenKind::Newline | TokenKind::Comment
        )
    });
    let after = significant
        .clone()
        .find(|token| token.span.start >= entry.end);
    let comma = after
        .filter(|token| token.kind == TokenKind::Comma)
        .or_else(|| {
            significant
                .rev()
                .find(|token| token.span.end <= entry.start)
                .filter(|token| token.kind == TokenKind::Comma)
        });
    let mut spans = vec![entry];
    if let Some(comma) = comma {
        spans.push(comma.span);
    }
    spans.sort_by_key(|span| std::cmp::Reverse(span.start));
    let mut source = document.as_source().to_owned();
    for span in spans {
        source.replace_range(span.start as usize..span.end as usize, "");
    }
    YamlDoc::parse_owned(source).map_err(|_| not_editable())
}

fn pointer(path: &[&str]) -> Result<JsonPointer> {
    // Components are schema fields or validated target/profile names.
    JsonPointer::parse(&format!("/{}", path.join("/"))).map_err(|_| not_editable())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserting_a_section_preserves_crlf_issue_1169() {
        let source = "# 控制台配置\r\nversion: 1\r\n";
        let dir = tempfile::tempdir().unwrap();
        let (_, mut editor) = Editor::open(source, dir.path()).unwrap();
        editor.insert("targets", "db", serde_json::json!({
            "kind": "server", "endpoint": "https://db.example.test", "auth": {"provider":"anonymous"}
        })).unwrap();
        let actual = editor.finish(dir.path()).unwrap();
        assert!(actual.starts_with(source));
        assert!(
            actual
                .split("\r\n")
                .all(|line| !line.contains(['\r', '\n']))
        );
    }

    #[test]
    fn removing_a_flow_entry_keeps_neighbouring_comments_and_crlf_issue_1169() {
        let server =
            "{kind: server, endpoint: 'https://example.test', auth: {provider: anonymous}}";
        let source = format!(
            "version: 1\r\ntargets: {{a: {server},\r\n  # keep this comma, and the next target\r\n  b: {server}}}\r\n"
        );
        let dir = tempfile::tempdir().unwrap();
        for name in ["a", "b"] {
            let (_, mut editor) = Editor::open(&source, dir.path()).unwrap();
            editor.remove("targets", name).unwrap();
            let actual = editor.finish(dir.path()).unwrap();
            let expected = source
                .replacen(&format!("{name}: {server}"), "", 1)
                .replacen(",\r\n", "\r\n", 1);
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn inserting_after_an_unterminated_scalar_keeps_its_chomping_indicator_issue_1169() {
        let source = "version: 1\ntargets:\n  a:\n    kind: server\n    auth: {provider: anonymous}\n    endpoint: |+\n      https://example.test";
        let dir = tempfile::tempdir().unwrap();
        let (_, mut editor) = Editor::open(source, dir.path()).unwrap();
        let add = editor.insert("targets", "b", serde_json::json!({
            "kind": "server", "endpoint": "https://other.example.test", "auth":{"provider":"anonymous"}
        }));
        match add {
            Ok(()) => assert!(editor.finish(dir.path()).unwrap().starts_with(source)),
            Err(error) => assert_eq!(
                error
                    .downcast_ref::<crate::targets::TargetError>()
                    .unwrap()
                    .code,
                "config_not_editable"
            ),
        }
    }

    #[test]
    fn operator_renames_preserve_strings_that_look_like_yaml_keywords_issue_1169() {
        let dir = tempfile::tempdir().unwrap();
        let source = "version: 1\nprofiles: {work: {api: 'https://api.example.test', organization_id: org_a, auth: {provider: omnigraph}}}\ndefaults: {profile: work} # active\n";
        for new in ["true", "null"] {
            let (_, mut editor) = Editor::open(source, dir.path()).unwrap();
            editor.rename("profiles", "work", new).unwrap();
            editor.set_reference(&["defaults", "profile"], new).unwrap();
            let result = editor.finish(dir.path()).unwrap();
            assert!(result.ends_with("} # active\n"));
            let decoded = super::super::parse(&result, dir.path()).unwrap();
            assert!(decoded.profiles.contains_key(new));
            assert_eq!(decoded.defaults.profile.as_deref(), Some(new));
        }
    }

    #[test]
    fn endpoint_refresh_only_edits_the_selected_leaf_issue_1169() {
        let source = concat!(
            "version: 1\nprofiles:\n  prod: {api: 'https://api.example.test', organization_id: org_a, auth: {provider: omnigraph}}\n",
            "targets:\n  db:\n    kind: managed\n    profile: prod\n    cluster_id: cluster_a\n    cluster_incarnation: incarnation_a\n",
            "    endpoint: https://before.example.test # preserve\n"
        );
        let dir = tempfile::tempdir().unwrap();
        let (_, mut editor) = Editor::open(source, dir.path()).unwrap();
        editor
            .put(
                &["targets", "db", "endpoint"],
                &serde_json::json!("https://after.example.test"),
            )
            .unwrap();
        assert_eq!(
            editor.finish(dir.path()).unwrap(),
            source.replace("before.example", "after.example")
        );
    }
}
