use std::collections::BTreeMap;

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};

use crate::{
    InventoryRow, SURFACE_USER_DOCS, containing_value_fingerprint, percent_encode,
    vocabulary_matches,
};

pub(crate) const SITE_KINDS: &[&str] = &[
    "heading",
    "paragraph",
    "list_item",
    "block_quote",
    "table_cell",
    "link_label",
    "image_label",
    "inline_code",
    "fenced_code",
    "indented_code",
    "compatibility_anchor",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Container {
    Paragraph,
    ListItem,
    BlockQuote,
    TableCell,
    Link,
    Image,
}

#[derive(Debug)]
struct LogicalEvent {
    heading: String,
    site_kind: &'static str,
    value: String,
}

pub(crate) fn scan(contents: &str, source_path: &str) -> Vec<InventoryRow> {
    let mut headings: Vec<String> = Vec::new();
    let mut heading: Option<(usize, String)> = None;
    let mut containers = Vec::new();
    let mut code_block: Option<(&'static str, String)> = None;
    let mut block_text: Option<(&'static str, String)> = None;
    let mut label_text: Option<(&'static str, String)> = None;
    let mut events = Vec::new();
    let mut has_table_layout_anchor = false;

    let parser = Parser::new_ext(contents, Options::all());
    for event in parser {
        match event {
            Event::Start(Tag::Heading { level, .. }) => {
                heading = Some((heading_level(level), String::new()));
            }
            Event::End(TagEnd::Heading(_)) => {
                let Some((level, text)) = heading.take() else {
                    continue;
                };
                let normalized = normalize_markdown_text(&text);
                if normalized.is_empty() {
                    continue;
                }
                has_table_layout_anchor |= markdown_heading_slug(&normalized) == "table-layout";
                headings.truncate(level.saturating_sub(1));
                while headings.len() < level.saturating_sub(1) {
                    headings.push("<untitled>".to_owned());
                }
                headings.push(normalized.clone());
                events.push(LogicalEvent {
                    heading: headings.join(" / "),
                    site_kind: "heading",
                    value: normalized,
                });
            }
            Event::Start(Tag::Paragraph) => {
                containers.push(Container::Paragraph);
                block_text = Some((container_site_kind(&containers), String::new()));
            }
            Event::End(TagEnd::Paragraph) => {
                flush_text(&mut events, &headings, &mut block_text);
                pop_container(&mut containers, Container::Paragraph);
            }
            Event::Start(Tag::Item) => containers.push(Container::ListItem),
            Event::End(TagEnd::Item) => {
                flush_text(&mut events, &headings, &mut block_text);
                pop_container(&mut containers, Container::ListItem);
            }
            Event::Start(Tag::BlockQuote(_)) => containers.push(Container::BlockQuote),
            Event::End(TagEnd::BlockQuote(_)) => {
                flush_text(&mut events, &headings, &mut block_text);
                pop_container(&mut containers, Container::BlockQuote)
            }
            Event::Start(Tag::TableCell) => {
                containers.push(Container::TableCell);
                block_text = Some(("table_cell", String::new()));
            }
            Event::End(TagEnd::TableCell) => {
                flush_text(&mut events, &headings, &mut block_text);
                pop_container(&mut containers, Container::TableCell);
            }
            Event::Start(Tag::Link { .. }) => {
                containers.push(Container::Link);
                label_text = Some(("link_label", String::new()));
            }
            Event::End(TagEnd::Link) => {
                flush_text(&mut events, &headings, &mut label_text);
                if let Some((_, block)) = &mut block_text {
                    block.push(' ');
                }
                pop_container(&mut containers, Container::Link);
            }
            Event::Start(Tag::Image { .. }) => {
                containers.push(Container::Image);
                label_text = Some(("image_label", String::new()));
            }
            Event::End(TagEnd::Image) => {
                flush_text(&mut events, &headings, &mut label_text);
                if let Some((_, block)) = &mut block_text {
                    block.push(' ');
                }
                pop_container(&mut containers, Container::Image);
            }
            Event::Start(Tag::CodeBlock(kind)) => {
                flush_text(&mut events, &headings, &mut block_text);
                let site_kind = match kind {
                    CodeBlockKind::Fenced(_) => "fenced_code",
                    CodeBlockKind::Indented => "indented_code",
                };
                code_block = Some((site_kind, String::new()));
            }
            Event::End(TagEnd::CodeBlock) => {
                if let Some((site_kind, value)) = code_block.take() {
                    push_event(&mut events, &headings, site_kind, value);
                }
            }
            Event::Text(text) => {
                if let Some((_, buffer)) = &mut heading {
                    buffer.push_str(&text);
                } else if let Some((_, buffer)) = &mut code_block {
                    buffer.push_str(&text);
                } else if let Some((_, buffer)) = &mut label_text {
                    buffer.push_str(&text);
                } else {
                    let (_, buffer) = block_text
                        .get_or_insert_with(|| (container_site_kind(&containers), String::new()));
                    buffer.push_str(&text);
                }
            }
            Event::Code(code) => {
                if let Some((_, buffer)) = &mut heading {
                    buffer.push_str(&code);
                } else {
                    push_event(&mut events, &headings, "inline_code", code.into_string());
                    if let Some((_, block)) = &mut block_text {
                        block.push(' ');
                    }
                }
            }
            Event::SoftBreak | Event::HardBreak => {
                if let Some((_, buffer)) = &mut heading {
                    buffer.push(' ');
                } else if let Some((_, buffer)) = &mut code_block {
                    buffer.push('\n');
                } else if let Some((_, buffer)) = &mut label_text {
                    buffer.push(' ');
                } else if let Some((_, buffer)) = &mut block_text {
                    buffer.push(' ');
                }
            }
            // Attribute values and comments are not rendered text. Text nodes
            // in raw HTML are, and the legacy deep-link anchor is a shipped
            // compatibility contract with a representation-independent
            // semantic observation.
            Event::Html(html) | Event::InlineHtml(html) => {
                has_table_layout_anchor |= contains_table_layout_anchor(&html);
                let visible = visible_html_text(&html);
                if !visible.is_empty() {
                    let (_, buffer) = block_text
                        .get_or_insert_with(|| (container_site_kind(&containers), String::new()));
                    if !buffer.is_empty() {
                        buffer.push(' ');
                    }
                    buffer.push_str(&visible);
                }
            }
            // URLs/titles, comments, metadata, rules, and task markers are
            // deliberately not rendered-text vocabulary sites.
            Event::Rule
            | Event::TaskListMarker(_)
            | Event::InlineMath(_)
            | Event::DisplayMath(_)
            | Event::FootnoteReference(_) => {}
            Event::Start(_) | Event::End(_) => {}
        }
    }

    flush_text(&mut events, &headings, &mut label_text);
    flush_text(&mut events, &headings, &mut block_text);
    if has_table_layout_anchor {
        events.push(LogicalEvent {
            heading: "<document>".to_owned(),
            site_kind: "compatibility_anchor",
            value: "table-layout".to_owned(),
        });
    }

    observations(source_path, events)
}

fn flush_text(
    events: &mut Vec<LogicalEvent>,
    headings: &[String],
    text: &mut Option<(&'static str, String)>,
) {
    if let Some((site_kind, value)) = text.take() {
        push_event(events, headings, site_kind, value);
    }
}

fn container_site_kind(containers: &[Container]) -> &'static str {
    // Semantic block containers take precedence over the Paragraph wrapper
    // emitted inside them. Link/image labels are buffered separately.
    for (container, site_kind) in [
        (Container::TableCell, "table_cell"),
        (Container::ListItem, "list_item"),
        (Container::BlockQuote, "block_quote"),
        (Container::Paragraph, "paragraph"),
    ] {
        if containers.contains(&container) {
            return site_kind;
        }
    }
    "paragraph"
}

fn push_event(
    events: &mut Vec<LogicalEvent>,
    headings: &[String],
    site_kind: &'static str,
    value: String,
) {
    let value = normalize_markdown_text(&value);
    if !value.is_empty() {
        events.push(LogicalEvent {
            heading: if headings.is_empty() {
                "<document>".to_owned()
            } else {
                headings.join(" / ")
            },
            site_kind,
            value,
        });
    }
}

fn observations(source_path: &str, events: Vec<LogicalEvent>) -> Vec<InventoryRow> {
    let mut duplicate_events: BTreeMap<(String, &'static str, String), usize> = BTreeMap::new();
    let mut rows = Vec::new();
    for event in events {
        let fingerprint = containing_value_fingerprint(&event.value);
        let duplicate_key = (event.heading.clone(), event.site_kind, fingerprint.clone());
        let event_ordinal = duplicate_events.entry(duplicate_key).or_default();
        *event_ordinal += 1;

        let mut term_ordinals: BTreeMap<String, usize> = BTreeMap::new();
        for found in vocabulary_matches(&event.value) {
            let term_ordinal = term_ordinals.entry(found.term.clone()).or_default();
            *term_ordinal += 1;
            let boundary = format!(
                "{} :: {} :: {} #{}",
                event.heading, event.site_kind, fingerprint, event_ordinal
            );
            rows.push(InventoryRow::observation(
                format!(
                    "{SURFACE_USER_DOCS}:{}:{}:{}:{fingerprint}:{}:{}:{}",
                    percent_encode(source_path),
                    percent_encode(&event.heading),
                    event.site_kind,
                    percent_encode(&found.term),
                    event_ordinal,
                    term_ordinal
                ),
                SURFACE_USER_DOCS,
                source_path,
                &boundary,
                event.site_kind,
                found.term,
            ));
        }
    }
    rows.sort_by(|left, right| left.occurrence_id.cmp(&right.occurrence_id));
    rows
}

fn pop_container(containers: &mut Vec<Container>, expected: Container) {
    if let Some(index) = containers.iter().rposition(|value| *value == expected) {
        containers.remove(index);
    }
}

fn heading_level(level: HeadingLevel) -> usize {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

fn normalize_markdown_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn markdown_heading_slug(value: &str) -> String {
    let mut slug = String::new();
    let mut separator = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            if separator && !slug.is_empty() {
                slug.push('-');
            }
            slug.push(character.to_ascii_lowercase());
            separator = false;
        } else if character.is_ascii_whitespace() || character == '-' {
            separator = true;
        }
    }
    slug
}

fn contains_table_layout_anchor(html: &str) -> bool {
    let mut remaining = html;
    while let Some(start) = remaining.find('<') {
        remaining = &remaining[start + 1..];
        if remaining.starts_with("!--") {
            let Some(end) = remaining.find("-->") else {
                return false;
            };
            remaining = &remaining[end + 3..];
            continue;
        }
        if remaining.starts_with(['!', '?', '/']) {
            continue;
        }

        let Some(end) = html_tag_end(remaining) else {
            return false;
        };
        if html_start_tag_has_exact_id(&remaining[..end], "table-layout") {
            return true;
        }
        remaining = &remaining[end + 1..];
    }
    false
}

fn visible_html_text(html: &str) -> String {
    let mut output = String::new();
    let mut remaining = html;
    let mut hidden_elements: Vec<String> = Vec::new();
    while !remaining.is_empty() {
        let Some(start) = remaining.find('<') else {
            if hidden_elements.is_empty() {
                append_html_text(&mut output, remaining);
            }
            break;
        };
        if hidden_elements.is_empty() {
            append_html_text(&mut output, &remaining[..start]);
        }
        remaining = &remaining[start + 1..];
        if remaining.starts_with("!--") {
            let Some(end) = remaining.find("-->") else {
                break;
            };
            remaining = &remaining[end + 3..];
            continue;
        }
        let Some(end) = html_tag_end(remaining) else {
            break;
        };
        let tag = remaining[..end].trim();
        let closing = tag.starts_with('/');
        let name = html_tag_name(tag.trim_start_matches('/')).to_ascii_lowercase();
        if closing {
            if hidden_elements.last() == Some(&name) {
                hidden_elements.pop();
            }
        } else if matches!(name.as_str(), "script" | "style" | "template") && !tag.ends_with('/') {
            hidden_elements.push(name);
        }
        remaining = &remaining[end + 1..];
    }
    normalize_markdown_text(&output)
}

fn append_html_text(output: &mut String, text: &str) {
    if text.trim().is_empty() {
        return;
    }
    if !output.is_empty() {
        output.push(' ');
    }
    output.push_str(&decode_numeric_html_references(text));
}

fn decode_numeric_html_references(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut remaining = text;
    while let Some(start) = remaining.find("&#") {
        output.push_str(&remaining[..start]);
        let reference = &remaining[start..];
        let Some(end) = reference.find(';') else {
            output.push_str(reference);
            return output;
        };
        let digits = &reference[2..end];
        let parsed = digits
            .strip_prefix(['x', 'X'])
            .map_or_else(|| digits.parse::<u32>(), |hex| u32::from_str_radix(hex, 16))
            .ok()
            .and_then(char::from_u32);
        if let Some(character) = parsed {
            output.push(character);
        } else {
            output.push_str(&reference[..=end]);
        }
        remaining = &reference[end + 1..];
    }
    output.push_str(remaining);
    output
}

fn html_tag_name(tag: &str) -> &str {
    let end = tag
        .bytes()
        .position(|byte| !is_html_name_byte(byte))
        .unwrap_or(tag.len());
    &tag[..end]
}

fn html_tag_end(tag: &str) -> Option<usize> {
    let mut quote = None;
    for (index, character) in tag.char_indices() {
        match (quote, character) {
            (Some(expected), actual) if expected == actual => quote = None,
            (None, '\'' | '"') => quote = Some(character),
            (None, '>') => return Some(index),
            _ => {}
        }
    }
    None
}

fn html_start_tag_has_exact_id(tag: &str, expected: &str) -> bool {
    let bytes = tag.as_bytes();
    let mut cursor = 0;
    skip_html_space(bytes, &mut cursor);
    while cursor < bytes.len() && is_html_name_byte(bytes[cursor]) {
        cursor += 1;
    }
    if cursor == 0 {
        return false;
    }

    while cursor < bytes.len() {
        skip_html_space(bytes, &mut cursor);
        if cursor >= bytes.len() || bytes[cursor] == b'/' {
            break;
        }
        let name_start = cursor;
        while cursor < bytes.len() && is_html_name_byte(bytes[cursor]) {
            cursor += 1;
        }
        if name_start == cursor {
            cursor += 1;
            continue;
        }
        let name = &tag[name_start..cursor];
        skip_html_space(bytes, &mut cursor);
        if cursor >= bytes.len() || bytes[cursor] != b'=' {
            continue;
        }
        cursor += 1;
        skip_html_space(bytes, &mut cursor);
        if cursor >= bytes.len() {
            return false;
        }
        let (value_start, value_end) = if matches!(bytes[cursor], b'\'' | b'"') {
            let quote = bytes[cursor];
            cursor += 1;
            let start = cursor;
            while cursor < bytes.len() && bytes[cursor] != quote {
                cursor += 1;
            }
            if cursor >= bytes.len() {
                return false;
            }
            let end = cursor;
            cursor += 1;
            (start, end)
        } else {
            let start = cursor;
            while cursor < bytes.len()
                && !bytes[cursor].is_ascii_whitespace()
                && bytes[cursor] != b'/'
            {
                cursor += 1;
            }
            (start, cursor)
        };
        if name.eq_ignore_ascii_case("id") && &tag[value_start..value_end] == expected {
            return true;
        }
    }
    false
}

fn skip_html_space(bytes: &[u8], cursor: &mut usize) {
    while *cursor < bytes.len() && bytes[*cursor].is_ascii_whitespace() {
        *cursor += 1;
    }
}

fn is_html_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_structural_text_and_code_but_not_urls_html_or_delimiters() {
        let fixture = r#"
# Table guide

Paragraph rows and [column label](https://example.test/table-row).

- list table

> quote row

| Column | value |
| --- | --- |
| row | `table_key` |

```json
{"sidecar_rows": []}
```

<span data-table="row">ignored column</span>
"#;
        let rows = scan(fixture, "docs/user/fixture.md");
        let kinds: std::collections::BTreeSet<_> =
            rows.iter().map(|row| row.site_kind.as_str()).collect();
        for expected in [
            "heading",
            "paragraph",
            "link_label",
            "list_item",
            "block_quote",
            "table_cell",
            "inline_code",
            "fenced_code",
        ] {
            assert!(kinds.contains(expected), "missing {expected}: {rows:#?}");
        }
        assert!(
            rows.iter()
                .all(|row| !row.boundary.contains("example.test"))
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row.current_term.eq_ignore_ascii_case("table"))
                .count(),
            3
        );
    }

    #[test]
    fn whitespace_is_identity_stable_and_duplicate_blocks_have_ordinals() {
        let compact = scan("# A\n\ntable rows\n\ntable rows\n", "docs/user/a.md");
        let spaced = scan("# A\n\n  table   rows\n\n table\n rows\n", "docs/user/a.md");
        let compact_ids: Vec<_> = compact
            .iter()
            .map(|row| row.occurrence_id.as_str())
            .collect();
        let spaced_ids: Vec<_> = spaced
            .iter()
            .map(|row| row.occurrence_id.as_str())
            .collect();
        assert_eq!(compact_ids, spaced_ids);
        assert!(compact.iter().any(|row| row.boundary.ends_with("#1")));
        assert!(compact.iter().any(|row| row.boundary.ends_with("#2")));
    }

    #[test]
    fn schema_version_is_shared_with_the_inventory() {
        let rows = scan("table", "docs/user/a.md");
        assert!(
            rows.iter()
                .all(|row| row.schema_version == crate::SCHEMA_VERSION)
        );
    }

    #[test]
    fn table_layout_anchor_identity_survives_heading_rename() {
        let implicit = scan("## Table layout\n", "docs/user/schema/index.md");
        let explicit = scan(
            "<a id=\"table-layout\"></a>\n\n## Type and dataset layout\n",
            "docs/user/schema/index.md",
        );
        let anchor = |rows: Vec<InventoryRow>| {
            rows.into_iter()
                .find(|row| row.site_kind == "compatibility_anchor")
                .expect("compatibility anchor")
        };
        assert_eq!(anchor(implicit), anchor(explicit));
    }

    #[test]
    fn table_layout_anchor_requires_an_actual_exact_id_attribute() {
        for fake in [
            "<!-- <a id=\"table-layout\"></a> -->",
            "<span data-example='id=\"table-layout\"'></span>",
            "<a id=\"table-layout-old\"></a>",
        ] {
            assert!(
                scan(fake, "docs/user/schema/index.md")
                    .iter()
                    .all(|row| row.site_kind != "compatibility_anchor"),
                "fake anchor was accepted: {fake}"
            );
        }
        assert!(
            scan(
                "<a class='legacy' id = \"table-layout\"></a>",
                "docs/user/schema/index.md"
            )
            .iter()
            .any(|row| row.site_kind == "compatibility_anchor")
        );
    }

    #[test]
    fn raw_html_scans_visible_text_but_not_comments_attributes_or_hidden_elements() {
        let rows = scan(
            r#"
<p class="table rows">&#116;able &#x72;ows</p>
<!-- hidden table rows -->
<SCRIPT>table rows</SCRIPT>
"#,
            "docs/user/a.md",
        );
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.site_kind == "paragraph"));
        assert_eq!(
            rows.iter()
                .map(|row| row.current_term.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            ["rows", "table"].into_iter().collect()
        );
    }
}
