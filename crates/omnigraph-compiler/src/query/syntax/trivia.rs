//! The comments of a source, as the grammar skips them (`COMMENT` in
//! `grammar.pest`): a line comment `// …` up to the end of its line, and a
//! block comment `/* … */`, never inside a string literal (`"…"` or `'…'`,
//! a backslash escaping the next character).
//!
//! A comment is trivia: the parse tree holds none. So a fix that rebuilds
//! text from the tree, or copies a construct's text, carries the comments
//! of the text it replaces by these spans ([`comments_in`]) and writes
//! each where the construct it preceded starts ([`carried`]), a line comment
//! ending its line: a fix never drops a comment, and never writes code after
//! a `//` on its line.

/// The byte range of every comment of `text`, in order. A line comment
/// ends before its newline; an unterminated block comment or string runs
/// to the end of the text.
pub fn comment_spans(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'"' | b'\'') => {
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                let end = text[i..].find('\n').map_or(text.len(), |n| i + n);
                out.push((i, end));
                i = end;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let end = text[i + 2..]
                    .find("*/")
                    .map_or(text.len(), |n| i + 2 + n + 2);
                out.push((i, end));
                i = end;
            }
            _ => i += 1,
        }
    }
    out.into_iter()
        .map(|(s, e)| (s, e.min(text.len())))
        .collect()
}

/// The text of every comment of `text` in `start..end` that lies wholly
/// inside it, in order.
pub fn comments_in(text: &str, start: usize, end: usize) -> Vec<String> {
    comment_spans(text)
        .into_iter()
        .filter(|(s, e)| *s >= start && *e <= end)
        .map(|(s, e)| text[s..e].to_string())
        .collect()
}

/// The comments that follow `at` in `text` with only whitespace and
/// commas between them: those written after a construct, before the next.
pub fn comments_after(text: &str, at: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = at;
    let spans = comment_spans(text);
    loop {
        let rest = &text[i..];
        let skip = rest.len()
            - rest
                .trim_start_matches(|c: char| c.is_whitespace() || c == ',')
                .len();
        i += skip;
        match spans.iter().find(|(s, _)| *s == i) {
            Some((s, e)) => {
                out.push(text[*s..*e].to_string());
                i = *e;
            }
            None => return out,
        }
    }
}

/// `text` with every comment replaced by one space (a line comment keeps
/// its newline, which follows the span).
pub fn without_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    for (s, e) in comment_spans(text) {
        out.push_str(&text[at..s]);
        out.push(' ');
        at = e;
    }
    out.push_str(&text[at..]);
    out
}

/// `comments`, each written where a construct of a rewrite starts: a line
/// comment followed by a line break and `indent`, so the code after it
/// starts the next line, and a block comment followed by a space.
pub fn carried(comments: &[String], indent: &str) -> String {
    comments
        .iter()
        .map(|c| {
            if c.starts_with("//") {
                format!("{c}\n{indent}")
            } else {
                format!("{c} ")
            }
        })
        .collect()
}

/// The indentation of the line `at` stands on in `text`.
pub fn indent_at(text: &str, at: usize) -> &str {
    let line = text[..at].rfind('\n').map_or(0, |n| n + 1);
    let rest = &text[line..];
    &rest[..rest.len() - rest.trim_start_matches([' ', '\t']).len()]
}

/// Whether the comment at `span` of `text` stands on a line of its own.
fn own_line(text: &str, span: (usize, usize)) -> bool {
    let line = text[..span.0].rfind('\n').map_or(0, |n| n + 1);
    text[line..span.0].trim().is_empty()
}

/// `edits` of `source`, one suggestion, rewritten so that applying them
/// keeps every comment of the text they replace exactly once, where it
/// was written as nearly as the rewrite allows, and never reads code as
/// comment. A fix copies text from the tree, whose spans can run on over
/// the trivia after a construct, and rebuilds text from the tree, which
/// holds no trivia: so a copied comment can be followed by code on its
/// line, commenting it out, and a comment between rebuilt pieces is lost.
///
/// The comments the edits replace are matched, by their text, to the
/// comments their replacements hold. A matched comment that runs on over
/// code ends its line there. A comment in a replacement that matches none
/// and copies one that still stands outside the edits is removed; one the
/// fix writes itself stays. A
/// replaced comment no replacement holds is written at the start of its
/// edit's replacement ([`carried`]), on a line of its own if it stood on
/// one. An edit that removes the line break before a comment standing on
/// its own line keeps one, and a replacement ending in a line comment,
/// before code on the same line, ends its line.
pub fn keep_comments(
    source: &str,
    edits: &[crate::query::diagnostic::TextEdit],
) -> Vec<crate::query::diagnostic::TextEdit> {
    use crate::query::diagnostic::TextEdit;
    let spans = comment_spans(source);
    // The comments each edit replaces, and whether each is matched yet.
    let mut removed: Vec<(usize, (usize, usize), bool)> = Vec::new();
    for (i, e) in edits.iter().enumerate() {
        for span in spans
            .iter()
            .filter(|(s, t)| *s >= e.start && *t <= e.end && e.end > e.start)
        {
            removed.push((i, *span, false));
        }
    }
    let mut out: Vec<TextEdit> = Vec::with_capacity(edits.len());
    for e in edits {
        let indent = indent_at(source, e.start).to_string();
        let r = &e.replacement;
        let mut text = String::with_capacity(r.len());
        let mut at = 0;
        for (cs, ce) in comment_spans(r) {
            text.push_str(&r[at..cs]);
            at = ce;
            let written = &r[cs..ce];
            let matched = removed
                .iter_mut()
                .find(|(_, (s, t), used)| !*used && written.starts_with(&source[*s..*t]));
            let Some((_, (s, t), used)) = matched else {
                // A copy of a comment that stands outside the edits is
                // removed; a comment the fix writes itself (a foreign
                // comment respelled `//`) stays.
                let copies_one_outside = spans.iter().any(|(cs, ce)| {
                    !edits
                        .iter()
                        .any(|x| *cs >= x.start && *ce <= x.end && x.end > x.start)
                        && written.starts_with(&source[*cs..*ce])
                });
                if !copies_one_outside {
                    text.push_str(written);
                }
                continue;
            };
            *used = true;
            let comment = &source[*s..*t];
            // A comment that stood on a line of its own keeps one.
            let line_so_far = match text.rfind('\n') {
                Some(n) => &text[n + 1..],
                None => &source[source[..e.start].rfind('\n').map_or(0, |n| n + 1)..e.start],
            };
            let before_on_line = match text.rfind('\n') {
                Some(_) => line_so_far.to_string(),
                None => format!("{line_so_far}{text}"),
            };
            if own_line(source, (*s, *t)) && !before_on_line.trim().is_empty() {
                let trimmed = text.trim_end_matches([' ', '\t']).len();
                text.truncate(trimmed);
                text.push('\n');
                text.push_str(&indent);
            }
            text.push_str(comment);
            let rest = written[comment.len()..].trim_start_matches([' ', '\t']);
            if !rest.is_empty() {
                text.push('\n');
                text.push_str(&indent);
                text.push_str(rest);
            }
        }
        text.push_str(&r[at..]);
        out.push(TextEdit {
            start: e.start,
            end: e.end,
            replacement: text,
        });
    }
    // The replaced comments no replacement holds, carried at the start of
    // their edit's replacement in the order they were written.
    let mut leads: Vec<String> = vec![String::new(); out.len()];
    for (i, span, used) in &removed {
        if *used {
            continue;
        }
        let start = out[*i].start;
        let indent = indent_at(source, start).to_string();
        let comment = source[span.0..span.1].to_string();
        let lead = &mut leads[*i];
        let before = &source[source[..start].rfind('\n').map_or(0, |n| n + 1)..start];
        if lead.is_empty() && own_line(source, *span) && !before.trim().is_empty() {
            lead.push('\n');
            lead.push_str(&indent);
        }
        lead.push_str(&carried(&[comment], &indent));
    }
    for (e, lead) in out.iter_mut().zip(leads) {
        e.replacement.insert_str(0, &lead);
    }
    // An edit that removes the line break before a comment standing on a
    // line of its own keeps one, so the comment does not join the line
    // before it.
    for e in &mut out {
        let after = &source[e.end..];
        let gap = after.len() - after.trim_start_matches([' ', '\t']).len();
        let Some(span) = spans.iter().find(|(s, _)| *s == e.end + gap) else {
            continue;
        };
        let removed_break = source[e.start..e.end].contains('\n');
        if own_line(source, *span)
            && removed_break
            && !e.replacement.trim_end_matches([' ', '\t']).ends_with('\n')
        {
            // The comment's own indentation stays after the edit.
            e.replacement = e.replacement.trim_end_matches([' ', '\t']).to_string();
            e.replacement.push('\n');
        }
    }
    for e in &mut out {
        let r = &e.replacement;
        // A copied comment, never one the fix opens to run on into the
        // source after it (a foreign comment respelled `//`).
        let ends_in_line_comment = comment_spans(r).last().is_some_and(|(s, t)| {
            *t == r.len()
                && r[*s..].starts_with("//")
                && spans.iter().any(|(cs, ce)| source[*cs..*ce] == r[*s..*t])
        });
        let code_follows = source[e.end..]
            .split('\n')
            .next()
            .is_some_and(|line| !line.trim().is_empty());
        if ends_in_line_comment && code_follows {
            let indent = indent_at(source, e.start).to_string();
            e.replacement.push('\n');
            e.replacement.push_str(&indent);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comments_are_found_outside_strings_only() {
        let text = "a // one\nb \"// no\" '/* no */' /* two */ c \"\\\"//\" //three";
        let found: Vec<&str> = comment_spans(text)
            .into_iter()
            .map(|(s, e)| &text[s..e])
            .collect();
        assert_eq!(found, ["// one", "/* two */", "//three"]);
        assert_eq!(without_comments("x // c\ny /* d */ z"), "x  \ny   z");
    }

    #[test]
    fn edits_keep_every_comment_they_replace_once() {
        use crate::query::diagnostic::TextEdit;
        let edit = |start, end, r: &str| TextEdit {
            start,
            end,
            replacement: r.to_string(),
        };
        // A copied comment running on over code ends its line.
        let src = "  a // c\n  b";
        let got = keep_comments(src, &[edit(2, src.len(), "x // c  y")]);
        assert_eq!(got[0].replacement, "x // c\n  y");
        // A dropped comment is carried, on a line of its own.
        let src = "f(\n  // c\n  a)";
        let got = keep_comments(src, &[edit(0, src.len(), "g(a)")]);
        assert_eq!(got[0].replacement, "// c\ng(a)");
        // A copy of a comment outside the edits is removed.
        let src = "a /* k */ b";
        let got = keep_comments(src, &[edit(0, 1, "/* k */ z")]);
        assert_eq!(got[0].replacement, " z");
    }

    #[test]
    fn a_carried_line_comment_ends_its_line() {
        let c = ["// a".to_string(), "/* b */".to_string()];
        assert_eq!(carried(&c, "  "), "// a\n  /* b */ ");
    }
}
