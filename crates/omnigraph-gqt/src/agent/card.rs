//! The reference cards an agent reads, one per edition, and their pins.
//!
//! The edition-2 card is generated from the bundled agent skill
//! (`skills/omnigraph`), whole sections by heading, so it cannot drift from
//! what agents are given today without the instrument's test noticing. The
//! edition-3 card is written by hand from the GQ 3 RFC. Both are pinned by
//! SHA-256 in `manifest.json`, which the runner checks before every run.

use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// The skill sections the edition-2 card holds, in order: the file under
/// `skills/omnigraph` and the exact heading line. A missing heading fails the
/// generation instead of shrinking the card.
const EDITION2_SECTIONS: &[(&str, &str)] = &[
    ("SKILL.md", "### `.gq` grammar"),
    ("references/queries.md", "## Query Structure"),
    (
        "references/queries.md",
        "## System Fields and Result Values",
    ),
    ("references/queries.md", "## Search Functions"),
    ("references/queries.md", "## Aggregations"),
    ("references/queries.md", "## Filter Operators"),
    ("references/queries.md", "## Mutations"),
];

const EDITION2_PREFACE: &str = "# GQ reference card (edition 2)\n\n\
The query language as it ships today: the grammar notes of the Omnigraph agent skill and its \
query reference, section by section.\n";

pub const MANIFEST: &str = "manifest.json";

pub fn card_path(root: &Path, edition: u8) -> PathBuf {
    root.join("cards").join(format!("edition{edition}.md"))
}

fn heading_level(line: &str) -> Option<usize> {
    let level = line.bytes().take_while(|b| *b == b'#').count();
    (level > 0 && line.as_bytes().get(level) == Some(&b' ')).then_some(level)
}

/// The section of `text` that starts at the line `heading` and runs to the
/// next heading of the same or a higher level outside a code fence.
fn section<'a>(text: &'a str, heading: &str) -> Option<&'a str> {
    let level = heading_level(heading)?;
    let mut start = None;
    let mut offset = 0;
    let mut fenced = false;
    for line in text.split_inclusive('\n') {
        let bare = line.trim_end_matches(['\n', '\r']);
        if bare.trim_start().starts_with("```") {
            fenced = !fenced;
        } else if !fenced {
            match (start, heading_level(bare)) {
                (None, _) if bare == heading => start = Some(offset),
                (Some(begin), Some(found)) if found <= level => {
                    return Some(&text[begin..offset]);
                }
                _ => {}
            }
        }
        offset += line.len();
    }
    start.map(|begin| &text[begin..])
}

/// The edition-2 card, generated from the skill under `skill_root`.
pub fn edition2(skill_root: &Path) -> Result<String, String> {
    let mut out = String::from(EDITION2_PREFACE);
    for (file, heading) in EDITION2_SECTIONS {
        let path = skill_root.join(file);
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let body = section(&text, heading)
            .ok_or_else(|| format!("{}: no heading `{heading}`", path.display()))?;
        out.push('\n');
        out.push_str(body.trim_end());
        out.push('\n');
    }
    Ok(out)
}

pub fn skill_root(repo_root: &Path) -> PathBuf {
    repo_root.join("skills").join("omnigraph")
}

pub fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// `manifest.json`: the pinned agent model and each card's digest.
#[derive(Debug, Clone, PartialEq)]
pub struct Manifest {
    pub model: String,
    pub cards: Vec<(u8, String)>,
}

impl Manifest {
    pub fn read(root: &Path) -> Result<Self, String> {
        let path = root.join(MANIFEST);
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let value: Value =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        let model = value["model"]
            .as_str()
            .ok_or("manifest: `model` must be a string")?
            .to_string();
        let mut cards = Vec::new();
        for edition in [2u8, 3] {
            let digest = value["cards"][format!("edition{edition}.md")]
                .as_str()
                .ok_or_else(|| format!("manifest: no digest for edition{edition}.md"))?;
            cards.push((edition, digest.to_string()));
        }
        Ok(Manifest { model, cards })
    }

    pub fn render(&self) -> String {
        let cards: serde_json::Map<String, Value> = self
            .cards
            .iter()
            .map(|(edition, digest)| (format!("edition{edition}.md"), json!(digest)))
            .collect();
        let mut out = serde_json::to_string_pretty(&json!({
            "model": self.model,
            "cards": cards,
        }))
        .expect("manifest serializes");
        out.push('\n');
        out
    }

    /// Every card on disk against its pinned digest; the mismatches.
    pub fn verify(&self, root: &Path) -> Vec<String> {
        let mut out = Vec::new();
        for (edition, pinned) in &self.cards {
            let path = card_path(root, *edition);
            match std::fs::read(&path) {
                Ok(bytes) if sha256(&bytes) == *pinned => {}
                Ok(bytes) => out.push(format!(
                    "{} has digest {} but manifest.json pins {pinned}",
                    path.display(),
                    sha256(&bytes)
                )),
                Err(e) => out.push(format!("{}: {e}", path.display())),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_section_ends_at_the_next_heading_of_its_level_outside_fences() {
        let text = "# A\n## B\nb\n```\n## not a heading\n```\n### C\nc\n## D\nd\n";
        assert_eq!(
            section(text, "## B"),
            Some("## B\nb\n```\n## not a heading\n```\n### C\nc\n")
        );
        assert_eq!(section(text, "## D"), Some("## D\nd\n"));
        assert_eq!(section(text, "## E"), None);
    }
}
