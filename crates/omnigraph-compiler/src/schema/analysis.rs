//! Full-text analysis declared on a String property:
//! `@index(fulltext, analyzer = "<profile>", scorer = "<scorer>")`
//! (docs/rfcs/2026-11-07-gq-3.md, section 4, Schema: Text).
//!
//! A profile names one immutable pipeline. Every setting of it is spelled
//! here, so no unspecified substrate default is part of the contract; a
//! different pipeline is a new profile, never an edit of an accepted one.
//! A property whose full-text index declares no analyzer keeps edition 2's
//! index analysis (the substrate's default) and carries no [`FullTextAnalysis`].

use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::ast::Annotation;

/// The index kind `@index(fulltext, ...)` names on a property.
pub const FULL_TEXT_INDEX_KIND: &str = "fulltext";

/// Whether `annotation` declares the property's index: `@index` or
/// `@index(fulltext, ...)`.
pub fn is_property_index_annotation(annotation: &Annotation) -> bool {
    annotation.name == "index"
        && annotation
            .value
            .as_deref()
            .is_none_or(|kind| kind == FULL_TEXT_INDEX_KIND)
}

/// The analysis a property's annotations declare: the options of its
/// `@index(fulltext, ...)`, `None` when they are edition 2's. Names the parser
/// has validated; an unknown one reads as absent.
pub fn declared_analysis(annotations: &[Annotation]) -> Option<FullTextAnalysis> {
    let index = annotations.iter().find(|annotation| {
        annotation.name == "index" && annotation.value.as_deref() == Some(FULL_TEXT_INDEX_KIND)
    })?;
    let analyzer = index
        .kwargs
        .get("analyzer")
        .and_then(|name| AnalyzerProfile::parse(name));
    let scorer = index
        .kwargs
        .get("scorer")
        .and_then(|name| FullTextScorer::parse(name))
        .unwrap_or_default();
    FullTextAnalysis::declared(analyzer, scorer)
}

/// The analyzer profiles a schema can declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AnalyzerProfile {
    #[serde(rename = "standard_v1")]
    StandardV1,
    #[serde(rename = "standard_folded_v1")]
    StandardFoldedV1,
    #[serde(rename = "english_v1")]
    EnglishV1,
}

/// The tokenizer a profile splits text with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzerTokenizer {
    /// Splits on every character that is not alphanumeric.
    Simple,
}

/// The language a stemmer or a stop-word list belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzerLanguage {
    English,
}

/// One token filter, applied in the profile's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzerFilter {
    Lowercase,
    Stem(AnalyzerLanguage),
    StopWords(AnalyzerLanguage),
    AsciiFolding,
}

/// The Unicode normalization a profile applies before tokenization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnicodeNormalization {
    Nfc,
}

/// Every setting of one profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalyzerPipeline {
    pub normalization: UnicodeNormalization,
    pub tokenizer: AnalyzerTokenizer,
    /// Applied in this order, after tokenization.
    pub filters: &'static [AnalyzerFilter],
    /// The longest token kept, `None` when no token-length filter applies.
    pub max_token_length: Option<usize>,
}

impl AnalyzerProfile {
    pub const ALL: [Self; 3] = [Self::StandardV1, Self::StandardFoldedV1, Self::EnglishV1];

    /// The profile the edition 2 index analysis is closest to: it differs from
    /// the substrate's default by the 40-byte token filter and NFC.
    pub const CLOSEST_TO_EDITION_2: Self = Self::EnglishV1;

    pub fn name(self) -> &'static str {
        match self {
            Self::StandardV1 => "standard_v1",
            Self::StandardFoldedV1 => "standard_folded_v1",
            Self::EnglishV1 => "english_v1",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|profile| profile.name() == name)
    }

    /// The profile table of the RFC. A row never changes once released.
    pub fn pipeline(self) -> AnalyzerPipeline {
        use AnalyzerFilter::*;
        use AnalyzerLanguage::English;
        let filters: &'static [AnalyzerFilter] = match self {
            Self::StandardV1 => &[Lowercase],
            Self::StandardFoldedV1 => &[Lowercase, AsciiFolding],
            Self::EnglishV1 => &[Lowercase, Stem(English), StopWords(English), AsciiFolding],
        };
        AnalyzerPipeline {
            normalization: UnicodeNormalization::Nfc,
            tokenizer: AnalyzerTokenizer::Simple,
            filters,
            max_token_length: None,
        }
    }
}

impl fmt::Display for AnalyzerProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// How a full-text field ranks.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub enum FullTextScorer {
    /// The versioned lexical scorer `bm25` reads.
    #[default]
    #[serde(rename = "bm25_v1")]
    Bm25V1,
    /// Matching only: a ranking call on the field is a type error.
    #[serde(rename = "none")]
    None,
}

impl FullTextScorer {
    pub const ALL: [Self; 2] = [Self::Bm25V1, Self::None];

    pub fn name(self) -> &'static str {
        match self {
            Self::Bm25V1 => "bm25_v1",
            Self::None => "none",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|scorer| scorer.name() == name)
    }

    pub fn ranks(self) -> bool {
        matches!(self, Self::Bm25V1)
    }
}

impl fmt::Display for FullTextScorer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// The full-text analysis a property's index declares beyond edition 2's: a
/// profile, a scorer other than the default, or both. Absent (`None` on the
/// property) when the index declares neither, so an edition 2 schema keeps
/// its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FullTextAnalysis {
    /// `None` keeps edition 2's index analysis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<AnalyzerProfile>,
    #[serde(default)]
    pub scorer: FullTextScorer,
}

impl FullTextAnalysis {
    /// The declaration as written, `None` when it is edition 2's: no profile
    /// and the default scorer.
    pub fn declared(analyzer: Option<AnalyzerProfile>, scorer: FullTextScorer) -> Option<Self> {
        (analyzer.is_some() || scorer != FullTextScorer::default())
            .then_some(Self { analyzer, scorer })
    }

    /// The analyzer fingerprint of this declaration on the property
    /// `property_id`: `None` when no profile is declared, since edition 2's
    /// analysis is the substrate's and its certificate names its generation
    /// alone (RFC 0043).
    ///
    /// The fingerprint is a derived view (a digest) of a typed, versioned
    /// descriptor: the property identity, the profile, its normalization form,
    /// its tokenizer, its filters in order, its token-length filter and the
    /// scorer version. A rename keeps the property identity, so it keeps the
    /// fingerprint; drop and re-add mints a new identity and a new fingerprint
    /// (RFC 0040, invariant 6).
    ///
    /// The descriptor records the normalization form the profile fixes, not
    /// the identity of a normalizer implementation or of its Unicode data:
    /// nothing in the engine normalizes yet, and how that identity enters the
    /// fingerprint is open in the RFC (Unresolved question 9).
    pub fn fingerprint(&self, property_id: u64) -> Option<String> {
        let profile = self.analyzer?;
        Some(fingerprint_of(&descriptor(
            property_id,
            profile,
            self.scorer,
        )))
    }
}

/// Whether a change of a property's declaration from `from` to `to` changes
/// what its full-text index is built with or certified for: its profile, or,
/// under a profile, the scorer its fingerprint names. A scorer change on a
/// field without a profile changes neither.
pub fn changes_index_analysis(
    from: Option<&FullTextAnalysis>,
    to: Option<&FullTextAnalysis>,
) -> bool {
    let profile = |analysis: Option<&FullTextAnalysis>| analysis.and_then(|a| a.analyzer);
    let scorer =
        |analysis: Option<&FullTextAnalysis>| analysis.map(|a| a.scorer).unwrap_or_default();
    profile(from) != profile(to) || (profile(to).is_some() && scorer(from) != scorer(to))
}

/// The canonical descriptor bytes the fingerprint digests, one field per
/// line, in a fixed order.
fn descriptor(property_id: u64, profile: AnalyzerProfile, scorer: FullTextScorer) -> String {
    let pipeline = profile.pipeline();
    let normalization = match pipeline.normalization {
        UnicodeNormalization::Nfc => "nfc",
    };
    let tokenizer = match pipeline.tokenizer {
        AnalyzerTokenizer::Simple => "simple",
    };
    let filters = pipeline
        .filters
        .iter()
        .map(|filter| match filter {
            AnalyzerFilter::Lowercase => "lowercase",
            AnalyzerFilter::Stem(AnalyzerLanguage::English) => "stem:english",
            AnalyzerFilter::StopWords(AnalyzerLanguage::English) => "stop_words:english",
            AnalyzerFilter::AsciiFolding => "ascii_folding",
        })
        .collect::<Vec<_>>()
        .join(",");
    let max_token_length = pipeline
        .max_token_length
        .map_or_else(|| "disabled".to_string(), |length| length.to_string());
    format!(
        "omnigraph.full-text-analysis.v1\n\
         property_id={property_id}\n\
         profile={profile}\n\
         normalization={normalization}\n\
         tokenizer={tokenizer}\n\
         filters={filters}\n\
         max_token_length={max_token_length}\n\
         scorer={scorer}\n"
    )
}

fn fingerprint_of(descriptor: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(descriptor.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

/// The names `analyzer =` admits, for diagnostics.
pub fn admitted_profiles() -> String {
    AnalyzerProfile::ALL
        .iter()
        .map(|profile| format!("\"{profile}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The names `scorer =` admits, for diagnostics.
pub fn admitted_scorers() -> String {
    FullTextScorer::ALL
        .iter()
        .map(|scorer| format!("\"{scorer}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_profile_table_is_the_rfcs() {
        use AnalyzerFilter::*;
        use AnalyzerLanguage::English;
        for profile in AnalyzerProfile::ALL {
            let pipeline = profile.pipeline();
            assert_eq!(pipeline.normalization, UnicodeNormalization::Nfc);
            assert_eq!(pipeline.tokenizer, AnalyzerTokenizer::Simple);
            assert_eq!(pipeline.max_token_length, None, "{profile}");
            assert_eq!(AnalyzerProfile::parse(profile.name()), Some(profile));
        }
        assert_eq!(AnalyzerProfile::StandardV1.pipeline().filters, &[Lowercase]);
        assert_eq!(
            AnalyzerProfile::StandardFoldedV1.pipeline().filters,
            &[Lowercase, AsciiFolding]
        );
        assert_eq!(
            AnalyzerProfile::EnglishV1.pipeline().filters,
            &[Lowercase, Stem(English), StopWords(English), AsciiFolding]
        );
        assert_eq!(AnalyzerProfile::parse("english"), None);
        assert_eq!(FullTextScorer::parse("bm25"), None);
    }

    /// A released profile never changes: these digests pin every setting of
    /// every profile with both scorers. A new pipeline is a new profile.
    #[test]
    fn released_fingerprints_never_change() {
        let pinned = [
            (AnalyzerProfile::StandardV1, FullTextScorer::Bm25V1),
            (AnalyzerProfile::StandardV1, FullTextScorer::None),
            (AnalyzerProfile::StandardFoldedV1, FullTextScorer::Bm25V1),
            (AnalyzerProfile::StandardFoldedV1, FullTextScorer::None),
            (AnalyzerProfile::EnglishV1, FullTextScorer::Bm25V1),
            (AnalyzerProfile::EnglishV1, FullTextScorer::None),
        ];
        let rendered = pinned
            .iter()
            .map(|(profile, scorer)| descriptor(7, *profile, *scorer))
            .collect::<String>();
        assert_eq!(
            rendered,
            "omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=standard_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase\nmax_token_length=disabled\nscorer=bm25_v1\n\
             omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=standard_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase\nmax_token_length=disabled\nscorer=none\n\
             omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=standard_folded_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase,ascii_folding\nmax_token_length=disabled\nscorer=bm25_v1\n\
             omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=standard_folded_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase,ascii_folding\nmax_token_length=disabled\nscorer=none\n\
             omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=english_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase,stem:english,stop_words:english,ascii_folding\nmax_token_length=disabled\nscorer=bm25_v1\n\
             omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=english_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase,stem:english,stop_words:english,ascii_folding\nmax_token_length=disabled\nscorer=none\n"
        );
    }

    #[test]
    fn the_fingerprint_binds_property_identity_profile_and_scorer() {
        let english = FullTextAnalysis {
            analyzer: Some(AnalyzerProfile::EnglishV1),
            scorer: FullTextScorer::Bm25V1,
        };
        let base = english.fingerprint(7).unwrap();
        assert!(base.starts_with("sha256:"));
        assert_eq!(english.fingerprint(7).unwrap(), base);
        assert_ne!(english.fingerprint(8).unwrap(), base);
        let standard = FullTextAnalysis {
            analyzer: Some(AnalyzerProfile::StandardV1),
            ..english
        };
        assert_ne!(standard.fingerprint(7).unwrap(), base);
        let unranked = FullTextAnalysis {
            scorer: FullTextScorer::None,
            ..english
        };
        assert_ne!(unranked.fingerprint(7).unwrap(), base);
        let edition_2 = FullTextAnalysis {
            analyzer: None,
            scorer: FullTextScorer::None,
        };
        assert_eq!(edition_2.fingerprint(7), None);
    }

    #[test]
    fn only_a_profile_or_its_scorer_changes_the_index() {
        let english = FullTextAnalysis {
            analyzer: Some(AnalyzerProfile::EnglishV1),
            scorer: FullTextScorer::Bm25V1,
        };
        let unranked = FullTextAnalysis {
            analyzer: None,
            scorer: FullTextScorer::None,
        };
        assert!(changes_index_analysis(None, Some(&english)));
        assert!(changes_index_analysis(Some(&english), None));
        assert!(changes_index_analysis(
            Some(&english),
            Some(&FullTextAnalysis {
                scorer: FullTextScorer::None,
                ..english
            })
        ));
        assert!(!changes_index_analysis(None, Some(&unranked)));
        assert!(!changes_index_analysis(Some(&unranked), None));
        assert!(!changes_index_analysis(Some(&english), Some(&english)));
    }

    #[test]
    fn an_edition_2_declaration_is_absent() {
        assert_eq!(
            FullTextAnalysis::declared(None, FullTextScorer::Bm25V1),
            None
        );
        assert!(FullTextAnalysis::declared(None, FullTextScorer::None).is_some());
        assert!(
            FullTextAnalysis::declared(Some(AnalyzerProfile::StandardV1), FullTextScorer::Bm25V1)
                .is_some()
        );
    }
}
