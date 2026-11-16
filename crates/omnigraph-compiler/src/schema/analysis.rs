//! Full-text analysis declared on a String property:
//! `@index(fulltext, analyzer = "<profile>", scorer = "<scorer>")`
//! (docs/rfcs/2026-11-07-gq-3.md, section 4, Schema: Text).
//!
//! A profile names one immutable pipeline. Every setting of it is spelled
//! here, so no unspecified substrate default is part of the contract; a
//! different pipeline is a new profile, never an edit of an accepted one.
//! A property whose full-text index declares no analyzer takes
//! [`AnalyzerProfile::DEFAULT`], `english_v1`, as an omitted distance takes
//! `l2`, so every full-text field has exactly one profile. The declaration
//! as written is kept: an omitted analyzer carries no [`FullTextAnalysis`].

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
/// `@index(fulltext, ...)`, `None` when they are the defaults (no analyzer,
/// the default scorer). Names the parser has validated; an unknown one reads
/// as absent.
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

impl AnalyzerPipeline {
    /// The stop list the pipeline's stop-word filter removes, `None` when it
    /// has none.
    pub fn stop_words(&self) -> Option<&'static [&'static str]> {
        self.filters
            .iter()
            .any(|filter| matches!(filter, AnalyzerFilter::StopWords(AnalyzerLanguage::English)))
            .then_some(ENGLISH_V1_STOP_WORDS)
    }
}

/// `english_v1`'s stop list: each of NLTK's 198 English stop words and its
/// Snowball English stem, sorted. Stemming runs before stop-word removal
/// (the only order Lance's index builder produces), so a list of plain words
/// would miss the stop words the stemmer changes (`onli`, `veri`, `becaus`,
/// …); the stems are listed too. The list is passed to Lance explicitly,
/// never taken from its default, and the profile's descriptor records its
/// digest (docs/rfcs/2026-11-07-gq-3.md, section 4, Schema: Text).
pub const ENGLISH_V1_STOP_WORDS: &[&str] = &[
    "a", "about", "abov", "above", "after", "again", "against", "ain", "all", "am", "an", "and",
    "ani", "any", "are", "aren", "aren't", "as", "at", "be", "becaus", "because", "been", "befor",
    "before", "being", "below", "between", "both", "but", "by", "can", "couldn", "couldn't", "d",
    "did", "didn", "didn't", "do", "doe", "does", "doesn", "doesn't", "doing", "don", "don't",
    "down", "dure", "during", "each", "few", "for", "from", "further", "had", "hadn", "hadn't",
    "has", "hasn", "hasn't", "have", "haven", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "hers", "herself", "him", "himself", "his", "how", "i", "i'd", "i'll", "i'm",
    "i'v", "i've", "if", "in", "into", "is", "isn", "isn't", "it", "it'd", "it'll", "it's", "its",
    "itself", "just", "ll", "m", "ma", "me", "mightn", "mightn't", "more", "most", "mustn",
    "mustn't", "my", "myself", "needn", "needn't", "no", "nor", "not", "now", "o", "of", "off",
    "on", "onc", "once", "onli", "only", "or", "other", "our", "ours", "ourselv", "ourselves",
    "out", "over", "own", "re", "s", "same", "shan", "shan't", "she", "she'd", "she'll", "she's",
    "should", "should'v", "should've", "shouldn", "shouldn't", "so", "some", "such", "t", "than",
    "that", "that'll", "the", "their", "theirs", "them", "themselv", "themselves", "then", "there",
    "these", "they", "they'd", "they'll", "they'r", "they're", "they'v", "they've", "this",
    "those", "through", "to", "too", "under", "until", "up", "ve", "veri", "very", "was", "wasn",
    "wasn't", "we", "we'd", "we'll", "we'r", "we're", "we'v", "we've", "were", "weren", "weren't",
    "what", "when", "where", "whi", "which", "while", "who", "whom", "why", "will", "with", "won",
    "won't", "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you'r", "you're", "you'v",
    "you've", "your", "yours", "yourself", "yourselv", "yourselves",
];

/// The stemmer `english_v1` applies: Snowball English as the stemmer crate
/// Lance 13 uses implements it, at the version the lockfile resolves. A
/// stemmer change is a new profile, so this identity is part of the
/// profile's descriptor, and a test holds it to the resolved version.
pub const ENGLISH_STEMMER: &str = "snowball-english@frostem-1.20260821.3";

/// The SHA-256 of a stop list: its words joined by newlines.
pub fn stop_list_digest(words: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(words.join("\n").as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

impl AnalyzerProfile {
    pub const ALL: [Self; 3] = [Self::StandardV1, Self::StandardFoldedV1, Self::EnglishV1];

    /// The profile a full-text index without `analyzer` takes.
    pub const DEFAULT: Self = Self::EnglishV1;

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

/// The full-text analysis a property's index declares beyond the defaults: a
/// profile, a scorer other than the default, or both. Absent (`None` on the
/// property) when the index declares neither; [`FullTextAnalysis::resolved`]
/// gives the analysis that holds either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FullTextAnalysis {
    /// `None` is [`AnalyzerProfile::DEFAULT`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<AnalyzerProfile>,
    #[serde(default)]
    pub scorer: FullTextScorer,
}

impl FullTextAnalysis {
    /// The declaration as written, `None` when it is the defaults: no
    /// profile and the default scorer.
    pub fn declared(analyzer: Option<AnalyzerProfile>, scorer: FullTextScorer) -> Option<Self> {
        (analyzer.is_some() || scorer != FullTextScorer::default())
            .then_some(Self { analyzer, scorer })
    }

    /// The analysis a full-text field declaring `declared` has: its profile,
    /// [`AnalyzerProfile::DEFAULT`] when it names none, and its scorer.
    pub fn resolved(declared: Option<&Self>) -> Self {
        let analyzer = declared.and_then(|a| a.analyzer).unwrap_or(AnalyzerProfile::DEFAULT);
        let scorer = declared.map(|a| a.scorer).unwrap_or_default();
        Self {
            analyzer: Some(analyzer),
            scorer,
        }
    }

    /// The profile this analysis applies.
    pub fn profile(&self) -> AnalyzerProfile {
        self.analyzer.unwrap_or(AnalyzerProfile::DEFAULT)
    }

    /// The analyzer fingerprint of this analysis on the property
    /// `property_id`.
    ///
    /// The fingerprint is a derived view (a digest) of a typed, versioned
    /// descriptor: the property identity, the profile, its normalization form,
    /// its tokenizer, its filters in order, its stop list's digest, its
    /// stemmer's identity, its token-length filter and the scorer version. A
    /// rename keeps the property identity, so it keeps the fingerprint; drop
    /// and re-add mints a new identity and a new fingerprint (RFC 0040,
    /// invariant 6). Whether an index stores term positions is a build option
    /// of the index, recorded in its RFC 0043 certificate, never here.
    ///
    /// The descriptor records the normalization form the profile fixes, not
    /// the identity of a normalizer implementation or of its Unicode data;
    /// how that identity enters the fingerprint is open in the RFC
    /// (Unresolved question 9).
    pub fn fingerprint(&self, property_id: u64) -> String {
        fingerprint_of(&descriptor(property_id, self.profile(), self.scorer))
    }
}

/// Whether a change of a property's declaration from `from` to `to` changes
/// what its full-text index is built with or certified for: its resolved
/// profile or the scorer its fingerprint names. Writing out the default
/// profile, or dropping it, changes neither.
pub fn changes_index_analysis(
    from: Option<&FullTextAnalysis>,
    to: Option<&FullTextAnalysis>,
) -> bool {
    FullTextAnalysis::resolved(from) != FullTextAnalysis::resolved(to)
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
    let stems = pipeline
        .filters
        .iter()
        .any(|filter| matches!(filter, AnalyzerFilter::Stem(_)));
    let stop_words = pipeline
        .stop_words()
        .map_or_else(|| "none".to_string(), stop_list_digest);
    let stemmer = if stems { ENGLISH_STEMMER } else { "none" };
    format!(
        "omnigraph.full-text-analysis.v1\n\
         property_id={property_id}\n\
         profile={profile}\n\
         normalization={normalization}\n\
         tokenizer={tokenizer}\n\
         filters={filters}\n\
         stop_words={stop_words}\n\
         stemmer={stemmer}\n\
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

    /// The rendered descriptor of each profile with each scorer.
    fn pinned_descriptors() -> String {
        [
            (AnalyzerProfile::StandardV1, FullTextScorer::Bm25V1),
            (AnalyzerProfile::StandardV1, FullTextScorer::None),
            (AnalyzerProfile::StandardFoldedV1, FullTextScorer::Bm25V1),
            (AnalyzerProfile::StandardFoldedV1, FullTextScorer::None),
            (AnalyzerProfile::EnglishV1, FullTextScorer::Bm25V1),
            (AnalyzerProfile::EnglishV1, FullTextScorer::None),
        ]
        .iter()
        .map(|(profile, scorer)| descriptor(7, *profile, *scorer))
        .collect()
    }

    /// A released profile never changes: the descriptor text pins every
    /// setting of every profile with both scorers, and the fingerprints pin
    /// the digest a certificate records. A new pipeline is a new profile.
    #[test]
    fn released_fingerprints_never_change() {
        let english_stop_words = stop_list_digest(ENGLISH_V1_STOP_WORDS);
        assert_eq!(
            english_stop_words,
            "sha256:5ad60369da134f034311b0466fbbafac8f0ef376674b76eec77f80f2a96fec8f"
        );
        assert_eq!(
            pinned_descriptors(),
            format!(
                "omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=standard_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase\nstop_words=none\nstemmer=none\nmax_token_length=disabled\nscorer=bm25_v1\n\
                 omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=standard_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase\nstop_words=none\nstemmer=none\nmax_token_length=disabled\nscorer=none\n\
                 omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=standard_folded_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase,ascii_folding\nstop_words=none\nstemmer=none\nmax_token_length=disabled\nscorer=bm25_v1\n\
                 omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=standard_folded_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase,ascii_folding\nstop_words=none\nstemmer=none\nmax_token_length=disabled\nscorer=none\n\
                 omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=english_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase,stem:english,stop_words:english,ascii_folding\nstop_words={english_stop_words}\nstemmer={ENGLISH_STEMMER}\nmax_token_length=disabled\nscorer=bm25_v1\n\
                 omnigraph.full-text-analysis.v1\nproperty_id=7\nprofile=english_v1\nnormalization=nfc\ntokenizer=simple\nfilters=lowercase,stem:english,stop_words:english,ascii_folding\nstop_words={english_stop_words}\nstemmer={ENGLISH_STEMMER}\nmax_token_length=disabled\nscorer=none\n"
            )
        );
        let fingerprints: Vec<String> = AnalyzerProfile::ALL
            .iter()
            .flat_map(|profile| {
                FullTextScorer::ALL.iter().map(|scorer| {
                    FullTextAnalysis {
                        analyzer: Some(*profile),
                        scorer: *scorer,
                    }
                    .fingerprint(7)
                })
            })
            .collect();
        assert_eq!(
            fingerprints,
            [
                "sha256:6ad77934fad9ff0dda843aded3ed181c2bc83da4c49ce948635549662d12923e",
                "sha256:42f7b664f81c323e425c82aa5060a2bbeae4d9612b3e3d95f5ef5f00ec51a490",
                "sha256:cfb99c30e5b911dedb8b69be27240e504433047f767204ef23f95b7a7f146a96",
                "sha256:640e3b66055e49c7f3935dc4cc553a7981bc9186c6913c58e883a7f1f35f55dd",
                "sha256:5c73936bcc2ef1e590aa912ffc8b17d17d7767b7398a4ca134551ddacb1c3b61",
                "sha256:6dad0095c7d82fa4ebbc57172e351a59f6369ed409a14514e52fb51d1b2a299b",
            ]
        );
        // Term positions are a build option of an index, never part of a
        // profile.
        assert!(!pinned_descriptors().contains("position"));
    }

    /// The stop list is each of NLTK's 198 English stop words and its
    /// Snowball English stem: the 13 stems a token can take that are not
    /// stop words themselves are in it, sorted and without repeats.
    #[test]
    fn english_v1_stop_words_hold_each_word_and_its_stem() {
        assert_eq!(ENGLISH_V1_STOP_WORDS.len(), 219);
        assert!(ENGLISH_V1_STOP_WORDS.windows(2).all(|pair| pair[0] < pair[1]));
        for stem in [
            "abov", "ani", "becaus", "befor", "doe", "dure", "onc", "onli", "ourselv", "themselv",
            "veri", "whi", "yourselv",
        ] {
            assert!(ENGLISH_V1_STOP_WORDS.contains(&stem), "{stem}");
        }
        for word in ["only", "very", "the", "being", "be", "you've", "you'v"] {
            assert!(ENGLISH_V1_STOP_WORDS.contains(&word), "{word}");
        }
    }

    /// The stemmer identity names the version the lockfile resolves.
    #[test]
    fn the_stemmer_identity_is_the_resolved_stemmer() {
        let lock = include_str!("../../../../Cargo.lock");
        let resolved = lock
            .split("[[package]]")
            .find(|package| package.contains("name = \"frostem\""))
            .and_then(|package| package.split("version = \"").nth(1))
            .and_then(|rest| rest.split('"').next())
            .unwrap();
        assert_eq!(ENGLISH_STEMMER, format!("snowball-english@frostem-{resolved}"));
    }

    #[test]
    fn the_fingerprint_binds_property_identity_profile_and_scorer() {
        let english = FullTextAnalysis {
            analyzer: Some(AnalyzerProfile::EnglishV1),
            scorer: FullTextScorer::Bm25V1,
        };
        let base = english.fingerprint(7);
        assert!(base.starts_with("sha256:"));
        assert_eq!(english.fingerprint(7), base);
        assert_ne!(english.fingerprint(8), base);
        let standard = FullTextAnalysis {
            analyzer: Some(AnalyzerProfile::StandardV1),
            ..english
        };
        assert_ne!(standard.fingerprint(7), base);
        let unranked = FullTextAnalysis {
            scorer: FullTextScorer::None,
            ..english
        };
        assert_ne!(unranked.fingerprint(7), base);
        // An omitted analyzer is `english_v1`.
        let omitted = FullTextAnalysis {
            analyzer: None,
            scorer: FullTextScorer::Bm25V1,
        };
        assert_eq!(omitted.fingerprint(7), base);
        assert_eq!(FullTextAnalysis::resolved(None), english);
    }

    #[test]
    fn only_a_resolved_profile_or_scorer_changes_the_index() {
        let english = FullTextAnalysis {
            analyzer: Some(AnalyzerProfile::EnglishV1),
            scorer: FullTextScorer::Bm25V1,
        };
        let standard = FullTextAnalysis {
            analyzer: Some(AnalyzerProfile::StandardV1),
            scorer: FullTextScorer::Bm25V1,
        };
        let unranked = FullTextAnalysis {
            analyzer: None,
            scorer: FullTextScorer::None,
        };
        // Writing out the default profile changes nothing.
        assert!(!changes_index_analysis(None, Some(&english)));
        assert!(!changes_index_analysis(Some(&english), None));
        assert!(changes_index_analysis(None, Some(&standard)));
        assert!(changes_index_analysis(Some(&standard), None));
        assert!(changes_index_analysis(None, Some(&unranked)));
        assert!(changes_index_analysis(
            Some(&english),
            Some(&FullTextAnalysis {
                scorer: FullTextScorer::None,
                ..english
            })
        ));
        assert!(!changes_index_analysis(Some(&english), Some(&english)));
    }

    #[test]
    fn a_default_declaration_is_absent() {
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
