//! What a full-text analyzer profile emits, and the measured fingerprint that
//! proves it (the GQ 3 RFC, section 4, Schema: Text, and RFC 0043).
//!
//! A profile has two implementations in this binary:
//!
//! - the *profile analyzer* ([`ProfileAnalyzer`]): NFC, then the profile's
//!   tokenizer and filters. The exact lexical scan runs it on every value
//!   and query, so it is the profile's meaning.
//! - the *native configuration* ([`native_params`]): the Lance parameters a
//!   full-text index of the profile is built with. Lance's builder tokenizes
//!   documents with `InvertedIndexParams::build` and its query path the
//!   same tokenizer's search stream; it has no Unicode normalization step.
//!
//! The declared analyzer fingerprint (`omnigraph_compiler`'s
//! `FullTextAnalysis::fingerprint`) names a profile; it does not prove that
//! the code running it still behaves as the profile did. The measured
//! fingerprint does: a digest of the tokens an implementation emits over the
//! checked-in probe corpus (`fts/probe_corpus.txt`), document and query path,
//! probe by probe. A certificate records the native configuration's measured
//! fingerprint when the index is built, and an index qualifies only where it
//! equals the one the running binary measures and that one equals the
//! profile analyzer's ([`crate::fts_compat::verify_index`]).
//!
//! Each profile is measured once per process, on first use, never per row,
//! query or verification.

use std::sync::OnceLock;

use lance_index::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer;
use lance_index::scalar::inverted::{
    DocumentGranularity, InvertedIndexParams, InvertedListFormatVersion, Language,
};
use lance_tokenizer::TokenStream;
use omnigraph_compiler::schema::analysis::{AnalyzerFilter, AnalyzerProfile, AnalyzerTokenizer};
use sha2::{Digest, Sha256};
use unicode_normalization::{IsNormalized, UnicodeNormalization, is_nfc_quick};

use crate::error::{OmniError, Result};
use crate::fts_compat::{NativeBuild, NativeNormalization};

/// The probe corpus, one JSON string literal per line (see its header).
pub const PROBE_CORPUS: &str = include_str!("../fts/probe_corpus.txt");

/// The framing of a measured fingerprint. A change of the framing, like a
/// change of the corpus, changes every measured fingerprint.
const MEASUREMENT_FRAMING: &[u8] = b"omnigraph.fts-measured-analysis.v1\0";

/// The posting block size every full-text build sets.
pub const NATIVE_BLOCK_SIZE: u32 = 128;
/// The inverted-list format every full-text build sets.
pub const NATIVE_POSTING_FORMAT: InvertedListFormatVersion = InvertedListFormatVersion::V2;

/// What every full-text build's postings hold, recorded in the segment's
/// certificate beside the field's fingerprint: no Unicode normalization
/// (Lance has none, so a profile's NFC is not applied to the postings), the
/// block size and format [`native_params`] sets, and no positions.
pub fn native_build() -> NativeBuild {
    NativeBuild {
        normalization: NativeNormalization::None,
        block_size: NATIVE_BLOCK_SIZE,
        posting_format: match NATIVE_POSTING_FORMAT {
            InvertedListFormatVersion::V1 => 1,
            InvertedListFormatVersion::V2 => 2,
            InvertedListFormatVersion::V3 => 3,
        },
        positions: false,
    }
}

/// The Lance parameters `profile` builds a full-text segment with, which are
/// also the tokenizer of its profile analyzer. Every parameter is set
/// explicitly (the GQ 3 RFC, section 4: no unspecified Lance default is part
/// of a profile), including the stop list, which is the profile's checked-in
/// list, never Lance's default, and the posting block size and format, so
/// neither Lance's defaults nor `LANCE_FTS_FORMAT_VERSION` can change it.
/// Term positions are not stored: they are a build option of the index, not
/// part of the profile. Lance 13 applies its filters in the order
/// `max_token_length`, lowercase, stemmer, stop words, ASCII folding
/// (`InvertedIndexParams::build`), which is every profile's order.
///
/// Lance has no Unicode normalization step: the profile's NFC is not applied
/// on this path, so text that is not already NFC tokenizes as written. The
/// measured fingerprints show where that changes the tokens.
pub fn native_params(profile: AnalyzerProfile) -> Result<InvertedIndexParams> {
    let pipeline = profile.pipeline();
    let tokenizer = match pipeline.tokenizer {
        AnalyzerTokenizer::Simple => "simple",
    };
    let has = |wanted: fn(&AnalyzerFilter) -> bool| pipeline.filters.iter().any(wanted);
    InvertedIndexParams::new(tokenizer.to_string(), Language::English)
        .document_granularity(DocumentGranularity::Row)
        .lance_tokenizer("text".to_string())
        .with_position(false)
        .max_token_length(pipeline.max_token_length)
        .lower_case(has(|filter| matches!(filter, AnalyzerFilter::Lowercase)))
        .stem(has(|filter| matches!(filter, AnalyzerFilter::Stem(_))))
        .remove_stop_words(has(|filter| matches!(filter, AnalyzerFilter::StopWords(_))))
        .custom_stop_words(
            pipeline
                .stop_words()
                .map(|words| words.iter().map(|word| (*word).to_string()).collect()),
        )
        .ascii_folding(has(|filter| matches!(filter, AnalyzerFilter::AsciiFolding)))
        .ngram_min_length(3)
        .ngram_max_length(3)
        .ngram_prefix_only(false)
        .split_identifiers(false)
        .split_on_numerics(false)
        .preserve_original(false)
        .index_operators(false)
        .format_version(NATIVE_POSTING_FORMAT)
        .block_size(NATIVE_BLOCK_SIZE as usize)
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "the {profile} full-text parameters are invalid: {error}"
            ))
        })
}

/// The Lance tokenizer of `profile`'s native configuration.
fn native_tokenizer(profile: AnalyzerProfile) -> Result<Box<dyn LanceTokenizer>> {
    native_params(profile)?.build().map_err(|error| {
        OmniError::manifest_internal(format!("the {profile} tokenizer does not build: {error}"))
    })
}

/// One profile's analyzer: NFC, then the profile's tokenizer and filters.
/// The exact lexical scan analyzes every value and query with it.
pub struct ProfileAnalyzer {
    tokenizer: Box<dyn LanceTokenizer>,
}

impl ProfileAnalyzer {
    /// The analyzer `profile` names.
    pub fn of(profile: AnalyzerProfile) -> Result<Self> {
        Ok(Self {
            tokenizer: native_tokenizer(profile)?,
        })
    }

    /// Every analyzed token of `text` into `token`, in order, through Lance's
    /// search path for a `query` and its document path otherwise. `text` and
    /// its NFC form hold at most `limit` bytes; `resource` names that bound.
    /// `normalized` is told the bytes of an NFC copy before it is built.
    pub fn analyze(
        &mut self,
        text: &str,
        limit: usize,
        resource: &'static str,
        query: bool,
        mut normalized: impl FnMut(usize) -> Result<()>,
        mut token: impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        if text.len() > limit {
            return Err(OmniError::resource_limit(
                resource,
                limit as u64,
                text.len() as u64,
            ));
        }
        let copy;
        let text = if is_nfc_quick(text.chars()) == IsNormalized::Yes {
            text
        } else {
            normalized(text.len())?;
            let mut out = String::with_capacity(text.len());
            for c in text.nfc() {
                if out.len() + c.len_utf8() > limit {
                    return Err(OmniError::resource_limit(
                        resource,
                        limit as u64,
                        (out.len() + c.len_utf8()) as u64,
                    ));
                }
                out.push(c);
            }
            copy = out;
            copy.as_str()
        };
        let mut stream = if query {
            self.tokenizer.token_stream_for_search(text)
        } else {
            self.tokenizer.token_stream_for_doc(text)
        };
        while stream.advance() {
            token(&stream.token().text)?;
        }
        Ok(())
    }
}

/// The two implementations of a profile this module measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Implementation {
    /// The Lance tokenizer a full-text index of the profile is built and
    /// queried with ([`native_params`]).
    Native,
    /// The profile analyzer the exact lexical scan runs ([`ProfileAnalyzer`]).
    Analyzer,
}

/// One probe's tokens through one implementation: the document path, then
/// the query path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeTokens {
    pub document: Vec<String>,
    pub query: Vec<String>,
}

/// The measured fingerprints of one profile in this binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasuredAnalysis {
    /// The digest of the native configuration's tokens over the corpus.
    pub native: String,
    /// The digest of the profile analyzer's tokens over the corpus.
    pub analyzer: String,
}

impl MeasuredAnalysis {
    /// Whether a native index of the profile emits the profile analyzer's
    /// tokens on every probe: the parity between index and scan.
    pub fn has_parity(&self) -> bool {
        self.native == self.analyzer
    }
}

/// The probes of [`PROBE_CORPUS`], in order.
pub fn probes() -> Result<&'static [String]> {
    static PROBES: OnceLock<std::result::Result<Vec<String>, String>> = OnceLock::new();
    PROBES
        .get_or_init(|| parse_probes(PROBE_CORPUS))
        .as_deref()
        .map_err(|error| OmniError::manifest_internal(error.clone()))
}

fn parse_probes(corpus: &str) -> std::result::Result<Vec<String>, String> {
    corpus
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.is_empty() && !line.starts_with('#'))
        .map(|(number, line)| {
            serde_json::from_str::<String>(line).map_err(|error| {
                format!(
                    "the full-text probe corpus line {} is not a JSON string: {error}",
                    number + 1
                )
            })
        })
        .collect()
}

/// The tokens `implementation` of `profile` emits for every probe, in order.
pub fn probe_tokens(
    profile: AnalyzerProfile,
    implementation: Implementation,
) -> Result<Vec<ProbeTokens>> {
    let probes = probes()?;
    let mut out = Vec::with_capacity(probes.len());
    match implementation {
        Implementation::Native => {
            let mut tokenizer = native_tokenizer(profile)?;
            for probe in probes {
                let mut path = |query: bool| {
                    let mut stream = if query {
                        tokenizer.token_stream_for_search(probe)
                    } else {
                        tokenizer.token_stream_for_doc(probe)
                    };
                    let mut tokens = Vec::new();
                    while stream.advance() {
                        tokens.push(stream.token().text.clone());
                    }
                    tokens
                };
                let document = path(false);
                let query = path(true);
                out.push(ProbeTokens { document, query });
            }
        }
        Implementation::Analyzer => {
            let mut analyzer = ProfileAnalyzer::of(profile)?;
            for probe in probes {
                let mut path = |query: bool| {
                    let mut tokens = Vec::new();
                    analyzer.analyze(
                        probe,
                        usize::MAX,
                        "full_text_probe_bytes",
                        query,
                        |_| Ok(()),
                        |token| {
                            tokens.push(token.to_string());
                            Ok(())
                        },
                    )?;
                    Ok::<_, OmniError>(tokens)
                };
                let document = path(false)?;
                let query = path(true)?;
                out.push(ProbeTokens { document, query });
            }
        }
    }
    Ok(out)
}

/// The measured fingerprint of `tokens`, the output of one implementation
/// over [`probes`]: a SHA-256 over the framing, then per probe its bytes and
/// its document and query token sequences, each count and string
/// length-prefixed, so no two different outputs share a digest.
fn fingerprint(probes: &[String], tokens: &[ProbeTokens]) -> String {
    fn bytes(digest: &mut Sha256, bytes: &[u8]) {
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    let mut digest = Sha256::new();
    digest.update(MEASUREMENT_FRAMING);
    digest.update((probes.len() as u64).to_le_bytes());
    for (probe, tokens) in probes.iter().zip(tokens) {
        bytes(&mut digest, probe.as_bytes());
        for path in [&tokens.document, &tokens.query] {
            digest.update((path.len() as u64).to_le_bytes());
            for token in path {
                bytes(&mut digest, token.as_bytes());
            }
        }
    }
    format!("sha256:{:x}", digest.finalize())
}

fn measure(profile: AnalyzerProfile) -> Result<MeasuredAnalysis> {
    let probes = probes()?;
    Ok(MeasuredAnalysis {
        native: fingerprint(probes, &probe_tokens(profile, Implementation::Native)?),
        analyzer: fingerprint(probes, &probe_tokens(profile, Implementation::Analyzer)?),
    })
}

/// The measured fingerprints of `profile` in this binary, computed on the
/// first call for the profile and kept for the process.
pub fn measured(profile: AnalyzerProfile) -> Result<&'static MeasuredAnalysis> {
    static MEASURED: [OnceLock<std::result::Result<MeasuredAnalysis, String>>;
        AnalyzerProfile::ALL.len()] = [const { OnceLock::new() }; AnalyzerProfile::ALL.len()];
    let slot = AnalyzerProfile::ALL
        .iter()
        .position(|candidate| *candidate == profile)
        .expect("every profile is in AnalyzerProfile::ALL");
    MEASURED[slot]
        .get_or_init(|| measure(profile).map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|error| {
            OmniError::manifest_internal(format!(
                "the {profile} measured fingerprint is unavailable: {error}"
            ))
        })
}

/// One probe on which a profile's native configuration and its analyzer
/// emit different tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeDifference {
    pub probe: String,
    pub native: ProbeTokens,
    pub analyzer: ProbeTokens,
}

/// The probes on which `profile`'s native configuration and its analyzer
/// disagree: empty exactly when [`MeasuredAnalysis::has_parity`].
pub fn parity_differences(profile: AnalyzerProfile) -> Result<Vec<ProbeDifference>> {
    let native = probe_tokens(profile, Implementation::Native)?;
    let analyzer = probe_tokens(profile, Implementation::Analyzer)?;
    Ok(probes()?
        .iter()
        .zip(native.into_iter().zip(analyzer))
        .filter(|(_, (native, analyzer))| native != analyzer)
        .map(|(probe, (native, analyzer))| ProbeDifference {
            probe: probe.clone(),
            native,
            analyzer,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    use omnigraph_compiler::schema::analysis::ENGLISH_V1_STOP_WORDS;

    /// Where the golden of every profile's measured fingerprints lives.
    const GOLDEN_FILE: &str = "fts/measured_fingerprints.golden";
    const GOLDEN: &str = include_str!("../fts/measured_fingerprints.golden");
    /// Set to `1` to rewrite [`GOLDEN_FILE`] from this binary; refused in CI.
    const REGENERATE_ENV: &str = "OMNIGRAPH_REGEN_FTS_FINGERPRINTS";

    fn render_golden() -> String {
        let mut out = String::from(
            "# The measured fingerprints of every analyzer profile: the native configuration's\n\
             # and the profile analyzer's, over fts/probe_corpus.txt. Equal fingerprints mean\n\
             # parity between a native index and the exact scan. Regenerate only as\n\
             # docs/dev/testing.md describes; never refresh it to turn the test green.\n",
        );
        for profile in AnalyzerProfile::ALL {
            let measured = measured(profile).unwrap();
            out.push_str(&format!("{profile} native {}\n", measured.native));
            out.push_str(&format!("{profile} analyzer {}\n", measured.analyzer));
        }
        out
    }

    /// A released profile never changes once accepted (the GQ 3 RFC,
    /// section 4), and this golden pins what each one emits in this binary.
    /// A dependency bump that changes the tokens of either implementation of
    /// a profile turns it red, naming the profile.
    #[test]
    fn measured_fingerprints_are_the_golden() {
        let actual = render_golden();
        if std::env::var(REGENERATE_ENV).as_deref() == Ok("1") {
            assert!(
                std::env::var_os("CI").is_none(),
                "{REGENERATE_ENV} regenerates the golden locally, never in CI"
            );
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN_FILE);
            std::fs::write(&path, &actual).unwrap();
            return;
        }
        let pinned = |text: &str| -> Vec<String> {
            text.lines()
                .filter(|line| !line.starts_with('#'))
                .map(str::to_owned)
                .collect()
        };
        let (expected, actual) = (pinned(GOLDEN), pinned(&actual));
        for line in &expected {
            assert!(
                actual.contains(line),
                "the measured fingerprint `{line}` changed; this binary measures:\n{}\n\n\
                 This is a profile change: the tokens this profile emits over the probe corpus \
                 are not the tokens it emitted when it was accepted. A profile never changes \
                 once accepted (the GQ 3 RFC, section 4). Keep the old behavior (pin or patch \
                 the dependency that changed it) or define a new profile; never refresh the \
                 golden alone. Regenerate ({REGENERATE_ENV}=1, docs/dev/testing.md) only for a \
                 new profile or a reviewed change of the probe corpus.",
                actual.join("\n")
            );
        }
        assert_eq!(actual, expected, "the golden names every profile once");
    }

    #[test]
    fn the_probe_corpus_exercises_every_pipeline_step() {
        let probes = probes().unwrap();
        let words: std::collections::HashSet<&str> = probes
            .iter()
            .flat_map(|probe| probe.split_whitespace())
            .collect();
        for word in ENGLISH_V1_STOP_WORDS {
            assert!(words.contains(word), "stop word `{word}` is not probed");
        }
        // Empty and whitespace-only values.
        assert!(probes.iter().any(String::is_empty));
        assert!(
            probes
                .iter()
                .any(|probe| !probe.is_empty() && probe.chars().all(char::is_whitespace))
        );
        // Text NFC changes, and text it keeps that NFKC would change.
        let nfc_changes = probes
            .iter()
            .filter(|probe| probe.nfc().collect::<String>() != **probe)
            .count();
        assert!(nfc_changes >= 5, "{nfc_changes} probes are not NFC");
        assert!(probes.iter().any(|probe| probe.contains('\u{fb01}')));
        // Tokens at and around Lance's default 40-byte limit, and far over it.
        for length in [39, 40, 41, 300] {
            assert!(
                probes
                    .iter()
                    .any(|probe| probe.split_whitespace().any(|word| word.len() == length)),
                "no {length}-byte token"
            );
        }
        // A letter directly followed by the trade mark sign.
        assert!(probes.iter().any(|probe| probe.contains("x\u{2122}")));
        // Scripts beyond Latin.
        for script in ['М', 'Ο', 'م', 'ש', 'न', '東', 'カ', '한', 'ภ', '😀'] {
            assert!(
                probes.iter().any(|probe| probe.contains(script)),
                "no probe holds {script}"
            );
        }
        let distinct: std::collections::HashSet<_> = probes.iter().collect();
        assert_eq!(distinct.len(), probes.len(), "a probe is repeated");
    }

    /// Lance 13's native configuration has no NFC step, so no profile's
    /// native index emits its analyzer's tokens: parity fails for every
    /// profile, and on exactly the probes NFC changes. A dependency change
    /// that makes them differ anywhere else turns this red.
    #[test]
    fn native_configurations_differ_from_their_analyzers_only_by_nfc() {
        for profile in AnalyzerProfile::ALL {
            let measured = measured(profile).unwrap();
            let differences = parity_differences(profile).unwrap();
            for difference in &differences {
                println!(
                    "{profile}: {:?} native {:?} analyzer {:?}",
                    difference.probe, difference.native.document, difference.analyzer.document
                );
            }
            assert_eq!(measured.has_parity(), differences.is_empty(), "{profile}");
            assert!(!measured.has_parity(), "{profile} has parity");
            for difference in &differences {
                assert_ne!(
                    difference.probe.nfc().collect::<String>(),
                    difference.probe,
                    "{profile} differs on an NFC probe: {difference:?}"
                );
            }
        }
    }

    /// The profile analyzer is the native tokenizer behind NFC: on NFC text
    /// the two emit the same tokens on both paths.
    #[test]
    fn the_analyzer_is_nfc_then_the_native_tokenizer() {
        for profile in AnalyzerProfile::ALL {
            let native = probe_tokens(profile, Implementation::Native).unwrap();
            let analyzer = probe_tokens(profile, Implementation::Analyzer).unwrap();
            for ((probe, native), analyzer) in probes().unwrap().iter().zip(&native).zip(&analyzer)
            {
                assert_eq!(native.document, native.query, "{profile} {probe:?}");
                assert_eq!(analyzer.document, analyzer.query, "{profile} {probe:?}");
                if probe.nfc().collect::<String>() == *probe {
                    assert_eq!(native, analyzer, "{profile} {probe:?}");
                }
            }
        }
    }

    #[test]
    fn a_fingerprint_binds_every_probe_and_token() {
        let probes = vec!["ab".to_string(), "c".to_string()];
        let tokens = |document: &[&str]| ProbeTokens {
            document: document.iter().map(|token| token.to_string()).collect(),
            query: document.iter().map(|token| token.to_string()).collect(),
        };
        let base = fingerprint(&probes, &[tokens(&["ab"]), tokens(&["c"])]);
        assert!(base.starts_with("sha256:"));
        assert_ne!(
            fingerprint(&probes, &[tokens(&["a", "b"]), tokens(&["c"])]),
            base
        );
        assert_ne!(
            fingerprint(&probes, &[tokens(&["ab", "c"]), tokens(&[])]),
            base
        );
        let mut query = tokens(&["ab"]);
        query.query.clear();
        assert_ne!(fingerprint(&probes, &[query, tokens(&["c"])]), base);
        let other = vec!["ab".to_string(), "d".to_string()];
        assert_ne!(
            fingerprint(&other, &[tokens(&["ab"]), tokens(&["c"])]),
            base
        );
        assert!(parse_probes("# a comment\n\n\"x\"\nnot json\n").is_err());
        assert_eq!(
            parse_probes("# c\n\"\"\n\" \"\n\"\\u0301\"\n").unwrap(),
            ["", " ", "\u{301}"]
        );
    }

    /// Measuring every profile is a one-time process cost.
    #[test]
    fn measuring_every_profile_is_cheap() {
        let started = std::time::Instant::now();
        for profile in AnalyzerProfile::ALL {
            measure(profile).unwrap();
        }
        let elapsed = started.elapsed();
        println!(
            "measured {} profiles over {} probes in {elapsed:?}",
            AnalyzerProfile::ALL.len(),
            probes().unwrap().len()
        );
        assert!(elapsed < std::time::Duration::from_secs(5), "{elapsed:?}");
    }
}
