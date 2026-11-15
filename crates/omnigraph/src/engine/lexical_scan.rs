//! The exact lexical scan (docs/rfcs/2026-11-07-gq-3.md, section 4, "The
//! lexical query", "bm25_v1" and "Exact execution and qualified
//! acceleration").
//!
//! A field whose full-text index declares an analyzer profile is answered
//! here, at every coverage state, without reading the index: the analyzer is
//! instantiated from the accepted schema's profile whether or not an index
//! exists, so a declared but unbuilt index answers. The scan streams the
//! field's table at the pinned snapshot, normalizes each value to NFC,
//! tokenizes it with the profile's pipeline (Lance's own builder, so the
//! tokens are the ones the profile's index would hold) and evaluates the
//! `terms` relation: every (`all`) or some (`any`) distinct analyzed query
//! term has a stored term within the edit budget, the edit distance counting
//! one per inserted, deleted or substituted Unicode scalar value, so a
//! transposition costs two. A null or token-empty value never matches.
//!
//! `bm25` scores the matches with `bm25_v1` in `F64`: the statistics are the
//! field corpus, every entity of the table with a non-empty analyzed value,
//! before any graph or property filter; alternatives within a term's budget
//! share its group's IDF, the best of them weighted `1`, `1/2` or `1/4` by
//! its edits; parts add in ascending UTF-8 query-term order with no
//! reassociation; the IDF uses `libm`'s `log1p`; counts are checked `u64`.
//!
//! Query bytes, distinct query terms and value bytes are bounded before the
//! text is normalized (NFC can expand UTF-8) or tokenized, and every analyzed
//! token and term comparison is charged against the read's work budget; the
//! results the scan keeps are charged to the query's memory. Exhaustion is a
//! typed failure, never a partial answer.

use std::collections::{HashMap, HashSet};

use arrow_array::{Array, RecordBatch, StringArray};
use futures::StreamExt;
use lance_index::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer;
use lance_tokenizer::TokenStream;
use omnigraph_compiler::catalog::{Catalog, NodeType};
use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::ir::IRExpr;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::ir::expr::{ScalarFunc, TermsMode};
use omnigraph_compiler::query::codes::{E1, E3, T20, T71};
use omnigraph_compiler::query::diagnostic::QueryDiagnostic;
use omnigraph_compiler::schema::analysis::AnalyzerProfile;
use unicode_normalization::{IsNormalized, UnicodeNormalization, is_nfc_quick};

use super::operators::memory::WorkMemory;
use super::{OmniError, Result, Snapshot};

/// The most bytes a lexical query's text may hold, before and after NFC.
pub(crate) const QUERY_MAX_BYTES: usize = 64 * 1024;
/// The most distinct analyzed terms a lexical query may hold.
pub(crate) const QUERY_MAX_TERMS: usize = 1024;
/// The most bytes one stored value may hold, before and after NFC.
pub(crate) const VALUE_MAX_BYTES: usize = 16 * 1024 * 1024;
/// The largest edit budget `terms(…)` admits.
pub(crate) const MAX_EDITS: u8 = 2;

/// The edit weights of `bm25_v1`, by edits: `1`, `1/2`, `1/4`.
const EDIT_WEIGHTS: [f64; 3] = [1.0, 0.5, 0.25];

/// One profile's analyzer: NFC, then the profile's tokenizer and filters.
pub(crate) struct Analyzer {
    tokenizer: Box<dyn LanceTokenizer>,
}

impl Analyzer {
    /// The analyzer `profile` names, built from the parameters its index is
    /// built with.
    pub(crate) fn of(profile: AnalyzerProfile) -> Result<Self> {
        let params =
            crate::table_store::full_text_index_params(&crate::storage_layer::FullTextBuild {
                profile: Some(profile),
                fingerprint: None,
            })?;
        let tokenizer = params.build().map_err(|error| {
            OmniError::manifest_internal(format!("the {profile} analyzer does not build: {error}"))
        })?;
        Ok(Self { tokenizer })
    }

    /// Every analyzed token of `text` into `token`, in order. `text` and its
    /// NFC form hold at most `limit` bytes; `resource` names that bound.
    fn analyze(
        &mut self,
        text: &str,
        limit: usize,
        resource: &'static str,
        mut token: impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        if text.len() > limit {
            return Err(OmniError::resource_limit(
                resource,
                limit as u64,
                text.len() as u64,
            ));
        }
        let normalized;
        let text = if is_nfc_quick(text.chars()) == IsNormalized::Yes {
            text
        } else {
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
            normalized = out;
            normalized.as_str()
        };
        let mut stream = self.tokenizer.token_stream_for_doc(text);
        while stream.advance() {
            token(&stream.token().text)?;
        }
        Ok(())
    }
}

/// A `terms(text, mode, max_edits)` value analyzed with its field's
/// analyzer: the distinct terms in ascending UTF-8 order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LexicalQuery {
    terms: Vec<String>,
    /// Each term as Unicode scalar values, for the edit distance.
    chars: Vec<Vec<char>>,
    all: bool,
    max_edits: u8,
}

impl LexicalQuery {
    /// `text` analyzed with `analyzer`, every term (`all`) or any one
    /// required, within `max_edits`. A budget outside `0..=2` is `T71`, a
    /// text with no searchable term `T20`, both type errors.
    pub(crate) fn analyze(
        analyzer: &mut Analyzer,
        text: &str,
        all: bool,
        max_edits: i128,
    ) -> Result<Self> {
        let max_edits = u8::try_from(max_edits)
            .ok()
            .filter(|edits| *edits <= MAX_EDITS)
            .ok_or_else(|| type_error(T71, format!("`max_edits: {max_edits}` is outside 0..=2")))?;
        let mut terms: Vec<String> = Vec::new();
        analyzer.analyze(text, QUERY_MAX_BYTES, "lexical_query_bytes", |token| {
            if let Err(at) = terms.binary_search_by(|term| term.as_str().cmp(token)) {
                if terms.len() == QUERY_MAX_TERMS {
                    return Err(OmniError::resource_limit(
                        "lexical_query_terms",
                        QUERY_MAX_TERMS as u64,
                        QUERY_MAX_TERMS as u64 + 1,
                    ));
                }
                terms.insert(at, token.to_string());
            }
            Ok(())
        })?;
        if terms.is_empty() {
            return Err(type_error(
                T20,
                format!(
                    "the lexical query `{text}` has no searchable terms under its field's analyzer"
                ),
            ));
        }
        let chars = terms.iter().map(|term| term.chars().collect()).collect();
        Ok(Self {
            terms,
            chars,
            all,
            max_edits,
        })
    }

    #[cfg(test)]
    pub(crate) fn terms(&self) -> &[String] {
        &self.terms
    }
}

fn type_error(
    code: omnigraph_compiler::query::diagnostic::QueryCode,
    message: String,
) -> OmniError {
    OmniError::Compiler(CompilerError::query(QueryDiagnostic::typecheck(
        code, message,
    )))
}

fn numeric_failure(
    code: omnigraph_compiler::query::diagnostic::QueryCode,
    message: String,
) -> OmniError {
    OmniError::Compiler(CompilerError::query(QueryDiagnostic::execution(
        code, message,
    )))
}

/// The edit distance of `a` to `b` over Unicode scalar values (insert,
/// delete and substitute one each), when it is at most `bound`; `None`
/// beyond it. A band of `2 · bound + 1` cells per row, so the work is
/// linear in the shorter term.
pub(crate) fn bounded_edits(a: &[char], b: &[char], bound: u8) -> Option<u8> {
    let k = usize::from(bound);
    let (n, m) = (a.len(), b.len());
    if n.abs_diff(m) > k {
        return None;
    }
    if k == 0 {
        return (a == b).then_some(0);
    }
    const FAR: usize = usize::MAX / 4;
    let mut prev = vec![FAR; m + 1];
    for (j, cell) in prev.iter_mut().enumerate().take(k.min(m) + 1) {
        *cell = j;
    }
    let mut cur = vec![FAR; m + 1];
    for i in 1..=n {
        let lo = i.saturating_sub(k);
        let hi = (i + k).min(m);
        let mut row_min = FAR;
        if lo == 0 {
            cur[0] = i;
            row_min = i;
        } else {
            cur[lo - 1] = FAR;
        }
        for j in lo.max(1)..=hi {
            let substitute = prev[j - 1] + usize::from(a[i - 1] != b[j - 1]);
            let delete = prev[j] + 1;
            let insert = cur[j - 1] + 1;
            cur[j] = substitute.min(delete).min(insert);
            row_min = row_min.min(cur[j]);
        }
        if hi < m {
            cur[hi + 1] = FAR;
        }
        if row_min > k {
            return None;
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    (prev[m] <= k).then(|| prev[m] as u8)
}

/// One analyzed value: its distinct terms with their occurrences, and its
/// token count.
struct Document {
    terms: Vec<(String, u64)>,
    len: u64,
}

/// What one value holds within each query term's budget: per term, the
/// stored alternatives as `(edits, occurrences)`.
struct Hits {
    by_term: Vec<Vec<(u8, u64)>>,
}

impl Hits {
    fn matched(&self, all: bool) -> bool {
        if all {
            self.by_term.iter().all(|hits| !hits.is_empty())
        } else {
            self.by_term.iter().any(|hits| !hits.is_empty())
        }
    }
}

/// The scan of one field: its table at the snapshot, its analyzer and the
/// memory and work it charges.
struct FieldScan<'a> {
    analyzer: Analyzer,
    query: &'a LexicalQuery,
    memory: &'a WorkMemory,
}

impl FieldScan<'_> {
    /// `text` analyzed: `None` when it holds no token.
    fn document(&mut self, text: &str) -> Result<Option<Document>> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        let mut len = 0u64;
        self.analyzer
            .analyze(text, VALUE_MAX_BYTES, "lexical_value_bytes", |token| {
                len = checked(len, 1, "a value's token count")?;
                match counts.get_mut(token) {
                    Some(count) => *count = checked(*count, 1, "a term's occurrences")?,
                    None => {
                        counts.insert(token.to_string(), 1);
                    }
                }
                Ok(())
            })?;
        if len == 0 {
            return Ok(None);
        }
        self.memory
            .charge_produced(len)
            .map_err(|error| self.memory.error(error))?;
        let mut terms: Vec<(String, u64)> = counts.into_iter().collect();
        terms.sort_unstable();
        Ok(Some(Document { terms, len }))
    }

    /// The stored alternatives of `document` within each query term's
    /// budget.
    fn hits(&self, document: &Document) -> Result<Hits> {
        let query = self.query;
        let mut by_term = vec![Vec::new(); query.terms.len()];
        if query.max_edits == 0 {
            self.memory
                .charge_produced(query.terms.len() as u64)
                .map_err(|error| self.memory.error(error))?;
            for (term, hits) in query.terms.iter().zip(&mut by_term) {
                if let Ok(at) = document
                    .terms
                    .binary_search_by(|(stored, _)| stored.as_str().cmp(term))
                {
                    hits.push((0, document.terms[at].1));
                }
            }
            return Ok(Hits { by_term });
        }
        let comparisons = (document.terms.len() as u64).saturating_mul(query.terms.len() as u64);
        self.memory
            .charge_produced(comparisons)
            .map_err(|error| self.memory.error(error))?;
        for (stored, occurrences) in &document.terms {
            let stored: Vec<char> = stored.chars().collect();
            for (term, hits) in query.chars.iter().zip(&mut by_term) {
                if let Some(edits) = bounded_edits(term, &stored, query.max_edits) {
                    hits.push((edits, *occurrences));
                }
            }
        }
        Ok(Hits { by_term })
    }
}

fn checked(left: u64, right: u64, what: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| numeric_failure(E1, format!("{what} overflowed u64 in `bm25_v1`")))
}

/// The string column `name` of `batch`.
fn strings<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b StringArray> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "the exact lexical scan reads `{name}`, which its batch does not carry as a string"
            ))
        })
}

/// Stream `type_name`'s `property` with its identity at `snapshot`, calling
/// `row` with each row's identity and value.
async fn scan_field(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    property: &str,
    memory: &WorkMemory,
    mut row: impl FnMut(&str, Option<&str>) -> Result<()>,
) -> Result<()> {
    let id_column = catalog.system_columns.id;
    let dataset = snapshot
        .open_lance_dataset(&format!("node:{type_name}"))
        .await?;
    let plan = crate::table_store::TableStore::scan_plan_with(
        &dataset,
        Some(&[id_column, property]),
        None,
        false,
        |_| Ok(()),
    )
    .await?;
    let batches = memory
        .child("exact lexical scan batch")
        .map_err(|error| memory.error(error))?;
    let (_, mut stream) = batches.stream(plan).map_err(|error| memory.error(error))?;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| memory.error(error))?;
        batches.hold(&batch).map_err(|error| memory.error(error))?;
        let ids = strings(&batch, id_column)?;
        let values = strings(&batch, property)?;
        for index in 0..batch.num_rows() {
            if ids.is_null(index) {
                continue;
            }
            let value = values.is_valid(index).then(|| values.value(index));
            row(ids.value(index), value)?;
        }
        batches.release_work();
    }
    Ok(())
}

/// The node type `type_name` and the analyzer its `property` declares.
fn field_analyzer(catalog: &Catalog, type_name: &str, property: &str) -> Result<Analyzer> {
    let profile = declared_profile(catalog, type_name, property).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "the exact lexical scan reads `{type_name}.{property}`, whose index declares no analyzer"
        ))
    })?;
    Analyzer::of(profile)
}

/// The profile `type_name.property`'s full-text index declares.
pub(crate) fn declared_profile(
    catalog: &Catalog,
    type_name: &str,
    property: &str,
) -> Option<AnalyzerProfile> {
    catalog
        .node_types
        .get(type_name)
        .and_then(|node: &NodeType| node.full_text_profile(property))
}

/// A `match_terms(field, terms(text, mode, max_edits))` call's query,
/// analyzed with the analyzer `type_name.property` declares.
pub(crate) fn query_of_call(
    catalog: &Catalog,
    type_name: &str,
    call: &IRExpr,
    params: &ParamMap,
) -> Result<(String, LexicalQuery)> {
    let IRExpr::Call {
        func: ScalarFunc::MatchTerms { mode },
        args,
        ..
    } = call
    else {
        return Err(OmniError::manifest_internal(format!(
            "the exact lexical scan answers `match_terms`, not `{call}`"
        )));
    };
    let [IRExpr::PropAccess { property, .. }, text, max_edits] = args.as_slice() else {
        return Err(OmniError::manifest_internal(format!(
            "`{call}` is not a field, a text and an edit budget"
        )));
    };
    let text = super::scan::resolve_to_string(text, params)?;
    let max_edits = super::scan::resolve_to_int(max_edits, params)?;
    let mut analyzer = field_analyzer(catalog, type_name, property)?;
    let query = LexicalQuery::analyze(
        &mut analyzer,
        &text,
        matches!(mode, TermsMode::All),
        max_edits,
    )?;
    Ok((property.clone(), query))
}

/// The identities of every entity of `type_name` whose `property` matches
/// `query`.
pub(crate) async fn members(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    property: &str,
    query: &LexicalQuery,
    memory: &WorkMemory,
) -> Result<HashSet<String>> {
    let mut scan = FieldScan {
        analyzer: field_analyzer(catalog, type_name, property)?,
        query,
        memory,
    };
    let mut ids = HashSet::new();
    scan_field(
        snapshot,
        catalog,
        type_name,
        property,
        memory,
        |id, value| {
            let Some(value) = value else {
                return Ok(());
            };
            let Some(document) = scan.document(value)? else {
                return Ok(());
            };
            if scan.hits(&document)?.matched(query.all) {
                memory
                    .string(id.len())
                    .map_err(|error| memory.error(error))?;
                memory
                    .entries::<String>(1)
                    .map_err(|error| memory.error(error))?;
                ids.insert(id.to_string());
            }
            Ok(())
        },
    )
    .await?;
    Ok(ids)
}

/// One matching value kept for scoring once the corpus statistics are known.
struct Match {
    id: String,
    len: u64,
    /// Per query term, its stored alternatives as `(edits, occurrences)`.
    hits: Vec<Vec<(u8, u64)>>,
}

/// The corpus statistics of `bm25_v1`: the field corpus size, its total
/// token count and, per query term, the documents holding an alternative.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct Corpus {
    pub(crate) documents: u64,
    pub(crate) tokens: u64,
    pub(crate) holding: Vec<u64>,
}

impl Corpus {
    /// The IDF of each query term, in query-term order.
    pub(crate) fn idf(&self) -> Vec<f64> {
        let n = self.documents as f64;
        self.holding
            .iter()
            .map(|&df| {
                let df = df as f64;
                libm::log1p((n - df + 0.5) / (df + 0.5))
            })
            .collect()
    }

    /// The `bm25_v1` score of a matching document of `len` tokens whose
    /// alternatives per query term are `hits`, with `idf` from [`Self::idf`].
    pub(crate) fn score(&self, idf: &[f64], len: u64, hits: &[Vec<(u8, u64)>]) -> Result<f64> {
        let avg_len = self.tokens as f64 / self.documents as f64;
        let norm = 1.2 * (0.25 + 0.75 * len as f64 / avg_len);
        let mut total = 0f64;
        for (idf, hits) in idf.iter().zip(hits) {
            let mut best: Option<f64> = None;
            for &(edits, occurrences) in hits {
                let tf = occurrences as f64;
                let weight = EDIT_WEIGHTS[usize::from(edits)] * 2.2 * tf / (tf + norm);
                best = Some(best.map_or(weight, |best| best.max(weight)));
            }
            if let Some(best) = best {
                total += idf * best;
            }
        }
        if !total.is_finite() || total <= 0.0 {
            return Err(numeric_failure(
                E3,
                format!("a `bm25_v1` score is {total}, not a finite positive number"),
            ));
        }
        Ok(total)
    }
}

/// `bm25_v1` of every entity of `type_name` whose `property` matches `query`,
/// by identity, against the field corpus.
pub(crate) async fn bm25(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    property: &str,
    query: &LexicalQuery,
    memory: &WorkMemory,
) -> Result<HashMap<String, f64>> {
    let mut scan = FieldScan {
        analyzer: field_analyzer(catalog, type_name, property)?,
        query,
        memory,
    };
    let mut corpus = Corpus {
        holding: vec![0; query.terms.len()],
        ..Corpus::default()
    };
    let mut matches: Vec<Match> = Vec::new();
    scan_field(
        snapshot,
        catalog,
        type_name,
        property,
        memory,
        |id, value| {
            let Some(value) = value else {
                return Ok(());
            };
            let Some(document) = scan.document(value)? else {
                return Ok(());
            };
            corpus.documents = checked(corpus.documents, 1, "the field corpus size")?;
            corpus.tokens = checked(corpus.tokens, document.len, "the field corpus token count")?;
            let hits = scan.hits(&document)?;
            for (holding, hits) in corpus.holding.iter_mut().zip(&hits.by_term) {
                if !hits.is_empty() {
                    *holding = checked(*holding, 1, "a term's document frequency")?;
                }
            }
            if hits.matched(query.all) {
                let alternatives: usize = hits.by_term.iter().map(Vec::len).sum();
                memory
                    .string(id.len())
                    .map_err(|error| memory.error(error))?;
                memory
                    .entries::<(u8, u64)>(alternatives.saturating_add(hits.by_term.len()))
                    .map_err(|error| memory.error(error))?;
                matches.push(Match {
                    id: id.to_string(),
                    len: document.len,
                    hits: hits.by_term,
                });
            }
            Ok(())
        },
    )
    .await?;
    let idf = corpus.idf();
    memory
        .entries::<(String, f64)>(matches.len())
        .map_err(|error| memory.error(error))?;
    let mut scores = HashMap::with_capacity(matches.len());
    for found in matches {
        let score = corpus.score(&idf, found.len, &found.hits)?;
        scores.insert(found.id, score);
    }
    Ok(scores)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chars(text: &str) -> Vec<char> {
        text.chars().collect()
    }

    #[test]
    fn the_edit_distance_counts_scalar_values_and_a_transposition_twice() {
        let edits = |a: &str, b: &str, k| bounded_edits(&chars(a), &chars(b), k);
        assert_eq!(edits("beta", "beta", 0), Some(0));
        assert_eq!(edits("beta", "beto", 0), None);
        assert_eq!(edits("beta", "beto", 1), Some(1));
        assert_eq!(edits("abcd", "acbd", 1), None);
        assert_eq!(edits("abcd", "acbd", 2), Some(2));
        assert_eq!(edits("abcd", "ab", 2), Some(2));
        assert_eq!(edits("abcd", "a", 2), None);
        assert_eq!(edits("café", "cafe", 1), Some(1));
        assert_eq!(edits("", "ab", 2), Some(2));
        assert_eq!(edits("abcd", "abef", 2), Some(2));
        assert_eq!(edits("abcd", "zxyz", 2), None);
        // The band agrees with the full table on every short pair.
        let alphabet = ['a', 'b', 'c'];
        let mut words = vec![String::new()];
        for _ in 0..4 {
            let longer: Vec<String> = words
                .iter()
                .flat_map(|w| alphabet.iter().map(move |c| format!("{w}{c}")))
                .collect();
            words.extend(longer);
            words.sort();
            words.dedup();
        }
        let full = |a: &[char], b: &[char]| {
            let mut prev: Vec<usize> = (0..=b.len()).collect();
            for (i, ca) in a.iter().enumerate() {
                let mut cur = vec![i + 1];
                for (j, cb) in b.iter().enumerate() {
                    cur.push(
                        (prev[j] + usize::from(ca != cb))
                            .min(prev[j + 1] + 1)
                            .min(cur[j] + 1),
                    );
                }
                prev = cur;
            }
            prev[b.len()]
        };
        for a in &words {
            for b in &words {
                let distance = full(&chars(a), &chars(b));
                for k in 0..=2u8 {
                    let expected = (distance <= usize::from(k)).then_some(distance as u8);
                    assert_eq!(
                        bounded_edits(&chars(a), &chars(b), k),
                        expected,
                        "{a} {b} {k}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_query_is_its_distinct_analyzed_terms_in_utf8_order() {
        let mut analyzer = Analyzer::of(AnalyzerProfile::StandardV1).unwrap();
        let query = LexicalQuery::analyze(&mut analyzer, "Gamma beta GAMMA", false, 0).unwrap();
        assert_eq!(query.terms(), ["beta", "gamma"]);
        let mut folded = Analyzer::of(AnalyzerProfile::StandardFoldedV1).unwrap();
        let query = LexicalQuery::analyze(&mut folded, "Café", true, 1).unwrap();
        assert_eq!(query.terms(), ["cafe"]);
        let mut english = Analyzer::of(AnalyzerProfile::EnglishV1).unwrap();
        let query = LexicalQuery::analyze(&mut english, "running engines", true, 0).unwrap();
        assert_eq!(query.terms(), ["engin", "run"]);
    }

    /// NFC comes before tokenization: a decomposed `é` and a composed one
    /// are one term, and a combining mark is not a separator.
    #[test]
    fn nfc_comes_before_tokenization() {
        let mut analyzer = Analyzer::of(AnalyzerProfile::StandardV1).unwrap();
        let composed = LexicalQuery::analyze(&mut analyzer, "café", false, 0).unwrap();
        let decomposed = LexicalQuery::analyze(&mut analyzer, "cafe\u{301}", false, 0).unwrap();
        assert_eq!(composed.terms(), ["café"]);
        assert_eq!(decomposed.terms(), composed.terms());
    }

    #[test]
    fn the_query_and_value_bounds_are_typed_failures() {
        let mut analyzer = Analyzer::of(AnalyzerProfile::StandardV1).unwrap();
        let error = LexicalQuery::analyze(&mut analyzer, "beta", false, 3).unwrap_err();
        assert!(error.to_string().contains("T71"), "{error}");
        let error = LexicalQuery::analyze(&mut analyzer, "beta", false, -1).unwrap_err();
        assert!(error.to_string().contains("T71"), "{error}");
        let mut english = Analyzer::of(AnalyzerProfile::EnglishV1).unwrap();
        let error = LexicalQuery::analyze(&mut english, "the and of", false, 0).unwrap_err();
        assert!(error.to_string().contains("T20"), "{error}");
        let long = "a".repeat(QUERY_MAX_BYTES + 1);
        match LexicalQuery::analyze(&mut analyzer, &long, false, 0).unwrap_err() {
            OmniError::ResourceLimitExceeded {
                resource, limit, ..
            } => {
                assert_eq!(resource, "lexical_query_bytes");
                assert_eq!(limit, QUERY_MAX_BYTES as u64);
            }
            other => panic!("{other}"),
        }
        // NFC can expand UTF-8: a text under the bound whose NFC form is
        // over it is refused before the form is held whole.
        let expanding = "\u{FB2C}".repeat(QUERY_MAX_BYTES / 3);
        assert!(expanding.len() <= QUERY_MAX_BYTES);
        let expanded: String = expanding.nfc().collect();
        assert!(expanded.len() > QUERY_MAX_BYTES, "{}", expanded.len());
        match LexicalQuery::analyze(&mut analyzer, &expanding, false, 0).unwrap_err() {
            OmniError::ResourceLimitExceeded { resource, .. } => {
                assert_eq!(resource, "lexical_query_bytes")
            }
            other => panic!("{other}"),
        }
        let value = "a".repeat(VALUE_MAX_BYTES + 1);
        let error = analyzer
            .analyze(&value, VALUE_MAX_BYTES, "lexical_value_bytes", |_| Ok(()))
            .unwrap_err();
        match error {
            OmniError::ResourceLimitExceeded {
                resource, limit, ..
            } => {
                assert_eq!(resource, "lexical_value_bytes");
                assert_eq!(limit, VALUE_MAX_BYTES as u64);
            }
            other => panic!("{other}"),
        }
        let many: String = (0..=QUERY_MAX_TERMS).map(|i| format!("t{i} ")).collect();
        match LexicalQuery::analyze(&mut analyzer, &many, false, 0).unwrap_err() {
            OmniError::ResourceLimitExceeded { resource, .. } => {
                assert_eq!(resource, "lexical_query_terms")
            }
            other => panic!("{other}"),
        }
    }

    #[test]
    fn a_count_that_overflows_is_e1_and_a_score_that_is_not_finite_and_positive_is_e3() {
        let error = checked(u64::MAX, 1, "the field corpus size").unwrap_err();
        assert!(error.to_string().contains("E1"), "{error}");
        let corpus = Corpus {
            documents: 0,
            tokens: 0,
            holding: vec![0],
        };
        let error = corpus.score(&corpus.idf(), 1, &[vec![(0, 1)]]).unwrap_err();
        assert!(error.to_string().contains("E3"), "{error}");
    }
}
