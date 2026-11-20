//! The exact lexical scan (the GQ 3 RFC, section 4, "The
//! lexical query", "bm25_v1" and "Exact execution and qualified
//! acceleration").
//!
//! Every full-text field has a profile (`english_v1` where its index names
//! none), and every lexical call is answered here, at every coverage state,
//! without reading the index: the analyzer is instantiated from the accepted
//! schema's profile whether or not an index exists, so a declared but unbuilt
//! index answers. The scan streams the field's table at the pinned snapshot
//! under the engine's scan tuning, normalizes each value to NFC, tokenizes it
//! with the profile's pipeline (Lance's own builder with the profile's
//! explicit parameters, so the tokens are the ones the profile's index would
//! hold; the query through Lance's search path, the documents through its
//! document path) and evaluates the `terms` relation: every (`all`) or some (`any`) distinct analyzed query
//! term has a stored term within the edit budget, the edit distance counting
//! one per inserted, deleted or substituted Unicode scalar value, so a
//! transposition costs two. A null or token-empty value never matches.
//!
//! `bm25` scores the matches with `bm25_v1` in `F64`: the statistics are the
//! field corpus, every entity of the table whose value holds at least one
//! analyzed term, before any graph or property filter; alternatives within a term's budget
//! share its group's IDF, the best of them weighted `1`, `1/2` or `1/4` by
//! its edits; parts add in ascending UTF-8 query-term order with no
//! reassociation; the IDF uses `libm`'s `log1p`; counts are checked `u64`.
//!
//! Query bytes, distinct query terms and value bytes are bounded before the
//! text is normalized (NFC can expand UTF-8) or tokenized, and every analyzed
//! token and term comparison is charged against the read's work budget; what
//! one value's analysis holds, its normalized text and its distinct terms, is
//! charged to the query's memory while it is held, and the results the scan
//! keeps for the operator's lifetime. Exhaustion is a typed failure, never a
//! partial answer. The rows read and tokens analyzed are counted on the
//! operator ([`super::lexical::SCAN_ROWS_METRIC`]).

use std::collections::{HashMap, HashSet};

use arrow_array::{Array, RecordBatch, StringArray};
use futures::StreamExt;
use omnigraph_compiler::catalog::{Catalog, NodeType};
use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::ir::IRExpr;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::ir::expr::{ScalarFunc, TermsMode};
use omnigraph_compiler::query::codes::{E1, E3, T71};
use omnigraph_compiler::query::diagnostic::QueryDiagnostic;
use omnigraph_compiler::schema::analysis::AnalyzerProfile;

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
/// It lives in `omnigraph-core` beside the measurement of what it emits, so
/// the measured fingerprint of the profile analyzer is this scan's own code.
pub(crate) use omnigraph_core::fts_analysis::ProfileAnalyzer as Analyzer;

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
    /// type error. A text that analyzes to no term, a query of only stop
    /// words, is a query that matches nothing ([`Self::matches_nothing`]),
    /// never a refusal: the text is usually a parameter its author never
    /// sees.
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
        analyzer.analyze(
            text,
            QUERY_MAX_BYTES,
            "lexical_query_bytes",
            true,
            |_| Ok(()),
            |token| {
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
            },
        )?;
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

    /// The text analyzed to no term: no document matches it, under either
    /// mode and any edit budget, so no field is read for it.
    pub(crate) fn matches_nothing(&self) -> bool {
        self.terms.is_empty()
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
    /// Whether the value matches: every query term (`all`) or one has an
    /// alternative. A query of no term matches nothing, though every one of
    /// its terms vacuously has one.
    fn matched(&self, all: bool) -> bool {
        if self.by_term.is_empty() {
            return false;
        }
        if all {
            self.by_term.iter().all(|hits| !hits.is_empty())
        } else {
            self.by_term.iter().any(|hits| !hits.is_empty())
        }
    }
}

/// The scan of one field: its table at the snapshot, its analyzer, the
/// memory and work it charges, and the rows and tokens it read.
struct FieldScan<'a> {
    analyzer: Analyzer,
    query: &'a LexicalQuery,
    memory: &'a WorkMemory,
    /// What one value's analysis holds while it is held, released after
    /// each row.
    document: WorkMemory,
    rows: u64,
    tokens: u64,
}

impl<'a> FieldScan<'a> {
    fn new(analyzer: Analyzer, query: &'a LexicalQuery, memory: &'a WorkMemory) -> Result<Self> {
        let document = memory
            .child("exact lexical scan value")
            .map_err(|error| memory.error(error))?;
        Ok(Self {
            analyzer,
            query,
            memory,
            document,
            rows: 0,
            tokens: 0,
        })
    }

    /// `text` analyzed: `None` when it holds no token. Its NFC copy and its
    /// distinct terms are charged to the query's memory until [`Self::row_done`].
    fn document(&mut self, text: &str) -> Result<Option<Document>> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        let mut len = 0u64;
        let memory = &self.document;
        self.analyzer.analyze(
            text,
            VALUE_MAX_BYTES,
            "lexical_value_bytes",
            false,
            |bytes| memory.string(bytes).map_err(|error| memory.error(error)),
            |token| {
                len = checked(len, 1, "a value's token count")?;
                match counts.get_mut(token) {
                    Some(count) => *count = checked(*count, 1, "a term's occurrences")?,
                    None => {
                        memory
                            .string(token.len())
                            .and_then(|()| memory.entries::<(String, u64)>(1))
                            .map_err(|error| memory.error(error))?;
                        counts.insert(token.to_string(), 1);
                    }
                }
                Ok(())
            },
        )?;
        self.tokens = self.tokens.saturating_add(len);
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

    /// Release what one row's analysis held, and count the row.
    fn row_done(&mut self) {
        self.rows = self.rows.saturating_add(1);
        self.document.shrink_to(0);
    }

    /// Count the rows read and the tokens analyzed on the operator.
    fn report(&self) {
        let usize_of = |count: u64| usize::try_from(count).unwrap_or(usize::MAX);
        self.memory
            .metric(super::lexical::SCAN_ROWS_METRIC, usize_of(self.rows));
        self.memory
            .metric(super::lexical::SCAN_TOKENS_METRIC, usize_of(self.tokens));
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
        let longest = document
            .terms
            .iter()
            .map(|(stored, _)| stored.len())
            .max()
            .unwrap_or(0);
        self.document
            .entries::<char>(longest)
            .map_err(|error| self.document.error(error))?;
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
    // The engine's scan tuning: the query's batch rows and bytes, bounded
    // read-ahead and I/O buffer, never Lance's defaults or the environment.
    let (rows, bytes) = (memory.batch_rows(), memory.batch_bytes());
    let plan = crate::table_store::TableStore::scan_plan_with(
        &dataset,
        Some(&[id_column, property]),
        None,
        false,
        |scanner| {
            scanner.batch_size(rows);
            scanner.batch_size_bytes(bytes as u64);
            scanner.pipelined();
            Ok(())
        },
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
    let mut ids = HashSet::new();
    if query.matches_nothing() {
        return Ok(ids);
    }
    let mut scan = FieldScan::new(field_analyzer(catalog, type_name, property)?, query, memory)?;
    scan_field(
        snapshot,
        catalog,
        type_name,
        property,
        memory,
        |id, value| {
            let matched = match value {
                Some(value) => match scan.document(value)? {
                    Some(document) => scan.hits(&document)?.matched(query.all),
                    None => false,
                },
                None => false,
            };
            scan.row_done();
            if matched {
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
    scan.report();
    Ok(ids)
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

/// What scoring a value by `bm25_v1` needs once its field corpus has been
/// read: the field's analyzer profile, the query, and the corpus statistics
/// with each query term's IDF. It holds nothing per document, so a
/// population of any size is scored in the memory one value's analysis
/// takes, row by row as the population streams.
pub(crate) struct Bm25Scorer {
    profile: AnalyzerProfile,
    query: LexicalQuery,
    corpus: Corpus,
    idf: Vec<f64>,
}

impl Bm25Scorer {
    /// Each of `values` scored, in order: its `bm25_v1` score, or `None` for
    /// a value outside the matches (a null, a value with no analyzed term,
    /// one that misses the query's mode). Each value's analysis is charged
    /// to `memory` while it is held and its tokens and comparisons to the
    /// query's work.
    pub(crate) fn score(
        &self,
        values: &StringArray,
        memory: &WorkMemory,
    ) -> Result<Vec<Option<f64>>> {
        if self.query.matches_nothing() {
            return Ok(vec![None; values.len()]);
        }
        let mut scan = FieldScan::new(Analyzer::of(self.profile)?, &self.query, memory)?;
        let mut out = Vec::with_capacity(values.len());
        for row in 0..values.len() {
            let document = match values.is_valid(row) {
                true => scan.document(values.value(row))?,
                false => None,
            };
            let score = match document {
                Some(document) => {
                    let hits = scan.hits(&document)?;
                    match hits.matched(self.query.all) {
                        true => Some(self.corpus.score(&self.idf, document.len, &hits.by_term)?),
                        false => None,
                    }
                }
                None => None,
            };
            scan.row_done();
            out.push(score);
        }
        scan.report();
        Ok(out)
    }
}

/// The `bm25_v1` corpus statistics of `type_name`'s `property` at `snapshot`
/// for `query`: one streamed pass over the field that counts the field
/// corpus, its tokens and each query term's document frequency, keeping no
/// document.
pub(crate) async fn bm25(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    property: &str,
    query: LexicalQuery,
    memory: &WorkMemory,
) -> Result<Bm25Scorer> {
    let profile = declared_profile(catalog, type_name, property).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "the exact lexical scan reads `{type_name}.{property}`, whose index declares no analyzer"
        ))
    })?;
    let mut corpus = Corpus {
        holding: vec![0; query.terms.len()],
        ..Corpus::default()
    };
    // No document matches a query of no term, so its statistics score
    // nothing and the corpus is not read.
    if query.matches_nothing() {
        return Ok(Bm25Scorer {
            profile,
            query,
            corpus,
            idf: Vec::new(),
        });
    }
    let mut scan = FieldScan::new(Analyzer::of(profile)?, &query, memory)?;
    scan_field(
        snapshot,
        catalog,
        type_name,
        property,
        memory,
        |_, value| {
            let document = match value {
                Some(value) => scan.document(value)?,
                None => None,
            };
            // A null value, or one with no analyzed term, is outside the
            // field corpus.
            let Some(document) = document else {
                scan.row_done();
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
            scan.row_done();
            Ok(())
        },
    )
    .await?;
    scan.report();
    drop(scan);
    let idf = corpus.idf();
    Ok(Bm25Scorer {
        profile,
        query,
        corpus,
        idf,
    })
}

#[cfg(test)]
mod tests {
    use unicode_normalization::UnicodeNormalization;

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

    /// The banded distance against the full dynamic-programming table, over
    /// generated term pairs (`arbitrary::search`): a term and the same term
    /// after zero to four random edits, transpositions among them, over
    /// ASCII, an accented letter, a sharp s, an ideograph and a digit.
    fn banded_distance_law(cases: u32) {
        use omnigraph_compiler::arbitrary::search::{edited, term};
        use omnigraph_compiler::arbitrary::{Choices, choices};
        use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestRng, TestRunner};
        let full = |a: &[char], b: &[char]| {
            let mut table = vec![vec![0usize; b.len() + 1]; a.len() + 1];
            for (i, row) in table.iter_mut().enumerate() {
                row[0] = i;
            }
            for (j, cell) in table[0].iter_mut().enumerate() {
                *cell = j;
            }
            for i in 1..=a.len() {
                for j in 1..=b.len() {
                    table[i][j] = (table[i - 1][j - 1] + usize::from(a[i - 1] != b[j - 1]))
                        .min(table[i - 1][j] + 1)
                        .min(table[i][j - 1] + 1);
                }
            }
            table[a.len()][b.len()]
        };
        let config = Config {
            cases,
            failure_persistence: None,
            ..Config::default()
        };
        let mut runner =
            TestRunner::new_with_rng(config, TestRng::deterministic_rng(RngAlgorithm::ChaCha));
        runner
            .run(&choices(32), |stream| {
                let mut c = Choices::new(&stream);
                let a = term(&mut c);
                let edits = c.below(5);
                let b = edited(&mut c, &a, edits);
                let (a, b) = (chars(&a), chars(&b));
                let distance = full(&a, &b);
                for k in 0..=2u8 {
                    let expected = (distance <= usize::from(k)).then_some(distance as u8);
                    let got = bounded_edits(&a, &b, k);
                    if got != expected {
                        return Err(TestCaseError::fail(format!(
                            "{a:?} {b:?} within {k}: {got:?}, the table gives {distance}"
                        )));
                    }
                }
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn the_banded_distance_equals_the_table_on_generated_terms() {
        banded_distance_law(20_000);
    }

    #[test]
    #[ignore = "nightly: the banded distance over generated terms at its nightly count"]
    fn the_banded_distance_equals_the_table_on_generated_terms_at_nightly_breadth() {
        banded_distance_law(1_000_000);
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

    /// The query is analyzed through Lance's search path and documents
    /// through its document path; under every profile the two give the same
    /// tokens, so a query term and the stored term it names agree.
    #[test]
    fn the_search_and_document_paths_analyze_alike_under_every_profile() {
        let texts = [
            "Running engines, résumés and resumes",
            "The only way: very human beings!",
            "Straße CAFÉ naïve 42 a1b2",
            "don't you've they're",
        ];
        for profile in AnalyzerProfile::ALL {
            let mut analyzer = Analyzer::of(profile).unwrap();
            for text in texts {
                let mut tokens = |query: bool| {
                    let mut out = Vec::new();
                    analyzer
                        .analyze(
                            text,
                            QUERY_MAX_BYTES,
                            "lexical_query_bytes",
                            query,
                            |_| Ok(()),
                            |token| {
                                out.push(token.to_string());
                                Ok(())
                            },
                        )
                        .unwrap();
                    out
                };
                let search = tokens(true);
                assert_eq!(search, tokens(false), "{profile}: {text}");
            }
        }
    }

    /// `english_v1` removes each stop word and its stem: `only` and `very`
    /// stem to `onli` and `veri`, which the list holds, so a query of them
    /// has no searchable term and matches nothing; a word whose stem is a
    /// stop word drops too, so "human beings" analyzes to `human`.
    #[test]
    fn english_v1_removes_stop_words_and_their_stems() {
        let mut english = Analyzer::of(AnalyzerProfile::EnglishV1).unwrap();
        for text in ["only", "very", "only very because", "why during any"] {
            for (all, edits) in [(false, 0), (true, 0), (false, 2), (true, 1)] {
                let query = LexicalQuery::analyze(&mut english, text, all, edits).unwrap();
                assert!(query.matches_nothing(), "{text}");
            }
        }
        // Every term vacuously has an alternative under `all`; a query of
        // none still matches nothing.
        let none = Hits { by_term: vec![] };
        assert!(!none.matched(true) && !none.matched(false));
        let query = LexicalQuery::analyze(&mut english, "human beings", false, 0).unwrap();
        assert_eq!(query.terms(), ["human"]);
        // The standard profiles remove no stop word.
        let mut standard = Analyzer::of(AnalyzerProfile::StandardV1).unwrap();
        let query = LexicalQuery::analyze(&mut standard, "only very", false, 0).unwrap();
        assert_eq!(query.terms(), ["only", "very"]);
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
            .analyze(
                &value,
                VALUE_MAX_BYTES,
                "lexical_value_bytes",
                false,
                |_| Ok(()),
                |_| Ok(()),
            )
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
