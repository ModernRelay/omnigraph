//! Bounded scans of a pinned dataset for keys in one column or either of two columns.
use std::collections::HashSet;
use std::mem::size_of;

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, UInt64Array};
use datafusion::prelude::{Expr, col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance_table::format::Fragment;

use super::{ID_SCAN_BATCH_BYTES, ID_SCAN_BATCH_ROWS, TableStore};
use crate::error::{OmniError, Result};

/// Peak allowance for the UTF-8 payloads and Expr slots in the pushed IN lists,
/// including the copies in Lance's per-fragment read plans.
pub(crate) const MAX_LITERAL_BYTES: usize = 4 * 1024 * 1024;

/// The engine uses the same count ceiling that the planner costs per probe.
pub(crate) const PROBE_CHUNK_KEYS: usize = omnigraph_planner::PROBE_CHUNK_KEYS as usize;

pub(crate) fn string_col<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| OmniError::manifest(format!("batch missing column '{name}'")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("column '{name}' is not Utf8")))
}

type ColumnKeys<'a, K> = (&'a str, &'a [K]);

/// The two predicates are ORed. An empty key slice omits that predicate.
/// K can be owned or borrowed, so planning never needs to copy key strings.
pub(crate) struct KeyReads<'a, K> {
    columns: [ColumnKeys<'a, K>; 2],
    /// None pushes the filter to the whole dataset. Some([]) skips every probe.
    probed: Option<Vec<Fragment>>,
    matched: Vec<Fragment>,
    literal_copies: usize,
}

fn row_literal_bytes<K: AsRef<str>>(columns: &[ColumnKeys<'_, K>; 2], row: usize) -> usize {
    columns
        .iter()
        .filter_map(|(_, keys)| keys.get(row))
        .fold(0usize, |bytes, key| {
            bytes
                .saturating_add(size_of::<Expr>())
                .saturating_add(key.as_ref().len())
        })
}

impl<'a, K: AsRef<str>> KeyReads<'a, K> {
    pub(crate) async fn plan(
        ds: &Dataset,
        first: ColumnKeys<'a, K>,
        other: Option<ColumnKeys<'a, K>>,
    ) -> Result<Self> {
        let columns = [first, other.unwrap_or((first.0, &[]))];
        let rows = columns[0].1.len().max(columns[1].1.len());
        if rows == 0 || ds.fragments().is_empty() {
            return Ok(Self {
                columns,
                probed: Some(Vec::new()),
                matched: Vec::new(),
                literal_copies: 1,
            });
        }
        let mut bytes = 0usize;
        for row in 0..rows {
            let row_bytes = row_literal_bytes(&columns, row);
            // One wide key (or pair) cannot fit even in a single indexed probe.
            if row_bytes > MAX_LITERAL_BYTES {
                return Ok(Self {
                    columns,
                    probed: Some(Vec::new()),
                    matched: ds.fragments().to_vec(),
                    literal_copies: 1,
                });
            }
            bytes = bytes.saturating_add(row_bytes);
        }

        // A point lookup that fits even if every fragment needs its own filter
        // can skip the index metadata read entirely. In this case scratch is
        // reserved conservatively for the whole table's potential copies.
        let copies = |uncovered: usize| uncovered.saturating_mul(2).saturating_add(1);
        let fits = |uncovered| {
            rows.saturating_mul(uncovered) <= PROBE_CHUNK_KEYS
                && bytes.saturating_mul(copies(uncovered)) <= MAX_LITERAL_BYTES
        };
        let all = ds.fragments().len();
        if fits(all) {
            return Ok(Self {
                columns,
                probed: None,
                matched: Vec::new(),
                literal_copies: copies(all),
            });
        }
        let mut covered: Option<std::collections::BTreeSet<u64>> = None;
        for (column, keys) in columns {
            if !keys.is_empty() {
                let fragments =
                    crate::dataset_index::key_column_covered_fragments(ds, column).await?;
                covered = Some(match covered {
                    None => fragments,
                    Some(mut common) => {
                        common.retain(|id| fragments.contains(id));
                        common
                    }
                });
            }
        }
        let covered = covered.unwrap_or_default();
        let uncovered = all - covered.len();
        // FilteredReadExec clones the full filter into the fragment plan and
        // again into its scoped read. Exact BTREE matches omit both copies.
        if uncovered == 0 || fits(uncovered) {
            Ok(Self {
                columns,
                probed: None,
                matched: Vec::new(),
                literal_copies: copies(uncovered),
            })
        } else {
            let (indexed, tail) = ds
                .fragments()
                .iter()
                .cloned()
                .partition(|fragment| covered.contains(&fragment.id));
            Ok(Self {
                columns,
                probed: Some(indexed),
                matched: tail,
                literal_copies: 1,
            })
        }
    }

    /// One filter in flight. For an OR, an edge can match different endpoints
    /// in different chunks; multi-column consumers deduplicate by id or address.
    pub(crate) fn probes(&self) -> impl Iterator<Item = KeyProbe<'a, K>> + '_ {
        let mut start = 0;
        let rows = if self.probed.as_ref().is_some_and(Vec::is_empty) {
            0
        } else {
            self.columns[0].1.len().max(self.columns[1].1.len())
        };
        std::iter::from_fn(move || {
            if start == rows {
                return None;
            }
            let mut bytes = 0usize;
            let mut end = start;
            while end < rows && end - start < PROBE_CHUNK_KEYS {
                let next =
                    row_literal_bytes(&self.columns, end).saturating_mul(self.literal_copies);
                if next > MAX_LITERAL_BYTES - bytes {
                    break;
                }
                bytes += next;
                end += 1;
            }
            debug_assert!(start < end, "the plan sends oversized literals to the scan");
            let columns = self
                .columns
                .map(|(column, keys)| (column, &keys[start.min(keys.len())..end.min(keys.len())]));
            start = end;
            Some(KeyProbe { columns, bytes })
        })
    }

    /// None leaves the scanner on the whole pinned dataset.
    pub(crate) fn probe_fragments(&self) -> Option<Vec<Fragment>> {
        self.probed.clone()
    }

    /// Hash-table entries for borrowed keys, with a shared set for self edges.
    fn match_bytes(&self) -> usize {
        let [(_, first), (_, other)] = self.columns;
        let rows = if std::ptr::eq(first, other) {
            first.len()
        } else {
            first.len().saturating_add(other.len())
        };
        // Hash-table buckets, growth and control bytes, as in WorkMemory::entries.
        rows.saturating_mul((size_of::<&str>() + 1) * 4)
    }

    pub(crate) fn matched(&self) -> Option<(Vec<Fragment>, KeyMatch<'a>)> {
        if self.matched.is_empty() {
            return None;
        }
        let [(first_col, first_keys), (other_col, other_keys)] = self.columns;
        let set = |keys: &'a [K]| keys.iter().map(AsRef::as_ref).collect::<HashSet<&str>>();
        let first = (!first_keys.is_empty()).then(|| (first_col, set(first_keys)));
        let other = (!other_keys.is_empty()).then(|| {
            let own = (!std::ptr::eq(first_keys, other_keys)).then(|| set(other_keys));
            (other_col, own)
        });
        Some((self.matched.clone(), KeyMatch { first, other }))
    }
}

pub(crate) struct KeyProbe<'a, K> {
    columns: [ColumnKeys<'a, K>; 2],
    bytes: usize,
}

impl<K: AsRef<str>> KeyProbe<'_, K> {
    pub(crate) fn filter(self) -> Expr {
        self.columns
            .into_iter()
            .filter(|(_, keys)| !keys.is_empty())
            .map(|(column, keys)| {
                col(column).in_list(keys.iter().map(|key| lit(key.as_ref())).collect(), false)
            })
            .reduce(Expr::or)
            .expect("a probe has at least one key")
    }
}

/// The same OR membership as a probe, over a bounded projected batch.
pub(crate) struct KeyMatch<'a> {
    first: Option<(&'a str, HashSet<&'a str>)>,
    /// No set means both columns share the first column's set.
    other: Option<(&'a str, Option<HashSet<&'a str>>)>,
}

impl KeyMatch<'_> {
    pub(crate) fn columns(&self) -> impl Iterator<Item = &str> + '_ {
        self.first
            .iter()
            .map(|(column, _)| *column)
            .chain(self.other.iter().map(|(column, _)| *column))
    }

    pub(crate) fn keep(&self, batch: &RecordBatch, limit_one: bool) -> Result<RecordBatch> {
        let first = match &self.first {
            Some((column, keys)) => Some((string_col(batch, column)?, keys)),
            None => None,
        };
        let other = match &self.other {
            Some((column, keys)) => Some((
                string_col(batch, column)?,
                keys.as_ref()
                    .or(self.first.as_ref().map(|(_, keys)| keys))
                    .expect("only a shared set omits the other keys"),
            )),
            None => None,
        };
        let hit = |values: &StringArray, keys: &HashSet<&str>, row| {
            !values.is_null(row) && keys.contains(values.value(row))
        };
        let matches = |row| {
            first.is_some_and(|(v, keys)| hit(v, keys, row))
                || other.is_some_and(|(v, keys)| hit(v, keys, row))
        };
        if limit_one {
            let Some(row) = (0..batch.num_rows()).find(|&row| matches(row)) else {
                return Ok(RecordBatch::new_empty(batch.schema()));
            };
            return copy_row(batch, row);
        }
        let mask: BooleanArray = (0..batch.num_rows())
            .map(|row| Some(matches(row)))
            .collect();
        arrow_select::filter::filter_record_batch(batch, &mask).map_err(OmniError::arrow_internal)
    }
}

/// A slice would retain and charge all of the scanner's backing string buffers.
fn copy_row(batch: &RecordBatch, row: usize) -> Result<RecordBatch> {
    arrow_select::take::take_record_batch(batch, &UInt64Array::from(vec![row as u64]))
        .map_err(OmniError::arrow_internal)
}

/// Visit the matching rows of one pinned dataset. Only the projected
/// keys and result columns are read. Release each filter reservation before
/// starting another scan or building the tail's membership set.
pub(crate) async fn visit_key_matches<K: AsRef<str> + Sync>(
    ds: &Dataset,
    projection: &[&str],
    first: ColumnKeys<'_, K>,
    other: Option<ColumnKeys<'_, K>>,
    limit_one: bool,
    admit: &mut (impl FnMut(usize) -> Result<Box<dyn Send>> + ?Sized),
    mut visit: impl FnMut(&RecordBatch) -> Result<()>,
) -> Result<()> {
    let reads = KeyReads::plan(ds, first, other).await?;
    let probes = reads.probes().map(Some);
    let tail = (!reads.matched.is_empty()).then_some(None);
    for read in probes.chain(tail) {
        let bytes = read
            .as_ref()
            .map_or_else(|| reads.match_bytes(), |probe| probe.bytes);
        let _scratch = admit(bytes)?;
        let (filter, fragments, matcher) = match read {
            Some(probe) => (Some(probe.filter()), reads.probe_fragments(), None),
            None => {
                let (fragments, matcher) = reads.matched().expect("nonempty tail");
                (None, Some(fragments), Some(matcher))
            }
        };
        let mut scan_columns = projection.to_vec();
        if let Some(matcher) = &matcher {
            for key in matcher.columns() {
                if !scan_columns.contains(&key) {
                    scan_columns.push(key);
                }
            }
        }
        let mut stream =
            TableStore::scan_stream_with(ds, Some(&scan_columns), None, None, false, |scanner| {
                scanner.batch_size(ID_SCAN_BATCH_ROWS);
                scanner.batch_size_bytes(ID_SCAN_BATCH_BYTES);
                if limit_one {
                    scanner.batch_readahead(1);
                    if filter.is_some() {
                        scanner.limit(1)?;
                        scanner.batch_size(1);
                    }
                }
                if let Some(filter) = filter {
                    scanner.filter_expr(filter);
                }
                if let Some(fragments) = fragments {
                    scanner.with_fragments(fragments);
                }
                Ok(())
            })
            .await?;
        while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
            let kept = match &matcher {
                Some(matcher) => matcher.keep(&batch, limit_one)?,
                None if limit_one && batch.num_rows() > 0 => copy_row(&batch, 0)?,
                None => batch,
            };
            if kept.num_rows() > 0 {
                visit(&kept)?;
                if limit_one {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{KeyReads, MAX_LITERAL_BYTES, PROBE_CHUNK_KEYS};
    use datafusion::prelude::{Expr, col, lit};
    use datafusion::scalar::ScalarValue;

    /// The probe filters of a removal whose every fragment is probed.
    fn endpoint_probe_filters<'a>(
        src: (&'a str, &'a [String]),
        dst: (&'a str, &'a [String]),
    ) -> Vec<Expr> {
        KeyReads {
            columns: [src, dst],
            literal_copies: 1,
            probed: None,
            matched: Vec::new(),
        }
        .probes()
        .map(|p| p.filter())
        .collect()
    }

    /// A removal's edge probes carry at most one chunk of ids per endpoint in
    /// each filter, and together list every id once, in order; one chunk
    /// keeps the single `src IN … OR dst IN …` filter.
    #[test]
    fn removed_id_probes_hold_one_chunk_per_filter_issue_1442() {
        let src: Vec<String> = (0..2 * PROBE_CHUNK_KEYS + 1)
            .map(|i| format!("s{i:07}"))
            .collect();
        let dst: Vec<String> = (0..PROBE_CHUNK_KEYS + 3)
            .map(|i| format!("d{i:07}"))
            .collect();
        let mut shapes = Vec::new();
        let mut listed: Vec<(String, String)> = Vec::new();
        for filter in endpoint_probe_filters(("__src", &src), ("__dst", &dst)) {
            let mut lists = Vec::new();
            for test in or_terms(filter) {
                let Expr::InList(in_list) = test else {
                    panic!("an endpoint probe ORs IN lists: {test}");
                };
                assert!(!in_list.negated);
                let Expr::Column(column) = *in_list.expr else {
                    panic!("an endpoint probe tests a column");
                };
                lists.push((column.name.clone(), in_list.list.len()));
                for item in in_list.list {
                    let Expr::Literal(ScalarValue::Utf8(Some(id)), _) = item else {
                        panic!("an endpoint probe lists string literals: {item}");
                    };
                    listed.push((column.name.clone(), id));
                }
            }
            shapes.push(lists);
        }
        let full = PROBE_CHUNK_KEYS;
        assert_eq!(
            shapes,
            vec![
                vec![("__src".to_string(), full), ("__dst".to_string(), full)],
                vec![("__src".to_string(), full), ("__dst".to_string(), 3)],
                vec![("__src".to_string(), 1)],
            ]
        );
        let src_listed: Vec<&String> = listed
            .iter()
            .filter(|(c, _)| c == "__src")
            .map(|(_, id)| id)
            .collect();
        let dst_listed: Vec<&String> = listed
            .iter()
            .filter(|(c, _)| c == "__dst")
            .map(|(_, id)| id)
            .collect();
        assert_eq!(src_listed, src.iter().collect::<Vec<_>>());
        assert_eq!(dst_listed, dst.iter().collect::<Vec<_>>());

        let one = vec!["a".to_string()];
        let single: Vec<Expr> = endpoint_probe_filters(("__src", &one), ("__dst", &one));
        assert_eq!(
            single,
            vec![
                col("__src")
                    .in_list(vec![lit("a")], false)
                    .or(col("__dst").in_list(vec![lit("a")], false))
            ]
        );
        assert!(endpoint_probe_filters(("__src", &[]), ("__dst", &[])).is_empty());
        let none_probed = KeyReads {
            columns: [("__src", &one), ("__dst", &one)],
            literal_copies: 1,
            probed: Some(Vec::new()),
            matched: Vec::new(),
        };
        assert_eq!(none_probed.probes().count(), 0);
    }

    /// Indexed probes must split at the byte ceiling even when the keys
    /// all fit under the count ceiling. Both columns consume that allowance.
    #[test]
    fn wide_indexed_keys_split_at_the_literal_byte_ceiling_issue_1445() {
        let keys = vec!["k".repeat(MAX_LITERAL_BYTES / 8); 5];
        let reads = KeyReads {
            columns: [("__src", keys.as_slice()), ("__dst", keys.as_slice())],
            probed: None,
            matched: Vec::new(),
            literal_copies: 1,
        };
        let sizes: Vec<_> = reads
            .probes()
            .map(|probe| {
                assert!(probe.bytes <= MAX_LITERAL_BYTES);
                probe.columns.map(|(_, keys)| keys.len())
            })
            .collect();
        assert_eq!(sizes, [[3, 3], [2, 2]]);
    }

    /// On the fragments read whole, a row is kept when its src is a removed
    /// src id or its dst a removed dst id, null endpoints never; a self-edge
    /// removal tests both endpoints against its one id list.
    #[test]
    fn matched_fragments_keep_the_rows_a_probe_selects_issue_1442() {
        use arrow_array::{Array, RecordBatch, StringArray};
        use std::sync::Arc;
        let wide_nonmatch = "k".repeat(64 * 1024);
        let batch = RecordBatch::try_from_iter([
            (
                "__id",
                Arc::new(StringArray::from(vec!["e0", "e1", "e2", "e3", "e4"])) as _,
            ),
            (
                "__src",
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("k"),
                    Some(wide_nonmatch.as_str()),
                    None,
                    Some("b"),
                ])) as _,
            ),
            (
                "__dst",
                Arc::new(StringArray::from(vec![
                    Some("k"),
                    Some("b"),
                    Some("k"),
                    Some("a"),
                    Some("a"),
                ])) as _,
            ),
        ])
        .unwrap();
        let fragment = lance_table::format::Fragment::new(0);
        let kept = |src: &[String], dst: &[String]| -> Vec<String> {
            let reads = KeyReads {
                columns: [("__src", src), ("__dst", dst)],
                literal_copies: 1,
                probed: Some(Vec::new()),
                matched: vec![fragment.clone()],
            };
            let (fragments, matcher) = reads.matched().expect("a fragment is read whole");
            assert_eq!(fragments.len(), 1);
            let first = matcher.keep(&batch, true).unwrap();
            assert_eq!(first.num_rows(), 1);
            assert!(
                first.get_array_memory_size() < wide_nonmatch.len(),
                "the first hit still owns the batch's wide unrelated key"
            );
            let kept = matcher.keep(&batch, false).unwrap();
            let ids = kept
                .column_by_name("__id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            (0..ids.len()).map(|i| ids.value(i).to_string()).collect()
        };
        let a = vec!["a".to_string()];
        let b = vec!["b".to_string()];
        assert_eq!(kept(&a, &[]), vec!["e0"]);
        assert_eq!(kept(&[], &a), vec!["e3", "e4"]);
        assert_eq!(kept(&a, &b), vec!["e0", "e1"]);
        assert_eq!(kept(&a, &a), vec!["e0", "e3", "e4"]);
        let every = KeyReads {
            columns: [("__src", &a), ("__dst", &a)],
            literal_copies: 1,
            probed: None,
            matched: Vec::new(),
        };
        assert!(every.matched().is_none());
    }

    fn or_terms(expr: Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Or => {
                let mut terms = or_terms(*binary.left);
                terms.extend(or_terms(*binary.right));
                terms
            }
            other => vec![other],
        }
    }
}
