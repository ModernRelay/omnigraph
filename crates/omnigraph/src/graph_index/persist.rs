//! Persisted adjacency artifact: the in-memory CSR/CSC graph index serialized
//! to the object store, so a cold process loads topology with one GET instead
//! of scanning every edge table.
//!
//! Ownership contract: **only `optimize` writes the artifact; the query path
//! only loads.** Reads stay read-only toward the store, and a store that has
//! never been optimized simply misses and builds in memory as before.
//!
//! Format (binary; chosen over a JSON+base64 encoding by measurement at 100M
//! edges — decode+verify 1.17 s → 0.61 s, 21% smaller, encode 3× faster):
//!
//! ```text
//! [8B magic "OGCSRIDX"] [u32 LE format version] [u64 LE header length]
//! [header: JSON (stamps, payload digest, section lengths)]
//! [payload: dictionaries + adjacency arrays, verbatim little-endian]
//! ```
//!
//! The payload is the in-memory representation dumped as-is: each `u32` array
//! is its bytes (a memcpy out, one memcpy back in); only the id dictionaries
//! need framing (strings are variable-length: per id, a u32 length prefix +
//! UTF-8 bytes). Section order is fixed by the header's metadata order: every
//! dictionary, then per edge type its csr-offsets / csr-targets / csc-offsets
//! / csc-targets.
//!
//! One artifact per store, at a fixed name, covering every edge type in the
//! catalog at persist time. A query asks for a SCOPED edge set (only the
//! types it traverses — the in-memory cache is keyed the same way), so the
//! loader verifies freshness PER REQUESTED EDGE against embedded identity
//! stamps: each stamp carries the same physical-identity fields as
//! `runtime_cache::GraphIndexCacheKey` (`TableIdentity`, published dataset
//! version, native branch, Lance manifest e_tag, catalog endpoints), and
//! every requested edge must match its stamp exactly. A superset artifact
//! serves any scoped subset — extra edges and extra dictionary nodes are
//! semantically inert, since executors look adjacency up by edge name and
//! dense ids are internal to one index. An edge write changes that table's
//! stamp, so only queries touching the written edge miss; the rest keep
//! loading. Lazy-fork branches share main's physical identity and therefore
//! its artifact, mirroring the in-memory key's design.
//!
//! Failure contract: loading is fail-open. A missing, truncated, corrupt,
//! digest- or stamp-mismatched artifact logs and falls back to the in-memory
//! build; it can slow a query, never wrong it. The sha256 over the payload is
//! load-bearing for exactly the class binary makes dangerous: raw arrays have
//! no syntax, so a flipped byte parses fine and becomes silently wrong
//! topology — the digest is the end-to-end check that converts that into a
//! rejected load (~0.3 s/GiB, measured; the price of never returning wrong
//! rows from a damaged artifact).

use std::collections::HashMap;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::db::Snapshot;
use crate::error::{OmniError, Result};
use crate::storage::{StorageAdapter, storage_for_uri};

use super::{CsrIndex, GraphIndex, TypeIndex};

const MAGIC: &[u8; 8] = b"OGCSRIDX";

/// Bump when the layout changes; a version mismatch is a miss (the loader
/// rejects and rebuilds in memory, and the next optimize rewrites current).
const FORMAT_VERSION: u32 = 1;

/// Ceiling on the artifact body accepted at load. Guards the fail-open read
/// path against a corrupt object materializing unbounded memory before parse.
/// 4 GiB covers ~400M edges with a generous dictionary.
const MAX_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Sanity ceiling on the embedded header (stamps + section metadata — small
/// even for hundreds of edge types).
const MAX_HEADER_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct Header {
    /// One stamp per covered edge type: the freshness authority.
    tables: Vec<TableStamp>,
    /// sha256 (base64) over the payload bytes; see the module contract.
    payload_sha256_b64: String,
    /// Dictionary sections, in payload order.
    dicts: Vec<DictMeta>,
    /// Adjacency sections, in payload order after the dictionaries.
    adjacencies: Vec<AdjMeta>,
}

#[derive(Serialize, Deserialize)]
struct DictMeta {
    type_name: String,
    /// Number of ids (dense slots).
    count: u64,
    /// Section length in bytes.
    len: u64,
}

#[derive(Serialize, Deserialize)]
struct AdjMeta {
    edge_name: String,
    /// Section lengths in bytes, in payload order:
    /// csr_offsets, csr_targets, csc_offsets, csc_targets.
    lens: [u64; 4],
}

/// The physical identity of one edge table at persist time. Field set mirrors
/// `runtime_cache::GraphIndexCacheKey`; the two must stay in lockstep or the
/// artifact outlives the in-memory key's invalidation rules.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct TableStamp {
    edge_name: String,
    /// `TableIdentity` rendered via its stable `Display` (`stable:incarnation`).
    identity: String,
    table_version: u64,
    table_branch: Option<String>,
    e_tag: Option<String>,
    from_type: String,
    to_type: String,
}

/// The artifact's fixed object URI under a store root.
pub(crate) fn artifact_uri(root_uri: &str) -> String {
    format!(
        "{}/__graph_index/csr-current.bin",
        root_uri.trim_end_matches('/')
    )
}

/// The stamp the CURRENT snapshot implies for one edge type; `None` when the
/// edge table is absent (not yet materialized).
fn snapshot_stamp(
    snapshot: &Snapshot,
    edge_name: &str,
    from_type: &str,
    to_type: &str,
) -> Option<TableStamp> {
    let entry = snapshot.dataset(&format!("edge:{}", edge_name))?;
    Some(TableStamp {
        edge_name: edge_name.to_string(),
        identity: entry.identity.to_string(),
        table_version: entry.published_dataset_version,
        table_branch: entry.native_dataset_branch.clone(),
        e_tag: entry.version_metadata.e_tag().map(str::to_string),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    })
}

fn push_u32s(out: &mut Vec<u8>, values: &[u32]) {
    out.reserve(values.len() * 4);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn parse_u32s(bytes: &[u8]) -> Result<Vec<u32>> {
    if !bytes.len().is_multiple_of(4) {
        return Err(OmniError::manifest(
            "graph index artifact u32 section has a truncated tail".to_string(),
        ));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

/// Bounds-checked payload slicing: every section read advances a cursor and
/// must stay inside the payload, so a lying header fails loudly instead of
/// panicking or reading a neighbor section.
fn take<'a>(payload: &'a [u8], cursor: &mut usize, len: u64) -> Result<&'a [u8]> {
    let len = usize::try_from(len)
        .map_err(|_| OmniError::manifest("graph index artifact section too large".to_string()))?;
    let start = *cursor;
    let end = start
        .checked_add(len)
        .filter(|&e| e <= payload.len())
        .ok_or_else(|| {
            OmniError::manifest("graph index artifact section exceeds the payload".to_string())
        })?;
    *cursor = end;
    Ok(&payload[start..end])
}

/// Serialize an index with the given per-edge stamps to the artifact bytes.
fn serialize(index: &GraphIndex, tables: Vec<TableStamp>) -> Result<Vec<u8>> {
    let (types, csr, csc) = index.parts();

    // Deterministic section order: sorted names.
    let mut type_names: Vec<&String> = types.keys().collect();
    type_names.sort();
    let mut edge_names: Vec<&String> = csr.keys().collect();
    edge_names.sort();

    let mut payload: Vec<u8> = Vec::new();
    let mut dicts = Vec::with_capacity(type_names.len());
    for name in &type_names {
        let ids = types[*name].ids();
        let before = payload.len();
        for id in ids {
            payload.extend_from_slice(&(id.len() as u32).to_le_bytes());
            payload.extend_from_slice(id.as_bytes());
        }
        dicts.push(DictMeta {
            type_name: (*name).clone(),
            count: ids.len() as u64,
            len: (payload.len() - before) as u64,
        });
    }
    let mut adjacencies = Vec::with_capacity(edge_names.len());
    for name in &edge_names {
        let csr_adj = &csr[*name];
        let csc_adj = csc.get(*name).ok_or_else(|| {
            OmniError::manifest(format!("graph index misses csc for edge '{name}'"))
        })?;
        let mut lens = [0u64; 4];
        for (slot, section) in [
            (0, csr_adj.offsets()),
            (1, csr_adj.targets()),
            (2, csc_adj.offsets()),
            (3, csc_adj.targets()),
        ] {
            let before = payload.len();
            push_u32s(&mut payload, section);
            lens[slot] = (payload.len() - before) as u64;
        }
        adjacencies.push(AdjMeta {
            edge_name: (*name).clone(),
            lens,
        });
    }

    let digest = base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(&payload));
    let header = serde_json::to_vec(&Header {
        tables,
        payload_sha256_b64: digest,
        dicts,
        adjacencies,
    })
    .map_err(|e| OmniError::manifest(format!("graph index artifact header serialize: {e}")))?;

    let mut out = Vec::with_capacity(MAGIC.len() + 4 + 8 + header.len() + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&(header.len() as u64).to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Parse the artifact, verify the payload digest, verify every REQUESTED edge
/// against its embedded stamp and the snapshot's current identity, and
/// rebuild the typed index. Structural invariants of the arrays are
/// re-validated in the `from_parts` constructors before the index is trusted.
fn deserialize_verified(
    bytes: &[u8],
    snapshot: &Snapshot,
    edge_types: &HashMap<String, (String, String)>,
) -> Result<GraphIndex> {
    let fixed = MAGIC.len() + 4 + 8;
    if bytes.len() < fixed || &bytes[..MAGIC.len()] != MAGIC {
        return Err(OmniError::manifest(
            "graph index artifact magic mismatch".to_string(),
        ));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().expect("fixed slice"));
    if version != FORMAT_VERSION {
        return Err(OmniError::manifest(format!(
            "graph index artifact format {version} != supported {FORMAT_VERSION}"
        )));
    }
    let header_len = u64::from_le_bytes(bytes[12..20].try_into().expect("fixed slice"));
    if header_len > MAX_HEADER_BYTES {
        return Err(OmniError::manifest(
            "graph index artifact header exceeds its ceiling".to_string(),
        ));
    }
    let header_len = header_len as usize;
    let header_end = fixed
        .checked_add(header_len)
        .filter(|&e| e <= bytes.len())
        .ok_or_else(|| {
            OmniError::manifest("graph index artifact header exceeds the object".to_string())
        })?;
    let header: Header = serde_json::from_slice(&bytes[fixed..header_end])
        .map_err(|e| OmniError::manifest(format!("graph index artifact header parse: {e}")))?;
    let payload = &bytes[header_end..];

    // Freshness first (cheap), digest second (one pass over the payload),
    // structure last (allocates).
    for (edge_name, (from_type, to_type)) in edge_types {
        let expected =
            snapshot_stamp(snapshot, edge_name, from_type, to_type).ok_or_else(|| {
                OmniError::manifest(format!(
                    "graph index artifact requested for absent edge table '{edge_name}'"
                ))
            })?;
        let stamped = header
            .tables
            .iter()
            .find(|t| t.edge_name == *edge_name)
            .ok_or_else(|| {
                OmniError::manifest(format!(
                    "graph index artifact does not cover edge '{edge_name}'"
                ))
            })?;
        if *stamped != expected {
            return Err(OmniError::manifest(format!(
                "graph index artifact is stale for edge '{edge_name}'"
            )));
        }
    }
    let digest = base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(payload));
    if digest != header.payload_sha256_b64 {
        return Err(OmniError::manifest(
            "graph index artifact payload digest mismatch".to_string(),
        ));
    }

    let index = decode_body(&header, payload)?;
    verify_stamped_adjacencies(&header, &index)?;
    Ok(index)
}

/// A stamp is a freshness claim, not proof the adjacency was serialized. An
/// artifact that stamps a requested edge but carries no adjacency for it
/// would otherwise decode into an index that ERRORS at traversal dispatch
/// ("no adjacency index for edge …") instead of falling back to the scan
/// build — and the persist gate would never repair it, since the object
/// exists. Require the decoded maps to cover every stamped edge (a superset
/// of the requested set, which the freshness loop has already matched against
/// stamps), so stamp set == adjacency set exactly as the writer produces.
fn verify_stamped_adjacencies(header: &Header, index: &GraphIndex) -> Result<()> {
    for stamp in &header.tables {
        if index.csr(&stamp.edge_name).is_none() || index.csc(&stamp.edge_name).is_none() {
            return Err(OmniError::manifest(format!(
                "graph index artifact stamps edge '{}' without its adjacency",
                stamp.edge_name
            )));
        }
    }
    Ok(())
}

/// Walk the payload sections into the typed index, enforcing every invariant
/// the executors rely on. The digest only proves the payload is what the
/// writer wrote — it says nothing about internal consistency, and raw arrays
/// have no syntax to fail on, so an inconsistent writer (or a crafted object)
/// must be caught HERE by cross-checks or it becomes a panic or a silent
/// wrong answer at query time:
///
/// - allocation hints are clamped by the section's actual size, so a lying
///   `count` cannot drive an unbounded allocation before its mismatch check;
/// - each adjacency's offsets row count must equal its endpoint dictionary's
///   size + 1 (`CsrIndex::neighbors` indexes `offsets[node + 1]` unchecked
///   for any dense id the dictionary can produce);
/// - each adjacency's targets must all fall inside the OPPOSITE endpoint's
///   dictionary (a target enters the frontier and is itself looked up on the
///   next hop). Endpoints come from the artifact's own stamps, which the
///   caller has already matched against the live snapshot for requested edges.
fn decode_body(header: &Header, payload: &[u8]) -> Result<GraphIndex> {
    let mut cursor = 0usize;
    let mut type_indices: HashMap<String, TypeIndex> = HashMap::with_capacity(header.dicts.len());
    for dict in &header.dicts {
        let section = take(payload, &mut cursor, dict.len)?;
        // Every id costs at least its 4-byte length prefix, so the section
        // bounds the possible id count regardless of what `count` claims.
        let capacity_ceiling = section.len() / 4;
        let mut ids = Vec::with_capacity(
            usize::try_from(dict.count)
                .unwrap_or(0)
                .min(capacity_ceiling),
        );
        let mut d = 0usize;
        while d < section.len() {
            let end = d
                .checked_add(4)
                .filter(|&e| e <= section.len())
                .ok_or_else(|| {
                    OmniError::manifest("graph index artifact dictionary truncated".to_string())
                })?;
            let len = u32::from_le_bytes(section[d..end].try_into().expect("fixed slice")) as usize;
            d = end;
            let id_end = d
                .checked_add(len)
                .filter(|&e| e <= section.len())
                .ok_or_else(|| {
                    OmniError::manifest("graph index artifact dictionary truncated".to_string())
                })?;
            let id = std::str::from_utf8(&section[d..id_end]).map_err(|_| {
                OmniError::manifest("graph index artifact dictionary id is not UTF-8".to_string())
            })?;
            ids.push(id.to_string());
            d = id_end;
        }
        if ids.len() as u64 != dict.count {
            return Err(OmniError::manifest(format!(
                "graph index artifact dictionary '{}' count mismatch",
                dict.type_name
            )));
        }
        type_indices.insert(dict.type_name.clone(), TypeIndex::from_ids(ids)?);
    }

    let dict_len = |type_name: &str| -> Result<usize> {
        type_indices
            .get(type_name)
            .map(TypeIndex::len)
            .ok_or_else(|| {
                OmniError::manifest(format!(
                    "graph index artifact misses the dictionary for type '{type_name}'"
                ))
            })
    };
    let check_adjacency = |adj_name: &str,
                           orientation: &str,
                           index: &CsrIndex,
                           keyed: usize,
                           opposite: usize|
     -> Result<()> {
        if index.offsets().len() != keyed + 1 {
            return Err(OmniError::manifest(format!(
                "graph index artifact {orientation} offsets for edge '{adj_name}' do not \
                     cover its keyed dictionary"
            )));
        }
        // `t >= opposite` also covers an empty opposite dictionary: any
        // target at all is then out of range.
        if index.targets().iter().any(|&t| (t as usize) >= opposite) {
            return Err(OmniError::manifest(format!(
                "graph index artifact {orientation} targets for edge '{adj_name}' exceed \
                     the opposite dictionary"
            )));
        }
        Ok(())
    };

    let mut csr = HashMap::with_capacity(header.adjacencies.len());
    let mut csc = HashMap::with_capacity(header.adjacencies.len());
    for adj in &header.adjacencies {
        let stamp = header
            .tables
            .iter()
            .find(|t| t.edge_name == adj.edge_name)
            .ok_or_else(|| {
                OmniError::manifest(format!(
                    "graph index artifact adjacency '{}' has no identity stamp",
                    adj.edge_name
                ))
            })?;
        let from_len = dict_len(&stamp.from_type)?;
        let to_len = dict_len(&stamp.to_type)?;

        let csr_offsets = parse_u32s(take(payload, &mut cursor, adj.lens[0])?)?;
        let csr_targets = parse_u32s(take(payload, &mut cursor, adj.lens[1])?)?;
        let csc_offsets = parse_u32s(take(payload, &mut cursor, adj.lens[2])?)?;
        let csc_targets = parse_u32s(take(payload, &mut cursor, adj.lens[3])?)?;
        let csr_index = CsrIndex::from_parts(csr_offsets, csr_targets)?;
        let csc_index = CsrIndex::from_parts(csc_offsets, csc_targets)?;
        check_adjacency(&adj.edge_name, "csr", &csr_index, from_len, to_len)?;
        check_adjacency(&adj.edge_name, "csc", &csc_index, to_len, from_len)?;
        csr.insert(adj.edge_name.clone(), csr_index);
        csc.insert(adj.edge_name.clone(), csc_index);
    }
    if cursor != payload.len() {
        return Err(OmniError::manifest(
            "graph index artifact carries unaccounted trailing bytes".to_string(),
        ));
    }
    GraphIndex::from_parts(type_indices, csr, csc)
}

/// Try to load a persisted index fresh for every edge in `edge_types` under
/// this snapshot. `None` on any miss — absent artifact, stale or missing
/// stamp, digest / parse / validation failure (logged) — so callers always
/// have the in-memory build as the fallback.
pub(crate) async fn load(
    snapshot: &Snapshot,
    edge_types: &HashMap<String, (String, String)>,
    adapter: Option<&dyn StorageAdapter>,
) -> Option<GraphIndex> {
    let root = snapshot.root_uri();
    let uri = artifact_uri(root);
    // Prefer the caller's adapter (the db's own, possibly instrumented or
    // injected — a fallback-constructed adapter would bypass IO counting and
    // any injected configuration); derive one from the root URI only when no
    // owning adapter exists (the Direct builder).
    let fallback;
    let adapter: &dyn StorageAdapter = match adapter {
        Some(a) => a,
        None => match storage_for_uri(root) {
            Ok(a) => {
                fallback = a;
                fallback.as_ref()
            }
            Err(e) => {
                tracing::debug!(target: "omnigraph::graph_index", error = %e,
                    "graph index artifact store unavailable; building in memory");
                return None;
            }
        },
    };
    let bytes = match adapter
        .read_bytes_if_exists_bounded(&uri, MAX_ARTIFACT_BYTES)
        .await
    {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(target: "omnigraph::graph_index", error = %e, uri = %uri,
                "graph index artifact read failed; building in memory");
            return None;
        }
    };
    match deserialize_verified(&bytes, snapshot, edge_types) {
        Ok(index) => {
            tracing::debug!(target: "omnigraph::graph_index", uri = %uri,
                "graph index loaded from persisted artifact");
            Some(index)
        }
        Err(e) => {
            tracing::debug!(target: "omnigraph::graph_index", error = %e, uri = %uri,
                "graph index artifact rejected; building in memory");
            None
        }
    }
}

/// Persist `index` (covering `edge_types`) for this snapshot. Called by
/// `optimize` only (see the module contract). `Ok(None)` when any covered
/// edge table is absent from the snapshot — a partially-materialized store
/// has no complete identity to stamp. Errors are the caller's to log; a
/// failed write costs nothing but the next cold build.
pub(crate) async fn save(
    snapshot: &Snapshot,
    adapter: &dyn StorageAdapter,
    edge_types: &HashMap<String, (String, String)>,
    index: &GraphIndex,
) -> Result<Option<String>> {
    let mut tables = Vec::with_capacity(edge_types.len());
    for (edge_name, (from_type, to_type)) in edge_types {
        match snapshot_stamp(snapshot, edge_name, from_type, to_type) {
            Some(stamp) => tables.push(stamp),
            None => return Ok(None),
        }
    }
    tables.sort_by(|a, b| a.edge_name.cmp(&b.edge_name));
    let uri = artifact_uri(snapshot.root_uri());
    let body = serialize(index, tables)?;
    adapter.write_bytes(&uri, &body).await?;
    Ok(Some(uri))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u32_codec_round_trips() {
        let values = vec![0u32, 1, 42, u32::MAX, 7];
        let mut bytes = Vec::new();
        push_u32s(&mut bytes, &values);
        assert_eq!(parse_u32s(&bytes).unwrap(), values);
        assert_eq!(parse_u32s(&[]).unwrap(), Vec::<u32>::new());
    }

    #[test]
    fn u32_codec_rejects_truncated_tail() {
        assert!(parse_u32s(&[1u8, 2, 3]).is_err());
    }

    fn sample_index() -> GraphIndex {
        let mut types = HashMap::new();
        types.insert(
            "Person".to_string(),
            TypeIndex::from_ids(vec!["a".into(), "b".into(), "c".into()]).unwrap(),
        );
        let mut csr = HashMap::new();
        csr.insert(
            "Knows".to_string(),
            CsrIndex::build(3, &[(0, 1), (1, 2), (0, 2)]),
        );
        let mut csc = HashMap::new();
        csc.insert(
            "Knows".to_string(),
            CsrIndex::build(3, &[(1, 0), (2, 1), (2, 0)]),
        );
        GraphIndex::from_parts(types, csr, csc).unwrap()
    }

    fn sample_stamp() -> TableStamp {
        TableStamp {
            edge_name: "Knows".to_string(),
            identity: "0000000000000001:0000000000000002".to_string(),
            table_version: 7,
            table_branch: None,
            e_tag: Some("etag-1".to_string()),
            from_type: "Person".to_string(),
            to_type: "Person".to_string(),
        }
    }

    /// Header/payload halves round-trip. Stamp verification against a live
    /// snapshot rides the integration tests in `tests/traversal_adaptive.rs`.
    #[test]
    fn artifact_round_trips_binary() {
        let index = sample_index();
        let bytes = serialize(&index, vec![sample_stamp()]).unwrap();
        assert_eq!(&bytes[..8], MAGIC);

        let fixed = MAGIC.len() + 4 + 8;
        let header_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize;
        let header: Header = serde_json::from_slice(&bytes[fixed..fixed + header_len]).unwrap();
        let payload = &bytes[fixed + header_len..];
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(payload)),
            header.payload_sha256_b64,
        );

        // Rebuild through the section walk (bypassing the snapshot-stamp
        // check, which needs a live snapshot).
        let mut cursor = 0usize;
        let dict = &header.dicts[0];
        let _ = take(payload, &mut cursor, dict.len).unwrap();
        let adj = &header.adjacencies[0];
        let offs = parse_u32s(take(payload, &mut cursor, adj.lens[0]).unwrap()).unwrap();
        let tgts = parse_u32s(take(payload, &mut cursor, adj.lens[1]).unwrap()).unwrap();
        let rebuilt = CsrIndex::from_parts(offs, tgts).unwrap();
        let mut n: Vec<u32> = rebuilt.neighbors(0).to_vec();
        n.sort_unstable();
        assert_eq!(n, vec![1, 2]);
    }

    #[test]
    fn corrupted_payload_changes_the_digest() {
        let index = sample_index();
        let mut bytes = serialize(&index, vec![sample_stamp()]).unwrap();
        let fixed = MAGIC.len() + 4 + 8;
        let header_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize;
        let header: Header = serde_json::from_slice(&bytes[fixed..fixed + header_len]).unwrap();
        // Flip one byte in the payload — a "valid" u32 either way; only the
        // digest can catch it.
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        let payload = &bytes[fixed + header_len..];
        assert_ne!(
            base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(payload)),
            header.payload_sha256_b64,
        );
    }

    #[test]
    fn take_rejects_out_of_bounds_sections() {
        let payload = [0u8; 8];
        let mut cursor = 0usize;
        assert!(take(&payload, &mut cursor, 8).is_ok());
        assert!(take(&payload, &mut cursor, 1).is_err());
        let mut cursor = 0usize;
        assert!(take(&payload, &mut cursor, 9).is_err());
    }

    /// Split serialized bytes into (header, payload) for direct decode_body
    /// attacks (bypassing the digest/stamp stages, which have their own tests).
    fn split(bytes: &[u8]) -> (Header, Vec<u8>) {
        let fixed = MAGIC.len() + 4 + 8;
        let header_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize;
        let header: Header = serde_json::from_slice(&bytes[fixed..fixed + header_len]).unwrap();
        (header, bytes[fixed + header_len..].to_vec())
    }

    // A lying header `count` must not drive the allocation — the clamp bounds
    // it by the section size and the mismatch check rejects the artifact.
    #[test]
    fn crafted_dictionary_count_is_rejected_without_allocating() {
        let bytes = serialize(&sample_index(), vec![sample_stamp()]).unwrap();
        let (mut header, payload) = split(&bytes);
        header.dicts[0].count = 1 << 40;
        let err = decode_body(&header, &payload).unwrap_err();
        assert!(err.to_string().contains("count mismatch"), "{err}");
    }

    // Offsets must cover the keyed dictionary, or a valid dense seed id
    // panics inside `neighbors` at query time.
    #[test]
    fn crafted_short_offsets_are_rejected() {
        let mut types = HashMap::new();
        types.insert(
            "Person".to_string(),
            TypeIndex::from_ids(vec!["a".into(), "b".into(), "c".into()]).unwrap(),
        );
        // Adjacency built for TWO nodes while the dictionary has three.
        let mut csr = HashMap::new();
        csr.insert("Knows".to_string(), CsrIndex::build(2, &[(0, 1)]));
        let mut csc = HashMap::new();
        csc.insert("Knows".to_string(), CsrIndex::build(2, &[(1, 0)]));
        let index = GraphIndex::from_parts(types, csr, csc).unwrap();
        let bytes = serialize(&index, vec![sample_stamp()]).unwrap();
        let (header, payload) = split(&bytes);
        let err = decode_body(&header, &payload).unwrap_err();
        assert!(err.to_string().contains("do not cover"), "{err}");
    }

    // A target outside the opposite dictionary enters the frontier and panics
    // on the next hop's `neighbors` call.
    #[test]
    fn crafted_out_of_range_target_is_rejected() {
        let mut types = HashMap::new();
        types.insert(
            "Person".to_string(),
            TypeIndex::from_ids(vec!["a".into(), "b".into(), "c".into()]).unwrap(),
        );
        let mut csr = HashMap::new();
        // Target 7 has no dictionary slot.
        csr.insert("Knows".to_string(), CsrIndex::build(3, &[(0, 7)]));
        let mut csc = HashMap::new();
        csc.insert("Knows".to_string(), CsrIndex::build(3, &[(1, 0)]));
        let index = GraphIndex::from_parts(types, csr, csc).unwrap();
        let bytes = serialize(&index, vec![sample_stamp()]).unwrap();
        let (header, payload) = split(&bytes);
        let err = decode_body(&header, &payload).unwrap_err();
        assert!(err.to_string().contains("exceed"), "{err}");
    }

    // A stamp without its adjacency must be rejected at load, or traversal
    // dispatch errors ("no adjacency index") instead of falling back to the
    // scan build.
    #[test]
    fn stamp_without_an_adjacency_is_rejected() {
        // An index carrying only dictionaries — no adjacency for the stamped
        // edge (the class a writer bug or crafted object would produce).
        let mut types = HashMap::new();
        types.insert(
            "Person".to_string(),
            TypeIndex::from_ids(vec!["a".into()]).unwrap(),
        );
        let empty = GraphIndex::from_parts(types, HashMap::new(), HashMap::new()).unwrap();
        let bytes = serialize(&empty, vec![sample_stamp()]).unwrap();
        let (header, payload) = split(&bytes);
        let index = decode_body(&header, &payload).unwrap();
        let err = verify_stamped_adjacencies(&header, &index).unwrap_err();
        assert!(err.to_string().contains("without its adjacency"), "{err}");
    }

    // An adjacency whose edge has no identity stamp is undecodable — the
    // stamps carry the endpoint types the cross-checks need.
    #[test]
    fn adjacency_without_a_stamp_is_rejected() {
        let bytes = serialize(&sample_index(), vec![sample_stamp()]).unwrap();
        let (mut header, payload) = split(&bytes);
        header.tables.clear();
        let err = decode_body(&header, &payload).unwrap_err();
        assert!(err.to_string().contains("identity stamp"), "{err}");
    }

    #[test]
    fn duplicate_dictionary_ids_are_refused() {
        assert!(TypeIndex::from_ids(vec!["a".into(), "a".into()]).is_err());
    }

    #[test]
    fn invalid_adjacency_structure_is_refused() {
        // offsets not zero-based
        assert!(CsrIndex::from_parts(vec![1, 2], vec![0, 0]).is_err());
        // offsets not monotone
        assert!(CsrIndex::from_parts(vec![0, 2, 1], vec![0, 0]).is_err());
        // final offset does not cover targets
        assert!(CsrIndex::from_parts(vec![0, 1], vec![0, 0]).is_err());
        // valid
        assert!(CsrIndex::from_parts(vec![0, 1, 2], vec![1, 0]).is_ok());
    }
}
