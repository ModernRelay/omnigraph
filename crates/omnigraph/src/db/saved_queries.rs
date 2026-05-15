//! Saved queries — named, parameterized `.gq` source persisted under
//! `<root>/queries/<name>.json`. Each saved query is one JSON file containing
//! `{ name, description?, source, params, updated_at_us }`. Single-file means
//! one `write_text` is the atomic boundary — no staging/rename dance like
//! schema apply needs.
//!
//! The HTTP/MCP layer surfaces these so an agent can list, retrieve, and
//! invoke user-authored queries by name (one MCP tool per saved query). The
//! source must declare exactly one `query <name>(...) { ... }` block whose
//! name matches the URL key, so the saved-query → callable-tool mapping is
//! 1:1 and unambiguous.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use omnigraph_compiler::query::parser::parse_query;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::{OmniError, Result};
use crate::storage::{StorageAdapter, join_uri};

pub(crate) const QUERIES_DIR: &str = "queries";
const QUERY_FILE_SUFFIX: &str = ".json";
const NAME_MAX_LEN: usize = 64;
const SAVED_QUERY_FORMAT_VERSION: u32 = 1;

/// Built-in MCP tool names that saved queries must not shadow. The MCP
/// layer prefixes saved-query tools with `q_`, so collisions can only occur
/// if a saved query is itself named e.g. `q_read`. Enforcing this at the
/// engine boundary keeps the rule in one place.
const RESERVED_NAMES: &[&str] = &[
    "health",
    "snapshot",
    "read",
    "change",
    "ingest",
    "schema_get",
    "schema_apply",
    "branches_list",
    "branches_create",
    "branches_delete",
    "branches_merge",
    "commits_list",
    "commits_get",
];

/// Persistent form of a saved query. Serialized as JSON under
/// `<root>/queries/<name>.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedQuery {
    pub format_version: u32,
    pub name: String,
    pub description: Option<String>,
    pub source: String,
    pub params: Vec<SavedQueryParam>,
    /// Microseconds since UNIX epoch. String for consistency with recovery
    /// sidecar timestamps elsewhere in the engine.
    pub updated_at_us: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedQueryParam {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

pub(crate) async fn list(
    root_uri: &str,
    storage: Arc<dyn StorageAdapter>,
) -> Result<Vec<SavedQuery>> {
    let dir = queries_dir_uri(root_uri);
    let entries = storage.list_dir(&dir).await?;
    let mut out = Vec::with_capacity(entries.len());
    for uri in entries {
        let Some(filename_stem) = filename_stem_for_query_file(&uri) else {
            continue;
        };
        let text = storage.read_text(&uri).await?;
        let parsed: SavedQuery = match serde_json::from_str::<SavedQuery>(&text) {
            Ok(p) => p,
            Err(err) => {
                warn!(uri = %uri, error = %err, "skipping saved query: JSON parse failed");
                continue;
            }
        };
        // Guard against operator tampering with the on-disk file (rename,
        // hand-edit, format version bump). Skipping rather than erroring keeps
        // one bad file from poisoning the entire list — the warn() log makes
        // the omission loud for the operator to investigate.
        if let Err(err) = check_record_invariants(&parsed, filename_stem) {
            warn!(uri = %uri, error = %err, "skipping saved query: failed invariant check");
            continue;
        }
        out.push(parsed);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

pub(crate) async fn get(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    name: &str,
) -> Result<SavedQuery> {
    validate_name(name)?;
    let uri = query_file_uri(root_uri, name);
    if !storage.exists(&uri).await? {
        return Err(OmniError::manifest_not_found(format!(
            "saved query '{}' not found",
            name
        )));
    }
    let text = storage.read_text(&uri).await?;
    let parsed: SavedQuery = serde_json::from_str(&text).map_err(|err| {
        OmniError::manifest_internal(format!("failed to parse saved query '{}': {}", name, err))
    })?;
    // get() is loud (single-record fetch with a known name from the caller);
    // a failed invariant check is a hard error rather than a silent skip.
    check_record_invariants(&parsed, name).map_err(OmniError::manifest_internal)?;
    Ok(parsed)
}

/// Validate that an on-disk record matches the contract we wrote: name is
/// well-formed, name matches the filename it came from, and format_version
/// is one we understand.
fn check_record_invariants(record: &SavedQuery, expected_name: &str) -> std::result::Result<(), String> {
    if record.format_version != SAVED_QUERY_FORMAT_VERSION {
        return Err(format!(
            "format_version {} is not supported (this build understands {})",
            record.format_version, SAVED_QUERY_FORMAT_VERSION
        ));
    }
    if record.name != expected_name {
        return Err(format!(
            "record name '{}' does not match filename '{}'",
            record.name, expected_name
        ));
    }
    validate_name(&record.name).map_err(|err| err.to_string())?;
    Ok(())
}

/// Pull the `<name>` out of a `.../queries/<name>.json` URI. Returns None for
/// any entry that does not match — non-JSON files, hidden files, etc.
fn filename_stem_for_query_file(uri: &str) -> Option<&str> {
    let stem = uri.strip_suffix(QUERY_FILE_SUFFIX)?;
    stem.rsplit('/').next()
}

pub(crate) async fn save(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    name: &str,
    source: &str,
    description: Option<String>,
) -> Result<SavedQuery> {
    validate_name(name)?;
    let params = extract_params(name, source)?;
    let updated_at_us = now_us();
    let record = SavedQuery {
        format_version: SAVED_QUERY_FORMAT_VERSION,
        name: name.to_string(),
        description,
        source: source.to_string(),
        params,
        updated_at_us,
    };
    let json = serde_json::to_string_pretty(&record).map_err(|err| {
        OmniError::manifest_internal(format!("failed to serialize saved query: {}", err))
    })?;
    storage
        .write_text(&query_file_uri(root_uri, name), &json)
        .await?;
    Ok(record)
}

pub(crate) async fn delete(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    name: &str,
) -> Result<bool> {
    validate_name(name)?;
    let uri = query_file_uri(root_uri, name);
    let existed = storage.exists(&uri).await?;
    storage.delete(&uri).await?;
    Ok(existed)
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(OmniError::manifest("saved query name must not be empty"));
    }
    if name.len() > NAME_MAX_LEN {
        return Err(OmniError::manifest(format!(
            "saved query name '{}' exceeds {} chars",
            name, NAME_MAX_LEN
        )));
    }
    let mut chars = name.chars();
    let first = chars.next().expect("non-empty checked above");
    if !first.is_ascii_lowercase() {
        return Err(OmniError::manifest(format!(
            "saved query name '{}' must start with a lowercase ASCII letter",
            name
        )));
    }
    for ch in chars {
        if !(ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_') {
            return Err(OmniError::manifest(format!(
                "saved query name '{}' may only contain [a-z0-9_]",
                name
            )));
        }
    }
    if RESERVED_NAMES.contains(&name) {
        return Err(OmniError::manifest(format!(
            "saved query name '{}' is reserved",
            name
        )));
    }
    Ok(())
}

/// Parse `source` and pull out the single declared query's params. The
/// source must contain exactly one `query <name>(...)` block whose name
/// matches the URL name — keeps the saved-query → MCP-tool mapping 1:1.
fn extract_params(expected_name: &str, source: &str) -> Result<Vec<SavedQueryParam>> {
    let file = parse_query(source).map_err(OmniError::Compiler)?;
    if file.queries.is_empty() {
        return Err(OmniError::manifest(
            "saved query source must declare exactly one query block",
        ));
    }
    if file.queries.len() > 1 {
        return Err(OmniError::manifest(format!(
            "saved query source declares {} query blocks; exactly one is required",
            file.queries.len()
        )));
    }
    let decl = &file.queries[0];
    if decl.name != expected_name {
        return Err(OmniError::manifest(format!(
            "source declares query '{}' but URL name is '{}' — they must match",
            decl.name, expected_name
        )));
    }
    Ok(decl
        .params
        .iter()
        .map(|p| SavedQueryParam {
            name: p.name.clone(),
            type_name: p.type_name.clone(),
            nullable: p.nullable,
        })
        .collect())
}

fn queries_dir_uri(root_uri: &str) -> String {
    join_uri(root_uri, QUERIES_DIR)
}

fn query_file_uri(root_uri: &str, name: &str) -> String {
    join_uri(root_uri, &format!("{}/{}{}", QUERIES_DIR, name, QUERY_FILE_SUFFIX))
}

fn now_us() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => format!("{}", d.as_micros()),
        Err(_) => "0".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalStorageAdapter;
    use tempfile::tempdir;

    fn local() -> Arc<dyn StorageAdapter> {
        Arc::new(LocalStorageAdapter)
    }

    #[tokio::test]
    async fn save_list_get_delete_round_trip() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = local();

        let src = "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name } }";
        let saved = save(root, storage.as_ref(), "find_person", src, Some("by name".to_string()))
            .await
            .unwrap();
        assert_eq!(saved.name, "find_person");
        assert_eq!(saved.params.len(), 1);
        assert_eq!(saved.params[0].name, "name");
        assert_eq!(saved.params[0].type_name, "String");
        assert!(!saved.params[0].nullable);

        let listed = list(root, Arc::clone(&storage)).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "find_person");

        let fetched = get(root, storage.as_ref(), "find_person").await.unwrap();
        assert_eq!(fetched.source, src);
        assert_eq!(fetched.description.as_deref(), Some("by name"));

        let existed = delete(root, storage.as_ref(), "find_person").await.unwrap();
        assert!(existed);
        let listed = list(root, Arc::clone(&storage)).await.unwrap();
        assert!(listed.is_empty());

        // delete is idempotent
        let existed = delete(root, storage.as_ref(), "find_person").await.unwrap();
        assert!(!existed);
    }

    #[tokio::test]
    async fn save_overwrites_existing() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = local();

        let v1 = "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name } }";
        save(root, storage.as_ref(), "find_person", v1, None).await.unwrap();

        let v2 = "query find_person($name: String, $age: I32?) { match { $p: Person { name: $name } } return { $p.name } }";
        let saved = save(root, storage.as_ref(), "find_person", v2, None).await.unwrap();
        assert_eq!(saved.params.len(), 2);
        assert!(saved.params[1].nullable);
    }

    #[tokio::test]
    async fn name_validation_rejects_bad_input() {
        assert!(validate_name("").is_err());
        assert!(validate_name("Foo").is_err());
        assert!(validate_name("1foo").is_err());
        assert!(validate_name("foo-bar").is_err());
        assert!(validate_name("../escape").is_err());
        assert!(validate_name("read").is_err()); // reserved
        assert!(validate_name(&"a".repeat(NAME_MAX_LEN + 1)).is_err());
        assert!(validate_name("find_person_v2").is_ok());
    }

    #[tokio::test]
    async fn source_must_match_url_name() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = local();

        let src = "query foo($x: String) { match { $p: Person { name: $x } } return { $p.name } }";
        let err = save(root, storage.as_ref(), "bar", src, None).await.unwrap_err();
        assert!(err.to_string().contains("must match"));
    }

    #[tokio::test]
    async fn source_must_have_exactly_one_query() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = local();

        let two = "query a($x: String) { match { $p: Person { name: $x } } return { $p.name } } query b() { match { $p: Person } return { $p.name } }";
        let err = save(root, storage.as_ref(), "a", two, None).await.unwrap_err();
        assert!(err.to_string().contains("exactly one"));
    }

    #[tokio::test]
    async fn get_on_missing_returns_not_found() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = local();

        let err = get(root, storage.as_ref(), "nope").await.unwrap_err();
        match err {
            OmniError::Manifest(m) => assert_eq!(m.kind, crate::error::ManifestErrorKind::NotFound),
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn list_skips_records_with_invariant_violations() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = local();

        // Save one valid record so the queries/ dir exists.
        let src =
            "query find_person($name: String) { match { $p: Person { name: $name } } return { $p.name } }";
        save(root, storage.as_ref(), "find_person", src, None).await.unwrap();

        // 1) Filename / record-name mismatch: write a renamed copy.
        let renamed = serde_json::to_string(&SavedQuery {
            format_version: SAVED_QUERY_FORMAT_VERSION,
            name: "different_name".to_string(),
            description: None,
            source: src.to_string(),
            params: vec![],
            updated_at_us: "0".to_string(),
        })
        .unwrap();
        storage
            .write_text(&query_file_uri(root, "renamed_on_disk"), &renamed)
            .await
            .unwrap();

        // 2) Future format_version we don't understand.
        let future = serde_json::to_string(&SavedQuery {
            format_version: SAVED_QUERY_FORMAT_VERSION + 1,
            name: "future".to_string(),
            description: None,
            source: src.to_string(),
            params: vec![],
            updated_at_us: "0".to_string(),
        })
        .unwrap();
        storage
            .write_text(&query_file_uri(root, "future"), &future)
            .await
            .unwrap();

        // 3) Malformed JSON entirely.
        storage
            .write_text(&query_file_uri(root, "broken"), "{not json}")
            .await
            .unwrap();

        // 4) A name that wouldn't pass validate_name (e.g. uppercase). The
        //    filename + record name agree, so the mismatch check passes —
        //    but validate_name should reject it.
        let bad_name = serde_json::to_string(&SavedQuery {
            format_version: SAVED_QUERY_FORMAT_VERSION,
            name: "Bad_Name".to_string(),
            description: None,
            source: src.to_string(),
            params: vec![],
            updated_at_us: "0".to_string(),
        })
        .unwrap();
        storage
            .write_text(&query_file_uri(root, "Bad_Name"), &bad_name)
            .await
            .unwrap();

        let listed = list(root, Arc::clone(&storage)).await.unwrap();
        // Only the valid `find_person` should survive the invariant checks.
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "find_person");
    }

    #[tokio::test]
    async fn get_rejects_records_with_invariant_violations() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = local();

        let renamed = serde_json::to_string(&SavedQuery {
            format_version: SAVED_QUERY_FORMAT_VERSION,
            name: "different_name".to_string(),
            description: None,
            source: "query different_name() { match { $p: Person } return { $p.name } }".to_string(),
            params: vec![],
            updated_at_us: "0".to_string(),
        })
        .unwrap();
        storage
            .write_text(&query_file_uri(root, "find_person"), &renamed)
            .await
            .unwrap();

        let err = get(root, storage.as_ref(), "find_person").await.unwrap_err();
        match err {
            OmniError::Manifest(m) => assert_eq!(m.kind, crate::error::ManifestErrorKind::Internal),
            other => panic!("expected Internal, got {:?}", other),
        }
    }
}
