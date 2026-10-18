//! The engine catalogue's rows: every `@` type's rows derived from the value
//! the running binary executes with (the planner's generated registries, the
//! compiler's settings definitions, this crate's format constants), built
//! once per process and fingerprinted. No dataset, no snapshot, no store.

use std::sync::{Arc, LazyLock};

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use omnigraph_compiler::catalog::NodeType;
use omnigraph_compiler::catalog::catalogue::{
    CATALOGUE_TYPES, CatalogueProp, CatalogueType, ENGINE, LOGICAL_KIND, OPERATOR, PASS, SETTING,
};
use omnigraph_compiler::settings::{DEFINITIONS, SettingKind};
use omnigraph_planner::{LogicalKind, operator_rows, pass_rows};
use sha2::{Digest, Sha256};

use crate::db::manifest::{
    INTERNAL_MANIFEST_SCHEMA_VERSION, MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
};
use crate::error::{OmniError, Result};

/// One value of a catalogue row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cell {
    Text(String),
    Integer(i64),
    Integers(Vec<i64>),
}

/// One catalogue type's rows, in registry order, with the fingerprint a
/// plan records and a replay checks.
pub(crate) struct CatalogueTable {
    pub(crate) ty: &'static CatalogueType,
    pub(crate) rows: Vec<Vec<Cell>>,
    pub(crate) fingerprint: String,
}

/// Every catalogue table, built from the binary's registries.
pub(crate) struct Catalogue {
    tables: Vec<CatalogueTable>,
}

static CATALOGUE: LazyLock<Catalogue> = LazyLock::new(Catalogue::derive);

/// The catalogue table of the type `name` (`@Operator`), `None` for any
/// other name.
pub(crate) fn table(name: &str) -> Option<&'static CatalogueTable> {
    CATALOGUE.tables.iter().find(|table| table.ty.name == name)
}

/// The catalogue table of the type key `node:@Operator`.
pub(crate) fn table_of_key(type_key: &str) -> Option<&'static CatalogueTable> {
    table(type_key.strip_prefix("node:")?)
}

impl Catalogue {
    fn derive() -> Self {
        let tables = CATALOGUE_TYPES
            .iter()
            .map(|ty| {
                let rows = rows_of(ty);
                CatalogueTable {
                    fingerprint: fingerprint(ty, &rows),
                    ty,
                    rows,
                }
            })
            .collect();
        Self { tables }
    }
}

/// The rows of one type, each cell in the type's column order. The match
/// is over the five declared types by identity; a sixth declaration in the
/// compiler without rows here fails the catalogue tests.
fn rows_of(ty: &CatalogueType) -> Vec<Vec<Cell>> {
    if *ty == OPERATOR {
        operator_rows()
            .into_iter()
            .map(|row| {
                vec![
                    Cell::Text(row.name.to_string()),
                    Cell::Integers(
                        row.inputs
                            .into_iter()
                            .map(|count| i64::try_from(count).unwrap_or(i64::MAX))
                            .collect(),
                    ),
                ]
            })
            .collect()
    } else if *ty == PASS {
        pass_rows()
            .into_iter()
            .map(|row| {
                vec![
                    Cell::Text(row.name.to_string()),
                    Cell::Integer(i64::try_from(row.position).unwrap_or(i64::MAX)),
                ]
            })
            .collect()
    } else if *ty == SETTING {
        DEFINITIONS
            .iter()
            .map(|spec| {
                vec![
                    Cell::Text(spec.name.to_string()),
                    Cell::Text(setting_kind_text(spec.kind)),
                    Cell::Text(spec.default.to_string()),
                    Cell::Text(spec.scope.as_str().to_string()),
                ]
            })
            .collect()
    } else if *ty == ENGINE {
        vec![vec![
            Cell::Text(env!("CARGO_PKG_VERSION").to_string()),
            Cell::Integer(i64::from(INTERNAL_MANIFEST_SCHEMA_VERSION)),
            Cell::Integer(i64::from(MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION)),
        ]]
    } else if *ty == LOGICAL_KIND {
        LogicalKind::ALL
            .iter()
            .map(|kind| {
                vec![
                    Cell::Text(kind.name().to_string()),
                    Cell::Text(kind.coverage().name().to_string()),
                ]
            })
            .collect()
    } else {
        Vec::new()
    }
}

/// The `@Setting.kind` spelling of a setting's legal values.
fn setting_kind_text(kind: SettingKind) -> String {
    match kind {
        SettingKind::Enum(values) => format!("enum({})", values.join(", ")),
        SettingKind::Integer { min, max } => match max {
            Some(max) => format!("integer({min}..={max})"),
            None => format!("integer({min}..)"),
        },
        SettingKind::Text => "text".to_string(),
    }
}

/// The SHA-256 of the type's declaration and every row, cell by cell: the
/// value changes exactly when the rows or the schema change.
fn fingerprint(ty: &CatalogueType, rows: &[Vec<Cell>]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(ty.declaration().as_bytes());
    for row in rows {
        hasher.update(b"\n");
        for cell in row {
            match cell {
                Cell::Text(text) => {
                    hasher.update(b"t:");
                    hasher.update(text.as_bytes());
                }
                Cell::Integer(value) => {
                    hasher.update(b"i:");
                    hasher.update(value.to_le_bytes());
                }
                Cell::Integers(values) => {
                    hasher.update(b"l:");
                    for value in values {
                        hasher.update(value.to_le_bytes());
                    }
                }
            }
            hasher.update(b"\x1f");
        }
    }
    format!("sha256:{:x}", hasher.finalize())
}

impl CatalogueTable {
    pub(crate) fn row_count(&self) -> u64 {
        self.rows.len() as u64
    }

    /// The rows as one batch of `node_type`'s Arrow schema (the identity
    /// column spelled as the graph spells it, then every column): the
    /// identity is the key value.
    pub(crate) fn batch(&self, node_type: &NodeType) -> Result<RecordBatch> {
        let schema = Arc::clone(&node_type.arrow_schema);
        if schema.fields().len() != self.ty.columns.len() + 1 {
            return Err(OmniError::manifest_internal(format!(
                "catalogue type `{}` declares {} columns; its query schema has {}",
                self.ty.name,
                self.ty.columns.len(),
                schema.fields().len()
            )));
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        let ids: Vec<&str> = self
            .rows
            .iter()
            .map(|row| match &row[0] {
                Cell::Text(key) => key.as_str(),
                Cell::Integer(_) | Cell::Integers(_) => "",
            })
            .collect();
        columns.push(Arc::new(StringArray::from(ids)));
        for (index, (name, prop)) in self.ty.columns.iter().enumerate() {
            let cells = self.rows.iter().map(|row| &row[index]);
            let column: ArrayRef = match prop {
                CatalogueProp::String => Arc::new(StringArray::from(
                    cells
                        .map(|cell| match cell {
                            Cell::Text(text) => Ok(text.as_str()),
                            _ => Err(cell_mismatch(self.ty, name)),
                        })
                        .collect::<Result<Vec<_>>>()?,
                )),
                CatalogueProp::I64 => Arc::new(Int64Array::from(
                    cells
                        .map(|cell| match cell {
                            Cell::Integer(value) => Ok(*value),
                            _ => Err(cell_mismatch(self.ty, name)),
                        })
                        .collect::<Result<Vec<_>>>()?,
                )),
                CatalogueProp::I64List => {
                    let mut builder = ListBuilder::new(Int64Builder::new());
                    for cell in cells {
                        let Cell::Integers(values) = cell else {
                            return Err(cell_mismatch(self.ty, name));
                        };
                        builder.values().append_slice(values);
                        builder.append(true);
                    }
                    Arc::new(builder.finish())
                }
            };
            columns.push(column);
        }
        RecordBatch::try_new(schema, columns).map_err(OmniError::arrow_internal)
    }
}

fn cell_mismatch(ty: &CatalogueType, column: &str) -> OmniError {
    OmniError::manifest_internal(format!(
        "catalogue type `{}` column `{column}` holds a value of another kind",
        ty.name
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_compiler::SYSTEM_COLUMNS_V3;
    use omnigraph_planner::{PassId, PhysicalNodeKind};

    #[test]
    fn every_declared_type_has_rows_and_a_batch_of_its_schema() {
        for ty in CATALOGUE_TYPES {
            let table = table(ty.name).expect("every declared type has a table");
            assert!(!table.rows.is_empty(), "{} has no rows", ty.name);
            let node = ty.node_type(SYSTEM_COLUMNS_V3);
            let batch = table.batch(&node).expect("rows fit the query schema");
            assert_eq!(batch.num_rows() as u64, table.row_count());
            assert_eq!(batch.schema(), node.arrow_schema);
            let keys: Vec<_> = table.rows.iter().map(|row| row[0].clone()).collect();
            let mut distinct = keys.clone();
            distinct.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            distinct.dedup();
            assert_eq!(keys.len(), distinct.len(), "{} repeats a key", ty.name);
            assert!(table.fingerprint.starts_with("sha256:"));
        }
    }

    #[test]
    fn operator_rows_cover_every_physical_kind_once() {
        let table = table("@Operator").unwrap();
        for kind in PhysicalNodeKind::ALL {
            let groups: Vec<_> = table
                .rows
                .iter()
                .filter(|row| row[0] == Cell::Text(kind.explain_name().to_string()))
                .collect();
            assert_eq!(groups.len(), 1, "{kind:?} is in {} rows", groups.len());
            let Cell::Integers(inputs) = &groups[0][1] else {
                panic!("inputs is a list");
            };
            for count in kind.input_counts() {
                assert!(inputs.contains(&(count as i64)), "{kind:?} admits {count}");
            }
        }
    }

    #[test]
    fn pass_rows_follow_the_registration_order() {
        let table = table("@Pass").unwrap();
        assert_eq!(table.rows.len(), PassId::ALL.len());
        for (row, pass) in table.rows.iter().zip(PassId::ALL) {
            assert_eq!(row[0], Cell::Text(pass.name().to_string()));
            assert_eq!(row[1], Cell::Integer(pass.position() as i64));
        }
    }

    #[test]
    fn setting_rows_parse_through_the_setting_parser() {
        let table = table("@Setting").unwrap();
        assert_eq!(table.rows.len(), DEFINITIONS.len());
        for row in &table.rows {
            let (Cell::Text(name), Cell::Text(default), Cell::Text(scope)) =
                (&row[0], &row[2], &row[3])
            else {
                panic!("text cells");
            };
            let id = omnigraph_compiler::settings::SettingId::parse(name).expect("a known name");
            assert_eq!(id.scope().as_str(), scope);
            if scope == "request" {
                omnigraph_compiler::settings::SettingId::parse_assignment(name, default)
                    .unwrap_or_else(|error| panic!("default of `{name}` parses: {error}"));
            }
        }
    }

    #[test]
    fn engine_row_is_the_binary_constants() {
        let table = table("@Engine").unwrap();
        assert_eq!(
            table.rows,
            vec![vec![
                Cell::Text(env!("CARGO_PKG_VERSION").to_string()),
                Cell::Integer(i64::from(INTERNAL_MANIFEST_SCHEMA_VERSION)),
                Cell::Integer(i64::from(MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION)),
            ]]
        );
    }

    #[test]
    fn logical_kind_rows_carry_every_coverage() {
        let table = table("@LogicalKind").unwrap();
        assert_eq!(table.rows.len(), LogicalKind::ALL.len());
        for (row, kind) in table.rows.iter().zip(LogicalKind::ALL) {
            assert_eq!(row[0], Cell::Text(kind.name().to_string()));
            assert_eq!(row[1], Cell::Text(kind.coverage().name().to_string()));
        }
    }
}
