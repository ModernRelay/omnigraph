//! The `--- expect shape` section: the author's statement of a rows step's
//! result columns, one `<name>: <type>` line per column in `.pg` property
//! syntax, held against the executed batch before the rows are compared.

use arrow_schema::{DataType, Schema};
use omnigraph_compiler::schema::ast::SchemaDecl;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{PropType, QueryResult, ScalarType};

use crate::BodySpan;

#[derive(Debug, Clone)]
pub(crate) struct ShapeLine {
    pub(crate) name: String,
    pub(crate) prop_type: PropType,
}

#[derive(Debug, Clone)]
pub(crate) struct ShapeExpect {
    pub(crate) lines: Vec<ShapeLine>,
    pub(crate) span: BodySpan,
}

/// Parses a shape body. Blank lines are skipped; an empty body is the bless
/// target and asserts zero columns.
pub(crate) fn parse_shape_body(body: &[(usize, &str)]) -> Result<Vec<ShapeLine>, String> {
    let mut lines = Vec::new();
    for (idx, raw) in body {
        let line_no = idx + 1;
        let text = raw.trim();
        if text.is_empty() {
            continue;
        }
        if text.starts_with('#') || text.contains("//") || text.contains("/*") {
            return Err(format!(
                "line {line_no}: comments are refused inside the shape section; comments live in the header"
            ));
        }
        if text.contains("${") {
            return Err(format!(
                "line {line_no}: `${{` is refused in a shape body; a column cannot depend on the iteration"
            ));
        }
        let Some((name, type_text)) = text.split_once(':') else {
            return Err(format!("line {line_no}: not a `<name>: <type>` shape line"));
        };
        let name = name.trim();
        if !is_column_name(name) {
            return Err(format!(
                "line {line_no}: `{name}` is not a column name (`ident` or `ident.ident`)"
            ));
        }
        let type_text = type_text.trim();
        if type_text.contains('@') {
            return Err(format!(
                "line {line_no}: annotations and body constraints are not allowed in a shape line"
            ));
        }
        let prop_type = parse_type(type_text)
            .ok_or_else(|| format!("line {line_no}: unknown type `{type_text}`"))?;
        if prop_type.enum_values.is_some() {
            return Err(format!(
                "line {line_no}: `enum(...)` is refused; the column's Arrow type is `Utf8`, write `String`"
            ));
        }
        if prop_type.scalar == ScalarType::Blob {
            return Err(format!(
                "line {line_no}: `Blob` is refused; a Blob is not a read value (T24)"
            ));
        }
        lines.push(ShapeLine {
            name: name.to_string(),
            prop_type,
        });
    }
    Ok(lines)
}

fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn is_column_name(name: &str) -> bool {
    match name.split_once('.') {
        Some((var, prop)) => is_ident(var) && is_ident(prop),
        None => is_ident(name),
    }
}

/// The `.pg` type of one shape line, through the product schema parser: the
/// text is parsed as the one property of a `node Shape { }` declaration.
fn parse_type(type_text: &str) -> Option<PropType> {
    let file = parse_schema(&format!("node Shape {{\n    v: {type_text}\n}}\n")).ok()?;
    let [SchemaDecl::Node(node)] = file.declarations.as_slice() else {
        return None;
    };
    let [prop] = node.properties.as_slice() else {
        return None;
    };
    Some(prop.prop_type.clone())
}

/// Why the executed result disagrees with the shape section, if it does:
/// column count, then per position the name, the Arrow type, and a null cell
/// in a column written without `?`. `inferred` is the compiler's schema for
/// the step, consulted only to say when it sides with the shape line.
pub(crate) fn shape_mismatch(
    shape: &[ShapeLine],
    result: &QueryResult,
    inferred: &Schema,
) -> Option<String> {
    let executed = result.schema();
    if executed.fields().len() != shape.len() {
        let hint = if shape.is_empty() {
            "; fill it with OMNIGRAPH_GQ_BLESS=1 and review the diff"
        } else {
            ""
        };
        return Some(format!(
            "result shape mismatch: the shape section names {} column(s), the executor returned {}{hint}",
            shape.len(),
            executed.fields().len()
        ));
    }
    for (i, (want, got)) in shape.iter().zip(executed.fields()).enumerate() {
        if got.name() != &want.name {
            return Some(format!(
                "result shape mismatch at column {i}: expected name `{}`, the executor returned `{}`",
                want.name,
                got.name()
            ));
        }
        let want_arrow = want.prop_type.to_arrow();
        if got.data_type() != &want_arrow {
            let expected = spell_pg(&want.prop_type.scalar, want.prop_type.list);
            let verdict = if inferred
                .fields()
                .get(i)
                .is_some_and(|f| f.data_type() == &want_arrow)
            {
                format!(
                    "; the compiler infers {expected} too, so the executor is wrong, not the shape line"
                )
            } else {
                String::new()
            };
            return Some(format!(
                "result shape mismatch at column {i} `{}`: expected {expected}, the executor returned {}{verdict}",
                want.name,
                spell_arrow(got.data_type())
            ));
        }
        if !want.prop_type.nullable {
            let nulls = null_cells(result, i);
            if nulls > 0 {
                return Some(format!(
                    "result shape mismatch at column {i} `{}`: written without `?`, the executor returned {nulls} null(s)",
                    want.name
                ));
            }
        }
    }
    None
}

pub(crate) fn null_cells(result: &QueryResult, column: usize) -> usize {
    result
        .batches()
        .iter()
        .map(|batch| batch.column(column).null_count())
        .sum()
}

/// The shape lines bless writes for an executed result: the name as
/// executed, the `.pg` spelling of the Arrow type, `?` exactly when the
/// column holds a null cell. `Err` names a column the shape grammar cannot
/// spell, so the section is not rewritten.
pub(crate) fn bless_shape_lines(result: &QueryResult) -> Result<Vec<String>, String> {
    let mut lines = Vec::with_capacity(result.schema().fields().len());
    for (i, field) in result.schema().fields().iter().enumerate() {
        if !is_column_name(field.name()) {
            return Err(format!(
                "column `{}` is not a column name the shape section can spell; the shape is not rewritten",
                field.name()
            ));
        }
        let Some(mut prop_type) = PropType::from_arrow(field.data_type()) else {
            return Err(format!(
                "column `{}` has Arrow type {} with no `.pg` spelling; the shape is not rewritten",
                field.name(),
                spell_arrow(field.data_type())
            ));
        };
        prop_type.nullable = null_cells(result, i) > 0;
        lines.push(format!("{}: {}", field.name(), prop_type.display_name()));
    }
    Ok(lines)
}

fn spell_pg(scalar: &ScalarType, list: bool) -> String {
    if list {
        format!("[{scalar}]")
    } else {
        scalar.to_string()
    }
}

/// An executed Arrow type in `.pg` spelling when it has one, Arrow spelling
/// otherwise.
fn spell_arrow(data_type: &DataType) -> String {
    match PropType::from_arrow(data_type) {
        Some(prop_type) => spell_pg(&prop_type.scalar, prop_type.list),
        None => format!("{data_type:?}"),
    }
}
