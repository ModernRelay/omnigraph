use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field};

use crate::error::{NanoError, Result};

const FLAT_BINDING_SEP: &str = "::";

pub(crate) fn flat_binding_col(variable: &str, property: &str) -> String {
    format!("{variable}{FLAT_BINDING_SEP}{property}")
}

pub(crate) fn split_flat_binding_col(name: &str) -> Option<(&str, &str)> {
    name.split_once(FLAT_BINDING_SEP)
}

pub(crate) fn binding_field_columns(batch: &RecordBatch, variable: &str) -> Vec<(Field, ArrayRef)> {
    let prefix = format!("{variable}{FLAT_BINDING_SEP}");
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            field.name().strip_prefix(&prefix).map(|property| {
                (
                    Field::new(property, field.data_type().clone(), field.is_nullable()),
                    batch.column(idx).clone(),
                )
            })
        })
        .collect()
}

pub(crate) fn binding_property_array(
    batch: &RecordBatch,
    variable: &str,
    property: &str,
) -> Result<ArrayRef> {
    let flat_name = flat_binding_col(variable, property);
    if let Some(col) = batch.column_by_name(&flat_name) {
        return Ok(col.clone());
    }

    if let Some(col) = batch.column_by_name(variable) {
        let struct_arr = col
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| NanoError::Execution(format!("column {} is not a struct", variable)))?;
        let prop_col = struct_arr.column_by_name(property).ok_or_else(|| {
            NanoError::Execution(format!("struct {} has no field {}", variable, property))
        })?;
        return Ok(prop_col.clone());
    }

    Err(NanoError::Execution(format!(
        "column {} not found",
        flat_name
    )))
}

pub(crate) fn binding_struct_array(batch: &RecordBatch, variable: &str) -> Result<ArrayRef> {
    if let Some(col) = batch.column_by_name(variable) {
        return Ok(col.clone());
    }

    let fields_and_columns = binding_field_columns(batch, variable);
    if fields_and_columns.is_empty() {
        return Err(NanoError::Execution(format!(
            "variable {} not found",
            variable
        )));
    }

    let (fields, columns): (Vec<_>, Vec<_>) = fields_and_columns.into_iter().unzip();
    Ok(Arc::new(StructArray::new(fields.into(), columns, None)) as ArrayRef)
}

pub(crate) fn binding_data_type(batch: &RecordBatch, variable: &str) -> Result<DataType> {
    if let Some(col) = batch.column_by_name(variable) {
        return Ok(col.data_type().clone());
    }

    let fields: Vec<Field> = binding_field_columns(batch, variable)
        .into_iter()
        .map(|(field, _)| field)
        .collect();
    if fields.is_empty() {
        return Err(NanoError::Execution(format!(
            "variable {} not found",
            variable
        )));
    }
    Ok(DataType::Struct(fields.into()))
}
