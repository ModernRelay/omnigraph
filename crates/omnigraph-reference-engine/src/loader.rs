use std::sync::Arc;

use arrow_array::{Array, Date32Array, Date64Array, StringArray};
use arrow_schema::DataType;
use omnigraph_core::error::{OmniError, Result};

pub(crate) fn parse_date32_literal(value: &str) -> Result<i32> {
    omnigraph_compiler::check_date_literal(value).map_err(OmniError::manifest)?;
    let raw: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(value)]));
    let casted = arrow_cast::cast::cast(raw.as_ref(), &DataType::Date32)
        .map_err(|e| OmniError::manifest(format!("invalid Date literal '{}': {}", value, e)))?;
    let out = casted
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| OmniError::manifest("Date32 cast produced unexpected array"))?;
    if out.is_null(0) {
        return Err(OmniError::manifest(format!(
            "invalid Date literal '{}'",
            value
        )));
    }
    Ok(out.value(0))
}

pub(crate) fn parse_date64_literal(value: &str) -> Result<i64> {
    if value.starts_with(['+', '-']) {
        return ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"]
            .iter()
            .find_map(|format| chrono::NaiveDateTime::parse_from_str(value, format).ok())
            .map(|datetime| datetime.and_utc().timestamp_millis())
            .ok_or_else(|| OmniError::manifest(format!("invalid DateTime literal '{value}'")));
    }
    let raw: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(value)]));
    let casted = arrow_cast::cast::cast(raw.as_ref(), &DataType::Date64)
        .map_err(|e| OmniError::manifest(format!("invalid DateTime literal '{}': {}", value, e)))?;
    let out = casted
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| OmniError::manifest("Date64 cast produced unexpected array"))?;
    if out.is_null(0) {
        return Err(OmniError::manifest(format!(
            "invalid DateTime literal '{}'",
            value
        )));
    }
    Ok(out.value(0))
}
