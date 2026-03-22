use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float64Array, Int64Array, ListArray,
    StringArray,
};

use crate::json_output::array_value_to_json;
use crate::query::ast::Literal;
use crate::store::loader::{parse_date32_literal, parse_date64_literal};

pub(crate) fn literal_to_lance_sql(literal: &Literal) -> Option<String> {
    match literal {
        Literal::String(s) => Some(format!("'{}'", s.replace('\'', "''"))),
        Literal::Integer(v) => Some(v.to_string()),
        Literal::Float(v) => {
            if v.is_finite() {
                Some(v.to_string())
            } else {
                None
            }
        }
        Literal::Bool(v) => Some(if *v { "true" } else { "false" }.to_string()),
        Literal::Date(s) => {
            let days = parse_date32_literal(s).ok()?;
            let iso = arrow_array::temporal_conversions::date32_to_datetime(days)?
                .format("%Y-%m-%d")
                .to_string();
            Some(format!("CAST('{}' AS DATE)", iso))
        }
        Literal::DateTime(s) => {
            let ms = parse_date64_literal(s).ok()?;
            let ts = arrow_array::temporal_conversions::date64_to_datetime(ms)?
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string();
            Some(format!("CAST('{}' AS TIMESTAMP(3))", ts))
        }
        Literal::List(_) => None,
    }
}

pub(crate) fn literal_to_array(
    lit: &Literal,
    num_rows: usize,
) -> std::result::Result<ArrayRef, String> {
    match lit {
        Literal::String(s) => Ok(Arc::new(StringArray::from(vec![s.as_str(); num_rows]))),
        Literal::Integer(n) => Ok(Arc::new(Int64Array::from(vec![*n; num_rows]))),
        Literal::Float(f) => Ok(Arc::new(Float64Array::from(vec![*f; num_rows]))),
        Literal::Bool(b) => Ok(Arc::new(BooleanArray::from(vec![*b; num_rows]))),
        Literal::Date(s) => {
            let days = parse_date32_literal(s).map_err(|e| e.to_string())?;
            Ok(Arc::new(Date32Array::from(vec![days; num_rows])))
        }
        Literal::DateTime(s) => {
            let ms = parse_date64_literal(s).map_err(|e| e.to_string())?;
            Ok(Arc::new(Date64Array::from(vec![ms; num_rows])))
        }
        Literal::List(items) => {
            let rendered = serde_json::Value::Array(
                items
                    .iter()
                    .map(literal_to_json_value_for_display)
                    .collect::<Vec<_>>(),
            )
            .to_string();
            Ok(Arc::new(StringArray::from(vec![rendered; num_rows])))
        }
    }
}

pub(crate) fn literal_to_json_value_for_display(lit: &Literal) -> serde_json::Value {
    match lit {
        Literal::String(v) => serde_json::Value::String(v.clone()),
        Literal::Integer(v) => serde_json::Value::Number((*v).into()),
        Literal::Float(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Literal::Bool(v) => serde_json::Value::Bool(*v),
        Literal::Date(v) => serde_json::Value::String(v.clone()),
        Literal::DateTime(v) => serde_json::Value::String(v.clone()),
        Literal::List(values) => serde_json::Value::Array(
            values
                .iter()
                .map(literal_to_json_value_for_display)
                .collect(),
        ),
    }
}

pub(crate) fn compare_list_membership(
    left: &ArrayRef,
    right: &ArrayRef,
) -> std::result::Result<BooleanArray, String> {
    let list = left.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        format!(
            "contains requires left operand to be List(..), got {:?}",
            left.data_type()
        )
    })?;

    if right.data_type().is_nested() {
        return Err(format!(
            "contains requires a scalar right operand, got {:?}",
            right.data_type()
        ));
    }

    if list.len() != right.len() {
        return Err(format!(
            "contains requires equal row counts, got {} and {}",
            list.len(),
            right.len()
        ));
    }

    let mut builder = BooleanBuilder::new();
    for row in 0..list.len() {
        if list.is_null(row) || right.is_null(row) {
            builder.append_value(false);
            continue;
        }

        let needle = array_value_to_json(right, row);
        let values = list.value(row);
        let found = (0..values.len()).any(|idx| array_value_to_json(&values, idx) == needle);
        builder.append_value(found);
    }

    Ok(builder.finish())
}
