use arrow_schema::DataType;
use serde::{Deserialize, Serialize};

const MAX_VECTOR_DIM: u32 = i32::MAX as u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScalarType {
    String,
    Bool,
    I32,
    I64,
    U32,
    U64,
    F32,
    F64,
    Date,
    DateTime,
    Vector(u32),
    Blob,
}

impl ScalarType {
    pub fn from_str_name(s: &str) -> Option<Self> {
        if let Some(inner) = s.strip_prefix("Vector(").and_then(|t| t.strip_suffix(')')) {
            let dim = inner.parse::<u32>().ok()?;
            if dim == 0 || dim > MAX_VECTOR_DIM {
                return None;
            }
            return Some(Self::Vector(dim));
        }

        match s {
            "String" => Some(Self::String),
            "Bool" => Some(Self::Bool),
            "I32" => Some(Self::I32),
            "I64" => Some(Self::I64),
            "U32" => Some(Self::U32),
            "U64" => Some(Self::U64),
            "F32" => Some(Self::F32),
            "F64" => Some(Self::F64),
            "Date" => Some(Self::Date),
            "DateTime" => Some(Self::DateTime),
            "Blob" => Some(Self::Blob),
            _ => None,
        }
    }

    pub fn to_arrow(&self) -> DataType {
        match self {
            Self::String => DataType::Utf8,
            Self::Bool => DataType::Boolean,
            Self::I32 => DataType::Int32,
            Self::I64 => DataType::Int64,
            Self::U32 => DataType::UInt32,
            Self::U64 => DataType::UInt64,
            Self::F32 => DataType::Float32,
            Self::F64 => DataType::Float64,
            Self::Date => DataType::Date32,
            Self::DateTime => DataType::Date64,
            Self::Blob => DataType::LargeBinary,
            Self::Vector(dim) => {
                let dim = i32::try_from(*dim)
                    .expect("vector dimension exceeds Arrow FixedSizeList i32 bound");
                DataType::FixedSizeList(
                    std::sync::Arc::new(arrow_schema::Field::new("item", DataType::Float32, true)),
                    dim,
                )
            }
        }
    }

    /// The inverse of [`Self::to_arrow`] over its image; `None` for an Arrow
    /// type no scalar maps to.
    pub fn from_arrow(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Utf8 => Self::String,
            DataType::Boolean => Self::Bool,
            DataType::Int32 => Self::I32,
            DataType::Int64 => Self::I64,
            DataType::UInt32 => Self::U32,
            DataType::UInt64 => Self::U64,
            DataType::Float32 => Self::F32,
            DataType::Float64 => Self::F64,
            DataType::Date32 => Self::Date,
            DataType::Date64 => Self::DateTime,
            DataType::LargeBinary => Self::Blob,
            DataType::FixedSizeList(item, dim)
                if item.name() == "item"
                    && item.is_nullable()
                    && *item.data_type() == DataType::Float32 =>
            {
                Self::Vector(u32::try_from(*dim).ok()?)
            }
            _ => return None,
        })
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::I32 | Self::I64 | Self::U32 | Self::U64 | Self::F32 | Self::F64
        )
    }
}

impl std::fmt::Display for ScalarType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::String => "String",
            Self::Bool => "Bool",
            Self::I32 => "I32",
            Self::I64 => "I64",
            Self::U32 => "U32",
            Self::U64 => "U64",
            Self::F32 => "F32",
            Self::F64 => "F64",
            Self::Date => "Date",
            Self::DateTime => "DateTime",
            Self::Blob => "Blob",
            Self::Vector(dim) => return write!(f, "Vector({})", dim),
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PropType {
    pub scalar: ScalarType,
    pub nullable: bool,
    pub list: bool,
    pub enum_values: Option<Vec<String>>,
}

impl PropType {
    pub fn from_param_type_name(s: &str, nullable: bool) -> Option<Self> {
        if let Some(inner) = s
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
        {
            let scalar = ScalarType::from_str_name(inner)?;
            return Some(Self::list_of(scalar, nullable));
        }

        let scalar = ScalarType::from_str_name(s)?;
        Some(Self::scalar(scalar, nullable))
    }

    pub fn scalar(scalar: ScalarType, nullable: bool) -> Self {
        Self {
            scalar,
            nullable,
            list: false,
            enum_values: None,
        }
    }

    pub fn list_of(scalar: ScalarType, nullable: bool) -> Self {
        Self {
            scalar,
            nullable,
            list: true,
            enum_values: None,
        }
    }

    pub fn enum_type(mut values: Vec<String>, nullable: bool) -> Self {
        values.sort();
        values.dedup();
        Self {
            scalar: ScalarType::String,
            nullable,
            list: false,
            enum_values: Some(values),
        }
    }

    pub fn is_enum(&self) -> bool {
        self.enum_values.is_some()
    }

    pub fn to_arrow(&self) -> DataType {
        let scalar_dt = self.scalar.to_arrow();
        if self.list {
            DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
                "item", scalar_dt, true,
            )))
        } else {
            scalar_dt
        }
    }

    /// The inverse of [`Self::to_arrow`] over its image, non-nullable and
    /// enum-free (both are erased by `to_arrow`); `None` outside the image.
    pub fn from_arrow(data_type: &DataType) -> Option<Self> {
        if let DataType::List(item) = data_type {
            if item.name() != "item" || !item.is_nullable() {
                return None;
            }
            return Some(Self::list_of(
                ScalarType::from_arrow(item.data_type())?,
                false,
            ));
        }
        Some(Self::scalar(ScalarType::from_arrow(data_type)?, false))
    }

    pub fn display_name(&self) -> String {
        let base = if let Some(values) = &self.enum_values {
            format!("enum({})", values.join(", "))
        } else {
            self.scalar.to_string()
        };
        let wrapped = if self.list {
            format!("[{}]", base)
        } else {
            base
        };
        if self.nullable {
            format!("{}?", wrapped)
        } else {
            wrapped
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    /// Undirected: traverse the edge both ways, deduplicated per source
    /// (`$a <edge> $b`). Only valid on same-endpoint-type edges.
    Both,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn vector_to_arrow_uses_nullable_float32_child() {
        let dt = ScalarType::Vector(4).to_arrow();
        assert_eq!(
            dt,
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4)
        );
    }

    #[test]
    fn scalar_type_from_str_name_rejects_vector_dimensions_outside_arrow_bounds() {
        let too_large = format!("Vector({})", (i32::MAX as u64) + 1);
        assert!(ScalarType::from_str_name(&too_large).is_none());
        assert_eq!(
            ScalarType::from_str_name("Vector(2147483647)"),
            Some(ScalarType::Vector(2147483647))
        );
    }

    const EVERY_SCALAR: [ScalarType; 12] = [
        ScalarType::String,
        ScalarType::Bool,
        ScalarType::I32,
        ScalarType::I64,
        ScalarType::U32,
        ScalarType::U64,
        ScalarType::F32,
        ScalarType::F64,
        ScalarType::Date,
        ScalarType::DateTime,
        ScalarType::Vector(3),
        ScalarType::Blob,
    ];

    #[test]
    fn from_arrow_inverts_to_arrow_over_every_scalar_list_and_nullability() {
        for scalar in EVERY_SCALAR {
            assert_eq!(ScalarType::from_arrow(&scalar.to_arrow()), Some(scalar));
            for list in [false, true] {
                for nullable in [false, true] {
                    let prop = if list {
                        PropType::list_of(scalar, nullable)
                    } else {
                        PropType::scalar(scalar, nullable)
                    };
                    let mut expected = prop.clone();
                    expected.nullable = false;
                    assert_eq!(
                        PropType::from_arrow(&prop.to_arrow()),
                        Some(expected),
                        "{prop:?}"
                    );
                }
            }
        }
        assert_eq!(
            PropType::from_arrow(&PropType::enum_type(vec!["a".into()], true).to_arrow()),
            Some(PropType::scalar(ScalarType::String, false))
        );
        assert_eq!(
            ScalarType::from_arrow(&DataType::Struct(Default::default())),
            None
        );
        assert_eq!(ScalarType::from_arrow(&DataType::Int8), None);
    }

    #[test]
    fn to_arrow_image_table_is_pinned() {
        let table: [(ScalarType, DataType); 11] = [
            (ScalarType::String, DataType::Utf8),
            (ScalarType::Bool, DataType::Boolean),
            (ScalarType::I32, DataType::Int32),
            (ScalarType::I64, DataType::Int64),
            (ScalarType::U32, DataType::UInt32),
            (ScalarType::U64, DataType::UInt64),
            (ScalarType::F32, DataType::Float32),
            (ScalarType::F64, DataType::Float64),
            (ScalarType::Date, DataType::Date32),
            (ScalarType::DateTime, DataType::Date64),
            (ScalarType::Blob, DataType::LargeBinary),
        ];
        for (scalar, data_type) in table {
            assert_eq!(scalar.to_arrow(), data_type, "{scalar:?}");
        }
        assert_eq!(
            PropType::list_of(ScalarType::I32, true).to_arrow(),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
    }

    #[test]
    fn prop_type_from_param_type_name_supports_lists_and_nullable_scalars() {
        assert_eq!(
            PropType::from_param_type_name("[DateTime]", false),
            Some(PropType::list_of(ScalarType::DateTime, false))
        );
        assert_eq!(
            PropType::from_param_type_name("DateTime", true),
            Some(PropType::scalar(ScalarType::DateTime, true))
        );
    }
}
