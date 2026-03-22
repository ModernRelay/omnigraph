use arrow_schema::DataType;

pub type NodeId = u64;
pub type EdgeId = u64;
const MAX_VECTOR_DIM: u32 = i32::MAX as u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
            Self::Vector(dim) => return write!(f, "Vector({})", dim),
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PropType {
    pub scalar: ScalarType,
    pub nullable: bool,
    pub list: bool,
    pub enum_values: Option<Vec<String>>,
}

impl PropType {
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
}
