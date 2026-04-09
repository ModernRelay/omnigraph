pub mod catalog;
pub mod embedding;
pub mod error;
pub mod ir;
pub mod json_output;
pub mod query;
pub mod query_input;
pub mod result;
pub mod schema;
pub mod types;

pub use catalog::build_catalog;
pub use catalog::schema_ir::{
    SchemaIR, build_catalog_from_ir, build_schema_ir, schema_ir_hash, schema_ir_json,
    schema_ir_pretty_json,
};
pub use ir::ParamMap;
pub use ir::lower::{lower_mutation_query, lower_query};
pub use query::ast::Literal;
pub use query_input::{
    JsonParamMode, RunInputError, RunInputResult, ToParam, find_named_query,
    json_params_to_param_map,
};
pub use result::{MutationExecResult, MutationResult, QueryResult, RunResult};
pub use types::{Direction, PropType, ScalarType};
