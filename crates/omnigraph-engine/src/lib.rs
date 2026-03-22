mod catalog;
mod embedding;
pub mod error;
mod ir;
pub mod json_output;
mod plan;
pub mod query;
pub mod query_input;
pub mod result;
pub mod schema;
pub mod store;
mod types;

pub use catalog::build_catalog;
pub use catalog::schema_ir;
pub use ir::ParamMap;
pub use ir::lower::{lower_mutation_query, lower_query};
pub use plan::physical::MutationExecResult;
pub use query::ast::Literal;
pub use query_input::{
    JsonParamMode, RunInputError, RunInputResult, ToParam, find_named_query,
    json_params_to_param_map,
};
pub use result::{MutationResult, QueryResult, RunResult};
pub use types::{Direction, EdgeId, NodeId, PropType, ScalarType};
