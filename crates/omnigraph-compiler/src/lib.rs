pub mod catalog;
pub mod embedding;
pub mod embedding_models;
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
pub use catalog::schema_plan::{
    SchemaMigrationPlan, SchemaMigrationStep, SchemaTypeKind, plan_schema_migration,
};
pub use embedding_models::{
    DEFAULT_EMBEDDING_MODEL, EmbeddingModelSpec, EmbeddingProvider, SUPPORTED_EMBEDDING_MODELS,
    default_embedding_model, embedding_model_by_name, supported_embedding_model_names,
};
pub use ir::ParamMap;
pub use ir::lower::{lower_mutation_query, lower_query};
pub use query::ast::Literal;
pub use query::lint::{
    QueryLintFinding, QueryLintOutput, QueryLintQueryKind, QueryLintQueryResult,
    QueryLintSchemaSource, QueryLintSchemaSourceKind, QueryLintSeverity, QueryLintStatus,
    lint_query_file,
};
pub use query_input::{
    JsonParamMode, RunInputError, RunInputResult, ToParam, find_named_query,
    json_params_to_param_map,
};
pub use result::{MutationExecResult, MutationResult, QueryResult, RunResult};
pub use types::{Direction, PropType, ScalarType};
