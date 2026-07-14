pub mod catalog;
pub mod error;
pub mod ir;
pub mod json_output;
pub mod lint;
pub mod query;
pub mod query_input;
pub mod result;
pub mod schema;
pub mod types;

pub use catalog::schema_ir::{
    ConstraintIR, EdgeIR, EmbedSourceIR, FieldRefIR, InterfaceIR, NodeIR, PropertyIR,
    PropertyRefIR, SCHEMA_IR_VERSION, SchemaIR, SchemaIdentityDiagnostic,
    SchemaIdentityDiagnosticKind, SchemaIdentityDomain, SchemaResolution, StablePropertyId,
    StableTypeId, SystemFieldRefIR, SystemFieldRole, TableIncarnationId, TypeRefIR,
    initialize_schema_ir, resolve_schema_ir, schema_ir_hash, schema_ir_json, schema_ir_pretty_json,
    schema_shape_from_ir, schema_shape_hash_from_ir, validate_schema_ir,
};
pub use catalog::schema_plan::{
    DropMode, SchemaMigrationPlan, SchemaMigrationStep, SchemaTypeKind, plan_schema_migration,
};
pub use catalog::schema_shape::{
    EdgeShape, EmbedSourceShape, InterfaceShape, NodeShape, PropertyConstraintShape, PropertyShape,
    SchemaShape, ShapePropertyRef, compile_schema_shape, schema_shape_hash, schema_shape_json,
    schema_shape_pretty_json,
};
pub use catalog::{CatalogIdentity, build_catalog, build_catalog_from_ir};
pub use ir::ParamMap;
pub use ir::lower::{lower_mutation_query, lower_query};
pub use lint::{DiagnosticCode, Family, SafetyTier, Severity};
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
