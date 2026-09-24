pub mod catalog;
pub mod error;
pub mod ir;
pub mod json_output;
pub mod lint;
pub mod query;
pub mod query_input;
pub mod result;
pub mod schema;
pub mod settings;
pub mod types;

pub use catalog::schema_ir::{
    ConstraintIR, EdgeIR, EmbedSourceIR, FEATURE_EDGE_KEYS, FEATURE_SYSTEM_COLUMNS, FieldRefIR,
    InterfaceIR, NodeIR, PropertyIR, PropertyRefIR, SCHEMA_IR_VERSION, SCHEMA_IR_VERSION_EDGE_KEYS,
    SCHEMA_IR_VERSION_FEATURES, SYSTEM_COLUMNS_LEGACY, SYSTEM_COLUMNS_META, SYSTEM_COLUMNS_V3,
    SchemaIR, SchemaIdentityDiagnostic, SchemaIdentityDiagnosticKind, SchemaIdentityDomain,
    SchemaResolution, StablePropertyId, StableTypeId, SystemColumns, SystemFieldRefIR,
    SystemFieldRole, TableIncarnationId, TypeRefIR, initialize_schema_ir,
    into_legacy_image_vintage, into_legacy_vintage, into_system_columns_vintage, is_known_feature,
    is_supported_ir_version, required_features, required_ir_version, resolve_schema_ir,
    schema_ir_hash, schema_ir_json, schema_ir_pretty_json, schema_shape_from_ir,
    schema_shape_hash_from_ir, system_columns_for_features, validate_schema_ir,
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

/// The GQ grammar's version, major when it accepts less or a produced shape
/// changes, minor when it only accepts more (`docs/rfcs/2026-09-14-compatibility-surfaces.md`);
/// `(2, 0)` reserves `and`, `or`, `not`, `is`, `null` (`docs/rfcs/2026-09-24-shared-expression-model.md`).
pub const GQ_LANGUAGE_VERSION: (u16, u16) = (2, 0);
pub use ir::lower::{lower_mutation_query, lower_query};
pub use lint::{DiagnosticCode, Family, SafetyTier, Severity};
pub use query::ast::Literal;
pub use query::descriptor::{
    QueryGraphFact, QueryGraphFactKind, QueryOperationDescriptor, QueryResultFieldDescriptor,
    QueryValueKind, describe_query_operation,
};
pub use query::lint::{
    QueryLintFinding, QueryLintOutput, QueryLintQueryKind, QueryLintQueryResult,
    QueryLintSchemaSource, QueryLintSchemaSourceKind, QueryLintSeverity, QueryLintStatus,
    lint_query_file,
};
pub use query_input::{
    JsonParamMode, ReadStatement, RunInputError, RunInputResult, ToParam, find_named_query,
    find_read_statement, json_params_to_param_map,
};
pub use result::{MutationExecResult, MutationResult, QueryResult, RunResult};
pub use types::{Direction, PropType, ScalarType, check_date_literal};
