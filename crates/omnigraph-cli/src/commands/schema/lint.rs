use color_eyre::eyre::Result;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{QueryLintOutput, QueryLintSchemaSource, build_catalog, lint_query_file};
use std::{fs, io::Write, path::PathBuf};

use super::{LintArgs, SchemaScope};
use crate::app::Completion;
use crate::helpers::resolve_query_path;
use crate::output::finish_query_lint;

pub(crate) async fn run_lint(
    args: LintArgs,
    scope: SchemaScope<'_>,
    stdout: &mut dyn Write,
    _stderr: &mut dyn Write,
) -> Result<Completion> {
    let LintArgs {
        query,
        schema,
        json,
    } = args;
    let binding = if schema.is_some() {
        None
    } else {
        let target = scope
            .target
            .ok_or_else(|| color_eyre::eyre::eyre!("live-schema lint requires a target"))?;
        let uri = crate::scope::direct_graph_uri(target, scope.graph).await?;
        Some(crate::scope::capture_storage_binding(target)?.for_uri(&uri)?)
    };
    let output = execute_query_lint(binding.as_ref(), schema.as_ref(), &query).await?;
    finish_query_lint(&output, json, stdout)
}

pub(crate) async fn execute_query_lint(
    binding: Option<&omnigraph::storage::CapturedStorageBinding>,
    schema_path: Option<&PathBuf>,
    query_path: &PathBuf,
) -> Result<QueryLintOutput> {
    let resolved_query_path = resolve_query_path(Some(query_path), None)?;
    let query_source = fs::read_to_string(&resolved_query_path)?;
    let query_path = resolved_query_path.to_string_lossy().into_owned();

    if let Some(schema_path) = schema_path {
        let schema_source = fs::read_to_string(schema_path)?;
        let schema =
            parse_schema(&schema_source).map_err(|err| color_eyre::eyre::eyre!(err.to_string()))?;
        let catalog =
            build_catalog(&schema).map_err(|err| color_eyre::eyre::eyre!(err.to_string()))?;
        return Ok(lint_query_file(
            &catalog,
            &query_source,
            query_path,
            QueryLintSchemaSource::file(schema_path.to_string_lossy().into_owned()),
        ));
    }

    let binding = binding.ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "lint requires --schema <schema.pg> or a direct graph target (--target NAME|ADDRESS)"
        )
    })?;
    let db = crate::admission::open_read_only_with_binding(binding, None).await?;
    Ok(lint_query_file(
        &db.catalog(),
        &query_source,
        query_path,
        QueryLintSchemaSource::graph(binding.uri().to_owned()),
    ))
}
