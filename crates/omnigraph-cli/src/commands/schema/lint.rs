use color_eyre::eyre::{Result, bail, eyre};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{QueryLintOutput, QueryLintSchemaSource, build_catalog, lint_query_file};
use std::sync::Arc;
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
    let binding = if args.schema.is_some() || args.config.is_some() {
        None
    } else {
        let target = scope
            .target
            .ok_or_else(|| color_eyre::eyre::eyre!("live-schema lint requires a target"))?;
        let uri = crate::scope::direct_graph_uri(target, scope.graph).await?;
        Some(crate::scope::capture_storage_binding(target)?.for_uri(&uri)?)
    };
    if args.fix {
        return super::lint_fix::run(args, binding.as_ref(), stdout).await;
    }
    let query = args
        .query
        .ok_or_else(|| eyre!("lint requires --query <file.gq>"))?;
    let output = execute_query_lint(binding.as_ref(), args.schema.as_ref(), &query).await?;
    finish_query_lint(&output, args.json, stdout)
}

/// The catalog a lint checks against: the `--schema` file's, or the target
/// graph's accepted schema; and how the report names it.
pub(crate) async fn lint_catalog(
    binding: Option<&omnigraph::storage::CapturedStorageBinding>,
    schema_path: Option<&PathBuf>,
) -> Result<(Arc<Catalog>, QueryLintSchemaSource)> {
    if let Some(schema_path) = schema_path {
        let catalog = catalog_of_schema_file(schema_path)?;
        return Ok((
            Arc::new(catalog),
            QueryLintSchemaSource::file(schema_path.to_string_lossy().into_owned()),
        ));
    }
    let binding = binding.ok_or_else(|| {
        eyre!("lint requires --schema <schema.pg> or a direct graph target (--target NAME|ADDRESS)")
    })?;
    let db = crate::admission::open_read_only_with_binding(binding, None, None).await?;
    Ok((
        db.catalog(),
        QueryLintSchemaSource::graph(binding.uri().to_owned()),
    ))
}

/// The catalog of a `.pg` schema file.
pub(crate) fn catalog_of_schema_file(schema_path: &PathBuf) -> Result<Catalog> {
    let schema_source = fs::read_to_string(schema_path)
        .map_err(|err| eyre!("could not read schema `{}`: {err}", schema_path.display()))?;
    let schema = parse_schema(&schema_source)
        .map_err(|err| eyre!("schema `{}`: {err}", schema_path.display()))?;
    build_catalog(&schema).map_err(|err| eyre!("schema `{}`: {err}", schema_path.display()))
}

pub(crate) async fn execute_query_lint(
    binding: Option<&omnigraph::storage::CapturedStorageBinding>,
    schema_path: Option<&PathBuf>,
    query_path: &PathBuf,
) -> Result<QueryLintOutput> {
    let resolved_query_path = resolve_query_path(Some(query_path), None)?;
    if resolved_query_path.is_dir() {
        bail!(
            "`{}` is a directory; lint reads one `.gq` file, and `lint --fix` a directory",
            resolved_query_path.display()
        );
    }
    let query_source = fs::read_to_string(&resolved_query_path)?;
    let query_path = resolved_query_path.to_string_lossy().into_owned();
    let (catalog, schema_source) = lint_catalog(binding, schema_path).await?;
    Ok(lint_query_file(
        &catalog,
        &query_source,
        query_path,
        schema_source,
    ))
}
