use color_eyre::eyre::{Result, bail};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{QueryLintOutput, QueryLintSchemaSource, build_catalog, lint_query_file};
use std::{fs, io::Write, path::PathBuf};

use super::{LintArgs, SchemaScope};
use crate::app::Completion;
use crate::helpers::{resolve_local_uri, resolve_maintenance_uri, resolve_query_path};
use crate::output::finish_query_lint;

pub(crate) async fn run_lint(
    args: LintArgs,
    scope: SchemaScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let LintArgs {
        uri,
        query,
        schema,
        json,
    } = args;
    // A graph target (when `--schema` is absent) resolves through the
    // direct scope path (positional URI / --store / --profile /
    // defaults.store). Offline (`--schema`) needs no graph, so leave
    // the uri unresolved in that case.
    let graph_uri = if schema.is_some() {
        uri
    } else {
        Some(
            resolve_maintenance_uri(
                scope.operator,
                scope.profile.as_deref(),
                scope.store.as_deref(),
                scope.cluster,
                scope.graph,
                uri,
                "lint",
                true,
                stderr,
            )
            .await?,
        )
    };
    let output = execute_query_lint(graph_uri, schema.as_ref(), &query).await?;
    finish_query_lint(&output, json, stdout)
}

pub(crate) async fn execute_query_lint(
    cli_uri: Option<String>,
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

    if cli_uri.is_none() {
        bail!(
            "lint requires --schema <schema.pg> (offline) or a graph target \
             (--store <uri> / --cluster <dir> --graph <id>)"
        );
    }

    let uri = resolve_local_uri(cli_uri, "lint")?;
    let db = crate::admission::open_read_only(&uri, None).await?;
    Ok(lint_query_file(
        &db.catalog(),
        &query_source,
        query_path,
        QueryLintSchemaSource::graph(uri),
    ))
}
