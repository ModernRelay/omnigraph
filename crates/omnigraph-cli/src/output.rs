//! Human/JSON output formatting for every command (moved verbatim from
//! main.rs in the modularization).

use super::*;
use crate::app::Completion;
use omnigraph_api_types::CommitListOutput;

pub(crate) fn graph_type_subject(type_key: &str) -> String {
    if let Some(type_name) = type_key.strip_prefix("node:") {
        format!("node type '{type_name}'")
    } else if let Some(type_name) = type_key.strip_prefix("edge:") {
        format!("edge type '{type_name}'")
    } else {
        format!("dataset '{type_key}'")
    }
}

pub(crate) fn write_optimize(
    uri: &str,
    stats: &[omnigraph::db::DatasetOptimizeStats],
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        let value = serde_json::json!({
            "uri": uri,
            "datasets": stats.iter().map(|s| serde_json::json!({
                "type_key": s.type_key,
                "fragments_removed": s.fragments_removed,
                "fragments_added": s.fragments_added,
                "committed": s.committed,
                "skipped": s.skipped.map(|r| r.as_str()),
                "published_dataset_version": s.published_dataset_version,
                "lance_head_version": s.lance_head_version,
                "pending_indexes": s.pending_indexes.iter().map(|p| serde_json::json!({
                    "property": p.property,
                    "reason": p.reason,
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        });
        write_json(&value, stdout)?;
    } else {
        writeln!(stdout, "optimize {} — {} datasets", uri, stats.len())?;
        for s in stats {
            let subject = graph_type_subject(&s.type_key);
            if let Some(reason) = s.skipped {
                writeln!(stdout, "  {subject:<40} skipped ({reason})")?;
            } else if s.committed {
                writeln!(
                    stdout,
                    "  {:<40} frags {} → {} ✓",
                    subject, s.fragments_removed, s.fragments_added
                )?;
            } else {
                writeln!(stdout, "  {subject:<40} no-op")?;
            }
            for p in &s.pending_indexes {
                writeln!(
                    stdout,
                    "    ↳ index pending on property '{}': {}",
                    p.property, p.reason
                )?;
            }
        }
    }
    Ok(())
}

pub(crate) fn write_full_text_rebuild(
    uri: &str,
    result: &omnigraph::db::FullTextIndexRebuildResult,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let warnings = if result.rebuilt_indexes.is_empty() {
        Vec::new()
    } else {
        vec![
            "Full-text indexes were rebuilt with the default English analyzer; any previous custom tokenizer settings were replaced.",
        ]
    };
    if json {
        write_json(
            &serde_json::json!({
                "uri": uri,
                "branch": result.branch,
                "graph_commit_id": result.graph_commit_id,
                "warnings": warnings,
                "rebuilt_indexes": result.rebuilt_indexes.iter().map(|index| {
                    serde_json::json!({
                        "type_key": index.type_key,
                        "property": index.property,
                    })
                }).collect::<Vec<_>>(),
            }),
            stdout,
        )?;
    } else {
        for warning in &warnings {
            writeln!(stderr, "warning: {warning}")?;
        }
        writeln!(
            stdout,
            "rebuild-full-text-indexes {} — branch {}, {} indexes rebuilt",
            uri,
            result.branch,
            result.rebuilt_indexes.len(),
        )?;
        for index in &result.rebuilt_indexes {
            writeln!(
                stdout,
                "  {}, property '{}'",
                graph_type_subject(&index.type_key),
                index.property,
            )?;
        }
        if let Some(commit_id) = &result.graph_commit_id {
            writeln!(stdout, "graph commit: {commit_id}")?;
        } else {
            writeln!(stdout, "no-op; no graph commit published")?;
        }
    }
    Ok(())
}

pub(crate) fn write_repair(
    uri: &str,
    stats: &omnigraph::db::RepairStats,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        let value = serde_json::json!({
            "uri": uri,
            "graph_manifest_version": stats.graph_manifest_version,
            "datasets": stats.datasets.iter().map(|s| serde_json::json!({
                "type_key": s.type_key,
                "published_dataset_version": s.published_dataset_version,
                "lance_head_version": s.lance_head_version,
                "classification": s.classification.as_str(),
                "action": s.action.as_str(),
                "operations": s.operations,
                "error": s.error,
            })).collect::<Vec<_>>(),
        });
        write_json(&value, stdout)?;
    } else {
        writeln!(stdout, "repair {} — {} datasets", uri, stats.datasets.len())?;
        for s in &stats.datasets {
            let drift = if s.published_dataset_version == s.lance_head_version {
                format!("{}", s.published_dataset_version)
            } else {
                format!("{} → {}", s.published_dataset_version, s.lance_head_version)
            };
            let ops = if s.operations.is_empty() {
                String::new()
            } else {
                format!(" [{}]", s.operations.join(", "))
            };
            let err = s
                .error
                .as_ref()
                .map(|err| format!(" ({err})"))
                .unwrap_or_default();
            writeln!(
                stdout,
                "  {:<40} {:<12} {:<22} {}{}{}",
                graph_type_subject(&s.type_key),
                s.action.as_str(),
                s.classification.as_str(),
                drift,
                ops,
                err
            )?;
        }
    }
    Ok(())
}

pub(crate) fn write_cleanup(
    uri: &str,
    policy: &omnigraph::db::CleanupPolicyOptions,
    policy_desc: &str,
    stats: &[omnigraph::db::DatasetCleanupStats],
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let keep = policy.keep_versions;
    let older_than_dur = policy.older_than;
    if json {
        let value = serde_json::json!({
            "uri": uri,
            "keep_versions": keep,
            "older_than_secs": older_than_dur.map(|d| d.as_secs()),
            "datasets": stats.iter().map(|s| serde_json::json!({
                "type_key": s.type_key,
                "bytes_removed": s.bytes_removed,
                "old_versions_removed": s.old_versions_removed,
                "error": s.error,
                "manifests_removed": s.manifests_removed,
                "unpublished_manifests": s.unpublished_manifests,
                "unpublished_bytes": s.unpublished_bytes,
                "foreign_versions": s.foreign_versions,
            })).collect::<Vec<_>>(),
        });
        write_json(&value, stdout)?;
    } else {
        let total_bytes: u64 = stats.iter().map(|s| s.bytes_removed).sum();
        let total_versions: u64 = stats.iter().map(|s| s.old_versions_removed).sum();
        let failed: Vec<String> = stats
            .iter()
            .filter(|s| s.error.is_some())
            .map(|s| graph_type_subject(&s.type_key))
            .collect();
        writeln!(
            stdout,
            "cleanup {} ({}) — removed {} versions ({} bytes) across {} datasets",
            uri,
            policy_desc,
            total_versions,
            total_bytes,
            stats.len() - failed.len()
        )?;
        if !failed.is_empty() {
            writeln!(
                stdout,
                "  {} dataset(s) failed and will be retried on the next cleanup: {}",
                failed.len(),
                failed.join(", ")
            )?;
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub(crate) struct LoadOutput {
    pub(crate) uri: String,
    pub(crate) branch: String,
    pub(crate) mode: &'static str,
    /// Present only when `--from` was given; echoes the requested base.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) base_branch: Option<String>,
    pub(crate) branch_created: bool,
    pub(crate) nodes: Vec<GraphBatchDeclarationOutput>,
    pub(crate) edges: Vec<GraphBatchDeclarationOutput>,
    pub(crate) total_entities: usize,
    pub(crate) embedding_generation: Option<omnigraph_api_types::LoadEmbeddingGeneration>,
    pub(crate) commit: Option<CommitOutput>,
}

pub(crate) fn load_output_from_graph_batch(
    uri: &str,
    mode: &'static str,
    output: &GraphBatchLoadOutput,
) -> LoadOutput {
    LoadOutput {
        uri: uri.to_string(),
        branch: output.branch.clone(),
        mode,
        base_branch: output.base_branch.clone(),
        branch_created: output.branch_created,
        nodes: output.nodes.clone(),
        edges: output.edges.clone(),
        total_entities: output.total_entities,
        embedding_generation: output.embedding_generation,
        commit: output.commit.clone(),
    }
}

/// The local arm's twin of `load_output_from_graph_batch`: build the same
/// `LoadOutput` from the engine `LoadResult` directly (the remote arm has the
/// logical graph-batch DTO; the local arm has the full result). Both load
/// mappings live here, next to the struct — RFC-009
/// Phase 2's "one place" for the `-> LoadOutput` mapping that used to fork
/// between this file and main.rs's inline construction.
pub(crate) fn load_output_from_receipt(
    uri: &str,
    branch: &str,
    mode: &'static str,
    receipt: &omnigraph::loader::LoadReceipt,
    catalog: &omnigraph_compiler::catalog::Catalog,
) -> LoadOutput {
    let result = &receipt.result;
    let mut nodes = result
        .nodes_loaded
        .iter()
        .map(|(name, entities_loaded)| GraphBatchDeclarationOutput {
            name: name.clone(),
            entities_loaded: *entities_loaded,
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.name.cmp(&right.name));
    let mut edges = result
        .edges_loaded
        .iter()
        .map(|(name, entities_loaded)| GraphBatchDeclarationOutput {
            name: name.clone(),
            entities_loaded: *entities_loaded,
        })
        .collect::<Vec<_>>();
    edges.sort_by(|left, right| left.name.cmp(&right.name));
    let total_entities = nodes
        .iter()
        .chain(&edges)
        .map(|entry| entry.entities_loaded)
        .sum();
    LoadOutput {
        uri: uri.to_string(),
        branch: branch.to_string(),
        mode,
        base_branch: result.base_branch.clone(),
        branch_created: result.branch_created,
        nodes,
        edges,
        total_entities,
        embedding_generation: omnigraph_api_types::LoadEmbeddingGeneration::for_load(
            catalog, result,
        ),
        commit: Some(omnigraph_api_types::commit_output(&receipt.commit)),
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct SchemaPlanOutput<'a> {
    pub(crate) uri: &'a str,
    pub(crate) supported: bool,
    pub(crate) step_count: usize,
    pub(crate) steps: &'a [SchemaMigrationStep],
}

pub(crate) fn write_schema_apply_human(
    output: &SchemaApplyOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    writeln!(writer, "schema apply for {}", output.uri)?;
    writeln!(
        writer,
        "supported: {}",
        if output.supported { "yes" } else { "no" }
    )?;
    writeln!(
        writer,
        "applied: {}",
        if output.applied { "yes" } else { "no" }
    )?;
    writeln!(
        writer,
        "graph_manifest_version: {}",
        output.graph_manifest_version
    )?;
    if output.steps.is_empty() {
        if output.applied {
            writeln!(writer, "schema source updated; no table migration steps")?;
        } else {
            writeln!(writer, "no schema changes")?;
        }
        return Ok(());
    }
    for step in &output.steps {
        writeln!(writer, "- {}", render_schema_plan_step(step))?;
    }
    Ok(())
}

pub(crate) fn query_kind_label(kind: QueryLintQueryKind) -> &'static str {
    match kind {
        QueryLintQueryKind::Read => "read",
        QueryLintQueryKind::Mutation => "mutation",
    }
}

pub(crate) fn severity_label(severity: QueryLintSeverity) -> &'static str {
    match severity {
        QueryLintSeverity::Error => "ERROR",
        QueryLintSeverity::Warning => "WARN ",
        QueryLintSeverity::Info => "INFO ",
    }
}

pub(crate) fn write_query_lint_human(
    output: &QueryLintOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    for result in &output.results {
        match result.status {
            QueryLintStatus::Ok => {
                writeln!(
                    writer,
                    "OK    query `{}` ({})",
                    result.name,
                    query_kind_label(result.kind)
                )?;
            }
            QueryLintStatus::Error => {
                writeln!(
                    writer,
                    "ERROR query `{}`: {}",
                    result.name,
                    result.error.as_deref().unwrap_or("unknown error")
                )?;
            }
        }

        for warning in &result.warnings {
            writeln!(writer, "WARN  query `{}`: {}", result.name, warning)?;
        }
    }

    for finding in &output.findings {
        writeln!(
            writer,
            "{} {}",
            severity_label(finding.severity),
            finding.message
        )?;
    }

    writeln!(
        writer,
        "INFO  Lint complete: {} queries processed ({} error(s), {} warning(s), {} info item(s))",
        output.queries_processed, output.errors, output.warnings, output.infos
    )?;
    Ok(())
}

pub(crate) fn finish_query_lint(
    output: &QueryLintOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    if json {
        write_json(output, stdout)?;
    } else {
        write_query_lint_human(output, stdout)?;
    }

    if output.status == QueryLintStatus::Error {
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }

    Ok(Completion::success())
}

pub(crate) fn write_json<T: Serialize>(value: &T, writer: &mut dyn Write) -> Result<()> {
    writeln!(writer, "{}", serde_json::to_string_pretty(value)?)?;
    Ok(())
}

pub(crate) fn write_cluster_validate_human(
    output: &ValidateOutput,
    stdout: &mut dyn Write,
) -> Result<()> {
    if output.ok {
        writeln!(
            stdout,
            "cluster config valid: {} resource(s), {} dependency edge(s)",
            output.resources.len(),
            output.dependencies.len()
        )?;
    } else {
        writeln!(stdout, "cluster config invalid")?;
    }
    write_cluster_diagnostics(&output.diagnostics, stdout)
}

pub(crate) fn write_cluster_plan_human(output: &PlanOutput, stdout: &mut dyn Write) -> Result<()> {
    if output.ok {
        writeln!(stdout, "cluster plan: {} change(s)", output.changes.len())?;
        match output.authority {
            omnigraph_cluster::LedgerAuthority::Observed => {
                writeln!(
                    stdout,
                    "  authority: observed (no lock taken, nothing written)"
                )?;
            }
            omnigraph_cluster::LedgerAuthority::Unlocked => {
                writeln!(stdout, "  authority: unlocked (state.lock is false)")?;
            }
            omnigraph_cluster::LedgerAuthority::Locked => {}
        }
        for change in &output.changes {
            let bindings = if change.binding_change {
                " [bindings]"
            } else {
                ""
            };
            writeln!(
                stdout,
                "  {:?} {}{bindings}",
                change.operation, change.resource
            )?;
            if let Some(migration) = &change.migration {
                if !migration.supported {
                    writeln!(stdout, "      migration UNSUPPORTED:")?;
                }
                for step in &migration.steps {
                    writeln!(
                        stdout,
                        "      {}",
                        serde_json::to_string(step).unwrap_or_else(|_| format!("{step:?}"))
                    )?;
                }
            }
        }
        if output.changes.is_empty() {
            writeln!(stdout, "  no changes")?;
        }
    } else {
        writeln!(stdout, "cluster plan failed")?;
    }
    write_cluster_diagnostics(&output.diagnostics, stdout)
}

pub(crate) fn write_cluster_status_human(
    output: &StatusOutput,
    stdout: &mut dyn Write,
) -> Result<()> {
    if output.ok {
        let state = &output.state_observations;
        if state.state_found {
            writeln!(
                stdout,
                "cluster state: revision {}, {} resource(s)",
                state.state_revision, state.resource_count
            )?;
            if let Some(digest) = state.applied_config_digest.as_deref() {
                writeln!(stdout, "  applied config: {digest}")?;
            }
            if state.locked {
                writeln!(stdout, "  lock: held{}", cluster_lock_summary(state))?;
            } else {
                writeln!(stdout, "  lock: not held")?;
            }
        } else {
            writeln!(stdout, "cluster state missing")?;
        }
    } else {
        writeln!(stdout, "cluster status failed")?;
    }
    write_cluster_diagnostics(&output.diagnostics, stdout)
}

pub(crate) fn write_cluster_state_sync_human(
    output: &StateSyncOutput,
    stdout: &mut dyn Write,
) -> Result<()> {
    let operation = match output.operation {
        omnigraph_cluster::StateSyncOperation::Observe => "observe",
    };
    if output.ok {
        let state = &output.state_observations;
        writeln!(
            stdout,
            "cluster {operation}: revision {}, {} resource(s)",
            state.state_revision, state.resource_count
        )?;
        match output.authority {
            omnigraph_cluster::LedgerAuthority::Observed => {
                writeln!(
                    stdout,
                    "  authority: observed (no lock taken, nothing written)"
                )?;
            }
            omnigraph_cluster::LedgerAuthority::Unlocked => {
                writeln!(stdout, "  authority: unlocked (state.lock is false)")?;
            }
            omnigraph_cluster::LedgerAuthority::Locked => {}
        }
        if let Some(cas) = state.state_cas.as_deref() {
            writeln!(stdout, "  state_cas: {cas}")?;
        }
        if state.lock_acquired {
            writeln!(stdout, "  lock: acquired{}", cluster_lock_summary(state))?;
        } else if state.locked {
            writeln!(
                stdout,
                "  lock: held by another process{}",
                cluster_lock_summary(state)
            )?;
        } else {
            writeln!(stdout, "  lock: not acquired")?;
        }
    } else {
        writeln!(stdout, "cluster {operation} failed")?;
    }
    write_cluster_diagnostics(&output.diagnostics, stdout)
}

pub(crate) fn write_cluster_force_unlock_human(
    output: &ForceUnlockOutput,
    stdout: &mut dyn Write,
) -> Result<()> {
    if output.ok {
        if output.lock_removed {
            writeln!(
                stdout,
                "cluster force-unlock: removed lock{}",
                cluster_lock_summary(&output.state_observations)
            )?;
        } else {
            writeln!(stdout, "cluster force-unlock: no lock removed")?;
        }
    } else {
        writeln!(stdout, "cluster force-unlock failed")?;
        if output.state_observations.locked {
            writeln!(
                stdout,
                "  lock: held{}",
                cluster_lock_summary(&output.state_observations)
            )?;
        }
    }
    write_cluster_diagnostics(&output.diagnostics, stdout)
}

pub(crate) fn cluster_lock_summary(state: &omnigraph_cluster::StateObservations) -> String {
    let Some(lock_id) = state.lock_id.as_deref() else {
        return String::new();
    };
    let mut parts = vec![format!("id={lock_id}")];
    if let Some(operation) = state.lock_operation.as_deref() {
        parts.push(format!("operation={operation}"));
    }
    if let Some(pid) = state.lock_pid {
        parts.push(format!("pid={pid}"));
    }
    if let Some(created_at) = state.lock_created_at.as_deref() {
        parts.push(format!("created_at={created_at}"));
    }
    if let Some(age_seconds) = state.lock_age_seconds {
        parts.push(format!("age_seconds={age_seconds}"));
    }
    format!(" ({})", parts.join(", "))
}

/// Where a refused query fails and its one fix, one indented line each, to
/// print under the line that names the refusal: `--> line, column` for a
/// parse refusal, `--> stage: expression` for a later one, then `fix:`.
pub(crate) fn diagnostic_detail_lines(
    diagnostic: &omnigraph_api_types::DiagnosticOutput,
) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(at) = &diagnostic.position {
        lines.push(format!("  --> line {}, column {}", at.line, at.column));
    } else if let Some(stage) = &diagnostic.stage {
        match &diagnostic.expression {
            Some(expression) => lines.push(format!("  --> {stage}: {expression}")),
            None => lines.push(format!("  --> {stage}")),
        }
    }
    if let Some(fix) = &diagnostic.fix {
        lines.push(format!("  fix: {fix}"));
    }
    lines
}

pub(crate) fn write_cluster_diagnostics(
    diagnostics: &[omnigraph_cluster::Diagnostic],
    stdout: &mut dyn Write,
) -> Result<()> {
    for diagnostic in diagnostics {
        let label = match diagnostic.severity {
            DiagnosticSeverity::Error => "ERROR",
            DiagnosticSeverity::Warning => "WARN ",
        };
        writeln!(
            stdout,
            "{label} {} {}: {}",
            diagnostic.code, diagnostic.path, diagnostic.message
        )?;
        if let Some(detail) = &diagnostic.detail {
            let detail = omnigraph_api_types::DiagnosticOutput::from(detail.as_ref());
            for line in diagnostic_detail_lines(&detail) {
                writeln!(stdout, "{line}")?;
            }
        }
    }
    Ok(())
}

pub(crate) fn finish_cluster_validate(
    output: &ValidateOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    if json {
        write_json(output, stdout)?;
    } else {
        write_cluster_validate_human(output, stdout)?;
    }
    if !output.ok {
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }
    Ok(Completion::success())
}

pub(crate) fn finish_cluster_plan(
    output: &PlanOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    if json {
        write_json(output, stdout)?;
    } else {
        write_cluster_plan_human(output, stdout)?;
    }
    if !output.ok {
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }
    Ok(Completion::success())
}

pub(crate) fn finish_cluster_status(
    output: &StatusOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    if json {
        write_json(output, stdout)?;
    } else {
        write_cluster_status_human(output, stdout)?;
    }
    if !output.ok {
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }
    Ok(Completion::success())
}

pub(crate) fn finish_cluster_state_sync(
    output: &StateSyncOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    if json {
        write_json(output, stdout)?;
    } else {
        write_cluster_state_sync_human(output, stdout)?;
    }
    if !output.ok {
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }
    Ok(Completion::success())
}

pub(crate) fn finish_cluster_force_unlock(
    output: &ForceUnlockOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    if json {
        write_json(output, stdout)?;
    } else {
        write_cluster_force_unlock_human(output, stdout)?;
    }
    if !output.ok {
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }
    Ok(Completion::success())
}

pub(crate) fn write_load_human(payload: &LoadOutput, writer: &mut dyn Write) -> Result<()> {
    writeln!(
        writer,
        "loaded {} on branch {} with {}: {} entities across {} node types and {} edge types",
        payload.uri,
        payload.branch,
        payload.mode,
        payload.total_entities,
        payload.nodes.len(),
        payload.edges.len()
    )?;
    if payload.branch_created {
        if let Some(base) = &payload.base_branch {
            writeln!(writer, "branch {} created from {}", payload.branch, base)?;
        }
    }
    if let Some(diagnostic) = payload.embedding_generation {
        writeln!(writer, "{}", diagnostic.message())?;
    }
    Ok(())
}

pub(crate) fn write_schema_plan_human(
    uri: &str,
    plan: &SchemaMigrationPlan,
    writer: &mut dyn Write,
) -> Result<()> {
    writeln!(writer, "schema plan for {}", uri)?;
    writeln!(
        writer,
        "supported: {}",
        if plan.supported { "yes" } else { "no" }
    )?;
    if plan.steps.is_empty() {
        writeln!(writer, "no table migration steps")?;
        return Ok(());
    }
    for step in &plan.steps {
        writeln!(writer, "- {}", render_schema_plan_step(step))?;
    }
    Ok(())
}

pub(crate) fn render_schema_plan_step(step: &SchemaMigrationStep) -> String {
    match step {
        SchemaMigrationStep::AddType { type_kind, name } => {
            format!("add {} type '{}'", schema_type_kind_label(*type_kind), name)
        }
        SchemaMigrationStep::RenameType {
            type_kind,
            from,
            to,
        } => format!(
            "rename {} type '{}' -> '{}'",
            schema_type_kind_label(*type_kind),
            from,
            to
        ),
        SchemaMigrationStep::AddProperty {
            type_kind,
            type_name,
            property_name,
            property_type,
        } => format!(
            "add property '{}.{}' ({}) on {} '{}'",
            type_name,
            property_name,
            render_prop_type(property_type),
            schema_type_kind_label(*type_kind),
            type_name
        ),
        SchemaMigrationStep::RenameProperty {
            type_kind,
            type_name,
            from,
            to,
        } => format!(
            "rename property '{}.{}' -> '{}.{}' on {} '{}'",
            type_name,
            from,
            type_name,
            to,
            schema_type_kind_label(*type_kind),
            type_name
        ),
        SchemaMigrationStep::AddConstraint {
            type_kind,
            type_name,
            constraint,
        } => format!(
            "add constraint {} on {} '{}'",
            constraint,
            schema_type_kind_label(*type_kind),
            type_name
        ),
        SchemaMigrationStep::ExtendEnum {
            type_kind,
            type_name,
            property_name,
            added_values,
        } => format!(
            "extend enum '{}.{}' (+{}) on {} '{}'",
            type_name,
            property_name,
            added_values.join(", +"),
            schema_type_kind_label(*type_kind),
            type_name
        ),
        SchemaMigrationStep::UpdateTypeMetadata {
            type_kind,
            name,
            annotations,
        } => format!(
            "update metadata on {} '{}' ({})",
            schema_type_kind_label(*type_kind),
            name,
            render_annotations(annotations)
        ),
        SchemaMigrationStep::UpdatePropertyMetadata {
            type_kind,
            type_name,
            property_name,
            annotations,
        } => format!(
            "update metadata on property '{}.{}' of {} '{}' ({})",
            type_name,
            property_name,
            schema_type_kind_label(*type_kind),
            type_name,
            render_annotations(annotations)
        ),
        SchemaMigrationStep::DropType { type_kind, name } => format!(
            "drop {} type '{}'",
            schema_type_kind_label(*type_kind),
            name,
        ),
        SchemaMigrationStep::DropProperty {
            type_kind,
            type_name,
            property_name,
        } => format!(
            "drop property '{}.{}' of {} '{}'",
            type_name,
            property_name,
            schema_type_kind_label(*type_kind),
            type_name,
        ),
        SchemaMigrationStep::UnsupportedChange { entity, reason, .. } => {
            // When a schema-lint code is attached, render code + tier
            // so operators see at-a-glance the kind of risk (destructive
            // / validated / safe) — not just the rule identifier.
            // Reach the diagnostic via the `diagnostic()` helper so the
            // CLI doesn't need to know how the lookup works.
            match step.diagnostic() {
                Some(diag) => format!(
                    "unsupported change on {} [{}, {}]: {}",
                    entity,
                    diag.code,
                    schema_lint_tier_label(diag.tier),
                    reason,
                ),
                None => format!("unsupported change on {}: {}", entity, reason),
            }
        }
    }
}

pub(crate) fn schema_type_kind_label(kind: omnigraph_compiler::SchemaTypeKind) -> &'static str {
    match kind {
        omnigraph_compiler::SchemaTypeKind::Interface => "interface",
        omnigraph_compiler::SchemaTypeKind::Node => "node",
        omnigraph_compiler::SchemaTypeKind::Edge => "edge",
    }
}

pub(crate) fn schema_lint_tier_label(tier: omnigraph_compiler::SafetyTier) -> &'static str {
    match tier {
        omnigraph_compiler::SafetyTier::Safe => "safe",
        omnigraph_compiler::SafetyTier::Validated => "validated",
        omnigraph_compiler::SafetyTier::Destructive => "destructive",
    }
}

pub(crate) fn render_prop_type(prop_type: &omnigraph_compiler::PropType) -> String {
    let base = if let Some(values) = &prop_type.enum_values {
        format!("Enum({})", values.join("|"))
    } else {
        prop_type.scalar.to_string()
    };
    let base = if prop_type.list {
        format!("[{}]", base)
    } else {
        base
    };
    if prop_type.nullable {
        format!("{}?", base)
    } else {
        base
    }
}

pub(crate) fn render_annotations(
    annotations: &[omnigraph_compiler::schema::ast::Annotation],
) -> String {
    annotations
        .iter()
        .map(|annotation| {
            let mut args: Vec<String> = Vec::new();
            // Values are parsed via `decode_string_literal` (quotes stripped), so
            // re-quote them as string literals on render — otherwise a value with
            // non-ident chars (e.g. `model=openai/text-embedding-3-large`) fails to
            // round-trip back through the schema parser (`annotation_kwarg` wants a
            // quoted `literal`, not a bare token).
            if let Some(value) = &annotation.value {
                args.push(format!("\"{}\"", value));
            }
            for (key, val) in &annotation.kwargs {
                args.push(format!("{}=\"{}\"", key, val));
            }
            if args.is_empty() {
                format!("@{}", annotation.name)
            } else {
                format!("@{}({})", annotation.name, args.join(", "))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn write_snapshot_human(
    graph_branch: &str,
    graph_manifest_version: u64,
    internal_schema_version: u32,
    entries: &[SnapshotDatasetOutput],
    writer: &mut dyn Write,
) -> Result<()> {
    writeln!(writer, "graph_branch: {}", graph_branch)?;
    writeln!(writer, "graph_manifest_version: {}", graph_manifest_version)?;
    writeln!(
        writer,
        "internal_schema_version: {}",
        internal_schema_version
    )?;
    for entry in entries {
        writeln!(
            writer,
            "{} type '{}' published_dataset_version={} native_dataset_branch={} entities={}",
            entry.entity_kind.as_str(),
            entry.type_name,
            entry.published_dataset_version,
            entry.native_dataset_branch.as_deref().unwrap_or("main"),
            entry.entity_count
        )?;
    }
    Ok(())
}

/// A Blob write receipt: the cell, the state it was left in and the commit
/// that published it, or that nothing was published.
pub(crate) fn write_blob_write_human(
    output: &omnigraph_api_types::BlobWriteOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    let entity = match output.selector.entity {
        omnigraph_api_types::BlobEntityKind::Node => "node",
        omnigraph_api_types::BlobEntityKind::Edge => "edge",
    };
    writeln!(writer, "entity: {entity}")?;
    writeln!(writer, "type: {}", output.selector.r#type)?;
    writeln!(writer, "id: {}", output.selector.id)?;
    writeln!(writer, "property: {}", output.selector.property)?;
    writeln!(writer, "branch: {}", output.branch)?;
    match output.kind {
        omnigraph_api_types::BlobWriteStateOutput::Managed => {
            writeln!(writer, "kind: managed")?;
            if let Some(size) = output.size {
                writeln!(writer, "size: {size}")?;
            }
            if let Some(etag) = output.etag.as_deref() {
                writeln!(writer, "etag: {etag}")?;
            }
        }
        omnigraph_api_types::BlobWriteStateOutput::Null => writeln!(writer, "kind: null")?,
    }
    if let Some(actor) = output.actor_id.as_deref() {
        writeln!(writer, "actor: {actor}")?;
    }
    match &output.commit {
        Some(commit) => writeln!(writer, "commit: {}", commit.graph_commit_id)?,
        None => writeln!(writer, "commit: none (the cell was already null)")?,
    }
    Ok(())
}

pub(crate) fn write_blob_stat_human(output: &BlobStatOutput, writer: &mut dyn Write) -> Result<()> {
    let entity = match output.selector.entity {
        omnigraph_api_types::BlobEntityKind::Node => "node",
        omnigraph_api_types::BlobEntityKind::Edge => "edge",
    };
    writeln!(writer, "entity: {entity}")?;
    writeln!(writer, "type: {}", output.selector.r#type)?;
    writeln!(writer, "id: {}", output.selector.id)?;
    writeln!(writer, "property: {}", output.selector.property)?;
    match output.kind {
        BlobContentKindOutput::Managed => {
            writeln!(writer, "kind: managed")?;
            if let Some(size) = output.size {
                writeln!(writer, "size: {size}")?;
            }
            if let Some(etag) = output.etag.as_deref() {
                writeln!(writer, "etag: {etag}")?;
            }
        }
        BlobContentKindOutput::External => {
            writeln!(writer, "kind: external")?;
            if let Some(uri) = output.uri.as_deref() {
                writeln!(writer, "uri: {uri}")?;
            }
        }
    }
    if let Some(branch) = output.target.branch.as_deref() {
        writeln!(writer, "branch: {branch}")?;
    }
    if let Some(snapshot) = output.target.snapshot.as_deref() {
        writeln!(writer, "snapshot: {snapshot}")?;
    }
    writeln!(
        writer,
        "resolved_snapshot: {}",
        output.target.resolved_snapshot
    )?;
    Ok(())
}

/// A branch statement's `ChangeOutput` prints the line its `branch` verb
/// prints; a mutation's prints the affected counts.
pub(crate) fn write_change_human(output: &ChangeOutput, writer: &mut dyn Write) -> Result<()> {
    match &output.outcome {
        Some(BranchOutcomeOutput::Created {
            from,
            name,
            from_commit_id,
        }) => {
            writeln!(
                writer,
                "created branch {name} from {from} at {from_commit_id}"
            )?;
        }
        Some(BranchOutcomeOutput::Deleted { name }) => writeln!(writer, "deleted branch {name}")?,
        Some(BranchOutcomeOutput::Merged {
            source,
            target,
            merge,
        }) => writeln!(writer, "merged {source} into {target}: {}", merge.as_str())?,
        Some(BranchOutcomeOutput::Published) => writeln!(
            writer,
            "published commit {} on {}",
            output
                .commit
                .as_ref()
                .map_or("?", |commit| commit.graph_commit_id.as_str()),
            output.branch
        )?,
        Some(BranchOutcomeOutput::Dropped { graph_commit_id }) => writeln!(
            writer,
            "dropped commit {graph_commit_id} on {}",
            output.branch
        )?,
        None => writeln!(
            writer,
            "changed {} via {}: {} nodes, {} edges",
            output.branch, output.query_name, output.affected_nodes, output.affected_edges
        )?,
    }
    if let Some(actor_id) = &output.actor_id {
        writeln!(writer, "actor_id: {}", actor_id)?;
    }
    Ok(())
}

pub(crate) fn write_commit_list_human(
    list: &CommitListOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    if let Some(head_commit_id) = &list.head_commit_id {
        writeln!(writer, "head_commit_id: {head_commit_id}")?;
    }
    for commit in &list.commits {
        let branch = commit.graph_branch.as_deref().unwrap_or("main");
        writeln!(
            writer,
            "{} graph_branch={} graph_manifest_version={}{}{}",
            commit.graph_commit_id,
            branch,
            commit.graph_manifest_version,
            commit
                .actor_id
                .as_deref()
                .map(|actor| format!(" actor={}", actor))
                .unwrap_or_default(),
            commit_kind_suffix(commit),
        )?;
    }
    if let Some(next_page_token) = &list.next_page_token {
        writeln!(writer, "next_page_token: {next_page_token}")?;
    }
    Ok(())
}

/// ` kind=unattached root=<id>` for an unattached snapshot, empty otherwise:
/// `published: false` alone does not say whether an id can publish.
fn commit_kind_suffix(commit: &CommitOutput) -> String {
    match commit.kind {
        CommitKindOutput::Unattached => format!(
            " kind=unattached root={}",
            commit.root_commit_id.as_deref().unwrap_or("?")
        ),
        CommitKindOutput::Published | CommitKindOutput::Unpublished => String::new(),
    }
}

pub(crate) fn write_commit_human(commit: &CommitOutput, writer: &mut dyn Write) -> Result<()> {
    writeln!(writer, "graph_commit_id: {}", commit.graph_commit_id)?;
    writeln!(
        writer,
        "graph_branch: {}",
        commit.graph_branch.as_deref().unwrap_or("main")
    )?;
    writeln!(
        writer,
        "graph_manifest_version: {}",
        commit.graph_manifest_version
    )?;
    if !commit.published {
        writeln!(writer, "published: false")?;
    }
    writeln!(
        writer,
        "kind: {}",
        match commit.kind {
            CommitKindOutput::Published => "published",
            CommitKindOutput::Unpublished => "unpublished",
            CommitKindOutput::Unattached => "unattached",
        }
    )?;
    if let Some(root_commit_id) = &commit.root_commit_id {
        writeln!(writer, "root_commit_id: {}", root_commit_id)?;
    }
    if let Some(parent_commit_id) = &commit.parent_commit_id {
        writeln!(writer, "parent_commit_id: {}", parent_commit_id)?;
    }
    if let Some(merged_parent_commit_id) = &commit.merged_parent_commit_id {
        writeln!(
            writer,
            "merged_parent_commit_id: {}",
            merged_parent_commit_id
        )?;
    }
    if let Some(actor_id) = &commit.actor_id {
        writeln!(writer, "actor_id: {}", actor_id)?;
    }
    writeln!(writer, "created_at: {}", commit.created_at)?;
    Ok(())
}

pub(crate) fn write_change_cause_human(
    cause: &omnigraph_api_types::ChangeCauseOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    writeln!(writer, "commit: {}", cause.graph_commit_id)?;
    writeln!(writer, "branch: {}", cause.authored_branch)?;
    if let Some(parent_commit_id) = &cause.parent_commit_id {
        writeln!(writer, "parent: {}", parent_commit_id)?;
    }
    if let Some(merged_parent_commit_id) = &cause.merged_parent_commit_id {
        writeln!(writer, "merged_parent: {}", merged_parent_commit_id)?;
    }
    if let Some(actor_id) = &cause.actor_id {
        writeln!(writer, "actor: {}", actor_id)?;
    }
    writeln!(writer, "authored_at: {}", cause.authored_at)?;
    Ok(())
}

fn write_entity_change_row(
    change: &omnigraph_api_types::EntityChangeOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    writeln!(
        writer,
        "{} {} {} {}",
        change.op.as_str(),
        change.kind.as_str(),
        change.r#type.name,
        change.id
    )?;
    // Edge endpoints (present on edge images only). An endpoint-moving update
    // must show BOTH pairs; `after.or(before)` alone silently hid the old
    // endpoints of a move.
    let before_endpoints = change
        .before
        .as_ref()
        .and_then(|image| image.endpoints.as_ref());
    let after_endpoints = change
        .after
        .as_ref()
        .and_then(|image| image.endpoints.as_ref());
    if let Some(rendered) = format_endpoint_change(before_endpoints, after_endpoints) {
        writeln!(writer, "  {rendered}")?;
    }
    // The before/after property values are the point of a change feed; the
    // previous output dropped them. Show inserted/deleted values, or the
    // changed keys of an update as `before -> after`.
    match (&change.before, &change.after) {
        (None, Some(after)) => write_change_properties("  +", &after.properties, writer)?,
        (Some(before), None) => write_change_properties("  -", &before.properties, writer)?,
        (Some(before), Some(after)) => write_change_property_diff(before, after, writer)?,
        (None, None) => {}
    }
    Ok(())
}

/// Render an edge change's endpoints for human output. An endpoint-moving
/// update shows `old_from -> old_to => new_from -> new_to`; an insert, delete,
/// or endpoint-preserving update shows the single pair. Nodes (no endpoints)
/// render nothing.
fn format_endpoint_change(
    before: Option<&omnigraph_api_types::ChangeEndpointsOutput>,
    after: Option<&omnigraph_api_types::ChangeEndpointsOutput>,
) -> Option<String> {
    match (before, after) {
        (Some(b), Some(a)) if b.from != a.from || b.to != a.to => {
            Some(format!("{} -> {} => {} -> {}", b.from, b.to, a.from, a.to))
        }
        (Some(endpoints), Some(_)) | (Some(endpoints), None) | (None, Some(endpoints)) => {
            Some(format!("{} -> {}", endpoints.from, endpoints.to))
        }
        (None, None) => None,
    }
}

/// Render one property value for human output so the ambiguous states stay
/// distinguishable: a JSON string prints verbatim (so the literal string
/// `"null"` prints as `null`), a JSON null prints the sentinel `<null>`, and an
/// absent key (only one image has it) prints `<absent>` at the diff call site.
/// The `--json` output remains the exact, machine-parseable form.
fn render_change_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Null => "<null>".to_string(),
        other => other.to_string(),
    }
}

/// The sentinel a property diff prints when a key is present in only one image.
/// Distinct from an empty string (`""` renders empty) and from a JSON null
/// (`<null>`), so add/drop-of-key reads differently from a value change to/from
/// null or empty.
const ABSENT_PROPERTY: &str = "<absent>";

fn write_change_properties(
    marker: &str,
    properties: &serde_json::Value,
    writer: &mut dyn Write,
) -> Result<()> {
    if let Some(map) = properties.as_object() {
        for (key, value) in map {
            writeln!(writer, "{marker} {key}: {}", render_change_value(value))?;
        }
    }
    Ok(())
}

fn write_change_property_diff(
    before: &omnigraph_api_types::ChangeImageOutput,
    after: &omnigraph_api_types::ChangeImageOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    let (Some(before_map), Some(after_map)) =
        (before.properties.as_object(), after.properties.as_object())
    else {
        return Ok(());
    };
    let mut keys: Vec<&String> = before_map.keys().chain(after_map.keys()).collect();
    keys.sort();
    keys.dedup();
    for key in keys {
        let before_value = before_map.get(key);
        let after_value = after_map.get(key);
        if before_value != after_value {
            let before_str = before_value
                .map(render_change_value)
                .unwrap_or_else(|| ABSENT_PROPERTY.to_string());
            let after_str = after_value
                .map(render_change_value)
                .unwrap_or_else(|| ABSENT_PROPERTY.to_string());
            writeln!(writer, "  {key}: {before_str} -> {after_str}")?;
        }
    }
    Ok(())
}

pub(crate) fn write_commit_changes_human(
    page: &omnigraph_api_types::CommitChangesOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    write_change_cause_human(&page.cause, writer)?;
    for change in &page.changes {
        write_entity_change_row(change, writer)?;
    }
    if let Some(next_page_token) = &page.next_page_token {
        writeln!(writer, "next_page_token: {}", next_page_token)?;
    }
    Ok(())
}

/// Incremental JSON rendering for an auto-paginated finite commit diff.
///
/// The wire pages remain bounded, and this renderer preserves the historical
/// aggregate JSON shape without retaining earlier pages: one cause, one open
/// `changes` array, and each change serialized as soon as its page arrives.
pub(crate) struct CommitChangesJsonStream<W: Write> {
    writer: W,
    cause: Option<omnigraph_api_types::ChangeCauseOutput>,
    first_change: bool,
}

/// Add a fixed continuation indent to pretty JSON after each newline while
/// leaving the first line inline with its enclosing field/array prefix.
struct PrettyContinuation<'a, W> {
    writer: &'a mut W,
    indent: usize,
    at_line_start: bool,
}

impl<W: Write> Write for PrettyContinuation<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut offset = 0;
        while offset < bytes.len() {
            if self.at_line_start {
                const SPACES: &[u8] = b"        ";
                debug_assert!(self.indent <= SPACES.len());
                self.writer.write_all(&SPACES[..self.indent])?;
                self.at_line_start = false;
            }
            match bytes[offset..].iter().position(|byte| *byte == b'\n') {
                Some(relative) => {
                    let end = offset + relative + 1;
                    self.writer.write_all(&bytes[offset..end])?;
                    self.at_line_start = true;
                    offset = end;
                }
                None => {
                    self.writer.write_all(&bytes[offset..])?;
                    offset = bytes.len();
                }
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

fn write_pretty_inline<W: Write, T: Serialize>(
    writer: &mut W,
    value: &T,
    continuation_indent: usize,
) -> Result<()> {
    let mut indented = PrettyContinuation {
        writer,
        indent: continuation_indent,
        at_line_start: false,
    };
    serde_json::to_writer_pretty(&mut indented, value)?;
    Ok(())
}

impl<W: Write> CommitChangesJsonStream<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            cause: None,
            first_change: true,
        }
    }

    pub(crate) fn write_page(
        &mut self,
        page: &omnigraph_api_types::CommitChangesOutput,
    ) -> Result<()> {
        match &self.cause {
            None => {
                self.writer.write_all(b"{\n  \"cause\": ")?;
                write_pretty_inline(&mut self.writer, &page.cause, 2)?;
                self.writer.write_all(b",\n  \"changes\": [")?;
                self.cause = Some(page.cause.clone());
            }
            Some(cause) if cause == &page.cause => {}
            Some(_) => bail!("commit changes continuation changed its cause while auto-paginating"),
        }
        for change in &page.changes {
            if !self.first_change {
                self.writer.write_all(b",")?;
            }
            self.writer.write_all(b"\n    ")?;
            write_pretty_inline(&mut self.writer, change, 4)?;
            self.first_change = false;
        }
        // Make the incremental behavior real even when stdout is a pipe: a
        // later page failure may leave a partial document, but never forces
        // all earlier pages to remain resident in process memory.
        self.writer.flush()?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<W> {
        if self.cause.is_none() {
            bail!("commit changes auto-pagination completed without a page");
        }
        if self.first_change {
            self.writer.write_all(b"]\n}\n")?;
        } else {
            self.writer.write_all(b"\n  ]\n}\n")?;
        }
        self.writer.flush()?;
        Ok(self.writer)
    }
}

/// Incremental human rendering for an auto-paginated finite commit diff.
/// A successful page includes a writer flush; callers stop on failure before
/// requesting a continuation, just as with the JSON stream.
pub(crate) struct CommitChangesHumanStream<W: Write> {
    writer: W,
    cause: Option<omnigraph_api_types::ChangeCauseOutput>,
}

impl<W: Write> CommitChangesHumanStream<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            cause: None,
        }
    }

    pub(crate) fn write_page(
        &mut self,
        page: &omnigraph_api_types::CommitChangesOutput,
    ) -> Result<()> {
        match &self.cause {
            None => {
                write_change_cause_human(&page.cause, &mut self.writer)?;
                self.cause = Some(page.cause.clone());
            }
            Some(cause) if cause == &page.cause => {}
            Some(_) => bail!("commit changes continuation changed its cause while auto-paginating"),
        }
        for change in &page.changes {
            write_entity_change_row(change, &mut self.writer)?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

/// The endpoints a graph diff page names; every page of one auto-paginated
/// diff must name the same ones.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DiffEndpointsHeader {
    from_commit_id: String,
    to_commit_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_commit_id: Option<String>,
}

impl DiffEndpointsHeader {
    fn of(page: &omnigraph_api_types::DiffOutput) -> Self {
        Self {
            from_commit_id: page.from_commit_id.clone(),
            to_commit_id: page.to_commit_id.clone(),
            base_commit_id: page.base_commit_id.clone(),
        }
    }
}

fn write_diff_endpoints_human(header: &DiffEndpointsHeader, writer: &mut dyn Write) -> Result<()> {
    if let Some(base_commit_id) = &header.base_commit_id {
        writeln!(writer, "merge_base: {base_commit_id}")?;
    }
    writeln!(writer, "from: {}", header.from_commit_id)?;
    writeln!(writer, "to: {}", header.to_commit_id)?;
    Ok(())
}

pub(crate) fn write_diff_human(
    page: &omnigraph_api_types::DiffOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    write_diff_endpoints_human(&DiffEndpointsHeader::of(page), writer)?;
    for change in &page.changes {
        write_entity_change_row(change, writer)?;
    }
    if let Some(next_page_token) = &page.next_page_token {
        writeln!(writer, "next_page_token: {}", next_page_token)?;
    }
    Ok(())
}

/// Incremental JSON rendering for an auto-paginated graph diff: the endpoint
/// fields once, then one open `changes` array, each change serialized as its
/// page arrives.
pub(crate) struct DiffJsonStream<W: Write> {
    writer: W,
    header: Option<DiffEndpointsHeader>,
    first_change: bool,
}

impl<W: Write> DiffJsonStream<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            header: None,
            first_change: true,
        }
    }

    pub(crate) fn write_page(&mut self, page: &omnigraph_api_types::DiffOutput) -> Result<()> {
        let header = DiffEndpointsHeader::of(page);
        match &self.header {
            None => {
                self.writer.write_all(b"{\n  \"from_commit_id\": ")?;
                serde_json::to_writer(&mut self.writer, &header.from_commit_id)?;
                self.writer.write_all(b",\n  \"to_commit_id\": ")?;
                serde_json::to_writer(&mut self.writer, &header.to_commit_id)?;
                if let Some(base_commit_id) = &header.base_commit_id {
                    self.writer.write_all(b",\n  \"base_commit_id\": ")?;
                    serde_json::to_writer(&mut self.writer, base_commit_id)?;
                }
                self.writer.write_all(b",\n  \"changes\": [")?;
                self.header = Some(header);
            }
            Some(seen) if seen == &header => {}
            Some(_) => bail!("diff continuation changed its endpoints while auto-paginating"),
        }
        for change in &page.changes {
            if !self.first_change {
                self.writer.write_all(b",")?;
            }
            self.writer.write_all(b"\n    ")?;
            write_pretty_inline(&mut self.writer, change, 4)?;
            self.first_change = false;
        }
        self.writer.flush()?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<W> {
        if self.header.is_none() {
            bail!("diff auto-pagination completed without a page");
        }
        if self.first_change {
            self.writer.write_all(b"]\n}\n")?;
        } else {
            self.writer.write_all(b"\n  ]\n}\n")?;
        }
        self.writer.flush()?;
        Ok(self.writer)
    }
}

/// Incremental human rendering for an auto-paginated graph diff.
/// Each page is flushed before the caller requests its continuation.
pub(crate) struct DiffHumanStream<W: Write> {
    writer: W,
    header: Option<DiffEndpointsHeader>,
}

impl<W: Write> DiffHumanStream<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            header: None,
        }
    }

    pub(crate) fn write_page(&mut self, page: &omnigraph_api_types::DiffOutput) -> Result<()> {
        let header = DiffEndpointsHeader::of(page);
        match &self.header {
            None => {
                write_diff_endpoints_human(&header, &mut self.writer)?;
                self.header = Some(header);
            }
            Some(seen) if seen == &header => {}
            Some(_) => bail!("diff continuation changed its endpoints while auto-paginating"),
        }
        for change in &page.changes {
            write_entity_change_row(change, &mut self.writer)?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

pub(crate) fn write_merge_base_human(
    output: &omnigraph_api_types::MergeBaseOutput,
    writer: &mut dyn Write,
) -> Result<()> {
    writeln!(writer, "merge_base: {}", output.merge_base.graph_commit_id)?;
    writeln!(
        writer,
        "source: {} at {}",
        output.source, output.source_commit_id
    )?;
    writeln!(
        writer,
        "target: {} at {}",
        output.target, output.target_commit_id
    )?;
    Ok(())
}

/// Incremental JSON rendering for one auto-paginated feed poll.
///
/// A block may straddle pages. Keeping its JSON object open lets the renderer
/// stitch that split block while retaining only the cause of the current
/// block; completed blocks and pages are never accumulated.
pub(crate) struct ChangeFeedJsonStream<W: Write> {
    writer: W,
    started: bool,
    current_cause: Option<omnigraph_api_types::ChangeCauseOutput>,
    first_block: bool,
    first_change: bool,
}

impl<W: Write> ChangeFeedJsonStream<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            started: false,
            current_cause: None,
            first_block: true,
            first_change: true,
        }
    }

    pub(crate) fn write_page(
        &mut self,
        page: &omnigraph_api_types::ChangeFeedOutput,
    ) -> Result<()> {
        if !self.started {
            self.writer.write_all(b"{\n  \"blocks\": [")?;
            self.started = true;
        }
        for block in &page.blocks {
            let same_block = match &self.current_cause {
                Some(cause) if cause == &block.cause => true,
                Some(cause) if cause.graph_commit_id == block.cause.graph_commit_id => {
                    bail!("change-feed continuation changed the cause of a split block")
                }
                _ => false,
            };
            if !same_block {
                if self.current_cause.is_some() {
                    self.close_current_block()?;
                }
                if !self.first_block {
                    self.writer.write_all(b",")?;
                }
                self.writer.write_all(b"\n    {\n      \"cause\": ")?;
                write_pretty_inline(&mut self.writer, &block.cause, 6)?;
                self.writer.write_all(b",\n      \"changes\": [")?;
                self.current_cause = Some(block.cause.clone());
                self.first_block = false;
                self.first_change = true;
            }
            for change in &block.changes {
                if !self.first_change {
                    self.writer.write_all(b",")?;
                }
                self.writer.write_all(b"\n        ")?;
                write_pretty_inline(&mut self.writer, change, 8)?;
                self.first_change = false;
            }
        }
        self.writer.flush()?;
        Ok(())
    }

    fn close_current_block(&mut self) -> Result<()> {
        if self.first_change {
            self.writer.write_all(b"]\n    }")?;
        } else {
            self.writer.write_all(b"\n      ]\n    }")?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self, cursor: Option<&str>, caught_up: Option<bool>) -> Result<W> {
        if !self.started {
            self.writer.write_all(b"{\n  \"blocks\": [")?;
        }
        if self.current_cause.is_some() {
            self.close_current_block()?;
        }
        if self.first_block {
            self.writer.write_all(b"]")?;
        } else {
            self.writer.write_all(b"\n  ]")?;
        }
        if let Some(cursor) = cursor {
            self.writer.write_all(b",\n  \"cursor\": ")?;
            serde_json::to_writer(&mut self.writer, cursor)?;
        }
        if let Some(caught_up) = caught_up {
            self.writer.write_all(b",\n  \"caught_up\": ")?;
            serde_json::to_writer(&mut self.writer, &caught_up)?;
        }
        self.writer.write_all(b"\n}\n")?;
        self.writer.flush()?;
        Ok(self.writer)
    }
}

/// Incremental human rendering for one auto-paginated feed poll. A repeated
/// cause at the start of a continuation page is suppressed so a split commit
/// still reads as one block. Each page is flushed before the next fetch.
pub(crate) struct ChangeFeedHumanStream<W: Write> {
    writer: W,
    current_cause: Option<omnigraph_api_types::ChangeCauseOutput>,
    wrote_block: bool,
}

impl<W: Write> ChangeFeedHumanStream<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            current_cause: None,
            wrote_block: false,
        }
    }

    pub(crate) fn write_page(
        &mut self,
        page: &omnigraph_api_types::ChangeFeedOutput,
    ) -> Result<()> {
        for block in &page.blocks {
            let same_block = match &self.current_cause {
                Some(cause) if cause == &block.cause => true,
                Some(cause) if cause.graph_commit_id == block.cause.graph_commit_id => {
                    bail!("change-feed continuation changed the cause of a split block")
                }
                _ => false,
            };
            if !same_block {
                if self.wrote_block {
                    writeln!(self.writer)?;
                }
                write_change_cause_human(&block.cause, &mut self.writer)?;
                self.current_cause = Some(block.cause.clone());
                self.wrote_block = true;
            }
            for change in &block.changes {
                write_entity_change_row(change, &mut self.writer)?;
            }
        }
        self.writer.flush()?;
        Ok(())
    }

    pub(crate) fn finish(mut self, cursor: Option<&str>, caught_up: Option<bool>) -> Result<W> {
        if !self.wrote_block {
            writeln!(self.writer, "(no new commits)")?;
        }
        if let Some(cursor) = cursor {
            writeln!(self.writer)?;
            writeln!(self.writer, "cursor: {cursor}")?;
            if let Some(caught_up) = caught_up {
                writeln!(self.writer, "caught_up: {caught_up}")?;
            }
        }
        self.writer.flush()?;
        Ok(self.writer)
    }
}

pub(crate) fn write_change_baseline_human(
    baseline: &omnigraph_api_types::ChangeBaselineOutput,
    out_path: &std::path::Path,
    writer: &mut dyn Write,
) -> Result<()> {
    writeln!(writer, "snapshot: {}", out_path.display())?;
    writeln!(
        writer,
        "snapshot_commit_id: {}",
        baseline.snapshot_commit_id
    )?;
    writeln!(writer, "resume_cursor: {}", baseline.resume_cursor)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use omnigraph_compiler::schema::ast::Annotation;
    use omnigraph_compiler::schema::parser::parse_schema;
    use std::collections::BTreeMap;
    use std::io::{self, Write};

    use super::{
        ChangeFeedHumanStream, ChangeFeedJsonStream, CommitChangesHumanStream,
        CommitChangesJsonStream, DiffHumanStream, DiffJsonStream, graph_type_subject,
        render_annotations,
    };

    #[test]
    fn graph_type_subject_hides_type_key_syntax() {
        assert_eq!(graph_type_subject("node:Person"), "node type 'Person'");
        assert_eq!(graph_type_subject("edge:Knows"), "edge type 'Knows'");
        assert_eq!(graph_type_subject("__manifest"), "dataset '__manifest'");
    }

    fn cause(commit: &str) -> omnigraph_api_types::ChangeCauseOutput {
        omnigraph_api_types::ChangeCauseOutput {
            graph_commit_id: commit.to_string(),
            parent_commit_id: Some("parent".to_string()),
            merged_parent_commit_id: None,
            authored_branch: "main".to_string(),
            actor_id: Some("act-test".to_string()),
            authored_at: 42,
        }
    }

    fn change(id: &str) -> omnigraph_api_types::EntityChangeOutput {
        omnigraph_api_types::EntityChangeOutput {
            kind: omnigraph_api_types::EntityKindOutput::Node,
            r#type: omnigraph_api_types::ChangeTypeOutput {
                id: "type-public".to_string(),
                name: "Person".to_string(),
            },
            id: id.to_string(),
            op: omnigraph_api_types::ChangeOpOutput::Insert,
            before: None,
            after: Some(omnigraph_api_types::ChangeImageOutput {
                properties: serde_json::json!({"name": id}),
                endpoints: None,
            }),
        }
    }

    #[test]
    fn render_annotations_quotes_values_so_embed_round_trips() {
        let mut kwargs = BTreeMap::new();
        kwargs.insert(
            "model".to_string(),
            "openai/text-embedding-3-large".to_string(),
        );
        let embed = Annotation {
            name: "embed".to_string(),
            value: Some("title".to_string()),
            kwargs,
        };

        let rendered = render_annotations(std::slice::from_ref(&embed));
        assert_eq!(
            rendered,
            r#"@embed("title", model="openai/text-embedding-3-large")"#
        );

        // The bug: an unquoted `model=openai/text-embedding-3-large` is not a
        // valid `annotation_kwarg` literal, so `schema show` output did not
        // re-parse. The rendered form must round-trip through the grammar.
        let schema = format!("node Doc {{\ntitle: String\nembedding: Vector(3) {rendered}\n}}\n");
        let parsed = parse_schema(&schema);
        assert!(
            parsed.is_ok(),
            "rendered @embed must re-parse: {:?}",
            parsed.err()
        );
    }

    fn endpoints(from: &str, to: &str) -> omnigraph_api_types::ChangeEndpointsOutput {
        omnigraph_api_types::ChangeEndpointsOutput {
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    #[test]
    fn endpoint_change_shows_both_pairs_on_a_move() {
        // An endpoint-moving update must not hide the old endpoints — the
        // previous `after.or(before)` printed only `a -> d`, losing `a -> b`.
        let before = endpoints("a", "b");
        let after = endpoints("a", "d");
        assert_eq!(
            super::format_endpoint_change(Some(&before), Some(&after)),
            Some("a -> b => a -> d".to_string())
        );
    }

    #[test]
    fn endpoint_change_shows_single_pair_when_unchanged_or_one_sided() {
        let ab = endpoints("a", "b");
        // Endpoint-preserving update collapses to one pair.
        assert_eq!(
            super::format_endpoint_change(Some(&ab), Some(&endpoints("a", "b"))),
            Some("a -> b".to_string())
        );
        // Insert (after only) and delete (before only) each show their pair.
        assert_eq!(
            super::format_endpoint_change(None, Some(&ab)),
            Some("a -> b".to_string())
        );
        assert_eq!(
            super::format_endpoint_change(Some(&ab), None),
            Some("a -> b".to_string())
        );
        // Nodes carry no endpoints.
        assert_eq!(super::format_endpoint_change(None, None), None);
    }

    #[test]
    fn render_change_value_distinguishes_null_from_the_string_null() {
        // The core F7 ambiguity: JSON null and the literal string "null" used to
        // render identically. They must now differ.
        let json_null = super::render_change_value(&serde_json::Value::Null);
        let string_null = super::render_change_value(&serde_json::json!("null"));
        assert_eq!(json_null, "<null>");
        assert_eq!(string_null, "null");
        assert_ne!(json_null, string_null);
        // An empty string renders empty — distinct from null and from an absent
        // key (`<absent>`), so the four states never collide.
        assert_eq!(super::render_change_value(&serde_json::json!("")), "");
        assert_ne!(
            super::render_change_value(&serde_json::json!("")),
            json_null
        );
        assert_ne!(
            super::render_change_value(&serde_json::json!("")),
            super::ABSENT_PROPERTY
        );
    }

    fn assert_human_format(
        expected: &str,
        write: impl Fn(&mut dyn Write) -> color_eyre::Result<()>,
    ) {
        let mut bytes = Vec::new();
        write(&mut bytes).unwrap();
        assert_eq!(bytes, expected.as_bytes());
        assert_writer_failure("human formatter", WriterFault::AfterPrefix, write);
    }

    fn assert_maintenance_report(
        human: &str,
        json: serde_json::Value,
        render: impl Fn(bool, &mut dyn Write) -> color_eyre::Result<()>,
    ) {
        assert_human_format(human, |writer| render(false, writer));
        let mut bytes = Vec::new();
        render(true, &mut bytes).unwrap();
        assert_eq!(
            bytes,
            format!("{}\n", serde_json::to_string_pretty(&json).unwrap()).as_bytes()
        );
        assert_writer_failure("maintenance JSON", WriterFault::AfterPrefix, |writer| {
            render(true, writer)
        });
    }

    #[tokio::test]
    async fn maintenance_optimize_and_repair_reports_preserve_all_result_fields() {
        use omnigraph::db::{
            Omnigraph, PendingIndex, RepairAction, RepairClassification, RepairOptions, SkipReason,
        };
        // These engine result types are non_exhaustive. Obtain one valid value
        // from a tiny graph, then set its public presentation fields; no engine
        // outcome or storage behavior is asserted by this renderer test.
        let temp = tempfile::tempdir().unwrap();
        let uri = temp
            .path()
            .join("graph.omni")
            .to_string_lossy()
            .into_owned();
        let db = Omnigraph::init(&uri, "node Person {\n    slug: String @key\n}\n")
            .await
            .unwrap();
        let mut compacted = db.optimize().await.unwrap().pop().unwrap();
        compacted.type_key = "node:Person".into();
        compacted.fragments_removed = 3;
        compacted.fragments_added = 1;
        compacted.committed = true;
        compacted.skipped = None;
        compacted.published_dataset_version = None;
        compacted.lance_head_version = None;
        compacted.pending_indexes = vec![PendingIndex {
            type_key: "node:Person".into(),
            property: "embedding".into(),
            reason: "no trainable vectors".into(),
        }];
        let mut skipped = compacted.clone();
        skipped.type_key = "edge:Knows".into();
        skipped.fragments_removed = 0;
        skipped.fragments_added = 0;
        skipped.committed = false;
        skipped.skipped = Some(SkipReason::DriftNeedsRepair);
        skipped.published_dataset_version = Some(4);
        skipped.lance_head_version = Some(5);
        skipped.pending_indexes.clear();
        let mut noop = skipped.clone();
        noop.type_key = "__manifest".into();
        noop.skipped = None;
        noop.published_dataset_version = None;
        noop.lance_head_version = None;
        let optimize = [compacted, skipped, noop];
        assert_maintenance_report(
            concat!(
                "optimize graph — 3 datasets\n",
                "  node type 'Person'                       frags 3 → 1 ✓\n",
                "    ↳ index pending on property 'embedding': no trainable vectors\n",
                "  edge type 'Knows'                        skipped (published dataset/Lance HEAD drift — run omnigraph repair)\n",
                "  dataset '__manifest'                     no-op\n",
            ),
            serde_json::json!({
                "uri":"graph",
                "datasets":[
                    {"type_key":"node:Person","fragments_removed":3,"fragments_added":1,"committed":true,"skipped":null,"published_dataset_version":null,"lance_head_version":null,"pending_indexes":[{"property":"embedding","reason":"no trainable vectors"}]},
                    {"type_key":"edge:Knows","fragments_removed":0,"fragments_added":0,"committed":false,"skipped":"drift_needs_repair","published_dataset_version":4,"lance_head_version":5,"pending_indexes":[]},
                    {"type_key":"__manifest","fragments_removed":0,"fragments_added":0,"committed":false,"skipped":null,"published_dataset_version":null,"lance_head_version":null,"pending_indexes":[]},
                ],
            }),
            |json, writer| super::write_optimize("graph", &optimize, json, writer),
        );
        assert_maintenance_report(
            "optimize graph — 0 datasets\n",
            serde_json::json!({"uri":"graph","datasets":[]}),
            |json, writer| super::write_optimize("graph", &[], json, writer),
        );

        let mut repair = db.repair(RepairOptions::default()).await.unwrap();
        let mut clean = repair.datasets.pop().unwrap();
        clean.type_key = "node:Person".into();
        clean.published_dataset_version = 4;
        clean.lance_head_version = 4;
        clean.classification = RepairClassification::NoDrift;
        clean.action = RepairAction::NoOp;
        clean.operations.clear();
        clean.error = None;
        let mut refused = clean.clone();
        refused.type_key = "edge:Knows".into();
        refused.lance_head_version = 6;
        refused.classification = RepairClassification::ForeignDrift;
        refused.action = RepairAction::Refused;
        refused.operations = vec!["Append".into(), "Delete".into()];
        refused.error = Some("foreign write".into());
        repair.datasets = vec![clean, refused];
        repair.graph_manifest_version = Some(9);
        assert_maintenance_report(
            concat!(
                "repair graph — 2 datasets\n",
                "  node type 'Person'                       no_op        no_drift               4\n",
                "  edge type 'Knows'                        refused      foreign_drift          4 → 6 [Append, Delete] (foreign write)\n",
            ),
            serde_json::json!({
                "uri":"graph", "graph_manifest_version":9,
                "datasets":[
                    {"type_key":"node:Person","published_dataset_version":4,"lance_head_version":4,"classification":"no_drift","action":"no_op","operations":[],"error":null},
                    {"type_key":"edge:Knows","published_dataset_version":4,"lance_head_version":6,"classification":"foreign_drift","action":"refused","operations":["Append","Delete"],"error":"foreign write"},
                ],
            }),
            |json, writer| super::write_repair("graph", &repair, json, writer),
        );
        repair.datasets.clear();
        repair.graph_manifest_version = None;
        assert_maintenance_report(
            "repair graph — 0 datasets\n",
            serde_json::json!({"uri":"graph","graph_manifest_version":null,"datasets":[]}),
            |json, writer| super::write_repair("graph", &repair, json, writer),
        );
    }

    #[test]
    fn maintenance_cleanup_reports_preserve_partial_failure_and_policy() {
        use omnigraph::db::{CleanupPolicyOptions, DatasetCleanupStats};
        use std::time::Duration;
        let policy = CleanupPolicyOptions {
            keep_versions: Some(2),
            older_than: Some(Duration::from_secs(60)),
        };
        let stats = [
            DatasetCleanupStats {
                type_key: "node:Person".into(),
                bytes_removed: 12,
                old_versions_removed: 3,
                error: None,
                manifests_removed: 4,
                unpublished_manifests: 5,
                unpublished_bytes: 6,
                foreign_versions: vec![7, 8],
            },
            DatasetCleanupStats {
                type_key: "edge:Knows".into(),
                bytes_removed: 2,
                old_versions_removed: 1,
                error: Some("access denied".into()),
                manifests_removed: 0,
                unpublished_manifests: 0,
                unpublished_bytes: 0,
                foreign_versions: vec![],
            },
        ];
        let description = "keep 2 versions, remove anything older than 60s";
        assert_maintenance_report(
            concat!(
                "cleanup graph (keep 2 versions, remove anything older than 60s) — removed 4 versions (14 bytes) across 1 datasets\n",
                "  1 dataset(s) failed and will be retried on the next cleanup: edge type 'Knows'\n",
            ),
            serde_json::json!({
                "uri":"graph", "keep_versions":2,"older_than_secs":60,
                "datasets":[
                    {"type_key":"node:Person","bytes_removed":12,"old_versions_removed":3,"error":null,"manifests_removed":4,"unpublished_manifests":5,"unpublished_bytes":6,"foreign_versions":[7,8]},
                    {"type_key":"edge:Knows","bytes_removed":2,"old_versions_removed":1,"error":"access denied","manifests_removed":0,"unpublished_manifests":0,"unpublished_bytes":0,"foreign_versions":[]},
                ],
            }),
            |json, writer| {
                super::write_cleanup("graph", &policy, description, &stats, json, writer)
            },
        );
        let policy = CleanupPolicyOptions {
            keep_versions: None,
            older_than: Some(Duration::from_secs(60)),
        };
        assert_maintenance_report(
            "cleanup graph (remove anything older than 60s) — removed 0 versions (0 bytes) across 0 datasets\n",
            serde_json::json!({"uri":"graph","keep_versions":null,"older_than_secs":60,"datasets":[]}),
            |json, writer| {
                super::write_cleanup(
                    "graph",
                    &policy,
                    "remove anything older than 60s",
                    &[],
                    json,
                    writer,
                )
            },
        );
    }

    #[test]
    fn maintenance_full_text_reports_keep_warning_stream_and_noop_distinct() {
        use omnigraph::db::{FullTextIndexRebuildResult, RebuiltFullTextIndex};
        let warning = "Full-text indexes were rebuilt with the default English analyzer; any previous custom tokenizer settings were replaced.";
        let result = FullTextIndexRebuildResult {
            branch: "dev".into(),
            graph_commit_id: Some("commit-1".into()),
            rebuilt_indexes: vec![RebuiltFullTextIndex {
                type_key: "node:Doc".into(),
                property: "body".into(),
            }],
        };
        assert_maintenance_report(
            "rebuild-full-text-indexes graph — branch dev, 1 indexes rebuilt\n  node type 'Doc', property 'body'\ngraph commit: commit-1\n",
            serde_json::json!({"uri":"graph","branch":"dev","graph_commit_id":"commit-1","warnings":[warning],"rebuilt_indexes":[{"type_key":"node:Doc","property":"body"}]}),
            |json, writer| {
                let mut stderr = Vec::new();
                let output =
                    super::write_full_text_rebuild("graph", &result, json, writer, &mut stderr);
                assert_eq!(
                    stderr,
                    if json {
                        Vec::new()
                    } else {
                        format!("warning: {warning}\n").into_bytes()
                    }
                );
                output
            },
        );
        assert_writer_failure("full-text warning", WriterFault::AfterPrefix, |stderr| {
            let mut stdout = Vec::new();
            let error =
                super::write_full_text_rebuild("graph", &result, false, &mut stdout, stderr);
            assert!(
                stdout.is_empty(),
                "warning is delivered before the human report"
            );
            error
        });
        let noop = FullTextIndexRebuildResult {
            branch: "main".into(),
            graph_commit_id: None,
            rebuilt_indexes: vec![],
        };
        assert_maintenance_report(
            "rebuild-full-text-indexes graph — branch main, 0 indexes rebuilt\nno-op; no graph commit published\n",
            serde_json::json!({"uri":"graph","branch":"main","graph_commit_id":null,"warnings":[],"rebuilt_indexes":[]}),
            |json, writer| {
                let mut stderr = Vec::new();
                let output =
                    super::write_full_text_rebuild("graph", &noop, json, writer, &mut stderr);
                assert!(stderr.is_empty());
                output
            },
        );
    }

    #[test]
    fn schema_human_reports_preserve_empty_unsupported_and_applied_shapes() {
        use omnigraph_compiler::{SchemaMigrationPlan, SchemaMigrationStep, SchemaTypeKind};

        let added = SchemaMigrationStep::AddType {
            type_kind: SchemaTypeKind::Node,
            name: "Doc".to_string(),
        };
        for (plan, expected) in [
            (
                SchemaMigrationPlan {
                    supported: true,
                    steps: Vec::new(),
                },
                "schema plan for graph\nsupported: yes\nno table migration steps\n",
            ),
            (
                SchemaMigrationPlan {
                    supported: true,
                    steps: vec![added.clone()],
                },
                "schema plan for graph\nsupported: yes\n- add node type 'Doc'\n",
            ),
            (
                SchemaMigrationPlan {
                    supported: false,
                    steps: vec![SchemaMigrationStep::UnsupportedChange {
                        entity: "Doc.body".to_string(),
                        reason: "type changed".to_string(),
                        code: None,
                    }],
                },
                "schema plan for graph\nsupported: no\n- unsupported change on Doc.body: type changed\n",
            ),
        ] {
            assert_human_format(expected, |writer| {
                super::write_schema_plan_human("graph", &plan, writer)
            });
        }

        for (applied, steps, expected) in [
            (
                false,
                Vec::new(),
                "schema apply for graph\nsupported: yes\napplied: no\ngraph_manifest_version: 7\nno schema changes\n",
            ),
            (
                true,
                Vec::new(),
                "schema apply for graph\nsupported: yes\napplied: yes\ngraph_manifest_version: 7\nschema source updated; no table migration steps\n",
            ),
            (
                true,
                vec![added],
                "schema apply for graph\nsupported: yes\napplied: yes\ngraph_manifest_version: 7\n- add node type 'Doc'\n",
            ),
        ] {
            let output = omnigraph_api_types::SchemaApplyOutput {
                uri: "graph".to_string(),
                supported: true,
                applied,
                step_count: steps.len(),
                graph_manifest_version: 7,
                steps,
            };
            assert_human_format(expected, |writer| {
                super::write_schema_apply_human(&output, writer)
            });
        }
    }

    #[test]
    fn lint_report_preserves_text_status_and_report_flush_failures() {
        use omnigraph_compiler::{
            QueryLintFinding, QueryLintOutput, QueryLintQueryKind, QueryLintQueryResult,
            QueryLintSchemaSource, QueryLintSeverity, QueryLintStatus,
        };

        let output = QueryLintOutput {
            status: QueryLintStatus::Error,
            schema_source: QueryLintSchemaSource::file("schema.pg"),
            query_path: "queries.gq".to_string(),
            queries_processed: 3,
            errors: 2,
            warnings: 2,
            infos: 1,
            results: vec![
                QueryLintQueryResult {
                    name: "read_docs".to_string(),
                    kind: QueryLintQueryKind::Read,
                    status: QueryLintStatus::Ok,
                    warnings: vec!["unused parameter".to_string()],
                    error: None,
                    operation: None,
                },
                QueryLintQueryResult {
                    name: "update_docs".to_string(),
                    kind: QueryLintQueryKind::Mutation,
                    status: QueryLintStatus::Ok,
                    warnings: Vec::new(),
                    error: None,
                    operation: None,
                },
                QueryLintQueryResult {
                    name: "broken".to_string(),
                    kind: QueryLintQueryKind::Read,
                    status: QueryLintStatus::Error,
                    warnings: Vec::new(),
                    error: None,
                    operation: None,
                },
            ],
            findings: [
                (QueryLintSeverity::Error, "invalid read"),
                (QueryLintSeverity::Warning, "missing coverage"),
                (QueryLintSeverity::Info, "index available"),
            ]
            .into_iter()
            .map(|(severity, message)| QueryLintFinding {
                severity,
                code: "L000".to_string(),
                message: message.to_string(),
                type_name: None,
                property: None,
                query_names: Vec::new(),
            })
            .collect(),
        };
        let human = "OK    query `read_docs` (read)\nWARN  query `read_docs`: unused parameter\n\
            OK    query `update_docs` (mutation)\nERROR query `broken`: unknown error\n\
            ERROR invalid read\nWARN  missing coverage\nINFO  index available\n\
            INFO  Lint complete: 3 queries processed (2 error(s), 2 warning(s), 1 info item(s))\n";
        assert_human_format(human, |writer| {
            super::write_query_lint_human(&output, writer)
        });
        for json in [false, true] {
            let mut bytes = Vec::new();
            let completion = super::finish_query_lint(&output, json, &mut bytes).unwrap();
            assert_eq!(completion.code(), 1);
            let expected = if json {
                format!("{}\n", serde_json::to_string_pretty(&output).unwrap())
            } else {
                human.to_string()
            };
            assert_eq!(bytes, expected.as_bytes());
            for fault in [WriterFault::AfterPrefix, WriterFault::Flush] {
                assert_writer_failure("lint report", fault, |writer| {
                    super::finish_query_lint(&output, json, writer).map(|_| ())
                });
            }
        }

        let clean = QueryLintOutput {
            status: QueryLintStatus::Ok,
            queries_processed: 0,
            errors: 0,
            warnings: 0,
            infos: 0,
            results: Vec::new(),
            findings: Vec::new(),
            ..output
        };
        let mut writer = FailingWriter {
            fault: WriterFault::Flush,
            bytes: Vec::new(),
            flushes: 0,
        };
        assert_eq!(
            super::finish_query_lint(&clean, false, &mut writer)
                .unwrap()
                .code(),
            0
        );
        assert_eq!(
            writer.flushes, 0,
            "successful report flush stays with application completion"
        );
    }

    #[test]
    fn load_human_preserves_branch_and_embedding_diagnostics() {
        let mut output = super::LoadOutput {
            uri: "graph".to_string(),
            branch: "work".to_string(),
            mode: "append",
            base_branch: Some("main".to_string()),
            branch_created: true,
            nodes: vec![omnigraph_api_types::GraphBatchDeclarationOutput {
                name: "Doc".to_string(),
                entities_loaded: 2,
            }],
            edges: Vec::new(),
            total_entities: 2,
            embedding_generation: Some(omnigraph_api_types::LoadEmbeddingGeneration::Unsupported),
            commit: None,
        };
        assert_human_format(
            "loaded graph on branch work with append: 2 entities across 1 node types and 0 edge types\n\
             branch work created from main\n\
             Loads do not generate embeddings. Supplied vectors are preserved; omitted vectors follow schema nullability. Supply vectors in the input or prepare them with omnigraph embed.\n",
            |writer| super::write_load_human(&output, writer),
        );
        output.branch_created = false;
        output.embedding_generation = None;
        assert_human_format(
            "loaded graph on branch work with append: 2 entities across 1 node types and 0 edge types\n",
            |writer| super::write_load_human(&output, writer),
        );
    }

    #[test]
    fn blob_stat_human_preserves_managed_external_and_read_target_fields() {
        let mut query = omnigraph_api_types::BlobReadQuery {
            entity: omnigraph_api_types::BlobEntityKind::Node,
            r#type: "Doc".to_string(),
            id: "one".to_string(),
            property: "body".to_string(),
            branch: Some("work".to_string()),
            snapshot: None,
        };
        let output =
            omnigraph_api_types::BlobStatOutput::managed(&query, "resolved", 0, "\"etag\"");
        assert_human_format(
            "entity: node\ntype: Doc\nid: one\nproperty: body\nkind: managed\n\
             size: 0\netag: \"etag\"\nbranch: work\nresolved_snapshot: resolved\n",
            |writer| super::write_blob_stat_human(&output, writer),
        );

        query.entity = omnigraph_api_types::BlobEntityKind::Edge;
        query.branch = None;
        query.snapshot = Some("requested".to_string());
        let output =
            omnigraph_api_types::BlobStatOutput::external(&query, "resolved", "s3://bucket/body");
        assert_human_format(
            "entity: edge\ntype: Doc\nid: one\nproperty: body\nkind: external\n\
             uri: s3://bucket/body\nsnapshot: requested\nresolved_snapshot: resolved\n",
            |writer| super::write_blob_stat_human(&output, writer),
        );
    }

    #[test]
    fn blob_write_human_preserves_receipt_and_already_null_text() {
        let mut output = omnigraph_api_types::BlobWriteOutput {
            selector: omnigraph_api_types::BlobSelectorOutput {
                entity: omnigraph_api_types::BlobEntityKind::Node,
                r#type: "Doc".to_string(),
                id: "one".to_string(),
                property: "body".to_string(),
            },
            branch: "work".to_string(),
            kind: omnigraph_api_types::BlobWriteStateOutput::Managed,
            size: Some(0),
            etag: Some("\"etag\"".to_string()),
            actor_id: Some("actor".to_string()),
            commit: Some(omnigraph_api_types::CommitOutput {
                graph_commit_id: "published".to_string(),
                graph_branch: Some("work".to_string()),
                graph_manifest_version: 2,
                parent_commit_id: None,
                merged_parent_commit_id: None,
                actor_id: Some("actor".to_string()),
                created_at: 42,
                published: true,
                kind: omnigraph_api_types::CommitKindOutput::Published,
                root_commit_id: None,
            }),
        };
        assert_human_format(
            "entity: node\ntype: Doc\nid: one\nproperty: body\nbranch: work\n\
             kind: managed\nsize: 0\netag: \"etag\"\nactor: actor\ncommit: published\n",
            |writer| super::write_blob_write_human(&output, writer),
        );

        output.selector.entity = omnigraph_api_types::BlobEntityKind::Edge;
        output.kind = omnigraph_api_types::BlobWriteStateOutput::Null;
        output.size = None;
        output.etag = None;
        output.actor_id = None;
        output.commit = None;
        assert_human_format(
            "entity: edge\ntype: Doc\nid: one\nproperty: body\nbranch: work\n\
             kind: null\ncommit: none (the cell was already null)\n",
            |writer| super::write_blob_write_human(&output, writer),
        );
    }

    #[test]
    fn commit_json_stream_preserves_aggregate_shape_without_page_aggregation() {
        let cause = cause("commit-1");
        let first = omnigraph_api_types::CommitChangesOutput {
            cause: cause.clone(),
            changes: vec![change("a")],
            next_page_token: Some("continue".to_string()),
        };
        let second = omnigraph_api_types::CommitChangesOutput {
            cause,
            changes: vec![change("b")],
            next_page_token: None,
        };

        let mut stream = CommitChangesJsonStream::new(Vec::new());
        stream.write_page(&first).unwrap();
        stream.write_page(&second).unwrap();
        let bytes = stream.finish().unwrap();
        let expected = omnigraph_api_types::CommitChangesOutput {
            cause: first.cause.clone(),
            changes: vec![change("a"), change("b")],
            next_page_token: None,
        };
        assert_eq!(
            bytes,
            format!("{}\n", serde_json::to_string_pretty(&expected).unwrap()).into_bytes(),
            "incremental JSON remains byte-compatible with the previous pretty aggregate"
        );
        let rendered: omnigraph_api_types::CommitChangesOutput =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            rendered
                .changes
                .iter()
                .map(|change| change.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert!(rendered.next_page_token.is_none());

        let mut human = Vec::new();
        let mut stream = CommitChangesHumanStream::new(&mut human);
        stream.write_page(&first).unwrap();
        stream.write_page(&second).unwrap();
        assert_eq!(
            String::from_utf8(human).unwrap(),
            "commit: commit-1\nbranch: main\nparent: parent\nactor: act-test\nauthored_at: 42\n\
             insert node Person a\n  + name: a\ninsert node Person b\n  + name: b\n"
        );
        let mut single_page = Vec::new();
        super::write_commit_changes_human(&first, &mut single_page).unwrap();
        assert_eq!(
            String::from_utf8(single_page).unwrap(),
            "commit: commit-1\nbranch: main\nparent: parent\nactor: act-test\nauthored_at: 42\n\
             insert node Person a\n  + name: a\nnext_page_token: continue\n"
        );
    }

    #[test]
    fn feed_json_stream_stitches_only_the_open_split_block() {
        let first = omnigraph_api_types::ChangeFeedOutput {
            blocks: vec![omnigraph_api_types::ChangeBlockOutput {
                cause: cause("commit-1"),
                changes: vec![change("a")],
            }],
            next_page_token: Some("continue".to_string()),
            cursor: None,
            caught_up: None,
        };
        let second = omnigraph_api_types::ChangeFeedOutput {
            blocks: vec![
                omnigraph_api_types::ChangeBlockOutput {
                    cause: cause("commit-1"),
                    changes: vec![change("b")],
                },
                omnigraph_api_types::ChangeBlockOutput {
                    cause: cause("commit-2"),
                    changes: vec![change("c")],
                },
            ],
            next_page_token: None,
            cursor: Some("durable".to_string()),
            caught_up: Some(true),
        };

        let mut stream = ChangeFeedJsonStream::new(Vec::new());
        stream.write_page(&first).unwrap();
        stream.write_page(&second).unwrap();
        let bytes = stream
            .finish(second.cursor.as_deref(), second.caught_up)
            .unwrap();
        let expected = omnigraph_api_types::ChangeFeedOutput {
            blocks: vec![
                omnigraph_api_types::ChangeBlockOutput {
                    cause: cause("commit-1"),
                    changes: vec![change("a"), change("b")],
                },
                omnigraph_api_types::ChangeBlockOutput {
                    cause: cause("commit-2"),
                    changes: vec![change("c")],
                },
            ],
            next_page_token: None,
            cursor: Some("durable".to_string()),
            caught_up: Some(true),
        };
        assert_eq!(
            bytes,
            format!("{}\n", serde_json::to_string_pretty(&expected).unwrap()).into_bytes(),
            "incremental JSON remains byte-compatible with the previous pretty aggregate"
        );
        let rendered: omnigraph_api_types::ChangeFeedOutput =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(rendered.blocks.len(), 2);
        assert_eq!(
            rendered.blocks[0]
                .changes
                .iter()
                .map(|change| change.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(rendered.blocks[1].changes[0].id, "c");
        assert_eq!(rendered.cursor.as_deref(), Some("durable"));
        assert_eq!(rendered.caught_up, Some(true));

        let mut human = ChangeFeedHumanStream::new(Vec::new());
        human.write_page(&first).unwrap();
        human.write_page(&second).unwrap();
        let bytes = human
            .finish(second.cursor.as_deref(), second.caught_up)
            .unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "commit: commit-1\nbranch: main\nparent: parent\nactor: act-test\nauthored_at: 42\n\
             insert node Person a\n  + name: a\ninsert node Person b\n  + name: b\n\n\
             commit: commit-2\nbranch: main\nparent: parent\nactor: act-test\nauthored_at: 42\n\
             insert node Person c\n  + name: c\n\ncursor: durable\ncaught_up: true\n"
        );
        let empty = ChangeFeedHumanStream::new(Vec::new())
            .finish(Some("durable"), Some(true))
            .unwrap();
        assert_eq!(
            empty,
            b"(no new commits)\n\ncursor: durable\ncaught_up: true\n"
        );
    }

    #[test]
    fn diff_streams_keep_one_header_and_single_pages_keep_the_continuation() {
        let first = omnigraph_api_types::DiffOutput {
            from_commit_id: "from".to_string(),
            to_commit_id: "to".to_string(),
            base_commit_id: Some("base".to_string()),
            changes: vec![change("a")],
            next_page_token: Some("continue".to_string()),
        };
        let second = omnigraph_api_types::DiffOutput {
            changes: vec![change("b")],
            next_page_token: None,
            ..first.clone()
        };
        let mut json = DiffJsonStream::new(Vec::new());
        json.write_page(&first).unwrap();
        json.write_page(&second).unwrap();
        let aggregate = omnigraph_api_types::DiffOutput {
            changes: vec![change("a"), change("b")],
            next_page_token: None,
            ..first.clone()
        };
        assert_eq!(
            json.finish().unwrap(),
            format!("{}\n", serde_json::to_string_pretty(&aggregate).unwrap()).into_bytes()
        );

        let mut human = Vec::new();
        let mut stream = DiffHumanStream::new(&mut human);
        stream.write_page(&first).unwrap();
        stream.write_page(&second).unwrap();
        assert_eq!(
            String::from_utf8(human).unwrap(),
            "merge_base: base\nfrom: from\nto: to\n\
             insert node Person a\n  + name: a\ninsert node Person b\n  + name: b\n"
        );
        let mut single_page = Vec::new();
        super::write_diff_human(&first, &mut single_page).unwrap();
        assert_eq!(
            String::from_utf8(single_page).unwrap(),
            "merge_base: base\nfrom: from\nto: to\n\
             insert node Person a\n  + name: a\nnext_page_token: continue\n"
        );
    }

    #[derive(Clone, Copy, Debug)]
    enum WriterFault {
        AfterPrefix,
        Flush,
    }

    struct FailingWriter {
        fault: WriterFault,
        bytes: Vec<u8>,
        flushes: usize,
    }

    impl Write for FailingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let count = match self.fault {
                WriterFault::AfterPrefix => {
                    let remaining = 5 - self.bytes.len();
                    if remaining == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "history output closed",
                        ));
                    }
                    remaining.min(bytes.len())
                }
                WriterFault::Flush => bytes.len(),
            };
            self.bytes.extend_from_slice(&bytes[..count]);
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "history output closed",
            ))
        }
    }

    fn assert_writer_failure(
        label: &str,
        fault: WriterFault,
        write: impl FnOnce(&mut dyn Write) -> color_eyre::Result<()>,
    ) {
        let mut writer = FailingWriter {
            fault,
            bytes: Vec::new(),
            flushes: 0,
        };
        let error = write(&mut writer).unwrap_err();
        assert!(
            error.to_string().contains("history output closed"),
            "{label} {fault:?}: {error}"
        );
        match fault {
            WriterFault::AfterPrefix => {
                assert_eq!(writer.bytes.len(), 5, "{label}");
                assert_eq!(
                    writer.flushes, 0,
                    "a failed write must stop immediately: {label}"
                );
            }
            WriterFault::Flush => {
                assert!(!writer.bytes.is_empty(), "{label}");
                assert_eq!(
                    writer.flushes, 1,
                    "the page must flush before returning: {label}"
                );
            }
        }
    }

    #[test]
    fn history_stream_pages_propagate_partial_write_and_flush_failures() {
        let commit = omnigraph_api_types::CommitChangesOutput {
            cause: cause("commit-1"),
            changes: vec![change("a")],
            next_page_token: Some("continue".to_string()),
        };
        let diff = omnigraph_api_types::DiffOutput {
            from_commit_id: "from".to_string(),
            to_commit_id: "to".to_string(),
            base_commit_id: None,
            changes: commit.changes.clone(),
            next_page_token: commit.next_page_token.clone(),
        };
        let feed = omnigraph_api_types::ChangeFeedOutput {
            blocks: vec![omnigraph_api_types::ChangeBlockOutput {
                cause: commit.cause.clone(),
                changes: commit.changes.clone(),
            }],
            next_page_token: commit.next_page_token.clone(),
            cursor: None,
            caught_up: None,
        };
        for fault in [WriterFault::AfterPrefix, WriterFault::Flush] {
            assert_writer_failure("commit JSON", fault, |writer| {
                CommitChangesJsonStream::new(writer).write_page(&commit)
            });
            assert_writer_failure("commit human", fault, |writer| {
                CommitChangesHumanStream::new(writer).write_page(&commit)
            });
            assert_writer_failure("diff JSON", fault, |writer| {
                DiffJsonStream::new(writer).write_page(&diff)
            });
            assert_writer_failure("diff human", fault, |writer| {
                DiffHumanStream::new(writer).write_page(&diff)
            });
            assert_writer_failure("feed JSON", fault, |writer| {
                ChangeFeedJsonStream::new(writer).write_page(&feed)
            });
            assert_writer_failure("feed human", fault, |writer| {
                ChangeFeedHumanStream::new(writer).write_page(&feed)
            });
            assert_writer_failure("feed human terminal cursor", fault, |writer| {
                ChangeFeedHumanStream::new(writer)
                    .finish(Some("durable"), Some(true))
                    .map(|_| ())
            });
        }
    }
}
