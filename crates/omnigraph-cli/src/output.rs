//! Human/JSON output formatting for every command (moved verbatim from
//! main.rs in the modularization).

use super::*;

pub(crate) fn graph_type_subject(table_key: &str) -> String {
    if let Some(type_name) = table_key.strip_prefix("node:") {
        format!("node type '{type_name}'")
    } else if let Some(type_name) = table_key.strip_prefix("edge:") {
        format!("edge type '{type_name}'")
    } else {
        format!("dataset '{table_key}'")
    }
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
    pub(crate) nodes_loaded: usize,
    pub(crate) edges_loaded: usize,
    pub(crate) node_types_loaded: usize,
    pub(crate) edge_types_loaded: usize,
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
        nodes_loaded: output.nodes.iter().map(|entry| entry.rows_loaded).sum(),
        edges_loaded: output.edges.iter().map(|entry| entry.rows_loaded).sum(),
        node_types_loaded: output.nodes.len(),
        edge_types_loaded: output.edges.len(),
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
) -> LoadOutput {
    let result = &receipt.result;
    LoadOutput {
        uri: uri.to_string(),
        branch: branch.to_string(),
        mode,
        base_branch: result.base_branch.clone(),
        branch_created: result.branch_created,
        nodes_loaded: result.nodes_loaded.values().sum(),
        edges_loaded: result.edges_loaded.values().sum(),
        node_types_loaded: result.nodes_loaded.len(),
        edge_types_loaded: result.edges_loaded.len(),
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

pub(crate) fn print_schema_apply_human(output: &SchemaApplyOutput) {
    println!("schema apply for {}", output.uri);
    println!("supported: {}", if output.supported { "yes" } else { "no" });
    println!("applied: {}", if output.applied { "yes" } else { "no" });
    println!("graph_manifest_version: {}", output.manifest_version);
    if output.steps.is_empty() {
        println!("no schema changes");
        return;
    }
    for step in &output.steps {
        println!("- {}", render_schema_plan_step(step));
    }
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

pub(crate) fn print_query_lint_human(output: &QueryLintOutput) {
    for result in &output.results {
        match result.status {
            QueryLintStatus::Ok => {
                println!(
                    "OK    query `{}` ({})",
                    result.name,
                    query_kind_label(result.kind)
                );
            }
            QueryLintStatus::Error => {
                println!(
                    "ERROR query `{}`: {}",
                    result.name,
                    result.error.as_deref().unwrap_or("unknown error")
                );
            }
        }

        for warning in &result.warnings {
            println!("WARN  query `{}`: {}", result.name, warning);
        }
    }

    for finding in &output.findings {
        println!("{} {}", severity_label(finding.severity), finding.message);
    }

    println!(
        "INFO  Lint complete: {} queries processed ({} error(s), {} warning(s), {} info item(s))",
        output.queries_processed, output.errors, output.warnings, output.infos
    );
}

pub(crate) fn finish_query_lint(output: &QueryLintOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else {
        print_query_lint_human(output);
    }

    if output.status == QueryLintStatus::Error {
        io::stdout().flush()?;
        std::process::exit(1);
    }

    Ok(())
}

pub(crate) fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub(crate) fn print_cluster_validate_human(output: &ValidateOutput) {
    if output.ok {
        println!(
            "cluster config valid: {} resource(s), {} dependency edge(s)",
            output.resources.len(),
            output.dependencies.len()
        );
    } else {
        println!("cluster config invalid");
    }
    print_cluster_diagnostics(&output.diagnostics);
}

pub(crate) fn print_cluster_plan_human(output: &PlanOutput) {
    if output.ok {
        println!(
            "cluster plan: {} change(s), {} approval gate(s)",
            output.changes.len(),
            output.approvals_required.len()
        );
        for change in &output.changes {
            let bindings = if change.binding_change {
                " [bindings]"
            } else {
                ""
            };
            println!("  {:?} {}{bindings}", change.operation, change.resource);
            if let Some(migration) = &change.migration {
                if !migration.supported {
                    println!("      migration UNSUPPORTED:");
                }
                for step in &migration.steps {
                    println!(
                        "      {}",
                        serde_json::to_string(step).unwrap_or_else(|_| format!("{step:?}"))
                    );
                }
            }
        }
        if output.changes.is_empty() {
            println!("  no changes");
        }
    } else {
        println!("cluster plan failed");
    }
    print_cluster_diagnostics(&output.diagnostics);
}

pub(crate) fn print_cluster_apply_human(output: &ApplyOutput) {
    if output.ok {
        println!(
            "cluster apply: {} applied, {} deferred/blocked",
            output.applied_count, output.deferred_count
        );
    } else {
        println!("cluster apply failed");
    }
    // The change list prints on failure too: an operator debugging a partial
    // apply (payload or state-write error) needs to see what was attempted.
    print_cluster_apply_changes(&output.changes);
    if output.ok {
        let state = &output.state_observations;
        println!(
            "  state: revision {}, converged: {}, written: {}",
            state.state_revision, output.converged, output.state_written
        );
        println!(
            "  note: cluster-booted servers (--cluster) serve this on their next restart; omnigraph.yaml deployments are unaffected"
        );
    }
    print_cluster_diagnostics(&output.diagnostics);
}

pub(crate) fn print_cluster_apply_changes(changes: &[omnigraph_cluster::PlanChange]) {
    for change in changes {
        let bindings = if change.binding_change {
            " [bindings]"
        } else {
            ""
        };
        match (&change.disposition, change.reason.as_deref()) {
            (Some(disposition), Some(reason)) => println!(
                "  {:?} {}{bindings} [{disposition:?}: {reason}]",
                change.operation, change.resource
            ),
            (Some(disposition), None) => println!(
                "  {:?} {}{bindings} [{disposition:?}]",
                change.operation, change.resource
            ),
            _ => println!("  {:?} {}{bindings}", change.operation, change.resource),
        }
    }
    if changes.is_empty() {
        println!("  no changes");
    }
}

pub(crate) fn print_cluster_status_human(output: &StatusOutput) {
    if output.ok {
        let state = &output.state_observations;
        if state.state_found {
            println!(
                "cluster state: revision {}, {} resource(s)",
                state.state_revision, state.resource_count
            );
            if let Some(digest) = state.applied_config_digest.as_deref() {
                println!("  applied config: {digest}");
            }
            if state.locked {
                println!("  lock: held{}", cluster_lock_summary(state));
            } else {
                println!("  lock: not held");
            }
        } else {
            println!("cluster state missing");
        }
    } else {
        println!("cluster status failed");
    }
    print_cluster_diagnostics(&output.diagnostics);
}

pub(crate) fn print_cluster_state_sync_human(output: &StateSyncOutput) {
    let operation = match output.operation {
        omnigraph_cluster::StateSyncOperation::Refresh => "refresh",
        omnigraph_cluster::StateSyncOperation::Import => "import",
    };
    if output.ok {
        let state = &output.state_observations;
        println!(
            "cluster {operation}: revision {}, {} resource(s)",
            state.state_revision, state.resource_count
        );
        if let Some(cas) = state.state_cas.as_deref() {
            println!("  state_cas: {cas}");
        }
        if state.locked {
            println!("  lock: acquired{}", cluster_lock_summary(state));
        } else {
            println!("  lock: not acquired");
        }
    } else {
        println!("cluster {operation} failed");
    }
    print_cluster_diagnostics(&output.diagnostics);
}

pub(crate) fn print_cluster_force_unlock_human(output: &ForceUnlockOutput) {
    if output.ok {
        if output.lock_removed {
            println!(
                "cluster force-unlock: removed lock{}",
                cluster_lock_summary(&output.state_observations)
            );
        } else {
            println!("cluster force-unlock: no lock removed");
        }
    } else {
        println!("cluster force-unlock failed");
        if output.state_observations.locked {
            println!(
                "  lock: held{}",
                cluster_lock_summary(&output.state_observations)
            );
        }
    }
    print_cluster_diagnostics(&output.diagnostics);
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

pub(crate) fn print_cluster_diagnostics(diagnostics: &[omnigraph_cluster::Diagnostic]) {
    for diagnostic in diagnostics {
        let label = match diagnostic.severity {
            DiagnosticSeverity::Error => "ERROR",
            DiagnosticSeverity::Warning => "WARN ",
        };
        println!(
            "{label} {} {}: {}",
            diagnostic.code, diagnostic.path, diagnostic.message
        );
    }
}

pub(crate) fn finish_cluster_validate(output: &ValidateOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else {
        print_cluster_validate_human(output);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_cluster_plan(output: &PlanOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else {
        print_cluster_plan_human(output);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_cluster_apply(output: &ApplyOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else {
        print_cluster_apply_human(output);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_cluster_approve(output: &ApproveOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else if output.ok {
        println!(
            "cluster approve: {} {} approved by {} (approval {})",
            output
                .operation
                .as_ref()
                .map(|operation| format!("{operation:?}").to_lowercase())
                .unwrap_or_default(),
            output.resource.as_deref().unwrap_or("?"),
            output.approved_by.as_deref().unwrap_or("?"),
            output.approval_id.as_deref().unwrap_or("?"),
        );
        print_cluster_diagnostics(&output.diagnostics);
    } else {
        println!("cluster approve failed");
        print_cluster_diagnostics(&output.diagnostics);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_cluster_status(output: &StatusOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else {
        print_cluster_status_human(output);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_cluster_state_sync(output: &StateSyncOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else {
        print_cluster_state_sync_human(output);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_cluster_force_unlock(output: &ForceUnlockOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else {
        print_cluster_force_unlock_human(output);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn print_load_human(payload: &LoadOutput) {
    println!(
        "loaded {} on branch {} with {}: {} nodes across {} node types, {} edges across {} edge types",
        payload.uri,
        payload.branch,
        payload.mode,
        payload.nodes_loaded,
        payload.node_types_loaded,
        payload.edges_loaded,
        payload.edge_types_loaded
    );
    if payload.branch_created {
        if let Some(base) = &payload.base_branch {
            println!("branch {} created from {}", payload.branch, base);
        }
    }
}

pub(crate) fn print_ingest_human(output: &IngestOutput) {
    println!(
        "ingested {} into branch {} from {} with {} ({})",
        output.uri,
        output.branch,
        output.base_branch.as_deref().unwrap_or("main"),
        output.mode.as_str(),
        if output.branch_created {
            "branch created"
        } else {
            "branch exists"
        }
    );
    for table in &output.tables {
        println!(
            "{}: {} entities loaded",
            graph_type_subject(&table.table_key),
            table.rows_loaded
        );
    }
    if let Some(actor_id) = &output.actor_id {
        println!("actor_id: {}", actor_id);
    }
}

pub(crate) fn print_schema_plan_human(uri: &str, plan: &SchemaMigrationPlan) {
    println!("schema plan for {}", uri);
    println!("supported: {}", if plan.supported { "yes" } else { "no" });
    if plan.steps.is_empty() {
        println!("no schema changes");
        return;
    }
    for step in &plan.steps {
        println!("- {}", render_schema_plan_step(step));
    }
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
            render_constraint(constraint),
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
        SchemaMigrationStep::DropType {
            type_kind,
            name,
            mode,
        } => format!(
            "drop {} type '{}' ({} mode)",
            schema_type_kind_label(*type_kind),
            name,
            drop_mode_label(*mode),
        ),
        SchemaMigrationStep::DropProperty {
            type_kind,
            type_name,
            property_name,
            mode,
        } => format!(
            "drop property '{}.{}' of {} '{}' ({} mode)",
            type_name,
            property_name,
            schema_type_kind_label(*type_kind),
            type_name,
            drop_mode_label(*mode),
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

pub(crate) fn drop_mode_label(mode: omnigraph_compiler::DropMode) -> &'static str {
    match mode {
        omnigraph_compiler::DropMode::Soft => "soft",
        omnigraph_compiler::DropMode::Hard => "hard",
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

pub(crate) fn render_constraint(
    constraint: &omnigraph_compiler::schema::ast::Constraint,
) -> String {
    match constraint {
        omnigraph_compiler::schema::ast::Constraint::Key(columns) => {
            format!("@key({})", columns.join(", "))
        }
        omnigraph_compiler::schema::ast::Constraint::Unique(columns) => {
            format!("@unique({})", columns.join(", "))
        }
        omnigraph_compiler::schema::ast::Constraint::Index(columns) => {
            format!("@index({})", columns.join(", "))
        }
        omnigraph_compiler::schema::ast::Constraint::Range { property, min, max } => {
            format!("@range({}, {:?}, {:?})", property, min, max)
        }
        omnigraph_compiler::schema::ast::Constraint::Check { property, pattern } => {
            format!("@check({}, {:?})", property, pattern)
        }
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

pub(crate) fn print_embed_human(output: &EmbedOutput) {
    println!(
        "embedded {} records (selected {}, cleaned {}) from {} -> {} [{} {}d]",
        output.embedded_rows,
        output.selected_rows,
        output.cleaned_rows,
        output.input,
        output.output,
        output.mode,
        output.dimension
    );
}

pub(crate) fn print_snapshot_human(
    branch: &str,
    manifest_version: u64,
    internal_schema_version: u32,
    entries: &[SnapshotTableOutput],
) {
    println!("graph_branch: {}", branch);
    println!("graph_manifest_version: {}", manifest_version);
    println!("internal_schema_version: {}", internal_schema_version);
    for entry in entries {
        println!(
            "{} published_dataset_version={} native_dataset_branch={} entities={}",
            graph_type_subject(&entry.table_key),
            entry.table_version,
            entry.table_branch.as_deref().unwrap_or("main"),
            entry.row_count
        );
    }
}

pub(crate) fn print_blob_stat_human(output: &BlobStatOutput) {
    let entity = match output.selector.entity {
        omnigraph_api_types::BlobEntityKind::Node => "node",
        omnigraph_api_types::BlobEntityKind::Edge => "edge",
    };
    println!("entity: {entity}");
    println!("type: {}", output.selector.r#type);
    println!("id: {}", output.selector.id);
    println!("property: {}", output.selector.property);
    match output.kind {
        BlobContentKindOutput::Managed => {
            println!("kind: managed");
            if let Some(size) = output.size {
                println!("size: {size}");
            }
            if let Some(etag) = output.etag.as_deref() {
                println!("etag: {etag}");
            }
        }
        BlobContentKindOutput::External => {
            println!("kind: external");
            if let Some(uri) = output.uri.as_deref() {
                println!("uri: {uri}");
            }
        }
    }
    if let Some(branch) = output.target.branch.as_deref() {
        println!("branch: {branch}");
    }
    if let Some(snapshot) = output.target.snapshot.as_deref() {
        println!("snapshot: {snapshot}");
    }
    println!("resolved_snapshot: {}", output.target.resolved_snapshot);
}

pub(crate) fn print_read_output(output: &ReadOutput, format: ReadOutputFormat) -> Result<()> {
    println!(
        "{}",
        render_read(output, format, &resolve_table_render_options())?
    );
    Ok(())
}

pub(crate) fn print_change_human(output: &ChangeOutput) {
    println!(
        "changed {} via {}: {} nodes, {} edges",
        output.branch, output.query_name, output.affected_nodes, output.affected_edges
    );
    if let Some(actor_id) = &output.actor_id {
        println!("actor_id: {}", actor_id);
    }
}

pub(crate) fn print_commit_list_human(commits: &[CommitOutput]) {
    for commit in commits {
        let branch = commit.manifest_branch.as_deref().unwrap_or("main");
        println!(
            "{} graph_branch={} graph_manifest_version={}{}",
            commit.graph_commit_id,
            branch,
            commit.manifest_version,
            commit
                .actor_id
                .as_deref()
                .map(|actor| format!(" actor={}", actor))
                .unwrap_or_default()
        );
    }
}

pub(crate) fn print_commit_human(commit: &CommitOutput) {
    println!("graph_commit_id: {}", commit.graph_commit_id);
    println!(
        "graph_branch: {}",
        commit.manifest_branch.as_deref().unwrap_or("main")
    );
    println!("graph_manifest_version: {}", commit.manifest_version);
    if let Some(parent_commit_id) = &commit.parent_commit_id {
        println!("parent_commit_id: {}", parent_commit_id);
    }
    if let Some(merged_parent_commit_id) = &commit.merged_parent_commit_id {
        println!("merged_parent_commit_id: {}", merged_parent_commit_id);
    }
    if let Some(actor_id) = &commit.actor_id {
        println!("actor_id: {}", actor_id);
    }
    println!("created_at: {}", commit.created_at);
}

pub(crate) fn print_policy_explain(
    decision: &PolicyDecision,
    actor_id: &str,
    request: &PolicyRequest,
) {
    println!(
        "decision: {}",
        if decision.allowed { "allow" } else { "deny" }
    );
    println!("actor: {}", actor_id);
    println!("action: {}", request.action);
    if let Some(branch) = &request.branch {
        println!("branch: {}", branch);
    }
    if let Some(target_branch) = &request.target_branch {
        println!("target_branch: {}", target_branch);
    }
    if let Some(rule_id) = &decision.matched_rule_id {
        println!("matched_rule: {}", rule_id);
    }
    println!("message: {}", decision.message);
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesIssue {
    pub(crate) query: String,
    pub(crate) message: String,
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesValidateOutput {
    pub(crate) ok: bool,
    pub(crate) breakages: Vec<QueriesIssue>,
    pub(crate) warnings: Vec<QueriesIssue>,
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesParam {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) type_name: String,
    pub(crate) nullable: bool,
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesListItem {
    pub(crate) name: String,
    pub(crate) mcp_expose: bool,
    pub(crate) tool_name: Option<String>,
    pub(crate) mutation: bool,
    /// `@description` from the query declaration — what the query is for.
    /// Carried so the CLI catalog matches the HTTP `GET /queries` surface.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// `@instruction` from the query declaration — how/when to invoke it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) instruction: Option<String>,
    pub(crate) params: Vec<QueriesParam>,
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesListOutput {
    pub(crate) queries: Vec<QueriesListItem>,
}

pub(crate) fn finish_login(
    server: &str,
    credentials_path: &std::path::Path,
    declared: bool,
    json: bool,
) -> Result<()> {
    if json {
        print_json(&serde_json::json!({
            "server": server,
            "credentials_path": credentials_path.display().to_string(),
            "declared": declared,
        }))?;
    } else {
        println!(
            "stored credential for '{server}' in {}",
            credentials_path.display()
        );
    }
    if !declared {
        eprintln!(
            "note: '{server}' is not declared under servers: in the operator config; the token applies once you add `servers:\n  {server}:\n    url: <server url>` to ~/.omnigraph/config.yaml"
        );
    }
    Ok(())
}

pub(crate) fn finish_logout(
    server: &str,
    credentials_path: &std::path::Path,
    json: bool,
) -> Result<()> {
    if json {
        print_json(&serde_json::json!({
            "server": server,
            "credentials_path": credentials_path.display().to_string(),
        }))?;
    } else {
        println!(
            "removed credential for '{server}' from {}",
            credentials_path.display()
        );
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub(crate) struct ProfileListItem {
    pub(crate) name: String,
    /// `server: <n>` / `cluster: <n>` / `store: <uri>` / `invalid: <reason>`.
    pub(crate) binding: String,
    /// `server` | `cluster` | `store` | `invalid`.
    pub(crate) scope_kind: String,
    /// The bound server/cluster name, or the store URI. `None` when invalid.
    pub(crate) target: Option<String>,
    pub(crate) valid: bool,
    pub(crate) error: Option<String>,
    pub(crate) default_graph: Option<String>,
    pub(crate) active: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProfileDetail {
    /// Profile name, or `(defaults)` for the no-name flat-defaults view.
    pub(crate) name: String,
    /// `server` | `cluster` | `store` | `none`.
    pub(crate) scope_kind: String,
    /// The bound server/cluster name, or the store URI.
    pub(crate) target: Option<String>,
    /// Resolved endpoint: a server's URL / a cluster's root / the store URI;
    /// `None` if a named server/cluster isn't defined in this config.
    pub(crate) endpoint: Option<String>,
    pub(crate) default_graph: Option<String>,
    pub(crate) output_format: Option<String>,
}

pub(crate) fn print_profile_list(items: &[ProfileListItem], json: bool) -> Result<()> {
    if json {
        return print_json(&items);
    }
    if items.is_empty() {
        println!("no profiles defined in the operator config");
        return Ok(());
    }
    for item in items {
        let active = if item.active { " (active)" } else { "" };
        let graph = item
            .default_graph
            .as_deref()
            .map(|g| format!(" · graph: {g}"))
            .unwrap_or_default();
        println!("{}{active}  {}{graph}", item.name, item.binding);
    }
    Ok(())
}

pub(crate) fn print_profile_detail(detail: &ProfileDetail, json: bool) -> Result<()> {
    if json {
        return print_json(detail);
    }
    println!("profile: {}", detail.name);
    let target = detail
        .target
        .as_deref()
        .map(|t| format!(" {t}"))
        .unwrap_or_default();
    println!("  scope:   {}{target}", detail.scope_kind);
    if let Some(endpoint) = &detail.endpoint {
        println!("  endpoint: {endpoint}");
    } else if matches!(detail.scope_kind.as_str(), "server" | "cluster") {
        println!("  endpoint: (undefined — name not in this config)");
    }
    if let Some(graph) = &detail.default_graph {
        println!("  default graph: {graph}");
    }
    if let Some(format) = &detail.output_format {
        println!("  output: {format}");
    }
    Ok(())
}

/// Table prefs cascade (RFC-011): operator defaults.table_* > built-in.
pub(crate) fn resolve_table_render_options() -> ReadRenderOptions {
    let operator = crate::operator::load_operator_config().unwrap_or_default();
    ReadRenderOptions {
        max_column_width: operator.defaults.table_max_column_width.unwrap_or(80),
        cell_layout: operator.defaults.table_cell_layout.unwrap_or_default(),
    }
}

pub(crate) fn print_change_cause_human(cause: &omnigraph_api_types::ChangeCauseOutput) {
    println!("commit: {}", cause.graph_commit_id);
    println!("branch: {}", cause.authored_branch);
    if let Some(parent_commit_id) = &cause.parent_commit_id {
        println!("parent: {}", parent_commit_id);
    }
    if let Some(merged_parent_commit_id) = &cause.merged_parent_commit_id {
        println!("merged_parent: {}", merged_parent_commit_id);
    }
    if let Some(actor_id) = &cause.actor_id {
        println!("actor: {}", actor_id);
    }
    println!("authored_at: {}", cause.authored_at);
}

fn print_entity_change_row(change: &omnigraph_api_types::EntityChangeOutput) {
    println!(
        "{} {} {} {}",
        change.op.as_str(),
        change.kind.as_str(),
        change.r#type.name,
        change.id
    );
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
        println!("  {rendered}");
    }
    // The before/after property values are the point of a change feed; the
    // previous output dropped them. Show inserted/deleted values, or the
    // changed keys of an update as `before -> after`.
    match (&change.before, &change.after) {
        (None, Some(after)) => print_change_properties("  +", &after.properties),
        (Some(before), None) => print_change_properties("  -", &before.properties),
        (Some(before), Some(after)) => print_change_property_diff(before, after),
        (None, None) => {}
    }
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

fn print_change_properties(marker: &str, properties: &serde_json::Value) {
    if let Some(map) = properties.as_object() {
        for (key, value) in map {
            println!("{marker} {key}: {}", render_change_value(value));
        }
    }
}

fn print_change_property_diff(
    before: &omnigraph_api_types::ChangeImageOutput,
    after: &omnigraph_api_types::ChangeImageOutput,
) {
    let (Some(before_map), Some(after_map)) =
        (before.properties.as_object(), after.properties.as_object())
    else {
        return;
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
            println!("  {key}: {before_str} -> {after_str}");
        }
    }
}

pub(crate) fn print_commit_changes_human(page: &omnigraph_api_types::CommitChangesOutput) {
    print_change_cause_human(&page.cause);
    for change in &page.changes {
        print_entity_change_row(change);
    }
    if let Some(next_page_token) = &page.next_page_token {
        println!("next_page_token: {}", next_page_token);
    }
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
pub(crate) struct CommitChangesHumanStream {
    cause: Option<omnigraph_api_types::ChangeCauseOutput>,
}

impl CommitChangesHumanStream {
    pub(crate) fn new() -> Self {
        Self { cause: None }
    }

    pub(crate) fn write_page(
        &mut self,
        page: &omnigraph_api_types::CommitChangesOutput,
    ) -> Result<()> {
        match &self.cause {
            None => {
                print_change_cause_human(&page.cause);
                self.cause = Some(page.cause.clone());
            }
            Some(cause) if cause == &page.cause => {}
            Some(_) => bail!("commit changes continuation changed its cause while auto-paginating"),
        }
        for change in &page.changes {
            print_entity_change_row(change);
        }
        io::stdout().flush()?;
        Ok(())
    }
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
/// still reads as one block.
pub(crate) struct ChangeFeedHumanStream {
    current_cause: Option<omnigraph_api_types::ChangeCauseOutput>,
    wrote_block: bool,
}

impl ChangeFeedHumanStream {
    pub(crate) fn new() -> Self {
        Self {
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
                    println!();
                }
                print_change_cause_human(&block.cause);
                self.current_cause = Some(block.cause.clone());
                self.wrote_block = true;
            }
            for change in &block.changes {
                print_entity_change_row(change);
            }
        }
        io::stdout().flush()?;
        Ok(())
    }

    pub(crate) fn finish(self, cursor: Option<&str>, caught_up: Option<bool>) {
        if !self.wrote_block {
            println!("(no new commits)");
        }
        if let Some(cursor) = cursor {
            println!();
            println!("cursor: {cursor}");
            if let Some(caught_up) = caught_up {
                println!("caught_up: {caught_up}");
            }
        }
    }
}

pub(crate) fn print_change_baseline_human(
    baseline: &omnigraph_api_types::ChangeBaselineOutput,
    out_path: &std::path::Path,
) {
    println!("snapshot: {}", out_path.display());
    println!("snapshot_commit_id: {}", baseline.snapshot_commit_id);
    println!("resume_cursor: {}", baseline.resume_cursor);
}

#[cfg(test)]
mod tests {
    use omnigraph_compiler::schema::ast::Annotation;
    use omnigraph_compiler::schema::parser::parse_schema;
    use std::collections::BTreeMap;

    use super::{
        ChangeFeedJsonStream, CommitChangesJsonStream, graph_type_subject, render_annotations,
    };

    #[test]
    fn graph_type_subject_hides_internal_table_key_syntax() {
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
            kind: omnigraph_api_types::ChangeEntityKind::Node,
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
    }
}
