//! Human/JSON output formatting for every command (moved verbatim from
//! main.rs in the modularization).

use super::*;

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
}

pub(crate) fn load_output_from_tables(
    uri: &str,
    branch: &str,
    mode: &'static str,
    output: &IngestOutput,
) -> LoadOutput {
    let mut nodes_loaded = 0;
    let mut edges_loaded = 0;
    let mut node_types_loaded = 0;
    let mut edge_types_loaded = 0;
    for table in &output.tables {
        if table.table_key.starts_with("node:") {
            nodes_loaded += table.rows_loaded;
            node_types_loaded += 1;
        } else if table.table_key.starts_with("edge:") {
            edges_loaded += table.rows_loaded;
            edge_types_loaded += 1;
        }
    }
    LoadOutput {
        uri: uri.to_string(),
        branch: branch.to_string(),
        mode,
        base_branch: output.base_branch.clone(),
        branch_created: output.branch_created,
        nodes_loaded,
        edges_loaded,
        node_types_loaded,
        edge_types_loaded,
    }
}

/// The local arm's twin of `load_output_from_tables`: build the same
/// `LoadOutput` from the engine `LoadResult` directly (the remote arm only
/// has the wire `IngestOutput`'s table list; the local arm has the full
/// result). Both load mappings live here, next to the struct — RFC-009
/// Phase 2's "one place" for the `-> LoadOutput` mapping that used to fork
/// between this file and main.rs's inline construction.
pub(crate) fn load_output_from_result(
    uri: &str,
    branch: &str,
    mode: &'static str,
    result: &omnigraph::loader::LoadResult,
) -> LoadOutput {
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
    println!("manifest_version: {}", output.manifest_version);
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
        println!("{} rows_loaded={}", table.table_key, table.rows_loaded);
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
        "embedded {} rows (selected {}, cleaned {}) from {} -> {} [{} {}d]",
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
    println!("branch: {}", branch);
    println!("manifest_version: {}", manifest_version);
    println!("internal_schema_version: {}", internal_schema_version);
    for entry in entries {
        println!(
            "{} v{} branch={} rows={}",
            entry.table_key,
            entry.table_version,
            entry.table_branch.as_deref().unwrap_or("main"),
            entry.row_count
        );
    }
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
            "{} branch={} version={}{}",
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
        "manifest_branch: {}",
        commit.manifest_branch.as_deref().unwrap_or("main")
    );
    println!("manifest_version: {}", commit.manifest_version);
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

#[cfg(test)]
mod tests {
    use omnigraph_compiler::schema::ast::Annotation;
    use omnigraph_compiler::schema::parser::parse_schema;
    use std::collections::BTreeMap;

    use super::render_annotations;

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
}
