//! Human/JSON output formatting for every command (moved verbatim from
//! main.rs in the modularization).

use super::*;
use std::fmt::Write as _;

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

fn stream_profile_mode_label(mode: StreamProfileModeOutput) -> &'static str {
    match mode {
        StreamProfileModeOutput::Disabled => "disabled",
        StreamProfileModeOutput::Enabled => "enabled",
        StreamProfileModeOutput::Disabling => "disabling",
        StreamProfileModeOutput::Retired => "retired",
    }
}

fn stream_lifecycle_label(lifecycle: StreamLifecycleOutput) -> &'static str {
    match lifecycle {
        StreamLifecycleOutput::Open => "open",
        StreamLifecycleOutput::Draining => "draining",
        StreamLifecycleOutput::Sealed => "sealed",
    }
}

fn stream_driver_state_label(state: StreamDriverStateOutput) -> &'static str {
    match state {
        StreamDriverStateOutput::Stopped => "stopped",
        StreamDriverStateOutput::Running => "running",
        StreamDriverStateOutput::Stopping => "stopping",
        StreamDriverStateOutput::Failed => "failed",
    }
}

fn stream_declaration_label(kind: StreamIngestKindOutput, type_name: &str) -> String {
    let kind = match kind {
        StreamIngestKindOutput::Node => "node",
        StreamIngestKindOutput::Edge => "edge",
    };
    format!("{kind} {type_name}")
}

fn render_stream_rebuild_blocker(rendered: &mut String, blocker: &StreamRebuildBlockerOutput) {
    let declaration =
        |kind: StreamIngestKindOutput, type_name: &str| stream_declaration_label(kind, type_name);
    let result = match blocker {
        StreamRebuildBlockerOutput::ProfileNotTerminal => {
            writeln!(rendered, "    - streaming profile is not terminal")
        }
        StreamRebuildBlockerOutput::DeclarationNotSealed { declaration: item } => writeln!(
            rendered,
            "    - {} is not sealed",
            declaration(item.kind, &item.type_name)
        ),
        StreamRebuildBlockerOutput::StrictBlock { declaration: item } => writeln!(
            rendered,
            "    - {} has a strict validation block",
            declaration(item.kind, &item.type_name)
        ),
        StreamRebuildBlockerOutput::PendingWork { declaration: item } => writeln!(
            rendered,
            "    - {} has pending work",
            declaration(item.kind, &item.type_name)
        ),
        StreamRebuildBlockerOutput::PendingWorkUnavailable { declaration: item } => writeln!(
            rendered,
            "    - {} pending work is unavailable",
            declaration(item.kind, &item.type_name)
        ),
        StreamRebuildBlockerOutput::RecoveryPending { count } => {
            writeln!(rendered, "    - {count} recovery operation(s) pending")
        }
        StreamRebuildBlockerOutput::TerminalTokenAuthority {
            withdrawn_count,
            dead_lettered_count,
        } => writeln!(
            rendered,
            "    - terminal sequencing authority: {withdrawn_count} withdrawn, \
             {dead_lettered_count} dead-lettered"
        ),
    };
    result.expect("writing stream status to a String cannot fail");
}

pub(crate) fn render_stream_status_human(output: &StreamStatusOutput) -> String {
    let mut rendered = String::new();
    writeln!(
        rendered,
        "stream profile: {} (revision {}, manifest {})",
        stream_profile_mode_label(output.profile_mode),
        output.profile_revision,
        output.manifest_version
    )
    .expect("writing stream status to a String cannot fail");
    if output.enrolled_declarations.is_empty() {
        writeln!(
            rendered,
            "  enrolled declarations: none (streaming state initializes lazily on first ingest)"
        )
        .expect("writing stream status to a String cannot fail");
    } else {
        writeln!(rendered, "  enrolled declarations:")
            .expect("writing stream status to a String cannot fail");
        for status in &output.enrolled_declarations {
            let declaration =
                stream_declaration_label(status.declaration.kind, &status.declaration.type_name);
            let pending = match &status.pending {
                StreamPendingStatusOutput::Exact {
                    rows,
                    arrow_bytes,
                    batches,
                } => format!("{rows} row(s), {arrow_bytes} Arrow byte(s), {batches} batch(es)"),
                StreamPendingStatusOutput::Unavailable {
                    cold_replay,
                    flushed,
                    recovery,
                } => {
                    let mut reasons = Vec::new();
                    if *cold_replay {
                        reasons.push("cold replay");
                    }
                    if *flushed {
                        reasons.push("flushed state");
                    }
                    if *recovery {
                        reasons.push("recovery");
                    }
                    if reasons.is_empty() {
                        "unavailable".to_string()
                    } else {
                        format!("unavailable ({})", reasons.join(", "))
                    }
                }
            };
            writeln!(
                rendered,
                "    {declaration}: {} (revision {}), pending {pending}",
                stream_lifecycle_label(status.lifecycle),
                status.lifecycle_revision
            )
            .expect("writing stream status to a String cannot fail");
            if let Some(block) = status.strict_block.as_ref() {
                writeln!(
                    rendered,
                    "      strict block: {} / {}",
                    block.kind, block.violation_code
                )
                .expect("writing stream status to a String cannot fail");
            }
            if let Some(drain) = status.drain.as_ref() {
                writeln!(rendered, "      drain: {} / {}", drain.goal, drain.phase)
                    .expect("writing stream status to a String cannot fail");
            }
            if let Some(fold) = status.last_fold.as_ref() {
                writeln!(
                    rendered,
                    "      last fold: {} ({} input row(s), {} visible row(s), recorded {})",
                    fold.outcome, fold.input_rows, fold.visible_rows, fold.recorded_at
                )
                .expect("writing stream status to a String cannot fail");
            }
        }
    }
    writeln!(
        rendered,
        "  sequencing authority: {} present, {} withdrawn, {} dead-lettered",
        output.token_counts.present,
        output.token_counts.withdrawn,
        output.token_counts.dead_lettered
    )
    .expect("writing stream status to a String cannot fail");
    writeln!(
        rendered,
        "  recovery: {} pending",
        output.recovery_pending_count
    )
    .expect("writing stream status to a String cannot fail");
    writeln!(
        rendered,
        "  driver: {} ({}; {} pending, {} open fold(s) published)",
        stream_driver_state_label(output.driver.state),
        if output.driver.authoritative {
            "authoritative"
        } else {
            "advisory"
        },
        output.driver.pending_count,
        output.driver.published_open_folds
    )
    .expect("writing stream status to a String cannot fail");
    if let Some(error) = output.driver.last_error.as_ref() {
        let result = match error.retry_in_ms {
            Some(retry_in_ms) => {
                writeln!(
                    rendered,
                    "    last error: {} (retry in {retry_in_ms} ms)",
                    error.kind
                )
            }
            None => writeln!(rendered, "    last error: {}", error.kind),
        };
        result.expect("writing stream status to a String cannot fail");
    }
    if let Some(kind) = output.driver.last_completion_kind.as_deref() {
        writeln!(rendered, "    last completion: {kind}")
            .expect("writing stream status to a String cannot fail");
    }
    if output.rebuild.ready {
        writeln!(rendered, "  rebuild: ready")
            .expect("writing stream status to a String cannot fail");
    } else {
        writeln!(rendered, "  rebuild: blocked")
            .expect("writing stream status to a String cannot fail");
        for blocker in &output.rebuild.blockers {
            render_stream_rebuild_blocker(&mut rendered, blocker);
        }
    }
    let next = if output.recovery_pending_count > 0 {
        "reopen/restart the graph to resolve recovery before another stream operation"
    } else if output
        .enrolled_declarations
        .iter()
        .any(|status| status.strict_block.is_some())
    {
        "stop serving, inspect the graph-level block token, correct it, then retry the control"
    } else if output
        .enrolled_declarations
        .iter()
        .any(|status| status.lifecycle == StreamLifecycleOutput::Draining)
    {
        "wait for the drain, or retry the stopped-cluster apply after correcting a block"
    } else {
        match output.profile_mode {
            StreamProfileModeOutput::Enabled
                if output
                    .enrolled_declarations
                    .iter()
                    .any(|status| status.lifecycle == StreamLifecycleOutput::Sealed) =>
            {
                "optionally run graph-wide `stream maintenance ensure-indices|optimize`, then `stream resume`"
            }
            StreamProfileModeOutput::Enabled => {
                "send graph rows with `omnigraph stream ingest`"
            }
            StreamProfileModeOutput::Disabling => {
                "stop serving and retry `omnigraph cluster apply` to finish disabling"
            }
            StreamProfileModeOutput::Disabled => {
                "set `streaming: true`, apply the cluster config, and restart the server"
            }
            StreamProfileModeOutput::Retired => "export and rebuild into a fresh graph",
        }
    };
    writeln!(rendered, "  next: {next}")
        .expect("writing stream status to a String cannot fail");
    rendered
}

pub(crate) fn print_stream_status_human(output: &StreamStatusOutput) {
    print!("{}", render_stream_status_human(output));
}

pub(crate) fn finish_stream_status(output: &StreamStatusOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)
    } else {
        print_stream_status_human(output);
        Ok(())
    }
}

pub(crate) fn finish_stream_resume(output: &StreamResumeOutput, json: bool) -> Result<()> {
    if json {
        return print_json(output);
    }
    println!(
        "stream resume: {} resumed, {} already open ({} enrolled; profile revision {})",
        output.resumed_declarations,
        output.already_open_declarations,
        output.enrolled_declarations,
        output.profile_revision
    );
    Ok(())
}

pub(crate) fn finish_stream_ensure_indices(
    output: &StreamEnsureIndicesOutput,
    json: bool,
) -> Result<()> {
    if json {
        return print_json(output);
    }
    println!(
        "stream index maintenance: {} ({} pending index(es))",
        if output.changed { "published" } else { "no change" },
        output.pending_index_count
    );
    Ok(())
}

pub(crate) fn finish_stream_optimize(output: &StreamOptimizeOutput, json: bool) -> Result<()> {
    if json {
        return print_json(output);
    }
    println!(
        "stream optimize: {} ({} pending index(es))",
        if output.changed { "published" } else { "no change" },
        output.pending_index_count
    );
    if output.requires_repair {
        println!("{}", stream_optimize_drift_advice());
    }
    Ok(())
}

fn stream_optimize_drift_advice() -> &'static str {
    concat!(
        "  uncovered storage drift needs offline review; stop serving and preserve the root\n",
        "  enrolled drift has no supported in-place repair; rebuild a fresh graph from the last verified clean export or backup",
    )
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

pub(crate) fn finish_stream_authority_retirement_plan(
    output: &StreamAuthorityRetirementPlanOutput,
    json: bool,
) -> Result<()> {
    if json {
        print_json(output)?;
    } else if let Some(plan) = output.plan.as_ref() {
        println!("stream authority retirement plan for {}", output.graph_id);
        println!("  plan digest: {}", plan.plan_digest);
        println!("  source manifest: {}", plan.source_manifest_version);
        println!(
            "  source profile revision: {}",
            plan.source_profile_revision
        );
        println!("  present tokens: {}", plan.present_token_count);
        println!("  withdrawn tokens: {}", plan.withdrawn_token_count);
        println!("  dead-lettered tokens: {}", plan.dead_lettered_token_count);
        println!("  export cut: {}", plan.export_cut_digest);
        println!("  no graph state changed; confirm this exact digest to retire");
        print_cluster_diagnostics(&output.diagnostics);
    } else {
        println!(
            "stream authority retirement plan failed for {}",
            output.graph_id
        );
        print_cluster_diagnostics(&output.diagnostics);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_stream_block_show(output: &StreamBlockShowOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else if let Some(page) = output.page.as_ref() {
        println!("stream data block for {}", output.graph_id);
        println!(
            "  declaration: {} {}",
            page.declaration.kind, page.declaration.type_name
        );
        println!("  block token: {}", page.block_token);
        println!("  lifecycle revision: {}", page.lifecycle_revision);
        println!("  correction view: {}", page.correction_view_digest);
        println!("  entries:");
        for entry in &page.entries {
            println!("    {}", serde_json::to_string(entry)?);
        }
        if let Some(cursor) = page.next_cursor.as_deref() {
            println!("  next cursor: {cursor}");
        }
        print_cluster_diagnostics(&output.diagnostics);
    } else {
        println!(
            "stream data-block inspection failed for {}",
            output.graph_id
        );
        print_cluster_diagnostics(&output.diagnostics);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_stream_dead_letter_list(
    output: &StreamDeadLetterListOutput,
    json: bool,
) -> Result<()> {
    if json {
        print_json(output)?;
    } else if let Some(page) = output.page.as_ref() {
        println!("current stream dead letters for {}", output.graph_id);
        println!("  source manifest: {}", page.source_manifest_version);
        println!(
            "  source profile revision: {}",
            page.source_profile_revision
        );
        println!("  entries:");
        for entry in &page.entries {
            println!("    {}", serde_json::to_string(entry)?);
        }
        if let Some(cursor) = page.next_cursor.as_deref() {
            println!("  next cursor: {cursor}");
        }
        print_cluster_diagnostics(&output.diagnostics);
    } else {
        println!("stream dead-letter listing failed for {}", output.graph_id);
        print_cluster_diagnostics(&output.diagnostics);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_stream_dead_letter_export(
    output: &StreamDeadLetterExportOutput,
    json: bool,
) -> Result<()> {
    if json {
        print_json(output)?;
    } else if let Some(page) = output.page.as_ref() {
        println!(
            "current stream dead-letter payloads for {}",
            output.graph_id
        );
        println!("  source manifest: {}", page.source_manifest_version);
        println!(
            "  source profile revision: {}",
            page.source_profile_revision
        );
        println!("  entries:");
        for entry in &page.entries {
            println!("    {}", serde_json::to_string(entry)?);
        }
        if let Some(cursor) = page.next_cursor.as_deref() {
            println!("  next cursor: {cursor}");
        }
        print_cluster_diagnostics(&output.diagnostics);
    } else {
        println!("stream dead-letter export failed for {}", output.graph_id);
        print_cluster_diagnostics(&output.diagnostics);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_stream_block_correct(
    output: &StreamBlockCorrectOutput,
    json: bool,
) -> Result<()> {
    if json {
        print_json(output)?;
    } else if let Some(result) = output.result.as_ref() {
        println!("stream data block corrected for {}", output.graph_id);
        println!("  correction id: {}", result.correction_id);
        println!("  plan digest: {}", result.plan_digest);
        println!("  graph commit: {}", result.graph_commit_id);
        println!("  lifecycle revision: {}", result.lifecycle_revision);
        println!("  manifest version: {}", result.manifest_version);
        println!("  changed: {}", result.changed);
        print_cluster_diagnostics(&output.diagnostics);
    } else {
        println!(
            "stream data-block correction failed for {}",
            output.graph_id
        );
        print_cluster_diagnostics(&output.diagnostics);
    }
    if !output.ok {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

pub(crate) fn finish_stream_authority_retirement_confirm(
    output: &StreamAuthorityRetirementConfirmOutput,
    json: bool,
) -> Result<()> {
    if json {
        print_json(output)?;
    } else if let Some(result) = output.result.as_ref() {
        println!("stream authority retired for {}", output.graph_id);
        println!("  retirement id: {}", result.retirement_id);
        println!("  plan digest: {}", result.plan_digest);
        println!("  export cut: {}", result.export_cut_digest);
        println!("  profile revision: {}", result.profile_revision);
        println!(
            "  dead-lettered tokens: {}",
            result.dead_lettered_token_count
        );
        println!(
            "  source is permanently read/query/status/export-only; rebuild into a fresh graph"
        );
        print_cluster_diagnostics(&output.diagnostics);
    } else {
        println!(
            "stream authority retirement confirmation failed for {}",
            output.graph_id
        );
        print_cluster_diagnostics(&output.diagnostics);
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
    use omnigraph_api_types::{
        StreamDriverErrorOutput, StreamDriverStateOutput, StreamDriverStatusOutput,
        StreamProfileModeOutput, StreamRebuildBlockerOutput, StreamRebuildStatusOutput,
        StreamStatusOutput, StreamTokenCountsOutput,
    };
    use omnigraph_compiler::schema::ast::Annotation;
    use omnigraph_compiler::schema::parser::parse_schema;
    use std::collections::BTreeMap;

    use super::{render_annotations, render_stream_status_human, stream_optimize_drift_advice};

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

    #[test]
    fn stream_status_human_output_stays_graph_logical() {
        let output = StreamStatusOutput {
            manifest_version: 42,
            profile_mode: StreamProfileModeOutput::Retired,
            profile_revision: 7,
            enrolled_declarations: Vec::new(),
            token_counts: StreamTokenCountsOutput {
                present: 3,
                withdrawn: 1,
                dead_lettered: 2,
            },
            recovery_pending_count: 4,
            driver: StreamDriverStatusOutput {
                scope: "graph".to_string(),
                authoritative: false,
                state: StreamDriverStateOutput::Failed,
                pending_count: 5,
                published_open_folds: 6,
                last_completion_kind: Some("folded".to_string()),
                last_error: Some(StreamDriverErrorOutput {
                    kind: "retryable".to_string(),
                    retry_in_ms: Some(250),
                }),
            },
            rebuild: StreamRebuildStatusOutput {
                ready: false,
                blockers: vec![StreamRebuildBlockerOutput::TerminalTokenAuthority {
                    withdrawn_count: 1,
                    dead_lettered_count: 2,
                }],
            },
        };

        let rendered = render_stream_status_human(&output);
        assert!(rendered.contains("stream profile: retired (revision 7, manifest 42)"));
        assert!(rendered.contains(
            "enrolled declarations: none (streaming state initializes lazily on first ingest)"
        ));
        assert!(
            rendered.contains("driver: failed (advisory; 5 pending, 6 open fold(s) published)")
        );
        assert!(rendered.contains("last completion: folded"));
        assert!(rendered.contains("terminal sequencing authority: 1 withdrawn, 2 dead-lettered"));
        assert!(rendered.contains("next: reopen/restart the graph to resolve recovery"));
        for forbidden in [
            "dataset",
            "table_key",
            "lane",
            "binding",
            "shard",
            "generation",
            "recovery_id",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "human output leaked private vocabulary: {forbidden}"
            );
        }
    }

    #[test]
    fn stream_optimize_drift_advice_does_not_promise_enrolled_repair() {
        let advice = stream_optimize_drift_advice();
        assert!(advice.contains("stop serving and preserve the root"));
        assert!(advice.contains("enrolled drift has no supported in-place repair"));
        assert!(advice.contains("rebuild a fresh graph"));
        assert!(!advice.contains("omnigraph repair"));
    }
}
