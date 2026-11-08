//! Historical Rust fixture oracle retained only for GQT parity tests.
use super::*;
pub async fn initialize_local_fixture(
    root_uri: &str,
    plan: &BranchMergePlan,
) -> BranchMergeResult<FixtureBuildSummary> {
    let preflight = plan.preflight()?;
    if root_uri.contains("://") && !root_uri.starts_with("file://") {
        return Err(unsupported(
            "invocation.root_uri",
            format!("local fixture initialization cannot use {root_uri:?}"),
        ));
    }
    let schema = schema_source(plan.tables);
    let db = Session::from_defaults(
        Arc::new(Omnigraph::init(root_uri, &schema).await.map_err(|error| {
            fixture_error(format!("initialize fixture at {root_uri}: {error}"))
        })?),
        SessionSettings::default(),
    );
    let base_load_commits = load_base(&db, plan).await?;
    if u64::try_from(base_load_commits).ok() != Some(preflight.base_load_commits) {
        return Err(fixture_error(format!(
            "builder-v3 base-load recipe drifted: preflight declared {} publications, execution produced {base_load_commits}",
            preflight.base_load_commits
        )));
    }
    let optimized_user_tables = match plan.compaction_recency {
        CompactionRecency::Optimized => {
            let outcomes = db
                .optimize()
                .await
                .map_err(|error| fixture_error(format!("optimize fixture main branch: {error}")))?;
            let intended_keys = (0..plan.node_tables())
                .map(node_table_key)
                .chain((0..plan.edge_tables()).map(edge_table_key));
            for key in intended_keys {
                let outcome = outcomes
                    .iter()
                    .find(|outcome| outcome.type_key == key)
                    .ok_or_else(|| {
                        fixture_error(format!(
                            "optimized fixture returned no outcome for intended user table {key}"
                        ))
                    })?;
                if outcome.skipped.is_some()
                    || !outcome.committed
                    || outcome.fragments_removed == 0
                    || outcome.fragments_added == 0
                {
                    return Err(fixture_error(format!(
                        "optimized fixture did not productively compact {key}: committed={}, fragments_removed={}, fragments_added={}, skipped={:?}",
                        outcome.committed,
                        outcome.fragments_removed,
                        outcome.fragments_added,
                        outcome.skipped
                    )));
                }
            }
            plan.tables
        }
        CompactionRecency::NotOptimized => 0,
    };

    prepare_reversible_updates(&db, plan).await?;

    db.branch_create_from(ReadTarget::branch(MAIN_BRANCH), SOURCE_BRANCH)
        .await
        .map_err(|error| fixture_error(format!("create {SOURCE_BRANCH}: {error}")))?;
    db.branch_create_from(ReadTarget::branch(MAIN_BRANCH), TARGET_BRANCH)
        .await
        .map_err(|error| fixture_error(format!("create {TARGET_BRANCH}: {error}")))?;

    let queries = mutation_queries(plan.diverged_tables);
    diverge(&db, SOURCE_BRANCH, Side::Source, plan, &queries).await?;
    diverge(&db, TARGET_BRANCH, Side::Target, plan, &queries).await?;

    let schema_shape = verified_schema_shape_json(&db, plan)?;
    let mut logical_digest = Sha256::new();
    logical_digest.update(LOGICAL_FIXTURE_DIGEST_DOMAIN);
    hash_logical_field(
        &mut logical_digest,
        b"schema-shape",
        schema_shape.as_bytes(),
    );
    hash_logical_field(&mut logical_digest, b"logical-index-inventory", b"[]");
    verify_branch(
        &db,
        MAIN_BRANCH,
        plan,
        BranchState::Main,
        Some(&mut logical_digest),
    )
    .await?;
    verify_branch(
        &db,
        SOURCE_BRANCH,
        plan,
        BranchState::Source,
        Some(&mut logical_digest),
    )
    .await?;
    verify_branch(
        &db,
        TARGET_BRANCH,
        plan,
        BranchState::Target,
        Some(&mut logical_digest),
    )
    .await?;
    let source_history_depth = u64::try_from(
        db.list_commits(Some(SOURCE_BRANCH))
            .await
            .map_err(|error| fixture_error(format!("list {SOURCE_BRANCH} commits: {error}")))?
            .len(),
    )
    .map_err(|_| fixture_error("source history depth does not fit u64"))?;
    let target_history_depth = u64::try_from(
        db.list_commits(Some(TARGET_BRANCH))
            .await
            .map_err(|error| fixture_error(format!("list {TARGET_BRANCH} commits: {error}")))?
            .len(),
    )
    .map_err(|_| fixture_error("target history depth does not fit u64"))?;
    if source_history_depth != plan.requested_history_depth
        || target_history_depth != plan.requested_history_depth
    {
        return Err(unsupported(
            "fixture.state.history_depth",
            format!(
                "requested exactly {} reachable commits per branch, but deterministic construction produced {source_history_depth} on {SOURCE_BRANCH} and {target_history_depth} on {TARGET_BRANCH}; builder v3 does not silently pad or squash history; declare the observed depth or revise the versioned deterministic builder contract",
                plan.requested_history_depth
            ),
        ));
    }
    let logical_content_sha256 = format!("{:x}", logical_digest.finalize());

    drop(db);
    Ok(FixtureBuildSummary {
        base_load_commits,
        optimized_user_tables,
        source_history_depth,
        target_history_depth,
        logical_content_sha256,
    })
}
