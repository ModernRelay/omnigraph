//! Typed workload administration through the existing organization transport.
use super::*;
use crate::managed::args::OrganizationWorkloadsCommand as Command;

const PATH: &str = "/v1/organization/workloads";
const EXISTING_CREDENTIAL_WINDOW: u64 = 330;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Patch {
    version: u32,
    organization_id: String,
    client_id: String,
    action: String,
    expected_grant_version: i64,
    #[serde(default)]
    add: Changes,
    #[serde(default)]
    remove: Changes,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Workload {
    issuer: String,
    client_id: String,
    application_id: String,
    principal_id: String,
    name: String,
    enabled: bool,
    grant_version: i64,
    scopes: Scopes,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Directory {
    organization_id: String,
    workloads: Vec<Workload>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    organization_id: String,
    action: String,
    previous_grant_version: i64,
    workload: Workload,
    provider_changed: bool,
    graph_policy_changed: bool,
    maximum_existing_native_credential_lifetime_seconds: u64,
}

fn client_id(value: &str) -> bool {
    value.starts_with("client_") && identity(value)
}

pub(super) fn key(command: &Command) -> Option<&str> {
    match command {
        Command::List => None,
        Command::Enroll {
            idempotency_key, ..
        }
        | Command::Update {
            idempotency_key, ..
        }
        | Command::Revoke {
            idempotency_key, ..
        } => Some(idempotency_key),
    }
}

fn invalid_patch() -> Failure {
    Failure::refused(
        "workload_patch_invalid",
        "workload patch must be a regular YAML file of at most 256 KiB with version 1, exact organization/client IDs, the selected action, current grant version and exact grant changes; workloads cannot receive members_manage",
    )
}

pub(super) fn prepare(command: &Command, organization: &str) -> Result<Option<Patch>> {
    let patch: Patch = match command {
        Command::List => return Ok(None),
        Command::Enroll { file, .. } | Command::Update { file, .. } => {
            read_patch_document(file, invalid_patch)?
        }
        Command::Revoke {
            client_id,
            expected_version,
            ..
        } => Patch {
            version: 1,
            organization_id: organization.into(),
            client_id: client_id.clone(),
            action: "revoke".into(),
            expected_grant_version: *expected_version,
            add: Changes::default(),
            remove: Changes::default(),
        },
    };
    let action = match command {
        Command::Enroll { .. } => "enroll",
        Command::Update { .. } => "update",
        Command::Revoke { .. } => "revoke",
        Command::List => unreachable!(),
    };
    if patch.version != 1
        || !identity(&patch.organization_id)
        || !client_id(&patch.client_id)
        || patch.action != action
        || patch.expected_grant_version < 0
        || (action != "enroll" && patch.expected_grant_version == 0)
        || (patch.expected_grant_version == 0 && !empty(&patch.remove))
        || !valid_changes(&patch.add, true)
        || !valid_changes(&patch.remove, false)
        || patch
            .add
            .control_grants
            .iter()
            .any(|grant| grant.verb == "members_manage")
        || patch
            .add
            .control_grants
            .iter()
            .any(|grant| patch.remove.control_grants.contains(grant))
        || patch
            .add
            .cluster_admissions
            .iter()
            .any(|id| patch.remove.cluster_admissions.contains(id))
    {
        return Err(invalid_patch());
    }
    if patch.organization_id != organization {
        return Err(Failure::refused(
            "target_mismatch",
            "the workload patch belongs to another organization",
        ));
    }
    Ok(Some(patch))
}

fn empty_scopes(scopes: &Scopes) -> bool {
    !scopes.admin
        && scopes.clusters.is_empty()
        && scopes.verbs.is_empty()
        && scopes.data_grants.is_empty()
        && scopes.control_grants.is_empty()
        && scopes.cluster_admissions.is_empty()
}

fn validate(workload: &Workload) -> Result<()> {
    validate_scopes(&workload.scopes)?;
    if workload.issuer.len() > 512
        || !url::Url::parse(&workload.issuer).is_ok_and(|issuer| {
            issuer.scheme() == "https"
                && issuer.host_str().is_some()
                && issuer.port().is_none()
                && issuer.username().is_empty()
                && issuer.password().is_none()
                && issuer.origin().ascii_serialization() == workload.issuer
        })
        || !client_id(&workload.client_id)
        || !identity(&workload.application_id)
        || !identity(&workload.principal_id)
        || !plain(&workload.name, 256)
        || workload.grant_version <= 0
        || workload.scopes.admin
        || !workload.scopes.clusters.is_empty()
        || !workload.scopes.verbs.is_empty()
        || !workload.scopes.data_grants.is_empty()
        || workload
            .scopes
            .control_grants
            .iter()
            .any(|grant| grant.verb == "members_manage" || !valid_control_grant(grant, true))
        || workload
            .scopes
            .control_grants
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != workload.scopes.control_grants.len()
        || workload
            .scopes
            .cluster_admissions
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != workload.scopes.cluster_admissions.len()
        || (!workload.enabled && !empty_scopes(&workload.scopes))
    {
        return Err(Failure::protocol());
    }
    Ok(())
}

fn directory_human(data: &Value, organization: &str) -> Result<String> {
    let directory: Directory = parse(data)?;
    if directory.organization_id != organization || directory.workloads.len() > 1000 {
        return Err(Failure::protocol());
    }
    let mut clients = BTreeSet::new();
    let mut principals = BTreeSet::new();
    let mut applications = BTreeSet::new();
    let mut human = format!("Organization {organization} workloads\n");
    for workload in directory.workloads {
        validate(&workload)?;
        if !clients.insert(workload.client_id.clone())
            || !principals.insert(workload.principal_id.clone())
            || !applications.insert(workload.application_id.clone())
        {
            return Err(Failure::protocol());
        }
        writeln!(
            human,
            "\n{} · {} · {}\n  Application: {}\n  Principal: {} · grant version: {}",
            workload.name,
            workload.client_id,
            if workload.enabled {
                "enabled"
            } else {
                "revoked"
            },
            workload.application_id,
            workload.principal_id,
            workload.grant_version
        )
        .unwrap();
        let grants: Vec<_> = workload
            .scopes
            .control_grants
            .iter()
            .map(|grant| {
                format!(
                    "{}:{}",
                    grant.cluster.as_deref().unwrap_or("organization"),
                    grant.verb
                )
            })
            .collect();
        writeln!(
            human,
            "  Control grants: {}\n  Cluster admission: {}",
            if grants.is_empty() {
                "none".into()
            } else {
                grants.join(", ")
            },
            if workload.scopes.cluster_admissions.is_empty() {
                "none".into()
            } else {
                workload.scopes.cluster_admissions.join(", ")
            }
        )
        .unwrap();
    }
    if clients.is_empty() {
        human.push_str("No enrolled workloads.\n");
    }
    human.push_str("\nApplied Cedar owns graph and configuration permissions.\n");
    Ok(human)
}

fn receipt_human(data: &Value, patch: &Patch) -> Result<String> {
    let receipt: Receipt = parse(data)?;
    let workload = &receipt.workload;
    validate(workload)?;
    if receipt.organization_id != patch.organization_id
        || receipt.action != patch.action
        || receipt.previous_grant_version != patch.expected_grant_version
        || Some(workload.grant_version) != patch.expected_grant_version.checked_add(1)
        || workload.client_id != patch.client_id
        || workload.enabled != (patch.action != "revoke")
        || receipt.provider_changed
        || receipt.graph_policy_changed
        || receipt.maximum_existing_native_credential_lifetime_seconds != EXISTING_CREDENTIAL_WINDOW
        || patch
            .add
            .control_grants
            .iter()
            .any(|grant| !workload.scopes.control_grants.contains(grant))
        || patch
            .remove
            .control_grants
            .iter()
            .any(|grant| workload.scopes.control_grants.contains(grant))
        || patch
            .add
            .cluster_admissions
            .iter()
            .any(|id| !workload.scopes.cluster_admissions.contains(id))
        || patch
            .remove
            .cluster_admissions
            .iter()
            .any(|id| workload.scopes.cluster_admissions.contains(id))
    {
        return Err(Failure::protocol());
    }
    Ok(format!(
        "Workload {}: {} applied\nPrincipal: {}\nGrant version: {}\nProvider application and graph policy are unchanged. Existing graph credentials may remain valid for at most 330 seconds; applied Cedar owns graph permissions.\n",
        workload.client_id, patch.action, workload.principal_id, workload.grant_version
    ))
}

pub(super) async fn execute(
    api: &Api,
    command: &Command,
    patch: Option<Patch>,
    key: Option<&str>,
) -> Result<(Value, String, i32)> {
    let organization = api.account_id()?;
    if matches!(command, Command::List) {
        let body = request(api, Method::GET, PATH, None, None).await?;
        let human = directory_human(&body["data"], organization)?;
        return Ok((body, human, 0));
    }
    let patch = patch.ok_or_else(Failure::protocol)?;
    if patch.organization_id != organization {
        return Err(Failure::refused(
            "context_mismatch",
            "the workload patch belongs to another organization",
        ));
    }
    let input = serde_json::to_value(&patch).map_err(|_| Failure::protocol())?;
    let body = request(api, Method::POST, PATH, Some(&input), key).await?;
    let human = receipt_human(&body["data"], &patch)?;
    Ok((body, human, 0))
}
