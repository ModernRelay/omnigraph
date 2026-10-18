//! Selected-organization team administration; graph authority remains in Cedar.
use super::*;
use crate::cli::{
    OrganizationArgs, OrganizationCommand, OrganizationInvitationsCommand as Invitations,
    OrganizationMembersCommand as Members,
};
use std::collections::BTreeSet;
use std::fmt::Write as _;

const TEAM_TIMEOUT: Duration = Duration::from_secs(35);
const MAX_PATCH: u64 = 256 * 1024;
const CREDENTIAL_LIFETIME: u64 = 86400;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
struct Grant {
    cluster: Option<String>,
    verb: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Changes {
    #[serde(default)]
    control_grants: Vec<Grant>,
    #[serde(default)]
    cluster_admissions: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Patch {
    version: u32,
    organization_id: String,
    user_id: String,
    action: String,
    expected_grant_version: i64,
    #[serde(default)]
    add: Changes,
    #[serde(default)]
    remove: Changes,
}

#[derive(Deserialize)]
struct Scopes {
    admin: bool,
    clusters: Vec<String>,
    verbs: Vec<String>,
    control_grants: Vec<Grant>,
    data_grants: Vec<Value>,
    #[serde(default)]
    cluster_admissions: Vec<String>,
}

#[derive(Deserialize)]
struct Member {
    user_id: String,
    principal_id: Option<String>,
    grant_version: i64,
    scopes: Scopes,
}

#[derive(Deserialize)]
struct DirectoryMember {
    #[serde(flatten)]
    local: Member,
    membership_id: Option<String>,
    membership_status: String,
    name: Option<String>,
    email: Option<String>,
}

#[derive(Deserialize)]
struct Directory {
    members: Vec<DirectoryMember>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Invitation {
    id: String,
    email: String,
    state: String,
    organization_id: String,
    accepted_user_id: Option<String>,
    created_at: String,
    expires_at: String,
}

#[derive(Deserialize)]
struct InvitationPage {
    invitations: Vec<Invitation>,
    after: Option<String>,
}

fn plain(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

fn identity(value: &str) -> bool {
    value.len() <= 128 && identifier(value).is_ok()
}

fn optional_identity(value: &Option<String>) -> bool {
    value.as_deref().is_none_or(identity)
}

fn optional_text(value: &Option<String>, max: usize) -> bool {
    value.as_deref().is_none_or(|s| plain(s, max))
}

fn email(value: &str) -> bool {
    plain(value, 320)
        && value.trim() == value
        && !value.chars().any(char::is_whitespace)
        && value.split_once('@').is_some_and(|(local, domain)| {
            !local.is_empty() && !domain.is_empty() && !domain.contains('@')
        })
}

fn valid_changes(changes: &Changes, adding: bool) -> bool {
    changes.cluster_admissions.len() <= 4096
        && changes.cluster_admissions.iter().all(|id| identity(id))
        && changes
            .cluster_admissions
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            == changes.cluster_admissions.len()
        && changes.control_grants.len() <= 32768
        && changes.control_grants.iter().collect::<BTreeSet<_>>().len()
            == changes.control_grants.len()
        && changes
            .control_grants
            .iter()
            .all(|grant| match grant.cluster.as_deref() {
                None => matches!(grant.verb.as_str(), "create" | "members_manage"),
                Some(cluster) => {
                    identity(cluster)
                        && (matches!(
                            grant.verb.as_str(),
                            "status" | "delete" | "source_read" | "source_write"
                        ) || (!adding
                            && matches!(grant.verb.as_str(), "plan" | "apply" | "cancel")))
                }
            })
}

fn empty(changes: &Changes) -> bool {
    changes.control_grants.is_empty() && changes.cluster_admissions.is_empty()
}

fn read_patch(path: &Path) -> Result<Patch> {
    let invalid = || {
        Failure::refused(
            "member_patch_invalid",
            "member patch must be a regular YAML file of at most 256 KiB with version 1, exact identities, action add/update/remove, a matching expected version and exact grant changes",
        )
    };
    let metadata = std::fs::symlink_metadata(path).map_err(|_| invalid())?;
    if !metadata.is_file() || metadata.len() > MAX_PATCH {
        return Err(invalid());
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|_| invalid())?;
    if !file.metadata().map_err(|_| invalid())?.is_file() {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    file.take(MAX_PATCH + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    if bytes.len() as u64 > MAX_PATCH {
        return Err(invalid());
    }
    let patch: Patch = yaml_serde::from_slice(&bytes).map_err(|_| invalid())?;
    if patch.version != 1
        || !identity(&patch.organization_id)
        || !identity(&patch.user_id)
        || !match patch.action.as_str() {
            "add" => patch.expected_grant_version == 0 && empty(&patch.remove),
            "update" => patch.expected_grant_version > 0,
            "remove" => {
                patch.expected_grant_version > 0 && empty(&patch.add) && empty(&patch.remove)
            }
            _ => false,
        }
        || !valid_changes(&patch.add, true)
        || !valid_changes(&patch.remove, false)
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
        return Err(invalid());
    }
    Ok(patch)
}

fn validate_scopes(scopes: &Scopes) -> Result<()> {
    if scopes.clusters.len() > 4096
        || scopes.verbs.len() > 9
        || scopes.control_grants.len() > 32768
        || scopes.cluster_admissions.len() > 4096
        || scopes.data_grants.len() > 4096
        || scopes.clusters.iter().any(|id| !plain(id, 512))
        || scopes.verbs.iter().any(|verb| !identity(verb))
        || scopes.control_grants.iter().any(|grant| {
            !identity(&grant.verb) || grant.cluster.as_ref().is_some_and(|id| !plain(id, 512))
        })
        || scopes.cluster_admissions.iter().any(|id| !identity(id))
    {
        return Err(Failure::protocol());
    }
    Ok(())
}

fn validate_member(member: &Member) -> Result<()> {
    if !identity(&member.user_id)
        || !optional_identity(&member.principal_id)
        || member.grant_version < 0
        || (member.principal_id.is_none() != (member.grant_version == 0))
        || (member.principal_id.is_none()
            && (member.scopes.admin
                || !member.scopes.clusters.is_empty()
                || !member.scopes.verbs.is_empty()
                || !member.scopes.control_grants.is_empty()
                || !member.scopes.data_grants.is_empty()
                || !member.scopes.cluster_admissions.is_empty()))
    {
        return Err(Failure::protocol());
    }
    validate_scopes(&member.scopes)
}

fn validate_invitation(invitation: &Invitation, organization: &str) -> Result<()> {
    if invitation.organization_id != organization
        || !identity(&invitation.id)
        || !email(&invitation.email)
        || !optional_identity(&invitation.accepted_user_id)
        || !matches!(
            invitation.state.as_str(),
            "pending" | "accepted" | "revoked" | "expired"
        )
        || [&invitation.created_at, &invitation.expires_at]
            .iter()
            .any(|s| {
                s.len() > 64
                    || time::OffsetDateTime::parse(
                        s,
                        &time::format_description::well_known::Rfc3339,
                    )
                    .is_err()
            })
    {
        return Err(Failure::protocol());
    }
    Ok(())
}

fn parse<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
    serde_json::from_value(value.clone()).map_err(|_| Failure::protocol())
}

fn directory_human(data: &Value, organization: &str) -> Result<String> {
    let directory: Directory = parse(data)?;
    if directory.members.len() > 1000 {
        return Err(Failure::protocol());
    }
    let mut users = BTreeSet::new();
    let mut principals = BTreeSet::new();
    let mut memberships = BTreeSet::new();
    let mut human = format!("Organization {organization} members\n");
    for member in directory.members {
        validate_member(&member.local)?;
        if !users.insert(member.local.user_id.clone())
            || !optional_identity(&member.membership_id)
            || member
                .local
                .principal_id
                .as_ref()
                .is_some_and(|id| !principals.insert(id.clone()))
            || member
                .membership_id
                .as_ref()
                .is_some_and(|id| !memberships.insert(id.clone()))
            || !matches!(
                member.membership_status.as_str(),
                "active" | "pending" | "inactive" | "absent"
            )
            || (member.membership_id.is_none() != (member.membership_status == "absent"))
            || !optional_text(&member.name, 256)
            || !optional_text(&member.email, 320)
        {
            return Err(Failure::protocol());
        }
        writeln!(
            human,
            "\n{} · {}",
            member.name.as_deref().unwrap_or("Name unavailable"),
            member.email.as_deref().unwrap_or("email unavailable")
        )
        .unwrap();
        writeln!(
            human,
            "  User: {} · membership: {}",
            member.local.user_id, member.membership_status
        )
        .unwrap();
        writeln!(
            human,
            "  Principal: {} · grant version: {}",
            member
                .local
                .principal_id
                .as_deref()
                .unwrap_or("not enrolled"),
            member.local.grant_version
        )
        .unwrap();
        let scopes = member.local.scopes;
        let grants: Vec<_> = scopes
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
            "  Control grants: {}",
            if grants.is_empty() {
                "none".into()
            } else {
                grants.join(", ")
            }
        )
        .unwrap();
        writeln!(
            human,
            "  Cluster admission: {}",
            if scopes.cluster_admissions.is_empty() {
                "none".into()
            } else {
                scopes.cluster_admissions.join(", ")
            }
        )
        .unwrap();
        if scopes.admin
            || !scopes.clusters.is_empty()
            || !scopes.verbs.is_empty()
            || !scopes.data_grants.is_empty()
        {
            human.push_str(
                "  Additional legacy scopes are present; use --json for the exact record.\n",
            );
        }
    }
    if users.is_empty() {
        human.push_str("No organization members.\n");
    }
    human.push_str(
        "\nInvitation acceptance does not enroll a member. Applied Cedar owns graph permissions.\n",
    );
    Ok(human)
}

fn invitation_page_human(data: &Value, organization: &str) -> Result<String> {
    let page: InvitationPage = parse(data)?;
    if page.invitations.len() > 100 || !optional_text(&page.after, 256) {
        return Err(Failure::protocol());
    }
    let mut seen = BTreeSet::new();
    let mut human = format!("Organization {organization} invitations\n");
    for invitation in page.invitations {
        validate_invitation(&invitation, organization)?;
        if !seen.insert(invitation.id.clone()) {
            return Err(Failure::protocol());
        }
        writeln!(
            human,
            "{}  {}  {}  expires {}",
            invitation.id, invitation.state, invitation.email, invitation.expires_at
        )
        .unwrap();
    }
    if seen.is_empty() {
        human.push_str("No invitations on this page.\n");
    }
    if let Some(after) = page.after {
        writeln!(human, "Next page cursor: {}", json!(after)).unwrap();
    }
    Ok(human)
}

async fn client(target: &OrganizationArgs) -> Result<Api> {
    let context = read_context(&target.config)?;
    let origin = match (&target.api, &context) {
        (Some(api), _) => canonical_origin(api)?,
        (None, Some(context)) => context.api.clone(),
        (None, None) => {
            return Err(Failure::refused(
                "managed_context_required",
                "organization commands require --api or a managed folder context",
            ));
        }
    };
    let selected =
        selected_organization(context.as_ref(), &origin, target.organization.as_deref())?;
    Api::authenticated(origin, selected).await
}

async fn request(
    api: &Api,
    method: Method,
    path: &str,
    body: Option<&Value>,
    key: Option<&str>,
) -> Result<Value> {
    let response = api
        .raw_with_timeout(method, path, body, key, TEAM_TIMEOUT)
        .await?;
    if !response.status.is_success() {
        return Err(Failure {
            body: response.body,
            exit: if response.status.is_client_error() {
                2
            } else {
                1
            },
        });
    }
    if response
        .body
        .pointer("/data/organization_id")
        .and_then(Value::as_str)
        != Some(api.account_id()?)
    {
        return Err(Failure::refused(
            "context_mismatch",
            "the team response belongs to another organization",
        ));
    }
    Ok(response.body)
}

fn offboard_human(data: &Value, user_id: &str, expected_version: i64) -> Result<(String, i32)> {
    #[derive(Deserialize)]
    struct Sessions {
        revoked: u64,
        total: u64,
        state: String,
    }
    #[derive(Deserialize)]
    struct Receipt {
        user_id: String,
        principal_id: Option<String>,
        grant_version: i64,
        local_access_removed: bool,
        provider_membership: String,
        sessions: Sessions,
        graph_policy_changed: bool,
        maximum_existing_native_credential_lifetime_seconds: u64,
    }
    let receipt: Receipt = parse(data)?;
    if receipt.user_id != user_id
        || !optional_identity(&receipt.principal_id)
        || receipt.grant_version < expected_version
        || (receipt.principal_id.is_none() != (receipt.grant_version == 0))
        || !matches!(
            receipt.provider_membership.as_str(),
            "removed" | "already_absent" | "uncertain" | "not_attempted"
        )
        || !matches!(
            receipt.sessions.state.as_str(),
            "complete" | "partial" | "not_attempted"
        )
        || receipt.sessions.revoked > receipt.sessions.total
        || receipt.sessions.total > 1000
        || (receipt.sessions.state == "complete"
            && receipt.sessions.revoked != receipt.sessions.total)
        || receipt.graph_policy_changed
        || receipt.maximum_existing_native_credential_lifetime_seconds != CREDENTIAL_LIFETIME
    {
        return Err(Failure::protocol());
    }
    let complete = receipt.local_access_removed
        && matches!(
            receipt.provider_membership.as_str(),
            "removed" | "already_absent"
        )
        && receipt.sessions.state == "complete";
    Ok((
        format!(
            "Offboarding {user_id}: {}\nLocal access removed: {} · grant version: {}\nProvider membership: {}\nOrganization sessions: {}/{} revoked ({})\nGraph policy is unchanged. Existing graph credentials may last up to 86400 seconds; deploy Cedar removal and verify denial with a previously issued credential.\n",
            if complete {
                "complete"
            } else {
                "partial; inspect the retained result before further cleanup"
            },
            receipt.local_access_removed,
            receipt.grant_version,
            receipt.provider_membership,
            receipt.sessions.revoked,
            receipt.sessions.total,
            receipt.sessions.state
        ),
        if complete { 0 } else { 3 },
    ))
}

fn patch_human(data: &Value, patch: &Patch) -> Result<String> {
    let member: Member = parse(&data["member"])?;
    validate_member(&member)?;
    if member.user_id != patch.user_id
        || member.principal_id.is_none()
        || member.grant_version < patch.expected_grant_version
        || data["action"].as_str() != Some(&patch.action)
        || data["previous_grant_version"].as_i64() != Some(patch.expected_grant_version)
        || data["provider_membership_verified"].as_bool() != Some(patch.action != "remove")
        || data["provider_changed"].as_bool() != Some(false)
        || data["graph_policy_changed"].as_bool() != Some(false)
        || data["maximum_existing_native_credential_lifetime_seconds"].as_u64()
            != Some(CREDENTIAL_LIFETIME)
        || patch
            .add
            .control_grants
            .iter()
            .any(|grant| !member.scopes.control_grants.contains(grant))
        || patch
            .remove
            .control_grants
            .iter()
            .any(|grant| member.scopes.control_grants.contains(grant))
        || patch
            .add
            .cluster_admissions
            .iter()
            .any(|id| !member.scopes.cluster_admissions.contains(id))
        || patch
            .remove
            .cluster_admissions
            .iter()
            .any(|id| member.scopes.cluster_admissions.contains(id))
        || (patch.action == "remove"
            && (member.scopes.admin
                || !member.scopes.clusters.is_empty()
                || !member.scopes.verbs.is_empty()
                || !member.scopes.control_grants.is_empty()
                || !member.scopes.data_grants.is_empty()
                || !member.scopes.cluster_admissions.is_empty()))
    {
        return Err(Failure::protocol());
    }
    Ok(format!(
        "Member {}: {} applied\nPrincipal: {}\nGrant version: {}\nGraph policy is unchanged; use this stable principal in the normal Cedar configuration workflow.\n",
        member.user_id,
        patch.action,
        member.principal_id.unwrap(),
        member.grant_version
    ))
}

pub(super) async fn dispatch(
    target: &OrganizationArgs,
    command: &OrganizationCommand,
    stderr: &mut dyn Write,
) -> Output {
    let key = match command {
        OrganizationCommand::Members {
            command:
                Members::Apply {
                    idempotency_key, ..
                }
                | Members::Offboard {
                    idempotency_key, ..
                },
        }
        | OrganizationCommand::Invitations {
            command:
                Invitations::Send {
                    idempotency_key, ..
                }
                | Invitations::Resend {
                    idempotency_key, ..
                }
                | Invitations::Revoke {
                    idempotency_key, ..
                },
        } => Some(idempotency_key.as_str()),
        _ => None,
    };
    let result = async {
        if let Some(key) = key { idempotency_key(Some(key), stderr)?; }
        // Validate all caller-provided write targets before authentication or HTTP.
        let patch = match command {
            OrganizationCommand::Members { command: Members::Apply { file, .. } } => Some(read_patch(file)?),
            OrganizationCommand::Members { command: Members::Offboard { user_id, .. } } if !identity(user_id) => return Err(Failure::refused("identifier_invalid", "user ID must contain 1–128 ASCII letters, digits, underscores, or hyphens")),
            OrganizationCommand::Invitations { command: Invitations::Send { email: address, .. } } if !email(address) => return Err(Failure::refused("email_invalid", "provide a bounded email address without whitespace or control characters")),
            OrganizationCommand::Invitations { command: Invitations::Resend { invitation_id, .. } | Invitations::Revoke { invitation_id, .. } } if !identity(invitation_id) => return Err(Failure::refused("identifier_invalid", "invitation ID must contain 1–128 ASCII letters, digits, underscores, or hyphens")),
            OrganizationCommand::Invitations { command: Invitations::List { after: Some(cursor) } } if !plain(cursor, 256) => return Err(Failure::refused("cursor_invalid", "invitation cursor must contain 1–256 bytes without control characters")),
            _ => None,
        };
        // A bound or explicitly selected organization must agree with YAML before any HTTP.
        if let Some(patch) = &patch {
            let context = read_context(&target.config)?;
            if target.organization.as_ref().is_some_and(|id| id != &patch.organization_id)
                || context.as_ref().is_some_and(|c| c.organization_id != patch.organization_id)
            { return Err(Failure::refused("context_mismatch", "the member patch belongs to another organization")); }
        }
        let api = client(target).await?;
        let organization = api.account_id()?;
        if patch.as_ref().is_some_and(|patch| patch.organization_id != organization) {
            return Err(Failure::refused("context_mismatch", "the member patch belongs to another organization"));
        }
        let (body, human, exit) = match command {
            OrganizationCommand::Members { command: Members::List } => {
                let body = request(&api, Method::GET, "/v1/organization/members", None, None).await?;
                let human = directory_human(&body["data"], organization)?;
                (body, human, 0)
            }
            OrganizationCommand::Members { command: Members::Apply { .. } } => {
                let patch = patch.expect("validated patch");
                let input = serde_json::to_value(&patch).map_err(|_| Failure::protocol())?;
                let body = request(&api, Method::POST, "/v1/organization/members", Some(&input), key).await?;
                let human = patch_human(&body["data"], &patch)?;
                (body, human, 0)
            }
            OrganizationCommand::Members { command: Members::Offboard { user_id, expected_version, .. } } => {
                let input = json!({"version":1,"organization_id":organization,"user_id":user_id,"expected_grant_version":expected_version});
                let body = request(&api, Method::POST, &format!("/v1/organization/members/{user_id}:offboard"), Some(&input), key).await?;
                let (human, exit) = offboard_human(&body["data"], user_id, *expected_version)?;
                (body, human, exit)
            }
            OrganizationCommand::Invitations { command: Invitations::List { after } } => {
                let mut path = "/v1/organization/invitations".to_owned();
                if let Some(after) = after { path.push('?'); path.push_str(&url::form_urlencoded::Serializer::new(String::new()).append_pair("after", after).finish()); }
                let body = request(&api, Method::GET, &path, None, None).await?;
                let human = invitation_page_human(&body["data"], organization)?;
                (body, human, 0)
            }
            OrganizationCommand::Invitations { command } => {
                let (action, target_id, address) = match command {
                    Invitations::Send { email, .. } => ("send", None, Some(email.as_str())),
                    Invitations::Resend { invitation_id, .. } => ("resend", Some(invitation_id.as_str()), None),
                    Invitations::Revoke { invitation_id, .. } => ("revoke", Some(invitation_id.as_str()), None),
                    Invitations::List { .. } => unreachable!(),
                };
                let path = target_id.map_or_else(|| "/v1/organization/invitations".into(), |id| format!("/v1/organization/invitations/{id}:{action}"));
                let mut input = json!({"organization_id":organization});
                if let Some(address) = address { input["email"] = json!(address); }
                let body = request(&api, Method::POST, &path, Some(&input), key).await?;
                let invitation: Invitation = parse(&body["data"]["invitation"])?;
                validate_invitation(&invitation, organization)?;
                if body["data"]["action"].as_str() != Some(action)
                    || target_id.is_some_and(|id| id != invitation.id)
                    || address.is_some_and(|address| !address.eq_ignore_ascii_case(&invitation.email))
                    || (action == "revoke" && invitation.state != "revoked")
                { return Err(Failure::protocol()); }
                let human = format!("Invitation {}: {} · {}\nEmail: {}\nAcceptance alone does not grant cluster access.\n", invitation.id, action, invitation.state, invitation.email);
                (body, human, 0)
            }
        };
        Ok((body, human, exit))
    }.await;
    match result {
        Ok((body, human, exit)) => Output {
            body,
            human: Some(human),
            exit,
            json: target.json,
            recovery: key.map(|key| json!({"idempotency_key":key})),
        },
        Err(mut error) => {
            if let Some(key) = key {
                // A lost acknowledgement never allocates or submits a replacement action.
                if key.len() <= 256 && key.bytes().all(|b| b.is_ascii_graphic()) {
                    error.body["idempotency_key"] = json!(key);
                    if error.body["type"] != "output_delivery_failed" {
                        if let Err(mut delivery) = write_diagnostic(
                            stderr,
                            format_args!(
                                "Retain this idempotency key; inspect the result or repeat the identical request with the same key. A new key starts a new action.\n"
                            ),
                        ) {
                            delivery.body["original_error"] = error.body;
                            delivery.body["idempotency_key"] = json!(key);
                            error = delivery;
                        }
                    }
                }
            }
            Output::from_result(Err(error), target.json, 1)
        }
    }
}
