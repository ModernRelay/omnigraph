//! Selected-organization team administration; graph authority remains in Cedar.
use super::args::OrganizationArgs;
use super::*;
use crate::cli::{
    OrganizationCommand, OrganizationInvitationsCommand as Invitations,
    OrganizationMembersCommand as Members,
};
use std::collections::BTreeSet;
use std::fmt::Write as _;

const TEAM_TIMEOUT: Duration = Duration::from_secs(35);
const CREDENTIAL_LIFETIME: u64 = 86400;

#[derive(Deserialize)]
struct DirectoryMember {
    user_id: String,
    #[serde(deserialize_with = "Option::deserialize")]
    principal_id: Option<String>,
    membership_id: String,
    membership_status: String,
    service_permissions: Vec<String>,
    name: Option<String>,
    email: Option<String>,
}

#[derive(Deserialize)]
struct Directory {
    provider_observed_at: String,
    provider_expires_at: String,
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
    provider_observation(
        &directory.provider_observed_at,
        &directory.provider_expires_at,
    )?;
    if directory.members.len() > 1000 {
        return Err(Failure::protocol());
    }
    let mut users = BTreeSet::new();
    let mut principals = BTreeSet::new();
    let mut memberships = BTreeSet::new();
    let mut human = format!("Organization {organization} members\n");
    for member in directory.members {
        validate_service_permissions(&member.service_permissions)?;
        if !identity(&member.user_id)
            || !optional_identity(&member.principal_id)
            || !identity(&member.membership_id)
            || !member.membership_id.starts_with("om_")
            || !users.insert(member.user_id.clone())
            || !memberships.insert(member.membership_id.clone())
            || member
                .principal_id
                .as_ref()
                .is_some_and(|id| !principals.insert(id.clone()))
            || !matches!(
                member.membership_status.as_str(),
                "active" | "pending" | "inactive"
            )
            || (member.membership_status != "active" && !member.service_permissions.is_empty())
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
            "  User: {} · membership: {} ({})",
            member.user_id, member.membership_status, member.membership_id
        )
        .unwrap();
        writeln!(
            human,
            "  Principal: {}",
            member
                .principal_id
                .as_deref()
                .unwrap_or("not yet signed in")
        )
        .unwrap();
        writeln!(
            human,
            "  Organization service permissions: {}",
            if member.service_permissions.is_empty() {
                "none".into()
            } else {
                member.service_permissions.join(", ")
            }
        )
        .unwrap();
    }
    if users.is_empty() {
        human.push_str("No organization members.\n");
    }
    writeln!(
        human,
        "\nProvider observed: {} · expires: {}",
        directory.provider_observed_at, directory.provider_expires_at
    )
    .unwrap();
    human.push_str("Active membership establishes identity eligibility. Applied Cedar owns native graph and configuration permissions.\n");
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
            response_status: Some(response.status),
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

fn offboard_human(
    data: &Value,
    user_id: &str,
    expected_membership: Option<&str>,
) -> Result<(String, i32)> {
    #[derive(Deserialize)]
    struct Sessions {
        revoked: u64,
        total: u64,
        state: String,
    }
    #[derive(Deserialize)]
    struct Receipt {
        user_id: String,
        #[serde(deserialize_with = "Option::deserialize")]
        principal_id: Option<String>,
        #[serde(deserialize_with = "Option::deserialize")]
        membership_id: Option<String>,
        provider_observed_at: String,
        provider_membership: String,
        sessions: Sessions,
        graph_policy_changed: bool,
        maximum_existing_native_credential_lifetime_seconds: u64,
    }
    let receipt: Receipt = parse(data)?;
    provider_timestamp(&receipt.provider_observed_at)?;
    if receipt.user_id != user_id
        || !optional_identity(&receipt.principal_id)
        || receipt.membership_id.as_deref() != expected_membership
        || !matches!(
            receipt.provider_membership.as_str(),
            "removed" | "already_absent" | "uncertain"
        )
        || (receipt.provider_membership == "already_absent") != receipt.membership_id.is_none()
        || !matches!(
            receipt.sessions.state.as_str(),
            "complete" | "partial" | "not_attempted"
        )
        || receipt.sessions.revoked > receipt.sessions.total
        || receipt.sessions.total > 1000
        || (receipt.sessions.state == "complete"
            && receipt.sessions.revoked != receipt.sessions.total)
        || (receipt.sessions.state == "not_attempted" && receipt.sessions.revoked != 0)
        || (receipt.provider_membership == "uncertain" && receipt.sessions.state != "not_attempted")
        || receipt.graph_policy_changed
        || receipt.maximum_existing_native_credential_lifetime_seconds != CREDENTIAL_LIFETIME
    {
        return Err(Failure::protocol());
    }
    let complete = matches!(
        receipt.provider_membership.as_str(),
        "removed" | "already_absent"
    ) && receipt.sessions.state == "complete";
    Ok((
        format!(
            "Offboarding {user_id}: {}\nMembership: {} · provider result: {}\nOrganization sessions: {}/{} revoked ({})\nGraph policy is unchanged. Existing graph credentials may last up to 86400 seconds; deploy Cedar removal and verify denial with a previously issued credential.\n",
            if complete {
                "complete"
            } else {
                "partial; inspect the retained result before further cleanup"
            },
            receipt.membership_id.as_deref().unwrap_or("absent"),
            receipt.provider_membership,
            receipt.sessions.revoked,
            receipt.sessions.total,
            receipt.sessions.state
        ),
        if complete { 0 } else { 3 },
    ))
}

pub(super) async fn dispatch(
    binding: &Binding,
    args: &OrganizationArgs,
    command: &OrganizationCommand,
    stderr: &mut dyn Write,
) -> Output {
    let key = match command {
        OrganizationCommand::Members {
            command: Members::Offboard {
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
        match command {
            OrganizationCommand::Members { command: Members::Offboard { user_id, .. } } if !identity(user_id) => return Err(Failure::refused("identifier_invalid", "user ID must contain 1–128 ASCII letters, digits, underscores, or hyphens")),
            OrganizationCommand::Invitations { command: Invitations::Send { email: address, .. } } if !email(address) => return Err(Failure::refused("email_invalid", "provide a bounded email address without whitespace or control characters")),
            OrganizationCommand::Invitations { command: Invitations::Resend { invitation_id, .. } | Invitations::Revoke { invitation_id, .. } } if !identity(invitation_id) => return Err(Failure::refused("identifier_invalid", "invitation ID must contain 1–128 ASCII letters, digits, underscores, or hyphens")),
            OrganizationCommand::Invitations { command: Invitations::List { after: Some(cursor) } } if !plain(cursor, 256) => return Err(Failure::refused("cursor_invalid", "invitation cursor must contain 1–256 bytes without control characters")),
            OrganizationCommand::Members { command: Members::Offboard { expected_membership, .. } }
                if expected_membership != "absent" && (!expected_membership.starts_with("om_") || !identity(expected_membership)) => return Err(Failure::refused("membership_invalid", "provide the exact WorkOS membership ID, or absent after observing no membership")),
            _ => (),
        };
        binding.organization()?;
        let api = binding.client().await?;
        let organization = api.account_id()?;
        let (body, human, exit) = match command {
            OrganizationCommand::Members { command: Members::List } => {
                let body = request(&api, Method::GET, "/v1/organization/members", None, None).await?;
                let human = directory_human(&body["data"], organization)?;
                (body, human, 0)
            }
            OrganizationCommand::Members { command: Members::Offboard { user_id, expected_membership, .. } } => {
                let membership = (expected_membership != "absent").then_some(expected_membership.as_str());
                let input = json!({"version":2,"organization_id":organization,"user_id":user_id,"expected_membership_id":membership});
                let body = request(&api, Method::POST, &format!("/v1/organization/members/{user_id}:offboard"), Some(&input), key).await?;
                let (human, exit) = offboard_human(&body["data"], user_id, membership)?;
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
                let human = format!("Invitation {}: {} · {}\nEmail: {}\nActive membership establishes identity eligibility; native permissions remain in applied Cedar.\n", invitation.id, action, invitation.state, invitation.email);
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
            json: args.json,
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
            Output::from_result(Err(error), args.json, 1)
        }
    }
}
