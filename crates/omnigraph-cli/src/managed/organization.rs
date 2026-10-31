//! Selected-organization team administration; graph authority remains in Cedar.
use super::args::OrganizationArgs;
use super::*;
use crate::cli::{
    OrganizationCommand, OrganizationInvitationsCommand as Invitations,
    OrganizationMembersCommand as Members,
};
use std::collections::BTreeSet;
use std::fmt::Write as _;

mod custody;

const TEAM_TIMEOUT: Duration = Duration::from_secs(35);
const CREDENTIAL_LIFETIME: u64 = 86400;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Set {
        selector: String,
        roles: Vec<String>,
    },
    Offboard {
        selector: String,
        expected_membership: Option<String>,
    },
    Send {
        email: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role: Option<String>,
    },
    Resend {
        selector: String,
    },
    Revoke {
        selector: String,
    },
}

impl Operation {
    fn from_command(command: &OrganizationCommand) -> Option<(Self, &str)> {
        Some(match command {
            OrganizationCommand::Members {
                command:
                    Members::Set {
                        selector,
                        roles,
                        idempotency_key,
                    },
            } => (
                Self::Set {
                    selector: selector.clone(),
                    roles: {
                        let mut roles = roles.clone();
                        roles.sort();
                        roles
                    },
                },
                idempotency_key,
            ),
            OrganizationCommand::Members {
                command:
                    Members::Offboard {
                        selector,
                        expected_membership,
                        idempotency_key,
                    },
            } => (
                Self::Offboard {
                    selector: selector.clone(),
                    expected_membership: expected_membership.clone(),
                },
                idempotency_key,
            ),
            OrganizationCommand::Invitations {
                command:
                    Invitations::Send {
                        email,
                        role,
                        idempotency_key,
                    },
            } => (
                Self::Send {
                    email: email.clone(),
                    role: role.clone(),
                },
                idempotency_key,
            ),
            OrganizationCommand::Invitations {
                command:
                    Invitations::Resend {
                        selector,
                        idempotency_key,
                    },
            } => (
                Self::Resend {
                    selector: selector.clone(),
                },
                idempotency_key,
            ),
            OrganizationCommand::Invitations {
                command:
                    Invitations::Revoke {
                        selector,
                        idempotency_key,
                    },
            } => (
                Self::Revoke {
                    selector: selector.clone(),
                },
                idempotency_key,
            ),
            _ => return None,
        })
    }

    fn validate(&self) -> Result<()> {
        let selector = match self {
            Self::Set { selector, roles } => {
                if validate_roles(roles).is_err() || !roles.windows(2).all(|pair| pair[0] < pair[1])
                {
                    return Err(Failure::refused(
                        "roles_invalid",
                        "select 1–256 distinct service roles; slugs contain 1–128 lowercase ASCII letters, digits, underscores or hyphens",
                    ));
                }
                selector
            }
            Self::Offboard {
                selector,
                expected_membership,
            } => {
                if expected_membership.as_deref().is_some_and(|membership| {
                    membership != "absent" && !valid_membership(membership)
                }) {
                    return Err(Failure::refused(
                        "membership_invalid",
                        "provide an exact membership ID, or absent after observing no membership",
                    ));
                }
                if expected_membership.as_deref() == Some("absent") && !identity(selector) {
                    return Err(Failure::refused(
                        "membership_invalid",
                        "absent membership cleanup requires an exact user ID",
                    ));
                }
                selector
            }
            Self::Send {
                email: address,
                role,
            } => {
                if role.as_deref().is_some_and(|role| !role_slug(role)) {
                    return Err(Failure::refused(
                        "role_invalid",
                        "select an existing service-role slug",
                    ));
                }
                return if email(address) {
                    Ok(())
                } else {
                    Err(Failure::refused(
                        "email_invalid",
                        "provide a bounded email address without whitespace or control characters",
                    ))
                };
            }
            Self::Resend { selector } | Self::Revoke { selector } => selector,
        };
        if !email(selector) && !identity(selector) {
            return Err(Failure::refused(
                "identifier_invalid",
                "provide a valid email or exact ID without whitespace, path separators or control characters",
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct DirectoryMember {
    user_id: String,
    #[serde(deserialize_with = "Option::deserialize")]
    principal_id: Option<String>,
    membership_id: String,
    membership_status: String,
    roles: Vec<String>,
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
    role: Option<String>,
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
        || invitation
            .role
            .as_deref()
            .is_some_and(|role| !role_slug(role))
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

fn role_slug(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}

fn validate_roles(roles: &[String]) -> Result<()> {
    let mut unique = BTreeSet::new();
    if roles.is_empty()
        || roles.len() > 256
        || roles
            .iter()
            .any(|role| !role_slug(role) || !unique.insert(role))
    {
        return Err(Failure::protocol());
    }
    Ok(())
}

fn valid_membership(value: &str) -> bool {
    value.starts_with("om_") && identity(value)
}

fn directory(data: &Value) -> Result<Directory> {
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
    for member in &directory.members {
        validate_service_permissions(&member.service_permissions)?;
        validate_roles(&member.roles)?;
        if !identity(&member.user_id)
            || !optional_identity(&member.principal_id)
            || !valid_membership(&member.membership_id)
            || !users.insert(&member.user_id)
            || !memberships.insert(&member.membership_id)
            || member
                .principal_id
                .as_ref()
                .is_some_and(|id| !principals.insert(id))
            || !matches!(
                member.membership_status.as_str(),
                "active" | "pending" | "inactive"
            )
            || (member.membership_status != "active" && !member.service_permissions.is_empty())
            || !optional_text(&member.name, 256)
            || member
                .email
                .as_deref()
                .is_some_and(|address| !email(address))
        {
            return Err(Failure::protocol());
        }
    }
    Ok(directory)
}

fn directory_human(data: &Value, organization: &str) -> Result<String> {
    let directory = directory(data)?;
    let mut human = format!("Organization {organization} members\n");
    for member in &directory.members {
        writeln!(
            human,
            "\n{} · {}",
            member.name.as_deref().unwrap_or(&member.user_id),
            member.email.as_deref().unwrap_or("email unavailable")
        )
        .unwrap();
        writeln!(
            human,
            "  Membership: {} · service roles: {}",
            member.membership_status,
            member.roles.join(", ")
        )
        .unwrap();
        if let Some(principal) = &member.principal_id {
            writeln!(human, "  Cedar principal: principal:{principal}").unwrap();
        } else {
            human.push_str("  Cedar principal: not yet signed in\n");
        }
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
    if directory.members.is_empty() {
        human.push_str("No organization members.\n");
    }
    human.push_str("\nActive membership establishes identity eligibility. Applied Cedar owns native graph and configuration permissions.\n");
    Ok(human)
}

fn invitation_page(data: &Value, organization: &str) -> Result<InvitationPage> {
    let page: InvitationPage = parse(data)?;
    if page.invitations.len() > 100 || !optional_text(&page.after, 256) {
        return Err(Failure::protocol());
    }
    let mut seen = BTreeSet::new();
    for invitation in &page.invitations {
        validate_invitation(invitation, organization)?;
        if !seen.insert(&invitation.id) {
            return Err(Failure::protocol());
        }
    }
    Ok(page)
}

fn invitation_page_human(data: &Value, organization: &str) -> Result<String> {
    let page = invitation_page(data, organization)?;
    let mut human = format!("Organization {organization} invitations\n");
    for invitation in &page.invitations {
        writeln!(
            human,
            "{}  {}  {}  expires {}",
            invitation.id, invitation.state, invitation.email, invitation.expires_at
        )
        .unwrap();
    }
    if page.invitations.is_empty() {
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

fn invitation_path(after: Option<&str>) -> String {
    let mut path = "/v1/organization/invitations".to_owned();
    if let Some(after) = after {
        path.push('?');
        path.push_str(
            &url::form_urlencoded::Serializer::new(String::new())
                .append_pair("after", after)
                .finish(),
        );
    }
    path
}

async fn resolve_member(api: &Api, selector: &str) -> Result<DirectoryMember> {
    let body = request(api, Method::GET, "/v1/organization/members", None, None).await?;
    let mut matches = directory(&body["data"])?
        .members
        .into_iter()
        .filter(|member| {
            if email(selector) {
                member
                    .email
                    .as_deref()
                    .is_some_and(|address| address.eq_ignore_ascii_case(selector))
            } else {
                member.user_id == selector
            }
        });
    let selected = matches.next().ok_or_else(|| {
        Failure::refused(
            "organization_member_not_found",
            "no member matches this selector in the selected organization",
        )
    })?;
    if matches.next().is_some() {
        return Err(Failure::refused(
            "organization_member_ambiguous",
            "more than one member matches this email; use an exact user ID",
        ));
    }
    Ok(selected)
}

async fn resolve_invitation(api: &Api, selector: &str) -> Result<String> {
    if !email(selector) {
        return Ok(selector.into());
    }
    tokio::time::timeout(TEAM_TIMEOUT, async {
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut selected = None;
        for _ in 0..10 {
            let body = request(api, Method::GET, &invitation_path(cursor.as_deref()), None, None).await?;
            let page = invitation_page(&body["data"], api.account_id()?)?;
            for invitation in page.invitations {
                if !ids.insert(invitation.id.clone()) { return Err(Failure::protocol()); }
                if invitation.state == "pending" && invitation.email.eq_ignore_ascii_case(selector) {
                    if selected.is_some() { return Err(Failure::refused("organization_invitation_ambiguous", "more than one pending invitation matches this email; use an exact invitation ID")); }
                    selected = Some(invitation.id);
                }
            }
            if ids.len() > 1000 { return Err(Failure::protocol()); }
            match page.after {
                Some(after) if cursors.insert(after.clone()) => cursor = Some(after),
                Some(_) => return Err(Failure::refused("organization_invitation_scan_incomplete", "invitation pagination repeated a cursor; no invitation was changed")),
                None => return selected.ok_or_else(|| Failure::refused("organization_invitation_not_found", "no pending invitation matches this email in the selected organization")),
            }
        }
        Err(Failure::refused("organization_invitation_scan_incomplete", "invitation lookup exceeds ten pages or 1,000 entries; use an exact invitation ID"))
    }).await.map_err(|_| Failure::new("organization_invitation_scan_timeout", "invitation lookup exceeded 35 seconds; no invitation was changed", 1))?
}

async fn resolve(api: &Api, operation: &Operation) -> Result<custody::ExactRequest> {
    let organization = api.account_id()?;
    let (path, body) = match operation {
        Operation::Set { selector, roles } => {
            let member = resolve_member(api, selector).await?;
            if member.membership_status != "active" {
                return Err(Failure::refused(
                    "organization_member_inactive",
                    "role assignment requires an existing active membership",
                ));
            }
            (
                format!("/v1/organization/members/{}:set-role", member.user_id),
                json!({"version":1,"organization_id":organization,"user_id":member.user_id,"expected_membership_id":member.membership_id,"roles":roles}),
            )
        }
        Operation::Offboard {
            selector,
            expected_membership,
        } => {
            // An exact user+membership assertion already supplies the immutable binding.
            let (user, membership) = if !email(selector) && expected_membership.is_some() {
                (
                    selector.clone(),
                    expected_membership
                        .as_deref()
                        .filter(|value| *value != "absent")
                        .map(str::to_owned),
                )
            } else {
                let member = resolve_member(api, selector).await?;
                if expected_membership
                    .as_deref()
                    .is_some_and(|expected| expected != member.membership_id)
                {
                    return Err(Failure::refused(
                        "membership_mismatch",
                        "the selected member has a different membership; inspect it before creating a new request",
                    ));
                }
                (member.user_id, Some(member.membership_id))
            };
            (
                format!("/v1/organization/members/{user}:offboard"),
                json!({"version":2,"organization_id":organization,"user_id":user,"expected_membership_id":membership}),
            )
        }
        Operation::Send { email, role } => {
            if role.is_none() {
                return Err(Failure::refused(
                    "role_required",
                    "new invitations require --role; an omitted role can only retrieve an existing retained request",
                ));
            }
            (
                "/v1/organization/invitations".into(),
                invitation_body(organization, email, role.as_deref()),
            )
        }
        Operation::Resend { selector } | Operation::Revoke { selector } => {
            let invitation = resolve_invitation(api, selector).await?;
            let action = if matches!(operation, Operation::Resend { .. }) {
                "resend"
            } else {
                "revoke"
            };
            (
                format!("/v1/organization/invitations/{invitation}:{action}"),
                json!({"organization_id":organization}),
            )
        }
    };
    Ok(custody::ExactRequest { path, body })
}

fn invitation_body(organization: &str, email: &str, role: Option<&str>) -> Value {
    let mut body = json!({"organization_id":organization,"email":email});
    if let Some(role) = role {
        body["role"] = json!(role);
    }
    body
}

fn role_human(data: &Value, exact: &custody::ExactRequest) -> Result<(String, i32)> {
    #[derive(Deserialize)]
    struct Receipt {
        user_id: String,
        membership_id: String,
        previous_roles: Vec<String>,
        requested_roles: Option<Vec<String>>,
        requested_role: Option<String>,
        role_update: String,
        roles: Vec<String>,
        service_permissions: Vec<String>,
        provider_observed_at: String,
        graph_policy_changed: bool,
    }
    let receipt: Receipt = parse(data)?;
    // A request retained before role sets sent one `role`; the service that
    // accepted it answers with one `requested_role`.
    let requested = match (
        &exact.body["role"],
        receipt.requested_roles,
        receipt.requested_role,
    ) {
        (Value::Null, Some(roles), None) if exact.body["roles"] == json!(roles) => roles,
        (Value::String(role), None, Some(requested)) if *role == requested => vec![requested],
        _ => return Err(Failure::protocol()),
    };
    validate_roles(&receipt.previous_roles)?;
    validate_roles(&requested)?;
    validate_roles(&receipt.roles)?;
    validate_service_permissions(&receipt.service_permissions)?;
    let updated = receipt.role_update == "updated";
    if exact.body["user_id"].as_str() != Some(receipt.user_id.as_str())
        || exact.body["expected_membership_id"].as_str() != Some(receipt.membership_id.as_str())
        || !matches!(receipt.role_update.as_str(), "updated" | "uncertain")
        || updated && receipt.roles != requested
        || !updated && receipt.roles != receipt.previous_roles
        || receipt.graph_policy_changed
        || receipt.provider_observed_at.len() > 64
        || time::OffsetDateTime::parse(
            &receipt.provider_observed_at,
            &time::format_description::well_known::Rfc3339,
        )
        .is_err()
    {
        return Err(Failure::protocol());
    }
    Ok((
        format!(
            "Service roles for {}: {}\n{}: {}\nOrganization service permissions: {}\nGraph policy is unchanged; applied Cedar owns native permissions.\n",
            receipt.user_id,
            receipt.role_update,
            if updated {
                "Assigned roles"
            } else {
                "Last verified roles (requested change is uncertain)"
            },
            receipt.roles.join(", "),
            if receipt.service_permissions.is_empty() {
                "none".into()
            } else {
                receipt.service_permissions.join(", ")
            }
        ),
        if updated { 0 } else { 3 },
    ))
}

fn mutation_human(
    data: &Value,
    operation: &Operation,
    exact: &custody::ExactRequest,
    organization: &str,
) -> Result<(String, i32)> {
    match operation {
        Operation::Set { .. } => role_human(data, exact),
        Operation::Offboard { .. } => offboard_human(
            data,
            exact.body["user_id"]
                .as_str()
                .ok_or_else(Failure::protocol)?,
            exact.body["expected_membership_id"].as_str(),
        ),
        Operation::Send { .. } | Operation::Resend { .. } | Operation::Revoke { .. } => {
            let (action, address, role) = match operation {
                Operation::Send { email, role } => ("send", Some(email.as_str()), role.as_deref()),
                Operation::Resend { .. } => ("resend", None, None),
                Operation::Revoke { .. } => ("revoke", None, None),
                _ => unreachable!(),
            };
            let invitation: Invitation = parse(&data["invitation"])?;
            validate_invitation(&invitation, organization)?;
            if data["action"].as_str() != Some(action)
                || action != "send"
                    && exact.path
                        != format!("/v1/organization/invitations/{}:{action}", invitation.id)
                || address.is_some_and(|address| !address.eq_ignore_ascii_case(&invitation.email))
                || role.is_some_and(|role| invitation.role.as_deref() != Some(role))
                || action == "revoke" && invitation.state != "revoked"
            {
                return Err(Failure::protocol());
            }
            Ok((
                format!(
                    "Invitation {}: {} · {}\nEmail: {}\nActive membership establishes identity eligibility; native permissions remain in applied Cedar.\n",
                    invitation.id, action, invitation.state, invitation.email
                ),
                0,
            ))
        }
    }
}

pub(super) async fn dispatch(
    input: &crate::operator::InvocationConfig,
    binding: &Binding,
    args: &OrganizationArgs,
    command: &OrganizationCommand,
    stderr: &mut dyn Write,
) -> Output {
    let operation = Operation::from_command(command);
    let key = operation.as_ref().map(|(_, key)| *key);
    let result = async {
        if let Some((operation, key)) = &operation {
            idempotency_key(Some(key), stderr)?;
            operation.validate()?;
        }
        if let OrganizationCommand::Invitations {
            command: Invitations::List {
                after: Some(cursor),
            },
        } = command
            && !plain(cursor, 256)
        {
            return Err(Failure::refused(
                "cursor_invalid",
                "invitation cursor must contain 1–256 bytes without control characters",
            ));
        }
        binding.organization()?;
        let api = binding.client().await?;
        let organization = api.account_id()?;
        if let Some((operation, key)) = &operation {
            let mut journal = custody::Journal::open(input, binding, &api)?;
            // Look up original custody before any selector read, even after a failed write.
            let exact = match journal.retained(key, operation)? {
                Some(exact) => exact,
                None => {
                    let exact = resolve(&api, operation).await?;
                    journal.retain(key, operation.clone(), exact)?
                }
            };
            let body = request(
                &api,
                Method::POST,
                &exact.path,
                Some(&exact.body),
                Some(key),
            )
            .await?;
            let (human, exit) = mutation_human(&body["data"], operation, &exact, organization)?;
            Ok((body, human, exit))
        } else {
            let (path, members) = match command {
                OrganizationCommand::Members {
                    command: Members::List,
                } => ("/v1/organization/members".into(), true),
                OrganizationCommand::Invitations {
                    command: Invitations::List { after },
                } => (invitation_path(after.as_deref()), false),
                _ => unreachable!(),
            };
            let body = request(&api, Method::GET, &path, None, None).await?;
            let human = if members {
                directory_human(&body["data"], organization)?
            } else {
                invitation_page_human(&body["data"], organization)?
            };
            Ok((body, human, 0))
        }
    }
    .await;
    match result {
        Ok((body, human, exit)) => Output {
            body,
            human: Some(human),
            exit,
            json: args.json,
            recovery: key.map(|key| json!({"idempotency_key":key})),
        },
        Err(mut error) => {
            if let Some(key) = key
                && !key.is_empty()
                && key.len() <= 256
                && key.bytes().all(|b| b.is_ascii_graphic())
            {
                error.body["idempotency_key"] = json!(key);
                if error.body["type"] != "output_delivery_failed" {
                    if let Err(mut delivery) = write_diagnostic(
                        stderr,
                        format_args!(
                            "Retain this operator home, profile and idempotency key; repeat the identical request with the same key. A new key starts a new action.\n"
                        ),
                    ) {
                        delivery.body["original_error"] = error.body;
                        delivery.body["idempotency_key"] = json!(key);
                        error = delivery;
                    }
                }
            }
            Output::from_result(Err(error), args.json, 1)
        }
    }
}
