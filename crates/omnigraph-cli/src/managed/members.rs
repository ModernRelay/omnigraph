//! Organization membership eligibility. Applied Cedar owns native permissions.
use super::*;
use std::collections::BTreeSet;
use std::fmt::Write as _;

#[derive(Deserialize)]
struct Directory {
    cluster_id: String,
    organization_id: String,
    provider_observed_at: String,
    provider_expires_at: String,
    members: Vec<Member>,
}

#[derive(Deserialize)]
struct Member {
    user_id: String,
    membership_id: String,
    #[serde(deserialize_with = "Option::deserialize")]
    principal_id: Option<String>,
    name: Option<String>,
    email: Option<String>,
    eligible: bool,
    service_permissions: Vec<String>,
}

fn directory(body: &Value, cluster: &str, organization: &str) -> Result<Directory> {
    cluster_matches(body, cluster)?;
    let directory: Directory =
        serde_json::from_value(body["data"].clone()).map_err(|_| Failure::protocol())?;
    if directory.cluster_id != cluster || directory.organization_id != organization {
        return Err(Failure::refused(
            "context_mismatch",
            "the member list belongs to another cluster or organization",
        ));
    }
    let incarnation = body["meta"]["incarnation"]
        .as_str()
        .ok_or_else(Failure::protocol)?;
    if identifier(incarnation).is_err() || directory.members.len() > 1000 {
        return Err(Failure::protocol());
    }
    let mut users = BTreeSet::new();
    let mut principals = BTreeSet::new();
    let mut memberships = BTreeSet::new();
    provider_observation(
        &directory.provider_observed_at,
        &directory.provider_expires_at,
    )?;
    for member in &directory.members {
        validate_service_permissions(&member.service_permissions)?;
        if identifier(&member.user_id).is_err()
            || identifier(&member.membership_id).is_err()
            || !member.membership_id.starts_with("om_")
            || member
                .principal_id
                .as_ref()
                .is_some_and(|id| identifier(id).is_err())
            || !users.insert(&member.user_id)
            || !memberships.insert(&member.membership_id)
            || member
                .principal_id
                .as_ref()
                .is_some_and(|id| !principals.insert(id))
            || !member.eligible
            || [(&member.name, 256), (&member.email, 320)]
                .into_iter()
                .any(|(value, limit)| {
                    value.as_ref().is_some_and(|value| {
                        value.len() > limit || value.chars().any(char::is_control)
                    })
                })
        {
            return Err(Failure::protocol());
        }
    }
    Ok(directory)
}

fn human(directory: &Directory) -> String {
    let mut output = format!("Cluster {} members\n", directory.cluster_id);
    if directory.members.is_empty() {
        output.push_str("No active organization members.\n");
    } else {
        let rows: Vec<_> = std::iter::once(vec![
            "NAME".to_owned(),
            "EMAIL".to_owned(),
            "PRINCIPAL".to_owned(),
            "ELIGIBLE".to_owned(),
            "ORG SERVICE PERMISSIONS".to_owned(),
        ])
        .chain(directory.members.iter().map(|member| {
            vec![
                member.name.clone().unwrap_or_else(|| "—".into()),
                member.email.clone().unwrap_or_else(|| "—".into()),
                member
                    .principal_id
                    .clone()
                    .unwrap_or_else(|| "not yet signed in".into()),
                "yes".into(),
                if member.service_permissions.is_empty() {
                    "—".into()
                } else {
                    member.service_permissions.join(", ")
                },
            ]
        }))
        .collect();
        let widths: Vec<_> = (0..4)
            .map(|column| {
                rows.iter()
                    .map(|row| row[column].chars().count())
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        for row in rows {
            for (index, width) in widths.iter().enumerate() {
                write!(output, "{:<width$}  ", row[index]).expect("writing a string");
            }
            writeln!(output, "{}", row[4]).expect("writing a string");
        }
    }
    writeln!(
        output,
        "\nOrganization {} · provider observed: {} · expires: {}",
        directory.organization_id, directory.provider_observed_at, directory.provider_expires_at
    )
    .expect("writing a string");
    output.push_str("Active organization membership establishes identity eligibility.\nGraph permissions are evaluated by Cedar separately.\n");
    output
}

pub(super) async fn list(context: &Context, json: bool) -> Output {
    let result = async {
        let api = context.client().await?;
        let body = api
            .request(
                Method::GET,
                &format!("/v1/clusters/{}/members", context.cluster),
                None,
                None,
            )
            .await?;
        context.verify(&body)?;
        let directory = directory(&body, &context.cluster, api.account_id()?)?;
        Ok((body, human(&directory)))
    }
    .await;
    match result {
        Ok((body, human)) => Output {
            body,
            exit: 0,
            json,
            human: Some(human),
            recovery: None,
        },
        Err(error) => Output::from_result(Err(error), json, 1),
    }
}
