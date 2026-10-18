//! Read-only cluster enrollment. Applied Cedar owns graph permissions separately.
use super::*;
use std::collections::BTreeSet;
use std::fmt::Write as _;

#[derive(Deserialize)]
struct Directory {
    cluster_id: String,
    members: Vec<Member>,
}

#[derive(Deserialize)]
struct Member {
    user_id: String,
    principal_id: String,
    name: Option<String>,
    email: Option<String>,
    admitted: bool,
    control_grants: Vec<String>,
}

fn directory(body: &Value, cluster: &str) -> Result<Directory> {
    cluster_matches(body, cluster)?;
    let directory: Directory =
        serde_json::from_value(body["data"].clone()).map_err(|_| Failure::protocol())?;
    if directory.cluster_id != cluster {
        return Err(Failure::refused(
            "context_mismatch",
            "the member list belongs to another cluster",
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
    for member in &directory.members {
        if identifier(&member.user_id).is_err()
            || identifier(&member.principal_id).is_err()
            || !users.insert(&member.user_id)
            || !principals.insert(&member.principal_id)
            || (!member.admitted && member.control_grants.is_empty())
            || member
                .control_grants
                .iter()
                .any(|grant| identifier(grant).is_err())
            || member
                .control_grants
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
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
        output.push_str("No explicitly enrolled members.\n");
    } else {
        let rows: Vec<_> = std::iter::once(vec![
            "NAME".to_owned(),
            "EMAIL".to_owned(),
            "PRINCIPAL".to_owned(),
            "ADMITTED".to_owned(),
            "CONTROL GRANTS".to_owned(),
        ])
        .chain(directory.members.iter().map(|member| {
            vec![
                member.name.clone().unwrap_or_else(|| "—".into()),
                member.email.clone().unwrap_or_else(|| "—".into()),
                member.principal_id.clone(),
                if member.admitted { "yes" } else { "no" }.into(),
                if member.control_grants.is_empty() {
                    "—".into()
                } else {
                    member.control_grants.join(", ")
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
    output.push_str("\nExplicit cluster enrollment only; invitations are not listed.\nGraph permissions are evaluated by Cedar separately.\n");
    output
}

pub(super) async fn list(config: &Path, json: bool) -> Output {
    let result = async {
        let context = read_context(config)?.ok_or_else(|| {
            Failure::refused(
                "managed_context_required",
                "cluster members list --managed requires the selected folder's .omnigraph/context",
            )
        })?;
        let api = Api::authenticated(context.api.clone(), Some(&context.organization_id)).await?;
        let body = api
            .request(
                Method::GET,
                &format!("/v1/clusters/{}/members", context.cluster),
                None,
                None,
            )
            .await?;
        let directory = directory(&body, &context.cluster)?;
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
