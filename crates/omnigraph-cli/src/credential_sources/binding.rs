use serde::Serialize;

use super::{CredentialError, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OwnerKind {
    Profile,
    Server,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct OwnerBinding {
    kind: OwnerKind,
    owner_id: String,
    profile_id: String,
    route: String,
    organization_id: Option<String>,
}

impl OwnerBinding {
    pub(crate) fn new(
        kind: OwnerKind,
        owner_id: String,
        profile_id: String,
        canonical_route: String,
        organization_id: Option<String>,
    ) -> Result<Self> {
        if !valid_field(&owner_id)
            || !valid_field(&profile_id)
            || !valid_route(&canonical_route)
            || organization_id
                .as_deref()
                .is_some_and(|value| !valid_field(value))
            || (kind == OwnerKind::Profile) != organization_id.is_some()
        {
            return Err(CredentialError::InvalidBinding);
        }
        Ok(Self {
            kind,
            owner_id,
            profile_id,
            route: canonical_route,
            organization_id,
        })
    }

    pub(crate) fn route(&self) -> &str {
        &self.route
    }

    pub(crate) fn organization_id(&self) -> Option<&str> {
        self.organization_id.as_deref()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Purpose {
    Control,
    Data,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ResourceIdentity {
    pub(crate) cluster_id: String,
    pub(crate) cluster_incarnation: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CredentialRequest {
    owner: OwnerBinding,
    purpose: Purpose,
    endpoint: String,
    audience: Option<String>,
    resource: Option<ResourceIdentity>,
}

impl CredentialRequest {
    pub(crate) fn new(
        owner: OwnerBinding,
        purpose: Purpose,
        canonical_endpoint: String,
        audience: Option<String>,
        resource: Option<ResourceIdentity>,
    ) -> Result<Self> {
        if !valid_route(&canonical_endpoint)
            || audience.as_deref().is_some_and(|value| !valid_field(value))
            || resource.as_ref().is_some_and(|value| {
                !valid_field(&value.cluster_id) || !valid_field(&value.cluster_incarnation)
            })
            || match (owner.kind, purpose) {
                (OwnerKind::Profile, Purpose::Control) => {
                    canonical_endpoint != owner.route || resource.is_some()
                }
                (OwnerKind::Profile, Purpose::Data) => resource.is_none(),
                (OwnerKind::Server, Purpose::Data) => {
                    canonical_endpoint != owner.route || resource.is_some()
                }
                (OwnerKind::Server, Purpose::Control) => true,
            }
        {
            return Err(CredentialError::InvalidBinding);
        }
        Ok(Self {
            owner,
            purpose,
            endpoint: canonical_endpoint,
            audience,
            resource,
        })
    }

    pub(crate) fn owner(&self) -> &OwnerBinding {
        &self.owner
    }
    pub(crate) fn purpose(&self) -> Purpose {
        self.purpose
    }
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }
    pub(crate) fn audience(&self) -> Option<&str> {
        self.audience.as_deref()
    }
    pub(crate) fn resource(&self) -> Option<&ResourceIdentity> {
        self.resource.as_ref()
    }
}

fn valid_field(value: &str) -> bool {
    !value.is_empty() && value.len() <= 8192 && !value.chars().any(char::is_control)
}

fn valid_route(value: &str) -> bool {
    valid_field(value)
        && url::Url::parse(value).is_ok_and(|url| {
            url.scheme() == "https"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
        })
}
