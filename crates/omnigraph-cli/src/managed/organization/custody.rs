//! Exact organization requests survive transport and output failures unchanged.
use super::*;
use crate::managed::custody as files;
use sha2::{Digest as _, Sha256};
use std::fs::File;
use std::path::PathBuf;

const RECORDS: &str = "requests.json";
const MAX_RECORDS: usize = 128;
const MAX_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExactRequest {
    pub path: String,
    pub body: Value,
}

impl ExactRequest {
    fn valid(&self, operation: &Retained, organization: &str) -> bool {
        let user = self.body["user_id"].as_str();
        let membership = self.body["expected_membership_id"].as_str();
        let selector_matches = |selector: &str, id: &str| email(selector) || selector == id;
        let set_role = |selector: &str, field: &str, roles: Value| {
            user.is_some_and(|user| {
                identity(user)
                    && selector_matches(selector, user)
                    && membership.is_some_and(valid_membership)
                    && self.path == format!("/v1/organization/members/{user}:set-role")
                    && self.body
                        == json!({
                            "version": 1,
                            "organization_id": organization,
                            "user_id": user,
                            "expected_membership_id": membership,
                            field: roles,
                        })
            })
        };
        let operation = match operation {
            Retained::Current(operation) => operation,
            Retained::SingleRole(SingleRole::Set { selector, role }) => {
                return set_role(selector, "role", json!(role));
            }
        };
        match operation {
            Operation::Set { selector, roles } => set_role(selector, "roles", json!(roles)),
            Operation::Offboard { selector, expected_membership } => user.is_some_and(|user| {
                identity(user) && selector_matches(selector, user)
                    && match expected_membership.as_deref() {
                        Some("absent") => membership.is_none(),
                        Some(expected) => membership == Some(expected),
                        None => membership.is_some_and(valid_membership),
                    }
                    && self.path == format!("/v1/organization/members/{user}:offboard")
                    && self.body == json!({"version":2,"organization_id":organization,"user_id":user,"expected_membership_id":membership})
            }),
            Operation::Send { email, role } => self.path == "/v1/organization/invitations"
                && self.body == invitation_body(organization, email, role.as_deref()),
            Operation::Resend { selector } | Operation::Revoke { selector } => {
                let suffix = if matches!(operation, Operation::Resend { .. }) { ":resend" } else { ":revoke" };
                self.path.strip_prefix("/v1/organization/invitations/")
                    .and_then(|path| path.strip_suffix(suffix))
                    .is_some_and(|id| identity(id) && selector_matches(selector, id))
                    && self.body == json!({"organization_id":organization})
            }
        }
    }
}

/// A record's operation in the shape it was retained with. Rewriting the file
/// writes every earlier record back in its own spelling, never a translation.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Retained {
    Current(Operation),
    SingleRole(SingleRole),
}

/// `members set` as builds before role sets retained it, still record version 1:
/// one role, sent as `role`. The same command with that one `--role` selects it.
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum SingleRole {
    Set { selector: String, role: String },
}

impl Retained {
    fn operation(&self) -> Operation {
        match self {
            Self::Current(operation) => operation.clone(),
            Self::SingleRole(SingleRole::Set { selector, role }) => Operation::Set {
                selector: selector.clone(),
                roles: vec![role.clone()],
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u8,
    api: String,
    organization_id: String,
    principal_id: String,
    owner_profile: String,
    idempotency_key: String,
    operation: Retained,
    request: ExactRequest,
}

fn failure() -> Failure {
    Failure::new(
        "organization_record_failed",
        "could not safely read or persist organization request custody; preserve this operator home",
        1,
    )
}

fn full() -> Failure {
    Failure::refused(
        "organization_custody_full",
        "organization request custody is full; exact same-key retries remain available; no records were evicted",
    )
}

pub(super) struct Journal {
    directory: PathBuf,
    _lock: File,
    records: Vec<Record>,
    api: String,
    organization: String,
    principal: String,
    owner: String,
}

impl Journal {
    pub fn open(
        input: &crate::operator::InvocationConfig,
        binding: &Binding,
        api: &Api,
    ) -> Result<Self> {
        let home = input.directory().ok_or_else(|| Failure::refused("operator_home_required", "organization writes require OMNIGRAPH_HOME or a user home directory for exact request custody"))?;
        let organization = api.account_id()?;
        let identity = crate::operator_routing::data_profile_namespace().ok_or_else(failure)?;
        let directory = home
            .join("organization")
            .join(format!("{:x}", Sha256::digest(identity)));
        let lock = files::lock(&directory, "requests.lock").map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                Failure::refused(
                    "organization_submission_busy",
                    "another process is submitting for this organization; retry after it returns",
                )
            } else {
                failure()
            }
        })?;
        let records: Vec<Record> = files::read(&directory, RECORDS, MAX_BYTES)
            .map_err(|_| failure())?
            .unwrap_or_default();
        let mut keys = BTreeSet::new();
        if records.len() > MAX_RECORDS
            || records.iter().any(|record| {
                record.version != 1
                    || canonical_origin(&record.api).is_err()
                    || organization_id(&record.organization_id).is_err()
                    || !plain(&record.principal_id, 1024)
                    || !plain(&record.owner_profile, 1024)
                    || record.idempotency_key.is_empty()
                    || record.idempotency_key.len() > 256
                    || !record.idempotency_key.bytes().all(|b| b.is_ascii_graphic())
                    || !keys.insert(&record.idempotency_key)
                    || record.operation.operation().validate().is_err()
                    || !record
                        .request
                        .valid(&record.operation, &record.organization_id)
            })
        {
            return Err(failure());
        }
        Ok(Self {
            directory,
            _lock: lock,
            records,
            api: binding.api.clone(),
            organization: organization.into(),
            principal: api
                .selection
                .as_ref()
                .ok_or_else(Failure::protocol)?
                .principal_id
                .clone(),
            owner: crate::operator_routing::journal_owner().ok_or_else(failure)?,
        })
    }

    pub fn retained(&self, key: &str, operation: &Operation) -> Result<Option<ExactRequest>> {
        if let Some(record) = self
            .records
            .iter()
            .find(|record| record.idempotency_key == key)
        {
            if record.operation.operation() != *operation
                || record.api != self.api
                || record.organization_id != self.organization
                || record.principal_id != self.principal
                || record.owner_profile != self.owner
            {
                return Err(Failure::refused(
                    "organization_request_conflict",
                    "this key belongs to different arguments, principal or profile custody; retain the original request and identity",
                ));
            }
            return Ok(Some(record.request.clone()));
        }
        if self.records.len() == MAX_RECORDS {
            return Err(full());
        }
        Ok(None)
    }

    pub fn retain(
        &mut self,
        key: &str,
        operation: Operation,
        request: ExactRequest,
    ) -> Result<ExactRequest> {
        let operation = Retained::Current(operation);
        if !request.valid(&operation, &self.organization) {
            return Err(failure());
        }
        self.records.push(Record {
            version: 1,
            api: self.api.clone(),
            organization_id: self.organization.clone(),
            principal_id: self.principal.clone(),
            owner_profile: self.owner.clone(),
            idempotency_key: key.into(),
            operation,
            request: request.clone(),
        });
        if self.records.len() > MAX_RECORDS
            || serde_json::to_vec(&self.records)
                .map_err(|_| failure())?
                .len() as u64
                > MAX_BYTES
        {
            return Err(full());
        }
        files::save(&self.directory, RECORDS, &self.records, MAX_BYTES).map_err(|_| failure())?;
        Ok(request)
    }
}
