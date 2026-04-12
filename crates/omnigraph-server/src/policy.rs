use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use cedar_policy::{
    Authorizer, Context, Decision, Entities, Entity, EntityId, EntityTypeName, EntityUid, Policy,
    PolicyId, PolicySet, Request, Schema, ValidationMode, Validator,
};
use clap::ValueEnum;
use color_eyre::eyre::{Result, bail, eyre};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    Read,
    Export,
    Change,
    SchemaApply,
    BranchCreate,
    BranchDelete,
    BranchMerge,
    RunPublish,
    RunAbort,
    Admin,
}

impl PolicyAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Export => "export",
            Self::Change => "change",
            Self::SchemaApply => "schema_apply",
            Self::BranchCreate => "branch_create",
            Self::BranchDelete => "branch_delete",
            Self::BranchMerge => "branch_merge",
            Self::RunPublish => "run_publish",
            Self::RunAbort => "run_abort",
            Self::Admin => "admin",
        }
    }

    fn uses_branch_scope(self) -> bool {
        matches!(self, Self::Read | Self::Export | Self::Change)
    }

    fn uses_target_branch_scope(self) -> bool {
        matches!(
            self,
            Self::BranchCreate
                | Self::SchemaApply
                | Self::BranchDelete
                | Self::BranchMerge
                | Self::RunPublish
                | Self::RunAbort
        )
    }
}

impl fmt::Display for PolicyAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PolicyAction {
    type Err = color_eyre::eyre::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim() {
            "read" => Ok(Self::Read),
            "export" => Ok(Self::Export),
            "change" => Ok(Self::Change),
            "schema_apply" => Ok(Self::SchemaApply),
            "branch_create" => Ok(Self::BranchCreate),
            "branch_delete" => Ok(Self::BranchDelete),
            "branch_merge" => Ok(Self::BranchMerge),
            "run_publish" => Ok(Self::RunPublish),
            "run_abort" => Ok(Self::RunAbort),
            "admin" => Ok(Self::Admin),
            other => bail!("unknown policy action '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyBranchScope {
    Any,
    Protected,
    Unprotected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyActorSelector {
    pub group: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyAllowRule {
    pub actors: PolicyActorSelector,
    pub actions: Vec<PolicyAction>,
    pub branch_scope: Option<PolicyBranchScope>,
    pub target_branch_scope: Option<PolicyBranchScope>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    pub id: String,
    pub allow: PolicyAllowRule,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    pub version: u32,
    #[serde(default)]
    pub groups: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub protected_branches: Vec<String>,
    #[serde(default)]
    pub rules: Vec<PolicyRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyTestConfig {
    pub version: u32,
    #[serde(default)]
    pub cases: Vec<PolicyTestCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyTestCase {
    pub id: String,
    pub actor: String,
    pub action: PolicyAction,
    pub branch: Option<String>,
    pub target_branch: Option<String>,
    pub expect: PolicyExpectation,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyExpectation {
    Allow,
    Deny,
}

#[derive(Debug, Clone)]
pub struct PolicyRequest {
    pub actor_id: String,
    pub action: PolicyAction,
    pub branch: Option<String>,
    pub target_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PolicyDecision {
    pub allowed: bool,
    pub matched_rule_id: Option<String>,
    pub message: String,
}

pub struct PolicyCompiler;

#[derive(Clone)]
pub struct PolicyEngine {
    repo_id: String,
    protected_branches: BTreeSet<String>,
    known_actors: BTreeSet<String>,
    schema: Schema,
    entities: Entities,
    policies: PolicySet,
    policy_to_rule: HashMap<String, String>,
}

impl PolicyConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let config: Self = serde_yaml::from_str(&fs::read_to_string(path)?)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            bail!("policy version must be 1");
        }

        for (group, members) in &self.groups {
            if group.trim().is_empty() {
                bail!("policy group names must not be blank");
            }
            if members.is_empty() {
                bail!("policy group '{group}' must not be empty");
            }
            for actor in members {
                if actor.trim().is_empty() {
                    bail!("policy group '{group}' contains a blank actor id");
                }
            }
        }

        for branch in &self.protected_branches {
            if branch.trim().is_empty() {
                bail!("protected branch names must not be blank");
            }
        }

        let mut seen_rule_ids = HashSet::new();
        for rule in &self.rules {
            if rule.id.trim().is_empty() {
                bail!("policy rule ids must not be blank");
            }
            if !seen_rule_ids.insert(rule.id.clone()) {
                bail!("duplicate policy rule id '{}'", rule.id);
            }
            if rule.allow.actors.group.trim().is_empty() {
                bail!("policy rule '{}' must reference a non-blank group", rule.id);
            }
            if !self.groups.contains_key(rule.allow.actors.group.as_str()) {
                bail!(
                    "policy rule '{}' references unknown group '{}'",
                    rule.id,
                    rule.allow.actors.group
                );
            }
            if rule.allow.actions.is_empty() {
                bail!("policy rule '{}' must include at least one action", rule.id);
            }
            if rule.allow.branch_scope.is_some() && rule.allow.target_branch_scope.is_some() {
                bail!(
                    "policy rule '{}' may specify branch_scope or target_branch_scope, not both",
                    rule.id
                );
            }
            if let Some(_) = rule.allow.branch_scope {
                for action in &rule.allow.actions {
                    if !action.uses_branch_scope() {
                        bail!(
                            "policy rule '{}' uses branch_scope with unsupported action '{}'",
                            rule.id,
                            action
                        );
                    }
                }
            }
            if let Some(_) = rule.allow.target_branch_scope {
                for action in &rule.allow.actions {
                    if !action.uses_target_branch_scope() {
                        bail!(
                            "policy rule '{}' uses target_branch_scope with unsupported action '{}'",
                            rule.id,
                            action
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

impl PolicyTestConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let config: Self = serde_yaml::from_str(&fs::read_to_string(path)?)?;
        if config.version != 1 {
            bail!("policy test version must be 1");
        }
        let mut seen = HashSet::new();
        for case in &config.cases {
            if case.id.trim().is_empty() {
                bail!("policy test case ids must not be blank");
            }
            if !seen.insert(case.id.clone()) {
                bail!("duplicate policy test case id '{}'", case.id);
            }
            if case.actor.trim().is_empty() {
                bail!("policy test case '{}' must not use a blank actor", case.id);
            }
        }
        Ok(config)
    }
}

impl PolicyCompiler {
    pub fn compile(config: &PolicyConfig, repo_id: &str) -> Result<PolicyEngine> {
        config.validate()?;
        let (schema, schema_warnings) = Schema::from_cedarschema_str(policy_schema_source())?;
        let schema_warnings = schema_warnings
            .map(|warning| warning.to_string())
            .collect::<Vec<_>>();
        if !schema_warnings.is_empty() {
            bail!("policy schema warnings:\n{}", schema_warnings.join("\n"));
        }
        let entities = compile_entities(config, repo_id, &schema)?;
        let (policies, policy_to_rule) = compile_policies(config, repo_id)?;
        let validator = Validator::new(schema.clone());
        let validation = validator.validate(&policies, ValidationMode::Strict);
        let errors = validation
            .validation_errors()
            .map(|err| err.to_string())
            .collect::<Vec<_>>();
        if !errors.is_empty() {
            bail!("policy validation failed:\n{}", errors.join("\n"));
        }

        let known_actors = config
            .groups
            .values()
            .flat_map(|members| members.iter().cloned())
            .collect();
        Ok(PolicyEngine {
            repo_id: repo_id.to_string(),
            protected_branches: config.protected_branches.iter().cloned().collect(),
            known_actors,
            schema,
            entities,
            policies,
            policy_to_rule,
        })
    }
}

impl PolicyEngine {
    pub fn load(path: &Path, repo_id: &str) -> Result<Self> {
        let config = PolicyConfig::load(path)?;
        PolicyCompiler::compile(&config, repo_id)
    }

    pub fn authorize(&self, request: &PolicyRequest) -> Result<PolicyDecision> {
        if !self.known_actors.contains(request.actor_id.as_str()) {
            return Ok(self.deny(
                request,
                None,
                format!(
                    "policy denied action '{}' for unknown actor '{}'",
                    request.action, request.actor_id
                ),
            ));
        }

        let principal = entity_uid("Actor", &request.actor_id)?;
        let action = entity_uid("Action", request.action.as_str())?;
        let resource = entity_uid("Repo", &self.repo_id)?;
        let context_value = json!({
            "has_branch": request.branch.is_some(),
            "branch": request.branch.clone().unwrap_or_default(),
            "has_target_branch": request.target_branch.is_some(),
            "target_branch": request.target_branch.clone().unwrap_or_default(),
            "branch_is_protected": request.branch.as_ref().is_some_and(|branch| self.protected_branches.contains(branch)),
            "target_branch_is_protected": request.target_branch.as_ref().is_some_and(|branch| self.protected_branches.contains(branch)),
        });
        let context = Context::from_json_value(context_value, Some((&self.schema, &action)))?;
        let cedar_request = Request::new(principal, action, resource, context, Some(&self.schema))?;
        let response =
            Authorizer::new().is_authorized(&cedar_request, &self.policies, &self.entities);
        let errors = response
            .diagnostics()
            .errors()
            .map(|err| err.to_string())
            .collect::<Vec<_>>();
        if !errors.is_empty() {
            bail!("policy evaluation failed:\n{}", errors.join("\n"));
        }

        let matched_rule_id = response
            .diagnostics()
            .reason()
            .filter_map(|policy_id| {
                let key: &str = policy_id.as_ref();
                self.policy_to_rule.get(key).cloned()
            })
            .min();

        Ok(match response.decision() {
            Decision::Allow => PolicyDecision {
                allowed: true,
                matched_rule_id: matched_rule_id.clone(),
                message: format!(
                    "policy allowed action '{}' for actor '{}'",
                    request.action, request.actor_id
                ),
            },
            Decision::Deny => {
                let message = format!(
                    "policy denied action '{}'{}{} for actor '{}'",
                    request.action,
                    request
                        .branch
                        .as_deref()
                        .map(|branch| format!(" on branch '{}'", branch))
                        .unwrap_or_default(),
                    request
                        .target_branch
                        .as_deref()
                        .map(|branch| format!(" targeting branch '{}'", branch))
                        .unwrap_or_default(),
                    request.actor_id
                );
                self.deny(request, matched_rule_id, message)
            }
        })
    }

    pub fn validate_request(&self, request: &PolicyRequest) -> Result<()> {
        let _ = self.authorize(request)?;
        Ok(())
    }

    pub fn run_tests(&self, tests: &PolicyTestConfig) -> Result<()> {
        if tests.version != 1 {
            bail!("policy test version must be 1");
        }
        let mut failures = Vec::new();
        for case in &tests.cases {
            let decision = self.authorize(&PolicyRequest {
                actor_id: case.actor.clone(),
                action: case.action,
                branch: case.branch.clone(),
                target_branch: case.target_branch.clone(),
            })?;
            let expected_allowed = matches!(case.expect, PolicyExpectation::Allow);
            if decision.allowed != expected_allowed {
                failures.push(format!(
                    "{}: expected {:?} but got {}",
                    case.id,
                    case.expect,
                    if decision.allowed { "allow" } else { "deny" }
                ));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            bail!("policy tests failed:\n{}", failures.join("\n"))
        }
    }

    pub fn known_actor_count(&self) -> usize {
        self.known_actors.len()
    }

    fn deny(
        &self,
        _request: &PolicyRequest,
        matched_rule_id: Option<String>,
        message: String,
    ) -> PolicyDecision {
        PolicyDecision {
            allowed: false,
            matched_rule_id,
            message,
        }
    }
}

fn compile_entities(config: &PolicyConfig, repo_id: &str, schema: &Schema) -> Result<Entities> {
    let mut group_entities = Vec::new();
    for group in config.groups.keys() {
        group_entities.push(Entity::new(
            entity_uid("Group", group)?,
            HashMap::new(),
            HashSet::<EntityUid>::new(),
        )?);
    }

    let mut actor_groups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (group, members) in &config.groups {
        for actor in members {
            actor_groups
                .entry(actor.clone())
                .or_default()
                .insert(group.clone());
        }
    }

    let mut actor_entities = Vec::new();
    for (actor, groups) in actor_groups {
        let parents = groups
            .iter()
            .map(|group| entity_uid("Group", group))
            .collect::<Result<HashSet<_>>>()?;
        actor_entities.push(Entity::new(
            entity_uid("Actor", &actor)?,
            HashMap::new(),
            parents,
        )?);
    }

    let repo_entity = Entity::new(
        entity_uid("Repo", repo_id)?,
        HashMap::new(),
        HashSet::<EntityUid>::new(),
    )?;

    let mut entities = Vec::new();
    entities.extend(group_entities);
    entities.extend(actor_entities);
    entities.push(repo_entity);
    Ok(Entities::from_entities(entities, Some(schema))?)
}

fn compile_policies(
    config: &PolicyConfig,
    repo_id: &str,
) -> Result<(PolicySet, HashMap<String, String>)> {
    let mut policies = Vec::new();
    let mut policy_to_rule = HashMap::new();

    for rule in &config.rules {
        for action in &rule.allow.actions {
            let policy_id = PolicyId::new(format!("{}:{}", rule.id, action.as_str()));
            let source = compile_policy_source(rule, action, repo_id);
            let policy = Policy::parse(Some(policy_id.clone()), source.as_str())?;
            policy_to_rule.insert(policy_id.to_string(), rule.id.clone());
            policies.push(policy);
        }
    }

    Ok((PolicySet::from_policies(policies)?, policy_to_rule))
}

fn compile_policy_source(rule: &PolicyRule, action: &PolicyAction, repo_id: &str) -> String {
    let mut conditions = Vec::new();
    if let Some(scope) = rule.allow.branch_scope {
        conditions.push(branch_scope_condition(scope));
    }
    if let Some(scope) = rule.allow.target_branch_scope {
        conditions.push(target_branch_scope_condition(scope));
    }

    let when = if conditions.is_empty() {
        String::new()
    } else {
        format!("\nwhen {{ {} }}", conditions.join(" && "))
    };

    format!(
        r#"permit (
    principal in Omnigraph::Group::{group},
    action == Omnigraph::Action::{action},
    resource == Omnigraph::Repo::{repo}
){when};"#,
        group = cedar_literal(&rule.allow.actors.group),
        action = cedar_literal(action.as_str()),
        repo = cedar_literal(repo_id),
        when = when,
    )
}

fn branch_scope_condition(scope: PolicyBranchScope) -> String {
    match scope {
        PolicyBranchScope::Any => "true".to_string(),
        PolicyBranchScope::Protected => {
            "context.has_branch && context.branch_is_protected".to_string()
        }
        PolicyBranchScope::Unprotected => {
            "context.has_branch && context.branch_is_protected == false".to_string()
        }
    }
}

fn target_branch_scope_condition(scope: PolicyBranchScope) -> String {
    match scope {
        PolicyBranchScope::Any => "true".to_string(),
        PolicyBranchScope::Protected => {
            "context.has_target_branch && context.target_branch_is_protected".to_string()
        }
        PolicyBranchScope::Unprotected => {
            "context.has_target_branch && context.target_branch_is_protected == false".to_string()
        }
    }
}

fn policy_schema_source() -> &'static str {
    r#"
namespace Omnigraph {
    type RequestContext = {
        has_branch: Bool,
        branch: String,
        has_target_branch: Bool,
        target_branch: String,
        branch_is_protected: Bool,
        target_branch_is_protected: Bool,
    };

    entity Actor in [Group];
    entity Group;
    entity Repo;

    action "read" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "export" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "change" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "schema_apply" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "branch_create" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "branch_delete" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "branch_merge" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "run_publish" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "run_abort" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
    action "admin" appliesTo { principal: Actor, resource: Repo, context: RequestContext };
}
"#
}

fn entity_uid(entity_type: &str, id: &str) -> Result<EntityUid> {
    let typename = EntityTypeName::from_str(&format!("Omnigraph::{entity_type}"))?;
    let entity_id = EntityId::from_str(id).map_err(|err| eyre!(err.to_string()))?;
    Ok(EntityUid::from_type_name_and_id(typename, entity_id))
}

fn cedar_literal(value: &str) -> String {
    serde_json::to_string(value).expect("string literal should serialize")
}

impl PolicyRequest {
    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    pub fn action(&self) -> PolicyAction {
        self.action
    }

    pub fn branch(&self) -> Option<&str> {
        self.branch.as_deref()
    }

    pub fn target_branch(&self) -> Option<&str> {
        self.target_branch.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PolicyAction, PolicyCompiler, PolicyConfig, PolicyExpectation, PolicyRequest,
        PolicyTestCase, PolicyTestConfig,
    };

    #[test]
    fn rejects_duplicate_rule_ids() {
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: same
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
  - id: same
    allow:
      actors: { group: team }
      actions: [export]
      branch_scope: any
"#,
        )
        .unwrap();

        let err = policy.validate().unwrap_err();
        assert!(err.to_string().contains("duplicate policy rule id"));
    }

    #[test]
    fn rejects_unknown_group_references() {
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: bad
    allow:
      actors: { group: admins }
      actions: [read]
      branch_scope: any
"#,
        )
        .unwrap();

        let err = policy.validate().unwrap_err();
        assert!(err.to_string().contains("references unknown group"));
    }

    #[test]
    fn rejects_invalid_scope_action_combinations() {
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: bad
    allow:
      actors: { group: team }
      actions: [branch_merge]
      branch_scope: protected
"#,
        )
        .unwrap();

        let err = policy.validate().unwrap_err();
        assert!(err.to_string().contains("unsupported action"));
    }

    #[test]
    fn compiles_and_authorizes_branch_and_target_rules() {
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  team: [act-andrew, act-bruno]
  admins: [act-andrew]
protected_branches: [main]
rules:
  - id: team-read
    allow:
      actors: { group: team }
      actions: [read, export]
      branch_scope: any
  - id: team-write
    allow:
      actors: { group: team }
      actions: [change]
      branch_scope: unprotected
  - id: admins-promote
    allow:
      actors: { group: admins }
      actions: [branch_delete, branch_merge, run_publish]
      target_branch_scope: protected
"#,
        )
        .unwrap();

        let engine = PolicyCompiler::compile(&policy, "repo").unwrap();
        let allow = engine
            .authorize(&PolicyRequest {
                actor_id: "act-bruno".to_string(),
                action: PolicyAction::Change,
                branch: Some("feature".to_string()),
                target_branch: None,
            })
            .unwrap();
        assert!(allow.allowed);
        assert_eq!(allow.matched_rule_id.as_deref(), Some("team-write"));

        let deny = engine
            .authorize(&PolicyRequest {
                actor_id: "act-bruno".to_string(),
                action: PolicyAction::BranchDelete,
                branch: None,
                target_branch: Some("main".to_string()),
            })
            .unwrap();
        assert!(!deny.allowed);

        let admin = engine
            .authorize(&PolicyRequest {
                actor_id: "act-andrew".to_string(),
                action: PolicyAction::BranchDelete,
                branch: None,
                target_branch: Some("main".to_string()),
            })
            .unwrap();
        assert!(admin.allowed);
        assert_eq!(admin.matched_rule_id.as_deref(), Some("admins-promote"));
    }

    #[test]
    fn policy_tests_enforce_expected_outcomes() {
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  team: [act-andrew]
protected_branches: [main]
rules:
  - id: team-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
"#,
        )
        .unwrap();
        let engine = PolicyCompiler::compile(&policy, "repo").unwrap();
        let tests = PolicyTestConfig {
            version: 1,
            cases: vec![
                PolicyTestCase {
                    id: "allow-read".to_string(),
                    actor: "act-andrew".to_string(),
                    action: PolicyAction::Read,
                    branch: Some("main".to_string()),
                    target_branch: None,
                    expect: PolicyExpectation::Allow,
                },
                PolicyTestCase {
                    id: "deny-change".to_string(),
                    actor: "act-andrew".to_string(),
                    action: PolicyAction::Change,
                    branch: Some("main".to_string()),
                    target_branch: None,
                    expect: PolicyExpectation::Deny,
                },
            ],
        };

        engine.run_tests(&tests).unwrap();
    }

    #[test]
    fn schema_apply_uses_target_branch_scope() {
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  admins: [act-ragnor]
protected_branches: [main]
rules:
  - id: admins-schema-apply
    allow:
      actors: { group: admins }
      actions: [schema_apply]
      target_branch_scope: protected
"#,
        )
        .unwrap();

        let engine = PolicyCompiler::compile(&policy, "repo").unwrap();
        let allow = engine
            .authorize(&PolicyRequest {
                actor_id: "act-ragnor".to_string(),
                action: PolicyAction::SchemaApply,
                branch: None,
                target_branch: Some("main".to_string()),
            })
            .unwrap();
        assert!(allow.allowed);

        let deny = engine
            .authorize(&PolicyRequest {
                actor_id: "act-ragnor".to_string(),
                action: PolicyAction::SchemaApply,
                branch: None,
                target_branch: Some("feature".to_string()),
            })
            .unwrap();
        assert!(!deny.allowed);
    }
}
