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
    /// Reserved for **policy-management** surfaces. Per MR-724 Option A,
    /// this gates operator actions like hot-reloading policy / tokens
    /// (MR-726), querying the audit log (MR-732), and listing /
    /// approving pending two-person-rule requests (MR-734). None of
    /// those endpoints exist yet, so today no engine or HTTP code
    /// calls `enforce(Admin, ...)`. The variant is kept in the enum so
    /// the action vocabulary is complete from chassis day one — when
    /// the first consumer surface ships, it can just call
    /// `enforce(Admin, ResourceScope::Graph, actor)` without needing
    /// to add the enum variant + update policy.yaml schemas + redeploy.
    ///
    /// Operators can write Cedar rules referencing `admin` today; they
    /// won't fire (no call site) but they're load-bearing for the
    /// future shape. Avoid writing such rules until the first consumer
    /// endpoint ships to prevent confusion.
    Admin,
    /// MR-668: management action that operates on the server's graph
    /// registry, not on a single graph's contents. The Cedar `appliesTo`
    /// declaration binds it to `resource: Server` instead of the
    /// per-graph `resource: Graph`. Operators authorize a group with:
    /// ```yaml
    /// rules:
    ///   - id: admins-can-list-graphs
    ///     allow:
    ///       actors: { group: admins }
    ///       actions: [graph_list]
    /// ```
    /// `branch_scope` and `target_branch_scope` are NOT supported for
    /// this action — there's no branch context at the server level.
    /// Runtime `graph_create` / `graph_delete` are intentionally omitted
    /// from v0.6.0; operators add and remove graphs by editing
    /// `omnigraph.yaml` and restarting.
    GraphList,
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
            Self::Admin => "admin",
            Self::GraphList => "graph_list",
        }
    }

    fn uses_branch_scope(self) -> bool {
        matches!(self, Self::Read | Self::Export | Self::Change)
    }

    fn uses_target_branch_scope(self) -> bool {
        matches!(
            self,
            Self::BranchCreate | Self::SchemaApply | Self::BranchDelete | Self::BranchMerge
        )
    }

    /// Which Cedar resource entity governs this action.
    /// Per-graph actions (Read, Change, …) apply to `Omnigraph::Graph::"<id>"`.
    /// Server-scoped management actions (GraphList) apply to
    /// `Omnigraph::Server::"root"`. `Admin` is reserved without a current
    /// call site; classified as per-graph until MR-724 picks a shape.
    pub fn resource_kind(self) -> PolicyResourceKind {
        match self {
            Self::GraphList => PolicyResourceKind::Server,
            Self::Read
            | Self::Export
            | Self::Change
            | Self::SchemaApply
            | Self::BranchCreate
            | Self::BranchDelete
            | Self::BranchMerge
            | Self::Admin => PolicyResourceKind::Graph,
        }
    }
}

/// Which Cedar entity an action's policies apply to. Internal to
/// `omnigraph-policy` — drives the `compile_policy_source` template
/// and the request-time resource UID construction.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PolicyResourceKind {
    /// `Omnigraph::Graph::"<graph_label>"` — per-graph actions.
    Graph,
    /// `Omnigraph::Server::"root"` — management actions.
    Server,
}

/// Which kind of policy file the caller is loading. Drives the
/// load-time validation that catches a "wrong action in wrong file"
/// mistake — a graph policy with `graph_list` rules, or a server
/// policy with `read` rules, both compile silently as Cedar but
/// never match any actual request. Typing the loader makes the
/// mistake a load-time error.
///
/// Pairs with [`PolicyAction::resource_kind`]: every action's resource
/// kind must match the engine kind it's loaded under.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PolicyEngineKind {
    /// Engine is loaded for a single graph; only actions whose
    /// `resource_kind()` is `PolicyResourceKind::Graph` are allowed.
    Graph,
    /// Engine is loaded for server-level management endpoints; only
    /// actions whose `resource_kind()` is `PolicyResourceKind::Server`
    /// are allowed.
    Server,
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
            "admin" => Ok(Self::Admin),
            "graph_list" => Ok(Self::GraphList),
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

/// What a caller wants to do, sans identity. Actor identity flows
/// through a separate `actor_id: &str` parameter on
/// [`PolicyEngine::authorize`] / [`PolicyChecker::check`] — encoding
/// the architectural invariant that actor identity is server-authoritative
/// and must not be supplied by the same code path that supplies the
/// requested action. In the HTTP layer, the bearer-token middleware
/// resolves the actor and passes it independently; clients cannot
/// smuggle identity inside this struct.
#[derive(Debug, Clone)]
pub struct PolicyRequest {
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
    graph_id: String,
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
            // MR-668: server-scoped actions have no branch context and
            // must not be mixed with per-graph actions in the same
            // rule (each rule generates one Cedar `permit` referencing
            // a specific resource kind).
            let mut server_scoped = false;
            let mut graph_scoped = false;
            for action in &rule.allow.actions {
                match action.resource_kind() {
                    PolicyResourceKind::Server => server_scoped = true,
                    PolicyResourceKind::Graph => graph_scoped = true,
                }
            }
            if server_scoped && graph_scoped {
                bail!(
                    "policy rule '{}' mixes the server-scoped action `graph_list` \
                     with per-graph actions; split into separate rules",
                    rule.id
                );
            }
            if server_scoped
                && (rule.allow.branch_scope.is_some() || rule.allow.target_branch_scope.is_some())
            {
                bail!(
                    "policy rule '{}' uses branch_scope/target_branch_scope with a \
                     server-scoped action; server-scoped actions have no branch context",
                    rule.id
                );
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
    pub fn compile(config: &PolicyConfig, graph_id: &str) -> Result<PolicyEngine> {
        config.validate()?;
        let (schema, schema_warnings) = Schema::from_cedarschema_str(policy_schema_source())?;
        let schema_warnings = schema_warnings
            .map(|warning| warning.to_string())
            .collect::<Vec<_>>();
        if !schema_warnings.is_empty() {
            bail!("policy schema warnings:\n{}", schema_warnings.join("\n"));
        }
        let entities = compile_entities(config, graph_id, &schema)?;
        let (policies, policy_to_rule) = compile_policies(config, graph_id)?;
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
            graph_id: graph_id.to_string(),
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
    /// Load a per-graph policy file. Rejects rules whose actions are
    /// server-scoped (e.g. `graph_list`) — those belong in a server
    /// policy file, not a per-graph one.
    ///
    /// `graph_id` is the label of the graph this engine governs;
    /// becomes the Cedar `Omnigraph::Graph::"<graph_id>"` resource
    /// for every per-graph action evaluated against this engine.
    pub fn load_graph(path: &Path, graph_id: &str) -> Result<Self> {
        let config = PolicyConfig::load(path)?;
        validate_kind_alignment(&config, PolicyEngineKind::Graph)?;
        PolicyCompiler::compile(&config, graph_id)
    }

    /// Load a server-level policy file. Rejects rules whose actions
    /// are per-graph (e.g. `read`, `change`) — those belong in a
    /// per-graph policy file, not the server one. Takes no `graph_id`:
    /// server-scoped actions resolve against the singleton
    /// `Omnigraph::Server::"root"` entity, never a Graph.
    pub fn load_server(path: &Path) -> Result<Self> {
        let config = PolicyConfig::load(path)?;
        validate_kind_alignment(&config, PolicyEngineKind::Server)?;
        // The Graph entity created by the compiler is never referenced
        // by a server-scoped rule, so the label below is purely a
        // placeholder. Use the canonical SERVER_RESOURCE_ID so any
        // future inspection of an unreachable Graph entity at least
        // points at the right concept.
        PolicyCompiler::compile(&config, SERVER_RESOURCE_ID)
    }

    /// Evaluate a request. `actor_id` is supplied as a separate
    /// argument (not inside `PolicyRequest`) so the type system enforces
    /// the "server-authoritative actor identity" invariant — clients
    /// supplying a `PolicyRequest` cannot smuggle identity through the
    /// same struct that carries the requested action.
    pub fn authorize(&self, actor_id: &str, request: &PolicyRequest) -> Result<PolicyDecision> {
        if !self.known_actors.contains(actor_id) {
            return Ok(self.deny(
                None,
                format!(
                    "policy denied action '{}' for unknown actor '{}'",
                    request.action, actor_id
                ),
            ));
        }

        let principal = entity_uid("Actor", actor_id)?;
        let action = entity_uid("Action", request.action.as_str())?;
        // Pick the resource entity based on the action's `resource_kind`.
        // Server-scoped actions (`graph_list`) bind to
        // `Omnigraph::Server::"root"`; per-graph actions bind to
        // `Omnigraph::Graph::"<graph_label>"`.
        let resource = match request.action.resource_kind() {
            PolicyResourceKind::Server => entity_uid("Server", SERVER_RESOURCE_ID)?,
            PolicyResourceKind::Graph => entity_uid("Graph", &self.graph_id)?,
        };
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
                    request.action, actor_id
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
                    actor_id
                );
                self.deny(matched_rule_id, message)
            }
        })
    }

    pub fn run_tests(&self, tests: &PolicyTestConfig) -> Result<()> {
        if tests.version != 1 {
            bail!("policy test version must be 1");
        }
        let mut failures = Vec::new();
        for case in &tests.cases {
            let decision = self.authorize(
                &case.actor,
                &PolicyRequest {
                    action: case.action,
                    branch: case.branch.clone(),
                    target_branch: case.target_branch.clone(),
                },
            )?;
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

    fn deny(&self, matched_rule_id: Option<String>, message: String) -> PolicyDecision {
        PolicyDecision {
            allowed: false,
            matched_rule_id,
            message,
        }
    }
}

/// Reject any rule whose actions don't match the engine kind
/// being loaded. Closes the "wrong action in wrong file silently
/// no-ops" class — `graph_list` in a per-graph file or `read` in
/// a server file fails at load time instead of compiling cleanly
/// and never matching a request.
fn validate_kind_alignment(config: &PolicyConfig, kind: PolicyEngineKind) -> Result<()> {
    let required = match kind {
        PolicyEngineKind::Graph => PolicyResourceKind::Graph,
        PolicyEngineKind::Server => PolicyResourceKind::Server,
    };
    for rule in &config.rules {
        for action in &rule.allow.actions {
            if action.resource_kind() != required {
                let (got, expected_file) = match action.resource_kind() {
                    PolicyResourceKind::Server => ("server-scoped", "server policy file"),
                    PolicyResourceKind::Graph => ("per-graph", "per-graph policy file"),
                };
                bail!(
                    "policy rule '{}' uses {} action '{}' in a {:?} policy file; \
                     move it to a {}",
                    rule.id,
                    got,
                    action,
                    kind,
                    expected_file
                );
            }
        }
    }
    Ok(())
}

fn compile_entities(config: &PolicyConfig, graph_id: &str, schema: &Schema) -> Result<Entities> {
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

    let graph_entity = Entity::new(
        entity_uid("Graph", graph_id)?,
        HashMap::new(),
        HashSet::<EntityUid>::new(),
    )?;

    let mut entities = Vec::new();
    entities.extend(group_entities);
    entities.extend(actor_entities);
    entities.push(graph_entity);

    // MR-668: include the `Omnigraph::Server::"root"` entity
    // whenever any rule references a server-scoped action. Cedar's
    // schema validator will otherwise reject the policy. Keeping this
    // conditional (rather than always-on) avoids polluting test
    // assertions for graph-only policies.
    let any_server_scoped = config.rules.iter().any(|rule| {
        rule.allow
            .actions
            .iter()
            .any(|action| action.resource_kind() == PolicyResourceKind::Server)
    });
    if any_server_scoped {
        entities.push(Entity::new(
            entity_uid("Server", SERVER_RESOURCE_ID)?,
            HashMap::new(),
            HashSet::<EntityUid>::new(),
        )?);
    }

    Ok(Entities::from_entities(entities, Some(schema))?)
}

fn compile_policies(
    config: &PolicyConfig,
    graph_id: &str,
) -> Result<(PolicySet, HashMap<String, String>)> {
    let mut policies = Vec::new();
    let mut policy_to_rule = HashMap::new();

    for rule in &config.rules {
        for action in &rule.allow.actions {
            let policy_id = PolicyId::new(format!("{}:{}", rule.id, action.as_str()));
            let source = compile_policy_source(rule, action, graph_id);
            let policy = Policy::parse(Some(policy_id.clone()), source.as_str())?;
            policy_to_rule.insert(policy_id.to_string(), rule.id.clone());
            policies.push(policy);
        }
    }

    Ok((PolicySet::from_policies(policies)?, policy_to_rule))
}

fn compile_policy_source(rule: &PolicyRule, action: &PolicyAction, graph_id: &str) -> String {
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

    // MR-668: emit the resource literal that matches the action's
    // `resource_kind`. Per-graph actions reference the engine's
    // `Omnigraph::Graph::"<graph_label>"` instance; server-scoped
    // actions reference the singleton `Omnigraph::Server::"root"`.
    let resource_literal = match action.resource_kind() {
        PolicyResourceKind::Graph => {
            format!("Omnigraph::Graph::{}", cedar_literal(graph_id))
        }
        PolicyResourceKind::Server => {
            format!("Omnigraph::Server::{}", cedar_literal(SERVER_RESOURCE_ID))
        }
    };

    format!(
        r#"permit (
    principal in Omnigraph::Group::{group},
    action == Omnigraph::Action::{action},
    resource == {resource_literal}
){when};"#,
        group = cedar_literal(&rule.allow.actors.group),
        action = cedar_literal(action.as_str()),
        when = when,
        resource_literal = resource_literal,
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
    // MR-668: `entity Server;` plus the `graph_list` action that
    // binds to it. Per-graph actions stay bound to `Graph`.
    // The Cedar schema string lives here (not on a fixture file) so any
    // omnigraph-policy build picks up the new vocabulary in lock-step
    // with the Rust code.
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
    entity Graph;
    entity Server;

    action "read" appliesTo { principal: Actor, resource: Graph, context: RequestContext };
    action "export" appliesTo { principal: Actor, resource: Graph, context: RequestContext };
    action "change" appliesTo { principal: Actor, resource: Graph, context: RequestContext };
    action "schema_apply" appliesTo { principal: Actor, resource: Graph, context: RequestContext };
    action "branch_create" appliesTo { principal: Actor, resource: Graph, context: RequestContext };
    action "branch_delete" appliesTo { principal: Actor, resource: Graph, context: RequestContext };
    action "branch_merge" appliesTo { principal: Actor, resource: Graph, context: RequestContext };
    action "admin" appliesTo { principal: Actor, resource: Graph, context: RequestContext };

    action "graph_list" appliesTo { principal: Actor, resource: Server, context: RequestContext };
}
"#
}

/// Canonical id of the `Omnigraph::Server` Cedar entity. There's only one
/// (the running server); the id is fixed at `"root"` so Cedar rules can
/// reference it unambiguously: `resource == Omnigraph::Server::"root"`.
const SERVER_RESOURCE_ID: &str = "root";

fn entity_uid(entity_type: &str, id: &str) -> Result<EntityUid> {
    let typename = EntityTypeName::from_str(&format!("Omnigraph::{entity_type}"))?;
    let entity_id = EntityId::from_str(id).map_err(|err| eyre!(err.to_string()))?;
    Ok(EntityUid::from_type_name_and_id(typename, entity_id))
}

fn cedar_literal(value: &str) -> String {
    serde_json::to_string(value).expect("string literal should serialize")
}

impl PolicyRequest {
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

// ─── PolicyChecker trait + ResourceScope (MR-722 chassis core) ───────────────
//
// The trait below is the engine-layer integration point for policy
// enforcement. `Omnigraph::enforce()` calls `check()` at the head of
// every mutating method; consumers in the engine crate hold an
// `Arc<dyn PolicyChecker>` and don't reach into Cedar internals.
//
// Two enforcement layers compose via this trait — different methods,
// same Cedar policies:
//
// * **Engine-layer (this trait — `check`)** — coarse gate at operation
//   entry. Answers "can this actor invoke this action on this scope at all?"
// * **Query-layer (MR-725 — will add `predicate_for`)** — fine gate
//   inside the query planner. Answers "for the rows/types touched, which
//   can the actor see/modify?" Cedar predicates compile to DataFusion
//   `Expr` and push into the scan.
//
// The two layers have non-overlapping responsibilities and must not
// drift. `ResourceScope` deliberately stays at branch granularity;
// per-type and per-row scope live in MR-725 via the (future)
// `predicate_for` method. Do not add `Type(TypeRef)` or `Row(predicate)`
// variants to `ResourceScope` — that's the boundary the chassis design
// pins (see MR-722 design refinements comment, 2026-05-17).

/// Resource scope for a policy decision. Branch-grained on purpose —
/// per-type / per-row granularity is owned by the query-layer (MR-725).
///
/// The variants map to today's `(branch, target_branch)` pair convention
/// in [`PolicyRequest`]. Each writer in the engine picks the variant
/// that matches how the existing HTTP-layer Cedar policies were
/// written, so the engine-layer enforce() call and the HTTP-layer
/// authorize_request() call evaluate the same decision.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ResourceScope {
    /// Action applies to the graph as a whole (no branch context).
    /// Used by graph-level ops if any ever go through enforcement.
    /// Maps to `(branch: None, target_branch: None)`.
    Graph,
    /// Action operates on a single branch — reading from it, writing
    /// to it, mutating it. Maps to `(branch: Some(X), target_branch: None)`.
    /// Used by Read, Export, Change.
    Branch(String),
    /// Action targets a branch as its destination/effect. The action
    /// modifies this branch (SchemaApply applies the new schema to it)
    /// or removes it (BranchDelete). Maps to
    /// `(branch: None, target_branch: Some(X))`.
    /// Used by SchemaApply, BranchDelete.
    TargetBranch(String),
    /// Action transitions between two branches. `source` is the
    /// branch being read-from / merged-from / forked-from; `target`
    /// is the destination. Maps to
    /// `(branch: Some(source), target_branch: Some(target))`.
    /// Used by BranchCreate (from→new), BranchMerge (source→target).
    BranchTransition { source: String, target: String },
}

impl ResourceScope {
    /// Lower the scope into the (branch, target_branch) pair carried
    /// by today's [`PolicyRequest`]. The mapping preserves the
    /// HTTP-layer's existing scope conventions so Cedar policies don't
    /// have to be rewritten when engine-layer enforcement is enabled.
    pub fn to_branch_pair(&self) -> (Option<&str>, Option<&str>) {
        match self {
            ResourceScope::Graph => (None, None),
            ResourceScope::Branch(branch) => (Some(branch.as_str()), None),
            ResourceScope::TargetBranch(target) => (None, Some(target.as_str())),
            ResourceScope::BranchTransition { source, target } => {
                (Some(source.as_str()), Some(target.as_str()))
            }
        }
    }
}

/// Engine-layer policy enforcement error. `Denied` is the normal "policy
/// said no" path; `Internal` covers evaluation failures (malformed rule,
/// Cedar internal error, etc.).
#[derive(Debug, Clone)]
pub enum PolicyError {
    /// Policy evaluated successfully and denied the action.
    Denied(String),
    /// Policy evaluation itself failed (not a denial — a bug or
    /// configuration error).
    Internal(String),
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyError::Denied(msg) => write!(f, "policy denied: {msg}"),
            PolicyError::Internal(msg) => write!(f, "policy evaluation failed: {msg}"),
        }
    }
}

impl std::error::Error for PolicyError {}

/// Engine-layer policy enforcement trait. Implemented by `PolicyEngine`
/// (Cedar-backed) and any mock checker used in tests.
///
/// MR-725 will extend this trait with a query-layer pushdown method —
/// roughly `fn predicate_for(&self, type_ref: &TypeRef, actor: &str) ->
/// Option<DataFusionExpr>`. Engine and query-layer enforcement back to
/// the same Cedar policies but consume different methods. Don't conflate
/// them by overloading `check`.
pub trait PolicyChecker: Send + Sync {
    /// Engine-layer gate. Called at the head of every mutating engine
    /// method. `Ok(())` allows the action; `Err(PolicyError::Denied)`
    /// denies; `Err(PolicyError::Internal)` reports an evaluation bug.
    fn check(
        &self,
        action: PolicyAction,
        scope: &ResourceScope,
        actor: &str,
    ) -> Result<(), PolicyError>;
}

impl PolicyChecker for PolicyEngine {
    fn check(
        &self,
        action: PolicyAction,
        scope: &ResourceScope,
        actor: &str,
    ) -> Result<(), PolicyError> {
        let (branch, target_branch) = scope.to_branch_pair();
        let request = PolicyRequest {
            action,
            branch: branch.map(|s| s.to_string()),
            target_branch: target_branch.map(|s| s.to_string()),
        };
        let decision = self
            .authorize(actor, &request)
            .map_err(|e| PolicyError::Internal(e.to_string()))?;
        if decision.allowed {
            Ok(())
        } else {
            Err(PolicyError::Denied(decision.message))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PolicyAction, PolicyCompiler, PolicyConfig, PolicyEngine, PolicyExpectation, PolicyRequest,
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
      actions: [branch_delete, branch_merge]
      target_branch_scope: protected
"#,
        )
        .unwrap();

        let engine = PolicyCompiler::compile(&policy, "graph").unwrap();
        let allow = engine
            .authorize(
                "act-bruno",
                &PolicyRequest {
                    action: PolicyAction::Change,
                    branch: Some("feature".to_string()),
                    target_branch: None,
                },
            )
            .unwrap();
        assert!(allow.allowed);
        assert_eq!(allow.matched_rule_id.as_deref(), Some("team-write"));

        let deny = engine
            .authorize(
                "act-bruno",
                &PolicyRequest {
                    action: PolicyAction::BranchDelete,
                    branch: None,
                    target_branch: Some("main".to_string()),
                },
            )
            .unwrap();
        assert!(!deny.allowed);

        let admin = engine
            .authorize(
                "act-andrew",
                &PolicyRequest {
                    action: PolicyAction::BranchDelete,
                    branch: None,
                    target_branch: Some("main".to_string()),
                },
            )
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
        let engine = PolicyCompiler::compile(&policy, "graph").unwrap();
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

        let engine = PolicyCompiler::compile(&policy, "graph").unwrap();
        let allow = engine
            .authorize(
                "act-ragnor",
                &PolicyRequest {
                    action: PolicyAction::SchemaApply,
                    branch: None,
                    target_branch: Some("main".to_string()),
                },
            )
            .unwrap();
        assert!(allow.allowed);

        let deny = engine
            .authorize(
                "act-ragnor",
                &PolicyRequest {
                    action: PolicyAction::SchemaApply,
                    branch: None,
                    target_branch: Some("feature".to_string()),
                },
            )
            .unwrap();
        assert!(!deny.allowed);
    }

    // ─── MR-668 — server-scoped action (graph_list) ─

    #[test]
    fn graph_list_action_authorizes_against_server_resource() {
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  admins: [act-andrew]
  viewers: [act-bruno]
rules:
  - id: admins-list-graphs
    allow:
      actors: { group: admins }
      actions: [graph_list]
"#,
        )
        .unwrap();

        // The graph_label passed at compile time is irrelevant for
        // server-scoped actions — they resolve against
        // `Omnigraph::Server::"root"` regardless. We pass a sentinel
        // so it's obvious the value isn't used.
        let engine = PolicyCompiler::compile(&policy, "ignored").unwrap();

        let allow = engine
            .authorize(
                "act-andrew",
                &PolicyRequest {
                    action: PolicyAction::GraphList,
                    branch: None,
                    target_branch: None,
                },
            )
            .unwrap();
        assert!(allow.allowed);
        assert_eq!(allow.matched_rule_id.as_deref(), Some("admins-list-graphs"));

        // Different actor, same policy → deny.
        let deny = engine
            .authorize(
                "act-bruno",
                &PolicyRequest {
                    action: PolicyAction::GraphList,
                    branch: None,
                    target_branch: None,
                },
            )
            .unwrap();
        assert!(!deny.allowed);
    }

    #[test]
    fn server_scoped_rule_cannot_use_branch_scope() {
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  admins: [act-andrew]
rules:
  - id: bad-branch-scope-on-graph-list
    allow:
      actors: { group: admins }
      actions: [graph_list]
      branch_scope: any
"#,
        )
        .unwrap();
        let err = policy.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("branch_scope") || msg.contains("server-scoped"),
            "expected branch_scope rejection for server-scoped action; got: {msg}"
        );
    }

    #[test]
    fn rule_mixing_server_and_per_graph_actions_is_rejected() {
        // A single rule must reference exactly one resource kind.
        // `graph_list` (Server) + `read` (Graph) in one allow block
        // is invalid — operators must split the rule.
        let policy: PolicyConfig = serde_yaml::from_str(
            r#"
version: 1
groups:
  admins: [act-andrew]
rules:
  - id: mixed-resource-kinds
    allow:
      actors: { group: admins }
      actions: [graph_list, read]
"#,
        )
        .unwrap();
        let err = policy.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("server-scoped") || msg.contains("split into separate rules"),
            "expected mix-resource-kinds rejection; got: {msg}"
        );
    }

    #[test]
    fn per_graph_rules_continue_to_work_alongside_server_rules() {
        // Decision 6 contract: existing operator policies (which only
        // reference per-graph actions) keep compiling and authorizing
        // as before, even when the compiled-in schema now declares
        // `Server` + `graph_*` actions. This pins the "Cedar refactor
        // is operator-invisible" promise.
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
      actions: [read, export]
      branch_scope: any
"#,
        )
        .unwrap();
        let engine = PolicyCompiler::compile(&policy, "graph").unwrap();
        let allow = engine
            .authorize(
                "act-andrew",
                &PolicyRequest {
                    action: PolicyAction::Read,
                    branch: Some("main".to_string()),
                    target_branch: None,
                },
            )
            .unwrap();
        assert!(allow.allowed);
        assert_eq!(allow.matched_rule_id.as_deref(), Some("team-read"));
    }

    // ─── MR-668 follow-up — load_graph / load_server kind alignment ─

    /// A per-graph policy file containing a `graph_list` rule fails
    /// at load time. Pre-fix, the file compiled cleanly and the rule
    /// silently never matched (per-graph engine never gets a
    /// `graph_list` check). Closes the "wrong action, wrong file,
    /// silent no-op" class.
    #[test]
    fn load_graph_rejects_server_scoped_action() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad-graph-policy.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
groups:
  admins: [act-andrew]
rules:
  - id: misplaced-graph-list
    allow:
      actors: { group: admins }
      actions: [graph_list]
"#,
        )
        .unwrap();
        let err = match PolicyEngine::load_graph(&path, "g1") {
            Ok(_) => panic!("expected server-scoped action in per-graph file to be rejected"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("server-scoped") && msg.contains("graph_list"),
            "expected server-scoped-in-graph-file rejection, got: {msg}"
        );
    }

    /// A server policy file containing a `read` rule fails at load
    /// time. Pre-fix, the file compiled cleanly and the rule silently
    /// never matched (server engine never gets a `read` check).
    #[test]
    fn load_server_rejects_per_graph_action() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad-server-policy.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: misplaced-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
"#,
        )
        .unwrap();
        let err = match PolicyEngine::load_server(&path) {
            Ok(_) => panic!("expected per-graph action in server file to be rejected"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("per-graph") && msg.contains("read"),
            "expected per-graph-in-server-file rejection, got: {msg}"
        );
    }

    /// Positive case: a properly-shaped per-graph policy loads via
    /// `load_graph` and authorizes as expected. Verifies the
    /// kind-alignment check is permissive when the file is correct.
    #[test]
    fn load_graph_accepts_per_graph_only_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ok-graph-policy.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
groups:
  team: [act-andrew]
rules:
  - id: team-read
    allow:
      actors: { group: team }
      actions: [read]
      branch_scope: any
"#,
        )
        .unwrap();
        let engine = PolicyEngine::load_graph(&path, "g1").unwrap();
        let decision = engine
            .authorize(
                "act-andrew",
                &PolicyRequest {
                    action: PolicyAction::Read,
                    branch: Some("main".to_string()),
                    target_branch: None,
                },
            )
            .unwrap();
        assert!(decision.allowed);
    }

    /// Positive case: a properly-shaped server policy loads via
    /// `load_server` and authorizes the `graph_list` action.
    #[test]
    fn load_server_accepts_server_only_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ok-server-policy.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
groups:
  admins: [act-andrew]
rules:
  - id: admins-list-graphs
    allow:
      actors: { group: admins }
      actions: [graph_list]
"#,
        )
        .unwrap();
        let engine = PolicyEngine::load_server(&path).unwrap();
        let decision = engine
            .authorize(
                "act-andrew",
                &PolicyRequest {
                    action: PolicyAction::GraphList,
                    branch: None,
                    target_branch: None,
                },
            )
            .unwrap();
        assert!(decision.allowed);
    }
}
