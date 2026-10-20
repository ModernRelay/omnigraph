//! Explicit one-way conversion; these legacy keys are never used for routing.

use std::collections::BTreeMap;
use std::path::Path;

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use yaml_serde::{Mapping, Value};

use super::{StorageProvider, TargetEntry, TargetKind, TargetScope};

pub(crate) struct Imported {
    pub(crate) text: String,
    pub(crate) credentials: Vec<(String, String)>,
}

fn mapping(value: Option<Value>, name: &str) -> Result<Mapping> {
    match value {
        None | Some(Value::Null) => Ok(Mapping::new()),
        Some(Value::Mapping(m)) => Ok(m),
        _ => bail!("legacy {name} must be a mapping"),
    }
}
fn string(map: &Mapping, key: &str) -> Result<Option<String>> {
    map.get(key)
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or_else(|| eyre!("legacy {key} must be a string"))
        })
        .transpose()
}
fn require(map: &Mapping, key: &str) -> Result<String> {
    string(map, key)?.ok_or_else(|| eyre!("legacy binding is missing {key}"))
}
fn only(map: &Mapping, keys: &[&str]) -> Result<()> {
    for key in map.keys() {
        if !key.as_str().is_some_and(|key| keys.contains(&key)) {
            bail!("legacy entry contains an unknown field; repair it before importing");
        }
    }
    Ok(())
}
fn entry(
    kind: TargetKind,
    scope: Option<TargetScope>,
    uri: Option<String>,
    endpoint: Option<String>,
    credential: Option<String>,
    graph: Option<String>,
) -> TargetEntry {
    TargetEntry {
        kind,
        scope,
        uri,
        endpoint,
        credential,
        default_graph: graph,
        provider: None,
        auth: None,
        api: None,
        organization_id: None,
        cluster_id: None,
        cluster_incarnation: None,
        control_auth: None,
    }
}
fn direct(
    raw: String,
    scope: TargetScope,
    graph: Option<String>,
    base: &Path,
) -> Result<TargetEntry> {
    if !raw.contains("://") && !Path::new(&raw).is_absolute() {
        bail!(
            "legacy relative storage '{raw}' changed meaning with the working directory; make it absolute before importing"
        );
    }
    let uri = super::normalize_storage_uri(&raw, base)?;
    let mut target = entry(
        TargetKind::Direct,
        Some(scope),
        Some(uri.clone()),
        None,
        None,
        graph,
    );
    // Import is explicit and records the captured provider destination; runtime
    // reads of the converted target never inherit ambient destinations.
    target.provider = if uri.starts_with("s3://") || uri.starts_with("az://") {
        Some(StorageProvider::capture(&uri)?)
    } else {
        None
    };
    Ok(target)
}

pub(crate) fn convert(text: &str, base: &Path) -> Result<Imported> {
    let mut document = mapping(
        if text.trim().is_empty() {
            None
        } else {
            Some(yaml_serde::from_str(text)?)
        },
        "configuration",
    )?;
    if document.contains_key("target_config_version") {
        crate::operator::parse_operator_config(text)?;
        return Ok(Imported {
            text: text.into(),
            credentials: Vec::new(),
        });
    }
    if document.contains_key("targets") {
        bail!(
            "legacy file already contains targets without its version marker; repair it before importing"
        );
    }
    let servers = mapping(document.remove("servers"), "servers")?;
    let roots = mapping(document.remove("clusters"), "clusters")?;
    let profiles = mapping(document.remove("profiles"), "profiles")?;
    let mut defaults = mapping(document.remove("defaults"), "defaults")?;
    let mut aliases = mapping(document.remove("aliases"), "aliases")?;
    let mut targets: BTreeMap<String, TargetEntry> = BTreeMap::new();
    let mut credentials = Vec::new();
    let insert = |targets: &mut BTreeMap<String, TargetEntry>,
                  name: String,
                  target: TargetEntry|
     -> Result<()> {
        if !super::valid_name(&name) {
            bail!("legacy name '{name}' is not a valid target name; rename it before importing");
        }
        target.binding_at(&name, base)?;
        if targets.insert(name.clone(), target).is_some() {
            bail!(
                "legacy nickname '{name}' names more than one binding; rename the colliding entries before importing"
            );
        }
        Ok(())
    };
    for (name, value) in servers {
        let name = name
            .as_str()
            .ok_or_else(|| eyre!("server name must be a string"))?
            .to_owned();
        let server = mapping(Some(value), "server")?;
        only(&server, &["url"])?;
        let endpoint = require(&server, "url")?;
        insert(
            &mut targets,
            name.clone(),
            entry(
                TargetKind::Server,
                None,
                None,
                Some(endpoint.clone()),
                Some(name.clone()),
                None,
            ),
        )?;
        credentials.push((name, endpoint));
    }
    for (name, value) in roots {
        let name = name
            .as_str()
            .ok_or_else(|| eyre!("cluster name must be a string"))?
            .to_owned();
        let root = mapping(Some(value), "cluster")?;
        only(&root, &["root"])?;
        insert(
            &mut targets,
            name,
            direct(require(&root, "root")?, TargetScope::Root, None, base)?,
        )?;
    }
    for (name, value) in profiles {
        let name = name
            .as_str()
            .ok_or_else(|| eyre!("profile name must be a string"))?
            .to_owned();
        let profile = mapping(Some(value), "profile")?;
        only(&profile, &["server", "cluster", "store", "default_graph"])?;
        let bindings: Vec<(&str, String)> = ["server", "cluster", "store"]
            .into_iter()
            .map(|key| string(&profile, key).map(|value| value.map(|value| (key, value))))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        if bindings.len() != 1 {
            bail!("legacy profile '{name}' must bind exactly one server, cluster or store");
        }
        let graph = string(&profile, "default_graph")?;
        let (kind, value) = &bindings[0];
        let target = if *kind == "store" {
            direct(value.clone(), TargetScope::Graph, graph, base)?
        } else {
            let mut target = targets.get(value).cloned().ok_or_else(|| {
                eyre!("legacy profile '{name}' references unknown {kind} '{value}'")
            })?;
            if (*kind == "server" && target.kind != TargetKind::Server)
                || (*kind == "cluster" && target.scope != Some(TargetScope::Root))
            {
                bail!("legacy profile '{name}' references the wrong binding kind");
            }
            target.default_graph = graph;
            target
        };
        insert(&mut targets, name, target)?;
    }
    let default_server = string(&defaults, "server")?;
    let default_store = string(&defaults, "store")?;
    let default_graph = string(&defaults, "default_graph")?;
    defaults.remove("server");
    defaults.remove("store");
    defaults.remove("default_graph");
    if default_server.is_some() && default_store.is_some() {
        bail!("legacy defaults contain both server and store");
    }
    if let Some(server) = default_server {
        let mut target = targets
            .get(&server)
            .filter(|entry| entry.kind == TargetKind::Server)
            .cloned()
            .ok_or_else(|| eyre!("legacy default server is unknown"))?;
        let name = if default_graph.is_some() {
            target.default_graph = default_graph;
            insert(&mut targets, "default".into(), target)?;
            "default".to_owned()
        } else {
            server
        };
        defaults.insert("target".into(), name.into());
    } else if let Some(store) = default_store {
        insert(
            &mut targets,
            "default".into(),
            direct(store, TargetScope::Graph, default_graph, base)?,
        )?;
        defaults.insert("target".into(), "default".into());
    } else if default_graph.is_some() {
        bail!(
            "legacy default_graph has no destination; make the intended binding explicit before importing"
        );
    }
    for (name, value) in &mut aliases {
        let alias = value
            .as_mapping_mut()
            .ok_or_else(|| eyre!("legacy alias must be a mapping"))?;
        let server = alias
            .remove("server")
            .ok_or_else(|| eyre!("legacy alias is missing server"))?;
        if !server.as_str().is_some_and(|name| {
            targets
                .get(name)
                .is_some_and(|target| target.kind == TargetKind::Server)
        }) {
            bail!("legacy alias {name:?} references an unknown server");
        }
        alias.insert("target".into(), server);
    }
    document.insert("target_config_version".into(), 1.into());
    document.insert("targets".into(), yaml_serde::to_value(&targets)?);
    if !defaults.is_empty() {
        document.insert("defaults".into(), Value::Mapping(defaults));
    }
    if !aliases.is_empty() {
        document.insert("aliases".into(), Value::Mapping(aliases));
    }
    let text = yaml_serde::to_string(&document)?;
    crate::operator::parse_operator_config(&text)?;
    Ok(Imported { text, credentials })
}
