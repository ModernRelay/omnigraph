//! Layered config merge — RFC-002 §4. Folds parsed config layers low→high into
//! one merged [`OmnigraphConfig`] plus a per-field [`Provenance`] map that
//! `config view --show-origin` reads. Every layer reaching here is already
//! version-gated, legacy-scanned, and path-resolved (absolute), so merge is pure
//! structure — it never touches the filesystem or re-parses.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::{Layer, OmnigraphConfig, Provenance};

/// One parsed config layer fed to [`merge_layers`].
pub struct LoadedLayer {
    pub layer: Layer,
    pub config: OmnigraphConfig,
}

/// Fold `layers` (ordered low→high) into a single merged config and its per-field
/// origin map. Merge rules:
/// - **settings-objects** (`defaults`, `serve`, `auth`, `query`, top-level
///   `policy`) deep-merge per leaf: a higher layer's `Some`/non-empty value wins,
///   `None`/empty inherits the lower;
/// - **named-resource maps** (`servers`, `graphs`, `aliases`, top-level
///   `queries`) union by key, a higher layer's entry replacing the lower
///   *wholesale* (no intra-entry deep-merge);
/// - **lists** (`serve.graphs`, `query.roots`) and **scalars** (`version`)
///   replace.
///
/// The legacy `project`/`server` fields are empty after a layer's load (folded or
/// rejected), and per-layer deprecation warnings are surfaced by the caller from
/// each layer directly — so neither is merged here.
pub fn merge_layers(layers: Vec<LoadedLayer>) -> (OmnigraphConfig, Provenance) {
    let mut acc = OmnigraphConfig::default();
    let mut prov: BTreeMap<String, Layer> = BTreeMap::new();
    let mut version = None;
    let mut base_dir = PathBuf::new();
    let mut loaded_from_file = false;

    for LoadedLayer { layer, config } in layers {
        let OmnigraphConfig {
            version: layer_version,
            defaults,
            serve,
            auth,
            query,
            policy,
            servers,
            graphs,
            aliases,
            queries,
            base_dir: layer_base_dir,
            loaded_from_file: layer_loaded,
            // Ignored: `project` (no consumer) and `server` (folded into `serve`
            // at load) are empty here; `legacy_keys` drives per-layer warnings the
            // caller emits before merge.
            ..
        } = config;

        if layer_version.is_some() {
            version = layer_version;
        }
        if layer_loaded {
            loaded_from_file = true;
            base_dir = layer_base_dir;
        }

        // Settings-objects — deep-merge per leaf.
        take(
            &mut acc.defaults.graph,
            defaults.graph,
            "defaults.graph",
            layer,
            &mut prov,
        );
        take(
            &mut acc.defaults.branch,
            defaults.branch,
            "defaults.branch",
            layer,
            &mut prov,
        );
        take(
            &mut acc.defaults.output_format,
            defaults.output_format,
            "defaults.output_format",
            layer,
            &mut prov,
        );
        take(
            &mut acc.defaults.table_max_column_width,
            defaults.table_max_column_width,
            "defaults.table_max_column_width",
            layer,
            &mut prov,
        );
        take(
            &mut acc.defaults.table_cell_layout,
            defaults.table_cell_layout,
            "defaults.table_cell_layout",
            layer,
            &mut prov,
        );
        take(
            &mut acc.defaults.actor,
            defaults.actor,
            "defaults.actor",
            layer,
            &mut prov,
        );

        take(
            &mut acc.serve.bind,
            serve.bind,
            "serve.bind",
            layer,
            &mut prov,
        );
        take(
            &mut acc.serve.policy.file,
            serve.policy.file,
            "serve.policy.file",
            layer,
            &mut prov,
        );
        if !serve.graphs.is_empty() {
            acc.serve.graphs = serve.graphs;
            prov.insert("serve.graphs".to_string(), layer);
        }

        take(
            &mut acc.auth.env_file,
            auth.env_file,
            "auth.env_file",
            layer,
            &mut prov,
        );
        take(
            &mut acc.policy.file,
            policy.file,
            "policy.file",
            layer,
            &mut prov,
        );

        if !query.roots.is_empty() {
            acc.query.roots = query.roots;
            prov.insert("query.roots".to_string(), layer);
        }

        // Named-resource maps — union by key, higher entry replaces wholesale.
        merge_map(&mut acc.servers, servers, "servers", layer, &mut prov);
        merge_map(&mut acc.graphs, graphs, "graphs", layer, &mut prov);
        merge_map(&mut acc.aliases, aliases, "aliases", layer, &mut prov);
        merge_map(&mut acc.queries, queries, "queries", layer, &mut prov);
    }

    acc.version = version;
    acc.base_dir = base_dir;
    acc.loaded_from_file = loaded_from_file;
    (acc, Provenance(prov))
}

/// Overwrite `acc` with `incoming` and record its `layer` when `incoming` is set;
/// `None` leaves the lower layer's value and provenance intact.
fn take<T>(
    acc: &mut Option<T>,
    incoming: Option<T>,
    path: &str,
    layer: Layer,
    prov: &mut BTreeMap<String, Layer>,
) {
    if incoming.is_some() {
        *acc = incoming;
        prov.insert(path.to_string(), layer);
    }
}

/// Union `incoming` into `acc` by key — a colliding key takes the higher layer's
/// entry wholesale (no intra-entry merge) — recording `prefix.<key>` provenance.
fn merge_map<V>(
    acc: &mut BTreeMap<String, V>,
    incoming: BTreeMap<String, V>,
    prefix: &str,
    layer: Layer,
    prov: &mut BTreeMap<String, Layer>,
) {
    for (key, value) in incoming {
        prov.insert(format!("{prefix}.{key}"), layer);
        acc.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::{TempDir, tempdir};

    use super::{LoadedLayer, merge_layers};
    use crate::{Layer, OmnigraphConfig, ReadOutputFormat};

    /// Load a config from inline YAML in a fresh temp dir (kept alive by the
    /// returned `TempDir` so eager-resolved paths stay distinct per layer).
    fn config(yaml: &str) -> (OmnigraphConfig, TempDir) {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("omnigraph.yaml"), yaml).unwrap();
        let config = crate::load_config_in(temp.path(), None).unwrap();
        (config, temp)
    }

    fn layer(layer: Layer, config: OmnigraphConfig) -> LoadedLayer {
        LoadedLayer { layer, config }
    }

    #[test]
    fn merge_settings_deep_merges_per_leaf() {
        let (g, _g) = config("version: 1\ndefaults:\n  output_format: kv\n");
        let (p, _p) = config("version: 1\ndefaults:\n  graph: prod\n");
        let (merged, prov) = merge_layers(vec![layer(Layer::Global, g), layer(Layer::Project, p)]);

        // A leaf each layer set survives; neither shadows the other.
        assert_eq!(merged.default_output_format(), ReadOutputFormat::Kv);
        assert_eq!(merged.default_graph_name(), Some("prod"));
        assert_eq!(prov.origin("defaults.output_format"), Some(Layer::Global));
        assert_eq!(prov.origin("defaults.graph"), Some(Layer::Project));
    }

    #[test]
    fn merge_higher_layer_wins_same_leaf() {
        let (g, _g) = config("version: 1\ndefaults:\n  branch: main\n");
        let (p, _p) = config("version: 1\ndefaults:\n  branch: review\n");
        let (merged, prov) = merge_layers(vec![layer(Layer::Global, g), layer(Layer::Project, p)]);
        assert_eq!(merged.default_branch(), "review");
        assert_eq!(prov.origin("defaults.branch"), Some(Layer::Project));
    }

    #[test]
    fn merge_named_maps_replace_entry_wholesale() {
        // Global's `prod` carries a branch; project's `prod` does not. Wholesale
        // replace must drop the branch (no intra-entry bleed-through).
        let (g, _g) =
            config("version: 1\ngraphs:\n  prod:\n    storage: ./a.omni\n    branch: main\n");
        let (p, _p) = config("version: 1\ngraphs:\n  prod:\n    storage: ./b.omni\n");
        let (merged, prov) = merge_layers(vec![layer(Layer::Global, g), layer(Layer::Project, p)]);

        assert!(merged.graphs["prod"].uri.ends_with("b.omni"));
        assert_eq!(
            merged.graphs["prod"].branch, None,
            "branch:main must not bleed through"
        );
        assert_eq!(prov.origin("graphs.prod"), Some(Layer::Project));
    }

    #[test]
    fn merge_unions_disjoint_map_keys() {
        let (g, _g) = config("version: 1\nservers:\n  a: { endpoint: https://a }\n");
        let (p, _p) = config("version: 1\nservers:\n  b: { endpoint: https://b }\n");
        let (merged, prov) = merge_layers(vec![layer(Layer::Global, g), layer(Layer::Project, p)]);

        assert!(merged.servers.contains_key("a") && merged.servers.contains_key("b"));
        assert_eq!(prov.origin("servers.a"), Some(Layer::Global));
        assert_eq!(prov.origin("servers.b"), Some(Layer::Project));
    }

    #[test]
    fn merge_lists_replace_not_append() {
        let (g, _g) = config("version: 1\nquery:\n  roots: [g]\n");
        let (p, _p) = config("version: 1\nquery:\n  roots: [p]\n");
        let (merged, _prov) = merge_layers(vec![layer(Layer::Global, g), layer(Layer::Project, p)]);
        // Lists replace; the result is exactly the project's (one entry, eager-resolved).
        assert_eq!(merged.query.roots.len(), 1);
        assert!(merged.query.roots[0].ends_with("p"));
    }

    #[test]
    fn merge_provenance_iterates_sorted() {
        let (g, _g) = config("version: 1\nservers:\n  b: { endpoint: https://b }\n");
        let (p, _p) =
            config("version: 1\ndefaults:\n  graph: x\nservers:\n  a: { endpoint: https://a }\n");
        let (_merged, prov) = merge_layers(vec![layer(Layer::Global, g), layer(Layer::Project, p)]);
        let keys: Vec<&String> = prov.iter().map(|(k, _)| k).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(
            keys, sorted,
            "provenance must iterate in deterministic sorted order"
        );
    }
}
