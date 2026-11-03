//! Every classification of a `PhysicalNode` kind is a token on its line of
//! the `physical_nodes!` declaration (`src/physical.rs`), and the matches the
//! macro generates list every kind. A hand-written `match` over
//! `PhysicalNode` is a behaviour site: it reads fields, and it is one of the
//! sites named below. This test parses the crate's sources with `syn` and
//! refuses (1) a `_` arm, or a bare binding, in a match that names half the
//! kinds or more, which would let a new kind take a default nobody declared,
//! and (2) a match over two or more kinds outside the named sites, which is a
//! classification that belongs on the declaration line. A selector that names
//! a few kinds and defaults the rest (`is_query`, `ranked`) keeps its default:
//! a new kind is not one of the few. A site is a file, the enclosing impl's
//! type when there is one, and the function. The walk recognises the
//! `PhysicalNode::Kind` and `Self::Kind` spellings, so a `use` that imports
//! the enum's variants or renames the enum, or a type alias of it, is refused
//! as well: nothing may match the kinds under another name.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use omnigraph_planner::PhysicalNodeKind;
use syn::visit::Visit;
use syn::{
    ExprMatch, ImplItemFn, ItemFn, ItemImpl, ItemType, ItemUse, Pat, Path as SynPath, Type, UseTree,
};

/// The hand-written matches over two or more `PhysicalNode` kinds (file,
/// function, what it reads of them); a constant per kind is a declaration
/// token, never an entry here.
const BEHAVIOUR_SITES: &[(&str, &str, &str)] = &[
    (
        "aggregate.rs",
        "validate_aggregate_specs",
        "the return expressions and aggregate specs of Aggregate, AntiJoin and MetadataCount",
    ),
    (
        "lower.rs",
        "PhysicalPlan::lower_node",
        "the walk: one Lower method per kind, called with the kind's fields",
    ),
    (
        "mirror.rs",
        "NodeMirror::from",
        "the serde mirror of every kind's fields, both directions",
    ),
    (
        "optimizer.rs",
        "derive_properties",
        "every kind's output schema, order, row estimate and bytes from its fields and inputs",
    ),
    (
        "optimizer.rs",
        "render",
        "a test module's plan text of SortMergeJoin, Scan and HydrateByAddress fields",
    ),
    (
        "output.rs",
        "validate_output_schemas",
        "the output schema the return-bearing kinds declare",
    ),
    (
        "physical.rs",
        "PhysicalPlan::is_query",
        "whether a plan reads a table: a Scan or a MetadataCount",
    ),
    (
        "physical.rs",
        "PhysicalPlan::node_json",
        "the explain JSON of every kind's fields",
    ),
    (
        "ranking.rs",
        "planned_ranking",
        "a Scan's ranked access and a RankFuse's arms",
    ),
    (
        "ranking.rs",
        "return_list",
        "the return expressions of a Projection or an Aggregate",
    ),
    (
        "validate/invariants.rs",
        "check_policies",
        "the declared policies of a Scan and a RankFuse",
    ),
    (
        "validate/invariants.rs",
        "Requirements::check_row_cuts",
        "the cuts of Limit, Scan and Sort justified against the query",
    ),
    (
        "validate/source.rs",
        "validate_catalogue_sources",
        "the catalogue-bound reads of MetadataCount, HydrateColumns, Scan and ContainsJoin",
    ),
    (
        "validate/subset.rs",
        "chain_of",
        "the subset chain through HydrateColumns, Scan, Filter, Projection, Sort and Limit",
    ),
    (
        "validate/tests.rs",
        "changed_aggregate_comparison_returns_are_refused",
        "a test fixture that rewrites an Aggregate's or a Projection's return list",
    ),
];

#[derive(Default)]
struct Walk {
    kinds: BTreeSet<String>,
    file: String,
    impl_self: Vec<String>,
    function: Vec<String>,
    sites: BTreeSet<(String, String)>,
    defaults: Vec<String>,
    renames: Vec<String>,
}

fn imports_kinds_or_renames(tree: &UseTree, under_node: bool) -> bool {
    match tree {
        UseTree::Path(path) => {
            imports_kinds_or_renames(&path.tree, under_node || path.ident == "PhysicalNode")
        }
        UseTree::Rename(rename) => under_node || rename.ident == "PhysicalNode",
        UseTree::Group(group) => group
            .items
            .iter()
            .any(|item| imports_kinds_or_renames(item, under_node)),
        UseTree::Name(_) | UseTree::Glob(_) => under_node,
    }
}

impl Walk {
    fn names_kind(&self, path: &SynPath) -> bool {
        let segments: Vec<String> = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        let [.., owner, variant] = segments.as_slice() else {
            return false;
        };
        let owner_is_node = owner == "PhysicalNode"
            || (owner == "Self" && self.impl_self.last().is_some_and(|ty| ty == "PhysicalNode"));
        owner_is_node && self.kinds.contains(variant)
    }

    fn collect(&self, pat: &Pat, named: &mut BTreeSet<String>, default: &mut bool) {
        match pat {
            Pat::Or(or) => {
                for case in &or.cases {
                    self.collect(case, named, default);
                }
            }
            Pat::Paren(paren) => self.collect(&paren.pat, named, default),
            Pat::Reference(reference) => self.collect(&reference.pat, named, default),
            Pat::Ident(ident) => match &ident.subpat {
                Some((_, subpat)) => self.collect(subpat, named, default),
                None => *default = true,
            },
            Pat::Wild(_) => *default = true,
            Pat::Struct(pattern) if self.names_kind(&pattern.path) => {
                named.insert(pattern.path.segments.last().unwrap().ident.to_string());
            }
            Pat::TupleStruct(pattern) if self.names_kind(&pattern.path) => {
                named.insert(pattern.path.segments.last().unwrap().ident.to_string());
            }
            Pat::Path(pattern) if self.names_kind(&pattern.path) => {
                named.insert(pattern.path.segments.last().unwrap().ident.to_string());
            }
            _ => {}
        }
    }

    fn function_key(&self) -> String {
        let function = self
            .function
            .last()
            .map(String::as_str)
            .unwrap_or("<module>");
        match self.impl_self.last() {
            Some(ty) if !ty.is_empty() => format!("{ty}::{function}"),
            _ => function.to_string(),
        }
    }

    fn where_am_i(&self) -> String {
        format!("{}::{}", self.file, self.function_key())
    }
}

impl<'ast> Visit<'ast> for Walk {
    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        let name = match &*item.self_ty {
            Type::Path(path) => path
                .path
                .segments
                .last()
                .map(|segment| segment.ident.to_string())
                .unwrap_or_default(),
            _ => String::new(),
        };
        self.impl_self.push(name);
        syn::visit::visit_item_impl(self, item);
        self.impl_self.pop();
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        if imports_kinds_or_renames(&item.tree, false) {
            self.renames.push(format!(
                "{}: a `use` of PhysicalNode's variants or a rename of the enum",
                self.file
            ));
        }
        syn::visit::visit_item_use(self, item);
    }

    fn visit_item_type(&mut self, item: &'ast ItemType) {
        if let Type::Path(path) = &*item.ty {
            if path
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "PhysicalNode")
            {
                self.renames.push(format!(
                    "{}: type alias `{}` of PhysicalNode",
                    self.file, item.ident
                ));
            }
        }
        syn::visit::visit_item_type(self, item);
    }

    fn visit_item_fn(&mut self, item: &'ast ItemFn) {
        self.function.push(item.sig.ident.to_string());
        syn::visit::visit_item_fn(self, item);
        self.function.pop();
    }

    fn visit_impl_item_fn(&mut self, item: &'ast ImplItemFn) {
        self.function.push(item.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, item);
        self.function.pop();
    }

    fn visit_expr_match(&mut self, expr: &'ast ExprMatch) {
        let mut named = BTreeSet::new();
        let mut default = false;
        for arm in &expr.arms {
            self.collect(&arm.pat, &mut named, &mut default);
        }
        if named.len() >= 2 {
            let site = self.where_am_i();
            if default && named.len() * 2 >= self.kinds.len() {
                self.defaults
                    .push(format!("{site} (kinds named: {named:?})"));
            }
            self.sites.insert((self.file.clone(), self.function_key()));
        }
        syn::visit::visit_expr_match(self, expr);
    }
}

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("source dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn every_match_over_physical_node_kinds_is_a_named_behaviour_site_without_a_default_arm() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    sources(&src, &mut files);
    files.sort();
    let mut walk = Walk {
        kinds: PhysicalNodeKind::ALL
            .iter()
            .map(|kind| format!("{kind:?}"))
            .collect(),
        ..Walk::default()
    };
    for path in &files {
        let text = std::fs::read_to_string(path).expect("source text");
        let file =
            syn::parse_file(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        walk.file = path
            .strip_prefix(&src)
            .expect("under src")
            .to_string_lossy()
            .replace('\\', "/");
        walk.visit_file(&file);
    }
    let expected: BTreeSet<(String, String)> = BEHAVIOUR_SITES
        .iter()
        .map(|(file, function, _reads)| (file.to_string(), function.to_string()))
        .collect();
    let unexpected: Vec<String> = walk
        .sites
        .difference(&expected)
        .map(|(file, function)| format!("{file}::{function}"))
        .collect();
    let stale: Vec<String> = expected
        .difference(&walk.sites)
        .map(|(file, function)| format!("{file}::{function}"))
        .collect();
    assert!(
        walk.defaults.is_empty()
            && unexpected.is_empty()
            && stale.is_empty()
            && walk.renames.is_empty(),
        "matches over PhysicalNode kinds with a default arm beside half the kinds or more (list every kind, or move the classification onto its declaration line):\n{}\nmatches over PhysicalNode kinds not in BEHAVIOUR_SITES (a classification belongs on the declaration line):\n{}\nBEHAVIOUR_SITES entries with no such match:\n{}\nPhysicalNode under another name (the walk cannot see such matches):\n{}",
        walk.defaults.join("\n"),
        unexpected.join("\n"),
        stale.join("\n"),
        walk.renames.join("\n")
    );
}
