use std::collections::BTreeMap;

use proc_macro2::{TokenStream, TokenTree};
use quote::ToTokens;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};
use syn::{
    Attribute, Expr, ExprCall, ExprMethodCall, ExprStruct, Field, ImplItemConst, ImplItemFn,
    ImplItemType, ItemConst, ItemEnum, ItemFn, ItemImpl, ItemMod, ItemStatic, ItemStruct,
    ItemTrait, ItemType, Lit, LitStr, Macro, Meta, Token, TraitItemConst, TraitItemFn,
    TraitItemType, Variant, Visibility,
};

use crate::{
    GuardError, InventoryRow, SURFACE_RUST_STRING, containing_value_fingerprint, percent_encode,
    vocabulary_matches,
};

pub(crate) const SITE_KINDS: &[&str] = &[
    "thiserror_attribute",
    "diagnostic_attribute",
    "clap_help",
    "openapi_description",
    "compiler_lint",
    "message_field",
    "deprecation_note",
    "display_impl",
    "presentation_helper",
    "rustdoc",
    "diagnostic_constructor",
    "error_constructor",
    "error_macro",
    "cli_human_output",
];

#[derive(Debug)]
struct StringSite {
    item: String,
    site_kind: &'static str,
    value: String,
}

pub(crate) fn scan(contents: &str, source_path: &str) -> Result<Vec<InventoryRow>, GuardError> {
    let file = syn::parse_file(contents).map_err(|error| GuardError::RustSource {
        path: source_path.to_owned(),
        message: error.to_string(),
    })?;
    let mut collector = Collector {
        source_path,
        item_stack: vec!["<module>".to_owned()],
        public_docs: vec![false],
        sites: Vec::new(),
    };
    collector.visit_file(&file);
    Ok(observations(source_path, collector.sites))
}

struct Collector<'a> {
    source_path: &'a str,
    item_stack: Vec<String>,
    public_docs: Vec<bool>,
    sites: Vec<StringSite>,
}

impl Collector<'_> {
    fn with_item(&mut self, label: String, visit: impl FnOnce(&mut Self)) {
        self.item_stack.push(label);
        visit(self);
        self.item_stack.pop();
    }

    fn item(&self) -> String {
        self.item_stack.join("::")
    }

    fn with_public_docs(&mut self, visible: bool, visit: impl FnOnce(&mut Self)) {
        self.public_docs.push(visible);
        visit(self);
        self.public_docs.pop();
    }

    fn public_docs(&self) -> bool {
        self.public_docs.last().copied().unwrap_or(false)
    }

    fn in_error_impl(&self) -> bool {
        self.item_stack.iter().any(|item| {
            item.strip_prefix("impl ").is_some_and(|target| {
                target
                    .split('<')
                    .next()
                    .is_some_and(|name| name.trim().ends_with("Error"))
            })
        })
    }

    fn observe_lits(&mut self, site_kind: &'static str, literals: Vec<String>) {
        let item = self.item();
        self.sites
            .extend(literals.into_iter().map(|value| StringSite {
                item: item.clone(),
                site_kind,
                value,
            }));
    }

    fn visit_attributes(&mut self, attributes: &[Attribute]) {
        for attribute in attributes {
            let last = attribute
                .path()
                .segments
                .last()
                .map(|segment| segment.ident.to_string())
                .unwrap_or_default();
            let site_kind = match last.as_str() {
                "error" => Some("thiserror_attribute"),
                "diagnostic" | "help" | "label" => Some("diagnostic_attribute"),
                "command" | "arg" | "value" => Some("clap_help"),
                "path" | "schema" | "parameter" => Some("openapi_description"),
                "doc" if is_cli_source(self.source_path) => Some("clap_help"),
                "doc" if is_http_rustdoc(self.source_path) => Some("openapi_description"),
                "doc" if self.public_docs() => Some("rustdoc"),
                "deprecated" => Some("deprecation_note"),
                _ => None,
            };
            if let Some(site_kind) = site_kind {
                self.observe_lits(site_kind, attribute_literals(attribute));
            }
        }
    }

    fn visit_field_with_public_docs(&mut self, node: &Field, visible: bool) {
        let label = node
            .ident
            .as_ref()
            .map_or_else(|| "field".to_owned(), |ident| format!("field {ident}"));
        self.with_item(label, |this| {
            this.with_public_docs(visible, |this| {
                this.visit_attributes(&node.attrs);
                this.visit_type(&node.ty);
            });
        });
    }

    fn skip(attributes: &[Attribute]) -> bool {
        attributes.iter().any(|attribute| {
            if attribute.path().is_ident("test") {
                return true;
            }
            let Meta::List(list) = &attribute.meta else {
                return false;
            };
            attribute.path().is_ident("cfg")
                && syn::parse2::<Meta>(list.tokens.clone())
                    .is_ok_and(|predicate| cfg_requires_test(&predicate))
        })
    }
}

fn cfg_requires_test(predicate: &Meta) -> bool {
    match predicate {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) if list.path.is_ident("all") || list.path.is_ident("any") => {
            let Ok(nested) =
                Punctuated::<Meta, Token![,]>::parse_terminated.parse2(list.tokens.clone())
            else {
                return false;
            };
            if list.path.is_ident("all") {
                nested.iter().any(cfg_requires_test)
            } else {
                !nested.is_empty() && nested.iter().all(cfg_requires_test)
            }
        }
        Meta::List(list) if list.path.is_ident("not") => {
            let Ok(inner) = syn::parse2::<Meta>(list.tokens.clone()) else {
                return false;
            };
            let Meta::List(inner_not) = inner else {
                return false;
            };
            inner_not.path.is_ident("not")
                && syn::parse2::<Meta>(inner_not.tokens.clone())
                    .is_ok_and(|inner| cfg_requires_test(&inner))
        }
        Meta::List(_) | Meta::NameValue(_) => false,
    }
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_file(&mut self, node: &'ast syn::File) {
        for attribute in &node.attrs {
            if attribute.path().is_ident("doc") {
                self.observe_lits("rustdoc", attribute_literals(attribute));
            }
        }
        for item in &node.items {
            self.visit_item(item);
        }
    }

    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        if Self::skip(&node.attrs) {
            return;
        }
        self.with_item(format!("fn {}", node.sig.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                if is_presentation_helper(this.source_path, &node.sig.ident.to_string()) {
                    this.observe_lits(
                        "presentation_helper",
                        token_literals(node.block.to_token_stream()),
                    );
                }
                visit::visit_signature(this, &node.sig);
                visit::visit_block(this, &node.block);
            });
        });
    }

    fn visit_impl_item_fn(&mut self, node: &'ast ImplItemFn) {
        if Self::skip(&node.attrs) {
            return;
        }
        self.with_item(format!("fn {}", node.sig.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                visit::visit_signature(this, &node.sig);
                visit::visit_block(this, &node.block);
            });
        });
    }

    fn visit_trait_item_fn(&mut self, node: &'ast TraitItemFn) {
        self.with_item(format!("fn {}", node.sig.ident), |this| {
            this.visit_attributes(&node.attrs);
            visit::visit_signature(this, &node.sig);
            if let Some(default) = &node.default {
                visit::visit_block(this, default);
            }
        });
    }

    fn visit_item_impl(&mut self, node: &'ast ItemImpl) {
        let target = compact_tokens(&node.self_ty.to_token_stream());
        let display_impl = node.trait_.as_ref().is_some_and(|(_, path, _)| {
            path.segments
                .last()
                .is_some_and(|segment| segment.ident == "Display")
        });
        self.with_item(format!("impl {target}"), |this| {
            this.with_public_docs(false, |this| {
                this.visit_attributes(&node.attrs);
                if display_impl {
                    let literals = node
                        .items
                        .iter()
                        .flat_map(|item| token_literals(item.to_token_stream()))
                        .collect();
                    this.observe_lits("display_impl", literals);
                } else {
                    for item in &node.items {
                        this.visit_impl_item(item);
                    }
                }
            });
        });
    }

    fn visit_item_mod(&mut self, node: &'ast ItemMod) {
        if Self::skip(&node.attrs) {
            return;
        }
        self.with_item(format!("mod {}", node.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                if let Some((_, items)) = &node.content {
                    for item in items {
                        this.visit_item(item);
                    }
                }
            });
        });
    }

    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        self.with_item(format!("struct {}", node.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
            });
            for field in &node.fields {
                this.visit_field_with_public_docs(field, is_public(&field.vis));
            }
        });
    }

    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        self.with_item(format!("enum {}", node.ident), |this| {
            let visible = is_public(&node.vis);
            this.with_public_docs(visible, |this| {
                this.visit_attributes(&node.attrs);
                for variant in &node.variants {
                    this.visit_variant(variant);
                }
            });
        });
    }

    fn visit_item_trait(&mut self, node: &'ast ItemTrait) {
        self.with_item(format!("trait {}", node.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                for item in &node.items {
                    this.visit_trait_item(item);
                }
            });
        });
    }

    fn visit_variant(&mut self, node: &'ast Variant) {
        let visible = self.public_docs();
        self.with_item(format!("variant {}", node.ident), |this| {
            this.visit_attributes(&node.attrs);
            for field in &node.fields {
                this.visit_field_with_public_docs(field, visible);
            }
            if let Some((_, expression)) = &node.discriminant {
                this.visit_expr(expression);
            }
        });
    }

    fn visit_field(&mut self, node: &'ast Field) {
        self.visit_field_with_public_docs(node, is_public(&node.vis));
    }

    fn visit_item_type(&mut self, node: &'ast ItemType) {
        self.with_item(format!("type {}", node.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                this.visit_type(&node.ty);
            });
        });
    }

    fn visit_trait_item_const(&mut self, node: &'ast TraitItemConst) {
        self.with_item(format!("const {}", node.ident), |this| {
            this.visit_attributes(&node.attrs);
            this.visit_type(&node.ty);
            if let Some((_, default)) = &node.default {
                this.visit_expr(default);
            }
        });
    }

    fn visit_trait_item_type(&mut self, node: &'ast TraitItemType) {
        self.with_item(format!("type {}", node.ident), |this| {
            this.visit_attributes(&node.attrs);
            for bound in &node.bounds {
                this.visit_type_param_bound(bound);
            }
            if let Some((_, default)) = &node.default {
                this.visit_type(default);
            }
        });
    }

    fn visit_impl_item_const(&mut self, node: &'ast ImplItemConst) {
        self.with_item(format!("const {}", node.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                this.visit_type(&node.ty);
                this.visit_expr(&node.expr);
            });
        });
    }

    fn visit_impl_item_type(&mut self, node: &'ast ImplItemType) {
        self.with_item(format!("type {}", node.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                this.visit_type(&node.ty);
            });
        });
    }

    fn visit_item_const(&mut self, node: &'ast ItemConst) {
        self.with_item(format!("const {}", node.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                this.visit_expr(&node.expr);
            });
        });
    }

    fn visit_item_static(&mut self, node: &'ast ItemStatic) {
        self.with_item(format!("static {}", node.ident), |this| {
            this.with_public_docs(is_public(&node.vis), |this| {
                this.visit_attributes(&node.attrs);
                this.visit_expr(&node.expr);
            });
        });
    }

    fn visit_attribute(&mut self, _node: &'ast Attribute) {
        // Attributes are handled exactly once by the enclosing named item.
    }

    fn visit_lit(&mut self, node: &'ast Lit) {
        if let Lit::Str(value) = node {
            if self.in_error_impl() {
                self.observe_lits("error_constructor", vec![value.value()]);
            } else if is_cli_renderer(self.source_path) {
                self.observe_lits("cli_human_output", vec![value.value()]);
            }
        }
    }

    fn visit_macro(&mut self, node: &'ast Macro) {
        let name = node
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default();
        let site_kind = if matches!(
            name.as_str(),
            "bail" | "ensure" | "anyhow" | "eyre" | "format_err"
        ) {
            Some("error_macro")
        } else if self.in_error_impl()
            && matches!(name.as_str(), "format" | "format_args" | "concat")
        {
            Some("error_constructor")
        } else if (is_cli_source(self.source_path)
            && matches!(
                name.as_str(),
                "print" | "println" | "eprint" | "eprintln" | "write" | "writeln"
            ))
            || (is_cli_renderer(self.source_path) && name == "format")
        {
            Some("cli_human_output")
        } else {
            None
        };
        if let Some(site_kind) = site_kind {
            self.observe_lits(site_kind, token_literals(node.tokens.clone()));
        }
    }

    fn visit_expr_call(&mut self, node: &'ast ExprCall) {
        let Expr::Path(path) = node.func.as_ref() else {
            visit::visit_expr_call(self, node);
            return;
        };
        let called = path.path.to_token_stream().to_string().replace(' ', "");
        if is_diagnostic_constructor(&called) {
            self.observe_lits(
                "diagnostic_constructor",
                expression_list_literals(&node.args),
            );
            return;
        }
        if is_error_constructor(&called) || (called.starts_with("Self::") && self.in_error_impl()) {
            self.observe_lits("error_constructor", expression_list_literals(&node.args));
            return;
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast ExprMethodCall) {
        if matches!(
            node.method.to_string().as_str(),
            "context" | "with_context" | "wrap_err" | "wrap_err_with"
        ) {
            self.observe_lits("error_constructor", expression_list_literals(&node.args));
            self.visit_expr(&node.receiver);
            return;
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast ExprStruct) {
        let site_kind = if is_compiler_source(self.source_path) {
            "compiler_lint"
        } else {
            "message_field"
        };
        for field in &node.fields {
            let member = field.member.to_token_stream().to_string();
            if is_message_field(&member) {
                self.observe_lits(site_kind, expression_literals(&field.expr));
            }
            self.visit_expr(&field.expr);
        }
        if let Some(rest) = &node.rest {
            self.visit_expr(rest);
        }
    }
}

fn is_message_field(member: &str) -> bool {
    matches!(member, "message" | "description" | "reason" | "short")
}

fn is_public(visibility: &Visibility) -> bool {
    matches!(visibility, Visibility::Public(_))
}

fn observations(source_path: &str, sites: Vec<StringSite>) -> Vec<InventoryRow> {
    let mut duplicate_sites: BTreeMap<(String, &'static str, String), usize> = BTreeMap::new();
    let mut rows = Vec::new();
    for site in sites {
        let value = observable_site_value(site.site_kind, &site.value);
        let fingerprint = containing_value_fingerprint(&value);
        let site_ordinal = duplicate_sites
            .entry((site.item.clone(), site.site_kind, fingerprint.clone()))
            .or_default();
        *site_ordinal += 1;
        let mut term_ordinals: BTreeMap<String, usize> = BTreeMap::new();
        for found in vocabulary_matches(&value) {
            let term_ordinal = term_ordinals.entry(found.term.clone()).or_default();
            *term_ordinal += 1;
            let boundary = format!(
                "{} :: {} :: {} #{}",
                site.item, site.site_kind, fingerprint, site_ordinal
            );
            rows.push(InventoryRow::observation(
                format!(
                    "{SURFACE_RUST_STRING}:{}:{}:{}:{fingerprint}:{}:{}:{}",
                    percent_encode(source_path),
                    percent_encode(&site.item),
                    site.site_kind,
                    percent_encode(&found.term),
                    site_ordinal,
                    term_ordinal
                ),
                SURFACE_RUST_STRING,
                source_path,
                &boundary,
                site.site_kind,
                found.term,
            ));
        }
    }
    rows.sort_by(|left, right| left.occurrence_id.cmp(&right.occurrence_id));
    rows
}

fn observable_site_value(site_kind: &str, value: &str) -> String {
    if matches!(
        site_kind,
        "thiserror_attribute"
            | "diagnostic_attribute"
            | "diagnostic_constructor"
            | "error_constructor"
            | "error_macro"
            | "cli_human_output"
            | "message_field"
            | "display_impl"
            | "presentation_helper"
    ) {
        normalize_format_template(value)
    } else {
        value.to_owned()
    }
}

fn normalize_format_template(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '{' if characters.peek() == Some(&'{') => {
                characters.next();
                output.push('{');
            }
            '}' if characters.peek() == Some(&'}') => {
                characters.next();
                output.push('}');
            }
            '{' => {
                for nested in characters.by_ref() {
                    if nested == '}' {
                        break;
                    }
                }
                output.push_str("{}");
            }
            _ => output.push(character),
        }
    }
    output
}

fn is_error_constructor(called: &str) -> bool {
    [
        "OmniError::",
        "CompilerError::",
        "ApiError::",
        "ParseDiagnostic::",
        "RegistryError::",
    ]
    .iter()
    .any(|prefix| called.contains(prefix))
}

fn is_diagnostic_constructor(called: &str) -> bool {
    matches!(called, "Diagnostic::error" | "Diagnostic::warning")
        || called.ends_with("::Diagnostic::error")
        || called.ends_with("::Diagnostic::warning")
}

fn is_presentation_helper(path: &str, function: &str) -> bool {
    match path {
        "crates/omnigraph/src/error.rs" => matches!(
            function,
            "graph_type_subject"
                | "dataset_subject"
                | "missing_graph_type_at_snapshot"
                | "format_key_conflict"
                | "format_merge_conflicts"
        ),
        "crates/omnigraph-server/src/lib.rs" => matches!(
            function,
            "summarize_merge_conflicts" | "graph_type_subject" | "format_key_conflict"
        ),
        _ => false,
    }
}

fn is_cli_source(path: &str) -> bool {
    path.starts_with("crates/omnigraph-cli/src/")
}

fn is_cli_renderer(path: &str) -> bool {
    matches!(
        path,
        "crates/omnigraph-cli/src/output.rs"
            | "crates/omnigraph-cli/src/helpers.rs"
            | "crates/omnigraph-cli/src/read_format.rs"
    )
}

fn is_http_rustdoc(path: &str) -> bool {
    path.starts_with("crates/omnigraph-server/src/")
        || path.starts_with("crates/omnigraph-api-types/src/")
}

fn is_compiler_source(path: &str) -> bool {
    path.starts_with("crates/omnigraph-compiler/src/")
}

fn attribute_literals(attribute: &Attribute) -> Vec<String> {
    match &attribute.meta {
        Meta::NameValue(name_value) => expression_literals(&name_value.value),
        Meta::List(list) => token_literals(list.tokens.clone()),
        Meta::Path(_) => Vec::new(),
    }
}

fn expression_list_literals(
    expressions: &syn::punctuated::Punctuated<Expr, syn::token::Comma>,
) -> Vec<String> {
    expressions.iter().flat_map(expression_literals).collect()
}

fn expression_literals(expression: &Expr) -> Vec<String> {
    struct LiteralVisitor(Vec<String>);
    impl<'ast> Visit<'ast> for LiteralVisitor {
        fn visit_lit(&mut self, literal: &'ast Lit) {
            if let Lit::Str(value) = literal {
                self.0.push(value.value());
            }
        }

        fn visit_macro(&mut self, node: &'ast Macro) {
            self.0.extend(token_literals(node.tokens.clone()));
        }
    }
    let mut visitor = LiteralVisitor(Vec::new());
    visitor.visit_expr(expression);
    visitor.0
}

fn token_literals(tokens: TokenStream) -> Vec<String> {
    let mut output = Vec::new();
    for token in tokens {
        match token {
            TokenTree::Group(group) => output.extend(token_literals(group.stream())),
            TokenTree::Literal(literal) => {
                if let Ok(value) = syn::parse_str::<LitStr>(&literal.to_string()) {
                    output.push(value.value());
                }
            }
            TokenTree::Ident(_) | TokenTree::Punct(_) => {}
        }
    }
    output
}

fn compact_tokens(tokens: &TokenStream) -> String {
    tokens
        .to_string()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_only_closed_user_visible_site_kinds() {
        let source = r#"
#[derive(thiserror::Error)]
enum Failure {
    #[error("table row failed")]
    Table,
}

#[derive(clap::Parser)]
#[command(about = "manage table rows")]
struct Cli {
    #[arg(long, help = "select a column")]
    table: String,
}

fn run() -> Result<(), Failure> {
    let internal = "table rows are not a public site";
    anyhow::bail!("cannot load table rows");
    Ok(())
}
"#;
        let rows = scan(source, "crates/example/src/lib.rs").unwrap();
        let kinds: std::collections::BTreeSet<_> =
            rows.iter().map(|row| row.site_kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["clap_help", "error_macro", "thiserror_attribute"]
                .into_iter()
                .collect()
        );
        assert!(rows.iter().all(|row| !row.boundary.contains("public site")));
    }

    #[test]
    fn diagnostic_error_and_warning_constructors_are_observed() {
        let rows = scan(
            r#"
fn validate() {
    let _ = Diagnostic::error("table row is invalid");
    let _ = Diagnostic::warning(format!("column {name} is ignored"));
    let _ = OtherDiagnostic::note("hidden table row");
    let _ = OtherDiagnostic::error("hidden table row");
}
"#,
            "crates/omnigraph-cluster/src/config.rs",
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert!(
            rows.iter()
                .all(|row| row.site_kind == "diagnostic_constructor")
        );
        assert!(rows.iter().all(|row| !row.boundary.contains("hidden")));
    }

    #[test]
    fn item_identity_ignores_lines_and_whitespace() {
        let compact = scan(
            "fn f(){ anyhow::bail!(\"table rows\"); }",
            "crates/a/src/lib.rs",
        )
        .unwrap();
        let spaced = scan(
            "\n\nfn f() {\n anyhow::bail!( \"table   rows\" );\n}\n",
            "crates/a/src/lib.rs",
        )
        .unwrap();
        assert_eq!(compact, spaced);
    }

    #[test]
    fn skips_cfg_test_modules() {
        let rows = scan(
            "#[cfg(test)] mod tests { fn t() { anyhow::bail!(\"table row\"); } }",
            "crates/a/src/lib.rs",
        )
        .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn cfg_with_a_production_alternative_is_not_skipped() {
        let rows = scan(
            r#"
#[cfg(any(test, feature = "production"))]
fn reachable() { anyhow::bail!("table rows"); }

#[cfg(all(test, feature = "test-support"))]
fn test_only() { anyhow::bail!("hidden table row"); }
"#,
            "crates/a/src/lib.rs",
        )
        .unwrap();
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| row.boundary.contains("reachable")));
    }

    #[test]
    fn extracts_diagnostic_code_short_text_and_deprecation_notes() {
        let source = r#"
const CODE: DiagnosticCode = DiagnosticCode {
    short: "drop table rows",
    family: Family::DS,
};

#[deprecated(note = "use entity records instead of table rows")]
pub fn ingest() {}
"#;
        let rows = scan(source, "crates/omnigraph-compiler/src/lint/codes.rs").unwrap();
        let kinds: std::collections::BTreeSet<_> =
            rows.iter().map(|row| row.site_kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["compiler_lint", "deprecation_note"].into_iter().collect()
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row.site_kind == "compiler_lint")
                .count(),
            2
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row.site_kind == "deprecation_note")
                .count(),
            2
        );
    }

    #[test]
    fn extracts_message_fields_display_text_and_public_rustdoc() {
        let source = r#"
/// Public table state.
pub struct PendingIndex {
    /// Explains why this table is pending.
    pub reason: String,
    /// Hidden table detail.
    hidden: String,
}

fn pending() -> PendingIndex {
    PendingIndex {
        reason: "physical table rows are pending".to_owned(),
        hidden: String::new(),
    }
}

enum SkipReason { Missing }
impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("table row was skipped")
    }
}
"#;
        let rows = scan(source, "crates/omnigraph/src/index.rs").unwrap();
        let kinds: std::collections::BTreeSet<_> =
            rows.iter().map(|row| row.site_kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["display_impl", "message_field", "rustdoc"]
                .into_iter()
                .collect()
        );
        assert!(rows.iter().any(|row| row.site_kind == "message_field"));
        assert!(rows.iter().any(|row| row.site_kind == "display_impl"));
        assert!(
            rows.iter()
                .any(|row| { row.site_kind == "rustdoc" && row.boundary.contains("field reason") })
        );
        assert!(
            rows.iter()
                .all(|row| !row.boundary.contains("field hidden"))
        );
    }

    #[test]
    fn cli_doc_comments_are_clap_help_even_on_private_items() {
        let source = r#"
/// Manage table rows.
#[derive(clap::Parser)]
struct Cli {
    /// Select a result column.
    table: String,
}
"#;
        let rows = scan(source, "crates/omnigraph-cli/src/cli.rs").unwrap();
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| row.site_kind == "clap_help"));
        assert!(rows.iter().any(|row| row.boundary.contains("struct Cli")));
        assert!(rows.iter().any(|row| row.boundary.contains("field table")));
    }

    #[test]
    fn read_formatter_format_macros_are_human_output() {
        let rows = scan(
            r#"fn render(count: usize) { let _ = format!("{count} rows from table"); }"#,
            "crates/omnigraph-cli/src/read_format.rs",
        )
        .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.site_kind.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            ["cli_human_output"].into_iter().collect()
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn read_formatter_direct_string_literals_are_human_output() {
        let rows = scan(
            r#"fn render() -> String { "(no table rows)".to_owned() }"#,
            "crates/omnigraph-cli/src/read_format.rs",
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.site_kind == "cli_human_output"));
    }

    #[test]
    fn inner_module_rustdoc_is_observed() {
        let rows = scan(
            "//! Public per-table, row-oriented workload support.\npub fn run() {}\n",
            "crates/omnigraph-server/src/workload.rs",
        )
        .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.site_kind.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            ["rustdoc"].into_iter().collect()
        );
        assert!(rows.iter().all(|row| row.boundary.contains("<module>")));
    }

    #[test]
    fn self_error_constructors_and_registered_helpers_are_observed() {
        let source = r#"
struct ApiError;
impl ApiError {
    fn from_omni() -> Self {
        Self::internal(format!("historical table version"))
    }
}

fn summarize_merge_conflicts() -> String {
    format!("table row conflict")
}

fn unrelated_internal_helper() -> String {
    format!("hidden table row")
}
"#;
        let rows = scan(source, "crates/omnigraph-server/src/lib.rs").unwrap();
        let kinds: std::collections::BTreeSet<_> =
            rows.iter().map(|row| row.site_kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["error_constructor", "presentation_helper"]
                .into_iter()
                .collect()
        );
        assert!(
            rows.iter()
                .all(|row| !row.boundary.contains("unrelated_internal_helper"))
        );
    }

    #[test]
    fn error_impl_local_format_macros_are_observed() {
        let rows = scan(
            r#"
impl<T> ApiError<T> {
    fn from_engine() -> Self {
        let static_message = "stale row";
        let message = format!("historical table version {version}");
        Self::internal(message)
    }
}
"#,
            "crates/omnigraph-server/src/lib.rs",
        )
        .unwrap();
        assert!(rows.iter().any(|row| {
            row.site_kind == "error_constructor"
                && row.current_term == "table"
                && row.boundary.contains("fn from_engine")
        }));
        assert!(rows.iter().any(|row| {
            row.site_kind == "error_constructor"
                && row.current_term == "row"
                && row.boundary.contains("fn from_engine")
        }));
    }

    #[test]
    fn formatter_placeholder_names_are_not_treated_as_visible_words() {
        let rows = scan(
            r#"
#[derive(thiserror::Error)]
enum Error {
    #[error("duplicate entities at positions {first_row} and {second_row}")]
    Duplicate { first_row: usize, second_row: usize },
    #[error("visible row at {{table}} {position}")]
    Visible { position: usize },
}
"#,
            "crates/a/src/lib.rs",
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row.current_term == "row" || row.current_term == "table")
        );
        assert!(
            rows.iter()
                .all(|row| row.boundary.contains("variant Visible"))
        );
    }
}
