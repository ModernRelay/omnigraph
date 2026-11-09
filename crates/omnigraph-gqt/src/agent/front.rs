//! Accept or refuse an answer through the compiler's front door: the parser,
//! then the type checker and lowering of every declaration, as the engine
//! compiles a source before it runs it.

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::ir::core3::CoreDecl;
use omnigraph_compiler::query::ast::FileBody;
use omnigraph_compiler::query::parser::parse_query_diagnostic;
use omnigraph_compiler::query::typecheck::{CheckedQuery, typecheck_query_decl};
use omnigraph_compiler::{QueryDiagnostic, lower_mutation_query, lower_query};
use serde_json::{Value, json};

use super::normalize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edition {
    Two,
    Three,
}

impl Edition {
    pub fn parse(text: &str) -> Result<Self, String> {
        match text {
            "2" => Ok(Edition::Two),
            "3" => Ok(Edition::Three),
            other => Err(format!("edition `{other}` is not 2 or 3")),
        }
    }

    pub fn number(self) -> u8 {
        match self {
            Edition::Two => 2,
            Edition::Three => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Read,
    Write,
}

/// One accepted declaration with its typed IR, normalised so that two
/// declarations differing only in the names of variables, aliases and the
/// declaration itself compare equal.
#[derive(Debug, Clone)]
pub struct Declaration {
    pub name: String,
    pub kind: Kind,
    pub params: Vec<String>,
    pub ir: String,
}

#[derive(Debug, Clone)]
pub enum Verdict {
    Accepted(Vec<Declaration>),
    /// The diagnostic as the compiler reports it, in its JSON form.
    Refused(Value),
}

impl Verdict {
    pub fn to_json(&self) -> Value {
        match self {
            Verdict::Accepted(decls) => json!({
                "accepted": true,
                "declarations": decls.iter().map(|d| json!({
                    "name": d.name,
                    "kind": match d.kind { Kind::Read => "read", Kind::Write => "write" },
                    "params": d.params,
                    "ir": d.ir,
                })).collect::<Vec<_>>(),
            }),
            Verdict::Refused(diagnostic) => json!({
                "accepted": false,
                "diagnostic": diagnostic,
                "rendered": render(diagnostic),
            }),
        }
    }

    pub fn code(&self) -> Option<&str> {
        match self {
            Verdict::Refused(d) => d.get("code").and_then(Value::as_str),
            _ => None,
        }
    }
}

/// The diagnostic exactly as an agent would read it from `omnigraph lint
/// --json` or an HTTP refusal: the serialized four-field diagnostic.
pub fn render(diagnostic: &Value) -> String {
    serde_json::to_string_pretty(diagnostic).unwrap_or_else(|_| diagnostic.to_string())
}

fn diagnostic_json(d: &QueryDiagnostic) -> Value {
    serde_json::to_value(d).unwrap_or_else(|e| json!({"message": e.to_string()}))
}

fn compiler_error_json(e: &CompilerError) -> Value {
    match e.diagnostic() {
        Some(d) => diagnostic_json(d),
        None => json!({"message": e.to_string()}),
    }
}

/// Accept or refuse `text` under `edition` against `catalog`.
///
/// Both editions go through the one front door the engine uses: the edition
/// line selects the parser, and the type checker types an edition-2
/// declaration into the typed IR it lowers and an edition-3 one into the core
/// IR. The text must be in the edition asked for, so an edition-2 answer that
/// opens with `gq 3;`, or the reverse, is refused.
pub fn accept(edition: Edition, catalog: &Catalog, text: &str) -> Verdict {
    let file = match parse_query_diagnostic(text) {
        Ok(file) => file,
        Err(d) => return Verdict::Refused(diagnostic_json(&d)),
    };
    let FileBody::Queries(decls) = file.body else {
        return Verdict::Refused(json!({
            "message": "the answer is a statement, not query declarations",
        }));
    };
    let names = normalize::source_names(text);
    let mut out = Vec::with_capacity(decls.len());
    for decl in &decls {
        if decl.edition3.is_some() != (edition == Edition::Three) {
            return Verdict::Refused(json!({
                "message": format!("the answer is not edition {}", edition.number()),
            }));
        }
        let checked = match typecheck_query_decl(catalog, decl) {
            Ok(checked) => checked,
            Err(e) => return Verdict::Refused(compiler_error_json(&e)),
        };
        let (kind, ir) = match checked {
            CheckedQuery::Read(read) => (
                Kind::Read,
                lower_query(catalog, &read).map(|ir| format!("{ir:#?}")),
            ),
            CheckedQuery::Mutation(write) => (
                Kind::Write,
                lower_mutation_query(catalog, &write).map(|ir| format!("{ir:#?}")),
            ),
            CheckedQuery::Edition3(core) => (
                match core.as_ref() {
                    CoreDecl::Read(_) => Kind::Read,
                    CoreDecl::Write(_) => Kind::Write,
                },
                Ok(format!("{core:#?}")),
            ),
        };
        let ir = match ir {
            Ok(ir) => ir,
            Err(e) => return Verdict::Refused(compiler_error_json(&e)),
        };
        let params: Vec<String> = decl.params.iter().map(|p| p.name.clone()).collect();
        out.push(Declaration {
            name: decl.name.clone(),
            kind,
            ir: normalize::rename(&ir, &names, &params),
            params,
        });
    }
    if out.is_empty() {
        return Verdict::Refused(json!({"message": "the answer declares no query"}));
    }
    Verdict::Accepted(out)
}
