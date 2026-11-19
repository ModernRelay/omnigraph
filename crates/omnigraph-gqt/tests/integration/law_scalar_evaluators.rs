//! Law 2: the three evaluators of a scalar expression agree (the GQ 3
//! RFC, section 2, arithmetic and guards), and its type is the RFC's join.
//!
//! The generator (`omnigraph_compiler::arbitrary::ScalarExpr`) draws a
//! well-typed expression of integer, float, Boolean or string class over
//! one input of each of `I32`, `I64`, `U32`, `F32`, `F64`, `Bool` and
//! `String` and over literals: `+ - * / %`, unary minus, `if`, `coalesce`,
//! comparisons across the numeric types, `and`, `or`, `not`, `is [not]
//! null`, and the registry's scalar rows: `lower`, `upper`, `trim`,
//! `ltrim`, `rtrim`, `length`, `left`, `right`, `substring`, `abs`,
//! `floor`, `ceil`, `round` with and without `digits:`, `sqrt`, `exp`,
//! `ln`, `log10`, `power` and `cast` between every pair of the inputs'
//! types it converts. The inputs are one generated row (nulls, zeros,
//! `-0.0`, the integer extremes, `1e30` in an `F32`, Strings with Unicode
//! whitespace and case mappings, and texts `cast` reads or refuses as a
//! number or a Boolean).
//!
//! One expression is evaluated:
//!
//! - by the engine per row, over `$x.p_<t>` property reads (the batch
//!   evaluator);
//! - by the engine over `$<t>` parameters (the bind-time constant path);
//! - by the reference interpreter, over the unfolded core term of the first
//!   form;
//! - and, for an expression without arithmetic, by the compiler's constant
//!   folder, with the inputs written as literals.
//!
//! The law: all give the same value or refuse with the same code, and the
//! checked result type equals [`ScalarExpr::rfc_type`]. Expressions that
//! negate an unsigned input are not drawn: the checker refuses them
//! (`T72`), which the refusal table owns.

use std::sync::Arc;

use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph_compiler::ParamMap;
use omnigraph_compiler::arbitrary::{
    Choices, Class, Inputs, SCALAR_INPUTS, SCALAR_SCHEMA, Sc, ScalarExpr, choices,
    scalar_input_value,
};
use omnigraph_compiler::ir::core::CoreDecl;
use omnigraph_compiler::query::value::Literal;
use proptest::test_runner::TestCaseError;

use crate::law_engine_reference::law_runner;

const CASES: u32 = 300;
const NIGHTLY: u32 = 6_000;

/// The row's inputs: `(type, JSON value, literal)`, `None` for null.
type Row = Vec<(Sc, Option<(String, Literal)>)>;

/// The Arrow type a result column of `sc` carries, as Arrow spells it.
fn arrow(sc: Sc) -> &'static str {
    match sc {
        Sc::I32 => "Int32",
        Sc::I64 => "Int64",
        Sc::U32 => "UInt32",
        Sc::F32 => "Float32",
        Sc::F64 => "Float64",
        Sc::Bool => "Boolean",
        Sc::Str => "Utf8",
        Sc::Date => "Date32",
        Sc::DateTime => unreachable!(),
    }
}

fn name(sc: Sc) -> String {
    let p = ScalarExpr::Input(sc).text(Inputs::Params);
    p[1..].to_string()
}

fn describe(expr: &ScalarExpr, row: &Row) -> String {
    let values: Vec<String> = row
        .iter()
        .map(|(sc, v)| {
            format!(
                "{}={}",
                name(*sc),
                v.as_ref().map_or("null".to_string(), |(j, _)| j.clone())
            )
        })
        .collect();
    format!(
        "--- expression\n{}\n--- row\n{}",
        expr.text(Inputs::Props),
        values.join(", ")
    )
}

/// The literal text of one input, for the folded form.
fn literal_text(sc: Sc, value: &Option<(String, Literal)>) -> String {
    match value {
        None => "null".to_string(),
        Some((json, _)) => match sc {
            Sc::F32 | Sc::F64 => {
                // The literal holds the stored value exactly: an `F32`'s
                // value widened, written in GQ's float spelling.
                let mut f: f64 = json.parse().unwrap();
                if sc == Sc::F32 {
                    f = f64::from(f as f32);
                }
                let text = format!("{:e}", f.abs());
                let text = match text.split_once('e') {
                    Some((m, e)) if !m.contains('.') => format!("{m}.0e{e}"),
                    _ => text,
                };
                if f.is_sign_negative() {
                    format!("(-{text})")
                } else {
                    text
                }
            }
            Sc::I32 | Sc::I64 | Sc::U32 if json.starts_with('-') => format!("(-{})", &json[1..]),
            _ => json.clone(),
        },
    }
}

fn substitute_literals(expr: &ScalarExpr, row: &Row) -> String {
    let mut text = expr.text(Inputs::Props);
    for (sc, value) in row {
        text = text.replace(&format!("$x.p_{}", name(*sc)), &literal_text(*sc, value));
    }
    text
}

fn has_arithmetic(expr: &ScalarExpr) -> bool {
    expr.text(Inputs::Props).contains(['+', '*', '/', '%'])
        || expr.text(Inputs::Props).contains(" - ")
        || expr.text(Inputs::Props).contains("(-")
}

async fn engine_rows(session: &Session, source: &str, params: &ParamMap) -> Result<String, String> {
    match session
        .query(ReadTarget::branch("main"), source, "q", params)
        .await
    {
        Ok(result) => Ok(result
            .to_rust_json()
            .map_err(|e| e.to_string())?
            .to_string()),
        Err(error) => Err(match omnigraph_gqt_core::EngineRefusal::of(&error) {
            omnigraph_gqt_core::EngineRefusal::Coded(code) => code.to_string(),
            _ => format!("refused: {error}"),
        }),
    }
}

async fn check(expr: &ScalarExpr, row: &Row) -> Result<(), String> {
    let fail = |what: String| format!("{what}\n{}", describe(expr, row));
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Omnigraph::init(dir.path().to_str().unwrap(), SCALAR_SCHEMA)
        .await
        .map_err(|e| e.to_string())?;
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    let mut data = vec!["\"k\":\"a\"".to_string()];
    for (sc, value) in row {
        if let Some((json, _)) = value {
            data.push(format!("\"p_{}\":{json}", name(*sc)));
        }
    }
    session
        .load_jsonl(
            &format!("{{\"type\":\"X\",\"data\":{{{}}}}}\n", data.join(",")),
            LoadMode::Overwrite,
        )
        .await
        .map_err(|e| fail(format!("load failed: {e}")))?;
    // Per row, against the reference.
    let props = format!(
        "query q() {{ match {{ $x: X }} return {{ {} as v }} }}",
        expr.text(Inputs::Props)
    );
    let judgment =
        omnigraph_gqt::judge_read_against_reference(&session, &props, "q", &ParamMap::new(), false)
            .await
            .map_err(&fail)?;
    judgment.verdict.clone().map_err(|e| {
        fail(format!(
            "per row against the reference: {e}\nengine: {}\nreference: {}",
            judgment.engine, judgment.reference
        ))
    })?;
    let per_row = engine_rows(&session, &props, &ParamMap::new()).await;
    // The type.
    let catalog = session.catalog();
    let checked = omnigraph_compiler::query::check::check_source(&catalog, &props)
        .map_err(|d| fail(d.message))?
        .pop()
        .unwrap()
        .1
        .map_err(|d| fail(d.message))?;
    let CoreDecl::Read(read) = checked.core else {
        unreachable!()
    };
    let schema = omnigraph_compiler::query::check::result_schema(&catalog, &read)
        .map_err(|e| fail(e.to_string()))?;
    let field = schema.field(0);
    let (sc, nullable) = expr.rfc_type();
    let sc = sc.unwrap_or(Sc::I64);
    if format!("{:?}", field.data_type()) != arrow(sc) || field.is_nullable() != nullable {
        return Err(fail(format!(
            "type: the checker gives {:?} (nullable {}), the RFC's join {:?} (nullable {nullable})",
            field.data_type(),
            field.is_nullable(),
            sc
        )));
    }
    // Over parameters.
    let mut used = Vec::new();
    expr.inputs(&mut used);
    let decls: Vec<String> = used
        .iter()
        .map(|sc| format!("${}: {}?", name(*sc), sc.spelling()))
        .collect();
    let mut params = ParamMap::new();
    for (sc, value) in row {
        if let (true, Some((_, literal))) = (used.contains(sc), value) {
            params.insert(name(*sc), literal.clone());
        }
    }
    let over_params = format!(
        "query q({}) {{ match {{ $x: X }} return {{ {} as v }} }}",
        decls.join(", "),
        expr.text(Inputs::Params)
    );
    let bound = engine_rows(&session, &over_params, &params).await;
    if bound != per_row {
        return Err(fail(format!(
            "over parameters: {bound:?}\nper row: {per_row:?}"
        )));
    }
    // Folded, where the folder decides: a Boolean without arithmetic (a
    // literal of a numeric result has another type than its input).
    if !has_arithmetic(expr) && expr.rfc_type().0 == Some(Sc::Bool) {
        let folded = format!(
            "query q() {{ match {{ $x: X }} return {{ {} as v }} }}",
            substitute_literals(expr, row)
        );
        let value = engine_rows(&session, &folded, &ParamMap::new()).await;
        // A literal null has no type of its own: the folded form may be
        // refused where an input is null; it is compared only when typed.
        let comparable = !matches!(&value, Err(e) if e.contains("refused"));
        if comparable && value != per_row {
            return Err(fail(format!(
                "folded ({folded}): {value:?}\nper row: {per_row:?}"
            )));
        }
    }
    Ok(())
}

fn run_law(cases: u32) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = law_runner(cases, "tests/regressions/law_scalar_evaluators.txt");
    let result = runner.run(&(choices(32), choices(256)), |(r, e)| {
        let mut c = Choices::new(&e);
        let class = *c.pick(&[Class::Int, Class::Float, Class::Bool, Class::Str]);
        let expr = ScalarExpr::generate(&mut c, class, 3);
        if expr.negates_unsigned() {
            return Ok(());
        }
        let mut rc = Choices::new(&r);
        let row: Row = SCALAR_INPUTS
            .iter()
            .map(|sc| (*sc, scalar_input_value(&mut rc, *sc)))
            .collect();
        rt.block_on(check(&expr, &row)).map_err(TestCaseError::fail)
    });
    if let Err(e) = result {
        panic!("{e}");
    }
}

#[test]
fn scalar_evaluators_agree_on_generated_expressions() {
    run_law(CASES);
}

#[test]
#[ignore = "nightly: 6,000 generated expressions; the gate runs 300"]
fn scalar_evaluators_agree_on_generated_expressions_at_nightly_breadth() {
    run_law(NIGHTLY);
}
