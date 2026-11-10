//! The compiler's intermediate representations: the typed core IR every
//! declaration lowers to ([`core`]), the bound expression its terms hold
//! ([`expr`]), and the load and write-table IR of `load` statements and
//! schema writes.

pub mod coerce;
pub mod core;
pub mod expr;
pub mod fold;
pub mod load;
pub mod write;

pub use core::is_fresh_variable;
pub use expr::{BlockAggregateExpr, IRExpr, IROrdering, IRProjection, SubqueryPredicate};
pub use load::{Endpoint, LoadCheck, LoadIR, UniqueRole, ValidateCheck, checks_for, lower_load};
pub use write::{DeltaClass, WriteKind, WriteTableIR, lower_write_table};

use std::collections::HashMap;

use crate::query::file::Param;
use crate::query::value::Literal;
use crate::types::ExprType;

/// A declaration and the type resolved for it by the compiler.
#[derive(Debug, Clone)]
pub struct IRParam {
    pub declaration: Param,
    pub ty: ExprType,
}

/// Resolved runtime parameters: param name → literal value.
pub type ParamMap = HashMap<String, Literal>;
