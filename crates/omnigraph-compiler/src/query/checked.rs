//! A read declaration the type checker accepted, kept beside its checked
//! query so a later stage derives what the query requires from what it
//! wrote (RFC 0047, "Requirements come from the checked query"). The checked
//! query retains every typed expression; the declaration keeps the written
//! spelling of the score calls a projection or the leading order key names.

use std::sync::Arc;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::query::ast::QueryDecl;
use crate::query::typecheck::{CheckedReadQuery, typecheck_query};

/// One checked read declaration and the checked query the type checker
/// built for it. Shared by `Arc`: a compiled query is cached and read by
/// every door.
#[derive(Debug, Clone)]
pub struct CheckedRead {
    decl: Arc<QueryDecl>,
    query: Arc<CheckedReadQuery>,
}

impl CheckedRead {
    /// Type-check `decl` against `catalog`, keeping both.
    pub fn check(catalog: &Catalog, decl: &QueryDecl) -> Result<Self> {
        let query = typecheck_query(catalog, decl)?;
        Ok(Self {
            decl: Arc::new(decl.clone()),
            query: Arc::new(query),
        })
    }

    /// The declaration as the query wrote it.
    pub fn decl(&self) -> &QueryDecl {
        &self.decl
    }

    /// The checked query the type checker built for [`Self::decl`].
    pub fn query(&self) -> &CheckedReadQuery {
        &self.query
    }
}
