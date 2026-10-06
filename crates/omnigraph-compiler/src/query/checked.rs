//! A read declaration the type checker accepted, kept beside its IR so a
//! later stage derives what the query requires from what it wrote, not from
//! the IR alone (RFC 0047, "Requirements come from the checked query"). The
//! IR loses meaning on the way: a projected `bm25($d.title, $q)` becomes the
//! column `$d._score`, inline matches become comparisons, and literals fold.

use std::sync::Arc;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::query::ast::QueryDecl;
use crate::query::typecheck::{TypeContext, typecheck_query};

/// One checked read declaration and the type context the checker built for
/// it. Shared by `Arc`: a compiled query is cached and read by every door.
#[derive(Debug, Clone)]
pub struct CheckedQuery {
    decl: Arc<QueryDecl>,
    types: Arc<TypeContext>,
}

impl CheckedQuery {
    /// Type-check `decl` against `catalog`, keeping both.
    pub fn check(catalog: &Catalog, decl: &QueryDecl) -> Result<Self> {
        let types = typecheck_query(catalog, decl)?;
        Ok(Self {
            decl: Arc::new(decl.clone()),
            types: Arc::new(types),
        })
    }

    /// The declaration as the query wrote it.
    pub fn decl(&self) -> &QueryDecl {
        &self.decl
    }

    /// The type context the checker built for [`Self::decl`].
    pub fn types(&self) -> &TypeContext {
        &self.types
    }
}
