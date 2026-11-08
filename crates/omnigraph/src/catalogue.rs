//! The `og_catalog` scope: the engine catalogue queried with no store. A
//! [`CatalogueScope`] compiles a read against a catalog that declares only
//! the built-in `@` types, plans it with no snapshot and runs it through the
//! same acceptance, lowering and DataFusion path every graph query takes;
//! every scan is a catalogue scan, and a node that asks for graph storage
//! fails its lowering. Nothing here opens, initializes or reads a store.

use std::sync::Arc;

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::catalog::catalogue::catalogue_declarations;
use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::SettingStmt;
use omnigraph_compiler::query::codes::Q003;
use omnigraph_compiler::query::diagnostic::QueryDiagnostic;
use omnigraph_compiler::query::parser::{has_settings_prefix, parse_query};
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::settings::{
    SessionSettings, SessionSettingsError, SettingId, SettingValue,
};
use omnigraph_compiler::{CheckedRead, build_catalog, lower_query};

use crate::engine;
use crate::error::{OmniError, Result};
use crate::runtime_cache::CompiledQuery;

pub use omnigraph_compiler::CATALOGUE_SCOPE;

/// The catalogue-only query scope of one process: the built-in types and
/// the settings the reads run under.
#[derive(Clone)]
pub struct CatalogueScope {
    catalog: Arc<Catalog>,
    baseline: SessionSettings,
    settings: SessionSettings,
}

impl CatalogueScope {
    /// A scope over the catalogue types alone, reading under `settings`.
    ///
    /// # Errors
    ///
    /// The empty user schema fails to build, which it never does.
    pub fn new(settings: SessionSettings) -> Result<Self> {
        let schema = omnigraph_compiler::schema::parser::parse_schema("")?;
        let catalog = build_catalog(&schema)?;
        Ok(Self {
            catalog: Arc::new(catalog),
            baseline: settings.clone(),
            settings,
        })
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    pub fn settings(&self) -> &SessionSettings {
        &self.settings
    }

    /// Override one setting for subsequent reads, retaining the process
    /// defaults as the baseline for a query's `reset` prefix.
    ///
    /// # Errors
    ///
    /// The value has the wrong kind or is outside the setting's allowed range.
    pub fn set(
        &mut self,
        id: SettingId,
        value: &SettingValue,
    ) -> std::result::Result<(), SessionSettingsError> {
        self.settings.set(id, value)
    }

    /// The `schema show` text of the scope: one declaration per catalogue
    /// type.
    pub fn schema_source(&self) -> String {
        catalogue_declarations()
    }

    /// The settings a source runs under: this scope's, with the source's
    /// `set` and `reset` prefix applied in file order.
    fn effective(&self, source: &str) -> Result<SessionSettings> {
        if !has_settings_prefix(source) {
            return Ok(self.settings.clone());
        }
        let file = parse_query(source).map_err(OmniError::Compiler)?;
        let mut settings = self.settings.clone();
        for stmt in &file.settings {
            let applied = match stmt {
                SettingStmt::Set { id, value } => settings.set(*id, value),
                SettingStmt::Reset { id: Some(id) } => {
                    settings.copy_field(&self.baseline, *id);
                    Ok(())
                }
                SettingStmt::Reset { id: None } => {
                    settings = self.baseline.clone();
                    Ok(())
                }
            };
            applied.map_err(|error| {
                OmniError::Compiler(CompilerError::query(QueryDiagnostic::parse(
                    Q003,
                    error.to_string(),
                    None,
                )))
            })?;
        }
        Ok(settings)
    }

    fn compile(&self, source: &str, name: &str) -> Result<(CompiledQuery, bool)> {
        let statement = omnigraph_compiler::find_read_statement(source, name)
            .map_err(crate::exec::query_lookup_error)?;
        let checked = CheckedRead::check(&self.catalog, statement.decl())?;
        let ir = Arc::new(lower_query(&self.catalog, checked.query())?);
        Ok((CompiledQuery { ir, checked }, statement.is_explain()))
    }

    /// Run the read statement `name` of `source` (a declaration, or
    /// `explain` over one) and answer its rows.
    ///
    /// # Errors
    ///
    /// A parse, check or planning refusal; a read of a stored type, which
    /// this scope has none of; every run-time error of the plan.
    pub async fn query(&self, source: &str, name: &str, params: &ParamMap) -> Result<QueryResult> {
        let settings = self.effective(source)?;
        let (query, explain) = self.compile(source, name)?;
        if explain {
            return engine::explain_rows(&query, params, None, &self.catalog, &settings).await;
        }
        engine::execute_query(
            &query,
            params,
            None,
            engine::GraphIndexHandle::none(),
            &self.catalog,
            &engine::EmbeddingResolver::explain(),
            &settings,
        )
        .await
    }

    /// The planner's document for the read `name` of `source`, run nothing.
    ///
    /// # Errors
    ///
    /// As [`Self::query`], before execution.
    pub async fn explain(
        &self,
        source: &str,
        name: &str,
        params: &ParamMap,
    ) -> Result<serde_json::Value> {
        let settings = self.effective(source)?;
        let (query, _) = self.compile(source, name)?;
        engine::explain_document(&query, params, &self.catalog, None, &settings).await
    }
}
