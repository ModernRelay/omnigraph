//! A session: one `Arc<Omnigraph>` and the settings every operation that
//! consults a setting reads from. The operations that consult a setting are
//! methods of [`Session`] and exist nowhere else; every other operation of
//! the handle is reachable through `Deref`.

use std::ops::Deref;
use std::sync::Arc;

use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::query::ast::SettingStmt;
use omnigraph_compiler::query::parser::{has_settings_prefix, parse_query};
use omnigraph_compiler::settings::{
    DEFINITIONS, SessionSettings, SessionSettingsError, SettingId, SettingRow, SettingValue,
    Source, Sources,
};

use crate::db::Omnigraph;
use crate::error::{OmniError, Result};

/// The scope of `set`: one file at the CLI, one request at the server, one
/// value an embedded caller builds. Never persisted; never shared between
/// requests.
#[derive(Clone)]
pub struct Session {
    db: Arc<Omnigraph>,
    settings: SessionSettings,
    sources: Sources,
    baseline: SessionSettings,
    baseline_sources: Sources,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("settings", &self.settings)
            .field("sources", &self.sources)
            .field("baseline", &self.baseline)
            .finish_non_exhaustive()
    }
}

impl Omnigraph {
    /// A session over this handle whose baseline is `settings` with the
    /// given sources, the pair [`omnigraph_compiler::settings::from_env`]
    /// returns for a process door.
    pub fn session(self: &Arc<Self>, settings: SessionSettings, sources: Sources) -> Session {
        Session {
            db: Arc::clone(self),
            baseline: settings.clone(),
            baseline_sources: sources,
            settings,
            sources,
        }
    }
}

impl Deref for Session {
    type Target = Arc<Omnigraph>;

    fn deref(&self) -> &Arc<Omnigraph> {
        &self.db
    }
}

impl Session {
    /// A session whose baseline is `settings` with every source `default`:
    /// `show` reports `default` and `reset` returns to these values.
    pub fn from_defaults(db: Arc<Omnigraph>, settings: SessionSettings) -> Session {
        db.session(settings, [Source::Default; DEFINITIONS.len()])
    }

    /// The same session over a reopened handle: settings, sources and the
    /// baseline survive as they are.
    pub fn rebind(self, db: Arc<Omnigraph>) -> Session {
        self.detach().attach(db)
    }

    /// Drop the handle and keep the settings: a restart releases the old
    /// `Arc<Omnigraph>` before it reopens the store.
    pub fn detach(self) -> Detached {
        Detached {
            settings: self.settings,
            sources: self.sources,
            baseline: self.baseline,
            baseline_sources: self.baseline_sources,
        }
    }

    pub fn db(&self) -> &Arc<Omnigraph> {
        &self.db
    }

    pub fn settings(&self) -> &SessionSettings {
        &self.settings
    }

    pub fn sources(&self) -> &Sources {
        &self.sources
    }

    /// Assign one setting for the rest of the session. The only mutator.
    ///
    /// # Errors
    ///
    /// The definition's refusal of `value`; the session is unchanged then.
    pub fn set(
        &mut self,
        id: SettingId,
        value: &SettingValue,
        source: Source,
    ) -> std::result::Result<(), SessionSettingsError> {
        self.settings.set(id, value)?;
        self.sources[id as usize] = source;
        Ok(())
    }

    /// Restore one setting, or every setting on `None`, to the baseline and
    /// its source.
    pub fn reset(&mut self, id: Option<SettingId>) {
        let ids = id.map_or_else(|| SettingId::ALL.to_vec(), |id| vec![id]);
        for id in ids {
            let index = id as usize;
            self.settings.copy_field(&self.baseline, id);
            self.sources[index] = self.baseline_sources[index];
        }
    }

    /// Apply one line of a settings prefix: a `set` with source `file`, a
    /// `reset` from the baseline.
    ///
    /// # Errors
    ///
    /// As [`Session::set`].
    pub fn apply(&mut self, stmt: &SettingStmt) -> std::result::Result<(), SessionSettingsError> {
        match stmt {
            SettingStmt::Set { id, value } => self.set(*id, value, Source::File),
            SettingStmt::Reset { id } => {
                self.reset(*id);
                Ok(())
            }
        }
    }

    /// The `show` rows: one setting, or every setting in definition order.
    pub fn show(&self, id: Option<SettingId>) -> Vec<SettingRow> {
        let ids = id.map_or_else(|| SettingId::ALL.to_vec(), |id| vec![id]);
        ids.into_iter()
            .map(|id| self.settings.row(id, self.sources[id as usize]))
            .collect()
    }

    /// The settings one call runs under: the session's, with the settings
    /// prefix of `source` applied in file order on a copy. The session is
    /// unchanged. A source without a prefix (`has_settings_prefix`) is not
    /// parsed here, so a warm query reaches its compiled-query cache without
    /// a parse.
    ///
    /// # Errors
    ///
    /// A prefix the parser or the definition refuses.
    pub fn effective(&self, source: &str) -> Result<SessionSettings> {
        if !has_settings_prefix(source) {
            return Ok(self.settings.clone());
        }
        let file = parse_query(source).map_err(OmniError::Compiler)?;
        let scoped = self
            .with_prefix(&file.settings)
            .map_err(|error| OmniError::Compiler(CompilerError::Parse(error.to_string())))?;
        Ok(scoped.settings)
    }

    /// A copy of this session with a settings prefix applied in file order;
    /// this session is unchanged.
    ///
    /// # Errors
    ///
    /// The first line the definition refuses.
    pub fn with_prefix(
        &self,
        stmts: &[SettingStmt],
    ) -> std::result::Result<Session, SessionSettingsError> {
        let mut scoped = self.clone();
        for stmt in stmts {
            scoped.apply(stmt)?;
        }
        Ok(scoped)
    }
}

/// A session with no handle: the settings, sources and baseline of a
/// [`Session`] whose `Arc<Omnigraph>` was dropped, until `attach` gives them
/// a reopened one.
#[derive(Debug, Clone)]
pub struct Detached {
    settings: SessionSettings,
    sources: Sources,
    baseline: SessionSettings,
    baseline_sources: Sources,
}

impl Detached {
    /// The same settings over a reopened handle.
    pub fn attach(self, db: Arc<Omnigraph>) -> Session {
        Session {
            db,
            settings: self.settings,
            sources: self.sources,
            baseline: self.baseline,
            baseline_sources: self.baseline_sources,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = "node Person { name: String @key }\n";

    fn seeded_settings() -> SessionSettings {
        SessionSettings::default()
            .with("merge_lineage", "off")
            .unwrap()
            .with("stage_write_concurrency", "4")
            .unwrap()
    }

    fn seeded_sources() -> Sources {
        let mut sources = [Source::Default; DEFINITIONS.len()];
        sources[SettingId::MergeLineage as usize] = Source::Env;
        sources[SettingId::StageWriteConcurrency as usize] = Source::Env;
        sources
    }

    async fn seeded_session(dir: &tempfile::TempDir) -> Session {
        let uri = dir.path().to_str().unwrap();
        let db = Arc::new(Omnigraph::init(uri, SCHEMA).await.unwrap());
        db.session(seeded_settings(), seeded_sources())
    }

    fn shown(session: &Session, id: SettingId) -> (String, Source) {
        let rows = session.show(Some(id));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, id.name());
        (rows[0].value.clone(), rows[0].source)
    }

    fn assert_at_baseline(session: &Session) {
        assert_eq!(session.settings(), &seeded_settings());
        assert_eq!(session.sources(), &seeded_sources());
        let rows = session.show(None);
        assert_eq!(rows.len(), DEFINITIONS.len());
        for (row, id) in rows.iter().zip(SettingId::ALL) {
            assert_eq!(row.value, seeded_settings().get(id));
            assert_eq!(row.source, seeded_sources()[id as usize]);
        }
    }

    #[tokio::test]
    async fn baseline_reset_and_refusals_hold_value_and_source_together() {
        let dir = tempfile::tempdir().unwrap();
        let mut session = seeded_session(&dir).await;
        assert_eq!(
            shown(&session, SettingId::MergeLineage),
            ("off".to_string(), Source::Env)
        );
        assert_eq!(
            shown(&session, SettingId::StageWriteConcurrency),
            ("4".to_string(), Source::Env)
        );
        assert_eq!(
            shown(&session, SettingId::RrfPlan),
            ("auto".to_string(), Source::Default)
        );

        session
            .apply(&SettingStmt::Set {
                id: SettingId::MergeLineage,
                value: SettingValue::Ident("on".to_string()),
            })
            .unwrap();
        session
            .set(
                SettingId::StageWriteConcurrency,
                &SettingValue::Integer(2),
                Source::Request,
            )
            .unwrap();
        assert_eq!(
            shown(&session, SettingId::MergeLineage),
            ("on".to_string(), Source::File)
        );
        assert_eq!(
            shown(&session, SettingId::StageWriteConcurrency),
            ("2".to_string(), Source::Request)
        );

        session.reset(Some(SettingId::MergeLineage));
        assert_eq!(
            shown(&session, SettingId::MergeLineage),
            ("off".to_string(), Source::Env)
        );
        assert_eq!(
            shown(&session, SettingId::StageWriteConcurrency),
            ("2".to_string(), Source::Request)
        );

        let wrong_kind = session
            .set(
                SettingId::StageWriteConcurrency,
                &SettingValue::Ident("wide".to_string()),
                Source::File,
            )
            .unwrap_err();
        assert!(matches!(
            wrong_kind,
            SessionSettingsError::WrongKind {
                setting: SettingId::StageWriteConcurrency,
                ..
            }
        ));
        let unknown_value = session
            .set(
                SettingId::MergeLineage,
                &SettingValue::Ident("sometimes".to_string()),
                Source::File,
            )
            .unwrap_err();
        assert!(matches!(
            unknown_value,
            SessionSettingsError::UnknownValue {
                setting: SettingId::MergeLineage,
                ..
            }
        ));
        let out_of_range = session
            .set(
                SettingId::StageWriteConcurrency,
                &SettingValue::Integer(0),
                Source::Request,
            )
            .unwrap_err();
        assert!(matches!(
            out_of_range,
            SessionSettingsError::OutOfRange {
                setting: SettingId::StageWriteConcurrency,
                ..
            }
        ));
        assert_eq!(
            shown(&session, SettingId::MergeLineage),
            ("off".to_string(), Source::Env),
            "a refused assignment changes neither value nor source"
        );
        assert_eq!(
            shown(&session, SettingId::StageWriteConcurrency),
            ("2".to_string(), Source::Request)
        );

        session.apply(&SettingStmt::Reset { id: None }).unwrap();
        assert_at_baseline(&session);

        let refused = session
            .effective("set merge_lineage = sometimes;\nshow all;")
            .unwrap_err();
        assert!(matches!(refused, OmniError::Compiler(_)), "{refused:?}");
        assert!(
            refused.to_string().contains(
                "unknown value `sometimes` for setting `merge_lineage`; expected one of off, on, verify"
            ),
            "{refused}"
        );
        assert_at_baseline(&session);
    }
}
