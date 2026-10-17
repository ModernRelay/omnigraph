//! Pure query-door preparation over already captured source and arguments.
//!
//! Parsing here recognizes control statements and their CLI envelope rules.
//! Unrecognized source stays opaque: the execution owner retains responsibility
//! for syntax diagnostics, including source accepted by a newer server.

use color_eyre::eyre::{Result, bail};
use omnigraph_api_types::{branch_statement_refusals, query_file_refusals};
use omnigraph_compiler::query::ast::{
    BranchRead, BranchStmt, BranchWrite, CommitWrite, EXPLAIN_STATEMENT_NAME, EmptyFile, FileBody,
    LoadStmt, QueryFile, SettingStmt, show_statement_name,
};
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::settings::SettingId;
use serde_json::{Map, Value};

use crate::operator::OperatorAlias;

/// Source presence, not a positional name, selects the ad-hoc invocation lane.
pub(super) enum QueryInput<'a> {
    Supplied(&'a str),
    Stored,
}

/// Explicit envelope facts are retained separately from effective defaults.
pub(super) struct Envelope<'a> {
    pub name: Option<&'a str>,
    pub has_params: bool,
    pub has_target: bool,
    pub has_expected_head: bool,
    pub has_settings: bool,
}

pub(super) enum ReadOperation<'a> {
    AdHoc(&'a str),
    Stored(&'a str),
    BranchList(&'a str),
    BranchRead(&'a str, BranchRead),
    Show(&'a str, Option<SettingId>),
}

pub(super) enum MutationOperation<'a> {
    AdHoc(&'a str),
    Stored(&'a str),
    Branch(&'a str, BranchWrite),
    Commit(&'a str, CommitWrite),
    Load(&'a str, LoadStmt),
}

pub(super) fn prepare_read<'a>(
    input: QueryInput<'a>,
    envelope: Envelope<'a>,
    remote: bool,
) -> Result<ReadOperation<'a>> {
    let QueryInput::Supplied(source) = input else {
        refuse_stored_settings(envelope.has_settings)?;
        let Some(name) = envelope.name else {
            bail!(
                "provide a query name to invoke from the catalog, or -e '<gq>' / \
                 --query <file> for an ad-hoc query"
            );
        };
        return Ok(ReadOperation::Stored(name));
    };
    match parse_query(source) {
        Ok(QueryFile {
            body: FileBody::Branch(statement),
            settings: prefix,
            ..
        }) => {
            refuse_process_prefix_when_remote(remote, &prefix)?;
            if let BranchStmt::Write(write) = &statement {
                bail!(
                    "{}",
                    branch_statement_refusals::with_statement(
                        branch_statement_refusals::CONTROL_WRITE_AT_READ_DOOR,
                        write.statement_name(),
                    )
                );
            }
            refuse_statement_envelope(&envelope, envelope.has_target)?;
            match statement {
                BranchStmt::List => Ok(ReadOperation::BranchList(source)),
                BranchStmt::Read(read) => Ok(ReadOperation::BranchRead(source, read)),
                BranchStmt::Write(_) => unreachable!("write refused above"),
            }
        }
        Ok(QueryFile {
            body: FileBody::Commit(write),
            ..
        }) => bail!(
            "{}",
            branch_statement_refusals::with_statement(
                branch_statement_refusals::CONTROL_WRITE_AT_READ_DOOR,
                write.statement_name(),
            )
        ),
        Ok(QueryFile {
            body: FileBody::Load(statement),
            ..
        }) => bail!(
            "{}",
            branch_statement_refusals::with_statement(
                branch_statement_refusals::CONTROL_WRITE_AT_READ_DOOR,
                statement.statement_name(),
            )
        ),
        Ok(QueryFile {
            body: FileBody::Show(id),
            settings: prefix,
            ..
        }) => {
            refuse_process_prefix_when_remote(remote, &prefix)?;
            refuse_statement_envelope(&envelope, envelope.has_target)?;
            Ok(ReadOperation::Show(source, id))
        }
        parsed => {
            if let Ok(file) = &parsed {
                refuse_prefix_this_door_cannot_run(remote, file)?;
            }
            Ok(ReadOperation::AdHoc(source))
        }
    }
}

pub(super) fn prepare_mutation<'a>(
    input: QueryInput<'a>,
    envelope: Envelope<'a>,
    remote: bool,
) -> Result<MutationOperation<'a>> {
    let QueryInput::Supplied(source) = input else {
        refuse_stored_settings(envelope.has_settings)?;
        let Some(name) = envelope.name else {
            bail!(
                "provide a mutation name to invoke from the catalog, or -e '<gq>' / \
                 --query <file> for an ad-hoc mutation"
            );
        };
        return Ok(MutationOperation::Stored(name));
    };
    match parse_query(source) {
        Ok(QueryFile {
            body: FileBody::Branch(statement),
            settings: prefix,
            ..
        }) => {
            refuse_process_prefix_when_remote(remote, &prefix)?;
            let statement_name = statement.statement_name();
            let BranchStmt::Write(write) = statement else {
                bail!("{}", read_at_write_door(statement_name));
            };
            refuse_statement_envelope(&envelope, envelope.has_target)?;
            Ok(MutationOperation::Branch(source, write))
        }
        Ok(QueryFile {
            body: FileBody::Commit(write),
            settings: prefix,
            ..
        }) => {
            refuse_process_prefix_when_remote(remote, &prefix)?;
            refuse_statement_envelope(&envelope, envelope.has_target)?;
            Ok(MutationOperation::Commit(source, write))
        }
        Ok(QueryFile {
            body: FileBody::Load(statement),
            settings: prefix,
            ..
        }) => {
            refuse_process_prefix_when_remote(remote, &prefix)?;
            // A load statement consumes --branch, unlike other control writes.
            refuse_statement_envelope(&envelope, false)?;
            Ok(MutationOperation::Load(source, statement))
        }
        Ok(QueryFile {
            body: FileBody::Show(id),
            ..
        }) => bail!("{}", read_at_write_door(&show_statement_name(id))),
        Ok(QueryFile {
            body: FileBody::Explain(_),
            ..
        }) => bail!("{}", read_at_write_door(EXPLAIN_STATEMENT_NAME)),
        parsed => {
            if let Ok(file) = &parsed {
                refuse_prefix_this_door_cannot_run(remote, file)?;
            }
            Ok(MutationOperation::AdHoc(source))
        }
    }
}

fn refuse_stored_settings(has_settings: bool) -> Result<()> {
    if has_settings {
        bail!(
            "--set applies to an ad-hoc source (-e '<gq>' / --query <file>), not to a stored query"
        );
    }
    Ok(())
}

/// A source prefix cannot stand alone or change served process settings.
fn refuse_prefix_this_door_cannot_run(remote: bool, file: &QueryFile) -> Result<()> {
    if matches!(file.empty_kind(), Some(EmptyFile::SettingsOnly)) {
        bail!(query_file_refusals::ONLY_SETTINGS);
    }
    refuse_process_prefix_when_remote(remote, &file.settings)
}

fn refuse_process_prefix_when_remote(remote: bool, prefix: &[SettingStmt]) -> Result<()> {
    if remote {
        for id in prefix.iter().filter_map(SettingStmt::id) {
            id.refuse_from_request()?;
        }
    }
    Ok(())
}

fn read_at_write_door(statement_name: &str) -> String {
    branch_statement_refusals::with_statement(
        branch_statement_refusals::READ_AT_WRITE_DOOR,
        statement_name,
    )
}

/// Keep the server's envelope refusal order: target, name/params, precondition.
fn refuse_statement_envelope(envelope: &Envelope<'_>, has_target: bool) -> Result<()> {
    if has_target {
        bail!(branch_statement_refusals::REQUEST_TARGET);
    }
    if envelope.name.is_some() || envelope.has_params {
        bail!(branch_statement_refusals::NAME_OR_PARAMS);
    }
    if envelope.has_expected_head {
        bail!(branch_statement_refusals::COMMIT_PRECONDITION);
    }
    Ok(())
}

/// Alias defaults, positional values, then explicit JSON form one parameter map.
/// Its caller preserves credential setup before validating these values.
pub(super) fn alias_params(
    alias_name: &str,
    alias: &OperatorAlias,
    args: &[String],
    explicit: Option<Value>,
) -> Result<Map<String, Value>> {
    let mut params = Map::new();
    for (key, value) in &alias.params {
        let Some(key) = key.as_str() else {
            bail!("alias '{alias_name}': params keys must be strings");
        };
        params.insert(key.to_string(), serde_json::to_value(value)?);
    }
    if args.len() > alias.args.len() {
        bail!(
            "alias '{alias_name}' takes {} positional arg(s) ({}), got {}",
            alias.args.len(),
            alias.args.join(", "),
            args.len()
        );
    }
    for (name, value) in alias.args.iter().zip(args) {
        params.insert(
            name.clone(),
            serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string())),
        );
    }
    if let Some(Value::Object(explicit)) = explicit {
        for (key, value) in explicit {
            params.insert(key, value);
        }
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::{
        Envelope, MutationOperation, QueryInput, ReadOperation, alias_params, prepare_mutation,
        prepare_read,
    };
    use crate::operator::OperatorAlias;

    fn envelope() -> Envelope<'static> {
        Envelope {
            name: None,
            has_params: false,
            has_target: false,
            has_expected_head: false,
            has_settings: false,
        }
    }

    #[test]
    fn unrecognized_source_keeps_its_bytes_and_does_not_become_stored_invocation() {
        let source = "  syntax known only to the execution owner\n";
        let invocation = || Envelope {
            name: Some("selected"),
            ..envelope()
        };
        assert!(matches!(
            prepare_read(QueryInput::Supplied(source), invocation(), true).unwrap(),
            ReadOperation::AdHoc(forwarded) if forwarded == source
        ));
        assert!(matches!(
            prepare_mutation(QueryInput::Supplied(source), invocation(), true).unwrap(),
            MutationOperation::AdHoc(forwarded) if forwarded == source
        ));
        assert!(matches!(
            prepare_read(QueryInput::Stored, invocation(), true).unwrap(),
            ReadOperation::Stored("selected")
        ));
    }

    #[test]
    fn statement_door_refusal_precedes_envelope_and_target_precedes_name() {
        let conflicting = || Envelope {
            name: Some("selected"),
            has_params: true,
            has_target: true,
            has_expected_head: true,
            ..envelope()
        };
        let wrong_door = prepare_read(
            QueryInput::Supplied("branch create feature"),
            conflicting(),
            true,
        )
        .err()
        .unwrap();
        assert_eq!(
            wrong_door.to_string(),
            "statement 'branch create' is a control write; use POST /mutate"
        );
        let target = prepare_read(QueryInput::Supplied("branch list"), conflicting(), true)
            .err()
            .unwrap();
        assert_eq!(
            target.to_string(),
            "a branch statement names its branches itself; drop the request target"
        );
    }

    #[test]
    fn alias_parameters_keep_defaults_positionals_and_explicit_precedence() {
        let alias: OperatorAlias = yaml_serde::from_str(
            "server: example\nquery: lookup\nargs: [name, enabled]\nparams:\n  name: default\n  limit: 4\n",
        )
        .unwrap();
        let params = alias_params(
            "lookup",
            &alias,
            &["positional".into(), "true".into()],
            Some(serde_json::json!({"name": "explicit"})),
        )
        .unwrap();
        assert_eq!(
            serde_json::Value::Object(params),
            serde_json::json!({"name": "explicit", "enabled": true, "limit": 4})
        );
    }
}
