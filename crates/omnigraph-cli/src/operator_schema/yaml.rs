use yaml_serde::Value;

use super::ConfigError;

pub(super) fn parse(text: &str) -> Result<Value, ConfigError> {
    reject_yaml_extensions(text)?;
    let value: Value = yaml_serde::from_str(text)
        .map_err(|error| ConfigError(format!("invalid operator YAML: {error}")))?;
    if !value.is_mapping() {
        return Err(ConfigError("operator document must be a mapping".into()));
    }
    validate(&value, &mut Vec::new(), false)?;
    Ok(value)
}

fn reject_yaml_extensions(text: &str) -> Result<(), ConfigError> {
    use std::mem::MaybeUninit;

    use libyaml_rs as sys;

    let mut parser = MaybeUninit::<sys::yaml_parser_t>::uninit();
    unsafe {
        let parser = parser.as_mut_ptr();
        if sys::yaml_parser_initialize(parser).fail {
            return Err(ConfigError("could not initialize YAML scanner".into()));
        }
        sys::yaml_parser_set_input_string(parser, text.as_ptr(), text.len() as u64);
        let result = loop {
            let mut token = MaybeUninit::<sys::yaml_token_t>::uninit();
            if sys::yaml_parser_scan(parser, token.as_mut_ptr()).fail {
                break Err(ConfigError("invalid operator YAML token stream".into()));
            }
            let kind = (*token.as_ptr()).type_;
            sys::yaml_token_delete(token.as_mut_ptr());
            match kind {
                sys::YAML_TAG_TOKEN | sys::YAML_TAG_DIRECTIVE_TOKEN => {
                    break Err(ConfigError("YAML tags are forbidden".into()));
                }
                sys::YAML_ANCHOR_TOKEN | sys::YAML_ALIAS_TOKEN => {
                    break Err(ConfigError("YAML anchors and aliases are forbidden".into()));
                }
                sys::YAML_STREAM_END_TOKEN => break Ok(()),
                _ => {}
            }
        };
        sys::yaml_parser_delete(parser);
        result
    }
}

fn validate(value: &Value, path: &mut Vec<String>, parameter: bool) -> Result<(), ConfigError> {
    let location = path.join(".");
    let fail = |message: &str| ConfigError(format!("{location}: {message}"));
    match value {
        Value::Tagged(_) => return Err(fail("YAML tags are forbidden")),
        Value::Null if !parameter => {
            return Err(fail("null is allowed only inside alias parameter values"));
        }
        Value::Number(number) if number.as_f64().is_some_and(|n| !n.is_finite()) => {
            return Err(fail("non-finite numbers are not JSON-compatible"));
        }
        Value::Mapping(mapping) => {
            for (key, value) in mapping {
                let Some(key) = key.as_str() else {
                    return Err(fail("mapping keys must be strings"));
                };
                if key == "<<" {
                    return Err(fail("YAML merge keys are forbidden"));
                }
                let parameter_value =
                    parameter || (path.len() == 3 && path[0] == "aliases" && path[2] == "params");
                path.push(key.to_owned());
                validate(value, path, parameter_value)?;
                path.pop();
            }
        }
        Value::Sequence(values) => {
            for value in values {
                validate(value, path, parameter)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn anchors_and_aliases_are_rejected_without_rejecting_literal_text() {
        for text in [
            "version: 1\ndefaults: &defaults {}",
            "version: 1\ndefaults: &defaults {}\ntargets: *defaults",
            "version: 1\ndefaults: &defaults {}\ntargets: {<<: *defaults}",
        ] {
            assert!(super::parse(text).is_err(), "{text}");
        }
        assert!(super::parse("version: 1\ntext: '&anchor *alias !tag' # &comment").is_ok());
    }

    #[test]
    fn legacy_document_error_names_the_upgrade_guide() {
        let error = crate::operator_schema::parse(
            "target_config_version: 1\n",
            std::path::Path::new("/operator"),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("target_config_version"));
        assert!(error.contains("docs/user/cli/targets.md#upgrade-from-the-old-format"));
        assert!(error.contains("no automatic importer"));
    }
}
