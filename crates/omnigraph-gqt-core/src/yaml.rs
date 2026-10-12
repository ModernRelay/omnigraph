use std::ffi::CStr;
use std::mem::MaybeUninit;

const FORBIDDEN_YAML: &str = "invalid_case: YAML aliases, anchors, tags and merge keys are refused";

struct YamlTokens<'input> {
    parser: *mut unsafe_libyaml::yaml_parser_t,
    _input: &'input str,
}

impl<'input> YamlTokens<'input> {
    fn new(input: &'input str) -> Result<Self, String> {
        let input_len = input
            .len()
            .try_into()
            .map_err(|_| "invalid_case: YAML input length is out of range".to_string())?;
        let allocation = Box::into_raw(Box::<unsafe_libyaml::yaml_parser_t>::new_uninit());
        let parser = allocation.cast::<unsafe_libyaml::yaml_parser_t>();
        // SAFETY: allocation is non-null, properly aligned and large enough for yaml_parser_t. Initialization writes the previously uninitialized object in place. No reference or pointer to a moved parser is created.
        if unsafe { unsafe_libyaml::yaml_parser_initialize(parser) }.fail {
            // SAFETY: the allocation still has its original MaybeUninit type; no initialized parser ownership has been established on failure.
            unsafe { drop(Box::from_raw(allocation)) };
            return Err("invalid_case: failed to initialize YAML parser".into());
        }
        let tokens = Self {
            parser,
            _input: input,
        };
        // SAFETY: parser was initialized successfully at this stable address. The input slice remains valid for this owner's lifetime; its byte length was converted without truncation. This sets the input once.
        unsafe {
            unsafe_libyaml::yaml_parser_set_input_string(parser, input.as_ptr(), input_len);
        }
        Ok(tokens)
    }

    fn next(&mut self) -> Result<YamlToken, String> {
        let mut token = MaybeUninit::<unsafe_libyaml::yaml_token_t>::uninit();
        // SAFETY: parser is initialized, exclusively accessed here and remains at its original address. The token output points to writable storage. Only scanner calls are made; scan/parse/load are never interleaved.
        if unsafe { unsafe_libyaml::yaml_parser_scan(self.parser, token.as_mut_ptr()) }.fail {
            return Err(self.error());
        }
        // SAFETY: successful yaml_parser_scan initialized the complete token. Its owned buffers are transferred into one non-Copy RAII owner.
        let token = YamlToken(unsafe { token.assume_init() });
        if token.kind() == unsafe_libyaml::YAML_NO_TOKEN {
            return Err("invalid_case: YAML scanner returned no token".into());
        }
        Ok(token)
    }

    fn error(&self) -> String {
        // SAFETY: the parser remains initialized and is not being mutated while the error fields are read. libyaml owns the non-null NUL-terminated problem string through this parser's lifetime. Format copies it into the returned owned String before parser destruction.
        unsafe {
            let parser = &*self.parser;
            let problem = if parser.problem.is_null() {
                "invalid YAML".into()
            } else {
                CStr::from_ptr(parser.problem.cast()).to_string_lossy()
            };
            format!(
                "invalid_case: invalid YAML at line {} column {}: {problem}",
                parser.problem_mark.line.saturating_add(1),
                parser.problem_mark.column.saturating_add(1),
            )
        }
    }
}

impl Drop for YamlTokens<'_> {
    fn drop(&mut self) {
        // SAFETY: initialization succeeded before this owner was constructed; it is the unique owner. Delete internal allocations, then free the original Box<MaybeUninit<yaml_parser_t>> allocation exactly once.
        unsafe {
            unsafe_libyaml::yaml_parser_delete(self.parser);
            drop(Box::from_raw(
                self.parser
                    .cast::<MaybeUninit<unsafe_libyaml::yaml_parser_t>>(),
            ));
        }
    }
}

struct YamlToken(unsafe_libyaml::yaml_token_t);

impl YamlToken {
    fn kind(&self) -> unsafe_libyaml::yaml_token_type_t {
        self.0.type_
    }

    fn is_plain_merge_key(&self) -> bool {
        if self.kind() != unsafe_libyaml::YAML_SCALAR_TOKEN {
            return false;
        }
        // SAFETY: the token discriminator proves scalar is the active union member. For a successful scalar token, libyaml owns at least length bytes at value. Read two bytes only when length is exactly two, and only while this token (and therefore its buffers) is alive.
        unsafe {
            let scalar = self.0.data.scalar;
            scalar.style == unsafe_libyaml::YAML_PLAIN_SCALAR_STYLE
                && scalar.length == 2
                && std::slice::from_raw_parts(scalar.value, 2) == b"<<"
        }
    }
}

impl Drop for YamlToken {
    fn drop(&mut self) {
        // SAFETY: this uniquely owns a successfully scanned token; libyaml selects and frees the appropriate buffers from its discriminator.
        unsafe { unsafe_libyaml::yaml_token_delete(&mut self.0) };
    }
}

fn validate_yaml_tokens(body: &str) -> Result<(), String> {
    let mut tokens = YamlTokens::new(body)?;
    let mut previous = unsafe_libyaml::YAML_NO_TOKEN;
    loop {
        let token = tokens.next()?;
        let kind = token.kind();
        if matches!(
            kind,
            unsafe_libyaml::YAML_ALIAS_TOKEN
                | unsafe_libyaml::YAML_ANCHOR_TOKEN
                | unsafe_libyaml::YAML_TAG_TOKEN
                | unsafe_libyaml::YAML_TAG_DIRECTIVE_TOKEN
        ) || (previous == unsafe_libyaml::YAML_KEY_TOKEN && token.is_plain_merge_key())
        {
            return Err(FORBIDDEN_YAML.into());
        }
        if kind == unsafe_libyaml::YAML_STREAM_END_TOKEN {
            return Ok(());
        }
        previous = kind;
    }
}

fn refuse_non_string_keys(value: &serde_yaml::Value) -> Result<(), String> {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            for (key, value) in mapping {
                if !key.is_string() {
                    let kind = if key.is_bool() {
                        "boolean"
                    } else if key.is_number() {
                        "number"
                    } else {
                        "null"
                    };
                    let rendered = serde_yaml::to_string(key).unwrap_or_default();
                    return Err(format!(
                        "invalid_case: YAML mapping keys must be strings; `{}` reads as a {kind}, quote it",
                        rendered.trim()
                    ));
                }
                refuse_non_string_keys(value)?;
            }
            Ok(())
        }
        serde_yaml::Value::Sequence(items) => items.iter().try_for_each(refuse_non_string_keys),
        _ => Ok(()),
    }
}

pub(crate) fn yaml<T: serde::de::DeserializeOwned>(body: &str, section: &str) -> Result<T, String> {
    validate_yaml_tokens(body)?;
    let invalid = |error: serde_yaml::Error| format!("invalid_case: invalid {section}: {error}");
    let value: serde_yaml::Value = serde_yaml::from_str(body).map_err(invalid)?;
    refuse_non_string_keys(&value)?;
    serde_yaml::from_str(body).map_err(invalid)
}
