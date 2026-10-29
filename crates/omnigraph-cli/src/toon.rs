//! TOON encoder for the JSON data model.
//!
//! Targets specification 4.4 (https://github.com/toon-format/spec), comma
//! delimiter and a two-space indent unless a caller passes other options.
//! The input is a [`serde_json::Value`]: integers stay decimal, finite floats
//! use the shortest round trip and then the specification's canonical range,
//! and a non-finite float would become `null`. Object key order is the order
//! the value already has. The result has no trailing newline.

use serde_json::{Map, Number, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Delimiter {
    Comma,
    /// Covered by the specification suite. The CLI emits [`Self::Comma`].
    #[cfg_attr(not(test), allow(dead_code))]
    Tab,
    /// Covered by the specification suite. The CLI emits [`Self::Comma`].
    #[cfg_attr(not(test), allow(dead_code))]
    Pipe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EncodeOptions {
    pub(crate) indent: usize,
    pub(crate) delimiter: Delimiter,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            indent: 2,
            delimiter: Delimiter::Comma,
        }
    }
}

pub(crate) fn encode(value: &Value) -> String {
    encode_with(value, &EncodeOptions::default())
}

pub(crate) fn encode_with(value: &Value, options: &EncodeOptions) -> String {
    let mut encoder = Encoder {
        indent: options.indent.max(1),
        delimiter: options.delimiter,
        lines: Vec::new(),
    };
    encoder.write_root(value);
    encoder.lines.join("\n")
}

struct Col {
    name: String,
    nested: Option<Vec<Col>>,
}

struct Encoder {
    indent: usize,
    delimiter: Delimiter,
    lines: Vec<String>,
}

impl Encoder {
    fn write_root(&mut self, value: &Value) {
        match value {
            Value::Array(values) => self.write_root_array(values),
            Value::Object(object) if object.is_empty() => {}
            Value::Object(object) => {
                if let Some(cols) = keyed_columns(object) {
                    self.write_keyed(object, &cols, "", 0, 1);
                } else {
                    self.write_object_fields(object, 0);
                }
            }
            other => self.line(0, &self.primitive(other, true)),
        }
    }

    fn write_root_array(&mut self, values: &[Value]) {
        if values.is_empty() {
            self.line(0, "[]");
            return;
        }
        if let Some(inline) = self.inline_primitives(values) {
            let header = self.open_header(values.len(), false, None);
            self.line(0, &format!("{header} {inline}"));
            return;
        }
        if let Some(cols) = tabular_columns(values) {
            let header = self.open_header(values.len(), false, Some(&cols));
            self.line(0, &header);
            for value in values {
                let row = self.row_cells(object_of(value), &cols);
                self.line(1, &row);
            }
            return;
        }
        let header = self.open_header(values.len(), false, None);
        self.line(0, &header);
        for value in values {
            self.write_list_item(value, 1);
        }
    }

    fn write_object_fields(&mut self, object: &Map<String, Value>, depth: usize) {
        for (key, value) in object {
            self.write_field("", key, value, depth, depth + 1);
        }
    }

    /// `nested_depth` is where a scope this field opens places its content.
    /// A list item's first field passes the hyphen depth as `line_depth` and
    /// two levels down as `nested_depth` (specification §10).
    fn write_field(
        &mut self,
        prefix: &str,
        key: &str,
        value: &Value,
        line_depth: usize,
        nested_depth: usize,
    ) {
        let key_tok = self.encode_key(key);
        match value {
            Value::Array(values) if values.is_empty() => {
                self.line(line_depth, &format!("{prefix}{key_tok}: []"));
            }
            Value::Array(values) => {
                self.write_field_array(prefix, &key_tok, values, line_depth, nested_depth);
            }
            Value::Object(object) if object.is_empty() => {
                self.line(line_depth, &format!("{prefix}{key_tok}:"));
            }
            Value::Object(object) => {
                if let Some(cols) = keyed_columns(object) {
                    let header = self.open_header(object.len(), true, Some(&cols));
                    self.line(line_depth, &format!("{prefix}{key_tok}{header}"));
                    self.write_keyed_rows(object, &cols, nested_depth);
                } else {
                    self.line(line_depth, &format!("{prefix}{key_tok}:"));
                    self.write_object_fields(object, nested_depth);
                }
            }
            other => {
                let rendered = self.primitive(other, false);
                self.line(line_depth, &format!("{prefix}{key_tok}: {rendered}"));
            }
        }
    }

    fn write_field_array(
        &mut self,
        prefix: &str,
        key_tok: &str,
        values: &[Value],
        line_depth: usize,
        nested_depth: usize,
    ) {
        if let Some(inline) = self.inline_primitives(values) {
            let header = self.open_header(values.len(), false, None);
            self.line(line_depth, &format!("{prefix}{key_tok}{header} {inline}"));
            return;
        }
        if let Some(cols) = tabular_columns(values) {
            let header = self.open_header(values.len(), false, Some(&cols));
            self.line(line_depth, &format!("{prefix}{key_tok}{header}"));
            for value in values {
                let row = self.row_cells(object_of(value), &cols);
                self.line(nested_depth, &row);
            }
            return;
        }
        let header = self.open_header(values.len(), false, None);
        self.line(line_depth, &format!("{prefix}{key_tok}{header}"));
        for value in values {
            self.write_list_item(value, nested_depth);
        }
    }

    fn write_keyed(
        &mut self,
        object: &Map<String, Value>,
        cols: &[Col],
        prefix: &str,
        line_depth: usize,
        row_depth: usize,
    ) {
        let header = self.open_header(object.len(), true, Some(cols));
        self.line(line_depth, &format!("{prefix}{header}"));
        self.write_keyed_rows(object, cols, row_depth);
    }

    fn write_keyed_rows(&mut self, object: &Map<String, Value>, cols: &[Col], row_depth: usize) {
        for (entry_key, entry_value) in object {
            let cells = self.row_cells(object_of(entry_value), cols);
            let entry_key = self.encode_key(entry_key);
            self.line(row_depth, &format!("{entry_key}: {cells}"));
        }
    }

    /// An array element. Tabular and keyed-tabular headers need a key or the
    /// document root, so an element uses list form (specification §6, §9.4, §10).
    fn write_list_item(&mut self, value: &Value, depth: usize) {
        match value {
            Value::Array(values) if values.is_empty() => self.line(depth, "- [0]:"),
            Value::Array(values) => {
                if let Some(inline) = self.inline_primitives(values) {
                    let header = self.open_header(values.len(), false, None);
                    self.line(depth, &format!("- {header} {inline}"));
                } else {
                    let header = self.open_header(values.len(), false, None);
                    self.line(depth, &format!("- {header}"));
                    for value in values {
                        self.write_list_item(value, depth + 1);
                    }
                }
            }
            Value::Object(object) if object.is_empty() => self.line(depth, "-"),
            Value::Object(object) => {
                let mut fields = object.iter();
                let (key, value) = fields.next().expect("a non-empty object has a field");
                self.write_field("- ", key, value, depth, depth + 2);
                for (key, value) in fields {
                    self.write_field("", key, value, depth + 1, depth + 2);
                }
            }
            other => {
                let rendered = self.primitive(other, false);
                self.line(depth, &format!("- {rendered}"));
            }
        }
    }

    fn inline_primitives(&self, values: &[Value]) -> Option<String> {
        if values.is_empty() || values.iter().any(|value| !is_primitive(value)) {
            return None;
        }
        Some(
            values
                .iter()
                .map(|value| self.primitive(value, false))
                .collect::<Vec<_>>()
                .join(self.sep()),
        )
    }

    fn row_cells(&self, object: &Map<String, Value>, cols: &[Col]) -> String {
        let mut cells = Vec::new();
        self.collect_leaves(object, cols, &mut cells);
        cells.join(self.sep())
    }

    fn collect_leaves(&self, object: &Map<String, Value>, cols: &[Col], cells: &mut Vec<String>) {
        for col in cols {
            let value = &object[&col.name];
            match &col.nested {
                None => cells.push(self.primitive(value, false)),
                Some(nested) => self.collect_leaves(object_of(value), nested, cells),
            }
        }
    }

    fn open_header(&self, len: usize, keyed: bool, fields: Option<&[Col]>) -> String {
        let mut header = self.bracket(len, keyed);
        if let Some(fields) = fields {
            header.push('{');
            header.push_str(&self.header_fields(fields));
            header.push('}');
        }
        header.push(':');
        header
    }

    fn header_fields(&self, cols: &[Col]) -> String {
        cols.iter()
            .map(|col| self.field_entry(col))
            .collect::<Vec<_>>()
            .join(self.sep())
    }

    fn field_entry(&self, col: &Col) -> String {
        let name = self.encode_key(&col.name);
        match &col.nested {
            None => name,
            Some(nested) => format!("{name}{{{}}}", self.header_fields(nested)),
        }
    }

    fn bracket(&self, len: usize, keyed: bool) -> String {
        let symbol = match self.delimiter {
            Delimiter::Comma => "",
            Delimiter::Tab => "\t",
            Delimiter::Pipe => "|",
        };
        if keyed {
            format!("[{len}:{symbol}]")
        } else {
            format!("[{len}{symbol}]")
        }
    }

    fn sep(&self) -> &str {
        match self.delimiter {
            Delimiter::Comma => ",",
            Delimiter::Tab => "\t",
            Delimiter::Pipe => "|",
        }
    }

    fn delimiter_char(&self) -> char {
        match self.delimiter {
            Delimiter::Comma => ',',
            Delimiter::Tab => '\t',
            Delimiter::Pipe => '|',
        }
    }

    fn primitive(&self, value: &Value, root: bool) -> String {
        match value {
            Value::Null => "null".to_owned(),
            Value::Bool(true) => "true".to_owned(),
            Value::Bool(false) => "false".to_owned(),
            Value::Number(number) => canonical_number(number),
            Value::String(text) => self.encode_string(text, root),
            Value::Array(_) | Value::Object(_) => {
                unreachable!("a primitive slot does not hold a container")
            }
        }
    }

    fn encode_string(&self, value: &str, root: bool) -> String {
        if must_quote(value, root, self.delimiter_char()) {
            quote(value)
        } else {
            value.to_owned()
        }
    }

    fn encode_key(&self, key: &str) -> String {
        if is_unquoted_key(key) {
            key.to_owned()
        } else {
            quote(key)
        }
    }

    fn line(&mut self, depth: usize, text: &str) {
        let mut line = String::with_capacity(depth * self.indent + text.len());
        line.extend(std::iter::repeat_n(' ', depth * self.indent));
        line.push_str(text);
        self.lines.push(line);
    }
}

fn object_of(value: &Value) -> &Map<String, Value> {
    match value {
        Value::Object(object) => object,
        _ => unreachable!("tabular and keyed forms only walk objects"),
    }
}

fn is_primitive(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn tabular_columns(values: &[Value]) -> Option<Vec<Col>> {
    let mut objects = Vec::with_capacity(values.len());
    for value in values {
        let Value::Object(object) = value else {
            return None;
        };
        objects.push(object);
    }
    columns_of(&objects)
}

fn keyed_columns(object: &Map<String, Value>) -> Option<Vec<Col>> {
    if object.len() < 2 {
        return None;
    }
    let objects: Vec<&Map<String, Value>> = object
        .values()
        .map(|value| match value {
            Value::Object(nested) => Some(nested),
            _ => None,
        })
        .collect::<Option<_>>()?;
    columns_of(&objects)
}

fn columns_of(objects: &[&Map<String, Value>]) -> Option<Vec<Col>> {
    if objects.is_empty()
        || objects.iter().any(|object| object.is_empty())
        || !same_key_set(objects)
    {
        return None;
    }
    let mut cols = Vec::with_capacity(objects[0].len());
    for key in objects[0].keys() {
        let values: Vec<&Value> = objects.iter().map(|object| &object[key]).collect();
        cols.push(Col {
            name: key.clone(),
            nested: column_nested(&values)?,
        });
    }
    Some(cols)
}

fn same_key_set(objects: &[&Map<String, Value>]) -> bool {
    let first = objects[0];
    objects.iter().all(|object| {
        object.len() == first.len() && object.keys().all(|key| first.contains_key(key))
    })
}

/// `Some(None)` is a primitive column. `Some(Some(cols))` is nested-uniform.
/// `None` disqualifies the array or keyed object.
fn column_nested(values: &[&Value]) -> Option<Option<Vec<Col>>> {
    if values.iter().copied().all(is_primitive) {
        return Some(None);
    }
    let mut objects = Vec::with_capacity(values.len());
    for value in values {
        let Value::Object(object) = value else {
            return None;
        };
        objects.push(object);
    }
    Some(Some(columns_of(&objects)?))
}

fn canonical_number(number: &Number) -> String {
    if let Some(value) = number.as_i64() {
        return value.to_string();
    }
    if let Some(value) = number.as_u64() {
        return value.to_string();
    }
    let Some(value) = number.as_f64() else {
        return "null".to_owned();
    };
    if !value.is_finite() || value == 0.0 {
        return if value.is_finite() {
            "0".to_owned()
        } else {
            "null".to_owned()
        };
    }
    let raw = number.to_string();
    let abs = value.abs();
    if (1e-6..1e21).contains(&abs) {
        expand_plain(&raw)
    } else {
        exponent_form(&raw)
    }
}

fn expand_plain(raw: &str) -> String {
    let (negative, body) = split_sign(raw);
    let Some((mantissa, exponent)) = split_exponent(body) else {
        return trim_fraction(raw);
    };
    let Ok(exponent) = exponent.parse::<i32>() else {
        return raw.to_owned();
    };
    let (digits, frac_len) = mantissa_digits(mantissa);
    let plain = place_point(&digits, exponent - frac_len as i32);
    if negative { format!("-{plain}") } else { plain }
}

fn exponent_form(raw: &str) -> String {
    let (negative, body) = split_sign(raw);
    let sign = if negative { "-" } else { "" };
    if let Some((mantissa, exponent)) = split_exponent(body) {
        let exponent_sign = if exponent.starts_with('-') { "-" } else { "+" };
        let digits = exponent.trim_start_matches(['+', '-']);
        return format!("{sign}{mantissa}e{exponent_sign}{digits}");
    }
    let (whole, frac) = match body.split_once('.') {
        Some((whole, frac)) => (whole, frac),
        None => (body, ""),
    };
    let mut digits = String::new();
    digits.push_str(whole);
    digits.push_str(frac);
    let point = whole.len();
    let Some(first) = digits.find(|c: char| c != '0') else {
        return "0".to_owned();
    };
    let last = digits.rfind(|c: char| c != '0').unwrap_or(first);
    let significant = &digits[first..=last];
    let exp = point as i32 - first as i32 - 1;
    let exponent_sign = if exp < 0 { "-" } else { "+" };
    let mut significand = String::new();
    significand.push(significant.chars().next().expect("a significant digit"));
    let rest: String = significant.chars().skip(1).collect();
    if !rest.is_empty() {
        significand.push('.');
        significand.push_str(&rest);
    }
    format!("{sign}{significand}e{exponent_sign}{}", exp.unsigned_abs())
}

fn split_sign(raw: &str) -> (bool, &str) {
    match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw),
    }
}

fn split_exponent(body: &str) -> Option<(&str, &str)> {
    let index = body.find(['e', 'E'])?;
    Some((&body[..index], &body[index + 1..]))
}

fn mantissa_digits(mantissa: &str) -> (String, usize) {
    let (whole, frac) = match mantissa.split_once('.') {
        Some((whole, frac)) => (whole, frac),
        None => (mantissa, ""),
    };
    let mut digits = String::new();
    digits.push_str(whole);
    digits.push_str(frac);
    let frac_len = frac.len();
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        ("0".to_owned(), frac_len)
    } else if trimmed.len() == digits.len() {
        (digits, frac_len)
    } else {
        let stripped = digits.len() - trimmed.len();
        let frac_len = frac_len.saturating_sub(stripped);
        (trimmed.to_owned(), frac_len)
    }
}

fn place_point(digits: &str, shift: i32) -> String {
    if shift >= 0 {
        let mut out = digits.to_owned();
        for _ in 0..shift {
            out.push('0');
        }
        return out;
    }
    let places = shift.unsigned_abs() as usize;
    if places < digits.len() {
        let point = digits.len() - places;
        let mut out = String::new();
        out.push_str(&digits[..point]);
        out.push('.');
        out.push_str(&digits[point..]);
        trim_fraction(&out)
    } else {
        let mut out = String::from("0.");
        out.extend(std::iter::repeat_n('0', places - digits.len()));
        out.push_str(digits);
        trim_fraction(&out)
    }
}

fn trim_fraction(value: &str) -> String {
    let Some((whole, frac)) = value.split_once('.') else {
        return value.to_owned();
    };
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        whole.to_owned()
    } else {
        format!("{whole}.{frac}")
    }
}

fn must_quote(value: &str, root: bool, delimiter: char) -> bool {
    if value.is_empty()
        || value == "true"
        || value == "false"
        || value == "null"
        || is_numeric_like(value)
        || value.starts_with(['-', '#'])
        || (root && value.starts_with('\u{FEFF}'))
    {
        return true;
    }
    let first = value.chars().next();
    let last = value.chars().next_back();
    if matches!(first, Some(' ' | '\t')) || matches!(last, Some(' ' | '\t')) {
        return true;
    }
    value.chars().any(|c| {
        matches!(c, ':' | '"' | '\\' | '[' | ']' | '{' | '}') || (c as u32) < 0x20 || c == delimiter
    })
}

fn is_numeric_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    if index < bytes.len() && (bytes[index] == b'+' || bytes[index] == b'-') {
        index += 1;
    }
    if index >= bytes.len() || !bytes[index].is_ascii_digit() {
        return false;
    }
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    if index < bytes.len() && bytes[index] == b'.' {
        index += 1;
        if index >= bytes.len() || !bytes[index].is_ascii_digit() {
            return false;
        }
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
    }
    if index < bytes.len() && (bytes[index] == b'e' || bytes[index] == b'E') {
        index += 1;
        if index < bytes.len() && (bytes[index] == b'+' || bytes[index] == b'-') {
            index += 1;
        }
        if index >= bytes.len() || !bytes[index].is_ascii_digit() {
            return false;
        }
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
    }
    index == bytes.len()
}

fn is_unquoted_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{code:04x}", code = u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn parsed(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn canonical_numbers_follow_the_specification_range() {
        assert_eq!(encode(&parsed("0")), "0");
        assert_eq!(encode(&parsed("-0")), "0");
        assert_eq!(encode(&parsed("1e6")), "1000000");
        assert_eq!(encode(&parsed("0.000001")), "0.000001");
        assert_eq!(encode(&parsed("1.0")), "1");
        assert_eq!(encode(&parsed("1e21")), "1e+21");
        assert_eq!(encode(&parsed("1e-7")), "1e-7");
        assert_eq!(encode(&parsed("-1e-7")), "-1e-7");
    }

    /// Encode fixtures from the TOON specification 4.4 suite
    /// (`tests/fixtures/encode` in https://github.com/toon-format/spec), MIT.
    #[test]
    fn specification_encode_fixtures() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/toon/encode");
        let mut files: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        files.sort();
        assert!(!files.is_empty());
        for path in files {
            let file: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
            for (index, test) in file["tests"].as_array().unwrap().iter().enumerate() {
                let options = fixture_options(test.get("options"));
                let encoded = encode_with(&test["input"], &options);
                let expected = test["expected"].as_str().unwrap();
                assert_eq!(
                    encoded,
                    expected,
                    "{}#{index} {}",
                    path.file_name().unwrap().to_string_lossy(),
                    test["name"].as_str().unwrap_or("")
                );
            }
        }
    }

    fn fixture_options(options: Option<&Value>) -> EncodeOptions {
        let Some(options) = options.filter(|value| !value.is_null()) else {
            return EncodeOptions::default();
        };
        let mut parsed = EncodeOptions::default();
        let object = options.as_object().unwrap();
        for (key, value) in object {
            match key.as_str() {
                "indentSize" => {
                    parsed.indent = usize::try_from(value.as_u64().unwrap()).unwrap();
                }
                "delimiter" => {
                    parsed.delimiter = match value.as_str().unwrap() {
                        "," => Delimiter::Comma,
                        "\t" => Delimiter::Tab,
                        "|" => Delimiter::Pipe,
                        other => panic!("unknown delimiter {other:?}"),
                    };
                }
                other => panic!("unknown encode option {other}"),
            }
        }
        parsed
    }
}
