//! Bounded JSONResponse/DRF formatting for combined statistics.
//! Original Dispatcharr/Django/DRF authors retain all format/algorithm credit.
use serde_json::Value;
use std::io::{self, Write};

pub(super) struct Output {
    data: Vec<u8>,
    maximum: usize,
}
impl Output {
    pub(super) fn new(maximum: usize) -> Self {
        Self {
            data: Vec::new(),
            maximum,
        }
    }
    pub(super) fn finish(self) -> Vec<u8> {
        self.data
    }
    pub(super) fn text(&mut self, value: &str) -> Result<(), ()> {
        self.write_all(value.as_bytes()).map_err(|_| ())
    }
}
impl Write for Output {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() > self.maximum.saturating_sub(self.data.len()) {
            return Err(io::Error::other("combined statistics output capacity"));
        }
        self.data.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[derive(Clone, Copy)]
pub(super) enum Format {
    Django,
    Drf(Option<usize>),
}
fn quoted(output: &mut Output, value: &str, ascii: bool) -> Result<(), ()> {
    output.text("\"")?;
    for c in value.chars() {
        match c {
            '"' => output.text("\\\"")?,
            '\\' => output.text("\\\\")?,
            '\n' => output.text("\\n")?,
            '\r' => output.text("\\r")?,
            '\t' => output.text("\\t")?,
            '\u{8}' => output.text("\\b")?,
            '\u{c}' => output.text("\\f")?,
            c if c < ' ' || (ascii && !c.is_ascii()) => {
                let mut words = [0u16; 2];
                for word in c.encode_utf16(&mut words) {
                    output.text(&format!("\\u{word:04x}"))?;
                }
            }
            c => {
                let mut bytes = [0u8; 4];
                output.text(c.encode_utf8(&mut bytes))?;
            }
        }
    }
    output.text("\"")
}
fn newline(output: &mut Output, indent: usize, depth: usize) -> Result<(), ()> {
    output.text("\n")?;
    for _ in 0..depth {
        output.text(&"        "[..indent])?;
    }
    Ok(())
}
fn value(output: &mut Output, input: &Value, format: Format, depth: usize) -> Result<(), ()> {
    if depth > 64 {
        return Err(());
    }
    let indent = match format {
        Format::Drf(Some(indent)) if indent > 0 => Some(indent),
        _ => None,
    };
    let spaced = matches!(format, Format::Django);
    match input {
        Value::Null => output.text("null"),
        Value::Bool(true) => output.text("true"),
        Value::Bool(false) => output.text("false"),
        Value::Number(n) => output.text(&n.to_string()),
        Value::String(s) => quoted(output, s, spaced),
        Value::Array(values) => {
            output.text("[")?;
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    output.text(if spaced { ", " } else { "," })?;
                }
                if let Some(indent) = indent {
                    newline(output, indent, depth + 1)?;
                }
                value(output, item, format, depth + 1)?;
            }
            if !values.is_empty() {
                if let Some(indent) = indent {
                    newline(output, indent, depth)?;
                }
            }
            output.text("]")
        }
        Value::Object(values) => {
            output.text("{")?;
            for (index, (key, item)) in values.iter().enumerate() {
                if index > 0 {
                    output.text(if spaced { ", " } else { "," })?;
                }
                if let Some(indent) = indent {
                    newline(output, indent, depth + 1)?;
                }
                quoted(output, key, spaced)?;
                output.text(if spaced || indent.is_some() {
                    ": "
                } else {
                    ":"
                })?;
                value(output, item, format, depth + 1)?;
            }
            if !values.is_empty() {
                if let Some(indent) = indent {
                    newline(output, indent, depth)?;
                }
            }
            output.text("}")
        }
    }
}
pub(super) fn json(input: &Value, format: Format, maximum: usize) -> Result<Vec<u8>, ()> {
    let mut output = Output::new(maximum);
    value(&mut output, input, format, 0)?;
    Ok(output.finish())
}
pub(super) fn escape(output: &mut Output, input: &str) -> Result<(), ()> {
    for c in input.chars() {
        match c {
            '&' => output.text("&amp;")?,
            '<' => output.text("&lt;")?,
            '>' => output.text("&gt;")?,
            '"' => output.text("&quot;")?,
            '\'' => output.text("&#x27;")?,
            c => {
                let mut bytes = [0u8; 4];
                output.text(c.encode_utf8(&mut bytes))?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn django_ascii_and_drf_unicode_have_independent_source_formats() {
        let input = json!({"x":"Å🙂\n","empty":[],"n":3});
        assert_eq!(
            String::from_utf8(json(&input, Format::Django, 1024).unwrap()).unwrap(),
            "{\"x\": \"\\u00c5\\ud83d\\ude42\\n\", \"empty\": [], \"n\": 3}"
        );
        assert_eq!(
            String::from_utf8(json(&input, Format::Drf(None), 1024).unwrap()).unwrap(),
            "{\"x\":\"Å🙂\\n\",\"empty\":[],\"n\":3}"
        );
        assert!(json(&input, Format::Django, 8).is_err());
    }
    #[test]
    fn pretty_width_and_depth_are_bounded() {
        let input = json!({"a":[1]});
        assert_eq!(
            String::from_utf8(json(&input, Format::Drf(Some(4)), 1024).unwrap()).unwrap(),
            "{\n    \"a\": [\n        1\n    ]\n}"
        );
        let mut nested = Value::Null;
        for _ in 0..66 {
            nested = json!([nested]);
        }
        assert!(json(&nested, Format::Django, 4096).is_err());
    }
}
