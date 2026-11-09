//! Bounded source Python str(JSON collection) for dismissal CharField values.
//! Application credit remains with original Dispatcharr authors/contributors.
use serde_json::Value;
use std::sync::OnceLock;
pub(crate) fn printable(c: char) -> bool {
    static RANGES: OnceLock<Vec<(u32, u32)>> = OnceLock::new();
    let ranges = RANGES.get_or_init(|| {
        let data: Value =
            serde_json::from_str(include_str!("../datasets/python-printability.json"))
                .expect("pinned Python Unicode dataset");
        data["nonprintable_ranges"]
            .as_array()
            .expect("ranges")
            .iter()
            .map(|pair| {
                (
                    pair[0].as_u64().expect("start") as u32,
                    pair[1].as_u64().expect("end") as u32,
                )
            })
            .collect()
    });
    let code = c as u32;
    let index = ranges.partition_point(|(_, end)| *end < code);
    ranges.get(index).is_none_or(|(start, _)| *start > code)
}
fn quoted(value: &str, out: &mut String, max: usize) -> Option<()> {
    let quote = if value.contains('\'') && !value.contains('"') {
        '"'
    } else {
        '\''
    };
    out.push(quote);
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c if c == quote => {
                out.push('\\');
                out.push(c)
            }
            c if !printable(c) => {
                let code = c as u32;
                out.push_str(&if code <= 0xff {
                    format!("\\x{code:02x}")
                } else if code <= 0xffff {
                    format!("\\u{code:04x}")
                } else {
                    format!("\\U{code:08x}")
                })
            }
            c => out.push(c),
        }
        if out.len() > max.checked_mul(4)? {
            return None;
        }
    }
    out.push(quote);
    Some(())
}
fn render(value: &Value, out: &mut String, max: usize, depth: usize) -> Option<()> {
    if depth > 64 || out.len() > max.checked_mul(4)? {
        return None;
    }
    match value {
        Value::Null => out.push_str("None"),
        Value::Bool(v) => out.push_str(if *v { "True" } else { "False" }),
        Value::Number(v) => out.push_str(&if v.is_f64() {
            crate::json_compat::float_string(v.as_f64()?)
        } else {
            v.to_string()
        }),
        Value::String(v) => quoted(v, out, max)?,
        Value::Array(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ")
                }
                render(value, out, max, depth + 1)?
            }
            out.push(']')
        }
        Value::Object(values) => {
            out.push('{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ")
                }
                quoted(key, out, max)?;
                out.push_str(": ");
                render(value, out, max, depth + 1)?
            }
            out.push('}')
        }
    }
    if out.len() > max.checked_mul(4)? {
        return None;
    }
    Some(())
}
pub(crate) fn collection_text(value: &Value, max: usize) -> Option<String> {
    let mut out = String::new();
    render(value, &mut out, max, 0)?;
    if out.chars().count() > max {
        return None;
    }
    Some(out)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_python_collection_vectors_and_limit() {
        let data: Vec<Value> = serde_json::from_str(include_str!(
            "../datasets/python-action-string-vectors.json"
        ))
        .unwrap();
        for item in data {
            assert_eq!(
                collection_text(&item["value"], 1024),
                item["text"].as_str().map(str::to_owned)
            );
            assert_eq!(
                collection_text(&item["value"], 50).is_some(),
                item["text"].as_str().unwrap().chars().count() <= 50
            );
        }
    }
}
