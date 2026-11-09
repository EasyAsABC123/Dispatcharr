//! Python html.unescape used by original Django multipart filename parsing.
//! Original Dispatcharr contributors retain application credit.
use serde_json::Value;
use std::sync::OnceLock;
fn data() -> &'static Value {
    static TABLE: OnceLock<Value> = OnceLock::new();
    TABLE.get_or_init(|| {
        serde_json::from_str(include_str!("../datasets/source-html-charrefs.json"))
            .expect("original HTML tables")
    })
}
pub(crate) fn unescape(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(index) = rest.find('&') {
        result.push_str(&rest[..index]);
        rest = &rest[index + 1..];
        if let Some(numeric) = rest.strip_prefix('#') {
            let (digits, radix, offset) = if numeric.starts_with(['x', 'X']) {
                (&numeric[1..], 16, 2)
            } else {
                (numeric, 10, 1)
            };
            let count = digits
                .bytes()
                .take_while(|v| {
                    if radix == 16 {
                        v.is_ascii_hexdigit()
                    } else {
                        v.is_ascii_digit()
                    }
                })
                .count();
            if count > 0 {
                let raw = digits[..count].trim_start_matches('0');
                let number =
                    u32::from_str_radix(if raw.is_empty() { "0" } else { raw }, radix).ok();
                let consumed = offset + count + usize::from(digits[count..].starts_with(';'));
                rest = &rest[consumed..];
                if let Some(number) = number {
                    if let Some(replacement) = data()["invalid_charrefs"]
                        .get(number.to_string())
                        .and_then(Value::as_str)
                    {
                        result.push_str(replacement);
                    } else if number > 0x10ffff || (0xd800..=0xdfff).contains(&number) {
                        result.push('\u{fffd}');
                    } else if !data()["invalid_codepoints"]
                        .as_array()
                        .expect("invalid codepoints")
                        .iter()
                        .any(|v| v.as_u64() == Some(u64::from(number)))
                    {
                        if let Some(character) = char::from_u32(number) {
                            result.push(character);
                        }
                    }
                } else {
                    result.push('\u{fffd}');
                }
                continue;
            }
        }
        let mut end = 0;
        for character in rest.chars().take(32) {
            if matches!(
                character,
                '\t' | '\n' | '\x0c' | ' ' | '<' | '&' | '#' | ';'
            ) {
                break;
            }
            end += character.len_utf8();
        }
        if end > 0 {
            if rest[end..].starts_with(';') {
                end += 1;
            }
            let candidate = &rest[..end];
            let mut replaced = false;
            for split in candidate
                .char_indices()
                .map(|(i, _)| i)
                .chain(std::iter::once(candidate.len()))
                .rev()
            {
                if split == 0 {
                    continue;
                }
                if let Some(replacement) = data()["entities"]
                    .get(&candidate[..split])
                    .and_then(Value::as_str)
                {
                    result.push_str(replacement);
                    result.push_str(&candidate[split..]);
                    replaced = true;
                    break;
                }
            }
            if !replaced {
                result.push('&');
                result.push_str(candidate);
            }
            rest = &rest[end..];
        } else {
            result.push('&');
        }
    }
    result.push_str(rest);
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_named_numeric_and_invalid_references() {
        assert_eq!(
            unescape("rust-probe&#45;upload.png"),
            "rust-probe-upload.png"
        );
        assert_eq!(unescape("A&amp;B&notit;&unknown;"), "A&B¬it;&unknown;");
        assert_eq!(unescape("&#x80;&#0;&#xD800;&#x110000;&#x1f;"), "€���");
        assert_eq!(unescape("&NewLine;&sol;&bsol;"), "\n/\\");
    }
}
