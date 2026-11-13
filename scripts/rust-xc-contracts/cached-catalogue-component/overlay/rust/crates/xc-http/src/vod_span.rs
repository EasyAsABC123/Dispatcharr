//! Internal borrowed JSON lexical spans. No serde feature or Value tree.
//! Existing SQL text and scratch grants own every returned slice. Original
//! Dispatcharr authors retain application/algorithm credit.
use crate::vod_json::Error;
use std::borrow::Cow;

/// A validated span is represented by its original borrowed UTF-8 text. This
/// alias adds no reinterpretation to serde_json::Value or ordinary Store APIs.
pub(crate) type RawValue = str;

struct Cursor<'a> {
    text: &'a str,
    at: usize,
}
impl<'a> Cursor<'a> {
    fn bytes(&self) -> &[u8] {
        self.text.as_bytes()
    }
    fn whitespace(&mut self) {
        while self
            .bytes()
            .get(self.at)
            .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.at += 1;
        }
    }
    fn byte(&mut self, wanted: u8) -> Result<(), Error> {
        self.whitespace();
        if self.bytes().get(self.at) != Some(&wanted) {
            return Err(Error::Syntax);
        }
        self.at += 1;
        Ok(())
    }
    fn string(&mut self) -> Result<&'a str, Error> {
        self.whitespace();
        let begin = self.at;
        if self.bytes().get(self.at) != Some(&b'"') {
            return Err(Error::Syntax);
        }
        self.at += 1;
        while let Some(&byte) = self.bytes().get(self.at) {
            match byte {
                b'"' => {
                    self.at += 1;
                    let value = &self.text[begin..self.at];
                    let _ = decode_string(value)?;
                    return Ok(value);
                }
                b'\\' => {
                    self.at += 2;
                    if self.at > self.text.len() {
                        return Err(Error::Syntax);
                    }
                }
                0..=31 => return Err(Error::Syntax),
                _ => self.at += 1,
            }
        }
        Err(Error::Syntax)
    }
    fn number(&mut self) -> Result<(), Error> {
        if self.bytes().get(self.at) == Some(&b'-') {
            self.at += 1;
        }
        match self.bytes().get(self.at) {
            Some(b'0') => self.at += 1,
            Some(b'1'..=b'9') => {
                self.at += 1;
                while self.bytes().get(self.at).is_some_and(u8::is_ascii_digit) {
                    self.at += 1;
                }
            }
            _ => return Err(Error::Syntax),
        }
        if self.bytes().get(self.at) == Some(&b'.') {
            self.at += 1;
            let start = self.at;
            while self.bytes().get(self.at).is_some_and(u8::is_ascii_digit) {
                self.at += 1;
            }
            if start == self.at {
                return Err(Error::Syntax);
            }
        }
        if self
            .bytes()
            .get(self.at)
            .is_some_and(|b| matches!(b, b'e' | b'E'))
        {
            self.at += 1;
            if self
                .bytes()
                .get(self.at)
                .is_some_and(|b| matches!(b, b'+' | b'-'))
            {
                self.at += 1;
            }
            let start = self.at;
            while self.bytes().get(self.at).is_some_and(u8::is_ascii_digit) {
                self.at += 1;
            }
            if start == self.at {
                return Err(Error::Syntax);
            }
        }
        Ok(())
    }
    fn value(&mut self, depth: usize) -> Result<&'a RawValue, Error> {
        if depth >= 128 {
            return Err(crate::reader::capacity().into());
        }
        self.whitespace();
        let begin = self.at;
        match self.bytes().get(self.at).copied() {
            Some(b'"') => {
                self.string()?;
            }
            Some(b'{') => {
                self.at += 1;
                self.whitespace();
                if self.bytes().get(self.at) == Some(&b'}') {
                    self.at += 1;
                } else {
                    loop {
                        self.string()?;
                        self.byte(b':')?;
                        self.value(depth + 1)?;
                        self.whitespace();
                        match self.bytes().get(self.at) {
                            Some(b'}') => {
                                self.at += 1;
                                break;
                            }
                            Some(b',') => self.at += 1,
                            _ => return Err(Error::Syntax),
                        }
                    }
                }
            }
            Some(b'[') => {
                self.at += 1;
                self.whitespace();
                if self.bytes().get(self.at) == Some(&b']') {
                    self.at += 1;
                } else {
                    loop {
                        self.value(depth + 1)?;
                        self.whitespace();
                        match self.bytes().get(self.at) {
                            Some(b']') => {
                                self.at += 1;
                                break;
                            }
                            Some(b',') => self.at += 1,
                            _ => return Err(Error::Syntax),
                        }
                    }
                }
            }
            Some(b'-' | b'0'..=b'9') => self.number()?,
            Some(b't' | b'f' | b'n') => {
                let word = match self.bytes()[self.at] {
                    b't' => "true",
                    b'f' => "false",
                    _ => "null",
                };
                if !self.text[self.at..].starts_with(word) {
                    return Err(Error::Syntax);
                }
                self.at += word.len();
            }
            _ => return Err(Error::Syntax),
        }
        Ok(&self.text[begin..self.at])
    }
}
/// Decode at most the current string. Unescaped keys/values stay borrowed;
/// escaped-string scratch is bounded by the already charged input text.
pub(crate) fn decode_string(text: &str) -> Result<Cow<'_, str>, Error> {
    if text.len() < 2 || !text.starts_with('"') || !text.ends_with('"') {
        return Err(Error::Syntax);
    }
    let inner = &text[1..text.len() - 1];
    if inner.contains('\\') {
        serde_json::from_str::<String>(text)
            .map(Cow::Owned)
            .map_err(|_| Error::Syntax)
    } else if inner.bytes().any(|b| b < 32 || b == b'"') {
        Err(Error::Syntax)
    } else {
        Ok(Cow::Borrowed(inner))
    }
}
pub(crate) fn parse(text: &str) -> Result<&RawValue, Error> {
    let mut cursor = Cursor { text, at: 0 };
    let value = cursor.value(0)?;
    cursor.whitespace();
    if cursor.at != text.len() {
        return Err(Error::Syntax);
    }
    Ok(value)
}

pub(crate) struct Object<'a> {
    cursor: Cursor<'a>,
    first: bool,
    done: bool,
}
impl<'a> Object<'a> {
    pub fn new(raw: &'a RawValue) -> Result<Self, Error> {
        let raw = parse(raw)?;
        if !raw.starts_with('{') {
            return Err(Error::SourceObject);
        }
        Ok(Self {
            cursor: Cursor { text: raw, at: 1 },
            first: true,
            done: false,
        })
    }
}
impl<'a> Iterator for Object<'a> {
    type Item = Result<(Cow<'a, str>, &'a RawValue), Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        self.cursor.whitespace();
        if self.cursor.bytes().get(self.cursor.at) == Some(&b'}') {
            self.done = true;
            return None;
        }
        let result = (|| {
            if !self.first {
                self.cursor.byte(b',')?;
            }
            self.first = false;
            let key = self.cursor.string()?;
            self.cursor.byte(b':')?;
            let value = self.cursor.value(0)?;
            Ok((decode_string(key)?, value))
        })();
        if result.is_err() {
            self.done = true;
        }
        Some(result)
    }
}
pub(crate) struct Array<'a> {
    cursor: Cursor<'a>,
    first: bool,
    done: bool,
}
impl<'a> Array<'a> {
    pub fn new(raw: &'a RawValue) -> Result<Self, Error> {
        let raw = parse(raw)?;
        if !raw.starts_with('[') {
            return Err(Error::SourceObject);
        }
        Ok(Self {
            cursor: Cursor { text: raw, at: 1 },
            first: true,
            done: false,
        })
    }
}
impl<'a> Iterator for Array<'a> {
    type Item = Result<&'a RawValue, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        self.cursor.whitespace();
        if self.cursor.bytes().get(self.cursor.at) == Some(&b']') {
            self.done = true;
            return None;
        }
        let result = (|| {
            if !self.first {
                self.cursor.byte(b',')?;
            }
            self.first = false;
            self.cursor.value(0)
        })();
        if result.is_err() {
            self.done = true;
        }
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn borrowed_nested_spans_preserve_literal_private_keys_and_order() {
        let input = r#" {"$serde_json::private::RawValue":"[1]","unknown":{"x":[true,null]},"escaped\u0020key":"東"} "#;
        let members = Object::new(parse(input).unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(members.len(), 3);
        assert_eq!(members[0].0, "$serde_json::private::RawValue");
        assert_eq!(members[0].1, r#""[1]""#);
        assert_eq!(members[1].1, r#"{"x":[true,null]}"#);
        assert_eq!(members[2].0, "escaped key");
        for (_, span) in &members {
            assert!(span.as_ptr() >= input.as_ptr());
            assert!(
                (span.as_ptr() as usize) + span.len() <= (input.as_ptr() as usize) + input.len()
            );
        }
    }
    #[test]
    fn invalid_grammar_and_unicode_fail_without_partial_success() {
        for input in [
            "",
            "[1,]",
            "{\"x\":1,}",
            "01",
            "-01",
            "1.",
            "1e+",
            "true false",
            "{\"x\" 1}",
            r#""\uD800""#,
            r#""\q""#,
            "[",
            "{",
            "\"x",
        ] {
            assert!(parse(input).is_err(), "{input}");
        }
        for input in [
            "null",
            "false",
            "0",
            "-0",
            "1e9999",
            "[{}]",
            r#""\uD83D\uDE00""#,
        ] {
            assert!(parse(input).is_ok(), "{input}");
        }
    }
    #[test]
    fn depth_refusal_and_large_unselected_number_do_not_build_values() {
        let deep = "[".repeat(129) + &"]".repeat(129);
        assert!(matches!(parse(&deep), Err(Error::Capacity(_))));
        let digits = "1".repeat(4301);
        assert_eq!(parse(&digits).unwrap(), digits);
        assert!(matches!(
            crate::vod_json::validate_source_integer_limits(&digits),
            Err(Error::SourceIntegerLimit)
        ));
        assert_eq!(
            Array::new("[0, [1], {\"x\":2}]")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            ["0", "[1]", "{\"x\":2}"]
        );
    }
}
