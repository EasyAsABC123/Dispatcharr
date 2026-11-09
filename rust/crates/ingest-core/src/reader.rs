//! Bounded on-disk playlist reader; mirrors original UTF-8 errors="ignore".
use super::{Entry, Error, Limits, Parser};
use std::{
    io::{self, BufRead},
    str,
};

#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    Parser(Error),
}
/// Streaming iterator over a buffered byte source. Neither the raw line buffer
/// nor the parser's pending entry can grow with the total playlist size.
pub struct Entries<R> {
    reader: R,
    parser: Parser,
    line: Vec<u8>,
    line_limit: usize,
    finished: bool,
}
impl<R: BufRead> Entries<R> {
    pub fn new(reader: R, limits: Limits) -> Result<Self, Error> {
        let line_limit = limits.max_line_bytes;
        Ok(Self {
            reader,
            parser: Parser::new(limits)?,
            line: Vec::new(),
            line_limit,
            finished: false,
        })
    }
    fn read_line(&mut self) -> Result<bool, ReadError> {
        self.line.clear();
        loop {
            let buffer = self.reader.fill_buf().map_err(ReadError::Io)?;
            if buffer.is_empty() {
                return Ok(!self.line.is_empty());
            }
            // Python text files use universal newlines: CR, LF, CRLF all end a
            // logical line. An extra empty CRLF half is ignored by the parser.
            let delimiter = buffer
                .iter()
                .position(|byte| *byte == b'\r' || *byte == b'\n');
            let count = delimiter.unwrap_or(buffer.len());
            if count > self.line_limit.saturating_sub(self.line.len()) {
                return Err(ReadError::Parser(Error::LineCapacity));
            }
            self.line.extend_from_slice(&buffer[..count]);
            self.reader
                .consume(count + usize::from(delimiter.is_some()));
            if delimiter.is_some() {
                return Ok(true);
            }
        }
    }
}
impl<R: BufRead> Iterator for Entries<R> {
    type Item = Result<Entry, ReadError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            match self.read_line() {
                Ok(false) => {
                    self.finished = true;
                    self.parser.finish();
                    return None;
                }
                Err(error) => {
                    self.finished = true;
                    self.parser.finish();
                    return Some(Err(error));
                }
                Ok(true) => {}
            }
            let line = decode_ignoring_invalid_utf8(&self.line);
            match self.parser.feed(&line) {
                Ok(Some(entry)) => return Some(Ok(entry)),
                Ok(None) => {}
                Err(error) => {
                    self.finished = true;
                    return Some(Err(ReadError::Parser(error)));
                }
            }
        }
    }
}
pub(crate) fn decode_ignoring_invalid_utf8(mut bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len());
    loop {
        match str::from_utf8(bytes) {
            Ok(valid) => {
                text.push_str(valid);
                break;
            }
            Err(error) => {
                let valid = error.valid_up_to();
                // The prefix was checked by from_utf8; use its safe conversion.
                text.push_str(str::from_utf8(&bytes[..valid]).unwrap());
                match error.error_len() {
                    Some(invalid) => bytes = &bytes[valid + invalid..],
                    None => break,
                }
            }
        }
    }
    text
}
