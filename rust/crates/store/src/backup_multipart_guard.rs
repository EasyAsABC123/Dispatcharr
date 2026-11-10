//! Bounded envelope preflight before Multer buffering; original credit retained.
//! This does not parse or rewrite multipart data. It limits headers and total bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GuardError {
    Headers,
    Body,
}
struct Matcher {
    bytes: Vec<u8>,
    prefix: Vec<usize>,
    matched: usize,
}
impl Matcher {
    fn new(bytes: Vec<u8>) -> Self {
        let mut prefix = vec![0; bytes.len()];
        for i in 1..bytes.len() {
            let mut j = prefix[i - 1];
            while j > 0 && bytes[i] != bytes[j] {
                j = prefix[j - 1]
            }
            if bytes[i] == bytes[j] {
                j += 1
            }
            prefix[i] = j
        }
        Self {
            bytes,
            prefix,
            matched: 0,
        }
    }
    fn advance(&mut self, byte: u8) -> bool {
        while self.matched > 0 && byte != self.bytes[self.matched] {
            self.matched = self.prefix[self.matched - 1]
        }
        if byte == self.bytes[self.matched] {
            self.matched += 1
        }
        if self.matched == self.bytes.len() {
            self.matched = self.prefix[self.matched - 1];
            true
        } else {
            false
        }
    }
}
#[derive(Clone, Copy)]
enum Phase {
    Initial,
    Body,
    Suffix(Option<u8>),
    Headers(usize, u32),
    Done,
}
pub(super) struct Guard {
    initial: Matcher,
    body: Matcher,
    phase: Phase,
    total: u64,
    limit: u64,
}
impl Guard {
    pub(super) fn new(boundary: &str, limit: u64) -> Self {
        Self {
            initial: Matcher::new(format!("--{boundary}").into_bytes()),
            body: Matcher::new(format!("\r\n--{boundary}").into_bytes()),
            phase: Phase::Initial,
            total: 0,
            limit,
        }
    }
    pub(super) fn inspect(&mut self, bytes: &[u8]) -> Result<(), GuardError> {
        self.total = self
            .total
            .checked_add(bytes.len() as u64)
            .ok_or(GuardError::Body)?;
        if self.total > self.limit {
            return Err(GuardError::Body);
        }
        for &byte in bytes {
            self.phase = match self.phase {
                Phase::Initial => {
                    if self.initial.advance(byte) {
                        Phase::Suffix(None)
                    } else {
                        Phase::Initial
                    }
                }
                Phase::Body => {
                    if self.body.advance(byte) {
                        Phase::Suffix(None)
                    } else {
                        Phase::Body
                    }
                }
                Phase::Suffix(None) => Phase::Suffix(Some(byte)),
                Phase::Suffix(Some(first)) => {
                    if first == b'\r' && byte == b'\n' {
                        Phase::Headers(2, 0x0d0a)
                    } else if first == b'-' && byte == b'-' {
                        Phase::Done
                    } else {
                        self.body.matched = 0;
                        self.body.advance(first);
                        self.body.advance(byte);
                        Phase::Body
                    }
                }
                Phase::Headers(count, tail) => {
                    let count = count + 1;
                    let tail = (tail << 8) | u32::from(byte);
                    if count > 1024 {
                        return Err(GuardError::Headers);
                    }
                    if tail == 0x0d0a0d0a {
                        self.body.matched = 0;
                        Phase::Body
                    } else {
                        Phase::Headers(count, tail)
                    }
                }
                Phase::Done => Phase::Done,
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_chunk_split_preserves_payload_boundary_overlap_and_header_count() {
        let raw=b"preamble--abab\r\nContent-Disposition: form-data; name=\"file\"; filename=\"file.zip\"\r\n\r\nbody\r\n--ababaNOTBOUNDARY\r\n--abab\r\nX: y\r\n\r\nnext\r\n--abab--epilogue";
        for split in 1..raw.len() {
            let mut guard = Guard::new("abab", 10000);
            for bytes in raw.chunks(split) {
                guard.inspect(bytes).unwrap()
            }
            assert!(matches!(guard.phase, Phase::Done));
        }
    }
    #[test]
    fn huge_headers_reject_at_a_fixed_bound_across_every_chunk_split() {
        let mut raw = b"--boundary\r\nX: ".to_vec();
        raw.extend(vec![b'x'; 10000]);
        for split in [1, 2, 3, 4, 7, 64, 1024, 8192] {
            let mut guard = Guard::new("boundary", 1 << 20);
            let mut rejected = false;
            for bytes in raw.chunks(split) {
                if guard.inspect(bytes) == Err(GuardError::Headers) {
                    rejected = true;
                    break;
                }
            }
            assert!(rejected);
            assert!(guard.initial.bytes.len() < 256 && guard.body.prefix.len() < 256);
        }
    }
    #[test]
    fn body_limit_applies_even_after_closing_boundary() {
        let mut guard = Guard::new("x", 10);
        assert_eq!(guard.inspect(b"--x--"), Ok(()));
        assert_eq!(guard.inspect(b"123456"), Err(GuardError::Body));
    }
}
