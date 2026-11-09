//! fMP4 framing ported from original Dispatcharr FMP4RemuxManager.
//! Full application authorship belongs to its original authors and contributors.

/// Preserve upstream scanning, including its one-byte resync after invalid sizes.
/// `moof` is recognized once its header is present, before its body is complete.
pub fn find_moof_offset(data: &[u8], start: usize) -> Option<usize> {
    let mut offset = start;
    while offset.checked_add(8).is_some_and(|end| end <= data.len()) {
        let size = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        if &data[offset + 4..offset + 8] == b"moof" {
            return Some(offset);
        }
        offset = offset.checked_add(if size < 8 { 1 } else { size })?;
    }
    None
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Init(Vec<u8>),
    Fragment(Vec<u8>),
}
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidLimit,
    InputTooLarge,
    InitTooLarge,
    FragmentTooLarge,
    Finished,
}

/// Bounded parser for FFmpeg's stdout. Each push is at most the original 64 KiB
/// read size. Completed event owners leave the parser immediately; callers must
/// also bound their retained output and response-frame owners.
pub struct FragmentParser {
    buffer: Vec<u8>,
    init_stored: bool,
    finished: bool,
    max_init: usize,
    max_fragment: usize,
}
impl FragmentParser {
    pub fn new(max_init: usize, max_fragment: usize) -> Result<Self, Error> {
        if max_init < 8
            || max_fragment < 8
            || max_init > 10 * 1024 * 1024
            || max_fragment > 64 * 1024 * 1024
        {
            return Err(Error::InvalidLimit);
        }
        Ok(Self {
            buffer: Vec::new(),
            init_stored: false,
            finished: false,
            max_init,
            max_fragment,
        })
    }
    pub fn retained_bytes(&self) -> usize {
        self.buffer.len()
    }
    pub fn push(&mut self, data: &[u8]) -> Result<Vec<Event>, Error> {
        if self.finished {
            return Err(Error::Finished);
        }
        if data.len() > 65536 {
            return Err(Error::InputTooLarge);
        }
        let limit = if self.init_stored {
            self.max_fragment
        } else {
            self.max_init
        };
        // One read of headroom permits discovery of the next boundary before
        // applying the bound, matching the original init detection order.
        if self
            .buffer
            .len()
            .checked_add(data.len())
            .is_none_or(|size| size > limit + 65536)
        {
            return self.fail(if self.init_stored {
                Error::FragmentTooLarge
            } else {
                Error::InitTooLarge
            });
        }
        self.buffer.extend_from_slice(data);
        let mut events = Vec::new();
        if !self.init_stored {
            if let Some(offset) = find_moof_offset(&self.buffer, 0) {
                if offset > self.max_init {
                    return self.fail(Error::InitTooLarge);
                }
                let tail = self.buffer.split_off(offset);
                events.push(Event::Init(std::mem::replace(&mut self.buffer, tail)));
                self.init_stored = true;
            } else {
                if self.buffer.len() > self.max_init {
                    return self.fail(Error::InitTooLarge);
                }
                return Ok(events);
            }
        }
        while self.buffer.len() >= 8 {
            if &self.buffer[4..8] != b"moof" {
                match find_moof_offset(&self.buffer, 1) {
                    Some(offset) => {
                        self.buffer.drain(..offset);
                    }
                    None => {
                        self.buffer.clear();
                        return Ok(events);
                    }
                }
                continue;
            }
            let size = u32::from_be_bytes(self.buffer[..4].try_into().unwrap()) as usize;
            if size < 8 {
                break;
            }
            let Some(next) = find_moof_offset(&self.buffer, size) else {
                break;
            };
            if next > self.max_fragment {
                return self.fail(Error::FragmentTooLarge);
            }
            let tail = self.buffer.split_off(next);
            events.push(Event::Fragment(std::mem::replace(&mut self.buffer, tail)));
        }
        if self.buffer.len() > self.max_fragment {
            return self.fail(Error::FragmentTooLarge);
        }
        Ok(events)
    }
    /// Upstream flushes its remaining fragment bytes at stdout EOF, even when
    /// truncated. Preserve those bytes; decoding remains a separate validation.
    pub fn finish(&mut self) -> Result<Option<Event>, Error> {
        if self.finished {
            return Err(Error::Finished);
        }
        self.finished = true;
        Ok((self.init_stored && !self.buffer.is_empty())
            .then(|| Event::Fragment(std::mem::take(&mut self.buffer))))
    }
    fn fail<T>(&mut self, error: Error) -> Result<T, Error> {
        self.finished = true;
        self.buffer.clear();
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn boxed(name: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut result = ((payload.len() + 8) as u32).to_be_bytes().to_vec();
        result.extend(name);
        result.extend(payload);
        result
    }
    #[test]
    fn partial_reads_preserve_init_and_complete_fragment_boundaries() {
        let init = [boxed(b"ftyp", b"isom"), boxed(b"moov", b"codec")].concat();
        let first = [boxed(b"moof", b"first"), boxed(b"mdat", b"audio/video")].concat();
        let second = [boxed(b"moof", b"second"), boxed(b"mdat", b"more")].concat();
        let stream = [init.clone(), first.clone(), second.clone()].concat();
        for width in 1..=stream.len() {
            let mut parser = FragmentParser::new(1024, 1024).unwrap();
            let mut events = Vec::new();
            for part in stream.chunks(width) {
                events.extend(parser.push(part).unwrap());
            }
            events.extend(parser.finish().unwrap());
            assert_eq!(
                events,
                vec![
                    Event::Init(init.clone()),
                    Event::Fragment(first.clone()),
                    Event::Fragment(second.clone())
                ]
            );
        }
    }
    #[test]
    fn absent_moof_and_unbounded_fragment_fail_closed_and_clear_memory() {
        let mut parser = FragmentParser::new(8, 16).unwrap();
        assert_eq!(parser.push(&[0; 9]), Err(Error::InitTooLarge));
        assert_eq!(parser.retained_bytes(), 0);
        let mut parser = FragmentParser::new(8, 16).unwrap();
        let _ = parser.push(&boxed(b"moof", b"x")).unwrap();
        assert_eq!(parser.push(&[0; 20]), Err(Error::FragmentTooLarge));
        assert_eq!(parser.retained_bytes(), 0);
        assert_eq!(parser.finish(), Err(Error::Finished));
    }
    #[test]
    fn invalid_size_resynchronization_and_extended_size_follow_source() {
        let mut data = vec![0];
        data.extend(boxed(b"moof", b"abc"));
        assert_eq!(find_moof_offset(&data, 0), Some(1));
        assert_eq!(
            find_moof_offset(&[0, 0, 0, 1, b'f', b'r', b'e', b'e'], 0),
            None
        );
        assert_eq!(find_moof_offset(&data, usize::MAX), None);
    }
}
