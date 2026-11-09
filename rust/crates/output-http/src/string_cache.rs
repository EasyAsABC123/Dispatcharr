//! A bounded string-only subset of Django's pickle cache representation.
//! No object, import, constructor, reducer, container or reference execution.
//! Original application/cache contracts belong to Dispatcharr and Django authors.
const OVERHEAD: usize = 32;
pub fn max_encoded(max_bytes: usize) -> usize {
    max_bytes.saturating_add(OVERHEAD)
}
pub fn encode(value: &str, max_bytes: usize) -> Option<Vec<u8>> {
    if value.len() > max_bytes {
        return None;
    }
    let length = u32::try_from(value.len()).ok()?;
    let mut bytes = Vec::with_capacity(value.len().checked_add(8)?);
    bytes.extend_from_slice(&[0x80, 4, b'X']);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    bytes.push(b'.');
    Some(bytes)
}
pub fn decode(mut bytes: Vec<u8>, max_bytes: usize) -> Option<String> {
    if bytes.len() > max_encoded(max_bytes)
        || bytes.len() < 4
        || bytes[0] != 0x80
        || !matches!(bytes[1], 4 | 5)
    {
        return None;
    }
    let mut position = 2usize;
    if bytes.get(position) == Some(&0x95) {
        let length = u64::from_le_bytes(bytes.get(position + 1..position + 9)?.try_into().ok()?);
        position += 9;
        if usize::try_from(length).ok()? != bytes.len().checked_sub(position)? {
            return None;
        }
    }
    let length = match *bytes.get(position)? {
        0x8c => {
            let value = usize::from(*bytes.get(position + 1)?);
            position += 2;
            value
        }
        b'X' => {
            let value = u32::from_le_bytes(bytes.get(position + 1..position + 5)?.try_into().ok()?);
            position += 5;
            usize::try_from(value).ok()?
        }
        0x8d => {
            let value = u64::from_le_bytes(bytes.get(position + 1..position + 9)?.try_into().ok()?);
            position += 9;
            usize::try_from(value).ok()?
        }
        _ => return None,
    };
    if length > max_bytes {
        return None;
    }
    let end = position.checked_add(length)?;
    let mut tail = end;
    if bytes.get(tail) == Some(&0x94) {
        tail += 1;
    }
    if bytes.get(tail) != Some(&b'.') || tail.checked_add(1)? != bytes.len() {
        return None;
    }
    std::str::from_utf8(bytes.get(position..end)?).ok()?;
    // Reuse the admitted input allocation instead of cloning a large cache hit.
    bytes.truncate(end);
    bytes.drain(..position);
    String::from_utf8(bytes).ok()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multi_megabyte_unicode_roundtrip_and_bounds() {
        let text = "Ω & News\n".repeat(400000);
        let bytes = encode(&text, text.len()).unwrap();
        assert_eq!(
            decode(bytes.clone(), text.len()).as_deref(),
            Some(text.as_str())
        );
        assert!(decode(bytes, text.len() - 1).is_none());
        assert!(encode(&text, text.len() - 1).is_none());
    }
    #[test]
    fn source_protocol4_and5_short_framed_and_long_unframed() {
        for protocol in [4, 5] {
            let short = vec![
                0x80, protocol, 0x95, 6, 0, 0, 0, 0, 0, 0, 0, 0x8c, 2, b'h', b'i', 0x94, b'.',
            ];
            assert_eq!(decode(short, 2).as_deref(), Some("hi"));
            let mut long = encode("hi", 2).unwrap();
            long[1] = protocol;
            long.insert(long.len() - 1, 0x94);
            assert_eq!(decode(long, 2).as_deref(), Some("hi"));
        }
    }
    #[test]
    fn reject_truncation_trailing_data_invalid_utf8_objects_and_lied_frame() {
        let bytes = encode("valid", 5).unwrap();
        for length in 0..bytes.len() {
            assert!(decode(bytes[..length].to_vec(), 5).is_none());
        }
        let mut extra = bytes.clone();
        extra.push(b'.');
        assert!(decode(extra, 5).is_none());
        let mut invalid = bytes.clone();
        invalid[7] = 0xff;
        assert!(decode(invalid, 5).is_none());
        let mut object = bytes;
        object[2] = b'c';
        assert!(decode(object, 5).is_none());
        assert!(decode(vec![0x80, 4, 0x95, 255, 0, 0, 0, 0, 0, 0, 0, b'.'], 5).is_none());
    }
}
