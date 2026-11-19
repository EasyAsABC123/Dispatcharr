//! The cursor and complete-line rules follow the original Dispatcharr log browser.
use super::{family, missing_log, LogDirectory};
use crate::StoreError;
use axum::{
    body::{Body, Bytes},
    http::header,
    response::Response,
};
use std::{
    io::{self, Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::Path,
    sync::OnceLock,
};

use tokio::sync::OwnedSemaphorePermit;

const MAX_VIEW: u64 = 24 * 1024 * 1024;
const OUTPUT_CHUNK: usize = 32 * 1024;
const PREFIX: &[u8] = b"{\"content\":\"";

struct Tail {
    content: Bytes,
    cursor: String,
    reset: bool,
    truncated: bool,
}

fn at_line_start(file: &mut (impl Read + Seek), offset: u64) -> io::Result<bool> {
    if offset == 0 {
        return Ok(true);
    }
    file.seek(SeekFrom::Start(offset - 1))?;
    let mut previous = [0];
    Ok(file.read(&mut previous)? == 1 && previous[0] == b'\n')
}

fn cursor_offset(raw: &str, inode: u64, size: u64) -> Option<u64> {
    let (id, offset) = raw.split_once('-')?;
    if id != inode.to_string() || offset.chars().count() >= 20 {
        return None;
    }
    static DECIMAL: OnceLock<regex::Regex> = OnceLock::new();
    if !DECIMAL
        .get_or_init(|| regex::Regex::new(r"\A\d+\z").expect("fixed decimal grammar"))
        .is_match(offset)
    {
        return None;
    }
    let offset = dispatcharr_ingest_core::source_integer_literal(offset)?
        .parse::<u64>()
        .ok()?;
    (offset <= size).then_some(offset)
}

fn read_tail(
    source: &mut (impl Read + Seek),
    size: u64,
    inode: u64,
    cursor: &str,
    limit: u64,
) -> io::Result<Tail> {
    let mut start = 0;
    let mut reset = true;
    if let Some(offset) = cursor_offset(cursor, inode, size) {
        if at_line_start(source, offset)? {
            start = offset;
            reset = false;
        }
    }
    let truncated = size - start > limit;
    if truncated {
        start = size - limit;
        reset = true;
    }
    source.seek(SeekFrom::Start(start))?;
    let length = size - start;
    let mut raw = Vec::with_capacity(length as usize);
    (&mut *source).take(length).read_to_end(&mut raw)?;
    let mut skip = 0;
    if truncated && !at_line_start(source, start)? {
        if let Some(newline) = raw.iter().position(|byte| *byte == b'\n') {
            skip = newline + 1;
        }
    }
    let end = raw[skip..]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(skip, |offset| skip + offset + 1);
    let content = Bytes::from(raw).slice(skip..end);
    Ok(Tail {
        content,
        cursor: format!("{inode}-{}", start + end as u64),
        reset,
        truncated,
    })
}

fn simple_escape(ch: char) -> Option<&'static [u8]> {
    match ch {
        '"' => Some(br#"\""#),
        '\\' => Some(br"\\"),
        '\x08' => Some(br"\b"),
        '\t' => Some(br"\t"),
        '\n' => Some(br"\n"),
        '\x0c' => Some(br"\f"),
        '\r' => Some(br"\r"),
        '\u{2028}' => Some(br"\u2028"),
        '\u{2029}' => Some(br"\u2029"),
        _ => None,
    }
}

fn escaped_len(text: &str) -> usize {
    text.chars()
        .map(|ch| {
            if let Some(escaped) = simple_escape(ch) {
                escaped.len()
            } else if ch < '\x20' {
                6
            } else {
                ch.len_utf8()
            }
        })
        .sum()
}

fn append_char(output: &mut Vec<u8>, ch: char) {
    if let Some(escaped) = simple_escape(ch) {
        output.extend_from_slice(escaped);
    } else if ch < '\x20' {
        const HEX: &[u8] = b"0123456789abcdef";
        let byte = ch as usize;
        output.extend_from_slice(br"\u00");
        output.push(HEX[byte >> 4]);
        output.push(HEX[byte & 15]);
    } else {
        output.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
    }
}

fn json_response(tail: Tail, permit: OwnedSemaphorePermit) -> Result<Response, StoreError> {
    let suffix = format!(
        "\",\"cursor\":\"{}\",\"reset\":{},\"truncated\":{}}}",
        tail.cursor, tail.reset, tail.truncated
    );
    let content_length = PREFIX.len()
        + suffix.len()
        + tail
            .content
            .utf8_chunks()
            .map(|part| escaped_len(part.valid()) + 3 * usize::from(!part.invalid().is_empty()))
            .sum::<usize>();
    // Encode in pieces so control bytes cannot amplify the bounded raw
    // snapshot into a single oversized response allocation.
    let payload = async_stream::stream! {
        let _owner = permit;
        let mut output = Vec::with_capacity(OUTPUT_CHUNK + 6);
        output.extend_from_slice(PREFIX);
        for part in tail.content.utf8_chunks() {
            let characters = part.valid().chars().chain((!part.invalid().is_empty()).then_some('�'));
            for ch in characters {
                append_char(&mut output, ch);
                if output.len() >= OUTPUT_CHUNK {
                    yield Ok::<_, io::Error>(Bytes::from(output));
                    output = Vec::with_capacity(OUTPUT_CHUNK + 6);
                }
            }
        }
        output.extend_from_slice(suffix.as_bytes());
        yield Ok::<_, io::Error>(Bytes::from(output));
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, content_length)
        .body(Body::from_stream(payload))
        .map_err(|_| StoreError::UnhandledRestFault)
}

pub(super) fn serve(
    permit: OwnedSemaphorePermit,
    root: &Path,
    name: &str,
    cursor: &str,
) -> Result<Response, StoreError> {
    if !family(name) {
        return Err(missing_log());
    }
    let mut file = LogDirectory::open(root)
        .and_then(|dir| dir.open_regular(name))
        .ok_or_else(missing_log)?;
    let stat = file
        .metadata()
        .map_err(|_| StoreError::UnhandledRestFault)?;
    let tail = read_tail(&mut file, stat.len(), stat.ino(), cursor, MAX_VIEW)
        .map_err(|_| StoreError::UnhandledRestFault)?;
    json_response(tail, permit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn tail(raw: &[u8], offset: &str, limit: u64) -> Tail {
        read_tail(&mut Cursor::new(raw), raw.len() as u64, 17, offset, limit).unwrap()
    }

    #[test]
    fn cursor_exposes_complete_lines_and_resets_on_rotation_or_mid_record() {
        let raw = b"initial\ncomplete\nfragment";
        let first = tail(raw, "", MAX_VIEW);
        assert_eq!(&first.content[..], b"initial\ncomplete\n");
        assert_eq!(first.cursor, "17-17");
        assert!(first.reset);
        let delta = tail(raw, "17-8", MAX_VIEW);
        assert_eq!(&delta.content[..], b"complete\n");
        assert!(!delta.reset);
        assert!(tail(raw, "18-8", MAX_VIEW).reset);
        assert!(tail(raw, "17-6", MAX_VIEW).reset);
        assert!(tail(raw, "17-999", MAX_VIEW).reset);
    }

    #[test]
    fn cap_skips_a_cut_record_and_keeps_exact_boundary() {
        let raw = b"initial\ncomplete\nfragment";
        let exact = tail(raw, "", 17);
        assert!(exact.truncated && exact.reset);
        assert_eq!(&exact.content[..], b"complete\n");
        let cut = tail(raw, "", 16);
        assert!(cut.truncated && cut.reset);
        assert!(cut.content.is_empty());
    }

    #[tokio::test]
    async fn body_holds_dedicated_slot_until_delivery_or_drop() {
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        let slots = Arc::new(Semaphore::new(1));
        let raw = b"message\xff\n";
        let response = json_response(
            tail(raw, "", MAX_VIEW),
            slots.clone().try_acquire_owned().unwrap(),
        )
        .unwrap();
        assert_eq!(slots.available_permits(), 0);
        let length = response.headers()[header::CONTENT_LENGTH]
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(length, body.len());
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["content"], "message�\n");
        assert_eq!(slots.available_permits(), 1);

        let response = json_response(
            tail(raw, "", MAX_VIEW),
            slots.clone().try_acquire_owned().unwrap(),
        )
        .unwrap();
        assert_eq!(slots.available_permits(), 0);
        drop(response);
        assert_eq!(slots.available_permits(), 1);
    }

    #[test]
    fn bounded_serializer_matches_serde_escapes_and_drf_separators() {
        let valid = "\"\u{0000}\n\\\u{0008}é";
        let mut emitted = Vec::new();
        for ch in valid.chars() {
            append_char(&mut emitted, ch);
        }
        let expected = serde_json::to_vec(valid).unwrap();
        assert_eq!(emitted, expected[1..expected.len() - 1]);
        assert_eq!(escaped_len(valid), emitted.len());
        append_char(&mut emitted, '\u{2028}');
        append_char(&mut emitted, '\u{2029}');
        assert!(emitted.ends_with(br"\u2028\u2029"));
    }
}
