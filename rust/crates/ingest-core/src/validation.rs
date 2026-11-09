//! Literal original fetch_m3u_lines validation of the first 32 KiB of a download.
use crate::{reader::decode_ignoring_invalid_utf8, trim};
pub const VALIDATION_READ_BYTES: usize = 32768;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeadValidation {
    pub valid: bool,
    pub error: Option<String>,
}

pub fn validate_playlist_head(bytes: &[u8], url: &str) -> HeadValidation {
    let head = decode_ignoring_invalid_utf8(&bytes[..bytes.len().min(VALIDATION_READ_BYTES)]);
    let lines = trim(&head).split('\n');
    let valid = lines
        .clone()
        .next()
        .is_some_and(|line| trim(line).to_uppercase().starts_with("#EXTM3U"))
        || lines.clone().any(|line| trim(line).starts_with("#EXTINF:"))
        || lines.clone().any(|line| trim(line).starts_with("http"))
        || lines.clone().any(|line| {
            ["rtsp", "rtp", "udp"]
                .iter()
                .any(|prefix| trim(line).starts_with(prefix))
        });
    if valid {
        return HeadValidation {
            valid: true,
            error: None,
        };
    }
    let lower = head.to_lowercase();
    let error = if lower.contains("<html") || lower.contains("<!doctype html") {
        format!("Server returned HTML page instead of M3U file from URL: {url}. This usually indicates an error or authentication issue.")
    } else if lower.contains("error") || lower.contains("not found") {
        let preview: String = head.chars().take(100).collect();
        format!("Server returned an error message instead of M3U file from URL: {url}. Content: {preview}")
    } else if trim(&head).is_empty() {
        format!("Server returned completely empty response from URL: {url}")
    } else {
        format!("Server provided invalid M3U content from URL: {url}. Content does not appear to be a valid M3U file.")
    };
    HeadValidation {
        valid: false,
        error: Some(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validation_keeps_source_permissive_prefixes_and_fixed_head_window() {
        assert!(validate_playlist_head(b"httpNotAWellFormedUrl", "fixture").valid);
        assert!(validate_playlist_head(b"<html>\nhttp://fixture/error-page-link", "fixture").valid);
        assert!(!validate_playlist_head(b"HTTP://uppercase-only", "fixture").valid);
        let mut bytes = vec![b' '; VALIDATION_READ_BYTES];
        bytes.extend_from_slice(b"#EXTM3U");
        assert!(!validate_playlist_head(&bytes, "fixture").valid);
        assert!(validate_playlist_head(b"\xff#extm3u\n", "fixture").valid);
    }
}
