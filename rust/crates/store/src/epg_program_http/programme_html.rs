//! Stored programme list rendered using the pinned Original DRF template.
//! The original Dispatcharr, Django, and DRF authors retain full credit.
use super::{unavailable, BoundedJson};
use crate::{
    source_browsable_csrf,
    vod_api_draft::{
        source_browsable_content, source_browsable_cookie, source_browsable_escape, ReadError,
    },
    StoreError,
};
use axum::{body::Body, http::HeaderMap, response::Response};
use bytes::Bytes;
use serde::Serialize;
use serde_json::{ser::PrettyFormatter, Serializer, Value};
use std::io::Write;

const TEMPLATE: &str = include!("../../datasets/epg-programme-html/list-admin.rs.in");
const MAXIMUM_PROGRAMME_JSON_BYTES: usize = 256 * 1024;
const MAXIMUM_RENDERED_BYTES: usize = 1024 * 1024;

struct HtmlResponse {
    data: Vec<u8>,
    _programmes: Bytes,
}

impl AsRef<[u8]> for HtmlResponse {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

fn render_error(error: ReadError) -> StoreError {
    match error {
        ReadError::Unsupported(detail) => StoreError::Unsupported(detail),
        _ => unavailable(),
    }
}

fn render(
    programmes: &[u8],
    username: &str,
    origin: &str,
    csrf: &str,
) -> Result<Vec<u8>, StoreError> {
    if programmes.len() > MAXIMUM_PROGRAMME_JSON_BYTES {
        return Err(unavailable());
    }
    let programmes: Value = serde_json::from_slice(programmes).map_err(|_| unavailable())?;
    let mut pretty = BoundedJson::new(MAXIMUM_RENDERED_BYTES);
    let formatter = PrettyFormatter::with_indent(b"    ");
    programmes
        .serialize(&mut Serializer::with_formatter(&mut pretty, formatter))
        .map_err(|_| unavailable())?;
    let pretty = pretty.into_data();
    let pretty = std::str::from_utf8(&pretty).map_err(|_| unavailable())?;

    let mut output = BoundedJson::new(MAXIMUM_RENDERED_BYTES);
    let mut remaining = TEMPLATE;
    while let Some(start) = remaining.find("@@GUIDE_") {
        output
            .write_all(&remaining.as_bytes()[..start])
            .map_err(|_| unavailable())?;
        let tail = &remaining[start..];
        let end = tail[2..].find("@@").ok_or_else(unavailable)? + 4;
        match &tail[..end] {
            "@@GUIDE_USERNAME@@" => source_browsable_escape(&mut output, username),
            "@@GUIDE_ORIGIN@@" => source_browsable_escape(&mut output, origin),
            "@@GUIDE_CSRF_MASK@@" => source_browsable_escape(&mut output, csrf),
            "@@GUIDE_JSON_HTML@@" => source_browsable_content(&mut output, pretty),
            _ => return Err(unavailable()),
        }
        .map_err(render_error)?;
        remaining = &tail[end..];
    }
    output
        .write_all(remaining.as_bytes())
        .map_err(|_| unavailable())?;
    Ok(output.into_data())
}

pub(super) fn response(
    headers: &HeaderMap,
    username: &str,
    origin: &str,
    programmes: Bytes,
) -> Result<Response, StoreError> {
    let incoming_cookie = source_browsable_cookie(headers);
    let (_, csrf, cookie) = source_browsable_csrf(incoming_cookie.as_deref());
    let data = render(&programmes, username, origin, &csrf)?;
    // The JSON bytes own the guide admission permit. Retain them through body delivery.
    let body = Bytes::from_owner(HtmlResponse {
        data,
        _programmes: programmes,
    });
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .header("vary", "Accept, origin, Cookie")
        .header("allow", "GET, POST, HEAD, OPTIONS")
        .header("set-cookie", cookie)
        .body(Body::from(body))
        .map_err(|_| unavailable())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_original_form_and_programme_slots() {
        let html = render(
            br#"[{"title":"A & B"}]"#,
            "guide-admin",
            "https://guide.example.invalid",
            "mask-fixture",
        )
        .unwrap();
        let html = std::str::from_utf8(&html).unwrap();
        assert!(html.contains("Program List"));
        assert!(html.contains("A &amp; B"));
        assert!(html.contains("https://guide.example.invalid/api/epg/programs/search/"));
        assert!(html.contains(">guide-admin</li>"));
        assert_eq!(html.matches("mask-fixture").count(), 2);
        assert!(!html.contains("@@GUIDE_"));
    }
}
