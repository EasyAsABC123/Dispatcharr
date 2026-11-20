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
use axum::{
    body::Body,
    http::{HeaderMap, Method},
    response::Response,
};
use bytes::Bytes;
use serde::Serialize;
use serde_json::{ser::PrettyFormatter, Serializer, Value};
use std::io::Write;

const MAXIMUM_PROGRAMME_JSON_BYTES: usize = 256 * 1024;
const MAXIMUM_RENDERED_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy)]
pub(super) enum Template {
    Admin,
    AdminFormatAlias,
    Standard,
}

impl Template {
    fn source(self) -> &'static str {
        match self {
            Self::Admin => include!("../../datasets/epg-programme-html/list-admin.rs.in"),
            Self::AdminFormatAlias => {
                include!("../../datasets/epg-programme-html/list-admin-format-api.rs.in")
            }
            Self::Standard => include!("../../datasets/epg-programme-html/list-standard.rs.in"),
        }
    }
}

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
    method: &Method,
    template: Template,
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
    let mut remaining = template.source();
    while let Some(start) = remaining.find("@@GUIDE_") {
        output
            .write_all(&remaining.as_bytes()[..start])
            .map_err(|_| unavailable())?;
        let tail = &remaining[start..];
        let end = tail[2..].find("@@").ok_or_else(unavailable)? + 4;
        match &tail[..end] {
            "@@GUIDE_METHOD@@" => source_browsable_escape(&mut output, method.as_str()),
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
    method: Method,
    template: Template,
    username: &str,
    origin: &str,
    programmes: Bytes,
) -> Result<Response, StoreError> {
    let incoming_cookie = source_browsable_cookie(headers);
    let (_, csrf, cookie) = source_browsable_csrf(incoming_cookie.as_deref());
    let data = render(&programmes, &method, template, username, origin, &csrf)?;
    let length = data.len();
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        // The JSON bytes own the guide admission permit through body delivery.
        Body::from(Bytes::from_owner(HtmlResponse {
            data,
            _programmes: programmes,
        }))
    };
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .header("vary", "Accept, origin, Cookie")
        .header("allow", "GET, POST, HEAD, OPTIONS")
        .header("set-cookie", cookie)
        .header("content-length", length.to_string())
        .body(body)
        .map_err(|_| unavailable())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_original_form_and_programme_slots() {
        let html = render(
            br#"[{"title":"A & B"}]"#,
            &Method::GET,
            Template::Admin,
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

    #[test]
    fn alias_retains_its_query_and_standard_list_omits_post_forms() {
        let render_fixture = |template| {
            render(
                br#"[{"title":"Fixture"}]"#,
                &Method::GET,
                template,
                "guide-user",
                "http://guide.example.invalid",
                "mask-fixture",
            )
            .unwrap()
        };
        let alias = render_fixture(Template::AdminFormatAlias);
        let alias = std::str::from_utf8(&alias).unwrap();
        assert!(alias.contains("<b>GET</b> /api/epg/programs/?format=api</pre>"));
        assert_eq!(alias.matches("method=\"POST\"").count(), 2);
        assert_eq!(alias.matches("mask-fixture").count(), 2);
        let standard = render_fixture(Template::Standard);
        let standard = std::str::from_utf8(&standard).unwrap();
        assert!(standard.contains("<b>GET</b> /api/epg/programs/</pre>"));
        assert!(!standard.contains("method=\"POST\""));
        assert_eq!(standard.matches("mask-fixture").count(), 1);
    }

    #[tokio::test]
    async fn head_reports_the_representation_length_with_an_empty_body() {
        let headers = HeaderMap::new();
        let programme = Bytes::from_static(br#"[{"title":"Fixture"}]"#);
        let origin = "http://guide.example.invalid";
        let get = response(
            &headers,
            Method::GET,
            Template::Admin,
            "guide-admin",
            origin,
            programme.clone(),
        )
        .unwrap();
        let head = response(
            &headers,
            Method::HEAD,
            Template::Admin,
            "guide-admin",
            origin,
            programme,
        )
        .unwrap();
        let (get_parts, get_body) = get.into_parts();
        let get_bytes = axum::body::to_bytes(get_body, MAXIMUM_RENDERED_BYTES)
            .await
            .unwrap();
        assert_eq!(
            get_parts.headers["content-length"]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            get_bytes.len()
        );
        assert_eq!(
            head.headers()["content-length"]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            get_bytes.len() + 1
        );
        assert!(axum::body::to_bytes(head.into_body(), 0)
            .await
            .unwrap()
            .is_empty());
    }
}
