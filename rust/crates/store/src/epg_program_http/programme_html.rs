//! Stored programme representations rendered using pinned Original DRF templates.
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
pub(super) enum Template<'a> {
    Admin,
    AdminFormatAlias,
    Standard,
    StandardFormatAlias,
    AdminDetail(&'a str),
    AdminDetailFormatAlias(&'a str),
    StandardDetail(&'a str),
}

impl<'a> Template<'a> {
    fn source(self) -> &'static str {
        match self {
            Self::Admin => include!("../../datasets/epg-programme-html/list-admin.rs.in"),
            Self::AdminFormatAlias => {
                include!("../../datasets/epg-programme-html/list-admin-format-api.rs.in")
            }
            Self::Standard => include!("../../datasets/epg-programme-html/list-standard.rs.in"),
            Self::StandardFormatAlias => {
                include!("../../datasets/epg-programme-html/list-standard-format-api.rs.in")
            }
            Self::AdminDetail(_) => {
                include!("../../datasets/epg-programme-html/detail-admin.rs.in")
            }
            Self::AdminDetailFormatAlias(_) => {
                include!("../../datasets/epg-programme-html/detail-admin-format-api.rs.in")
            }
            Self::StandardDetail(_) => {
                include!("../../datasets/epg-programme-html/detail-standard.rs.in")
            }
        }
    }

    fn detail_path(self) -> Option<&'a str> {
        match self {
            Self::AdminDetail(path)
            | Self::AdminDetailFormatAlias(path)
            | Self::StandardDetail(path) => Some(path),
            _ => None,
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

fn pretty_json(programme: &Value) -> Result<Vec<u8>, StoreError> {
    let mut output = BoundedJson::new(MAXIMUM_RENDERED_BYTES);
    let formatter = PrettyFormatter::with_indent(b"    ");
    programme
        .serialize(&mut Serializer::with_formatter(&mut output, formatter))
        .map_err(|_| unavailable())?;
    Ok(output.into_data())
}

struct DetailForm {
    content: Vec<u8>,
    start_time: String,
    end_time: String,
}

impl DetailForm {
    fn from_programme(programme: &Value) -> Result<Self, StoreError> {
        const FIELDS: [&str; 13] = [
            "id",
            "start_time",
            "end_time",
            "title",
            "sub_title",
            "description",
            "tvg_id",
            "season",
            "episode",
            "is_new",
            "is_live",
            "is_premiere",
            "is_finale",
        ];
        let mut raw = serde_json::Map::new();
        for field in FIELDS {
            raw.insert(
                field.into(),
                programme.get(field).ok_or_else(unavailable)?.clone(),
            );
        }
        let datetime = |field: &str| {
            let value = programme[field].as_str().ok_or_else(unavailable)?;
            let value = chrono::DateTime::parse_from_rfc3339(value).map_err(|_| unavailable())?;
            Ok::<_, StoreError>(
                value
                    .naive_utc()
                    .format("%Y-%m-%dT%H:%M:%S%.3f")
                    .to_string(),
            )
        };
        Ok(Self {
            content: pretty_json(&Value::Object(raw))?,
            start_time: datetime("start_time")?,
            end_time: datetime("end_time")?,
        })
    }
}

fn render(
    programmes: &[u8],
    method: &Method,
    template: Template<'_>,
    username: &str,
    origin: &str,
    csrf: &str,
) -> Result<Vec<u8>, StoreError> {
    if programmes.len() > MAXIMUM_PROGRAMME_JSON_BYTES {
        return Err(unavailable());
    }
    let programmes: Value = serde_json::from_slice(programmes).map_err(|_| unavailable())?;
    let pretty = pretty_json(&programmes)?;
    let pretty = std::str::from_utf8(&pretty).map_err(|_| unavailable())?;
    let form = match template {
        Template::AdminDetail(_) | Template::AdminDetailFormatAlias(_) => {
            Some(DetailForm::from_programme(&programmes)?)
        }
        _ => None,
    };

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
            "@@GUIDE_OWNED_DETAIL_PATH@@" => {
                let path = template.detail_path().ok_or_else(unavailable)?;
                source_browsable_escape(&mut output, path)
            }
            "@@GUIDE_FORM_START_TIME@@" | "@@GUIDE_FORM_END_TIME@@" => {
                let form = form.as_ref().ok_or_else(unavailable)?;
                let value = if &tail[..end] == "@@GUIDE_FORM_START_TIME@@" {
                    &form.start_time
                } else {
                    &form.end_time
                };
                source_browsable_escape(&mut output, value)
            }
            "@@GUIDE_FORM_TITLE@@" => {
                source_browsable_escape(&mut output, programmes["title"].as_str().unwrap_or(""))
            }
            "@@GUIDE_FORM_TVG_ID@@" => {
                source_browsable_escape(&mut output, programmes["tvg_id"].as_str().unwrap_or(""))
            }
            "@@GUIDE_TEXTAREA_SUB_TITLE@@" => {
                source_browsable_escape(&mut output, programmes["sub_title"].as_str().unwrap_or(""))
            }
            "@@GUIDE_TEXTAREA_DESCRIPTION@@" => source_browsable_escape(
                &mut output,
                programmes["description"].as_str().unwrap_or(""),
            ),
            "@@GUIDE_TEXTAREA_CONTENT@@" => {
                let form = form.as_ref().ok_or_else(unavailable)?;
                let content = std::str::from_utf8(&form.content).map_err(|_| unavailable())?;
                output.write_all(b"\n").map_err(|_| unavailable())?;
                source_browsable_escape(&mut output, content)
            }
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
    template: Template<'_>,
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
    let allow = if template.detail_path().is_some() {
        super::PROGRAMME_DETAIL_ALLOW
    } else {
        super::PROGRAMME_LIST_ALLOW
    };
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .header("vary", "Accept, origin, Cookie")
        .header("allow", allow)
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
        let standard_alias = render_fixture(Template::StandardFormatAlias);
        let standard_alias = std::str::from_utf8(&standard_alias).unwrap();
        assert!(standard_alias.contains("<b>GET</b> /api/epg/programs/?format=api</pre>"));
        assert!(!standard_alias.contains("method=\"POST\""));
        assert_eq!(standard_alias.matches("mask-fixture").count(), 1);
    }

    #[test]
    fn admin_detail_renders_dates_and_the_editable_serializer_projection() {
        let programme = serde_json::json!({
            "id": 7,
            "start_time": "2026-04-13T11:00:00.123901Z",
            "end_time": "2026-04-13T11:59:59.999999Z",
            "title": "A & B",
            "sub_title": null,
            "description": " One\nTwo <曲> ",
            "tvg_id": " guide-id ",
            "season": null,
            "episode": null,
            "is_new": false,
            "is_live": false,
            "is_premiere": false,
            "is_finale": false,
            "rating_system": null,
        });
        let form = DetailForm::from_programme(&programme).unwrap();
        assert_eq!(form.start_time, "2026-04-13T11:00:00.123");
        assert_eq!(form.end_time, "2026-04-13T11:59:59.999");
        let raw = std::str::from_utf8(&form.content).unwrap();
        assert!(raw.starts_with("{\n    \"id\": 7,\n    \"start_time\":"));
        assert!(raw.contains("Two <曲>"));
        assert!(!raw.contains("rating_system"));

        let path = "/api/epg/programs/7/";
        let html = render(
            &serde_json::to_vec(&programme).unwrap(),
            &Method::GET,
            Template::AdminDetail(path),
            "guide-admin",
            "http://guide.example.invalid",
            "mask-fixture",
        )
        .unwrap();
        let html = std::str::from_utf8(&html).unwrap();
        assert_eq!(html.matches(path).count(), 10);
        assert!(html.contains("value=\"A &amp; B\""));
        assert!(html.contains("value=\" guide-id \""));
        assert!(html.contains("> One\nTwo &lt;曲&gt; </textarea>"));
        assert!(html.contains("\"form-control\">\n{\n    &quot;id&quot;: 7,"));
        assert!(html.contains("&quot;rating_system&quot;"));
        assert_eq!(html.matches("mask-fixture").count(), 1);

        let alias = render(
            &serde_json::to_vec(&programme).unwrap(),
            &Method::GET,
            Template::AdminDetailFormatAlias(path),
            "guide-admin",
            "http://guide.example.invalid",
            "mask-fixture",
        )
        .unwrap();
        let alias = std::str::from_utf8(&alias).unwrap();
        assert!(alias.contains("<b>GET</b> /api/epg/programs/7/?format=api</pre>"));
        assert!(alias.contains("/api/epg/programs/7/poster/?format=api"));
        assert!(alias.contains("value=\"2026-04-13T11:00:00.123\""));
        assert!(alias.contains("> One\nTwo &lt;曲&gt; </textarea>"));
        assert_eq!(alias.matches(path).count(), 10);
        assert!(!alias.contains("@@GUIDE_"));
    }

    #[test]
    fn standard_detail_renders_without_constructing_admin_form() {
        let path = "/api/epg/programs/7/";
        let html = render(
            br#"{"id":7,"title":"A & B"}"#,
            &Method::GET,
            Template::StandardDetail(path),
            "guide-standard",
            "http://guide.example.invalid",
            "mask-fixture",
        )
        .unwrap();
        let html = std::str::from_utf8(&html).unwrap();
        assert_eq!(html.matches(path).count(), 6);
        assert_eq!(html.matches("mask-fixture").count(), 1);
        assert!(html.contains("A &amp; B"));
        assert!(html.contains("/api/epg/programs/7/poster/"));
        assert!(!html.contains("<textarea"));
        assert!(!html.contains("data-method=\"DELETE\""));
        assert!(!html.contains("@@GUIDE_"));
    }

    #[tokio::test]
    async fn detail_admin_and_list_standard_head_add_method_byte_without_entity() {
        let headers = HeaderMap::new();
        let programme = Bytes::from_static(
            br#"{"id":7,"start_time":"2026-04-13T11:00:00Z","end_time":"2026-04-13T11:30:00Z","title":"Programme","sub_title":null,"description":null,"tvg_id":null,"season":null,"episode":null,"is_new":false,"is_live":false,"is_premiere":false,"is_finale":false}"#,
        );
        let origin = "http://guide.example.invalid";
        for (template, data, allow) in [
            (
                Template::AdminDetail("/api/epg/programs/7/"),
                programme,
                super::super::PROGRAMME_DETAIL_ALLOW,
            ),
            (
                Template::Standard,
                Bytes::from_static(br#"[{"id":7,"title":"Programme"}]"#),
                super::super::PROGRAMME_LIST_ALLOW,
            ),
        ] {
            let get = response(
                &headers,
                Method::GET,
                template,
                "guide-user",
                origin,
                data.clone(),
            )
            .unwrap();
            let head =
                response(&headers, Method::HEAD, template, "guide-user", origin, data).unwrap();
            let (headers, body) = head.into_parts();
            let get_bytes = axum::body::to_bytes(get.into_body(), MAXIMUM_RENDERED_BYTES)
                .await
                .unwrap();
            assert_eq!(
                headers.headers["content-length"]
                    .to_str()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap(),
                get_bytes.len() + 1
            );
            assert_eq!(headers.headers["allow"], allow);
            assert_eq!(headers.headers["vary"], "Accept, origin, Cookie");
            assert!(axum::body::to_bytes(body, MAXIMUM_RENDERED_BYTES)
                .await
                .unwrap()
                .is_empty());
        }
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
