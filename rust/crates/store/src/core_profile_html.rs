//! Direct Core browsable templates measured on pinned Original Dispatcharr.
//! Original Dispatcharr, Django and Django REST Framework authors retain full credit.
use crate::{
    epg_program_http::BoundedJson,
    source_browsable_csrf,
    vod_api_draft::{source_browsable_content, source_browsable_cookie, source_browsable_escape},
    StoreError,
};
use axum::{
    body::Body,
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::Response,
};
use serde::Serialize;
use serde_json::{ser::PrettyFormatter, Serializer, Value};
use std::io::Write;

const MAXIMUM_HTML: usize = 1024 * 1024;
const AGENT_LIST: &str = include!("../datasets/core-profile-html/agent-list.rs.in");
const PROFILE_DETAIL: &str = include!("../datasets/core-profile-html/profile-detail.rs.in");
pub(super) const MAXIMUM_ROWS: i64 = 1000;

#[derive(Clone, Copy)]
pub(super) enum Page<'a> {
    AgentList,
    AgentCreated,
    ProfileDetail(&'a str),
    ProfileUpdated(&'a str),
}

impl Page<'_> {
    fn method(self) -> &'static str {
        match self {
            Self::AgentList | Self::ProfileDetail(_) => "GET",
            Self::AgentCreated => "POST",
            Self::ProfileUpdated(_) => "PUT",
        }
    }

    fn status(self) -> StatusCode {
        match self {
            Self::AgentCreated => StatusCode::CREATED,
            _ => StatusCode::OK,
        }
    }
}

const NAVIGATION_ACCEPT: &str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
const AJAX_ACCEPT: &str = "text/html; q=1.0, */*";

pub(super) fn multipart(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media| media.trim() == "multipart/form-data")
}

pub(super) fn selected_write(method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
    let expected = match *method {
        Method::POST if uri.path() == "/api/core/useragents/" => NAVIGATION_ACCEPT,
        Method::PUT
            if uri.path().starts_with("/api/core/streamprofiles/") && uri.path().ends_with('/') =>
        {
            AJAX_ACCEPT
        }
        _ => return false,
    };
    uri.query().is_none() && multipart(headers) && accepts_signed_html(headers, expected)
}

fn accepts_signed_html(headers: &HeaderMap, expected: &str) -> bool {
    let mut accepts = headers.get_all(header::ACCEPT).iter();
    accepts.next().is_some_and(|value| value == expected)
        && accepts.next().is_none()
        && headers
            .get(header::AUTHORIZATION)
            .is_some_and(|value| value.as_bytes().starts_with(b"Bearer "))
}

pub(super) fn selected(method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
    method == Method::GET && uri.query().is_none() && accepts_signed_html(headers, "text/html")
}

pub(super) fn unavailable() -> StoreError {
    StoreError::Unsupported("Core browsable profile response outside the measured bounds")
}

pub(super) fn response(
    page: Page<'_>,
    headers: &HeaderMap,
    actor: &str,
    entity: &Value,
    choices: &[(i64, String)],
    allow: &'static str,
) -> Result<Response, StoreError> {
    let incoming = source_browsable_cookie(headers);
    let (_, mask, cookie) = source_browsable_csrf(incoming.as_deref());
    let data = render(page, actor, entity, choices, [&mask, &mask])?;
    Response::builder()
        .status(page.status())
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::ALLOW, allow)
        .header(header::VARY, "Accept, origin, Cookie")
        .header(header::SET_COOKIE, cookie)
        .header(header::CONTENT_LENGTH, data.len().to_string())
        .body(Body::from(data))
        .map_err(|_| unavailable())
}

fn emit(out: &mut BoundedJson, value: &str) -> Result<(), StoreError> {
    out.write_all(value.as_bytes()).map_err(|_| unavailable())
}

fn escaped(out: &mut BoundedJson, value: &str) -> Result<(), StoreError> {
    source_browsable_escape(out, value).map_err(|_| unavailable())
}

fn choice_options(
    out: &mut BoundedJson,
    choices: &[(i64, String)],
    selected: Option<i64>,
) -> Result<(), StoreError> {
    emit(out, "\n      \n        <option value=\"\" ")?;
    emit(out, if selected.is_none() { "selected" } else { "" })?;
    emit(out, ">--------</option>\n      \n")?;
    for (id, label) in choices {
        emit(out, "      \n          \n            <option value=\"")?;
        write!(out, "{id}").map_err(|_| unavailable())?;
        emit(out, "\" ")?;
        emit(
            out,
            if Some(*id) == selected {
                "selected"
            } else {
                ""
            },
        )?;
        emit(out, " >")?;
        escaped(out, label)?;
        emit(out, "</option>\n          \n")?;
    }
    emit(out, "      \n    ")
}

fn render(
    page: Page<'_>,
    actor: &str,
    entity: &Value,
    choices: &[(i64, String)],
    masks: [&str; 2],
) -> Result<Vec<u8>, StoreError> {
    let mut pretty = BoundedJson::new(MAXIMUM_HTML);
    entity
        .serialize(&mut Serializer::with_formatter(
            &mut pretty,
            PrettyFormatter::with_indent(b"    "),
        ))
        .map_err(|_| unavailable())?;
    let bytes = pretty.into_data();
    let json = std::str::from_utf8(&bytes).map_err(|_| unavailable())?;
    let (mut template, path) = match page {
        Page::AgentList | Page::AgentCreated => (AGENT_LIST, ""),
        Page::ProfileDetail(path) | Page::ProfileUpdated(path) => (PROFILE_DETAIL, path),
    };
    let text = |field: &str| entity[field].as_str().ok_or_else(unavailable);
    let agent_field = |field: &str| match page {
        Page::AgentCreated => text(field),
        _ => Ok(""),
    };
    let checked = |field: &str| {
        entity[field]
            .as_bool()
            .map(|value| if value { "checked" } else { "" })
            .ok_or_else(unavailable)
    };
    let mut output = BoundedJson::new(MAXIMUM_HTML);
    let mut display = 0;
    while let Some(start) = template.find("@@CORE_") {
        let (prefix, slot) = template.split_at(start);
        emit(&mut output, prefix)?;
        let end = slot[2..].find("@@").ok_or_else(unavailable)? + 4;
        match &slot[..end] {
            "@@CORE_ACTOR@@" => escaped(&mut output, actor)?,
            "@@CORE_PATH@@" => escaped(&mut output, path)?,
            "@@CORE_METHOD@@" => emit(&mut output, page.method())?,
            "@@CORE_STATUS@@" => emit(
                &mut output,
                if page.status() == StatusCode::CREATED {
                    "201 Created"
                } else {
                    "200 OK"
                },
            )?,
            "@@CORE_AGENT_NAME@@" => escaped(&mut output, agent_field("name")?)?,
            "@@CORE_AGENT_VALUE@@" => escaped(&mut output, agent_field("user_agent")?)?,
            "@@CORE_AGENT_DESCRIPTION@@" => escaped(&mut output, agent_field("description")?)?,
            "@@CORE_MASK_0@@" => emit(&mut output, masks[0])?,
            "@@CORE_MASK_1@@" => emit(&mut output, masks[1])?,
            "@@CORE_DISPLAY@@" => {
                if display == 0 {
                    source_browsable_content(&mut output, json).map_err(|_| unavailable())?;
                } else {
                    escaped(&mut output, json)?;
                }
                display += 1;
            }
            "@@CORE_NAME_WITH_QUOTE@@" => {
                emit(&mut output, "value=\"")?;
                escaped(&mut output, text("name")?)?;
                emit(&mut output, "\" >")?;
            }
            "@@CORE_COMMAND@@" => escaped(&mut output, text("command")?)?,
            "@@CORE_PARAMETERS@@" => escaped(&mut output, text("parameters")?)?,
            "@@CORE_ACTIVE@@" => emit(
                &mut output,
                if matches!(page, Page::AgentList) {
                    ""
                } else {
                    checked("is_active")?
                },
            )?,
            "@@CORE_LOCKED@@" => emit(&mut output, checked("locked")?)?,
            "@@CORE_CHOICES@@" => {
                choice_options(&mut output, choices, entity["user_agent"].as_i64())?
            }
            _ => return Err(unavailable()),
        }
        template = &slot[end..];
    }
    emit(&mut output, template)?;
    Ok(output.into_data())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn only_the_measured_bare_explicit_get_selects_this_renderer() {
        let path: Uri = "/api/core/useragents/".parse().unwrap();
        let with_query: Uri = "/api/core/useragents/?format=api".parse().unwrap();
        let mut request = HeaderMap::new();
        request.insert(
            header::AUTHORIZATION,
            "Bearer malformed-test-header".parse().unwrap(),
        );
        request.insert(header::ACCEPT, "text/html".parse().unwrap());
        assert!(selected(&Method::GET, &path, &request));
        assert!(!selected(&Method::HEAD, &path, &request));
        assert!(!selected(&Method::OPTIONS, &path, &request));
        assert!(!selected(&Method::GET, &with_query, &request));
        request.append(header::ACCEPT, "application/json".parse().unwrap());
        assert!(!selected(&Method::GET, &path, &request));
        request.insert(
            header::ACCEPT,
            "application/json, text/html;q=0.5".parse().unwrap(),
        );
        assert!(!selected(&Method::GET, &path, &request));
    }

    #[test]
    fn selected_write_requires_the_observed_canonical_multipart_headers() {
        let agent: Uri = "/api/core/useragents/".parse().unwrap();
        let alias: Uri = "/api/core/useragents.json".parse().unwrap();
        let profile: Uri = "/api/core/streamprofiles/6/".parse().unwrap();
        let with_query: Uri = "/api/core/streamprofiles/6/?format=json".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "multipart/form-data; boundary=fixture".parse().unwrap(),
        );
        headers.insert(header::ACCEPT, NAVIGATION_ACCEPT.parse().unwrap());
        assert!(!selected_write(&Method::POST, &agent, &headers));
        headers.insert(header::AUTHORIZATION, "Bearer fixture".parse().unwrap());
        assert!(selected_write(&Method::POST, &agent, &headers));
        assert!(!selected_write(&Method::POST, &alias, &headers));
        assert!(!selected_write(&Method::PUT, &profile, &headers));
        headers.insert(header::ACCEPT, AJAX_ACCEPT.parse().unwrap());
        assert!(selected_write(&Method::PUT, &profile, &headers));
        assert!(!selected_write(&Method::PUT, &with_query, &headers));
        headers.append(header::ACCEPT, "application/json".parse().unwrap());
        assert!(!selected_write(&Method::PUT, &profile, &headers));
        headers.insert(header::ACCEPT, AJAX_ACCEPT.parse().unwrap());
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        assert!(!selected_write(&Method::PUT, &profile, &headers));
    }

    #[test]
    fn captured_core_write_bytes_match_after_exact_csrf_normalization() {
        for (method, source) in [
            (
                "POST",
                include_str!("../datasets/core-profile-html/agent-create-witness.json"),
            ),
            (
                "PUT",
                include_str!("../datasets/core-profile-html/profile-update-witness.json"),
            ),
        ] {
            let witness: Value = serde_json::from_str(source).unwrap();
            let page = if method == "POST" {
                Page::AgentCreated
            } else {
                Page::ProfileUpdated(witness["path"].as_str().unwrap())
            };
            let choices: Vec<_> = witness["choices"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    (
                        row[0].as_i64().unwrap(),
                        row[1].as_str().unwrap().to_owned(),
                    )
                })
                .collect();
            let rendered = render(
                page,
                witness["actor"].as_str().unwrap(),
                &witness["entity"],
                &choices,
                ["@@CORE_BROWSE_CSRF_SLOT_0@@", "@@CORE_BROWSE_CSRF_SLOT_1@@"],
            )
            .unwrap();
            assert_eq!(
                format!("{:x}", Sha256::digest(rendered)),
                witness["normalized_sha256"].as_str().unwrap()
            );
        }
    }

    #[tokio::test]
    async fn browsable_write_status_and_cookie_match_the_rendered_mask() {
        let mut request = HeaderMap::new();
        let (old_cookie, _, _) = source_browsable_csrf(None);
        request.insert(
            header::COOKIE,
            format!("csrftoken={old_cookie}").parse().unwrap(),
        );
        let reply = response(Page::AgentCreated, &request, "fixture actor",
            &serde_json::json!({"id":9,"name":"Owned & Viewer","user_agent":"Fixture/1.0","description":"form","is_active":true}),
            &[], "GET, POST, HEAD, OPTIONS").unwrap();
        let (head, body) = reply.into_parts();
        assert_eq!(head.status, StatusCode::CREATED);
        let raw = axum::body::to_bytes(body, MAXIMUM_HTML).await.unwrap();
        let body = std::str::from_utf8(&raw).unwrap();
        assert!(body.contains("HTTP 201 Created"));
        assert!(body.contains("Owned &amp; Viewer"));
        let mask = body
            .split_once(r#""csrfToken": ""#)
            .unwrap()
            .1
            .split('"')
            .next()
            .unwrap();
        let (matched_cookie, _, _) = source_browsable_csrf(Some(mask));
        assert_eq!(matched_cookie, old_cookie);
        assert!(head.headers[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .starts_with(&format!("csrftoken={old_cookie};")));
        assert!(body.contains(&format!(r#"name="csrfmiddlewaretoken" value="{mask}""#)));
    }

    #[tokio::test]
    async fn browsable_response_carries_source_cookie_headers_and_a_real_mask() {
        let reply = response(
            Page::AgentList,
            &HeaderMap::new(),
            "Fixture & actor",
            &serde_json::json!([]),
            &[],
            "GET, POST, HEAD, OPTIONS",
        )
        .unwrap();
        let (head, body) = reply.into_parts();
        assert_eq!(
            head.headers[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert_eq!(head.headers[header::VARY], "Accept, origin, Cookie");
        assert_eq!(head.headers[header::ALLOW], "GET, POST, HEAD, OPTIONS");
        let body = axum::body::to_bytes(body, MAXIMUM_HTML).await.unwrap();
        assert_eq!(
            head.headers[header::CONTENT_LENGTH].to_str().unwrap(),
            body.len().to_string()
        );
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("Fixture &amp; actor"));
        let mask = body
            .split_once(r#""csrfToken": ""#)
            .unwrap()
            .1
            .split('"')
            .next()
            .unwrap();
        assert_eq!(mask.len(), 64);
        let (secret, _, _) = source_browsable_csrf(Some(mask));
        let cookie = head.headers[header::SET_COOKIE].to_str().unwrap();
        assert!(cookie.starts_with(&format!("csrftoken={secret};")));
        assert!(cookie.ends_with("Max-Age=31449600; Path=/; SameSite=Lax"));
        assert!(body.contains(&format!(r#"name="csrfmiddlewaretoken" value="{mask}""#)));
    }

    #[test]
    fn captured_core_page_bytes_match_after_exact_csrf_normalization() {
        for (detail, source) in [
            (
                false,
                include_str!("../datasets/core-profile-html/agent-list-witness.json"),
            ),
            (
                true,
                include_str!("../datasets/core-profile-html/profile-detail-witness.json"),
            ),
        ] {
            let input: Value = serde_json::from_str(source).unwrap();
            let page = if detail {
                Page::ProfileDetail(input["path"].as_str().unwrap())
            } else {
                Page::AgentList
            };
            let choices = input["choices"].as_array().map_or_else(Vec::new, |rows| {
                rows.iter()
                    .map(|row| (row[0].as_i64().unwrap(), row[1].as_str().unwrap().into()))
                    .collect()
            });
            let rendered = render(
                page,
                input["actor"].as_str().unwrap(),
                &input["entity"],
                &choices,
                ["@@CORE_BROWSE_CSRF_SLOT_0@@", "@@CORE_BROWSE_CSRF_SLOT_1@@"],
            )
            .unwrap();
            assert_eq!(
                format!("{:x}", Sha256::digest(rendered)),
                input["normalized_sha256"].as_str().unwrap()
            );
        }
    }
}
