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
    http::{header, HeaderMap, Method, Uri},
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
    ProfileDetail(&'a str),
}

pub(super) fn selected(method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
    let mut accepts = headers.get_all(header::ACCEPT).iter();
    method == Method::GET
        && uri.query().is_none()
        && accepts.next().is_some_and(|value| value == "text/html")
        && accepts.next().is_none()
        && headers
            .get(header::AUTHORIZATION)
            .is_some_and(|value| value.as_bytes().starts_with(b"Bearer "))
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
        Page::AgentList => (AGENT_LIST, ""),
        Page::ProfileDetail(path) => (PROFILE_DETAIL, path),
    };
    let text = |field: &str| entity[field].as_str().ok_or_else(unavailable);
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
            "@@CORE_ACTIVE@@" => emit(&mut output, checked("is_active")?)?,
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
