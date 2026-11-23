//! Bounded delivery-log GET representation from the measured Original DRF shell.
//! Original Dispatcharr, Django and Django REST Framework authors retain credit.
use super::{capacity, delivery_logs::LogPage, RetainedResponse};
use crate::{
    epg_program_http::BoundedJson,
    source_browsable_csrf,
    vod_api_draft::{
        source_browsable_content, source_browsable_cookie, source_browsable_escape,
        vod_api_pagination::{page_url, Page},
        ReadError,
    },
    StoreError,
};
use axum::{
    body::Body,
    http::{header, HeaderMap, Uri},
    response::Response,
};
use bytes::Bytes;
use serde::Serialize;
use serde_json::{ser::PrettyFormatter, Serializer, Value};
use std::{io::Write, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

const SHELL: &str = include!("../datasets/connect-log-browsable.rs.in");
const MAXIMUM_HTML: usize = 1024 * 1024;
const MAXIMUM_REQUEST: usize = 8 * 1024;

pub(super) fn response(
    headers: &HeaderMap,
    uri: &Uri,
    username: &str,
    page: LogPage,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<Response, StoreError> {
    let incoming = source_browsable_cookie(headers);
    let (_, csrf, cookie) = source_browsable_csrf(incoming.as_deref());
    let rendered = render(uri, username, &csrf, &page)?;
    let length = rendered.len();
    let body = Bytes::from_owner(RetainedResponse {
        bytes: rendered,
        _permit: permit,
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::ALLOW, "GET, HEAD, OPTIONS")
        .header(header::VARY, "Accept, origin, Cookie")
        .header(header::SET_COOKIE, cookie)
        .header(header::CONTENT_LENGTH, length)
        .body(Body::from(body))
        .map_err(|_| capacity())
}

fn render(uri: &Uri, username: &str, csrf: &str, page: &LogPage) -> Result<Vec<u8>, StoreError> {
    let request = uri.path_and_query().ok_or_else(capacity)?.as_str();
    if request.len() > MAXIMUM_REQUEST {
        return Err(capacity());
    }
    let json: Value = serde_json::from_str(&page.payload).map_err(|_| capacity())?;
    let mut pretty = BoundedJson::new(MAXIMUM_HTML);
    json.serialize(&mut Serializer::with_formatter(
        &mut pretty,
        PrettyFormatter::with_indent(b"    "),
    ))
    .map_err(|_| capacity())?;
    let rendered_json = pretty.into_data();
    let rendered_json = std::str::from_utf8(&rendered_json).map_err(|_| capacity())?;
    let mut out = BoundedJson::new(MAXIMUM_HTML);
    let mut template = SHELL;
    while let Some(start) = template.find("@@CONNECT_") {
        out.write_all(&template.as_bytes()[..start])
            .map_err(|_| capacity())?;
        let tail = &template[start..];
        let end = tail[2..].find("@@").ok_or_else(capacity)? + 4;
        match &tail[..end] {
            "@@CONNECT_ACTOR@@" => source_browsable_escape(&mut out, username),
            "@@CONNECT_CSRF@@" => source_browsable_escape(&mut out, csrf),
            "@@CONNECT_REQUEST@@" => source_browsable_escape(&mut out, request),
            "@@CONNECT_API_LINK@@" => {
                source_browsable_escape(&mut out, &format_url(uri.path(), &page.query, "api"))
            }
            "@@CONNECT_JSON_LINK@@" => {
                source_browsable_escape(&mut out, &format_url(uri.path(), &page.query, "json"))
            }
            "@@CONNECT_PAGER@@" => navigation(&mut out, page, uri.path()),
            "@@CONNECT_JSON_HTML@@" => source_browsable_content(&mut out, rendered_json),
            _ => return Err(capacity()),
        }
        .map_err(|error| match error {
            ReadError::Unsupported(reason) => StoreError::Unsupported(reason),
            _ => capacity(),
        })?;
        template = &tail[end..];
    }
    out.write_all(template.as_bytes()).map_err(|_| capacity())?;
    Ok(out.into_data())
}

fn format_url(path: &str, query: &[(String, String)], renderer: &str) -> String {
    let mut pairs: Vec<_> = query
        .iter()
        .filter(|(name, _)| name != "format")
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    pairs.push(("format", renderer));
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let encoded = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish();
    format!("{path}?{encoded}")
}

fn navigation(out: &mut impl Write, logs: &LogPage, path: &str) -> Result<(), ReadError> {
    let page = &logs.page;
    if page.pages <= 1 {
        return Ok(());
    }
    let url = |number| {
        page_url(
            &logs.origin,
            path,
            &logs.query,
            (number > 1).then_some(number),
        )
    };
    writeln!(
        out,
        "                <nav class=\"pull-right\">\n                  <ul class=\"pagination\">"
    )
    .map_err(|_| ReadError::Capacity)?;
    arrow(
        out,
        "Previous",
        "&laquo;",
        page.has_previous().then(|| url(page.number - 1)),
    )?;
    writeln!(out, "  ").map_err(|_| ReadError::Capacity)?;
    for number in displayed_pages(page) {
        writeln!(out, "    ").map_err(|_| ReadError::Capacity)?;
        if let Some(number) = number {
            let class = if number == page.number {
                " class=\"active\""
            } else {
                ""
            };
            write!(out, "      \n        <li{class}>\n          <a href=\"")
                .map_err(|_| ReadError::Capacity)?;
            source_browsable_escape(out, &url(number))?;
            writeln!(out, "\">{number}</a>\n        </li>\n      \n    ")
                .map_err(|_| ReadError::Capacity)?;
        } else {
            writeln!(
                out,
                "      <li class=\"disabled\"><a href=\"#\">&hellip;</a></li>\n    "
            )
            .map_err(|_| ReadError::Capacity)?;
        }
        writeln!(out, "  ").map_err(|_| ReadError::Capacity)?;
    }
    arrow(
        out,
        "Next",
        "&raquo;",
        page.has_next().then(|| url(page.number + 1)),
    )?;
    writeln!(out, "</ul>\n\n                </nav>").map_err(|_| ReadError::Capacity)?;
    Ok(())
}

fn arrow(
    out: &mut impl Write,
    label: &str,
    glyph: &str,
    link: Option<String>,
) -> Result<(), ReadError> {
    let class = if link.is_some() {
        ""
    } else {
        " class=\"disabled\""
    };
    write!(out, "  \n    <li{class}>\n      <a href=\"").map_err(|_| ReadError::Capacity)?;
    source_browsable_escape(out, link.as_deref().unwrap_or("#"))?;
    writeln!(
        out,
        "\" aria-label=\"{label}\">\n        <span aria-hidden=\"true\">{glyph}</span>\n      </a>\n    </li>\n  "
    ).map_err(|_| ReadError::Capacity)?;
    Ok(())
}

fn displayed_pages(page: &Page) -> Vec<Option<u64>> {
    if page.pages <= 5 {
        return (1..=page.pages).map(Some).collect();
    }
    // Pinned Django REST Framework contextual page window.
    let mut numbers = vec![1, page.number - 1, page.number, page.number + 1, page.pages];
    if page.number <= 4 {
        numbers.extend([2, 3]);
    }
    if page.number >= page.pages - 3 {
        numbers.extend([page.pages - 1, page.pages - 2]);
    }
    numbers.sort_unstable();
    numbers.dedup();
    let mut result = numbers
        .into_iter()
        .filter(|n| *n > 0 && *n <= page.pages)
        .map(Some)
        .collect::<Vec<_>>();
    if page.number > 4 {
        result.insert(1, None);
    }
    if page.number < page.pages - 3 {
        result.insert(result.len() - 1, None);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_browsable_uses_real_rows_escaped_actor_filters_and_active_page() {
        let uri = "/api/connect/logs.api?integration=712&type=webhook&page_size=2&page=2"
            .parse::<Uri>()
            .unwrap();
        let value = serde_json::json!({
            "count": 3, "next": null,
            "previous": "http://example.test/api/connect/logs.api?integration=712&page_size=2&type=webhook",
            "results": [{"id": 1729, "request_payload": {"marker": "A & <script>"}}]
        });
        let logs = LogPage {
            payload: value.to_string(),
            page: Page {
                number: 2,
                pages: 2,
                offset: 2,
                size: 2,
            },
            query: url::form_urlencoded::parse(uri.query().unwrap().as_bytes())
                .into_owned()
                .collect(),
            origin: "http://example.test".to_owned(),
        };
        let html =
            String::from_utf8(render(&uri, "<owned actor>", "csrf-test", &logs).unwrap()).unwrap();
        assert!(html.contains("&lt;owned actor&gt;"));
        assert!(html.contains("A &amp; &lt;script&gt;"));
        assert!(html.contains("1729"));
        assert!(html.contains("/api/connect/logs.api?format=json&amp;integration=712&amp;page=2&amp;page_size=2&amp;type=webhook"));
        assert!(html.contains("href=\"http://example.test/api/connect/logs.api?integration=712&amp;page_size=2&amp;type=webhook\" aria-label=\"Previous\""));
        assert!(html.contains("class=\"active\">\n          <a href=\"http://example.test/api/connect/logs.api?integration=712&amp;page=2&amp;page_size=2&amp;type=webhook\">2"));
        assert!(html.contains("/static/rest_framework/js/jquery-3.7.1.min.js"));
        assert!(!html.contains("@@CONNECT_") && !html.contains("owned-connect-browser-shell"));
    }

    #[tokio::test]
    async fn connect_browsable_response_retires_the_permit_with_the_body() {
        let uri = "/api/connect/logs.api".parse::<Uri>().unwrap();
        let logs = LogPage {
            payload: r#"{"count":0,"next":null,"previous":null,"results":[]}"#.to_owned(),
            page: Page {
                number: 1,
                pages: 1,
                offset: 0,
                size: 50,
            },
            query: Vec::new(),
            origin: "http://example.test".to_owned(),
        };
        let owner = Arc::new(tokio::sync::Semaphore::new(1));
        let lease = Arc::new(owner.clone().try_acquire_owned().unwrap());
        let response = response(&HeaderMap::new(), &uri, "reader", logs, lease).unwrap();
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert_eq!(response.headers()[header::VARY], "Accept, origin, Cookie");
        assert!(owner.try_acquire().is_err());
        let body = axum::body::to_bytes(response.into_body(), MAXIMUM_HTML)
            .await
            .unwrap();
        let html = std::str::from_utf8(&body).unwrap();
        assert!(!html.contains("class=\"pagination\""));
        assert!(html.contains("&quot;results&quot;: []"));
        drop(body);
        assert!(owner.try_acquire().is_ok());
    }

    #[test]
    fn connect_browsable_multi_page_table_is_bounded_by_the_stock_window() {
        let page = Page {
            number: 14,
            pages: 16,
            offset: 26,
            size: 2,
        };
        assert_eq!(
            displayed_pages(&page),
            [Some(1), None, Some(13), Some(14), Some(15), Some(16)]
        );
    }
}
