//! Original current-programme API adapter. Original authors retain all credit.
//! Independent router until actual source/native HTTP/resource gates pass.
use crate::{CurrentProgramSelection, EpgProgramReads, StoreError};
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, Method},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use serde_json::{json, Value};
use std::net::SocketAddr;

pub(crate) fn router(state: EpgProgramReads) -> Router {
    Router::new()
        .route("/api/epg/current-programs/", any(handle))
        .with_state(state)
}
fn source_error() -> Response {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        [("content-type", "text/html; charset=utf-8")],
        r#"
<!doctype html>
<html lang="en">
<head>
  <title>Server Error (500)</title>
</head>
<body>
  <h1>Server Error (500)</h1><p></p>
</body>
</html>
"#,
    )
        .into_response()
}
fn json_error(raw: &str, error: serde_json::Error) -> StoreError {
    let message = error.to_string();
    let mut position = 0usize;
    for (index, line) in raw.split_inclusive('\n').enumerate() {
        if index + 1 == error.line() {
            let bytes = error.column().saturating_sub(1).min(line.len());
            position += line.as_bytes()[..bytes]
                .iter()
                .filter(|byte| (**byte & 0xc0) != 0x80)
                .count();
            break;
        }
        position += line.chars().count();
    }
    let detail = if message.starts_with("key must be a string") {
        "Expecting property name enclosed in double quotes"
    } else if message.starts_with("expected `:`") {
        "Expecting ':' delimiter"
    } else if message.starts_with("expected `,` or `}") || message.starts_with("expected `,` or `]")
    {
        "Expecting ',' delimiter"
    } else if message.starts_with("trailing characters") {
        "Extra data"
    } else {
        "Expecting value"
    };
    StoreError::StatusBody(
        400,
        json!({"detail":format!("JSON parse error - {detail}: line {} column {} (char {position})",error.line(),error.column())}),
    )
}
async fn multipart_body(content_type: &str, bytes: Bytes) -> Result<Value, StoreError> {
    let outside_supported_form = || {
        StoreError::Unsupported(
            "Current-programme multipart envelope or file selector is unsupported",
        )
    };
    // A plain Django QueryDict value is a string, even when its spelling
    // looks like a JSON array or null. Repeating a field does not produce a
    // list through request.data.get(). Only selector presence affects the
    // view, so do not retain field values or upload contents.
    let boundary = match multer::parse_boundary(content_type) {
        Ok(boundary) => boundary,
        Err(multer::Error::NoBoundary) => {
            return Err(StoreError::StatusBody(
                400,
                json!({"detail":"Multipart form parse error - Invalid boundary in multipart: None"}),
            ));
        }
        Err(_) => return Err(outside_supported_form()),
    };
    if boundary.is_empty()
        || boundary.len() > 201
        || !boundary.bytes().all(|byte| (32..=126).contains(&byte))
        || boundary.ends_with(' ')
    {
        return Err(StoreError::StatusBody(
            400,
            json!({"detail":format!("Multipart form parse error - Invalid boundary in multipart: {boundary}")}),
        ));
    }
    let stream = futures_util::stream::once(async move { Ok::<_, std::io::Error>(bytes) });
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut fields = serde_json::Map::new();
    let (mut text_count, mut file_count) = (0, 0);
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| outside_supported_form())?
    {
        let file = field.file_name().is_some_and(|name| !name.is_empty());
        if file {
            file_count += 1;
        } else {
            text_count += 1;
        }
        if text_count > 1000 || file_count > 100 {
            return Err(outside_supported_form());
        }
        let name = field
            .name()
            .unwrap_or_default()
            .trim_matches(|value| matches!(value, '\t'..='\r' | ' '));
        if matches!(name, "channel_uuids" | "epg_data_ids") {
            if file {
                return Err(outside_supported_form());
            }
            fields.insert(name.to_owned(), Value::String(String::new()));
        }
    }
    Ok(Value::Object(fields))
}

async fn response(
    state: &EpgProgramReads,
    peer: SocketAddr,
    request: Request,
) -> Result<Response, StoreError> {
    crate::epg_program_http::json_negotiation(request.headers(), request.uri())?;
    let method = request.method().clone();
    let user = state
        .auth()
        .account_user(
            peer,
            request.headers().clone(),
            !matches!(method, Method::GET | Method::HEAD),
        )
        .await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    if method == Method::OPTIONS {
        return Ok(Json(
            serde_json::from_str::<Value>(include_str!("../datasets/source-current-options.json"))
                .expect("original options metadata"),
        )
        .into_response());
    }
    if method != Method::POST {
        return Err(StoreError::StatusBody(
            405,
            json!({"detail":format!("Method \"{method}\" not allowed.")}),
        ));
    }
    let raw_content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let content_type = raw_content_type.split(';').next().unwrap_or("");
    let bytes = to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .map_err(|_| {
            StoreError::StatusBody(413, json!({"detail":"Request body exceeded capacity."}))
        })?;
    let body = if bytes.is_empty() {
        json!({})
    } else if content_type == "application/json" {
        let raw = std::str::from_utf8(&bytes)
            .map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?;
        serde_json::from_str(raw).map_err(|error| json_error(raw, error))?
    } else if content_type == "application/x-www-form-urlencoded" {
        let mut value = serde_json::Map::new();
        for (key, item) in url::form_urlencoded::parse(&bytes) {
            value.insert(key.into_owned(), Value::String(item.into_owned()));
        }
        Value::Object(value)
    } else if content_type == "multipart/form-data" {
        multipart_body(&raw_content_type, bytes).await?
    } else {
        return Err(StoreError::StatusBody(
            415,
            json!({"detail":format!("Unsupported media type \"{content_type}\" in request.")}),
        ));
    };
    let selection = CurrentProgramSelection::from_json(&body)?;
    let data = state
        .read_current_selected(selection, state.current_now())
        .await?;
    Ok(Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(data))
        .expect("static current-programme headers"))
}
async fn handle(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let mut response = match response(&state, peer, request).await {
        Ok(response) => response,
        Err(StoreError::StatusBody(500, _)) => source_error(),
        Err(error) => error.into_response(),
    };
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("POST, OPTIONS"));
    response
        .headers_mut()
        .append("vary", HeaderValue::from_static("Accept"));
    response
}

#[cfg(test)]
mod multipart_tests {
    use super::*;

    const CONTENT_TYPE: &str = "multipart/form-data; boundary=dispatcharr";

    fn form(fields: &[(&str, &str)]) -> Bytes {
        let mut body = String::new();
        for (name, value) in fields {
            body.push_str(&format!(
                "--dispatcharr\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            ));
        }
        body.push_str("--dispatcharr--\r\n");
        Bytes::from(body)
    }

    #[tokio::test]
    async fn ordinary_multipart_fields_follow_source_query_dict_selection() {
        for (fields, message) in [
            (
                &[("epg_data_ids", "[]")][..],
                "epg_data_ids must be an array of integers or null",
            ),
            (
                &[("channel_uuids", "[]"), ("channel_uuids", "null")][..],
                "channel_uuids must be an array of strings or null",
            ),
            (
                &[(" epg_data_ids ", "")][..],
                "epg_data_ids must be an array of integers or null",
            ),
            (
                &[("channel_uuids", "null"), ("epg_data_ids", "null")][..],
                "Provide either channel_uuids or epg_data_ids, not both",
            ),
        ] {
            let data = multipart_body(CONTENT_TYPE, form(fields)).await.unwrap();
            let selection = CurrentProgramSelection::from_json(&data);
            assert!(matches!(
                selection,
                Err(StoreError::StatusBody(400, ref body)) if body == &json!({"error":message})
            ));
        }

        let data = multipart_body(CONTENT_TYPE, form(&[("unrelated", "ignored")]))
            .await
            .unwrap();
        assert!(matches!(
            CurrentProgramSelection::from_json(&data),
            Ok(CurrentProgramSelection::Channels(None))
        ));
    }

    #[tokio::test]
    async fn missing_multipart_boundary_returns_source_parse_error() {
        let result = multipart_body("multipart/form-data", form(&[("epg_data_ids", "[]")])).await;
        assert!(matches!(
            result,
            Err(StoreError::StatusBody(400, ref body))
                if body == &json!({"detail":"Multipart form parse error - Invalid boundary in multipart: None"})
        ));
    }
}
