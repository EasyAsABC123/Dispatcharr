//! Original current-programme API adapter. Original authors retain all credit.
//! Independent router until actual source/native HTTP/resource gates pass.
use crate::{CurrentProgramSelection, EpgProgramReads, StoreError};
use axum::{
    body::{to_bytes, Body},
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
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .to_owned();
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
        return Err(StoreError::Unsupported(
            "Current-programme multipart input is not yet implemented",
        ));
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
