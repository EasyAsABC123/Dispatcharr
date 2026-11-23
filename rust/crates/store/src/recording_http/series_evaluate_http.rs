//! Original Dispatcharr's synchronous Series action at 1241b3f7.
//! Original Dispatcharr, Django and DRF contributors retain behavior credit.
use super::{access, admit, reply_bytes, Owner, RecordingHttp, RAW};
use crate::{channels::truthy, series_evaluate::Report, StoreError};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};

pub(super) const PATH: &str = "/api/channels/series-rules/evaluate/";

fn selector(body: &Value) -> Result<Option<String>, StoreError> {
    let object = body.as_object().ok_or(StoreError::UnhandledRestFault)?;
    match object.get("tvg_id").filter(|v| truthy(Some(v))) {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(Value::Bool(true)) => Ok(Some("True".into())),
        Some(value @ Value::Number(_)) => {
            let bytes = crate::json_compat::compact_json_bytes(value)
                .map_err(|_| StoreError::UnhandledRestFault)?;
            let text = String::from_utf8(bytes).map_err(|_| StoreError::UnhandledRestFault)?;
            Ok(Some(text))
        }
        Some(_) => Err(StoreError::Unsupported("Series TVG identifier type")),
    }
}

fn success_bytes(report: &Report) -> Result<Vec<u8>, StoreError> {
    // The store enables preserve_order and the evaluator constructs each
    // detail in the same insertion order as the Original Python dictionaries.
    let value = json!({"success":true,"scheduled":report.scheduled,"details":report.details});
    let bytes = crate::json_compat::compact_json_bytes(&value)
        .map_err(|_| StoreError::UnhandledRestFault)?;
    let text = String::from_utf8(bytes).map_err(|_| StoreError::UnhandledRestFault)?;
    Ok(text
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
        .into_bytes())
}

#[derive(Serialize)]
struct Metadata {
    name: &'static str,
    description: &'static str,
    renders: [&'static str; 2],
    parses: [&'static str; 3],
}

fn metadata(owner: Arc<Owner>) -> Result<Response, StoreError> {
    let bytes = serde_json::to_vec(&Metadata {
        name: "Evaluate Series Rules API",
        description: "",
        renders: ["application/json", "text/html"],
        parses: [
            "application/json",
            "application/x-www-form-urlencoded",
            "multipart/form-data",
        ],
    })
    .map_err(|_| StoreError::UnhandledRestFault)?;
    reply_bytes(200, bytes, owner)
}

fn parse_selector(raw: &[u8], content_type: &str) -> Result<Option<String>, StoreError> {
    if raw.is_empty() {
        return Ok(None);
    }
    let mime = content_type.split(';').next().unwrap_or("").trim();
    if mime.eq_ignore_ascii_case("application/x-www-form-urlencoded") {
        return Ok(url::form_urlencoded::parse(raw)
            .filter(|(key, _)| key == "tvg_id")
            .last()
            .map(|(_, value)| value.into_owned())
            .filter(|value| !value.is_empty()));
    }
    if !mime.eq_ignore_ascii_case("application/json") {
        return Err(StoreError::StatusBody(
            415,
            json!({"detail":format!("Unsupported media type \"{content_type}\" in request.")}),
        ));
    }
    let body = serde_json::from_slice(raw).map_err(|error| {
        StoreError::InvalidInput(json!({"detail":format!(
            "JSON parse error - {}", crate::source_json_error(raw, error)
        )}))
    })?;
    selector(&body)
}

async fn execute(
    owner: Arc<Owner>,
    peer: SocketAddr,
    request: Request,
) -> Result<Response, StoreError> {
    crate::epg_program_http::json_negotiation(request.headers(), request.uri())?;
    let auth = owner.state.auth.clone().with_retained_scope(owner.clone());
    let user = auth
        .account_user(peer, request.headers().clone(), false)
        .await?;
    access(&user, true)?;
    let method = request.method();
    if *method == Method::OPTIONS {
        return metadata(owner);
    }
    if *method != Method::POST {
        return Err(StoreError::StatusBody(
            405,
            json!({"detail":format!("Method \"{method}\" not allowed.")}),
        ));
    }
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let raw = tokio::time::timeout(Duration::from_secs(5), to_bytes(request.into_body(), RAW))
        .await
        .map_err(|_| StoreError::StatusBody(408, json!({"detail":"Request body timed out."})))?
        .map_err(|_| StoreError::StatusBody(413, json!({"detail":"Request body too large."})))?;
    let tvg_id = parse_selector(&raw, &content_type)?;
    let writer = auth.task_sql_writer()?;
    let operation_owner = Arc::new((owner.clone(), writer));
    let report = owner
        .state
        .jobs
        .evaluate_series(tvg_id, operation_owner)
        .await?;
    reply_bytes(200, success_bytes(&report)?, owner)
}

fn response(result: Result<Response, StoreError>) -> Response {
    let mut response = result.unwrap_or_else(IntoResponse::into_response);
    if response.status() != StatusCode::INTERNAL_SERVER_ERROR {
        let headers = response.headers_mut();
        headers.insert(header::ALLOW, HeaderValue::from_static("POST, OPTIONS"));
        headers.insert(header::VARY, HeaderValue::from_static("Accept"));
    }
    response
}

pub(super) async fn handle(
    State(api): State<RecordingHttp>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let result = async {
        let owner = admit(&api)?;
        tokio::spawn(execute(owner, peer, request))
            .await
            .map_err(|_| StoreError::UnhandledRestFault)?
    }
    .await;
    response(result)
}

pub(super) async fn with_playback(
    State((api, _)): State<(RecordingHttp, crate::RecordingPlayback)>,
    peer: ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    handle(State(api), peer, request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_selector_retains_whitespace_and_truth_rules() {
        for value in [
            json!({}),
            json!({"tvg_id":null}),
            json!({"tvg_id":false}),
            json!({"tvg_id":0}),
            json!({"tvg_id":""}),
            json!({"tvg_id":[]}),
        ] {
            assert_eq!(selector(&value).unwrap(), None);
        }
        assert_eq!(
            selector(&json!({"tvg_id":"  id  "})).unwrap(),
            Some("  id  ".into())
        );
        assert_eq!(
            selector(&json!({"tvg_id":true})).unwrap(),
            Some("True".into())
        );
        assert_eq!(selector(&json!({"tvg_id":18})).unwrap(), Some("18".into()));
        assert_eq!(
            selector(&json!({"tvg_id":1e-6})).unwrap(),
            Some("1e-06".into())
        );
        assert!(matches!(
            selector(&json!([])),
            Err(StoreError::UnhandledRestFault)
        ));
    }

    #[test]
    fn parser_preserves_missing_form_unsupported_and_bad_json_cases() {
        assert_eq!(parse_selector(b"", "").unwrap(), None);
        assert_eq!(
            parse_selector(
                b"tvg_id=first&tvg_id=+last+",
                "application/x-www-form-urlencoded"
            )
            .unwrap(),
            Some(" last ".into()),
        );
        assert!(matches!(
            parse_selector(b"{}", "text/plain"),
            Err(StoreError::StatusBody(415, _))
        ));
        assert!(matches!(
            parse_selector(b"{", "application/json"),
            Err(StoreError::InvalidInput(_))
        ));
    }

    #[test]
    fn wire_bytes_keep_original_outer_and_inner_key_order() {
        let report = Report {
            scheduled: 1,
            details: vec![
                json!({"tvg_id":"a","status":"invalid_rule"}),
                json!({"tvg_id":"station","epg_source_id":7,"status":"no_epg_match"}),
                json!({"tvg_id":"next","status":"error","error":"failure"}),
                json!({"tvg_id":"next","title":"Show","status":"ok","created":1}),
            ],
        };
        assert_eq!(success_bytes(&report).unwrap(),
            br#"{"success":true,"scheduled":1,"details":[{"tvg_id":"a","status":"invalid_rule"},{"tvg_id":"station","epg_source_id":7,"status":"no_epg_match"},{"tvg_id":"next","status":"error","error":"failure"},{"tvg_id":"next","title":"Show","status":"ok","created":1}]}"#);
        assert_eq!(success_bytes(&Report::busy()).unwrap(),
            br#"{"success":true,"scheduled":0,"details":[{"status":"skipped","reason":"concurrent evaluation in progress"}]}"#);
        assert_eq!(
            success_bytes(&Report::default()).unwrap(),
            br#"{"success":true,"scheduled":0,"details":[]}"#
        );
    }

    #[test]
    fn response_headers_distinguish_drf_errors_from_generic_faults() {
        let denied = response(Err(StoreError::PermissionDenied));
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        assert_eq!(denied.headers()[header::ALLOW], "POST, OPTIONS");
        assert_eq!(denied.headers()[header::VARY], "Accept");
        let fault = response(Err(StoreError::UnhandledRestFault));
        assert_eq!(fault.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            fault.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert!(!fault.headers().contains_key(header::ALLOW));
    }
}
