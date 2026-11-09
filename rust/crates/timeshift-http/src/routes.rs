use crate::CatchupSessions;
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, post},
    Router,
};
use dispatcharr_contracts::PlaybackRequest;
use serde_json::{json, Map, Value};
use std::net::SocketAddr;

impl CatchupSessions {
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/catchup/sessions/", post(create))
            .route("/api/catchup/sessions/{id}/", delete(revoke))
            .route("/api/catchup/sessions/{id}/position/", post(position))
            .with_state(self)
    }
}
fn identity(request: &Request<Body>) -> Result<PlaybackRequest, dispatcharr_store::StoreError> {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .ok_or(dispatcharr_store::StoreError::Unsupported(
            "HTTP socket peer metadata is required",
        ))?
        .0
        .ip();
    Ok(PlaybackRequest {
        channel_id: String::new(),
        query: url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
            .into_owned()
            .collect(),
        headers: request
            .headers()
            .iter()
            .filter_map(|(key, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (key.as_str().into(), value.into()))
            })
            .collect(),
        peer_ip: peer,
    })
}
fn error_response(error: dispatcharr_store::StoreError, allow: &str) -> Response {
    let mut response = error.into_response();
    response
        .headers_mut()
        .insert("allow", allow.parse().unwrap());
    response
}
fn response(status: StatusCode, value: Value, allow: &str) -> Response {
    let mut response = (status, axum::Json(value)).into_response();
    response
        .headers_mut()
        .insert("allow", allow.parse().unwrap());
    response
}
fn empty(allow: &str) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert("allow", allow.parse().unwrap());
    response
}
async fn body(request: Request<Body>) -> Result<Value, Response> {
    let data = axum::body::to_bytes(request.into_body(), 64 * 1024)
        .await
        .map_err(|_| {
            response(
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"detail":"Request body too large."}),
                "POST, OPTIONS",
            )
        })?;
    serde_json::from_slice(&data).map_err(|_| {
        response(
            StatusCode::BAD_REQUEST,
            json!({"detail":"JSON parse error"}),
            "POST, OPTIONS",
        )
    })
}
fn required<'a>(
    data: &'a Map<String, Value>,
    field: &str,
    errors: &mut Map<String, Value>,
) -> Option<&'a Value> {
    match data.get(field) {
        None => {
            errors.insert(field.into(), json!(["This field is required."]));
            None
        }
        Some(Value::Null) => {
            errors.insert(field.into(), json!(["This field may not be null."]));
            None
        }
        Some(value) => Some(value),
    }
}
fn number_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.trim().to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}
fn integer(value: &Value) -> Option<i64> {
    let mut value = number_string(value)?;
    if let Some((base, fraction)) = value.split_once('.') {
        if !fraction.bytes().all(|v| v == b'0') {
            return None;
        }
        value = base.into();
    }
    value.parse().ok()
}
fn dict(value: &Value) -> Result<&Map<String, Value>, Value> {
    value.as_object().ok_or_else(||{
        let kind=match value {Value::Array(_)=>"list",Value::String(_)=>"str",Value::Number(_)=>"int",Value::Bool(_)=>"bool",Value::Null=>"NoneType",_=>"object"};
        json!({"non_field_errors":[format!("Invalid data. Expected a dictionary, but got {kind}.")]})
    })
}

async fn create(State(service): State<CatchupSessions>, request: Request<Body>) -> Response {
    let identity = match identity(&request) {
        Ok(identity) => identity,
        Err(error) => return error_response(error, "POST, OPTIONS"),
    };
    let user = match service.resolver.authenticate(&identity).await {
        Ok(user) => user,
        Err(error) => return error_response(error, "POST, OPTIONS"),
    };
    match service.resolver.enabled(&user).await {
        Ok(true) => {}
        Ok(false) => {
            return response(
                StatusCode::FORBIDDEN,
                json!({"error":"Catch-up is disabled"}),
                "POST, OPTIONS",
            )
        }
        Err(error) => return error_response(error, "POST, OPTIONS"),
    }
    let value = match body(request).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    let data = match dict(&value) {
        Ok(data) => data,
        Err(error) => return response(StatusCode::BAD_REQUEST, error, "POST, OPTIONS"),
    };
    let mut errors = Map::new();
    let channel = required(data, "channel_uuid", &mut errors).and_then(|value| {
        value
            .as_str()
            .and_then(|v| uuid::Uuid::parse_str(v).ok())
            .or_else(|| {
                errors.insert("channel_uuid".into(), json!(["Must be a valid UUID."]));
                None
            })
    });
    let start = required(data, "start", &mut errors).and_then(|value| match number_string(value) {
        None => {
            errors.insert("start".into(), json!(["Not a valid string."]));
            None
        }
        Some(value) if value.is_empty() => {
            errors.insert("start".into(), json!(["This field may not be blank."]));
            None
        }
        value => value,
    });
    let duration = match data.get("duration") {
        None => None,
        Some(Value::Null) => {
            errors.insert("duration".into(), json!(["This field may not be null."]));
            None
        }
        Some(value) => match integer(value) {
            None => {
                errors.insert("duration".into(), json!(["A valid integer is required."]));
                None
            }
            Some(n) if n < 1 => {
                errors.insert(
                    "duration".into(),
                    json!(["Ensure this value is greater than or equal to 1."]),
                );
                None
            }
            Some(n) if n > 480 => {
                errors.insert(
                    "duration".into(),
                    json!(["Ensure this value is less than or equal to 480."]),
                );
                None
            }
            Some(n) => Some(n as u32),
        },
    };
    if !errors.is_empty() {
        return response(
            StatusCode::BAD_REQUEST,
            Value::Object(errors),
            "POST, OPTIONS",
        );
    }
    match service
        .create(&user, channel.unwrap(), start.unwrap(), duration)
        .await
    {
        Ok(session) => response(StatusCode::CREATED, session.json(), "POST, OPTIONS"),
        Err(error) => error_response(error, "POST, OPTIONS"),
    }
}
async fn revoke(
    State(service): State<CatchupSessions>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    let identity = match identity(&request) {
        Ok(identity) => identity,
        Err(error) => return error_response(error, "DELETE, OPTIONS"),
    };
    let user = match service.resolver.authenticate(&identity).await {
        Ok(user) => user,
        Err(error) => return error_response(error, "DELETE, OPTIONS"),
    };
    if service.delete(&id, user.id).await {
        empty("DELETE, OPTIONS")
    } else {
        response(
            StatusCode::NOT_FOUND,
            json!({"error":"Session not found"}),
            "DELETE, OPTIONS",
        )
    }
}
async fn position(
    State(service): State<CatchupSessions>,
    Path(id): Path<String>,
    request: Request<Body>,
) -> Response {
    let identity = match identity(&request) {
        Ok(identity) => identity,
        Err(error) => return error_response(error, "POST, OPTIONS"),
    };
    let user = match service.resolver.authenticate(&identity).await {
        Ok(user) => user,
        Err(error) => return error_response(error, "POST, OPTIONS"),
    };
    if !service.owns(&id, user.id).await {
        return response(
            StatusCode::NOT_FOUND,
            json!({"error":"Session not found"}),
            "POST, OPTIONS",
        );
    }
    let value = match body(request).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    let data = match dict(&value) {
        Ok(data) => data,
        Err(error) => return response(StatusCode::BAD_REQUEST, error, "POST, OPTIONS"),
    };
    let mut errors = Map::new();
    let position = required(data, "position_secs", &mut errors).and_then(|value| {
        match number_string(value).and_then(|v| v.parse::<f64>().ok()) {
            None => {
                errors.insert(
                    "position_secs".into(),
                    json!(["A valid number is required."]),
                );
                None
            }
            Some(n) if n < 0.0 => {
                errors.insert(
                    "position_secs".into(),
                    json!(["Ensure this value is greater than or equal to 0."]),
                );
                None
            }
            Some(n) if n > 28800.0 => {
                errors.insert(
                    "position_secs".into(),
                    json!(["Ensure this value is less than or equal to 28800."]),
                );
                None
            }
            Some(n) => Some(n),
        }
    });
    let paused = match data.get("paused") {
        None => None,
        Some(value) => match boolean(value) {
            Some(value) => Some(value),
            None => {
                errors.insert(
                    "paused".into(),
                    json!([if value.is_null() {
                        "This field may not be null."
                    } else {
                        "Must be a valid boolean."
                    }]),
                );
                None
            }
        },
    };
    if !errors.is_empty() {
        return response(
            StatusCode::BAD_REQUEST,
            Value::Object(errors),
            "POST, OPTIONS",
        );
    }
    if service
        .position(&id, user.id, position.unwrap(), paused)
        .await
    {
        empty("POST, OPTIONS")
    } else {
        response(
            StatusCode::NOT_FOUND,
            json!({"error":"No active playback for this session"}),
            "POST, OPTIONS",
        )
    }
}
fn boolean(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(value) if value.as_i64() == Some(0) => Some(false),
        Value::Number(value) if value.as_i64() == Some(1) => Some(true),
        Value::String(value) => match value.to_ascii_lowercase().as_str() {
            "t" | "y" | "yes" | "true" | "on" | "1" => Some(true),
            "f" | "n" | "no" | "false" | "off" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}
