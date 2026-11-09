//! Plain Django first-superuser API on an existing original migrated schema.
//! Original Dispatcharr contributors retain full application credit.
use crate::{AuthService, StoreError};
use axum::{
    body::to_bytes,
    extract::Request,
    http::Method,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::net::SocketAddr;
fn error(message: impl Into<String>) -> Response {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":message.into()})),
    )
        .into_response()
}
fn python_trim(value: &str) -> &str {
    value.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
}
fn attribute_error(value: &Value, name: &str) -> String {
    format!(
        "'{}' object has no attribute '{name}'",
        crate::channel_memberships::datatype(value)
    )
}
fn email(value: Option<&Value>) -> Result<String, String> {
    let Some(value) = value.filter(|value| crate::channels::truthy(Some(value))) else {
        return Ok(String::new());
    };
    let Some(value) = value.as_str() else {
        return Err(attribute_error(value, "strip"));
    };
    Ok(
        if let Some((name, domain)) = python_trim(value).rsplit_once('@') {
            format!("{name}@{}", crate::username::lower_source(domain))
        } else {
            value.to_owned()
        },
    )
}
pub(crate) fn json_error(raw: &[u8], e: serde_json::Error) -> String {
    let message = e.to_string();
    let (label, line, column) = if raw.iter().all(u8::is_ascii_whitespace) {
        ("Expecting value", 1, 1)
    } else if message.starts_with("EOF while parsing an object")
        || message.starts_with("key must be a string")
    {
        (
            "Expecting property name enclosed in double quotes",
            e.line(),
            e.column() + usize::from(e.is_eof()),
        )
    } else if message.starts_with("expected value")
        || message.starts_with("EOF while parsing a value")
    {
        ("Expecting value", e.line(), e.column().max(1))
    } else if message.starts_with("trailing characters") {
        ("Extra data", e.line(), e.column())
    } else {
        ("Invalid JSON", e.line(), e.column())
    };
    let prefix = raw
        .split(|v| *v == b'\n')
        .take(line.saturating_sub(1))
        .map(|line| line.len() + 1)
        .sum::<usize>();
    let position = prefix + column.saturating_sub(1);
    format!("{label}: line {line} column {column} (char {position})")
}
fn database_error(error: sqlx::Error) -> String {
    if let Some(pg) = error
        .as_database_error()
        .and_then(|db| db.try_downcast_ref::<sqlx::postgres::PgDatabaseError>())
    {
        if let Some(detail) = pg.detail() {
            return format!("{}\nDETAIL:  {detail}", pg.message());
        }
        return pg.message().to_owned();
    }
    "Database unavailable".into()
}
pub(crate) async fn response(auth: AuthService, peer: SocketAddr, request: Request) -> Response {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM accounts_user WHERE user_level>=10)",
    )
    .fetch_one(auth.store.pool())
    .await;
    match exists {
        Ok(true) => return Json(json!({"superuser_exists":true})).into_response(),
        Err(error_value) => return StoreError::Database(error_value).into_response(),
        _ => {}
    }
    let method = request.method().clone();
    let ip = auth.client_ip(&crate::auth::request(peer, request.headers().clone()));
    let allowed = crate::startup::setup_allowed(
        ip,
        &std::env::var("DISPATCHARR_SETUP_ALLOWED_IP").unwrap_or_default(),
    );
    if method != Method::POST {
        return Json(
            json!({"superuser_exists":false,"client_ip":ip.to_string(),"setup_allowed":allowed}),
        )
        .into_response();
    }
    if !allowed {
        return StoreError::StatusBody(403,json!({"error":"Web setup is limited to local networks by default. Set DISPATCHARR_SETUP_ALLOWED_IP to your IP to allow setup from this network, or create the account with: python manage.py createsuperuser","client_ip":ip.to_string(),"setup_allowed":false})).into_response();
    }
    // Preserve configured/IP-denied body bypass. This is the original Django
    // default DATA_UPLOAD_MAX_MEMORY_SIZE, independent of DRF parsers.
    let raw = match to_bytes(request.into_body(), 2_621_440).await {
        Ok(raw) => raw,
        Err(_) => return error("Request body exceeded settings.DATA_UPLOAD_MAX_MEMORY_SIZE."),
    };
    let body: Value = match serde_json::from_slice(&raw) {
        Ok(body) => body,
        Err(value) => return error(json_error(&raw, value)),
    };
    let Some(fields) = body.as_object() else {
        return error(attribute_error(&body, "get"));
    };
    let username = fields.get("username");
    let password = fields.get("password");
    if !crate::channels::truthy(username) || !crate::channels::truthy(password) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({"error":"Username and password are required."})),
        )
            .into_response();
    }
    let username = username.unwrap();
    let username = match username {
        Value::String(value) => crate::username::normalize_username(value),
        value => match crate::python_action::collection_text(value, 16384) {
            Some(value) => value,
            None => return error("Username exceeds bounded implementation limit."),
        },
    };
    let email = match email(fields.get("email")) {
        Ok(value) => value,
        Err(value) => return error(value),
    };
    let password = password.unwrap();
    let Some(password) = password.as_str() else {
        return error(format!(
            "Password must be a string or bytes, got {}.",
            crate::channel_memberships::datatype(password)
        ));
    };
    let encoded = match auth.hash_password(password.to_owned()).await {
        Ok(value) => value,
        Err(_) => {
            return error("Password exceeds bounded implementation limit or worker capacity.")
        }
    };
    // Like the source, this route does not apply UserSerializer validation or
    // serialize setup callers globally; the initial admin existence check and
    // insertion remain separate, with ordinary database uniqueness enforced.
    let result=sqlx::query("INSERT INTO accounts_user(password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,avatar_config,stream_limit,api_key) VALUES($1,NULL,TRUE,$2,'','',$3,TRUE,TRUE,NOW(),10,'{}'::jsonb,'{}'::jsonb,0,NULL)").bind(encoded).bind(username).bind(email).execute(auth.store.pool()).await;
    match result {
        Ok(_) => Json(json!({"superuser_exists":true})).into_response(),
        Err(value) => error(database_error(value)),
    }
}
