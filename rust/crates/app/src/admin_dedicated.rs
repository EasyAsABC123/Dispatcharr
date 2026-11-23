//! Private loopback Django login/logout. Public ingress routes separately.
use std::net::SocketAddr;

use axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
    middleware,
    response::Response,
    routing::{get, post},
    Router,
};
use chrono::{Duration, Utc};
use dispatcharr_store::{AuthService, DjangoAdminSessions, StoreError};
use subtle::ConstantTimeEq;
use url::{form_urlencoded, Url};

use super::admin_channelgroup::{cookie, escape, html_response, http_date, Csrf, PATH};

const HOST: &str = "127.0.0.1:5656";
const ORIGIN: &str = "http://127.0.0.1:5656";
const LOGIN: &str = "/admin/login/";
const LOGOUT: &str = "/admin/logout/";
const INDEX: &str = "/admin/";
const PROFILE: &str = "/accounts/profile/";
const SESSION_AGE: i64 = 14 * 24 * 60 * 60;
const MAX_FORM_BYTES: usize = 16 * 1024;
const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

#[derive(Clone)]
struct AdminState {
    sessions: DjangoAdminSessions,
    auth: AuthService,
}

pub(super) fn configured_bind() -> Result<Option<SocketAddr>, &'static str> {
    match std::env::var("RUST_ADMIN_BIND") {
        Ok(base) if base == HOST => Ok(Some(HOST.parse().expect("fixed loopback address"))),
        Err(std::env::VarError::NotPresent) => Ok(None),
        _ => Err("RUST_ADMIN_BIND must select the dedicated loopback 127.0.0.1:5656"),
    }
}

pub(super) fn router(sessions: DjangoAdminSessions, auth: AuthService) -> Router {
    Router::new()
        .route(LOGIN, get(login_get).post(login_post))
        .route(LOGOUT, post(logout).options(logout_options))
        .route(INDEX, get(index))
        .route(PATH, get(changelist))
        .fallback(asset)
        .with_state(AdminState { sessions, auth })
        .layer(middleware::from_fn(only_direct_authority))
        .layer(middleware::from_fn(super::response_headers::apply))
}

async fn only_direct_authority(request: Request, next: middleware::Next) -> Response {
    let mut hosts = request.headers().get_all(header::HOST).iter();
    if hosts.next().is_some_and(|host| host == HOST) && hosts.next().is_none() {
        return next.run(request).await;
    }
    error(StatusCode::BAD_REQUEST)
}

async fn login_get(State(state): State<AdminState>, headers: HeaderMap, uri: Uri) -> Response {
    if let Some(key) = cookie(&headers, "sessionid") {
        match state.sessions.signed_staff(key).await {
            Ok(true) => return redirect(INDEX, None),
            Err(error) => return storage_failure(error),
            Ok(false) => {}
        }
    }
    let next = form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .filter(|(k, _)| k == "next")
        .map(|(_, v)| v.into_owned())
        .last();
    let next = next.as_deref().and_then(safe_next).unwrap_or(INDEX);
    login_form(&headers, next, "", false)
}

async fn login_post(State(state): State<AdminState>, request: Request) -> Response {
    let Ok((headers, form)) = parse_form(request).await else {
        return error(StatusCode::BAD_REQUEST);
    };
    if !csrf_valid(&headers, form.csrf.as_deref()) {
        return error(StatusCode::FORBIDDEN);
    }
    let next = form.next.as_deref().and_then(safe_next).unwrap_or(PROFILE);
    let current = cookie(&headers, "sessionid");
    match state
        .sessions
        .login(&state.auth, &form.username, form.password, current)
        .await
    {
        Ok(Some(key)) => {
            let csrf = Csrf::for_response(None);
            let mut response = redirect(next, Some(&csrf));
            let expires = http_date(Utc::now() + Duration::seconds(SESSION_AGE));
            let cookie = format!("sessionid={key}; expires={expires}; HttpOnly; Max-Age={SESSION_AGE}; Path=/; SameSite=Lax");
            add_cookie(&mut response, &cookie);
            response
        }
        Ok(None) => login_form(&headers, next, &form.username, true),
        Err(error) => storage_failure(error),
    }
}

async fn logout(State(state): State<AdminState>, request: Request) -> Response {
    let Ok((headers, form)) = parse_form(request).await else {
        return error(StatusCode::BAD_REQUEST);
    };
    if !csrf_valid(&headers, form.csrf.as_deref()) {
        return error(StatusCode::FORBIDDEN);
    }
    let Some(key) = cookie(&headers, "sessionid") else {
        return redirect(INDEX, None);
    };
    match state.sessions.logout(&state.auth, key).await {
        Ok(true) => {
            let next = form
                .next
                .as_deref()
                .and_then(safe_next)
                .filter(|next| *next != LOGOUT);
            let mut completed = if let Some(next) = next {
                redirect(next, None)
            } else {
                html_response(
                    StatusCode::OK,
                    document(
                        "Logged out",
                        "<h1>Logged out</h1><a href=\"/admin/login/\">Log in again</a>",
                    ),
                    None,
                )
            };
            add_cookie(
                &mut completed,
                "sessionid=\"\"; expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; Path=/; SameSite=Lax",
            );
            completed
        }
        Ok(false) => redirect(INDEX, None),
        Err(error) => storage_failure(error),
    }
}

async fn logout_options() -> Response {
    let mut reply = Response::new(Body::empty());
    reply
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST, OPTIONS"));
    reply
}

async fn index(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    match cookie(&headers, "sessionid") {
        Some(key) => match state.sessions.signed_staff(key).await {
            Ok(true) => redirect(PATH, None),
            Ok(false) => redirect(LOGIN, None),
            Err(error) => storage_failure(error),
        },
        None => redirect(LOGIN, None),
    }
}

async fn changelist(State(state): State<AdminState>, headers: HeaderMap, uri: Uri) -> Response {
    super::admin_channelgroup::handle(Some(&state.sessions), &uri, &headers).await
}

async fn asset(request: Request) -> Response {
    super::admin_channelgroup_assets::serve(request.uri().path(), request.method())
        .unwrap_or_else(|| error(StatusCode::NOT_FOUND))
}

#[derive(Default)]
struct LoginForm {
    username: String,
    password: String,
    next: Option<String>,
    csrf: Option<String>,
}

async fn parse_form(request: Request) -> Result<(HeaderMap, LoginForm), ()> {
    let (parts, body) = request.into_parts();
    let media = parts
        .headers
        .get(header::CONTENT_TYPE)
        .ok_or(())?
        .to_str()
        .map_err(|_| ())?;
    if !media.split(';').next().is_some_and(|value| {
        value
            .trim()
            .eq_ignore_ascii_case("application/x-www-form-urlencoded")
    }) {
        return Err(());
    }
    let data = to_bytes(body, MAX_FORM_BYTES).await.map_err(|_| ())?;
    let mut form = LoginForm::default();
    for (key, value) in form_urlencoded::parse(&data) {
        match key.as_ref() {
            "username" => form.username = value.into_owned(),
            "password" => form.password = value.into_owned(),
            "next" => form.next = Some(value.into_owned()),
            "csrfmiddlewaretoken" => form.csrf = Some(value.into_owned()),
            _ => {}
        }
    }
    if form.next.is_none() {
        form.next = form_urlencoded::parse(parts.uri.query().unwrap_or_default().as_bytes())
            .filter(|(name, _)| name == "next")
            .map(|(_, value)| value.into_owned())
            .last();
    }
    Ok((parts.headers, form))
}

fn csrf_valid(headers: &HeaderMap, form: Option<&str>) -> bool {
    let mut origins = headers.get_all(header::ORIGIN).iter();
    if origins.next().is_some_and(|origin| origin != ORIGIN) || origins.next().is_some() {
        return false;
    }
    let Some(secret) = cookie(headers, "csrftoken").and_then(token_secret) else {
        return false;
    };
    let token = form.filter(|value| !value.is_empty()).or_else(|| {
        let mut values = headers.get_all("x-csrftoken").iter();
        let first = values.next()?.to_str().ok()?;
        values.next().is_none().then_some(first)
    });
    token
        .and_then(token_secret)
        .is_some_and(|provided| bool::from(provided.ct_eq(&secret)))
}

fn token_secret(value: &str) -> Option<[u8; 32]> {
    if !matches!(value.len(), 32 | 64) || !value.bytes().all(|value| value.is_ascii_alphanumeric())
    {
        return None;
    }
    let mut secret = [0; 32];
    if value.len() == 32 {
        secret.copy_from_slice(value.as_bytes());
    } else {
        for (index, out) in secret.iter_mut().enumerate() {
            let mask = ALPHABET
                .iter()
                .position(|value_byte| *value_byte == value.as_bytes()[index])?;
            let encoded = ALPHABET
                .iter()
                .position(|value_byte| *value_byte == value.as_bytes()[index + 32])?;
            *out = ALPHABET[(encoded + ALPHABET.len() - mask) % ALPHABET.len()];
        }
    }
    Some(secret)
}

fn safe_next(value: &str) -> Option<&str> {
    let value = value.trim();
    if !value.starts_with('/')
        || value.starts_with("//")
        || value.contains('\\')
        || value.chars().any(char::is_control)
    {
        return None;
    }
    HeaderValue::from_str(value).ok()?;
    let base = Url::parse(ORIGIN).ok()?;
    let next = base.join(value).ok()?;
    (next.origin() == base.origin()).then_some(value)
}

fn login_form(headers: &HeaderMap, next: &str, username: &str, denied: bool) -> Response {
    let csrf = Csrf::for_response(cookie(headers, "csrftoken"));
    let errors = if denied {
        "<p class=\"errornote\">Please enter the correct username and password for a staff account. Note that both fields may be case-sensitive.</p>"
    } else {
        ""
    };
    let body = format!(
        "<h1>Log in</h1>{errors}<form action=\"{LOGIN}\" method=\"post\" id=\"login-form\">\
        <input type=\"hidden\" name=\"csrfmiddlewaretoken\" value=\"{}\">\
        <label for=\"id_username\">Username:</label><input id=\"id_username\" type=\"text\" name=\"username\" \
        value=\"{}\" maxlength=\"150\" autocomplete=\"username\" required>\
        <label for=\"id_password\">Password:</label><input id=\"id_password\" type=\"password\" name=\"password\" \
        autocomplete=\"current-password\" required><input type=\"hidden\" name=\"next\" value=\"{}\">\
        <input type=\"submit\" value=\"Log in\"></form>",
        csrf.token, escape(username), escape(next)
    );
    html_response(StatusCode::OK, document("Log in", &body), Some(&csrf))
}

fn document(title: &str, content: &str) -> String {
    format!("<!doctype html><html lang=\"en-us\"><head><meta charset=\"utf-8\"><title>{title} | Django site admin</title>\
        <link rel=\"stylesheet\" href=\"/static/admin/css/base.css\"></head><body><main>{content}</main></body></html>")
}

fn add_cookie(response: &mut Response, cookie: &str) {
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(cookie).expect("dedicated cookie ASCII"),
    );
}

fn redirect(target: &str, csrf: Option<&Csrf>) -> Response {
    let mut response = html_response(StatusCode::FOUND, String::new(), csrf);
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(target).expect("checked local admin URL"),
    );
    response
}

fn error(status: StatusCode) -> Response {
    html_response(
        status,
        document(
            "Admin request",
            "<h1>Admin request could not be completed</h1>",
        ),
        None,
    )
}

fn storage_failure(error_value: StoreError) -> Response {
    match error_value {
        StoreError::RateLimited(_) | StoreError::StatusBody(503, _) => {
            error(StatusCode::SERVICE_UNAVAILABLE)
        }
        _ => error(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn trusted_loopback_csrf_requires_cookie_and_matching_form_or_header() {
        let alphabet = std::str::from_utf8(ALPHABET).unwrap();
        let secret = &alphabet[..32];
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("csrftoken={secret}")).unwrap(),
        );
        headers.insert(header::ORIGIN, HeaderValue::from_static(ORIGIN));
        let csrf = Csrf::for_response(Some(secret));
        assert!(csrf_valid(&headers, Some(&csrf.token)));
        assert!(csrf_valid(&headers, Some(secret)));
        assert!(!csrf_valid(&headers, None));
        headers.insert("x-csrftoken", HeaderValue::from_str(secret).unwrap());
        assert!(csrf_valid(&headers, None));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:9192"),
        );
        assert!(!csrf_valid(&headers, Some(&csrf.token)));
        headers.remove(header::ORIGIN);
        assert!(csrf_valid(&headers, Some(&csrf.token)));
    }

    #[test]
    fn redirects_accept_local_next_and_escape_form_values() {
        assert_eq!(safe_next(PATH), Some(PATH));
        assert_eq!(safe_next(" /admin/?p=2 "), Some("/admin/?p=2"));
        for target in [
            "",
            "https://other-owned.test/admin/",
            "//other-owned.test/",
            "/\\other-owned.test/",
        ] {
            assert!(safe_next(target).is_none());
        }
        let response = login_form(&HeaderMap::new(), PATH, "", false);
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .starts_with("csrftoken="));
    }

    #[tokio::test]
    async fn form_fields_use_normal_urlencoding() {
        let request = Request::builder()
            .method(Method::POST)
            .uri(LOGIN)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(
                "username=Owned+viewer&password=%20synthetic%20&next=%2Fadmin%2F",
            ))
            .unwrap();
        let (_, form) = parse_form(request).await.unwrap();
        assert_eq!(form.username, "Owned viewer");
        assert_eq!(form.password, " synthetic ");
        assert_eq!(form.next.as_deref(), Some(INDEX));
    }
}
