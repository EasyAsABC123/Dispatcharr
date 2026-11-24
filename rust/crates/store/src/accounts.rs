//! Account API shapes and permissions mirror apps/accounts serializers and views.
use crate::{auth::request, core_reads::JsonReadId, AuthService, Store, StoreError, User};
use axum::{
    extract::{ConnectInfo, FromRequest, FromRequestParts, Path, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::Row;
use std::net::SocketAddr;

pub(crate) fn router(auth: AuthService) -> Router {
    let app_data = crate::app_data::router(auth.clone());
    let user_read = get(user).layer(from_fn_with_state(
        (auth.clone(), AccountDetail::User),
        account_detail_json_alias,
    ));
    let group_read = get(group).layer(from_fn_with_state(
        (auth.clone(), AccountDetail::Group),
        account_detail_json_alias,
    ));
    Router::new()
        .route(
            "/api/accounts/users/",
            get(users).post(crate::admin_writes::create_user),
        )
        .route(
            "/api/accounts/users.json",
            post(create_account_json)
                .layer(Extension(AccountDetail::User))
                .fallback(crate::core_reads::omitted_json_method),
        )
        .route(
            "/api/accounts/users/me/",
            get(me).patch(crate::account_writes::patch_me),
        )
        .route(
            "/api/accounts/users/{id}/",
            user_read
                .clone()
                .patch(crate::admin_writes::patch_user)
                .put(crate::admin_writes::put_user)
                .delete(crate::admin_writes::delete_user),
        )
        .route(
            "/api/accounts/users/{id}",
            user_read
                .head(crate::core_reads::omitted_json_method)
                .put(change_account_json)
                .delete(change_account_json)
                .layer(Extension(AccountDetail::User))
                .fallback(crate::core_reads::omitted_json_method),
        )
        .route(
            "/api/accounts/groups/",
            get(groups).post(crate::admin_writes::create_group),
        )
        .route(
            "/api/accounts/groups.json",
            post(create_account_json)
                .layer(Extension(AccountDetail::Group))
                .fallback(crate::core_reads::omitted_json_method),
        )
        .route(
            "/api/accounts/groups/{id}/",
            group_read
                .clone()
                .patch(crate::admin_writes::patch_group)
                .put(crate::admin_writes::put_group)
                .delete(crate::admin_writes::delete_group),
        )
        .route(
            "/api/accounts/groups/{id}",
            group_read
                .head(crate::core_reads::omitted_json_method)
                .put(change_account_json)
                .patch(change_account_json)
                .delete(change_account_json)
                .layer(Extension(AccountDetail::Group))
                .fallback(crate::core_reads::omitted_json_method),
        )
        .route("/api/accounts/permissions/", get(permissions))
        .route("/api/accounts/api-keys/", get(api_key))
        .route(
            "/api/accounts/api-keys/generate/",
            axum::routing::post(crate::account_writes::generate_key),
        )
        .route(
            "/api/accounts/api-keys/revoke/",
            axum::routing::post(crate::account_writes::revoke_key),
        )
        .with_state(auth)
        .merge(app_data)
}

impl AuthService {
    /// REST authentication/UI-network policy, preserving raw header validation.
    pub async fn account_user(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        admin: bool,
    ) -> Result<User, StoreError> {
        if ["authorization", "x-api-key"].iter().any(|name| {
            headers
                .get(*name)
                .is_some_and(|value| value.to_str().is_err())
        }) {
            return Err(StoreError::Unauthorized("Invalid authentication header"));
        }
        let request = request(peer, headers);
        self.authorize_ui(&request, admin).await
    }
    /// Request-based REST policy for management adapters. This is independent
    /// of native media and XC authentication and has no STREAMS gate.
    pub async fn authorize_ui(
        &self,
        request: &dispatcharr_contracts::PlaybackRequest,
        admin: bool,
    ) -> Result<User, StoreError> {
        let user = self
            .authenticate(request)
            .await?
            .ok_or(StoreError::Unauthorized(
                "Authentication credentials were not provided.",
            ))?;
        // Source Authenticated evaluates network/custom properties before
        // the derived IsAdmin/IsStandardUser role predicate.
        if !self
            .source_ui_network_allowed(self.client_ip(request), Some(&user))
            .await?
            || (admin && user.user_level < 10)
        {
            return Err(StoreError::PermissionDenied);
        }
        Ok(user)
    }
    pub async fn authorize_ui_admin(
        &self,
        request: &dispatcharr_contracts::PlaybackRequest,
    ) -> Result<User, StoreError> {
        self.authorize_ui(request, true).await
    }
}

pub(crate) const SERIALIZED_USER: &str = "SELECT u.id::bigint AS id,u.username,u.api_key,u.email,u.user_level,COALESCE(u.custom_properties,'null'::jsonb) AS custom_properties,COALESCE(u.avatar_config,'null'::jsonb) AS avatar_config,u.stream_limit,u.is_staff,u.is_superuser,u.last_login,u.date_joined,u.first_name,u.last_name,COALESCE((SELECT jsonb_agg(p.channelprofile_id ORDER BY p.id) FROM accounts_user_channel_profiles p WHERE p.user_id=u.id),'[]'::jsonb) AS channel_profiles FROM accounts_user u";

pub(crate) fn datetime(value: Option<DateTime<Utc>>) -> Value {
    value.map_or(Value::Null, |value| {
        Value::String(value.to_rfc3339_opts(
            if value.timestamp_subsec_micros() == 0 {
                SecondsFormat::Secs
            } else {
                SecondsFormat::Micros
            },
            true,
        ))
    })
}

pub(crate) fn serialized_user(row: sqlx::postgres::PgRow) -> Result<Value, StoreError> {
    Ok(json!({
        "id":row.try_get::<i64,_>("id")?,"username":row.try_get::<String,_>("username")?,
        "api_key":row.try_get::<Option<String>,_>("api_key")?,"email":row.try_get::<String,_>("email")?,
        "user_level":row.try_get::<i32,_>("user_level")?,"channel_profiles":row.try_get::<Value,_>("channel_profiles")?,
        "custom_properties":row.try_get::<Value,_>("custom_properties")?,"avatar_config":row.try_get::<Value,_>("avatar_config")?,
        "stream_limit":row.try_get::<i32,_>("stream_limit")?,"is_staff":row.try_get::<bool,_>("is_staff")?,
        "is_superuser":row.try_get::<bool,_>("is_superuser")?,"last_login":datetime(row.try_get("last_login")?),
        "date_joined":datetime(row.try_get("date_joined")?),"first_name":row.try_get::<String,_>("first_name")?,
        "last_name":row.try_get::<String,_>("last_name")?
    }))
}

impl Store {
    pub async fn serialized_users(&self) -> Result<Value, StoreError> {
        let rows = sqlx::query(&format!("{SERIALIZED_USER} ORDER BY u.id"))
            .fetch_all(self.pool())
            .await?;
        Ok(Value::Array(
            rows.into_iter()
                .map(serialized_user)
                .collect::<Result<_, _>>()?,
        ))
    }
    pub async fn serialized_user(&self, id: i64) -> Result<Value, StoreError> {
        let row = sqlx::query(&format!("{SERIALIZED_USER} WHERE u.id=$1"))
            .bind(id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| {
                StoreError::StatusBody(404, json!({"detail":"No User matches the given query."}))
            })?;
        serialized_user(row)
    }
    pub async fn serialized_groups(&self, id: Option<i64>) -> Result<Value, StoreError> {
        let rows=sqlx::query("SELECT g.id::bigint AS id,g.name,COALESCE((SELECT jsonb_agg(p.id ORDER BY ct.app_label,ct.model,p.codename) FROM auth_group_permissions gp JOIN auth_permission p ON p.id=gp.permission_id JOIN django_content_type ct ON ct.id=p.content_type_id WHERE gp.group_id=g.id),'[]'::jsonb) AS permissions FROM auth_group g WHERE ($1::bigint IS NULL OR g.id=$1) ORDER BY g.id")
            .bind(id).fetch_all(self.pool()).await?;
        let mut values=rows.into_iter().map(|row| Ok(json!({"id":row.try_get::<i64,_>("id")?,"name":row.try_get::<String,_>("name")?,"permissions":row.try_get::<Value,_>("permissions")?}))).collect::<Result<Vec<_>,sqlx::Error>>()?;
        if id.is_some() {
            return values.pop().ok_or_else(|| {
                StoreError::StatusBody(404, json!({"detail":"No Group matches the given query."}))
            });
        }
        Ok(Value::Array(values))
    }
    pub async fn serialized_permissions(&self) -> Result<Value, StoreError> {
        let rows=sqlx::query("SELECT p.id::bigint AS id,p.name,p.codename FROM auth_permission p JOIN django_content_type ct ON ct.id=p.content_type_id ORDER BY ct.app_label,ct.model,p.codename").fetch_all(self.pool()).await?;
        Ok(Value::Array(rows.into_iter().map(|row|Ok(json!({"id":row.try_get::<i64,_>("id")?,"name":row.try_get::<String,_>("name")?,"codename":row.try_get::<String,_>("codename")?}))).collect::<Result<_,sqlx::Error>>()?))
    }
}

async fn users(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    Ok(Json(auth.store.serialized_users().await?))
}
async fn me(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    Ok(Json(auth.store.serialized_user(user.id).await?))
}
async fn user(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    Ok(Json(auth.store.serialized_user(id).await?))
}
async fn groups(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    Ok(Json(auth.store.serialized_groups(None).await?))
}
async fn group(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    Ok(Json(auth.store.serialized_groups(Some(id)).await?))
}

#[derive(Clone, Copy)]
enum AccountDetail {
    User,
    Group,
}

impl AccountDetail {
    fn path(self) -> &'static str {
        match self {
            Self::User => "/api/accounts/users/",
            Self::Group => "/api/accounts/groups/",
        }
    }
}

async fn create_account_json(
    auth: State<AuthService>,
    Extension(owner): Extension<AccountDetail>,
    peer: ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    crate::core_reads::negotiate_literal_json_alias(request.headers())?;
    let headers = request.headers().clone();
    let body = Json::from_request(request, &auth.0).await;
    match owner {
        AccountDetail::User => crate::admin_writes::create_user(auth, peer, headers, body).await,
        AccountDetail::Group => crate::admin_writes::create_group(auth, peer, headers, body).await,
    }
}

async fn change_account_json(
    auth: State<AuthService>,
    Extension(owner): Extension<AccountDetail>,
    peer: ConnectInfo<SocketAddr>,
    JsonReadId(raw_id): JsonReadId,
    request: Request,
) -> Result<Response, StoreError> {
    crate::core_reads::negotiate_literal_json_alias(request.headers())?;
    let id = Path(raw_id.parse::<i64>().map_err(|_| StoreError::NotFound)?);
    let method = request.method().clone();
    let headers = request.headers().clone();
    if method == Method::DELETE {
        let status = match owner {
            AccountDetail::User => {
                crate::admin_writes::delete_user(auth, id, peer, headers).await?
            }
            AccountDetail::Group => {
                crate::admin_writes::delete_group(auth, id, peer, headers).await?
            }
        };
        return Ok(status.into_response());
    }
    let body = Json::from_request(request, &auth.0).await;
    let entity = match (owner, method) {
        (AccountDetail::User, Method::PUT) => {
            crate::admin_writes::put_user(auth, id, peer, headers, body).await?
        }
        (AccountDetail::Group, Method::PUT) => {
            crate::admin_writes::put_group(auth, id, peer, headers, body).await?
        }
        (AccountDetail::Group, Method::PATCH) => {
            crate::admin_writes::patch_group(auth, id, peer, headers, body).await?
        }
        _ => return Err(StoreError::NotFound),
    };
    Ok(entity.into_response())
}

async fn account_detail_json_alias(
    State((auth, owner)): State<(AuthService, AccountDetail)>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let numeric = path
        .strip_suffix('/')
        .unwrap_or(path)
        .strip_prefix(owner.path())
        .and_then(|tail| tail.strip_suffix(".json"))
        .filter(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()));
    if let Some(id) = numeric.filter(|_| request.method() == Method::GET) {
        let id = id.parse::<i64>();
        let (mut parts, _) = request.into_parts();
        let Ok(peer) = ConnectInfo::<SocketAddr>::from_request_parts(&mut parts, &auth).await
        else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let result = async {
            crate::core_reads::negotiate_literal_json_alias(&parts.headers)?;
            let id = id.map_err(|_| StoreError::NotFound)?;
            match owner {
                AccountDetail::User => user(State(auth), Path(id), peer, parts.headers).await,
                AccountDetail::Group => group(State(auth), Path(id), peer, parts.headers).await,
            }
        }
        .await;
        return crate::channels::channel_options_headers(
            result.into_response(),
            HeaderValue::from_static("GET, PUT, PATCH, DELETE, HEAD, OPTIONS"),
        );
    }
    if path.ends_with('/') {
        next.run(request).await
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn permissions(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    Ok(Json(auth.store.serialized_permissions().await?))
}
async fn api_key(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    let value = auth.store.serialized_user(user.id).await?;
    Ok(Json(json!({"key":value["api_key"]})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn measured_json_account_writes_reach_the_existing_admin_writers() {
        use axum::{
            body::{to_bytes, Body},
            http::header,
        };
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let app = router(AuthService::new(
            Store::from_pool(pool),
            crate::AuthConfig::new("owned-account-json-test-only".into()).unwrap(),
        ));
        for (method, path) in [
            (Method::POST, "/api/accounts/users.json"),
            (Method::PUT, "/api/accounts/users/27.json"),
            (Method::DELETE, "/api/accounts/users/23.json"),
            (Method::POST, "/api/accounts/groups.json"),
            (Method::PUT, "/api/accounts/groups/10.json"),
            (Method::PATCH, "/api/accounts/groups/8.json"),
            (Method::DELETE, "/api/accounts/groups/4.json"),
        ] {
            for (accept, expected) in [
                ("application/json", StatusCode::UNAUTHORIZED),
                ("text/html", StatusCode::NOT_ACCEPTABLE),
            ] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(method.clone())
                            .uri(path)
                            .extension(ConnectInfo("127.0.0.1:1234".parse::<SocketAddr>().unwrap()))
                            .header(header::ACCEPT, accept)
                            .header(header::CONTENT_TYPE, "application/json")
                            .body(Body::from("{"))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), expected, "{method} {path} {accept}");
                if expected == StatusCode::UNAUTHORIZED {
                    assert_eq!(
                        response.headers()[header::WWW_AUTHENTICATE],
                        r#"Bearer realm="api""#
                    );
                    assert_eq!(
                        to_bytes(response.into_body(), 128).await.unwrap().as_ref(),
                        br#"{"detail":"Authentication credentials were not provided."}"#
                    );
                }
            }
        }
        for (method, path) in [
            (Method::PATCH, "/api/accounts/users/27.json"),
            (Method::PUT, "/api/accounts/users/27"),
            (Method::PATCH, "/api/accounts/groups/8.xml"),
            (Method::OPTIONS, "/api/accounts/groups/8.json"),
            (Method::GET, "/api/accounts/users.json"),
            (Method::OPTIONS, "/api/accounts/groups.json"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .extension(ConnectInfo("127.0.0.1:1234".parse::<SocketAddr>().unwrap()))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn ordinary_account_user_detail_json_uses_the_admin_read_handler() {
        use axum::{
            body::{to_bytes, Body},
            http::header,
        };
        use tower::ServiceExt;

        let auth = AuthService::new(
            Store::from_pool(
                sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy("postgresql://unused@127.0.0.1:1/unused")
                    .unwrap(),
            ),
            crate::AuthConfig::new("owned-user-detail-fixture".into()).unwrap(),
        );
        let app = router(auth);
        for (method, path, accept, expected) in [
            (
                Method::GET,
                "/api/accounts/users/25.json",
                "application/json",
                StatusCode::UNAUTHORIZED,
            ),
            (
                Method::GET,
                "/api/accounts/users/25.json/?format=api",
                "application/json",
                StatusCode::UNAUTHORIZED,
            ),
            (
                Method::GET,
                "/api/accounts/users/25.json",
                "text/html",
                StatusCode::NOT_ACCEPTABLE,
            ),
            (
                Method::GET,
                "/api/accounts/users/unqualified/",
                "application/json",
                StatusCode::BAD_REQUEST,
            ),
            (
                Method::GET,
                "/api/accounts/users/me.json",
                "application/json",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::HEAD,
                "/api/accounts/users/25.json",
                "application/json",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::PATCH,
                "/api/accounts/users/25.json",
                "application/json",
                StatusCode::NOT_FOUND,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header(header::ACCEPT, accept)
                        .extension(ConnectInfo("127.0.0.1:1234".parse::<SocketAddr>().unwrap()))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{path}");
            if expected == StatusCode::UNAUTHORIZED {
                assert_eq!(
                    response.headers()[header::WWW_AUTHENTICATE],
                    "Bearer realm=\"api\""
                );
                assert_eq!(
                    response.headers()[header::ALLOW],
                    "GET, PUT, PATCH, DELETE, HEAD, OPTIONS"
                );
                let body = to_bytes(response.into_body(), 128).await.unwrap();
                assert_eq!(
                    body.as_ref(),
                    br#"{"detail":"Authentication credentials were not provided."}"#
                );
            }
        }
    }

    #[tokio::test]
    async fn ordinary_account_group_detail_json_uses_the_admin_read_handler() {
        use axum::{
            body::{to_bytes, Body},
            http::header,
        };
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://fixture:fixture@127.0.0.1:1/fixture")
            .unwrap();
        let auth = AuthService::new(
            Store::from_pool(pool),
            crate::AuthConfig::new("owned-group-alias-fixture".into()).unwrap(),
        );
        let app = router(auth);
        for (method, path, accept, expected) in [
            (
                Method::GET,
                "/api/accounts/groups/6.json",
                "application/json",
                StatusCode::UNAUTHORIZED,
            ),
            (
                Method::GET,
                "/api/accounts/groups/6.json/?format=api",
                "application/json",
                StatusCode::UNAUTHORIZED,
            ),
            (
                Method::GET,
                "/api/accounts/groups/6.json/",
                "text/html",
                StatusCode::NOT_ACCEPTABLE,
            ),
            (
                Method::GET,
                "/api/accounts/groups/unqualified/",
                "application/json",
                StatusCode::BAD_REQUEST,
            ),
            (
                Method::GET,
                "/api/accounts/groups/6",
                "application/json",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::POST,
                "/api/accounts/groups/6.json",
                "application/json",
                StatusCode::NOT_FOUND,
            ),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .header(header::ACCEPT, accept)
                .body(Body::empty())
                .unwrap();
            request
                .extensions_mut()
                .insert(ConnectInfo("127.0.0.1:1234".parse::<SocketAddr>().unwrap()));
            let reply = app.clone().oneshot(request).await.unwrap();
            assert_eq!(reply.status(), expected);
            if expected == StatusCode::UNAUTHORIZED {
                assert_eq!(
                    reply.headers()[header::WWW_AUTHENTICATE],
                    "Bearer realm=\"api\""
                );
                assert_eq!(
                    reply.headers()[header::ALLOW],
                    "GET, PUT, PATCH, DELETE, HEAD, OPTIONS"
                );
                assert_eq!(
                    to_bytes(reply.into_body(), 128).await.unwrap().as_ref(),
                    br#"{"detail":"Authentication credentials were not provided."}"#
                );
            }
        }
    }

    #[test]
    fn datetime_matches_django_utc_microseconds_and_null() {
        assert_eq!(datetime(None), Value::Null);
        assert_eq!(
            datetime(Some("2026-11-09T12:00:00Z".parse().unwrap())),
            json!("2026-11-09T12:00:00Z")
        );
        assert_eq!(
            datetime(Some("2026-11-09T12:00:00.123000Z".parse().unwrap())),
            json!("2026-11-09T12:00:00.123000Z")
        );
    }
}
