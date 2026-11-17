//! Actual Source auth route45 method/finalization and cookie logout behavior.
//! Original Dispatcharr, Django and DRF authors retain full implementation credit.
use super::*;
use axum::{
    body::{to_bytes, Body, HttpBody},
    extract::{FromRequest, Request},
    http::{header, HeaderValue, Method},
};

#[derive(Clone, Copy)]
enum Route {
    Login,
    Logout,
    Refresh,
}

fn metadata(route: Route) -> Value {
    let mut body = json!({"name":"Auth","description":"Handles user login and logout","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]});
    if matches!(route, Route::Refresh) {
        body["name"] = json!("Token Refresh");
        body["description"] = json!("");
        body["actions"] = json!({"POST":{"refresh":{"type":"string","required":true,"read_only":false,"label":"Refresh"},"access":{"type":"string","required":false,"read_only":true,"label":"Access"}}});
    }
    body
}
fn finalized(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST, OPTIONS"));
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept, origin"));
    if let Some(length) = response.body().size_hint().exact() {
        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&length.to_string()).expect("bounded decimal response length"),
        );
    }
    response
}
async fn dispatch(
    service: AuthService,
    peer: SocketAddr,
    mut request: Request,
    route: Route,
) -> Response {
    let method = request.method().clone();
    if method != Method::POST {
        // AuthViewSet authenticates even for unsupported verbs/metadata, but
        // its logout permission applies to that action, not anonymous OPTIONS.
        if !matches!(route, Route::Refresh) {
            if let Err(error) = service
                .authenticate(&super::request(peer, request.headers().clone()))
                .await
            {
                return finalized(error.into_response());
            }
        }
        return finalized(if method == Method::OPTIONS {
            Json(metadata(route)).into_response()
        } else {
            (
                StatusCode::METHOD_NOT_ALLOWED,
                Json(json!({"detail":format!("Method \"{}\" not allowed.",method)})),
            )
                .into_response()
        });
    }
    if matches!(route, Route::Logout) {
        return finalized(cookie_logout(service, peer, request.headers().clone()).await);
    }
    // DRF's empty request.data is {}, including a POST with no Content-Type.
    // Nonempty JSON continues through the existing exact parser/error path.
    let (mut parts, body) = request.into_parts();
    let bytes = match to_bytes(body, 65536).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return finalized(
                StoreError::Unsupported("Authentication request capacity unavailable")
                    .into_response(),
            )
        }
    };
    if bytes.is_empty() {
        parts.headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        parts.headers.remove(header::CONTENT_LENGTH);
        request = Request::from_parts(parts, Body::from("{}"));
    } else {
        request = Request::from_parts(parts, Body::from(bytes));
    }
    let headers = request.headers().clone();
    let body = Json::<Value>::from_request(request, &service).await;
    finalized(match route {
        Route::Login => super::login_alias(State(service), ConnectInfo(peer), headers, body).await,
        Route::Refresh => super::refresh(State(service), ConnectInfo(peer), headers, body).await,
        Route::Logout => unreachable!(),
    })
}
fn session_cookie(headers: &HeaderMap) -> Result<Option<String>, StoreError> {
    let mut found = None;
    for value in headers.get_all(header::COOKIE) {
        let value = value
            .to_str()
            .map_err(|_| StoreError::Unsupported("Session cookie encoding remains unqualified"))?;
        for part in value.split(';') {
            if let Some((name, value)) = part.trim().split_once('=') {
                if name == "sessionid" {
                    let value = value.trim();
                    if value.len() != 32
                        || !value
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                    {
                        return Err(StoreError::Unsupported(
                            "Session cookie shape remains unqualified",
                        ));
                    }
                    found = Some(value.to_owned());
                }
            }
        }
    }
    Ok(found)
}
async fn cookie_logout(service: AuthService, peer: SocketAddr, headers: HeaderMap) -> Response {
    if !headers.contains_key(header::COOKIE) {
        return super::logout(State(service), ConnectInfo(peer), headers).await;
    }
    let writer = match service.task_sql_writer() {
        Ok(writer) => writer,
        Err(error) => return error.into_response(),
    };
    let service = match service.with_request_writer_permit(writer) {
        Ok(service) => service,
        Err(error) => return error.into_response(),
    };
    // One original writer/owned actor survives HTTP observer loss through event,
    // session DELETE and Ready; no new pool, task producer or logout revocation.
    match tokio::spawn(async move {
        let user=service.account_user(peer,headers.clone(),false).await?;
        let cookie=session_cookie(&headers)?;
        service.auth_event(&super::request(peer,headers),"logout",json!(user.username),None).await;
        if let Some(key)=&cookie {
            let mut connection=service.store.pool().acquire().await?;
            let deleted=sqlx::query("DELETE FROM django_session WHERE session_key=$1").bind(key).execute(&mut *connection).await;
            if let Err(error)=deleted {let _=connection.close().await;return Err(error.into())}
            if let Err(error)=sqlx::Connection::ping(&mut *connection).await {let _=connection.close().await;return Err(error.into())}
            connection.return_to_pool().await;drop(connection);
        }
        let mut response=Json(json!({"message":"Logout successful"})).into_response();
        if cookie.is_some() {response.headers_mut().append(header::SET_COOKIE,HeaderValue::from_static("sessionid=\"\"; expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; Path=/; SameSite=Lax"));}
        Ok::<_,StoreError>(response)
    }).await {
        Ok(Ok(response))=>response,
        Ok(Err(error))=>error.into_response(),
        Err(_)=>StoreError::Unsupported("Authentication logout owner failed").into_response(),
    }
}
pub(super) async fn login(
    State(service): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    dispatch(service, peer, request, Route::Login).await
}
pub(super) async fn logout(
    State(service): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    dispatch(service, peer, request, Route::Logout).await
}
pub(super) async fn refresh(
    State(service): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    dispatch(service, peer, request, Route::Refresh).await
}
