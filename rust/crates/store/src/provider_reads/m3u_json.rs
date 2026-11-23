//! Literal JSON aliases for the physically observed M3U child operations.
//! Original Dispatcharr authors and contributors retain full credit.
use super::{filter_detail, filters, profile_detail, profiles};
use crate::{
    core_reads::{negotiate_literal_json_alias, JsonReadId},
    provider_mutations, provider_profile_writes, AuthService, StoreError,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, FromRequestParts, Path, State},
    http::{request::Parts, HeaderMap, StatusCode},
    Json,
};
use serde_json::Value;
use std::net::SocketAddr;

type JsonBody = Result<Json<Value>, JsonRejection>;

pub(super) struct JsonParent(String);
pub(super) struct JsonChild((String, String));

fn parent(path: &str, depth: usize) -> Result<String, StoreError> {
    path.rsplit('/')
        .nth(depth)
        .filter(|key| !key.is_empty() && key.bytes().all(|byte| byte.is_ascii_digit()))
        .map(str::to_owned)
        .ok_or(StoreError::NotFound)
}

impl<S: Send + Sync> FromRequestParts<S> for JsonParent {
    type Rejection = StoreError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parent(parts.uri.path(), 1).map(Self)
    }
}

impl<S: Send + Sync> FromRequestParts<S> for JsonChild {
    type Rejection = StoreError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let JsonReadId(id) = JsonReadId::from_request_parts(parts, state).await?;
        Ok(Self((parent(parts.uri.path(), 2)?, id)))
    }
}

// The suffix controls JSON negotiation and the raw path grammar. Authentication,
// body validation, transactions and task effects stay in the canonical handlers.
macro_rules! alias {
    ($name:ident, $handler:path, [$($key:ident: $shape:ty),*], $result:ty $(, $body:ident: $payload:ty)?) => {
        pub(super) async fn $name(
            auth: State<AuthService>,
            $($key: $shape,)*
            peer: ConnectInfo<SocketAddr>,
            headers: HeaderMap,
            $($body: $payload,)?
        ) -> Result<$result, StoreError> {
            negotiate_literal_json_alias(&headers)?;
            $handler(auth, $(Path($key.0),)* peer, headers, $($body,)?).await
        }
    };
}

alias!(read_profiles, profiles, [key: JsonParent], Json<Value>);
alias!(
    read_profile,
    profile_detail,
    [key: JsonChild],
    Json<Value>
);
alias!(
    create_profile,
    provider_profile_writes::create,
    [key: JsonParent],
    (StatusCode, Json<Value>),
    body: JsonBody
);
alias!(
    patch_profile,
    provider_profile_writes::patch,
    [key: JsonChild],
    Json<Value>,
    body: JsonBody
);
alias!(
    put_profile,
    provider_profile_writes::put,
    [key: JsonChild],
    Json<Value>,
    body: JsonBody
);
alias!(
    delete_profile,
    provider_profile_writes::delete,
    [key: JsonChild],
    StatusCode
);

alias!(read_filters, filters, [key: JsonParent], Json<Value>);
alias!(read_filter, filter_detail, [key: JsonChild], Json<Value>);
alias!(
    create_filter,
    provider_mutations::create_filter,
    [key: JsonParent],
    (StatusCode, Json<Value>),
    body: JsonBody
);
alias!(
    patch_filter,
    provider_mutations::patch_filter,
    [key: JsonChild],
    Json<Value>,
    body: JsonBody
);
alias!(
    put_filter,
    provider_mutations::put_filter,
    [key: JsonChild],
    Json<Value>,
    body: JsonBody
);
alias!(
    delete_filter,
    provider_mutations::delete_filter,
    [key: JsonChild],
    StatusCode
);

alias!(
    create_group,
    provider_mutations::create_server_group,
    [],
    (StatusCode, Json<Value>),
    body: JsonBody
);
alias!(
    patch_group,
    provider_mutations::patch_server_group,
    [key: JsonReadId],
    Json<Value>,
    body: JsonBody
);
alias!(
    put_group,
    provider_mutations::put_server_group,
    [key: JsonReadId],
    Json<Value>,
    body: JsonBody
);
alias!(
    delete_group,
    provider_mutations::delete_server_group,
    [key: JsonReadId],
    StatusCode
);

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{header, Method, Request},
        Extension, Router,
    };
    use tower::ServiceExt;

    use super::*;

    async fn status(app: &Router, method: &Method, path: &str, accept: &str) -> StatusCode {
        app.clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(path)
                    .header(header::ACCEPT, accept)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn observed_m3u_json_methods_reach_auth_with_literal_path_and_media_only() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("owned-m3u-child-alias-fixture".into()).unwrap(),
        );
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let app = super::super::router(auth).layer(Extension(ConnectInfo(peer)));
        for (path, methods) in [
            (
                "/api/m3u/accounts/2/profiles.json",
                &[Method::GET, Method::POST][..],
            ),
            (
                "/api/m3u/accounts/2/filters.json",
                &[Method::GET, Method::POST][..],
            ),
            (
                "/api/m3u/accounts/2/profiles/3.json",
                &[Method::GET, Method::PUT, Method::PATCH, Method::DELETE][..],
            ),
            (
                "/api/m3u/accounts/2/filters/3.json",
                &[Method::GET, Method::PUT, Method::PATCH, Method::DELETE][..],
            ),
            ("/api/m3u/server-groups.json", &[Method::POST][..]),
            (
                "/api/m3u/server-groups/3.json",
                &[Method::PUT, Method::PATCH, Method::DELETE][..],
            ),
        ] {
            for method in methods {
                assert_eq!(
                    status(&app, method, path, "application/json").await,
                    StatusCode::UNAUTHORIZED,
                    "{method} {path}"
                );
                assert_eq!(
                    status(&app, method, path, "text/html").await,
                    StatusCode::NOT_ACCEPTABLE,
                    "{method} {path}"
                );
            }
            for method in [Method::HEAD, Method::OPTIONS] {
                assert_eq!(
                    status(&app, &method, path, "application/json").await,
                    StatusCode::NOT_FOUND,
                    "{method} {path}"
                );
            }
        }
        for path in [
            "/api/m3u/accounts/2/profiles/3",
            "/api/m3u/accounts/2/profiles/3%2Ejson",
            "/api/m3u/accounts/2/profiles/3.JSON",
            "/api/m3u/accounts/2/profiles/3.json.json",
            "/api/m3u/accounts/2/profiles/+3.json",
            "/api/m3u/accounts/%32/filters/3.json",
            "/api/m3u/accounts/not-an-id/filters.json",
            "/api/m3u/server-groups/3.YAML",
        ] {
            for method in [Method::GET, Method::PATCH] {
                assert_eq!(
                    status(&app, &method, path, "application/json").await,
                    StatusCode::NOT_FOUND,
                    "{method} {path}"
                );
            }
        }
        assert_eq!(
            status(
                &app,
                &Method::PATCH,
                "/api/m3u/accounts/2/profiles/3/",
                "application/json"
            )
            .await,
            StatusCode::UNAUTHORIZED
        );
    }
}
