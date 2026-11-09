//! Account API shapes and permissions mirror apps/accounts serializers and views.
use crate::{auth::request, AuthService, Store, StoreError, User};
use axum::{
    extract::{ConnectInfo, Path, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::Row;
use std::net::SocketAddr;

pub(crate) fn router(auth: AuthService) -> Router {
    let app_data = crate::app_data::router(auth.clone());
    Router::new()
        .route(
            "/api/accounts/users/",
            get(users).post(crate::admin_writes::create_user),
        )
        .route(
            "/api/accounts/users/me/",
            get(me).patch(crate::account_writes::patch_me),
        )
        .route(
            "/api/accounts/users/{id}/",
            get(user)
                .patch(crate::admin_writes::patch_user)
                .put(crate::admin_writes::put_user)
                .delete(crate::admin_writes::delete_user),
        )
        .route(
            "/api/accounts/groups/",
            get(groups).post(crate::admin_writes::create_group),
        )
        .route(
            "/api/accounts/groups/{id}/",
            get(group)
                .patch(crate::admin_writes::patch_group)
                .put(crate::admin_writes::put_group)
                .delete(crate::admin_writes::delete_group),
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
        if (admin && user.user_level < 10)
            || !self
                .store
                .network_allowed(self.client_ip(request), "UI", Some(&user))
                .await?
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

fn datetime(value: Option<DateTime<Utc>>) -> Value {
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
