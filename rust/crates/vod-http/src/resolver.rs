use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_store::{AuthService, Store, StoreError, User};
use serde_json::Value;
use sqlx::Row;
use std::collections::HashSet;
use url::Url;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentKind { Movie, Episode, Series }
impl ContentKind {
    pub fn parse(value: &str) -> Option<Self> {
        match value { "movie" => Some(Self::Movie), "episode" => Some(Self::Episode), "series" => Some(Self::Series), _ => None }
    }
    pub fn flag(self) -> &'static str {
        if self == Self::Movie { "vod_movies_enabled" } else { "vod_series_enabled" }
    }
    pub fn storage_name(self) -> &'static str {
        if self == Self::Movie { "movie" } else { "episode" }
    }
}

#[derive(Clone)]
pub struct VodRequest {
    pub playback: PlaybackRequest,
    pub kind: String,
    pub content_id: String,
    pub session_id: Option<String>,
    pub profile_id: Option<i64>,
}

// No Debug derives: provider URLs can contain private credentials.
#[derive(Clone)]
pub struct VodCandidate {
    pub relation_id: i64,
    pub account_id: i64,
    pub profile_id: i64,
    pub max_streams: u32,
    pub url: String,
    pub user_agent: String,
}
#[derive(Clone)]
pub struct VodSource {
    pub kind: ContentKind,
    pub content_uuid: String,
    pub name: String,
    pub user: Option<User>,
    pub candidates: Vec<VodCandidate>,
    pub redirect: bool,
}

pub trait VodResolver: Send + Sync {
    fn authorize<'a>(&'a self, request: &'a VodRequest) -> BoxFuture<'a, Result<Option<User>, StoreError>>;
    fn resolve<'a>(&'a self, request: &'a VodRequest, user: Option<User>) -> BoxFuture<'a, Result<VodSource, StoreError>>;
    fn restore_user(&self, _id: i64) -> BoxFuture<'_, Result<Option<User>, StoreError>> { Box::pin(async { Ok(None) }) }
    fn admin<'a>(&'a self, _request: &'a PlaybackRequest) -> BoxFuture<'a, Result<(), StoreError>> { Box::pin(async { Err(StoreError::PermissionDenied) }) }
    fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>>;
}

#[derive(Clone)]
pub struct SqlVodResolver { store: Store, auth: AuthService }
impl SqlVodResolver {
    pub fn new(store: Store, auth: AuthService) -> Self { Self { store, auth } }
    async fn authenticated(&self, request: &VodRequest) -> Result<Option<User>, StoreError> {
        let user = self.auth.authenticate(&request.playback).await?;
        if user.is_some() {
            return Ok(user);
        }
        // QueryParamJWTAuthentication ignores invalid/expired query tokens. Valid
        // tokens with a deleted/inactive user still fail like JWT get_user().
        if let Some(token) = request.playback.parameter("token").filter(|value| !value.is_empty()) {
            if self.auth.decode_token(token, "access").is_ok() {
                let mut adapted = request.playback.clone();
                adapted.headers.push(("authorization".into(), format!("Bearer {token}")));
                return self.auth.authenticate(&adapted).await;
            }
        }
        Ok(None)
    }
}

impl VodResolver for SqlVodResolver {
    fn restore_user(&self, id: i64) -> BoxFuture<'_, Result<Option<User>, StoreError>> {
        Box::pin(async move { Ok(self.store.user_by_id(id).await?.filter(|u| u.is_active)) })
    }
    fn admin<'a>(&'a self, request: &'a PlaybackRequest) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let user = self.auth.authenticate(request).await?.ok_or(StoreError::Unauthorized("Authentication credentials were not provided."))?;
            if user.user_level < 10 || !self.store.network_allowed(self.auth.client_ip(request), "UI", Some(&user)).await? { return Err(StoreError::PermissionDenied); }
            Ok(())
        })
    }
    fn authorize<'a>(&'a self, request: &'a VodRequest) -> BoxFuture<'a, Result<Option<User>, StoreError>> {
        Box::pin(async move {
            // DRF authenticates before entering the view, then the native VOD
            // route applies STREAMS globally before its user VOD flag.
            let user = self.authenticated(request).await?;
            if !self.store.network_allowed(self.auth.client_ip(&request.playback), "STREAMS", None).await? {
                return Err(StoreError::Forbidden);
            }
            if let Some(kind) = ContentKind::parse(&request.kind) {
                if user.as_ref().is_some_and(|user| user.custom_properties.get(kind.flag()) == Some(&Value::Bool(false))) {
                    return Err(StoreError::Forbidden);
                }
            }
            Ok(user)
        })
    }

    fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async move {
            let settings = self.store.setting("stream_settings").await?;
            let id = settings.get("default_stream_profile").and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()));
            let Some(id) = id else { return Ok(false); };
            Ok(sqlx::query_scalar::<_, bool>("SELECT locked AND name='Redirect' FROM core_streamprofile WHERE id=$1")
                .bind(id).fetch_optional(self.store.pool()).await?.unwrap_or(false))
        })
    }

    fn resolve<'a>(&'a self, request: &'a VodRequest, user: Option<User>) -> BoxFuture<'a, Result<VodSource, StoreError>> {
        Box::pin(async move {
            let kind = ContentKind::parse(&request.kind).ok_or(StoreError::StatusBody(503, Value::String("No available stream".into())))?;
            if user.as_ref().is_some_and(|user| user.custom_properties.get(kind.flag()) == Some(&Value::Bool(false))) { return Err(StoreError::Forbidden); }
            if user.as_ref().is_some_and(|user| user.stream_limit > 0) {
                return Err(StoreError::Unsupported("VOD user-cap admission is not implemented"));
            }
            for name in ["utc_start", "start", "playliststart", "utc_end", "end", "playlistend", "offset", "seek", "t", "timestamp", "time"] {
                if request.playback.parameter(name).is_some_and(|value| !value.is_empty()) {
                    return Err(StoreError::Unsupported("VOD timeshift/offset transforms are not implemented"));
                }
            }
            let requested = uuid::Uuid::parse_str(&request.content_id).map_err(|_| StoreError::NotFound)?;
            let preferred_account = request.playback.parameter("m3u_account_id").and_then(|value| value.parse::<i64>().ok());
            let preferred_stream = request.playback.parameter("stream_id").filter(|value| !value.is_empty());
            let redirect = self.default_redirect().await?;
            let allowed: Option<HashSet<i64>> = if redirect && request.session_id.is_none() {
                user.as_ref().and_then(|user| user.custom_properties.get("allowed_m3u_profile_ids"))
                    .filter(|value| !value.is_null()).map(|value| value.as_array().and_then(|values| values.iter().map(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok())).collect::<Option<HashSet<_>>>()).unwrap_or_default())
            } else { None };
            let (content_table, relation_table, foreign_key) = if kind == ContentKind::Movie {
                ("vod_movie", "vod_m3umovierelation", "movie_id")
            } else { ("vod_episode", "vod_m3uepisoderelation", "episode_id") };
            let content = if kind == ContentKind::Series {
                sqlx::query("SELECT e.id::bigint AS id,e.uuid::text AS uuid,e.name FROM vod_episode e JOIN vod_series s ON s.id=e.series_id WHERE s.uuid=$1 ORDER BY e.season_number ASC NULLS LAST,e.episode_number ASC NULLS LAST,e.id LIMIT 1")
                    .bind(requested).fetch_optional(self.store.pool()).await?
            } else {
                let statement = format!("SELECT id::bigint AS id,uuid::text AS uuid,name FROM {content_table} WHERE uuid=$1");
                let found = sqlx::query(&statement).bind(requested).fetch_optional(self.store.pool()).await?;
                if found.is_none() && preferred_stream.is_some() {
                    let fallback = format!("SELECT c.id::bigint AS id,c.uuid::text AS uuid,c.name FROM {relation_table} r JOIN {content_table} c ON c.id=r.{foreign_key} JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.stream_id=$1 AND a.is_active ORDER BY CASE WHEN a.id=$2 THEN 0 ELSE 1 END,a.priority DESC,r.id LIMIT 1");
                    sqlx::query(&fallback).bind(preferred_stream).bind(preferred_account).fetch_optional(self.store.pool()).await?
                } else { found }
            }.ok_or(StoreError::StatusBody(503, Value::String("No available stream".into())))?;
            let content_id: i64 = content.try_get("id")?;
            let rows = sqlx::query(&format!("SELECT r.id::bigint AS relation_id,r.stream_id,r.container_extension,a.id::bigint AS account_id,a.account_type,a.server_url,a.username,a.password,a.server_group_id::bigint AS server_group_id,p.id::bigint AS profile_id,p.max_streams,p.search_pattern,p.replace_pattern,COALESCE(ua.user_agent,'Dispatcharr/0.33.0') AS user_agent FROM {relation_table} r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id JOIN m3u_m3uaccountprofile p ON p.m3u_account_id=a.id LEFT JOIN core_useragent ua ON ua.id=a.user_agent_id WHERE r.{foreign_key}=$1 AND a.is_active AND p.is_active AND (EXISTS(SELECT 1 FROM m3u_m3uaccountprofile d WHERE d.m3u_account_id=a.id AND d.is_default AND d.is_active) OR p.id=$4 OR $5) ORDER BY CASE WHEN r.stream_id=$2 THEN 0 WHEN a.id=$3 THEN 1 ELSE 2 END,a.priority DESC,r.id,CASE WHEN p.id=$4 THEN 0 WHEN p.is_default THEN 1 ELSE 2 END,p.id"))
                .bind(content_id).bind(preferred_stream).bind(preferred_account).bind(request.profile_id).bind(allowed.is_some())
                .fetch_all(self.store.pool()).await?;
            let mut candidates = Vec::new();
            let mut unsupported = false;
            for row in rows {
                let profile_id: i64 = row.try_get("profile_id")?;
                if allowed.as_ref().is_some_and(|ids| !ids.contains(&profile_id)) { continue; }
                let account_type: String = row.try_get("account_type")?;
                if account_type != "XC" { continue; }
                let search: String = row.try_get("search_pattern")?;
                let replacement: String = row.try_get("replace_pattern")?;
                if !((search.is_empty() || replacement.is_empty()) || (search == "^(.*)$" && replacement == "$1")) {
                    unsupported = true; continue;
                }
                if row.try_get::<Option<i64>, _>("server_group_id")?.is_some() {
                    unsupported = true; continue;
                }
                let server: Option<String> = row.try_get("server_url")?;
                let username: Option<String> = row.try_get("username")?;
                let password: Option<String> = row.try_get("password")?;
                let Some((server, username, password)) = server.zip(username).zip(password).map(|((s,u),p)| (s,u,p)) else { continue; };
                if username.is_empty() || password.is_empty() { continue; }
                let Ok(mut parsed) = Url::parse(server.trim()) else { continue; };
                let path = parsed.path().trim_end_matches('/');
                let last = path.rsplit('/').next().unwrap_or("");
                let normalized_path = if last.ends_with(".php") { path.strip_suffix(last).unwrap_or(path).trim_end_matches('/') } else { path }.to_owned();
                parsed.set_path(&normalized_path);
                parsed.set_query(None);
                parsed.set_fragment(None);
                if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() { unsupported = true; continue; }
                let path = if kind == ContentKind::Movie { "movie" } else { "series" };
                let extension: Option<String> = row.try_get("container_extension")?;
                let stream_id: String = row.try_get("stream_id")?;
                let extension = extension.filter(|value| !value.is_empty()).unwrap_or_else(|| "mp4".into());
                let url = format!("{}/{path}/{username}/{password}/{stream_id}.{extension}", parsed.as_str().trim_end_matches('/'));
                if Url::parse(&url).is_err() { continue; }
                candidates.push(VodCandidate { relation_id: row.try_get("relation_id")?, account_id: row.try_get("account_id")?,
                    profile_id, max_streams: row.try_get::<i32, _>("max_streams")?.max(0) as u32,
                    url, user_agent: row.try_get("user_agent")? });
            }
            if candidates.is_empty() {
                return Err(if unsupported { StoreError::Unsupported("VOD credential transforms/server groups are not implemented") }
                    else { StoreError::StatusBody(503, Value::String("No available stream".into())) });
            }
            Ok(VodSource { kind, content_uuid: content.try_get("uuid")?, name: content.try_get("name")?,
                user, candidates, redirect })
        })
    }
}
