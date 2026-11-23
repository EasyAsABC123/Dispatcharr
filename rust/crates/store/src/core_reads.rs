//! Core read contracts ported from Dispatcharr's core serializers and permission map.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{AuthService, StoreError};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, FromRequestParts, Path, Request, State},
    http::{header, request::Parts, HeaderMap, Method},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Row};
use std::net::SocketAddr;

#[derive(Clone, Copy)]
enum Resource {
    Settings,
    UserAgents,
    StreamProfiles,
    OutputProfiles,
}
impl Resource {
    fn query(self) -> &'static str {
        match self {
        Self::Settings => "SELECT id::bigint AS id,key,name,value FROM core_coresettings WHERE ($1::bigint IS NULL OR id=$1)",
        Self::UserAgents => "SELECT id::bigint AS id,name,user_agent,description,is_active,created_at,updated_at FROM core_useragent WHERE ($1::bigint IS NULL OR id=$1)",
        Self::StreamProfiles => "SELECT id::bigint AS id,name,command,parameters,is_active,locked,user_agent_id::bigint AS user_agent FROM core_streamprofile WHERE ($1::bigint IS NULL OR id=$1)",
        Self::OutputProfiles => "SELECT id::bigint AS id,name,command,parameters,is_active,locked FROM core_outputprofile WHERE ($1::bigint IS NULL OR id=$1)",
    }
    }
    fn model(self) -> &'static str {
        match self {
            Self::Settings => "CoreSettings",
            Self::UserAgents => "UserAgent",
            Self::StreamProfiles => "StreamProfile",
            Self::OutputProfiles => "OutputProfile",
        }
    }
    fn missing(self) -> StoreError {
        StoreError::StatusBody(
            404,
            json!({"detail":format!("No {} matches the given query.",self.model())}),
        )
    }
    fn serialize(self, row: PgRow) -> Result<Value, sqlx::Error> {
        if matches!(self, Self::Settings) {
            return Ok(settings_value(
                row.try_get("id")?,
                row.try_get("key")?,
                row.try_get("name")?,
                row.try_get("value")?,
            ));
        }
        let mut value =
            json!({"id":row.try_get::<i64,_>("id")?,"name":row.try_get::<String,_>("name")?});
        value["is_active"] = row.try_get::<bool, _>("is_active")?.into();
        if matches!(self, Self::UserAgents) {
            for field in ["user_agent", "description"] {
                value[field] = row.try_get::<String, _>(field)?.into();
            }
            for field in ["created_at", "updated_at"] {
                let date: DateTime<Utc> = row.try_get(field)?;
                value[field] = date
                    .to_rfc3339_opts(
                        if date.timestamp_subsec_micros() == 0 {
                            SecondsFormat::Secs
                        } else {
                            SecondsFormat::Micros
                        },
                        true,
                    )
                    .into();
            }
        } else {
            for field in ["command", "parameters"] {
                value[field] = row.try_get::<String, _>(field)?.into();
            }
            value["locked"] = row.try_get::<bool, _>("locked")?.into();
            if matches!(self, Self::StreamProfiles) {
                value["user_agent"] = row
                    .try_get::<Option<i64>, _>("user_agent")?
                    .map_or(Value::Null, Value::from);
            }
        }
        Ok(value)
    }
}

pub(crate) fn settings_value(id: i64, key: String, name: String, value: Value) -> Value {
    // Actual Source51 case16 ModelSerializer order, not map alphabetization.
    json!({"id":id,"key":key,"name":name,"value":value})
}
impl AuthService {
    /// Existing collection GET only, for explicit route composition alongside
    /// CoreSettingsWrites. It does NOT inherit the new bounded writer/reader
    /// ownership contract; other legacy read routes and Main are unchanged.
    #[doc(hidden)]
    pub fn core_settings_collection_router(self) -> Router {
        Router::new()
            .route("/api/core/settings/", get(settings))
            .with_state(self)
    }
}
pub(crate) async fn user_agent_value(auth: &AuthService, id: i64) -> Result<Value, StoreError> {
    let resource = Resource::UserAgents;
    let row = sqlx::query(resource.query())
        .bind(Some(id))
        .fetch_optional(auth.store.pool())
        .await?
        .ok_or_else(|| resource.missing())?;
    Ok(resource.serialize(row)?)
}
async fn read(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    resource: Resource,
    id: Option<String>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    // DRF applies permissions before looking up an object, including malformed IDs.
    let id = id
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))
        })
        .transpose()?;
    let rows = sqlx::query(resource.query())
        .bind(id)
        .fetch_all(auth.store.pool())
        .await?;
    let mut values = rows
        .into_iter()
        .map(|row| resource.serialize(row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(if id.is_some() {
        values.pop().ok_or_else(|| resource.missing())?
    } else {
        Value::Array(values)
    }))
}

macro_rules! endpoints {
    ($list:ident,$detail:ident,$kind:ident) => {
        async fn $list(
            State(auth): State<AuthService>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            read(auth, peer, headers, Resource::$kind, None).await
        }
        async fn $detail(
            State(auth): State<AuthService>,
            Path(id): Path<String>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            read(auth, peer, headers, Resource::$kind, Some(id)).await
        }
    };
}
endpoints!(settings, setting, Settings);
endpoints!(useragents, useragent, UserAgents);
endpoints!(streamprofiles, streamprofile, StreamProfiles);
endpoints!(outputprofiles, outputprofile, OutputProfiles);

const USER_AGENT_FIELDS: &[&str] = &[
    "id",
    "name",
    "user_agent",
    "description",
    "is_active",
    "created_at",
    "updated_at",
];
const STREAM_PROFILE_FIELDS: &[&str] = &[
    "id",
    "name",
    "command",
    "parameters",
    "is_active",
    "user_agent",
    "locked",
];

fn order_profile_fields(fields: &mut Map<String, Value>, source_order: &[&str]) {
    let mut ordered = Map::with_capacity(fields.len());
    for key in source_order {
        if let Some((name, value)) = fields.shift_remove_entry(*key) {
            ordered.insert(name, value);
        }
    }
    ordered.extend(std::mem::take(fields));
    *fields = ordered;
}

fn direct_profile_response(
    method: Method,
    detail: bool,
    source_order: &[&str],
    Json(mut value): Json<Value>,
) -> Response {
    if method != Method::GET {
        return Json(value).into_response();
    }
    match &mut value {
        Value::Array(rows) => {
            for fields in rows.iter_mut().filter_map(Value::as_object_mut) {
                order_profile_fields(fields, source_order);
            }
        }
        Value::Object(fields) => order_profile_fields(fields, source_order),
        _ => {}
    }
    let allow = if detail {
        "GET, PUT, PATCH, DELETE, HEAD, OPTIONS"
    } else {
        "GET, POST, HEAD, OPTIONS"
    };
    // Main's existing response middleware appends the origin token to Vary.
    (
        [(header::ALLOW, allow), (header::VARY, "Accept")],
        Json(value),
    )
        .into_response()
}

macro_rules! direct_profile_gets {
    ($list:ident, $detail:ident, $canonical_list:ident, $canonical_detail:ident, $fields:ident) => {
        async fn $list(
            auth: State<AuthService>,
            peer: ConnectInfo<SocketAddr>,
            method: Method,
            headers: HeaderMap,
        ) -> Result<Response, StoreError> {
            let value = $canonical_list(auth, peer, headers).await?;
            Ok(direct_profile_response(method, false, $fields, value))
        }

        async fn $detail(
            auth: State<AuthService>,
            id: Path<String>,
            peer: ConnectInfo<SocketAddr>,
            method: Method,
            headers: HeaderMap,
        ) -> Result<Response, StoreError> {
            let value = $canonical_detail(auth, id, peer, headers).await?;
            Ok(direct_profile_response(method, true, $fields, value))
        }
    };
}
direct_profile_gets!(
    direct_useragents,
    direct_useragent,
    useragents,
    useragent,
    USER_AGENT_FIELDS
);
direct_profile_gets!(
    direct_streamprofiles,
    direct_streamprofile,
    streamprofiles,
    streamprofile,
    STREAM_PROFILE_FIELDS
);

struct JsonProfileId(String);

fn literal_json_profile_id(path: &str) -> Option<&str> {
    let (_, final_segment) = path.rsplit_once('/')?;
    let key = final_segment.strip_suffix(".json")?;
    (!key.is_empty() && key.bytes().all(|byte| byte.is_ascii_digit())).then_some(key)
}

impl<S: Send + Sync> FromRequestParts<S> for JsonProfileId {
    type Rejection = StoreError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        literal_json_profile_id(parts.uri.path())
            .map(|id| Self(id.to_owned()))
            .ok_or(StoreError::NotFound)
    }
}

async fn omitted_json_method() -> StoreError {
    StoreError::NotFound
}

macro_rules! json_reader {
    ($alias:ident, $canonical:ident) => {
        async fn $alias(
            auth: State<AuthService>,
            JsonProfileId(id): JsonProfileId,
            peer: ConnectInfo<SocketAddr>,
            headers: HeaderMap,
        ) -> Result<Json<Value>, StoreError> {
            $canonical(auth, Path(id), peer, headers).await
        }
    };
}
json_reader!(useragent_json, useragent);
json_reader!(streamprofile_json, streamprofile);
json_reader!(outputprofile_json, outputprofile);

macro_rules! useragent_json_writer {
    ($alias:ident, $canonical:ident, $payload:ty) => {
        async fn $alias(
            auth: State<AuthService>,
            peer: ConnectInfo<SocketAddr>,
            JsonProfileId(id): JsonProfileId,
            payload: $payload,
        ) -> Result<Response, StoreError> {
            crate::user_agent_writes::$canonical(auth, peer, Path(id), payload).await
        }
    };
}
useragent_json_writer!(patch_useragent_json, patch, Request);
useragent_json_writer!(put_useragent_json, put, Request);
useragent_json_writer!(delete_useragent_json, delete, HeaderMap);

async fn put_streamprofile_json(
    auth: State<AuthService>,
    JsonProfileId(id): JsonProfileId,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, StoreError> {
    crate::core_stream_writes::put(auth, Path(id), peer, headers, body).await
}

pub(crate) fn router(auth: AuthService) -> Router {
    router_with_settings_detail(auth, true)
}
pub(crate) fn router_without_settings_detail(auth: AuthService) -> Router {
    router_with_settings_detail(auth, false)
}
fn router_with_settings_detail(auth: AuthService, detail: bool) -> Router {
    let mut router = Router::new()
        .route("/api/core/settings/", get(settings))
        .route(
            "/api/core/useragents/",
            get(direct_useragents).post(crate::user_agent_writes::create),
        )
        .route(
            "/api/core/useragents/{id}/",
            get(direct_useragent)
                .patch(crate::user_agent_writes::patch)
                .put(crate::user_agent_writes::put)
                .delete(crate::user_agent_writes::delete),
        )
        .route(
            "/api/core/streamprofiles/",
            get(direct_streamprofiles).post(crate::core_stream_writes::create),
        )
        .route(
            "/api/core/streamprofiles/{id}/",
            get(direct_streamprofile)
                .patch(crate::core_stream_writes::patch)
                .put(crate::core_stream_writes::put)
                .delete(crate::core_stream_writes::delete),
        )
        .route("/api/core/outputprofiles/", get(outputprofiles))
        .route("/api/core/outputprofiles/{id}/", get(outputprofile))
        .route(
            "/api/core/useragents.json",
            get(useragents)
                .post(crate::user_agent_writes::create)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/core/useragents/{json_id}",
            get(useragent_json)
                .patch(patch_useragent_json)
                .put(put_useragent_json)
                .delete(delete_useragent_json)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/core/streamprofiles.json",
            get(streamprofiles)
                .post(crate::core_stream_writes::create)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/core/streamprofiles/{json_id}",
            get(streamprofile_json)
                .put(put_streamprofile_json)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/core/outputprofiles.json",
            get(outputprofiles)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        )
        .route(
            "/api/core/outputprofiles/{json_id}",
            get(outputprofile_json)
                .head(omitted_json_method)
                .fallback(omitted_json_method),
        );
    if detail {
        router = router.route("/api/core/settings/{id}/", get(setting));
    }
    router.with_state(auth)
}

#[cfg(test)]
mod special_source16_order_tests {
    use super::*;
    #[test]
    fn existing_collection_dto_preserves_actual_source16_literal_bytes() {
        use sha2::{Digest, Sha256};
        let vectors: Value =
            serde_json::from_str(include_str!("core_settings_special_vectors.json")).unwrap();
        let case = vectors["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["id"] == 16)
            .unwrap();
        let rows = case["json"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                settings_value(
                    row["id"].as_i64().unwrap(),
                    row["key"].as_str().unwrap().into(),
                    row["name"].as_str().unwrap().into(),
                    row["value"].clone(),
                )
            })
            .collect::<Vec<_>>();
        let raw = serde_json::to_vec(&rows).unwrap();
        let hex = raw.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(hex, case["response_body_hex"]);
        assert_eq!(
            format!("{:x}", Sha256::digest(&raw)),
            case["response_body_sha256"].as_str().unwrap()
        );
        assert_eq!(raw.len(), 1147);
    }
}

#[cfg(test)]
mod direct_profile_wire_tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn successful_direct_profile_gets_use_original_wire_order_and_headers() {
        let agent = json!({"id":4,"name":"Owned","is_active":true,"user_agent":"Owned/1.0",
            "description":"fixture","created_at":"2026-01-08T08:12:03.123456Z",
            "updated_at":"2026-01-08T08:12:03.123456Z"});
        let profile = json!({"id":9,"name":"Owned profile","is_active":true,"command":"ffmpeg",
            "parameters":"-v quiet","locked":false,"user_agent":null});
        let agent_bytes = r#"{"id":4,"name":"Owned","user_agent":"Owned/1.0","description":"fixture","is_active":true,"created_at":"2026-01-08T08:12:03.123456Z","updated_at":"2026-01-08T08:12:03.123456Z"}"#;
        let profile_bytes = r#"{"id":9,"name":"Owned profile","command":"ffmpeg","parameters":"-v quiet","is_active":true,"user_agent":null,"locked":false}"#;
        for (body, fields, row) in [
            (agent, USER_AGENT_FIELDS, agent_bytes),
            (profile, STREAM_PROFILE_FIELDS, profile_bytes),
        ] {
            for (detail, payload, expected, allow) in [
                (
                    true,
                    body.clone(),
                    row.to_owned(),
                    "GET, PUT, PATCH, DELETE, HEAD, OPTIONS",
                ),
                (
                    false,
                    json!([body.clone(), body.clone()]),
                    format!("[{row},{row}]"),
                    "GET, POST, HEAD, OPTIONS",
                ),
            ] {
                let response = direct_profile_response(Method::GET, detail, fields, Json(payload));
                assert_eq!(response.status(), axum::http::StatusCode::OK);
                assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
                assert_eq!(response.headers()[header::ALLOW], allow);
                assert_eq!(response.headers()[header::VARY], "Accept");
                let raw = to_bytes(response.into_body(), 4096).await.unwrap();
                assert_eq!(raw.as_ref(), expected.as_bytes());
            }
            let legacy = serde_json::to_vec(&body).unwrap();
            let head = direct_profile_response(Method::HEAD, true, fields, Json(body));
            assert!(!head.headers().contains_key(header::ALLOW));
            assert!(!head.headers().contains_key(header::VARY));
            let raw = to_bytes(head.into_body(), 4096).await.unwrap();
            assert_eq!(raw.as_ref(), legacy.as_slice());
        }
    }
}

#[cfg(test)]
mod json_profile_tests {
    use super::*;
    use axum::{body::Body, http::StatusCode};
    use tower::ServiceExt;

    #[test]
    fn literal_decimal_suffixes_are_independent_of_canonical_routes() {
        assert_eq!(
            literal_json_profile_id("/api/core/useragents/0001.json"),
            Some("0001")
        );
        for path in [
            "/api/core/useragents/12/",
            "/api/core/useragents/12",
            "/api/core/useragents/12.json/",
            "/api/core/useragents/12.csv",
            "/api/core/useragents/%31.json",
            "/api/core/useragents/12.2.json",
            "/api/core/useragents/.json",
        ] {
            assert_eq!(literal_json_profile_id(path), None, "{path}");
        }
    }

    #[tokio::test]
    async fn alias_extractor_does_not_change_the_canonical_path() {
        let app = Router::new()
            .route(
                "/api/core/useragents/{id}/",
                get(|Path(id): Path<String>| async move {
                    assert_eq!(id, "12");
                    StatusCode::OK
                }),
            )
            .route(
                "/api/core/useragents/{json_id}",
                get(|JsonProfileId(id): JsonProfileId| async move {
                    assert_eq!(id, "12");
                    StatusCode::ACCEPTED
                })
                .head(omitted_json_method)
                .fallback(omitted_json_method),
            );
        for (method, path, status) in [
            ("GET", "/api/core/useragents/12.json", StatusCode::ACCEPTED),
            ("GET", "/api/core/useragents/12/", StatusCode::OK),
            ("GET", "/api/core/useragents/12.csv", StatusCode::NOT_FOUND),
            ("GET", "/api/core/useragents/12", StatusCode::NOT_FOUND),
            (
                "HEAD",
                "/api/core/useragents/12.json",
                StatusCode::NOT_FOUND,
            ),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap();
            assert_eq!(app.clone().oneshot(request).await.unwrap().status(), status);
        }
    }
}
