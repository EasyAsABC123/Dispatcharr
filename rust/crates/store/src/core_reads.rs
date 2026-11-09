//! Core read contracts ported from Dispatcharr's core serializers and permission map.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, Path, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
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
        let mut value =
            json!({"id":row.try_get::<i64,_>("id")?,"name":row.try_get::<String,_>("name")?});
        if matches!(self, Self::Settings) {
            value["key"] = row.try_get::<String, _>("key")?.into();
            value["value"] = row.try_get::<Value, _>("value")?;
        } else {
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
        }
        Ok(value)
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
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/core/settings/", get(settings))
        .route("/api/core/settings/{id}/", get(setting))
        .route(
            "/api/core/useragents/",
            get(useragents).post(crate::user_agent_writes::create),
        )
        .route(
            "/api/core/useragents/{id}/",
            get(useragent)
                .patch(crate::user_agent_writes::patch)
                .put(crate::user_agent_writes::put)
                .delete(crate::user_agent_writes::delete),
        )
        .route(
            "/api/core/streamprofiles/",
            get(streamprofiles).post(crate::core_stream_writes::create),
        )
        .route(
            "/api/core/streamprofiles/{id}/",
            get(streamprofile)
                .patch(crate::core_stream_writes::patch)
                .put(crate::core_stream_writes::put)
                .delete(crate::core_stream_writes::delete),
        )
        .route("/api/core/outputprofiles/", get(outputprofiles))
        .route("/api/core/outputprofiles/{id}/", get(outputprofile))
        .with_state(auth)
}
